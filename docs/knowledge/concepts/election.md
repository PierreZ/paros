# Election, P2c merge and gap fill

Group: Leader. Status: verified 2026-10-31.

A node becomes the leader of a cell by winning **Phase 1** at a fresh ballot. The win gives it
two duties before it may propose new commands: re-propose every value that a quorum reports
(the **P2c merge**), and fill every slot that nobody reports with a `Noop` (the **gap fill**).
paros also records every round the leader opens in the leader's own log
(`record_own_round`), so the allocator frontier survives a restart.

## 1. Problem

Multi-Paxos runs one Phase 1 for many slots, then streams Phase 2 for each new command
([PMMC §2 "Leaders, Commanders, Scouts"]; [PML §4.2]). When the leader changes, the new
leader does not know which slots the old leader got chosen. It must find out, for each slot
in the uncommitted suffix, whether a value **could have been** chosen, and if so, propose that
same value. If it proposes another value, two values are chosen for one slot.

A pipelined leader adds a second problem. The old leader can get slot 8 chosen while slot 7
reached only one acceptor. No quorum reports slot 7, so the new leader has nothing to
re-propose there. If it starts its allocator at 9, no node ever proposes slot 7 again, and the
contiguous chosen prefix stops at 6 for ever ([paxos-vs-raft §5]: "classic Paxos often allows
out-of-order decisions + a gap-filling protocol").

## 2. Model

- Crash-recovery processes with durable storage. A process that restarts keeps its promise and
  its accepted records ([PMMC §3]: a process that recovers from disk is slow, not crashed).
- Asynchronous network: messages can be lost, delayed, duplicated and reordered. No bound on
  delay is needed for safety.
- Liveness needs a period where timeouts are long enough for one Phase 1 round trip (§7).
- Clocks: none in `paros-core`. Time is the driver's `tick()`. The driver draws the election
  timeout in `[T, 2T)` ticks (`draw_election_timeout`, `paros-roles/src/node/report.rs`).

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Acceptor | `promised` ballot | yes, `WriteOp` `SetPromise`, flushed before the `Promise` leaves |
| Acceptor | accepted record per slot: `(Ballot, Command)` | yes, flushed before the `Accepted` leaves |
| Candidate | `Election`: the ballot, `C_b`, `H_b` (`prior`), `recovered`, `faulty_reports`, the promise tally | no, volatile (`proposer/election.rs`) |
| Leader | `ballot`, `next_slot` (allocator frontier), `Recovery` cursor, Phase-2 rounds | `ballot` and `next_slot` are derived again at boot from the durable promise and records; the rest is volatile |

A `Ballot` is `{ round: u64, node: NodeId }`, ordered by round, then by node
(`paros-core/src/types.rs`). The node id makes every ballot belong to one proposer
([PMMC §2 "The Synod Protocol"]: lexicographic `(int, leader-id)` pairs).

## 4. Normal flow

1. The election clock fires on a non-leader (`ColocatedNode::tick` → `on_check_leader`).
2. `campaign` picks a round strictly above the promise, the operating ballot and the
   `round_floor` (`next_campaign_round`), promises it durably, and drops all leadership state.
3. On a deployment with matchmakers, the candidate first registers `(b, C_b)` (see the
   matchmaking card). On a plain deployment it goes straight to `start_phase1` with
   `H_b = [C]`.
4. `start_phase1` opens the `Election` with the candidate's own records as its first promise,
   sends `Prepare { ballot, from_slot, config }` to every member of `H_b ∪ {C_b}`, and
   broadcasts a `CatchUpRequest` from the same slot.
5. Each acceptor that can promise raises its promise durably and answers with a `Promise` page
   of its accepted suffix from `from_slot` (`on_prepare`, `node/acceptor.rs`).
6. `fold_promise` merges each page by highest ballot per slot (`merge_report`).
7. When `Election::covered` is true, `try_become_leader` closes Phase 1, sets `next_slot` one
   past the highest reported slot, opens the authority (see the CheckQuorum card), and opens a
   `Recovery` with `RecoveryPolicy::Phase1Backed`.
8. `pump_leader_recovery` walks `[first_unchosen, next_slot)` one bounded page at a time: a
   reported slot is re-proposed with its value (`RecoveryStep::Recovered`), an unreported slot
   gets `Control::Noop` (`RecoveryStep::Fill`).

```mermaid
sequenceDiagram
    participant C as Candidate (ballot b)
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    Note over C: tick: election timeout fires<br/>promise b durably
    C->>A1: Prepare(b, from_slot=5)
    C->>A2: Prepare(b, from_slot=5)
    Note over A1,A2: raise promise to b, persist, then send
    A1->>C: Promise(b, {6:(b1,x), 8:(b1,z)})
    A2->>C: Promise(b, {6:(b0,y)})
    Note over C: P2c merge: 6→x (b1 > b0), 8→z<br/>quorum covered: leader, next_slot = 9
    C->>A1: Accept(b, 5, Noop)
    C->>A1: Accept(b, 6, x)
    C->>A1: Accept(b, 7, Noop)
    C->>A1: Accept(b, 8, z)
    Note over C: same Accepts to A2; slots 5 and 7 are gap fills
```

## 5. Recovery flow

- **The candidate loses.** A `Nack` at the campaign's ballot (`on_nack`) or a higher `Prepare`
  closes the campaign. The node waits for its next election timeout and campaigns again above
  everything it saw ([PMMC §2 "Leaders, Commanders, Scouts"]: preempted, go passive, bump the ballot).
- **The candidate crashes.** Leadership is volatile: `ColocatedNode::new` boots a `Follower`.
  The durable promise stays, so the next campaign is at a strictly higher round.
- **The new leader crashes mid-recovery.** The re-proposals it sent are ordinary accepted
  records at its ballot. The next leader's Phase 1 reports them like any other value.
- **An acceptor truncated the range.** It answers `Nack` without raising its promise
  (`PrepareOutcome::BelowFloor`). It never sends a short `Promise`, because a short report
  would let the gap fill overwrite a chosen value.

## 6. Why it is safe

The invariant is [PMMC §2] **A5**: if a quorum accepted `⟨b, s, c⟩`, every pvalue
`⟨b', s, c'⟩` with `b' > b` has `c' = c`. The leader enforces it per slot with the rule
[PMMC §2 "Passive → active"] calls the correctness crux:

1. **A slot with reports.** Take the value with the highest accepted ballot (`merge_report`).
   If a value `v` was chosen at ballot `b0`, a quorum accepted it, and that quorum meets the
   promise quorum. By A5 every acceptance above `b0` carries `v`, so the highest report is `v`.
   This is the same rule in [PML §4.1] ("adopt the value with the highest sequence number")
   and in [paxos-vs-raft §3.3] (the candidate adopts the received entry with the greatest term).
2. **A slot with no report.** Nothing can be chosen there below `b`. A chosen value would have
   a quorum, the quorum would meet the promise quorum, and one `Promise` would report it.
   Nothing reported means free; the leader may propose anything, and paros proposes `Noop`
   ([PMMC §2], case 1; frankenpaxos `Leader.scala:541`, which fills with `Noop` in the same
   loop, see `docs/references/frankenpaxos/03-multipaxos-core.md` §3).
3. **One proposer per ballot.** Only the node in `ballot.node` proposes at `ballot`, and it
   proposes at most one command per slot ([PMMC §2] commander invariant **C1**). The allocator
   `next_slot` only moves forward, so a slot is never allocated twice at one ballot.

Rule 3 is where `record_own_round` matters. The allocator is derived at boot from the
accepted log. If a leader opens a round but leaves no record of it, a restart rewinds the
frontier, and an inherited authority (see the handoff card) can allocate the slot again at
the same ballot. So the leader records every round it opens in its own log, durably, before
the `Accept` leaves, even when its own vote does not count.

Under matchmakers, the quorum in rules 1 and 2 must be a Phase-1 quorum of **every** prior
configuration in `H_b`, not a quorum of their union (`Election::covered`, #121). A union
quorum drawn mostly from `C1` can miss every Phase-2 quorum of `C2`.

## 7. Liveness

- Two candidates can preempt each other for ever ([PMMC §3]: FLP; the "ping-pong").
  paros breaks the tie with randomized election timeouts in `[T, 2T)`. The floor of `T`
  is one Phase-1 round trip with its `fsync`s (`election_timeout_base`, 10 ticks,
  `paros-tunables/src/floors.rs`).
- A long suffix pages: the `Promise` comes in bounded pages (`promise_page`). The paging
  campaign keeps its promisers quiet with `keep_alive_from` (#428). That is the
  paging card's subject.
- A recovery drains one bounded page per `Ready` batch (`recovery_page`), so a long gap fill
  does not block the node loop.
- The win needs a Phase-1 quorum of every configuration in `H_b` to answer.

## 8. Costs

- Phase 1: one round trip (`Prepare`, `Promise`), plus one more per extra `Promise` page.
  One durable write per acceptor (the promise) before it answers.
- Message count: `2n` for `n` addressees, plus pages.
- Quorum: a Phase-1 quorum of each configuration in `H_b`; under a majority system that is
  `⌊n/2⌋ + 1` acceptors.
- Recovery: one Phase-2 round per slot in `[first_unchosen, next_slot)`, re-proposed or filled.
- Compared with Raft, a Paxos election ships log entries in the votes ([paxos-vs-raft §4]),
  but it has no split votes, because ballots are per-node.

## 9. In paros

| What | Where |
|---|---|
| The campaign clock and ballot choice | `paros-core/src/node/election.rs`: `on_check_leader`, `next_campaign_round`, `campaign` |
| The Phase-1 boundary | `node/election.rs`: `start_phase1` |
| The promise tally and P2c merge | `paros-core/src/proposer/election.rs`: `Election`, `Proposer::open_phase1`, `fold_promise`, `phase1_won`, `close_phase1`; `proposer.rs`: `merge_report` |
| The win, the allocator, the gap fill | `node/election.rs`: `try_become_leader`, `pump_leader_recovery` |
| What licenses a fill | `paros-core/src/proposer/recovery.rs`: `RecoveryPolicy::Phase1Backed` vs `Inherited` |
| The `Noop` | `paros-core/src/journal/command.rs`: `Control::Noop` |
| The durable frontier | `paros-core/src/node/phase2.rs`: `record_own_round` |
| The acceptor side | `paros-core/src/node/acceptor.rs`: `on_prepare` |

The rule is stated in the doc comments of `Election` (cross-configuration completion),
`RecoveryPolicy`, `try_become_leader` (the gap-fill block) and `record_own_round`. AGENTS.md
lists two of them under "Protocol invariants to remember".

**Sim gates and oracles** (`paros-sim/src/audit/`):

- `"one ballot proposes at most one command for a slot"` and
  `"a durable accept quorum never decides two values for a slot"` (`state.rs`): the safety
  claims this card exists for.
- `"a new leader gap-fills a hole its promise quorum never reported"` (`node.rs`, a
  `reach_once!`): the gap fill runs.
- `"a fresh leader has not promised a ballot above the one it won"` and
  `"a node's leadership ballots strictly increase"` (`node.rs`).
- Core probe `"phase1: a promise quorum reports two different values for one slot and P2c keeps
  the higher ballot"` (`proposer.rs`): the merge sees a real conflict.
- Unit tests: `election_fills_a_hole_the_promise_quorum_never_reported`,
  `recovery_picks_highest_ballot_value_per_slot`, `truncated_quorum_refuses_a_blind_candidate`
  (`paros-core/src/node/tests/election.rs`).

**Divergences from the sources:**

- [PMMC §2] lets each ballot's leader pick any free value; paros always picks `Noop`, which
  carries no client id and does nothing at apply (commit `bf28d0f`).
- [paxos-vs-raft §3.3] re-stamps each adopted entry with the new term; paros does the same
  (the re-proposal runs at the new ballot), and Raft does not.
- frankenpaxos sends the recovery `Phase2a`s through a proxy leader. paros never delegates an
  election's recovery: `pump_leader_recovery` uses `Delegation::Colocated` for
  `Phase1Backed`, so a fresh leadership depends on no proxy (`node/phase2.rs` module doc).
- [PMMC §4.1] sends the first unknown slot in `p1a`; paros does the same with `from_slot`, and
  starts it lower when a faulty or folded hole sits below the chosen prefix
  (`start_phase1`, Stage 8).

## 10. Pitfalls

- **The unreported hole (#54).** The first leader re-proposed only what the quorum reported.
  `next_slot` jumped over a slot that one acceptor held alone, and the chosen prefix froze for
  ever. Seeds 53 and 249 were red; the `Noop` fill made them green (commit `bf28d0f`).
- **A win below the node's own promise (#67, #88).** A `Commit` learned mid-campaign raised the
  promise past the campaign's ballot. The win then skipped every self-accept, `next_slot`
  landed under an in-flight slot, and a second command could go to one `(slot, ballot)`.
  `phase1_won` now refuses a ballot below the promise (commit `5bef695`), and every path that
  raises the promise closes the campaign (witness 13099780746496209636, found on PR #556,
  fixed in commit `629aa62`, #577).
- **Union quorum is not enough (#121).** See §6. `Election::covered` is one predicate in one
  place for that reason.
- **The read basis before recovery (#263).** Under `q2 = 1` the leader's own accept decides a
  re-proposed slot inside the pump. The read basis must be open first (witness
  17670199652380691451, commit `11486ba`).
- **The record of a delegated round.** The proxy model checker (seed 756) found that a
  delegated round left no record at the leader; a rebooted successor rewound its frontier and
  allocated one slot twice at one ballot. `record_own_round` closed it.
- **Fill only from a Phase-1 report.** A `Noop` fill without a quorum report can overwrite a
  chosen value. That is why `RecoveryPolicy` is an explicit type and why a handoff never fills.

## 11. Relatives

- [Leadership fence and CheckQuorum](check-quorum.md): the authority the win opens.
- [Leader handoff](leader-handoff.md): a leadership without Phase 1, and the reason
  `RecoveryPolicy::Inherited` exists.
- Paging a campaign (`keep_alive_from`, #428): card waits for a source.
- [Single-decree Paxos](single-decree-paxos.md), [the Multi-Paxos log](multi-paxos-log.md),
  [persist before send](persist-before-send.md), [pipelining and resend](pipelining-and-resend.md),
  [catch-up](catch-up.md): the Kernel cards.
- Reconfiguration cards (planned): matchmaking, `H_b` and `C_b`.
- Storage cards (planned): the CTRL repair probe that takes the slots Phase 1 cannot decide.

## 12. Sources

- [PMMC] van Renesse, Altinbuken, *Paxos Made Moderately Complex*:
  `docs/references/papers/paxos-made-moderately-complex/transcript.md` §2 (acceptor invariants
  A1–A5, commander invariants C1–C2, "Passive → active selection"), §3 (liveness), §4.1 (state
  reduction, first unknown slot).
- [PML] Chandra, Griesemer, Redstone, *Paxos Made Live*:
  `docs/references/papers/paxos-made-live/transcript.md` §4.1 (promise carries the most recent
  value; adopt the highest), §4.2 (Multi-Paxos, the master optimization).
- [paxos-vs-raft] Howard, Mortier: `docs/references/papers/paxos-vs-raft/transcript.md` §3.3
  (Paxos election adopts the greatest-term entry), §4 (election cost), §5 (gap filling).
- frankenpaxos, `Leader.scala:541` (Phase-1 gap fill with `Noop`) and `Acceptor.scala:166`:
  `docs/references/frankenpaxos/03-multipaxos-core.md` §3.

[PMMC §2]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PMMC §2 "Leaders, Commanders, Scouts"]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PMMC §2 "The Synod Protocol"]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PMMC §2 "Passive → active"]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PMMC §3]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PMMC §4.1]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PML §4.1]: ../../references/papers/paxos-made-live/transcript.md
[PML §4.2]: ../../references/papers/paxos-made-live/transcript.md
[paxos-vs-raft §3.3]: ../../references/papers/paxos-vs-raft/transcript.md
[paxos-vs-raft §4]: ../../references/papers/paxos-vs-raft/transcript.md
[paxos-vs-raft §5]: ../../references/papers/paxos-vs-raft/transcript.md
