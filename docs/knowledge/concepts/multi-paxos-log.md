# Multi-Paxos log and stable leader

Group: Kernel. Status: verified 2026-10-31.

Multi-Paxos runs one single-decree instance per **slot** of a log. A stable leader runs
**one** Phase 1 for every slot from its first unchosen slot on, then pays only Phase 2 per new
command. A replica applies the chosen commands in slot order. In paros the log is a journal:
each slot carries a `Command` (a `Write`, a `Truncate`, a `SetLeader`, a `Noop`), and the
"state machine" is the journal-control fold (`journal/state.rs`).

## 1. Problem

A replicated state machine needs every replica to apply the same commands in the same order
([PMS §3]; [PMMC §2] invariants R1–R4). Single-decree Paxos agrees on one value at a cost of two
round trips. Running it from scratch for each command is slow and contends for nothing.

## 2. Model

The single-decree model ([PMS §2.1]), plus:

- one log of slots `0, 1, 2, …`, one Paxos instance per slot;
- a slot is **chosen** when a Phase-2 quorum accepts one `(ballot, command)` there;
- the **chosen prefix** is the longest run of chosen slots from the floor: a replica applies
  only inside it.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Acceptor | one promise for **all** slots, and one accepted `(Ballot, Command)` per slot | yes |
| Proposer (leader) | `ballot`, `next_slot`, one open `Round` per in-flight slot | no; `next_slot` is derived from the log at boot |
| Replica (learner) | the chosen map, the chosen index (the commit index), the journal fold | the chosen index is a relaxed write (`WriteOp::SetChosenIndex`); the fold is rebuilt at boot |

[PMMC §2] separates replicas, leaders and acceptors. paros keeps one component per role in
`paros-core` (`acceptor.rs`, `proposer.rs`, `replica.rs`) and wires them in `ColocatedNode`.
A component never reads another's state: the caller hands in what a tally needs (the
`proposer.rs` module doc).

## 4. Normal flow

1. A client's call reaches the leader: `ColocatedNode::propose` (a `Write`) or
   `propose_control` (a `Truncate`, a `SetLeader`). A non-leader answers
   `ProposeResult::NotLeader` with a hint.
2. The leader takes `next_slot`, records the round in its own log (`record_own_round`), opens
   a `Round` and sends `Accept { ballot, slot, command }` to the Phase-2 addressees
   (`start_accept_round`, `node/phase2.rs`).
3. Each acceptor accepts if `ballot` is at or above its promise, writes the record, and answers
   `Accepted`.
4. With a Phase-2 quorum (`try_decide`), the leader marks the slot chosen and broadcasts
   `Commit { ballot, slot, command }` to the learners.
5. Each node learns (`on_commit` → `mark_chosen`, `node/learn.rs`). The replica walks the
   contiguous prefix (`Replica::advance`) and folds each command into the journal state, in
   slot order. `Ready::committed` hands the driver the newly applied slots.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader (ballot b, Phase 1 done)
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    C->>L: Write(w)
    Note over L: slot 7 = next_slot; record own round
    L->>A1: Accept(b, 7, w)
    L->>A2: Accept(b, 7, w)
    A1->>L: Accepted(b, 7)
    Note over L: quorum {L, A1}: slot 7 chosen
    L->>A1: Commit(b, 7, w)
    L->>A2: Commit(b, 7, w)
    Note over L,A2: each replica applies 7 once 0..6 are applied
    L->>C: outcome of slot 7
```

## 5. Recovery flow

- **A leader change.** The new leader runs one Phase 1 over the whole suffix from its first
  unchosen slot ([PMS §3]: "one proposal number" for all instances; [PMMC §4.1]: send the first
  unknown slot). It re-proposes what is reported and fills the rest with `Noop`. See the
  election card.
- **A missed `Commit`.** A learner that missed it learns the slot by catch-up. See the catch-up
  card.
- **A restart.** The acceptor reads back its promise and records; the replica rebuilds the fold
  from the floor's sealed state and the retained log. The chosen index can lag (a relaxed
  write): catch-up and the next beat raise it again.

## 6. Why it is safe

Each slot is a single-decree instance, so the single-decree argument holds per slot. Two
facts make one Phase 1 serve all slots:

- **One promise covers every slot.** A promise at `b` refuses every `Accept` below `b` at any
  slot (`Acceptor::admit`). So a Phase 1 at `b` constrains every slot at once.
- **One proposer per ballot, one command per slot.** The leader's allocator only moves
  forward, and the leader opens at most one round per slot per leadership ([PMMC §2] commander
  invariant **C1**, which gives acceptor invariant **A4**). The sim checks it directly:
  `"one ballot proposes at most one command for a slot"`.

The replica applies only the contiguous chosen prefix. A slot outside the prefix is never
applied, so every replica applies the same sequence ([PMMC §2] R1–R3 ⇒ same state).

## 7. Liveness

- One stable leader per cell ([PMS §2.4], [PML §4.2] "master"). paros elects it with a
  randomized election timeout and keeps it with beats (`HEARTBEAT_TICKS` = 1).
- A pipelined leader can leave holes; the next leader fills them (election card), and the
  current one re-sends open rounds (pipelining card).
- A request to a non-leader gets a hint, not a forward: the client retries at the leader.

## 8. Costs

- Steady state: one round trip from the leader per command, one durable write per acceptor
  ([PML §4.2]: the master optimization reduces the instance to one write per replica). Phase 2
  is the minimum cost of any fault-tolerant agreement ([PMS §3], Keidar and Rajsbaum).
- Messages per command: `n` `Accept`s, up to `n` `Accepted`s, `n` `Commit`s.
- The leader change pays one Phase 1 for the whole suffix, plus one Phase 2 per slot in it.

## 9. In paros

| What | Where |
|---|---|
| The client entry points | `paros-core/src/node.rs`: `ColocatedNode::propose`, `propose_in`, `propose_control`, `ProposeResult` |
| A round, and its decision | `paros-core/src/node/phase2.rs`: `start_accept_round`, `on_accepted`, `try_decide`; `paros-core/src/proposer/rounds.rs`: `Rounds` |
| Learning a decision | `paros-core/src/node/learn.rs`: `on_commit`, `mark_chosen`, `advance_chosen_index` |
| The replica: chosen map, chosen index, the walk, the fold | `paros-core/src/replica.rs`: `Replica::advance`, `first_unchosen`, `chosen_gap` |
| What a slot carries | `paros-core/src/journal/command.rs`: `Command`, `Control` |
| The judge at apply | `paros-core/src/journal/state.rs` |

The rule is stated in the module docs of `proposer.rs` (volatile tallies, judged over a quorum
system), `replica.rs` (the fold is contiguous or it stops) and `node/phase2.rs` (delegation
never covers Phase-1-shaped work).

**Sim gates and oracles** (`paros-sim/src/audit/state.rs`):

- `"one ballot proposes at most one command for a slot"`,
  `"a durable accept quorum never decides two values for a slot"`,
  `"an accept at or above a decided ballot carries the decided value"`.
- `"a node's applied prefix advances one slot at a time (a forward jump only at the compaction
  floor or a snapshot install)"`.
- Gates: `"a stable leader streams several slots"`, `"a multi-slot prefix is applied"`.
- The client's history is checked for a linearization against the journal's model
  (`ClientHistory`, `paros-sim`).

**Divergences from the sources:**

- [PMS §3] and [PMMC §2] apply arbitrary deterministic commands. paros runs no user application
  (#186): every slot is folded by the one journal-control state machine, judged at apply.
- [PMMC §2] lets a replica propose its command for its lowest unused slot. In paros only the
  leader allocates slots; a client retry takes a fresh slot and is answered from the log
  (`propose` doc).
- [PMMC §4.3] co-locates leaders, replicas and acceptors and warns that a co-located leader and
  acceptor need separate ballot variables. `ColocatedNode` keeps the operating `ballot` apart
  from the acceptor's `promised`.

## 10. Pitfalls

- **Slot 0 is a real slot (#56).** A sentinel encoding of "nothing chosen" folded into slot 0,
  and a follower that missed exactly slot 0 had no way to notice. `Option<Slot>` fixed it
  (witness seed 283, commit `3a22400`).
- **The contiguous prefix stops at a hole.** A slot that nobody will ever propose again freezes
  every replica (#54). The gap fill exists for it (election card).
- **A replica must not walk past a faulty record.** A walk past the hole decided a #94 duplicate
  as its first application (seed 16921589310752617664, `replica.rs` module doc).
- **A decision after compaction.** A decision can arrive after its slot was chosen elsewhere and
  compacted away; `mark_chosen` is then a no-op below the floor (`try_decide` comments).

## 11. Relatives

- [Single-decree Paxos](single-decree-paxos.md): one instance.
- [Election, P2c merge and gap fill](election.md): the one Phase 1 for the whole suffix.
- [Persist before send](persist-before-send.md), [pipelining and resend](pipelining-and-resend.md),
  [catch-up](catch-up.md): the rest of the Kernel group.
- Proxy leaders, batchers, replica tier (planned): the same log with the leader's work split.
- Writer fence (planned): what the journal fold judges at apply.

## 12. Sources

- [PMS] *Paxos Made Simple*: `docs/references/papers/paxos-made-simple/transcript.md` §2.4
  (a distinguished proposer), §3 (one instance per command, new-leader recovery with one
  proposal number, Phase 2 is optimal).
- [PMMC] *Paxos Made Moderately Complex*:
  `docs/references/papers/paxos-made-moderately-complex/transcript.md` §2 (replicas and slots,
  R1–R4, A1–A5, C1–C2), §4.1 (first unknown slot in `p1a`), §4.3 (co-location).
- [PML] *Paxos Made Live*: `docs/references/papers/paxos-made-live/transcript.md` §4.2
  (Multi-Paxos, the master optimization, one write per instance).
- frankenpaxos Multi-Paxos core: `docs/references/frankenpaxos/03-multipaxos-core.md`.

[PMS §2.1]: ../../references/papers/paxos-made-simple/transcript.md
[PMS §2.4]: ../../references/papers/paxos-made-simple/transcript.md
[PMS §3]: ../../references/papers/paxos-made-simple/transcript.md
[PMMC §2]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PMMC §4.1]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PMMC §4.3]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[PML §4.2]: ../../references/papers/paxos-made-live/transcript.md
