# Pipelining and resend

Group: Kernel. Status: verified 2026-10-31.

A stable leader does not wait for slot `i` to be chosen before it proposes slot `i+1`: it keeps
several Phase-2 rounds open at once (**pipelining**). A lost `Accept` leaves a round open with
no quorum, so the leader **re-sends** the open rounds on each beat. In paros the re-send is a
method the driver calls, `ColocatedNode::resend_pending`, and skipping a call is always safe.

## 1. Problem

One round trip per command, in series, caps throughput at one command per round trip. Pipelining
lifts the cap, at two costs:

- A message loss leaves a round open while later rounds close. Someone must re-send it, or it
  stays undecided.
- A leader crash can leave **gaps**: slots below the leader's frontier that no quorum accepted.
  [PMS §3]: a leader `α` commands ahead can leave a gap of up to `α−1` commands.

## 2. Model

The Multi-Paxos model ([multi-paxos-log.md](multi-paxos-log.md) §2). Messages are lost,
duplicated and reordered. A round is identified by `(slot, ballot)` and carries one command.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Leader (`Rounds`, `proposer/rounds.rs`) | one open `Round` per in-flight slot: ballot, command, the accept tally, the column (grid), the proxy (if delegated), a re-send count | no |
| Leader | a fair re-send cursor (`resend_cursor`) | no |
| Acceptor | its accepted record per slot | yes |

paros tracks **slots**, not followers. etcd-raft keeps one `Progress` per follower with an
inflight window ([etcd-raft replication §2–§3]); paros has no per-follower window. A round
closes when a Phase-2 quorum accepts it, whoever is in that quorum.

## 4. Normal flow

1. The leader opens a round for each new command at `next_slot` and sends its `Accept`s at
   once (`start_accept_round`). It does not wait for earlier rounds.
2. The rounds close in any order, each with its own Phase-2 quorum (`try_decide`).
3. On each beat, the driver calls `tick`, then `resend_pending` when `has_pending_accepts` is
   true. `resend_pending` asks `Proposer::resend_page` for a fair bounded page of open rounds
   and sends each `Accept` again, at the round's own ballot, to the same column and the same
   proxy.
4. An acceptor that already accepted the same `(slot, ballot, command)` accepts it again and
   answers again: Phase 2b is idempotent.

```mermaid
sequenceDiagram
    participant L as Leader
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    L->>A1: Accept(b, 7, w7)
    L->>A2: Accept(b, 7, w7)
    L-xA1: Accept(b, 8, w8) (lost)
    L-xA2: Accept(b, 8, w8) (lost)
    A1->>L: Accepted(b, 7)
    Note over L: slot 7 chosen with {L, A1}; slot 8 open
    Note over L: beat: resend_pending
    L->>A1: Accept(b, 8, w8)
    L->>A2: Accept(b, 8, w8)
    A2->>L: Accepted(b, 8)
    Note over L: slot 8 chosen with {L, A2}
```

The page is fair: `resend_page` starts after the last slot of the previous page and wraps
around, so a large backlog does not starve its highest rounds (probe `"phase2: a re-send page
wraps around past the highest open round"`).

## 5. Recovery flow

- **A lost `Accept` or `Accepted`.** The next beat re-sends the round. If the leader loses its
  leadership first, the next leader's Phase 1 recovers the slot: it re-proposes what a quorum
  reports and fills the rest with `Noop` (election card).
- **A delegated round** (proxy leaders, #142). The re-send goes to the proxy the round was
  handed to, and counts. After the driver's budget of re-delegations without a `Commit`,
  `take_back_delegated` runs the round colocated. Taking a round back early is safe too: two
  fan-outs of one `(slot, ballot, command)` can only agree (the `take_back_delegated` doc).
- **A leader crash with a pipeline open.** Slots below the frontier can be undecided while a
  higher slot is decided. That is the #54 hole: the contiguous prefix stops there until the next
  leader fills it.

## 6. Why it is safe

Each slot is its own single-decree instance, so pipelining changes nothing per slot. A re-send
carries the round's own `(slot, ballot, command)`: `resend_pending` asserts `"a re-sent Accept
runs at the leader's ballot"`, and `Proposer::resend_page` asserts `"a re-send page carries open
rounds at their ballots"`. One ballot proposes one command per slot ([PMMC §2] **C1**), so a
re-send never carries a second value.

Skipping a re-send is safe because "a round that never gathers a quorum is simply *undecided*,
which is a state the protocol is built to survive" (the `resend_pending` doc).

## 7. Liveness

- The re-send is the leader's only repair for a lost Phase-2 message. Without it, a lost
  `Accept` waits for the next election.
- The gap fill ([PMS §3]: fill gaps with no-op) is the liveness repair for the leader that
  crashed with a pipeline open. Without it, a hole freezes the chosen prefix on every replica.
- A dead proxy is the leader's matter: `take_back_delegated` falls back to colocated Phase 2.

## 8. Costs

- Throughput: the leader opens a new round without waiting for the last one to close, so a
  round trip no longer bounds it.
- A re-send page holds at most `Limits::resend_page` rounds, at most `RESEND_BATCH` = 64
  (`paros-core/src/contract/limits.rs`). The driver sets it from `DriverTunables::resend_page`.
- Re-sends cost bandwidth only while rounds are open; `has_pending_accepts` gates the call.

## 9. In paros

| What | Where |
|---|---|
| The re-send and the take-back | `paros-core/src/node/policy.rs`: `ColocatedNode::resend_pending`, `take_back_delegated` |
| The open rounds and the fair page | `paros-core/src/proposer/rounds.rs`: `Rounds::resend_page`, `Proposer::resend_page` |
| The gate the driver reads | `paros-core/src/node/view.rs`: `has_pending_accepts` |
| The page ceiling | `paros-core/src/contract/limits.rs`: `RESEND_BATCH`, `Limits::resend_page` |
| The driver's call | `paros-roles/src/node/shared.rs` (the beat) |

The rule is stated in the `resend_pending` doc: a driver-policy method with an honest contract,
not something `tick` does, because skipping it is what makes the #54 hole reachable.

**Sim gates and oracles:**

- The driver skips a re-send on an inline `buggify_fault_with_prob!(0.95)` site, silent in the
  recovery tail, with the gate `"the driver skips a pending accept re-send"`
  (`paros-roles/src/node/shared.rs`).
- Knob gates: `"a node runs with a small re-send page"`, `"a node runs an extreme internal
  re-send policy"` (`paros-sim/src/shape/node.rs`).
- The Phase-2 oracles of the Multi-Paxos card: `"one ballot proposes at most one command for a
  slot"`, `"a durable accept quorum never decides two values for a slot"`.

**Divergences from the sources:**

- [PMS §3] bounds the pipeline by `α`, because the server set of instance `i+α` is fixed by
  command `i`. paros has no `α`: a reconfiguration is a round change (reconfiguration cards,
  planned), so the core does not bound how many rounds are open. The client's pipeline and the
  batcher bound it.
- etcd-raft bounds a per-follower window by count and bytes ([etcd-raft replication §3]). paros
  bounds each page, not each follower.

## 10. Pitfalls

- **The #54 hole.** A skipped re-send plus a leader crash leaves an undecided slot below a
  decided one. No partition produces it alone ("a partition takes slots away in contiguous
  runs", the `resend_pending` doc). The gap fill closes it, and the sweep reaches it because the
  driver's skip site, once active on a seed, skips each re-send with probability 0.95.
- **A re-send that widens.** A grid round re-sent to another column would change its quorum. The
  re-send keeps the round's own column and proxy.
- **An unfair page.** A page that always starts at the lowest slot would starve the high rounds
  of a large backlog; the cursor and the wrap avoid it.

## 11. Relatives

- [Multi-Paxos log](multi-paxos-log.md): the rounds that pipelining keeps open.
- [Election, P2c merge and gap fill](election.md): the repair for a pipeline a crash left open.
- [Catch-up](catch-up.md): the repair for a `Commit` lost after a round closed.
- Proxy leaders (planned): delegated rounds and the take-back.

## 12. Sources

- [PMS] *Paxos Made Simple*: `docs/references/papers/paxos-made-simple/transcript.md` §3
  (pipelining by `α`, gaps of up to `α−1`, no-op fill).
- [PMMC] *Paxos Made Moderately Complex*:
  `docs/references/papers/paxos-made-moderately-complex/transcript.md` §2 (commanders, C1).
- [etcd-raft replication §2–§3]: `docs/references/etcd-raft/replication.md` §2 (progress
  states), §3 (the inflight window).

[PMS §3]: ../../references/papers/paxos-made-simple/transcript.md
[PMMC §2]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[etcd-raft replication §2–§3]: ../../references/etcd-raft/replication.md
[etcd-raft replication §3]: ../../references/etcd-raft/replication.md
