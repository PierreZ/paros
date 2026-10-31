# Catch-up

Group: Kernel. Status: verified 2026-10-31.

A node that missed a `Commit` (and often its `Accept`) has a hole in its chosen prefix. The
leader re-sends only rounds that are still open, so nothing would ever fill that hole. **Catch-up**
fills it: a node that learns it is behind asks a peer to replay the decided slots, and a node
behind a peer's compaction floor jumps to that floor (`Message::TrimmedTo`). In paros catch-up is
a **pull**, triggered by the leader's beat, with a push in the other direction for a leader that
lost its chosen index.

## 1. Problem

[PML §4.2]: "A catch-up mechanism lets lagging replicas catch up." A replica can lag because it
was down, partitioned, or lost a `Commit`. It cannot apply past the first slot it does not know
is chosen, so one lost message freezes its whole prefix. Two harder cases:

- The slots it needs are **compacted** on every peer. Entries cannot help; it needs the state at
  the floor ([PML §5.5], [PMMC §4.2]: a lagging replica fetches a snapshot).
- The **leader** is the one behind: its relaxed chosen index was lost in a crash, so its beat
  advertises a stale value and no follower thinks it is behind.

## 2. Model

The Multi-Paxos model ([multi-paxos-log.md](multi-paxos-log.md) §2). A chosen slot is
immutable: any holder serves the same `(ballot, command)`, so the source of a replay does not
matter.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Requester | its chosen index, its first unchosen slot | the chosen index is relaxed |
| Requester | `pull_beats`, `pull_index`: how long it has pulled at one chosen index | no |
| Server | its chosen map, its floor (`first_slot`), the journal state below its floor (`journal_base`) | the records and the floor are durable |

## 4. Normal flow

1. The leader sends a beat each `HEARTBEAT_TICKS` with its `commit`, its contiguous chosen
   index.
2. A follower compares it with its own chosen index (`on_heartbeat`,
   `paros-core/src/node/replication.rs`), in **both** directions:
   - **Behind** (`commit > ci`): `pull_gap` sends a `CatchUpRequest` from its first unchosen
     slot to the beat's sender.
   - **Ahead** (`commit < ci`): it pushes the sender what the sender lacks (`serve_catchup` from
     `commit + 1`).
3. The server answers from its own chosen prefix (`serve_catchup`,
   `paros-core/src/node/catch_up.rs`): at most `CATCHUP_BATCH` = 64 contiguous decided slots,
   each with its choosing ballot, in one `CatchUpResponse`.
4. The requester learns each entry (`on_catchup_response` → `mark_chosen`) and walks its prefix
   on. If more is missing, the next beat pulls again.

```mermaid
sequenceDiagram
    participant L as Leader (chosen to 9)
    participant F as Follower (chosen to 5)
    L->>F: Heartbeat(commit = 9)
    Note over F: commit 9 > ci 5: pull_gap
    F->>L: CatchUpRequest(from 6)
    L->>F: CatchUpResponse(6..=9, each with its ballot)
    Note over F: mark_chosen 6..9; walk the prefix
```

Catch-up is **not** gated on the beat's ballot: it learns immutable chosen history, not
leadership (the `on_heartbeat` comment).

## 5. Recovery flow

- **Below the floor.** A request below the server's floor gets `Message::TrimmedTo { point,
  state }`: the trim point and the journal state its log folded to below it, no bytes and **no
  ballot**. The requester jumps (`on_trimmed_to`): it raises its floor to `point`, marks
  everything below chosen, takes the state if its own fold is behind. The walk and the next
  catch-up continue from `point`. **The promise does not move** (#180): a trim point says nothing about a ballot.
- **A stalled pull.** If the pulls at one chosen index outlast one election timeout (at least
  `PULL_FALLBACK_BEATS` = 3 beats), the follower broadcasts the request to the whole pool, and any
  holder serves it (#641, commit `c1df74f`).
- **A leader with a stale chosen index.** A follower that is ahead replays the prefix to the
  leader. The leader then advertises the true prefix, and the followers that are behind pull.
- **No leader.** A node whose election timer fires broadcasts a `CatchUpRequest` next to its
  `Prepare`s (`start_phase1`, `paros-core/src/node/election.rs`). It heals even if the election
  is lost.
- **A server with a hole.** A server whose own chosen record is faulty stops its page **at** the
  hole, never past it: a page with a silent gap would claim a range it does not cover. Another
  peer, or a trim point, serves past it.

## 6. Why it is safe

Every replayed entry is chosen, so it is durable on a Phase-2 quorum and it is the only value
that slot can ever have. Learning it from any holder is the same as learning it from a `Commit`
(the `serve_catchup` doc). The server sends only its contiguous chosen prefix, from at or above
its floor, and asserts it (`"catch-up is served from inside the chosen prefix"`, `"a catch-up
page is contiguous"`).

A trim-point jump is safe because everything below a floor is chosen and compacted on the node
that sent it. The jump moves the floor and the chosen index, never the promise, so the node's
Phase-1 answers stay what they were (`"a trim-point jump never moves the promise"`).

## 7. Liveness

- A follower behind a live leader catches up within a beat or two per page.
- A follower whose path to the leader is dead still catches up from any peer after the
  fallback (#641).
- A leader behind its followers catches up through the push.
- With no leader, the election timer's broadcast is the repair.

## 8. Costs

- One request and one response per page of 64 slots; a backlog of `k` slots takes `⌈k/64⌉`
  beats.
- A trim-point jump costs one message and no bytes: the journal state is a small fold (paros
  runs no application, #186), not a snapshot of user data.
- The per-beat cadence rate-limits the pulls and heals a lost request on its own.

## 9. In paros

| What | Where |
|---|---|
| The beat's reconciliation, the pull and its fallback | `paros-core/src/node/replication.rs`: `on_heartbeat`, `pull_gap` |
| Serving, learning, the jump | `paros-core/src/node/catch_up.rs`: `CATCHUP_BATCH`, `on_catchup_request`, `serve_catchup`, `on_catchup_response`, `on_trimmed_to` |
| The fallback bound | `paros-core/src/node.rs`: `PULL_FALLBACK_BEATS` |
| The election's broadcast | `paros-core/src/node/election.rs`: `start_phase1` |
| The messages | `Message::CatchUpRequest`, `CatchUpResponse`, `TrimmedTo` |

The rule is stated in the doc comments of `serve_catchup`, `on_trimmed_to` and `pull_gap`,
and in the comments of `on_heartbeat`.

**Core probes:**

- `"catch-up: a follower replays the decided prefix to a leader that advertises a lower commit"`.
- `"catch-up: a stalled pull asks the whole pool"`.
- `"catch-up: a replay stops at the server's own faulty chosen record"`.
- `"catch-up: a trim-point jump resolves a repair probe's blocked slot"`.

**Sim gates and oracles:**

- `"a lagging node converges via catch-up"` (`paros-sim/src/audit/world.rs`).
- `"the driver drops a catch-up request at the send seam"` (`paros-sim/src/audit/net.rs`).
- `"a node's applied prefix advances one slot at a time (a forward jump only at the compaction
  floor or a snapshot install)"` (`audit/state.rs`): a jump is legal only at a floor.

**Divergences from the sources:**

- etcd-raft's leader **probes** each follower and pushes appends ([etcd-raft replication §2]).
  paros's follower **pulls**, because paros tracks slots, not followers.
- [PML §5.5] and [PMMC §4.2] install an application snapshot. paros runs no application, so a
  `TrimmedTo` carries only the floor and the journal state.
- `TrimmedTo` carries no ballot, so a jump cannot move a promise (#180).

## 10. Pitfalls

- **A missed slot 0 (#56).** "Nothing chosen" and "slot 0 chosen" were one wire value, so a
  follower that missed exactly slot 0 never pulled. `Option<Slot>` fixed it: `None` orders below
  `Some(Slot(0))` (the `on_heartbeat` comment; witness seed 283, commit `3a22400`).
- **A pull to a dead address (#641).** After a machine rename, a lagging founder's lane to the
  leader dialed a dead address, every pull went there, and the settle-tail prefix check failed.
  The pool fallback fixed it. Red to green: seed 9185416370593795425, red on main `7cc3bbf`.
- **An off-by-one at the floor.** Serving a push from `commit` instead of `commit + 1` wasted an
  entry and, at the floor boundary, turned a one-slot-behind peer into a trim-point jump (the
  `on_heartbeat` comment).
- **A page with a silent gap.** A server that skipped its own faulty record would make the
  requester's walk stall on a range the reply claimed to cover. A page stops at the hole.

## 11. Relatives

- [Multi-Paxos log](multi-paxos-log.md): the chosen prefix that catch-up extends.
- [Pipelining and resend](pipelining-and-resend.md): the repair for open rounds; catch-up is the
  repair for closed ones.
- [Persist before send](persist-before-send.md): why the chosen index can be relaxed.
- Truncation and the trim-point jump (planned, Storage group): how a floor moves.

## 12. Sources

- [PML] *Paxos Made Live*: `docs/references/papers/paxos-made-live/transcript.md` §4.2 (the
  catch-up mechanism), §5.5 (catch-up via snapshot).
- [PMMC] *Paxos Made Moderately Complex*:
  `docs/references/papers/paxos-made-moderately-complex/transcript.md` §4.2 (garbage collection;
  a lagging replica fetches a snapshot).
- [etcd-raft replication §2, §4]: `docs/references/etcd-raft/replication.md` §2 (progress
  states), §4 (the snapshot fallback).

[PML §4.2]: ../../references/papers/paxos-made-live/transcript.md
[PML §5.5]: ../../references/papers/paxos-made-live/transcript.md
[PMMC §4.2]: ../../references/papers/paxos-made-moderately-complex/transcript.md
[etcd-raft replication §2]: ../../references/etcd-raft/replication.md
[etcd-raft replication §2, §4]: ../../references/etcd-raft/replication.md
