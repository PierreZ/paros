# Truncation, compaction floor and trim-point jump

Group: Storage. Status: verified 2026-10-31.

A log that only grows needs unbounded disk and unbounded recovery time ([PML §5.5]). paros keeps
it finite in three steps. **Truncation** is a Paxos-decided control command, `Truncate`, judged
at apply and, on a single-writer journal, fenced by the leader uuid like a `Write`: it raises
the journal's `first_seq`. Each
node then **compacts** lazily when its apply walk reaches that slot, and raises its **compaction
floor**. A node whose catch-up asks below a peer's floor gets a **trim-point jump**
(`Message::TrimmedTo`): the floor and the journal state, no bytes and **no ballot**.

## 1. Problem

- In [PML §5.5] the application takes snapshots, and the framework truncates the log behind them.
  Each replica decides alone, so the snapshots differ. A laggard that cannot get old records
  fetches a snapshot from another replica.
- [PAR §3.5.1] shows a better shape: the leader puts a `gc` marker in the log, and every node
  collects up to the same index when it applies the marker.
- paros runs no application and takes no snapshot (#186). Record bytes are opaque. So the only
  question is where a journal's retained history starts, and every node must agree on it.

## 2. Model

- The Multi-Paxos log ([multi-paxos-log.md](multi-paxos-log.md) §2), plus the journal state
  machine judged at apply: `first_seq`, `next_seq`, the leader uuid and a term
  (`journal/state.rs`).
- A floor is per node: below it, every slot is chosen and its record is gone **on this node**.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Journal fold (`JournalState`) | `first_seq`, the first retained position | the sealed state rides `WriteOp::Truncate` and `TrimmedTo` |
| Replica | the compaction target: the highest slot a decided truncation lets it drop, kept below every record a retained retry still reads | no, recomputed |
| Acceptor | the compaction floor (`first_slot`), the records and faulty entries above it | yes, the floor in the same commit as the sealed state |
| Checkpoint owner (`paros::client::checkpoint`) | the checkpoint it wrote, the position to truncate to | in the journal it checkpoints |

## 4. Normal flow

1. The writer (or a checkpoint owner, #230) sends `Truncate(leader_uuid, up_to_seq)`.
2. The leader proposes it in a slot, like any command.
3. When the apply walk reaches that slot, the journal state machine judges it. On a
   single-writer journal it is accepted iff the uuid is the current leader; a stale or foreign
   caller is refused in place (`Outcome::TruncateRefused`). On a multi-writer journal it carries
   the unset uuid, and anyone may send it (`WriterMode::Multi`). Accepted, `first_seq` rises to
   `up_to_seq`, clamped to `next_seq`, and never goes down (`Outcome::Trimmed`).
4. After the walk, the node compacts: it drops every record up to the compaction target and
   emits `WriteOp::Truncate` with the sealed journal state, ordered after the walk's
   `SetChosenIndex` writes (`ColocatedNode::advance_chosen_index`).
5. Every node does steps 3 and 4 when its own walk reaches the slot, so every floor lands on the
   same decided point.

```mermaid
sequenceDiagram
    participant W as Writer
    participant L as Leader
    participant A as Acceptors
    participant N as Laggard
    W->>L: Truncate(uuid, up_to 500)
    L->>A: Accept(b, 90, Truncate)
    A->>L: Accepted
    Note over L,A: slot 90 applied: first_seq = 500; each node compacts below it
    N->>A: CatchUpRequest(from slot 12)
    A->>N: TrimmedTo(point, journal state)
    Note over N: floor = point; promise unchanged
    N->>A: CatchUpRequest(from point)
```

## 5. Recovery flow

- **A laggard below a peer's floor.** The peer answers `TrimmedTo { point, state }`. The laggard
  drops what it holds below `point`, moves its chosen index to at least `point − 1`, raises its
  floor, takes the journal state, and catches up from there (`on_trimmed_to`). A point at or below
  its own floor teaches nothing.
- **A `Prepare` below the floor.** The acceptor refuses it with `PrepareOutcome::BelowFloor`,
  without moving its promise: those slots are chosen and gone, so no promise could report them.
- **A reboot.** The boot scan reads the floor and the sealed state; a truncated record is never
  recovered.
- **An interrupted checkpoint.** An owner that stops between its checkpoint and its truncate
  leaves the checkpoint mid-log and the floor where it was; a later owner's own run truncates
  (`"checkpoint: an owner stops between its checkpoint and its truncate"`,
  `workload/system.rs`). A superseded owner's truncate is refused by the fence.

## 6. Why it is safe

- **Only chosen slots go.** The floor moves only inside the chosen prefix ("a walk's compaction
  never outruns the chosen prefix"), so a compacted slot can never be needed by a Phase 1: any
  candidate's Phase-1 quorum meets a member that refuses a `Prepare` below its floor, which
  tells the candidate those slots are already chosen (the `node/gc.rs` module doc).
- **The jump moves no promise** (#180). A trim point says nothing about a ballot, so a node that
  jumps keeps every promise it made, and a candidate still collects promises by Phase 1. The jump
  only raises the floor and the chosen index, which is the same fact the peer already holds.
- **One decision for every node.** Because truncation is a decided command judged at apply, every
  node computes the same `first_seq` at the same slot. The floor itself may lag per node; the
  journal's view does not.
- **The fence.** On a single-writer journal, a truncate under a superseded leader uuid is
  refused, so nobody who holds the tenant can truncate to a position that is not the current
  writer's checkpoint (#228). A multi-writer journal has no fence: anyone may truncate it.

The compaction floor and the GC watermark never wait on each other: one is per node and says
"these slots are gone here"; the other is per matchmaker and says "these configurations are gone"
([configuration GC](configuration-gc.md)).

## 7. Liveness

- A truncation commits like a write: one slot, one Phase 2.
- Compaction is lazy, so it never blocks a walk; [Ceph §6] does the same, queuing the trim with
  the next transaction.
- A laggard heals in one round trip plus catch-up of the retained suffix.

## 8. Costs

- A `Truncate` consumes a slot and no position: positions stay dense (`journal/state.rs`).
- A trim-point jump costs one message and no bytes: the journal state is four scalars.
- The floor keeps every record a retained retry still reads, so it may sit below `first_seq`'s
  slot.

## 9. In paros

| What | Where |
|---|---|
| The judgment at apply | `paros-core/src/journal/state.rs`: `Control::Truncate`, `Outcome::Trimmed`, `Outcome::TruncateRefused` |
| The lazy compaction | `paros-core/src/node/learn.rs`: `advance_chosen_index`; `paros-core/src/replica.rs`: `compaction_target` |
| The floor ops | `paros-core/src/acceptor/retention.rs`: `Acceptor::truncate`, `Acceptor::trim_to` |
| The jump | `paros-core/src/node/catch_up.rs`: `serve_catchup`, `on_trimmed_to` |
| The client | `paros-client/src/lib.rs`: `truncate`; `paros-client/src/checkpoint.rs`: `Checkpointer`, `truncate_to` |
| The decision | `docs/architecture/02-data-plane.md` §2 |

**Sim gates and oracles** (`paros-sim/src/audit/node.rs` unless named):

- `"a node truncates its log prefix behind the chosen index"`, `"a compaction floor never
  regresses"`, `"a compaction floor never passes the quorum-decided frontier"`.
- `"a truncated record is never recovered on boot (the log stays bounded)"`.
- `"a candidate prepares below a peer's compaction floor"`; `"a below-floor node recovers by
  jumping to a peer's trim point"` (`audit/state.rs`).
- `"chain: a superseded owner sends a stale truncate"`, `"chain: a truncation never lowers the
  journal's floor"` (`workload/truncate.rs`); `"checkpoint: a superseded owner's truncate is never
  applied"` (`workload/system.rs`).
- The split-floor scenario: acceptors whose floor passed a slot the others hold go down, then the
  others (`paros_sim::world::scenarios::split_floor`, #409).

**Divergences from the sources:**

- [PML §5.5] lets each replica snapshot and truncate alone. paros truncates by a decided command,
  as [PAR §3.5.1]'s `gc` marker does, and keeps no snapshot.
- [PML §5.5] and [Ceph §6] heal a laggard with a snapshot or the missing values. paros sends a
  floor and four scalars, because the records below it are opaque and gone.
- [Ceph §6] trims a prefix and records the new floor, `first_committed`. paros trims only where a
  decided `Truncate` asked.

## 10. Pitfalls

- **A floor that drops a record a retry reads.** Dropping only up to the first retained record
  left a rebooted node nothing to compare a retained retry against: it folded `Refused` where
  every other node folded `Duplicate` ("every node judges a slot to the same outcome", witness
  9212868850674249062, #204; the `compaction_target` doc).
- **A jump that moves a promise.** A trim point is not a ballot. `TrimmedTo` carries none, so the
  jump can only move the floor and the chosen index, never the promise (#180's rule, pinned by the
  unit test `a_trim_point_jump_moves_the_prefix_and_never_the_promise`).
- **An unfenced truncate.** Before #228, anyone holding the tenant could truncate to a position
  that was not the leader's checkpoint.

## 11. Relatives

- [Catch-up](catch-up.md): the pull that meets a floor.
- [Corrupted storage recovery](corrupted-storage-recovery.md): nothing below the floor is faulty.
- [Boot safety](boot-safety.md): a wiped node is not a laggard.
- [Configuration GC and retirement](configuration-gc.md): the other floor.

## 12. Sources

- [PML] Chandra, Griesemer, Redstone, *Paxos Made Live*:
  `docs/references/papers/paxos-made-live/transcript.md` §5.5 (snapshots).
- [PAR] Alagappan et al., *Protocol-Aware Recovery for Consensus-Based Distributed Storage*:
  `docs/references/papers/protocol-aware-recovery/transcript.md` §3.5.1 (leader-initiated
  snapshots and the `gc` marker).
- [Ceph] `docs/references/ceph/mon-paxos-patterns.md` §6 (state sharing and trim).

[PML]: ../../references/papers/paxos-made-live/transcript.md
[PML §5.5]: ../../references/papers/paxos-made-live/transcript.md
[PAR]: ../../references/papers/protocol-aware-recovery/transcript.md
[PAR §3.5.1]: ../../references/papers/protocol-aware-recovery/transcript.md
[Ceph]: ../../references/ceph/mon-paxos-patterns.md
[Ceph §6]: ../../references/ceph/mon-paxos-patterns.md
