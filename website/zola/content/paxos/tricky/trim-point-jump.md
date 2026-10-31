+++
title = "Truncation, the floor and the trim-point jump"
weight = 8
+++

A log cannot grow forever. A node must drop old slots, but a dropped slot must never look like
a slot that nobody voted in. paros decides each truncation through the log, so every node drops
the same prefix. A node that fell below that prefix jumps to the trim point, and the jump never
moves its promise.

{{ scenario(name="act3/the-stranded-node") }}

## The failure

**Each node prunes on its own.** A2 drops slots 0 to 9, and A1 does not. A slow proposer sends
Prepare from slot 5. A2 answers "nothing accepted" for slot 5, and A3 did not vote there. The
proposer counts A2 and A3 as a quorum and fills slot 5 with a `Noop`, over a chosen value.

**A stranded node.** n2 is down while the cell truncates past its log. When n2 restarts, no
peer can replay slots 0 to 9. Catch-up stalls forever.

**A jump that moves the promise.** If a catch-up jump also moved the promise of n2, the voting
state of n2 would be wrong, and a stale leader could win a vote it must lose.

## The rule

- A truncation is a command decided through the log, `Control::Truncate`, fenced by the leader
  uuid like a write.
- When the in-order apply walk reaches it, `ColocatedNode::compact` raises the floor. The floor
  never goes past the journal's compaction target, which is inside the chosen prefix. The node
  writes a synced `WriteOp::Truncate` that carries the sealed journal state.
- `RetainedWindow` is a map with a floor under it. The floor never moves back, nothing stays
  below it, and a query below it is refused.
- A Prepare or an Accept below the floor gets `BelowFloor`, and the promise does not change.
- A catch-up request below the peer's floor gets `Message::TrimmedTo { point, state }`. The
  receiver raises its floor and its chosen index to the point, seals the state, writes
  `WriteOp::TrimmedTo`, and asserts that its promise did not move. Then it catches up from the
  point.

## Why the rule is enough

A node drops only slots that are chosen and applied, and every node drops the same prefix,
because the prefix is decided. So "below the floor" always means "chosen", and never
"unknown". The jump carries no ballot. The promises stay monotonic, and a candidate must still
win Phase 1.

## The code map

| What | Where |
|---|---|
| The decided truncation | `crates/lib/paros-core/src/journal/command.rs:56` `Control::Truncate` |
| Raise the floor at apply | `crates/lib/paros-core/src/node.rs:753` `ColocatedNode::compact` |
| A map with a floor | `crates/lib/paros-core/src/retained.rs:25` `RetainedWindow` |
| The acceptor's two floor moves | `crates/lib/paros-core/src/acceptor/retention.rs:30` `Acceptor::truncate`, `:54` `Acceptor::trim_to` |
| Answer below the floor with the trim point | `crates/lib/paros-core/src/node/catch_up.rs:38` `serve_catchup` |
| Take the jump | `crates/lib/paros-core/src/node/catch_up.rs:169` `on_trimmed_to` |

## The proof

All the oracles are in `crates/sim/paros-sim/src/audit/`.

- "a compaction floor never regresses" (`node.rs:154`) and "a compaction floor never passes the
  quorum-decided frontier" (`node.rs:165`).
- "a node never persists an accept below its compaction floor" (`node.rs:67`).
- "a truncated record is never recovered on boot" (`node.rs:1198`).
- The applied prefix advances one slot at a time, with a forward jump only at the compaction
  floor or a snapshot install (`state.rs:1608`).
- The simulation reaches a candidate that prepares below a peer's floor (`node.rs:1488`).
- Unit tests in `crates/lib/paros-core/src/node/tests/catch_up.rs`:
  `below_floor_catchup_request_is_answered_with_the_trim_point` and
  `a_trim_point_jump_moves_the_prefix_and_never_the_promise`.

The level [`act3/truncate-by-consensus`](../../../play/#act3/truncate-by-consensus) shows the
decided floor. The field guide is
[Truncation and the trim point](@/paxos/truncation-and-snapshots.md).
