+++
title = "One proposer per slot and ballot"
weight = 3
+++

Paxos assumes that one ballot proposes at most one value for a slot. Two commands under one
slot and one ballot break the proof: an acceptor that votes for one cannot tell it from the
other. A leader hands out slots from an **allocator frontier**, and paros makes that frontier
survive a crash and a handoff.

## The failure

The proxy model checker found this trace (seed 756):

1. Successor S inherits ballot `b` by a handoff. It opens a delegated round at slot 7 and keeps
   no record of the round itself.
2. S crashes and reboots. It derives its next slot from its own accepted log, so the frontier
   goes back to 7.
3. A duplicated `Relinquish` installs the authority again. S hands out slot 7 again, at ballot
   `b`, with a different command.
4. Two commands now exist at slot 7 and ballot `b`. "The same ballot means the same value" is
   false, and P2c can pick the wrong one.

## The rule

- `allocate` hands out `next_slot` and moves it forward. `set_next_slot` installs the frontier
  when a leadership starts: the highest slot reported plus one after an election, or the value
  carried by a handoff.
- The leader records each round it opens in its own accepted log first, with
  `record_own_round`. This covers colocated, delegated and out-of-column rounds. The record is
  in the same batch as the Accepts, and persist-before-send makes it durable before they leave.
- So the frontier that a reboot derives is the real frontier.
- If the promise already moved past the ballot, nothing is recorded. The round cannot be
  chosen, and the leader steps down on the `Nack`.
- `on_relinquish` refuses a handoff that would move the allocator back.

## Why the rule is enough

A ballot has one owner. Inside that ballot, a slot is handed out once, because the frontier
only moves forward, across crashes and across handoffs.

## The code map

| What | Where |
|---|---|
| The frontier | `crates/lib/paros-core/src/proposer/rounds.rs:718` `next_slot` |
| Hand out a slot | `crates/lib/paros-core/src/proposer/rounds.rs:728` `allocate` |
| Install a frontier | `crates/lib/paros-core/src/proposer/rounds.rs:746` `set_next_slot` |
| Open a round | `crates/lib/paros-core/src/node/phase2.rs:139` `start_accept_round_in` |
| Make the frontier durable | `crates/lib/paros-core/src/node/phase2.rs:254` `record_own_round` |
| Refuse a rewind on handoff | `crates/lib/paros-core/src/node/handoff.rs:507` `on_relinquish` |

## The proof

- **One command per slot and ballot.** The audit asserts that "one ballot proposes at most one
  command for a slot" (`crates/sim/paros-sim/src/audit/state.rs:718`) and that "at most one
  command is ever accepted for one (slot, ballot)" (`crates/sim/paros-sim/src/audit/state.rs:1129`).
- **No rewind.** The audit asserts that "an inherited allocator frontier never rewinds"
  (`crates/sim/paros-sim/src/audit/node.rs:634`) and that "a transferred allocator frontier
  never rewinds" (`crates/sim/paros-sim/src/audit/state.rs:1397`).
- **The model checker.** The proxy model (`crates/lib/paros-core/src/models/proxy.rs`) explores
  crashes and duplicated messages around delegated rounds. It found the trace above.
- **Unit tests.** `a_leader_trailing_its_own_frontier_past_the_bound_is_refused`
  (`crates/lib/paros-core/src/node/tests/handoff.rs:430`) and
  `a_stale_trim_point_never_rewinds_a_frontier`
  (`crates/lib/paros-core/src/node/tests/invariants.rs:209`).

The level [`act4/the-handoff`](../../../play/#act4/the-handoff) shows a handoff, and a second
handoff that the core refuses.
