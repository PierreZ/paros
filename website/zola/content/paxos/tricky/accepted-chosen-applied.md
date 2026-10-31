+++
title = "Accepted, chosen and applied"
weight = 4
+++

A slot goes through three states, and they are facts about different things. **Accepted** is a
fact about one acceptor: it voted. **Chosen** is a fact about the cell: a quorum voted for one
value. **Applied** is a fact about one node: its state machine ran the command. paros keeps the
three apart, and it applies only along a contiguous prefix.

{{ scenario(name="act2/a-log-of-decisions") }}

## The failure

**Apply on decide.** The leader pipelines, so slot 6 is chosen before slot 5. A node that
applies a slot when it learns that the slot is chosen runs 6, then 5. A peer runs 5, then 6.
The two replicas now hold different states.

**Accepted taken as chosen.** A node accepted slot 5 and serves it as if it were chosen. A
later leader re-proposes another value at slot 5 under a higher ballot, and that value is
chosen. The node served a value that the log never held.

## The rule

- *Accepted* is a record in an acceptor.
- *Chosen* is learned from a `Commit` (`on_commit`) or by the leader's own tally. `mark_chosen`
  records it, in any order.
- *Applied* happens only in `Replica::advance`. The walk moves the chosen index one slot at a
  time from the first slot not chosen (`first_unchosen`), in bounded pages. It stops at a slot
  in the prefix whose value this node does not have (`fold_hole`).
- `chosen_gap` reports the first missing slot and the highest chosen slot above it. It only
  observes. A gap is normal for a short time. A gap that survives when the cell is quiet is the
  wedge that [the gap fill](@/paxos/tricky/recovery-and-gap-fill.md) prevents.

## Why the rule is enough

Every node applies the same contiguous prefix, in slot order, so every node runs the same
sequence of commands. A choice is permanent and per slot, so a node can learn choices in any
order and stay safe.

## The code map

| What | Where |
|---|---|
| Learn a choice from a Commit | `crates/lib/paros-core/src/node/learn.rs:22` `on_commit` |
| Record a choice, in any order | `crates/lib/paros-core/src/node/learn.rs:57` `mark_chosen` |
| The first slot not chosen | `crates/lib/paros-core/src/replica.rs:400` `first_unchosen` |
| The hole that stops the walk | `crates/lib/paros-core/src/replica.rs:518` `fold_hole` |
| Report a gap | `crates/lib/paros-core/src/replica.rs:607` `chosen_gap` |
| The apply walk | `crates/lib/paros-core/src/replica.rs:1011` `advance` |

## The proof

- **Applied matches chosen.** The audit asserts that "an applied value matches the decided
  value" (`crates/sim/paros-sim/src/audit/node.rs:256`), and that "an applied slot was decided
  by a durable accept quorum before any node applied it" (`crates/sim/paros-sim/src/audit/node.rs:273`).
- **One slot at a time.** The audit asserts that "a node's applied prefix advances one slot at a
  time" (`crates/sim/paros-sim/src/audit/state.rs:1606`).
- **Unit tests.** `chosen_index_advances_only_over_contiguous_prefix`
  (`crates/lib/paros-core/src/node/tests/decide_apply.rs:46`),
  `a_faulty_chosen_record_holds_the_walk_until_the_fold_heals`
  (`crates/lib/paros-core/src/node/tests/recovery.rs:300`) and
  `follower_fills_a_hole_via_commit_replay_catch_up`
  (`crates/lib/paros-core/src/node/tests/catch_up.rs:94`).

The level [`act3/chosen-is-not-applied`](../../../play/#act3/chosen-is-not-applied) asks a
client retry about a slot that is chosen above a hole. The field guide is
[From one value to a log](@/paxos/replicated-log.md).
