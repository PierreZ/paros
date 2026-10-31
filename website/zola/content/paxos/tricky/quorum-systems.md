+++
title = "Quorum systems are one boundary"
weight = 10
+++

Paxos needs one property of its quorums: every Phase-1 quorum meets every Phase-2 quorum. A
majority has this property, and so do other shapes. paros supports three shapes, and every
tally in the core asks the same object if a set of answers is a quorum. No tally counts on its
own.

{{ scenario(name="act4/the-grid") }}

## The failure

Four acceptors, A, B, C and D. Phase 2 needs two votes and Phase 1 needs three. These quorums
meet, because 3 + 2 > 4.

Now a second tally copies the number 2 and uses it for Phase 1 too. This is an easy mistake
when each tally keeps its own threshold.

1. Leader L1, at ballot 1, chooses `v` at slot 5 with the votes of A and B.
2. Leader L2, at ballot 2, wins Phase 1 with the promises of C and D. Neither saw `v`.
3. L2 proposes `w` at slot 5, and C and D vote for it. Slot 5 now has two chosen values.

A threshold copied into each tally also drifts from the configuration in force. A
reconfiguration from a majority to a grid changes what a quorum is, and every copy must change
with it.

## The rule

- `QuorumSystem` is a value inside each configuration. It has three variants: `Majority`,
  `Flexible { q1, q2 }` and `Grid { rows, cols, acks }`.
- A grid is a set of **acceptor partitions**, one per column. A Phase-2 quorum is any `acks`
  acceptors of one column. A Phase-1 quorum is any `rows - acks + 1` acceptors of **every**
  column. `QuorumSystem::partitions(f, p)` gives `rows = 2F+1` and `acks = F+1`: a majority of
  one partition decides a write, and a majority of every partition elects a leader.
- Every tally asks `AcceptorConfig::has_phase1_quorum` or `has_phase2_quorum` (or their `_in`
  forms). This covers the proposer, the quorum reads, CheckQuorum, the GC fence and the
  matchmakers. These calls delegate to the `QuorumSystem` predicates.
- The constructor of a configuration asserts `cross_intersects` once. The only count helper,
  `counted`, is private to the membership module.

## Why the rule is enough

The safety proof of Paxos uses only the intersection of the two kinds of quorum. A majority
intersects because `2q > n`. A flexible split intersects because `q1 + q2 > n`. A grid
intersects in each column, because `(rows - acks + 1) + acks > rows`. The check runs once, when
the configuration is built, and every tally uses the same predicates. So no tally can disagree
with the check.

## The code map

| What | Where |
|---|---|
| The three shapes | `crates/lib/paros-core/src/membership/quorum_system.rs:19` `QuorumSystem` |
| The only count helper | `crates/lib/paros-core/src/membership/quorum_system.rs:106` `counted` |
| Acceptor partitions | `crates/lib/paros-core/src/membership/quorum_system.rs:219` `QuorumSystem::partitions` |
| The intersection check | `crates/lib/paros-core/src/membership/quorum_system.rs:331` `QuorumSystem::cross_intersects` |
| A configuration, checked once | `crates/lib/paros-core/src/membership/acceptor_config.rs:120` `AcceptorConfig::new` |
| The predicate every tally asks | `crates/lib/paros-core/src/membership/acceptor_config.rs:234` `has_phase1_quorum` |

## The proof

- **One value per slot.** The audit asserts that "at most one value is ever chosen for a slot"
  (`crates/sim/paros-sim/src/audit/node.rs:244`), for every shape the simulation draws.
- **No bad configuration starts.** The reconfiguration workload asserts that a configuration
  that does not admit its quorum system never starts
  (`crates/sim/paros-sim/src/workload/reconfigure.rs:561`).
- **The shapes are exercised.** `assert_sometimes!` checks a flexible election, a slot decided
  by fewer votes than a majority, a slot decided on a column, an election covered by a row, and
  a reconfiguration between a grid and a majority (`crates/sim/paros-sim/src/audit/state.rs:809`).
- **Unit tests.** `a_grid_admits_exactly_what_it_tiles` and
  `partitions_decide_on_a_majority_of_one_column`
  (`crates/lib/paros-core/src/membership/quorum_system/tests.rs`), and
  `a_flexible_policy_cross_intersects_at_every_size` (`crates/sim/paros-sim/src/shape.rs:443`).

The levels [`act1/quorum-intersection`](../../../play/#act1/quorum-intersection) and
[`act4/flexible-quorums`](../../../play/#act4/flexible-quorums) show the intersection by hand.
The field guide is [Beyond Multi-Paxos](@/paxos/beyond-multi-paxos.md#flexible-quorums).
