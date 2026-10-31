+++
title = "Recovery and the gap fill"
weight = 2
+++

A new leader inherits the slots that the old leader left half done. Before it proposes anything
new, it must finish them: it re-proposes every value that Phase 1 reported, and it fills every
slot that nobody reported with a `Noop`. A leader that got its authority by a handoff, without
a Phase 1, must not fill a hole.

{{ scenario(name="act2/the-permanent-gap") }}

## The failure

1. Leader A sends Accept for slot 0 and slot 1. The Accepts for slot 0 reach only A. Slot 1
   reaches a quorum and is chosen.
2. A crashes.
3. B wins Phase 1 with the promises of B and C. They report only slot 1, so B starts at slot 2.
4. Nobody proposes slot 0 again. Every node applies in order, so every node stops before slot
   0, while slots above it are still chosen. Catch-up cannot help: no node has a value for
   slot 0.

The cell is wedged forever.

The handoff variant: a successor that ran no Phase 1 fills a slot it knows nothing about with a
`Noop`. A value may have been chosen there under an older ballot, and the `Noop` overwrites it.

## The rule

- When a leader wins, it opens a bounded `Recovery` over the slots from the first slot not
  chosen up to its next slot.
- For a reported slot, `recovery_next` gives `Recovered(cmd)`. The leader re-proposes that
  command at its new ballot.
- For a slot that nobody reported, the answer depends on the `RecoveryPolicy`:
  - `Phase1Backed` (an election) gives `Fill`, and the leader proposes `Control::Noop`.
  - `Inherited` (a handoff install) gives `Undescribed`, and the leader skips the slot.
- The leader starts at most `recovery_page` rounds per `Ready` batch (the ceiling is
  `RECOVERY_BATCH`, 64). A slot blocked by a faulty answer goes to the
  [repair probe](@/paxos/tricky/faulty-records.md).

## Why the rule is enough

After an election, a value chosen at a slot that nobody reported would have been reported:
quorum intersection guarantees it, and an acceptor that truncated answers with a `Nack`. So
silence proves that the slot is free, and a `Noop` is safe. A handoff has no quorum report
behind it, so silence proves nothing. Its tail must name every slot, and a fence that the
successor cannot cover makes it resign to an ordinary election
([A ballot is not a leadership](@/paxos/tricky/ballot-is-not-leadership.md)).

## The code map

| What | Where |
|---|---|
| The two policies | `crates/lib/paros-core/src/proposer/recovery.rs:14` `RecoveryPolicy` |
| One recovery | `crates/lib/paros-core/src/proposer/recovery.rs:28` `Recovery` |
| The next step of a recovery | `crates/lib/paros-core/src/proposer/recovery.rs:224` `recovery_next` |
| The page ceiling | `crates/lib/paros-core/src/contract/limits.rs:20` `RECOVERY_BATCH` |
| Win the election, open recovery | `crates/lib/paros-core/src/node/election.rs:561` `try_become_leader` |
| Pump the recovery | `crates/lib/paros-core/src/node/election.rs:743` `pump_leader_recovery` |
| The inherited recovery | `crates/lib/paros-core/src/node/handoff.rs:507` `on_relinquish` |

## The proof

- **Bounded.** The audit asserts that "a leader starts at most one bounded recovery chunk per
  Ready" and that "a recovery batch reports only gap fills it actually started"
  (`crates/sim/paros-sim/src/audit/node.rs:1457`).
- **The gap fill happens.** The simulation reaches "a new leader gap-fills a hole its promise
  quorum never reported" (`crates/sim/paros-sim/src/audit/node.rs`).
- **Unit tests.** `election_fills_a_hole_the_promise_quorum_never_reported`
  (`crates/lib/paros-core/src/node/tests/election.rs:92`),
  `the_transferred_tail_names_every_slot_below_the_frontier`
  (`crates/lib/paros-core/src/node/tests/handoff.rs:562`) and
  `a_step_down_makes_a_never_re_sent_hole_permanent_until_the_noop_fill`
  (`crates/lib/paros-core/src/node/tests/replication.rs:206`).

The levels [`act1/recovery-is-not-catch-up`](../../../play/#act1/recovery-is-not-catch-up) and
[`act4/the-handoff`](../../../play/#act4/the-handoff) show the rest. The field guide is
[The stable leader](@/paxos/stable-leader.md).
