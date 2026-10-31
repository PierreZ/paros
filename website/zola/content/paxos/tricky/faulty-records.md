+++
title = "The third answer: faulty"
weight = 7
+++

Classic Paxos assumes that a disk keeps what it wrote. Real disks lose records. When an
acceptor cannot read its record for a slot, it must not answer "I did not vote". paros gives it
a third answer, **faulty**: "I voted at this ballot, and I lost the value". This is the CTRL
approach to storage faults.

{{ scenario(name="act4/faulty-records") }}

## The failure

1. Slot 3 is chosen at ballot 2 with value V, on acceptors A1 and A2.
2. The record for slot 3 rots on the disk of A1. With two answers only, A1 says "nothing
   accepted".
3. A new leader at ballot 7 gets promises from A1 and A3. Both report nothing for slot 3.
4. The leader fills slot 3 with a `Noop`. V is overwritten, and a chosen value is lost.

A second failure waits behind the first. Suppose the only clean copy of V is on a node behind a
partition. A leader that waits for that copy waits forever.

## The rule

- A Promise page (`PromisePage`) has three answers per slot: `accepted` (the record reads
  back), `faulty` (the ballot is known, the value is lost) or no entry (no vote). A damaged
  record is always reported as faulty, never as no vote.
- An answer **qualifies** when it is "no vote", or a record at a ballot no higher than the best
  value reported (`qualifying_answers`).
- A slot with a faulty answer is decided only when the qualifying answers form a full Phase-1
  quorum in every earlier configuration (`slot_decidable`). Then the leader re-proposes the
  best value reported (case 1) or a `Noop` (case 2).
- Otherwise the slot is blocked (case 3). The leader's `RepairProbe` sends Prepare at its own
  ballot to the acceptors that did not answer yet, on each tick.
- If the probe stays blocked for `election_timeout * REPAIR_TIMEOUT_ELECTIONS` ticks, the
  leader steps down. `REPAIR_TIMEOUT_ELECTIONS` is 3.

## Why the rule is enough

A faulty answer never counts toward a quorum of "no vote". So a leader writes a `Noop` only
when quorum intersection proves that no value was chosen. When the leader cannot prove it, it
waits, and after a time it resigns. Another node may then reach the missing copy.

## The code map

| What | Where |
|---|---|
| The three answers of a promise | `crates/lib/paros-core/src/acceptor.rs:86` `PromisePage` |
| Build a promise page | `crates/lib/paros-core/src/acceptor.rs:451` `Acceptor::promise_page` |
| Which answers qualify | `crates/lib/paros-core/src/proposer.rs:245` `qualifying_answers` |
| When a slot is decidable | `crates/lib/paros-core/src/proposer.rs:306` `slot_decidable` |
| The repair probe | `crates/lib/paros-core/src/proposer/probe.rs:26` `RepairProbe` |
| The resign timeout | `crates/lib/paros-core/src/node.rs:63` `REPAIR_TIMEOUT_ELECTIONS` |

## The proof

- **One value per slot.** The audit asserts that "a durable accept quorum never decides two
  values for a slot" (`crates/sim/paros-sim/src/audit/state.rs:1206`). A version of
  `slot_decidable` that checked only the newest earlier configuration turned the sweep red
  (#267 (faulty slots over every configuration)).
- **The repair paths happen.** The simulation reaches "a faulty record is repaired in place
  from the cluster", case 1, case 2, and "a leader that cannot finish recovery resigns"
  (`crates/sim/paros-sim/src/audit/node.rs:1426`).
- **Unit tests** in `crates/lib/paros-core/src/node/tests/recovery.rs`:
  `promise_reports_faulty_tristate_never_none`,
  `full_none_quorum_noop_fills_over_an_excluded_faulty_reporter`,
  `blocked_slot_waits_then_resolves_case1_from_a_straggler` and
  `recovery_timeout_steps_the_leader_down`.

The field guide is [Beyond Multi-Paxos](@/paxos/beyond-multi-paxos.md#faulty-records).
