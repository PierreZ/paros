+++
title = "Phase 1 over a log"
weight = 1
+++

Single-decree Paxos runs Phase 1 for one slot. A log has an unbounded number of slots, and a
new leader cannot send one Prepare per slot. paros sends one Prepare for the whole suffix of the
log. Each acceptor answers with its votes in bounded pages, and the leader keeps the
highest-ballot vote for each slot.

{{ scenario(name="act2/elect-a-leader") }}

## The failure

Three nodes, A, B and C. Leader A, at ballot `1.A`, sends slot 3 = `x` to A and B. Slot 3 is
chosen.

1. C campaigns at ballot `2.C`. It asks only from slot 4, or it counts a promise whose first
   page did not reach slot 3.
2. C hears nothing about slot 3. It proposes `y` there, and B and C vote for it.
3. Slot 3 now has two chosen values.

A second failure: B reports slot 3 = `x` at `1.A`, and A reports slot 3 = `z` at a later ballot
`2.A`. If C keeps the first report, it re-proposes a stale value.

## The rule

- A candidate sends one `Prepare(from_slot, ballot)`. It claims every slot from `from_slot`,
  which is the lower of the first hole and the first slot not chosen.
- Each acceptor answers with `Promise` pages. A page holds at most `promise_page` records (the
  ceiling is `PROMISE_BATCH`, 64), with a cursor to the next page.
- A page counts only at the cursor that the candidate expects from that acceptor. A promise
  counts toward the quorum only when its whole suffix arrived.
- `merge_report` keeps the highest-ballot vote for each slot. This is P2c. Two votes with the
  same slot and ballot must carry the same command, and the core panics if they do not.
- `close_phase1` records the highest slot reported (`highest_reported`). The allocator and the
  fence of the new leadership come from it.
- An acceptor that truncated above `from_slot` answers `Nack` (`BelowFloor`). It never answers
  with fewer votes than it has.

## Why the rule is enough

A chosen value was accepted by a Phase-2 quorum. That quorum meets every complete promise
quorum, so each chosen slot at or above `from_slot` is reported. P2c then makes the
highest-ballot vote the only value that the slot can still choose.

## The code map

| What | Where |
|---|---|
| Start a campaign, compute `from_slot` | `crates/lib/paros-core/src/node/election.rs:209` `start_phase1` |
| The acceptor's side | `crates/lib/paros-core/src/acceptor.rs:402` `Acceptor::prepare` |
| The page ceiling | `crates/lib/paros-core/src/contract/limits.rs:12` `PROMISE_BATCH` |
| Fold one page | `crates/lib/paros-core/src/proposer/election.rs:421` `fold_promise` |
| P2c: the highest ballot wins | `crates/lib/paros-core/src/proposer.rs:356` `merge_report` |
| Close Phase 1 | `crates/lib/paros-core/src/proposer/election.rs:511` `close_phase1` |

## The proof

- **One value per slot.** The audit asserts that "at most one value is ever chosen for a slot"
  (`crates/sim/paros-sim/src/audit/node.rs:244`), and that "an accept at or above a decided
  ballot carries the decided value" (`crates/sim/paros-sim/src/audit/state.rs:1140`).
- **Bounded pages.** The audit asserts that "a Promise carries at most one bounded suffix
  chunk" (`crates/sim/paros-sim/src/audit/node.rs:337`).
- **P2c happens.** A probe in `merge_report` checks that a promise quorum sometimes reports two
  values for one slot, and that P2c keeps the higher ballot.
- **Unit tests** in `crates/lib/paros-core/src/node/tests/election.rs`:
  `recovery_picks_highest_ballot_value_per_slot`, `promise_suffix_is_served_in_bounded_pages`,
  `a_partial_promise_page_does_not_count_toward_the_quorum` and
  `truncated_quorum_refuses_a_blind_candidate`.

The field guide is [The stable leader](@/paxos/stable-leader.md).
