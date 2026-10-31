+++
title = "Matchmaking before Phase 1"
weight = 12
+++

On a cell with matchmakers, a campaign does not start with `Prepare`. The candidate first
registers its ballot and its acceptor set with a matchmaker quorum. The answers tell it every
older set, and its Phase 1 must win a quorum of each of those sets. A node that does not know
the set in force asks the matchmakers before it campaigns.

{{ scenario(name="act4/matchmaking") }}

## The failure

**A Prepare before the registration.** Candidate X campaigns at ballot 8 with the set
`{2, 3, 4}`. It sends `Prepare` at once, and only one matchmaker records ballot 8. X gets a
slot chosen with 3 and 4, then stops. Candidate Y asks the two other matchmakers. Neither
knows ballot 8, so Y never asks `{2, 3, 4}`, and it fills the slot with another value.

**A quorum of the union.** `H_b` holds `C1 = {0, 1, 2}` and `C2 = {2, 3, 4}`. Under `C2`, a
leader chose slot 5 with 3 and 4. A new candidate counts a majority of the union
`{0, 1, 2, 3, 4}`, and gets promises from 0, 1 and 2. That is three of five, and it is a
majority of `C1`. But 0, 1 and 2 never saw slot 5, so the candidate fills it with a `Noop`.
The slot has two values.

**The wrong watermark.** Each matchmaker reports the floor below which it forgot old sets.
If a candidate keeps the sets above the *lowest* floor it heard, it keeps a set that another
matchmaker already forgot. That set can name an acceptor that the operator stopped, and a
Phase 1 that waits for it waits forever.

**A stale belief wins.** Node 4 is down while the cell moves from `{0, 1, 4}` to `{0, 1, 2}`.
Node 4 restarts with its old belief, campaigns with `{0, 1, 4}`, and wins: 0 and 1 are a
majority of that set too. The cell now runs the old set again, and nobody asked for it.

**Two beliefs that chase each other.** If a candidate adopts the newest registration it hears,
two candidates with different beliefs adopt the belief of the other, again and again. The
simulation found this on seed 7519660681720567139: 182 aborted campaigns and no leader for 50
seconds.

## The rule

1. **Register first.** A candidate promises its own ballot, then sends `(b, C_b)` to every
   matchmaker. It sends no `Prepare` until a matchmaker quorum answers.
2. **`H_b` is a union, filtered once.** Each answer carries the sets the matchmaker holds below
   `b`, its floor, and the set in force. The candidate takes the union of the histories and the
   **maximum** floor. At closure, `H_b` is every distinct set registered at or above that
   maximum.
3. **A quorum of each set.** Phase 1 sends `Prepare` to every member of `H_b` and of `C_b`. It
   completes only when **every** set in `H_b` holds a Phase-1 quorum of promises. One promise
   counts for each set that contains its sender. A quorum of the union never counts.
4. **Facts, not beliefs.** A registration is a *belief* (an ordinary campaign) or a
   *reconfiguration* (an operator's change). The set in force is the highest-ballot
   reconfiguration a matchmaker quorum holds. An ordinary campaign whose answers name another
   set in force gives up, adopts that set, and campaigns again. Beliefs never cause this.
5. **Probe at boot.** A node whose belief is only its bootstrap default does not campaign. It
   asks a matchmaker quorum for the set in force, and registers nothing. It campaigns only if
   that set names it.

## Why the rule is enough

Every ballot registered with a matchmaker quorum before any acceptor voted for it. Any two
matchmaker quorums share a matchmaker, so a candidate's quorum holds the record of every
earlier ballot. `H_b` therefore misses no set that could hold a chosen value. A set that was
never used only costs a few extra promises. A quorum of each set meets each Phase-2 quorum
that could have chosen a value, so the old Paxos argument holds once per set.

A reconfiguration request only moves forward by ballot, and no campaign makes one. So
adopting the highest one cannot loop, and a node that missed it cannot be elected under the
old set.

## The code map

| What | Where |
|---|---|
| Register before Prepare | `crates/lib/paros-core/src/node/election.rs:143` `ColocatedNode::campaign` |
| Fold one answer: union, maximum floor, set in force | `crates/lib/paros-core/src/matchmaking.rs:399` `Matchmaking::fold` |
| `H_b`, filtered at the maximum floor | `crates/lib/paros-core/src/matchmaking.rs:553` `Matchmaking::prior` |
| The stale-belief signal | `crates/lib/paros-core/src/matchmaking.rs:589` `Matchmaking::stale_belief` |
| The boundary from matchmaking to Phase 1 | `crates/lib/paros-core/src/node/matchmaking.rs:637` `fold_registration` |
| A quorum of every set in `H_b` | `crates/lib/paros-core/src/proposer/election.rs:128` `Election::covered` |
| Probe instead of campaign | `crates/lib/paros-core/src/node/election.rs:47` `on_check_leader`, `crates/lib/paros-core/src/node/matchmaking.rs:379` `probe_membership` |
| The probe tally | `crates/lib/paros-core/src/matchmaking/probe.rs:72` `MembershipProbe` |

## The proof

The oracles are in `crates/sim/paros-sim/src/audit/`.

- "matchmaking: Phase 1 opens only on a matchmaker quorum" (`matchmaking.rs:551`).
- "matchmaking: the watermark used is the maximum reported" (`matchmaker.rs:433`) and
  "matchmaking: the prior set is the union of every registering reply above the watermark"
  (`matchmaker.rs:447`).
- "matchmaking: a completed campaign runs the effective configuration" (`matchmaker.rs:1052`)
  and "matchmaking: a stale-belief abort adopts a reconfiguration registration"
  (`matchmaking.rs:669`).
- "matchmaking: a membership probe learns every reconfiguration a matchmaker quorum held when
  it opened" (`matchmaking.rs:739`).
- Above all of these, "at most one value is ever chosen for a slot" (`node.rs:244`).
- The handover model checker (`crates/lib/paros-core/src/models/handover.rs`) runs the real
  `Matchmaking` tally. Its claim 4 asserts that a closed `H_b` holds every registration a
  majority held, across GC and handovers.
- Unit tests: `phase1_needs_a_quorum_of_every_prior_configuration`
  (`crates/lib/paros-core/src/proposer/tests.rs`), and in
  `crates/lib/paros-core/src/node/tests/matchmaking.rs`:
  `a_campaign_registers_before_it_prepares`,
  `a_majority_of_the_union_that_fails_one_configuration_does_not_win`,
  `the_prior_set_is_the_union_above_the_maximum_watermark`,
  `a_stale_candidate_adopts_the_highest_reconfiguration_and_re_campaigns` and
  `other_candidates_beliefs_never_change_a_belief`. The probe tests are in
  `node/tests/membership_probe.rs`.
