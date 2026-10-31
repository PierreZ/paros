+++
title = "Reconfiguration is a round change"
weight = 13
+++

An acceptor set belongs to one ballot, and no node edits it. To change the set, the leader
gives up its ballot and campaigns again at a fresh ballot with the new set. The new ballot's
Phase 1 covers the old set, and its Phase 2 uses the new set. New commands wait for one
matchmaking round trip and one Phase 1. paros accepts that stall.

{{ scenario(name="act4/reconfigure") }}

## The failure

**A set edited under a live ballot.** A leader at ballot 7 runs `{0, 1, 2}`. Slot 9 has one
vote, from 0. The leader edits its set to `{0, 3, 4}` and keeps ballot 7. Node 3 votes, and the
leader counts 0 and 3 as a majority of the new set: slot 9 is chosen. The matchmakers still say
that ballot 7 uses `{0, 1, 2}`. A later candidate asks 1 and 2, a quorum of that set. Neither
voted at slot 9, so the candidate fills it with another value.

**A handoff across the change.** A cooperative handoff gives the leadership to a successor at
the **same** ballot, with no Phase 1. If a handoff also changed the set, the successor would
run a new set with no Phase 1 over the old one. That is the first failure again.

**A removed node that goes silent.** The cell moves from `{0, 1, 2}` to `{0, 3, 4}`. Nodes 1
and 2 stop answering because "they are not members". A leader elected next still has
`{0, 1, 2}` in `H_b`, and it needs a quorum of it. It never gets one, and the cell has no
leader. "Removed" is not "shut down".

**A change quietly lost.** A cell with no matchmakers has no place to record a second set. If
it accepted a reconfiguration and did nothing, the operator would believe in a set that does
not exist.

## The rule

1. **Only a settled leader starts a change.** `reconfigure(C_new)` refuses with a reason
   when the node is not the leader, when the cell has no matchmakers, when the set is
   unchanged, when it names a node outside the pool, when it does not admit its quorum system,
   or when a recovery or a repair probe is still open.
2. **A fresh ballot.** The leader campaigns at a round strictly above its own, and registers
   `C_new` as a *reconfiguration*, not as a belief. It drops its open rounds: the new ballot's
   Phase 1 recovers them, as for any deposed leader.
3. **Phase 1 over every set.** The matchmakers report the old set in `H_b`, so the new ballot
   needs a quorum of the old set and of `C_new`. Recovered values and gap fills are then
   chosen at the new ballot, under `C_new`.
4. **Joining and leaving.** A joining node gets the `Prepare`, so it promises the new ballot
   and learns the set before any `Accept` reaches it. It fills its log by the normal catch-up.
   A removed node keeps answering Phase 1: its `Prepare` guard checks the pool, never the set.
   A leader that its own change removed finishes the change, then resigns, and an ordinary
   election puts the leadership inside `C_new`.
5. **No handoff across a change.** The handoff keeps its ballot, so it never carries a new set.
6. **No matchmakers, no change.** The request is refused with `NoMatchmakers`.

## Why the rule is enough

A set bound to one ballot makes the matchmakers' map true: each ballot used exactly the set
they hold for it. Then the page before applies. `H_b` names every set that could hold a chosen
value, and a quorum of each finds that value. The old leader's open rounds are no special
case: a new ballot treats them as the work of a deposed leader.

The change is in force when its registration reaches a matchmaker quorum. That does not depend
on any Phase 1 or Phase 2 under the new set. A request answered "started" can still be lost
before that point, like any proposal that has not reached a quorum.

The stall costs availability only, never safety. The paper describes an overlap of two
ballots with no stall. paros does not implement it, because it needs a leader that holds two
live rounds at once.

## The code map

| What | Where |
|---|---|
| The entry point and its refusals | `crates/lib/paros-core/src/node/reconfigure.rs:120` `ColocatedNode::reconfigure`, `:69` `ReconfigureRefusal` |
| The fresh campaign, registered as a reconfiguration | `crates/lib/paros-core/src/node/election.rs:143` `ColocatedNode::campaign` |
| The registration kinds | `crates/lib/paros-core/src/matchmaker/state.rs:456` `RegistrationKind` |
| A removed node still answers Phase 1 | `crates/lib/paros-core/src/node/acceptor.rs:32` `on_prepare` |
| A removed leader resigns once settled | `crates/lib/paros-core/src/node.rs:849` in `ColocatedNode::tick` |
| A node outside the set does not campaign | `crates/lib/paros-core/src/node/election.rs:57` in `on_check_leader` |

## The proof

- "at most one value is ever chosen for a slot" (`crates/sim/paros-sim/src/audit/node.rs:244`)
  holds while the simulation's reconfiguration workload
  (`crates/sim/paros-sim/src/workload/reconfigure.rs`) grows, shrinks and replaces the acceptor
  set, and moves it between a majority and a grid.
- The same workload asserts "reconfiguration: a deployment without matchmakers never accepts a
  reconfiguration" (`reconfigure.rs:558`) and "reconfiguration: a configuration that does not
  admit its quorum system is never started" (`reconfigure.rs:563`).
- The audit checks "reconfiguration: a deployment without matchmakers never starts one"
  (`crates/sim/paros-sim/src/audit/matchmaking.rs:1455`). The core marks the resignation of a
  removed leader as a reachable site ("reconfiguration: a leader its own reconfiguration removed
  resigns").
- Unit tests in `crates/lib/paros-core/src/node/tests/reconfigure.rs`:
  `reconfiguration_is_refused_where_it_cannot_run`,
  `a_reconfiguration_moves_the_leadership_to_a_fresh_ballot_and_configuration`,
  `a_leader_removed_by_its_own_reconfiguration_resigns_once_settled` and
  `a_reconfiguration_waits_for_a_settled_leadership`. In `node/tests/matchmaking.rs`:
  `a_value_held_only_by_an_old_configuration_is_re_proposed`,
  `gap_fill_waits_for_every_configuration` and
  `a_removed_member_still_answers_phase_one_for_a_non_member_leader`.
