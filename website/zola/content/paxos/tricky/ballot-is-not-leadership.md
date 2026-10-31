+++
title = "A ballot is not a leadership"
weight = 5
+++

A node that won a ballot is not a leader forever. Another node can win a higher ballot behind
its back, and a partitioned leader hears no `Nack` to tell it. paros keeps leadership as a
separate, volatile **authority**. The authority holds a fence and a window of acknowledgements,
and it dies when the leader cannot prove that a quorum still follows it.

{{ scenario(name="act4/the-handoff") }}

## The failure

**The leader behind a partition.**

1. Leader A, at ballot `2.A`, is cut off and gets no traffic. Its election clock stops, and it
   sends no Accept, so no `Nack` reaches it.
2. B wins ballot `3.B` and the cell moves on.
3. A still believes it leads. It admits proposals into a suffix that is no longer its own. This
   was a real double apply (#94 (stale leader double apply)).

**The fence a successor never covers.** A successor of a handoff holds ballot `b`, but it did
not recover the slots below its inherited frontier. If only the old leader held a decision
there, the prefix stops below the fence forever.

## The rule

- `Authority` is the volatile state of a leadership: a `fence` and the CheckQuorum
  acknowledgement window. `open_authority` opens it when the node wins an election (the fence
  is its next slot minus one) or installs a handoff (the fence is the inherited next slot minus
  one). It goes away whole when the node becomes a follower.
- An acknowledgement counts only if it carries the current ballot: a HeartbeatAck or an
  Accepted.
- `tick_check_quorum`: after `election_timeout` ticks, the window must hold a Phase-2 quorum
  (`Authority::holds`). If it does not, the leader resigns.
- `tick_handoff_fence`: a leader that came from a handoff must cover its fence within
  `HANDOFF_FENCE_ELECTIONS` (3) election timeouts. If it does not, it resigns to an ordinary
  Phase 1.
- `step_down` is the voluntary resign. It drops the rounds and the fence, and keeps the durable
  state.

The same fence is the [read basis](@/paxos/tricky/quorum-reads.md) of the quorum reads.

## Why the rule is enough

Every later Phase-1 quorum meets a Phase-2 set of acknowledgements at the current ballot. So a
window that holds proves that no later ballot decided anything behind the leader. An empty
window ends the leadership within one election timeout. What an uncovered fence cannot heal, an
ordinary Phase 1 heals.

## The code map

| What | Where |
|---|---|
| The authority | `crates/lib/paros-core/src/proposer/authority.rs:26` `Authority` |
| Does the window hold a quorum | `crates/lib/paros-core/src/proposer/authority.rs:176` `holds` |
| Open an authority | `crates/lib/paros-core/src/proposer/authority.rs:238` `Proposer::open_authority` |
| CheckQuorum | `crates/lib/paros-core/src/node/authority.rs:31` `tick_check_quorum` |
| The handoff fence deadline | `crates/lib/paros-core/src/node/handoff.rs:702` `tick_handoff_fence` |
| The voluntary resign | `crates/lib/paros-core/src/node/policy.rs:242` `step_down` |

## The proof

- **A deposed leader stops.** The audit asserts that "a leader deposed by a promise-majority
  stops beating within an election timeout (CheckQuorum)"
  (`crates/sim/paros-sim/src/audit/state.rs:1363`).
- **The resigns happen.** `assert_sometimes!` checks that "a leader without an ack quorum steps
  down (CheckQuorum)" (`crates/sim/paros-sim/src/audit/state.rs:869`). The simulation also
  reaches "an uncovered inherited fence resigns back to an ordinary election".
- **Unit tests** in `crates/lib/paros-core/src/node/tests/replication.rs`:
  `a_leader_without_an_ack_quorum_steps_down_after_its_window`,
  `a_leader_hearing_acks_keeps_leadership_across_windows` and
  `voluntary_step_down_resigns_and_drops_the_volatile_leadership_state`; and
  `an_uncovered_inherited_fence_resigns_back_to_an_ordinary_election`
  (`crates/lib/paros-core/src/node/tests/handoff.rs:497`).

The field guide is [Beyond Multi-Paxos](@/paxos/beyond-multi-paxos.md#cooperative-leader-handoff).
