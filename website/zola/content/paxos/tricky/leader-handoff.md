+++
title = "Leader handoff without an election"
weight = 17
+++

A leader that is alive and must move (a clean stop of its machine, a drain) does not need an
election. It gives its **Phase-2 authority** to one peer with one `Relinquish` message: its
ballot, its next free slot, and its unfinished rounds. The peer continues **under the same
ballot**, with no Phase 1. The old leader steps down in the same call that queues the message.
The source is the *Leader Handoff* of DPaxos (§4.4).

{{ scenario(name="act4/the-handoff") }}

## The failure

**An election for a planned move.** The successor selects a higher ballot, runs Phase 1, and
learns again the log suffix that the old leader already knew. The cell waits an election
timeout, then a round trip, then a quorum of promise writes. All of it is waste, because the
old leader is alive and can say what it knows.

**Two proposers at one ballot.** A naive handoff lets the old leader keep a copy of its role:

1. Leader A holds ballot `b` and slot 8 is open with command `z`.
2. A sends its authority to C. C proposes `z` at slot 8, and then `y` at slot 9.
3. A delayed client command reaches A. A still believes it leads, and it proposes `x` at slot 9
   under the same ballot `b`.
4. Two quorums vote at `(9, b)` for two values. Slot 9 has two chosen values.

**A replayed message.** If C may hand the authority on to D, a delayed copy of A's first
message can reach C again. Every check passes, and C installs `b` a second time while D uses it.

## The rule

1. **Abdicate in the same call.** `relinquish_to` queues the `Relinquish` and makes the leader a
   follower before it returns. The message cannot exist without the abdication.
2. **A crash is an abdication.** Leadership is not durable. A node that restarts is a follower,
   and its next campaign is at a strictly higher round.
3. **The successor is in the message.** Every other node refuses it.
4. **The tail tiles exactly.** The message splits `[first_unchosen, next_slot)` into the slots
   known chosen and the open rounds. The successor proposes each open round again **unchanged**
   at the same ballot, and it never fills a slot that the message did not describe.
5. **One hop only.** Only the node that won `b` in Phase 1 may relinquish it.
6. **A stale `Accept` does not depose the successor.** The predecessor's `Accept`s can still be
   in flight. One that reaches the successor names the predecessor as leader, at the very ballot
   the successor now holds. The successor votes for it (the value is the same) and keeps
   leading. This rule was found by the game's
   [`act5/handoff-through-a-proxy`](../../../play/#act5/handoff-through-a-proxy) level, where a
   proxy's fan-out is in flight across the move (#313).

{{ scenario(name="act5/handoff-through-a-proxy") }}

On a proxy deployment, the successor delegates each inherited round again to `slot % proxies`.
That is the same proxy, because the rule is a pure function of the slot. The proxy refreshes the
leader that it names and finishes the round under the same ballot.

## Why the rule is enough

Paxos needs one proposer per ballot and slot (commander invariant C1 of *Paxos Made Moderately
Complex*). Rules 1 and 2 make sure that the old leader never proposes at `b` again. Rule 3 makes
sure that one node installs `b`. Rule 5 closes the replay: C did not win `b`, so C cannot pass
it on, and a replayed message finds C already above it (its promise and its frontier, rule 4).
Rule 4 also keeps each open round's value: a proposal of the same value at the same
`(slot, ballot)` is idempotent, and a slot nobody described is not free, because no Phase-1
quorum reported it. Rule 6 changes no vote. It only keeps the one proposer of `b` in place.

A lost `Relinquish` costs availability only: nobody leads, and the next election timeout starts
an ordinary election.

## Compared with other systems

| System | How a live leader moves | Phase 1 for the successor? | If the move fails |
|---|---|---|---|
| paros | the ballot itself moves, in one message | no | an ordinary election |
| Raft (`TimeoutNow`, etcd-raft) | the leader stops proposals, brings the target up to date, and tells it to campaign | yes: a new term and a vote | the old leader keeps leading after one election timeout |
| Amazon MemoryDB | a *collaborative leadership transfer* during rolling updates; leadership is a conditional append to the transaction log, under a lease | the new primary must have seen the last log entry | lease expiry and a new append |

Raft's transfer is a forced election: it saves the timeout, not the vote. MemoryDB moves the
leadership through its log service, which is the consensus. paros moves the Paxos authority
itself, so the successor skips the vote too.

## The code map

| What | Where |
|---|---|
| May this leader hand off? | `crates/lib/paros-core/src/node/handoff.rs:289` `ColocatedNode::can_relinquish` |
| The send and the abdication | `crates/lib/paros-core/src/node/handoff.rs:398` `ColocatedNode::relinquish_to` |
| The install and its checks | `crates/lib/paros-core/src/node/handoff.rs:507` `on_relinquish` |
| The exact tiling | `crates/lib/paros-core/src/node/handoff.rs:203` `tail_shape_valid` |
| How the leadership started | `crates/lib/paros-core/src/node/handoff.rs:136` `LeadershipOrigin` |
| Never fill a slot nobody described | `crates/lib/paros-core/src/proposer/recovery.rs:24` `RecoveryPolicy::Inherited` |
| A stale `Accept` keeps the successor | `crates/lib/paros-core/src/node/acceptor.rs:222` `on_accept` |
| The successor's deadline | `crates/lib/paros-core/src/node/handoff.rs:702` `tick_handoff_fence` |

## The proof

The simulation rebuilds authority from the wire: an `Accept` sent means "exercises", a
`Relinquish` sent means "released". Its oracles in `crates/sim/paros-sim/src/audit/state.rs`
are "a relinquished authority is never exercised again", "one physical node at a time exercises
a logical Paxos authority" and "a transferred allocator frontier never rewinds"; the gate "proxy:
a delegated round survives a leader handoff" proves that the proxy case runs. The driver offers
a handoff at a BUGGIFY site, and it drops and duplicates `Relinquish` messages. The unit tests
are in `crates/lib/paros-core/src/node/tests/handoff.rs`, among them
`a_stale_accept_of_the_predecessor_does_not_depose_the_successor`.
