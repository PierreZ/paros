+++
title = "Proxy leaders: the leader only sequences"
weight = 18
+++

A Multi-Paxos leader **sequences** (it selects the slot) and **broadcasts** (it sends each
`Accept`, counts each `Accepted`, sends each `Commit`). Only the first job must be serial. A
**proxy leader** takes the second job: the leader gives the slot's `Accept` to a proxy, and the
proxy runs Phase 2. A proxy votes on nothing and orders nothing, so it cannot break agreement.

{{ scenario(name="act5/proxy-leaders") }}

## The failure

**The leader is the bottleneck.** With `f` faults to tolerate, a classic leader sends and
receives at least `3f + 4` messages per command. Add acceptors or learners and the leader gets
slower. A second leader is not a fix: there is one sequencer per ballot.

**A proxy that decides.** Suppose a proxy may adopt a ballot or choose a value:

1. Leader L1 delegates slot 7 at ballot 1 to proxy P.
2. Leader L2 wins ballot 2 and opens slot 7 with another value.
3. P receives a late `Nack` and, on its own, opens slot 7 again at ballot 2 with L1's value.
4. Ballot 2 now has two proposers for slot 7. Two values can be chosen.

**A dead proxy.** A proxy crashes with an open round. Nobody counts the votes, and the slot
stays open for ever.

## The rule

1. The leader selects slot `s` and the proxy `s % proxies` (`ProxyId::of`). It sends that proxy
   one `Accept` whose `reply_to` names the proxy and whose `leader` names itself.
2. The proxy sends the same `Accept` to the slot's column, counts the `Accepted`s with the same
   predicate as a leader (`has_phase2_quorum_in`), and sends the `Commit` to every learner.
3. The proxy adopts no ballot. It works for the highest ballot a leader delegated to it, and it
   **relays** a `Nack` to the leader. Only the leader decides what a higher ballot means.
4. The leader keeps liveness. On each beat it delegates each open round again. After a budget of
   re-delegations with no `Commit`, it **takes the round back** and runs it colocated
   (`take_back_delegated`).

{{ scenario(name="act5/the-dead-proxy") }}

## Why the rule is enough

A chosen value is still exactly what a Phase-2 quorum accepted durably at one ballot. The proxy
only observes that quorum and announces it. Two fan-outs of one `(slot, ballot, command)`, from
two proxies or from a proxy and the leader, are the same proposal, so their verdicts agree. A
proxy keeps nothing durable, so a crash loses no decision: the leader's re-delegation and
take-back bring the round back. The leader handles two messages per command, whatever the width
of the grid.

## The code map

| What | Where |
|---|---|
| The proxy's state machine | `crates/lib/paros-core/src/proxy_leader.rs:180` `ProxyLeader` |
| Its single entry point | `crates/lib/paros-core/src/proxy_leader.rs:239` `ProxyLeader::step` |
| Its beat | `crates/lib/paros-core/src/proxy_leader.rs:503` `resend_pending` |
| Bounded retention | `crates/lib/paros-core/src/proxy_leader.rs:563` `expire_stale` |
| The leader's choice | `crates/lib/paros-core/src/node.rs:118` `Delegation`, `:570` `propose_in` |
| The proxy of a slot | `crates/lib/paros-core/src/membership/ids.rs:47` `ProxyId::of` |
| The take-back | `crates/lib/paros-core/src/node/policy.rs:99` `take_back_delegated` |

## The proof

A sans-IO model checker beside the core, `crates/lib/paros-core/src/models/proxy.rs`, runs real
nodes and proxies over a lossy, reordering, duplicating scheduler, with crashes and a handoff in
the middle of a round. After each step it asserts one value per slot, and that each proxy
`Commit` rests on a Phase-2 quorum of durable accepts at one ballot. In the simulation, the
oracle "proxy: a Commit a proxy emits is backed by a durable Phase-2 quorum at one ballot" checks
the same claim, and the gates "proxy: a slot is decided through a proxy leader" and "proxy: a
leader takes a delegated round back" prove that both paths run
(`crates/sim/paros-sim/src/audit/state.rs`).
