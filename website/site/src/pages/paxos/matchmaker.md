---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
lab: true
trace: matchmaker
title: Matchmaker Paxos
description: Change the acceptors of a journal while it takes writes. A reconfiguration is only a new ballot with a new configuration.
---

## The problem

Machines fail and get replaced, and a tenant can ask for more durability. The acceptors of a
journal must change while writes go on, with no stop and no external configuration store.

## Step through

**Matchmakers** (`mm`) keep, for each [ballot](../#ballot), the acceptors that the ballot uses. A leader
registers its [configuration](../#configuration) with the matchmakers (`MatchRequest`) before its Phase 1. The
answer names the configurations of the lower ballots. A `GcRequest` lets the matchmakers
forget the configurations that no longer hold a chosen slot.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## Your turn

Now you are the network. Deliver or drop each message, and reach the goal.
`paros-consensus` runs in this page and decides everything else.

<div data-lab="old-set"><p class="aside">The exercises need JavaScript.</p></div>

## The rule

**Phase 1 asks a quorum of every configuration that the matchmakers report for a lower
ballot, and Phase 2 writes only to the new one.** So Phase 1 finds any value that an old
configuration chose, and writes do not stop during the change.

## Segments

A log grows for ever, so the leader can cut it into **segments**. A segment is a range of
slots. The cut is a command of the log, the `Roll`: `ColocatedNode::roll` proposes it like a
write, and the leader does not wait for it. When a valid `Roll` is chosen at slot `s`, slot `s` is
the last slot of its segment, and the next segment starts at `s + 1`. A `Roll` is valid when it
names the segment after the tail; any other `Roll` changes nothing. Paxos chooses one value for
each slot, so every node ends a segment at the same slot. A roll needs matchmakers: a deployment
without them refuses it. A change of the acceptors is a new ballot, and it cuts no segment.

The replicas that keep a sealed segment, and the chain of segments at the matchmakers, are not
built yet. The design is
[§4.7 Segments in the core](https://github.com/PierreZ/paros/blob/main/docs/architecture/04-7-segments.md).

## In paros

- `paros-consensus`: `Matchmaker` ([`matchmaker/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/src/matchmaker)) and the matchmaking phase
  ([`matchmaking/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/src/matchmaking)).
- The matchmaker set itself is chosen by single-decree Paxos, and moves to a new set by a
  generation handover.
- A removed acceptor is not shut down at once. It retires only when the GC floor is
  effective and every matchmaker of the set says that no registration above the floor names
  it (`ColocatedNode::may_retire`). Until then, a later Phase 1 may need its promise. Each
  matchmaker that agrees records the node as retired, durably, and refuses every later
  registration that names it (`MatchRefusal::Retired`). A handover carries these records to
  the successor set. When placement books the node on that journal again, the node first
  rejoins: at a new incarnation, it asks every matchmaker to drop its record.
- Segments: `Roll` and `Segment` ([`journal/segment.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/journal/segment.rs)) and
  the driver call `ColocatedNode::roll` ([`node/roll.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/node/roll.rs)).
  Example: [`grow`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/grow.rs): stages 3 to 6 reconfigure and hand over the matchmakers, stage 8 rolls a segment.
- Consensus: [the deployments](../../consensus/deployments/#matchmaker-and-matchmakerreconfigurer) show how a node and its matchmakers are driven together.
