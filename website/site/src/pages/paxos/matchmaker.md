---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: matchmaker
title: Matchmaker Paxos
description: Change the acceptors of a journal while it takes writes. A reconfiguration is only a new ballot with a new configuration.
---

## The problem

Machines fail and get replaced, and a tenant can ask for more durability. The acceptors of a
journal must change while writes go on, with no stop and no external configuration store.

## Step through

**Matchmakers** (`mm`) keep, for each ballot, the acceptors that the ballot uses. A leader
registers its configuration with the matchmakers (`MatchRequest`) before its Phase 1. The
answer names the configurations of the lower ballots. A `GcRequest` lets the matchmakers
forget the configurations that no longer hold a chosen slot.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## The rule

**Phase 1 asks a quorum of every configuration that the matchmakers report for a lower
ballot, and Phase 2 writes only to the new one.** So Phase 1 finds any value that an old
configuration chose, and writes do not stop during the change.

## In paros

- `paros-core`: `Matchmaker` (`matchmaker/`) and the matchmaking phase (`matchmaking/`).
- Each tenant has one matchmaker set. The set itself is chosen by single-decree Paxos.
- `parosctl configure` and `parosctl cell drain-machine` end in a reconfiguration. The
  tenant coordinator books the new acceptors, and the leader changes configuration.
