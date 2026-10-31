---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
title: Matchmaker Paxos
description: Change the acceptors of a journal while it takes writes. A reconfiguration is only a new ballot with a new configuration.
---

## The problem

Machines fail and get replaced, and a tenant can ask for more durability. The acceptors of a
journal must change while writes go on, with no stop and no external configuration store.

## The protocol

**Matchmakers** keep, for each ballot, the configuration of acceptors that the ballot uses.
A new leader with a new configuration talks to the matchmakers before its Phase 1.

<div class="seq"><table>
<thead><tr><th>leader</th><th>matchmakers</th><th>old acceptors</th><th>new acceptors</th><th class="note">what happens</th></tr></thead>
<tbody>
<tr><td class="msg">MatchA(8, C2) →</td><td>●</td><td></td><td></td><td class="note">record: ballot 8 uses configuration C2</td></tr>
<tr><td>←</td><td class="msg">C1 at ballot 7</td><td></td><td></td><td class="note">the configurations of lower ballots</td></tr>
<tr><td class="msg">Prepare(8) →</td><td></td><td>●</td><td></td><td class="note">Phase 1 on the old configurations only</td></tr>
<tr><td class="msg">Accept(8, …) →</td><td></td><td></td><td>●</td><td class="note">Phase 2 on the new configuration only</td></tr>
<tr><td class="msg">GC</td><td>●</td><td>retire</td><td></td><td class="note">when C2 holds every chosen slot, C1 is forgotten</td></tr>
</tbody></table></div>

## Why it is safe

Phase 1 must find any value that a lower ballot could have chosen. The matchmakers name
every configuration that a lower ballot used, so Phase 1 asks the right acceptors. Phase 2
then writes only to the new acceptors. Writes do not stop: the leader keeps taking writes in
the new configuration while the old one drains.

## In paros

- `paros-core`: `Matchmaker` (`matchmaker/`) and the matchmaking phase (`matchmaking/`).
- Each tenant has one matchmaker set. The set itself is chosen by single-decree Paxos.
- `parosctl configure` and `parosctl cell drain-machine` end in a reconfiguration. The
  tenant coordinator books the new acceptors, and the leader changes configuration.
