---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
title: Single-decree Paxos
description: A group of acceptors chooses one value. Once chosen, it never changes, even when proposers race and machines crash.
---

## The problem

Three machines must agree on one value. Two proposers can try at the same time, messages can
be lost, and a machine can crash between two steps. When a value is chosen, no later
proposal may replace it.

## The protocol

A proposer picks a **ballot**, a number larger than any it used before. Then it runs two
phases against the acceptors.

<div class="seq"><table>
<thead><tr><th>proposer</th><th>a1</th><th>a2</th><th>a3</th><th class="note">what happens</th></tr></thead>
<tbody>
<tr><td class="msg">Prepare(5) →</td><td>●</td><td>●</td><td>●</td><td class="note">Phase 1: ask for a promise at ballot 5</td></tr>
<tr><td>←</td><td class="msg">Promise</td><td class="msg">Promise</td><td>·</td><td class="note">a1, a2 promise to ignore ballots below 5 and report what they accepted</td></tr>
<tr><td class="msg">Accept(5, v) →</td><td>●</td><td>●</td><td>●</td><td class="note">Phase 2: v is the value of the highest accepted ballot, or its own</td></tr>
<tr><td>←</td><td class="msg">Accepted</td><td class="msg">Accepted</td><td>·</td><td class="note">a majority accepted ballot 5: v is chosen</td></tr>
</tbody></table></div>

## Why it is safe

Any two majorities share at least one acceptor. A proposer at a higher ballot first hears
from a majority, and so from one acceptor that accepted the chosen value. The rule of
Phase 2 makes it propose that value again. So every later ballot proposes the chosen value.

An acceptor writes its promise and its vote to disk **before** it answers. An acceptor that
forgets a vote after a restart can let a second value be chosen.

## Liveness

Two proposers can block each other forever: each new Prepare cancels the other's ballot.
paros keeps one stable leader, and a proposer that loses waits before it tries again.

## In paros

- `paros-core`: `Decree` in `crates/lib/paros-core/src/decree.rs`, a sans-IO single decree.
- `cell init` is a single-decree Paxos over the founding machines. They choose the plan of
  the cell (`crates/lib/paros-machine/src/cell_init.rs`).
- The matchmaker set of a tenant is chosen the same way.
