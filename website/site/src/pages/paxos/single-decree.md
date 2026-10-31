---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: single-decree
title: Single-decree Paxos
description: A group of acceptors chooses one value. Once chosen, it never changes, even when proposers race and machines crash.
---

## The problem

Three acceptors must agree on one value. Two proposers can try at the same time, messages
get lost, and a machine can stop between two steps. A chosen value must never change.

## Step through

A proposer picks a **ballot**. Phase 1 (`Prepare`, `Promise`) claims the ballot and asks
what the acceptors already voted for. Phase 2 (`Accept`, `Accepted`) asks for a vote. A
majority of votes at one ballot chooses the value.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## The rule

**A proposer that hears of a vote in Phase 1 must propose the value of the highest ballot it
heard of, not its own.** Any two majorities share an acceptor, so a later ballot always hears
of the chosen value. An acceptor writes its promise and its vote to disk before it answers.

## In paros

- `paros-core`: `Decree` in `crates/lib/paros-core/src/decree.rs`, a sans-IO single decree.
- `cell init` is a single-decree Paxos over the founding machines. They choose the plan of
  the cell (`crates/lib/paros-machine/src/cell_init.rs`).
- The matchmaker set of a tenant is chosen the same way.
- Two proposers can block each other without end. paros keeps one stable leader, and a
  proposer that loses waits before it tries again.
