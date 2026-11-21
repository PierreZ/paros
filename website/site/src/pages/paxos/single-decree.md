---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: single-decree
lab: true
title: Single-decree Paxos
description: A group of acceptors chooses one value. Once chosen, it never changes, even when proposers race and machines crash.
---

## The problem

Three acceptors must agree on one value. Two proposers can try at the same time, messages
get lost, and a machine can stop between two steps. A chosen value must never change.

## The story

A [proposer](../#leader) picks a **[ballot](../#ballot)**. Phase 1 (`Prepare`, [`Promise`](../#promise)) claims the ballot and asks
what the [acceptors](../#acceptor) already voted for. Phase 2 (`Accept`, `Accepted`) asks for a vote. A
majority of votes at one ballot [chooses](../#chosen) the value.

The story shows one idea per screen, recorded from the real `paros-consensus`. Before some
screens play, the story asks what will happen: answer, then watch. At the end, choose what
goes wrong. "Show each message" opens the same run one message at a time.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## Your turn

Now you are the network. Deliver or drop each message, and reach the goal of each exercise.
`paros-consensus` runs in this page and decides everything else. The second exercise uses a
build with one rule off, and the page says which one.

<div data-lab="vote-not-chosen two-values duel"><p class="aside">The exercises need JavaScript.</p></div>

## The rule

**A proposer that hears of a vote in Phase 1 must propose the value of the highest ballot it
heard of, not its own.** Any two majorities share an acceptor, so a later ballot always hears
of the chosen value. An acceptor writes its promise and its vote to disk before it answers.
The run "Break the rule" shows why: an acceptor loses its disk and answers again, and a
second value is chosen. paros refuses to boot a member whose disk is empty (`BootKind`).

## In paros

- `paros-consensus`: `Decree` in [`crates/lib/paros-consensus/src/decree.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/decree.rs), a sans-IO single decree.
- The matchmaker handover chooses the next matchmaker set with the same single decree
  (`MatchmakerReconfigurer`).
- In the journal service of [`v0.2.0-alpha.1`](https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1), `cell init` was a single-decree Paxos
  over the initial members ([`paros-machine/src/cell_init.rs`](https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-machine/src/cell_init.rs)).
- Two proposers can block each other without end. paros keeps one stable leader, and a
  proposer that loses waits before it tries again.
- Consensus: [paros-consensus](../../consensus/#flavors) lists every flavor with the type that builds it and an example to run.
