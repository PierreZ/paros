+++
title = "Matchmaker Paxos in one page"
weight = 11
+++

Every page before this one keeps the acceptor set fixed. A real cell must replace machines, so
the acceptor set must change while the cell runs. paros does this with *Matchmaker Paxos*
(Whittaker et al., the paper is in `docs/references/papers/matchmaker-paxos/`). This page gives
the whole idea. The next four pages give one rule each.

## The problem

Paxos is safe because a Phase-1 quorum meets every Phase-2 quorum. That argument talks about
**one** acceptor set. Change the set, and the argument can fail:

1. The acceptors are `{0, 1, 2}`. A leader gets slot 5 chosen with the votes of 0 and 1. It
   stops before it tells any other node.
2. An operator moves the cell to `{2, 3, 4}`.
3. A new leader asks a majority of the new set, 3 and 4. Both answer "nothing accepted at slot
   5". That answer is correct: they were not members.
4. The new leader fills slot 5 with a `Noop`. Slot 5 now holds two chosen values.

The new leader asked the right quorum of the wrong set. It had to also ask a quorum of
`{0, 1, 2}`, but it did not know that set ever existed.

## The idea

A small, separate service remembers which sets exist. A **matchmaker** keeps one durable map,
from a ballot to the acceptor set that ballot uses. A matchmaker holds no log and votes on no
slot. No write and no read on the command path asks it anything. It speaks only when a node
campaigns, when a leader collects old sets, and when the matchmakers themselves are replaced.

The four rules, in the order of the next pages:

1. **[Matchmaking before Phase 1](@/paxos/tricky/matchmaking-before-phase-1.md).** A candidate
   registers its ballot and its set with a matchmaker quorum before its first `Prepare`. The
   answers name every older set, `H_b`. Phase 1 needs a quorum of **each** set in `H_b`.
2. **[Reconfiguration is a round change](@/paxos/tricky/reconfiguration-is-a-round-change.md).**
   A set belongs to one ballot, and nobody edits it. To change the set, the leader campaigns
   again at a fresh ballot with the new set.
3. **[GC and watermarks](@/paxos/tricky/gc-and-watermarks.md).** Old sets pile up. A leader
   proves that no future leader needs them, and then the matchmakers raise a floor that
   forgets them. Only then may an operator stop a removed acceptor.
4. **[Matchmaker generations](@/paxos/tricky/matchmaker-generations.md).** The matchmakers can
   be replaced too. Single-decree Paxos, run by the old matchmakers, chooses the next set of
   matchmakers.

## What it costs, and what it does not

- **Plain Multi-Paxos does not change.** A deployment with no matchmakers never sends a
  matchmaker message. Its campaign goes straight to `Prepare`, and `H_b` is its one set. It
  refuses every reconfiguration.
- **One extra round trip per election**, to the matchmakers. Writes and reads do not pay it.
- **A short stall per reconfiguration**: one matchmaking round trip and one Phase 1. paros does
  not implement the paper's zero-stall overlap of two ballots.
- **One matchmaker set per tenant.** It keeps one registry per journal of the tenant, in one
  store. Each journal has its own history, floor and set in force.

## Where to start in the code

| What | Where |
|---|---|
| The candidate's matchmaking tally | `crates/lib/paros-core/src/matchmaking.rs:162` `Matchmaking` |
| The matchmaker role | `crates/lib/paros-core/src/matchmaker.rs:547` `Matchmaker::step` |
| The campaign: register first, or go to Phase 1 on a plain cell | `crates/lib/paros-core/src/node/election.rs:143` `ColocatedNode::campaign` |
| A reconfiguration | `crates/lib/paros-core/src/node/reconfigure.rs:120` `ColocatedNode::reconfigure` |
| The GC tally | `crates/lib/paros-core/src/collector.rs:289` `Collector::covered` |
| The matchmaker handover | `crates/lib/paros-core/src/matchmaker/reconfigurer.rs:305` `MatchmakerReconfigurer` |

The game plays each rule as a level: [`act4/matchmaking`](../../../play/#act4/matchmaking),
[`act4/reconfigure`](../../../play/#act4/reconfigure),
[`act4/garbage-collection`](../../../play/#act4/garbage-collection) and
[`act4/matchmaker-generations`](../../../play/#act4/matchmaker-generations). The field guide is
[Beyond Multi-Paxos](@/paxos/beyond-multi-paxos.md).
