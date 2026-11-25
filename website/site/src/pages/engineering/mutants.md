---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
title: Mutants and assertions
description: cargo-mutants changes the core one edit at a time, and a test must fail for each edit. Hard assertions, on in release, stop the process at a broken state. The score below is of a dated run.
---

## The problem

A green suite says that the code passes the tests. It does not say that the tests would see a
bug. A test suite that passes with a line of the core changed does not test that line.

## Mutation testing of the code

[cargo-mutants](https://mutants.rs/) makes one small edit to the core at a time: a function that
returns a default, a `>` that becomes `>=`, a guard that becomes `true`. For each edit, a test
must fail. An edit that every test passes is a **survivor**, and it gets the test that sees it.

- **Each night**, the `Mutants nightly` workflow runs the edits of the lines that main changed
  since the last green night, in `paros-consensus`. It has two passes:
  the unit tests and the walks, then the TLC trace checks on the edits that the first pass
  missed. A survivor fails the night, and the next night judges those lines again. A pull request
  runs no edit.
- **Each week**, the `Mutants` workflow runs every edit of the core in 48 shards and fails on any
  survivor.
- A timeout counts as caught, and so does a test that the runner stops when it is short of memory.
  An exclusion is only for an edit that changes nothing, with its reason. There is no allow list.

[`scripts/mutants.sh`](https://github.com/PierreZ/paros/blob/main/scripts/mutants.sh) runs it.

## The score, as of a dated run

The first complete run of every shard: Mutants run
[44867488746](https://github.com/PierreZ/paros/actions/runs/44867488746) on `c2a456a`,
2026-11-21.

<div class="strip" aria-label="Mutants run 44867488746">
  <div><span class="k">Viable mutants</span><span class="v">2,466</span></div>
  <div><span class="k">Caught</span><span class="v">2,160</span></div>
  <div><span class="k">Score</span><span class="v">87.5 %</span></div>
  <div><span class="k">Survivors</span><span class="v">306</span></div>
</div>

The first pass caught 2,151 and the trace checks caught 6 more; 3 were timeouts. The survivors
are in five groups of #1282 (kill the surviving mutants of paros-consensus). After that run, the
pull requests of groups B (#1356, the matchmaker and the membership), D (#1357, the replicas,
the proxy leader, the reads and the contract) and E (#1358, the invariant helpers, counters and
view of the node) merged, each with no survivor left in its group. Groups A and C are open
(#1354, #1355). The next full run gives the next score; this page shows the last complete one.

## Planted bugs in the walks

The walks have their own mutants: a bug planted in the driver of a walk, never in the core. Each
oracle and each trace check must fail on its bug. For example, `SkipPersist` sends before the
disk write (`PERSIST_BEFORE_SEND` fails), `ForgetVotes` loses votes in a crash (`ONE_VALUE`
fails), and `RewindHandoff` empties the payload of a handoff (the trace check of
`LeaderHandoffTrace.tla` refuses it). The [safety case](../) names the mutants of each claim.

## Hard assertions

`paros-consensus` stops at a broken state. It follows TigerBeetle's Tiger Style:

- `assert!`, on in release. There is no `debug_assert!` in the crate.
- Preconditions, postconditions and invariants, about two assertions for each function on
  average. A property is checked where it is written and again where it is read back (at persist
  and at boot). The negative space is asserted too.
- `ColocatedNode::assert_invariants` runs at boot and at each public entry that changes state.
- Each assertion is an oracle for free in each test, each walk and each mutant run.

An assertion never judges what a correct peer or client can send. A peer can be late, send a
message two times or restart, and a client can send any request: each handler ignores such a
message or answers with a result. A message that breaks a Paxos rule (two commands at one slot and
ballot) is a detected safety violation, and its assertion names the rule.
