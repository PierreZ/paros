---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
title: TLA+ models
description: No code before its model. TLC checks each rule of paros on a small instance, each rule has a control that must fail without it, and each guard of each model is mutated to test the checks.
---

## The problem

A Paxos rule is easy to state and easy to get wrong. An argument in prose can miss one order of
messages, one crash between two writes, or one case that a second rule also has to cover. A test
of the code finds only the orders that it runs.

## The rule

**No code before its model.** Each Paxos flavor and each journal rule has a TLA+ model in
[`specs/tla/`](https://github.com/PierreZ/paros/tree/main/specs/tla) before a line of Rust is
built on it. A change to the protocol of a paper is a proposal, and it needs a model first. When
there is a doubt about safety, the answer is a TLA+ check, not a paragraph. The rules are in
[`AGENTS.md`](https://github.com/PierreZ/paros/blob/main/AGENTS.md).

## How a model is checked

A folder holds one model: the spec, `MC.tla` for the constants, one `.cfg` file for each TLC run,
and `checks.txt`. Each line of `checks.txt` names a run and the result it must give:

- **pass**: TLC explores every state of the instance and finds no error.
- **violation**: TLC must find that the invariant of the file is false. A **control** turns one
  rule off, so the check shows that the rule is necessary. A **reach** check asks TLC to find a
  state that the passing checks must cover, so a pass is never a pass over an empty state space.

```text
nix develop .#tla --command scripts/check-tla.sh          # the checks that CI runs
nix develop .#tla --command scripts/check-tla.sh reads    # one folder
```

CI runs the folders that a change can break, packed into a few jobs. A `long` line runs only with `--all`; a long line with a time
bound stops at that bound and is a bounded run, never a proof. The README of
[`specs/tla/`](https://github.com/PierreZ/paros/blob/main/specs/tla/README.md) lists every model
with its checks and its controls. The [safety case](../) names, for each claim, the model, the
invariant and some of its controls.

## The state counts of the course

Each lesson of the [Paxos course](../../paxos/#course) names one TLC run. The counts come from
TLC's output (TLC 2.19, 2026-11-19), with `-workers auto -deadlock`.

| Model and run | What it checks | Distinct states |
|---|---|---|
| `decree/` `handover` | two values are never chosen, with three acceptors, two values and three ballots | 17,153 |
| `grid/` `grid` | every Phase 1 quorum meets every Phase 2 quorum, in every grid of up to nine acceptors | 69 |
| `multi-paxos/` `majority` | no two commands are chosen at one slot, with three acceptors, two slots and two ballots | 362,774 |
| `reads/` `reads_plain` | a read sees every write that was acknowledged before it, with four acceptors and two ballots | 663,441 |
| `multi-paxos/` `partitions_down` | the last leader learns every slot that it proposed, on a 3 x 2 grid with one acceptor down in each partition | 176,882 |
| `compartments/` `batch` | each write of a batch gets one range of the log, in order, with a leader change and a batcher that stops | 14,154 |
| `matchmaker/` `matchmaker` | two values are never chosen across a reconfiguration, with three matchmakers and two ballots | 1,421,865 |

## Inductive invariants

TLC checks a small instance. An inductive invariant `Inv` closes part of that gap: `Inv` holds
at the start, and every step from a state where `Inv` holds goes to a state where it holds. Then
`Inv` holds at every size and every length of run (#1307 (inductive invariants, then proofs)).

- **`decree/`**: the invariant of Lamport's proved `Paxos.tla`. TLC starts from every state of
  the instance where `Inv` holds and takes one step. The control `inductive_ignore` (a proposer
  that ignores the reported votes) breaks `Inv` in one step. TLAPS proves it at every size
  (`DecreeProof.tla`, 692 obligations): `Inv` holds at the start, every step keeps it, and it
  implies `Agreement`. LS4, the temporal backend of TLAPS, checks the last step: the TLA rule
  that joins the three into `Spec => []Agreement`.
- **`multi-paxos/`**: the decree invariant for each slot, and the rules of the log, the
  allocator, the recovery of a new leader and the disk. The states are too many to list, so
  TLC draws 100,000 of them with a fixed seed. TLAPS proves it at every size, for every number
  of slots and every quorum system where each Phase-1 quorum meets each Phase-2 quorum
  (`MultiPaxosProof.tla` and the five modules it extends, 2,289 obligations), the last step
  with LS4 too.

The Lean proofs are a later step of #1307 and are **open**.

## Guard mutants: the checks are checked

A model that still passes when one of its guards is removed checks nothing about that guard.
[`scripts/mutate-tla.py`](https://github.com/PierreZ/paros/blob/main/scripts/mutate-tla.py)
sets each guard of each action to `TRUE` in a copy of the specs and runs the `ci` checks of the
folder. One check must give another result than its line says. A survivor means the guard is not
necessary, or a property is missing: remove the guard or add the check. There is no allow list
(#1311 (mutate spec guards)).

The first runs, on PR #1338 (2026-11-21), judged 839 of the 1,187 guards: 434 caught by a
failing check, 101 caught because a check ran past three times its time, and **304 survived**.
#1345 (guards that survive mutation) lists each survivor; the weekly `TLA mutants` workflow
stays red until the list is empty. The trace specs (`*Trace.tla`) are not mutated by this script:
the [edits of recorded traces](../simulation/#each-walk-is-checked-against-its-model) test them.
