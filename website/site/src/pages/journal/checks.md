---
layout: ../../layouts/Doc.astro
kicker: Journal
section: journal/
status: journal
title: How it is checked
description: Every technique that checks the journal v1, in one page. A TLA+ model before the code, then three judges on the same code, then checks that test the checks. Each line names where it runs.
---

## The problem

A journal must keep each acked write, in order, across crashes, lost messages, a new leader,
a new grid and a controller that is wrong. One technique does not find every bug. A model checks
the design, not the code. A test checks the case that its author thought of. A walk finds the
orders of events that nobody wrote down, but only if its judge is right. So the journal uses many
techniques, and each one covers a gap of another. Their rules are in §J7 of
[`07-verification.md`](https://github.com/PierreZ/paros/blob/main/docs/journal/07-verification.md).

<div class="strip" aria-label="The checks of the journal in figures">
  <div><span class="k">Before code</span><span class="v">TLA+</span></div>
  <div><span class="k">Walks</span><span class="v">7</span></div>
  <div><span class="k">Trace specs</span><span class="v">4</span></div>
  <div><span class="k">Allow lists</span><span class="v">0</span></div>
</div>

## The techniques at a glance

| Technique | What it finds | Where it runs |
|---|---|---|
| [A TLA+ model for each rule](#models) | a rule that is not safe, or not necessary | `spec` jobs of the `TLA` workflow |
| [The composed model](#composed) | two safe rules that are not safe together | `spec` jobs |
| [Guard mutants of the models](#guard-mutants) | a guard that no check needs | `TLA mutants`, each week |
| [Proofs of the core below](#proofs) | a gap of the small instance that TLC checks | `proofs` jobs |
| [Pure roles](#pure-roles) | a run that cannot replay, a role that does I/O | the layer checks of `checks` |
| [Seeded walks with faults](#walks) | an order of events that breaks the code | `test` job |
| [Oracles with no shared code](#oracles) | a wrong answer, judged from the contract | each step of each walk |
| [A planted mutant per oracle](#planted) | an oracle that cannot fail | `test` job |
| [Reach sites and probes](#reach) | a walk that never meets a rare case | `test` job |
| [Trace checks against the model](#traces) | a step of the code that the model does not have | `traces` jobs or `test` job |
| [Edits of recorded traces](#edits) | a trace check that accepts a wrong trace | `trace-edits` job |
| [The live trace check](#live) | a divergence deep in a long run | `test` job, and `Journal live` each night |
| [A second `decide`](#decide) | a wrong decision of the controller | `test` job |
| [cargo-mutants on the code](#mutants) | a line that no test checks | `mutants` job, on each pull request |
| [Hard assertions](#assertions) | a broken state, at the line that made it | every test, walk and mutant run |
| [Contract suites of the backends](#contracts) | a backend that breaks the IO contract | `test` job |
| [Layers, wasm and examples](#static) | a pure crate that does I/O, an API that does not compose | `checks` and `wasm` jobs |
| [The rules of the work](#rules) | a red check that someone makes green with no fix | review of each pull request |

<h2 id="models">A TLA+ model for each rule</h2>

**No code before its model.** Each journal rule has a TLA+ model in
[`specs/tla/`](https://github.com/PierreZ/paros/tree/main/specs/tla) before a line of Rust is
built on it. TLC explores every state of a small instance. Each model has three kinds of check:

- **pass**: the rule holds in every state.
- **control**: the same model with one rule off. TLC must find the bug. So the rule is necessary.
- **reach**: TLC must find a state that the passing checks must cover. So a pass is never a pass
  over an empty set of states.

§J7.1 maps each rule of the journal to its model: the flight rule (`flight-rule/`), the
multi-writer mode (`multi-writer/`), the seal (`seal-chain/`, `seal-ctrl/`), the segment records
across a handover (`segment-records/`), quorum everywhere (`quorum-everywhere/`), damage and
repair (`ctrl-grid/`, `ctrl-segments/`), re-replication (`re-replication/`), the controller
(`grid-controller/`, `health/`) and more. When there is a doubt about safety, the answer is a TLA+
check, not a paragraph. More on how a model is checked:
[TLA+ models](../../engineering/tla/).

<h2 id="composed">The composed model</h2>

Two rules can each be safe and still break together. So
[`journal-state/`](https://github.com/PierreZ/paros/tree/main/specs/tla/journal-state) composes
them: three segments, Paxos inside each one, the seal, register writes that are partial or lost,
the boundary state, a replica set per segment, the fold with `Below` and `Resolve`, retire and
trim. **Its state is the disk**: each acceptor has a memory, a queue of writes and a disk, and it
can crash between two writes. Its controls show the write orders that are not safe, for example a
vote written before the promise that it raises (`order_vote_first`). §J7.2 lists what it decided.

<h2 id="guard-mutants">Guard mutants of the models</h2>

A model that still passes with one of its guards removed checks nothing about that guard. Each
week the `TLA mutants` workflow sets each guard of each action to `TRUE`, one at a time, and runs
the checks of the folder
([`scripts/mutate-tla.py`](https://github.com/PierreZ/paros/blob/main/scripts/mutate-tla.py)).
One check must fail. A survivor fails the run: remove the guard, or add the check that needs it.
Each `spec` job also checks that each guard mutant of its folders parses.

<h2 id="proofs">Proofs of the core below</h2>

The journal runs on the Paxos of `paros-consensus`. TLC checks a small instance; a proof covers
every size. The single decree and Multi-Paxos each have an inductive invariant `Inv`, and TLAPS
proves at every size that `Inv` holds at the start, that each step keeps it, and that it implies
agreement (`DecreeProof.tla`, `MultiPaxosProof.tla`; LS4 checks the temporal last step).
`specs/tla/PROVED.sha256` pins the proved files, and CI fails when one changed and the pin was not
written again. The journal models have no proof yet: they are checked by TLC on small instances.

<h2 id="pure-roles">Pure roles</h2>

Every role of the journal is a pure state machine: a deterministic step, no clock, no random
number, no I/O. The roles and their driver follow the flight rule (§J4.2): each write to disk
has an id, and each message names the write that it waits on. So every input can be recorded, and
a run replays from its seed. The flight rule has its own model,
[`flight-rule/`](https://github.com/PierreZ/paros/tree/main/specs/tla/flight-rule). The code of
a role is never buggified: a rare path is reached only through what a driver may do. Without this,
none of the techniques below could run the code that ships.

<h2 id="walks">Seeded walks with faults</h2>

[`journal-sim`](https://github.com/PierreZ/paros/tree/main/crates/sim/journal-sim) runs seeded
random walks (moonpool-walk) over the real code. A seed is a run, so a red seed replays. The
faults stay inside the contract of the code: a crash loses only the writes that no fsync covered,
the disk fails a commit before or after the write, and messages are lost, repeated and reordered.

| Walk | What it drives | Its oracles |
|---|---|---|
| fold | the journal fold over a chain of segments: three writers, resends across seals, multi-writer and back, truncate, `Resolve`, a replica that refolds | `FOLD`, `VIEW`, `RESOLVE`, `REFOLD` |
| register | the matchmaker register on four machines, across handovers of the set, with crashes | `CARRIED`, `ACTED` |
| roles | acceptors, leaders and segment replicas with disks, three proposers, reshapes, seals, retires | `AGREE`, `SEALED`, `FOLD`, `VIEW`, `BOUNDARY`, `RESOLVE` |
| driver | three processes, each a `Driver` of `paros-journal-io`, with a disk that fails commits | `AGREE`, `FLIGHT`, `ANSWER` |
| client | two programs with the pure client, held and lost answers, seals, reads | `WRITTEN`, `ONCE`, `REFUSED`, `LED`, `TAIL`, `PAGE` |
| controller | controllers that race, crash and get deposed, lost requests and replies | `FENCED`, `BOUNDED_RESHAPE_RATE`, `COST_BOUND`, `NO_OSCILLATION`, `BOUNDED_IN_FLIGHT`, `CONVERGES` |
| whole journal | two journals on nine machines in three zones, their controllers and a client, on the in-memory IO; crashes, the loss of a zone for a while, disk faults | `AGREE`, `SEALED`, `FLIGHT`, `ANSWER`, `ONCE`, `ACKED`, `ALERTS` |

At the end of the whole-journal walk, each journal must converge: a controller holds the term,
the newest segment is in force, the older ones are retired, and every replica of the newest
segment holds the whole log. More on the walks: [Simulation](../../engineering/simulation/).

<h2 id="oracles">Oracles with no shared code</h2>

An oracle that uses the code it judges agrees with the bug. So each oracle of `journal-sim`
shares no code with the journal. `Truth` folds the whole log again from §J2, with no segment;
the fold walk judges every answer against it. The client walk reads the log that the walk keeps,
never the client. The oracles judge every step: no case is skipped because it is rare.

<h2 id="planted">A planted mutant per oracle</h2>

An oracle that never fails proves nothing. Each oracle has a bug planted for it, in the driver of
the walk, never in the code under test, and a test proves that the oracle catches it. For
example, `SendBeforePersist` makes a register member reply before its write is durable (`CARRIED`
fails), `LyingDisk` reports a commit that is not on disk (`FLIGHT` fails), and `TakenAsWritten`
takes a `Taken` answer as written. A new oracle gets its mutant in the same pull request.

<h2 id="reach">Reach sites and probes</h2>

A walk that never meets a rare case judges nothing about it. Each walk names its reach sites: a
seal lost, a `Noop` below the start, an old vote chosen again in a new grid, a third segment,
`Below`, `Resolve`, and more. The campaign of each walk must reach every site, or the test fails.
The controller crates put a `probe!` on each branch of `decide` and of the role, and a test
requires every probe. A site that no walk reaches gets a walk move, never only a larger budget.

<h2 id="traces">Trace checks against the model</h2>

The model and the code can disagree. So each walk records its events, and TLC checks that the
trace is a run of the TLA+ model (moonpool-tla). Four trace specs:

| Walk | Trace spec |
|---|---|
| fold | `journal-state/FoldTrace.tla` |
| roles (the default grid) | `journal-state/RolesTrace.tla` |
| register | `segment-records/RegisterTrace.tla` |
| controller | `grid-controller/ControllerTrace.tla` |

A trace spec extends the model that TLC checks and defines no step of its own. So a step of the
code that the model does not have fails the check, and becomes a step of the model, with its
check, in the same pull request. The roles trace check found three such steps. Each trace spec
also names the rules that its checked walks must meet (a lost seal, a `Resolve` answered both
ways, two seals), and each has a control that the oracles pass and TLC refuses (for example
`BoundaryBeforeDurable`: a boundary write before its batch is durable). A test that needs TLC
fails when TLC is missing; it does not skip.

<h2 id="edits">Edits of recorded traces</h2>

A trace check that accepts every trace checks nothing. So each trace spec must refuse at least 20
named edits of its recorded walks (a dropped event, a changed parameter, two events swapped), at
the edited event or after it, never before.

<h2 id="live">The live trace check</h2>

One TLC run follows at most 65,535 states of one behaviour, and a check after the run sees a
divergence only when the run ends. moonpool-tla's `Live` checks the events while the run writes
them, in chunks; each chunk starts from every state where the chunk before it ended. In CI, one
fold walk of about 7,000 events and 199 seals is checked live, and its control is stopped at its
first wrong answer. Each night, the `Journal live` workflow checks one fold walk with no end for
four hours.

<h2 id="decide">A second `decide`</h2>

The controller's decision is a pure function, so it is checked on every small view: about 390
thousand views (up to 3 columns, up to `F = 2`, zero, three or four zones, every count of up,
suspect and down members, mixed signals). Each decision must match a reference `decide` written
again from §J6.2 with no code of `paros-journal-control` (`SAME_AS_REFERENCE`), and must pass the
oracles `NO_UNSAFE_PLAN`, `FENCED`, `NO_STALE_PLAN`, `NO_SUSPECT_REPLACED`, `IDEMPOTENT` and
`MONOTONE`, each with its mutant.

<h2 id="mutants">cargo-mutants on the code</h2>

A green suite does not say that the tests would see a bug. On each pull request, the `mutants`
job makes one small edit at a time to the changed lines of `paros-journal`,
`paros-journal-control` and `paros-journal-io`, and a test must fail for each edit. For the
journal core, the judges are its unit tests and the walks, then the trace checks on the edits that
the walks missed. A survivor fails the job, and gets the test that sees it in the same pull
request. More: [Mutants and assertions](../../engineering/mutants/).

<h2 id="assertions">Hard assertions</h2>

The pure crates use `assert!`, on in release, with no `debug_assert!`. A property is checked where
it is written and again where it is read back. Each assertion is an oracle for free in each test,
each walk and each mutant run. An assertion never judges what a correct peer or client can send:
that is a result, never a panic.

<h2 id="contracts">Contract suites of the backends</h2>

A program uses the journal through four traits of `paros-journal-io`: `Storage`, `Transport`,
`Clock` and `Register`. Contract suites (`storage`, `storage_reopen`, `transport`, `clock`)
check that a backend keeps the contract. A suite certifies the API only: crash safety is claimed
only on a disk that can inject faults, as in the walks.

<h2 id="static">Layers, wasm and examples</h2>

- **No I/O in a pure crate.** `scripts/check-core-deps.sh` fails when a pure crate depends on a
  runtime, a storage engine or a network crate. `scripts/check-layers.sh` fails on a dependency
  that points up or sideways, and on a fault switch in a pure crate.
- **It builds for the browser.** Each pure crate builds for `wasm32-unknown-unknown`, and the labs
  of these pages run the real `paros-journal`.
- **The examples are tests.** Each example of `paros-journal`, `paros-journal-io` and
  `paros-journal-control` asserts what it shows, and CI runs each one.
- **Clippy pedantic**, with no lint allowed.

<h2 id="rules">The rules of the work</h2>

The techniques work only if nobody bends them to make a red check green.

- **No exception in simulation**: no excuse, no allow list, no skip, no weaker check, no retry. A
  failure of a walk is a finding: fix the code, or fix the walk when it does a thing that the
  contract does not permit
  ([`AGENTS.md`](https://github.com/PierreZ/paros/blob/main/AGENTS.md#absolute-rule-no-exception-in-simulation)).
- **A flaky check is a finding.** A check is never run again until it is green.
- **A lesson becomes a check.** When a machine could have caught a mistake, the fix adds the check.
- **The builder never grades its own code.** An independent reviewer reads each pull request
  against [`REVIEW.md`](https://github.com/PierreZ/paros/blob/main/REVIEW.md) and hunts races,
  edge cases and broken invariants.

## What is not there yet

- The coverage gate over the journal's probes, as the core has (the next rule group of #1326
  (the journal roles over the core)).
- CTRL faulty votes on the acceptors' disks in the roles walk: the leader's blocked slot has unit
  tests only.
- A trace check for the driver walk, and for the roles walk on grids of two columns.
- The permanent loss of a machine in the whole-journal walk: it needs re-replication (#1314).
- A moonpool-sim run of the whole journal, with the adapter: after v1, in #1417 (the moonpool
  adapter on the generic host).
- Proofs of the journal models: they are checked by TLC only.
