# consensus-sim

Deterministic simulation testing of `paros-consensus` (#1197 (consensus-sim walks)): a seeded
random walk over the sans-IO roles, with moonpool-walk (moonpool #554). A seed is a run. The walk
delivers, drops, duplicates and reorders messages, crashes a party and restarts it from its disk,
and makes the moves of the clients and the clocks. After each step, the oracles judge the run.
Test only: `publish = false`, never a dependency of another crate.

## Rules

- **Depend on `paros-consensus` directly**, never on the `paros` facade (rule 3 of
  `crates/lib/paros/AGENTS.md`).
- **Drive the core through its public API only.** The core is never buggified and never mutated.
  The cell is a driver: it honors the driver contract (`ready`, persist the writes, send the
  messages, `advance`) and crashes a party at the two points that the contract allows (before
  the persist, and between the persist and the send).
- **An oracle shares no code with the code it judges** (#909 (oracles that do not judge)). The
  Phase-2 rule (`oracle/quorum.rs`) is written again from the papers. Its one link to the core is
  a test that agrees with `AcceptorConfig::has_phase2_quorum` on every subset of small systems.
- **Every oracle has a control**: a `Mutant` planted in the driver or the disk, and a test in
  `tests/mutants.rs` that the oracle catches it. A new oracle gets its mutant in the same PR.
- **Every flavor names the sites it must reach** (`reach::*`), and `campaign` fails when no seed
  reaches one. A site name is at most 63 bytes: the assertion table keeps no more, and a cut
  name never matches (`campaign::try_campaign` refuses a longer one).
- **The campaign owns the assertion table** (`src/campaign.rs`). moonpool-walk judges every
  sometimes site it sees, the core's `probe!`s too. The campaign installs the table itself, so
  the walk judges no coverage, and the campaign judges only the sites of the flavor. To require
  the core's probes is #1199 (coverage-driven simulation).
- The moonpool rev of `moonpool-walk`, `moonpool-assertions` and `moonpool-tla` here is the rev
  of the `moonpool-assertions` line of `paros-consensus`: one copy of the assertion table.
- **A trace records only the steps that change the model's state** (#1198 (trace checking)). A
  step is recorded when its batch reached the disk; a message sent again is not a step. Each
  event names the action of the model it is, with every argument, so TLC follows the trace with
  no search. A step of the code that the model does not have is written in the trace spec as
  its own step, with the reason in its header, never hidden.
- **Every trace spec has a control**: a mutant that the oracles miss on some seed and TLC
  refuses (`tests/traces.rs`).

## Map

- `campaign.rs` → `try_campaign` / `campaign`: run a `Walk`, then check the expected sites.
- `oracle.rs` + `oracle/` → the safety oracles, independent of the core:
  - `quorum.rs` → `Phase2Rule`: a majority, or `acks` votes in one column of a grid.
  - `ledger.rs` → `Ledger`: the durable votes and promises of every node, each ballot bound to
    the configuration the matchmakers registered for it. Oracles `ONE_VALUE`, `COMMIT_BACKED`,
    `PERSIST_BEFORE_SEND`, `LEARNED_CHOSEN`, `ONE_FOLD`, `PREFIX_SHRINKS`.
  - `reads.rs` → `Reads`: `READ_SEES_WRITES`, a leaderless read sees every write that completed
    before it started.
- `cell.rs` + `cell/` → `Cell`, the model of a deployment of `ColocatedNode`s, and its driver
  loop. `Flavor` (`cell/flavor.rs`) says what the cell holds (acceptors, quorum system, proxy
  leaders, replicas, matchmakers, spares) and which optional moves are on (handoff, reads,
  truncation, reconfiguration). `Mutant` is the bug of a control run: a planted driver bug that
  an oracle or a trace check catches. `cell/sites.rs` holds each party with its disk. The liveness judge (`Cell::converged`): once the faults stop, one leader
  stands with no open round, and every node and replica learned its log.
- `cell/trace.rs` + `cell/trace/` → `Tracer`, `CellEvent`, `Spec`: the steps of a cell walk as
  events, and the trace specs that judge them. `encode.rs` writes every event in one encoding;
  `multi_paxos.rs`, `proxy_leader.rs`, `leader_handoff.rs` and `matchmaker.rs` each say which
  events their model keeps (`Spec::keeps`) and define its constants.
- `decree.rs` → `DecreeModel`: single-decree Paxos (`paros_consensus::decree`), three acceptors
  and two proposers, for each `Phase1Rule`. `DecreeEvent` and `trace` are its steps for
  `specs/tla/decree/DecreeTrace.tla`.
- `trace.rs` → what every trace check shares: `spec` (a trace spec of `specs/tla/`),
  `tlc_or_skip`, `Ranks` (the code's ballots as the model's `1..NB`), the names of nodes.
- `batcher.rs` → `BatchModel`: the batcher and the unbatcher (Compartmentalized Paxos §3.5).

## Tests and seeds

- `tests/flavors.rs` → one walk for each flavor: single decree, Multi-Paxos on three and on five
  (with truncation), the handoff, grid quorums, acceptor partitions, proxy leaders (on a majority
  and on a grid), replicas, leaderless reads, every compartment, the batcher, matchmakers with
  reconfiguration, and matchmakers with every compartment. 60 seeds each in CI.
- `tests/mutants.rs` → each oracle catches its mutant.
- `tests/traces.rs` → model-based trace checking with TLC (moonpool-tla, moonpool #550): four
  short walks of each flavor are a behaviour of the flavor's model, and TLC refuses a seed of
  each control that the oracles pass. Single decree against `decree/`; every cell flavor without
  matchmakers against `multi-paxos/`; the flavors with proxy leaders against `proxy-leader/`;
  the handoff against `leader-handoff/`; the matchmakers against `matchmaker/`. TLC (`tlc`) is
  in the nix dev shell; outside it the tests skip, and in CI a missing TLC fails. Knobs: `TRACE_SEEDS`, `TRACE_FROM`,
  `TRACE_CHAOS`, `TRACE_DUMP`.
- A long run: `MOONPOOL_WALK_SEEDS=1500 cargo nextest run --release -p consensus-sim`. One seed:
  `MOONPOOL_WALK_SEED=17`. The other knobs: `MOONPOOL_WALK_FROM`, `_STEPS`, `_TAIL`.
- A red seed is a finding. Reproduce it alone, find the cause, and fix the core with a red unit
  test first, or fix the oracle with the reason in the commit message.

## Not walked yet

The matchmaker set handover (`MatchmakerReconfigurer`) is judged by
`paros-consensus/src/models/handover.rs` only. Coverage-driven simulation is #1199.

Not traced yet (#1198 (trace checking)):

- The read fence of `reads/`. Its model is a Matchmaker log with the Phase-1 bypass and one
  read; the `reads` flavor has no matchmakers and many reads.
- The floor of `truncation/`. Its fold reads one record for each command, and the trace would
  need the outcome of each write and each `SetLeader` as the model's values.
- The batcher and the replica of `compartments/`.
- `reconfigured-compartments`: matchmakers with a grid for each configuration, proxies and the
  handoff in one trace spec.
- The matchmaker set handover, which no walk runs.

Those walks are checked against `multi-paxos/` only, or not at all.
