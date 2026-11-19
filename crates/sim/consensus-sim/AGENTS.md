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
  the walk judges no coverage, and the campaign judges only the sites of the flavor. The core's
  probes are judged once, over every walk, by `tests/coverage.rs`.
- **Every `probe!` of the core is reached** (#1199 (coverage-driven DST)). `tests/coverage.rs`
  walks every flavor with exploration on and judges the union against the probes that
  `probes::core_probes` reads from the source of `paros-consensus`. There is no list of excused
  probes (Pierre, 2026-11-19: no excuses in simulation, no allow lists). A new probe in the core
  is reached by a walk in the same PR: add the walk move, the flavor or the steer that bends the
  cell to it, never a larger budget alone. Each flavor also makes every move it allows.
- **A steer bends the walk, never the core.** A steer is a choice of the driver that makes a
  rare state likely: an aimed fault (`cell/faults.rs` strikes the split floor), a
  flavor with a small quorum or small pages. It stays a legal move of a real deployment.
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
- `probes.rs` → `core_probes`: every `probe!` site of `paros-consensus`, read from its source.
- `oracle.rs` + `oracle/` → the safety oracles, independent of the core:
  - `quorum.rs` → `Phase2Rule`: a majority, or `acks` votes in one column of a grid.
  - `ledger.rs` → `Ledger`: the durable votes and promises of every node, each ballot bound to
    the configuration the matchmakers registered for it. Oracles `ONE_VALUE`, `COMMIT_BACKED`,
    `PERSIST_BEFORE_SEND`, `LEARNED_CHOSEN`, `ONE_FOLD`, `PREFIX_SHRINKS`.
  - `reads.rs` → `Reads`: `READ_SEES_WRITES`, a leaderless read sees every write that completed
    before it started.
- `cell.rs` + `cell/` → `Cell`, the model of a deployment of `ColocatedNode`s, and its driver
  loop. `Flavor` (`cell/flavor.rs`) says what the cell holds (acceptors, quorum system, proxy
  leaders, replicas, matchmakers, spares, late spares, matchmaker spares), which optional moves
  are on (`Flavor::allows`) and the `Limits` of every party (`Flavor::paged` puts every page
  at its floor, so the paging branches run). `Mutant` is the bug of a control run: a planted driver bug that
  an oracle or a trace check catches. `cell/sites.rs` holds each party with its disk. The liveness judge (`Cell::converged`): once the faults stop, one leader
  stands with no open round, and every node and replica that is not retired learned its log.
  The moves beyond the clients and the clock (#1225 (walk moves for the unreached probes)):
  - `cell/faults.rs` → `Action::Rot`: a disk loses the value of one record and keeps its
    identity (`MemStorage::rot`, CTRL's faulty entry), at most one faulty copy of a slot that no
    node trimmed; a scrub may find it while the node runs (`ColocatedNode::repair_record`). The split
    floor (`Cell::split_floor`): a slot that one node trimmed, that a second node still holds,
    and that a node which did not trim it has not learned chosen. Half the rots, and half the
    compactions of a flavor with rots, strike there: every copy left below the trim point rots,
    and the nodes that hold a copy or trimmed the slot stop. The walk restarts them; a lagging
    node can then win with a promise quorum of faulty copies, its repair blocks, and the trim
    point resolves it. Below a trim point a fault on every copy loses nothing: the trim point
    carries the journal state. Flavor `disk-faults-3` has three acceptors, so a promise quorum
    holds one peer beside the candidate.
  - `cell/retire.rs` → `Action::Retire` (the operator retires a node on an effective GC floor or
    on the floor a beat carried, and never one that a reconfiguration it asked for above the
    floor names), `Action::Rejoin` (a retired node boots again, or a running non-member
    rejoins), `Action::Admit` (a node admits a late spare to its pool; until then a
    configuration that names the spare is outside that node's pool).
  - `cell/handover.rs` → `Action::Handover`: each node's driver holds a
    `MatchmakerReconfigurer`, starts a handover to a target from the matchmaker pool, finishes
    a frozen generation it meets, adopts a chosen set, and republishes it to a matchmaker left
    behind. A sibling journal on the bootstrap set makes a freeze and a bootstrap carry two
    journals, so a small page pages them.
  - `cell.rs` → `Action::MoveRound` (a leader moves a stuck round to another column,
    `ColocatedNode::move_round`), and a client retry inside `Action::Propose` (the same records
    at a position written before).
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
  reconfiguration, matchmakers with every compartment, disk faults (on five and on three), retirement
  and rejoin, the matchmaker set handover, and leaderless reads on matchmakers whose pools lag
  (`lagging-reads`, #1228 (stale read by a lagging reader)). 60 seeds each in CI; `lagging-reads`
  takes 120. The `lagging-reads` walk is steered (`Flavor::lagging_reader`) and is the control of
  #1228: with the reader gated on its belief ballot, seeds 67, 71 and 99 serve a stale read that
  `READ_SEES_WRITES` catches.
- `tests/mutants.rs` → each oracle catches its mutant.
- `tests/coverage.rs` → coverage-driven simulation (#1199): every flavor, and nine of them with
  small pages, walk with exploration on (moonpool-walk's `Walk::explore`): a run that passes a
  site for the first time, or covers a new edge of the core, is branched, and its children
  replay it up to that step and then draw a new schedule. The gate is the union over all walks:
  every core probe reached, every allowed move made. Its budget is its own (`SEEDS` = 300 root
  seeds, `EXPLORE` = 10), and CI runs it as it is. Each walk runs in a process of its own (the
  ignored test `one_walk`, started again from the same binary), one per CPU (`COVERAGE_JOBS`):
  a process has its own branch counters, so a walk branches the same way whatever runs beside
  it. Branch coverage needs the instrumented build that CI's `coverage` job runs:
  `RUSTC_WRAPPER=$PWD/scripts/sancov-rustc.sh SANCOV_CRATES=paros_consensus cargo nextest run
  --release --target-dir target/sancov -p consensus-sim --test coverage --no-capture`. Without
  it, `moonpool_explorer::sancov_edges_covered_live` returns 0 and only the probes steer. The
  `test` job of CI leaves the gate out (it is too long in debug). The `Coverage nightly`
  workflow runs it with 900 root seeds from a first seed that moves with the date.
- `tests/traces.rs` → model-based trace checking with TLC (moonpool-tla, moonpool #550): four
  short walks of each flavor are a behaviour of the flavor's model, and TLC refuses a seed of
  each control that the oracles pass. Single decree against `decree/`; every cell flavor without
  matchmakers against `multi-paxos/`; the flavors with proxy leaders against `proxy-leader/`;
  the handoff against `leader-handoff/`; the matchmakers against `matchmaker/`. TLC (`tlc`) is
  in the nix dev shell; outside it the tests skip, and in CI a missing TLC fails. Knobs: `TRACE_SEEDS`, `TRACE_FROM`,
  `TRACE_CHAOS`, `TRACE_DUMP`.
- A long run: `MOONPOOL_WALK_SEEDS=1500 cargo nextest run --release -p consensus-sim`. One seed:
  `MOONPOOL_WALK_SEED=17`. The other knobs: `MOONPOOL_WALK_FROM`, `_STEPS`, `_TAIL`, and
  `_EXPLORE` (the runs each root seed may add). A failure in a branched run prints its timeline:
  `MOONPOOL_WALK_TIMELINE=17@120:9e37…` runs it again alone.
- A red seed is a finding. Reproduce it alone, find the cause, and fix the core with a red unit
  test first, or fix the oracle with the reason in the commit message.

## Not traced yet

Not traced yet (#1198 (trace checking)):

- The read fence of `reads/`. Its model is a Matchmaker log with the Phase-1 bypass and one
  read; the `reads` flavor has no matchmakers and many reads.
- The floor of `truncation/`. Its fold reads one record for each command, and the trace would
  need the outcome of each write and each `SetLeader` as the model's values.
- The batcher and the replica of `compartments/`.
- `reconfigured-compartments`: matchmakers with a grid for each configuration, proxies and the
  handoff in one trace spec.
- The flavors `disk-faults`, `disk-faults-3`, `retirement` and `handover`: no model has faulty entries, the
  retirement check or the matchmaker set handover.

Those walks are checked against `multi-paxos/` only, or not at all.
