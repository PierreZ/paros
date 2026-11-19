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
- The moonpool rev of `moonpool-walk` and `moonpool-assertions` here is the rev of the
  `moonpool-assertions` line of `paros-consensus`: one copy of the assertion table.

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
  truncation, reconfiguration). `Mutant` is the bug of a control run. `cell/sites.rs` holds each
  party with its disk. The liveness judge (`Cell::converged`): once the faults stop, one leader
  stands with no open round, and every node and replica learned its log.
- `decree.rs` → `DecreeModel`: single-decree Paxos (`paros_consensus::decree`), three acceptors
  and two proposers, for each `Phase1Rule`.
- `batcher.rs` → `BatchModel`: the batcher and the unbatcher (Compartmentalized Paxos §3.5).

## Tests and seeds

- `tests/flavors.rs` → one walk for each flavor: single decree, Multi-Paxos on three and on five
  (with truncation), the handoff, grid quorums, acceptor partitions, proxy leaders (on a majority
  and on a grid), replicas, leaderless reads, every compartment, the batcher, matchmakers with
  reconfiguration, and matchmakers with every compartment. 60 seeds each in CI.
- `tests/mutants.rs` → each oracle catches its mutant.
- A long run: `MOONPOOL_WALK_SEEDS=1500 cargo nextest run --release -p consensus-sim`. One seed:
  `MOONPOOL_WALK_SEED=17`. The other knobs: `MOONPOOL_WALK_FROM`, `_STEPS`, `_TAIL`.
- A red seed is a finding. Reproduce it alone, find the cause, and fix the core with a red unit
  test first, or fix the oracle with the reason in the commit message.

## Not walked yet

The matchmaker set handover (`MatchmakerReconfigurer`) is judged by
`paros-consensus/src/models/handover.rs` only. Model-based trace checking against the TLA+
models is #1198, and coverage-driven simulation is #1199.
