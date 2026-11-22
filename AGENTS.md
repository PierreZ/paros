# paros

Learning project: the Paxos consensus algorithm in Rust. WIP, not for production.

## ABSOLUTE RULE: no exception in simulation

This rule has no exception. It is more important than every other rule in this file (Pierre,
2026-11-19).

**The goal of simulation is to bend the software enough to trigger its rough edge cases,
without breaking it, so that the work makes progress.**

- **No excuse.** A failure in a simulation run is a finding. Find its cause and fix the code,
  or fix the walk if the walk does a thing that the contract of the code does not permit. Never
  accept the failure.
- **No allow list.** Do not write a list of failures, seeds, messages, probes, oracles, flavors
  or faults that a run may ignore. Do not write a list of probes that a run need not reach.
- **No skip.** An oracle judges every case. Do not skip a case because it is rare, hard or
  "not possible". A case that is not possible fails the oracle. A simulation test never skips
  itself because a tool is missing: it fails. Do not add `#[ignore]` to a simulation test,
  except to a helper that another test runs.
- **No weaker check.** Do not make an oracle less strict, a budget larger, or a fault less
  frequent to make a red run green. Do not turn off a fault because the code does not survive
  it. A probe that a walk does not reach gets a walk move, a flavor or a steer. Only an oracle
  that asks for more than the contract of the code may change, and only to match that contract,
  with Pierre's agreement.
- **No retry.** Do not run a red seed again until it is green.
- **Bend, do not break.** A fault stays inside the contract of the code (for example, the disk
  keeps what an fsync covered). A fault that breaks the contract tests nothing.

If you think that a case needs an exception, stop and ask Pierre. Do not add the exception.

**Where paros is now (2026-11-21).** The repository holds the consensus core and its proofs:
`paros-consensus` (the sans-IO Paxos roles and their tests), the `paros` facade, the TLA+
models of `specs/tla/`, the Paxos and Consensus pages of the website and their wasm engine. The journal v1 has
its pure core, `paros-journal`, and its walk crate, `journal-sim` (#1324 (paros-journal pure
core)); its first roles (segments, the seal and writes; #1326) are built, and so are its IO
traits, its pure `Host` and its generic driver, `paros-journal-io` (#1329). The journal
service that sat above the core (`parosd`, `parosctl`, `paros-bench`, the library layers, the
simulation `paros-sim`) was removed on 2026-11-19. It is at the git tag `v0.2.0-alpha.1`, and
`docs/archive/v0.2.0-alpha.1/README.md` has one recap per removed crate: what it did, its key
files, the ideas worth reusing, and how to read it at the tag. Read the recap before you copy
an idea back.

**The order of work.** First, a TLA+ model for every Paxos flavor that `paros-consensus` builds
(single decree, Multi-Paxos, leader handoff, matchmakers, grid quorums, proxy leaders,
replicas, leaderless reads), and a full understanding of each paper. No code is built on a
flavor before its model. Done so far: single decree (`specs/tla/decree/`), plain Multi-Paxos
(`specs/tla/multi-paxos/`: Phase 1 recovery, the allocator frontier, the `Noop` gap fill,
commit and catch-up, restarts; #1185 (rework 1.14)), the cooperative leader handoff
(`specs/tla/leader-handoff/`: one hop, the frontier guard, the durable frontier, the pending
rounds proposed again, and the beat guard that rework 2.7 adds; #1187 (rework 2.8)), grid quorums
(`specs/tla/grid/`, run by `config-register/`, `reads/`, `multi-paxos/`, `leader-handoff/`, `compartments/`, `proxy-leader/`, `quorum-everywhere/`, `degraded-reshape/` and `health/`; #1102 (rework 1.13)), Matchmaker
Paxos (`specs/tla/matchmaker/`: matchmaking, Phase 1 over every configuration of `H_b`, a
reconfiguration as a round change, GC and retirement; #1200 (TLA+ model Matchmaker Paxos)), the
matchmaker set handover (`specs/tla/handover/`: a target that the caller names, a size that
only a resize record changes, the floor and the register across the handover; #1097 (rework
1.8); `specs/tla/handover-chain/`: three generations, clients that reach one member at a time,
and the two fences of the state transfer; #1230 (rules no check tests)), the batcher, the replica and the read replica (`specs/tla/compartments/`: each write of a
batch gets one range, the walk in slot order, the refold at boot, the read replica's leaderless
read, the held answers of a deposed leader (#1232 (held writes)); #1190 (rework 6.10)), the proxy leader (`specs/tla/proxy-leader/`: a `Commit` only on a
Phase-2 quorum of the configuration that the round's `Accept` carried, a round that moves to
another proxy when its proxy is gone; #1188 (rework 2.9)), leaderless reads
(`specs/tla/reads/`: the read basis of a leadership that won, the fence of a `PreReadAck` on the
promise with the old belief fence as a control, a lagging pool, a lagging reader gated on its promise (#1228 (stale read by a lagging reader)), a restart and the refusal of a
rebooted node; #1016 (model of leaderless reads), #1099 (rework 1.10)), truncation on plain
Multi-Paxos (`specs/tla/truncation/`: the decided `Truncate` and the floor as a fold of the
journal, the floor guard of Phase 1 and the retry of a refused laggard, the trim-point jump that
keeps the promise, the vote watermark over the floor, the faulty entries of CTRL with the
tri-state tally and the repair probe; #1017 (truncation model), #1216 (truncation model, part
2)), truncation across a reconfiguration onto empty stores (`specs/tla/truncation-reconfig/`: the
GC of `node/gc.rs` forgets the old configuration only once a Phase-2 quorum of the new one holds
the prefix up to the election fence, with a control that forgets it at the win and a control
that counts an empty new member; #1218 (truncation model, part 3)), the Phase-1 bypass of a
reconfiguration (`specs/tla/zero-stall/`: the bypass check, the decision hold and the read prefix that it gates (#1270 (bypass read order)), GC after the fence
Phase 1, the handoff of a ballot that bypassed, two bypasses in a row; #1018 (zero-stall model,
more rules)), quorum everywhere (`specs/tla/quorum-everywhere/`: every step of the journal, the
write, the election, the leaderless read, the seal, the boundary write, the retire, the offload mark, the trim, the acceptor compaction and the
matchmaker handover, waits for a quorum only and completes with F members down in every column,
in each matchmaker set and in the replica set, with a control that blocks each step one member
later and a control that waits for every member as a FoundationDB TLog commit; #1257 (quorum
everywhere)), the grid controller (`specs/tla/grid-controller/`: its election by a term in the
matchmaker record and the fenced `reshape(expected configuration, new grid, term)` call, with two
candidates that race, a deposed controller that keeps calling, a crash between plan and call, a
takeover, a lost reply and the bound `F + 1 <= acks <= rows - F`; #1259 (the fenced reshape call of
the grid controller)), segment replica sets (`specs/tla/segment-replicas/`: a write reaches only the
replica set of its segment, the boundary state of the seal (`next_seq` in the segment record, the
leader uuid in the journal's matchmaker record and a `Fence` at the first slot of the next
segment; the uuid part is superseded by `seal-chain/`), reads routed to the set of the segment, retire at `F + 1` holders; #1271 (a write
reaches only the replica set of its segment)), multi-writer mode (`specs/tla/multi-writer/`:
`SetLeader(None)` and back, the fence judged at apply in both directions, the position of a fenced
write judged before its uuid so a resend resolves a held write, the writer's basis from the view,
`Truncate` in both modes, a `Fence(None)` at a seal (superseded by the uuid in the segment record of `seal-chain/`), a resend across a seal answered `below` (#1301
(a resent write across a seal)); #1274 (multi-writer mode through SetLeader(None) and expected_seq)), a reshape when a grid column is below quorum
(`specs/tla/degraded-reshape/`: the Phase-1 bypass, the decision hold that never acks then, the
segment-record check and a hold before Phase 2 that stay live, the seal end of a bypass; #1258
(reshape with a column below quorum)), segment records across a matchmaker set handover
(`specs/tla/segment-records/`: the seal, the retire, the offload mark, the controller election and
the reshape call as compare-and-sets on the matchmaker register while the set hands over; a
replica drops its copy only after a write of the offload mark is done at a majority; #1275
(segment records across a matchmaker handover)), the offload of sealed segments (`specs/tla/offload/`:
the offloader's full copy, conditional put and compare-and-set mark, a found object read and
marked whatever its release, the delete of the key by a mark that finds the record trimmed, the
scrub of the objects below the trim floor, the drop after the mark; #1256 (offload races and the
offloader role)), the health measures of the grid (`specs/tla/health/`: the spares of a column,
electable, writable and the hold of a bypass as counts against the quorums, the seven journal
states and their fixed actions, no placement over two zones after a zone loss; #1260 (journal
health measures)), a user truncate across segments (`specs/tla/segment-truncate/`: the trim floor
in the boundary state, the floor of a read from the segment in force, the floor check of an
offloaded read, the trim of a sealed segment once the floor is decided over it, the acceptors'
compaction after `F + 1` replicas with a floor kept for each segment (#1318), the trim-point jump; #1276 (user truncate across segments)), two seals in a row (`specs/tla/seal-chain/`: a chain of
segments with an empty segment and a seal before the `Fence`; the whole boundary state, the
leader uuid too, in the segment record, with no `Fence` slot; #1303 (two seals in a row)), the seal
end against a CTRL faulty record (`specs/tla/seal-ctrl/`: the end counts faulty votes, so a Phase 1
that reads the old grid below the end keeps one value; #1313 (the seal end against a CTRL faulty
record)), CTRL in the journal (`specs/tla/ctrl-grid/`: the tri-state tally on a grid and across the
configurations of `H_b`, a scrub and a boot that give one faulty record, the repair of a chosen
value in one write; `specs/tla/ctrl-segments/`: the copies of a sealed segment across retire and
offload, the repair of a replica log and of an object, the data-loss report; #1263 (CTRL recovery
in the journal)), the journal v1 composed (`specs/tla/journal-state/`: three segments, the seal,
register writes that are partial or done, the boundary state, a replica set per segment, the fold
with `Below` and its `Resolve` call (#1320), retire, trim; the acceptors' state is a memory, a queue of writes and a disk, with a
crash between two writes, and an `Accept` that raises the promise writes the promise first or in
the same write; #1189 (the journal state model), #1309 (the state is the disk)), the heal of a sealed segment's replica set
(`specs/tla/re-replication/`: a new replica counts only on its own complete copy from a full
member of the set, the new set written by compare-and-set on the version of the register, a "done"
report that belongs to one copy, the refusal of a replica that does not hold the slot, a trim and a
removed replica that comes back during the heal; #1314 (re-replication of a sealed segment's
replica set)).
`specs/tla/README.md` lists every flavor with its model. `decree/` and `multi-paxos/` also have an
inductive invariant `Inv`, which TLC checks is kept by every step (`inductive*.cfg`; exhaustive for
the decree, drawn states for Multi-Paxos; #1307 (inductive invariants, then proofs)). The decree's
`Inv` is also proved with TLAPS at every size (`specs/tla/decree/DecreeProof.tla`,
`scripts/check-proofs.sh` in `nix develop .#proofs`, the `proofs` job of the `TLA` workflow;
`specs/tla/README.md`, "Proofs"). `docs/journal/03-segments-and-records.md` (§J3.4) lists every record of the
journal's matchmaker register with its writer, its guard and the models that check it (#1315). Then the journal is
designed again from the ground up, together with Pierre. Its sans-IO contract (the shape of
`step`: the flight rule, recorded inputs, a pure `Host`, one storage trait, the `Events` and
`Assignments` ports) is `docs/journal/04-roles.md` §J4.2 (#1255 (journal sans-IO contract)),
and its flight rule has a model, `specs/tla/flight-rule/`. The pure journal core is
`crates/lib/paros-journal` (the fold, the `Resolve` call, the held answers, the segment record
and the codec; #1324 (paros-journal pure core)), walked and traced against
`specs/tla/journal-state/FoldTrace.tla` by `crates/sim/journal-sim`. The journal's matchmaker
register is `paros_journal::register`, an optional role by composition (the core does not
change), walked and traced against `specs/tla/segment-records/RegisterTrace.tla` (#1325 (the
matchmaker register as an optional role)). The first journal roles are `paros_journal::roles`
(the acceptor, the leader with its Phase 1 over the segments and the seal, the segment replica;
#1326 (the journal roles over the core), first rule group), walked by `journal-sim`'s roles walk;
the other rule groups of #1326 (proxy leader, leaderless reads, retire, truncate,
re-replication) are not built yet. The roles run on the traits of `crates/lib/paros-journal-io`
(storage, transport, clock, register), the pure `Host` that serves many journals in one process,
and the generic driver under the flight rule (#1329 (paros-journal-io)), judged by
`journal-sim`'s driver walk.
**`docs/journal/` is the agents' map to the journal v1** (#1332 (journal architecture docs),
2026-11-20): the one home of every journal rule, with its decision, its issue and its model, and
the build steps of #1323 (build the paros journal v1). Start there for any journal work.

**The consensus core must be extremely strong** (Pierre, 2026-11-19). Three judges, all on the
same code: model-based trace checking (each run of the core is checked against its TLA+
model, with moonpool-tla, moonpool #550), deterministic simulation testing (a seed is a run,
faults on, with moonpool-walk, moonpool #554, a seeded random walk over the sans-IO state
machines), and coverage-driven simulation (the seeds are steered toward the branches not yet
reached; model-state coverage is for later, Pierre 2026-11-19). They live in the `consensus-sim`
crate (`crates/sim/consensus-sim/AGENTS.md`). The random walk is there (#1197 (consensus-sim
walks)). Trace checking is there for every walk (#1198 (trace checking), `tests/traces.rs`), and every
trace spec extends a model that TLC checks and defines no step of its own (#1306 (trace specs
extend the checked model)): `decree/`; `node/`, the implementation model of `ColocatedNode` (the
log, matchmaking, the GC and the retirement, grids, the compaction floor and the trim jump, the
faulty entries of CTRL with the tri-state tally, the read replica, the leaderless reads, the
rounds of the proxy leaders and the cooperative handoff; #1343 (merge proxy and handoff into the
node model)), which ten trace specs extend; `generations/` (the chain of matchmaker set
handovers); and `batcher/`. `proxy-leader/` and `leader-handoff/` stay as design models with
their own checks, the liveness checks too. Each trace spec names the steps its checked walks must take (a `Noop` gap
fill, a reconfiguration, GC above frontier 0, a compaction that keeps a record, a promise that
reports a faulty record, a node that retires and rejoins, and more; #1231 (traced walks reach the
rules)). The trace checks are tested too: each trace spec must refuse about 20 named edits of
its recorded walks at the edited event or after it (#1308 (mutate recorded traces),
`trace/mutate.rs`). Coverage-driven simulation is there (#1199, `tests/coverage.rs`): the walks
branch from the runs that reach a new probe or a new edge of the core, and a gate judges every
`probe!` of the core over all the walks. The walks also rot disks, retire and rejoin nodes,
admit late nodes, hand the matchmaker set over and move stuck rounds (#1225 (walk moves for the
unreached probes)). Every one of the 79 probes is reached in CI, with no list of excused probes
(Pierre, 2026-11-19: no excuses in simulation, no allow lists; each job of the gate has 30 minutes of CI, Pierre 2026-11-20).

**The end goal is `docs/architecture/`**: a multi-tenant journal service with a four-call data
plane (`Write`, `Read`, `Truncate`, `SetLeader`). It is the target picture, not a description
of the code today: most of what it names is at the tag, not in the tree. **The first step to it
is the journal v1, `docs/journal/`** (Pierre, 2026-11-20): the journal alone, with its data
plane, segments, roles, quorums, controller, verification and build steps. The journal rules of
`docs/architecture/` moved there; each moved architecture section keeps its heading and a link.

## The goals

Pierre, 2026-11-22. The goals say what paros is for and how it is built; a rule of this file
or of `docs/` is a consequence of one of them (`docs/architecture/01-goal.md` §1.1).

| # | Goal | In short |
|---|---|---|
| 1 | **A journal service for tenants** | Four calls: `Write`, `Read`, `Truncate`, `SetLeader`. A durable, ordered, fenced log. paros owns the log, never the state. |
| 2 | **The cell heals with nobody watching** | After the faults stop, each journal and each set is whole again within one bound, or reports a true reason. |
| 3 | **paros eats its own food** | State is in journals, who acts is an election on a journal, agreement is a Paxos flavor of the core. The control plane gets no call that a tenant does not have. |
| 4 | **One shape at every level** | Universe, cell and tenant are each a control journal, a pure fold and one coordinator. Levels talk only through records. |
| 5 | **The person says what, the system says where** | A person types names and what a journal must survive. The system chooses machines and ids, and refuses what it cannot place. |
| 6 | **Everything can move** | A namespace moves to another cell with no change of protocol, and a cell is rolled out by evacuation. |
| 7 | **Layers that stand alone** | Each layer is a crate that a program that is not paros can use alone. A layer depends only on the layers below it. The core has no tenant. |
| 8 | **Every role is a pure actor** | A deterministic step, persist before send, a refusal is data. I/O is only in adapters. So each layer has its model, its walk and its trace check. |
| 9 | **Proved before built** | No code before its TLA+ model, and no excuse in simulation. |
| 10 | **Everything simulates, and it runs in the browser** | Every layer, and the whole service, runs in the deterministic simulation. The same code compiles to wasm and runs in a browser. |

The features of the service at the tag stay; its structure changes (goals 7 and 8,
`docs/architecture/04-roles.md` §4.4). When you add a crate, ask: can a program that is not
paros use it alone?

## Who owns the code

This codebase is yours: how it's built is your call, and you'll keep building on it. When you
work in a file, leave it better: merge repeated code, fix the problems you find next to your
change, and delete what your change makes obsolete, in the same PR. Name every change in the PR
description, the small ones too, because the reviewer reads the diff against it.

What paros is for is Pierre's call. A change to the goal, to a recorded decision or to the
order of work is a proposal: write it in the PR or in an issue, and do not merge it on your own.

## Finishing work

Do not stop while you still owe work that was asked for. Carry on with work that does not
depend on an open question. When you meet an ordinary obstacle (an error, a timeout, a busy
proxy, a failing tool, an empty result), diagnose it, then work through it: wait and retry, fix
the request, or use another tool. Stop only when the task needs Pierre's input, or when the
blocker is deliberate (access that was withheld, a rule in this file). Then say plainly what you
found.

A pull request merges on green CI and after a review that is okay. Never merge on red; fix the
red, in the PR or in its own PR. A flaky check is a finding, never a reason to run it again.
These steps need Pierre's go-ahead: a change to the goal or to a recorded design decision, a
merge on red, and a force-push or a delete on a branch you did not create.

## Build, test, gates

These are the commands CI runs (`.github/workflows/rust.yml`), always through Nix (next section):

- `cargo build`
- `cargo nextest run` (CI: `--profile ci`; fall back to `cargo test`)
- `cargo fmt` (CI: `cargo fmt --all -- --check`)
- `cargo clippy --all-targets -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc -p paros-consensus --no-deps`, and the same with
  `-p paros-journal` and with `-p paros-journal-io`
- `scripts/check-module-style.sh`, `scripts/check-core-layers.sh` and `scripts/check-core-deps.sh`
  (the pure crates depend on no runtime, I/O or storage crate; `docs/journal/04-roles.md` §J4.3)
- the eleven examples: `cargo run -p paros-consensus --example <name>` (`crates/lib/paros-consensus/examples/`),
  the journal examples: `cargo run -p paros-journal --example <name>` (`fold`, `register`) and
  `cargo run -p paros-journal-io --example <name>` (`in_memory`, `custom_storage`,
  `multi_writer`; #1370 (journal examples)), and the facade examples:
  `cargo run -p paros --example <name>` (`journal`, `tokio_tasks`)
- `scripts/check-examples.sh`: every composing crate has an example, and the `examples` job runs
  each one (the workflow rule "Composing crates have examples" below)
- the doc examples: `cargo test --doc -p paros-consensus -p paros-journal-io` (nextest runs no
  doc test)
- the coverage gate of consensus-sim (`tests/coverage.rs`) in release, with `paros-consensus`
  instrumented by SanitizerCoverage (`RUSTC_WRAPPER=scripts/sancov-rustc.sh`,
  `SANCOV_CRATES=paros_consensus`, `--target-dir target/sancov`). Each CI job has at most 30
  minutes, so four `coverage-shard` jobs walk a quarter of the walks each
  (`COVERAGE_SHARD=i/4`, `COVERAGE_SAVE`), and the `coverage` job judges their union
  (`COVERAGE_FROM`). The `test` job leaves this gate out. The `Coverage nightly` workflow runs it again every night on new
  seeds (`MOONPOOL_WALK_FROM` moves with the date).
- the edits of recorded traces (#1308 (mutate recorded traces)), in release in the
  `trace-edits` job: `cargo nextest run --release -p consensus-sim --test traces -E
  'test(edited) | test(no_record)'`. The `test` job leaves them out.
- the `mutants` job (pull requests only): cargo-mutants on the lines of
  `crates/lib/paros-consensus/src` that the pull request changes (`scripts/mutants.sh --in-diff
  pr.diff`). Two passes, built in the `mutants` profile: the tests of `paros-consensus` and of
  `consensus-sim` without the coverage gate and the trace checks (`.cargo/mutants.toml`), then
  the trace checks on the mutants that the first pass missed (`.cargo/mutants-tlc.toml`). The
  same job runs the two passes on the changed lines of `crates/lib/paros-journal/src`
  (`scripts/mutants.sh --journal`: the tests of `paros-journal` and the walks of `journal-sim`,
  then the trace checks of `journal-sim`; `.cargo/mutants-journal.toml`,
  `.cargo/mutants-journal-tlc.toml`), and one pass on the changed lines of
  `crates/lib/paros-journal-io/src` (`scripts/mutants.sh --journal-io`: its tests and the driver
  walk of `journal-sim`; `.cargo/mutants-journal-io.toml`). A
  surviving mutant fails the job: add the test that sees it in the same pull request. The
  `Mutants` workflow runs every mutant of the core (about 2,850) in 48 shards each Sunday and on
  demand, and fails on any survivor. A timeout counts as caught, and so does a test that
  earlyoom stops when the runner is short of memory. An `exclude_re` entry is only for an edit
  that changes nothing, with its reason.
- wasm gates: `cargo check --target wasm32-unknown-unknown -p paros-consensus`, the same with
  `--no-default-features`, `cargo check -p paros-consensus --features serde`,
  `cargo check --target wasm32-unknown-unknown -p paros-journal`, the same with
  `-p paros-journal-io`, and
  `cargo check --target wasm32-unknown-unknown -p paros`
- the `wasm` job: `cargo test -p paros-wasm --test paxos_traces`, then
  `git diff --exit-code -- website/site/src/traces`, then `scripts/build-wasm.sh --wasm-only`
- the site (`.github/workflows/pages.yml`, on pull requests that touch the site, `paros-wasm`,
  `paros-consensus`, `paros-journal` or the Cargo files): `scripts/build-wasm.sh`, the Astro
  build, the link check of the built site (`npm run check-links`) and the browser smoke test
  of its live parts (`nix develop .#site --command npm test`, Playwright and Chromium;
  `website/site/AGENTS.md`, "Tests")
- `scripts/check-tla.sh` in the `tla` shell (`nix develop .#tla --command scripts/check-tla.sh`):
  TLC on the TLA+ models of `specs/tla/` (CI's `TLA` workflow, one job for each folder, and the trace checks of consensus-sim and journal-sim, the live check of `journal-sim/tests/live.rs` too, in four
  `traces` shards, `--partition count:i/4`, #1340 (the traces job reaches its timeout)); the job of each folder also checks that
  each guard mutant parses (`scripts/mutate-tla.py --parse`)
- the `TLA mutants` workflow, each week and on demand: `scripts/mutate-tla.py` sets each guard
  of each action of `specs/tla/` to `TRUE` and runs the folder's `ci` checks; one check must
  fail. A survivor fails the run: remove the guard or add the check that needs it, never an
  allow list (#1311 (mutate spec guards), `specs/tla/README.md`)

A pull request that touches only the browser side (`website/**`, `scripts/build-wasm.sh`,
Markdown, `docs/`, `specs/`) runs only the `wasm` job of `rust.yml`; a push to `main` runs
everything. Rust 2024 edition, toolchain pinned in `rust-toolchain.toml` (incl.
`wasm32-unknown-unknown`), clippy pedantic on (`[workspace.lints]`). The whole workspace passes
pedantic with no lint allowed (Pierre, 2026-11-19): fix the code, never add `#[allow(clippy::..)]`.
When a lint is wrong for one item, write `#[expect(clippy::<lint>, reason = "..")]` on that item
and name it in the PR description.

**Releases** (Pierre, 2026-11-21: release-plz back on, publish more often; he decides when).
`release-plz.toml` and `.github/workflows/release-plz.yml`. The public crates are
`paros-consensus`, `paros-journal` and `paros`, at one version (`version_group`), one `v{version}` tag and one
draft GitHub release. release-plz creates and writes every changelog; none is written by hand.
`consensus-sim`, `journal-sim`, `paros-wasm` and `paros-authz-biscuit` stay `publish = false`. A push to `main`
only opens or updates the release pull request. **Nothing publishes by itself**: only Pierre
merges a release pull request, and only Pierre starts the `release` job (workflow_dispatch),
which publishes. An agent never merges a release pull request and never starts that job. A new
public crate (for example `parosd`) gets the crates.io metadata, a `version` on
its workspace dependency line, and a `[[package]]` entry in the `paros` group of
`release-plz.toml`, in the PR that creates it. The `portability` job checks that the public
crates package and build as crates.io builds them (`cargo publish --dry-run --workspace`): each
git dependency is replaced by its `version`, so a moonpool pin that needs an API not yet on
crates.io turns that job red until moonpool publishes it.

## Environment & Nix

At the start of a session, run:

    echo "entrypoint=$CLAUDE_CODE_ENTRYPOINT sandboxed=$CLAUDE_CODE_SANDBOXED"

If `CLAUDE_CODE_ENTRYPOINT` starts with `remote` (e.g. `remote`, `remote_mobile`), this is
**Claude Code on the web**. Set up Nix before anything else:

    # The sandbox's third-party APT sources 403, so install from the cached lists first.
    if ! command -v nix-store >/dev/null 2>&1; then
      sudo apt-get install -y nix-bin \
        || { sudo apt-get update; sudo apt-get install -y nix-bin; }
    fi
    # Export in every shell that runs `nix` (or add to ~/.bashrc):
    export NIX_CONFIG="experimental-features = nix-command flakes"
    export NIX_SSL_CERT_FILE=/root/.ccr/ca-bundle.crt

Then use `nix develop`, on the web as locally. Every flake input is fetched over git
(`git+https`), not as a GitHub tarball, because the web session's egress policy refuses GitHub
tarballs (#727 (flake inputs over git)). Keep it that way: a new input never uses `github:`.
Any other value (`cli`, `vscode`) is local: Nix is already set up; use `nix develop` (or direnv).

**Use Nix-provided software for all tooling.** Never run the sandbox's preinstalled binaries
(the `rustup`/`cargo`/`rustc` under `/root/.cargo`), never `apt-get`/`pip install`/`npm -g`/
`brew` (`nix-bin` above is the one exception). Run every command in the dev shell:

    nix develop --command cargo build
    nix develop --command cargo nextest run

Other tools: `nix shell nixpkgs#<tool> -c …`; a missing tool goes into the flake.

## Workflow rules

- **Record what the user decides, in the same session.** A design decision or preference the
  user states (what paros is, how it behaves) goes into `docs/journal/` when it is a journal
  rule, else into `docs/architecture/`, dated and with its issue; a preference about how to work goes into this file. Amend the section it touches, never
  only a chat, a commit message or an issue comment, and correct a recorded decision the user
  later overturns rather than adding a second one.
- **No excuse in simulation, and no allow list**: see the absolute rule at the top of this file.
- **Composing crates have examples** (Pierre, 2026-11-21: "I really like examples to review the
  abstraction and dev experience", "Composing crates only"). A composing crate is a library
  crate of `crates/lib/` that depends on another library crate of the workspace: today `paros`
  (the facade), `paros-journal` and `paros-journal-io`; later the moonpool adapter and the
  journal client. A user programs against these crates, so each one has runnable examples in
  `examples/` that use its public API as a user would (the facade's examples import `paros::`
  paths only). An example asserts what it shows, so CI runs it as a test: add each new example
  to the `examples` job of `.github/workflows/rust.yml`. A change that reshapes the public API of
  a composing crate updates its examples, or adds one, in the same pull request: the examples are
  how a reviewer judges the abstraction. `scripts/check-examples.sh` fails when a composing crate
  has no example or when CI does not run one. The internal crates (`consensus-sim`,
  `journal-sim`, `paros-wasm`) need none. `paros-consensus` composes nothing, but its examples
  stay: they are the lessons of each flavor.
- **release-plz writes every changelog** (Pierre, 2026-11-21). Never create or edit a
  `CHANGELOG.md` by hand. release-plz creates each one from the commit messages in the release
  pull request, so the commit message is where a change is described.
- **A flaky check is a finding** (decided on 2026-11-17). A check that is red and then green
  with no change teaches you to run it again in place of a fix. Never run a check again until
  it is green, and never add a retry around it. Find the cause: a bug in the code is fixed with
  a test that is red first, and a bad check is corrected or deleted, with the reason in the
  commit message. No runner has a retry setting (nextest runs each test one time), and that
  stays.
- **Never guess** (Pierre, 2026-11-16). Do not invent a number, a default, a name or a reading
  of a decision. When a fact is missing, find it (the code, the docs, FoundationDB, the
  issue) or ask Pierre. When a sentence is your reading and not a checked fact, say so. In a
  design text, an open point is marked **Open** with the step that decides it.
- **Moonpool questions**: read <https://pierrez.github.io/moonpool/llms.html> before its source
  (`moonpool-consultant` agent).
- **Name every issue and PR number** (decided on 2026-10-10): write a short name next to it,
  e.g. "#216 (finish bootstrap)", in chats, comments, PR descriptions and commit messages.
- **A comment states the invariant in the present tense** (decided on 2026-11-15, decision 9 of
  the rework). A comment or a doc comment says what is true of the code now: the rule, why it
  holds, what breaks without it. The story of a change goes in the commit message: the seed
  witness, the commit hash, the date of a decision, the bug that a line fixed and the
  red→green result. A comment may keep one issue number when that issue names a design
  decision. Apply the rule file by file: when you change a file, rewrite the comments of the
  code you touch, and do not sweep the tree in a separate PR.
- **Write documents in ASD-STE100 Simplified Technical English, most of the time** (decided on
  2026-10-09): `docs/`, the site, READMEs, AGENTS.md files, issues and PR descriptions.
  Short sentences, one idea per sentence, active voice, and the same word for the same thing
  every time. Use a paragraph of flowing prose where ideas connect, and bullets for lists of
  separate items. Code identifiers and protocol terms stay as they are. Leave the style when a
  sentence would lose precision. (The hard word counts were dropped on 2026-11-11: newer models
  follow them so closely that the prose gets choppy.)
- **Write for a reader who did not watch you work**: a PR description, an issue, a handoff and
  a final report are read by someone who has not seen your tool calls or the files you opened.
  Put the key points in the first sentences. Do not use a term you made up during the work.
  Say what you found, what you did, how you proved it, and what is next.
- **Quorum everywhere** (Pierre, 2026-11-20, #1257): no step of the journal waits for every
  member of a group (no full broadcast as a FoundationDB TLog commit). A new journal step gets
  its row in `specs/tla/quorum-everywhere/`, with a control that blocks it one member later
  (`docs/journal/05-quorums-and-failures.md` §J5.1).
- **When in doubt about safety, use TLA+** (Pierre, 2026-11-20): when you are not sure that a
  rule is safe, or that one rule also covers another case, write a TLA+ check (a check that
  passes with the rule and a control that fails without it). Do not argue it in prose.
- **TLA+ is a thinking tool** (decided on 2026-11-12, #1020 (TLA+ ranked list)): when a
  protocol problem has more than one possible rule, you can write a small TLA+ model to find
  the solution, before you write Rust. Model each candidate rule as a constant, give each a
  control (a check that must find the bug with the rule off), and let TLC select. Put the model
  in `specs/tla/` when its rule ships (`specs/tla/README.md`); a model that only helped you
  think can stay out of the tree, with its result in the issue. A trace that TLC finds becomes a red unit test before the fix.
- **Follow the papers; a change to them needs a TLA+ check** (decided on 2026-11-12, #579
  (zero-stall reconfiguration)): paros builds a protocol as its paper and its reference
  implementation give it. A change to that protocol (a different rule, or a rule that the paper
  does not have) is a proposal. It needs a model in `specs/tla/` before the rule is written in
  `paros-consensus`: a check that passes with the rule, and a control that finds the bug without it.
  A design document names each such change and its check.
- **Keep the knowledge base in step with the protocol** (decided on 2026-10-27, #745): a PR
  that adds or changes a Paxos concept paros uses updates its card in
  `docs/knowledge/concepts/` (a new concept gets its card in the same PR), and the index
  `docs/knowledge/concepts.md`. A card cites a source outside paros, never memory
  (`docs/knowledge/CLAUDE.md`).
- **Organize as you grow**: a module holds one concern; split a file the moment it holds a
  second. Deleting is part of every change: a superseded axis, type, flag or gate goes in the
  PR that supersedes it.
- **One module style** (#722 (three products)): a module with children is `foo.rs` beside
  `foo/`, never `foo/mod.rs`, and no file is included with `#[path]`. The one `mod.rs` is
  `examples/common/mod.rs`, the code that examples share: Cargo builds `examples/common.rs` as
  an example of its own. CI runs `scripts/check-module-style.sh`.

## Architecture

**Sans-IO core, driver outside** (etcd-raft's `RawNode`/`Node` split). `ColocatedNode` in
`paros-consensus` is a pure synchronous state machine: `step`/`tick` in, one `Ready` out,
`advance()`; no I/O, clock, RNG or deps. `ready(&mut self) -> Ready<'_>` holds the unique
borrow, so a second `ready()` before `advance()` is a compile error. Persist-before-send
ordering is documented on `Ready`/`HardState`
(`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`). The driver that owns the core and
does its I/O was in the removed `paros-roles` (`docs/archive/v0.2.0-alpha.1/paros-roles.md`);
today only the examples, the model checkers and the wasm engine drive the core. The journal's
roles follow a different contract, the flight rule (`docs/journal/04-roles.md` §J4.2): each
persist batch has an id and each send names the persist id it waits on, so a driver may pipeline
and group commit. §4.2 lists every difference from `Ready`/`advance` as a change or a proposal.

**One file per role; `ColocatedNode` is wiring** (timers, messages, the persist-before-send
batch) and holds **no protocol tally of its own**:

- `acceptor.rs` (+ `acceptor/retention.rs`, the floor-moving ops) — promise, records, floor, CTRL
- `proposer.rs` (+ `proposer/rounds.rs` the standalone Phase-2 tally, `proposer/authority.rs`
  the standing authority) — election, repair probe, recovery
- `replica.rs` — chosen prefix, apply walk, journal fold · `journal/state.rs` — the one
  journal-control state machine, judged at apply
- `quorum_read.rs` — leaderless reads · `collector.rs` — the leader-side GC tally
- `membership/` — `AcceptorConfig`, `MatchmakerSet`, `QuorumSystem`
- `matchmaking.rs` — the candidate's matchmaking phase · `matchmaker.rs` (+ `reconfigurer.rs`)
  — the registry and its generation handover · `decree.rs` — the generic single-decree
  Paxos (`Decree<Id, V>`): the handover's successor, and any one-shot decision of a caller above the core
- `proxy_leader.rs`, `replica_node.rs` — the second and third deployments

Beside the roles, the **foundations**: `types.rs` (the Paxos scalars), `journal/` (what the log
carries), `contract/` (what a driver honors: `Message`, `WriteOp`, `Storage`, `MemStorage`,
`HardState`, `Config`, `Limits`) and `retained.rs`. The three layers point down only
(foundations ← roles ← deployments, #730 (paros-consensus rework)): a role never uses a deployment,
and a deployment never uses another (`scripts/check-core-layers.sh`). Every module is `pub`;
the root re-exports only what a driver loop needs. A deployment takes its `Limits` at
construction and reports its counters in one snapshot (`ColocatedNode::counters`).

The map is `crates/lib/paros-consensus/AGENTS.md`. **A component must not acquire knowledge merely because
the current deployment colocates it**: the proposer builds no message, the acceptor never reads
the chosen prefix, the replica never sees a ballot tally; the caller hands each the data it
needs. **Every quorum question crosses `membership/`**: predicates are phase-split
(`has_phase1_quorum` / `has_phase2_quorum`, cross-intersecting) and no tally compares a count
against a threshold on its own; a new quorum system is a `QuorumSystem` variant, never a tally
rewrite. Policies are explicit types, never flags.


**Paper names are the vocabulary**: a role keeps the name of Compartmentalized Paxos (proxy
leader, acceptor grid, replica, leaderless read, batcher, unbatcher) or Matchmaker Paxos
(matchmaker, matchmaking) in the code and the docs.

## Plain Multi-Paxos is a shape of the core, not of the service

Multi-Paxos without matchmakers — a fixed membership read once from storage — is a permanent
shape of `paros-consensus`, a library shape, not a transitional state (the journal of the target
design always has matchmakers, decided on 2026-11-16, decision 15). Before touching
`on_check_leader`, `Election`, `HardState` or the role map:

- The static case is the **`None` arm of the same state machine**: never a cargo feature, never
  conditional compilation (`serde`, `tracing` and `assertions` are paros-consensus's only features, all observation-only).
- No matchmaker message, no `HardState` field and no extra round trip enters the plain path;
  `ColocatedNode` never steps a matchmaker message. Removing every opt-in feature must leave the
  plain program's behaviour unchanged.
- A reconfiguration request on a deployment without matchmakers is **refused**, never honored.
- The quorum systems are the majority and the grid (flexible quorums are deleted, decision 27,
  step 0.12 of the rework). Grids, matchmakers, proxies and replicas are **configuration data**,
  never implied by code being present. Every peer and client message carries a `JournalIdentifier`
  `(TenantId, JournalId)`; under that identifier the plain deployment exchanges the same
  messages and persists the same scalars. The tenant leaves the core (Pierre, 2026-11-22): a crate
  that wraps the core adds it, and the core will carry one journal id (#1402 (remove the tenant
  from the core)); the code still carries the pair until then.

**The core is a library for everyone** (decided by Pierre on 2026-11-20,
`docs/architecture/04-roles.md` §4.3). `paros-consensus` stays usable by other people for plain
Multi-Paxos, single-decree Paxos and every flavor it already builds. What paros needs comes from
optional behaviour and software composition, never from a service concept inside the core (no
cell, no tenant policy, no `cell init`; #1264 (remove the cell from paros-consensus)). A paros
need that composition cannot meet is a new optional behaviour, with the plain arm unchanged.

**No runtime or storage dependency** (Pierre, 2026-11-21, `docs/journal/04-roles.md` §J4.3). A
program uses paros by implementing its traits (storage, transport, clock). The pure crates
(`paros-consensus`, `paros-journal`, and the journal's IO traits, controller and client crates)
depend on no async runtime (tokio, compio, smol), no storage engine and no network or file crate,
and their traits require no `Send` or executor. With no feature on, a pure crate depends on
workspace crates only; its features only observe (`serde`, `tracing`, `assertions`). A crate that
does I/O is an adapter of its own (`paros-journal-moonpool`), behind a facade feature that is off
by default. `scripts/check-core-deps.sh` enforces it in CI; a new pure crate joins its `PURE` list
in the pull request that creates it.

## Matchmaking, reconfiguration, GC, generations

- **Matchmaking, then Phase 1.** A candidate registers `(b, C_b)` at a matchmaker quorum before
  any `Prepare`; histories are unioned above the **maximum** watermark into `H_b`. Phase 1 needs
  a promise quorum of **every** configuration in `H_b` — never `quorum(union)` — and Phase 2
  addresses `C_b` alone. A refusal abandons the campaign; a slow one is re-asked on election
  timeouts, never abandoned by the clock.
- **A belief is not a fact.** A registration is a belief (an ordinary campaign) or a
  reconfiguration; the **effective configuration** is the highest-ballot reconfiguration the
  matchmakers report: a candidate takes the maximum over the answers it heard, and a handover's
  `close_stop` the maximum over its stop quorum, so a reconfiguration that only a minority
  holds can become effective. A campaign whose histories name another abandons, adopts it and re-campaigns
  (`MatchStep::StaleConfiguration`), unless it sits at or below a leadership the node knows won
  (the probe floor, #278: a minority held it); beliefs never trigger that.
- **Membership probe** (`MembershipProbe`): every incarnation boots believing the bootstrap
  configuration and asks a matchmaker quorum for the effective one, registering nothing, before
  its first campaign or skip. A node only registers a belief it heard. A node whose heard belief
  leaves it outside re-probes on every election timeout (#270). A probe adopts a
  reconfiguration only above what the node knows is in force, the fact its belief matches or
  a leadership it knows won, never a campaign it only promised; a probe that closed with its
  node outside still folds its late answers (#278). A re-ask also asks the matchmaker pool
  outside the believed set; an outsider's answer counts only as a `Generation` refusal that
  names a later set (#858 (boot from the cached registry)). A campaign's re-ask on an election
  timeout does the same, and such a refusal abandons the campaign: a node that heard a belief
  naming it skips the probe and campaigns on its boot-time set, which may be gone for good.
- **A reconfiguration is a round change** (`ColocatedNode::reconfigure`): a configuration is bound
  to a ballot and never edited; the leader re-campaigns with `C_new`. **Removed is not shut
  down**: a removed node keeps answering Phase 1 for ballots it took part in (acceptor guards are
  pool-based, never configuration-based).
- **GC**: a configuration is forgotten only once no future leader can need its Phase-1 quorum. A
  floor is **effective only once a matchmaker quorum acked it durably** (`GcStep::Effective`);
  only then are acceptors retirable. The compaction floor and the GC watermark never wait on
  each other.
- **Retire carries the evidence**: the operator sends the effective watermark and
  `ColocatedNode::may_retire` checks six legs (matchmakers, not a member, not the leader,
  watermark above `last_member_ballot`, belief bound to exactly that watermark, the
  matchmakers' check of the registrations that name the node). The fence
  `last_member_ballot` also covers every campaign the node opened with a configuration naming
  it: the registration stays in the matchmakers' histories whether or not the campaign went on
  (decided on 2026-11-07; witness `3055323747627105565 515957:2164842326828835157` on 2949e4e). A leader's beat carries the floor it made effective
  (`Heartbeat::gc_floor`), and a node whose slot was **released** without a `Retire` keeps
  answering the journal until `may_retire` holds on a floor it heard
  (`ColocatedNode::may_retire_heard`, `SystemCtx::settle_lingering`): a released slot is not a
  retirement. The fence is durable and covers registrations of other nodes too (decided on
  2026-11-11, #1002 (retire fence gaps), option (b)): the registration fence is a node scalar
  (`WriteOp::RegistrationFence`, read back at boot), and a sixth leg needs every matchmaker
  of the set in force to have agreed, under the node's belief, that no registration at or above
  the floor names it (a membership probe whose answers carry `named`; `check_retirement`, a
  `Retire` refused `unchecked` until then). Each matchmaker that agrees records the node as
  retired, durably, before it answers, and refuses every later registration naming it
  (`MatchRefusal::Retired`, decided on 2026-11-12); the record crosses a handover as a union.
  A candidate drops from `H_b` a configuration that only abandoned ballots registered
  (decided on 2026-11-14, PR #1064 (gone-node gap)): every registration naming it lies strictly
  between the watermark and the ballot, and each ballot's owner promised the candidate with an
  owner testimony (`Promise::prepared`, the highest ballot it sent a `Prepare` at) below it.
  A history page keeps each owner's lowest registration of each configuration, so the
  candidate sees every owner (a page that hid one chose two values on the canary).
  A retired node that placement books again rejoins (decided on 2026-11-12, #1037
  (rejoin)): its boot reads a checked `RetireMark`, moves to the next incarnation durably and
  asks every matchmaker to drop the record (`MatchPurpose::Rejoin`), a quorum enough to name it
  again and nothing waiting for the rest. A boot that reads an unchecked mark above incarnation
  0 asks again at that incarnation: the mark is durable before the first `Rejoin` leaves, so a
  stop between the two would leave the node retired at every matchmaker (PR #1064 (gone-node
  gap)). A journal booked again while it still runs there
  (it lingered) rejoins the same way with no reboot
  (`ColocatedNode::rejoin`, called whether or not a check was asked); a check of an earlier
  incarnation is refused (`MatchRefusal::Rejoined`), and the later incarnation wins a handover.
  The plain path writes and sends nothing new. The operator's half: retire no node a
  reconfiguration it asked for above the floor names.
- **Matchmaker sets are generations**: every matchmaking message is fenced by generation, the
  coordinator's `ReconfigureMatchmakers` too (#563: the node refuses an ask whose generation is
  no longer in force, so a re-ask the read left stale never starts a second handover);
  matchmaker quorums are **majorities only**; the handover's decree is the shared `Proposer` +
  `Acceptor` over a one-slot log (no second Paxos kernel) and opens strictly above the decree
  round on the node's disk, which the driver writes before the round's first request leaves
  (`ReconfigurerReady::round`; decided on 2026-11-19, #1229 (a stale `Stopped` reply reuses a
  decree ballot): a `Stopped` sent to an earlier incarnation can reach a later one, so the stop
  quorum's maximum decree promise alone does not keep two incarnations apart), and above that
  maximum promise, which only skips rounds the matchmakers refuse; any node that meets `Stopped { successor: None }` finishes
  the handover. `MatchmakerSet::new` / `AcceptorConfig::new` are the only constructors.

Depth: module docs of `matchmaking.rs`, `node/matchmaking.rs`, `node/reconfigure.rs`,
`node/gc.rs` (`may_retire`), `matchmaker.rs`, `matchmaker/reconfigurer.rs`,
`models/handover.rs`; `docs/analysis/consensus/matchmaker-gc-and-generations.md` and
`matchmaker-interaction-verification.md`.

## Protocol invariants to remember

- **Truncation is a Paxos-decided control command** (`Truncate`, proposed by the leader), **fenced
  by the leader uuid like a `Write`** (#228, #241): judged at apply, a stale or foreign caller is
  refused in place; an accepted one raises `first_seq` and every node compacts lazily when its
  walk reaches the slot. paros runs no application and takes no snapshot; record bytes are
  opaque.
- **The leader uuid is the writer fence** (#241): `SetLeader(new, old)` is a compare-and-set,
  `Write` and `Truncate` carry the uuid, and the core's term counter never reaches a data-plane
  reply (a client sees `JournalView`; an operator's `Inspect` shows the term). The journal trusts its clients to draw fresh uuids (decided on
  2026-10-09): a reinstated uuid wins (`docs/journal/02-data-plane.md` §J2.3).
- **A trim-point jump** (`Message::TrimmedTo`) recovers a below-floor node: it carries a floor
  and the journal state, no bytes and **no ballot**; the promise never moves.
- **Cooperative handoff** (`DPaxos`, `relinquish_to`): abdication is synchronous with the
  decision, the successor is named inside the payload, and only the elected minter of a ballot
  may hand it on (one hop). A failed handoff costs availability, never safety
  (`docs/analysis/consensus/dpaxos-leader-handoff.md`).
- **Election gap fill**: a new leader re-proposes every reported slot *and* fills every
  unreported slot below its frontier with a `Noop`, because pipelining can strand a slot no one
  would ever propose again, freezing the chosen prefix cluster-wide.
- **The allocator frontier is durable by construction**: a leader records every round it opens
  (`record_own_round`).

## Assertions, probes, spans

- **`paros-consensus` uses hard `assert!`**, on in release; no `debug_assert!` anywhere. Never assert
  on what a correct peer or client can send (decided on 2026-11-16, decision 30): a correct
  peer can be late, can send a message two times and can restart, and a client can send any
  request; each handler ignores such a message or answers with a result, and puts a limit on
  each size. The transport proves the sender and the bytes (step 1.11 of the rework checks
  it), so a role trusts a peer of its pool.
  A message that breaks a Paxos rule (two commands at one slot and ballot) is a detected safety
  violation: the assertion stays, with a comment that names the rule
  (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.11). `ColocatedNode::assert_invariants` runs at
  boot and every public mutating entry; public functions that assert document `# Panics`.
- **Assertions are TigerStyle** (decided on 2026-10-04, #248): in `paros-consensus`, an average of at least two assertions per function; assert preconditions,
  postconditions and invariants; **pair** assertions (check a property where data is written and
  again where it is read back, e.g. at persist and at boot); assert the negative space as well as
  the positive; assert relationships between constants at compile time (`const _: () =
  assert!(..)`); split compound conditions into one assertion each. Every assertion is an
  oracle for free in every test and every future simulation. The trust rule above still holds: what a correct peer or
  client can send is a result, never a panic. Reference: TigerBeetle's `TIGER_STYLE.md`.
- **Coverage probes in `paros-consensus`** (decided on 2026-10-09, #317): a rare protocol branch a
  test or a simulation must reach is a `probe!(reachable, "..")` or `probe!(sometimes, cond, "..")` at the line
  (`paros-consensus/src/probe.rs`, the `assertions` feature, default on, backed by
  `moonpool-assertions`). A probe only observes: no buggify, hint, RNG, clock or knob; with the
  feature off it compiles to nothing. A probe is not an invariant: an impossible state is still
  a hard `assert!`.
- **`paros-consensus` is never buggified**: no RNG, knob or conditional compilation. A rare-but-valid
  decision is exposed as a method with an honest contract (`resend_pending`, `step_down`) and
  perturbed only by a caller that stops calling.
- **Spans**: in `paros-consensus` write `#[cfg_attr(feature = "tracing", tracing::instrument(..))]`.
  Use `skip_all` plus a few cheap `fields` (node id, `from`, `round`, `slot`); entry points at
  `debug`, per-message and per-tick at `trace`; no `ret`/`err`. Spans are for humans only.

Depth: the `changing-paros-consensus` skill.

## Layout

Cargo workspace, every package under `crates/`. Each crate's `AGENTS.md` is its map.

- `crates/lib/paros-consensus` — the sans-IO Paxos roles and `ColocatedNode`; dependency-free with
  `default-features = false`, wasm-safe. Its optional, default-on `assertions` feature (#317)
  adds `moonpool-assertions` for coverage probes only. Its tests, the model checkers
  (`src/models/`), the driver contract (`src/contract/`) and the eleven examples
  (`examples/`: seven lessons, and four that drive `ColocatedNode` as a driver does, from
  `quickstart`) live in the crate. Its `README.md` is the entry point of a new user.
- `crates/lib/paros-journal` — the pure core of the journal v1 (`docs/journal/`): the journal
  fold, the `Resolve` call, the held answers, the segment record, the matchmaker register, the
  journal roles (`roles`: acceptor, leader, segment replica) and the codec. Sans-IO, wasm-safe, on `paros-consensus` with its features off; it adds no concept to the core.
  Its examples (`examples/`): `fold` drives the fold alone, `register` the compare-and-sets of the
  matchmaker register.
- `crates/lib/paros-journal-io` — the journal's IO side, pure (#1329): the four traits a backend
  implements (`Storage`, `Transport`, `Clock`, `Register`), the on-disk format (`layout`), the
  pure `Host` that serves many roles of many journals in one process, the generic `Driver` under
  the flight rule (group commit, held sends), the `Events` port, backends in memory (`mem`) and
  the contract suites (`contract`). Its examples `in_memory`, `custom_storage` and `multi_writer`
  run the roles on it.
- `crates/lib/paros` — the facade: each layer as a module (`paros::consensus`,
  `paros::journal`, `paros::journal_io`). A program depends on `paros` alone. Wasm-safe and provider-free
  with its default features; it is where moonpool's storage and network get bundled, behind a
  feature, once the adapter exists (#1330). Its examples (`examples/`) run a journal through
  `paros::` paths alone: `journal` in one process, `tokio_tasks` on three tokio tasks that talk
  over channels (tokio is a dev-dependency only).
- `crates/sim/journal-sim` — test only: the seeded walk of the journal fold over a chain of
  segments, the walk of the matchmaker register across handovers, the walk of the journal
  roles and the walk of the generic driver of `paros-journal-io` on three processes, with oracles that share no
  code with `paros-journal`, a planted mutant for each, and the trace check of each walk against
  `specs/tla/journal-state/FoldTrace.tla` and `specs/tla/segment-records/RegisterTrace.tla`, and the live trace check of a long fold walk while it runs (moonpool-tla `Live`, #1310
  (check long runs live); four hours each night in the `Journal live` workflow).
- `crates/sim/consensus-sim` — test only: the seeded random walks over the sans-IO roles
  (moonpool-walk), one walk for each Paxos flavor, with safety oracles that share no code with
  the core and a planted mutant that each oracle catches, and the trace check of the walks
  against the TLA+ models with TLC (moonpool-tla), and the coverage gate over every core probe
  (exploration with moonpool-walk, branch coverage with moonpool-explorer's sancov reader). On
  `paros-consensus` only.
- `crates/support/paros-wasm` — the site's wasm tools (#540): feature `paxos`, the engine of the
  step-through diagrams of the Paxos pages (#811), on `paros-consensus` only; feature `journal`,
  the fold lab, the register lab and the record lab of the Journal pages (#1363, #1364), on `paros-journal` only.
- `crates/support/paros-authz-biscuit` — Biscuit tokens (#245 (Biscuit tokens)): root keys,
  offline mint and derive, and the verifier of a request with one Datalog policy. It stands
  alone: no other paros crate, no runtime, and no crate of the workspace uses it yet. It came
  back from the tag without its adapter to the old frontend. It does not build for wasm32, so
  it stays out of the facade.

Elsewhere: `specs/tla/` (the TLA+ models; `specs/tla/README.md`; `specs/tla/experiments/`
holds the models that compare candidate designs before Pierre decides, such as the segments
experiment of #1213 (segments in the protocol or in storage) and its check of leaderless reads
across a reshape, #1254), `website/site/` (the website,
Astro, one style for every page; `website/site/AGENTS.md`, the `update-the-site` skill),
`website/shared/` (the design system: tokens, fonts, logo), `scripts/build-wasm.sh` (builds the
two wasm bundles and stages them for the site), `docs/architecture/` (the target design of the service), `docs/journal/` (the target
design of the journal v1: every journal rule, its model and the build steps; its `README.md`
is the map), `docs/knowledge/`
(the Paxos knowledge base: one card per concept paros uses, #745), `docs/analysis/` (design
notes), `docs/references/` (papers and source references; `docs/references/CLAUDE.md` indexes
them, with the shared-log prior art of the journal design: Delos, Northguard, LazyLog,
SpecLog), `docs/archive/v0.2.0-alpha.1/` (the recaps of the removed crates), `scripts/` (`build-wasm.sh`, `check-module-style.sh`,
`check-core-layers.sh`, `check-core-deps.sh`, `check-examples.sh`, `check-tla.sh`, `mutate-tla.py`, `sancov-rustc.sh`), `REVIEW.md` (what a reviewer checks for each kind of
diff), `.claude/skills/` and `.claude/agents/`.
