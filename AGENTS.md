# paros

Learning project: the Paxos consensus algorithm in Rust. WIP, not for production.

**The end goal is `docs/architecture.md`**: a multi-tenant journal service, `parosd`, with a
four-call data plane (`Write`, `Read`, `Truncate`, `SetLeader`) and the milestones that get there.
Read it before planning any work. Where the two disagree, this file is the present (what paros is
and the doctrine every change follows) and that one is the direction. Each section below ends
with where the depth lives; every crate has its own `AGENTS.md` map.

## Build, test, gates

These are the commands CI runs (`.github/workflows/rust.yml`), always through Nix (next section):

- `cargo build`
- `cargo nextest run` (fall back to `cargo test`)
- `cargo fmt` (CI: `cargo fmt --all -- --check`)
- `cargo clippy --all-targets -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc -p paros-core --no-deps`
- wasm gates: `cargo check --target wasm32-unknown-unknown -p paros-core`, the same with
  `--no-default-features`, and `cargo check --target wasm32-unknown-unknown -p paros`
- `cargo xtask sim run-all`: the sancov-guided sweep (`scripts/sancov-rustc.sh` is the
  `RUSTC_WRAPPER`, gated by `SANCOV_CRATES`; the flake `shellHook` exports it). The registered
  campaign is `paros-chain` (binary `sim-paros-chain`; `cargo xtask sim run paros-chain`).

CI also runs the `paros-core` examples and the `paros-play` web build. Rust 2024 edition,
toolchain pinned in `rust-toolchain.toml` (incl. `wasm32-unknown-unknown`), clippy pedantic on
(`[workspace.lints]`). The full local gate is the `validate` skill.

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

Any other value (`cli`, `vscode`) is local: Nix is already set up; use `nix develop` (or direnv).

**Use Nix-provided software for all tooling.** The one documented exception is the image
(`Dockerfile`, `docker-compose.yml`, #196): a plain multi-stage Rust build so a fresh clone runs
with Docker alone. It is the user's demo, never a test: no smoke script, no test that spawns
`parosd` processes (behaviour is proved in simulation); CI only builds the image; its Rust version must equal `rust-toolchain.toml`'s channel (CI's `image` job,
`scripts/check-dockerfile-toolchain.sh`). Never run the sandbox's preinstalled binaries
(the `rustup`/`cargo`/`rustc` under `/root/.cargo`), never `apt-get`/`pip install`/`npm -g`/
`brew` (`nix-bin` above is the one exception). On the web the flake's inputs are GitHub tarballs
the egress policy blocks (a 403 is org policy, not a bug to retry), so use a Nix `rustup`, which
reads `rust-toolchain.toml`, plus `protobuf` (`crates/paros/build.rs` runs `prost-build`):

    nix shell nixpkgs#rustup nixpkgs#protobuf -c bash -c 'export PROTOC=$(command -v protoc); cargo build'
    nix shell nixpkgs#rustup nixpkgs#protobuf nixpkgs#cargo-nextest \
      -c bash -c 'export PROTOC=$(command -v protoc); cargo nextest run'

Other tools: `nix shell nixpkgs#<tool> -c …`; a missing tool goes into the flake.

## Workflow rules

- **Record what the user decides, in the same session.** A design decision or preference the
  user states (what paros is, how it behaves) goes into `docs/architecture.md`, dated and with its
  issue; a preference about how to work goes into this file. Amend the section it touches, never
  only a chat, a commit message or an issue comment, and correct a recorded decision the user
  later overturns rather than adding a second one.
- **Meta issue #69** (`meta: up next`) is the rolling backlog pointer, exactly ten issues (raised from three on 2026-10-04);
  update it in the session that merges a PR closing or advancing one (`meta-issue-upkeep` skill).
- **Moonpool questions**: read <https://pierrez.github.io/moonpool/llms.html> before its source
  (`moonpool-consultant` agent).
- **A reusable moonpool gap** (simulator infrastructure, not a paros bug) becomes a focused issue
  in `PierreZ/moonpool`; keep paros-side defense in depth meanwhile (`upstream-to-moonpool`).
  When paros needs the fix, improve moonpool autonomously (decided on 2026-10-09): open the
  moonpool PR, merge it once its CI is green, then advance paros's pin (all eight lines).
- **Never edit a `CHANGELOG.md` by hand**: release-plz generates it; the commit message is where
  a change is described.
- **Simulation before features** (decided on 2026-10-04): when the harness cannot exercise what
  `parosd` ships, or an oracle the doc lists is missing, harness work ranks ahead of new features
  in #69. A feature lands only on a harness that can prove it.
- **Organize as you grow**: a module holds one concern; split a file the moment it holds a
  second. Deleting is part of every change: a superseded axis, type, flag or gate goes in the
  PR that supersedes it.

## Simulation rules

- **Sweep vs. smoke.** The coverage-guided sweep (`cargo xtask sim`, sancov-guided) is the CI
  gate: it exits non-zero on any safety violation and must *saturate* assertion and code
  coverage; prove a red→green result there. The nextest sim tests are a fast smoke
  (`SMOKE_ITERATIONS = 50` seeds through the safety oracles) and assert no saturation. Never
  put a multi-thousand-iteration `explore()` into a nextest test.
- **No pinned seeds.** A seed names a draw schedule, not a scenario: any added or removed draw
  shifts every seed, so a "stays red/green" replay silently stops testing anything. Do not add
  seed constants, seed lists or seed-replay tests; cite a witness in a commit or doc comment
  and let it go. A test may hard-code a seed that is not a witness (a determinism replay, a
  display seed). Reach comes from volume and BUGGIFY.
- **Amplify the worst states** (decided on 2026-10-08): a gate that needs several independent
  coins to line up gets one per-seed scenario draw, its own BUGGIFY location, that turns its
  ingredients on together (`shape::departed_straggler`); each ingredient keeps its own coin
  on the other seeds. Measure a gate's per-seed rate before and after.
- **Hunt budget** (`sim-paros-hunt`): 2,000–3,000 seeds normally, 10,000 only for a substantial
  protocol, harness or fault-model change, more only when the user asks. A hunt's deliverable
  is a failing seed and its diagnosis.
- **Canary** (`sim-paros-hunt canary`, moonpool's `check_determinism`): run a few hundred seeds
  after any change to the harness's randomness, the driver hooks or the process lifecycle.
- **Mutation hunt** (`cargo xtask mutants`, #269): cargo-mutants over the safety-critical
  `paros-core` modules (`.cargo/mutants.toml`), each mutant judged by a fixed-seed hunt of the
  main campaign (`paros_sim::chain_mutants`, seeds `1..=300`, the same for every mutant, so
  survivors compare between runs; not witnesses). Cadence decided on 2026-10-09: **weekly**
  (`.github/workflows/mutants.yml`, 16 shards, plus `workflow_dispatch`), never a PR gate. Its
  survivors land in one rolling issue (label `mutation-survivors`); triage each as an
  equivalent mutant (excluded in `.cargo/mutants.toml` with a reason), a missing oracle, an
  unreachable state, or a mechanism to pin once proven. Hand-made mutation proofs it cannot
  generate (#263, #267) stay documented next to the rule they prove.
- **Assertion budget**: 2048 slots per campaign process (moonpool's `MAX_ASSERTION_SLOTS`) and
  256 `sometimes_each` buckets, shared with moonpool's internals. A slot is the hash of its
  message: never reword a message, keep messages short with no interpolated ids, and never use
  slots, ballots, request ids, seeds or hashes as identities. The sweep explores with forked
  workers, one per core but the controller's (decided on 2026-10-08: same coverage, 1.4x
  faster); replays and focused explorations stay in-process (`workers: 0`). Every workload
  and process is factory-created.

Depth: the `sim-sweep` and `debug-a-seed` skills, `crates/paros-sim-runner/AGENTS.md`.

## The harness in one paragraph

One campaign, one workload, two judges. The **campaign** is a pool of
`NodeProcess::chaotic()` acceptors plus optional matchmakers, proxy leaders, replicas and
joiners, every role storing on the library's journal stores over moonpool's simulated disk,
under every moonpool fault, the driver hooks, the power cuts and the ledgered injector
(`paros_sim::world`). There is no scripted corpus and no fake disk (#261, #263, decided on
2026-10-08): a shape the corpus once scripted is a per-seed BUGGIFY or swarm draw, judged by the
same oracles. The one workload is `ChainWorkload`,
which drives every call through the library client `paros::client`; its misbehaviours (stale
writes, duplicates, dual submits) are explicit calls, never the library's defaults. Every run is
judged by the client's own history (`ClientHistory`, searched for a linearization against the
journal's model) and the shared `AuditWorld`. Roles come from moonpool process groups
(`paros-node`, `paros-matchmaker`, `paros-proxy`, `paros-replica`, `paros-joiner`) through the
deployment/role map `paros_sim::roles`. No third workload, no per-scenario process type, no
check that reads a trace. Depth: `crates/paros-sim/AGENTS.md` (the op-id table, the role map,
the quorum-system and grid draws, the journal and storage coins).

## Architecture

**Sans-IO core, driver outside** (etcd-raft's `RawNode`/`Node` split: `ColocatedNode` in
`paros-core`, `paros::run_node` in `paros`). The core is a pure synchronous state machine:
`step`/`tick` in, one `Ready` out, `advance()`; no I/O, clock, RNG or deps. `ready(&mut self)
-> Ready<'_>` holds the unique borrow, so a second `ready()` before `advance()` is a compile
error. Persist-before-send ordering is documented on `Ready`/`HardState`
(`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`).

**One file per role; `ColocatedNode` is wiring** (timers, messages, the persist-before-send
batch) and holds **no protocol tally of its own**:

- `acceptor.rs` (+ `acceptor/retention.rs`, the floor-moving ops) — promise, records, floor, CTRL
- `proposer.rs` (+ `proposer/rounds.rs` the standalone Phase-2 tally, `proposer/authority.rs`
  the standing authority) — election, repair probe, recovery
- `replica.rs` — chosen prefix, apply walk, journal fold · `journal_state.rs` — the one
  journal-control state machine, judged at apply
- `quorum_read.rs` — leaderless reads · `collector.rs` — the leader-side GC tally
- `membership.rs` — `AcceptorConfig`, `MatchmakerSet`, `QuorumSystem`
- `matchmaking.rs` — the candidate's matchmaking phase · `matchmaker.rs` (+ `reconfigurer.rs`,
  `decree.rs`) — the registry, its generation handover and decree
- `proxy_leader.rs`, `replica_node.rs` — the second and third deployments

The map is `crates/paros-core/AGENTS.md`. **A component must not acquire knowledge merely because
the current deployment colocates it**: the proposer builds no message, the acceptor never reads
the chosen prefix, the replica never sees a ballot tally; the caller hands each the data it
needs. **Every quorum question crosses `membership.rs`**: predicates are phase-split
(`has_phase1_quorum` / `has_phase2_quorum`, cross-intersecting) and no tally compares a count
against a threshold on its own; a new quorum system is a `QuorumSystem` variant, never a tally
rewrite. Policies are explicit types, never flags.

**The driver is written once, generic over moonpool's `P: Providers`** (`run_node`,
`run_journals`, `run_matchmaker`, `run_proxy`, `run_replica`), so the same code runs in
production (`TokioProviders`, `parosd`) and in simulation ("test the code you ship"). Protocol
logic lives there, never in a sim-only path. `paros::client` is likewise provider-generic.

## Plain Multi-Paxos is first-class and permanent; everything else is opt-in

Multi-Paxos without matchmakers — a fixed membership read once from storage — is a permanent
configuration, not a transitional state. Before touching `on_check_leader`, `Election`,
`HardState` or the role map:

- The static case is the **`None` arm of the same state machine**: never a cargo feature, never
  conditional compilation (`serde` and `tracing` are paros-core's only features, observation-only).
- No matchmaker message, no `HardState` field and no extra round trip enters the plain path;
  `ColocatedNode` never steps a matchmaker message. Removing every opt-in feature must leave the
  plain program's behaviour unchanged.
- A reconfiguration request on a deployment without matchmakers is **refused**, never honored.
- Flexible quorums, grids, matchmakers, proxies and replicas are **configuration data**, never
  implied by code being present, and in simulation each is drawn per seed so one campaign proves
  every mode. Every peer and client message carries a `JournalIdentifier` `(TenantId, JournalId)`;
  under that identifier the plain deployment exchanges the same messages and persists the same
  scalars.

## Matchmaking, reconfiguration, GC, generations

- **Matchmaking, then Phase 1.** A candidate registers `(b, C_b)` at a matchmaker quorum before
  any `Prepare`; histories are unioned above the **maximum** watermark into `H_b`. Phase 1 needs
  a promise quorum of **every** configuration in `H_b` — never `quorum(union)` — and Phase 2
  addresses `C_b` alone. A refusal abandons the campaign; a slow one is re-asked on election
  timeouts, never abandoned by the clock.
- **A belief is not a fact.** A registration is a belief (an ordinary campaign) or a
  reconfiguration; the **effective configuration** is the highest-ballot reconfiguration a
  quorum holds. A campaign whose histories name another abandons, adopts it and re-campaigns
  (`MatchStep::StaleConfiguration`); beliefs never trigger that.
- **Membership probe** (`MembershipProbe`): every incarnation boots believing the bootstrap
  configuration and asks a matchmaker quorum for the effective one, registering nothing, before
  its first campaign or skip. A node only registers a belief it heard. A node whose heard belief
  leaves it outside re-probes on every election timeout, and a probe adopts only a strictly
  newer configuration (#270).
- **A reconfiguration is a round change** (`ColocatedNode::reconfigure`): a configuration is bound
  to a ballot and never edited; the leader re-campaigns with `C_new`. **Removed is not shut
  down**: a removed node keeps answering Phase 1 for ballots it took part in (acceptor guards are
  pool-based, never configuration-based).
- **GC**: a configuration is forgotten only once no future leader can need its Phase-1 quorum. A
  floor is **effective only once a matchmaker quorum acked it durably** (`GcStep::Effective`);
  only then are acceptors retirable. The compaction floor and the GC watermark never wait on
  each other.
- **Retire carries the evidence**: the operator sends the effective watermark and
  `ColocatedNode::may_retire` checks five legs (matchmakers, not a member, not the leader,
  watermark above `last_member_ballot`, belief bound to exactly that watermark). The operator's
  half: retire no node a reconfiguration it asked for above the floor names.
- **Matchmaker sets are generations**: every matchmaking message is fenced by generation;
  matchmaker quorums are **majorities only**; the handover's decree is the shared `Proposer` +
  `Acceptor` over a one-slot log (no second Paxos kernel) and opens strictly above the stop
  quorum's maximum decree promise; any node that meets `Stopped { successor: None }` finishes
  the handover. `MatchmakerSet::new` / `AcceptorConfig::new` are the only constructors.

Depth: module docs of `matchmaking.rs`, `node/matchmaking.rs`, `node/reconfigure.rs`,
`node/gc.rs` (`may_retire`), `matchmaker.rs`, `matchmaker/reconfigurer.rs`,
`matchmaker/handover_model.rs`; `docs/analysis/consensus/matchmaker-gc-and-generations.md` and
`matchmaker-interaction-verification.md`.

## Journals, system journals, storage, boot safety

- **Share processes, disks and connections, never protocol state.** Each journal has its own
  `ColocatedNode`, ballots, log and store; the one cross-journal property is non-interference.
  The `JournalIdentifier { tenant, journal }` (#235) rides the `Deliver` envelope per message
  (never a fingerprint) and every public call; the driver demuxes on the pair before the core;
  each journal has its own peer-mailbox lane. Ids are random or minted by the one writer that can
  check them, never a log position. **No id is fixed and none has a default**
  (`docs/architecture.md` §3.8): `0` is unset and the only value with a meaning, there is no
  reserved range and no well-known tenant or journal; the control journals' identifiers are drawn at
  `init`, recorded in the cell plan and learned through a node-only `Inspect`
  (`paros::machine::ControlJournals`); an `Inspect` names its journal or asks for the node alone;
  the sim draws every identifier per seed (`paros_sim::shape::Identifiers`). Stores live at
  `journals/<tenant>/<journal>/`.
- **A storage fault quarantines its journal, not the process**; it re-opens after
  `DriverTunables::quarantine_ticks`. A seam crash is the process dying, for every journal.
- **System journals** (the directory = a user tenant's control journal, the node registry = the
  cell tenant's control journal, both identifiers in the `SystemPlan`) are opt-in through a
  `SystemPlan` (`None` is the static deployment), folded by `paros::system::{Directory,
  Registry}`; a created journal's id is drawn by its creator and checked at apply (`IdTaken`: the creator
  redraws), never reused. The core's pool grows, never shrinks
  (`extend_pool`), and only with matchmakers. The registry is keyed by `node_id` with each
  machine's class and capacity, judges capacity bookings at apply (#211), and is checkpointed
  and truncated with `paros::client::checkpoint` (#230); a `stateless` machine never serves a
  journal.
- **The fleet tenant** (`paros::fleet`, #229; named *meta* until #244) holds the fleet directory
  (`FleetDirectory`): tenant → cell plus the cell entries, hosted by the fleet's one cell. Every
  tenant has a set of groups (`paros::fleet::Groups`), fixed at registration: the fleet tenant
  `{internal, fleet}`, a cell tenant `{internal, cell}` (both created only by
  `init`/adding a cell), a served tenant `{users}` (the tenant API's); only `cell` forbids a move,
  and there is no per-tenant placement. Fleet operations (`init`'s fleet half, tenant create
  and remove) are idempotent state machines over the fleet tenant and the cell control journal
  (`paros::client::fleet`): one write per step, every entry fenced by its fleet id, resumed from
  what the journals hold. A tenant is created once: a creation is named by its drawn identifier, and
  any other creation of a held name is refused (`NameTaken`); finishing an interrupted one is the
  coordinator's job (#225), never another client's.
- **The storage seam is async** (`LogStorage` / `MatchmakerStorage`: every device-touching method
  returns a `Send` future, awaited in persist-before-send order); the core's recovery ports
  stay synchronous, served from memory after `boot_scan`. Production stores are
  `paros::journal` on `moonpool-journal`, a CLSTORE journal: the state itself (slot = position,
  ballot in the entry's identity, scalars in its two-copy metainfo), no fold, no checkpoint.
  `JournalStores::open` is async: an opener resolves an interrupted format from the disk at every
  open, a quarantined journal's re-open included.
- **Boot safety is the library's job.** Every store carries a format marker; `run_node` /
  `run_matchmaker` take the operator's `BootKind` as data and refuse `Amnesia` (an existing member
  with no marker), `AlreadyFormatted` and `ConfigMismatch` (the marker records the `Config`; the
  membership, quorum system and counts are never edited across a restart). **A wiped node never
  rejoins**: a trim jump restores the floor, not the promise. The acceptor set heals around it by
  reconfiguration; a wiped matchmaker is replaced by a handover.

Depth: `crates/paros/AGENTS.md`; module docs of `driver/journals.rs`, `driver/system.rs`,
`driver/boot.rs`, `storage/mod.rs`, `journal/mod.rs`, `matchmaker/mod.rs`.

## Turbulence layers

Three layers, and nothing crosses them:

- **Environmental faults belong to moonpool** (drop, delay, duplicate, reorder, partitions,
  close, attrition, scheduling), swarm-masked per seed, on **one combined campaign axis**
  (`chaos_surfaces()`); after `CHAOS_DURATION_MS` moonpool enters recovery mode, so the tail is
  a genuine recovery and liveness oracles apply. Never re-implement one in paros or re-split
  the axis. One exception (decided on 2026-10-09): on a seed that draws the departed-straggler
  scenario, its outage may strike early in the tail, once the owner's removal took effect and
  at most `LATE_WINDOW` in (`paros_sim::world::late_outage`); the rest of the tail is still the
  recovery the oracles judge. Likewise on a seed that draws the bare-quorum scenario, an outage
  strikes inside the chaos window the moment a decided slot lacks a member's copy
  (`paros_sim::world::bare_outage`, #270).
- **`paros-core` is never buggified**: no RNG, knob or conditional compilation. A rare-but-valid
  decision is exposed as a method with an honest contract (`resend_pending`, `step_down`) and
  perturbed only by a caller that stops calling.
- **Prong 1, `DriverHooks`**: the driver's rare-but-valid choices, one `buggify_with_prob!`
  location each in `paros-sim` (`NoHooks` in production). Consult a hook only where its answer
  has an observable effect, trace what happened, quiet disruptive hooks after the chaos window.
  Hooks are consulted **only from the node loop**, which is compile-enforced (`H: DriverHooks`
  is deliberately not `Send + 'static`); a decision a spawned task needs is carried to it.
  Each durability `Seam` (`BeforeSync`, `AfterSyncBeforeSend`, `MatchBeforeSync`,
  `MatchAfterSyncBeforeReply`, and `AfterPrepareSent`, a reconfiguring candidate dying with its
  campaign in flight, #260) is its own location.
- **Prong 2, knobs**: anything that shapes a run is config data the harness draws per seed, one
  `buggify_knob!` per tunable, born that way. **Every knob documents its floor**: an extreme
  must stay a valid, winnable configuration (a queue that cannot hold one tick's traffic is a
  partition, not a knob). **Never buggified**: oracle thresholds (`DEPOSED_TICK_SLACK`,
  `PLATEAU_SEEDS`, `CHAOS_DURATION_MS`, `SETTLE`, `WAIT_SETTLE`) and schedule ceilings
  (`*_ITERATIONS`). Constants a correctness argument depends on (`MAX_TORN_TAIL`) are not tunables.

Depth: the `adding-a-buggify-site` skill, `crates/paros/src/hooks.rs`.

## Audit, correctness, assertions, spans

- **The audit observes, never perturbs.** `paros::Audit` (`NoAudit` in production) reports each
  meaningful transition once, typed, where its `tracing` event is; it returns nothing, draws no
  randomness, reads no clock.
- **Correctness lives in `paros_sim::audit` and the workload's `check()`**, folded into O(1)
  incremental state — never a scan of the trace. A missing fact gets a new `Audit` callback.
- **`paros-core` uses hard `assert!`**, on in release; no `debug_assert!` anywhere. Never assert
  on external input (operating errors are results). `ColocatedNode::assert_invariants` runs at
  boot and every public mutating entry; public functions that assert document `# Panics`.
- **Assertions are TigerStyle** (decided on 2026-10-04, #248): in `paros-core` and the drivers of
  `paros`, an average of at least two assertions per function; assert preconditions,
  postconditions and invariants; **pair** assertions (check a property where data is written and
  again where it is read back, e.g. at persist and at boot); assert the negative space as well as
  the positive; assert relationships between constants at compile time (`const _: () =
  assert!(..)`); split compound conditions into one assertion each. Every assertion is a
  simulation oracle for free. The external-input rule above still holds: a bad request is a
  result, never a panic. Reference: TigerBeetle's `TIGER_STYLE.md`.
- **Sim layers use moonpool macros, never plain `assert!`**: `assert_always!` with a detail map
  (record and continue), `assert_sometimes!` for an **outcome** the run must reach,
  `reach_once!`/`assert_reachable!` for a **cause** that fired. A perturbation never gets a
  `sometimes`; every BUGGIFY site is paired with a reachable.
- **Spans**: `#[tracing::instrument]` is non-optional on the important methods of `paros` and
  `paros-sim`; in `paros-core` write `#[cfg_attr(feature = "tracing", tracing::instrument(..))]`.
  Use `skip_all` plus a few cheap `fields` (node id, `from`, `round`, `slot`); entry points at
  `debug`, per-message and per-tick at `trace`; no `ret`/`err`. Spans are for humans only.

Depth: the `adding-an-audit-check` and `changing-paros-core` skills.

## Protocol invariants to remember

- **Truncation is a Paxos-decided control command** (`Truncate`, proposed by the leader), **fenced
  by `(generation, owner)` like a `Write`** (#228): judged at apply, a stale or foreign caller is
  refused in place; an accepted one raises `first_seq` and every node compacts lazily when its
  walk reaches the slot. paros runs no application and takes no snapshot; record bytes are
  opaque.
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

## Simulation-driven development

The deterministic simulation is the source of truth. For a suspected safety or liveness bug:

1. State the invariant it would violate.
2. Make it reachable (chaos, `buggify!`/`buggify_knob!`); build a missing harness capability.
3. Put the check where the fact arrives (audit, workload `check()`, storage callbacks).
4. Run the sweep: **red** on the unfixed code; replay that seed while working.
5. Fix `paros-core`.
6. Run the sweep: **green** and saturated.
7. Record red→green in the commit and the doc comment of the rule it proved; let the seed go.

A unit test may pin a mechanism afterwards, never replace step 4. A claim the simulation cannot
reproduce is unproven: no speculative defensive code. Growing the sim surface is part of every
change: a new path lands with its gates, hooks and knobs, and new BUGGIFY sites go wherever a
rare-but-valid state should become likely. References:
[BUGGIFY](https://transactional.blog/simulation/buggify),
[Designing Rust FDB Workloads That Actually Find Bugs](https://pierrezemb.fr/posts/writing-rust-fdb-workloads-that-find-bugs/).
Procedure: the `simulation-driven-fix` skill.

## Layout

Cargo workspace, every package under `crates/`. Dependency stack: `paros-core` ← `paros` ←
`paros-sim` ← `paros-sim-runner`, and `paros` ← `parosd`. Each crate's `AGENTS.md` is its map.

- `paros-core` — the sans-IO roles and `ColocatedNode`; dependency-free with
  `default-features = false`, wasm-safe; sancov crate-under-test.
- `paros` — the library: provider-generic drivers, RPC contract (`proto/`, built by
  `prost-build`), stores, `paros::client`; wasm-safe and provider-free.
- `parosd` — the uniform `parosd` daemon over Tokio (one binary per machine: `PAROS_*` config,
  `node_id` minted at format, waits for `parosctl init`, #196) and `parosctl`
  (`src/bin/parosctl/`), the CLI over `paros::client` (`publish = false`). The image and the
  Compose toy are `Dockerfile` and `docker-compose.yml` at the root; `DEMO.md` is how to run it.
- `paros-sim` — the DST harness: processes, role map, storage ledger and injector, workload, audit.
- `paros-sim-runner` — `sim-paros-chain` and `sim-paros-hunt` (`publish = false`).
- `paros-play` — the interactive Paxos game's engine and wasm glue; the app is `web/play/`.
- `xtask` — `cargo xtask sim` (the sancov runner) and `cargo xtask mutants` (the mutation hunt, #269).

Elsewhere: `book/` (mdbook; `book/CLAUDE.md`, the `update-the-book` skill),
`docs/architecture.md`, `docs/analysis/` (design notes), `docs/references/` (papers and source
references), `scripts/` (`sancov-rustc.sh`, `build-play.sh`, `check-dockerfile-toolchain.sh`, `mutants-report.sh`), `.claude/skills/` and
`.claude/agents/`.

Publishing mirrors moonpool: library crates share a release-plz `version_group` with per-crate
`CHANGELOG.md`; binaries and xtask are `publish = false`. `paros`, `paros-sim` and `parosd` pin
moonpool by **git** rev (eight lines across their `Cargo.toml`s, advanced together), so only
`paros-core` is truly publishable.
