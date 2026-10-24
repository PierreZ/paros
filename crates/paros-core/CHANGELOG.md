# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

## [0.2.0] - 2026-10-24

### 🚀 Features

- **parosd**: Run every binary on one compio thread ([#719](https://github.com/PierreZ/paros/pull/719))
- **paros-sim**: Keep chaos on for the whole run ([#454](https://github.com/PierreZ/paros/pull/454)) ([#510](https://github.com/PierreZ/paros/pull/510))
- TLS and mutual TLS on every session, parosctl tls, TLS on half the sim seeds ([#509](https://github.com/PierreZ/paros/pull/509))
- Place matchmaker sets off the founders and move them on drain ([#501](https://github.com/PierreZ/paros/pull/501)) ([#503](https://github.com/PierreZ/paros/pull/503))
- **sim**: CPU model, gray failures and a slow-machine scenario (#424 step 2) ([#437](https://github.com/PierreZ/paros/pull/437))
- Busyness metrics in parosctl, step 1 of #424 (busyness metrics) ([#427](https://github.com/PierreZ/paros/pull/427))
- **paros**: Read page and wait_ms limits, journal API page ([#241](https://github.com/PierreZ/paros/pull/241)) ([#328](https://github.com/PierreZ/paros/pull/328))
- Journal API — Write, Read, Truncate, SetLeader and the journal state machine ([#204](https://github.com/PierreZ/paros/pull/204)) ([#217](https://github.com/PierreZ/paros/pull/217))
- Convergence budget ([#177](https://github.com/PierreZ/paros/pull/177)), matchmaker format marker ([#183](https://github.com/PierreZ/paros/pull/183)), system journals ([#189](https://github.com/PierreZ/paros/pull/189)) ([#200](https://github.com/PierreZ/paros/pull/200))
- **paros**: The replica tier, driver and sim half; quorum reads on replicas; bare acceptors ([#144](https://github.com/PierreZ/paros/pull/144)) ([#172](https://github.com/PierreZ/paros/pull/172))
- **core**: The replica tier, core half (#144 part A) ([#171](https://github.com/PierreZ/paros/pull/171))
- **paros**: Quorum reads, driver and sim half (#143 part B) ([#169](https://github.com/PierreZ/paros/pull/169))
- **core**: Read-only accessors the game asked for ([#155](https://github.com/PierreZ/paros/pull/155)) ([#168](https://github.com/PierreZ/paros/pull/168))
- **paros**: Proxy leaders — part B of #142, the driver and the process group ([#159](https://github.com/PierreZ/paros/pull/159))
- **core**: Proxy leaders — part A of #142, plus six core refactors ([#158](https://github.com/PierreZ/paros/pull/158))
- **core**: Cooperative leader handoff (DPaxos "Leader Handoff") ([#118](https://github.com/PierreZ/paros/pull/118))
- Stage 8 — disk faults C: protocol-aware recovery (CTRL) ([#21](https://github.com/PierreZ/paros/pull/21)) ([#112](https://github.com/PierreZ/paros/pull/112))
- **core**: Plumb configuration identities ([#109](https://github.com/PierreZ/paros/pull/109))
- TigerBeetle-style assertions in the core; moonpool assertions at the sim storage seam
- **core**: CheckQuorum — a leader without an ack quorum for an election timeout steps down

### 🐛 Bug Fixes

- One matchmaker host, held set slots, shared quorum reads, campaign probe floor, fast follow pages ([#718](https://github.com/PierreZ/paros/pull/718))
- Keep-alive flood, election silence, set thinning and sweep oracle gaps ([#705](https://github.com/PierreZ/paros/pull/705))
- Make main's sweep green after PR #515 ([#529](https://github.com/PierreZ/paros/pull/529)) ([#530](https://github.com/PierreZ/paros/pull/530))
- **sim**: Chaos at sane rates; no founder renamed before every founder formed (#495 (chaos window)) ([#508](https://github.com/PierreZ/paros/pull/508))
- **paros-core**: A paging campaign keeps its promisers quiet, #428 (paging livelock) ([#431](https://github.com/PierreZ/paros/pull/431))
- **paros-core**: A won election learns its read basis before recovery runs (#262, #263)
- **paros-core**: Judge quorum reads over a won leadership's basis, never a campaign's ([#262](https://github.com/PierreZ/paros/pull/262))
- A node probes before acting on its bootstrap belief ([#173](https://github.com/PierreZ/paros/pull/173)); operators coordinate retirements ([#198](https://github.com/PierreZ/paros/pull/198)) ([#197](https://github.com/PierreZ/paros/pull/197))
- **core**: A node retires only on a belief bound to the floor ([#165](https://github.com/PierreZ/paros/pull/165)) ([#178](https://github.com/PierreZ/paros/pull/178))
- **core**: Bound promise and recovery batches ([#106](https://github.com/PierreZ/paros/pull/106))
- **core**: #94 at-most-once — session ledger travels with truncation and snapshots; duplicates suppress at the apply seam
- **core**: Land the adversarial review's findings; arm the at-most-once oracle
- **core**: Chosen acks require an executed seq, never seq <= latest
- **core**: A candidate refuses an election win below its own promise

### 📚 Documentation

- Add Claude Code skills, agents and crate-local AGENTS.md ([#139](https://github.com/PierreZ/paros/pull/139))
- **core**: Three runnable teaching examples for the composable roles ([#135](https://github.com/PierreZ/paros/pull/135))

### ⚡ Performance

- **paros**: Adopt moonpool#352 (recycle segments), drop the spare task ([#710](https://github.com/PierreZ/paros/pull/710))
- O(1) acceptor and replica invariants (#619 (O(n) invariants)) ([#622](https://github.com/PierreZ/paros/pull/622))
- **paros**: Spare segments, RWF_DSYNC and no bounce buffer, moonpool pin to 4bab77e ([#642](https://github.com/PierreZ/paros/pull/642))

### 🚜 Refactor

- Abstraction cleanup across core, drivers, client and play ([#249](https://github.com/PierreZ/paros/pull/249))
- Second simplify pass across every crate ([#170](https://github.com/PierreZ/paros/pull/170))
- Simplify and factorize every crate ([#167](https://github.com/PierreZ/paros/pull/167))
- Delete the inert wire plumbing, settle the open judgment calls, advance the moonpool pin ([#137](https://github.com/PierreZ/paros/pull/137))
- **core**: Split node into protocol modules ([#108](https://github.com/PierreZ/paros/pull/108))

### 🧪 Testing

- **core**: Adapt the unit corpus to CheckQuorum; pin its contract
- **sim**: Pin the #94/#95-arc regression seeds; clippy/doc polish

### ⚙️ Miscellaneous Tasks

- Advance the moonpool pin to the deployment handle (moonpool#328 (deployment handle)) ([#507](https://github.com/PierreZ/paros/pull/507))
- Bump Rust to 1.99 and update the Nix flake
- Organize rust crates under crates

### 📦 Other

- Batchers and tenant role counts (#145 (batchers), #604 (role counts)) ([#698](https://github.com/PierreZ/paros/pull/698))
- The shared wasm build and one paros-wasm crate (#307 (paros-sim in wasm), #540 (one wasm crate)) ([#704](https://github.com/PierreZ/paros/pull/704))
- Roll machine wipes: one unhealed wipe at a time (moonpool #344 (rolling wipes)) ([#689](https://github.com/PierreZ/paros/pull/689))
- Disk full goes read-only, admission control answers busy (#360 (disk full)) ([#683](https://github.com/PierreZ/paros/pull/683))
- Split docs/architecture.md into docs/architecture/ (#675 (split the architecture doc)) ([#676](https://github.com/PierreZ/paros/pull/676))
- Background scrubbing of stored records (#362 (background scrubbing)) ([#631](https://github.com/PierreZ/paros/pull/631))
- Fix main's red sweep: a uuid leads one term, a passed leader proposes nothing, moves keep their size and retire only drained members, reads skip a silent server, and oracle fixes ([#648](https://github.com/PierreZ/paros/pull/648))
- Fix main's red sweep: a stalled catch-up pull falls back to the whole pool ([#641](https://github.com/PierreZ/paros/pull/641))
- Balanced journal geometry, and the small store layout removed (#600 (drop the small layout)) ([#624](https://github.com/PierreZ/paros/pull/624))
- Fix main's red sweep: a mode change leaves a journal on a gone machine to its move ([#629](https://github.com/PierreZ/paros/pull/629))
- Re-place after the Down bound, maintenance and removal, retire coordination in the registry (#475, #359, #477) ([#559](https://github.com/PierreZ/paros/pull/559))
- Failure domains in AcceptorConfig, the zone rule, a zone-outage scenario (#215 (zones)) ([#595](https://github.com/PierreZ/paros/pull/595))
- Restart recovery: write a lagging chosen index (#602 (dark after a full restart)) ([#612](https://github.com/PierreZ/paros/pull/612))
- Fix main's red sweep, parts 6 to 9: matchmaker pages, parked-call verdicts, tenant cell steps, grid back-off, founder re-teach ([#590](https://github.com/PierreZ/paros/pull/590))
- Fix main's red sweep, part 5: a learned chosen slot closes a campaign; reseed on a first registration ([#577](https://github.com/PierreZ/paros/pull/577))
- Acceptor partitions and moving a stuck round (#532 (acceptor partitions), #533 (move a stuck round)) ([#562](https://github.com/PierreZ/paros/pull/562))
- Operator calls name their journal, the handover is paged (#439 (operator calls name their journal)) ([#551](https://github.com/PierreZ/paros/pull/551))
- Split membership.rs into one file per type ([#420](https://github.com/PierreZ/paros/pull/420)) ([#514](https://github.com/PierreZ/paros/pull/514))
- One set per tenant, a registry per journal (#190 (matchmakers everywhere)) ([#434](https://github.com/PierreZ/paros/pull/434))
- Pin the wire-hygiene checks with unit tests (#344 (malformed wire input)) ([#440](https://github.com/PierreZ/paros/pull/440))
- Hold a node's pool admission so a configuration names an unpooled joiner (#387 (unpooled joiner in a configuration)) ([#436](https://github.com/PierreZ/paros/pull/436))
- Split-floor outage reaches a leader's repair probe at a trim point (#409 (repair probe blocked below a trim point)) ([#432](https://github.com/PierreZ/paros/pull/432))
- Draw the 64-entry page bounds per seed (#338 64-entry bounds, mutation survivors) ([#411](https://github.com/PierreZ/paros/pull/411))
- Coalesce beats per lane, so a slow link still confirms quorum reads (#386 slow peer link starves quorum reads) ([#406](https://github.com/PierreZ/paros/pull/406))
- A stalled-proxy scenario, catching the #341 mutation survivors (proxy take-back on a grid, eviction, supersession) ([#391](https://github.com/PierreZ/paros/pull/391))
- Judge every compaction, catching the #342 mutation survivors (compaction progress, #204 retained retry) ([#389](https://github.com/PierreZ/paros/pull/389))
- Catch the #343 mutation survivors (probe quorum, repair re-query, pool, fold hole) ([#385](https://github.com/PierreZ/paros/pull/385))
- An acceptor campaigns before an overwrite of its value (#376 two-values gate is rare) ([#378](https://github.com/PierreZ/paros/pull/378))
- Mutation hunt: triage the first run and kill its survivors ([#269](https://github.com/PierreZ/paros/pull/269)) ([#334](https://github.com/PierreZ/paros/pull/334))
- A per-seed recovery page size, so multi-page recovery is reachable ([#330](https://github.com/PierreZ/paros/pull/330)) ([#345](https://github.com/PierreZ/paros/pull/345))
- An outage waits until no victim has a budgeted commit in flight ([#332](https://github.com/PierreZ/paros/pull/332)) ([#336](https://github.com/PierreZ/paros/pull/336))
- The P2 and P3 coverage probes ([#324](https://github.com/PierreZ/paros/pull/324)) ([#329](https://github.com/PierreZ/paros/pull/329))
- The three per-seed latches are named BUGGIFY locations; DriverHooks is gone (#318 E) ([#327](https://github.com/PierreZ/paros/pull/327))
- An assertions feature for coverage probes ([#317](https://github.com/PierreZ/paros/pull/317)) ([#322](https://github.com/PierreZ/paros/pull/322))
- Add the multi-writer journal mode ([#241](https://github.com/PierreZ/paros/pull/241)) ([#296](https://github.com/PierreZ/paros/pull/296))
- Cell init: a single-decree Paxos on the cell plan, no seeds ([#277](https://github.com/PierreZ/paros/pull/277)) ([#289](https://github.com/PierreZ/paros/pull/289))
- Leader uuid fences the journal; the term never reaches clients ([#241](https://github.com/PierreZ/paros/pull/241)) ([#281](https://github.com/PierreZ/paros/pull/281))
- A removed candidate steps down before it re-probes its membership ([#283](https://github.com/PierreZ/paros/pull/283))
- The membership probe no longer wedges a cluster outside its belief ([#278](https://github.com/PierreZ/paros/pull/278)) ([#282](https://github.com/PierreZ/paros/pull/282))
- Prove the departed straggler's rule; bare-quorum and lost-verdict scenarios ([#267](https://github.com/PierreZ/paros/pull/267)) ([#270](https://github.com/PierreZ/paros/pull/270))
- Dead paths ([#243](https://github.com/PierreZ/paros/pull/243)), renames ([#244](https://github.com/PierreZ/paros/pull/244)), control-plane oracles ([#247](https://github.com/PierreZ/paros/pull/247)), TigerStyle first pass ([#248](https://github.com/PierreZ/paros/pull/248)) ([#250](https://github.com/PierreZ/paros/pull/250))
- Meta tenant, fleet operations and no fixed ids ([#229](https://github.com/PierreZ/paros/pull/229)) ([#238](https://github.com/PierreZ/paros/pull/238))
- Fenced Truncate ([#228](https://github.com/PierreZ/paros/pull/228)), the (TenantId, JournalId) frame ([#235](https://github.com/PierreZ/paros/pull/235)), uniform parosd + Compose ([#196](https://github.com/PierreZ/paros/pull/196)) ([#236](https://github.com/PierreZ/paros/pull/236))
- Paros::client ([#221](https://github.com/PierreZ/paros/pull/221)) and parosctl ([#220](https://github.com/PierreZ/paros/pull/220)); AGENTS.md and skills refreshed ([#223](https://github.com/PierreZ/paros/pull/223))
- Parosd over Tokio and real disks ([#206](https://github.com/PierreZ/paros/pull/206)), Config durable at format ([#207](https://github.com/PierreZ/paros/pull/207)) ([#219](https://github.com/PierreZ/paros/pull/219))
- A linearizability checker over the four calls, and the three races as sim shapes ([#205](https://github.com/PierreZ/paros/pull/205)) ([#218](https://github.com/PierreZ/paros/pull/218))
- Journal API, no application, many journals per process, sim on JournalStorage (#185, #186, #188, #187) ([#199](https://github.com/PierreZ/paros/pull/199))
- The matchmaker interaction, verified: two protocol fixes, six model-checker claims, the design note, and the accept starvation the hunt found ([#151](https://github.com/PierreZ/paros/pull/151))
- Rounds extracted (#142 rung 0), quorum reads core half ([#143](https://github.com/PierreZ/paros/pull/143)), acceptor grid sim half ([#141](https://github.com/PierreZ/paros/pull/141)) ([#150](https://github.com/PierreZ/paros/pull/150))
- Acceptor grid (core half, #141) and flexible quorums drawn per seed (sim half, #140) ([#149](https://github.com/PierreZ/paros/pull/149))
- Flexible quorums (core half, #140), the chunk-restore seam visited ([#146](https://github.com/PierreZ/paros/pull/146)), the wiped-identity format marker ([#147](https://github.com/PierreZ/paros/pull/147)) ([#148](https://github.com/PierreZ/paros/pull/148))
- Review of #133: protocol fixes, the composable core, the driver, and the simulation's reach ([#134](https://github.com/PierreZ/paros/pull/134))
- Matchmaker GC, reconfiguration under the full fault matrix, matchmaker-set generations (#123, #124, #125) ([#133](https://github.com/PierreZ/paros/pull/133))
- Leader matchmaking phase, cross-configuration Phase 1, online reconfiguration (#120, #121, #122) ([#132](https://github.com/PierreZ/paros/pull/132))
- Matchmaker state machine, durable configuration registry, and a per-seed deployment role map ([#119](https://github.com/PierreZ/paros/pull/119)) ([#131](https://github.com/PierreZ/paros/pull/131))
- Instrument every important method; core spans behind a default-on `tracing` feature ([#130](https://github.com/PierreZ/paros/pull/130))
- Sim harness: node shape survives restarts, frontier-tied convergence, hook coverage; core: local protocol assertions ([#129](https://github.com/PierreZ/paros/pull/129))
- One workload, one audit, everything knobbed — the paros-sim simplification ([#128](https://github.com/PierreZ/paros/pull/128))
- Retire the red demos and every pinned seed; widen the BUGGIFY surface ([#127](https://github.com/PierreZ/paros/pull/127))
- Audit follow-up, randomness half: repair-plane chaos, chunk-repair seams, born-buggified knobs ([#117](https://github.com/PierreZ/paros/pull/117))
- Audit follow-up: strengthen core asserts, audit oracles, and the BUGGIFY surface ([#116](https://github.com/PierreZ/paros/pull/116))
- M3 finishing batch: CTRL evaluation corpus ([#113](https://github.com/PierreZ/paros/pull/113)) + Control::Snap chunked snapshot repair ([#101](https://github.com/PierreZ/paros/pull/101)) ([#114](https://github.com/PierreZ/paros/pull/114))


### 🚀 Features

- **core**: the candidate's matchmaking phase is a public role,
  `paros_core::matchmaking::Matchmaking` — the registration tally, the union
  of histories above the maximum watermark (`H_b`), the effective
  configuration and the stale-belief signal — folded page by page
  (`RegisteredPage`, `MatchFold`) exactly as `ColocatedNode` drives it. The
  node keeps the wiring; the role can now be composed by hand.
- **core**: `MemRegistry`, the reference in-memory `RegistryStorage` with the
  library's semantics for every `MatchmakerWriteOp`, replaces the four ad-hoc
  test registries; the handover model checker now reboots matchmakers from it.
- **core**: `Decree::ballot`, `value`, `adopted_prior_vote` and `preempted`
  are public, so a driver can observe a running decree through
  `ReconfigurerPhase::Deciding`.

### 📚 Documentation

- **core**: three runnable, deterministic examples that drive the composable
  roles by hand — `single_decree` (Phase 1, P2c, Phase 2), `multi_paxos`
  (slots versus ballots, amortized Phase 1, per-slot recovery) and
  `matchmaker` (configuration discovery, reconfiguration, and the matchmaker
  set chosen by the same single-decree Paxos over `Vec<MatchmakerId>`). Run
  with `cargo run -p paros-core --example <name>`.
- **core**: every intra-doc link in the crate resolves; CI builds the docs
  with warnings denied.

### 🚜 Refactor

- **core**: `RawNode` is renamed `ColocatedNode`. The type is not a "raw"
  anything: it is the deployment that colocates the `Acceptor`, `Proposer`
  and `Replica` roles on one node and wires them together. The driver half of
  the etcd-raft split it mirrors keeps its name, `paros::run_node`, so only
  the type and its methods are affected — a caller renames the type and
  nothing else.
