# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-11

### 🚀 Features

- **sim**: CPU model, gray failures and a slow-machine scenario (#424 step 2) ([#437](https://github.com/PierreZ/paros/pull/437))
- Busyness metrics in parosctl, step 1 of #424 (busyness metrics) ([#427](https://github.com/PierreZ/paros/pull/427))
- **machine**: Resolve answered by every machine of a cell (#216 finish bootstrap) ([#408](https://github.com/PierreZ/paros/pull/408))
- **machine**: Durable cached registry fold on every machine (#211 machine registry) ([#388](https://github.com/PierreZ/paros/pull/388))
- **paros**: Peer batches carry the cell id; a foreign cell is refused ([#216](https://github.com/PierreZ/paros/pull/216)) ([#335](https://github.com/PierreZ/paros/pull/335))
- **paros**: Read page and wait_ms limits, journal API page ([#241](https://github.com/PierreZ/paros/pull/241)) ([#328](https://github.com/PierreZ/paros/pull/328))
- **paros-sim**: Make the departed straggler reachable inside the chaos window (#263, #267)
- **paros-sim**: Every role on the journal stores; delete the world stores and the corpus (#261, #263)
- **paros-sim**: A ledgered journal-aware injector ([#261](https://github.com/PierreZ/paros/pull/261))
- Paros stores on the CLSTORE moonpool-journal, with storage chaos on journal seeds ([#256](https://github.com/PierreZ/paros/pull/256))
- Journal API — Write, Read, Truncate, SetLeader and the journal state machine ([#204](https://github.com/PierreZ/paros/pull/204)) ([#217](https://github.com/PierreZ/paros/pull/217))
- Convergence budget ([#177](https://github.com/PierreZ/paros/pull/177)), matchmaker format marker ([#183](https://github.com/PierreZ/paros/pull/183)), system journals ([#189](https://github.com/PierreZ/paros/pull/189)) ([#200](https://github.com/PierreZ/paros/pull/200))
- **paros**: Durable node and matchmaker stores on moonpool-journal ([#175](https://github.com/PierreZ/paros/pull/175))
- **paros**: Speak moonpool-rpc instead of gRPC ([#174](https://github.com/PierreZ/paros/pull/174))
- **paros**: The replica tier, driver and sim half; quorum reads on replicas; bare acceptors ([#144](https://github.com/PierreZ/paros/pull/144)) ([#172](https://github.com/PierreZ/paros/pull/172))
- **core**: The replica tier, core half (#144 part A) ([#171](https://github.com/PierreZ/paros/pull/171))
- **paros**: Quorum reads, driver and sim half (#143 part B) ([#169](https://github.com/PierreZ/paros/pull/169))
- **core**: Read-only accessors the game asked for ([#155](https://github.com/PierreZ/paros/pull/155)) ([#168](https://github.com/PierreZ/paros/pull/168))
- **paros**: Proxy leaders — part B of #142, the driver and the process group ([#159](https://github.com/PierreZ/paros/pull/159))
- **core**: Proxy leaders — part A of #142, plus six core refactors ([#158](https://github.com/PierreZ/paros/pull/158))
- **paros**: Make the NodeStorage and MatchmakerStorage seams async ([#136](https://github.com/PierreZ/paros/pull/136))
- **core**: Cooperative leader handoff (DPaxos "Leader Handoff") ([#118](https://github.com/PierreZ/paros/pull/118))
- Stage 8 — disk faults C: protocol-aware recovery (CTRL) ([#21](https://github.com/PierreZ/paros/pull/21)) ([#112](https://github.com/PierreZ/paros/pull/112))
- **storage**: Stage 7 disk faults B — corruption detection (CLStore) ([#111](https://github.com/PierreZ/paros/pull/111))
- **storage**: Stage 6 disk faults A — fail-stop ([#19](https://github.com/PierreZ/paros/pull/19)) ([#110](https://github.com/PierreZ/paros/pull/110))
- **core**: Plumb configuration identities ([#109](https://github.com/PierreZ/paros/pull/109))
- **core**: CheckQuorum — a leader without an ack quorum for an election timeout steps down
- **sim**: Widen the BUGGIFY surface; drift-immune gap-wedge detection
- **sim**: Make the #88 mid-election snapshot window reachable
- **sim**: Raise the odds of the #88/#80/#60 scenarios per seed
- **sim**: Saturate guided chain exploration
- **sim**: Add chain state-machine campaign

### 🐛 Bug Fixes

- **paros-core**: A paging campaign keeps its promisers quiet, #428 (paging livelock) ([#431](https://github.com/PierreZ/paros/pull/431))
- **paros**: Cell init heals around a wiped founding member ([#323](https://github.com/PierreZ/paros/pull/323))
- **paros**: Journal stores order dependent state across commits (#264, #176)
- **paros-core**: Judge quorum reads over a won leadership's basis, never a campaign's ([#262](https://github.com/PierreZ/paros/pull/262))
- A node probes before acting on its bootstrap belief ([#173](https://github.com/PierreZ/paros/pull/173)); operators coordinate retirements ([#198](https://github.com/PierreZ/paros/pull/198)) ([#197](https://github.com/PierreZ/paros/pull/197))
- **core**: A node retires only on a belief bound to the floor ([#165](https://github.com/PierreZ/paros/pull/165)) ([#178](https://github.com/PierreZ/paros/pull/178))
- **core**: Bound promise and recovery batches ([#106](https://github.com/PierreZ/paros/pull/106))
- **paros**: Ack-on-commit verifies the decided command is the waiter's own
- **paros**: A durable compaction floor never outruns the durable application state
- **core**: #94 at-most-once — session ledger travels with truncation and snapshots; duplicates suppress at the apply seam
- **core**: Land the adversarial review's findings; arm the at-most-once oracle
- **paros**: The tick deadline is absolute, not a fresh sleep per select pass
- **sim**: Drive the mid-election snapshot gate from the driver's role check
- Close grpc channels on shutdown

### 📚 Documentation

- Map the provisioning probe's settle (#348 false amnesia refusal) ([#393](https://github.com/PierreZ/paros/pull/393))
- Remove merge-conflict markers from crates/paros/AGENTS.md ([#294](https://github.com/PierreZ/paros/pull/294)) ([#298](https://github.com/PierreZ/paros/pull/298))
- Faults live in the shipped code as hint! and inline buggify ([#294](https://github.com/PierreZ/paros/pull/294)) ([#295](https://github.com/PierreZ/paros/pull/295))
- Add Claude Code skills, agents and crate-local AGENTS.md ([#139](https://github.com/PierreZ/paros/pull/139))
- **core**: Three runnable teaching examples for the composable roles ([#135](https://github.com/PierreZ/paros/pull/135))
- Add project logo and README quick reference

### 🚜 Refactor

- **paros-sim**: Withhold_gc's gate where it fires; stale world-store docs (#261, #263)
- Abstraction cleanup across core, drivers, client and play ([#249](https://github.com/PierreZ/paros/pull/249))
- Second simplify pass across every crate ([#170](https://github.com/PierreZ/paros/pull/170))
- Simplify and factorize every crate ([#167](https://github.com/PierreZ/paros/pull/167))
- Delete the inert wire plumbing, settle the open judgment calls, advance the moonpool pin ([#137](https://github.com/PierreZ/paros/pull/137))
- **sim**: Move correctness checking from trace scanning to an audit port
- **rpc**: Encode consensus traffic with protobuf

### 🧪 Testing

- **sim**: Compare external replica digests
- **sim**: Pin the #94/#95-arc regression seeds; clippy/doc polish

### ⚙️ Miscellaneous Tasks

- Bump Rust to 1.99 and update the Nix flake
- Organize rust crates under crates

### 📦 Other

- Authorize in names, resolve, forward; run it in the simulation (#192 (the frontend)) ([#412](https://github.com/PierreZ/paros/pull/412))
- Init passes a machine admitted at a founder's address (#423 init refuses a healthy cell) ([#425](https://github.com/PierreZ/paros/pull/425))
- Draw the 64-entry page bounds per seed (#338 64-entry bounds, mutation survivors) ([#411](https://github.com/PierreZ/paros/pull/411))
- Admin CLI views: machine, cell, tenant and roles, answered by one cell (#399 admin CLI views) ([#407](https://github.com/PierreZ/paros/pull/407))
- Coalesce beats per lane, so a slow link still confirms quorum reads (#386 slow peer link starves quorum reads) ([#406](https://github.com/PierreZ/paros/pull/406))
- A retried journal request after its tenant's removal reads unknown_tenant ([#395](https://github.com/PierreZ/paros/pull/395)) ([#405](https://github.com/PierreZ/paros/pull/405))
- One contest per overwrite, and a torn grid column (#396 contest_overwrite duels forever) ([#397](https://github.com/PierreZ/paros/pull/397))
- A stalled-proxy scenario, catching the #341 mutation survivors (proxy take-back on a grid, eviction, supersession) ([#391](https://github.com/PierreZ/paros/pull/391))
- Settle a store before trusting its format marker (#348 false amnesia refusal) ([#384](https://github.com/PierreZ/paros/pull/384))
- Catch the #343 mutation survivors (probe quorum, repair re-query, pool, fold hole) ([#385](https://github.com/PierreZ/paros/pull/385))
- A resigned term never comes back (#246 simulate the uniform parosd machine) ([#380](https://github.com/PierreZ/paros/pull/380))
- Checkpoints in many slots: a run of Begin, Chunk and End records ([#353](https://github.com/PierreZ/paros/pull/353)) ([#381](https://github.com/PierreZ/paros/pull/381))
- An acceptor campaigns before an overwrite of its value (#376 two-values gate is rare) ([#378](https://github.com/PierreZ/paros/pull/378))
- Tenant control journal: journal create and delete as real RPCs ([#210](https://github.com/PierreZ/paros/pull/210)) ([#347](https://github.com/PierreZ/paros/pull/347))
- #349 (registry address books): peers dial the registry's addresses, and machines register a moved address ([#377](https://github.com/PierreZ/paros/pull/377))
- Machine registry: bookings by role, incarnations and liveness ([#211](https://github.com/PierreZ/paros/pull/211)) ([#352](https://github.com/PierreZ/paros/pull/352))
- Split listen and advertised addresses ([#257](https://github.com/PierreZ/paros/pull/257)) ([#350](https://github.com/PierreZ/paros/pull/350))
- Names at the edge: paros://tenant/journal and short hex ids ([#239](https://github.com/PierreZ/paros/pull/239)) ([#346](https://github.com/PierreZ/paros/pull/346))
- Mutation hunt: triage the first run and kill its survivors ([#269](https://github.com/PierreZ/paros/pull/269)) ([#334](https://github.com/PierreZ/paros/pull/334))
- The cell election over a multi-writer journal, a lease as a liveness hint ([#240](https://github.com/PierreZ/paros/pull/240)) ([#337](https://github.com/PierreZ/paros/pull/337))
- A per-seed recovery page size, so multi-page recovery is reachable ([#330](https://github.com/PierreZ/paros/pull/330)) ([#345](https://github.com/PierreZ/paros/pull/345))
- An outage waits until no victim has a budgeted commit in flight ([#332](https://github.com/PierreZ/paros/pull/332)) ([#336](https://github.com/PierreZ/paros/pull/336))
- Cell add-machine admits an idle machine into a cell ([#216](https://github.com/PierreZ/paros/pull/216)) ([#319](https://github.com/PierreZ/paros/pull/319))
- The P2 and P3 coverage probes ([#324](https://github.com/PierreZ/paros/pull/324)) ([#329](https://github.com/PierreZ/paros/pull/329))
- The three per-seed latches are named BUGGIFY locations; DriverHooks is gone (#318 E) ([#327](https://github.com/PierreZ/paros/pull/327))
- Every per-call DriverHooks choice is an inline BUGGIFY site (#318 A-D) ([#321](https://github.com/PierreZ/paros/pull/321))
- An assertions feature for coverage probes ([#317](https://github.com/PierreZ/paros/pull/317)) ([#322](https://github.com/PierreZ/paros/pull/322))
- The Zola + Goyo skeleton at web/site/, the book ported, Pages on Zola ([#306](https://github.com/PierreZ/paros/pull/306)) ([#320](https://github.com/PierreZ/paros/pull/320))
- Shed a stale peer backlog per journal lane ([#299](https://github.com/PierreZ/paros/pull/299)) ([#303](https://github.com/PierreZ/paros/pull/303))
- The re-sends and election choices are inline BUGGIFY sites ([#294](https://github.com/PierreZ/paros/pull/294)) ([#302](https://github.com/PierreZ/paros/pull/302))
- A power loss mid-commit is the journal stores' own hint!; PowerCut goes ([#294](https://github.com/PierreZ/paros/pull/294)) ([#300](https://github.com/PierreZ/paros/pull/300))
- The driver seams are hint!s; RunError::SeamCrash and restart_delay! go ([#294](https://github.com/PierreZ/paros/pull/294)) ([#297](https://github.com/PierreZ/paros/pull/297))
- Add the multi-writer journal mode ([#241](https://github.com/PierreZ/paros/pull/241)) ([#296](https://github.com/PierreZ/paros/pull/296))
- Hint! the cell init moments, no sim wrapper around run_machine ([#246](https://github.com/PierreZ/paros/pull/246)) ([#292](https://github.com/PierreZ/paros/pull/292))
- Refuse a batch over the node's limits at the edge ([#241](https://github.com/PierreZ/paros/pull/241)) ([#290](https://github.com/PierreZ/paros/pull/290))
- Cell init: a single-decree Paxos on the cell plan, no seeds ([#277](https://github.com/PierreZ/paros/pull/277)) ([#289](https://github.com/PierreZ/paros/pull/289))
- Leader uuid fences the journal; the term never reaches clients ([#241](https://github.com/PierreZ/paros/pull/241)) ([#281](https://github.com/PierreZ/paros/pull/281))
- The membership probe no longer wedges a cluster outside its belief ([#278](https://github.com/PierreZ/paros/pull/278)) ([#282](https://github.com/PierreZ/paros/pull/282))
- Simulated parosd machines, formed by the workload's init ([#246](https://github.com/PierreZ/paros/pull/246)) ([#279](https://github.com/PierreZ/paros/pull/279))
- Init whole is paros::client::initialize ([#246](https://github.com/PierreZ/paros/pull/246)) ([#275](https://github.com/PierreZ/paros/pull/275))
- Prove the departed straggler's rule; bare-quorum and lost-verdict scenarios ([#267](https://github.com/PierreZ/paros/pull/267)) ([#270](https://github.com/PierreZ/paros/pull/270))
- The machine lifecycle is the library's (#246, part 1)
- Advance moonpool to 0a68199; the outage is moonpool's, the journals lose their extra commit (#176, #263, #264)
- Advance the moonpool pin to 160d897 (moonpool#305), with the two bugs the new draws found ([#251](https://github.com/PierreZ/paros/pull/251))
- Dead paths ([#243](https://github.com/PierreZ/paros/pull/243)), renames ([#244](https://github.com/PierreZ/paros/pull/244)), control-plane oracles ([#247](https://github.com/PierreZ/paros/pull/247)), TigerStyle first pass ([#248](https://github.com/PierreZ/paros/pull/248)) ([#250](https://github.com/PierreZ/paros/pull/250))
- Meta tenant, fleet operations and no fixed ids ([#229](https://github.com/PierreZ/paros/pull/229)) ([#238](https://github.com/PierreZ/paros/pull/238))
- Multi-journal matchmaker seeds ([#201](https://github.com/PierreZ/paros/pull/201)), checkpoint library ([#230](https://github.com/PierreZ/paros/pull/230)), registry with class/capacity/bookings ([#211](https://github.com/PierreZ/paros/pull/211)) ([#237](https://github.com/PierreZ/paros/pull/237))
- Fenced Truncate ([#228](https://github.com/PierreZ/paros/pull/228)), the (TenantId, JournalId) frame ([#235](https://github.com/PierreZ/paros/pull/235)), uniform parosd + Compose ([#196](https://github.com/PierreZ/paros/pull/196)) ([#236](https://github.com/PierreZ/paros/pull/236))
- Parosd provision ([#208](https://github.com/PierreZ/paros/pull/208)), hostnames and production tunables ([#209](https://github.com/PierreZ/paros/pull/209)), client hint fix ([#224](https://github.com/PierreZ/paros/pull/224))
- Paros::client ([#221](https://github.com/PierreZ/paros/pull/221)) and parosctl ([#220](https://github.com/PierreZ/paros/pull/220)); AGENTS.md and skills refreshed ([#223](https://github.com/PierreZ/paros/pull/223))
- Parosd over Tokio and real disks ([#206](https://github.com/PierreZ/paros/pull/206)), Config durable at format ([#207](https://github.com/PierreZ/paros/pull/207)) ([#219](https://github.com/PierreZ/paros/pull/219))
- A linearizability checker over the four calls, and the three races as sim shapes ([#205](https://github.com/PierreZ/paros/pull/205)) ([#218](https://github.com/PierreZ/paros/pull/218))
- Journal API, no application, many journals per process, sim on JournalStorage (#185, #186, #188, #187) ([#199](https://github.com/PierreZ/paros/pull/199))
- Advance the moonpool pin to ed4f338 (h2 reset streams expire by count; in-flight faults and TCP flow control) ([#152](https://github.com/PierreZ/paros/pull/152))
- The matchmaker interaction, verified: two protocol fixes, six model-checker claims, the design note, and the accept starvation the hunt found ([#151](https://github.com/PierreZ/paros/pull/151))
- Rounds extracted (#142 rung 0), quorum reads core half ([#143](https://github.com/PierreZ/paros/pull/143)), acceptor grid sim half ([#141](https://github.com/PierreZ/paros/pull/141)) ([#150](https://github.com/PierreZ/paros/pull/150))
- Acceptor grid (core half, #141) and flexible quorums drawn per seed (sim half, #140) ([#149](https://github.com/PierreZ/paros/pull/149))
- Flexible quorums (core half, #140), the chunk-restore seam visited ([#146](https://github.com/PierreZ/paros/pull/146)), the wiped-identity format marker ([#147](https://github.com/PierreZ/paros/pull/147)) ([#148](https://github.com/PierreZ/paros/pull/148))
- Advance moonpool to the single-stream canary and put the campaign under it ([#138](https://github.com/PierreZ/paros/pull/138))
- Review of #133: protocol fixes, the composable core, the driver, and the simulation's reach ([#134](https://github.com/PierreZ/paros/pull/134))
- Matchmaker GC, reconfiguration under the full fault matrix, matchmaker-set generations (#123, #124, #125) ([#133](https://github.com/PierreZ/paros/pull/133))
- Leader matchmaking phase, cross-configuration Phase 1, online reconfiguration (#120, #121, #122) ([#132](https://github.com/PierreZ/paros/pull/132))
- Matchmaker state machine, durable configuration registry, and a per-seed deployment role map ([#119](https://github.com/PierreZ/paros/pull/119)) ([#131](https://github.com/PierreZ/paros/pull/131))
- Instrument every important method; core spans behind a default-on `tracing` feature ([#130](https://github.com/PierreZ/paros/pull/130))
- Sim harness: node shape survives restarts, frontier-tied convergence, hook coverage; core: local protocol assertions ([#129](https://github.com/PierreZ/paros/pull/129))
- One workload, one audit, everything knobbed — the paros-sim simplification ([#128](https://github.com/PierreZ/paros/pull/128))
- Retire the red demos and every pinned seed; widen the BUGGIFY surface ([#127](https://github.com/PierreZ/paros/pull/127))
- Unify network chaos into the main campaign (moonpool 43304d8); driver: per-kind keep-newest peer mailbox ([#126](https://github.com/PierreZ/paros/pull/126))
- Audit follow-up, randomness half: repair-plane chaos, chunk-repair seams, born-buggified knobs ([#117](https://github.com/PierreZ/paros/pull/117))
- Audit follow-up: strengthen core asserts, audit oracles, and the BUGGIFY surface ([#116](https://github.com/PierreZ/paros/pull/116))
- M3 finishing batch: CTRL evaluation corpus ([#113](https://github.com/PierreZ/paros/pull/113)) + Control::Snap chunked snapshot repair ([#101](https://github.com/PierreZ/paros/pull/101)) ([#114](https://github.com/PierreZ/paros/pull/114))

