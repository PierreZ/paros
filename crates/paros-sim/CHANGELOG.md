# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-24

### 🚀 Features

- **parosd**: Run every binary on one compio thread ([#719](https://github.com/PierreZ/paros/pull/719))
- **paros**: Durability mode per journal, ordered or batched (#599 (durability mode per journal)) ([#643](https://github.com/PierreZ/paros/pull/643))
- **paros**: Drawn internal client tunables, Resolve folds that escape (#457 (internal clients)) ([#516](https://github.com/PierreZ/paros/pull/516))
- **paros-sim**: Keep chaos on for the whole run ([#454](https://github.com/PierreZ/paros/pull/454)) ([#510](https://github.com/PierreZ/paros/pull/510))
- TLS and mutual TLS on every session, parosctl tls, TLS on half the sim seeds ([#509](https://github.com/PierreZ/paros/pull/509))
- Place matchmaker sets off the founders and move them on drain ([#501](https://github.com/PierreZ/paros/pull/501)) ([#503](https://github.com/PierreZ/paros/pull/503))
- **paros**: Journals on admitted machines, and moves off a draining machine (#474 (journals on admitted machines)) ([#499](https://github.com/PierreZ/paros/pull/499))
- **paros**: The status words and parosctl status (#191 (status)) ([#494](https://github.com/PierreZ/paros/pull/494))
- **paros**: Placement books a journal's acceptors before its create (#212 (placement)) ([#478](https://github.com/PierreZ/paros/pull/478))
- **sim**: CPU model, gray failures and a slow-machine scenario (#424 step 2) ([#437](https://github.com/PierreZ/paros/pull/437))
- Busyness metrics in parosctl, step 1 of #424 (busyness metrics) ([#427](https://github.com/PierreZ/paros/pull/427))
- **machine**: Resolve answered by every machine of a cell (#216 finish bootstrap) ([#408](https://github.com/PierreZ/paros/pull/408))
- **machine**: Durable cached registry fold on every machine (#211 machine registry) ([#388](https://github.com/PierreZ/paros/pull/388))
- **paros**: Peer batches carry the cell id; a foreign cell is refused ([#216](https://github.com/PierreZ/paros/pull/216)) ([#335](https://github.com/PierreZ/paros/pull/335))
- **paros**: Read page and wait_ms limits, journal API page ([#241](https://github.com/PierreZ/paros/pull/241)) ([#328](https://github.com/PierreZ/paros/pull/328))
- **paros-sim**: Let the departed straggler's outage strike early in the tail ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-sim**: Amplify the two gates CI's sweep left unfired ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-sim**: Draw the departed-straggler scenario as one per-seed coin ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-sim**: Make the departed straggler reachable inside the chaos window (#263, #267)
- **paros-sim**: A correlated outage provokes the CTRL shapes; the audit judges them from the ledger ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-sim**: Every role on the journal stores; delete the world stores and the corpus (#261, #263)
- **paros-sim**: A ledgered journal-aware injector ([#261](https://github.com/PierreZ/paros/pull/261))
- **paros-sim**: Journal stores on every perturbed seed, matchmakers on JournalMatchmakerStorage (#176, #264)
- Paros stores on the CLSTORE moonpool-journal, with storage chaos on journal seeds ([#256](https://github.com/PierreZ/paros/pull/256))
- Journal API — Write, Read, Truncate, SetLeader and the journal state machine ([#204](https://github.com/PierreZ/paros/pull/204)) ([#217](https://github.com/PierreZ/paros/pull/217))
- Convergence budget ([#177](https://github.com/PierreZ/paros/pull/177)), matchmaker format marker ([#183](https://github.com/PierreZ/paros/pull/183)), system journals ([#189](https://github.com/PierreZ/paros/pull/189)) ([#200](https://github.com/PierreZ/paros/pull/200))
- **paros**: Durable node and matchmaker stores on moonpool-journal ([#175](https://github.com/PierreZ/paros/pull/175))
- **paros**: Speak moonpool-rpc instead of gRPC ([#174](https://github.com/PierreZ/paros/pull/174))
- **paros**: The replica tier, driver and sim half; quorum reads on replicas; bare acceptors ([#144](https://github.com/PierreZ/paros/pull/144)) ([#172](https://github.com/PierreZ/paros/pull/172))
- **core**: The replica tier, core half (#144 part A) ([#171](https://github.com/PierreZ/paros/pull/171))
- **paros**: Quorum reads, driver and sim half (#143 part B) ([#169](https://github.com/PierreZ/paros/pull/169))
- **paros**: Proxy leaders — part B of #142, the driver and the process group ([#159](https://github.com/PierreZ/paros/pull/159))
- **core**: Proxy leaders — part A of #142, plus six core refactors ([#158](https://github.com/PierreZ/paros/pull/158))
- **paros**: Make the NodeStorage and MatchmakerStorage seams async ([#136](https://github.com/PierreZ/paros/pull/136))
- **core**: Cooperative leader handoff (DPaxos "Leader Handoff") ([#118](https://github.com/PierreZ/paros/pull/118))
- Stage 8 — disk faults C: protocol-aware recovery (CTRL) ([#21](https://github.com/PierreZ/paros/pull/21)) ([#112](https://github.com/PierreZ/paros/pull/112))
- **storage**: Stage 7 disk faults B — corruption detection (CLStore) ([#111](https://github.com/PierreZ/paros/pull/111))
- **storage**: Stage 6 disk faults A — fail-stop ([#19](https://github.com/PierreZ/paros/pull/19)) ([#110](https://github.com/PierreZ/paros/pull/110))
- **core**: Plumb configuration identities ([#109](https://github.com/PierreZ/paros/pull/109))
- TigerBeetle-style assertions in the core; moonpool assertions at the sim storage seam
- **core**: CheckQuorum — a leader without an ack quorum for an election timeout steps down
- **sim**: Red-seed hunt harness — raw-iteration network sweep + replay binary
- **sim**: Close out #61 — full cluster-size swarm, regime gates, witnesses
- **sim**: Widen the BUGGIFY surface; drift-immune gap-wedge detection
- **sim**: Make the #88 mid-election snapshot window reachable
- **sim**: Raise the odds of the #88/#80/#60 scenarios per seed
- **sim**: Saturate guided chain exploration
- **sim**: Add chain state-machine campaign

### 🐛 Bug Fixes

- One matchmaker host, held set slots, shared quorum reads, campaign probe floor, fast follow pages ([#718](https://github.com/PierreZ/paros/pull/718))
- Election watch renews between pages, mode change waits for down members, dead incarnation's term ([#711](https://github.com/PierreZ/paros/pull/711)) ([#712](https://github.com/PierreZ/paros/pull/712))
- Keep-alive flood, election silence, set thinning and sweep oracle gaps ([#705](https://github.com/PierreZ/paros/pull/705))
- **paros-sim**: An init that lists a removed machine is refused as retired (#359 (drain, remove and maintenance for machines)) ([#696](https://github.com/PierreZ/paros/pull/696))
- **paros-sim**: Excuse a decided slot a torn re-accept froze under the CTRL rule ([#697](https://github.com/PierreZ/paros/pull/697))
- **paros-sim**: Judge a deposed actor where it sends, not where a frontend relays ([#695](https://github.com/PierreZ/paros/pull/695))
- **paros-sim**: A tenant dropped inside a registry checkpoint owes no convergence ([#694](https://github.com/PierreZ/paros/pull/694))
- **paros-sim**: Judge a move's quorum excuse by the configuration in force ([#693](https://github.com/PierreZ/paros/pull/693))
- **election**: A reader the floor outruns waits at the tail ([#640](https://github.com/PierreZ/paros/pull/640))
- **sets**: Size a matchmaker set over the machines that can take a slot ([#634](https://github.com/PierreZ/paros/pull/634))
- Make main's sweep green after PR #515 ([#529](https://github.com/PierreZ/paros/pull/529)) ([#530](https://github.com/PierreZ/paros/pull/530))
- **paros**: Placement books only machines that serve the cell ([#511](https://github.com/PierreZ/paros/pull/511)) ([#515](https://github.com/PierreZ/paros/pull/515))
- **paros-sim**: Unknown writes land only where the pages show their records (#512 (search budget)) ([#527](https://github.com/PierreZ/paros/pull/527))
- Stabilize main ([#525](https://github.com/PierreZ/paros/pull/525))
- **sim**: Chaos at sane rates; no founder renamed before every founder formed (#495 (chaos window)) ([#508](https://github.com/PierreZ/paros/pull/508))
- Fold a boot's unreported floor, spare thin matchmaker sets, tell a behind matchmaker its set (#496 (two red hunt seeds)) ([#500](https://github.com/PierreZ/paros/pull/500))
- **sim**: A misdirected cell init ends no operator init ([#438](https://github.com/PierreZ/paros/pull/438))
- **paros-core**: A paging campaign keeps its promisers quiet, #428 (paging livelock) ([#431](https://github.com/PierreZ/paros/pull/431))
- **paros**: Cell init heals around a wiped founding member ([#323](https://github.com/PierreZ/paros/pull/323))
- **paros-sim**: Recognize the departed straggler against the latest bound configuration ([#267](https://github.com/PierreZ/paros/pull/267))
- **paros-sim**: Count an accept its own commit's floor dropped ([#264](https://github.com/PierreZ/paros/pull/264))
- **paros-sim**: Judge an outage's lost slot by every copy the leader can reach ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-sim**: An entry the open reports faulty is never aimed at again ([#261](https://github.com/PierreZ/paros/pull/261))
- **paros-sim**: The injector remembers a rotted persist record until it is re-written
- **paros-sim**: A replica is never a holder of an outage's lost slot ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-sim**: An outage's loss is never claimed unrecoverable over a ballot the audit does not know ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-sim**: A Reconfigure reply's leader is adopted only where it speaks for this journal
- **paros-sim**: A replica's chosen-index watermark restarts at zero again ([#263](https://github.com/PierreZ/paros/pull/263))
- **paros-core**: Judge quorum reads over a won leadership's basis, never a campaign's ([#262](https://github.com/PierreZ/paros/pull/262))
- A node probes before acting on its bootstrap belief ([#173](https://github.com/PierreZ/paros/pull/173)); operators coordinate retirements ([#198](https://github.com/PierreZ/paros/pull/198)) ([#197](https://github.com/PierreZ/paros/pull/197))
- **core**: A node retires only on a belief bound to the floor ([#165](https://github.com/PierreZ/paros/pull/165)) ([#178](https://github.com/PierreZ/paros/pull/178))
- **core**: Bound promise and recovery batches ([#106](https://github.com/PierreZ/paros/pull/106))
- **paros**: Ack-on-commit verifies the decided command is the waiter's own
- **core**: #94 at-most-once — session ledger travels with truncation and snapshots; duplicates suppress at the apply seam
- **core**: Land the adversarial review's findings; arm the at-most-once oracle
- **sim**: Tighten audit gates per the equivalence review
- **sim**: Keep-alive on workload client channels — pin seed 6442591786636745658
- **sim**: Drive the mid-election snapshot gate from the driver's role check
- **sim**: Gate native replay regression
- Close grpc channels on shutdown

### 📚 Documentation

- M9 lists #245 as landed; sim: root-key gates fire every sweep ([#545](https://github.com/PierreZ/paros/pull/545))
- Map the provisioning probe's settle (#348 false amnesia refusal) ([#393](https://github.com/PierreZ/paros/pull/393))
- Faults live in the shipped code as hint! and inline buggify ([#294](https://github.com/PierreZ/paros/pull/294)) ([#295](https://github.com/PierreZ/paros/pull/295))
- **paros-sim**: Record the mutation proofs of the outage's oracles ([#263](https://github.com/PierreZ/paros/pull/263))
- Add Claude Code skills, agents and crate-local AGENTS.md ([#139](https://github.com/PierreZ/paros/pull/139))

### ⚡ Performance

- **paros**: Adopt moonpool#352 (recycle segments), drop the spare task ([#710](https://github.com/PierreZ/paros/pull/710))
- **paros**: Spare segments, RWF_DSYNC and no bounce buffer, moonpool pin to 4bab77e ([#642](https://github.com/PierreZ/paros/pull/642))

### 🚜 Refactor

- **paros-sim**: Withhold_gc's gate where it fires; stale world-store docs (#261, #263)
- Abstraction cleanup across core, drivers, client and play ([#249](https://github.com/PierreZ/paros/pull/249))
- Second simplify pass across every crate ([#170](https://github.com/PierreZ/paros/pull/170))
- Simplify and factorize every crate ([#167](https://github.com/PierreZ/paros/pull/167))
- Delete the inert wire plumbing, settle the open judgment calls, advance the moonpool pin ([#137](https://github.com/PierreZ/paros/pull/137))
- **sim**: Move correctness checking from trace scanning to an audit port
- **rpc**: Encode consensus traffic with protobuf

### 🧪 Testing

- **sim**: Expand adversarial chain alphabet ([#105](https://github.com/PierreZ/paros/pull/105))
- **sim**: Compare external replica digests
- **sim**: Observe pre-application chosen gaps ([#103](https://github.com/PierreZ/paros/pull/103))
- **sim**: Pin the #94/#95-arc regression seeds; clippy/doc polish

### ⚙️ Miscellaneous Tasks

- Advance the moonpool pin to the deployment handle (moonpool#328 (deployment handle)) ([#507](https://github.com/PierreZ/paros/pull/507))
- Bump Rust to 1.99 and update the Nix flake
- **paros-sim**: Explore with forked workers in the sweep
- Require deterministic collections
- Organize rust crates under crates

### 📦 Other

- Batchers and tenant role counts (#145 (batchers), #604 (role counts)) ([#698](https://github.com/PierreZ/paros/pull/698))
- Machine lifecycle oracles (#654 (live machine marked down), #657 (minted node id is new), #661 (maintenance windows)) ([#706](https://github.com/PierreZ/paros/pull/706))
- The shared wasm build and one paros-wasm crate (#307 (paros-sim in wasm), #540 (one wasm crate)) ([#704](https://github.com/PierreZ/paros/pull/704))
- Election oracles: cross-journal check on every seed, term install order; coordinator installs only over a judged leader (bundle D) ([#703](https://github.com/PierreZ/paros/pull/703))
- Client write inputs: late abandonment, edge-limit batches, conflicting rewrite (bundle A) ([#691](https://github.com/PierreZ/paros/pull/691))
- Judge verdicts no client received, burst multi-writer appends (#656, #662) ([#692](https://github.com/PierreZ/paros/pull/692))
- Roll machine wipes: one unhealed wipe at a time (moonpool #344 (rolling wipes)) ([#689](https://github.com/PierreZ/paros/pull/689))
- Fix main's red: the convergence check reads a journal's stops through the tail ([#690](https://github.com/PierreZ/paros/pull/690))
- Disk full goes read-only, admission control answers busy (#360 (disk full)) ([#683](https://github.com/PierreZ/paros/pull/683))
- The machines' registry gets the registry fold oracles (#650 (registry fold oracles on the machines)) ([#682](https://github.com/PierreZ/paros/pull/682))
- Fix main's red: a lost matchmaker set excuses a quiet journal's convergence ([#688](https://github.com/PierreZ/paros/pull/688))
- Fix main's reds: a coordinator's watch outlives its term, a write passes a server that lacks the journal, a lost matchmaker set ([#684](https://github.com/PierreZ/paros/pull/684))
- Fix main's red sweep: election fold at the tail, a store released while down, a set shrink under maintenance, a superseded writer's duplicate, a node's own echo ([#681](https://github.com/PierreZ/paros/pull/681))
- Clean Ctrl-C shutdown: hand off leaderships, flush, then exit (#560 (clean Ctrl-C shutdown)) ([#679](https://github.com/PierreZ/paros/pull/679))
- Fix main's red sweep: a read-back proof behind its own pipeline, the mid-batch truncation gate, four sim shards ([#680](https://github.com/PierreZ/paros/pull/680))
- Acked writes judged by their bytes, full alphabet on created journals, mid-batch truncation (#651 (acked bytes), #658 (full alphabet), #659 (partial truncation)) ([#678](https://github.com/PierreZ/paros/pull/678))
- Split docs/architecture.md into docs/architecture/ (#675 (split the architecture doc)) ([#676](https://github.com/PierreZ/paros/pull/676))
- Fix main's red sweep: five reds (sets check, election re-anchor, a moved founder's lane, the install in-flight report, a create on an unsettled fold) ([#677](https://github.com/PierreZ/paros/pull/677))
- Fix main's red sweep: the fault ledger keeps damaged copies it stopped holding ([#666](https://github.com/PierreZ/paros/pull/666))
- Fix main's clippy: the scrub pace leaves NodeShape::draw (#362 (background scrubbing)) ([#649](https://github.com/PierreZ/paros/pull/649))
- Background scrubbing of stored records (#362 (background scrubbing)) ([#631](https://github.com/PierreZ/paros/pull/631))
- Fix main's red sweep: the zone-outage scenario on a full coin (#215 (zones)) ([#646](https://github.com/PierreZ/paros/pull/646))
- Fix main's red sweep: a uuid leads one term, a passed leader proposes nothing, moves keep their size and retire only drained members, reads skip a silent server, and oracle fixes ([#648](https://github.com/PierreZ/paros/pull/648))
- A journal's flush leaves the node loop, with group commit (#613 (sync hand-off)) ([#639](https://github.com/PierreZ/paros/pull/639))
- Fix main's red sweep: an Accepted for a slot its own batch compacted, a served term follows a renamed founder, a retired joiner's copy serves no slot ([#644](https://github.com/PierreZ/paros/pull/644))
- Fix main: an unsynced flip's sector, and an Admit to a machine driving cell init ([#647](https://github.com/PierreZ/paros/pull/647))
- Fix main's red sweep: the maintenance oracle and a tenant's own mode change ([#359](https://github.com/PierreZ/paros/pull/359)), init resumed whole ([#605](https://github.com/PierreZ/paros/pull/605)), four marginal zones gates ([#215](https://github.com/PierreZ/paros/pull/215)) ([#636](https://github.com/PierreZ/paros/pull/636))
- Mark slow machines and move roles off them (#361 (slow machines)) ([#610](https://github.com/PierreZ/paros/pull/610))
- Fix main's red sweep: a failed registry sync keeps its commit in flight ([#627](https://github.com/PierreZ/paros/pull/627))
- Add and remove cells: idle cells, universe add-cell and remove-cell (#232 (multiple cells), part 1) ([#593](https://github.com/PierreZ/paros/pull/593))
- Fix main's red sweep: an operator re-runs an interrupted init whole (#605 (init pending in the recovery tail)) ([#645](https://github.com/PierreZ/paros/pull/645))
- Frontend and created-journal oracles (#449, #450, #451) ([#625](https://github.com/PierreZ/paros/pull/625))
- A leader's Accepts leave before its own flush (#578 (leader sends Accept before its own fsync)) ([#626](https://github.com/PierreZ/paros/pull/626))
- Balanced journal geometry, and the small store layout removed (#600 (drop the small layout)) ([#624](https://github.com/PierreZ/paros/pull/624))
- Several writes in flight per journal (#597 (several writes in flight per journal)) ([#637](https://github.com/PierreZ/paros/pull/637))
- Re-place after the Down bound, maintenance and removal, retire coordination in the registry (#475, #359, #477) ([#559](https://github.com/PierreZ/paros/pull/559))
- Failure domains in AcceptorConfig, the zone rule, a zone-outage scenario (#215 (zones)) ([#595](https://github.com/PierreZ/paros/pull/595))
- Restart recovery: write a lagging chosen index (#602 (dark after a full restart)) ([#612](https://github.com/PierreZ/paros/pull/612))
- Fix main's red sweep: init patience, per-journal follow seeds, admission founders (#605, #607, #608) ([#606](https://github.com/PierreZ/paros/pull/606))
- Value tags on machines, cells, tenants and journals (#575 (key:value tags)) ([#587](https://github.com/PierreZ/paros/pull/587))
- Tenant modes: configure a journal's durability (#214 (tenant modes)) ([#574](https://github.com/PierreZ/paros/pull/574))
- Client read-back settles a write as Written (#564 (client read-back settles a write)) ([#569](https://github.com/PierreZ/paros/pull/569))
- Fix main's red sweep, parts 6 to 9: matchmaker pages, parked-call verdicts, tenant cell steps, grid back-off, founder re-teach ([#590](https://github.com/PierreZ/paros/pull/590))
- Fix main's red sweep, part 10: hold a founder rename until a renamed machine registered ([#589](https://github.com/PierreZ/paros/pull/589))
- Metrics, probes, usage ([#366](https://github.com/PierreZ/paros/pull/366)) and the bench ([#194](https://github.com/PierreZ/paros/pull/194)) ([#567](https://github.com/PierreZ/paros/pull/567))
- Three founding members by default, admit the other listed machines ([#568](https://github.com/PierreZ/paros/pull/568))
- Tenant birth footprint, capacity request RPC, tenant coordinator placement ([#476](https://github.com/PierreZ/paros/pull/476)) ([#556](https://github.com/PierreZ/paros/pull/556))
- Acceptor partitions and moving a stuck round (#532 (acceptor partitions), #533 (move a stuck round)) ([#562](https://github.com/PierreZ/paros/pull/562))
- Fix main's red sweep, part 4: internal call floor, the coordinator joins the cell (#565, #513) ([#566](https://github.com/PierreZ/paros/pull/566))
- Fix main's red sweep, part 3: settled sets, take-back ceiling, admit report, paging gates ([#561](https://github.com/PierreZ/paros/pull/561))
- Fix main's red sweep, part 2: resolve, set handover, election, audit, connect floor (#549, #548, #542) ([#555](https://github.com/PierreZ/paros/pull/555))
- Journal-tagged proxy leaders and replicas (#193 (journal-tagged proxy leaders)) ([#557](https://github.com/PierreZ/paros/pull/557))
- Operator calls name their journal, the handover is paged (#439 (operator calls name their journal)) ([#551](https://github.com/PierreZ/paros/pull/551))
- Serve reads from the reported fold, then release the next page (#550 (replica serves an unreported fold page)) ([#552](https://github.com/PierreZ/paros/pull/552))
- A drain picks a renamed machine; drained seeds rename admitted ones (#504 (red drain seeds)) ([#544](https://github.com/PierreZ/paros/pull/544))
- Fix main's red sweep: init oracles, gate floor, rare gates (#536 (cell init oracle reads a stale board), #446 (a gate that never fires cannot fail the sweep)) ([#553](https://github.com/PierreZ/paros/pull/553))
- Renew the term while it installs and serves (#542 (coordinator starves its lease while it installs)) ([#547](https://github.com/PierreZ/paros/pull/547))
- Renamed founder: judge its current name, campaign only once registered (#506 (renamed founder not registered)) ([#546](https://github.com/PierreZ/paros/pull/546))
- Heal a stranded admitted machine (#505 (stranded admitted machine)) ([#543](https://github.com/PierreZ/paros/pull/543))
- Biscuit tokens: root keys in the universe entry, the bootstrap pin (#245 (Biscuit tokens)) ([#535](https://github.com/PierreZ/paros/pull/535))
- Rename the redundancy modes to durability (#523 (rename the redundancy modes)) ([#538](https://github.com/PierreZ/paros/pull/538))
- A handoff storm and faster handover abandons reach the chaos window (#484 (silent driver fault sites)) ([#497](https://github.com/PierreZ/paros/pull/497))
- Judge every compaction floor against the decided frontier and Truncate (#448 (compaction floor checks)) ([#492](https://github.com/PierreZ/paros/pull/492))
- Judge every journal world's model and the quiet ones' convergence (#447 (model checks for every journal world)) ([#481](https://github.com/PierreZ/paros/pull/481))
- A declared gate that never runs fails the sweep (#446 (a gate that never fires cannot fail the sweep)) ([#482](https://github.com/PierreZ/paros/pull/482))
- The settle tail waits for a joiner's spare (#466 (late joiner spare)) ([#483](https://github.com/PierreZ/paros/pull/483))
- Three founding members, one held down (#213 (three founding members)) ([#480](https://github.com/PierreZ/paros/pull/480))
- A planned loss never marks a quiet seat (#471 (quiet seat loss)) ([#472](https://github.com/PierreZ/paros/pull/472))
- Matchmaker calls and the data plane join the machine group (#404 (static vs dynamic endpoints)) ([#473](https://github.com/PierreZ/paros/pull/473))
- Tenant coordinator: a short catch-up answers unavailable, never panics (#445 (ignored load result)) ([#465](https://github.com/PierreZ/paros/pull/465))
- Hold a write resolved not written to it ([#444](https://github.com/PierreZ/paros/pull/444)) ([#470](https://github.com/PierreZ/paros/pull/470))
- A planned injection unsettles its custody (#441 (metainfo twin)) ([#468](https://github.com/PierreZ/paros/pull/468))
- Resolve names the universe and the cell; machine logs name the machine (#239 (names at the edge)) ([#464](https://github.com/PierreZ/paros/pull/464))
- One set per tenant, a registry per journal (#190 (matchmakers everywhere)) ([#434](https://github.com/PierreZ/paros/pull/434))
- Dynamic groups for the machine and the coordinator (#404 (static vs dynamic endpoints)) ([#463](https://github.com/PierreZ/paros/pull/463))
- Search every logged journal's history, and fail an undecided search (#442 (search every journal's history), #443 (undecided search passed)) ([#462](https://github.com/PierreZ/paros/pull/462))
- Hold a node's pool admission so a configuration names an unpooled joiner (#387 (unpooled joiner in a configuration)) ([#436](https://github.com/PierreZ/paros/pull/436))
- The lifecycle injector ends the static-stability hold at its deadline (#426 (static-stability hold)) ([#435](https://github.com/PierreZ/paros/pull/435))
- Split-floor outage reaches a leader's repair probe at a trim point (#409 (repair probe blocked below a trim point)) ([#432](https://github.com/PierreZ/paros/pull/432))
- Authorize in names, resolve, forward; run it in the simulation (#192 (the frontend)) ([#412](https://github.com/PierreZ/paros/pull/412))
- Init passes a machine admitted at a founder's address (#423 init refuses a healthy cell) ([#425](https://github.com/PierreZ/paros/pull/425))
- A lagging-acceptor scenario for the #340 (trim-point jump) mutation survivors ([#410](https://github.com/PierreZ/paros/pull/410))
- Split chain_workload, process and audit/mod as pure moves (#415, #416, #417) ([#422](https://github.com/PierreZ/paros/pull/422))
- Draw the 64-entry page bounds per seed (#338 64-entry bounds, mutation survivors) ([#411](https://github.com/PierreZ/paros/pull/411))
- Admin CLI views: machine, cell, tenant and roles, answered by one cell (#399 admin CLI views) ([#407](https://github.com/PierreZ/paros/pull/407))
- Coalesce beats per lane, so a slow link still confirms quorum reads (#386 slow peer link starves quorum reads) ([#406](https://github.com/PierreZ/paros/pull/406))
- A retried journal request after its tenant's removal reads unknown_tenant ([#395](https://github.com/PierreZ/paros/pull/395)) ([#405](https://github.com/PierreZ/paros/pull/405))
- The linearizability search grows with the history (#392 linearizability search past its step cap) ([#403](https://github.com/PierreZ/paros/pull/403))
- A spare's clean copy sets no loss-excuse threshold (#375 loss excuse counts a spare's copy) ([#398](https://github.com/PierreZ/paros/pull/398))
- A stalled-proxy scenario, catching the #341 mutation survivors (proxy take-back on a grid, eviction, supersession) ([#391](https://github.com/PierreZ/paros/pull/391))
- Judge a double fault's corruption park at the journal's verdict (#351 stale corruption park) ([#394](https://github.com/PierreZ/paros/pull/394))
- Settle a store before trusting its format marker (#348 false amnesia refusal) ([#384](https://github.com/PierreZ/paros/pull/384))
- Catch the #343 mutation survivors (probe quorum, repair re-query, pool, fold hole) ([#385](https://github.com/PierreZ/paros/pull/385))
- Send the journal calls the workload never sent (#339 mutation survivors) ([#383](https://github.com/PierreZ/paros/pull/383))
- Checkpoints in many slots: a run of Begin, Chunk and End records ([#353](https://github.com/PierreZ/paros/pull/353)) ([#381](https://github.com/PierreZ/paros/pull/381))
- Tenant control journal: journal create and delete as real RPCs ([#210](https://github.com/PierreZ/paros/pull/210)) ([#347](https://github.com/PierreZ/paros/pull/347))
- #349 (registry address books): peers dial the registry's addresses, and machines register a moved address ([#377](https://github.com/PierreZ/paros/pull/377))
- One pre-tail snapshot per journal (#357 reader tail snapshot) ([#373](https://github.com/PierreZ/paros/pull/373))
- Machine registry: bookings by role, incarnations and liveness ([#211](https://github.com/PierreZ/paros/pull/211)) ([#352](https://github.com/PierreZ/paros/pull/352))
- Split listen and advertised addresses ([#257](https://github.com/PierreZ/paros/pull/257)) ([#350](https://github.com/PierreZ/paros/pull/350))
- Names at the edge: paros://tenant/journal and short hex ids ([#239](https://github.com/PierreZ/paros/pull/239)) ([#346](https://github.com/PierreZ/paros/pull/346))
- Mutation hunt: triage the first run and kill its survivors ([#269](https://github.com/PierreZ/paros/pull/269)) ([#334](https://github.com/PierreZ/paros/pull/334))
- The cell election over a multi-writer journal, a lease as a liveness hint ([#240](https://github.com/PierreZ/paros/pull/240)) ([#337](https://github.com/PierreZ/paros/pull/337))
- A per-seed recovery page size, so multi-page recovery is reachable ([#330](https://github.com/PierreZ/paros/pull/330)) ([#345](https://github.com/PierreZ/paros/pull/345))
- An outage waits until no victim has a budgeted commit in flight ([#332](https://github.com/PierreZ/paros/pull/332)) ([#336](https://github.com/PierreZ/paros/pull/336))
- A cut mid-commit and a wipe spend one loss budget ([#331](https://github.com/PierreZ/paros/pull/331)) ([#333](https://github.com/PierreZ/paros/pull/333))
- Cell add-machine admits an idle machine into a cell ([#216](https://github.com/PierreZ/paros/pull/216)) ([#319](https://github.com/PierreZ/paros/pull/319))
- The P2 and P3 coverage probes ([#324](https://github.com/PierreZ/paros/pull/324)) ([#329](https://github.com/PierreZ/paros/pull/329))
- The three per-seed latches are named BUGGIFY locations; DriverHooks is gone (#318 E) ([#327](https://github.com/PierreZ/paros/pull/327))
- The operator forgets a cell its machines do not know ([#304](https://github.com/PierreZ/paros/pull/304)) ([#326](https://github.com/PierreZ/paros/pull/326))
- Every per-call DriverHooks choice is an inline BUGGIFY site (#318 A-D) ([#321](https://github.com/PierreZ/paros/pull/321))
- The re-sends and election choices are inline BUGGIFY sites ([#294](https://github.com/PierreZ/paros/pull/294)) ([#302](https://github.com/PierreZ/paros/pull/302))
- A founding member wiped during init, and the "no unlearned id" oracle ([#246](https://github.com/PierreZ/paros/pull/246)) ([#301](https://github.com/PierreZ/paros/pull/301))
- A power loss mid-commit is the journal stores' own hint!; PowerCut goes ([#294](https://github.com/PierreZ/paros/pull/294)) ([#300](https://github.com/PierreZ/paros/pull/300))
- The driver seams are hint!s; RunError::SeamCrash and restart_delay! go ([#294](https://github.com/PierreZ/paros/pull/294)) ([#297](https://github.com/PierreZ/paros/pull/297))
- Add the multi-writer journal mode ([#241](https://github.com/PierreZ/paros/pull/241)) ([#296](https://github.com/PierreZ/paros/pull/296))
- Hint! the cell init moments, no sim wrapper around run_machine ([#246](https://github.com/PierreZ/paros/pull/246)) ([#292](https://github.com/PierreZ/paros/pull/292))
- Two oracle gaps a #246 hunt found ([#291](https://github.com/PierreZ/paros/pull/291))
- Refuse a batch over the node's limits at the edge ([#241](https://github.com/PierreZ/paros/pull/241)) ([#290](https://github.com/PierreZ/paros/pull/290))
- Name the node and journal when unknown_journal fires ([#265](https://github.com/PierreZ/paros/pull/265)) ([#286](https://github.com/PierreZ/paros/pull/286))
- Cell init: a single-decree Paxos on the cell plan, no seeds ([#277](https://github.com/PierreZ/paros/pull/277)) ([#289](https://github.com/PierreZ/paros/pull/289))
- An identical unknown claim may win again after a reinstatement ([#288](https://github.com/PierreZ/paros/pull/288))
- A loss excuses liveness when any asked configuration cannot qualify ([#287](https://github.com/PierreZ/paros/pull/287))
- Leader uuid fences the journal; the term never reaches clients ([#241](https://github.com/PierreZ/paros/pull/241)) ([#281](https://github.com/PierreZ/paros/pull/281))
- The membership probe no longer wedges a cluster outside its belief ([#278](https://github.com/PierreZ/paros/pull/278)) ([#282](https://github.com/PierreZ/paros/pull/282))
- Simulated parosd machines, formed by the workload's init ([#246](https://github.com/PierreZ/paros/pull/246)) ([#279](https://github.com/PierreZ/paros/pull/279))
- Mutation-test paros-core weekly, the hunt as the test ([#269](https://github.com/PierreZ/paros/pull/269)) ([#276](https://github.com/PierreZ/paros/pull/276))
- Prove the departed straggler's rule; bare-quorum and lost-verdict scenarios ([#267](https://github.com/PierreZ/paros/pull/267)) ([#270](https://github.com/PierreZ/paros/pull/270))
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

