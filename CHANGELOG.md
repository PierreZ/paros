# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-28

### 🚀 Features

- **consensus**: Feedback() on the four deployments (#1564 step 2) ([#1569](https://github.com/PierreZ/paros/pull/1569))
- **authz**: Bring back paros-authz-biscuit without its frontend adapter ([#1396](https://github.com/PierreZ/paros/pull/1396))
- A README, a quick start, four driver examples and doc tests ([#1237](https://github.com/PierreZ/paros/pull/1237))
- The frontend is a role of parosd (#723 (frontend as a parosd role))
- **sandbox**: Run a paros universe in the browser (#518 (Paxos visualization sandbox)) ([#519](https://github.com/PierreZ/paros/pull/519))
- TLS and mutual TLS on every session, parosctl tls, TLS on half the sim seeds ([#509](https://github.com/PierreZ/paros/pull/509))
- **core**: The replica tier, core half (#144 part A) ([#171](https://github.com/PierreZ/paros/pull/171))
- **segments**: SetReplicas of the open segment (step 5d, D10) ([#1590](https://github.com/PierreZ/paros/pull/1590))
- A lagging replica pulls its segment from a replica peer of its set ([#1584](https://github.com/PierreZ/paros/pull/1584))
- A lagging replica of a released segment stops as trimmed ([#1583](https://github.com/PierreZ/paros/pull/1583))
- Kill the nightly mutant survivors of the segment code, and the two mutants that ran in every second pass ([#1582](https://github.com/PierreZ/paros/pull/1582))
- Drop held records at the acceptors, and a released segment counts as held (segments step 6) ([#1581](https://github.com/PierreZ/paros/pull/1581))
- A walk where journal controllers heal a lost member ([#1578](https://github.com/PierreZ/paros/pull/1578))
- **consensus**: A reconfiguration needs a proven leadership, and a learn closes the round of its slot (#1564 step 5) ([#1575](https://github.com/PierreZ/paros/pull/1575))
- **consensus**: The decided Hold of a held segment (segments step 6) ([#1576](https://github.com/PierreZ/paros/pull/1576))
- **consensus**: The journal controller replaces a silent acceptor (#1564 step 4) ([#1574](https://github.com/PierreZ/paros/pull/1574))
- **consensus**: The journal controller's view and rules 1 to 7 (#1564 step 3) ([#1573](https://github.com/PierreZ/paros/pull/1573))
- **consensus**: The held tally of a sealed segment (segments step 6) ([#1568](https://github.com/PierreZ/paros/pull/1568))
- **consensus**: Count the quorum windows each member missed (#1564 step 1) ([#1567](https://github.com/PierreZ/paros/pull/1567))
- **consensus**: Route each Commit to the replicas of its segment (step 5, routing) ([#1555](https://github.com/PierreZ/paros/pull/1555))
- **consensus**: A replica keeps one segment (step 5, first part) ([#1553](https://github.com/PierreZ/paros/pull/1553))
- **consensus**: The chain of segments at the matchmakers (step 4) ([#1544](https://github.com/PierreZ/paros/pull/1544))
- **consensus**: The Roll cut of segments (step 3b of the segments in the core) ([#1543](https://github.com/PierreZ/paros/pull/1543))
- Consensus, journal: the journal's Phase 1 pages its promises with the core's PromiseTally ([#1512](https://github.com/PierreZ/paros/pull/1512))
- Consensus, journal: the journal's Phase 1 uses the core's P2c merge and CTRL tally ([#1509](https://github.com/PierreZ/paros/pull/1509))
- Count the retained bytes of a node and a replica node ([#1153](https://github.com/PierreZ/paros/pull/1153)) ([#1492](https://github.com/PierreZ/paros/pull/1492))
- One Belief role, one copy of the retained log, and the design of its memory bound (#1101, #1153) ([#1488](https://github.com/PierreZ/paros/pull/1488))
- Core checks (flight token, catch-up bounds, invariants, layer check, assertion density) ([#1482](https://github.com/PierreZ/paros/pull/1482))
- Close the node handler gaps of #902 ([#1401](https://github.com/PierreZ/paros/pull/1401))
- Tighten the driver contract ([#906](https://github.com/PierreZ/paros/pull/906)) ([#1405](https://github.com/PierreZ/paros/pull/1405))
- Seal each row of a full grid in Phase 1 ([#1209](https://github.com/PierreZ/paros/pull/1209)) ([#1400](https://github.com/PierreZ/paros/pull/1400))
- One command per ballot and slot, and the duplicated Relinquish after a restart ([#1292](https://github.com/PierreZ/paros/pull/1292)) ([#1399](https://github.com/PierreZ/paros/pull/1399))
- Kill the matchmaking, GC and collector mutation survivors ([#1282](https://github.com/PierreZ/paros/pull/1282)) ([#1354](https://github.com/PierreZ/paros/pull/1354))
- Kill the proposer, election and Phase-2 mutation survivors ([#1282](https://github.com/PierreZ/paros/pull/1282)) ([#1355](https://github.com/PierreZ/paros/pull/1355))
- Kill the replica, proxy, read, batcher and contract mutation survivors ([#1282](https://github.com/PierreZ/paros/pull/1282)) ([#1357](https://github.com/PierreZ/paros/pull/1357))
- Kill the matchmaker and membership mutation survivors (#1282 group B) ([#1356](https://github.com/PierreZ/paros/pull/1356))
- Kill the group E mutation survivors ([#1282](https://github.com/PierreZ/paros/pull/1282)) ([#1358](https://github.com/PierreZ/paros/pull/1358))
- Keep the handover's decree round on disk ([#1229](https://github.com/PierreZ/paros/pull/1229)) ([#1265](https://github.com/PierreZ/paros/pull/1265))
- Remove the cell (#1264 (remove the cell from paros-consensus)) ([#1268](https://github.com/PierreZ/paros/pull/1268))
- Gate a leaderless read on the reader's promise ([#1228](https://github.com/PierreZ/paros/pull/1228)) ([#1238](https://github.com/PierreZ/paros/pull/1238))
- Reach every core probe in CI, with no GAPS list ([#1239](https://github.com/PierreZ/paros/pull/1239))
- Walk moves for the unreached probes ([#1225](https://github.com/PierreZ/paros/pull/1225)), and a rejoin fix ([#1234](https://github.com/PierreZ/paros/pull/1234))
- Coverage-driven simulation and a gate on every core probe ([#1199](https://github.com/PierreZ/paros/pull/1199)) ([#1226](https://github.com/PierreZ/paros/pull/1226))

### 🐛 Bug Fixes

- **journal**: Give each multi-writer mode a fresh tag ([#1498](https://github.com/PierreZ/paros/pull/1498))
- **controller**: Rule 6 checks again a node checked on an earlier floor ([#1588](https://github.com/PierreZ/paros/pull/1588))
- **consensus**: A compaction below its target keeps the records its retries read ([#1565](https://github.com/PierreZ/paros/pull/1565))
- **consensus**: A learner above the leader by its own unprepared campaign reads ([#890](https://github.com/PierreZ/paros/pull/890)) ([#1398](https://github.com/PierreZ/paros/pull/1398))

### 📚 Documentation

- **consensus**: One example, grow, that grows a journal through every flavor ([#1554](https://github.com/PierreZ/paros/pull/1554))
- **consensus**: A segments example that rolls the log and reads the chain ([#1552](https://github.com/PierreZ/paros/pull/1552))
- **consensus**: Every example drives the public deployments ([#1550](https://github.com/PierreZ/paros/pull/1550))
- Bring the human docs and the architecture up to date (#859, #687, #837, #860) ([#866](https://github.com/PierreZ/paros/pull/866))
- A map for every crate, and the docs name the new paths (#722 (three products))
- **core**: Three runnable teaching examples for the composable roles ([#135](https://github.com/PierreZ/paros/pull/135))
- Add project logo and README quick reference

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- One crate per layer, grouped under crates/ (#722 (three products))
- Delete the inert wire plumbing, settle the open judgment calls, advance the moonpool pin ([#137](https://github.com/PierreZ/paros/pull/137))
- **consensus**: One matchmaker serves one journal ([#1572](https://github.com/PierreZ/paros/pull/1572))

### 🧪 Testing

- **consensus-sim**: Walk every Paxos flavor with moonpool-walk (#1197 (consensus-sim walks)) ([#1220](https://github.com/PierreZ/paros/pull/1220))
- Sim, parosctl: a second cell mid-run, the operator lane, cross-cell commands (#490 (second cell mid-run), #489 (operator workload)) ([#786](https://github.com/PierreZ/paros/pull/786))
- Batchers and tenant role counts (#145 (batchers), #604 (role counts)) ([#698](https://github.com/PierreZ/paros/pull/698))
- One workload, one audit, everything knobbed — the paros-sim simplification ([#128](https://github.com/PierreZ/paros/pull/128))
- **journal-sim**: Check long fold walks live while they run ([#1310](https://github.com/PierreZ/paros/pull/1310)) ([#1369](https://github.com/PierreZ/paros/pull/1369))

### ⚙️ Miscellaneous Tasks

- Remove paros-host, its TLA+ models and its tooling ([#1548](https://github.com/PierreZ/paros/pull/1548))
- Run only the jobs a change can break, in fewer jobs (40-job limit) ([#1462](https://github.com/PierreZ/paros/pull/1462))
- Organize rust crates under crates
- Move mutation testing from pull requests to a nightly run ([#1499](https://github.com/PierreZ/paros/pull/1499))

### 📦 Other

- Remove the journal of design A (step 0 of the segments plan) ([#1538](https://github.com/PierreZ/paros/pull/1538))
- Register operations interleave in full_journal ([#1519](https://github.com/PierreZ/paros/pull/1519))
- The journal, live: every role of the journal v1 on six hosts in the page ([#1513](https://github.com/PierreZ/paros/pull/1513))
- Full_journal example, every journal role on six tokio hosts ([#1510](https://github.com/PierreZ/paros/pull/1510))
- A leader binds its ballot, and Phase 1 reads every unretired grid ([#1505](https://github.com/PierreZ/paros/pull/1505))
- Fix the findings of the journal v1 Fable review (items 1 to 4, judges A, B, C, D, F, G) ([#1504](https://github.com/PierreZ/paros/pull/1504))
- Split the generic host of actors out of paros-journal-io ([#1410](https://github.com/PierreZ/paros/pull/1410)), and the model of recorded inputs ([#1413](https://github.com/PierreZ/paros/pull/1413)) ([#1500](https://github.com/PierreZ/paros/pull/1500))
- Journal roles: proxy leader, leaderless reads, trim, and the leader time (#1326, #1299) ([#1485](https://github.com/PierreZ/paros/pull/1485))
- Require examples in composing crates; add examples at each layer ([#1390](https://github.com/PierreZ/paros/pull/1390))
- The journal's IO traits, pure Host and generic driver, #1329 (paros-journal-io) ([#1388](https://github.com/PierreZ/paros/pull/1388))
- Release-plz back on, paros-consensus, paros-journal and paros ready to publish ([#1348](https://github.com/PierreZ/paros/pull/1348))
- The pure journal core, with journal-sim ([#1324](https://github.com/PierreZ/paros/pull/1324)) ([#1347](https://github.com/PierreZ/paros/pull/1347))
- Clean up paros down to its consensus core ([#1194](https://github.com/PierreZ/paros/pull/1194))
- Rework 0.9: delete the tenant move between cells ([#1086](https://github.com/PierreZ/paros/pull/1086)) ([#1169](https://github.com/PierreZ/paros/pull/1169))
- Rework 0.12 + 0.11: delete flexible quorums ([#1156](https://github.com/PierreZ/paros/pull/1156))
- Placement counts a store's disk (#985 (placement counts store disk)) ([#990](https://github.com/PierreZ/paros/pull/990))
- A moved tenant: a moved verdict, and a client that follows it (#785 (moved record)) ([#876](https://github.com/PierreZ/paros/pull/876))
- Move a tenant between cells (#782-#785) ([#862](https://github.com/PierreZ/paros/pull/862))
- Roles, presets, moving rate, init/run/clean/report (#724, #725) ([#868](https://github.com/PierreZ/paros/pull/868))
- Founders are a boot hint; auto F grows; start with one machine (#685 steps 5 to 7) ([#859](https://github.com/PierreZ/paros/pull/859))
- Add --layout colocated|compartmentalized to journal create ([#840](https://github.com/PierreZ/paros/pull/840))
- Transparent logo with a dark-mode variant ([#841](https://github.com/PierreZ/paros/pull/841))
- One Astro site in one style, a landing page that sells parosd ([#810](https://github.com/PierreZ/paros/pull/810)) ([#814](https://github.com/PierreZ/paros/pull/814))
- Place tenants by override, then by bucket (#713 (bucket placement), #672 (cell tag selector)) ([#787](https://github.com/PierreZ/paros/pull/787))
- A universe-level role that redirects to the tenant's cell (#233 (resolver)) ([#776](https://github.com/PierreZ/paros/pull/776))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))
- Disk full goes read-only, admission control answers busy (#360 (disk full)) ([#683](https://github.com/PierreZ/paros/pull/683))
- Background scrubbing of stored records (#362 (background scrubbing)) ([#631](https://github.com/PierreZ/paros/pull/631))
- Add and remove cells: idle cells, universe add-cell and remove-cell (#232 (multiple cells), part 1) ([#593](https://github.com/PierreZ/paros/pull/593))
- Tenant modes: configure a journal's durability (#214 (tenant modes)) ([#574](https://github.com/PierreZ/paros/pull/574))
- Metrics, probes, usage ([#366](https://github.com/PierreZ/paros/pull/366)) and the bench ([#194](https://github.com/PierreZ/paros/pull/194)) ([#567](https://github.com/PierreZ/paros/pull/567))
- Acceptor partitions and moving a stuck round (#532 (acceptor partitions), #533 (move a stuck round)) ([#562](https://github.com/PierreZ/paros/pull/562))
- Tenant control journal: journal create and delete as real RPCs ([#210](https://github.com/PierreZ/paros/pull/210)) ([#347](https://github.com/PierreZ/paros/pull/347))
- The Zola + Goyo skeleton at web/site/, the book ported, Pages on Zola ([#306](https://github.com/PierreZ/paros/pull/306)) ([#320](https://github.com/PierreZ/paros/pull/320))
- Leader uuid fences the journal; the term never reaches clients ([#241](https://github.com/PierreZ/paros/pull/241)) ([#281](https://github.com/PierreZ/paros/pull/281))
- Meta tenant, fleet operations and no fixed ids ([#229](https://github.com/PierreZ/paros/pull/229)) ([#238](https://github.com/PierreZ/paros/pull/238))
- Fenced Truncate ([#228](https://github.com/PierreZ/paros/pull/228)), the (TenantId, JournalId) frame ([#235](https://github.com/PierreZ/paros/pull/235)), uniform parosd + Compose ([#196](https://github.com/PierreZ/paros/pull/196)) ([#236](https://github.com/PierreZ/paros/pull/236))
- Journal API, no application, many journals per process, sim on JournalStorage (#185, #186, #188, #187) ([#199](https://github.com/PierreZ/paros/pull/199))
- Rounds extracted (#142 rung 0), quorum reads core half ([#143](https://github.com/PierreZ/paros/pull/143)), acceptor grid sim half ([#141](https://github.com/PierreZ/paros/pull/141)) ([#150](https://github.com/PierreZ/paros/pull/150))
- Acceptor grid (core half, #141) and flexible quorums drawn per seed (sim half, #140) ([#149](https://github.com/PierreZ/paros/pull/149))
- Flexible quorums (core half, #140), the chunk-restore seam visited ([#146](https://github.com/PierreZ/paros/pull/146)), the wiped-identity format marker ([#147](https://github.com/PierreZ/paros/pull/147)) ([#148](https://github.com/PierreZ/paros/pull/148))
- Advance moonpool to the single-stream canary and put the campaign under it ([#138](https://github.com/PierreZ/paros/pull/138))
- Instrument every important method; core spans behind a default-on `tracing` feature ([#130](https://github.com/PierreZ/paros/pull/130))
- A held tally that never forgets a report is safe (segments step 6) ([#1570](https://github.com/PierreZ/paros/pull/1570))
- **compartments**: The replicas of a segment (step 5, model) ([#1551](https://github.com/PierreZ/paros/pull/1551))
- Moonpool at 989a5b78, the live check's hand-off survives TLC's progress line ([#1507](https://github.com/PierreZ/paros/pull/1507))
- **paros**: Bump moonpool to ab3d8a8 (journal recovery fix, moonpool #568) ([#1494](https://github.com/PierreZ/paros/pull/1494))
- **deps**: Move every moonpool line to c698161 ([#1490](https://github.com/PierreZ/paros/pull/1490))
- Held answers of a deposed leader, #1232 (held writes) ([#1298](https://github.com/PierreZ/paros/pull/1298))
- Rewire mutation testing on paros-consensus and consensus-sim ([#1250](https://github.com/PierreZ/paros/pull/1250))
- Check the rules that no check tested (#1230, part 1) ([#1252](https://github.com/PierreZ/paros/pull/1252))
- Truncation model, part 3: a reconfiguration onto empty stores ([#1218](https://github.com/PierreZ/paros/pull/1218)) ([#1222](https://github.com/PierreZ/paros/pull/1222))
- Model truncation part 2 in TLA+ (#1216 (truncation model, part 2)) ([#1219](https://github.com/PierreZ/paros/pull/1219))
- Model truncation in TLA+ (#1017 (truncation model)) ([#1214](https://github.com/PierreZ/paros/pull/1214))
- Model the fence of a leaderless read and a rebooted node (#1099 (rework 1.10)) ([#1212](https://github.com/PierreZ/paros/pull/1212))
- Model the proxy leader in TLA+ (#1188 (rework 2.9)) ([#1211](https://github.com/PierreZ/paros/pull/1211))
- Model plain Multi-Paxos (#1185 (rework 1.14)) ([#1204](https://github.com/PierreZ/paros/pull/1204))
- Bump flake and Rust 1.100.0; pass clippy pedantic with no allows ([#1202](https://github.com/PierreZ/paros/pull/1202))
- Model single-decree Paxos (#1186 (rework 1.15)) ([#1201](https://github.com/PierreZ/paros/pull/1201))

