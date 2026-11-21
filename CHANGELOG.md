# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-21

### 🚀 Features

- A README, a quick start, four driver examples and doc tests ([#1237](https://github.com/PierreZ/paros/pull/1237))
- The frontend is a role of parosd (#723 (frontend as a parosd role))
- **sandbox**: Run a paros universe in the browser (#518 (Paxos visualization sandbox)) ([#519](https://github.com/PierreZ/paros/pull/519))
- TLS and mutual TLS on every session, parosctl tls, TLS on half the sim seeds ([#509](https://github.com/PierreZ/paros/pull/509))
- **core**: The replica tier, core half (#144 part A) ([#171](https://github.com/PierreZ/paros/pull/171))
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
- **paros-journal**: The journal roles over the core, first rule group ([#1360](https://github.com/PierreZ/paros/pull/1360))

### 📚 Documentation

- Bring the human docs and the architecture up to date (#859, #687, #837, #860) ([#866](https://github.com/PierreZ/paros/pull/866))
- A map for every crate, and the docs name the new paths (#722 (three products))
- **core**: Three runnable teaching examples for the composable roles ([#135](https://github.com/PierreZ/paros/pull/135))
- Add project logo and README quick reference

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- One crate per layer, grouped under crates/ (#722 (three products))
- Delete the inert wire plumbing, settle the open judgment calls, advance the moonpool pin ([#137](https://github.com/PierreZ/paros/pull/137))

### 🧪 Testing

- **consensus-sim**: Walk every Paxos flavor with moonpool-walk (#1197 (consensus-sim walks)) ([#1220](https://github.com/PierreZ/paros/pull/1220))
- Sim, parosctl: a second cell mid-run, the operator lane, cross-cell commands (#490 (second cell mid-run), #489 (operator workload)) ([#786](https://github.com/PierreZ/paros/pull/786))
- Batchers and tenant role counts (#145 (batchers), #604 (role counts)) ([#698](https://github.com/PierreZ/paros/pull/698))
- One workload, one audit, everything knobbed — the paros-sim simplification ([#128](https://github.com/PierreZ/paros/pull/128))
- **journal-sim**: Check long fold walks live while they run ([#1310](https://github.com/PierreZ/paros/pull/1310)) ([#1369](https://github.com/PierreZ/paros/pull/1369))

### ⚙️ Miscellaneous Tasks

- Organize rust crates under crates

### 📦 Other

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
- The first examples, fold and in_memory (#1370 (journal examples)) ([#1383](https://github.com/PierreZ/paros/pull/1383))
- The matchmaker register as an optional role ([#1325](https://github.com/PierreZ/paros/pull/1325)) ([#1353](https://github.com/PierreZ/paros/pull/1353))

