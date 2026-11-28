# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-28

### 🚀 Features

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
- **consensus**: Feedback() on the four deployments (#1564 step 2) ([#1569](https://github.com/PierreZ/paros/pull/1569))
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
- A README, a quick start, four driver examples and doc tests ([#1237](https://github.com/PierreZ/paros/pull/1237))
- Walk moves for the unreached probes ([#1225](https://github.com/PierreZ/paros/pull/1225)), and a rejoin fix ([#1234](https://github.com/PierreZ/paros/pull/1234))
- Coverage-driven simulation and a gate on every core probe ([#1199](https://github.com/PierreZ/paros/pull/1199)) ([#1226](https://github.com/PierreZ/paros/pull/1226))

### 🐛 Bug Fixes

- **controller**: Rule 6 checks again a node checked on an earlier floor ([#1588](https://github.com/PierreZ/paros/pull/1588))
- **consensus**: A compaction below its target keeps the records its retries read ([#1565](https://github.com/PierreZ/paros/pull/1565))
- **consensus**: A learner above the leader by its own unprepared campaign reads ([#890](https://github.com/PierreZ/paros/pull/890)) ([#1398](https://github.com/PierreZ/paros/pull/1398))

### 📚 Documentation

- **consensus**: One example, grow, that grows a journal through every flavor ([#1554](https://github.com/PierreZ/paros/pull/1554))
- **consensus**: A segments example that rolls the log and reads the chain ([#1552](https://github.com/PierreZ/paros/pull/1552))
- **consensus**: Every example drives the public deployments ([#1550](https://github.com/PierreZ/paros/pull/1550))

### 🚜 Refactor

- **consensus**: One matchmaker serves one journal ([#1572](https://github.com/PierreZ/paros/pull/1572))

### 🧪 Testing

- **journal-sim**: Check long fold walks live while they run ([#1310](https://github.com/PierreZ/paros/pull/1310)) ([#1369](https://github.com/PierreZ/paros/pull/1369))
- **consensus-sim**: Walk every Paxos flavor with moonpool-walk (#1197 (consensus-sim walks)) ([#1220](https://github.com/PierreZ/paros/pull/1220))

### ⚙️ Miscellaneous Tasks

- Move mutation testing from pull requests to a nightly run ([#1499](https://github.com/PierreZ/paros/pull/1499))
- Run only the jobs a change can break, in fewer jobs (40-job limit) ([#1462](https://github.com/PierreZ/paros/pull/1462))

### 📦 Other

- A held tally that never forgets a report is safe (segments step 6) ([#1570](https://github.com/PierreZ/paros/pull/1570))
- **compartments**: The replicas of a segment (step 5, model) ([#1551](https://github.com/PierreZ/paros/pull/1551))
- Moonpool at 989a5b78, the live check's hand-off survives TLC's progress line ([#1507](https://github.com/PierreZ/paros/pull/1507))
- **paros**: Bump moonpool to ab3d8a8 (journal recovery fix, moonpool #568) ([#1494](https://github.com/PierreZ/paros/pull/1494))
- **deps**: Move every moonpool line to c698161 ([#1490](https://github.com/PierreZ/paros/pull/1490))
- Release-plz back on, paros-consensus, paros-journal and paros ready to publish ([#1348](https://github.com/PierreZ/paros/pull/1348))
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
- Clean up paros down to its consensus core ([#1194](https://github.com/PierreZ/paros/pull/1194))

