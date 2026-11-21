# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-21

### 🚀 Features

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

### 🧪 Testing

- **journal-sim**: Check long fold walks live while they run ([#1310](https://github.com/PierreZ/paros/pull/1310)) ([#1369](https://github.com/PierreZ/paros/pull/1369))
- **consensus-sim**: Walk every Paxos flavor with moonpool-walk (#1197 (consensus-sim walks)) ([#1220](https://github.com/PierreZ/paros/pull/1220))

### 📦 Other

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

