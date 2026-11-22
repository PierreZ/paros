[← 04-5-old-duties.md](04-5-old-duties.md) · [Index](README.md) · [05-failure-model-and-zones.md →](05-failure-model-and-zones.md)

### 4.6 The features of the tag and their home

**Each feature that a user or an operator could see at the tag `v0.2.0-alpha.1` has one row
here, with the issue of #1409 (build the layered parosd) that brings it back, or the decision
that drops it** (#1437 (the parity check), part 1, 2026-11-22). Pierre: "the whole old global
architecture was good in term of features. I just want it to be layered and to use the new
journal stuff." So the aim of #1409 is checked against this list. The list comes from the 20
recaps of [`docs/archive/v0.2.0-alpha.1/`](../archive/v0.2.0-alpha.1/README.md), the command
tree of `parosctl` at the tag (`crates/bin/parosctl/src/main.rs` and `src/cmd/`), and the duty
table of §4.5.

Part 2 of #1437 (the final check) adds to each row a test, a simulation scenario or a `DEMO.md`
command that shows the feature on the new service. A row marked **after v1** is a feature that
the journal v1 puts out of its scope (§J1.2); it is listed, not missing. A row marked
**Finding** has no issue yet: it is a question for Pierre on #1437 (file an issue, or drop it).

The marks in the "Home" column:

- an issue number: the issue of #1409 (or of #1323 (build the paros journal v1)) that brings it
  back;
- **Dropped**: a recorded decision removes it, named in the row;
- **After v1**: out of the journal v1, §J1.2;
- **Later**: kept, but not needed for parity (Pierre's answer on #1437);
- **Finding**: no home yet, asked on #1437.

#### Bootstrap, machines and the cell

| # | Feature | At the tag | Home |
|---|---|---|---|
| F1 | `parosctl init`: form a cell by a single-decree Paxos over the listed machines, with or without a universe (`--idle`); a re-run resumes | `cmd/init.rs`, `paros-machine/src/cell_init.rs` | #1426 (paros-machine), #1434 (parosctl) |
| F2 | A machine formats once with a random identity; a disk with stores and no record is refused (amnesia); the class is fixed at format | `paros-machine/src/lifecycle.rs` | #1426 |
| F3 | A wiped machine is a new machine and never rejoins as itself; an initial member wiped during `init` does not block it while fewer than a majority are wiped | `cell_init.rs` (`intact`) | #1426 |
| F4 | A member that missed the decree is taught by a formed peer | `formed.rs` (`Teacher`) | #1426 |
| F5 | `cell add-machine`: admit an idle machine | `cmd/cell.rs`, `paros-machine/src/wait.rs` | #1426, #1423 (paros-cell), #1434 |
| F6 | `cell drain-machine`: move every role off a machine | `cmd/cell.rs`, `tenant/moves.rs` | #1421 (the placement port), #1423, #1434 |
| F7 | `cell remove-machine`: remove a drained machine for good; its id is never used again | `cmd/cell.rs` | #1423, #1434 |
| F8 | `cell maintenance`: a window over a machine or a zone, in which a down machine's roles are not placed again; `--for 0s` ends it | `cmd/cell.rs`, `cell/watch.rs` (`held`) | #1423, #1434 |
| F9 | Heal: a machine down past `machine_replace_after` is gone, and its roles are placed again, with nobody watching | `cell/watch.rs`, `tenant/moves.rs` | #1421, #1423; the heal of a journal is a v1 duty of the grid controller (§J6) |
| F10 | A slow machine (gray failure) is held slow and its roles move; at most a minority is held slow | `cell/slow.rs` | #1423, #1421 |
| F11 | A machine low on disk space is marked (`MachineSpace`); a full disk makes a journal read-only, not a crash | `cell/watch.rs`, `node/control.rs` | #1423; #1414 (Storage lists the roles and reports typed faults) |
| F12 | A storage fault that may have landed (fsyncgate) stops the machine with exit code 75, and it boots from its disk | `paros-storage`, `parosd/src/main.rs` | #1427 (paros-service-moonpool), #1429 (the first parosd) |
| F13 | A machine that comes back at another address registers it; a machine whose registry fold is stuck gets the cell's book again | `register.rs`, `cell/watch.rs` | #1422 (the takeover by a new controller), #1425 (the info tables) |
| F14 | Static stability: a machine serves its roles and finds its peers from its cached registry when the coordinator is down | `cache.rs`, `node/system.rs` | #1426, #1425, #1427 (the machine lookup) |
| F15 | Balance the cell: acceptors and leaders move off loaded machines; `cell balance` shows it, `--pause` and `--resume` | `tenant/balancing.rs`, `cmd/cell/balance.rs` | #1423, #1421, #1434. The balance of leaders: question 2 of §4.5 |
| F16 | `cell configure`: the durability of the control plane, `auto` (grows with the storage machines) or `manual` | `cmd/cell.rs`, `tenant/controls.rs` | #1423 (the cell's own control journal is **Open** in #1421 and #1423) |
| F17 | Machine classes `storage` and `stateless`; a machine's zone and rack | `paros-model/src/machine.rs`, `MachineFacts` | #1426 |
| F18 | The cell coordinator is elected among the machines; no machine is special after `init` | `cell/campaign.rs` | #1420 (paros-election), #1423 |
| F19 | A clean stop (first signal) hands each leadership on, with a bound and a backstop; a second signal exits as a crash | `parosd/src/main.rs` | #1429 |
| F20 | Exit codes that tell the supervisor what to do (75 restart, 78 do not restart, 1, 2, 130) | `parosd/src/main.rs` | #1429 |
| F21 | Every `PAROS_*` variable is also a flag; an unknown `PAROS_*` variable is an error; knobs with floors | `parosd`, `paros-tunables` | #1429, #1415 (knobs) |

#### Tenants and journals

| # | Feature | At the tag | Home |
|---|---|---|---|
| F22 | `tenant create`, `tenant delete`; a re-run resumes; an interrupted creation says how to free its name | `cmd/tenant.rs`, `paros-client/src/fleet.rs` | #1430 (paros-tenant), #1432 (paros-universe), #1434 |
| F23 | A tenant survives `az` or `region`, fixed at creation | `cmd/tenant.rs` | #1430 |
| F24 | `journal create`, `journal delete` by name; the service draws the id; a deleted id is never used again | `cmd/journal.rs`, `tenant.rs` | #1430, #1419 (the tenancy wrapper and paros-names) |
| F25 | `configure` of a journal or of a tenant's default: `--survive machines=M` (`F`), `--partitions P` | `cmd/journal.rs`, `tenant/modes.rs` | #1430, #1434 |
| F26 | `--writers single\|multi`, fixed for the journal's life | `cmd/journal.rs` | #1430; the two modes are journal v1 (§J2.4) |
| F27 | A layout: `colocated` or `compartmentalized`, with counts of proxy leaders, replicas and batchers | `cmd/journal.rs`, `tenant/roles.rs` | Question 3 of §4.5 (who books the stateless roles). Counts by load: **after v1** (§J1.2) |
| F28 | The commit mode of the stores: `ordered` (two syncs) or `batched` (one sync) (#599) | `cmd/journal.rs` | **Dropped** (Pierre, 2026-11-22: "Ordered vs batch is no more because we can set leader to none") |
| F29 | Quorum systems: majority and grid (partitions); flexible quorums | `cmd/journal.rs` | Majority and the partitioned grid: journal v1 (§J1.1). Flexible quorums: **Dropped** (decision 27, §3.4). Other grids: **after v1** |
| F30 | A tenant's frontends: `configure --frontend-count N` books them on `stateless` machines | `cmd/journal.rs`, `tenant/roles.rs` | #1430, #1431 (the frontend role) |
| F31 | The matchmaker set of each tenant, moved off a machine that leaves | `tenant/sets.rs` | The set is now one for each journal (§3.4, 2026-11-19). Who moves it: question 1 of §4.5 |
| F32 | The control journals of the cell and of each tenant grow with the cell and move off a machine that leaves | `tenant/controls.rs` | #1421, #1423, #1430 |
| F33 | Background scrub of every record on every acceptor, at a low rate, with repair and a mismatch report | `tenant/scrub.rs` | The grid controller (#1328 (paros-journal-control), §J6); the scrub of objects is **after v1** |
| F34 | User tags (`key:value`) on cells, machines, tenants and journals (`cell tag`, `cell tag-machine`, `tenant tag`, `journal tag`), and selectors in `list` and in `tenant create --cell-selector` (#575) | `cmd/*.rs` | #1449 (user tags and tag selectors; Pierre, 2026-11-22: "Keep those") |

#### The data plane and the client

| # | Feature | At the tag | Home |
|---|---|---|---|
| F35 | `write`, `read`, `tail`, `truncate`, `set-leader` | `cmd/data.rs` | Journal v1 (§J2), #1331 (the pure journal client); through the frontend: #1431, #1434 |
| F36 | Fenced single writer by leader uuid; multi-writer with `SetLeader(None)` | `paros-client` | Journal v1 (§J2.2 to §J2.4), #1331 |
| F37 | An ambiguous write is a typed result, settled by read-back and the same re-send | `paros-client/src/lib.rs` (`resolve`) | #1331, #1431 |
| F38 | A pipeline of writes in flight at consecutive positions, merged | `paros-client/src/pipeline.rs` | #1331, #1431 (model first) |
| F39 | A reader cursor with gap reports; tail by long poll | `paros-client/src/reader.rs` | Journal v1 (§J2.5), #1331 |
| F40 | Leaderless reads | `paros-roles` | Journal v1 (§J2.5, #1326) |
| F41 | Write admission: a busy leader answers `busy`; a full backlog answers `Overloaded` | `node/calls.rs` | #1416 (host features), #1431 |
| F42 | `inspect`: the operator's view of a journal (term, configuration, watermark) | `cmd/data.rs` | #1434, as a read-only `journal inspect`: the segment table, the grid, the leader and the health measures of §J6.2; it changes nothing (Pierre, 2026-11-22) |
| F43 | `reconfigure` and `retire`: the operator changes a journal's members by hand | `cmd/data.rs` | **Dropped** (Pierre, 2026-11-22): the grid controller's fenced reshape is the only membership change (§J6.1), and the retire of a segment is a protocol step (§J3.1) |
| F44 | The checkpoint of a journal's state, then a truncate below it | `paros-client/src/checkpoint.rs` | #1418 (paros-rsm) |
| F45 | The election library for a user's own program | `paros-client/src/election.rs` | #1420 |

#### Many cells

| # | Feature | At the tag | Home |
|---|---|---|---|
| F46 | `universe add-cell`, `universe remove-cell` (an id never used again) | `cmd/universe.rs`, `universe.rs` | #1432 |
| F47 | Tenant to cell by the bucket table (`CRC16(name) mod 16384`), `universe buckets show` and `assign` | `cmd/universe.rs` | #1432 |
| F48 | An override: `tenant create --cell` with a reason | `cmd/tenant.rs` | #1432 |
| F49 | `universe status`: cells, buckets, tenants per cell | `cmd/universe.rs` | #1432, #1434 |
| F50 | `tenant move`: a tenant moves to another cell while it serves | `cmd/tenant.rs`, `cell/moving.rs`, `tenant/arrivals.rs` | #1433 (the move of a tenant between cells) |
| F51 | `resolve` and the resolver: a tenant's name to its cell, then to its frontends | `paros-machine/src/resolve.rs` | #1431, #1432 |

#### Security

| # | Feature | At the tag | Home |
|---|---|---|---|
| F52 | Root keys of the universe: `universe key list\|add\|remove`; `key generate\|show` offline | `cmd/universe.rs`, `cmd/key.rs` | #1432 (root keys in the directory), #1434 |
| F53 | Biscuit tokens: `token mint\|derive\|inspect` offline; admin and tenant roles; tokens name tenants and journals by name, and a tenant token is pinned to the tenant's id (§3.8) | `paros-authz-biscuit`, `cmd/token.rs` | #1431, #1434 |
| F54 | The frontend checks the token on each call | `paros-roles` (frontend), `BiscuitAuthz` | #1431 |
| F55 | The bootstrap pin: an idle machine checks an `admin` token on `init` and `Admit` | `paros-machine/src/pin.rs` | #1426, #1431 |
| F56 | Mutual TLS between machines, frontends and operators | `paros-net`, `parosd` | #1427 |
| F57 | `tls init\|ca\|issue\|show\|verify`: the offline tools of a deployment's certificates | `cmd/tls.rs` | #1434 (offline tools); the identities: #1427 |

#### Status and observation

| # | Feature | At the tag | Home |
|---|---|---|---|
| F58 | `status`: the universe, a cell or a tenant, with desired, current and available for each journal and a status word with hysteresis | `cmd/status.rs`, `paros-machine/src/views.rs` | #1434; the journal states of §J6.2; the views of #1423, #1430, #1432 |
| F59 | `machine list\|show`, `tenant list\|show`, `journal list`, `roles`: the views, by name, text or `--json` | `cmd/*.rs`, `views.rs` | #1434 |
| F60 | Names first: hex ids only for debugging; a hex prefix names one candidate | `support/labels.rs`, `support/names.rs` | #1419, #1434 |
| F61 | The three-way ending and exit codes of `parosctl` (done, refused, ambiguous, unreachable) | `support/connect.rs` | #1434 |
| F62 | `Load`: each machine's busyness, summed by `status` | `paros-machine/src/load.rs` | #1428 (the Events and paros-metrics) |
| F63 | Prometheus metrics at `/metrics` | `paros-observe/src/metrics.rs`, `parosd/src/http.rs` | #1428 |
| F64 | OTLP export of spans and metrics | `parosd/src/otel.rs` | #1428 |
| F65 | Health probes `/healthz` and `/readyz` (`503` until the machine serves its cell) | `parosd/src/http.rs` | #1427 (health is a fixed service), #1429 |
| F66 | `store inspect <data-dir>`: offline inspection of a machine's disk, read-only, with names | `paros-machine/src/inspect.rs`, `cmd/store.rs` | #1434, #1414 |

#### Tools around the service

| # | Feature | At the tag | Home |
|---|---|---|---|
| F67 | The bench (`paros-bench`): writers, tailers and readers, phases `init`, `run`, `clean`, `report`; `--backlog`; `--topology` | `crates/bin/paros-bench` | #1371 (journal bench); on the service: #1434 (`paros-cli`) |
| F68 | The live playground in the browser: the whole service, driven by operator buttons and faults | `paros-wasm` (`playground`) | #1435 (the wasm parosd engine), #1441 (the page) |
| F69 | The Engineering pages that run the engine live | `website/site/public/engineering.js` | #1444 (Engineering pages with live runs) |
| F70 | A two-cell demo with Docker Compose under mutual TLS, and `DEMO.md` | `docker-compose.yml`, `DEMO.md` | `DEMO.md` for one cell: #1429. The image and the two-cell Compose demo: #1450 (Pierre, 2026-11-22: "Keep those") |
| F71 | Release archives of the binaries for `cargo binstall` | `release-plz.toml` | **Later**, after the parity check (Pierre, 2026-11-22: "We can do this later") |
