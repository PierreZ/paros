[← 04-roles.md](04-roles.md) · [Index](README.md) · [04-6-parity.md →](04-6-parity.md)

### 4.5 The duty table of the old coordinator and machine

**Each module of the old coordinator, of the old machine and of the node part of the old roles
has one row here, with its duty, the state that it reads, and its new home under the cut of
§4.4** (#1412 (the duty table), step A1 of #1409 (build the layered parosd), 2026-11-22). The
issues of #1409 cite the rows by their number (C3 for the third row of the coordinator table).
The old code is at the tag `v0.2.0-alpha.1`; its recaps are
[`docs/archive/v0.2.0-alpha.1/`](../archive/v0.2.0-alpha.1/README.md). The crate names are the
placeholders of §4.4 and of the issues; they stay **Open** until #1411 (CI checks that hold the
layering) fixes the layer table.

The table comes from the code, not from the module docs alone: each entry point was followed
(`serve_term`, the campaign loop of `cell/campaign.rs`, `Watch::round`, `answer_until`,
`run_machine`, `CellHost::serve`, `ControlFollower`, `SystemCtx::apply`).

#### How one old term ran

The elected cell coordinator was also the tenant coordinator of every hosted tenant and the
cell's half of the universe. Its term did three things in one task:

1. **Install** (`cell.rs`, `open_term`): `SetLeader(uuid, current)` on the cell control journal,
   a fold to the tail, the admissions in flight finished, the universe directory followed, and
   the term's group of endpoints published.
2. **A watch round at each renewal period** (`cell/watch.rs`, `Watch::round`): follow the
   universe directory (`universe::follow`), follow the tenant moves (`cell::moving::follow`),
   release the slots of a dropped or moved tenant, `Identify` each machine, write `MachineUp`,
   `MachineDown`, `MachineAdmitted` or a new registration, hand a stuck machine the cell's book
   again, then ask each machine that answered for its `Load` and write `MachineSlow` and
   `MachineSpace` on a change.
3. **One chain of steps after each round** (`cell/campaign.rs`, lines 757 to 920 at the tag):
   balance, then one move, one mode change, one set step, one footprint step, one role-count
   step, one scrub page, one control-journal step, one arrival step. The footprint, mode, move,
   role and scrub steps take the next key in turn (`turn.rs`). A step that returns `None` (a write did not land) ends the chain and the
   watch. Between two rounds the term answers its inbox (`cell/answers.rs`, `answer_until`):
   `Register`, `JournalRequest`, `CapacityRequest`, `JoinUniverse`.

So the chain was one step for each watch round over every journal of the cell: a rate limit for
the whole cell. Almost every step reads the registry of the whole cell (`moves::stays`,
`moves::bookable`, the busyness of the round, the `gone` and `held` sets of the watch). Only the
sets step (`tenant/sets.rs`) and the scrub keep state for one tenant. This is why §4.4 gives the
membership loop of one journal to its grid controller and the cell-wide questions to the
placement port (#1421 (the placement port)), and why "gone" becomes a durable record of the cell:
at the tag it was the memory of the watch (`Watch::gone`), resumed across the terms of one
candidacy only.

#### The coordinator (`crates/lib/paros-coordinator/src/`, 13.8k lines)

| # | Module | Duty | State it reads | New home |
|---|---|---|---|---|
| C1 | `lib.rs` | The crate root: three coordinators in one crate, above the client and below the machine | — | Replaced by one crate for each control level: `paros-cell` (#1423), `paros-tenant` (#1430), `paros-universe` (#1432) |
| C2 | `cell.rs` | Serve a won term: install the uuid, fold the cell control journal, finish the admissions, publish the term's group, stop at the first refused write; `cell_client` and `follow_book` dial the cell from its book | The cell control journal, the cell plan, the book of addresses | The pure cell coordinator of `paros-cell` (#1423) on `paros-rsm` (#1418): the fence and the fold. The published group becomes the `CellInfo` push (#1425) |
| C3 | `cell/campaign.rs` | The candidacy: the election loop, the candidates from the election journal's slots, the jitter, the hand-on to a successor, the renewals while a duty runs, the chain of steps, the BUGGIFY draws of a term | The election journal, the registry (`displaced`, `candidates`) | `paros-election` (#1420): the term, the campaign, the renewal, the hand-on. The candidates come from the bookings of `paros-cell`. The chain of steps is replaced by the placement port's grants (C10) |
| C4 | `cell/answers.rs` | The request inbox of a term: `Register`, `JournalRequest`, `CapacityRequest`, `JoinUniverse`, with `not_coordinator` from an ended term | The term's desk and session | `paros-rsm` (#1418): one answer for each request id, from the recorded outcome. Each request kind is a typed method of its role's interface in the adapter (#1417). `Register` becomes the registration of #1422 and #1425 |
| C5 | `cell/capacity.rs` | The grant of a booking: a request id, a booking id drawn for each slot, one `GrantCapacity`; a refusal at apply recorded under the same id | The registry (machines, capacity, class, bookings) | `paros-cell` (#1423): the answer to `place` of the placement port (#1421). Correction 7 of the plan applies: the id is in the fold before any slot is booked |
| C6 | `cell/footprints.rs` | Keep each tenant's footprint whole: place a tenant's coordinator on a `stateless` machine, place a dead one again, move a slot off a draining machine; book the cell and universe tenants' footprints once they fit | The registry (grants, bookings, machines) | `paros-cell` (#1423): footprints are grants of the port. **Open** in #1423 and #1430: whether a tenant coordinator runs hosted by the cell term on a small cell |
| C7 | `cell/moving.rs` | The cell's side of a tenant move between cells: `ReleaseTenant`, `MovePeers`, `AdoptTenant`, `MovedTenant` | The universe directory, the other cell's registry | #1433 (the move of a tenant between cells), with the records in `paros-cell` and the work order in `paros-universe` |
| C8 | `cell/slow.rs` | The pure slow judge: round trip against the median, busy ratios, hysteresis, at most a minority held slow | The round's `Identify` times and `Load` windows | `paros-cell` (#1423), a pure judge. Its result is a "must leave" answer of the placement port (#1421) |
| C9 | `cell/watch.rs` | The failure detector: `Identify` each machine; `MachineUp`, `MachineDown`, `MachineAdmitted`, a reboot by incarnation; gone after `machine_replace_after` outside a maintenance window; a stuck machine handed the book again; `MachineSpace`; the `Probe` of a moved machine's address; the release of a dropped or moved tenant's slots | The registry, the round's answers, the silence times of the candidacy | `paros-cell` (#1423): the detector is a pure judge over facts; the probes are `Ask`s of the host (#1416). "Gone" is a durable record of the cell (§4.4). A stuck machine reads `CellInfo` (#1425) |
| C10 | `turn.rs` | The rule of turns: each step takes the next key, wraps, so one stuck key starves no other | The keys of each step | `paros-cell` (#1423): the order and the limit of the placement port's grants (#1421). Each coordinator keeps the rule for its own keys |
| C11 | `universe.rs` | The cell's side of the universe: `JoinFleet`, a tenant's birth footprint, `HostTenant`, `DropTenant`; follow the directory; join another universe (`JoinUniverse`) | The universe directory, the registry | `paros-universe` (#1432) for the directory and its fold; `paros-cell` writes the cell's step after it reads the directory, and never calls it (§4.4) |
| C12 | `tenant.rs` | The tenant coordinator's desk: claim each tenant control journal, describe a tenant, answer create, delete and configure by request id, draw journal and booking ids, place a create, release the bookings of an unanswered create, record `Placed` and `Scrubbed` | The tenant control journal, the registry, the busyness of the round | `paros-tenant` (#1430), the catalog. Ids for people and drawn ids: `paros-names` (#1419). The placement is a request to the port (#1421) |
| C13 | `tenant/arrivals.rs` | The target's half of a move between cells: move each journal onto this cell's machines, then the tenant's matchmaker set, then `SettleTenant` | The registry, the tenant control journal, each journal's leader | #1433 (the move of a tenant between cells). Each journal's move is a reshape by its grid controller |
| C14 | `tenant/balancing.rs` | Record the balancer's decision (`paros_model::balance`): `BalanceStarted`, `BalanceAbandoned`, and a leader `Handoff` | The registry, the busyness and the leaders of the round | `paros-cell` (#1423): the pure balance decision, given to the grid controller as a "must leave" answer of the port (#1421). The leader part: answer 2 below, the handoff at the end of the release of the leader |
| C15 | `tenant/controls.rs` | Keep each control journal at its `2F+1` on storage machines; grow, shrink and move it off a machine that leaves; never shrink while a kept member is down (`holds_shrink`, `must_serve`) | The registry, the journal's leader (`Inspect`) | The grid controller of each control journal (`paros-journal-control`, #1421). Its size is the `Configure` of the level that owns it. The cell places its own control journal through the same port (Pierre, 2026-11-22, §J6.3) |
| C16 | `tenant/modes.rs` | Move a journal to its desired mode (`Configure`): book, reconfigure, record `Placed`, release; wait while a move holds the journal | The registry, the tenant control journal, the journal's leader | `paros-tenant` (#1430) records the desired mode; the grid controller reshapes to it through the port (#1421) |
| C17 | `tenant/moves.rs` | The membership loop of one journal off a draining, slow or gone machine: book, warm the new member, hand the leadership off, reconfigure, wait for the GC watermark, record `Placed`, retire the old member, release. `stays`, `bookable` and `Leaving` decide who leaves | The registry (standing, slow, bookable machines), the watch (`gone`, `held`), the leader (`InspectReply`) | The grid controller of the journal (`paros-journal-control`, #1421); `stays` and `bookable` answer through the port from `paros-cell`. The warm step goes: a reshape opens a new segment on the new grid (§J3), so a new member catches up on nothing. The wait for the GC watermark and the retire become the retire of the old segment at `F + 1` replicas (§J3.1). The handoff: answer 2 below, the leader role hands on at the end of its release |
| C18 | `tenant/roles.rs` | Book role counts: a tenant's frontends on `stateless` machines; a journal's proxy leaders and batchers on `stateless` machines and replicas on `storage` machines; release a slot on a machine that leaves | The registry, the tenant control journal | The frontends: `paros-tenant` (#1430), booked through the port. The journal's stateless roles: answer 3 below, the grid controller books them through the port |
| C19 | `tenant/scrub.rs` | The scrubber: read every record on each acceptor at a low rate, compare fingerprints (never a quorum read), report mismatches, record the progress | The tenant control journal, each acceptor's `Scrub` answer | The grid controller of each journal (`paros-journal-control`): scrub with repair through the protocol is a v1 duty (§J6, §J5.3). The rate is a knob of that layer (#1415) |
| C20 | `tenant/sets.rs` | Keep each tenant's matchmaker set at `2F+1` from the tenant's largest `F`; book, hand over (`ReconfigureMatchmakers` fenced by generation), release; never onto a down target; keep it odd | The registry, the set in force (`Inspect`) | The grid controller of each journal, through the port: each journal has its own matchmaker set since 2026-11-19 (§3.4, §J3.4), so the set of a tenant is gone. Its handover is not an action of v1's `decide` (§J6.2): answer 1 below |
| C21 | `fixture.rs` | Test registry folded one command at a time | — | The walks of `control-sim` (#1423) |

#### The machine (`crates/lib/paros-machine/src/`, 11.1k lines)

| # | Module | Duty | State it reads | New home |
|---|---|---|---|---|
| M1 | `lib.rs` | `CellMachine` (formed or admitted) and its `CellHost::serve`: start views, registration, the candidacy, the frontend and the matchmaker host by hand; `candidate_plan` | The machine record, the cached registry | `paros-machine` (#1426) for the life of the machine; the services are hosted through ports by the adapter (#1427); the candidates come from `paros-election` (#1420) |
| M2 | `lifecycle.rs` | Format once (a random `node_id`), refuse amnesia and a changed class, wait, serve | The disk, the machine record | `paros-machine` (#1426), part 1 |
| M3 | `wait.rs` | The idle machine contract: `Identify`, the decree as acceptor, `CellInit` as proposer, `Admit` | The machine record (promise and vote) | `paros-machine` (#1426); `Identify`, the bootstrap and the admission are fixed services of the adapter (#1427) |
| M4 | `cell_init.rs` | The decree proposer: every member in Phase 1, a majority in Phase 2, a wiped member by identity, the ballot draw | The answers of the listed machines | `paros-machine` (#1426), on the shared register kernel and the checked storage contract |
| M5 | `formed.rs` | A formed vote is final; the teacher sends Phase 2a again to a member that answers idle | The machine record, the peers' `Identify` | `paros-machine` (#1426) |
| M6 | `admitted.rs` | An admitted machine finds its cell in its record, serves the node loop, answers `Identify`, `Inspect`, `Admit`, `Resolve` | The admission record, the cached registry | `paros-machine` (#1426); the boot path restores its roles before it registers (#1414, #1426 part 2) |
| M7 | `pin.rs` | The bootstrap pin: an idle machine checks an `admin` token on the calls that put it in a cell | The pinned root key, the machine clock | `paros-machine` (#1426), through the Biscuit adapter of #1431 |
| M8 | `disk.rs` | The disk layout and the atomic record rewrite (stage, sync, rename, sync the directories); `read_durable` | The provider's files | `Storage` (#1414) and the disks of the adapter (#1427); the record goes into the checked storage contract (#1426) |
| M9 | `stores.rs` | Provision a journal store at its first open, the `created` and `deleted` lines as commit points | The machine record | The durable ledger of `paros-machine` (#1426, part 4): a deleted key never starts again |
| M10 | `matchmakers.rs` | The matchmaker host: provision a set at its first request, keep a frozen set open after its release | The `sets` file, the registry's bookings | The generic host (#1410, #1416) hosts the matchmaker role; the release rule is `paros-machine` (#1426, part 4) |
| M11 | `register.rs` | Register the advertised address with the coordinator; stop on its answer or on the machine's own read of the registry | The election journal, the registry | The registration of #1422 ("I am up", the hosted roles) into the info tables (#1425); the adapter sends it |
| M12 | `follow.rs` | An admitted machine follows the registry from its book | The registry through its peers | `CellInfo` read by long poll (#1425) |
| M13 | `cache.rs` | Write the cached registry fold durably | The followed registry | `paros-machine` (#1426): the durable table of assignments and the last cell table it read, for static stability |
| M14 | `resolve.rs` | `Resolve`: a tenant's name to its cell and machines | The universe directory, the registry | The frontend role and the resolver (#1431), on the fold of `paros-universe` (#1432) |
| M15 | `resolve/entry.rs` | `ResolveEntry`: the resolver's redirect to a cell, from the last directory read | The universe directory | The resolver of #1431 |
| M16 | `views.rs` | `View`: the cell view built from the folds, filtered by scope, with the status word and its hysteresis | The registry, the directory, the election journal, the tenant control journals | Each level's crate builds its view from its fold (#1423, #1430, #1432); `parosctl` asks it (#1434); the status of a journal is its state of §J6.2 |
| M17 | `load.rs` | Sample the machine's busyness and answer `Load` | The provider's counters | The `Events` of each layer and `paros-metrics` (#1428) |
| M18 | `inspect.rs`, `inspect/names.rs`, `inspect/scratch.rs` | Offline store inspection on scratch copies, with names from the folds on the disk | The data directory | `paros-cli` (#1434), over the role list of `Storage` (#1414) |
| M19 | `rig.rs` | A test rig of machines on moonpool's simulated network and disk | — | The simulation of the whole service (#1429) |

#### The node part of the old roles (`crates/lib/paros-roles/src/node/`)

| # | Module | Duty | State it reads | New home |
|---|---|---|---|---|
| N1 | `system.rs` | The control journals' follower: fold the registry and each hosted tenant's control journal; start a hosted tenant's journals, stop a dropped tenant's; admit peers to the pool; stop on its own retirement | The registry and the tenant control journals, local or by a long-poll `Read` | The info tables (#1425) for the peers and the references; from a booking to an `Assign` (#1424) for the start and the stop of a role |
| N2 | `control.rs` | Apply each fold event: open a journal, stop an unbooked one, keep a released one answering until `may_retire`, refuse an unknown journal, enter read-only, rejoin | The follower's folds, the journal's node | `Unassign` starts a release that the role ends, and the ledger (#1426, part 4); an unknown role is refused by the generic host (#1416); read-only comes from the typed `Full` fault of `Storage` (#1414) |
| N3 | `host.rs`, `MachineFacts`: `node_id`, `class`, `locality`, `disk`, `name` | The pure facts of a machine | The machine record | `paros-machine` (#1426) |
| N4 | `host.rs`, `MachineFacts`: `addr`, `listen`, `names`, `incarnation`, `machine_interface` | The address and the incarnation of the runtime | The RPC runtime | The adapter (#1427): a reference names one incarnation (§4.4) |
| N5 | `host.rs`, `MachineFacts`: `load`, `metrics`, `frontend` | Load, metrics and the frontend role setting | The load board, the metrics handle | `load` and `metrics`: `Events` and `paros-metrics` (#1428). `frontend`: the frontend role (#1431) |
| N6 | `host.rs`, `CellHost`, `FormedCell` | The port through which the node loop starts the machine's services; an initial member's formed cell | The machine | The adapter hosts the services (#1427); `FormedCell` is `paros-machine` (#1426) |

#### Duties that had no home, and their home

Three duties of the tag had no home in the layer table. Pierre gave each one its home on
2026-11-22 (#1412 (the duty table)). One rule covers the three: **the tenant coordinator gives
birth to a journal and keeps its controller alive; the grid controller of the journal keeps
every other role of its journal alive; the cell says where, through the placement port.**

1. **The matchmaker set of each journal** (C20). Every journal has its own set (§3.4, §J3.4);
   there is no set of a tenant (#1430 (paros-tenant)). The tenant coordinator books the first
   set when it creates the journal: the grid controller is elected by a term in the register
   that this set holds, so the set exists before any controller. After that, the grid
   controller of the journal hands the set over off a machine that leaves. It asks the port for
   the target, and the handover names that target (§3.4). The handover is a step after v1
   (§J6.2). When the set is below a majority, no controller can act: hold and page.
2. **The leader of a journal on a machine that leaves, and the balance of leaders** (C14, C17).
   The grid controller still never calls `SetLeader`, which is the fence of the writer and
   belongs to the client (§J6). The Paxos leader moves by the cooperative handoff, as the end
   of its release: the grid controller releases the leader on the machine that leaves and names
   the successor, and the leader role hands its ballot on and then stops (a release is ended by
   the role, §4.4). A handoff that fails costs availability, never safety
   (`specs/tla/leader-handoff/`). The balance of leaders stays as a feature, after v1: the cell
   decides it, as it decides the balance of acceptors, and gives it as a "must leave" answer of
   the port; the same handoff runs it. v1 takes no action from load (§J6).
3. **The stateless roles of a journal and the grid controller itself** (C18). The grid
   controller books the proxy leaders, the batchers and the leader candidates of its journal
   through the port, and replaces one on a machine that leaves. The counts come from the
   `Configure` of the tenant. A controller cannot book itself, so the tenant coordinator books
   the controller candidates of each journal and replaces one that is gone. **Open**: no rule
   of `docs/journal/` (tag `journal-design-a`) says where the leader candidates of a journal run; this answer puts them
   with the stateless roles, and the issue of the port (#1421) confirms it.

The live moves that v1's `decide` does not make (a drain, a slow machine, a maintenance window,
the balance of acceptors, a `Configure`) have a home: #1421 gives the grid controller's loop the
port for heal, drain and balance, and #1430 gives it the desired mode.
