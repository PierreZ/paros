# paros-sim

The DST harness on top of `paros`: moonpool `Process` adapters, the deployment/role map, the
fault world, the one client workload and the audit. Stack: `paros-core` ←
`paros` ← **`paros-sim`** ← `paros-sim-runner`. Correctness lives here (audit + workload
`check()`), never in a trace scan. Doctrine: root *Simulation rules*, *Turbulence layers*,
*Audit, correctness, assertions, spans*.

## Map

- `lib.rs` → `chain_builder`, `chaos_surfaces()`, the campaign constants, the entry points.
- `roles.rs` → `Deployment`, `Role`, group names, `joiner_node_id`, `replica_node_id` → the per-seed role map.
- `shape.rs` → `NodeShape`, `QuorumPolicy`, `JournalPlan` → every per-seed and per-node draw (below).
- `process/mod.rs` → `NodeProcess::chaotic`, `MatchmakerProcess`, `ProxyProcess`, `ReplicaProcess`, `dispatch`, `arm_role` → one process group per role (#416): `process/acceptor.rs` (`run_acceptor`, one `Seat` per journal, the system journals' rig), `process/stores.rs` (`SimStores`, provisioning), `process/matchmaker.rs`, `process/proxy.rs`, `process/replica.rs`, `process/joiner.rs` (`JoinerProcess`, `IdleProcess`), `process/contract.rs` (`ContractSuiteWorkload`).
- `machine.rs` → `MachineProcess`, `MachineBoard` → the machines (#246): the shipped `paros::machine::run_machine` on a bare `ProviderDisk` over the simulated disk (no wrapper, #294), from an empty disk, formed by the workload's `init` (`cell init`'s decree over the layout's founding members, #277); stores ordered (a cut mid-commit is torn or whole), outside the injector; oracles: no cell forms without `init`, every formation names the one cell, only a founding member forms and over exactly the founders, every admission names a cell `init` formed and an admitted machine holds no vote and no promise (#216), the other cell an operator founds is a plan over its one machine and never the run's (#216), a durable cached registry fold only moves forward and a boot reads only a cache its machine wrote (#211) (the lifecycle reports through the audit port, `NodeAudit::on_machines`), a failed record write restarts the machine (the one loop: `parosd`'s supervisor); its faults are the lifecycle's own `hint!`s, struck by the machine group's attrition, which also draws moonpool's `CrashAndWipe` (`MACHINE_WIPE_WEIGHT`): a wiped machine is a new one, recognized at its boot on an empty disk; a vote that names the old one keeps it as a dead member, and the cell is lost only when a majority of the plan's members are wiped (`cell_lost`, the Paxos limit; the control-plane liveness is excused only then).
- `machine.rs` (#257) → `names`, `process_ip`, `boot_listen` → the machines' name table (moonpool's `ScriptedResolver`, `machine-<rank>.paros`): on a seed whose layout is `named`, each machine advertises its name, and a rebooted machine comes back at a new IP behind it (`MachineLayout::move_pct`, a knob: default 0, extreme 20–80%, floor 0; the new IP is an alias its name points to); the board maps an advertised address to the process IP a kill strikes. Moonpool's network faults strike by process IP, so a moved machine's alias IP is outside them. A cell machine's reboot may also come back under a new name at a new IP (`MachineLayout::rename_pct`, #349, a knob: default 0, extreme 20–60%, floor 0): `machine-<rank>-<n>.paros`; the old name keeps the old IP, where nobody listens. The machines resolve through one table and the workload's clients through another (`names`), where the rank's name follows its machine, as an operator updates its own entry. A rename never takes the cell's majority: no founding member wiped, no other founding member renamed, and not a cell of two (`may_rename`); a later wipe counts a renamed member as lost (`cell_lost`). The board knows a machine by its rank's address. After chaos the final check judges that the registry holds every renamed founding member at its new name (`FleetOps::renamed_registered`).
- `lifecycle.rs` → `ScriptedLifecycle` → the `fault_factory` injector (the chain client's operator crashes and reboots, #173).
- `client.rs` → `ClientRuntime`, `ChainClient = paros::client::Client<SimProviders>`, `Connector` → a workload's client-only RPC runtime; `Connector` builds clients over servers learned at runtime (the machines, #246).
- `state.rs` → `published`, `journal_key` → get-or-publish of per-iteration singletons on the `StateHandle`.
- `chain.rs` → `ChainState` → the Chain-of-Blocks fold a journal client computes (#186).
- `chain_workload.rs` → `ChainWorkload`, `Step` → `setup`, the operation program in `run`, `check`. `chain_workload/config.rs` → the op alphabet, `ChainConfig` and its weights. `chain_workload/write.rs` → ops 0, 1 and 5 (`write_step`, `dup_write_step`), `Submission`. `chain_workload/truncate.rs` → ops 2 and 7. `chain_workload/reconfigure.rs` → ops 11 to 13, the reconfiguration shape rings, `compose_reconfiguration`. `chain_workload/reads.rs` → `judge_read`, `Tail`, `SETTLE` (#415).
- `chain_workload/rpc.rs` → `CallLog` → the library's `CallObserver` (the history), per-answer oracles, one-attempt calls, the retry-identity oracle (`open_write` / `close_write`).
- `chain_workload/races.rs` → races 1 and 2 of #205 (`burst`, `ack_race`).
- `chain_workload/multi.rs` → the ops on a multi-writer journal (#241): unfenced appends through `paros::client::multi`, re-sent at-least-once, open truncations, and the wrong-mode calls (a claim, a fenced write, a fenced truncate, #339) that must be refused; a single-writer journal's unfenced write and unfenced truncate are refused too.
- `chain_workload/owner.rs` → an owner's own misbehaviours on a single-writer journal (#339), each its own BUGGIFY location: `SetLeader(L, Some(L))` from the leader `L` and a write ahead of `next_seq`, both refused.
- `chain_workload/foreign.rs` → the cross-tenant attack (#247): a `Write`, `Truncate` or `SetLeader` under another tenant's journal or an identifier nobody serves, refused and never applied (`AuditWorld::note_foreign`).
- `chain_workload/fold.rs` → the client's fold and the trim fence · `chain_workload/system.rs` → ops 19–21, 23 and 24, and their read-back (the registry's through a checkpoint `Folder`); `Announce`, the audit observer for library writes to system journals and the logger of every attempt at them into the control journals' shared history (#247, `rpc::control_attempts`); over the machines it holds the "no unlearned id" oracle (`Announce::learned_only`, #246): every call names a journal its operator learned from `init`'s reply or through `Inspect`.
- `chain_workload/fleet.rs` → `FleetOps` → ops 25 and 26 (#229, #246): `init` whole through `paros::client::initialize` against the machines (`cell init` over the layout's founding members — started at a drawn founder, or with a second concurrent `cell init` at another founder that must converge on the one cell, on their own BUGGIFY locations — again once known, or misdirected to a non-founder that must refuse it as `not_a_member`), the cell learned from that run or through `Inspect` (never injected), `init`'s fleet half and tenant create/remove through `FleetSession`, the crash-at-a-step, target-kill (#247), changed-identity and fleet-tenant checkpoint-crash shapes, a reachable per `Stage`, and the check that the fleet directory equals the cell's tenant list — mid-run when both folds are one instant's, and on every run over the final folds, with the recovery tail's control-plane liveness (`settle`, `final_check`).
- `chain_workload/fleet/resolve.rs` → `Resolve` at the entry endpoint (#216): after each created tenant's library resolution, the operator asks the machines (every one, from a drawn one on; idle ones passed over) through `paros::client::resolve`; oracles: a removed, internal or never-created name (`umbrella`, on a BUGGIFY draw) never resolves, a resolved tenant is the operator's cell's, and the answer decodes.
- `chain_workload/fleet/other_cell.rs` → another cell (#216): on its own BUGGIFY location in `ADMIT`, an operator founds a one-machine cell on the machine that replaced a wiped member the run's cell still names, so that cell's peer traffic meets another cell (`Deliver`'s `cell_id` refuses it); operators learn their cell from a majority of the founding members.
- Names (#239): a created journal is addressed by its name through `paros::client::names` (`SystemOps::address_created`), each resolution held against the directory the nodes folded at the position it was read at (`SystemBoard::named_at`); a cached resolution is read through on a later step and, refused as unknown, dropped and resolved again; a created tenant's name resolves through the universe directory, never to a removed tenant, and an internal tenant never resolves (`FleetOps::resolve_tenant_name`).
- `chain_workload/fleet/admit.rs` → op 27 (#216): `cell add-machine` against the machines through `paros::client::cell::CellSession`, judged (a founder is in its cell already, `in_cell_init` only at a founder mid-decree, another cell only after a wipe), a reachable per `Stage`; the final fleet check asserts every admitted machine is registered in its cell.
- `chain_workload/fleet/election.rs` → op 28 (#240) and the election oracles: one leader per term across every fold (`note_terms`), and after chaos the election settles on one renewing leader (`election_settles`, excused only for a lost cell). The deposed-actor oracle (no fenced call under a uuid a refusal named deposed) is `Announce`'s. The founding members' coordinators log their calls through `Announce::of_machine`, handed over by `Audit::call_observer`; the election journal's history is checked multi-writer, and the audit models it multi-writer (`machine::is_election`).
- `chain_workload/fleet/view.rs` → op 29 (#399): one administrative view through `paros::client::views`, the scope drawn per call (admin, or a tenant by one of the run's names) and the query too (cell, tenant, universe); oracles: a tenant scope sees only its own spread and is refused (`forbidden`) exactly beyond it, an answer from a member names its cell, a tenant view names only the machines it uses, a cell view marks exactly the founding members, a member's registry position never goes back.
- `chain_workload/fleet/load.rs` → op 30 (#424 (busyness metrics)): an admin cell view, then `Load` to every machine it names through `paros::client::load` (before a cell formed, to the machines the operator was given); oracles: every ratio in range, `cpu_cores` never above `cores`, no window shorter than `LOAD_INTERVAL_FLOOR`, and every answer exactly `Busyness::between` of two samples moonpool handed that machine's process (`SimContext::system_samples`), CPU model on or off; gates: a disk and a CPU over 90 % busy, a slow machine answers (`SimContext::slowness`), and in one round a CPU- or disk-slow machine is busier than every healthy one (the slow-machine scenario lines them up).
- `chain_workload/fleet/cell.rs` → `Cell` → `init` whole against the machines (`paros::client::initialize`) and the cell learned from that run or through `Inspect` (#246).
- `world/mod.rs` → `StorageWorld`, `storage_world_for` → the storage ledger: copy budget, parked ids, provisioning ledger, reconfiguration ledger, custody ledger. There is no fake disk: every role stores on the library's journal stores over moonpool's simulated disk (#261).
- `world/node_store.rs` → `LedgeredJournal` → `JournalStorage` on `SimStorageProvider` (#187) for acceptors, replicas, joiners and every journal; the journal store tells the audit what each commit has in flight (`AuditWorld::note_in_flight`, #264).
- `world/registry_store.rs` → `LedgeredRegistry` → `JournalMatchmakerStorage` on `SimStorageProvider` (#176), with the provisioning ledger and the registry's in-flight writes (`AuditWorld::note_registry_in_flight`).
- `world/cut.rs` → `InFlight`, `Budget`, `Owner` → the copy budget of a power loss inside a commit (#176, #294). The cut itself is the shipped stores' own `hint!`s (inside a `moonpool-journal` commit, and between the commits of one `paros::journal` sync), struck by the attrition regime; a store registers each writing commit while it is in flight, and moonpool's `HintVeto` permits a kill only if every commit the process has in flight fits its budget, then spends it. One reachable per owner.
- `world/injector.rs` → `Custody`, `Injection`, `apply`, `judge` → the ledgered journal-aware injector (#261): the custody ledger each completed sync records (`note_synced`), one family of boot-time byte damage per boot (entry rot, record rot, double fault, metainfo rot, header rot, each its own BUGGIFY location and budget), judged against the journal's verdict at open; an entry rot aims at the most recent slot held, the oldest (a fold hole under the chosen index, #343) or a uniform one. A double fault's plan only reserves its corruption park; the journal's next verdict decides it (#351): a refused open makes it terminal, an open releases it (an entry reported faulty there a lost copy). A BUGGIFY location stops its apply halfway, and a `hint!` between its two regions lets attrition kill the process there.
- `world/outage.rs` → `regime`, `OutageLosses`, `LossShape` → the correlated outage (#263): moonpool's `Chaos::Outage` (moonpool#311) takes every acceptor and proxy down at once on some seeds, each back after its own delay (one straggler last); at its `OutageLanded` notice, a loss of one slot's copies planned for each holder's next boot (`StorageWorld::plan_outage_loss`: aimed at the most recent or a uniform slot, the usual budget or the loss budget's extreme leaving one clean copy or none, or — the departed straggler — exactly one clean copy, on a node the operator's last reconfiguration removed, at a slot most of the successor never held when there is one (#267), and on a BUGGIFY coin every spare's copy kept clean beside it, a spare's re-accepted copy above the straggler's ballot that no Phase 1 asks (#375); the outage strikes 2.5 s into the window at the earliest, once a kind seed has claimed, reconfigured and re-elected).
- `world/late_outage.rs` → `LateOutage`, `LATE_WINDOW` → the departed-straggler scenario's outage (decided on 2026-10-09): on a scenario seed, once a leadership won under a configuration that removed a bootstrap member (`AuditWorld::has_departure`; a lone matchmaker registration does not count, #278), every acceptor and proxy goes down at once, at most `LATE_WINDOW` into the recovery tail, the straggler loss planned at that instant and its one clean holder back last (#267); the owner's after-claim removal, a rotation onto the spares (the scenario bootstraps at the floor), stays armed until then.
- `world/bare_outage.rs` → `BareOutage` → the bare-quorum scenario's outage (#270): on a scenario seed (`shape::bare_quorum`), every acceptor and proxy goes down the moment the custody ledger holds a decided slot a deciding member never held (`StorageWorld::holds_short_slot`), and every copy of it is lost (`LossShape::BARE_QUORUM`), so the tally reads `faulty, faulty, none`; its gate fired on 3 of 2,094 checks over 1,000 hunt seeds before it and 42 of 2,898 over 1,400 after.
- `world/wiped_founder.rs` → `WipedFounder` → the wiped-founder scenario (#246, `shape::wiped_founder`): client 0 runs `init` first, and once every founder promised (a founder other than `cell init`'s receiver) or once a founder voted and another did not (that one), the founder is wiped through moonpool's `CrashAndWipe`; the founders that kept their disks then choose the plan with the old id as a dead member, a two-founder decree with no vote redraws over the new machine, and a plan that lost a majority is refused `cell_lost`.
- `world/silent_machine.rs` → `SilentMachine` → the silent-machine scenario (#211, `shape::silent_machine`): once the cell formed, one admitted machine (or a founder of a cell of three or more) crashes through moonpool's `Crash` and stays down 4.5–6.5 s, past every `machine_down_after`, so the cell coordinator marks it down and then up; it may strike up to `LATE_WINDOW` into the tail, the cell keeping a majority. Gate rate: 1 machine marked down and none back up in 1,000 hunt seeds without it; 51 marked down and 9 back up in 2,000 with it.
- `world/slow_machine.rs` → `SlowMachine` → the slow-machine scenario (#424 (busyness metrics), `shape::slow_machine`): client 0 runs `init` first, every machine samples at the 1 s `load_interval` floor, `LOAD` runs at `OP_WEIGHT_CEILING` whatever the swarm mask, and once the cell formed the first live founding member is slowed through moonpool's `FaultContext::set_slowness` for 4–6 s: CPU ×500–1000 (only on a CPU-model seed), disk ×50–100, or both; then healthy again. It may strike up to `LATE_WINDOW` into the tail; a slow machine stays a member. Gate rates over 2,000 hunt seeds without it / with it (struck on 934): a disk over 90 % busy 0 of 3,354 / 1,761 of 24,635; a CPU over 90 % busy 0 of 3,354 / 162 of 24,635; a slow machine busier than the healthy never judged / 582 of 846.
- `world/lagging_acceptor.rs` → `LaggingAcceptor` → the lagging-acceptor scenario (#340, `shape::lagging_acceptor`, never on a departed-straggler or bare-quorum seed): one acceptor crashes 0.8–2 s into the chaos window (once no budgeted commit is in flight on it) and stays down until a peer's floor passes the chosen prefix it holds (`AuditWorld::floor_passed`), at most `LATE_WINDOW` into the tail; every client compacts at every truncation step. It boots below every floor and jumps to a peer's trim point. Gate rate: 4 jumps in the mutation hunt's 300 seeds without it, 7 with it.
- `world/settle.rs` → `settle_store` → the operator's durable probe (#348): a store settled (`paros::journal::settle`, a failed sync retried up to the wipe's hang guard) before an interrupted provisioning is resolved from its marker.
- `world/moved_founder.rs` → `MovedFounder` → the moved-founder scenario (#211, `shape::moved_founder`): the machines advertise names; once another machine of the cell cached the registry, a founding member crashes and comes back renamed, then a machine whose durable cached registry fold names its new name crashes, so its boot dials the founder where only its cache knows it is (`machine: a cached registry fold serves a boot with a moved machine`). Gate rate: 0 of 21 cached boots in 300 hunt seeds without it; fired within 300 with it.
- `world/replaced_founder.rs` → `ReplacedFounder` → the replaced-founder scenario (#423, `shape::replaced_founder`, never on a wiped-founder or moved-founder seed): the layout lists three founders or more, client 0 runs `init` first, and once every founder formed the highest-ranked live founder is wiped through moonpool's `CrashAndWipe`; client 0 then admits the idle machine at its address (`chain_workload/fleet/replaced.rs`) and runs `init` again, which must pass the admitted machine and find the cell (`init: a re-run passes an admitted machine at a listed address`).
- `world/wipe.rs` → `wipe_dir` → the wipe coin's physical half on a journal seed: the journal's files deleted and the deletion synced.
- `audit/mod.rs` → `NodeAudit`, `reach_once!` (the matchmaker callbacks delegate to `audit/matchmaker.rs`, #417) · `audit/world.rs` → `AuditWorld`, `audit_world_for`, `check_run`, `check_final_convergence`.
- `audit/state.rs` → `AuditState` (per-transition protocol safety) · `audit/matchmaker.rs` → `MatchmakerAudit`.
- `audit/losses.rs` → `Losses` → an outage's losses as the journal reports them (#263): the shape recognized (no, one, fewer than a quorum of clean copies; `faulty, faulty, none`; the only clean copy on a removed node), the CTRL rule re-derived to name a slot unrecoverable, "an unrecoverable slot is never accepted again", the four outcome gates, and the convergence excuse.
- `audit/client.rs` → `ClientHistory`, `check_control_history` (the registry, the control journals of the cell the machines formed, and every tenant control journal a machine folded, #247, #246, #210; the cell's election journal multi-writer, #240) · `audit/linearizability.rs` → Wing & Gong search over every attempt (#205), its own journal model; an unknown attempt leaves the search past its deadline, waiting copies stay out of the scan, and an unknown claim no answer can observe is not judged (#392), so the steps grow with the history.
- `audit/journal_model.rs` → the §6 invariants over every node's `applied` reports (one verdict per slot, dense positions, the leader chain — every verdict names the leader in force, a write accepted only under a uuid won in the log, a won `SetLeader` only over the leader in force and with another uuid — monotone `first_seq`; a reinstated uuid is reachable, never a violation, #241; the wrong-mode check runs both ways, and a write ahead of the journal has its own gate, #339).
- `audit/journals.rs`, `audit/system.rs`, `audit/tenants.rs` → the journal board (#188), the system board (#189) and the tenant board (#210), below.
- `chain_workload/fleet/journals.rs` → ops 17 and 18 (#210): a tenant's journals through the tenant coordinator (`paros::client::journals`).
- `audit/liveness.rs` → `JudgedRegistry` → the cell control journal's fold with an observer (#211): the final fleet check reads the cell through it, and every liveness refusal in the log (`LivenessUnchanged`, `StaleIncarnation`) is a coordinator that wrote a non-change; gates for a machine marked down, one back up without re-placement, and a rebooted machine registered again.

## Harness shape

- **Main campaign**: process groups `paros-node` (acceptors, 3–6), `paros-matchmaker` (0–5),
  `paros-proxy` (0–3, `ProxyId(rank)`), `paros-replica` (0–2, `NodeId(1000 + rank)`, a quiet
  journal store outside the copy budget), `paros-joiner` (0–2, `NodeId(100 + rank)`, idle without system
  journals), `paros-machine` (1–3, a minted `node_id`; `shape::machine_layout` draws the founding
  members, each machine's class, capacity and failure domain). Attrition per group (`AttritionVictims::group`); joiners are no victim. 1–3
  `ChainWorkload` clients. Zero matchmakers = the plain Multi-Paxos deployment.
- **Bootstrap**: `bootstrap_ranks` — the whole pool, or on a matchmaker seed a subset of at least
  `MIN_BOOTSTRAP` leaving *spares* a `Reconfigure` pulls in.
- **One campaign** (#263): the scripted CTRL corpus is folded in. Shapes are provoked through
  BUGGIFY and swarm, never scripted: the correlated outage and its planned losses
  (`world/outage.rs`), aimed rot, `withhold_gc` (a removed straggler stays worth waiting for),
  an owner's member-removing reconfiguration right after its claim (`ChainConfig::reconfigure_after_claim`),
  each recognized by the audit from the ledger and judged by the same oracles
  (`audit/losses.rs`).

## Per-seed draws (`shape.rs`)

- `quorum_policy` → `Majority`, `Flexible { q2 }` (`q2` knob clamped `1..=n/2`, `q1 = n - q2 + 1`),
  or `Grid { rows, cols }` from `grid_layouts` (floor `rows >= 2`, `cols >= 2`: `2×2`, `2×3`,
  `3×2`). `QuorumPolicy::system(n)` applies it per configuration size; a size no layout tiles
  runs a majority. The composer may switch a successor to majority, never the reverse.
- `config_floor` → `MIN_BOOTSTRAP` on a matchmaker seed, the whole pool otherwise.
  `QuorumPolicy::clean_copies(floor, pool)` → floor minus the smallest `tolerated_loss` over
  `floor..=pool`; a grid tolerates zero, so a grid seed injects no lost leg and parks nobody.
- `journals` → `JournalPlan`: 1–3 journals (on matchmaker seeds too, #201), on a multi-journal seed a draw that
  turns on `paros::scenario::HOLD_JOURNAL` (`set_activation`): every node holds its highest journal for the chaos window, and the driver reports it (`Audit::journal_held`). Each journal but the first is multi-writer on a coin (`JournalPlan::multi`, #241); a directory create draws its mode too, and `writer_mode` answers the mode of either. The first is the run's main identifier (`Identifiers::main`; `identifiers` draws it, the
  directory's and the registry's once per seed: no identifier is fixed; the cell's and the fleet tenant's are `init`'s, on a machine); the
  others' identifiers are drawn
  (#235: a random journal id in the default tenant or a random one, sometimes the first's journal
  id under another tenant). `journal_layout` → every seed's journal stores (#176, #261): acceptors, replicas, joiners
  and every journal on `JournalStorage`, matchmakers on `JournalMatchmakerStorage`; its
  `Durability` a knob (two syncs by default, one at the extreme) and its segment geometry a knob
  per region (`journal_geometry`: floors `PERSIST_BLOCKS_FLOOR` = 2 blocks,
  `ENTRY_BLOCKS_FLOOR` = 17, the workload's largest write). A crash at a driver `hint!` is a
  power loss the attrition regime takes, a cut mid-commit is a journal store's own `hint!` budgeted by `world/cut.rs`, a wipe deletes the journal's files, the ledgered injector
  damages a boot, and moonpool's storage chaos runs under it. A quiet seat (a replica, a held
  journal) stores ordered with the injector dark; a cut there is torn or whole and spends no budget. `withhold_gc` → a seed
  whose nodes withhold GC requests for the chaos window (#263). `lagging_acceptor` → the lagging-acceptor scenario (#340): one acceptor held down until a peer's floor passes it, and every client compacting at every truncation step. `stalled_proxy` → the stalled-proxy scenario (#341): every proxy drops the acceptors' answers for the chaos window (`paros::scenario::STALL_PROXY`), a leader that holds delegated rounds resigns and campaigns again at once (`RESIGN_DELEGATING`), and `quorum_policy` runs a grid where the pool tiles one; so a leader takes rounds back on a grid column, a proxy evicts rounds nobody answers, and a new leadership's delegation meets the rounds a proxy still holds. `system_journals` → the registry on half the seeds (kept at 50% from the sweep's coverage, #247; the fleet operations run on every seed, against the machines, #246), on
  `SEED_COUNT` (1) seed ranks. `NodeShape::draw` → `DriverTunables` (one knob per field, the recovery page size included (#330: `1..=LEADER_RECOVERY_BATCH`, so a leader's recovery spans several pages), and the promise, re-send, apply and registry page sizes (#338: an extreme of `1..=4` under each 64-entry ceiling, so a Phase 1, a re-send cursor, an apply walk and a matchmaker history span several pages; `pair_extremes` holds the BUGGIFY pairings), or on its own location the whole `DriverTunables::production()` profile `parosd` ships, #209), wipe/loss %, `config_edit_pct`.

## Chain workload op ids (`chain_workload/config.rs`; ids never shift)

`WRITE=0` (owner writes at its believed next position; a superseded writer's stale write must be
refused, unless a reinstatement made its uuid lead again) · `WRITE_TO_NON_LEADER=1` · `TRUNCATE=2` (an owner's, under its own fence and clamped by the trim fence; a superseded owner's stale truncate, `stale_truncate_pct`, must be refused, #228) · `READ_STATE=3`
(fold to tail) · `PAUSE=4` · `DUP_WRITE=5` (must fold `Duplicate`) · `DUAL_SUBMIT=6` (one
position per verdict) · `TRUNCATE_STORM=7` · `READ_INDEX=8` retired · `MATCHMAKE=9`,
`MATCH_GC=10` retired · `RECONFIGURE=11` (compose from the live pool; refused on a plain seed)
· `RECONFIGURE_MATCHMAKERS=12` · `RETIRE=13` · `QUORUM_READ=14` retired · `READ=15` (judged as
it arrives) · `CHECK_TAIL=16` retired · `CREATE_JOURNAL=17`, `DELETE_JOURNAL=18` (#210: a request to the tenant
coordinator on the machines through `paros::client::journals`, in a `READY` tenant of the fleet
directory: an idempotency id the client draws, a name from three, a writer mode and a desired
mode; an undecided request is sent again with the same id at the next journal step, a decided one
on a BUGGIFY location must read back its first answer, or `unknown_tenant` once the tenant is removed (on its own location the client removes it before the retry, #395); a created single-writer journal takes one
append) · `REGISTER_NODE=19`, `DRAIN_NODE=20`, `RETIRE_NODE=21` (a `Write` to the registry; refused
`unknown_journal` without system journals; a register carries the joiner's drawn class and
capacity, and a registered joiner registering again is a reboot, #211) · `SET_LEADER=22` (CAS on
the leader uuid; a superseded writer reinstates the uuid it last led with, `reinstate_pct`, the misbehaviour the journal does not refuse, #241) · `CHECKPOINT=23` (the registry's owner, through `paros::client::checkpoint`:
claim, fold to the tail, checkpoint and truncate when the policy finds it due, #230) ·
`BOOK_CAPACITY=24` (book or release a joiner's slot for a role of a journal or matchmaker set; a role of the other class must be refused, and so must a booking under an id the client released,
#211) · `FLEET_INIT=25` (`init` whole through `paros::client::initialize` while the cell is unknown or on a coin, else `init`'s fleet half through `paros::client::fleet`, #229, #246) · `TENANT=26`
(create or remove a tenant through the fleet tenant and the cell; either may stop after one step, a BUGGIFY
crash, and is resumed by the client's next fleet step) · `ADMIT=27` (`cell add-machine` through
`paros::client::cell`: an idle machine outside the founders, or on a coin a founder, registered
then admitted; it may stop after the registration and is resumed by the client's next `ADMIT`,
#216) · `ELECTION=28` (a candidacy in the cell's election through `paros::client::election`, beside the founding members' coordinators: on BUGGIFY locations it campaigns without waiting out the lease, first stops an admission after its registration so a won term finishes it, then hands its term on to a founder, resigns or abandons it; a won term runs the coordinator's own `serve_term`, #240) · `VIEW=29` (an administrative view under the admin's or a tenant's scope, #399) · `LOAD=30` (how busy every machine of the cell is, through `paros::client::load`, #424) · `OP_COUNT=31`. Retired ids are no-ops that keep their slot in the alphabet.

- Each client is an **owner** or a **reader** for the run (knob; each journal's first client
  owns). Every library writer, session and checkpointer a client starts draws a fresh leader seed
  (`LeaderSeeds`, #241), so a well-behaved client never reinstates a uuid. Owners claim before writing — the opening claim re-asked within `claim_patience_ms`
  while the cluster leaves it unresolved — and re-claim when superseded.
- Every client folds its journal into `ChainState` and reports each step
  (`AuditWorld::fold_applied`); truncations are clamped below the lowest folding cursor.
- Races (#205, each a BUGGIFY location): a claim racing its own burst, a write with
  `ack_race_timeout_ms` below its ack retried across a re-claim, a `READ` from a lagging cursor
  racing the client's truncation.
- **Every call goes through `paros::client`**; misbehaviours (`Writer::stale_entry`, `Writer::stale_truncate_request`,
  `write_attempt`, `DUAL_SUBMIT`, `DUP_WRITE`) are explicit calls. Timeouts are `Ambiguous`; a
  retry is the identical write. `ChainConfig::tunables` maps knobs onto `ClientTunables`
  (`write_redirect_limit` → `redirect_limit`, `resolve_attempts` → `retry_budget`, …).

## Boards

- **Journal board** (`audit/journals.rs`): every slot of journal `j` applies only an identity
  appended to `j` (a write sent under another tenant's fence only as a refusal, #247); a
  quarantined journal sends nothing; a sibling keeps committing while one is held; a node keeps
  serving the rest while one is quarantined; a tenant journal commits while the control
  journals' seed is held down (static stability, #247).
- **System board** (`audit/system.rs`): every node folds the registry alike per LSN; a checkpoint a node (or a client) meets
  with the whole prefix folded is that prefix's state (#230); a `stateless` joiner never serves
  a journal, a booking takes a slot of its node's class and never past its capacity, and a
  booking id is booked at most once, across checkpoints (#211, on the registry's events in LSN
  order; the model crosses a truncation at the checkpoint a restoring node meets, resuming its
  bookings and spent ids, and equals every checkpoint it reaches, #247); no genesis node's message waits on the registry fold; every live node's
  registry fold reaches the tail after chaos; gates for
  joiners learning before admission, refused-then-accepted joiner messages, a re-registration,
  and a fold restarting from a checkpoint once one truncated.
- **Tenant board** (`audit/tenants.rs`, #210): every machine folds a tenant control journal alike
  per LSN; a created journal takes a set id never used before, inside its own tenant (`IdTaken`
  only for an id used before); a request folds to one outcome, and a repeat reads it back; a
  delete names a journal created before; no append acked after its tombstone; the writer mode a
  created journal runs in (`writer_mode`); gates for `name_taken`, an `IdTaken` redraw and a
  retried request.

## Entry points (`lib.rs:323-428`)

`explore`, `run_chain_seed`, `chain_seed_digest`, `chain_seed_canary`, `chain_canary_hunt`,
`chain_smoke`, `explore_chain_seed`, `run_storage_contract_suite`.

## Local rules

- moonpool macros only; never plain `assert!`; never reword a message. Budget: 2048 slots
  (moonpool `MAX_ASSERTION_SLOTS`), 256 buckets; no slot, ballot, id, seed or hash as identity.
- No seed constants, seed lists or seed-replay tests (root *Simulation rules*).
- **No new fault wrapper here** (root *Simulation rules*, #294). This crate never decides a
  fault by wrapping shipped code: no new `Ledgered*` store, `Sim*` disk or stores, timer race
  around a commit (the deleted `PowerCut`), hook trait (the deleted `BuggifyHooks`), `crash_self` call on a code path, or workload that stops the
  library's own loop to fake a crash. Put an inline `buggify!` or a `hint!` in `paros` instead,
  and keep here only what observes (`Audit`, oracles), the environment's injectors (outages,
  disk rot) and an operator's explicit misbehaviour. `LedgeredJournal` and `LedgeredRegistry`
  are the old pattern, migrating away (`SimDisk`, `SimMachineStores`, `PowerCut` and
  `BuggifyHooks` are gone); extend none of them. A per-seed scenario decides the driver's named
  locations with `moonpool_sim::set_activation` in `shape.rs`, never with a wrapper.
- A wiped identity stays down because the **library** refuses it (#147, #183): the ledger parks
  it for the budget and composer only, and `StorageWorld::provisioned` is the `BootKind` claim.
- Operators coordinate through `StorageWorld::retire` / `reserve_joiner_retirement`. Spans are non-optional.

## Tests & gates

- `cargo nextest run -p paros-sim`: `tests/sim.rs` (single seed converges, storage contract,
  same-seed digest replay, canary pair, `chain_smoke(SMOKE_ITERATIONS)`).
  Saturation is `cargo xtask sim run paros-chain`; hunts are `sim-paros-hunt`.

## Constants (`lib.rs`, `shape.rs`)

`pub(crate)`: `PROCESS_POOL_RANGE = 3..=6` (`lib.rs:98`), `MATCHMAKER_POOL_RANGE = 0..=5`
(`:112`), `PROXY_POOL_RANGE = 0..=3` (`:124`), `REPLICA_POOL_RANGE = 0..=2` (`:134`),
`JOINER_POOL_RANGE = 0..=2` (`:141`), `MACHINE_POOL_RANGE = 1..=3`, `CLIENT_COUNT_RANGE = 1..4` (`:147`), `PLATEAU_SEEDS = 8`
(`:154`), `CHAOS_DURATION_MS = 4_000` (`:188`); `pub`: `SMOKE_ITERATIONS = 50` (`:158`),
`COVERAGE_ITERATIONS = 1024` (`:161`), `EXPLORATION_TIMELINES_PER_SEED = 8` (`:163`).
`shape.rs`: `ROUND_TRIP_FLOOR_MS = 250` (`:48`), `ENTRY_BLOCKS_FLOOR = 17` (`:657`),
`PERSIST_BLOCKS_FLOOR = 2` (`:665`), `SEED_COUNT = 1` (`:712`), `MIN_BOOTSTRAP = 3` (`:939`). `chaos_surfaces()` = `Network(Swarm)` +
five per-group attritions + the acceptor-and-proxy `Outage(Swarm)` + `BuggifyKnobs` + `Storage(Swarm)` + `Cpu(Swarm)` (moonpool's CPU model, #424 (busyness metrics)) + two `GrayFailure(Swarm)` regimes, one slow acceptor (`ACCEPTOR_GROUP`) and one slow machine (`MACHINE_GROUP`), each `max_slow = 1`; `BitFlip` masked; storage masked to
crash damage, failed syncs, short transfers and lost directory entries (`storage_fault_mask()`:
rot, phantom writes, degradation and disk failure stay out, #176); `prob_wipe = 0` except on the machines (`MACHINE_WIPE_WEIGHT`, #246).
Deps: `paros`, `moonpool-sim` (`exploration`, `Cargo.toml:20`), `moonpool-rpc` (`:29`) — pin
shared with `paros` and `parosd`.
