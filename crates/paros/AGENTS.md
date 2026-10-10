# paros

The library: `pub use paros_core::*` plus everything that does I/O — the provider-generic
drivers for the four roles, the storage seam and its stores (in-memory and the durable
`journal`), the RPC contract, the client (`paros::client`) and the system journals' folds
(`paros::system`). Written once over moonpool's `P: Providers`; the same code runs in
production (`parosd`) and in simulation (`paros-sim`). Stack: `paros-core` ← **`paros`** ←
`paros-sim` ← runner; `paros` ← `parosd`. Stays wasm-checkable and provider-free.

## Map

- `driver/mod.rs` → `run_node`, `run_journals`, `RunError`, `BootKind` → the node loop; one journal or a static list (#188); `run_journals` takes `formed: Option<FormedCell>` and serves its decree answers (#277).
- `driver/journals.rs` → `JournalStores` (`opened`: a store passed its boot, #208; `node_audit`: the node's own facts, #243), `SingleStore` → per-journal runtime and quarantine (`quarantine_ticks`).
- `driver/system.rs` → `ControlPlan`, `ControlFollower` → the control journals' follower: the registry and every hosted tenant's control journal (#189, #210); `run_journals` starts and stops journals from their events.
- `provision.rs` → `provision_store`, `provision_matchmaker_store`, `Provisioned` → format a store ahead of its first start; an interrupted run resumes from the disk (#208).
- `driver/{boot,ready,report}.rs` → format-marker check, the `Ready` I/O side in persist-before-send order, boot report.
- `address.rs` → `Address`, `Names` → a machine's advertised `HOST:PORT`, kept as written (a literal or a name), and the one resolver its dialers share; a name is resolved at dial time (#257).
- `driver/transport.rs` → `PeerMailbox`, `LaneOpener`, `peer_address`, `peer_target` → keep-newest lanes per journal, round-robin; a lane's `Dialer` resolves its peer's name before a batch and forgets it after a failed delivery (#257).
- `driver/{matchmaking,handover,operator,events}.rs` → matchmaker wire, set handover, Reconfigure/Retire/Inspect, events.
- `driver/edge.rs` → `RpcEdge`, `NodeInbox`, `ReplicaInbox`, `MatchmakerInbox` → the inbound edge, polled as a `select!` arm.
- `driver/reply.rs` → `answer`, `match_answer`, `maybe_duplicate` → the one client-reply seam.
- `driver/calls.rs` → held `Write`/`SetLeader`/`Truncate`, answered with the verdict their slot folded to (#204).
- `driver/log_reads.rs` → `JournalReads`, `ReadLimits` → the public `Read`: quorum-confirmed, served from the fold, its page cut to `max_read_records`/`max_read_bytes` and its tail wait to `min_wait_ms..=max_wait_ms` (#241).
- `driver/config.rs` → `DriverTunables` → every driver cadence/budget (the cell election's lease, renewal and compaction bound too, #240); `default()` is the sim's baseline · `driver/tunables.rs` → `DriverTunables::production`, `check_floors`, `BelowFloor` → the shipped profile and the floors (#209).
- `scenario.rs` → `WITHHOLD_GC`, `HOLD_JOURNAL`, `LOSE_VERDICTS`, `LAG_FOLLOW`, `STALL_PROXY` (a proxy drops the acceptors' answers, #341), `RESIGN_DELEGATING` (a leader that holds delegated rounds resigns and campaigns again, #341) → the named BUGGIFY locations a harness forces per seed (`buggify_named!`, `set_activation`, #318 E). Every other choice is an inline `buggify_fault_with_prob!` (send seam `driver/transport.rs`, reply seam `driver/reply.rs`, picks and handoff `driver/mod.rs`, #318), a grid leader that tears one slot's Phase-2 column and resigns (`tear_column` in `driver/ready.rs`, #396), and one plain `buggify_with_prob!` that stays on in the recovery tail: an acceptor about to overwrite a different value campaigns first (`contest_overwrite`, #376), once per `(slot, held value, incoming value)` (`JournalRt::contested`, #396). The durability moments are inline `hint!`s in `driver/ready.rs`, `driver/mod.rs` (a journal re-opened after its quarantine, before its next sync, #348), `matchmaker/mod.rs`, `replica_tier/mod.rs` (#297) and the journal stores (#294).
- `audit.rs` → `Audit`, `NoAudit` → the observation port.
- `storage/mod.rs` → `LogStorage`, `StorageError`, `StorageRecord`, `WriteOutcome` → the async seam.
- `storage/mem.rs` → `MemStorage` · `storage/contract.rs` → `storage_contract_suite`.
- `matchmaker/{mod,storage}.rs` → `run_matchmaker`, `MatchmakerStorage`, `MemMatchmakerStorage`, `matchmaker_storage_contract_suite`.
- `proxy/mod.rs` → `run_proxy`, `ProxyConfig` → Phase-2 subset, nothing durable (#142).
- `replica_tier/mod.rs` → `run_replica` → learner subset over a `LogStorage`; serves `Read` (#144).
- `rpc/methods.rs` → one `RpcMethod` per call, `WellKnownMethod` ids (public `0x5041_00xx`, internal `0x5041_01xx`, matchmaker `0x5041_02xx`, machine `0x5041_03xx`: `Identify`, `FormCell`, `CellInit` `0x5041_0304`, `PrepareCell` `0x5041_0305`, `Admit` `0x5041_0306`, `Register` `0x5041_0307` (#349), `JournalRequest` `0x5041_0308` (#210), `View` `0x5041_0309` (#399), `Resolve` `0x5041_030A` (#216), `Load` `0x5041_030B` (#424); `0x5041_0303`, the old `Init`, is retired; retired ids never reused).
- `machine/mod.rs` → `MachineFacts`, `CellPlan`, `Class`, `ControlJournals` (the cell's, the election journal, the fleet's, one type for driver and client, #243, #240) → a machine's facts (no peer: a machine is configured with none, #277) and its cell's plan, the value of `cell init`'s decree (every identifier drawn by the machine that drives it).
- `machine/lifecycle.rs` → `run_machine`, `MachineSettings`, `MachineAddresses`, `MachineError` → the whole machine lifecycle `parosd` and the simulation run (#246): format (mint `node_id`), the amnesia and class checks, wait (an acceptor of any `cell init` that lists it), serve the plan and keep answering the decree; it binds the listen address and advertises the other (`MachineAddresses`, #257: a wildcard listen address with no advertised one is refused), and dials through the caller's `Names`; the disk is a bare `ProviderDisk` over the caller's provider and the audit port is the caller's (`AuditScope`, #294); a late boot is an inline `buggify_range!`; a failed record read or write is `MachineError::Storage`, a restart, never a refusal.
- `machine/wait.rs` → `wait_for_cell`, `CellLedger` → the idle machine (#196, #216, #277): `Identify`; `PrepareCell` and `FormCell` as an acceptor of the cell decree (accepting is forming: `format`, then the vote; a `hint!` after the durable promise and one between the format and the vote, #246, #294); `CellInit` as its proposer over the founding members, every one an acceptor: all answer Phase 1, a majority chooses (#246: a wiped member stays a dead member of the plan; `cell_lost` once a majority is wiped) (adopt a reported plan, else draw one; form the others, then itself; a vote for another list's plan is another cell's, never adopted, #216).
- `machine/admitted.rs` → `AdmittedMachine` → a machine `cell add-machine` admitted (#216): serves `Identify` and a node-only `Inspect` with its cell, acks `Admit` into its own cell, refuses the cell decree (`in_cell`, so `cell init` is `cell_exists`); no journal until placement (#212). `Admission` (in `machine/mod.rs`) is what `Admit` carries and the record keeps (`admitted`, `control`, `fleet`, `peer` lines).
- `machine/coordinator.rs` → `serve_term`, `TermDuty`, `Watch`, `election_tunables` → the cell coordinator (#240): every founding member campaigns in a task beside its node loop (seed and BUGGIFY decisions drawn on the loop: a stalled leader, a hand-off); a won term installs its uuid on the cell control journal, folds it, finishes in-flight admissions, then publishes the interface; a lost install resigns. For the rest of a served term its `Watch` (#211, D6) `Identify`s every founding member and registered machine at each renewal period and writes changes only: a new incarnation of a registered machine registers again, `MachineUp` for one held down or seen as another incarnation, `MachineDown` after `machine_down_after` of silence; the term's first write that does not land ends the watch (never fight an admin session's fence). Every founding member serves `Register` (#349): the term's coordinator `Identify`s the machine at the address it asks for and writes `RegisterNode` when the address book holds another; a member that serves no term refuses `not_coordinator`. A served term answers `JournalRequest` as the tenant coordinator (`machine/tenants.rs`, #210); no term answers `not_coordinator`. `Audit::call_observer` hands a harness the coordinator's calls.
- `machine/cache.rs` → `CachedRegistry`, `CacheSink`, `spawn_writer`, `starting_book` → the durable cached registry fold (#211): the cell's address book at one registry position, in the file `registry` beside the record; written forward only by a task beside the machine; read at every start, so a machine dials moved machines where the cache says (a hint: a wrong address costs liveness only).
- `machine/follow.rs` → an admitted machine folds the registry through the machines it knows each renewal period and offers each new book to its cache (#211).
- `machine/resolve.rs` → `Resolver` → `Resolve` (#216, §3.5): every machine of a cell, founding or admitted, answers "which references serve tenant T" in a task of its own, from folds of the universe directory (the tenant's id, control journal and cell, with `universe_id`; `tenant_in` judges the name) and the registry (the cell's machines, `cell_book`), kept between requests and read through the cell's machines like a client; each answer bounded by `ANSWER_WITHIN`; one BUGGIFY decision drawn at spawn answers every request from fresh folds.
- `machine/book.rs` → `address_book`, `cell_book` → the cell's address book (#349): the founding members at the address the registry holds for them, else the plan's; `cell_book` adds every registered machine not retired.
- `machine/register.rs` → the machine-to-coordinator request path (#349): on every start a formed or admitted machine reads the election journal for the coordinator's published interface and sends it `Register` with the address it advertises now, again each renewal period until the registry holds it. Its receiver ends a moved founding member's hold: the node loop beats no journal until then (#390).
- `driver/book.rs` → `PeerBook` → a founding member folds its own copy of the cell control journal after each tick, from its cached registry fold's addresses on (no lane moves below the cache's position, and each later book is offered to the cache, #211); a peer the address book moved is dialed at its new address from its lane's next batch on (`PeerMailbox::readdress`, #349).
- `machine/formed.rs` → `FormedCell` → a formed machine answers the decree (`PrepareCell`, `FormCell`, `Identify`) from its record while it serves its cell (#277).
- `machine/record.rs` → `MachineRecord`, `journal_config` → the machine record's text (identity, class, capacity, failure domain; the decree's acceptor state: `promised <round>/<node>` and the vote `plan <cell_id> <round>/<node>`, written after the stores, the commit point) and a plan journal's `Config`.
- `machine/disk.rs` → `ProviderDisk` → the record's atomic rewrite (staged, synced, renamed, every directory on the way synced), the amnesia probe and the formation's stores over any `StorageProvider`: `parosd` and the simulation both hand it to `run_machine` bare (#246, #294).
- `machine/stores.rs` → `AuditScope`, `MachineStores` → a formed machine's `JournalStorage` per plan journal, an existing member's; a journal a tenant created is provisioned at its first open (`provision_store`, then the record's `created` line, the commit point), then boots as an existing member (#210).
- `machine/tenants.rs` → `TenantDesk` → the tenant coordinator for one term (#210): claims each tenant control journal under the term uuid, catches the cell's fold up at every request so a dropped tenant is refused by every term (#395), writes `Describe` once, answers a request from its recorded outcome or writes it (id drawn from the candidacy's seed; a reused id on a BUGGIFY decision of the node loop), placement on the founding members.
- `rpc/inspect.rs` → `InspectTarget`, `InspectRefusal` → what an `Inspect` asks for: a named journal or the node alone; an unset identifier is refused (#243).
- `rpc/inbound.rs` → `Inbound`, `ReplySender`, `serve_deliveries`, `rpc_config`, `MAX_FRAME_BYTES` → a `Deliver` batch whose `cell_id` is not the receiver's is refused whole (`EdgeRejection::ForeignCell`, #216); the sender stamps its cell on every batch (`driver/transport.rs`, `LaneOpener::cell_id`; `0` without a cell plan).
- `rpc/client.rs` → `NodeClient` (one at-most-once attempt per call; `named` resolves its `Address` per call, #257), `MatchmakerClient`.
- `rpc/codec.rs` → shared scalar codecs (ballot, party, quorum system, config, command).
- `rpc/consensus.rs` → `Message` ↔ protobuf · `rpc/matchmaker_codec.rs` → matchmaker wire ↔ protobuf.
- `rpc/tests.rs` → round-trip tests; malformed input refused.
- `client/mod.rs` → `Client`, `ClientTunables`, `Retarget`, `LeaderHint` → policy loops: `write`, `resolve`, `read_any`, `journal_state`, `claim`, `set_leader`, `truncate`, `reconfigure*`, `inspect`, `retire`; `*_attempt` one-shot calls.
- `client/outcome.rs` → `WriteOutcome`, `ReadOutcome`, … → every reply judged once.
- `client/writer.rs` → `Writer`, `leader_uuid` (a uuid per term derived from the caller's random seed, #241; `with_uuid` for an operator-named one; `truncate` carries the leader's fence, #228; `stale_entry` and `stale_truncate_request` are the explicit misbehaviours) · `client/reader.rs` → `Reader`, `ReaderOutcome::Gap`.
- `client/multi.rs` → `append_entry`, `append_request`, `open_truncate_request` → the unfenced calls of a multi-writer journal (#241): the unset leader uuid and `seq` 0; the library never re-sends one on its own.
- `client/observer.rs` → `CallObserver`, `NoObserver` · `client/tests.rs` → the pure parts pinned.
- `client/checkpoint.rs` → `Checkpointable`, `Folder`, `Checkpointer`, `CheckpointRecord` (`MAGIC`, `Begin` / `Chunk` / `End`), `run`, `load` → checkpoint and truncate for any journal owner (#230): a checkpoint is a run of small records committed by its `End`, written in batches a node's `TooLarge` splits; a run with no valid `End` is never restored (#353); `Folder` is also the registry follower's fold.
- `client/bootstrap.rs` → `cell_init`, `InitOutcome`, `discover`, `majority_cell` (the cell a majority of a list serve, #216), `control_journals`, `control_journals_of`, `cell_members`, `identify`, `admit` → `parosctl init`'s calls: `cell_init(providers, rpc, target, members, patience)` sends `CellInit` to one founding member (#277); server ids and the control journals learned from a node-only `Inspect` (#196, §3.8, #243).
- `client/election/{mod,fold}.rs` → `Election`, `ElectionTunables`, `Step`, `ElectionFold`, `ElectionRecord`, `Candidate`, `Leader`, `hand_off`, `read_election` → the election library over a multi-writer journal (#240): campaign, renew, watch, resign; a lease on the watcher's own clock as a liveness hint only; the leader truncates to its own latest renewal; the caller hands in the jitter and the seed.
- `client/cell.rs` → `CellSession` (`with_leader`: an elected coordinator's term uuid, #240) → `cell add-machine` (#216) as an idempotent state machine: `Identify` the machine, `RegisterNode` in the cell control journal unless held, then `Admit` with the cell and the machines the session knows; reuses the fleet operations' `Step`, `Stage`, `Run`.
- `client/initialize.rs` → `initialize`, `InitRun`, `Initialized`, `InitRefusal`, `Unreachable`, `InitParams` → `init` whole (`cell init` at the first listed member still idle, retrying `member_unreachable` and `contended` within its patience; a wait for the elected cell coordinator, #240; the fleet steps) as one resumable operation over the founding members, typed; `parosctl init` prints it and the simulation runs it (#246).
- `view.rs` → `Scope`, `authorize`, `CellFacts`, `cell_view`, `tenant_view`, `universe_view`, `within_tenant_scope` → the administrative views (#399): pure answers from one cell's folds, filtered by the caller's scope (an admin sees every detail; a tenant scope sees only its own tenant and, of each machine it uses, the name, failure domain and up state). `authorize` is the Authz seam until #192.
- `machine/views.rs` → every founding member serves `View` (#399): folds the registry and the universe directory to their tails (a lagging member answers once from its fold, an inline `buggify_with_prob!`), reads the election journal and the tenant control journals, then answers through `crate::view`.
- `client/views.rs` → `ViewOutcome`, `request`, `ask` → a view sent to the servers of one cell in turn, until one answers; `unavailable` passes it on.
- `load.rs` → `Busyness`, `DiskBusyness` → how busy a machine and its disk were over one window (#424): pure arithmetic over two moonpool `SystemSample`s, FDB's ratios; a counter that went back gives no window.
- `machine/load.rs` → `LoadBoard`, `Window`, `answer` → a machine samples its own counters every `load_interval` in one task per run, keeps its last full window, and every phase serves `Load` from it (#424); one BUGGIFY decision, drawn in `run_machine`, skips the first window.
- `client/load.rs` → `LoadOutcome`, `ask_all` → `Load` to every machine of a cell view at once, each within a timeout; `Silent` is `no metrics` (#424).
- `name.rs` → `JournalName`, `Abbreviations`, `match_prefix` → names at the edge (#239): `paros://<tenant>/<journal>` (and `<tenant>/<journal>`) parsed and printed; ids printed as short hex, git-style (at least 6 digits, widened until the listing is unambiguous); a unique hex prefix matched among a listing's ids. Pure, wasm-safe.
- `client/resolve.rs` → `resolve`, `Resolution`, `Resolved`, `from_wire` → `Resolve` at the entry endpoint (#216): each address in turn, a machine that does not answer or answers `unavailable` passed over; `parosctl resolve <tenant>` prints it.
- `client/names.rs` → `resolve`, `resolve_tenant`, `resolve_journal`, `JournalNames` → name resolution (#239, §3.5): the tenant name through the universe directory (only a `READY` `users` tenant; an internal one never), then the journal name through the tenant's control journal (`Directory`, only a live journal); `JournalNames` caches each resolution and drops it when a call refuses its journal as unknown (`stale`).
- `fleet.rs` → `FleetEntry`, `FleetCommand`, `FleetDirectory` (`label`: a tenant's display label, its name or `universe` / `cell`, #239), `FleetEvent`, `FleetDirectoryRefusal`, `Group`, `Groups` (a tenant's set of groups; only `cell` forbids a move), `CellState`, `TenantState` → the
  fleet tenant's pure fold, the fleet directory (#229): the fleet, cell and tenant entries, every entry fenced by its
  fleet id and metadata version, ids checked at apply, `Checkpointable`.
- `client/fleet.rs` → `FleetSession`, `Step`, `Stage`, `FleetRefusal`, `read_directory` → `init`'s fleet
  steps and tenant create/remove as idempotent state machines over the fleet tenant and the cell control
  journal, one write per step, resumed from what the journals hold (#229).
- `tenant.rs` → `TenantCommand`, `TenantControl`, `TenantEvent`, `Desired`, `Survives`, `CellKind` → the pure fold of a tenant's control journal (#210): `Describe` once, `CreateJournal` / `DeleteJournal` per request id, `IdTaken` records nothing, `Checkpointable`.
- `client/journals.rs` → `JournalRequest`, `JournalAnswer`, `request`, `coordinator`, `list` → a tenant's journals (#210): the coordinator found from the election journal's published interface, one request id re-sent until decided; `list` folds the control journal.
- `system.rs` → `SystemCommand`, `Registry`, `HostedTenant`, `Role`, `BookingTarget`, `Liveness` → the pure fold of the registry (the cell tenant's control journal, #235); `HostTenant` records the tenant's control journal, name and `survives` (#210); the registry is keyed by `node_id` with class, capacity, the RPC incarnation (with the address, the machine's `InterfaceRef` identity) and bookings keyed by role of a journal or matchmaker set (#211: one live booking per node, target and role; a booking id is never booked twice, `spent` kept across checkpoints); liveness `MachineDown`/`MachineUp`, changes only (`LivenessUnchanged`, `StaleIncarnation`); `Checkpointable` (#230); the cell's side of the fleet (`JoinFleet`, `HostTenant`,
  `DropTenant`, #229).
- `corruption.rs` → `IntegrityFault`, `CorruptionVerdict` → the typed corruption verdict a store surfaces (the journal classifies).
- `journal/mod.rs` → `JournalStoreConfig` (geometry, direct I/O, `Durability`), the identity and error mappings · `journal/node.rs` → `JournalStorage` (slot = position, ballot in the identity, scalars in the metainfo; a sync commits a raised promise, then the entries with the floor and metainfo: the journal writes metainfo only after its batch, moonpool#309, #264; a `hint!` after the promise commit and after each packed entry batch, #294), `JournalBootFacts` · `journal/matchmaker.rs` → `JournalMatchmakerStorage` (a registration per position, its generation in the identity, scalars in the metainfo; a sync commits the registrations with the metainfo, then the clears, with a `hint!` between, #294; a boot keeps what the metainfo vouches for and raises the effective scalar over the reconfigurations it keeps, #176).
- `journal/settle.rs` → `settle` → a store's files and every directory on the way to it synced as they stand, no byte written (#348): a caller settles before it trusts a format marker a failed sync may have left staged (the sim's provisioning probes; `ProviderDisk::format` and `MachineStores::provision` on `Provisioned::Resumed`).
- `journal/tests.rs` → both contract suites, faulty votes from targeted damage, the floor across reboots, the format probe, lost segments, registration damage, on `SimStorageProvider` (the journal's crash physics are `moonpool-journal`'s tests and the sim's).
- `proto/{common,paros,internal,matchmaker,system,machine,checkpoint,election,fleet,tenant,view}.proto` → compiled by `build.rs` with `prost-build`.

## Public surface

`run_node`, `run_journals`, `run_matchmaker`, `run_proxy`, `run_replica`; `provision_store`,
`provision_matchmaker_store`; `paros::client`;
`paros::system`; `paros::tenant`; `paros::fleet`; `paros::journal`; `paros::machine`; `paros::wire::{methods,
checkpoint, common, fleet, public, internal, matchmaker, system, machine, view}`; `paros::view`; the RPC request/ack types (`lib.rs`).

## Local rules

- **Faults are inline, in this crate** (root *Simulation rules*, #294). A new driver decision is
  an inline `buggify_with_prob!` / `buggify_pick!` at the line that makes it, consulted only where
  its answer is observable. A moment where a crash is interesting (staged not synced, durable not
  sent) is `hint!("label").await` at that line. Never add a hook trait (the deleted `DriverHooks`, `ClientHooks`, commit hooks), a `Seam`
  variant, or a store or disk trait whose only second
  implementation would be a sim wrapper. A path the code walks gets a `reachable!` probe, with
  the message the sim's gate already uses, never reworded.
- A new tunable is a `DriverTunables` field with a default (`driver/config.rs`) and a
  `buggify_knob!` in `paros-sim`'s `NodeShape`; a new durability boundary is an inline
  `hint!("label").await` with a `reachable!` on `Strike::Killed`.
- Every call is one at-most-once attempt (`try_get_reply`), never `get_reply`.
- `paros::client` draws no randomness: every choice is the caller's.
- Storage implementations pass both contract suites.
- Spans are non-optional (`#[tracing::instrument(skip_all, fields(..))]`); see root *Tracing spans*,
  *Audit doctrine*.

## Tests & gates

- `cargo nextest run -p paros` — `rpc/tests.rs`, `client/tests.rs`, `journal/tests.rs`, inline tests.
- `cargo check --target wasm32-unknown-unknown -p paros` (CI `portability`).
- Building needs `protoc` (`build.rs` runs `prost-build`): the flake ships `protobuf` (`flake.nix:46`);
  on the web see root *Environment & Nix* (`PROTOC`).

## Deps & pins (`Cargo.toml`)

- `paros-core` with `tracing` + `serde` (`:17`); `prost` (`:34`), `postcard` (`:37`), `serde`
  (`:38`), `crc32c` (`:39`), `tracing` (`:42`), `tokio` `sync` (`:45`), `tokio-util` (`:48`).
- moonpool, rev `0b6ca9a` (moonpool#323 (system provider): `SystemProvider` and the simulated disk's counters, after moonpool#318's `buggify_named!` and `set_activation`): `moonpool-core` (`select`, `:26`), `moonpool-rpc` (`prost`, `:28`),
  `moonpool-journal` (`:32`), `moonpool-buggify` (`:36`), `moonpool-assertions` (`:37`), dev `moonpool-sim` (`:61`).
- Dev: `futures` executor (`:53`), `tokio` `rt`+`macros` (`:57`). Build: `prost-build` (`:60`).
- The pin is **eleven lines**: six here, `crates/paros-core/Cargo.toml` (`moonpool-assertions`),
  `crates/paros-sim/Cargo.toml:20,29`, `crates/parosd/Cargo.toml:22,24` — advance every line
  together.
