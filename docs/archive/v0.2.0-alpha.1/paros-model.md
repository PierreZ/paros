# paros-model

The pure service model (`paros::model`): the registry fold, the tenant and fleet folds, names, tags, placement, footprints, balance, status, views, checkpoints, the authz port and the machine's data types. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-model/` (21,997 lines of Rust).

## What it did

It sat above `paros-core` and `paros-wire` and imported nothing else of paros. It had no I/O, no clock of its own and no randomness, and it built for wasm32. Almost every upper crate used it: `paros-net`, `paros-storage`, `paros-client`, `paros-observe`, `paros-roles`, `paros-coordinator`, `paros-machine`, the `paros` facade and `paros-sim`.

The crate is the one reading of paros's own control journals. Each control journal has a typed command (encoded with protobuf from `paros-wire`) and a pure fold over the chosen entries in position order. Every reader that folds the same prefix agrees. A malformed entry folds to a typed refusal, never a panic: entries are external input.

- **Registry** (`registry.rs`): the cell control journal (the cell tenant's control journal, #235 (the (TenantId, JournalId) frame)). Nodes keyed by `node_id`, their class, address and RPC incarnation, bookings by role, liveness, maintenance windows, balance state, control-plane mode, and the cell's side of the fleet (`JoinFleet`, `HostTenant`, `DropTenant`).
- **Tenant** (`tenant.rs`): a tenant's control journal (#210 (tenant control journal)): journal create, delete, configure, placed, scrubbed; each request applies once per request id.
- **Fleet directory** (`fleet.rs`): the universe directory of cells and tenants (#229 (meta tenant and fleet operations)), buckets (`fleet/buckets.rs`), root keys (`fleet/keys.rs`).
- **Pure planners**: `placement.rs` (#212 (placement)), `footprint.rs` (#476 (tenant footprint)), `balance.rs` (#687 (rebalance a cell)), `status.rs` (#191 (status)), `view.rs` (#399 (admin CLI views)), `load.rs` (#424 (busyness metrics)).
- **Checkpoint format and fold** (`checkpoint.rs`, #230 (checkpoint library), #353 (checkpoints in many slots)): `Checkpointable`, `Folder`, `CheckpointRecord`. The async owner loop was in `paros-client/src/checkpoint.rs`.
- **Machine data** (`machine.rs`, `machine/*.rs`): `CellPlan`, `Admission`, `Locality`, `MachineRecord` (a text file), the durable cached registry (`CachedRegistry`), `cell_book`, `Disk`.
- **Edge types**: `name.rs` (`paros://<tenant>/<journal>`, short hex ids), `tags.rs`, `address.rs`, `authz.rs` (the `Authz` port; Biscuit lived in `paros-authz-biscuit`), `corruption.rs` (`IntegrityFault`, `CorruptionVerdict`).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-model/src/registry.rs` | `Registry` (`new(initial)`, `pool`, `serves`, `stores`, `store_room`, `disk_bound`, `control_plane`, `universe`), `Liveness`, `HostedTenant`; the long module doc is the registry spec |
| `crates/lib/paros-model/src/registry/command.rs` | `SystemCommand` (one entry), `Grant`, `GrantKey` (`Footprint(tenant)` or `Request(id)`) |
| `crates/lib/paros-model/src/registry/event.rs` | `RegistryEvent`, `RegistryRefusal` (`LivenessUnchanged`, `StaleIncarnation`, `DiskFull`, `NoFootprint`, `NoBalanceMove`, …) |
| `crates/lib/paros-model/src/registry/records.rs` | `Role`, `BookingTarget`, `Booking`, `NodeStanding`, `MaintenanceScope`, `SlowFacts` |
| `crates/lib/paros-model/src/registry/checkpoint.rs`, `balancing.rs`, `control_plane.rs`, `universe.rs` | `impl Checkpointable for Registry`; `BalanceState`, `BalanceMove`; `ControlPlane`, `ControlPlaneMode`; `UniversePointer` |
| `crates/lib/paros-model/src/tenant.rs`, `tenant/scrub.rs` | `TenantCommand`, `TenantControl`, `TenantEvent`, `Desired`, `Durability`, `Survives`, `Layout`, `TenantRoles`; `ScrubProgress` |
| `crates/lib/paros-model/src/fleet.rs`, `fleet/buckets.rs`, `fleet/keys.rs` | `FleetDirectory`, `FleetCommand`, `FleetEvent`, `FleetDirectoryRefusal`, `CellState`, `TenantState`; `bucket_of` (CRC16-XMODEM mod 16384), `BucketTable`; `RootKeys` |
| `crates/lib/paros-model/src/checkpoint.rs` | `MAGIC`, `Checkpointable`, `CheckpointRecord` (`Begin`/`Chunk`/`End`), `run`, `chunks_checksum`, `Folded`, `Folder` (`fold`, `jump`, `is_whole`, `in_run`) |
| `crates/lib/paros-model/src/placement.rs` | `plan`, `Machine`, `Want`, `Step`, `Shortfall`, `zoned`, `zone_rule_binds`, `ZONE_DRAWS` = 8 |
| `crates/lib/paros-model/src/footprint.rs`, `balance.rs` | `Footprint`, `plan`, `fix`; `judge`, `Judgement`, `Balance`, `ABANDON_ROUNDS` |
| `crates/lib/paros-model/src/status.rs` | `Word` (Healthy, Degraded, Unavailable), `judge`, `observe`, `Hysteresis`, `summarize` |
| `crates/lib/paros-model/src/machine.rs`, `machine/record.rs`, `machine/cache.rs`, `machine/book.rs` | `CellPlan`, `Admission`, `Class`, `Locality`, `ControlJournals`; `MachineRecord`, `journal_config`; `CachedRegistry`, `CacheSink`, `starting_book`; `cell_book` |
| `crates/lib/paros-model/src/name.rs`, `tags.rs`, `authz.rs` | `JournalName`, `Abbreviations`, `match_prefix`; `Tag`, `Tags`, `UserTags`, `Selector`; `Authz`, `Request`, `Operation`, `Denial` |

## Ideas worth reusing

- **A control plane is a journal plus a pure fold.** `SystemCommand` + `Registry::fold`, `TenantCommand` + `TenantControl`, `FleetCommand` + `FleetDirectory`. Every node and every client that reads back its own request runs the same fold. Simple to test, simple to model in TLA+ as a deterministic function of the log.
- **Judged at apply, refused as data.** Every rule (class of a booking, one live booking per node/target/role, a booking id never booked twice, `DiskFull`, `NoFootprint`, a stale fleet id) is checked when the entry folds, and a refusal is a typed `RegistryRefusal`. Writers may race; the log decides.
- **Once per request id.** Configure, tags, control-plane mode and grants act once per request id; a repeat folds to `Unchanged`. A re-run step is harmless. Tests `a_request_applies_once`, `a_create_is_recorded_and_a_retry_acts_once`.
- **Changes only.** `MachineDown`/`MachineUp`/`MachineSlow` and space marks are refused when they change nothing (`liveness_entries_are_changes_only`). This keeps the log small and the fold's events meaningful.
- **Ids never reused.** A retired node id, a deleted journal id (tombstoned), a removed cell id, a root key id and label, a booking id (`Registry::spent`, kept across checkpoints).
- **Grants all or none** (`GrantBookings` under a `GrantKey`): the first grant under a key decides it. A clean way to book a footprint atomically through a log.
- **Seeded fold** (#685 (no founders)): `Registry::new(initial)` seeds the initial members as `storage` nodes with `registrations == 0`. No rule reads the seed after formation, and the checkpoint holds it (`a_checkpoint_holds_the_seed`).
- **Checkpoint as a run in the same journal** (`checkpoint.rs`): `Begin` covers every position below it; small `Chunk`s; `End` is the commit point with chunk count and CRC-32C. A run with no valid `End` is never restored. A fold that holds the whole prefix verifies a mid-log checkpoint against its own state and resets on it. `MAGIC` keeps run records apart from user entries. The rule "`checkpoint()` is a pure function of state, never of history" lets a restored fold and a whole fold compare equal byte for byte.
- **Pure, seeded placement** (`placement::plan`): ties broken by a draw seeded with the tenant id; same inputs give the same steps. A property test over many cells (`every_plan_keeps_the_rules`).
- **Status through the core's quorum predicates** (`status.rs`): this module counts nothing itself; every quorum question crosses `paros_core::membership::AcceptorConfig`. `Hysteresis` stops a word from flapping.
- **Durable cached registry** (`machine/cache.rs`, #211 (machine registry)): written forward only; a wrong address costs liveness only. A static-stability idea (like CockroachDB gossip).
- **Names at the edge** (`name.rs`): ids printed as git-style short hex, widened until unambiguous.

Mistakes not to copy:

- **The journal node folds this crate's registry** (leak 1): `paros-roles/src/node/system.rs` and `node/control.rs` read `paros_model::registry` to open journals. A new journal must get assignments through a port, not fold the service model.
- **The checkpoint state machine split across crates** (leak 14): `Checkpointable` and `Folder` here (`src/checkpoint.rs`), `Checkpointer` in `paros-client`, wired again by hand in `CellSession`, `TenantDesk` and `universe.rs`. Give the replicated-state-machine layer one home.
- **Bootstrap consensus state in a text file with no checksum** (leak 15): `MachineRecord` (`src/machine/record.rs`) keeps the cell decree's `promised` and vote lines.
- **Service ids leak down** (leak 5): `TenantId` and `JournalIdentifier` come from `paros-core`; the model then depends on that shape.
- **Too much in one crate**: storage (`corruption.rs`), auth (`authz.rs`), placement and machine files live beside the folds. `paros-net` imports `Address`, `Names` and `JournalName` from here (leak 11).

## Bugs and scenarios to cover again

- **Checkpoint crash points** (#353 (checkpoints in many slots)): a crash inside a run (no `End`), a crash between `End` and truncate (a checkpoint in the middle of the log), a reader racing a truncate. Tests in `src/checkpoint.rs`: `a_run_with_no_valid_end_is_never_restored`, `a_whole_fold_verifies_a_checkpoint_and_resets_on_a_wrong_one`, `a_gap_is_skipped_until_a_checkpoint_heals_it`, `an_unusable_checkpoint_changes_nothing`, `the_retired_forms_are_refused`.
- **Restore equals whole fold**: `a_registry_restored_from_its_checkpoint_is_the_registry_folded_whole`, `a_checkpoint_restores_the_whole_directory`, `a_checkpoint_restores_the_same_state` (tenant). Every new state machine needs this oracle.
- **A deleted journal must never start again**, even in a later incarnation whose fold has not reached the delete (witness `replay-recipe 10513847976889139006 1062642:1295322016819584231` on 69f7267; `deleted` lines in `MachineRecord`).
- **Amnesia**: a disk with stores but no record, or a record that names a store the disk lost, is refused (`src/machine/record.rs`; enforced in `paros-machine`).
- **A booking id is never booked twice, even after its release** (`a_booking_id_is_never_booked_twice_even_after_its_release`).
- **Disk room** (#985 (placement counts store disk)): a full disk refuses a user journal's store, never the control plane's (`a_full_disk_refuses_a_user_journal_never_the_control_plane`, `registry/tests/disk.rs`).
- **Placement books only machines that serve the cell** (#511 (placement books machines that never served)): `a_registered_machine_is_offered_only_once_it_serves`.
- **Zone rule** (#215 (zones)): `the_zone_rule_binds_over_three_zones_and_a_spread_plan_meets_it`; a plan that breaks it is redrawn up to `ZONE_DRAWS` times, then recorded `Unplaceable`.
- **A reboot keeps its class** and may take more roles, never fewer (`a_reboot_registers_again_under_the_same_class`); a stale incarnation is refused (`StaleIncarnation`).
- **Fleet fencing**: an entry naming another fleet is refused; only the current metadata version is read; a cell is removed only empty and `REMOVING`, and its id never comes back (`a_cell_is_removed_only_empty_and_removing_and_never_comes_back`).
- **Balance moves** (#687 (rebalance a cell)): one move at a time, judged at apply, ended by the release of its booking, abandoned after `ABANDON_ROUNDS` (`a_balance_move_is_judged_at_apply_and_ends_with_its_release`).
- Oracle reuse warning (leak 16): `paros-sim/src/audit/state.rs` called the core's quorum predicate and `audit/node.rs` decoded registry events with this crate's fold. A bug in a reused fold is judged green. The new judges should keep an independent reference fold.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-model/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-model

Where to start reading: `src/checkpoint.rs` (header, then `Folder::fold`), `src/registry.rs` (header), `src/registry/command.rs`, `src/registry/event.rs`, `src/tenant.rs`. Its own map at the tag: `crates/lib/paros-model/AGENTS.md`. The map had drifted: it names `MachineFacts` and `spawn_writer`, which do not exist at the tag, and it puts `Checkpointer`, `run` and `load` in `checkpoint.rs`, but `Checkpointer` and `load` were in `paros-client/src/checkpoint.rs`.
