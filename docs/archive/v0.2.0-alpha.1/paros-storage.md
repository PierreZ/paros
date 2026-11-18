# paros-storage

The durable store of an acceptor and of a matchmaker: an async write seam over the core's read-only recovery port, an in-memory store, two stores on `moonpool-journal`, a typed fault model and two contract suites. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-storage/` (6,789 lines of Rust).

## What it did

The crate sat just above `paros-core` and `paros-model` in the library layers. `paros-roles` (the node and matchmaker drivers), `paros-machine`, `paros-coordinator`, `paros-observe` and the `paros` facade (`paros::storage`) depended on it. `paros-sim` ran its stores on moonpool's simulated disk. `parosd` ran them on the real file system.

The core reads durable state once, at construction, through `paros_core::Storage` (an acceptor) or `paros_core::matchmaker::RegistryStorage` (a matchmaker). The driver owns every write. It applies each `WriteOp` of a `Ready` through `LogStorage` (or `MatchmakerStorage`), then calls `sync` before any message leaves (persist before send). Every write and the boot scan are async and return `Result<_, StorageError>`, so a fault is injectable at every call. The read ports stay sync and answer from memory that `boot_scan` loaded.

A store keeps state, not a log of operations. Slot `s` is the journal position `s`. The entry identity holds the ballot and a kind byte. The node scalars (promise, format marker and its `Config`, chosen index, sealed `JournalState`, registration fence, retire mark) live in the journal's two-copy metainfo. A boot reads the state back: no fold, no checkpoint.

Main flows:

- **Format and boot.** `provision_store` / `provision_matchmaker_store` write the format marker once. The driver compares the operator's `BootKind` claim with the marker and refuses with a `BootRefusal` (`Amnesia`, `AlreadyFormatted`, `ConfigMismatch`). `check_promise` refuses a store that holds a ballot above its promise (a rolled-back metainfo).
- **Sync.** `JournalStorage::sync` makes up to three commits, each durable before the next: a raised promise alone, then the entries, then the floor and metainfo. The journal writes the metainfo of a commit only after its batch is durable (moonpool#309), so a metainfo vouches for its batch, never the reverse.
- **Matchmaker sync.** `JournalMatchmakerStorage` commits the boot's own drops, then the new registrations with the metainfo, then the clears. The scalars are paged in chunks of `meta_chunk` bytes at a fresh epoch per write (#439 (paged scalars)).
- **Scrub and idle witness.** `LogStorage::scrub` reads records back from the device and marks a damaged slot for rewrite (#362 (background scrubbing)). `LogStorage::idle_beat` writes the metainfo alone after quiet beats, so the last batch has a witness (#991 (metainfo write on idle)).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-storage/src/lib.rs` | `LogStorage` (the write seam), `StorageError`, `StorageRecord`, `WriteOutcome`, `MetadataFault`, `ScrubbedSlot`, `ScrubPage`, `command_bytes`. The `boot_scan` doc states the full CLStore record contract. |
| `crates/lib/paros-storage/src/contract.rs` | `storage_contract_suite(fresh, reopen)`: one behavioral walk every `LogStorage` must pass; `scrub_walk`. |
| `crates/lib/paros-storage/src/matchmaker.rs` | `MatchmakerStorage`, `MemMatchmakerStorage`, `Registrations`, `matchmaker_storage_contract_suite`. |
| `crates/lib/paros-storage/src/mem.rs` | `MemStorage`: scalars and per-slot log kept apart, never one blob. |
| `crates/lib/paros-storage/src/boot.rs` | `BootKind`, `BootRefusal` (with `label`), `check_promise`, `RunError` (`Storage`, `Refused`, `Infra`). |
| `crates/lib/paros-storage/src/provision.rs` | `provision_store`, `provision_matchmaker_store`, `Provisioned` (`Formatted` / `Resumed`). |
| `crates/lib/paros-storage/src/journal.rs` | `JournalStoreConfig` (`geometry`, `direct_io`, `meta_chunk`, `store_bytes`), `store_id`, `ballot_id` / `id_ballot`, `encode` / `decode` with `FORMAT_VERSION`, `open_error`, `commit_error`, `is_storage_full`. |
| `crates/lib/paros-storage/src/journal/node.rs` | `JournalStorage`, `NodeMeta`, `JournalBootFacts`, `CHOSEN_INDEX_LAG` (64), `SYNCS_PER_COMMIT` (2), `peek_formatted`. |
| `crates/lib/paros-storage/src/journal/matchmaker.rs` | `JournalMatchmakerStorage`, `matchmaker_shard`; the long module doc explains every crash point of a registry sync. |
| `crates/lib/paros-storage/src/journal/settle.rs` | `settle`, `drop_caches`, `sync_names` (#348 (settle), #720 (fsyncgate)). |
| `crates/lib/paros-storage/src/journal/tests.rs` and `journal/tests/{cost,errors,scrub,settle,witness}.rs` | The journal stores on `moonpool-sim`'s `SimStorageProvider`: contract suites, targeted damage, crash points, verdict tables. |
| `crates/lib/paros-storage/src/tests.rs` | Error texts, records and causes; `command_bytes`. |

## Ideas worth reusing

- **A typed fault model, data not strings** (`src/lib.rs`). `StorageError` names the fault kind as the variant and carries the `StorageRecord` and `WriteOutcome` as data. `WriteOutcome::Unknown` is the fsyncgate hook: a failed write may still be durable, so the only safe answer is fail-stop and boot from the disk. `Full` is a known-absent write that makes the store read-only, not a crash. Copy this whole enum shape.
- **The CLStore record contract** written on `LogStorage::boot_scan` (`src/lib.rs`, and `docs/analysis/storage/clstore-record-contract.md`, which is kept). Checksum every record, identity inside the checked region, absence detectable, two copies of the hard state, never truncate on a corruption verdict.
- **Faulty, not empty** (`journal/node.rs`, `Storage::faulty_entries`). A damaged entry whose identity survived is reported as `faulty(slot, ballot)`. It is never "nothing accepted here". This is CTRL's central rule.
- **One verdict table for every journal error** (`journal.rs`, `open_error` and `commit_error`). Each `OpenError` and `CommitError` arm maps to one `StorageError`, and `journal/tests/errors.rs` pins each arm. A new journal needs the same total table.
- **Commit order across commits** (`journal/node.rs` `sync`, `journal/matchmaker.rs`). Put in one commit a batch and the metainfo that depends on it, never a batch that depends on the metainfo. A promise goes alone before the entries it covers.
- **Contract suites with `fresh` and `reopen` closures** (`contract.rs`, `matchmaker.rs`). Every read-back goes through a reopen, so the in-memory and durable stores cannot drift. Cheap and strong: keep it.
- **Pair asserts on every encoder** (`journal.rs`: `encode` checks `decode`, `ballot_id` checks `id_ballot`, `store_id` checks its halves). A version byte (`FORMAT_VERSION`) makes an old store refuse to boot instead of misread.
- **Idempotent re-accept** (`journal/node.rs`, `append_accepted`). A held `(ballot, command)` writes nothing. A scrub-damaged slot is written again.
- **Chosen index rides other writes** (`CHOSEN_INDEX_LAG` in `journal/node.rs`). It forces its own write only after 64 slots of lag, so a reboot recovers about one page (#602 (dark after a full restart)).
- **Crash points made likely with `hint!`** (`journal/node.rs` lines near 498 and 802, `journal/matchmaker.rs` near 863, 895, 937). Each split between commits is a named BUGGIFY hint, so the simulation hits the crash between two commits often.
- **`store_bytes` pinned by a test** (`journal.rs`, `journal/tests/cost.rs`): placement counts store disk with the same number the files show (#985 (placement counts store disk)).
- **`drop_caches` before every open** (`journal/settle.rs`): after a failed writeback the page cache lies, so the open reads the device.

Leaks and mistakes not to copy:

- **Upward knowledge in a low layer.** `src/boot.rs` documents `paros_roles::node::run_node`, and `RunError` (a driver exit) lives here and wraps moonpool's `SimulationError` as `Infra`. A driver exit type belongs to the driver layer, not to storage.
- **Unused dependencies.** `Cargo.toml` lists `paros-wire` and `crc32c`, but no file in `src/` uses them.
- **Two parallel traits with copy-paste contracts.** `LogStorage` and `MatchmakerStorage` repeat the format marker, boot scan and sync shape. The matchmaker store grew paged scalars, epochs and a cross-journal drop floor (#488) on top: the most complex file of the crate. A rebuild should find one store shape for both.
- **Fence and retire scalars** (`persist_registration_fence`, `persist_retire_mark`, #1002 (retire fence gaps)) are part of the retire fence stack that `docs/architecture` rule 1 (seal the latest configuration) deletes. Do not carry them into the new seam.
- **Issue history in doc comments.** Most docs tell why a line changed, with witness seeds. Useful for this recap, heavy for a new reader.

## Bugs and scenarios to cover again

- **Amnesia.** An existing member whose store has no format marker must refuse to boot (`BootRefusal::Amnesia`, #147). A first boot on a formatted store refuses too. A store formatted under another `Config` refuses (`ConfigMismatch`, #207). Tests: `provision.rs` (`a_store_formatted_under_another_configuration_is_refused`), contract suite start.
- **Protocol writes never format a store.** Contract suite, first block in `contract.rs`.
- **Interrupted provisioning resumes from the disk** (#208): `a_store_is_formatted_once_and_an_interrupted_run_resumes`.
- **Promise before entries.** An entry durable above its promise is an accept the node never promised. Hint "promise durable, entries staged".
- **Entries before metainfo.** `a_split_sync_commits_its_entries_before_its_metainfo`, `a_node_killed_between_its_entries_and_its_metainfo_boots_on_the_old_metainfo`.
- **Rolled-back promise** (#992): two lost metainfo writes put an older promise back. `check_promise` in `boot.rs` and `a_rolled_back_promise_is_a_corruption_verdict` in `journal/tests/witness.rs`.
- **Unwitnessed last batch** (#991, moonpool #411): two faults on the last batch read as a torn tail and forget an acknowledged vote. `two_faults_on_an_unwitnessed_last_batch_read_as_torn` shows the hole, `two_faults_on_a_witnessed_last_batch_refuse_the_open` and `an_idle_store_writes_its_metainfo_once_after_the_bound` close it.
- **Failed sync is ambiguous; the reopen decides** (fsyncgate, #720): `a_failed_sync_is_ambiguous_and_the_reopen_decides`. Only a failed segment roll is known lost.
- **Damaged accepted entry is faulty, mid-log and at the tail**: `a_damaged_accepted_entry_is_reported_faulty_mid_log_and_at_the_tail`.
- **Lost segment is a crash verdict**: `a_lost_segment_is_a_crash_verdict`.
- **Damaged live registration is a crash; one below the watermark is dropped**: `a_damaged_registration_is_a_crash_and_a_collected_one_is_not`, `a_registration_below_the_watermark_is_collected_even_when_damaged`.
- **A boot drop stays dropped** (#488 (matchmaker boot drop), witness seed 4532891387820985116, PR #478): a dropped registration came back after a later sync named a new journal. Oracle: "a restart never invents or alters a registration".
- **Re-sent accepts flood the disk** (witness seed 5953348164786240469: 1,073 re-sends of one slot): `a_held_entry_re_accepted_writes_nothing`.
- **Full restart goes dark** (#602): `a_lagging_chosen_index_is_written_alone`; sim oracle "a reboot rewinds the chosen index by less than a page".
- **A sync larger than one segment**: `a_sync_larger_than_one_segment_lands_in_several_commits`.
- **Scalars larger than the metainfo** (#439): `scalars_larger_than_the_metainfo_survive_a_reboot_in_chunks`, `a_damaged_scalar_chunk_is_a_crash_verdict`.
- **Scrub** (#362, #992): `a_rotted_record_reads_back_damaged_and_is_written_again`, `a_faulty_vote_reads_back_damaged`, `a_staged_slot_is_not_read`, `a_scrub_finds_a_rotted_persist_record`.
- **Disk full** (#360): `a_full_disk_is_known_by_its_kind_or_its_code` (the kind natively, the code 28 on wasm32).
- **Settle writes no byte** (#348): `settle_writes_no_byte_and_the_store_opens_the_same`.
- **Floor and sealed state survive a reboot** (#186, #204): `laggy_slots_and_the_floor_survive_a_reboot`; trim-point jump block in `contract.rs`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-storage/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-storage

Where to start reading: `src/lib.rs` (the seam and the fault model), `src/contract.rs`, `src/journal.rs` (module doc and verdict tables), `src/journal/node.rs` (`sync`, `load`), then `src/journal/matchmaker.rs` (module doc). Its own map at the tag: `crates/lib/paros-storage/AGENTS.md`.
