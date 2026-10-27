# paros-storage

The `LogStorage` seam (`paros-core`'s read-only `Storage` port plus the async write extension), the in-memory store, the durable `moonpool-journal` stores, provisioning and boot markers.

A layer of the library (#722 (three products)): it depends only on the layers below it (root *Layout*). Programs use it through the facade, `paros::storage`.

## Map

- `provision.rs` → `provision_store`, `provision_matchmaker_store`, `Provisioned` → format a store ahead of its first start; an interrupted run resumes from the disk (#208).
- `lib.rs` → `LogStorage`, `StorageError`, `StorageRecord`, `WriteOutcome` → the async seam.
- `mem.rs` → `MemStorage` · `contract.rs` → `storage_contract_suite`.
- `journal.rs` → `JournalStoreConfig` (geometry, direct I/O, `Durability`), the identity and error mappings · `journal/node.rs` → `JournalStorage` (slot = position, ballot in the identity, scalars in the metainfo; a sync commits a raised promise, then the entries with the floor and metainfo: the journal writes metainfo only after its batch, moonpool#309, #264; a `hint!` after the promise commit and after each packed entry batch, #294), `JournalBootFacts` · `journal/matchmaker.rs` → `JournalMatchmakerStorage` (a registration per position, its generation in the identity, scalars in chunks of `meta_chunk` bytes at a fresh epoch per write, the metainfo a header that names them, #439; a sync commits the boot's own drops first, then the registrations with the metainfo, then the clears, with a `hint!` between each, #294, #488 (matchmaker boot drop); the registrations land ahead of the metainfo on a BUGGIFY decision; a boot keeps what the metainfo vouches for, judges an undamaged registration by its own journal's watermark only, and raises the effective scalar over the reconfigurations it keeps, #176).
- `journal/settle.rs` → `settle` → a store's files and every directory on the way to it synced as they stand, no byte written (#348): a caller settles before it trusts a format marker a failed sync may have left staged (the sim's provisioning probes; `ProviderDisk::format` and `MachineStores::provision` on `Provisioned::Resumed`).
- `journal/tests.rs` → both contract suites, faulty votes from targeted damage, the floor across reboots, the format probe, lost segments, registration damage, on `SimStorageProvider` (the journal's crash physics are `moonpool-journal`'s tests and the sim's).

## Tests & gates

- `cargo nextest run -p paros-storage` — both contract suites (`contract.rs`) on every store, `journal/tests.rs`.
- A store passes both contract suites.
