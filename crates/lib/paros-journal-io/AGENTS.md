# paros-journal-io

The traits that a backend of the journal implements, the pure `Host`, the generic `Driver` and
the contract suites (#1329 (paros-journal-io), step 8 of `docs/journal/08-build-steps.md`). The
contract is `docs/journal/04-roles.md` §J4.2 (the five rules) and the layering rule is §J4.3.
Read them before you change the code, and change them in the same PR when a rule changes.

## Map

| Module | What it holds | Rule |
|---|---|---|
| `wire.rs` | `Address` (a journal and a node), `Envelope`, `Time` | rule 3 |
| `layout.rs` | the on-disk format: `Key` and its bytes, `Kind`, `Op`, `Entry`, the ops of each role's batch, the decode of a role's records with its `Damage` | rule 4 |
| `storage.rs`, `transport.rs`, `clock.rs`, `register.rs` | the four traits a backend implements | rules 2, 4 |
| `events.rs` | the `Events` port and `Event` | rule 5 |
| `host.rs` | the pure `Host`: `Assignment`, `Input` (with `Join` and `Abandon`, the new replica of a heal, #1314), `Effect`, `Stop`, `CallOutcome` | rules 1 to 3, 5 |
| `driver.rs` | the generic `Driver`: group commit, held sends and register changes, recovery reads, the timer, the events port | rule 1 |
| `mem.rs` | backends in memory: `MemStorage`, `MemNetwork` and its `Endpoint`, `ManualClock`, `MemRegister` | |
| `contract.rs` | the suites that check a backend: `storage`, `storage_reopen`, `transport`, `clock` | rule 4 |

Each module keeps its unit tests in `<module>/tests.rs`. The driver walk of
`crates/sim/journal-sim` (`src/host.rs`, `tests/host.rs`) judges the driver and the `Host`
under faults.

`examples/`: `in_memory` (a journal on three processes), `custom_storage` (a backend written
by hand, checked by the contract suite), `single_writer` (the pure client of
`paros_journal::client`: exactly-once across a lost reply and a seal, `Below` settled by
`Resolve`, a deposed writer) and `multi_writer` (the client with `SetLeader(None)`, open writes
from two writers, a resend that lands twice, and back to one writer). `common::Program` is the
glue a program writes around a client. Shared code is in `examples/common/mod.rs`. Each asserts
its result and runs in the `checks` job of CI.

## Rules

- **A pure crate** (§J4.3): it depends on workspace crates only, in `[dependencies]` and in
  `[dev-dependencies]` (`scripts/check-core-deps.sh`). No trait requires `Send`, an executor or
  a runtime type. An adapter that does I/O is a crate of its own (the moonpool adapter is #1417, in the layered parosd).
- **The traits are not stable yet**: they are called stable only once the moonpool adapter of #1417
  runs on them. A change that the adapter needs is expected.
- **The `Host` is pure**: no I/O, no clock, no randomness. Every input is recorded, and the
  time comes with it.
- **The format is this crate's**: a backend stores keys and bytes and never reads them. A
  change of a `Kind` number or of a value's bytes is a change of the disk format.
- **The flight rule is the driver's**: a send or a register change that names a batch waits
  until the batch is durable; a failed commit stops every role it held (rule 4).
- **No change to the core's contract**: `paros-consensus` and the roles of `paros-journal` keep
  their API (#1329). What this crate needs from a role is a new optional behaviour there.
- **The absolute rule of simulation** (root `AGENTS.md`): a red seed of the driver walk is a
  finding.

## Gates

`cargo nextest run -p paros-journal-io -p journal-sim`, `cargo run -p paros-journal-io --example
in_memory`, `--example custom_storage`, `--example single_writer` and `--example multi_writer`, `cargo test --doc -p paros-journal-io`,
`cargo clippy --all-targets -- -D warnings`,
`RUSTDOCFLAGS="-D warnings" cargo doc -p paros-journal-io --no-deps`,
`cargo check --target wasm32-unknown-unknown -p paros-journal-io`,
`scripts/check-core-deps.sh` and `scripts/mutants.sh --journal-io`, all in `nix develop`.
