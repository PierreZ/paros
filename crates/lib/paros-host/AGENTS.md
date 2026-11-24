# paros-host

The pure host of actors (#1410 (split the generic host of actors), step 1 of #1409 (build the
layered parosd)). One host serves every role of a process: later the roles of the core, the
journal, the election, the coordinators and the frontend. The crate knows none of them. The
contract is §J4.2 of `docs/journal/04-roles.md` at the git tag `journal-design-a` (the flight
rule and the four other rules; `docs/archive/journal-design-a/README.md`) and
`docs/architecture/04-roles.md` §4.4 ("A role is an actor"). Read them before you change the
code, and change them in the same PR when a rule changes.

## Map

| Module | What it holds | Rule |
|---|---|---|
| `time.rs` | `Time` | rule 2 |
| `family.rs` | `Family`: the types that every role of one host shares (an opaque address, a message, a batch id, an op, the records, an assignment, an input, an output, a stop) and the facts the driver needs (route a message, the durable batch of a scan, a scan with the group on top, which output waits, which stop keeps its batches) | rules 1 to 4 |
| `role.rs` | `Role` (start, recover, step, tick, stop, done) and `Start` | rule 1 |
| `host.rs` | `Host<R>`: the map of address to role, a start with a read first, routing, one deadline for the timer of each role, stop; `Input` and `Effect`; the blocks of a step for a family that routes some inputs itself | rules 1 to 3, 5 |
| `machine.rs` | `Machine`: what the driver drives; `Host<R>` is one | |
| `io.rs` | `Storage<F>` (commit, scan, list), `Transport<F>`, `Clock`, `Entropy` (and `NoDraws`), the typed `StorageError` (`Full`, `Lost`, `Unknown`) and its `Verdict`: the traits a backend implements | rules 2 to 4 |
| `ports.rs` | `Events` and `Event` (rule 5), `Ports` (the outputs of the roles) | rule 5 |
| `driver.rs` | `Driver`: group commit, held sends and outputs, recovery reads, the timer of each role, the draws, the events port, the verdict of each storage fault (a halt on `Unknown`), the list of the roles on the disk (`on_disk`) | rules 1, 4 |
| `toy.rs` | a toy family for the tests: a counter with a disk and a watcher with none | |

Each module keeps its unit tests in `<module>/tests.rs`. No crate of the workspace runs its
roles on this crate yet: the journal of design A did, and it was removed on 2026-11-24.

`examples/coordinator.rs`: a toy coordinator with a disk and two workers with none, on the host
and the driver, with a crash and a restart. It names no journal. CI runs it in the `checks` job.

## Rules

- **A pure crate** (§J4.3): it depends on no crate, in `[dependencies]` and in
  `[dev-dependencies]`. No trait requires `Send`, an executor or a runtime type. Layer 0 of
  `scripts/layers.sh`.
- **It knows no role.** No journal, no tenant, no coordinator name in the code. A fact that only
  one family needs is a method of `Family` or `Role` with a default, or it stays in the family's
  crate.
- **The host is pure**: no I/O, no clock, no randomness. Every input is recorded, and the time
  comes with it. Each role sets its own timer, and a fire goes to that role alone. A role asks a
  random value (`Effect::Draw`); the driver draws it from its `Entropy` and gives it back as
  `Input::Drawn` (#1413 (the time, the timers and the draws in each role),
  `specs/tla/recorded-inputs/`). The test `a_recorded_run_replays_to_the_same_effects` holds it.
- **The flight rule is the driver's**: a send or an output that names a batch waits until the
  batch is durable; a failed commit stops every role it held, or the process (rule 4).
- **One verdict for each storage fault** (#1414, `specs/tla/flight-rule/`): `Full` stops the
  roles of the call, which may start again; `Lost` stops them for good; `Unknown` halts the
  driver, and the program starts a new one on the disk. The driver reads `StorageError::verdict`,
  never the call.
- **The traits are not stable yet**: they are called stable once the moonpool adapter of #1417
  runs on them.

## Gates

`cargo nextest run -p paros-host`, `cargo run -p paros-host
--example coordinator`, `cargo clippy --all-targets -- -D warnings`,
`RUSTDOCFLAGS="-D warnings" cargo doc -p paros-host --no-deps`,
`scripts/check-layers.sh --wasm`, `scripts/check-core-deps.sh` and `scripts/mutants.sh --host`,
all in `nix develop`.
