# paros-journal-control

The grid controller of the journal v1 (#1328 (paros-journal-control), step 7 of
`docs/journal/08-build-steps.md`; `docs/journal/06-controller.md` §J6). **The controller decides
when and where; the protocol decides whether it is safe.** This crate holds the controller's pure
decision: no I/O, no clock, no randomness, no dependency but `paros-consensus` and
`paros-journal` (and `moonpool-assertions` behind `assertions`, for the probes only). It builds
for `wasm32-unknown-unknown`. The guards of the fenced reshape call are not here: they are the
register's (`paros_journal::register::value`, §J6.1).

## Map

| Module | What it holds | Rule |
|---|---|---|
| `measure.rs` | `Liveness`, `Spares`, `GridState` (the six v1 states), `column_spares`, `electable`, `writable`, `grid_state`: the P measures of a v1 grid, each asked through `membership/` | §J6.2 |
| `placement.rs` | `GridZones` and `grid_zones` (the zone rule of a grid is the rule of each column), `heal_grid` (each member that is down replaced by a candidate, by backtracking), `heal_replicas`, `heal_one_replica` (one member that is down replaced, the re-replication of a retired set, #1496) | §J6.2, "A zone loss" |
| `decide.rs` | `View`, `Tunables`, `Decision`, `Action`, `Wait`, `Alert`, `decide` and `PROBES`: the one decision, in the order of its module doc | §J6 |
| `role.rs` | `Controller`, `Standing`, `Ask`, `Outcome` and `PROBES`: the controller role, its election by a term and its fenced reshape call, as asks to a driver that runs each register operation; no state but the term, the call in flight and the heal of a retired segment in flight (#1314) | §J6.1 |
| `signals.rs` | `ProcessId`, `Report` (`Beat`, `Writes`), `Windows`, `Signals` and `PROBES`: what the processes of a journal publish to its controller, stamped with the controller's clock, and the liveness, `leader_known` and `signal_age` it reads from them | §J6.2, "The signals and the driver as built" |
| `driver.rs` | `ControlDriver`, its ports `ControlRegister` and `Cell`, `ControlConfig`, `Polled` and `PROBES`: the driver of one journal's controller (the campaign rule, the view, the register operations, the sends, the token) | §J6.2, "The signals and the driver as built" |
| `probe.rs` | the `probe!` shim, as in `paros-consensus` | §J6.2, test 6 |

Each module keeps its unit tests in `<module>/tests.rs`.

`examples/controller.rs` drives the role as a driver does, over a register written by hand (an
election, a heal, a lost reply, a takeover). `examples/driver.rs` runs two `ControlDriver`s over
ports written by hand (a campaign, a heal after a process goes silent, a takeover). Each asserts
its steps and runs in the `checks` job of CI (root `AGENTS.md`, "Composing crates have
examples"). A change to the role's or the driver's API updates them.

## Rules

- **`decide` is a pure function of its view.** Every number of §J6 that is **Open** (the stale
  age, the minimum interval, the stall window, the suspicion window, the cap of the rate limiter)
  comes from the host: `Tunables` has no default, the liveness and the token are in the view.
  Never set a number here.
- **No load, latency or error signal in the view** (v1, §J6): no plan can follow one.
- **A probe on every branch and every wait or refusal of `decide`**, with its message in
  `PROBES` (and every branch of the role, of the driver and of the signals, in `role::PROBES`,
  `driver::PROBES` and `signals::PROBES`). The coverage gates
  (`crates/sim/journal-sim/tests/control.rs`, `tests/controller.rs`, `tests/whole.rs`) require every one;
  never reword a message. A new branch gets its probe and its line in `PROBES` in the same PR.
- **A change of `decide` changes the reference** (`journal-sim/src/control/reference.rs`), which
  is written again from §J6.2 with no code of this crate, in the same PR, and the doc of §J6 when
  a rule changes.
- **Every quorum question crosses `membership/`**, as in the core: a count against a threshold
  is for the oracles only.
- **Tiger Style assertions**: hard `assert!`, pre- and postconditions, `# Panics` sections.

## Gates

`cargo nextest run -p paros-journal-control -p journal-sim`, `cargo clippy --all-targets -- -D
warnings`, `RUSTDOCFLAGS="-D warnings" cargo doc -p paros-journal-control --no-deps`,
`cargo check --target wasm32-unknown-unknown -p paros-journal-control`,
`cargo run -p paros-journal-control --example controller` and `--example driver`,
`scripts/check-core-deps.sh` (a pure crate) and `scripts/mutants.sh --control` (no survivor),
all in `nix develop`.
