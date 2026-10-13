# paros-sandbox

The Paxos sandbox's engine (#518 (Paxos visualization sandbox)): a paros universe on moonpool's
simulated network, disk and clock, run from a `Config` and an action log, natively or as wasm.
`publish = false`; `cdylib` for the browser, `rlib` for the tests. Depends on `paros` (no
`tls`) and `moonpool-sim` with default features off (no exploration), never on `paros-sim` or
`parosd`. The page is `website/sandbox/`; the rules are in `website/zola/AGENTS.md`.

## The rule

**Same code as production.** Every machine is the shipped `paros::machine::run_machine` on a
bare `ProviderDisk`; the operator calls `paros::client` as `parosctl` does. No wrapper, no
sim-only path, no buggify or hint here. The timeline is an `Invariant` that only copies named
`tracing` events. A fact the page needs from inside paros is a read-only accessor in `paros`.

## Map

- `src/lib.rs` → `run`, `run_json`, `runSandbox` (wasm) → one seed of `SimulationBuilder`: the
  machine group, the operator workload, the fault injector, the timeline; `chaos_until_run_ends`
  with no random fault enabled.
- `src/model.rs` → `Config`, `Action`, `Op`, `StepResult`, `Event`, `RunView` → the JSON contract.
- `src/machine.rs` → `MachineProcess`, `MACHINE_GROUP`, `machine_ip`, `machine_addr` → machine
  `i` is `10.0.1.{i+1}:4500`, literal addresses, plaintext.
- `src/operator.rs` → `Operator` → `init` (`paros::client::initialize`), the first write creates
  tenant `sandbox` and journal `log` (`FleetSession::create_tenant`, `journals::request`), then
  `Writer` claim and write; `cell add-machine` (`CellSession::add_machine`).
- `src/faults.rs` → `Faults` → crash, restart, partition, heal on a fixed 50 ms tick: the tick is
  the same whatever the log holds, so an added action never shifts the random stream before its
  time.
- `src/timeline.rs` → `Timeline`, `NAMES` → the events the page draws.
- `src/bin/paros-sandbox.rs` → the same JSON on stdin and stdout, natively.
- `tests/probe.rs` → milestone 1: a cell forms and chooses a value, survives its leader's crash,
  admits a fourth machine; replay is identical, and a longer log keeps the shorter one's steps.

## Gates

`cargo nextest run -p paros-sandbox`; `cargo check --target wasm32-unknown-unknown -p
paros-sandbox` (CI `portability`); `scripts/build-sandbox.sh` in the Pages workflow.
