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

- `src/lib.rs` → `run`, `run_json`, `runSandbox` (wasm), `check` → one seed of
  `SimulationBuilder`: the machine group, the operator workload, the timeline; no fault
  injection: every machine and the operator call `moonpool_sim::buggify_reset` first, so the
  shipped BUGGIFY sites and crash hints are inert, as in production. A log starts with `init`
  at 0 ms (over the first `machines`) and holds no other `init`.
- `src/model.rs` → `Config`, `Action`, `Op` (`init`, `create_tenant`, `create_journal` with
  its `mode`: `single`, `double`, `triple` or `grid:RxC`, `write`, `read`), `StepResult`,
  `Topology` (each machine's short and full node id and its zone), `TenantPlace`,
  `JournalPlace` (its `key`, acceptors in membership order, `Quorum`), `Event`, `RunView` →
  the JSON contract.
- `src/machine.rs` → `MachineProcess`, `MACHINE_GROUP`, `machine_ip`, `machine_addr`, `zone_of`
  → machine `i` is `10.0.1.{i+1}:4500` in `zone-{i mod 3}`, literal addresses, plaintext.
- `src/operator.rs` → `Operator` → `init` over the first machines (`paros::client::initialize`);
  `FleetSession::create_tenant`; `journals::request` (create); a write claims and writes as the
  journal's one `Writer`, one entry per record, the identical entry again after an unsettled
  answer; a read walks a `Reader` from position 0. After each step, `survey` reads the topology through the admin API
  only: `read_directory`, `journals::list` and `Client::inspect` on every machine.
- `src/calls.rs` → `CallTrace` → a read-only `CallObserver` on the operator's journal clients:
  each attempt a `client_call` event, each answer a `client_answer` event.
- `src/timeline.rs` → `Timeline`, `NAMES` → the events the page draws and lists (`msg_sent`
  names its journal, so the page colours each message by tenant).
- `src/bin/paros-sandbox.rs` → the same JSON on stdin and stdout, natively.
- `tests/probe.rs` → `init` over four of five machines; two tenants create, write and read; the topology names each journal's acceptors
  and leader, and a `grid:2x2` journal's quorum shape; replay is identical, and a longer log
  keeps the shorter one's steps.

## Gates

`cargo nextest run -p paros-sandbox`; `cargo check --target wasm32-unknown-unknown -p
paros-sandbox` (CI `portability`); `scripts/build-sandbox.sh` in the Pages workflow.
