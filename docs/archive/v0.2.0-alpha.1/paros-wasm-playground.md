# paros-wasm-playground

The `playground` feature of `paros-wasm`: one live paros universe on moonpool's simulated network, disk and clock, driven one step at a time from a browser tab (`website/sandbox/`) or natively. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`); the `paxos` feature of the same crate stays. At the tag it lived in `crates/support/paros-wasm/` (`src/playground.rs`, `src/playground/`, `src/bin/paros-playground.rs`, `tests/playground_*.rs`: 5,178 lines of Rust), with the page in `website/sandbox/` (about 3,100 lines of JS and CSS).

## What it did

A support crate (`publish = false`), feature `playground` (#518 (paros playground), #813 (live playground)). It depended on `paros` (no `tls`), `moonpool-sim` and `moonpool-rpc` with default features off, never on `paros-sim` or `parosd`. It built to the wasm bundle `paros_playground` (`scripts/build-wasm.sh`), staged at `/playground/app/`. The Engineering pages also called its `runJson` export (`website/site/public/engineering.js`, `engineering-worker.js`, #951 (live Engineering pages)).

The rule: same code as production. Every machine is the shipped `paros::machine::run_machine` on a bare `ProviderDisk`. The `Operator` calls `paros::client` the way `parosctl` does. No wrapper, no sim-only path. Unless `Config::chaos.crashes` is on, every machine and the operator call `moonpool_sim::buggify_reset()` first, so the shipped BUGGIFY sites and crash hints are inert, as in production.

Flow: `Live::start` builds one seed of `SimulationBuilder::live()` with the machine group (`MachineProcess`, machine `i` at `10.0.1.{i+1}:4500` in `zone-{i mod 3}`), the operator workload, the fault `Injector` and the `Timeline`. `Live::act` sends a step: a fault step (`crash`, `restart`, `isolate`, `heal`) goes to the injector and strikes at once; any other step queues for the operator. `Live::advance` moves simulated time and returns a `Frame` (steps that ended, topologies the operator read, events). The operator reads the `Topology` after each step and every second, through the admin API only. `run` / `run_json` play a whole action log on the same engine for tests and the native binary.

The page: `worker.js` runs the wasm `Universe` off the main thread and posts a frame every 100 ms; `blocks.js` draws one action block per operator verb with dropdowns filled from `Frame.choices` (#844 (playground action blocks)); `stage.js` draws cells, machines, journals, messages and calls; `matchmakers.js` and `election.js` fold events into views and are pure, tested with `node --test`.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/support/paros-wasm/src/playground.rs` | `Live` (`start`, `act`, `advance`, `finish`), `Frame`, wasm `Universe` (`new`, `act`, `advance`, `nowMs`), `run`, `run_json` (`runJson`), `check_config` / `check_op`; bounds `MAX_MACHINES` (9), `MAX_RUN_MS`, `MAX_COUNT`. |
| `crates/support/paros-wasm/src/playground/model.rs` | The JSON contract: `Config`, `Chaos`, `Action`, `Op`, `StepResult`, `Topology`, `TenantPlace`, `JournalPlace`, `Quorum`, `Event`, `RunView`. |
| `crates/support/paros-wasm/src/playground/operator.rs` | `Operator`: init, add cell, tenants, journals, configure, writes (single-writer `Writer` and multi-writer), set-leader, traffic, drain, remove, maintenance, resolve; `survey` of the topology. 1,818 lines. |
| `crates/support/paros-wasm/src/playground/machine.rs` | `MachineProcess`, `MACHINE_GROUP`, `machine_ip`, `machine_addr`, `zone_of`, `class_for` (`stateless` machines, #822 (stateless playground roles)). |
| `crates/support/paros-wasm/src/playground/faults.rs` | `Injector`: fault steps on the simulated world. |
| `crates/support/paros-wasm/src/playground/chaos.rs` | `surfaces`, `storage_mask`, `HintBudget`, `veto`: `Config::chaos` mapped to moonpool chaos surfaces. |
| `crates/support/paros-wasm/src/playground/timeline.rs` | `Timeline` (a moonpool `Invariant` that only copies events), `NAMES` (the event names the page draws). |
| `crates/support/paros-wasm/src/playground/calls.rs` | `CallTrace`: a read-only `CallObserver` that logs `client_call` and `client_answer`. |
| `crates/support/paros-wasm/src/playground/choices.rs` | `Choices`: the valid values of each block field, from the last topology. |
| `crates/support/paros-wasm/src/bin/paros-playground.rs` | The same JSON on stdin and stdout. |
| `crates/support/paros-wasm/tests/playground_probe.rs`, `playground_matchmakers.rs`, `playground_chaos.rs` | End-to-end runs of the engine. |
| `website/sandbox/` (`worker.js`, `main.js`, `blocks.js`, `stage.js`, `matchmakers.js`, `election.js`) | The page; its own map is `website/sandbox/AGENTS.md`. |

## Ideas worth reusing

- **Run the shipped binary's code in the browser** (`machine.rs`, `operator.rs`). A demo that cannot drift from production, because it is production code on a simulated world.
- **Observe, never act** (`timeline.rs`, `calls.rs`). The timeline is a moonpool `Invariant` that copies named `tracing` events; the call trace is a read-only observer. A fact the page needs is a read-only accessor in `paros`.
- **Faults on the world, not the code** (`faults.rs`, `chaos.rs`). `crash`, `isolate`, `heal` are moonpool injector actions. Chaos mode turns on moonpool's own surfaces and the shipped hints, with a `HintBudget` that opens only after `init`.
- **One JSON contract for wasm, native binary and tests** (`model.rs`, `run_json`). Replay is a test: the same config and log give the same run (`tests/playground_probe.rs`, near line 133).
- **Blocks list only valid values** (`choices.rs`): the engine computes what a click may name, so the page has no protocol logic.
- **Pure fold modules on the page** (`website/sandbox/matchmakers.js`, `election.js`) tested with `node --test`.
- **Bounds checked before a run** (`check_config`, `check_op`): `a_step_out_of_its_bounds_is_refused_with_its_bound`, `a_json_request_answers_its_error_without_a_run`.

Leaks and mistakes not to copy:

- **Log event names as an API.** `NAMES` in `timeline.rs` and the page's JS depend on `tracing` event names and field names (`journal_committed` with `syncs`, `match_registered`, ...). A rename in the library breaks the page with no compile error. A new design should export typed events.
- **The operator re-implements `parosctl` flows** (`operator.rs`, 1,818 lines): writer pacing, retries after an unsettled answer, drain and removal texts. It copies the old control-plane commands (`add_machine`, `drain`, `maintenance`, `configure` with layouts) that the rework replaces (`docs/architecture` rules 3, 4 and 7).
- **The `Config` keeps old defaults for old action logs** (`an_old_action_log_parses_with_the_old_defaults`, `default_durability` in `model.rs`): compatibility cost in a demo.

## Bugs and scenarios to cover again

These runs are good end-to-end acceptance scenarios for a new journal:

- **Form, create, write, read, show topology; replay identical**: `tenants_write_read_and_show_their_topology`.
- **A crashed leader is replaced; acceptors show their state**: `a_crashed_leader_is_replaced_and_acceptors_show_their_state`.
- **An isolated machine is cut, then healed**: `an_isolated_machine_is_cut_then_healed`.
- **A reconfiguration registers a new configuration and GC drops the old** (matchmakers): `a_reconfiguration_registers_a_new_configuration_and_gc_drops_the_old` in `tests/playground_matchmakers.rs`.
- **With crashes on, machines die at the shipped hints and the cell recovers; with no chaos, no fault**: `crashes_strike_at_hints_and_the_run_goes_on`, `no_chaos_draws_no_fault` in `tests/playground_chaos.rs`.
- **Every store commit syncs twice** (the ordered protocol, `docs/architecture` §2.9): `every_store_commit_syncs_twice`.
- **Storage chaos never fails a disk for good; a wiped machine never rejoins; one machine down at a time** (`chaos.rs`): `storage_chaos_never_fails_a_disk_for_good`, `crashes_strike_only_machines`.
- **A stateless machine founds no cell and takes proxies, batchers and frontends**: `a_stateless_machine_founds_no_cell`, `a_stateless_machine_takes_the_proxy_leaders_batchers_and_frontends`.
- **Faults on a machine outside the universe are refused; adding a machine twice fails**: `a_fault_on_a_machine_outside_the_universe_is_refused`, `adding_a_machine_already_in_the_cell_fails_and_outside_the_universe_is_refused`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/support/paros-wasm/src/playground.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/support/paros-wasm (and `website/sandbox` beside it)

Where to start reading: `src/playground.rs` (module doc, `Live`), `src/playground/model.rs`, `src/playground/machine.rs`, `src/playground/timeline.rs`, then `tests/playground_probe.rs`. Its own map at the tag: `crates/support/paros-wasm/AGENTS.md` (section `playground`) and `website/sandbox/AGENTS.md`.
