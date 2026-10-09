# Fault injection in the shipped code

Date: 2026-10-09. Scope: PierreZ/paros at `8070f24`, PierreZ/moonpool at `607f404`.

Rule (decided on 2026-10-09, root `AGENTS.md`, *Turbulence layers*): a fault that a code path can meet is decided by a buggify point in the shipped code. The
point asks a hook that is a no-op in production. Sim wrappers only observe. The harness keeps
only what an operator does offline, and the environment (moonpool's chaos).

## A. Convert: the sim decides a fault from outside the code

| # | Where | What the sim decides now | Target in shipped code | PR order |
|---|---|---|---|---|
| A1 | `paros-sim/src/process.rs` `restart_delay!` (9 sites: node, joiner, matchmaker, proxy, replica restarts) | A buggified sleep in the sim wrapper before the next incarnation boots | `DriverHooks::boot_delay()` consulted at the top of `run_journals`, `run_matchmaker`, `run_proxy`, `run_replica`: a buggified `provider.sleep`. Same hook as the machine rework of #292 | 2 (after #292 lands its hook) |
| A2 | `paros-sim/src/world/power.rs` `PowerCut` (wraps node and matchmaker store syncs in `world/node_store.rs`, `world/registry_store.rs`) | A wrapper races a timer against the commit and calls `SelfCrash` | Crash points inside `moonpool-journal`'s write protocol (after the entry writes, after the record writes, before each sync), through a crash hook in the journal config, no-op in production. Moonpool PR first, then the paros pin bump, then `power.rs` goes | 3 |
| A3 | `paros-sim/src/chain_workload/fleet.rs`: "an init / a tenant creation / a tenant removal stops after one step" and `checkpoint_directory_and_stop` | The workload calls one step and returns, as if the operator died | `ClientHooks::stop_at(StopPoint)` in `paros::client`, asked by the fleet step loops and by `Checkpointer::checkpoint` before its truncate; `NoClientHooks` in production. Also covers the registry owner's stop before its truncate (`chain_workload/system.rs`) | 1: done, `ClientHooks` |
| A4 | `paros-sim/src/chain_workload/fleet.rs` `killer` (#247) | The workload kills an operation's target machine a drawn delay into the call | A seam crash in the driver when a client call to a control journal is in flight (`Seam::ClientCallInFlight`, via `hooks.crash_at`) | 4 |
| A5 | `paros-sim/src/chain_workload.rs` `held_replica` (a replica held down across the tail truncation) and `parent_seed` (the control-journal host held down, #247 static stability) | The workload crashes a chosen process and restarts it later | A long buggified `boot_delay` on that role's restart (A1's hook, a longer arm), plus the existing seam crashes. Keeps the gates "held down across the truncation" and "control journals' host held down" | 5 |

## B. Stays in the harness: what an operator does offline

- Disk wipe of a node or a matchmaker (`process.rs` wipe coins, `world/wipe.rs`): a replaced disk.
- Edited configuration file on restart (`OperatorEdit`, #207).
- Operator reboots every member of a new configuration (#173, `lifecycle.rs`).

## C. Stays: the environment (moonpool's layer)

- Attrition, network and storage chaos (`chaos_surfaces()`), `Chaos::Outage` (`world/outage.rs`).
- The scenario outages that strike on a ledger fact: `world/late_outage.rs`, `world/bare_outage.rs`.
- Disk rot at boot (`world/injector.rs`) and an outage's planned losses: bytes that go bad on the
  disk, aimed by the custody ledger and held to the copy budget. No code path decides bit rot.
  Open question for Pierre: should this move into moonpool's storage chaos?
- The reboot time after a `SelfCrash` (`crash_self(.., Some(250..3000 ms))`): the time the power
  is off. A1 removes the extra wrapper sleep on top of it.

## D. Already right

- `DriverHooks` / `BuggifyHooks` (`paros/src/hooks.rs`, `paros-sim/src/hooks.rs`) and the durability
  seams (`crash_at`).
- Per-seed knobs (`buggify_knob!` in `shape.rs`, `ChainConfig`).
- Client misbehaviours as explicit calls (stale writes, foreign attacks, races).

## Not touched by this sweep

- `paros-sim/src/machine.rs` (`MachineDisk` crashes, machine boot delay): the "Work through issue
  69 backlog" thread reworks it in #292 with `Seam::CellPromised`, `Seam::CellFormatted` and
  `hooks.boot_delay()`.
