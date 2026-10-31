---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
title: Same code in production and simulation
description: parosd and the simulator call the same run_machine. Only the providers change. The faults that the simulator injects live in the shipped code.
---

## The problem

A simulator that runs a model of the system finds bugs in the model. If the tested code has
`Sim*` types, mocks or a second code path, the bugs that matter hide in the code that ships.

## The technique

Every machine of paros is one function, `paros::machine::run_machine`. It takes its network,
clock, task spawner and disk as **providers** (moonpool's traits). `parosd` gives it the real
ones. The simulator gives it simulated ones. Nothing else changes.

```rust
// crates/bin/parosd/src/main.rs: production, on compio.
paros::machine::run_machine(
    CompioProviders::new(),
    ProviderDisk::new(CompioStorageProvider::new(), data_dir, config),
    |_| NoAudit,
    &machine, addresses, Names::new(CompioResolver::new()), tunables, transport, shutdown,
)
```

```rust
// crates/sim/paros-sim/src/machine.rs: simulation, on moonpool-sim.
paros::machine::run_machine(
    ctx.providers().clone(),   // SimProviders: simulated network, clock and tasks
    disk(),                    // a ProviderDisk on the simulated disk
    audits.clone(),            // the safety audit sees what the machine does
    &settings, addresses.clone(), names.clone(), tunables,
    crate::tls::serving(ctx, paros::net::tls::Role::Machine, ctx.my_ip())?,
    ctx.shutdown().clone(),
)
```

The playground on this site calls the same function too, compiled to wasm.

## Faults in the shipped code

A fault point is a line in the shipped code. `hint!("label").await` marks a point where a crash
or a delay is dangerous: a sync is done and the answer is not sent yet. `buggify!` makes a rare
branch frequent: a machine that starts late, a short timeout. In production both do nothing.
Under simulation, moonpool decides with the seed whether to strike.

Some of the points in paros, with their real labels:

| Label | Where | What a strike tests |
|---|---|---|
| `batch durable, not sent` | `paros-roles/src/node/ready.rs` | a crash after a vote is on disk and before anyone hears it |
| `accepts sent, own vote not durable` | `paros-roles/src/node/ready.rs` | the leader dies with its `Accept`s out and its own copy lost |
| `promise durable, entries staged` | `paros-storage/src/journal/node.rs` | a power loss inside a store commit |
| `journal re-opened, not synced` | `paros-roles/src/node.rs` | a crash on a store that a failed sync left behind |
| `cell init promise durable, answer not sent` | `paros-machine/src/wait.rs` | a crash in the middle of `cell init` |
| `registration durable, reply not sent` | `paros-roles/src/matchmaker.rs` | a matchmaker that dies before it answers |
| `move booked, not reconfigured` | `paros-coordinator/src/tenant/moves.rs` | a coordinator that dies inside a tenant operation |

## The rules

- No `Sim*` type and no wrapper around shipped code. A fact the simulator needs comes from a
  read-only accessor or from the audit port.
- `paros-core` has no `hint!` and no `buggify!`. It is sans-IO, so the simulator reaches it
  only through its inputs (see [the sans-IO core](../sans-io/)).
- A fault point has a `reachable!` assertion beside it. A point that no seed reaches is a bug
  in the test.
