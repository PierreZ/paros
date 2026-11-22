---
layout: ../../layouts/Doc.astro
kicker: parosd
section: parosd/
status: parosd
title: Layers that stand alone
description: Each layer of parosd is a crate that a program that is not paros can use alone. A layer depends only on the layers below it, and the core has no tenant.
---

## The problem

The service at the tag `v0.2.0-alpha.1` worked, but its crates were cut by kind of code: one
crate for the wire, one for the model, one for the knobs, one for the observation. Each of them
knew the journal, the cell, the tenant and the machine at once. So no layer could be built,
proved or used alone, and a change in one layer touched every crate.

## The rule

**Cut by layer of meaning, not by kind of code** (goal 7 of
[§1.1](https://github.com/PierreZ/paros/blob/main/docs/architecture/01-goal.md#11-the-goals-and-the-root-rules),
[§4.4](https://github.com/PierreZ/paros/blob/main/docs/architecture/04-roles.md#44-the-layers-of-the-service)).
Each layer owns its messages, its knobs, its events, its model and its walk. The test for each
crate is one question:

<pre><code>Can a program that is not paros use it alone?</code></pre>

## The layers

The features of the tag stay; the structure changes. The names of the crates still to build, and
the exact table, are **Open** (§4.4).

| Layer | What it is | Today |
|---|---|---|
| 0 | The Paxos flavors. No tenant, no cell. | `paros-consensus` |
| 1 | The journal v1, its grid controller, and the client | `paros-journal`, `paros-journal-control` |
| 2 | The host of actors: the role contract, the flight rule, group commit, the IO traits | `paros-journal-io` |
| 3 | A fold over a journal, and the election by `SetLeader` | to build |
| 4 | One crate for each control level: universe, cell, tenant. The tenancy wrapper makes the pair (tenant, journal) here, above the core | to build |
| 5 | The machine, the names, the tokens | `paros-authz-biscuit`; the rest to build |
| 6 | The adapters: protobuf over moonpool RPC, disks, metrics. The only layer that does I/O | to build (#1330) |
| 7 | The programs: `parosd`, `parosctl`, the bench | at the tag only |

Layers 0 to 5 are pure. They depend on no async runtime, no storage engine and no network crate,
and they build for `wasm32-unknown-unknown` (goal 10). `paros-authz-biscuit` does not build for
wasm today: §4.4 calls that a finding.

## Who uses a layer alone

- A program that needs one decision, or a replicated log, takes `paros-consensus` alone: the
  [Consensus](../../consensus/) pages show it.
- A program that needs a durable, fenced log takes the journal and the host, and implements the
  traits of `paros-journal-io` for its own disk and network: the [Journal](../../journal/) pages.
- A program that needs a leader takes the election crate of layer 3 and the journal. The
  coordinators of paros use the same API (§3.3, "One election mechanism for everyone").
- A program that needs tenants wraps the journal with the tenancy crate. The core never learns
  what a tenant is (#1402 (remove the tenant from the core)).

## A role is a pure actor

A role (an acceptor, a leader, a coordinator, the frontend) is a pure `step` under the flight
rule of the journal
([§J4.2](https://github.com/PierreZ/paros/blob/main/docs/journal/04-roles.md#j42-the-journals-sans-io-contract)):
a send waits for the persist batch that it names, the step reads no clock and draws no random
number, and a refusal is data. One pure host serves every role of a process (goal 8). So each
layer gets its own TLA+ model, its own walk and its own trace check, as the
[core](../../consensus/proof/) and the journal have now.

The frontend is a role like the others. It has no layer and no crate of its own. moonpool gets
no actor layer: an activation of a role is a group of endpoints of moonpool RPC, and the paros
adapter keeps the map from a role's id to its group (§4.4).

## The checks that hold the layering

A rule that no check holds decays. These checks run in CI today:

- `scripts/check-core-deps.sh`: a pure crate depends on no runtime, I/O or storage crate.
- `scripts/check-core-layers.sh`: inside `paros-consensus`, the layers point down only.
- `scripts/check-examples.sh`: each composing crate has examples, and CI runs each one. The
  examples are how a reviewer judges a layer from the outside.
- The wasm gates: `cargo check --target wasm32-unknown-unknown` on each pure crate.

#1411 (the CI checks that hold the layering) adds the rest: a table of the layer of each crate,
where a dependency that points up or sideways fails CI; a wasm build of each crate of layers 0
to 5; and no fault switch in a pure crate. When a crate of the stack exists, its row in the table
above links its example.
