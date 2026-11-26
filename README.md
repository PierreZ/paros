<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/paros-logo-dark.png" />
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/paros-logo-light.png" />
    <img src="docs/assets/paros-logo-light.png" alt="paros" width="200" />
  </picture>
  <h1>paros</h1>
  <p><strong>Paxos, in Rust.</strong> A learning implementation of the Paxos family of
  consensus algorithms, built from the ground up, one proved flavor at a time.</p>
</div>

> ⚠️ A learning project, work in progress. Not intended for production use.

## The name

`paros` is named after [Paros](https://en.wikipedia.org/wiki/Paros), my favorite Greek
island, and winks at [Paxos](https://en.wikipedia.org/wiki/Paxos), the (other) Greek island
Leslie Lamport set [the consensus algorithm](https://en.wikipedia.org/wiki/Paxos_(computer_science))
on. Two islands, one parliament.

## What it is

The design is **sans-IO**: [`paros-consensus`](crates/lib/paros-consensus) is a pure synchronous state machine
(`step` / `tick` in, one `Ready` out, an `advance()` handshake) with no I/O, no clock, and no
randomness. A driver owns it and performs all side effects, honoring the persist-before-send
rule at the heart of Paxos safety.

Today the repository holds the consensus core and its proofs: the core and its tests, the
[`paros`](crates/lib/paros) facade, the TLA+ models of [`specs/tla/`](specs/tla), and the Paxos pages of the
site. The next step is a TLA+ model for every Paxos flavor the core builds, then a journal
designed again on top of it. [`docs/architecture/`](docs/architecture) is the target design.

The journal service that came before (`parosd`, `parosctl`, `paros-bench`, the library layers
and the `paros-sim` simulation) is at the git tag
[`v0.2.0-alpha.1`](https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1).
[`docs/archive/v0.2.0-alpha.1/`](docs/archive/v0.2.0-alpha.1) has one recap per removed crate.

The journal v1 of design A (`paros-journal`, its IO crate, its controller, the election library and
the `journal-sim` walks, with their TLA+ models and the `docs/journal/` design) was removed on
2026-11-24. It is at the git tag `journal-design-a`, and
[`docs/archive/journal-design-a/`](docs/archive/journal-design-a) lists what it held. The pure host of
actors `paros-host` was removed on 2026-11-25
([`docs/archive/paros-host/`](docs/archive/paros-host)). The work now brings segments into the
consensus core; a new plan for what comes above the core follows.

👉 **[Read the site](https://pierrez.github.io/paros/)**

## At a glance

| Path | What it is |
|------|------------|
| [`crates/lib/paros-consensus`](crates/lib/paros-consensus) | the sans-IO Paxos roles (proposer, acceptor, replica, matchmaker, proxy leader, quorum reads) and `ColocatedNode`; std-only, wasm-safe, zero deps with `default-features = false` |
| [`crates/lib/paros`](crates/lib/paros) | the facade a program depends on |
| [`crates/support/paros-wasm`](crates/support/paros-wasm) | the engine of the step-through diagrams of the site's Paxos pages |
| [`crates/support/paros-authz-biscuit`](crates/support/paros-authz-biscuit) | Biscuit tokens: root keys, offline mint and derive, the verifier of a request |
| [`specs/tla`](specs/tla) | TLA+ models of the protocol rules, checked with TLC |
| [`website`](website) | the site |
| [`docs`](docs) | the target design, the Paxos knowledge base, design notes and paper references |

## Learning Paxos with paros-consensus

To use the crate, start with its [README](crates/lib/paros-consensus/README.md) and with
[`grow.rs`](crates/lib/paros-consensus/examples/grow.rs).

Two deterministic, runnable examples are in [`paros-consensus`](crates/lib/paros-consensus).
Each one prints a trace and asserts each claim of it.

1. [`single_decree.rs`](crates/lib/paros-consensus/examples/single_decree.rs) — Phase 1, P2c, Phase 2 for one value: why a
   higher ballot must adopt a value that it finds accepted, and an acceptor that restarts from
   its disk. A decree has no deployment, so it gives each `Acceptor` its disk itself.
2. [`grow.rs`](crates/lib/paros-consensus/examples/grow.rs) — one journal that grows from three nodes to the whole
   deployment, one reconfiguration at a time. It drives the deployments (`ColocatedNode`,
   `ProxyLeader`, `ReplicaNode`, `Matchmaker`) as a driver does, with one disk for each process
   and a toy network ([`examples/common/cluster.rs`](crates/lib/paros-consensus/examples/common/cluster.rs)). Its stages:

   1. Three nodes elect a leader. A client becomes the writer and appends; a retry, a wrong
      position and another uuid get their `Outcome`.
   2. The leader crashes with a slot open. The new leader proposes again the value that a promise
      reported (P2c) and fills the slot that nobody reported with a `Noop`.
   3. `reconfigure` grows the journal to five acceptors, and node 3 leaves: register at the matchmakers, Phase 1
      over the old and the new configuration.
   4. `reconfigure` grows it to a 2×3 grid: one column decides a slot, and a row serves a read
      with no leader.
   5. `reconfigure` adds two proxy leaders, which run Phase 2 for the leader, and two replicas,
      which learn the log and answer the clients of their slots.
   6. The matchmaker set hands over to a new set (`MatchmakerReconfigurer`).
   7. The leader hands its ballot to another node with no election (`relinquish_to`), one hop.
   8. The leader cuts the log into a new segment (`roll`), and the matchmakers hold the chain. The replicas keep segment 0: they seal at its end.
   9. The leader crashes in the full deployment. A new leader takes over, and garbage collection
      lets the matchmakers forget the old configurations.
   10. Each deployment reports its `feedback` as plain data. The leader sees a crashed member miss
       its quorum windows.

```sh
cargo run -p paros-consensus --example single_decree
cargo run -p paros-consensus --example grow
```

## Build and test

Enter the Nix dev shell (`nix develop`, or rely on direnv), then:

```sh
cargo build
cargo nextest run                  # or: cargo test
cargo fmt && cargo clippy --all-targets -- -D warnings
nix develop .#tla --command scripts/check-tla.sh   # TLC on the TLA+ models
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
