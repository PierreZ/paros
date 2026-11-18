<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/paros-logo-dark.png" />
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/paros-logo-light.png" />
    <img src="docs/assets/paros-logo-light.png" alt="paros" width="200" />
  </picture>
  <h1>paros</h1>
  <p><strong>Paxos, in Rust.</strong> A learning implementation of the Paxos family of
  consensus algorithms.</p>
</div>

> ⚠️ A learning project, work in progress. Not intended for production use.

## The name

`paros` is named after [Paros](https://en.wikipedia.org/wiki/Paros), my favorite Greek
island, and winks at [Paxos](https://en.wikipedia.org/wiki/Paxos), the (other) Greek island
Leslie Lamport set [the consensus algorithm](https://en.wikipedia.org/wiki/Paxos_(computer_science))
on. Two islands, one parliament.

## What it is

The design is **sans-IO**: [`paros-core`](crates/lib/paros-core) is a pure synchronous state machine
(`step` / `tick` in, one `Ready` out, an `advance()` handshake) with no I/O, no clock, and no
randomness. A driver wraps it and performs all side effects, honoring the persist-before-send
rule at the heart of Paxos safety.

Today the tree is the core and the journal:

| Crate | What it is |
|-------|------------|
| [`paros-core`](crates/lib/paros-core) | The sans-IO Multi-Paxos roles and `ColocatedNode` (std-only, wasm-safe, zero deps with `default-features = false`) |
| [`paros-storage`](crates/lib/paros-storage) | The log storage seam and the stores on [moonpool](https://github.com/PierreZ/moonpool)'s `moonpool-journal` |
| [`paros-testkit`](crates/testkit/paros-testkit) | The judges of a journal: a linearizability checker and a journal model |

The TLA+ models of the protocol rules are in [`specs/tla/`](specs/tla).

The journal service that ran on it (`parosd`, `parosctl`, the bench, the simulation harness and
the site) left the tree on 2026-11-18. The git tag `last-service` holds it, and
[`docs/archive/last-service/`](docs/archive/last-service/README.md) records what each part did.

## Learning Paxos with paros-core

Seven small, deterministic, runnable examples drive the composable roles of
[`paros-core`](crates/lib/paros-core) (`Proposer`, `Acceptor`, `Replica`, `Matchmaker`) by hand —
direct calls for the network, a `Vec` for the disk, a printed trace and assertions for the
property each one teaches. Read them in order:

1. [`single_decree.rs`](crates/lib/paros-core/examples/single_decree.rs) — Phase 1, P2c, Phase 2:
   one value, and why a higher ballot must adopt a value it finds accepted.
2. [`multi_paxos.rs`](crates/lib/paros-core/examples/multi_paxos.rs) — slots versus ballots, one
   Phase 1 amortized over many slots, and leader recovery as P2c applied slot by slot.
3. [`matchmaker.rs`](crates/lib/paros-core/examples/matchmaker.rs) — configuration discovery
   through the matchmakers, a reconfiguration as a round change, and the matchmaker set
   itself chosen by the same single-decree Paxos over `Vec<MatchmakerId>`.
4. [`acceptor_grid.rs`](crates/lib/paros-core/examples/acceptor_grid.rs) — the acceptor grid of
   Compartmentalized Paxos: six nodes under `QuorumSystem::grid(2, 3)`, every slot's Phase 2
   addressed to one column so each acceptor sees a third of the commands, a majority of
   promises that still misses a column, and the next election needing one acceptor of every
   column (a full row is one) because each promise in it reports exactly its own column's value.
5. [`quorum_read.rs`](crates/lib/paros-core/examples/quorum_read.rs) — Paxos Quorum Reads: a
   row of the same grid answers with its vote watermarks while a column is mid-decision, the
   reader settles on the maximum and waits for its replica to apply it, and the leader sends
   no beat and opens no round — a linearizable read with no leader and no clock.
6. [`proxy_leader.rs`](crates/lib/paros-core/examples/proxy_leader.rs) — proxy leaders: the leader
   sequences and a `ProxyLeader` on another process fans each slot's `Accept` out, folds the
   votes and announces the `Commit`; a dead proxy's round is taken back, and a handoff
   successor re-delegates what it inherited.
7. [`replica_tier.rs`](crates/lib/paros-core/examples/replica_tier.rs) — the replica tier: three
   acceptors vote and keep the chosen prefix, two `ReplicaNode`s learn it and never vote, a replica that lost a `Commit` catches up from the
   leader's beat, and each slot's reply is owned by one replica (`slot % replica_count`).

```sh
cargo run -p paros-core --example single_decree
cargo run -p paros-core --example multi_paxos
cargo run -p paros-core --example matchmaker
cargo run -p paros-core --example acceptor_grid
cargo run -p paros-core --example quorum_read
cargo run -p paros-core --example proxy_leader
cargo run -p paros-core --example replica_tier
```

## Build and test

Enter the Nix dev shell (`nix develop`, or rely on direnv), then:

```sh
cargo build
cargo nextest run                  # or: cargo test
cargo fmt && cargo clippy --all-targets -- -D warnings
nix develop .#tla --command scripts/check-tla.sh   # TLC on specs/tla/
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
