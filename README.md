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
[`quickstart.rs`](crates/lib/paros-consensus/examples/quickstart.rs): three nodes, the driver loop, a
crash and a restart.

Eleven small, deterministic, runnable examples are in
[`paros-consensus`](crates/lib/paros-consensus). Each one prints a trace and asserts the property
that it shows. The first runs single-decree Paxos on the public `Decree`. Each other example
drives the deployments (`ColocatedNode`, `ProxyLeader`, `ReplicaNode`, `Matchmaker`) as a driver
does, with one disk for each process and a toy network
([`examples/common/cluster.rs`](crates/lib/paros-consensus/examples/common/cluster.rs)). No example builds a node from the roles
by hand: what an example does is what a program does. Read them in order:

1. [`single_decree.rs`](crates/lib/paros-consensus/examples/single_decree.rs) — Phase 1, P2c, Phase 2 for one value: why a
   higher ballot must adopt a value that it finds accepted, and an acceptor that restarts from
   its disk.
2. [`quickstart.rs`](crates/lib/paros-consensus/examples/quickstart.rs) — the driver loop (persist, then send, then learn), an
   election, writes, a follower that crashes and catches up, and a new leader.
3. [`multi_paxos.rs`](crates/lib/paros-consensus/examples/multi_paxos.rs) — one Phase 1 for many slots, and the recovery of a
   new leader: it proposes again the value that an acceptor reported (P2c) and fills the slot
   that nobody reported with a `Noop`.
4. [`journal_calls.rs`](crates/lib/paros-consensus/examples/journal_calls.rs) — the journal that the log carries: `SetLeader`,
   `Write`, a retry, a fenced writer, a read, `Truncate`, and the `Outcome` of each one.
5. [`leader_handoff.rs`](crates/lib/paros-consensus/examples/leader_handoff.rs) — the leader hands its ballot to a successor
   with no election, the one-hop rule, and a lost handoff that an ordinary election repairs.
6. [`acceptor_grid.rs`](crates/lib/paros-consensus/examples/acceptor_grid.rs) — the acceptor grid of Compartmentalized Paxos:
   six nodes under `QuorumSystem::grid(2, 3)`. One column decides a slot, so each acceptor
   votes for a third of the commands. A round whose column is down moves to another column.
   An election needs one acceptor of every column, so four nodes of six do not always elect.
7. [`quorum_read.rs`](crates/lib/paros-consensus/examples/quorum_read.rs) — leaderless reads: a row of the grid answers with
   its vote watermarks while a column is mid-decision, and the reader waits for the maximum.
   A linearizable read with no leader and no clock.
8. [`proxy_leader.rs`](crates/lib/paros-consensus/examples/proxy_leader.rs) — proxy leaders: the leader gives the slot, and a
   `ProxyLeader` sends the `Accept`, counts the votes and sends the `Commit`. The leader takes
   back the round of a dead proxy leader, and a handoff successor delegates again what it got.
9. [`replica_tier.rs`](crates/lib/paros-consensus/examples/replica_tier.rs) — replicas: three acceptors vote, and two
   `ReplicaNode`s learn and never vote. A replica that lost a `Commit` catches up from the
   beat, a replica restarts from its disk, and one replica owns the reply of each slot.
10. [`matchmaker.rs`](crates/lib/paros-consensus/examples/matchmaker.rs) — Matchmaker Paxos: a candidate registers its
    configuration at a majority of the matchmakers before Phase 1, a matchmaker restarts from
    its registry, and the matchmaker set hands over to a new set with `MatchmakerReconfigurer`.
11. [`reconfiguration.rs`](crates/lib/paros-consensus/examples/reconfiguration.rs) — five nodes and three matchmakers:
    `{1, 2, 3}` becomes `{1, 4, 5}`, the matchmakers forget the old configuration, and a
    removed node retires.

```sh
cargo run -p paros-consensus --example single_decree
cargo run -p paros-consensus --example quickstart
cargo run -p paros-consensus --example multi_paxos
cargo run -p paros-consensus --example journal_calls
cargo run -p paros-consensus --example leader_handoff
cargo run -p paros-consensus --example acceptor_grid
cargo run -p paros-consensus --example quorum_read
cargo run -p paros-consensus --example proxy_leader
cargo run -p paros-consensus --example replica_tier
cargo run -p paros-consensus --example matchmaker
cargo run -p paros-consensus --example reconfiguration
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
