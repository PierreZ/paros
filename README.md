<div align="center">
  <img src="website/zola/static/paros-logo.png" alt="paros" width="200" />
  <h1>paros</h1>
  <p><strong>Paxos, in Rust.</strong> A learning implementation of the Paxos family of
  consensus algorithms, built and validated with deterministic simulation testing.</p>
</div>

> ⚠️ A learning project, work in progress. Not intended for production use.

## The name

`paros` is named after [Paros](https://en.wikipedia.org/wiki/Paros), my favorite Greek
island, and winks at [Paxos](https://en.wikipedia.org/wiki/Paxos), the (other) Greek island
Leslie Lamport set [the consensus algorithm](https://en.wikipedia.org/wiki/Paxos_(computer_science))
on. Two islands, one parliament.

## What it is

The design is **sans-IO**: [`paros-core`](crates/paros-core) is a pure synchronous state machine
(`step` / `tick` in, one `Ready` out, an `advance()` handshake) with no I/O, no clock, and no
randomness. An async driver built on [moonpool](https://github.com/PierreZ/moonpool) wraps it
and performs all side effects, honoring the persist-before-send rule at the heart of Paxos
safety.

The same code runs in production and in **deterministic simulation**: every seed replays
bit-for-bit, network chaos is injected, and an audit asserts on every transition that no two
acceptors ever choose different values.

👉 **[Read the site](https://pierrez.github.io/paros/)**

## At a glance

| Crate | Role |
|-------|------|
| [`paros-core`](crates/paros-core) | sans-IO Multi-Paxos state machine: std-only, wasm-safe, zero deps with `default-features = false` (only `tracing` spans on by default) |
| [`paros`](crates/paros) | the provider-generic node driver, default storage, and RPC contract |
| [`paros-sim`](crates/paros-sim) | the deterministic-simulation harness: the workload, the fault world, the audit |
| [`parosd`](crates/parosd) | the `parosd` daemon, one uniform binary per machine, and `parosctl`, the CLI |

## Run it

A cell of five machines and a frontend with Docker alone, from a fresh clone. Two tool
services act: `admin` is the operator, `acme` is a tenant that reaches only the frontend. Their
tokens and the root key the machines pin are in `docker-compose.yml`, made once with
`parosctl key bootstrap --tenant acme --ttl 3650d` ([`DEMO.md`](DEMO.md) has the whole demo and
shell aliases):

```sh
docker compose up -d --build
docker compose run --rm admin init --members node1:4500,node2:4500,node3:4500
docker compose run --rm admin tenant create acme
docker compose run --rm admin journal create acme orders
docker compose run --rm acme write acme/orders hello world --leader 7
docker compose run --rm acme read acme/orders
```

Every session can run over TLS, mutual by default: `parosctl tls init` writes the
authority and every certificate in one step.

A journal's durability can change while it runs: `parosctl configure --tenant acme --journal
orders --failures-per-partition 2` asks for five acceptors, and the tenant coordinator moves
the journal there by reconfiguration, without losing a write (#214 (tenant modes)).

A universe can hold more than one cell: `parosctl init --idle` forms a cell with no universe,
`parosctl universe add-cell` joins it, and `parosctl tenant create --cell` puts a tenant in it.
Any machine resolves a tenant to its cell. `parosctl universe remove-cell` removes an empty
cell for good.

Each tenant scrubs its journals in the background: its coordinator reads every acceptor's copy
of every record again, and an acceptor writes a damaged record again from its own copy
(#362 (background scrubbing)). `parosctl status --tenant acme` shows the pass and what it found.

A full disk is a normal result, never a crash or a lost write: the store goes read-only (it
still votes in Phase 1 and serves reads) and tries again later, placement passes over a machine
low on space, and a leader whose acceptors fall behind answers writes `busy` until they catch up
(#360 (disk full)).

Each machine serves Prometheus metrics and two probes (`/metrics`, `/healthz`, `/readyz`) when
`PAROS_HTTP_LISTEN` is set. It pushes its spans and metrics to an OpenTelemetry collector when
`PAROS_OTLP_ENDPOINT` is set. To measure throughput and latency on one machine, with no cell to
start first:

```sh
parosctl bench --local 3 --duration 30s
```

The copy-paste demo, with Docker Compose or three local processes, is
[`DEMO.md`](DEMO.md). The walkthrough — init, write and read, kill and restart a
machine, supersede a writer, lose a disk — is
[`crates/parosd/README.md`](crates/parosd/README.md).

Roadmap (filed as GitHub issues): **M1** safety kernel, **M2** Multi-Paxos, **M3**
storage-fault tolerance, **M4** online reconfiguration, **M5** scale-out and hardening.

## Learning Paxos with paros-core

Eight small, deterministic, runnable examples drive the composable roles of
[`paros-core`](crates/paros-core) (`Proposer`, `Acceptor`, `Replica`, `Matchmaker`) by hand —
direct calls for the network, a `Vec` for the disk, a printed trace and assertions for the
property each one teaches. Read them in order:

1. [`single_decree.rs`](crates/paros-core/examples/single_decree.rs) — Phase 1, P2c, Phase 2:
   one value, and why a higher ballot must adopt a value it finds accepted.
2. [`multi_paxos.rs`](crates/paros-core/examples/multi_paxos.rs) — slots versus ballots, one
   Phase 1 amortized over many slots, and leader recovery as P2c applied slot by slot.
3. [`matchmaker.rs`](crates/paros-core/examples/matchmaker.rs) — configuration discovery
   through the matchmakers, a reconfiguration as a round change, and the matchmaker set
   itself chosen by the same single-decree Paxos over `Vec<MatchmakerId>`.
4. [`flexible_quorums.rs`](crates/paros-core/examples/flexible_quorums.rs) — Flexible Paxos:
   four nodes under `Flexible { q1: 3, q2: 2 }` decide on two accepts, and the next election
   needs three promises so that one of them reports what the deciding pair chose.
5. [`acceptor_grid.rs`](crates/paros-core/examples/acceptor_grid.rs) — the acceptor grid of
   Compartmentalized Paxos: six nodes under `QuorumSystem::grid(2, 3)`, every slot's Phase 2
   addressed to one column so each acceptor sees a third of the commands, a majority of
   promises that still misses a column, and the next election needing one acceptor of every
   column (a full row is one) because each promise in it reports exactly its own column's value.
6. [`quorum_read.rs`](crates/paros-core/examples/quorum_read.rs) — Paxos Quorum Reads: a
   row of the same grid answers with its vote watermarks while a column is mid-decision, the
   reader settles on the maximum and waits for its replica to apply it, and the leader sends
   no beat and opens no round — a linearizable read with no leader and no clock.
7. [`proxy_leader.rs`](crates/paros-core/examples/proxy_leader.rs) — proxy leaders: the leader
   sequences and a `ProxyLeader` on another process fans each slot's `Accept` out, folds the
   votes and announces the `Commit`; a dead proxy's round is taken back, and a handoff
   successor re-delegates what it inherited.
8. [`replica_tier.rs`](crates/paros-core/examples/replica_tier.rs) — the replica tier: three
   acceptors vote and keep the chosen prefix, two `ReplicaNode`s learn it and never vote, a replica that lost a `Commit` catches up from the
   leader's beat, and each slot's reply is owned by one replica (`slot % replica_count`).

```sh
cargo run -p paros-core --example single_decree
cargo run -p paros-core --example multi_paxos
cargo run -p paros-core --example matchmaker
cargo run -p paros-core --example flexible_quorums
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
cargo fmt && cargo clippy -- -D warnings
cargo xtask sim run paros-chain             # coverage-guided (sancov) simulation sweep, the CI gate
cargo run --bin sim-paros-hunt -- main 2000 # raw seed hunt; a failing seed is its deliverable
cargo run --bin sim-paros-hunt -- canary 500 # every seed twice under moonpool's determinism canary; a diverging replay is its deliverable
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
