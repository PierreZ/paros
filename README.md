<div align="center">
  <img src="book/src/paros-logo.png" alt="paros" width="200" />
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

The design is **sans-IO**: [`paros-core`](paros-core) is a pure synchronous state machine
(`step` / `tick` in, one `Ready` out, an `advance()` handshake) with no I/O, no clock, and no
randomness. An async driver built on [moonpool](https://github.com/PierreZ/moonpool) wraps it
and performs all side effects, honoring the persist-before-send rule at the heart of Paxos
safety.

The same code runs in production and in **deterministic simulation**: every seed replays
bit-for-bit, network chaos is injected, and a safety oracle asserts on every step that no two
acceptors ever choose different values. Because the core compiles to WebAssembly, the
simulation that runs in CI also runs in your browser.

👉 **[Read the book and play with the live demo](https://pierrez.github.io/paros/)**

## At a glance

| Crate | Role |
|-------|------|
| [`paros-core`](paros-core) | sans-IO Multi-Paxos state machine: zero deps, std-only, wasm-safe |
| [`paros`](paros) | the provider-generic node driver, default storage, and RPC contract |
| [`paros-sim`](paros-sim) | the deterministic-simulation harness: workloads and oracles |
| [`paros-wasm-demo`](paros-wasm-demo) | the browser visualization (GitHub Pages) |

Roadmap (filed as GitHub issues): **M1** safety kernel, **M2** Multi-Paxos, **M3**
storage-fault tolerance, **M4** online reconfiguration, **M5** scale-out and hardening.

## Build and test

Enter the Nix dev shell (`nix develop`, or rely on direnv), then:

```sh
cargo build
cargo nextest run                  # or: cargo test
cargo fmt && cargo clippy -- -D warnings
cargo run -p paros-sim-runner      # native safety sweep + one seed's timeline
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
