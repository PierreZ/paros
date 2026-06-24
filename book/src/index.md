<div style="text-align: center;">
  <img src="paros-logo.svg" alt="paros logo" width="180" />
</div>

# paros

**paros** is a learning project: an implementation of the Paxos family of
consensus algorithms in Rust, built and validated with
[deterministic simulation testing](https://pierrez.github.io/moonpool/) (DST).
It is a work in progress and not for production.

The name is a nod to two Greek islands:
[Paros](https://en.wikipedia.org/wiki/Paros) (a favorite) and
[Paxos](https://en.wikipedia.org/wiki/Paxos), the island Leslie Lamport set the
consensus algorithm on.

The design is **sans-IO**: `paros-core` is a pure synchronous state machine —
`step`/`tick` in, one `Ready` out, an `advance()` handshake — with no I/O, no
clock, and no randomness. An async driver (built on
[moonpool](https://github.com/PierreZ/moonpool)) wraps the core and performs all
side effects in the order the `Ready` documents, honoring the persist-before-send
durability rule at the heart of Paxos safety.

Because the core is portable to WebAssembly, the *same* simulation that runs in
CI runs in your browser tab. The next chapter embeds it live.

> **Where we are.** This book is built from Stage 2, the single-decree safety
> kernel: three acceptors run Prepare/Promise/Accept/Accepted under a chaotic
> network, and a safety oracle proves that at most one value is ever chosen.
> Leader election, a replicated log, and storage faults arrive in later stages and
> will extend this same demo.
