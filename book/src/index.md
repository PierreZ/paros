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
CI runs in your browser tab. The [Watch it live](single-decree.md) page runs it there.

> **How to read this book.** It is a guided tour of the algorithm, grounded in the
> papers and mapped onto the real `paros-core` code. Part one,
> *Single-decree Paxos*, covers [how a value is chosen](choose-one-value.md) and
> [why that choice is safe](safety.md). Part two, *Multi-Paxos*, builds the
> [replicated log](replicated-log.md), elects a [stable leader](stable-leader.md),
> and works through a [crash and restart safety](restart-safety.md) bug the
> simulation caught. Every chapter explains with diagrams; to watch the
> single-decree kernel actually run, open [Watch it live](single-decree.md).
