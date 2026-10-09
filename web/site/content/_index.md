+++
title = "paros"
sort_by = "weight"
template = "section.html"
+++

<div style="text-align: center;">
  <img src="paros-logo.svg" alt="paros logo" style="width: 180px; margin: 0 auto;" />
</div>

**paros** is a learning project. It implements the Paxos family of consensus algorithms in
Rust, and it is built and validated with
[deterministic simulation testing](https://pierrez.github.io/moonpool/) (DST). It is a work in
progress and not for production.

The name refers to two Greek islands: [Paros](https://en.wikipedia.org/wiki/Paros) and
[Paxos](https://en.wikipedia.org/wiki/Paxos), the island where Leslie Lamport set the consensus
algorithm.

This site has two parts:

- **[parosd](@/parosd/_index.md)**, the system: a multi-tenant journal service with four calls
  (`Write`, `Read`, `Truncate`, `SetLeader`).
- **[The Paxos implementation](@/paxos/_index.md)**: what `paros-core` and `paros` do, told as
  a learning path, from the classic two phases to the difficult parts around them.

**[paros play](play/)** is an interactive game. It runs the real `paros-core` in your browser.
You control the network, the clock and each role.
