+++
title = "paros"
sort_by = "weight"
template = "landing.html"
+++

## The name

The name refers to two Greek islands: [Paros](https://en.wikipedia.org/wiki/Paros) and
[Paxos](https://en.wikipedia.org/wiki/Paxos), the island where Leslie Lamport set the consensus
algorithm. The logo is an archipelago: the islands are the machines, the dotted routes are the
messages, and the island in the accent colour is the leader.

## How it is tested

paros is built and validated with
[deterministic simulation testing](https://pierrez.github.io/moonpool/) (DST). The simulator runs
the same code as production on a simulated network, disk and clock, and it injects faults for the
whole run. A failing run replays exactly from its seed.
