# frankenpaxos — protocol patterns for a sans-IO Multi-Paxos

An analysis of [**frankenpaxos**](https://github.com/mwhittaker/frankenpaxos), Michael
Whittaker's Scala research codebase, read as a reference for building `paros` — a learning
**sans-IO Multi-Paxos** library in Rust. frankenpaxos is the most complete open implementation
of the Paxos lineage we care about: it contains single-decree Paxos, MultiPaxos, and
Whittaker's own **Compartmentalized MultiPaxos** and **Matchmaker Paxos**, whose papers already
live in [`../papers/`](../papers/).

## Read this first: what transfers and what doesn't

frankenpaxos is **actor/transport-based, not sans-IO**. Each protocol role is an `Actor` that
*calls* a `Transport` to send messages and arm timers — I/O is baked into the framework. That is
the architectural *opposite* of what `paros` wants: a pure core that does zero I/O, where the
caller feeds in events (`step(event)`) and drains the side effects it must perform (a
`Ready`-style struct). So we do **not** copy frankenpaxos's structure. Its value to us is twofold:

1. **Protocol content.** The single-decree value-selection rule, the MultiPaxos slot-log and its
   recovery/watermark machinery, the compartmentalization roles, and matchmaker reconfiguration
   are *algorithm*, not I/O. They transfer directly once you strip the actor wrapper.
2. **The deterministic simulator.** frankenpaxos's `FakeTransport` + property-based `Simulator`
   show that a consensus protocol can be tested by enumerating message/timer deliveries against
   invariants — no wall clock, no sockets, fully reproducible. A sans-IO core makes exactly this
   the *default* mode of operation, with the real network a thin bolt-on. This is the single most
   important idea to carry over.

Throughout, the **(c)** part of each section calls out what frankenpaxos *couples* that `paros`
should *invert*.

## How each section is structured

Mirroring the sibling doc [`../../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../../analysis/go-raft/etcd-raft-sans-io-patterns.md),
every section has three parts:

- **(a) What frankenpaxos does** — verbatim Scala with a `file:line` reference.
- **(b) The principle** — the language-neutral pattern worth keeping.
- **(c) Maps to sans-IO Multi-Paxos** — how it lands in a Rust core driven by the caller, with a
  log of slots each holding a chosen value, and which coupling to invert.

> **Line numbers** are from the checkout this was written against and will drift; symbol names
> are stable. Paths are relative to the frankenpaxos repo root
> (`shared/src/main/scala/frankenpaxos/…`).

## The parts

| File | Topic | For paros v1? |
|------|-------|---------------|
| [`01-framework-and-simulation.md`](01-framework-and-simulation.md) | The Actor/Chan/Transport skeleton, `FakeTransport` vs Netty, serialization, and the property-based `Simulator` | **Yes** — adopt the *simulator-first* mindset |
| [`02-single-decree-paxos.md`](02-single-decree-paxos.md) | Acceptor/Leader/Client, the value-selection rule, `RoundSystem`, election/heartbeat | **Yes** — the algorithmic kernel |
| [`03-multipaxos-core.md`](03-multipaxos-core.md) | The slot log, in-order execution, gap-fill, watermarks/recovery, `ClientTable`, `StateMachine`, quorum systems, read tiers | **Yes** — this *is* the bulk of v1 |
| [`04-compartmentalization.md`](04-compartmentalization.md) | Batcher / ProxyLeader / ProxyReplica / ReadBatcher and `DistributionScheme` | Later — throughput scaling |
| [`05-matchmaker-reconfiguration.md`](05-matchmaker-reconfiguration.md) | Matchmakers, prior-quorum intersection, GC, the Reconfigurer | Later — acceptor-set changes |

## v1 vs. later

- **Parts 1–3 are the v1 surface.** A first `paros` should be a single-leader, fixed-membership,
  synchronous Multi-Paxos: a sans-IO core (part 3) built from the single-decree kernel (part 2),
  developed and tested simulator-first (part 1).
- **Parts 4–5 are deliberately deferred.** Compartmentalization (throughput) and matchmaker
  reconfiguration (membership) are documented as *design horizons* so the v1 core doesn't paint
  itself into a corner — not as v1 work. Each is grounded in its paper:
  [Compartmentalization](../papers/scaling-rsm-compartmentalization/) and
  [Matchmaker Paxos](../papers/matchmaker-paxos/).

## Suggested reading order

1. This README.
2. `01` — internalize the simulator-first testing model (it shapes the whole API).
3. `02` then `03` — the algorithm, single-slot then log.
4. `04` and `05` when scaling throughput / changing membership becomes relevant.

## Map of the frankenpaxos code touched

```
shared/src/main/scala/frankenpaxos/
├── Actor.scala, Chan.scala, Transport.scala     # actor + I/O abstraction          → 01
├── FakeTransport.scala, NettyTcpTransport.scala  # deterministic sim vs real net    → 01
├── Serializer.scala, ProtoSerializer.scala       # pluggable codec (scalapb)        → 01
├── Simulator.scala                               # property-based test harness      → 01
├── roundsystem/RoundSystem.scala                 # round→leader assignment          → 02
├── election/, heartbeat/                         # leader election, failure detect  → 02
├── paxos/                                         # single-decree Paxos              → 02
├── multipaxos/                                    # MultiPaxos + compartmentalization→ 03, 04
├── clienttable/, statemachine/, quorums/         # dedup, SM iface, quorum systems  → 03
├── matchmakerpaxos/, matchmakermultipaxos/        # reconfiguration                  → 05
```

## Other protocols in this repo (not covered here)

frankenpaxos also implements EPaxos, Bipartisan Paxos (several variants), Fast (Multi)Paxos,
Mencius, CASPaxos, CRAQ, and Scalog. They're out of scope for `paros`, which targets the
classic Paxos → MultiPaxos lineage, but the same framework (part 1) underlies all of them, so the
testing approach carries over if we ever explore them.
