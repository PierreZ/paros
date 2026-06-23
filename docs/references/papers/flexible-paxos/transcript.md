# Flexible Paxos: Quorum Intersection Revisited

**Authors:** Heidi Howard, Dahlia Malkhi, Alexander Spiegelman (VMware Research / University of Cambridge / Technion)
**Date:** 24 Aug 2016 (arXiv:1608.06696v1). Published at OPODIS 2016.
**Source:** https://arxiv.org/abs/1608.06696

**Abstract (in full):** *"Distributed consensus is integral to modern distributed systems. The widely adopted Paxos algorithm uses two phases, each requiring majority agreement, to reliably reach consensus. In this paper, we demonstrate that Paxos, which lies at the foundation of many production systems, is conservative. Specifically, we observe that each of the phases of Paxos may use non-intersecting quorums. Majority quorums are not necessary as intersection is required only across phases. Using this weakening of the requirements made in the original formulation, we propose Flexible Paxos, which generalizes over the Paxos algorithm to provide flexible quorums. We show that Flexible Paxos is safe, efficient and easy to utilize in existing distributed systems. We conclude by discussing the wide reaching implications of this result. Examples include improved availability from reducing the size of second phase quorums by one when the number of acceptors is even and utilizing small disjoint phase-2 quorums to speed up the steady-state."*

---

## The one-sentence result

Classic Paxos requires that **all** quorums intersect, and satisfies this with majorities. But the
safety proof only ever uses intersection **between a phase-1 quorum and a phase-2 quorum**. So it is
safe to require only that *every Q1 intersects every Q2*; Q1s need not intersect each other, and Q2s
need not intersect each other. This generalization is **Flexible Paxos (FPaxos)**; Paxos is the
special case where all quorums are majorities.

## 2. Paxos recap (the baseline)

Single-value consensus over asynchronous, lossy, crash-prone processes with three roles —
**proposer, acceptor, learner**. Two phases, each (classically) needing a majority:

- **Phase 1 — Prepare & Promise.** Proposer picks a unique proposal number `p`, sends `prepare(p)`.
  An acceptor that has not promised a higher number persists `p` and replies `promise(p',v')` with its
  last accepted proposal `(p', v')` if any.
- **Phase 2 — Propose & Accept.** Once a quorum of promises is in, the proposer picks the value: the
  `v'` of the **highest** `p'` returned, or its own value if none were returned. It sends
  `propose(p,v)`; an acceptor accepts iff `p ≥` its highest promise (persisting first) and replies
  `accept(p)`. A quorum of accepts means `v` is **decided** — final, unchangeable.

Multi-Paxos: run phase 1 once to become **leader** (it is value-independent and can be batched over a
range of slots), then use phase 2 repeatedly to commit a sequence of values (slots). Because phase 2
(replication) dominates phase 1 (leader election) in frequency, phase-2 cost is what matters in steady
state.

## 3. The FPaxos observation

Name the two quorum sets `Q1` (phase 1) and `Q2` (phase 2). The required property weakens from
"all quorums intersect" to just:

> **∀ Q1 ∈ 𝒬₁, ∀ Q2 ∈ 𝒬₂ : Q1 ∩ Q2 ≠ ∅** (cross-phase intersection only).

Consequences:
- You may use **non-intersecting quorum systems** within a phase.
- The straightforward use: **shrink Q2 at the cost of growing Q1**. Since replication is the common
  path, smaller Q2 means lower latency, better throughput (disjoint acceptor sets can serve different
  proposals), and tolerance of slow acceptors — paid for by needing more acceptors to elect a new
  leader (larger Q1).
- **Progress within a phase:** if failures prevent forming Q1 but Q2 is still formable, the current
  leader keeps committing safely until a new leader is needed; if acceptors recover before the leader
  fails, no availability is lost.

## 4. Quorum systems enabled

- **Majority quorums (the freebie).** With an **even** `n`, classic Paxos uses `n/2 + 1` for both
  phases. FPaxos can drop Q2 to `n/2` (keep Q1 at `n/2 + 1`). Trivial change; lower latency, higher
  throughput, *and* better fault tolerance — if exactly `n/2` acceptors fail while the leader is up,
  FPaxos still makes progress.
- **Simple quorums.** Any acceptor counts equally; require `|Q1| + |Q2| > N`. Pick `|Q2| < N/2` and
  set `|Q1| = N − |Q2| + 1`. Tolerates up to `|Q2| − 1` failures always; between `|Q2|` and
  `N − |Q2|` failures it can keep replicating until a new leader is needed.
- **Grid quorums.** Arrange `N` acceptors in an `N₁ × N₂` grid. FPaxos can take **Q1 = a full row**
  (size `N₁`) and **Q2 = a full column** (size `N₂`) — these always cross-intersect, while same-phase
  quorums never intersect (nice for load distribution). This shrinks *both* phases relative to a
  majority. Failure tolerance becomes about *which* nodes fail, not just how many: a whole failed row
  still lets Q1 complete (recover all past decisions) and fall back to reconfiguration.
- **Thought experiments.** `|Q1| = N, |Q2| = 1`: any single acceptor learns a value in one hop, but
  recovery needs everyone up. `|Q1| = 1, |Q2| = N`: tolerate `f` failures with only `f + 1` acceptors
  (any survivor can complete phase 1).
- **Message savings:** send prepare/propose to only `|Q1|`/`|Q2|` acceptors (not all `N`), retrying
  on the rest if needed — `4N` messages → `2|Q1| + 2|Q2|`, at the cost of latency on failure.

## 5. Safety (proof sketch)

**Theorem (final agreement):** if `v` is decided with `p` and `v'` with `p'`, then `v = v'`.
Stronger form proved: if `v` is decided with `p`, then every `propose(p', v')` with `p' > p` has
`v' = v`. Proof by contradiction on the **smallest** `p' > p` violating it. Pick an acceptor `acc` in
`A̅ = Q(p,2) ∩ Q(p',1)` — nonempty by cross-phase intersection. Case-analysis on whether `acc` saw
`prepare(p')` or `propose(p,v)` first shows every branch either contradicts the protocol rules or
forces the proposer of `p'` to re-select `v` (the highest-numbered promised value), contradicting the
assumption. The crucial point: the proof **only** needs `Q(p,2) ∩ Q(p',1) ≠ ∅`, never Q1–Q1 or
Q2–Q2 intersection. A 2-page TLA⁺ spec (a minor edit of Lamport's Paxos spec) was model-checked with
disjoint quorums against this safety property.

## 6. Prototype

A naïve FPaxos built by modifying **LibPaxos3** (C, TCP/IP) — just generalize the sizes of Q1/Q2 and
send only to a quorum. Reducing Q2 lowers latency and raises throughput as expected. Notably FPaxos
beats vanilla LibPaxos even at equal quorum sizes (it messages only a quorum, not all replicas).
Example: 8 replicas, Q2 = 4 — handles more failures than Paxos *and* improves latency (42→37 ms) and
throughput (198→264 reqs/s).

## 7. Enhancements (dynamic quorums)

Safety actually needs only that a given **Q1 intersect all Q2s used with *lower* proposal numbers** —
not all possible Q2s. If a leader learns/announces which Q2s were used at lower numbers, quorum
requirements weaken further. Achieving this safely means weaving quorum-selection announcements into
leader election — "akin to Paxos reconfiguration," along the lines of Vertical Paxos. Example: with
`N = 100f`, a leader announces a fixed Q2 of size `f + 1`; all higher proposal numbers (and readers)
need only intersect that Q2, tolerating `N − f` failures.

## 8 & 9. Related work / conclusion

The observation is **orthogonal** to existing Paxos variants (Cheap Paxos, Fast Paxos, Mencius,
Ring/Multi-Ring Paxos, Chain Replication, Generalized/Egalitarian Paxos, Corfu, …) and can be folded
into them and into production systems. It revisits the *foundations* rather than adding a new variant.
By removing the requirement that replication quorums intersect, it lifts a limit on scalability and
exposes the phase-2 quorum size as a tunable trade-off between failure tolerance and steady-state
latency. (Sougoumarane independently made the same observation in a 2016 blog post.)

---

## Why this matters for `paros`

- **Quorums are a pluggable trait, and Q1/Q2 are separate.** A sans-IO Multi-Paxos core should not
  bake in "majority." Model a `QuorumSystem` with distinct read (Q1) and write (Q2) quorum predicates;
  the only invariant the core must enforce is cross-phase intersection. This is exactly the abstraction
  the frankenpaxos analysis surfaced (`quorums/QuorumSystem.scala`, `Grid.scala`) — FPaxos is the
  theory behind it.
- **Grid quorums** are the concrete win: smaller phase-2 quorums for the hot replication path. paros
  can ship majority quorums in v1 but should keep the Q1/Q2 split in the type system so grid/flexible
  quorums drop in later with no protocol change.
- **The dynamic-quorum enhancement (§7) connects to reconfiguration** — see the matchmaker-paxos
  reference, which generalizes "leader learns which prior quorums were used" into a first-class
  protocol.
- **The safety proof is quorum-system-agnostic**, which is encouraging for a learning implementation:
  get the cross-phase intersection right and the existing Paxos safety argument carries over unchanged.
