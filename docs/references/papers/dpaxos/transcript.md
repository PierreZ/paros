# DPaxos: Managing Data Closer to Users for Low-Latency and Mobile Applications

**Authors:** Faisal Nawab (UC Santa Cruz), Divyakant Agrawal, Amr El Abbadi (UC Santa Barbara)

**Date:** SIGMOD 2018 (2018 International Conference on Management of Data), June 10-15, 2018,
Houston, TX. 16 pages.
**Source:** <https://doi.org/10.1145/3183713.3196928>,
<https://www.nawab.me/Uploads/Nawab_DPaxos_SIGMOD2018.pdf>

---

## Abstract (in full)

"In this paper, we propose Dynamic Paxos (DPaxos), a Paxos-based consensus protocol to manage
access to partitioned data across globally-distributed datacenters and edge nodes. DPaxos is
intended to implement a State Machine Replication component in data management systems for the
edge. DPaxos targets the unique opportunities of utilizing edge computing resources to support
emerging applications with stringent mobility and real-time requirements such as Augmented and
Virtual Reality and vehicular applications. The main objective of DPaxos is to reduce the latency
of serving user requests, recovering from failures, and reacting to mobility. DPaxos achieves
these objectives by a few proposed changes to the traditional Paxos protocol. Most notably,
DPaxos proposes a dynamic allocation of quorums (i.e., groups of nodes) that are needed for Paxos
Leader Election. Leader Election quorums in DPaxos are smaller than traditional Paxos and expand
only in the presence of conflicts."

---

## 1. Introduction

Edge nodes (cloudlets, micro datacenters) are expected to host data management, not just caching,
for AR/VR and vehicular applications whose latency budgets the ~100 ms WAN RTT to a traditional
datacenter cannot meet. DPaxos is a Paxos-based **State Machine Replication (SMR)** protocol for
this setting, built on Flexible Paxos but adapted to the practical constraints of edge
deployments. The paper frames the problem as three goals: **(1) access locality** (serve requests
from a nearby edge node), **(2) data mobility** (partition copies follow moving users), and
**(3) flexible fault tolerance** (nearby edge nodes recover from failures).

Traditional and Flexible-Paxos-style replication rely on majority (or majority-of-a-phase)
quorums; at edge scale (potentially huge `n`), majority communication for every step is
prohibitive. DPaxos's answer is **Zone-centric Quorums**: a **zone** is a disjoint, administrator-
defined set of neighboring nodes (Figure 1, described: 8 example zones across traditional
datacenters and edge nodes, with illustrative inter-zone latencies such as 100 ms and 150 ms
labeled between distant zones), and DPaxos confines a partition's Replication communication to
its zone.

**Three techniques**, building incrementally on each other:

1. **Zone-centric Quorums**: redefine Leader Election and Replication quorums (via Flexible
   Paxos) so Replication is zone-local and as small as possible; the cost is that Leader Election
   must then intersect **all** possible Replication quorums (the **inter-intersection
   condition**), which is expensive.
2. **Expanding Quorums**: overcome the inter-intersection condition, so that Leader Election
   quorums only need to intersect **each other** (the **intra-intersection condition**), not every Replication
   quorum, because in practice a new leader only needs to beat *concurrent* leaders, not every
   theoretically possible one. Two concrete instantiations: **(2.a) Delegate Quorums** (Leader
   Election = majority of zones, each contributing a majority of its nodes) and **(2.b) Leader
   Zone Quorums** (Leader Election = a single zone, typically).
3. **Leader Handoff**: a single lightweight message lets a *cooperating* leader relinquish
   leadership to a new node, for the mobility case (as opposed to the failure case, which still
   needs Leader Election).

## 2. Paxos Background

Standard two-phase Paxos: **Leader Election** sends `prepare(p)` to a majority; an acceptor
replies `promise()` (with its last accepted `(q, vq)`, if any) iff `p` is the highest it has seen.
A leader with a majority of promises proceeds to **Replication** with the highest-numbered
received value (or its own, if none was returned), sending `propose(p', v)`; a node `accept(p')`s
iff `p'` is `≥` its highest promised proposal. A majority of accepts decides `v`. **Multi-Paxos**
elects a **prolonged leader** that skips Leader Election for a run of slots; a failure or a
deliberate move of leader location still requires a fresh Leader Election round. Since Multi-Paxos
is the prevalent variant in practice, DPaxos is built on it.

## 3. System Model

**Globally-distributed edge model.** `node`/`replica`/`datacenter` are used interchangeably; a
`zone` is an administrator-defined disjoint set of nodes. Traditional datacenters host tens of
thousands of machines; edge nodes (cloudlets, micro datacenters) are smaller.

**Mobility.** A partition's access location can move frequently (e.g. vehicular users); DPaxos
treats the eventual migration of the partition's data as a non-imminent concern as long as
redirecting requests costs only a small latency difference, so migration efficiency matters more
than migration speed.

**Fault-tolerance model.** Two failure classes: **individual datacenter outages** (frequent) and
**zone-scale (natural-disaster-style) failures** (rare: cited at **3%** of all outages affecting
more than one datacenter at once, per Gunawi et al.'s "Why does the cloud stop computing?"). DPaxos
therefore parametrizes fault tolerance with **two** values instead of one: `fd`, the tolerated
individual-datacenter failures **per zone**, and `fz`, the tolerated **zone** failures. The paper
assumes each zone has **at least `2fd + 1`** datacenters and there are **at least `2fz + 1`**
zones; both `fd` and `fz` are user-configurable.

## 4. DPaxos Design

### 4.1 Overview

Three techniques reduce quorum sizes, described above; Algorithm/Figure numbers below follow the
paper.

### 4.2 Zone-Centric Quorums

> **Definition 1 (Inter-Intersection Condition).** A Leader Election quorum must intersect with
> all Replication quorums.

This needs no protocol change beyond how quorums are defined (any Replication quorum `Qr` must
intersect any Leader Election quorum `Qle`). The **ideal Replication quorum** is the smallest
that tolerates failures: **`fd + 1` nodes in `fz + 1` zones**. Worked example (Figure 1's 8 zones,
`fd = 1`, `fz = 0`): a Replication quorum is any pair of nodes in one zone (Figure 2, described: a
Replication quorum of 2 nodes inside zone 1, alongside the much larger Leader Election quorum
needed to intersect it and every other zone's Replication quorums). Because a Replication quorum
can sit inside a single zone, the Leader Election quorum must span **`|Z| − fz`** zones and, in
each zone `i`, **`|Zi| − fd`** nodes (with 3 nodes per zone in the example, that is 2 nodes in
every zone). The inter-intersection condition suffices for correctness because any aspiring
leader's Leader Election quorum is guaranteed to intersect the current and every past leader's
Replication quorum (Figure 3, described: a leader in zone 1 decides slots `i` to `i+8` locally,
then a node in zone 4 steals leadership for slot `i+9` by winning a Leader Election quorum that
is guaranteed to include node `A`, the one node shared with zone 1's Replication quorum, which
blocks the old leader from any further commit).

**Summary:** Zone-centric Quorums alone makes Replication cheap and zone-local, but Leader
Election correspondingly expensive (must span nearly all zones). The rest of the design attacks
that cost while keeping Replication small.

### 4.3 Expanding Quorums

Observation: Paxos's safety argument only needs a Leader Election quorum to intersect the
Replication quorums that **were or are** actually in use by other leaders: not every
*possible* Replication quorum. DPaxos operationalizes this by having leaders **announce the
Replication quorum they intend to use**, called an **intent**, in their `prepare()` message; a
`promise()` carries back the list of intents the responder has accumulated (excluding intents
from `prepare()`s it did not positively answer). If an aspiring leader discovers intents from its
first round, it starts a **second round**, sending `prepare()` to those intents' Replication
quorums, so its own Leader Election quorum **expands** to intersect them (Algorithm 1, condensed:
send `prepare(p, intent)` to `Qle`; on a quorum of promises, if intents were returned, send
`prepare(p, intent)` to each; terminate/retry if nothing from the intents answers; otherwise adopt
the highest returned `(p', v')` or proceed with its own `(p, v)`).

> **Definition 2 (Intra-Intersection Condition).** Any two Leader Election quorums must intersect.

This replaces Definition 1: the *only* requirement on the initial allocation of Leader Election
quorums is that they intersect each other, which is enough to guarantee any later intent is
eventually detected. **Two instantiations:**

**4.3.1 Delegate Quorums.** A Delegate Leader Election quorum consists of a **majority of zones**,
with a **majority of nodes within each** zone in that majority: guaranteeing any two Delegate
quorums intersect (Definition 2). Figure 4 (described: a Delegate quorum spanning 5 of 8 zones,
contrasted with Flexible Paxos needing all zones when Replication is confined to one zone). A
Delegate quorum does **not** intersect every Replication quorum, only every other Delegate
quorum; any intent surfaced through that intersection triggers the second-round expansion. Worked
example (Figure 5, 8 zones, 3 nodes/zone, `fd = 1, fz = 0`, described): a zone-1 leader holds
slots `i..i+4`; a zone-4 node's majority-of-zones poll skips zone 1 but intersects the zone-1
Delegate quorum and so receives its intent, triggering one extra round to collect a single
intersecting vote from zone 1 before zone 4 takes over for slots `i+5..i+10`. The second round is
only needed when an intent is not already covered by the first round; it is also possible to
proactively over-collect votes outside the initial majority to avoid a guaranteed second round (an
optimization detailed in §4.6). A second-round promise naming an intent the first round never saw
can be safely discarded: it originates from a concurrent aspiring leader who is, by construction,
guaranteed to learn of *this* leader's intent symmetrically.

**4.3.2 Leader Zone Quorums.** Leader Election quorums shrink to **a single zone** (ideally the
zone of the current leader), called the **Leader Zone**: `QLE` is every majority in that zone, so
any two aspiring leaders' quorums trivially intersect (Definition 2) by contending for the same
zone's votes (Algorithm 2, condensed from Algorithm 1: `QLE` starts as majorities of the (initial)
Leader Zone; on learning a next Leader Zone is in transition, `QLE` becomes unions of a majority
from the current zone and a majority from the next; on learning the transition is complete, `QLE`
becomes majorities of the new zone alone). A **fixed** Leader Zone is efficient only while leaders
stay near it; DPaxos supports **moving** the Leader Zone via a three-step protocol driven by any
node `i`:

1. **Register a unique next Leader Zone.** `i` decides `Zi` as the next Leader Zone inside a
   separate, single-purpose **Leader Zone Instance** (a Paxos instance run inside the *current*
   Leader Zone, itself reconfigured to follow wherever the Leader Zone currently is). Only one
   zone can be registered as next at a time.
2. **Transition phase.** `i` asks a majority of the current Leader Zone `Zj` to (a) send back all
   intents they hold so `Zi` can take custody of them (a majority of `Zi` maintains them), (b)
   piggyback the new Leader Zone `Zi` on their `promise()`s, and (c) stop accumulating any new
   intents. During transition, an aspiring leader that learns of the pending move must get
   promises from **two** majorities, one from `Zi` and one from `Zj`.
3. **Complete the transition.** Once step 2 guarantees `Zi` holds every intent, `i` announces
   (lazily, in the background) that `Zi` is the new Leader Zone; any aspiring leader unaware of the
   move still consults the old `Zj`, which redirects it to `Zi`.

Worked example (Figure 6, 8 zones, `fd = 1, fz = 0`, described: Zone 1 starts as Leader Zone; a
node in Zone 2 wins a majority there with no prior intents and leads slots 1-6; a node in Zone 4
polls Zone 1, inherits Zone 2's intent, expands to intersect it, and leads slots 7-10; it then
registers Zone 4 as next Leader Zone via the Leader Zone Instance in Zone 1, transitions, and
announces Zone 4 as the new Leader Zone).

**4.3.3 Safety.** DPaxos's safety rests on adapting Flexible Paxos's Theorem 1 (if `v` is decided
at proposal `p`, any later `propose(p2, v2)` with `p2 > p` has `v2 = v`), whose original proof
needs the *inter*-intersection between a Replication quorum `Qr^p` and a later Leader Election
quorum `Qle^p2`. DPaxos's quorums satisfy only the *intra*-intersection condition by definition, so
it proves the needed inter-intersection is still *enforced*, via expansion:

> **Theorem 2.** Consider a Replication quorum with proposal id `p`, `Qr^p`, and a Leader Election
> quorum with proposal id `p2`, where `p < p2`. DPaxos ensures `Qr^p ∩ Qel^p2 ≠ ∅` (where `Qel^p2`
> is the Leader Election quorum **after** any expansion, as opposed to `Qol^p2`, the original,
> pre-expansion one).
>
> *Proof (by contradiction).* Assume `Qr^p ∩ Qel^p2 = ∅`. By Definition 2, the original Leader
> Election quorums at `p` and `p2` intersect at some node `A` (`Qol^p ∩ Qol^p2 ≠ ∅`). Two cases:
> **Case 1**: `A` received `prepare(p, intent=Qr^p)` before `prepare(p2, intent=Qr^p2)`. Then `A`
> returns intent `Qr^p` to the `p2` proposer, which must expand `Qol^p2` to `Qel^p2` to intersect
> it, contradicting the assumption. **Case 2**: `A` received `prepare(p2, ...)` first. Then `A`
> does not promise `p` (since `p < p2`), so the Leader Election at `p` fails and `Qr^p` is never
> used, contradicting the assumption that it was. □

This suffices by Flexible Paxos's own proof, which only requires `Qr^p ∩ Qle^p2 ≠ ∅` for whatever
quorum ends up actually used.

**4.3.4 Intents Garbage Collection.** Intents accumulate at nodes across Leader Election rounds
(failed *and* successful-but-superseded attempts), inflating `promise()` size and the number of
zones/nodes a future leader must intersect with. A separate **garbage collector** process
(Algorithm 3, condensed): maintain a threshold `P`, initially 0; repeatedly pick a node `i`
(round-robin in DPaxos's implementation), poll `Pi`, the **highest proposal id for which `i`
received a `propose()`** (not merely a `prepare()`); if `Pi > P`, raise `P` and asynchronously
broadcast it. Any node receiving a new `P` discards every intent with a lower proposal id,
regardless of whether that intent's leader election succeeded or failed. More than one garbage
collector may run concurrently; collectors can stop and resume arbitrarily.

> **Theorem 3.** An intent's Replication quorum cannot accept any new `propose()` with proposal
> id `p` once `p < P`.
>
> *Proof sketch* (full proof in Appendix C): by contradiction, assume the Replication quorum of an
> obsolete intent `p < P` still accepted a `propose(p, ...)`. There is a node `n(p)` that won a
> Leader Election at `p`, and (since `P` was raised) a node `n(P)` that won one at `P`. By
> Definition 2, their Leader Election quorums share a node `L`, which promised both, `p` first
> (since `p < P`). Two cases: either `L` still held `n(p)`'s intent when `n(P)`'s `prepare`
> arrived, in which case `n(P)` expanded to intersect `n(p)`'s Replication quorum and so some node
> there already refused `n(p)`'s stale proposal (a contradiction), or `L`'s copy of the intent had
> *already* been garbage collected by an earlier node that reasoned the same way, which recurses
> to the same contradiction. □

Worked example (Figure 7, described: 8 zones, Delegate Quorums; `z1` wins proposal `p=3` over
zones 1-5 while a concurrent, lower `p=2` attempt by `z8` fails to get a majority of zones; `z1`
decides slots 1-5 locally; while its slot-6 `propose` is delayed, `z6` wins `p=4` by expanding into
zones 1 and 8, inheriting both stale intents; once a collector polls zone 6, `P` becomes 4 and both
stale intents are purged cluster-wide; `z1`'s delayed slot-6 `propose` then arrives too late: some
node in `z6`'s Leader Election quorum already refuses to honor ballot 3's Replication quorum, so
the stale proposal cannot be re-accepted even though no node consulted `z1`'s Replication quorum
directly).

Other garbage collection strategies are mentioned but not adopted: Stoppable Paxos-style
collection at predetermined "stop" points (one-shot/periodic rather than continuous: discussed
further in Appendix B.1); discovering an obsolete intent as soon as any one of its Replication
quorum's nodes promises a higher proposal; or a newly elected leader immediately broadcasting its
own `P` without waiting for a poll.

### 4.4 Leader Handoff

Two distinct motivations for a new leader: **fault tolerance** (the current leader failed) and
**mobility** (the workload moved but the current leader is still alive). Leader Handoff optimizes
the second case by treating "leader" as a **logical role**, not something physically bound to one
node: a leader can use any Replication quorum and need not stay in one location.

**Exact rules, as stated in the paper:**

- Handoff is a single **`relinquish()`** message from the current leader to the new leader,
  carrying the current leader's state and the (possibly unbounded) set of slots being relinquished.
- **A leader sends this message at most once for any given slot**, and **after sending it, it
  refrains from acting as leader for those slots.**
- **If the `relinquish()` message is lost, neither the old nor the new leader may act as leader**
  for those slots: only a fresh Leader Election round can recover, exactly as under failure.
- Combined with Expanding Quorums, the new leader **may only use Replication quorums already
  declared by the relinquishing leader's intent(s)**: motivating declaring more than one intent
  (§4.6) so a handoff (or a slow/inaccessible intersecting node) has a fallback without forcing a
  fresh Leader Election.
- Leader Handoff is explicitly framed as **general to Paxos variants**, not specific to DPaxos.

### 4.5 Read Leases

To serve read-only requests without a full Replication round, DPaxos adopts a **leader-based read
lease** (as in Spanner, EPaxos, Chubby), rather than a majority-voted or quorum-based lease
(Moraru et al.'s Paxos Quorum Leases), because DPaxos targets spatially-local workloads where the
leader-based approach is the more natural fit. Safety needs that no two nodes hold a read lease
concurrently at once; DPaxos achieves this while restricting lease-renewal communication to a
single **Replication** quorum (smaller than a Leader Election quorum) by making lease
request/vote **implicit**:

- Lease requests/votes piggyback on `propose`/`accept` messages (no extra round trip).
- An `accept` (a lease vote) carries an implicit promise **not to respond to `prepare()`** (i.e.,
  not to participate in Leader Election) until the lease expires.
- A leader can therefore acquire or renew its lease with votes from its Replication quorum alone;
  safety holds because no node can win a Leader Election promise from the current leader's
  Replication quorum before the lease lapses, and garbage collection cannot threaten a live lease's
  intent (no node can be elected before the lease expires, so that intent is never collectible).

### 4.6 Summary and Practical Considerations

Bulleted recap, as given:

- A DPaxos **Replication** quorum is any `fd + 1` nodes in `fz + 1` zones.
- A **Flexible Paxos** (and plain Zone-centric, pre-Expanding) Leader Election quorum must
  intersect all Replication quorums: all zones minus `fz`, and in each zone, all nodes minus `fd`.
- A **Delegate** quorum: a majority of zones, with a majority of nodes from each of those zones.
- A **Leader Zone** quorum: a majority of nodes in the Leader Zone(s) (extendable beyond one zone
  to tolerate zone failures, needing a majority of Leader Zones too; the paper's exposition stays
  with a single-zone Leader Zone for clarity).
- **Leader Handoff** relinquishes all or part of a leader's slots via one lightweight message;
  failure still requires a full Leader Election round.

**Configuration** of `fd`/`fz` and the choice among Delegate / Leader Zone / Handoff is left to the
administrator based on the workload's spatial-locality characteristics; the paper does not solve
automatic configuration, though it notes global-scale placement techniques (Zakhary, Nawab,
Agrawal, El Abbadi) could be adapted to it.

**Multiple intents** per aspiring leader are allowed: declaring two intents lets a leader pick
whichever Replication quorum is faster/reachable without a fresh Leader Election if one becomes
slow, at the cost of a bigger intersection requirement for future leaders (who must then intersect
*every* declared intent). The trade-off is left as a workload-dependent design decision.

**Consolidating rounds.** Expanding Quorums' two rounds (first to `Qle`, then to the discovered
intents' Replication quorums) send identical message types and can be merged into one round when
there is enough information to predict the intents in advance: i.e. `prepare()` can be sent to
`Qle` and the predicted `Qi` simultaneously.

## 5. Evaluation

### Setup

Real deployment on **seven Amazon AWS datacenters, each treated as one zone**: **California (C),
Virginia (V), Oregon (O), Tokyo (T), Ireland (I), Singapore (S), Mumbai (M)**. **Three nodes per
datacenter**, with an artificial **10 ms** delay added between nodes modeled as being in the same
zone but on different edge locations (to emulate intra-zone edge dispersion). Machines: **`m4.large`**
(2 vCPUs, 8 GB RAM) on Linux. Workload: small OLTP transactions, **5 operations each, random keys
from 1,000,000**, 50-byte values, half read half write, all read-write transactions (read-only
transactions are evaluated separately in Appendix A.2). Default fault tolerance throughout:
**`fd = 1, fz = 0`** (tolerates one datacenter failure). Each experiment runs **1 minute**
(confirmed no significant difference for longer runs). Baselines: **Multi-Paxos, Flexible Paxos**,
and an **optimal leaderless Paxos** stand-in (majority Replication quorum, which may be unsafe but
gives a best-case leaderless benchmark; compared against Egalitarian Paxos / Fast Paxos / MDCC
conceptually).

**Table 1: Average Round-Trip Time (ms) between each pair of the 7 datacenters (zones):**

|   |  C  |  O  |  V  |  T  |  I  |  S  |  M  |
|---|-----|-----|-----|-----|-----|-----|-----|
| **C** |   0 |  19 |  62 | 113 | 134 | 183 | 249 |
| **O** |  19 |   0 | 117 | 104 | 133 | 161 | 221 |
| **V** |  62 | 117 |   0 | 172 |  81 | 244 | 182 |
| **T** | 113 | 104 | 172 |   0 | 214 |  67 | 124 |
| **I** | 134 | 133 |  81 | 214 |   0 | 179 | 120 |
| **S** | 183 | 161 | 244 |  67 | 179 |   0 |  58 |
| **M** | 249 | 221 | 182 | 124 | 120 |  58 |   0 |

### 5.1 Replication Phase Performance

Each prolonged leader is co-located with its own partition's zone; each of the 7 zones serves its
own partition (emulating 7 partitions), and each measures latency/throughput for its own
Replication phase (Figure 8, described: per-datacenter latency and throughput bars for DPaxos,
Flexible Paxos, Multi-Paxos). **DPaxos and Flexible Paxos decide values at an average 11-13 ms in
every location** (identical Replication quorums); **Multi-Paxos ranges from 91 ms (Virginia) to
282 ms (Mumbai)**, since its majority vote's latency depends on the proposer's location.
Throughput: **DPaxos and Flexible Paxos reach 75.8-85.2 KB/s everywhere**; **Multi-Paxos ranges
from 3.5 KB/s (Mumbai) to 10.9 KB/s (Virginia)**. **Overall, DPaxos and Flexible Paxos average 23×
the throughput of Multi-Paxos**, because their Replication cost is independent of nodes outside
the zone, while Multi-Paxos's majority cost grows with the deployment's geographic spread.

### 5.2 Leader Election Performance

Comparing **Leader Zone**, **Delegate**, **Flexible Paxos**, **Multi-Paxos**, and **Leader
Handoff** Leader Election latency as observed by an aspiring leader in California, varying the
previous leader's location, with no outstanding ungarbage-collected intents beyond the previous
leader's own (Figure 9, described: Leader Election latency vs. previous-leader datacenter):

- **Leader Zone** takes one round to the previous leader's zone: **11 ms** (same zone) up to
  **267 ms** (Mumbai).
- **Delegate Quorums** and **Multi-Paxos** Leader Election take one round to the closest zones
  regardless of the previous leader's location: **149-152 ms**, faster than Leader Zone only when
  the previous leader was in Singapore or Mumbai.
- **Flexible Paxos** Leader Election is the most expensive, collecting votes from **all** zones:
  **262 ms** (the C-Mumbai RTT) in this experiment. Leader Zone only matches this high cost when
  the previous leader was in Mumbai.
- **Leader Handoff** has similar latency characteristics to Leader Zone, but requires the previous
  leader's cooperation (unlike all of the above, which work after a failure too).

### 5.3 Comparing with Leaderless Paxos

Two experiments, proposer in California (Figure 10, described):

**(a) Overhead of Leader Election vs. an optimal leaderless baseline.** Varying DPaxos's
Leader-Election-invocation rate: **0%** (**12 ms**, pure Replication-phase latency, i.e. a
prolonged leader that never fails or moves), **50%** (17-147 ms depending on the previous
leader's zone), **100%** (24-286 ms, full Leader Election overhead every request). Optimal
leaderless Paxos:
**152 ms** flat. **DPaxos beats leaderless Paxos even at a 50% Leader Election rate**, regardless
of where the previous leader was; at 100%, leaderless only wins if the previous datacenter was
Singapore or Mumbai.

**(b) Overhead of remote (non-local) requests**, leader fixed in California, varying the share of
requests originating elsewhere (0/50/100%): DPaxos best case (no remote requests) is **12 ms**; at
100% remote, the farthest case (Mumbai) is **260 ms**, otherwise 22-195 ms. Leaderless Paxos is
**152 ms** at 0% remote, **122-217 ms** at 50%, **91-282 ms** at 100%. **Leaderless Paxos beats
DPaxos only in the 100%-remote-from-Mumbai case**; otherwise the gap shrinks but favors DPaxos.
General conclusion: remote requests measurably erode DPaxos's advantage, and leaderless variants
can win outright under low-locality workloads.

## 6. Conclusion

DPaxos is a Paxos-based protocol purpose-built for edge data management, combining **(1)
Zone-centric Quorums** (Flexible Paxos quorums made small and zone-local), **(2) Expanding
Quorums** (dynamically-growing Leader Election quorums that need only intersect each other, via
Delegate or Leader Zone instantiations), and **(3) Leader Handoff** (lightweight cooperative
leadership transfer for mobility). A 7-datacenter real deployment shows these yield significant
performance gains over Multi-Paxos and leaderless alternatives for spatially-local workloads.

## Appendix highlights (A, B, C)

- **A.1 Batching:** increasing batch size 1 KB -> 100 KB raises throughput **68×** for DPaxos,
  **64×** for Flexible Paxos, **25×** for Multi-Paxos (most of the gain by 50 KB); latency rises
  from 11-12 ms to 18 ms for DPaxos/Flexible Paxos, and from 95 ms to 268 ms for Multi-Paxos, which
  also thrashes above 50 KB (DPaxos/Flexible Paxos only lose 1.5× beyond that point).
- **A.2 Read-only requests:** with master leases, read-only latency is **< 1 ms** regardless of
  batch size, vs. an 11 ms read-modify floor; at 1 MB batches, a 50%-read-only workload gets
  **75%** higher throughput and a 95%-read-only workload gets **313%** higher throughput than an
  all-read-modify workload.
- **A.3 Multi-programming level:** raising concurrent in-flight slots from 1 to 8 improves
  throughput by **86%** (DPaxos), **77%** (Flexible Paxos), **71%** (Multi-Paxos, which thrashes at
  level 4).
- **A.4 Stale intents:** with garbage collection disabled and intents deliberately spanning 1-7
  zones, Leader Election latency with the two-phase (unconsolidated) Expanding Quorum ranges
  **22-270 ms**; consolidating the two rounds into one (§4.6) ranges **11-259 ms**.
- **B. Related work** situates DPaxos against majority-based multi-datacenter Paxos variants
  (EPaxos, MDCC/Fast Paxos), Flexible Paxos (the direct theoretical ancestor), and reconfiguration-
  based / hierarchical approaches (Vertical Paxos, Stoppable Paxos, Cheap Paxos, ZooKeeper-as-
  control-layer). It explicitly cross-references **WPaxos**: "DPaxos is similar to WPaxos in that
  they utilize Flexible Paxos quorums to commit in nearby nodes rather than a majority. WPaxos
  proposes a novel object stealing technique... DPaxos can adopt this method to increase its
  adaptability to access locality. Likewise, WPaxos can also adopt DPaxos's Expanding Quorums and
  Leader Handoff approaches to overcome the expensive Leader Election inherited from Flexible
  Paxos."
- **C. Garbage collection correctness** gives the full case-by-case proof of Theorem 3 (summarized
  in §4.3.4 above).

---

## Why this matters for `paros`

- **Expanding/Delegate/Leader Zone quorums are not adopted.** paros keeps classic FPaxos-style
  cross-phase intersection (every Phase-1 quorum vs. every Phase-2 quorum, never only
  quorum-vs-quorum) judged entirely through `membership.rs`'s phase-split predicates
  (`has_phase1_quorum` / `has_phase2_quorum`); DPaxos's intra-intersection relaxation, its
  `intent`-tracking and its garbage collector for stale intents have no counterpart because paros
  never ships a Leader Election quorum smaller than its intersection obligation in the first
  place. `docs/architecture.md` §5 reaches the WPaxos-adjacent conclusion for the same reason:
  shrinking Phase-1 at the expense of zone survival is a trade paros's single-region,
  multi-AZ deployment doesn't need to make.
- **Leader Handoff (§4.4) maps directly onto `ColocatedNode::relinquish_to`**, restated in
  `docs/analysis/consensus/dpaxos-leader-handoff.md`. The paper's exact rules line up one for one
  with that note's structural safety argument: "a leader sends `relinquish()` at most once for any
  slot, and refrains from acting as leader for those slots afterward" is the note's "the old
  leader relinquishes each authority at most once, and once it has, it never exercises that
  authority again" (the whole of the Paxos Phase-2 safety argument, per that note); "if the
  message is lost, neither leader may act, only a Leader Election round can recover" is the note's
  "a failed handoff costs availability, never safety" plus its fence argument (`become_follower`
  happens synchronously with queuing `Relinquish`, so a lost message just leaves an ordinary
  election to run). The paper frames Handoff as general to any Paxos variant, not DPaxos-specific,
  which is exactly how paros's `node/handoff.rs` treats it (independent of matchmakers,
  reconfiguration, or any particular deployment shape).
- **One place paros goes beyond the paper.** DPaxos's rule ("at most once per slot, then refrain")
  is sufficient for a single hop but the design note records that paros's *simulation* found a gap
  the paper does not address: a duplicated/delayed `Relinquish` can let a node that already
  stepped down re-install a stale authority if handoffs chain (A -> C -> D). paros's fix is **not**
  a durable fence (which the paper's model implicitly assumes is unnecessary, since it does not
  discuss multi-hop replay) but a stricter structural rule: only the ballot's original Phase-1
  minter may ever relinquish it (`LeadershipOrigin::Elected`), so a chain of handoffs costs an
  ordinary election, never a second relinquish. This is a concrete instance of paros's
  "simulation-driven development" doctrine finding an edge case a paper's prose proof does not
  cover.
- **Zone-centric Replication quorums (`fd + 1` nodes in `fz + 1` zones) parallel paros's
  `AcceptorConfig` zone labels** (§3.4 Tenant modes, §5 Failure model and zones of
  `docs/architecture.md`): both tie a quorum's shape to zone membership recorded with the
  configuration rather than read live. paros does not adopt DPaxos's *dynamic* Leader Zone
  migration protocol (the three-step register/transition/complete sequence over a dedicated
  Leader Zone Instance) because paros has no analogous "second Paxos instance just to move where
  leadership is allowed to start": `relinquish_to` moves the leader directly, and
  `Reconfigure`/matchmaker-quorum reconfiguration (`docs/architecture.md` §3.3 Coordinators and
  placement, and AGENTS.md's "Matchmaking, reconfiguration, GC, generations" section) is the
  mechanism for moving where the *acceptors* themselves live.
- **Read leases (§4.5) are not adopted.** paros's reads are leaderless quorum reads
  (`quorum_read.rs`, the Compartmentalized Paxos-style read `docs/architecture.md` §2.5 describes:
  a Phase-1 quorum vote on watermarks, no leader on the read path), not a leader-local lease. The
  implicit lease-via-accept-message technique (no extra round trip, lease vote piggybacked on the
  ordinary Phase-2 ack) has no counterpart there.

## Selected references (from the paper's bibliography)

- Lamport, *The Part-Time Parliament* (1998); Lamport, *Paxos Made Simple* (2001): the base
  protocol DPaxos's §2 recap follows.
- Howard, Malkhi, Spiegelman, *Flexible Paxos: Quorum Intersection Revisited* (2016): the
  theoretical foundation for Zone-centric Quorums and both intersection conditions
  (`docs/references/papers/flexible-paxos/`).
- Ailijiang, Charapko, Demirbas, Kosar, *WPaxos* (arXiv:1703.08905, cited here as reference [2]):
  the sibling flexible-quorum WAN protocol, cross-referenced extensively in §B.1
  (`docs/references/papers/wpaxos/`).
- Moraru, Andersen, Kaminsky, *There Is More Consensus in Egalitarian Parliaments* (EPaxos, SOSP
  2013) and *Paxos Quorum Leases: Fast Reads Without Sacrificing Writes* (SoCC 2014): the
  leaderless comparison point and the quorum-lease alternative DPaxos's leader-based lease departs
  from.
- Gray & Cheriton, *Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache
  Consistency* (1989): the master-lease approach DPaxos's read lease is inspired by.
- Lamport, Malkhi, Zhou, *Vertical Paxos and Primary-Backup Replication* (2009); *Reconfiguring a
  State Machine* (Stoppable Paxos, 2010); Lamport & Massa, *Cheap Paxos* (2004): the
  reconfiguration-based alternatives surveyed in §B.1.
- Gunawi et al., *Why Does the Cloud Stop Computing? Lessons from Hundreds of Service Outages*
  (SoCC 2016): source of the "3% of outages span more than one datacenter" figure behind the
  `fd`/`fz` split.
- Corbett et al., *Spanner* (OSDI 2012); Burrows, *The Chubby Lock Service* (OSDI 2006): sources
  of the leader-based read-lease pattern DPaxos's §4.5 follows.
