# WPaxos: Wide Area Network Flexible Consensus

**Authors:** Ailidani Ailijiang, Aleksey Charapko, Murat Demirbas, Tevfik Kosar (Department of
Computer Science and Engineering, University at Buffalo, SUNY)

**Date:** IEEE Transactions on Parallel and Distributed Systems (TPDS), 2019.
arXiv:1703.08905v4 [cs.DC], revised 3 Apr 2019. 12 pages.
**Source:** <https://arxiv.org/abs/1703.08905>

Note on authorship: `docs/architecture.md`'s bibliography (near line 1086) gives the fourth
author as "Tasci". The paper's own byline and its listed e-mails
(`ailidani, acharapk, demirbas, tkosar @buffalo.edu`) both say **Kosar**. This transcript follows
the PDF; see "Unresolved issues" below.

---

## Abstract (in full)

"WPaxos is a multileader Paxos protocol that provides low-latency and high-throughput consensus
across wide-area network (WAN) deployments. WPaxos uses multileaders, and partitions the
object-space among these multileaders. Unlike statically partitioned multiple Paxos deployments,
WPaxos is able to adapt to the changing access locality through object stealing. Multiple
concurrent leaders coinciding in different zones steal ownership of objects from each other using
phase-1 of Paxos, and then use phase-2 to commit update-requests on these objects locally until
they are stolen by other leaders. To achieve fast phase-2 commits, WPaxos adopts the flexible
quorums idea in a novel manner, and appoints phase-2 acceptors to be close to their respective
leaders. We implemented WPaxos and evaluated it on WAN deployments across 5 AWS regions. The
dynamic partitioning of the object-space and emphasis on zone-local commits allow WPaxos to
significantly outperform both partitioned Paxos deployments and leaderless Paxos approaches."

---

## 1. Introduction

Classic Paxos deployments depend on one centralized leader (Chubby/Paxos, ZooKeeper/Zab,
etcd/Raft), which does not deal with write-intensive WAN scenarios well. Leaderless (EPaxos) and
static multi-leader (Spanner, ZooNet, Bizur) approaches were proposed to remove the single-leader
bottleneck, but EPaxos's opportunistic fast-quorum commit still needs roughly 3/4 of the
acceptors (footnote 1: for a deployment of size `2F+1`, fast-quorum is `F + ⌊(F+1)/2⌋`) and a
second round on conflicts, and static partitioning pays WAN latency persistently for any object
mapped to a remote zone.

**Contribution.** WPaxos is a multileader Paxos protocol using **flexible quorums** (Howard,
Malkhi, Spiegelman) in a novel way: it appoints multiple concurrent leaders across the WAN, each
owning a disjoint subset of the object space, so that each object gets its own commit log and
per-object linearizability. Phase-2 acceptors are chosen close to the leader, so steady-state
commits stay local. Leadership moves between zones by **object stealing**: a concurrent leader in
another zone runs phase-1 of Paxos to take over an object, then phase-2 to commit locally until
the object is stolen again. Because stealing is phase-1 itself, WPaxos needs no separate
configuration/relocation service (unlike Spanner's `movedir` or Vertical Paxos's master). The
protocol is modeled in TLA+/PlusCal and its consistency property verified by model checking
(footnote 2: the spec is at `github.com/ailidani/paxi/tree/master/tla`, not inspected for this
transcript; see "Unresolved issues").

**Headline results** (Go implementation, Paxi framework, 5 AWS regions): WPaxos achieves **15×**
faster average request latency than EPaxos at ~70% access locality, and **39×** faster at ~90%
locality, in some regions. Under a steady 10,000 req/s with ~70% locality, WPaxos is **9×** faster
on average latency and **54×** faster on median latency than EPaxos. WPaxos also tolerates leader
failure gracefully: other leaders absorb the failed leader's objects via object stealing, so
safety is upheld through node failure/recovery, message loss, and asynchronous concurrent
execution without a separate failover mechanism.

## 2. Related Work

**2.1 Paxos protocol.** Three phases (Fig. 1, described: a leader self-appoints as command leader
→ phase-1a `Propose`/phase-1b `Promise` exchange a ballot `b` → phase-2a `Accept`/phase-2b
`Accepted` exchange a value `v` → phase-3 `Commit`). In phase-1 a node proposes ballot `b`; other
nodes accept only if `b` is the highest seen, and a leader needs a majority of accepts to proceed.
Phase-2 also needs a majority ack, and once committed a value cannot be lost by any future leader.
Multi-Paxos reuses one leader across many phase-2 rounds (slots) to avoid repeated phase-1; a new,
higher-ballot leader rejects the old leader's phase-2 and takes over.

**2.2 Paxos variants.** The paper classifies non-Byzantine consensus into five architectures
(Fig. 2, described): **(a) single-leader** (Multi-Paxos, Raft; Mencius rotates the leader
round-robin to balance load); **(b) multi-leader** (M²Paxos, ZooNet: parallel commands across
disjoint conflict domains, partial not total order); **(c) multi-leader multi-quorum**, WPaxos's
own class, where different leaders use different quorums as long as inter-quorum communication
preserves safety: the paper notes **DPaxos cites the WPaxos technical report and adopts a
similar protocol for the edge domain**; **(d) hierarchical** (WanKeeper, Vertical Paxos: a
higher-level leader/quorum assigns conflict-domain ownership to lower-level ones, with
quorum specialization, unlike WPaxos's flat setup); **(e) leaderless** (EPaxos: any replica
opportunistically proposes, with a second round only on detected conflicts).

## 3. WPaxos Overview

Nodes communicate by asynchronous message passing, deployed in **zones** (the unit of
availability isolation: anything from a cluster/datacenter to a geographic region). Each node is
identified by `(zone ID, node ID)`, i.e. `Nodes ≜ 1..Z × 1..N` (note: this `N`, the per-zone
node-id range in the `Nodes` tuple, is reused later for the *total* node count in the `Fmax`
formula, while §3.1 introduces `l` for the per-zone node count; an inconsistent overload of `N`
across the paper, see "Unresolved issues"). Every node keeps a sequence of instances ordered by
an increasing slot number, committed under a ballot; each ballot has a unique leader, constructed
as a lexicographically-ordered `⟨counter, leader-id⟩` pair (`Ballots ≜ Nat × Nodes`), so ballots
are unique, totally ordered, and self-identify their leader.

### 3.1 WPaxos Quorums

WPaxos builds on **flexible quorums**: Paxos's "all quorums intersect" requirement weakens to
"only quorums from different phases intersect" (phase-1 quorums `Q1` need only intersect phase-2
quorums `Q2`), letting `Q1` grow so `Q2` can shrink (phase-2, the common case, is cheaper; phase-1,
the rare leader-election case, is more expensive).

> **Definition 1.** A quorum system over the set of nodes is safe if the quorums used in phase-1
> and phase-2, named `Q1` and `Q2`, intersect. That is, `∀q1 ∈ Q1, q2 ∈ Q2 : q1 ∩ q2 ≠ ∅`.

WPaxos derives its quorum system from a **grid** layout: rows are `Q1`, columns are `Q2`
(Fig. 3a, described: a 4-by-3 grid, `fn = fz = 0`, with one `Q1` highlighted as a full row and one
`Q2` as a full column). A grid's attractive property is that `|Q1| + |Q2|` need not exceed `N`
(total acceptors) to guarantee intersection, because rows and columns always cross. WPaxos
generalizes this with two parameters: **`fz`**, the number of **zone** failures tolerated, and
**`fn`**, the number of **node** failures a single zone can tolerate before losing availability.
To tolerate `fn` crash failures per zone, WPaxos picks `fn + 1` nodes in a zone (over `l` nodes in
that zone), regardless of row position; to tolerate `fz` zone failures across `Z` zones, `q1 ∈ Q1`
is selected from `Z − fz` zones and `q2 ∈ Q2` from `fz + 1` zones.

**The printed TLA+-style definitions (quoted verbatim):**

```
Q1 ≜ {q ∈ SUBSET Nodes :
      Cardinality(q) = (fn + 1) × (Z − fz) ∧
      ¬∃k ∈ SUBSET q : ∀i, j ∈ k : i[1] = j[1] ∧ Cardinality(k) > fn + 1}

Q2 ≜ {q ∈ SUBSET Nodes :
      Cardinality(q) = (l − fn) × (fz + 1) ∧
      ¬∃k ∈ SUBSET q : ∀i, j ∈ k : i[1] = j[1] ∧ Cardinality(k) > l − fn}
```

(`SUBSET S` is the set of subsets of `S`; `i[1]` is a node's zone component, so the second
conjunct says no all-same-zone subset of `q` exceeds `fn + 1`, resp. `l − fn`, members, i.e. a
quorum caps, but does not pin, how many of its members any one zone contributes.)

> **Lemma 1.** WPaxos `Q1` and `Q2` quorums satisfy the intersection requirement (Definition 1).
>
> *Proof.* (1) WPaxos `q1`s involve `Z − fz` zones and `q2`s involve `fz + 1` zones; since
> `Z − fz + fz + 1 = Z + 1 > Z`, there is at least one zone selected by both quorums. (2) Within
> the common zone, `q1` selects `fn + 1` nodes and `q2` selects `l − fn` nodes out of `l` nodes
> forming a zone. Since `l − fn + fn + 1 > l`, there is at least one node in the intersection. □

Figure 3b (described) shows the `fn = fz = 1` case on the same 4-by-3 grid: each zone has 3 nodes,
each `q2` takes 2 of 3 nodes from 2 zones, and each `q1` spans 3 of 4 zones taking any 2 nodes per
zone; the paper notes a 2-row `q1` (rather than 1-row) costs negligible latency and buys more
fault tolerance.

**Fault count depends on topology, not just size.** Because quorums come from a grid, *which*
nodes fail matters (unlike EPaxos's set-based quorums). The paper defines:

- `Fmin = Min(Cardinality(Q2), Cardinality(Q1)) − 1`, the worst placement, where faults
  concentrated on one `Q2` wipe it (and, since every `Q1` intersects every `Q2`, wipe every `Q1`
  too); symmetrized for whichever of `Q1`/`Q2` is smaller.
- `Fmax = N − Cardinality(Q1) − Cardinality(Q2) + (fz + 1) × (fn + 1)`, the best placement, where
  faults miss a union of one `Q1` and one `Q2`, leaving at least one of each intact; derived by
  subtracting `|Q1|` and `|Q2|` from `N` and adding back the maximum possible overlap between a
  `Q1` and a `Q2`.

For the 4-by-3 grid with `fn = fz = 0` (Fig. 3a): `Fmin = 2`, `Fmax = 6`. For `fn = fz = 1`
(Fig. 3b): `Fmin = 3`, `Fmax = 6`.

### Checking the architecture.md claim: does the printed definition intersect?

`docs/architecture.md` (bibliography, near line 1086) states: *"The printed TLA+ quorum
definition does not intersect; only the floor form the proof uses is sound."* This transcript
verified that claim against the printed `Q1`/`Q2` set-builders above, independently of the GitHub
`paxi/tla` specification (not inspected; see "Unresolved issues").

**Confirmed.** The printed set-builders only *cap* a zone's contribution to a quorum from above
(`≤ fn + 1` for `Q1`, `≤ l − fn` for `Q2`); they do not force a quorum to spend *exactly* that cap
in *exactly* the minimum number of zones. A quorum satisfying the cardinality equation can
legally spread itself over *more* zones than the minimum, under-filling some of them. Lemma 1's
proof silently assumes the minimum-zones, cap-filled shape depicted in Figure 3 (each `q1` uses
exactly `Z − fz` zones with exactly `fn + 1` nodes each; each `q2` uses exactly `fz + 1` zones with
exactly `l − fn` nodes each): call this the **floor form**. Step (1) of the proof (a shared zone
exists) survives for the general printed definition, because the per-zone cap still forces at
least `Z − fz` (resp. `fz + 1`) zones to be touched. Step (2) (a shared *node* exists inside that
zone) does not survive in general, because the printed definition lets a quorum's contribution to
the shared zone fall below the cap.

**Counterexample, built on the paper's own Figure 3b grid** (`Z = 4` zones of `l = 3` nodes each,
`fn = fz = 1`, zones `A, B, C, D`): `|Q1| = (fn+1)(Z−fz) = 2×3 = 6` with per-zone cap `2`;
`|Q2| = (l−fn)(fz+1) = 2×2 = 4` with per-zone cap `2`. Take

```
q1 = {A1, A2, B1, C1, D1, D2}   (zone counts: A=2, B=1, C=1, D=2, all ≤ 2, total 6)
q2 = {A3, B2, C2, D3}          (zone counts: A=1, B=1, C=1, D=1, all ≤ 2, total 4)
```

Both are admitted by the printed `Q1` and `Q2` (cardinality and per-zone-cap conjuncts both hold),
yet `q1 ∩ q2 = ∅` zone by zone (`{A1,A2}` vs `{A3}`, `{B1}` vs `{B2}`, `{C1}` vs `{C2}`, `{D1,D2}`
vs `{D3}`). So the printed definition admits a non-intersecting `(q1, q2)` pair: **Definition 1
fails for it**. The proof's prose form: each `q1` built from exactly `fn + 1` nodes in each of
exactly `Z − fz` zones, each `q2` from exactly `l − fn` nodes in each of exactly `fz + 1` zones
(the "floor form"): is the stronger, sound definition the Lemma actually needs; the printed
set-builder is a looser superset of it.

### 3.2 Multi-leader

Every node can lead a disjoint subset of objects concurrently, each with its own ballot/slot
state.

### 3.3 Object Stealing

To steal an object from a remote leader, a node consults its cache for the last known ballot and
runs phase-1 on some `q1 ∈ Q1` with a larger ballot; stealing succeeds in one phase-1 attempt if
the candidate can out-ballot the current leader (assuming an up-to-date cache and no concurrent
phase-1 on the same object). Once stolen, the old leader cannot act further: even a node outside
the deciding `q1` learns of the higher ballot because the *intersecting* node in any future `q2`
will reject old-ballot operations. In-flight but uncommitted commands for the object are
recovered by the new leader from the `"1b"` replies.

**Ballots are kept per object, not per leader**, to isolate stealing: a single per-leader ballot
would force out-balloting *every* object of a remote leader to steal just one, causing **leader
dueling** (two nodes racing to steal different objects from each other with ever-higher ballots).
Per-object ballots reduce but do not eliminate dueling (it can still occur when two leaders chase
the *same* object from a third); two safeguards mitigate it: **(1)** ties in the ballot counter
are broken by zone ID then node ID, and **(2)** a random back-off is used if dueling restarts
anyway.

Compared to Spanner's `movedir` or Vertical Paxos's master-mediated relocation (three WAN round
trips to change leadership there), WPaxos needs only **one** WAN communication, because stealing
is ordinary phase-1.

## 4. WPaxos Algorithm

Every node keeps, per object `o`: `ballots[o]` (last-known ballot, initially `⟨0, self⟩`),
`slots[o]` (highest used slot, from 0), `own` (the set of objects this node currently leads,
initially empty), and a per-object `log[o][s] = {b, v, c}` (ballot, value, committed flag).
Figure 4 (described) shows the normal-case flow across two zones (`Z1` with nodes `Z1:1`, `Z1:2`;
`Z2` with nodes `Z2:1`, `Z2:2`): phase-1a/1b run globally ("Phase I: Global Latency"), then
phase-2a/2b/phase-3 run locally within the leader's zone ("Phase II: Local Latency").

- **Phase-1a (Algorithm 1, `p1a`).** A client's request for object `o` not in `own` triggers a
  larger ballot `⟨ballots[o].counter+1, self⟩` and a `"1a"` message to a `Q1` quorum.
- **Phase-1b (Algorithm 2, `p1b`).** On `"1a"` with `m.b ≥ ballots[m.o]`, the node adopts `m.b`,
  drops `o` from its own `own` if it held it, and replies `"1b"` with its known ballot and the
  highest slot for `o` (so the new leader can recover unresolved commands).
- **Phase-2a (Algorithm 3, `p2a`).** The aspiring leader collects `"1b"`s; once `Q1Satisfied(o,b)`
  holds (a full `Q1` of matching `"1b"`s), it adds `o` to `own`, recovers any uncommitted slot
  with its suggested value, and starts accepting pending requests by incrementing `slots[o]` and
  sending `"2a"`.
- **Phase-2b (Algorithm 4, `p2b`).** On `"2a"` with `m.b ≥ ballots[m.o]`, a node updates its
  instance and replies `"2b"`.
- **Phase-3 (Algorithm 5, `p3`).** The leader commits once `Q2Satisfied(o,b,s)` holds (a full `Q2`
  of matching `"2b"`s) and sends `"3"`; a rejection (a higher known ballot) re-queues the request
  for retry at a bumped ballot.

**Properties** (model-checked in TLA+): **non-triviality** (every committed sequence is a sequence
of client-proposed commands); **stability** (committed sequences only grow: any committed command
survives into the future); **consistency** (no two leaders ever commit different values for the
same slot of the same object: object stealing and recovery never override an accepted/committed
value); **liveness**, matching ordinary Paxos (progress as long as some `q1 ∈ Q1` and `q2 ∈ Q2`
stay alive). WPaxos's consistency guarantee is on par with EPaxos's generalized-consensus
guarantee, except WPaxos partitions by object rather than by non-interference analysis, giving
per-object (rather than per-command) linearizability.

## 5. Extensions

### 5.1 Locality Adaptive Object Stealing

The basic protocol migrates an object on its very first remote request, which degrades
performance for objects accessed frequently from many zones. The **majority-zone migration
policy** (Fig. 5, described: leader `α` observes heavy traffic from node `β`'s zone and triggers
`β` to steal) instead moves an object only to the zone sending the most requests for it, since the
current leader already knows each request's origin; clients from less-frequent zones keep being
forwarded to the (possibly remote) leader.

### 5.2 Replication Set

A `Q2` phase-2a message need not broadcast to every zone in `Q2`: the user may choose a
**replication `Q2` (`RQ2`)** anywhere from the minimal `F + 1` zones up to all `Z` zones, trading
communication overhead against predictable latency. Nodes outside `RQ2` can learn state as
non-voting learners to avoid catch-up delay if they later become leader.

### 5.3 Fault Tolerance and Reconfiguration

WPaxos progresses as long as it can form valid `q1` and `q2`; the flexibility lets an operator
tune the system toward performance or fault tolerance per deployment. **By default, WPaxos
configures quorums to tolerate one zone failure and minority node failures per zone**, matching
Spanner-with-Paxos-groups-over-three-zones fault tolerance.

**Degraded operation.** When more zones fail than the configured tolerance, no valid `q1` can
form, which halts object stealing: but operations continue for objects already owned in the
remaining live zones, as long as a `q2` can still be formed there. So WPaxos degrades to
*partial* availability (owned objects keep serving) rather than losing availability entirely.

**Reconfiguration.** WPaxos adopts Raft's general two-phase reconfiguration (commit a combined
configuration `C + C'`, only then propose the new `C'` alone) for an arbitrary new config
`C' = ⟨Q1', Q2'⟩` from `C = ⟨Q1, Q2⟩`, generalized because the "combined quorum" intersection
argument Raft relies on carries over to flexible quorums. For the common single-zone-or-row
add/remove cases (Fig. 6, described: adding a dashed new zone), the two phases **collapse into
one**, because the restriction `Q1' ∪ Q1 = Q1'` and `Q2' ∪ Q2 = Q2'` already makes the combined
quorum equivalent to the new configuration's quorum alone: i.e. `C'` already dominates `C`, so no
separate transition commit is needed.

## 6. Evaluation

### 6.1 Setup

Framework: **Paxi**, a Go framework built for this evaluation (open-sourced at
`github.com/ailidani/paxi`) implementing WPaxos, EPaxos, M²Paxos and other Paxos variants for
controlled, identical-workload comparison; it has a quorum module supporting majority, fast, grid
and flexible quorums, and a RESTful client library over TCP, UDP or simulated Go channels.
Deployment: **AWS EC2 across 5 regions (Tokyo (T), California (C), Ohio (O), Virginia (V),
Ireland (I))**, using **4 `m5.large` instances per region, hosting 3 WPaxos nodes and 20
concurrent clients**; WPaxos runs in **adaptive** (locality-adaptive stealing) mode by default.

**Locality model.** A pool of 1,000 common objects is drawn per-region from a Normal distribution
`N(µ, σ²)` (Fig. 7, described: per-region bell curves over 1,000 keys), with `µ` varied per zone to
control locality and `σ` shared. **Locality `L`** is defined as the complement of the two
distributions' **overlapping coefficient (OVL)**: `L = 1 − OVL`, computed via the CDFs' crossing
point `x̂` as `L = Φ₁(x̂) − Φ₂(x̂)`; `L = 0` when the distributions coincide, `L = 1` when disjoint.

### 6.2 Object Space

Preloading one thousand to one million keys, evenly distributed among three regions (**Virginia,
Oregon and California**; note: Oregon does not appear among the five deployment regions of §6.1,
which lists Ohio, not Oregon, see "Unresolved issues"), with requests drawn uniformly at random,
shows **no significant latency impact from object-space size** (Fig. 8, described: flat average
latency line from 1,000 to 1,000,000 keys): expected, since the leader index is an O(1) hash map
riding inside each object's last ballot entry, adding no extra memory. One million keys (no
snapshots, no GC) used about 1.6 GB of the experiment's 8 GB VM.

### 6.3 WPaxos Quorum Latencies

Comparing `(fz, fn) ∈ {(0,0), (0,1), (1,1)}` (Fig. 10, described: phase-1 latency left, phase-2
right, across regions C, V, O, T, I): with `fn = 0`, `Q1` uses a single node per zone and `Q2`
needs every node in one zone; `fn = 1` needs one fewer node in `Q2`. With `fz = 0`, `Q1` spans all
5 zones and `Q2` stays in one region; with `fz = 1`, `Q1` spans 4 zones (lower phase-1 latency) but
`Q2` needs 2 zones (incurring WAN latency). Phase-1 latency is roughly one RTT to the farthest peer
regardless of `fn` (parallel communication); within a zone, `fn = 1` tolerates one straggler,
improving the more-frequent `Q2` latency.

### 6.4 Conflicting Commands

Varying the conflict rate from 0% to 100% (any two commands on the same object conflict;
Fig. 9, described: average latency vs. conflict ratio for WPaxos `fz=0`, `fz=1`, and EPaxos):
WPaxos `fz = 0` beats WPaxos `fz = 1` in every case, because its `Q2` stays inside one region,
avoiding inter-region RTT for non-conflicting commands. Ohio, being geographically central, becomes
leader of most conflicting objects and its latency is largely conflict-independent. At 100%
conflict, WPaxos commits via one RTT between California and Ohio (**49 ms**) plus one RTT between
Virginia and Ohio (**11 ms**), beating EPaxos's two full C–O RTTs.

### 6.5 Latency Comparison

Across random (Fig. 11a), ~70% locality `N(µz, σ=100)` (Fig. 11b), and ~95% locality `N(µz,
σ=50)` (Fig. 11c) workloads, comparing WPaxos (`fz = 0, 1, 2`), EPaxos and M²Paxos over 5 regions:
under random access, WPaxos `fz ≥ 1` loses to EPaxos (extra WAN RTTs from forwarding), but
`fz = 0` wins everywhere from its local `Q2`. Under ~70% locality, regions near the geographic
center improve for all three protocols, and WPaxos `fz = 0, 1` outperforms the others everywhere.
Under ~95% locality, WPaxos avoids WAN forwarding almost entirely and pulls far ahead of EPaxos.

**Tail latency and object-stealing mode** (Fig. 12, described: CDF of request latency, 70%
locality, WPaxos immediate vs. adaptive vs. EPaxos with 5 and 15 nodes): WPaxos **immediate**
stealing hurts edge regions (long `Q1` latency to steal); WPaxos **adaptive** smooths this:
even under low locality, about half of requests still commit at local-area latency.

### 6.6 Throughput Comparison

Using 15 large EC2 nodes (one EPaxos node per zone; EPaxos with 15 nodes was excluded after
preliminary tests showed much worse latency) and driving increasing load (Fig. 13, described:
average and median latency vs. aggregate throughput 1,000–20,000 req/s): at low load, both WPaxos
modes beat EPaxos; as load and contention rise, immediate-stealing WPaxos degrades from leader
dueling between neighboring zones (each restarting phase-1 before the other finishes phase-2),
while EPaxos degrades from rising conflict probability forcing its slow-path second round.
**Adaptive WPaxos barely degrades until CPU/network saturation; at 10,000 req/s with ~70%
locality it is 9× faster on average latency and 54× faster on median latency than EPaxos.**

### 6.7 Shifting Locality Workload

Under a diurnal-style shifting locality (mean of the locality distribution moved at 2 objects/sec;
Fig. 14, described: per-second average latency, WPaxos vs. statically-partitioned KPaxos): KPaxos
degrades steadily as access drifts from its static partition, while adaptive WPaxos migrates
objects toward demand and keeps latency stable.

### 6.8 Fault Tolerance

Fault-injection experiments (10-second fault, then recovery), measured in region V:

- **`fn = 1, fz = 0`** (Fig. 15a, described: latency/throughput over 60s with "crash one node" and
  "crash two local nodes" markers): crashing one node in V has no effect (`|q2| = 2` of 3 nodes
  still available there); crashing two local nodes forces `V` to borrow 2 acks from neighboring
  Ohio, adding 11 ms.
- **`fn = fz = 1`** (Fig. 15b, described: "crash neighbor zone" / "partition 4 nodes" markers over
  60s): latency holds at 11 ms (`q2` spans V and O) until O is crashed entirely, after which the
  leader falls back to C, raising latency to 60 ms; once O recovers, partitioning 4 of 9 nodes
  (3 from C, 1 from O, a minority) has no effect on throughput.

In every injected failure, **WPaxos remained available**.

## 7. Concluding Remarks

WPaxos dynamically partitions objects across strategically-placed multileaders using flexible
quorums, outperforming other WAN Paxos approaches by emphasizing local operations. Because object
stealing is just phase-1 of Paxos, WPaxos needs no auxiliary relocation service and inherits
Paxos's safety under concurrency, asynchrony and faults; performance can then be tuned
orthogonally. Future work: smarter, more proactive object-stealing policies, and more efficient
transactions atop WPaxos.

---

## Why this matters for `paros`

- **The printed WPaxos TLA+-style `Q1`/`Q2` definitions do not intersect on their own terms.**
  Confirmed above by direct construction: the set-builder only caps a quorum's per-zone
  contribution, so a quorum can legally spread thinner than the cap across extra zones and miss
  the shared zone's node intersection the Lemma's proof assumes. This is consistent with (though
  not the stated reason for) paros binding every quorum question to code in `QuorumSystem` rather
  than a hand-derived formula: a quorum predicate is checked against the *exact* membership a
  ballot committed to, not trusted from a paper's prose form. The reason `docs/architecture.md`
  itself gives for binding zone labels to `AcceptorConfig` at the ballot is the one in the next
  bullet: two nodes that disagree on a member's zone must not evaluate different quorums.
- **`docs/architecture.md` §5 already rejects WPaxos's per-zone quorum system for paros**:
  surviving one zone loss needs `fz = 1`, at which point every Phase 2 spans two zones (no better
  than a zone-balanced majority) while tolerating *fewer* node failures than a majority; WPaxos's
  latency win only exists at `fz = 0`, which gives up zone survival outright. paros's grid
  (`QuorumSystem::Grid`, §3.4) cannot express zone survival either (rows/columns are positional
  over sorted ids: column-as-zone kills every row on a zone loss, row-as-zone kills every column),
  so the grid stays an opt-in throughput mode, not the zone-surviving default.
- **What paros does adopt from WPaxos**: zone labels live inside `AcceptorConfig`, bound to the
  ballot with the configuration (so two nodes can never evaluate different quorums from disagreed
  zone data); the coordinator's placement rule is judged through `QuorumSystem` so that removing
  any one zone still leaves a Phase-1 and a Phase-2 quorum, with every Phase-2 quorum spanning at
  least two zones; a journal's leader is placed toward the zone that writes to it via
  `relinquish_to`, which is WPaxos's "steal without a Phase 1" (§3.3) reused as a cooperative
  handoff rather than a competitive one; and matchmaker sets stay zone-spread because their
  quorums are plain majorities.
- **Object stealing itself (§3.2–3.3, multi-leader-per-object ownership) is not adopted.** paros
  is single-leader per journal (`JournalIdentifier`-scoped `ColocatedNode`), not multi-leader
  per-object; WPaxos's dueling-leader safeguards (zone/node tie-break, random backoff) have no
  counterpart because paros's election already serializes through one ballot per journal.

## Unresolved issues

- **Author name.** `docs/architecture.md` (near line 1086) lists the fourth author as "Tasci";
  the paper's own byline and author e-mails say **Kosar**. Left for the maintainer to reconcile.
- **TLA+ specification scope.** The quorum-intersection check above is against the *printed*,
  in-paper set-builder definitions and Lemma 1's proof only. The companion machine-checked
  specification (footnote 2, `github.com/ailidani/paxi/tree/master/tla`) was not fetched or
  inspected; it is plausible that repository spec encodes the stricter "floor form" directly (e.g.
  as an explicit per-zone partition) and is unaffected by this finding.
- **Paper-internal inconsistencies, recorded as found (not fixed):**
  - §4.2 says phase-2 runs "on a `Q2` quorum residing in the closest `F + 1` zones", and §5.2
    again writes "the minimal required `F + 1` zones" for the replication set; in both places `F`
    is the fault-count metric defined in §3.1 (`Fmin`/`Fmax`), not the zone-tolerance parameter
    `fz`. Every other section (Definition 1, Lemma 1, §5.3) uses `fz + 1` zones for `Q2`. Likely a
    typo for `fz` in both spots.
  - §3's `Nodes ≜ 1..Z × 1..N` uses `N` as a per-zone node-id range, while §3.1's `Fmax` formula
    and §6 use `N` as the *total* acceptor count and introduce `l` for the per-zone count: an
    overloaded `N` across the paper.
  - §6.1 lists the five deployed regions as Tokyo, California, **Ohio**, Virginia, Ireland, but
    §6.2's object-space experiment preloads "Virginia, **Oregon** and California": Oregon is not
    one of the five regions set up in §6.1.

## Selected references (from the paper's bibliography)

- Lamport, *The Part-Time Parliament* (1998); Lamport, *Paxos Made Simple* (SIGACT News 2001):
  the base protocol.
- Van Renesse & Altinbuken, *Paxos Made Moderately Complex* (CSUR 2015): the Multi-Paxos /
  ballot-construction convention WPaxos reuses.
- Howard, Malkhi, Spiegelman, *Flexible Paxos: Quorum Intersection Revisited* (2016): the
  cross-phase-only intersection result WPaxos's grid quorums build on (`docs/references/papers/flexible-paxos/`).
- Moraru, Andersen, Kaminsky, *There Is More Consensus in Egalitarian Parliaments* (EPaxos, SOSP
  2013): WPaxos's main point of comparison throughout the evaluation.
- Lamport, Malkhi, Zhou, *Vertical Paxos and Primary-Backup Replication* (PODC 2009): the
  hierarchical/master-mediated reconfiguration WPaxos's object stealing avoids.
- Corbett et al., *Spanner: Google's Globally-Distributed Database* (OSDI 2012): the static
  object-space partitioning and `movedir` relocation service WPaxos contrasts with.
- Nawab, Agrawal, El Abbadi, *DPaxos: Managing Data Closer to Users for Low-Latency and Mobile
  Applications* (SIGMOD 2018): cited as adopting a similar flexible-quorum protocol for edge
  computing (`docs/references/papers/dpaxos/`).
- Mao, Junqueira, Marzullo, *Mencius: Building Efficient Replicated State Machines for WANs*
  (OSDI 2008): the round-robin single-leader-rotation alternative.
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (Raft, ATC 2014):
  source of the two-phase reconfiguration WPaxos generalizes in §5.3.
