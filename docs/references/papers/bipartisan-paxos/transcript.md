# SoK: A Generalized Multi-Leader State Machine Replication Tutorial

Also known as **Bipartisan Paxos (BPaxos)**: the protocol family the paper builds, and the name
of its PDF and of the earlier draft.

**Authors:** Michael Whittaker (UC Berkeley), Neil Giridharan (UC Berkeley), Adriana Szekeres
(University of Washington), Joseph M. Hellerstein (UC Berkeley), Ion Stoica (UC Berkeley)

**Venue:** Submitted to the Journal of Systems Research (JSys), 2021. 23 pages.
**Source:** <https://mwhittaker.github.io/publications/bipartisan_paxos.pdf>
**Implementation:** <https://github.com/mwhittaker/frankenpaxos> (see
[`../../frankenpaxos/`](../../frankenpaxos/))

Related entries: [`../matchmaker-paxos/`](../matchmaker-paxos/) (its §6 names BPaxos as a
protocol that can swap in Matchmaker Paxos for reconfiguration),
[`../scaling-rsm-compartmentalization/`](../scaling-rsm-compartmentalization/) (the other way to
remove the leader bottleneck), [`../flexible-paxos/`](../flexible-paxos/) (the quorum
intersection rules that §7.1 extends to Fast Paxos).

---

## Abstract (verbatim)

> MultiPaxos and Raft are the two most popular and widely deployed state machine replication
> protocols. There is a more sophisticated family of generalized multi-leader state machine
> replication protocols like EPaxos, Caesar, and Atlas that have better performance, but they
> are extremely complicated and hard to understand. Due to their complexity, they have seen
> little to no industry adoption, and academically there has been a lack of clarity in
> analyzing, comparing, and extending the protocols. This paper is a tutorial on generalized
> multi-leader protocols. We explain why the protocols work the way they do, what they have in
> common, where they differ, which parts of the protocols are straightforward, which are more
> subtle than they appear, and so on. In doing so, we present four new generalized multi-leader
> protocols, identify key insights into existing protocols, and taxonomize the space.

---

## 1. Introduction

MultiPaxos and Raft have two properties. They are **leader based**: every command goes through
one leader. They are **totally ordered**: every replica executes every command in log order.

EPaxos, Caesar and Atlas relax both:

- **Multi-leader**: any of several leaders accepts a command, so no single leader limits
  throughput.
- **Generalized** (Lamport's Generalized Consensus): replicas execute conflicting commands in the
  same order, but commuting commands in any order.

These protocols are hard. EPaxos had bugs that stayed undiscovered for years. Domain experts
find them difficult. The authors think this complexity is why industry does not use them.

The tutorial has four parts, each with a new protocol:

1. **Simple BPaxos** (§4): slow, but simple. It contains every mechanism and invariant the other
   protocols share.
2. **Fast BPaxos** (§6): fast, but **unsafe**, on purpose. Its failure shows the central
   difficulty of the family.
3. **Unanimous BPaxos** (§7): a safe, fast protocol that *avoids* the difficulty with large
   quorums. Basic EPaxos and Atlas are optimized variants of it.
4. **Majority Commit BPaxos** (§8): a safe, fast protocol that *resolves* the difficulty during
   recovery. EPaxos and Caesar belong to this class.

## 2. Primer on SMR, Paxos and MultiPaxos

System model: asynchronous network (drop, delay, reorder), crash failures only, no clock
synchronization, at most `f` failures for liveness. Safety holds with more failures.

Paxos with `f+1` proposers and `2f+1` acceptors, in rounds with two phases. The section repeats
the textbook protocol. One remark matters for the rest of the paper: **the leader of round 0 may
skip Phase 1**. Phase 1 learns about lower rounds and blocks them, and no round is lower than 0.

MultiPaxos runs one Paxos instance per log entry. The leader sends and receives 7 messages per
command; acceptors and replicas handle at most 2. That is the known leader bottleneck.

## 3. Conflict graphs

### 3.1 Definitions

Two commands **conflict** if some state exists where executing `x` then `y` gives different
responses or a different final state than `y` then `x`. Otherwise they **commute**.

A generalized protocol does not build a log. It builds a **conflict graph**: a directed graph of
vertices, each with a unique vertex id, a command and a set of dependencies `deps(v)`
(outbound edges). Every pair of conflicting commands has an edge between them. Commuting
commands need no edge. Cycles can occur and are sometimes unavoidable (§4.3).

### 3.2 Execution

Replicas execute the graph in **reverse topological order, one strongly connected component at a
time**. Inside a component, every replica uses the same deterministic order, for example sorted
by vertex id. Conflicting commands then run in the same order on every replica.

### 3.3 Construction

The graph grows one vertex at a time. Each vertex is one consensus instance that chooses the
pair `(command, deps)`. A vertex can be chosen before its dependencies are chosen, just as a
MultiPaxos log entry can be chosen before a lower one.

Table 1 (logs against graphs):

| | Logs | Graphs |
|---|---|---|
| data structure | log | conflict graph |
| unit | log entry | vertex |
| identifier | log index | vertex id |
| order | total | partial |
| execution order | log order | reverse topological order |
| what is chosen | commands | commands and dependencies |

### 3.4 The two invariants

Every protocol in the family relies on two invariants:

- **Invariant 1 (consensus):** at most one value `(x, deps(v))` is chosen for every vertex `v`.
- **Invariant 2 (dependency):** if conflicting commands `x` and `y` are chosen in `vx` and `vy`,
  then `vx ∈ deps(vy)` or `vy ∈ deps(vx)` or both.

Together they give linearizable execution: the history equals a serial history in some reverse
topological order of the graph.

## 4. Simple BPaxos

### 4.1 Roles

Simple BPaxos is **bipartite**. It splits the work into two independent services:

- `2f+1` **dependency service** nodes compute dependencies and keep Invariant 2.
- `f+1` proposers and `2f+1` acceptors run one Paxos instance per vertex and keep Invariant 1.
- `f+1` replicas build and execute the graph and answer clients.

Normal path (7 network delays):

1. A client sends `x` to **any** proposer. Every proposer is a leader.
2. Proposer `pi` places `x` in vertex `vx = (pi, m)`, with `m` a counter local to `pi`. Vertex
   ids are therefore unique without coordination.
3. `pi` sends `(vx, x)` to the dependency service and waits for `f+1` replies.
4. `pi` runs **Phase 2 only** to choose `(x, deps(vx))`. By design, `pi` owns round 0 of every
   vertex `(pi, m)`, so it may skip Phase 1.
5. `pi` broadcasts the chosen vertex to the replicas, which execute and reply.

### 4.2 The dependency service

Each dependency service node keeps its own acyclic conflict graph. On a new vertex, it adds an
edge to every conflicting vertex it already holds and returns those edges. On a vertex it already
holds, it returns the same edges again and changes nothing (idempotent). The proposer takes the
**union** of the first `f+1` replies.

**Invariant 3:** two conflicting vertices get dependencies where at least one names the other.
Proof: the two sets of `f+1` nodes intersect, and the shared node saw one of the two vertices
first.

### 4.3 Example

Two proposers send conflicting `x` and `y` concurrently. Two dependency nodes see them in
opposite orders. Both commands are chosen depending on each other: a cycle. Forcing every node to
see commands in the same order would itself be consensus.

### 4.4 Recovery

A proposer can crash after it sends `vx` to the dependency service. A chosen `vy` can then depend
on `vx`, and `vy` can never execute. To fix this, a replica that waits too long on an unchosen
vertex notifies a proposer. That proposer runs **both** Paxos phases in a round above 0 and
chooses `noop` with no dependencies. `noop` conflicts with nothing.

Two points:

- There is **no separate recovery protocol**. A `noop` is proposed exactly like a command.
- If a client's command is replaced by a `noop`, the client times out and sends it again.

### 4.5 Safety

Invariant 1 holds because the protocol is Paxos. Invariant 2 follows from Invariant 3 and the
fact that `noop` conflicts with nothing.

## 5. Fast Paxos (review)

Clients send values straight to the acceptors in **round 0, the fast round**. Let
`maj(n) = ⌈(n+1)/2⌉`. A value is chosen in round 0 with `f + maj(f+1)` matching votes (the **fast
Phase 2 quorum**). Later rounds use classic `f+1` quorums.

**Recovery** (Algorithm 1). A recovering proposer runs Phase 1 in round `i > 0` with `f+1`
acceptors and takes `k`, the largest voted round:

- `k = -1`: propose anything.
- `k > 0`: propose the value of round `k`, as in Paxos.
- `k = 0`: if `maj(f+1)` of the replies voted for the same `v0`, then `v0` may be chosen; propose
  it. Otherwise propose anything.

The fast quorum must be `f + maj(f+1)`, not `f+1`. Counterexample (`f = 2`, five acceptors,
choose with 3 votes): a recovering proposer hears `x` from `a3`, `y` from `a4` and nothing from
`a5`. Both `x` and `y` may have 3 votes (with `a1`, `a2`). The proposer cannot choose between
them and is stuck.

**Coordinated recovery.** If the leader of round 0 also owns round 1, it can treat each round-0
`Phase2B` as a round-1 `Phase1B` and go straight to Phase 2 of round 1. This saves the Phase 1
round trip after a collision.

## 6. Fast BPaxos (unsafe on purpose)

### 6.1 The protocol

Same as Simple BPaxos, but with Fast Paxos in place of Paxos. Each of `2f+1` servers co-locates a
dependency node, an acceptor, a proposer and a replica. A dependency node does not reply to the
proposer. It proposes `(x, deps)` to its own acceptor in round 0. The commit time drops to **4
network delays**.

### 6.2 Recovery

As in Simple BPaxos: a replica that waits too long on a vertex asks a proposer to recover it with
a `noop`, through Fast Paxos recovery.

### 6.3 Why it is unsafe

Execution with `f = 2` (Figure 13):

1. `p1` receives `x`. Only `d1` and `d2` receive it. They compute `deps = ∅` and their acceptors
   vote for `(x, ∅)`. `p1` crashes.
2. `p5` receives a conflicting `y`. Only `d4` and `d5` receive it. Their acceptors vote for
   `(y, ∅)`. `p5` crashes.
3. `p2` recovers `vx` with `a1, a2, a3`. Two of three voted `(x, ∅)` in round 0, which is
   `maj(f+1)`, so Fast Paxos **forces** `p2` to propose `(x, ∅)`.
4. `p4` recovers `vy` the same way and is forced to propose `(y, ∅)`.
5. `x` and `y` conflict, yet neither depends on the other. Replicas can execute them in
   different orders and diverge.

The cause: the forced value `(x, ∅)` came from only two dependency nodes, not a majority.

**The fundamental tension.** Keeping Invariant 1 alone is easy (use Paxos). Keeping Invariant 2
alone is easy (use the dependency service). During recovery, a proposer can be forced to propose a
value to keep consensus and simultaneously forbidden to propose it to keep the dependency
invariant. All fast protocols in the family share the same normal path. **They differ only in how
they handle this tension.** That is the paper's central claim and the basis of its taxonomy:

- **Tension avoidance:** enlarge quorums so the forced value always has majority-computed
  dependencies (Basic EPaxos, Atlas).
- **Tension resolution:** keep small quorums and do extra detective work in recovery (EPaxos,
  Caesar).

## 7. Tension avoidance

### 7.1 Unanimous BPaxos

Howard, Charapko and Mortier (Fast Flexible Paxos) prove Fast Paxos safe when:

1. Every Phase 1 quorum intersects every classic Phase 2 quorum.
2. Every Phase 1 quorum intersects **every pair** of fast Phase 2 quorums
   (`Q ∩ Q' ∩ Q'' ≠ ∅`).

Unanimous BPaxos sets the fast Phase 2 quorum to all `2f+1` acceptors (Algorithm 3). A recovering
proposer is then forced to propose a round-0 value only if **all** `f+1` Phase 1 replies voted for
it. Those `f+1` votes came from `f+1` dependency nodes, a majority, so the dependencies are safe.
The tension cannot arise.

Cost: a round trip with every acceptor per command, and a single failed acceptor stops the fast
path.

### 7.2 The Basic EPaxos optimization

This reduces the fast quorum to `2f`. The proposer's own dependency node computes first and sends
its result to the others, which union it with their own. The proposer's co-located acceptor
records the value as chosen, locally. A recovering proposer that hears from that acceptor learns
the outcome directly. Otherwise, `f` matching round-0 votes are enough, because every other node
included the owner's dependencies. For `f = 1` the quorum conditions fail. The fix: the leader
sends to only `2f` acceptors. With cosmetic differences, this is Basic EPaxos.

### 7.3 The Atlas optimization

Let `popular(X1..X2f+1)` be the elements present in at least `f+1` sets. The proposer takes the
fast path with the union of all replies when that union equals `popular(...)`: every dependency
any node reported, a majority reported. Dependency sets no longer need to be identical. Recovery
gets simpler: propose the union of `f+1` sets. Atlas is Unanimous BPaxos plus the Basic EPaxos
optimization, the Atlas optimization and flexible quorums.

## 8. Tension resolution

### 8.1 Pruned dependencies

`vx` need not depend on `vy` when `vy` is already chosen with `noop` or with `vx ∈ deps(vy)`.
The edge already exists in the other direction, or no conflict exists. **Invariant 4 (pruned
dependency):** every vertex holds `(noop, ∅)` or `(x, deps(vx) - P)`, where `deps(vx)` came from
the dependency service and `P` contains only such prunable vertices. Invariant 4 implies
Invariant 2.

### 8.2 Majority Commit BPaxos

Same as Fast BPaxos with two changes:

1. Acceptors keep a conflict graph of chosen vertices, like replicas. A `Phase2A` that carries
   pruned dependencies also carries the chosen values of `P`.
2. Recovery (Algorithm 4) does detective work when `maj(f+1)` replies voted for `v0` in round 0:
   - Ask the dependency nodes co-located with the Phase 1 quorum for `deps(vx)_A`, a superset of
     `deps(vx)`.
   - For every extra `vy`, recover `vy` first if needed (recursion). If `vy` is `noop` or depends
     on `vx`, prune it.
   - Otherwise ask an acceptor quorum. If one knows `vx` is chosen, stop and use that value.
     If none does, `v0` **cannot have been chosen** in round 0: every dependency node in `vy`'s
     quorum processed `vy` before `vx`, so no fast quorum processed `vx` first. Propose anything
     that keeps the invariant.
   - If all extras are pruned, `v0` is safe: propose it with the values of `P`.

### 8.3 Liveness

The recursion can **deadlock without any failure**: a chain of vertices where each recovery waits
for the next, in a cycle. The fix has three parts. A fast-path value counts as chosen only if, for
each dependency, an `f+1` subset of the fast quorum knew that dependency was chosen when it voted.
Round-0 votes carry the chosen dependencies the voter knows. Recovery (Algorithm 5) can then break
any deferral: either it proves `v0` was not chosen, or it settles the deferred vertex directly.

### 8.4 EPaxos and Caesar

EPaxos is close to Majority Commit BPaxos plus the Basic EPaxos optimization (fast quorum one
smaller). Caesar does not need identical fast votes (like Atlas) and avoids recursive recovery
with logical timestamps and barriers.

## 9. Related work

- **Advantages of generalized multi-leader** over MultiPaxos, Raft and VR: no single-leader
  throughput limit; throughput drops but not to zero when a leader fails; a nearby leader for
  every geo-distributed client; lower tail latency, because a slow command delays only the
  commands that depend on it, not every later log entry.
- **Losa et al.**: a generic dependency-set plus map-agreement algorithm, close to Simple BPaxos.
- **Generalized Paxos, GPaxos**: generalized but not multi-leader; one leader resolves collisions.
- **SpecPaxos, NOPaxos, CURP**: speculation to reach 2 network delays.
- **Mencius**: multi-leader but **not generalized**; leaders own log entries round-robin. Much
  simpler than EPaxos. The paper concludes that **most of the complexity comes from being
  generalized, not from being multi-leader.**
- **Chain Replication, Scalog**: remove the leader bottleneck without multiple leaders.
- **PQR, Harmonia, CRAQ**: reads without the leader. Applying them to generalized protocols is
  open work.

## 10. Conclusion

Taxonomy (Figure 15, as text):

- One leader, not generalized: MultiPaxos, Raft, VR, Chain Replication.
- One leader, generalized: Generalized Paxos, GPaxos.
- Many leaders, not generalized: Mencius.
- Many leaders, generalized, commit time above 4: Simple BPaxos.
- Many leaders, generalized, commit time 4 or less, tension avoiding: Unanimous BPaxos, Basic
  EPaxos, Atlas.
- Many leaders, generalized, commit time 4 or less, tension resolving: Majority Commit BPaxos,
  EPaxos, Caesar.

Table 2 (all with Phase 1 quorum `f+1`; `n` nodes for Atlas):

| Protocol | Safe | Commit time | Tension | Nodes | Classic Phase 2 | Fast Phase 2 |
|---|---|---|---|---|---|---|
| Simple BPaxos | yes | 7 | n/a | `2f+1` | `f+1` | n/a |
| Fast BPaxos | **no** | 4 | n/a | `2f+1` | `f+1` | `f + maj(f+1)` |
| Unanimous BPaxos | yes | 4 | avoidance | `2f+1` | `f+1` | `2f+1` |
| Basic EPaxos | yes | 4 | avoidance | `2f+1` | `f+1` | `2f` |
| Atlas | yes | 4 | avoidance | `n` | `n-f` | `⌊n/2⌋ + f` |
| Maj. Commit BPaxos | yes | 4 | resolution | `2f+1` | `f+1` | `f + maj(f+1)` |
| EPaxos | yes | 4 | resolution | `2f+1` | `f+1` | `f + maj(f+1) - 1` |
| Caesar | yes | 4 | resolution | `2f+1` | `f+1` | `f + maj(f+1)` |

---

## Why this matters for `paros`

**The generalized half does not apply.** A paros journal is a total order by definition: every
record gets a dense `seq`, and record bytes are opaque (`docs/architecture.md` §2.1, §2.6). paros
cannot tell whether two writes commute, and a `Read` exposes their order anyway, so every pair
of writes in one journal conflicts. The conflict graph of one journal degenerates to a chain. The
tension of §6.3 cannot arise: paros never chooses dependencies, only opaque values.

**paros is already multi-leader at the right grain.** Across journals, the conflict relation is
"same journal". Every journal has its own `ColocatedNode`, ballots and leader, and the one
cross-journal property is non-interference. In the paper's terms, the multi-tenant service is a
generalized SMR whose conflict graph is a set of disjoint chains, known in advance from the
`JournalIdentifier`. No dependency service is needed, because the dependencies are static. This
is a useful frame for the site.

**Mencius is the only multi-leader design that fits one journal.** The paper's own conclusion is
that most of the complexity comes from generalization, not multi-leadership. If a multi-writer
journal (§2.4) ever hits a leader limit that batchers and proxy leaders do not remove, Mencius
(round-robin ownership of slots) is the candidate, not EPaxos. This is a pointer, not a plan.

**Mechanisms paros already has:**

- §4.4 recovery of an abandoned vertex with `noop`, proposed exactly like a command, is the
  election gap fill: a new leader fills every unreported slot below its frontier with a `Noop`
  (`proposer.rs`), because pipelining can strand a slot no one would propose again.
- The client that re-sends a command replaced by `noop` is the ambiguous-write case that
  `paros::client` settles with `Client::resolve`.
- §4.1 round-0 ownership (the owner of a round may skip Phase 1) has the same shape as paros's
  cooperative handoff: the successor continues the **same** ballot with no Phase 1, and only the
  elected minter may hand it on (`relinquish_to`,
  [`../../../analysis/consensus/dpaxos-leader-handoff.md`](../../../analysis/consensus/dpaxos-leader-handoff.md)).
  The safety arguments differ. Round 0 skips Phase 1 because no lower round exists. The handoff
  continues a ballot that already won Phase 1, and the abdication is synchronous with the
  decision. paros does not skip Phase 1 at the lowest ballot.

**Judged at apply avoids the tension by construction.** Fast BPaxos fails because a value carries
derived data (`deps`) that recovery can force while only a minority computed it. paros proposes
opaque values and derives everything (the `seq`, the leader-uuid fence verdict, `first_seq`) at
apply, deterministically, from the chosen prefix. No derived data needs a quorum of its own.

**If a fast path ever enters `membership.rs`.** Howard's second condition (§7.1) is a three-way
intersection, `Q1 ∩ Q2f ∩ Q2f' ≠ ∅`. It would be a third phase-split predicate on `QuorumSystem`,
never a count in a tally. For a journal, though, every concurrent write collides on the next
`seq`, so a fast path helps only without contention. Not planned.

**Simulation doctrine.** EPaxos bugs stayed hidden for years. Majority Commit BPaxos deadlocks in
executions with no failure at all. Both are the kind of result paros's sweep and its liveness
oracles exist to find, and support "simulation before features".

**Pedagogy.** The method "study the broken protocol to see why the safe one works" fits the site
and `paros-play`. The §5 counterexample (`f = 2`, quorum of 3, `x` and `y` both possibly chosen)
and the §6.3 execution are compact levels for "why quorums must intersect the way they do".
