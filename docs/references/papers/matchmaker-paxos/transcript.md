# Matchmaker Paxos: A Reconfigurable Consensus Protocol

**Authors:** Michael Whittaker (UC Berkeley), Neil Giridharan (UC Berkeley), Adriana Szekeres (University of Washington), Joseph M. Hellerstein (UC Berkeley), Heidi Howard (University of Cambridge), Faisal Nawab (UC Santa Cruz), Ion Stoica (UC Berkeley)

**Venue:** Submitted to the Journal of Systems Research (JSys), 2021.
**Source:** https://mwhittaker.github.io/publications/matchmaker_paxos.pdf
**Implementation:** https://github.com/mwhittaker/frankenpaxos

---

## Abstract

SMR protocols (MultiPaxos, Raft) must replace failed machines with new ones — **reconfiguration** — and the need for frequent reconfiguration has grown (elastic/proactive scaling, serverless, spot instances, edge/IoT). Despite this, reconfiguration has been largely neglected in the literature. This paper presents **Matchmaker Paxos** (reconfigurable consensus) and **Matchmaker MultiPaxos** (reconfigurable SMR). They reconfigure with little-to-no impact on latency/throughput, in a few milliseconds, and provide a framework that generalizes to other replication protocols where prior techniques cannot. Both are proven safe with an open-source implementation.

---

## 1. Introduction — Two key design ideas

1. **Decouple reconfiguration from the standard processing path.** Introduce dedicated **matchmaker** machines, solely responsible for managing reconfigurations and off the critical path. Matchmakers are a *source of truth*: they always know the current configuration.
2. **Reconfigure across rounds (vertical reconfiguration** [Vertical Paxos]**).** Every round of consensus can use a different configuration.

At the start of each round, the leader queries the matchmakers to discover older configurations used in previous rounds, and simultaneously tells them the configuration it intends to use now. Matchmakers act as a **registry for configurations** — leaders "query the past and update the present" in a single round trip that happens rarely. Optimizations move matchmaking entirely off the critical path; a garbage collection protocol deletes old configurations.

### Desirable properties
- **Little to no performance degradation** (< 4% effect on median throughput/latency).
- **Quick reconfiguration** — one round trip in the normal case; a few ms within a datacenter; ~5 ms to shut down old machines.
- **Generality** — classic MultiPaxos reconfigures *across log entries* (**horizontal reconfiguration**), which requires a log. Many SMR protocols don't replicate a log (EPaxos, Generalized Paxos, CASPaxos, TAPIR, Meerkat...) but all have **rounds**, so they can use **vertical reconfiguration**. Matchmaker Paxos is a foundation for reconfiguration in non-log-based protocols.
- **Theoretical insights** — generalizes Vertical Paxos; first protocol to achieve the theoretical lower bound on Fast Paxos quorum sizes; corrects errors in DPaxos.
- **Proven safe.**

---

## 2. Background

### 2.1 System Model
Asynchronous network (drop/delay/reorder); crash (non-Byzantine) failures; arbitrary speeds; no clock sync; a discovery service (need not be strongly consistent — DNS suffices). For liveness, ≤ `f` failures. Safe but not fully live (FLP).

### 2.2 Paxos
`f+1` proposers, `2f+1` acceptors. Execution divided into **rounds**, each with Phase 1 and Phase 2, orchestrated by a predetermined proposer. Rounds are a totally ordered set, commonly `(r, id)` pairs. Phase 1: learn of any possibly-chosen value and block lower rounds. Phase 2: propose a value (only a safe one) and get a majority vote.

### 2.3 Flexible Paxos
Introduces a **configuration** `C = (A; P1; P2)`: a set of acceptors `A`, a set of **Phase 1 quorums** `P1`, and **Phase 2 quorums** `P2`, where every P1 quorum intersects every P2 quorum (read/write quorums need not self-intersect). The rest of the paper uses arbitrary configurations instead of majorities.

---

## 3. Matchmaker Paxos

### 3.1 Overview
Like Paxos but **every round can use a different configuration of acceptors** (round 0 → `C0`, round 1 → `C1`, …). The proposer of round `i` must contact *all configurations used in rounds less than i* in Phase 1. To know which acceptors those are, the deployment adds `2f+1` **matchmakers**. Flow:
1. Client → proposer with value `x`.
2–3. Proposer picks configuration `Ci`, sends it to the matchmakers (the **Matchmaking phase**); matchmakers reply with configurations used in previous rounds.
4–5. Proposer runs Phase 1 against the prior configurations returned.
6–7. Proposer runs Phase 2 with `Ci` to choose `x`.
8. Proposer informs the client.

### 3.2 Details
Each matchmaker keeps a log `L` of configurations indexed by round. On `MatchA⟨i, Ci⟩`: if it already saw a config in round `j ≥ i`, ignore; else compute `Hi = {(j, Cj) | j < i, Cj ∈ L}`, insert `Ci` at `L[i]`, reply `MatchB⟨i, Hi⟩`. The proposer collects `MatchB` from `f+1` matchmakers and unions them: `Hi = ∪ Hi^j`. Because each round is statically assigned to one proposer that picks one config, matchmakers can't disagree about a round's config. The proposer runs Phase 1 against a Phase 1 quorum of *every* config in `Hi`, then Phase 2 with `Ci`. (Acceptor pseudocode is identical to Paxos; proposer is Flexible Paxos + matchmaking.)

### 3.3 Proof of Safety
Proves `P(i)`: if a proposer proposes `v` in round `i`, then no value other than `v` has been or will be chosen in any round `< i`. By strong induction with a case analysis (`j > k`, `j = k`, `j < k`). The key argument for `j > k`: either `Hi` contains `Cj` (Phase 1 quorum of `Cj` blocks round `j` and intersects any Phase 2 quorum), or `Hi` lacks `Cj` (then the `f+1` matchmakers `Mi` never processed round `j` and never will, and they intersect any future matchmaker set `Mj`). Either way round `j` can't choose another value.

### 3.4 Garbage Collection (How)
Can't shut down a config's acceptors until matchmakers will never again return it. A proposer sends `GarbageA⟨i⟩`; matchmakers delete `L[j]` for all `j < i` and raise a GC watermark `w = max(w, i)`, replying `GarbageB⟨i⟩`. Matchmakers ignore `MatchA⟨i, Ci⟩` if `i < w`, and return `w` in every `MatchB`. Once a proposer gets `GarbageB⟨i⟩` from `f+1` matchmakers, all future matchmaking phases exclude rounds `< i` (intersection argument), so those acceptors can be shut down.

### 3.5 Garbage Collection (When) — three safe scenarios
- **Scenario 1:** proposer `pi` got a value chosen in round `i` → safe to `GarbageA⟨i⟩` (future proposers re-learn the value; lower rounds are redundant).
- **Scenario 2:** proposer `pi` ran Phase 1 and found `k = −1` (no value chosen below `i`) → safe to GC.
- **Scenario 3:** `pi` learns a value `x` is already chosen and stored on `f+1` non-acceptor machines → after informing a Phase 2 quorum of `Ci`, safe to GC. Future proposers contact a Phase 1 quorum of `Ci`, learn `x` is chosen, stop, and fetch `x` from one of the `f+1` machines.

### 3.6 Optimizations
- **Proactive matchmaking** — run the matchmaking phase before hearing from a client (like proactively running Phase 1).
- **Phase 1 bypassing** — if a proposer ran matchmaking + Phase 1 in round `i`, found `k = −1`, and didn't run Phase 2, then on moving to round `i+1` it can skip Phase 1 and go straight to Phase 2 (it already established no value chosen `≤ i`). Requires the same proposer owns rounds `i` and `i+1`; achieved with round tuples `(r, id, s)` so proposer `p` owning `(r, p, s)` also owns `(r, p, s+1)`. Essential for good Matchmaker MultiPaxos performance; also applies to plain Paxos/MultiPaxos.

---

## 4. Matchmaker MultiPaxos

### 4.1 MultiPaxos summary
SMR agreeing on a log; one Paxos instance per entry. Leader knows entries `≤ kc` are chosen (**commit index**). Runs Phase 1 once per round (not per command) with a single `Phase1A⟨i⟩` covering all entries `> kc`. After Phase 1 the log splits into three regions by commit index `kc` and **pending index** `kp`:
- **Region 1** `[0, kc]`: chosen.
- **Region 3** `[kp+1, ∞)`: no value chosen below round `i`.
- **Region 2** `[kc+1, kp]`: maybe-chosen entries (plus known-chosen and known-holes; holes get no-ops). Phase 1 happens only on leader change — an uncommon event.

### 4.2 Matchmaker MultiPaxos
Deployment: clients, `f+1` proposers, `2f+1` matchmakers, a dynamic set of acceptors (one config per round), `f+1` replicas. A stable leader in round `i` picks `Ci` for every entry (config selection is orthogonal — admin or external service). Leader runs matchmaking, then Phase 1 with the configs from `Hi`, then Phase 2 with `Ci` (Region 2 + new client commands in Region 3). Proactive matchmaking lets matchmaking + Phase 1 happen before any client request.

### 4.3 Discussion
To reconfigure from `Cold` (round `i`) to `Cnew`: leader advances to round `i+1` and selects `Cnew` — active immediately after the matchmaking round trip. New acceptors need no warm-up/bootstrap and contact no other config. Old config can't be deactivated until GC. Matchmaking + Phase 1 are **off the critical path** of normal operation (only on leader change / round change); in the normal case Matchmaker MultiPaxos = MultiPaxos with no overhead. Configs may be reused across rounds. More nodes → shorter time to `f` failures, but mean time to failure ≫ reconfiguration time.

### 4.4 Optimization — avoiding stalls during reconfiguration
Case analysis on when a command arrives during a reconfiguration (round `i` → `i+1`):
- **Matchmaking phase:** process normally in round `i` with `Cold` (acceptors oblivious).
- **Phase 1:** the command **must be delayed** — `Cold` acceptors reject rounds `< i+1`, and `Cnew` hasn't finished Phase 1.
- **Phase 2:** process with `Cnew` in round `i+1` (normal case).

**Phase 1 bypassing eliminates the stall:** at the end of matchmaking, let `k` be the largest assigned entry; all entries `> k` are empty and satisfy the bypass preconditions. New commands get entries `> k`, skip Phase 1, and run Phase 2 with `Cnew` immediately. Result: commands before/during matchmaking are chosen by `Cold` (entries `≤ k`); later commands by `Cnew` (entries `k+1, …`). No command is delayed.

### 4.5 Garbage Collection
Leader `pi` GCs once every entry satisfies one of the three scenarios, mapped to the regions: Scenario 2 → Region 3; Scenario 1 → Region 2 (after choosing its commands); Scenario 3 → Region 1 (deploy `2f+1` replicas, ensure the chosen prefix is on `f+1` replicas, inform a Phase 2 quorum of `Ci`). Then `GarbageA⟨i⟩`, await `f+1` `GarbageB`. Old configs shut down. Client commands proceed as soon as Phase 2 starts (no stall during GC). Empirically `Hi` almost always holds just one config (`Ci−1`). (Log GC via snapshots is orthogonal.)

---

## 5. Reconfiguring Matchmakers
- **Proposers/replicas** reconfigure exactly as in MultiPaxos (add/remove anytime; keep `f+1`-replicated entries `f+1`-replicated; new proposer/replica copies state from an existing one).
- **Matchmakers** are idle whenever there's a single stable leader, so their reconfiguration must be safe but need not be efficient. Use a **stop-the-world** approach: send `StopA⟨⟩` to `Mold`; each matchmaker stops and replies `StopB⟨Li, wi⟩`. From `f+1` responses compute `w = max wi` and `L = union of logs` (pruning rounds `< w`). Send `L, w` to `Mnew` as their initial state. To prevent two concurrent reconfigurations to disjoint sets, every `Mold` matchmaker doubles as a Paxos acceptor and the new set `Mnew` is *chosen* via Paxos. Stale matchmakers point to their successors (a chain); successor identity is persisted in a name service (e.g., DNS).

---

## 6. Theoretical Insights

**vs. Horizontal MultiPaxos** (reconfigure by choosing `N'` at log index `i`; commands from `i+α` use `N'`):
1. Horizontal reconfiguration has hidden subtleties (a new leader with a stale log may not know the latest config or whom to ask); matchmakers always provide the latest config and GC says exactly when to retire configs.
2. Horizontal reconfiguration **requires a log** — incompatible with EPaxos, Generalized Paxos, Atlas, Caesar, CASPaxos, TAPIR, Meerkat, even Raft. All have rounds → can use/borrow Matchmaker Paxos (e.g., BPaxos uses Paxos as a black box → swap in Matchmaker Paxos; Matchmaker CASPaxos).
3. The `α` parameter is hard to tune (too low limits pipeline parallelism and *normal-case* throughput; too high → slow reconfiguration). Matchmaker MultiPaxos has **no α**.
4. Horizontal reconfiguration needs a Phase 1 **and** Phase 2 quorum of the old config after a leader failure; Matchmaker MultiPaxos needs only a **Phase 1 quorum** — better for read-optimized variants with tiny Phase 1 / huge Phase 2 quorums.
- Caveat: a well-tuned Horizontal MultiPaxos with small Phase 2 quorums also reconfigures "optimally." Horizontal's advantage: no separate matchmaker reconfiguration protocol.

**vs. Vertical Paxos:** Matchmaker improves practicality — Vertical Paxos is consensus-only (its GC lacks Scenario 3, so old configs can never be safely shut down for SMR); needs an external master (Matchmakers show no nested SMR is required); requires Phase 1 to reconfigure (Phase 1 bypassing avoids this); assumes configs are fixed by an oracle (matchmakers store every config).

**vs. Fast Paxos:** Matchmakers let Fast Paxos run with a fixed `f+1` acceptors (unanimous Phase 2 quorum, singleton Phase 1 quorums) — first to hit the theoretical lower bound on Fast Paxos quorum sizes. (Proof in Appendix C; Phase 1 bypassing can't apply to Fast Paxos.)

**vs. DPaxos:** Matchmaker obviates DPaxos's fixed node set, works across multiple consensus instances, and **fixes a bug in DPaxos's garbage collection** (Appendix D walks a concrete `fd=1, fz=0`, three-zone counterexample where GC of intents lets a leader miss an already-chosen value `x` and choose `z`).

---

## 7. Evaluation
Scala + Netty, AWS `m5.xlarge`, single AZ, `f=1`, `f+1` proposers / `2f+1` acceptors / `2f+1` matchmakers / `2f+1` replicas, trivial 1-byte no-op state machine.

- **7.1 Reconfiguration:** 1/4/8 clients, closed loop, 35 s. Reconfigure acceptors once/sec from 10–20 s (worst-case stress). New acceptors active within **1 ms**; old ones GC'd within **5 ms** (so matchmakers return a single config). Reconfiguration has < ~4% effect (often ~2%) on median latency/throughput. Compared against Horizontal MultiPaxos (`α = 8`); both reconfigure without degradation. After an acceptor failure (with thriftiness), throughput/latency dip and recover within ~2 s.
- **7.2 Leader Failure:** throughput drops to zero on leader failure, recovers within ~2 s of a new election; the Matchmaker phase adds negligible latency.
- **7.3 Matchmaker Reconfiguration:** reconfiguring matchmakers (and a matchmaker failure/recovery) has no effect on latency/throughput — confirming matchmakers are off the critical path — and doesn't affect subsequent acceptor reconfiguration.

---

## 8. Related Work
- **SMART** — resolves horizontal-reconfiguration ambiguities but is log-based, assumes acceptors+replicas co-located, and has higher-latency GC (waits for snapshot vs. Matchmaker waiting for `f+1`-replicated prefix).
- **Cheap Paxos** — `f+1` main + `f` auxiliary acceptors; Matchmaker needs only `f+1` acceptors (`f` fewer), plus `2f+1` matchmakers that process a single message per reconfiguration.
- **Raft joint consensus** / **VR stop-the-world** / **Stoppable Paxos** — log-based or inefficient.
- **Fast Paxos coordinated recovery** — similar to but subsumed by Phase 1 bypassing.
- **DynaStore** — reconfiguring atomic storage without consensus (matchmakers also avoid consensus for the registry).
- **ZooKeeper / ZAB** — fast reconfiguration after leader failures.

---

## 9. Conclusion
Matchmaker Paxos / MultiPaxos address the neglected, increasingly important topic of reconfiguration: reconfigure without performance degradation, provide theoretical insights into existing protocols, and generalize better than prior techniques (any protocol with rounds).

---

## Appendices
- **A** Garbage Collection Safety — extends §3.3 proof to cover GC'd configs (Scenarios 1/2/3 each give a contradiction).
- **B** Matchmaker Reconfiguration Safety — extends the proof across matchmaker sets (each new set initialized from a majority of the previous, so configs/GC propagate).
- **C** Fast Paxos with matchmakers — proposer pseudocode (Algorithm 5) and a safety proof for `f+1` acceptors with unanimous Phase 2 / singleton Phase 1 quorums.
- **D** DPaxos bug — a concrete execution where DPaxos's GC causes a committed value to be overwritten.

---

## Key Takeaways
- **Matchmakers = an external configuration registry** queried per round-change, off the critical path; "query the past, update the present" in one round trip.
- **Vertical (across-rounds) reconfiguration** works for any protocol with rounds — including log-less protocols that horizontal reconfiguration can't serve.
- **Phase 1 bypassing** is what makes reconfiguration free of stalls and gives "no α to tune."
- Needs only an `f+1` Phase-1 quorum of the old config to reconfigure (vs. horizontal's Phase 1 + Phase 2).
- Three GC scenarios determine *when* an old configuration can be retired; intersection of `f+1` matchmaker sets is the core safety argument.
- Generalizes Vertical Paxos, hits the Fast Paxos quorum lower bound, and fixes a DPaxos GC bug.
