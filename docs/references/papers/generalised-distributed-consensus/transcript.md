# A Generalised Solution to Distributed Consensus

**Authors:** Heidi Howard, Richard Mortier (University of Cambridge)
**Date:** 18 Feb 2019 (arXiv:1902.06776v1).
**Source:** https://arxiv.org/abs/1902.06776

**Abstract (in full):** *"Distributed consensus, the ability to reach agreement in the face of failures and asynchrony, is a fundamental primitive for constructing reliable distributed systems from unreliable components. The Paxos algorithm is synonymous with distributed consensus, yet it performs poorly in practice and is famously difficult to understand. In this paper, we re-examine the foundations of distributed consensus. We derive an abstract solution to consensus, which utilises immutable state for intuitive reasoning about safety. We prove that our abstract solution generalises over Paxos as well as the Fast Paxos and Flexible Paxos algorithms. The surprising result of this analysis is a substantial weakening to the quorum requirements of these widely studied algorithms."*

---

## The big idea

Recast consensus on top of **immutable, write-once registers**. A server holds an infinite sequence
of write-once registers `R0, R1, …`; consensus over them is governed by **four correctness rules**.
Paxos, Flexible Paxos, and Fast Paxos all fall out as *conservative* instances of this one abstract
algorithm — and reframing them this way exposes that their quorum-intersection and quorum-agreement
requirements can be **substantially weakened**. Immutability is the trick that makes safety
reasoning intuitive: once a register has a value, every read of it stays valid forever, so a client
can accumulate knowledge monotonically.

## 1–2. Why, and the problem

Paxos performs poorly (majorities → round trips to many participants → limited to 3–5 nodes;
Multi-Paxos funnels everything through one master) and is hard to understand. Paxos is just *one
point* in the trade-off space; the goal is a generalisation that lets engineers pick their own point.

**Problem (single-value consensus):** non-Byzantine, asynchronous, unreliable. Two participant
types: **servers** store the value, **clients** read/write it (clients input a value, output the
decided value). Three requirements: **Non-triviality** (output values were some client's input),
**Agreement** (all outputs equal), **Progress** (eventual output under enough synchrony — FLP means
that's the best obtainable). One server with a single write-once register `R0` trivially solves it
(first write wins, later clients read it) but isn't fault-tolerant → generalise to many servers.

## 3. The generalised solution

Each register is **unwritten**, **contains a value** (A, B, …), or **contains nil (⊥)**. A **register
set `i`** is "register `Ri` across all servers" (a row in the *state table*; each column is a server).
Each register set has a configured set of **quorums** `Qi`. A value `v` is **decided** if some
quorum's servers all hold the same non-nil `v` in the same register set.

### The four correctness rules (Figure 3 — the heart of the paper)

- **Rule 1 (Quorum agreement):** a client may only *output* `v` if it read `v` from a whole quorum in
  one register set. *(⇒ only decided values are output.)*
- **Rule 2 (New value):** a client may only *write* `v` if `v` is its own input or was read from some
  register. *(⇒ only input values get decided — non-triviality.)*
- **Rule 3 (Current decision):** a client may only write `v` to register `r` if doing so cannot let a
  *different* value also be decided in register set `r`. *(⇒ a register set decides at most one value.)*
- **Rule 4 (Previous decisions):** a client may only write `v` to register set `r` if no different
  value can be decided by register sets `0..r−1`. *(⇒ all register sets agree — agreement.)*

### Implementing the rules

- **Rule 3** two ways (combinable per register set): **intersecting-quorum** configs (all quorums in a
  set share a server) *or* **client-restricted** configs (register sets are assigned to clients
  round-robin; a client writes only its own sets, with at most one value).
- **Rule 4** via per-client **decision tables.** Because registers are write-once and persistent, a
  client's local state table only ever grows monotonically. Each quorum is tracked as one of four
  decision states: **Any → Maybe v / None → Decided v** (Decided/None are final). Reading a non-nil
  `v` pushes predecessors' quorums to `Maybe v` (and conflicting `Maybe v'` to `None`); reading nil
  pushes affected quorums to `None`. A client may write `v` to set `r` once every quorum in `0..r−1`
  is `None`/`Maybe v`/`Decided v`; it may output `v` once any quorum is `Decided v`.

## 4. Paxos as an instance

(Unoptimised) **Paxos = this algorithm with: majority quorums for every register set (intersecting)
+ client-restricted for every register set.** Phase 1 implements Rule 4; phase 2 implements Rule 1.
A round/proposal number maps to a **register set index**. In write-once-register form:

- **Phase 1:** client picks an assigned-but-unused register set `r`, sends `P1a(r)`; each server, if
  `Rr` is unwritten, sets all unwritten registers `0..r−1` to **nil** and replies `P1b(r, S)` with its
  written non-nil registers. On a majority of `P1b`, the client adopts the value from the greatest
  register (or its own input if none) → phase 2.
- **Phase 2:** `P2a(r, v)`; each server, if `Rr` unwritten, nils `0..r−1` and writes `Rr := v`,
  replies `P2b(r, v)`. A majority of `P2b` → `v` decided.

**Weakened quorum intersection (§4.1):** intersection is required *only* between the phase-1 quorum of
register set `r` and the phase-2 quorums of sets `0..r−1`: `∀r, ∀r' < r : I(Q¹ᵣ, Q²ᵣ')`. This
**reconfirms Flexible Paxos** from first principles. **Progress without quorums (§4.2):** a client can
terminate the moment it *learns* a value is decided (not necessarily after finishing both phases); and
once it reads a non-nil `v` from register `r`, it no longer needs to intersect phase-2 quorums up to
`r` — fewer responses needed than "wait for a full quorum."

## 5. Fast Paxos as an instance

**Fast Paxos** additionally allows some register sets to be **fast** (intersecting-quorum, any client
may write, no single-value restriction) while the rest stay **classic** (client-restricted). Classic
quorums need `> ½` of servers; fast quorums need `≥ ¾`. Value selection at the end of phase 1 must
pick the value that *might be decided* — for a fast set with multiple values read, the **most common**.
Weakened intersection adds two requirements over Paxos's: fast sets must self-intersect, and a phase-1
quorum must intersect *pairs* of prior fast phase-2 quorums: `∀r∈F: I(Q²ᵣ,Q²ᵣ)` and
`∀r,∀r'∈F<r: I(Q¹ᵣ, Q²ᵣ', Q²ᵣ')`. Same "progress without quorums" optimisations apply, with extra
slack for classic sets.

## 6. Three new algorithms (the payoff)

Picking different configurations yields novel trade-offs:
- **Co-located consensus:** all-servers quorum for the first `k` sets, majorities after → one round
  trip to all when healthy, two round trips to a majority on failure (server+client co-located).
- **Fixed-majority consensus:** one specific majority for set 0 (intersecting), majorities after
  (client-restricted) → one round trip to a *specific* majority, or two to *any* majority.
- **Reconfigurable consensus:** partition servers into primary/backup; use primary-only quorums for
  sets `0..k−1` and backup-only from set `k`. Running Paxos at set `k` migrates the system to the
  backups — no primary reply needed thereafter. (Reconfiguration as just a configuration change.)

## 7 & Appendix A. Conclusion / proofs

Reframing consensus around write-once registers unifies Paxos / Flexible Paxos / Fast Paxos and shows
all are conservative — their quorum requirements weaken substantially. The stated aim is that the
abstraction makes correctness *intuitive enough that proofs aren't needed to be convinced*; Appendix A
nonetheless proves non-triviality (Rule 2 invariant) and agreement (case analysis on register-set
order, using write-once + quorum intersection / client-restriction), then proves the decision-table
rules implement the four rules, and that (Fast) Paxos implements them.

---

## Why this matters for `paros`

- **The cleanest mental model for a Multi-Paxos slot log.** A "register set" is exactly a **slot**, and
  "write-once register per server per slot" is precisely what an acceptor stores. paros's per-slot
  consensus *is* this abstraction; modelling acceptor state as immutable write-once cells (per slot:
  promised ballot, accepted (ballot,value)) makes safety reasoning local and monotonic — the same
  property that makes the paper's decision tables work makes a sim-tested core easy to reason about.
- **Quorums belong to (phase × slot), and intersection is the only invariant.** Generalises the
  Flexible Paxos result (see [`../flexible-paxos/`](../flexible-paxos/)): the core must enforce only
  `I(Q¹ᵣ, Q²ᵣ')` for `r' < r`. A `paros` `QuorumSystem` trait keyed by phase (and potentially per
  slot) is the right shape; majorities are just the default config.
- **Two implementation strategies for Rule 3 map to real design choices:** *intersecting quorums*
  (any proposer, quorums must overlap — classic Paxos) vs *client-restricted* (partition rounds/slots
  to proposers so writes can't conflict — this is exactly round-robin ballot ownership / the
  `RoundSystem` seen in frankenpaxos and the leader-per-term rule in Paxos-vs-Raft). paros can pick
  per concern.
- **"Progress without quorums" is a concrete latency optimisation** worth keeping on the roadmap: a
  learner can decide as soon as it *observes* a decision, and a leader can skip intersecting prior
  quorums once it has read a committed value — both expressible as outputs of a sans-IO core that
  tracks a decision-table-like view.
- **Reconfiguration falls out as a configuration change over slots** (§6), which connects to the
  matchmaker-paxos reference — both treat "which quorum system governs which slot/round" as
  first-class, carried in state, rather than a bolted-on protocol.
- **Fast Paxos's fast/classic split** is the bridge to [`fast-flexible-paxos`] (not in this set) — if
  paros ever explores a leaderless fast path, the value-selection rule changes to "pick the value that
  *might* be decided / most common," which this paper specifies precisely.
