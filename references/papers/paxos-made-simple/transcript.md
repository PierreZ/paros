# Paxos Made Simple

**Author:** Leslie Lamport
**Date:** 01 Nov 2001 (ACM SIGACT News, Distributed Computing Column, 32(4):51–58, 2001).
**Source:** https://lamport.azurewebsites.net/pubs/paxos-simple.pdf

**Abstract (in full):** *"The Paxos algorithm, when presented in plain English, is very simple."*

---

## 1. Introduction

Paxos was regarded as hard to understand — partly because the original presentation ("The Part-Time Parliament") "was Greek to many readers." In fact it is among the simplest distributed algorithms. At its heart is a **consensus algorithm** (the "synod" algorithm), which the paper shows "follows almost unavoidably from the properties we want it to satisfy." The complete Paxos algorithm is then obtained by applying consensus to the **state machine approach** (Schneider's well-known SMR technique).

---

## 2. The Consensus Algorithm

### 2.1 The Problem
Processes propose values; consensus chooses a single one. **Safety requirements:**
- Only a **proposed** value may be chosen.
- Only a **single** value is chosen.
- A process never learns a value as chosen unless it actually has been.

Liveness goal (not precisely specified): some proposed value is eventually chosen, and a process can eventually learn it.

**Three roles** (agents): **proposers, acceptors, learners** (one process can play several). Model: **asynchronous, non-Byzantine**. Agents run at arbitrary speed, may fail by stopping and may restart (so they need **stable storage** to remember information across a restart). Messages take arbitrarily long, may be duplicated or lost, but **are not corrupted**.

### 2.2 Choosing a Value — derived almost inevitably
A single acceptor is unsatisfactory (its failure blocks progress). Use **multiple acceptors**; a value is **chosen** when a **majority** accept it (any two majorities intersect, so at most one value can be chosen — *if* an acceptor accepts at most one value).

To choose a value even when only one is proposed:
- **P1.** An acceptor must accept the **first** proposal it receives.

But P1 + "majority to choose" forces acceptors to accept **more than one** proposal → number each proposal: a proposal is `(number, value)` with globally **unique numbers**. A value is chosen when a single proposal with that value is accepted by a majority.

Allowing multiple proposals to be chosen but requiring them to agree, it suffices (by induction on proposal number):
- **P2.** If a proposal with value `v` is chosen, then every **higher-numbered** proposal that is chosen has value `v`.

Strengthen down the chain (each implies the previous):
- **P2a.** … every higher-numbered proposal **accepted by any acceptor** has value `v`.
- **P2b.** … every higher-numbered proposal **issued by any proposer** has value `v`. (Needed because an acceptor that never saw the chosen proposal would, by P1, accept a conflicting higher one — so constrain the *proposer*.)

To satisfy P2b, derive the key invariant by induction:
- **P2c.** For any `v, n`: if a proposal `(n, v)` is issued, there is a majority set `S` of acceptors such that **either** (a) no acceptor in `S` has accepted any proposal numbered `< n`, **or** (b) `v` is the value of the **highest-numbered proposal `< n` accepted by the acceptors in `S`**.

**Proposer algorithm** (to maintain P2c): instead of predicting future acceptances, **control** them by extracting a promise:
1. Choose a new number `n`; send a **prepare request** `n` to some set of acceptors, asking each to reply with (a) a **promise** never again to accept a proposal numbered `< n`, and (b) the highest-numbered proposal `< n` it has accepted, if any.
2. On responses from a **majority**, send an **accept request** `(n, v)` where `v` is the highest-numbered value among the responses, or **any** value if none reported. (The accept set need not be the same as the prepare set.)

**Acceptor algorithm:** an acceptor can always reply to a prepare request; it may accept an accept request **iff it hasn't promised otherwise**:
- **P1a.** An acceptor can accept a proposal numbered `n` iff it has **not responded to a prepare request numbered > n**. (P1a subsumes P1.)

**Optimization:** an acceptor ignores a prepare request `n` if it already promised a higher number (it would reject the proposal anyway), and ignores a prepare for a proposal it already accepted. So an acceptor need only remember **the highest-numbered proposal it has accepted** and **the highest prepare number it has responded to** — both kept in **stable storage** across restarts. A proposer may abandon a proposal anytime (as long as it never reuses that number).

**The two-phase algorithm:**
- **Phase 1.** (a) Proposer picks `n`, sends **prepare** `n` to a majority. (b) If an acceptor sees a prepare `n` higher than any it has answered, it **promises** (no proposals `< n`) and returns its highest accepted proposal (if any).
- **Phase 2.** (a) On prepare responses from a majority, the proposer sends **accept** `(n, v)` (`v` = highest-numbered response value, or any value if none). (b) An acceptor accepts unless it has since promised a higher number.

A rejected proposer should be informed so it can abandon and retry (performance, not correctness).

### 2.3 Learning a Chosen Value
A learner must learn a proposal was accepted by a majority. Options trade reliability vs. communication:
- Each acceptor → **all learners** (fastest; `acceptors × learners` messages).
- Acceptors → **one distinguished learner** → others (one extra round, less reliable if it fails; `acceptors + learners` messages).
- Acceptors → a **set** of distinguished learners (tunable reliability vs. cost).

Because of message loss a value may be chosen with no learner noticing; a learner that needs to know can **have a proposer issue a (new) proposal**.

### 2.4 Progress
Two proposers can **dueling-livelock**: each completes phase 1 with an ever-higher number, preempting the other's phase 2, forever. **Fix:** elect a **single distinguished proposer** (leader); if it can reach a majority with a high-enough number, it succeeds. By **FLP** (Fischer–Lynch–Paterson), a reliable leader-election must use **randomness or real time** (e.g., timeouts). **Safety holds regardless** of election success or failure.

### 2.5 The Implementation
Each process plays proposer + acceptor + learner. The algorithm elects a **leader** = distinguished proposer + distinguished learner. Messages are ordinary, response messages tagged with the proposal number. **Stable storage** holds what an acceptor must remember; an acceptor records its intended response **before sending it**. Unique proposal numbers: proposers draw from **disjoint number sets**; each remembers (in stable storage) its highest tried number.

---

## 3. Implementing a State Machine

A distributed system = clients issuing commands to a **deterministic state machine** (current state + command → output + new state; e.g., bank tellers over account balances). A single central server fails with the server, so use **multiple servers each running the state machine**; determinism ⇒ same command sequence ⇒ same states/outputs, and a client can use any server's output.

To agree on the command sequence, run **a separate instance of Paxos consensus per command slot** — the value chosen by the `i`-th instance is the `i`-th command. Each server plays all roles in every instance. (Initially the server set is fixed.)

**Normal operation:** elect one **leader** = distinguished proposer for all instances. Clients send commands to the leader, which assigns each a sequence position (e.g., "make this the 135th command" = chosen value of instance 135). It usually succeeds; it may fail (crash, or a rival leader) — but consensus guarantees **at most one** command per slot. **Efficiency key:** the proposed value isn't fixed until **phase 2**.

**New-leader recovery (worked example):** a new leader (a learner) knows commands 1–134, 138, 139. It runs **phase 1** for instances 135–137 and **all** instances `> 139` using **one proposal number** — a single short message, since an acceptor replies with more than "OK" only for slots where it already accepted a phase-2 value (here only 135 and 140). Phase 1 may constrain some slots (say 135 and 140); the leader runs phase 2 there to choose them. It **fills gaps** (136, 137) with a special **no-op** command (leaves state unchanged) so execution can proceed; then 1–140 are chosen and it assigns new client commands to 141, 142, … freely.

**Pipelining and gaps:** the leader can propose command `142` before learning `141` was chosen; if it gets `α` commands ahead (propose `i+1…i+α` after `1…i` chosen), a crash can leave a gap of up to `α−1` commands.

**Cost & optimality:** since leader changes are rare, the steady-state cost per command is just **phase 2**. Phase 2 of Paxos has been shown to have the **minimum possible cost** of any fault-tolerant agreement algorithm (Keidar & Rajsbaum) — so Paxos is **essentially optimal**.

**Abnormal cases:** if no leader → no new commands; if multiple leaders → they may collide in an instance and stall it — but **safety is always preserved** (servers never disagree on the `i`-th command). Leader election is needed only for **progress**.

**Reconfiguration:** if the server set can change, decide it **through the state machine itself** — make the current server set part of the state, changed by ordinary commands. Let instance `i+α` be served by the set specified by the state after command `i`. This gives a simple implementation of an arbitrarily sophisticated reconfiguration algorithm.

---

## References (cited)
FLP impossibility [1]; Keidar & Rajsbaum on the cost of fault-tolerant consensus [2]; Lamport "Implementation of reliable distributed multiprocess systems" (quorum generalization) [3]; Lamport "Time, Clocks…" (state machine approach) [4]; Lamport "The Part-Time Parliament" (original Paxos) [5].

---

## Key Takeaways
- **The consensus algorithm is *derived*, not invented:** start from "majority chooses," add P1 (accept the first), discover you need numbered proposals, then tighten P2 → P2a → P2b → **P2c**, which directly yields the **two-phase prepare/accept** protocol. This derivation is the paper's whole pedagogical point.
- **Two phases:** Phase 1 = prepare/promise (learn prior values + block lower numbers); Phase 2 = accept (propose the constrained-or-free value). The proposer's freedom to pick **any** value in phase 2 (when unconstrained) is what makes Multi-Paxos efficient.
- **Safety vs. liveness are cleanly separated:** safety holds with multiple/zero leaders; a **single distinguished proposer** is needed only for progress, and FLP forces it to use timeouts/randomness.
- **Stable storage is mandatory** — acceptors must remember the highest accepted proposal and highest promised prepare number across crashes.
- **Multi-Paxos = one consensus instance per log slot**, a leader assigning positions, **no-ops** to fill gaps, phase-1-once-per-leader (a single short message covers infinitely many slots), and steady-state cost = phase 2 (provably optimal).
- **Reconfiguration via the state machine itself** (server set is part of the state, changed by commands, with an `α`-command lookahead). This is the seed of later reconfiguration work (cf. [[matchmaker-paxos]]).
- Foundational ancestor of the other references here: the engineering reality is [[paxos-made-live]], the full operational pseudocode/implementation is [[paxos-made-moderately-complex]], and throughput/recovery/reconfig extensions are [[scaling-rsm-compartmentalization]], [[protocol-aware-recovery]], [[matchmaker-paxos]].
