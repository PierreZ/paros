# Paxos Made Moderately Complex

**Authors:** Robbert van Renesse (Cornell). The published version adds Deniz Altinbuken.
**Venue:** ACM Computing Surveys, Vol. 47, No. 3, Article 42 (April 2015). DOI 10.1145/2673577.
**Source:** https://paxos.systems/paper/ (companion site) — PDF from https://www.cs.cornell.edu/home/rvr/Paxos/
**Code:** Java reference implementation included in the appendix; also at https://www.cs.cornell.edu/home/rvr/Paxos/ and https://paxos.systems/code/

> Note: this PDF is the author-hosted version (with the full Java listing). The companion site **paxos.systems** renders the same content as HTML, with a glossary, code, and variants.

---

## Abstract / Premise

Paxos is *not* a simple protocol to implement, even though it rests on simple invariants. The paper gives **pseudocode for the full multi-decree Paxos (Multi-Paxos)** protocol — not shying away from implementation detail — structured around **invariants** rather than correctness proofs. It starts with an unoptimized, comprehensible version, then covers liveness and the optimizations that make it practical. The pedagogical device: Paxos as **five kinds of processes**, each with a simple operational spec — **clients, replicas, leaders (with scouts and commanders), and acceptors.**

---

## 1. Introduction — terms and model

- **State machine:** states + transitions + current state; a transition responds to an operation and produces output. Read-only ops are self-transitions. **Deterministic** = transition/output are a function only of state + operation.
- **Asynchronous environment:** no timing bounds (clocks, network, transitions all take arbitrarily long).
- **Crash failure:** a state machine that makes no more transitions. In an asynchronous environment, crashes **cannot be reliably detected** (unlike fail-stop).
- **State Machine Replication (SMR):** replicate a deterministic state machine; feed all replicas the **same sequence of operations** → same states/outputs. Assume ≥ one replica never crashes (but not which).
- A **command** is `⟨κ, cid, operation⟩` (client id, client-local unique command id, operation). A client may reissue the same operation under different cids; responses carry the cid for matching.
- The SMR stub broadcasts a command to all replicas and returns the **first** response.
- **Messaging assumption:** reliable but not FIFO — a message from a non-faulty sender to a non-faulty receiver is eventually received (≥ once); received messages were genuinely sent (no garbling, no spontaneous messages).

The paper covers **multi-decree** Paxos (the industrially used one — Chubby, ZooKeeper), not single-decree. It stresses **invariants**: (1) each operation can be checked against them (if it holds before, it holds after), and (2) they make correctness intuitive without full proofs.

---

## 2. How and Why Paxos Works

### Replicas and Slots
To tolerate `f` crashes, need ≥ `f+1` **replicas**. Replicas fill a sequence of **slots** with commands; a `(s, c)` pair is a **proposal** for slot `s`. On a client request, a replica proposes the command for its lowest unused slot, but **waits for a decision** before applying it (concurrent clients may cause different replicas to propose different commands for the same slot). Each replica `ρ` holds: `ρ.state`, `ρ.slot_num` (next slot needing a decision, init 1), `ρ.proposals`, `ρ.decisions`.

**Replica invariants:**
- **R1:** no two different commands decided for the same slot.
- **R2:** all slots up to `slot_num` have decisions.
- **R3:** `ρ.state` = applying the decided commands in slot order from `initial_state`.
- **R4:** `slot_num` never decreases.

R1–R3 ⇒ all replicas apply ops in the same order (same slot_num ⇒ same state). On a **request**, the replica calls `propose(c)` (ignored if already decided). On a **decision**, it adds to `decisions` and, in slot order, **re-proposes** any of its own commands that lost their slot, then `perform()`s the decided command (executing the op only if new; always incrementing `slot_num`). `proposals`/`decisions` are **append-only** (simplifies invariants; GC discussed in §4.2). The hard part is enforcing **R1** — agreement on the order of commands — which is **consensus**, implemented by the **multi-decree Synod protocol**.

### The Synod Protocol, Ballots, and Acceptors
- An infinite collection of **ballots** (they "just are", not created) — key to liveness. Each ballot has a unique **leader** (a deterministic state machine). Tolerating `f` failures needs ≥ `f+1` leaders.
- **Ballot numbers** are totally ordered, here lexicographic `(int, leader-id)` pairs (so the leader is read off the ballot number). A special `⊥` precedes all. **Ballot numbers ≠ slot numbers** (orthogonal: one ballot decides many slots; one slot may be considered by many ballots).
- **Acceptors** (≥ `2f+1`, tolerating a strict minority of crashes) hold the fault-tolerant memory. A **pvalue** is `⟨b, s, c⟩` (ballot, slot, command). Acceptor `α` state: `α.ballot_num` (init `⊥`), `α.accepted` (set of pvalues). To **adopt** `b` = set ballot_num to `b`; to **accept** `p` = add `p` to accepted.

**Acceptor invariants:**
- **A1:** an acceptor adopts strictly increasing ballot numbers.
- **A2:** `α` accepts `⟨b,s,c⟩` only if `b = α.ballot_num`.
- **A3:** `α` never removes pvalues (relaxed later for GC).
- **A4:** for a given ballot+slot, at most one command across all acceptors.
- **A5 (crucial):** if a majority accepted `⟨b,s,c⟩`, then any `⟨b', s, c'⟩` with `b' > b` has `c' = c`. (Works "both ways": a majority-accepted value forces all later ballots to the same command; and a value accepted on a later ballot before any earlier majority constrains earlier ballots too.)

**Acceptor pseudocode (Figure 2):** on `⟨p1a, λ, b⟩` adopt `b` if higher, reply `⟨p1b, ballot_num, accepted⟩`. On `⟨p2a, λ, ⟨b,s,c⟩⟩` if `b ≥ ballot_num` adopt+accept, reply `⟨p2b, ballot_num⟩`. Trivially enforces A1–A3.

### Leaders, Commanders, Scouts
The leader of a ballot picks a command per slot so it can't conflict with other ballots (A5). To get `⟨b,s,c⟩` chosen it spawns a **commander** thread. **Commander invariants:** **C1** (≤ one commander per ballot+slot → implies A4) and **C2** (majority accepted `⟨b,s,c⟩` + commander spawned for `⟨b',s,c'⟩`, `b'>b` ⇒ `c'=c` → implies A5).

- **Commander (Figure 3a):** sends `p2a` to all acceptors. If it gets `p2b` (matching ballot) from a **majority** → command chosen → notify replicas with `decision`, exit. If it gets a `p2b` with a different (higher) ballot → ballot preempted → tell leader `preempted`, exit.
- **Scout (Figure 3b):** runs the **view change** (Phase 1) for a ballot. Sends `p1a` to all; on `p1b` from a **majority**, returns `⟨adopted, b, ∪ pvalues⟩` to the leader (the union lets the leader enforce C2). On a higher ballot → `preempted`.

**Leader (Figure 4):** state = `ballot_num` (init `(0, self)`), `active` (init false), `proposals`. Starts a scout. Handles three messages:
- **propose** — record the proposal; if active, spawn a commander.
- **adopted** — became active; update proposals via `pmax` and spawn commanders.
- **preempted** (higher ballot) — go passive, bump ballot number, spawn a new scout.

**Passive → active selection (the correctness crux):** once active, the leader knows a majority `A` adopted `ballot_num` (so they won't accept lower ballots, by A1/A2) and has their pvalues. Per slot:
1. **No pvalue in pvals** → nothing was/will be chosen below `ballot_num` (majority intersection argument) → the leader may propose **any** command (enforcing C2).
2. **Otherwise** → take the pvalue with the **max ballot** for that slot (unique by A4); that ballot's leader already picked it to satisfy C2, and no pvalue between `b` and `ballot_num` can be chosen → reuse its command `c` (enforcing C2).

This inductive argument: C2 ⇒ A5 ⇒ R1. The leader computes `pmax(pvals)` (per slot, the command of the max-ballot pvalue) and merges with `◁` (`x ◁ y` = `y` plus entries of `x` with slots not in `y`). New proposals while active satisfy C2 directly. A higher ballot seen by any scout/commander → `preempted` → leader goes passive and restarts with a higher ballot.

---

## 3. When Paxos Works (Liveness)

**Liveness fails** even with no failures: two leaders `λ < λ'` can **ping-pong** indefinitely, each preempting the other's ballot before it can get a majority to accept — no slot ever decided, even if both propose the same command. This is the **FLP impossibility result**: no consensus protocol in an asynchronous environment with crash failures can guarantee termination (randomization doesn't escape it either). *(Aside: failures can help liveness — if all but one leader fail, Paxos terminates.)*

**Fix via failure detection + weak timing assumptions.** When preempted by a higher ballot's leader `λ'`, instead of immediately escalating, `λ` **monitors `λ'` by pinging**; it only escalates if `λ'` stops responding. Assume (without knowing the bounds) that **clock drift** and **message delivery+handling time** are bounded. Use **ballot-number-dependent timeouts** (higher competing ballot → longer wait), so eventually some correct leader's ballot survives. For good performance, tune timeouts with a **TCP-like AIMD** scheme (multiplicative increase on preemption, linear decrease per chosen proposal). *(Aside: calling this "leader election" is misleading — each ballot has a fixed, unelected leader.)*

Further liveness: avoid crashes by keeping acceptor/replica/leader state **on disk** (a power-failure-then-recover process is "slow", not crashed; only a permanent disk failure counts as crashed). Rely on **reliable communication** (retransmit-until-ack); a client retransmits until it gets a response. With ≥ `f+1` replicas, ≥ one assigns a slot and proposes; a losing command gets reassigned to a new slot (possible starvation, but any outstanding request eventually decided in some slot absent new requests).

---

## 4. Paxos Made Pragmatic (optimizations)

### 4.1 State Reduction
The leader only needs, per slot, **the max pvalue** (or empty). So acceptors keep only the **most recently accepted pvalue per slot** and return those in `p1b`. (A subtle but safe effect: history can be "overwritten" so a majority may no longer share the same most-recent pvalue — but by C2 any later ballot still selects the same command, so no inconsistency. **Footnote warning:** do *not* conclude a value is chosen merely because a majority's *most-recent* pvalues agree — that's wrong.) Leaders also track **already-decided slots**: include the first unknown slot in `p1a` (acceptors skip smaller slots); don't spawn commanders or keep proposals for decided slots. A replica's `requests` only needs slots above `slot_num`.

### 4.2 Garbage Collection
Once **all** replicas have performed a slot's command, acceptors can drop its pvalues. Acceptors track a low-water slot number (included in `p1b` so leaders don't misread missing pvalues). This breaks if a replica is faulty/slow → use **`2f+1` replicas** and GC when **> f** replicas have performed a command; a lagging replica that misses a decision fetches a **snapshot** from another replica. Alternatively make the replica set **dynamic** (reconfiguration via a special command).

### 4.3 Co-location
Typically **leaders co-located with replicas** — a replica forwards its proposal to its local leader (passive leader forwards to the active one; active leader spawns a commander). Alternative: **clients co-located with leaders** → resembles **Quorum Replication** but supporting arbitrary deterministic ops (unpopular — too much trust in clients). Replicas often co-located with acceptors (need as many replicas as acceptors anyway); co-located leaders+acceptors must use **separate ballot-number variables**.

### 4.4 Read-only Commands
Reads get no special treatment by default → unnecessary overhead, but naively querying one replica risks **stale state**. Reads are sent to the active leader; one optimization is to send a chosen read to a **single** replica (only one needs to compute the result; fall back to broadcast if that replica is faulty). To serve reads **without accepting a pvalue**, the leader must know its ballot is still current — running a scout per read is too costly. **Leases** (assuming bounded clock drift): the leader records the time and includes a **lease period** in `p1a`; an acceptor adopting the ballot promises not to adopt a higher one until the lease expires (local clock). With a majority, the leader knows no other leader can introduce updates during the lease → it can serve reads from a (local) replica after outstanding updates decide. Integrable with the adaptive-timeout scheme.

---

## 5. Exercises
Nine exercises against the accompanying Java code: implement state reduction; simplify ballot numbers for a fixed ordered leader set (`i, i+n, i+2n, …`, `⊥ = 0`); a replicated bank app; real TCP transport (treat TCP as unreliable, reconnect); the failure-detection scheme (mostly one active leader); leader/replica co-location + leader-state GC; persist acceptor/leader state to disk (handle mid-save crashes); acceptor pvalue GC; the read-only leasing scheme.

---

## 6. Conclusion & Lineage
Paxos presented as **five process types** with simple operational specs; start impractical-but-clear, then optimize to practical. Situated in a long line of Paxos papers: **Viewstamped Replication** (Oki & Liskov, 1988); Lamport's **Part-Time Parliament** (written 1989, published 1998); Lampson's pseudocode explanation (1996) and ABCD's of Paxos (2001); De Prisco/Lampson/Lynch (2000); Lamport's **Paxos Made Simple** (2001); Boichat et al. "Deconstructing Paxos" (2003); **Chandra et al. "Paxos Made Live"** (2007) *(footnote: "there is a subtle bug in their implementation — see if you can spot it")*; Li et al. "Paxos register" (2007); Mazières "Paxos Made Practical" (2007); Kirsch & Amir (2008); Alvaro et al. Paxos-in-Overlog (2009).

---

## Appendix — Java Reference Implementation
Complete, runnable Java (`java Env`) mirroring the pseudocode:
- **ProcessId** (string), **Command** `(client, req_id, op)`, **BallotNumber** `(round, leader_id)` lexicographic, **PValue** `(ballot, slot, command)`.
- **PaxosMessage** hierarchy: `P1a/P1b/P2a/P2b/Preempted/Adopted/Decision/Request/Propose`.
- **Queue** (in-memory message queue — no real network) and **Process** (a Thread with an inbox + `Env`).
- **Replica**, **Acceptor**, **Commander**, **Scout**, **Leader** — each closely matching its figure (note: commanders/scouts use `2 * waitfor.size() >= acceptors.length` as the majority test).
- **Env** — creates `nAcceptors=3, nReplicas=2, nLeaders=2`, fires `nRequests` and routes messages.

---

## Key Takeaways
- **Five roles, simple specs:** clients → replicas (slots/decisions) → leaders (with **scouts** for Phase 1 and **commanders** for Phase 2) → acceptors (the fault-tolerant memory). `f+1` replicas, `f+1` leaders, `2f+1` acceptors.
- **Invariant-driven:** R1 (no two commands per slot) is the hard one; it reduces to **C2 ⇒ A5 ⇒ R1**. The leader's active-mode value-selection (max-ballot pvalue, or free choice if none) is the correctness crux.
- **Ballots ≠ slots.** Ballot numbers `(round, leader-id)` make the leader readable and totally ordered; one ballot decides many slots.
- **FLP is unavoidable** — the unoptimized Synod can livelock with two dueling leaders. Practical liveness comes from **failure detection + ballot-dependent (AIMD) timeouts** under weak timing assumptions, not from a stronger safety argument.
- **Practicality** needs: **state reduction** (keep only the max pvalue per slot), **garbage collection** (needs `2f+1` replicas, GC when `>f` performed), **co-location** (leader+replica), and **leases** for cheap, consistent **read-only** ops.
- A reference **Java implementation** maps 1:1 to the pseudocode — the whole point of "moderately complex": bridge the Paxos-Made-Simple ↔ Paxos-Made-Live gap.
