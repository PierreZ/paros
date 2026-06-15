# Paxos Made Live — An Engineering Perspective

**Authors:** Tushar Chandra, Robert Griesemer, Joshua Redstone (Google)
**Venue:** ACM PODC 2007 (minor revision). June 26, 2007.
**Source:** https://15799.courses.cs.cmu.edu/fall2013/static/papers/paxos_made_live.pdf

---

## Abstract

Experience building a fault-tolerant database using the Paxos consensus algorithm (the engine behind Google's **Chubby**). Despite the existing literature, building such a database was non-trivial. The paper describes selected algorithmic and engineering problems and their solutions. Measurements show a competitive system.

---

## 1. Introduction — Why theory → practice is hard

Fault tolerance via replication + a consensus algorithm applied repeatedly → an identical **log** on every replica; applying the log gives mutually consistent state machines (e.g., a database). Paxos used as the base for a fault-tolerant log, which underlies a fault-tolerant database. Building a production system was hard for several reasons:
- Paxos is a page of pseudocode, but the complete implementation is **several thousand lines of C++** — many features and optimizations (some published, some not).
- Proving one-page algorithms doesn't scale to thousands of lines; **different methods** (testing) were needed for confidence.
- Real-world failure modes far exceed the carefully-selected faults the algorithm tolerates — algorithm bugs, implementation bugs, **operator error**.
- Real systems are **rarely specified precisely**, and the spec may change during implementation → the implementation must be malleable.

---

## 2. Background — Chubby

**Chubby** is Google's fault-tolerant distributed lock service that also stores small files; one instance ("cell") per data center. GFS and Bigtable use it for coordination and metadata. A cell = **5 replicas** running the same code; every Chubby object is an entry in a **replicated database**. One replica is the **master** and serves all requests; non-masters redirect clients to the master. On master failure, a new master is auto-elected and serves from its local DB copy.

The first Chubby was built on a third-party commercial DB ("**3DB**") with a history of replication bugs and an unproven (possibly incorrect) replication mechanism. Given Chubby's importance, Google replaced 3DB with a Paxos-based solution.

---

## 3. Architecture outline

Three layers per replica (Figure 1):
1. **Fault-tolerant replicated log** (Paxos) at the bottom — each replica keeps a local log; Paxos keeps logs identical; replicas talk via a Paxos-specific protocol.
2. **Fault-tolerant replicated database** — a local **snapshot** + a **replay-log** of DB operations. New ops submitted to the replicated log; applied to the local DB when they appear.
3. **Chubby** — stores its state in the DB; clients talk to one replica via a Chubby-specific protocol.

Clean interfaces between Paxos/DB/Chubby — partly for clarity, partly to **reuse the replicated-log layer** in future systems ("a fault-tolerant log is a powerful primitive"). **Log API** (Figure 2): a `submit` call adds a value; once it enters the log, the system invokes a **callback** at each replica with the value. **Multi-threaded** — multiple values submitted concurrently; the log creates no threads of its own (key for testing).

---

## 4. On Paxos

### 4.1 Paxos Basics
Replicas agree on a single value despite crashes (recoverable), message drops, and persistent storage that survives crashes. Three phases (repeatable on failure):
1. **Elect a coordinator.**
2. Coordinator selects a value and broadcasts an **accept** message; others acknowledge or reject.
3. Once a majority acknowledge → consensus; coordinator broadcasts a **commit**.

Because multiple replicas may simultaneously believe they're coordinator (and pick different values), Paxos adds: (1) an **ordering of coordinators** via increasing, unique **sequence numbers** (replica `r` of `n` picks smallest `s` with `s mod n = ir`); (2) a **restriction on value choice**. The propose→promise exchange: a candidate broadcasts a **propose** with a higher sequence number; if a majority reply they've seen nothing higher (**promise** messages), it becomes coordinator. To preserve a previously-chosen value, **promises carry the most recent value heard + its sequence number**; the new coordinator must adopt the value with the highest sequence number (or pick freely if none). Correctness intuition: a majority responds to propose, so any prior consensus is seen by at least one responder and has the highest sequence number.

### 4.2 Multi-Paxos
Repeated Paxos instances → a sequence of values (the log). A **catch-up** mechanism lets lagging replicas catch up. Each replica keeps a **persistent log** of Paxos actions, replayed on recovery and used to help laggards. Naively, the algorithm needs **5 disk writes per instance** (propose, promise, accept, ack, commit — each flushed before proceeding); disk flush can dominate latency.

**Master optimization** (well-known): if the coordinator doesn't change between instances, **propose messages can be omitted**. Pick a long-lived coordinator — the **master** — reducing to **a single disk write per instance per replica**, done in parallel (master writes after sending accept; others write before sending ack). Any replica can still try to become coordinator with a higher sequence number. **Batching** multiple application threads' values into one Paxos instance boosts throughput.

---

## 5. Algorithmic challenges (gaps in the literature)

### 5.1 Handling disk corruption
A corrupted disk losing persistent state lets a replica **renege on past promises** — violating a Paxos assumption. Two manifestations: file contents change (detected via **per-file checksums**) or files become inaccessible (indistinguishable from a fresh empty disk — detected by leaving a **marker in GFS** at first start-up; an empty disk + existing GFS marker ⇒ corruption). A corrupted replica **rebuilds as a non-voting member**: it catches up but sends no promise/ack messages until it observes **one full Paxos instance started after rebuild began**, guaranteeing it couldn't have reneged on an earlier promise. (Enables a future optimization: tolerating occasional corruption could let the system skip immediate disk flushes — not yet implemented.)

### 5.2 Master leases
Reads via Paxos serialize against updates (ensuring fresh reads), but are expensive and the master can't serve reads locally (another replica might have become master and changed the data → stale reads). **Master leases**: while the master holds the lease, no other replica can submit values, so the master's local copy is up-to-date and serves reads **locally**. Master renews before expiry (holds leases for days). All replicas implicitly grant the lease to the previous instance's master and refuse other replicas' Paxos messages while held; the master uses a **shorter** timeout than replicas (guards against clock drift) and refreshes via a dummy **heartbeat** value. **Stability problem:** intermittent outages let a disconnected old master raise its sequence number and later displace the new master → master churn; fixed by having the master **periodically boost its sequence number** with a full Paxos round (<1% of instances run full Paxos under load). Leases can extend to **all replicas** (local reads when reads ≫ writes) — examined but not implemented.

### 5.3 Epoch numbers
A request may arrive at the master, which then loses (and possibly regains) mastership before the request updates the DB. Chubby requires aborting a request if mastership changed during handling. Solution: a **global epoch number** — two reads of it at the master return the same value **iff** the replica was master continuously between them. Stored as a DB entry; **all DB operations are made conditional on it**.

### 5.4 Group membership
Handling changes in the replica set. Paxos itself can implement group membership, but the exact details with Multi-Paxos, disk corruption, etc. are subtle, unaddressed and unproven in the literature — the team had to fill the gaps (details beyond the paper's scope).

### 5.5 Snapshots
An ever-growing log → unbounded disk use and unbounded recovery time. **Snapshot** the data structure directly so the preceding log can be discarded. The Paxos framework only knows about the log's consistency; the **application** owns the data structure and is responsible for taking snapshots, informing the framework, which then **truncates the log** before the snapshot. On recovery: install the latest snapshot + replay the truncated log. Snapshots are **not synchronized** across replicas — each decides independently. Subtleties:
- **Snapshot handle** — the application stores a framework-provided handle (a snapshot of Paxos state: the **Paxos instance number** of the snapshot + the **group membership** at that point) and returns it on recovery so the framework can coordinate snapshot with log.
- **Three-phase snapshotting** (can't freeze the log): (1) request a handle, (2) take the snapshot, possibly in a background thread while still in Paxos (the snapshot must reflect the state at the handle's log position — needs care to snapshot a live-updating structure), (3) inform the framework + pass the handle → log truncated. (First impl briefly blocked to make an in-memory copy; later, **virtually pause-less snapshots** using a "shadow" structure tracking updates during serialization.)
- **Snapshots may fail** — the framework truncates only after being told a snapshot was taken; the app can verify integrity and discard a bad snapshot (and may even take several concurrently).
- **Catch-up via snapshot** — if a laggard can't get old-enough log records, it fetches a snapshot from another replica (handle says up to which instance), installs it, then fetches remaining log records. A leading replica may snapshot/advance while a laggard installs an older snapshot → the laggard must fetch a more recent one; the snapshot provider may fail → contact another. A general mechanism passes **snapshot location info** between replicas (direct transfer or GFS lookup).

### 5.6 Database transactions — MultiOp
Chubby's DB stores key→value (arbitrary strings) with insert/delete/lookup/**cas**/iterate. Log-structured: full-DB snapshot + Paxos log of operations. **cas** made atomic by submitting all its data as a single Paxos value. Generalized into **MultiOp** — a powerful primitive implementing all ops except iteration, applied **atomically**, with three components:
1. **guard** — a list of tests on DB entries (presence/absence/value comparison). All run; if all true → execute `t_op`, else → `f_op`.
2. **t_op** — a list of insert/delete/lookup ops, executed if guard is true.
3. **f_op** — same, executed if guard is false.

When epoch numbers were needed later, MultiOp accommodated it cleanly: store the epoch as a DB entry and add an epoch-check **guard** to every call — evidence that MultiOp is a powerful primitive.

---

## 6. Software Engineering

Users tolerate bugs far less in a fault-tolerant system (a bug can render it unusable). Methods used:

### 6.1 Expressing the algorithm effectively
Coded the core algorithm as **two explicit state machines** in a custom **state-machine specification language** + a compiler to C++. The language is terse (a full algorithm fits on one screen); the compiler also auto-generates state-transition logging and **code-coverage** instrumentation. Payoff: a late, fundamental group-membership change (from a 3-state "join once, never rejoin" model to a 2-state "in/out, can toggle often" model — intermittent failures were more common than expected) took **~1 hour** to change the spec (and 3 days to update tests) — far easier than if intermingled with the rest of the system.

### 6.2 Runtime consistency checking
Liberal `assert`s + explicit verification code. **Distributed DB checksum:** the master periodically submits a checksum request to the log; each replica checksums its local DB; since the Paxos log serializes ops identically, all should match; the master broadcasts its checksum for comparison (uses a shadow structure for concurrency). Three inconsistency incidents: (1) operator error; (2) unexplained — the replica's log replayed consistently, so likely a **random hardware memory corruption**; (3) suspected an **illegal memory access** from errant included code — now keep a **second DB of checksums** and double-check every access. All resolved by manual intervention before reaching Chubby.

### 6.3 Testing
Can't prove a real system correct → test thoroughly. Tests designed in from the start. Two modes:
1. **Safety mode** — verify consistency; progress not required (ops may fail / report unavailable).
2. **Liveness mode** — verify consistency **and progress**; all ops complete.

Tests start in safety mode, inject random failures, then stop injecting, let the system recover, and switch to liveness (to detect deadlock). The **fault-tolerant log test** simulates a random number of replicas through random network outages, message delays, timeouts, crashes/recoveries, file corruptions, schedule interleavings, etc. **Repeatable** via a seeded RNG and **single-threaded** execution (possible because the log creates no threads) → failing seeds re-run with logging in a debugger. Found subtle protocol errors (group membership, disk-corruption handling). Strength measured by leaving known bugs in and confirming detection; then run on **hundreds of machines** — some bugs took weeks of simulated time at high failure rates. Another test injects lower-level failures via **hooks** (crash a replica, disconnect it, force it to drop mastership) — found 5 subtle master-failover bugs in two weeks. Built a **failure-injecting filesystem** too.
**Unsolved challenge:** fault tolerance **masks** bugs/misconfigurations while silently lowering fault tolerance — e.g., a misspelled replica name left a 5-replica cell silently tolerating only 1 failure instead of 2 (the misconfigured replica ran perpetually in catch-up mode). Now detected, but unknown how many other masked problems exist.

### 6.4 Concurrency
Wanted **repeatable** tests → the log has no threads of its own; threading enters only at the networking edges. As the product grew, repeatability had to be sacrificed: Chubby is multi-threaded at its core; the DB became multi-threaded (snapshots, checksums, iterators while serving requests); even the local log handling became multi-threaded. Right goals set, but couldn't fully adhere as needs grew.

---

## 7. Unexpected failures (100+ machine-years in production)

- **Thread starvation → master-churn cascade:** shipping with 10× worker threads caused timeouts under load → rapid master failover → en-masse client migration → new master overwhelmed → more failovers. Rolled back to 3DB in one data center, but the **undocumented, unfamiliar rollback** (no dev present) used the wrong (old) snapshot → **lost 15 hours of data**; key datasets rebuilt.
- **Upgrade leftover files:** a later upgrade failed because old failed-upgrade files weren't deleted → ran on a months-old snapshot for minutes → **lost ~30 minutes of data** (clients recovered).
- **Semantics mismatch:** Chubby expected an op to **fail** if the DB lost mastership mid-op, but the system could re-install the replica as master and succeed → required implementing **epoch numbers** (MultiOp made this easy).
- **Three replica divergences** — caught by the periodic checksum comparison.
- **Upgrade-script failures** (e.g., a basic Google program missing on a cell).
- **OS bug:** Linux 2.4 kernel could hang flushing a small file when many buffered writes to other files exist (right after a DB snapshot write) — small Paxos-log writes stalled seconds. Workaround: write **all large files in small chunks, flushing after each**, protecting critical log writes.

Reflection: a few failures in 100 machine-years is excellent for most systems, but **too high for Chubby**. 3 were upgrade/rollback (fixed per-incident; vanish once cells are upgraded), 2 were since-fixed bugs (mitigated by the verification test), 2 were operator errors during rollout (now use well-tested automated scripts — most recent release rolled out to hundreds of machines without incident), 1 was memory corruption (log-structured design allowed replaying to the exact failure point, confirming logs were correct; added more checksums and now crash a replica on detection).

---

## 8. Measurements

Goal: equal/superior performance to 3DB. Benchmarked full Chubby (client + network + server + DB) on 5 Pentium-class servers, write-intensive (reads are served locally by the lease-holding master and don't exercise Paxos). Each worker repeatedly creates a file and waits (one DB write per op). **Table 1** (Paxos-Chubby with a 100 MB DB vs. 3DB-Chubby with a small DB):

| Test | Workers | File size | Paxos-Chubby | 3DB-Chubby | Speedup |
|------|---------|-----------|--------------|------------|---------|
| Ops/s | 1 | 5 B | 91 ops/s | 75 ops/s | 1.2× |
| Ops/s | 10 | 5 B | 490 ops/s | 134 ops/s | 3.7× |
| Ops/s | 20 | 5 B | 640 ops/s | 178 ops/s | 3.6× |
| MB/s | 1 | 8 KB | 345 KB/s | 172 KB/s | 2× |
| MB/s | 4 | 8 KB | 777–949 KB/s | 217 KB/s | 3.6–4.4× |
| MB/s | 1 | 32 KB | 672–822 KB/s | 338 KB/s | 2.0–2.4× |

Multi-worker results show **batching** gains. The last two show **snapshot** cost: configured to snapshot when the log exceeds 100 MB (~every 100 s), performance dips during a snapshot (extra DB copy to disk). Not optimized for performance; given the win over 3DB, further optimization isn't a priority.

---

## 9. Summary and open problems

Despite 15+ years of literature and an experienced team, the system was significantly harder to build than anticipated, attributed to shortcomings in the field:
- **Large gaps between the Paxos description and a real system** — experts must assemble scattered ideas and make many small extensions; cumulative effort is substantial and the final protocol is **unproven**.
- The fault-tolerance community **hasn't built tools** to make implementation easy.
- The community **hasn't emphasized testing** enough — a key ingredient.

Contrast with **compiler construction**: tools like yacc/ANTLR/CoCO-R emerged soon after the theory matured, and once-cutting-edge topics like parsing are now "solved" and taught to undergrads. The fault-tolerant distributed computing community hasn't closed its theory↔practice gap with comparable vigor; these gaps are non-trivial and merit research attention.

---

## Key Takeaways
- **A one-page algorithm becomes thousands of lines** of production code; the gap is features, optimizations, and unaddressed real-world cases (disk corruption, group membership, snapshots), not verbosity.
- **Master + Multi-Paxos** turns 5 disk writes per instance into 1 and skips propose messages; **master leases** make reads local (huge, since reads dominate). Guard master churn by periodically boosting the sequence number.
- **Snapshots are subtle** — log/snapshot must stay mutually consistent via a **snapshot handle** (Paxos instance # + group membership); three-phase, failure-tolerant, app-owned, log truncated only after confirmation.
- **MultiOp** (guard / t_op / f_op, applied atomically) gives transaction-style power without a real transaction system, and cleanly absorbed the later **epoch-number** requirement.
- **Disk corruption** breaks Paxos's no-renege assumption → checksums + GFS markers + rebuild as a non-voting member until one full post-rebuild instance is observed.
- Confidence comes from **testing, not proofs**: seeded, repeatable, single-threaded fault injection (safety vs. liveness modes), run on hundreds of machines; a custom **state-machine spec language + compiler** isolates the core algorithm for reasoning/changes.
- **Fault tolerance masks bugs and operator/config errors** while silently eroding redundancy — a genuinely hard, unsolved testing problem.
- Real production pain was mostly **operator error, upgrades/rollbacks, OS bugs, and hardware corruption** — not the consensus core. The field needs better **tools and testing culture** to close the theory↔practice gap (cf. compilers).
