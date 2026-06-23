# Protocol-Aware Recovery for Consensus-Based Distributed Storage

**Authors:** Ramnatthan Alagappan, Aishwarya Ganesan (UW–Madison), Eric Lee (UT Austin), Aws Albarghouthi (UW–Madison), Vijay Chidambaram (UT Austin), Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau (UW–Madison)

**Venue:** ACM Transactions on Storage, Vol. 14, No. 3, Article 21 (Oct 2018). Extended version of the FAST '18 paper.
**Source:** https://research.cs.wisc.edu/adsl/Publications/par-tos18.pdf
**DOI:** 10.1145/3241062

---

## Abstract

Introduces **protocol-aware recovery (Par)** — exploit protocol-specific knowledge to correctly recover from **storage faults** (corruption / inaccessible data) in distributed systems. Demonstrated via **corruption-tolerant replication (Ctrl)**, a Par mechanism for replicated state machine (RSM) systems. Ctrl versions of **LogCabin** (Raft) and **ZooKeeper** (ZAB) safely recover from storage faults with high availability and little performance overhead, while unmodified versions can lose data or become unavailable.

---

## 1. Introduction

Redundancy across nodes is central to reliability. In a *static* setting, recovering corrupted data is trivial (fetch from any node). In a *dynamic* setting it's surprisingly hard: when a node recovers corrupted data, some nodes may be down, and some may be lagging/stale — fixing from a stale node can **overwrite newer data → data loss**.

A recovery approach should be **protocol-aware**: designed around how the system performs updates, elects the leader, etc. (e.g., a faulty node must query at least `R` other nodes to recover safely). The paper applies Par to RSM systems because (1) correct recovery is hardest there (strong consistency/durability), and (2) RSM systems hold critical data others depend on (Bigtable/GFS on Chubby/ZooKeeper).

Contributions: an **RSM recovery taxonomy** (most deployed approaches use no protocol knowledge → data loss/unavailability), and **Ctrl**, with three novel techniques:
- **Crash-corruption disentanglement** (storage layer) — distinguish crash-induced from disk-induced corruption.
- **Global-commitment determination** (distributed recovery) — separate committed (must recover) from uncommitted (safe to discard quickly) items.
- **Leader-initiated snapshotting** — identical snapshots across nodes to simplify recovery.

---

## 2. Background and Motivation

### 2.1 Storage Faults
Disks/SSDs exhibit **block errors (latent sector errors)** — device throws an error on access — and **corruption** — silent, from lost/misdirected writes. Rare per-device but prevalent at scale. Local file systems either propagate the fault (ext4 returns corrupted data) or transform it (btrfs returns an error). RAID-like per-node redundancy is wasteful when data is already replicated across nodes — use the inherent cross-node redundancy.

### 2.2 RSM-Based Storage Systems
Nodes execute identical commands on a state machine. Leader durably writes each command to an on-disk **log**, replicates to followers; **committed** once a majority persist it. Commands applied **in-order**; losing/overwriting committed commands violates safety. Periodically a **snapshot** of the state machine is written and the log is garbage collected. Each node keeps three persistent structures: the **log**, **snapshots**, and **metainfo** (critical per-node metadata, e.g., log start index, current epoch, votes).

### 2.3 RSM Recovery Taxonomy (all current approaches are unsafe and/or unavailable)
Analyzed ZooKeeper, LogCabin, etcd, a Paxos-based system via fault injection. **In all of Figure 1's scenarios, entries 1/2/3 are committed.**

| Class | Approach | Problem |
|-------|----------|---------|
| Oblivious | **NoDetection** | trust the stack; serves corrupted data → unsafe |
| Oblivious | **Crash** | checksum + crash on fault; safe but **severe unavailability** (persistent faults re-crash; single fault → unavailable) |
| Oblivious | **Truncate** | discard faulty entry + all after it; faulty node + lagging nodes form a majority → **silent global data loss** (Fig. 2) and slow recovery |
| Oblivious | **DeleteRebuild** | delete all data + restart; same data loss + slow recovery as Truncate |
| Aware | **MarkNonVoting** (Google Paxos) | delete data, become non-voting until one consensus round; can still violate safety (lost *promises* let an old leader overwrite a committed entry); single fault → unavailable |
| Aware | **Reconfigure** | remove faulty node, add new; needs a majority to commit config change → unavailable in many cases |
| Aware | **BFT** | theoretically tolerates storage faults but ~½ throughput, needs `3f+1` nodes → mostly unavailable |
| Aware | **Ctrl** | **safe + highly available in all cases** |

**Truncate safety violation (Fig. 2):** S1's entry 1 corrupts; S1 truncates (loses 1,2,3); S2(leader)+S3 crash; S1,S4,S5 form a majority, elect S1, commit x,y,z over 1,2,3; when S2,S3 recover they follow S1 → committed data gone.

---

## 3. Corruption-Tolerant Replication (Ctrl)

Built on protocol-level observations common to RSM systems:
- **Leader-Based** — all updates flow through one leader.
- **Epochs** — time partitioned into epochs; one leader per epoch; an `⟨epoch, index⟩` pair **uniquely identifies a log entry**.
- **Leader Completeness** — a node won't vote for a less up-to-date candidate, so the elected leader has **all committed data**.

(Ctrl doesn't directly apply to multi-leader Paxos variants — left as future work.) Ctrl = **local storage layer (Clstore)** detects faults + **distributed recovery protocol** recovers from redundant copies.

### 3.1 Fault Model
Crash + network failures (standard) **plus** persistent data corrupted/inaccessible. Faults in **user data** (corrupted/inaccessible blocks) and **FS metadata** (missing/unopenable files, wrong size, read-only/unmountable FS). Verified these outcomes occur on ext4.

### 3.2 Guarantees
If ≥ one correct copy of a **committed** item exists, it will be recovered (or the system waits for it to be fixed) — committed data is never lost. If all copies of a committed item are faulty, the system **correctly remains unavailable**. Decisions about **uncommitted** faulty items are made as early as possible for availability.

### 3.3 Ctrl Local Storage Layer (Clstore)
Three requirements: reliably **detect** faults, **disentangle** crashes from corruptions, **identify** which pieces are faulty.

- **Structures & granularity:** Log → per-**entry** recovery, id = `⟨epoch, index⟩`. Snapshot → per-**chunk** recovery, id = `⟨snap-index, chunk#⟩`. **Metainfo cannot be recovered from other nodes** (it's node-unique) → store **two local copies**, checksummed (tiny, infrequent).
- **Detection:** inaccessible data via return codes (EIO); corruption via checksum mismatch (assume item+checksum agree ⇒ not faulty). FS metadata faults handled (fixed-size preallocated files; stored snapshot size cross-checked). On most FS metadata faults, Clstore **crashes the node** (safe; metadata blocks are far rarer than data blocks).
- **Disentangling crashes vs. corruption in the log:** A checksum mismatch can be a **crash** mid-write (partial entry — safe to discard, never acked) or a **corruption** (could be committed — must not discard). Clstore writes a **persist record `pi`** after entry `ei`. Append protocol (no extra fsync): `t1: write(ei), t2: write(pi), t3: fsync()`. On mismatch for `ei`: if `pi` absent → crash, discard. If `pi` present → corruption, **unless** `ei` is the **last entry** (a crash between t2 and t3 could persist `pi` while `ei` is partial, since FS can reorder writes and `ei` spans sectors). **Last-entry ambiguity is a fundamental limitation** of log-based systems (Appendix A proof); Clstore marks it *corrupted* and lets distributed recovery decide. Snapshots/metainfo use temp-file + atomic rename, so they never have crash-induced corruption.
- **Identifying faulty data:** store the **identifier separately** from the item (a misdirected write could corrupt both if adjacent). `⟨epoch, index⟩` (+ offset) stored at the head of the log doubles as the persist record. Nominal overhead (32 B log, 12 B snapshot), atomically written, checksummed. If both item and identifier are faulty (unlikely), crash the node.

### 3.4 Ctrl Distributed Log Recovery
**Naive (Leader Restriction):** forbid electing a leader whose log has a faulty entry → leader is clean and fixes followers. **Fixing followers** is easy: followers report faulty `⟨epoch, index⟩`; leader supplies correct entries, or — if the leader lacks an entry the follower has — that entry must be **uncommitted** (leader completeness), so the leader tells the follower to truncate it. **Problem:** unavailability when no alive node has a complete clean log (even a single fault, Fig. 4(b)).

**3.4.1 Removing the restriction:** allow the most up-to-date node to become leader even with faulty entries. Two questions: when can the faulty leader accept new commands (only after fixing its faulty entries — commands apply in order, no skipping), and is electing a faulty leader safe?

**3.4.2 Determining Commitment** (the key insight): fix the leader's log by distinguishing **uncommitted** (discard) from **possibly committed** (recover). The leader queries followers for `⟨epoch:e, index:i⟩`; if a **majority respond `dontHave`** → uncommitted → discard it **and all subsequent entries** (entries commit only in order). If any responds with the entry → committed → fix it. **Waiting:** if some nodes are down/slow and others respond `haveFaulty`, the leader must wait (discarding without waiting could lose committed data). If all copies of an entry are faulty → remain unavailable.

**3.4.3 Complete protocol — three follower responses:** `have` (correct entry), `dontHave`, `haveFaulty`. Leader actions:
- **Case 1:** ≥ one `have` → fix the entry.
- **Case 2:** majority `dontHave` → uncommitted → discard entry + all after.
- **Case 3:** `haveFaulty` → wait for Case 1 or Case 2.
Cases 1 and 2 may occur in any order; both safe. A leader that crashes mid-recovery is harmless (its partial repair only fixed committed or discarded uncommitted entries).

### 3.5 Ctrl Distributed Snapshot Recovery
All snapshot data is committed/applied → snapshots can **never** be discarded (unlike uncommitted log entries).

**3.5.1 Leader-Initiated Identical Snapshots:** current systems snapshot independently at different indexes → can't fetch a snapshot from another node, no chunk-based recovery. Ctrl makes all nodes snapshot at the **same index** via the log: leader inserts a **`snap` marker**; when committed and applied, every node snapshots at that index (reusing the existing fork-ed-child mechanism so new commands still apply). Identical snapshots because the marker lands at the same index everywhere. Once a majority have snapshotted at `i`, the leader inserts a **`gc` marker**; when applied, nodes garbage-collect the log up to `i`. (Doesn't insert the huge snapshot into the log — preserves common-case performance.)

**3.5.2 Recovering Snapshot Chunks:** local storage gives the distributed layer the snapshot index + faulty chunk. Three cases: (1) log not yet GC'd → recover snapshot locally from the log; (2) log GC'd → a majority must have the identical snapshot (gc marker requires majority) → fetch chunks; (3) leader doesn't know the follower's queried snapshot → supply the full advanced snapshot.

### 3.6 Summary
Clstore detects + disentangles + identifies, passing identifiers up. Distributed recovery decouples follower recovery (leader supplies correct data, piggybacked on replication) from leader recovery (fix snapshot locally if log present, else from majority; fix log via commitment determination). Figure 6 gives the full protocol (boxes A/B leader recovery, C/D normal operation).

---

## 4. Implementation
Ctrl in **LogCabin v1.0** (Raft) and **ZooKeeper v3.4.8** (ZAB), ~**1,500 LOC** each.
- **Local layer:** log = fixed-size preallocated files; identifiers in the header, ≥ a few MB physically apart from entries; batched appends → on a fault, discard the first entry without an identifier and all after, mark earlier faulty entries as corrupted. State machine = data tree → index-consistent identical snapshots on `snap` marker; **4K chunks**. Metainfo: LogCabin `currentTerm`/`votedFor`, ZooKeeper `acceptedEpoch`/`currentEpoch` — two checksummed copies. **CRC32** everywhere; EIO → zero-fill buffer → checksum mismatch; lost writes caught (preallocated zeros); misdirected writes usually mismatch, else caught by a monotonic-index sanity check.
- **Distributed:** LogCabin — `term`=epoch, modified **AppendEntries** RPC (followers report faulty entries/chunks, leader sends fixes); new RPC for leader recovery; leader steps down after a recovery timeout (e.g., partition); batched. ZooKeeper — `zxid` packs epoch+index; modified Phase 1 (followers report faults) and Phase 2 (leader sends fixes) and `newEpoch` (leader's faults); leader won't proceed to Phase 2 until fixed.

---

## 5. Evaluation
3-node cluster, 1-Gb network, 40-core Xeon, 128 GB RAM, SSD + HDD, ext4.

### 5.1 Correctness (fault-injection framework: corruptions, errors, crashes simulating lagging nodes)
- **Targeted log corruptions** (4 entries, all 4096 combos across 3 nodes; recovery possible in 2401, impossible in 1695): originals recover only **46/2401**; the other 2355 are unsafe (truncate) or unavailable (crash). **Ctrl correct in all 2401**, and correctly **unavailable** in all 1695 impossible cases.
- **Random block corruptions/errors** (5000 each): originals unsafe/unavailable ~30% (corruptions) / ~50% (errors via crash). **Ctrl correct in all.**
- **Crashed + lagging nodes** (5000, mixed epochs/uncommitted): **Ctrl recovers all**; originals unsafe/unavailable in many.
- **Model checking:** Python model — **2.5M+ log states**, all correct; tweaking key decisions (e.g., needing `⌊N/2⌋+1` `dontHave`) immediately surfaces violations. Added Ctrl's log recovery to Raft's **TLA+** spec — recovers correctly, original spec violates safety.
- **Snapshot recovery** (1000 over states l/t/g): **Ctrl all correct**; originals ~half wrong (LogCabin loads faulty snapshots / crashes; ZooKeeper crashes / truncates).
- **FS metadata faults** (1000): **Ctrl always safe** (reliably crashes), recovering 566/498; originals violate safety in 36/192 cases by not detecting the fault.

### 5.2 Performance
- **Write (worst case, 1K entries):** identifier writes induce a seek (HDD) amortized by batching → **8–10% overhead** at 32 clients on HDD, **≤ 4%** on SSD.
- **Read** (served from memory): **no overhead.**
- **Fast log recovery:** corrupt the first of 30K 1K-entries — original LogCabin truncates and re-transfers everything (**1.24 s, 32 MB**); **Ctrl fixes just the faulty entry (1.2 ms, 7 KB)**.

---

## 6. Par for Other Classes of Systems
- **Primary-Backup (Kafka):** primary≈leader, backups≈followers; `min.insync.replicas` ≈ commit threshold; in-sync-replica election ≈ leader completeness. Identifier = `⟨topic_id, message_id⟩`. Commitment determination: if `N − min.insync.replicas` nodes `dontHave` → uncommitted, discard; else fix from any correct response. (Redis would need bigger changes — no checksums.)
- **Dynamo-style Quorums (Cassandra):** no leader; ring; write quorum `W`, read quorum `R`. Identifier = `⟨primary key K, timestamp⟩`. Each faulty node queries others for `K`; waits for `N − W + 1` responses to determine commitment; fix from a correct `⟨data, timestamp⟩`.

---

## 7. Related Work
- **Storage-fault studies** (LSEs, SSD failures, cheap/near-line devices) motivate the analysis.
- **Authors' prior work** [Ganesan et al.] showed redundancy ≠ fault tolerance, but with a single-fault model — this paper's model (crashes + network + storage) exposes new safety/availability violations.
- **Targeted approaches** [Bolosky NSDI'11; Chandra "Paxos Made Live"] suffer unavailability; MarkNonVoting can lose promises (unsafe) — Ctrl stores two metainfo copies.
- **Generic approaches:** **PASC** (two full state copies, 2× space, crashes on fault), **XFT** (tolerates `⌊(N−1)/2⌋` total crash+non-crash), **UpRight** (bounded total faults). Ctrl differs by **focusing on storage faults**: fine-grained fault attribution (per item, not per node) → available as long as a majority is up and ≥ one clean copy of each item exists. Ctrl can augment generic approaches (e.g., PASC for memory + Ctrl for storage).

---

## 8. Conclusion
Recovering from storage faults in distributed systems is hard. **Par** exploits protocol-specific knowledge; **Ctrl** is the RSM instantiation, recovering from a range of storage faults with little overhead. A first step — primary-backup and Dynamo-style quorums remain to be hardened.

## Appendix A — Impossibility of Last-Entry Disentanglement
Formal model (log as entry-list `Le` + identifier-list `Lid`; `write`/`fsync`; disentangled sequences; crashes only between `write(idi)` and `fsync`). **Theorem:** for the **last** entry, a crash-state log `L1 = σcr_n` and a corruption-state log `L2 = σco_n` can be made **identical by construction**, so no deterministic algorithm can tell them apart. For a **non-last** entry (`i < n`), they're always distinguishable (the next entry fixes it).

---

## Key Takeaways
- **Local recovery is dangerous.** Truncate / DeleteRebuild act locally and interact unsafely with the distributed protocol → silent global data loss. Recovery must be **protocol-aware**.
- **Separate committed from uncommitted** (global-commitment determination): recovering committed items is required for safety; discarding uncommitted ones fast is required for availability. A **majority of `dontHave`** ⇒ uncommitted ⇒ discard.
- **Persist record + separated identifier** enable disentangling crash-vs-corruption and identifying faulty items — but the **last log entry is fundamentally ambiguous** (push the decision to distributed recovery).
- **Metainfo is node-unique** → must be recovered locally (two copies), never from peers.
- **Leader-initiated identical snapshots** (snap/gc markers via the log) enable chunk-based snapshot recovery without hurting the common case.
- Leverages **leader completeness** (leader has all committed data) and `⟨epoch, index⟩` identity throughout.
- Generalizes to primary-backup (Kafka: `N − min.insync.replicas` `dontHave`) and Dynamo quorums (Cassandra: wait for `N − W + 1`).
