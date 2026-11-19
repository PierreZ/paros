# Virtual Consensus in Delos

**Authors:** Mahesh Balakrishnan, Jason Flinn, Chen Shen, Mihir Dharamshi, Ahmed Jafri, Xiao Shi,
Santosh Ghosh, Hazem Hassan, Aaryaman Sagar, Rhed Shi, Jingming Liu, Filip Gruszczynski, Xianan
Zhang, Huy Hoang, Ahmed Yossef, Francois Richard, Yee Jiun Song (Facebook)

**Venue:** OSDI 2020 (14th USENIX Symposium on Operating Systems Design and Implementation),
pages 617-632. Best paper.
**Source:** <https://www.usenix.org/conference/osdi20/presentation/balakrishnan>
(PDF: <https://www.usenix.org/system/files/osdi20-balakrishnan.pdf>). Read from the PDF on
2026-11-19.

Follow-up: [`../delos-log-structured-protocols/`](../delos-log-structured-protocols/) (SOSP 2021),
the layer above the log.

---

## Abstract (summary)

Delos is a replicated table store for the Facebook control plane. It virtualizes consensus by
virtualizing the shared log API. A **VirtualLog** gives applications one ordinary shared log
(`append`, `checkTail`, `readNext`, `prefixTrim`). Under it, the VirtualLog chains a list of
independent shared logs, the **Loglets**. Each Loglet is one segment of the address space. To
change the protocol, the leader, the membership or the hardware, Delos seals the last Loglet
and appends a new one to the chain. In production this let Delos start on a ZooKeeper-backed
Loglet and switch online to a custom one (NativeLoglet) for a 10X latency gain, without
downtime.

---

## 1-2. Why virtualize

A consensus protocol has a control plane (leader election, membership, reconfiguration) and a
data plane (ordering on the critical path). Raft and most protocols merge the two, so a new
ordering scheme means a new control plane. Delos splits them:

- the **VirtualLog** is the control plane: generic, fault tolerant, slow is acceptable because it
  runs only at reconfiguration;
- the **Loglet** is the data plane: fast and simple, and it does **not** need fault-tolerant
  consensus. It needs only a fault-tolerant **seal**.

## 3. Abstractions

### 3.1 The VirtualLog and the chain

The VirtualLog maps ranges of its address space to Loglets. The paper writes a chain as
`[0 →A 100 →B 150 →C ∞)`: positions 0-99 are Loglet A, 100-149 are Loglet B (its positions
0-49), and 150 on are Loglet C. Only the last Loglet takes appends (the **active segment**). All
other Loglets are sealed (**sealed segments**). `append` and `checkTail` go to the last Loglet;
`readNext` goes to the Loglet that holds the range.

Three reconfiguration calls change the chain:

- `reconfigExtend`: seal the active segment and add a new Loglet at the end;
- `reconfigModify`: change a **sealed** segment, for example replace a failed server or copy the
  segment to another Loglet and remap it;
- `reconfigTruncate`: remove the first sealed segment after a trim.

### 3.2 The reconfiguration protocol

The VirtualLog is a client library plus a **MetaStore** that stores the chain. Each client
caches the chain, possibly stale. Any client can start a reconfiguration or finish one that
another client started:

1. **Seal** the active Loglet (idempotent; many clients can seal at the same time). Then call
   `checkTail` on it. The tail is the start of the new segment.
2. **Install** the new chain in the MetaStore with a conditional write on the version. If many
   clients race, at most one wins.
3. **Fetch** the chain again if the write lost.

A client that appends on a stale chain gets a "sealed" error, refetches the chain and retries.
A `checkTail` also returns the seal bit, so a client knows when its chain is stale.

**Failure atomicity.** If a client sealed but did not install a new chain (it crashed), another
client waits for a timeout and **rolls forward**: it installs a default chain that clones the
configuration of the old active segment, because it does not know what the first client wanted.

**Only a conflicting chain needs a seal.** A seal is needed only when the new chain maps an
unwritten address to another Loglet. `reconfigModify` (rebuild or move a sealed segment) and
`reconfigTruncate` (drop the first segment) do not move the tail, so they need no seal. Clients
can work in the old and the new chain at the same time. Rebuild and garbage collection of
sealed segments do not stop appends at the tail.

**Policy.** Three things trigger a reconfiguration: an operator (a planned change), the
VirtualLog itself (`reconfigTruncate` after a `prefixTrim` covers a whole sealed Loglet), and the
Loglet (it detects a failure and asks for `reconfigExtend` with a configuration that leaves out
the failed server).

### 3.3 The MetaStore

The MetaStore is **one versioned register with a conditional write**. It is the only part that
needs fault-tolerant consensus, and it does not need to be fast. The paper explains why the next
configuration is not written inside the old Loglet (as Raft or SMART do): that needs the old
Loglet to be available for writes, so it needs consensus inside the Loglet. A separate MetaStore
removes this need. The cost is one seal, one `checkTail` and one MetaStore write per
reconfiguration, "10s of ms" in production. For planned changes across regions, the authors say
they may later write the reconfiguration inline in the Loglet (α-windows).

### 3.4 The Loglet

A Loglet is a totally ordered, durable shared log with a static configuration. It needs no
leader election and no membership change. It must give:

- a highly available `seal`: after a successful seal, no new append is acknowledged;
- a `checkTail` that returns `(tail, sealed)`. The two parts need not be read atomically.

A seal bit is weaker than consensus (only one value can be set), so an atomic register is
enough. After a seal, appends that were in flight can still become durable ("zombie appends"),
and `checkTail` can return a larger tail later. This is safe because the chain fixes the segment
boundary: entries past the boundary are not part of the VirtualLog.

## 4. Delos implementation

Delos servers keep a RocksDB copy of the state and play the VirtualLog (a Tango-style shared log
database). A read-only transaction calls `checkTail`, plays the log to that position, and then
reads locally. The MetaStore first lived in an external ZooKeeper. It is now embedded: Lamport's
construction from the original Paxos paper, **a sequence of independent single-slot Paxos
instances**, each instance holding the membership of the next one, with no pipelining and no
leader optimization (two round trips for each read and write).

Loglets in production: **ZKLoglet** (a shim over ZooKeeper), **LogDeviceLoglet**,
**BackupLoglet** (over an HDFS-like cold store), and **NativeLoglet**.

### 4.1 NativeLoglet: no consensus, only a seal

One sequencer plus one LogServer on each Delos server. Each LogServer has a local log and a seal
bit; a sealed LogServer refuses appends.

- **append**: the sequencer gives the position and sends the entry to all LogServers. The append
  is globally committed at a majority of unsealed LogServers (with every earlier position
  committed). If a majority says "sealed", the append fails with "sealed". Retries are
  idempotent (same entry, same position).
- **seal**: any client sets the seal bit on a majority.
- **checkTail**: ask all LogServers and wait for a majority. If all answers are sealed, the
  client **repairs** the answering LogServers up to the highest tail it saw and returns it. This
  is safe because one sequencer never puts two entries at one position. If some answers are
  sealed, the client seals again and retries. If none is sealed, it waits for its known tail to
  reach the maximum.
- **readNext**: a read needs no quorum. The position is already known to be committed, so any
  copy is enough.

If the sequencer fails, the NativeLoglet is unavailable for appends. Delos then seals it and
installs a new NativeLoglet with another sequencer. Leader election is a VirtualLog
reconfiguration. The NativeLoglet took under four months to build and ship.

### 4.2 StripedLoglet

A thin layer (about 300 lines) that stripes one address space round robin over several Loglets.
An append is acknowledged only when all earlier logical positions are filled. With the same
LogServers and two sequencers it rotates the sequencer; with disjoint Loglets it shards the
acceptors (over 1M appends/s in a log-only test). The whole StripedLoglet must be sealed even if
only one stripe changes.

## 5-6. Evaluation and lessons

- The online switch from ZKLoglet to NativeLoglet cut p99 latency by 10X with no downtime. The
  `reconfigTruncate` that removed the old ZKLoglet a few minutes later caused no disruption,
  because it needs no seal.
- Reconfigurations take tens of milliseconds. A production cluster saw 98 in one month, most from
  software deployments and machine preemptions, few from real failures.
- Virtualization adds 100-150 µs to p99 `append` and `checkTail`.
- Delos can switch between a converged Loglet (on the database servers) and a disaggregated one
  (on other machines) while it runs. Operators used this to decouple the log from a fragile
  database tier, or to run on ZKLoglet while a NativeLoglet bug was fixed.
- Because the chain is metadata, an entry can be removed or remapped by a metadata change. Delos
  used this to skip a "poison" entry that hung every learner.
- **Limits.** A planned leader change goes through the VirtualLog, so it costs a seal and a
  MetaStore write. Protocols with partial orders (EPaxos style) do not fit the API.

---

## Segments, sealing, offload and ordering

| Question | Delos |
|---|---|
| What a segment is | One Loglet: a whole shared log with its own static configuration, mapped to one range of the VirtualLog |
| Where segments are declared | The chain in the MetaStore: a list of segments with start and stop positions and an opaque Loglet configuration each |
| Who seals | Any client of the VirtualLog; the Loglet asks for it when it detects a failure |
| How the end is fixed | Seal, then `checkTail` on the sealed Loglet; the tail becomes the start of the next segment in the chain written by conditional write. Zombie appends past it do not count |
| Ordering inside a segment | The Loglet's own (NativeLoglet: one sequencer, majority of LogServers) |
| Ordering across segments | The chain order |
| Leader change | A reconfiguration: seal and a new segment |
| Offload | BackupLoglet: a sealed segment is copied to cold storage and the chain is remapped (`reconfigModify`), with no seal of the tail |
| Trim | `prefixTrim`, then `reconfigTruncate` drops the first segment, with no seal |
| Readers | Cache the chain; refetch it on a "sealed" error |

## What this means for the paros journal

The paros journal design (journal design doc, "Prior art" section; #1213 (segments
experiment)) runs **one** Matchmaker Paxos log across segments (decision 1.A), with segments
probably declared in the matchmakers and one leader per journal. Delos is the closest prior art
for design A of #1213, with these points:

- **The chain is design A's segment table.** Delos keeps start and stop positions and a
  configuration per segment in one register, written by a conditional write. The paros lean
  ("write-once end slot by CAS in the matchmaker record") has the same shape. Delos shows it works
  at small scale with a register that is slow and simple.
- **Delos's consensus is outside the segment; ours is inside.** A Loglet needs only a seal, and
  every leader change is a new segment. paros decided the reverse: Paxos orders the data, and a
  leader change opens no segment (plan rule 1, Pierre 2026-11-19). So paros does not get the
  "Loglet needs no consensus" benefit, and it does not pay a seal per leader change. The
  NativeLoglet `checkTail` repair (copy up to the highest tail on a sealed majority) is close to
  what a new paros leader does in Phase 1 with gap fill.
- **Changes to sealed segments need no seal.** `reconfigModify` and `reconfigTruncate` do not
  move the tail. For #1213 design A this means: offload, re-replication of a sealed segment and
  trim should be record updates that do not stop writes. A model of design A can check that an
  offload or a trim never blocks a write at the tail.
- **The end is fixed by the record, not by the Loglet.** Zombie appends after a seal are
  harmless because the chain fixes the boundary. This supports a write-once end slot in the
  matchmaker record: a vote that lands in a sealed configuration after the end slot is written
  is outside the journal.
- **Roll forward of a half-done seal.** A client that finds a sealed tail and no new chain
  installs a default one. The #1213 model should have the same race: two leaders, one stops after
  the seal, the other finishes.
- **Cold data stays declared.** BackupLoglet is a segment of the chain like any other. This
  supports check 6 of #1213 (a range whose only copy is in the object store is still declared).
- **Readers refetch on "sealed".** The same path applies to paros readers with a cached segment
  view (check 5 of #1213).
- **The MetaStore is a chain of single-decree Paxos instances**, each naming the next
  membership. This is close to the paros matchmaker set handover (`specs/tla/handover/`, #1097),
  which uses a one-slot `Decree` per generation.

Correction to earlier notes: the design doc and #1213 said the MetaStore is "a versioned
key-value store on Multi-Paxos". The paper says it is one versioned register, first on
ZooKeeper, then on a sequence of independent single-slot Paxos instances.
