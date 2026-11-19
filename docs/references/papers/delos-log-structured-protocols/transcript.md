# Log-structured Protocols in Delos

**Authors:** Mahesh Balakrishnan, Chen Shen, Ahmed Jafri, Suyog Mapara, David Geraghty, Jason
Flinn, Vidhya Venkat, Ivailo Nedelchev, Santosh Ghosh, Mihir Dharamshi, Jingming Liu, Filip
Gruszczynski, Jun Li, Rounak Tibrewal, Ali Zaveri, Rajeev Nagar, Ahmed Yossef, Francois Richard,
Yee Jiun Song (Facebook)

**Venue:** SOSP 2021 (28th ACM Symposium on Operating Systems Principles), October 2021.
**Source:** <https://doi.org/10.1145/3477132.3483544>. The ACM site refuses scripted downloads;
the PDF here is the authors' copy, <https://maheshba.bitbucket.io/papers/delos-sosp2021.pdf>.
Read from that PDF on 2026-11-19.

Earlier paper: [`../delos-virtual-consensus/`](../delos-virtual-consensus/) (OSDI 2020), the
layer under the log.

---

## Abstract (summary)

The OSDI 2020 paper layered consensus **under** the shared log. This paper layers the
replicated state machine **above** it. A **log-structured protocol** is a small replicated state
machine that runs over a shared log and can be stacked like a network protocol stack. Facebook
built two databases on Delos this way, DelosTable and Zelos (a ZooKeeper clone), with nine such
protocols. Layering costs little, and single engines gave up to 100X lower read latency
(leases) and 2X throughput (batching).

---

## 1-2. Why

Every replicated database re-implements the same generic logic over its log: playback, trim
coordination, batching and group commit, leases, session order, exactly-once, repair tools,
backup. Delos first put this logic in one monolithic platform state machine. When the ZooKeeper
team built Zelos on it, the two teams needed different behaviors, and the platform became a set
of flags. The fix was to split it into layers.

## 3. The engine API

Each engine implements `propose(entry)`, `sync()`, `registerUpcall(applicator)` and
`setTrimPrefix(pos)`, and receives `apply(txn, entry, pos)` and `postApply(entry, pos)` from the
engine below. Proposals flow down the stack to the shared log; entries flow up from the log.

- **BaseEngine** (bottom) plays the log forward. It keeps its playback cursor in the LocalStore
  (RocksDB) and calls `apply` from one apply thread inside one LocalStore transaction per entry.
  A `propose` appends and then plays the log until that entry and returns the result: a durable,
  failure-atomic, linearizable replicated RPC. A `sync` checks the tail, plays up to it, and
  returns a read-only snapshot.
- An engine adds its own **header** to an entry on the way down, and reads only its own header on
  the way up. An engine can filter, delay, batch or change entries, and can write its own
  control entries (for example from a timer).
- The LocalStore is a deterministic function of the log. Each engine keeps its own state in it.
- **Trim is the minimum of every engine's opinion.** An engine that cares about GC calls
  `setTrimPrefix` downward with the minimum of its own safe prefix and the one from above. The
  BaseEngine trims only what every engine allows.
- **Dynamic updates.** A new engine is first rolled out disabled (it can append but not change
  local state), then enabled by a command **in the log**, so every server switches at the same log
  position. Removal is the reverse.
- An entry is a **map** of headers, not a stack, so an engine survives entries written by an older
  stack.
- An exception in `apply` rolls back that engine's sub-transaction. Deterministic exceptions are
  results; non-deterministic ones (out of space) crash the server.

## 4. The nine engines

| Engine | Year | Job |
|---|---|---|
| BaseEngine | 2018 | SMR over the shared log |
| ViewTrackingEngine | 2018 | Tracks each server's durable playback position (piggybacked on proposals). Trims the log below the slowest live server. A server joins the view on its first append; a server silent too long is ejected by a log command |
| ObserverEngine | 2018 | Measures latency of each layer |
| BrainDoctorEngine | 2019 | Proposes raw LocalStore writes for emergency repair |
| LogBackupEngine | 2019 | Backs up the log for point-in-time restore. Nodes **bid in the log** for disjoint log segments to upload; a node that wins uploads and marks the bid done in the log. Trim waits until the backed-up prefix covers it |
| SessionOrderEngine | 2020 | ZooKeeper session order: sequence numbers per session; an entry out of order is dropped and re-proposed |
| BatchingEngine | 2020 | Batches proposals and applies a batch in one LocalStore transaction (group commit). 2X throughput |
| TimeEngine | 2021 | Timers that fire after a time elapsed on a fixed number of servers, robust to clock skew; used for time-based trim so non-voting followers can catch up |
| LeaseEngine | 2021 | Elects a designated proposer above the log by log commands; it serves strongly consistent reads with no round trip while its lease holds. 48 ms to 220 µs p99 reads on a cross-US cluster |

## 5-6. Production and evaluation

DelosTable ran on 107 clusters (over 3B transactions per day) and Zelos on 155 clusters (6.5B
writes and 30B reads per day), May 2021. The apply thread is rarely busy: reads skip it, and
write-heavy clusters are bound by the consensus fsync. Passive non-voting followers run a
smaller stack without the ViewTrackingEngine. The authors kept a linear stack and not a DAG.

---

## Segments, sealing, offload and ordering

This paper does not change segments or sealing: the shared log under the stack is the
VirtualLog of the OSDI 2020 paper, which keeps the chain of Loglets. Two engines touch the
questions of #1213 (segments experiment):

- **Offload coordinated through the log itself.** The LogBackupEngine divides the log into
  segments for upload, and the claim and the completion of each upload are log entries. The
  backed-up prefix is replicated state, so every server agrees on it. Trim waits for it.
- **Trim as the minimum of several constraints.** Playback of every live server
  (ViewTrackingEngine), backup (LogBackupEngine), snapshot backup, and time (TimeEngine) each hold
  back the trim point.

## What this means for the paros journal

- **Design B of #1213 has a Delos precedent.** In design B, replicas cut the chosen log into
  chunks and offload them locally, and the matchmakers know nothing. The LogBackupEngine shows a
  middle way: offload claims and completions as entries **in the journal's own log**. That gives
  every reader one decided view of what is offloaded (check 6 of #1213) without putting a segment
  table in the matchmakers. The #1213 report can name this as variant B' if design B fails
  check 6. This is my reading; the paper does not compare it with a metadata table.
- **Trim needs a minimum over named constraints.** The journal design's open problem 7 (cursors
  versus `Truncate`) and problem 8 (offload before trim) are the same question Delos answers with
  `setTrimPrefix`: each holder states a floor, and the log trims below the lowest. In paros
  `Truncate` is a client decision fenced by the writer uuid, so the order between a client
  `Truncate` and an offload floor is still **Open** (journal design, problem 7).
- **A rule change at one log position.** Delos enables an engine by a log command so all servers
  switch at the same position. paros already judges journal control commands at apply
  (`journal/state.rs`). A future change of an offload or trim policy can use the same pattern.
- **Leases above the log.** The LeaseEngine shows that 0-RTT reads need a designated proposer,
  not a leader inside consensus. paros has one leader per journal, so this is already the
  case; leaderless reads (`specs/tla/reads/`) are the other path.
