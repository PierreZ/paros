# A bound on the memory of the retained log (#1153)

Status: **the measure is built**. This note is the design that #1153 (a bound on the memory
of the retained log) asked for before code. Pierre asked for the simplest bound (2026-11-23):
a measure in the core and a refusal in the driver. The measure is in the tree
(`ColocatedNode::retained_bytes`, `ReplicaNode::retained_bytes`); the window below is not.

## Where paros is after #1101

#1101 (one Belief role, one copy of the retained log) removed the two extra copies:

- A `Value` shares its bytes (`Arc<[u8]>`). A clone of a command costs a reference count.
- The replica keeps no chosen map. It holds the set of chosen slots and reads each chosen
  command through the port `replica::ChosenLog`, from the acceptor's records on a
  `ColocatedNode` and from the learned records on a `ReplicaNode`.

So a node holds each retained command once in memory. The memory still grows with the log:
every record from the compaction floor (`Acceptor::first_slot`) to the highest accepted slot
stays in memory until a `Truncate` raises the floor. Nothing bounds it.

## What FoundationDB does

Source: `apple/foundationdb`, branch `release-7.3`, files `fdbclient/ServerKnobs.cpp` and
`fdbserver/TLogServer.actor.cpp`. The line numbers are of that branch.

- **The measure.** A TLog counts `bytesInput - bytesDurable`: the bytes of the commits it
  holds in memory and did not yet move to its persistent B-tree (or pop). This is the only
  measure that the two limits below use.
- **Spill above a soft limit.** `TLOG_SPILL_THRESHOLD` is `1500e6` bytes
  (`ServerKnobs.cpp:900`). In `updateStorage`, when the measure is at or above the threshold
  (`targetVolatileBytes`, `TLogServer.actor.cpp:396` and `:1414`, `:1468`), the TLog moves the
  oldest versions to the B-tree, in batches of at most
  `REFERENCE_SPILL_UPDATE_STORAGE_BYTE_LIMIT` = `20e6` bytes (`ServerKnobs.cpp:901`), until the
  measure is below the threshold again. Below it, the loop sleeps
  `TLOG_STORAGE_MIN_UPDATE_INTERVAL` = 0.5 s (`ServerKnobs.cpp:55`).
- **Spill by reference.** The commit bytes are already in the TLog's disk queue. A spill by
  reference writes only a pointer into the disk queue to the B-tree, not the bytes
  (`shouldSpillByValue`, `TLogServer.actor.cpp:1991`).
- **Read of spilled data.** A peek below `persistentDataDurableVersion` reads the B-tree
  (`TLogServer.actor.cpp:1979`), and then the disk queue for each reference. The read is
  bounded: at most `TLOG_SPILL_REFERENCE_MAX_BATCHES_PER_PEEK` = 100 batches of at most
  `TLOG_SPILL_REFERENCE_MAX_BYTES_PER_BATCH` = 16 KiB per peek, and a memory limiter of
  `TLOG_SPILL_REFERENCE_MAX_PEEK_MEMORY_BYTES` = `2e9` bytes over all peeks
  (`ServerKnobs.cpp:85-87`). A peek by value is cut at `DESIRED_TOTAL_BYTES` = 150 000 bytes
  (`ServerKnobs.cpp:57`).
- **Stop above a hard limit.** `TLOG_HARD_LIMIT_BYTES` is `3000e6` bytes
  (`ServerKnobs.cpp:902`). While the measure is at or above it, `tLogCommit` does not refuse
  the commit: it waits in a loop of `delayJittered(.005)` and logs `TLogUpdateLag` once per
  second (`TLogServer.actor.cpp:2397`, and `:2974` for the recovery path). The commit is
  slowed, never failed.
- **Throttle before the hard limit.** Ratekeeper slows the whole cluster before a TLog
  reaches the hard limit: its target is `TARGET_BYTES_PER_TLOG` = `2400e6` bytes with a spring
  of `SPRING_BYTES_TLOG` = `400e6` bytes (`ServerKnobs.cpp:896-897`). Recovery uses
  `TLOG_RECOVER_MEMORY_LIMIT` = target + spring (`ServerKnobs.cpp:903`).

So FoundationDB has three steps: spill old data (free, because it is already on disk), then
throttle the clients, then block the commit.

## What is different in paros

- **Every record is already on disk.** The driver contract is persist before send: an
  acceptor's record is durable (`WriteOp::Accepted`, `WriteOp::Learned`) before any message
  that depends on it leaves. So a spill in paros writes nothing: it is only the eviction of the
  in-memory copy. This is the same as a FoundationDB spill by reference, with no B-tree write.
- **The core does no I/O.** `Storage` is a read-only port that a deployment reads at boot
  (`contract/storage.rs`). Today no step of the core reads storage. A read of an evicted record
  is new I/O inside a step, or a new request to the driver.
- **No clock and no sleep.** A sans-IO step cannot wait like `tLogCommit`. The core can only
  answer: accept the input, or refuse it with a result (goal 8: a refusal is data).

## Who reads a retained record

Each reader decides what the window must hold:

| Reader | Where | What it reads | Hot or cold |
|---|---|---|---|
| The apply walk | `Replica::advance`, `node/learn.rs` | each chosen slot above the fold | hot: the newest slots |
| A `Commit` and a vote | `node/phase2.rs`, `node/acceptor.rs` | the slot of the round | hot |
| A promise page | `Acceptor::promise_page`, `node/election.rs` | every record above the asked slot | cold after a leader change |
| Catch-up | `node/catch_up.rs` | a page of chosen records from the asked slot | cold for a lagging peer |
| A handoff | `node/handoff.rs` | the pending records of the old leader | hot |
| A journal read | `Replica::read` through `ChosenLog` | the records of the read range | cold for an old position |
| A compaction | `Replica::compaction_target`, `assert_compacted` | the first retained records | cold |

The hot readers read the newest slots. The cold readers read a page from a slot and go
forward, as a FoundationDB peek does.

## Proposal: a measure, and the driver refuses

The simplest bound changes no protocol rule. The core counts, and the driver decides.

1. **The measure.** The retained bytes: the sum of the weights of the records that a node's
   acceptor (or a `ReplicaNode`) holds, from the compaction floor up. A value says its weight
   through the trait `types::Weigh`: a client write weighs its records' bytes (the same
   measure as `max_bytes` of a read), a control command weighs nothing. The acceptor keeps the
   sum as it writes and drops records, so a read costs nothing per step.
   `ColocatedNode::retained_bytes` and `ReplicaNode::retained_bytes` report it.
2. **The driver refuses above its limit.** The driver reads the counter and turns a client
   away before it calls `propose` when the measure is above a limit that the host sets. The
   refusal is the driver's answer to its client, as FoundationDB's Ratekeeper throttles the
   clients before a TLog reaches its hard limit. The core gets no new message, no new
   `Outcome` and no new step, so this needs no TLA+ model.
3. **The client brings the measure down.** A `Truncate` raises the compaction floor, as today.
   In the journal v1 the seal and the offload of a segment, and the acceptors' compaction at
   `F + 1` replicas, bound the retained log too.
4. **The plain arm is unchanged.** A driver that reads no counter behaves as today. The
   digests of `consensus-sim/tests/differential.txt` hold.

How it is checked: unit tests pair the counter with the records (it rises on each record,
swaps the bytes of a record it replaces, falls on a compaction and a trim-point jump, and
reads back the same after a reboot, on a node and on a replica node), and paired assertions
in the core hold it in every walk of consensus-sim (no record, no byte; the sum bounds each
end record's weight; a drop never takes more than the sum). A consensus-sim oracle would
compare two outputs of the core, which no planted mutant of the driver can make differ, so
the core's own tests and its mutants judge it.

## Later, if a deployment needs it: a window in memory

If a deployment cannot ask its clients to truncate, the core can keep only a window in memory.
This is the shape of FoundationDB's spill. It is not proposed now.

- **A window.** The acceptor keeps in memory every record at or above
  `max(first_slot, applied - window)` and every record that is not yet chosen. A record below
  the window is evicted from memory. It stays on the disk, where the driver already wrote it.
- **A read of an evicted record is a request to the driver.** A cold reader that needs an
  evicted range puts a `ReadRecords { from, limit, max_bytes }` in its `Ready`. The driver
  reads its storage and steps the records back in, one page per request, in the page sizes of
  `Limits`.
- **A refusal in the core at a hard limit.** `propose` answers a new `Outcome` (such as
  `Busy`) and opens no round.
- **Checks.** A TLA+ model first (the window, the eviction, the read request and its answer, a
  crash between the two, the refusal), with a control that evicts a record that is not yet
  chosen and a control that answers a read with a record of another ballot; a consensus-sim
  flavor with a small window, so the cold readers run on evicted ranges.

## Open points

- **Open: the numbers.** paros sets no limit in the core: the driver's limit comes from the
  host. FoundationDB uses 1.5 GB (spill) and 3 GB (hard limit) per TLog.
- **Open: the window.** Built only when a deployment shows that a `Truncate` cannot bound its
  memory. Then its two choices are Pierre's: a request to the driver or a synchronous read of
  `Storage` inside a step (which the sans-IO contract forbids today), and a refusal or a hold
  at the hard limit.
- **Open: the journal v1.** The journal roles of `paros-journal` keep their own records
  (`docs/journal/04-roles.md`). Whether they report the same measure is a question for #1323
  (build the paros journal v1).
