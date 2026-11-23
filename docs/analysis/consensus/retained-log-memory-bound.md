# A bound on the memory of the retained log (#1153)

Status: **proposal**. This note is the design that #1153 (a bound on the memory of the
retained log) asks for before code. Pierre decides the open points. No code of the bound is
in the tree yet.

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

## Proposal

1. **The measure.** The retained bytes in memory: the sum of the record sizes that the
   acceptor holds in memory (the size of the command bytes, the same measure as `max_bytes` of
   a read). The acceptor keeps the sum as it writes and drops records, so the measure costs
   nothing per step.
2. **A window in memory, the rest on disk.** The acceptor keeps in memory every record at or
   above `max(first_slot, applied - window)` and every record that is not yet chosen. A record
   below the window is evicted from memory. It stays on the disk, where the driver already
   wrote it. The soft limit is a number of bytes in `Limits` (`retained_bytes`), as
   `TLOG_SPILL_THRESHOLD` is for FoundationDB.
3. **A read of an evicted record is a request to the driver.** The core does no I/O in a step.
   A cold reader that needs an evicted range puts a `ReadRecords { from, limit, max_bytes }` in
   its `Ready`. The driver reads its storage and steps the records back in. The reader then
   goes on, as a paged promise or a paged catch-up goes on today. The read has the same bounds
   as a FoundationDB peek: one page per request, in the existing page sizes of `Limits`.
4. **At the hard limit, refuse.** When the measure is at or above a hard limit
   (`retained_hard_bytes` in `Limits`, larger than the soft limit), the records that cannot be
   evicted are the ones not yet chosen. Then `propose` answers with a refusal (a new
   `Outcome`, such as `Busy`) and opens no round. A client retries later. This is the paros
   form of the commit wait of `tLogCommit` at `TLOG_HARD_LIMIT_BYTES`: the core cannot wait, so
   it refuses as data, and the driver or the client owns the wait.
5. **The plain arm is unchanged.** With no limit set (the default), no record is evicted and
   no request is sent: a deployment that sets no limit behaves as today. The digests of
   `consensus-sim/tests/differential.txt` hold for it.

## How it is checked

- A TLA+ model before the code (root `AGENTS.md`, "No code before its model"): the window,
  the eviction, the read request and its answer, a crash between the two, and the refusal at
  the hard limit, with a control that evicts a record that is not yet chosen and a control
  that answers a read with a record of another ballot.
- consensus-sim: a flavor with a small window and small pages, so the cold readers run on
  evicted ranges; the oracles of the ledger stay as they are; a probe on each new branch (an
  eviction, a read request, a refusal at the hard limit).
- The differential test: every flavor with no limit keeps its digest.

## Open points

- **Open: a sync read or a request.** Point 3 adds a request to the driver. The other choice
  is a synchronous read of the `Storage` port inside a step: simpler, but a step then does I/O,
  which the sans-IO contract forbids today. Pierre decides; the TLA+ model of the step above
  checks the chosen form.
- **Open: refuse or hold.** Point 4 refuses at the hard limit. FoundationDB holds the commit.
  A hold in paros means a queue of proposals in memory, which the limit is there to stop. The
  recommendation is the refusal; Pierre decides.
- **Open: the numbers.** FoundationDB uses 1.5 GB and 3 GB per TLog. paros sets no number in
  the core: the soft and the hard limits come from the host, as the other `Limits` do, and the
  default is no limit (point 5).
- **Open: the journal v1.** The journal roles of `paros-journal` keep their own records
  (`docs/journal/04-roles.md`). Whether they take the same window, or a segment replica's
  offload covers it (`specs/tla/offload/`), is a question for #1323 (build the paros journal
  v1).
