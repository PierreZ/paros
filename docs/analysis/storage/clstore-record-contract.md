# The durable-record contract: CLStore-shaped detection, TigerBeetle-hardened

Stage 7 (issue #20) design note. This is the **contract** the corruption-detection layer
assumes about every durable record, the spec the future production storage engine must
implement, and the semantics the simulation's `StorageWorld` models — as first-class *read
outcomes* at the `NodeStorage` seam, never as serialized bytes.

Sources: CTRL §3.3/§4.1 (the CLStore design from *Protocol-Aware Recovery for
Consensus-Based Storage*, FAST '18), TigerBeetle's journal recovery + checksum machinery,
and the #70/#71 review decisions (fixed constraints).

## Goals (the Stage-7 invariants)

1. **Zero silent bad reads.** No bytes that fail their integrity check ever reach
   `paros-core` or the driver's protocol logic. Detection is total regardless of what the
   fault hit — the detection *pressure* is protocol-blind (#70: moonpool's provider-level
   faults and the world's per-record faults both feed the same detector).
2. **Never truncate on a mismatch.** CTRL Figure 2 (found in both ZooKeeper and LogCabin):
   a node detects corruption in entry 1, truncates entries 1–3, then wins an election with
   lagging peers and silently erases committed data cluster-wide. Stage 7's baseline is
   *crash*, never truncate; the truncate-on-mismatch red demo proves the sim catches this
   bug class.
3. **Crash and corruption are disentangled.** A torn write at the tail (crash mid-update,
   never acked — safe to discard locally) is distinguished from a mismatch on a
   previously-persisted record (possibly chosen — must NOT be discarded).
4. **Detection is classified, not just detected.** The verdict (`CrashTail` / `Corrupted`
   / `Undecidable`) is a typed value on the storage error surface
   (`StorageError::Corruption`), which Stage 8's crash-relevance logic consumes.

The baseline reaction — **detect ⇒ crash** — is deliberately an availability disaster
(unmodified LogCabin/ZooKeeper: correct in only 46 of 2,401 recoverable
targeted-corruption cases; ~50% unavailable under block errors). Stage 8 (#21) buys the
availability back *without* ever paying in safety.

## The record format contract

- **Every persisted record is checksummed:** each accepted entry, the snapshot, the
  `HardState` scalars (promise + chosen index + truncation floor), and the sealed-sessions
  ledger.
- **Each log entry has an identifier physically separate from the entry:**
  `⟨slot, accepted_ballot, offset, cksum⟩` (CTRL: 32 bytes, atomically writable, itself
  checksummed). Separation is the point — a misdirected write that clobbers the entry
  cannot also clobber its identifier. The identifier doubles as the entry's **persist
  record** (the disentanglement witness) and carries `offset` so one corrupt entry never
  ends the ability to parse subsequent entries.
- **Identity lives inside the checksummed region and is re-derived on every read**
  (TigerBeetle `header_ok`): a record read back with a valid checksum but the wrong
  slot/cluster is a *misdirected* read/write, its own detected outcome. Validate the
  checksum before touching any other field.
- **Absence must be detectable — TigerBeetle-style reserved records (decided in #20).**
  A lost write is never indistinguishable from a never-written slot. CTRL achieves this by
  zero-preallocating (zeros ⇒ checksum mismatch); we adopt TigerBeetle's stronger
  discipline instead: every slot is formatted with a real, checksummed `Empty`/reserved
  record carrying its own slot identity, so all-zeros is *always* invalid ⇒ faulty, never
  "empty". This also removes the "stale-but-valid bytes from a previous file incarnation"
  hazard that zero-preallocation leaves open, and makes free-vs-faulty a property of the
  *record*, not of a byte pattern.
- **Sanity backstop for block-aligned misdirects:** slot indices in the log must be in
  order and monotonically increasing — **on `slot` only, never on `accepted_ballot`**.
  Ballots are legitimately non-monotonic across slots in Multi-Paxos: a new leader
  re-proposes recovered slots at its ballot while neighbours keep old ballots. This is the
  one place CTRL's Raft-shaped rule must be restated.
- **`HardState` keeps two local checksummed copies** (CTRL metainfo doctrine — tens of
  bytes, updated rarely). One copy bad ⇒ use the other and repair it; both bad ⇒ crash:
  the node cannot know what it promised, and no peer can tell it (that is Stage 8's safety
  argument, pre-stated here). TigerBeetle's 4-copy superblock with
  `write_quorum + read_quorum = copies + 1` and a sequence/parent chain is the
  production-grade version — noted for the eventual storage engine, not built in the sim.

## The update protocol and the disentanglement witness

Per accepted entry: `write(e_i); write(id_i); fsync()` — two writes, **one** fsync. The
identifier `id_i` is the witness: it can only be durable if it was written after `e_i` in
the same batch, so its presence proves the batch reached its fsync (or the fsync's
completion is in doubt only for the *last* entry — the proven-undecidable case).

The classification rule (CTRL §3.3.3), for a mismatched entry `e_i`:

| Local evidence                                   | Verdict          | Action (Stage 7) |
|--------------------------------------------------|------------------|------------------|
| `id_i` absent, nothing durable past it           | crash before `id_i` hit disk | discard locally — safe: never acked to anyone |
| `id_i` present ∧ durable evidence past `e_i`     | corruption       | crash (Stage 8: recover) |
| `id_i` present ∧ `e_i` is the last entry         | **undecidable** — proven fundamental (CTRL Thm A.1) | treat as corruption: crash (Stage 8: distributed commitment determination) |
| `id_i` AND `e_i` both faulty                     | corruption, record unidentifiable | crash |

**Batching rule** (paros batches `WriteOp`s): the first faulty entry *without* an
identifier and everything after it is crash-truncatable; faulty entries *before* that
point are corruption. Snapshot and `HardState` are exempt from entanglement entirely
(atomic-rename discipline: a partial write is discarded, so a mismatch there is always
corruption).

**TigerBeetle hardening on the tail rule:**

- truncate-as-crash only inside the window past a *provably certain* head;
- cap the number of truncatable slots by the maximum concurrently in-flight accept
  writes (`paros::classify_log`'s `max_inflight`);
- abandon truncation entirely (⇒ crash) on the first fault that breaks head-certainty or
  on any witnessed/valid record *inside* the window (it might be a misdirected read).

The classifier is a **total, named, exhaustively-tested decision function**:
`paros::corruption::decide` over the evidence cube (2 entry states × 3 identifier states ×
2 successor states — the unit test enumerates all 12 points), each `RecoveryCase` mapping
to its `CorruptionVerdict`, with the case label on the tracing event. `classify_log`
applies the batching rule + hardening over a whole retained log.

## EIO collapses into the corruption channel

CTRL §4.1: an unreadable record (`EIO` on a *read*) is treated exactly as a checksum
mismatch — "zero-fill then mismatch" semantics (`IntegrityFault::ReadError`). One
detection path, one classification path. Write-side `EIO`/fsync faults keep Stage 6's
crash semantics (`StorageError::Io` / `FsyncFailed` with the `WriteOutcome` ambiguity).

## Boot-time scan

On recovery, `NodeStorage::boot_scan` — called by the driver **before**
`ColocatedNode::new` reads anything — scans the durable records and produces the
`faulty_entries` / `faulty_snapshot` / `faulty_metainfo` sets. In Stage 7 any non-empty
set ⇒ the classified `StorageError` ⇒ the driver's typed crash decision. The scan itself
may only:

- discard a **crash-truncatable tail** (never acked to anyone), and
- repair a single bad `HardState` copy from its verified twin.

The write-side flush ordering and the boot read-back are asserted as a paired property
(the assertion doctrine's two-code-paths rule): the sim world asserts the flush never
leaves a floor past the chosen index, and `restore` re-asserts the same bound on what it
reads back.

## User data vs FS metadata

The FS-metadata family is modeled at *file* granularity (`StorageError::Metadata`): record
store missing/unopenable, wrong size (checkable: fixed-size preallocation + separately
stored snapshot size), read-only store. Verdict: **reliably crash** — never attempt
recovery on metadata, in Stage 8 either. The oracle judging these is asymmetric:
unavailable = pass, unsafe = fail.

## What the simulation models

The `StorageWorld` stores **semantic records, not bytes** (#20 fixed decision): every
corruption-family member is a first-class read outcome at the `NodeStorage` seam —

| Injected fault                                        | Surfaces as |
|-------------------------------------------------------|-------------|
| bit-flip / latent sector error / torn write on a persisted record | checksum-mismatch outcome |
| lost write                                            | absence-where-identifier-exists (the reserved-record contract) |
| misdirected write                                     | wrong-but-valid record: checksum passes, identity check catches it |
| EIO on read                                           | zero-fill ⇒ mismatch (same channel) |
| corruption at the tail after a seam crash             | the `CrashTail` leg (torn un-synced batch) |
| FS-metadata fault                                     | `StorageError::Metadata`, reliably crash |

All seeded, riding the Stage-6 per-record budget plus a dead-node budget (a
detected-persistent-corruption node stays crashed — detect ⇒ crash — so injections are
capped to leave a live quorum). A single block fault can hit a contiguous run of entries
(CTRL injects per FS block); snapshot corruption is its own kind and its own gate (#71).
Every record is targetable in one call (`StorageWorld::corrupt`) so #21's adversarial
promise-corruption test is a single injection.
