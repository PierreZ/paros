# The durable-record contract (Stage 7 — corruption detection)

The CLStore-equivalent design paros' corruption detection assumes, adapted from
CTRL (§3.3/§4.1) with TigerBeetle's refinements where they are strictly better.
This is the **contract** the simulation's semantic read outcomes encode (the
world stores records, not bytes — #70/#71) and the **spec** for the eventual
production storage engine. The classifier over this contract's read-back
evidence lives in `paros::corruption`; the trait-level summary is on
`paros::NodeStorage`.

Goal (issue #20): make all silent corruption manifest as a detected,
*classified* mismatch. Detection only — recovery is Stage 8 (#21). The Stage-7
baseline reaction is **detect ⇒ crash**, deliberately paying availability
(CTRL is blunt: unmodified LogCabin/ZooKeeper were correct in 46 of 2,401
recoverable targeted-corruption cases) so that Stage 8 can buy availability
back without ever paying in safety.

## Record classes

Every persisted record is checksummed, and identity lives **inside** the
checksummed region, re-derived on every read (TigerBeetle `header_ok`):
validate the checksum before touching any other field; a valid-checksum record
answering for the wrong slot/cluster is a *misdirected* read/write, its own
detected outcome.

| record | contents | notes |
|---|---|---|
| log entry `e_i` | the accepted `(ballot, command)` for slot `i` | opaque client bytes stay opaque |
| entry identifier `id_i` | `⟨slot, accepted_ballot, offset, cksum⟩` | 32 bytes, atomically writable, itself checksummed, physically separate from `e_i` |
| metainfo (`HardState`) | promise + chosen index + truncation floor + config id | **two** local checksummed copies |
| snapshot | the opaque application snapshot + boundary | first-class corruption target with its own checksum (#71) |
| sealed-sessions ledger | the at-most-once records truncation seals (#94) | rides the truncation record |

**Separation is the point** of the identifier: a misdirected write that
clobbers the entry cannot also clobber its identifier, and `offset` means one
corrupt entry never ends the ability to parse subsequent entries.

## Absence is detectable (reserved records — decided)

The contract is that a lost write is never indistinguishable from a
never-written slot. CTRL zero-preallocates (zeros ⇒ checksum mismatch); we
adopt TigerBeetle's stronger discipline instead: every slot is formatted with
a real, checksummed `Empty`/reserved record carrying its own slot identity, so
all-zeros is *always* invalid ⇒ faulty, never "empty". This also removes the
stale-but-valid-bytes-from-a-previous-file-incarnation hazard that
zero-preallocation leaves open, and makes free-vs-faulty a property of the
*record*, not of a byte pattern.

## The update protocol and the persist witness

Per entry: `write(e_i); write(id_i); fsync()` — two writes, **one** fsync
(CTRL §3.3.3). Because the identifier is written second and the
acknowledgement is predicated on the fsync, `id_i` doubles as the entry's
**persist record**: an absent (still-reserved) `id_i` proves the fsync never
completed for this update, so `e_i` was never acknowledged to anyone and
discarding it locally is safe. That witness is the entire basis of
crash-vs-corruption disentanglement:

| local evidence for mismatched `e_i` | verdict | Stage-7 action |
|---|---|---|
| `id_i` absent | crash before `id_i` hit disk | discard locally (`CrashTail`) |
| `id_i` present ∧ (`e_{i+1}` or `id_{i+1}` present) | corruption | crash (`Corrupted`; Stage 8: recover) |
| `id_i` present ∧ `e_i` is the last entry | **undecidable** — proven fundamental (CTRL Thm A.1), not an engineering gap | crash (`Undecidable`; Stage 8: distributed commitment determination) |
| `id_i` and `e_i` both faulty | corruption, record unidentifiable | crash |

**Batching** (paros batches `WriteOp`s): the first faulty entry *without* an
identifier and everything after it is crash-truncatable; faulty entries before
that point are corruption. Snapshot and metainfo are exempt from entanglement
entirely — atomic-rename discipline discards a partial update on read, so a
mismatch there is always corruption (of that copy).

**TigerBeetle hardening on the tail rule** (`paros::classify_log`):
truncate-as-crash only inside the window strictly past a *provably certain*
head (the durable chosen index — a chosen slot is provably acknowledged), cap
the truncatable window by the maximum concurrently in-flight accept writes,
and abandon truncation entirely (⇒ crash) on any *valid-checksum* record past
the window opener (it might be a misdirected read masking the true shape).
The classifier is a total, named, exhaustively-tested decision function — a
`RecoveryCase` per point of the evidence cube, the case label on the tracing
event — after TigerBeetle's 16-case journal recovery table.

**Never truncate on a mismatch** otherwise: the classic bug (CTRL Figure 2,
found in both ZooKeeper and LogCabin) truncates from the faulty entry onward,
then wins an election with lagging peers and silently erases committed data
cluster-wide. The Stage-7 red demo reproduces exactly this bug class and the
audit must catch it.

## Sanity backstop for block-aligned misdirects

Slot indices in the physical log must be in order and monotonically
increasing — **on `slot` only, never on `accepted_ballot`**. Ballots are
legitimately non-monotonic across slots in Multi-Paxos: a new leader
re-proposes recovered slots at its ballot while neighbours keep old ballots.
This is the one place CTRL's Raft-shaped rule must be restated for Paxos.

## Metainfo doctrine

`HardState` is tens of bytes, updated rarely: keep **two** local checksummed
copies. One copy bad ⇒ use the other and repair it (`MetainfoVerdict::
RepairCopy`); both bad ⇒ crash — the node cannot know what it promised, and no
peer can tell it (that is Stage 8's safety argument, pre-stated here).
TigerBeetle's production-grade version is the 4-copy superblock with
`write_quorum + read_quorum = copies + 1` and a sequence/parent chain — noted
for the eventual storage engine, not built in the sim.

## EIO on read collapses into the corruption channel

An unreadable record is treated exactly as a checksum mismatch —
"zero-fill then mismatch" semantics (CTRL §4.1) — stamped
`CorruptionKind::ReadIo`. One detection path, one classification path.
Stage 6's `Io` taxonomy keeps its meaning on *writes* (crash semantics,
ambiguous durability); the read side routes here.

## User data vs filesystem metadata

Record corruption is disentangled and classified; **filesystem-metadata**
faults are not. Store missing/unopenable, wrong size (fixed-size preallocation
plus a separately stored snapshot size make it checkable), read-only store:
verdict is **reliably crash**, never attempted recovery — in Stage 8 either
(`StorageError::Metadata`). The oracle for this family is asymmetric:
unavailable = pass, unsafe = fail.

## Boot-time scan

On recovery the node scans its durable records **before** the core reads a
byte (`NodeStorage::boot_scan`, called at the top of `run_node`): verify every
record, produce the faulty sets, discard a crash-truncatable tail, repair a
lone bad metainfo copy — and in Stage 7 any remaining faulty record ⇒ crash
with the classified `StorageError::Corruption`. Zero silent bad reads: the
caller sees the typed outcome, never the bytes. The scan is the write-side
flush ordering's read-back pair, per the assertion doctrine's two-path rule.

## Detection pressure is protocol-blind (#70)

Both fault layers feed the same detector: moonpool's provider-level storage
faults prove detection is total regardless of what the fault hit, while the
sim world's per-record faults (riding the Stage-6 per-record budget) are
targetable per record — `corrupt(node, record)` — so #21's adversarial
promise-corruption test is a single injection. A single injected "block"
fault can hit a contiguous run of entries (CTRL injects per FS block), so
Stage 8's recovery must not assume faults are singletons.
