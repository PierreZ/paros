[← 01-goal.md](01-goal.md) · [Index](README.md) · [03-segments-and-records.md →](03-segments-and-records.md)

## J2. The data plane

*Moved here from `docs/architecture/02-data-plane.md` §2 to §2.9 and §2.14 (#1332 (journal architecture docs)).*

**Names at the tag.** Some paragraphs below describe how the service built a rule at the git tag
`v0.2.0-alpha.1`: names of the form `paros::client::…`, `paros::roles::…`, `DriverTunables`,
`PAROS_*`, `parosctl` and the simulation's BUGGIFY knobs. That code is not in the tree
(`docs/archive/v0.2.0-alpha.1/README.md`). The rule is the target; the name is a pointer to the
old code. The v1 names come with the journal crates: the fold and the codec with #1324
(paros-journal), the client with #1331 (the pure journal client).

Every journal exposes four calls, in one of two **modes**: **single-writer**, where one leader
appends under its leader uuid, and **multi-writer**, where anyone with access appends. The mode
is what the last applied `SetLeader` left: `SetLeader(None, old)` makes the journal
multi-writer, and `SetLeader(new, None)` makes it single-writer again (decided by Pierre on
2026-11-20, #1274 (multi-writer mode through SetLeader(None) and expected_seq); it replaces the
mode fixed at create time of 2026-10-04). §J2.4 has the rules of the switch. Every rule below is judged at apply time, in
slot order, by a small per-journal state machine
`(mode, leader_uuid, term, next_seq, first_seq)` beside the replica's walk. **The journal fold
lives in the journal crates, not in `paros-consensus`** (decided by Pierre on 2026-11-20, #1315
(bring docs and models in step with the v1 scope), decision 2): `SetLeader(None)`, the unset
leader, the order "position before fence" and the answer `Below` change no type of the core. The
core's fold (`journal/command.rs`, `journal/state.rs`) stays as it is today, for the users of the
library (architecture §4.3).

| Call | Single-writer | Multi-writer |
|---|---|---|
| `Write` | `Write(leader_uuid, expected_seq, batch) -> seq`: fenced by the leader uuid, contiguous by `expected_seq`, idempotent on retry, pipelineable. | `Write(batch) -> seq`: unfenced; the journal orders writes and assigns `seq` at apply. At-least-once on an ambiguous retry. |
| `Read(from_seq, limit, wait_ms?)` | The committed records from `from_seq`, plus `first_seq`, `next_seq` and the current leader uuid, or `Truncated` when `from_seq < first_seq`. `wait_ms` is capped and floored (§J2.7); a read at the tail waits up to it. | The same. |
| `Truncate` | `Truncate(leader_uuid, up_to_seq)`: fenced like `Write`. | `Truncate(up_to_seq)`: anyone may truncate. |
| `SetLeader(new_uuid, old_uuid)` | Compare-and-set the leader. Returns the journal's view after it: the leader uuid, `next_seq`, `first_seq`. `new_uuid = None` switches to multi-writer. | The same compare-and-set with `old_uuid = None`: it switches back to single-writer. |

The API is Brooker's fourth MemoryDB journal API (architecture §10): `set_leader_uuid(new, old)`,
`write(payload, leader_uuid)`, `read()`, plus the expected-sequence precondition of DSQL's
adjudicator. It replaced the `(generation, owner)` pair of M7 (#204), which exposed two fields
where one fence suffices and let a caller pick an owner id that another process could share.

**The answers of a call.** Each answer below is defined in the section that it names. "Final"
means that a resend of the same write gets the same answer for ever.

| Answer | Call | Final? | Meaning | Section |
|---|---|---|---|---|
| ok with `seq` | `Write` | yes | The write is chosen, applied in prefix order and holds `[seq, seq + n)` | §J2.1, §J2.2 |
| `Duplicate` with `seq` | fenced `Write` | yes | The record at that position is the same write (same uuid, same batch) | §J2.2, §J2.4.1 |
| refused, position taken | fenced `Write` | yes | Another write holds that position in this segment | §J2.4.1 |
| refused, `fenced` | fenced `Write`, `Truncate` | yes | The uuid is not the leader. Uuids are fresh, so the write never lands | §J2.3, §J2.4.1 |
| early | fenced `Write` | no | `expected_seq` is above `next_seq`: send it again later | §J2.2, §J2.4.1 |
| `Truncated` | `Write`, `Read` | no | The position is below `first_seq`. For a write, the fate is unknown | §J2.2, §J2.6 |
| `Below` | fenced `Write` | no | The position is in an older segment: the client reads that segment to learn the fate. **Open**: the wire shape (#1320) | §J2.2 |
| `Unknown` | `Write` | no | A deposed leader held the write and cannot tell its fate. The client resends the same write at the same `expected_seq` | §J2.2 |
| `WRONG_MODE` | `Write`, `Truncate` | yes for that copy | The call is shaped for the other mode | §J2.4.1 |
| `TooLarge` | `Write` | yes for that node | The batch is over the limits of the node; the write is in no slot | §J2.7 |
| `busy` | `Write` | no | Admission control; the write is in no slot | §J2.7 |
| unserved | `Read` | no | This server cannot produce the page; ask another server | §J2.5 |
| the view | `SetLeader` | yes | The leader value, `next_seq` and `first_seq` after the compare-and-set, or the refusal with the current leader | §J2.3, §J2.4.1 |

A write that holds a slot is never answered `NotLeader` (§J2.2, #1232 (held writes)).

### J2.1 Positions

`seq` is the dense position of accepted records, assigned at apply. One `seq` per record: a
batch of `n` records occupies `[seq, seq + n)`, in one Paxos slot, accepted or refused whole.
Paxos slots stay internal: a `Noop` a new leader fills a hole with, a control command, a
leadership change, a refused `Write` and a `Truncate` each consume a slot and no `seq`. Readers
never see a hole.

### J2.2 Single-writer writes and fencing

A single-writer `Write` is accepted iff its `leader_uuid` is the journal's current one and
`expected_seq == next_seq`. Otherwise it is refused in place, and the refusal names the current
leader uuid and `next_seq` so the leader can continue or learn it was superseded.

Retries are answered from the log itself: a `Write` with `expected_seq < next_seq` identical to
the write accepted at that `seq` — the same leader uuid and batch — is an idempotent ack;
anything else is refused. A retry whose `expected_seq < first_seq` is answered `Truncated`, and
the leader treats it as ambiguous and reads the tail. The log is the deduplication table: there
is no per-client session ledger and nothing to expire. The client settles an ambiguous `Write`
with a read-back before each identical re-send (`Client::resolve`): a position below the floor is
`Truncated`; a tail short of it under another leader proves it not written; a page that holds
exactly the batch at its position, ending at the tail, while the journal's leader is still the
write's uuid, at or above a position where the writer saw that uuid lead (`Writer::led_from`),
proves it written (decided on 2026-10-15, #564), so a client whose deadline is shorter than the
commit path still settles. That proof needs one more fact: no other write the writer opened under
that uuid shares the batch's positions. A writer that rolls back behind writes whose fate it never
learned (a broken pipeline) can find its own earlier batches there with the same bytes, and the
journal refuses the new batch. Such a write settles by a re-send instead. Both proofs rest on fresh uuids (§J2.3). One slot holds one single-writer `Write`;
a single-writer journal batches in its client (`paros::client::Writer`), never in a batcher.

A leader may refuse a `Write` from its own fold at propose time, so a superseded leader's
pipelined burst does not burn slots. That is an optimisation; the apply-time check is the safety.

**A held answer belongs to the command, not to the slot** (decided by Pierre on 2026-11-20,
#1232 (held writes)). The consensus leader holds the answer of each `Write` it proposed until the
slot is applied. After a leader change, the slot of a deposed leader can choose another command
or a `Noop`. So a leader answers a held write from the outcome of its slot only when the command
applied at the slot is the one it proposed there. Else it answers `Unknown`, and it may answer
`Unknown` at any time. It never answers `NotLeader` for a write that holds a slot: the caller
would send it again, and both copies can land. A single writer resolves an `Unknown` with a
resend at the same `expected_seq` (exactly-once, the fold judges the position); a batcher answers
each write of the batch unanswered (at-least-once, §J2.4). TLC checks the rule in
`specs/tla/compartments/` (`AckedChosen`; the control `held_slot` loses an acked write, the
control `held_notleader` lands a write two times) and the resend in `specs/tla/multi-writer/`.

**A resend across a seal is answered `Below`** (#1301 (a resent write across a seal), TLC
2026-11-20, `specs/tla/multi-writer/`). The fold of a segment holds the boundary state, not the
records of the old segment (§J3.1, #1271), so it cannot tell a `Duplicate` from a refusal at a
position of the old segment. It answers `Below` there. `Below` is not final, like `Truncated`.
The client reads that position from the replica set of the old segment: a sealed segment does
not change, so the record there tells it the fate of its write, written or not (or `Truncated`
below `first_seq`). Without the rule, the fold answers the resend of a written write "position
taken", and a writer that writes it again lands it two times (controls `seal_local`,
`seal_local_twice`). A fold that reads the old record at apply is safe too (`seal_global`), but
apply would wait for another replica set. A digest of the last writes in the boundary state is
safe only while it covers the writer's whole pipeline (`seal_digest`).

**Open** (#1320 (the wire shape that resolves a `Below` answer); Pierre picks): how the
client resolves `Below`. The model resolves it with the writer uuid and the write of the record
at that position, which a `Read` does not return today. The options are a `Resolve` call that a
replica of the old segment judges against its stored entry, a field of `Read` with the writer
uuid of each record, or a compare of bytes. Each one is a format fact of v1.

The client pipelines a single-writer journal's writes with `paros::client::Pipeline` (#597
(several writes in flight per journal)): one writer's leadership, shared by its callers, each
write opened at the position after the last one and sent without waiting for the answer of the
write in front of it. A write that reaches the leader first is refused under its own uuid at a
tail below its position. It waits until every write in front of it settled, then is sent again
unchanged when the tail reached its position (a retry is the same write). When a write in front
of it failed, the tail never reaches its position, and its one verdict is that refusal. A write
whose answer is lost while a write in front of it is in flight waits the same way (#910 (paths
that almost never run)): the re-send that settles an ambiguous write is refused while the tail is
below it, so only the writes in front of it can settle it. When the tail reached its position, it
is sent again unchanged (the log answers `Duplicate` if it holds it); when a write in front of it
failed, it stays ambiguous. A write
that is not written stops new writes until the ones in flight settle; the next one then opens
at the tail the writer knows. A claim that wins a new term opens at the tail its view names at
once, because the old term's writes in flight are fenced or already counted. The simulation's
owner writes through it on its own BUGGIFY location, on a coin last position first and on
another with a claim racing the writes; on a pipelined-writer seed it does so on half its writes,
always last position first.

Writes sent together merge into one write (decided on 2026-10-29, #709 (merge fenced writes)).
A send waits one turn of the executor, so the sends beside it get ready too. The first ready
write then takes the ready writes at the positions after it, under the same uuid, into one write
at its own position, up to 256 records and 256 KiB (`MERGE_RECORDS`, `MERGE_BYTES`). The journal
judges the merged write as one write in one slot, and each caller gets its own `seq` range. A
verdict that is not a write (a refusal, a fence, an ambiguity) is every caller's verdict, and the
rules above then apply. A merged write that a leader refuses as too large comes apart: each write
is sent alone, and never merged again. The merge is in the pipeline, not in a batcher. The log
deduplicates a re-send only when it is the identical write (same uuid, position and records). A
pipeline re-sends its merged write unchanged: an early write, or a resolve of an ambiguous one.
A batcher cannot keep that promise: a caller re-sends its own write alone, or the batcher merges
it again with other writes, and the journal then refuses a write that it holds.

### J2.3 Leadership

Leadership is a pure compare-and-set, no lease and no clock. `SetLeader(new_uuid, old_uuid)`
succeeds iff `old_uuid` is the current leader (unset on a fresh journal); the journal records the
change as an ordinary log entry and answers the view after it (the leader uuid, `next_seq`,
`first_seq`) so the new leader
can continue the sequence. Every tailer learns the leader changed in-band, without a side channel.

**The leader uuid is the fence.** It is a 128-bit random value the leader draws for one
leadership term, never per process: a process that wins again draws a new uuid, which fences its
own older in-flight writes. It is not a secret and not an authentication token; the frontend
decides who may touch a tenant at all (architecture §3.5), the leader uuid decides which of the
tenant's clients holds the pen. The core keeps a hidden term counter beside it, raised by every
`SetLeader`, that never reaches a data-plane reply (only an operator's `Inspect` shows it).

**The journal trusts its clients to draw fresh uuids** (decided on 2026-10-09, #241). A
`SetLeader` is refused only when `old_uuid` is not the current leader, when `new_uuid` is the
current one, or when it is the unset uuid; a uuid that led before and is named again wins. The
journal keeps no history of past uuids: the term counter alone cannot tell a reinstated uuid from a
fresh one, and remembering every uuid that ever led would grow the state each trim point carries.
Since every call names both `old_uuid` and `new_uuid`, a client that reinstates a former leader's
uuid (an `A → B → A` sequence) does so deliberately, and only its own in-flight writes under that
uuid are at stake. `paros::client::Writer` derives a new uuid for every term from a random seed.
This is **decision C** (PR #281): a reinstated uuid leads again, and the core does not refuse it.
The audit rule "a uuid is never reinstated" is replaced by a reachable, "a reinstated uuid leads
again". The simulation checks the journal's guarantees under clients that do: a
swarm knob (`reinstate_pct`) makes a superseded writer reinstate the uuid it last led with, and
every invariant of architecture §6 holds through it.

A superseded leader's writes and truncations are refused at apply, which is the whole safety
argument. What keeps a superseded leader from *serving* stale data is a rule on the leader, not
on paros: a leader serves nothing from local state it did not read back from the journal (DSQL's
adjudicator is a rebuildable cache over the log and holds no truth of its own). MemoryDB's lease
and self-demotion are deliberately not implemented in the journal: **paros enforces no lease**.
Who becomes leader is decided by the callers of `SetLeader`: the compare-and-set is itself the
election, as the term of architecture §3.3 uses it (decision 13), and the public election API offers
that term to customers. The journal gives safety to writers, the election gives liveness
(Brooker).

Two fences, two layers, never confused: the Paxos ballot says which *machine* runs a journal's
consensus leader and is invisible to clients; the leader uuid says which *client* may write and
is invisible to Paxos.

### J2.4 Multi-writer journals

A multi-writer journal has no leader: `Write(batch)` is accepted whenever its batch is within
the limits (§J2.7), and the journal assigns its `seq` at apply. There is no expected
sequence and no deduplication: an ambiguous write retried may land twice, and the writers own
that (at-least-once). Anyone with access to the journal may `Truncate` it; `first_seq` stays
monotone. A multi-writer journal is what many independent producers append to. Batchers (architecture §4) merge multi-writer writes into fewer
slots; the unbatcher hands each write its own `seq` range.

How the batcher works (#145 (batchers), decided on 2026-10-19). A batcher serves `BatchWrite`
(`0x5041_0501`): the plain `Write` request and answer, sent to a batcher and not to a node. It
keeps one lane per journal (`paros_consensus::Batcher`, pure). A lane flushes when it holds
`batch_flush_records` records, when the next write would pass the leader's limits, or at the
`batch_tick` after its first write. A flushed lane goes to the journal's leader as one unfenced
`Write` of the concatenated records, so one slot holds many writers' writes. The journal gives
the merged write its range `[seq, seq + n)` at apply. The unbatcher (`paros_consensus::Unbatcher`)
cuts that range in order: each write gets the positions of its own records, densely. A merged
write is accepted or refused whole. The unbatcher runs in the batcher's process: the leader
answers a held write itself, so the answer comes back to the batcher that sent it.

The batcher follows the leader hints itself and removes them from the answer. A merged write that
the leader refuses as too large teaches the batcher the leader's limits, and the batcher sends
each write of it again alone. A merged write that may have run with no answer is answered
unanswered (ambiguous), the same as a frontend's, and the batcher never sends it again: the
writers own an ambiguous write (at-least-once). A write that carries a leader uuid (a
single-writer journal's) is never merged: it goes to the leader alone, with its fence and its
position, and the journal judges it (a single-writer journal merges in its `Pipeline`, §J2.2,
#709 (merge fenced writes)). An empty write also goes alone.


#### J2.4.1 The switch between modes

`SetLeader(new, old)` may name `new = None` (decided by Pierre on 2026-11-20, #1274). The
journal state holds one leader value: a uuid (single-writer) or `None` (multi-writer). The
compare-and-set does not change: it succeeds iff `old` is the current value and `new` is not.
There is no dedup table and no dedup role: a single writer gets exactly-once through
`expected_seq`, and multi-writer is at-least-once. TLC checks the rules below in
`specs/tla/multi-writer/`, each one with a control that breaks it:

- **The fence is judged at apply, in both directions.** A fenced `Write` or `Truncate` is
  accepted only while its uuid is the leader, an unfenced one only while the leader is `None`.
  A leader that also refuses at propose time saves slots, but a check at propose time only lets
  a fenced write of a deposed writer land after `SetLeader(None, it)`, or an unfenced truncate
  land after `SetLeader(u, None)` (controls `fence_propose`, `fence_propose_write`).
- **The fold judges the position of a fenced `Write` before its fence.** In this order: below
  `first_seq` it is `Truncated`; below `next_seq` it is `Duplicate` (with its `seq`) when the
  record there is the same write (same uuid, same batch), else it is refused (the position is
  taken); then a uuid that is not the leader is refused (`fenced`); above `next_seq` it is early;
  else it is accepted. So the resend of a write that was in flight across a switch tells the
  writer its fate: `Duplicate` means written at that `seq`; `fenced` or position taken means not
  written, now and for ever, because uuids are fresh (§J2.3). With the fence first, the
  resend of a write that was applied before `SetLeader(None, w)` is refused as `fenced`; the
  writer then writes it again, unfenced, and it is in the journal two times (controls
  `dedup_after`, `dedup_after_twice`).
- **What a writer sees for a write in flight across `SetLeader(None, w)`.** The first answer
  may be lost (`Unknown`, #1232 (held writes)). A resend with the same `expected_seq` gets one of
  four answers: `Duplicate` (written), refused (not written, final), early (resend later), or
  `Truncated`. `Truncated` leaves the fate unknown, as in §J2.2. In multi-writer mode any
  writer may truncate (§J2.6), so a writer that is not the leader can push the position of
  another writer's held write below `first_seq` (check `reach_truncated`). This is the only
  case where a resend does not resolve an `Unknown`.
- **A writer that leaves single-writer mode stays exactly-once** when it writes again, unfenced,
  only after a final refusal: the write is then in the journal once (`ExactlyOnce` in `safety`).
- **The switch back, `SetLeader(w2, None)`.** After it applies, only fenced writes of `w2` are
  accepted, and unfenced writes in flight are refused `WRONG_MODE`. `w2` takes its next
  `expected_seq` from the view of the answer (`next_seq` after the `SetLeader`), never from a
  position it knew before, so it never aims a write at a position that the multi-writer mode
  used (control `basis_stale`). The journal stays safe without this rule: the position check
  refuses such a write. The rule is for the writer: its first write after the switch lands.
- **A refusal of an unfenced write is true for that copy only.** An unfenced write has no
  identity in the log, so a resend cannot tell the writer whether an earlier copy landed: a copy
  can land while another copy is refused `WRONG_MODE` (check `multi_refusal`, not a rule). A
  multi-writer writer that needs to know reads the journal.
- **`Truncate` keeps its rules in both modes** (#228, #241, §J2.6): fenced by the uuid in
  single-writer mode, open in multi-writer mode, judged at apply, `first_seq` monotone and
  clamped to `next_seq`.
- **The seal carries `None`.** The leader uuid of the boundary state (§J3.1) may be
  `None`. The segment record holds it, and the next segment's fold starts from that record, so
  it starts multi-writer (#1303 (two seals in a row)). `multi-writer/` checked the first form of
  §J3.1: the journal's matchmaker record held the uuid, and the first leader of the next
  segment proposed `Fence(None)` (check `seal_long`, reach `reach_fence_none`). There, a `Fence`
  read before the seal wrote the uuid lets a writer of the wrong mode in (control
  `boundary_early`). The record needs no third value for "not written": a replica of the next
  segment starts only from a written record.

**The leader of a fresh journal is unset**, as §J2.3 says: every write and truncate is refused
until a `SetLeader`. A fresh journal that is multi-writer (`None`) is safe too. `multi-writer/`
passes with both (`safety`, `unset`), and so does the composed model `journal-state/` across three
segments (`chain` starts unset, `chain_fresh_none` starts at `None`; #1189 (the journal state
model)). The choice is not a safety rule: a change to `None` needs no new check.

### J2.5 Reads

`Read` is served by whatever holds the journal's replica state — the journal's replicas (a
create with no other word gets one, decision 22), or the colocated node — and always through the leaderless read of
Compartmentalized Paxos §3.4 (paros's `QuorumRead`): the server asks a Phase-1 quorum of the
acceptors for their vote watermarks, waits until its own applied prefix covers the maximum, and
answers. No read goes through the Paxos leader, so reads scale with replicas and cost the
acceptors one watermark round per page (decided on 2026-10-04). The read-index path and
`CheckTail` retire.

A read costs one Phase-1 round of watermarks (decided on 2026-10-07, #253). In a multi-region cell
`QuorumRead` asks three of five acceptors, so a page costs one cross-region round trip (the 2025
Journal reconstruction says the same: reads are performed from at least two regions, architecture §10).
There is no lease read, because paros enforces no lease (§J2.3). The page size (§J2.7)
is the mitigation already in the API, with the tail wait on `wait_ms`.

A server answers only from records it holds (decided on 2026-10-06). A server whose floor rose
ahead of its fold — it jumped to a peer's trim point, and the `Truncate` that let the peer's
floor rise lies above it, not yet applied here — still counts records below its floor in the
journal. A read there is answered **unserved**, the same answer as a read whose confirmation
timed out, and the client asks another server: never `Truncated` (the journal still has the
record) and never a page (this server cannot produce it).

**A read is fenced on the promise** (review of paros-consensus of 2026-11-16, finding 1; built in PR
#1074 (stale read fix), step 1.10). The server asks a Phase-1 quorum of the acceptors for their
vote watermarks, and each answer carries the acceptor's promise, never the ballot of the
configuration that the acceptor believes. An acceptor that promised a newer campaign therefore
cannot let a read complete under an older configuration. A server answers once its fold covers
the maximum watermark of the quorum and the highest slot that a won Phase 1 could have found
chosen. Reads are unavailable during a campaign, never stale. Plain Multi-Paxos is unchanged:
its configuration is static.

**A server learns its configuration from the register only** (rule 1, architecture §4.1, step 2.5).
The matchmakers hold the configuration in force and at most one open successor, so no server
holds a belief that a quorum never put in force, and no server campaigns on such a belief. The
belief rules and the exception of #890 (learner above the leader) leave with the registrations.
One `Belief` role holds the rule to learn a configuration and the read basis, and every
deployment calls it (step 1.12).

**The row read on a full grid needs a seal that reaches each row** (decision 31, decided on
2026-11-19 with the trace of step 1.13, #1102). On a full grid a column writes and a row elects
and reads; these are quorums but not majorities, and two rows never meet. The reads model
(`specs/tla/reads/`, check `grid_full`) shows the stale read: the Phase 1 of a new configuration
takes one row of the old grid, a column of the new grid chooses and the leader acks, and a
reader whose basis is still the old grid asks the other row, which did not hear of the new
ballot. The rule: Phase 1 takes a Phase-1 quorum of each configuration of `H_b` **and** one
promise in each row of each full grid of `H_b`, so each row of an old grid holds an acceptor
that promised the new ballot and the `PreReadAck` ballot ends a read over it (check
`grid_full_seal` passes). A full grid keeps its change of members. This rule is not in the
papers (Compartmentalized Paxos §3.4 reads one row of a static grid); its model is the check. The
code follows in #1209 (seal each row of a full grid). The partitioned grid of architecture §3.4 (F+1
of one column write, F+1 of each column elect) needs no seal: two of its Phase-1 quorums meet.

### J2.6 Truncation

A `Truncate` is proposed through consensus and judged at apply in slot order like a `Write`: in
a single-writer journal it is accepted iff its leader uuid is the current one, in a multi-writer
journal always. It never lowers `first_seq`, and it is clamped to `next_seq`. A refusal names the
current leader, like a refused `Write`. An accepted `Truncate` is applied lazily by every node
when its contiguous chosen walk reaches it, exactly today's `Truncate` control command.

The single-writer fence was decided on 2026-10-02 (#227, #228). Unfenced, anyone with access to
the tenant could truncate any of its journals, so a stale or buggy caller could truncate to a
position that is not a checkpoint and break every reader's fold. Kafka refuses client
`DeleteRecords` on its metadata topic for the same reason (KIP-630). A multi-writer journal has
no checkpoint discipline to protect, so its truncation is open (decided on 2026-10-04).

The leader truncates only after it has secured whatever checkpoint it needs; paros does not
check that, does not verify checkpoints and never will. A reader below `first_seq` is told
`Truncated` and nothing else: where it restarts is the application's contract with itself. The
control plane's own contract is architecture §3.9.

### J2.7 Limits

The limits are part of the API, not a driver detail (decided on 2026-10-04): a maximum batch
(bytes and records) refused at the edge before it reaches consensus, a maximum `Read` page
(records and bytes), and a maximum and a minimum `wait_ms`. Each value is a tunable with a
documented floor and a `buggify_knob!` in simulation; the toy's values are today's (a 256-record,
64 KiB page, a batch well under the 4 MiB RPC frame). **`wait_ms` is a capped and floored field
only** (decided on 2026-10-09, #241): the node caps it at the maximum and raises a non-zero one
to the minimum (the maximum wins when the two cross; 0 still answers at once). #241 builds no
new long-poll: the tail wait of #185 stays as it was, a confirmed read at or past `next_seq`
re-served after every batch until its capped wait runs out, then answered empty. A checkpoint
(architecture §3.9) is a run of small records, so no state must fit one batch. Every id, `seq` and the term counter are `u64`; the
leader uuid is 128 bits.

The batch limits are `DriverTunables::max_batch_records` (1,024 by default) and
`max_batch_bytes` (1 MiB by default, the sum of the record bytes). The node that receives a
`Write` checks them before it proposes anything. A batch over either limit gets the answer
`TooLarge`, which names the two limits. That write is in no slot. Different nodes can have
different limits, so a retry to another node can get a different answer (#241).

**Admission control** (#360 (disk full), decided on 2026-10-18). A leader slows `Write` admission
when its acceptors fall behind: when it holds `admission_rounds` undecided Phase-2 rounds or more
(256 in the shipped profile, floor 1), it answers a new `Write` `busy` before it proposes
anything. The lag is the leader's own count of rounds its acceptors have not decided, so a slow,
full or read-only acceptor shows there first. `busy` is no verdict and the write is in no slot.
The client library sends the identical write to the same leader again after `redirect_backoff`,
until its request deadline (`WriteOutcome::Busy` when the deadline passes). The metrics count it
under the outcome `busy`.

The read limits are `DriverTunables::max_read_records` (256 by default, floor 1: a `limit` of 0
or above it is cut to it), `max_read_bytes` (64 KiB, floor 1: a page always holds one record,
whatever its size), `max_wait_ms` (400 ms in the simulation's baseline, 1 s in `parosd`, floor 0)
and `min_wait_ms` (0, floor 0). They are `PAROS_*` overrides in `parosd`. The node applies them
when it parks the read (`paros::roles::node::log_reads::ReadLimits`); a page shorter than the
client's `limit` is the server's limit, and the client reads on from the page's end.

### J2.8 Record headers

**Every journal record carries headers** (decided by Pierre on 2026-11-20, #1299 (record headers
from clients and from paros)). A client can give headers with its `Write`, and paros can add its
own, for example the leader time. This is the only idea of the Apache Iggy study that paros
takes. The headers belong to the journal record format, not to `paros-consensus`, which keeps
carrying opaque bytes (architecture §4.3). Pierre answered three points of #1299 on
2026-11-20:

- **The leader time is a hint only.** It promises no order across a leader change or a seal,
  enters no safety rule and needs no TLA+ invariant.
- **It is stamped per write**, not per batch.
- **Client headers are opaque.** The journal never reads them, and a read cannot filter on them.
- **In v1 paros adds only the leader time**, and keeps a reserved key space for later paros
  headers (approved by Pierre on 2026-11-20).

**Open** (#1299 decides each one): who stamps the leader time (the leader at propose or the
batcher); the encoding, the size limits and the exact reserved key space; and
whether the headers are inside the value checksum and the offload hash.

### J2.9 Underneath

Paxos is unchanged: replication, holes, gap fills, the contiguous chosen prefix, CTRL, the trim
point, matchmaker reconfiguration. Every proof paros has holds per journal; the one new thing to
prove is the control state machine's rules, and they are judged in the simulation like every
other rule.

### J2.10 Store durability

**Every store uses the ordered protocol** (decision 15, question 4; step 0.10). Each commit
writes the entries, syncs, writes the persist records and syncs again, so a crash never leaves a
batch ambiguous. There is no durability mode for a journal: the batched store, its commit path,
its create word (`--store-durability`) and its simulation coin are deleted. The batched store
can come back later as a measured change, if the benchmark shows the cost of the second sync
(architecture §11).
