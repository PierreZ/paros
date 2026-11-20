[← 01-goal.md](01-goal.md) · [Index](README.md) · [03-1-cell-tenant-and-bootstrap.md →](03-1-cell-tenant-and-bootstrap.md)

## 2. The data plane

Every journal exposes four calls, in one of two **modes**: **single-writer**, where one leader
appends under its leader uuid, and **multi-writer**, where anyone with access appends. The mode
is what the last applied `SetLeader` left: `SetLeader(None, old)` makes the journal
multi-writer, and `SetLeader(new, None)` makes it single-writer again (decided by Pierre on
2026-11-20, #1274 (multi-writer mode through SetLeader(None) and expected_seq); it replaces the
mode fixed at create time of 2026-10-04). Section 2.4 has the rules of the switch. Every rule below is judged at apply time, in
slot order, by a small per-journal state machine
`(mode, leader_uuid, term, next_seq, first_seq)` that lives in `paros-consensus` beside the replica's
walk.

| Call | Single-writer | Multi-writer |
|---|---|---|
| `Write` | `Write(leader_uuid, expected_seq, batch) -> seq`: fenced by the leader uuid, contiguous by `expected_seq`, idempotent on retry, pipelineable. | `Write(batch) -> seq`: unfenced; the journal orders writes and assigns `seq` at apply. At-least-once on an ambiguous retry. |
| `Read(from_seq, limit, wait_ms?)` | The committed records from `from_seq`, plus `first_seq`, `next_seq` and the current leader uuid, or `Truncated` when `from_seq < first_seq`. `wait_ms` is capped and floored (section 2.7); a read at the tail waits up to it. | The same. |
| `Truncate` | `Truncate(leader_uuid, up_to_seq)`: fenced like `Write`. | `Truncate(up_to_seq)`: anyone may truncate. |
| `SetLeader(new_uuid, old_uuid)` | Compare-and-set the leader. Returns the journal's view after it: the leader uuid, `next_seq`, `first_seq`. `new_uuid = None` switches to multi-writer. | The same compare-and-set with `old_uuid = None`: it switches back to single-writer. |

The API is Brooker's fourth MemoryDB journal API (section 10): `set_leader_uuid(new, old)`,
`write(payload, leader_uuid)`, `read()`, plus the expected-sequence precondition of DSQL's
adjudicator. It replaced the `(generation, owner)` pair of M7 (#204), which exposed two fields
where one fence suffices and let a caller pick an owner id that another process could share.

### 2.1 Positions

`seq` is the dense position of accepted records, assigned at apply. One `seq` per record: a
batch of `n` records occupies `[seq, seq + n)`, in one Paxos slot, accepted or refused whole.
Paxos slots stay internal: a `Noop` a new leader fills a hole with, a control command, a
leadership change, a refused `Write` and a `Truncate` each consume a slot and no `seq`. Readers
never see a hole.

### 2.2 Single-writer writes and fencing

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
journal refuses the new batch. Such a write settles by a re-send instead. Both proofs rest on fresh uuids (§2.3). One slot holds one single-writer `Write`;
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
each write of the batch unanswered (at-least-once, §2.4). TLC checks the rule in
`specs/tla/compartments/` (`AckedChosen`; the control `held_slot` loses an acked write, the
control `held_notleader` lands a write two times) and the resend in `specs/tla/multi-writer/`.

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

### 2.3 Leadership

Leadership is a pure compare-and-set, no lease and no clock. `SetLeader(new_uuid, old_uuid)`
succeeds iff `old_uuid` is the current leader (unset on a fresh journal); the journal records the
change as an ordinary log entry and answers the view after it (the leader uuid, `next_seq`,
`first_seq`) so the new leader
can continue the sequence. Every tailer learns the leader changed in-band, without a side channel.

**The leader uuid is the fence.** It is a 128-bit random value the leader draws for one
leadership term, never per process: a process that wins again draws a new uuid, which fences its
own older in-flight writes. It is not a secret and not an authentication token; the frontend
decides who may touch a tenant at all (section 3.5), the leader uuid decides which of the
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
every invariant of section 6 holds through it.

A superseded leader's writes and truncations are refused at apply, which is the whole safety
argument. What keeps a superseded leader from *serving* stale data is a rule on the leader, not
on paros: a leader serves nothing from local state it did not read back from the journal (DSQL's
adjudicator is a rebuildable cache over the log and holds no truth of its own). MemoryDB's lease
and self-demotion are deliberately not implemented in the journal: **paros enforces no lease**.
Who becomes leader is decided by the callers of `SetLeader`: the compare-and-set is itself the
election, as the term of section 3.3 uses it (decision 13), and the public election API offers
that term to customers. The journal gives safety to writers, the election gives liveness
(Brooker).

Two fences, two layers, never confused: the Paxos ballot says which *machine* runs a journal's
consensus leader and is invisible to clients; the leader uuid says which *client* may write and
is invisible to Paxos.

### 2.4 Multi-writer journals

A multi-writer journal has no leader: `Write(batch)` is accepted whenever its batch is within
the limits (section 2.7), and the journal assigns its `seq` at apply. There is no expected
sequence and no deduplication: an ambiguous write retried may land twice, and the writers own
that (at-least-once). Anyone with access to the journal may `Truncate` it; `first_seq` stays
monotone. A multi-writer journal is what many independent producers append to. Batchers (section 4) merge multi-writer writes into fewer
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
position, and the journal judges it (a single-writer journal merges in its `Pipeline`, §2.2,
#709 (merge fenced writes)). An empty write also goes alone.

Every machine serves the batcher endpoint over the cell's address book. The cell books each
multi-writer journal's batchers, by the `batchers` count that a person typed for it (rule 7), on
the cell's `stateless` machines (section 3.4), and `Resolve` names them (section 3.5). A client of a multi-writer journal sends its writes
to a batcher it was given (`Resolved::batcher_servers`). `paros::roles::batcher::run_batcher` runs a
batcher on its own address, in front of a fixed list of nodes.

How it was built at the tag `v0.2.0-alpha.1` (#241, part 3), before the mode became the
leader of the journal state (#1274, section 2.4.1): the mode was a field of the journal's
`Config`, so the format marker recorded it and a restart could not change it. `CreateJournal`
carried it, and a journal without one was single-writer. On the wire a multi-writer call uses the same messages: a
`Write` or `Truncate` carries the unset leader uuid (`0`) and `expected_seq` 0, and the core
ignores that `seq`. The mode is judged at apply, like every other rule. A call shaped for the
other mode is refused in place with `WRONG_MODE` and the journal's view: an unfenced call on
a single-writer journal, and a fenced call on a multi-writer one.
`paros::client::multi` builds the unfenced requests, and `parosctl write --multi` and
`parosctl truncate --multi` send them. `parosctl journal create --writers` (named `--mode` until #523) comes with the
journal-create request to the tenant coordinator (#210, decided on 2026-10-09), not with #241. The simulation draws the mode per journal, judges every
attempt against the same model in that mode, and a client sends the wrong-mode calls on
purpose.

#### 2.4.1 The switch between modes

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
  written, now and for ever, because uuids are fresh (section 2.3). With the fence first, the
  resend of a write that was applied before `SetLeader(None, w)` is refused as `fenced`; the
  writer then writes it again, unfenced, and it is in the journal two times (controls
  `dedup_after`, `dedup_after_twice`).
- **What a writer sees for a write in flight across `SetLeader(None, w)`.** The first answer
  may be lost (`Unknown`, #1232 (held writes)). A resend with the same `expected_seq` gets one of
  four answers: `Duplicate` (written), refused (not written, final), early (resend later), or
  `Truncated`. `Truncated` leaves the fate unknown, as in section 2.2. In multi-writer mode any
  writer may truncate (section 2.6), so a writer that is not the leader can push the position of
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
- **`Truncate` keeps its rules in both modes** (#228, #241, section 2.6): fenced by the uuid in
  single-writer mode, open in multi-writer mode, judged at apply, `first_seq` monotone and
  clamped to `next_seq`.
- **The seal carries `None`.** The leader uuid of the boundary state (section 2.10) may be
  `None`. The journal's matchmaker record holds it, and the first leader of the next segment
  proposes `Fence(None)`, which makes the next segment's fold multi-writer (check `seal_long`,
  reach `reach_fence_none`). The record needs no third value for "not written": the leader
  reads the uuid only after the seal wrote the boundary state, the same compare-and-set that
  writes `next_seq` into the segment record (#1271). A `Fence` read before that write lets a
  writer of the wrong mode in (control `boundary_early`).

**Open**: the leader of a fresh journal, before its first `SetLeader`: `None` (a fresh journal
is multi-writer) or unset (every write and truncate is refused until a `SetLeader`). The model
passes with both (`safety`, `unset`); the journal state model (#1189) decides it.

### 2.5 Reads

`Read` is served by whatever holds the journal's replica state — the journal's replicas (a
create with no other word gets one, decision 22), or the colocated node — and always through the leaderless read of
Compartmentalized Paxos §3.4 (paros's `QuorumRead`): the server asks a Phase-1 quorum of the
acceptors for their vote watermarks, waits until its own applied prefix covers the maximum, and
answers. No read goes through the Paxos leader, so reads scale with replicas and cost the
acceptors one watermark round per page (decided on 2026-10-04). The read-index path and
`CheckTail` retire.

A read costs one Phase-1 round of watermarks (decided on 2026-10-07, #253). In a multi-region cell
`QuorumRead` asks three of five acceptors, so a page costs one cross-region round trip (the 2025
Journal reconstruction says the same: reads are performed from at least two regions, section 10).
There is no lease read, because paros enforces no lease (section 2.3). The page size (section 2.7)
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

**A server learns its configuration from the register only** (rule 1, section 4.1, step 2.5).
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
code follows in #1209 (seal each row of a full grid). The partitioned grid of section 3.4 (F+1
of one column write, F+1 of each column elect) needs no seal: two of its Phase-1 quorums meet.

### 2.6 Truncation

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
control plane's own contract is section 3.9.

### 2.7 Limits

The limits are part of the API, not a driver detail (decided on 2026-10-04): a maximum batch
(bytes and records) refused at the edge before it reaches consensus, a maximum `Read` page
(records and bytes), and a maximum and a minimum `wait_ms`. Each value is a tunable with a
documented floor and a `buggify_knob!` in simulation; the toy's values are today's (a 256-record,
64 KiB page, a batch well under the 4 MiB RPC frame). **`wait_ms` is a capped and floored field
only** (decided on 2026-10-09, #241): the node caps it at the maximum and raises a non-zero one
to the minimum (the maximum wins when the two cross; 0 still answers at once). #241 builds no
new long-poll: the tail wait of #185 stays as it was, a confirmed read at or past `next_seq`
re-served after every batch until its capped wait runs out, then answered empty. A checkpoint
(section 3.9) is a run of small records, so no state must fit one batch. Every id, `seq` and the term counter are `u64`; the
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

### 2.8 Underneath

Paxos is unchanged: replication, holes, gap fills, the contiguous chosen prefix, CTRL, the trim
point, matchmaker reconfiguration. Every proof paros has holds per journal; the one new thing to
prove is the control state machine's rules, and they are judged in the simulation like every
other rule.

### 2.9 Store durability

**Every store uses the ordered protocol** (decision 15, question 4; step 0.10). Each commit
writes the entries, syncs, writes the persist records and syncs again, so a crash never leaves a
batch ambiguous. There is no durability mode for a journal: the batched store, its commit path,
its create word (`--store-durability`) and its simulation coin are deleted. The batched store
can come back later as a measured change, if the benchmark shows the cost of the second sync
(section 11).

### 2.10 Segments

**The matchmakers of a journal hold its segment table** (decided by Pierre on 2026-11-19, with
the TLA+ experiment of #1213 (segments in the protocol or in storage),
`specs/tla/experiments/segments/`). A segment is the run of slots that one configuration (one
grid) holds. Only a reshape or a roll opens a segment; a leader change opens none. The
matchmakers keep one record for each segment until trim: its start slot, its end slot, its grid
and its state (open, sealed, retired, offloaded, trimmed). A segment whose only copy is in the
object store is still in the table, so the grid controller, placement, the offload policy and
the readers see where each slot range lives as decided state. The rules that the experiment
checked, each with a control that TLC breaks:

- The first leader of the next configuration writes the end slot once, by compare-and-set, after
  its Phase 1. The end is **one above the highest slot where its Phase 1 saw a vote in the old
  grid**, not the first uncommitted slot: with that end, a Phase 1 that reads the old grid only
  below the end chooses two values for one slot. The sealing leader proposes again, in its own
  grid, each vote below the end that the replicas did not apply.
- **A leader of the configuration in force writes every slot above the applied prefix and below
  the start of its segment**: the vote that its Phase 1 saw, or a `Noop` where it saw none (the
  election gap fill of the core, #1233 (segments gap fill)). Without the `Noop`, a slot below
  the end with no vote is a hole that no ballot can write, because the old grid is sealed: the
  replicas stop at it for ever, so retire, offload and trim never come.
- A later Phase 1 reads a sealed configuration only below its end.
- A leader's writes are fenced by its promise quorum, never by the table version, so a change of
  a sealed segment (retire, offload, trim) never stops the tail (as in Delos).
- A replica drops its copy of a segment only after the record says offloaded. The object key is
  the journal and the segment, so the record stores no location. **"Says offloaded" means a
  write of the mark that is done at a matchmaker majority** (#1275 (segment records across a
  matchmaker handover), checked by `specs/tla/segment-records/`): the offloader's own
  compare-and-set, after which it tells the replicas, or the replica's own compare-and-set of the
  same value. A read that shows the mark is not enough: the mark can be on a minority only, and
  another majority, or the successor of a matchmaker handover, says retired (control
  `drop_read`). Each step that acts on a record (the retire of the old acceptors, a drop) acts on
  a write that it saw done, never on a read alone.
- **The records cross a handover of the matchmaker set** (#1275, `specs/tla/segment-records/`).
  The records and the matchmaker record are one compare-and-set register for each journal, fenced
  by the matchmaker generation. The handover's successor takes the highest write of the stop
  quorum and the highest read generation, as for the configuration register (`handover/`), and
  a frozen matchmaker takes nothing. So each record write that a majority took is in the
  successor, no record moves back, the seal end is written once, a compare-and-set on the old
  set after the stop fails, and a deposed controller's reshape fails across the handover too.
  Controls: a successor that copies one member of the stop quorum (`close_one`,
  `close_one_term`), a frozen member that takes writes (`frozen_serves`).
- **Offload is prepared, not built, in v1** (Pierre, 2026-11-20). v1 keeps the offloaded state in
  the segment record, the read path that checks it, and the record rules of #1256 and #1276
  (user truncate across segments). The offloader role, the object store code and the scrub of the
  objects come later. The rules below are the design for that day.
- **The offloader** (a stateless role, Pierre 2026-11-20; checked by `specs/tla/offload/`, #1256
  (offload races and the offloader role)) offloads a retired segment in four steps: it reads the
  segment from a replica that applied every slot up to the `next_seq` of the record, builds the
  object with deterministic bytes, writes it with a conditional put at the key, and marks
  `offloaded` by compare-and-set from "not offloaded and not trimmed". The races of two
  offloaders, a lost mark, an offloader that stops between the put and the mark, and a trim that
  comes during the upload need these rules, each with a control that TLC breaks:
  - A put that finds an object is not a failure: the offloader reads the object and marks when it
    holds the whole segment, whatever the release in its header. Giving up never marks after a
    lost mark; comparing bytes never marks across a rolling upgrade.
  - A failed mark deletes the key only when the record says trimmed (the late upload after
    trim). A delete on any failed mark deletes the object that the other offloader marked.
  - The trim deletes the object by its key, marked or not, and the controller scrubs: it lists
    the store and deletes each object of a segment below the trim floor. The scrub never deletes
    an object of a segment that is not trimmed, even with no mark: that object can be between
    its put and its mark. A tombstone at trim was checked too; it also needs a list (a
    controller can stop between the trim and the tombstone) and stays for good, so the design
    takes the scrub.
  - Offload does not need the retire: one complete copy and `F + 1` holders until the mark are
    enough. The controller still offloads retired segments only.
- **Each segment has its own replica set, and a write reaches only that set** (#1271 (a write
  reaches only the replica set of its segment), checked by `specs/tla/segment-replicas/`). Over
  the whole journal the replicas therefore do not bound write throughput, as they do in
  Compartmentalized Paxos (every replica executes every write, sections 3.3 and 8.5); inside one
  segment they still do. Because the journal state is folded at apply (sections 2.1 and 2.2), a
  replica of the next segment starts from the **boundary state**, the journal state at the seal:
  `next_seq`, the trim floor `first_seq` and the leader uuid. **The whole boundary state is in
  the segment record** (#1303 (two seals in a row), checked by `specs/tla/seal-chain/`; Pierre
  chose on 2026-11-20 that TLA+ decides where the uuid lives). The first replica of the old set
  that applied every slot of the segment writes it once, by compare-and-set at the matchmakers,
  into the record of that segment. A replica of the next segment starts from the record of the
  segment just before it, never from the newest record written (control `record_latest`: a
  deposed writer writes). An empty segment, sealed before its first slot, gets the record of its
  predecessor again, and a `Noop` at any slot changes nothing, so no slot of the next segment is
  needed for the boundary. `next_seq` and `first_seq` never move back across a chain of seals.
  The first form of the rule put the uuid in the journal's matchmaker record and a `Fence(uuid)`
  at the first slot of the next segment (#1271 (a write reaches only the replica set of its
  segment), `specs/tla/segment-replicas/`, `meta`). Through two seals in a row it is also safe,
  but it needs two more rules, each with a control where a deposed writer writes: the leader
  reads the uuid in the same read as the record of the segment before (`meta_any`), and the seal
  of a segment with no `Fence` leaves the field as it is (`meta_blank`). It also needs the
  `Fence` slot and the refusal of every command before it when the gap fill takes that slot
  (#1302 (the Fence slot against the Noop gap fill)). The segment record needs none of these. A
  record with `next_seq` and no uuid lets a writer that a `SetLeader` of the old segment deposed
  write again. A reader routes a read by the `seq` range of the segment records. A segment
  retires after `F + 1` replicas of its set hold it and its boundary state is written; after
  that the next segment never needs its slots.

**A leaderless read of the tail across a reshape** (#1254 (leaderless reads across a reshape),
checked on 2026-11-20 in `specs/tla/experiments/segments/reads/`). A reader that holds the record
of an old segment asks the old grid. With no fence, its answers need not see a slot that the new
grid chose, and the read is stale (controls `part_bare` on the partitioned grid, `bare` on the
full grid). The fence of the code closes it: each `PreReadAck` carries the read fence of the
acceptor (`wire_read_fence`: the higher of its promise and its belief ballot), and an answer above
the record ends the read. On the partitioned grid (the default grid, section 2.11) two Phase-1
quorums meet, so the fence alone passes (`part_fence`); on a full grid it also needs the seal of
each row (decision 31, section 2.5, #1209; `fence_rows`). The record check that the journal design
proposed (the reader also reads the segment table at a matchmaker quorum, rule 1 of #1254) adds
nothing while each acceptor keeps its promise. It adds safety only when an acceptor of a retired
segment loses its state and answers as an empty store (`part_wipe_fence` and `wipe_fence_rows` fail
with the promise alone); the belief half of the fence, with the refusal of an acceptor that did
not hear what is in force, closes that case too (`part_wipe_belief`, `wipe_belief` pass).
**Decided** (Pierre, 2026-11-20, #1254): the journal keeps the fence of the code, both halves
(the promise and the belief ballot), with the refusal of an acceptor that did not hear what is in
force and, on a full grid, the seal of each row. A read does not read the segment table at a
matchmaker quorum. The journal state model (#1189) carries this rule. Across a Phase-1 bypass
(#579) the promise fence alone is not enough, because the old acceptors never see the new ballot
(#1258 (reshapes with a column below quorum), PR #1283). Since a read has no record check, the
bypass closes that case with its own hold, section 2.13: the bypassing leader sends no `Accept` at its new ballot until its promises meet every read quorum of the old grid.

The rejected design is in section 11 (alternatives). The code follows once the journal state
model (#1189) includes these rules.

### 2.11 Quorum everywhere

**Every step of the journal waits for a quorum only** (decided by Pierre on 2026-11-20, #1257
(quorum everywhere), checked by `specs/tla/quorum-everywhere/`). No step waits for every member
of a group, as a FoundationDB commit waits for every TLog. With `F` acceptors down in every column
of the old and the new grid, `F` matchmakers down in each matchmaker set and `F` replicas down in
a segment's replica set, each step completes:

| Step | It waits for |
|---|---|
| Write, and the decided `Truncate` | `F + 1` acceptors of one column; a stuck round moves to the next column (#533) |
| Election | a matchmaker majority, then `F + 1` acceptors of every column of each configuration of `H_b` |
| Leaderless read | `F + 1` acceptors of every column of the configuration in force; no matchmaker read: the fence of the code closes a reshape (#1254, decided in section 2.10) |
| Seal | a Phase-1 quorum of the old grid, then a matchmaker majority for the compare-and-set of the end slot |
| Retire | `F + 1` replicas of the segment, then a matchmaker majority |
| Offload mark, record of a trim | a matchmaker majority |
| Matchmaker set handover | a majority of the old set (the stop quorum), then a majority of the new set (the successor decree) |
| Grid controller election, reshape call | a matchmaker majority for each compare-and-set on the matchmaker record (section 2.12, #1259) |
| Hold before Phase 2 of a bypass | `F + 1` acceptors of one column of the old grid; a stuck column moves (section 2.13, #1258) |

Each step blocks with one member more down than its quorum allows, and each step that waits for
every target blocks with `F` down: the model sees both. On a grid that is not full the leaderless
read sends its `PreRead` to every member, but it completes on a Phase-1 quorum of the answers
(`QuorumRead`, `has_phase1_quorum_in`): it sends to all and waits for a quorum, so it keeps its
targets.

A grid layout meets the rule for `F` exactly when `F + 1 <= acks <= rows - F`
(`grid/GridCheck.tla`, `PerColumnTolerance`). The partitioned grid of section 3.4
(`rows = 2F + 1`, `acks = F + 1`) and the majority meet it. The full grid (`acks = rows`) does not:
a write needs a whole column. The journal therefore offers only the majority and the partitioned
grid, and a grid that the grid controller picks keeps `F + 1 <= acks <= rows - F`.

### 2.12 The grid controller's fenced reshape

**The grid controller is its own role in each journal, elected by a term in the journal's
matchmaker record** (decided by Pierre on 2026-11-20, #1259 (the fenced reshape call of the grid
controller)). It runs on stateless machines; the Paxos leader does not run it. Its only effect on
the journal is one compare-and-set on the matchmaker record:

    reshape(expected configuration, new grid, controller term)

The controller decides when and where; the record's guards decide whether it is safe. So the
record trusts nothing that the controller computed. `specs/tla/grid-controller/` checks the
election and the call with two controllers, a candidate race, a deposed controller that keeps
calling, a crash between the plan and the call, a takeover after a crash, a lost request or
reply, and an acceptor that fails after a plan named it. Each guard has a control that TLC
breaks:

- **The election raises the term.** A candidate reads the term, then writes term + 1 by
  compare-and-set on the term it read; of two candidates that read the same term, one loses.
  An election that keeps the term gives two controllers the same term, and both open segments.
- **The call carries the term, and the record refuses any other term.** A deposed controller
  that keeps calling changes nothing, even when its call names the configuration in force. A
  refusal for the term tells it to stop.
- **The expected configuration is the newest configuration of the table.** With this check, at
  most one segment opens from one configuration, with or without the term check. A segment opens
  twice from one configuration only when both checks are off: a deposed controller's call lands
  after the new holder's.
- **No reshape while a segment is not retired.** The newest configuration must be in force and
  every older one retired, so one configuration is in force and at most one other is not retired
  (rule 1 of the plan). A reshape asked while a segment is sealed but not retired is refused.
- **The record checks `F + 1 <= acks <= rows - F` itself** (section 2.11). A grid outside the
  bound can open and then never come into force: with `acks = 1` and one acceptor down, Phase 1
  needs every member, the new configuration stays pending, and the guard above blocks every later
  reshape, so the journal can never heal.
- **The record answers a retry with the same answer** (a rule that the model found, decided by
  Pierre on 2026-11-20). The record keeps the identity of the call that opened each segment (the
  controller term, the expected configuration and the grid), and answers a call that it already
  took with "ok". Without it, a lost "ok" makes the retry fail its expected-configuration check,
  and the controller is told "refused" for a reshape that took place.

After the faults stop, every call is answered (done or refused) and every configuration that a
reshape opened comes into force. The controller's crate (`paros-journal-control`) and its
consensus-sim flavor (#1261 (grid controller decision tests)) come after the journal state model
(#1189); the flavor's walk then trace-checks against this model.

### 2.13 A column below quorum

**Not in v1** (Pierre, 2026-11-20, from the first review of the journal plan): a column below
quorum is `F + 1` losses in one column, which is outside the promise of section 2.11. In v1 the
journal holds and alerts. The Phase-1 bypass (#579) is not built in v1, so the hold before Phase 2
below and the read rule of #1270 (bypass read order) do not apply to v1. They stay here as the
rules for the day #579 comes back.

**A column is below quorum** when fewer than `F + 1` of its acceptors are live (the grid of
section 2.11: `rows = 2F + 1`, `acks = F + 1`). Writes go on in the other columns (#533 (move a
stuck round)), but no Phase-1 quorum of the grid exists, so no new leader can be elected and no
reshape can run a full Phase 1 over the old grid. #1258 (reshape with a column below quorum)
checked with `specs/tla/degraded-reshape/` whether the live leader can reshape away with the
Phase-1 bypass of #579 (zero-stall reconfiguration, `specs/tla/zero-stall/`):

- **With the decision hold of #579, no.** The bypass moves the leader to the new grid with no
  Phase 1, and its writes are chosen there. But the hold acks no slot before a Phase-1 quorum of
  the old grid promised the new ballot, and GC waits for the same. That quorum does not exist, so
  no slot is acked and the old grid never retires.
- **The decision hold also does not order two reads**: #1270 (a read at the bypass ballot), and a
  read on the old basis that saw the old vote of one acceptor for a slot that the new grid chose,
  followed by a read on the old basis at a quorum without that acceptor. Gating the read basis
  closes the first, not the second.
- **The rule: a hold before Phase 2 on a blocking set** (decided by Pierre on 2026-11-20, #1258).
  The bypassing leader sends no `Accept` at its new ballot until the acceptors that promised it
  meet every read quorum of each prior configuration. On this grid that is `F + 1` acceptors of
  one old column, which a column below quorum leaves. Its acks and its GC wait for the same
  promises, not for a Phase-1 quorum of the old grid. The fence of the code (section 2.10, the
  promise in each `PreReadAck`) then orders every read, with no matchmaker read on the read path,
  which matches the choice of section 2.10. The bypass waits one round trip to one old column, not
  for a full Phase 1. Checked by `block_propose`, `block_propose_maj` and `live_block_propose`;
  with one promise less, TLC finds a stale read (`block_propose_short`). The segment-record check
  on each read, with no hold, also passes (`safe_record`, `live_record`) and is the rejected
  alternative.
- **The rule needs a live leader.** If the leader is lost while a column is below quorum, no
  election can run, and writes stop until a member of that column comes back: the grid
  controller holds and alerts (section 2.12).
- **The seal end of a bypass is the bypassing leader's allocator frontier**: one above the highest
  slot that it proposed at its old ballot. A bypass runs no Phase 1, so the end of section 2.10
  (one above the highest vote its Phase 1 saw) is replaced by the frontier, which is never lower,
  because a leader bypasses only after it proposed every slot that its own election recovered. An
  end at the chosen prefix chooses two values.
- **A heal that moves survivors to other columns is safe**: the layout is row-major over the
  sorted members (`grid/Grid.tla`), and a move between two partitioned grids where every survivor
  changes column keeps one value (`config-register/heal_partitioned`); with Phase 1 one acceptor
  short in every column, TLC finds two values (`heal_partitioned_short`).

### 2.15 The v1 scope of the journal

Pierre's answers to the first review of the journal plan (2026-11-20,
`/mnt/project-files/review/journal-plan-2026-11-20/fable-review-1.md`):

- **A column below quorum is not in the v1 promise**: the journal holds and alerts (section 2.13).
  The zero-stall bypass (#579) is out of v1.
- **The elected controller stays**: its own role, elected by a term in the matchmaker record
  (section 2.12).
- **Offload is prepared, not built** (section 2.10). This replaces "offload retired segments" in
  the controller's first scope: the controller heals, rolls and scrubs.
- **The v1 controller has no SLOs and no scaling from latency or load.** It acts only on failures,
  from protocol facts: heal, roll and scrub. The roles publish their metrics to the controller;
  which metric drives which action is decided after tests (#1260 (health measures)).
- **The boundary uuid lives in the segment record**, the result of #1303 (two seals in a row,
  section 2.10).
- **A write stall at each roll is accepted in v1, and measured.**
- **The journal gets its own walk crate**: a `journal-sim` beside `consensus-sim`. `consensus-sim`
  stays on `paros-consensus` only.
