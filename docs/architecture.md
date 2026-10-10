# paros: the journal service

This is the end goal. AGENTS.md describes what paros is today and the doctrine every change
follows; this document describes what paros is becoming, so that every issue, plan and session
aims at the same target. Where the two disagree, AGENTS.md is the present and this is the
direction. Decided on 2026-09-30; the universe, the control hierarchy, identifiers, checkpoints,
recovery and the fenced `Truncate` decided on 2026-10-02; the leader-uuid API with its two
journal modes, election over a journal, the request channel, liveness, names, trust, capacity as
role slots, journals born with their matchmaker set, tenant modes, the data-plane limits, storage
chaos in simulation and the deferral of recovery decided on 2026-10-04; the resolver and the
frontend, zones inside a cell and multi-region cells decided on 2026-10-07; bootstrap by admin
calls, `cell init` as a single-decree Paxos, the election journal on the founding members and the
tenant control journal's fields decided on 2026-10-09. The milestones at the end carry the issue
numbers.

## 1. Goal

`parosd`: one binary on N machines, started with `docker compose`, serving journals to tenants
over a four-call data plane, healing itself through reconfiguration when a disk or a machine is
lost. One region first, several failure domains; multi-region cells are the last item of M12
(decided on 2026-10-07, #253; folded into M12 on 2026-10-09).

**First-class citizen: paros eats its own food** (decided on 2026-10-09). Journals, leader election and
the Paxos flavors are first-class citizens of paros itself, not only products it offers: every
hard problem paros has is solved with them, never with a side mechanism: state lives in **journals**,
who acts is decided by **leader election** over a journal, and agreement comes from one of the
**Paxos flavors** already in `paros-core`: Multi-Paxos for logs, single-decree Paxos for one-shot
decisions (the matchmaker handover's decree, the cell plan at `cell init`), Matchmaker Paxos for
reconfiguration, Compartmentalized Paxos for scale. A new control-plane need is first asked
"which journal, which election, which flavor?" before anything new is built; a cluster file, a
lock service, gossip or a hand-rolled agreement protocol is the wrong answer.

The control plane is itself stored in journals and coordinated through the same election
primitive the tenants use. It has four levels, all built the same way:
one actor elected over an election journal and installed as its control journal's leader with
`SetLeader`, holding no state that journal does not hold (section 3.3).

| Level | Elected actor | Its control journal holds |
|---|---|---|
| Universe | the universe coordinator (the universe tenant's coordinator) | tenant → cell, cell entries |
| Cell | the cell coordinator (the cell tenant's coordinator) | machine registry, capacity bookings |
| Tenant | the tenant coordinator | its name and desired state, its `survives`, the name and kind of its cell, journal names, placement inside capacity granted by the cell |
| Journal | the client leader (single-writer) or any writer (multi-writer) | the data |

A paros deployment is always a universe, and the universe runs from M9. For now it has exactly one
cell, and that cell plays both roles: it hosts the universe tenant (the universe level) and it is an
ordinary cell holding tenants. Every universe behaviour exists and is exercised from M9: `init`
creates the universe, tenants are created through the universe tenant, routing resolves tenant → cell through
the universe directory, registrations are verified on both sides. The answer is always "this cell"
today, but the code path is real and runs in every simulation. A second cell, moves between
cells and a separate resolver are M12 (section 3.7). The test for every design until then: adding
a second cell adds an entry to the universe directory and a routing choice, never a protocol or
data-model change. That is why M9 already carries each cell entry's `kind` and entry endpoint
and each tenant's `survives` (section 3.7): a universe that mixes regional and multi-region cells
adds entries, never fields.

**Every tenant can be transferred** (decided on 2026-10-04): the design must be able to move any
tenant to another cell, the universe tenant and its universe coordinator included, with no protocol or
data-model change and without stopping the tenants that do not move. Nothing may assume a tenant
stays in the cell it was born in. The reason is **rolling out a cell by evacuation**: stand up a
new cell, move every movable tenant onto it, then retire the old cell. The one tenant that never
leaves its cell is a cell's own cell tenant, because it *is* that cell (its registry and its
bookings): served from another cell it would make the cell depend on a foreign control plane,
break static stability and cross the blast-radius boundary, and after a rollout it would describe
machines that no longer exist. It is still reconfigured within its own cell (a dead founding member replaced,
a machine drained), and it retires with its cell; the new cell has its own from its `cell init`. The
moves themselves are M12; M9 carries what they need (section 3.7).

Compaction and snapshots are the user's business: paros owns the log, never the state.
`paros-core` never compacts, snapshots or verifies anything. The control plane is a tenant and
obeys the same rule: it compacts its own control journals the way any user would, using only
`Write` and `Truncate` (section 3.9). That is the dogfooding rule: the control plane gets no call
a tenant does not have.

The model is the AWS Journal, the replicated log behind Aurora DSQL, MemoryDB and Lambda, as
described in the public sources listed in section 10: a durable, ordered, fenced log that stores
decided outcomes, that a single leader appends to under its leader uuid (or that many writers
append to, unfenced), and that every consumer tails without a second protocol.

The first deliverable is a toy: a local Docker Compose toy cell an operator can initialize, create
a tenant on, write to, read from, break and watch heal. The homelab is not in scope, and one
region comes first: multi-region cells are the last item of M12 (decided on 2026-10-07, #253;
folded into M12 on 2026-10-09).

## 2. The data plane

Every journal exposes four calls, in one of two **modes** fixed when the journal is created
(decided on 2026-10-04): **single-writer**, where one leader appends under its leader uuid, and
**multi-writer**, where anyone with access appends. Every rule below is judged at apply time, in
slot order, by a small per-journal state machine
`(mode, leader_uuid, term, next_seq, first_seq)` that lives in `paros-core` beside the replica's
walk.

| Call | Single-writer | Multi-writer |
|---|---|---|
| `Write` | `Write(leader_uuid, expected_seq, batch) -> seq`: fenced by the leader uuid, contiguous by `expected_seq`, idempotent on retry, pipelineable. | `Write(batch) -> seq`: unfenced; the journal orders writes and assigns `seq` at apply. At-least-once on an ambiguous retry. |
| `Read(from_seq, limit, wait_ms?)` | The committed records from `from_seq`, plus `first_seq`, `next_seq` and the current leader uuid, or `Truncated` when `from_seq < first_seq`. `wait_ms` is capped and floored (section 2.7); a read at the tail waits up to it. | The same. |
| `Truncate` | `Truncate(leader_uuid, up_to_seq)`: fenced like `Write`. | `Truncate(up_to_seq)`: anyone may truncate. |
| `SetLeader(new_uuid, old_uuid)` | Compare-and-set the leader. Returns the journal's view after it: the leader uuid, `next_seq`, `first_seq`. | Refused: a multi-writer journal has no leader. |

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
is no per-client session ledger and nothing to expire. One slot holds one single-writer `Write`;
a single-writer journal batches in its client (`paros::client::Writer`), never in a batcher.

A leader may refuse a `Write` from its own fold at propose time, so a superseded leader's
pipelined burst does not burn slots. That is an optimisation; the apply-time check is the safety.

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
Who becomes leader is decided outside the journal, by an election (section 3.3); the journal
gives safety to writers, the election gives liveness (Brooker).

Two fences, two layers, never confused: the Paxos ballot says which *machine* runs a journal's
consensus leader and is invisible to clients; the leader uuid says which *client* may write and
is invisible to Paxos.

### 2.4 Multi-writer journals

A multi-writer journal has no leader: `Write(batch)` is accepted whenever its batch is within
the limits (section 2.7), and the journal assigns its `seq` at apply. There is no expected
sequence and no deduplication: an ambiguous write retried may land twice, and the writers own
that (at-least-once). Anyone with access to the journal may `Truncate` it; `first_seq` stays
monotone. A multi-writer journal is what many independent producers append to, and what an
election runs over (section 3.3). Batchers (section 4) merge multi-writer writes into fewer
slots; the unbatcher hands each write its own `seq` range.

How it is built (#241, part 3). The mode is a field of the journal's `Config`, so the format
marker records it and a restart cannot change it. `CreateJournal` carries it, and a journal
without one is single-writer. On the wire a multi-writer call uses the same messages: a
`Write` or `Truncate` carries the unset leader uuid (`0`) and `expected_seq` 0, and the core
ignores that `seq`. The mode is judged at apply, like every other rule. A call shaped for the
other mode is refused in place with `WRONG_MODE` and the journal's view: an unfenced call on
a single-writer journal, and a fenced call or a `SetLeader` on a multi-writer one.
`paros::client::multi` builds the unfenced requests, and `parosctl write --multi` and
`parosctl truncate --multi` send them. `parosctl journal create --mode` comes with the
journal-create request to the tenant coordinator (#210, decided on 2026-10-09), not with #241. The simulation draws the mode per journal, judges every
attempt against the same model in that mode, and a client sends the wrong-mode calls on
purpose.

### 2.5 Reads

`Read` is served by whatever holds the journal's replica state — the colocated node by default,
the replica tier when the tenant asks for replicas — and always through the leaderless read of
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

A read is judged over the configuration of a leadership that **won** its ballot, never over a
campaign's (decided on 2026-10-07, #260, "option A"). A node believes a campaign's configuration
as soon as it promises its `Prepare`, but a campaign may never finish, and the older
configurations it must cover can still hold slots its own quorums never voted: the witness was a
leader under `{0,1,3}` with `q2 = 1` that chose a `SetLeader` with its own vote, campaigned with
`{0,2,3}` and died, and an acceptor that promised the campaign served generation 0 for a second
from a majority of the new configuration. So every server keeps a **read basis**: the
configuration, the ballot and the **fence** (the highest slot the winning Phase 1 could have found
chosen) of the last won leadership it heard, from its own election or handoff or from the leader's
heartbeat, which now carries the fence on matchmaker deployments. A read is served once the
server's fold covers both the row's maximum watermark and the fence. A server whose own belief
moved above its basis (it promised a newer campaign) and a server that has heard no leader since it
booted open no read, and the client is answered unserved; a rebooted acceptor answers no watermark
query until it has heard what configuration is in force. Reads are unavailable during a campaign,
never stale. Plain Multi-Paxos is unchanged: its basis is its static configuration with no fence.
The alternatives were a basis that advances only once garbage collection of the older
configurations is effective (longer unavailability) and asking the matchmakers for the whole
history on every read (a round trip per page).

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

The read limits are `DriverTunables::max_read_records` (256 by default, floor 1: a `limit` of 0
or above it is cut to it), `max_read_bytes` (64 KiB, floor 1: a page always holds one record,
whatever its size), `max_wait_ms` (400 ms in the simulation's baseline, 1 s in `parosd`, floor 0)
and `min_wait_ms` (0, floor 0). They are `PAROS_*` overrides in `parosd`. The node applies them
when it parks the read (`paros::driver::log_reads::ReadLimits`); a page shorter than the
client's `limit` is the server's limit, and the client reads on from the page's end.

### 2.8 Underneath

Paxos is unchanged: replication, holes, gap fills, the contiguous chosen prefix, CTRL, the trim
point, matchmaker reconfiguration. Every proof paros has holds per journal; the one new thing to
prove is the control state machine's rules, and they are judged in the simulation like every
other rule.

## 3. The control plane

### 3.1 The cell tenant and bootstrap

A cell's machines and capacity are the control journal of the **cell tenant**, served by the
same `parosd`s, the same Paxos and the same stores as every tenant's journals. Its coordinator is
the **cell coordinator**. The control state has four levels: the machine registry and the
capacity bookings are the cell's control journal; the universe directory maps tenant → cell (the
universe level); each tenant's own control journal holds its description and its journals
(#210, landed); desired state lives in the tenant control journals. System journals are written with `Write` like any
journal; there is no special path.

Every tenant gets a control journal when it is created. It is **self-describing**: it holds the
tenant's name and desired state, its `survives` (section 3.4), the name and kind of the cell that
hosts it (section 3.7), its journal names and its placement (decided on 2026-10-09, #210). It
holds no rendezvous name. Every index above it
(the cell's list of hosted tenants, the universe directory) can be rebuilt from it (section 3.3).

**Bootstrap: idle until an admin admits it** (decided on 2026-10-09, #216, PR #280, replacing the
2026-10-02 "start and wait with a join list, then `init` to a seed"). There are no seeds, no join
list and no rendezvous name. The same rule holds at two levels:

- **An idle machine** is formatted (its `node_id` minted, its machine record written) and waits,
  serving only `Identify`. It joins a cell only when an admin call admits it. Its configuration
  names no cell and no peer.
- **An idle cell** is formed and serves its own control journal, but hosts no tenant. It joins a
  universe only when an admin call admits it.

| Admin call | Sent to | What it does |
|---|---|---|
| `parosctl cell init --members a,b,c [--name]` | the listed idle machines | forms an idle cell on them |
| `parosctl cell add-machine <addr>` | any member of the cell | the cell coordinator admits an idle machine |
| `parosctl universe init --cell <addr> [--name]` | any member of an idle cell | creates the universe tenant there and admits that cell as the universe's first |
| `parosctl universe add-cell <addr>` | the universe, and any member of the idle cell | admits another idle cell (M12) |

`parosctl init` stays as a shortcut for `cell init` followed by `universe init` (the toy).

- **`cell init` is a single-decree Paxos on the cell plan** (decided on 2026-10-09, #277). The
  machine that receives it drives it, with every listed machine as an acceptor. The quorums are
  `q1 = n` and `q2 = majority` (decided on 2026-10-09, #246, PR #323): every listed machine must
  answer the ask, so every vote still on a disk is heard and adopted, and a majority of accepts
  chooses the plan, so a wiped founding member does not block `init`:
  1. **Ask, as a reservation.** It sends a fresh random `init_id` (its ballot) to every listed
     machine, which answers like `Identify`: it must be `storage`. It also promises to accept no
     plan under a lower `init_id`, and answers with any plan it has already accepted. A formed
     machine answers every ask with its accepted plan and never accepts another one. A listed
     address that now hosts another machine than the accepted plan names is a wiped member.
     The new machine never accepts the plan as the old one (`not_a_member`), so it never votes
     with the old one's lost promises. The other members choose the plan while they are a
     majority, and the old id stays a dead member of the cell until a reconfiguration replaces
     it. When a majority of the plan's members are wiped, no plan can be chosen, and the
     receiver refuses (`cell_lost`): the Paxos limit.
  2. **Adopt or draw.** If an answer carries an accepted plan, the receiver must finish that plan
     instead of its own: with the same member list, the two `init`s converge on one cell; with
     another list, it refuses (`other_cell_init`). Otherwise it draws the plan: `cell_id` and the
     cell tenant's `JournalIdentifier` (its random `TenantId` and the random `JournalId` of its
     control journal, section 3.8).
  3. **Form.** It sends `FormCell(plan, init_id)`; each machine accepts it unless it has promised
     a higher `init_id`, and is then formed. A majority of accepts chooses the plan. The receiver
     sends its own accept last, and only when the others can still choose the plan with it. A
     machine that receives a `FormCell` without being asked first treats it as both steps.

  A receiver that crashes midway leaves no lock: the next `cell init`, sent to any listed machine,
  finds the accepted plan in its ask and finishes it. A plain question ("is anything ongoing?")
  would not do, because two receivers could both hear "no" and both go ahead. The decree is
  paros-core's single-decree `Proposer` and `Acceptor`, as the matchmaker handover already uses
  them at slot zero. FDB reaches the same end differently: its cluster file names coordinators
  that elect a controller before any database exists, the controller's provisional proxy takes
  the first `configure new` transaction, and each client reads `\xff/init_id` back to learn
  whether it won. paros has nothing elected before `cell init`, so the decree is that election.

  The listed machines are the cell's **founding members**. `cell init` creates on them the cell
  control journal and the cell's first election journal, both born `double` with the cell's first
  matchmaker set (decided on 2026-10-09, #240). The founding members campaign over that election
  journal for the first cell coordinator, and the winner installs itself as the control journal's
  leader with `SetLeader(its fresh uuid, unset)`. The founding list lives only in the cell
  plan; it is never a role a machine keeps. After formation the membership changes only by
  reconfiguration.
- **`add-machine`** goes to any member, which routes it to the cell coordinator as a request to a
  leader (section 3.3). The coordinator reaches the idle machine at the address the operator
  gives and asks it to `Identify` itself. It writes its `RegisterNode` into the cell control
  journal, with the machine's advertised address (section 3.2, #257), then sends it `Admit` with the `cell_id`, the
  control `JournalIdentifier`s and a registry snapshot, which the machine caches durably. An
  interrupted admission is finished by the coordinator like any in-flight entry (section 3.7).
  *Landed* (#216, 2026-10-09): until the cell coordinator exists (#225, #240), the admin's own
  call takes these steps, as `paros::client::cell::CellSession`, an operation state machine like
  the fleet operations: it claims the cell control journal with `SetLeader`, writes
  `RegisterNode` unless the registry holds the machine, then sends `Admit`. Each step is decided
  from what the journal and the machine hold, so a re-run resumes. The snapshot is the founding
  members and every registered machine, by id and address. An admitted machine serves `Identify`
  and a node-only `Inspect` with its cell, and no journal until placement (#212). A machine that
  promised in a `cell init` and has no vote refuses `Admit` (`in_cell_init`): that `cell init`
  can still form a cell over it, and a stalled `cell init` is safer than a machine in two cells.
  An admitted machine refuses every `cell init` that lists it (`cell_exists`).
- **`universe init`** creates the universe tenant inside that cell (the universe tenant is a
  tenant, so the cell must exist first to grant it capacity), mints `universe_id` and the universe
  tenant's `JournalIdentifier`, both random, recorded in the cell plan of the admitted cell
  (`universe add-cell` records the same two in each cell it admits later), then registers the cell as the first entry in the universe directory and
  writes the matching registration on the cell side (section 3.7).
- Each step is idempotent and is a step of an operation state machine (section 3.7): re-running a
  call after a crash resumes it, learning the ids back through `Inspect`.
- **No implicit formation.** A machine with an empty, uninitialized store waits indefinitely. It
  never forms a cell, joins one, or creates a universe on its own.
- **Trust.** An idle machine accepts the first `FormCell` or `Admit` that reaches it; under the
  network trust of section 3.5 that is the admin's. With Biscuit tokens (#245) both carry an admin
  token the machine checks, since otherwise another host could impersonate the cell (kubeadm pins
  the control plane's CA for this reason).

This is the mainstream pattern, checked on 2026-10-09: Kafka KRaft passes the founding voters
explicitly at format time (`--initial-controllers`, identical on every founder) and never
auto-formats a blank directory, since a majority starting empty could elect a leader missing
committed data; etcd forms from `--initial-cluster` plus a cluster token and adds later members
with `member add`; YugabyteDB adds a master to its universe with `change_master_config ADD_SERVER`;
kubeadm runs `init`, then `join` against any control-plane address. Redpanda recommends disabling
`empty_seed_starts_cluster` for the same reason paros has no implicit formation. Founding on one
machine and growing by reconfiguration (Kafka's `--standalone`) was rejected: two concurrent
`cell init`s sent to different machines would then form two cells, where the decree over the
whole founding list forms at most one.

**Naming** (decided on 2026-10-09). The top level is the **universe** (formerly the fleet; the
name Spanner and YugabyteDB use). "Seed" now only ever means a simulation seed, and the word
"cluster" is not used: a set of machines is a cell. Some code names still say `fleet` and `seeds`
(for example `paros::fleet` and `SystemPlan::seeds`); they go when the rename lands (#246's
follow-up).

**Every journal is born with its matchmaker set** (decided on 2026-10-04). The cell control
journal is born on the founding members with the matchmaker set `cell init` starts there, and the
universe tenant's control journal with its tenant's set; every tenant journal is born with its tenant's set. There is no
plain-to-matchmaker transition anywhere, so there is nothing to migrate. **Matchmakers are
mandatory everywhere in the service**, because reconfiguration needs them and reconfiguration is
how everything heals and moves: the cell's control
journals are reconfigured like any tenant's from their first entry, and healing works in
production from the first boot. The matchmaker-free plain deployment stays what AGENTS.md says it
is, a permanent library-level configuration; `parosd` never runs it.

### 3.2 Machines

Every `parosd` is uniform. **Identity** has three parts (decided on 2026-10-02, #225):

- `node_id`: random, minted at format, stored beside the format marker (#147). It is the
  member's identity and the registry key. A wiped disk gets a new `node_id`, so "a wiped
  identity never rejoins" holds by construction.
- `addr`: an attribute that may change across restarts, the machine's advertised address
  (`PAROS_ADVERTISE`, below).
- `incarnation`: moonpool-rpc's per-start `Incarnation`, carried in the `InterfaceRef`.

The same `node_id` with a new incarnation is a reboot: the machine rejoins in place and keeps its
assignments. It is re-placed only if it does not come back within a bound (a cell tunable, drawn
per seed in simulation), so a whole cell
restarting does not trigger a re-placement storm (CockroachDB saw the equivalent: after a mass
restart, lease renewals flooded the cluster until liveness heartbeats timed out). The same
`node_id` with a new `addr` is a machine that moved; ScyllaDB moved from IP-based to host-ID-based
identity for exactly this reason.

At start a machine registers in the cell control journal with
`RegisterNode { node_id, addr, interface, class, capacity, failure_domain }`, publishing its RPC
interface reference (moonpool-rpc's incarnation-bearing `InterfaceRef`), and it starts the driver
for every `(tenant, journal, role)` assigned to it. A `RegisterNode` for an existing `node_id`
with a new incarnation is the reboot signal: the cell coordinator learns at publication time, not
after a heartbeat window (Akka identifies nodes by `host:port:uid`, and a new incarnation joining
at the same address removes the old member). Peers holding the old reference learn on their next
request, refused with `StaleIncarnation`. That pull model is sufficient: Delos clients refresh
their cached view only when an append fails on a sealed loglet. No push detection is asked of
moonpool.

`StaleIncarnation` needs dynamic references (amended on 2026-10-10, #216). moonpool-rpc refuses a
stale reference only on a dynamic endpoint. A well-known endpoint answers every incarnation. Today
every paros method is well known, so no call can be refused as stale. A restart at the same
address rejoins in place, and that is the decided behaviour. A machine that moved is found
through the registry fold (#349, #211). So `StaleIncarnation` and the `InterfaceRef` in
`RegisterNode` move to #404 (static endpoints for bootstrap only), which makes the machine,
coordinator and data-plane interfaces dynamic. #216 does not add an application-level copy of
the check.

**Listen and advertised addresses** (decided on 2026-10-07, #257). A machine's configuration
carries two addresses, as FDB's `listen_address` / `public_address` and CockroachDB's
`--listen-addr` / `--advertise-addr` do: `PAROS_LISTEN`, what it binds (may be a wildcard), and
`PAROS_ADVERTISE`, what peers and clients dial (may be a hostname). `PAROS_ADVERTISE` defaults to
`PAROS_LISTEN` when that is not a wildcard, and start is refused when `PAROS_LISTEN` is a
wildcard and `PAROS_ADVERTISE` is unset. A machine always binds `PAROS_LISTEN`, never an address
read from the cell plan or the registry; `RegisterNode`'s `addr` is the advertised address, and
peer address books are folds of the registry (#216), never of the plan `init` wrote. An
advertised name is kept as a name and resolved at dial time, so a machine whose IP changes heals
with no registry write (amends #209's "names resolved once at startup" for peer addresses; the
listen side still resolves once). A changed `PAROS_ADVERTISE` across a restart is the
machine-moved case above: same `node_id`, new `addr`.

Landed on 2026-10-10 (#257). `paros::Address` is the advertised `HOST:PORT`, kept as written.
`paros::Names` is the one resolver all dialers of a machine share. A peer lane resolves its
peer's name before each batch and forgets the result after a failed delivery (FDB's
`removeCachedDNS`). The client and `parosctl` resolve per call. `MachineAddresses` refuses a
wildcard listen address with no advertised one, and a wildcard advertised one. A machine takes
part in `cell init` only when the member list names its own advertised address, so
`parosctl init --members` lists the advertised addresses. The simulation advertises a name on
half the seeds, and a rebooted machine on such a seed comes back at a new IP behind its name
(`move_pct`).

Landed on 2026-10-10 (#349, registry address books). A changed `PAROS_ADVERTISE` across a
restart is supported:

- **The registry holds every machine's address.** A founding member registers too: its
  `RegisterNode` replaces the address the cell plan names. It stays in the genesis pool and is
  never drained. `paros::machine::address_book` is the founding members at their registered
  addresses, else at the plan's; `cell_book` adds every registered machine not retired.
- **The request path.** On every start, a formed or admitted machine reads the election
  journal, finds the coordinator's published interface, and sends it `Register` (machine
  method `0x5041_0307`) with its identity and the address it advertises now. The coordinator
  calls `Identify` at that address. When the machine answers there as the same incarnation, and
  the cell's address book holds another address, the coordinator writes `RegisterNode`. A
  member that serves no term refuses (`not_coordinator`); the machine asks again after one
  renewal period. A restart at the same address writes nothing.
- **The peer book.** A founding member folds its own copy of the cell control journal after
  each tick. When the address book moves a peer, that peer's lane dials the new address from
  its next batch on. The coordinator's watch also dials each machine where the book says.
- **The limit.** Until the registry holds the new address, no peer can send to the moved
  machine. So the registry write needs a majority of the other members. A cell of two cannot
  heal a moved member, and neither can a cell that lost another member. A machine whose cached
  book names no reachable machine cannot find its coordinator: the same static-stability limit
  as a machine whose cached members are all gone (below). The durable cached registry fold
  (below) lets a machine that restarts after others moved dial them where they are.
- **The simulation.** On a seed whose machines advertise names, a cell machine's reboot comes
  back under a new name at a new IP (`rename_pct`). Its old name keeps the old IP, where
  nobody listens. The operators' entry for the machine follows it. After chaos, the oracle
  judges that the registry holds every renamed founding member at its new name.

**Liveness** (decided on 2026-10-04). The cell coordinator watches the cell's machines with the
transport's failure detector and writes only the *changes* into the cell control journal (`Down`,
`Up`), so control writes stay rare (section 3.9) and the registry's "seen alive" is a fold, not a
heartbeat log. A machine `Down` past the re-placement bound has its roles re-placed. A machine
never heartbeats into a journal.

Landed in #211: the cell coordinator's `Watch` (`paros::machine::coordinator`) sends `Identify` to
every founding member and every registered machine at each renewal period. The detector's timeout
is `machine_down_after` (a driver tunable, floor above `election_renew`): a machine silent that
long is written `MachineDown`, one held down or seen as another incarnation `MachineUp`, and a new
incarnation of a registered machine registers again. The registry refuses a liveness entry that
changes nothing (`LivenessUnchanged`), and the simulation asserts that none is ever written. The
watch writes only while its term's session holds the cell control journal; its first write that
does not land ends the watch for the term. `RegisterNode` and `IdentifyAck` carry the machine's RPC
incarnation: with the address, it is the machine's `InterfaceRef` identity, since a machine serves
well-known endpoints only. Capacity bookings are keyed `(node, journal or matchmaker set, role)`,
the role names the class, and a booking id is never booked twice, across checkpoints (the
registry keeps the spent ids). The re-placement bound and the re-placement itself are #212.

Landed in #211 (2026-10-10): the **durable cached registry fold**
(`paros::machine::CachedRegistry`). Every machine of a cell keeps the cell's address book
(`cell_book`) at one registry position in a file `registry` beside its record, rewritten whole
and atomically, as the record is:

- **Who writes it.** A founding member offers each book its own registry fold reaches
  (`driver/book.rs`). An admitted machine serves no journal, so it folds the registry through
  the machines it knows, each renewal period (`machine/follow.rs`). It learns the genesis pool
  from the control journal's membership. A writer task beside the machine writes a book only
  at a later position and only when the book changed. The node loop never waits on the disk.
- **Who reads it.** At every start, a formed or admitted machine dials the machines of its
  cell where the cache says, else where its plan or admission says (`starting_book`): its peer
  lanes, its control-journal seeds, its coordinator client and its registration (#349). A new
  incarnation folds the registry again from position 0. Its book moves no lane until the fold
  passes the cache's position: below it, the cache is the newer book.
- **A hint, never a fact.** The cache names its machine (`node`), and a cache another
  identity left, or one that does not parse, is ignored. A wrong address costs liveness, never
  safety: every peer batch names its cell (#216).
- **The simulation.** A durable cache only moves forward, and a boot reads only a cache its
  machine wrote. The gate is a boot that dials a moved machine from its cache, the
  static-stability case.

The same change fixed a restore bug from #349: a founding member that registered its address
broke every restore of a registry checkpoint, because the restore refused a genesis node in the
state.

It also fixed #390 (renamed founder never registers). A founding member that comes back at a
new address hears none of its peers: they send to the address the registry knows. Its election
clocks still fired, and each campaign raised its peers' promises above the sitting leader's. The
cell control journal then stalled, so the registration that would move the peers' lanes never
landed. Now such a member holds its journals' clocks until its registration ends
(`driver/mod.rs`). It still answers every message that reaches it. A lone founding member is its
own quorum, so it never holds.

**Finding the cell** (amended on 2026-10-09). There is no cluster file and no rendezvous name.
A machine's configuration names no cell and no peer: it learns its cell when it is admitted
(`FormCell` or `Admit`, section 3.1) and caches it durably. On every later start it finds the cell
from its **cached registry fold**. If every cached member is gone, the machine cannot find its
cell, and an admin admits it again (YugabyteDB makes the same trade: a tserver none of whose
configured masters remain cannot rejoin).

A client's only static input is an **entry endpoint**: a few addresses, or a DNS name the
operator puts in front of them, never stored by paros (as Kafka's `bootstrap.servers`). From
there it calls `Resolve` (below).

- The durable cached registry fold plays the role CockroachDB gives gossip (node addresses off the
  consensus path): it lets machines find each other while the registry is unavailable. It is a
  static-stability requirement, not an optimisation.
- **No journal is found by convention** (decided on 2026-10-04, section 3.8): there is no
  well-known tenant or journal id. Every machine of a formed cell answers `Inspect` (and
  `Resolve`) with its `cell_id`, the cell tenant's control `JournalIdentifier` and, once the cell is
  admitted, the universe tenant's `JournalIdentifier`, all read from its durable cell plan, plus the
  cell that hosts the universe tenant now, read from its universe pointer (section 3.7). A client or an operator handed
  only addresses learns the control `JournalIdentifier`s from any machine, then resolves everything else
  through them: the universe directory gives a tenant's cell and the `JournalIdentifier` of its control journal, and
  that control journal gives the tenant's journals. A re-run of an admin call learns the cell's
  ids the same way. **An `Inspect` names its journal or asks for the node alone** (decided on 2026-10-05,
  #243): a node-only `Inspect` answers the machine's own facts — its `node_id`, its `cell_id` and
  the control `JournalIdentifier`s — and nothing about any journal, which is how a client handed
  only addresses starts; an `Inspect` that names no journal without asking for the node alone is
  refused (`unset`), never read as "the node's first journal", and one naming a journal the
  machine does not serve is refused (`unknown_journal`). The machine's own facts ride every
  answer, refusals included.
- `cell_id` is carried in the session `Hello`; a peer with another id is refused.
  ScyllaDB carries its cluster id in gossip for the same reason: nodes from different clusters
  cannot talk after a bad configuration. The `Hello` is the peer lane's `Deliver` batch (decided
  on 2026-10-10, #216): every batch carries the sender's `cell_id`, and a receiver of another
  cell refuses the whole batch before it decodes a message. The simulation makes the shape: an operator founds another cell on the
  machine that replaced a wiped member, and the first cell still sends to that address.
  Discovery has the same hazard, so a re-run `init` learns the cell a majority of the founding
  members serve, and `cell init` never adopts a vote for another list's plan.
  **`universe_id` is not on the peer batch** (amended on 2026-10-10, #216). Peer traffic stays
  inside one cell, and a cell joins one universe once (`JoinFleet`), so two machines with the
  same `cell_id` always share a universe: a check on the batch could never fire. The
  `universe_id` rides the `Resolve` answer, and it joins the peer batch with the first call
  that crosses cells (M12). It is minted at `universe init`, after the cell plan, so the cell
  holds it in its registry fold (`JoinFleet`), not in the plan.
- **Well-known endpoints** are the bootstrap set — `Identify`, `FormCell`, `Admit`, `Inspect` —
  and **`Resolve`**, keyed by tenant: "which references serve tenant T" (named on 2026-10-09).
  Everything else is a dynamic reference (amended on 2026-10-04:
  the bootstrap calls were well known already). One call, two answerers (decided on 2026-10-07,
  #233): a resolver answers the cell half (the tenant's cell and that cell's entry references),
  any machine of that cell the frontend half (the tenant's frontends, from its registry fold),
  section 3.5. `Resolve` landed on 2026-10-10 (#216, `0x5041_030A`), section 3.5.

The decision and its alternatives are #216. Classes are FDB's:

- `storage`: anything with a durable store. Acceptors, replicas, matchmakers.
- `stateless`: frontends, resolvers, proxy leaders, batchers, unbatchers, coordinators. The cell
  and universe coordinators run on the founding members until a `stateless` machine registers
  (section 3.3).

**Capacity is role slots** (decided on 2026-10-04). A machine offers `capacity` opaque slots of
its class; one slot holds one role instance (an acceptor, replica, matchmaker, coordinator,
frontend, batcher or unbatcher) of one journal or tenant. A booking is keyed
`(tenant, journal or matchmaker set, role)` and holds one slot; only the cell coordinator writes
bookings. Bytes, IOPS and weighted roles are not modelled. Scaling a role for a tenant is adding
machines of the right class and raising the tenant's desired counts.

### 3.3 Coordinators and placement

The four levels of section 1 are built the same way (decided on 2026-10-02, #225): one actor per
level, the leader of that level's control journal, so it is fenced by its leader uuid like any
writer, and it holds no state that journal does not hold: a new one resumes from a fold. All four
exist from M9.

**Election over a journal** (decided on 2026-10-04, #240). Who leads is decided outside the
journal it governs, by one library, `paros::client::election` (decided on 2026-10-09), used by the cell, universe and tenant
coordinators and offered to customers. An election runs over a **multi-writer** journal
(section 2.4): candidates append campaigns, the leader renews by appending, and every watcher
folds the same deterministic rule. The renewals are a **lease used as a liveness hint only**:
a watcher deems the leader gone after it has seen no renewal for a bound measured on its own
clock, never by comparing clocks across machines, and campaigns. The winner draws a fresh leader
uuid and installs it with `SetLeader(new, old)` on the control journal it governs; that uuid is
the only fence (section 2.3), so two actors that both believe they lead can never both write. On
winning, an actor folds its journal to the tail before its first control write and finishes every
operation the journal holds in flight (`REGISTERING`, `REMOVING`); on its first refused write it
stops. Hand-off is `SetLeader(successor, me)`. An election journal is low-throughput, so it always uses
a redundancy mode (majority Multi-Paxos, section 3.4), never grid, and like every journal it
has its tenant's matchmaker set (decided on 2026-10-04).

**Where candidates run** (decided on 2026-10-04, amended on 2026-10-09, #240). The founding members
campaign for the first cell coordinator over the election journal `cell init` creates on them,
born `double` (section 3.1). The cell and universe coordinators run on the founding members until a
`stateless` machine registers, then move there; the cell coordinator places tenant coordinators on
`stateless` machines.

*Landed* (#240, 2026-10-10). The library is `paros::client::election`, and the cell
coordinator is `paros::machine::coordinator`:

- **The election journal.** The cell plan names it (`CellPlan::election`): a journal of the
  cell tenant, multi-writer, over the founding members. Machines learn it from the plan,
  `Inspect` and `Admit`, like the other control journals.
- **Records.** A record is a campaign, a renewal or a resignation, each with its term. The
  first campaign for the next term wins. A renewal counts only from the term's leader. A
  resignation can name a successor and the successor's uuid; the successor then leads the
  next term.
- **The lease.** A candidate campaigns when it saw no renewal for `lease` plus the
  caller's jitter, on its own clock. The library draws no randomness. A caller that campaigns
  earlier deposes a live leader: that costs availability, never safety.
- **Uuids.** A candidate derives a fresh uuid per term from the caller's seed. A
  restarted leader does not take back its old term: it waits a lease and takes the next term.
- **Log space.** Each renewal describes the whole leadership, so the leader truncates the
  election journal to its own latest renewal once `compact_after` records lie below it. A
  renewal is its own checkpoint. A reader that starts at the floor anchors on the leader's
  record there. Nothing else may truncate an election journal.
- **The interface.** Records carry the candidate's `InterfaceRef` (its address). The
  coordinator publishes it with its first renewal after its term's duties; until then the
  interface is empty.
- **The coordinator.** Every founding member runs a candidate in a task beside its node loop.
  When it wins a term, it installs the term's uuid on the cell control journal with
  `SetLeader(uuid, current)`, folds that journal to its tail, and admits again every registered
  machine that is not a founding member and not retired (`Admit` is idempotent). Then it
  publishes its interface. A lost install ends the term, and the coordinator resigns.
- **Interim.** The admin calls are not yet requests to the coordinator (#212, #225). An
  admin session still claims the cell control journal and fences the coordinator. The
  coordinator does not fight back within its term: it resigns, and the next term installs
  a fresh uuid (#349: a fenced term cannot register a moved machine). `parosctl init` claims
  nothing: it waits until the coordinator installed its uuid, then runs the fleet steps.
- **Knobs.** `DriverTunables::election_lease`, `election_renew` and `election_compact_after`,
  each with a floor; `parosd` reads them as `PAROS_ELECTION_*` variables.

**Requests to a leader** (decided on 2026-10-04). Every control journal has one writer, so
anyone else — a tenant coordinator asking for capacity, an operator changing desired state or
draining a machine, a user creating a journal — sends a **request RPC to the elected
coordinator**, found through the `InterfaceRef` the coordinator publishes in its journal when it
wins. A request carries an idempotency id; the coordinator records the outcome in its journal,
so a retry that crosses a coordinator change finds the answer there instead of acting twice.
The first one landed is a machine's `Register` (#349, section 3.2): its idempotency is the
registry itself, which holds the address once the request is done.

- **Single writer per journal.** Only the cell coordinator writes capacity. A tenant coordinator
  *asks* the cell coordinator for capacity and never writes capacity itself; it then computes
  placement deterministically, inside the capacity it was granted, from the registry, the failure
  domains and its desired state, and writes it as fenced entries into its own control journal.
  There is no rival write to resolve: separate journals have no order between them, and a
  single-writer journal makes the rival write impossible. Machines act on what they fold.
- **A parent places its children's actors.** The cell coordinator places the tenant coordinators,
  the universe tenant's included, and re-places a dead one; tenant coordinators place their roles.
  This is FDB's recruitment by process class, per tenant.
- **Static stability.** A child keeps serving while its parent is down. Only new capacity, new
  tenants and moves wait for the parent. In particular, existing tenants keep serving while the
  universe tenant is unavailable, and the simulation shows it with one cell.
- **Rebuild from below.** Every level's journal can be reconstructed from the level beneath it. A
  tenant's name and desired state live in its own control journal; the cell's list of hosted
  tenants and the universe directory are rebuildable indexes. This is the pattern of DSQL's adjudicator
  (section 2.3) one level up, and what makes recovery possible without Paxos surgery
  (section 3.10, deferred).
- **Tenant birth.** When the cell applies `HostTenant`, the cell coordinator books the tenant's
  footprint, places its matchmaker set and its coordinator, and the new tenant coordinator claims
  the tenant's control journal; nobody else ever writes it (decided on 2026-10-04).
- **Coordinators checkpoint their control journals** with the library of section 3.9, so a
  control journal's length is bounded by its live entities, never by its history.

`Reconfigure` is how a journal moves: off a drained machine, off a dead identity, onto a spare,
between quorum systems. The tenant coordinator sends it to the journal's own leader, and
retirements wait for the GC watermark (`may_retire`).

### 3.4 Tenant modes

A tenant's desired state is FDB's `configure` (decided on 2026-10-04): per journal or as a tenant
default, a **redundancy mode** — `single`, `double` or `triple`, a majority over one, three or
five acceptors — or the opt-in throughput mode **grid** `{rows, cols}`, plus per-role counts
(`frontends=`, `proxy_leaders=` and `batchers=` per tenant, `replicas=` per journal).

**`single` survives no zone** (decided on 2026-10-07, #215): a majority over one acceptor cannot
meet the zone rule of section 5. It stays a mode without zone survival, for development and the
toy, shown `Degraded` in `parosctl status` (section 3.6), and it is refused for every control
journal and every election journal. Control and election journals are born `double` (the cell's
own on its three founding members, one per zone) and raised to `triple` by the cell coordinator, through
ordinary reconfiguration, once five `storage` machines span three zones.

**What a tenant survives** (decided on 2026-10-07, #252): a tenant's universe directory entry carries
`survives: az | region`, recorded at `REGISTERING` and mirrored into the tenant's control journal
(rebuild from below, section 3.3). It constrains the kind of cell the tenant may live in, a
regional cell for `az` and a multi-region cell for `region` (section 3.7), never whether it may
move: the groups alone decide that.

**How roles are sized** (decided on 2026-10-04). Stateless roles (frontends, proxy leaders,
batchers) are **per-tenant pools**: one pool per role, shared by all the tenant's journals and
keyed by `JournalIdentifier` inside, so a hot journal uses every instance and a quiet one none
(#193). Storage roles — acceptors, replicas — are **per journal**, because they hold that
journal's data; matchmakers are one set per tenant. **Placement spreads load across the cell**
(#212): every role's instances go to the eligible machines of its class zone by zone in round
robin, least-loaded inside a zone, ties broken by a draw seeded per tenant (shuffle sharding
inside the cell), and one journal's storage instances never share a machine (amended on
2026-10-07, #215: least-loaded slots first could put three of five acceptors in one zone). With
six `storage` machines available, a tenant with three journals of two replicas each gets its six
replicas on six machines; its pool of proxy leaders spreads the same way over the `stateless`
machines, each instance serving all three journals.

**Pools per region in a multi-region cell** (decided on 2026-10-07, #253; amends the per-tenant pools
above): proxy leaders, batchers and frontends are pooled per `(tenant, region)`, with at least one
frontend per AZ, and a journal's leader uses its own region's pool. `ProxyId::of(slot,
proxy_count)` (`membership.rs`) assigns `slot_rank(slot, proxy_count)`, so one pool across
regions would put a cross-region hop on a share of every journal's slots. The core does not
change: `ProxyId` is a logical rank and `proxy_count` sits in `Config` under the format marker,
so every region's pool has the same count, and the driver resolves rank `r` to the instance of
rank `r` in the leader's region.

**The leader follows its writer** (decided on 2026-10-07, #215), the main latency lever inside a
cell: with a journal's leader in its writer's zone a write costs one cross-zone round trip,
elsewhere two. Three pieces carry the writer's zone: the `Resolve` answer tags each reference
with its zone (section 3.5), the client prefers a frontend in its own zone (client configuration,
never drawn by the library), and the frontend stamps the origin zone on every write it forwards.
The leader's driver in `paros`, never the core, counts origins per zone over a window and hands
off through `relinquish_to` when another zone dominates past a hysteresis threshold (WPaxos §5.1's
majority-zone policy). The window and the threshold are driver tunables, so `buggify_knob!`s with
documented floors. The cost is one hop, and only a ballot's minter may relinquish, so each
election buys one free move and the next costs a Phase 1; the hysteresis keeps it from being
spent on noise.

Flexible `{q1, q2}` quorums stay a library capability and
are not a tenant mode. A caller never names members or an `AcceptorConfig`: the tenant
coordinator picks them, inside its granted slots, and applies every change through
reconfiguration. A journal's writer mode (section 2) is chosen when it is created and never
changes.

Every tenant has one matchmaker set: per tenant, not per journal (the registry is keyed by
journal inside the set, so a set per journal buys nothing) and not shared across tenants (a
shared set is one role that could not scale per tenant and a blast radius across tenants). The
set is named by the tenant's id (section 3.8). A set is logical: matchmaker *processes* are shared,
each hosting many tenants' sets keyed by `JournalIdentifier`, the way every role is a per-tenant pool.
**One store per matchmaker set** (decided on 2026-10-09, #190): a matchmaker process keeps one
store for each set it hosts, and the registries of that set's journals, keyed by `JournalId`, are
inside it. Every journal of the set, the control and election journals included, has the set from
its first entry; there is no bootstrap exception.
Reconfiguration is the operational primitive for everything, so no tenant opts out of it.

Every tenant has a **minimum footprint**, booked in slots against its cell's capacity when the
tenant is created: one coordinator slot, its matchmaker set, one acceptor quorum of its
redundancy mode for data and one `double` quorum for its control journal (amended on 2026-10-07,
#215: a `single` tenant's control journal still needs `double`). The universe tenant and the cell
tenant count too. A cell refuses a tenant whose footprint it cannot book.

### 3.5 The frontend and the resolver

Two entry roles (decided on 2026-10-07, #233): a **resolver** per region at the universe level, which
redirects a client to its tenant's cell, and a **frontend** per tenant inside that cell, which
forwards the client's calls.

**The frontend.** A stateless process in front of the machines (Spanner's per-region API
frontends authenticate and route the same way). **Frontends are per tenant**
(decided on 2026-10-04): the tenant coordinator places them in role slots like any role and the
tenant's mode sizes the pool, so a frontend folds no other tenant's control journal and one
tenant's load never reaches another's frontends. A client finds its tenant's frontends through the
second hop of **`Resolve`** (below), which any machine of the tenant's cell answers from
the cell registry it already folds (a frontend is a booked slot keyed by tenant). Administration
(`init`, tenant create and delete, drains) is served by the universe tenant's own frontends. It
authorizes the caller through an `Authz` trait
whose implementation verifies a Biscuit token (below), and it routes
each call to the machine serving the journal, so a client never knows placement. It forwards
calls and their answers rather than redirecting the client (decided on 2026-10-04): clients only
ever reach frontends for data, which is what keeps the network the trust boundary. Quotas are M10
(decided on 2026-10-04). Past the frontend nothing knows a tenant name, only
`(TenantId, JournalId)`.

**The resolver** (decided on 2026-10-07, #233). One pool per region, at the universe level, stateless
and shared by every tenant. It answers the first hop of `Resolve`, "which references
serve tenant T", from its cached fold of the universe directory: the tenant's `TenantId`, its cell id
and that cell's entry references. It **redirects**: it never carries data and never forwards
a call. It keeps resolving from its cached fold while the universe tenant is unavailable, which is the
AWS guidance's thinnest possible router and static stability one level up (section 3.3). Before
it answers, it verifies the token's Biscuit signature and that its scope covers the tenant asked
for (an `admin` token may resolve any tenant), using only the root public keys
the universe entry carries: it holds no private key and no state of its own, and Biscuit stays out of
`paros` and `paros-core` as below. A resolver folds the universe directory and nothing else, never a
cell's registry, so it stays thin and every cell answers for itself.

**Resolution is two hops: one call, two answerers.** A client asks a resolver of its region for
tenant T and gets T's cell id and that cell's entry references; it then asks any machine of
that cell the same call and gets T's frontends, each tagged with its zone, from that machine's
registry fold, and it prefers a frontend in its own region and zone. Both answers are cached and
refreshed on `StaleIncarnation` or a redirect. The resolver stops at the cell: inside it, the cell's
machines answer and the frontend forwards, so forward-not-redirect stands and nodes still do no
authorization.

**Where `Resolve` stands** (#216, landed on 2026-10-10). There is no resolver (M12) and no frontend
(#192) yet, so every machine of a cell, founding or admitted, answers both hops in one answer
(`paros::machine::resolve`). It reads two folds through the cell's machines, as a client does, and
keeps them between requests:

- The universe directory, when the cell serves it. A `READY` `users` tenant resolves to its
  `TenantId`, its control `JournalIdentifier` and its cell, with the `universe_id`. Refusals:
  `unknown_tenant`, `not_ready`, `internal` (no user name), `other_cell` (the answer names the
  tenant's cell, for M12), `no_universe` (the cell serves no universe directory yet) and
  `unavailable` (a fold could not be read to its tail, so ask another machine).
- The registry. The answer lists the cell's machines at the addresses the registry holds
  (`cell_book`). A client calls them until the frontend exists. Then the same answer carries the
  tenant's frontends, and with #404 each reference is an `InterfaceRef`.

`paros::client::resolve` asks the entry endpoint's addresses in turn and passes over a machine
that does not answer or answers `unavailable`. `parosctl resolve <tenant>` prints the answer. An
answer is a hint from a fold: a stale one costs the client a refused call, and it resolves again.
The simulation resolves every created tenant at the entry endpoint (every machine, from a drawn
one on) beside the library's own resolution. Its oracles: a removed, internal or never-created
name never resolves, and a resolved tenant is served by the operator's cell. Machines do no
authorization: the Biscuit check is the resolver's and the frontend's (#245).

```
 client ──Resolve(T)──► resolver (its region: universe directory fold, Biscuit sig + scope)
        ◄── cell X, X's entry refs ──┘
 client ──Resolve(T)──► any machine of cell X (registry fold)
        ◄── T's frontends, zone-tagged ──┘
 client ──Write/Read/...──► frontend T (Authz, names) ──forwards──► leader / replica / batcher
```

**Names** (decided on 2026-10-04, #239). A user addresses `paros://<tenant>/<journal>`; the URI
names data, not a location, and the client's entry endpoint (the resolvers' addresses, a
frontend's address until the resolver exists) is client configuration. **Only the entry roles resolve names**:
clients send names and never read the universe tenant, so no tenant sees another tenant's names
(amended on 2026-10-07, #233: the resolver resolves the tenant name, only for a tenant the token's
scope covers, and the frontend the journal name). Until the frontend exists (#192), `parosctl`
resolves with operator rights. A name is free again once its delete completes; a recreated
tenant or journal draws a fresh id, so an old id never aliases a new name.

**Display** (decided on 2026-10-07, #239). Human output prints an id as short hex, git-style
(e.g. `cell=2c94f1`), widened when a prefix is ambiguous within the listing; `--json` keeps the
full id, and a command that takes an id accepts a unique prefix. An internal tenant (section 3.7)
shows a display label derived from its groups (`universe`, `cell` with its cell), display only,
never a resolvable name or an id: section 3.8's no-well-known-id rule stands.

**Where names stand** (#239, landed on 2026-10-10). The library parses and prints
`paros://<tenant>/<journal>` (`paros::name`) and resolves it in two hops
(`paros::client::names`): the tenant name through the universe directory, where only a `READY`
`users` tenant resolves; then the journal name through the tenant's control journal, the
`TenantControl` fold (#210 (tenant control journal)), where only a live journal resolves. A client caches a resolution and drops it
when a call is refused as naming an unknown journal; the next resolution reads the control journal
again. `parosctl` resolves the names its journal commands take. An operator can name a journal by
its ids, `id:<tenant>/<journal>` in hex, each half a unique prefix of an id that `parosctl` can
list, or all 16 digits. Since #210 (tenant control journal), the machines serve every tenant's
control journal, so a name resolves both hops on `parosd`, and `init` creates no user journal.

**Trust** (decided on 2026-10-04). The boundary is the network: only frontends and peers reach
a node's journals (a separate network in the Compose toy; a client reaches a machine only for the
well-known `Resolve` call, section 3.2), and nodes do no authorization.

**Tokens are Biscuits** (decided on 2026-10-04, #245; JWT is in section 11). paros is its own
issuer: a root key pair, Ed25519, whose public half (with its root key id) is recorded in the
universe entry; rotation is adding a key then removing the old one. Tokens are short-lived and there
is no revocation. `parosctl` works **offline**, with no running universe: it generates root key
pairs and mints tokens for any role.

- **Roles** are facts in the authority block, and there are two (decided on 2026-10-10, #400):
  `admin` administers the universe (`init`, cells, machines, tenants, everything below), and a
  `tenant` token is scoped to one tenant for its data plane, its journals and its own view of its
  spread over its cell (#399). There is no `tenant-manager` role: it would have needed the data
  rights of every `users` tenant. Only `admin` creates, deletes and lists tenants, tenant creation
  returns no token, and a key holder mints each `tenant` token offline. No frontend or resolver
  holds a key.
- **Names, not ids** (decided on 2026-10-10, #400): a token and the verifier's facts name a
  tenant and a journal by their full string names, never by hex ids. The entry roles already
  resolve names. An `internal` tenant has no resolvable name, so only `admin` reaches it. A name
  is free again after its delete, so a token for `acme` also works on a later tenant named
  `acme`; short lifetimes limit this.
- **Token content** (#400, `paros-authz-biscuit`). The authority block holds `role(..)`,
  `tenant(<name>)` for the tenant role, a `subject(..)` label and an expiry check. For each
  request the verifier adds `time`, `operation`, `op_class`, `access` (`read` or `write`),
  `target_tenant`, `target_kind` (`users` or `internal`) and `target_journal`, then runs one
  Datalog policy in the server: the meaning of a role changes without new tokens. A view of #399
  asks `view.detail` (admin only) for full detail, else its own operation for the tenant detail.
  A refusal is `InvalidToken`, `Expired` or `Forbidden`.
- **Keys** (#400). `parosctl key generate --label <name>` writes `<name>.private` (mode 0600)
  and `<name>.public`: JSON with the label, a random `u32` key id (the Biscuit `root_key_id`,
  inside the token format only) and the key in Biscuit's own text form. A token without a key id
  the universe entry holds is refused.
- **Names for people, hex ids for local debugging only** (decided on 2026-10-10): everything a
  human reads or types names a key by its label and a tenant or journal by its name. A minted
  authority block carries `root_key(<label>)`, and `token inspect` names the key that verified it. An
  idle machine pins the key with `parosd --root-public-key` to check `FormCell` and `Admit`
  (#245).
- **Derive, macaroon style**: any holder narrows a token offline with `parosctl token derive`
  (read-only, one tenant, one journal, an earlier expiry), with no key: the key that signs the
  next block is inside the token. `--seal` removes it, so nothing more can be derived.

**Deterministic first, fewer features** (decided on 2026-10-04). Biscuit runs only in a way the
simulation can replay, and features that cannot are left out:

- Every key and every appended block's next key comes from an RNG the caller passes in
  (`new_with_rng`, `build_with_rng`, `append_with_key`): a `ChaCha20Rng` seeded with 32 bytes
  drawn from the provider's random source, the OS-seeded thread RNG in production and the seeded
  RNG in the simulation (#400). The thread-RNG defaults are banned (clippy
  `disallowed-methods`). `biscuit-auth` is pinned by git rev until 6.1: 6.0.0 still uses
  `prost` 0.10 and `rand_core` 0.6.
- The authorizer's wall-clock budget (`RunLimits::max_time`, default 1 ms, checked against
  `Instant::now()`) is set out of reach; evaluation is bounded by `max_iterations` and
  `max_facts`, which count deterministically. The returned execution time is never read.
- The current time is a `time(...)` fact the frontend or the resolver adds from its provider's
  clock, never `AuthorizerBuilder::time()`, which reads `SystemTime::now()`.
- Decisions are allow or deny; no query result is consumed, because the Datalog engine's
  `HashMap` iteration order is per process. A refusal is judged by its kind, never by which
  error came first.
- Out of scope: third-party blocks, revocation ids, P-256, snapshots, extern functions and
  Datalog queries.
- Biscuit stays out of `paros-core` and `paros` (its wasm32 clock needs JavaScript's
  `performance`): `paros` defines the `Authz` trait and carries tokens as opaque bytes; the
  Biscuit implementation is its own crate, `paros-authz-biscuit` (#400), used by `parosctl` now
  and by `parosd` and `paros-sim` with #245.

**Routing goes through the universe tenant from M9.** The tenant name → `TenantId` → cell step is
the resolver's from M12; until then the frontend resolves it from its fold of the universe directory,
and with one cell it always answers "this cell", and it runs anyway. The frontend then resolves the
journal name → `JournalId` and its placement from its fold of the tenant's control journal.
Resolvers and the cell's machines answer the same `Resolve` call, "which references serve tenant
T", so `paros://<tenant>/<journal>` never changes when a second cell appears.

Tenants are created and administered through the same frontends, with an `admin` token,
through the universe tenant (section 3.7): one API, one `Authz` trait, exercised in the simulation like
every other call.

**The frontend routes to the data-plane roles** (decided on 2026-10-04).
A single-writer `Write`, a `Truncate` and a `SetLeader` go to the journal's Paxos leader; a
multi-writer `Write` goes through the tenant's batchers when it has any (section 2.4), and the
unbatchers' answers come back through it; a `Read` goes to whatever holds the journal's replica
state. The frontend is not the **proxy leader** of Compartmentalized Paxos, which does Phase 2 for
the Paxos leader (section 4.1); "proxy leader" is never shortened to "proxy", and the entry role
is never called a proxy (decided on 2026-10-07, #233).

### 3.6 Status

`parosctl status [--tenant t] [--cell c]` shows three columns, per tenant, per cell and for the
universe: desired (what the tenant asked for), available (machines registered, not drained, seen
alive) and current (what is placed and serving, with each journal's word: Healthy, Degraded,
Unavailable). The tenant view folds the tenant's control journal, the cell view the cell control
journal, the universe view the universe directory with each cell entry's state. Status is computed live
from those folds and `Inspect`, never written back: there is no separate monitoring store.

It starts small, like `fdbcli status`, and grows by views, each one an admin RPC scoped by the
caller's authorization: **role slots** per machine (class, total, booked, and what holds each
slot), per tenant (its slots and footprint), per cell (free and booked per class) and for the
universe (decided on 2026-10-04).

**The first views** (#399, decided on 2026-10-10). Each view is a request to **one cell**, and the
request chooses the cell: `parosctl` asks the servers it names, and a founding member of that
cell answers. The member reads the cell's own journals: the registry, the universe directory
when the cell hosts it, the election journal and the tenant control journals. A view changes
nothing. The RPC is `View` (`paros.view.View`, method `0x5041_0309`), with three queries:

- **Cell**: every machine (name, address, class, capacity, bookings, standing, up or down,
  founding member), every hosted tenant with its journals and their acceptors, and the cell
  coordinator. Admin only.
- **Tenant**, by name: the tenant's journals and the machines they use. A tenant hosted by
  another cell is refused `other_cell` with the name of that cell.
- **Universe**: the cells and the tenants of the universe directory. Admin only. Only the cell
  that hosts the universe tenant answers it.

**The server filters by scope; the caller never filters.** An `admin` sees every detail. A
`tenant` caller sees only its own tenant, and of each machine that the tenant uses only its name,
its failure domain and whether it is up. Any other query from a `tenant` caller is refused
`forbidden`. Until the frontend checks tokens (#192), the caller states its scope and
`paros::view::authorize` takes it as given. That function is the seam where the Biscuit roles of
section 3.5 (`admin`, `tenant`, `view.detail`) will decide the scope.

`parosctl` shows the views as tables, and `--json` gives one document for scripts:
`machine list|show`, `cell list|show`, `tenant list|show` and `roles` (who holds which role: the
cell coordinator, the acceptors and matchmakers of each journal, and the capacity bookings).
`--as-tenant NAME` asks in the scope of one tenant. `parosctl status` (above) stays the target
that these views grow into.

### 3.7 The universe

The universe runs from M9 with one cell (decided on 2026-10-02, #226).

**A cell** is one set of `parosd` machines. A **regional** cell lives in one region across at
least three availability zones (decided on 2026-10-07, #215): enough failure domains for its
quorums. Zone survival stays a property of each journal's quorums (section 5). Three boundaries
nest: the zone is the infrastructure failure domain, the cell the blast radius, the tenant the
isolation unit. A cell is a blast-radius boundary for bad deploys, overload and poison pills, not
a failover domain; the AWS cell-based architecture guidance says the same (cells contain
overload and bad deployments and are not designed for failover; multi-AZ cells avoid replicating
between cells). Cell creation is refused if its machines span fewer than three zones.

**A universe mixes cell kinds** (decided on 2026-10-07, #253). A cell entry in the universe directory
carries its `kind`, `Regional { region }` or `MultiRegion { regions, witness }`, and the cell's
entry endpoint (given at `universe init` or `add-cell`), which a resolver hands back. A **multi-region** cell (M12, decided on 2026-10-07, #253) spans three regions, each
across several AZs; one of them is the **witness region**, which holds acceptors and matchmakers
with full records and no other role (section 5). A tenant's `survives` (section 3.4) picks the
kind of its cell: `az` a regional cell, `region` a multi-region cell. Each region has its resolvers
(section 3.5), and every cell answers the second hop for its own tenants.

**The universe and each cell have a name** (decided on 2026-10-07, #252; kept on 2026-10-09): a
label chosen at `universe init --name` and `cell init --name`; stored
in the universe entry and the cell entry, unique within the universe, refused when taken. `tenant
list`, `inspect` and `status` (section 3.6) show it beside the id. Same rule as a tenant's or a
journal's name (section 3.5): a label, never the identity; ids stay random (section 3.8).

```
 universe F: the universe directory (tenant → cell), held by the universe tenant
 ┌────────────────────────────┬────────────────────────────┬────────────────────────────┐
 │ region W                   │ region C                   │ region N                   │
 │ resolvers W (thin, cached) │ resolvers C                │ resolvers N                │
 ├────────────────────────────┼────────────────────────────┼────────────────────────────┤
 │ cell W1 regional, 3 AZ     │ cell C1 regional, 3 AZ     │ cell N1 regional, 3 AZ     │
 │  cell tenant W1            │  cell tenant C1            │  cell tenant N1            │
 │  tenants: survives = az    │  tenants: survives = az    │  tenants: survives = az    │
 ├────────────────────────────┴────────────────────────────┴────────────────────────────┤
 │ cell MR1 multi-region over W, C, N (N = witness)                                     │
 │  cell tenant MR1 (2/2/1)   universe tenant (2/2/1)   tenants: survives = region         │
 └──────────────────────────────────────────────────────────────────────────────────────┘
 client: hop 1  resolver of its region → (cell id, the cell's entry references)
         hop 2  any machine of that cell → the tenant's frontends (zone-tagged, pick local)
```

**The universe tenant always exists** (named *meta* until 2026-10-04). It is one tenant whose control journal also holds the directory, so it is
a single journal. In M9 it lives in the only cell; any cell may host it later. Once a
multi-region cell exists, the universe tenant is hosted there (decided on 2026-10-07, #253), moved by
its M12 move, so a region loss never stops tenant creation and moves; resolvers in every region
keep resolving on their folds regardless. It stays small: it
answers only "which tenant lives in which cell" plus the cell entries. Quotas, billing and global
status go elsewhere. When the directory grows large (M12) it is split across several journals by
tenant range, the AWS guidance's range-based mapping.

**A tenant lives in exactly one cell.** A tenant too large for a cell gets a dedicated cell; a
tenant is never split by journal.

**Every universe operation is an idempotent state machine** (FDB's metacluster, section 10). A
tenant's directory entry carries a state: `REGISTERING`, `READY`, `REMOVING`,
`UPDATING_CONFIGURATION` or `ERROR` (`RENAMING` was dropped on 2026-10-04: no milestone renames
a tenant, and names are the frontend's). Creating a tenant (`parosctl tenant create`)
writes it into the universe directory in `REGISTERING` with a cell assignment (always the one cell
today), creates the tenant in its cell, then marks it `READY`. If an operation fails partway,
re-running the same operation is allowed and resumes where it stopped; on success the tenant
returns to `READY`. **A tenant is created once** (decided on 2026-10-04): a creation is named by
the tenant id its creator drew, so only a re-run carrying that id resumes it; any other creation
of a name the universe tenant holds, in any state and whatever its placement, is refused (`NameTaken`), never
merged into the first. Until #225 the client drives the steps, so an interrupted creation stays
`REGISTERING` until it is deleted; with #225 the coordinator that owns the control journal
finishes every `REGISTERING` and `REMOVING` entry it finds (decided on 2026-10-04: the entry is
the work order, no client resumes another's creation). A tenant in any state may be removed; only a `READY` or
`UPDATING_CONFIGURATION` tenant may be reconfigured. `init` follows the same rule. Cell entries
carry a state too: `REGISTERING`, `READY`, `REMOVING` or `RESTORING`, and only a `READY` cell
receives new tenants. In M9 the one cell goes `REGISTERING` → `READY` during `init`, and
`RESTORING` during a recovery.

**Registration is recorded on both sides and verified on every step.** The universe directory's cell entry holds the
cell's id; the cell's durable cell plan and its control journal hold the universe's id; both hold a
metadata version number. Every
multi-step operation checks, at each step, that it still talks to the same universe and the same
cell as on its previous step, and refuses otherwise (FDB's `MetaclusterOperationContext`). The
metadata version lets a reader refuse a format it does not understand.

**What M9 carries so that M12 adds no protocol or data-model change:**

- The machine record carries `node_id`; the durable cell plan carries `cell_id` (amended on
  2026-10-04: the code keeps it in the cell plan, not `Config`). `universe_id` and the metadata
  version are minted after the plan, so the cell control journal holds them (`JoinFleet`,
  amended on 2026-10-10, #216).
- `Hello` carries `cell_id`; `universe_id` joins it with the first call that crosses cells (amended
  on 2026-10-10, #216, section 3.2).
- `Resolve` is keyed by tenant.
- Tenant control journals are self-describing (name, desired state, `survives`, the name and kind
  of the tenant's cell, #210).
- The universe directory's tenant entries carry the universe-unique `TenantId`, the `JournalIdentifier` of the tenant's control
  journal, the cell assignment, the state, a configuration sequence number, the tenant's
  **group**; the universe directory's cell entries carry the cell id, the cell tenant's `JournalIdentifier`, the state and
  the metadata version. No id is well known (section 3.8): a second cell learns the universe tenant's `JournalIdentifier`
  when it is admitted, from the universe tenant, and records it in its cell plan.
- The universe entry carries the universe's name; the cell entries also carry the cell's `kind`, its
  entry endpoint and its own name, and the tenant entries the tenant's `survives`, mirrored into
  its control journal (decided on 2026-10-07, #252), so a universe that mixes cell kinds is entries,
  never a new field.
- Every peer and client message carries its `JournalIdentifier` `(TenantId, JournalId)` (section 3.8).
- A checkpoint is a run of small records in the journal it checkpoints (section 3.9), so no
  state is bounded by one batch.
- No component assumes there is only one cell: every lookup goes through the universe directory.

**Tenant groups** (decided on 2026-10-04). A tenant belongs to a **set of groups**, recorded in
the universe directory's tenant entry when the tenant is registered and never changed afterwards.
Each group carries a rule, and a tenant obeys the rules of every group it is in. The set of
groups is fixed by paros for now; operator-defined groups (rollout waves, co-location), as labels
without rules, may come later (decided on 2026-10-04):

| Group | Rule | Members |
|---|---|---|
| `internal` | created only by paros's own operations (`init`, adding a cell), never through the tenant API; reached only for administration, through the universe tenant's frontends | the universe tenant, every cell tenant |
| `cell` | **never leaves its cell**: it *is* its cell (section 1); reconfigured only within it | each cell's cell tenant |
| `universe` | holds the universe directory; moves with its coordinator | the universe tenant |
| `users` | created by the tenant API (`parosctl tenant create`, through the universe tenant's frontends); served by its own frontends | every served tenant |

So the universe tenant is `{internal, universe}`, a cell tenant `{internal, cell}` and a served tenant
`{users}`. **The groups alone decide whether a tenant moves**: it moves unless one of its groups
forbids it, and today only `cell` does. There is no per-tenant movability flag (a
`movable`/`pinned` placement was dropped the same day); the universe tenant refuses a move of a
tenant in `cell` when it applies the move's first entry, so no step of the move runs. The tenant
API creates tenants in `users` only, and the universe tenant refuses any other group from it.

**Moving a tenant (M12)**, any tenant outside the `cell` group, reconfigures its journals and matchmaker set onto the target cell, then
transfers ownership with `SetLeader` on the tenant's control journal (the one moment ownership
changes), then flips the directory pointer, then the old cell forgets the tenant: the AWS
guidance's four migration phases, copy, flip, redirect, forget. The directory entry is a pointer,
never the authority: if it disagrees with the control journal's leader, the leader wins.
The universe tenant moves the same way, being one journal; moving it to a dedicated cell is the escape hatch from
co-locating it with tenants.

**Moving the universe tenant** (decided on 2026-10-09). The universe information and the tenant
list are tenant data, so they move like the data of any user tenant. The universe tenant's
control journal holds the universe entry, the cell entries and the tenant entries (the universe
directory). A move of the universe tenant moves all of them, with the same four phases and no
new message or field:

- **Who drives it.** The universe coordinator drives its own move. It writes each step in its own
  control journal, so the move is an operation state machine like any other and resumes after a
  crash.
- **Copy.** It reconfigures its journals and its matchmaker set onto the target cell, inside
  capacity that the target cell grants.
- **Flip.** `SetLeader` on its control journal. From then on, its leader runs in the target cell.
- **Redirect.** It writes its own tenant entry with the target cell. Then it tells every `READY`
  cell the new host. Each cell records it in its **universe pointer**, a durable cached value
  beside its registry fold. Resolvers fold the tenant entry like any other.
- **Forget.** The old cell releases the bookings. Its tenant list keeps a moved entry that names
  the target cell, so a caller with a stale pointer is redirected, never left without an answer.

These values do not change when the universe tenant moves: `universe_id`, its `TenantId` and the
`JournalIdentifier` of its control journal. Every admitted cell records them in its cell plan at
admission, so a move writes no cell plan. Only the universe pointer changes. The hosting cell is
a location, not an identity, and no component finds the universe tenant by the cell it was born
in. Admin calls (`universe add-cell`, `tenant create`, drains) follow the pointer like any client.

The same rule covers every future universe-level tenant (for example quotas or billing): it is
in `internal` and not in `cell`, so it moves. Only a cell's own cell tenant stays in its cell
(section 1).

**Moving between cell kinds** (M12, decided on 2026-10-07, #253) is the same four phases.
Copy: the tenant coordinator reconfigures each journal to a `C_new` whose members carry
`(region, az)` failure domains over the target cell's machines; catch-up crosses regions and needs a
throttle knob; the matchmaker set hands over by generation. Flip: `SetLeader` on the tenant's
control journal. Redirect: the directory pointer, folded by the resolvers. Forget: the old cell
releases the bookings. With the `cell_id` in the configuration (below), the copy itself names
the owning cell. The reverse move, from a multi-region cell to a regional one, is the same under
the regional rule.

**The cell inside the configuration** (decided on 2026-10-07, #215, #232; open until then). A
matchmaker's registry binds an acceptor set and its quorum system to a ballot, and from M11
`AcceptorConfig` also carries the configuration's `cell_id`, in the same single format bump as
its failure domains (section 5). Node ids are universe-unique, so a reconfiguration onto another cell's
machines already moves a journal's data; with the `cell_id` in the configuration the move is
itself decided by Paxos: the effective configuration (the highest-ballot reconfiguration a
matchmaker quorum holds) names the cell that owns the journal, the directory is a cache of that
fact, and a reconfiguration naming another cell for a cell tenant's journal can be refused where
it is registered. The cost is one field in `AcceptorConfig` that the core never decides on, and
every node knowing its own cell.

**M12, "Multiple cells"** (#232, #233): adding a second cell, removing a cell (its id goes into a
tombstone set so it cannot silently rejoin), moving tenants between cells, moving the universe
tenant, the resolver (section 3.5), splitting the universe tenant by range, placement across cells
(each cell entry with a configured capacity and an allocated count, an ordered index of cells
with room, the fullest cell of the kind the tenant's `survives` asks for that still has room
after a quick availability check (amended on 2026-10-07, #232), an optional preferred cell, a
per-cell switch that stops new placements), and tenant locks (`UNLOCKED`, `READ_ONLY` or
`LOCKED` with an owner id) to make a tenant read-only during a move.

### 3.8 Identifiers

Every identifier is random or minted by the one writer that can check it, never derived from a
cell's log position, so nothing is renumbered when cells are added, removed or restored (decided
on 2026-10-02, #226). **No identifier is fixed** (decided on 2026-10-04): there is no well-known
tenant, no well-known journal and no reserved range. `0` means unset in every id space, and that
is the only value with a meaning. **No id has a default** either: an id is drawn or read, never
assumed, and unset is a state to refuse, not a value to fall back on. Names are labels beside
these ids (decided on 2026-10-07, #252): the universe's, a cell's, a tenant's and a journal's name
(sections 3.5, 3.7) are chosen and unique within their scope, never derived from or reused as an
id.

**Ids on the wire, labels for people** (decided on 2026-10-09). Every RPC and every stored record
names the universe, a cell, a tenant and a journal by its `u64` id only; a name never crosses the
protocol below the entry roles. Each of them also has a **string label**, chosen when it is
created (`universe init --name`, `cell init --name`, tenant and journal create) and shown by
`parosctl`. A label resolves to its id only at the edge (the resolver and
the frontend, section 3.5, and `parosctl` for administration), and an id never becomes a label.

**Hex ids are for local debugging only** (decided on 2026-10-10, #399). Every input and output
for a person uses names: `parosctl` prints the name of each machine, cell, universe, tenant and
journal, and takes a name wherever it takes an argument. A short hex id appears only when no
name is known. A machine has a name too: `parosd --name` (`PAROS_NAME`), by default the host of
its advertised address. The machine record and the registry keep it, and a new name registers
the machine again. `parosctl init --universe-name --cell-name` names the universe and the first
cell, and the universe directory keeps both names; a second cell with a name already in use is
refused.

- `node_id`, `cell_id`, `universe_id`: random, minted at format, `cell init` and `universe init`
  respectively, and
  stored in the machine record (`node_id`), the durable cell plan (`cell_id`) and the cell control
  journal (`universe_id`, `JoinFleet`, amended on 2026-10-10, #216). They
  are written once, unset → set, and a later mismatch is refused at boot like any `Config`
  mismatch.
- The **leader uuid** of a single-writer journal (section 2.3): 128-bit random, drawn by the
  leader for one term. A writer never chooses an identity that another process could share. The
  journal keeps no history of past uuids and does not refuse one that led before: it trusts its
  clients to draw fresh uuids (decision C, section 2.3, #241).
- **Failure domains** are not identifiers: `AcceptorConfig` carries the registry's
  `failure_domain` names unchanged (section 5).
- **Tombstones** (removed tenant ids, dropped tenants, deleted journal ids) are kept forever, a
  `u64` each. They are the one part of control state bounded by history rather than by live
  entities (section 3.9), accepted as such (decided on 2026-10-04). Names are not tombstoned
  (section 3.5).
- `TenantId(u64)`: random, drawn by the creator and recorded by the universe tenant in the `REGISTERING` step;
  the universe tenant refuses a duplicate at apply and the creator redraws. It is universe-unique, so moving a
  tenant between cells never needs a new id. The system tenants are no exception: each cell's
  cell tenant gets a random id at the cell's `init` (so two cells' cell tenants differ), and the
  universe tenant gets one when `init` creates the universe, kept when the universe tenant moves. The universe tenant records both
  (its own in its first entry, each cell tenant in that cell's entry), so its duplicate check
  covers them too. The universe tenant records each tenant's groups beside its id
  (section 3.7). FDB gave each metacluster an id prefix for the same goal; a random draw
  checked by the universe tenant needs no prefix.
- `JournalId(u64)`: random, unique within its tenant, recorded and checked at apply by the tenant
  coordinator, the single writer of the tenant's control journal; a duplicate is refused and the
  creator redraws. A tenant's **control journal** has a random id too, drawn with the tenant and
  recorded where the tenant is recorded: in the universe directory's tenant entry, and for the two system tenants
  in the cell plan (the cell tenant's in its own cell's plan, the universe tenant's in the plan of
  every admitted cell). A journal's id never changes when its tenant moves; a control journal that
  recovery rebuilds (section 3.10) gets a new one, so the old and the new can never be mistaken
  for each other.
- **Discovery replaces convention.** The only fixed starting points are a machine's addresses:
  the `JournalIdentifier`s of the cell's and the universe tenant's control journals are learned from any machine of the cell
  (section 3.2), with the cell that hosts the universe tenant now, and everything below them through their folds.
- **Every peer and client message carries its `JournalIdentifier` `(TenantId, JournalId)`**, riding
  the `Deliver` envelope (#235), so uniqueness is only ever needed where it can
  be checked. A tenant's matchmaker set is named by its `TenantId`.

### 3.9 Checkpoints

The control plane compacts its control journals with the same two calls any tenant has (decided
on 2026-10-02, #227): `paros-core` gains nothing. The pattern, shipped in `paros::client` (#230)
for the control plane first and offered to users as a recipe, never a guarantee paros enforces:

1. The coordinator pauses its own control writes (it is the single writer, and control writes are
   rare).
2. It writes a checkpoint of the folded state as a run of ordinary fenced `Write` batches.
3. It calls the fenced `Truncate` up to the run's first record.
4. Readers start at `first_seq`, which is always the first record of a complete run, and fold
   forward.

A checkpoint replaces the state entirely, so a crash between the write and the truncate is
harmless: the next fold meets a checkpoint in the middle of the log and resets on it. `Truncate`
only ever targets the first record of a complete run. A reader that gets `Truncated` restarts
from `first_seq`.

- **Control state is bounded by live entities, never by history**: the latest entry per
  `node_id`, current assignments only. That is what keeps checkpoints small. The one exception is
  the id tombstones (section 3.8).
- **Trigger**: checkpoint when the log since the last checkpoint reaches `k ×` the current state
  size, plus a time bound, which caps the extra writes at `1/k` (shipped defaults: `k = 4`, one
  minute; both are knobs in simulation). Kafka (KIP-630) snapshots only
  after a minimum number of bytes and a minimum share of changed records, and KIP-876 added a
  time trigger; Redpanda snapshots its controller after each command or at most every 60 seconds.
- **Local copies of a fold are caches only**, never the only copy of anything: a coordinator's
  memory, a machine's cached registry. That is why the checkpoint lives in the journal and not in
  a local snapshot file per replica, as Kafka and Redpanda do it: coordinators are `stateless`,
  so a newly elected coordinator has no local file to start from.
- **No checkpoint is one large record** (decided on 2026-10-10, #353). Pierre asked for the
  simplest design that scales. A checkpoint is a run of records in the journal it checkpoints,
  each record the 8-byte magic and a `paros.checkpoint.v1.CheckpointRecord`
  (`proto/checkpoint.proto`):
  - `Begin { covers_up_to }`, at position `covers_up_to`: the state covers every position below.
  - `Chunk { bytes }` records: the state cut into pieces of at most
    `ClientTunables::checkpoint_chunk_bytes` (8 KiB by default).
  - `End { chunks, checksum }`, the **commit point**: the chunk count and the CRC-32C of the
    chunks concatenated.

  The owner writes the run in batches under `checkpoint_batch_records` and
  `checkpoint_batch_bytes`; a node's `TooLarge` lowers both for the rest of the run. A small state
  fits one batch, so one slot. A large one takes as many small batches as it needs. Batches go
  one at a time; pipelining them waits until a pause is too long. No entry is ever inside a
  run, because the owner holds the writer. A reader collects a run from `Begin` and restores (or
  verifies) at a valid `End`. An entry, a new `Begin` or a gap inside a run drops it. A run with
  no `End`, or with a wrong count or checksum, is never restored. A crash inside a run is
  harmless for the same reason. The retired `Inline` and `Ref` forms (#230) decode to no form
  and are refused: no cell wrote `Ref`, and no cell outlives the change. A second checkpoint
  journal (`Ref`, Pulsar PIP-14) and a rolling sharded checkpoint were considered and rejected as
  more complex (#353).

### 3.10 Recovery (deferred)

**Deferred out of M9** (decided on 2026-10-04, #231): nothing of it is built, and the simulation
cannot reach a lost control quorum while it runs one founding member (#213). The design below stays the
direction and is taken up as its own later issue. Two questions it must answer then: with the universe
tenant's and the cell's control journals on the same founding members, recovery reads the machines' own stores
(`journals/<tenant>/<journal>/`, their `JournalIdentifier`s and assignments) to rebuild both, tombstones
included; and a new control `JournalId` must reach machines whose cell plan is written once.

Losing a control quorum is recoverable without unsafe Paxos surgery, at both levels, because
control journals are rebuilt from below (decided on 2026-10-02, #225, #231). User data is never
touched: the tenants' journals have their own quorums.

- **Cell.** `parosctl init --recover` starts a fresh cell control journal, under a new random
  `JournalId` recorded in the cell plan, and a new recovery generation; live machines re-register; tenant coordinators re-report from their own control
  journals; any node still holding the old configuration is refused.
- **Universe.** The universe directory is rebuilt from the cells' tenant lists, the way FDB's metacluster
  could rebuild a lost management cluster from its data clusters.
- Every recovery has a **dry-run** mode that reports what it would change without changing it,
  and takes a **recovery id** recorded in the journal so two recoveries cannot run at once. Both
  come from FDB's metacluster restore.

## 4. Roles

All six roles of Compartmentalized Paxos, plus the matchmakers of Matchmaker Paxos, each
scalable independently per tenant by its coordinator:

| Role | Class | In paros |
|---|---|---|
| Leader (proposer) | storage | `Proposer` inside `ColocatedNode` |
| Proxy leader | stateless | `ProxyLeader`, `run_proxy` |
| Acceptor, grid quorums | storage | `Acceptor`, `QuorumSystem::Grid` |
| Replica | storage | `ReplicaNode`, `run_replica`; serves `Read` |
| Batcher | stateless | to build; multi-writer journals only (section 2.4) |
| Unbatcher | stateless | to build; multi-writer journals only |
| Matchmaker | storage | `Matchmaker`, `run_matchmaker`; one logical set per tenant, processes shared |
| Frontend (the entry role) | stateless | to build: one pool per tenant, per `(tenant, region)` in a multi-region cell (section 3.4); authorizes, resolves names and forwards each call to the role that serves it (section 3.5) |
| Resolver | stateless | to build in M12: universe level, one pool per region shared by every tenant; redirects a client to its tenant's cell, never carries data (section 3.5) |
| Coordinator (universe, cell, tenant) | stateless, or a founding member at bootstrap | to build: the election library over a multi-writer journal (#240) |

### 4.1 How the pieces fit

Two papers, one per concern, both in `docs/references/papers/`. **Compartmentalized Paxos**
(Whittaker et al.) splits the leader's work into roles that scale on their own; **Matchmaker
Paxos** (Whittaker et al.) lets the acceptor set change without stopping. paros runs both per
journal, and the journal's leader-uuid rules (section 2) sit on top, judged at apply.

**The write path.** The leader only *sequences*: it gives each command a slot and hands it off.
A proxy leader does the Phase-2 work for that slot, so the leader's CPU and network stop being the
bottleneck. Acceptors may form a grid, so a Phase-2 quorum is one column and not a majority.
Replicas learn chosen slots, walk the contiguous prefix and apply them; that apply is where the
journal state machine judges each `Write`, `Truncate` and `SetLeader`. Batchers and unbatchers
(multi-writer journals only, section 2.4) group many clients' writes into one slot and fan the
answers back out.

```
 clients ──► batchers ──► LEADER ──────► proxy leaders ──► acceptors (grid)
   ▲         (opt-in)    (Proposer:      (Phase 2a for      ┌────┬────┬────┐
   │                      slot = next)    one slot)         │ a1 │ a2 │ a3 │  Phase 2 quorum
   │                                         ▲              ├────┼────┼────┤  = one column
   │                                         │ 2b acks      │ a4 │ a5 │ a6 │  Phase 1 quorum
   │                                         └──────────────└────┴────┴────┘  = one row
   │                                         │ chosen(slot)
   │                                         ▼
   └──── unbatchers ◄──── replicas: walk the chosen prefix, apply in slot order
          (opt-in)        (journal state machine: leader uuid, expected_seq, first_seq)
```

**The read path.** A read never touches the leader (Compartmentalized Paxos §3.4, paros's
`QuorumRead`). The server holding the journal's replica state asks one Phase-1 quorum (a row) for
the highest slot each acceptor has voted in. Any chosen slot was voted by some acceptor in every
row, so the maximum covers everything chosen. The server waits until its applied prefix reaches
that maximum, then answers.

```
 client ──Read──► replica ──"highest vote?"──► one row of acceptors
                     │    ◄── max watermark w ──┘
                     │  wait until applied prefix ≥ w
 client ◄──records───┘
```

**Matchmaking, then Phase 1.** A new leader (or a reconfiguration, which is just a new ballot)
first registers its ballot `b` and the configuration `C_b` it will use at a quorum of the
tenant's matchmakers. They answer with the earlier configurations still in force, `H_b`. The
leader then runs Phase 1 against a quorum of **every** configuration in `H_b`, because any of them
may hold a chosen value, and Phase 2 against `C_b` alone. Once no future leader can need an old
configuration, the matchmakers garbage-collect it, and its acceptors can retire (`may_retire`).

```
 candidate (ballot b, config C_b)
   │ 1. MatchA(b, C_b) ─────────────► matchmakers (majority of the tenant's set)
   │ ◄──────────── H_b = { C_x, C_y }  configurations of lower ballots, not yet GC'd
   │ 2. Phase 1a(b) ────────────────► a quorum of C_x  AND  a quorum of C_y
   │ ◄──────────── promises + votes    (adopt the highest-ballot vote per slot)
   │ 3. Phase 2a(b) ────────────────► C_b only, slot by slot (through proxy leaders)
   ▼
 leading in C_b.  Reconfigure = steps 1–3 again with C_new at a higher ballot.
 GC: when every slot below a floor is chosen in C_b, older configs are forgotten.
```

Matchmakers are mandatory in the service (section 3.1): every journal has its tenant's set,
because reconfiguration is how a dead disk is replaced, a machine drained, a journal moved and a
quorum system changed.

## 5. Failure model and zones

The service survives disk loss and machine loss in one region: a corrupted record is CTRL's
`faulty` and repaired by the protocol, a wiped disk is refused at boot as amnesia and the
identity is replaced by reconfiguration, a crashed machine restarts as an existing member, a dead
machine is reconfigured out and its journals placed elsewhere. This is what the simulation
already exercises; what changes is who drives the healing: today the harness's client composes
the reconfigurations, in the service the tenant's coordinator does, from desired state. Losing a
control quorum will be recoverable at both the cell and the universe level, without touching user
data (section 3.10, deferred). Beyond one region, a multi-region cell survives the loss of a
region (M12, below).

**The storage contract.** paros's stores are moonpool-journal's (`paros::journal`), and the
simulation must give them at least the chaos the harness's in-memory stores carry today (decided
on 2026-10-04, #176, #202). Every storage fault moonpool can inject is eventually in contract.
Three families start masked through moonpool's `storage_fault_mask` (moonpool#292), each with
an issue to lift the mask: phantom writes (a write acknowledged as synced that never lands loses
an acknowledged vote, which no quorum survives), a hung disk (`DiskFailure`: it needs a storage
watchdog in the driver) and stall/throttle episodes (`Degradation`). Barrier violations ("sync
lies") and moonpool's slow-disk extremes run unmasked (decided on 2026-10-06, #176): no moonpool
profile draws barrier violations, and the slow-disk extremes are a performance knob the mask
does not cover; if paros does not stay live on them, that is a finding, not a knob to clamp.
Matchmaker stores take damage too, under a minority or rolling pattern counted as the run's one
matchmaker loss and exclusive with the matchmaker wipe; matchmakers get their failure domains
as a second `.cluster()` group with its own `replicated_storage_faults` call (moonpool#297
gives `.processes()` groups no locality). moonpool now lets a `.cluster()` group draw zero
processes (moonpool#310, pinned since 0a68199), so nothing blocks the move; matchmakers stay a
`.processes()` group until the replicated fault patterns that need them land (#202; an
implementation deferral of 2026-10-08, #176, not a change of direction). moonpool reports no per-process damage (moonpool#295,
closed as not planned), so the ground truth stays the ledgered injector's and the journal's
own verdicts.

**The journal is CLSTORE, and paros uses it directly** (decided on 2026-10-07, moonpool#308). The
first moonpool-journal was a dense Raft-shaped log; paros journaled operations into it and folded
them at boot, with checkpoint brackets. It is rewritten as a protocol-free CLSTORE journal: an entry
is a `u64` position plus an opaque identity, written in any order (laggy slots), with overwrites,
tombstones and a floor; a persist record far from each entry; two-copy metainfo in its own file,
the copies written one after the other; no snapshots and no compaction. A commit is one sync
(CLSTORE's, the last batch ambiguous) or two (always decided), recorded per batch; production
ships two, the simulation draws either. paros stores the state itself: a slot is a position, its
ballot the entry's identity, the promise, chosen index, floor, sealed state and format marker the
metainfo; a matchmaker registration is a position of its own, its generation in its identity.
**A commit is atomic one way only**: the journal writes a commit's metainfo only once its batch
is durable (moonpool#309, pinned since 0a68199), so a durable metainfo vouches for its batch,
but a crash between the two lands the batch without its metainfo. paros puts a batch and the
metainfo that depends on it in one commit, and orders the reverse across commits, each durable
before the next (#264, #176): a node commits a raised promise alone (an entry never lands above
its promise), then its entries with its floor and metainfo (a chosen index never lands ahead of
the entry that makes its slot chosen); a matchmaker commits its new registrations with its
metainfo, then its clears, and a boot keeps only the registrations the durable metainfo vouches
for (at or above its watermark, of its generation). Storage chaos on journal seeds is the
local half of this contract: moonpool's crash damage, failed syncs, short transfers and lost
directory entries, a crash at a driver `hint!`, and a power loss inside a commit (the journal's
own `hint!`s, #294); rot waits for
replicated fault patterns. The simulation was proven to catch journal durability bugs by
mutation (an unsynced commit, a recovery that drops an acknowledged batch, an unsynced promise:
each red).

Zones are failure domains, and a cell spans enough of them for its quorums (section 3.7). What
the WPaxos read (section 10) established for one region with several availability zones:

- Surviving one zone loss needs `fz = 1`, and then every Phase 2 spans two zones, exactly like a
  zone-balanced majority, while tolerating fewer node failures than that majority. WPaxos's
  latency win exists only at `fz = 0`, which gives up zone survival. Its per-zone quorum system
  is therefore not adopted.
- paros's grid cannot express zone survival either: rows and columns are positional over sorted
  ids, and with column = zone a zone loss kills every row, with row = zone it kills every column.
  The grid stays the opt-in throughput mode (section 3.4), with its cost (one dead acceptor
  freezes its column until reconfiguration) stated to the tenant that picks it; the redundancy
  modes are the zone-surviving default.
- What is adopted (decided on 2026-10-07, #215), detailed below: failure domains inside
  `AcceptorConfig`, a placement rule judged through `QuorumSystem` and never a count, the leader
  following its writer through `relinquish_to` (WPaxos's steal without a Phase 1, section 3.4),
  zone-spread matchmaker sets, and in the simulation a zone-kill attrition mode and a zone-aware
  copy budget, without which it cannot prove zone survival (section 6).

**Failure domains in the configuration.** `AcceptorConfig` carries a vector of
`FailureDomain { region, az }` parallel to its sorted members, holding the names copied from the
registry's `failure_domain` (`RegisterNode`, section 3.2, e.g. `("eu-west-1", "eu-west-1a")`):
short bounded strings, compared for equality only, never parsed or ordered by the core. Names mean
the same thing universe-wide, so a move between cells (section 3.7) compares domains correctly; a
cell-scoped pair of integers would let one cell's `az 2` equal another's and the zone rule could
accept a placement that does not survive a zone. The cost is a few dozen bytes per member in a
configuration of at most nine members. The vector is bound to the ballot with the
configuration, because two nodes that disagree on a member's failure domain evaluate different
quorums; the registry's `failure_domain` is the composer's input, copied when it composes, never
read live by a tally. An empty vector is the plain arm, byte-identical to today's configuration,
so plain Multi-Paxos is unchanged. The configuration's `cell_id` rides the same single format bump
(section 3.7).

**The zone rule** is two predicates over a configuration's members `M`, built from
`has_phase1_quorum` and `has_phase2_quorum` over subsets of voters, never a count:

1. **It survives any one failure domain**: for every domain `d`, both `has_phase1_quorum` and
   `has_phase2_quorum` hold on `M` minus `d`.
2. **No domain commits alone**: for every domain `d`, `has_phase2_quorum` fails on `M`
   intersected with `d`.

The rule is valid for `Majority`, `Flexible` and `Grid` alike. It is one pure function in
`membership.rs`, shared by the tenant coordinator's placement and the simulation's composer.

**The shape of a regional cell.** A regional cell spans at least three zones (section 3.7). Its
minimum production shape is two `storage` machines and one `stateless` machine per zone, nine
machines: `triple` needs five acceptors on five distinct machines and survives a zone only when
spread 2/2/1.

- **Acceptors**: `triple` 2/2/1, the lone acceptor preferably in the zone that writes least (a
  later reconfiguration adjusts it); `double` 1/1/1; `single` none (section 3.4); the grid is
  unchanged and outside the rule, the risk its tenant states when it picks it (section 3.4).
- **Matchmakers**: 1/1/1 or 2/2/1, majorities only; a tenant's matchmaker preferably never
  shares a machine with its acceptors, while capacity permits.
- **Stateless roles**: once a tenant's pool has three instances, at least one frontend and one
  proxy leader per zone. Coordinators run on `stateless` machines and are re-elected in a
  surviving zone after a zone loss.
- **Replicas** spread over the zones, and a `Read` goes to the replica in the reader's zone.

**The degraded trap.** After the loss of a zone that held two of a `triple` journal's acceptors,
three of five remain with zero slack, and the zone rule cannot be met again over two zones. The
coordinator waits out the re-placement bound (section 3.2), the journal shows `Degraded`
(section 3.6), and it is never re-placed into two zones; it is re-placed into a fourth zone only
where the region has one.

**A zone loss, a region loss.** A regional cell serves through the loss of one zone. The loss of
its region takes it down by design: cells are not failover domains (section 3.7), resolvers
elsewhere keep pointing at it, and nothing fails over. **Static stability** holds inside the
cell: the data plane and the `Resolve` answers come from folds, so only placement, new tenants,
new capacity and moves wait.

```
                AZ a                   AZ b                   AZ c
 entry          frontend T             frontend T             frontend T
 stateless      proxy leader           proxy leader           proxy leader, coordinator T
 journal J      acc1 (LEADER*), acc2   acc3, acc4             acc5             majority 3 of 5
                replica r1             replica r2
 tenant T       matchmaker m1          matchmaker m2          matchmaker m3    majority 2 of 3
 cell tenant    registry acceptor      registry acceptor      registry acceptor
 * the leader follows its writer's zone (relinquish_to, with hysteresis)
 zone c lost: 4 of 5 acceptors, 2 of 3 matchmakers, serves
 zone a lost: 3 of 5, zero slack, serves; Degraded, never re-placed into two zones
```

**Multi-region cells** (M12, decided on 2026-10-07, #253; M13 folded into M12 on 2026-10-09). Failure domains are two-level,
`(region, az)`: in a regional cell every member shares one region and the rule runs over AZs; in a
multi-region cell it runs at both levels. A `triple` journal is placed 2/2/1 over the
three regions, its lone acceptor in the witness region, and each region's acceptors of one
journal sit in distinct AZs. The rule: for every region `r`, both quorums hold on `M` minus `r`;
for every AZ `a`, both hold on `M` minus `a`; and no region alone holds a Phase-2 quorum.
**Region loss and AZ loss are separate guarantees, not a joint one**: a region lost together
with an AZ elsewhere leaves two of five, and the journal stops. Joint survival needs 3/3/3 (nine
acceptors, a majority of five), a later mode.

- **Latency.** A write commits from the leader's region with its two local acceptors plus the
  fastest remote one: one round trip to the nearest second region, DSQL's measured behaviour. A
  read costs one cross-region round trip (section 2.5). After a region loss three of five remain,
  with zero slack, and every slot waits for the farthest survivor (Brooker's 2-of-2, section 10).
- **The witness region** holds acceptors and matchmakers with full records, bytes included, and
  no other role: no frontend, replica, batcher, proxy leader or coordinator slot. A server answers
  only from records it holds (section 2.5), and Phase-1 recovery and CTRL repair are unchanged.
  `ColocatedNode` folds by default, so "no replica" means no `Read` routed there and no replica
  slot booked, not a node without bytes. Any `ColocatedNode` may campaign, so keeping the leader
  out of the witness region is a driver placement policy (an election backoff by placement),
  never the core's.
- **The leader moves cheaply.** It holds no durable leadership state, so it moves as cheaply as
  DSQL's adjudicator: one `Relinquish` with its pending rounds (section 3.4).
- **A partition through the witness.** When regions W and C cannot reach each other and both
  reach the witness N, writers commit through N, and the far side's readers stay live and catch
  up from N through ordinary peer catch-up (the 2025 reconstruction's third region that forwards,
  with no relay code). The hazard: both sides can form three of five through N's acceptor, so
  elections can duel across the partition with the witness as tiebreaker. It needs a simulation
  shape and a liveness oracle, leadership settling after the chaos window (section 6); the
  election backoff and the handoff hysteresis are its knobs.
- **Control journals** of a multi-region cell (the cell tenant's, the election journals, the
  tenant control journals) follow 2/2/1, the witness holding one acceptor, and the coordinators
  live in the two full regions. A matchmaker quorum, two of three, needs one remote matchmaker
  per campaign, which is rare.
- **A region loss** leaves a multi-region cell serving, with its leaders in the surviving full
  region; an AZ loss leaves it serving as it does a regional cell.

```
 ┌─ region W ──────────────────┐ ┌─ region C ──────────────────┐ ┌─ region N (witness) ─┐
 │ frontends T (one per AZ)    │ │ frontends T (one per AZ)    │ │ az-a: acceptor n1    │
 │ proxy leaders T(W)          │ │ proxy leaders T(C)          │ │       matchmaker m3  │
 │ batchers T(W), coordinator T│ │ coordinator of cell MR1     │ │ no frontend, replica,│
 │ az-b: acc w1 (LEADER), m1   │ │ az-b: acc c1, m2            │ │ proxy leader, batcher│
 │ az-c: acc w2, replica r1    │ │ az-c: acc c2, replica r2    │ │ or coordinator       │
 └─────────────────────────────┘ └─────────────────────────────┘ └──────────────────────┘
 write from W: w1, w2 + the fastest of c1, c2, n1 = one RTT to the nearest second region
 read from C:  replica r2 asks 3 of 5 for watermarks, so it crosses a region
 region W lost: c1, c2, n1 = 3 of 5, zero slack, a leader elected in C
 region N lost: 4 of 5, writes unchanged
```

**Hierarchical quorums are deferred** (decided on 2026-10-07, #253). `QuorumSystem::Zoned { fz, fn }`
in WPaxos's floor form (Phase 1: `fn + 1` nodes in each of `Z - fz` zones; Phase 2: `l - fn`
nodes in each of `fz + 1` zones) is a legitimate later variant, deferred until the simulation
runs nine acceptors. On three regions of three AZs it gives quorums of four instead of a
majority of five: the same round trip to the nearest region, one ack fewer, and a different
tolerance shape (better against a region plus one straggler in each survivor, worse against two
losses inside one region). It trades acks and tolerance, never latency. The printed WPaxos
set-builder definition does not intersect: on the paper's Figure 3b grid, with `Z = 4`, `l = 3`
and `fz = fn = 1`, `q1 = {A1, A2, B1, C1, D1, D2}` and `q2 = {A3, B2, C2, D3}` satisfy the
printed cardinality and per-zone cap, yet share no node. The printed form only bounds a zone's
contribution from above, while Lemma 1's proof uses exact counts, which is the floor form.

## 6. Verification

Simulation is the investment. Every milestone lands with its share of:

- Invariants in the audit, where the fact arrives: a leader uuid leads only from a `SetLeader`
  won in the log, every verdict naming the leader in force (through a misbehaving client's
  reinstatement too, section 2.3), every leadership change present in the log, `seq` dense per journal, a
  single-writer `Write` never re-accepted with other bytes, `Truncate` monotone and `first_seq`
  never above a served cursor, a single-writer `Truncate` accepted only from the current leader, a
  `SetLeader` winning at most once per `old_uuid` in a linearization, a multi-writer journal never refusing an
  in-limits write, a capacity slot booked at most once (a booking id never booked twice, across
  checkpoints), a tenant never reaching a journal outside its own
  `TenantId`, no component reaching a journal by an id it did not learn (the simulation draws
  every `JournalIdentifier`, the system tenants' included, per seed).
- Control-plane invariants: a child keeps serving through its parent's outage; folding from a
  checkpoint yields the same state as folding the full history; the universe directory equals the union
  of the cells' tenant lists (assignments and counts), checked even with one cell (FDB's
  metacluster consistency checker); every universe operation resumes correctly when re-run after a
  crash at any step; an election never has two leaders whose writes both land, and settles on one
  leader after the chaos window (a liveness oracle in recovery mode).
- A real linearizability checker in the workload's `check()`, over the four-call history with
  `Ambiguous` outcomes, against the sequential model of a journal (a mode, a leader, a dense log,
  a floor). It replaced the per-operation rules of `ClientHistory` (#205).
- The three races made likely rather than lucky, each a knob or an inline `buggify!` at its own
  site, with its reachable: a `SetLeader` drawn in the middle of a pipelined burst, a client
  timeout shorter than the ack so a retry crosses an ownership change, a `Truncate` racing a
  reader's cursor.
- Control-plane shapes: one cell hosting the universe tenant, now; a crash at each step of `init` and of tenant
  creation, each step with its own reachable; a crash inside a checkpoint run and between its
  `End` and its truncate, for the registry and for the universe tenant; a truncate refused from a stale leader; the universe tenant
  unavailable while tenants serve; a coordinator killed mid-operation and its successor finishing
  it. In M12: a second cell, a tenant move and a move of the universe tenant (a crash at each phase, then
  a cell with a stale universe pointer still reaches it). With recovery (deferred): a lost cell
  control quorum recovered by `init --recover`.
- Storage chaos on the shipped stores: every role runs on moonpool-journal (landed, #176, #261),
  with the ledgered journal-aware injector aimed through `Journal::regions` (striped by slot)
  under the copy budget, power losses inside a sync (the stores' own `hint!`s, under the same
  budget, #294), and moonpool's environmental storage chaos
  under it; its gates name journal verdicts (slot rebuilt, double fault parked, meta repaired).
  The in-memory stores and the scripted corpus are gone: one campaign (#263, 2026-10-08). The
  corpus's shapes are provoked, never scripted: a correlated outage of every acceptor plans one
  slot's loss for each holder's next boot (aimed at the most recent slot, its holders, every
  copy, or leaving the clean copy on a removed node), and a loss budget's extreme lets at most
  two slots lose every clean copy. The audit recognizes the E1, bare-quorum and
  departed-straggler shapes from the journal's verdicts, excuses an unrecoverable slot's journal
  from convergence, and asserts it is never accepted again. Still to come: replicated fault
  patterns (#202).
- Zones (decided on 2026-10-07, #215): a zone label per process and a zone-kill attrition mode
  (one zone at a time, the outage length drawn across the re-placement bound and restored inside
  `CHAOS_DURATION_MS`); moonpool#297 gives `.processes()` groups no locality, so paros keeps its
  own label map meanwhile, an `upstream-to-moonpool` candidate; a latency model per zone pair
  (likely moonpool work); the zone-aware copy budget, where a zone counts as one fault; a composer
  that draws failure domains per seed and applies the zone rule, with a reachable for a refused
  shape; the client's zone and the frontend's origin stamp drawn per seed, the hysteresis knobs, a
  `sometimes` for a leader moved toward its writer's zone and a reachable for a move the
  hysteresis refused. Oracles: two nodes never evaluate one configuration under different failure
  domains (`assert_always!`); every chosen slot's Phase-2 voters span two zones, folded from
  `Accepted` and the failure domains into O(1) audit state; every journal kept committing through
  a one-zone kill (`sometimes`); a two-zone kill is a safety-only shape. M11 is a fault-model change: 10,000
  seeds, and the canary after every new draw.
- Regions and the resolver (decided on 2026-10-07, #253): the copy budget counts a region as one fault;
  in a multi-region cell every chosen slot's Phase-2 voters span two regions; a partition through
  the witness region, after which leadership settles (a liveness oracle in recovery mode); a
  client's two-hop resolution keeps succeeding through an outage of the universe tenant. In M12, with #253: the
  witness region, the per-region pools and a move between cell kinds.
- Setup under simulation (decided on 2026-10-09, #246): all of paros runs in the simulation,
  setup included. Simulated `parosd` machines run the shipped `paros::machine::run_machine`
  from an empty disk (format, a minted `node_id`, the machine record, the wait); the workload's
  `init` is `paros::client::initialize`, the code `parosctl init` prints; the machines it forms
  host the cell control journal and the universe tenant, and every other process learns their
  identifiers through `Inspect`, never by injection. Forming the cell is part of every run, never
  injected state: the seed draws the layout (machine count, classes, capacities, which machine
  receives `init`), when and how often `init` and the commands after it are sent, and every
  fault stays on through setup. The harness's own setup holds only what an operator does offline,
  outside paros (generating the Biscuit root key pair, #245). Until #190 makes the main journal plan data
  (the directory went with #210), the acceptors that serve them keep the per-seed harness draw
  (matchmakers, proxies, replicas, grids) beside the machines; after that they become machines
  too. A machine's disk can be wiped: the machines' attrition draws moonpool's `CrashAndWipe`,
  and a per-seed scenario aims it at a founding member in the middle of `init`. The machine at
  that address is a new machine with a new `node_id`, and it never rejoins as the old one.
  When no vote names the old machine, `init` forms the cell over the new one. When a vote names
  it, the founding members that kept their disks choose that plan (`q1 = n`, `q2 = majority`,
  section 3.1), and the cell forms with the old id as a dead member. Only when a majority of the
  plan's members are wiped is the cell lost (`cell_lost`) until an operator acts outside paros;
  the control-plane liveness oracles do not apply to a lost cell. The "no
  unlearned id" oracle holds on every run: an operator's call to the cell names only a
  `JournalIdentifier` that it learned from `init`'s reply or through `Inspect`.
- Faults in the shipped code (decided on 2026-10-09, #294): the simulation runs the same machine
  and disk as `parosd`, with no sim substitute. A choice the code makes is an inline buggify. A
  moment where an environmental fault is interesting is a hint: the code names the moment
  (`hint!("batch durable, not sent").await`, FDB's `if (buggify()) throw please_reboot()`), and
  moonpool decides whether and how to strike, under the seed's attrition regime. Faults come only
  from these two forms; a `buggify_knob!` is a form of inline buggify. Both are inert in
  production. `paros-core` gets none. `DriverHooks`, `SimDisk`, `LedgeredJournal` and `PowerCut`
  go (`DriverHooks` went with #318); `Audit` stays as the one observation seam, for the facts a cross-node or harness oracle
  folds. A fact that only fires a gate is an inline `reachable!`/`sometimes!` instead, and the
  gate-only callbacks move inline site by site (decided on 2026-10-09)
  (`docs/analysis/simulation/production-fault-hints.md`).
- New BUGGIFY sites for every new decision the driver, the frontend, the resolver and the
  coordinators take, and the coverage-guided sweep saturating over them.

No new model checker and no separate specification: the two existing sans-IO model checkers
stay as they are.

## 7. What changes against today

Only what is still to change; landed changes (the four-call cut-over, the fenced `Truncate`, random
ids and the `JournalIdentifier`, start-and-wait plus `init`, the uniform `parosd`, the retired
read-index path and the unset `JournalIdentifier`s that meant "the first journal", #243; the
universe tenant, `JournalIdentifier` and proxy renames in the code, #244) are in the history and
AGENTS.md.

- The `(generation, owner)` pair of M7 becomes a single 128-bit leader uuid, compare-and-set by
  `SetLeader(new, old)`, with a hidden term counter in the core; `Write` takes an explicit
  `expected_seq`; journals gain a writer mode, single or multi (section 2, #241). No compatibility
  layer: `parosctl --owner` becomes `--leader`, and the chain workload's alphabet, the
  linearizability model and the audit follow. The single-writer half (PR #281), the writer mode
  (PR #296), the batch limits and the read limits landed (#241 is done); `parosctl journal
  create --mode` landed with #210.
- Landed (#210): the directory is gone. Every hosted tenant has its own control journal: its
  identifier is drawn with the tenant, recorded in the universe directory and in the cell's
  `HostTenant` (with the tenant's name and `survives`), and it is born on the founding members,
  plain Multi-Paxos, majority. It holds the tenant's description (no rendezvous name) and every
  journal create and delete, each a request with an idempotency id judged at apply
  (`paros::tenant`). Until #212 and #225 the elected cell coordinator is the tenant coordinator
  of every hosted tenant: it fences each tenant control journal with its term uuid, answers
  `JournalRequest` at its published interface, and places journals on the founding members.
  Every machine follows the cell control journal and every hosted tenant's control journal
  (`ControlPlan`), and provisions a created journal at its first open (the machine record's
  `created` line is the commit point). `init` creates no hidden journal. Still to change: the
  acceptors' harness registry in the simulation (a `ControlPlan` over the genesis pool) becomes
  the machines' when placement lands (#211, #212), and capacity becomes the cell coordinator's
  alone.
- `parosd` stops running the plain deployment: every journal is born with its matchmaker set, and
  journal-tagged matchmaker planes replace "only the first journal of a process" (#190); proxy leaders
  and replicas follow (#193).
- The coordinators replace the operator's client (#212): admin calls become requests to the
  elected coordinator. Since #240, `parosctl init` no longer claims the cell control journal: the
  elected cell coordinator installs its uuid there.

## 8. Milestones

Milestones are labels (`milestone:M7` and up); `milestone:M6` stays the epic's umbrella. The
toy is the end of M9. The epic is #184, the backlog pointer #69, the verification track #24.

| Milestone | Name | Content |
|---|---|---|
| M7 | Journal API (#204, #205) | the four calls, the journal state machine in core, the wire and the driver, the chain workload's alphabet, the linearizability checker, the race knobs and BUGGIFY sites, the cut-over |
| M8 | parosd deployable (#206 to #209, #221, #220, #196, #201) | Tokio providers linked, the stores on a real filesystem for the first time, the `JournalStores` opener, `Config` durable at `format`, `parosd provision` (replaced by `init` in M9), the uniform binary with class and capacity, Compose, `paros::client` (#221) and the `parosctl` CLI (#220), a tracing subscriber, exit codes |
| M9 | The universe with one cell (#225, #226, #227 and #216 first; landed: #228, #235, #229, #230, #211's core, and #176, #261, #263, #264 (PR #266), #267; sim first: #202, #213, #246, #247, #248; then #241, #243, #244, #240, #210, #239, #190, #212, #192, #245, #191, #211, #213, #252, #257) | the control hierarchy and its decisions, the fenced `Truncate` on the wire, random ids and the `(TenantId, JournalId)` `JournalIdentifier`, the leader-uuid API and its two writer modes, `init` creating the universe with its matchmaker sets, the cell tenant and its machine registry with role slots and liveness, the universe tenant with its directory and tenant creation state machine, the election library and the coordinators it runs, requests to a leader, placement inside capacity granted by the cell, the checkpoint-and-truncate library, names at the frontend, the frontend with Biscuit `Authz` routing through the universe tenant, per-tenant matchmaker sets, `parosctl status` |
| M10 | Roles per tenant (#193, #214, #194, #145, #195) | journal-tagged proxy leaders and replicas, batchers and unbatchers for multi-writer journals, tenant modes (redundancy, grid, role counts) applied by the tenant coordinator, quotas, the benchmark, then scale work |
| M11 | Zones (#215) | `(region, az)` `FailureDomain`s in `AcceptorConfig` with its `cell_id` (one format bump), the two-predicate zone rule, zone round-robin placement, the `single` exemption, the leader following its writer's zone, zone-kill attrition and a zone-aware budget in the simulation, zone-spread matchmaker sets |
| M12 | Multiple cells (#232, #233, then #253) | adding and removing cells with tombstones, placement across cells by `kind` and `survives` (both carried since M9 with the cell's entry endpoint), tenant locks, moving tenants and the universe tenant between cells, splitting the universe tenant by range, the resolver beside the frontend (section 3.5); last, multi-region cells: the `MultiRegion` cell kind with its witness region, the two-level zone rule, pools per `(tenant, region)`, the universe tenant hosted in a multi-region cell, the partition through the witness in the simulation, moving tenants between cell kinds |
| M13 | Upgrades and network compatibility (#363) | decided on 2026-10-10: a very late milestone. A wire version in the RPC handshake, a cell-wide active version in the cell control journal, features that turn on only when every machine supports them, rolling upgrades one machine at a time, a downgrade by one version, mixed-version cells in the simulation. This M13 is new: the earlier M13 (multi-region cells) went into M12 on 2026-10-09 |

Verification is not a milestone: every milestone carries its own share of section 6. M9 opens
with a **simulation-first phase** (decided on 2026-10-04): storage chaos on the shipped stores
(#176, #202), three founding members (#213), the `parosd` machine lifecycle simulated as shipped (#246), the
control-plane oracle debt (#247) and TigerStyle assertions (#248) rank ahead of M9's features. Recovery (#231, section 3.10) is
deferred and carries no milestone yet. The interactive game and the lessons (`track:play`, #162,
#163) run outside the milestones, behind the service.

## 9. The toy, done means

The Compose toy is the user's demo, for running paros by hand, and is no part of the test suite
(decided on 2026-10-04): CI only checks that the image builds, and behaviour is proved by the
simulation. Its machines are plain nodes (`node1`..`node3` over three failure domains, `storage4`,
`stateless1`); `parosctl init --members` founds the cell on the first three, and an admin
admits the others with `cell add-machine`. Its journals run
`double`: four `storage` machines cannot hold `triple`'s five acceptors on distinct machines, so
the toy cannot run `triple` (decided on 2026-10-07, #215).

From a fresh clone: `docker compose up`, then `parosctl init` against `node1`, which creates the
universe, its one cell and the universe tenant. Generate a root key and mint an `admin` token offline with `parosctl`, then create a tenant and
mint its `tenant` token offline. Create a
journal. `write`, `read` and `tail` from `parosctl`, addressing `acme/orders`. `set-leader` to a second
client and see the first one refused, for a `write` and for a `truncate`. Create a multi-writer
journal and append to it from two clients at once. Kill one `storage` and one `stateless`
container and keep writing. Wipe one volume, see the amnesia refusal, and see the journal healed
by reconfiguration onto another machine. Stop the universe tenant's quorum and keep writing to an
existing tenant. Kill the cell coordinator's machine and see another one elected. `parosctl
status` shows desired, available and current, and the role slots, per machine, per tenant, per
cell and for the universe. (Recovery of a lost cell control quorum is deferred, section 3.10.) The simulation is green in every shape and the
coverage-guided sweep saturates.

## 10. Sources

The AWS Journal, as publicly described:

- Brooker, "MemoryDB: Speed, Durability, and Composition",
  <https://brooker.co.za/blog/2024/04/25/memorydb.html>: the API evolution `write` and `read`,
  then lease-fenced writes, then `set_leader` as a compare-and-swap.
- Bowes, "The Adjudicator", <https://marc-bowes.com/dsql-adjudicator.html>: a superseded writer's
  commit is rejected; pipelining by expected sequence number, a refusal cascading to the writes
  behind it; generations as a later precondition.
- Bowes, "Meet Coupler", <https://marc-bowes.com/dsql-coupler.html>: journals added and removed
  transparently; every consumer a tailer.
- Brooker, "DSQL Vignette: Transactions and Durability",
  <https://brooker.co.za/blog/2024/12/05/inside-dsql-writes.html>: the journal stores decided
  outcomes, never proposals; durability is the journal's commit.
- Brooker, "Why Strong Consistency?", <https://brooker.co.za/blog/2025/11/18/consistency.html>:
  strictly monotone per-journal streams plus a reader that waits until caught up are the whole
  consistency mechanism.
- Brooker, "Wait! Isn't That Impossible?",
  <https://brooker.co.za/blog/2024/12/06/inside-dsql-cap.html>: the adjudicator holds no durable
  state and is rebuilt from the committed transactions, so it moves cheaply to the majority side;
  the witness region holds only a copy of the Journal; after a region loss every commit waits on
  two of two.
- Brooker, "Control Planes vs Data Planes", <https://brooker.co.za/blog/2019/03/17/control.html>:
  what belongs on the request path and what scales with the universe.
- Amazon MemoryDB, SIGMOD 2024,
  <https://cdn.amazon.science/e0/1b/ba6c28034babbc1b18f54aa8102e/amazon-memorydb-a-fast-and-durable-memory-first-cloud-database.pdf>:
  §4.1 conditional append and leadership as one more conditional append, the lease and
  self-demotion; §7.2.1 snapshot verification by a checksum carried in the log.
- Aurora DSQL, arXiv 2607.13276, <https://arxiv.org/html/2607.13276v2>: §5 the journal's
  precondition on timestamp monotonicity, §6 TLA+ and P then deterministic simulation; §5.4 the
  Journal replicates inside a region with a variant of chain replication across AZs and across
  regions with a variant of Paxos (two of three). paros keeps quorum writes everywhere, inside a
  region too, by choice: a slow or dead acceptor costs nothing until reconfiguration, where a chain
  stalls until its membership is changed (section 11). Transcript:
  `docs/references/papers/aurora-dsql/transcript.md`.
- Demirbas's MemoryDB summary,
  <http://muratbuffalo.blogspot.com/2024/05/amazon-memorydb-fast-and-durable-memory.html>, and
  the 2025 Journal reconstruction,
  <https://ajalab.github.io/posts/2025-08-14-journal-distributed-log-replication-behind-aws/>:
  secondary readings; the latter states plainly that retention and truncation are undocumented.
  It also reports, from re:Invent 2024, that the Journal replicates to three AZs or regions,
  commits on two of three regions, performs reads from at least two regions, and has the third
  region relay a write between a partitioned pair; it notes that what a region member is made of
  is undocumented.

Cells and static stability:

- AWS Well-Architected, "Reducing the Scope of Impact with Cell-Based Architecture",
  <https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/what-is-a-cell-based-architecture.html>:
  cells contain overload and bad deployments and are not a failover domain; multi-AZ cells; the
  thinnest possible router routing on its cached map; range-based mapping; migration as copy,
  flip, redirect, forget; multiple cells and migration from day one; shuffle sharding inside a cell
  (its FAQ). Transcript: `docs/references/papers/aws-cell-based-architecture/transcript.md`.
- Amazon Builders' Library, "Static stability using Availability Zones",
  <https://aws.amazon.com/builders-library/static-stability-using-availability-zones>: a data plane
  that keeps serving while its control plane is down.
- FoundationDB's metacluster, deleted with multitenancy in apple/foundationdb commit `bab7637d8`
  on 2025-12-09 as an unowned, unfinished community contribution, not for a design failure; read
  at the parent commit, <https://github.com/apple/foundationdb/tree/bab7637d8%5E/metacluster>:
  `metacluster/include/metacluster/MetaclusterTypes.h` (tenant and cluster states, tenant groups,
  tenant locks), `MetaclusterMetadata.h` (the id prefix, the capacity index, tombstones),
  `CreateTenant.actor.h` (the resumable creation state machine), `RestoreCluster.actor.h` (restore
  in both directions, dry run, the restore id), `MetaclusterConsistency.actor.h` (the consistency
  checker) and `fdbclient/include/fdbclient/MetaclusterRegistration.h` (registration on both
  sides, verified on every step).

Bootstrap, identity and membership:

- CockroachDB, `cockroach init` and `cockroach start`,
  <https://www.cockroachlabs.com/docs/stable/cockroach-init>,
  <https://docs.cockroachlabs.com/docs/stable/cockroach-start>: start-and-wait plus a one-time
  init, `--join` stored and still passed on every start.
- Redpanda, `empty_seed_starts_cluster`,
  <https://docs.redpanda.com/docs/reference/node-configuration-sample>: implicit formation disabled
  in production.
- Kafka KIP-899 and KIP-1102, client rebootstrap,
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-899:+Allow+producer+and+consumer+clients+to+rebootstrap>,
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1102:+Enable+clients+to+rebootstrap+based+on+timeout+or+error+code>.
- Akka cluster membership, <https://doc.akka.io/docs/akka/current/typed/cluster-membership.html>:
  `host:port:uid` identity, a new incarnation at the same address removes the old member.
- Orleans cluster management,
  <https://learn.microsoft.com/dotnet/orleans/implementation/cluster-management>.
- Balakrishnan et al., "Virtual Consensus in Delos", OSDI 2020,
  <https://www.usenix.org/system/files/osdi20-balakrishnan.pdf>: clients refresh their view only
  when an append fails on a sealed loglet.
- Suresh et al., "Rapid", USENIX ATC 2018,
  <https://www.usenix.org/conference/atc18/presentation/suresh>.
- ScyllaDB, node-failure recovery,
  <https://docs.scylladb.com/manual/stable/troubleshooting/handling-node-failures.html>: host-id
  identity, the cluster id in gossip, the recovery leader picked by host id.

Checkpoints:

- Apache Pulsar PIP-14, "Topic compaction",
  <https://github.com/apache/pulsar/wiki/PIP-14:-Topic-compaction>, and the topic compaction docs,
  <https://pulsar.apache.org/docs/concepts-topic-compaction>: the compacted ledger and the
  compaction horizon.
- Kafka KIP-630, "Kafka Raft Snapshot",
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-630:+Kafka+Raft+Snapshot>, and KIP-876,
  "Time based cluster metadata snapshots",
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-876:+Time+based+cluster+metadata+snapshots>.
- Redpanda architecture, the controller partition and its snapshots,
  <https://docs.redpanda.com/current/get-started/architecture/>.
- Balakrishnan et al., "CORFU: A Shared Log Design for Flash Clusters",
  <https://www.cs.fsu.edu/~awang/courses/cop5611_s2024/corfu.pdf>, and the NSX / CorfuDB
  troubleshooting note on checkpoint failures for large tables,
  <https://knowledge.broadcom.com/external/article/378470/troubleshooting-vmware-nsx-datastore-cor.html>.

Zones:

- Ailijiang, Charapko, Demirbas, Kosar, "WPaxos: Wide Area Network Flexible Consensus", IEEE
  TPDS 2019, <https://arxiv.org/abs/1703.08905>: §3.1 the per-zone quorums `fz`, `fn`; §3.2 to
  §4 object stealing; §5.1 the majority-zone leader policy; §5.3 degraded operation and
  reconfiguration. The printed TLA+ quorum definition does not intersect; only the floor form the
  proof uses is sound (the counterexample is in section 5). Transcript:
  `docs/references/papers/wpaxos/transcript.md`.
- Nawab, Agrawal, El Abbadi, "DPaxos: Managing Data Closer to Users for Low-Latency and Mobile
  Applications", SIGMOD 2018, <https://www.nawab.me/Uploads/Nawab_DPaxos_SIGMOD2018.pdf>: leader
  handoff, adopted in a stricter form (`docs/analysis/consensus/dpaxos-leader-handoff.md`);
  expanding quorums, which Matchmaker Paxos already provides with the intent made durable at the
  matchmakers before Phase 1 (the matchmakers' watermark is the intent's GC); delegate and
  leader-zone quorums, rejected (section 11). Transcript:
  `docs/references/papers/dpaxos/transcript.md`.

Tokens:

- Biscuit, <https://www.biscuitsec.org/>, and `biscuit-auth` 6.0.0
  (<https://github.com/eclipse-biscuit/biscuit-rust>, read at `01778d2`): offline attenuation,
  Datalog authorization, the seeded-RNG constructors, the wall-clock `RunLimits::max_time`
  (`datalog/mod.rs`), `AuthorizerBuilder::time()` reading `SystemTime::now()`, and the
  `HashMap`-backed fact and rule sets.

Compartmentalized Paxos and Matchmaker Paxos are in `docs/references/papers/`.

## 11. Alternatives considered

- **Recovering a control quorum by surgery.** Kafka KRaft is adding an override-voters flag to
  re-bootstrap a controller quorum that lost its majority, with data loss; ScyllaDB has a recovery
  procedure where an operator picks a recovery leader by host id. paros needs neither, because its
  control journals are rebuilt from below (section 3.10).
- **One cell playing both roles.** FDB's metacluster made a cluster exactly one of standalone,
  management or data, never two. paros co-locates the universe tenant with tenants in one cell because the universe tenant is a
  tenant with its own coordinator and quorums, which FDB could not do (its management data lived
  in the system keyspace). Moving the universe tenant to a dedicated cell is the escape hatch.
- **Multiple cells from day one.** The AWS guidance recommends multiple cells and migration from
  day one. paros runs the universe machinery from day one with one cell and defers the second cell
  to M12; the fields of section 3.7 keep that deferral free of protocol changes.
- **Checkpoints as local snapshot files per replica** (Kafka KIP-630, Redpanda): rejected,
  coordinators are stateless. KIP-630 rejected snapshots in the log for bandwidth, since the
  leader replicates them to every replica; that argument is weak for paros, whose control state is
  small. Key-based log compaction is rejected for KIP-630's reasons (compaction drops deletion
  markers on a timer, so replicas can diverge; metadata is mostly events) and because `paros-core`
  would have to understand keys. Corfu-style per-entry trim plus copying live entries forward is
  kept as the future answer if write amplification is ever measured as the bottleneck, since it
  changes `paros-core`'s read semantics; the decision is #227.
- **A `pinned` flag per tenant** (2026-10-04, dropped the same day). A creator's pin would block
  rolling a cell out by evacuation; the only tenant that must stay is a cell tenant, and its
  `cell` group says so (section 3.7).
- **A provisioning step that names the seeds to each other**, a cluster file, gossip discovery and
  the frontend as the rendezvous: #216.
- **Seeds, a join list and a rendezvous name** (2026-10-02 to 2026-10-09, #216). Every machine
  carried the seed list and `init` had to target a seed; being a seed meant nothing after
  formation, and "seed" collided with the simulation's seed. Replaced by founding members named
  in `cell init` and admin-admitted machines and cells (section 3.1).
- **The `(generation, owner)` pair** (M7, #204; replaced on 2026-10-04). Two fields where one
  fence suffices, and an owner id the caller chose, so two processes could share it and both pass
  the owner check once they read the public generation. A per-term random leader uuid is
  Brooker's final MemoryDB API and cannot be shared by accident; the term counter stays, hidden
  in the core.
- **Refusing a reinstated leader uuid** (2026-10-09, #241). Remembering every uuid that ever led
  (a `term → uuid` map carried with the trim point) would make `A → B → A` impossible even for a
  misbehaving client. Rejected: `SetLeader` names both uuids, so the client that reinstates one
  does it on purpose, the state each trim point carries would grow with every term, and the
  simulation shows the journal's guarantees hold through it (section 2.3).
- **A lease in the journal** (MemoryDB's lease-fenced writes). Rejected: a lease fence depends
  on clocks and pauses; the leader uuid fences without either, and the election library keeps a
  lease only as a liveness hint (section 3.3).
- **JWT** for frontend tokens (chosen on 2026-10-04 morning, replaced the same day, #245).
  Roles become ad-hoc claims checked by hand, a token returned at tenant creation needs a signing
  key at the frontend or a call to an external issuer, and a holder cannot narrow its own token.
  Biscuit gives roles as Datalog facts, attenuation without the root key (tenant creation, users
  narrowing their tokens offline) and offline minting. JWT stays the way to plug an external
  identity provider in later, as a second `Authz` implementation or a token exchange.
- **Well-known system ids** (universe tenant `1`, cell tenant `2`, every control journal `1`,
  `0..=255` reserved; decided on 2026-10-02, reversed on 2026-10-04). They let a component find a
  control journal without asking, but every component must then agree on the convention forever,
  the cell tenant's id repeats in every cell (unique only within its cell), a rebuilt control
  journal reuses its predecessor's id, and the reserved range is a second id space every check has
  to special-case. Random ids recorded where they are created, and learned from any machine of
  the cell, cost one `Inspect` at bootstrap.
- **A single entry role** (rejected on 2026-10-07, #233): a per-tenant role cannot find the cell
  of a tenant it does not serve, so the first hop needs a universe-level answerer, the resolver.
- **A resolver folding every cell's registry** (rejected on 2026-10-07, #233): one hop instead of
  two, but the resolver is no longer thin, no cell stays statically stable without it, and it makes
  every cell depend on a universe-level role.
- **Chain replication inside a region** (rejected on 2026-10-07, #215), the variant DSQL's journal
  runs across AZs: paros keeps quorum writes everywhere, because a slow or dead acceptor costs
  nothing until reconfiguration, where a chain stalls until its membership is changed.
- **Cell-scoped integer tags** (rejected on 2026-10-07, #215): not comparable across cells during
  a move (section 5).
- **Vote-only witnesses** (rejected on 2026-10-07, #253): a vote without bytes is a guaranteed `faulty`
  from Phase 1's view and contradicts "a server answers only from records it holds" (section 2.5).
- **Two regional cells replicating to each other** (rejected on 2026-10-07, #253): a dependency
  across cells, and a tenant lives in exactly one cell (section 3.7).
- **Lease reads** (rejected on 2026-10-07, #253): paros enforces no lease (section 2.3).
- **DPaxos's expanding quorums and leader zones** (rejected on 2026-10-07): a second Paxos kernel,
  and leader-zone quorums contradict the zone rule of section 5; Matchmaker Paxos already expands
  quorums with the intent durable before Phase 1.
- **WPaxos's joint reconfiguration** (rejected on 2026-10-07): matchmakers reconfigure
  (section 4.1).
- **Hierarchical `Zoned` quorums** (deferred on 2026-10-07, #253): a later `QuorumSystem` variant once the
  simulation runs nine acceptors (section 5).
