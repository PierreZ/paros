# paros: the journal service

This is the end goal. AGENTS.md describes what paros is today and the doctrine every change
follows; this document describes what paros is becoming, so that every issue, plan and session
aims at the same target. Where the two disagree, AGENTS.md is the present and this is the
direction. Decided on 2026-09-30; the fleet, the control hierarchy, identifiers, checkpoints,
recovery and the fenced `Truncate` decided on 2026-10-02; the leader-uuid API with its two
journal modes, election over a journal, the request channel, liveness, names, trust, capacity as
role slots, journals born with their matchmaker set, tenant modes, the data-plane limits, storage
chaos in simulation and the deferral of recovery decided on 2026-10-04. The milestones at the end
carry the issue numbers.

## 1. Goal

`parosd`: one binary on N machines, started with `docker compose`, serving journals to tenants
over a four-call data plane, healing itself through reconfiguration when a disk or a machine is
lost. Single region, several failure domains.

The control plane is itself stored in journals and coordinated through the same election
primitive the tenants use: paros eats its own food. It has four levels, all built the same way:
one actor elected over an election journal and installed as its control journal's leader with
`SetLeader`, holding no state that journal does not hold (section 3.3).

| Level | Elected actor | Its control journal holds |
|---|---|---|
| Fleet | the fleet coordinator (the fleet tenant's coordinator) | tenant → cell, cell entries |
| Cell | the cell coordinator (the cell tenant's coordinator) | machine registry, capacity bookings |
| Tenant | the tenant coordinator | its name and desired state, journal names, placement inside capacity granted by the cell |
| Journal | the client leader (single-writer) or any writer (multi-writer) | the data |

A paros deployment is always a fleet, and the fleet runs from M9. For now it has exactly one
cell, and that cell plays both roles: it hosts the fleet tenant (the fleet level) and it is an
ordinary cell holding tenants. Every fleet behaviour exists and is exercised from M9: `init`
creates the fleet, tenants are created through the fleet tenant, routing resolves tenant → cell through
the fleet directory, registrations are verified on both sides. The answer is always "this cell"
today, but the code path is real and runs in every simulation. A second cell, moves between
cells and a separate router are M12 (section 3.7). The test for every design until then: adding
a second cell adds an entry to the fleet directory and a routing choice, never a protocol or
data-model change.

**Every tenant can be transferred** (decided on 2026-10-04): the design must be able to move any
tenant to another cell, the fleet tenant and its fleet coordinator included, with no protocol or
data-model change and without stopping the tenants that do not move. Nothing may assume a tenant
stays in the cell it was born in. The reason is **rolling out a cell by evacuation**: stand up a
new cell, move every movable tenant onto it, then retire the old cell. The one tenant that never
leaves its cell is a cell's own cell tenant, because it *is* that cell (its registry and its
bookings): served from another cell it would make the cell depend on a foreign control plane,
break static stability and cross the blast-radius boundary, and after a rollout it would describe
machines that no longer exist. It is still reconfigured within its own cell (a dead seed replaced,
a machine drained), and it retires with its cell; the new cell has its own from its `init`. The
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

The first deliverable is a toy: a local Docker Compose cluster an operator can initialize, create
a tenant on, write to, read from, break and watch heal. The homelab and anything beyond one region
are not in scope.

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
| `Read(from_seq, limit, wait_ms?)` | The committed records from `from_seq`, plus `first_seq`, `next_seq` and the current leader uuid, or `Truncated` when `from_seq < first_seq`. Long-polls at the tail for `wait_ms`. | The same. |
| `Truncate` | `Truncate(leader_uuid, up_to_seq)`: fenced like `Write`. | `Truncate(up_to_seq)`: anyone may truncate. |
| `SetLeader(new_uuid, old_uuid)` | Compare-and-set the leader. Returns `{ old_uuid, next_seq, first_seq }`. | Refused: a multi-writer journal has no leader. |

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
change as an ordinary log entry and answers `{ old_uuid, next_seq, first_seq }` so the new leader
can continue the sequence. Every tailer learns the leader changed in-band, without a side channel.

**The leader uuid is the fence.** It is a 128-bit random value the leader draws for one
leadership term, never per process: a process that wins again draws a new uuid, which fences its
own older in-flight writes. It is not a secret and not an authentication token; the proxy
decides who may touch a tenant at all (section 3.5), the leader uuid decides which of the
tenant's clients holds the pen. The core keeps a hidden term counter beside it, raised by every
`SetLeader`, so a uuid that ever led can never lead again (no ABA through
`SetLeader(old, current)`); the counter never leaves `paros-core`.

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

### 2.5 Reads

`Read` is served by whatever holds the journal's replica state — the colocated node by default,
the replica tier when the tenant asks for replicas — and always through the leaderless read of
Compartmentalized Paxos §3.4 (paros's `QuorumRead`): the server asks a Phase-1 quorum of the
acceptors for their vote watermarks, waits until its own applied prefix covers the maximum, and
answers. No read goes through the Paxos leader, so reads scale with replicas and cost the
acceptors one watermark round per page (decided on 2026-10-04). The read-index path and
`CheckTail` retire.

A server answers only from records it holds (decided on 2026-10-06). A server whose floor rose
ahead of its fold — it jumped to a peer's trim point, and the `Truncate` that let the peer's
floor rise lies above it, not yet applied here — still counts records below its floor in the
journal. A read there is answered **unserved**, the same answer as a read whose confirmation
timed out, and the client asks another server: never `Truncated` (the journal still has the
record) and never a page (this server cannot produce it).

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
(records and bytes), and a maximum `wait_ms`. Each value is a tunable with a documented floor
and a `buggify_knob!` in simulation; the toy's values are today's (a 256-record, 64 KiB page, a
long-poll of a few hundred milliseconds, a batch well under the 4 MiB RPC frame). An `Inline`
checkpoint (section 3.9) must fit one batch. Every id, `seq` and the term counter are `u64`; the
leader uuid is 128 bits.

### 2.8 Underneath

Paxos is unchanged: replication, holes, gap fills, the contiguous chosen prefix, CTRL, the trim
point, matchmaker reconfiguration. Every proof paros has holds per journal; the one new thing to
prove is the control state machine's rules, and they are judged in the simulation like every
other rule.

## 3. The control plane

### 3.1 The cell tenant and bootstrap

A cell's machines and capacity are the control journal of the **cell tenant**, served by the
same `parosd`s, the same Paxos and the same stores as every tenant's journals. Its coordinator is
the **cell coordinator**. Today's system journals are dissolved into the four levels: the machine
registry and the capacity bookings are the cell's control journal; the directory splits into
the fleet directory (tenant → cell, the fleet level) and each tenant's own control journal; desired
state moves into the tenant control journals. System journals are written with `Write` like any
journal; there is no special path.

Every tenant gets a control journal when it is created. It is **self-describing**: it holds the
tenant's name and desired state, its journal names and its placement, so every index above it
(the cell's list of hosted tenants, the fleet directory) can be rebuilt from it (section 3.3).

**Bootstrap: start and wait, then one `init` that creates the fleet** (decided on 2026-10-02,
#216). There is no provisioning step that names the seeds to each other.

- The seeds start with the same rendezvous name or short join list and wait.
- `parosctl init` is sent to one of them, which every seed's join list must name. It needs every
  seed to answer, and it is refused if the cell is already initialized. In order:
  1. **forms the cell**: mints `cell_id` and the cell tenant's `JournalIdentifier` (its random `TenantId` and
     the random `JournalId` of its control journal, section 3.8), writes them into every seed's
     durable cell plan, starts the cell's first matchmaker set on the seeds, and the first cell
     coordinator installs itself as the cell control journal's leader with
     `SetLeader(its fresh uuid, unset)`;
  2. **creates the fleet tenant** inside that cell (the fleet tenant is a tenant, so the cell must exist first
     to grant it capacity): mints `fleet_id` and the fleet tenant's `JournalIdentifier`, both random, recorded in the cell
     plan of the cell that hosts the fleet tenant;
  3. **registers the cell** as the first entry in the fleet directory, and writes the matching
     registration on the cell side (section 3.7).
- Each step is idempotent and is a step of the fleet's operation state machine (section 3.7):
  re-running `init` after a crash resumes it.
- The other seeds join through the rendezvous call.
- **No implicit formation.** A node with an empty, uninitialized store waits indefinitely. It
  never forms a cell or a fleet on its own, including when the rendezvous resolves to a wiped
  node.

This is CockroachDB's `cockroach init`: nodes start with the same `--join` list and wait, a
one-time init sent to any of them bootstraps the cluster, init is refused on an initialized
cluster and must target a node every join list names. Redpanda recommends disabling
`empty_seed_starts_cluster` in production for the same reason paros has no implicit formation.

**Every journal is born with its matchmaker set** (decided on 2026-10-04). The cell control
journal and the fleet tenant's control journal are placed on the seeds and born with a matchmaker set that
`init` starts there; every tenant journal is born with its tenant's set. There is no
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
- `addr`: an attribute that may change across restarts.
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

**Liveness** (decided on 2026-10-04). The cell coordinator watches the cell's machines with the
transport's failure detector and writes only the *changes* into the cell control journal (`Down`,
`Up`), so control writes stay rare (section 3.9) and the registry's "seen alive" is a fold, not a
heartbeat log. A machine `Down` past the re-placement bound has its roles re-placed. A machine
never heartbeats into a journal.

**Finding the cell.** There is no cluster file. A machine's and a client's only static input is
one rendezvous name or a short join list, stored durably in the machine record and re-read on every boot
(CockroachDB stores `--join` in the data directory but still recommends passing it on every start,
so a node can rejoin after losing its data directory). With one cell, the fleet's rendezvous and
the cell's are the same name.

- A durable cache of the cell's seed set is tried first; the rendezvous is the fallback when no
  cached seed answers. Kafka's KIP-899 and KIP-1102 let clients rebootstrap this way, and verify
  the cluster id when they do.
- The durable cached registry fold plays the role CockroachDB gives gossip (node addresses off the
  consensus path): it lets machines find each other while the registry is unavailable. It is a
  static-stability requirement, not an optimisation.
- **No journal is found by convention** (decided on 2026-10-04, section 3.8): there is no
  well-known tenant or journal id. Every machine of a formed cell answers `Inspect` (and, later,
  the rendezvous call) with its `cell_id`, the cell tenant's control `JournalIdentifier` and, on the cell that
  hosts it, the fleet tenant's `JournalIdentifier`, all read from its durable cell plan. A client or an operator handed
  only addresses learns the control `JournalIdentifier`s from any machine, then resolves everything else
  through them: the fleet directory gives a tenant's cell and the `JournalIdentifier` of its control journal, and
  that control journal gives the tenant's journals. A re-run of `init` learns the cell's ids the
  same way. **An `Inspect` names its journal or asks for the node alone** (decided on 2026-10-05,
  #243): a node-only `Inspect` answers the machine's own facts — its `node_id`, its `cell_id` and
  the control `JournalIdentifier`s — and nothing about any journal, which is how a client handed
  only addresses starts; an `Inspect` that names no journal without asking for the node alone is
  refused (`unset`), never read as "the node's first journal", and one naming a journal the
  machine does not serve is refused (`unknown_journal`). The machine's own facts ride every
  answer, refusals included.
- `cell_id` and `fleet_id` are carried in the session `Hello`; a peer with another id is refused.
  ScyllaDB carries its cluster id in gossip for the same reason: nodes from different clusters
  cannot talk after a bad seed configuration.
- **Well-known endpoints** are the bootstrap set — `Identify`, `Init`, `FormCell`, `Inspect` —
  and the **rendezvous call**, keyed by tenant: "which references serve tenant T". Everything else
  is a dynamic reference (amended on 2026-10-04: the bootstrap calls were well known already).

The decision and its alternatives are #216. Classes are FDB's:

- `storage`: anything with a durable store. Acceptors, replicas, matchmakers.
- `stateless`: the proxy, proxy leaders, batchers, unbatchers, coordinators. The cell and fleet
  coordinators run on the seeds until a `stateless` machine registers (section 3.3).

**Capacity is role slots** (decided on 2026-10-04). A machine offers `capacity` opaque slots of
its class; one slot holds one role instance (an acceptor, replica, matchmaker, coordinator,
proxy, batcher or unbatcher) of one journal or tenant. A booking is keyed
`(tenant, journal or matchmaker set, role)` and holds one slot; only the cell coordinator writes
bookings. Bytes, IOPS and weighted roles are not modelled. Scaling a role for a tenant is adding
machines of the right class and raising the tenant's desired counts.

### 3.3 Coordinators and placement

The four levels of section 1 are built the same way (decided on 2026-10-02, #225): one actor per
level, the leader of that level's control journal, so it is fenced by its leader uuid like any
writer, and it holds no state that journal does not hold: a new one resumes from a fold. All four
exist from M9.

**Election over a journal** (decided on 2026-10-04, #240). Who leads is decided outside the
journal it governs, by one library, `paros::client`'s election, used by the cell, fleet and tenant
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

**Where candidates run** (decided on 2026-10-04). The cell and fleet coordinators campaign on the
seeds until a `stateless` machine registers, then move there; the cell coordinator places tenant
coordinators on `stateless` machines.

**Requests to a leader** (decided on 2026-10-04). Every control journal has one writer, so
anyone else — a tenant coordinator asking for capacity, an operator changing desired state or
draining a machine, a user creating a journal — sends a **request RPC to the elected
coordinator**, found through the `InterfaceRef` the coordinator publishes in its journal when it
wins. A request carries an idempotency id; the coordinator records the outcome in its journal,
so a retry that crosses a coordinator change finds the answer there instead of acting twice.

- **Single writer per journal.** Only the cell coordinator writes capacity. A tenant coordinator
  *asks* the cell coordinator for capacity and never writes capacity itself; it then computes
  placement deterministically, inside the capacity it was granted, from the registry, the failure
  domains and its desired state, and writes it as fenced entries into its own control journal.
  There is no rival write to resolve: separate journals have no order between them, and a
  single-writer journal makes the rival write impossible. Machines act on what they fold.
- **A parent places its children's actors.** The cell coordinator places the tenant coordinators,
  the fleet tenant's included, and re-places a dead one; tenant coordinators place their roles.
  This is FDB's recruitment by process class, per tenant.
- **Static stability.** A child keeps serving while its parent is down. Only new capacity, new
  tenants and moves wait for the parent. In particular, existing tenants keep serving while the
  fleet tenant is unavailable, and the simulation shows it with one cell.
- **Rebuild from below.** Every level's journal can be reconstructed from the level beneath it. A
  tenant's name and desired state live in its own control journal; the cell's list of hosted
  tenants and the fleet directory are rebuildable indexes. This is the pattern of DSQL's adjudicator
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
(`proxies=`, `proxy_leaders=` and `batchers=` per tenant, `replicas=` per journal).

**How roles are sized** (decided on 2026-10-04). Stateless roles — proxies, proxy leaders,
batchers — are **per-tenant pools**: one pool per role, shared by all the tenant's journals and
keyed by `JournalIdentifier` inside, so a hot journal uses every instance and a quiet one none
(#193). Storage roles — acceptors, replicas — are **per journal**, because they hold that
journal's data; matchmakers are one set per tenant. **Placement spreads load across the cell**
(#212): every role's instances go to the eligible machines of its class, least-loaded slots
first, and one journal's storage instances never share a machine. With six `storage` machines
available, a tenant with three journals of two replicas each gets its six replicas on six
machines; its pool of proxy leaders spreads the same way over the `stateless` machines, each
instance serving all three journals.

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
Reconfiguration is the operational primitive for everything, so no tenant opts out of it.

Every tenant has a **minimum footprint**, booked in slots against its cell's capacity when the
tenant is created: one coordinator slot, its matchmaker set and one acceptor quorum of its
redundancy mode. The fleet tenant and the cell tenant count too. A cell refuses a tenant whose
footprint it cannot book.

### 3.5 The proxy

A stateless process in front of the machines. **Proxies are per tenant** (decided on 2026-10-04):
the tenant coordinator places them in role slots like any role and the tenant's mode sizes the
pool, so a proxy folds only its own tenant's control journal and one tenant's load never reaches
another's proxies. A client finds its tenant's proxies through the **rendezvous call**, which any
machine of the cell answers from the cell registry it already folds (a proxy is a booked slot
keyed by tenant); the rendezvous also resolves the tenant name to its `TenantId` through the
fleet directory. Administration (`init`, tenant create and delete, drains) is served by the fleet
tenant's own proxies. It authorizes the caller through an `Authz` trait
whose implementation verifies a Biscuit token (below), and it routes
each call to the machine serving the journal, so a client never knows placement. It forwards
calls and their answers rather than redirecting the client (decided on 2026-10-04): clients only
ever reach proxies, which is what keeps the network the trust boundary. Quotas are M10
(decided on 2026-10-04). Past the proxy nothing knows a tenant name, only
`(TenantId, JournalId)`.

**Names** (decided on 2026-10-04, #239). A user addresses `paros://<tenant>/<journal>`; the URI
names data, not a location, and the fleet endpoint (a proxy's address) is client
configuration. **The proxy alone resolves names**: clients send names and never read the fleet tenant,
so no tenant sees another tenant's names. Until the proxy exists (#192), `parosctl`
resolves with operator rights. A name is free again once its delete completes; a recreated
tenant or journal draws a fresh id, so an old id never aliases a new name.

**Trust** (decided on 2026-10-04). The boundary is the network: only proxies and peers reach
a node (a separate network in the Compose toy), and nodes do no authorization.

**Tokens are Biscuits** (decided on 2026-10-04, #245; JWT is in section 11). paros is its own
issuer: a root key pair, Ed25519, whose public half (with its root key id) is recorded in the
fleet entry; rotation is adding a key then removing the old one. Tokens are short-lived and there
is no revocation. `parosctl` works **offline**, with no running fleet: it generates root key
pairs and mints tokens for any role.

- **Roles** are facts in the authority block: `admin` administers the fleet (`init`, cells,
  machines, everything below), `tenant-manager` creates, deletes and lists `users` tenants
  through the fleet tenant, and a `tenant` token is scoped to one `TenantId` for its data plane
  and journals. The proxy's policies are Datalog, one per call.
- **Creating a tenant returns a valid tenant token**: the proxy *attenuates* the caller's
  token with a block that restricts it to the new tenant. Attenuation needs no private key, so no
  proxy holds the root key.
- **Users attenuate offline**: a tenant can narrow its own token (read-only, one journal, an
  earlier expiry) without asking paros.

**Deterministic first, fewer features** (decided on 2026-10-04). Biscuit runs only in a way the
simulation can replay, and features that cannot are left out:

- Every key and every appended block's next key comes from a seeded RNG
  (`new_with_rng`, `build_with_rng`, `append_with_key`); the `OsRng` defaults are banned
  (clippy `disallowed-methods`).
- The authorizer's wall-clock budget (`RunLimits::max_time`, default 1 ms, checked against
  `Instant::now()`) is set out of reach; evaluation is bounded by `max_iterations` and
  `max_facts`, which count deterministically. The returned execution time is never read.
- The current time is a `time(...)` fact the proxy adds from its provider's clock, never
  `AuthorizerBuilder::time()`, which reads `SystemTime::now()`.
- Decisions are allow or deny; no query result is consumed, because the Datalog engine's
  `HashMap` iteration order is per process. A refusal is judged by its kind, never by which
  error came first.
- Out of scope: third-party blocks, revocation ids, P-256, snapshots, extern functions and
  Datalog queries.
- Biscuit stays out of `paros-core` and `paros` (its wasm32 clock needs JavaScript's
  `performance`): `paros` defines the `Authz` trait and carries tokens as opaque bytes; the
  Biscuit implementation is its own crate, used by `parosd`, `parosctl` and `paros-sim`.

**Routing goes through the fleet tenant from M9.** The proxy resolves the tenant name → `TenantId` →
cell from its fold of the fleet directory, then the journal name → `JournalId` and its placement
from its fold of the tenant's control journal. With one cell the first step always answers "this
cell", and it runs anyway. A cell's proxy and a future fleet router answer the same
rendezvous call, "which references serve tenant T", so `paros://<tenant>/<journal>` never
changes when a second cell appears. The AWS cell-based architecture guidance wants the router to
be the thinnest possible layer, one that keeps routing on its cached map while the control plane
is down; this proxy also does authorization and naming, so whether M12 needs a separate
router role or a proxy mode is open (#233).

Tenants are created and administered through the same proxy, with an `admin` or
`tenant-manager` token, through the fleet tenant (section 3.7): one API, one `Authz` trait, exercised in the simulation like
every other call.

**The proxy routes to the data-plane roles** (decided on 2026-10-04: renamed from "proxy").
A single-writer `Write`, a `Truncate` and a `SetLeader` go to the journal's Paxos leader; a
multi-writer `Write` goes through the tenant's batchers when it has any (section 2.4), and the
unbatchers' answers come back through it; a `Read` goes to whatever holds the journal's replica
state. The proxy is not the **proxy leader** of Compartmentalized Paxos, which does Phase 2 for
the Paxos leader (section 4.1); "proxy leader" is never shortened to "proxy".

### 3.6 Status

`parosctl status [--tenant t] [--cell c]` shows three columns, per tenant, per cell and for the
fleet: desired (what the tenant asked for), available (machines registered, not drained, seen
alive) and current (what is placed and serving, with each journal's word: Healthy, Degraded,
Unavailable). The tenant view folds the tenant's control journal, the cell view the cell control
journal, the fleet view the fleet directory with each cell entry's state. Status is computed live
from those folds and `Inspect`, never written back: there is no separate monitoring store.

It starts small, like `fdbcli status`, and grows by views, each one an admin RPC scoped by the
caller's authorization: **role slots** per machine (class, total, booked, and what holds each
slot), per tenant (its slots and footprint), per cell (free and booked per class) and for the
fleet (decided on 2026-10-04).

### 3.7 The fleet

The fleet runs from M9 with one cell (decided on 2026-10-02, #226).

**A cell** spans several availability zones: enough failure domains for its quorums. Zone survival
stays a property of each journal's quorums (section 5). A cell is a blast-radius boundary for bad
deploys, overload and poison pills, not a failover domain; the AWS cell-based architecture
guidance says the same (cells contain overload and bad deployments and are not designed for
failover; multi-AZ cells avoid replicating between cells). Cell creation is refused if its
machines do not cover the required zones.

**The fleet tenant always exists** (named *meta* until 2026-10-04). It is one tenant whose control journal also holds the directory, so it is
a single journal. In M9 it lives in the only cell; any cell may host it later. It stays small: it
answers only "which tenant lives in which cell" plus the cell entries. Quotas, billing and global
status go elsewhere. When the directory grows large (M12) it is split across several journals by
tenant range, the AWS guidance's range-based mapping.

**A tenant lives in exactly one cell.** A tenant too large for a cell gets a dedicated cell; a
tenant is never split by journal.

**Every fleet operation is an idempotent state machine** (FDB's metacluster, section 10). A
tenant's directory entry carries a state: `REGISTERING`, `READY`, `REMOVING`,
`UPDATING_CONFIGURATION` or `ERROR` (`RENAMING` was dropped on 2026-10-04: no milestone renames
a tenant, and names are the proxy's). Creating a tenant (`parosctl tenant create`)
writes it into the fleet directory in `REGISTERING` with a cell assignment (always the one cell
today), creates the tenant in its cell, then marks it `READY`. If an operation fails partway,
re-running the same operation is allowed and resumes where it stopped; on success the tenant
returns to `READY`. **A tenant is created once** (decided on 2026-10-04): a creation is named by
the tenant id its creator drew, so only a re-run carrying that id resumes it; any other creation
of a name the fleet tenant holds, in any state and whatever its placement, is refused (`NameTaken`), never
merged into the first. Until #225 the client drives the steps, so an interrupted creation stays
`REGISTERING` until it is deleted; with #225 the coordinator that owns the control journal
finishes every `REGISTERING` and `REMOVING` entry it finds (decided on 2026-10-04: the entry is
the work order, no client resumes another's creation). A tenant in any state may be removed; only a `READY` or
`UPDATING_CONFIGURATION` tenant may be reconfigured. `init` follows the same rule. Cell entries
carry a state too: `REGISTERING`, `READY`, `REMOVING` or `RESTORING`, and only a `READY` cell
receives new tenants. In M9 the one cell goes `REGISTERING` → `READY` during `init`, and
`RESTORING` during a recovery.

**Registration is recorded on both sides and verified on every step.** The fleet directory's cell entry holds the
cell's id; the cell's durable cell plan and its control journal hold the fleet's id; both hold a
metadata version number. Every
multi-step operation checks, at each step, that it still talks to the same fleet and the same
cell as on its previous step, and refuses otherwise (FDB's `MetaclusterOperationContext`). The
metadata version lets a reader refuse a format it does not understand.

**What M9 carries so that M12 adds no protocol or data-model change:**

- The machine record carries `node_id`; the durable cell plan carries `cell_id`, `fleet_id` and
  the metadata version (amended on 2026-10-04: the code keeps them in the cell plan, not `Config`).
- `Hello` carries `cell_id` and `fleet_id`.
- The rendezvous call is keyed by tenant.
- Tenant control journals are self-describing (name, desired state).
- The fleet directory's tenant entries carry the fleet-unique `TenantId`, the `JournalIdentifier` of the tenant's control
  journal, the cell assignment, the state, a configuration sequence number, the tenant's
  **group**; the fleet directory's cell entries carry the cell id, the cell tenant's `JournalIdentifier`, the state and
  the metadata version. No id is well known (section 3.8): a second cell learns the fleet tenant's `JournalIdentifier`
  when it joins the fleet, from the cell that hosts the fleet tenant.
- Every peer and client message carries its `JournalIdentifier` `(TenantId, JournalId)` (section 3.8).
- The checkpoint record format has both its `Inline` and `Ref` forms (section 3.9).
- No component assumes there is only one cell: every lookup goes through the fleet directory.

**Tenant groups** (decided on 2026-10-04). A tenant belongs to a **set of groups**, recorded in
the fleet directory's tenant entry when the tenant is registered and never changed afterwards.
Each group carries a rule, and a tenant obeys the rules of every group it is in. The set of
groups is fixed by paros for now; operator-defined groups (rollout waves, co-location), as labels
without rules, may come later (decided on 2026-10-04):

| Group | Rule | Members |
|---|---|---|
| `internal` | created only by paros's own operations (`init`, adding a cell), never through the tenant API; reached only for administration, through the fleet tenant's proxies | the fleet tenant, every cell tenant |
| `cell` | **never leaves its cell**: it *is* its cell (section 1); reconfigured only within it | each cell's cell tenant |
| `fleet` | holds the fleet directory; moves with its coordinator | the fleet tenant |
| `users` | created by the tenant API (`parosctl tenant create`, through the fleet tenant's proxies); served by its own proxies | every served tenant |

So the fleet tenant is `{internal, fleet}`, a cell tenant `{internal, cell}` and a served tenant
`{users}`. **The groups alone decide whether a tenant moves**: it moves unless one of its groups
forbids it, and today only `cell` does. There is no per-tenant movability flag (a
`movable`/`pinned` placement was dropped the same day); the fleet tenant refuses a move of a
tenant in `cell` when it applies the move's first entry, so no step of the move runs. The tenant
API creates tenants in `users` only, and the fleet tenant refuses any other group from it.

**Moving a tenant (M12)**, any tenant outside the `cell` group, reconfigures its journals and matchmaker set onto the target cell, then
transfers ownership with `SetLeader` on the tenant's control journal (the one moment ownership
changes), then flips the directory pointer, then the old cell forgets the tenant: the AWS
guidance's four migration phases, copy, flip, redirect, forget. The directory entry is a pointer,
never the authority: if it disagrees with the control journal's leader, the leader wins.
The fleet tenant moves the same way, being one journal; moving it to a dedicated cell is the escape hatch from
co-locating it with tenants.

**Open for M12: the cell inside the configuration.** Today a matchmaker's registry binds an
acceptor set and its quorum system to a ballot, and names no cell. Node ids are fleet-unique, so a
reconfiguration onto another cell's machines already moves a journal's data. If a configuration
also carried its `cell_id`, the move would itself be decided by Paxos: the effective
configuration (the highest-ballot reconfiguration a matchmaker quorum holds) would name the cell
that owns the journal, the directory would become a cache of that fact, and a reconfiguration
naming another cell for a cell tenant's journal could be refused where it is registered. The
cost is one field in `AcceptorConfig` that the core never decides on, and every node knowing its
own cell. Recorded on #232.

**M12, "Multiple cells"** (#232, #233): adding a second cell, removing a cell (its id goes into a
tombstone set so it cannot silently rejoin), moving tenants between cells, moving the fleet tenant, a separate
router role, splitting the fleet tenant by range, placement across cells (each cell entry with a configured
capacity and an allocated count, an ordered index of cells with room, the fullest cell that still
has room after a quick availability check, an optional preferred cell, a per-cell switch that
stops new placements), and tenant locks (`UNLOCKED`, `READ_ONLY` or `LOCKED` with an owner id) to
make a tenant read-only during a move.

### 3.8 Identifiers

Every identifier is random or minted by the one writer that can check it, never derived from a
cell's log position, so nothing is renumbered when cells are added, removed or restored (decided
on 2026-10-02, #226). **No identifier is fixed** (decided on 2026-10-04): there is no well-known
tenant, no well-known journal and no reserved range. `0` means unset in every id space, and that
is the only value with a meaning. **No id has a default** either: an id is drawn or read, never
assumed, and unset is a state to refuse, not a value to fall back on.

- `node_id`, `cell_id`, `fleet_id`: random, minted at format, `init` and `init` respectively, and
  stored in the machine record (`node_id`) and the durable cell plan (`cell_id`, `fleet_id`). They
  are written once, unset → set, and a later mismatch is refused at boot like any `Config`
  mismatch.
- The **leader uuid** of a single-writer journal (section 2.3): 128-bit random, drawn by the
  leader for one term, never reused. A writer never chooses an identity that another process could
  share.
- **Tombstones** (removed tenant ids, dropped tenants, deleted journal ids) are kept forever, a
  `u64` each. They are the one part of control state bounded by history rather than by live
  entities (section 3.9), accepted as such (decided on 2026-10-04). Names are not tombstoned
  (section 3.5).
- `TenantId(u64)`: random, drawn by the creator and recorded by the fleet tenant in the `REGISTERING` step;
  the fleet tenant refuses a duplicate at apply and the creator redraws. It is fleet-unique, so moving a
  tenant between cells never needs a new id. The system tenants are no exception: each cell's
  cell tenant gets a random id at the cell's `init` (so two cells' cell tenants differ), and the
  fleet tenant gets one when `init` creates the fleet, kept when the fleet tenant moves. The fleet tenant records both
  (its own in its first entry, each cell tenant in that cell's entry), so its duplicate check
  covers them too. The fleet tenant records each tenant's groups beside its id
  (section 3.7). FDB gave each metacluster an id prefix for the same goal; a random draw
  checked by the fleet tenant needs no prefix.
- `JournalId(u64)`: random, unique within its tenant, recorded and checked at apply by the tenant
  coordinator, the single writer of the tenant's control journal; a duplicate is refused and the
  creator redraws. A tenant's **control journal** has a random id too, drawn with the tenant and
  recorded where the tenant is recorded: in the fleet directory's tenant entry, and for the two system tenants
  in the cell plan. A journal's id never changes when its tenant moves; a control journal that
  recovery rebuilds (section 3.10) gets a new one, so the old and the new can never be mistaken
  for each other.
- **Discovery replaces convention.** The only fixed starting points are a machine's addresses:
  the `JournalIdentifier`s of the cell's and the fleet tenant's control journals are learned from any machine of the cell
  (section 3.2), and everything below them through their folds.
- **Every peer and client message carries its `JournalIdentifier` `(TenantId, JournalId)`** (named `JournalKey` in the code until #244), riding the `Deliver`
  envelope where `JournalId` alone rides it today, so uniqueness is only ever needed where it can
  be checked. A tenant's matchmaker set is named by its `TenantId`.

### 3.9 Checkpoints

The control plane compacts its control journals with the same two calls any tenant has (decided
on 2026-10-02, #227): `paros-core` gains nothing. The pattern, shipped in `paros::client` (#230)
for the control plane first and offered to users as a recipe, never a guarantee paros enforces:

1. The coordinator pauses its own control writes (it is the single writer, and control writes are
   rare).
2. It writes a checkpoint of the folded state as an ordinary fenced `Write`.
3. It calls the fenced `Truncate` up to the checkpoint's `seq`.
4. Readers start at `first_seq`, which is always a checkpoint, and fold forward.

A checkpoint replaces the state entirely, so a crash between the write and the truncate is
harmless: the next fold meets a checkpoint in the middle of the log and resets on it. `Truncate`
only ever targets a checkpoint. A reader that gets `Truncated` restarts from `first_seq`.

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
- **The record format is fixed from day one**, as one of two forms:
  - `Inline { chunks }`: the state inside the journal, chunked by key range. Always one chunk in
    M9.
  - `Ref { journal_id, covers_up_to, end_seq, checksum }`: the state written in many small
    batches into a separate checkpoint journal of the same tenant, never written again once
    referenced; the main journal holds only this pointer. This is Pulsar's topic compaction
    (PIP-14): the compacted data goes to a separate, closed ledger, and the topic's metadata
    records that ledger plus the compaction horizon. The next coordinator deletes unreferenced
    checkpoint journals; at most two exist at a time.

  M9 writes `Inline` only. `Ref` removes the batch-size limit and never blocks the main journal;
  it is needed when a journal's state outgrows one batch (the fleet tenant in a large fleet, M12). Readers
  handle both forms from the start, so adopting `Ref` later changes only the writer.

### 3.10 Recovery (deferred)

**Deferred out of M9** (decided on 2026-10-04, #231): nothing of it is built, and the simulation
cannot reach a lost control quorum while it runs one seed (#213). The design below stays the
direction and is taken up as its own later issue. Two questions it must answer then: with the fleet
tenant's and the cell's control journals on the same seeds, recovery reads the machines' own stores
(`journals/<tenant>/<journal>/`, their `JournalIdentifier`s and assignments) to rebuild both, tombstones
included; and a new control `JournalId` must reach machines whose cell plan is written once.

Losing a control quorum is recoverable without unsafe Paxos surgery, at both levels, because
control journals are rebuilt from below (decided on 2026-10-02, #225, #231). User data is never
touched: the tenants' journals have their own quorums.

- **Cell.** `parosctl init --recover` starts a fresh cell control journal, under a new random
  `JournalId` recorded in the cell plan, and a new recovery generation; live machines re-register; tenant coordinators re-report from their own control
  journals; any node still holding the old configuration is refused.
- **Fleet.** The fleet directory is rebuilt from the cells' tenant lists, the way FDB's metacluster
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
| Proxy (the entry role) | stateless | to build: one pool per cell, routes each call to the role that serves it (section 3.5) |
| Coordinator (fleet, cell, tenant) | stateless, or a seed at bootstrap | to build: the election library over a multi-writer journal (#240) |

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
control quorum will be recoverable at both the cell and the fleet level, without touching user
data (section 3.10, deferred). Cross-region replication and witness replicas are out of scope.

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
gives `.processes()` groups no locality). moonpool reports no per-process damage (moonpool#295,
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
metainfo; a matchmaker registration is a position of its own. Storage chaos on journal seeds is the
local half of this contract: moonpool's crash damage, failed syncs, short transfers and lost
directory entries, a seam crash as a power loss, and a power cut inside a commit; rot waits for
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
- What is adopted: zone labels live inside `AcceptorConfig`, bound to the ballot with the
  configuration, because two nodes that disagree on a member's zone evaluate different quorums
  (the registry's failure domain is the composer's input, never read live by a tally); the
  coordinator's placement rule is judged through `QuorumSystem`, never a count: removing any one
  zone must leave a Phase-1 and a Phase-2 quorum, and every Phase-2 quorum spans at least two
  zones; a journal's leader is placed toward the zone that writes to it through `relinquish_to`,
  which is WPaxos's steal without a Phase 1; matchmaker sets are zone-spread, since their quorums
  are majorities; and the simulation gains a zone-kill attrition mode and a zone-aware copy
  budget, without which it cannot prove zone survival.

## 6. Verification

Simulation is the investment. Every milestone lands with its share of:

- Invariants in the audit, where the fact arrives: a leader uuid leads at most one term and is
  never reinstated, every leadership change present in the log, `seq` dense per journal, a
  single-writer `Write` never re-accepted with other bytes, `Truncate` monotone and `first_seq`
  never above a served cursor, a single-writer `Truncate` accepted only from the current leader, a
  `SetLeader` winning at most once per `old_uuid`, a multi-writer journal never refusing an
  in-limits write, a capacity slot booked at most once (a booking id never booked twice, across
  checkpoints), a tenant never reaching a journal outside its own
  `TenantId`, no component reaching a journal by an id it did not learn (the simulation draws
  every `JournalIdentifier`, the system tenants' included, per seed).
- Control-plane invariants: a child keeps serving through its parent's outage; folding from a
  checkpoint yields the same state as folding the full history; the fleet directory equals the union
  of the cells' tenant lists (assignments and counts), checked even with one cell (FDB's
  metacluster consistency checker); every fleet operation resumes correctly when re-run after a
  crash at any step; an election never has two leaders whose writes both land, and settles on one
  leader after the chaos window (a liveness oracle in recovery mode).
- A real linearizability checker in the workload's `check()`, over the four-call history with
  `Ambiguous` outcomes, against the sequential model of a journal (a mode, a leader, a dense log,
  a floor). It replaced the per-operation rules of `ClientHistory` (#205).
- The three races made likely rather than lucky, each a knob or a hook with its own BUGGIFY
  location and its reachable: a `SetLeader` drawn in the middle of a pipelined burst, a client
  timeout shorter than the ack so a retry crosses an ownership change, a `Truncate` racing a
  reader's cursor.
- Control-plane shapes: one cell hosting the fleet tenant, now; a crash at each step of `init` and of tenant
  creation, each step with its own reachable; a crash between a checkpoint's write and its
  truncate, for the registry and for the fleet tenant; a truncate refused from a stale leader; the fleet tenant
  unavailable while tenants serve; a coordinator killed mid-operation and its successor finishing
  it. In M12: a second cell, a tenant move and a move of the fleet tenant. With recovery (deferred): a lost cell
  control quorum recovered by `init --recover`.
- Storage chaos on the shipped stores: every node and matchmaker runs on moonpool-journal, with a
  journal-aware injector aimed through `Journal::regions` (striped by slot) under the copy budget, corpus
  masks re-expressed as journal targets, crashes inside a sync, then moonpool's environmental
  storage chaos with replicated fault patterns (#176, #202). Gates name journal verdicts (slot
  rebuilt, double fault parked, meta repaired, ambiguous batch kept); the in-memory stores and
  their gates retire.
- New BUGGIFY sites for every new decision the driver, the proxy and the coordinators take,
  and the coverage-guided sweep saturating over them.

No new model checker and no separate specification: the two existing sans-IO model checkers
stay as they are.

## 7. What changes against today

Only what is still to change; landed changes (the four-call cut-over, the fenced `Truncate`, random
ids and the `JournalIdentifier`, start-and-wait plus `init`, the uniform `parosd`, the retired
read-index path and the unset `JournalIdentifier`s that meant "the first journal", #243; the
fleet tenant, `JournalIdentifier` and proxy renames in the code, #244) are in the history and
AGENTS.md.

- The `(generation, owner)` pair of M7 becomes a single 128-bit leader uuid, compare-and-set by
  `SetLeader(new, old)`, with a hidden term counter in the core; `Write` takes an explicit
  `expected_seq`; journals gain a writer mode, single or multi (section 2, #241). No compatibility
  layer: `parosctl --owner` becomes `--leader`, and the chain workload's alphabet, the
  linearizability model and the audit follow.
- The system journals (`SystemPlan`, the directory, the genesis pool) dissolve into the four
  levels: tenant names and desired state move into each tenant's control journal, capacity is
  owned by the cell coordinator alone, and `init` stops creating a hidden journal (#210).
- `parosd` stops running the plain deployment: every journal is born with its matchmaker set, and
  journal-tagged matchmaker planes replace "only the first journal of a process" (#190); proxy leaders
  and replicas follow (#193).
- The coordinators replace the operator's client: `parosctl` stops writing as the lowest seed's
  node id (#240, #212).
- The in-memory "world" stores of the simulation retire; every role runs on moonpool-journal
  under at least the same chaos (section 5, #176, #202).

## 8. Milestones

Milestones are labels (`milestone:M7` and up); `milestone:M6` stays the epic's umbrella. The
toy is the end of M9. The epic is #184, the backlog pointer #69, the verification track #24.

| Milestone | Name | Content |
|---|---|---|
| M7 | Journal API (#204, #205) | the four calls, the journal state machine in core, the wire and the driver, the chain workload's alphabet, the linearizability checker, the race knobs and hooks, the cut-over |
| M8 | parosd deployable (#206 to #209, #221, #220, #196, #201) | Tokio providers linked, the stores on a real filesystem for the first time, the `JournalStores` opener, `Config` durable at `format`, `parosd provision` (replaced by `init` in M9), the uniform binary with class and capacity, Compose, `paros::client` (#221) and the `parosctl` CLI (#220), a tracing subscriber, exit codes |
| M9 | The fleet with one cell (#225, #226, #227 and #216 first; landed: #228, #235, #229, #230 and #211's core; sim first: #176, #202, #213, #246, #247, #248; then #241, #243, #244, #240, #210, #239, #190, #212, #192, #245, #191, #211, #213) | the control hierarchy and its decisions, the fenced `Truncate` on the wire, random ids and the `(TenantId, JournalId)` `JournalIdentifier`, the leader-uuid API and its two writer modes, `init` creating the fleet with its matchmaker sets, the cell tenant and its machine registry with role slots and liveness, the fleet tenant with its directory and tenant creation state machine, the election library and the coordinators it runs, requests to a leader, placement inside capacity granted by the cell, the checkpoint-and-truncate library, names at the proxy, the proxy with Biscuit `Authz` routing through the fleet tenant, per-tenant matchmaker sets, `parosctl status` |
| M10 | Roles per tenant (#193, #214, #194, #145, #195) | journal-tagged proxy leaders and replicas, batchers and unbatchers for multi-writer journals, tenant modes (redundancy, grid, role counts) applied by the tenant coordinator, quotas, the benchmark, then scale work |
| M11 | Zones (#215) | zone labels in `AcceptorConfig`, the placement rule, leader placement toward the writer's zone, zone-kill attrition and a zone-aware budget in the simulation, zone-spread matchmaker sets |
| M12 | Multiple cells (#232, #233) | adding and removing cells with tombstones, placement across cells, tenant locks, moving tenants and the fleet tenant between cells, splitting the fleet tenant by range, the `Ref` checkpoint writer, the router question |

Verification is not a milestone: every milestone carries its own share of section 6. M9 opens
with a **simulation-first phase** (decided on 2026-10-04): storage chaos on the shipped stores
(#176, #202), three seeds (#213), the `parosd` machine lifecycle simulated as shipped (#246), the
control-plane oracle debt (#247) and TigerStyle assertions (#248) rank ahead of M9's features. Recovery (#231, section 3.10) is
deferred and carries no milestone yet. The interactive game and the lessons (`track:play`, #162,
#163) run outside the milestones, behind the service.

## 9. The toy, done means

The Compose toy is the user's demo, for running paros by hand, and is no part of the test suite
(decided on 2026-10-04): CI only checks that the image builds, and behaviour is proved by the
simulation. Its machines are plain nodes (`node1`..`node3` over three failure domains, `storage4`,
`proxy1`); the rendezvous list that names the first three is the `seeds` alias.

From a fresh clone: `docker compose up`, then `parosctl init` against `node1`, which creates the
fleet, its one cell and the fleet tenant. Generate a root key and mint an `admin` token offline with `parosctl`, then create a tenant and
receive its token. Create a
journal. `write`, `read` and `tail` from `parosctl`, addressing `acme/orders`. `set-leader` to a second
client and see the first one refused, for a `write` and for a `truncate`. Create a multi-writer
journal and append to it from two clients at once. Kill one `storage` and one `stateless`
container and keep writing. Wipe one volume, see the amnesia refusal, and see the journal healed
by reconfiguration onto another machine. Stop the fleet tenant's quorum and keep writing to an
existing tenant. Kill the cell coordinator's machine and see another one elected. `parosctl
status` shows desired, available and current, and the role slots, per machine, per tenant, per
cell and for the fleet. (Recovery of a lost cell control quorum is deferred, section 3.10.) The simulation is green in every shape and the
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
  state and is rebuilt from the committed transactions; the witness region.
- Brooker, "Control Planes vs Data Planes", <https://brooker.co.za/blog/2019/03/17/control.html>:
  what belongs on the request path and what scales with the fleet.
- Amazon MemoryDB, SIGMOD 2024,
  <https://cdn.amazon.science/e0/1b/ba6c28034babbc1b18f54aa8102e/amazon-memorydb-a-fast-and-durable-memory-first-cloud-database.pdf>:
  §4.1 conditional append and leadership as one more conditional append, the lease and
  self-demotion; §7.2.1 snapshot verification by a checksum carried in the log.
- Aurora DSQL, arXiv 2607.13276, <https://arxiv.org/html/2607.13276v2>: §5 the journal's
  precondition on timestamp monotonicity, §6 TLA+ and P then deterministic simulation.
- Demirbas's MemoryDB summary,
  <http://muratbuffalo.blogspot.com/2024/05/amazon-memorydb-fast-and-durable-memory.html>, and
  the 2025 Journal reconstruction,
  <https://ajalab.github.io/posts/2025-08-14-journal-distributed-log-replication-behind-aws/>:
  secondary readings; the latter states plainly that retention and truncation are undocumented.

Cells and static stability:

- AWS Well-Architected, "Reducing the Scope of Impact with Cell-Based Architecture",
  <https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/what-is-a-cell-based-architecture.html>:
  cells contain overload and bad deployments and are not a failover domain; multi-AZ cells; the
  thinnest possible router routing on its cached map; range-based mapping; migration as copy,
  flip, redirect, forget; multiple cells and migration from day one.
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

- Ailijiang, Charapko, Demirbas, Tasci, "WPaxos: Wide Area Network Flexible Consensus", IEEE
  TPDS 2019, <https://arxiv.org/abs/1703.08905>: §3.1 the per-zone quorums `fz`, `fn`; §3.2 to
  §4 object stealing; §5.3 degraded operation and reconfiguration. The printed TLA+ quorum
  definition does not intersect; only the floor form the proof uses is sound.

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
  management or data, never two. paros co-locates the fleet tenant with tenants in one cell because the fleet tenant is a
  tenant with its own coordinator and quorums, which FDB could not do (its management data lived
  in the system keyspace). Moving the fleet tenant to a dedicated cell is the escape hatch.
- **Multiple cells from day one.** The AWS guidance recommends multiple cells and migration from
  day one. paros runs the fleet machinery from day one with one cell and defers the second cell
  to M12; the hooks of section 3.7 keep that deferral free of protocol changes.
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
  the front door (now the proxy) as the rendezvous: #216.
- **The `(generation, owner)` pair** (M7, #204; replaced on 2026-10-04). Two fields where one
  fence suffices, and an owner id the caller chose, so two processes could share it and both pass
  the owner check once they read the public generation. A per-term random leader uuid is
  Brooker's final MemoryDB API and cannot be shared by accident; the term counter stays, hidden
  in the core, to rule out a reinstated uuid.
- **A lease in the journal** (MemoryDB's lease-fenced writes). Rejected: a lease fence depends
  on clocks and pauses; the leader uuid fences without either, and the election library keeps a
  lease only as a liveness hint (section 3.3).
- **JWT** for proxy tokens (chosen on 2026-10-04 morning, replaced the same day, #245).
  Roles become ad-hoc claims checked by hand, a token returned at tenant creation needs a signing
  key at the proxy or a call to an external issuer, and a holder cannot narrow its own token.
  Biscuit gives roles as Datalog facts, attenuation without the root key (tenant creation, users
  narrowing their tokens offline) and offline minting. JWT stays the way to plug an external
  identity provider in later, as a second `Authz` implementation or a token exchange.
- **Well-known system ids** (fleet tenant `1`, cell tenant `2`, every control journal `1`,
  `0..=255` reserved; decided on 2026-10-02, reversed on 2026-10-04). They let a component find a
  control journal without asking, but every component must then agree on the convention forever,
  the cell tenant's id repeats in every cell (unique only within its cell), a rebuilt control
  journal reuses its predecessor's id, and the reserved range is a second id space every check has
  to special-case. Random ids recorded where they are created, and learned from any machine of
  the cell, cost one `Inspect` at bootstrap.
