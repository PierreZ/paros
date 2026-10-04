# paros: the journal service

This is the end goal. AGENTS.md describes what paros is today and the doctrine every change
follows; this document describes what paros is becoming, so that every issue, plan and session
aims at the same target. Where the two disagree, AGENTS.md is the present and this is the
direction. Decided on 2026-09-30; the fleet, the control hierarchy, identifiers, checkpoints,
recovery and the fenced `Truncate` decided on 2026-10-02. The milestones at the end carry the
issue numbers.

## 1. Goal

`parosd`: one binary on N machines, started with `docker compose`, serving journals to tenants
over a four-call data plane, healing itself through reconfiguration when a disk or a machine is
lost. Single region, several failure domains.

The control plane is itself stored in journals and coordinated through the same election
primitive the tenants use: paros eats its own food. It has four levels, all built the same way:
one actor elected by `SetLeader` on a control journal, holding no state that journal does not
hold (section 3.3).

| Level | Elected actor | Its control journal holds |
|---|---|---|
| Fleet | the meta coordinator (the meta tenant's coordinator) | tenant → cell, cell entries |
| Cell | the cell coordinator (the cell tenant's coordinator) | machine registry, capacity bookings |
| Tenant | the tenant coordinator | its name and desired state, journal names, placement inside capacity granted by the cell |
| Journal | the client writer | the data |

A paros deployment is always a fleet, and the fleet runs from M9. For now it has exactly one
cell, and that cell plays both roles: it hosts the meta tenant (the fleet level) and it is an
ordinary cell holding tenants. Every fleet behaviour exists and is exercised from M9: `init`
creates the fleet, tenants are created through meta, routing resolves tenant → cell through
meta's directory, registrations are verified on both sides. The answer is always "this cell"
today, but the code path is real and runs in every simulation. A second cell, moves between
cells and a separate router are M12 (section 3.7). The test for every design until then: adding
a second cell adds an entry to meta's directory and a routing choice, never a protocol or
data-model change.

Compaction and snapshots are the user's business: paros owns the log, never the state.
`paros-core` never compacts, snapshots or verifies anything. The control plane is a tenant and
obeys the same rule: it compacts its own control journals the way any user would, using only
`Write` and `Truncate` (section 3.9). That is the dogfooding rule: the control plane gets no call
a tenant does not have.

The model is the AWS Journal, the replicated log behind Aurora DSQL, MemoryDB and Lambda, as
described in the public sources listed in section 10: a durable, ordered, fenced log that stores
decided outcomes, that a single writer appends to under a generation, and that every consumer
tails without a second protocol.

The first deliverable is a toy: a local Docker Compose cluster an operator can initialize, create
a tenant on, write to, read from, break and watch heal. The homelab and anything beyond one region
are not in scope.

## 2. The data plane

Every journal exposes four calls. Every rule below is judged at apply time, in slot order, by a
small per-journal state machine `(owner, generation, next_seq, first_seq)` that lives in
`paros-core` beside the replica's walk.

| Call | Meaning |
|---|---|
| `Write(generation, owner, seq, batch)` | Append `batch` at `seq`. Fenced by `(generation, owner)`, contiguous by `seq`, idempotent on retry, pipelineable. |
| `Read(from_seq, limit, wait_ms?)` | The committed records from `from_seq`, plus `first_seq`, `next_seq` and `cur_gen`, or `Truncated` when `from_seq < first_seq`. Long-polls at the tail for `wait_ms`. |
| `Truncate(generation, owner, up_to_seq)` | Drop every record below `up_to_seq`. Fenced by `(generation, owner)` like `Write`, monotone. The only retention API. |
| `SetLeader(expected_gen, new_owner)` | Compare-and-swap the owner. Returns `{ generation, next_seq }`. |

### 2.1 Positions

`seq` is the dense position of accepted records, assigned at apply. One `seq` per record: a
batch of `n` records occupies `[seq, seq + n)`, in one Paxos slot, accepted or refused whole.
Paxos slots stay internal: a `Noop` a new leader fills a hole with, a control command, a
generation change, a refused `Write` and a `Truncate` each consume a slot and no `seq`. Readers
never see a hole.

### 2.2 Writes and fencing

A `Write` is accepted iff its `(generation, owner)` is the journal's current one and
`seq == next_seq`. Otherwise it is refused in place, and the refusal names the current
generation and `next_seq` so the owner can continue or learn it was superseded.

Retries are answered from the log itself: a `Write` with `seq < next_seq` identical to the write
accepted at `seq` — the same generation, owner and batch — is an idempotent ack; anything else is
refused. A retry whose
`seq < first_seq` is answered `Truncated`, and the owner treats it as ambiguous and reads the
tail. The log is the deduplication table: there is no per-client session ledger and nothing to
expire.

A leader may refuse a `Write` from its own fold at propose time, so a superseded owner's
pipelined burst does not burn slots. That is an optimisation; the apply-time check is the safety.

### 2.3 Ownership

Ownership is a pure compare-and-swap, no lease and no clock. `SetLeader(expected_gen, new_owner)`
succeeds iff `expected_gen` is the current generation; the journal assigns `generation + 1`,
records the change as an ordinary log entry, and answers `{ generation, next_seq }` so the new
owner can continue the sequence. Every tailer learns the owner changed in-band, without a side
channel.

A superseded owner's writes and truncations are refused at apply, which is the whole safety
argument. What keeps a superseded owner from *serving* stale data is a rule on the owner, not on
paros: an owner serves nothing from local state it did not read back from the journal (DSQL's
adjudicator is a rebuildable cache over the log and holds no truth of its own). MemoryDB's lease
and self-demotion are deliberately not implemented; the sources are in section 10.

Two fences, two layers, never confused: the Paxos ballot says which *machine* runs a journal's
leader and is invisible to clients; the generation says which *client* may write and is
invisible to Paxos.

### 2.4 Reads

`Read` is served by the replica tier through the leaderless read of Compartmentalized Paxos
§3.4 (paros's `QuorumRead`): the replica asks a Phase-1 quorum of the acceptors for their vote
watermarks, waits until its own applied prefix covers the maximum, and answers. No read goes
through the leader, so reads scale with replicas and cost the acceptors one watermark round per
page. The read-index path and `CheckTail` retire.

### 2.5 Truncation

`Truncate(generation, owner, up_to_seq)` is proposed through consensus and judged at apply in
slot order like a `Write`: it is accepted iff `(generation, owner)` is the journal's current one,
and it never lowers `first_seq`. A refusal names the current generation, like a refused `Write`.
An accepted `Truncate` is applied lazily by every node when its contiguous chosen walk reaches
it, exactly today's `Truncate` control command.

The fence was decided on 2026-10-02 (#227, #228). Unfenced, anyone with access to the tenant
could truncate any of its journals, so a stale or buggy caller could truncate to a position that
is not a checkpoint and break every reader's fold. Kafka refuses client `DeleteRecords` on its
metadata topic for the same reason (KIP-630).

The owner truncates only after it has secured whatever checkpoint it needs; paros does not check
that, does not verify checkpoints and never will. A reader below `first_seq` is told `Truncated`
and nothing else: where it restarts is the application's contract with itself. The control
plane's own contract is section 3.9.

### 2.6 Underneath

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
meta's directory (tenant → cell, the fleet level) and each tenant's own control journal; desired
state moves into the tenant control journals. System journals are written with `Write` like any
journal; there is no special path.

Every tenant gets a control journal when it is created. It is **self-describing**: it holds the
tenant's name and desired state, its journal names and its placement, so every index above it
(the cell's list of hosted tenants, meta's directory) can be rebuilt from it (section 3.3).

**Bootstrap: start and wait, then one `init` that creates the fleet** (decided on 2026-10-02,
#216). There is no provisioning step that names the seeds to each other.

- The seeds start with the same rendezvous name or short join list and wait.
- `parosctl init` is sent to one of them, which every seed's join list must name. It is refused
  if the cell is already initialized. In order:
  1. **forms the cell**: mints `cell_id` and the cell tenant's frame (its random `TenantId` and
     the random `JournalId` of its control journal, section 3.8), writes them into every seed's
     durable cell plan, and the first cell coordinator claims the cell control journal with
     `SetLeader(expected_gen = 0, me)`;
  2. **creates the meta tenant** inside that cell (meta is a tenant, so the cell must exist first
     to grant it capacity): mints `fleet_id` and meta's frame, both random, recorded in the cell
     plan of the cell that hosts meta;
  3. **registers the cell** as the first entry in meta's directory, and writes the matching
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

The cell control journal starts as a plain journal over the seeds, and the meta tenant's control
journal is placed on the same seeds. They are the one matchmaker-free exception at bootstrap: they
run plain until the cell coordinator has enough registered machines to give each tenant its
matchmaker set through reconfiguration, and from then on they are reconfigured like any tenant's.

### 3.2 Machines

Every `parosd` is uniform. **Identity** has three parts (decided on 2026-10-02, #225):

- `node_id`: random, minted at format, stored beside the format marker (#147). It is the
  member's identity and the registry key. A wiped disk gets a new `node_id`, so "a wiped
  identity never rejoins" holds by construction.
- `addr`: an attribute that may change across restarts.
- `incarnation`: moonpool-rpc's per-start `Incarnation`, carried in the `InterfaceRef`.

The same `node_id` with a new incarnation is a reboot: the machine rejoins in place and keeps its
assignments. It is re-placed only if it does not come back within a bound, so a whole cell
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

**Finding the cell.** There is no cluster file. A machine's and a client's only static input is
one rendezvous name or a short join list, stored durably in `Config` and re-read on every boot
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
  the rendezvous call) with its `cell_id`, the cell tenant's control frame and, on the cell that
  hosts it, meta's frame, all read from its durable cell plan. A client or an operator handed
  only addresses learns the control frames from any machine, then resolves everything else
  through them: meta's directory gives a tenant's cell and the frame of its control journal, and
  that control journal gives the tenant's journals. A re-run of `init` learns the cell's ids the
  same way.
- `cell_id` and `fleet_id` are carried in the session `Hello`; a peer with another id is refused.
  ScyllaDB carries its cluster id in gossip for the same reason: nodes from different clusters
  cannot talk after a bad seed configuration.
- Well-known endpoints stay for exactly one call, the **rendezvous call**, keyed by tenant:
  "which references serve tenant T". Everything else is a dynamic reference.

The decision and its alternatives are #216. Classes are FDB's:

- `storage`: anything with a durable store. Acceptors, replicas, matchmakers.
- `stateless`: the front door, proxy leaders, batchers, unbatchers, coordinators.

Scaling a role for a tenant is adding machines of the right class and raising the tenant's
desired counts.

### 3.3 Coordinators and placement

The four levels of section 1 are built the same way (decided on 2026-10-02, #225): one actor per
level, elected by `SetLeader` on that level's control journal, so it holds a generation and is
fenced like any writer, and it holds no state that journal does not hold: a new one resumes from
a fold. All four exist from M9.

- **Single writer per journal.** Only the cell coordinator writes capacity. A tenant coordinator
  *asks* the cell coordinator for capacity and never writes capacity itself; it then computes
  placement deterministically, inside the capacity it was granted, from the registry, the failure
  domains and its desired state, and writes it as fenced entries into its own control journal.
  There is no rival write to resolve: separate journals have no order between them, and a
  single-writer journal makes the rival write impossible. Machines act on what they fold.
- **A parent places its children's actors.** The cell coordinator places the tenant coordinators,
  the meta tenant's included, and re-places a dead one; tenant coordinators place their roles.
  This is FDB's recruitment by process class, per tenant.
- **Static stability.** A child keeps serving while its parent is down. Only new capacity, new
  tenants and moves wait for the parent. In particular, existing tenants keep serving while the
  meta tenant is unavailable, and the simulation shows it with one cell.
- **Rebuild from below.** Every level's journal can be reconstructed from the level beneath it. A
  tenant's name and desired state live in its own control journal; the cell's list of hosted
  tenants and meta's directory are rebuildable indexes. This is the pattern of DSQL's adjudicator
  (section 2.3) one level up, and what makes recovery possible without Paxos surgery
  (section 3.10).
- **Coordinators checkpoint their control journals** with the library of section 3.9, so a
  control journal's length is bounded by its live entities, never by its history.

`Reconfigure` is how a journal moves: off a drained machine, off a dead identity, onto a spare,
between quorum systems. The tenant coordinator sends it to the journal's own leader, and
retirements wait for the GC watermark (`may_retire`).

### 3.4 Tenant modes

A tenant's desired state names, per journal or as a tenant default, its quorum system
(majority, flexible `{q1, q2}`, grid `{rows, cols}`), its replication count and, per role, how
many proxies, replicas and batchers it wants. This is FDB's `configure`, applied by the tenant
coordinator through reconfiguration. "Classic Multi-Paxos" is quorum system = majority.

Every tenant has one matchmaker set: per tenant, not per journal (the registry is keyed by
journal inside the set, so a set per journal buys nothing) and not shared across tenants (a
shared set is one role that could not scale per tenant and a blast radius across tenants). The
set is named by the tenant's id (section 3.8). Reconfiguration is the operational primitive for
everything, so no tenant opts out of it. The matchmaker-free plain deployment stays what
AGENTS.md says it is: a permanent library-level configuration, not a tenant mode.

Every tenant has a **minimum footprint**, counted against its cell's capacity when the tenant is
created: its coordinator, its matchmaker set and one acceptor quorum. The meta tenant and the cell
tenant count too. A cell refuses a tenant whose footprint it cannot book.

### 3.5 The front door

A stateless process in front of the machines. It authorizes the caller through an `Authz` trait
whose first implementation verifies a signed JWT carrying the tenant as a claim; it enforces
quotas; and it routes each call to the machine serving the journal, so a client never knows
placement. Past the front door nothing knows a tenant name, only `(TenantId, JournalId)`.

**Routing goes through meta from M9.** The front door resolves the tenant name → `TenantId` →
cell from its fold of meta's directory, then the journal name → `JournalId` and its placement
from its fold of the tenant's control journal. With one cell the first step always answers "this
cell", and it runs anyway. A cell's front door and a future fleet router answer the same
rendezvous call, "which references serve tenant T", so `paros://<name>/<tenant>/<journal>` never
changes when a second cell appears. The AWS cell-based architecture guidance wants the router to
be the thinnest possible layer, one that keeps routing on its cached map while the control plane
is down; this front door also does authorization and quotas, so whether M12 needs a separate
router role or a front-door mode is open (#233).

Tenants are created and administered through the same front door, with a fleet-administration
JWT, through meta (section 3.7): one API, one `Authz` trait, exercised in the simulation like
every other call.

The front door is not the batcher. The batcher is a data-plane role of Compartmentalized Paxos
that coalesces writes before a leader; the front door is authorization, naming, quotas and
routing.

### 3.6 Status

`parosctl status [--tenant t] [--cell c]` shows three columns, per tenant, per cell and for the
fleet: desired (what the tenant asked for), available (machines registered, not drained, seen
alive) and current (what is placed and serving, with each journal's word: Healthy, Degraded,
Unavailable). The tenant view folds the tenant's control journal, the cell view the cell control
journal, the fleet view meta's directory with each cell entry's state. There is no separate
monitoring store.

### 3.7 The fleet

The fleet runs from M9 with one cell (decided on 2026-10-02, #226).

**A cell** spans several availability zones: enough failure domains for its quorums. Zone survival
stays a property of each journal's quorums (section 5). A cell is a blast-radius boundary for bad
deploys, overload and poison pills, not a failover domain; the AWS cell-based architecture
guidance says the same (cells contain overload and bad deployments and are not designed for
failover; multi-AZ cells avoid replicating between cells). Cell creation is refused if its
machines do not cover the required zones.

**Meta always exists.** It is one tenant whose control journal also holds the directory, so it is
a single journal. In M9 it lives in the only cell; any cell may host it later. It stays small: it
answers only "which tenant lives in which cell" plus the cell entries. Quotas, billing and global
status go elsewhere. When the directory grows large (M12) it is split across several journals by
tenant range, the AWS guidance's range-based mapping.

**A tenant lives in exactly one cell.** A tenant too large for a cell gets a dedicated cell; a
tenant is never split by journal.

**Every fleet operation is an idempotent state machine** (FDB's metacluster, section 10). A
tenant's directory entry carries a state: `REGISTERING`, `READY`, `REMOVING`,
`UPDATING_CONFIGURATION`, `RENAMING` or `ERROR`. Creating a tenant (`parosctl tenant create`)
writes it into meta's directory in `REGISTERING` with a cell assignment (always the one cell
today), creates the tenant in its cell, then marks it `READY`. If an operation fails partway,
re-running the same operation is allowed and resumes where it stopped; on success the tenant
returns to `READY`. A tenant in any state may be removed; only a `READY` or
`UPDATING_CONFIGURATION` tenant may be reconfigured. `init` follows the same rule. Cell entries
carry a state too: `REGISTERING`, `READY`, `REMOVING` or `RESTORING`, and only a `READY` cell
receives new tenants. In M9 the one cell goes `REGISTERING` → `READY` during `init`, and
`RESTORING` during a recovery.

**Registration is recorded on both sides and verified on every step.** Meta's cell entry holds the
cell's id; the cell's `Config` holds the fleet's id; both hold a metadata version number. Every
multi-step operation checks, at each step, that it still talks to the same fleet and the same
cell as on its previous step, and refuses otherwise (FDB's `MetaclusterOperationContext`). The
metadata version lets a reader refuse a format it does not understand.

**What M9 carries so that M12 adds no protocol or data-model change:**

- `Config` (#207) carries `node_id`, `cell_id`, `fleet_id` and the metadata version.
- `Hello` carries `cell_id` and `fleet_id`.
- The rendezvous call is keyed by tenant.
- Tenant control journals are self-describing (name, desired state).
- Meta's tenant entries carry the fleet-unique `TenantId`, the frame of the tenant's control
  journal, the cell assignment, the state, a configuration sequence number, the tenant's
  **group** and its **placement** (`movable` or `pinned`); meta's cell entries carry the cell id, the cell tenant's frame, the state and
  the metadata version. No id is well known (section 3.8): a second cell learns meta's frame
  when it joins the fleet, from the cell that hosts meta.
- Every peer and client message is framed by `(TenantId, JournalId)` (section 3.8).
- The checkpoint record format has both its `Inline` and `Ref` forms (section 3.9).
- No component assumes there is only one cell: every lookup goes through meta's directory.

**Tenant groups and placement** (decided on 2026-10-04). Two attributes of every tenant, both
recorded in meta's tenant entry when the tenant is registered and never changed afterwards:

- **Group: `internal` or `users`, and nothing else.** The group only separates the tenants paros
  needs to administrate itself from the tenants it serves. `internal` tenants are created by
  paros's own operations: `init` creates meta and the first cell tenant, and adding a cell (M12)
  creates its cell tenant. The tenant API (`parosctl tenant create`, the front door) creates
  `users` tenants only, and meta refuses an `internal` registration from it. A front door serves
  `users` tenants only.
- **Placement: `movable` or `pinned`, the creator's choice.** It is independent of the group: an
  `internal` tenant may move (meta's journal moves to a dedicated cell, M12), and a `users`
  tenant may be pinned. `parosctl tenant create` registers a tenant `movable` unless told
  `--pinned`. `init` registers a cell's cell tenant `pinned`, since it is that cell's registry
  and capacity and lives and dies with it, and meta `movable`. Meta refuses a move of a `pinned`
  tenant when it applies the move's first entry, so no step of the move runs.

**Moving a tenant (M12)**, never a `pinned` one, reconfigures its journals and matchmaker set onto the target cell, then
transfers ownership with `SetLeader` on the tenant's control journal (the one moment ownership
changes), then flips the directory pointer, then the old cell forgets the tenant: the AWS
guidance's four migration phases, copy, flip, redirect, forget. The directory entry is a pointer,
never the authority: if it disagrees with the control journal's generation, the generation wins.
Meta moves the same way, being one journal; moving it to a dedicated cell is the escape hatch from
co-locating it with tenants.

**Open for M12: the cell inside the configuration.** Today a matchmaker's registry binds an
acceptor set and its quorum system to a ballot, and names no cell. Node ids are fleet-unique, so a
reconfiguration onto another cell's machines already moves a journal's data. If a configuration
also carried its `cell_id`, the move would itself be decided by Paxos: the effective
configuration (the highest-ballot reconfiguration a matchmaker quorum holds) would name the cell
that owns the journal, the directory would become a cache of that fact, and a reconfiguration
naming another cell for a `pinned` tenant's journal could be refused where it is registered. The
cost is one field in `AcceptorConfig` that the core never decides on, and every node knowing its
own cell. Recorded on #232.

**M12, "Multiple cells"** (#232, #233): adding a second cell, removing a cell (its id goes into a
tombstone set so it cannot silently rejoin), moving tenants between cells, moving meta, a separate
router role, splitting meta by range, placement across cells (each cell entry with a configured
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
  stored in `Config`. They are written once, unset → set, and a later mismatch is refused at boot
  like any `Config` mismatch.
- `TenantId(u64)`: random, drawn by the creator and recorded by meta in the `REGISTERING` step;
  meta refuses a duplicate at apply and the creator redraws. It is fleet-unique, so moving a
  tenant between cells never needs a new id. The system tenants are no exception: each cell's
  cell tenant gets a random id at the cell's `init` (so two cells' cell tenants differ), and the
  meta tenant gets one when `init` creates the fleet, kept when meta moves. Meta records both
  (its own in its first entry, each cell tenant in that cell's entry), so its duplicate check
  covers them too. Meta records each tenant's group (`internal` or `users`) and its placement
  (`movable` or `pinned`) beside its id (section 3.7). FDB gave each metacluster an id prefix for the same goal; a random draw
  checked by meta needs no prefix.
- `JournalId(u64)`: random, unique within its tenant, recorded and checked at apply by the tenant
  coordinator, the single writer of the tenant's control journal; a duplicate is refused and the
  creator redraws. A tenant's **control journal** has a random id too, drawn with the tenant and
  recorded where the tenant is recorded: in meta's tenant entry, and for the two system tenants
  in the cell plan. A journal's id never changes when its tenant moves; a control journal that
  recovery rebuilds (section 3.10) gets a new one, so the old and the new can never be mistaken
  for each other.
- **Discovery replaces convention.** The only fixed starting points are a machine's addresses:
  the frames of the cell's and meta's control journals are learned from any machine of the cell
  (section 3.2), and everything below them through their folds.
- **Every peer and client message is framed by `(TenantId, JournalId)`**, riding the `Deliver`
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
  `node_id`, current assignments only. That is what keeps checkpoints small.
- **Trigger**: checkpoint when the log since the last checkpoint reaches `k ×` the current state
  size, plus a time bound, which caps the extra writes at `1/k`. Kafka (KIP-630) snapshots only
  after a minimum number of bytes and a minimum share of changed records, and KIP-876 added a
  time trigger; Redpanda snapshots its controller after each command or at most every 60 seconds.
  Truncation may be delayed until known readers (machines folding the registry, front doors) have
  passed the checkpoint, or a time bound elapses, as KIP-630 delays advancing the log start until
  live replicas caught up or a timeout passed.
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
  it is needed when a journal's state outgrows one batch (meta in a large fleet, M12). Readers
  handle both forms from the start, so adopting `Ref` later changes only the writer.

### 3.10 Recovery

Losing a control quorum is recoverable without unsafe Paxos surgery, at both levels, because
control journals are rebuilt from below (decided on 2026-10-02, #225, #231). User data is never
touched: the tenants' journals have their own quorums.

- **Cell.** `parosctl init --recover` starts a fresh cell control journal, under a new random
  `JournalId` recorded in the cell plan, and a new recovery generation; live machines re-register; tenant coordinators re-report from their own control
  journals; any node still holding the old configuration is refused.
- **Fleet.** Meta's directory is rebuilt from the cells' tenant lists, the way FDB's metacluster
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
| Batcher | stateless | to build |
| Unbatcher | stateless | to build |
| Matchmaker | storage | `Matchmaker`, `run_matchmaker`; one set per tenant |
| Front door | stateless | to build |
| Coordinator (meta, cell, tenant) | stateless | to build |

## 5. Failure model and zones

The service survives disk loss and machine loss in one region: a corrupted record is CTRL's
`faulty` and repaired by the protocol, a wiped disk is refused at boot as amnesia and the
identity is replaced by reconfiguration, a crashed machine restarts as an existing member, a dead
machine is reconfigured out and its journals placed elsewhere. This is what the simulation
already exercises; what changes is who drives the healing: today the harness's client composes
the reconfigurations, in the service the tenant's coordinator does, from desired state. Losing a
control quorum is recoverable at both the cell and the fleet level, without touching user data
(section 3.10). Cross-region replication and witness replicas are out of scope.

Zones are failure domains, and a cell spans enough of them for its quorums (section 3.7). What
the WPaxos read (section 10) established for one region with several availability zones:

- Surviving one zone loss needs `fz = 1`, and then every Phase 2 spans two zones, exactly like a
  zone-balanced majority, while tolerating fewer node failures than that majority. WPaxos's
  latency win exists only at `fz = 0`, which gives up zone survival. Its per-zone quorum system
  is therefore not adopted.
- paros's grid cannot express zone survival either: rows and columns are positional over sorted
  ids, and with column = zone a zone loss kills every row, with row = zone it kills every column.
  The grid stays the throughput mode, with its cost (one dead acceptor freezes its column until
  reconfiguration) stated to the tenant that picks it.
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

- Invariants in the audit, where the fact arrives: one owner per generation, generations
  monotone and present in the log, `seq` dense per journal, a `Write` never re-accepted with
  other bytes, `Truncate` monotone and `first_seq` never above a served cursor, a `Truncate`
  accepted only from the current owner, a `SetLeader` winning at most once per `expected_gen`, a
  capacity slot booked at most once, a tenant never reaching a journal outside its own
  `TenantId`, no component reaching a journal by an id it did not learn (the simulation draws
  every frame, the system tenants' included, per seed).
- Control-plane invariants: a child keeps serving through its parent's outage; folding from a
  checkpoint yields the same state as folding the full history; meta's directory equals the union
  of the cells' tenant lists (assignments and counts), checked even with one cell (FDB's
  metacluster consistency checker); every fleet operation resumes correctly when re-run after a
  crash at any step; a recovery rebuild yields exactly the same tenants.
- A real linearizability checker in the workload's `check()`, over the four-call history with
  `Ambiguous` outcomes, against the sequential model of a journal (an owner, a generation, a
  dense log, a floor). It replaced the per-operation rules of `ClientHistory` (#205).
- The three races made likely rather than lucky, each a knob or a hook with its own BUGGIFY
  location and its reachable: a `SetLeader` drawn in the middle of a pipelined burst, a client
  timeout shorter than the ack so a retry crosses an ownership change, a `Truncate` racing a
  reader's cursor.
- Control-plane shapes: one cell hosting meta, now; a crash at each step of `init` and of tenant
  creation; a crash between a checkpoint's write and its truncate; a truncate refused from a
  stale owner; meta unavailable while tenants serve; a lost cell control quorum recovered by
  `init --recover`. In M12: a second cell, a tenant move and a meta move.
- New BUGGIFY sites for every new decision the driver, the front door and the coordinators take,
  and the coverage-guided sweep saturating over them.

No new model checker and no separate specification: the two existing sans-IO model checkers
stay as they are.

## 7. What changes against today

- The journal API of #185 (`Append`, `Read`, `CheckTail`, `Trim`) is cut over to the four calls.
  No compatibility layer. The chain workload's operation ids for retired calls stay reserved.
- `Truncate` gains the `(generation, owner)` fence of `Write` (#227, #228).
- The "#186: paros runs no application" line becomes: paros runs no *user* application, and one
  journal-control state machine per journal, in `paros-core`, judged at apply.
- The `(client, seq)` at-most-once session ledger goes away; the log is the deduplication table.
- The read-index path retires; the leaderless read serves `Read`.
- Journal ids stop being `128 +` a directory LSN, and the `1..=127` system range goes: a journal
  is named by `(TenantId, JournalId)`, both random u64 with no reserved range and no well-known
  value (`0` is unset), and that pair frames every message (section 3.8). The fixed ids of
  2026-10-02 (meta `1`, cell `2`, control journal `1`, `0..=255` reserved) go with them.
- The system journals dissolve into the four levels: the admin tenant becomes the cell tenant
  and its coordinator the cell coordinator; the meta tenant exists from day one; tenant names and
  desired state move into each tenant's control journal; capacity is owned by the cell
  coordinator alone.
- `parosd provision` (#208), which names the seeds to each other, is replaced by start-and-wait
  plus `parosctl init`, which creates the fleet (#216).
- Only the first user journal of a process may have matchmakers, proxies or replicas today; per
  tenant modes on many journals per process make journal-tagged planes and per-tenant matchmaker
  sets prerequisites, not options.
- `parosd` is uniform with a class, not one role per process.
- The homelab is not a target; Docker Compose on one host is.

## 8. Milestones

Milestones are labels (`milestone:M7` and up); `milestone:M6` stays the epic's umbrella. The
toy is the end of M9. The epic is #184, the backlog pointer #69, the verification track #24.

| Milestone | Name | Content |
|---|---|---|
| M7 | Journal API (#204, #205) | the four calls, the journal state machine in core, the wire and the driver, the chain workload's alphabet, the linearizability checker, the race knobs and hooks, the cut-over |
| M8 | parosd deployable (#206 to #209, #221, #220, #196; #176, #201, #202 join it) | Tokio providers linked, the stores on a real filesystem for the first time, the `JournalStores` opener, `Config` durable at `format`, `parosd provision` (replaced by `init` in M9), the uniform binary with class and capacity, Compose, `paros::client` (#221) and the `parosctl` CLI (#220), a tracing subscriber, exit codes |
| M9 | The fleet with one cell (#225, #226, #227 and #216 first; then #228, #210, #211, #229, #190, #212, #230, #192, #191, #231, #213) | the control hierarchy and its decisions, the fenced `Truncate` on the wire, random ids and the `(TenantId, JournalId)` frame, `init` creating the fleet, the cell tenant and its machine registry with class and capacity, the meta tenant with its directory and tenant creation state machine, the per-tenant coordinator via `SetLeader`, placement inside capacity granted by the cell, the checkpoint-and-truncate library, `init --recover`, the front door with JWT `Authz` routing through meta, per-tenant matchmaker sets, `parosctl status` |
| M10 | Roles per tenant (#193, #214, #194, #145, #195) | journal-tagged proxies and replicas, batchers and unbatchers, tenant modes applied by the tenant coordinator, the benchmark, then scale work |
| M11 | Zones (#215) | zone labels in `AcceptorConfig`, the placement rule, leader placement toward the writer's zone, zone-kill attrition and a zone-aware budget in the simulation, zone-spread matchmaker sets |
| M12 | Multiple cells (#232, #233) | adding and removing cells with tombstones, placement across cells, tenant locks, moving tenants and meta between cells, splitting meta by range, the `Ref` checkpoint writer, the router question |

Verification is not a milestone: every milestone carries its own share of section 6.

## 9. The toy, done means

From a fresh clone: `docker compose up`, then `parosctl init` against one seed, which creates the
fleet, its one cell and the meta tenant. Create a tenant through meta and mint its JWT. Create a
journal. `write`, `read` and `tail` from `parosctl`. `set-leader` to a second client and see the
first one refused, for a `write` and for a `truncate`. Kill one `storage` and one `stateless`
container and keep writing. Wipe one volume, see the amnesia refusal, and see the journal healed
by reconfiguration onto another machine. Stop the meta tenant's quorum and keep writing to an
existing tenant. Lose the cell control quorum, run `parosctl init --recover --dry-run`, then the
recovery, and see the same tenants back. `parosctl status` shows desired, available and current,
per tenant, per cell and for the fleet. The simulation is green in every shape and the
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

Compartmentalized Paxos and Matchmaker Paxos are in `docs/references/papers/`.

## 11. Alternatives considered

- **Recovering a control quorum by surgery.** Kafka KRaft is adding an override-voters flag to
  re-bootstrap a controller quorum that lost its majority, with data loss; ScyllaDB has a recovery
  procedure where an operator picks a recovery leader by host id. paros needs neither, because its
  control journals are rebuilt from below (section 3.10).
- **One cell playing both roles.** FDB's metacluster made a cluster exactly one of standalone,
  management or data, never two. paros co-locates meta with tenants in one cell because meta is a
  tenant with its own coordinator and quorums, which FDB could not do (its management data lived
  in the system keyspace). Moving meta to a dedicated cell is the escape hatch.
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
- **A provisioning step that names the seeds to each other**, a cluster file, gossip discovery and
  the front door as the rendezvous: #216.
- **Well-known system ids** (meta tenant `1`, cell tenant `2`, every control journal `1`,
  `0..=255` reserved; decided on 2026-10-02, reversed on 2026-10-04). They let a component find a
  control journal without asking, but every component must then agree on the convention forever,
  the cell tenant's id repeats in every cell (unique only within its cell), a rebuilt control
  journal reuses its predecessor's id, and the reserved range is a second id space every check has
  to special-case. Random ids recorded where they are created, and learned from any machine of
  the cell, cost one `Inspect` at bootstrap.
