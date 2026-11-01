[← 03-3-coordinators-and-placement.md](03-3-coordinators-and-placement.md) · [Index](README.md) · [03-5-frontend-and-resolver.md →](03-5-frontend-and-resolver.md)

### 3.4 Tenant modes

A tenant's desired state is FDB's `configure` (decided on 2026-10-04; amended on 2026-10-28): per
journal or as a tenant default, **what the journal must survive**
(`--survive machines=M,zones=Z,regions=R`, #748 (survive)), plus four per-journal **scale
counts**: `partitions`, `proxies`, `batchers` and `replicas` (#753 (per-journal proxies), #755
(partitions leave durability)).

**A journal states what it must survive** (decided on 2026-10-28, #748 (survive); it replaces
the `failures-per-partition` names of 2026-10-14, #523 (durability names), section 11).

- `--survive machines=0..3,zones=0..1,regions=0..1` replaces `--failures-per-partition`.
  `machines` is the old `F`. The user never types `F` or an acceptor count.
- The coordinator derives the acceptor count (`2F+1` per acceptor partition) and the spread from
  `survive`.
- The tenant keeps `survives: az | region` (below) as a constraint on the kind of cell
  (section 3.7). A journal that asks for more than its cell kind gives is refused, with the
  reason.
- Zone or region survival is never downgraded without a word. Until now, a cell with one or two
  zones placed a journal without zone survival (`zone_rule_binds`, section 5). That becomes an
  explicit outcome of the plan (placeable degraded, below).
- The rename landed on 2026-10-28: the CLI takes `--survive machines=N` (`zones` and
  `regions` take only 0 until #748 (survive) lands), a mode string is `machines=N,partitions=P`,
  and the field is `Durability::survive_machines`. `partitions` stays inside `Durability` until
  #755 (partitions leave durability) lands.

| 2026-10-04 | 2026-10-14 (#523) | 2026-10-28 (#748) |
|---|---|---|
| `single` | `failures-per-partition=0` | `machines=0` (1 acceptor) |
| `double` | `failures-per-partition=1` | `machines=1` (3 acceptors, the default) |
| `triple` | `failures-per-partition=2` | `machines=2` (5 acceptors) |
| (none) | `failures-per-partition=3` | `machines=3` (7 acceptors) |
| `grid {rows, cols}` | `failures-per-partition=F partitions=P` | `machines=F` and the scale count `partitions=P` |

**Durability named the guarantee** (decided on 2026-10-14, #523 (durability names); its flag
names were replaced on 2026-10-28 by `survive`, above). In FDB the name counts the copies; in
paros each acceptor stores a copy, so `double` stored three. What stays from 2026-10-14:

- **`machines` (`F`)** is Compartmentalized Paxos's `f`, per acceptor partition.
  Each acceptor partition has `2F+1` acceptors. The journal survives any `F` failures, and up to
  `F` per acceptor partition when they are spread. `F` is 0 to 3.
- **`partitions` (`P`)** is how many acceptor partitions a journal's writes spread over. It is a
  scale count, not durability (decided on 2026-10-28, #755 (partitions leave durability), below).
  The default is 1, the plain majority; 0 is refused. Each entry goes to one acceptor partition and
  is done at a majority of it; an election or a tail read needs a majority of every acceptor
  partition (#532 (acceptor partitions), landed). In the core an acceptor partition is a grid
  column: `QuorumSystem::Grid { rows: 2F+1, cols: P, acks: F+1 }`
  (`QuorumSystem::partitions`). Acks from two acceptor partitions never add up. Elections and
  tail reads stop when any acceptor partition loses its majority, so their availability is lower
  than one big majority; reads at a known offset need no quorum. The core predicates stay
  general (`acks` of `rows` in one column for Phase 2, `rows - acks + 1` of every column for
  Phase 1), and the simulation draws other `acks` too, the full column (`acks = rows`) included;
  the CLI and the docs expose only acceptor partitions.
- **A stuck round moves** (#533 (move a stuck round)): a Phase-2 round that stays open for
  `DriverTunables::partition_move_resends` re-sends moves to the next acceptor partition
  (`ColocatedNode::move_round`): the same slot, ballot and value, its tally counting only the new
  acceptor partition. Inside one ballot a slot has one value, and every Phase-1 quorum meets
  every acceptor partition, so a later election finds the value or proves it was not chosen.
  Slots are independent, so the new acceptor partition needs no backfill. Reconfiguration away
  from a failing acceptor partition still needs a majority of every old acceptor partition in
  the new configuration's Phase 1: replace an acceptor partition while it keeps a live majority.
- **Derived numbers are shown, never typed, and the partition count is always shown**:
  `survive 1 machine · 1 partition (any 1) · 3 acceptors`;
  `survive 1 machine · 3 partitions (any 1, up to 3 spread) · 9 acceptors` (the 2026-10-14
  label `1 per partition` became `survive 1 machine` on 2026-10-28, #748 (survive)).
- Docs say "acceptor partition" the first time in a paragraph; a network fault stays a
  "network partition". Zones keep their own word, the tenant's `survives`.
- **The writer mode flag** is `--writers single|multi` (it was `--mode`), so `single` never
  stands beside the durability flags. The type stays `WriterMode`.
- **A tenant's matchmaker set** has `2F+1` matchmakers, `F` the largest `machines`
  of the tenant (its default and every live journal, at least 1 for its control journal), on at
  most every storage machine; `partitions` plays no part, since the set stores configurations,
  not slots. The tenant coordinator grows or shrinks the set by handover (landed with #214
  (tenant modes), `machine/sets.rs`): it books the slots a larger set lacks, or hands a smaller
  set over onto the slots placement keeps and releases the others once it is in force.
- **Flexible quorums** (`QuorumSystem::Flexible`) stay core- and simulation-only; the CLI does
  not expose them. Changing a live journal's durability is an ordinary reconfiguration through
  `configure` (#214).

**Proxy leaders and batchers belong to the journal** (decided on 2026-10-28, #753 (per-journal
proxies); it replaces the per-tenant pools of 2026-10-04, #193, the per-`(tenant, region)` pools
of 2026-10-07, #253, and the tenant counts `proxy_leaders=` and `batchers=` of #604 (role
counts), section 11).

- `proxies`, `batchers` (multi-writer journals only) and `replicas` are per-journal counts.
  A count of 0 means the job runs in the acceptor or leader process (colocated). A count of N
  means N roles on `stateless` machines, spread zone by zone.
- These three counts are the range from colocated to compartmentalized.
- **One layout flag names the range** (decided on 2026-11-01; it replaces "no named presets"
  of 2026-10-28). `--layout colocated|compartmentalized` on `journal create` and `configure`
  is the one flavor choice of a journal. Every journal is Multi-Paxos with matchmaker
  reconfiguration, so the flag never names Multi-Paxos, and `multi` stays the writer mode.
  `colocated` sets the three counts to 0. `compartmentalized` sets each to 2, so one loss
  leaves one; a single-writer journal books no batcher. A count named beside the flag changes
  that count. `colocated` with a count above 0 is refused. The layout is not stored: it is
  read back from the counts (`Layout::of`, compartmentalized when a count is above 0), and
  `journal list` shows it. A mode string takes `layout=L` too.
- A role slot is a booking, not a process. Processes stay shared and keyed by
  `JournalIdentifier`.
- A stateless booking counts toward the spread rule, but it reserves almost no capacity, so
  idle journals do not fill the cell. That was the reason for the per-tenant pools of #193.
- Frontends stay at the cell: every `parosd` is a frontend (#723 (frontend as a parosd role)).
  Matchmaker sets, the tenant coordinator and the tenant control journal stay per tenant.
- The reason: the core already holds `proxy_count` per journal in `Config` (`ProxyId::of`), and
  a journal's proxy demand comes from its own fan-out.

**`partitions` is a scale count, not durability** (decided on 2026-10-28, #755 (partitions
leave durability); it replaces `partitions` inside `Durability` of 2026-10-14, section 11).

- `partitions` leaves `Durability`. It sits beside `replicas`, `proxies` and `batchers`.
- It is the one count that changes the quorum system, so a change of it is a full
  reconfiguration. A change of another count is a round change (below).
- When `partitions` is above 1, the plan states its availability cost in one line: an election
  or a tail read needs a majority of every acceptor partition.
- When `partitions` is above 1 and `proxies` is 0, the plan gives a hint and decides nothing:
  the leader sends every `Accept` itself.

**The counts leave the format marker** (decided on 2026-10-28, #754 (counts off the format
marker); it removes the "still to do" blocker below). `proxy_count` and `replica_count` move out
of the format marker into the journal's configuration that is bound to the ballot. A change of a
count is a round change, the same mechanism as a reconfiguration. Acceptors use the counts only
for wire checks, so safety does not depend on them. This is a `paros-core` change, so the sweep
proves it before it lands.

**`configure --plan`: what a change requires against what the cell has** (decided on
2026-10-28, #749 (configure plan), #750 (plan oracle), #751 (cell capabilities)).

- `parosctl configure <tenant>/<journal> ... --plan` runs the coordinator's own pure function
  (`paros::model::placement::plan`) and writes nothing.
- The answer is one table. For each role it gives what the change requires, what is
  available, and the spread by zone or by region. A grid shows its shape per column, for
  example `5×3 grid · per column 2/2/1`. "Available" is what the cell can still book for this
  tenant. Slots the journal already holds are not counted twice.
- There are three outcomes:
  - **placeable**: the change is recorded and applied;
  - **unplaceable**: the change is refused, and the reason names the shortfall and its cure,
    for example `proxy-leader short by 1, stateless, any zone`;
  - **placeable degraded**: the change is applied only with `--allow-degraded`.
- The plan states what remains after the loss of one failure domain, for example
  `after a zone loss: 0 spare per column, 3 columns at risk`, and the latency class of a write
  (cross-AZ or cross-region).
- A `configure` without `--plan` gives the same table in its answer. The structured reason
  replaces the bare `Unplaceable`.
- **The plan is a simulation oracle** (#750 (plan oracle)). For every `configure` the workload
  sends, the oracle checks the outcome after chaos settles. An unplaceable plan is never
  placed. A placeable plan converges to the planned shape, or the journal shows degraded with
  the reason the plan named.
- **`parosctl cell capabilities`** (#751 (cell capabilities)) is a fold over the registry. It
  shows what the cell can offer: the highest `survive`, the highest `partitions` and the free
  stateless slots. It adds no state.

*Landed* (#214 (tenant modes), 2026-10-15): `configure` for a journal's durability and the
tenant's default.

- **The request.** `parosctl configure --tenant t [--journal j] [--survive machines=F]
  [--partitions P] [--replicas R]` sends a `JournalRequest` with a `ConfigureOp`
  (`paros::client::journals::JournalOp::Configure`) and an idempotency id. The tenant
  coordinator judges whether the cell can place the mode now (as many storage machines as a
  grid needs, a free slot for each new acceptor: `paros::model::placement::plan`), and writes
  `Configure` to the tenant's control journal, or records the refusal (`Unplaceable` with no
  name, so a retry reads it back). The fold changes the journal's `desired`, or the tenant's
  default, which a create that names no mode takes (`partitions` 0 on the wire). The answer
  `configured` says the mode is recorded, not in force. The writer mode never changes.
- **The application** (`machine/modes.rs`). At each watch round, after the move step, the
  coordinator takes one step of one journal whose recorded placement (`TenantJournal::placed`,
  the create's configuration until a `Placed` entry) is not its desired shape on exactly its
  slots: book the slots the mode lacks; ask the leader to reconfigure onto the new members under
  the new quorum system (a smaller mode keeps the slots placement does not release); once in
  force, write `Placed` with the configuration in force, then, once the leader's GC watermark
  reached it, release the slots of the members it left. It retires none, unlike a move off a
  draining machine (a move off a slow or gone machine retires none either): the machine stays bookable, and a later mode may place the journal on it again, where a retired
  identity never comes back. A journal with a slot on a draining machine is the move's until the move is done. One acceptor partition is a majority
  over at most every storage machine, so a mode the cell grew into is applied later. Each step
  starts from what the registry, the tenant control journal and the leader hold, so a later
  term finishes a change an earlier one started. The create's configuration stays the journal's
  bootstrap configuration for life; views show `placed`.
- **The simulation.** `CREATE_JOURNAL` sends a configure on a BUGGIFY decision (a journal of
  the tenant or its default, a durability from the default to four acceptor partitions), and a
  create names no mode on one draw in four. Oracles: a refused configure records `Unplaceable`
  and no new mode, a configured journal is one its tenant created before, a placement is recorded
  only for a journal created before, and the placed members hold their slots after chaos.
- **Role counts** (#604 (role counts), decided on 2026-10-19; for proxy leaders and batchers
  replaced on 2026-10-28 by per-journal counts, #753 (per-journal proxies), above). A
  `Configure` of the tenant default may carry `frontends=` (`TenantRoles`, at most 64; refused
  on a journal target). The tenant control journal records it, and its checkpoint keeps it.
  The tenant counts `proxy_leaders=` and `batchers=` and their wire fields are gone (landed
  with R1, 2026-10-29).
- **Scale counts per journal** (#753 (per-journal proxies), #754 (counts off the format
  marker), landed with R1 on 2026-10-29). A journal's `Desired` carries `replicas`, `proxies`
  and `batchers`, each at most 64, beside its durability; the tenant default carries them too,
  and a create that names no mode takes them.
  - **The request.** `parosctl configure --tenant T [--journal J] [--layout L] [--replicas R]
    [--proxies X] [--batchers B] [--frontend-count N]`, and `journal create` takes the same
    `--layout` and counts. A count or a durability that the command does not name keeps the
    target's; a create that names a layout and no durability takes the tenant default's
    durability. A mode string is
    `machines=F,partitions=P,layout=L,replicas=R,proxies=X,batchers=B`.
  - **The bookings** (`paros-coordinator`'s `tenant/roles.rs`). One step per watch round,
    after the footprint step, in turn over the hosted tenants: first the tenant's
    `Role::Frontend` slots (`BookingTarget::Tenant`), then for each live journal its
    `Role::ProxyLeader` and `Role::Batcher` slots on `stateless` machines and its `Role::Replica`
    slots on `storage` machines (`BookingTarget::Journal`). A single-writer journal books no
    batcher. A replica never shares a machine with the journal's acceptors (placement's rule
    2). The step first releases a slot on a machine that leaves, then books or releases until
    the journal holds `min(count, machines of the class that can take one)`.
  - **Weightless slots.** A journal's proxy leader or batcher booking reserves no capacity
    (`Booking::weighs`, `Registry::weight`): the registry never refuses it as `NoCapacity`, and
    placement breaks ties inside a zone on these bookings after the weighed ones. A tenant's
    frontend slot and a replica still weigh one slot.
  - **The counts in force.** `proxy_count` and `replica_count` are part of every
    `AcceptorConfig`, bound to its ballot, and on the wire (`AcceptorConfig` fields 9 and 10;
    `Reconfigure` takes them as optional fields, absent keeps the counts in force). The mode
    step (`tenant/modes.rs`) composes the journal's configuration with the booked proxy leader
    and replica counts. A journal whose configuration in force names other counts is not
    settled, and the step reconfigures it onto the same members: a round change. The
    configuration a node runs decides delegation (`ProxyId::of(slot, proxy_count)`) and reply
    ownership (`AcceptorConfig::reply_owner`). An acceptor checks a proxy's rank against the cap
    of 64, never the count: it may not know the configuration of the ballot it votes in yet.
  - **Bootstrap counts.** `Config::proxy_count` and `Config::replica_count` stay as the counts of
    the bootstrap configuration (`Config::bootstrap`), so a plain deployment without matchmakers
    still names its proxy leaders and replicas. The format marker no longer records them
    (`Config::recorded`, store format version 6): a restart may name other counts, and a boot is
    never refused for a count.
  - **Serving.** Every machine serves the batcher endpoint, and `Resolve` names the machines
    that hold a batcher slot of any of the tenant's journals. A leader resolves proxy rank `r`
    only to a lane its driver holds, and runs the round colocated when it holds none
    (`delegation_choice`).
  - **Proxy leaders on machines** (#765, landed 2026-10-30). Rank `r` of a journal is the
    machine of its `r`-th `Role::ProxyLeader` booking, in booking order. Every machine reads
    the ranks from its registry fold, and a leader or an acceptor sends to rank `r` through
    the peer lane of that machine. The proxy leader has no endpoint of its own: a message
    carries only its journal, and a `stateless` machine serves no journal it is a proxy
    leader of. So the node loop of a machine that holds a slot of a journal it does not
    serve steps that journal's delegated `Accept`, `Accepted` and `Nack` into a proxy leader.
    The proxy leader is born at the first delegation, with the rank its `reply_to` names and
    the `C_b` it carries, and a delegation that names another rank replaces it. It stops when
    the machine loses the slot.
  - **Replicas on machines** (#765, landed 2026-10-30). A `storage` machine that holds a
    `Role::Replica` slot of a journal runs the journal's replica in its node loop, with the
    code of `run_replica`. The machine is no acceptor of the journal, and a journal's learners
    on a machine are the registry's pool. So the leader's `Commit`s and beats already reach it,
    and the node loop steps every message for a journal it does not serve into the replica. A
    `Read` of that journal is a leaderless read of the replica's fold. The store is at
    `replicas/<tenant>/<journal>/`, provisioned at its first open under the journal's bootstrap
    acceptors with the machine as the replica, and no record line names it: a replica holds no
    promise, so a lost store is a first boot that learns again. A storage fault parks the
    replica for `quarantine_ticks`. A released slot stops it and keeps its store. `Resolve`
    names the machines that hold a replica slot of the tenant's journals (`replicas`, field
    15), so a client can send its reads there. The replica settles in the node loop,
    not off it (#613 (settle off the node loop) is for the acceptors).
  - **The simulation.** A create or a configure draws the counts on one draw in three, 0 to 3
    each. The plane's reconfiguration composer changes the counts on a coin, the proxy count up
    to one past the deployed proxy leaders, so a leader meets a rank it has no lane for; nodes
    with a lagged bootstrap count run beside the others. Oracles: a journal holds at most one
    slot of a role per machine, each on a machine of the role's class, a replica on no machine of
    the journal's acceptors, and a single-writer journal no batcher; gates fire when a journal
    holds its proxy leaders, batchers and replicas after chaos.

**`machines=0` survives no zone** (decided on 2026-10-07, #215): a majority over one
acceptor cannot meet the zone rule of section 5. It stays a mode without zone survival, for
development and the toy, shown `Degraded` in `parosctl status` (section 3.6), and it is refused
for every control journal and every election journal. Control and election journals are born at
`machines=1` (the cell's own on its three founding members, one per zone) and
raised to `machines=2` by the cell coordinator, through ordinary reconfiguration,
once five `storage` machines span three zones. Since 2026-10-28 (#748 (survive)), a journal
that asks `machines=0,zones=1` is refused with the reason, never placed without zone survival.

**What a tenant survives** (decided on 2026-10-07, #252): a tenant's universe directory entry carries
`survives: az | region`, recorded at `REGISTERING` and mirrored into the tenant's control journal
(rebuild from below, section 3.3). It constrains the kind of cell the tenant may live in, a
regional cell for `az` and a multi-region cell for `region` (section 3.7), never whether it may
move: its `paros/movable` tag alone decides that (section 3.7).

**How roles are sized** (decided on 2026-10-04; amended on 2026-10-28, #753 (per-journal
proxies)). Until 2026-10-28, stateless roles (frontends, proxy leaders, batchers) were
**per-tenant pools**: one pool per role, shared by all the tenant's journals and keyed by
`JournalIdentifier` inside (#193). Now proxy leaders and batchers are per-journal counts
(above), and frontends are the cell's (#723). Processes stay shared and keyed by
`JournalIdentifier`; only the bookings are per journal. Storage roles — acceptors, replicas — are **per journal**, because they hold that
journal's data; matchmakers are one set per tenant. **Placement spreads load across the cell**
(#212): every role's instances go to the eligible machines of its class zone by zone in round
robin, least-loaded inside a zone, ties broken by a draw seeded per tenant (shuffle sharding
inside the cell), and one journal's storage instances never share a machine (amended on
2026-10-07, #215: least-loaded slots first could put three of five acceptors in one zone). With
six `storage` machines available, a tenant with three journals of two replicas each gets its six
replicas on six machines; each journal's proxy leaders spread the same way over the
`stateless` machines, zone by zone.

**Roles in a multi-region cell** (decided on 2026-10-07, #253; amended on 2026-10-28, #753
(per-journal proxies)). The 2026-10-07 decision pooled proxy leaders, batchers and frontends per
`(tenant, region)`. Proxy leaders and batchers are now per-journal counts, so a journal's
proxy leaders and batchers are booked in its leader's region, and there is at least one frontend
per AZ. `ProxyId::of(slot, proxy_count)` (`membership/`) assigns `slot_rank(slot,
proxy_count)`, so proxies spread across regions would put a cross-region hop on a share of every
journal's slots. `ProxyId` stays a logical rank, and the driver resolves rank `r` to the
journal's booked proxy leader of rank `r`.

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
its first entry; there is no bootstrap exception. A storage fault in one registry closes the whole
set, and the set opens again at its next request. **Every tenant's matchmaker set is born on
the cell's founding members** (decided on 2026-10-10, #190): each founding member hosts the
matchmaker named by its node id, on the address it serves its journals at, and provisions a
tenant's set at the first request that names that tenant. The tenant coordinator then places
the set on the cell's storage machines and moves it off a draining machine by generation
handover (decided on 2026-10-11, #501 (matchmaker sets off the founders), section 3.3); every
storage machine hosts the matchmaker named by its node id, and a machine that is not a founding
member provisions only a set the registry books on it. A matchmaker request names
the matchmaker it addresses, and a process that is another one drops it, so a machine that took
over an address never answers for a set it does not hold.
Reconfiguration is the operational primitive for everything, so no tenant opts out of it.

Every tenant has a **minimum footprint**, booked in slots against its cell's capacity when the
tenant is created: one coordinator slot, its matchmaker set, one acceptor quorum of its
durability for data and one `machines=1` quorum for its control journal (amended on
2026-10-07, #215: a `machines=0` tenant's control journal still needs
`machines=1`). The universe tenant and the cell
tenant count too. A cell refuses a tenant whose footprint it cannot book (landed with #476
(tenant footprint), section 3.3: the footprint's durability is `machines=1`
until a creation names one).

**The control plane is a tenant's mode too** (decided on 2026-10-18, #685 (special tenants
are tenants); the mode record landed on 2026-10-31). The special tenants (the cell tenant and
the universe tenant) are the tenants the system uses. They are tenants like any other, except
that the platform manages them. Their journals are the cell control journal, the election
journal and the universe directory. An `admin` call sets how their `F` is chosen:

- `parosctl cell configure --control-plane auto [--survive machines=T]`: the cell coordinator
  wants `F = min(T, (storage machines - 1) / 2)`. `T` is 1 by default. A cell that recorded no
  mode follows auto with `T = 1`, so `init` needs no extra step.
- `parosctl cell configure --control-plane manual --survive machines=F`: `F` stays as it is. The
  registry refuses it at apply when the cell has fewer than `2F+1` storage machines
  (`ControlPlaneTooWide`).
- The mode is a `SetControlPlane` record in the cell control journal, applied once per request
  id (`paros::model::registry::control_plane`). `status` shows it as the special tenants'
  `survives`, and each of their journals as `<mode> · machines=<wanted> wanted ·
  machines=<in force> in force`.
- **A user tenant's control journal follows the tenant's own mode** (decided on 2026-10-31,
  #685): `2F+1` acceptors, `F` the tenant's largest `survive_machines` (its default and every
  live journal's, as its matchmaker set). The coordinator grows it from the founding members
  onto booked storage machines (`paros_coordinator::tenant::controls`), and moves it off a
  machine that leaves. The founders stay members, so every reader still dials them. Only the
  special tenants (the cell tenant and the universe tenant) follow the control plane mode.
- The universe tenant's matchmaker set moves like any tenant's (#685 step 3,
  `paros_coordinator::tenant::sets`). The cell tenant's set stays on the founding members until
  step 4: a handover onto admitted machines needs them to fold their booking from the registry,
  and the registry needs the set. The universe tenant's set has `2F+1` matchmakers, `F` the control
  plane's, on at most the machines placement may book, and its size is always odd: a set of
  two adds a machine the cell's elections wait on and no failure it survives. The universe set
  born on the founding members stays there while they are as many as it needs. It moves only
  to grow, and it keeps every founding member until step 5 of #685 drains them: it books
  slots only beside them. The capacity grant books slots for the universe tenant as for a hosted one.
- The cell control journal and the election journal still run on the founding members, so
  their `F` in force is the founders'. The next steps move them, then make the founders a boot
  hint only.
