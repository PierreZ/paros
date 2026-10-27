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
- The code still says `failures-per-partition` and `Durability { failures_per_partition,
  partitions }` until #748 (survive) and #755 (partitions leave durability) land. In this
  folder, `machines=N` is the old `failures-per-partition=N`.

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
- These three counts are the range from colocated to compartmentalized. There are no named
  presets.
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

- **The request.** `parosctl configure --tenant t [--journal j] [--failures-per-partition F]
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
  replaced on 2026-10-28 by per-journal counts, #753 (per-journal proxies), above: the tenant
  counts `proxy_leaders=` and `batchers=` and the tenant step for `Role::Batcher` go, and
  `frontends=` stays a cell matter, #723). A `Configure` of the tenant
  default may carry `frontends=`, `proxy_leaders=` and `batchers=` (`TenantRoles`, each at most
  64; refused on a journal target). The tenant control journal records them, and its checkpoint
  keeps them. `parosctl configure --tenant T --batchers 3` sends them; a count that the command
  does not name keeps the tenant's. The tenant coordinator applies `batchers=` and, since #723
  (frontend as a parosd role, decided on 2026-10-27), `frontends=`: one step per watch round,
  after the footprint step, in turn over the hosted tenants and for each over the two roles
  (`paros-coordinator`'s `tenant/roles.rs`). It first releases a slot of the role on a machine
  that leaves. Then it books or releases slots with `paros::model::placement` (`Role::Batcher` or
  `Role::Frontend`, `BookingTarget::Tenant`) until the tenant holds
  `min(count, stateless machines that can take one)`. A cell with no `stateless` machine
  places none. A cell that grows places the rest later. A slot is placement only: every machine
  serves both endpoints, and `Resolve` names the booked machines.
- **Still to do.** The count of proxy leaders and the `replicas=` mode field are recorded but
  not placed. `proxy_count` and `replica_count` sit in the core's `Config`
  under the format marker. The design change this needed is decided (2026-10-28, #754
  (counts off the format marker), above): the counts move into the configuration bound to the
  ballot, and #753 (per-journal proxies) places them per journal.

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
