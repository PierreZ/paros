[← 03-3-coordinators-and-placement.md](03-3-coordinators-and-placement.md) · [Index](README.md) · [03-5-frontend-and-resolver.md →](03-5-frontend-and-resolver.md)

### 3.4 Tenant modes

A tenant's desired state is FDB's `configure` (decided on 2026-10-04): per journal or as a tenant
default, a **durability** `Durability { failures_per_partition, partitions }`
(`parosctl journal create --failures-per-partition F --partitions P`), plus per-role counts
(`frontends=`, `proxy_leaders=` and `batchers=` per tenant, `replicas=` per journal).

**Durability names the guarantee** (decided on 2026-10-14, #523; it replaces the FDB names
`single`, `double`, `triple` and `grid {rows, cols}` of 2026-10-04). In FDB the name counts the
copies; in paros each acceptor stores a copy, so `double` stored three. The new names:

| Old | New |
|---|---|
| `single` | `failures-per-partition=0` (1 acceptor) |
| `double` | `failures-per-partition=1` (3 acceptors, the default) |
| `triple` | `failures-per-partition=2` (5 acceptors) |
| (new) | `failures-per-partition=3` (7 acceptors) |
| `grid {rows, cols}` | `failures-per-partition=F partitions=P` |

- **`failures-per-partition` (`F`)** is Compartmentalized Paxos's `f`, per acceptor partition.
  Each acceptor partition has `2F+1` acceptors. The journal survives any `F` failures, and up to
  `F` per acceptor partition when they are spread. `F` is 0 to 3.
- **`partitions` (`P`)** is how many acceptor partitions a journal's writes spread over. The
  default is 1, the plain majority; 0 is refused. Each entry goes to one acceptor partition and
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
  `1 per partition · 1 partition (any 1) · 3 acceptors`;
  `1 per partition · 3 partitions (any 1, up to 3 spread) · 9 acceptors`.
- Docs say "acceptor partition" the first time in a paragraph; a network fault stays a
  "network partition". Zones keep their own word, the tenant's `survives`.
- **The writer mode flag** is `--writers single|multi` (it was `--mode`), so `single` never
  stands beside the durability flags. The type stays `WriterMode`.
- **A tenant's matchmaker set** has `2F+1` matchmakers, `F` the largest `failures-per-partition`
  of the tenant (its default and every live journal, at least 1 for its control journal), on at
  most every storage machine; `partitions` plays no part, since the set stores configurations,
  not slots. The tenant coordinator grows or shrinks the set by handover (landed with #214
  (tenant modes), `machine/sets.rs`): it books the slots a larger set lacks, or hands a smaller
  set over onto the slots placement keeps and releases the others once it is in force.
- **Flexible quorums** (`QuorumSystem::Flexible`) stay core- and simulation-only; the CLI does
  not expose them. Changing a live journal's durability is an ordinary reconfiguration through
  `configure` (#214).

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
- **Role counts** (#604 (role counts), decided on 2026-10-19). A `Configure` of the tenant
  default may carry `frontends=`, `proxy_leaders=` and `batchers=` (`TenantRoles`, each at most
  64; refused on a journal target). The tenant control journal records them, and its checkpoint
  keeps them. `parosctl configure --tenant T --batchers 3` sends them; a count that the command
  does not name keeps the tenant's. The tenant coordinator applies `batchers=`: one step per
  watch round, after the footprint step, in turn over the hosted tenants (`machine/roles.rs`).
  It first releases a batcher slot on a machine that leaves. Then it books or releases slots
  with `paros::model::placement` (`Role::Batcher`, `BookingTarget::Tenant`) until the tenant holds
  `min(batchers, stateless machines that can take one)`. A cell with no `stateless` machine
  places none. A cell that grows places the rest later.
- **Still to do.** The counts of frontends and proxy leaders, and the `replicas=` mode field,
  are recorded but not placed. `proxy_count` and `replica_count` sit in the core's `Config`
  under the format marker, so placing them needs a change of that design.

**`failures-per-partition=0` survives no zone** (decided on 2026-10-07, #215): a majority over one
acceptor cannot meet the zone rule of section 5. It stays a mode without zone survival, for
development and the toy, shown `Degraded` in `parosctl status` (section 3.6), and it is refused
for every control journal and every election journal. Control and election journals are born at
`failures-per-partition=1` (the cell's own on its three founding members, one per zone) and
raised to `failures-per-partition=2` by the cell coordinator, through ordinary reconfiguration,
once five `storage` machines span three zones.

**What a tenant survives** (decided on 2026-10-07, #252): a tenant's universe directory entry carries
`survives: az | region`, recorded at `REGISTERING` and mirrored into the tenant's control journal
(rebuild from below, section 3.3). It constrains the kind of cell the tenant may live in, a
regional cell for `az` and a multi-region cell for `region` (section 3.7), never whether it may
move: its `paros/movable` tag alone decides that (section 3.7).

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
proxy_count)` (`membership/`) assigns `slot_rank(slot, proxy_count)`, so one pool across
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
durability for data and one `failures-per-partition=1` quorum for its control journal (amended on
2026-10-07, #215: a `failures-per-partition=0` tenant's control journal still needs
`failures-per-partition=1`). The universe tenant and the cell
tenant count too. A cell refuses a tenant whose footprint it cannot book (landed with #476
(tenant footprint), section 3.3: the footprint's durability is `failures-per-partition=1`
until a creation names one).
