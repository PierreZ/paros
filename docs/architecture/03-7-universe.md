[← 03-6-status.md](03-6-status.md) · [Index](README.md) · [03-8-identifiers-checkpoints-recovery-security.md →](03-8-identifiers-checkpoints-recovery-security.md)

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
the work order, no client resumes another's creation).
*Landed in part* (2026-10-15, #225, the tenant half of #513 (fleet Init duel)): the cell
coordinator writes the cell's step of each entry. At its install and at each watch round, it
reads the universe directory. For each `users` tenant of its cell that is `REGISTERING` and not
hosted, it books the tenant's footprint (`GrantCapacity`, section 3.3), then writes `HostTenant`
once the footprint is granted. For each one that is `REMOVING` and not dropped, it writes
`DropTenant`. The operator writes only the directory steps: it waits until the cell hosts the
tenant before it marks it `READY` (or marks it `ERROR` when the cell refused its footprint), and
until the cell dropped it before it removes it. So no operator claims the
cell control journal for a tenant. Before, each claim fenced the coordinator and the next term
fenced the operator in turn, the duel #513 removed from `init`. The sweep of 07424f8 left two
creations `REGISTERING` after the recovery tail (seed 15189977391382984753, which did not replay
red alone). The directory steps are still the operator's: the universe coordinator that
finishes them is #225's remaining work. A tenant in any state may be removed; only a `READY` or
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
  **tags** (section 3.7); the universe directory's cell entries carry the cell id, the cell tenant's `JournalIdentifier`, the state and
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

**Tags** (decided on 2026-10-15, landed on 2026-10-16, #575 (key:value tags); they replace the
tenant groups of 2026-10-04). Tenants, journals, machines and cells carry `key:value` tags. A tag
is a key of at most 63 bytes and a value of at most 255 bytes, and an entity carries at most 32
tags. A key that starts with `paros/` is **reserved**: only the control plane writes it, through
the command that owns it, and paros parses it into a typed policy. Every other key is a **user
tag**: opaque to paros, and no code reads one to decide. A user tag never changes an outcome.

| Reserved tag | On | Written by | Means |
|---|---|---|---|
| `paros/kind` | tenant | `RegisterTenant`, `init`, adding a cell | `internal` or `users` |
| `paros/role` | internal tenant | the same | `universe` or `cell` |
| `paros/movable` | tenant | the same | `true` or `false`; a cell tenant is always `false` |
| `paros/class` | machine | `RegisterNode`, from `PAROS_CLASS` | `storage` or `stateless` |
| `paros/zone`, `paros/rack` | machine | `RegisterNode`, from `PAROS_ZONE`, `PAROS_RACK` | where it runs; placement spreads over zones |
| `paros/kind` | cell | `AddCell` | `regional` or `multi-region` |
| `paros/region`, `paros/regions`, `paros/witness` | cell | `AddCell` | the regions of its kind |

So the universe tenant is `paros/kind:internal, paros/role:universe, paros/movable:true`, a cell
tenant `paros/kind:internal, paros/role:cell, paros/movable:false`, and a served tenant
`paros/kind:users, paros/movable:true`. The capacity stays a typed field.
The decisions (Pierre, 2026-10-15):

1. **No stored inheritance.** A journal stores no copy of its tenant's tags. A selector
   matches a tenant's tag with `tenant.<key>` (`parosctl journal list -l tenant.team:x`).
2. **The tenant writes the user tags of its journals**, with its token, through the frontend
   (`FrontSetJournalTags`, Biscuit operation `journal.tag`, class `journal`). An admin can write
   them too, through the tenant coordinator.
3. **Reserved tags change only through their own commands**: the tag commands refuse a
   `paros/` key at decode, so a reserved tag is enforced at apply. The cell tenant is always
   pinned.
4. **The machine class is a reserved tag** (`paros/class:storage`).
5. **The caps** are 32 tags per entity, keys of at most 63 bytes, values of at most 255 bytes.
6. **Authorization by tag** (Biscuit `tag(...)` facts) is a follow-up.
7. **No prefix marks an internal name.** An internal tenant shows as `<kind>:<name>`, its
   journals as `(control)`, `(election)` and `(directory)`, in every output. A user tenant's
   name refuses `:` (`name_refused`), so a user tenant never looks like an internal one.

**Setting user tags.** Each entity's user tags are set whole: a request names the full set,
and an empty set clears it. Every request has an idempotency id, judged at apply in the journal
that owns the entity, and acts once:

- a tenant's and a cell's in the universe directory (`SetTenantTags`, `SetCellTags`;
  `parosctl tenant tag`, `parosctl cell tag`);
- a machine's in the cell control journal (`SetMachineTags`; `parosctl cell tag-machine`);
- a journal's in its tenant's control journal (`SetJournalTags`, sent to the tenant coordinator;
  `parosctl journal tag`, through the frontends when `--frontends` is given).

A create carries the birth tags (`--tag` on `tenant create` and `journal create`). The views
show every tag, filtered by scope: a tenant scope sees only the `paros/zone` and `paros/rack`
tags of the machines it uses. A selector (`-l key`, `-l key:value`, comma-separated, all must
match) narrows `machine list`, `cell list`, `tenant list` and `journal list`.

**Moving a tenant (M12)**, any tenant tagged `paros/movable:true`, reconfigures its journals and matchmaker set onto the target cell, then
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

**Adding and removing cells, part 1** (decided on 2026-10-16, #232 (multiple cells)). These rules
are in force now:

- **An idle cell.** `parosctl init --idle` (`InitParams::idle`, `CellInit.idle`) forms a cell
  with no universe tenant (`CellPlan.fleet` is `None`). The cell elects its coordinator and
  serves its cell tenant. It hosts no other tenant until it joins a universe.
- **`universe add-cell`** (`FleetSession::add_cell`) has two halves, like `init`. The operator
  writes the directory half: `AddCell` with the cell's control journal and its **entry
  endpoint** (its founding members by id and address), then `MarkCell READY` once the cell
  joined. The cell's coordinator writes the cell half: `JoinFleet` with the **universe
  pointer**, the directory journal and the machines of the cell that hosts it. The operator
  asks the coordinator to join with `JoinUniverse` (`0x5041_030D`, in the `CoordinatorInterface`
  group). The coordinator reads the directory at the pointer, and joins only when the directory
  holds its cell under its cell tenant's control journal. A cell joins once, and joins one
  universe.
- **The pointer lives in the cell control journal**, in `JoinFleet`, not in the cell plan. The
  plan of an idle cell is decided before the cell knows its universe, and a plan is never
  edited. The coordinator is the one writer of the cell control journal, so it is the one
  writer of the pointer. This amends the sentence above that a cell records the universe values
  "in its cell plan at admission".
- **Reading the directory from another cell.** A machine whose cell does not host the universe
  tenant reads the directory through the pointer, with a client over the hosting cell's
  machines (`Client::elsewhere`). The coordinator, the resolver, the views and the frontend use
  it.
- **Tenants in a second cell.** `tenant create --cell NAME` (`FleetSession::with_cell`) creates a
  tenant in a `READY` cell. Without `--cell`, the operator's own cell takes it when it is
  `READY`, else the first `READY` cell. Placement across cells is still open (below).
- **Resolve across cells.** A machine answers `other_cell` for a tenant of another cell, with
  that cell's entry endpoint. The client asks that cell once more (one hop). A frontend serves
  only the tenants of its own cell; another cell's tenant is unknown there.
- **`universe remove-cell`** marks the cell `REMOVING`, then writes `RemoveCell`. The fold
  refuses it while the cell hosts a tenant other than its cell tenant (`CellNotEmpty`). It
  removes the cell, its entry endpoint and its cell tenant, and puts the cell id in a tombstone
  set (`removed_cells`). `AddCell` refuses a tombstoned id (`CellTombstoned`). The cell that
  hosts the universe tenant is not removed.

Still open for M12: moving tenants and the universe tenant between cells, placement across
cells, tenant locks, splitting the universe tenant, and multi-region cells. A move needs the
cell fence of section 3.2 to let a reconfiguration cross cells, and the `cell_id` in
`AcceptorConfig` (above), so it is a later part. `JoinUniverse` has no authorization check yet: it
changes nothing unless the directory already holds the cell, which only an admin writes.
