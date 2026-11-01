[← 03-2-machines.md](03-2-machines.md) · [Index](README.md) · [03-4-tenant-modes.md →](03-4-tenant-modes.md)

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
one acceptor partition (majority Multi-Paxos, section 3.4), never several, and like every journal it
has its tenant's matchmaker set (decided on 2026-10-04).

**Where candidates run** (decided on 2026-10-04, amended on 2026-10-09, #240). The founding members
campaign for the first cell coordinator over the election journal `cell init` creates on them,
born at `machines=1` (section 3.1). The cell and universe coordinators run on the founding members until a
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
  caller's jitter, on its own clock, over reads that served. A read no server served shows
  nothing and counts for nothing (PR #705 (fix main)): a watcher whose
  election reads fail for a lease deposed a live coordinator, and the next term took 11 s to
  open. The library draws no randomness. A caller that campaigns
  earlier deposes a live leader: that costs availability, never safety.
- **Uuids.** A candidate derives a fresh uuid per term from the caller's seed. A
  restarted leader does not take back its old term: it waits a lease and takes the next term.
- **Log space.** Each renewal describes the whole leadership, so the leader truncates the
  election journal to its own latest renewal once `compact_after` records lie below it. A
  renewal is its own checkpoint. A reader that starts at the floor anchors on the leader's
  record there: a renewal, or the campaign or hand-off that began the term when the leader
  truncated before its first renewal counted. Only that record anchors a reader. A renewal
  elsewhere may be a deposed leader's late one, so a reader that the floor outran keeps the
  records it reads at the tail. It anchors when the floor reaches one of them (decided on
  2026-10-18, a fix-main red). Nothing else may truncate an election journal.
- **The interface.** Records carry the candidate's interface: its address, and the encoded
  `InterfaceRef` of its term's group (`reference`, #404), opaque to the election. The
  coordinator publishes them with its first renewal after its term's duties; until then both
  are empty.
- **The coordinator.** Every founding member runs a candidate in a task beside its node loop.
  When it wins a term, it installs the term's uuid on the cell control journal with
  `SetLeader(uuid, current)`, folds that journal to its tail, and admits again every registered
  machine that is not a founding member and not retired (`Admit` is idempotent). Then it
  publishes its interface. A lost install ends the term, and the coordinator resigns.
  The coordinator renews each time a renewal is due while the install and the term's later
  duties run (decided on 2026-10-15, #542 (coordinator starves its lease while it
  installs)). So the lease covers one renewal's round trip, never an install: a slow cell
  kept no term before.
- **Interim.** `cell add-machine` and `cell drain-machine` are not yet requests to the
  coordinator (#212, #225). Their admin session still claims the cell control journal and
  fences the coordinator. The coordinator does not fight back within its term: it resigns,
  and the next term installs a fresh uuid (#349: a fenced term cannot register a moved
  machine). The fleet operations claim nothing on the cell control journal. `parosctl init`
  waits until the coordinator installed its uuid, then runs the fleet steps, and the
  coordinator writes the cell's `JoinFleet` (#513 (fleet Init duel)). A tenant's creation
  and removal write only the universe directory, and the coordinator writes the cell's
  steps: the tenant's footprint, `HostTenant` and `DropTenant` (decided on 2026-10-15, #225,
  section 3.7).
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

*Landed* (#212 (placement), 2026-10-11): the placement function and the acceptor bookings of a
create.

- **The function.** `paros::model::placement::plan` maps the desired count, the machines the registry
  offers and the target's live bookings to steps: slots to book and bookings to release. It is
  pure, so a new coordinator that folds the same registry computes the same steps. Its rules are
  section 3.4's: class, no machine with two storage instances of one target, zone
  by zone in round robin, least busy then least loaded inside a zone (#837 (remove role slots)), zones that tie in the order of their
  machines' mean load, ties broken by a draw seeded with the tenant id, and a machine marked
  `Down` only after every machine that is up. A release takes a down machine first, then the
  most loaded.
- **Only a machine that serves the cell is offered** (decided on 2026-10-13, #511 (placement
  books machines that never served)). `cell add-machine` writes `RegisterNode` before its
  `Admit`, and an `Admit` may never win: a wiped founder's replacement that promised in a
  `cell init` refuses every admission (`in_cell_init`, by design: a stalled `cell init` is
  safer than a machine in two cells). The cell coordinator's watch writes `MachineAdmitted`
  once a registered machine answers `Identify` with this cell's id (`Registry::serves`), and
  `paros::model::placement::machines` offers a founding member or such a machine only.
- **Booking before the create.** The tenant coordinator plans a create's acceptors over the
  cell's storage machines, founding or admitted (#474), and books one acceptor slot
  on each in the cell control journal, through the term's cell session, before it writes the
  create. The elected cell coordinator is still every tenant's coordinator, so the request for
  capacity is a call into its own session (#476 (tenant footprint) made it a `CapacityRequest`).
- **A refusal is recorded.** A create the zone rule refuses, or too wide for the founding
  members, is written `Unplaceable` under its request id, so a retry reads the refusal back; the
  caller asks again under a new id. A create whose founding members are not all registered yet
  answers `unavailable`.
- **Releases.** A delete releases its journal's slots after the tombstone. A slot booked for a
  journal its tenant does not hold live (an abandoned create, a release that did not land) is
  released by the next create in that tenant, once the claim on the tenant control journal
  fenced every earlier term. The watch releases the slots of a tenant the cell dropped.
- **No capacity** (#837 (remove role slots), 2026-11-01). A machine takes a role while it serves
  the cell. Inside a zone, placement books the machine with the lowest busyness step first: the
  larger of its CPU and disk busy ratios from its last `Load` window, in steps of 10 %. The cell
  coordinator's watch polls these windows each round for the slow judge and hands them to
  placement, which stays a pure function of its inputs. Among equal steps, the machine that
  hosts the fewest roles goes first. `PAROS_CAPACITY` is ignored.
- **The simulation.** A term's first create may stop right after its bookings (a BUGGIFY decision), and a `hint!`
  sits between the bookings and the create. After chaos, every live journal of a hosted tenant
  holds one acceptor booking per member. Gates: a slot no journal holds is released, a journal's
  members span two zones, and a less busy machine takes a role over a less loaded one. A journal
  is excused from the heal oracle only when the cell has fewer storage machines to go to than
  members that fail to serve (#739 (heal from ground truth)): never for want of a slot.
- **Journals on admitted machines** (#474 (journals on admitted machines), landed on
  2026-10-11). Every machine of a cell runs the node loop. An admitted machine follows the
  control journals through the founding members its admission names. A machine serves a
  journal while the registry books an acceptor slot of it there and its tenant holds it live. It
  stops the journal when the slot is released, and it keeps the store. A journal starts as a
  member of its create's configuration, or else as a spare beside it. The configuration depends
  only on the create, so every start opens the store under the same one. `Reconfigure` and
  `Retire` name their journal, and `ReconfigureMatchmakers` names its tenant, because a machine
  serves many. No journal or tenant is the default (decided on 2026-10-11, #439 (operator calls
  name their journal)): a node refuses a call that names none, or one it does not serve
  (`unknown_journal`, `unknown_tenant`).
- **Moves.** `cell drain-machine` (`CellSession::drain`) marks a machine draining. At each watch
  round, the tenant coordinator takes one step of one move, in turn over the journals with a
  slot on a draining machine. A step books a slot on another storage machine, or asks the
  journal's leader to reconfigure onto the machines that stay, or retires the old member and
  releases its slot. The retirement waits until the leader's GC watermark reaches the
  configuration in force. Only a member that the leader's floor names retirable is retired: a
  floor that rose past an earlier floor names none, and the released slot stops the journal on
  that machine all the same. The release does not wait for the old member's answer: the
  watermark proves that no leader asks it again. The coordinator reads the cell control journal
  to its tail before each step, because an operator drains through a session of its own. Each step starts from what the registry and the journal
  hold, so a later term finishes a move that an earlier term started. No step names the
  draining machine in a configuration. The simulation drains an admitted machine on a BUGGIFY
  location of `ADMIT`, and the first term that advances a move resigns on a BUGGIFY decision.
  Oracles: a live journal holds at least as many slots as its create named, and only a member
  that left the registered machines loses its slot. After chaos, a journal that has a machine to
  go to holds no slot on a draining machine.
- **Re-placement** (#475 (re-place after the Down bound)). A machine down past
  `machine_replace_after` is gone (section 3.2). Its journals and sets move as a drain moves
  them, and the machine stays registered. A move with no spare machine for a gone machine's slot
  waits: it never shrinks a configuration off a gone machine, because that machine can answer
  again. The simulation takes a machine that holds a slot down
  for longer than the bound on a gone-machine seed (`paros_sim::world::gone_machine`). Oracle:
  after chaos, every data journal that the cell can hold shows `healthy` in `parosctl status`.
  The view judges a journal against the configuration its bookings place.
- **Rebalance** (decided by Pierre on 2026-11-01, #687 (rebalance a cell), first part). Placement
  spreads a journal at create, and the moves take it off a machine that leaves. Nothing else
  moved a journal, so a machine that `cell add-machine` admitted hosted only the journals
  created after it. The cell coordinator now evens out the roles of the machines of each zone.
  Each watch round, it compares the most and the least loaded machine of a zone, among the
  storage machines that stay and are up. When the gap stays above `rebalance_spread` (default
  1) for `rebalance_rounds` rounds in a row, it moves one acceptor off the most loaded machine.
  It picks the first acceptor booking whose journal holds no role on the least loaded machine.
  The move uses the move path: book a slot elsewhere (never on the machine it leaves),
  reconfigure, then release the old slot. It retires no member, because the machine stays.
  Repair comes first: no rebalance starts while a machine is draining, slow, down or gone, or
  while a role sits on a machine that leaves. One rebalance runs at a time, and one that does
  not end in 64 rounds is abandoned. The rebalance in flight lives in the term only. A term that
  ends in the middle leaves an extra slot, which the mode change releases. A data journal of
  the cell tenant, a `CellData` RPC with `parosctl cell data`, busyness and leader balance, and a
  warm replica before the reconfigure are the next parts of #687. The simulation draws both
  tunables per seed. Gates: the cell picks a rebalance move, and a rebalance move finishes.
- **Maintenance and removal** (#359). `cell maintenance <machine> --for <duration>` (or
  `--zone <zone>`) writes a `Maintenance` entry: while the window is open, the coordinator
  re-places no role of a down machine that it covers. `--for 0` ends the window. The window
  counts on the coordinator's clock from the first time it folds the entry, so it is never
  shorter than the operator asked. `cell remove-machine` retires a drained machine: it must be
  draining and hold no slot. The registry then tombstones its `node_id`. `parosctl status`
  shows `drained` for a draining machine that holds no slot, and `down, in maintenance` for a
  down machine a window covers. Oracles: a window that is still open keeps every slot its
  machine held, unless the tenant's own mode change placed the journal elsewhere, and a removed machine is retired and holds no slot. A graceful stop that hands
  the leadership on before `parosd` exits is a follow-up issue.
- **Retire coordination** (#477 (retire coordination moves into the control journals)). The
  simulation's operators coordinate a joiner's retirement through the registry, not through a
  harness ledger. A composer books an acceptor slot of the journal on a joiner before it names
  the joiner in a configuration. A retirement needs a draining joiner that holds no such slot,
  and the registry books no slot on a draining machine. So a joiner that a configuration names
  is never retired. The acceptor pool's retirement keeps its ledger check
  (`StorageWorld::retire`): that pool runs no control journal on half the seeds.
- **Matchmaker sets** (decided on 2026-10-11, #501 (matchmaker sets off the founders)). Every
  tenant's set is born on the founding members, the bootstrap generation, without a booking. At
  each watch round, after the journal move step, the tenant coordinator takes one step of one
  set, in turn over the hosted tenants (never the cell tenant: its set serves the elections
  that choose the coordinator). A step books the slots the set lacks with `paros::model::placement`
  (`Role::Matchmaker`, target `BookingTarget::Set { tenant, set: 1 }`, the bootstrap
  generation's size, the slots on machines that stay as the current ones), or asks a founding
  member to hand the set over onto the booked machines that stay (`ReconfigureMatchmakers`,
  which names its tenant), or, once the set in force is those machines, releases every slot on
  a draining machine. The set in force is the latest generation that the founding members
  believe for the tenant's control journal (`Inspect`). So a set is placed the first time a
  coordinator sees it, and a drain moves it like a journal. Each step starts from what the
  registry and the founding members hold, so a later term finishes a move that an earlier term
  started. A machine that is not a founding member hosts a set only when the registry books
  it there; every node links to each registered machine's matchmaker and admits it to its
  matchmaker pool (`ColocatedNode::extend_matchmaker_pool`), so a journal learns a set that
  names it. A released member keeps its frozen store: it answers a late proposer with its
  successor, the chain that a journal booting at generation 0 walks to the set in force.
  Oracle: after chaos, every booked set in force is its booked machines, and a set that has
  a machine to go to holds no slot on a draining machine.
- **A handover is paged** (#439 (operator calls name their journal)). A set keeps one registry
  per journal of its tenant, so one message cannot carry them all. A `Stop` names the last
  journal already read (`after`), and the `Stopped` answer carries one page of the frozen
  registries and the last journal of a page cut short (`next`). The reconfigurer counts a
  member toward the stop quorum only when its report is whole. A `Bootstrap` carries one page
  of the reconstruction (`BootstrapPage`). A member holds the pages in order and answers the
  last journal it holds, and only a whole bootstrap activates. The page size is the matchmakers'
  `registry_page` (at most `REGISTRY_PAGE` journals). Pages of two reconstructions may mix in
  one member: each journal's registry is valid on its own, so the mix is safe.
- **Still to do.** The tenant coordinator actor that runs on the stateless machine its slot
  books (a follow-up of #476).

*Landed* (#476 (tenant footprint), 2026-10-15): the tenant birth footprint, the capacity
request RPC and the placement of tenant coordinators.

- **The footprint is a grant.** `SystemCommand::GrantCapacity` books a set of slots all or
  none, under a key: `Footprint(tenant)` or `Request(id)`. The first grant under a key decides
  it, and a refused grant is recorded too, so a retry reads the outcome back. A slot that the
  fold refuses at apply (class, a machine that left, a spent id) refuses the whole grant, and the fold
  records nothing for it. Footprint slots use the target `BookingTarget::Tenant { tenant }`:
  a tenant-level slot, so two of them may share a machine.
- **What a footprint holds** (`paros::model::footprint`). Since #837 (remove role slots,
  2026-11-01) a footprint reserves no storage: it holds only the coordinator slot, and a cell
  refuses a tenant only when no storage machine serves it. The cell coordinator releases an
  acceptor or matchmaker slot that an older cell booked for a footprint. Before #837: at birth the durability is
  `Durability::DEFAULT` (`machines=1`, until a creation names one): one acceptor
  quorum (2 slots), one quorum for the control journal (2 slots) and the matchmaker set
  (3 slots), on storage machines, plus one coordinator slot on a `stateless` machine. When no
  stateless machine serves with a free slot, the founding members host the coordinator and
  the footprint holds no coordinator slot.
- **A host needs its footprint.** The fold refuses `HostTenant` (`NoFootprint`) for a tenant
  with no granted footprint. The cell coordinator writes the footprint as a step of its own:
  it books a `REGISTERING` tenant's footprint before its `HostTenant`, as it follows the
  universe directory (decided on 2026-10-15, #225: no operator claims the cell control
  journal). It books the cell tenant's and the universe tenant's too, since it also writes
  `JoinFleet` (#513 (fleet Init duel)). A creation whose footprint the cell refuses marks the
  tenant `ERROR` in the universe directory and answers `NoFootprint`; a delete frees its name.
  The cell records a refusal only once every founding member is registered, so a missing
  founder delays a creation and never refuses it.
- **`init` does not wait for the founding members.** A founding member registers through the
  cell coordinator. `init` claims nothing on the cell control journal (#513 (fleet Init
  duel)), so it does not fence the coordinator, and it runs its fleet steps at once. The
  coordinator books the cell's and the universe's footprints once they fit, and never
  refuses them. Until 2026-10-16 `init` waited for the registry to hold every founding
  member, a wait #476 (tenant footprint) added when the fleet steps claimed the cell. That
  wait delayed a cell formed in the recovery tail past the fleet's settle bound.
- **The capacity request RPC** (`CapacityRequest`, `0x5041_030C`, served in the
  `CoordinatorInterface` group). A request names its tenant, an idempotency id and its slots
  (node, role, target). The cell coordinator answers a decided id from its record, refuses a
  tenant the cell neither hosts nor booked, and else writes the grant, or its refusal, in the
  cell control journal. The tenant desk books a create's acceptors through the same path,
  in process (`machine/capacity.rs`); `paros::client::capacity` is the remote caller.
- **Placement of tenant coordinators.** At each watch round, after the journal and set steps,
  the cell coordinator takes one footprint step of one tenant, in turn
  (`machine/footprints.rs`, `paros::model::footprint::fix`): it books a coordinator slot on a live
  stateless machine when the tenant has none or its machine is down, releases the dead one
  once replaced, books a missing storage slot, and moves a slot off a draining machine.
  Until the follow-up, the tenant coordinator actor still runs inside the cell coordinator's
  term; the slot is its placement.
- **The simulation.** The machines' capacity floor is three footprints (the cell's, the
  universe's and one tenant's: 21 slots), so a founding member can host them. A creation may
  ask for a tenant-level slot through the capacity request RPC, and may retry the same id.
  Oracles: every hosted tenant has its footprint, a tenant coordinator holds at most one live
  slot and only on a stateless machine, a grant names one booking per slot, and a retried
  request reads back its first answer. Gates: a tenant refused for its footprint, a tenant
  coordinator placed on a stateless machine, a request refused.
