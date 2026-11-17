[← 03-2-machines.md](03-2-machines.md) · [Index](README.md) · [03-4-tenant-modes.md →](03-4-tenant-modes.md)

### 3.3 Control planes, recovery and placement

#### Rule 3: a small authority, one writer

**A coordinator term is won by one compare-and-set on its control journal, with no read of a
second journal** (decision 1). **Not proved** until the TLA+ gate of phase 3 is green: the model
`cell-control`, in which, with no more machines gone than the survive number of the cell and
with faults stopped, a coordinator serves again; its controls (two writers of one control
journal never settle, an order with no fence is taken, candidates from bookings leave no
candidate) fail.

- **The coordinator is the writer of its control journal, not its Paxos leader.** It is a client
  of the control journal that holds the leader uuid (section 2.3). It can run on any machine of
  the cell. Two fences, two layers: the Paxos ballot says which machine runs the journal's
  consensus, and the leader uuid says which coordinator may write.
- **The term is `SetLeader(new, old)`.** The first candidate that replaces the old uuid wins the
  term, and the loser learns the winner from the refusal. No other journal, no registry entry
  and no address registration are on that path. "No read" means no election journal, and no
  read-back on the normal path: only a candidate that got no answer reads the leader uuid, and
  if it is its own new uuid, it won (FoundationDB's generation register, TigerBeetle's rule of
  no read). It does not mean no fold: the new coordinator folds its control journal to the tail
  before its first control write. With rules 1 and 2, the path to a term is a majority of a set
  that never shrinks and a quorum of one configuration: one healthy Paxos group, not two.
- **Each coordinator write is a renewal.** An idle coordinator writes a small renewal record at
  the cell and universe levels only (decision 19). The first record of a term names the
  coordinator's interface (its address and its `InterfaceRef`) and gives the term its number,
  which is the count of terms in the fold.
- **At least one coordinator runs.** A machine that saw no write under the current uuid for one
  lease, on its own clock, sends the compare-and-set after a jitter. A storage machine waits a
  longer jitter than a stateless one (decision 15, question 5). Safety does not depend on the
  preference or on the clock: a caller that campaigns early deposes a live coordinator, which
  costs availability, never safety.
- **Candidates** (decision 7). Any machine of the cell that proved it serves can be a candidate:
  it is registered, alive, and has followed the control journal to its tail. No list of
  candidates is kept, and no candidate comes from a booking or from the first configuration
  (step 3.6, #1023).
- **One election mechanism for everyone** (decision 13). The term of the kit (win a journal with
  `SetLeader`, renew by writing, stop at the first refused write, hand on with `SetLeader(next,
  me)`) is the public election API. The election journal library (`paros::client::election`) is
  deleted. Multi-writer journals stay a data-plane feature.
- **Each control journal has one writer, its coordinator.** An operator or a client sends a
  request with an idempotency id, and the coordinator writes it under its own term. No admin
  session claims a control journal (section 3.1).

**The fence covers each order, not only the control journal** (finding 2 of the review of
2026-11-16; steps 3.2, 3.6 and 4.2). The leader uuid fences the writes to the control journal.
It does not fence an order that the coordinator sends outside that journal: to a machine, or to
the matchmakers of a data journal. So five rules hold together:

1. The coordinator records each action in the control journal before it does it, so a deposed
   coordinator can only repeat a recorded step.
2. Each order carries the number of the term, and a receiver refuses an order below the highest
   term that it saw for that control journal.
3. The receiver keeps that highest term on disk, so a restart does not make it take a stale
   order again.
4. An order for a journal names the epoch of its target (the epoch of the register, the
   generation of the matchmaker set), and the target refuses an order for an old epoch. An
   order for a machine has no journal epoch, so it names the incarnation of the machine, which
   the nonce echo of rule 4 proves.
5. The release of a store needs no order at all: a machine releases its store for a journal only
   when the register of that journal shows a configuration in force that does not name the
   machine.

**Three small control planes of one shape** (decision 6, option C of the split study; step 3.4).
Each entity has its own control plane: tenant, cell and universe. Each one is one control
journal, one fold, one term uuid and one group of duties, built from one kit: the term by
`SetLeader`, renewals, a recovery pass, a fold with a checkpoint (section 3.9), and request and
outcome records. Levels talk only through records with idempotency ids, never through a call
that needs the other level alive.

| Level | Its duties |
|---|---|
| Cell | machines, admission, liveness, bookings, placement, drain, repair, balance, the matchmaker sets, the growth of its own control journals |
| Tenant | the catalog (namespaces, journals and the configuration typed for each), the requests of the tenant, the scrub of its journals (section 5) |
| Universe | the directory: tenants, namespaces, cells, buckets and the moves between cells |

- **Hosted in one process.** The three levels run in one process under the cell's term. That
  hosted form stays for good in a cell with no stateless machine. A level gets its own actor
  later with a small step, and a tenant level that gets its own machine then needs its own
  renewal.
- **A tenant control journal has no lease of its own** while its level runs in the process of
  the cell (decision 19): the coordinator claims it with `SetLeader` at its first use in a term,
  and folds it to the tail before it writes. An idle tenant costs no write. A tenant control
  journal that refuses its uuid stops that tenant only.
- **The universe control plane runs in the coordinator of the home cell**, the cell that holds
  the universe control journal (Pierre, 2026-11-16). It has a separate term on the universe
  journal and the candidate rule above. While the home cell is down, nobody can create a tenant
  or add a cell, and the journals of the other cells still serve. A universe journal with
  acceptors in several cells is later work.
- **Two guards keep the boundaries:** a layer script, and a BUGGIFY decision that gives one
  level's term to another machine.
- **No coordinator slot.** The booked `Role::Coordinator` slot and its bookings are deleted
  (decisions 7 and 15, question 5).

The study is `/mnt/project-files/analysis/control-plane-split-2026-11-15.md` of the project: a
reading of the code, with nothing run.

**Requests to a coordinator** (decided on 2026-10-04; every admin command since rule 3, step
3.1). Every control journal has one writer, so anyone else (a machine that registers, an
operator who admits or drains a machine, a person who creates a journal or sends a `configure`)
sends a **request RPC to the coordinator**, found through the interface that the first record
of its term publishes. A request carries an idempotency id; the coordinator records the request
and its outcome in its journal, so a retry that crosses a change of term finds the answer there
instead of acting twice. A raw `reconfigure --members` and `retire` are requests too, or a debug
mode with no coordinator (step 3.5).

- **Static stability.** A child keeps serving while its parent is down. Only new tenants, new
  bookings and moves wait for the parent. The data plane needs no control plane to serve: with
  every coordinator held down for a time, each healthy journal still serves writes, reads and a
  leader change (the simulation gate of phase 3).
- **Rebuild from below.** Every level's journal can be reconstructed from the level beneath it. A
  tenant's name and its catalog live in its own control journal; the cell's list of hosted
  tenants and the universe directory are rebuildable indexes. This is the pattern of DSQL's
  adjudicator (section 2.3) one level up, and what makes recovery possible without Paxos surgery
  (section 3.10, deferred).
- **Tenant birth.** When the cell applies `HostTenant`, the cell books the tenant's control
  journal and its matchmaker set, and the tenant level claims the tenant's control journal at its
  first use; nobody else ever writes it (decided on 2026-10-04).
- **Coordinators checkpoint their control journals** with the library of section 3.9, so a
  control journal's length is bounded by its live entities, never by its history.

#### Rule 4: recovery rebuilds

**Each term starts with one recovery pass: fence, rebuild the live view from fresh proof,
compute the target from the desired state, and drive one routine to it** (steps 4.1 to 4.6).
**Not proved** until the TLA+ gate of phase 4 is green: in `cell-control`, every set returns to
its target, or reports that it cannot place (a leads-to property); the control, a repair in
place that waits for the broken member, fails.

- **The registry and the bookings are hints.** The live view is rebuilt from fresh proof at each
  term. Views derive from the configuration in force that the register holds (rule 1), so there
  is no second truth (no `Placed` record and no recheck by polling).
- **Proof before use** (step 4.2, #983). Before any step that cannot be undone (a
  reconfiguration, a booking, a handover), the member echoes a fresh nonce for its incarnation.
  A term is durable before any message advertises it.
- **One routine moves a journal's membership from A to B** (step 4.1): prove, warm, seal,
  switch, release. B can have another size (rule 2). The differences between a drain, a repair,
  a balance move, a `configure` and a move between cells are data: which journal, which target,
  whether to warm, whether to retire. The warm step is where a joiner restores a checkpoint
  (#1068), and the leader hands its leadership to a member of B when it sits on a machine that
  leaves (the cooperative handoff, `relinquish_to`), so a move costs no election.
- **A step that fails ends the pass, and the pass starts again**; nothing waits on a broken
  member.
- **Duties run on events, in parallel under a budget for each machine** (step 4.4, #981), and the
  steps that block commits go first. An attempt timeout grows to a cap, then raises an alarm.
- **The "better set exists" check** (step 4.6, FoundationDB's team checker): a set with a member
  that is gone, or with a member that did not prove it serves, starts a change.

**Rule 2 at the control plane: the size changes only by a resize record.** A set has a durable
size, 2F+1 for each acceptor partition, F the survive number of the journal that owns it. A
failure never changes the size: the size is the recorded number of members, and a member that is
gone does not change it. A change of members replaces named members one for one, and the
caller names the target set. The coordinator first writes the new size in the control journal.
Three events cause it, and no other step changes a size:

1. a `configure` for a journal (a new `survive` or `partitions`, section 3.4);
2. a cell that grows, for the sets of its own control journals only (decision 5, as decisions 17
   and 18 limit it);
3. an operator who lowers a number.

The cell never lowers a survive number by itself (decision 17). When machines are gone for good,
a set keeps its size, serves with its live members, reports "cannot place", and heals when a
machine joins. FoundationDB does the same: it never changes its redundancy mode by itself.

#### Placement

**One placement predicate for every step** (step 4.5, #616, #750): count, class, disk and the
zone rule (section 5), with a typed "cannot place" reason. A create, a `configure`, a repair, a
drain and a balance move all ask it, so no step places a journal that another step would refuse.

- **The function.** `paros::model::placement::plan` maps the desired count, the machines the
  registry offers and the target's live bookings to steps: slots to book and bookings to
  release. It is pure, so a new coordinator that folds the same registry computes the same
  steps. Its rules: the role's class, no machine with two storage instances of one target, zone
  by zone in round robin, least busy then least loaded inside a zone (#837 (remove role slots)),
  ties broken by a draw seeded with the tenant id, a down machine only after every machine that
  is up, and a user journal's store only on a machine whose disk holds one more (#985, section
  3.2). A release takes a down machine first, then the most loaded.
- **Only a machine that serves the cell is offered** (#511): a machine is offered once it was
  admitted and answered `Identify` with this cell's id.
- **No capacity** (#837, 2026-11-01). A machine takes a role while it serves the cell. Inside a
  zone, placement books the machine with the lowest busyness step first: the larger of its CPU
  and disk busy ratios from its last `Load` window, in steps of 10 %. Among equal steps, the
  machine that hosts the fewest roles goes first.
- **Booking before the create.** The cell books a create's acceptors before the tenant level
  records the create. A create that the predicate refuses is refused with its typed reason, and
  nothing is booked (rule 7, section 3.4).
- **Bookings, not capacity** (step 0.2). The capacity request RPC is deleted; the commands that
  booked capacity are named bookings. The tenant level asks the cell level for bookings through
  records with idempotency ids.
- **Releases.** A delete releases its journal's slots after the tombstone. A slot booked for a
  journal its tenant does not hold live is released by the next pass.

**Drain, maintenance and removal.** `cell drain-machine` marks a machine draining, and the
routine of rule 4 moves each of its roles to a machine that stays. `cell maintenance <machine>
--for <duration>` (or `--zone <zone>`) writes a `Maintenance` record: while the window is open,
the coordinator re-places no role of a down machine that it covers. `--for 0` ends the window.
The window counts on the coordinator's clock from the first time it folds the record.
`cell remove-machine` removes a drained machine that holds no slot: the registry tombstones its
`node_id`. Each is a request to the cell coordinator (rule 3).

**Balance** (decided by Pierre on 2026-11-01, #687 (rebalance a cell)). The cell's **balancer**
evens out the machines of each zone. The decision is the pure `paros_model::balance`,
unit-tested like `placement`; the coordinator only records it and runs it with the routine of
rule 4. The vocabulary is **balance**: the actor is the balancer, its view is `CellBalance`, and
`parosctl cell balance` shows and sets it (`--pause`, `--resume`).

- **What it balances.** Each round, among the storage machines that stay and are up, it compares
  the most and the least loaded machine of a zone. A gap above `balance_spread` (default 1) is a
  gap. When every zone is within it, it compares the busiest and the least busy machine of a
  zone: a gap of `balance_busy_spread` busyness steps or more (default 5) is a gap. A gap must
  hold for `balance_rounds` rounds in a row (the hysteresis). On a balanced cell it also
  balances the leaders, with the cooperative handoff.
- **Repair comes first.** No balance move starts while a machine is draining, slow, down or gone,
  or while a role sits on a machine that leaves. One balance move runs at a time.
- **Its state is in the registry.** `SetBalance` pauses or resumes it, `BalanceStarted` records
  a move, and `BalanceAbandoned` gives one up. A later term goes on with the move in flight.

**Matchmaker sets.** Every tenant has one matchmaker set (section 3.4). The cell places it with
the same predicate, on storage machines, and moves it with a generation handover whose target
the caller names (rule 2, section 4.1). A set has `2F+1` matchmakers; its size is always odd,
and only a resize record changes it. A tenant has its own typed survive
number (decision 35). It sizes the tenant's matchmaker set and its tenant control journal. A
create of a tenant must name it, and only a `configure` on the tenant changes it. The cell
refuses a create or a `configure` of a journal with a survive number above that of its tenant,
with a typed reason, so a person raises the tenant first. The sets never follow a journal by
themselves, and nothing makes them smaller by itself. FoundationDB does the same for its
coordinators: only the `coordinators` command of a person changes them, and `configure` does
not. **Not proved:** the bound (the set survives at least as much as each journal that uses
it) and the order (the sets grow before a journal records a higher number) wait for the TLA+
gate of phase 1 (steps 1.6 to 1.8).
