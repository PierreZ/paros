[← 02-data-plane.md](02-data-plane.md) · [Index](README.md) · [03-2-machines.md →](03-2-machines.md)

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
| `parosctl universe add-cell --members a,b,c [--name]` | the universe, and the coordinator of the idle cell | admits another idle cell (#232, section 3.7) |
| `parosctl universe remove-cell <name>` | the universe | removes an empty cell and tombstones its id (#232, section 3.7) |

`parosctl init` stays as a shortcut for `cell init` followed by `universe init` (the toy).
Today, `parosctl init --idle` is `cell init` alone: it forms an idle cell with no universe tenant
(#232 (multiple cells), section 3.7).

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
  4. **Teach** (the decree's learner catch-up, #277 (cell decree)). The receiver sends
     `FormCell` once, so a founding member that was down at that moment stays idle while the
     plan names it. Each formed member asks every other member of its plan to `Identify` itself
     at each election renewal period. It sends its own vote, the plan at the ballot it accepted
     it at, to a member that answers idle. This is the same Phase 2a again, so it is safe at
     any time. The idle member promised that ballot in step 1, so it accepts. The `FormCell`
     carries the operator's token, the one that formed the teacher; the teacher keeps it in
     memory only, never in its record. Teaching of a member ends when it answers in the cell,
     answers as another machine (a wiped member), refuses the token, or the address book names
     it at another address than the plan's. A member that restarted holds no token and teaches
     nothing. Before, only a new `init` formed that member, and a cell with one more member down
     had no quorum (seeds 7461995534021400970 on main 7feccc9 and 16403044264830861989 on
     629aa62).

  A receiver that crashes midway leaves no lock: the next `cell init`, sent to any listed machine,
  finds the accepted plan in its ask and finishes it. A plain question ("is anything ongoing?")
  would not do, because two receivers could both hear "no" and both go ahead. The decree is
  paros-core's single-decree `Proposer` and `Acceptor`, as the matchmaker handover already uses
  them at slot zero. FDB reaches the same end differently: its cluster file names coordinators
  that elect a controller before any database exists, the controller's provisional proxy takes
  the first `configure new` transaction, and each client reads `\xff/init_id` back to learn
  whether it won. paros has nothing elected before `cell init`, so the decree is that election.

  The first `2F+1` listed machines are the cell's **founding members**, with `F` from `parosctl
  init --failures-per-partition` (default 1, so three founding members, decided on 2026-10-15:
  before, every listed machine was a founding member, and seven listed machines gave the cell and
  tenant control journals seven acceptors). When fewer are listed, every listed machine is a
  founding member. `init` admits the other listed machines after the fleet steps, as `add-machine`
  does (`paros::client::initialize::founding_members`), so a re-run lists the same machines in the
  same order. `cell init` creates on the founding members the cell
  control journal and the cell's first election journal, both born with the cell's first
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
  and a node-only `Inspect` with its cell, and it serves the journals placed on it (#474). A machine that
  promised in a `cell init` and has no vote refuses `Admit` (`in_cell_init`): that `cell init`
  can still form a cell over it, and a stalled `cell init` is safer than a machine in two cells.
  An admitted machine refuses every `cell init` that lists it (`cell_exists`).
- **`universe init`** creates the universe tenant inside that cell (the universe tenant is a
  tenant, so the cell must exist first to grant it capacity), mints `universe_id` and the universe
  tenant's `JournalIdentifier`, both random, recorded in the cell plan of the admitted cell
  (`universe add-cell` records the same two in each cell it admits later), then registers the cell as the first entry in the universe directory and
  writes the matching registration on the cell side (section 3.7).
  The cell coordinator writes the cell's side (`JoinFleet`), not the operator (decided on
  2026-10-15, #513 (fleet Init duel)). The coordinator reads the universe directory the cell
  hosts. When the directory holds the cell and the cell has not joined, the coordinator writes
  `JoinFleet`, at its install and at each watch round. `init` reads the cell control journal,
  waits for the join, then marks the cell `READY`. So `init` never claims the cell control
  journal. Before, its claim fenced the coordinator, the next term fenced `init` in turn, and
  with slow links `init` did not end.
- Each step is idempotent and is a step of an operation state machine (section 3.7): re-running a
  call after a crash resumes it, learning the ids back through `Inspect`.
- **No implicit formation.** A machine with an empty, uninitialized store waits indefinitely. It
  never forms a cell, joins one, or creates a universe on its own.
- **Trust.** An idle machine accepts the first `FormCell` or `Admit` that reaches it; under the
  network trust of section 3.5 that is the admin's. With Biscuit tokens (#245) both carry an admin
  token the machine checks, since otherwise another host could impersonate the cell (kubeadm pins
  the control plane's CA for this reason). Landed on 2026-10-15 (#245 (Biscuit tokens)): a
  machine started with `PAROS_ROOT_PUBLIC_KEY` pins those keys (`paros::machine::AdminPin`) and,
  while it is idle, checks an `admin` token on `CellInit`, `PrepareCell`, `FormCell` and `Admit`
  (operations `form_cell` and `admit`, on an internal target). The proposer of `cell init` sends
  the operator's token on to the other founding members, and a formed member that teaches the
  decree (step 4 above) sends the same token. A refusal answers `invalid_token`, `expired` or
  `forbidden`. A teacher with no token (it restarted) or with a refused one stops: the operator
  runs `init` again with its token. A machine started without the variable trusts the network, as before.
  The cell coordinator that finishes an admission an operator left in flight has no token, so a
  pinned machine refuses it: the operator runs `cell add-machine` again with its token.

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
(for example `paros::model::fleet` and `SystemPlan::seeds`); they go when the rename lands (#246's
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
