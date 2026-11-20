[← 02-data-plane.md](02-data-plane.md) · [Index](README.md) · [03-2-machines.md →](03-2-machines.md)

## 3. The control plane

### 3.1 The cell tenant and bootstrap

Cells are a concept of the layers above the journal. The consensus core and the journal know
no cell: the journal takes an initial configuration as input, and `cell init` is the upper
stack's way to agree on one (decided by Pierre on 2026-11-20, #1264 (remove the cell from
paros-consensus), section 3.7).

A cell's machines and bookings are the control journal of the **cell tenant**, served by the
same `parosd`s, the same Paxos and the same stores as every tenant's journals. Its writer is the
**cell coordinator**. The control state has three levels, each with one control journal and one
writer (rule 3, section 3.3): the cell control journal holds the machine registry, the
admissions, the liveness records, the bookings and the cell policy; the universe control journal
holds the directory (tenant and namespace → cell); each tenant's own control journal holds its
description, its namespaces and its journals. System journals are written with `Write` like any
journal; there is no special path.

Every tenant gets a control journal when it is created. It is **self-describing**: it holds the
tenant's name, its namespaces, its `survives` (section 3.7), the name and kind of the cell that
hosts it, its journals and the configuration typed for each (decided on 2026-10-09, #210). It
holds no rendezvous name. Every index above it (the cell's list of hosted tenants, the universe
directory) can be rebuilt from it (section 3.3).

**Bootstrap: idle until an admin admits it** (decided on 2026-10-09, #216). There are no seeds,
no join list and no rendezvous name. The same rule holds at two levels:

- **An idle machine** is formatted (its `node_id` minted, its machine record written) and waits,
  serving only `Identify`. It joins a cell only when an admin call admits it.
- **An idle cell** is formed and serves its own control journal, but hosts no tenant. It joins a
  universe only when an admin call admits it.

| Admin call | Sent to | What it does |
|---|---|---|
| `parosctl cell init --members a,b,c [--name]` | the listed idle machines | forms an idle cell on them |
| `parosctl cell add-machine <addr>` | the cell coordinator, as a request | the coordinator admits an idle machine |
| `parosctl universe init --cell <addr> [--name]` | the cell coordinator, as a request | creates the universe tenant there and admits that cell as the universe's first |
| `parosctl universe add-cell --members a,b,c [--name]` | the universe coordinator, as a request | admits another idle cell (#232, section 3.7) |
| `parosctl universe remove-cell <name>` | the universe coordinator, as a request | removes an empty cell and tombstones its id (#232, section 3.7) |

`parosctl init` stays as a shortcut for `cell init` followed by `universe init` (the toy), and
`parosctl init --idle` is `cell init` alone. **Every admin call after `cell init` is a request to
a coordinator** (rule 3, step 3.1): the operator sends it with an idempotency id to the
coordinator's endpoint, and the coordinator records it, does it and records the outcome. An
admin session never claims a control journal, because that claim would depose the coordinator.
With no coordinator, a command waits for the next term.

- **`cell init` is a single-decree Paxos on the cell plan** (decided on 2026-10-09, #277). The
  machine that receives it drives it, with every listed machine as an acceptor. Phase 1 hears
  every listed machine and Phase 2 needs a majority (decided on 2026-10-09, #246): every listed
  machine must answer the ask, so every vote still on a disk is heard and adopted (the decree
  runs under a majority with the Phase-1 rule `EveryAcceptor`, because flexible quorums are
  deleted, decision 27), and a majority of accepts chooses
  the plan, so a wiped listed machine does not block `init`:
  1. **Ask, as a reservation.** It sends a fresh random `init_id` (its ballot) to every listed
     machine, which answers like `Identify`: it must take the `storage` role. It also promises to
     accept no plan under a lower `init_id`, and answers with any plan it has already accepted. A
     formed machine answers every ask with its accepted plan and never accepts another one. A
     listed address that now hosts another machine than the accepted plan names is a wiped
     machine: the new machine never accepts the plan as the old one (`not_a_member`), so it never
     votes with the old one's lost promises. When a majority of the plan's machines are wiped, no
     plan can be chosen, and the receiver refuses (`cell_lost`): the Paxos limit.
  2. **Adopt or draw.** If an answer carries an accepted plan, the receiver must finish that plan
     instead of its own: with the same list, the two `init`s converge on one cell; with another
     list, it refuses (`other_cell_init`). Otherwise it draws the plan: `cell_id` and the cell
     tenant's `JournalIdentifier` (section 3.8).
  3. **Form.** It sends `FormCell(plan, init_id)`; each machine accepts it unless it has promised
     a higher `init_id`, and is then formed. A majority of accepts chooses the plan. The receiver
     sends its own accept last, and only when the others can still choose the plan with it.
  4. **Teach** (the decree's learner catch-up, #277). A formed machine sends its own vote, the
     plan at the ballot it accepted it at, to a listed machine that answers idle. This is the same
     Phase 2a again, so it is safe at any time. The `FormCell` carries the operator's token, kept
     in memory only.

  A receiver that crashes midway leaves no lock: the next `cell init`, sent to any listed machine,
  finds the accepted plan in its ask and finishes it. The decree is `paros-consensus`'s single-decree
  `Proposer` and `Acceptor`, as the matchmaker handover uses them at slot zero. FDB reaches the
  same end differently: its cluster file names coordinators that elect a controller before any
  database exists. paros has nothing elected before `cell init`, so the decree is that election.

- **`init` only bootstraps** (decided on 2026-11-04, #685). It writes the first configuration of
  the cell control journal and of its matchmaker set on the listed machines, and an admission
  record for each listed machine, of the same shape as the record `Admit` writes (one machine
  lifecycle, rule 5, step 5.1). The first coordinator wins its term with `SetLeader(its fresh
  uuid, unset)` on the cell control journal (rule 3). Nothing else: no election journal, no
  list of "initial members" that any rule reads later. After formation every machine is a
  machine with an id, the registry books its roles like any machine's, and it drains and leaves
  like any machine. Every machine finds the coordinator from its cached registry (section 3.2),
  never from a list of seed addresses. After formation the membership changes only by
  reconfiguration. `init --survive` names the survive number of the cell's own control journals
  (decision 15 did not change it, section 3.4).
- **A cell can start on one machine** (decision 5). `parosctl init --members A` forms a cell of
  one machine with one copy (`survive 0`), to run paros locally: it starts, restarts and grows.
  It does not heal a lost disk. In the `auto` mode of its control plane (section 3.4), the cell
  raises the survive number of its own control journals when it has three machines or more.
- **`add-machine`** is a request to the cell coordinator (step 3.1). The coordinator reaches the
  idle machine at the address the operator gives and asks it to `Identify` itself. It writes the
  machine's admission into the cell control journal, then sends it `Admit` with the `cell_id`,
  the control `JournalIdentifier`s and a registry snapshot, which the machine caches durably. An
  interrupted admission is finished by the coordinator like any request it recorded. An admitted
  machine refuses every `cell init` that lists it (`cell_exists`), and a machine that promised in
  a `cell init` and has no vote refuses `Admit` (`in_cell_init`): a stalled `cell init` is safer
  than a machine in two cells.
- **Admission by credential** (decision 4, step 5.4). A `parosd` that lost its disk mints a new
  `node_id` itself and is admitted again with a credential that the operator gave it, guarded
  like a join token. It is a new machine that can take the place of the lost one (rule 5,
  section 3.2). The old id is never admitted again.
- **`universe init`** creates the universe tenant inside that cell (the universe tenant is a
  tenant, so the cell must exist first), mints `universe_id` and the universe tenant's
  `JournalIdentifier`, both random, then registers the cell as the first entry in the universe
  directory. The cell coordinator writes the cell's side (`JoinFleet`), never the operator
  (decided on 2026-10-15, #513).
- Each step is idempotent and is a step of an operation state machine (section 3.7): re-running a
  call after a crash resumes it, learning the ids back through `Inspect`.
- **No implicit formation.** A machine with an empty, uninitialized store waits indefinitely. It
  never forms a cell, joins one, or creates a universe on its own.
- **Trust.** An idle machine accepts the first `FormCell` or `Admit` that reaches it; under the
  network trust of section 3.5 that is the admin's. A machine started with
  `PAROS_ROOT_PUBLIC_KEY` pins those keys and, while it is idle, checks an `admin` token on
  `CellInit`, `PrepareCell`, `FormCell` and `Admit` (#245 (Biscuit tokens)). A refusal answers
  `invalid_token`, `expired` or `forbidden`. A machine started without the variable trusts the
  network.

This is the mainstream pattern, checked on 2026-10-09: Kafka KRaft passes the initial voters
explicitly at format time (`--initial-controllers`) and never auto-formats a blank directory,
since a majority starting empty could elect a leader missing committed data; etcd forms from
`--initial-cluster` plus a cluster token and adds later members with `member add`; kubeadm runs
`init`, then `join` against any control-plane address. Redpanda recommends disabling
`empty_seed_starts_cluster` for the same reason paros has no implicit formation. A machine that
forms a cell on its own and grows by reconfiguration (Kafka's `--standalone`) was rejected: two
concurrent `cell init`s sent to different machines would then form two cells, where the decree
over the whole list forms at most one.

**Naming** (decided on 2026-10-09). The top level is the **universe** (formerly the fleet; the
name Spanner and YugabyteDB use). "Seed" only ever means a simulation seed, and the word
"cluster" is not used: a set of machines is a cell. The addresses a machine keeps to find its
cell are its **entry names** (decision 8, section 3.2), never "seeds". Some code names still say
`fleet` and `seeds`; they go when the code they name is rewritten.

**Every journal is born with its matchmaker set** (decided on 2026-10-04). The cell control
journal is born with the matchmaker set `cell init` starts, and the universe tenant's control
journal with its tenant's set; every tenant journal is born with its tenant's set. **Matchmakers
are mandatory everywhere in the service** (decision 15, question 2 (c)): each journal of the
service reconfigures and has the split of roles. Plain Multi-Paxos with no matchmakers is a
shape of `paros-consensus` as a library, and `parosd` never runs it (step 0.11).
