[← 05-failure-model-and-zones.md](05-failure-model-and-zones.md) · [Index](README.md) · [07-changes-milestones-done.md →](07-changes-milestones-done.md)

## 6. Verification

Simulation is the investment, and TLA+ selects the rules before the code (the proof rule,
section 1.1).

### Rule 8: one quiescence oracle

**Chaos and operator actions stop at one cutoff, dead machines stay dead, and one predicate must
hold within one bound** (steps 1.1 to 1.5 and 1.9; decisions 16 and 33). The predicate is the
definition of "healed":

- **The cell healthy predicate** covers data journals, control journals, sets, the coordinator
  and the registry, each with every member live, for N passes in a row. A set has two healthy
  cases: every member live, or a typed "cannot place" reason that the ground truth of the
  simulator confirms (no live machine can take the missing member). In the second case the set
  must still hold a quorum and serve. A cell with no spare machine is healthy only in this second
  way.
- **The tail restarts nothing, drains nothing and creates nothing.** No operator call comes after
  the cutoff: the cell heals itself. A cancelled token in the tail is a violation.
- **The bound comes from the drawn tunables** and never grows to fit a red. One fixed recovery
  bound replaces the four windows of each feature (20 s, 20 s, 60 s and 100 s, the gate of phase 4),
  and time to heal is reported as a histogram.
- **No excuse and no skip** (decision 16). The simulator decides which faults are allowed, from a
  policy that paros gives it (`can_kill`, step 1.9, section 5): it refuses first each fault that
  the cell does not promise to survive (a kill, a wipe, rot, and a fault that would be one more
  than the survive number of a set). For all the other faults the cell must heal, and the oracle
  judges. Each oracle judges on each seed, and each red has a typed cause. FoundationDB does the
  same; the 34 excuses of today, `Excuse::ALL`, `paros-sim/src/excuses.rs` and the skips of the
  oracles are deleted (step 1.2).
- **Machines stay dead for good**, up to the survive number of the cell, and the policy refuses a
  kill that takes the quorum of any set. The network and disks turn fast. Then N healthy passes
  in a row inside the bound (FoundationDB's three healthy passes, TigerBeetle's healed quorum
  core).
- **A larger cell on a share of the seeds** (step 1.3, #459): 5 to 9 machines, `max_dead` equal
  to the drawn survive number of the cell, one outage of every machine. Some of these seeds draw
  spares (2F+1 storage machines for each partition, plus F more), so every set can have every
  member live again; the others draw no spare, and the oracle judges the typed reason.
- **Injected liveness bugs** (step 1.5, #773, #973): a hunt mode turns on a known bug (the
  answerer rule, a stale registration) and must go red.
- **Two metastable checks** (decision 10, step 6.9). A metastable failure is a bad state that
  stays after its trigger is gone, because the load or the retries keep it alive. First, the
  client load goes on after the cutoff, and the rate of served writes returns to its level before
  the chaos, inside the recovery bound. Second, the load stops, and the message rate falls to the
  idle rate inside the bound (FoundationDB's quiet database). Both are counters that the audit
  folds. The reds #857, #705 and #588 are of this kind.

**Safety and liveness causes** (decision 33). From the simulation gate of phase 1, a safety red
always blocks a pull request. A liveness cause is **open** until the gate of the phase that owns
it: an open cause is counted on the **heal scorecard** (the count of each cause for each commit,
in the main-red record) and does not block. At its gate a phase closes its causes ("a ceiling of
zero"), and a closed cause blocks from then on. The **list of open causes** is one file in the
tree: each line names a liveness cause and the phase that closes it, and a safety cause is never
on it. The list only gets shorter, and phase 7 ends when it is empty and deleted (step 7.6).

**The simulation is off in CI until the gate of phase 1** (decision 33, step 0.13). Main was red
on almost each pull request for causes that the rework replaces, so a red said nothing about a
pull request. From step 0.13 a pull request is judged on the classic tests: the build, the lints,
the unit and integration tests and the TLA+ checks; `paros-sim` still builds so that it does not
rot. The nightly hunt stops too, and the red record of main keeps its last state. No thread fixes
a simulation red of main while the simulation is off. The simulation comes back at step 1.4,
when steps 1.1 to 1.5 and 1.9 are merged and before any step of phase 2. FoundationDB keeps its
large hunt outside the gate of a pull request in the same way: the pull request runs the build and
`ctest`, and Joshua runs the ensembles and records each failure with its seed.

### The models

Six models carry the gates (section 7). Four are new, and two extend a model in the tree. Each
has a `LiveSpec` with fairness and a control that must fail with its rule off.

| Model | Phases | Safety | Liveness, with `LiveSpec` | Control that must fail |
|---|---|---|---|---|
| `config-register` (new, from `cas-register`) | 1, 2 | Agreement, with three ballots and a candidate that stops after any step; also with a partitioned layout and with a full grid (step 1.13), and with a successor of another size | With at most F acceptors gone, a campaign wins in the end | A switch before the recovered slots are held; a stored stale belief; an abandoned successor; a store that is released before the switch |
| `handover` (extends `handover-gc`) | 1, 2 | Only a resize record changes the size; the first write needs every new member; the floor and the register cross the handover, also while a journal has an open successor | A handover with at most F members down ends | The answerer rule |
| `cell-control` (new) | 1, 3, 4, 5 | One writer for each control journal in each term; the cell never lowers its survive number without an operator record; an order of a deposed coordinator changes nothing that was not recorded; a record between two levels is applied exactly once across a change of term | A coordinator serves again; every set returns to its target, or reports that it cannot place; the registry names no dead id | Two writers; an order with no fence; a repair in place; candidates from bookings; an address written through the coordinator |
| `tenant-move` (deleted in phase 0; a new model in phase 5 for the move of a namespace) | 5 | The invariants of the new move, from #1070 | The move ends, or its abort ends | The controls of the new move, written with the model |
| `election` (new, small) | 6 | One leader for each ballot | One leader holds after a healed partition | No pre-vote |
| `reads` (extends the model in the tree) | 1 | A read never returns a state older than a write that was complete before the read started; with a majority, a partitioned grid and a full grid with the row read, and across a change of members | Not in this model | A fence on the believed ballot in place of the promise (step 1.10); a row that did not hear of a completed successor (step 1.13) |

The check with three ballots of `config-register` runs in CI, not as `long`, because two ballots
cannot show the bug of the one-slot register (section 4.1). Each `long` check of the tree is run
one time and its result recorded (step 1.6). The models `retire-fence` and `abandoned-ballot`
leave CI when phase 2 deletes their rules; their traces stay as controls of `config-register`.

### What every phase lands with

Every phase lands with its share of:

- Invariants in the audit, where the fact arrives: a leader uuid leads only from a `SetLeader`
  won in the log, every verdict naming the leader in force (through a misbehaving client's
  reinstatement too, section 2.3), every leadership change present in the log, `seq` dense per journal, a
  single-writer `Write` never re-accepted with other bytes, `Truncate` monotone and `first_seq`
  never above a served cursor, a single-writer `Truncate` accepted only from the current leader, a
  `SetLeader` winning at most once per `old_uuid` in a linearization, a multi-writer journal never refusing an
  in-limits write, a capacity slot booked at most once (a booking id never booked twice, across
  checkpoints), a tenant never reaching a journal outside its own
  `TenantId`, no component reaching a journal by an id it did not learn (the simulation draws
  every `JournalIdentifier`, the system tenants' included, per seed).
- Control-plane invariants: a child keeps serving through its parent's outage; folding from a
  checkpoint yields the same state as folding the full history; the universe directory equals the union
  of the cells' tenant lists (assignments and counts), checked even with one cell (FDB's
  metacluster consistency checker); every universe operation resumes correctly when re-run after a
  crash at any step; two coordinators never both write one control journal in one term, and a
  coordinator serves again after the cutoff; with every coordinator held down for a time, each
  healthy journal still serves writes, reads and a leader change.
- A real linearizability checker in the workload's `check()`, over the four-call history with
  `Ambiguous` outcomes, against the sequential model of a journal (a mode, a leader, a dense log,
  a floor). It replaced the per-operation rules of `ClientHistory` (#205).
- The three races made likely rather than lucky, each a knob or an inline `buggify!` at its own
  site, with its reachable: a `SetLeader` drawn in the middle of a pipelined burst, a client
  timeout shorter than the ack so a retry crosses an ownership change, a `Truncate` racing a
  reader's cursor.
- Control-plane shapes: one cell hosting the universe tenant, now; a crash at each step of `init` and of tenant
  creation, each step with its own reachable; a crash inside a checkpoint run and between its
  `End` and its truncate, for the registry and for the universe tenant; a truncate refused from a stale leader; the universe tenant
  unavailable while tenants serve; a coordinator killed mid-operation and its successor finishing
  it. A second cell, formed under chaos on a layout machine or on 1 to 3 machines an operator
  racks in the middle of the run (#490 (second cell mid-run), moonpool's `spawn_group`). An
  operator lane (#489 (operator workload)): on a seed that draws it, one operator runs admin
  processes one after another for the whole chaos phase (an admission, a drain, a window, a
  tenant operation, a candidacy, a tag) while every client writes and reads; a write acknowledged
  while each kind is open is a gate. The operator lane runs through the whole chaos phase, and an
  admin request is answered in the term that received it (the gate of phase 3). From phase 5: a
  move of a namespace and of the universe tenant (a crash at each phase, then a cell with a stale
  universe pointer still reaches it), and the scenario "lose a disk and walk away". With recovery (deferred): a lost cell
  control quorum recovered by `init --recover`.
- Storage chaos on the shipped stores: every role runs on moonpool-journal (landed, #176, #261),
  with the ledgered journal-aware injector aimed through `Journal::regions` (striped by slot)
  under the copy budget, power losses inside a sync (the stores' own `hint!`s, under the same
  budget, #294), and moonpool's environmental storage chaos
  under it; its gates name journal verdicts (slot rebuilt, double fault parked, meta repaired).
  The in-memory stores and the scripted corpus are gone: one campaign (#263, 2026-10-08). The
  corpus's shapes are provoked, never scripted: a correlated outage of every acceptor plans one
  slot's loss for each holder's next boot (aimed at the most recent slot, its holders, every
  copy, or leaving the clean copy on a removed node), and a loss budget's extreme lets at most
  two slots lose every clean copy. The audit recognizes the E1, bare-quorum and
  departed-straggler shapes from the journal's verdicts, and asserts that an unrecoverable slot is
  never accepted again. The simulator's policy (`can_kill`) keeps every other fault inside what a
  set survives, so no journal is excused from convergence. Still to come: replicated fault
  patterns (#202).
- Zones (decided on 2026-10-07, #215): a zone label per process and a zone-kill attrition mode
  (one zone at a time, the outage length drawn across the re-placement bound and restored inside
  the chaos window); moonpool#297 gives `.processes()` groups no locality, so paros keeps its
  own label map meanwhile, an `upstream-to-moonpool` candidate; a latency model per zone pair
  (likely moonpool work); the zone-aware copy budget, where a zone counts as one fault; a composer
  that draws failure domains per seed and applies the zone rule, with a reachable for a refused
  shape; the client's zone and the frontend's origin stamp drawn per seed, the hysteresis knobs, a
  `sometimes` for a leader moved toward its writer's zone and a reachable for a move the
  hysteresis refused. Oracles: two nodes never evaluate one configuration under different failure
  domains (`assert_always!`); every chosen slot's Phase-2 voters span two zones, folded from
  `Accepted` and the failure domains into O(1) audit state; every journal kept committing through
  a one-zone kill (`sometimes`); a two-zone kill is a safety-only shape. M11 is a fault-model change: 10,000
  seeds, and the canary after every new draw.
  **As built** (2026-10-16): the zone-outage scenario (`paros_sim::world::zone_outage`,
  `shape::zone_outage`) forms the cell on three machines, machine `r` in zone `zone-<r % 3>`, and
  crashes every machine of the zone with the most live machines for 10 to 16 s (6 to 10 s in the recovery tail), once the cell
  formed and a client created a journal; while it is down, a client appends to a journal it
  created. Gates "zones: a tenant is created with a zone down" and "zones: a created journal
  commits an append with a zone down".
  The machines' stores sit outside the copy budget, so the zone outage spends none. On a third of
  the seeds the acceptor pool is labeled (`shape::acceptor_zones`, 2 or 3 zones) and the composer
  sends labeled configurations, each judged by the zone rule (a reachable per verdict). Oracles:
  a configuration's labels are bound to its ballot, and a placed journal that spans three zones
  holds the zone rule. Still to come: the Phase-2 voters oracle and the zone-pair latency model.
- Regions and the resolver (decided on 2026-10-07, #253): the copy budget counts a region as one fault;
  in a multi-region cell every chosen slot's Phase-2 voters span two regions; a partition through
  the witness region, after which leadership settles (a liveness oracle in recovery mode); a
  client's two-hop resolution keeps succeeding through an outage of the universe tenant. In M12, with #253: the
  witness region, per-journal proxy leaders in the leader's region (#753 (per-journal proxies)) and a
  move between cell kinds.
- Setup under simulation (decided on 2026-10-09, #246): all of paros runs in the simulation,
  setup included. Simulated `parosd` machines run the shipped `paros::machine::run_machine`
  from an empty disk (format, a minted `node_id`, the machine record, the wait); the workload's
  `init` is `paros::client::initialize`, the code `parosctl init` prints, and every other process
  learns the identifiers through `Inspect`, never by injection. Forming the cell is part of every
  run, never injected state: the seed draws the layout (machine count, roles, which machine
  receives `init`), when and how often `init` and the commands after it are sent, and every fault
  stays on through setup. The harness's own setup holds only what an operator does offline,
  outside paros (generating the Biscuit root key pair, #245). A machine's disk can be wiped: the
  machine at that address comes back as a new machine with a new `node_id`, admitted by
  credential (decision 4), and the cell replaces the old id one for one (rule 5). The harness
  plays no operator to heal it (rule 8). The "no unlearned id" oracle holds on every run: an
  operator's call to the cell names only a `JournalIdentifier` that it learned from `init`'s
  reply or through `Inspect`.
- Faults in the shipped code (decided on 2026-10-09, #294): the simulation runs the same machine
  and disk as `parosd`, with no sim substitute. A choice the code makes is an inline buggify. A
  moment where an environmental fault is interesting is a hint: the code names the moment
  (`hint!("batch durable, not sent").await`, FDB's `if (buggify()) throw please_reboot()`), and
  moonpool decides whether and how to strike, under the seed's attrition regime. Faults come only
  from these two forms; a `buggify_knob!` is a form of inline buggify. Both are inert in
  production. `paros-core` gets none. `DriverHooks`, `SimDisk`, `LedgeredJournal` and `PowerCut`
  go (`DriverHooks` went with #318); `Audit` stays as the one observation seam, for the facts a cross-node or harness oracle
  folds. A fact that only fires a gate is an inline `reachable!`/`sometimes!` instead, and the
  gate-only callbacks move inline site by site (decided on 2026-10-09)
  (`docs/analysis/simulation/production-fault-hints.md`).
- Chaos for the whole run (decided on 2026-10-12, #454 (control plane meets chaos)): the faults
  stop at the cutoff of rule 8, when the client program's `run()` returns, not after a fixed 4 s. Each seed draws its run
  length (the `run_end` knob: default 60 s, extreme 30 to 120 s); `CHAOS_CAP` (150 s) is the
  bound, never buggified. The quiet tail (recovery batch, fleet settle, convergence probes,
  final read-back) runs in `check()`, on the live cluster in recovery mode
  (moonpool's `chaos_until_run_ends`, moonpool#331). FoundationDB tests run the same way. Seeds
  take longer: the point is to test the control plane under faults.
- New BUGGIFY sites for every new decision the driver, the frontend, the resolver and the
  control-plane kit take (#919), and the coverage-guided sweep saturating over them.
- A seed on the production profile (#916) and a ceiling on messages for each second (the gate of
  phase 6).

The TLA+ models of `specs/tla/` are the specification of each liveness or reconfiguration rule
(the proof rule, section 1.1); they do not replace the simulation, and a trace that TLC finds
becomes a red unit test and a scenario before the fix.
