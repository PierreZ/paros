[← 05-failure-model-and-zones.md](05-failure-model-and-zones.md) · [Index](README.md) · [07-changes-milestones-done.md →](07-changes-milestones-done.md)

## 6. Verification

Simulation is the investment. Every milestone lands with its share of:

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
  crash at any step; an election never has two leaders whose writes both land, and settles on one
  leader after the chaos window (a liveness oracle in recovery mode).
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
  it. In M12: a second cell, a tenant move and a move of the universe tenant (a crash at each phase, then
  a cell with a stale universe pointer still reaches it). With recovery (deferred): a lost cell
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
  departed-straggler shapes from the journal's verdicts, excuses an unrecoverable slot's journal
  from convergence, and asserts it is never accepted again. Still to come: replicated fault
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
  `shape::zone_outage`) lists three founding members, machine `r` in zone `zone-<r % 3>`, and
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
  witness region, the per-region pools and a move between cell kinds.
- Setup under simulation (decided on 2026-10-09, #246): all of paros runs in the simulation,
  setup included. Simulated `parosd` machines run the shipped `paros::machine::run_machine`
  from an empty disk (format, a minted `node_id`, the machine record, the wait); the workload's
  `init` is `paros::client::initialize`, the code `parosctl init` prints; the machines it forms
  host the cell control journal and the universe tenant, and every other process learns their
  identifiers through `Inspect`, never by injection. Forming the cell is part of every run, never
  injected state: the seed draws the layout (machine count, classes, capacities, which machine
  receives `init`), when and how often `init` and the commands after it are sent, and every
  fault stays on through setup. The harness's own setup holds only what an operator does offline,
  outside paros (generating the Biscuit root key pair, #245). Until #190 makes the main journal plan data
  (the directory went with #210), the acceptors that serve them keep the per-seed harness draw
  (matchmakers, proxies, replicas, grids) beside the machines; after that they become machines
  too. A machine's disk can be wiped: the machines' attrition draws moonpool's `CrashAndWipe`,
  and a per-seed scenario aims it at a founding member in the middle of `init`. The wipes roll
  (decided on 2026-10-18, moonpool #344 (rolling wipes)): while one wiped machine is unhealed,
  moonpool turns every other wipe it draws in the group into a plain crash. The harness heals a
  wiped machine once the cell holds no booking on its old id. A founder's old id never heals,
  because the cell tenant's own matchmaker set stays on the founders, so a run wipes at most one
  founder. The machine at
  that address is a new machine with a new `node_id`, and it never rejoins as the old one.
  When no vote names the old machine, `init` forms the cell over the new one. When a vote names
  it, the founding members that kept their disks choose that plan (`q1 = n`, `q2 = majority`,
  section 3.1), and the cell forms with the old id as a dead member. Only when a majority of the
  plan's members are wiped is the cell lost (`cell_lost`) until an operator acts outside paros;
  the control-plane liveness oracles do not apply to a lost cell. With three founding members, one of
  them may go down for 6 to 10 s during `init` or once the cell formed (the downed-founder
  scenario, #213 (three founding members)): every tenant creation and every control-journal
  write still complete over the other two. A strike during `init` delays the cell decree until
  the founder is back, because its Phase 1 needs every founding member (`q1 = n`). The acceptors' node registry runs on
  one or three founding members, a per-run draw (#213 (three founding members)). The "no
  unlearned id" oracle holds on every run: an operator's call to the cell names only a
  `JournalIdentifier` that it learned from `init`'s reply or through `Inspect`.
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
  stop when the client program's `run()` returns, not after a fixed 4 s. Each seed draws its run
  length (the `run_end` knob: default 60 s, extreme 30 to 120 s); `CHAOS_CAP` (150 s) is the
  bound, never buggified. The quiet tail (recovery batch, fleet settle, convergence probes,
  final read-back) runs in `check()`, on the live cluster in recovery mode
  (moonpool's `chaos_until_run_ends`, moonpool#331). FoundationDB tests run the same way. Seeds
  take longer: the point is to test the control plane under faults.
- New BUGGIFY sites for every new decision the driver, the frontend, the resolver and the
  coordinators take, and the coverage-guided sweep saturating over them.

No new model checker and no separate specification: the two existing sans-IO model checkers
stay as they are.
