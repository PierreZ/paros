# journal-sim

Deterministic simulation testing of `paros-journal` (#1327 (journal-sim), built with #1324
(paros-journal pure core) in the same PR; `docs/journal/07-verification.md`). A seeded random
walk (moonpool-walk) drives the journal fold over a chain of segments: three writers, resends
across seals, `SetLeader` with `None`, truncates, crashes of the replica that refold its segment,
and the `Resolve` call that settles each `Below`. A second walk drives the journal's matchmaker
register across handovers of the set (#1325). A third walk drives the journal roles
(`paros_journal::roles`, #1326): acceptors, leaders and segment replicas with disks, over a chain
of segments. A fourth walk runs the generic driver of `paros-journal-io` (#1329) on three processes. A fifth walk
drives the pure journal client (#1331) against a journal that loses and holds answers.
A sixth walk runs the whole journal on the in-memory IO of `paros-journal-io` (#1445): two
journals on nine machines in three zones, with their controllers and a client. A seventh walk
drives the coordination store of `paros-election` (#1420) over one journal.
Test only: `publish = false`.

## Map

- `src/oracle.rs`: `Truth`, the fold of the whole log written again from
  `docs/journal/02-data-plane.md`, with no segment. It shares no code with `paros_journal::fold`.
  `as_written` is a command as its client wrote it, with no leader time.
- `src/fold.rs`: the walk (`FoldModel`), its shapes (`Shape::TRACED`, small enough for TLC;
  `Shape::WIDE`; `Shape::LONG`, the traced walk over 200 segments for the live check), its events
  port (`FoldModel::build_reported`, `Reported`), its oracles (`FOLD`, `VIEW`, `RESOLVE`, `REFOLD`), its reach sites and its
  planted mutants (`FoldMutant`).
- `src/register.rs` (+ `src/register/order.rs`): the walk of the matchmaker register
  (`RegisterModel`, #1325) across handovers of the set, its shapes (`TRACED`, `WIDE`), its oracles
  (`CARRIED`, `ACTED`), its reach sites and its planted mutants (`RegisterMutant`).
- `src/roles.rs`: the walk of the roles (`RolesModel`, #1326), its shapes (`NARROW`, the default
  grid; `WIDE`, two columns and promise pages of two), its oracles (`AGREE`, `SEALED`, `FOLD`,
  `VIEW`, `BOUNDARY`, `RESOLVE`; the truth is `Truth` over the chosen log), its reach sites and
  its planted mutants (`RolesMutant`), its events (`RolesEvent`, one action of `Journal.tla`
  each), the leader's retire judged by `RETIRE` (mutant `HoldsLie`) and the trace control `BoundaryBeforeDurable`.
  It also runs proxy leaders (`PROXY`, mutant `ForgedAccepted`); leaderless reads, half of them at
  a replica that hears the register late (`READ`, mutant `ReadNoFence`); the trim of a retired
  segment (`TRIM`, mutant `TrimIntent`, trace events `Trim` and `Drop`) and a reader that takes its floor from
  the segment in force (`FLOOR`, mutant `FloorFromHolder`); and the leader time of each write
  (`STAMPED`, mutant `UnstampedAccept`). The compaction of the acceptors and the replicas on
  `F + 1` reports (`COMPACT`, mutant `ForgedCompacted`; a Phase 1 that hides the dropped votes,
  mutant `DroppedHidden`, is caught by `AGREE`), the trim-point jump of a laggard and the blind
  fold that follows it (trace events `Compact`, `CompactReplica` and `Jump`): a deaf replica misses
  its segment while a truncate in flight cuts it, and one campaign in three writes from the start
  of its oldest segment, so its Phase 1 reads the dropped slots. CTRL (§J5.3): a move damages a vote of an
  acceptor's disk at a boot, inside the contract (a replica applied the slot, or the slot chose
  another command); the leader's blocked slot, its probe on a late answer, its learn from a
  replica, its skip of a slot that a late answer dropped, and the acceptor's repair (`REPAIRED`,
  mutant `RepairForged`; trace events `Rot`, `Probe`, `Skip`, `Learn` and `Repair`).
- `src/control.rs` (+ `src/control/reference.rs`): the grid controller's decisions (#1261 (grid
  controller decision tests), §J6.2 tests 1, 2, 5 and 6). `views` gives every small view (`P <= 3`,
  `F <= 2`, no zones or three or four zones, each count of up, suspect and down members of each
  column, the candidates, the leader, and mixed signals: about 390 thousand views); `check`
  judges each decision with the oracles (`NO_UNSAFE_PLAN`, `BOUNDED_RESHAPE_RATE`, `FENCED`,
  `NO_STALE_PLAN`, `NO_SUSPECT_REPLACED`, `IDEMPOTENT`, `MONOTONE`, `SAME_AS_REFERENCE`), each
  with its `ControlMutant`; `reference` is `decide` written again from §J6.2, with no code of
  `paros-journal-control`. `tests/control.rs` also requires every `probe!` of `decide`
  (`PROBES`), through `campaign::try_sites`.
- `src/controller.rs`: the walk of the controller role (`ControllerModel`, #1261, §J6.2 test 3):
  controllers that race, crash and get deposed, lost requests and replies, failing acceptors, and
  the journal's seal and retire, over an atomic register. Its shapes (`TRACED`, `WIDE` with stale
  views and recoveries, `MANY`: four journals on one shared machine and a token with a cap of
  one), its oracles (`FENCED`, `BOUNDED_RESHAPE_RATE`, `COST_BOUND`, `NO_OSCILLATION`,
  `BOUNDED_IN_FLIGHT`, `CONVERGES`), its reach sites and its planted mutants (`ControllerMutant`).
  `tests/controller.rs` also requires every `probe!` of the role (`role::PROBES`).
- `src/heal.rs`: the heal walk (`HealModel`, #1314 (re-replication of a sealed segment's replica
  set)): the controller role heals a retired segment while replica machines are lost for good or
  come back, two controllers depose each other, requests and replies are lost and the segment is
  trimmed, and requests arrive twice and late; the walk plays the replicas itself (copy from a
  full member, the highest copy per node, drop outside the set, each on the current register value,
  so it cannot see a stale view: `specs/tla/heal-reuse/` checks that), and a node ends an orphan
  copy (a copy of a term that is over, when the register does not name it). Its oracles (`HELD`,
  `COMPARED`, `CONVERGES`), its reach sites, its planted mutants (`HealMutant`, `KeepOrphans`
  too) and its events (`HealEvent`, one action of `HealReuse.tla` each; the trace control
  `SameCopyAgain`). `tests/heal.rs`; `tests/controller.rs` runs it in the gate of the role's probes.
- `src/host.rs`: the driver walk (`HostModel`, #1329): three processes, each a `Driver` of
  `paros-journal-io` with a disk that fails commits inside the contract, a transport through the
  walk's mailbox, the register on three matchmakers (`QuorumRegister`, one matchmaker on each
  process, with lost, half and late writes), crashes that keep the disk and restarts that read it back; its oracles (`AGREE`,
  `FLIGHT`, `ANSWER`), its reach sites and its planted mutants (`HostMutant`). `tests/host.rs`.
- `src/client.rs`: the client walk (`ClientModel`, #1331): two programs with a `Client` of
  `paros_journal::client`, a lossy mailbox, held and lost answers, seals, `Resolve`, reads; its
  oracles (`WRITTEN`, `ONCE`, `REFUSED`, `LED`, `TAIL`, `PAGE`, `FLOOR`: a page holds no record
  below the floor, while a sealed segment still serves the records that a later `Truncate` cut), judged on a log that the walk keeps, its
  reach sites and its planted mutants (`ClientMutant`). `tests/client.rs`.
- `src/election.rs`: the election walk (`ElectionModel`, #1420 (paros-election)): three members of
  `paros_election` over one multi-writer journal and the walk's mailbox, with lost, repeated and
  reordered requests, timeouts, crashes that lose a member's memory and session, early expiries,
  revokes, leaves and joins, checkpoints and trims, and orders to a receiver with the term fence.
  Its oracles (`AGREE`, `TRUTH`: `Truth`, the store's rules written again from
  `specs/tla/coord/Coord.tla` with no code of the fold, `ONCE`, `ACTS`, `SETTLED`, `ONE_HOLDER`,
  `FENCE`), its reach sites, its liveness claim and its planted mutants (`ElectionMutant`).
  `tests/election.rs`. `Truth` reads the entries through the store's codec. No trace check yet
  (**Open**).
- `src/backend.rs`: the backends of the driver walk and the whole walk: `Disk`, a disk that
  survives its process and fails commits inside the contract, `Wire`, a transport that judges
  the flight rule at each send, and `QuorumRegister`, the journal's register on three
  matchmakers (one `RegisterCall` per change, a read and a write at the members that are up, in
  an order drawn from the seed). A matchmaker is down while its machine is: a change or a read
  that no majority answers gives no answer. A read gives the top write that a majority holds,
  or repairs a write that a minority took with a `Confirm` (a choice of this fake; the repair can
  be refused, and the read gives no answer).
  `LossyRegister` loses a change's request or its reply, makes its write reach one member only,
  or holds it in flight (`Fault::Late`) while other operations run, until `QuorumRegister::land`
  writes it at the members that are up. The oracle `AGAIN`: a change asked again over a value
  that holds it is never refused (§J3.4, "a change asked again"). `tests/backend.rs` runs the
  register contract suite of `paros-journal-io` (`contract::register`) on both registers: a
  fake that breaks its trait's contract tests nothing.
- `src/whole.rs`: the whole-journal walk (`WholeModel`, #1445): two journals on nine machines in
  three zones, a `Driver` on each machine, the controllers of each journal run by
  `paros_journal_control::ControlDriver` on the reports of the machines (the walk is its register
  and cell ports; each controller is a process of its own, and the move `Mute` loses its reports
  so the other takes the term from a holder that runs), a shared reshape token, a client that
  writes, resends and resolves, crashes, the loss of a zone for a while, the loss of one machine
  for good (`Lose`), lost register calls, register writes that one matchmaker only takes, and disk faults, the leader's retire and the heal of a
  retired segment's replica set on the `Host` (#1314); its oracles (`AGREE`, `SEALED`, `FLIGHT`,
  `ANSWER`, `ONCE`, `ACKED`, `ALERTS`, `HEAL_HELD`, `BOUNDED_IN_FLIGHT`, `AGAIN`, `DISK`: every
  replica disk holds the chosen log, `STOPPED`: every leader that stops on its own has a true cause
  in the facts that a matchmaker ever held or an acceptor's disk promised, `CONTROLLER_STOPPED`:
  every controller that stops (on any of its four ways) was deposed by a term that a value it
  read holds and a matchmaker held, or by a refusal on its term, mutant
  `ForgedTerm`), its reach sites and its
  planted mutants (`WholeMutant`). `tests/whole.rs` also requires every `probe!` of the
  controller's driver and signals.
- `tests/determinism.rs`: each walk runs two seeds twice and compares the logs
  (`Walk::check_determinism`).
- `src/campaign.rs`: the campaign, which owns the assertion table and judges the reach sites.
- `src/trace.rs`: the events of a walk as a trace of `specs/tla/journal-state/FoldTrace.tla`.
- `src/trace/roles.rs`: the events of a roles walk as a trace of
  `specs/tla/journal-ctrl/RolesTrace.tla`, and the named edits of its recorded traces.
- `src/trace/register.rs`: the events of a register walk as a trace of
  `specs/tla/segment-records/RegisterTrace.tla`, and the named edits of its recorded traces.
- `src/trace/controller.rs`: the events of a controller walk as a trace of
  `specs/tla/grid-controller/ControllerTrace.tla`, and the named edits of its recorded traces.
- `src/trace/heal.rs`: the events of a heal walk as a trace of
  `specs/tla/heal-reuse/HealTrace.tla`, and the named edits of its recorded traces.
- `src/trace/mutate.rs`: the named edits of recorded traces (#1308 (mutate recorded traces)).
- `tests/walks.rs`, `tests/mutants.rs`, `tests/traces.rs`; `tests/roles.rs` (the roles walks and
  their mutants); `tests/roles_traces.rs` (the roles trace check on the narrow and the wide shapes, its rules, its
  control `BoundaryBeforeDurable` and its edits); `tests/register.rs` (the register
  walks and their mutants) and `tests/register_traces.rs` (the register trace check, its rules,
  its control `ForgedBootstrap` and its edits); `tests/controller.rs` (the controller walks, their
  mutants and the role's probes) and `tests/controller_traces.rs` (the controller trace check, its
  rules, its control `ElectNoCas` and its edits); `tests/heal_traces.rs` (the heal trace check, its
  rules, its control `SameCopyAgain` and its edits); `tests/live.rs` (#1310 (check long runs live)): a
  long fold walk checked live through its events port by moonpool-tla's `Live`, and the control
  `BelowInSegment` stopped at its first wrong answer; `LIVE_MINUTES` runs one walk with no end for
  that long (the `Journal live` workflow, four hours each night).

## Rules

The rules of `crates/sim/consensus-sim/AGENTS.md` hold here too. In short:

- **No excuse, no allow list, no skip** (root `AGENTS.md`). A red seed is a finding.
- **An oracle shares no code with the code it judges.** `Truth` uses the journal's types as data
  only.
- **Every oracle has a control**: a `FoldMutant` (or a `RegisterMutant`) in the driver, and a
  test in `tests/mutants.rs` (or `tests/register.rs`) that the oracle catches it. A new oracle gets its mutant in the same PR.
- **Every reach site is reached** by the campaign of its shape; a site the walk does not reach
  gets a walk move, never a larger budget alone.
- **`FoldTrace` extends `Journal.tla` and defines no step of its own** (#1306 (trace specs extend
  the checked model)): each event applies the model's operators (`Res`, `Step`, `Bound`,
  `CoversOn`, `ResolveOn`). A step of the code that the model does not have becomes a step of the
  model, with its check, in the same PR.
- **`RegisterTrace` extends `SegmentRecords.tla`**, **`ControllerTrace` extends
  `GridController.tla`** and **`HealTrace` extends `HealReuse.tla`**, in the same way: each event is an action of the model. **`RolesTrace`
  extends `JournalCtrl.tla`**, CTRL over the compaction over `Journal.tla`: each event of the roles walk is one action of the model, with the
  parameters that the code chose.
- **The trace check has a control** (`BelowInSegment`; `ForgedBootstrap` for the register;
  `BoundaryBeforeDurable` for the roles; `ElectNoCas` for the controller; `SameCopyAgain` for the
  heal: the
  oracles pass it, TLC refuses it) and **refuses the edits** of the recorded walks at the edited event or after it.
  **Every edit finds a place** (`mutate::judge`): an edit with no place in the checked walks
  takes the first walk of up to `PLACE_SEEDS` more seeds that has one, and TLC must accept that
  walk as recorded. An edit with no place fails the test. A new event kind gets its edits in the
  same PR.
- **TLC checks both roles shapes**: the narrow walks, and the wide walks (grids of two columns,
  promises in pages of two). `RolesTrace` gives each slot the Phase 2 quorums of its column
  (`Q2Of`, `slot % columns`) and Phase 1 a majority of every column.
- **A walk that heals judges what it heals.** When the walk replaces a role that gave up (a
  stopped leader, a failed role), an oracle judges that the role gave up for a true cause first
  (`STOPPED` in the whole walk). Else a wrong stop costs nothing that an oracle sees.
- **Every walk has a determinism test** in `tests/determinism.rs`, and every draw of a walk's
  backend starts from the seed (`QuorumRegister::new` takes its draw from `cx.rng()`).
- **A mutant test judges the clean walk too**: each seed that fails with the mutant must pass
  without it (`caught` in `tests/roles.rs` and `tests/whole.rs`).
- **The walks run on fresh seeds each night** (`.github/workflows/journal-nightly.yml`): the first
  seed of every campaign is `campaign::first_seed()`, `JOURNAL_SIM_FROM` or 1. A campaign must
  reach its sites from any first seed: a site that only the CI seeds reach needs a walk move.
- **The checked walks meet every rule** of `RULES` in `tests/traces.rs`,
  `tests/register_traces.rs` and `tests/roles_traces.rs` (a resend answered
  `Below`, a `Resolve` answered both ways, two seals, and more). A rule no walk meets fails the
  test.
- TLC must run: `require_tlc` fails the test when `tlc` is missing (the default nix shell has it).
- **A test that runs TLC is named `tlc_*`**, and only such a test runs TLC: the mutants jobs run
  them in its second pass only (`.cargo/mutants-journal.toml`, `not test(/^tlc_/)`).
