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
- **No excuse and no skip** (decision 16). The simulator decides which faults are allowed, from
  the shards that the shipped code declares (`shard!`, step 1.9 and #1179, section 5): it refuses first each fault that
  the cell does not promise to survive (a kill, a wipe, rot, and a fault that would be one more
  than the survive number of a set). For all the other faults the cell must heal, and the oracle
  judges. Each oracle judges on each seed, and each red has a typed cause. FoundationDB does the
  same; the 32 excuses, `Excuse::ALL`, `paros-sim/src/excuses.rs` and the skips of the
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

**Step 1.1 in the code** (#1090 (one tail clock)). The tail clock (`paros_sim::tail::Clock`)
starts at moonpool's chaos cutoff (`chaos_ended_at`), recorded when the last client's `run()`
returns, and every deadline of the heal check counts from it. The cell healthy predicate
(`paros_sim::tail::predicate`) is a pure function over one admin view and the ground truth of the
simulator. It gives each unhealthy group one typed cause: no live quorum, a member under a
maintenance window, a member that a live machine can take (`Placeable`), or a group the cell does
not serve. The check reads the cell once each pass and heals on 3 passes in a row, 2 s apart, and
waits 1 s after a failed pass, as `FoundationDB`'s `waitForQuietDatabase` does in simulation
(`fdbserver/QuietDatabase.actor.cpp`, lines 925 to 979, release 7.3.43). Each cause is its own
assertion, so a red names its cause. No scenario strikes after the cutoff, and no operator starts
or finishes an operation, ends a window, drains, creates or restarts in the tail. **Open**: the
bound is today `recovery_budget_ms` plus the longest `machine_replace_after` a node drew; the gate
of phase 4 replaces it with one fixed bound. **Open**: the target sizes of control journals and
sets are not judged yet, only their members (decided at the gate of phase 4).
The other tail checks still run after the heal check.

**Step 1.2 in the code** (#1091 (oracle with no excuse)). The excuse tally is deleted:
`paros-sim/src/excuses.rs`, `Excuse::ALL`, the runner's excuse table and the excuse dimension
of the negative hunt (a red on any seed now catches a mutant). Each oracle judges on each seed,
and a red has one typed cause, each its own assertion message. A journal that does not converge,
a chain run that does not serve, and a created journal that takes no write name their cause:
an outage's loss beyond the quorum (`Beyond::Loss`), wipes that took the Phase-1 quorum
(`Beyond::Wipes`), storage faults that took a record's clean quorum (the chain only), a live
quorum missing or a member under maintenance (the leads check), or none, which is a bug. A
matchmaker set or a control journal with a wiped machine must leave it when a live machine can
take its place, with a cause of its own. An injected damage that gets no verdict at its boot is
a red of its own. A tail check that sees the run's token cancelled is a violation
(`tail::cut_short`): moonpool cancels it in the tail only when its stall guard ends a run that
stopped making progress. The nine "cell lost" and "set lost" excuses are one verdict from ground
truth (`tail::judge_lost`), a red asserted once at the end of the tail; the checks of a lost
cell or a lost set do not judge it again, and step 7.2 deletes the verdict. The simulator
refuses first the faults beyond a promise (step 1.9): a red with the cause of a loss or of wipes
past a quorum is a gap in that refusal. **Open**: a double storage fault is not asked of the
policy yet; the rot atlas of step 7.1 places rot. Some checks keep a scope that is a
definition and not a skip: a node down for good (a wipe, a retirement, a corruption crash with
its own assertion, a stop) is not waited for, because a dead machine stays dead; a record
another fact proves (the four second proofs of `audit/claims.rs`) is proved; an admission below
the position a registry model resumed from has no fact to be judged against; a control
journal's call answered after the search of its history is not in the history the search
judged.

**Step 1.3 in the code** (#1092 (larger cell)). One draw a seed sets the machine group's
count, its attrition budget and its outage, so the three agree: a moonpool seed plan
(`SimulationBuilder::seed_plan`, `paros_sim::shape::larger`). On a quarter of the seeds the
machines are a larger cell of one of six shapes, `(2F+1)·P` storage machines plus `F` spares on
some: F=2 in 5 machines, 2 partitions in 6, 2 partitions and a spare in 7, F=2 and 2 spares in
7, F=3 in 7, and 3 partitions in 9. The attrition of the machines gets `max_dead = F` before
the swarm draws it, and an outage takes every machine down at one instant
(`world::outage::machine_regime`). Every other seed calms the machine group, so that outage does
not strike. The layout lists `2F+1` initial members and every other machine, all storage, so
`init` gives the cell a survive number of `F`; the scenarios whose layout needs another shape
give way to it. On three creates in four, a journal asks for `F` failures over `P` partitions,
the mode the storage machines exactly place: with spares a lost member has a machine to go to,
and without one the predicate judges the typed reason (`CannotPlace`). Each shape is its own
gate. **Open**: the share of a quarter is this step's reading of "a share of the seeds"; the
scorecard of step 1.4 decides it.

**Step 1.9 in the code** (#1098 (`can_kill` policy), then #1179 (the kill policy as shards)).
moonpool's shard gate judges each kill and wipe of its fault context: attrition, a reboot hint,
an outage and a scripted crash. FoundationDB does the same with `canKillProcesses`
(`fdbrpc/sim2.actor.cpp`, release 7.3.43), which turns a delete that would leave too few copies
into a reboot. A wipe, or a crash held down with no scheduled restart, is a loss for good. Pierre
decided on 2026-11-17 that the gate judges it from the `shard!` declarations of the shipped
code, not from a policy of the harness, so `paros_sim::world::can_kill` is deleted. paros runs
the gate with `ShardFaults::durable_only` (moonpool PR #546 (held-down copies)): a wipe that
would break a shard's `durable` need becomes a crash, a hold-down that would break it restarts
in place, and a held-down machine counts as a lost copy until it restarts. The shards: each
machine that holds a vote declares the plan of `cell init`, a majority of its members
(`paros::machine::plan_shard`); each journal's leader declares its configurations in force; each
node declares its tenant's matchmaker set, and each matchmaker the successor its record binds
it to. A scenario that wipes past what a journal declares takes the journal's shard out of the
gate, but never the plan's. The harness points a renamed machine's old name at no address, and
the gate counts a replica named by it as down and not intact: one wiped member and one renamed
member leave a plan of three with no majority that can write the new name. A crash with a
scheduled restart is a transient fault: it runs, and the cell must heal. This is a reading of
decision 16: a transient kill is not a loss that the survive number counts. Two hunts of 200
seeds with the policy off and the plan declared: the first had 3 lost-cell reds, each from the
wiped-initial scenario taking the plan out of the gate; with the plan kept in the gate, the
second had none (88 seeds red, each with a liveness cause). An outage's planned loss keeps a quorum of clean copies (decided by
Pierre on 2026-11-17): a slot with no clean copy, one clean copy, or its only clean copy on a
removed node is beyond the promise, so the departed-straggler and bare-quorum scenarios are
deleted. The one verdict (`tail::judge_lost`) and the causes `Beyond::Wipes` and `Beyond::Loss`
stay as judges, and a red of one is a gap in the refusal.

**Step 1.5 in the code** (#1094 (injected liveness bugs)). The hunt mode that turns on a known
bug is the negative hunt (`cargo xtask negative`, `crates/sim/paros-sim-runner/negative/`): each
mutant is one known bug, applied to the source, and the fixed-seed hunt must catch it. Two
mutants are the bugs that the plan names: the answerer rule on every handover
(`handover-finish-proposes-answerers`, caught by the sets oracle) and a campaign that keeps a
stale registration over the reconfiguration in force (`matchmaking-keeps-a-stale-registration`,
caught by the matchmaking oracles). Main is red on a share of the seeds while the causes of
phase 1 are open, so a red alone shows nothing: the command first records the reds of the
unmutated hunt over the same seeds (the baseline), and a mutant is caught only by a red that the
baseline does not have on that seed, of one of the causes that the mutant names
(`paros_sim::mutant_hunt`). The weekly mutation hunt (`cargo xtask mutants`) judges each
cargo-mutants mutant against the same kind of baseline. Over 300 seeds, 21 of 23 mutants are
caught; four of them first by the liveness cause "no tenant is left mid-operation", not by the oracle
their bug is aimed at. Two survive because the hunt's seeds do not reach their state
(#1177 (mutants whose state the seeds never reach)), and one equivalent mutant is deleted.
**Open**: the items of #973 (mutants for the oracles) that are not mutants of a known bug (a
cargo-mutants run over `audit/`, the workload oracles as pure functions, the four oracle
families with no mutant) are tracked in #1177.

**Safety and liveness causes** (decision 33). A cause is an always-assertion that a seed's own
run violated, or a failed run. A safety cause that a hunt finds is fixed before the next step.
A liveness cause belongs to the phase whose mechanism heals it, and its gate closes it ("a
ceiling of zero"): the gate's hunt shows none of the phase's causes. There is no list in the
tree that lets a red pass (Pierre, 2026-11-18): when the simulation is on again in CI, every
cause blocks. The liveness causes that the hunts of main found at step 1.4 are a record for the
gates, #1183 (liveness causes by phase), with the phase that owns each one; the phase is a
reading of the gates, and each gate checks it.

**The simulation is off in CI until the cell heals itself** (decision 33, step 0.13). Main was red
on almost each pull request for causes that the rework replaces, so a red said nothing about a
pull request. From step 0.13 a pull request is judged on the classic tests: the build, the lints,
the unit and integration tests and the TLA+ checks; `paros-sim` still builds so that it does not
rot. The nightly hunt stops too, and the red record of main keeps its last state. No thread fixes
a simulation red of main while the simulation is off. The simulation comes back in CI only when
the mechanisms that heal the cell are built, with no list of known causes (Pierre, 2026-11-18:
"sim should be added back once proper mechanisms have been implemented"). **Open**: the step
that turns it on again; the reading is after the gates of phases 4 (recovery) and 5 (heal),
which close most of the causes of #1183. Until then, each simulation gate is a local hunt, and
its result goes in the gate's pull request. FoundationDB keeps its
large hunt outside the gate of a pull request in the same way: the pull request runs the build and
`ctest`, and Joshua runs the ensembles and records each failure with its seed.

### The models

Six models carry the gates (section 7). Four are new, and two extend a model in the tree. Each
has a `LiveSpec` with fairness and a control that must fail with its rule off.

| Model | Phases | Safety | Liveness, with `LiveSpec` | Control that must fail |
|---|---|---|---|---|
| `config-register` (new, from `cas-register`) | 1, 2 | Agreement, with three ballots and a candidate that stops after any step; also with a partitioned layout and with a full grid (step 1.13), and with a successor of another size | With at most F acceptors gone, a campaign wins in the end | A switch before the recovered slots are held; a stored stale belief; an abandoned successor; a store that is released before the switch |
| `handover` (extends `handover-gc`; **done**, #1097 (rework 1.8)) | 1, 2 | Only a resize record changes the size; the first write needs every new member; the floor and the register cross the handover, also while a journal has an open successor | A handover with at most F members down ends | The answerer rule |
| `cell-control` (new) | 1, 3, 4, 5 | One writer for each control journal in each term; the cell never lowers its survive number without an operator record; an order of a deposed coordinator changes nothing that was not recorded; a record between two levels is applied exactly once across a change of term | A coordinator serves again; every set returns to its target, or reports that it cannot place; the registry names no dead id | Two writers; an order with no fence; a repair in place; candidates from bookings; an address written through the coordinator |
| `tenant-move` (deleted in phase 0; a new model in phase 5 for the move of a namespace) | 5 | The invariants of the new move, from #1070 | The move ends, or its abort ends | The controls of the new move, written with the model |
| `election` (new, small) | 6 | One leader for each ballot | One leader holds after a healed partition | No pre-vote |
| `reads` (extends the model in the tree) | 1 | A read never returns a state older than a write that was complete before the read started; with a majority, a partitioned grid and a full grid with the row read, and across a change of members | Not in this model | A fence on the believed ballot in place of the promise (step 1.10); a row that did not hear of a completed successor (step 1.13) |

The check with three ballots of `config-register` runs in CI, not as `long`, because two ballots
cannot show the bug of the one-slot register (section 4.1). Each `long` check of the tree is run
one time and its result recorded (step 1.6). The models `retire-fence` and `abandoned-ballot`
leave CI when phase 2 deletes their rules; their traces stay as controls of `config-register`.

Each Paxos flavor of `paros-consensus` has a model too, written before any code on the flavor
(decided on 2026-11-18, decision 37; the list is in #1185 (TLA+ model multi-paxos)). Single-decree
Paxos is `specs/tla/decree/` (#1186 (rework 1.15)): `Agreement` and `Ends` for its two uses, the
matchmaker handover under a majority and `cell init` with `Phase1Rule::EveryAcceptor`. Its
controls are a proposer that ignores the highest reported vote and a `cell init` whose Phase 1
takes a majority while one member was wiped. The model also shows that `EveryAcceptor` alone is
safe only while fewer than a majority are wiped; past that, the rule that a wiped member never
votes keeps the decree safe.
Plain Multi-Paxos is `specs/tla/multi-paxos/` (#1185 (rework 1.14)): `Agreement`, `LearnedChosen`
and `OneProposal` (a new leader allocates above each slot that its Phase 1 reported), and `Ends`
(with at most F acceptors gone, the last leader learns every slot below its frontier), with the
majority, the partitioned grid and the full grid. Its controls are a leader that allocates from
its first unlearned slot, an acceptor that answers before its write is on disk, no `Noop` for a
gap, and a Phase-1 quorum inside one grid column. It also shows that the plain log has no slack
for a wiped acceptor: one wipe that votes again as the old acceptor breaks `Agreement`. The table
of every flavor with its model is in `specs/tla/README.md`.

The cooperative leader handoff of DPaxos is `specs/tla/leader-handoff/` (#1187 (rework 2.8)):
plain Multi-Paxos where the leadership is a state of a node, so an elected leader can give its
ballot, its allocator frontier and its tail (decided slots and open rounds) to a successor with
no Phase 1. Its safety checks are `Agreement`, `LearnedChosen`, `OneProposal` (one proposal for
each slot and ballot, across the handoff) and `OneHolder` (one node at a time leads at a
ballot), with a payload that arrives late or two times, a successor that restarts or resigns on
its fence, and a later election. Its liveness check `Ends` has two forms: a handoff ends, and an
election replaces a successor that is gone (it restarted, or the payload was lost). Each rule has
a control that fails: a handoff leader that hands on again (two holders), a successor that takes
a replayed payload below its frontier, a leader that does not record its own rounds (a restart
forgets the frontier and a replayed payload rewinds it), and a successor that drops the open
rounds (a slot stays open for ever). The model also shows review finding 3 of paros-consensus:
with `on_heartbeat` as it is today, a late beat of the predecessor deposes the successor. That
costs liveness only, never safety: the control `live_late_beat` fails and `late_beat_safe`
passes. The fix of step 2.7 is the `BeatGuard` rule of the model, the guard that `on_accept`
has: a leader keeps its role on a beat at the ballot that it holds.

Matchmaker Paxos is `specs/tla/matchmaker/` (#1200 (TLA+ model Matchmaker Paxos)): `Agreement`
and `Electable` for single-decree Matchmaker Paxos with matchmaking, Phase 1 over every
configuration of `H_b`, a reconfiguration as a round change, GC and the retirement of a removed
acceptor. Each rule has a control that fails: Phase 1 in `C_b` alone or at a majority of the union
of `H_b`, a matchmaker that registers below a ballot it holds, a history from one answer, a Phase 2
to a configuration the ballot did not register, GC as soon as Phase 1 is done, a removed acceptor
that shuts down at once, and a floor that counts as effective at one ack.

The other roles of Compartmentalized Paxos are `specs/tla/compartments/` (#1190 (rework 6.10)):
the batcher and the unbatcher, the walk and the journal fold of the replica, and the read
replica, over an abstract log that `multi-paxos/` checks. Its safety checks are `OneRange` (each
write of a batch gets its own `seq` range one time, also when a batch is sent again: a
`TooLarge` split, an `Accept` sent again, a slot that a new leader recovers), `AnswerAgrees` (the
unbatcher's range is the range that each replica folds), `InOrder` (a replica folds the chosen
log in slot order with no gap) and `ReadFresh` (a read that a read replica serves holds each
write that was complete before the read started). Its liveness check `Ends`: each batch that was
sent ends in the log and at each replica, also when its batcher stopped. Each rule has a control
that fails: a batcher that sends an ambiguous batch again (a write applied two times), a walk
that folds past a gap, a fold state that survives a restart (a slot applied two times), a read
served from the replica's prefix alone, a read replica that counts as a voter, and a leader that
does not send lost `Accept`s again.

### The checks beside the simulation

Decided on 2026-11-17, from the article "How the Claude Code team thinks about verification"
(Anthropic, 2026-11-16). The simulation and the models stay the proof of the protocol. Three
kinds of check go beside them. Each one is cheap to run, hard to fool, and not written from the
same view as the change that it judges.

- **End to end: run the change.** `scripts/verify-cell.sh` (the `verify` skill) starts three
  `parosd` processes on loopback and drives the flow of a user with `parosctl`: `init`, a
  tenant, a journal, writes and reads, a superseded writer, a machine killed with SIGKILL and
  started again, a clean stop. It runs before each pull request that changes code a machine
  runs. It is a local check and not a CI job. While the simulation is off in CI (decision 33),
  it is the only check that starts whole machines on each change. A step that fails names its
  command, its output and the logs of the machines.
- **Property tests for the pure parts.** A property is a statement that cannot fail, and a
  library generates the inputs that try to break it. The pure parts of paros have such
  statements, and the step that next changes a part writes its property first, so that the
  property judges the change. Where no step is named below, it is the first step of the rework
  that changes the part:
  - a wire message that is encoded and then decoded is the same message, for each message of
    `paros-wire`, and a decode of any bytes is a refusal and never a panic (step 0.1, before the
    one renumbering of the proto fields);
  - a fold from a checkpoint gives the same state as a fold of the full history, for the
    registry, a tenant control journal and the universe directory (`paros-model`);
  - a placement never books two members of one set in one failure domain when the zone rule
    binds, never books a machine that cannot take the role, and gives a typed "cannot place"
    reason only when no set of machines fits (`paros_model::placement::plan`; step 4.5);
  - a balance round on a balanced cell asks for no move, and a move never makes the widest gap
    larger (`paros_model::balance`);
  - an operation that is applied two times has the effect of one: a request with an
    idempotency id, a `SetLeader` with the same uuids, a booking with the same id (the folds of
    `paros-model`).

  A property test is a unit test: it runs in `cargo nextest run`, it is fast, and it draws from
  a fixed seed so that a run is the same each time. A case that it finds becomes a named unit
  test with the shrunk input, and no file of failing seeds is kept ("No pinned seeds").
  The values come from `arbitrary`, a dev-dependency of the crate under test, drawn from a
  fixed byte stream in the tree (step 0.1, `paros-wire`'s `tests/property.rs`); `paros-consensus`
  takes no new dependency.
- **Differential tests: the old code is the oracle.** When a step replaces a part and the
  behaviour must stay the same, the old part and the new part run on the same generated inputs,
  and each difference is a bug or a change that the pull request names. The steps of the rework
  with this shape:
  - step 1.12 (one Belief role, shared bytes, the chosen map read from the acceptor records):
    "no change of behaviour" is checked on the stream of `Ready` values, for the same schedule
    of messages and ticks, before and after;
  - step 2.3, first and second pull requests: while the register is stored beside the
    registrations, each campaign reads both, and an assertion says that the register names the
    same configuration in force as the fold of the registrations. The third pull request
    deletes the registrations and the assertion with them;
  - the committed traces of the Paxos pages (`website/site/src/traces`) are already a check of
    this kind for `paros-consensus`: a change of behaviour shows as a diff of a trace.

  A trace that TLC finds is also an input: it becomes a red unit test before the fix (the proof
  rule). **Open** (step 1.6 decides it): whether the traces that a model accepts are also
  replayed on `paros-consensus` as a conformance check.

Three working rules keep these checks honest (`AGENTS.md`, *Workflow rules*): a pull request
does not change its own judge, a flaky check is a finding, and a thread that is red after three
rounds stops and reports. `REVIEW.md` lists what a reviewer checks for each kind of diff.

### What every phase lands with

Every phase lands with its share of:

- Invariants in the audit, where the fact arrives: a leader uuid leads only from a `SetLeader`
  won in the log, every verdict naming the leader in force (through a misbehaving client's
  reinstatement too, section 2.3), every leadership change present in the log, `seq` dense per journal, a
  single-writer `Write` never re-accepted with other bytes, `Truncate` monotone and `first_seq`
  never above a served cursor, a single-writer `Truncate` accepted only from the current leader, a
  `SetLeader` winning at most once per `old_uuid` in a linearization, a multi-writer journal never refusing an
  in-limits write, a booking id never booked twice (across
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
  slot's loss for each holder's next boot (aimed at the most recent slot or a uniform one), and
  the plan keeps a quorum of clean copies. A loss beyond the survive number of a set (no clean
  copy, one clean copy, or the only clean copy on a removed node) is a fault that the cell does
  not promise to survive, so the simulator refuses it first (decided by Pierre on 2026-11-17,
  step 1.9, #1098 (`can_kill` policy)): the departed-straggler and bare-quorum scenarios are
  deleted. The audit still asserts that an unrecoverable slot is never accepted again. The
  simulator's shard gate keeps every other fault inside what a set survives, so no
  journal is excused from convergence. Still to come: replicated fault
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
  production. `paros-consensus` gets none. `DriverHooks`, `SimDisk`, `LedgeredJournal` and `PowerCut`
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
