[← 05-quorums-and-failures.md](05-quorums-and-failures.md) · [Index](README.md) · [07-verification.md →](07-verification.md)

## J6. The controller

**The controller decides when and where; the protocol decides whether it is safe.** Each journal
has one grid controller, elected by a term in the journal's matchmaker register, on stateless
machines (§J6.1). Its only effect on the journal is the fenced `reshape` call. It never orders,
never decides the safety of a retire, never calls `SetLeader` or `Truncate`, and never moves data
itself (the consolidated v1 plan, section 6).

**The v1 duties** (Pierre, 2026-11-20): heal (a reshape, or the re-replication of a sealed
segment), scrub with repair through the protocol (§J5.3), and hold and alert in every other case
(a zone loss, a column below quorum, data loss). No roll, no offload, no service level
objective, no scaling from load. The roles publish their metrics to the controller; which
measure drives which action beyond heal is decided after tests (§J6.2). A simple rate limiter
caps the expensive actions in flight.

**Open** (decided with #1328 (paros-journal-control), after #1189): the cap of the rate limiter,
the minimum interval between two reshapes, and the suspicion windows. No number is set until
Pierre or a source gives one.

### J6.1 The grid controller's fenced reshape

*Moved here from `docs/architecture/02-data-plane.md` §2.12 (#1332 (journal architecture docs)).*

**The grid controller is its own role in each journal, elected by a term in the journal's
matchmaker record** (decided by Pierre on 2026-11-20, #1259 (the fenced reshape call of the grid
controller)). It runs on stateless machines; the Paxos leader does not run it. Its only effect on
the journal is one compare-and-set on the matchmaker record:

    reshape(expected configuration, new grid, controller term)

The new grid names the acceptors of the new segment and its replica set, so the record of the
segment gets both in one write (§J3.4; `specs/tla/journal-state/`, #1189 (the journal state model)).
The controller decides when and where; the record's guards decide whether it is safe. So the
record trusts nothing that the controller computed. `specs/tla/grid-controller/` checks the
election and the call with two controllers, a candidate race, a deposed controller that keeps
calling, a crash between the plan and the call, a takeover after a crash, a lost request or
reply, and an acceptor that fails after a plan named it. Each guard has a control that TLC
breaks:

- **The election raises the term.** A candidate reads the term, then writes term + 1 by
  compare-and-set on the term it read; of two candidates that read the same term, one loses.
  An election that keeps the term gives two controllers the same term, and both open segments.
- **The call carries the term, and the record refuses any other term.** A deposed controller
  that keeps calling changes nothing, even when its call names the configuration in force. A
  refusal for the term tells it to stop.
- **The expected configuration is the newest configuration of the table.** With this check, at
  most one segment opens from one configuration, with or without the term check. A segment opens
  twice from one configuration only when both checks are off: a deposed controller's call lands
  after the new holder's.
- **No reshape while a segment is not retired.** The newest configuration must be in force and
  every older one retired, so one configuration is in force and at most one other is not retired
  (rule 1 of the plan). A reshape asked while a segment is sealed but not retired is refused.
- **The record checks the grid itself** (§J5.1): in v1 it takes only `rows = 2F + 1`,
  `acks = F + 1`; the model checks the wider bound `F + 1 <= acks <= rows - F`. A grid outside the
  bound can open and then never come into force: with `acks = 1` and one acceptor down, Phase 1
  needs every member, the new configuration stays pending, and the guard above blocks every later
  reshape, so the journal can never heal.
- **The record answers a retry with the same answer** (a rule that the model found, decided by
  Pierre on 2026-11-20). The record keeps the identity of the call that opened each segment (the
  controller term, the expected configuration and the grid), and answers a call that it already
  took with "ok". Without it, a lost "ok" makes the retry fail its expected-configuration check,
  and the controller is told "refused" for a reshape that took place.

After the faults stop, every call is answered (done or refused) and every configuration that a
reshape opened comes into force. The controller's crate (`paros-journal-control`) and its
walk in `journal-sim` (#1261 (grid controller decision tests), §J1.1) come after the
journal state model (#1189); the walk then trace-checks against this model.

### J6.2 Journal health

*Moved here from `docs/architecture/05-failure-model-and-zones.md` §5.1 (#1332 (journal architecture docs)).*

**Every part of a journal has a named health measure, and every measure has a fixed action**
(decided by Pierre on 2026-11-20, #1260 (journal health measures)). Pierre asked for the full
list, so that the system and its tests know the state of every part. **The v1 scope of the second review** (Pierre, 2026-11-20, #1315 (bring docs and models in step
with the v1 scope)) cuts this section for v1: v1 grids are the partitioned grid only
(`rows = 2F + 1`, `acks = F + 1`, the majority being one column), so the two spares of a column
are one number and WritesBlocked is not a v1 state; the bypass of §J5.5 is out of v1, so
WritesDegraded holds and alerts; the Offload rows of the catalogue are for after v1. The model
`specs/tla/health/` checks v1 with its `V1` switch (the `v1` check, #1319 (a v1 switch in
health/); "Which checks cover v1" below), and keeps its other checks, on the wider grids and the
bypass hold, as the design for after v1. The other decisions of that
day bind this section: the grid controller (§J6.1) is the journal's own elected role on
stateless machines and also owns scrubbing; after a zone loss it holds and alerts; v1 has no
unsafe recover; the autoscaling decisions get heavy tests. **The v1 controller has no service
level objectives and no scaling driven by latency or load** (Pierre, 2026-11-20): in v1 it
heals (re-replication of a sealed segment included) and scrubs, and it changes a count only to
replace a member or a role that is gone. It does not roll segments and does not offload in v1
(§J1.1, decisions of #1315). **v1 stays simple** (Pierre, 2026-11-20): each role publishes its metrics to the
controller role, and which measure drives which action is decided later, after more tests. So
the catalogue below is the reference list of measures. Its actions bind v1 only where heal
needs them: the liveness measures, the controller's pace, and the guards of the protocol (the
journal states, the flags, the retire guards, `data_lost`, `drop_before_mark`). Every other
action in the table is a proposal, **decided later**. The minimum interval between two reshapes, the suspicion windows and the cap of the rate
limiter are **Open**: no number is set until Pierre gives one or a source gives one (the
controller's crate, after #1189 (journal state model), decides them).

**Two kinds of measure.**

- **P, a protocol fact.** A pure function over the configuration and the set of live members,
  built on the predicates of `membership/` (`has_phase1_quorum`, `has_phase2_quorum_in`,
  `MatchmakerSet::has_quorum`, `zone_rule`), or a fact of the segment table at the matchmakers.
  Each one is named below. The functions that are not in `membership/` today come with the
  controller's crate (`paros-journal-control`, after #1189). A P measure never counts a member
  against a threshold of its own: it crosses `membership/`, as a tally does.
- **O, an operational signal.** A count or a time from the `Events` port of the journal's step
  contract (§J4.2, rule 5, #1255 (journal sans-IO contract)): latencies, rates, queue depths.
  In v1 only the O measures of liveness (`live`, `flap_rate`, `live_count`, `zone_live`) and of
  the controller's own pace (`seal_age`, `signal_age`, `since_last_reshape`, `tokens_held`) make
  the controller act. The O measures of latency, load and errors are shown on the status and
  trigger nothing in v1: the table marks them "shown". An O measure never changes a quorum.

**The controller decides when and where; the protocol decides whether it is safe.** Each action
below is a request that passes the guards of its step: the fenced reshape (§J6.1), the
hold of a bypass (§J5.5, after v1), the retire at `F + 1` holders (§J3.1), the repair of CTRL
(#1263 (CTRL recovery in the journal)). A wrong measure can make the controller ask at the wrong
time. It can never make an unsafe step pass.

#### The spares of a column

A column has two spares, because Phase 1 and Phase 2 need different counts of it (`grid/Grid.tla`):

- `phase2_spare(column) = live − acks`: how many more members the column can lose and still
  choose a slot.
- `phase1_spare(column) = live − (rows − acks + 1)`: how many more members the column can lose
  and still give the `rows − acks + 1` promises that a Phase-1 quorum takes of every column.
- `spare(column)` is the lower of the two.

On the partitioned grid of §J5.1 (`rows = 2F + 1`, `acks = F + 1`) and on the majority
(one column) the two spares are one number, and the status shows one spare per column. These are
the only v1 grids (§J5.1). Other grids inside the bound `F + 1 <= acks <= rows - F` split
them; they are not in v1, and the catalogue keeps both spares for the day they come:

- `rows = 4`, `acks = 2`, a column with 2 live: it still writes (`phase2_spare = 0`), but no
  election can run (`phase1_spare = −1`).
- `rows = 4`, `acks = 3`, every column with 2 live: an election can run (`phase1_spare = 0`), but
  no column can write (`phase2_spare = −1`). A new leader can still reshape, because a reshape
  needs Phase 1 of the old grid and Phase 2 of the new grid only.

`specs/tla/health/` checks every count of this section against the quorums of `grid/Grid.tla`,
on every grid that the fenced reshape accepts up to 9 acceptors, for every set of live members
(`health`, 29 thousand states):

- **The counts say what the quorums say.** Every column has `phase1_spare >= 0` exactly when a
  Phase-1 quorum is live (`electable`). A column has `phase2_spare >= 0` exactly when it can
  choose a slot (`writable`). A count that reads the wrong threshold fails: electability judged
  on `acks` per column (`p2_only`), on a majority of all members (`majority`), and writability
  judged on `rows − acks + 1` (`p1_only`) each give a wrong answer on a grid of four rows.
- **A spare in every column means one more loss is safe.** `spare >= 1` in every column exactly
  when any one more loss leaves every column writable and an election possible.
- **The hold of a bypass can complete exactly when a column can write.** The hold of §J5.5
  waits until the promises meet every read quorum of the old grid. A set of live members meets
  every Phase-1 quorum exactly when it holds `acks` members of one column, which is
  `phase2_spare >= 0` in that column. With one promise less (`hold_short`) the count is wrong.

#### The journal states

The grid part of a journal's health is one of seven states. The states are a partition: each
set of live members falls in exactly one (`OneState`). Each state has one fixed action, and the
model checks that the action can run in that state, or that no action can (`ActionCanRun`).

| State | Condition | Action |
|---|---|---|
| Healthy | `spare >= 1` in every column | none |
| AtRisk | a column has `spare = 0`; electable and writable; a placement that meets the zone rule exists | one heal reshape (it can run its full Phase 1). Two columns at risk: one reshape heals both |
| AtRisk-Unplaceable | as AtRisk, and no placement meets the zone rule (a zone loss on three zones) | hold and alert: the degraded trap of architecture §5 |
| WritesDegraded | not electable; a column can write; the leader is live | writes skip the columns that cannot write (#533 (move a stuck round)). v1: hold and alert. After v1: a heal by the bypass of §J5.5, whose hold can complete. No reshape runs, so the open segment grows until a member comes back |
| ElectionBlocked | not electable; a column can write; no live leader | hold and page: no leader can be elected, so no write and no reshape until a member comes back |
| WritesBlocked | electable; no column can write | a new leader heals by a full reshape (only on a grid where `acks > rows − acks + 1`; no v1 grid, so not a v1 state) |
| Stopped | not electable; no column can write | hold and page: no election, no write and no bypass hold |

The first draft of this table (in the journal design document) named WritesDegraded and
ElectionBlocked by "a column below quorum" and the leader alone, and had no state for a grid with
no writable column. TLC breaks both: with no column that can write, a live leader put the journal
in WritesDegraded, whose bypass hold cannot complete (`draft_states`), and an electable grid with
no writable column had no state (`draft_one_state`). The table above adds WritesBlocked and
Stopped.

**Which checks cover v1** (#1319 (a v1 switch in health/)). The constant `V1` of
`specs/tla/health/` keeps only the v1 grids (the partitioned grid, `rows = 2F + 1` and
`acks = F + 1`, the majority being one column) and the v1 actions (no bypass). The `v1` check
runs it on every v1 grid up to 9 acceptors and every set of live members (5 thousand states):

- the counts say what the quorums say (`ElectableMeaning`, `WritableMeaning`, `SpareMeaning`);
- the states are a partition (`OneState`);
- the action of each state can run (`ActionCanRun`): WritesDegraded needs only a column that can
  write, because v1 holds and alerts and runs no bypass;
- the two spares of a column are one number (`PartitionedOneSpare`), so the status shows one
  spare per column;
- WritesBlocked is empty (`NoWritesBlocked`): on a v1 grid a Phase-1 quorum holds `acks` members
  of every column, so an electable grid can write in every column. Without `V1` the same
  invariant fails (`reach_writes_blocked`), so the state stays in the table for after v1.

The control `v1_draft` runs the states of the first draft on the v1 grids: a live leader with no
column that can write falls in WritesDegraded and cannot write, and `ActionCanRun` fails. The
zone checks (`zones2`, `zones3`) hold on every grid, the v1 grids among them. Every other check
of the folder (`health`, the controls of a wrong threshold, `hold_short`, `draft_states`,
`draft_one_state`) is the design for after v1.

Beside the grid state, three flags change what the controller may start. Each one comes from a
rule that a TLA+ model checked:

| Flag | Condition | Action |
|---|---|---|
| Reshaping | a segment is sealed and not retired (§J6.1: no reshape while a segment is not retired) | no second reshape; the heal waits for the retire |
| ReshapeStalled | the seal is older than the stall window (**Open**) | alert with the cause: a replica set below `F + 1` holders, a found damage that is not repaired (`repairs_pending`), a matchmaker set below a majority, or an old grid that cannot give Phase 1 (or, after v1, the bypass hold) |
| ControlFrozen | the journal's matchmakers are below a majority | with no reshape in flight, the data plane serves under the standing leader, and leaderless reads go on (they read no matchmaker, §J3.1). Between a seal and the boundary write, acked writes and tail reads of the new segment stop until a majority is back (accepted for v1, §J3.4); no election, seal, retire, reshape, offload mark or controller election can run (§J5.1). The action is a matchmaker handover when a majority of the old set is live, else hold and page |

**A column lost for good is beyond the contract.** v1 has no unsafe recover (Pierre,
2026-11-20). A column that lost more than `F` disks for good leaves the state Stopped or
ElectionBlocked for ever, and the journal says so. A slot that only that column holds, above
the applied prefix of the replicas, can be lost. The journal reports it as data loss
(`data_lost` below). It never fills such a slot with a `Noop`: the tri-state tally of CTRL
blocks on a slot that it cannot prove empty (#1216 (truncation model, part 2)).

#### The catalogue

P = a protocol fact; O = an operational signal. A window, a target or a rate without a number
is **Open** (the first paragraph of this section). "Shown" means the status shows the measure and
the v1 controller does not act on it. An action outside heal, the protocol guards and the controller's pace is
a proposal, decided later (the first paragraph of this section).

| Level | Measure | Kind | Definition | Unit | States and the fixed action |
|---|---|---|---|---|---|
| Acceptor | `live` | O | the acceptor answered inside the suspicion window | up, suspect, down | suspect and down count as lost for the spares |
| Acceptor | `ack_latency` | O | p99 time from `Accept` to `Accepted`, against the median of its column | ratio | shown (no latency-driven action in v1) |
| Acceptor | `flap_rate` | O | changes between up and down in one window | changes per window | flapping: held as suspect; replaced only after N quiet windows, longer than the minimum reshape interval |
| Acceptor | `faulty_slots` | P | CTRL faulty records (#1216, #1263) that are not repaired | slots | above 0: a repair through the protocol (the CTRL repair probe, a chosen value written under the record's ballot in one write); stuck: alert |
| Column | `phase2_spare` | P | `live − acks` (`has_phase2_quorum_in` on the live set) | acceptors | `>= 1` healthy; `0` AtRisk; `< 0` writes skip the column |
| Column | `phase1_spare` | P | `live − (rows − acks + 1)` (`phase1_per_column`) | acceptors | `>= 1` healthy; `0` AtRisk; `< 0` not electable: WritesDegraded, ElectionBlocked or Stopped |
| Column | `move_rate` | O | rounds of the column that moved to another column (#533), per round | ratio | shown; a high rate while `phase2_spare >= 0` points to a gray failure |
| Grid | `electable` | P | a Phase-1 quorum of every configuration of `H_b` is live (`has_phase1_quorum`) | yes, no | no: see the journal states |
| Grid | `writable` | P | some column can choose a slot | yes, no | no: WritesBlocked or Stopped |
| Grid | `bypass_hold` | P | the live members meet every read quorum of the configuration in force (§J5.5); equal to `writable` | yes, no | after v1: the WritesDegraded heal can run |
| Grid | `zone_rule` | P | `AcceptorConfig::zone_rule` on the configuration and on its live members | Holds, LosesQuorum, CommitsAlone, Unlabeled | not Holds on a plan: the plan is refused |
| Grid | `zone_margin` | P | the lowest `phase1_spare` and `phase2_spare` after the loss of any one zone | acceptors | `< 0`: a zone loss stops elections or writes; shown on the status |
| Grid | `segments_unretired` | P | segments that are open or sealed (segment table) | segments | above 1: the Reshaping flag |
| Grid | `seal_age` | O | time since the seal of the oldest segment that is not retired | seconds | above the stall window: ReshapeStalled |
| Journal | `leader_known` | P | a won leadership with a fresh beat | led, electing, leaderless | leaderless while not electable: ElectionBlocked |
| Journal | `commit_latency` | O | time from propose to chosen | seconds | shown |
| Journal | `open_rounds` | O | allocator frontier minus chosen prefix | slots | shown |
| Journal | `apply_lag` | O | chosen prefix minus applied prefix at the replicas | slots | shown |
| Journal | `election_rate` | O | won elections per window | elections per window | shown |
| Matchmakers | `mm_spare` | P | live members minus a majority (`MatchmakerSet::quorum_size`) | matchmakers | `0`: a matchmaker handover; `< 0`: ControlFrozen |
| Matchmakers | `generation_frozen` | P | a `Stopped { successor: None }` is seen, so a handover is in progress | yes, with an age | alert on age: any node that sees it finishes the handover |
| Replicas | `holders(segment)` | P | replicas of the segment's set that hold the whole segment with a good scrub | replicas | below `F + 1`: no retire (safety); below the full set on a sealed segment: re-replicate from a full member of the set; the new replica counts only on its own complete copy, and the set changes by compare-and-set on the register version (v1, §J3.1, #1314). A found damage that is not repaired also blocks the retire (`repairs_pending`, a margin) |
| Replicas | `boundary_written(segment)` | P | the boundary state of the seal is at the matchmakers (§J3.1) | yes, no | no while sealed: no retire; the first replica that applied every slot writes it |
| Proxy leaders, batchers, unbatchers | `live_count`, `queue_depth`, `batch_fill`, `utilisation` | O | per process | count, entries, ratio, ratio | a role below its count is started again (`live_count`); the others shown |
| Offload (after v1) | `offload_backlog` | P | segments that are retired and not marked offloaded | segments | late: alert; more offloaders |
| Offload (after v1) | `store_errors` | O | failed puts and reads of the object store | errors per window | shown; the offloader backs off on each error |
| Offload (after v1) | `drop_before_mark` | P | a replica dropped its copy before the record said offloaded | events | must be 0: a safety alert (§J3.1) |
| Offload (after v1) | `orphan_objects` | P | objects whose segment is below the trim floor | objects | above 0: the controller's scrub deletes them (#1256 (offload races)); an object of a segment that is not trimmed is never deleted |
| Scrub | `scrub_coverage(segment, member)` | P | share of the member's copy of the segment that a scrub read in this pass | ratio | below 1 at the end of a pass: the pass is late |
| Scrub | `scrub_age(segment, member)` | O | time since the last scrub of that copy | seconds | above the pass bound: the next scrub goes there first |
| Scrub | `damage_found` | P | records, replica log entries and objects that failed their checksum | records | above 0: a repair is queued |
| Scrub | `repairs_pending` | P | damage found and not repaired, per segment | records | above 0 on a segment: no retire of that segment until the repair is done (a margin, kept by Pierre on 2026-11-20, #1263: `F + 1` holders already leave a good copy, so safety does not need it); above 0 for N windows: alert |
| Data | `data_lost` | P | a slot or a segment with no good copy at the acceptors, the replicas or the object store | slots | must be 0; above 0: page. A report is always true (#1263); no unsafe recover in v1 |
| Controller | `term_current` | P | the controller's term is the term of the matchmaker record | yes, no | no: stop (§J6.1) |
| Controller | `signal_age` | O | age of the oldest input of a plan | seconds | stale: no expensive plan |
| Controller | `since_last_reshape` | O | time since the last reshape of the journal | seconds | inside the minimum interval: no reshape |
| Controller | `plans_refused`, `direction_changes` | O | refused calls by reason; up-then-down changes of a count | count per window | shown; a direction change in one window is an alert |
| Controller | `tokens_held` | O | expensive actions in flight, against the per-cell cap of the rate limiter | actions | at the cap: the action waits |
| Zone | `zone_live` | O | share of a zone's machines that answer | ratio | lost: one correlated decision for every journal of the zone, not one per journal |

#### A zone loss

**A zone loss takes every column to its lowest spare at once.** Each column spreads over the
zones like one majority. With `F = 1` over three zones the layout is row = zone, so a zone loss
takes one acceptor from every column, and every column drops to `spare = 0` together
(`ZoneLossEveryColumn`; on the partitioned grid with `2F + 1` zones, to `F − 1`). A rule "heal a
column when its spare reaches 0" would then start one heal for every column of every journal of
the zone.

**The controller heals after a zone loss only where a placement meets the zone rule.** No
placement over two zones survives the loss of one of them, on any grid: a Phase-1 quorum inside
one zone misses a Phase-2 quorum inside the other (`NoPlacement`, checked over every map of up to
six acceptors to two zones; with three zones TLC finds a placement, `zones3_placeable`). So on a
cell of three zones, every journal of the lost zone is AtRisk-Unplaceable: it serves, it holds
its configuration, and the controller alerts once for the zone (`zone_live`), as the degraded
trap of architecture §5 says. It is re-placed only into a fourth zone, where the region has one. A heal that
replaces one acceptor can move survivors to other columns (the layout is a function of the
sorted members), so the controller judges the zone rule and the spares on the new layout, and
the move itself is safe (`config-register/heal_partitioned`, §J5.5).

#### Tests of the health measures and the decisions

The pure function `decide` of the controller takes the measures and returns at most one plan.
Its tests come with its crate, after #1189, and follow the plan of the design document:

1. Property tests of `decide`: every plan meets the zone rule on the new layout; no reshape
   inside the minimum interval; no plan from a latency, load or error signal (v1); no plan on
   stale signals or while a segment is not retired; the answers are monotone, deterministic and idempotent, with
   hysteresis.
2. A reference `decide` written again from this section, with no shared code, compared with
   `decide` on every small view (`P <= 3`, `F <= 2`, three or four zones).
3. A walk in `journal-sim` (§J1.1) where a controller step is a walk move, with two controllers of
   different terms, trace-checked against `specs/tla/grid-controller/` (#1261 (grid controller
   decision tests)).
4. A moonpool-sim scenario library: loads (ramp, step, spike, diurnal, a sawtooth at the
   controller's own period, a load that sits on a threshold, one hot journal among many) and
   faults (kills during a seal, a retire and an offload, a matchmaker loss during a reshape, a
   controller crash between plan and call, a gray acceptor, a flapping acceptor, a zone kill).
5. Oracles, each with a planted mutant: `NO_UNSAFE_PLAN`, `BOUNDED_RESHAPE_RATE`,
   `NO_OSCILLATION`, `CONVERGES`, `COST_BOUND`, `FENCED`, `NO_LOAD_PLAN` (no plan follows a
   change of load alone), and one that recomputes
   each P measure from the walk's own state, with no shared code, and compares.
6. A `probe!` on every branch and every refusal reason of `decide`; the coverage gate requires
   all of them, with no excused probe (the absolute rule of `AGENTS.md`).

#### The decision as built

`crates/lib/paros-journal-control` (#1328 (paros-journal-control), #1261 (grid controller
decision tests)) builds `decide` from this chapter. Its view is a done read of the register, the
liveness of each member (`up`, `suspect`, `down`), the zones, the candidates that the host's
placement offers, the leader, the matchmakers, the scrub's pending repairs, `data_lost`, the ages
of the signals and of the oldest seal, the time since the last reshape, and whether the rate
limiter has a token. The numbers that are **Open** above stay the host's: `Tunables` (the stale
age, the minimum interval, the stall window) has no default. The view has no load, latency or
error signal, so `NO_LOAD_PLAN` holds by the type. `decide` gives at most one action and every
alert, in this order:

1. A controller whose term is not the register's stops and reports nothing (§J6.1).
2. The alerts: `data_lost`; the matchmakers below a majority (`ControlFrozen`, a page) or at a
   majority with no spare; the journal states of the table above that hold (Unplaceable,
   WritesDegraded, ElectionBlocked and Stopped; the last two page); a seal older than the stall
   window (`ReshapeStalled`).
3. No action while the matchmakers are frozen or a signal is stale.
4. The cheap action first: the repair of a found damage, with no token.
5. The re-replication of the first sealed or retired segment that has a replica down, under a
   token.
6. The heal reshape of the segment in force, when the state is AtRisk and a member is down, or a
   replica of its set is down: it waits for the retire of every older segment, the minimum
   interval and a token. The new grid keeps every member that is not down in its place and puts
   a candidate in the place of each member that is down, the first placement in the host's
   order that meets the zone rule; the replica set the same, with no zone rule.

The tests as built: the unit tests of the crate; tests 1, 2, 5 (the oracles that one decision can
judge: `NO_UNSAFE_PLAN`, `BOUNDED_RESHAPE_RATE`, `FENCED`, `NO_STALE_PLAN`, `NO_SUSPECT_REPLACED`,
`IDEMPOTENT`, `MONOTONE` and `SAME_AS_REFERENCE`, each with a planted mutant) and 6 run in
`journal-sim`'s `tests/control.rs` over every small view (`P <= 3`, `F <= 2`, no zones or three or
four zones: about 390 thousand views); `scripts/mutants.sh --control` leaves no survivor.

The controller role as built (`role.rs`): a `Controller` holds its standing (idle, campaigning,
or holding a term) and the reshape call in flight, and nothing else, so a restart is a new
`Controller` (`grid-controller/`: a crash forgets the term and the plan). It runs no register
operation. It gives the driver the change to run (`Elect`, or the `Reshape` of its call) and
takes back the outcome (done, refused, or no answer). It gives the call in flight again until the
call is answered, and plans nothing else meanwhile; a refusal for the term, or a view of another
term, makes it idle. The driver decides when to send a call again: after a timeout, it reports
"no answer" and sends the same call, which the register answers "ok" if it already took it.

Test 3 runs in `journal-sim`'s controller walk (`src/controller.rs`, `tests/controller.rs`):
controllers that race, crash between plan and call, and get deposed; lost requests and replies;
acceptors that fail (and recover, in the wide shape); views that read the register late; the
journal's seal and retire. Its oracles share no code with the controller: `FENCED`,
`BOUNDED_RESHAPE_RATE`, `COST_BOUND` (a reshape replaces only members that failed),
`NO_OSCILLATION` (a removed member comes back only after it recovered), `BOUNDED_IN_FLIGHT` and
`CONVERGES` (once the faults stop, each journal has one live controller and its newest segment in
force on live members only, every older one retired). Each oracle catches its planted mutant of
the driver. The many-journals shape of #1261 has four journals whose grids share one machine,
which dies, and a token with a cap of one for all of them. The walks reach every `probe!` of the
role. TLC checks the traced walks against `ControllerTrace.tla`, which extends
`GridController.tla` (`tests/controller_traces.rs`), refuses the control `ElectNoCas` (a campaign
with no compare, which the oracles pass) and refuses every edit of the recorded traces at or
after the edited event. The model has the step `Stand` for this: a holder with no plan that reads
another term stops, as `decide` does first. Test 4 (the moonpool-sim scenario library) waits for
the driver (#1329, #1330).

**Proposals of the build, for Pierre's review** (each is a reading of this chapter, not a
decision of Pierre):

- **A suspect member is not replaced.** It counts as lost for the spares (the catalogue), and a
  heal replaces only members that are `down`. A column at risk only through suspects waits.
- **The minimum interval holds every reshape, a heal at spare 0 too** (item 1 of the tests above
  says "no reshape inside the minimum interval"; the first text of #1261 said "unless the spare
  is 0", which in v1 would leave the interval no effect, since a heal runs only at spare 0).
- **A replica down in the segment in force heals by a reshape**; the re-replication of §J3.1 is
  for sealed and retired segments (v1 has no offload, so a retired segment's replicas hold its
  only copy).
- **A heal does not move survivors**: a member that is down is replaced in its own place. The
  zone rule of a v1 grid is the rule of each column.
- **The matchmakers at a majority with no spare alert**; the handover of the catalogue is not an
  action of v1's `decide` yet.

### J6.3 The placement port

**The grid controller asks the cell where; the cell answers through a placement port**
(Pierre, 2026-11-22, #1421 (the placement port); `docs/architecture/04-roles.md` §4.4, "The cut
of the old tenant coordinator"). The loop that changes the membership of one journal (ask, reshape,
release) is the grid controller's. Which member must leave, where a new member goes and how many
moves run at one time are questions of the whole cell. `paros-journal-control` defines the port
as a trait with plain data; `paros-cell` answers it. The journal layer names no cell type. A
program with no cell answers it from a fixed list of machines.

**The calls** (Pierre, 2026-11-22, decision card of #1421: "place, release, standing"):

- `standing(machine)`: whether the machine must lose its members (it is gone, it drains, it is
  slow, it is held for maintenance). "Gone" and "drains" are durable records of the cell, not the
  memory of one coordinator.
- `place(request id, journal, role, keep, leave, count, needs disk)`: one grant, all or none.
  The caller draws the request id. The cell writes the grant and its bookings in **one** write of
  its control journal, so the id is in the fold before any machine is booked. The same request id
  gets the same answer. `busy` (the limit of moves in flight is reached) writes nothing, and the
  caller asks again with the same id. `refused` (no machine fits) is recorded under the id, and
  the caller asks later with a new id.
- `release(request id, members)`: the move is over. The cell ends the grant and frees its
  bookings of the machines that are not members of the journal now. After a reshape, these are
  the members that left. After an **abandon**, they are the new machines: a controller whose
  journal is below a majority cannot write a reshape, so it holds and pages, and it releases the
  grant at once. A grant in flight is the token of the rate limit of the cell (the old chain of
  steps of `turn.rs`), so every grant must end: a journal below a majority that kept its grant
  would stop every move of the cell, the heal of the cell's own control journal too. The same
  release again changes nothing.

**A refusal is a true reason.** The controller asks again, with a new request id, only once the
records of the cell (gone, draining, the bookings) have changed. Until then the journal reports
the refusal.

**The cell places its own control journal through the same port** (Pierre, 2026-11-22, decision
card of #1421: "The cell answers itself"). The grid controller of the cell's control journal asks
the port of its own cell, and the grant is a write to that same journal. The journal stays
writable while at most `F` of its members are down, so the heal goes on. Below a majority, the
cell writes no grant, so no journal of the cell heals: hold and page (§J6.2, `ControlFrozen`).

`specs/tla/placement-port/` checks the port. Safety: one request id is one grant
(`OneGrantPerRid`); a journal keeps its size (`KeepsSize`: a member leaves only in the reshape
that puts its replacement in, which is the real all-or-none check; `GrantWhole` states it of the
grant); the grants in flight and the journals that reshape on a grant never pass the limit
(`InFlight`, `MovesInFlight`); a machine holds at most its roles (`Capacity`); each member has its
booking (`MembersBooked`); no booking leaks, after an abandon too (`NoOrphan`); a released booking
is free again (`ReleasedFree`); every write of the cell and every reshape waits for a majority
(`Writes`). Liveness (`Healed`): once the faults stop, while the cell has a majority, every grant
ends and every journal is off the machines that failed or drain, or reports a true reason (it is
below a majority, or the cell refused it and nothing changed since). Seven rules have a control: the
bookings in one write and the grant in a later write, with a restart of the coordinator between
(`book_first`); no look-up of the request id (`no_dedup`); a partial grant (`partial`); no limit
(`no_limit`); a release that keeps the bookings (`no_free`); a cell control journal that does not
use the port (`fixed`); a controller that keeps the grant of a journal below a majority
(`no_abandon`). The restart of the coordinator matters only in `book_first`: the rule keeps no
grant in the coordinator's memory.

**What the model leaves out.** It replaces members at the same size: `count` is the number of
members that leave. **Open** (#1421): a grow or a shrink of a control journal (row C15 of §4.5)
through `place` with another `count`. The model has the reasons gone and drain of `standing`;
slow, maintenance and the balance of `paros-cell` (#1423) give the same "must leave" answer.
`place` answers a repeated request from the fold with no majority: a grant in the fold is durable
and never changes its answer. **Open** (#1421): the order of the grants (the rule of turns of
`turn.rs`), so that one journal never starves another (the model has a limit of one move for the
whole cell, and its fairness covers the requests together, not each one), and whether the cell's
own control journal skips the limit.
