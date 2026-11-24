[← 06-controller.md](06-controller.md) · [Index](README.md) · [08-build-steps.md →](08-build-steps.md)

## J7. Verification

**No code before its model** (#1323 (build the paros journal v1)). Each journal rule that code
builds has a TLA+ check in `specs/tla/`: a check that passes with the rule and a control that
fails without it (`AGENTS.md`, "When in doubt about safety, use TLA+"). The journal then has the
same three judges as the consensus core, on the same code: model-based trace checking,
deterministic simulation testing and coverage-driven simulation. The verification of the whole
service (rule 8, the quiescence oracle, the checks beside the simulation) is architecture §6.

### J7.1 The model of each rule

`specs/tla/README.md` is the index of every model, with how to run it, its checks and its
controls. This table maps each journal rule to its model. "Open" models are not merged.

| Rule | Section | Model | Issue |
|---|---|---|---|
| The flight rule of the sans-IO contract | §J4.2 | `flight-rule/` | #1255 (journal sans-IO contract) |
| Held writes: `Unknown`, never `NotLeader` (`AckedChosen`) | §J2.2 | `compartments/` | #1232 (held writes) |
| Multi-writer mode through a `SetLeader` to a tagged mode (`Leader::Multi(tag)`, #1495), position before fence, the resend across a seal (`Below`) | §J2.2, §J2.4.1 | `multi-writer/` | #1274 (multi-writer mode), #1301 (a resent write across a seal) |
| The `Resolve` call that settles `Below` | §J2.2 | `journal-state/` | #1320 (the wire shape that resolves a `Below` answer) |
| Leaderless reads: the read fence on the promise and the belief ballot | §J2.5 | `reads/` | #1016 (model of leaderless reads), #1099 (rework 1.10), #1228 (stale read by a lagging reader) |
| The row seal of a full grid (not a v1 grid) | §J2.5 | `reads/` (`grid_full_seal`, `grid_full_seal_others`) | #1102 (rework 1.13), #1209 (seal each row of a full grid) |
| Truncation on plain Multi-Paxos: the floor, the trim-point jump | §J2.6 | `truncation/` | #1017 (truncation model), #1216 (truncation model, part 2) |
| Segments in the matchmakers, the seal end, the gap fill | §J3.1 | `experiments/segments/a/` | #1213 (segments in the protocol or in storage), #1233 (segments gap fill) |
| A write reaches only the replica set of its segment; retire at `F + 1` holders | §J3.1 | `segment-replicas/` | #1271 (a write reaches only the replica set of its segment) |
| The whole boundary state in the segment record; two seals in a row | §J3.1 | `seal-chain/` | #1303 (two seals in a row) |
| A user truncate across segments; the acceptors' floor for each segment | §J3.2 | `segment-truncate/` | #1276 (user truncate across segments); #1318 (`acc_grid`); #1321 (make `crash_all` and `live_all` finish) |
| A leaderless read of the tail across a reshape | §J3.3 | `experiments/segments/reads/` | #1254 (leaderless reads across a reshape) |
| Segment records across a matchmaker set handover; act on a write seen done | §J3.4 | `segment-records/` | #1275 (segment records across a matchmaker handover) |
| The register as a compare-and-set at a majority of one generation | §J3.4 | `cas-register/`, `handover/` | PR #1064 (gone-node gap), #1097 (rework 1.8) |
| The offloader and the races of offload (after v1) | §J3.1 | `offload/` | #1256 (offload races and the offloader role) |
| Quorum everywhere and the grid bound | §J5.1 | `quorum-everywhere/`, `grid/` | #1257 (quorum everywhere); #1318 (the rows of the boundary write and the acceptor compaction) |
| CTRL on a grid and across segments | §J5.3 | `ctrl-grid/`, `ctrl-segments/` | #1263 (CTRL recovery in the journal) |
| The seal end against a faulty record: a faulty vote counts | §J3.1, §J5.3 | `seal-ctrl/` | #1313 (the seal end against a CTRL faulty record) |
| CTRL in the journal roles: a damaged vote, the tri-state tally of the leader, the probe, the learn from a replica and the repair at the higher ballot | §J5.3 | `journal-ctrl/` | #1326 (the journal roles over the core) |
| Re-replication of a sealed segment | §J3.1, §J5.2 | `re-replication/`, `heal-reuse/` (a node asked again, late requests, a node's own view), its row in `quorum-everywhere/` | #1314 (re-replication of a sealed segment's replica set) |
| The rules composed across three segments, with the acceptors' disk and the write orders | §J7.2 | `journal-state/` | #1189 (the journal state model), #1309 (the state is the disk) |
| A column below quorum, the bypass and its hold (after v1) | §J5.5 | `degraded-reshape/`, `zero-stall/` | #1258 (reshape with a column below quorum), #1018 (zero-stall model, more rules) |
| The controller's election and fenced reshape | §J6.1 | `grid-controller/` | #1259 (the fenced reshape call of the grid controller) |
| The placement port between the grid controller and the cell, the cell's own control journal too | §J6.3 | `placement-port/` | #1421 (the placement port) |
| The health measures and the journal states | §J6.2 | `health/` (v1: its `v1` and `v1_draft` checks, #1319 (a v1 switch in health/)) | #1260 (journal health measures) |

### J7.2 The composed model

**#1189 (the journal state model) composes the rules above** into one model: at least three
segments, the rules of chapters J2 to J6 with the control of each, and reach checks for the
seams between them (the consolidated v1 plan, section 8). Its state is the disk, and its
messages come from the disk (#1309 (the state is the disk)). It is step 2 of the build
(section J8), after this chapter and before any journal code.

**The model is `specs/tla/journal-state/`** (its section in `specs/tla/README.md` lists every
check). Three segments, Paxos inside each one, the seal by the first leader of the next grid,
register writes that are partial and can be lost, the boundary state, a replica set for each
segment, the journal fold with `Below`, retire, trim and the reshape guard. Each acceptor keeps
a memory, a queue of writes and a disk, and can crash between two writes. The CI set takes about
13 minutes. Every check gives its expected result, and no decision of these files is refuted.
What it decided:

- **The leader of a fresh journal** stays unset; `None` is safe too (§J2.4.1).
- **The writer of the replica set** is the reshape call, with the grid (§J3.4).
- **The writer of `trimmed`** is a replica whose fold reached the end of the segment (§J3.4).
- **The boundary write** may be acted on while partial; the seal end only once done (§J3.4,
  control `end_read`).
- **The write orders of an acceptor** (§J4.2, #1309): a promise and a vote are sent only once
  their write is durable (controls `reply_mem1b`, `reply_mem2b`), and an `Accept` that raises
  the promise writes the promise before the vote or in the same write. The vote first is unsafe
  (control `order_vote_first`: after a crash, the acceptor takes a lower ballot over a chosen
  vote).

- **The resolve of `Below`** (§J2.2, #1320 (the wire shape that resolves a `Below` answer),
  Pierre's option (a)): a `Resolve(uuid, expected_seq, write)` that a replica of the old segment
  judges with the compare of apply, once its fold covers the position (controls `resolve_uuid`,
  `resolve_early`).

The register role lives in `paros-journal`, by composition (#1325, §J3.4).

### J7.3 `journal-sim`

**The journal gets its own walk crate**, `crates/sim/journal-sim`, beside `consensus-sim`
(Pierre, 2026-11-20). `consensus-sim` stays on `paros-consensus` only. `journal-sim` is built
with the steps of the journal core and the roles, in the same pull requests (#1327 (journal-sim)):

- Seeded random walks (moonpool-walk) over the journal's pure roles, with faults on: crashes that
  lose unsynced writes, disk rot inside the contract, message loss and reorder, leader changes,
  reshapes, seals, matchmaker handovers, retire, truncate, re-replication, and two controllers of
  different terms (#1261 (grid controller decision tests)).
- Safety oracles that share no code with the journal, and a planted mutant that each oracle
  catches.
- The trace check of the walks against #1189 with TLC (moonpool-tla); each trace spec names the
  steps that its checked walks must take.
- A coverage gate over every journal probe, with no excused probe, inside the CI budget of 30
  minutes per job (shards if needed; Pierre, 2026-11-20).

**What it holds today** (#1324 (paros-journal)): the walk of the journal fold over a chain of
segments (`src/fold.rs`). Three writers write, resend across seals and under deposed uuids,
switch the journal to multi-writer and back, truncate, and settle each `Below` with `Resolve`;
the replica of the segment in force crashes and refolds from the boundary state. Four oracles
judge it against `Truth`, the fold of the whole log written again with no segment (`FOLD`,
`VIEW`, `RESOLVE` and `REFOLD`, the walk forms of `AckTrue`, `OneFold` and `ResolveTrue`), each
with its planted mutant. TLC checks each walk against `journal-state/FoldTrace.tla`, which
extends `Journal.tla` and binds each event to the model's fold operators; the checked walks meet
16 named rules, a planted `Below` in the segment in force is the control, and TLC refuses 24
named edits of the recorded walks. The probes and the coverage gate come with the roles (#1326).

**The walk of the roles** (#1326 (the journal roles over the core), `src/roles.rs`): the
acceptors, the leaders and the segment replicas of `paros_journal::roles`, with a disk each
(batches durable in id order, a crash that loses the batches in flight and the held sends), the
register as an atomic compare-and-set, three proposers that campaign, reshapes to new grids from
a pool of six acceptors, seals, boundary writes, retires, and the clients of the fold walk (one
call in flight each, a call answered `Unknown` sent again, `Below` settled with `Resolve`). Six
oracles judge it: one command chosen at each slot (`AGREE`), nothing chosen in a sealed grid at
or above its end (`SEALED`), each answer the whole log's (`FOLD`), each replica's fold the whole
log's at its front (`VIEW`), each boundary state the fold at the end (`BOUNDARY`) and each
`Resolve` the fate the whole log gives (`RESOLVE`). Each has its planted mutant in the driver:
an acceptor that loses its disk, a leader bound to a stale grid, a `Duplicate` answered `Taken`,
a replica that takes a stale base, a stale boundary write and a `Resolve` at the segment in
force. The narrow shape is the default grid (one column of three); the wide shape adds grids of
two columns and promises in pages of two. Each campaign reaches its named sites (a seal lost, a
`Noop` below the start, an old vote chosen again in a new grid, a third segment, a retire, a
pull from a peer, a leader stopped on a higher promise, `Unknown`, `Below`, `Resolve`, and
more). TLC checks the walks of the narrow shape against `journal-ctrl/RolesTrace.tla`, which
extends `Journal.tla` and binds each event to an action of the model with the parameters the
code chose (`Register`, `Promise`, `Accept`, `Flush`, `Crash`, `Phase1On`, `SealWrite`, `Open`,
`Propose`, `TakeBase`, `Apply`, `BoundWrite`, `Retire`, `ResolveBelow`). The checked walks meet
16 named rules (a lost seal, a `Noop` below and above the start, an old vote proposed again, a
promise after its Phase 1 closed, a crash that lost a queued write, a third segment, `Below`,
`Resolve`, and more). A boundary write before its batch is durable is the control: the oracles
pass it, TLC refuses the write. TLC refuses 27 named edits of the recorded walks. The trace check
found three steps of the code that the model did not have; each is now a rule of both: a leader
keeps the read set and the first slot of its campaign (`Register` takes them), the gap fill
writes a `Noop` above the start below the highest vote it saw, and the replica's boundary write
and its `Resolve` answer wait on the replica's batch. The wide shape (two columns) has no trace
check: `Journal.tla` chooses a slot at a quorum of the whole grid, not of one column.
The walk also runs proxy leaders (oracle `PROXY`, mutant `ForgedAccepted`), leaderless reads at
the replicas, one of which hears the register late (oracle `READ`, mutant `ReadNoFence`), the
trim of a retired segment and the reader's floor (oracles `TRIM` and `FLOOR`, mutants
`TrimIntent` and `FloorFromHolder`; the trace events `Trim` and `Drop`), and the leader time of each write
(oracle `STAMPED`, mutant `UnstampedAccept`). It damages a vote of an acceptor's disk at a boot,
inside the contract (a replica applied the slot, or the slot chose another command), so a leader
meets a blocked slot, decides it on a late answer or learns it from a replica, and repairs the
faulty votes (oracle `REPAIRED`: a repaired vote holds the chosen command at or above the ballot
that chose it; mutant `RepairForged`; the trace events `rot`, `probe`, `learn` and `repair`, bound
to `Rot`, `ProbeDecide`, `ProbeSkip`, `LearnChosen` and `AccRepair` of `journal-ctrl/`).

**The coverage gate of the roles** (#1326, `tests/roles_coverage.rs`): the roles walks in both
shapes, branched from the runs that reach a new site, and the walk of the whole journal reach
every `probe!` of `paros_journal::roles` (`REACHABLE` and `SOMETIMES` of `roles.rs`, with no
excused probe; a unit test checks that the lists name each probe of the sources once). It runs
in release, in the `trace-edits` job. Steers bring the walks to the rare branches: a rot that
damages a vote above a repair that the network holds late, or a vote that no quorum can choose;
a second campaign while a first leader waits on its seal; a campaign that the dropper of a
damaged slot answers late, while the other members of the column keep the slot (the late answer
skips the blocked slot); a seal whose answer is lost, asked again only once a later leader
opened in a later segment (so a later seal of the first leader's own segment stops it, §J3.4,
"a change asked again"). The checked walks of the narrow shape also meet the rules "a late answer
that dropped a blocked slot skipped it", "a late answer decided a blocked slot with a `Noop`" and
"an acceptor answered a `Prepare` again at its promise".

**The heal walk** (#1314 (re-replication of a sealed segment's replica set), `src/heal.rs`):
the controller role heals a retired segment while replica machines are lost for good or come
back with their disk, two controllers depose each other, requests and replies are lost and the
segment is trimmed, and requests are duplicated and arrive late. The walk plays the replicas
itself (the `SegmentReplica` of a heal has unit tests, and the whole-journal walk runs it on the
`Host`): a copy moves only while a member of the set it read is up and full, a node runs the
highest copy it was asked, and a replica in no set with no copy drops it. Three oracles judge
it: `HELD` (every member of the set of a segment that is not trimmed holds a complete copy),
`COMPARED` (a heal writes its set only if no set change came since its plan) and `CONVERGES`
(once the faults stop, the set is all up, or the segment is trimmed and no live machine keeps
it). Their mutants: a copy reported when it starts (`EarlyReport`), a lower copy taken
(`AnyCopy`), a heal written with no compare (`NoCompare`), with no term check (`NoTermCheck`) or
with no count of heals (`NoHealCount`), and a compare-and-set never sent again (`NoResend`). The
roles walk's retire is the leader's now, judged by `RETIRE` (a retire counts `F + 1` replicas
that hold the whole segment on their disk; mutant `HoldsLie`). `specs/tla/heal-reuse/` checks the five rules of the code that these mutants
remove and one more (highest copy, term check, heal count, a sent heal that ends only on an answer,
and a fresh register value before an abandon or a copy wipes a node), each with a control. The
heal walk plays its replicas on the current register value, so it cannot see the last rule. The
walk also ends orphan copies (a copy of a term that is over, on a node the register does not
name; sites `ORPHAN_ENDED`, `ORPHAN_REFUSED`), and `CONVERGES` catches the mutant that keeps them
(`KeepOrphans`). TLC checks the traced walks against `heal-reuse/HealTrace.tla`, which extends
`HealReuse.tla` and defines no step of its own (`tests/heal_traces.rs`): each event of the walk
(`HealEvent`) is one action of the model, the walks meet every rule of its `RULES`, TLC refuses
the control `SameCopyAgain` (a node starts a copy again from empty, which the oracles pass) and
refuses each of 27 edits of the recorded traces at the edited event or after it.

**The walk of the register** (#1325 (the matchmaker register as an optional role),
`src/register.rs`) runs `paros_journal::register` over four machines: operations of every change
of §J3.4 and handovers of the matchmaker set, with crashes that lose what a member did not
persist and restarts from the disk. Two oracles judge it: `CARRIED` (each majority that serves the
generation of a done write, or a later one, returns it or a later value, and the done values never
move back) and `ACTED` (a retire of the old acceptors and a drop of a replica's copy act on a value
that every serving majority holds). Each has its planted mutant: a
member that replies before its write is durable (`SendBeforePersist`) and a caller that acts on
a read (`ActOnRead`). The walk has two shapes. The wide shape runs three callers, two handovers
and every change. The traced shape is small enough for TLC: TLC checks each of its walks against
`segment-records/RegisterTrace.tla`, which extends `SegmentRecords.tla` and binds each event to
an action of the model. The checked walks meet 9 named rules, a forged `Bootstrapped` reply is
the control (the oracles pass it, TLC refuses the decree before every bootstrap), and TLC refuses
23 named edits of the recorded walks.

**The live trace check** (#1310 (check long runs live), `tests/live.rs`). A long run writes more
events than one TLC run can follow (one behaviour holds at most 65,535 states), and a check after
the run sees a divergence only when the run ends. moonpool-tla's `Live` checks the events while
the run writes them, in chunks on a thread of its own: each chunk starts from every model state
at which the chunk before it ended, so the chunks check what one TLC run over the whole trace
would check, with no limit on the length. The first chunk that no behaviour matches stops the
check, and the run stops with it. The fold walk has an events port for it
(`FoldModel::build_reported`, the walk's form of `Events`, rule 5 of §J4.2): it reports its fresh
journal, then each event as it records it. Its long shape (`Shape::LONG`) seals up to 200
segments. In CI one long walk (about 7,000 events, 199 seals) is checked live, and the trace
control (`BelowInSegment`) is stopped at its first wrong answer, before the walk ends. Each night
the `Journal live` workflow checks one fold walk with no end live for four hours.

### J7.4 The whole journal under moonpool-sim

A moonpool-sim of the whole journal runs the driver, the adapter, many journals in one process,
disk faults inside the contract, machine and zone loss, the controller and clients that retry
(#1330 (paros-journal-moonpool), now part of #1417 (the moonpool adapter on the generic host)).
The moonpool-sim version is not part of the journal v1: the adapter moved to the layered parosd
(#1409; Pierre, 2026-11-22). The v1 keeps the same scenarios with the same oracles as a
`journal-sim` walk of the whole journal on the driver and the in-memory backends of
`paros-journal-io` (#1445 (the whole journal on the in-memory IO)), so the move removes no check. Every input is recorded (§J4.2, rule 2), so a red run replays.
A contract suite certifies a backend's API only; crash safety is claimed only on a disk that can
inject faults (Pierre, 2026-11-19).

Its `Events` port feeds the live trace check of §J7.3, so a run of many hours is judged against
#1189 while it runs (#1310 (check long runs live)). The driver reports each write before it makes
it (announce, make, confirm): after a kill, only the last announced write is uncertain, and the
report at restart decides. A control tears a persist batch during a reconfiguration (as E3 of
etcd-raft, a lost commit index across a membership change), and the live check must refuse it.
**Open**: these three parts need the moonpool adapter, #1417 (after v1). The
driver and its `Events` port are built (#1329 (paros-journal-io), §J4.2): the driver reports each
step of the `Host` once its batches are settled. The announce, make and confirm of each write, the
live check fed by the port, and the torn-batch control land with the adapter.

**The driver walk** (#1329, `journal-sim/tests/host.rs`). Before the moonpool-sim, `journal-sim`
walks the generic driver itself: three processes, each a `Driver` with a disk that fails calls
inside the contract (a commit that fails `Full`, or `Unknown` after its write or with nothing
written; a scan that fails `Full` or `Unknown`; a list at a boot that fails `Full` and is asked
again; the list of the in-memory store must name each role that the disk's commits and losses wrote, and only those), a transport through the walk's mailbox (lost,
repeated, reordered), the journal's register on three matchmakers (one on each process, down
while its process is; a change can lose its request or its reply, reach one member only, or wait
in flight), crashes that keep the disk, restarts that read it back, campaigns that depose a
leader, and a writer that sends a call again. Each register fake keeps the register contract of
`paros-journal-io` (`contract::register`, run on it in `journal-sim/tests/backend.rs`). Its oracles share no code with the
driver: `AGREE` (no two `Commit`s on the wire name two commands at one slot), `FLIGHT` (an
`Accepted` or a `Promise` leaves only once the state it reports is on the disk) and `ANSWER`
(each answer is the whole log's at a slot of the command). Each has its planted mutant: a disk
that lies about a commit (`LyingDisk`), a process that loses its disk (`WipeDisk`), and a client
that takes a `Taken` as written (`TakenAsWritten`). The driver has no trace check yet.

**The client walk** (#1331 (the pure journal client), `journal-sim/tests/client.rs`). Two programs,
each with a `paros_journal::client::Client`, claim the journal with fresh uuids and depose each
other, make it multi-writer and back, write batches, truncate and read. Requests and answers go
through the walk's mailbox (lost, repeated, reordered); a command applies and loses its answer,
applies and is answered `Unknown`, or takes no slot (`Unknown`, `NotLeader`); the walk seals the
segment in force, so a resend is answered `Below` and settled by `Resolve`; a replica lags and a
read is unserved. The oracles read the whole log the walk keeps, never the client: `WRITTEN` (a
write reported written holds its positions), `ONCE` (a fenced write lands at most once),
`REFUSED` (a fenced write reported refused is never in the log), `LED` (a copy of a `SetLeader`
reported set applied), `TAIL` (the first copy of a new write of the uuid that leads names the tail)
and `PAGE` (a page holds the log's records). A late copy of an old command (a truncate one past the
tail, most often) reaches the journal after the client moved on, so a write of the leading uuid is
answered `Truncated` and the client must follow the tail. Each has its planted mutant in
the program or the journal, never in the client. A finding of the walk: with fresh uuids and one
call in flight, a `Resolve` that answers not written means that the writer was deposed, so the
client's resend at the tail is answered `Fenced` (site `RESOLVED_AGAIN`).

**The whole-journal walk** (#1445 (simulate the whole journal in memory),
`journal-sim/tests/whole.rs`). Before the moonpool-sim too, `journal-sim` runs the whole journal on
the in-memory IO of `paros-journal-io`: nine machines in three zones, two journals that share the
machines and one matchmaker register, a `Driver` and its `Host` on each machine (acceptors, leaders
and segment replicas of both journals), two controllers for each journal
(`paros_journal_control::role`) that are elected, heal a column, seal and get deposed, a reshape
token shared by the journals, a client that claims the writer, writes, sends again, resends with a
stale position and resolves each `Below`, crashes and restarts of a machine, the loss of a zone for
a while, register operations whose request or reply is lost, whose write reaches one matchmaker
only, or whose write waits in flight while other operations run and lands later, matchmakers that
are down with their machine (a read or a change that no majority answers gives no answer, and a
read repairs a write that a minority took), and disk faults inside the contract (the faults of
the driver walk's disk).
Its oracles share no code with the code they judge: `AGREE` (no two `Commit`s name two commands
at one slot), `SEALED` (no `Commit` of a sealed segment is at or above its end), `FLIGHT`,
`ANSWER` (each answer and each `Resolve` of each replica is the whole log's), `ONCE` (a write with
its `expected_seq` is in the log once), `ACKED` (each acked write is in the log at its position),
`ALERTS` (each alert of the controller is true of the machines), `DISK` (each replica's disk, on
every machine up or down, holds the chosen log: each slot it holds, its base and its compaction
point are the chosen log's, and its compaction floor is at or below the decided floor),
`STOPPED` (each leader that stops on its own has a true cause for the phase it stopped from: a
higher bound or a later segment for a bind, a higher promise in Phase 1, a seal of its own segment
for an open leader, another end of the segment before it or a newer segment for a sealing leader;
the facts
include the writes that a minority of the matchmakers took, since a role may act on one) and
`CONTROLLER_STOPPED` (each controller that stops, on a decision `Stop`, an abandoned heal or a
change refused on its term, was deposed: a value it read holds a later term or its term won by
another node, and a matchmaker held that term; or a change was refused on its term while a
matchmaker held such a term). Each has its planted mutant (`WholeMutant`; `RefuseSeal` for
`STOPPED`, `ForgedTerm` for `CONTROLLER_STOPPED`, `CorruptCopy` for `DISK`).
`STOPPED` exists because the walk heals what it judges: placement replaces a stopped leader, so a
leader that stops for a wrong reason (the bug of #1511 (a change asked again over its own write))
costs nothing that another oracle sees. Each walk of `journal-sim` also runs two seeds twice and
compares the logs (`tests/determinism.rs`). At the end of the walk each journal converges: a controller holds the term, the
newest segment is in force, the older ones are retired (by the leader, §J3.1), and every replica of the newest segment
holds the whole log. One replica node serves one replica for each segment of its journal, so a
heal keeps the live replicas of a column (Pierre, 2026-11-22, #1445); the address and the disk key
of a replica name its segment (§J4.2). A member of a retired segment that stays down long enough
is healed by re-replication (#1314): the walk asks the new replica's driver to copy and to drop an
abandoned copy (`Driver::join`, `Driver::abandon`, which read the register first, so the `Host`
judges each ask on a value done after it was sent), may lose an ask or give it again later
(site `LATE_ASK`), and gives each complete copy to the controllers (sites `REREPLICATE`,
`COPIED`, `REREPLICATED`). The oracle `HEAL_HELD` judges that each node a done heal added holds
every slot of the retired segment on its disk while the set names it (`DISK` judges the bytes) (mutant `EarlyCopied`: the
walk reports a copy complete when it asks it). The walk also loses one machine that hosts no
matchmaker for good (move `Lose`, site `LOST`): `CONVERGES` then needs the newest grid, its replica
set and every retired replica set to name no node of that machine (site `LOST_HEALED`). A walk
host gives the controller the free machines first and the machines that already serve a member
last, so a heal colocates two nodes on one machine only when no free machine meets the zone rule.

### J7.5 The controller's `decide`

Property tests, a second `decide` with no shared code, and the controller walks in
`journal-sim` (§J6.2, "Tests of the health measures and the decisions"; #1328 (paros-journal-control)).

### J7.6 Mutants

The `Mutants nightly` workflow runs cargo-mutants each night on the lines that main changed in
`paros-consensus` and `paros-journal` (`AGENTS.md`, "Build, test, gates"; #1324 (paros-journal)). For the journal core
the judge is its unit tests and the walks of `journal-sim`, then the trace checks of
`journal-sim` on the mutants that they miss (`scripts/mutants.sh --journal`). A surviving mutant
fails the night, as for the core. Each later journal crate joins `scripts/mutants-plan.sh` in the
pull request that creates it.

### J7.7 The absolute rule

**No exception in simulation** (`AGENTS.md`, the absolute rule, Pierre, 2026-11-19): no excuse,
no allow list, no skip, no weaker check, no retry. A failure of a walk is a finding: fix the
code, or fix the walk when it does a thing that the contract of the code does not permit. This
binds `journal-sim` and the moonpool-sim of the journal in full.
