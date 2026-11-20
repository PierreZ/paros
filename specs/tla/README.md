# TLA+ specs

Small TLA+ models of one protocol rule. A model is checked with TLC before the rule is written
in `paros-consensus`. A model does not replace the simulation: it searches a small abstract protocol
completely, and the simulation tests the code that ships.

## How to run

    nix develop .#tla --command scripts/check-tla.sh          # the checks that CI runs
    nix develop .#tla --command scripts/check-tla.sh --all    # the long checks too
    nix develop .#tla --command scripts/check-tla.sh reads    # one folder; CI runs one job for each folder

`tlc` is also in the default dev shell. CI runs the first command in the `TLA` workflow, only
when a spec, the script or the flake changes.

A long check that does not finish has a time bound in minutes as a fourth word of its line in
`checks.txt` (`partitions pass long 50`). TLC stops at the bound, and the line is `ok` when no
violation was found until then. A line with no bound must finish. Every bounded line is a
bounded run, never a proof: its table line in this file says so.

## Layout

Each folder is one model:

- `<Name>.tla`: the spec.
- `MC.tla`: the constants that a TLC configuration file cannot write (tuples, sets of sets).
- `<check>.cfg`: one TLC configuration.
- `checks.txt`: one line for each configuration, with the result that it must give (`pass` or
  `violation`) and where it runs (`ci` or `long`).

A check that must give `violation` is a control. It shows that the model can find the bug that
the rule prevents. A model with no control proves little.

A model may also have a trace spec, `<Name>Trace.tla` and `<Name>Trace.cfg`: the model, forced
to follow the events of one run of the code (#1198 (trace checking)). It is not in `checks.txt`:
`crates/sim/consensus-sim/tests/traces.rs` runs it with TLC, through moonpool-tla, on the walks
of `consensus-sim`. The trace specs today are:

- `decree/DecreeTrace`: the decree walk.
- `multi-paxos/MultiPaxosTrace`: the log of every cell flavor without matchmakers.
- `proxy-leader/ProxyLeaderTrace`: the proxy's `Commit` rule, on the flavors with proxy leaders.
- `leader-handoff/LeaderHandoffTrace`: the guards of the cooperative handoff, on the handoff
  flavor.
- `matchmaker/MatchmakerTrace`: matchmaking, Phase 1 over every configuration of `H_b`, Phase 2
  in `C_b` and the GC, on the flavor with matchmakers. The model decides one value; the trace
  spec takes its matchmaking and GC, and writes the log as its own steps.
- `reads/ReadsTrace`: `MatchmakerTrace` and the leaderless reads (the read basis of a won
  leadership, the read fence of each answer, the served index), on the lagging-reads flavor.
- `truncation/TruncationTrace`: `MultiPaxosTrace` and the compaction floor (the journal fold,
  each compaction against `first_seq`, the trim jump, the floor guards, a restart that holds
  the slots below its floor), on the `compaction` flavor (Multi-Paxos on five, steered toward
  compactions).
- `compartments/CompartmentsTrace`: `MultiPaxosTrace` and the read replica (its learns, its walk
  in slot order, its floors, its boot and its leaderless read), on the every-compartment flavor.
- `compartments/BatcherTrace`: the batcher and the unbatcher (each push fits the lane, a batch is
  the whole lane in push order or one write alone, the unbatcher cuts the range in batch order),
  on the batcher walk. The walk has no Paxos under the batcher, so this spec states the batcher
  rules of `Compartments.tla` and does not extend it.
- `truncation/FaultsTrace`: `TruncationTrace` and the faulty entries of CTRL (a boot that reads
  a record faulty, the pages of a promise with their faulty reports, the tri-state tally of the
  win, the repair probe), on the flavors `disk-faults` and `disk-faults-3`.
- `matchmaker/RetireTrace`: `MatchmakerTrace` and the retirement (a node retires on an effective
  floor that no registration at or above names, a retired node takes no step, and a rejoin
  books it again), on the `retiring` flavor.
- `matchmaker/MatchmakerGridTrace`: `MatchmakerTrace` with each configuration a grid (a win
  needs a Phase-1 quorum of every grid of `H_b`, a value is chosen in one column of `C_b`) and
  each proxy's `Commit` backed by a column of `C_b`, on the steered `reconfiguring` flavor
  (matchmakers and every compartment).
- `handover/HandoverTrace`: the matchmaker set handover over a chain of generations (a member
  freezes, a member of a proposed set holds its whole bootstrap, a member of the old set votes
  in the successor decree, a member of the chosen set activates with its bootstrap), with the
  registrations and the GC acks of one journal, on the `handover` flavor. It states its rules
  for many generations and does not extend `Handover.tla`, which hands generation 0 over once.

A trace spec that extends the trace spec of another model links that model's modules into its
folder, as `reads/` links `matchmaker/`.

Each trace spec has a control: a bug planted in the driver that TLC refuses
(`tests/traces.rs`). Each trace spec also names the steps that some checked walk must take
(`Spec::rules` in `crates/sim/consensus-sim/src/cluster/trace/rules.rs`, #1231 (traced walks
reach the rules)): a walk that never fills a gap leaves the gap-fill guard unjudged. A change
of `specs/tla/` alone runs the trace checks in the `traces` job of the `TLA` workflow, because
a trace spec `EXTENDS` its model.

A module that several models share lives in its own folder and is linked into each folder that
uses it by a symbolic link: `grid/Grid.tla`, the quorums of a grid, is linked into
`config-register/`, `reads/`, `multi-paxos/`, `leader-handoff/`, `compartments/`,
`proxy-leader/`, `quorum-everywhere/`, `degraded-reshape/` and `health/`.

## Experiments

`specs/tla/experiments/` holds models that compare candidate designs before a decision. They are
not models of the plan, and no rule of `paros-consensus` comes from them until Pierre decides.
Each experiment has one folder for each design, with the same layout as a model, and a
`README.md` with the report. `scripts/check-tla.sh` runs an experiment only when it is named
(`scripts/check-tla.sh experiments/segments/a`); CI runs a job for each experiment folder too.

- `experiments/segments/`: #1213 (segments in the protocol or in storage). Design A, the segment
  table in the matchmakers (`a/`), against design B, plain Matchmaker Paxos with chunks local to
  each replica and a variant with offload claims in the log (`b/`).
  `reads/` checks the leaderless read of the tail across a reshape on design A, on the
  partitioned grid and the full grid, for #1254 (leaderless reads across a reshape): the read
  fence on the promise passes (with the seal of each row of #1209 on a full grid), and the
  record check of the reader (rule 1) passes too. An acceptor of a retired segment that loses its
  state breaks the promise fence alone; the belief half of the fence with the refusal of an
  acceptor that did not hear closes it, and so does the record check. Pierre chose the fence of
  the code, with no record check (2026-11-20, `docs/architecture/02-data-plane.md` §2.10).
- `experiments/stale-stopped/`: #1229 (a stale `Stopped` reply reuses a decree ballot). The
  ballot of the handover's decree across a restart of the reconfigurer (`restart/`): the rule of
  the code today as the control, against a nonce of the incarnation in `Stop` and `Stopped`
  and a decree round on disk.

## The Paxos flavors and their models

Each Paxos flavor that `paros-consensus` builds has a TLA+ model, and a new flavor gets its model
before its code (decision 37 of the rework, Pierre 2026-11-18; the list is in #1185 (TLA+ model
multi-paxos)). The index of the models is #1020 (the models of the rework).

| Flavor | Code | Model | Step |
|---|---|---|---|
| Single-decree Paxos | `decree.rs` | `decree/` | #1186 (rework 1.15), done |
| Plain Multi-Paxos, with the majority, the partitioned grid and the full grid | `node.rs`, `proposer/`, `acceptor.rs`, `replica.rs`, `membership/` | `multi-paxos/` | #1185 (rework 1.14), done |
| The cooperative handoff of DPaxos | `node/handoff.rs` | `leader-handoff/` | #1187 (rework 2.8), done |
| Matchmaker Paxos | `matchmaking.rs`, `matchmaker.rs`, `collector.rs` | `matchmaker/` | #1200 (TLA+ model Matchmaker Paxos), done |
| The matchmaker set handover | `matchmaker/reconfigurer.rs` | `handover/`, which extends `handover-gc/`; `handover-chain/` for three generations and clients that reach one member at a time | #1097 (rework 1.8), done |
| Grid quorums | `membership/quorum_system.rs` | `grid/`, used by `config-register/`, `reads/`, `multi-paxos/`, `leader-handoff/`, `compartments/` and `proxy-leader/` | #1102 (rework 1.13), done |
| The proxy leader | `proxy_leader.rs` | `proxy-leader/` | #1188 (rework 2.9), done |
| The batcher, the replica and the read replica | `batcher.rs`, `replica.rs`, `replica_node.rs` | `compartments/` | #1190 (rework 6.10), done |
| Leaderless reads | `quorum_read.rs`, `node/quorum_reads.rs` | `reads/` | #1016 (model of leaderless reads), #1099 (rework 1.10), done |
| Phase-1 bypass of Matchmaker MultiPaxos (§3.6), with GC, the handoff and two bypasses in a row | not built (#579 (zero-stall reconfiguration) is at the tag) | `zero-stall/` | #1018 (zero-stall model, more rules), done |
| Truncation: the compaction floor as a fold of the journal, the trim-point jump, the retry of a laggard, the gap fill, and the faulty entries of CTRL with the repair probe, on plain Multi-Paxos | `acceptor/retention.rs`, `node/catch_up.rs`, `node/acceptor.rs`, `node.rs` (`compact`), `replica.rs` (`compaction_target`), `proposer.rs` (`slot_decidable`), `proposer/probe.rs` | `truncation/` | #1017 (truncation model), #1216 (truncation model, part 2), done |
| Truncation across a reconfiguration onto empty stores: the GC of the old configuration only once a Phase-2 quorum of the new one holds the prefix up to the election fence, a new member that gets its floor from `TrimmedTo` or catches up | `node/gc.rs`, `collector.rs` (`covered`), `node/learn.rs` (`mark_chosen`), `node/reconfigure.rs` | `truncation-reconfig/` | #1218 (truncation model, part 3), done |
| Quorum everywhere: every journal step (write, election, leaderless read, seal, retire, offload mark, trim, matchmaker handover, grid controller election, reshape call) waits for a quorum only | `membership/quorum_system.rs`, `quorum_read.rs`; the journal steps are designed, not built | `quorum-everywhere/` | #1257 (quorum everywhere), done |
| The grid controller's election by a term in the matchmaker record, and its fenced `reshape(expected configuration, new grid, term)` call | not built (`paros-journal-control`, after #1189) | `grid-controller/` | #1259 (the fenced reshape call of the grid controller), done |
| A reshape when a column of the grid is below quorum: the Phase-1 bypass, the hold, the seal end of a bypass, the read fence across it | not built (#579 (zero-stall reconfiguration) is at the tag) | `degraded-reshape/` | #1258 (reshape with a column below quorum), done |
| The health measures of the grid (the spares of a column, electable, writable, the hold of a bypass, the zone margin) against the quorums, and the journal states with their fixed actions | not built (`paros-journal-control`, after #1189) | `health/` | #1260 (journal health measures), done |
| Segment replica sets: a write reaches only the replica set of its segment; the boundary state of the seal (now the whole state in the segment record, `seal-chain/`; first checked as `next_seq` in the segment record, the uuid in the journal's matchmaker record and a `Fence` at the next segment's first slot); reads routed to the set of the segment; retire at `F + 1` holders | the journal steps are designed, not built | `segment-replicas/` | #1271 (a write reaches only the replica set of its segment), done |
| A user truncate across segments: the trim floor in the boundary state, the floor of a read from the segment in force, the floor check of an offloaded read, the trim of a sealed segment only once the floor is decided over it, the acceptors' compaction after `F + 1` replicas, the trim-point jump | the journal steps are designed, not built | `segment-truncate/` | #1276 (user truncate across segments), done |
| Multi-writer mode through `SetLeader(None)`: the switch in both directions, the fence at apply, the position judged before the fence so a resend resolves a held write, the writer's basis from the view, `Truncate` in both modes, a `Fence(None)` at a seal, a resend across a seal answered `below` (#1301 (a resent write across a seal)) | the journal steps are designed, not built | `multi-writer/` | #1274 (multi-writer mode through SetLeader(None) and expected_seq), done |
| Segment records across a matchmaker set handover: the seal, the retire, the offload mark, the controller election and the reshape call, each a compare-and-set on the matchmaker register while the set hands over | the journal steps are designed, not built | `segment-records/` | #1275 (segment records across a matchmaker handover), done |
| Offload of sealed segments: the offloader role (full copy, conditional put, compare-and-set mark), the late upload after trim, the lost mark, the duplicate upload, an offloader that stops between the put and the mark, the drop after the mark, the scrub of the controller | not built (the offloader and the controller, after #1189) | `offload/` | #1256 (offload races and the offloader role), done |
| Two seals in a row: a chain of segments with an empty segment and a seal before the `Fence`; where the boundary leader uuid lives (the segment record, or the journal metadata with a `Fence` at the next segment) | the journal steps are designed, not built | `seal-chain/` | #1303 (two seals in a row) |
| The journal state and its `SetLeader` fence | `journal/state.rs` | `journal-state/` (to write) | #1189 (rework 3.7) |
| The flight rule of the journal's sans-IO contract: persist ids, a send that names the persist id it waits on, group commit, a crash that drops the batches in flight (`docs/architecture/04-roles.md` §4.2) | not built (the journal core, step 3 of #69 (plan of record)) | `flight-rule/` | #1255 (journal sans-IO contract), done |
| CheckQuorum | `proposer/authority.rs` | `election` (to write) | #1136 (rework 6.7) |

`paros-consensus` has no leases and no flexible quorums (step 0.12 of the rework), so they need no
model.

## The long checks, run once for rework 1.6

Every `long` check of the tree ran once on 2026-11-17 (#1095 (rework 1.6)), with 4 workers.
Each gave the result that its `checks.txt` expects, except the two bounded runs at the end.

| Check | Result | Distinct states | Time |
|---|---|---|---|
| `cas-register/cas_long` | violation | 819 thousand | 32 s |
| `abandoned-ballot/owner_noguard` | violation | 9.9 million | 5 min |
| `reads/learner_strict` | pass | 4.2 million | 1.5 min |
| `reads/learner_any` | pass | 4.2 million | 2 min |
| `retire-fence/ask_quorum_tomb_weak` | pass | 3.7 million | 6 min |
| `retire-fence/ask_all_tomb_no_operator` | pass | 3.3 million | 5 min |
| `retire-fence/quorum_prune_safe` | pass | 3.7 million | 7 min |
| `retire-fence/quorum_prune_all` | pass | 3.7 million | 6 min |
| `retire-fence/rejoin_tagged` | pass | 4.6 million | 4 min |
| `retire-fence/crash_majority` | pass | 7.8 million | 16 min |
| `retire-fence/retired_abandon` | pass | 9.4 million | 15 min |
| `retire-fence/retired_abandon_live` | pass | 17.7 million | 53 min |
| `zero-stall/carry_check` | pass | 10.2 million | 6 min |
| `zero-stall/p212_check` | pass | 10.1 million | 8 min |
| `zero-stall/carry_paper` | pass | 32.4 million | 20 min |
| `zero-stall/p212_paper` | pass | 19.7 million | 14 min |
| `zero-stall/gc_p212_paper` | pass | 2.7 million | 3 min |
| `zero-stall/gc_carry_paper` | pass | 2.8 million | 3 min |
| `config-register/*` | see the table of the model | | |
| `config-register/grid_long`, `reads/grid_partitions` | no violation in two bounded runs of 2 hours (2026-11-19, #1102 (rework 1.13)); see the tables of the models | | |
| `abandoned-ballot/owner` | no violation in 89 million distinct states (bounded: the disk filled after 50 min) | 89 million | 50 min |

## Models

### `zero-stall/`: Phase-1 bypassing for #579 (zero-stall reconfiguration)

The model is Matchmaker MultiPaxos (Whittaker et al., §3 and §4) with matchmakers, one
configuration for each ballot, Phase 1 over every prior configuration, and a leader that moves
from its ballot `b` to its next ballot `b2` with no Phase 1 for the free slots.

Constants select the variant:

| Constant | Values |
|---|---|
| `Owner` | `OwnerPaper3`: the paper's rounds `(r, id, s)`, a proposer owns two consecutive ballots. `OwnerParos3`: paros's `(round, node)`, a ballot of another proposer is between. |
| `Check` | `TRUE`: bypass only if the matchmaker answers show no registration between `b` and `b2`. |
| `Mode` | `carry`: the leader proposes its own values again at `b2`, with no Phase 1. `p212`: frankenpaxos `Phase212`, the slots below the frontier get a Phase 1 at `b2`. `none`: no bypass. |
| `Gc` | `off`: no garbage collection. `chosen`: the leader of `b` asks the matchmakers to forget the ballots below `b` when each slot that it recovered or carried is chosen at `b` (the paper, §3.5) and, with `Hold`, when its Phase 1 is covered. `nofence`: a control, the leader of `b2` asks before its Phase 1 at `b2` (the fence Phase 1) ends. `early`: a control, the leader asks with no condition. |
| `CheckWm` | `TRUE`: bypass only if the highest watermark in the matchmaker answers is at most `b`. |
| `Reads` | `TRUE`: the leader acks the chosen slots, and the model has the leaderless reads of Compartmentalized Paxos (§3.4) with the rule of `quorum_read.rs`: a reader asks a quorum of the configuration of a leadership that won, abandons the read if an answer shows a higher ballot, and reads at the highest slot voted or at the fence. |
| `Hold` | `TRUE`: the decision hold. A leader acks no slot before a quorum of each prior configuration promised its ballot. |
| `ReadHold` | `TRUE`: the rule of #1270 (bypass read order). A read (`ReadReturn`) counts a slot in its chosen prefix only once the ballot that chose it is covered. `FALSE`: a control. |
| `Handoff` | `TRUE`: the cooperative handoff of DPaxos (`relinquish_to`). The node that minted a ballot hands it on one time, with its proposals and its open slots, while it leads at the ballot. The successor leads with no Phase 1. |
| `Relinquish` | `TRUE`: a node that handed its ballot on never bypasses from it (it drops its proactive matchmaking). `FALSE`: a control, the minter still bypasses after the handoff. |
| `SuccessorBypass` | `FALSE`: a successor never bypasses from a ballot that it did not mint. `TRUE`: a control, the successor bypasses to its own next ballot. |
| `HandoffCarry` | `TRUE`: the handoff carries the slots that wait for the fence Phase 1. `FALSE`: a control, the successor takes them as free. |
| `ChainRule` | `TRUE`: a leader bypasses again only after the fence Phase 1 of its last bypass (each slot it recovered or carried is proposed at its ballot). `FALSE`: a control, the slots that wait for the fence Phase 1 do not count. |
| `CfgChoices` | The configurations that the ballots can have: any (`CfgAny`), or a few fixed journal moves (`CfgMovePaper3`, `CfgWm3`, `CfgWm4`, `CfgChain3`, `CfgChain4`), which keep the GC, watermark and chain checks small. |
| `Restarts` | How many restarts a run has. A restart forgets what the node proposed (`known`); the votes and the registrations stay. |
| `RestartBypass` | `FALSE`: a restart ends every campaign and every leadership of the node, so only the incarnation that won `b` bypasses from `b`. `TRUE`: a control, the node still leads after the restart. |

The invariants are `Agreement` (one value is chosen for each slot), `ReadSafe` (no leaderless
read that starts now can miss a slot that is acked, or a slot that an earlier read returned) and `WmRedundant` (when the matchmaker answers
of `b2` show no registration strictly between `b` and `b2`, their highest watermark is at most `b`).

The model has three or four ballots, one or three matchmakers, two values, one or two slots and
two configurations of two acceptors.

Not in the model: matchmaker generations, lost durable state (a restart loses only volatile
state), the committed prefix and the floor of a bypass leadership (Scenario 3 of the paper, §4.5;
`truncation-reconfig/` has them for a reconfiguration with Phase 1), no liveness property, and a
handoff message that is late or sent two times (`leader-handoff/` has them, with its control `two_hops`). A reader is abstract
(a read can use the basis of any leadership that won); `reads/` has an explicit reader.

The rules of #1018 (zero-stall model, more rules), each with its check and its control:

| Rule | Check (`pass`) | Control (`violation`) |
|---|---|---|
| Bypass check, the registration half (rule 3) | `p212_check` | `p212_nocheck` |
| Bypass check, the watermark half (rule 3) | see the finding below | none: the registration half implies it |
| GC fence (I5): the leader at `b2` makes no floor effective before its fence Phase 1 ends | `gc_p212_paper`, `wm_nocheck_small` | `gc_nofence` (21 steps), `gc_early` |
| Handoff (8.2): one hop, the open slots carried, the minter that handed on never bypasses, the successor never bypasses | `handoff`, `handoff_paros` | `handoff_drop`, `relinquish_off`, `successor_bypass`; two hops: `leader-handoff/two_hops` |
| Chained bypass (8.3): a second bypass waits for the fence Phase 1 of the first | `chain_small`, `chain` (7.1 million states, 5 minutes, `long`) | `chain_open_small`, `chain_open` |
| A reader's chosen prefix waits for the fence Phase 1 of the ballot that chose the slot (#1270 (bypass read order)) | `read_carry_hold` (343 thousand states), `read_p212_hold`, and every check with `Reads` | `read_carry_no_read_hold`, `read_p212_no_read_hold`: `ReadSafe` breaks in 13 states (ballot 1 in `{a1, a2}` bypasses to ballot 3 in `{a3, a4}`, which chooses slot 1, and a read on ballot 3 returns it; a read on ballot 1 asks `{a1, a2}` and misses it) |
| Only the incarnation that won `b` bypasses from `b` (#1230 (rules no check tests)) | `restart_bypass` (489 thousand states) | `restart_bypass_off`: Agreement breaks in 16 states (propose at `b`, restart, bypass, the proposed slot is free at `b2`, a second value is chosen) |

Findings of #1018 (2026-11-19). No design decision is refuted.

- **The watermark half of the bypass check adds nothing to safety.** A matchmaker moves its
  watermark to `w` only for a leader at `w`, and a leader at `w` registered at a matchmaker quorum.
  That quorum meets the quorum of `b2` at a matchmaker that holds `w` (GC keeps the registrations
  at or above the watermark) or that refused `w`. So when the answers show no registration between
  `b` and `b2`, the highest watermark is at most `b` (`WmRedundant`). The registration half alone
  is safe with GC on (`wm_nocheck_small`, 1.8 million states, complete). On four ballots and three
  proposers, TLC found no violation in bounded runs (`wm_nocheck`: 34 million states in 25 minutes;
  `wm_redundant_small`: 26.7 million states in 9 minutes) and did not end. The check is cheap, so
  code can keep it as a defense; no control can fail for it.
- **A successor must not bypass.** `successor_bypass`: on the paper's ballots, the minter hands
  ballot 1 on, the successor bypasses to its own ballot 3, and ballot 2 of the minter is between.
  The paper's ballots have no check, so ballot 2 chooses a value that ballot 3 does not see. Only
  the minter that still holds a ballot bypasses from it.
- **The handoff carries the open slots.** `handoff_drop`: a successor that takes the slots that
  wait for the fence Phase 1 as free proposes a second value for a slot that the minter chose at
  the old ballot.
- **A minter that handed on drops its proactive matchmaking.** `relinquish_off`: the minter bypasses
  with what it knew at the handoff, and misses a value that the successor chose after it.
- **GC waits for the fence Phase 1.** `gc_nofence`: the leader at `b2` collects the old ballots
  while a slot that it carried is still open; a rival at `b3` then runs Phase 1 only on the new
  configuration and chooses a second value.

The rules together and the larger shapes (#1230 (rules no check tests)), all `pass` and complete:

| Check | What it adds | States |
|---|---|---|
| `chain_gc_reads`, `chain_gc_reads_carry` | two bypasses in a row with GC, reads and the hold, `p212` and `carry`, one matchmaker | 41 thousand, 113 thousand |
| `handoff_gc_reads` | a handoff with GC, reads and the hold, one matchmaker | 149 thousand |
| `owner_paros` (`long`) | two proposers that can both bypass (`OwnerParos`: 1 to 3, 2 to 4), GC, reads and the hold | 1.1 million |
| `chain_two_slots` | two slots: two bypasses, a carried slot and a free slot in one leadership | 366 thousand |
| `gc_two_slots` (`long`) | two slots with GC, reads and the hold: a read fence above a non-zero fence, a GC with one carried and one free slot | 2.2 million, 3 minutes |
| `handoff_two_slots` (`long`) | two slots: a handoff that carries one pending and one free slot | 6.2 million, 8 minutes |

Liveness (#1230 (rules no check tests)), with `LiveSpec`: every step of a campaign, a leadership
and an ack is fair; a handoff, a GC request and a restart are not. `AckEnds`: each slot that a
leader proposed is acked to its client in the end. The shape is a journal move by a bypass:
one proposer, ballot 1 on `{a1, a2}`, ballot 2 on `{a3, a4}`, two slots, `p212`, the decision
hold.

| Check | Shape | Result |
|---|---|---|
| `live` | the move, every acceptor stays | `AckEnds` pass (2.8 thousand states) |
| `live_old_gone` | `a1`, an old member, takes part in ballot 1 only (`Gone`) | `AckEnds` violation (341 states) |

The trace of `live_old_gone`: ballot 1 proposes slot 1, and only `a1` votes. Ballot 2 bypasses:
slot 1 waits for the fence Phase 1, and slot 2 is free. `{a3, a4}` choose slot 2 at ballot 2.
The fence Phase 1 needs a Phase-1 quorum of `{a1, a2}` at ballot 2, and `a1` is gone, so slot 1
waits for good, and the decision hold keeps slot 2 from its ack. A bypass thus needs a Phase-1
quorum of the old configuration after the switch, as Phase 1 of an ordinary campaign does.

- **With a grid column below quorum, the decision hold never acks** (#1258 (reshape with a
  column below quorum), `degraded-reshape/`): it waits for a Phase-1 quorum of the old grid,
  which no longer exists. The hold that gates only the acks and the read basis also misorders
  two reads on the old basis. A hold before Phase 2 on `F + 1` of one old column, or the
  segment-record check, closes both and stays live. GC with no hold keeps `Agreement`
  (`gc_carry_nohold`).

Results are in `zero-stall/checks.txt` and in the design document of #579 (zero-stall
reconfiguration).

### `retire-fence/`: when a removed node may shut down, for #1002 (retire fence gaps)

The model is the control side of Matchmaker Paxos: campaigns that register a belief,
reconfigurations, the GC watermark of each matchmaker, and the retirement of a node. It has no
values and no votes. It answers one question: after a node retires, does each later campaign
still get a Phase-1 quorum of each configuration that the matchmakers give it?

Constants select the rule:

| Constant | Values |
|---|---|
| `Fence` | `none`: the fence `last_member_ballot` moves only when the node hears a configuration that names it. `volatile`: a campaign also raises the fence of the node that registers, and a restart loses it (#893 (retire fence)). `durable`: the fence survives a restart. |
| `Ask` | `none`: the node retires on the five conditions of `may_retire`. `quorum`: it first asks a matchmaker quorum, and a matchmaker says yes if it holds no registration above the watermark that names the node. `quorumTomb`: the same, and a matchmaker that said yes refuses each later registration that names the node. `allTomb`: as `quorumTomb`, but every matchmaker must say yes. `quorumPrune`: as `quorumTomb`, and a candidate drops a registration from its answers when a matchmaker quorum in those answers recorded, below that registration, the retirement of a node that it names. |
| `OperatorRule` | `TRUE`: the operator retires no node that a reconfiguration above the watermark names, and names no retired node in a reconfiguration. |
| `Phase1All` | `TRUE`: a Phase-1 quorum is every member (`q1 = n`). `FALSE`: a majority. |
| `Crashes` | How many nodes may crash for good. A crashed node does not campaign, answer or run the retire check. A crash stays inside what the configurations are sized for: each configuration the journal still needs (`Needed`: the one in force at the highest effective floor, each that led above it, each reconfiguration above it) keeps a live Phase-1 quorum. `0` in every check but the crash checks. |

The invariants are `Electable` (each matchmaker quorum gives only configurations that still have
a Phase-1 quorum) and `SomeElectable` (at least one matchmaker quorum does).

The model has three nodes, a shrink from three members to two with `q1 = n`, three matchmakers,
two ballots and one restart.

Results (`retire-fence/checks.txt`):

| Rule | `Electable` | `SomeElectable` |
|---|---|---|
| No fence, the #893 fence, a durable fence | violation | |
| Durable fence and a quorum ask (the plan of #1002 on 2026-11-11) | violation | violation |
| Durable fence, a quorum ask, and the matchmakers refuse later registrations | violation | pass |
| Durable fence, every matchmaker answers and refuses later registrations | pass | |

The quorum rule with pruning (`quorumPrune`, added on 2026-11-12 to answer "what if a matchmaker is
not accessible?"):

| Check | Result |
|---|---|
| `PruneSafe`: a candidate drops only a registration whose campaign never completed its matchmaking phase | pass |
| `ElectableAll`: with an answer of every matchmaker, no campaign is stranded | pass |
| `ElectableOneDown`: the same with one matchmaker down | violation |

So the two rules that are safe differ in what waits for a matchmaker that is down. With
`allTomb`, a retirement waits. With `quorumPrune`, an election can wait: a candidate needs the
answers of the matchmakers that recorded the retirement, and one of them can be the one that is
down. An election is on the availability path and a retirement is not, thus `allTomb` is the
rule to ship. In the `quorumPrune` checks, a campaign in the model does not win by pruning: the
invariants judge what a candidate would get in each state.

The quorum ask fails because a registration can arrive after the answer. A slow campaign of a
member that still believes the old configuration registers it at a matchmaker after that
matchmaker said yes. The last row also passes with `OperatorRule = FALSE`: the matchmakers then
check the operator's half of the retirement contract.

The rejoin (added on 2026-11-12 for #1037 (rejoin)). Placement can book a retired node again,
and every matchmaker then refuses each configuration that names it. The `Rejoin` constant
selects how the node comes back; `Comebacks` bounds how often, and `Kinds` fixes which ballot
is a reconfiguration so the state space stays small (two matchmakers, three ballots).

| `Rejoin` | Rule | Check | Result |
|---|---|---|---|
| `off` (reach) | no rejoin: a booked-back node stays refused | `NoRenamed`, a reachability control | violation |
| `ignored` | the node comes back, and the matchmakers keep the record | `Nameable` | violation |
| `early` | the node is named before every matchmaker took the rejoin | `Electable` | violation |
| `untagged` | a rejoin drops the record; a check carries no incarnation | `Nameable` | violation |
| `tagged` | the rejoin and the check carry the incarnation; a matchmaker refuses a check of an earlier one, and a rejoin keeps the record of its own incarnation | `Electable`, `Nameable` | pass (`long`) |

`Nameable`, judged per matchmaker: a matchmaker that took the rejoin of a node that came back
holds no record of an earlier incarnation, so it lets a configuration name the node. Because it
holds for any set of matchmakers, a quorum that took the rejoin is enough to name the node,
and nothing waits for the rest (decided on 2026-11-12: Pierre did not want a rejoin to need the
whole set). A retirement check may open while the rejoin is still on its way, as `SendAsk`
does not wait for it. The
`untagged` control fails because a retirement check of the old incarnation that arrives after
the rejoin records the node retired again.

The crash (added on 2026-11-14, the review of the gone-node gap). The fence protects a node that
retires. A node that crashes for good never runs the retire check, so no matchmaker records it
and nothing refuses a registration that names it. A stale belief that registers a configuration
the crash left without a Phase-1 quorum strands every later campaign, and only a leader can raise
the floor past it.

| Check | Rule | Result |
|---|---|---|
| `crash_reach` | `NoCrash`, a reachability control | violation |
| `crash_all_tomb` | the rule that ships (`allTomb`, durable fence), `q1 = n`, one crash | `Electable` violation |
| `crash_majority` | the same with majorities, one crash | pass (`long`, 7.7 million states) |

The trace of `crash_all_tomb`: `n0` shrinks `{n0,n1,n2}` to `{n0,n1}` at ballot 1 and its floor is
effective. `n1` missed the change and its campaign registers the old configuration at ballot 2 at
one matchmaker. `n2`, removed, crashes as the paper allows. A matchmaker quorum that holds the
stale registration gives `{n0,n1,n2}`, and with `q1 = n` it has no Phase-1 quorum. The unit test
`a_crashed_member_named_by_a_stale_belief_stays_in_the_history` (`paros-consensus`,
`node/tests/retire_fence.rs`) shows the same gap with five acceptors under majorities and two more
crashes. With three members under majorities, one crash leaves the stale configuration a quorum,
so `crash_majority` passes.

The abandoned reconfiguration (added on 2026-11-14 for #1065 (retired reconfiguration)). A
leader can ask for a reconfiguration that names a node that came back before the rejoin reached
the matchmakers. If that node is down, the rejoin never comes, and every matchmaker that holds the
record refuses the registration (`MatchRefusal::Retired`). The `Abandon` constant selects what
the leader does: `FALSE` ignores the refusal (the rule before #1065), `TRUE` records it and
abandons the campaign once the matchmakers that did not refuse cannot make a quorum. These two
checks use `LiveSpec`: fairness on the answers of the matchmakers, on the close at a quorum and
on the leader's reaction to a refusal, and none on a rejoin.

With `Abandon = TRUE`, a node that runs on while a matchmaker holds it retired at its
incarnation also rejoins (`Comeback`): this is the node whose own campaign is refused as
`Retired` because its retirement check was taken but its belief named it again.

| Check | `Abandon` | Spec | Invariant or property | Result |
|---|---|---|---|---|
| `retired_livelock` | `FALSE` | `LiveSpec` | `NoRetiredLivelock`: no reconfiguration stays blocked by refusals for ever | violation |
| `retired_abandon_live` | `TRUE` | `LiveSpec` | `NoRetiredLivelock` | pass (`long`) |
| `retired_abandon` | `TRUE` | `Spec` | `Electable`, `Nameable` | pass (`long`, 9.4 million states) |

The control shows the livelock of the witness: the campaign stays open, so its leader opens no
other campaign. With the rule, the campaign ends, and `Electable` still holds: an abandoned
campaign only leaves its registrations where they are, as a slow campaign does. A node that
rejoins while it runs keeps `Electable` and `Nameable` too.

Not in the model: values and votes, the promises of the acceptors, matchmaker generations, the
pages of an answer and lost matchmaker state.

### `abandoned-ballot/`: skip a registration whose ballot never opened Phase 1, for PR #1064 (gone-node gap)

The model is single-decree Matchmaker Paxos with values and votes. A campaign registers a
configuration (a stale belief, a reconfiguration, or the one in force) and may stop at any time
before its first `Prepare`. Its registration stays above the GC floor, so every later Phase 1
needs a quorum of that configuration. The rule under test lets a candidate drop such a
configuration from `H_b` when the ballot provably never opened Phase 1. The quorum system is a
2x2 grid `G`, whose Phase-1 quorums are disjoint rows, and a majority `M`.

| Constant | Values |
|---|---|
| `Skip` | `"none"`: no skip. `"owner"`: the owner of ballot `j` promised the candidate, and its promise reports the highest ballot at which it sent a `Prepare`, below `j`. A node opens Phase 1 only at its own promise, so it never opens `j` (the rule that ships, `Promise::prepared`). `"ownerNoGuard"`: a control, the same with no promise guard on `Prepare`. `"acceptor"`, `"both"`: an acceptor-side fallback for a dead owner (not shipped, not checked here). |
| `Ask`, `Anchor` | Used by the acceptor fallback only. |

Results (`abandoned-ballot/checks.txt`):

| Check | Rule | Result |
|---|---|---|
| `reach_skip` | `NoSkip`, a reachability control under `"owner"` | violation (14 thousand states) |
| `reach_choice` | `NoChoice`, a value is chosen | violation |
| `owner_noguard` | the owner rule with no promise guard | `Agreement` violation (11.6 million states) |
| `owner` | the owner rule | no violation in 45 million distinct states, to depth 24 (a 40-minute bounded run; `long`). The control broke at 11.6 million. |

The control shows why the guard matters: an owner whose testimony is below `j` can still open
Phase 1 at `j` if it does not promise first, and a value chosen there is lost.

The simulation found one more way to break the rule, outside the model: the matchmaker's history
page left out a belief that a later registration repeats (#570), so a candidate saw only the
newest registration of a configuration and asked only its owner. The page now keeps each owner's
lowest registration of each configuration (`paros-consensus` `matchmaker.rs`, `history_page`).

Not in the model: matchmaker generations, lost durable state, wipes, history pages.

### `cas-register/`: the matchmakers as a compare-and-set register, for PR #1064 (gone-node gap)

The model is the design of FoundationDB's coordinators (`LocalGenerationReg`, `DBCoreState`):
the matchmakers hold one register. A candidate at ballot `b` reads it at a matchmaker majority
(each matchmaker raises its read generation to `b`), takes the value of the highest write (the
configuration in force), runs Phase 1 over that one configuration, writes its own configuration
with a CAS (taken only where the read generation is at most `b` and the write generation below
`b`), and then runs Phase 2. A campaign that stops before its write leaves nothing behind. A
crash is allowed while every configuration a read could return, and every write in flight, keeps
a live Phase-1 quorum (FoundationDB's kill policy).

| Check | Rule | Result |
|---|---|---|
| `reach` | a value chosen after a reconfiguration and a crash | violation (12 thousand states) |
| `nocheck` | the CAS with no read generation, two ballots | `Agreement` violation (480 thousand states) |
| `cas` | the CAS, two ballots, one crash | pass: `Agreement` and `Electable` (2.5 million states) |
| `cas_long` | the same with three ballots, one matchmaker | `Agreement` violation (803 thousand states, a 25-state trace): ballot 1 chooses a value in A; ballot 2 writes B in force and stops before Phase 2; ballot 3 reads B, runs Phase 1 in B only, and chooses a second value. A one-slot register takes force before the new configuration holds the chosen values. `config-register/` is the corrected design |

The comparison with paros's registrations (`retire-fence/`, `crash_all_tomb`): with a register,
an abandoned campaign leaves no configuration in any later history, so `Electable` holds when
each crash keeps the configurations in force live. With registrations, the same kind of guard
breaks `Electable` (`crash_all_tomb`), because a stale registration names a configuration that
is not in force. What the register would let paros delete:
the retire fence's matchmaker records (`allTomb`, `MatchRefusal::Retired`, the `Rejoin` purpose)
and the sixth leg of `may_retire`, because only the configuration in force can name a node; and
the owner testimony of PR #1064, because no stale registration exists. What it costs: one more
round trip to the matchmakers (the write) before Phase 2 of every election, and a different
matchmaker protocol, which is a change to the paper (Matchmaker Paxos keeps every registration
above the floor). That change is a proposal, not part of PR #1064.

### `config-register/`: seal the latest configuration, for #1095 (rework 1.6)

The model of rule 1 of the rework (`docs/architecture/04-roles.md`) and of the resize of rule 2.
It starts from `cas-register/` and corrects the bug of `cas_long`. The matchmakers hold a
register for each journal: the configuration in force `cur`, and at most one open successor
`suc`. Each matchmaker has a read generation and a write generation, as FoundationDB's
`LocalGenerationReg` (`fdbserver/coordinator/Coordination.cpp`). The write order is
FoundationDB's recovery order (`fdbserver/ClusterRecovery.actor.cpp`): the new `DBCoreState` is
written only after the new log system holds the recovered data.

A candidate at ballot `b`, for one slot:

1. It reads the register at a matchmaker majority (each matchmaker raises its read generation
   to `b`) and takes the value of the highest write.
2. It runs Phase 1 over a quorum of `cur`, and of `suc` when one is open. This seals the old
   configuration.
3. With no open successor, it stays in `cur` and writes nothing, or it starts a move to a live
   configuration `c`: it announces `<<cur, c>>` at a majority (a compare-and-set at write
   generation `2b`). With an open successor, it finishes that move, and it first announces the
   successor again at a majority: a read can return a successor that only one matchmaker holds.
4. It runs Phase 2 in the target only.
5. After its value is chosen in the target, it switches the register to `<<target, none>>` at a
   majority (a compare-and-set at `2b + 1`).

A candidate can stop after any step. A machine releases its store for the journal once one
switch is durable: a matchmaker majority holds the same write (one generation), with no
successor, whose configuration does not name the machine. A move never names again, under the
same id, a machine that left a configuration in force: a machine that comes back is a new id
with a new store (rule 5). A crash is allowed while every configuration that a read can return
keeps a live Phase-1 quorum (FoundationDB's kill policy). The quorum system and the sizes are
constants: `resize` moves from three acceptors to five (rule 2), `partitioned` moves between
two layouts of two partitions, where Phase 1 needs a majority of every partition and Phase 2 a
majority of the first one (decision 15), and `grid` moves between two full grids of 2 x 2
(#1102 (rework 1.13)). The grids come from `grid/Grid.tla`: the layout is row-major over the
sorted members, as in the code, so the move of `grid` (`a5` replaces `a1`) also moves `a2`,
`a3` and `a4` to other cells. Phase 2 takes a whole column, and any column may take the slot.

Each control turns off one rule, and each one fails:

| Check | Rule | Result |
|---|---|---|
| `reach` | a store released after a move, with a value chosen | violation (3 thousand states) |
| `switch_before` | control: the one-slot register of `cas_long` (the target is in force before Phase 2), three ballots | `Agreement` violation (70 thousand states) |
| `stale` | control: the compare-and-set ignores the read generation, so a belief that a later read passed is stored | `Agreement` violation (11 thousand states) |
| `abandon` | control: a candidate drops an open successor that it read | `Agreement` violation (8 thousand states) |
| `direct` | control: a candidate finishes a successor that it read with no announce of its own; three matchmakers, one crash, one value | `Electable` violation (1.3 million states) |
| `rejoin` | control: a move names again, under the same id, a machine that left; one matchmaker, one crash, one value | `Electable` violation (59 thousand states; the same check with `Rejoin = FALSE` passes) |
| `release_early` | control: a machine releases once a majority holds a successor that does not name it, before the switch | `Electable` violation (1 thousand states) |
| `mixed` | control: a machine releases on a majority of different writes, each with no successor and not naming it; three matchmakers, one crash | `Electable` violation (19 million states, `long`) |
| `live_mm_small` | control: one matchmaker on a machine that can die (the matchmakers survive no loss, the journal survives one) | `Wins` violation (40 thousand states) |
| `agree` | the rule, three ballots, one matchmaker, stores released | pass: `Agreement` and `Electable` (4.0 million states, 3.5 minutes) |
| `resize` | the rule, a move from three acceptors to five, two ballots | pass (460 thousand states) |
| `agree_mm3` | the rule, three matchmakers on three machines, one crash, two ballots | pass (26 million states, 20 minutes, `long`) |
| `partitioned` | the rule, the partitioned layouts, seven acceptors, two ballots | pass (21 million states, 17 minutes, `long`) |
| `grid_slack` | control: two full grids, Phase 2 takes one acceptor of a column and Phase 1 still one of each column, where it needs two | `Agreement` violation (7 thousand states) |
| `grid` | the rule, a move between two full grids of 2 x 2, five acceptors, two ballots | pass (453 thousand states, 40 seconds) |
| `grid_long` | `grid` with three ballots | no violation in 53.6 million distinct states, to depth 27 (a bounded run of 2 hours, 2 workers; `long`) |
| `live` | `LiveSpec`: three matchmakers, one crash, no release; the top ballot wins | pass (15.5 million states, 17 minutes, `long`) |
| `live_release` | `live` with stores released | pass (21 million states, 25 minutes, `long`) |
| `agree_long` | `agree_mm3` with three ballots | no violation in 181 million distinct states, to depth 23 (a bounded run of 2 hours that filled the disk; `long`) |
| `heal_partitioned_short` | control: the heal `J` to `K` of the journal grid 3 × 2 (`a7` replaces `a1`, so every member that stays changes its column), Phase 1 one acceptor short in every column | `Agreement` violation (417 thousand states, 4 minutes) |
| `heal_partitioned` | the rule on that heal, seven acceptors, two ballots (#1258 (reshape with a column below quorum), T5) | pass: `Agreement` and `Electable` (32.8 million states, 59 minutes, `long`) |
| `resize_long` | `resize` with three ballots | no violation in 109 million distinct states (a bounded run of 2 hours; `long`) |

`agree` runs in CI because two ballots cannot show the bug of `switch_before`. The checks with
three matchmakers are slow, so the control `direct` runs in CI with one value and the passes run
as `long`. With one matchmaker each write is at a majority, so the bugs of `direct` and `mixed`
need three. The bug of `rejoin` does not need three: it shows with one matchmaker, so its control
is small. `Electable` does not read the value, so one value is enough for both controls. `live_mm_small` is the check of the survive numbers of plan step 1.6: when the
matchmakers survive fewer losses than the journal, a loss stops every campaign.

### `cell-control/`: the coarse control plane of a cell, for #1096 (rework 1.7)

The model of rule 3 (a small authority, one writer) and of the cell parts of rules 4 and 5
(`docs/architecture/03-3-coordinators-and-placement.md`, `03-2-machines.md`). It has no Paxos
inside: a set has members, and it serves while a majority of them is up at a known address.
It carries the TLA+ gates of phases 3, 4 and 5.

- **The term** (decision 1) is one compare-and-set of the leader uuid on the cell control
  journal. Any machine that serves the cell may campaign once no coordinator holds the term for
  one lease (decision 7). The coordinator is the one writer of the journal.
- **Orders.** The coordinator records each action before it does it, and each order carries its
  term. The receiver keeps the highest term it saw on disk and refuses an order below it, so the
  order of a deposed coordinator that is still in flight changes nothing.
- **Duties.** Admit a machine with a new id; replace a member that is gone one for one, or
  report "cannot place" when no machine can take its place; write `MachineGone` once the id is
  in no set; apply a request of the tenant level once (its outcome record, by idempotency id).
- **The operator** lowers the survive number and declares a machine gone (`remove-machine
  --failed`, decision 24) only through records; the declaration has a safety check: the live
  members of each set without the machine are a quorum.
- **Faults.** One machine wiped for good, whose disk comes back as a new id (decision 4);
  machines that stop and start at a new address; a receiver of orders that restarts.

| Check | Rule | Result |
|---|---|---|
| `two_writers` | control: a coordinator writes with no uuid fence | `OneWriter` violation (63 states) |
| `no_fence` | control: a receiver takes an order of any term | `NoRegress` violation (106 thousand states) |
| `volatile` | control: a receiver forgets its highest term when it restarts | `NoRegress` violation (304 thousand states) |
| `twice` | control: a coordinator looks only for an outcome of its own term | `ExactlyOnce` violation |
| `shrink` | control: a change keeps only the members that answered (#846) | `SizeKept` violation |
| `auto_lower` | control: the cell lowers the survive number when it cannot place | `SurviveKept` violation |
| `gone_unchecked` | control: the operator declares a machine gone with no safety check | `QuorumKept` violation |
| `bookings` | control: only a booked machine may campaign (the candidate rule of the old election journal, seed 2 of the election reds of 2026-11-10) | `CoordinatorServes` violation: the booked machine is wiped and no candidate is left |
| `inplace` | control: a repair waits for the broken member to come back | `Heals` violation |
| `address` | control: a new address is written by the coordinator | `CoordinatorServes` violation: a member of the control journal is wiped, another starts at a new address, and the journal cannot serve to record it |
| `safety_small` | the rules, three machines, both sets on all three, a candidate that deposes a live coordinator | pass: every invariant (59 thousand states) |
| `live` | `LiveSpec`, four machines and a new id: a coordinator serves again, each set heals or reports "cannot place", and the registry names no dead id | pass (129 thousand states) |
| `safety` | the rules, four machines and a new id, three terms, a campaign only after the lease | pass: every invariant (129 thousand states) |

`safety` with a candidate that deposes a live coordinator, on four machines, did not finish in 30
minutes (28 million states, no violation); `safety_small` checks that case on three machines.
What the model does not check: the old design's election journal as a second majority set. With
one wipe, a second set of three keeps its quorum, so its cycle is its candidate rule, which
`bookings` checks. The cell does not compute its survive number here; `SurviveKept` checks that
only an operator record changes it.

### `handover-gc/`: the GC floor across a handover, for #1014 (model of the handover and the GC floor)

The model is one handover of a matchmaker set, from the set `M0` to the set `M1`, with the GC
floor and the registrations of each matchmaker. It has no values, no votes and no acceptors. It
answers two questions. When a leader reports a floor as effective, does a majority of each
generation in force hold that floor? When a matchmaking phase closed at a majority, does each
majority of the generation in force still give that registration, unless a floor passed it?

A matchmaker of `M0` freezes and reports its floor and its registrations (`Stopped`). The
successor state is the maximum floor of a majority of reports and the union of their
registrations (`close_stop`). A member of `M1` activates with that state.

Constants select the rule:

| Constant | Values |
|---|---|
| `Collector` | `watermark`: the collector counts each ack whose watermark is at or above the floor, applied or refused (`Collector::fold_ack` today). `applied`: it counts only an ack that applied the request. |
| `PoolLag` | `TRUE`: the leader cannot learn the later set from a refusal, because a member of `M1` is not in its pool yet. The refusal goes to the collector. `FALSE`: the leader learns the later set and drops the ack. |
| `CloseFloor` | `max`: the successor floor is the maximum of the reports. `one`: it is the floor of one report. |
| `FrozenRegisters` | `TRUE`: a frozen matchmaker still registers a ballot. |

The invariants are `FloorCarried` (the first question) and `RegCarried` (the second).

The model has the sets `{X, Y, Z}` and `{Y, Z, W}` and two ballots.

Results (`handover-gc/checks.txt`):

| Rule | Invariant | Result |
|---|---|---|
| `watermark` collector, the leader cannot learn the later set | `FloorCarried` | violation |
| `watermark` collector, the leader learns the later set | `FloorCarried` | pass |
| `applied` collector, the leader cannot learn the later set | `FloorCarried` | pass |
| The successor floor is the floor of one report | `FloorCarried` | violation |
| A frozen matchmaker registers | `RegCarried` | violation |
| The rules of today | `RegCarried` | pass |

The first row is a floor that is effective at no majority. `X` applies the floor of ballot 1 at
generation 0. The handover closes on the reports of `Y` and `Z`, with the floor 0. Ballot 2
raises the floor of `Y` at generation 1. `Y` then refuses the request of ballot 1 for generation
0, and its refusal carries the watermark 2. The collector of ballot 1 counts `X` and `Y`: the
floor 1 is effective, but `Z` and `W` hold the floor 0. This is class 5 of item 4 of #903
(matchmaker safety and liveness).

Not in the model: the decree that chooses the successor, lost matchmaker state, the effective
configuration, the size of the successor set and more than one journal. `handover/` (#1097
(rework 1.8)) extends this model with the target, the size and the register.

### `handover/`: a matchmaker set handover onto a named target, for #1097 (rework 1.8)

The model of rule 2 of the rework at the matchmakers: sets never shrink
(`docs/architecture/04-roles.md`). It extends `handover-gc/`: the GC floor crosses the
handover as there (with the `applied` collector, the rule that passed), and the register of
`config-register/` crosses it too, also while a journal has an open successor. The steps are
those of `matchmaker/reconfigurer.rs`:

1. The coordinator names a target. The target has the size that the control journal records,
   and only live members. It names a new target only when a member of the last one is down.
2. A member of the old set `M0` freezes (`Stop`) and keeps the target durably.
3. A reconfigurer closes the stop on a majority of frozen members. The successor state is the
   maximum floor, the maximum read generation and the register of the highest write. The
   reconfigurer of the coordinator proposes its target; a finish proposes the target that the
   frozen members keep.
4. Each member of the proposed set stores the state (`Bootstrap`).
5. The decree over `M0` chooses one proposal (one step here; the decree is `decree/`).
6. Each member of the chosen set activates and serves generation 1.

Beside the handover, the clients of generation 0: a candidate reads the register (each
member of a quorum raises its read generation), then writes it with a compare-and-set. Write 1
announces a successor (`AB`: `A` in force, `B` open), and write 2 switches (`B`). A leader
raises the GC floor. Each client step reaches a whole quorum at once. The operator can record
a new size (3 to 5) before the handover, and a journal can then record a higher survive
number.

Constants select the rule. The first value of each is the rule of the rework.

| Constant | Values |
|---|---|
| `Finish` | `"target"`: a finish proposes the target that the frozen members keep. `"answerers"`: it proposes the members that answered the freeze, thinned to an odd count (`odd_answerers`, the code today). |
| `FirstWrite` | `"every"`: the decree chooses a proposal only when every member holds the bootstrap. `"majority"`: a majority of the proposed set is enough. |
| `CloseState` | `"max"`: the maximum floor and read generation, the register of the highest write. `"inforce"`: the register keeps only the configuration in force and drops the open successor. `"noread"`: the read generation does not cross. |
| `Retarget` | `TRUE`: the coordinator names a new target when a member of the last one is down. |
| `JournalFirst` | `TRUE`: a journal records a higher survive number before its tenant's set has the size for it (decision 35). |
| `FrozenServes` | `FALSE`: a frozen member serves nothing. `TRUE`: a control, a frozen member still serves its clients. |
| `ResizeGuard` | `TRUE`: the operator records a new size only while no target is named, so a resize and a handover never overlap. `FALSE`: a control, a resize during a handover. |

The invariants:

- `SizeKept`: the chosen set has the size that the control journal records. Only a resize
  record changes the size.
- `FirstWriteFull`: every member of the chosen set holds the successor state.
- `FloorCarried`: an effective floor is held by each serving quorum of generation 1.
- `ReadCarried`: a completed read stays a fence in each serving quorum of generation 1.
- `RegisterCarried`: a completed write, or a higher one, is in each serving quorum of
  generation 1, with its value. The value of write 1 is an open successor.
- `MatchmakersOutlive`: the journal never records a survive number above the one of its
  tenant's set in force (decision 35).
- `Ends` (with `LiveSpec`): the decree chooses a set, and each live member of it serves
  generation 1. The handover steps are fair, the clients and the crashes are not.

The old set is `{X, Y, Z}`. The targets are `{Y, Z, W}`, `{Z, W, V}` and, for the resize, the
five machines. One machine can go down for good. The liveness checks have four targets of
three, so that one target of live machines is left whichever machine is down.

Results (`handover/checks.txt`):

| Check | Rule | Result |
|---|---|---|
| `reach` | `NoHandoverWithState`, a reachability control: the handover ends after a write with an open successor, with an effective floor and one machine down | violation (about 1.4 million states) |
| `answerers` | control: a finish proposes the members that answered | `SizeKept` violation (42 thousand states) |
| `first_write_majority` | control: the decree chooses a set that a majority holds | `FirstWriteFull` violation (79 thousand states) |
| `inforce` | control: the reconstruction drops the open successor | `RegisterCarried` violation (550 thousand states) |
| `noread` | control: the read generation does not cross | `ReadCarried` violation (460 thousand states) |
| `journal_first` | control: the journal records survive 2 while the set has three members | `MatchmakersOutlive` violation (68 states) |
| `live_noretarget` | control: the coordinator never names a new target | `Ends` violation (1.7 thousand states) |
| `handover` | the rules, one crash | pass: every safety invariant (3.7 million states, 1.5 minutes with 4 workers) |
| `resize` | the rules, a resize from three to five and survive 2 | pass (8.5 thousand states) |
| `live` | `LiveSpec`, the rules, one crash | pass: `Ends` (33 thousand states) |
| `live_resize` | `LiveSpec`, the resize, no crash | pass (538 states) |
| `first_write_majority_safe` | `FirstWrite = "majority"`, every invariant but `FirstWriteFull` | pass (6.9 million states, 3 minutes; `long`) |
| `frozen_serves` | control: a frozen member serves its clients (one target, no crash) | `ReadCarried` violation (13 states deep, 1.4 million states) |
| `resize_during` | control: the operator records the size 5 while the target `{Y, Z, W}` is named | `SizeKept` violation (10 states: `X` and `Y` freeze, the stop closes, the size is recorded, the decree chooses the set of three) |

The trace of `answerers`: the coordinator names `{Y, Z, W}`. `Y` and `Z` freeze, and a finish
closes on them. It proposes the members that answered, thinned to an odd count: `{Z}`. `Z`
stores the bootstrap, and the decree chooses a set of one where the control journal records
three. This is #846 (finish shrinks a set) and #895 (odd sets). With `Finish = "target"`, the
finish proposes `{Y, Z, W}`, which the frozen members keep.

The trace of `noread`: a candidate reads at generation 2 at `{X, Y}`. `X` and `Z` freeze, and the
stop closes on them with no read generation. `Y`, `Z` and `W` activate; `Z` and `W` are a
serving quorum with read generation 0, so a compare-and-set at generation 1, below the
completed read, can complete. This is the bug of `config-register/stale` across a handover.
The trace of `inforce`: write 1 (an open successor) completes at generation 0. `X` and `Y`
freeze, and the stop closes on them with the configuration in force only. `Z` and `W` activate
with `A` and no successor, so their quorum reports no open move, and a candidate can start a
third configuration (the bug of `config-register/abandon`).

`live_noretarget`: `W` goes down after the coordinator named `{Y, Z, W}`. The bootstrap waits
for `W` for good, because the first write needs every new member. Only a new target of the
coordinator ends the handover: a frozen set never picks its own successor.

Finding: the rule "the first write needs every new member" is not a safety rule. With
`FirstWrite = "majority"`, the floor, the read generation and the register still cross
(`first_write_majority_safe` passes), because each majority of the new set meets the majority
that holds the bootstrap. The rule is the size rule: a member that holds no state cannot serve,
so a set chosen at a majority serves with fewer members than its size, and one more loss
stops it (`first_write_majority` shows the chosen set with a member that holds nothing).

Finding of #1230 (rules no check tests): `SizeKept` needs a rule that no design document
writes. The decree runs over the old matchmakers, which do not read the control journal, so
nothing in the handover sees a resize that the operator records after the target is named
(`resize_during`). The model keeps the size with `ResizeGuard`: a resize waits until no target is
named. This is the rule (Pierre, 2026-11-20): a resize waits for the handover
(`docs/architecture/04-roles.md`, rule 2).

Not in the model: the decree (`decree/`), lost matchmaker state, more than one journal (a
handover is paged, #439) and the registrations, which leave with rule 1. A client step that
reaches a minority, the clients of generation 1 and a second handover are in
`handover-chain/`.

### `handover-chain/`: a chain of handovers with clients that reach one member at a time, for #1230 (rules no check tests)

`handover/` has two generations, and each client step reaches a whole quorum in one step. So
every stop quorum builds the same successor state, and no tally of a client can count the
wrong acks. `handover-chain/HandoverChain.tla` checks what that leaves out. The target rules,
the size and the crashes stay in `handover/`; here the targets are fixed, to keep the chain
small.

- Three generations: `{X, Y, Z}`, then `{Y, Z, W}`, then `{Z, W, V}`. The clients act at each
  generation. `Z` is in every set, so it can activate generation 2 with no stop at
  generation 1 (it skips a generation, as `matchmaker/generation.rs` lets it).
- A client step reaches one member. A read, a compare-and-set write and the GC floor each keep
  the set of members that acked, and complete on a majority of one generation. A step can stop
  at a minority, so two stop quorums can build two successor states.
- A member keeps the first bootstrap of a generation that reaches it, and the decree chooses
  the set only (`reconfigurer.rs`: the decree value is the member list). So the members of one
  chosen set can activate with different states.

The invariants are those of `handover/`, for every generation at or after the one where the
client step completed: `FloorCarried`, `ReadCarried` and `RegisterCarried`.

| Constant | Values |
|---|---|
| `Fence` | `TRUE`: a client tally of generation `g` counts only the acks that members gave while they served `g` (`collector.rs` checks the generation before it counts a reply). `FALSE`: a control, an ack counts for each generation whose set names the member. |
| `FrozenServes` | `FALSE`: a frozen member serves nothing. `TRUE`: a control, a frozen member still acks. |
| `Writes`, `Floor` | the write numbers (each with its read before it), and whether the leader raises the GC floor. |

Results (`handover-chain/checks.txt`):

| Check | Shape | Result |
|---|---|---|
| `reach` | `NoChain`, a reachability control: the third generation serves after the read and the write completed, the write at generation 1 or 2, and a member skipped a generation | violation (116 thousand states, 23 states deep) |
| `one_state` | `OneState`, a reachability control: two members of one chosen set keep different bootstraps | violation (5.9 thousand states) |
| `no_fence` | control: a tally counts an ack of another generation; two generations, one write, the floor | `ReadCarried` violation (43 thousand states, 14 states deep) |
| `frozen_serves` | control: a frozen member acks; the same shape | `ReadCarried` violation (183 thousand states) |
| `two` | the rules; two generations, one write, the floor | pass (337 thousand states, 25 seconds) |
| `chain` | the rules; three generations, one write, no floor | pass (188 thousand states, 31 seconds) |
| `chain_floor` (`long`, bounded at 30 minutes) | `chain` with the floor | no violation in 14.2 million distinct states (bounded: 30 minutes, 2 workers) |
| `chain_two_writes` (`long`, bounded at 30 minutes) | `chain` with two writes | no violation in 12.7 million distinct states (bounded: 30 minutes, 2 workers) |

The trace of `no_fence`: `X` acks a read at generation 0. `Y` and `Z` freeze, and the stop
closes on them. Neither acked, so the bootstrap of `W` has read generation 0. The decree
chooses `{Y, Z, W}`, and `Y` activates generation 1 and acks the read there. The tally of
generation 0 counts `X` and `Y`, a majority of `{X, Y, Z}`, and the read completes. `Z` and `W`
then serve generation 1 with read generation 0: a compare-and-set below the completed read can
complete. This is the class of `handover-gc/collector_watermark`, for the read and the
register.

No design decision is refuted. With the two fences, a state that only a minority holds and
two members with different bootstraps change nothing: a completed client step is at a majority
of its generation, every frozen majority meets it, and a member that activates keeps its own
state where it is higher. The floor moves as the read generation does (each member keeps the
maximum), so the long chain leaves it out.

Not in the model: the target rules, the size, the crashes (`handover/`), the decree (`decree/`),
lost matchmaker state, and a late ack of a client that already completed (it only raises a
member's state, as a new ack does).

### `reads/`: leaderless reads, for #1016 (model of leaderless reads)

The model is a copy of `zero-stall/Bypass.tla` with the acks to the clients and one leaderless
read. A reader asks a read target of the configuration of its read basis for vote watermarks, and
the read completes on a Phase-1 quorum of the answers. It serves the read at the maximum of the
watermarks and the fence of the basis. The invariant is
`ReadSeesAcked`: a served read covers each slot that a leader acked before the read started.

Constants select the rule:

| Constant | Values |
|---|---|
| `Basis` | `won`: a node learns its read basis only from a leadership that won its ballot (#260). `belief`: the basis is the configuration of the ballot that the node promised. |
| `Fence` | What a `PreReadAck` carries; an answer above the basis ends the read (#1099 (rework 1.10)). `promise`: the higher of the promise and the belief ballot of the acceptor (`wire_read_fence`, the code since PR #1074 (fence a leaderless read on the promise)). `belief`: the belief ballot alone (`acceptors_since`, the code before PR #1074), a control. `none`: no ballot, a control. |
| `Laggers` | The acceptors whose pool can lag: such an acceptor can ignore the configuration of a ballot that it promises or accepts (`learn_config`), and its belief ballot stays where it was. Its promise moves all the same. |
| `Restarters` | The acceptors that can restart. The promise and the votes are durable. The belief goes back to the bootstrap, bound to no ballot, and the node did not hear what is in force until its membership probe answers. A reader also forgets its read basis and its open read. |
| `Refuse` | `TRUE`: an acceptor that did not hear answers no `PreRead` (`on_pre_read`, the code). |
| `Hold` | `TRUE`: the decision hold of #579 (zero-stall reconfiguration). `zero-stall/` holds the full checks of the hold. |
| `Learner` | The options of #890 (learner above the leader). A reader campaigns with a stale belief, promises its own ballot and sends no `Prepare`. `strict`: the node never takes a leadership below its promise as its read basis, and opens or serves no read on it (`main` today). `own`: option (b), the node takes the basis when the only promise above it is its own abandoned campaign. `any`: the node ignores its promise. `off`: no such campaign. |
| `Q1s`, `Q2s`, `Targets` | The quorum system, for each configuration (#1102 (rework 1.13)): the Phase-1 quorums, the Phase-2 quorums, and the sets of acceptors that a reader asks. A majority asks the whole membership. A grid takes them from `grid/Grid.tla`: the partitions ask the whole membership, the full grid asks one row (`row_of`). |
| `Seal` | `quorum`: Phase 1 takes a Phase-1 quorum of each prior configuration (the code). `reads`: a candidate rule, not in the code: Phase 1 also takes one promise in each read target of each prior configuration, so each row of an old full grid hears of the campaign. |
| `ReadAgain` | `TRUE`: a client gets the answer of a served read (`ReadClose`), and a second read can start. `ReadSeesAcked` then also orders reads: a served read covers each slot that an earlier read returned. |
| `ReadHold` | The candidate rules of #1270 (bypass read order): what the decision hold also gates on the read side of a ballot. `off`: nothing, the hold gates only the acks of the leader (a control). `basis`: a reader takes no read basis at a ballot before its fence Phase 1 is covered. `serve`: a reader serves a read on a basis only when the ballot is covered. `prefix`: the chosen prefix of a reader counts a slot only once the ballot that chose it is covered, so a leader sends no `Commit` of a bypass ballot before its fence Phase 1. A ballot that won by a full Phase 1 is covered when it leads, so each rule acts only on a bypass ballot. |
| `ReaderGate` | What gates the reader itself, to open, keep and serve a read (#1228 (stale read by a lagging reader)). `promise`: the rule of `Learner` on the promise of the reader (the code: `read_basis` and `fence_quorum_reads` use the read fence). `belief`: the belief ballot of the reader alone, and the reader counts its own answer with no fence (the code before #1228), a control. |

The majority checks have four acceptors, the configurations `{a1, a2, a3}` and `{a3, a4}`, the
readers `a1` and `a3`, one matchmaker, one slot and two ballots. The learner checks have a third
ballot that is only the abandoned campaign of a reader.

The lagging-reader checks (#1228) have five acceptors, the configurations `{a1, a2, a3}` and
`{a3, a4, a5}`, the reader `a1` whose pool can lag, one matchmaker, one slot and two ballots. They
check `Agreement` too.

The grid checks (#1102 (rework 1.13)) have a change of members, one matchmaker, one slot, two
ballots and the reader `a1`, the member that leaves:

- the full grid of 2 x 2: `G` over `a1..a4` (rows `{a1, a2}` and `{a3, a4}`, columns `{a1, a3}`
  and `{a2, a4}`), then `H` over `a2..a5`, where `a5` replaces `a1`;
- the partitions with `F = 1` and two partitions (a grid of 3 x 2 with `acks = 2`): `P` over
  `a1..a6`, then `R` over `a2..a7`.

Results (`reads/checks.txt`):

| Rule | Result |
|---|---|
| The basis is the belief of the node | violation |
| A `PreReadAck` carries no ballot | violation |
| `fence_belief_lag`: the ack carries the belief ballot, `a2` can lag | violation (190 thousand states, 4 seconds) |
| `reboot_belief_open`: the ack carries the belief ballot, `a2` can restart, no refusal | violation (620 thousand states, 11 seconds) |
| `reboot_belief_refuse`: the ack carries the belief ballot, `a1` and `a2` can restart, the refusal (`long`) | pass (2.5 million states, 1 minute) |
| `reboot_promise_open`: the ack carries the promise, `a2` can lag and restart, no refusal | pass (2.6 million states, 1 minute) |
| `reboot_promise_open_reader`: the same, and the reader `a1` can restart too (`long`) | pass (4.7 million states, 2 minutes) |
| The bypass of #579 with no decision hold | violation |
| The rules of `main` | pass |
| `reads_lag_reader_belief`: the reader `a1` can lag, the reader is gated on its belief ballot | violation (114 thousand states): `a1` and `a2` promise ballot 2 and `a1` ignores its configuration; ballot 2 chooses slot 1 in `{a3, a4, a5}`; `a1` reads on ballot 1 with its own answer and the answer of `a3`, and serves index 0 |
| `reads_lag_reader`: the same, the reader is gated on its promise (the code) | pass (557 thousand states) |
| The bypass of #579 with the decision hold | pass |
| `reads_plain_again`: the rules of `main`, and a second read | pass (897 thousand states) |
| `reads_bypass_hold_again`: the bypass of #579 with the decision hold, and a second read, `ReadHold = "off"` | violation (1.6 million states, 21 steps): see below |
| `reads_bypass_hold_again_p212`: the same, `Mode = "p212"` | violation (759 thousand states, 21 steps) |
| `reads_bypass_hold_again_basis`: `ReadHold = "basis"` | violation (1.3 million states, 22 steps): see below |
| `reads_bypass_hold_again_serve`: `ReadHold = "serve"` | violation (1.7 million states, 22 steps) |
| `reads_bypass_hold_again_prefix`: `ReadHold = "prefix"`, the rule | pass (3.1 million states, 1.5 minutes) |
| `reads_bypass_hold_again_prefix_p212`: the rule, `Mode = "p212"` | pass (2.1 million states, 2 minutes) |
| `reads_bypass_hold_again_prefix_two_slots`: the rule, two slots (`long`) | no violation in 45.9 million distinct states (bounded: 30 minutes, 2 workers) |
| `reads_bypass_hold_again_prefix_chain`: the rule, three ballots of one proposer, so two bypasses in a row (`long`) | no violation in 50.2 million distinct states (bounded: 30 minutes, 2 workers) |
| `reads_two_slots`: `reads_plain` with two slots (`long`) | no violation in 30.7 million distinct states, to depth 24 (bounded: 50 minutes, 1 worker) |
| `reads_three`: `reads_plain` with three ballots, `OwnerParos3`, so a reader can hold the basis of the middle leadership (`long`) | no violation in 33.4 million distinct states, to depth 25 (bounded: 50 minutes, 1 worker) |
| #890, `strict` (`main` today) | pass |
| #890, option (b) | pass |
| #890, the node ignores its promise | pass |
| `grid_full`: the full grid, a change of members, the row read (the code) | violation (300 thousand states, 15 seconds) |
| `grid_full_static`: the full grid, no change of members | pass (588 thousand states, 35 seconds) |
| `grid_full_seal`: the full grid, a change of members, `Seal = "reads"` | pass (1.6 million states, 1.5 minutes) |
| `grid_partitions`: the partitions, a change of members | no violation in 71.2 million distinct states, to depth 23 (a bounded run of 2 hours, 2 workers; `long`) |

The trace of `grid_full` is the stale read that `quorum_read.rs` names (a row "wholly unaware of
a completed successor"):

1. Ballot 1 leads in `G`. Ballot 2 registers `H`, and its history holds `G` and `H`.
2. `a3` and `a4` promise ballot 2. That is the row `{a3, a4}` of `G`, a Phase-1 quorum of `G`
   (one acceptor of each column), and a Phase-1 quorum of `H` too. Ballot 2 leads.
3. `a3` and `a5`, the column `{a3, a5}` of `H`, accept slot 1 at ballot 2. The leader acks it.
4. The reader `a1` hears a late beat of ballot 1 and takes `G` as its read basis.
5. It asks the row `{a1, a2}` of `G`. Neither heard of ballot 2: their ballot is 1 and their
   watermark is 0. The read is served at index 0 and misses slot 1.

The `PreReadAck` catch (`AckBallot`) needs one acceptor of the row that promised the later
ballot. Under a majority, and on the partitions, two Phase-1 quorums of one configuration always
meet, so the read finds that acceptor. On the full grid two rows do not meet
(`grid/grid_full_rows`). Two rules close the gap, and each check passes: a full grid that
refuses a change of members (`grid_full_static`), and a seal that reaches each row
(`grid_full_seal`). Pierre chose the seal that reaches each row on 2026-11-19 (#1102, decision 31
in `docs/architecture/02-data-plane.md` section 2.5); the code is #1209 (seal each row of a full
grid).

The fence of the `PreReadAck` (#1099 (rework 1.10), findings 1 and 11 of the review of
`paros-consensus` of 2026-11-16). The trace of `fence_belief_lag` is finding 1:

1. Ballot 2 registers `{a3, a4}`. Its history holds `{a1, a2, a3}`, the configuration of
   ballot 1.
2. `a2` promises ballot 2, but its pool lags, so it ignores the configuration of ballot 2. Its
   belief ballot stays 0. `a3` promises ballot 2 too. That is a Phase-1 quorum of
   `{a1, a2, a3}`, the one configuration of the history, and ballot 2 leads.
3. `a3` and `a4` accept slot 1 at ballot 2. The leader acks it.
4. The reader `a1` hears a late beat of ballot 1 and takes `{a1, a2, a3}` as its read basis.
5. `a1` and `a2` answer with the belief ballot 0 and the watermark 0. That is a Phase-1 quorum
   of the basis. The read is served at index 0 and misses slot 1.

With the promise in the ack (the code since PR #1074), the answer of `a2` carries ballot 2 and
ends the read: `reboot_promise_open` passes with the same lagger.

The refusal of a rebooted node, examined again. `on_pre_read` answers no `PreRead` until the
node heard what is in force. Under the old fence, the refusal is necessary:
`reboot_belief_open` finds the same stale read, where `a2` restarts after its promise of ballot
2, in place of a lag, and its belief goes back to ballot 0. Under the fence of the code, the
refusal is not necessary for safety: `reboot_promise_open` and `reboot_promise_open_reader` pass
with no refusal, because the promise is durable and the ack of a rebooted node carries it. The
refusal only costs availability: a rebooted acceptor does not help a read until its membership
probe answers. The code keeps it today. To remove it is a change of `on_pre_read` with this
check as its model; it is not part of this step.

Finding of #1230 (rules no check tests): the decision hold of the bypass does not order two
reads (#1270 (bypass read order)). The trace of `reads_bypass_hold_again`: ballot 1 leads in
`{a3, a4}` and bypasses to ballot 2 in `{a1, a2, a3}`. `a1` and `a2` accept slot 1 at ballot 2;
the slot is chosen and not acked (the hold). `a1` takes the basis of ballot 2, asks `{a1, a2}`
and is served at index 1, and its client gets the answer. Then `a3` takes the basis of ballot 1
from a late beat, asks `{a3, a4}`, where no one promised ballot 2, and is served at index 0. The
hold gates only the acks of the leader, and a reader serves on any chosen slot. The code of
today has no bypass and passes (`reads_plain_again`).

The rule for #579 (Pierre asked for its own check on 2026-11-20): **a reader's chosen prefix
counts a slot only once the ballot that chose it is covered** (`ReadHold = "prefix"`). The leader
of a bypass ballot sends no `Commit` before its fence Phase 1 has a Phase-1 quorum of each prior
configuration, as it sends no ack. Then a read that returns the slot comes after the fence Phase
1, and a later read on an old basis meets an acceptor that promised the bypass ballot, and ends.
The two weaker candidates fail. A gate on the read basis (`basis`) does not help, because the
first read need not use the bypass ballot: `a3` voted slot 1 at ballot 1, answers a read on the
basis of ballot 1 with its watermark 1, and then `a3` and `a4` choose slot 1 at ballot 2. The
read is served at index 1. A second read on ballot 1 asks `{a1, a2}`, which never heard of ballot
2, and is served at index 0. A gate on the serve (`serve`) fails with the same trace. The rule
must gate the chosen prefix, which every read uses. `zero-stall/` checks the same rule
(`ReadHold`) with its abstract reader.

Two more controls show that the model reaches the case of #890. `learner_stranded` finds a
leader and a reader outside its configuration whose own abandoned campaign is above it.
`learner_own_serves` finds a read that option (b) serves and that `strict` refuses.

Option (a) of #890, where the leader campaigns above the promise, adds no rule to the model: it
is one more ordinary campaign, and `reads_plain` covers it. Its cost is a leader change for each
stale campaign of a removed node. The row "the node ignores its promise" passes in this model
because an acceptor of the basis configuration always reports a later ballot. It is not a
recommendation: the learner checks have only majority quorums.

Not in the model: GC watermarks, matchmaker generations, the replica tier, a read that expires,
an acceptor that loses its disk (its promise is durable here), the `StaleConfiguration` path
that binds the belief below the promise (the fence of the code covers it, because it takes the
higher of the two), the registration of the abandoned campaign, and a move between a grid and a majority. Two slots
(`reads_two_slots`) and three leaderships (`reads_three`) are `long` checks on the majority only
(so a slot's column is any column), and no check has the leader's fill up to an orphan
watermark (`fill_to_watermark`).

Liveness (#1230 (rules no check tests)), with `LiveSpec`: each asked acceptor answers, a node
that did not hear probes, and an open read is served or abandoned; with `LiveLeader`, the
campaign of the last ballot is fair too (it registers, wins, proposes every slot, and its
configuration accepts). Restarts are not fair. `ReadEnds`: an open read ends.

| Check | Shape | Result |
|---|---|---|
| `live` | `reads_plain` | `ReadEnds` pass (663 thousand states, 4 minutes with 2 workers) |
| `live_no_leader` | the same, no fair campaign (`LiveLeader = FALSE`) | `ReadEnds` violation (12 thousand states) |

`live_no_leader` shows what a leaderless read rests on: a read whose index is a slot that only a
minority voted waits until a leader finishes that slot.

### `campaign-discovery/`: how a campaign learns the matchmaker set in force, for #1034 (campaign asks the pool)

The model is one candidate and a chain of matchmaker generations. A handover moves the
generation in force to the next set. A matchmaker outside the set in force can be gone for good.
A frozen matchmaker can learn its successor (`Stopped { successor }`). The candidate believes a
generation, for example the birth set of its tenant, and campaigns. A matchmaker that knows a
later generation refuses with a `Generation` refusal that names it, and the candidate adopts it.
A matchmaker active in the believed generation registers the campaign. It answers one question:
can a campaign whose believed set is gone for good still learn the set in force?

Constants select the rule:

| Constant | Values |
|---|---|
| `Discover` | `TRUE`: a re-ask on an election timeout also asks the matchmaker pool outside the believed set (#1034). `FALSE`: it asks the believed set only (`main` before #1034). |
| `JudgeGeneration` | `TRUE`: a matchmaker registers only a campaign that names its own active generation. `FALSE`: a control, it registers any campaign. |

The invariants are `AdoptInForce` (the candidate never believes a generation that is not in
force yet), `RegInForce` (a registration names the generation in force) and `NotStuck` (a
candidate behind the generation in force can still learn it from a matchmaker it asks).

Two chains: `Witness`, the sets of the sim witness of #1034 (`{A}`, then `{C}`, then `{B, C}`),
and `Wide`, four sets of three where each handover replaces one member, so that the last set
shares no member with the first.

Results (`campaign-discovery/checks.txt`):

| Rule | Chain | Result |
|---|---|---|
| Re-ask the believed set only | `Witness`, `Wide` | violation (`NotStuck`) |
| Register any campaign (control) | `Witness` | violation (`RegInForce`) |
| Re-ask the pool too (#1034) | `Witness`, `Wide` | pass |

With three sets of three in `Wide`, the believed-only rule passes: one member of the first set
stays in every set and tells the candidate. The stall needs a chain whose set in force shares no
live member with the believed set.

Not in the model: ballots, votes, Phase 1, the contents of a registration, a matchmaker that
loses its state, more than one candidate, and a pool that does not hold every set.

### `decree/`: single-decree Paxos, for #1186 (rework 1.15)

The model of `decree.rs` (`Decree<Id, V>`): the shared `Proposer` and `Acceptor` over a log of
one slot. It is Lamport's message-passing Paxos (`Paxos.tla`): a message can arrive late, two
times or never, each ballot has one proposer, and a ballot that a `Nack` preempted only stops.
The caller's retry is a higher ballot.

The two uses of a decree are two sets of constants:

- **The matchmaker handover** (`matchmaker/reconfigurer.rs`): the successor set, over three
  matchmakers under a majority, with `Phase1 = "quorum"`.
- **`cell init`** (#277 (cell init as a decree)): the cell plan, over three listed members
  under a majority, with `Phase1 = "every"` (`Phase1Rule::EveryAcceptor`), and wipes.

| Constant | Values |
|---|---|
| `Phase1` | `"quorum"`: Phase 2 opens at a Phase-1 quorum of promises. `"every"`: Phase 2 opens only when every acceptor promised. |
| `Pick` | `"highest"`: the proposer takes the value of the highest vote that the promises report, else any value (P2c). `"ignore"`: a control, the proposer takes any value. |
| `Wipes` | How many acceptors can lose their disk, each one time. A wiped acceptor answers Phase 1 as a new acceptor, with no promise and no vote. |
| `Refuse` | `TRUE`: a wiped acceptor is refused at boot as amnesia (`docs/architecture/05-failure-model-and-zones.md`), and it takes no step: the rule of the matchmaker handover. `FALSE`: it answers as `Identity` says. |
| `Identity` | `TRUE`: a wiped acceptor never votes again. It answers with a new node id, and the plan names the old one (`not_a_member`, `docs/architecture/03-1-cell-tenant-and-bootstrap.md`). `FALSE`: it votes again as the old acceptor. This is all that `decree.rs` alone can see. |
| `Down` | The acceptors that never take a step (the liveness checks). |

The safety invariant is `Agreement`: one value is chosen, one time. A value is chosen when a
quorum sent its `2b` at one ballot, so a wipe cannot unchoose it. The liveness property is
`Ends` under `LiveSpec`: the last ballot is the retry that no higher ballot preempts, and its
proposer and each acceptor that is not down keep taking their steps.

Results (`decree/checks.txt`, three ballots, two values, all in CI, 35 seconds together):

| Check | Use | Rule | Result |
|---|---|---|---|
| `handover_reach` | handover | `NoChoice`, a reachability control | violation |
| `handover_ignore` | handover | the proposer ignores the highest reported vote | `Agreement` violation |
| `handover` | handover | P2c | pass (17 thousand states) |
| `handover_lost` | handover | two of three matchmakers down | `Ends` violation |
| `handover_live` | handover | one of three matchmakers down | pass |
| `handover_wipe` | handover | one wipe, the wiped matchmaker refused at boot (`Refuse`); its control is `init_quorum`, where the wiped member answers Phase 1 again | pass (65 thousand states) |
| `init_reach` | cell init | `NoChoiceAfterWipe`, a reachability control: a ballot above the first chooses after a wipe | violation |
| `init_quorum` | cell init | Phase 1 at a majority, one wipe | `Agreement` violation |
| `init` | cell init | Phase 1 at every member, one wipe | pass (13 thousand states) |
| `init_two` | cell init | the same, two wipes of three | pass |
| `init_revote` | cell init | a wiped member votes again, one wipe | pass |
| `init_revote_two` | cell init | a wiped member votes again, two wipes of three | `Agreement` violation |
| `init_down` | cell init | one member down, no wipe | `Ends` violation |
| `init_live` | cell init | one wipe, no member down | pass |

The trace of `init_quorum`: ballot 1 chooses `v1` at `{a1, a2}`. `a1` is wiped. Ballot 2 hears
`a1` (no vote) and `a3` (no vote), a majority, and proposes `v2`. `a2` and `a3` accept it, and
`v2` is chosen too. With `Phase1 = "every"`, ballot 2 waits for `a2`, which reports `v1`.

What the checks show about the two rules of `cell init`. `EveryAcceptor` alone is safe while
fewer than a majority of the members are wiped (`init_revote`): one member of each chosen
quorum keeps its vote and reports it. When a majority is wiped, the chosen value can be gone
from every disk (`init_revote_two`), and the identity rule is what keeps the decree safe: a
wiped member never votes, so no second quorum forms (`init_two`). The decree then cannot end,
which is the `cell_lost` refusal of the design. `EveryAcceptor` costs liveness: one member that
never answers blocks Phase 1 (`init_down`), and a wiped member that answers does not
(`init_live`).

Not in the model: the ballot floor of a retry (a ballot above every `Nack`), the matchmaker
generation that fences a handover, the stop quorum that opens the handover's decree (it is in
`experiments/stale-stopped/`, #1229 (a stale `Stopped` reply reuses a decree ballot)), the
`cell_lost` refusal, and more than three acceptors.

### `multi-paxos/`: plain Multi-Paxos, for #1185 (rework 1.14)

The model of plain Multi-Paxos as `ColocatedNode` runs it with no matchmaker: one static
configuration, a log of slots, and nodes that are a proposer, an acceptor and a learner at once.
A message can arrive late, two times or never. Each ballot has one owner, as `(round, node)` in
the code, and a preempted ballot only stops. The model has:

- **Phase 1 that recovers the suffix** (`node/election.rs`). A campaign asks from its first
  unlearned slot `from`, and a promise reports each vote at or above `from`
  (`Acceptor::promise_page`).
- **The candidate is its own first acceptor**: it promises its ballot when it opens Phase 1,
  and its promise is in the quorum that it wins on (`Proposer::open_phase1` takes its records).
- **The allocator frontier** of a new leader (`try_become_leader`): one past the highest slot
  that its Phase 1 reported, never below `from`.
- **The recovery** (`RecoveryPolicy::Phase1Backed`): each slot from `from` to the frontier gets
  the highest reported vote again, or a `Noop` when no promise reported a vote (the election gap
  fill).
- **Phase 2** for fresh commands at the frontier (`Proposer::allocate`).
- **Commit and catch-up.** The leader learns a slot at a Phase-2 quorum of its ballot
  (`node/learn.rs`). A node learns it from the leader's `Commit`, or catches up from any peer
  that learned it (`node/catch_up.rs`; the answer carries the peer's record of each slot).
- **A learn is a record** (`mark_chosen`): the node raises its promise to the ballot that chose
  the slot, and keeps the chosen value as its vote, unless it holds that value at a ballot at or
  above. A promise raised past the ballot of a campaign of the node ends the campaign.
- **A restart that keeps only durable state**: the promise and the votes on disk, and a prefix
  of what the node learned. The code rebuilds the chosen prefix up to its chosen index, and a
  relaxed write of the chosen index (`MustSync::Relaxed`) can be lost, so the kept prefix can
  be any prefix. Every ballot of the node dies with it.
- **The quorum system as a constant** (`membership/quorum_system.rs`): `Q1`, and `Q2` for each
  slot. `MC.tla` builds the majority and takes the grid from `grid/Grid.tla` (linked): a slot's
  Phase 2 goes to column `slot % cols` (`GridQ2At`), and `GridQ1` elects. The checks whose name
  has `any` or `move` let a slot be chosen in any column (`AnyQ2`): the code can move an open
  round to another column (`move_round`) or name its column (`propose_in`).

| Constant | Values |
|---|---|
| `Alloc` | `"frontier"`: fresh commands start at the frontier. `"prefix"`: a control, fresh commands start at `from`. |
| `Gap` | `"noop"`: an unreported slot below the frontier gets a `Noop`. `"skip"`: a control, it gets nothing. |
| `Durable` | `"before"`: an acceptor answers only when its promise or vote is on disk (persist before send, `Ready`). `"after"`: a control, it answers first and writes later. |
| `Restarts` | How many restarts can happen in the run. |
| `Wipes`, `Identity` | How many acceptors can lose their disk. With `Identity = TRUE`, a wiped acceptor never takes a step again: it comes back with a new node id, outside the static configuration. With `FALSE`, a control, it answers again as the old acceptor. |
| `Learners` | The nodes that catch up. The ballot owners always learn what they decide. A node that owns no ballot only adds states when it catches up, so most checks leave it out. |
| `Down` | The acceptors that never take a step (the liveness checks). |
| `Pick` | `"highest"`: Phase 1 takes the report of the highest ballot at a slot (`merge_report`). `"lowest"`: a control, it takes the lowest. Two reports of different ballots at one slot need three ballots. |
| `Restartable` | The nodes that may restart. A liveness check leaves out the owner of the last ballot, whose restart ends its ballot. |

The safety invariants are `Agreement` (one command for each slot), `LearnedChosen` (what a node
learned was chosen), `OneProposal` (one proposal for each slot and ballot: a new leader
allocates above each slot that its Phase 1 reported, so a fresh command never lands on a slot
that its recovery proposes) and `LearnRecordsHeld` (a learned slot holds its record: the
code's assertion "a chosen slot holds its authoritative accepted record"). A value is chosen when a Phase-2 quorum of the slot sent its `2b`
at one ballot, so a restart cannot unchoose it. The liveness property is `Ends` under
`LiveSpec`: the last ballot is the leader that no higher ballot preempts, and it learns every
slot below its frontier and every slot that it proposed. `Spreads` adds that each live node
learns what that leader learned.

Results (`multi-paxos/checks.txt`, three acceptors unless the line says otherwise, two ballots,
two slots, one command and `Noop`; the CI set runs in about nine minutes):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach` | ci | `NoChoice`, a reachability control | violation |
| `reach_gap` | ci | `NoNoopChosen`, a reachability control: a gap fill is chosen below a chosen command | violation |
| `majority` | ci | the rules of the code | pass (470 thousand states) |
| `three_ballots` | ci | three ballots, one slot, two commands: two reports of different ballots meet at one slot | pass (108 thousand states) |
| `three_ballots_lowest` | ci | the same, Phase 1 takes the lowest reported ballot | `Agreement` violation |
| `three_ballots_restart` | ci | the same, `a1` owns ballots 1 and 3, one restart: the old leader campaigns again over its own durable log | pass (558 thousand states) |
| `learn_record` | ci | `NoLearnRecord`, a reachability control: a node learned a slot that it never voted for, so its record comes from the learn | violation |
| `full_grid_any` | ci | 2 x 2 full grid, a slot chosen in any column, one slot, two commands | pass (79 thousand states) |
| `alloc_prefix` | ci | fresh commands start at `from`, two commands | `Agreement` violation |
| `durable_after` | ci | an acceptor answers before its write is on disk, one restart | `Agreement` violation |
| `wipe_revote` | ci | a wiped acceptor votes again as the old one, one wipe | `Agreement` violation |
| `live` | ci | the rules of the code | `Ends` pass |
| `live_down` | ci | one of three acceptors down | `Ends` pass |
| `live_restart` | ci | one restart during the run, of a node that is not the last leader | `Ends` pass (755 thousand states, 3 min) |
| `lost` | ci | two of three acceptors down | `Ends` violation |
| `gap_skip` | ci | no `Noop` for a gap | `Ends` violation |
| `full_grid_one_column` | ci | 2 x 2 full grid, a Phase-1 quorum in one column | `Agreement` violation |
| `full_grid_down` | ci | 2 x 2 full grid, one acceptor down: its column cannot decide | `Ends` violation |
| `full_grid_down_move` | ci | the same, a slot may be chosen in any column: the other column decides (what `move_round` restores) | `Ends` pass |
| `partitions_down` | ci | 3 x 2 partitioned grid (F = 1), one acceptor down in each partition | `Ends` pass |
| `live_wipe` | long | one wipe and one restart, of nodes that are not the last leader, the identity rule | `Ends` pass (1.8 million states, 13 min with 2 workers) |
| `live_wipe_two` | ci | two wipes: only the last leader is left | `Ends` violation |
| `restart` | long | one restart: the node keeps a prefix of what it learned | pass (2.3 million states, 3 min) |
| `wipe` | long | one wipe, the identity rule | pass (1.3 million states, 2.5 min) |
| `spreads` | long | every node catches up | `Ends` and `Spreads` pass (0.9 million states, 15 min) |
| `full_grid` | long | 2 x 2 full grid, a slot chosen in any column | pass (11.4 million states, 13 min, before the learn record and the any-column quorums) |
| `partitions` | long, bounded 50 min | 3 x 2 partitioned grid, a slot chosen in any column | no violation in 33 million distinct states (bounded: stopped after 50 min, 21 million states left on the queue) |

The long checks ran once on 2026-11-19 (#1216 (truncation model, part 2)) with 4 workers;
`restart` and `wipe` ran again for #1230 (rules that no check tests).

The traces of the controls (ballot 1 is `a1`'s, ballot 2 is `a2`'s):

- `alloc_prefix`: ballot 1 wins on `{a1, a3}` and proposes `c1` at slot 0; `a1` votes for it.
  Ballot 2 wins on `{a1, a2}`, hears that vote, and proposes `c1` again at slot 0. With
  `Alloc = "prefix"` it also allocates slot 0 to a fresh `c2`: two proposals at one slot and
  ballot. `a3` votes `c1` at ballot 1 and `{a1, a2}` vote `c2` at ballot 2, so both are chosen.
- `durable_after`: ballot 1 wins on `{a1, a3}` and proposes `c1` at slots 0 and 1. `a1` and `a3`
  vote for slot 0, so `c1` is chosen there. `a1` restarts before its vote reached the disk.
  Ballot 2 wins on `{a1, a2}`; `a1` reports only slot 1, so slot 0 gets a `Noop`, and `{a1, a2}`
  choose it.
- `wipe_revote`: a wiped acceptor that votes again as the old one breaks `Agreement` the same
  way, with one wipe of three. Phase 1 takes any majority, so no member of the quorum that chose
  a value has to answer. This is the result of `decree/` (`init_quorum`) again: plain
  Multi-Paxos has no `EveryAcceptor`, so the identity rule (a wiped acceptor never votes again as
  the old one) is what keeps the log safe, from the first wipe.
- `gap_skip`: ballot 1 proposes `c1` at slots 0 and 1, and only `a3` votes, for slot 1. Ballot 2
  wins on all three, hears slot 1 only, and its frontier is 2. With `Gap = "skip"` nobody
  proposes slot 0 again: slot 1 is chosen and slot 0 stays a hole for good.
- `full_grid_one_column`: a Phase-1 quorum inside one column misses the Phase-2 quorums of the
  other column, and two values are chosen at one slot.
- `three_ballots_lowest`: ballot 1 proposes `c1` at slot 0 and only `a3` votes for it. Ballot 2
  wins on `{a1, a2}`, hears no vote, proposes `c2`, and `{a1, a2}` choose it. Ballot 3 hears
  `c1` at ballot 1 from `a3` and `c2` at ballot 2, takes the lower one, and `{a1, a2}` choose
  `c1`: two values at one slot. With two ballots every report at
  one slot has the same ballot, so the rule "the highest reported ballot wins" needs three.

Not in the model: matchmakers and reconfiguration (`zero-stall/`, `config-register/`), the
cooperative handoff (`RecoveryPolicy::Inherited`), the CTRL tri-state of a rotted record and the
repair probe, truncation and the trim-point jump (all three in `truncation/`), the paging of a promise, the proxy leader
(`proxy-leader/`), and a leader that a restart keeps. Three ballots fit in CI with one slot
(`three_ballots*`); the long checks keep two ballots and two slots.

### `proxy-leader/`: the proxy leader, for #1188 (rework 2.9)

The model of the proxy leader of Compartmentalized Paxos as `paros-consensus` builds it
(`proxy_leader.rs`). The leader sequences the commands, and a proxy runs the Phase 2 of a slot
for it: it fans the `Accept` out, folds the `Accepted`s and sends the `Commit`. A proxy decides
nothing. A learner believes a `Commit` with no check of its own (`node/learn.rs`, `on_commit`),
so the safety of the log rests on one rule of the proxy: **it sends `Commit` only on a Phase-2
quorum of the configuration that the round's `Accept` carried, at the round's ballot.** This is
finding 10 of the review of paros-consensus of 2026-11-16.

The model has leaders, proxies and acceptors as three sets. A message can arrive late, two
times or never; a re-send is the same message, so a proxy can read any delegation again. The
model has:

- **A configuration for each ballot** (`Cfg`): a reconfiguration is a round change
  (`ColocatedNode::reconfigure`). Configuration 0 is the bootstrap configuration, the one a proxy
  holds when it boots (`ProxyLeader::new`).
- **Phase 1 over the configuration of every lower ballot** (`H_b` of Matchmaker Paxos, with no
  GC). This Phase 1 is the seal of the older configurations: an acceptor that promised the new
  ballot refuses every lower `Accept`, as the seal of rule 1 does
  (`docs/architecture/04-roles.md`).
- **The delegation** (`Message::Accept` with `reply_to = Party::Proxy`) to `ProxyOf[s]`
  (`ProxyId::of`). With `Move`, the same round can reach a second proxy: a driver can name the
  proxy (`Delegation::To`), and a handoff successor delegates the rounds it inherits again.
  `move_round` itself refuses a delegated round, so `Move` is a superset of the code for safety.
- **The take-back** (`take_back_delegated`): the leader takes a stalled delegated round back,
  sends its `Accept` itself and decides it on a Phase-2 quorum of its ballot's configuration. It
  is how the code stays live when a proxy is gone (`live`).
- **The proxy of the code**: it works for the highest ballot it was handed; a higher one closes
  every lower round (`close_below`), a lower one is ignored. It learns the configuration only
  from a delegation at a higher ballot (`learn_config`). It ignores the re-delegation of a round
  it decided (`done`). A crash loses every round: the proxy boots empty with the bootstrap
  configuration.
- **The quorums as constants** for each configuration: `Members`, `Q1`, and `Q2` for each slot.
  `MC.tla` builds the majority and takes the full grid from `grid/Grid.tla` (linked): the
  `Accept` of a slot goes to its column, and the column decides.

Not in the model: the `Nack` (it only closes a round, and a round that stays open decides
nothing more), the re-send beat and `expire_stale` (with no lost message they change nothing),
and the GC of `H_b`. One command for each ballot and slot is assumed here (`prop[b][s]` is
fixed); `leader-handoff/` checks it (`DurableFrontier`).

| Constant | Values |
|---|---|
| `Tally` | `"round"`: the proxy decides a round over the configuration that the round's `Accept` carried. `"belief"`: it decides over the configuration that it believes now. |
| `Hear` | `"delegation"`: the proxy learns a configuration only from a delegation, which closes every lower round (the code today). `"any"`: it also learns the configuration of any ballot that started, as from the register of rule 1, and keeps its rounds open. |
| `Count` | `"quorum"`: a decision needs a Phase-2 quorum of votes. `"short"`: a control, one vote less is enough. |
| `Move` | `TRUE`: a round can reach another proxy than `ProxyOf[s]`. `FALSE`: it only goes to `ProxyOf[s]`, as with `Delegation::Auto`. |
| `TakeBack` | `TRUE`: the leader may take a delegated round back. `FALSE`: a control for liveness. |
| `CloseBelow` | `TRUE`: a higher delegation closes every lower round of the proxy (`close_below`). `FALSE`: a control, it closes only the round of its own slot. |
| `Crashes` | How many proxy crashes can happen in the run. |
| `Down` | The proxies that never take a step (the liveness checks). |

The safety invariants are `CommitBacked` (each `Commit` is backed by a Phase-2 quorum of its
ballot's configuration, at that ballot), `CommitAgree` (two `Commit`s of one slot carry one
value, so two learners never learn two values) and `Agreement` (one value is chosen for each
slot). The liveness property is `Ends` under `LiveSpec`: the last ballot is the leader that no
higher ballot preempts, one proxy is gone for good, and every slot gets a `Commit`.

**The code is safe, and the rule makes it safe after rule 1 too.** Today a proxy learns a
configuration only from a delegation, and that delegation closes every lower round, so the
configuration it believes is always the configuration of its open rounds (`code` and
`code_slots`, `Tally = "belief"`, pass). With one slot a higher delegation replaces the only
round anyway, so the close of the other slots is checked by `code_slots` (two slots), and its
control `code_slots_noclose` is red. Rule 1 (step 2.5) lets a node learn its configuration from the register.
If a proxy learns it there and keeps its rounds, a tally over its belief is unsafe
(`belief_any`), and a tally over the round's own configuration is safe (`safety`). This is the
change that finding 10 proposes: bind the tally of a delegated round to the configuration that
its `Accept` carried. The code needs it before a proxy learns a configuration from anything but
a delegation.

Results (`proxy-leader/checks.txt`; three acceptors, configuration 0 is `{a1, a2, a3}` and
configuration 1 is `{a1}`; ballot 1 is `l1`'s on configuration 0, ballot 2 is `l2`'s on
configuration 1, ballot 3 is `l1`'s on configuration 0; two proxies; one slot and two commands
unless the line says otherwise; the CI set runs in about 2 minutes; the long checks have no proxy
crash and ran once on 2026-11-19 with 4 workers):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach` | ci | `NoCommit`, a reachability control | violation |
| `reach_sealed` | ci | `NoSealedCommit`, a reachability control: a proxy commits a round of a ballot that a higher ballot sealed, three ballots | violation |
| `reach_moved` | ci | `NoMovedCommit`, a reachability control: a slot moves to the second proxy and it decides, one crash | violation |
| `reach_both` | ci | `NoBothCommit`, a reachability control: two proxies decide the same slot (a round reached a second proxy while the first was slow but alive) | violation |
| `code` | ci | the code: `Tally = "belief"`, `Hear = "delegation"`, one crash | pass (26 thousand states) |
| `code_slots` | ci | the code, two slots, one command, no crash, no second proxy | pass (254 thousand states, 1 min) |
| `code_slots_noclose` | ci | the same, a higher delegation does not close the round of the other slot | `CommitBacked` violation |
| `safety` | ci | the rule: `Tally = "round"`, `Hear = "any"`, one crash | pass (107 thousand states) |
| `grid` | ci | the rule, configuration 1 is the 2 x 2 full grid of `a1`..`a4`, one crash | pass (147 thousand states) |
| `belief_any` | ci | `Tally = "belief"`, `Hear = "any"`: the proxy counts over a configuration it did not check | `CommitBacked` violation |
| `belief_any_agree` | ci | the same | `CommitAgree` violation |
| `short` | ci | `Count = "short"`: a `Commit` one vote short of a quorum | `CommitBacked` violation |
| `short_agree` | ci | the same | `CommitAgree` violation |
| `live` | ci | `p1` down, the slot only goes to `p1`, the leader takes it back (the code) | `Ends` pass |
| `live_move` | ci | `p1` down, no take-back, the slot reaches `p2` | `Ends` pass |
| `live_nomove` | ci | `p1` down, no take-back, the slot only goes to `p1` | `Ends` violation |
| `code_three` | long | the code, three ballots | pass (1.5 million states, 2 min) |
| `safety_three` | long | the rule, three ballots | pass (4.8 million states, 6 min) |
| `slots` | long | the rule, two slots, one command: a higher delegation closes the round of the other slot | pass (12.3 million states, 14 min) |

The traces of the controls:

- `belief_any_agree`: ballot 2 wins on `{a2, a3}`, a Phase-1 quorum of configuration 0, and
  hears no vote. Ballot 1 proposes `c1` and ballot 2 proposes `c2`. `p1` opens the round of
  ballot 1, then hears the configuration of ballot 2 and keeps the round. `a1` votes `c1` at
  ballot 1, and `p1` counts that one vote as a quorum of `{a1}`: a `Commit` of `c1` that no
  quorum of configuration 0 backs. `p1` then opens the round of ballot 2, `a1` votes `c2`, and
  `p1` commits `c2`. Two learners learn two values.
- `short_agree`: ballot 2 wins on `{a1, a2}` and hears no vote. `a3` votes `c1` at ballot 1,
  and `p1` commits it one vote short of a majority. Ballot 2 then commits `c2`.
- `code_slots_noclose`: `p1` opens the round of ballot 1 at slot 0, then takes the delegation of
  ballot 2 at slot 1 and believes configuration `{a1}` while its round of slot 0 stays open.
  `a1` votes at ballot 1, and `p1` commits slot 0 on that one vote.
- `live_nomove`: `p1` is gone, the slot only goes to `p1`, the leader never takes it back, and
  no `Commit` comes.

### `leader-handoff/`: the cooperative handoff of DPaxos, for #1187 (rework 2.8)

The model is plain Multi-Paxos where the leadership is a state of a node, so an elected leader
can give its ballot, its allocator frontier and its tail (the decided slots and the open rounds)
to a successor with no Phase 1 (`node/handoff.rs`: `relinquish_to`, `on_relinquish`). A payload
can arrive late, two times or never; a node can restart; a successor can resign on its fence; a
later ballot can win an election. The quorums come from `grid/Grid.tla`: a majority, or a 2 x 2
full grid where slot `s` uses column `s % 2` for Phase 2.

What the model follows in the code:

- A learn is a record (`mark_chosen`, `node/learn.rs`): `Decide`, `CatchUp` and the decided
  slots of a payload raise the promise to the ballot that chose the slot, and keep the chosen
  value as the vote unless the node holds it at a ballot at or above.
- A leader records each round that it opens as its own vote (`record_own_round`), in a grid
  column it is not in too. A leader whose promise a higher ballot passed still opens a round and
  records nothing (`AllocateStale`, `node/phase2.rs`): the promise guard of the successor is then
  the only cover against a replayed payload.
- The successor installs a payload only when the payload names it, its ballot is at or above the
  promise, the successor does not hold that ballot already, and the payload's frontier is at or
  above the successor's frontier and first unlearned slot. It proposes each pending slot again and
  fills no gap with a `Noop` (`RecoveryPolicy::Inherited`).

Each rule has a constant whose first value is the code, and a control:

| Constant | Rule (code) | Control |
|---|---|---|
| `OneHop` | only the elected minter of a ballot hands it on | `two_hops`: `OneHolder` breaks |
| `FrontierGuard` | a successor refuses a payload below its frontier | `replay`: `OneProposal` breaks |
| `DurableFrontier` | a leader records its own rounds | `volatile_frontier`: `OneProposal` breaks |
| `Pending` | `"keep"`: the successor proposes the pending slots again | `live_drop_pending`: `Ends` breaks |
| `BeatGuard` | `TRUE`: a beat at the leader's own ballot keeps it leader (rework 2.7; the code of today has `FALSE`) | `live_late_beat`: `Ends` breaks |
| `PromiseGuard` | a successor refuses a payload below its promise | `promise_guard_off`: `OneProposal` breaks in 11 states (ballot 1 hands on to a node that promised ballot 2; the successor opens slot 0 with no record, restarts, installs the replayed payload and opens slot 0 again with another command) |
| `RecordAll` | a leader records every round, in a column it is not in too | `grid_in_column_only`: `OneProposal` breaks in 10 states on the grid (the successor opens a slot of the other column with no record, restarts, installs the replay and opens it again) |
| `Abdicate` | the leader steps down in the step that sends the payload | `keep_leading`: `OneHolder` breaks in 6 states |
| `Named` | only the named successor installs a payload | `any_target`: `OneHolder` breaks in 7 states |

The invariants are `Agreement`, `LearnedChosen`, `OneProposal` (one proposal for each slot and
ballot, across the handoff), `OneHolder` (one node at a time leads at a ballot), and three
assertions of the code: `LeaderFrontier` (the frontier of a leader that can use its ballot covers
its first unlearned slot; `relinquish_to` asserts it), `OwnRecorded` (each open round of such a
leader is its own vote) and `LearnRecordsHeld` (a learned slot holds its record). `Ends` is the
liveness check: a handoff ends, and an election replaces a successor that is gone.

Results (`leader-handoff/checks.txt`):

| Check | Shape | Result |
|---|---|---|
| `reach` | `NoHandoffLearns`, a reachability control: a successor learns a slot | violation (8 states) |
| `safety` | NB 1, NS 2, two commands, one restart | pass (875 thousand states) |
| `safety_elect` | NB 2, NS 1, one restart, a later election | pass (2.5 million states) |
| `late_beat_safe` | `safety` with the beat of today's code (`BeatGuard = FALSE`) | pass (950 thousand states) |
| `live`, `live_gone` | `Ends` with a lost `Accept`, and with a lost payload or a restarted successor | pass (115 thousand, 441 thousand states) |
| `grid` (`long`) | the 2 x 2 full grid, NB 1, NS 2, no restart | pass (2.5 million states, 7 minutes) |
| `owner_twice` (`long`) | `safety_elect` where one node owns every ballot: the predecessor campaigns again after it hands on | pass (2.7 million states, 6 minutes) |
| `two_ballots` (`long`, bounded at 60 minutes) | NB 2, NS 2: a tail with a decided and a pending slot meets an election with its gap fill | no violation in 5.4 million distinct states, to depth 17 (a first run, stopped after 13 minutes) |
| `restart_twice` (`long`) | `safety` with two restarts | pass (1.9 million states, 9 minutes with 1 worker) |
| `safety_long` (`long`) | NB 2, NS 1, two commands | pass |

Not in the model: the paged recovery of the successor (`HANDOFF_BATCH`: the model installs the
whole payload in one step), the matchmaker `config` and `prior` of a payload, proxies and
delegation, the `rejected_unfit` refusal, and the read fence. Because the payload holds the whole
tail and a `2b` reaches every node, the stall that `tick_handoff_fence` exists for (a decision
that only the departed leader knew) cannot happen in the model: `FenceResign` is reachable
(`replay` uses it) but no check needs it. The code ships the whole tail in one page too
(`HANDOFF_BATCH`: a leader whose tail is longer is refused a handoff), so the model has no
paged recovery to add. `Quiet` checks it under elections that wait for a leader to be gone (the
beats hold the timeout of every other node): with no resignation, the run still ends after a
lost payload or a restarted successor (`live_quiet_no_resign`, 416 thousand states, `long`)
and after a lost `Accept` (`live_quiet_2a`, 838 thousand states, `long`), because the successor
proposes each open round again. Finding of #1230 (rules no check tests): in the model,
`tick_handoff_fence` is a defense that no schedule needs. A resignation with no timer also
loops in the model (the successor resigns, installs the same payload again, and resigns), so
the model leaves `FenceResign` unfair.

### `matchmaker/`: single-decree Matchmaker Paxos, for #1200 (TLA+ model Matchmaker Paxos)

The model is single-decree Matchmaker Paxos as the paper gives it (Whittaker et al., §3) and as
`paros-consensus` builds it (`matchmaking.rs`, `matchmaker.rs`, `collector.rs`). It is the base
model of the matchmaker flavor: `retire-fence/` (how a node learns that it may retire),
`handover-gc/` and the handover of #1097 (the matchmaker set itself), `abandoned-ballot/` (a
candidate drops a registration) and `zero-stall/` (Phase-1 bypassing in Multi-Paxos) each extend
one part of it.

Each ballot has one proposer, and the proposer selects one configuration for its ballot. A
reconfiguration is only a ballot whose configuration is new. A ballot runs three phases:

1. Matchmaking. The proposer registers `(b, C_b)` at the matchmakers. A matchmaker refuses `b`
   if it holds a registration at `b` or above, or if `b` is below its GC watermark. Else it
   records `(b, C_b)` and answers with its registrations below `b` and its watermark. The
   proposer closes on the answers of a matchmaker majority. `H_b` is the union of the answers,
   above the maximum watermark.
2. Phase 1 needs a promise quorum (a majority) of every configuration in `H_b`. An empty `H_b`
   needs no Phase 1.
3. Phase 2 addresses `C_b` alone.

GC follows §3.4 and §3.5 of the paper. The proposer of `b` asks the matchmakers to forget the
ballots below `b` when its value is chosen at `b` (scenario 1) or when its Phase 1 found no vote
(scenario 2). A matchmaker raises its watermark to `b`, deletes its registrations below `b` and
acks. The floor `b` is effective once a matchmaker majority acked it (`GcStep::Effective`). Only
then may an acceptor that no registration at `b` or above names retire, and a retired acceptor
takes no more steps. Until then a removed acceptor keeps answering Phase 1: the acceptor guards
are pool-based. The retirement takes its two facts from the global state: no registration at the
floor or above names the acceptor, and every matchmaker refuses a later registration that names
it. That is the `allTomb` rule of `retire-fence/`, which checks how a node learns those facts.

Constants select the rules. The first value of each is the rule of the paper and of the code.

| Constant | Values |
|---|---|
| `Phase1` | `"every"`: a Phase-1 quorum of each configuration in `H_b`. `"union"`: a majority of the union of `H_b`. `"own"`: a majority of `C_b` alone. |
| `MatchCheck` | `TRUE`: a matchmaker refuses `b` when it holds a registration at `b` or above. `FALSE`: it registers `b` anyway. |
| `Histories` | `"quorum"`: the proposer closes on a matchmaker majority. `"one"`: it closes on one answer. |
| `Reconfig` | `"round"`: Phase 2 addresses `C_b`. `"edit"`: the proposer may send its Phase 2 to another configuration, with no new ballot. |
| `Gc` | `"paper"`: scenarios 1 and 2. `"early"`: the proposer asks for GC as soon as its Phase 1 is done. `"off"`: no GC. |
| `Retire` | `"effective"`: an acceptor retires above an effective floor. `"oneAck"`: the floor counts as effective at one ack. `"removed"`: an acceptor retires when the newest configuration does not name it. `"off"`: no retirement. |
| `Watermark` | `"max"`: a history keeps the entries at or above the maximum watermark of the answers. `"min"`: a control, it keeps those at or above the minimum, so an entry that one matchmaker of the quorum collected comes back from another. |
| `Tomb` | `TRUE`: a matchmaker refuses a configuration that names a retired acceptor (`MatchRefusal::Retired`). `FALSE`: a control, it registers it. |

The invariants are `Agreement` (one value is chosen) and `Electable` (whatever matchmaker
majority a new ballot hears, each configuration of its history keeps a Phase-1 quorum that did
not retire; the same name as in `retire-fence/`). `Electable` reads the history through
`Union`, the rule that `CloseMatch` uses, so a wrong watermark filter breaks it
(`watermark_min`).

The watermark refusal of a matchmaker (`b` below its watermark, `BelowWatermark`) has no
control: removing it changes no state of `gc_two`. The registration check implies it here: the
highest ballot that closed matchmaking keeps its registration at a quorum, and the check refuses
every lower ballot there.

The pool has five acceptors and two configurations, `A = {a1, a2, a3}` and `B = {a3, a4, a5}`.
A move from `A` to `B` replaces `a1` and `a2`, and a majority of the union, `{a3, a4, a5}`, holds
no Phase-1 quorum of `A`. Each ballot selects `A` or `B`. Two values. The controls use one
matchmaker where the bug does not need more, two for `histories_one` and three for
`retire_one_ack`. The checks whose name starts with `gc_two`, and `reach_drop`, `watermark_min`
and `retire_no_tomb`, use the smallest move, `A1 = {a1}` to `B1 = {a2}`: with it, two
matchmakers and three ballots finish in seconds, and GC meets a real matchmaker quorum. The
check `matchmaker` (two ballots) cannot see a GC bug: with `Gc = "early"` it still passes,
because the floor of ballot 2 has no later ballot to hurt.

Results (`matchmaker/checks.txt`):

| Check | Rule | Result |
|---|---|---|
| `reach` | `NoChoiceAboveRetired`, a reachability control: a value chosen at a ballot is chosen again at a higher ballot in the other configuration, and an acceptor that voted for it at the lower ballot retired | violation |
| `reach_drop` | `NoDrop`, a reachability control: a matchmaker quorum answers a ballot with an entry below the maximum watermark of its answers, and `Union` drops it | violation |
| `phase1_own` | control: Phase 1 in `C_b` alone, two ballots | `Agreement` violation |
| `phase1_union` | control: Phase 1 at a majority of the union of `H_b`, three ballots | `Agreement` violation |
| `match_nocheck` | control: a matchmaker registers a ballot below one that it holds, two ballots | `Agreement` violation |
| `histories_one` | control: the proposer closes on one answer, two matchmakers | `Agreement` violation |
| `edit` | control: Phase 2 goes to a configuration that the ballot did not register | `Agreement` violation |
| `gc_early` | control: GC as soon as Phase 1 is done, three ballots | `Agreement` violation (1.4 million states) |
| `retire_removed` | control: an acceptor retires when the newest configuration does not name it | `Electable` violation |
| `retire_one_ack` | control: the floor counts as effective at one ack, three matchmakers | `Electable` violation |
| `gc_two` | the rules, two matchmakers, three ballots, the smallest move | pass: `Agreement` and `Electable` (56 thousand states) |
| `gc_two_early` | control: `gc_two` with GC as soon as Phase 1 is done | `Agreement` violation |
| `watermark_min` | control: a history keeps the entries above the minimum watermark, three matchmakers | `Electable` violation |
| `retire_no_tomb` | control: a matchmaker registers a configuration that names a retired acceptor | `Electable` violation |
| `matchmaker` | the rules, three matchmakers, two ballots | pass: `Agreement` and `Electable` (1.4 million states, 2.5 minutes) |
| `matchmaker_one` | the rules, one matchmaker, three ballots | pass (6.2 million states, 6.5 minutes, `long`) |
| `matchmaker_long` | the rules, three matchmakers, three ballots | no violation in 65.1 million distinct states, to depth 26 (a bounded run of 2 hours, 4 workers; `long`) |

The trace of `phase1_union`: ballot 1 (`A`, empty history) chooses `v1` at `{a2, a3}`. Ballot 2
registers `B`, so the history of ballot 3 holds `A` and `B`. Ballot 3 takes promises from
`{a1, a4, a5}`, a majority of the union of five, and none of them voted. It proposes `v2`, and
`{a1, a2}` choose it. With `Phase1 = "every"`, ballot 3 needs two promises of `A`, and one of them
reports `v1`.

The trace of `gc_early`: ballot 1 (`B`) chooses `v1` at `{a4, a5}`. Ballot 2 (`A`, history `{B}`)
takes promises of `B`, finds `v1` and asks for GC before its own Phase 2. The floor 2 deletes the
registration of ballot 1. Ballot 3 (`A`) gets the history `{A}` (the registration of ballot 2),
takes promises from `{a1, a2}`, which did not vote, and chooses `v2`. Scenario 1 of the paper
waits until the value of ballot 2 is chosen in `A`, so ballot 3 finds it there.

The trace of `retire_one_ack`: ballot 1 registers `A` at two matchmakers. Ballot 2 registers
`B`; its history `{A}` is not empty, but its Phase 1 finds no vote, so it asks for GC. One
matchmaker acks, and `a1` and `a2` retire. A new ballot that hears the two other matchmakers gets
`A` in its history, and `A` has one live acceptor.

Not in the model: Multi-Paxos (more than one slot), the matchmaker generations and the handover
(`handover-gc/`, `handover/`), Phase-1 bypassing (`zero-stall/`), scenario 3 of the GC (the chosen
value on replicas), the effective configuration and the membership probe, lost durable state,
how a node learns that it may retire (`retire-fence/`), the owner testimony (`abandoned-ballot/`),
and liveness.

### `grid/`: the quorums of a grid, for #1102 (rework 1.13)

`Grid.tla` is the quorums of `QuorumSystem::Grid { rows, cols, acks }`
(`membership/quorum_system.rs`) as sets, so the models that run a grid and the code agree on one
layout. A configuration is the tuple of its members in sorted order, laid out row-major: member
`i` sits at row `(i - 1) \div cols` and column `(i - 1) % cols`.

- Phase 2: `acks` acceptors of one column.
- Phase 1: `rows - acks + 1` acceptors of every column.
- A read: one row when `acks = rows` (the full grid, `row_of`), the whole membership else. It
  completes on a Phase-1 quorum of the answers.

The full grid of Compartmentalized Paxos is `acks = rows`. The partitions of #532 (acceptor
partitions) are `rows = 2F + 1` and `acks = F + 1`. `config-register/`, `reads/` and `multi-paxos/` take their
grids from this module; a later model of the journal's grids can take them too.

`GridCheck.tla` checks the arithmetic of the code on every layout of one to nine members, each
layout one initial state:

| Check | Rule | Result |
|---|---|---|
| `grid` | `CrossIntersect`: every Phase-1 quorum meets every Phase-2 quorum (`cross_intersects`). `ReadsArePhase1`: each read target holds a Phase-1 quorum. `ToleratedIsExact`: `tolerated_failures`, `min(acks - 1, cols * (rows - acks + 1) - 1)`, is the largest number of failures, wherever they are, that leaves a Phase-1 and a Phase-2 quorum. `PerColumnTolerance` (#1257 (quorum everywhere)): `f` down in every column leaves both quorums exactly when `f + 1 <= acks <= rows - f` | pass (69 layouts) |
| `grid_slack` | control: Phase 1 takes one acceptor less of each column, layouts of four members or more (on one member the empty set would be the first quorum) | `CrossIntersect` violation |
| `grid_full_rows` | `Phase1Meets` on the full grids with two columns or more: two Phase-1 quorums meet | violation: two rows of a 2 x 2 grid do not meet |

`grid` also checks `Tight` (with one acceptor less of each column, some Phase-1 quorum misses
some Phase-2 quorum, on every layout: `rows - acks + 1` is the smallest count) and `Admits`
(every layout tiles its members, `QuorumSystem::admits`). Each `MC.tla` that takes a grid from
this module, and each trace spec, assumes `GridAdmits` of its layouts, so a layout that does not
tile (five members on two columns) is refused before TLC runs.

`Phase1Meets` is not a rule of Paxos. It is the fact that the read of `reads/grid_full` needs:
two Phase-1 quorums of one configuration meet when `2 * (rows - acks + 1) > rows`, which holds
on the partitions and not on the full grid.


### `compartments/`: the batcher, the replica and the read replica, for #1190 (rework 6.10)

The model is the other roles of Compartmentalized Paxos as `paros-consensus` builds them: the
batcher and the unbatcher (`batcher.rs`), the walk and the journal fold of the replica
(`replica.rs`), and the read replica with its leaderless read (`replica_node.rs`,
`quorum_read.rs`). The proxy leader has its own model (`proxy-leader/`, #1188 (rework 2.9)).

The log under the roles is abstract, because `multi-paxos/` checks it: one value for each slot
and one proposal for each slot and ballot. Here a slot holds one proposal, and a Phase-2 quorum
chooses it. A leader change does to each open slot what `multi-paxos/` allows: the slot keeps its
proposal (the recovery proposes the highest reported vote again), or it gets a `Noop` (no promise
reported a vote), or, above the new frontier, it is free again. The model has:

- **The batcher** (`Batcher`): one lane of one multi-writer journal. A writer pushes a write
  while the lane fits the batcher's record limit, and the lane flushes as one batch at any time
  (`flush_records`, the tick). A batch is one unfenced `Write` to the leader, and the transport
  delivers a request at most one time (with `Copies`, a few more times).
- **The edge refusal**: the leader refuses a merged batch over its record limit (`TooLarge`)
  before it allocates a slot. The batcher lowers its limit (`lower_limits`) and sends each write
  of the batch again, alone (`docs/architecture/02-data-plane.md` §2.4).
- **The held answer**: the leader allocates the frontier slot, and answers once the slot and
  every slot below it are chosen, with the `seq` that its fold gives the batch, and only when the
  slot chose the batch that the leader proposed there. A new leader holds no answer of the old
  one.
- **The deposed leader** (#1232 (held writes)): the old leader stays up and keeps its held
  answers. It answers a held batch `Unknown` at any time, or `ok` when the slot chose that batch.
  It never answers `NotLeader` for a batch that holds a slot. The batcher treats `Unknown` as an
  ambiguous batch. A single writer resolves an `Unknown` with a resend at the same
  `expected_seq`; `multi-writer/` checks that half (`AckTrue`, `RefusalFinal`, `ExactlyOnce`).
- **The unbatcher** (`Unbatcher::scatter`) cuts the answer in batch order, so each write gets
  its own dense range.
- **An ambiguous batch**: a batch with no answer in time is answered unanswered, write by
  write, and the batcher never sends it again (§2.4). A late answer is dropped.
- **The `Accept`s sent again** (`resend_pending`): the `Accept`s of an open slot can be lost,
  and the leader sends them again.
- **The replica walk** (`Replica::advance`): a replica learns a chosen slot from a `Commit` or a
  catch-up, folds its chosen prefix in slot order, and the journal state gives a write
  `[next_seq, next_seq + n)` at apply.
- **A replica restart** keeps what the replica learned (`WriteOp::Learned`) and its chosen index
  as far as it reached the disk (the relaxed `SetChosenIndex`). The boot folds the state again
  from that prefix (`Replica::from_boot`).
- **The read replica** is not an acceptor: no step gives it a vote, and it counts in no quorum.
  Its read asks the acceptors for their vote watermarks, closes on a Phase-1 quorum, and is
  served when the replica's own prefix covers the maximum (`ReplicaNode::quorum_read_in`).

| Constant | Values |
|---|---|
| `Resend` | `"never"`: an ambiguous batch is never sent again (the code). `"ambiguous"`: a control, the batcher sends it again. |
| `Accepts` | `TRUE`: the leader sends lost `Accept`s again (the code). `FALSE`: a control, it does not. |
| `Walk` | `"prefix"`: the walk folds only the next slot (the code). `"skip"`: a control, it folds any slot that it learned. |
| `Boot` | `"refold"`: the boot folds the state again from the prefix on disk (the code). `"keep"`: a control, the state survives the restart and the walk starts again at the chosen index on disk. |
| `Read` | `"quorum"`: the leaderless read (the code). `"local"`: a control, the replica serves from its prefix with no watermarks. |
| `RQ` | The sets that close a read: a majority of the acceptors (the code), or the control `WithReplicas`, where a read replica counts as a voter and answers with its own prefix. |
| `LeaderMax`, `BatchMax` | The leader's record limit and the batcher's limit before it hears the leader's. With 3, the merged batch of `w1` (one record) and `w2` (two records) fits; with 2, the leader refuses it. |
| `Held` | What a deposed leader answers for a held batch. `"unknown"`: `Unknown`, or `ok` when the slot chose the batch (the rule, #1232). `"slot"`: a control, it answers from the outcome of the slot, whatever the slot chose. `"notleader"`: a control, it answers `NotLeader`, and the batcher follows the hint and sends the batch again (§2.4). |
| `Copies` | How many times in a run the transport may deliver a batch one more time. 0 in every check but `copies` and `copies_onerange`. |
| `Leaders`, `Restarts`, `Crash` | How many leader changes and replica restarts can happen, and whether the batcher can stop for good. |

The safety invariants:

- `OneRange`: each write of a batch gets its own `seq` range one time at each replica, also when a
  batch is sent again (a `TooLarge` split, an `Accept` sent again, a slot that a new leader
  recovers).
- `OneAnswer`: each write gets one answer, a range or unanswered.
- `AnswerAgrees`: the range that the unbatcher gives a write is the range that each replica
  folds for it.
- `AckedChosen`: the range that the unbatcher gives a write is the range that the fold of the
  chosen log gives it, also when no replica walked the slot yet. An answered write is never lost
  (#1232).
- `InOrder`: a replica folds the chosen log in slot order with no gap, and its state is the fold
  of that prefix.
- `Dense`: the ranges of a replica's state are disjoint and cover `[0, next_seq)`.
- `ReadFresh`: a read that a replica serves holds each write that was complete (answered with a
  range) before the read started. The same rule as `ReadSeesAcked` of `reads/`, on the replica
  tier that `reads/` leaves out.

The liveness property is `Ends` under `LiveSpec`: each batch that the batcher sent reaches the
log, and each slot below the frontier is chosen and folded at each replica, also when the batcher
stopped after it sent the batch. A lost request or `Accept` gets through in the end (strong
fairness on the steps that a loss can disable).

The checks have three acceptors, the writes `w1` and `w2`, one read replica unless the line says
otherwise, and the rules of the code. The rules are checked in three parts, so that each part is
small: `batch` (one leader change and a batcher that can stop, no reads and no restart),
`restart` (one replica restart) and `reads` (one reader).

Results (`compartments/checks.txt`; the CI set runs in about 45 seconds with 4 workers):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach_merged` | ci | `NoMergedChosen`, a reachability control: a merged batch is chosen | violation |
| `reach_split` | ci | `NoSplit`, a reachability control: each write of a refused batch is answered | violation |
| `reach_read` | ci | `NoReadAfterWrite`, a reachability control: a read is served after a write was complete | violation |
| `batch` | ci | the rules of the code | pass (14 thousand states) |
| `batch_split` | ci | the rules of the code, the leader refuses the merged batch | pass (27 thousand states) |
| `restart` | ci | the rules of the code, one replica restart | pass |
| `reads` | ci | the rules of the code, one reader | pass (700 thousand states) |
| `resend_ambiguous` | ci | the batcher sends an ambiguous batch again | `OneRange` violation |
| `reach_deposed_ok` | ci | `NoDeposedOk`, a reachability control: a deposed leader answers a held batch `ok` | violation |
| `held_slot` | ci | a deposed leader answers from the outcome of the slot | `AckedChosen` violation |
| `held_notleader` | ci | a deposed leader answers `NotLeader`, and the batcher sends the batch again | `OneRange` violation |
| `copies` | ci | the rules of the code, and the transport delivers a batch one more time; no `OneRange` and no `AnswerAgrees` | pass (3 million states, 50 s) |
| `copies_onerange` | ci | the same, with `OneRange`: a copy lands again (multi-writer is at-least-once) | `OneRange` violation |
| `walk_skip` | ci | the walk folds a slot past a gap | `AnswerAgrees` violation |
| `boot_keep` | ci | the fold state survives a restart, and the walk starts again at the chosen index on disk | `OneRange` violation |
| `read_local` | ci | the replica serves a read from its prefix | `ReadFresh` violation |
| `read_replicas` | ci | a read replica counts as a voter of the read | `ReadFresh` violation |
| `live` | ci | the rules of the code | `Ends` pass |
| `live_split` | ci | the rules of the code, the leader refuses the merged batch | `Ends` pass |
| `live_noresend` | ci | the leader does not send lost `Accept`s again | `Ends` violation |
| `reads_two` | long | two replicas, one reader | pass (4.5 million states, 2 min) |
| `safety` | long | the whole model at once: one leader change with a deposed leader that stays up, a batcher that can stop, one restart, one reader | pass (36.5 million states, 27 min) |

The long checks ran once on 2026-11-20 with 4 workers, with the held answers of #1232.

The traces of the controls:

- `resend_ambiguous`: the batcher sends `w1` alone, and the batch times out before the leader
  receives it. The batcher sends it again. The leader allocates slots 0 and 1 to the two copies,
  both are chosen, and the replica gives `w1` the ranges `[0, 1)` and `[1, 2)`. This is the
  at-least-once of §2.4: only a writer may send an ambiguous write again, never the batcher.
- `walk_skip`: `w1` and `w2` go in two batches, to slots 0 and 1. Both are chosen, and the leader
  answers `w2` with `[1, 3)`. The replica learns slot 1 only and folds it first: `w2` gets
  `[0, 2)` at the replica, which is not its answer.
- `boot_keep`: the replica folds slot 0 (`w1` and `w2`) and restarts before its chosen index
  reaches the disk. The state is kept and the walk folds slot 0 again: each write has two
  ranges.
- `read_local`: the leader answers `w1`, and the replica serves a read before it learned the
  slot.
- `read_replicas`: `{a1, a2}` choose slot 0 and the leader answers `w1`. A read closes on `a3`
  and the reader `r1` itself, two answers with watermark 0, and is served empty.
- `live_noresend`: the `Accept`s of the slot of `w1` are lost, and nothing sends them again: the
  slot stays open for good.
- `held_slot`: the leader allocates slot 0 to the batch of `w1`. A new leader wins with frontier 1
  and fills slot 0 with a `Noop`, which is chosen. The deposed leader answers `w1` from the
  outcome of slot 0: `[0, 1)`. No slot holds `w1`, so an acked write is lost.
- `held_notleader`: slot 0 chooses the batch of `w1`, and then a new leader wins. The
  deposed leader answers `NotLeader`, the batcher sends `w1` to the new leader, and the copy gets
  slot 1: the replica gives `w1` two ranges.
- `copies_onerange`: the transport delivers the batch of `w1` two times, and each copy gets a slot.
  This is the at-least-once of a multi-writer journal (§2.4). A single writer is exactly-once
  because the fold judges its `expected_seq` (`multi-writer/`).

What the model does not find: no rule of the code is refuted, and no design decision is in
question. The rule for held answers (#1232) is the decision of 2026-11-20 (`Unknown`, resolved
through `expected_seq`). The read replica's rule is the rule of `reads/`, and the walk's rule is the module doc
of `replica.rs` ("the fold is contiguous or it stops").

Not in the model: the log itself (`multi-paxos/`), the proxy leader (`proxy-leader/`), more
than one journal (a lane never mixes journals, so one lane is enough), the `NotLeader` refusal at
the edge (it takes no slot, so it is the same case as `TooLarge`; this is a reading of the code,
not a check; the `NotLeader` answer of a deposed leader for a held batch is `held_notleader`), a write
over the limits alone, the byte limit, truncation and the trim-point jump, a faulty record that
holds the walk, the reply owner (`slot % replica_count`, which nothing routes on), a read that
expires, and a change of members.

### `truncation/`: the compaction floor, the trim-point jump and the faulty entries, for #1017 (truncation model) and #1216 (truncation model, part 2)

The model is plain Multi-Paxos (`multi-paxos/`, on a majority and with no wipe) with truncation
added. The rules of `paros-consensus` that meet at the floor of an acceptor, and the model has
each of them:

- **The decided truncation and the fold.** A truncation is a value in a slot
  (`Control::Truncate { up_to }`): each value of `Cuts` truncates up to its position `CutAt`. The
  fold raises `first_seq` to that position, clamped to `next_seq`, and never lowers it
  (`JournalState::apply`). A position of `NS` or more (`trunc`) releases every record before the
  slot. A node compacts when it learned the slot and every slot below it. Its floor goes to the
  slot that holds the record at `first_seq`, or one past the slot when no record stays
  (`Replica::compaction_target`, `ColocatedNode::compact`). Each command is one record, and the
  model reads the fold from the chosen log: every node folds the same log. Each node compacts at
  its own time, so the floors of the nodes differ for some time. A compaction can also stop
  below the target, since `ColocatedNode::compact(up_to)` clamps the floor to the driver's
  `up_to`. An acceptor holds no vote and no learned value below its floor.
- **The floor guard in Phase 1** (`PrepareOutcome::BelowFloor`, `node/acceptor.rs`). A campaign
  asks from its first unchosen slot. An acceptor whose floor is above that slot does not promise
  and does not move its promise, because its promise could not report the slots that it
  compacted. An `Accept` below the floor is ignored (`AcceptOutcome::BelowFloor`).
- **The retry of a laggard.** A candidate that a floor refused campaigns again from its new first
  unchosen slot once a jump or a catch-up moved it (`on_nack`, then the next campaign). The model
  keeps the ballot and sends a new `1a` from the new slot; the code opens the next round. With
  two ballots in the model, a new round would be the last ballot, and the check of liveness needs
  that ballot for the leader that no one preempts.
- **The trim-point jump** (`Message::TrimmedTo`, `node/catch_up.rs`). A node that meets a
  `TrimmedTo` whose point is above its own floor jumps there, whatever its chosen prefix
  (`on_trimmed_to`). The point is the floor of the peer, or an older floor of it: the answer can
  arrive late, or come unasked (a follower answers a beat whose sender is below its floor,
  `node/replication.rs`). The jump carries no ballot: the promise does not move. A node that
  asks at or above the floor of the peer gets what the peer learned.
- **A restart** keeps the promise and the votes (on disk before the node answers) and the floor
  (dropped records and the new floor are one write, `WriteOp::Truncate`). It keeps a prefix of
  what the node learned, at or above its floor: the chosen index is a relaxed write
  (`SetChosenIndex` is not in `WriteOp::needs_sync`), so a boot can come back with a lower first
  unchosen slot (`node/boot.rs`).
- **The vote watermark** (`Acceptor::vote_watermark`): the last vote, faulty votes too, or the
  floor minus 1 when the acceptor holds no vote above its floor.
- **The faulty entries of CTRL** (`Acceptor::faulty`, `read_back_log` in `node/boot.rs`). A boot
  finds the record of a slot damaged: its ballot is known and its value is lost. The boot also
  drops the learned value of the slot, because the chosen map is rebuilt from the records. A
  later `Accept` over the slot heals it (`record_accepted`). A scrub that reads the damage first
  writes the record again from memory (`ColocatedNode::repair_record`). That write moves no state
  of the model, so the model checks the damage that the scrub did not reach: `Rot` is a boot that
  finds a faulty entry.
- **The tri-state tally and the repair probe** (`slot_decidable` in `proposer.rs`,
  `proposer/probe.rs`). At a slot with a faulty report, the threshold is the highest readable
  report. An answer qualifies when it is not a faulty report above the threshold. A slot is
  decided only when the qualifying answers hold a Phase-1 quorum; else the slot is blocked. The
  repair probe decides a blocked slot later on more promises of the same ballot, or drops it
  once the leader learned it. An acceptor whose promise is already at the ballot answers the
  probe too (`Acceptor::prepare` re-affirms a promise at its own ballot): it took an `Accept` of
  the ballot before the `Prepare` reached it.
- **The election gap fill**, as in `multi-paxos/`: a new leader proposes again each slot that its
  Phase 1 reported, and a `Noop` at each slot with no report below its frontier. A faulty report
  moves the frontier too.

| Constant | Values |
|---|---|
| `FloorGuard` | `"nack"`: an acceptor does not promise a campaign that asks from below its floor. `"ignore"`: a control, it promises and reports only what it still holds. |
| `TrimPromise` | `"keep"`: the trim jump keeps the promise. `"copy"`: a control, the jump takes the promise of the peer. |
| `CompactWhen` | `"prefix"`: a node compacts once it learned every slot up to the truncation. `"learned"`: a control, it compacts once it learned the truncation alone. |
| `FloorRule` | `"fold"`: the floor goes to the slot of the record at `first_seq`. `"slot"`: a control, the floor goes one past the slot of the truncation, whatever its position. |
| `Watermark` | `"floor"`: the watermark counts the floor. `"votes"`: a control, it counts the retained votes only (0 for a compacted acceptor). |
| `Tally` | `"ctrl"`: the tri-state tally. `"none"`: a control, a faulty report counts as no vote. `"have"`: a control, a faulty report qualifies as soon as one readable report exists (CTRL's Case 1 read with no ballot threshold). The two differ only when a faulty report is above a readable one, so its checks have three ballots. |
| `TrimPoint` | `"floor"`: a `TrimmedTo` carries the floor of the peer, or an older floor of it. `"learned"`: a control, the jump goes to any point of the chosen prefix of the peer, truncated or not. |
| `FloorWrite` | `"atomic"`: the store drops the records below a new floor and records the floor in one write. `"late"`: a control, the floor reaches the disk after the records are gone, and a restart can read the old floor. |
| `Retry` | `"jump"`: a refused candidate asks again from its new first unchosen slot. `"none"`: a control, it never asks again. |
| `Restarts` | How many restarts can happen in the run. |
| `Faults`, `RotNodes` | How many faulty entries a boot can find in the run, and on which nodes. |
| `Learners` | The nodes that catch up and jump. |
| `C3`, `R2` | A second reconfiguration: ballots from `R2` up use `C3`, whose new members start empty too. `R2 = NB + 1`: no second reconfiguration. |

The invariants:

- `Agreement`: one value for each slot, with the compacted slots included. A vote is a `2b`
  message that a compaction or a lost record does not remove, so `msgs` is the ghost that keeps
  what was chosen.
- `LearnedChosen`: what a node learned was chosen.
- `NoFillChosen`: a leader never proposes a `Noop` at a slot where a lower ballot chose another
  value.
- `FloorChosen`: each slot below the floor of an acceptor is chosen.
- `FloorTruncated`: each slot below a floor is at or below a chosen truncation.
- `FloorReleased`: each record below a floor is below `first_seq`. Only the records below
  `first_seq` may be forgotten.
- `Recoverable`: for each chosen slot, each Phase-1 quorum has a member that reports a vote
  there to a campaign from the slot (a faulty vote blocks the tally), or a member that refuses
  that campaign (the floor guard). Without the floor guard a member whose floor is above the
  slot promises and reports nothing (`recover_ignore`).
- `ValueKept`: each chosen slot is below a floor, or a node can still read its value.
- `WatermarkCovers`: the vote watermark of an acceptor is never below a slot that it voted.
- `Ends` (liveness, `LiveSpec`): the last ballot `NB` wins, closes its repair probe and holds
  every slot below its allocator. A chosen slot whose record a boot found faulty is still
  applied. The fairness covers the owner of `NB` (campaign, retry, win, probe, decide, catch-up,
  jump) and each acceptor (promise, accept).

Results (`truncation/checks.txt`, three acceptors, two ballots, two slots, the values `trunc` and
`Noop`, and `a1` and `a2` catch up, unless the line says otherwise; the CI set runs in about 7
minutes with 4 workers):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach_compact` | ci | `NoCompaction`, a reachability control: a node compacts | violation |
| `reach_split` | ci | `NoSplitFloor`, a reachability control: one node compacted, one did not, and a slot above the floor is chosen | violation |
| `reach_jump` | ci | `NoJump`, a reachability control: a node jumps over a slot that it never voted | violation |
| `floor_ignore` | ci | Phase 1 ignores the floor | `NoFillChosen` violation |
| `trim_promise` | ci | the trim jump takes the promise of the peer; one command too | `Agreement` violation (1.2 million states) |
| `compact_early` | ci | a node compacts once it learned the truncation alone | `FloorChosen` violation |
| `watermark_votes` | ci | the watermark leaves the floor out | `WatermarkCovers` violation |
| `safety` | ci | the rules of the code | pass (1.0 million states, 1.5 min) |
| `recover_ignore` | ci | Phase 1 ignores the floor | `Recoverable` violation |
| `trim_learned` | ci | the jump goes past a slot of the peer's chosen prefix that no truncation covers | `FloorTruncated` violation |
| `floor_late` | ci | the floor reaches the disk after its records are dropped; one restart | `NoFillChosen` violation |
| `reach_faulty` | ci | `NoFaultyChosen`, a reachability control: a boot finds a chosen slot faulty; one fault | violation |
| `reach_block` | ci | `NoBlock`, a reachability control: a repair probe opens; one fault | violation |
| `ctrl_none` | ci | a faulty report counts as no vote; one fault | `NoFillChosen` violation |
| `two_rots` | ci | two faults: more than the majority survives | `ValueKept` violation |
| `ctrl_one_slot` | ci | one fault, one slot | pass (32 thousand states) |
| `ctrl_three` | ci | one fault on `a1`, one slot, three ballots, `c1` too, no node catches up: a faulty report above a readable one | pass (687 thousand states, 2 min) |
| `ctrl_have` | ci | the same, a faulty report qualifies beside any readable report | `Agreement` violation |
| `rot_two` | ci | two faults, one slot, `c1` too | pass (146 thousand states): two faults cost liveness, never agreement |
| `ctrl_live` | ci | `Ends`, one fault on `a1` or `a3`, one slot, `a2` (the last leader) catches up | pass |
| `reach_retry` | ci | `NoRetry`, a reachability control: a refused candidate asks again; `a1` leads ballot 1 and `a3` ballot 2 | violation |
| `laggard_stuck` | ci | `Ends`, a refused candidate never asks again; one slot, `a2` and `a3` catch up | violation |
| `laggard` | ci | `Ends`, the retry of the code; one slot, `a2` and `a3` catch up | pass |
| `reach_keep` | ci | `NoKeep`, a reachability control: a node applied a truncation and the fold keeps its floor at or below the truncation's slot (the floor is the slot of the record at `first_seq`); one leader, three slots, `c1` and `cut1` (up to position 1), every node catches up | violation |
| `fold_slot` | ci | the floor goes one past the truncation; `c1` and `cut0` too | `FloorReleased` violation |
| `fold_one_leader` | ci | the fold, one ballot; `c1` and `cut0` too, every node catches up | pass (54 thousand states) |
| `fold_mid` | ci | the fold stops in the middle of the log: one leader, three slots, `c1` and `cut1`; `a1` and `a2` catch up | pass (208 thousand states) |
| `restart` | long | one restart | pass (4.0 million states, 6 min) |
| `full` | long | one command too, and every node catches up | pass (13.7 million states, 32 min) |
| `three_slots` | long | three slots | no violation in 31.8 million distinct states (bounded: stopped after 60 min, 12.3 million states left on the queue) |
| `ctrl` | long | one fault on `a3` | pass (4.3 million states, 6.5 min) |
| `ctrl_all` | long | one fault on any node | pass (10.2 million states, 15 min) |
| `fold` | long | the fold; `c1` and `cut0` too | pass (10.5 million states, 32 min) |
| `fold_keep` | long | the fold; `c1` and `cut0` only, no node catches up | pass (1.5 million states, 3 min) |

The long checks ran once on 2026-11-19 (#1216 (truncation model, part 2)) with 4 workers.

The traces of the controls (ballot 1 is `a1`'s, ballot 2 is `a2`'s, unless the line says
otherwise):

- `floor_ignore` (the split floor of #409): ballot 1 wins on `{a1, a3}` and proposes `trunc`
  at slots 0 and 1. `{a1, a3}` choose slot 0, and `a1` alone votes slot 1. `a1` learns slot 0
  and compacts to floor 1. Ballot 2 asks from slot 0, and `a1` promises with its floor ignored:
  it reports its vote at slot 1 and nothing at slot 0, because it holds nothing there. Ballot 2
  wins on `{a1, a2}`, its frontier is 2, and it fills slot 0 with a `Noop`.
- `trim_promise`: `a2` opens ballot 2 and promises it. Ballot 1 wins on `{a1, a3}` and proposes
  `trunc` at slot 0 and `c1` at slot 1. Ballot 2 wins on `{a2, a3}` and proposes another value
  at slot 1, and `{a2, a3}` choose it. `a1` compacts slot 0, and `a2` asks it from slot 0: the
  jump gives `a2` the promise 1 of `a1`. `a2` then votes `c1` at ballot 1, and `{a1, a2}` choose
  `c1` at slot 1 too. A jump that sets the promise can lower it; this is why `TrimmedTo` carries
  no ballot.
- `compact_early`: `a1` learns the truncation at slot 1 before slot 0 is chosen, compacts, and
  its floor covers a slot that is not chosen.
- `watermark_votes`: `{a1, a2}` choose `trunc` at slot 0, and `a2` compacts its only vote. With
  the floor left out, its watermark is `None`, below the slot that it voted. A quorum read would
  not wait for that slot.
- `ctrl_none`: `{a1, a3}` choose `trunc` at slot 0 at ballot 1. `a1` boots and finds slot 0
  faulty. Ballot 2 wins on `{a1, a2}`: `a1` reports a faulty vote at ballot 1, and `a2` reports
  nothing. When the faulty report counts as no vote, ballot 2 fills slot 0 with a `Noop` over the
  chosen `trunc`. The tri-state tally does not let `a1` qualify (its faulty vote is above the
  threshold 0), so `{a2}` alone is not a quorum and the slot is blocked; the repair probe waits
  for `a3`, which reports `trunc`.
- `two_rots`: `{a1, a2}` choose `trunc` at slot 0, and both boot with slot 0 faulty. No node can
  read the value. The tri-state tally keeps the slot blocked, so this costs liveness, not
  agreement: one fault is the limit of a majority of three.
- `laggard_stuck`: `a1` leads ballot 1 and `a3` ballot 2. `{a1, a2}` choose `trunc` at slot 0,
  and `a3` opens ballot 2 from slot 0. `a1` compacts to floor 1, and `a2` and `a3` jump to it.
  `a1` and `a2` refuse a campaign from slot 0, and `a3` never asks again: ballot 2 never wins.
  With the retry of the code (`laggard`), `a3` asks again from slot 1 and wins.
- `ctrl_have` (the ballot threshold of the tri-state tally, paros's own rule on top of CTRL):
  ballot 1 (`a1`) gets `a3` to vote `trunc` at slot 0. Ballot 2 (`a2`) wins on `{a1, a2}`, and
  `{a1, a2}` choose `c1`. `a1` boots with slot 0 faulty at ballot 2. Ballot 3 (`a3`) wins on
  `{a1, a3}`: `a3` reports `trunc` at ballot 1, and `a1` reports a faulty vote at ballot 2. The
  weak tally lets the faulty report qualify, proposes `trunc` at ballot 3, and `{a1, a2}`
  choose it. The code's tally refuses `a1` (its faulty report is above the threshold 1), and the
  slot is blocked.
- `floor_late`: ballot 1 proposes `trunc` at slots 0 and 1, and `{a1, a3}` choose slot 0. `a1`
  learns it and compacts to floor 1: the record of slot 0 leaves the disk. `a1` restarts before
  the new floor is on disk, so it reads floor 0 and no vote at slot 0. Ballot 2 wins on
  `{a1, a2}`, hears no vote at slot 0, and fills the chosen slot with a `Noop`. The store must
  drop the records and record the floor in one write (`WriteOp::Truncate`).
- `fold_slot`: ballot 1 chooses `c1` at slot 0 and `cut0` at slot 1. `cut0` truncates up to
  position 0, so `first_seq` stays 0 and the record of slot 0 stays. `a1` learns both slots and
  compacts one past slot 1: its floor drops a record at `first_seq`.

What the model finds: no rule of the code is refuted, and no design decision is in question. One
rule of the code was missing from the first model: an acceptor whose promise is at the ballot
answers a `Prepare` of that ballot again. Without it, `ctrl_live` gave a violation of `Ends`:
`a3` alone voted slot 0 at ballot 1 and booted with it faulty, ballot 2 won on `{a2, a3}` with
the slot blocked, and `a1` took an `Accept` of ballot 2 before its `Prepare`. The repair probe
could not get the answer of `a1`. The code re-affirms (`Acceptor::prepare`), so the model does
too.

The rule of the code differs from the text of #1017 at one point. The issue had a new leader
take the slots below the highest floor in its promises as decided. The code does not let such a
promise exist: an acceptor whose floor is above the slot that the campaign asks from refuses it
(`BelowFloor`), and the candidate catches up out of band (`TrimmedTo`) before a new campaign.
The model checks the code's rule.

The `restart` check keeps what a boot can lose: the chosen index (a prefix of what the node
learned) and, with the control `FloorWrite = "late"`, the floor. `dprom` is gone: a promise
reaches the disk before the node answers, so the promise on disk always equals the one in
memory.

Not in the model: a reconfiguration onto empty stores with a floor from `TrimmedTo` (step 2 of
#1216 was only to be done if the faulty entries were cheap; they are not, as `ctrl` is a long
check, so it moves to #1218 (truncation model, part 3)), a retained retry that holds the floor down
(`compaction_target` walks down to the record that a retry above `first_seq` read; the model has
no retry of a write), a scrub racing a boot (`Rot` is the damage that the scrub missed), a
faulty entry at a slot that a node learned with no vote (in the code a learned slot always has
its record), wipes (`multi-paxos/` checks the identity rule), and grids.

### `truncation-reconfig/`: truncation across a reconfiguration onto empty stores, for #1218 (truncation model, part 3)

The model is `truncation/` with a second configuration. A member of the new configuration
starts with an empty store, and the GC of `node/gc.rs` forgets the old configuration. The
question: once the old configuration is gone, does each chosen slot stay recoverable over the
Phase-1 quorums of the new one, when some slots are below the floor of a member and a new member
holds nothing below its own floor?

The model has these rules of `paros-consensus`:

- **A configuration per ballot.** Ballots below `R` use `C1 = {a1, a2, a3}`, and ballots from `R`
  up use `C2 = {a2, a3, a4}` (`ColocatedNode::reconfigure`: a reconfiguration is a round
  change). `a4` starts with floor 0, no vote and nothing learned. It promises the new ballot and
  takes part from its Phase 2, and it heals by catch-up or by the trim-point jump.
- **Matchmaking** (`matchmaking.rs`). A candidate registers its ballot before its `Prepare`, and
  a matchmaker refuses a ballot below one that it registered. The candidate gets `H_b`: the
  configurations of the registered ballots below its own, from the GC watermark up. Phase 1
  needs a Phase-1 quorum of each configuration of `H_b` and of its own. Phase 2 uses its own
  configuration only.
- **Learning writes a record** (`mark_chosen` in `node/learn.rs`). A node that learns a slot
  records the chosen value as its accepted record, at the choosing ballot, and raises its
  promise to that ballot. A held record of the same value at a ballot at or above it stays. A
  catch-up copies the record of the peer. This is what lets a new member answer Phase 1 for
  slots that it never voted.
- **The GC** (`node/gc.rs`, `Collector::covered`). The leader of ballot `b` raises the
  watermark of the matchmakers to `b`, and every later candidate forgets the configurations
  below `b`. The code raises it only once a Phase-2 quorum of `C_b` reports a chosen index at
  or past the election fence `F` (the highest slot that a promise of the win reported). The model
  raises the watermark in one step: a watermark is effective once a matchmaker quorum acked it,
  and each later matchmaking quorum meets that quorum.
- **The floor, the floor guard, the trim-point jump and the retry of a refused candidate** are
  the ones of `truncation/`.

To keep the state space small, the model leaves out what `truncation/` checks on one
configuration: each truncation releases every record before its slot (a node that compacts at
slot `t` moves its floor to `t + 1`, so the fold of `first_seq` is not in the model), and there
is no faulty entry and no restart.

| Constant | Values |
|---|---|
| `GcRule` | `"fence"`: the rule of the code, a Phase-2 quorum of `C_b` holds every slot up to `F`. `"won"`: a control, the leader forgets the old configuration as soon as it wins Phase 1. |
| `Cover` | `"chosen"`: a member counts toward the fence when it holds each slot up to `F` (it learned the slot, or the slot is below its floor). `"joined"`: a control, a new member counts too, with floor 0 and no record. |
| `Learners` | The nodes that catch up and jump. |
| `C3`, `R2` | A second reconfiguration: ballots from `R2` up use `C3`, whose new members start empty too. `R2 = NB + 1`: no second reconfiguration. |

The invariants are `Agreement`, `LearnedChosen`, `NoFillChosen` and `FloorChosen` of
`truncation/`, and:

- `RecordChosen`: an accepted record at a chosen slot holds the chosen value, or its ballot is
  below each ballot that chose the slot. A learn never writes a wrong record.
- `Recoverable`: for each chosen slot, one configuration that the next candidate must cover (a
  registered ballot at or above the watermark) has, in each of its Phase-1 quorums, a member
  that holds a record there or whose floor is above the slot. The candidate needs a quorum of
  each such configuration, so it meets that member. This is the question of #1218.

Results (`truncation-reconfig/checks.txt`: four acceptors, ballot 1 is `a1`'s, ballot 2 (the
reconfiguration) is `a3`'s, ballot 3 is `a2`'s, the values `trunc` and `Noop`; `a2`, `a3` and
`a4` catch up, unless the line says otherwise; the CI set runs in about 2 minutes with 4
workers):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach_gc` | ci | `NoGc`, a reachability control: the GC forgets `C1` after `C1` chose a slot; two ballots, one slot | violation |
| `reach_jump` | ci | `NoJoinJump`, a reachability control: the new member jumps over a slot that it never voted; two ballots, one slot | violation |
| `reach_split` | ci | `NoGcSplit`, a reachability control: the GC forgets `C1` while a member of `C2` compacted and the new member holds nothing below that floor; two ballots, one slot | violation |
| `reach_late_win` | ci | `NoLateWin`, a reachability control: after the GC, ballot 3 wins Phase 1 over `C2` alone; one slot | violation |
| `gc_won` | ci | the GC at the win; two ballots, one slot | `Recoverable` violation |
| `cover_joined` | ci | the new member counts with no record; two ballots, one slot | `Recoverable` violation |
| `gc_won_fill` | ci | the GC at the win; two slots, no node catches up | `NoFillChosen` violation (1.4 million states, 73 s) |
| `safety` | ci | the rules of the code; two ballots, one slot | pass (302 thousand states, 20 s) |
| `cover_joined_fill` | long | the new member counts with no record; two slots, `a3` alone catches up | `NoFillChosen` violation (4.8 million states, 4.5 min) |
| `three_ballots` | long | the rules of the code; one slot | pass (41.5 million states, 54 min) |
| `two_slots` | long | the rules of the code; two ballots | no violation in 43.8 million distinct states (bounded: stopped after 63 min, 21.2 million states left on the queue) |
| `reach_fill_gc` | ci | `NoGc` on the shape of `fill_code`: the GC fires there | violation (302 thousand states, 27 s) |
| `fill_code` | long | the rules of the code; three ballots and two slots, `a2` alone catches up: the smallest shape where the two `*_fill` controls find a `Noop` over a chosen slot | no violation in 8.2 million distinct states (bounded: 50 minutes, 1 worker) |
| `reach_chain` | ci | `NoThirdWin`, a reachability control: two reconfigurations (`C1 = {a1, a2, a3}`, `C2 = {a2, a3, a4}`, `C3 = {a3, a4, a5}`), and ballot 3 wins in `C3` with `C1` forgotten after `C1` chose a slot; one slot | violation (1.3 million states, 2.7 min with 1 worker) |
| `chain_joined` | ci | the same, a new member counts with no record | `Recoverable` violation (13 thousand states) |
| `chain` | long | the rules of the code over the two reconfigurations; one slot | no violation in 10.7 million distinct states, to depth 19 (bounded: 50 minutes, 1 worker) |
| `reach_wipe` | ci | `NoWipeAfterGc`, a reachability control: a member of `C2` is wiped after the GC forgot `C1`, once `C1` chose a slot; one wipe | violation (57 thousand states) |
| `wipe_revote` | ci | one wipe, the wiped acceptor comes back empty as the same acceptor (`Identity = FALSE`) | `Recoverable` violation (1.5 thousand states, 8 states deep) |
| `wipe` | ci | the rules of the code and one wipe, with the identity rule | pass (806 thousand states, 2.5 min with 2 workers) |

The long checks ran once on 2026-11-19 (#1218 (truncation model, part 3)) with 4 workers;
`fill_code` and `chain` ran on 2026-11-20 (#1230 (rules no check tests)).

A wipe (#1230 (rules no check tests)): an acceptor loses its whole store. With the identity
rule (`Identity`, the rule of `decree/` and `multi-paxos/`) a wiped acceptor never takes a step
again, so a Phase-1 quorum with it never forms, and `Recoverable` counts only the quorums
whose members are up. With the control it answers with an empty store: in `wipe_revote`, `a1`
votes slot 0 at ballot 1 and is wiped, then `a2` votes, so slot 0 is chosen on `{a1, a2}`, and
the quorum `{a1, a3}` of `C1` holds no record of it.

Not in the model, still: a faulty entry (CTRL) across configurations, which is the class of
#267 (the tri-state tally over every prior configuration, `proposer.rs`, `slot_decidable`;
`truncation/` checks CTRL on one configuration), and a restart (a restart keeps every durable
record, and `learned` is not part of `Recoverable`).

With one slot, a `Noop` never lands on a chosen slot: a candidate whose Phase 1 reports nothing
has its frontier at its first slot and fills nothing. So the two controls that must find the
`Noop` run with two slots, and the checks with one slot judge `Recoverable`, the property that
the `Noop` breaks.

The traces of the controls:

- `gc_won_fill`: ballot 1 wins on `{a1, a2}` and proposes `trunc` at slots 0 and 1. `{a1, a3}`
  choose slot 0, and `a2` votes slot 1. `a3` opens ballot 2 under `C2` and wins on `{a2, a3}`, a
  quorum of both `C1` and `C2`. It proposes `trunc` again at slots 0 and 1 and, with
  `GcRule = "won"`, raises the watermark at once, before a Phase-2 quorum of `C2` accepted
  or learned slot 0. Ballot 3 of `a2` matches with `C2` alone and wins on `{a2, a4}`. It hears slot 1 only
  (the vote of `a2`), its frontier is 2, and it fills slot 0 with a `Noop`. `a3`, the one member
  of `C2` that holds slot 0, is not in the quorum.
- `cover_joined_fill`: ballot 1 wins on `{a1, a2}`, and `{a1, a3}` choose `trunc` at slot 0.
  Ballot 2 of `a3` wins on `{a2, a3}`, proposes `trunc` again at slot 0, and allocates slot 1,
  which `a2` votes at ballot 2. Its fence covers slot 0. `a1` learns slot 0, and `a3` learns it
  by a catch-up from `a1`. With `Cover = "joined"`, `{a3, a4}` cover the fence although `a4`
  holds nothing, and the GC forgets `C1`. Ballot 3 of `a2` wins on `{a2, a4}`, hears slot 1
  only, and fills slot 0 with a `Noop`, as in `gc_won_fill`.

The answer to #1218: with the rule of the code, `Recoverable` holds over the Phase-1 quorums of
`C2` once `C1` is gone. A member of `C2` counts toward the fence only when it holds each slot
up to `F`: it learned it (and so holds a record, `mark_chosen`) or jumped above it with
`TrimmedTo` (and so refuses a campaign from below its floor). Each Phase-1 quorum of `C2` meets
that Phase-2 quorum. No decision is refuted.

### `quorum-everywhere/`: every journal step waits for a quorum only, for #1257 (quorum everywhere)

Pierre's rule (2026-11-20): no step of the journal waits for every member of a group, as a
FoundationDB commit waits for every TLog. The model is a liveness check. Members are down from
the start and stay down. The journal runs its steps one after the other, and each step is a list
of waits. A wait sends to its targets and completes when the answers hold one of its quorums. The
quorums are the sets of `grid/Grid.tla`, so the model and the code use one layout. A Phase-2 wait
has one alternative for each column, and it moves to the next column only when every live member
of its column answered and the answers hold no quorum (#533 (move a stuck round)). The property
`Done` says that every step completes, under weak fairness.

| Step | Waits, in order |
|---|---|
| `write` | `acks` of one column of `New`, with the move |
| `election` | a matchmaker majority; a Phase-1 quorum of `Old`; a Phase-1 quorum of `New` |
| `read` | a Phase-1 quorum of the answers to `GridReadTargets(New)`; a matchmaker majority (the segment-record check that #1254 proposed; the journal does not keep it, `docs/architecture/02-data-plane.md` §2.10, so this wait asks more than the read needs) |
| `seal` | a Phase-1 quorum of `Old` (the highest vote); a matchmaker majority (the compare-and-set of the end slot) |
| `retire` | `F + 1` replicas of the segment hold it; a matchmaker majority |
| `offload` | a matchmaker majority (the offload mark, #1256) |
| `trim` | `acks` of one column (the decided `Truncate`); a matchmaker majority |
| `handover` | a stop quorum of `MM`; Phase 1 and Phase 2 of the successor decree in `MM2` |
| `ctl_election` | a matchmaker majority (read the controller term); a matchmaker majority (its compare-and-set, #1259) |
| `reshape` | a matchmaker majority (the fenced reshape call, #1259) |
| `bypass_hold` | `acks` of one column of `Old`, with the move (the hold before Phase 2 of a bypass, #1258) |

`Old` is a sealed configuration and `New` the configuration in force: `New` moves the members
of `Old` by `Shift` places, so with `Shift = 1` each member that stays changes its column. The
down sets are the largest ones with at most `F` members down in each column of both
configurations, in each matchmaker set and in the replica set. A wait that completes with a down
set also completes with each smaller one, so the largest down sets are enough. An `ASSUME` checks
that some down set has exactly `F` down in every column of both configurations at once.
The answers come in one fixed order: a wait's guard only grows with the answers, so the order
changes nothing, and the state space stays small.

| Check | CI | Result |
|---|---|---|
| `quorum` | ci | pass: the grid 3 × 2 with `acks = 2`, `F = 1`, `Shift = 1` |
| `quorum_shift_row` | ci | pass: the same with `Shift = 2` (each member keeps its column) |
| `quorum_3x3` | ci | pass: 3 × 3, `acks = 2`, `F = 1` |
| `quorum_f2` | ci | pass: 5 × 2, `acks = 3`, `F = 2` (890 thousand distinct states with the two controller steps; 12 minutes locally with 4 workers while two other TLC runs shared the machine) |
| `quorum_majority` | ci | pass: the majority of three (`partitions = 1`, a grid 3 × 1) |
| `write_move` | ci | pass: `F + 1` down in one column, `F` in the others; the write moves and completes |
| `bypass_hold_move` | ci | pass: the same for the hold before Phase 2 of a bypass, in the old grid |
| `bypass_hold_every_column` | ci | violation: `F + 1` down in every column of `Old` blocks the hold |
| `write_every_column`, `trim_every_column` | ci | violation: `F + 1` down in every column of `New` blocks the write |
| `election_column`, `read_column`, `seal_column` | ci | violation: `F + 1` down in one column blocks Phase 1 |
| `retire_replica` | ci | violation: `F + 1` replicas down blocks the retire |
| `<step>_mm`, for each step that asks the matchmakers | ci | violation: `F + 1` down in one matchmaker set blocks the step |
| `<step>_all`, for each step | ci | violation: a wait that needs every target (a TLog commit) blocks with `F` down |
| `full_grid_write` | ci | violation: the full grid (`acks = rows`) writes only with a whole column |

The answer to #1257:

- **Every step needs a quorum only.** With `F` down in every column of the old and the new grid,
  `F` down in each matchmaker set and `F` down in the replica set, every step completes. Each
  control with one member more down blocks its step, so the quorums are real, and each step that
  waits for every target blocks, so the model sees a full broadcast.
- **The leaderless read on a grid that is not full waits for a quorum.** `GridReadTargets` sends
  the `PreRead` to every member, but the read completes on a Phase-1 quorum of the answers
  (`QuorumRead`, `has_phase1_quorum_in`). So it sends to all and waits for a quorum. It is not a
  full broadcast in the sense of the rule, and no change is necessary.
- **The layouts that meet the rule.** `grid/GridCheck.tla` checks `PerColumnTolerance` on every
  layout up to nine members: `f` members down in every column leave a Phase-1 quorum and a
  Phase-2 quorum exactly when `f + 1 <= acks <= rows - f`. The partitions `rows = 2F + 1`,
  `acks = F + 1` meet it for `F`. The full grid (`acks = rows`) survives no member down in
  every column, because a write needs a whole column. The service offers only the majority and
  the partitioned grid (`docs/architecture/03-4-tenant-modes.md`), so the rule holds; the full
  grid stays a shape of `paros-consensus` and of its models, outside the journal.

### `grid-controller/`: the controller's election and its fenced reshape call, for #1259 (the fenced reshape call of the grid controller)

Pierre's decisions (2026-11-20): the grid controller is its own role in each journal, on
stateless machines, elected by a term in the journal's matchmaker record, and its only effect on
the journal is `reshape(expected configuration, new grid, controller term)`, a compare-and-set
on that record. The record is one atomic register, as in `experiments/segments/a/`: the other
models check how a matchmaker quorum keeps such a register, and `quorum-everywhere/` checks that
each of its writes waits for a majority only. The record holds the term, its holder and the
segment table (each configuration: its grid, the configuration it was planned from, and a state:
`pending`, `inforce`, `sealed`, `retired`). The seal and the retire are abstract journal steps:
the seal needs a live Phase-1 quorum of the old grid and live Phase-1 and Phase-2 quorums of the
new grid. A grid is one column (`GridCheck.tla` shows the bound is the same for every column
count).

A controller is stateless: a crash forgets its term and its plan. It reads the term, then writes
term + 1 by compare-and-set on the term it read. A candidate can depose a live holder, and the
deposed holder keeps calling with its old term until a refusal tells it. The controller may ask
for any grid of `Grids` from the newest configuration it read, at any time: the record judges
what gets in, not the controller. The network loses at most one request or reply, and a
controller sends its request again while it has no answer. One acceptor of the pool fails
(at most `F`, inside the contract).

Properties: `OneFromEach` (at most one segment opens from one configuration), `HolderOnly` (only
the holder of the current term opens a segment), `StaleNoEffect` (a request with another term
changes nothing), `OneUnretired` (one configuration in force and at most one other not retired:
no second reshape while a segment is not retired), `AllBounded` (every grid of the table meets
`F + 1 <= acks <= rows - F`, #1257), `AnswerTrue` ("ok" exactly when the record took the
request), and two liveness properties under weak fairness once the faults stop: `Answered` (every
call is answered, done or refused) and `Settles` (every configuration that a reshape opened comes
into force).

The rules are constants; `TRUE` is the rule. `Raise` (the election writes term + 1), `TermCheck`,
`ExpCheck` (the expected configuration is the newest of the table), `RetireGuard` (the newest is
in force and every older one retired), `BoundGuard` (the record checks the bound itself) and
`Idempotent` (the record keeps the identity of the request that opened each segment and answers a
retry "ok").

| Check | CI | Result |
|---|---|---|
| `safety` | ci | pass: every rule; two controllers, terms up to 2, one controller crash, one lost message, six grids (two member sets, `acks` 1 to 3) |
| `safety_long` | long | pass: every rule with terms up to 3 and one acceptor failure too (24.6 million distinct states, 7 minutes locally with 4 workers) |
| `live_ci` | ci | pass: `Answered` and `Settles`, with every rule, one lost message and one acceptor down, no controller crash (257 thousand distinct states, under 2 minutes) |
| `live` | long | pass: the same with one controller crash too (1.8 million distinct states, 14 minutes locally with 4 workers while two other TLC runs shared the machine) |
| `elect_noraise` | ci | violation of `HolderOnly`: an election that keeps the term gives two controllers one term, and both open segments |
| `noterm`, `noterm_holder` | ci | violation of `StaleNoEffect` and `HolderOnly`: a deposed controller that read the newest configuration opens a segment with its old term |
| `noterm_one` | ci | pass: with no term check, `OneFromEach` still holds, because the expected-configuration check refuses every stale plan |
| `noexp_one` | ci | pass: with no expected-configuration check, `OneFromEach` still holds, because the term refuses a deposed controller and a holder has one call at a time |
| `noexp_noterm` | ci | violation of `OneFromEach`: with both checks off, the new holder's reshape lands, then the deposed controller's call from the same configuration lands too |
| `noexp_noidem` | ci | violation of `OneFromEach`: with no expected-configuration check and no idempotent answer, the retry of a call whose "ok" was lost opens a second segment |
| `noretire` | ci | violation of `OneUnretired`: a second reshape while the first is pending |
| `nobound` | ci | violation of `AllBounded`: the record takes a grid outside the bound |
| `live_nobound` | ci | violation of `Settles`: a grid with `acks = 1` and one acceptor down never comes into force (Phase 1 needs every member), and the retire guard then blocks every later reshape |
| `noidem` | ci | violation of `AnswerTrue`: the "ok" is lost, and the retry is refused for its expected configuration although the reshape took place |
| `reach_<event>` | ci | violation of `NotReached`, one for each case the issue names: `lost_race` (two candidates read one term, one loses the compare-and-set), `deposed` (a candidate deposes a live holder), `stale_refused` (the deposed controller's call is refused for its term), `takeover` and `open_after_takeover` (a candidate takes the term of a crashed holder, then opens a segment), `crash_after_plan` and `open_after_crash` (a controller crashes between its plan and its call, and the journal still reshapes), `sealed_refused` (a reshape asked while a segment is sealed but not retired is refused), `ok_lost` (an "ok" is lost), `fail_after_plan` (an acceptor fails after a plan named it), `inforce_with_failed` (that grid still comes into force) |

The answer to #1259:

- **The model passes, and each rule has a control that breaks it.** The election must raise the
  term; the record must check the term, the expected configuration, the retire state and the
  bound; and it must answer a retry with the answer it gave.
- **One claim of the issue is refuted as written.** "With no term check, two segments open from
  one configuration" is false while the expected-configuration check holds (`noterm_one`
  passes). The two checks overlap for `OneFromEach`: each one alone keeps it, and only with both
  off does a configuration open twice (`noexp_noterm`). The term's own job is `HolderOnly` and
  `StaleNoEffect`: a deposed controller that read the newest configuration passes the
  expected-configuration check, and only the term refuses it (`noterm`).
- **A rule that the issue did not name: an idempotent answer.** A call whose "ok" is lost is sent
  again; without an idempotent answer the record refuses it, and the controller believes a
  reshape failed that took place (`noidem`). The record keeps the identity of the call that
  opened each segment (term, expected configuration, grid) and answers it "ok" again. Pierre
  decided this rule on 2026-11-20 (`docs/architecture/02-data-plane.md` §2.12).
- **The bound must be a guard of the record, not only a policy of the controller.** A grid outside
  `F + 1 <= acks <= rows - F` can open and never come into force with one acceptor down; the
  retire guard then blocks every heal (`live_nobound`).
- **Trace checking.** The controller has no code yet. The model's steps (`ReadTerm`, `Elect`,
  `Plan`, `Call`, `Process`, `Receive`, `SealIn`, `Retire`) are the events a trace spec will
  follow once the `journal-sim` walk of the controller loop exists (#1261 (grid controller
  decision tests), after #1189).

### `degraded-reshape/`: a reshape when a column is below quorum, for #1258 (reshape with a column below quorum)

The journal grid is `Grid { rows: 2F + 1, cols: P, acks: F + 1 }` (section 2.11 of
`docs/architecture/02-data-plane.md`). A column is below quorum when fewer than `F + 1` of its
acceptors are live. Then no Phase-1 quorum of the grid exists, because Phase 1 needs `F + 1` of
every column. A write still completes in another column (#533 (move a stuck round)). So no new
leader can be elected, and a reconfiguration through a full Phase 1 is blocked. The question of
#1258 is whether the live leader can still reshape away from that column with the Phase-1 bypass
of `zero-stall/`, and then ack writes and retire the old grid.

The model is `zero-stall/Bypass.tla` in the mode "carry" with the bypass check on, plus grid
quorums from `grid/Grid.tla`, acceptors that crash, the seal end of design A (#1213 (segments
experiment)), GC, and leaderless reads whose answer a client sees (`Serve`). A leader at ballot 1
in the old grid bypasses to its ballot 2 in a new grid. A rival can lead at ballot 3. Constants:

| Constant | Values |
|---|---|
| `Layout` | each configuration's members and grid. `LShrink`: the 3 × 2 grid of `a1`..`a6` (columns `{a1, a3, a5}` and `{a2, a4, a6}`), then the majority of the healthy column. `LSwap`: the same old grid, then the majority of `a5`, `a6` and a new `a7`: its write quorum `{a5, a7}` holds no acceptor of the healthy old column. `LSmall`: the grid 1 × 2 (`F = 0`), then a heal where the survivor changes columns. `LMaj`, `LMove`: moves between majorities of three, all live. |
| `CanCrash`, `DownAtStart` | `a1` and `a3` crash (their column is below quorum), from the start in the checks of the degraded grid. The first ballot needs no Phase 1, so its leader leads in the old grid and writes in the healthy column. The read invariants judge every quorum, the crashed members too, so they also hold when a member comes back with its disk. |
| `Hold` | what the leader of a ballot waits for. `phase1`: a Phase-1 quorum of each prior configuration promised its ballot (the decision hold of #579). `block`: the promises meet every read quorum of each prior configuration: on the grid, `F + 1` of one column, a Phase-2 quorum of the old grid. `block_short`: a control, one promise short of `block`. `none`. |
| `Gate` | what the hold gates. `ack`: the acks (zero-stall today). `ack_read`: the acks and the read basis of the ballot (the option of #1270 (the hold does not order two reads)). `propose`: also the proposals: the leader sends no `Accept` at its ballot before its hold holds. |
| `RecordCheck` | `TRUE`: after its acceptor answers, the reader asks a matchmaker majority and retries when one holds a registration of another configuration or a GC floor above its basis (rule 1 of #1254 (leaderless reads across a reshape)). |
| `EndRule` | the end slot of the old segment, written once at the bypass. `frontier`: one above the highest slot that the leader proposed at its old ballot (its allocator frontier). `chosen`: a control, one above the chosen prefix. |

The invariants: `Agreement`; `ReadSafe`, no read that can start now misses a slot that a client
saw (a slot that a leader acked, or a slot that an earlier read returned); `ReadSafeAcked`, the
same for the acked slots only (the property of `zero-stall/`). The liveness properties are
`AckAfterBypass` (the bypass ballot acks a slot) and `RetireAfterBypass` (a matchmaker majority
holds a GC floor at the bypass ballot), each under weak fairness of the protocol steps. Every
step grows a set or moves a status, a promise or a watermark up, so every fair behavior ends in a
state where no step is enabled, and the goals only grow. TLC checks each property as an invariant
of those end states (`AckedAfterBypass`, `RetiredAfterBypass`), which is the same property and
much faster than a temporal check.

Not in the model: matchmaker crashes (`quorum-everywhere/` has them), matchmaker generations, the
handoff and two bypasses in a row (`zero-stall/` has them). A read is one atomic step, as in
`zero-stall/`. In a real read the matchmaker check of `RecordCheck` must come after the acceptor
answers, so that a registration that an ack waited for is seen.

| Check | CI | Result |
|---|---|---|
| `reach_degraded_ack` | ci | violation of `NoDegradedAck`: a bypass ballot acks a slot while a column of the old grid is below quorum |
| `live_phase1_small`, `retire_phase1_small`, `live_phase1`, `retire_phase1` | ci, long | violation: with the decision hold of #579, the bypass ballot's writes are chosen in the new grid, but it never acks, and the old grid never retires (12 thousand states on `LShrink`) |
| `live_record_small`, `live_record`, `live_record_plus` | ci | pass: with the record check and no hold, the bypass ballot acks and the old grid retires; `_plus` adds a crash in the healthy column (`F` down there) |
| `live_block_propose_small`, `live_block_propose` | ci, long | pass: with the hold `block` before Phase 2, the same (23 thousand states, 17 minutes for the 3 × 2 grid) |
| `safe_record` | ci | pass: `ReadSafe` on `LSwap` with the column below quorum (51 thousand states) |
| `block_propose` | ci | pass: the same with the hold `block` before Phase 2 (28 thousand states, 7 minutes) |
| `phase1_acked` | long | pass: the decision hold keeps `ReadSafeAcked` (it is safe, only not live) |
| `hold_none` | ci | control: no hold, no record check; a read on the old basis misses an acked slot |
| `block_short`, `block_propose_short` | ci | control: one promise short of `block` |
| `phase1_ack_gate` | ci | violation: #1270 again; a read at the bypass ballot returns a slot before the hold |
| `phase1_reads_maj`, `block_reads_maj` | ci | violation (a finding): a hold that gates the acks and the read basis still lets a read on the **old** basis return a slot that the new grid chose, from the old vote of one acceptor; a second read on the old basis, at a quorum without that acceptor, misses it (a trace of 15 states, all acceptors live) |
| `phase1_propose_maj`, `block_propose_maj`, `safe_record_maj` | ci | pass: the hold before Phase 2, or the record check, orders those reads |
| `seal_frontier` | ci | pass: the end at the allocator frontier (1.3 million states, 7 minutes) |
| `seal_chosen` | ci | control: the end at the chosen prefix; a slot that the old ballot chose by a late quorum falls above the end, a rival's Phase 1 ignores its old votes, and two values are chosen |

The answer to #1258:

- **T1: the bypass does not rescue a column below quorum with the rule of #579.** The reading of
  the journal design thread holds. The bypass itself works: the leader moves to the new grid with
  no Phase 1, and its writes are chosen there. But the decision hold waits for a Phase-1 quorum of
  the old grid, which no longer exists. So no slot is acked, GC never runs, and the old grid never
  retires (`live_phase1`, `retire_phase1`).
- **The decision hold has a second gap, besides #1270.** Gating the read basis of the bypass
  ballot (the option of #1270) is not enough. A reader on the old basis can see the old vote of
  one acceptor for a slot that the new grid chose, wait until the slot is chosen, and return it. A
  second reader on the old basis, at a quorum without that acceptor, sees no higher promise and
  returns less (`phase1_reads_maj`). Neither `ack` nor `ack_read` closes this. The rule that
  #1270 chose for `zero-stall/` (a reader's chosen prefix counts a slot only once the ballot that
  chose it is covered) is not in this model. By its definition it waits for a Phase-1 quorum of the
  old grid, so it does not stay live with a column below quorum either.
- **Two rules close both gaps and stay live with a column below quorum**:
  1. **The segment-record check** of #1254, with no hold at all (`safe_record`,
     `safe_record_maj`, `live_record`). The reader retries when a matchmaker majority shows a
     segment above its basis; the bypass ballot registered at a majority before it proposed.
     GC then waits only for the carried slots to be chosen at the new ballot, and keeps
     `Agreement` with a rival (`zero-stall/gc_carry_nohold`, 4 million states, 3.5 minutes,
     `long`; a rival in this model's 3 × 2 grid did not finish in 25 minutes).
  2. **A hold before Phase 2 on a blocking set**: the bypassing leader sends no `Accept` at its new
     ballot before the acceptors that promised it meet every read quorum of each prior
     configuration. On the journal grid that is `F + 1` acceptors of one old column, which a
     column below quorum leaves (`block_propose`, `block_propose_maj`, `live_block_propose`). The
     fence of the code (the promise in each `PreReadAck`) then needs no matchmaker read. The
     bypass costs one round trip to one old column before the first write, not a full Phase 1.
     With one promise less it fails (`block_propose_short`).

  Both change the rule of #579, which is not built. Pierre chose rule 2 on 2026-11-20 (section
  2.13 of `docs/architecture/02-data-plane.md`), with the fence of the code on the read path
  (section 2.10, #1254).
- **The limit of the rule.** A reshape away from a column below quorum needs a live leader: the
  first leader after the loss cannot run Phase 1 over the old grid. If the leader is lost too, the
  journal stops writes until a member of the column comes back, as the grid controller's "hold and
  alert" after a zone loss says.
- **T2: the seal end of a bypass is the bypassing leader's allocator frontier**, one above the
  highest slot it proposed at its old ballot (`seal_frontier`). It is the end of design A when no
  Phase 1 ran: the leader proposed each slot that its own election's Phase 1 recovered before it
  may bypass, so the frontier is at least one above the highest vote that Phase 1 saw. An end at
  the chosen prefix chooses two values (`seal_chosen`).
- **T5: a heal that moves survivors to other columns is safe.** The layout is row-major, so a
  heal where `a7` replaces `a1` moves every member that stays to another column. The rule keeps
  one value (`config-register/heal_partitioned`, 32.8 million states); with Phase 1 one acceptor
  short in every column, TLC finds two (`heal_partitioned_short`).

### `health/`: the health measures of a journal's grid, for #1260 (journal health measures)

The grid controller acts on counts: the live acceptors of a column against a threshold
(`docs/architecture/05-failure-model-and-zones.md` section 5.1). The protocol acts on quorums.
This model checks that each count says what the quorums of `grid/Grid.tla` say, on every grid
that the fenced reshape accepts (`F + 1 <= acks <= rows - F` with `F >= 1`, so `rows >= 3` and
`2 <= acks <= rows - 1`), up to 9 acceptors, for every set of live acceptors. The state is one
layout, one live set, a live leader or not, and a placement that meets the zone rule or not. It
never changes, so each invariant is checked once on each state, as in `grid/`.

| Constant | Values |
|---|---|
| `Measure` | `code`: the counts of section 5.1 (`phase1_spare = live − (rows − acks + 1)`, `phase2_spare = live − acks`, `spare` the lower). `p2_only`, `majority`, `p1_only`: controls that read a wrong threshold. |
| `States` | `code`: the seven journal states of section 5.1. `draft`: a control, the states of the first draft (WritesDegraded and ElectionBlocked by the leader alone, no state for a grid with no writable column). |
| `HoldSlack` | `0`: the hold of a bypass counts `acks` promises of one column. `1`: a control, one less. |
| `Zones` | `0`: the grid checks. `2`, `3`: the zone checks, every member live and every map of up to six members to the zones. |

The invariants: `ElectableMeaning`, `WritableMeaning` and `HoldMeaning` (each count against its
quorum predicate); `HoldIsWritable` (the hold of section 2.13 can complete exactly when a column
can write); `SpareMeaning` (a spare in every column exactly when one more loss leaves every column
writable and an election possible); `OneState` (each live set is in exactly one state);
`ActionCanRun` (the fixed action of each state can run, or nothing can); `PartitionedOneSpare`
(on `rows = 2F + 1`, `acks = F + 1` the two spares are equal). The zone invariants: `NoPlacement`
(no placement meets rule 1 of the zone rule), `ZoneMarginMeaning`, and `ZoneLossEveryColumn`
(with row = zone on the partitioned grid, a zone loss takes every column to `spare = acks − 2`).

| Check | CI | Result |
|---|---|---|
| `health` | ci | pass: every grid invariant (29 thousand states, about 2 minutes) |
| `p2_only` | ci | control: electability judged on `acks` per column; `rows = 4`, `acks = 2`, a column with 2 live writes but cannot give Phase 1 |
| `majority` | ci | control: electability judged on a majority of all members; `rows = 4`, `acks = 3`, 2 of 4 live |
| `p1_only` | ci | control: writability judged on `rows − acks + 1`; `rows = 4`, `acks = 2` |
| `hold_short` | ci | control: the hold with one promise less than `acks` of one column |
| `draft_states` | ci | control: with no live acceptor, the draft puts a live leader in WritesDegraded, whose bypass hold cannot complete |
| `draft_one_state` | ci | control: `rows = 4`, `acks = 3`, 2 of 4 live is electable and writes nowhere; the draft has no state for it |
| `reach_writes_blocked` | ci | reached: a grid that the reshape accepts can be electable and write nowhere (WritesBlocked is not empty) |
| `zones2` | ci | pass: no placement over two zones survives a zone, on any grid up to six acceptors; `zone_margin` says what the quorums say |
| `zones3` | ci | pass: `zone_margin` on three zones; row = zone takes every column to the same spare at once |
| `zones3_placeable` | ci | control: on three zones TLC finds a placement that meets the rule |

The answer to #1260: the counts of the catalogue are exact on every grid inside the bound, but a
grid that is not partitioned needs both spares of a column, and the journal states need
WritesBlocked and Stopped beside the five of the draft. After a zone loss on three zones no heal
can meet the zone rule, so the controller holds and alerts. The model does not check the
controller's choice of plan; that is `decide`, tested with its crate (#1261 (grid controller
decision tests)).

### `segment-replicas/`: a write reaches only the replica set of its segment, for #1271

Compartmentalized Paxos says that every replica executes every write (sections 3.3 and 8.5 of
`docs/references/papers/scaling-rsm-compartmentalization`), so the replicas bound write
throughput. In design A each segment has its own replica set (`docs/architecture/02-data-plane.md`
section 2.10). The model checks that a write can go to the replicas of its segment only.

The journal has a sealed segment S1 (slots `0 .. K - 1`) with the replica set R1, and the open
segment S2 (slots `K .. NS - 1`) with the disjoint replica set R2. The log is abstract (the leader
chooses the slots in order; `multi-paxos/` checks the agreement and `experiments/segments/a/`
the seal end). A replica gets a chosen slot from the `Commit`, from the acceptors of the slot's
grid until the segment retires, or from a live peer of its own set. It applies the slots of its
segment in order and folds the journal state: `next_seq` and the leader uuid, with a `Write`
accepted only under the current leader and a `SetLeader` as a compare-and-set. A live replica
of the segment that applied a slot acks the writer. A reader reads the `seq` of an acked write
at one replica: the replica fixes the basis of its own segment (the leaderless read that `reads/`
checks) and answers once its applied prefix covers it, with the slot, or `Tail` (the `seq` is at
or above its `next_seq`), or `Truncated` (the `seq` is below what it knows). At most `F` replicas
of each set crash, at any time. S1 retires once `F + 1` replicas of R1 hold it, and then its
acceptors are gone.

The finding that the model forced: a replica holds opaque bytes, but the journal state is folded
at apply, so a replica of S2 that never sees a slot of S1 must start from the journal state at
the seal (the boundary state: `next_seq` and the leader uuid). The rule `Boundary` selects where
it lives. The design was `meta` (Pierre, 2026-11-20 morning: the leader uuid goes to the journal's
metadata, not to the segment record; `seal-chain/` then found that `record` needs fewer rules
through two seals in a row, #1303): the first replica of R1 that applied every slot of S1
writes, in one compare-and-set at the matchmakers, `next_seq` into the segment record and the
leader uuid into the journal's matchmaker record. The first leader of S2 reads the uuid there and
proposes it at slot `K`, a `Fence(u)` that sets the leader uuid. A replica of S2 takes `next_seq`
from the record and the uuid from slot `K`. The true fold ignores a `Fence`, so a `Fence` with a
wrong uuid shows. `record` (the whole state in the segment record) is the first form of the rule,
and the checks without the `meta_` prefix run it.

| Check | CI | Result |
|---|---|---|
| `safety` | ci | pass: `NoForeign` (no replica holds a slot of another segment), `OneFold` (every replica's fold is the fold of the whole log), `RecordTrue`, `AckTrue`, `ReadTrue` (a served read of an acked write returns that write); `K = 1`, `NS = 2`, three values (a write under each uuid and the `SetLeader` from `u1` to `u2`); 4.9 million distinct states |
| `live` | ci | pass: with `F` replicas of each set down, every chosen slot is acked, every read is served, and S1 retires (`Ends`, weak fairness) |
| `live_gone` | ci | pass: after the retire every replica of R1 stops; S2 still acks and serves (`EndsS2`) |
| `reach_cross` | ci | violation: a read of each segment is served, across the seal |
| `reach_retire` | ci | violation: S1 retires and a write of S2 is acked |
| `deliver_all` | ci | violation of `NoForeign`: every replica gets every slot (the paper's replica); the invariant sees a broadcast |
| `boundary_zero` | ci | violation of `OneFold`: R2 starts from the empty state, so its fold is not the fold of the whole log (a `seq` that S1 used is given again, or the fence uses a leader uuid that a `SetLeader` of S1 replaced) |
| `boundary_early` | ci | violation of `OneFold`: the record is written before its writer applied every slot of S1 (`K = 2`) |
| `boundary_seq` | ci | violation of `AckTrue`: the record holds `next_seq` only, and R2 starts with the first leader uuid. S1 decides `SetLeader(u2, u1)`; R2 then accepts a `Write` of the deposed writer `u1` that the whole log refuses |
| `route_other` | ci | violation of `ReadTrue`: a read goes to a replica of the other set, which does not hold the `seq`: it answers `Truncated` (below its boundary state) or `Tail` (above the seal) for an acked write that nobody trimmed |
| `retire_early` | ci | violation of `Ends`: S1 retires at `F` holders; the holder crashes and the slot is lost for the other replicas of R1 |
| `set_size_f` | ci | violation of `Ends`: a replica set of `F` members; with `F` down nothing applies |
| `gone_peer` | ci | violation of `EndsS2`: R2 asks a replica of R1 for the boundary state; R1 stops after the retire, before R2 asked |
| `retire_norecord` | ci | violation of `EndsS2`: S1 retires before the record holds the boundary state; R1 stops, and nobody can write it |
| `safety_k2` | long | pass, bounded at 40 minutes: `K = 2`, `NS = 3` |
| `meta_safety_ci` | ci | pass: the safety invariants with the `meta` rule; two replicas in each set (safety needs no quorum of replicas), the `SetLeader` from `u1` to `u2` and a write of `u1` |
| `meta_stale` | ci | violation of `AckTrue`: the same configuration as `meta_safety_ci`, but the first leader of S2 reads the metadata before the seal wrote the uuid and proposes `Fence(u1)`; S1 decided `SetLeader(u2, u1)`, and the deposed `u1` writes in S2 |
| `meta_safety` | long | pass, bounded at 40 minutes: the `meta` rule, `NS = 3` (the `Fence` takes slot 1) |
| `meta_live` | long | pass: `Ends` with the `meta` rule (20.7 million distinct states, about 55 minutes locally with 4 workers) |
| `meta_gone` | long | pass, bounded at 40 minutes: `EndsS2` with the `meta` rule |

The answer to #1271:

- **A write reaches only the replica set of its segment.** No replica outside the set holds a
  slot of the segment, and every fold, ack and read stays right. So, over the whole journal,
  the replicas do not bound write throughput: each replica set executes only its segment's
  writes. Inside one segment the bound of the paper still holds, because every replica of the
  set executes every write of the segment.
- **The boundary state is written at the seal.** This rule is new: the design said that a
  record holds the start slot, the end slot, the grid and the state. `next_seq` goes to the
  segment record and the leader uuid to the journal's matchmaker record, and the first leader of
  the next segment fences it into the log. `next_seq` alone is not enough (`boundary_seq`), and
  neither is a `Fence` read before the seal wrote the uuid (`meta_stale`). A replica of the next
  segment starts its fold there. A replica of the old set is not a source: it can be gone
  (`gone_peer`), and so can the zero state (`boundary_zero`) and a state written before the whole
  segment was applied (`boundary_early`). The record also gives the reader the `seq` range of
  each segment, so a read goes to the right set (`route_other`).
- **Retire.** `F + 1` holders of the set are enough, and the retire also waits for the boundary
  state in the record (`retire_early`, `retire_norecord`). After that the replicas of the next
  segment never need the slots of the old one (`live_gone`).
- **Liveness.** With `F` replicas of a set of `2F + 1` down, writes and reads of the segment end;
  a set of `F` members does not (`set_size_f`).
- No decision is refuted. The model has no `Truncate`; `segment-truncate/` (#1276 (user
  truncate across segments)) checks that the trim floor (`first_seq`) is part of the boundary
  state.

### `segment-truncate/`: a user truncate across segments, for #1276

Truncation is driven by the user only (no retention policy, no cursors; Pierre, 2026-11-19): a
`Truncate` decided by Paxos and judged at apply raises `first_seq`. `truncation/` and
`truncation-reconfig/` check it on plain Multi-Paxos and across one reconfiguration; this model
checks it with segments (`docs/architecture/02-data-plane.md` section 2.10).

The journal has a sealed segment S1 (slots `0 .. K - 1`, replica set R1) and the open segment S2
(slots `K .. NS - 1`, replica set R2), as in `segment-replicas/`. One writer, so no leader uuid
(`segment-replicas/` checks the writer fence). A value is a `Write` of one record or a
`Truncate(up)`, which raises `first_seq` to `up`, clamped to `next_seq`, and never lowers it.
A replica learns the slots of its segment from the acceptors (at or above their floor, until S1
retires) or from a live peer, applies them in order, and compacts lazily below the slot that
holds the record at its `first_seq`. The acceptors of a segment compact too. A laggard whose
next slot has no source jumps to the trim point of a peer (`TrimmedTo`). The first replica of R1
that applied S1 writes the boundary state into the record of S1. The record of S1 goes from
sealed to retired (`RetireAt` holders), to offloaded (the object holds the whole segment; the
replicas of R1 then drop their copy), and to trimmed from any of these three; the trim deletes
the object by its key. A read of `seq` q takes the floor from a replica of R2 at the basis of its
leaderless read: below it the answer is "trunc"; above it, the read goes to R2 for a slot of S2,
to a holder in R1 for a sealed or retired S1, to the object (served by the replica of R2) for an
offloaded S1, and is "trunc" for a trimmed S1.

| Check | CI | Result |
|---|---|---|
| `safety` | ci | pass: `FoldTrue`, `BoundaryTrue`, `ReadFloor` (no read returns a record below the floor decided when it started), `ReadKeep` (no record at or above the floor is lost, and "trunc" only below it), `TrimTrue` (a trimmed record is a sealed segment wholly below the floor), `Findable` (each record at or above the floor is at a live replica, the acceptors or the object), `TrimStays` (a trimmed record never becomes live again); `K = 2`, `NS = 4`, a write, `Truncate(1)`, `Truncate(2)`; one replica in R1, two in R2; 2.0 million distinct states, about 2 minutes |
| `crash` | ci | pass: the safety invariants with two replicas in each set, `F = 1`, retire at `F + 1`, a write and `Truncate(1)`; 6.3 million distinct states, about 6 minutes |
| `live_reads` | ci | pass: `Ends`: a run ends (no step is enabled) with every read served, every live replica of R2 at the end of the log, S1 offloaded or trimmed (trimmed when the floor covers it), and the object of a trimmed S1 deleted; no crash; 3.9 million distinct states, about 3 minutes |
| `live` | long | pass: `Ends` with `F = 1` and crashes, three replicas in R1 and two in R2, a write and `Truncate(1)`, no reads; 10.7 million distinct states, about 11 minutes with 4 workers |
| `reach_inside` | ci | violation: the floor lands inside S1 and a read of S1 above it is served |
| `reach_object` | ci | violation: a read of the offloaded S1 is served from the object, with a floor above 0 |
| `reach_trim_sealed`, `reach_trim_retired`, `reach_trim_offloaded` | ci | violation: S1 is trimmed from each of its three states |
| `reach_seal_truncate` | ci | violation: a `Truncate` at the last slot of S1 lands in the boundary state |
| `reach_jump` | ci | violation: a laggard jumps |
| `nofirst` | ci | violation of `ReadFloor`: the boundary state holds `next_seq` only. S1 decides `Truncate(2)` (clamped to 1); R2 starts with `first_seq` 0 and sends a read of `seq` 0 to a holder of R1 that did not compact yet |
| `trimstate` | ci | violation of `ReadFloor`: R2 takes `first_seq` from the trimmed segment records; S1 is not trimmed yet, so the floor is 0 |
| `holder` | ci | violation of `ReadFloor`: the read takes the floor from the holder in R1, whose fold stops at the seal; S2 decided `Truncate(1)` |
| `cache_unchecked` | ci | violation of `ReadFloor`: the replica of R2 serves the object of the offloaded S1 with no check of the floor; the object still holds the record below a floor inside S1 |
| `trim_intent` | ci | violation of `ReadKeep`: S1 is trimmed once a `Truncate` that covers it exists, before it is decided; a read above the floor answers "trunc" |
| `trim_open` | ci | violation of `Findable`: the open S2 is trimmed when its range so far is below the floor; a later write of S2 has nowhere to live |
| `acc_floor` | ci | violation of `Ends`: the acceptors of S1 compact to the decided floor before a replica of R1 applied S1; nobody can fold S1, the boundary state is never written, and R2 never starts |
| `nojump` | ci | violation of `Ends`: no `TrimmedTo`; a replica of R2 behind the floor waits for ever for a slot that the acceptors and its peer dropped |
| `crash_all` | long | pass, bounded: did not finish in 38 minutes with 2 workers (23.5 million distinct states, depth 26, 3.76 million states left on the queue), no violation; run of the second Fable review of the journal plan, 2026-11-20. A cut that finishes is #1321 |
| `live_all` | long | pass, bounded: did not finish in 38 minutes with 2 workers (16.7 million distinct states, depth 20, 6.2 million states left and growing), no violation; `Ends` is checked as an invariant on a prefix of the state space only. Same run, same issue |

The answer to #1276:

- **The trim floor belongs in the boundary state.** The writer of the boundary state writes
  `first_seq` with `next_seq` into the segment record, in the same compare-and-set
  (`nofirst`, `trimstate` fail without it). The `Open` point of section 2.10 is closed.
- **Floor inside a segment.** A read below the floor is refused and a read above it works on
  every holder, in each state of the segment (`reach_inside`, `reach_object`). The floor of a read
  comes from the segment in force (`holder`), and an offloaded read checks it before it serves
  the object or a cache (`cache_unchecked`).
- **Floor over whole segments.** A sealed, retired or offloaded segment becomes trimmed once a
  replica's fold has the floor at or above its last `seq`; the trim deletes the object by key; a
  trimmed record stays trimmed. The open segment is never trimmed (`trim_open`), and a trim
  never runs before the `Truncate` is decided (`trim_intent`). This agrees with `offload/`
  (#1256): the scrub deletes only objects of segments below the trim floor.
- **Truncate at a seal.** A `Truncate` at the last slot of S1 lands in the boundary state
  (`reach_seal_truncate`); a `Truncate` at the first slots of S2 is applied by R2 on top of it.
- **Lagging replica.** A replica behind the floor jumps to a peer's trim point and never
  fetches a slot of a trimmed segment (`nojump`). For that, the acceptors of a segment compact
  only to a slot that `F + 1` replicas of its set compacted to (`acc_floor`): a new rule.
- No decision is refuted. Left out: the leader uuid and the fence (`segment-replicas/`), the
  upload races (`offload/`), Phase 1 and the seal end (`experiments/segments/a/`); the
  matchmakers are one atomic register.

### `multi-writer/`: multi-writer mode through `SetLeader(None)` and `expected_seq`, for #1274

Pierre decided on 2026-11-20 that `SetLeader(new, old)` may name `new = None`: the journal is
then multi-writer, and the mode is what the last applied `SetLeader` left. There is no dedup
table: a single writer gets exactly-once through `expected_seq`, multi-writer is at-least-once,
and a held write is answered `Unknown` and resolved by a resend with the same `expected_seq`
(#1232 (held writes)). No model checked the switch between modes.

The log is abstract (the slots are chosen in order; `multi-paxos/` checks the agreement). Two
clients call `SetLeader` with a fresh uuid of their own or `None` (a compare-and-set against the
leader they read, which races with the slots in flight), send fenced writes under the uuid they
last won (pipelined, each at the position after the last one; a deposed client keeps sending),
send unfenced writes in any mode, and truncate up to the tail, fenced or not. A request stays in
the network after it is proposed, so it can be proposed again in another slot: a resend, a copy
of the transport, or a write that an old consensus leader proposed. One fold judges each slot at
apply and records one answer; a client may never see it (the held write). The invariants judge
every answer, so the answer to a resend is judged too. A client that sees a final refusal of a
fenced write while the journal is multi-writer writes it again, unfenced. With `SealAt`, the slot
`SealAt` is the first slot of the next segment: the fold of the old segment writes the boundary
uuid into the matchmaker record after its last slot, and the first leader of the next segment
proposes `Fence(uuid)` there (#1271 (a write reaches only the replica set of its segment)). The
oracle `OracleAt` computes the leader at each slot from the `SetLeader` commands alone, with no
fold and no segment, so a `Fence` with a wrong value shows.

The fold of a fenced write, in this order: below `first_seq` it is `truncated`; below `next_seq`
it is `dup` when the record there is the same write, else `taken`; a uuid that is not the
leader is `fenced`; above `next_seq` it is `early`; else it is applied. An unfenced write is
applied iff the leader is `None`, else `wrongmode`.

| Check | CI | Result |
|---|---|---|
| `safety` | ci | pass: `FenceTrue` (each record and each accepted truncate was judged under the leader that the `SetLeader` commands alone give), `AckTrue` (every `applied` or `dup` answer is the record at its `seq`), `RefusalFinal` (a `fenced` or `taken` answer to a fenced write is true now and for ever), `ExactlyOnce` (a write is at most once in the journal as a fenced record, and never both fenced and unfenced), `BasisFresh` (a writer never aims below the `next_seq` at which it started to lead), `FirstMonotone`; two clients, two writes each, two `SetLeader`, one `Truncate`, five slots, a fresh journal whose leader is `None`; 2.0 million distinct states |
| `unset` | ci | pass: the same with a fresh journal whose leader is unset (every write and truncate refused until a `SetLeader`); 1.5 million distinct states |
| `seal_long` | ci | pass: the same with a seal, the `Fence` at slot 4, six slots; 1.2 million distinct states |
| `safety_long` | long | pass: six slots, no seal |
| `reach_switch` | ci | violation: a write applied before `SetLeader(None, w)` is answered `dup` on its resend after it |
| `reach_back` | ci | violation: after `SetLeader(w2, None)` a fenced write of `w2` is applied |
| `reach_truncated` | ci | violation: an unfenced `Truncate` pushes a fenced write below `first_seq`, and its resend is answered `truncated` |
| `reach_fence_none` | ci | violation: the `Fence` at the seal carries `None`, and a write after it is applied |
| `multi_refusal` | ci | violation of `UnfencedRefusalFinal`, not a rule: an unfenced write lands from one copy while another copy is refused `wrongmode`; a resend cannot resolve an unfenced write |
| `fence_propose` | ci | violation of `FenceTrue`: the fence is judged against the applied state at propose and not at apply; an unfenced truncate proposed while the leader is `None` lands after `SetLeader(a, None)` |
| `fence_propose_write` | ci | violation of `FenceTrue`: the same with no truncate; a write lands under a leader that is not its own |
| `dedup_after` | ci | violation of `RefusalFinal`: the fence comes before the position; a write applied before `SetLeader(None, a)` is refused `fenced` on its resend |
| `dedup_after_twice` | ci | violation of `ExactlyOnce`: the same; the writer then writes it again, unfenced, and it is in the journal two times |
| `basis_stale` | ci | violation of `BasisFresh`: a writer that won keeps the basis it had and aims at a position of the mode before |
| `boundary_early` | ci | violation of `FenceTrue`: the first leader of the next segment reads the matchmaker record before the seal wrote the uuid, and proposes the `Fence` with the old value |
| `reach_below` | ci | violation of `NoBelow`: a resend after the seal finds its write below the boundary and is answered `below` |
| `seal_global` | ci | pass: the fold of the next segment reads the old record at apply (rule 2 of #1301); 1.2 million distinct states |
| `seal_local` | ci | violation of `RefusalFinal`: the fold of the next segment finds no record at a position of the old segment and answers `taken` (the code of `apply_write` today) |
| `seal_local_twice` | ci | violation of `ExactlyOnce`: the same; the writer then writes it again, unfenced, and it is in the journal two times |
| `seal_digest` | ci | violation of `RefusalFinal`: the boundary state keeps the last record of the old segment only, and the writer pipelined two writes |

The answer to #1274:

- **The decisions hold.** No decision is refuted. `SetLeader(None)` and `SetLeader(new, None)`
  switch the mode safely, with the fence judged at apply in both directions (`fence_propose`).
- **The resend resolves a held write only if the fold judges the position before the fence.**
  This rule is new. With it, a write in flight across `SetLeader(None, w1)` gets one of four
  answers on a resend: `dup` (written), `fenced` or `taken` (not written, for ever), `early`
  (resend later), or `truncated`. Without it the resend of a written write is refused, and a
  writer that moves to unfenced writes lands it two times (`dedup_after`, `dedup_after_twice`).
- **`truncated` is the one answer that resolves nothing**, as section 2.2 of
  `docs/architecture/02-data-plane.md` already says. In multi-writer mode any writer may
  truncate, so another writer can cause it (`reach_truncated`). The issue's invariant "every
  `Unknown` resolves by a resend" holds except there.
- **The switch back.** Only fenced writes of `w2` land after `SetLeader(w2, None)`; `w2` takes
  its basis from the view of the answer (`basis_stale`). The journal is safe without the basis
  rule (the position check refuses such a write); the rule makes the writer's first write land.
- **An unfenced write cannot be resolved by a resend** (`multi_refusal`): that is the
  at-least-once of multi-writer mode, also for a refusal.
- **The seal.** `Fence(None)` works: the matchmaker record holds `None`, and the leader reads it
  only after the seal wrote it (`seal_long`, `boundary_early`).
- **Open**: the leader of a fresh journal (`None` or unset). Both pass; the journal state model
  (#1189) decides.

The answer to #1301 (a resent write across a seal). The fold of a segment holds the boundary
state (`next_seq`, `first_seq`, the leader uuid), not the records of the old segment (#1271).
`Lookup` selects what it answers for a fenced write whose position is below `next_seq` and whose
record is in the old segment:

- **`"local"`, the code today** (`apply_write` reads `accepted_at` in its own log): it finds no
  record and answers `taken`. The trace of `seal_local`: `SetLeader(a)`, a write at position 1
  applied at slot 3, the seal at slot 4, the resend at slot 5 answered `taken` for a write that
  is in the journal. A writer that then writes it again lands it two times
  (`seal_local_twice`). Exactly-once breaks.
- **`"boundary"`, the rule** (rule 1 of the issue): the fold answers `below`. It is not final,
  like `truncated`. The client reads that position from the replica set of the old segment
  (reads route to the segment that holds the position, #1271), and the sealed segment does not
  change, so the read gives the fate: the record is the write (written), another record (not
  written, final), or a position below `first_seq` (`truncated`). All of `seal_long` passes, and
  `reach_below` shows the answer happens. The fold stays local: apply reads nothing outside the
  segment.
- **`"global"`** (rule 2): the fold reads the old record at apply. It is safe too
  (`seal_global`), but apply then waits for a read of another replica set (or of the offloaded
  object), so the fold of a segment is no longer a function of its own log. Not selected.
- **`"digest"`** (rule 3): the boundary state keeps the last `DigestSize` records of the old
  segment. It is safe only while the digest covers every write that a writer can have in flight
  across the seal: with one record and two pipelined writes it fails (`seal_digest`), with two it
  passes. The pipeline depth of a writer has no bound in the journal, so no digest size is safe.
  Not selected.

### `segment-records/`: segment records across a matchmaker set handover, for #1275

In design A (`docs/architecture/02-data-plane.md` section 2.10) the matchmakers of a journal hold
its segment records and its matchmaker record (the controller term; the leader uuid of the
boundary state is in the segment record since #1303, `seal-chain/`). Every change
is a compare-and-set on that state: the seal, the retire, the offload mark, the controller
election and the reshape call. `handover/` checks the floor and the configuration register across
a handover of the matchmaker set, but it has no segment records. This model runs each record write
at the same time as a handover.

The record is one value `<<st, end, term, cfg>>`: the state of one segment (open, sealed, retired,
offloaded), the seal end (one value stands for the pair that the seal's compare-and-set writes,
`next_seq` and the leader uuid, PR #1277), the controller term, and the count of reshapes. The
matchmakers hold it as the compare-and-set register of `cas-register/` (FoundationDB's
`LocalGenerationReg`): the operation with index `i` reads at a majority of one matchmaker
generation (each member raises its read generation to `i`), checks its condition on the value of
the highest write, and writes at generation `i`, one member at a time; a member takes the write
only if its read generation is at most `i` and its write generation is below `i`. The operation
is done at a majority. Every message is fenced by the matchmaker generation. The set `{X, Y, Z}`
hands over to `{Y, Z, W}` with the steps of `handover/` (stop, close on a frozen majority,
bootstrap, decree, activate), at any point of the record writes, and one machine can go down for
good. The old acceptors retire when a retire write is done; a replica drops its copy when a write
of the offload mark is done (the offloader's, or its own write of the same value).

| Check | CI | Result |
|---|---|---|
| `seal` | ci | pass: two first leaders race to seal, then a retire; every invariant; 4.1 million distinct states, about 2.5 minutes with 4 workers |
| `offload` | ci | pass: from a retired record, the offload mark and a replica that writes it again before it drops; every invariant; 0.46 million distinct states |
| `control` | ci | pass: two elections, then a reshape of the controller of term 1, deposed by the second election; every invariant; 5.1 million distinct states |
| `mixed` | long | pass: from a sealed record, a retire, an election and a reshape of term 1; every invariant; 7.5 million distinct states, about 5 minutes |
| `reach_seal`, `reach_offload` | ci | violation of `NoCrossRun`: a write is done at each generation, then the old acceptors retire (or a replica drops its copy) |
| `close_one` | ci | violation of `Carried`: the successor copies the register of one member of the stop quorum, not the highest write; a seal that a majority took is lost |
| `close_one_term` | ci | violation of `ReshapeFenced`: the same, and the successor holds term 1 after the election of term 2; the deposed controller's reshape is done |
| `frozen_serves` | ci | violation of `Carried`: a frozen member still takes writes, so a compare-and-set is done on the old set after the stop quorum closed; the successor does not hold it |
| `drop_read` | ci | violation of `DropKept`: a replica drops its copy after a read that shows the offload mark; the mark was on one member only, and a majority that does not hold it (the successor's too) says retired |

The invariants: `Carried` (each write that a majority took is returned by each serving majority
of its generation and of each later one, or a later value), `NoBackward` (the done writes, in
the order of their generations, never move back: the state never returns, the seal end never
changes, the term and the reshapes never go down), `SealOnce` (every done seal wrote the same
end, and every serving majority returns that end or none), `ReshapeFenced` (a reshape of term 1
is done only if no earlier election raised the term above 1), `RetireKept` and `DropKept` (the
old acceptors retire, and a replica drops its copy, only when every serving majority, old or
new, holds the record at or after retired, or offloaded).

The answer to #1275:

- **The handover carries every record write that a majority took.** The successor takes the
  highest write of the stop quorum and the highest read generation, as `handover/` does for the
  configuration register (`close_one`, `close_one_term`). No record moves back and the seal end
  is written once.
- **A compare-and-set on the old set after the stop fails**, because a frozen member takes
  nothing (`frozen_serves`).
- **A deposed controller changes nothing across the handover**: the term crosses with the
  record, and the reshape checks it (`control`, `close_one_term`).
- **A rule that the model found: a replica drops its copy only after a write of the offload mark
  is done at a majority**: the offloader's own compare-and-set (it then tells the replicas), or
  the replica's own compare-and-set of the same value. A read that shows the mark is not enough
  (`drop_read`): the mark can be on a minority, and a read of another majority, or the successor
  after a handover, says retired. The same holds for each step that acts on a record: it acts on
  a write that it saw done, never on a read alone. This control fails with no handover too; the
  handover is one more way to lose a mark that a minority holds.
- No decision is refuted.

### `offload/`: the offloader and the races of offload, for #1256

In design A (`docs/architecture/02-data-plane.md` section 2.10) a sealed segment goes to an object
store, and its record in the journal's matchmaker record says `offloaded`. The upload is done by
the offloader, a stateless role (Pierre, 2026-11-20). For each retired segment it reads the
segment from a replica of the segment, builds the object with deterministic bytes, writes it with
a conditional put at the key journal id + segment number, and marks `offloaded` in the record by
compare-and-set. A replica drops its copy only after the mark.

The model has one segment of `L = 2` slots, three replicas (`F = 1`) that apply its slots in
order, and two offloaders of two releases (a rolling upgrade: the release is in the header, so
their bytes differ). One offloader can stop for good between any two steps; the controller can
stop between the record write of a trim and its delete. A mark can be lost (the write or its
reply), and the offloader then starts again from the record. A user truncate trims the segment at
any time (no fairness). The store is atomic: a put, a delete and the listing of one key are single
steps. The liveness checks have weak fairness on every step but a trim, a lost mark and a stop,
and strong fairness on the mark (a lost mark that comes back for ever is a network that never
heals).

| Check | CI | Result |
|---|---|---|
| `safety` | ci | pass: `ObjectTrue` (each object holds the whole segment), `Findable` (a slot that is not trimmed is at the acceptors before the retire, at `F + 1` replicas after it, and in the object once the record says offloaded), `AllOffloaded` (the segment ends offloaded or trimmed) and `NoOrphan` (no object of a trimmed segment stays for good); one stop; 14,627 distinct states |
| `safety_crash2` | ci | pass: the same with two stops (both offloaders, or one offloader and the controller) |
| `gate_sealed` | ci | pass: an offloader may also take a sealed segment that did not retire yet |
| `tomb` | ci | pass: the trim writes a tombstone at the key and nobody scrubs, while no controller stops |
| `safety_np2_l1` | long | pass: two segments of one slot, trimmed in order; 8,636,080 distinct states, about 40 minutes locally |
| `safety_np2` | long | pass, bounded at 40 minutes: two segments of two slots |
| `reach_duplicate` | ci | violation: a put finds the object of the other offloader |
| `reach_lateupload` | ci | violation: a put lands after the trim and its delete |
| `reach_stopbeforemark` | ci | violation: an offloader stops with its object in the store and no mark |
| `reach_alldropped` | ci | violation: every replica dropped the copy of an offloaded segment |
| `build_partial` | ci | violation of `ObjectTrue`: the offloader reads a replica that did not apply the whole segment |
| `conflict_bytes` | ci | violation of `AllOffloaded`: when its put finds an object, the offloader marks only if the bytes equal its own. The offloader of the other release put and stopped; the one that is left never marks |
| `conflict_abandon` | ci | violation of `AllOffloaded`: when its put finds an object, the offloader stops the attempt. After a lost mark, every next attempt finds its own object |
| `recheck_anyfail` | ci | violation of `Findable`: the offloader deletes the key on any failed mark. Its mark fails because the other offloader marked first, and it deletes the marked object |
| `late_upload` | ci | violation of `NoOrphan`: no delete on a failed mark and no scrub. The put of a slow offloader lands after the trim deleted the key |
| `stop_before_mark` | ci | violation of `NoOrphan`: the delete on a failed mark, no scrub. The late put lands, and its offloader stops before the mark |
| `scrub_unmarked` | ci | violation of `Findable`: the scrub also deletes an object with no mark. It deletes the object between the put and the mark, and the mark then names an object that is gone |
| `trim_record` | ci | violation of `NoOrphan`: the trim deletes only the object that the record names (marked). The mark was lost, the trim came, and the object stays |
| `drop_uploaded` | ci | violation of `Findable`: a replica drops its copy once the object is in the store, before the mark. The record does not name the object, and fewer than `F + 1` replicas hold the segment |
| `tomb_ctl_stop` | ci | violation of `NoOrphan`: tombstones and no scrub; the controller stops between the record write of the trim and the tombstone, and the next controller does not know that a delete is to do |
| `tomb_blind` | ci | violation of `NoOrphan`: tombstones with a put that overwrites; the late put replaces the tombstone |

The answer to #1256:

- **Late upload after trim.** An offloader that read `retired` can put after the trim deleted the
  key (`reach_lateupload`). A check of the record before the put cannot close it: the trim can
  come between the check and the put (`late_upload`). Two rules close it. The mark is a
  compare-and-set from "not offloaded and not trimmed", so it fails after a trim, and **an
  offloader whose mark finds the record trimmed deletes the key**. That is not enough when the
  offloader stops between the put and the mark (`stop_before_mark`), so **the controller scrubs**:
  it lists the store and deletes each object of a segment below the trim floor. The scrub needs
  no record: the segment number is in the key.
- **Lost record write.** After a lost mark the object has no mark. The next attempt finds the
  object, and **an offloader whose put finds an object reads it and marks when it holds the whole
  segment, whatever its release**. An offloader that gives up on a found object never marks
  (`conflict_abandon`), and one that compares bytes never marks across a rolling upgrade
  (`conflict_bytes`). If a trim comes before the next attempt, the trim deletes by the key, not
  by a location in the record (`trim_record`), and the scrub deletes it too.
- **Duplicate upload.** The conditional put keeps one object (`reach_duplicate`). The two
  offloaders can build different bytes (two releases), but every object holds the same slots,
  because **the offloader reads only a replica that applied every slot of the segment, up to the
  `next_seq` of the record** (`build_partial`). A failed mark deletes the key only when the record
  says trimmed: a delete on any failure deletes the marked object of the other offloader
  (`recheck_anyfail`).
- **Drop before mark.** A replica drops its copy only after the mark (`drop_uploaded`), as #1213
  (segments experiment) found. The scrub never deletes an object whose segment is not trimmed:
  the object of an offloader between its put and its mark is not an orphan yet
  (`scrub_unmarked`).
- **Retired or sealed.** An offloader may also take a sealed segment that did not retire yet
  (`gate_sealed` passes). Safety needs one replica that holds the whole segment, and `F + 1`
  holders until the mark; the retire is not needed. The design keeps "the controller offloads
  retired segments" (decided), which this model shows is safe, not required.
- **Tombstones.** A tombstone at trim also stops the late upload, with no scrub (`tomb`). But it
  is not enough alone: a controller that stops between the trim and the tombstone leaves a key
  open (`tomb_ctl_stop`), so a tombstone needs a list or a durable delete to do, as the scrub
  does, and it leaves one object per trimmed segment for good. The design takes the scrub.
- No decision is refuted.

What the model leaves out: the matchmakers are one atomic register (`matchmaker/` and
`handover/` check the quorum; `segment-records/` checks that the mark is a write done at a
matchmaker majority, across a handover); the store is atomic and has read-after-write consistency, as S3
and GCS give for a single key; a reader of the object is not modelled (`Findable` checks that the
bytes are where the record says); the trim floor of the core (`truncation/`) is one bit per
segment here.

### `seal-chain/`: two seals in a row, and where the boundary uuid lives, for #1303

The journal state is folded at apply, and each segment has its own replica set
(`segment-replicas/`), so a replica of a segment starts from the boundary state of the segment
before it: `next_seq`, the trim floor `first_seq` and the leader uuid. `segment-replicas/` has one
seal. This model has a chain of `G` segments and a seal at any time: before the first slot of a
segment (an empty segment), before its `Fence`, and before its first leader read the uuid. The
log is abstract (the leader chooses the slots in order; a seal closes the open segment at the
frontier). A segment's replica set is one fold here: it applies the slots of its segment from its
boundary state and answers the writer of each slot. The values are a write and a `Truncate` of
`u1`, the `SetLeader` from `u1` to `u2`, the `Noop` of the gap fill and `Fence(u)`. The truth is
the fold of the whole log.

| Constant | Values (first: the design that passes) |
|---|---|
| `Boundary` | `record`: the first replica of a sealed segment that applied every slot writes `next_seq`, `first_seq` and the uuid into the record of that segment by compare-and-set; the next segment starts from it, with no `Fence`. `meta`: the record holds `next_seq` and `first_seq`, the uuid goes into the journal metadata (one field) in the same compare-and-set, the first leader of the next segment reads it and proposes `Fence(uuid)`, and a replica takes the uuid from the `Fence`. |
| `BaseFrom` | (`record`) `pred`: segment `g` starts from the record of `g - 1`. `latest`: a control, from the highest record written. |
| `FenceRead` | (`meta`) `rec`: the leader of `g` reads the uuid once the record of `g - 1` is written, in the same read of the register. `any`: a control, once the metadata holds a uuid. `stale`: a control, at any time. |
| `EmptyMeta` | (`meta`) what the boundary write of a segment with no `Fence` applied does to the metadata. `keep`: nothing. `blank`: a control, it writes the uuid of the empty state. `wait`: a control, it waits for a uuid. |
| `SealGuard` | (`meta`) `TRUE`: a seal waits for the `Fence` of the segment. |
| `GapFill` | `FALSE`: in `meta`, the first slot of a segment is its `Fence`. `TRUE`: a `Noop` or a pipelined command can come first, and a replica refuses every command of its segment until it applied a `Fence` (#1302, candidate 1); in `record`, a `Noop` can take any slot. |

Safety: `AckTrue` (each writer gets the answer of the whole log: no deposed writer writes, no
`seq` twice), `OneFold`, `RecordTrue`, `NoMoveBack` (`next_seq` and `first_seq` never move back
across the seals), and in `meta` `FenceTrue` and `MetaTrue`. Liveness: `Ends` (weak fairness;
every slot is chosen, so the first write of a segment is never stuck behind its boundary state,
every sealed segment's record is written and every slot is acked). Three segments and four
slots, unless the name ends in `_g4` (four segments, five slots).

| Check | CI | Result |
|---|---|---|
| `record`, `record_gap`, `record_g4`, `record_gap_g4` | ci | pass: the safety invariants (17.7 thousand, 52.8 thousand, 283 thousand and 1.1 million distinct states) |
| `record_live` | ci | pass: `Ends` |
| `record_latest` | ci | violation of `AckTrue`: segment 3 starts from the record of segment 1 while the record of segment 2 is not written; segment 2 took a `SetLeader`, and the deposed `u1` writes in segment 3 |
| `record_reach_empty`, `record_reach_moved` | ci | violation: an empty middle segment and a write taken in segment 3; a `SetLeader` in segment 2 and the deposed writer refused in segment 3 |
| `meta`, `meta_gap`, `meta_g4`, `meta_guard` | ci | pass: the safety invariants |
| `meta_live`, `meta_gap_live`, `meta_guard_live` | ci | pass: `Ends` (`meta_guard_live` with `SealGuard` and `wait`) |
| `meta_any` | ci | violation of `AckTrue`: segment 1 is empty, segment 2 takes `Fence(u1)` and the `SetLeader` to `u2`; the leader of segment 3 reads `u1`, the uuid of the first seal, before the second seal wrote it, and `u1` writes in segment 3 |
| `meta_stale` | ci | violation of `AckTrue`: the leader of segment 2 reads the metadata before the first seal wrote it |
| `meta_blank` | ci | violation of `AckTrue`: segment 1 takes the `SetLeader` to `u2`, segment 2 is empty, its seal writes `u1` into the metadata, and `u1` writes in segment 3 |
| `meta_wait`, `meta_wait_gap` | ci | violation of `Ends`: segment 2 is sealed with no `Fence`, its replica has no uuid to write, and segment 3 never gets its `Fence` |
| `meta_reach_empty`, `meta_reach_moved` | ci | violation: the same reach checks as `record_` |
| `meta_gap_g4` | long | pass (5.5 million distinct states, 3 minutes) |

The answer to #1303: both places are safe and live through two seals in a row, but not with the
same rules. Pierre chose the segment record on 2026-11-20.

- **The uuid in the segment record** needs no rule beyond "start from the record of your
  predecessor". An empty segment copies that record as it is, a `Noop` anywhere is harmless, and
  no `Fence` slot is needed, so the gap fill question of #1302 (the `Fence` slot against the
  `Noop` gap fill) does not come up.
- **The uuid in the journal metadata** needs two more rules, each with a control that lets a
  deposed writer write: the leader reads the uuid in the same read as the record of the
  predecessor (`meta_any`), and the boundary write of a segment with no `Fence` leaves the
  metadata as it is (`meta_blank`; `meta_wait` stops the journal, unless a seal waits for the
  `Fence`, `meta_guard_live`). It also needs the `Fence` slot and, with the gap fill, the refusal
  rule of #1302. Here a segment chooses at most one `Fence`; a real log with leader changes can
  choose two copies, and then a replica must apply only the first (#1302; no model of that case
  is in the tree, because the design has no `Fence` slot).

What is not in the model: the crashes and the reads inside one replica set (`segment-replicas/`),
the seal end and the agreement (`experiments/segments/a/`, `multi-paxos/`), the register across a
matchmaker handover (`segment-records/`), multi-writer mode (`multi-writer/`).
