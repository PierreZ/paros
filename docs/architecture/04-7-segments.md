[← 04-6-parity.md](04-6-parity.md) · [Index](README.md) · [04-8-journal-controller.md →](04-8-journal-controller.md)

### 4.7 Segments in the core

**A segment is a range of slots of one Multi-Paxos log and the replicas that keep it, and it ends
at a decided command, the `Roll`** (Pierre, 2026-11-24, the plan
[`docs/archive/journal-design-a/prompt.md`](../archive/journal-design-a/prompt.md)). Segments are
an optional behaviour of `paros-consensus`, like the matchmakers, the proxy leaders and the
replica tier (§4.3): the `Some` arm of the same state machine. The plain arm does not change. A
journal above the core would compose them and write no consensus of its own (a later plan, D7).

This section is the desired state; the order of work is §6 of the plan. The model of the cut is
`specs/tla/segment-roll/` (step 1, PR #1540 (segment-roll)), and `specs/tla/multi-paxos-roll/`
puts the `Roll` on the Multi-Paxos model (step 3).

**What is built (step 3).** `paros-consensus` has the cut: `Command::Roll` and the types of
`journal/segment.rs` (`Roll`, `SegmentId`, `Segment`, `ReplicaSet`), the fold of R2 in
`JournalState::apply` (`Outcome::Rolled`, `Outcome::RollIgnored`), and the driver call
`ColocatedNode::roll` (`node/roll.rs`), refused with no matchmakers (D5) and with the tail of R4c.
The tail segment is a field of `JournalState`, so it is sealed at the floor and carried by
`TrimmedTo` with the rest of that state; the plan's separate `WriteOp` for the segment cursor is
not needed. The `segments` flavor of consensus-sim rolls, with the `Roll` rule in its journal
oracle and a mutant on the trim jump.

**What is built (step 4).** The chain at the matchmakers (R5): `journal/chain.rs` has the record
of a segment (`SegmentRecord`: a start, an opening with the replicas that the `Roll` named, an
end) and its merge, field by field, where two values of one field are a detected safety violation.
`JournalScalars::segments` holds the chain of the journal (a matchmaker serves one journal, §3.4). A node publishes the end of a segment
after its own fold applied the `Roll` (`SegmentPublish`, `node/chain.rs`): the walk, a refold
past a healed hole, a trim-point jump and the boot each publish the `Roll`s they fold, so an end
reaches the matchmakers even when every node that walked it restarted. It publishes to every matchmaker
of the set it believes in force, and the `Publisher` role (`publisher.rs`) counts the acks
(`SegmentAck`) until a majority holds it. A matchmaker merges the publish
(`Matchmaker::publish_segment`) and refuses it when it is not active for the generation. The
handover merges the chains of the stop quorum, and an activation merges the result into the
member's own chain. The model is the chain of `specs/tla/handover-scalars/` (`ChainTrue`,
`ChainCarried`, `ChainGrows`, with a control for each rule). The judges: the oracle
`CHAIN_TRUE` of consensus-sim with the mutant `EarlyPublish` (a publish on a proposal), and claim
11 of the handover model checker (`models/handover.rs`). Not in step 4: the boundary state in
the record and the chain in the matchmaking and probe answers, which come with the reader of the
chain (step 5); the states past `Sealed` (`Held` in step 6, `Trimmed` and `Offloaded` later; a
record is sealed when its end is known). **Open**: the chain has no size limit; it grows by one
record for each `Roll` until the states past `Sealed` let a matchmaker forget old segments (step 6
decides). The model of the replicas of a segment is `specs/tla/compartments/` (the join from
the boundary state, the stop at the end, the "sealed" answer; `specs/tla/README.md`).

**What is built (step 5, first part).** A `ReplicaNode` keeps one segment (R6). Its `Replica` is
bound to the segment of its base at boot (`Replica::bind_segment`): the walk, the refold and the
boot stop at the `Roll` that ends it, and the replica is then sealed (`Replica::is_sealed`). A
sealed replica keeps the `Roll`, refuses each slot past its end (the `past_end` counter), takes no
trim point, pulls nothing, and answers a read at or past the end with `LogRead::Sealed` (the end
and the next segment) and a quorum read there in `ReplicaReady::sealed_reads`. A fresh replica
joins a later segment from the boundary state (`ReplicaNode::join_segment`, refused off a fresh
replica or off a state that is not the start of a segment).

**What is built (step 5, second part).** The route of R4d. The journal state holds the replicas of
the tail segment (`JournalState::replicas`): a valid `Roll` that names a set gives it to the
segment it opens, and a `Roll` with none keeps the set before it (D4). The bootstrap configuration
names the replicas of the first segment (`Config::segment_replicas`; `None` is the plain arm). A
leader routes the `Commit` of each slot that its own fold applied to the replicas of the segment of
that slot (`Audience::ReplicasOf`, `node/route.rs`), so the route is a fold of a chosen prefix; the
`Commit` to the learners does not change (D2). A routed `Commit` carries a ballot that chose its
slot: the ballot of the leader's own decision, or of a `Commit` it learned. It never carries the
ballot of the leader's own record, which can stand at a later ballot that chose nothing (a
recovery vote for the chosen command). A proxy leader keeps its `Commit` to the learners:
the leader's walk routes every slot, delegated or not. The chain record holds the boundary state
(`Opening::boundary`, the journal state after the `Roll`), which the oracle `CHAIN_TRUE` of
consensus-sim judges against its own fold of the chosen log; the oracle `ROUTE_TRUE` judges each
routed `Commit` (its controls are the mutants `RouteFromTail`, caught by `ROUTE_TRUE`, and
`RouteOnDecision`, caught by `ROUTE_ON_PREFIX`). Segments exist only with matchmakers,
so a deployment without them routes nothing. **Open** (step 8): two kinds of slot are not routed. A slot that a
follower folded and the leader of its time never routed is not routed by the next leader. A slot
that the leader learned with no choosing ballot (a catch-up replay, the decided tail of a handoff, a
heal in Phase 1) is not routed either. Today the `Commit` to the
learners reaches every replica, and with pure voters that `Commit` goes away. Not built yet: the catch-up between
replicas, the chain in the matchmaking and probe answers, and the walk of consensus-sim with
replicas of segments that join from the chain. The acceptors may compact past the `Roll` of a segment before its
replicas hold it, since the compaction of a `Truncate` does not wait for `Held` (R7: a segment
that a `Truncate` released counts as held); a replica of that segment that lags below the `Roll`
then gets only trim points of a later segment and refuses each one. **A lagging replica of a
released segment stops as trimmed** (Pierre, 2026-11-27, on the card "Choose how a lagging
replica of a released segment finishes"; found by the `segment-replicas` walk of consensus-sim):
a `TrimmedTo` also carries the sender's `first_seq` at the head of its fold, since the state
sealed at the point can predate the `Truncate` that raised the point. When that state is in a
later segment and proves every write of the replica's segment below that `first_seq`
(`JournalState::releases`: it reaches the state's `next_seq`, or the `next_seq` of a `Hold` of
that segment or a later one), the replica marks its segment trimmed
(`ReplicaNode::is_trimmed`): it pulls nothing, serves no read (`LogRead::NotHeld`) and ignores
later trim points, and it opens no quorum read (`ReadOpen::Trimmed`). The mark is not durable:
after a boot, the replica pulls and serves its store again until the next trim point proves the
release again. In that window it serves what its store holds, under the fold it reached, as any
replica that lags behind a `Truncate` does.
Its model is `specs/tla/held-floor-trimmed/` (`TrimProof`, with the control `trim_segment`: a
trim point of any later segment as the proof marks a segment that its peers hold). **Open**
(step 6): a lagging replica of a segment that its peers hold (`Held` by reports, not released)
still refuses every trim point; the catch-up between replicas heals it. **Open** (step 6): a slot past the end that a replica learned before its seal (above a
hole below the `Roll`) stays in its durable store; the seal drops it from memory and the boot
skips it, and no `WriteOp` deletes a record above the floor. The states past `Sealed` decide
what a sealed replica's store keeps.

#### 4.7.1 The rule of the papers

**The agreement on each slot follows the papers** (Pierre, 2026-11-24). A rule that no paper has
is a departure, and each departure has its TLA+ check: a check that passes with the rule, and a
control that fails without it. A change that removes a departure is preferred to a change that
adds one. The departures of segments are listed in §4.7.4. This rule was in §J3.1 of the
journal of design A (tag `journal-design-a`); it applies to the whole core.

#### 4.7.2 Decisions

| # | Decision | Status |
|---|---|---|
| D0 | The chain of segments lives in the matchmakers | Pierre, 2026-11-24 |
| D1 | A segment ends at a decided command, the `Roll`. It is not computed from votes and raced at a register, as in design A | Pierre, 2026-11-24 ("Go roll") |
| D2 | Acceptors stay learners until step 8 of the plan: a chosen write goes to every acceptor and to the replicas of its segment. Step 8 makes the acceptors pure voters (#1271 (pure voters)): a chosen write then goes to the replicas of its segment only | Pierre, 2026-11-24 ("Keep"); changed 2026-11-25: pure voters are part of this work ("keep only 1271", "Later as now as part of the consensus work") |
| D3 | The matchmakers store the chain as the paper stores its registry: an add-only map, merged like `history`. A change of the replicas of a sealed segment (a heal) is one `Decree` | Pierre, 2026-11-24 |
| D4 | The `Roll` carries an optional new location: `replicas: Option<..>`. `None` keeps the replicas of the segment that it ends, so the next segment lives on the same machines; `Some` names the `NodeId`s of the next segment. The caller of `roll` chooses; balancing the load is for later, above the core | Pierre, 2026-11-25 ("keep simple, we will load balance afterwards"; "an optional new location passed as an option during roll") |
| D5 | A `roll` on a deployment with no matchmakers is refused, as `reconfigure` is (`ReconfigureRefusal::NoMatchmakers`): a roll needs healthy matchmakers | Pierre, 2026-11-25 |
| D6 | The journal of design A (its crates, walks, docs and TLA+ models) is removed. A journal above the core is for a later plan (D7) | Pierre, 2026-11-24, changed 2026-11-25; done in PR #1538 (remove the journal of design A), at the tag `journal-design-a` |
| D7 | The layering above the core and the journal built again on the core are dropped from this plan; a new plan comes after this work. `paros-host` is removed | Pierre, 2026-11-24; changed 2026-11-25 ("keep only 1271, and drop the layering and the new journal", "Remove Paros host too") |
| D8 | `F` of R7 is the `f` of the acceptor configuration: a held segment survives as many lost replicas as the acceptors of its slots did | Taken on 2026-11-26 under the rule "always take the recommendation" (Pierre, 2026-11-22); Pierre confirmed it on 2026-11-26 ("Go with the rest") |
| D9 | The floor at `Held` is a decided command, `Command::Hold`, as `Truncate` is: the leader proposes it once its tally saw the segment held, and every node raises its floor where its fold applies it. From the slot of the `Hold`, every node answers a retry of a write below the end of the segment "held". The drop keeps every record that a retry chosen before the `Hold` reads, as the compaction of a `Truncate` does. A floor that each node took from its own tally made two nodes judge one retry two ways (`held-floor/`, the control `hold_tally`) | Taken on 2026-11-26 under the rule "always take the recommendation" (Pierre, 2026-11-22); Pierre confirmed it on 2026-11-26 ("Go with the rest") |
| D10 | The replica set of the open segment changes by a decided command, `Command::SetReplicas { segment, replicas }`, proposed only by `ColocatedNode::set_replicas` (step 5d of the plan). It is judged at the fold and valid only for the tail segment; from the next slot the leader routes each `Commit` to the new set. A sealed segment's set changes at the matchmakers (R8). A rule that no paper has: its model `specs/tla/segment-reseat/` comes first. **Open** (that model): the join of a new replica and what it serves | Pierre, 2026-11-26: the mechanism confirmed ("Go with the rest"), the name `SetReplicas` chosen |

#### 4.7.3 Rules

Each rule names its source, or says that it is a departure. The rules summarize §4.1 of the
plan, with the result of the model of step 1. The plan keeps the details that this section
does not repeat: the table of who writes each field of the chain, the order of an offload, and
the open questions of its §8 (a new replica for the open segment, who asks for a roll and its
fence, where the segment id lives, the reply owner, the garbage of the chain).

- **R1. A segment is a range of slots and its replica set.** The replica set keeps the range
  after the acceptors drop it: replicas in the sense of Compartmentalized Paxos (§3.3), a
  `ReplicaNode` each. A node of the pool also runs the `Replica` role as a learner (D2); it is in
  no replica set unless a `Roll` names it. A segment has an id, a start, an end once it is sealed, its replicas and a
  state: `Open`, `Sealed`, `Held`, `Offloaded`, `Trimmed`. The grid is not part of a segment: a
  grid belongs to a round (Matchmaker Paxos §4.2).
- **R2. The cut is a command.** `Command::Roll(Roll { next, replicas: Option<..> })` is a command of the
  core, beside `Write` and `Control`, so that no call that proposes a `Control` can carry it: only
  `ColocatedNode::roll` proposes one. Only the apply path reads it. It is valid when `next` is the tail segment
  plus one. A valid `Roll` chosen at slot `s` ends the tail segment at `s + 1` and opens `next`
  at `s + 1`. A `Roll` that is not valid changes nothing. **Departure 1** (§4.7.4).
- **R3. A node acts on the chosen prefix only.** A replica applies only the contiguous chosen
  prefix of its own segment, and publishes an end only after its own walk applied the `Roll`.
  This is the one safety rule: Paxos gives one value for each slot, so the first valid `Roll`
  after the start of a segment is at the same slot for every node. The model checks that R3
  alone keeps one end for each segment, with every routing rule off (`safety_bare`). Source:
  the replica rule of the core (`replica.rs`: the fold is contiguous or it stops).
- **R4. Routing.** These rules keep a `Commit` away from the wrong replicas. Without them a
  `Commit` can go astray, and two ends still cannot exist.
  - **R4a. No pause** (Pierre, 2026-11-25). The plan had a pause: a leader proposes nothing
    above a `Roll` of its own ballot until that `Roll` is chosen at its ballot. With R4d the
    model shows that routing does not need it (`barrier_off_commit`, `barrier_off_full`), so
    it is dropped: a leader pipelines across a `Roll`. The switch `Barrier` of
    `specs/tla/segment-roll/` keeps the pause as a control.
  - **R4b. A hole takes a `Noop`.** The election gap fill never writes a `Roll` into a hole. This
    is the rule of the core today. With R4d it is not needed for routing either
    (`hole_any_commit`), and it stays because the core has it.
  - **R4c. A new leader takes its tail segment from its own chosen prefix** and from the values
    that its Phase 1 recovered, in slot order, never from the chain alone: the chain is behind
    the log by up to one roll. The model needs it (`route_commit_base_chain`).
  - **R4d. A `Commit` leaves on a known prefix** (Pierre, 2026-11-25, with R4a). A leader
    sends the `Commit` of slot `s` to the replicas of a segment only once it knows that every
    slot below `s` is chosen. Its route is then a fold of a chosen prefix, so it is the true
    segment. Proposals still pipeline; only the `Commit` waits. This is R3 applied to the
    leader. It was found by the model of step 1: with three ballots, the pause on and R4b and
    R4c on, a later ballot can choose, below a slot that a middle
    ballot chose and routed, the `Roll` of an older ballot that the middle ballot's Phase 1 did
    not see (`route_propose`). Two other candidates were compared: the window of Paxos Made
    Simple §3 (a `Roll` at `s` takes effect at `s + α`, and a leader proposes only `α` slots
    ahead of its chosen prefix), which with `α = 1` is no pipeline; and a heal by the replicas
    (a "sealed" answer and a resend), which adds a message and a repair.
- **R5. The chain is an add-only map at the matchmakers.** The matchmaker registry gets a map of
  segment records beside `history`. Each field that comes from the log (the start, the end, the
  replicas, which the `Roll` names or keeps from the segment before (D4), the boundary state) is written once, and
  every writer writes the same value, because each is a function of the chosen log. The state
  only grows
  (`Open` < `Sealed` < `Held` < `Offloaded` < `Trimmed`). A read is
  a merge over a matchmaker quorum, and a handover carries the map by the same merge, as it
  carries `history` and the retirement records. There is no compare-and-set. Source: the
  registry of Matchmaker Paxos §3.2.
- **R6. The replicas of a segment.** A `Commit` goes to the learners as today (D2), and also to
  the replicas of the segment of its slot. From step 8 on, it goes to the replicas of the
  segment of its slot only (pure voters, #1271). A replica starts at the start of its segment from the
  boundary state (the `JournalState` that `Message::TrimmedTo` carries), with the trim-point
  jump of the core. It stops at the end. It catches up from the acceptors while they hold the
  slots, and from a peer replica after. A read that reaches a sealed replica at or above its end
  is answered "sealed, the next segment is `k + 1`", and the reader fetches the chain again.
  Source: Delos §3.2 (the "sealed" error and the refetch; in Delos it answers an append, here
  a read too).
- **R7. Held, and the floor.** A segment is `Held` when `F + 1` of its replicas hold every slot of
  it and its seal is in the chain. The compaction floor of an acceptor may then rise to the end
  of that segment. To an acceptor, a held segment is a `Truncate` whose records are kept
  elsewhere: the floor stays inside the chosen prefix, a `Prepare` below the floor is refused,
  and a laggard below the floor jumps with `TrimmedTo`. The floor follows the chain, so it never
  passes an end that is not published. The floor moves on a decided `Hold` (D9): the leader
  proposes it only for a segment that its tally saw held, and each node drops the records below
  the end where its fold applies the `Hold`. From that slot, a retry of a write below the end
  is answered "held" on every node, and the reader asks the replicas of the segment. A retry
  chosen before the `Hold` folds as a duplicate, and a node that folds it again (after a jump or
  a boot) reads the record of its write. So the drop keeps every record that a retry chosen
  above the new floor reads, as the compaction of a `Truncate` does
  (`Replica::compaction_target`, without the retries that fold "held"): the floor stays below
  the end, and only a later drop whose fixed point lies above the retry lifts it. A
  `TrimmedTo` carries the floor of its sender, so it lands a laggard on a floor that some node
  held. A caller of `ColocatedNode::compact(up_to)` can ask for less than the target, so the
  clamp walks down from the floor `up_to + 1` to a fixed point of the same walk
  (`Replica::compaction_floor`), and `Replica::truncate` asserts that its floor is one: a floor
  between a retry and the record that it reads would fold the retry `Refused` on a refold (the
  class of #204). The plain `Compact` of a `Truncate` lands on the same fixed point:
  `compact_kept` in `specs/tla/held-floor-hold/` passes with `c1`, `c2`, a retry of `c2` and a
  `Truncate` that releases both writes. The target is slot 4, and a `Compact` that stops below it
  at slot 2 would cut the retry from its record; the floor lands on slot 1 or 3 instead
  (`reach_compact_kept`). Its control `compact_any`, the `Compact` of `truncation/`, which lands at
  any point up to its target, breaks `RetryKept`. `compact_target` passes on a target that is
  not closed itself (a retry of `c1` and a `Truncate` that releases `c1`).
  The tally of the step 6 code: a sealed `ReplicaNode` whose floor is the start of its segment
  answers each beat with `Message::SegmentHeld`; the leader counts a report only from a
  replica that its fold names for that segment, with the end its fold gives
  (`collector::HeldTally`, wired in `node/held.rs`); `AcceptorConfig::is_held_by` is the one
  comparison with `F`; the end is in the chain once the node's own publish completed
  (`Publisher::is_published`). `ColocatedNode::held_segments` lists the segments marked `Held`.
  A leader proposes a `Hold` for each segment that its tally marks, in segment order
  (`node/held.rs`, `propose_holds`), and the fold judges it (`JournalState::apply`): a `Hold` is
  valid only for the segment right after the held prefix, over a sealed segment, so the held
  segments of the log are a prefix (`FloorTo = "prefix"`, whose control `floor_any` loses a
  slot). A `Hold` carries the end of its segment and the next position at its `Roll`, from the
  leader's fold; the fold checks that both lie past the held prefix and inside what it reached
  (`JournalState::held`). The model names only the segment, and reads the end from the chain.
  Once a segment is held, a single-writer retry of a write below that position is
  `Outcome::Held`, judged before the log is asked: the write's slot is below the end exactly
  when its position is below the position at the `Roll`, because positions follow the slot
  order (this equivalence is a reading of this step). `Held` is never an ack and carries no
  fence verdict: a write below that position with a stale uuid or other bytes is `Held` too, and
  the replicas of the segment answer what the log holds. The drop at the acceptors is a policy
  of the replica (`replica::HeldRecords`): the replica of a `ColocatedNode` drops
  (`HeldRecords::Drop`, set at the boot), and a `ReplicaNode` keeps (`Keep`), since a bound
  replica keeps the segment that it is bound to. When the fold applies a `Hold`, a dropping
  replica asks for a compaction (`Replica::advance` returns a target), and
  `Replica::compaction_target` raises the floor to the end of the held prefix
  (`HoldDrop = "keep"` of `specs/tla/held-floor-hold/`). The floor then walks down to the fixed
  point of the retries, as for a `Truncate`. A retry that folded `Held` reads no record, so it
  holds no floor down. **Decided** (Pierre, 2026-11-26): a released
  segment counts as held. A segment whose writes all lie below `first_seq` is marked
  `Held` with no replica, since a `Truncate` already released its records; in the model, a
  segment with no write is released as soon as it ends. The code releases a segment with no
  write once `first_seq` reaches its position, never earlier (`HeldTally::judge`): a `Roll`
  never sets the replicas back to none, so the segments with no replicas are a prefix, and an
  empty one has the position of the segment before it, which needs the same release. The held segments are a prefix, so without this rule a
  segment with no replicas (the first segment of a journal with no `Config::segment_replicas`)
  stops every `Hold` after it. The check is `Skip = "released"` of
  `specs/tla/held-floor-released/`: a released segment with no replicas, then a held segment
  with one, and the floor passes both (`reach_skip`; with `Skip = "none"` it never does,
  `skip_none`). Its control `norep` (a segment with no replicas marked `Held` at once) loses a
  record, and `released_part` keeps a segment that a `Truncate` released only in part. In the
  code, `HeldTally::judge` marks a released segment once its end is published, with no report,
  and `ColocatedNode::judge_released` judges the released segments after each walk, since a
  `Truncate` releases a segment with no report or publish after it. **Open**
  (step 6): the tally notes a segment from the outcomes that a node keeps above its
  floor, so a leader whose floor passed a `Roll` (a restart, or a trim-point jump) forgets a
  segment that another leader held. The rule covers that segment only once it is released; the
  model has one `held` set for every leader and does not show the case. **Open** (step 6): the fold checks the end and the position of a `Hold`
  against bounds, not against the exact values of its segment; the drop needs them exact (the
  oracle of consensus-sim checks them exact). The tally never forgets a sender that reported, and keeps a report
  from the seal, before the chain publishes the end. Its safety rests on a fault model that the
  tally cannot see and the deployment must keep: at most `F` replicas are down at one time, where
  a replica is down from the loss of a segment that it held whole until it copies the segment
  again. This is the reading of this step, not a decision of Pierre; the model with losses over
  the whole run (`rlost < F`) stays beside it. Under that model a sender that lost its disk after
  it reported still counts, and the floor is safe: `held_ever` in `specs/tla/held-floor-retry/`
  passes with three replicas, `F = 1`, a tally that counts every reporter, and losses bounded at
  one time. Its control `held_ever_down` (`F + 1` replicas down at one time) loses a record, and
  `reach_held_stale` reaches a mark of `Held` while fewer than `F + 1` replicas hold the segment,
  on the report of a replica that lost its disk since. `F` is the `f` of the configuration
  that the leader believes in force when it judges (`judge_held`), not of the configurations
  that chose the slots of the segment; for a grid, `tolerated_failures` is the availability `f`
  (the minimum over the two phases). Pierre confirmed `F = f` on 2026-11-26 (D8); which
  configuration counts is the reading of this step. A segment held under one `f` stays held when
  a later configuration raises `f`: only a heal (R8) adds copies.
  **Open** (step 6):
  a sealed replica that compacted inside its segment, or jumped into it, reports nothing. It
  holds every slot that a decided `Truncate` did not release, but the rule that counts it needs
  a model with replicas that compact (`held-floor/` has none).
  **Departure 2** (§4.7.4).
- **R8. After the seal.** A heal replaces one replica of a sealed segment. The set can return to
  an earlier value, so it is not add-only: each change is one `Decree` over the matchmaker set, numbered by the count of heals of the record. A trim marks a sealed segment
  whose records are all below the trim floor. Source: Delos `reconfigModify`.
  **Decided** (Pierre, 2026-11-26): the heal has two halves. The core repairs the copies of the
  set that is named: a replica that lost slots of its segment pulls them from a peer of the same
  set (peer catch-up), and there is no choice to make. The journal controller (§4.8) changes the
  set: a replacement is an order, as autoscaling is. No code comes before the models of the four
  points below:
  - **Open** (safety): the held tally never forgets a report (R7). Across a change of the set
    it must count a report only from a member of the set in force, and a new member counts as
    down until it holds the segment whole. Model first: a TLA+ pass and a control, with
    `specs/tla/segment-reseat/` (D10) and a model of the heal of a sealed segment.
  - **Open** (liveness): the heal of a sealed segment is a `Decree` at the matchmakers, which
    the leader's fold does not see. The tally and the route must learn the new set from the
    chain.
  - **Open**: `ReplicaFeedback` (§4.8) carries no fact of a segment or of `Held`, so the
    controller cannot see a sealed segment that lost a copy.
  - **Open**: who proposes the `Decree` of the heal of a sealed segment.
- **R9. The grid does not change here.** A grid change is `ColocatedNode::reconfigure`, a round
  change, as today. It cuts no segment. What the removed journal called a reshape is two calls:
  `reconfigure(config)` and `roll(Some(replicas))` (D4).
- **R10. Opt-in.** With no matchmakers a `roll` is refused (D5), no `Roll` is ever chosen, and no
  new message, `HardState` field or round trip enters the plain path. The bootstrap `Config`
  names the replicas of the first segment; with none, the deployment has no segments.
- **R11. One segment is a valid deployment.** A deployment that never calls `roll` stays at one
  open segment for ever, and behaves as the matchmaker deployment of today. What one segment
  cannot do: move the log off the acceptors, which needs a sealed segment, and so a roll.
- **R12. Offload to an object store is prepared, not built.** A sealed segment has a fixed range
  and fixed content, so one object can hold it, with the state `Offloaded` in the chain. The
  object store is I/O and stays out of `paros-consensus`. Source: Delos (the BackupLoglet).

The cooperative handoff (`relinquish_to`) needs no rule of its own: the successor keeps the
ballot, and with R4d its route is a fold of the chosen prefix that it learns
(`specs/tla/segment-roll/`, the section of the model in `specs/tla/README.md`).

#### 4.7.4 Departures from the papers

| # | Rule | What no paper has | Check |
|---|---|---|---|
| 1 | R2, the `Roll` | A command that ends a range of learners. Its nearest source is Paxos Made Simple §3 (a reconfiguration is a command of the state machine). Stoppable Paxos also decides the end in the log, but it stops the instance; the `Roll` stops nothing | `specs/tla/segment-roll/` (`safety_small`, `safety`, `safety_bare`; the control `act_vote`; the `reach_*` checks show that the cases are reached); `specs/tla/multi-paxos-roll/` (the `Roll` as a command of the Multi-Paxos model: `roll` with `Inv`, the control `act_vote`) |
| 2 | R7, the floor at `Held` | Scenario 3 of Matchmaker Paxos §3.5 and §4.5 (the chosen prefix on `f + 1` replicas), used for the compaction floor. `node/gc.rs` declines Scenario 3 today. The GC watermark rule does not change | `specs/tla/held-floor/` (extends `truncation/`: the controls `held_f`, `Held` at `F` replicas loses a record, and `held_log`, a floor past an end that the chain does not publish, and `floor_any`, a floor past a segment that is not held; the passes `floor_prefix`, and `held` and `held_loss` in `specs/tla/held-floor-pass/`; a tally that never forgets a report, under at most `F` replicas down at one time: the pass `held_ever`, the control `held_ever_down` and the reach check `reach_held_stale`, in `specs/tla/held-floor-retry/`; the jump of a laggard below the floor; the decided `Hold` of D9: the pass `hold_decided` in `specs/tla/held-floor-hold/`, where every node gives one answer to a retry and a duplicate always has its record, the control `hold_tally`, a floor from each node's tally, which gives two answers, the control `hold_any`, a `Hold` of a segment that is not held, which loses a record, the reach checks `reach_hold_judge` and `reach_hold_drop`; the `Compact` of a `Truncate` over a retry: the passes `compact_kept` and `compact_target` in `specs/tla/held-floor-hold/`, where every `Compact` lands on the fixed point of the retries, and the control `compact_any`, a `Compact` that lands at any point up to its target; the retries in the log: the passes `hold_retry`, `hold_keep` and `hold_inner` in `specs/tla/held-floor-retry/`, where every floor stays closed (`RetryKept`), the control `hold_end`, a drop to the end over a retry that reads below it, and the control `hold_jump`, a jump to a point that no node held (the rule: a `TrimmedTo` carries the floor of its sender)) and `specs/tla/held-floor-reconfig/` (extends `truncation-reconfig/`: a reconfiguration onto an empty member after the acceptors dropped a held segment) |

R4d is not a departure: it changes no vote and no choice. It says when a learner message leaves,
from what the leader learned.
