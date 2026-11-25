[← 04-6-parity.md](04-6-parity.md) · [Index](README.md) · [05-failure-model-and-zones.md →](05-failure-model-and-zones.md)

### 4.7 Segments in the core

**A segment is a range of slots of one Multi-Paxos log and the replicas that keep it, and it ends
at a decided command, the `Roll`** (Pierre, 2026-11-24, the plan
[`docs/archive/journal-design-a/prompt.md`](../archive/journal-design-a/prompt.md)). Segments are
an optional behaviour of `paros-consensus`, like the matchmakers, the proxy leaders and the
replica tier (§4.3): the `Some` arm of the same state machine. The plain arm does not change. The
journal that comes later composes them and writes no consensus of its own.

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
`JournalScalars::segments` holds the chain of each journal. A node publishes the end of a segment
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
decides). Not built yet: the replicas of a segment (R6) and the
route of R4d, which need replicas to route to (step 5).

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
| D2 | Acceptors stay learners: a chosen write goes to every acceptor and to the replicas of its segment. Acceptors that only vote (#1271) are a later piece of work | Pierre, 2026-11-24 ("Keep") |
| D3 | The matchmakers store the chain as the paper stores its registry: an add-only map, merged like `history`. A change of the replicas of a sealed segment (a heal) is one `Decree` | Pierre, 2026-11-24 |
| D4 | The `Roll` carries an optional new location: `replicas: Option<..>`. `None` keeps the replicas of the segment that it ends, so the next segment lives on the same machines; `Some` names the `NodeId`s of the next segment. The caller of `roll` chooses; balancing the load is for later, above the core | Pierre, 2026-11-25 ("keep simple, we will load balance afterwards"; "an optional new location passed as an option during roll") |
| D5 | A `roll` on a deployment with no matchmakers is refused, as `reconfigure` is (`ReconfigureRefusal::NoMatchmakers`): a roll needs healthy matchmakers | Pierre, 2026-11-25 |
| D6 | The journal of design A (its crates, walks, docs and TLA+ models) is removed. The journal is built again later, from scratch, on the core with segments | Pierre, 2026-11-24; done in PR #1538 (remove the journal of design A), at the tag `journal-design-a` |
| D7 | The layering above the core (the journal as the final crate, the roles of the core on `paros-host`, storage and network as adapters) comes after this work | Pierre, 2026-11-24 |

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
  the replicas of the segment of its slot. A replica starts at the start of its segment from the
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
  passes an end that is not published.
  **Departure 2** (§4.7.4).
- **R8. After the seal.** A heal replaces one replica of a sealed segment. The set can return to
  an earlier value, so it is not add-only: each change is one `Decree` over the matchmaker set, numbered by the count of heals of the record. A trim marks a sealed segment
  whose records are all below the trim floor. Source: Delos `reconfigModify`.
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
| 2 | R7, the floor at `Held` | Scenario 3 of Matchmaker Paxos §3.5 and §4.5 (the chosen prefix on `f + 1` replicas), used for the compaction floor. `node/gc.rs` declines Scenario 3 today. The GC watermark rule does not change | `specs/tla/held-floor/` (extends `truncation/`: the controls `held_f`, `Held` at `F` replicas loses a record, and `held_log`, a floor past an end that the chain does not publish; the passes `held` and `held_loss`; the jump of a laggard below the floor) and `specs/tla/held-floor-reconfig/` (extends `truncation-reconfig/`: a reconfiguration onto an empty member after the acceptors dropped a held segment) |

R4d is not a departure: it changes no vote and no choice. It says when a learner message leaves,
from what the leader learned.
