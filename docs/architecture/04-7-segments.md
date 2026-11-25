[← 04-6-parity.md](04-6-parity.md) · [Index](README.md) · [05-failure-model-and-zones.md →](05-failure-model-and-zones.md)

### 4.7 Segments in the core

**A segment is a range of slots of one Multi-Paxos log and the replicas that keep it, and it ends
at a decided command, the `Roll`** (Pierre, 2026-11-24, the plan
[`docs/archive/journal-design-a/prompt.md`](../archive/journal-design-a/prompt.md)). Segments are
an optional behaviour of `paros-consensus`, like the matchmakers, the proxy leaders and the
replica tier (§4.3): the `Some` arm of the same state machine. The plain arm does not change. The
journal that comes later composes them and writes no consensus of its own.

This section is the desired state. None of it is built yet; the order of work is §6 of the plan.
The model of the cut is `specs/tla/segment-roll/` (step 1, PR #1540 (segment-roll)).

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
| D4 | The replicas of a segment are the `NodeId`s that its `Roll` names | **Open**: proposed in the session of 2026-11-24, Pierre did not object; he confirms it on this section |
| D5 | A `roll` on a deployment with no matchmakers is refused, as `reconfigure` is (`ReconfigureRefusal::NoMatchmakers`) | **Open**: as D4 |
| D6 | The journal of design A (its crates, walks, docs and TLA+ models) is removed. The journal is built again later, from scratch, on the core with segments | Pierre, 2026-11-24; done in PR #1538 (remove the journal of design A), at the tag `journal-design-a` |
| D7 | The layering above the core (the journal as the final crate, the roles of the core on `paros-host`, storage and network as adapters) comes after this work | Pierre, 2026-11-24 |

#### 4.7.3 Rules

Each rule names its source, or says that it is a departure. The rules are those of §4.1 of the
plan, with the result of the model of step 1.

- **R1. A segment is a range of slots and its replica set.** The replica set keeps the range
  after the acceptors drop it: replicas in the sense of Compartmentalized Paxos (§3.3), a
  `ReplicaNode` each. A segment has an id, a start, an end once it is sealed, its replicas and a
  state: `Open`, `Sealed`, `Held`, `Offloaded`, `Trimmed`. The grid is not part of a segment: a
  grid belongs to a round (Matchmaker Paxos §4.2).
- **R2. The cut is a command.** `Control::Roll { next, replicas }` is a control command of the
  core, like `Noop`. Only the apply path reads it. It is valid when `next` is the tail segment
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
  - **R4a. The pause.** A leader proposes nothing above a `Roll` of its own ballot until that
    `Roll` is chosen at its ballot. **Open**: with R4d the model shows that the pause is not
    needed for routing (`barrier_off_commit`, `barrier_off_full`); whether it stays is
    Pierre's call, asked on 2026-11-25.
  - **R4b. A hole takes a `Noop`.** The election gap fill never writes a `Roll` into a hole. This
    is the rule of the core today. With R4d it is not needed for routing either
    (`hole_any_commit`), and it stays because the core has it.
  - **R4c. A new leader takes its tail segment from its own chosen prefix** and from the values
    that its Phase 1 recovered, in slot order, never from the chain alone: the chain is behind
    the log by up to one roll. The model needs it (`route_commit_base_chain`).
  - **R4d. A `Commit` leaves on a known prefix.** A leader sends the `Commit` of slot `s` to the
    replicas of a segment only once it knows that every slot below `s` is chosen. Its route is
    then a fold of a chosen prefix, so it is the true segment. Proposals still pipeline; only
    the `Commit` waits. This is R3 applied to the leader. It was found by the model of step 1:
    with three ballots and R4a to R4c on, a later ballot can choose, below a slot that a middle
    ballot chose and routed, the `Roll` of an older ballot that the middle ballot's Phase 1 did
    not see (`route_propose`). Two other candidates were compared: the window of Paxos Made
    Simple §3 (a `Roll` at `s` takes effect at `s + α`, and a leader proposes only `α` slots
    ahead of its chosen prefix), which with `α = 1` is no pipeline; and a heal by the replicas
    (a "sealed" answer and a resend), which adds a message and a repair.
- **R5. The chain is an add-only map at the matchmakers.** The matchmaker registry gets a map of
  segment records beside `history`. Each field that comes from the log (the start, the end, the
  replicas that the `Roll` named, the boundary state) is written once, and every writer writes
  the same value, because each is a function of the chosen log. The state only grows. A read is
  a merge over a matchmaker quorum, and a handover carries the map by the same merge, as it
  carries `history` and the retirement records. There is no compare-and-set. Source: the
  registry of Matchmaker Paxos §3.2.
- **R6. The replicas of a segment.** A `Commit` goes to the learners as today (D2), and also to
  the replicas of the segment of its slot. A replica starts at the start of its segment from the
  boundary state (the `JournalState` that `Message::TrimmedTo` carries), with the trim-point
  jump of the core. It stops at the end. It catches up from the acceptors while they hold the
  slots, and from a peer replica after. A read that reaches a sealed replica at or above its end
  is answered "sealed, the next segment is `k + 1`", and the reader fetches the chain again.
  Source: Delos §3.1 (the "sealed" error and the refetch).
- **R7. Held, and the floor.** A segment is `Held` when `F + 1` of its replicas hold every slot of
  it and its seal is in the chain. The compaction floor of an acceptor may then rise to the end
  of that segment. The floor follows the chain, so it never passes an end that is not published.
  **Departure 2** (§4.7.4).
- **R8. After the seal.** A heal replaces one replica of a sealed segment, by one `Decree` over
  the matchmaker set, numbered by the count of heals of the record. A trim marks a sealed segment
  whose records are all below the trim floor. Source: Delos `reconfigModify`.
- **R9. The grid does not change here.** A grid change is `ColocatedNode::reconfigure`, a round
  change, as today. It cuts no segment.
- **R10. Opt-in.** With no matchmakers a `roll` is refused (D5), no `Roll` is ever chosen, and no
  new message, `HardState` field or round trip enters the plain path. The bootstrap `Config`
  names the replicas of the first segment; with none, the deployment has no segments.
- **R11. One segment is a valid deployment.** A deployment that never calls `roll` stays at one
  open segment for ever, and behaves as the matchmaker deployment of today.
- **R12. Offload to an object store is prepared, not built.** A sealed segment has a fixed range
  and fixed content, so one object can hold it, with the state `Offloaded` in the chain. The
  object store is I/O and stays out of `paros-consensus`. Source: Delos (the BackupLoglet).

The cooperative handoff (`relinquish_to`) needs no rule of its own: the successor keeps the
ballot, and with R4d its route is a fold of the chosen prefix that it learns
(`specs/tla/segment-roll/`, the section of the model in `specs/tla/README.md`).

#### 4.7.4 Departures from the papers

| # | Rule | What no paper has | Check |
|---|---|---|---|
| 1 | R2, the `Roll` | A command that ends a range of learners. Its nearest source is Paxos Made Simple §3 (a reconfiguration is a command of the state machine). Stoppable Paxos also decides the end in the log, but it stops the instance; the `Roll` stops nothing | `specs/tla/segment-roll/` (`safety_small`, `safety`, `safety_bare`; controls `act_vote` and the `reach_*` checks) |
| 2 | R7, the floor at `Held` | Scenario 3 of Matchmaker Paxos §3.5 and §4.5 (the chosen prefix on `f + 1` replicas), used for the compaction floor. `node/gc.rs` declines Scenario 3 today. The GC watermark rule does not change | to write in step 6 (`truncation/`, `truncation-reconfig/`) |

R4d is not a departure: it changes no vote and no choice. It says when a learner message leaves,
from what the leader learned.
