# Prompt: remove the journal work, then add segments to `paros-consensus`

You work in the repository `PierreZ/paros`. This prompt is from Pierre. It was written in a
design session on 2026-11-24, against commit `a2ab2eb`.

**Do two things, in this order, and nothing else.**

1. **Remove the journal work** (step 0 of section 6).
2. **Add segments to `paros-consensus`**, with the `Roll` cut (steps 1 to 8 of section 6).

**Do not** build a new journal, a new crate above the core, an adapter for storage or for the
network, or acceptors that only vote. Section 7 lists what comes later; start none of it.

**Status of the design.** Nothing in it is built. Nothing in it is model-checked: TLC did not
run in the design session. Each result in step 1 is a prediction, and step 1 checks it. If a
prediction fails, stop and report to Pierre before you go on.

**How to work.** Follow `AGENTS.md`. The order is the usual one: a TLA+ model first, then the
docs of the desired state, then the code, then the walk, the oracle and the mutant. Where this
prompt and `AGENTS.md` disagree, `AGENTS.md` wins and the difference goes to Pierre. Stop for
Pierre after step 1 (the report of the model) and after step 2 (the docs PR).

---

## 1. The problem

*This section is the reason for the work. Step 0 removes the code that it names.*

`paros-journal` wrote consensus again. It imports only the kernel of the core (`Acceptor<V>`,
the Phase-1 merge and paging functions, `Rounds`, `Decree`, the membership types and the
scalars). It never imports `matchmaking`, `replica`, `replica_node`, `proxy_leader`,
`quorum_read`, `collector`, `belief` or `node`. It has its own copy of each:

| The core has | The journal wrote again |
|---|---|
| `matchmaker` and its generation handover | `register/`: a second matchmaker (member, call, handover), about 2,000 lines |
| `matchmaking`, `node/election`: registration, then Phase 1 over `H_b` | `Change::Bind` and `roles/leader/phase1.rs` |
| `node`: election, recovery, gap fill, GC wiring | `roles/leader.rs` |
| `proxy_leader` | `roles/proxy.rs` |
| `replica`, `replica_node`, catch-up, the trim jump | `roles/replica.rs` |
| `quorum_read`, `belief` | the read path and the fence of the journal roles |
| `journal/command.rs`, `journal/state.rs` | `command.rs`, `fold.rs` |

**The cause.** The rule "no journal concept enters the core" met a segment that is one grid
(design A, `docs/journal/03-segments-and-records.md` §J3). A segment of design A changes what a
ballot binds to, what Phase 1 reads, where a vote may land and who learns a slot. Each of these
is a decision of consensus. So the journal could not compose the roles of the core.

**The decision** (Pierre, 2026-11-24). The segment comes down into `paros-consensus`, as an
optional behaviour, like the matchmakers, the proxy leaders and the replica tier. The plain arm
does not change.

## 2. Decisions

| # | Decision | Status |
|---|---|---|
| D0 | The chain of segments lives in the matchmakers | Pierre, 2026-11-24 |
| D1 | A segment ends at a decided command, the `Roll` (not design A: an end computed from votes and raced at the register) | Pierre, 2026-11-24 ("Go roll") |
| D3 | The matchmakers store the chain like the paper: an add-only map, merged like `history`. A change of a replica set (a heal) is one `Decree` | Pierre, 2026-11-24 |
| D4 | The replicas of a segment are `NodeId`s that the `Roll` names | Proposed in the session; Pierre did not object. Confirm in step 2 |
| D5 | A `roll` on a deployment with no matchmakers is refused, as `reconfigure` is (`ReconfigureRefusal::NoMatchmakers`) | Proposed in the session; Pierre did not object. Confirm in step 2 |
| D2 | Acceptors stay learners in this plan: a chosen write still goes to every acceptor and to the replica set of its segment. "Acceptors only vote" (#1271) is a later piece of work | Pierre, 2026-11-24 ("Keep") |
| D6 | The journal crates, their walks, their docs and their TLA+ models are removed. The journal is built again later, from scratch, on the core with segments | Pierre, 2026-11-24 |
| D7 | The layering above the core (the journal as the final crate, the roles of the core on `paros-host`, storage and network as adapters) comes after this work | Pierre, 2026-11-24 ("we will do the rest later") |

Pierre's rule of 2026-11-24 applies: the agreement on each slot follows the papers, a rule that
no paper has is a departure with its TLA+ check, and a change that removes a departure is
preferred to a change that adds one. (The rule is in §J3.1 of `docs/journal/`, which step 0
removes: the docs PR of step 2 gives it a home in the docs of the core.)

## 3. What each paper gives

- **Matchmaker Paxos.** A grid belongs to a round, not to a range of slots. A reconfiguration is
  a move to the next round (§4.2, §4.3). The replicas "can also be safely added or removed at
  any time", as long as a command that `f + 1` replicas hold stays on `f + 1` replicas (§5).
- **Compartmentalized Paxos.** The votes are with the acceptors. The log and its execution are
  with the replicas. They are two roles.
- **Delos.** The log is a chain of ranges in one register, each range with its own
  configuration. Only the last range takes appends. A change of a sealed range does not stop
  the tail. A reader with an old chain gets "sealed" and fetches the chain again.

Together: the acceptors do not change at the boundary of a segment, so nothing must be sealed.
Every party must only agree on the last slot of the segment. A log decides one value for each
slot, so a command decides it.

Stoppable Paxos (`docs/references/papers/stoppable-paxos/`) also decides the end in the log.
But it must stop the instance: nothing may be chosen above the stop, so the slots are no longer
independent (E4 to E6, voiding). The `Roll` stops nothing. The same Multi-Paxos log goes on,
and only the learners change.

## 4. The design

### 4.1 Rules

Each rule names its source, or says that it is a departure.

**R1. A segment is a range of slots and its replica set.** The replica set is the nodes that
keep the range after the acceptors drop it: replicas in the sense of Compartmentalized Paxos,
a `ReplicaNode` each in the core. A node of the pool also runs the `Replica` role as a learner;
it is in no replica set unless a `Roll` names it. A segment has an id, a start, an end (when it is sealed),
its replicas and a state: `Open`, `Sealed`, `Held`, `Offloaded`, `Trimmed`. (`Offloaded` is
prepared and not built, as in the journal v1: R12.) The grid is not part of a segment.
Source: Compartmentalized Paxos (replicas are a role), Matchmaker Paxos §5.

**R2. The cut is a command.** `Control::Roll { next: SegmentId, replicas: Vec<NodeId> }` is a
control command of the core, like `Noop`. Only the apply path reads it. It is valid when `next`
is the tail segment plus one. A valid `Roll` chosen at slot `s` ends the tail segment at
`s + 1` (the `Roll` is its last slot) and opens `next` at `s + 1`. A `Roll` that is not valid
changes nothing.
**Departure 1**: the command is new. Its nearest source is Paxos Made Simple §3 (a
reconfiguration is a command of the state machine).

**R3. A node acts on the chosen prefix only.** A replica applies only the contiguous chosen
prefix of its own segment. A node publishes the end of a segment (R5) only after its own walk
applied the `Roll`. No step acts on a vote or on a proposal. This is the one safety rule of the
design: Paxos gives one value for each slot, so the first valid `Roll` after the start of a
segment is the same slot for every node.
Source: the replica rule of the core (`replica.rs`: the fold is contiguous or it stops).

**R4. Routing rules.** They are not safety rules: without them a `Commit` can go to the wrong
replicas, and two ends still cannot exist.

- **R4a. The pause.** A leader proposes nothing above a `Roll` of its own ballot until that
  `Roll` is chosen at its ballot. This includes a `Roll` that its Phase 1 recovered and that it
  proposes again.
- **R4b. A hole takes a `Noop`.** The election gap fill never writes a `Roll` (or any other
  command) into a hole. This is the rule of the core today.
- **R4c. A new leader takes its tail segment from its own chosen prefix** and from the values
  that its Phase 1 recovered, in slot order. It does not take it from the chain alone: the
  chain is behind the log by up to one roll.

**R5. The chain is an add-only map at the matchmakers.** `JournalRegistry` gets
`segments: BTreeMap<SegmentId, SegmentRecord>` beside `history`. The fields of a record that
come from the log (the start, the end, the replicas that the `Roll` named, the boundary state)
are written once, and every writer writes the same value, because each is a function of the
chosen log. The state only grows (`Open` < `Sealed` < `Held` < `Offloaded` < `Trimmed`). So a read is a merge
over a matchmaker quorum, and a handover carries the map by the same merge, as it carries
`history` (a union) and `retired` (`Retirement::merge_all`). There is no compare-and-set.
Source: the registry of Matchmaker Paxos §3.2, where two matchmakers cannot disagree on the
configuration of a round. Here two matchmakers cannot disagree on the end of a segment.

Who writes what:

| Write | Writer | Guard |
|---|---|---|
| Seal of `k` at `s + 1`, with the boundary state, and `k + 1` open at `s + 1` | any node whose walk applied the `Roll` | R3 |
| `Held` | the leader, from its tally of replica reports | `F + 1` replicas hold the whole segment, and the seal is in the chain |
| `Trimmed` | a node whose fold has `first_seq` at or above the last `seq` of the segment | the segment is sealed |
| A new replica set (a heal) | one `Decree` for each change, numbered | see R8 |

The **boundary state** is the `JournalState` of the core at the cut. It is the value that
`Message::TrimmedTo` carries today.

**R6. The replicas of a segment.** A `Commit` goes to the learners as today, and also to the replicas of the
segment of its slot. A replica starts at the start of its segment from the boundary state, with
the existing trim-point jump (`Replica::trim_to`, `WriteOp::TrimmedTo`). It stops at the end.
It catches up from the acceptors while they hold the slots, and from a peer replica after. A
read that reaches a sealed replica at or above its end answers "sealed, the next segment is
`k + 1`", and the reader fetches the chain again.

**R7. Held, and the floor.** A segment is `Held` when `F + 1` of its replicas hold every slot of
it and its seal is in the chain. The compaction floor of an acceptor may then rise to the end
of that segment. To an acceptor, a held segment is a `Truncate` whose records are kept
elsewhere: the floor stays inside the chosen prefix, a `Prepare` below the floor is refused,
and a laggard below the floor jumps with `TrimmedTo`. The floor follows the chain, so it never
passes an end that is not published.
**Departure 2**: this is Scenario 3 of Matchmaker Paxos §3.5 and §4.5 (the chosen prefix on
`f + 1` replicas), used for the compaction floor. `node/gc.rs` declines Scenario 3 today. The
GC watermark rule does not change in this plan: a configuration is forgotten on a Phase-2
quorum of `C_b` whose chosen index covers the fence.

**R8. After the seal.** A heal replaces one replica of a sealed segment. The set can return to
an earlier value, so it is not add-only: each change is one single-decree Paxos (`Decree`) over
the matchmaker set, numbered by the count of heals of the record. A trim marks a sealed segment
whose records are all below the trim floor.

**R9. The grid does not change here.** A grid change is `ColocatedNode::reconfigure`, a round
change, as today. It cuts no segment. What the removed journal called a reshape becomes two calls:
`reconfigure(config)` and `roll(replicas)`.

**R10. Opt-in.** Segments are the `Some` arm of the same state machine. With no matchmakers, a
`roll` is refused, no `Roll` is ever chosen, and no new message, `HardState` field or round trip
enters the plain path. The bootstrap `Config` names the replicas of the first segment; with none,
the deployment has no segments.

**R11. One segment is a valid deployment.** A deployment that never calls `roll` stays at one
open segment from slot 0, for ever. No `Roll` is chosen, nothing is sealed, nothing is `Held`,
and the acceptors keep the log until a user `Truncate`, as the core does today. A grid change
(`reconfigure`) still works and cuts nothing, so the deployment stays at one segment across it.
(Design A cannot: there each grid change opens a segment.) What one segment cannot do: move
the log off the acceptors, which needs a sealed segment, and so a roll.

**R12. Offload to an object store is prepared.** A sealed segment has a fixed range, and its
content never changes. So one object holds it, at the key (journal, segment id), with
deterministic bytes. Its record stays in the chain with the state `Offloaded`, so a range whose
only copy is in the object store is still declared (check 6 of the experiment of #1213). Offload
changes a sealed segment only, and it never stops the tail. The order: the segment is `Held`;
the offloader reads it from a replica that holds all of it and puts the object with a
conditional put; the mark `Offloaded` is written; a replica drops its copy only after a write
of the mark that it saw done at a matchmaker majority. The mark is a state that only grows, so
the add-only map takes it: a late mark after a trim merges to `Trimmed`, the offloader sees
`Trimmed` in the answer and deletes the key. The rules of #1256 carry over; they are in
`specs/tla/offload/` at the tag of step 0. That model has a compare-and-set mark, so offload
gets a new model on the add-only map on the day it is built.
The object store is I/O: the offloader and the `ObjectStore` trait stay out of
`paros-consensus`. A deployment with one segment cannot offload (R11). A roll on size or on
age is what makes offload useful, and with the `Roll` it costs one log entry.
Source: Delos (`reconfigModify`, the BackupLoglet).

### 4.2 The life of a roll

1. The driver calls `roll(replicas)` on the leader.
2. The leader proposes `Roll { next: k + 1, replicas }` at its next slot `s`, and pauses above
   `s` (R4a).
3. The `Roll` is chosen. Segment `k` is sealed at this moment; no matchmaker write was needed.
   The leader goes on at `s + 1` and sends each `Commit` to the replicas of `k + 1`.
4. A node whose walk applied slot `s` publishes: the end `s + 1`, the boundary state, and
   `k + 1` open at `s + 1` (R5).
5. The replicas of `k + 1` start from that record (R6).
6. `F + 1` replicas of `k` hold it: the leader marks it `Held`, and the acceptors may drop it
   (R7).

A roll on size or on age keeps the same replicas. Steps 4 and 5 then cost no wait.

### 4.3 What goes away, against design A

| Design A (the removed journal, at the tag of step 0) | With the `Roll` |
|---|---|
| The end is one above the highest vote that the sealer's Phase 1 saw; faulty votes and dropped ranges count | The end is the slot of the chosen `Roll`, plus one |
| First leaders race to write the end; the end write must be seen done | Every writer publishes the same value; a partial write is already true |
| The sealer proposes the votes of the old grid again in its own grid | The election gap fill of the core, with no rule for segments |
| The acceptors keep a floor for each replica set, because boundary slots live in the new grid | One log, one floor |
| A ballot binds to a segment (`Change::Bind`) | A ballot binds to a grid, as in the paper |
| Only a reshape opens a segment | A leader can roll on size, on age or for new replicas |
| A grid change always cuts the log | A grid change cuts nothing |

### 4.4 Costs and limits

- A roll needs a leader and a Phase-2 quorum of the grid. A grid below quorum needs the
  Matchmaker change first.
- The chain is behind the log by up to one roll (R4c).
- A replica set that shares no member with the set before it waits for the boundary state.
- A reshape is two calls.
- A machine that votes and holds keeps two copies of a slot until its segment is `Held`.

## 5. Changes by layer

| Layer | Module | Change |
|---|---|---|
| Foundations | `journal/command.rs` | `Control::Roll`; its tag in `command_fingerprint`; it weighs nothing |
| | new `segment.rs` | `SegmentId`, `ReplicaSet`, `SegmentRecord`, `SegmentState`, `Chain` (contiguous: the start of `k + 1` is the end of `k`) |
| | `membership/` | the predicate "`F + 1` of the replicas" (every quorum question crosses `membership/`) |
| | `contract/message.rs` | `Audience::ReplicasOf { segment, replicas }` (as `AcceptorsOf` carries its configuration); the held floor in `Heartbeat`; a report of a replica |
| | `contract/write.rs`, `contract/config.rs` | one `WriteOp` for the segment cursor; the replicas of the first segment in `Config` (`None` is the plain arm) |
| Roles | `replica.rs` | a segment cursor beside the fold; the judgement of a `Roll` at apply; a replica stops at a `Roll` that ends its segment |
| | `collector.rs` | the tally of replica reports: `Held` at `F + 1` |
| | `acceptor/retention.rs` | a second reason for the floor to move: the end of a held segment |
| | `matchmaker/` (`state`, `message`, `storage`, `generation`, `reconfigurer`) | the `segments` map; a publish request and its ack (as `GcRequest` / `GcAck`); the chain in the matchmaking and probe replies, paged; the merge at a handover |
| | `quorum_read.rs`, `belief.rs` | the tail segment in the belief; the "sealed" answer |
| Deployments | `node/` | `roll(replicas)` and its refusals; the pause; routing by segment; the publish after the walk; the floor at `Held` |
| | `proxy_leader.rs` | a delegation names the replicas for its `Commit` |
| | `replica_node.rs` | a replica of one segment: a bounded range, catch-up from peer replicas, the "sealed" answer |

## 6. Order of work

Step 0 is a removal. From step 1 on, each step is: the model, then the code, then the judge (a
`consensus-sim` walk, an oracle that shares no code with the core, a planted mutant, the trace
check).

### Step 0. Remove the journal work

Pierre, 2026-11-24: the journal and the work around it go, and the journal is built again from
scratch after the core has segments. Port nothing, and keep no compatibility layer. One pull
request; its description is the list of what it removes.

**Before.** The commit before the removal gets a git tag, as `v0.2.0-alpha.1` was on
2026-11-19. Propose a name in the pull request; Pierre makes the tag, and the removal merges
after it. Add one page `docs/archive/<tag>/README.md` that lists what
was removed and points to this prompt. The tag is the record: design A, its rules and its models
stay readable there.

**Remove: crates.**

- `crates/lib/paros-journal`
- `crates/lib/paros-journal-io`
- `crates/lib/paros-journal-control`
- `crates/lib/paros-election` (it is built on `paros-journal`)
- `crates/sim/journal-sim`

**Remove: inside crates that stay.**

- The facade `crates/lib/paros`: the re-exports `journal` and `journal_io`, and the examples
  `journal.rs`, `tokio_tasks.rs` and `full_journal.rs`.
- `crates/support/paros-wasm`: the feature `journal`, its sources (`src/journal.rs`,
  `src/journal/`) and its test `tests/journal_traces.rs`. The default features become `paxos`.

**Remove: TLA+ models** (`specs/tla/`), with their sections of `specs/tla/README.md` and their
lines in `scripts/tla-minutes.txt` and `scripts/mutate-tla-shards.txt`:

`ctrl-grid`, `ctrl-segments`, `coord`, `degraded-reshape`, `election`, `grid-controller`,
`heal-reuse`, `health`, `journal-compact`, `journal-ctrl`, `journal-ctrl-re`,
`journal-ctrl-trunc`, `journal-read`, `journal-repromise`, `journal-state`, `multi-writer`,
`offload`, `placement-port`, `quorum-everywhere`, `re-replication`, `seal-chain`, `seal-ctrl`,
`segment-records`, `segment-replicas`, `segment-truncate`, and `experiments/segments/` (`a`,
`b`, `reads` and its README).

**Keep: the models of the core.** `abandoned-ballot`, `batcher`, `campaign-discovery`,
`cas-register`, `compartments`, `config-register`, `decree`, `generations`, `grid`, `handover`,
`handover-chain`, `handover-gc`, `handover-scalars`, `leader-handoff`, `matchmaker`,
`multi-paxos`, `node`, `proxy-leader`, `read-replica`, `reads`, `reads-rows4`, `retire-fence`,
`truncation`, `truncation-reconfig`, `zero-stall`, `experiments/stale-stopped`. Keep also
`flight-rule` and `recorded-inputs`: they are the models of `paros-host`.

**Pierre decides** (default: keep). Three models are about the service of
`docs/architecture/`, not about a journal crate: `cell-control`, `journal-ids`, `takeover`.

This sort comes from the title of each model in `specs/tla/README.md`. Check each folder
against its `checks.txt` and the code that names it before the removal, and report a folder
that this list put on the wrong side.

**Remove: docs and site.**

- `docs/journal/` (the journal v1 design).
- The knowledge card `docs/knowledge/concepts/segments-and-seal.md` and its line in
  `docs/knowledge/concepts.md`. Step 2 writes the new card.
- The site pages `website/site/src/pages/journal/` and the trace
  `website/site/src/traces/journal-seal.json`, with the links to them.

**Update: docs that stay.** `AGENTS.md` (the layout, the journal's contract, the journal
crates), the root `README.md`, `docs/architecture/` (a section that links to `docs/journal/`
says "to be designed again" and keeps no dead link), `docs/references/CLAUDE.md`, the
`AGENTS.md` of the facade and of `paros-wasm`.

**Remove or update: CI and scripts.**

- Workflows: remove `journal-live.yml` and `journal-nightly.yml`; remove the journal jobs of
  `rust.yml`, `tla.yml`, `mutants.yml`, `mutants-nightly.yml` and `pages.yml`.
- `.cargo/`: remove `mutants-journal.toml`, `mutants-journal-io.toml`,
  `mutants-journal-tlc.toml`, `mutants-control.toml` and `mutants-election.toml`.
- `scripts/layers.sh` (the rows of the removed crates), `scripts/ci-plan.py`,
  `scripts/check-examples.sh`, `scripts/build-wasm.sh`, `scripts/mutants.sh`,
  `scripts/mutants-plan.sh`, `scripts/check-core-deps.sh`.
- `release-plz.toml` (the packages `paros-journal`, `paros-journal-io`, `journal-sim`) and the
  workspace dependencies of the root `Cargo.toml`.

**Do not remove.**

- `crates/lib/paros-consensus/src/journal/`. It is not the journal crate: it is what the log of
  the core carries (`Command`, `Entry`, `JournalState`). The example `journal_calls.rs` and
  `JournalIdentifier` stay too.
- `crates/lib/paros-host`. It knows no journal (layer 0), and later the roles of the core run
  on it (section 7). Its mutants file and its two models stay with it.
- `consensus-sim`, `paros-authz-biscuit`, `docs/references/`, `docs/archive/`, `docs/analysis/`.

**Done when** every gate of `AGENTS.md` is green on what is left: the build, the tests, clippy,
`scripts/check-layers.sh`, `scripts/check-core-deps.sh`, `scripts/check-examples.sh`,
`scripts/check-tla.sh`, and the site build.

**Note.** `paros-journal` and `paros-journal-io` are release-plz packages. A version that is
already on crates.io stays there; a yank is Pierre's call.

### Step 1. The model of the cut: `specs/tla/segment-roll/`

A model of the plan, in the form of the other folders (`MC.tla`, one `.cfg` for each check,
`checks.txt`, its section in `specs/tla/README.md`, the guard mutation of
`scripts/mutate-tla.py`).

**The model.** One Multi-Paxos log on one fixed grid (a grid change is orthogonal; say so in
the limits). Slots `1..NS`, ballots `1..NB`, three acceptors with majorities. Values:
`0` is a `Noop`, `1` is a client command, `k >= 2` is a `Roll` to segment `k`. Replica sets: a
disjoint set for each segment, two replicas in segment 1.

State: the promises, the set of every vote ever cast, and for each ballot: the first slot it
recovers (`lo`), the values that Phase 1 forces, the next slot, the tail segment it believes,
its pending `Roll`. The proposals carry the segment that the leader routes to. For each replica:
its start, its next slot, its end. The chain: a start and an end for each segment.

Switches: `Barrier` (R4a), `HoleFill` (`"noop"` or `"any"`, R4b), `ActOn` (`"chosen"` or
`"vote"`, R3), `Base` (`"prefix"` or `"chain"`, R4c).

Actions: `Phase1(b, Q, lo)` (a quorum promises, `lo` is any slot whose prefix is chosen, the
forced value of a slot is the vote of the highest ballot in `Q`); `Propose(b)` at the next slot
(the forced value, or a `Noop` in a hole, or a command, or a `Roll` to the believed tail plus
one; under `Barrier`, only when the pending `Roll` is chosen at `b`); `Accept(a, b, s)`;
`Apply(r)` (a replica applies the value at its next slot, and a `Roll` to its segment plus one
sets its end); `Publish(r)` (a replica with an end writes the end of its segment and the start
of the next one, when the chain has none); `Start(r)` (a replica takes its start from the
chain).

Draft definitions (not parsed):

```tla
ChosenAt(s, b, v) == \E Q \in Quorum : \A a \in Q :
                        [a |-> a, b |-> b, s |-> s, v |-> v] \in voted
Chosen(s, v)      == \E b \in Ballot : ChosenAt(s, b, v)
IsChosen(s)       == \E v \in Val : Chosen(s, v)
ChosenVal(s)      == CHOOSE v \in Val : Chosen(s, v)
PrefixChosen(s)   == \A t \in Slot : t < s => IsChosen(t)

\* The segment of slot s: the fold of the valid Rolls below it.
\* Evaluate it only under PrefixChosen(s).
RECURSIVE SegAt(_)
SegAt(s) == IF s = 1 THEN 1
            ELSE LET k == SegAt(s - 1)
                 IN  IF ChosenVal(s - 1) = k + 1 THEN k + 1 ELSE k

Agreement == \A s \in Slot : \A v, w \in Val :
                Chosen(s, v) /\ Chosen(s, w) => v = w

OneEnd == \A r1, r2 \in SegReplica :
             SegOf(r1) = SegOf(r2) /\ rend[r1] # 0 /\ rend[r2] # 0
                => rend[r1] = rend[r2]

\* "Only the tail takes appends": a replica applies only slots of its segment.
ReplicaTrue == \A r \in SegReplica :
   /\ rstart[r] # 0 =>
        /\ PrefixChosen(rnext[r])
        /\ SegAt(rstart[r]) = SegOf(r)
        /\ (rstart[r] > 1 => SegAt(rstart[r] - 1) = SegOf(r) - 1)
        /\ (rnext[r] > rstart[r] => SegAt(rnext[r] - 1) = SegOf(r))
   /\ rend[r] # 0 => SegAt(rend[r]) = SegOf(r) + 1

ChainTrue == \A k \in Seg :
   /\ chain[k].start # 0 =>
        /\ PrefixChosen(chain[k].start)
        /\ SegAt(chain[k].start) = k
        /\ (k > 1 => SegAt(chain[k].start - 1) = k - 1)
   /\ chain[k].end # 0 =>
        /\ PrefixChosen(chain[k].end)
        /\ SegAt(chain[k].end) = k + 1
        /\ SegAt(chain[k].end - 1) = k

\* Routing: a slot chosen at ballot b went to the replicas of its true segment.
NoStray == \A p \in prop :
              ChosenAt(p.s, p.b, p.v) /\ PrefixChosen(p.s) => p.k = SegAt(p.s)
```

**The checks, and what the design session predicts** (not run):

| Check | Switches | Prediction |
|---|---|---|
| `safety` | every rule on | `Agreement`, `OneEnd`, `ReplicaTrue`, `ChainTrue`, `NoStray` hold |
| `safety_bare` | `Barrier` off, `HoleFill = "any"`, `Base = "chain"` | `Agreement`, `OneEnd`, `ReplicaTrue`, `ChainTrue` still hold: R3 alone carries safety |
| `act_vote` (control) | `ActOn = "vote"` | `OneEnd` fails: two replicas see two votes |
| `barrier_off` (control) | `Barrier` off | `NoStray` fails. A ballot proposes a `Roll` at 1 and a command at 2; the command is chosen and the `Roll` is not; the next ballot writes a `Noop` at 1 |
| `hole_any` (control) | `HoleFill = "any"` | `NoStray` fails. A later ballot writes a `Roll` into a hole below a chosen slot |
| `base_chain` (control) | `Base = "chain"` | `NoStray` fails. A `Roll` is chosen and not published; a new leader that starts above it routes to the old replicas |
| `reach_*` | | two rolls in the chain; a `Roll` chosen again by a later ballot; a `Roll` lost to a `Noop`; a replica of segment 2 applies a slot |

If `safety_bare` fails, R3 is not enough and the design changes: tell Pierre before step 2.

**The report** goes to Pierre: each check with its result against the prediction, and each
guard that the mutation needed.

### Step 2. The docs PR: the desired state

Write the rules of section 4.1 into the docs before any code: where the segments of the core
live in `docs/architecture/` (§4), a knowledge card for segments in the core, and the list of
departures (two: R2, R7). Confirm D4 and D5. Record D2 (acceptors stay learners; pure voters
later) and D6.

### Step 3. The cut

- Model: put the model of step 1 on `multi-paxos/`: the `Roll`, the pause, and a recovery
  through a `Roll`. The `Roll` is a value, so the inductive invariant of `MultiPaxosProof.tla`
  must not change; check that it does not.
- Code: the foundations of section 5, `roll()`, the segment cursor of the replica, the pause.
- A walk that never rolls is a check of its own (R11): a matchmaker deployment with one open
  segment behaves as the matchmaker deployment of today.
- Judge: `consensus-sim`, a new flavor with its walk; oracles "one end for each segment" and
  "a replica applies only its segment"; a mutant that applies a voted `Roll`. The traces of the
  plain walks do not change.

### Step 4. The chain at the matchmakers

- Model: the add-only map read by a quorum; a publish that only a minority took is already
  true; the map across a handover (extend `handover-scalars/`).
- Code: `matchmaker/`, the publish from the node, the chain in the replies.
- Judge: the handover model checker (`models/handover.rs`) and a walk; a mutant that publishes
  on a proposal.

### Step 5. The replicas of a segment

- Model: extend `compartments/` and `reads/`: a replica that starts from the boundary state, a
  read across a roll with an old chain.
- Code: `replica_node.rs` as a replica of one segment, `Audience::ReplicasOf`, the proxy's
  `Commit`, catch-up between replicas, the "sealed" answer.

### Step 6. Held, and the floor

- Model: extend `truncation/` and `truncation-reconfig/`: the floor at the end of a held
  segment, the laggard jump, a reconfiguration onto empty stores after a segment was dropped.
  Controls: `Held` at `F` replicas only (a slot is lost); a floor past an end that is not
  published.
- Code: the tally in `collector.rs`, the held floor in the beat, the drop in
  `acceptor/retention.rs`.

### Step 7. After the seal

The heal of a replica set by a `Decree`, and the trim. The removed journal found the rules of
the copy (`re-replication/` and `heal-reuse/`, at the tag of step 0). Read them, then model the
heal again on the `Decree`.

### Step 8. Surface

A twelfth example `segments.rs`, its rows in the two READMEs and in `src/lib.rs`, its CI step,
the knowledge card, the site page.

### Step 9. The website

Added by Pierre on 2026-11-25 ("add a step 9 to do the website like we did yesterday"). Done
as the guided Paxos tour of 2026-11-24 (`/paxos/tour/`, the plan
`/mnt/project-files/plans/paxos-guided-tour-2026-11-24.md`): the chapter of the tour on
matchmakers and segments, which waited for segments, with its guided exercises on the wasm
engine, and the pages of the site that segments change (`update-the-site` skill).

## 7. Later, not in this work

Start none of this. It is here so that this work leaves room for it.

**The journal, from scratch.** `paros-journal` comes back as the final crate, the one a program
uses. It composes the deployments of the core and writes no consensus of its own: no second
matchmaker, no second leader, no second replica.

**The layering** (direction of the session of 2026-11-24, to confirm when that work starts):

- `paros-consensus` stays sans-IO: it says what to persist and what to send.
- `paros-host` holds the contract of a role, the generic driver, and the IO traits (`Storage`,
  `Transport`, `Clock`, `Entropy`). Storage and network hook in there as traits.
- A new crate runs the roles of the core on the host: addresses, the message envelope, the disk
  layout, recovery, timers, backends in memory. It is the successor of what `paros-journal-io`
  did for the removed roles.
- The real disk and the real network are an adapter crate at the edge (for example on
  moonpool), which the program plugs in. No pure crate does I/O.
- The core speaks `Ready` and `advance`; the host speaks the flight rule. The first bridge wraps
  the core, with one batch in flight for each role. The move of the core to the flight rule
  (the proposal of §J4.2 of the removed docs) can come after.
- One fold, in the core (`journal/state.rs`). The removed journal had a second one (a
  multi-writer mode with its tag, the answer `Below`, record headers, `Resolve`); what the new
  journal needs of it goes into the fold of the core. This reverses the decision of 2026-11-20.

**Acceptors that only vote** (#1271). See section 8.

What the removed journal had that is not consensus, as a reference at the tag of step 0: the
data-plane rules (`docs/journal/02-data-plane.md`), the client, the codec and the on-disk
layout, the controller's policy (placement, health, the zone rule), and the coordination store
(`paros-election`).

## 8. Open

- **Pure voters, later** (D2 is decided: acceptors stay learners in this plan). Today a `Commit`
  goes to `Audience::Learners`, the whole pool, with the command, so each acceptor receives and
  folds each write. Seven couplings in the module doc of `replica_node.rs` rest on this, and so
  does the GC rule of `node/gc.rs`. With pure voters, a write reaches only the replica set of
  its segment (#1271). Replica sets, `Held` and the floor are what pure voters need, so this
  plan prepares that work and does not do it. When it comes, each of the seven couplings is
  derived again with its model, and the GC rule must count the replicas.
- **A new replica for the open segment.** In this plan a change of the replica set of the open
  segment is a roll. With acceptors as learners (D2), a replica of the open segment could also
  join with no cut and catch up from the acceptors, as a `ReplicaNode` does today. Not chosen.
- **Who asks for a roll, and its fence.** The removed journal had a grid controller with a term
  in its register. An add-only map cannot hold a term. `Roll { next }` already gives the guard
  "at most one segment opens from one tail" at apply. Where a controller and its term live is a
  question for the new journal.
- **Announce first.** A variant: a controller writes the next segment and its replicas at the
  matchmakers before the `Roll`, and the `Roll` names it. The matchmakers then know each
  segment before the log cuts, and only the end slot comes after. Not chosen.
- **Where the segment id lives**: in `JournalState`, or in a cursor beside it. The plain arm
  must persist and send what it does today, which points to the cursor.
- **The reply owner.** Today `slot % replica_count` of the ballot's configuration
  (`AcceptorConfig::reply_owner`). With segments it is a function of the replicas of the slot's
  segment.
- **The handoff.** `relinquish_to` hands a ballot on. Check the pause across a handoff
  (`leader-handoff/`).
- **The chain's own garbage.** When a trimmed record leaves the map.
- **Offload** (R12). Not built in this plan.

## 9. Read first

- `AGENTS.md`, and `crates/lib/paros-consensus/AGENTS.md` (the layers and the map).
- `crates/lib/paros-consensus/src/journal/command.rs` (`Control`), `src/replica.rs`,
  `src/replica_node.rs` (the seven couplings), `src/node/gc.rs` (the GC rule and Scenario 3),
  `src/node/reconfigure.rs`, `src/matchmaker/state.rs` (`JournalRegistry`),
  `src/contract/message.rs`, `src/decree.rs`.
- Before step 0 removes them, or at its tag after: `docs/journal/03-segments-and-records.md`
  (design A, every rule and every pitfall that its models found) and
  `specs/tla/experiments/segments/README.md` (how A and B were compared).
- `docs/references/papers/`: `matchmaker-paxos`, `scaling-rsm-compartmentalization`,
  `delos-virtual-consensus`, `stoppable-paxos`.
