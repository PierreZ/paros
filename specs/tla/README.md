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

## Layout

Each folder is one model:

- `<Name>.tla`: the spec.
- `MC.tla`: the constants that a TLC configuration file cannot write (tuples, sets of sets).
- `<check>.cfg`: one TLC configuration.
- `checks.txt`: one line for each configuration, with the result that it must give (`pass` or
  `violation`) and where it runs (`ci` or `long`).

A check that must give `violation` is a control. It shows that the model can find the bug that
the rule prevents. A model with no control proves little.

A module that several models share lives in its own folder and is linked into each folder that
uses it by a symbolic link: `grid/Grid.tla`, the quorums of a grid, is linked into
`config-register/`, `reads/`, `multi-paxos/`, `leader-handoff/`, `compartments/` and
`proxy-leader/`.

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
| The matchmaker set handover | `matchmaker/reconfigurer.rs` | `handover/`, which extends `handover-gc/` | #1097 (rework 1.8), done |
| Grid quorums | `membership/quorum_system.rs` | `grid/`, used by `config-register/`, `reads/`, `multi-paxos/`, `leader-handoff/`, `compartments/` and `proxy-leader/` | #1102 (rework 1.13), done |
| The proxy leader | `proxy_leader.rs` | `proxy-leader/` | #1188 (rework 2.9), done |
| The batcher, the replica and the read replica | `batcher.rs`, `replica.rs`, `replica_node.rs` | `compartments/` | #1190 (rework 6.10), done |
| Leaderless reads | `quorum_read.rs`, `node/quorum_reads.rs` | `reads/` | #1016 (model of leaderless reads), #1099 (rework 1.10), done |
| Truncation: the compaction floor as a fold of the journal, the trim-point jump, the retry of a laggard, the gap fill, and the faulty entries of CTRL with the repair probe, on plain Multi-Paxos | `acceptor/retention.rs`, `node/catch_up.rs`, `node/acceptor.rs`, `node.rs` (`compact`), `replica.rs` (`compaction_target`), `proposer.rs` (`slot_decidable`), `proposer/probe.rs` | `truncation/` | #1017 (truncation model), #1216 (truncation model, part 2), done |
| Truncation across a reconfiguration onto empty stores: the GC of the old configuration only once a Phase-2 quorum of the new one holds the prefix up to the election fence, a new member that gets its floor from `TrimmedTo` or catches up | `node/gc.rs`, `collector.rs` (`covered`), `node/learn.rs` (`mark_chosen`), `node/reconfigure.rs` | `truncation-reconfig/` | #1218 (truncation model, part 3), done |
| The journal state and its `SetLeader` fence | `journal/state.rs` | `journal-state/` (to write) | #1189 (rework 3.7) |
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
| `Gc` | `off`: no garbage collection. `chosen`: the leader of `b` asks the matchmakers to forget the ballots below `b` when each slot that it recovered or carried is chosen at `b` (the paper, §3.5) and, with `Hold`, when its Phase 1 is covered. `early`: a control, the leader asks with no condition. |
| `CheckWm` | `TRUE`: bypass only if the highest watermark in the matchmaker answers is at most `b`. |
| `Reads` | `TRUE`: the leader acks the chosen slots, and the model has the leaderless reads of Compartmentalized Paxos (§3.4) with the rule of `quorum_read.rs`: a reader asks a quorum of the configuration of a leadership that won, abandons the read if an answer shows a higher ballot, and reads at the highest slot voted or at the fence. |
| `Hold` | `TRUE`: the decision hold. A leader acks no slot before a quorum of each prior configuration promised its ballot. |
| `CfgChoices` | The configurations that the ballots can have: any (`CfgAny`), or one fixed journal move (`CfgMovePaper3`), which keeps the GC checks small. |

The invariants are `Agreement` (one value is chosen for each slot) and `ReadSafe` (no leaderless
read that starts now can miss a slot that is acked).

The model has three ballots, three matchmakers, two values, one or two slots and two
configurations of two acceptors.

Not in the model: matchmaker generations, lost durable state and the cooperative handoff. A
reader is abstract (a read can use the basis of any leadership that won); `reads/` has an
explicit reader. A GC check with four ballots on paros's ballots is too large for a complete
search so far.

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

Not in the model: the decree (`decree/`), a client step that reaches a minority
(`config-register/`), the clients of generation 1, a second handover, lost matchmaker state,
more than one journal (a handover is paged, #439) and the registrations, which leave with
rule 1.

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
| `Learner` | The options of #890 (learner above the leader). A reader campaigns with a stale belief, promises its own ballot and sends no `Prepare`. `strict`: the node never takes a leadership below its promise as its read basis (`main` today). `own`: option (b), the node takes the basis when the only promise above it is its own abandoned campaign. `any`: the node ignores its promise. `off`: no such campaign. |
| `Q1s`, `Q2s`, `Targets` | The quorum system, for each configuration (#1102 (rework 1.13)): the Phase-1 quorums, the Phase-2 quorums, and the sets of acceptors that a reader asks. A majority asks the whole membership. A grid takes them from `grid/Grid.tla`: the partitions ask the whole membership, the full grid asks one row (`row_of`). |
| `Seal` | `quorum`: Phase 1 takes a Phase-1 quorum of each prior configuration (the code). `reads`: a candidate rule, not in the code: Phase 1 also takes one promise in each read target of each prior configuration, so each row of an old full grid hears of the campaign. |

The majority checks have four acceptors, the configurations `{a1, a2, a3}` and `{a3, a4}`, the
readers `a1` and `a3`, one matchmaker, one slot and two ballots. The learner checks have a third
ballot that is only the abandoned campaign of a reader.

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
| The bypass of #579 with the decision hold | pass |
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
higher of the two), the registration of the abandoned campaign, more than one slot (so a slot's column is any
column), and a move between a grid and a majority.

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
generation that fences a handover, the stop quorum that opens the handover's decree, the
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
  that learned it (`node/catch_up.rs`).
- **A restart that keeps only durable state**: the promise and the votes on disk, and what the
  node learned. Every ballot of the node dies with it.
- **The quorum system as a constant** (`membership/quorum_system.rs`): `Q1`, and `Q2` for each
  slot. `MC.tla` builds the majority and takes the grid from `grid/Grid.tla` (linked): a slot's
  Phase 2 goes to column `slot % cols` (`GridQ2At`), and `GridQ1` elects.

| Constant | Values |
|---|---|
| `Alloc` | `"frontier"`: fresh commands start at the frontier. `"prefix"`: a control, fresh commands start at `from`. |
| `Gap` | `"noop"`: an unreported slot below the frontier gets a `Noop`. `"skip"`: a control, it gets nothing. |
| `Durable` | `"before"`: an acceptor answers only when its promise or vote is on disk (persist before send, `Ready`). `"after"`: a control, it answers first and writes later. |
| `Restarts` | How many restarts can happen in the run. |
| `Wipes`, `Identity` | How many acceptors can lose their disk. With `Identity = TRUE`, a wiped acceptor never takes a step again: it comes back with a new node id, outside the static configuration. With `FALSE`, a control, it answers again as the old acceptor. |
| `Learners` | The nodes that catch up. The ballot owners always learn what they decide. A node that owns no ballot only adds states when it catches up, so most checks leave it out. |
| `Down` | The acceptors that never take a step (the liveness checks). |

The safety invariants are `Agreement` (one command for each slot), `LearnedChosen` (what a node
learned was chosen) and `OneProposal` (one proposal for each slot and ballot: a new leader
allocates above each slot that its Phase 1 reported, so a fresh command never lands on a slot
that its recovery proposes). A value is chosen when a Phase-2 quorum of the slot sent its `2b`
at one ballot, so a restart cannot unchoose it. The liveness property is `Ends` under
`LiveSpec`: the last ballot is the leader that no higher ballot preempts, and it learns every
slot below its frontier and every slot that it proposed. `Spreads` adds that each live node
learns what that leader learned.

Results (`multi-paxos/checks.txt`, three acceptors unless the line says otherwise, two ballots,
two slots, one command and `Noop`; the CI set runs in about three minutes):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach` | ci | `NoChoice`, a reachability control | violation |
| `reach_gap` | ci | `NoNoopChosen`, a reachability control: a gap fill is chosen below a chosen command | violation |
| `majority` | ci | the rules of the code | pass (363 thousand states) |
| `alloc_prefix` | ci | fresh commands start at `from`, two commands | `Agreement` violation |
| `durable_after` | ci | an acceptor answers before its write is on disk, one restart | `Agreement` violation |
| `wipe_revote` | ci | a wiped acceptor votes again as the old one, one wipe | `Agreement` violation |
| `live` | ci | the rules of the code | `Ends` pass |
| `live_down` | ci | one of three acceptors down | `Ends` pass |
| `lost` | ci | two of three acceptors down | `Ends` violation |
| `gap_skip` | ci | no `Noop` for a gap | `Ends` violation |
| `full_grid_one_column` | ci | 2 x 2 full grid, a Phase-1 quorum in one column | `Agreement` violation |
| `full_grid_down` | ci | 2 x 2 full grid, one acceptor down: its column cannot decide | `Ends` violation |
| `partitions_down` | ci | 3 x 2 partitioned grid (F = 1), one acceptor down in each partition | `Ends` pass |
| `restart` | long | one restart | pass (1.4 million states, 2 min) |
| `wipe` | long | one wipe, the identity rule | pass (1.0 million states, 1 min) |
| `spreads` | long | every node catches up | `Ends` and `Spreads` pass (0.9 million states, 15 min) |
| `full_grid` | long | 2 x 2 full grid | pass (11.4 million states, 13 min) |
| `partitions` | long | 3 x 2 partitioned grid | no violation in 33 million distinct states (bounded: stopped after 50 min, 21 million states left on the queue) |

The long checks ran once on 2026-11-19 (#1216 (truncation model, part 2)) with 4 workers.

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

Not in the model: matchmakers and reconfiguration (`zero-stall/`, `config-register/`), the
cooperative handoff (`RecoveryPolicy::Inherited`), the CTRL tri-state of a rotted record and the
repair probe, truncation and the trim-point jump (all three in `truncation/`), the paging of a promise, the proxy leader
(`proxy-leader/`), a leader that a restart keeps, and more than two ballots (a check with three ballots does not finish in a CI job).

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
- **The delegation** (`Message::Accept` with `reply_to = Party::Proxy`): first to `ProxyOf[s]`
  (`ProxyId::of`), then again to any proxy while no `Commit` of the slot is out (`move_round`).
- **The proxy of the code**: it works for the highest ballot it was handed; a higher one closes
  every lower round (`close_below`), a lower one is ignored. It learns the configuration only
  from a delegation at a higher ballot (`learn_config`). It ignores the re-delegation of a round
  it decided (`done`). A crash loses every round: the proxy boots empty with the bootstrap
  configuration.
- **The quorums as constants** for each configuration: `Members`, `Q1`, and `Q2` for each slot.
  `MC.tla` builds the majority and takes the full grid from `grid/Grid.tla` (linked): the
  `Accept` of a slot goes to its column, and the column decides.

Not in the model: the `Nack` (it only closes a round, and a round that stays open decides
nothing more), the leader's take-back (`take_back_delegated`: the leader decides the round with
its own tally, which is `multi-paxos/`), the re-send beat and `expire_stale` (with no lost
message they change nothing), and the GC of `H_b`.

| Constant | Values |
|---|---|
| `Tally` | `"round"`: the proxy decides a round over the configuration that the round's `Accept` carried. `"belief"`: it decides over the configuration that it believes now. |
| `Hear` | `"delegation"`: the proxy learns a configuration only from a delegation, which closes every lower round (the code today). `"any"`: it also learns the configuration of any ballot that started, as from the register of rule 1, and keeps its rounds open. |
| `Count` | `"quorum"`: a decision needs a Phase-2 quorum of votes. `"short"`: a control, one vote less is enough. |
| `Move` | `TRUE`: a slot can go to another proxy. `FALSE`: a control, it only goes to `ProxyOf[s]`. |
| `Crashes` | How many proxy crashes can happen in the run. |
| `Down` | The proxies that never take a step (the liveness checks). |

The safety invariants are `CommitBacked` (each `Commit` is backed by a Phase-2 quorum of its
ballot's configuration, at that ballot), `CommitAgree` (two `Commit`s of one slot carry one
value, so two learners never learn two values) and `Agreement` (one value is chosen for each
slot). The liveness property is `Ends` under `LiveSpec`: the last ballot is the leader that no
higher ballot preempts, one proxy is gone for good, and every slot gets a `Commit`.

**The code is safe, and the rule makes it safe after rule 1 too.** Today a proxy learns a
configuration only from a delegation, and that delegation closes every lower round, so the
configuration it believes is always the configuration of its open rounds (`code`, `Tally =
"belief"`, passes). Rule 1 (step 2.5) lets a node learn its configuration from the register.
If a proxy learns it there and keeps its rounds, a tally over its belief is unsafe
(`belief_any`), and a tally over the round's own configuration is safe (`safety`). This is the
change that finding 10 proposes: bind the tally of a delegated round to the configuration that
its `Accept` carried. The code needs it before a proxy learns a configuration from anything but
a delegation.

Results (`proxy-leader/checks.txt`; three acceptors, configuration 0 is `{a1, a2, a3}` and
configuration 1 is `{a1}`; ballot 1 is `l1`'s on configuration 0, ballot 2 is `l2`'s on
configuration 1, ballot 3 is `l1`'s on configuration 0; two proxies; one slot and two commands
unless the line says otherwise; the CI set runs in about 40 seconds; the long checks have no proxy
crash and ran once on 2026-11-19 with 4 workers):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach` | ci | `NoCommit`, a reachability control | violation |
| `reach_sealed` | ci | `NoSealedCommit`, a reachability control: a proxy commits a round of a ballot that a higher ballot sealed, three ballots | violation |
| `reach_moved` | ci | `NoMovedCommit`, a reachability control: a slot moves to the second proxy and it decides, one crash | violation |
| `code` | ci | the code: `Tally = "belief"`, `Hear = "delegation"`, one crash | pass (26 thousand states) |
| `safety` | ci | the rule: `Tally = "round"`, `Hear = "any"`, one crash | pass (107 thousand states) |
| `grid` | ci | the rule, configuration 1 is the 2 x 2 full grid of `a1`..`a4`, one crash | pass (147 thousand states) |
| `belief_any` | ci | `Tally = "belief"`, `Hear = "any"`: the proxy counts over a configuration it did not check | `CommitBacked` violation |
| `belief_any_agree` | ci | the same | `CommitAgree` violation |
| `short` | ci | `Count = "short"`: a `Commit` one vote short of a quorum | `CommitBacked` violation |
| `short_agree` | ci | the same | `CommitAgree` violation |
| `live` | ci | `p1` down, the slot moves | `Ends` pass |
| `live_nomove` | ci | `p1` down, `Move = FALSE` | `Ends` violation |
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
- `live_nomove`: `p1` is gone, the slot only goes to `p1`, and no `Commit` comes.

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

The invariants are `Agreement` (one value is chosen) and `Electable` (whatever matchmaker
majority a new ballot hears, each configuration of its history keeps a Phase-1 quorum that did
not retire; the same name as in `retire-fence/`).

The pool has five acceptors and two configurations, `A = {a1, a2, a3}` and `B = {a3, a4, a5}`.
A move from `A` to `B` replaces `a1` and `a2`, and a majority of the union, `{a3, a4, a5}`, holds
no Phase-1 quorum of `A`. Each ballot selects `A` or `B`. Two values. The controls use one
matchmaker where the bug does not need more, two for `histories_one` and three for
`retire_one_ack`.

Results (`matchmaker/checks.txt`):

| Check | Rule | Result |
|---|---|---|
| `reach` | `NoChoiceAboveRetired`, a reachability control: a value is chosen above a ballot whose configuration named an acceptor that retired | violation |
| `phase1_own` | control: Phase 1 in `C_b` alone, two ballots | `Agreement` violation |
| `phase1_union` | control: Phase 1 at a majority of the union of `H_b`, three ballots | `Agreement` violation |
| `match_nocheck` | control: a matchmaker registers a ballot below one that it holds, two ballots | `Agreement` violation |
| `histories_one` | control: the proposer closes on one answer, two matchmakers | `Agreement` violation |
| `edit` | control: Phase 2 goes to a configuration that the ballot did not register | `Agreement` violation |
| `gc_early` | control: GC as soon as Phase 1 is done, three ballots | `Agreement` violation (1.4 million states) |
| `retire_removed` | control: an acceptor retires when the newest configuration does not name it | `Electable` violation |
| `retire_one_ack` | control: the floor counts as effective at one ack, three matchmakers | `Electable` violation |
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
| `grid` | `CrossIntersect`: every Phase-1 quorum meets every Phase-2 quorum (`cross_intersects`). `ReadsArePhase1`: each read target holds a Phase-1 quorum. `ToleratedIsExact`: `tolerated_failures`, `min(acks - 1, cols * (rows - acks + 1) - 1)`, is the largest number of failures, wherever they are, that leaves a Phase-1 and a Phase-2 quorum | pass (69 layouts) |
| `grid_slack` | control: Phase 1 takes one acceptor less of each column | `CrossIntersect` violation |
| `grid_full_rows` | `Phase1Meets` on the full grids with two columns or more: two Phase-1 quorums meet | violation: two rows of a 2 x 2 grid do not meet |

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
  delivers a request at most one time.
- **The edge refusal**: the leader refuses a merged batch over its record limit (`TooLarge`)
  before it allocates a slot. The batcher lowers its limit (`lower_limits`) and sends each write
  of the batch again, alone (`docs/architecture/02-data-plane.md` §2.4).
- **The held answer**: the leader allocates the frontier slot, and answers once the slot and
  every slot below it are chosen, with the `seq` that its fold gives the batch. A new leader holds
  no answer of the old one.
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
| `Leaders`, `Restarts`, `Crash` | How many leader changes and replica restarts can happen, and whether the batcher can stop for good. |

The safety invariants:

- `OneRange`: each write of a batch gets its own `seq` range one time at each replica, also when a
  batch is sent again (a `TooLarge` split, an `Accept` sent again, a slot that a new leader
  recovers).
- `OneAnswer`: each write gets one answer, a range or unanswered.
- `AnswerAgrees`: the range that the unbatcher gives a write is the range that each replica
  folds for it.
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
| `walk_skip` | ci | the walk folds a slot past a gap | `AnswerAgrees` violation |
| `boot_keep` | ci | the fold state survives a restart, and the walk starts again at the chosen index on disk | `OneRange` violation |
| `read_local` | ci | the replica serves a read from its prefix | `ReadFresh` violation |
| `read_replicas` | ci | a read replica counts as a voter of the read | `ReadFresh` violation |
| `live` | ci | the rules of the code | `Ends` pass |
| `live_split` | ci | the rules of the code, the leader refuses the merged batch | `Ends` pass |
| `live_noresend` | ci | the leader does not send lost `Accept`s again | `Ends` violation |
| `reads_two` | long | two replicas, one reader | pass (4.5 million states, 2 min) |
| `safety` | long | the whole model at once: one leader change, a batcher that can stop, one restart, one reader | pass (12.7 million states, 4.6 min) |

The long checks ran once on 2026-11-19 with 4 workers.

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

What the model does not find: no rule of the code is refuted, and no design decision is in
question. The read replica's rule is the rule of `reads/`, and the walk's rule is the module doc
of `replica.rs` ("the fold is contiguous or it stops").

Not in the model: the log itself (`multi-paxos/`), the proxy leader (`proxy-leader/`), more
than one journal (a lane never mixes journals, so one lane is enough), the `NotLeader` refusal (it
takes no slot, so it is the same case as `TooLarge`; this is a reading of the code, not a check), a write
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
  its own time, so the floors of the nodes differ for some time. An acceptor holds no vote and no
  learned value below its floor.
- **The floor guard in Phase 1** (`PrepareOutcome::BelowFloor`, `node/acceptor.rs`). A campaign
  asks from its first unchosen slot. An acceptor whose floor is above that slot does not promise
  and does not move its promise, because its promise could not report the slots that it
  compacted. An `Accept` below the floor is ignored (`AcceptOutcome::BelowFloor`).
- **The retry of a laggard.** A candidate that a floor refused campaigns again from its new first
  unchosen slot once a jump or a catch-up moved it (`on_nack`, then the next campaign). The model
  keeps the ballot and sends a new `1a` from the new slot; the code opens the next round. With
  two ballots in the model, a new round would be the last ballot, and the check of liveness needs
  that ballot for the leader that no one preempts.
- **The trim-point jump** (`Message::TrimmedTo`, `node/catch_up.rs`). A node that asks a peer
  for slots below the floor of the peer jumps to that floor. The jump carries no ballot: the
  promise does not move. A node that asks at or above the floor of the peer gets what the peer
  learned.
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
| `Tally` | `"ctrl"`: the tri-state tally. `"none"`: a control, a faulty report counts as no vote. |
| `Retry` | `"jump"`: a refused candidate asks again from its new first unchosen slot. `"none"`: a control, it never asks again. |
| `Restarts` | How many restarts can happen in the run. The floor is durable. |
| `Faults`, `RotNodes` | How many faulty entries a boot can find in the run, and on which nodes. |
| `Learners` | The nodes that catch up and jump. |

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
- `Recoverable`: for each chosen slot, each Phase-1 quorum has a member that holds a vote there
  (a faulty vote blocks the tally), or a member whose floor is above the slot (and that member
  refuses a campaign from below it).
- `ValueKept`: each chosen slot is below a floor, or a node can still read its value.
- `WatermarkCovers`: the vote watermark of an acceptor is never below a slot that it voted.
- `Ends` (liveness, `LiveSpec`): the last ballot `NB` wins, closes its repair probe and holds
  every slot below its allocator. A chosen slot whose record a boot found faulty is still
  applied. The fairness covers the owner of `NB` (campaign, retry, win, probe, decide, catch-up,
  jump) and each acceptor (promise, accept).

Results (`truncation/checks.txt`, three acceptors, two ballots, two slots, the values `trunc` and
`Noop`, and `a1` and `a2` catch up, unless the line says otherwise; the CI set runs in about 4
minutes with 4 workers):

| Check | Where | Rule | Result |
|---|---|---|---|
| `reach_compact` | ci | `NoCompaction`, a reachability control: a node compacts | violation |
| `reach_split` | ci | `NoSplitFloor`, a reachability control: one node compacted, one did not, and a slot above the floor is chosen | violation |
| `reach_jump` | ci | `NoJump`, a reachability control: a node jumps over a slot that it never voted | violation |
| `floor_ignore` | ci | Phase 1 ignores the floor | `NoFillChosen` violation |
| `trim_promise` | ci | the trim jump takes the promise of the peer; one command too | `Agreement` violation (731 thousand states) |
| `compact_early` | ci | a node compacts once it learned the truncation alone | `FloorChosen` violation |
| `watermark_votes` | ci | the watermark leaves the floor out | `WatermarkCovers` violation |
| `safety` | ci | the rules of the code | pass (1.0 million states, 1.5 min) |
| `reach_faulty` | ci | `NoFaultyChosen`, a reachability control: a boot finds a chosen slot faulty; one fault | violation |
| `reach_block` | ci | `NoBlock`, a reachability control: a repair probe opens; one fault | violation |
| `ctrl_none` | ci | a faulty report counts as no vote; one fault | `NoFillChosen` violation |
| `two_rots` | ci | two faults: more than the majority survives | `ValueKept` violation |
| `ctrl_one_slot` | ci | one fault, one slot | pass (32 thousand states) |
| `ctrl_live` | ci | `Ends`, one fault on `a1` or `a3`, one slot, `a2` (the last leader) catches up | pass |
| `reach_retry` | ci | `NoRetry`, a reachability control: a refused candidate asks again; `a1` leads ballot 1 and `a3` ballot 2 | violation |
| `laggard_stuck` | ci | `Ends`, a refused candidate never asks again; one slot, `a2` and `a3` catch up | violation |
| `laggard` | ci | `Ends`, the retry of the code; one slot, `a2` and `a3` catch up | pass |
| `reach_keep` | ci | `NoKeep`, a reachability control: a floor stays below a truncation that a node applied; `c1` and `cut0` too | violation |
| `fold_slot` | ci | the floor goes one past the truncation; `c1` and `cut0` too | `FloorReleased` violation |
| `fold_one_leader` | ci | the fold, one ballot; `c1` and `cut0` too, every node catches up | pass (54 thousand states) |
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
| `three_ballots` | long | the rules of the code; one slot | THREE_BALLOTS |
| `two_slots` | long | the rules of the code; two ballots | TWO_SLOTS |

The long checks ran once on 2026-11-19 (#1218 (truncation model, part 3)) with 4 workers.

With one slot, a `Noop` never lands on a chosen slot: a candidate whose Phase 1 reports nothing
has its frontier at its first slot and fills nothing. So the two controls that must find the
`Noop` run with two slots, and the checks with one slot judge `Recoverable`, the property that
the `Noop` breaks.

The traces of the controls:

- `gc_won_fill`: ballot 1 wins on `{a1, a2}` and proposes `trunc` at slots 0 and 1. `{a1, a3}`
  choose slot 0, and `a2` votes slot 1. `a3` opens ballot 2 under `C2` and wins on `{a2, a3}`, a
  quorum of both `C1` and `C2`. It proposes `trunc` again at slots 0 and 1 and, with
  `GcRule = "won"`, raises the watermark at once, before any member of `C2` accepted or learned
  slot 0. Ballot 3 of `a2` matches with `C2` alone and wins on `{a2, a4}`. It hears slot 1 only
  (the vote of `a2`), its frontier is 2, and it fills slot 0 with a `Noop`. `a3`, the one member
  of `C2` that holds slot 0, is not in the quorum.
- `cover_joined_fill`: `{a1, a3}` choose `trunc` at slot 0 at ballot 1, and `a3` learns it by a
  catch-up from `a1`. Ballot 2 of `a3` campaigns from slot 1, so its fence covers slot 0, which
  `a3` holds. With `Cover = "joined"`, `{a3, a4}` cover the fence although `a4` holds nothing,
  and the GC forgets `C1`. Ballot 3 of `a2` wins on `{a2, a4}` and fills slot 0 with a `Noop`, as
  in `gc_won_fill`.

The answer to #1218: with the rule of the code, `Recoverable` holds over the Phase-1 quorums of
`C2` once `C1` is gone. A member of `C2` counts toward the fence only when it holds each slot
up to `F`: it learned it (and so holds a record, `mark_chosen`) or jumped above it with
`TrimmedTo` (and so refuses a campaign from below its floor). Each Phase-1 quorum of `C2` meets
that Phase-2 quorum. No decision is refuted.
