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
`config-register/` and `reads/`.

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
| `grid_long` | `grid` with three ballots | GRIDLONG |
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
configuration, the size of the successor set and more than one journal.

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
| `AckBallot` | `TRUE`: a `PreReadAck` carries the ballot of the acceptor, and an answer above the basis ends the read. |
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
| The bypass of #579 with no decision hold | violation |
| The rules of `main` | pass |
| The bypass of #579 with the decision hold | pass |
| #890, `strict` (`main` today) | pass |
| #890, option (b) | pass |
| #890, the node ignores its promise | pass |
| `grid_full`: the full grid, a change of members, the row read (the code) | violation (300 thousand states, 15 seconds) |
| `grid_full_static`: the full grid, no change of members | pass (588 thousand states, 35 seconds) |
| `grid_full_seal`: the full grid, a change of members, `Seal = "reads"` | pass (1.6 million states, 1.5 minutes) |
| `grid_partitions`: the partitions, a change of members | GRIDPART |

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
(`grid_full_seal`). Which one paros takes is Pierre's choice (#1102); no code changes before it.

Two more controls show that the model reaches the case of #890. `learner_stranded` finds a
leader and a reader outside its configuration whose own abandoned campaign is above it.
`learner_own_serves` finds a read that option (b) serves and that `strict` refuses.

Option (a) of #890, where the leader campaigns above the promise, adds no rule to the model: it
is one more ordinary campaign, and `reads_plain` covers it. Its cost is a leader change for each
stale campaign of a removed node. The row "the node ignores its promise" passes in this model
because an acceptor of the basis configuration always reports a later ballot. It is not a
recommendation: the learner checks have only majority quorums.

Not in the model: GC watermarks, matchmaker generations, the replica tier, a read that expires,
the registration of the abandoned campaign, more than one slot (so a slot's column is any
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
partitions) are `rows = 2F + 1` and `acks = F + 1`. `config-register/` and `reads/` take their
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

