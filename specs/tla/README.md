# TLA+ specs

Small TLA+ models of one protocol rule. A model is checked with TLC before the rule is written
in `paros-consensus`. A model does not replace the simulation: it searches a small abstract protocol
completely, and the simulation tests the code that ships.

## How to run

    nix develop .#tla --command scripts/check-tla.sh          # the checks that CI runs
    nix develop .#tla --command scripts/check-tla.sh --all    # the long checks too
    nix develop .#tla --command scripts/check-tla.sh reads    # one folder

`tlc` is also in the default dev shell. CI runs the first command in the `TLA` workflow, only
when a spec, the script or the flake changes.

A long check that does not finish has a time bound in minutes as a fourth word of its line in
`checks.txt` (`partitions pass long 50`). TLC stops at the bound, and the line is `ok` when no
violation was found until then. A line with no bound must finish. Every bounded line is a
bounded run, never a proof: its table line in this file says so.

A line can also end with `seed=<n>`: TLC runs with `-seed <n>`. The inductive checks of
`multi-paxos/` draw random states, and the seed makes each run draw the same states.

## Inductive invariants

TLC checks a bounded instance only. An inductive invariant `Inv` closes that gap: `Inv` holds in
the initial state, and each step from a state where `Inv` holds goes to a state where `Inv` holds.
Then `Inv` holds in every reachable state, at every size and length of run, and so does each safety
property that `Inv` implies. This is step 1 of #1307 (inductive invariants, then proofs); the proofs
(TLAPS, then Lean / Veil) are its later steps. The method is the one of Antithesis's
`proof/MCInv.tla` (antithesishq/etcd-raft-formalized) and of Lamport's note "Using TLC to Check
Inductive Invariance".

| Model | `Inv` | How TLC checks it | Checks |
|---|---|---|---|
| `decree/` | the invariant of Lamport's proved `Paxos.tla`: each proposal is `SafeAt` its ballot, a vote is for the proposal of its ballot, a promise reports the highest vote below it, an acceptor's vote is its highest; a wiped acceptor that takes no step again counts as one that will not vote | every state of the instance where `Inv` holds (`IndInit` in `MC.tla` builds them from smaller choices), then one step: 3 acceptors, 2 values, one wipe; 13 thousand states at 2 ballots, 1.3 million at 3 ballots | `inductive`, `inductive_three` pass; `inductive_ignore` (a proposer that ignores the reports) violation |
| `multi-paxos/` | the decree invariant for each slot, plus: a record is a vote or a learn of a value chosen at or below it, a promise reports the record of each slot at or above `from`, each slot at or above a leader's allocator is free and holds no proposal of its ballot, the disk is the memory (`Durable = "before"`) | random states where `Inv` holds (`IndInit` draws 100,000 with `seed=1`), then one step: the states are too many to list (more than four million at one slot and two ballots). 3 acceptors, 2 slots, one restart, one wipe | `inductive` (2 ballots), `inductive_three` (3 ballots), `inductive_grid` (a 2 x 2 grid, rounds in any column) pass; `inductive_prefix` (a prefix allocator) and `inductive_lowest` (the lowest report) violation |

Each inductive check also has the safety properties of its model as invariants, so TLC checks
that `Inv` implies them on every state it starts from. `InvApplies` names the configurations
that `Inv` covers: `decree/` with no wipe or with a wiped acceptor that takes no step
(`init.cfg`, where `Phase1 = "every"` keeps a wiped acceptor's empty report safe, is not
covered), `multi-paxos/` with `Durable = "before"` and `Identity = TRUE`. A pass of a
`multi-paxos/` check says that no drawn state breaks `Inv` in one step; it is not a proof.

## Proofs

A proof holds at every size: every number of acceptors, values and ballots, and every quorum
system whose quorums intersect. A proof is a module `<Model>Proof.tla` beside its model, checked
by TLAPS (`nix develop .#proofs --command scripts/check-proofs.sh`; CI's `proofs` jobs of the
`TLA` workflow prove every obligation again, with no cache, one job for each `*Proof.tla`
module). A long proof is split into several `*Proof.tla` modules so that each module fits the 30
minutes of a job; the module that extends them all is the map of the proof. This is step 2 of
#1307.

`specs/tla/PROVED.sha256` pins the files that the proofs cover: each `*Proof.tla` module and each
module of its folder that a proof extends (step 4 of #1307, #1459 (pin the proved specs)). A full
pass of `scripts/check-proofs.sh` (no argument) writes it with `scripts/proof-hashes.sh write`.
The `plan` job of the `TLA` workflow runs `scripts/proof-hashes.sh check`, which fails when a
pinned file changed and the pin was not written again, or when a covered file is not pinned. The
pin shows a reader which files the proofs cover. It does not prove that a proof ran: the
`proofs` jobs do, since the plan runs them on each change of a proof folder. The pin follows only
the modules of the folder that a `*Proof.tla` names after `EXTENDS`, not the modules that those
modules extend in turn: today `Decree.tla` and `MultiPaxos.tla` extend standard modules only.

| Model | Proof | What it proves |
|---|---|---|
| `decree/` | `DecreeProof.tla` | `InitInv` (`Init => Inv`), `StepInv` (`Inv /\ [Next]_vars => Inv'`) and `InvAgreement` (`Inv => Agreement`), for the configurations of `InvApplies` with `Pick = "highest"` and both `Phase1` rules: 692 obligations, Z3, Zenon and LS4, about 2 minutes |
| `multi-paxos/` | `MultiPaxosProof.tla` (the map), with `MultiPaxosFactsProof.tla` (the shared lemmas, `InitInv`, `InvAgreement`), `MultiPaxosPhase1Proof.tla` (`Prepare`, `Promise`), `MultiPaxosWinProof.tla` (the recovery of `Win`), `MultiPaxosPhase2Proof.tla` (`Allocate`, `Accept`) and `MultiPaxosLearnProof.tla` (`Decide`, `CatchUp`, `Restart`, `Wipe`, and `Persist` as a stutter step) | The same three theorems for every number of slots, with every quorum system where each Phase-1 quorum meets each Phase-2 quorum of each slot (`QuorumFacts`, a hypothesis and not an `ASSUME`, because the control `full_grid_one_column` breaks it), for the configurations of `InvApplies` with `Pick = "highest"`, `Alloc = "frontier"` and both `Gap` rules: 2,307 obligations, Z3, Zenon and LS4, at most 6 minutes for each module |

The TLAPS of nixpkgs (1.4.5) has no Isabelle. So a proof uses no induction lemma (they
need Isabelle; a least or highest member of a set of naturals comes from `WFMin`, which is first
order, as `MinExists` and `MaxExists` of `DecreeProof.tla` and `MultiPaxosFactsProof.tla` do).
The last step of `Safety` (`Spec => []Agreement`) is the TLA rule INV1 applied to the three
theorems. LS4, the PTL backend, checks it (`BY PTL DEF Spec`). The flake builds LS4 and
`ptl_to_trp`, the translator that tlapm pipes into it, from source in the `proofs` shell,
because the TLAPS of nixpkgs has neither. No proof has a `PROOF OMITTED` step, and
`scripts/check-proofs.sh` fails on one.
The constant facts that a proof uses are named `ASSUME`s of the model (`AcceptorsFinite`,
`QuorumsType`, `NoneNotValue` and others), so TLC checks them on each configuration. A proof has a
control too: remove a guard that the proof needs (for example `prom[a] <= b` in `Accept`) and
TLAPS fails.

## Guard mutation

`scripts/mutate-tla.py` tests the checks of each model (#1311 (mutate spec guards)). A guard is
a conjunct of an action that changes no variable: a `/\` item of a definition that primes a
variable, with no prime, no `UNCHANGED` and no call of an action. For each guard, the script
sets that one item to `TRUE` in a copy of the specs and runs the `ci` checks of `checks.txt` on
the copy, the `pass` lines first. The mutant is caught when one check does not give its listed
result; a check of each folder that links the module counts too. A check that runs past 3 times
its time on the specs as they are (at least 60 seconds) is stopped, and the mutant counts as
caught: the guard bounds the state space of the check.

A mutant that every check accepts is a survivor. The guard is not necessary, or a property is
missing. Remove the guard, or add the check that fails without it (often a liveness check, or a
control). There is no allow list.

    nix develop .#tla --command scripts/mutate-tla.py decree            # every guard of a folder
    nix develop .#tla --command scripts/mutate-tla.py --list decree     # the guards only
    nix develop .#tla --command scripts/mutate-tla.py --parse decree    # each mutant parses

The `spec` job of the `TLA` workflow runs `--parse` on each folder. The `TLA mutants` workflow
runs every mutant each week and on demand, in slices of 30 minutes
(`scripts/mutate-tla-shards.txt`, where every folder has a line), and its `report` job lists
the survivors. A run on demand can name its folders (`folders`) and hold each of its two
matrices to a number of jobs at a time (`max_parallel`). The `--help` of the
script names the other options (`--shard`, `--budget`, `--baseline-out`, `--baseline-from`,
`--out`, `--skip`). The trace specs (`*Trace.tla`) are not mutated: `checks.txt` does not run
them.

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
of `consensus-sim`.

**Every trace spec extends a model that TLC checks, and defines no step of its own** (#1306
(trace specs extend the checked model)). A step of the code that the model does not have is a
step of the model, with a check and a control in its `checks.txt`, never a step of the trace
spec. A trace spec only binds each event to an action of its model, with the arguments of the
event, and checks what the event observes (the votes a promise reports, the frontier of a win,
a watermark). An event that changes nothing (a slot learned again) is a stutter. So the spec
that judges the code is the spec that TLC checks, and a trace check never accepts a step that
no checked model takes. The trace specs today are:

- `decree/DecreeTrace`: the decree walk.
- `node/`: the implementation model of `ColocatedNode`, `Node.tla`: the log (Phase 1 from the
  first slot the owner does not hold, the frontier, the recovery with the `Noop` gap fill, the
  learns and the records of a learn, a restart that loses a suffix of the chosen index),
  matchmaking, the GC, the retirement and the rejoin, the quorums of a configuration (a
  majority or a grid), the compaction floor and the trim jump, the faulty entries of CTRL with
  the pages of a promise, the tri-state tally and the repair probe, the read replica, the
  leaderless reads, the rounds of the proxy leaders (a delegation, the fan-out, the `Commit` on
  a Phase-2 quorum of the round's configuration, the close of a round on a relayed `Nack` or an
  eviction, a crash) and the cooperative handoff (a leadership as a state of a node, the payload,
  the install behind the promise guard and the frontier guard, the frontier read from disk, a
  step down of any cause). `NodeTrace.tla` binds the events of a walk to its actions, and ten
  thin trace specs extend it, one for each family of walks: `MultiPaxosTrace` (every cluster
  flavor without matchmakers), `ProxyLeaderTrace` (the flavors with proxy leaders),
  `LeaderHandoffTrace` (the handoff flavor), `MatchmakerTrace` (the flavor
  with matchmakers), `ReadsTrace` (the lagging-reads flavor), `TruncationTrace` (the
  `compaction` flavor), `CompartmentsTrace` (every compartment, and its `compaction` steer),
  `FaultsTrace` (`disk-faults` and `disk-faults-3`), `RetireTrace` (`retiring`) and
  `MatchmakerGridTrace` (`reconfiguring`). Each sets the bounds of the model from the events of
  its walk (`crates/sim/consensus-sim/src/cluster/trace/node.rs`).
- `generations/HandoverTrace`: the matchmaker set handover over a chain of generations, with
  the registrations and the GC acks of one journal, on the `handover` flavor. `Generations.tla`
  closes a bootstrap over a majority of the old generation that froze, and checks that every
  floor and every registration a majority held reaches each later generation.
- `batcher/BatcherTrace`: the batcher and the unbatcher, on the batcher walk. `Batcher.tla` has
  the walk's network (a batch lands whole, maybe twice, with no Paxos under it) and checks that
  each answer holds its own records and that a batch keeps push order. `limits` (ci) adds
  `BatchFits` (a batch holds a write; a lane batch fits the limits and holds no write that goes
  alone) and `SendOrder` (the batches of a journal leave in the order the batcher got their
  writes: a write that goes alone flushes its lane first), on an empty write and writes that
  pass one limit each together.
`proxy-leader/` and `leader-handoff/` stay as the design models of their rules, with their
liveness checks; `node/` has the same safety rules as constants, each with its control in
`node/checks.txt` (#1343 (merge proxy and handoff into the node model)). The beat guard, the fence
resignation and the pending slots that a successor proposes again are rules of liveness: in
`node/` a leader may step down at any time, so `leader-handoff/` keeps their checks.

Each trace spec has a control: a bug planted in the driver that TLC refuses
(`tests/traces.rs`). Each trace spec also names the steps that some checked walk must take
(`Spec::rules` in `crates/sim/consensus-sim/src/cluster/trace/rules.rs`, #1231 (traced walks
reach the rules)): a walk that never fills a gap leaves the gap-fill guard unjudged. A change
of `specs/tla/` alone also runs the trace checks, in the `traces` job of the `TLA` workflow (four
shards): a change of a model changes what its trace specs accept.

A module that several models share lives in its own folder and is linked into each folder that
uses it by a symbolic link: `grid/Grid.tla`, the quorums of a grid, is linked into
`config-register/`, `reads/`, `multi-paxos/`, `leader-handoff/`, `compartments/`, `node/` and
`proxy-leader/`.

## Experiments

`specs/tla/experiments/` holds models that compare candidate designs before a decision. They are
not models of the plan, and no rule of `paros-consensus` comes from them until Pierre decides.
Each experiment has one folder for each design, with the same layout as a model, and a
`README.md` with the report. `scripts/check-tla.sh` runs an experiment only when it is named
(`scripts/check-tla.sh experiments/stale-stopped/restart`); CI checks each experiment folder too.

- `experiments/stale-stopped/`: #1229 (a stale `Stopped` reply reuses a decree ballot). The
  ballot of the handover's decree across a restart of the reconfigurer (`restart/`): the rule of
  the code today as the control, against a nonce of the incarnation in `Stop` and `Stopped`
  and a decree round on disk. `Asked` (a matchmaker answers only a `Stop` or a `Prepare` that the
  node sent) and `FoldOnce` (the stop quorum counts one reply of each member, in phase "stop")
  hold in `nonce`, `durable` and `retry_one` (retries in one incarnation, nonce, 224 thousand
  states).

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
| The matchmaker set handover | `matchmaker/reconfigurer.rs` | `handover/`, which extends `handover-gc/`; `handover-chain/` for three generations and clients that reach one member at a time; `handover-scalars/` for the retirement records, the effective configuration and the chain of segments; `generations/` for the chain of handovers that the code runs, with the registrations and the floor of a journal | #1097 (rework 1.8), done |
| Grid quorums | `membership/quorum_system.rs` | `grid/`, used by `config-register/`, `reads/`, `multi-paxos/`, `leader-handoff/`, `compartments/` and `proxy-leader/` | #1102 (rework 1.13), done |
| The proxy leader | `proxy_leader.rs` | `proxy-leader/` | #1188 (rework 2.9), done |
| The batcher, the replica and the read replica | `batcher.rs`, `replica.rs`, `replica_node.rs` | `compartments/`; `batcher/` for the batcher walk | #1190 (rework 6.10), done |
| `ColocatedNode` as one implementation model: the log, the handoff, the rounds of the proxy leaders, matchmaking, the GC and the retirement, the floor and CTRL, the read replica and the leaderless reads, as the code runs them together | `node.rs`, `proxy_leader.rs` and the roles they wire | `node/`, which every trace spec of a cluster walk but `generations/` extends | #1306 (trace specs extend the checked model), #1343 (merge proxy and handoff into the node model), done |
| Leaderless reads | `quorum_read.rs`, `node/quorum_reads.rs` | `reads/`; `read-replica/` for the read replica with a stale basis | #1016 (model of leaderless reads), #1099 (rework 1.10), done |
| Phase-1 bypass of Matchmaker MultiPaxos (§3.6), with GC, the handoff and two bypasses in a row | not built (#579 (zero-stall reconfiguration) is at the tag) | `zero-stall/` | #1018 (zero-stall model, more rules), done |
| Truncation: the compaction floor as a fold of the journal, the trim-point jump, the retry of a laggard, the gap fill, and the faulty entries of CTRL with the repair probe, on plain Multi-Paxos | `acceptor/retention.rs`, `node/catch_up.rs`, `node/acceptor.rs`, `node.rs` (`compact`), `replica.rs` (`compaction_target`), `proposer.rs` (`slot_decidable`), `proposer/probe.rs` | `truncation/` | #1017 (truncation model), #1216 (truncation model, part 2), done |
| Truncation across a reconfiguration onto empty stores: the GC of the old configuration only once a Phase-2 quorum of the new one holds the prefix up to the election fence, a new member that gets its floor from `TrimmedTo` or catches up | `node/gc.rs`, `collector.rs` (`covered`), `node/learn.rs` (`mark_chosen`), `node/reconfigure.rs` | `truncation-reconfig/` | #1218 (truncation model, part 3), done |
| CTRL on a grid: the tri-state tally on a grid and across the configurations of `H_b`, a scrub and a boot that give one faulty record, the repair of a chosen value in one write | `proposer.rs` (`slot_decidable`), `acceptor.rs`; the scrub and the repair are designed, not built | `ctrl-grid/` | #1263 (CTRL recovery in the journal), done |
| One id names at most one journal, forever: the coordinator of a level draws a `TenantId` or a `JournalId`, the fold of the level checks it at apply and refuses an id in use (tombstones too), slots are booked only under an id that the fold gave, the host key is `(TenantId, JournalId)`, the cell tenant's id is drawn at `cell init` before any universe (§3.8 of `docs/architecture/`) | not built (the tenancy crate and `paros-names`, after journal v1) | `journal-ids/` | #1419 (the tenancy wrapper and paros-names) |
| The takeover by a new coordinator: the view of a term from the registrations of that term, the order checked and done in one write, the registration that raises the fence, the booking by durable id, a new reference at each term, `Assign` idempotent by request id, the stop of a deposed coordinator, the directory asked after a `gone` (not a Paxos flavor: it runs above the election) | not built (`paros-cell` and `paros-machine`, after #1420 (paros-election)) | `takeover/` | #1422 (the takeover by a new controller) |
| Segments in the core: the `Roll` cut, the replicas of a segment, the chain, the route of a `Commit` | not built (step 3 of `docs/archive/journal-design-a/prompt.md`) | `segment-roll/`, `multi-paxos-roll/` | step 1 of the segments in the core, done (PR #1540 (segment-roll)); step 3, the `Roll` on the Multi-Paxos model |
| Segments in the core: `Held` at `F + 1` replicas and the floor at the end of a held segment (R7, departure 2), the jump of a laggard below that floor, a reconfiguration onto empty stores after the acceptors dropped a segment | not built (step 6 of `docs/archive/journal-design-a/prompt.md`: `collector.rs`, `acceptor/retention.rs`, the beat) | `held-floor/` and `held-floor-pass/`, which extend `truncation/`; `held-floor-reconfig/`, which extends `truncation-reconfig/` | step 6 of the segments in the core, the model |
| CheckQuorum | `proposer/authority.rs` | `check-quorum` (to write) | #1136 (rework 6.7) |

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
| Handoff (8.2): one hop, the open slots carried, the minter that handed on never bypasses, the successor never bypasses | `handoff` (with `HandOnOneHop`: a ballot moves once, from its minter, while it leads), `handoff_paros` | `handoff_drop`, `relinquish_off`, `successor_bypass`; two hops: `leader-handoff/two_hops` |
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
  the old ballot. A hand-on to the minter itself drops the open slots the same way, so the control
  also fails with no successor.
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
  column below quorum), `degraded-reshape/` (at the git tag `journal-design-a`)): it waits for a Phase-1 quorum of the old grid,
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
| `owner_two` | the owner rule with two ballots: `Inv` and `Monotone` (below) | pass (2.6 million distinct states, about 3 minutes with 4 workers; ci) |
| `owner_noguard` | the owner rule with no promise guard, two ballots | `Agreement` violation (750 thousand states; ci) |
| `owner` | the owner rule, three ballots | no violation in 45 million distinct states, to depth 24 (a 40-minute bounded run; `long`). The control broke at 11.6 million. |

The rules that `owner_two` and `owner` check besides `Agreement` and `SkipSafe` (#1345 (guards
that survive mutation)): `OneValue` (a ballot proposes one value), `VotesInConfig` (Phase 2
addresses `C_b` alone), `RegTrue` (a matchmaker holds the configuration and the kind that the
ballot campaigned with), `HistComplete` (a matchmaker never registers a ballot below one it
answered, so a candidate knows every lower registration of its quorum), `FloorBelow` (the floor
that a candidate learns is at or below its ballot), `WmChosen` (a watermark sits at a ballot whose
value is chosen), `PromisedPrepared` (an acceptor promises only a ballot whose owner sent a
`Prepare`), and the action property `Monotone` (a promise and a watermark never go down, and
a ballot's life goes one way). The model has no crash and no stop action: a crashed acceptor or a
campaign that stops takes no more step, which every behaviour already holds. The GC is always on.

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
| `cas` | the CAS, two ballots, one crash; any acceptor may vote in Phase 2 (a vote is for the value Phase 1 made safe) | pass: `Agreement`, `Electable` and `GenUp`, the generations of a matchmaker never go down (7.7 million states) |
| `one` | one matchmaker, one configuration, three ballots, two crashes: the kill policy of `Crash` (#1345 (TLA+ guard survivors)) | pass: `Agreement`, `Electable` and `GenUp` (6.8 million states) |
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
| `live_mm_stay` | control: the same with one configuration: a read needs a live matchmaker (with the crash guard of `Read` off, the check passes) | `Wins` violation (10 thousand states) |
| `agree` | the rule, three ballots, one matchmaker, stores released | pass: `Agreement`, `Electable` and `GenUp` (4.0 million states, 3.5 minutes) |
| `agree_mm3_small` | the rule, three matchmakers, two ballots, no crash: a read or a write that skips a majority shows only with three matchmakers (#1345 (TLA+ guard survivors)) | pass: `Agreement`, `Electable` and `GenUp` (5.2 million states, 3.5 minutes) |
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
| `wipe_two` | `safety_small` with two wipes: the kill policy of `Wipe` stops the second wipe in a set of three (#1345 (TLA+ guard survivors)) | pass: every invariant |

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
The `pass` checks also check `RegHeld` (a registration that a majority of a generation took
stays held, or below the floor, in each serving majority of that generation and of each later
one), `RegsAboveFloor` (a matchmaker holds no registration below its floor) and `ChosenOnce`
(the successor state is chosen once). They make each guard of the model necessary (#1345
(guards that survive mutation)).

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
configuration and the retirement records (`handover-scalars/`), the size of the successor set and
more than one journal. `handover/` (#1097
(rework 1.8)) extends this model with the target, the size and the register.

### `handover-scalars/`: the retirement records, the effective configuration and the chain of segments across a handover, for #1230 (rules no check tests)

`handover-gc/` carries the GC floor and the registrations across one handover of a matchmaker
set. A journal's scalars (`JournalScalars`, `matchmaker/state.rs`) hold two more things that
cross the handover, and this model checks both. It is the same handover, from `{X, Y, Z}` to
`{Y, Z, W}`: a member of `M0` freezes and reports, the successor state is built from a majority
of reports (`close_stop`), and a member of `M1` activates with that state merged into its own.

- **The retirement record** of each node (#1002 (retire fence gaps), #1037 (rejoin)): an
  incarnation and a flag. A retirement check of incarnation `i` records `[i, retired]`; a rejoin
  of incarnation `j` clears it to `[j, not retired]`; a check of an incarnation below the
  record is refused (`Rejoined`), and a check of an earlier incarnation can arrive late. Each
  completes only when every matchmaker of one generation
  agreed (`RetireCheck`, `RejoinCheck`). The close merges the records node by node
  (`Retirement::merge`: the later incarnation wins, equal ones OR their flags). The node
  retires and rejoins once each (`NI = 2`).
- **The effective configuration**: the highest ballot of a reconfiguration registration that a
  matchmaker took. The GC floor drops registrations but never this scalar, and the close takes
  the maximum of the reports. Two ballots; ballot 1 reconfigures.
- **The chain of segments** (R5 of §4.7 of `docs/architecture/`, step 4 of the segments plan): an
  add-only map from a segment id to its record (a start, an end, a state that only grows). The
  log chooses `NS` `Roll` commands in order; a node whose walk applied the `k`-th publishes one
  write, segment `k` sealed at its end and `k + 1` open from there, to a matchmaker of one
  generation, which merges it; a publish completes when a majority of one generation took it. The
  close merges the chains of the stop quorum, and an activation merges the result into the
  member's own chain. The start and the end stand for every field that the log gives (the
  replicas too).

| Constant | Values |
|---|---|
| `Ask` | `all`: a check completes when every matchmaker of its generation agreed (the code). `quorum`: a majority is enough. |
| `Frozen` | `FALSE`: a frozen member answers no check (`generation_refusal`, the code). `TRUE`: it answers, after its report left. |
| `Close` | `merge`: the records of every report of the stop quorum, merged (the code). `one`: the records of one report. |
| `Eff` | `scalar`: the successor's effective configuration is the maximum of the reports' scalars (the code). `history`: it is the highest reconfiguration among the registrations carried above the floor. |
| `Source` | `applied`: a node publishes the end of a segment only after its walk applied the chosen `Roll` (R3, the code). `proposed`: it publishes on a proposal, with an end the log may not choose. |
| `Grow` | `max`: an activation merges the reconstructed chain into its own, field by field (the code). `overwrite`: it replaces its chain with the reconstruction. |

The invariants: `RetireHeld` (a completed retirement of `i` is held by every member that serves
its generation or a later one, unless a later incarnation came back), `RetireCarried` (the same,
for the later generation only), `RejoinHeld` (after a completed rejoin of `j`, no serving member
holds an earlier incarnation) and `EffCarried` (a reconfiguration registered at a majority is
the effective configuration, or a later one, at each member of a later generation and at a
member of each serving majority of its own generation). For the chain: `ChainTrue` (every
record at every matchmaker is the one the chosen log gives, so a read, a merge over any
matchmakers, is true, and a publish that only a minority took is already true), `ChainCarried`
(a seal a majority of `g` took is in the chain of each member of a later generation and of a
member of each serving majority of `g`) and `ChainGrows` (at each matchmaker a record never goes
away and its state never falls). The `pass` checks also check
`RegsAboveFloor`, `ActiveInSet` and `ChosenOnce`, which make each guard of the model necessary
(#1345 (guards that survive mutation)).

Results (`handover-scalars/checks.txt`):

| Check | Rule | Result |
|---|---|---|
| `retire` | the code | `RetireHeld`, `RetireCarried`, `RejoinHeld` pass (570 thousand states) |
| `retire_one` | the close takes the records of one report | pass (704 thousand states) |
| `retire_frozen` | a frozen member answers a check | `RetireCarried` violation |
| `quorum_merge` | a majority completes a check, the close merges | `RetireCarried` pass |
| `quorum_one` | a majority completes a check, the close takes one report | `RetireCarried` violation |
| `reach_retire`, `reach_rejoin` | reachability: a retirement, then a rejoin, completes at generation 0 and every member of `M1` is active | violation (reached) |
| `effective` | the code | `EffCarried` pass (246 thousand states) |
| `eff_history` | the successor rebuilds the effective configuration from the registrations | `EffCarried` violation |
| `chain` | the code, two rolls | `ChainTrue`, `ChainCarried`, `ChainGrows` pass (21 thousand states) |
| `chain_proposed` | a node publishes on a proposal | `ChainTrue` violation |
| `chain_one` | the close takes the chain of one report | `ChainCarried` violation |
| `chain_frozen` | a frozen member takes a publish | `ChainCarried` violation |
| `chain_overwrite` | an activation replaces its chain | `ChainGrows` violation |
| `reach_minority`, `reach_chain` | reachability: a seal that only a minority took crosses the handover; two seals cross it | violation (reached) |
| `reach_eff` | reachability: a report whose floor passed the matched reconfiguration | violation (reached) |

What the model finds: no rule of the code is refuted. The record crosses the handover because
of two rules together: a check needs every matchmaker, and a frozen matchmaker answers none.
Then each member of each stop quorum recorded the retirement before its report left, so even
a close that takes one report carries it (`retire_one` passes). The merge of the close matters
only for the weaker rule: with a check that a majority completes, the merge carries the record
(`quorum_merge` passes; the generation of the check itself is still wrong there,
`retire-fence/ask_quorum`), and one report does not (`quorum_one`).

The traces of the controls:

- `retire_frozen`: `X` agrees to retire `n`. `Y` freezes and reports no record, then agrees.
  `Z` freezes and reports no record, then agrees. The check completes on `{X, Y, Z}`, the close
  takes the reports of `Y` and `Z`, and `W` activates with no record of `n`.
- `quorum_one`: `X` and `Z` agree and the check completes on a majority. `Y` and `Z` freeze, and
  the close takes the report of `Y` only, which holds no record.
- `eff_history`: ballot 1 (a reconfiguration) is registered at a majority, ballot 2 is matched
  and its GC raises the floor of the stop quorum to 2, which drops the registration of
  ballot 1. The close finds no reconfiguration above the floor, and `M1` holds no effective
  configuration. This is the reason for the scalar (`JournalScalars::effective`).

Not in the model: the registrations that block a retirement (`retire-fence/` checks that rule),
Phase 1, the decree that chooses the successor, lost matchmaker state, more than one handover
and more than one journal.

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
- `ChosenOnce`: the decree chooses one set, one time.
- `Ends` (with `LiveSpec`): from some point on, the decree chose a set and each live member
  of it serves generation 1, active. The handover steps are fair, the clients and the crashes
  are not.

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
| `two` | the rules; two generations, one write, the floor | pass (926 thousand states) |
| `chain` | the rules; three generations, one write, no floor | pass (1.2 million states) |
| `two_writes` | the rules; two generations, two writes, no floor: a write that a member takes below a read or a write it holds lowers the register (`RegisterCarried`) | pass (1.25 million states) |
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
| `Learner` | The options of #890 (learner above the leader). A reader campaigns with a stale belief, promises its own ballot and sends no `Prepare`. `strict`: the node never takes a leadership below its promise as its read basis, and opens or serves no read on it (the code before option (b)). `own`: option (b), the code (`ColocatedNode::reader_fence`), the node takes the basis when the only promise above it is its own abandoned campaign. `any`: the node ignores its promise. `off`: no such campaign. |
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
| #890, `strict` (the code before option (b)) | pass |
| #890, option (b) (the code) | pass |
| #890, the node ignores its promise | pass |
| `grid_full`: the full grid, a change of members, the row read (the code) | violation (300 thousand states, 15 seconds) |
| `grid_full_static`: the full grid, no change of members | pass (588 thousand states, 35 seconds) |
| `grid_full_seal`: the full grid, a change of members, `Seal = "reads"` | pass (1.6 million states, 1.5 minutes) |
| `grid_full_seal_others`: `Seal = "others"`, the seal of each configuration of `H_b` other than `C_b` (the code, #1209) | pass (1.9 million states, 1.5 minutes) |
| `grid_full_seal_others3`: `grid_full_seal_others` with three ballots of two proposers, `OwnerParos3` (`long`) | no violation in 16.1 million distinct states (bounded: 30 minutes, 2 workers) |
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
in `docs/journal/02-data-plane.md` §J2.5 (tag `journal-design-a`)). The code is #1209 (seal each row of a full grid):
`Election::covered` asks the seal (`AcceptorConfig::has_phase1_seal`) of each configuration of
`H_b` other than `C_b`, and the Phase-1 quorum alone of `C_b` (`Seal = "others"`, check
`grid_full_seal_others`). A column of `C_b` chooses at the new ballot, and each row of `C_b`
meets that column, so a row of `C_b` needs no seal. The plain deployment (`H_b = [C_b]`) and a
new leader of the configuration in force pay nothing.

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
`learner_own_serves` finds a read that option (b) serves and that `strict` refuses. The code
builds option (b) (`ColocatedNode::reader_fence`) for every quorum system, so option (b) is
checked on majorities (`learner_own`), on the full `2 × 2` grid with the seal of the code
(`learner_own_grid`, 2.0 million states, 1.7 min) and with a lagging pool (`learner_own_lag`,
0.6 million states, 21 s). The two last are `long` checks: `reads` already fills the time of one
CI job. In the implementation model `node/`, a
campaign promises its own ballot at its `Prepare`, so the promise of a campaign that the
matchmakers refused before its `Prepare` is not in a trace, and the basis of option (b) is an
ordinary `Basis` step there.

Option (a) of #890, where the leader campaigns above the promise, adds no rule to the model: it
is one more ordinary campaign, and `reads_plain` covers it. Its cost is a leader change for each
stale campaign of a removed node. The row "the node ignores its promise" passes in this model
because an acceptor of the basis configuration always reports a later ballot. It is not a
recommendation: the learner checks have only majority quorums.

Not in the model: GC watermarks, matchmaker generations, the replicas that only apply, a read that expires,
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

**The read replica** (`replica_node.rs`, #1230 (rules no check tests)): the constant `Replicas`
names the readers that are not acceptors. A replica has no promise, so nothing of its own gates
its read. It learns its basis only from the beat of a leadership that won, and a beat can be
late. A replica restart forgets its basis and its open read, so its next basis can be the late
beat of an older leadership. A replica that hears its first beat late has a stale basis too
(the trace of `replica_no_fence` takes that path, with no restart). Only the fences of the
answers end a read on a stale basis. The checks run in their own folder, `read-replica/`, which links `Reads.tla` and
`MC.tla`, so that the `reads` job stays inside its minutes. The replica `r1` is the one reader.

| Check | Shape | Result |
|---|---|---|
| `replica_stale` | `reads_plain` with the replica | `ReadSeesAcked`, `Agreement` pass (120 thousand states) |
| `replica_again` | the same, and a second read (`ReadAgain`) | pass (164 thousand states) |
| `reach_replica_stale` | reachability: the replica opens a read on a basis below a ballot that won, after an ack | violation (reached) |
| `replica_no_fence` | the answers carry no fence (`Fence = "none"`) | `ReadSeesAcked` violation |
| `replica_grid` (`long`) | `grid_full_seal_others` with the replica | pass (2.0 million states, 1.5 minutes with 4 workers) |

A stale basis is safe for the reasons that the other readers are safe. A newer leadership won
only after a Phase-1 quorum of every configuration of its history promised it. Under a majority,
two Phase-1 quorums meet, so the answers that close a read on the old basis hold a fence above
the basis, and that answer ends the read. On the grid, two Phase-1 quorums need not meet; the
seal rule of the code (`Seal = "others"`, decision 31) puts a promise of the newer ballot in each
read target, and `replica_grid` checks it.

**A grid whose Phase-1 quorums do not meet** (#1483): `QuorumSystem::admits` lets in every grid
with `1 <= acks <= rows`. A grid that is not full (`acks < rows`) gets no row seal, and a reader
asks every member and closes on a Phase-1 quorum of the answers. When `2 * (rows - acks + 1)
<= rows`, two Phase-1 quorums of one column can be disjoint. The checks run in their own folder,
`reads-rows4/`, which links `Reads.tla` and `MC.tla`: four rows, one column, the members
`a1..a4` and then `a2..a5`, reader `a1`.

| Check | Shape | Result |
|---|---|---|
| `grid_rows4` | `acks = 3`: Phase 1 takes two of four, Phase 2 three | `ReadSeesAcked` violation (990 thousand states, 1 minute with 2 workers) |
| `grid_rows4_meet` | `acks = 2`: Phase 1 takes three, so two Phase-1 quorums meet | pass (3.5 million states, 6 minutes with 2 workers) |

The trace of `grid_rows4`: ballot 1 leads on `a1..a4`. Ballot 2 (the members `a2..a5`) wins
with the Phase-1 quorum `{a2, a3}` of the old members, and `{a2, a3, a5}` choose slot 1; the
leader acks it. `a1` hears the beat of ballot 1 and takes it as its basis, asks every member of
`a1..a4`, and closes on the answers of `a1` and `a4`, a Phase-1 quorum of the old members that
misses `{a2, a3}`. No answer holds a fence above 1 or a vote, so the read serves index 0 after
the ack. A write at the new members meets every Phase-1 quorum of the new members, but not of the
old ones. The rule that the checks select is candidate rule 1 of #1483: a grid that is not full
must have Phase-1 quorums that meet (`2 * (rows - acks + 1) > rows`), and `admits` must refuse
the others. **Open**: the code does not have this rule yet; `QuorumSystem::admits` still admits
`grid(4, 1, 3)`, and #1483 tracks the change. `grid_rows4_reach` shows that under `acks = 2` a
read that started after an ack is served. Rule 2 (seal each row) would make a one-column grid wait for every
member at an election, against quorum everywhere (#1257).

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
| Re-ask the pool too, liveness (`LearnsInForce`: the belief ends at the generation in force, under weak fairness) | `Witness`, `Wide` | pass |
| Re-ask the believed set only, liveness | `Witness` | violation (`LearnsInForce`) |

Each safety check also judges `BeliefForward`: a belief never goes back to an earlier
generation (#1345 (TLA+ guard survivors)).

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
code's assertion "a chosen slot holds its authoritative accepted record"). The action property
`OwnRecords` says that a won campaign's frontier is past every vote that the owner's own promise
reported: the candidate's records seed its tally (`open_phase1`), so a leader never allocates a
fresh command at a slot where it holds a vote. The action property `RecordMoves` says that a
record moves only by a vote of the node, a learn of a slot that it had not learned, or a fault:
`mark_chosen` returns early on a slot that is chosen already. A value is chosen when a Phase-2 quorum of the slot sent its `2b`
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
| `majority` | ci | the rules of the code, `OwnRecords` and `RecordMoves` | pass (470 thousand states) |
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
nothing more), the re-send beat and the retention of `ProxyLeader::tick` (with no lost message they change nothing),
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
| `BeatGuard` | `TRUE`: a beat at the leader's own ballot keeps it leader (`on_heartbeat` has it since #902 (node handler gaps)) | `live_late_beat`: `Ends` breaks |
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
| `late_beat_safe` | `safety` with the beat of the code before #902 (`BeatGuard = FALSE`) | pass (950 thousand states) |
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
every lower ballot there. `RegAboveWm` (a matchmaker holds no registration below its watermark)
needs the refusal in `matchmaker`. `matchmaker` and `gc_two` also check `HistFixed` (the history
of a ballot does not change once its matchmaking closed) and `RetiredFrozen` (a retired acceptor
neither promises nor votes).

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
  of the batch again, alone (`docs/journal/02-data-plane.md` §J2.4 (tag `journal-design-a`)).
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
| `Held` | What a deposed leader answers for a held batch. `"unknown"`: `Unknown`, or `ok` when the slot chose the batch (the rule, #1232). `"slot"`: a control, it answers `Unknown`, or `ok` from the outcome of the slot, whatever the slot chose. `"notleader"`: a control, it answers `NotLeader`, and the batcher follows the hint and sends the batch again (§2.4). |
| `Copies` | How many times in a run the transport may deliver a batch one more time. 0 in every check but `copies` and `copies_onerange`. |
| `Leaders`, `Restarts` | How many leader changes and replica restarts can happen. The batcher can stop for good at any time: no step of the batcher is fair, so a behaviour in which it takes no more step is that stop. |
| `MaxSends` | The ids of the batches. The rule sends each write in one batch, and again alone after a `TooLarge`, so no check of the rule sends more (`TypeOK`). The controls that send a batch again (`resend_ambiguous`, `held_notleader`) bound their search with the constraint `SendBound`. |

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
- `TypeOK` also says that an open slot holds a proposal and is not chosen yet; `HeldOwn`: the
  slots of the leader that holds answers change only by its own proposals; `NoTooLarge`: a
  batcher whose limit is the leader's never hears `TooLarge` (`batch_limit`, where the two writes
  do not fit one batch).

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

**The replicas of a segment** (R6 of `docs/architecture/04-7-segments.md`, step 5 of the
segments plan). The leader may propose a `Roll` at its frontier (`Rolls`). A chosen `Roll` at slot
`s` ends a segment at `s + 1` and takes no seq. Once the chosen prefix reaches a `Roll`, the chain
gets the start of the next segment and the boundary state, the fold at the `Roll` (`Publish`; the
chain itself is `handover-scalars/`). A replica of segment `k > 0` (`SegOf`) walks nothing before
it joins. It joins once the chain holds `k`: it jumps to the start of `k` with the boundary state
of the chain, as the trim-point jump does (`Join`). It then holds the scalars of that state and
no record of an earlier segment (its `base` is the first seq it holds). It walks its segment and
stops after the `Roll` that ends it. A read at a replica covers the writes of its segment and of
the later ones; a write below its base is read from an earlier segment. A sealed replica answers
a read with the records of its segment and "sealed" (`ReadSealed`), and the reader asks the
replica of the next segment, whose own read covers the rest.

The invariants judge a replica that started at its segment: `InOrder` (the fold of the prefix
above the base), `Dense` (the ranges cover `[base, next)`), `AnswerAgrees`, and `ReadFresh` (a
served read holds each complete write at or above the base; a "sealed" answer comes only from a
segment that ended, and holds each such write below its end). `InSegment` says that no slot a
replica walked comes after the `Roll` that ends its segment. `ChainTrue` says that each segment
of the chain starts after a chosen `Roll`, with every slot below chosen, and that its boundary
state is the fold below its start. `BaseTrue` says that the base of a replica is the next seq of
the boundary state of its segment, also after a restart, so that a replica drops no record of its
own segment. `Ends` also judges the segments: each replica walks to the end
of its segment, or to the frontier.

| Check | Result | What it shows |
|---|---|---|
| `segments_walk`, `segments_restart` | pass | `r2` keeps segment 1 and joins from the chain; with a restart |
| `segments_stop` | pass | `r1` stops at the end of segment 0 |
| `segments_restart_read` | pass | a read at the replica of segment 1, with a restart |
| `segments_read`, `segments_read_sealed` | pass | a read at the replica of segment 1, and at the sealed replica of segment 0 |
| `live_segments` | pass | the chain gets the segment, `r2` joins, and each replica walks to the end of its segment |
| `segments_empty` (control) | `InOrder` fails | the joining replica starts from the empty state, not the boundary state |
| `segments_early` (control) | `InOrder` fails | the chain holds the fold one slot before the `Roll` |
| `segments_rebase` (control) | `BaseTrue` fails | the boot folds again from the next seq of its refold, and drops the records of its segment |
| `segments_nostop` (control) | `InSegment` fails | the replica walks on past the `Roll` that ends its segment |
| `segments_serve` (control) | `ReadFresh` fails | a sealed replica serves a read past its end as if it were the tail: a write of segment 1 that was complete is missing |
| `reach_segment_read`, `reach_segment_sealed`, `reach_segment_sealed_answer` | violation | a replica of segment 1 serves a read after a write; a sealed replica and a joined one at once; a "sealed" answer to a read after a write |

The pass checks are small (one or two writes, one `Roll`, at most one restart, no leader change,
segments 0 and 1), so that the folder stays inside its CI budget. With two writes and both
replicas reading, the state space passed 79 million distinct states without an end; the checks
split the readers instead.

Not in the model for segments: a leader change with a `Roll` (a `Noop` that fills a `Roll` slot);
the route of a `Commit` to the replicas of its segment (R4d; `Learn` reaches every replica, and
`segment-roll/` checks the route) and the catch-up from a peer replica; a reader that walks the
chain across several answers (each read is judged at one replica, and a "sealed" answer sends
the reader on). The read in this folder has no ballots and no promise fence: the fences of a
leaderless read are `reads/`, which the segments leave as they are (2026-11-25): a read at a
replica of a segment takes the same quorum of watermarks, and the fences of `reads/` do not
depend on segments. The plan named `reads/` too for the read across a roll.

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
| `Rolls` | The `Roll` values of segments: control values that weigh nothing. `{}` in every check of this folder; `held-floor/` gives them a meaning. |
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
| `ctrl_plus_one` | ci | the same, the threshold is one above the highest readable report (#1263 (CTRL recovery in the journal), item 5) | `Agreement` violation (14 thousand states) |
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
| `Rolls` | The `Roll` values of segments: control values that weigh nothing. `{}` in every check of this folder; `held-floor-reconfig/` gives them a meaning. |

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
| `wipe` | ci | the rules of the code and one wipe, with the identity rule | pass (1.0 million states) |
| `wipe_cmd` | ci | `wipe` with a command as well as the truncate: with two values, a wiped owner that opens its ballot with an empty store breaks a chosen slot (#1345 (TLA+ guard survivors)) | pass (1.3 million states, 6 min with 2 workers) |

The long checks ran once on 2026-11-19 (#1218 (truncation model, part 3)) with 4 workers;
`fill_code` and `chain` ran on 2026-11-20 (#1230 (rules no check tests)).

A wipe (#1230 (rules no check tests)): an acceptor loses its whole store. With the identity
rule (`Identity`, the rule of `decree/` and `multi-paxos/`) a wiped acceptor never takes a step
again, so a Phase-1 quorum with it never forms, and `Recoverable` counts only the quorums
whose members are up. With the control it answers with an empty store: in `wipe_revote`, `a1`
votes slot 0 at ballot 1 and is wiped, then `a2` votes, so slot 0 is chosen on `{a1, a2}`, and
the quorum `{a1, a3}` of `C1` holds no record of it.

A faulty entry (CTRL) across configurations, the class of #267 (the tri-state tally over every
prior configuration, `proposer.rs`, `slot_decidable`), is not in this model: `ctrl-grid/` checks
it (`two_one` passes, and `two_current`, a tally that asks only the newest configuration, breaks
`NoFillChosen`). Not in the model, still: a restart (a restart keeps every durable
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

### `ctrl-grid/`: the CTRL tally on a grid and across configurations, the scrub and the repair, for #1263

`truncation/` checks CTRL on plain Multi-Paxos on a majority. This model adds grid quorums,
several configurations in `H_b` (the journal of design A read a sealed segment as an older
configuration, below its end; at the git tag `journal-design-a`), and a scrub, which finds a damaged record while the acceptor runs
(#1263 (CTRL recovery in the journal), items 1, 3 and 5). The model is the log of `truncation/`
with no floor and no truncation: a campaign asks from slot 0, a ballot is bound to one
configuration (`Cfg`), the win and the tally need a Phase-1 quorum of every configuration of
`H_b`, and Phase 2 runs in the configuration of the ballot. A damaged record is found by a scrub
(the acceptor keeps running) or by a boot (its ballots die and its learned value goes); both give
the same faulty record. A repair writes a faulty record again, under the ballot that it keeps.
`RecordsTrue` checks that a readable record at or above the ballot that chose a slot holds the
chosen value.

| Check | CI | Result |
|---|---|---|
| `grid4` | ci | pass: a grid of two columns of two (acks 1), two ballots, two commands, one damaged record, the repair of a chosen value (77 thousand states) |
| `grid4_restart` | ci | pass: the same with one restart, and `DeadSilent` (a ballot whose owner booted again proposes nothing) (263 thousand states) |
| `majority` | ci | pass: a majority of three, three ballots, so that a faulty report can sit above a readable one (2.8 million states, 7 min) |
| `two_one` | ci | pass: two configurations, `c1` (a1, a2, a3; ballot 1) and `c2` (a3, a4; ballots 2 and 3), one command |
| `two` | long | pass: the same with two commands (3.0 million states, 8 min) |
| `drop_first_up` | long | pass: a repair in two writes (delete, then write) with no restart |
| `drop_first_two`, `drop_first_lead` | ci | pass: the same with two ballots; in `drop_first_lead` the repairing acceptor owns the next ballot. The acceptor that deleted its record promises and campaigns only once it wrote the new one: without that, its empty promise fills a chosen slot. `drop_first_two` also checks `InBounds` (the faults and restarts stay in the bounds of the check) and `OwnRecords` (a won campaign's allocator is past every record of the owner's own promise) |
| `repair_none` | long | pass: no repair, only a later `Accept` heals: the repair adds a copy, it never decides |
| `live` | long | pass: `Ends`, the last ballot settles and every faulty record of a learned slot is repaired |
| `grid` | long, 60 min | pass (bounded): a grid of two columns of three, acks 2; no violation in 60 min (10.9 million distinct states, depth 19), a bounded run, not a proof |
| `grid_majority` | long | violation of `NoFillChosen`: the majority control on the default grid (668 thousand states, 1 min) |
| `reach_faulty`, `reach_block`, `reach_repair`, `reach_cross` | ci | violation: a faulty record of a chosen slot, a repair probe, a repair, and a ballot of `c2` that chooses a slot where a record of `c1` is faulty |
| `grid4_majority` | ci | violation of `NoFillChosen`: the qualifying answers need a majority of the four members. Ballot 1 chooses `v` on `a1`; a scrub finds `a1` damaged; ballot 2 hears `a1` faulty and three empty answers, three is a majority, and it fills the slot with a `Noop`. The grid needs both members of column 0 to qualify, and `a1` does not: the slot is blocked |
| `plus_one` | ci | violation of `NoFillChosen`: the threshold is one above the highest readable report (item 5) |
| `tally_none` | ci | violation of `NoFillChosen`: a faulty report counts as no vote |
| `two_current` | ci | violation of `NoFillChosen`: the tally asks only `c2`. Ballot 1 chooses `v` in `c1`, a scrub finds a record of `c1` damaged, and the leader of `c2` fills the slot with a `Noop` on the empty answers of `c2` |
| `damage_drop` | ci | violation of `Agreement`: a scrub drops the damaged record, and the slot reads as a slot with no vote |
| `repair_peer` | ci | violation of `Agreement`: the repair writes the value of a peer's vote under its own ballot, not a chosen value |
| `drop_first` | ci | violation of `NoFillChosen`: the repair deletes the record, the node restarts before it writes the new one, and the slot reads as a slot with no vote |

### `journal-ids/`: one id names at most one journal, forever, for #1419

An id is an address, never an authority (§3.8 of `docs/architecture/`). A create call carries a
name and a request id. The coordinator of the level draws the id, and the fold of the level
checks it at apply and refuses an id in use: the fold of the universe for a `TenantId`, the fold
of a tenant for a `JournalId`, unique inside its tenant. The model has two cells, whose cell
tenants draw their ids at `cell init` before any universe and are recorded by `AddCell` (the
directory half of `universe add-cell`), one user tenant created through the universe and placed
in cell `cA`, and three journals on one host
of `cA` (two of the cell tenant, one of the user tenant). Ids come from a small space, so draws
collide. A coordinator restart loses the draws not yet recorded; a create resumes by its request
id and reads back the id that the fold gave. A delete is final, and the host drops the records of
a deleted journal later.

| Constant | Values (first: the design that passes) |
|---|---|
| `Order` | `fold_first`: the host books slots only under an id that the fold gave. `book_first`: slots are booked under the id that the coordinator drew, before the fold checks it (the bug of the old resumed create). |
| `Tombs` | `TRUE`: a deleted id stays used in the fold. `FALSE`: a delete frees the id. |
| `Key` | `pair`: the host keys records by `(TenantId, JournalId)`. `journal`: by the `JournalId` alone. |
| `CellMint` | `draw`: the cell tenant draws its id at `cell init`. `counter`: a counter of the cell gives it (the first id). |
| `Fresh` | `TRUE` (liveness checks only): a `TenantId` draw never collides with another one, as a 64-bit random draw. `FALSE`: draws collide; safety never uses it. |
| `Restarts` | the restarts of the coordinators, 1. |

The rule, in four invariants: `JournalIdOnce` (a tenant fold gives a `JournalId` to at most one
journal over the whole history, deletes included), `TenantIdOnce` (the universe fold gives a
`TenantId` to at most one tenant), `DiskOneJournal` (on a host, one key holds the records of at most
one journal) and `BookedAfterRecord` (a host holds records under a key only after the fold gave
that key to the journal). Action properties: `GoneStays` (a delete is final) and `KeepLive` (the
host drops only the records of a deleted journal). Liveness: `AllCreated` (each cell joins, each
tenant and each journal gets its id) and `Dropped` (the host drops the records of a deleted
journal and books none again).

A draw never repeats an id that the fold refused for the same create: it stands for a 64-bit
random draw, which finds a free id in a few draws. A refused `AddCell` is final, because the
cell tenant's id is already on the cell's disks. So the liveness of the genesis rests on `Fresh`:
without it, the universe can draw for a user tenant the id of a cell that has not joined yet, and
that cell never joins (found while the model was written; the chance is about `2^-64` for each
pair of draws).

| Check | CI | Result |
|---|---|---|
| `ids` | ci | pass (9.0 million distinct states, about 2.5 minutes with 4 workers) |
| `live` | ci | pass: `AllCreated` and `Dropped` with `Fresh` (1.7 million) |
| `book_first` | ci | violation of `DiskOneJournal`: two journals of the tenant draw the same id and both book under it before the fold checks either (a create resumed after a lost draw is one more way to the same state) |
| `no_tombs` | ci | violation of `JournalIdOnce`: a journal is deleted, and a second journal of the tenant gets its id |
| `no_tombs_disk` | ci | violation of `DiskOneJournal`: the same, before the host drops the records of the deleted journal |
| `journal_key` | ci | violation of `DiskOneJournal`: a journal of the cell tenant and a journal of the user tenant get the same `JournalId` on one host |
| `counter` | ci | violation of `AllCreated`: both cell tenants get id 1, and the second `JoinFleet` is refused for good |
| `reach_journal_refused`, `reach_tomb_refused`, `reach_tenant_refused`, `reach_join_refused`, `reach_draw_lost` | ci | violation: a tenant fold refuses a used `JournalId`, then a tombstone; the universe refuses a used `TenantId` for a user tenant, then for a `JoinFleet`; a restart loses the draw of a create (`lost`), and that create still gets its id |

What is not in the model: the universe tenant's own id, drawn at `universe init`, and the id of a
tenant's control journal, drawn with the tenant and recorded beside it, not in the tenant's fold
(both are random draws, like the others, with the same `2^-64` chance of a collision); the names of `paros-names` (a name to an id, a pure fold), the delete
of a tenant (the same tombstone rule in the fold of the universe), the move of a tenant between
cells (its id does not change), a control journal rebuilt by recovery (§3.10, deferred; it forgets
its tombstones, the case for a birth nonce in `Assign`), and a peer that forges ids (out of scope).

### `takeover/`: the takeover by a new coordinator, for #1422 (the takeover by a new controller)

This model checks what a coordinator knows and does when it wins a term after another
coordinator (`docs/architecture/04-roles.md` §4.4, "The takeover by a new coordinator"). The term
is the election over a journal (`election/`, at the git tag `journal-design-a`), taken here as one atomic step. The pattern is FoundationDB's
`moveKeysLock`: a new holder fences the old one out of every machine, then builds its view again.

The steps:

- **Book.** A coordinator books a role for a request on a machine that its view shows. The booking
  is a record of its control journal, written under its term, and names the machine by its
  durable id. The journal refuses the write of a deposed coordinator, which then stops.
- **Order.** A coordinator orders the machine of each booked request with `Assign(request, term)`
  until an ack or a registration shows the role. A deposed coordinator does this too, with its old
  term.
- **Free.** A coordinator frees a booked request that it believes no machine hosts: the
  registration in its view does not name the role, no ack of its term names it, and it sent no
  order for it in its term. This is the step that trusts the view. It stands for every decision
  that a coordinator takes from its view (a free, a move, a heal).
- **Register.** A machine learns each new term from the journal and registers with its holder: its
  incarnation and the roles that it hosts. A machine sends its registration again until the holder
  acks it, so the model never loses a registration.
- **Fence.** A machine refuses an order below the highest term on its disk, with a nack.
- **Gone.** A message to an activation that is gone (an old term of a coordinator, an old
  incarnation of a machine) does not reach the new activation. An order to a gone incarnation
  comes back to its coordinator as `gone`. The coordinator then marks the machine as unknown and
  asks the fixed directory of the machine ("which reference serves this id now", §4.4) for its
  current registration. The reference of a machine changes at each incarnation because a dynamic
  reference of moonpool RPC names one incarnation. The model takes this as given, not as a rule.

The faults: a coordinator is deposed and keeps sending orders of its old term. A coordinator
crashes, loses its memory and acts again only after it wins a new term. A message is late,
reordered, sent two times or lost. A machine restarts as a new incarnation, and its disk stays.

The properties:

- `EffectBooked`: a role is hosted only on the machine that its booking names. So no role is
  hosted that the journal freed: a role that a coordinator freed from a wrong view is a ghost that
  no booking names.
- `OnceEach`: no role is hosted two times for one request.
- `NoLateEffect`: an order of an old term never takes effect after the machine wrote a newer term.
- `Complete`: for each registration of the current incarnation in the holder's view, every role
  that the machine hosts is in the registration, in an ack of the term or in an order of the term.
  It is the invariant that makes `Free` safe.
- `Settles`: once the faults stop, every request that is not freed is booked and hosted, and the
  holder knows it.
- `Quiet`: once the faults stop, the network is empty. So a deposed coordinator stops ordering.

The rules are constants, and `TRUE` is the rule: `OneWrite` (the machine checks the term of an
order and does its effect in one atomic write), `Idem` (`Assign` is idempotent by its request id),
`ClearView` (a new term starts with an empty view), `CoordRef` (a coordinator has a new reference
at each term), `IdInBooking` (a booking names the durable id; the reference of an order comes from
the view of the term), `RegFence` (a registration with term `t` raises the machine's highest term
to `t` in the same write), `NackStops` (a refused order stops its coordinator) and `GoneAsks` (a
`gone` marks the machine as unknown and asks its directory for its current registration).

| Check | Where | Result |
|---|---|---|
| `safety` | ci | pass: `EffectBooked`, `OnceEach`, `NoLateEffect` and `Complete`, two coordinators, one machine, one request, three terms, one crash, one restart and one loss |
| `safety_dup` | ci | pass: the same, with one copy of a message and two terms |
| `live` | ci | pass: `Settles` and `Quiet` under weak fairness, with the faults of `safety` and two terms |
| `live_dup` | ci | pass: the same, with one copy, two terms, and no crash or loss |
| `two_writes` | ci | violation of `EffectBooked`: the machine checks an order of the old term, registers with the new term, then does the effect. The registration does not name the role, and the new holder frees it |
| `two_writes_late` | ci | violation of `NoLateEffect`: the same trace, judged at the effect |
| `no_idem` | ci | violation of `OnceEach`: a copy of an order hosts the role two times |
| `keep_view` | ci | violation of `EffectBooked`: a coordinator wins a term again and keeps its view of its first term. It frees the role from the kept view while an order of the term between is in flight, and the order then takes effect |
| `fallback` | ci | violation of `EffectBooked`: a registration of an earlier term reaches the new term of the same coordinator through a fixed endpoint, does not name a role, and the coordinator frees it |
| `ref_in_booking` | ci | violation of `Settles`: after a restart, the order goes to the reference in the booking, which is gone, again and again |
| `reg_nofence` | ci | violation of `EffectBooked`: an order of the old term takes effect after the registration with the new term. The registration does not name the role, and the new holder frees it |
| `nack_ignored` | ci | violation of `Quiet`: a deposed coordinator sends its refused order again for ever |
| `gone_ignored` | ci | violation of `Quiet`: a coordinator whose view names a dead incarnation orders it for ever |
| `safety_full` | long, 30 minutes | pass: the faults of `safety` and one copy, with three terms |
| `live_full` | long, 30 minutes | pass: `live` with three terms |
| `safety_two` | long, 30 minutes | pass: two machines and two requests |

What the model found:

- **A registration raises the fence in the same write** (`reg_nofence`). Without it, an order of
  the old term that comes after the registration takes effect, and the new holder frees a role
  that is hosted.
- **A deposed coordinator must stop on a nack** (`nack_ignored`), **and a `gone` must ask the
  directory of the machine** (`gone_ignored`). An order to a dead incarnation never meets the
  fence of the machine, so the nack alone does not stop it.
- **"The newest incarnation wins in a view" is not a rule.** A registration of an older
  incarnation can replace a newer one with no harm, once a `gone` asks the directory: the order to
  the dead incarnation comes back, and the answer of the directory names the live one. With that
  rule off, `safety_dup` and `live` pass.

What is not in the model: the election itself (`election/` at the git tag `journal-design-a`), Paxos inside the control journal,
the placement of a role on another machine (`placement-port/` (at the git tag `journal-design-a`)), a machine that loses its disk (a
new machine), a machine that reads the term of the journal late (it learns each term at once
here), the ack of a registration (a registration is never lost), and the birth nonce of the key
that `Assign` carries (#1419 (the tenancy wrapper and paros-names), `journal-ids/`). The model
does not check a machine reference that survives a restart: the reference of moonpool RPC changes
at each incarnation.

### `segment-roll/`: the `Roll` cut of a segment, for step 1 of the segments in the core

The plan is `docs/archive/journal-design-a/prompt.md` (Pierre, 2026-11-24): segments come down
into `paros-consensus` as an optional behaviour, and a segment ends at a decided command, the
`Roll` (rules R2 to R5 of the prompt). The model is one Multi-Paxos log on one fixed set of three
acceptors with majorities. A value is `0` (a `Noop`), `1` (a client command) or `k >= 2` (a
`Roll` to segment `k`). A `Roll` to `k` is valid at the first slot of segment `k - 1` that holds
one; it ends segment `k - 1` at its slot plus `Alpha`, and any other `Roll` changes nothing. Each
segment has its own replicas (two in segment 1, so that two of them can disagree on an end). A
replica applies the chosen prefix of its segment, a `Roll` sets its end, and a replica with an
end writes the end and the next start into the chain (the add-only map of R5, here one map).

Each proposal carries the segment that its leader believes for the slot (`k`), and the `Commit`
of the slot goes to the replicas of that segment (`Sent`). The leader takes its belief from its
chosen prefix and from its own proposals. It can be wrong: a later ballot can choose, below the
slot, the `Roll` of an older ballot that the leader's Phase 1 did not see. TLC found this with
three ballots, against the prediction of the prompt (the stop point of step 1): ballot 1 votes a
`Roll` at slot 1 at one acceptor; ballot 2 does not see it, proposes commands at slots 1 and 2,
and slot 2 is chosen and routed to segment 1; ballot 3 sees the vote of ballot 1 and chooses the
`Roll` at slot 1, so slot 2 is in segment 2. No replica applies the wrong value (R3 holds), but a
`Commit` goes to the wrong replicas.

**The rule, `Route = "commit"`**: a leader sends the `Commit` of slot `s` only when it knows that
every slot below `s + 1 - Alpha` is chosen (R3 for the leader; with `Alpha = 1`, every slot below
`s`, and every check of the rule uses `Alpha = 1`). Its belief is then a fold of a chosen prefix,
so it is the true segment by construction; the checks confirm it. The leader still pipelines its
proposals; only the `Commit` waits.
Two other candidates were compared (Pierre, 2026-11-24): the window of Paxos Made Simple §3
(`Window`, a leader proposes at `s` only once the slots below `s + 1 - Alpha` are chosen; the
`Roll` takes effect `Alpha` slots later), and a heal (a replica that holds its end answers
"sealed" to a stray `Commit`, and the leader sends it again to the next segment). The window
passes with `Alpha = 1`, which is no pipeline at all, and the check with `Alpha = 2` was too large
to finish; the heal passed `Agreement`, `OneEnd`, `ReplicaTrue` and `ChainTrue` with 3 ballots
(3.1 million states, on an earlier version of this model with a `Deliver` switch, not kept in the
tree; the run is in PR #1540 (segment-roll)), but it adds a message and a repair. The rule at the
`Commit` adds neither.

**What the rule makes unneeded.** With `Route = "commit"`, the pause (R4a, `Barrier`) and the
`Noop` of a hole (R4b, `HoleFill`) are no longer needed for routing, because the route is a fold
of the chosen prefix whatever a leader proposes: `barrier_off_commit`, `hole_any_commit` and
`barrier_off_full` pass, as the definition says they must. The tail segment of a new leader is
still needed (R4c): it is the start of the fold, and `route_commit_base_chain` strays when it
comes from the chain. R3 alone keeps `OneEnd`, `ReplicaTrue` and `ChainTrue` with every routing
rule off (`safety_bare`). They stay as switches, and the controls
`barrier_off` and `hole_any` show what they do under `Route = "propose"`. Pierre dropped the pause
(R4a) on 2026-11-25 (`docs/architecture/04-7-segments.md` §4.7.3). R4b is the gap fill of the core today, so it
stays.

**The handoff** (DPaxos `relinquish_to`, Pierre 2026-11-24): the successor keeps the ballot and
runs no Phase 1, so it recovers nothing. With `Route = "commit"`, the route is a fold of the
chosen prefix that the leader knows, so a successor that learns the chosen slots of its ballot
routes right without the pending `Roll`. A first control, where the successor forgets the pending
`Roll` but keeps the knowledge of the chosen slots, failed only because the model kept the tail
segment in a variable apart from that knowledge (found in the review of PR #1540): it tested the
model, not the handoff, and it was removed. The handoff needs no step of its own here: the
successor is the minter under another name.

| Constant | Values (first: the design that passes) |
|---|---|
| `Route` | `"commit"`: the `Commit` waits for the chosen prefix below its slot. `"propose"`: it leaves when its slot is chosen at the ballot. |
| `Window`, `Alpha` | `FALSE`: a leader proposes at any slot. `TRUE`: only inside the window of `Alpha` slots. `Alpha` is 1 but in `window_off_alpha2`. |
| `ActOn` | `"chosen"`: a replica applies the chosen value at its next slot (R3). `"vote"`: any voted value. |
| `Base` | `"prefix"`: a new leader takes its tail segment from its chosen prefix and its Phase 1 (R4c). `"chain"`: from the chain alone. |
| `Barrier` | `TRUE`: a leader proposes nothing above its own pending `Roll` until it is chosen at its ballot (R4a). `FALSE`. |
| `HoleFill` | `"noop"`: a hole below the frontier gets a `Noop` (R4b). `"any"`: any value. |

Invariants: `Agreement`; `OneEnd` (two replicas of one segment never hold two ends);
`ReplicaTrue` (a replica applies only chosen slots of its own segment, and its start and end are
the true ones); `ChainTrue` (the chain says what the log decided); `NoStray` (a `Commit` goes to
the replicas of the true segment of its slot); `RouteKnown` (under `Route = "commit"` the true
segment exists when the `Commit` leaves). Action properties, checked by `safety_small`, `safety_bare` and the
long passes: `ReplicaForward` (a replica keeps its start and never goes back) and `ChainAddOnly`
(a start or an end of the chain, once written, never changes: the write is a merge, R5).

| Check | CI | Result |
|---|---|---|
| `safety_small` | ci | pass: every invariant, every rule on, 2 slots, 2 ballots, 2 replicas in segment 1 (65,096 distinct states) |
| `safety_bare` | ci | pass: `Agreement`, `OneEnd`, `ReplicaTrue` and `ChainTrue` with R3 alone: no pause, any value in a hole, the tail from the chain, `Route = "propose"` (112,233) |
| `route_commit` | ci | pass: `NoStray` and `RouteKnown` with 3 ballots, the case that TLC found (1.0 million, about 2 minutes with 4 workers) |
| `barrier_off_commit`, `hole_any_commit` | ci | pass: `NoStray` with `Route = "commit"` and no pause, or any value in a hole, 3 slots (0.73 and 0.72 million) |
| `route_propose_nb2` | ci | pass: with two ballots the belief at the proposal is right (0.46 million) |
| `window_alpha1` | ci | pass: `Route = "propose"` with the window of one slot (0.23 million) |
| `route_propose` | ci | violation of `NoStray`: the case of three ballots above |
| `route_commit_base_chain` | ci | violation of `NoStray`: a new leader takes its tail segment from the chain, which is behind the log by one `Roll` (R4c) |
| `act_vote` | ci | violation of `OneEnd`: two replicas of segment 1 apply two voted `Roll`s at two slots (R3) |
| `barrier_off`, `hole_any`, `base_chain` | ci | violation of `NoStray` under `Route = "propose"`: without the pause, with a command in a hole, with the tail segment from the chain |
| `reach_two_rolls`, `reach_roll_again`, `reach_roll_lost`, `reach_seg2_apply`, `reach_route_seg2`, `reach_route_stale` | ci | violation: two segments end; a later ballot chooses again a `Roll` that an earlier ballot chose; a voted `Roll` loses to a `Noop`; a replica of segment 2 applies a slot; a `Commit` goes to segment 2; a belief at a proposal is stale, and under `Route = "commit"` no `Commit` carries it |
| `safety_nb2` | long | pass: every invariant, 3 slots, 2 ballots (5.1 million, 23 minutes) |
| `barrier_off_full` | long | pass: the same with no pause (7.4 million, 36 minutes) |
| `safety` | long | pass: every invariant, 2 slots, 3 ballots (9.4 million, 43 minutes) |
| `window_off_alpha2` | long | violation of `NoStray`: with `Alpha = 2` and no window, the stray of three ballots comes back |

What is not in the model: a change of the acceptors (a grid change is a round change and cuts no
segment, R9), a restart or a lost disk, the chain at a matchmaker quorum (step 4), `Held`, the
floor and the heal of a segment (steps 6 and 7), and the replicas that a `Roll` names (the sets are
fixed).

### `multi-paxos-roll/`: the `Roll` on the Multi-Paxos model, for step 3 of the segments in the core

Step 3 of `docs/archive/journal-design-a/prompt.md` puts the `Roll` on the model of plain
Multi-Paxos before the code. `MultiPaxosRoll.tla` extends `MultiPaxos.tla` (a link to
`multi-paxos/`) and changes no action of it: a `Roll` is a command, so Phase 1 recovers it, the
gap fill gives a hole a `Noop` and never a `Roll` (R4b), and a learner learns it as any other value.
The spec is `Spec` of `MultiPaxos`: there is no pause (R4a, dropped by Pierre on 2026-11-25), and
`MultiPaxos.tla`, pinned in `PROVED.sha256`, does not change. The constant `RollTo` gives each
command its target: 0 for a client command, `k >= 2` for a `Roll` to segment `k`.

A node folds its segments from the prefix that it learned (R3, `SegFold`): a `Roll` to its
segment plus one ends the segment at the next slot, and any other `Roll` changes nothing (R2).
`SegsAgree` says that two nodes that learned a prefix fold the same segment at each slot of it,
so each segment has one end. `Inv`, the inductive invariant of `multi-paxos/` that TLAPS proves
for any set of commands, is checked on every reachable state too: the `Roll` changes no
argument of the proof. The control `act_vote` folds the votes in place of the learned prefix
(`VoteSegsAgree`), and two nodes fold two segments.

| Check | CI | Result |
|---|---|---|
| `roll` | ci | pass: `TypeOK`, `Agreement`, `LearnedChosen`, `Inv` and `SegsAgree`; one client command and one `Roll` to segment 2, 2 slots, 2 ballots (2.27 million distinct states, about 5 minutes with 4 workers) |
| `act_vote` | ci | violation of `VoteSegsAgree`: a fold of the votes, 2 ballots (R3) |
| `reach_seg2`, `reach_seg2_both`, `reach_seg3`, `reach_recovered`, `reach_lost` | ci | violation: a node folds segment 2; two nodes fold segment 2; a node folds segment 3 (two `Roll`s, as `segs3`); a later ballot proposes again, below its frontier (so from its Phase 1, never from `Allocate`), a `Roll` that an earlier ballot proposed, and it is learned; a proposed `Roll` is lost, and its slot is learned as another value |
| `segs3` | long | pass: `TypeOK`, `Agreement` and `SegsAgree` with two `Roll`s, to segments 2 and 3, and one client command (6.2 million, 13 minutes with 4 workers) |
| `roll_restart` | long | pass: the invariants of `roll` with one restart: the node goes back to its disk and keeps a prefix of what it learned (11.1 million, 25 minutes with 4 workers) |

Three ballots are not checked here: with one `Roll` alone, TLC passed 87 million distinct
states in two hours with 34 million on the queue, and it was stopped. The argument for every
number of ballots is the proof, not a bounded check. `Spec` and `Inv` are the text of
`MultiPaxos.tla`, and `RollTo` adds only an `ASSUME` about a new constant, so the TLAPS theorems
of `multi-paxos/` apply as written under their hypothesis `Hyp` (`InvApplies`, `Pick =
"highest"`, `Alloc = "frontier"`, `QuorumFacts`; every configuration of this folder meets it):
`Inv` holds in every reachable state and implies `Agreement` (`InvAgreement`). `LearnedChosen`
follows from the `learned` clause of `NodeSlotOK` in `Inv` (not a theorem of its own), and
`SegsAgree` follows from `Agreement` and `LearnedChosen`: two nodes that learned a prefix
learned the same value at each slot of it, so they fold the same segments.

What is not in this model: the replicas of a segment and the route of a `Commit` (they are in
`segment-roll/`), the chain, and a grid.

### `held-floor/`: `Held` and the floor of a held segment, for step 6 of the segments in the core

R7 of `docs/architecture/04-7-segments.md`: a segment is `Held` when `F + 1` of its replicas
hold every slot of it and its end is in the chain. The compaction floor of an acceptor may then
rise to the end of that segment: to an acceptor, a held segment is a `Truncate` whose records are
kept elsewhere. This is departure 2 of §4.7.4 (Scenario 3 of Matchmaker Paxos §3.5 and §4.5, the
chosen prefix on `f + 1` replicas, used for the compaction floor), and this folder is its check.

`HeldFloor.tla` extends `Truncation.tla` (a link to `truncation/`) and changes none of its
steps: the floor guard of Phase 1, the `Accept` below the floor, the catch-up, the trim-point jump
(`TrimTo`) and the retry of a refused candidate are the ones of `truncation/`. They read the floor
and not its reason, so the jump of a laggard below a held floor is the jump below a truncation.
`truncation/` has one new constant, `Rolls`: control values that weigh nothing, `{}` in every
check of that folder. The module adds:

- **The segments of the chosen log.** A `Roll` to `k + 1` at slot `s` ends segment `k` at
  `s + 1` when `k` is the tail (R2). The segment of a slot is a fold of the chosen prefix (R3).
- **The replicas.** Each segment has its replicas (`RepOf`), nodes that are not acceptors. A
  replica copies the chosen slots of its segments from a node that learned them or from a peer
  replica, all that it can in one step. Up to `F` replicas lose their disk.
- **The chain.** A node that applied the `Roll` publishes the end (R5). Every writer writes the
  same value, so the model writes the end of the chosen log; `segment-roll/` checks that a node
  publishes only an end that it applied.
- **`Held`** (the tally of `collector.rs`): the leader marks segment `k` when `F + 1` of its
  replicas hold every slot of it below its end, and the end is in the chain. The model reads
  what the replicas hold now; the code counts their reports, which can be late. A late report
  says less than the replica holds, and a replica loses its slots only by a lost disk, at most
  `F` in the run, so the count of `F + 1` holds for the reports too (an argument, not a check).
- **The held floor** (`acceptor/retention.rs`): an acceptor raises its floor to the end of
  segment `k` when every segment up to `k` is held, and drops the records below it in one write.
  It needs to have learned nothing. Only the acceptors of `HeldFloorNodes` do it; in the checks
  that must see a jump, the others reach the floor only with `TrimTo`.

| Constant | Values (first: the rule) |
|---|---|
| `HeldAt` | `"f_plus_one"`: `Held` needs `F + 1` replicas. `"f"`: a control, `F` replicas. |
| `HeldEnd` | `"chain"`: `Held` and the floor follow the end that the chain publishes. `"log"`: a control, the end of the chosen log, so a floor can pass an end that is not published. |
| `FloorTo` | `"prefix"`: the floor rises to the end of segment `k` only when every segment up to `k` is held. `"any"`: a control, to the end of any held segment, over a segment before it that is not held. |
| `F` | The replicas that may lose their disk. |
| `HeldFloorNodes` | The acceptors that raise their floor at `Held`. |

The checks have one command `c1` and one `Roll` to segment 2, three acceptors and two slots, and a
state constraint (`RollPlan`) makes a leader allocate `c1` at slot 0 and the `Roll` at slot 1; the
gap fill still writes a `Noop` at a hole. The last segment has no replica, so it is never held.
`floor_prefix` and `floor_any` have three slots and two `Roll`s (`RollPlan3`): `c1`, `roll2`,
then `roll3`, so segment 2 is the slot of `roll3`, and it keeps the replica of segment 1 (D4).

The invariants are those of `truncation/`, with two that know the held segments:
`FloorTruncatedOrHeld` (each slot below a floor is at or below a chosen truncation, or in a held
segment whose end the chain publishes, and in that segment by the fold of the chosen log) and
`FloorReleasedOrHeld` (`FloorReleased` with the held segments). The chain is how a reader finds
the replicas of a slot that the acceptors dropped, so `FloorTruncatedOrHeld` is the rule "the
floor follows the chain" written as a state: `held_log` breaks it by its definition, and
`floor_any` at a slot of a segment that is not held. New: `RecordKept` (each chosen command whose
record no truncation released is readable at an acceptor or held by a replica, after up to `F`
lost disks), `ChainTrue` (a published end is the end of the chosen log) and `ReplicaTrue` (a
replica holds only chosen slots of its own segments).

| Check | CI | Result |
|---|---|---|
| `reach_held`, `reach_held_drop`, `reach_held_win`, `reach_held_loss` | ci | violation: a segment is held; every acceptor dropped it; a leader wins Phase 1 from at or above the held floor; a replica loses its disk after a segment is held |
| `reach_held_jump` | ci | violation of `NoHeldJump`: `a2`, which does not raise its floor at `Held`, jumps with `TrimTo` over a held slot that it never voted |
| `reach_laggard_win` | ci | violation of `NoLaggardWin`: `a1` leads ballot 1 and `a3` ballot 2; `a3` does not raise its floor at `Held`; its campaign asks from below the held floor, it jumps with `TrimTo`, and it wins Phase 1 from at or above the floor (1.6 million states) |
| `held_f` | ci | violation of `RecordKept`: `Held` at `F = 1` replica; the acceptors drop slot 0, and the one replica that holds it loses its disk (0.40 million states) |
| `held_log` | ci | violation of `FloorTruncatedOrHeld`: the floor rises to an end that the chain does not publish |
| `floor_any` | ci | violation of `FloorTruncatedOrHeld`: segment 2 is held and segment 1 is not; the floor rises to the end of segment 2 over slot 0 |
| `held` (in `held-floor-pass/`) | ci | pass: every invariant, two ballots of one leader, `a2` catches up and jumps, `F = 0` and one replica (2.4 million states, 10 minutes with 4 workers) |
| `held_loss` (in `held-floor-pass/`) | ci | pass: every invariant, `F = 1` with two replicas and one lost disk, one ballot (14 thousand) |
| `floor_prefix` | ci | pass: every invariant, two `Roll`s, one ballot (0.21 million) |
| `held_laggard` | long | `Ends` (`HeldLiveSpec`) with the constants of `reach_laggard_win`: the laggard is refused by the held floor, jumps and wins |
| `two_leaders` | long | pass: `held` with the owners `a1`, `a2` and `a3`, and `a1` and `a2` catch up |
| `one_leader_f1` | long | pass: `held` with `F = 1`, two replicas and a lost disk |
| `full` | long | pass: three owners, `F = 1`, two replicas and a lost disk |
| `three_slots` | long | pass: a command in segment 2 (`CmdPlan3`), so slot 2 is above the held floor while it rises |

`held-floor-pass/` links `HeldFloor.tla`, `MC.tla` and `Truncation.tla` and runs `held` and
`held_loss`, so that each of the two jobs stays inside its minutes. The guard mutation of
`HeldFloor.tla` runs the checks of both folders.

What is not in the model: the chain at a matchmaker quorum (one variable here; step 4), the
replicas that a `Roll` names (the sets are fixed, D4), the heal and the trim of a sealed segment
(step 7), a restart and a faulty record (`truncation/` checks them, and the floor reaches the disk
in the same write as the drop), and a truncation beside the held floor (the two floors are one
floor with two reasons; `truncation/` checks the fold). The CI checks have two slots, so the held
floor is `NS` there: no vote of segment 2 is in flight while it rises. `three_slots` has one.

### `held-floor-reconfig/`: a reconfiguration onto empty stores after a held segment was dropped

`HeldFloorReconfig.tla` extends `TruncationReconfig.tla` (a link to `truncation-reconfig/`) with the
steps of `HeldFloor.tla`, and changes none of its steps. The question: once the acceptors of `C1`
dropped a held segment, can a reconfiguration onto `C2`, whose member `a4` starts empty, lose a
slot or choose a second value? The new member holds nothing below the held floor: it jumps with
`TrimmedTo`, or never holds the slots. The GC of `node/gc.rs` counts a member toward the fence when
it holds each slot up to the fence, and a slot below its floor counts, so a member that raised its
floor to a held end counts. `truncation-reconfig/` has one new constant, `Rolls`, `{}` in every
check of that folder. No replica loses its disk here, and `Held` always needs `F + 1` replicas:
that count is the question of `held-floor/`.

The CI checks have one slot: segment 1 is the `Roll` alone at slot 0 (`RollPlan1`). `a4` alone
catches up and jumps, and segment 1 has one replica with `F = 0`. `RecordKept` has no command to
judge there, so the CI checks judge `SlotKept`: each chosen slot that no truncation covers is
recorded at an acceptor that is up or held by a replica. A `Roll` is a slot of its segment, and a
replica holds it, so `SlotKept` fails when the acceptors drop the slot before a replica holds it.
The check with a command at slot 0 and the `Roll` at slot 1 is long.

| Check | CI | Result |
|---|---|---|
| `reach_held_join` | ci | violation of `NoHeldJoin`: the GC forgets `C1` while `a4`, which does not raise its floor at `Held`, jumped with `TrimTo` over the held segment and holds no slot of it |
| `reach_held_win` | ci | violation of `NoHeldWin`: a ballot of `C2` wins Phase 1 from at or above the held floor |
| `held_log` | ci | violation of `FloorTruncatedOrHeld`: the floor passes an end that the chain does not publish |
| `held` | ci | pass: `Agreement`, `LearnedChosen`, `NoFillChosen`, `FloorChosen`, `RecordChosen`, `Recoverable`, `FloorTruncatedOrHeld`, `RecordKept`, `ChainTrue`, `SlotKept`, `ReplicaTrue` (0.61 million states, 1.5 minutes) |
| `held_wipe` | ci | pass: the same with one wipe of an acceptor (2.4 million states, 5.5 minutes) |
| `held_cmd` | long | pass: two slots, a command and the `Roll`, `F = 0` |

What is not in the model: a third configuration, the restart and the faulty record of
`truncation/`, and the chain at a matchmaker quorum.
