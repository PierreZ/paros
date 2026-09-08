# The matchmaker interaction, verified

Design note for the verification PR #134 cut short. #134 fixed about 45 review findings
on the matchmaker plane in 81 commits and proved the result the way every change is proven
here — the sweep saturated, the hunt clean, the handover model checker green — but a green
gate only says the harness found nothing; it does not say the harness *could* have. Eight
of #134's findings changed how the nodes and the matchmakers interact, and for each of them
this note asks the two questions that turn "green" into "verified": *what rule enforces it,
cited*, and *what would go red if the rule were removed*. It sits beside
`matchmaker-gc-and-generations.md`, which restates the protocol; this one restates the
evidence.

Method. Each concern is read against the code with citations (a citation is a file and the
line it held when this note was written; the symbol beside it outlives the line). Where a
claim can be judged by the sans-IO handover model checker
(`crates/paros-core/src/matchmaker/handover_model.rs`) it is written there as an assertion
judged after every step, and then shown to *bite*: a mutation of the rule it names must go
red on the default seeds, and the line that goes red is recorded here. A concern the model
cannot reach (it has no acceptors and no reboot of the *node's* volatile state) is verified
by reading alone and says so. Two of the eight turned up protocol bugs; both are fixed in
the commit that precedes the model claims (`fix(core): a refusal of a dead decree moves
nothing, and a page at a raised floor is taken`) and both fixes are the red→green witnesses
the corresponding claim carries.

The model's claims are numbered 1–9. Claims 1–3 are #125's (one authoritative set per
generation, a chosen successor is a majority's durable vote, an activated registry carries
the complete reconstruction). Claims 4–9 are this note's, and the table at the end maps each
to the mutation that proves it load-bearing.

## 1. Retirement after a reboot

**The concern.** `Retire` carries the effective GC watermark as evidence and the node honors
it only above `last_member_ballot`, the highest ballot a configuration naming it was bound
to. That scalar is volatile. Does a reboot open a window in which a node that is still a
member accepts its own retirement?

**What enforces it.** `ColocatedNode::may_retire` (`crates/paros-core/src/node/gc.rs:254`)
requires four things: matchmakers, not an acceptor of the configuration the node believes
in force, not the leader, and `watermark > last_member_ballot`. `last_member_ballot` is
raised by `record_membership` (`crates/paros-core/src/node/helpers.rs:69`) from every
assignment to `acceptors_since` — `learn_config`, `try_become_leader`, a handoff install
(`node/handoff.rs:558`) and the adoption of an effective configuration
(`node/matchmaking.rs:417`) — and the invariant checker pins that a member's fence is at
least the ballot its configuration is bound to (`node/invariants.rs:114`). It boots at
`Ballot::zero()` (`crates/paros-core/src/node/boot.rs:102`), beside the volatile `acceptors`
that regresses to the bootstrap configuration.

**Reading.** The window exists in the code and is exactly this: a node pulled in by a
reconfiguration at ballot `b_join`, rebooted before it retires, believes the bootstrap
configuration (which need not name it) and remembers no membership. Condition 2 passes
because the belief does not name it; condition 4 passes for any watermark above zero. An
operator who reads a watermark `w` from a leader's `Inspect` and sends it will be honored
even when `w ≤ b_join` — a value the honest node would have refused as `not_collected`.
The window closes on the first `Prepare`, `Heartbeat` or `Relinquish` that names the node
(`learn_config` runs `record_membership`), so it is one heartbeat interval wide on a live
cluster, and it needs an operator who acts on a watermark read *before* the node's join
became effective.

**Verification.** By reading and by the harness's oracle, not by a model claim: the model
has no node reboot of `ColocatedNode` state. The harness aims one retirement in ten at a
*member* rather than at a retirable acceptor (`chain_workload.rs:2103`, the
`aim_at_member` coin, with the watermark it just read), and the audit's "gc: a node retires
only after an effective floor named it retirable" (`crates/paros-sim/src/audit/matchmaker.rs:1862`)
is an always-assertion on the retired node against the set the effective floor released.
Every sweep and hunt since #134 has been green under that coin, including this branch's.

**Verdict.** Unproven as a bug, and the doctrine says what that means: no speculative
defensive code. The fix, if the window is ever reached, has one honest shape — make
`last_member_ballot` a durable `HardState` scalar with its write op, storage record and
boot read-back — and that is a protocol change with a storage surface, not a patch. What is
recorded here is the exact window and what closes it, so a red witness has a diagnosis
waiting. Nothing changed.

## 2. Registry paging as protocol

**The concern.** A matchmaker's history answer is paged (`REGISTRY_PAGE = 64`,
`crates/paros-core/src/matchmaker.rs:158`) and the candidate re-asks from a cursor. Paging
is protocol, not transport: a page lost, duplicated, reordered, or answered from a registry
that moved between two pages must never let a campaign close with a hole in `H_b`, and must
never wedge it.

**What enforces it.** `Matchmaker::page` (`matchmaker.rs:562`) answers at most
`REGISTRY_PAGE` records from `max(cursor, watermark)` and names the next cursor only when
the page was full (`matchmaker.rs:439–443`). The candidate's `Matchmaking::accepts`
(`crates/paros-core/src/matchmaking.rs:238`) folds a page only from a matchmaker it asked,
at the cursor it owes (`page_next`, `matchmaking.rs:150`), and the fold maxes every reply's
watermark into the closing one, above which the union is filtered at closure.

**What was wrong.** A page's start is `max(cursor, watermark)`. When a leader's GC raised
the matchmaker's watermark *over the cursor* between two pages of one answer, the next page
started above the cursor the candidate asked for, `accepts` refused it as the wrong page,
the candidate re-asked the collected cursor on every election timeout, the matchmaker
answered from its floor every time, and — `ColocatedNode::tick` never abandons a pending
matchmaking, by doctrine — the campaign was wedged at that matchmaker for good. A candidate
that never leads: a liveness bug, not a safety one, but permanent.

**The fix.** A page above the cursor is taken when, and only when, it starts at the
sender's own reported watermark (`matchmaking.rs:257–260`, `at_cursor ||
cursor_collected`). Everything it skipped sits below a floor the fold maxes into the closing
watermark, so the union filtered at closure loses nothing it would have kept. Two node
tests pin the page shape and the end-to-end campaign against a real registry
(`node/tests/matchmaking.rs`: `a_page_that_starts_at_a_raised_watermark_above_the_cursor_is_taken`,
`a_floor_raised_over_the_cursor_between_two_pages_still_completes_the_campaign`).

**Verification.** Model claim 4, two halves. Completeness: at closure `H_b` holds every
registration below `b`, at or above the maximum reported watermark, that a majority of
*any* generation up to the addressed one durably holds — pages, cursors, GC raises and
handovers included (`handover_model.rs:1331`, "a completed matchmaking is complete"). Every
node in the model runs the real `Matchmaking` tally against the set it believes
authoritative, re-asked on the beat and never abandoned by the clock, as `ColocatedNode`
does; the tail raises GC floors between pages, drawn from the upper half of the round space
so a floor actually rises over one in force. Liveness: after quiescence every campaign
closed (`settle_campaigns`, `handover_model.rs:1849`), and a page that jumps the cursor at
the sender's raised watermark must fold (`handover_model.rs:1271`).

**Witness.** Reverting the fix (refuse the jumped page again) is red at
`handover_model.rs:1271` on the default seeds. The `campaign_paged` and
`campaign_page_jumped` reach counters are asserted non-zero, so the claim is not vacuous.

## 3. The effective configuration against GC and against a handover

**The concern.** The effective configuration — the highest-ballot reconfiguration
registration a matchmaker quorum holds — is what `MatchStep::StaleConfiguration` fires on,
so a node that missed a completed reconfiguration is never elected under the superseded
one. #134's P1 found that GC pruned its record. Two things must now hold: GC never lowers
it, and a handover carries it into the successor generation.

**What enforces it.** `MatchmakerHardState::effective` is a durable monotone scalar
(`crates/paros-core/src/matchmaker/state.rs:112–121`), folded as a max on every
registration and carried in every `Registered` reply; `advance_gc_watermark`
(`matchmaker.rs:530`) moves the window's floor and leaves the scalar alone; the freeze reply
carries it (`ReconfigureReply::Stopped { effective }`, `matchmaker/message.rs:316`) and the
reconstruction takes the maximum over the frozen quorum into the pending bootstrap
(`matchmaker/state.rs:66–72`).

**Verification.** Model claim 5, two halves. At every campaign's closure the tally's
effective configuration is at least the highest reconfiguration a majority durably
registered below `b`, whether or not its record was collected or the generation that took
it was replaced (`handover_model.rs:1352`, "a completed matchmaking learns the effective
configuration"); and on every disk the scalar is monotone across every write, the GC
watermark's included (`handover_model.rs:1003`, judged at every persist). The
`effective_outlived_its_record` counter proves the collected case is reached.

**Witness.** Making `advance_gc_watermark` clear the scalar (the DPaxos-shaped mistake of
P1, re-introduced) is red at `handover_model.rs:1352` and at `:1003` on the default seeds.

## 4. Phase-specific quorum semantics

**The concern.** #134's C3 split the quorum predicate by phase so a flexible or grid system
is an arm, not a rewrite. Paxos safety needs every Phase-1 quorum to intersect every
Phase-2 quorum, not each phase's quorums to intersect each other, so the split is only safe
if every tally is tagged with the phase whose claim it makes. Is any site tagged wrong?

**The rule** (AGENTS.md, *The core is composable*): Phase 1 wherever a tally concludes what
an *earlier* ballot could have chosen; Phase 2 wherever it claims no *later* ballot decided
behind it. Matchmaker quorums are majorities only (`MatchmakerSet::has_quorum`), and the
decree builds a `QuorumSystem::Majority` over the set it replaces.

**Reading, every call site.**

| site | predicate | claim it makes | tag right? |
| --- | --- | --- | --- |
| `proposer/election.rs:80`, `proposer.rs:239` | Phase 1, per configuration in `H_b` | what an earlier ballot could have chosen (P2c) | yes |
| `proposer/rounds.rs:191` (`decided`) | Phase 2, in the round's column | no later ballot decided behind this accept set | yes |
| `proposer/authority.rs:100`, `:183` (CheckQuorum, the read fence) | Phase 2 | the leadership is still unrefuted | yes |
| `collector.rs:176` (the GC licence) | Phase 2 of `C_b` | a Phase-2 quorum knows the chosen prefix | yes — and *not* Phase 1: a Phase-1 quorum reporting a chosen index proves nothing about a later ballot |
| `driver/mod.rs:970`, `snap_repair.rs:323` (snapshot custody) | Phase 2 | a decided point is held by a set every later Phase 1 intersects | yes |
| `collector.rs:220`, `matchmaking.rs:347` (GC acks, the matchmaking tally) | matchmaker majority | a matchmaker quorum durably holds it | yes |
| `reconfigurer.rs:438`, `:475`, `:893` (freeze, close, publication) | matchmaker majority of `M_g` (`:893`: of the successor) | frozen / reconstructed / activated at a quorum | yes |
| `matchmaker/decree.rs` | the shared `Proposer` over `Majority(M_g)` | the ordinary Paxos claims at slot zero | yes |

No site compares a count against a threshold (`quorum_size` survives only in the
"how many acks are still missing" reports), and no site asks the untagged predicate, which
no longer exists.

**Verification.** Reading, and the two negative tests the membership boundary pins
(`membership.rs:832`, `flexible_predicates_differ_by_phase`, and the grid's row-that-is-not-a-column
cases beside it). Under the swarm, #140's and #141's seeds run every site above under
a system where the two predicates *differ*, and the audit's flexible gates read through
the same boundary. The model checker cannot add to this: it has no acceptors.

**Verdict.** Verified by reading. Nothing changed.

## 5. Counted progress versus duplicates

**The concern.** #134's P4 made the reconfigurer report a fold that changed nothing as
`Ignored` so the driver's stall clock measures progress, not traffic: a phase that makes no
progress is abandoned after `reconfigure_timeout_elections`, and a duplicate ack must not
keep a dead phase alive. Is every reply that moves nothing `Ignored`, and does `Ignored`
really move nothing?

**What enforces it.** `MatchmakerReconfigurer::on_reply` (`reconfigurer.rs:648`) returns
`Ignored` from every arm whose fold changed no tally (`:679–781`), and the driver resets the
stall clock on anything else (`:650`, `stalled_for` at `:412`).

**What was wrong.** A preempted decree is dead until the driver's next re-send reopens it
above the refusing promise, but a further `Nacked` — the same refusal duplicated, or another
member's — answered `Preempted` again, and the driver reset the clock. A dueling finisher's
refusals kept a dead decree alive past the abandon budget: a stall the budget was written to
end, that it could not.

**The fix.** A second refusal of a preempted decree is `Ignored`; the promise it carries
still raises the floor the reopen clears (`Decree::on_nack`, `decree.rs:280`, keeps the
*maximum* refusal, not the first). The unit test
`a_second_refusal_of_a_preempted_decree_is_ignored_and_raises_the_floor` pins the clock and
the floor.

**Verification.** Model claim 6, judged on every reply against a `PhaseShape` snapshot of
the running phase taken before and after the fold (`handover_model.rs:1071`): an `Ignored`
reply left the shape unchanged and the stall clock untouched; a counted reply moved the
shape and restarted the clock (`:1083–1100`). The `reply_ignored` counter is asserted
non-zero.

**Witness.** Reverting the fix is red at `handover_model.rs:1095` on the first default seed
and in the directed `two_finishers_with_different_stop_quorums_choose_one_successor` case.

## 6. Freeze closure with two finishers

**The concern.** #134's P5 moved the close of the freeze from "the quorum's arrival" to the
driver's beat, so a finish no longer ratchets the matchmaker set down to whichever stop
quorum answered first. Two finishers meeting the same frozen generation must still choose
one successor, and a finish must propose the liveness it can vouch for — every member that
answered its freeze, never fewer.

**What enforces it.** `stop_quorum_reached` (`reconfigurer.rs:435`) only *reports*;
`close_stop` (`:464`) is called from the driver's beat (`driver/handover.rs:97`) and proposes
the operator's target or, for a finish, every member that answered (`:511`). Two finishers
are then two proposers of one decree over `M_g`, and the decree opens strictly above the
maximum promise the stop quorum reported (`Stopped.decree_promised`), so the loser adopts
the winner's vote (claim 2).

**Verification.** Model claim 7: a `Stopped` ack leaves the freeze open (`:1111`), a close
happens only once the quorum answered, and the closed proposal is exactly the target or the
answerers (`handover_model.rs:1576`); plus the directed cases
`a_straggler_that_answers_before_the_close_widens_the_finish` (`:2126`) and
`two_finishers_with_different_stop_quorums_choose_one_successor`.

**Witness.** Proposing only a quorum-sized subset of the answerers (the pre-#134 ratchet)
is red at `handover_model.rs:1576` on the default seeds and at `:2126` in the straggler
case.

## 7. Learned generation and publication

**The concern.** #134's P7 made a publication count a learner by the generation it
*holds*, and #137 made a re-sent `Chosen` idempotent at a member that already activated
the successor. Is a handover finished only once a majority of the successor is durably at
its generation, and is a lost `Learned` recovered by the re-send rather than aborting the
publication as superseded?

**What enforces it.** `Matchmaker::on_chosen` (`generation.rs:284`) answers `Learned {
activated: false, at: current.generation }` when the successor it is told is exactly the one
it already activated (`:290–306`), writing nothing; the reconfigurer counts a `Learned` by
`at` toward a majority of the successor (`reconfigurer.rs:873–893`) and refuses a `Chosen`
that contradicts the recorded successor.

**Verification.** Model claim 8: a publication is `Done` only when a majority of the
successor's disks are durably at `g + 1` (`handover_model.rs:1128`), and a `Chosen` re-sent
to a member that already activated it is answered `Learned { activated: false, at: g + 1 }`
and writes nothing (`:848`). The `chosen_resent_to_activated` counter is asserted non-zero.

**Witness.** Answering the re-sent `Chosen` with a refusal (the pre-#137 behaviour) is red
at `handover_model.rs:848` on the default seeds and in the two-finisher case.

## 8. The new failure seams

**The concern.** #134 and #133 added durability seams on the matchmaker plane
(`Seam::MatchBeforeSync`, `Seam::MatchAfterSyncBeforeReply`,
`crates/paros/src/hooks.rs:67–75`, consulted at `crates/paros/src/matchmaker/mod.rs:159`)
and a torn-batch crash. Every reply a matchmaker sends is a promise about its disk: the
freeze, the pending bootstrap, the decree promise and vote, the activation, the
registration. Is each one durable at the seam the reply leaves through?

**What enforces it.** The matchmaker driver's drain is persist → fsync → reply, structural
(`run_matchmaker`), and `Matchmaker::on_stop` freezes durably before the `Stopped` leaves
(`generation.rs:78–100`, the `freeze()` at `:92`). The model reproduces the three seams (before persist, after
persist before reply, after reply) and the torn prefix (`handover_model.rs:311–316`, `:918–925`).

**Verification.** Model claim 9, `assert_reply_backed` (`handover_model.rs:973`): every
reply the model delivers is checked against the disk of the matchmaker that sent it — a
`Stopped` leaves a durably frozen generation (`:1020`), a `Bootstrapped` a durable pending
set, a `Promised`/`Voted` the decree scalars, a `Learned` the activation, a `Registered` the
record — at the instant it leaves, under every crash seam.

**Witness.** Answering `Stopped` without freezing first is red at `handover_model.rs:1020`
in four of the directed cases on the first seed.

## The mutation table

Every claim below is green on `HANDOVER_MODEL_SEEDS=2000` and red under its mutation on the
default 400 seeds (or the named directed case). A mutation is applied by hand, run, and
reverted; none is kept, per the doctrine that a witness is evidence, not an artifact.

| concern | claim | mutation | red at |
| --- | --- | --- | --- |
| 2 paging | 4 | refuse a page that starts above the cursor at the sender's raised watermark (`matchmaking.rs:257`) | `handover_model.rs:1271` |
| 3 effective | 5 | `advance_gc_watermark` clears `effective` (`matchmaker.rs:530`) | `:1352`, `:1003` |
| 5 duplicates | 6 | a second `Nacked` on a preempted decree answers `Preempted` (`reconfigurer.rs:on_reply`) | `:1095`, `two_finishers…` |
| 6 freeze | 7 | a finish proposes only a quorum-sized subset of the answerers (`reconfigurer.rs:511`) | `:1576`, `:2126` |
| 7 publication | 8 | a re-sent `Chosen` at an activated member is refused (`generation.rs:290`) | `:848` |
| 8 seams | 9 | `on_stop` answers `Stopped` without `freeze()` (`generation.rs:92`) | `:1020` |

Concerns 1 and 4 are verified by reading and by the harness, not by a model claim; §1
records the one open window and the shape of its fix.

## What this note does not claim

- The model has no acceptors and no `ColocatedNode`: the leader-side GC preconditions, the
  cross-configuration Phase 1 and the retirement window of §1 are the sweep's and the
  hunt's to reach, and remain so.
- The matchmaker's registry still has no format marker (#147's parity item): a matchmaker
  that lost its disk is parked by the harness, never rebooted amnesiac, so claim 9 says
  nothing about a wiped matchmaker. Tracked in #69's on-deck list.
- A model claim is judged at the model's granularity — one step, one seed schedule — and a
  claim's mutation going red on 400 seeds says the claim is load-bearing there, not that
  the model reaches every interleaving the swarm can. The reach counters make each claim
  non-vacuous; they do not make it complete.
