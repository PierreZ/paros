+++
title = "Matchmaker generations and the handover decree"
weight = 15
+++

The matchmakers hold the true membership, so they must be replaceable too: a matchmaker loses
its disk and needs a successor. A tier of matchmakers for the matchmakers has no end. Instead,
each matchmaker set has a **generation**, and the members of generation `g` choose generation
`g + 1` with single-decree Paxos. The decree reuses the same proposer and acceptor that run the
log, over one slot.

{{ scenario(name="act4/matchmaker-generations") }}

## The failure

**A copy that moves.** A node copies the registries of the old matchmakers to the new ones
while the old ones still take registrations. A campaign at ballot 12 registers after the copy.
The new matchmakers never hold ballot 12. The next candidate asks them, its `H_b` misses the
set of ballot 12, and it can fill a slot that ballot 12 chose.

**Two successors.** Two operators replace the matchmakers at the same time, with different
targets. Each one bootstraps its target and publishes it. Generation `g + 1` now has two sets,
and two campaigns register with two registries that never meet. The handover model checker
finds this on its first seed when the publication skips the decree.

**An empty member.** A successor set activates while one of its members has not yet stored the
reconstruction. That member answers every campaign with an empty history, and a quorum that
includes it can miss an old set.

**A reused decree ballot.** A node starts a handover, gets a value accepted at decree ballot 4,
and restarts. Its new incarnation opens the decree at ballot 4 again, with another value. The
model checker found this on its seed 103: two values at one ballot.

**A frozen generation with nobody to finish it.** The node that froze generation `g` stops
before it decides a successor. Generation `g` registers nothing more. Without a rule, no
campaign completes, and a stable leader that does not campaign never notices.

## The rule

Every matchmaking message names its generation. A matchmaker serves only its own active
generation, and refuses every other one with what it knows. The handover has five steps:

1. **Stop.** Each member of `M_g` freezes, durably, before it answers. A frozen member
   registers nothing for `g` again. It stays up to vote in the decree and to point late
   proposers at the successor. Its answer carries its registry and its decree promise.
2. **Reconstruct.** From a stop quorum, take the maximum floor and the union of the histories
   above it, per journal.
3. **Bootstrap.** **Every** proposed member stores the reconstruction durably, as pending.
4. **Decide.** Single-decree Paxos over `M_g` chooses the successor. It opens strictly above
   every decree promise the stop quorum reported. If Phase 1 finds a vote for another
   proposal, it proposes that one.
5. **Publish.** `M_g` records the link to the successor, and `M_{g+1}` activates its pending
   bootstrap. A member that already activated answers a repeated `Chosen` again.

Three more rules:

- **Majority quorums only.** A matchmaker quorum is always a majority. The handover is proved
  for that model alone.
- **A decree keeps its `Nack`.** The log drops a refusing promise and lets a fresh election
  take over. A decree records the highest refusal, because its retry must open above it.
- **Anybody finishes.** A node that meets a frozen generation with no successor finishes the
  handover itself. A stable leader meets it in a refused GC ack, and finishes it too. A phase
  that makes no progress for a budget of election timeouts is abandoned and started again.

## Why the rule is enough

A frozen registry cannot change, so the copy is exact. Every completed registration of `g`
reached a majority of `M_g`, and it meets every stop quorum, so the reconstruction holds it.
The decree is plain Paxos over one slot, so it chooses one successor: the Phase 1 of a second
operator sees any successor that may be chosen, and proposes it. `Chosen` is sent only after a
Phase-2 quorum, so a published successor is always the chosen one. The reconfigurer keeps no
durable state. A crash loses only a proposal, and the next attempt adopts or completes it.

## The code map

| What | Where |
|---|---|
| The reconfigurer, the proposer side | `crates/lib/paros-core/src/matchmaker/reconfigurer.rs:305` `MatchmakerReconfigurer`, `:524` `start`, `:562` `finish` |
| Reconstruct from the stop quorum | `crates/lib/paros-core/src/matchmaker/reconfigurer.rs:688` `close_stop` |
| The matchmaker side, step by step | `crates/lib/paros-core/src/matchmaker/generation.rs:133` `on_stop`, `:173` `on_bootstrap`, `:321` `on_decree_prepare`, `:366` `on_decree_accept`, `:432` `on_chosen` |
| Activate the pending bootstrap | `crates/lib/paros-core/src/matchmaker/generation.rs:565` `activate` |
| The decree on the shared roles | `crates/lib/paros-core/src/decree.rs:82` `Decree`, `:348` `Decree::on_nack` |
| Majority quorums for matchmakers | `crates/lib/paros-core/src/membership/matchmaker_set.rs:162` `MatchmakerSet::has_quorum` |
| A stable leader meets a frozen generation | `crates/lib/paros-core/src/collector.rs:40` `GcStep::Frozen` |
| A node finishes a handover | `crates/lib/paros-roles/src/node/handover.rs:105` `HandoverDriver::finish` |

## The proof

**The handover model checker** (`crates/lib/paros-core/src/models/handover.rs`) runs the real
`Matchmaker` and `MatchmakerReconfigurer`, with a scheduler in place of the network and the
disks. Concurrent reconfigurers pick random targets. Each message can be dropped, duplicated or
reordered. Each matchmaker can crash at each durability seam, and each node can restart with its
bootstrap belief. After every step it asserts that at most one set is authoritative per
generation, that a chosen successor is what a majority of `M_g` voted at one ballot, and that
each activated registry holds the complete reconstruction. Tests include
`handover_holds_under_seeded_chaos_and_converges`,
`two_finishers_with_different_stop_quorums_choose_one_successor` and
`killing_the_reconfigurer_after_chosen_cannot_change_the_outcome`.

The simulation checks the same claims on the whole system, in `crates/sim/paros-sim/src/audit/`:

- "generation: at most one matchmaker set is authoritative per generation"
  (`matchmaker.rs:926`).
- "generation: a reconstruction is the union of the frozen quorum above its maximum watermark"
  (`matchmaker.rs:1547`).
- "generation: the decree opens only once every proposed member holds the bootstrap"
  (`matchmaking.rs:1179`) and "generation: one value is voted per decree ballot"
  (`matchmaking.rs:1204`).
- "generation: a chosen set was voted by a majority of its old generation at one ballot"
  (`matchmaking.rs:1254`).
- "generation: a node finishes a handover whose reconfigurer died" is a reachable site
  (`matchmaking.rs:1089`).
