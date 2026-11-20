# Experiment: the ballot of the handover's decree across a restart

#1229 (a stale `Stopped` reply reuses a decree ballot). This folder is an experiment, not a model
of the plan: it changes no model of `specs/tla/`, and no rule of `paros-consensus` comes from it
until Pierre decides. The report below goes to Pierre.

**Decision**: Pierre chose the durable decree round on 2026-11-19. The rule is recorded in
`docs/architecture/04-roles.md` (*Matchmaker sets are generations*).

## Result in short

TLC reproduces the bug of #1229 with the rule of the code today: a reconfigurer that restarts
counts a `Stopped` reply sent to its earlier incarnation, opens its decree at the round that
the earlier incarnation used, and proposes a second value at that round. Two successors are
chosen at one round (`stale_agreement`), and the node learns a value that is not chosen
(`stale_learn`), in a few seconds.

Both candidate fixes hold:

- **A nonce of the incarnation in `Stop` and `Stopped`** (`nonce`): pass. The floor rule stays
  necessary: with the nonce and no floor, a new incarnation opens at the round of the earlier
  one again (`nonce_nofloor`, violation).
- **A decree round on disk, written before the `Prepare` of the round** (`durable`): pass. With
  the durable round, the floor is not needed for safety (`durable_nofloor`, and
  `durable2a_nofloor` for the weaker write). `durable` and `durable_nofloor` reach the same
  7,446,297 states: with one node, every promise comes from a round of this node, so the floor
  never rises above the round on disk. With other nodes, the floor only skips rounds that the
  matchmakers would refuse.
- A variant of the durable round that is written only before the first `Accept` of the round
  (`durable2a`) passes too: a `Prepare` that the node sent at a round not on disk carries no
  value, so a later incarnation that uses that round again is safe.

## The model

`restart/StaleStopped.tla`: one node runs the handover over three matchmakers under a majority.
It sends `Stop`, counts the first `Stopped` of each member until a majority answered, and opens
the successor decree at `max(round, floor) + 1`, where `floor` is the highest decree promise of
the counted replies (`matchmaker/reconfigurer.rs`, `fold_stopping` and the opening of the
decree). Then single-decree Paxos runs over the matchmakers. The node can restart: its volatile
state is lost, and the new incarnation starts the handover again with another target.

As in the code: a message is never lost from `msgs`, so a reply can arrive late, two times, or
at a later incarnation; an acceptor promises again at an equal ballot; the node counts
`Promised` and `Accepted` by sender and ballot; and `Accepted` carries no value, so the node
learns its own value once a majority sent `Accepted` at its round.

| Constant | Values |
|---|---|
| `Bind` | `"none"`: nothing ties a `Stopped` to an incarnation (the code today). `"nonce"`: `Stop` carries a nonce of the incarnation, `Stopped` echoes it, and only a reply with the current nonce counts. `"durable"`: the round is on disk before the `Prepare` of that round, and a new incarnation starts from it. `"durable2a"`: the same write, made only before the first `Accept` of the round. |
| `Floor` | `TRUE`: the decree opens above the stop quorum's floor. `FALSE`: a control, the floor is not used. |
| `NR` | The highest round. |
| `Restarts` | How many times the node can restart. |
| `Retries` | `TRUE`: one incarnation can open its decree again at a higher round (`abort`, then `start`). |

Invariants:

- `Agreement`: one successor is chosen (a majority voted for it at one round).
- `LearnOnlyChosen`: a node learns only a chosen value.
- `OneValuePerRound`: no two votes at one round differ. A break is the panic of
  `proposer.rs:374` (two Phase-1 reports of one ballot) or of `acceptor.rs:612-615` (an `Accept`
  of a second value at a voted ballot) in the code.
- `NoLearnAfterRestart`: a reachability control, a node learns after a restart.

## Results

`restart/checks.txt`. Three matchmakers, two values, symmetry over both. The `long` checks
that say "not finished yet" were still running when this report was written; the PR of the
fix adds their results.

| Check | Rule | Constants | Result |
|---|---|---|---|
| `stale` | the code today | NR 2, 1 restart | `OneValuePerRound` violation |
| `stale_agreement` | the code today | NR 2, 1 restart | `Agreement` violation (depth 21) |
| `stale_learn` | the code today | NR 2, 1 restart | `LearnOnlyChosen` violation |
| `nonce_reach` | nonce | NR 2, 1 restart | `NoLearnAfterRestart` violation (reachable) |
| `nonce` | nonce + floor | NR 2, 1 restart | pass (1,089,061 states, 1 min) |
| `nonce_nofloor` | nonce, no floor | NR 2, 1 restart | `OneValuePerRound` violation |
| `durable_reach` | durable | NR 2, 1 restart | `NoLearnAfterRestart` violation (reachable) |
| `durable` | durable + floor | NR 2, 1 restart | pass (7,446,297 states, 6.5 min) |
| `durable_nofloor` | durable, no floor | NR 2, 1 restart | pass (7,446,297 states) |
| `durable2a` | durable before `Accept` + floor | NR 2, 1 restart | pass (7,256,239 states, 6.5 min) |
| `durable2a_nofloor` | durable before `Accept`, no floor | NR 2, 1 restart | pass (7,143,753 states, 6.5 min) |
| `nonce_retry` | nonce + floor | NR 2, 1 restart, retries | pass (20,921,801 states, 19 min) |
| `durable_retry` | durable + floor | NR 2, 1 restart, retries | not finished yet |
| `durable2a_retry` | durable before `Accept` + floor | NR 2, 1 restart, retries | not finished yet |
| `durable_nofloor_retry` | durable, no floor | NR 2, 1 restart, retries | not finished yet |
| `nonce_two` | nonce + floor | NR 3, 2 restarts | not finished (stopped at 44 million states, no error) |
| `durable_two` | durable + floor | NR 3, 2 restarts | not finished yet |

The trace of `stale_agreement` (depth 21) is shorter than the schedule of #1229, because the
new incarnation needs no fresh message at all. Incarnation 0 proposes `v1`: `m1` and `m2`
answer `Stop` with promise 0, the decree opens at round 1, `m1` and `m3` promise, and the node
sends `Accept(1, v1)`. The node restarts. Incarnation 1 proposes `v2`: it counts the two old
`Stopped` replies of `m1` and `m2` (floor 0), opens at round 1 again, counts the two old
promises of round 1 (they match its ballot), and sends `Accept(1, v2)`. Both `Accept` messages
are now in flight at round 1. `m1` and `m2` vote `v1`, then `v2`, and both values are chosen at
round 1. The model's acceptor has no assertion; in the code the second vote at one ballot is
the panic of `acceptor.rs:612-615`, and `stale` (`OneValuePerRound`) stops at the first two
different votes at round 1.

Why the nonce alone is not enough. Without the floor, a new incarnation starts at round 0 and
opens at round 1 again; the stop quorum is fresh, but the round is the one that the earlier
incarnation used. The floor is what moves the round: a fresh `Stopped` of a member of the
earlier promise quorum reports a promise at or above the earlier round, and every stop quorum
holds such a member.

## Cost of each fix

Neither fix exists in the code. This section is my reading of the code, not a check.

- **Nonce.** A wire change: `ReconfigureRequest::Stop` and `ReconfigureReply::Stopped` carry a
  `u64` nonce, the matchmaker echoes it and stores nothing, and `fold_stopping` ignores a reply
  with another nonce. `paros-consensus` has no RNG, so the driver gives the nonce to
  `MatchmakerReconfigurer::new`. The nonce must differ between two incarnations of one node: a
  random `u64` of the driver does it with a small chance of a collision (the same trust as the
  client's fresh leader uuids, `docs/architecture/02-data-plane.md` §2.3), or a boot counter on
  disk does it for sure (and that is a disk write again). No disk write in the handover itself,
  and the floor rule stays.
- **Durable round.** The reconfigurer keeps nothing on disk today. It needs a write in its
  `Ready` (the round, before the `Prepare` leaves: persist before send), and its constructor
  takes the round read back at boot. One durable write for each decree that opens, and a
  handover is rare. It is the rule that the rest of the core already uses against ballot reuse
  (the allocator frontier of Multi-Paxos, `record_own_round`) and that the `consensus-sim`
  decree walk gives its proposer. The floor becomes a liveness hint, not a safety rule. The
  write can wait until the first `Accept` (`durable2a`), which saves the write for a `Prepare`
  that a higher ballot preempts; the gain is small, since a handover opens few rounds.

## Not in the model

The ballots of other nodes (a ballot names its node, so they never equal a round of this node),
the bootstrap pages, the matchmaker generation, a successor that a `Stopped` reports (it only
makes a reconfigurer stop, so leaving it out adds behaviors), a wiped matchmaker (#1229 notes
that the handover decree is unsafe with one, `decree/` F2 of the gap review), and liveness. A
matchmaker answers the `Stop` of one incarnation once (the network can still deliver that
answer late, two times, or to a later incarnation): a second answer to one incarnation is left
out to keep the state space small, since the reconfigurer counts one answer of each member.
