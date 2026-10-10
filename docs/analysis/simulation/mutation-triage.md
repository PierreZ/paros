# Mutation hunt: triage of the first run (#269)

This note records the first full `cargo xtask mutants` run over the
safety-critical `paros-core` modules (`.cargo/mutants.toml`), and what was done
with every surviving mutant. The weekly workflow writes later runs into the
rolling `mutation-survivors` issue; this note is the baseline those runs start
from.

## The run (2026-10-10)

- Scope: the eleven modules of `.cargo/mutants.toml`, 726 mutants.
- Test: `paros_sim::chain_mutants`, seeds 1..=100 of the main campaign, in
  release, 4 jobs on 4 cores, about 3 hours.
- Result: 402 caught, 26 timeouts (a timeout counts as caught), 100 unviable,
  198 survived.

## Classes

The 198 survivors split into the four classes of #269:

| Class | Count | What happened |
|---|---:|---|
| Equivalent | about 70 | Excluded in `.cargo/mutants.toml` with a reason, or listed below when a regex cannot isolate the edit. |
| Missing oracle | about 45 | An assertion or an audit check added in PR #334 (next section). |
| Unreachable state | about 65 | Issues #338 to #343: the oracle exists, but no seed reaches the shape. |
| Wire hygiene | about 15 | Issue #344: malformed input that honest peers never send; a unit test pins it. |

## Oracles added

Each line names the rule and the mutants it kills.

- `serve_catchup`: a catch-up page stops only at its bound, at the end of the
  prefix, or at a hole (the `+` edits of the expected slot).
- `Replica::fold_one`: a folded verdict reads back through `outcome_at`
  (`outcome_at -> None`, the `Noop` filter inverted).
- `Replica::fold_hole`: a hole lies below the first unchosen slot (the `<=`
  and `==` edits).
- `Replica::truncate`: the dropped prefix holds no record at or past
  `first_seq`, checked apart from `compaction_target`.
- `Acceptor::record_accepted`: a repair is counted once.
- `ColocatedNode` recovery pump: a drained recovery closes in the page that
  drains it, and an open one still has slots to sweep
  (`Recovery::remaining -> 1`, `recovery_remaining -> 1`,
  `close_drained_recovery`).
- `start_accept_round_in`: a delegated round is tracked by its opener
  (`open_delegated_round -> ()`); auto delegation delegates exactly on a proxy
  deployment (`ProxyId::of -> None`); a slot names a column exactly under a
  grid (`column_of -> None`).
- `own_vote`, `Rounds::fold_accepted_in`: a vote counts only from the round's
  column, read from the addressee list (`is_phase2_addressee`'s `&&`).
- Quorum reads (node and replica): a read names a row exactly under a grid,
  and a reader answers itself only from inside its row (`read_row`, `row_of`,
  `is_phase1_addressee`).
- `take_back_delegated`, `ProxyLeader::expire_stale`: no round past its budget
  stays delegated or retained (`stalled_delegations`). `Rounds::stalled ->
  vec![]` survived until the stalled-proxy scenario (#341, below).
- `on_nack`: a Nack at the work in flight deposes it, and a Nack at no work in
  flight leaves the role alone (the four `supersedes` mutants).
- `learn`: a slot learned chosen leaves the repair probe
  (`probe_resolved_elsewhere -> ()`).
- `probe_membership`: a probe is tagged by its own fresh round
  (`MembershipProbe::ballot -> Default`).
- `Matchmaking::assert_invariants`: the disagreement count equals the extra
  configurations; `MatchStep::Completed` now carries the count, so the audit's
  "no two matchmakers disagree" sees it. Before, the driver read it after the
  phase closed and always got zero. `disagreements -> 0` still survives: no
  seed makes two matchmakers disagree (#343).
- `JournalState`: a won `SetLeader` names the leader in force and installs a
  different, set uuid; an accepted write takes the position it names. Both
  need a call the workload never sends (#339).
- The workload's malformed reconfiguration: "malformed" is now the workload's
  own arithmetic, with three shapes (`{1, 1}`, the boundary `{1, n - 1}`, a
  2-row grid that does not tile). It used the library's own `admits`, so a
  mutant of `admits` also stopped the request (`cross_intersects`, `admits`).

## Verification

The 137 mutants of the functions this PR touched ran again, first at 100
seeds, then the survivors at the weekly 300 seeds:

- 118 caught (6 of them by a timeout), 7 unviable.
- `column_of` (both) and the three recovery counters die only at 300 seeds:
  seeds 1..=100 draw no grid and drain few recoveries.
- 7 still survive:
  - `JournalState::apply_write` and `apply_set_leader`, `||` to `&&`: the
    workload never sends the call (#339).
  - `Matchmaking::disagreements -> 0` and `Rounds::stalled -> vec![]`
    (above).
  - `Replica::fold_hole -> None` and `<` to `>`: a fold hole needs a lost
    chosen record below the prefix, and no seed makes one (#343).
  - `read_row`'s guard to `false`: equivalent (next section).

## Liveness and matchmaker rules (#343)

The survivors #343 lists, each now judged or gone:

- `MembershipProbe::quorum_held -> true`: `MatchStep::ProbeClosed` carries
  `answered_by`, and the audit asserts "matchmaking: a membership probe
  closes only on a matchmaker quorum".
- `RepairProbe::stragglers` and `suffix_start`: the re-sent `Prepare` is one
  value, `RepairProbe::requery`. `ColocatedNode::tick_repair` restates the
  straggler set over `RepairProbe::answered` and `prior`: every unanswered
  prior member is asked again, and nobody else. The ballot keeps its check;
  the first slot is a field, not a function to mutate.
- `AcceptorConfig::is_drawn_from -> true` still survives. The node's pool
  invariants now use the pool itself (`pooled_all`), so the mutant fails the
  moment a node adopts a configuration naming a node it has not pooled. A
  probe marks each `UnknownMember` return; neither fired in 600 hunt seeds.
  A trial BUGGIFY that deferred a node's pool admission fired twice in 600
  seeds and did not reach the race either: a reconfiguration rarely names a
  freshly registered joiner. #387 (an unpooled joiner in a configuration)
  makes that shape likely.
- `Acceptor::first_faulty -> None`: deleted with both its call sites.
  `assert_invariants` asserts that the fold's hole covers every faulty slot
  under the chosen prefix.
- `Replica::fold_hole -> None` and `<` to `>`: they survived because the
  `first_faulty` leg did the hole's work. An entry rot may also aim at the
  oldest slot held (its own BUGGIFY location); the boot probe "the fold stops
  at a hole under the chosen prefix" fired on 326 boots in 600 hunt seeds.
- `Matchmaking::disagreements -> 0`: excluded as equivalent.

## Compaction progress and the #204 retained retry (#342)

`Replica::assert_compacted` judges every compaction that follows a folded
`Truncate`. It is checked apart from `compaction_target`, at both callers
(`ColocatedNode` and `ReplicaNode`):

- Safety: every retained single-writer retry still holds the record it was
  judged against ("a compaction keeps every record a retained retry reads").
  This is the #204 shape without the refold: a floor that drops the record
  fails at once, not only after a reboot or a trim-point jump.
- Progress: the floor slot is needed. It is the fold's head, or it holds the
  journal's first record, or it holds a record a retained retry read ("a
  compaction leaves a needed slot at the floor").

A probe marks the fixed point moving down ("compaction: a retained retry
holds the floor down"). It fired 1,341 times in a 300-seed hunt, so the
#204 shape needs no new scenario or BUGGIFY.

Each mutant applied by hand, 100 seeds:

- `compaction_target -> None`, `-> Some(Default)` and `slot_holding ->
  Default`: caught in seeds 1..=20 (progress).
- `==` to `!=` at the mode test: caught in seeds 1..=20 (both checks).
- `entry.seq >= at.first_seq` to `<`, and `entry.seq < at.next_seq` to `==`
  and to `>`: caught in seeds 1..=20 (safety).
- `&&` to `||` in the same test: caught in seeds 81..=100 (progress).

## Proxy take-back, eviction and supersession (#341)

The oracles existed. No seed in `1..=300` reached the three proxy states the
mutants need. The stalled-proxy scenario (`shape::stalled_proxy`) turns on
three ingredients together on one per-seed draw:

- Every proxy drops the `Accepted`s and `Nack`s it hears for the chaos window
  (`paros::scenario::STALL_PROXY`).
- A leader that holds delegated rounds resigns and campaigns again at once
  (`paros::scenario::RESIGN_DELEGATING`).
- The run runs an acceptor grid where the pool tiles one.

Two probes mark the states: "proxy: a leader takes a delegated round back on
a grid column" and "proxy: a higher-ballot delegation meets a superseded
leadership's rounds". The leader change needs the second ingredient: without
it, a new election raised the acceptors' promises, the proxy's re-send met
their `Nack`s and closed its rounds before the next leader delegated.

Each mutant applied by hand, at the weekly 300 seeds:

- `Round::column -> None`: caught in seeds 61..=80 ("every vote behind a
  decision comes from the round's column"; the take-back re-sends to the
  whole membership).
- `Rounds::close_below -> vec![]`, `<` to `==` and `<` to `>`: caught in
  seeds 1..=20 ("a superseded leadership's rounds are gone", "no round below
  the ballot survives").
- `Rounds::stalled -> vec![]`: caught in seeds 1..=20 ("no round past the
  retention budget survives").

## The 64-entry page bounds (#338)

No seed filled a 64-entry page, so every mutant of a page bound survived.
Each bound is now a per-node tunable under its 64-entry ceiling, drawn per
seed with an extreme of one to four entries (`DriverTunables::promise_page`,
`resend_page`, `apply_page`, `registry_page`; the recovery page was drawn
already, #330). The driver hands each size to the core at boot. A receiver
checks a promise page or a registry page against the ceiling, never against
the sender's size: a page with a cursor carries at least one entry. The
audit cuts a matchmaker's page window at that matchmaker's size.

The 103 mutants of the listed functions (`Acceptor::promise_page`,
`Matchmaking::accepts`, `Recovery::remaining`, `Proposer::recovery`, both
`resend_page`, `Replica::advance`, `Replica::read`), at the weekly 300
seeds:

- 67 caught, 5 timeouts (caught), 18 unviable.
- 3 equivalent, listed in the next section: the merge order `<` to `<=` in
  `promise_page`, `Replica::read`'s record-limit `<` to `<=` in the walk
  guard, and `Rounds::resend_page`'s wrap guard `<` to `<=`.
- 10 survive in `Matchmaking::accepts`: `-> true`, the cursor leg's
  `cursor_collected` edits (`>` to `==` and `>=`, `==` to `!=`), and the
  `&&` to `||` and `>` to `>=` edits on the page shape. Each one only
  accepts a page that an honest matchmaker never sends: a matchmaker
  asserts the shape of its own page. They are wire hygiene, #344.

## Equivalent edits a regex cannot isolate

These stay in the rolling report. Each one is equivalent; a regex that names
it would also hide a real mutant of the same function.

- `acceptor.rs` `promise_page`: `<` to `<=` in the merge order (the readable
  and faulty keys are disjoint).
- `matchmaking.rs` `heard_anyone`: `delete !` on the page cursor leg (at the
  only call, `registered_by` is not empty).
- `membership.rs`: `<` to `<=` in `QuorumSystem::is_phase1_quorum_in` and
  `is_phase2_quorum_in` (the row and the column are always in range);
  `read_row`'s guard to `false`.
- `replica.rs`: the `assert_invariants` guard `<` to `>`; `Replica::read` `<`
  to `<=` (two of three sites) and `>` to `<`; `refold`'s guard `<` to `>`;
  `trim_to` `<` to `<=`; `compaction_target` `<` to `<=` on the position test.
- `proposer/probe.rs`: `RepairProbe::blocked -> empty` (read only in
  assertions).
- `proposer/rounds.rs`: `close_below` and `resend_page` `<` to `<=` (the
  boundary cannot occur).

## Hand-made proofs

#267's mutation ("only the newest prior configuration is consulted":
`slot_decidable` checks `prior.last()` instead of every configuration) is not
an edit cargo-mutants generates. It stays documented next to the rule, on
`slot_decidable` in `proposer.rs`, with its red witness. `proposer.rs` itself
is outside the first pass's scope; the next pass can add it.
