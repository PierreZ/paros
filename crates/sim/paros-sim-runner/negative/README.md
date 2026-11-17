# Negative hunt: mutants the oracle must catch

Each file here is a mutant that turns on a known bug in paros. The hunt
(`paros-sim-runner`'s `mutants` test target) must catch it. FoundationDB
keeps the same kind of test in `tests/negative`, and Paxos Made Live
measured its tests "by leaving known bugs in and confirming detection".

**A mutant is judged against a baseline** (step 1.5 of the rework, #1094
(injected liveness bugs)). Main is red on a share of the seeds while the
causes of phase 1 are open (rule 8, `docs/architecture/06-verification.md`):
128 of the first 300 seeds on the step 1.5 commit, most of them "fleet: no
tenant is left mid-operation after chaos". So a red alone shows nothing.
The command first runs the unmutated hunt over the same seeds and records
each seed's reds. A mutant is caught at the first seed that has a red the
baseline does not have on that seed, of one of the causes the mutant names
(`cause:` lines; `paros_sim::mutant_hunt`). The causes of each mutant are
the reds it turned on at the seed that caught it.

Run them with `cargo xtask negative` (`--list`, `--seeds N`, `--shard K/N`,
`--baseline FILE`, `--baseline-only`, a name filter). The command records
the baseline, applies one mutant, builds the hunt, runs it, and puts the
tree back. The weekly `Mutants` workflow runs every mutant, and so does a
pull request that changes this folder: its `negative-baseline` job records
the baseline once for the four shards. The file format and the verdicts
are in `crates/support/xtask/src/negative.rs`.

## Mutants

Caught at: the first seed, of 300, that caught the mutant against the
baseline of 300 seeds (local run on the step 1.5 commit). A mutant caught
by "no tenant is left mid-operation" is caught by an open cause, not by the
oracle that its bug is aimed at: the bug stalls the tenant operation first.

| Mutant | What it breaks | Caught at | Expect |
|---|---|---|---|
| `handover-finish-proposes-answerers` | The answerer rule everywhere: a handover proposes the matchmakers that answered its freeze, also when the coordinator named the target (#846 (finish shrinks a set)). | seed 10, sets | caught |
| `matchmaking-keeps-a-stale-registration` | A campaign keeps its stale registration over the reconfiguration the matchmakers report (`StaleConfiguration`). | seed 3, matchmaking | caught |
| `loss-faulty-copy-never-qualifies` | A faulty copy at or below the best clean copy never qualifies (CTRL R2/R3). | seed 50, open cause | caught |
| `loss-probe-waits-for-every-member` | The repair probe waits for every member, not a Phase-1 quorum. | seed 78, open cause | caught |
| `storage-quarantine-never-ends` | A quarantined journal never opens again. | seed 3, convergence | caught |
| `wipe-amnesiac-store-boots-fresh` | A wiped member boots again with an empty store (no amnesia refusal). | seed 253, promise | caught |
| `gc-retire-on-stale-belief` | A node may retire on a stale belief of the configuration in force. | not in 300 | survives #1177 |
| `machine-released-booking-never-restarts` | A journal released once never starts again on that machine. | seed 5, controls | caught |
| `heal-maintenance-never-ends` | A maintenance window holds a down machine's roles for good. | not in 300 | survives #1177 |
| `heal-replace-after-never` | The coordinator never re-places a down machine's roles. | seed 31, heal | caught |
| `cell-fresh-plan-refused` | `cell init` refuses every fresh plan: no cell forms. | seed 1, init | caught |
| `storage-second-boot-drops-batch` | A store drops its last durable entry at its second boot. | seed 3, convergence | caught |
| `move-releases-a-staying-slot` | A finished move also releases the slot of a member that stays. | seed 116, placement | caught |
| `cell-lost-at-bare-quorum` | `cell init` refuses a plan as lost when a wipe leaves exactly a Phase-2 quorum intact. | seed 5, init | caught |
| `ledger-damaged-record-reads-empty` | A damaged record reads back as nothing accepted. | seed 7, journal store | caught |
| `ledger-promise-not-durable-first` | A raised promise is not durable before the entries under it. | seed 14, registry | caught |
| `registry-clears-before-registrations` | A matchmaker registry commits its clears before its registrations and metainfo. | seed 1, open cause | caught |
| `frontend-serves-a-read-only-token-write` | The frontend's authorizer counts a journal write as a read, so a read-only token writes. | seed 33, frontend | caught |
| `election-two-leaders-one-term` | A campaign wins the term the election fold is already at: two leaders share one term. | seed 26, open cause | caught |
| `promise-omits-an-accepted-record` | A Promise page skips the first readable record of its range. | seed 2, core oracle | caught |
| `plain-leads-on-one-promise` | Phase 1 counts half the members of a single configuration as a quorum. | seed 8, reconfiguration | caught |
| `accept-leaves-without-own-record` | A leader does not record its round when its log holds the same command at a lower ballot. | seed 2, core oracle | caught |
| `registry-admits-an-unregistered-member` | A fold at a checkpoint admits an initial member that never registered. | seed 28, core oracle | caught |

The `expect` key of each file holds its verdict. `survives #N` is a known
survivor: the hunt's seeds do not reach its state, and issue N makes the
state likely. `cell-lost-not-refused` (`cell init` does not refuse a plan
that lost its quorum) is deleted as an equivalent mutant: with fewer than a
Phase-2 quorum of intact members the plan cannot be chosen, so the refusal
changes no outcome.

The four `core oracle` mutants (`promise-omits-an-accepted-record`,
`plain-leads-on-one-promise`, `accept-leaves-without-own-record`,
`registry-admits-an-unregistered-member`) break paros-core and paros-roles
where only the oracles of #458 (core oracles) can see the bug: each edit
keeps the asserts of its own function true. The oracles are in
`paros-sim/src/audit/claims.rs` and `audit/system.rs`.

## Ledger pieces

#760 (moonpool owns fault placement) deletes the harness's fault ledger.
Each piece gets a mutant before it goes; the mutant must stay red after.

| Ledger piece | Mutant |
|---|---|
| Custody ledger, `may_corrupt_record`, `may_park_for_copies` | `ledger-damaged-record-reads-empty` |
| Commit registry and `HintVeto` (`world/cut.rs`), `permit_power_cut` | `ledger-promise-not-durable-first` (deleted) |
| Matchmaker cut budget, `HintVeto`, `OutageVeto` (`world/cut.rs`) | `registry-clears-before-registrations` (deleted) |
| `may_park`, `may_cut_node`, quiet seats | none yet |
