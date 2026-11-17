# Negative hunt: mutants the oracle must catch

Each file here is a mutant that breaks paros on purpose. The hunt
(`paros-sim-runner`'s `mutants` test target) must go red on it.
FoundationDB keeps the same kind of test in `tests/negative`. No oracle
has an excuse or a skip (rule 8, `docs/architecture/06-verification.md`,
step 1.2 of the rework), so a red on any seed catches the mutant.

Run them with `cargo xtask negative` (`--list`, `--seeds N`, `--shard K/N`,
a name filter). The command applies one mutant, builds the hunt, runs it,
and puts the tree back. The weekly `Mutants` workflow runs every mutant,
and so does a pull request that changes this folder. The file format and
the verdicts are in `crates/support/xtask/src/negative.rs`.

## Mutants

| Mutant | What it breaks | Expect |
|---|---|---|
| `loss-faulty-copy-never-qualifies` | A faulty copy at or below the best clean copy never qualifies (CTRL R2/R3). | caught |
| `loss-probe-waits-for-every-member` | The repair probe waits for every member, not a Phase-1 quorum. | either #773 |
| `storage-quarantine-never-ends` | A quarantined journal never opens again. | caught |
| `wipe-amnesiac-store-boots-fresh` | A wiped member boots again with an empty store (no amnesia refusal). | caught |
| `gc-retire-on-stale-belief` | A node may retire on a stale belief of the configuration in force. | survives #773 |
| `machine-released-booking-never-restarts` | A journal released once never starts again on that machine. | caught |
| `heal-maintenance-never-ends` | A maintenance window holds a down machine's roles for good. | survives #773 |
| `heal-replace-after-never` | The coordinator never re-places a down machine's roles. | either #773 |
| `cell-fresh-plan-refused` | `cell init` refuses every fresh plan: no cell forms. | caught |
| `cell-lost-not-refused` | `cell init` does not refuse a plan that lost its quorum. | survives #773 |
| `storage-second-boot-drops-batch` | A store drops its last durable entry at its second boot. | caught |
| `move-releases-a-staying-slot` | A finished move also releases the slot of a member that stays. | caught |
| `cell-lost-at-bare-quorum` | `cell init` refuses a plan as lost when a wipe leaves exactly a Phase-2 quorum intact. | caught |
| `ledger-damaged-record-reads-empty` | A damaged record reads back as nothing accepted. | caught |
| `ledger-promise-not-durable-first` | A raised promise is not durable before the entries under it. | caught |
| `registry-clears-before-registrations` | A matchmaker registry commits its clears before its registrations and metainfo. | caught |
| `frontend-serves-a-read-only-token-write` | The frontend's authorizer counts a journal write as a read, so a read-only token writes. | caught |
| `election-two-leaders-one-term` | A campaign wins the term the election fold is already at: two leaders share one term. | caught |
| `promise-omits-an-accepted-record` | A Promise page skips the first readable record of its range. | caught |
| `plain-leads-on-one-promise` | Phase 1 counts half the members of a single configuration as a quorum. | caught |
| `accept-leaves-without-own-record` | A leader does not record its round when its log holds the same command at a lower ballot. | caught |
| `registry-admits-an-unregistered-member` | A fold at a checkpoint admits an initial member that never registered. | caught |

The `expect` key of each file holds its verdict. `either #N` accepts
caught or survived: the fixed-seed hunt catches that mutant on some heads
and not on others, as the draws fall. `survives #N` is a known survivor.
Step 1.5 of the rework (#1094 (injected liveness bugs)) makes the state
of each such mutant likely on the hunt's seeds and sets its expectation
back to `caught`.

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
