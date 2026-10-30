# Negative hunt: one mutant per oracle excuse

Each oracle excuse (`paros_sim::excuses`, #738 (count excuses)) switches a
check off. Some excuses copy a rule of the system under test. If the copy
is wider than the rule, a real bug hides behind the excuse and the run
stays green. Each file here is a mutant that breaks paros on purpose in the
area of one excuse (#742 (mutant per excuse)). The hunt must go red on a
seed where that excuse fired. FoundationDB keeps the same kind of test in
`tests/negative`.

Run them with `cargo xtask negative` (`--list`, `--seeds N`, `--shard K/N`,
a name filter). The command applies one mutant, builds the hunt
(`paros-sim-runner`'s `mutants` test target), runs it, and puts the tree
back. The weekly `Mutants` workflow runs every mutant, and so does a pull
request that changes this folder. The file format and the verdicts are in
`crates/support/xtask/src/negative.rs`.

## Mutants

| Mutant | Excuse | What it breaks | Expect |
|---|---|---|---|
| `loss-faulty-copy-never-qualifies` | loss: unrecoverable slot | A faulty copy at or below the best clean copy never qualifies (CTRL R2/R3). | caught |
| `loss-probe-waits-for-every-member` | loss: blocked in asked | The repair probe waits for every member, not a Phase-1 quorum. | red elsewhere |
| `storage-quarantine-never-ends` | node: storage dead | A quarantined journal never opens again. | caught |
| `wipe-amnesiac-store-boots-fresh` | node: wiped | A wiped member boots again with an empty store (no amnesia refusal). | red elsewhere (by construction) |
| `gc-retire-on-stale-belief` | node: retired | A node may retire on a stale belief of the configuration in force. | survives #773 |
| `machine-released-booking-never-restarts` | node: left pool | A journal released once never starts again on that machine. | survives #773 |
| `heal-machine-free-one-short` | heal: cell cannot hold shape | Placement sees one free slot less on each machine. | survives #773 |
| `heal-maintenance-never-ends` | heal: maintenance window | A maintenance window holds a down machine's roles for good. | survives #773 |
| `heal-replace-after-never` | skip heal: re-place past tail | The coordinator never re-places a down machine's roles. | survives #773 |
| `cell-fresh-plan-refused` | skip converge: no cell | `cell init` refuses every fresh plan: no cell forms. | caught |
| `cell-lost-not-refused` | skip converge: cell lost | `cell init` does not refuse a plan that lost its quorum. | survives #773 |
| `ledger-damaged-record-reads-empty` | none (fault ledger, #760) | A damaged record reads back as nothing accepted. | caught |
| `ledger-promise-not-durable-first` | none (fault ledger, #760) | A raised promise is not durable before the entries under it. | caught |

The `expect` key of each file holds its verdict. `red elsewhere` means
the hunt is red, but never on a seed where the excuse fired. The wipe
mutant removes the amnesia refusal that the audit reads, so its excuse
cannot fire on a red seed. The six survivors and their reasons are in
#773 (six excuse mutants survive): two excuses are too rare at 600 seeds,
two mutants never bite, one is equivalent, and one shows a probable
oracle gap (no oracle checks that a journal gets its full shape back).

## Excuses with no mutant

These excuses fired on no run of the main sweep (1,099 judged runs, the
`sim (shard 1/4)` job on commit `a6f1cab`). No mutant can go red on a seed
where they fire, because no such seed is known. They are dead code until a
seed fires them; #760 (moonpool owns fault placement) turns the loss
excuses among them into checks that they never fire.

- loss: stranded slot
- loss: frozen by faulty
- wipes: no Phase-1 quorum
- chain: ended by sibling
- chain: storage faults explain
- heal: matchmaker set lost
- converge: matchmaker set lost
- skip heal: cell lost

## Ledger pieces

#760 (moonpool owns fault placement) deletes the harness's fault ledger.
Each piece gets a mutant before it goes; the mutant must stay red after.

| Ledger piece | Mutant |
|---|---|
| Custody ledger, `may_corrupt_record`, `may_park_for_copies` | `ledger-damaged-record-reads-empty` |
| Commit registry and `HintVeto` (`world/cut.rs`), `permit_power_cut` | `ledger-promise-not-durable-first` |
| `OutageVeto`, `may_park`, `may_cut_node`, quiet seats | none yet |
