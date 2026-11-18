# paros-sim-runner

The native binaries that ran the paros-sim campaign (sweep, hunt, replay), the declared-gate check and the mutation hunt test target. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/sim/paros-sim-runner/` (2,275 lines of Rust, plus 23 negative mutant files).

## What it did

It was the top of the sim stack: `paros-core` ← `paros` ← `paros-sim` ← `paros-sim-runner`. It depended only on `paros-sim` and on `moonpool-assertions` (for `MAX_ASSERTION_SLOTS`). Nothing depended on it; `xtask` ran its binaries and its test target.

- `sim-paros-chain` (`src/main.rs`): the CI sweep. It ran `paros_sim::explore(SWEEP_BATCH)` batch after batch until a time budget ended (`PAROS_SIM_SWEEP_MINUTES`, default 60). It stopped at the first red batch. It failed on a violation, a failed run, a gate that never fired, or a declared gate that never ran (unless listed in `gates::RARE`). It could chase missed gates (`PAROS_SIM_CHASE`) and write reds for the `main-reds` job (`PAROS_SIM_RED`).
- `sim-paros-hunt` (`src/hunt.rs`): raw seed volume (`main|canary [iterations] [gate-filter]`), single-seed replays (`replay-main`, `replay-canary`, `explore-main`, `replay-recipe`), a seed dump (`--dump <dir>`: fingerprint, violations, draws, gates), and `gate-diff`.
- `tests/mutants.rs`: the mutation hunt (#269). It ran seeds `1..=PAROS_MUTANT_SEEDS` one at a time and exited 1 at the first seed with a red that the baseline did not have, of one of the mutant's causes. Built only with the `mutants` feature.
- `build.rs` + `src/scan.rs`: a small lexer that read every literal assertion message in the scanned crates (`SCANNED`) and compiled them in as `gates::DECLARED` and `ASSERTIONS`.

The simulation was turned off in CI at step 0.13 (decision 33, `docs/architecture/06-verification.md`), so at the tag these binaries ran only locally.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/sim/paros-sim-runner/src/main.rs` | `sim-paros-chain`: `Sweep::run`, `sweep_budget`, `chase_targets`, `write_red`, `write_gate_defects`, `print_guidance` |
| `crates/sim/paros-sim-runner/src/hunt.rs` | `sim-paros-hunt`: `replay_for`, `dump`, `gate_diff`, `print_never_fired` |
| `crates/sim/paros-sim-runner/src/gates.rs` | `DECLARED`, `RARE` (with a reason each), `RARE_CEILING` = 76, `MAX_SLOTS`, `SLOT_LIMIT` (15/16 of 8192), `slot_name` (first 63 bytes), `judge`, `print_rarest` |
| `crates/sim/paros-sim-runner/src/scan.rs` | The declared-gate lexer: skips comments and `#[cfg(test)]` items, takes the first literal string argument of each gate macro |
| `crates/sim/paros-sim-runner/build.rs` | `SCANNED` crate list; writes `declared_gates.rs` to `OUT_DIR` |
| `crates/sim/paros-sim-runner/src/common.rs` | `arg`, `is_clean`, `print_seed_counts`, `print_failed_runs` |
| `crates/sim/paros-sim-runner/tests/mutants.rs` | The hunt test target; env `PAROS_MUTANT_SEEDS`, `PAROS_MUTANT_RECORD`, `PAROS_MUTANT_BASELINE`, `PAROS_MUTANT_CAUSES` |
| `crates/sim/paros-sim-runner/negative/*.mutant`, `negative/README.md` | 23 hand-written mutants and their table (caught at which seed, by which oracle) |

## Ideas worth reusing

- **Declared-gate check** (`build.rs`, `src/scan.rs`, `src/gates.rs`, #446 (a gate that never fires cannot fail the sweep)). moonpool makes a slot only when an assertion runs, so a gate on a dead path disappears from the coverage count. The scan lists every gate in the source, and the sweep fails on each one with no slot. Reuse this with moonpool-walk: a coverage gate that never runs must be a failure.
- **A rare list that only shrinks** (`RARE`, `RARE_CEILING`, unit test `the_rare_list_never_grows`, #911 (rare gates into buggify)). Each entry has a reason and a measured rate.
- **Slot budget checks** (`no_two_assertions_share_a_slot_name`, `the_static_assertion_count_fits_the_slot_limit`). They catch two messages with the same 63-byte prefix and a slot table overflow before a sweep turns every seed red.
- **Negative mutants as files** (`negative/*.mutant`): a find/replace edit, an `expect`, and the `cause:` lines that must catch it. Judged against a baseline (`paros_sim::mutant_hunt`). This is the best way to prove that an oracle catches the bug it is for. Port the format and the list.
- **Seed tools**: `--dump` and `gate-diff` (#921 (seed tools)) show what one seed drew and which gates only it fired.

Mistakes not to copy:

- Exit on "gate never fired" in the same job as safety. Gate defects needed CI shards, a chase mode and the `RARE` list to stay green. Keep coverage checks in their own job.
- A `SCANNED` list kept by hand beside the xtask sancov list; a unit test (`every_sancov_crate_is_scanned`) had to keep the two in step.

## Bugs and scenarios to cover again

The negative mutants are the regression list. Each names a known bug that a new journal or a new sim must catch again. From `negative/README.md` at the tag:

- `handover-finish-proposes-answerers`: a handover proposes the matchmakers that answered its freeze, also when the coordinator named the target (#846 (finish shrinks a set)). Caught by the sets oracle.
- `matchmaking-keeps-a-stale-registration`: a campaign keeps a stale registration over the reconfiguration in force. Caught by matchmaking oracles.
- `loss-faulty-copy-never-qualifies`: a faulty copy at or below the best clean copy never qualifies (CTRL R2/R3).
- `loss-probe-waits-for-every-member`: the repair probe waits for every member, not a Phase-1 quorum.
- `storage-quarantine-never-ends`, `storage-second-boot-drops-batch`: a quarantined journal never opens again; a store drops its last durable entry at its second boot.
- `wipe-amnesiac-store-boots-fresh`: a wiped member boots with an empty store (no amnesia refusal).
- `promise-omits-an-accepted-record`, `accept-leaves-without-own-record`, `plain-leads-on-one-promise`, `registry-admits-an-unregistered-member`: the four core-oracle mutants. Each keeps its own function's asserts true; only `paros-sim/src/audit/claims.rs` and `audit/system.rs` see them.
- `ledger-damaged-record-reads-empty`, `ledger-promise-not-durable-first`, `registry-clears-before-registrations`: storage write order and rot.
- `election-two-leaders-one-term`: two leaders share one term.
- `cell-fresh-plan-refused`, `cell-lost-at-bare-quorum`, `move-releases-a-staying-slot`, `machine-released-booking-never-restarts`, `heal-replace-after-never`, `frontend-serves-a-read-only-token-write`: cell, placement and frontend shapes (out of scope for consensus-sim).
- Known survivors: `gc-retire-on-stale-belief` and `heal-maintenance-never-ends`, tracked in #1177 (mutants whose state the seeds never reach).

At the tag, main was red on 128 of the first 300 seeds (mostly "fleet: no tenant is left mid-operation after chaos"). This is why every mutant is judged against a baseline.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/sim/paros-sim-runner/src/main.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/sim/paros-sim-runner

Where to start reading: `negative/README.md`, one `.mutant` file (for example `negative/handover-finish-proposes-answerers.mutant`), `tests/mutants.rs`, `src/gates.rs`, then `src/scan.rs`. Its own map at the tag: `crates/sim/paros-sim-runner/AGENTS.md`.
