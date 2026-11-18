# paros-observe

The observation layer (`paros::observe`): the `Audit` port that the simulation's oracles implement, a machine's Prometheus metrics, the named BUGGIFY locations and the trace helpers. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-observe/` (3,957 lines of Rust).

## What it did

It sat above `paros-core`, `paros-model`, `paros-wire`, `paros-storage`, `paros-net` and `paros-client`. `paros-roles`, `paros-coordinator`, `paros-machine`, the `paros` facade and `paros-sim` used it. It said "it changes no behaviour" (`src/lib.rs`), but see the leaks below.

Four parts:

- **`Audit`** (`src/audit.rs`): one trait with 112 callbacks, each a no-op by default. The driver calls one at each externally meaningful state change, after the change is real (a durable write after its fsync, a send beside the transmit). It returns nothing, so it cannot steer the run. Production passes `NoAudit`. The only real implementation is `NodeAudit` in `crates/sim/paros-sim/src/audit.rs`; `paros-roles/src/node/handover.rs` wraps it per tenant (`TenantAudits`). The callbacks cover every layer: Paxos (`promised`, `accepted`, `chosen_index`, `commit_started`, `truncated`, `applied`), leadership (`elected`, `stepped_down`, `handoff_refused`), proxies, matchmakers and reconfiguration, reads (`quorum_read_opened`, `log_read_served`), storage (`storage_fault`, `journal_quarantined`), machines (`machine_booted`, `cell_formatting`, `registry_cached`) and the control plane (`system_folded`, `checkpoint_folded`).
- **Scenarios** (`src/scenario.rs`): named BUGGIFY locations (`buggify_named!`) that a harness turns on together per seed with `moonpool_buggify::set_activation`. Each has a const label and a helper function that the driver calls.
- **Metrics** (`src/metrics.rs`, #366 (metrics)): `Metrics::off`/`on` over a plain `prometheus` registry; counters and gauges only, no histogram. `parosd` serves it as `/metrics`. The simulation draws metrics on or off per seed and judges both with the same oracles.
- **Events** (`src/events.rs`): pure helpers for stable trace fields: `config_hash`, `value_hash`, `command_hash` (FNV-1a), `registration_history_hash`, `message_kind`, `message_route`, `message_sender`. `src/reply.rs` names the reply kinds (`Reply`) for dropped or duplicated replies.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-observe/src/lib.rs` | four modules: `audit`, `events`, `metrics`, `reply`, `scenario` (10 lines) |
| `crates/lib/paros-observe/src/audit.rs` | `Audit` (line 200 on), `NoAudit`, `StorageFaultDecision` (`Crash`, `ReadOnly`) with `StorageFaultDecision::of(&StorageError)` |
| `crates/lib/paros-observe/src/scenario.rs` | labels `WITHHOLD_GC`, `HOLD_JOURNAL`, `LOSE_VERDICTS`, `LAG_FOLLOW`, `ADMIT_LATE`, `STALL_PROXY`, `RESIGN_DELEGATING`, `HAND_OFF`, `QUIT_MID_MOVE`, `BATCHER_ABOVE_LIMITS`; helpers `withhold_gc`, `hold_candidate`, `hold_journal`, `lose_verdict`, `lag_follow`, `late_admitter`, `admit_held`, `stall_proxy`, `resign_delegating`, `hand_off`, `quit_mid_move`, `batcher_above_limits` |
| `crates/lib/paros-observe/src/metrics.rs` | `Metrics`, `CallKind`, `CallOutcome`, `Family`, `FamilyKind`, `Phase`, `JournalUsage`, `Totals`; the metric table in the module doc |
| `crates/lib/paros-observe/src/events.rs` | hash and label helpers listed above |
| `crates/lib/paros-observe/src/reply.rs` | `Reply` (`Write`, `SetLeader`, `Truncate`, `Redirect`, …) |
| `crates/lib/paros-observe/src/*/tests.rs` | `audit/tests.rs`, `events/tests.rs`, `metrics/tests.rs`, `reply/tests.rs` |

## Ideas worth reusing

- **Observe, never perturb.** The `Audit` contract (`src/audit.rs` header): no return value, no randomness, no clock; deleting every call must leave the program bit-identical. An event fires only after its transition is durable. Keep this contract for the new `Events` port.
- **The audit is the mirror of BUGGIFY.** A BUGGIFY site perturbs; an audit callback observes. Both sit at the line that makes the choice.
- **Incremental oracles.** Callbacks carry typed values so the judge folds each transition in O(1), instead of re-scanning a trace. The trace stays for humans; nothing reads it back.
- **`commit_started` before the sync** (#454 (control plane meets chaos)): a crash inside a commit lands the batch whole, torn or not at all, and the driver reports none of it. The judge needs the batch's accepts beforehand to judge a recovered log. Same for `floor_requested` before `truncated`. A new journal must expose "about to make durable" as well as "durable".
- **`accepted` carries `promised`**, so "never accept above promise" is checkable per slot.
- **Named scenarios** (#318 (inline BUGGIFY sites)): rare combinations are turned on together per seed, so a scenario does not depend on the product of independent coins. Each label documents why it is always safe and which recovery gate it feeds. Labels are never reworded: the harness names them.
- **Metrics as observation only** (`src/metrics.rs`): written after the outcome is decided and never read by serving code; "seconds counter beside count" gives a mean over any window without a histogram; bounded label values.
- **Stable hashes** (`events.rs`): `config_hash` is order-independent over membership and pinned by tests, so traces compare configurations by equality.

Mistakes not to copy:

- **One trait for every layer** (leak 6): `Audit` has 112 methods, from `promised` to `machine_booted` and `system_folded`. It imports `paros_client::CallObserver`, `paros_model::machine::{CachedRegistry, CellPlan, MachineRecord}`, `paros_net::EdgeRejection` and `paros_storage`. The journal cannot be observed without the whole stack. Split it: one small event type per layer, with `paros-core` types only for the journal.
- **The observation crate changes behaviour** (leak 7): shipped code reads the scenario switches (`lose_verdict()` in `paros-roles/src/node/reply.rs`, `quit_mid_move()` in `paros-coordinator/src/cell/campaign.rs`), and `StorageFaultDecision::of` (`src/audit.rs`) is a driver policy (crash or read-only), not an observation. Fault switches belong with the fault injection; the storage fault policy belongs in the driver.
- **`call_observer` on the node's audit** ties the journal observer to the client crate.

## Bugs and scenarios to cover again

Each scenario below names a rare but legal behaviour and the recovery gate it must reach. A new journal's simulation needs the same shapes.

- `WITHHOLD_GC` (#123 (matchmaker GC)): a node sends no GC request; every prior configuration stays answerable.
- `HOLD_JOURNAL` (#188 (many journals per node)): one journal of a node is held (beat skipped, peer messages dropped); its siblings must keep committing. Gate "journal: a journal keeps committing while a sibling on its nodes is held".
- `LOSE_VERDICTS` (#204 (journal API)): a write verdict is lost; the client's retry must take the `Duplicate` path.
- `LAG_FOLLOW` (#189 (cell control journal)): a node folds the registry late; a message from a not-yet-folded node is refused, later accepted.
- `ADMIT_LATE` (#387 (unpooled joiner)): half the nodes admit the pool 2 s late (`ADMIT_LATE_FOR`); a configuration outside the pool must be ignored whole.
- `STALL_PROXY` and `RESIGN_DELEGATING` (#341 (proxy leaders)): a proxy drops acceptor answers; a leader with delegated rounds resigns and campaigns again. The leader must take rounds back; the proxy must evict rounds nobody answers and close rounds of an old leadership.
- `HAND_OFF` (#484 (silent driver fault sites)): about a third of beats hand leadership off in the chaos window. Gate "leadership is handed over more than once in a run".
- `QUIT_MID_MOVE` (#215 (zones), #498 (moves in the sweep)): a coordinator term resigns mid-move; the next term must finish it.
- `BATCHER_ABOVE_LIMITS` (#702 (batcher rare paths), #911 (rare gates into buggify)): a merged write over the leader's limits must be split.
- Storage fault policy: a full disk gives read-only, every other fault crashes (`a_full_disk_is_read_only_and_every_other_fault_crashes`, `src/audit/tests.rs`, #360 (disk full)).
- Metrics: a restart with the same `Metrics` keeps counting; metrics off record nothing (`src/metrics/tests.rs`).
- Hash stability: `config_hash_distinguishes_membership_and_is_order_independent`, `a_history_hash_tells_order_kind_and_cut_apart` (`src/events/tests.rs`).

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-observe/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-observe

Where to start reading: `src/audit.rs` (the header and the first 30 callbacks), `src/scenario.rs`, `src/metrics.rs` (the module doc), `src/events.rs`; then the implementation, `crates/sim/paros-sim/src/audit.rs` (`NodeAudit`). Its own map at the tag: `crates/lib/paros-observe/AGENTS.md` (it lists the scenarios but omits `QUIT_MID_MOVE`).
