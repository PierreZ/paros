---
name: adding-an-audit-check
description: Add or move a correctness check in paros the doctrine way - an Audit port callback in crates/paros/src/audit.rs reported once where the tracing event is, folded into O(1) state in paros_sim::audit (AuditState, MatchmakerAudit, ClientHistory, ChainState), asserted with moonpool macros (assert_always! with a detail map, assert_sometimes! for outcomes, reach_once! for causes), inside the 2048-slot / 256-bucket budget with stable message strings. Use when a fact a check needs is not visible, when a sometimes never fires, when tempted to read the trace, or when moving an existing assertion.
---

# Adding an audit check

Correctness lives in the audit and in the workload's `check()`, never in
trace scanning. The `Audit` port (`crates/paros/src/audit.rs`, production
passes `NoAudit`) *reports* every externally meaningful transition, typed,
once, at the instant it happens; `paros_sim::audit` folds each report into
incremental state and asserts there. Nothing an `Audit` implementation does
may change the run: no return value, no randomness, no wall clock; deleting
every audit call leaves the shipped program bit-identical.

## 1. Is the fact already reported?

`Audit` has roughly seventy-five callbacks (promise raised, accept persisted,
slot applied, message sent or dropped at the send seam, leader elected, gap
observed, client acked, node recovered, the matchmaking / GC / generation
steps, the trim-point jump). Read the trait before adding one. If the
fact exists nowhere the audit can see, add a callback with a default no-op
body, and call it in the driver **right where the matching `tracing` event
is emitted**, with the same coordinates (`node`, `from`, `round`, `slot`).

## 2. Fold it where it belongs

| Fact | Lives in |
|---|---|
| protocol safety per transition (promise monotonic across restart, one value per slot, floors, chosen prefix, storage gates) | `crates/paros-sim/src/audit/state.rs` (`AuditState`) |
| registry, leader-side matchmaking, GC, generations | `audit/matchmaker.rs` (`MatchmakerAudit`) |
| client-visible history: linearizability against the journal model | `audit/client.rs` (`ClientHistory`) and `audit/linearizability.rs`, fed by the workload, which alone knows its program order |
| the application's fold (#186: paros runs no application, the client is it): one command and one state per LSN across clients, each client's fold in increasing LSN order, proposal validity | `ChainState` (`chain.rs`), folded by each client from what it `Read`s (`chain_workload/fold.rs`) and reported through `AuditWorld::fold_applied` (`audit/world.rs`); keep its message strings — they are the old application check's |
| end-of-run claims (convergence of the recovery tail, `node.replica().chosen_gap()` at quiescence) | `audit/world.rs` (`check_run`, `AuditWorld::check_final_convergence`) |
| a relation *between* journals (#188: a slot applies only its own journal's identities, a quarantined journal sends nothing, a journal commits while a sibling is held or quarantined) | the journal board, `audit/journals.rs` — every per-journal fact stays on the journal's own `AuditWorld` (`audit_world_for`, one per journal), which is how every other oracle is keyed by journal without knowing it |

Keep the fold O(1) per callback; the audit runs on every transition of every
node of every seed.

## 3. Assert with the right macro

- `assert_always!(cond, "short stable message", { "node" => id, "slot" => s })`
  for an invariant. It records and continues, so the cascade of one root
  cause shows in one seed. Put ids in the detail map, never in the message.
- `assert_sometimes!(cond, "...")` only for an **outcome** the sweep must be
  proven to reach (a leader elected under a fault, a below-floor node healed
  by a trim-point jump, a read confirmed across a leader change). An evaluated-but-
  never-true `sometimes` fails the runner, so use it only where the campaign
  is certain to reach it.
- `reach_once!` (the harness's branch-guarded `assert_reachable!`, in
  `audit/mod.rs`) for a **cause** that fired: a hook, a knob extreme, a fault
  coin, an operation the client drew. It creates no slot when unreached, so
  it can never fail coverage.
- A perturbation never gets a `sometimes`.

## 4. Mind the budget and the identity

2048 assertion slots per campaign process (`MAX_ASSERTION_SLOTS` in the pinned
moonpool's `moonpool-assertions/src/slots.rs`), shared with moonpool's own
internals, and 256 `sometimes_each` buckets (`MAX_EACH_BUCKETS`, `buckets.rs`). Identity is the hash of the message: **never reword
an existing message** (its saturation history resets silently), and never use
a slot, ballot, request id, seed or hash as a bucket key. Count before adding.

## 5. Prove it

Run the sweep (`/sim-sweep`). A new `sometimes` must fire and saturate; a new
`always` should be checked red on a build that breaks it (the
simulation-driven loop, `/simulation-driven-fix`), otherwise it is decoration.
