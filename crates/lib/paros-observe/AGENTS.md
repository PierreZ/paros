# paros-observe

What a run reports about itself, and changes no behaviour: the `Audit` port the simulation's oracles implement, a machine's metrics, the named BUGGIFY locations (`scenario.rs`) and the trace helpers.

A layer of the library (#722 (three products)): it depends only on the layers below it (root *Layout*). Programs use it through the facade, `paros::observe`.

## Map

- `scenario.rs` → `WITHHOLD_GC`, `HOLD_JOURNAL`, `LOSE_VERDICTS`, `LAG_FOLLOW`, `STALL_PROXY` (a proxy drops the acceptors' answers, #341), `RESIGN_DELEGATING` (a leader that holds delegated rounds resigns and campaigns again, #341), `ADMIT_LATE` (half the nodes, drawn at boot, admit the registry's pool 2 s late, #387 (unpooled joiner)), `HAND_OFF` (a transferable leader hands off on about a third of its beats, #484 (silent driver fault sites)) → the named BUGGIFY locations a harness forces per seed (`buggify_named!`, `set_activation`, #318 E). Every other choice is an inline `buggify_fault_with_prob!` (send seam `paros-roles/src/node/transport.rs`, reply seam `paros-roles/src/node/reply.rs`, picks and handoff `paros-roles/src/node.rs`, #318), a grid leader that tears one slot's Phase-2 column and resigns (`tear_column` in `paros-roles/src/node/ready.rs`, #396), and one plain `buggify_with_prob!` that stays on in the recovery tail: an acceptor about to overwrite a different value campaigns first (`contest_overwrite`, #376), once per `(slot, held value, incoming value)` (`JournalRt::contested`, #396). The durability moments are inline `hint!`s in `paros-roles/src/node/ready.rs`, `paros-roles/src/node.rs` (a journal re-opened after its quarantine, before its next sync, #348), `paros-roles/src/matchmaker.rs`, `paros-roles/src/replica.rs` (#297) and the journal stores (#294).
- `audit.rs` → `Audit`, `NoAudit` → the observation port; `commit_started` and `commit_ended` bracket each sync of a node's batch (#599 (durability mode per journal): a harness budgets a cut inside a `Batched` commit); `call_received` and `call_answered` report each data-plane call a machine counts, only while it records metrics (#366 (metrics)).
- `metrics.rs` → `Metrics` (`off`, `on`; `registry`, `families`, `render`, `ready`, `usage`), `CallKind`, `CallOutcome`, `Family`, `FamilyKind`, `Phase`, `JournalUsage`, `Totals` → a machine's metrics in a plain `prometheus` registry (#366 (metrics), §3.6): counters and gauges only, no histogram (decided on 2026-10-15); calls, answers by outcome and their seconds, bytes per call and journal, the applied and chosen slots, the settles and their seconds, the busyness window, its heartbeat and the phase; observation only, never read by the code that serves; a journal's labels are its names and its ids in hex. `metrics/tests.rs` pins them.
- `events.rs` → the pure helpers that turn a domain value into the stable field a trace or an `Audit` callback carries (hashes, message labels, the route triple).

## Tests & gates

- `cargo nextest run -p paros-observe`.
- The audit observes, never perturbs: no return value, no randomness, no clock.
