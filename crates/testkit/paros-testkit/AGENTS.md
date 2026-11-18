# paros-testkit

The judges of a paros journal that hold no cell (`publish = false`). They came out of the
deleted simulation harness (`paros-sim/src/audit/`, tag `last-service`) with their tests, on
2026-11-18. Each reports with moonpool's assertion macros, so a red is an `assert_always!`
violation of whatever run drives it: a unit test, a seeded model, or a simulation when one
comes back.

## Map

- `linearizability.rs` → `Attempt`, `Call`, `Seen`, `check`, `Verdict`, `PAGE_RECORDS` → every
  attempt a client made at `Write`, `Read`, `SetLeader` and `Truncate`, checked against the
  sequential model of a journal (`docs/architecture/02-data-plane.md` §2) by a Wing and Gong
  search with Lowe's memoization. The model is its own and never calls `paros-core`'s
  `JournalView::apply`.
- `linearizability/reference.rs` → plain Wing and Gong with no pruning (tests only): the
  checker's prunings are compared with it on 100,000 small histories.
- `journal_model.rs` → `JournalModel`, `AckedWrite`, `record_hash` → the verdict each node
  reached at each slot it walked, checked against one model of the journal state machine.
- `journal_model/tests.rs` → its unit tests.
- `lib.rs` → `user_command_hash`, `splitmix` → the record hash and the mixer both judges share.
- `testing.rs` → `red`, the verdict probe over moonpool's violation counter (tests only).

## Rules

- A judge never loosens in the PR that changes the behaviour it judges (root `AGENTS.md`).
- Never reword an assertion message: the slot is the hash of its message.
- `PAGE_RECORDS` (256) is the most records a read page carries; a new journal server that
  pages differently changes it here.
