# paros-tunables

The driver's and the client's tunables and their floors: `DriverTunables`, `ClientTunables`, the transport constants they default to, `BelowFloor`, and `DriverTunables::limits()`, the core's `Limits` (#730 (paros-core rework)). Plain data, no I/O.

A layer of the library (#722 (three products)): it depends only on the layers below it (root *Layout*). Programs use it through the facade, `paros::tunables`.

## Map

- `lib.rs` → `DriverTunables` → every driver cadence/budget (`partition_move_resends`: a grid round re-sent that often moves to the next column, the budget doubled per move up to 64 times (`move_budget`), `move_stuck_rounds` in `paros-roles/src/node.rs`, #533 (move a stuck round)) (the cell election's lease, renewal and compaction bound too, #240; a machine's own clients, `internal_client()`, and the `Resolve` answer bound, #457 (hard-coded tunables)); `default()` is the sim's baseline · `floors.rs` → `DriverTunables::production`, `check_floors`, `BelowFloor` → the shipped profile and the floors (#209).
- `floors.rs` → the production profile of `DriverTunables` and the floors every profile must clear (#209).
- `scrub.rs` → the scrub page's ceilings: what one acceptor answers to one `Scrub` page.

## Tests & gates

- `cargo nextest run -p paros-tunables` — the floors and `limits()`.
- A new tunable is a `DriverTunables` field with a default here and a `buggify_knob!` in `paros-sim`'s `NodeShape`; it documents its floor.
