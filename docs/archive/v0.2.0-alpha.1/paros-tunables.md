# paros-tunables

Plain-data knobs for every role and for the client, each with a default, a floor and an operator override. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-tunables/` (1,803 lines of Rust).

## What it did

The crate sat just above `paros-core` (its only dependency) and below every other library layer. `paros-net`, `paros-roles`, `paros-machine`, `paros-coordinator`, `paros-sim` and `parosd` read it through the facade `paros::tunables`. It did no I/O.

`DriverTunables` held 62 knobs of one node: tick and election timing, keep-alive and delivery timeouts, inbox and mailbox capacities, resend cadences, paging, the cell election lease, machine liveness, load and balance thresholds, scrub rate, read-only and quarantine ticks, batcher flush. One macro row per knob (`driver_tunables!` in `src/lib.rs`, expanded by `src/table.rs`) gave the field, the default, the floor check and the `PAROS_*` override entry (`DriverTunables::FIELDS`, `TunableField`) that `parosd` reads.

Two profiles: `DriverTunables::default()` is the simulation baseline (50 ms tick). `DriverTunables::production()` is what `parosd` ran (100 ms tick, 10-tick election base), every value inside the range the simulation's knobs draw. `check_floors` refused any profile below a floor and named the field (`BelowFloor`). `limits()` built the core's `paros_core::Limits`, clamped to the core ceilings. `internal_client()` built the `ClientTunables` of a machine's own clients (#457 (hard-coded tunables)).

`ClientTunables` held the client's timeouts, redirect and retry budgets, read paging and the checkpoint policy (`CheckpointPolicy`).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-tunables/src/lib.rs` | The `driver_tunables!` table: `DriverTunables`, its docs and floors; transport constants (`MAX_FRAME_BYTES` = 4 MiB, `DELIVERY_BATCH_BYTES` = 3 MiB, `DELIVERY_BATCH` = 64), `BUSY_MARGIN`, `MAX_READ_RECORDS`; `limits()`, `internal_client()`. |
| `crates/lib/paros-tunables/src/table.rs` | The macro, `TunableField` (`name`, `millis`, `set`), the `Knob` trait (count or milliseconds, saturating). |
| `crates/lib/paros-tunables/src/floors.rs` | `DriverTunables::production` (with a table of each value and its reason), `check_floors`, `BelowFloor`. |
| `crates/lib/paros-tunables/src/client.rs` | `ClientTunables`, `CheckpointPolicy`, `batch_len`. |
| `crates/lib/paros-tunables/src/scrub.rs` | `SCRUB_PAGE`, `SCRUB_PAGE_BYTES`. |
| `crates/lib/paros-tunables/src/tests.rs`, `src/client/tests.rs` | Limits clamping, internal client, batch length. |

## Ideas worth reusing

- **One row per knob** (`src/lib.rs`, `src/table.rs`). Doc, type, default, floor and override come from one place, so no knob can miss its floor or its environment variable. FoundationDB knob style.
- **Floors as typed refusals** (`floors.rs`). An operator override is external input: refuse it with the field name, never assert. The floor test walks every field one below its floor (`each_field_one_below_its_floor_is_refused_by_name`).
- **Chained floors** (`floors.rs`). Lease above renewal, machine down after a missed renewal, replace after down: each bound sits above the one it follows. A cross-field rule is a floor too.
- **Compile-time asserts on constants** (`src/lib.rs`, `floors.rs`): a batch fits a frame, a keep-alive times out before the next ping, the election base clears `2 * HEARTBEAT_TICKS`.
- **The shipped profile is inside the simulated range** (`floors.rs` module doc). The simulation must run the production values.
- **A reason column for each production value** (`DriverTunables::production` doc table).

Leaks and mistakes not to copy:

- **One flat struct for every role.** `DriverTunables` mixes node, coordinator (`balance_*`, `slow_*`, `move_warm_*`), machine liveness and scrub knobs. `BUSY_MARGIN` documents `paros_coordinator::cell::slow`. A layer below the roles knows each role's policy.
- **Many fixed resend cadences** (`match_resend_ticks`, `gc_resend_ticks`, `reconfigurer_resend_ticks`, `proxy_*_resends`, `partition_move_resends`, `election_backoff_doublings`). `docs/architecture` rule 6 (one timing model: one backoff type, timeouts from measured round trips) replaces them.
- **Production values are reasoned, not measured** (`floors.rs` doc says so). Do not carry them as facts.

## Bugs and scenarios to cover again

- **Election base too small**: below `2 * HEARTBEAT_TICKS` a live leader loses the race with follower clocks. Floor of `election_timeout_base`.
- **Election base shorter than a Phase-1 round trip**: a candidate abandons its own round and no leader is ever elected; paged promises make it worse (#428 (paging livelock)). Documented on `election_timeout_base`.
- **Handover stall budget must outlast its resend cadence**: `the_handover_stall_budget_outlasts_its_resend_cadence` in `floors.rs`.
- **Zero capacity panics an mpsc channel**: every capacity floor is 1.
- **A page of zero**: `a_zero_page_is_raised_to_one_and_a_small_page_is_kept`, `a_zero_resend_or_apply_page_is_raised_to_one` in `src/tests.rs`.
- **A first record larger than the byte limit still goes** (no stuck batch): `a_first_record_larger_than_the_byte_limit_still_goes` in `src/client/tests.rs`.
- **An internal client must re-send an ambiguous write at least once**: `an_internal_client_without_a_retry_is_a_programmer_error`.
- **Client frame limit equals server frame limit**, or a large read page fails `ReplyTooLarge` on the client side (doc on `MAX_FRAME_BYTES`).

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-tunables/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-tunables

Where to start reading: `src/table.rs`, `src/lib.rs` (the table), `src/floors.rs`, then `src/client.rs`. Its own map at the tag: `crates/lib/paros-tunables/AGENTS.md`.
