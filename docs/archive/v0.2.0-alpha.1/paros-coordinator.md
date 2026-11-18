# paros-coordinator

The cell, tenant and universe coordinators: one elected term that watches machines, places journals, moves them, and changes their membership. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-coordinator/` (13,820 lines of Rust).

## What it did

A coordinator is a client of the cell. It sits above `paros-client` and `paros-roles`, and below `paros-machine`, which spawns it beside the node loop (`cell::spawn`). Programs used it through `paros::coordinator`.

**Cell coordinator** (`cell.rs`, `cell/campaign.rs`, #240 (cell coordinator election)). Every member in force of the election journal campaigns in a task (`Candidacy`). A won term:

1. installs its uuid on the cell control journal (`Client::claim_over`, only over the leader it read, #653 (election term to installed leader));
2. folds the registry, finishes admissions, joins the universe (`universe.rs`);
3. publishes its `CoordinatorInterface` and renews the lease while each duty runs (`renewing`, #542 (coordinator starves its lease));
4. runs one watch round per renewal period until its first write that does not land.

A watch round (`cell/watch.rs`, `Watch`, #211 (failure detector)) `Identify`s every machine and writes changes only: `MachineUp`, `MachineDown` after `machine_down_after`, gone after `machine_replace_after` outside a maintenance window (#475 (re-place after the Down bound)), `MachineAdmitted`, `MachineSpace` (#360 (disk full)), `MachineSlow` (`cell/slow.rs`, #361 (slow machines)). Then, in this order (the chain in `cell/campaign.rs`), one step each, in turn over keys (`turn.rs`): balance, moves, modes, sets, footprints, roles, scrubs, controls. A step that returns `None` (its write did not land) ends the chain.

**Tenant coordinator** (`tenant.rs`, `TenantDesk`, #210 (tenant control journals)). The same term is the single writer of every hosted tenant's control journal. It claims each one under the term uuid, answers `JournalRequest` (create, delete, configure) from the recorded outcome or writes it, and places a create with `paros_model::placement` (#474 (journals on admitted machines)). Slots are booked through the term's own cell session (`cell/bookings.rs`, `grant`, #476 (tenant footprint)).

**Membership steps** (all in `tenant/`). Each step starts from what the registry and the journal's leader hold now, so a later term finishes what an earlier one started:

- `moves.rs` (`Moves`): off a draining, gone or slow machine. Book a slot, warm the new member (`Warm`), hand leadership off a leaving machine (`hand_off`), `Reconfigure`, wait for the GC watermark, record `Placed`, `Retire` the old member, release the slot.
- `modes.rs` (`Modes`, #214 (tenant modes)): change quorum shape to the desired mode, same steps.
- `sets.rs` (`Sets`, #501 (matchmaker sets off the founders)): size a tenant's matchmaker set to `2F+1`, book slots, `ReconfigureMatchmakers`, release slots once the set in force is the target.
- `controls.rs` (`Controls`, #685 (special tenants are tenants)): keep each control journal at `2F+1`.
- `roles.rs` (`Roles`, #604 (role counts)): frontend, proxy leader, batcher, replica slot counts.
- `balancing.rs` (#687 (balance a cell)): runs the pure `paros_model::balance`, records `BalanceStarted`/`BalanceAbandoned`, asks a `Handoff`.
- `scrub.rs` (`Scrubs`, #362 (background scrubbing)): page through every acceptor, compare fingerprints, record `TenantCommand::Scrubbed`.

**Universe** (`universe.rs`, #513 (fleet Init duel), #225 (control hierarchy)): writes `JoinFleet`, a tenant's `GrantBookings`, `HostTenant`, `DropTenant` into the cell control journal.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-coordinator/src/cell.rs` | `serve_term`, `TermDuty`, `election_tunables`, `cell_client`, `follow_book` |
| `crates/lib/paros-coordinator/src/cell/campaign.rs` | `Candidacy`, `spawn`, `successor`, `chaos`, `renewing`; the BUGGIFY decisions of a candidacy (`stall`, `hand_off`, `quit_mid_move`, `stray_replica`, ...) |
| `crates/lib/paros-coordinator/src/cell/watch.rs` | `Watch` (`round`, `gone`, `held`), `Probe` |
| `crates/lib/paros-coordinator/src/cell/slow.rs` | `SlowJudge`, `SlowBounds`, `Sample`, `Judged`: pure, with hysteresis and a minority cap |
| `crates/lib/paros-coordinator/src/cell/bookings.rs` | `grant`, `decide`, `BookingRequest`, `BookingAnswer`: idempotent slot grants |
| `crates/lib/paros-coordinator/src/cell/footprints.rs` | `Footprints` |
| `crates/lib/paros-coordinator/src/cell/answers.rs` | `Register` (#349 (address books from the registry)) and journal requests |
| `crates/lib/paros-coordinator/src/tenant.rs` | `TenantDesk`, `Unread` |
| `crates/lib/paros-coordinator/src/tenant/moves.rs` | `Moves`, `Warm`, `hand_off`, `retire`, `Listed` |
| `crates/lib/paros-coordinator/src/tenant/sets.rs` | `Sets`, `SET`, `set_of`, `landed`, `special_size`, `odd_short`, `down_targets`, `quiet` |
| `crates/lib/paros-coordinator/src/tenant/controls.rs` | `Controls`, `ControlStep`, `must_serve`, `holds_shrink` |
| `crates/lib/paros-coordinator/src/tenant/modes.rs` | `Modes`, `change_of` |
| `crates/lib/paros-coordinator/src/tenant/roles.rs` | `Roles`, `RoleStep`, `count_steps` |
| `crates/lib/paros-coordinator/src/tenant/scrub.rs` | `Scrubs`, `ScrubPace`, `judge`, `PAGES_PER_STEP`, `FLUSH_PAGES` |
| `crates/lib/paros-coordinator/src/universe.rs` | `joining`, `tenant_steps`, `follow`, `directory_reader`, `join` |
| `crates/lib/paros-coordinator/src/turn.rs` | `next`, `next_entry`, `order`: the round-robin rule |
| `crates/lib/paros-coordinator/src/fixture.rs` | Test registry folded one `SystemCommand` at a time |

## Ideas worth reusing

- **Every step restarts from durable state** (all of `tenant/`). A step reads the registry, the tenant journal and the leader, then does one action. No step keeps a plan in memory across rounds. A new term finishes any half-done move. Keep this shape for any control loop.
- **One rule of turns** (`turn.rs`). Each step takes the next key after the last one, wraps, and so one stuck journal cannot starve the others.
- **Idempotent grants** (`cell/bookings.rs`). A request has an id; a decided id is answered from its record; a refusal at apply is recorded under the same id.
- **Term fenced by uuid** (`cell.rs`, `tenant.rs`). Install with `SetLeader(uuid, current)`; stop at the first write that does not land; never fight an admin fence.
- **Renew while a duty runs** (`renewing`, `cell/campaign.rs`). A long install or watch round must not starve the lease.
- **Hand the term on at a clean stop** (`successor`, `cell/campaign.rs`): name the next candidate by id.
- **Pure judges with unit tests** (`SlowJudge`, `Watch::gone`, `bookings::decide`, `count_steps`, `change_of`, scrub `judge`). The async glue is tested only by the simulation (see `AGENTS.md`, #827 (ub-mode: paros-coordinator)).
- **Scrub never uses a quorum read** (`tenant/scrub.rs`): a quorum read hides rot on the other copies. Progress is a journal entry, so a new term resumes.
- **Slow is not down** (`cell/slow.rs`): RTT against four times the round median, busy ratios, `slow_rounds` in a row, at most a minority held slow.

**Protocol decisions here that a model must cover.** The safety of a membership change is split between the core and this crate (leak 3 in `README.md`):

- `retire` (`tenant/moves.rs`) builds `RetireRequest` from the leader's `gc_watermark` in `InspectReply`. The coordinator decides when an old member may forget.
- `landed` (`tenant/sets.rs`) decides that a matchmaker handover is in force from `matchmaker_generation` and the member list.
- `holds_shrink` and `must_serve` (`tenant/controls.rs`): never shrink while a kept member is down or never registered; never grow onto down newcomers; newcomers and kept members must answer `Inspect` as themselves (PR #937 (shrink proves kept members)).
- `down_targets` (`tenant/sets.rs`): never hand a set onto a machine the registry holds down (#1038 (stall budget and election backoff)).
- `odd_short` and `special_size` (`tenant/sets.rs`, #846 (set below 2F+1)): a set stays odd.
- Wait for the GC watermark to reach the new configuration before `Placed` and before release (moves, modes, controls).
- Hand leadership off a leaving machine before the reconfigure (`hand_off`, `tenant/moves.rs`).

A new design should move these rules into a journal admin port ("move members from A to B and report progress") and model it, for example with `specs/tla/retire-fence` and `specs/tla/handover-gc`.

**Mistakes not to copy**:

- Shipped code reads a named fault switch: `paros_observe::scenario::quit_mid_move()` at `cell/campaign.rs` (leak 7).
- The coordinator reads Paxos internals from `InspectReply` (`gc_watermark`, `config_ballot`, `matchmaker_generation`), see above (leak 3).
- The state machine over a journal is wired again by hand here (`TenantDesk`, `universe.rs`, and `CellSession` in `paros-client/src/cell.rs`) (leak 14). Use one `paros-rsm`/`paros-term` recipe.
- One term does everything for every tenant. Steps share one watch round, so one slow call delays all steps.

## Bugs and scenarios to cover again

- **Operator claim duel**: `init` claimed the cell control journal, fenced the coordinator, the next term fenced `init`, and `init` never ended (witness 1522711066683701314, `universe.rs`, #513).
- **Lease starved by a long duty**: every term of a slow cell was deposed before it served (witness 17167272764625629279, `renewing`). Double renewals on disk (replay-main 10435710832576718291).
- **Lost resignation plus slow install in the tail** (replay-main 94182577319794271, `chaos`).
- **Shrink onto a dead quorum**: drained initial member while another was wiped; the registry shrank onto the live one and the wiped one (recipe 14421242880286626060). Election journal shrank onto a member that never registered (replay-main 17790034300954997751). Growth onto two down newcomers left four members with two live (replay-main 1808614973012915750). All in `tenant/controls.rs`.
- **Set handover onto a down or wiped target** froze the set and blocked every move of the tenant (recipe 16003761340205084037, #1038, `down_targets`).
- **Set of two** survived no failure (witness 10642616994586821844, `odd_short`).
- **Set held for a down machine** blocked plan journal growth past the tail (replay-main 15889736282838719514, `quiet`).
- **Cold move in the tail** missed the placement deadline (replay-main 17719190489388220565, `Warm::judged`).
- **Create with no storage machine** panicked the coordinator (witness 5117945864298433250, `tenant.rs`). **Delete missed by catch-up** freed a name too early (seed 4507917792168049601, `tenant.rs`).
- **Dropped tenant** must be refused by every term (#395 (dropped tenant refused)); a short catch-up answers `unavailable` (#445 (ignored load result)).
- **Watch unit tests** (`src/cell/watch/tests.rs`): `a_down_machine_silent_past_the_bound_is_gone`, `a_maintenance_window_holds_a_down_machines_roles`, `a_reboot_widens_its_roles_never_narrows_them`, `a_machine_whose_fold_stopped_below_the_tail_is_stuck`, `a_retired_initial_member_is_never_marked_down`.
- **Move unit tests** (`src/tenant/moves/tests.rs`): `a_handoff_goes_to_a_member_of_both_configurations`, `a_cold_member_holds_its_move_back_at_most_the_warm_rounds`, `a_waiting_move_gives_its_turn_to_one_that_steps`.
- **Booking tests** (`src/cell/bookings/tests.rs`): `a_recorded_request_is_answered_from_its_record`, `a_slot_on_a_machine_that_does_not_serve_is_refused`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-coordinator/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-coordinator

Where to start reading: `src/cell.rs` (module doc), `src/cell/campaign.rs`, `src/cell/watch.rs`, `src/tenant/moves.rs`, `src/tenant/sets.rs`. Its own map at the tag: `crates/lib/paros-coordinator/AGENTS.md`.
