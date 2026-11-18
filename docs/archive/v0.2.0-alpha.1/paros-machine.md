# paros-machine

One machine's whole life: format, wait for a cell, form it by a single-decree Paxos (`cell init`), or be admitted, then serve. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-machine/` (11,055 lines of Rust).

## What it did

The top library layer below the `paros` facade. It depends on every crate below it, `paros-roles` and `paros-coordinator` included. `parosd` and `paros-sim` both call `run_machine` with a bare `ProviderDisk` over their storage provider (#246 (machine lifecycle), #294 (no sim wrapper)). Programs used it through `paros::machine`.

Lifecycle (`lifecycle.rs`, `run_machine`):

1. **Format once.** An empty disk mints a random `node_id` (#225 (control hierarchy)) and writes the `MachineRecord`. A disk with stores and no record is amnesia and is refused. A class is fixed at format. `PAROS_ROLES` may widen to `all` at a restart, never narrow.
2. **Wait** (`wait.rs`, `wait_for_cell`, #196 (idle machine)). Serve `Identify`, the cell decree as an acceptor (`PrepareCell`, `FormCell`), `CellInit` as its proposer, and `Admit`. With a pinned root key, each call needs an `admin` token (`AdminPin`, `pin.rs`, #245 (Biscuit tokens)).
3. **Serve.** A formed machine (`FormedCell`) runs `paros_roles::node::run_journals` over its plan journals, keeps answering the decree from its record, teaches the decree to members that missed it (`Teacher`, `formed.rs`), registers its address with the coordinator (`register.rs`, #349 (address books from the registry)), campaigns for the cell coordinator, and serves `View`, `Load`, `Resolve`. An admitted machine (`AdmittedMachine`, `admitted.rs`, #216 (finish bootstrap)) runs the node loop over the journals placed on it, follows the registry into its cache (`follow.rs`, #211 (cached registry fold)), and campaigns once the election journal grows onto it.

**`cell init` is a single-decree Paxos on the `CellPlan`** (`cell_init.rs`, #277 (cell init as a decree)). It uses `paros_core::decree::Decree` with `Phase1Rule::EveryAcceptor`:

- Phase 1 (`PrepareCell`, ballot = fresh random `init_id`): every listed machine must promise. So every vote still on a disk is heard.
- Adopt the highest reported vote (P2c), else draw a plan (`draw_plan`). A vote for another list is another cell: refuse `other_cell_init`.
- Phase 2 (`FormCell`): a majority of accepts chooses. Accepting is forming: format the plan's journals, then record the vote (`CellLedger::form`).
- A listed address that now answers as another `node_id` is a wiped member. It never votes as the old one. If a majority is wiped, refuse `cell_lost`.
- A proposer opens each ballot above its own durable promise and above any promise that refused it.

The `CellPlan` names the cell id, the initial members (id and address), the cell control journal, the fleet control journal, and the election journal (#240 (cell coordinator election)).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-machine/src/lib.rs` | Module doc on the decree; `CellMachine` (`Formed`/`Admitted`), its `CellHost` impl (`serve`), `candidate_plan`, `serve_candidate` |
| `crates/lib/paros-machine/src/lifecycle.rs` | `run_machine`, `MachineSettings`, `MachineAddresses`, `MachineError`, `DiskLedger` (the disk `CellLedger`) |
| `crates/lib/paros-machine/src/wait.rs` | `wait_for_cell`, `CellLedger` trait (`promised`, `vote`, `promise`, `format`, `form`), `Joined`, `answer_admit` |
| `crates/lib/paros-machine/src/cell_init.rs` | The decree proposer: `propose`, `attempt`, `decree_over`, `fold_promises`, `fold_form`, `intact`, `draw_plan`, `majority` |
| `crates/lib/paros-machine/src/formed.rs` | `FormedExt`, `Teacher`, `form_ack` (a formed member's Phase 2b from its record); `FormedCell` itself is in `paros-roles/src/node/host.rs` |
| `crates/lib/paros-machine/src/admitted.rs` | `AdmittedMachine`; `Admission` is in `paros-model/src/machine.rs` |
| `crates/lib/paros-machine/src/disk.rs` | `ProviderDisk`: atomic record rewrite (stage, sync, rename, sync dirs), `read_durable`, amnesia probe |
| `crates/lib/paros-machine/src/stores.rs` | `MachineStores`, `AuditScope`; a created journal is formatted (`paros_storage::provision::provision_store`), then its `created` record line is written |
| `crates/lib/paros-machine/src/matchmakers.rs` | Matchmaker host on the node's edge; `SetBookings`; the `sets` file is the commit point |
| `crates/lib/paros-machine/src/register.rs` | `Register` loop to the coordinator; stops when the registry holds the address |
| `crates/lib/paros-machine/src/follow.rs`, `cache.rs` | Registry follow for admitted machines; `spawn_writer` for the durable `CachedRegistry` |
| `crates/lib/paros-machine/src/resolve.rs`, `resolve/entry.rs` | `Resolver`, `booked_on`: `Resolve` and `ResolveEntry` (#233 (resolver)) |
| `crates/lib/paros-machine/src/views.rs`, `load.rs` | `View` (#399 (views)), `Load` windows (#424 (busyness metrics)) |
| `crates/lib/paros-machine/src/inspect.rs`, `inspect/` | Offline store inspection (#365 (offline store inspection)) |
| `crates/lib/paros-machine/src/plan_shard.rs` | The plan declared as a moonpool `shard!` (#1179 (the kill policy as shards)) |
| `crates/lib/paros-machine/src/pin.rs` | `AdminPin` |

The record format is `MachineRecord` in `paros-model/src/machine/record.rs`: a text file with `created`, `deleted`, `admitted` and other lines.

## Ideas worth reusing

- **Bootstrap by consensus, not by a leader** (`cell_init.rs`). Phase 1 needs every member, Phase 2 a majority. Any listed machine can finish an interrupted `init`. No lock is left by a crash. This is a good kernel for `paros-register`.
- **Wiped member detection by identity** (`intact`, `cell_init.rs`). A new `node_id` at a planned address is no accept. A random identity at format makes "a wiped machine never rejoins" true by construction.
- **Formed vote is final** (`formed.rs`). A formed member answers every ballot with its vote and never moves its promise. It still treats another plan below its vote's ballot as a preemption (witness seed 9979247141707847612).
- **Learner catch-up for a decree** (`Teacher`, `formed.rs`). A formed member re-sends Phase 2a (same plan, same ballot) to a member that answers idle. The token stays in memory only.
- **Commit point after format** (`stores.rs`, `matchmakers.rs`). Format the store, then write the record line. A crash between the two formats again. A store the record names and the disk lost is amnesia, never formatted again.
- **Durable read before act** (`read_durable`, `disk.rs`). Sync the directories on read, so a power loss never takes the machine back to an older record it already acted on.
- **Record rewrite protocol** (`disk.rs`): stage, sync, rename, sync every directory on the way.
- **Register stops on two signals** (`register.rs`): the coordinator's answer, or its own read of the registry. The answer often arrives after the call gave up.

**Protocol decisions here that a model must cover**:

- The cell decree is a second Paxos driver outside the journal (leak 15 in `README.md`). `wait.rs` drives an `Acceptor`, `cell_init.rs` drives a `Decree`, with their own store (`CellLedger`) and wire messages. Model it as a register (see `specs/tla/cas-register`, `specs/tla/config-register`).
- `Phase1Rule::EveryAcceptor` with a Phase-2 majority, and the wiped-member rule. The intersection holds only while fewer than a majority are wiped.
- Ballot choice (`draw_ballot`): a random `init_id` when nothing bounds it, else a random step above the floor (its own promise, or a promise that refused an earlier ballot). The node id breaks ties.
- The teacher's re-send of Phase 2a at the old ballot.
- Hold of a moved initial member: its node loop beats no journal and it does not campaign until it is registered (#390 (renamed founding member), #506 (renamed founder not registered)), except a lone-member plan (`lib.rs`).

**Mistakes not to copy**:

- `MachineRecord` keeps the promise and the vote in a text file with no checksum (`paros-model/src/machine/record.rs`). Bootstrap consensus state lives outside the storage contract (leak 15).
- `CellMachine::serve` (`lib.rs`) wires views, registration, the coordinator's candidacy, the frontend and the matchmaker host by hand. A machine layer should host services through ports, not know each one.
- Re-exports glue layers together: `paros-machine` re-exports `paros_coordinator::tenant::sets::{SET, set_of}` and `paros_roles::node::host::*` (`lib.rs`).

## Bugs and scenarios to cover again

- **Member missed `FormCell`** stayed idle until an operator ran `init` again; with one more member wiped the cell lost quorum (seeds 7461995534021400970, 16403044264830861989, `formed.rs`). Test: `a_member_that_missed_the_decree_is_taught_by_its_peer` (`src/lifecycle/tests.rs`).
- **Two concurrent `cell init`s** refused `other_cell_init` over the cell they formed (seed 9979247141707847612, `form_ack`).
- **Proposer holds a vote while `in_init`** and must refuse `Admit` (recipe 128295:9515285717494050093, `answer_admit` in `wait.rs`).
- **Record renamed, directory not synced**: a cell formed that the simulation board never saw voted (witness 15405975583108970484, `disk.rs`, `hint!("machine record renamed, not synced")`).
- **Roles narrowed after power loss** (replay-main 11463140625769350733, `read_durable`). Tests: `a_formed_machine_never_takes_fewer_roles`, `a_machine_promised_in_cell_init_never_takes_fewer_roles`.
- **Amnesia**: `stores_without_a_record_are_amnesia_and_refused`, `a_damaged_record_is_refused` (`src/lifecycle/tests.rs`).
- **Created journal formatted, not recorded** (`hint!` in `stores.rs`, #446 (a gate that never fires)). A storage fault while provisioning must quarantine, not park for good (witness 16441966175508397884, recipe 11674261066061092807).
- **Cache write in flight at a crash** may land (replay-main 924061867710104752, `cache.rs`).
- **Renamed initial member** registered late; every answer came after its call ended (recipe 5368498764125928358, `register.rs`, #479 (renamed founder not registered)). A lone renamed member found no coordinator for 100 s (recipe 17745292072057130742, `lib.rs`).
- **Admitted candidate with a wiped initial member** never let the election settle (recipe 10537902676925133085, `candidate_plan` in `lib.rs`).
- **Decree unit tests** (`src/cell_init/tests.rs`): `every_member_must_promise_before_a_plan_is_selected`, `of_two_reported_votes_the_highest_ballot_is_adopted`, `a_vote_above_the_ballot_preempts_it`, `another_cells_vote_alone_refuses_the_list`, `a_new_machine_at_a_members_address_is_no_accept`, `a_majority_of_accepts_chooses_the_plan`.
- **Wait tests** (`src/wait/tests.rs`): `a_decree_preempted_by_a_member_promise_reopens_above_it`, `a_receiver_opens_above_its_own_promise`, `a_member_whose_format_or_vote_fails_forms_nothing_and_is_unreachable`, `a_pinned_machine_refuses_every_call_without_the_admin_token`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-machine/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-machine

Where to start reading: `src/lib.rs` (module doc), `src/cell_init.rs`, `src/wait.rs`, `src/formed.rs`, `src/lifecycle.rs`. Its own map at the tag: `crates/lib/paros-machine/AGENTS.md`.
