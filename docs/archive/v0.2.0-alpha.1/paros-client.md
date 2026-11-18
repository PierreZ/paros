# paros-client

The library client of a cell (`paros::client`): the journal policy loops (write, resolve, read, claim, truncate), the writer, the pipeline, the reader, the checkpointer, the election library and the control-plane sessions. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-client/` (18,319 lines of Rust).

## What it did

It sat above `paros-core`, `paros-wire`, `paros-model`, `paros-tunables` and `paros-net`. `paros-observe`, `paros-roles`, `paros-coordinator`, `paros-machine`, the `paros` facade, `parosctl` and `paros-sim` used it. The client was provider-generic (`P: ParosProviders`). The simulation, its linearizability checker and `parosctl` all ran the same code. It drew no randomness: the caller gave every seed, jitter and first server.

The crate held two products in one place (leak 8 in `README.md`):

- **The journal client** (about 5.4k lines): `Client` with its server list, leader hint and redirect rule; `Writer` (a fenced single-writer session); `Pipeline` (several writes in flight); `Reader` (a cursor with gap reports); `multi.rs` (unfenced multi-writer calls); `outcome.rs` (every reply judged once into a typed outcome); `checkpoint.rs` (the checkpoint owner loop); `election.rs` (leader election over a multi-writer journal).
- **The service client** (about 6.3k lines): `bootstrap.rs` and `initialize.rs` (`parosctl init`), `cell.rs` (`cell add-machine`, drain, remove), `fleet.rs` (universe and tenant create/remove), `journals.rs`, `names.rs`, `resolve.rs`, `views.rs`, `load.rs`, `universe.rs`.

Main flows:

1. **Write.** `Client::write` sends one `Write`, follows redirects (`Retarget`), re-asks a busy leader, and returns a `WriteReport`. A retry is always the identical request (same uuid, position and bytes), so the log answers `Duplicate`. A lost answer is `WriteOutcome::Ambiguous`, never "not done".
2. **Resolve.** `Client::resolve` settles an ambiguous write: a read-back first, then identical re-sends, each after a new read-back, up to `retry_budget`. It gives `Resolution::{Written, NotWritten, Truncated, Unresolved}`.
3. **Pipeline.** `Pipeline::open` gives consecutive positions under the writer's uuid; `Pipeline::send` sends through `Writer::write_entry`. Ready writes merge into one write (#709 (merge fenced writes)).
4. **Checkpoint.** `Checkpointer::open` claims, folds to the tail (restoring from a checkpoint at the floor); `checkpoint` writes a `Begin`/`Chunk`/`End` run and truncates below it.
5. **Election.** `Election` campaigns, renews, watches and resigns over a multi-writer journal; `ElectionFold` is the pure rule "the first campaign for the next term wins".
6. **Control-plane sessions.** `FleetSession` and `CellSession` are idempotent step machines (`Step`, `Stage`, `Run`): one write per step, resumed from what the journals hold.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-client/src/lib.rs` | `Client`, `Server`, `Retarget`, `LeaderHint`, `WriteOptions`, `WriteReport`, `Resolution`, `ResolveReport`, `ReadReport`. Policy loops `write`, `resolve` (with private `settle` and `read_back`), `read_any`, `claim`, `set_leader`, `truncate`, `reconfigure*`, `inspect`, `retire`; one-shot `*_attempt` calls. Service hooks `elsewhere`, `with_own_machine`, `rotating_over` |
| `crates/lib/paros-client/src/outcome.rs` | `WriteOutcome`, `SetLeaderOutcome`, `ClaimOutcome`, `ReadOutcome`, `TruncateOutcome`, `ReconfigureOutcome`, `RetireOutcome`; each has a pure `judge` from the reply |
| `crates/lib/paros-client/src/writer.rs` | `Writer` (`Copy`), `WriterOutcome`, `Learned`, `leader_uuid(seed, k)` (splitmix64, never the unset uuid), `Writer::learn`, `Writer::led_from`, `stale_entry`, `stale_truncate_request` |
| `crates/lib/paros-client/src/pipeline.rs` | `Pipeline`, `Ticket`, `Pipelined`, `MERGE_RECORDS` = 256, `MERGE_BYTES` = 256 KiB; pure state `Lanes` (`pub(crate)`) |
| `crates/lib/paros-client/src/reader.rs` | `Reader`, `ReaderOutcome::Gap` |
| `crates/lib/paros-client/src/multi.rs` | `append_entry`, `append_request`, `open_truncate_request` (multi-writer, #241 (leader uuid fence)) |
| `crates/lib/paros-client/src/servers.rs` | `Servers` (`pub(crate)`), `MAX_LEARNED`: a list that only grows; a learned leader is outside the rotation |
| `crates/lib/paros-client/src/observer.rs` | `CallObserver` (`resolving`, `resolved`, per-attempt hooks), `NoObserver` |
| `crates/lib/paros-client/src/checkpoint.rs` | `Checkpointer<S>`, `OpenOutcome`, `LoadOutcome`, `AppendOutcome`, `CheckpointOutcome`, free fn `load`; re-exports `paros_model::checkpoint::*` |
| `crates/lib/paros-client/src/election.rs`, `election/fold.rs` | `Election`, `ElectionTunables`, `Step`, `read_election`, `hand_off`; `ElectionFold`, `ElectionRecord`, `Candidate`, `Leader`, `Change` |
| `crates/lib/paros-client/src/bootstrap.rs`, `initialize.rs` | `cell_init`, `identify`, `admit`, `register`, `discover`, `majority_cell`, `CellServer`, `control_journals_of`; `initialize`, `InitRun`, `InitParams`, `InitRefusal`, `initial_count` |
| `crates/lib/paros-client/src/cell.rs`, `fleet.rs`, `fleet/placement.rs` | `CellSession`, `Removal`; `FleetSession`, `Step`, `Stage`, `FleetRefusal`, `read_directory` |
| `crates/lib/paros-client/src/names.rs`, `resolve.rs`, `journals.rs`, `views.rs`, `load.rs`, `universe.rs` | name resolution (`JournalNames` cache), `Resolve` at the entry endpoint, tenant journal requests, view and load fan-out, `JoinUniverse` |
| `crates/lib/paros-client/src/rig.rs`, `rig/cell.rs` | test rig: a client and fake nodes on moonpool's simulated network; fake nodes judge with the core's `JournalState::apply` and play scripted replies |
| `crates/lib/paros-client/src/tests.rs`, `tests/*.rs` | `policy.rs` (redirects, resolve), `writer.rs`, `pipeline.rs`, `election.rs`, `runs.rs` (step patience), `wire.rs` |

## Ideas worth reusing

- **A retry is the same write.** Every retry re-sends generation, owner, position and bytes unchanged; the journal answers it from the log as `Duplicate`. There is no "retry at a fresh position" anywhere (`src/lib.rs` header). Keep this as an invariant of any new client.
- **Ambiguity is a typed result.** A transport error is `Ambiguous`, never a refusal (`src/outcome.rs`, test `a_transport_error_is_ambiguous_never_a_refusal`). Every reply is judged once, in one pure function per call.
- **Read-back proof of an ambiguous write** (`Client::read_back`, `src/lib.rs`, #564 (client read-back settles a write)). Written only when position, length, records, leader, tail and `led_from` all match. Bytes alone prove nothing; leader alone proves nothing. Not written when the tail did not reach the position under another leader. Truncated when below the floor. This is a precise rule worth a TLA+ property.
- **`led_from` and own-overlap** (`Writer::led_from`, `src/writer.rs`). The proof is withheld when another write of the same uuid shares the positions. Good model of "my own earlier batches can tile the same bytes".
- **A writer's position never moves back** (`Writer::learn`). A stale view that names the writer's own uuid is adopted for ownership only; `next_seq` takes the max.
- **Pipeline rules** (`src/pipeline.rs` header): consecutive positions; an early write waits for the writes in front, then re-sends unchanged; a failure stops new writes until the flight drains; a won claim opens at its view's tail; merges are done here, not in a batcher, because only the pipeline re-sends the merged write unchanged. A `TooLarge` merge splits and is never merged again.
- **Pure state beside async code.** `Lanes` (pipeline), `ElectionFold` (election), `Folder` (checkpoint) are pure and unit-tested. The async loop only moves bytes.
- **Leader uuid derivation** (`leader_uuid`): one uuid per term from the caller's seed; the unset uuid never leads. Fresh uuids are the precondition both read-back proofs rest on.
- **Election as a library over a journal** (`src/election.rs`): the election says who leads; the governed journal's leader uuid is the only fence. The lease is a liveness hint on the watcher's own clock. A read that no server served counts for nothing. A watcher that sees its own id under a uuid it never minted deems that term gone.
- **Explicit misbehaviour.** `Writer::stale_entry`, `stale_truncate_request`, `Client::write_attempt` with a `listen` bound or two targets: a harness asks for bad behaviour by name; the normal path never does it.
- **Idempotent step machines** (`FleetSession`, `CellSession`, `tests/runs.rs`): one write per step, resume from the journal, patience counted from the last written step (#605 (init pending in the recovery tail)).
- **The fake-node rig** (`src/rig/cell.rs`): fake nodes judge with the real core state machine and play scripted rare replies. Cheap and exact for policy-loop tests.

Mistakes not to copy:

- **Journal and service in one crate** (leak 8): `Client::elsewhere`, `with_own_machine`, `rotating_over` (`src/lib.rs`), `WriteOutcome::Denied(paros_model::authz::Denial)` (`src/outcome.rs`), and reconfigure, retire, inspect and scrub on the same `Client`.
- **No model for the client's proofs** (leak 9): `read_back` and the pipeline merge rules are judged only by the simulation. Write the TLA+ first next time.
- **The checkpoint state machine split in two crates** (leak 14): `Checkpointer` here, `Checkpointable`/`Folder` in `paros-model`; `CellSession` and `universe.rs` wire them again by hand.
- **Moonpool in public types** (leak 12): `RpcError` in every judge (`src/outcome.rs`).

## Bugs and scenarios to cover again

- **Two own empty batches tile a four-record write** (hunt seed 8440881997986470091): a read-back proved a write that the journal did not hold as that write. Test `a_read_back_proof_is_withheld_where_two_own_writes_overlap` (`src/tests.rs`).
- **A pipeline reopened behind an unsettled write** (hunt seed 8885773904785394005, before PR #680 (read-back proof behind its own pipeline)): the second lane read back the batch's second record at the tail and was "proved". The tail bounds where a batch ends, never where it starts. Test `a_pipeline_reopened_behind_an_unsettled_write_proves_no_lane_inside_it`.
- **A stale view moved a writer back** from 8 to 6 (sweep seed 130138408517690301 on e5cdfff): the writer rewrote its own positions, and a write past the tail landed. Fixed in `Writer::learn`.
- **A predecessor's identical bytes below `led_from`** (hunt seed 10942456551287199571): a writer that wrote before its claim's answer came back. Guarded by the `led_from` condition in `read_back`.
- **Resolve oracle**: a write resolved `NotWritten` must never land in the leadership it was sent under (#444 (resolve settlement reported)). The history checker hears `CallObserver::resolving`/`resolved`.
- **Ambiguous write behind a write in flight** (#910 (paths that almost never run)): it must wait, then re-send unchanged, and stay ambiguous when a write in front failed. Test `an_ambiguous_write_behind_a_write_in_flight_waits_then_is_sent_again`.
- **Merged write over limits** must split and never merge again. Tests `a_merged_write_over_the_limits_comes_apart` (two places).
- **Election reader livelock** (#640 (election reader livelock), hunt seed 14109283004445105476 on a94cfb9): a leader that truncates at every renewal outran a reader 242 times in 36 s. Fixed with `FLOOR_RACE_WAIT_MS` and `ElectionFold::resume_at_tail`/`floor_seen`/`rewind`.
- **A deposed leader's late renewal at the tail** (hunt seed 10124030878967114087 on 1f1f3c6 with PR #679 (clean Ctrl-C shutdown)) and **a late campaign for a taken term** (witness 1055341407506467323 on 9d93679): no record at the tail anchors a fold on its own. Tests in `src/election/fold.rs` (`a_deposed_leaders_late_renewal_at_the_tail_anchors_nothing`, `a_fold_at_the_tail_rewinds_to_the_floor_and_anchors`).
- **A re-elected candidate kept the old term's interface** (witness 353104033508381057 on 860d6c7; hunt seed 5036195987595974417): tests `a_published_interface_ends_with_its_term`, `a_handed_term_begins_without_the_earlier_terms_interface`.
- **A journal hosted off every founder** (witness seed 452243684152051086, PR #885 (main reds seen on a hunt)): a write must walk the whole server list, not only its rotation. Test `a_write_asks_past_its_rotation_for_a_journal_hosted_off_it`.
- **Reader gaps**: a `truncated` answer resumes at the floor and reports `ReaderOutcome::Gap`, never a silent skip. Test `a_reader_resumes_at_the_floor_and_reports_the_gap`.
- **Checkpoint**: a run with no `End` stays open and is never restored; a record with the checkpoint prefix is never sent as an entry; a node's limits split a run into batches. Tests in `src/checkpoint.rs`.
- **Control-plane races**: fleet `Init` duel (#513 (fleet Init duel)); a tenant waits for a wiped founder (#614 (a tenant waits for a wiped founder)); refused footprint marks the tenant `ERROR` (#476 (tenant footprint)).

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-client/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-client

Where to start reading: `src/lib.rs` (the header, then `resolve`, `settle`, `read_back`), `src/outcome.rs`, `src/writer.rs`, `src/pipeline.rs` (the header and `Lanes`), `src/election.rs` with `src/election/fold.rs`. Its own map at the tag: `crates/lib/paros-client/AGENTS.md`.
