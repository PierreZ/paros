# paros-roles

The IO drivers of every role: the node loop (colocated leader, acceptor and learner), proxy leader, replica, batcher, matchmaker and frontend. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-roles/` (25,662 lines of Rust).

## What it did

Each role is a `run_*` loop over `P: ParosProviders`. The same code runs in `parosd` (compio) and in `paros-sim` (moonpool). Each loop owns a sans-IO core from `paros-core` (`ColocatedNode`, `ProxyLeader`, `ReplicaNode`, `Matchmaker`, `Batcher`/`Unbatcher`) and does all the IO: persist, send, tick, reply.

Place in the layers: above `paros-core`, `paros-model`, `paros-wire`, `paros-tunables`, `paros-storage`, `paros-net`, `paros-client`, `paros-observe`. Below `paros-coordinator` and `paros-machine`. Programs used it through `paros::roles`.

Main flows:

- **Node loop** (`run_node`, `run_journals` in `src/node.rs`). A `select!` over the RPC edge, peer messages, ticks, flights and shutdown. Many journals in one loop (#188 (many journals per node)), one `ColocatedNode`, store and audit port per journal. Each `Ready` drains in persist, send, learn, acks order. The flush runs off the loop (`Flight`, #613 (settle off the node loop)): one flight per journal, its inputs wait in a backlog, and the backlog is the next group-commit batch.
- **Control plane inside the data loop.** `node/system.rs` and `node/control.rs` fold the registry (cell control journal) and every hosted tenant's control journal. From the folds the loop starts, stops, parks and lingers journals, books proxy leaders and replicas (#765 (booked proxies and replicas)), and moves peer addresses (`node/book.rs`).
- **Client calls.** `Write`, `SetLeader`, `Truncate` are proposed and held until their slot applies (#204 (held calls)), then answered with the fold's verdict. `Read` is a leaderless quorum read (`node/log_reads.rs`), with long-poll and page limits (#241 (read limits)).
- **Operator calls.** `Reconfigure`, `Retire`, `Inspect`, `Handoff`, `ReconfigureMatchmakers`, `Scrub` (`node/operator.rs`, `node/handover.rs`, `node/scrub.rs`).
- **Clean stop** (`node/stop.rs`, #560 (clean Ctrl-C shutdown)): refuse new calls `stopping`, answer held calls, hand each leadership on, wait for flushes.
- **Frontend** (`frontend.rs`, #192 (the frontend)): authorize in names, resolve names to ids, forward and follow redirects itself.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-roles/src/node.rs` | `run_node`, `run_journals` (13 arguments, generic over `CellHost`), `serve_journals`, `RunError`, `BootKind` |
| `crates/lib/paros-roles/src/node/shared.rs` | `NodeLoop` (fixed context of each settle tail), `Shared` (state all journals share), `Shared::step_backlog` |
| `crates/lib/paros-roles/src/node/ready.rs` | The `Ready` IO side: `send_early`, `split_early` (Accept before own fsync), `persist_writes`, `storage_fault_crash` |
| `crates/lib/paros-roles/src/node/flight.rs` | `Flight`, `Flush`: one flush in flight per journal |
| `crates/lib/paros-roles/src/node/batch.rs` | What ends a group-commit batch, the held inputs |
| `crates/lib/paros-roles/src/node/journals.rs` | `JournalStores` (store port: open, provision, `host_on_edge`, `book_sets`), `Journals`, `JournalRt`, quarantine |
| `crates/lib/paros-roles/src/node/control.rs` | `enter_read_only`, `read_only_refuses`, `admit_pool`, `open_journal`, `lingers` |
| `crates/lib/paros-roles/src/node/system.rs` | `ControlPlan`, `ControlFollower`: the control journals' follower |
| `crates/lib/paros-roles/src/node/report.rs` | `maintain` (election timeout draw and backoff), `Deltas`, `Cadence` (re-send backoff, `MAX_DOUBLINGS`) |
| `crates/lib/paros-roles/src/node/draws.rs` | Driver choices: `contest_overwrite`, `abandon_early`, `initiate_handoff`, `delegation_choice`, `stretch_tick` |
| `crates/lib/paros-roles/src/node/grid.rs` | Grid quorum routing: `move_stuck_rounds`, `phase2_column`, `fast_row`, `fast_column`, `slow_members` |
| `crates/lib/paros-roles/src/node/calls.rs` | `Call` (held call), `busy` (write admission), `batch_too_large`, ack builders |
| `crates/lib/paros-roles/src/node/log_reads.rs` | `JournalReads`, `ReadLimits`: quorum read queue and long-poll |
| `crates/lib/paros-roles/src/node/handover.rs` | Driver policy around `MatchmakerReconfigurer`: `stall_budget`, `gc_refusal_behind` |
| `crates/lib/paros-roles/src/node/matchmaking.rs` | `MatchmakerLinks`, the matchmaker RPC tasks |
| `crates/lib/paros-roles/src/node/transport.rs` | `PeerMailbox` (keep-newest per kind, one lane per journal, round-robin), `Outbound`, `LaneOpener` |
| `crates/lib/paros-roles/src/node/edge.rs` | `RpcEdge`, `NodeInbox`, `ReplicaInbox`, `MatchmakerInbox`: the inbound edge, polled by the loop |
| `crates/lib/paros-roles/src/node/shards.rs` | Replication declared as moonpool `shard!` for fault placement (#760 (moonpool owns fault placement)) |
| `crates/lib/paros-roles/src/node/proxies.rs`, `node/replicas.rs` | `BookedProxies`, `BookedReplicas`: roles run inside the node loop on booked machines |
| `crates/lib/paros-roles/src/node/stop.rs` | `Stop`, `Open`, `hand_on`, `refused` |
| `crates/lib/paros-roles/src/node/reply.rs` | `answer`, `match_answer`, `maybe_duplicate`: the one reply seam with its BUGGIFY drops |
| `crates/lib/paros-roles/src/node/host.rs` | `CellHost`, `MachineFacts`, `FormedCell`, `Hosted`: the port to the machine layer |
| `crates/lib/paros-roles/src/proxy_leader.rs` | `run_proxy`, `ProxyConfig`, `ProxyJournal` (#142 (proxy leader)) |
| `crates/lib/paros-roles/src/replica.rs` | `run_replica` (#144 (replica)) |
| `crates/lib/paros-roles/src/batcher.rs` | `run_batcher`, `BatcherSettings`, `BatcherBook` (#145 (batchers)) |
| `crates/lib/paros-roles/src/matchmaker.rs` | `run_matchmaker`, `Host`, `HostedSet`, `SetOpener` (#190 (one set per tenant)) |
| `crates/lib/paros-roles/src/frontend.rs`, `frontend/routes.rs`, `frontend/forward.rs` | `run_frontend`, `spawn`, `Routes`, `Forwarded`, `forward` |
| `crates/lib/paros-roles/src/rig.rs` | Test rig: the shipped loops on moonpool's simulated network |

## Ideas worth reusing

- **Accept before the leader's own fsync** (`split_early`, `node/ready.rs`, #578 (leader sends Accept before its own fsync)). Only `Accept`s leave early, and only from a batch that does not raise the promise. A slot is chosen only by a quorum of durable votes, so a crash loses the leader's copy, never a decision. This is a protocol rule. Model it in TLA+ before a new engine takes it.
- **Group commit by flight** (`node/flight.rs`). One flush in flight per journal. Inputs wait in a backlog. The next batch takes the inputs of one phase only, so each observer sees at most one phase change per batch. Nothing that rests on the batch's durable writes leaves before the flush.
- **Keep-newest mailbox, eviction per kind** (`PeerMailbox`, `node/transport.rs`). A new message evicts the oldest of its own kind, so one kind cannot starve another. One lane per journal, drained round-robin. The doc comment has the seeds that broke plain FIFO and plain keep-newest.
- **Draw randomness only on the loop** (`node/transport.rs`). A BUGGIFY draw in a detached task broke `same_seed_replays_identically`. Draw on the loop, carry the decision in.
- **One quorum read for all waiting reads** (`node/log_reads.rs`, #941 (one quorum read per journal)). Every `Read` that arrived before the quorum read opens rides it. At most one opens per tick.
- **Write admission** (`busy`, `node/calls.rs`, #360 (disk full)). A leader with `admission_rounds` undecided rounds answers `busy`. Not a verdict.
- **Re-send backoff** (`Cadence`, `node/report.rs`, #853 (heal journals and balance the cell)): doubles per unanswered re-send, cap `2^3`, reset on an answer.
- **Batcher never re-sends an unanswered merged write** (`batcher.rs`). It answers it unanswered. A fenced write is never merged (#709 (merge fenced writes)). The frontend has the same rule (`FrontendVerdict::Unanswered`, `frontend/forward.rs`).
- **Clean stop order** (`node/stop.rs`): refuse new calls, answer held ones, hand leadership on, wait for the successor's beat, flush. Bounded by `shutdown_within`.
- **Scrub at the acceptor** (`node/scrub.rs`, #362 (background scrubbing)): read one page, answer ballot and fingerprint only, repair a bad record from the node's own vote.

**Protocol decisions that live in the driver, not the core.** A new journal must move each one into the engine and model it:

- `split_early` (`node/ready.rs`): the early Accept.
- `read_only_refuses` and `enter_read_only` (`node/control.rs`): a read-only journal drops `Accept`, `CatchUpResponse`, `Relinquish` behind the core's back, and sets the election timeout to 0. It still promises in Phase 1.
- `contest_overwrite` (`node/draws.rs`, #376 (overwrite contest), #396 (one contest per pair)): a follower campaigns above an `Accept` that would overwrite a different value it holds at a lower ballot.
- Election backoff in `maintain` (`node/report.rs`): failed campaigns double the base; a probing node does not grow the streak; a sole leader resets it.
- `stall_budget` and `gc_refusal_behind` (`node/handover.rs`): when to abandon a matchmaker handover, when to republish a chosen set.
- `abandon_early`, `initiate_handoff`, `delegation_choice` (`node/draws.rs`): handover abandon, handoff target, proxy delegation.
- `move_stuck_rounds`, `phase2_column`, `fast_row`, `fast_column` (`node/grid.rs`, #533 (stuck grid round), #141 (Phase 2 column), #361 (slow machines)).
- `busy` admission (`node/calls.rs`) and the read queue (`node/log_reads.rs`).
- The flight gate (`node/flight.rs`) and the persist-before-send order of each role.

**Leaks and mistakes not to copy** (leak numbers are rows of the leak table in `README.md`):

- The data loop folds the control plane (leak 1): `node/system.rs`, `node/control.rs`, `run_journals` in `node.rs`. A new driver gets `Serve`/`Stop`/`Peer` from an `Assignments` port and holds no registry.
- Protocol rules in the driver (leak 2): the list above.
- The store port is also an RPC host and registry cache (leak 13): `JournalStores::host_on_edge`, `book_sets` (`node/journals.rs`).
- Shipped code reads named fault switches from `paros_observe::scenario` (leak 7): `lose_verdict` in `node/reply.rs`, `hold_journal`, `lag_follow`, `late_admitter` in `node.rs`, `withhold_gc` in `node/shared.rs`, `stall_proxy` in `proxy_leader.rs`.
- The transport knows message kinds (`node/transport.rs`, 1,734 lines). A transport port should only move bytes.
- Proxy leaders and replicas run inside the node loop on a match of "journal I do not serve" (`node/proxies.rs`, `node/replicas.rs`). Routing by absence is fragile.

## Bugs and scenarios to cover again

- **Overwrite erases a lower copy before Phase 1 reads it** (`contest_overwrite`, #376). Gate: "phase1: a promise quorum reports two different values for one slot and P2c keeps the higher ballot", about 0.25% of seeds. Then a liveness loss on `Grid{2,2}`: two column-mates dueled from round 12 to 1652 (#396).
- **Election duel through deposed leaders**: two matchmaker-deployment candidates traded leadership for 183 rounds (seed 14889077543971178620, `node/report.rs`). And a probe that grew the backoff to 16 s and missed the tail (recipe 17274002723813095473, `node/report.rs`).
- **Handover stall budget that grows with the backoff** (`stall_budget`, replay-main 5639231618807006349, #1033 (overdue reads expire)).
- **Set never learned chosen**: a member frozen at generation 0 with no successor refused GC for the whole tail (`gc_refusal_behind`, replay-main 8742796038547511681).
- **Mailbox starvation**: catch-up responses evicted 983 of 983 times behind a slow link (seeds 14371623759479170018, 13938523914823716398), then evicted by Accept bursts (seeds 12153861921929631187, 9558440018523712995, 1336557888375411500). See `node/transport.rs`.
- **Reads that overflow a lane**: one quorum read per `Read` evicted `PreRead`s and expired reads (replay-main 5604921093031837643, replay-main 18315968079077685493). One read in flight per journal starved reads under loss (sweep seeds 8902432871580852778, 573972565736542941).
- **Disk full** (#360): journal re-opens read-only, keeps promises, refuses Phase 2, never leads, writable after `read_only_ticks`.
- **Crash with Accepts out, own vote not durable** (`hint!("accepts sent, own vote not durable")`, `node/ready.rs`). Also "batch durable, not sent" and "batch staged, not synced".
- **A flight that outlives its journal**: never open a store twice while an orphan flush flies (`node/flight.rs`).
- **Busy matchmaker under re-sends**: 1,212 timeouts and 744 overload refusals (seed 3985738071311278059, `Cadence`).
- **Retire only on the effective floor**, **grid round stuck on a column moves**, **matchmaker set hands over to a spare**: tests in `src/node/tests.rs` (`a_removed_acceptor_retires_only_on_the_effective_floor`, `a_grid_round_stuck_on_a_column_with_a_member_down_moves_to_the_other_column`, `a_matchmaker_set_hands_over_to_a_spare_and_the_cell_keeps_writing`, `a_rolled_back_promise_is_judged_before_the_marker`).
- **Batcher**: a merged write too large is split and re-sent write by write; a fenced write passes alone (`src/batcher/tests.rs`).
- **Frontend**: a write that may have run is `Unanswered`, never re-sent; a stale cell is forgotten (`src/frontend/forward/tests.rs`).

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-roles/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-roles

Where to start reading: `src/node.rs` (module doc and `run_journals`), `src/node/ready.rs`, `src/node/flight.rs`, `src/node/draws.rs`, `src/node/transport.rs`. Its own map at the tag: `crates/lib/paros-roles/AGENTS.md`.
