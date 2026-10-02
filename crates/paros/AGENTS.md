# paros

The library: `pub use paros_core::*`, the provider-generic driver, the
in-memory stores, the RPC contract, and the matchmaker driver. Everything here
is written once over moonpool's `P: Providers` and runs unchanged in
production and in simulation. Protocol logic that needs I/O policy lives in
the driver, never in a sim-only path.

## Map

- `driver/mod.rs` `run_node<P, S, H, A>` (the etcd-raft `Node` layer: one journal) and
  `run_journals<P, J: JournalStores, H>` (#188: a static list of journals, one
  `ColocatedNode` + store + audit port each, sharing the edge, the peer lanes and the
  tick; the loop routes every call and every `Deliver` message by its journal) ·
  `driver/journals.rs` `JournalStores` (the list and a per-journal opener),
  `SingleStore`, the per-journal runtime, and the quarantine (a storage fault drops the
  journal's runtime and re-opens it after `quarantine_ticks`; a node with no live journal
  exits with the fault) · `driver/system.rs` `SystemPlan` and the system journals' follower
  (#189: a seed folds its own journals 1 and 2, every other node keeps a long-polling `Read`
  open against a seed; `run_journals` applies the folds — created journals start, tombstoned
  ones stop, registered nodes get a lane, an unregistered sender is refused) ·
  `driver/{boot,ready,report,transport,matchmaking,handover,operator,events}.rs`
  by stage: the format-marker check and the boot report (nothing is
  replayed: there is no application, #186), the `Ready` handshake's I/O side in
  persist-before-send order, post-batch upkeep, the bounded keep-newest
  `PeerMailbox` (one keep-newest lane per journal, drained round-robin, #188;
  with `LaneOpener` and `peer_address`, the lane wiring every driver opens its
  peers through), the matchmaker wire, the
  matchmaker-set handover, the operator RPCs (reconfigure, retire,
  inspect) ·
  `driver/edge.rs` `RpcEdge` (the inbound edge all four drivers serve
  from: a listening moonpool-rpc runtime the loop polls as a `select!`
  arm, never spawned, so a crash drops its listener on the spot) and each
  role's typed inboxes (`NodeInbox`, `ReplicaInbox`, `MatchmakerInbox`) · `driver/reply.rs`
  the one client-reply seam (`answer`, `match_answer`, `maybe_duplicate`) ·
  `driver/config.rs` `DriverTunables` and its production defaults ·
  `driver/log_reads.rs` `JournalReads`, the journal `Read` (#204: confirmed by a
  quorum read first, then served by position from the fold; parked at the tail for
  the call's `wait_ms`, capped by `read_poll_ticks`, re-served after every batch and
  answered empty at the deadline), shared by the node and the replica driver ·
  `driver/calls.rs` the held `Write` / `SetLeader` / `Truncate` calls (#204: proposed
  into a slot, parked on it, and answered by `drain_ready` with the verdict the slot
  folded to — or no verdict when the slot decided something else).
- `hooks.rs` `DriverHooks` (the BUGGIFY prong-1 surface, every method
  defaulting to inert, `NoHooks` for production), `Seam` (four durability
  seams), `HandoffContext`, `Reply`. The `H: DriverHooks` bound on `run_node`
  is deliberately **not** `Send + 'static`: consulting a hook from a spawned
  task must not compile, because a hook answer is a randomness draw and a
  detached task can shift the next run's stream.
- `audit.rs` `Audit` (the observation port, `NoAudit` for production): report
  once, typed, where the matching `tracing` event is; an implementation
  returns nothing, draws nothing, reads no clock.
- `storage/` `LogStorage: Storage` (async seam: every method that may
  touch the device returns a `Send` future; the boot scan loads and verifies,
  the synchronous accessors answer from memory; the format marker
  `is_formatted` / `format`, #147, is what `run_node` judges the operator's
  `BootKind` against — `BootRefusal`, `RunError::Refused`), `MemStorage`,
  `storage_contract_suite` · `matchmaker/{mod,storage}.rs` `run_matchmaker`,
  `MatchmakerConfig`, `MatchmakerStorage: RegistryStorage`,
  `MemMatchmakerStorage`, `matchmaker_storage_contract_suite` ·
  `proxy/mod.rs` `run_proxy`, `ProxyConfig` (#142: the third driver — the
  node contract's Phase-2 subset over the same `Outbound` mailboxes, nothing
  durable, `RunError::Infra` its only exit; its beat evicts unanswered
  rounds on `proxy_round_resends` before re-fanning-out the rest). Every send
  names a `Party` sender and destination (`Outbound::sender`,
  `proxy_queues`), and a node's send to a proxy — a delegation or an
  acceptor's reply — reaches the audit as `sent_to_proxy`; `run_node` takes
  the deployment map's proxies beside its peers · `replica_tier/mod.rs`
  `run_replica` (#144: the fourth driver — the node contract's learner subset
  over a `LogStorage`, the node's boot scan, format marker and durability seams,
  sends catch-up requests and pre-reads, serves clients the public `Read` (a quorum read, #204) from its
  own chosen prefix and nothing else — the module doc says why). `run_node` and `run_proxy` take the deployment's replicas and
  `Outbound::resolve` adds them to every `Audience::Learners` send; `Outbound::learners`
  is the list and their lanes sit in `peer_queues`.
- `rpc/` + `proto/{common,internal,matchmaker,paros}.proto` (messages built
  by `build.rs` with `prost-build`; the transport is **moonpool-rpc**, so the
  wire is deterministic in simulation and `paros` stays wasm-checkable):
  `rpc/methods.rs` one `RpcMethod` marker per call, each a **well-known
  endpoint** (`WellKnownMethod`, method id = well-known id, never reused) —
  the public journal (#204: Write/Read/Truncate/SetLeader, ids
  `0x5041_000B..=0x5041_000E`, each naming a `JournalId` a node refuses unless it serves
  it; Reconfigure/ReconfigureMatchmakers; ids `0x5041_0001..=0x5041_0004`, the old
  Propose/Read/QuorumRead/Compact, and `0x5041_0007..=0x5041_000A`, #185's
  Append/Read/CheckTail/Trim, are retired), the internal contract (Deliver/Inspect/Retire; a
  proxy leader and a replica register their `Deliver` — a replica its
  `Inspect` and the public `Read` too — and a method a role does not
  register is refused `EndpointNotFound`), the matchmaker contract
  (Matchmake/GarbageCollect/Reconfigure) · `rpc/inbound.rs` `Inbound` (a
  request stream decoded into what the loop steps), `ReplySender` (the
  one-shot answer; dropping it is the caller's broken promise),
  `serve_deliveries` (the `Deliver` lane's edge task: ack on enqueue),
  `rpc_config` · `rpc/client.rs` `NodeClient` (the public client: one
  at-most-once attempt per call) and the driver's `MatchmakerClient`.
- `client/` (`pub mod client`, #221) the client's policy over `NodeClient`, provider-generic
  and wasm-safe: `mod.rs` `Client` (servers with node ids, the leader hint, `Retarget`,
  `ClientTunables`; observed one-attempt calls — the request built and reported when called —
  and the policy loops: `write` inside one deadline, `resolve` — read-back, then the identical
  re-send — `read_any` / `read_until`, `claim`, `set_leader`, `truncate`, `reconfigure`,
  `reconfigure_matchmakers`, `inspect`, `retire`) · `outcome.rs` every reply judged once into a
  typed outcome, refusal labels included · `writer.rs` `Writer` (generation, next position,
  stops when superseded; `stale_entry` is the explicit misbehaviour) · `reader.rs` `Reader`
  (a cursor; a truncation is a reported `Gap`) · `observer.rs` `CallObserver` (observation
  only, `NoObserver` for production). The client draws no randomness: every choice is the
  caller's.
- `system.rs` (`pub mod system`, #189) the system journals' entries (`SystemCommand`, one
  record per slot, `proto/system.proto`) and their pure folds: `Directory` (id = `128 +` the
  create's LSN, never reused, name races decided by slot order) and `Registry` (the pool is the
  genesis pool plus every registered node not retired) — the one reading every node and every
  client reading back shares.
- `corruption.rs` the CTRL record classification (`classify_log`).
- `journal/` the durable stores on `moonpool-journal` (`pub mod journal`):
  `JournalStorage` (`node.rs`, `LogStorage`), `JournalMatchmakerStorage`
  (`matchmaker.rs`), `JournalStoreConfig` — a log of write operations folded
  at boot. `frame.rs` one record ↔ one entry (epoch = the record kind,
  tag = identity: `(slot, ballot)`, a ballot; postcard
  payload behind a version byte) · `plan.rs` where a boot's fold starts
  (checkpoint brackets: cut an open one, start at the newest intact one,
  skip damaged copies while the history is on disk, trust the oldest
  strictly when it is not) · `node_image.rs` the node's records, the one
  fold live writes and boot replay share, and the per-kind corruption table
  · `tests.rs` both contract suites, targeted damage and a crash loop under
  two fault models, all on `SimStorageProvider`. The promise and the format
  marker live in the journal's two-copy metadata, flushed before the log.

## Rules local to this crate

- A new driver decision is a `DriverHooks` method with an honest contract;
  consult it only where the answer has an observable effect and only from
  the node loop; report what happened through `Audit`.
- A new tunable is born as a `DriverTunables` field with a default here and
  a `buggify_knob!` draw in `paros-sim`'s `NodeShape`.
- A new durability boundary is a new `Seam` variant.
- Spans are non-optional here (`#[tracing::instrument(skip_all, fields(..))]`
  on the loop stages, handlers and storage impls).
- Storage implementations pass the two contract suites (whose `fresh` /
  `reopen` are async: a disk-backed store opens and scans on the way up);
  the faulty fake the campaign runs on is `paros-sim`'s world-backed store,
  the durable stores are `journal/`.
- Deps: `paros-core` (with its observation-only `serde` derives, for the
  journal records), `moonpool-core` (git pin, `default-features = false`,
  `select`), `moonpool-rpc` (same pin, `default-features = false`, `prost`),
  `moonpool-journal` (same pin), prost, postcard, crc32c. The pin rev is
  repeated in `crates/paros-sim/Cargo.toml` and in the `moonpool-sim`
  dev-dependency here; advance every line together.
- Every paros call is one at-most-once attempt (`try_get_reply`): never
  `get_reply`, whose reconnect retransmission may execute a request twice
  behind the protocol's back.
