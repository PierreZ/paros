# paros-wire

The RPC contract (`paros::wire`): 11 protobuf packages, the well-known method ids, the shared request and reply types, and the codecs between `paros-core` types and protobuf. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-wire/` (3,611 lines of Rust, plus about 3,100 lines of `.proto` in `proto/`).

## What it did

It was the lowest layer above `paros-core`. It depended only on `paros-core`, `moonpool-rpc` (with `prost`), `prost`, `tracing` and `moonpool-assertions`. Every other paros crate used it. It was wasm-safe.

- **Build** (`build.rs`): `protox` parses the protos in pure Rust (so no `protoc` is needed, although `AGENTS.md` at the tag still says it is), and `prost-build` generates the messages. `common.proto` is compiled alone and the other ten packages point at it (`extern_path`), so shared types live in one Rust module. A test build derives `arbitrary::Arbitrary` on every generated type and writes `wire_messages.rs`, a list of every message for the property test. The `service` blocks in the protos only document the method set.
- **Packages** (`src/lib.rs`, one `pub mod` each): `common` (ballot, acceptor configuration, leader uuid, journal view, tags), `public` (`paros.v1`: `Write`, `Read`, `SetLeader`, `Truncate`, `Reconfigure*`, the frontend calls), `internal` (consensus messages, `Inspect`, `Retire`, `Handoff`, `Scrub`), `matchmaker`, `system` (registry entries and checkpoint), `fleet`, `machine` (the idle-machine contract: `Identify`, `CellInit`, `Admit`, `Register`, `View`, `Resolve`, `Load`, …), `checkpoint`, `election`, `tenant`, `view`.
- **Methods** (`src/methods.rs`): one moonpool-rpc `RpcMethod` marker per call, served as a well-known endpoint with a permanent id (`0x5041_00xx` public, `01xx` internal, `02xx` matchmaker, `03xx` machine, `04xx` frontend, `05xx` batcher). An incompatible body change bumps the method's `SchemaVersion`, which a server refuses before it decodes. Two dynamic groups (#404 (static vs dynamic endpoints)): `MachineInterface` (`Deliver`, the matchmaker calls and the data plane, one group per incarnation) and `CoordinatorInterface` (`Register`, `JournalRequest`, `JoinUniverse`, one group per coordinator term; no well-known endpoint).
- **Codecs**: `src/consensus.rs` (`message_to_proto`, `message_from_proto` for `paros_core::Message`), `src/matchmaker_codec.rs` (`wire_match_request`, `match_request_from_wire`, … and `Addressed`), `src/codec.rs` (ballot, quorum system, configuration, leader uuid, journal state and view, writer mode, role counts). Every decode returns `Result<_, &'static str>`: malformed input is refused, never a panic.
- **Typed refusals**: `src/refusal.rs` (`MatchmakersRefusal`, `RetireRefusal`, each with `label` and `from_label`, so a label is spelled once), `src/inspect.rs` (`InspectTarget`, `InspectRefusal`: an unset identifier is refused, never read as "the first journal", #243 (dead paths)).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-wire/build.rs` | `MODULES` (package to module), `protox::compile`, `prost_build` with `TEST_DERIVE`, writes `wire_messages.rs` |
| `crates/lib/paros-wire/src/lib.rs` | the 11 `pub mod`s and the re-exports (`Write`, `WriteAck`, `WriteOutcome`, `Read`, `ReadAck`, `SetLeader`, `Truncate`, `FrontendVerdict`, `InspectRequest`, `InspectReply`, `RetireRequest`, …) |
| `crates/lib/paros-wire/src/methods.rs` | `RpcMethod` markers, `WellKnownMethod` ids, `MachineInterface`, `CoordinatorInterface`, `SchemaVersion::new(1)` |
| `crates/lib/paros-wire/src/codec.rs` | `ballot_to_proto`, `config_to_proto`/`config_from_proto`, `WireQuorumSystem`, `quorum_system_*`, `leader_uuid_*`, `journal_state_*`, `journal_view_*`, `writer_mode_*`, `counts_from_proto`, `zoned_members`, `unique_map` (refuses repeated keys) |
| `crates/lib/paros-wire/src/consensus.rs` | `message_to_proto`, `message_from_proto` |
| `crates/lib/paros-wire/src/matchmaker_codec.rs` | matchmaker request, reply, GC and reconfigure codecs |
| `crates/lib/paros-wire/src/refusal.rs`, `src/inspect.rs` | `MatchmakersRefusal`, `RetireRefusal`; `InspectTarget`, `InspectRefusal` |
| `crates/lib/paros-wire/proto/common.proto`, `paros.proto`, `internal.proto` | the journal's own wire: ballot, configuration, `Write` (`journal`, `leader`, `seq`, `records`, `tenant`), `WriteOutcome` (`ACCEPTED`, `DUPLICATE`, `REFUSED`, `TRUNCATED`, …), `ConsensusMessage` |
| `crates/lib/paros-wire/src/tests.rs`, `src/tests/codec.rs`, `src/tests/property.rs` | round trips, malformed input refused, the property test over every message |

## Ideas worth reusing

- **Permanent explicit ids** (`src/methods.rs`): a method id is a fixed number, never derived from a Rust name, and never reused (the old `Init` `0x5041_0303` is retired). Version bumps are per method and refused before decode.
- **One shared `common` package** (`proto/common.proto`): a ballot and an acceptor configuration mean the same on the consensus wire and on the matchmaker wire, so they are declared once and cannot drift. It changes no bytes: protobuf encodes field numbers, not type names.
- **Pure-Rust proto build** (`build.rs`, `protox`): no `protoc` on the build machine, and `cargo install` works.
- **Property test from the proto list** (`src/tests/property.rs`, #1078 (renumber the proto fields)): `build.rs` lists every message, so a new message is tested with no edit. Two properties: a value drawn from bytes round-trips; a decode of any bytes is a value or a refusal, never a panic. Bytes come from a fixed xorshift stream (no RNG dependency). A case it finds becomes a named unit test.
- **Strict decoders**: a missing message kind, a duplicate slot in a catch-up suffix, an unknown quorum system, a journal state whose trim point passes its next seq or whose leader and term disagree, a role count past its cap, a page cursor of journal 0: all refused with a typed reason.
- **Labels spelled once** (`src/refusal.rs`): the driver encodes and the client decodes through the same enum.
- **No defaults for identifiers** (`src/inspect.rs`): an unset identifier is refused unless the call asks for the node alone.
- **Static plus dynamic endpoints** (#404 (static vs dynamic endpoints)): a well-known endpoint reaches whoever serves an address; a dynamic group names one incarnation or one coordinator term, so a call to an ended term fails at the transport. Plain Multi-Paxos still reaches peers by address alone.

Mistakes not to copy:

- **Journal and service in one wire crate** (leak 10): one build compiles 11 packages (`build.rs`, `MODULES`). A journal user cannot take the consensus wire without the registry, fleet, tenant, machine and view records. Split the journal wire (`common`, `public`, `internal`, `matchmaker`) from the service wire.
- **Service ids in the journal wire**: `Write` and `ConsensusMessage` carry `tenant` beside `journal` (`proto/paros.proto`, `src/tests.rs`). This follows leak 5 in `paros-core`.
- **Moonpool in the contract**: `methods.rs` builds on `moonpool_rpc` types (`RpcMethod`, `WellKnownId`, `RpcInterface`), so the wire is not usable without moonpool (leak 12).
- **Stale map**: `AGENTS.md` lists `proto/` twice and says the build needs `protoc`; the build uses `protox`.

## Bugs and scenarios to cover again

- **Any bytes decode to a value or a refusal, never a panic**, for every message: `every_message_round_trips_and_no_bytes_panic_its_decode` (`src/tests/property.rs`).
- **Duplicate slots in a catch-up suffix** must be refused: `protobuf_rejects_duplicate_slots_in_a_suffix` (`src/tests.rs`). A repeated slot in one message must not reach the core as two entries for one slot.
- **A missing message kind** is refused: `protobuf_rejects_a_missing_message_kind`.
- **A page cursor of journal 0** is malformed, never the first page (#439 (operator calls name their journal)): `a_page_cursor_of_journal_zero_is_refused`.
- **The `Accept` leader field stays off the wire** while it repeats the reply address (pinned for the day a proxied Phase 2 sets it): `a_leader_that_is_the_reply_address_stays_off_the_wire`.
- **A journal state keeps its order and its leader/term pair**: `a_journal_state_keeps_its_order_and_its_leader_term_pair`, `a_journal_view_round_trips_and_an_ill_formed_one_is_refused` (`src/tests/codec.rs`).
- **Quorum systems**: every variant round-trips with its own discriminant, and an unknown one is refused (`every_quorum_system_round_trips_with_its_wire_fields`, `each_quorum_system_has_its_own_discriminant`, `an_unknown_quorum_system_is_refused`). The quorum system must be identical on the consensus and matchmaker wires.
- **A party sends its node or its proxy field**: a proxy sends node 0 (which proto3 omits) and its proxy id; a node leaves the proxy field absent. `a_party_sends_its_node_or_its_proxy_field`.
- **Format migration** (#1078 (renumber the proto fields)): the fields were renumbered once and the readers of formats older than 0.2.0 were deleted. A new wire should fix its numbers before the first durable record.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-wire/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-wire

Where to start reading: `proto/common.proto`, `proto/paros.proto`, `proto/internal.proto`, `src/methods.rs`, `src/consensus.rs` (with `src/codec.rs`), then `build.rs` and `src/tests/property.rs`. Its own map at the tag: `crates/lib/paros-wire/AGENTS.md`.
