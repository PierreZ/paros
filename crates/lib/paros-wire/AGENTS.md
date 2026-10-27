# paros-wire

The RPC contract: the `proto/` messages (built by `prost-build` in `build.rs`), the well-known methods, the request and reply types, and the codecs between `paros-core`'s types and protobuf. The lowest layer above `paros-core`; wasm-safe.

A layer of the library (#722 (three products)): it depends only on the layers below it (root *Layout*). Programs use it through the facade, `paros::wire`.

## Map

- `methods.rs` → one `RpcMethod` per call, `WellKnownMethod` ids (public `0x5041_00xx`, internal `0x5041_01xx`, matchmaker `0x5041_02xx`, machine `0x5041_03xx`: `Identify`, `FormCell`, `CellInit` `0x5041_0304`, `PrepareCell` `0x5041_0305`, `Admit` `0x5041_0306`, `Register` `0x5041_0307` (#349) and `JournalRequest` `0x5041_0308` (#210) (method ids only: served in the `CoordinatorInterface` group, #404), `View` `0x5041_0309` (#399), `Resolve` `0x5041_030A` (#216), `Load` `0x5041_030B` (#424), `CapacityRequest` `0x5041_030C` (#476 (tenant footprint), served in the `CoordinatorInterface` group), `JoinUniverse` `0x5041_030D` (#232 (multiple cells), served in the `CoordinatorInterface` group); `0x5041_0303`, the old `Init`, is retired; frontend `0x5041_04xx`: `FrontWrite`, `FrontRead`, `FrontTruncate`, `FrontSetLeader`, answered with the plain acks, #192 (the frontend), and `FrontSetJournalTags` `0x5041_0405` (#575 (key:value tags)); batcher `0x5041_05xx`: `BatchWrite` `0x5041_0501` (#145 (batchers)); retired ids never reused). Two dynamic groups (#404 (static vs dynamic endpoints)): `MachineInterface` `0x5041_1001` (`Deliver`, the three matchmaker calls and the six data-plane calls, one per founding member's incarnation; each keeps its well-known endpoint too) and `CoordinatorInterface` `0x5041_1002` (`Register`, `JournalRequest`, `CapacityRequest`, `JoinUniverse`, one per coordinator term).
- `inspect.rs` → `InspectTarget`, `InspectRefusal` → what an `Inspect` asks for: a named journal or the node alone; an unset identifier is refused (#243).
- `codec.rs` → shared scalar codecs (ballot, party, quorum system, config, command).
- `consensus.rs` → `Message` ↔ protobuf · `matchmaker_codec.rs` → matchmaker wire ↔ protobuf.
- `tests.rs` → round-trip tests; malformed input refused.
- `proto/{common,paros,internal,matchmaker,system,machine,checkpoint,election,fleet,tenant,view}.proto` → compiled by `build.rs` with `prost-build`.
- `lib.rs` → the request, ack and refusal types every caller and server shares; `pub mod` per proto package.
- `refusal.rs` → `MatchmakersRefusal`, `RetireRefusal` → operator refusals as wire labels (`label`, `from_label`).
- `proto/` (crate root) → `{common,paros,internal,matchmaker,system,machine,checkpoint,election,fleet,tenant,view}.proto`, compiled by `build.rs` with `prost-build`.

## Tests & gates

- `cargo nextest run -p paros-wire` — `tests.rs`: round trips, malformed input refused.
- Building needs `protoc`: the flake ships `protobuf`; on the web see the root *Environment & Nix*.
