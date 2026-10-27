# paros-net

What every role shares to talk: addresses and name resolution, the transport and its sessions, the RPC edge (`Inbound`) and its clients (`NodeClient`), the runtime providers, and TLS (feature `tls`, PR #509 (mutual TLS)).

A layer of the library (#722 (three products)): it depends only on the layers below it (root *Layout*). Programs use it through the facade, `paros::net`.

## Map

- `transport.rs` → `Transport` (`Plaintext`, `Tls` with feature `tls`), `RuntimeFuture` → how a runtime's sessions travel (not the peer lanes of `paros-roles/src/node/transport.rs`); a parameter of every listening driver and a field of `FrontendSettings`; `client_only` builds a client runtime (parosctl, the sim's clients) (PR #509 (mutual TLS)).
- `tls.rs` (feature `tls`, off by default so the crate stays wasm-safe) → `Role`, `identity` (`spiffe://<universe>/<role>/<name>`), `leaf`, `SERVER_NAME` (`paros`, every serving leaf proves it, every client expects it), `ClientCertificates`, `Material`, `Files` (the three PEMs, all or none), `Pem` (each a file or the PEM text itself, `Pem::pick`), `serving`, `dialing`, `ProviderUtc` → moonpool-rpc's TLS and certificate authority (moonpool #330 (mutual TLS)); `parosctl tls` and the sim issue with the same `leaf`.
- `inbound.rs` → `Inbound` (`in_group`: a founding member serves a method in its machine group too, both streams one queue, #404), `ReplySender`, `serve_deliveries`, `rpc_config`, `MAX_FRAME_BYTES` → a `Deliver` batch whose `cell_id` is not the receiver's is refused whole (`EdgeRejection::ForeignCell`, #216); the sender stamps its cell on every batch (`paros-roles/src/node/transport.rs`, `LaneOpener::cell_id`; `0` without a cell plan). The matchmaker requests carry it too (`rpc::Addressed`, stamped from `MatchmakerLinks::cell_id`): a matchmaker host refuses another cell's request before it seats a set (`Host::admitted`, #539 (matchmaker cell fence)).
- `clients.rs` → `NodeClient` (one at-most-once attempt per call; `named` resolves its `Address` per call, #257; `with_interface` sends the data plane through a machine interface, never falling back on its own, #404; `frontend` sends the four journal calls as the `Front*` methods with a `Pass`, #192 (the frontend); `batcher` sends `Write` as `BatchWrite`, #145 (batchers)), `Pass` (a token and the names the caller bound to local ids; a call by name sends unset ids), `MatchmakerClient` (follows the peer book's address and machine interface, and the well-known endpoint after the interface went stale, #404).
- `address.rs` → the address parser the simulation dials with.
- `runtime.rs` → `ParosProviders`, `ThreadProviders`, `CompioProviders` → the providers paros runs on: one thread, local tasks.

## Tests & gates

- `cargo nextest run -p paros-net`.
- `cargo check --target wasm32-unknown-unknown -p paros` covers it with `tls` off.
- Every call is one at-most-once attempt (`try_get_reply`), never `get_reply`.
