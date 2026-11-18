# paros-net

The shared net layer: addresses and name resolution, the transport (plaintext or TLS), the inbound RPC edge with the cell fence, and the outbound clients. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/lib/paros-net/` (2,994 lines of Rust).

## What it did

The crate sat above `paros-core`, `paros-model`, `paros-wire` and `paros-tunables`, on `moonpool-rpc` and `moonpool-core`. `paros-roles`, `paros-machine`, `paros-coordinator`, `parosd`, `parosctl` and `paros-sim` used it through the facade `paros::net`.

Inbound: `Inbound` wraps a moonpool request stream (and, with `in_group`, a machine-group stream too, #404 (static vs dynamic endpoints)) into one queue the node loop reads. `serve_deliveries` takes each peer `Deliver` batch, refuses it whole on a foreign `cell_id` (`EdgeRejection::ForeignCell`, #216), on an unset journal id or on a decode error, and acks it only when the loop took it. It also records each peer's newest `SenderAdvert` so a moved peer is dialed at its new address.

Outbound: `NodeClient` makes exactly one at-most-once attempt per call (`try_get_reply`, never `get_reply`). It resolves its `Address` at dial time (#257), sends data-plane calls through a machine interface when it holds one, sends the `Front*` methods with a `Pass` (token plus bound names) to a frontend (#192 (the frontend)), and sends `Write` as `BatchWrite` to a batcher (#145 (batchers)). `MatchmakerClient` follows the peer book's address and falls back to the well-known endpoint once an interface goes stale.

Transport: `Transport::Plaintext` by default; `Transport::Tls` under the `tls` feature, built by `tls.rs` on moonpool-rpc's TLS and PKI (PR #509 (mutual TLS)). The feature is off by default so the crate stays wasm-safe.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/lib/paros-net/src/inbound.rs` | `Inbound`, `ReplySender`, `serve_deliveries`, `serve_well_known`, `rpc_config`, `EdgeRejection`, `SenderAdvert`, `SenderAdverts`, `OnReject`; re-export of `MAX_FRAME_BYTES`. |
| `crates/lib/paros-net/src/clients.rs` | `NodeClient`, `MatchmakerClient`, `Pass`, `well_known`, `in_group`, `aimed`, `published`. |
| `crates/lib/paros-net/src/fence.rs` | `CellFence` (`new`, `own`, `admits`). |
| `crates/lib/paros-net/src/transport.rs` | `Transport` (`listen`, `client_only`, `is_encrypted`), `RuntimeFuture`. |
| `crates/lib/paros-net/src/tls.rs` | `Role`, `identity` / `parse_identity` (`spiffe://<universe>/<role>/<name>`), `leaf`, `SERVER_NAME` (`paros`), `ClientCertificates`, `Material`, `Files`, `Pem`, `serving`, `dialing`, `client`, `ProviderUtc`. |
| `crates/lib/paros-net/src/resolve.rs` | `expand` (a Compose alias becomes literal addresses), `resolve` (listen address, once at start). |
| `crates/lib/paros-net/src/runtime.rs` | `ParosProviders` = `ThreadProviders + RpcProviders`: one thread, local tasks, no `Send` futures. |
| `crates/lib/paros-net/src/address.rs` | `parse_addr`: the sim's literal parser, port 4500 by default. |
| `crates/lib/paros-net/src/rig.rs` | Test rig: real RPC runtime on `moonpool-sim`'s `SimWorld`, one thread. |

## Ideas worth reusing

- **One attempt per call** (`clients.rs`). The client never retries inside a call. The caller owns retry and backoff, so one timing model can live above it (`docs/architecture` rule 6).
- **The cell fence at the edge** (`fence.rs`, `inbound.rs`). A whole batch of another cell is refused before the loop sees it. Cheap protection against a machine at a reused address.
- **Address is truth for where, reference is truth for which instance** (`aimed`, `published` in `clients.rs`). A wildcard bind is published at the loopback and re-aimed at the advertised address before each dial. Fits rule 5 (a machine is an id, an address is a hint).
- **Names resolved at dial time** (`resolve.rs`, `NodeClient::named`). A machine whose IP changes behind its name is reached again with no restart.
- **`ParosProviders`** (`runtime.rs`). One trait bound lets the same code run on compio and on the simulator.
- **SPIFFE-style identities and one shared server name** (`tls.rs`). Simple and testable PKI.
- **`rig.rs`**: a test drives the shipped edge and clients over a real RPC runtime on a simulated network.

Leaks and mistakes not to copy:

- `NodeClient` (`clients.rs`) knows every method: data plane, admin, matchmaker, scrub, retire, handoff, frontend and batcher routes in one type. A new design should split the data client from the admin client.
- `inbound.rs` depends on `paros_tunables::DriverTunables` (for `rpc_config`) and re-exports `MAX_FRAME_BYTES`: a policy crate leaks into the net layer.
- `transport.rs` docs name `paros_roles` and `paros_machine` drivers, an upward reference.
- `SenderAdverts` lets a peer batch readdress a lane before the registry names it (#851 (grow the cell plan journals)): a second source of truth for addresses.

## Bugs and scenarios to cover again

- **Foreign cell batch refused whole** (#216): `a_batch_from_another_cell_is_refused_whole`, `a_batch_from_the_own_cell_is_queued_and_acked` in `src/inbound/tests.rs`.
- **Matchmaker host refuses another cell's request before it seats a set** (#539 (matchmaker cell fence)): uses `CellFence`; the test is in the host, outside this crate.
- **Unset journal id or bad bytes stop the batch; a gone loop does not ack**: `a_message_that_names_no_journal_stops_the_batch`, `an_undecodable_message_stops_the_batch`, `a_gone_loop_does_not_ack`, `an_empty_batch_is_acked`.
- **Newest advert per peer, taken once**: `the_loop_takes_each_peers_newest_advert_once`.
- **Wildcard reference publishing** (#404): `a_wildcard_reference_is_published_at_the_loopback`, `aiming_moves_the_address_and_keeps_the_instance`.
- **Matchmaker client falls back when its interface goes stale**: `a_matchmaker_call_follows_the_interface_until_it_goes_stale`.
- **A call by name sends unset ids** (#192): `a_named_call_sends_the_name_and_unset_ids` in `src/clients/tests.rs`.
- **TLS material comes all or none**: `files_go_together`, `a_dialing_process_needs_the_authority_and_presents_a_whole_pair` in `src/tls.rs`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-net/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-net

Where to start reading: `src/inbound.rs`, `src/fence.rs`, `src/clients.rs`, `src/transport.rs`, then `src/tls.rs`. Its own map at the tag: `crates/lib/paros-net/AGENTS.md`.
