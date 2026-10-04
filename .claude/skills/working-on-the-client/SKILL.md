---
name: working-on-the-client
description: Change paros::client (crates/paros/src/client/ - Client, ClientTunables, Retarget, WriteOptions, Writer, Reader, the typed outcomes, CallObserver/NoObserver) or the parosctl CLI that wraps it, without breaking its rules - provider-generic and wasm-safe, no randomness, every outcome typed, a retry is the identical write, ambiguity is a result settled by Client::resolve, misbehaviours are explicit calls never defaults, every new ClientTunables field becomes a buggify_knob! in ChainConfig with a documented floor, every new library decision lands with a sometimes gate in the chain workload, and parosctl holds no policy (exit codes 0/3/4/5, --json). Use when touching the client library, a client policy (redirects, retries, claims, read resume), a parosctl command, or when the sim workload needs a client behaviour the library lacks.
---

# Working on the client

`paros::client` (#221, `crates/paros/src/client/`) is the client's **policy**
over the typed RPC stub `NodeClient`, written once and shipped twice: the
chain workload drives it in the simulation (`ChainClient =
paros::client::Client<SimProviders>`, `crates/paros-sim/src/client.rs`), and
`parosctl` drives it over Tokio. The client the sweep and the
linearizability checker judge is the client an operator runs. The module doc
(`client/mod.rs`) is the contract; read it first.

## The map

| Concern | Where |
|---|---|
| which server to ask, the leader hint, redirects, `Retarget` (`FollowHint`, `SameNode`, `NextNode`), `WriteOptions`, the journal calls (`write`, `resolve`, `read_any`, `claim`, `set_leader`, `truncate`) and the operator calls (`reconfigure`, `reconfigure_matchmakers`, `inspect`, `retire`), the one-attempt calls (`write_attempt`, `set_leader_attempt`, `read_attempt`, `truncate_attempt`) | `mod.rs` (`Client`, `ClientTunables`, `Server`, `LeaderHint`, `WriteReport`, `Resolution`, `ResolveReport`, `ReadReport`) |
| the typed outcomes, each with a `judge` from the wire reply | `outcome.rs` (`WriteOutcome`, `SetLeaderOutcome`, `ClaimOutcome`, `ReadOutcome`, `TruncateOutcome`, `ReconfigureOutcome`, `ReconfigureMatchmakersOutcome`, `RetireOutcome`, and the refusal enums) |
| the writer session: claim with `SetLeader` against the generation read, track generation and next position, stop when superseded | `writer.rs` (`Writer`, `WriterOutcome`, `Learned`, `write_request`) |
| the reader: a cursor, paged `Read`s with a long-poll, a `truncated` answer resumed at the floor and reported as `ReaderOutcome::Gap` | `reader.rs` (`Reader`, `ReaderOutcome`) |
| observation of every attempt and answer at the four journal calls | `observer.rs` (`CallObserver`, `NoObserver`, `Attempted`, `Answered`) |
| checkpoint and truncate (#230): the record format (`MAGIC`, `Inline` / `Ref`), the pure `Folder` every reader runs (the registry's node follower too), the owner's `Checkpointer` (`open`, `append`, `due`, `checkpoint` = `write_checkpoint` + `truncate_to`) | `checkpoint.rs` (`Checkpointable`, `Folder`, `Folded`, `Checkpointer`, `CheckpointPolicy` from `checkpoint_factor` / `checkpoint_interval`) |
| the pure parts pinned by unit tests | `tests.rs` |

## Rules

- **Provider-generic and wasm-safe.** `Client<P: Providers>` needs the
  provider's time and the caller's RPC runtime, nothing else; no Tokio, no
  `std::time::Instant`, no thread. The portability gate
  `cargo check --target wasm32-unknown-unknown -p paros` covers it.
- **No randomness.** Where a choice is the caller's (the first server, the
  `Retarget` rule, `WriteOptions`), the caller makes it and passes it in. A
  draw inside the library would move every simulation seed and could not be
  swarmed.
- **Every outcome is typed**, never a string or a bare `bool`. A new reply
  field gets a variant and its `judge`, and a refusal label the node sends gets
  a typed refusal (`every_refusal_label_the_node_sends_is_typed` in
  `tests.rs`).
- **A retry is the identical write**: same generation, owner, position and
  bytes, so the log answers it as a `Duplicate`. There is no "retry with a
  fresh position" anywhere; the simulation's `CallLog` (`open_write` /
  `close_write`, `crates/paros-sim/src/chain_workload/rpc.rs`) asserts it on
  every attempt.
- **Ambiguity is a result.** A write whose answer never came is
  `WriteOutcome::Ambiguous`, never "not done". `Client::resolve` settles it:
  read the position back, then re-send the identical write, within
  `retry_budget`; what it cannot settle stays `Resolution::Unresolved`.
- **Misbehaviours are explicit calls, never defaults.** A harness needs a
  stale-generation write (`Writer::stale_entry`), an attempt it stops
  listening to (`Client::write_attempt` with a `listen` bound, or
  `WriteOptions::abandon_first_after`), one write to two servers (two
  `write_attempt`s: the workload's `DUAL_SUBMIT`), a re-sent write it saw
  written (`DUP_WRITE`). Each is a method the caller chooses; `Writer::write`
  and `Client::write` never do any of them.
- **Every attempt is observed.** A new journal-call path reports through
  `CallObserver::invoked` when it builds the request and `answered` when it
  judges the reply; skipping either leaves a hole in the history the
  linearizability search reads.
- **Tunables are plain data with floors.** A new `ClientTunables` field
  documents its floor (the smallest value that is still a working client) and
  its `Default`, and in the same change becomes its own `buggify_knob!` field
  in `ChainConfig` (`crates/paros-sim/src/chain_workload.rs`), mapped in
  `ChainConfig::tunables()` (and `truncate_tunables()` /
  `reconfigure_tunables()` / `matchmakers_tunables()` where a call family
  overrides it). Floors already in use: `write_redirect_limit` and
  `resolve_attempts` floor at 1.
- **A new library decision lands with its gate.** The chain workload gates
  the library's decisions by their outcomes, with `assert_sometimes!` (today
  "client: a redirected write is written at the leader", "client: an
  ambiguous write is resolved by a read-back", "client: a superseded writer
  stops writing"). A new decision (a new retarget rule, a new resume path)
  adds its outcome gate there and is driven by the workload, so the sweep
  proves it is reached. Never reword an existing message.

## parosctl

`parosctl` (#220, `crates/parosd/src/bin/parosctl/`: `main.rs`, `commands.rs`,
`output.rs`) is the etcdctl to the library's clientv3: commands `write`,
`read`, `tail`, `truncate`, `set-leader`, `inspect`, `reconfigure`, `retire`;
`--servers ID=HOST:PORT,...` (or `PAROSCTL_SERVERS`), `--timeout-ms`, and
`--json` (one JSON document per answer). It holds **no client policy**: which
server to ask, what a redirect means, that a timeout is ambiguous, how a
writer claims and how a reader resumes are the library's. If a command needs
a behaviour the library lacks, add it to `paros::client` (with its gate) and
call it. Exit codes (`Ending` in `main.rs`): **0** success, **3** answered but
not what was asked (refused, lost, not served), **4** ambiguous (may or may
not have happened), **5** no server answered (nothing decided), **2** bad
arguments (clap).

## Tests and gates

- `crates/paros/src/client/tests.rs`: the pure parts only (the judges, the
  writer's belief, the reader's cursor). The policy loops are judged by the
  simulation, which runs them; do not add a mocked-network unit test for one.
- The simulation: the nextest smoke (`crates/paros-sim/tests/sim.rs`) for
  the safety oracles quickly, then `cargo xtask sim run paros-chain` to
  saturate the new gate (`/sim-sweep`); the retry-identity oracle and the
  linearizability search judge every attempt.
- Then the full gate (`/validate`): clippy `--all-targets`, nextest, and the
  wasm check of `paros`.
