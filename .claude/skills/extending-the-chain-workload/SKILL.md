---
name: extending-the-chain-workload
description: Add or change an operation in paros's ChainWorkload (the one main-campaign client) - the stable operation-id alphabet (WRITE=0 through TENANT=26, OP_COUNT=27, retired ids 8, 9, 10, 14, 16 reserved as no-ops), the per-op weight knobs in ChainConfig, swarm_op_enabled, every call driven through the library client paros::client (Client, Writer, Reader) with CallLog as its CallObserver and retry-identity oracle, ClientTunables drawn from ChainConfig knobs, deliberate misbehaviours as explicit calls, and the reachable gate for the draw. Use when adding a client-side operation, a reconfiguration or matchmaker shape, a system-journal operation, or when changing how the client retries or judges a reply.
---

# Extending the chain workload

`ChainWorkload` (`crates/paros-sim/src/chain_workload.rs`) is the only
main-campaign workload: one to three factory-created clients driving a
chaotic pool through the journal API of #204 (`Write`, `Read`, `Truncate`,
`SetLeader`). paros runs no application (#186): each client *is* the
Chain-of-Blocks application, reading the journal and folding every record
into its own `ChainState` (`chain_workload/fold.rs`), which the audit compares
across clients (`AuditWorld::fold_applied`). A fold needs every record from
the start, so every truncation a client asks for is clamped below the shared
trim fence (the lowest cursor of the clients still folding). Every client is
an **owner** (claims with `SetLeader`, then writes) or a **reader** for the
whole run. There is no second main-campaign workload and no per-scenario
process type; a new behaviour is a new operation in this alphabet, judged by
the same `ClientHistory` and the same `AuditWorld`.

Since #188 a client belongs to one journal (`JournalPlan::for_client`,
round-robin over the seed's one to three journals): every call names
`self.journal`, every world it reads is that journal's (`audit_world_for`,
`storage_world_for`, the per-journal trim fence), the matchmaker-plane
operations (`RECONFIGURE`, `RECONFIGURE_MATCHMAKERS`, `RETIRE`) run only on
the run's main journal (`shape::Identifiers::main`), and the run ends only once every journal a client writes
to has converged (the `converged` set on the shared `Tail`). The
system-journal operations (#189) live in `chain_workload/system.rs`; the
three races of `docs/architecture.md` §6 (#205) in `chain_workload/races.rs`
and the `READ` step.

## The alphabet is a wire format

```
WRITE=0  WRITE_TO_NON_LEADER=1  TRUNCATE=2  READ_STATE=3  PAUSE=4
DUP_WRITE=5  DUAL_SUBMIT=6  TRUNCATE_STORM=7  READ_INDEX=8 (retired)
MATCHMAKE=9 (retired)  MATCH_GC=10 (retired)  RECONFIGURE=11
RECONFIGURE_MATCHMAKERS=12  RETIRE=13  QUORUM_READ=14 (retired)  READ=15
CHECK_TAIL=16 (retired)  CREATE_JOURNAL=17  DELETE_JOURNAL=18
REGISTER_NODE=19  DRAIN_NODE=20  RETIRE_NODE=21  SET_LEADER=22
CHECKPOINT=23  BOOK_CAPACITY=24  FLEET_INIT=25  TENANT=26
OP_COUNT=27
```

moonpool's operation swarm decides per seed which ids are on as a pure
function of `(seed, id)`, so ids **never shift**: a retired operation keeps
its number as a no-op (8, 9, 10, 14, 16), a renamed one keeps its id (`WRITE`
was `PROPOSE`, `TRUNCATE` was `COMPACT`), and a new operation takes
`OP_COUNT` and bumps it. Add its weight to the `weights` array of
`ChainConfig::for_timeline` (its own `buggify_knob!`); the shape rings
(`RECONFIGURE_SHAPES`, `MATCHMAKER_SHAPES`) and their weight arrays follow
the same rule.

## Calls go through the library client (#221)

The workload drives every call through `paros::client`
(`crates/paros/src/client/`, see `/working-on-the-client`), as
`ChainClient = paros::client::Client<SimProviders>` (`crates/paros-sim/src/client.rs`):
the client the sweep judges is the client `parosctl` ships. Do not
re-implement a policy loop in the workload.

- `WRITE`: `Client::write`, and on an ambiguous outcome `Client::resolve`
  (read the position back, re-send the identical write). The owner's recovery
  batch writes with `Writer::write_entry`; a claim is `Client::claim`, its
  outcome folded with `Writer::claimed`; `TRUNCATE` through `Client::truncate`; `RECONFIGURE`,
  `RECONFIGURE_MATCHMAKERS` and `RETIRE` through `Client::reconfigure`,
  `Client::reconfigure_matchmakers`, `Client::inspect` and `Client::retire`.
  Race 3 folds its answer with the library's `Reader` (`Reader::new(journal,
  from)`), which resumes a truncated read at the floor it names.
- **`CallLog`** (`chain_workload/rpc.rs`) is the history: it implements
  `paros::client::CallObserver`, so every attempt the library builds and
  every answer it judges is logged at the RPC seam for the linearizability
  search (#205) — no call site can forget one. An attempt whose future is
  dropped (timeout, abandoned observation, shutdown) stays unknown, never
  aborted. `CallLog` also holds the **retry-identity oracle**: between
  `open_write(op)` and `close_write()` every `Write` attempt the library makes
  must carry the request the first one did — generation, owner, position and
  bytes. Wrap every write operation in that pair.
- **Deliberate misbehaviours are explicit calls**, never a client default:
  `Writer::stale_entry` (a superseded owner writing under its old
  generation), `Client::write_attempt` with a `listen` bound
  (`rpc::write_once` with `abandon`: stop listening before the ack),
  `DUAL_SUBMIT` (one `write_attempt` to two servers), `DUP_WRITE` (re-send a
  write this client saw written; it must fold as `Duplicate`). The one-attempt
  helpers in `rpc.rs` (`write_once`, `set_leader_once`, `read_once`) build and
  log their request when called and judge the answer (`judged_write`,
  `judged_set_leader`, `judged_truncate`).
- **`ClientTunables` come from `ChainConfig` knobs**: `ChainConfig::tunables()`
  maps `request_timeout_ms`, `read_timeout_ms`, `write_redirect_limit`,
  `redirect_sleep_ms`, `resolve_attempts`, `retry_backoff_ms`, `read_limit`,
  `read_wait_ms`; `truncate_tunables()`, `reconfigure_tunables()` and
  `matchmakers_tunables()` override the budget and beat per call family. A new
  `ClientTunables` field gets its own `buggify_knob!` field here with a
  documented floor.

## Steps

1. Give the operation a `const` id and a one-line doc saying what protocol
   path it exists to reach (the leader vs a non-leader, the refused-on-a-plain
   seed case, the system journal it writes).
2. Draw it through `swarm_op_enabled` like the others and remap a single draw
   into the enabled subset; never loop-resample (extra draws move every seed).
3. Gate the draw with a reachable (`assert_reachable!` inline, or the audit's
   `reach_once!` for a cause it reports), and gate the outcome it is meant to
   reach with an `assert_sometimes!` in the audit or the history. A
   perturbation never gets a `sometimes`.
4. Make the call through the library client (above); record the operation in
   `ClientHistory` (`audit/client.rs`) too, for the counts and gates. A retry
   is the same write, so the log answers it as a `Duplicate`; changing any of
   generation, owner, position or bytes makes it a new write.
5. Tunables the operation introduces (attempts, beats, sleeps) are
   `buggify_knob!` fields in `ChainConfig` with a documented floor; a constant
   buried in the operation is invisible to the swarm.
6. If the operation reads the acceptor or matchmaker set in force (as
   `RECONFIGURE`/`RECONFIGURE_MATCHMAKERS`/`RETIRE` do), compose from the
   **live** pool and move a dead identity out first; membership is protocol
   data, the pool is the role map's list (`roles.rs`), and the floor under
   any configuration is `shape::config_floor`.
7. On a seed without matchmakers a matchmaker-plane request is still sent and
   must be **refused**; on a seed without system journals a system-journal
   write is still sent and must be refused as `unknown_journal`. Assert the
   refusal leg.

## What the workload never does

It never reads the trace, never inspects node internals except through the
`Inspect` RPC (`Client::inspect`), never pins a seed, never truncates past
the fence, and never decides safety on its own: linearizability is the search
over every attempt at `check()` (`audit/linearizability.rs`), protocol safety
in the audit. Keep the assertion messages stable; they are slots.

Then run the sweep and confirm the new gates fire (`/sim-sweep`).
