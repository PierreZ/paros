---
name: extending-the-chain-workload
description: Add or change an operation in paros's ChainWorkload (the one main-campaign client) - the stable operation-id alphabet (PROPOSE=0 through RETIRE=13, retired ids reserved), OP_COUNT and the weight table, swarm_op_enabled, buggify_knob! tunables in ChainConfig, recording every observation in ClientHistory with Ambiguous timeouts, retries that preserve (client, seq, bytes), and the reach_once gate for the draw. Use when adding a client-side operation, a reconfiguration or matchmaker shape, or when changing how the client retries or judges a reply.
---

# Extending the chain workload

`ChainWorkload` (`crates/paros-sim/src/chain_workload.rs`) is the only
main-campaign workload: one to three factory-created clients driving the
Chain-of-Blocks application against a chaotic pool. There is no second
main-campaign workload and no per-scenario process type; a new behaviour is a
new operation in this alphabet, judged by the same `ClientHistory` and the
same `AuditWorld`.

## The alphabet is a wire format

```
PROPOSE=0  PROPOSE_TO_NON_LEADER=1  COMPACT=2  READ_STATE=3  PAUSE=4
DUP_REPROPOSE=5  DUAL_SUBMIT=6  COMPACT_STORM=7  READ_INDEX=8
MATCHMAKE=9 (retired)  MATCH_GC=10 (retired)  RECONFIGURE=11
RECONFIGURE_MATCHMAKERS=12  RETIRE=13          OP_COUNT=14
```

moonpool's operation swarm decides per seed which ids are on as a pure
function of `(seed, id)`, so ids **never shift**: a retired operation keeps
its number as a no-op (that is why 9 and 10 exist), and a new operation takes
`OP_COUNT` and bumps it. Add the weight in the `weights` table of
`ChainConfig::for_timeline` (its own `buggify_knob!`), and the shape rings
(`RECONFIGURE_SHAPES`, `MATCHMAKER_SHAPES`) follow the same rule.

## Steps

1. Give the operation a `const` id and a one-line doc saying what protocol
   path it exists to reach (the public read vs the internal probe, the leader
   vs a non-leader, the refused-on-a-plain-seed case).
2. Draw it through `swarm_op_enabled` like the others and remap a single draw
   into the enabled subset; never loop-resample (extra draws move every seed).
3. Gate the draw with `reach_once!` (a cause), and gate the outcome it is meant
   to reach with an `assert_sometimes!` in the audit or the history (an
   outcome). A perturbation never gets a `sometimes`.
4. Record **every** observation in `ClientHistory` (`audit/client.rs`): an
   ack, a refusal, a redirect, and a timeout or a deliberately abandoned
   observation as `Ambiguous`, never assumed aborted. A retry preserves
   `(client, seq, bytes)` so the server's at-most-once ledger can deduplicate;
   changing any of the three makes it a new command.
5. Tunables the operation introduces (attempts, beats, sleeps) are
   `buggify_knob!` fields in `ChainConfig` with a documented floor; a constant
   buried in the operation is invisible to the swarm.
6. If the operation reads the acceptor or matchmaker set in force (as
   `RECONFIGURE`/`RECONFIGURE_MATCHMAKERS`/`RETIRE` do), compose from the
   **live** pool and move a dead identity out first; membership is protocol
   data, the pool is the role map's list (`roles.rs`), and the floor under
   any configuration is `shape::config_floor`.
7. On a seed without matchmakers a matchmaker-plane request is still sent and
   must be **refused**; assert that leg.

## What the workload never does

It never reads the trace, never inspects node internals except through the
`Inspect` RPC (`READ_STATE`), never pins a seed, and never decides safety on
its own: linearizability and sequential-client consistency are checked in
`ClientHistory` at `check()`, protocol safety in the audit. Keep the
assertion messages stable; they are slots.

Then run the sweep and confirm the new gates fire (`/sim-sweep`).
