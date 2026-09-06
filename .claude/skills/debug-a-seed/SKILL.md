---
name: debug-a-seed
description: Diagnose a failing paros simulation seed - replay it with sim-paros-hunt replay-main / explore-main / replay-canary, read the audit's always-violation messages and detail maps, walk the protocol back to the decision that broke the invariant, and separate a protocol bug from an oracle bug from a determinism break (HashMap, wall clock, a hook consulted from a spawned task). Use when a hunt or the nextest smoke reports a violation, when a seed goes red, when a canary trips, or when a reproduced seed is unexpectedly green.
argument-hint: [seed]
---

# Debug a seed

The audit records and continues, so one seed carries the whole cascade of a
single root cause. Find the **first** violation, then the transition that
made it possible; later violations are usually consequences.

## 1. Replay it alone

```bash
cargo run -p paros-sim-runner --bin sim-paros-hunt replay-main <seed>
cargo run -p paros-sim-runner --bin sim-paros-hunt explore-main <seed>     # fork nearby timelines
cargo run -p paros-sim-runner --bin sim-paros-hunt replay-canary <seed>    # run twice, compare draws
```

Corpus seeds have their own commands (`replay-corpus`, `replay-corpus-mask`,
`replay-bare-quorum`, `replay-lifecycle`, `replay-departed`,
`replay-chunk-mask`, `replay-chunk-seed`). The binary prints `GREEN`/`RED`
and the `assertion_violations` list. The nextest smoke
(`crates/paros-sim/tests/sim.rs`) drives the same entry points
(`run_chain_seed`, `chain_seed_canary`, `chain_smoke`) if you want it inside a
test run.

## 2. Read the violation

Every oracle message is a stable string and its detail map carries the ids
(node, slot, ballot, client, seq). Locate the message in `paros-sim`:

- `crates/paros-sim/src/audit/state.rs` — protocol safety folded per
  transition (promise monotonicity across restarts, one value per slot, the
  chosen prefix, the storage gates);
- `audit/client.rs` — `ClientHistory`: linearizability over disclosed order
  and sequential-client consistency;
- `audit/matchmaker.rs` — the registry, the leader-side matchmaking, GC and
  generation oracles;
- `audit/mod.rs` — `AuditWorld`, `check_run` (the end-of-run convergence and
  chosen-gap claims), `reach_once!`;
- `chain.rs` — the application state machine check.

The message tells you which invariant; the check's code tells you which
`Audit` callback fed it (`crates/paros/src/audit.rs`), and that callback sits
next to the driver `tracing` event that reports the transition.

## 3. Walk back through the protocol

Ask, in order: which role made the decision (`Acceptor`, `Proposer`,
`Replica`, `Matchmaking`, `Matchmaker` in `paros-core`), which data was it
handed by `ColocatedNode`, and which fault or ordering meant that data was
stale. The usual shapes:

- a persist-before-send ordering broken at a `Seam` crash (`crash_at`);
- a value re-proposed under a wrong recovery policy after a leader change;
- a floor (compaction, GC watermark, promise) that regressed across a reboot;
- a configuration belief adopted from a `Prepare`/`Heartbeat` that a later
  reconfiguration superseded;
- a client retry that changed `(client, seq, bytes)` and was treated as new.

Use `tracing` output for reading only; never add a check that scans it.
Setting the seed's trace to DEBUG means raising the level in the runner's
`init_sim_tracing` call for the replay, not an environment variable.

## 4. Decide who is wrong

Three parties can be wrong and all three happen: the protocol (fix
`paros-core` or the driver), the harness (a fault the world injected that the
budget said it could not, a stand-in that lies), or the oracle (a claim
stronger than the protocol makes). A wrong oracle is fixed by making the
claim precise, never by weakening the message or deleting the gate; keep the
message string so its saturation history survives.

## 5. It does not reproduce

Then a draw came from outside the seed. Run `replay-canary`; a trip reports
the first diverging draw index. Check for: a `HashMap`/`HashSet` (denied by
`clippy.toml`, but a dependency can leak one), `Instant::now`/`SystemTime`, a
`static` that survives a run, a `spawn_task(..).detach()`ed task that
consulted a hook or drew randomness (hooks are consulted only on the node
loop and carried into tasks), or a `tokio::select!` where
`moonpool::select!` was meant.

## 6. Close it out

Fix, rerun the seed, run the hunt for volume, then the sancov sweep for
saturation (`/sim-sweep`), then `/validate`. Cite the seed in the commit and
the doc comment of the rule it proved; do not pin it as a test.
