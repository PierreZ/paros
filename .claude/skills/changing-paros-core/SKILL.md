---
name: changing-paros-core
description: Edit paros-core, the sans-IO Paxos roles and ColocatedNode, without breaking its contracts - one role per module and no knowledge acquired by colocation, every quorum question through the membership boundary, hard assert! with # Panics docs, assert_invariants at every mutating entry point, the plain Multi-Paxos None arm untouched, no RNG/clock/deps/features that decide anything, tracing behind cfg_attr, and the wasm and rustdoc gates. Use when touching acceptor.rs, proposer.rs, replica.rs, membership.rs, matchmaking.rs, matchmaker/, node.rs or node/*.rs, or when adding a Message, a WriteOp or a HardState field.
---

# Changing paros-core

`paros-core` is a pure synchronous state machine: `step`/`tick` in, one
`Ready` out, `advance()` to acknowledge. No I/O, no clock, no RNG, no
dependency with `--no-default-features`, and no simulation-only path: it is
perturbed only through the methods its caller chooses to call and the data it
is handed. Removing every perturbation leaves the shipped program unchanged
because the perturbation is a caller that stops calling.

## Where a change goes

| Concern | Module |
|---|---|
| durable promise, accepted log, compaction floor, CTRL faulty set | `acceptor.rs` (`Acceptor`) |
| Phase-1 election and P2c merge, CTRL probe, Phase-2 rounds (the standalone `Rounds` tally the proposer embeds; a round's `Custody` is colocated or delegated to a `ProxyId`; a proxy leader embeds the same tally, never a second kernel), bounded recovery; policies are explicit types (`RecoveryPolicy::{Phase1Backed, Inherited}`), never flags | `proposer.rs` + `proposer/{election,probe,rounds,recovery,authority}.rs` |
| the proxy leader (#142): a `Rounds` plus routing on its own process — fans a delegated `Accept` out to the column, folds, emits `Commit { from: Party::Proxy }`, relays a `Nack`, re-fans-out on its beat (`resend_pending`), works for the highest ballot it was handed; ephemeral, no `WriteOp`; proven by `proxy_model.rs` | `proxy_leader.rs` (`ProxyLeader`, `ProxyReady`) |
| chosen prefix, contiguous apply walk, at-most-once ledger, repair cursor; `chosen_gap()` lives here (`node.replica().chosen_gap()`) | `replica.rs` (`Replica`) |
| `AcceptorConfig`, `MatchmakerSet`, `QuorumSystem` — **every** quorum question crosses here; no tally compares a count to a threshold on its own; Phase-1 vs Phase-2 predicates are split on purpose; a grid's column is chosen here (`column_of`) and nowhere else | `membership.rs` |
| the leaderless read tally (#143): a row's vote watermarks, the maximum, bound to one configuration, TTL-bounded; the acceptor answers `vote_watermark`, the replica answers `covers` | `quorum_read.rs` (`QuorumRead`, `QuorumReads`) |
| the candidate's matchmaking phase (registration tally, `H_b`, effective configuration, stale belief) | `matchmaking.rs` |
| the registry and generations, the handover, the single decree over the shared roles at slot zero, the model checker | `matchmaker.rs`, `matchmaker/{reconfigurer,decree,generation,handover_model,storage,message,state,write}.rs` |
| wiring only: role transitions, timers, message construction, the persist-before-send batch, **no protocol tally**; `phase2` opens, fans out or delegates, folds, decides and takes back; `learn` is the learner half | `node.rs`, `node/{election,replication,phase2,learn,handoff,gc,matchmaking,reconfigure,reads,quorum_reads,catch_up_snapshot,boot,acceptor,helpers,invariants}.rs` |

A component must not learn something merely because the deployment colocates
it: the proposer builds no message and knows no role, the acceptor never reads
the chosen prefix, the replica never sees a tally. If a role needs a fact,
`ColocatedNode` hands it in (the acceptor's own records when a Phase 1 opens,
an "is this slot chosen" predicate when a probe closes). A new quorum shape is
a `QuorumSystem` variant, never a rewritten tally or fan-out. A grid's column
(`column_of`) and a read's row (`row_of`) are chosen in `membership.rs` and
nowhere else; a quorum read (`ColocatedNode::quorum_read`) runs on any node,
touches no leader state and reads no clock.

## Plain Multi-Paxos is the `None` arm

A deployment without matchmakers must exchange the same messages and persist
the same scalars it does today: no matchmaker message, no `HardState` field
and no extra round trip may enter the fixed-membership path; a reconfiguration
request there is refused (`accepted: false`). `ColocatedNode` never steps a
matchmaker message. There is no cargo feature and no `cfg` for this; it is
the `None` arm of the same state machine. Read this before touching
`on_check_leader`, `Election`, `HardState` or `Config`.

## Assertions

Hard `assert!`, always on, in release too; crash beats corruption. Operating
errors from external input (a non-leader proposal, a stale snapshot, a
below-floor prepare) are result values or guarded returns, asserted only once
past the validation boundary. Style: precondition stack at entry,
postconditions at exit, split compound conditions, assert positive and
negative space, pair each property across two paths (write-side ordering vs
boot read-back). `ColocatedNode::assert_invariants` (`node/invariants.rs`,
`pub(super)`) is called at boot and at the exit of every public mutating
entry point; a new entry point calls it too. No `debug_assert!` anywhere.
Every public function that can panic carries a `# Panics` section (pedantic
enforces it).

## Constructors and wire types

`AcceptorConfig::new` and `MatchmakerSet::new` are the only constructors
(deserialisation included) because the membership is binary-searched and
normalized; do not add a second path that skips the check. `HardState` is
`#[non_exhaustive]` with two scalars; adding one is a storage-surface change
across `paros` and `paros-sim`, so justify it in the design note first.

## Tracing

`#[cfg_attr(feature = "tracing", tracing::instrument(level = "debug" | "trace", skip_all, fields(node = self.config.id.0, ..)))]`
on every important method: public entry points at `debug`, per-message and
per-tick internals at `trace`; a few cheap fields (node, `from`, `round`,
`slot`), never a whole message; no `ret`, no `err`. The two features
(`tracing`, default on; `serde`, off) are observation-only and stay so.

## Gates before you are done

```bash
cargo check --target wasm32-unknown-unknown -p paros-core
cargo check --target wasm32-unknown-unknown -p paros-core --no-default-features
cargo check -p paros-core --features serde
RUSTDOCFLAGS="-D warnings" cargo doc -p paros-core --no-deps
cargo run -p paros-core --example single_decree   # also multi_paxos, matchmaker, flexible_quorums, acceptor_grid, quorum_read, proxy_leader
cargo nextest run -p paros-core                   # incl. the handover and proxy model checkers
```

Then the sweep: a core change is proven by `cargo xtask sim run paros-chain`
going green and saturating, not by the unit tests (`/simulation-driven-fix`).
The model checkers' knobs are env vars, the only ones in the workspace:
`HANDOVER_MODEL_SEEDS`, `HANDOVER_MODEL_STEPS`, `HANDOVER_MODEL_TRACE`,
`PROXY_MODEL_SEEDS`, `PROXY_MODEL_STEPS`, `PROXY_MODEL_FROM`, `PROXY_MODEL_TRACE`.

## Delegation is opt-in, and the allocator is durable

A `Config::proxy_count` of zero is the plain deployment: no `Audience::Proxy`,
no `Party::Proxy`, no `Accept.config` on the wire. Only rounds a *settled*
leadership opens through `propose` / `propose_control` are delegated; an
election's recovery and gap fills never are; a handoff successor re-delegates
what it inherited. **Every round a leader opens is recorded in its own log**
whenever its promise allows, whether or not its vote counts — that record is
what a reboot derives the allocator frontier from, and what the handoff's
replay guard rests on (the proxy model checker found it load-bearing).
