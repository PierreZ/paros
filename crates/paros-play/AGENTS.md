# paros-play

The interactive Paxos game's engine: `paros-core` driven **by hand**, plus the levels,
prompts and views the browser reads. `publish = false`; `cdylib` for the wasm bundle,
`rlib` for the native tests. The TypeScript app lives beside it in `web/play/`.

Spec: `docs/analysis/play/game-plan.md`. Read it before adding a level or a verb.

## The rule everything else follows from

**The core is never modified, never forked, and never given a wrong answer.** This crate
is a *driver* — the same shape as `crates/paros/src/driver`, with a player where the
network and the clock would be. When a level makes a role manual, the engine computes
`paros-core`'s own answer on a **clone of the role** (`Acceptor`, `Proposer` and
`Replica` are all `Clone`; `ColocatedNode` hands them out read-only) and only advances
the world when the player matches it. A wrong answer costs a mistake and an explanation;
it is never a state the world enters. There is no toy acceptor anywhere.

Two corollaries that bite in review:

- **A judge that restates a rule is a bug.** `expected` comes from a clone — `prepare`,
  `admit`, `close_phase1`, `recovery_next`, `confirm_reads`, `Replica::advance`,
  `Acceptor::install`, the two dedup ledgers. The one deliberate exception is a prompt
  whose answer is a *constant* because the core has no other state to be in
  (`PersistOrder`, `CommitOverwrite`); each says so in its doc comment and says why.
- **The engine validates before it calls the core.** The core asserts, and an assert in
  wasm is an abort with no stack, so every player-reachable refusal is an `ActionError`.
  A branch that would hand the core a contradiction is refused *and surfaced* — never
  silently swallowed (see `DecreeWorld::violation`).

## The map

- `src/lib.rs` — `Game`: the level, the world, the automation flags, and the action log
  that is also the undo stack. `undo` replays; the engine draws no randomness and reads
  no clock, so it is bit-exact. The `#[wasm_bindgen] WasmGame` surface is here too.
- `src/action.rs` — every player verb and the one error type. `ActionKind` is the
  payload-free family a level's `allowed_actions` lists.
- `src/world/` — **two worlds**, one wire renderer:
  - `world/decree/` — Act I: bare `Proposer` + `Acceptor` over slot 0, with the phase
    **reach** sets as the only network. No `ColocatedNode`, no disk, no clock.
  - `world/mod.rs` — Act II onward: `ColocatedNode`s, `Disk`s, a wire, a clock, clients,
    and the client history the linearizability judge reads.
  - `world/disk.rs` — the `Storage` impl plus the application: the applied log, and the
    opaque snapshot that log serialises to. The game *is* the application here, so the
    apply side is the one party entitled to read those bytes back.
  - `world/drain.rs` — **the drain contract**, and the only place a `Ready` is held.
  - `world/prompts.rs` — which delivery raises which question, and the clone it is
    judged on.
- `src/prompt.rs` — the questions, the choices, the judge, and one authored explanation
  per **wrong** choice (nothing is explained when nothing broke).
- `src/auto.rs` — automation as reward: one flag per decision, and the deterministic
  pump the delivery flags enable.
- `src/narration.rs` — what the game says just happened, **derived from the transition**.
- `src/level/` — the level DSL and one module per act.
- `src/view.rs` — the one contract the browser reads.

## The drain contract

After **any** call into a node, exactly once, in this order (it is
`paros::driver::ready`'s order, and `crates/paros-core/examples/quorum_read.rs`'s):

1. `ready()`, copy every bucket out, `advance()`. The guard is never held across a disk
   write, a prompt, or a player action.
2. Persist the writes — **`Truncate` held back**.
3. Send: one wire entry per `Audience::resolve`d addressee.
4. Apply `committed` to the application log, then flush the held-back truncates. A
   durable floor must never outrun the durable application state covering the slots it
   drops; the `AfterSyncBeforeSend` seam makes the same split for the same reason, and
   drops the truncates with the half of the batch that was lost.
5. Serve the batch's `snapshot_offers` — after the apply, so the bytes really do cover
   the boundary the message advertises (the driver's own guard:
   `applied_slot() == Some(offered_index)`).
6. Answer the `read_states`.
7. `advance_recovery()`, and drain again until the node is quiet.

Two prompts can hold a whole batch back (`PersistOrder`, `LeaderRecovery`). Their
narration is **deferred** until the answer, so the caption never prints above the
question it answers.

**The `LeaderRecovery` oracle is read off the core *before* the call that pumps the
page** (`World::plan_recovery`): a page after the first through a clone's
`recovery_next`, the first page through `close_phase1` on a clone of the campaign. A
`Noop` on the wire is a gap fill *or* a predecessor's gap fill that a Promise reported,
and only the recovery knows which — deriving it from the command told the player "the
quorum reported nothing" about a slot the quorum had explicitly described.

## Prompts are judged on role clones

Every `Prompt::*` constructor documents which core call answers it. When adding one:
compute the answer on a clone at **raise** time (the world is frozen while a prompt is
open, so raise time is answer time), give every wrong choice an authored explanation
naming the violation it would cause with this prompt's own numbers, and add the
`PromptKind` to `prompt::ALL_PROMPTS` and its governing `AutomationFlag` to
`auto::ALL_FLAGS`.

## Narration is derived, never scripted

`World::observe` diffs the node's own role accessors across a call and reads the
messages its batches sent. A sentence is emitted only when the corresponding accessor
actually moved or the corresponding message actually left. **Never write a line the diff
cannot support** — "it is behind the commit index it heard" was wrong for a node that
had heard nothing. Narration changes no state and draws no randomness.

## Levels and the reference DSL

A `Level` is data plus four function pointers: `setup`, `goal`, `hint`, `reference`. Ids
are stable strings (`act3/read-index`), never indices — the frontend's progress store
keys on them. A briefing is two or three paragraphs of **Paxos**, mechanism first; a
`paros-core` symbol appears only in `symbols` and the field-guide link, never in the
player-facing prose. `field_guide` is a **bare book filename**; the frontend prefixes
`../`. A level's `unlocks` must be flags it actually pinned manual.

References are **recorded, not written**: `level/script.rs` drives a real `Game`,
choosing messages by what they *are* (a `Prepare`, a slot-1 `Accept`) and answering
every prompt with the answer the core itself gives. A reference cannot drift when the
engine queues one more message, and cannot teach the wrong answer.

## The view contract

`src/view.rs` is the only thing JS reads. Every type derives `Serialize` + `ts_rs::TS`;
`cargo test -p paros-play --test bindings` writes `web/play/src/generated/`, which is
**committed** and diffed in CI. Conventions: a ballot is `round.node`; a value in prose
is quoted (`show_command`) and a value in the view is plain text (`value_text`) beside
`control_kind`; enums cross the boundary as enums, never as free-form strings.

## The gate

```
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run -p paros-play
cargo test -p paros-play --test bindings      # regenerates web/play/src/generated/
cargo check --target wasm32-unknown-unknown -p paros-play
RUSTDOCFLAGS="-D warnings" cargo doc -p paros-play --no-deps
```

Clippy pedantic is on: `# Panics` on anything that asserts, `#[must_use]` on the
accessors, no `HashMap`/`HashSet`. No randomness and no clock, ever — the action log
plus the flag set is the whole state, and `undo` depends on that being true.
