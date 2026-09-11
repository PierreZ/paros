# paros play: the interactive Paxos game

A browser game where the player runs the Paxos family *by hand* — chooses which message to
deliver, which node ticks, who crashes, who proposes — and, when a node has to answer, plays
that node's role and is checked against the real `paros-core` state machine compiled to
WebAssembly. Levels replace most of the book's prose; the book shrinks to a field guide the
levels link to.

The Raft visualisation *The Secret Lives of Data* is the ancestor: a real state machine, a
narration layer, and derived-not-animated rendering. What it never lets a learner do — choose
which in-flight message lands next, drive a ballot by hand, crash at a durability seam, see the
violation a wrong rule would cause, undo a decision — is exactly what this game is for.

## Decisions (agreed 2026-09-10)

- **Play model: scheduler + roles.** The player is the network and the clock (deliver, drop,
  duplicate, tick, crash, restart, propose), *and* when a manual role must answer, the player
  answers — promise or nack, which value to accept, which value to re-propose, fill the hole or
  not, serve the read or wait — and the core's answer judges it. Passing a role's levels unlocks
  automating it in every later level ("reward": heartbeats, acceptor replies, P2c, apply…).
- **The core is never buggified, and never forked.** A wrong answer never enters the core: the
  game explains the violation it *would* cause (authored explanation + the concrete numbers) and
  the level records the mistake; the world only advances on the right answer. No toy "wrong
  acceptor" exists anywhere.
- **Standalone `/play`; the book becomes a field guide.** One single-page app deployed on the
  same GitHub Pages site under `/play/`. Chapters shrink to short reference pages (the safety
  proof ladder, the symbol map, the doctrine) that levels link to; the removed mechanism prose
  becomes level briefings. Shrinking happens per act, in the PR that lands the act's levels.
- **One PR, all four acts** (revised the same day: the acts were first planned as one PR
  each). The acts still land in order inside the branch — engine, Act I, Act II, Act III,
  Act IV — and each is verified before the next starts.
- **The goal is to understand Paxos, not to read paros-core.** Every briefing, prompt, wrong-
  answer explanation, hint and narration line is written in Paxos terms — ballots, promises,
  quorums, what a node knows and does not know — and would make sense to someone who has never
  opened the crate. A paros-core symbol appears only in the level's "in the code" footnote
  and the field-guide link. A level is judged by the question it leaves the player able to
  answer ("why must the new proposer adopt a value nobody chose?"), never by the API it
  exercised.
- **All player-facing text follows ASD-STE100 (Simplified Technical English).** Briefings,
  prompt questions, choices, explanations, hints, goal texts, narration lines and UI labels,
  and the book pages written for the game (`play.md`, the field-guide chapters): one
  instruction per sentence, active voice, present tense, sentences of at most 20 words in
  procedures and 25 in descriptions, paragraphs of at most six sentences, no idioms, no
  figurative language, no noun clusters longer than three words, articles always written,
  approved vocabulary where STE defines one (`must` for obligation, `make sure`, `applicable`
  is not approved, `do not` rather than `never`), technical names (ballot, promise, quorum,
  slot, Prepare, Promise, Accept, Accepted, Nack, Commit, Heartbeat) kept as technical names.
  Warnings and cautions come before the step they apply to. A wrong-answer explanation says
  first what the result is, then why.
- **Frontend: full TypeScript + npm (vite, vitest), every tool from Nix.** `nodejs_22` from the
  flake; `package-lock.json` committed; `npm ci` in CI. No global installs, ever.
- **Levels and goals live in Rust.** The wasm crate owns the world, the action log, the level
  definitions, the goal predicates and the role prompts, all over real core types, and is
  tested natively with `cargo nextest`. TypeScript renders and collects input, nothing more.

## Layout

```
crates/paros-play/                 # the game engine + wasm glue; publish = false
  Cargo.toml                       # cdylib + rlib; paros-core (default-features=false, serde);
                                   # wasm-bindgen = "=0.2.117" (must equal the flake's CLI)
  src/lib.rs                       # #[wasm_bindgen] Game: new(level_id), act(action_json), undo(),
                                   #   view() -> JSON; also the native API the tests use
  src/world/                       # World: nodes, disks, wire, matchmakers, clock, clients
    mod.rs                         # World, InFlight, the client history, the deliver contract
    drain.rs                       # the drain contract: one Ready batch, the seams, the batch prompts
    prompts.rs                     # which delivery raises which question, on which role clone
    render.rs                      # the log world's view
    disk.rs                        # Disk: Storage impl (HardState, records, floor, sealed, app log,
                                   #   and the opaque application snapshot that log serialises to)
    decree/mod.rs                  # the Act I world: Proposer + Acceptor over Slot(0), no ColocatedNode
    decree/render.rs               # its view: an acceptor's promise + record, a proposer's attempt
    matchmakers/                   # the matchmaker plane: process, delivery, verbs, prompts, render
  src/action.rs                    # Action enum (serde): every player verb
  src/prompt/                      # the "play the role" questions and their judges, one file per role
  src/auto.rs                      # Automation flags and the auto-pump they enable
  src/view.rs                      # serde + ts-rs view structs (the one JS contract)
  src/level/                       # Level trait/DSL + one module per act
    mod.rs                         # Level, Goal, Hint, Briefing, the registry
    script.rs                      # references recorded by driving a real Game
    act1.rs … act4.rs              # the four acts
    act3.rs                        # truncation, snapshots, reads
    script.rs                      # a reference solution is *recorded* by driving a real Game
  tests/levels.rs                  # every level's reference solution reaches its goal;
                                   #   every prompt's wrong answers are refused
web/play/                          # the TypeScript app
  package.json, package-lock.json, vite.config.ts, tsconfig.json
  index.html
  src/main.ts                      # boot wasm, route level, drive the render loop
  src/generated/                   # ts-rs output (committed; `cargo test -p paros-play` regenerates,
                                   #   CI fails on diff)
  src/wasm/                        # wasm-bindgen output (gitignored, produced by the build script)
  src/render/                      # SVG stage: cluster, node, log, wire, timers, matchmakers
  src/ui/                          # briefing, goal, prompt card, action log + undo, level map,
                                   #   automation toggles
  src/progress.ts                  # localStorage: levels passed, automations unlocked
  src/*.test.ts                    # vitest
scripts/build-play.sh              # cargo build wasm → wasm-bindgen → npm ci → vite build
                                   #   → stage into book/output/play/
docs/analysis/play/game-plan.md    # this file
```

`crates/paros-play` is a workspace member (workspace lints apply; `cargo clippy --all-targets`
covers it). wasm-only deps sit under `[target.'cfg(target_arch = "wasm32")'.dependencies]` so
the native rlib and the tests build with none of them.

## The engine

**World.** `nodes: Vec<Option<ColocatedNode>>` (`None` = crashed), `disks: Vec<Disk>` (the
`Storage` impl, never dropped by a crash), `wire: Vec<InFlight>` — the in-flight queue, each
entry `{ id, from, to, envelope, sent_at }` where `Envelope` is `Node(Message)` or, from PR 3,
`Match(MatchRequest | MatchReply)`, `Gc(GcRequest | GcAck)`, `Reconfigure(ReconfigureRequest |
ReconfigureReply)` — plus `clock: u64`, the client registry (next seq per client), and the
pending client operations (a proposed `(client, seq)` waiting for its slot, a read waiting for
its `ReadState`).

**The drain contract** is `crates/paros-core/examples/quorum_read.rs:164-198`, generalised:
after any call into a node, `ready()` → copy `writes`, `messages`, `committed`,
`snapshot_offers`, `read_states`, `recovery_batch` out → `advance()` → apply the writes to the
disk in order (`Truncate` behind the app apply, as `crates/paros/src/driver/ready.rs:239-247`
does) → push one `InFlight` per `audience.resolve(&pool, id)` → apply `committed` to the disk's
app log → record served reads → `advance_recovery()`. `Ready` is never held across a player
action.

**Delivery is a player choice.** Nothing leaves the wire unless the player delivers it, drops
it, or duplicates it (an automation may do it for them, see below). Delivering to a crashed
node drops the message. There is no latency model and no partition object: a partition is the
player not delivering.

**Time is a player choice.** `Tick { node }` calls `tick()` on one node; `TickAll` ticks the
whole pool. Election timeouts are set per node by the level (`set_election_timeout`) and shown
as an arc filling with ticks, like the Raft visualisation's election ring. A hand-stepped
leader must not be demoted by `CheckQuorum` between the player's moves: the harness pumps ack
traffic on each tick when the heartbeat automation is on, and otherwise sets the leader's
timeout to the no-check-quorum sentinel exactly as `examples/quorum_read.rs:116` does.

**Crash and restart.** `Crash { node }` drops the `ColocatedNode` and keeps the disk;
`Restart { node }` is `ColocatedNode::new(&disk)`. `CrashAt { node, seam }` is the Act II seam
level: the pending `Ready` batch is cut before sync (nothing durable, nothing sent) or after
sync before send (writes durable, messages lost). `Wipe { node }` (Act IV) clears the disk; the
format-marker refusal is then the core's.

**Undo is replay.** The engine keeps the action log; `undo()` rebuilds the world from the
level's setup and replays all but the last action. The core is deterministic and draws nothing,
so this is exact and cheap. Every level therefore has a canonical trace, which is also what the
tests assert.

**Act I world.** Single-decree levels drive `Proposer<NodeId, Command>` + `Acceptor<Command>`
at `Slot(0)` with a bare `Vec<AcceptorWrite<Command>>` per acceptor, exactly as
`examples/single_decree.rs` does — no `ColocatedNode`, no `Storage`, no clock. It emits the
same `MessageView` shapes as the log world so one wire renderer serves both.

## Playing a role: prompts

A level names which roles are **manual**. When a message reaches a node whose role is manual,
the engine does not `step` it; it raises a `Prompt` and the world waits. The prompt shows the
node's relevant state and the choices; the player answers; the engine computes the core's own
answer on a **clone of the role** (`Acceptor`, `Proposer`, `Replica` are all `Clone`; the node
exposes them through `acceptor()`, `proposer()`, `replica()`) and judges. Right: the real node
steps and the world advances. Wrong: the prompt stays open, the explanation renders (the
authored consequence with this prompt's concrete ballots and values), the level's mistake
counter increments. The world never takes the wrong branch.

Prompt kinds for PR 1:

| kind | the question | judged by |
|---|---|---|
| `AcceptorPrepare` | Prepare at b arrives; promised p. Promise or Nack? | `Acceptor::prepare` on a clone (refuse `b < p`) |
| `AcceptorAccept` | Accept at b for slot s arrives; promised p. Accepted or Nack? | `Acceptor::admit` — the **same** test, refuse `b < p`; the two questions differ in what the answer is *for* (a Promise reports and fences, a vote records), not in which comparison they use |
| `ProposerValue` | Phase 1 complete; Promises reported these (ballot, value)s. Which value goes in the Accept? | `Proposer::close_phase1` → recovered vs own |
| `LeaderRecovery` | You just won. For slot s the quorum reported X / nothing. Re-propose X, fill Noop, or skip? | the `RecoveryStep` the core is about to take, read off a clone **before** the call that pumps the page: `recovery_next` for a page after the first, `close_phase1`'s `recovered` map for the first (which is opened *and* pumped inside one `step`). A `Noop` on the wire is a gap fill *or* a predecessor's gap fill that a Promise reported, and only the recovery knows which |
| `ReplicaApply` | Slots chosen: {…}. chosen_index = k. Apply slot s? | `Replica::learn` then `Replica::advance` on a clone: did the walk surface s as committed? |
| `PersistOrder` | A Ready holds writes and messages. Sync first or send first? | always sync first (the seam level's explanation) |
| `CommitOverwrite` | Commit says slot s is Y; your accepted record says X at a lower ballot. Keep X or take Y? | a **constant** (`take`), and the doc says why: `record_accepted` is an upsert by slot, and the prompt is raised only when what arrived was decided at a strictly higher ballot, so the core has no "keep" state to be in |
| `ReadServe` | Read ctx captured index i; quorum of acks held; chosen_index = c. Serve or wait? | `confirm_reads` (Act III's fresh-leader trap; the Act II variant is the deposed leader) |
| `SnapshotPromise` | A peer's snapshot at boundary i, taken under ballot b, arrived; you promised p. What is your promise now? | `set_promise` + `Acceptor::install` on a clone, driven exactly as `on_install_snapshot` drives the real one (Act III) |
| `AckWrite` | A client retried (client, seq). Acked as applied, held in flight, or given a fresh slot? | `Replica::applied_at` then `Replica::inflight_at` on a clone — the two dedup tables, in the order the core consults them (Act III) |

A prompt kind is unlocked for automation when the level that introduces it is passed; a later
level may still force it manual for teaching.

## Automation as reward

`auto.rs` holds flags: `deliver_heartbeats`, `acceptor_replies`, `proposer_p2c`,
`replica_apply`, `leader_recovery`, `persist_order`, `resend_pending`, `deliver_all_replies` …
Each is unlocked by passing the level that teaches it and is shown as a toggle. When on, the
engine runs the corresponding pump after every player action (deliver every Heartbeat and
HeartbeatAck on the wire; answer every acceptor prompt; tick the leader for its beat…). A
level may pin a flag off. The pump order is fixed and deterministic so replay stays exact.

## Levels

A `Level` is data plus three closures: `setup() -> World`, `goal(&World) -> GoalStatus`
(`Open`, `Reached`, `Failed(reason)`), and `hint(&World, mistakes) -> Option<String>`. It also
declares `manual_roles`, `pinned_automation`, `allowed_actions`, a markdown `briefing`, a
`field_guide` link into the book, the core symbols it names, and the `reference: Vec<Action>`
the tests replay. Level ids are stable strings (`act1/choose-a-value`), never indices.

### Act I — single decree (`world/decree/` world; 3 acceptors, 1–2 proposers)

1. `act1/choose-a-value` — deliver Prepare, Promise, Accept, Accepted by hand with one acceptor
   silent; goal: a value chosen with two of three. Teaches the two phases and "2 of 3 suffices".
2. `act1/be-the-acceptor` — acceptors manual: answer every Prepare and Accept; the two rules
   (`>` to promise, `>=` to vote). Reward: `acceptor_replies`.
3. `act1/adopt-the-value` — a second proposer opens a higher ballot after a value was accepted
   at a lower one; proposer manual: `ProposerValue`. Wrong answer = the double-choose
   explanation. Reward: `proposer_p2c`.
4. `act1/the-duel` — two proposers, Nacks; goal: one value chosen; the level also shows
   livelock is possible and never unsafe (goal is reachable, a "how many rounds" counter).
5. `act1/quorum-intersection` — the player picks the Phase-1 and Phase-2 reach sets (`&[NodeId]`
   slices as in `examples/single_decree.rs`); goal: get a value chosen with a Phase-1 quorum that
   does not contain the acceptor that voted. Unreachable by construction; the level's win is the
   explanation the game gives when the player gives up (the pivot acceptor). Sets up Act IV's
   flexible quorums.
6. `act1/recovery-is-not-catch-up` — a value was accepted by one acceptor only, nothing chosen;
   the new proposer must still adopt it. Goal: chosen, and the chosen value is the old one.

### Act II — a replicated log (`ColocatedNode` world; 3 nodes, 1 client)

7. `act2/persist-before-send` — `PersistOrder` prompt on every Ready; then the seam crash
   variants (`CrashAt` before sync / after sync before send) and what survives each. Reward:
   `persist_order`.
8. `act2/a-log-of-decisions` — propose three commands, deliver by hand, watch `chosen_index`;
   deliver slot 3's Accepted before slot 2's; `ReplicaApply` prompt: holes are not applied.
   Reward: `replica_apply`.
9. `act2/elect-a-leader` — tick a follower to its timeout; one Prepare with `from_slot`; Promises
   report the suffix; `LeaderRecovery` prompt: recover before streaming. Reward:
   `leader_recovery`.
10. `act2/steady-state` — one round trip per command, pipelining (slot 7's Accept before slot 6
    returns), the Heartbeat carrying commit; deliver beats by hand, then the reward:
    `deliver_heartbeats` — the first automation, the one the user named.
11. `act2/the-permanent-gap` — the book's flagship counterexample (`stable-leader.md`, diagram
    8): drop exactly the Accepts for slot 1, let slot 2 be chosen, crash the leader (volatile
    proposer map lost), restart, elect; `LeaderRecovery` prompt for slot 1: fill `Noop`. Goal:
    `chosen_gap()` is `None` and the client's commands are applied.
12. `act2/what-survives-a-crash` — crash and restart at each step of a decision; goal: the
    restarted node's `HardState` never regressed and the chosen value is intact. Includes the
    `CommitOverwrite` prompt (the stale-accept resurrection of `restart-safety.md`).
13. `act2/the-read-that-lies` — the deposed-but-unaware leader; `ReadServe` prompt: a read
    with no ack quorum must not be served. Goal: the client's read watermark never regresses.
    (Read-index proper, `read_floor` and linearizability are Act III.)

### Act III — truncation, snapshots, reads (log world; 3 nodes, 2 clients)

14. `act3/truncate-by-consensus` — a client asks the leader to compact; the leader proposes
    `Truncate` as a control command; deliver it; every node drops its prefix only when it
    *applies* that slot. Goal: one cluster-wide floor, no node stranded. Teaches why pruning
    per node fails and why the floor is a decided value.
15. `act3/the-stranded-node` — crash a node, let the cluster truncate past it, restart it:
    catch-up is refused below the floor; the peer offers a snapshot; `SnapshotPromise` prompt:
    the node adopts `max(promise, snapshot ballot)`, never lower. Goal: the node rejoins with
    its promise intact.
16. `act3/read-index` — a read must prove leadership *now*: capture the chosen index, confirm
    with a quorum of acks to the *current* beat, serve once applied. `ReadServe` prompt: acks
    to an older beat prove nothing. Reward: automation of the confirm step.
17. `act3/the-fresh-leader-trap` — a new leader with a quorum in hand but a lagging chosen
    index: `ReadServe` prompt says wait for the read floor; slot re-decides; the read fires.
18. `act3/linearizable-or-not` — two clients, one leader change; the player produces a history
    and the game judges it by the three conditions (a committed read sees every write acked
    before it began; watermarks never go backwards; a later write lands above an earlier
    read). Goal: a history with a read across the leader change that is linearizable, and one
    deliberately broken attempt the game refuses to certify.
19. `act3/chosen-is-not-applied` — pipelining: slot 6 is chosen while slot 5 is open; a client
    retry for slot 6's command arrives. `AckWrite` prompt: acked as applied, held in flight,
    or given a fresh slot? Teaches the difference between chosen and applied and why both
    dedup tables move together.

### Act IV — everything the book never wrote

20. `act4/flexible-quorums` — decree world, 4 acceptors, `q1 = 3`, `q2 = 2`. The player
    picks the Phase-1 and Phase-2 reach sets; goal: a value chosen by two acceptors, then
    recovered by a later ballot. Teaches `q1 + q2 > n`: the intersection that matters is
    between phases, not within one.
21. `act4/the-grid` — log world, 6 nodes as a 2 × 3 grid. A row elects, a column decides;
    each slot's Accept goes to `slot % cols`. Goal: two slots decided on two different columns;
    a stray Accepted from outside the column does not count (the game shows the tally).
22. `act4/quorum-reads` — a follower serves a linearizable read with no leader involved: ask a
    row for their vote watermarks, take the max, serve once covered. `QuorumReadServe` prompt.
23. `act4/the-handoff` — the leader relinquishes to a peer: same ballot, no Phase 1, the tail
    tiles exactly, gap fill off. Then try to hand on again: refused, one hop only, and the
    explanation of the replayed-Relinquish hazard.
24. `act4/matchmaking` — matchmakers on: the candidate registers `(b, C_b)`, gets the
    histories, and fans Phase 1 to every configuration in `H_b`. `Phase1Complete` prompt: a
    quorum of *every* configuration, not of the union.
25. `act4/reconfigure` — grow onto a spare: a reconfiguration is a round change; the joining
    node promises the new ballot before Phase 2 reaches it; the removed node keeps answering
    Phase 1. Goal: a command chosen under the new configuration.
26. `act4/garbage-collection` — when may the old configuration be forgotten? `MayRetire`
    prompt: installed is not collected; the retire request must carry the effective
    watermark. Goal: the removed acceptor retired with evidence.
27. `act4/matchmaker-generations` — replace the matchmaker set: stop, reconstruct, bootstrap,
    decide (single-decree Paxos again — Act I's roles at slot zero), publish. Goal: the new
    generation active and a campaign registered with it.
28. `act4/faulty-records` — corrupt one accepted record on one disk; the node boots with a
    faulty slot; the leader's repair probe re-decides it. `RepairVerdict` prompt: which CTRL
    case is this, and what may be re-proposed?
29. `act4/the-wiped-node` — wipe a disk: the node cannot rejoin (its promise is gone); the
    cluster heals around it by reconfiguration. Goal: a new command chosen without the wiped
    identity, and the wiped node's refusal explained.

The Act III world adds snapshots and truncation to `Disk` and the driver-side snapshot offer;
the Act IV world adds the matchmaker plane (`world/matchmakers/`), the grid column choice,
quorum reads, handoff, faulty records and the wipe.

## Narration: the game explains what just happened

The Raft visualisation's captions are what made it teach; here they are generated, not
scripted. Every action produces a list of **narration events** the engine derives from the
transition it just made, in Paxos vocabulary, with the concrete numbers: "B receives Prepare
2.1. Its promise was 1.0, so it promises 2.1 and reports what it accepted: nothing." · "A holds
Promises from A and B — two of three, a quorum. Phase 1 is complete; no acceptor reported a
value, so A may propose its own." · "Slot 4 is chosen: two of three acceptors accepted it at
ballot 2.1." · "C applies slot 3. Slot 4 is chosen but slot 3 was not, so C waits." · "A ticks:
its election timer is at 3 of 5." The events ride the view (`GameView.narration`, newest last,
also kept per action in the log) and the frontend shows the latest ones as the caption under
the stage and the full stream in the log. A wrong prompt answer's explanation is a narration
event of kind `violation`. Narration never changes the world and draws nothing.

## The view contract

`view.rs` is the only thing JS reads: `GameView { level: LevelView, world: WorldView, prompt:
Option<PromptView>, goal: GoalView, log: Vec<ActionView>, automation: AutomationView, mistakes }`.
`WorldView { clock, nodes: Vec<NodeView>, wire: Vec<MessageView>, clients: Vec<ClientView>,
matchmakers: Vec<MatchmakerView> }`. `NodeView` carries id, alive, role, ballot, leader,
promised, `accepted: Vec<SlotView { slot, ballot, value, chosen, applied }>`, `chosen_index`,
`first_unchosen`, `next_slot`, `chosen_gap`, floor, `election: { timeout, elapsed }`, open
rounds, pending accepts, read rounds, the acceptor set and quorum system, and the manual/auto
flags. `MessageView { id, kind, from, to, ballot, slot, summary, phase }` where `phase` is the
render family (`prepare`, `promise`, `accept`, `accepted`, `nack`, `commit`, `heartbeat`,
`catchup`, `snapshot`, `read`, `handoff`, `match`, `gc`, `reconfigure`). Every view type derives
`Serialize` + `ts_rs::TS` with `#[ts(export)]`; `cargo test -p paros-play` writes
`web/play/src/generated/`, which is committed, and CI diffs it. Ballots render as `round.node`.

The wasm surface is one class: `Game::new(level_id) `, `Game::levels()`, `act(json) -> json`
(the new view, or an error the UI shows), `undo()`, `view()`, `reset()`. Errors, not panics: the
glue validates every action against the world before calling the core (a column under a
`Majority`, a slot outside the chosen prefix, a node that is crashed…), and installs
`console_error_panic_hook` for whatever is left.

## Rendering (TypeScript)

SVG, hand drawn, derived from the view on every change — no animation state of its own.
Nodes on a circle with ballot, role ring and the election arc; the accepted log beside each
node as a column of slot boxes labelled with their ballot, coloured by the book palette (`done`
green chosen, `gap` red hole, `open` grey undecided, `shared` orange pivot); in-flight messages
as dots on the sender→receiver link, coloured by phase, **clickable to deliver**, with a
right-click / long-press menu to drop or duplicate; a wire list beside the stage for keyboard
play. A side panel: briefing, goal status, the prompt card when one is open, the action log
with undo, hints, the automation toggles, the field-guide link. A level map with progress from
`localStorage`. Light theme matching the book's `rust` theme, dark via `prefers-color-scheme`.
`prefers-reduced-motion` respected (the only motion is the delivery travel).

## Build, CI, deploy

- `flake.nix` adds `nodejs_22`, `wasm-bindgen-cli` (0.2.117 in the locked nixpkgs; the crate
  pins `=0.2.117` and a comment says why), `binaryen` (`wasm-opt -Os` on the release wasm).
- `scripts/build-play.sh`: `cargo build --release --target wasm32-unknown-unknown -p paros-play
  --lib` → `wasm-bindgen --target web --out-dir web/play/src/wasm` → `wasm-opt` → `cd web/play
  && npm ci && npm run build` → copy `web/play/dist/` to `book/output/play/`. Run after
  `mdbook build`.
- `.github/workflows/pages.yml`: one step, `nix develop --command scripts/build-play.sh`, after
  the book build and before the upload.
- `.github/workflows/rust.yml`: `cargo check --target wasm32-unknown-unknown -p paros-play`;
  `cargo nextest run -p paros-play`; a ts-rs export diff check; `npm ci && npm run check &&
  npm test && npm run build` under `nix develop`.
- Local: `npm run dev` (vite) after `scripts/build-play.sh --wasm-only`.

## Book changes

- `SUMMARY.md` gains a "Play" entry (a short `play.md` pointing at `/play/` with the level
  map) at the top; the index's "How to read this book" becomes "How to play, then read".
- `choose-one-value.md`, `safety.md`, `replicated-log.md`, `stable-leader.md`,
  `restart-safety.md` shrink to field-guide pages: the mechanism prose that a level now teaches
  is replaced by a one-paragraph statement plus a "Play it: act1/…" link; the proof ladder, the
  history, the paper citations, the symbol map, the doctrine sections stay. Stale oracle names
  (`SafetyOracle` and friends, deleted in #128) are replaced by the audit's message strings.
- `book/CLAUDE.md`'s "Live demos" section is rewritten: the game is the live surface, built
  on the core driven by hand, never on a trace.
- `truncation-and-snapshots.md` and `linearizable-reads.md` shrink the same way once Act III
  lands; Act IV has no chapter to shrink — `play.md` carries its level map and a short field
  guide section per mechanism links to the design notes under `docs/analysis/`.

## Verification

Before a PR opens: `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo nextest
run`, the wasm check for `paros-core` and `paros-play`, `npm run check`, `npm test`, `npm run
build`, `mdbook build`, then the game opened in a browser and at least two levels played through
by the reviewer, including one wrong prompt answer and one undo.

## Non-goals

No latency model, no random scheduling, no trace replay, no moonpool in the browser, no score
beyond passed/mistakes, no server, no accounts. Seeds are never levels.
