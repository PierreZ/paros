# paros

Learning project: implementing the Paxos consensus algorithm in Rust. WIP, not for production.

## Build & test

Dev shell is a Nix flake — enter `nix develop` (or rely on direnv) before running commands.
(On Claude Code on the web the flake can't be built — its inputs are egress-blocked; use
`nix shell nixpkgs#rustup -c cargo …` instead. See *Always use Nix-provided software* below.)

- `cargo build`
- `cargo nextest run` (fall back to `cargo test`)
- `cargo fmt` + `cargo clippy -- -D warnings` before committing

Rust 2024 edition, toolchain pinned in `rust-toolchain.toml` (incl. the `wasm32-unknown-unknown`
target). Clippy pedantic is on (`[workspace.lints]` in `Cargo.toml`).

**Meta issue upkeep.** Issue #69 (`meta: up next`, label `up-next`) is the rolling backlog
pointer — always exactly the next 3 issues, edited in place, never closed. Whenever a PR merges
that closes or materially advances a tracked issue, update #69 in the same session: move closed
work into "Recently landed" (one line: PR number, what it proved/fixed), promote
the next item from "On deck" into "Next 3" with a one-line why, and re-rank if the merge changed
the picture (e.g. a feeder bug closed, an oracle went from armed to proven). Keep the issue's own
maintenance contract: never more than 3 in "Next 3".

- `cargo check --target wasm32-unknown-unknown -p paros-core` — portability gate; `paros-core`
  must stay buildable for wasm (CI enforces it).
- `cargo xtask sim …` — sancov-instrumented simulation runner (`scripts/sancov-rustc.sh` is the
  `RUSTC_WRAPPER`, gated by `SANCOV_CRATES`; the flake `shellHook` exports it). The registered main
  campaign is `cargo xtask sim run paros-chain`.

**Sim sweep vs. sim smoke — where each lives.** The heavy, coverage-guided sweep (the one that
must *saturate* `AssertionCoverage`/`CodeCoverage`) always runs via `cargo xtask sim` so the
sancov code-coverage instrumentation guides seed selection; that runner (`paros-sim-runner`) exits
non-zero on any safety violation, so it is the real CI gate. The `cargo nextest` sim tests are only
a fast **smoke** (`SMOKE_ITERATIONS`, a few dozen random seeds through the safety oracles); they do
**not** assert coverage saturation. So: to prove a new red→green
oracle result saturates, run `cargo xtask sim`; the nextest suite just keeps the safety oracles
green quickly. Do not put a multi-thousand-iteration `explore()` back into a nextest test.

**The shape of the harness.** Two axes, one workload, one check. The *main campaign* is a
three-to-six process pool of `NodeProcess::chaotic()` plus zero to three `MatchmakerProcess`es
under every moonpool fault plus the driver hooks and the disk's fault coins, driven by one to
three `ChainWorkload` clients whose every tunable is a `buggify_knob!`; the *corpus* is a scripted
three-node cluster with every fault a targeted injection (`NodeProcess::scripted()`, kills and
restarts through moonpool's `fault_factory`) and an analytically known outcome per mask — plus
the one four-node, one-matchmaker case that needs a spare and a prior configuration
(`DepartedStragglerWorkload`, CTRL Case 3 across a reconfiguration boundary). Which
process plays which role is the **deployment/role map** (`paros_sim::roles`), read off moonpool's
**process groups** (moonpool #197: one `.processes()` registration per role, each with its own
per-seed count and IP range — `paros-node` is the acceptor pool, `paros-matchmaker` the
matchmakers — and attrition scoped per group with `AttritionVictims::group`), so every process
and every client derives the same map without coordination. Membership is never "every process
in the topology": the pool is the map's acceptor list, and the **bootstrap configuration** is
protocol data drawn once per seed (`paros_sim::shape::bootstrap_ranks`) — the whole pool by
default, or on a matchmaker seed a subset of at least `MIN_BOOTSTRAP` nodes that leaves the rest
as *spares* a `Reconfigure` pulls in. A seed whose matchmaker group drew zero members — no
matchmakers, every node an acceptor — is the plain Multi-Paxos deployment and the shape of every
existing axis (the corpus registers no matchmaker group and never draws). Every run is judged by
the same two things: the client's own history (`ClientHistory`, linearizability and
sequential-client consistency) and the shared `AuditWorld` (protocol safety, the application
state machine, the storage gates, the matchmaker registry, the leader-side matchmaking and
reconfiguration oracles, and one convergence claim at the end of the recovery tail). There is no
third workload, no per-scenario process type, and no check that reads a trace.

**Pinned seeds are not a regression mechanism.** A seed does not name a scenario, it names a
*draw schedule*, and every randomness draw the tree gains or loses — a new BUGGIFY location's
per-seed activation, a probability that was tuned, a mailbox that evicts a different message —
shifts every seed's interleaving. A seed therefore reproduces only the build it was found on, and
a "stays red" or "stays green" replay silently stops testing what it was written for the moment
anything moves (PR #126 re-hunted one witness four times inside a single branch). So: **do not add
seed constants, seed lists, or seed-replay tests.** A witness may be cited in a comment or a commit
message as the red→green evidence it was, never pinned as a live artifact. What replaces it is
volume plus reach: the coverage-guided sweep, the raw hunt, and — where a rare-but-valid state
needs to be *likely* rather than lucky — a new BUGGIFY location. A test may still hard-code a seed
when the seed is not a witness: a determinism replay (the same seed twice), a scripted corpus case
whose seed *is* its input (an E1 mask, a chunk mask), or an arbitrary display seed.

**Raw hunt budget.** For `sim-paros-hunt`, 2,000–3,000 ordinary seeds is the normal evidence
target. Raise that to 10,000 only when a substantial protocol, harness, or fault-model change is
introduced. Do not run larger hunts unless the user explicitly requests one; coverage-guided
saturation still belongs to `cargo xtask sim` and is not replaced by raw seed volume. A hunt's
deliverable is a *failing* seed and the diagnosis it leads to — replay it while you fix, cite it in
the commit, and let it go; it is evidence, not an artifact to keep. The `canary` axis
(`sim-paros-hunt canary [iterations]`) is the same campaign under moonpool's
`check_determinism`: every seed runs twice and the replay must reproduce the first run's draw
fingerprints, all of them — a `HashMap` iterated in its randomized order, a static that survives
a run, a wall-clock read, anything paros or the harness keeps outside the seed — and a failure
names the first diverging draw. The nextest smoke runs two seeds under it; run a few hundred
after any change to the harness's randomness, the driver hooks, or the process lifecycle.

**Chain campaign.** `paros-chain` drives a factory-created Chain-of-Blocks workload with stable
operation IDs: `PROPOSE=0`, `PROPOSE_TO_NON_LEADER=1`, `COMPACT=2`, `READ_STATE=3`, `PAUSE=4`,
`DUP_REPROPOSE=5`, `DUAL_SUBMIT=6`, `COMPACT_STORM=7`, `READ_INDEX=8` (the public
leadership-confirmed read, vs. `READ_STATE`'s internal inspect probe), `MATCHMAKE=9` and
`MATCH_GC=10` (**retired** no-ops: the client-side matchmaking stand-ins of #119, superseded by
the leader's own phase — the ids stay reserved so the alphabet never shifts), `RECONFIGURE=11`
(read the acceptor set in force, compose a new one — grow onto a spare, shrink, replace, remove
the leader, rotate the whole set — and ask the leader; on a seed without matchmakers the request
is still sent and must be refused; the composer draws from the *live* pool and moves a dead
identity out first), `RECONFIGURE_MATCHMAKERS=12` (read the matchmaker set a node believes
authoritative, compose a successor — grow, shrink, replace, rotate through the matchmaker pool —
and ask any node to drive the generation handover; refused on a plain seed) and `RETIRE=13`
(ask the leader which acceptors its effective GC floor released, park one in the storage world
for good, and tell it to shut down).
Its application state folds every user, `Truncate`, and `Noop` command into `(applied_count,
chain_hash)`; `NodeStorage::apply` is the production-generic application seam and snapshots carry
that opaque state. The audit's application check (over the `ChainState` the storage layer reports
through `app_applied`) asserts one command/state per applied index, contiguous local
application, and proposal validity. Keep its messages stable. Client timeouts and deliberately
abandoned observations are `Ambiguous`, never assumed aborted; retries preserve `(client, seq,
bytes)`. Exploration is in-process (`workers: 0`) and every workload/process is factory-created so
recipes replay from a fresh builder. The shared assertion tables allow at most 512 sites and 256
`sometimes_each` buckets; never use slots, ballots, request IDs, seeds, or hashes as identities.

**Moonpool questions.** For any question about moonpool's APIs or behavior, consult the
LLM-oriented docs at <https://pierrez.github.io/moonpool/llms.html> before digging through its
source.

**Upstream Moonpool improvements.** When paros work exposes a limitation that is properly reusable
Moonpool infrastructure—not a paros protocol or harness bug—open a focused issue in
`PierreZ/moonpool` instead of silently accepting or locally reimplementing it. Include the concrete
downstream evidence, the smallest requested API/behavior, deterministic replay constraints, and
testable acceptance criteria; then link the issue from the relevant paros plan and PR. Keep safe
paros-side defense in depth while the issue is pending, and advance the Moonpool pin once the
upstream fix lands and its compatibility gates pass.

## Architecture

Sans-IO core driven by moonpool (etcd-raft's `RawNode`/`Node` split: `ColocatedNode` in,
`paros::run_node` out). `paros-core` is a pure
synchronous state machine — `step`/`tick` in, one `Ready` out, `advance()` handshake; no I/O, clock,
RNG, or deps. The `ready()`/`advance()` handshake is type-enforced: `ready(&mut self) -> Ready<'_>`
holds the node's unique borrow, so a second `ready()` before `advance()` is a *compile* error.
Persist-before-send durability ordering is documented on `Ready`/`HardState`. Contract reference:
`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`.

**The core is composable: one file per role, `ColocatedNode` is wiring.** `paros-core` is not one
state machine but a small set of Paxos *roles*, each its own module and type, and `ColocatedNode` is
the one deployment that colocates them on a node — it holds the role transitions, the timers,
the message construction and the persist-before-send batch, and **no protocol tally of its
own**. The roles:

- `acceptor.rs` — `Acceptor`: the durable promise, the accepted record log, the compaction
  floor and the CTRL faulty set; it decides `prepare`/`admit` and emits the write ops.
- `proposer.rs` — `Proposer`: the Phase-1 election (per-configuration completion, the P2c
  merge), the CTRL repair probe, the Phase-2 rounds and their decision, the bounded recovery a
  fresh leadership drains. Its policies are **explicit types, never flags**
  (`RecoveryPolicy::{Phase1Backed, Inherited}` says what an undescribed slot means; a
  `gap_fill: bool` would not).
- `replica.rs` — `Replica`: the chosen prefix, the contiguous apply walk, the at-most-once
  ledger, the application repair cursor. It consumes "slot chosen, value" and nothing else.
- `membership.rs` — `AcceptorConfig`, `MatchmakerSet`, and `QuorumSystem`, the **one boundary
  every quorum question crosses**: the proposer's tallies, the read rounds, `CheckQuorum`, the
  GC fence, the matchmaker-side tallies and the decree kernel all ask
  `AcceptorConfig::has_phase1_quorum` / `has_phase2_quorum` (or `MatchmakerSet::has_quorum`),
  which ask `QuorumSystem`, and **no tally compares a count against a threshold on its own** —
  a `quorum_size` survives only where a caller reports how many acks are still missing.
  The predicates are **phase-split** because Paxos safety needs every Phase-1 quorum to
  intersect every Phase-2 quorum (`QuorumSystem::cross_intersects`, `q1 + q2 > n`), not each
  phase's quorums to intersect each other; under `Majority` the two coincide. Which phase a
  site is tagged with is a claim: Phase 1 wherever a tally concludes what an *earlier* ballot
  could have chosen (`Election::covered`, the CTRL R2/R3 rule), Phase 2 wherever it claims no
  *later* ballot decided behind it (a decision, the GC fence, a read's confirmation,
  `CheckQuorum`). Addressing goes through the same boundary
  (`QuorumSystem::phase2_addressees`), so flexible, grid and compartmentalized quorums are new
  variants there — never a rewrite of a tally or of a fan-out. `AcceptorConfig`'s fields are
  private and `new` is its only constructor (deserialisation included): the membership is
  binary-searched, so an unsorted one would silently miscount rather than fail.
- `matchmaking.rs` — `Matchmaking`: the candidate's matchmaking phase — the registration tally
  over a matchmaker set, the union of the histories above the maximum watermark (`H_b`), the
  effective configuration and the stale-belief signal. It reads no wire and knows no role; the
  node's `node/matchmaking.rs` is the wiring that feeds it and acts on its answers.
- `matchmaker.rs` — `Matchmaker`: the registry and its generations; `matchmaker/reconfigurer.rs`
  orchestrates the generation handover and *decides* it with a decree — a matchmaker is not an
  acceptor and never becomes one. `MemRegistry` is the reference in-memory registry every test,
  the handover model and the examples reboot a matchmaker from.

The rule that shapes every boundary: **a component must not acquire knowledge merely because
the current deployment happens to colocate it.** The proposer builds no message and knows no
role; the acceptor never reads the chosen prefix; the replica never sees a ballot tally; the
caller hands each one the data it needs (the acceptor's own records when a Phase 1 opens, a
"is this slot chosen" predicate when a probe closes). What that bought, in order: the single
decree is the same `Proposer` + `Acceptor` over a one-slot log (`matchmaker/decree.rs`; there
is no second Paxos kernel in the crate). Still to come: flexible quorums and Compartmentalized
Paxos become deployment data.

The **driver** (`paros::run_node`, the etcd-raft `Node` layer) owns the `ColocatedNode` and does all I/O.
It is written **once, generic over moonpool's `P: Providers`** (and `S: NodeStorage`), so the *same*
code runs in production (`TokioProviders` + a future `parosd` binary) and deterministic simulation
(`SimProviders`). The boundary is the only thing that differs: `paros-sim` adapts it to a moonpool
`Process`; production adapts a `tokio::main`. This "test the code you ship" rule is load-bearing —
protocol logic added in later stages lives in the provider-generic driver, never in a sim-only path.

**Plain Multi-Paxos is first-class and permanent; everything beyond it is opt-in.** Multi-Paxos
without matchmakers — a fixed membership read once from `Storage::initial_state()`, no matchmaker
processes, no matchmaking phase, no registry — is a **permanent** configuration of `paros-core`
and the `paros` driver, not a transitional state the Matchmaker milestone (#22) grows out of.
Anyone must be able to take the core and the driver and run exactly today's protocol with exactly
today's guarantees. The rules, which every later session reads before touching `on_check_leader`,
`Election`, `HardState`, or the harness role map:

- The static-membership case is the **`None` arm of the same state machine** — never a cargo
  feature and never conditional compilation (`paros-core`'s only features, `serde` and `tracing`,
  are observation-only and stay that way).
- No matchmaker message, no `HardState` field, and no extra round trip may enter the
  fixed-membership path. A cluster deployed without matchmakers exchanges the same messages and
  persists the same scalars it does today. The matchmaker is its own state machine
  (`paros_core::Matchmaker`), its own wire contract, and its own driver (`paros::run_matchmaker`);
  `ColocatedNode` never steps a matchmaker message.
- A reconfiguration request on a cluster without matchmakers is **refused** (`accepted: false`),
  never quietly honored.
- Removing every matchmaker feature must leave the plain program's behaviour unchanged — the same
  test the turbulence doctrine below applies to BUGGIFY.

The general rule this instantiates: **flexible quorums, matchmaker reconfiguration, and
compartmentalized Paxos are opt-in features** of paros. The default is plain Multi-Paxos; each
feature is enabled explicitly, as configuration data (a deployment that names matchmakers, a
`QuorumSystem` other than `Majority`), never implied by the presence of its code. In simulation
that configuration is **workload-buggified per seed** (prong 2 below, `buggify_knob!` style): the
harness's deployment/role map (`paros_sim::roles`) draws per seed whether the cluster runs with
matchmakers or without, exactly as it draws cluster size and client count, so **one campaign
exercises both modes**, the liveness and safety oracles hold in both, and the library is *proven*
to support both rather than assumed to. The "matchmakers off" seeds are the plain Multi-Paxos runs
of today and must keep behaving identically; the "matchmakers on" seeds add the registry, the
matchmaking phase and the cross-configuration Phase 1 on top. Every later feature in the list gets
the same treatment when it lands.

**Matchmaking and reconfiguration doctrine (M4.2–M4.4).** On a deployment that names
matchmakers, every campaign is *matchmaking, then Phase 1*: the candidate registers `(b, C_b)`
with the matchmakers (`Ready::match_requests`, `ColocatedNode::on_match_reply`) and sends no `Prepare`
until a matchmaker quorum answered; the replies' histories are unioned above the **maximum**
watermark into `H_b`; a refusal abandons the campaign (the next one opens above the refuser's
highest round); a campaign whose matchmakers are slow is re-asked on every election timeout,
never abandoned by the clock. **The ledger distinguishes a belief from a fact.** Every
registration carries a `kind: RegistrationKind` (`paros_core::Registration`): an ordinary campaign
registers the configuration the candidate *believes* in force (learned from a leader's
`Prepare`, `Heartbeat` or `Relinquish`), a `ColocatedNode::reconfigure` campaign registers an
operator's explicit change. The **effective configuration** is the highest-ballot
reconfiguration registration a matchmaker quorum holds: an ordinary campaign whose histories
name one other than what it registered abandons, adopts it, and re-campaigns
(`MatchStep::StaleConfiguration`), so a node that missed a completed reconfiguration can never be
elected under the superseded configuration; a reconfiguration campaign is exempt (it *is* the
next one). Beliefs never trigger the abort — "adopt the newest *registration*" flip-flopped two
candidates between their abandoned beliefs forever — and reconfiguration requests are monotone
by ballot and never manufactured by a campaign, so adopting the highest cannot. A
reconfiguration is guaranteed to be honored once its matchmaking completed at a quorum
(intersection hands the record to every later campaign); before that it may be lost like any
proposal that never reached a quorum. GC (#123) never retires the highest reconfiguration
registration. Phase 1
then fans out to `H_b ∪ C_b` and completes only with a promise quorum of **every** configuration
in `H_b` — never `quorum(union)`, the negative case the core tests pin — while Phase 2 addresses
`C_b` alone. A **reconfiguration is a round change** (`ColocatedNode::reconfigure`, the `Reconfigure`
RPC): a configuration is bound to a ballot and never edited, so the leader moves to a fresh ballot
registered with `C_new`, stalls command issuance for one matchmaking round trip plus one Phase 1
(the accepted trade — `FrankenPaxos`'s zero-stall overlap is deliberately not implemented), and
resigns afterwards if the change removed it. A joining node promises the new ballot before Phase 2
reaches it and heals as a replica; a removed node keeps answering Phase 1 for the ballots it took
part in ("removed" is not "shut down"; acceptor guards are pool-based, never configuration-based).
The harness treats membership as protocol data, with one floor under every configuration a run
puts in force: `paros_sim::shape::config_floor` — `MIN_BOOTSTRAP` on a matchmaker deployment (the
bootstrap never draws below it and no reconfiguration shrinks below it, whatever the pool), the
whole pool on a plain one. That floor, not the bootstrap size, is what the storage world's copy
budget is computed over: a budget keeping a clean quorum of the smallest configuration keeps one
of every larger configuration too. Module docs: `crates/paros-core/src/matchmaking.rs` (the role), `crates/paros-core/src/node/matchmaking.rs` (the wiring), `crates/paros-core/src/node/reconfigure.rs`.

**Garbage collection doctrine (M4.5, #123).** A configuration may be forgotten only when no
future leader can need its Phase-1 quorum to learn a value its Phase-2 quorum may have chosen.
paros has no replica tier, so the paper's Scenario 3 is *not* what it implements; what it has is
stronger for the purpose — a node that learns a slot chosen records it as its authoritative
accepted record before its chosen index advances, and a truncated member refuses a `Prepare`
below its floor — so the condition is: the leadership is settled (no leader recovery, CTRL probe
or application repair open) and **a Phase-2 quorum of `C_b` reports a chosen index at or past the
election fence** (`HeartbeatAck.chosen`, populated only on a matchmaker deployment). The leader
then asks the current generation's matchmakers to raise the watermark to its own ballot
(`GcRequest`, re-sent each beat, `DriverHooks::skip_gc_resend`); each matchmaker raises it
**durably before acking** and refuses campaigns below it; the floor is **effective only once a
matchmaker quorum acked** (`GcStep::Effective`), and only then does the leader name the
**retirable** acceptors — `members(H_b) \ C_b` — through `Inspect.retirable`. The compaction floor
(per node: "these slots are gone here, recover from a snapshot") and the GC watermark (per
matchmaker: "these configurations are never returned again") never need each other to move.
Retirement is an operator act: "removed is not shut down" until GC says nobody will ask again,
and the `Retire` RPC **carries the evidence**. The operator reads the effective watermark from a
leader's `Inspect` beside the retirable list and sends it in `RetireRequest.gc_watermark`; the
node honors the request (`ColocatedNode::may_retire`) only when it has matchmakers, is neither a member
of the configuration it believes in force nor the leader, *and* that watermark sits strictly above
`last_member_ballot` — the highest ballot a configuration naming this node was bound to. The first
three are beliefs and the third one is volatile (a reboot regresses `acceptors` to the bootstrap
configuration), so without the fourth "the cluster is done with me" would be the operator's
assumption rather than a protocol fact; the refusal leg is `"not_collected"`. The wrong rule (installed ⇒ deletable — DPaxos's rule, as *Matchmaker Paxos*'s Appendix D states it) and
its red→green evidence are recorded in the commit that landed the GC. Module doc:
`crates/paros-core/src/node/gc.rs`; design note:
`docs/analysis/consensus/matchmaker-gc-and-generations.md`.

**Matchmaker-set generations (M4.7, #125).** The matchmaker set is itself a chosen value:
`MatchmakerSet { generation, members }`, and every matchmaking message (`MatchRequest`,
`MatchReply`, `GcRequest`, `GcAck`, every `ReconfigureRequest`) is **fenced by generation** — a
matchmaker answers only its active generation and refuses everything else with what it knows
(`Stopped { successor }`, `Generation { current }`, `Inactive`), never serves it. The handover is
the explicit sans-IO `MatchmakerReconfigurer` (`crates/paros-core/src/matchmaker/reconfigurer.rs`),
driven by the provider-generic node driver: **stop** (a quorum of `M_g` freezes durably; a frozen
matchmaker registers nothing for `g` ever again but stays alive to vote and to point late
proposers at its successor) → **reconstruct** (max watermark, union above it) → **bootstrap**
(every proposed member holds it durably, pending) → **decide** (single-decree Paxos over `M_g`
— **the shared roles over a one-slot log**, not a second kernel: `matchmaker/decree.rs` drives
`Proposer<MatchmakerId, Vec<MatchmakerId>>` at slot zero against each matchmaker's own
`Acceptor<Vec<MatchmakerId>>`, whose two scalars are its durable `DecreeRecord`. The one
deliberate divergence stays outside the role: a `Nack` preempts the decree and the
reconfigurer reopens strictly above the promise that refused it, where the log side discards
the promise and falls back to an election) → **publish** (`Chosen`: `M_g` records the chain link,
`M_{g+1}` activates its pending bootstrap). Invariant 1 — at most one set is authoritative per
generation — rests on the decree (the loser adopts the winner's vote); reconstruction
completeness is asserted in the audit. **Matchmaker quorums are majorities only**
(`MatchmakerSet::has_quorum`; the decree builds a `QuorumSystem::Majority` over the set it
replaces and cannot be given any other quorum): the paper's flexible matchmaker
quorums are deliberately unsupported, and the handover's safety argument is made under the
majority model alone. The handover is proven by a sans-IO **model checker**
(`crates/paros-core/src/matchmaker/handover_model.rs`, run by `cargo nextest`; hundreds of
seeded schedules by default, thousands with `HANDOVER_MODEL_SEEDS`; `HANDOVER_MODEL_STEPS` sets the
chaos steps per schedule and `HANDOVER_MODEL_TRACE` prints a schedule as it runs): concurrent reconfigurers
and finishers over the real `Matchmaker` and `MatchmakerReconfigurer` with every message
dropped, duplicated or reordered, every matchmaker crashed at each durability seam and rebooted
from its disk, every reconfigurer killed or abandoned at any step and every node rebooted to its
bootstrap belief — asserting after each step that at most one set is authoritative per
generation, that a chosen set is what a majority of `M_g` durably voted at one ballot, and that
every activated registry carries the complete reconstruction; then that the pool converges and
that a node with no belief rediscovers the top generation from the bootstrap set. It bites:
publishing the bootstrapped proposal without the decree is red on its first seed, and it found
that a rebooted node's reconfigurer **reused the decree rounds of its earlier incarnation** (the
reconfigurer is volatile; seed 103 put two values at one ballot) — hence the rule that the
`Stopped` reply carries the matchmaker's decree promise and **the decree opens strictly above
the maximum over the stop quorum** (every promise quorum of an earlier decree at that node
intersects it). Liveness rules: a frozen generation with no successor is a
cluster that can elect nobody, so **any node that meets `Stopped { successor: None }` finishes
the handover** (`MatchmakerReconfigurer::finish`, proposing the members that answered the
freeze — the only liveness it can vouch for); and a phase that makes no progress is
**abandoned by the driver** after `DriverTunables::reconfigure_timeout_elections` election
timeouts (the core only reports the stall, `MatchmakerReconfigurer::stalled_for`; the budget is
driver policy and, per prong 2, a workload-buggified tunable rather than a constant — neither
inside the state machine nor hard-coded in the driver; the reconfigurer holds no durable state,
so the freeze, the bootstrap and the votes stay), so a dead proposed member never holds a
`busy` refusal for the rest of a run. The three matchmaker-plane cadences the driver owns —
`match_resend_ticks`, `gc_resend_ticks`, `reconfigurer_resend_ticks` — and the preempted
decree's backoff ceiling (`reconfigure_backoff_max_ticks`) are each their own knob with their
own floor, so a seed can be extreme in one and ordinary in the next. **A successor set must admit its quorum
system** (`MatchmakerSet::is_well_formed`), and it does so by construction: like
`AcceptorConfig`, `MatchmakerSet` has a private membership and `new` — which normalizes and
asserts well-formedness once — is its only constructor, deserialisation and the wire included,
so no `start`, `Bootstrap` or `Chosen` can name a malformed set and none of them checks for one
(the old `Malformed` refusals were unreachable and are gone); a `finish` proposes the members
that answered the freeze — a quorum of the old set, never fewer. A plain deployment holds no
`MatchmakerSet` at all (`ColocatedNode::matchmaker_set` is `None`), never an empty one. A
re-sent `Chosen` is **idempotent at a member that already activated** the successor: it answers
`Learned` again, so a lost ack is recovered by the re-send instead of aborting the publication
as *superseded*. **`Chosen` is a learner notification, not an
acceptor decision**: a matchmaker records or activates the successor it is told without
re-deriving the decree, on the protocol precondition that only a reconfigurer holding the
Phase-2 quorum (or a node relaying such a publication) emits it. Replacement is also how a matchmaker with unusable
state recovers — there is deliberately no matchmaker-specific in-place repair.

**Storage direction.** paros does **not** use moonpool's storage layer: it is too low-level for
what paros needs. The storage seam stays the high-level `NodeStorage` trait (apply / snapshot /
truncate / install_snapshot semantics), with the in-memory + sim implementations behind it. For
production we will later search for and adopt an existing high-level storage engine rather than
building on moonpool's primitives. **The seam is async.** Every
`NodeStorage` / `MatchmakerStorage` method that may touch the device — the writes, the flush,
the boot scan, producing or reading snapshot bytes, restoring the application — returns a `Send`
future (declared `-> impl Future<…> + Send`, moonpool's provider convention; implementations
write plain `async fn`s), and the driver awaits each one in persist-before-send order. The core's
read-only recovery ports (`paros_core::Storage`, `RegistryStorage`) and the accessors that
report what a store already knows about itself (`applied_slot`, `latest_snap_point`,
`snap_chunk_count`, `faulty_snap_chunks`) stay synchronous: `boot_scan` is where a store loads
and verifies its records, and everything the synchronous ports answer afterwards is served from
memory (`MemStorage::from_records` is that in-memory index). The sim's world-backed disks complete
every operation on the poll that started it, so the async seam moves no seed's draw schedule;
modelling device latency through the time provider is a future knob, not something the seam
implies.

**Where each kind of turbulence lives.** Three layers, and nothing crosses them (this is the FDB
separation; #81 removed the message-class nemesis, which mixed them):

- **Environmental faults belong to moonpool.** Drop, delay, duplicate, reorder, directional
  partitions (`AsymmetricSend`/`AsymmetricRecv`), random close, bit-flip, buggified delay,
  crash/restart attrition, seeded-random scheduling — all swarm-masked per seed. paros never
  re-implements one of these at the protocol layer. They all ride the **one combined campaign**
  axis (`chaos_surfaces()`): network turbulence used to need a separate safety-only axis because
  moonpool's faults outlived `chaos_duration`, but since the moonpool pin at `43304d8` the cutoff
  enters *recovery mode* — no new simulator faults, partitions in force healed, persistent damage
  (closed connections, degraded pair latency, clock skew, rotted records, killed processes) kept —
  so the workload's remaining lifetime is a genuine protocol-recovery tail and the liveness /
  convergence oracles apply to network faults too. Do not re-split the axis.
- **`paros-core` is never buggified.** No behavioral cargo feature, no conditional compilation, no
  RNG, no knob: the sans-IO core stays unconditionally pure (its two features, `serde` and
  `tracing`, add derives and spans — observation, never a decision), and it is perturbed **only through its public
  API** — the methods its caller chooses to call, and the data it is handed. Where a rare-but-valid
  decision needs to become reachable, the core's job is to *expose that decision as a method with an
  honest contract* (`ColocatedNode::resend_pending` — "the driver is expected to call this each beat;
  skipping is always safe, re-send is pure optimization"; `ColocatedNode::step_down` — "a leader may
  resign") and nothing more. Removing every perturbation must leave the shipped program unchanged,
  which here is trivially true: the perturbation is a caller that stops calling.
- **BUGGIFY, prong 1 — hook the driver's rare-but-valid decisions.** Timing and policy choices the
  driver owns (skip a pending `Accept` re-send, resign leadership, and future choices such as
  timeout-jitter extremes) are methods on the provider-generic `DriverHooks` trait. Production
  passes `NoHooks`, whose default methods are all false. `paros-sim` implements each behavior with
  its own `buggify_with_prob!` call site, preserving BUGGIFY's per-seed activation × per-call firing
  model without putting simulation dependencies in `paros` or `paros-core`. Consult a hook only
  when the choice can have an observable effect (for example, only ask to skip when accepts are
  pending), trace the action that actually happened, and disable disruptive hooks after the chaos
  window so recovery gets a quiet tail.

  **Consult a hook only from the node loop, never from a spawned task.** A hook answer is a
  randomness draw, and moonpool's BUGGIFY state is a thread-local whose draw order is stable only
  for a stable *call sequence*; the node loop is where the simulation steps deterministically,
  while a `spawn_task(..).detach()`ed task can outlive its simulation and shift the **next** run's
  stream. That is not theoretical: consulting two hooks from inside the peer-delivery task broke
  `same_seed_replays_identically` on CI (a seed's first in-process replay diverged from its second)
  while replaying clean locally, because whether the leftover task got polled was environment-
  dependent. A decision a spawned task needs is taken on the loop and *carried* to it — the peer
  mailbox's `hold_next` / `reverse_next` flags are the pattern.
- **BUGGIFY, prong 2 — tunables are workload-buggified config.** Anything that *shapes* a run — the
  cluster size, request counts, timing windows, fault firing rates, attrition knobs (the #61 swarm
  surface) — belongs in plain config data that the **workload/harness layer** randomizes
  per seed, FDB knob style (`if buggify → an extreme value, else the default`). New tunables should
  be **born that way**, as data a workload can buggify, not as a constant buried in core or driver
  code, so per-seed swarm variation composes without either layer knowing about it. One knob is one
  location: give each tunable its own `buggify_knob!` call site rather than one multiplier over a
  family, so a seed can be extreme in one dimension and ordinary in the next.

  **Every knob documents its floor**, and a knob only exists where the extreme is a *valid
  configuration*: pushing it must not make a run unwinnable. The floor is usually structural rather
  than numeric — the fault window closes long before the recovery tail does, a budget bounds how
  many copies of a record may be lost — and where it is numeric it is a lesson: a peer queue that
  cannot hold one tick's traffic starves whichever class is enqueued last *every* tick, and a
  one-message delivery batch caps per-peer throughput below the protocol's own rate. Both are
  permanent partitions wearing a knob's clothes, which is not a configuration but a defeat of
  eventual synchrony. Two things are **never** buggified: **oracle thresholds**
  (`DEPOSED_TICK_SLACK`, `PLATEAU_SEEDS`, `CHAOS_DURATION_MS`, `SETTLE`, `WAIT_SETTLE`, `FLOOR_GRACE`),
  because they are the judgement the run is measured against — moving them does not explore a new
  state, it changes the verdict on the old one — and **schedule ceilings** (`*_ITERATIONS`), which
  feed the guided seed schedule rather than the run. Constants that a correctness argument depends
  on (`MAX_TORN_TAIL` must equal the driver's real unwitnessed in-flight window) are not tunables at
  all, and say so where they are defined.

The driver's provider-generic `DriverHooks` also exposes the durability seams process-level
attrition cannot reach — eight today (`Seam` in `crates/paros/src/hooks.rs`): the node driver's
`BeforeSync`, `AfterSyncBeforeSend`, `AfterApplyBeforeSync` and `AfterBootReplayBeforeSync`, the
chunk-repair pair `BeforeChunkSync` / `AfterChunkRestoreBeforeSync`, and the matchmaker driver's
`MatchBeforeSync` / `MatchAfterSyncBeforeReply`. Give each seam its
own BUGGIFY location; sharing one location prevents the sweep from independently selecting the
distinct failure modes.

**Audit doctrine — observation, never perturbation.** The mirror image of the `DriverHooks` rule.
The driver also carries a provider-generic `Audit` port (`paros::Audit`, production passes
`NoAudit`): it *reports* every externally meaningful transition — promise raised, accept persisted,
slot applied, message sent or dropped at the send seam, leader elected, gap observed, client acked,
node recovered — typed, once, at the instant it happens, right where the matching `tracing` event is
emitted. Nothing an `Audit` implementation does may change the run: it returns nothing, it draws no
randomness, it reads no wall clock, and deleting every audit call must leave the shipped program
bit-identical. Hooks perturb; the audit only watches.

**Correctness lives in the audit + workload `check()`, not in trace scanning.** `paros-sim`'s
`audit::AuditWorld` is the per-iteration shared checker (published on the `StateHandle` beside the
storage world, factory-created per seed): every callback folds one transition into O(1) incremental
state and asserts there. Client-visible correctness — linearizability, client liveness — lives in
the **workload**, which records its own operation history and checks it in `check()`; the client is
the only party that knows its own program order. The application state machine
(`ChainState`: one command and one state per applied index, contiguous local application)
lives in the audit too, fed by the storage layer's `app_applied`/`app_snapshot`/`app_reset`
callbacks. Tracing is for humans only: nothing reads the trace back, and there is no `Invariant`
type to add one to. If a fact a check needs exists nowhere the audit can see, add the `Audit`
callback that reports it — never a scan over the event stream (it is O(trace²) across a run's
observability pumps). Preserve assertion **message strings** when moving a check — the assertion
slot is the hash of its message, so a reworded message silently resets the sweep's saturation
history.

**Tracing spans.** Every important method carries a `#[tracing::instrument]` span, and the rule is
by layer. In `paros` and `paros-sim` the spans are **non-optional**: the driver's loop stages
(`run_node`, `drain_ready`, `persist_writes`, `maintain`, `replay_boot_state`, the peer-delivery
task, the snapshot repair plane), the RPC handlers, the `NodeStorage` implementations, the process
and workload lifecycles, the fault world's injections, and the audit's gate checks. In `paros-core`
the same attribute is written `#[cfg_attr(feature = "tracing", tracing::instrument(..))]` behind
the default-on `tracing` feature, so a `default-features = false` build is the bare state machine
(the wasm gate checks both). Conventions: `skip_all` plus a few cheap, explicit `fields` — the node
id (`node = self.config.id.0`, `node = self_id`) and a message's coordinates (`from`, `round`,
`slot`), never a whole `Message`, `Command`, or storage handle; public entry points at
`level = "debug"`, per-message handlers and per-tick internals at `level = "trace"`; no `ret` and
no `err` (a seam crash is a deliberate exit, not an error to log). Spans nest under moonpool's
`process`/`workload` spans and the sim layer resolves an event's source by walking outward, so an
`#[instrument]` never changes what the timeline captures; and a span draws no randomness, so it
never moves a seed. Nor does a span slow the sweep: since the moonpool pin at `3a73c8e` the sim
subscriber is floored at `INFO`, so a `debug`/`trace` span is refused before the registry allocates
it — one level compare per call. Spans, like events, are for humans: nothing reads them back.

**Assertion doctrine (TigerBeetle-style).** Two assertion families, split by layer, and neither
substitutes for the other:

- **`paros-core` uses hard `assert!` — always on, in production too.** A broken invariant is a
  programmer error, never an operating condition: crash beats corruption. Operating errors (a
  non-leader proposal, a stale snapshot, a below-floor prepare) stay result values / guarded
  returns — never assert on external input; re-assert it only once it has crossed the validation
  boundary. Style rules: precondition stacks at function entry, postconditions at exit, split
  compound conditions, assert positive *and* negative space, pair each property across two code
  paths (e.g. the write-side flush ordering vs. the boot read-back). `ColocatedNode::assert_invariants`
  is the dedicated cross-field checker (ordering chain, role/election couplings, floor bounds,
  chosen-gap contract), called at boot and at every public mutating entry point — cheap checks
  stay O(1)/O(log n); O(N) structural checks are *also* hard `assert!` (owner's call: no
  `debug_assert!` anywhere in this project — the state maps are small and crash beats corruption
  in release too). Public functions that assert need a `# Panics` doc section (clippy
  pedantic enforces it). This adds no deps and no conditional compilation, so the "core is never
  buggified" rule is untouched.
- **Sim layers use moonpool macros, never plain `assert!`.** In `paros-sim`, a violation should
  *record and continue* (`assert_always!` + detail map), so one root cause surfaces its full
  cascade in a single deterministic trace; coverage claims are `assert_sometimes!` (only where the
  sweep is certain to reach it — an evaluated-but-never-true sometimes fails the runner) or a
  branch-guarded `assert_reachable!` (the `reach_once!` idiom; creates no slot when unreached, so
  it can never fail coverage); guidance is the numeric/`sometimes_all`/`sometimes_each` family.
  **Which one:** a `sometimes` names an *outcome* the run must be proven to reach — a leader is
  elected, a below-floor node recovers through a snapshot, a read commits across a leader change,
  a corruption class is detected — and its failing is a finding about the harness's reach. A
  `reachable` names a *cause* that fired — a hook, a knob extreme, a fault coin, an operation the
  client happened to draw — and only records that it did. A perturbation never gets a
  `sometimes`: whether a seed draws it is the swarm's business, and a gate on it turns a tuned
  probability into a CI failure. Pair every BUGGIFY site with a reachable proving it fired.
  **Budget:** one slot per
  unique message string (identity = the message hash — never reword an existing message), 512
  slots per campaign process shared with moonpool's own internals; overflow is reported as an
  always violation. Count before adding, keep messages short/stable/free of interpolated ids, and
  put dynamic context in the detail map.
- The audit (`paros_sim::audit`) and workload `check()` remain where *cluster-level protocol and
  client-visible* correctness live (see above); in-core asserts guard single-node state-machine
  invariants — the two catch different bug shapes and deliberately overlap (e.g. promise
  monotonicity is asserted in `set_promise` *and* audited across restarts).

**Truncation & snapshot doctrine.** Entry bytes are opaque: paros never *interprets or compacts*
application state. The application owns compaction of its own state. What paros does own is its
*log*, and it drops the log prefix two ways, both keeping the bytes opaque:

- **Truncation is a Paxos-decided control command.** A log slot decides a `Command`, which is either
  a `User(Entry)` (opaque client bytes) or a `Control` metadata command — `Truncate{up_to}`, the
  `Noop` a new leader fills an undecided hole with (see *Election gap fill* below), or the
  `Snap{at_index}` marker that decides a snapshot point (#101). A client
  asks the **leader** to truncate (the `Compact` RPC → `ColocatedNode::propose_control`); the leader
  proposes `Truncate` only once a quorum advertises custody of a decided snapshot point covering
  it (otherwise it seeds a `Snap` marker and answers `accepted: false` — clients can be refused
  and retry), decides it by ordinary consensus, and every node truncates *lazily* when it
  applies that slot (`ColocatedNode::compact`, `WriteOp::Truncate`), giving **one cluster-wide floor**
  forwarded by normal replication + catch-up. The consensus/acceptor paths treat `Command` fully
  opaquely (exactly as Compartmentalized Paxos treats a `Noop`); only the replica/apply path
  interprets a control command, which keeps the eventual M5 compartment split clean.
- **Snapshot transfer recovers a below-floor node.** Acceptors refuse `Prepare`/`Accept` below their
  truncation floor (safety). A node that was down while the cluster truncated past it comes back
  below the floor, where commit-replay catch-up cannot heal it (the entries are gone). paros **does**
  transfer a snapshot to recover it: a peer offers `Message::InstallSnapshot` carrying the
  **opaque, application-produced** snapshot (from `NodeStorage::snapshot()`, the same hook a backup
  would use) plus the boundary `chosen_index`/ballot; the node jumps its chosen prefix, adopts
  `max(promise, ballot)` (its durable promise never regresses), and installs via
  `NodeStorage::install_snapshot()`. paros transfers and tracks the boundary slot; it never
  interprets the bytes. "No compaction" was never "no snapshot transfer": the *application* produces
  the snapshot, paros ships it — and, since #101, also *retains* it: a decided `Snap` point's blob
  is kept durably (the `NodeStorage` custody surface: `record_snapshot`/`read_snap_chunk`/
  `write_snap_chunk`/`restore_from_snap_point`), advertised to peers, and healed chunk-by-chunk
  through a driver-terminal repair plane (`SnapAck`/`SnapChunkRequest`/`SnapChunkResponse` never
  enter `ColocatedNode`); a node can restore its application from a decided point it holds. The bytes
  stay opaque throughout — paros ships, stores, and checksums them, never reads them.

A **wiped** node that lost its durable *promise* (amnesia: a lost disk, not a clean crash) **never
rejoins** (#124): a snapshot restores the log, not the promise, so a naive rejoin could regress a
promise it once made. **Today that rule is enforced by the harness, not by the library**: the
storage world parks a wiped identity for the run, and an empty-but-openable store is
indistinguishable from a first boot to `ColocatedNode::new`, so `run_node` would happily bring a wiped
node back as a fresh member. Closing it needs a durable format marker on the store
(`NodeStorage`) that `run_node` refuses to boot an existing member without. That is an open
item, not a claim the library makes. A wiped identity is parked for the run exactly like a retired one and the
acceptor set heals around it by reconfiguration — the client's composer draws successors from the
live pool and moves a dead member out first. moonpool's `prob_wipe` stays `0` (it wipes moonpool's
storage layer, which paros does not use); the storage world draws its own wipe coin at a chaotic
restart on a matchmaker seed, under the same dead-node budget as a corruption park. A moonpool
issue asks for the reboot kind to be exposed to a restarted process so a harness-owned disk can
honor `CrashAndWipe` directly.

**Cooperative leader handoff (`DPaxos`).** Leadership changes hands two ways. An
*election* destroys a leader's authority and makes the successor rediscover the log
through Phase 1. A *handoff* moves the existing logical Phase-2 authority to another
physical node, which continues under the **same ballot** with **no second Phase 1** —
`ColocatedNode::relinquish_to` → `Message::Relinquish` → `on_relinquish`. What travels is
small and explicit: the ballot, the allocator frontier (`next_slot`), and the tail
`[first_unchosen, next_slot)` split into slots already chosen and slots with an open
Phase-2 round; the two **exactly tile** the range, and a handoff-installed recovery has
**gap filling off** (no Phase-1 quorum report licenses inventing a `Noop`). Three rules
carry the safety:

- **Abdication is synchronous with the decision.** `relinquish_to` queues the message and
  `become_follower`s in the *same call*, before any I/O — so emitting it without
  abdicating is not expressible. No durable fence is needed for the crash case, because
  paros leadership is entirely volatile (`ColocatedNode::new` always boots a Follower, and
  `on_check_leader` only campaigns at a strictly higher round): a crash *is* an
  abdication.
- **The successor is named inside the payload** (`to`), so a duplicate, a misroute, or a
  replay can never hand one authority to a second node; the receiver also refuses an
  authority its own promise dominates, one that would rewind the allocator, and one it
  already holds.
- **One hop only.** `can_relinquish` requires `LeadershipOrigin::Elected`: only the node
  that *minted* a ballot may hand it on. The sweep found the general case unsafe (a
  replayed `Relinquish` re-installs an authority at a node that already handed it on,
  while its successor still exercises it), and closing that would need a durable
  relinquishment fence — a new `HardState` scalar and its whole storage surface — for
  one extra cooperative hop. Handing leadership on again costs the ordinary election
  that mints a fresh ballot.

A handoff is refused while any Phase-1-shaped work is open (leader recovery, CTRL repair
probe, local `faulty` records, application repair) and while the tail exceeds
`HANDOFF_BATCH`. A successor whose inherited read fence stays uncovered for
`HANDOFF_FENCE_ELECTIONS` election timeouts resigns: ordinary Phase 1 is always the
fallback, and a failed handoff costs availability, never safety. Design note:
`docs/analysis/consensus/dpaxos-leader-handoff.md`.

**Election gap fill.** A new leader has two duties, not one. It re-proposes every slot its promise
quorum reported accepted (the P2c value-selection rule), *and* it fills every slot in
`first_unchosen()..next_slot` the quorum reported **nothing** for with a `Control::Noop`. The second
is not optional: pipelining lets a slot reach the old leader alone while a *later* slot reaches the
quorum, so the earlier slot lands in neither `chosen` nor `Election::recovered` while `next_slot`
(derived from the accepted log) steps over it. Nothing would ever propose it again — `propose` only
allocates `next_slot`, and a restart recomputes `next_slot` the same way — and the contiguous chosen
prefix would freeze one below it cluster-wide and forever, with reads fenced above it and
commit-replay catch-up unable to help (every node is frozen at the same place). Filling is safe by
quorum intersection: a value already chosen there would have been reported by some Promise. The core
surfaces the failure through `ColocatedNode::chosen_gap()` (the `Ready` handshake only ever hands out the
*contiguous* prefix, so a stranded chosen slot is otherwise invisible), which the driver reports
each tick through `Audit::chosen_gap` and `paros_sim::audit` asserts against at quiescence.

## Simulation-driven development

This project is simulation-first: the deterministic simulation (moonpool DST + the `paros-sim`
oracles) is the source of truth for correctness, not hand-written unit tests.

When reading code surfaces a *potential* safety or liveness bug, do NOT reach for a classic unit
test. Reproduce it as a **failing simulation**:

1. State the invariant it would violate (e.g. "at most one value is chosen per slot").
2. Make the scenario reachable. Add the chaos it needs (network loss/reorder, crash/restart via
   `Chaos::Attrition`, storage faults) and use `buggify!()` / `buggify_knob!()` to make the rare
   interleaving likely. If the harness lacks a capability (e.g. persistent storage across restart),
   **build that capability**, do not downgrade to a unit test.
3. Add or strengthen a check so the violation surfaces as a
   `SimulationReport.assertion_violation`. Put it where the fact arrives: a driver-observable
   transition goes in `paros_sim::audit` (adding an `Audit` callback if the driver does not report
   it yet), a client-observable one in the workload's own history + `check()`, an application
   or storage fact in the storage layer's audit callbacks. The trace is never read back.
4. Run the sweep, confirm it goes **red** on the unfixed code, and replay that seed while you work.
5. Fix `paros-core`.
6. Run the sweep, confirm it goes **green** and saturates.
7. Write the red→green result down where it stays true: the commit message, and the doc comment on
   the rule or oracle it proved load-bearing. Cite the witness seed there if it helps a reader —
   and then let the seed go. It is evidence that the step happened, not a live reproduction (see
   *Pinned seeds are not a regression mechanism*), and pinning it into the suite only buys a replay
   that quietly stops reproducing.

A deterministic unit test may pin the *mechanism* afterward — a core state-machine trap, a storage
contract — but it never replaces step 4, and it is written against the mechanism, not a seed. A
critical claim the simulation cannot reproduce is treated as **unproven** (it is probably not a real
bug: safety is often preserved by an invariant you missed). Do not add speculative defensive code
for an unreproducible claim.

The sim surface is never finished, and growing it is part of every change, not a follow-up.
Every new feature or protocol path lands *with* its `sometimes`/`reachable` gates (so saturation
proves the path is genuinely visited, not merely present), with its rare-but-valid decisions
hooked through `DriverHooks` BUGGIFY locations, and with its tunables born as workload-buggified
config. And beyond the operation alphabet: keep planting **new inline `buggify_with_prob!` /
`buggify_knob!` call sites** at the boundaries the BUGGIFY post names — optional work that can be
skipped, error-handling paths that can be taken spuriously, concurrency windows that can be
stretched, tuning knobs that can be pushed to extremes — wherever a rare-but-valid state needs to
become *likely* instead of waiting for the swarm to stumble into it. Those sites live in the
driver hooks and the sim/workload layers (per the turbulence doctrine above — never in
`paros-core`), each as its own independent location so per-seed activation composes.

## Simulation references

- [BUGGIFY](https://transactional.blog/simulation/buggify) — place high-level fault injection at
  optional-work, error-handling, concurrency, and tuning-knob boundaries; activate locations per
  run, fire them only sometimes, and stop disruptive injection when the test needs to recover.
- [Designing Rust FDB Workloads That Actually Find Bugs](https://pierrezemb.fr/posts/writing-rust-fdb-workloads-that-find-bugs/)
  — design deterministic operation alphabets and invariants, use seeded randomness exclusively,
  and bias simulation toward adversarial and rare-but-valid states.

## Layout

Cargo workspace (mirrors moonpool). All Rust packages live under `crates/`.
Dependency stack: `paros-core` ← `paros` ← `paros-sim` ← runner.
`paros-core` is dependency-free with `default-features = false` (its only deps, `serde` and
`tracing`, are optional and observation-only); everything ultimately points into it.

- `crates/paros-core/` — the sans-IO Paxos roles (`acceptor.rs`, `proposer.rs`, `replica.rs`,
  the membership boundary in `membership.rs`) and `ColocatedNode`, the node that wires them
  (`node.rs`; its `node/*.rs` submodules are named by *concern* — election, replication,
  handoff, GC, matchmaking, reconfiguration — and hold the wiring for that concern, never a
  role's state) and, beside it, the sans-IO
  matchmaker registry (`Matchmaker`, `crates/paros-core/src/matchmaker.rs` — a separate handle
  the caller drives, never stepped by `ColocatedNode`), its generation handover
  (`matchmaker/reconfigurer.rs`) and the successor decree it decides with over the shared
  roles (`matchmaker/decree.rs`): std-only, wasm-safe, and dependency-free
  with `default-features = false` (CI checks that build too). Two features, both observation-only:
  `serde` (off) adds derives; `tracing` (on) adds the `#[instrument]` spans described under
  *Tracing spans* — see the turbulence doctrine above: the core is never buggified and gains no
  simulation-only conditional compilation. Sancov crate-under-test.
- `crates/paros/` — **the library.** Re-exports `paros-core`, plus the provider-generic driver
  (`run_node` over `P: Providers`, `S: NodeStorage`), the default in-memory `MemStorage`, the
  node RPC contract (`Propose`/`ProposeAck`), and the matchmaker's driver + storage seam
  (`run_matchmaker` over `S: MatchmakerStorage`, `crates/paros/src/matchmaker/`). The client API
  + a `parosd` binary land here. Deps: `paros-core`, `moonpool-core` + `moonpool-hyper` and
  runtime-free tonic (wasm-safe). No dedicated storage crate: the faulty fake is the harness's
  world-backed store (`crates/paros-sim/src/world/storage.rs`).
- `crates/paros-sim/` — the DST harness on top of `paros`: the moonpool `Process` adapter, the
  deployment/role map, the fault world, the one client workload, the audit, and the scripted
  corpus. Depends on `paros` + `moonpool-sim`.
- `crates/paros-sim-runner/` — native sim runner + hunt binaries (`publish = false`).
- `crates/xtask/` — build automation (the sancov sim runner).
- `docs/references/papers/` — Paxos/consensus papers with transcripts.
- `docs/analysis/` — design notes (e.g. sans-IO patterns for Multi-Paxos, the `DPaxos`
  cooperative leader-handoff restatement, the matchmaker GC and generations restatement).

**Organize as you grow.** A module holds one concern. When a file starts holding a second one,
split it in the same change rather than in a follow-up, and never leave a file that needs a
table-of-contents comment to navigate. Deleting is part of every change: a superseded axis,
process type, flag, or gate goes out in the PR that supersedes it.

Publishing/changelogs mirror moonpool: library crates share a `version_group` with per-crate
`CHANGELOG.md` (release-plz); binaries/xtask are `publish = false`. Note: `paros` and
`paros-sim` depend on moonpool via a **git** pin, so they are *not* `cargo publish`-able until a
moonpool release is pinned — `paros-core` is currently the only truly publishable crate.

## Environment detection & setup

At the start of a session, run:

    echo "entrypoint=$CLAUDE_CODE_ENTRYPOINT sandboxed=$CLAUDE_CODE_SANDBOXED"

If `CLAUDE_CODE_ENTRYPOINT` starts with `remote` (e.g. `remote`, `remote_mobile`),
this project is open in **Claude Code on the web** (an isolated, Anthropic-managed
cloud VM). Set up Nix before doing anything else:

    # The sandbox ships broken third-party APT sources (deadsnakes, ondrej), so a
    # plain `apt-get update` fails with 403s — and `update && install` then
    # short-circuits before nix-bin is installed. Install straight from the
    # already-cached package lists; only fall back to `update` if that fails.
    if ! command -v nix-store >/dev/null 2>&1; then
      sudo apt-get install -y nix-bin \
        || { sudo apt-get update; sudo apt-get install -y nix-bin; }
    fi

    # Enable flakes and point Nix at the agent proxy's CA so it can fetch through
    # the proxy. Export these in every shell that runs a `nix` command (or add
    # them to ~/.bashrc):
    export NIX_CONFIG="experimental-features = nix-command flakes"
    export NIX_SSL_CERT_FILE=/root/.ccr/ca-bundle.crt

Any other value (e.g. `cli`, `vscode`) means it's running locally — do NOT run
the install; assume Nix is already set up on the host.

## Always use Nix-provided software

Use Nix-provided software for ALL tooling. **Never** run the sandbox's
pre-installed binaries — in particular the `rustup`/`cargo`/`rustc` under
`/root/.cargo`. Whatever the sandbox image ships is off-limits; every tool comes
from Nix so versions are reproducible across sessions.

- **Locally** (`cli`/`vscode` sessions) just use the project dev shell:
  `nix develop` (or rely on direnv), then run `cargo …` inside it.
- **On Claude Code on the web** the project's `nix develop` shell can't be built:
  its flake inputs (`flake-utils`, `rust-overlay`, `nix-systems`, `nixpkgs`) are
  fetched as GitHub tarballs the sandbox egress policy blocks with a 403
  (`…not enabled for this session`). Do not fight it — that is an org-policy
  denial, not a bug to retry. Instead get a **Nix-provided `rustup`**, which reads
  `rust-toolchain.toml` and runs the pinned channel (1.95.0 +
  `wasm32-unknown-unknown` + clippy/rustfmt):

      nix shell nixpkgs#rustup -c cargo build
      nix shell nixpkgs#rustup -c cargo test           # nextest: add nixpkgs#cargo-nextest
      nix shell nixpkgs#rustup -c cargo clippy -- -D warnings
      nix shell nixpkgs#rustup -c cargo fmt
      nix shell nixpkgs#rustup -c cargo check --target wasm32-unknown-unknown -p paros-core

  `nix shell nixpkgs#…` resolves against `cache.nixos.org` (which the egress
  policy allows), so it works even where `nix develop` does not. The `rustc`/
  `cargo` it runs are the official upstream pinned toolchain — never the sandbox's.
- **Other one-off tools:** `nix shell nixpkgs#<tool> -c <command>` or
  `nix run nixpkgs#<tool> -- <args>`. Do NOT use `apt-get`, `pip install`,
  `npm -g`, `brew`, or similar (`nix-bin` in setup is the sole exception, and only
  to bootstrap Nix itself).
- If a required tool isn't available, add it to the project's flake (or fetch it
  via `nixpkgs#<tool>`) rather than installing it globally.
