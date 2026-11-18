# paros

Learning project: the Paxos consensus algorithm in Rust. WIP, not for production.

**Today the tree is the core and the journal** (Pierre, 2026-11-18). The service (`parosd`,
`parosctl`, the bench, the coordinators, the machines, the simulation harness, the site) left
the tree in one commit. The tag `last-service` names the last commit that holds it (`6376854`),
and `docs/archive/last-service/README.md` records what each deleted part did and how to bring
it back. What stays: `paros-core` (the sans-IO Paxos roles), `paros-storage` (the stores on
`moonpool-journal`), `paros-testkit` (the linearizability checker and the journal model),
`specs/tla/` and `docs/`.

`docs/architecture/` still describes the service, a multi-tenant journal service with a
four-call data plane (`Write`, `Read`, `Truncate`, `SetLeader`), as the end goal. The next
plan (TLA+ in the core, the new moonpool features, the design and split of the journal crates)
is being written; until it lands, treat `docs/architecture/` as the direction and this file as
the present. Each section below ends with where the depth lives; every crate has its own
`AGENTS.md` map.

## Who owns the code

This codebase is yours: how it's built is your call, and you'll keep building on it. When you
work in a file, leave it better: merge repeated code, fix the problems you find next to your
change, and delete what your change makes obsolete, in the same PR. Name every change in the PR
description, the small ones too, because the reviewer reads the diff against it.

What paros is for is Pierre's call. The goal is `docs/architecture/`, and #69 (meta: up next)
points at the next steps. Each root rule of the rework (`docs/architecture/01-goal.md` §1.1)
marked **Not proved** waits for its TLA+ gate before its code. A change to a recorded decision
or to the goal is a proposal: write it in the PR or in an issue, and do not merge it on your
own.

## Finishing work

Do not stop while you still owe work that was asked for. Carry on with work that does not
depend on an open question. When you meet an ordinary obstacle (an error, a timeout, a busy
proxy, a failing tool, an empty result), diagnose it, then work through it: wait and retry, fix
the request, or use another tool. Stop only when the task needs Pierre's input, or when the
blocker is deliberate (access that was withheld, a rule in this file). Then say plainly what you
found.

These steps need no approval: retry a command or a network call that is safe to repeat; open,
update and rebase your own PR; merge your own PR once every CI job is green; file an issue for a
failure you qualified; open and merge a moonpool PR that paros needs, then advance the pin;
update #69. These steps need Pierre's go-ahead: a change to the goal or to a recorded design
decision, a merge on red, and a force-push or a delete on a branch you did not create.

## Build, test, gates

These are the commands CI runs (`.github/workflows/rust.yml`), always through Nix (next section):

- `cargo build`
- `cargo nextest run --profile ci` (fall back to `cargo test`)
- `cargo fmt` (CI: `cargo fmt --all -- --check`), `scripts/check-module-style.sh`,
  `scripts/check-core-layers.sh`
- `cargo clippy --all-targets -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc -p paros-core -p paros-storage -p paros-testkit --no-deps`
- the seven `paros-core` examples (`cargo run -p paros-core --example <name>`)
- wasm gates: `cargo check --target wasm32-unknown-unknown -p paros-core`, the same with
  `--no-default-features`, and `cargo check -p paros-core --features serde`
- `scripts/check-tla.sh` in the `tla` shell (`nix develop .#tla --command scripts/check-tla.sh`):
  TLC on the TLA+ specs of `specs/tla/` (CI's `TLA` workflow, only when a spec, the script or the
  flake changes; decided on 2026-11-11, #579 (zero-stall reconfiguration))

A pull request that touches only Markdown, `docs/` or `specs/` runs no Rust job. Rust 2024
edition, toolchain pinned in `rust-toolchain.toml` (incl. `wasm32-unknown-unknown`), clippy
pedantic on (`[workspace.lints]`). The full local gate is the `validate` skill.

There is no simulation on `main` today: it left with the service. The tests of `paros-core`
are its unit tests, its examples and its seeded models (`models/`, which drive
`ColocatedNode` under a seeded scheduler and check agreement). Every change gets local unit
tests. A protocol rule that the papers do not give gets its TLA+ model first (*Workflow
rules*).

## Environment & Nix

At the start of a session, run:

    echo "entrypoint=$CLAUDE_CODE_ENTRYPOINT sandboxed=$CLAUDE_CODE_SANDBOXED"

If `CLAUDE_CODE_ENTRYPOINT` starts with `remote` (e.g. `remote`, `remote_mobile`), this is
**Claude Code on the web**. Set up Nix before anything else:

    # The sandbox's third-party APT sources 403, so install from the cached lists first.
    if ! command -v nix-store >/dev/null 2>&1; then
      sudo apt-get install -y nix-bin \
        || { sudo apt-get update; sudo apt-get install -y nix-bin; }
    fi
    # Export in every shell that runs `nix` (or add to ~/.bashrc):
    export NIX_CONFIG="experimental-features = nix-command flakes"
    export NIX_SSL_CERT_FILE=/root/.ccr/ca-bundle.crt

Then use `nix develop`, on the web as locally. Every flake input is fetched over git
(`git+https`), not as a GitHub tarball, because the web session's egress policy refuses GitHub
tarballs (#727 (flake inputs over git)). Keep it that way: a new input never uses `github:`.
Any other value (`cli`, `vscode`) is local: Nix is already set up; use `nix develop` (or direnv).

**Use Nix-provided software for all tooling.** Never run the sandbox's preinstalled binaries
(the `rustup`/`cargo`/`rustc` under `/root/.cargo`), never `apt-get`/`pip install`/`npm -g`/
`brew` (`nix-bin` above is the one exception). Run every command in the dev shell:

    nix develop --command cargo build
    nix develop --command cargo nextest run

Other tools: `nix shell nixpkgs#<tool> -c …`; a missing tool goes into the flake.

## Workflow rules

- **Record what the user decides, in the same session.** A design decision or preference the
  user states (what paros is, how it behaves) goes into `docs/architecture/`, dated and with its
  issue; a preference about how to work goes into this file. Amend the section it touches, never
  only a chat, a commit message or an issue comment, and correct a recorded decision the user
  later overturns rather than adding a second one.
- **Never merge on red; always fix** (decided on 2026-10-15). A PR merges only on green CI,
  and a red is fixed, not merged past, even when it is not the PR's own. When several threads
  are red at once, ask the coordinator for one fix-main thread. A fix-main PR merges without
  review once every CI job is green (decided on 2026-10-16).
- **Three red rounds, then report** (decided on 2026-11-17). A round is one attempt to make a
  red check green: a diagnosis, a fix and a run. When the same check (a CI job, a test, a TLA+
  check) is still red after three rounds, stop. Do not start
  a fourth round. Send the coordinator the check, what each round tried, and what you now think
  the cause is. The coordinator decides the next move (another thread, a smaller step, a
  question to Pierre); no person is needed for the report itself. A round never weakens the
  check to pass it.
- **A flaky check is a finding** (decided on 2026-11-17). A check that is red and then green
  with no change teaches you to run it again in place of a fix. Never run a check again until
  it is green, and never add a retry around it. Find the cause: a bug in the code is fixed with
  a test that is red first, and a bad check is corrected or deleted, with the reason in the
  commit message. No runner has a retry setting (nextest runs each test one time), and that
  stays.
- **A pull request does not change its own judge** (decided on 2026-11-17). The judges are: an
  `assert!` of `paros-core`, a test, the judges of `paros-testkit`, a check of `specs/tla/` and
  its expected result, a workflow of `.github/workflows/`, `clippy.toml` and the lint levels. A
  pull request that changes behaviour does not also loosen a judge of that behaviour (delete a
  step, widen a bound, add an excuse, an `#[allow]`, a longer timeout, a skipped test). When a
  judge is wrong, correct it in its own pull request first, with the proof that it was wrong. A
  pull request that makes a judge stricter, or adds one, is welcome in the same diff.
  `REVIEW.md` lists what a reviewer checks for each kind of diff.
- **Check FoundationDB first** (decided on 2026-11-16, the rework plan). Before a choice on how
  paros handles a failure, a missing machine, an operator command or a configuration, read how
  FoundationDB does it, in its source and its documentation, and cite that answer (file, line
  and release) in the question, the issue or the design text. FoundationDB is the first
  reference, not a law: when paros differs, the text says why.
- **Never guess** (Pierre, 2026-11-16). Do not invent a number, a default, a name or a reading
  of a decision. When a fact is missing, find it (the code, the docs, FoundationDB, the
  issue) or ask Pierre. When a sentence is your reading and not a checked fact, say so. In a
  design text, an open point is marked **Open** with the step that decides it.
- **Meta issue #69** (`meta: up next`) is the rolling backlog pointer, exactly ten issues (raised from three on 2026-10-04);
  update it in the session that merges a PR closing or advancing one (`meta-issue-upkeep` skill).
- **Moonpool questions**: read <https://pierrez.github.io/moonpool/llms.html> before its source
  (`moonpool-consultant` agent).
- **A reusable moonpool gap** (simulator infrastructure, not a paros bug) becomes a focused issue
  in `PierreZ/moonpool`; keep paros-side defense in depth meanwhile (`upstream-to-moonpool`).
  When paros needs the fix, improve moonpool autonomously (decided on 2026-10-09): open the
  moonpool PR, merge it once its CI is green, then advance paros's pin (the five `moonpool-*` lines of the root `[workspace.dependencies]` and `paros-core`'s own `moonpool-assertions` line).
- **Name every issue and PR number** (decided on 2026-10-10): write a short name next to it,
  e.g. "#216 (finish bootstrap)", in chats, comments, PR descriptions and commit messages.
- **Never edit a generated `CHANGELOG.md` by hand**: release-plz generates them, except
  `paros-core`'s, which is hand-written; the commit message is where a change is described.
- **A comment states the invariant in the present tense** (decided on 2026-11-15, decision 9 of
  the rework). A comment or a doc comment says what is true of the code now: the rule, why it
  holds, what breaks without it. The story of a change goes in the commit message: the seed
  witness, the commit hash, the date of a decision, the bug that a line fixed and the
  red→green result. A comment may keep one issue number when that issue names a design
  decision. Apply the rule file by file: when you change a file, rewrite the comments of the
  code you touch, and do not sweep the tree in a separate PR.
- **Eat your own food, a first-class citizen** (decided on 2026-10-09, `docs/architecture/01-goal.md` §1): solve
  every control-plane problem with paros's own journals, leader election and Paxos flavors;
  ask "which journal, which election, which flavor?" before building anything new.
- **Write documents in ASD-STE100 Simplified Technical English, most of the time** (decided on
  2026-10-09): `docs/`, the site, READMEs, AGENTS.md files, issues and PR descriptions.
  Short sentences, one idea per sentence, active voice, and the same word for the same thing
  every time. Use a paragraph of flowing prose where ideas connect, and bullets for lists of
  separate items. Code identifiers and protocol terms stay as they are. Leave the style when a
  sentence would lose precision. (The hard word counts were dropped on 2026-11-11: newer models
  follow them so closely that the prose gets choppy.)
- **Write for a reader who did not watch you work**: a PR description, an issue, a handoff and
  a final report are read by someone who has not seen your tool calls or the files you opened.
  Put the key points in the first sentences. Do not use a term you made up during the work.
  Say what you found, what you did, how you proved it, and what is next.
- **Subagents cost a fresh context each.** The agents in `.claude/agents/` do one narrow job:
  diagnose a seed, review a large diff against the doctrine, cite a paper, answer a moonpool
  question. Use one when the job is large or runs in parallel with yours. Do a check of a few
  tool calls yourself.
- **TLA+ is a thinking tool** (decided on 2026-11-12, #1020 (TLA+ ranked list)): when a
  protocol problem has more than one possible rule, you can write a small TLA+ model to find
  the solution, before you write Rust. Model each candidate rule as a constant, give each a
  control (a check that must find the bug with the rule off), and let TLC select. Put the model
  in `specs/tla/` when its rule ships (`specs/tla/README.md`); a model that only helped you
  think can stay out of the tree, with its result in the issue. A model does not replace the
  test: a trace that TLC finds becomes a red unit test before the fix.
- **Follow the papers; a change to them needs a TLA+ check** (decided on 2026-11-12, #579
  (zero-stall reconfiguration)): paros builds a protocol as its paper and its reference
  implementation give it. A change to that protocol (a different rule, or a rule that the paper
  does not have) is a proposal. It needs a model in `specs/tla/` before the rule is written in
  `paros-core`: a check that passes with the rule, and a control that finds the bug without it.
  A design document names each such change and its check.
- **Keep the docs in step with the code** (decided on 2026-10-11): a PR that changes behaviour
  also updates the `README.md`, the crate's `AGENTS.md` and `docs/architecture/` where they
  describe it.
- **Keep the knowledge base in step with the protocol** (decided on 2026-10-27, #745): a PR
  that adds or changes a Paxos concept paros uses updates its card in
  `docs/knowledge/concepts/` (a new concept gets its card in the same PR), and the index
  `docs/knowledge/concepts.md`. A card cites a source outside paros, never memory
  (`docs/knowledge/CLAUDE.md`).
- **Organize as you grow**: a module holds one concern; split a file the moment it holds a
  second. Deleting is part of every change: a superseded axis, type, flag or gate goes in the
  PR that supersedes it.
- **Layers point down only** (decided on 2026-10-24, #722 (three products)): each layer of the
  library is its own crate, and a crate depends only on the layers below it (see *Layout*). Put
  new code in the lowest layer that can hold it. A type that two layers share moves down; a call
  up goes through a port (a trait the upper layer implements).
- **One module style** (#722 (three products)): a module with children is `foo.rs` beside
  `foo/`, never `foo/mod.rs`, and no file is included with `#[path]`. The one `mod.rs` is
  `examples/common/mod.rs`, the code that examples share: Cargo builds `examples/common.rs` as
  an example of its own. CI runs `scripts/check-module-style.sh`.
- **Paper names are the vocabulary** (#722 (three products)): a role keeps the name of
  Compartmentalized Paxos (proxy leader, acceptor grid, replica, leaderless read, batcher,
  unbatcher) or Matchmaker Paxos (matchmaker, matchmaking) in `paros-core` and the docs.

## Architecture

**Sans-IO core, driver outside** (etcd-raft's `RawNode`/`Node` split: `ColocatedNode` in
`paros-core`, the driver in a crate above it). The core is a pure synchronous state machine:
`step`/`tick` in, one `Ready` out, `advance()`; no I/O, clock, RNG or deps. `ready(&mut self)
-> Ready<'_>` holds the unique borrow, so a second `ready()` before `advance()` is a compile
error. Persist-before-send ordering is documented on `Ready`/`HardState`
(`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`).

**One file per role; `ColocatedNode` is wiring** (timers, messages, the persist-before-send
batch) and holds **no protocol tally of its own**:

- `acceptor.rs` (+ `acceptor/retention.rs`, the floor-moving ops) — promise, records, floor, CTRL
- `proposer.rs` (+ `proposer/rounds.rs` the standalone Phase-2 tally, `proposer/authority.rs`
  the standing authority) — election, repair probe, recovery
- `replica.rs` — chosen prefix, apply walk, journal fold · `journal/state.rs` — the one
  journal-control state machine, judged at apply
- `quorum_read.rs` — leaderless reads · `collector.rs` — the leader-side GC tally
- `membership/` — `AcceptorConfig`, `MatchmakerSet`, `QuorumSystem`
- `matchmaking.rs` — the candidate's matchmaking phase · `matchmaker.rs` (+ `reconfigurer.rs`)
  — the registry and its generation handover · `decree.rs` — the generic single-decree
  Paxos (`Decree<Id, V>`): the handover's successor and `cell init`'s plan (#277)
- `proxy_leader.rs`, `replica_node.rs` — the second and third deployments

Beside the roles, the **foundations**: `types.rs` (the Paxos scalars), `journal/` (what the log
carries), `contract/` (what a driver honors: `Message`, `WriteOp`, `Storage`, `MemStorage`,
`HardState`, `Config`, `Limits`) and `retained.rs`. The three layers point down only
(foundations ← roles ← deployments, #730 (paros-core rework)): a role never uses a deployment,
and a deployment never uses another (`scripts/check-core-layers.sh`). Every module is `pub`;
the root re-exports only what a driver loop needs. A deployment takes its `Limits` at
construction and reports its counters in one snapshot (`ColocatedNode::counters`).

The map is `crates/lib/paros-core/AGENTS.md`. **A component must not acquire knowledge merely because
the current deployment colocates it**: the proposer builds no message, the acceptor never reads
the chosen prefix, the replica never sees a ballot tally; the caller hands each the data it
needs. **Every quorum question crosses `membership/`**: predicates are phase-split
(`has_phase1_quorum` / `has_phase2_quorum`, cross-intersecting) and no tally compares a count
against a threshold on its own; a new quorum system is a `QuorumSystem` variant, never a tally
rewrite. Policies are explicit types, never flags.

**No driver is on `main` today.** The driver of the service (`paros::run_node` and the other
`run_*` loops, generic over moonpool's providers, the same code in `parosd` and in the
simulation) left with the service at tag `last-service`
(`docs/archive/last-service/README.md`). A new runtime crate over moonpool's network and
storage traits will drive the core; until then the core's own examples, unit tests and models
drive it.

## Plain Multi-Paxos is a shape of the core, not of the service

Multi-Paxos without matchmakers — a fixed membership read once from storage — is a permanent
shape of `paros-core`, a library shape, not a transitional state. It is not a shape of the
service: each journal that `parosd` serves has matchmakers and reconfigures (decided on
2026-11-16, decision 15, question 2 (c), step 0.11 of the rework). Before touching
`on_check_leader`, `Election`, `HardState` or the role map:

- The static case is the **`None` arm of the same state machine**: never a cargo feature, never
  conditional compilation (`serde`, `tracing` and `assertions` are paros-core's only features, all observation-only).
- No matchmaker message, no `HardState` field and no extra round trip enters the plain path;
  `ColocatedNode` never steps a matchmaker message. Removing every opt-in feature must leave the
  plain program's behaviour unchanged.
- A reconfiguration request on a deployment without matchmakers is **refused**, never honored.
- The quorum systems are the majority and the grid (flexible quorums are deleted, decision 27,
  step 0.12 of the rework). Grids, matchmakers, proxies and replicas are **configuration data**,
  never implied by code being present. Every peer and client message carries a `JournalIdentifier`
  `(TenantId, JournalId)`; under that identifier the plain deployment exchanges the same
  messages and persists the same scalars.

## Matchmaking, reconfiguration, GC, generations

The rules below are the core's. Where a rule names a service half (the operator, the
coordinator, `SystemCtx`, placement), that half left with the service at tag `last-service`.

- **Matchmaking, then Phase 1.** A candidate registers `(b, C_b)` at a matchmaker quorum before
  any `Prepare`; histories are unioned above the **maximum** watermark into `H_b`. Phase 1 needs
  a promise quorum of **every** configuration in `H_b` — never `quorum(union)` — and Phase 2
  addresses `C_b` alone. A refusal abandons the campaign; a slow one is re-asked on election
  timeouts, never abandoned by the clock.
- **A belief is not a fact.** A registration is a belief (an ordinary campaign) or a
  reconfiguration; the **effective configuration** is the highest-ballot reconfiguration the
  matchmakers report: a candidate takes the maximum over the answers it heard, and a handover's
  `close_stop` the maximum over its stop quorum, so a reconfiguration that only a minority
  holds can become effective. A campaign whose histories name another abandons, adopts it and re-campaigns
  (`MatchStep::StaleConfiguration`), unless it sits at or below a leadership the node knows won
  (the probe floor, #278: a minority held it); beliefs never trigger that.
- **Membership probe** (`MembershipProbe`): every incarnation boots believing the bootstrap
  configuration and asks a matchmaker quorum for the effective one, registering nothing, before
  its first campaign or skip. A node only registers a belief it heard. A node whose heard belief
  leaves it outside re-probes on every election timeout (#270). A probe adopts a
  reconfiguration only above what the node knows is in force, the fact its belief matches or
  a leadership it knows won, never a campaign it only promised; a probe that closed with its
  node outside still folds its late answers (#278). A re-ask also asks the matchmaker pool
  outside the believed set; an outsider's answer counts only as a `Generation` refusal that
  names a later set (#858 (boot from the cached registry)). A campaign's re-ask on an election
  timeout does the same, and such a refusal abandons the campaign: a node that heard a belief
  naming it skips the probe and campaigns on its boot-time set, which may be gone for good.
- **A reconfiguration is a round change** (`ColocatedNode::reconfigure`): a configuration is bound
  to a ballot and never edited; the leader re-campaigns with `C_new`. **Removed is not shut
  down**: a removed node keeps answering Phase 1 for ballots it took part in (acceptor guards are
  pool-based, never configuration-based).
- **GC**: a configuration is forgotten only once no future leader can need its Phase-1 quorum. A
  floor is **effective only once a matchmaker quorum acked it durably** (`GcStep::Effective`);
  only then are acceptors retirable. The compaction floor and the GC watermark never wait on
  each other.
- **Retire carries the evidence**: the operator sends the effective watermark and
  `ColocatedNode::may_retire` checks six legs (matchmakers, not a member, not the leader,
  watermark above `last_member_ballot`, belief bound to exactly that watermark, the
  matchmakers' check of the registrations that name the node). The fence
  `last_member_ballot` also covers every campaign the node opened with a configuration naming
  it: the registration stays in the matchmakers' histories whether or not the campaign went on
  (decided on 2026-11-07; witness `3055323747627105565 515957:2164842326828835157` on 2949e4e). A leader's beat carries the floor it made effective
  (`Heartbeat::gc_floor`), and a node whose slot was **released** without a `Retire` keeps
  answering the journal until `may_retire` holds on a floor it heard
  (`ColocatedNode::may_retire_heard`, `SystemCtx::settle_lingering`): a released slot is not a
  retirement. The fence is durable and covers registrations of other nodes too (decided on
  2026-11-11, #1002 (retire fence gaps), option (b)): the registration fence is a node scalar
  (`WriteOp::RegistrationFence`, read back at boot), and a sixth leg needs every matchmaker
  of the set in force to have agreed, under the node's belief, that no registration at or above
  the floor names it (a membership probe whose answers carry `named`; `check_retirement`, a
  `Retire` refused `unchecked` until then). Each matchmaker that agrees records the node as
  retired, durably, before it answers, and refuses every later registration naming it
  (`MatchRefusal::Retired`, decided on 2026-11-12); the record crosses a handover as a union.
  A candidate drops from `H_b` a configuration that only abandoned ballots registered
  (decided on 2026-11-14, PR #1064 (gone-node gap)): every registration naming it lies strictly
  between the watermark and the ballot, and each ballot's owner promised the candidate with an
  owner testimony (`Promise::prepared`, the highest ballot it sent a `Prepare` at) below it.
  A history page keeps each owner's lowest registration of each configuration, so the
  candidate sees every owner (a page that hid one chose two values on the canary).
  A retired node that placement books again rejoins (decided on 2026-11-12, #1037
  (rejoin)): its boot reads a checked `RetireMark`, moves to the next incarnation durably and
  asks every matchmaker to drop the record (`MatchPurpose::Rejoin`), a quorum enough to name it
  again and nothing waiting for the rest. A boot that reads an unchecked mark above incarnation
  0 asks again at that incarnation: the mark is durable before the first `Rejoin` leaves, so a
  stop between the two would leave the node retired at every matchmaker (PR #1064 (gone-node
  gap)). A journal booked again while it still runs there
  (it lingered) rejoins the same way with no reboot
  (`ColocatedNode::rejoin`, called whether or not a check was asked); a check of an earlier
  incarnation is refused (`MatchRefusal::Rejoined`), and the later incarnation wins a handover.
  The plain path writes and sends nothing new. The operator's half: retire no node a
  reconfiguration it asked for above the floor names.
- **Matchmaker sets are generations**: every matchmaking message is fenced by generation, the
  coordinator's `ReconfigureMatchmakers` too (#563: the node refuses an ask whose generation is
  no longer in force, so a re-ask the read left stale never starts a second handover);
  matchmaker quorums are **majorities only**; the handover's decree is the shared `Proposer` +
  `Acceptor` over a one-slot log (no second Paxos kernel) and opens strictly above the stop
  quorum's maximum decree promise; any node that meets `Stopped { successor: None }` finishes
  the handover. `MatchmakerSet::new` / `AcceptorConfig::new` are the only constructors.

Depth: module docs of `matchmaking.rs`, `node/matchmaking.rs`, `node/reconfigure.rs`,
`node/gc.rs` (`may_retire`), `matchmaker.rs`, `matchmaker/reconfigurer.rs`,
`models/handover.rs`; `docs/analysis/consensus/matchmaker-gc-and-generations.md` and
`matchmaker-interaction-verification.md`.

## Storage and boot safety

- **Share processes, disks and connections, never protocol state.** Each journal has its own
  `ColocatedNode`, ballots, log and store; the one cross-journal property is non-interference.
  Every peer and client message carries the `JournalIdentifier { tenant, journal }` (#235).
  **No id is fixed and none has a default**
  (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8): `0` is unset and
  the only value with a meaning. Stores live at `journals/<tenant>/<journal>/`.
- **The storage seam is async** (`paros-storage`'s `LogStorage` / `MatchmakerStorage`: every
  device-touching method returns a `Send` future, awaited in persist-before-send order); the
  core's recovery ports stay synchronous, served from memory after `boot_scan`. The stores are
  `paros_storage::journal` on `moonpool-journal`, a CLSTORE journal: the state itself (slot =
  position, ballot in the entry's identity, scalars in its two-copy metainfo), no fold, no
  checkpoint. A store surfaces corruption as a typed verdict (`paros_storage::corruption`).
- **Boot safety is the library's job.** Every store carries a format marker; a driver takes the
  operator's `BootKind` as data and refuses `Amnesia` (an existing member with no marker),
  `AlreadyFormatted` and `ConfigMismatch` (the marker records the `Config`; the membership and
  quorum system are never edited across a restart; the counts are not recorded, #754). **A
  wiped node never rejoins**: a trim jump restores the floor, not the promise. The acceptor set
  heals around it by reconfiguration; a wiped matchmaker is replaced by a handover.

Depth: `crates/lib/paros-storage/AGENTS.md`; module docs of `paros-storage`'s `lib.rs`,
`boot.rs` and `journal.rs`.

## paros-core is never buggified

`paros-core` has no RNG, knob, clock, I/O or conditional compilation that decides anything. A
rare-but-valid decision is exposed as a method with an honest contract (`resend_pending`,
`step_down`) and perturbed only by a caller that stops calling. Faults belong to the driver and
to moonpool: when a driver comes back, its choices are inline `buggify!` sites and its
durability moments are `hint!("moment").await` calls at the line, never a sim wrapper around
shipped code (#294; the full doctrine is in `docs/archive/last-service/AGENTS-root.md`,
*Turbulence layers*).

## Assertions, probes, spans

- **`paros-core` uses hard `assert!`**, on in release; no `debug_assert!` anywhere. Never assert
  on what a correct peer or client can send (decided on 2026-11-16, decision 30): a correct
  peer can be late, can send a message two times and can restart, and a client can send any
  request; each handler ignores such a message or answers with a result, and puts a limit on
  each size. The transport proves the sender and the bytes (step 1.11 of the rework checks
  it), so a role trusts a peer of its pool.
  A message that breaks a Paxos rule (two commands at one slot and ballot) is a detected safety
  violation: the assertion stays, with a comment that names the rule
  (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.11). `ColocatedNode::assert_invariants` runs at
  boot and every public mutating entry; public functions that assert document `# Panics`.
- **Assertions are TigerStyle** (decided on 2026-10-04, #248): in `paros-core` and any driver of it, an average of at least two assertions per function; assert preconditions,
  postconditions and invariants; **pair** assertions (check a property where data is written and
  again where it is read back, e.g. at persist and at boot); assert the negative space as well as
  the positive; assert relationships between constants at compile time (`const _: () =
  assert!(..)`); split compound conditions into one assertion each. Every assertion is a
  free oracle for every test and simulation that runs the code. The trust rule above still holds: what a correct peer or
  client can send is a result, never a panic. Reference: TigerBeetle's `TIGER_STYLE.md`.
- **Coverage probes in `paros-core`** (decided on 2026-10-09, #317): a rare protocol branch a
  test or a simulation must reach is a `probe!(reachable, "..")` or `probe!(sometimes, cond, "..")` at the line
  (`paros-core/src/probe.rs`, the `assertions` feature, default on, backed by
  `moonpool-assertions`). A probe only observes: no buggify, hint, RNG, clock or knob; with the
  feature off it compiles to nothing. A probe is not an invariant: an impossible state is still
  a hard `assert!`. A probe and a moonpool `assert_reachable!` with the same message share one
  slot, so never reword a probe's message.
- **Spans**: in `paros-core` write `#[cfg_attr(feature = "tracing", tracing::instrument(..))]`.
  Use `skip_all` plus a few cheap `fields` (node id, `from`, `round`, `slot`); entry points at
  `debug`, per-message and per-tick at `trace`; no `ret`/`err`. Spans are for humans only.

Depth: the `changing-paros-core` skill.

## Protocol invariants to remember

- **Truncation is a Paxos-decided control command** (`Truncate`, proposed by the leader), **fenced
  by the leader uuid like a `Write`** (#228, #241): judged at apply, a stale or foreign caller is
  refused in place; an accepted one raises `first_seq` and every node compacts lazily when its
  walk reaches the slot. paros runs no application and takes no snapshot; record bytes are
  opaque.
- **The leader uuid is the writer fence** (#241): `SetLeader(new, old)` is a compare-and-set,
  `Write` and `Truncate` carry the uuid, and the core's term counter never reaches a data-plane
  reply (a client sees `JournalView`; an operator's `Inspect` shows the term). The journal trusts its clients to draw fresh uuids (decided on
  2026-10-09): a reinstated uuid wins, and the deleted simulation's `reinstate_pct` knob proved the
  guarantees hold through it (`docs/architecture/02-data-plane.md` §2.3).
- **A trim-point jump** (`Message::TrimmedTo`) recovers a below-floor node: it carries a floor
  and the journal state, no bytes and **no ballot**; the promise never moves.
- **Cooperative handoff** (`DPaxos`, `relinquish_to`): abdication is synchronous with the
  decision, the successor is named inside the payload, and only the elected minter of a ballot
  may hand it on (one hop). A failed handoff costs availability, never safety
  (`docs/analysis/consensus/dpaxos-leader-handoff.md`).
- **Election gap fill**: a new leader re-proposes every reported slot *and* fills every
  unreported slot below its frontier with a `Noop`, because pipelining can strand a slot no one
  would ever propose again, freezing the chosen prefix cluster-wide.
- **The allocator frontier is durable by construction**: a leader records every round it opens
  (`record_own_round`).

## Layout

Cargo workspace, every package under `crates/`. Each crate's `AGENTS.md` is its map.

- `crates/lib/paros-core` — the sans-IO roles and `ColocatedNode`; dependency-free with
  `default-features = false`, wasm-safe. Its optional, default-on `assertions` feature (#317)
  adds `moonpool-assertions` for coverage probes only.
- `crates/lib/paros-storage` — the `LogStorage` seam, the moonpool-journal stores,
  provisioning, boot, and the typed corruption verdict. It depends on `paros-core` only among
  the paros crates.
- `crates/testkit/paros-testkit` — the judges of a journal that hold no cell: the
  linearizability checker (`linearizability`) and the journal model (`journal_model`),
  moved out of the deleted `paros-sim`. `publish = false`.

Elsewhere: `docs/architecture/` (the goal), `docs/knowledge/` (the Paxos knowledge base: one
card per concept paros uses, #745), `docs/analysis/` (design notes), `docs/references/`
(papers and source references), `docs/archive/last-service/` (the state of the deleted
service), `specs/tla/` (small TLA+ models of a protocol rule, checked with TLC before the rule
is written in `paros-core`; `specs/tla/README.md`), `scripts/` (`check-tla.sh`,
`check-module-style.sh`, `check-core-layers.sh`), `REVIEW.md` (what a reviewer checks for each
kind of diff), `.claude/skills/` and `.claude/agents/`.

Publishing: release-plz releases `paros-core` and `paros-storage` in one `version_group`, with
one `v{version}` tag and GitHub release on `paros-core` (`release-plz.toml`). `paros-testkit`
is `publish = false`. Each moonpool line of the root `[workspace.dependencies]` (and
`paros-core`'s own) carries a git `rev` and a crates.io `version`: a build uses the rev, a
publish resolves the version instead. Cargo checks that the pinned commit has that version, so
advance both together, to the moonpool release that holds the commit.
