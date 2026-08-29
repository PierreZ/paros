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
work into "Recently landed" (one line: PR number, what it proved/fixed, seeds pinned), promote
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
a fast **smoke** (`SMOKE_ITERATIONS`, a few dozen random seeds through the safety oracles) plus the
pinned `REGRESSION_SEEDS`; they do **not** assert coverage saturation. So: to prove a new red→green
oracle result saturates, run `cargo xtask sim`; the nextest suite just keeps the safety oracles
green quickly. Do not put a multi-thousand-iteration `explore()` back into a nextest test.

**Raw hunt budget.** For `sim-paros-hunt`, 2,000–3,000 ordinary seeds is the normal evidence
target. Raise that to 10,000 only when a substantial protocol, harness, or fault-model change is
introduced. Do not run larger hunts unless the user explicitly requests one; coverage-guided
saturation still belongs to `cargo xtask sim` and is not replaced by raw seed volume.

**Chain campaign.** `paros-chain` drives a factory-created Chain-of-Blocks workload with stable
operation IDs: `PROPOSE=0`, `PROPOSE_TO_NON_LEADER=1`, `COMPACT=2`, `READ_STATE=3`, `PAUSE=4`,
`DUP_REPROPOSE=5`, `DUAL_SUBMIT=6`, `COMPACT_STORM=7`, `READ_INDEX=8` (the public
leadership-confirmed read, vs. `READ_STATE`'s internal inspect probe).
Its application state folds every user, `Truncate`, and `Noop` command into `(applied_count,
chain_hash)`; `NodeStorage::apply` is the production-generic application seam and snapshots carry
that opaque state. `ChainAgreement` checks one command/state per applied index, contiguous local
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

Sans-IO core driven by moonpool (etcd-raft `RawNode`/`Node` model). `paros-core` is a pure
synchronous state machine — `step`/`tick` in, one `Ready` out, `advance()` handshake; no I/O, clock,
RNG, or deps. The `ready()`/`advance()` handshake is type-enforced: `ready(&mut self) -> Ready<'_>`
holds the node's unique borrow, so a second `ready()` before `advance()` is a *compile* error.
Persist-before-send durability ordering is documented on `Ready`/`HardState`. Contract reference:
`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`.

The **driver** (`paros::run_node`, the etcd-raft `Node` layer) owns the `RawNode` and does all I/O.
It is written **once, generic over moonpool's `P: Providers`** (and `S: NodeStorage`), so the *same*
code runs in production (`TokioProviders` + a future `parosd` binary) and deterministic simulation
(`SimProviders`). The boundary is the only thing that differs: `paros-sim` adapts it to a moonpool
`Process`; production adapts a `tokio::main`. This "test the code you ship" rule is load-bearing —
protocol logic added in later stages lives in the provider-generic driver, never in a sim-only path.

**Storage direction.** paros does **not** use moonpool's storage layer: it is too low-level for
what paros needs. The storage seam stays the high-level `NodeStorage` trait (apply / snapshot /
truncate / install_snapshot semantics), with the in-memory + sim implementations behind it. For
production we will later search for and adopt an existing high-level storage engine rather than
building on moonpool's primitives. (Moonpool's *storage chaos* still applies in simulation — it
perturbs the environment, not the abstraction we code against.)

**Where each kind of turbulence lives.** Three layers, and nothing crosses them (this is the FDB
separation; #81 removed the message-class nemesis, which mixed them):

- **Environmental faults belong to moonpool.** Drop, delay, duplicate, reorder, directional
  partitions (`AsymmetricSend`/`AsymmetricRecv`), random close, bit-flip, buggified delay,
  crash/restart attrition, seeded-random scheduling — all swarm-masked per seed. paros never
  re-implements one of these at the protocol layer.
- **`paros-core` is never buggified.** No cargo feature, no conditional compilation, no RNG, no
  knob: the sans-IO core stays unconditionally pure, and it is perturbed **only through its public
  API** — the methods its caller chooses to call, and the data it is handed. Where a rare-but-valid
  decision needs to become reachable, the core's job is to *expose that decision as a method with an
  honest contract* (`RawNode::resend_pending` — "the driver is expected to call this each beat;
  skipping is always safe, re-send is pure optimization"; `RawNode::step_down` — "a leader may
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
- **BUGGIFY, prong 2 — tunables are workload-buggified config.** Anything that *shapes* a run — the
  cluster size, request counts, timing windows, attrition knobs (the #61 swarm surface) — belongs in
  plain config data that the **workload/harness layer** randomizes
  per seed, FDB knob style (`if buggify → an extreme value, else the default`). New tunables should
  be **born that way**, as data a workload can buggify, not as a constant buried in core or driver
  code, so per-seed swarm variation composes without either layer knowing about it.

The driver's provider-generic `DriverHooks` also exposes the durability seams process-level
attrition cannot reach — five today: `BeforeSync`, `AfterSyncBeforeSend`, `AfterApplyBeforeSync`,
and the chunk-repair pair `BeforeChunkSync` / `AfterChunkRestoreBeforeSync`. Give each seam its
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
the only party that knows its own program order. Tracing stays for humans and the wasm demo, and
`oracle.rs` keeps only the demo-data recorders plus `ChainAgreement` (the *application* state
machine, whose transitions the storage layer emits as trace facts). Do not add a new
`Invariant` that re-scans an event stream to check the protocol: the scan is O(trace²) across a
run's observability pumps, and the audit callback for that transition already exists or is one
method away. Preserve assertion **message strings** when moving a check — the assertion slot is the
hash of its message, so a reworded message silently resets the sweep's saturation history.

**Assertion doctrine (TigerBeetle-style).** Two assertion families, split by layer, and neither
substitutes for the other:

- **`paros-core` uses hard `assert!` — always on, in production too.** A broken invariant is a
  programmer error, never an operating condition: crash beats corruption. Operating errors (a
  non-leader proposal, a stale snapshot, a below-floor prepare) stay result values / guarded
  returns — never assert on external input; re-assert it only once it has crossed the validation
  boundary. Style rules: precondition stacks at function entry, postconditions at exit, split
  compound conditions, assert positive *and* negative space, pair each property across two code
  paths (e.g. the write-side flush ordering vs. the boot read-back). `RawNode::assert_invariants`
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
  Pair every BUGGIFY site with a sometimes/reachable proving it fired. **Budget:** one slot per
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
  asks the **leader** to truncate (the `Compact` RPC → `RawNode::propose_control`); the leader
  proposes `Truncate` only once a quorum advertises custody of a decided snapshot point covering
  it (otherwise it seeds a `Snap` marker and answers `accepted: false` — clients can be refused
  and retry), decides it by ordinary consensus, and every node truncates *lazily* when it
  applies that slot (`RawNode::compact`, `WriteOp::Truncate`), giving **one cluster-wide floor**
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
  enter `RawNode`); a node can restore its application from a decided point it holds. The bytes
  stay opaque throughout — paros ships, stores, and checksums them, never reads them.

A **wiped** node that lost its durable *promise* (amnesia: a lost disk, not a clean crash) is still
out of scope: a snapshot restores the log, not the promise, so a naive rejoin can regress the
promise (a real safety violation). That belongs to the disk-fault stage (`prob_wipe` stays 0).

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
surfaces the failure through `RawNode::chosen_gap()` (the `Ready` handshake only ever hands out the
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
   it yet), a client-observable one in the workload's own history + `check()`. Only reach for a
   trace-scanning `Invariant` when the fact exists nowhere else (application state, simulator
   faults) — and then read it through a cursor, never a re-scan.
4. Run the sweep, confirm it goes **red** on the unfixed code, and record the failing seed.
5. Fix `paros-core`.
6. Run the sweep, confirm it goes **green** and saturates.

A regression unit test may *pin* the bug afterward, but it never replaces step 4. A critical claim
the simulation cannot reproduce is treated as **unproven** (it is probably not a real bug: safety is
often preserved by an invariant you missed). Do not add speculative defensive code for an
unreproducible claim.

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
Dependency stack: `paros-core` ← `paros` ← `paros-sim` ← {runner, wasm-demo}.
`paros-core` has no deps; everything ultimately points into it.

- `crates/paros-core/` — sans-IO Multi-Paxos state machine: zero *default* deps, std-only, wasm-safe (the
  optional `serde` feature adds derives only, and it is the crate's *only* feature — see the
  turbulence doctrine above: the core is never buggified and gains no simulation-only conditional
  compilation). Sancov crate-under-test; exempt from the global `#[instrument]`-on-pub-fns rule
  (must stay zero-dep by default).
- `crates/paros/` — **the library.** Re-exports `paros-core`, plus the provider-generic driver
  (`run_node` over `P: Providers`, `S: NodeStorage`), the default in-memory `MemStorage`, and the
  node RPC contract (`Propose`/`ProposeAck`). The client API + a `parosd` binary land here. Deps:
  `paros-core`, `moonpool-core` + `moonpool-hyper` and runtime-free tonic (wasm-safe). No
  dedicated storage crate — the Stage-4+ faulty fake lands here or in the harness.
- `crates/paros-sim/` — the DST harness on top of `paros`: the moonpool `Process` adapter, workloads,
  oracles (wasm-safe, `default-features = false`). Depends on `paros` + `moonpool-sim`.
- `crates/paros-sim-runner/` — native sim runner binary (`publish = false`).
- `crates/paros-wasm-demo/` — browser/wasm demo, `cdylib` + `rlib` (`publish = false`).
- `crates/xtask/` — build automation (the sancov sim runner).
- `docs/references/papers/` — Paxos/consensus papers with transcripts.
- `docs/analysis/` — design notes (e.g. sans-IO patterns for Multi-Paxos).

Publishing/changelogs mirror moonpool: library crates share a `version_group` with per-crate
`CHANGELOG.md` (release-plz); binaries/demos/xtask are `publish = false`. Note: `paros` and
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
