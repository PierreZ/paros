# The hint API: production code tells the simulator about a moment (#294)

Decided on 2026-10-09 (Pierre approved the recommendations of section 13). Line numbers point
into the two checkouts as read that day.

## 1. The question in one sentence

What is the smallest thing production code must say so that a deterministic simulator
can strike at the moments the code knows are interesting?

The answer this note defends: **the code names a moment. It never names a fault.**
The simulator decides whether to strike, with which fault, and how hard, from the seed's
own chaos configuration and from the state it already owns. The code's statement is a
buggify site with a label. That is all.

## 2. What the code expresses (question 1)

Four candidates were on the table. Each is judged by one test: does it tell the
simulator something it cannot see by itself?

### (a) A named fault: "reboot me"

This is FDB's `throw please_reboot()` behind a `buggify()`
(`fdbserver/storageserver/storageserver.cpp:13158`). It is precise and it is proven.
Its cost: the production code chooses the fault. A site that says "reboot" never tests a
failed sync at the same moment. A site that says "partition me" is wrong at a storage
seam. Pierre already moved away from it: "everything is a hint", "reboot will choose
what kind of reboot".

### (b) A weakness taxonomy: `Unsynced`, `Unannounced`, `Awaiting`, `Recovering`

This is the draft in moonpool `0f40fe9`. The code describes its own state. The
simulator keeps a table from weakness to fault family. Two problems. First, the table
is a policy about paros's protocol living inside moonpool. Every new paros state asks
for a new variant and a new mapping. Second, the taxonomy duplicates what the simulator
already knows: `Unsynced` is the disk's dirty-sector set (`storage/image.rs:590`,
`dirty_sectors`), `Awaiting` is the network's pending deliveries. The only variant the
simulator cannot see is `Unannounced`: durable but not yet sent. And that one is a
moment, not a state. It has zero width in the code.

### (c) A resource-scoped statement: "file X has unsynced writes", "RPC to Y in flight"

This is pure duplication. The simulated disk owns every unsynced sector per file and per
process. The simulated network owns every pending send per connection. `moonpool-rpc`
owns every in-flight call. A statement from the code adds nothing and can disagree with
the ledger.

### (d) Nothing new: the simulator infers from its own state

This is right for *what* is at stake and wrong for *when*. The disk knows the batch is
dirty. It does not know the batch is about to be synced, or that the sync just returned
and the `Accepted` replies are about to leave. Attrition lands "wherever a task happens
to yield" (`crates/paros/src/hooks.rs:3`). The persist-then-send edge is one await
wide. Volume alone does not hit it. The code must name the moment.

### The recommendation: a labelled point, no taxonomy

Combine (d) for the *what* with the shape of (a) for the *when*:

- The code calls `hint!("batch durable, not sent").await` at the moment.
- The label is a human string and a coverage bucket. It carries no semantics the
  simulator acts on.
- The simulator strikes the **calling process**. It chooses the fault among the families
  the seed enabled: a reboot (any moment), a storage fault on the process's next disk
  operations, a cut of the process's links. It weighs the choice by what it owns: dirty
  sectors make storage faults worthwhile, pending sends make network faults worthwhile.
  A reboot is always worthwhile. That weighing is internal to moonpool and can start as
  "always reboot".

This answers Pierre's doubt directly. Storage and network do not share one *weakness*.
They share one *moment*. A moment is the same thing for both. What differs is the strike,
and the strike is the simulator's. A reboot is the one fault that tests both at once: it
loses the unsynced bytes and the unsent messages. So "reboot for both" is not a
coincidence; it is why the first PR maps every hint to a reboot and is already useful.

## 3. Point versus window (question 2)

A **point** is a moment: "here, now". A **window** is a span: "from here until the
guard drops".

Points are necessary. The seams are points. The cell-init promise is a point. The step
between the promise commit and the entries commit in `JournalStorage::sync`
(`crates/paros/src/journal/node.rs:481-557`) is a point.

Windows are not necessary in the first version, for two reasons:

1. The storage window Pierre described, "pending work not saved durably", is the disk's
   dirty-sector set. moonpool can weight attrition's victim draw by it with **no API**:
   `AttritionInjector::inject_process` draws one victim; the draw's weights can read the
   storage engine. One draw, as today. This is a moonpool-internal improvement.
2. A protocol window that the simulator cannot see has a start the code knows. A point at
   the start, with a strike the sink may *delay*, covers it. The mid-commit power cut is
   this shape. Landed (#294 step 6, moonpool#317) without a delayed strike: the journal
   names the instants inside the span itself (entries written, records written, metainfo
   stale), so a plain point at each covers it. `PowerCut` and its timer race are deleted.
   The copy budget stays the harness's, through moonpool's `HintVeto`
   (`crates/paros-sim/src/world/cut.rs`).

How each injector consumes a point, without breaking determinism:

| injector | consumes a point how | draws |
|---|---|---|
| attrition (`fault_injector.rs:650`) | the sink asks the regime whose victims filter admits the process; `max_dead`, kind weights and recovery delay apply as for a timed reboot | one kind draw, one delay draw, inside the sink call |
| storage faults | the sink arms a short "focus in time" on the process's disk: the next `n` operations draw the enabled families at a raised rate; a `FaultFocus` in time, next to the one in sectors (`storage/provider.rs:49`) | one draw for `n` |
| network faults | the sink cuts the process's links for a drawn duration (`FaultContext::partition` per peer, or a black hole on the process) | one duration draw |

Every draw goes through the installed `RandomSource`, inside the call, from the task
being polled. A point never draws when its site is not active. The swarm masks are
untouched: a family the mask removed is never chosen, and choosing among the remaining
families is one draw that happens only when the site fires.

## 4. Who decides (question 3)

Three layers, each a veto, none a command:

1. **The call site.** A buggify location. Activated once per run at the run's activation
   probability. Fires per call at a rate. The rate is a literal at the site, FDB style,
   with a crate default of 5% (`POINT_PROB`). Silent in the recovery tail
   (`buggify_fault_internal`). A site at rate `1.0` is a per-seed scenario: activation
   is the scenario draw.
2. **The seed's chaos.** The sink consults the regimes and masks the seed drew. A seed
   with `max_dead = 0` never reboots on a hint (`runner/process.rs:284`). A seed whose
   storage mask dropped `SyncFailure` never fails a sync on a hint. The sink picks one
   family among those enabled; the pick is the sink's one draw.
3. **The budget.** `max_dead` over the eligible pool, as for timed reboots. A
   process a regime protects (the quiet seats, FDB's `protectAddress`) is never
   killed; it may still get a storage or network strike.

"Low probability" is the product: activation × fire rate × family enabled × budget.
In the sweep, 25% activation × 5% fire on a site reached ten times per run gives
roughly 10% of seeds one strike at that site. That matches the seam rates
`BuggifyHooks::crash_at` uses today, which is where "measure the per-seed rate before
and after" (AGENTS.md, "Amplify the worst states") keeps its meaning.

No rate lives in moonpool's config. No rate lives in paros's `DriverTunables`. A rate is
a literal next to the moment it describes, and the activation knob is the run's.

## 5. Attribution (question 4)

The sink must know which process called it. FDB uses `g_simulator->getCurrentProcess()`.
moonpool has no such thing today; only the `process` tracing span carries the ip.

Proposed, as in the draft: `TaskMeta.owner: Option<IpAddr>`, set on a process's root
task by `spawn_process` (`runner/process_manager.rs:101`), inherited by every task
spawned while one of its tasks polls, and a thread-local `CURRENT_OWNER` the executor
sets around each `runnable.run()` (`executor/mod.rs:477`). This is one field and one
cell. The alternative, a handle passed into `run_*`, is a hook again, and Pierre wants
hooks gone.

A hint from the driver, a workload or an injector has no owner. The sink answers
"no strike" and records `assert_reachable!("hint: a hint outside a process is ignored")`.
That makes a client-side hint (the fleet session, the checkpointer) a no-op by
construction; see section 8 for what stays a harness injector.

File and peer are not needed. The simulated disk is scoped by owner ip
(`SimStorageProvider::new(sim, owner_ip)`), and so is the network. A later version may
add `hint!("..").file(path)` or `.peer(addr)` to narrow a strike; nothing in the first
version needs it.

## 6. Production cost, inertness, wasm, dependencies (question 5)

- `hint!` expands to `buggify_fault_internal(prob, location)`. With buggify disabled
  that is one thread-local borrow and one `bool` read; no draw, no allocation. The
  returned future is `Ready` and `.await` on it is a no-op after inlining.
- `is_simulated()` is one thread-local read. paros uses it to tilt a rate or a cadence.
  It never changes a result a client can observe (rule to record in AGENTS.md).
- `moonpool-buggify` stays `std`-only with zero dependencies. `thread_local!` compiles on
  `wasm32-unknown-unknown`. The paros wasm gates (`cargo check --target
  wasm32-unknown-unknown -p paros`) must pass with the new dependency; moonpool's
  portability check gains `-p moonpool-buggify` for the same target.
- `paros-core` gets nothing. No buggify, no hint, no probe. Every site lives in `paros`
  and `parosd`.
- `Sink` is `Box<dyn Sink>` in a thread-local, set by `buggify_init` and cleared by
  `buggify_reset` and at the recovery boundary. Simulated ⇔ a sink is installed.

## 7. Probes (question 6)

`reachable!` is FDB's `CODE_PROBE` (`flow/include/flow/CodeProbe.h:331`). A probe is
accounting, not a decision, so it belongs in `moonpool-assertions`, the zero-dependency
crate that already owns the slot table and `assertion_bool` (`slots.rs:320`), not in
`moonpool-buggify`.

- `moonpool-assertions` gains `reachable!(msg)` and `sometimes!(cond, msg)`, both over
  `assertion_bool`. A slot is the hash of its message (`msg_hash`), so a probe in paros
  and the sim's `assert_reachable!` with the same text already land in one slot. The
  macros make the production spelling exist; they do not change hashing.
- `moonpool-sim` re-exports them, and its `assert_reachable!` keeps its extra tracing.
- Inert path: `assertion_bool` returns at `find_or_alloc_slot` when no table is
  installed. Ask moonpool to check the region pointer *before* hashing the message, so a
  production probe costs one pointer read and nothing else.
- Rule for paros: a probe message is never reworded. A changed probe is a new message,
  and the old one is deleted in the same PR.

No merge of the two crates. Each keeps one concern: `moonpool-buggify` decides,
`moonpool-assertions` counts. `paros` depends on both.

## 8. The API

```rust
// moonpool_buggify (std only, zero deps, wasm-clean)

/// Buggify is enabled and a random source is installed: this thread runs a simulation.
pub fn is_simulated() -> bool;

pub mod hint {
    /// What the simulator did with a hint that fired.
    pub enum Strike {
        /// Nothing: no family fits, the seed's chaos forbids it, or the caller is no process.
        None,
        /// A fault that lets the caller go on (a storage focus, a cut link).
        Struck,
        /// A kill is scheduled: the caller's future never resolves.
        Killed,
    }

    /// The simulator's side, installed per thread for one run.
    pub trait Sink {
        fn strike(&self, label: &'static str) -> Strike;
    }
    pub fn set_sink(sink: Box<dyn Sink>);
    pub fn clear_sink();

    /// The crate default firing rate of an active point, per call.
    pub const POINT_PROB: f64 = 0.05;

    /// Report a moment. Use `hint!`, which names the location.
    #[must_use = "await it, so no code after the moment runs once the process is killed"]
    pub fn at(label: &'static str, prob: f64, location: &'static str) -> Hinted;

    /// Ready unless the strike killed the caller; then Pending forever.
    pub struct Hinted { .. }
    impl Future for Hinted { type Output = (); .. }
}

/// `hint!("label")` at the default rate, `hint!("label", 0.2)` at a site rate.
#[macro_export]
macro_rules! hint {
    ($label:literal) => { $crate::hint::at($label, $crate::hint::POINT_PROB, concat!(file!(), ":", line!())) };
    ($label:literal, $prob:expr) => { $crate::hint::at($label, $prob as f64, concat!(file!(), ":", line!())) };
}

/// A `rand`-free draw over the f64 source, for the sites that pick a value.
#[macro_export] macro_rules! buggify_pick { ($prob:expr, $n:expr) => { .. } }   // Option<usize>
#[macro_export] macro_rules! buggify_range { ($prob:expr, $range:expr) => { .. } } // Option<u64>
```

```rust
// moonpool_assertions
#[macro_export] macro_rules! reachable { ($msg:expr) => { .. } }
#[macro_export] macro_rules! sometimes { ($cond:expr, $msg:expr) => { .. } }
```

```rust
// moonpool_sim (internal): the sink
pub(crate) struct HintSink { ctx: FaultContext, regimes: Vec<AttritionInjector> }
impl Sink for HintSink {
    fn strike(&self, label: &'static str) -> Strike {
        let Some(ip) = executor::current_owner() else { return Strike::None };
        // v1: the first regime that admits `ip` decides a reboot under its budget.
        // v2: one draw among {reboot, storage focus, link cut} restricted to the
        //     seed's enabled families, weighted by the engine's state for `ip`.
        ..
        assert_sometimes_each!("hint struck", [("label", hash(label)), ("kind", kind)]);
    }
}
```

The `Hinted` future is taken at the call, not at the poll. A kill lands within one
scheduler tick through `Event::ProcessForceKill`, the `SelfCrash::crash` path
(`runner/context.rs:277`). Every unsynced sector resolves by the disk's crash physics
(`storage/image.rs:379`). The recovery delay is the regime's, not paros's.

## 9. Call sites in paros

Four, from the shipped paths. Each replaces a `DriverHooks` method and its sim arm.

**The persist-then-send edge** (`crates/paros/src/driver/ready.rs:220-241`), two
seams today:

```rust
use moonpool_buggify::hint;

// inside persist_writes, writes staged, before the flush (was Seam::BeforeSync)
if !writes.is_empty() {
    hint!("batch staged, not synced").await;
}
storage.sync(must_sync).await?;

// back in drain_ready, after persist_writes, before the sends (was AfterSyncBeforeSend)
if !writes.is_empty() || !messages.is_empty() {
    hint!("batch durable, not sent").await;
}
send_messages(out, audit, journal, messages);
```

`audit.crashed(node, seam)` goes. The boot that follows reports what it found
(`Audit::recovered`), and the sweep's gate is the `sometimes_each` label bucket the
sink records. `Seam` survives as the audit's label enum only where a boot can tell the
seams apart; otherwise it goes too.

**The matchmaker's registration** (`crates/paros/src/matchmaker/mod.rs:207-221`):

```rust
hint!("registration staged, not synced").await;
storage.sync().await.map_err(|e| storage_fault_crash(audit, id, e))?;
hint!("registration durable, reply not sent").await;
reply(..);
```

**The cell init promise** (`crates/paros/src/machine/wait.rs:267-271`):

```rust
ledger.promise(ballot).await?;
hint!("cell promise durable, ack not sent").await;   // was crash_at(Seam::CellPromised)
```

`RunError::SeamCrash` goes: a killed process never returns. `paros-sim/src/machine.rs::
seam_crash` and `restart_delay!` go with it.

**The three commits of one sync** (`crates/paros/src/journal/node.rs:481-557`), the
#264 shape made likely:

```rust
self.commit(promise).await?;                 // the raised promise, alone
hint!("promise durable, entries staged").await;
.. commit the packed entry batches ..
hint!("entries durable, metainfo staged").await;
self.commit(staged).await?;                  // floor and metainfo ride the last batch
```

The cut *inside* one commit, between the segment write and its sync, is
`moonpool-journal`'s own seam (`journal.rs:638`, `commit`). moonpool-journal can hold
that `hint!` itself: it depends on `moonpool-core` only, and `moonpool-buggify` is
zero-dep. Then every user of the journal gets mid-commit cuts, and `PowerCut`'s timer
race is deleted from paros rather than moved.

**A hook that is not a hint** (`crates/paros/src/driver/mod.rs:516`,
`skip_accept_resend`): the driver's own rare-but-valid decision needs no simulator
action, so it is a plain inline site:

```rust
if pending_accepts && !moonpool_buggify::buggify_with_prob!(0.95) {
    node.resend_pending();
}
```

## 10. Mapping every existing site (question 7)

| today | after | kind |
|---|---|---|
| `Seam::BeforeSync`, `AfterSyncBeforeSend` (`ready.rs`) | two `hint!` points, section 9 | hint |
| `Seam::AfterPrepareSent` (#260) | `hint!("reconfiguring prepare sent")` after the send | hint |
| `Seam::MatchBeforeSync`, `MatchAfterSyncBeforeReply` | two points in `matchmaker/mod.rs` | hint |
| `Seam::CellPromised`, `CellFormatted` (`wait.rs`) | two points; `seam_crash` deleted | hint |
| `world/power.rs` `PowerCut::around` | points between the sync's commits (paros) and inside `commit` (moonpool-journal); the copy budget a `HintVeto` (landed, #294 step 6) | hint |
| `seam_crash_bias` ×10 | a higher rate literal on the write-side points (`hint!(.., 0.2)`) | rate |
| quiet seats outside the cut (`shape.rs`) | the regime's victims filter; a protected process gets no kill | moonpool config |
| `restart_delay!`, `seam_crash` recovery draw | the regime's `recovery_delay_ms` | moonpool |
| `delay_boot` | `if buggify_with_prob!(0.1) { time.sleep(buggify_range!(1.0, 250..2_501)) }` in `run_machine` | inline |
| `skip_*_resend`, `resign_leadership`, `stretch_tick_interval`, `expire_parked_read_early`, `skip_delegation`, mailbox hooks (`overtake`, `hold`, `reverse`, `evict`), election-timeout extremes, `abandon_reconfigurer` per phase | `buggify_with_prob!(p)` inline, same rates as `BuggifyHooks` | inline |
| `drop_outgoing`, `duplicate_outgoing`, `drop_client_reply`, `duplicate_client_reply` | `buggify_with_prob!` at the send and the reply, one site per message kind that has its own gate | inline |
| `handoff_target`, `phase2_column`, `read_row`, `proxy_for`, `initiate_handoff` | `buggify_pick!(p, n)` inline; `HandoffContext` stays as the tilt's input | inline |
| `withhold_gc_requests`, `hold_journal`, `lose_verdicts` (per-seed latched) | `buggify_named!(label, p)` at the site; the scenario draw decides it with `set_activation` (done, #318 E: `paros::scenario`) | inline, scenario |
| `DriverTunables` draws (`NodeShape`) | stay knobs, drawn by the harness: configuration, not a decision | unchanged |
| `world/injector.rs` boot damage | stays: storage chaos aimed by `FaultFocus`; `upstream-to-moonpool` candidate | environment |
| `ScriptedLifecycle`, `fleet.rs` target kill, late and bare outages | stay harness injectors over audit facts (FDB's `MachineAttrition`) | environment |
| fleet "stop after one step", checkpoint-then-stop | stay: an operator's explicit misbehaviour in the workload | operator |
| `DriverHooks`, `NoHooks`, `BuggifyHooks`, `RunError::SeamCrash`, `SimDisk`, `DirDisk` | deleted, one family per PR (all done; `DriverHooks` with #318) | — |

Two rules fall out. A **hint** is a moment where an *environmental* fault would be
interesting. An **inline buggify** is a *decision of the code* that needs no
environment. Nothing is both.

## 11. The first moonpool PR (question 8)

**PR A: points, reboot only.** Useful to paros on its own: it deletes the seven seams,
`seam_crash`, `restart_delay!` and `RunError::SeamCrash`.

- `moonpool-buggify`: `is_simulated()`, `hint!`, `hint::{Sink, Strike, set_sink,
  clear_sink, at, Hinted, POINT_PROB}`, `buggify_pick!`, `buggify_range!`.
- `moonpool-sim`: `TaskMeta.owner`, `CURRENT_OWNER`, `spawn_owned`; `HintSink` mapping
  every hint to `AttritionInjector::reboot_on_hint` under the admitting regime; sink
  installed by the orchestrator for the chaos window, cleared with
  `buggify_enter_recovery`; `assert_sometimes_each!("hint struck", label, kind)`.
- `moonpool-assertions`: `reachable!`, `sometimes!`; the pointer check before the hash.
- Tests: a hint inside a process schedules the kill and stays pending; a hint from a
  workload resolves; a hint under `max_dead = 0` resolves; one seed, two runs, one
  digest (`check_determinism`).

Deferred, in order: **PR B** storage and network strikes in the sink (one family draw,
weighted by engine state), the delayed strike for in-span cuts, and `moonpool-journal`'s
own commit point. **PR C** windows, only if a gate asks for one after PR B. Client-side
hints stay out until a workload needs the sink to reach a *named* process.

paros follows the plan in `docs/analysis/simulation/production-fault-hints.md` §4, with
one change: the `Weight`/`Fragile` window API and the `Weakness` enum are not built.

## 12. Trade-offs

- **A label is weaker than a type.** A typo makes a new bucket. A `&'static str` also
  cannot be matched on. Accepted: the simulator never matches on it, and the sweep shows
  every label it saw.
- **The sink's one draw shifts every seed** when a new family joins (PR B). That is the
  normal cost of any added draw; the canary proves determinism, not seed stability.
- **A hint cannot aim at another process.** The fleet killer and the outages stay harness
  injectors. That is consistent: those are environment events timed by audit facts, not
  moments of the code that dies.
- **Rates in production source.** Pierre accepted this. The rule: a rate is a literal, a
  default of 5%, and never a knob.
- **No `Weakness` means no "what would be lost" in the trace.** The label and the boot's
  `Audit::recovered` say it instead, and `sometimes_each` pairs label with strike kind.

## 13. Open questions for Pierre

Each answerable in one word.

1. Label as a free string, or a closed enum? (recommended: string)
2. A site rate literal next to the code, with a 5% default? (recommended: yes)
3. `reachable!` in `moonpool-assertions`, not `moonpool-buggify`? (recommended: yes)
4. `moonpool-journal` hints its own write-then-sync seam? (recommended: yes)
5. Windows in the first version? (recommended: no)
6. Hints from client code (workload process) in the first version? (recommended: no)
7. Keep `Seam` as the audit's label enum, or let the label replace it? (recommended: replace)
