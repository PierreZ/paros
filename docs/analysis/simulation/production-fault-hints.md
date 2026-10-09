# Production code hints its own failures (#294)

Decided on 2026-10-09. No `SimMachine`, no `SimDisk`: the simulation runs the shipped machine
and disk, and the shipped code hints its failures. FDB citations point into
FoundationDB's source (`flow/`, `fdbrpc/`, `fdbserver/`).

## 0. The worked example: the cell init promise

Today (`crates/paros/src/machine/wait.rs:271`), after the promise is durable:

```rust
if hooks.crash_at(Seam::CellPromised) { return Err(Seam::CellPromised); }
```

The error climbs to `run_machine`, then to `paros-sim/src/machine.rs::seam_crash`, which
calls `ctx.crash_self(..)`. The sim wraps the disk in `SimDisk` to observe what each boot
and each record write left. `NoHooks` keeps production inert.

After, in the same function:

```rust
use moonpool_buggify::{buggify_fault_with_prob, hint, is_simulated};
use moonpool_assertions::reachable;

ledger.promise(ballot).await?;                 // the promise is durable
if is_simulated() && buggify_fault_with_prob!(0.05) {
    reachable!("machine: a machine dies right after its cell init promise");
    tracing::warn!(seam = "cell_promised", "hint_reboot");
    hint::reboot().await;                      // sim: killed here; production: never reached
}
```

This is FDB's shape: `if (!history.empty() && buggify()) throw please_reboot();`
(`fdbserver/storageserver/storageserver.cpp:13158-13161`), and the worker turns
`please_reboot` into a simulated reboot only when `g_network->isSimulated()`
(`fdbserver/worker/worker.cpp:2105-2110`). `buggify()` itself is
`isEnabled && activatedAtThisFileLine && random01() < p` (`flow/include/flow/Buggify.h:92-96`),
which `moonpool_buggify::buggify_internal` already mirrors.

- The disk is the library's `ProviderDisk` in both `parosd` and the sim. `ProviderDisk`
  itself implements `MachineDisk`. `SimDisk`, `SimMachineStores` and the `DirDisk` wrapper go.
- One BUGGIFY location, activated per seed, firing per call, silent in the recovery tail
  (`buggify_fault_with_prob!`, FDB's `!speedUpSimulation` guard, `storageserver.cpp:1741`).
- moonpool turns the hint into the `ProcessForceKill` a `SelfCrash` schedules; the future
  never resolves, so no code after the seam runs. In production `is_simulated()` is one
  thread-local read and the branch is dead.
- The observations `SimDisk` makes today move (section 3): a path the lifecycle walks becomes
  a `reachable!` probe in `wait.rs`/`lifecycle.rs`, same message text (FDB's `CODE_PROBE`,
  `flow/include/flow/CodeProbe.h:331`); a fact the sim folds with harness knowledge (`init`
  was sent, the founders) becomes an `Audit` callback reported where its `tracing` event is.

## 1. The moonpool hint API

**Where.** Extend the two zero-dependency crates that exist. No new crate.

- `moonpool-buggify` (`buggify!`, `buggify_fault_with_prob!`, recovery mode, thread-local
  state, `RandomSource = fn() -> f64`) gains `is_simulated()` and a `hint` module: the
  production side of the fault injector.
- `moonpool-assertions` (zero-dep, wasm-safe; `assertion_bool` is a no-op while the table is
  not initialized, `slots.rs:320`) gains the macros production code needs; today they live
  in `moonpool-sim/src/chaos/assertions.rs:408-540`.

**`is_simulated()`.** FDB's `g_network->isSimulated()` (`flow/include/flow/network.h:230`,
`fdbrpc/sim2.cpp:1466`) is a global on the network. moonpool's `Providers` has no such
method, and the journal store and the client have no reason to carry one. So the flag lives
in `moonpool-buggify`'s thread-local state, set by `moonpool-sim` at run start together with
the random source and the hint sink, cleared at run end. One source of truth: simulated ⇔ a
sink is installed. `moonpool-sim` may add `Providers::is_simulated()` as a convenience that
reads it; paros uses the free function. Rule for paros: `is_simulated()` tilts rates, cadences
and extra checks (FDB's `EXPENSIVE_VALIDATION`, `Buggify.h:98`); it never changes an outcome
a client can see.

**Three kinds of hint.** A *point* is an order about now: "reboot me here", "sleep here"
(`please_reboot`). A *window* is advice about a span: "a bad moment to die" (what Pierre
asked for: pending work not durable). A *chosen moment* is the process deciding to reboot
later at a point it names (FDB's `rebootAfterDurableVersion`: the simulated storage server
arms a version, then throws `please_reboot` once that version is durable,
`storageserver.cpp:1741-1748`, `11328-11340`). The injector obeys a point and weighs a window.

```rust
// moonpool_buggify — every call is inert without an installed sink.
pub fn is_simulated() -> bool;

pub mod hint {
    /// The sink a simulation installs (per thread, by `buggify_init`).
    pub trait Sink {
        fn reboot_now(&self) -> bool;                                // true: a kill is scheduled
        fn open(&self, label: &'static str, weight: Weight) -> u64;  // a window id
        fn close(&self, id: u64);
    }
    pub fn set_sink(sink: &'static dyn Sink); pub fn clear_sink();

    /// Reboot the calling process now, as a power loss. With a sink the kill lands within
    /// one scheduler tick and this future never resolves. Without one it resolves at once.
    pub fn reboot() -> impl Future<Output = ()>;

    /// How much a window attracts the injector: `Avoid` ×0, `Normal` ×1, `High` ×10.
    pub enum Weight { Avoid, Normal, High }
    /// A fragile window: unsynced work in flight. Open on construction, closed on drop;
    /// a killed process's windows are cleared by the sim.
    #[must_use] pub struct Fragile(Option<u64>);
    pub fn fragile(label: &'static str, weight: Weight) -> Fragile;
}

/// `Some(d)` drawn in `range` when this site is active and fires (one draw).
#[macro_export] macro_rules! buggify_delay { ($prob:expr, $range:expr) => { .. } }
/// `Some(i)` among `n` when this site fires.
#[macro_export] macro_rules! buggify_pick { ($prob:expr, $n:expr) => { .. } }
/// A `rand`-free knob over the f64 source (integer and f64 ranges).
#[macro_export] macro_rules! buggify_knob { ($default:expr, $range:expr) => { .. } }

// moonpool_assertions
#[macro_export] macro_rules! reachable { ($msg:expr) => { .. } }   // AssertKind::Reachable
#[macro_export] macro_rules! sometimes { ($cond:expr, $msg:expr) => { .. } }
```

A window, replacing `world/power.rs` (`paros::journal::node`, `JournalStorage::sync`):

```rust
let _fragile = hint::fragile("journal commit", Weight::High);   // was seam_crash_bias ×10
write_entries().await?; sync().await?;                            // first durable step
write_metainfo().await?; sync().await?;                           // drop closes the window
```

A chosen moment (the driver, `drain_ready`), the `rebootAfterDurableVersion` pattern:

```rust
// armed once per boot, in the node loop
if is_simulated() && self.reboot_after.is_none() && buggify_with_prob!(0.02) {
    self.reboot_after = Some(self.applied + buggify_knob!(1, 1..64));
}
// consumed where the batch is durable and its messages are not yet sent
if self.reboot_after.is_some_and(|at| self.applied >= at) {
    self.audit.crashed(self.id, Seam::AfterSyncBeforeSend);
    hint::reboot().await;
}
```

**How the sim consumes a window.** `moonpool-sim` keeps a per-process window table in the
world (`ip -> Vec<(label, weight)>`), written only at `open`/`close`, with no draw.

- *Attrition* (`AttritionInjector`, `runner/fault_injector.rs:650`): when its timer says
  "kill one", it draws the victim among the alive processes weighted by their open windows
  (`Avoid` 0, none 1, `High` 10). One draw, as `reboot_random` makes today (`:542`); only
  the weights change. `assert_sometimes_each!("attrition_window", label)` records the window
  a kill landed in, so the sweep sees "a kill inside a journal commit".
- *Aimed cut* (new; `PowerCut::around`'s timer race moved into moonpool): on `open` of a
  `High` window, one `buggify_fault_with_prob!(p)` draw; when it fires the kill is scheduled
  at a random instant inside the window's expected length (the sink keeps the last length per
  label, as `PowerCut::last_commit` does). Chaos window only.

"Yes, this is a bad time to reboot" raises the chance a kill lands there. It never forces
one. Everything is a weight on a draw the injector already makes.

**No `fail_io_point!`.** Failed syncs, short transfers and lost directory entries are
moonpool storage chaos (`storage_fault_mask()`), as FDB's `AsyncFileChaos` draws its own disk
delays and bit flips (`fdbrpc/AsyncFileChaos.h:63,96`); a storage fault is environment, not a
decision of paros. The quarantine and `MachineError::Storage` paths are reached that way.

**Process attribution.** A sink call must know its process (FDB:
`g_simulator->getCurrentProcess()`, used all over `fdbrpc/AsyncFileNonDurable.cpp:51-60`).
Today only the `process` tracing span carries the ip (`process_manager.rs:123`). Proposed:
`TaskMeta` gains `owner: Option<IpAddr>`, inherited at spawn as the span is, and the executor
sets a thread-local `CURRENT_OWNER` around each poll (next to `IN_TASK`, `executor/mod.rs:150`).
`reboot_now` schedules `Event::ProcessForceKill` for it through the `SelfCrash::crash` path
(`runner/context.rs:275`), with its `assert_reachable!("crash: a process crashed itself")`.
From a workload or the driver the sink returns `false` and warns.

**Recovery delay.** Today paros draws it (`restart_delay!`, `seam_crash`). It is the
environment's: the sink draws it from a moonpool knob and the factory restarts the process.

**Recovery mode.** Reboots, delays and aimed cuts use the fault form, silent after
`buggify_enter_recovery` (the runner calls it when the chaos window closes). Paros's
`active()` cutoff in `BuggifyHooks` goes. The late and bare outages stay moonpool chaos.

**Determinism.** Every draw goes through the installed `RandomSource`. Opening and closing
a window draws nothing; the aimed cut draws once per open; the kill is a scheduled event.
The rule stays: a hint is consulted from the node loop or from work the process awaits,
never from a task that can outlive the run. A `Fragile` guard is held across awaits in one
task, never moved into a spawned one. The canary proves it after each PR.

**Inert in production.** `is_simulated()` is one thread-local read; `reboot()` a `Ready`
future; `fragile` returns `Fragile(None)`; a `reachable!` a null-pointer check (ask moonpool
to check the pointer before hashing). `std` only, `wasm32` clean.

**Fewer Sim-vs-Real interfaces.** After the migration the shared surface is `Providers`,
`moonpool-journal`, `moonpool-rpc` and `moonpool_buggify::{is_simulated, hint, buggify_*}`.
`DriverHooks`, `NoHooks`, `BuggifyHooks`, `RunError::SeamCrash`, `SimDisk`, `DirDisk`,
`PowerCut` and `restart_delay!` are deleted. `Audit`/`NoAudit` stays: observation is the one
seam the sim needs that production does not.

**Activation is the per-seed draw.** A location is activated once per run and fires per
call (`Buggify.h:107-112`). A site at `prob = 1.0` is a per-seed scenario. `withhold_gc`,
`hold_journal` and `lose_verdicts` need no harness state: they are sites with a high rate.

## 2. What paros hints, and where

`paros-core` gets nothing: no `buggify`, no `is_simulated`, no hint (confirmed by Pierre).
Every site lives in `paros` (drivers, stores, machine, client) and `parosd`.

| today | after | kind | gate (message kept verbatim) |
|---|---|---|---|
| `Seam::CellPromised` via `crash_at` | section 0 | point | `"machine: a machine dies right after its cell init promise"` at the site |
| `Seam::CellFormatted` | same, after `ledger.format`, before `ledger.form` | point | `"machine: a machine dies between the format and its vote"` |
| `delay_boot` | `buggify_delay!(0.1, 250..2_501)` + `time().sleep` in `run_machine` | point | `"machine: a machine starts late"` |
| `BeforeSync` (`driver/ready.rs`) | `fragile("ready batch", High)` from staging to the sync | window | audit `crashed(node, seam)` from the boot that finds the torn batch; `recovered` as today |
| `AfterSyncBeforeSend` | point after the sync, before the sends; plus the chosen-moment arm above | point | audit `crashed` |
| `MatchBeforeSync` / `MatchAfterSyncBeforeReply` | one window, one point, in `matchmaker/mod.rs` | window + point | audit `matchmaker_crashed` |
| `AfterPrepareSent` (#260) | point after the campaign's `Prepare` send | point | as today |
| `world/power.rs` | `fragile("journal commit", High)` in both journal syncs; the aimed cut in moonpool | window | `"journal store: a node loses power mid-commit"` from the sink's label bucket |
| `seam_crash_bias` ×10 | `Weight::High` on the write windows | weight | `"a write-window-biased seam crash fires"` |
| quiet seats "outside the power cut" (`shape.rs`) | `Weight::Avoid` on a replica's and a held journal's windows | weight | unchanged |
| `skip_*_resend`, `resign_leadership`, election timeout extremes, `stretch_tick_interval`, `expire_parked_read_early`, `skip_delegation` | `is_simulated() && buggify_with_prob!(p)` inline, rates as `BuggifyHooks` | bool | the audit gates they feed today |
| `drop/duplicate_outgoing`, mailbox hooks, `drop/duplicate_client_reply` | inline bool sites at the send and the mailbox | bool | `dropped_at_send`, `duplicated_at_send`, `client_reply_dropped`, … |
| `withhold_gc_requests`, `hold_journal`, lost verdicts | inline sites at rate 1.0 | bool | the gates they feed today |
| `handoff_target`, `phase2_column`, `read_row`, `proxy_for` | `buggify_pick!(p, n)` inline | choice | inline `reachable!` moved next to the site |
| `abandon_reconfigurer(phase)` | one site per phase arm | bool | one `reachable!` per phase |
| `restart_delay!` | gone: moonpool's recovery delay | environment | moonpool's reachable |
| `world/injector.rs` | stays: disk rot aimed by the custody ledger, judged against the journal's verdict; a later `upstream-to-moonpool` candidate | environment | unchanged |
| `lifecycle.rs` `ScriptedLifecycle`, `fleet.rs` target kill | stay: moonpool faults (FDB's `MachineAttrition.cpp:527`) | environment | unchanged |
| `fleet.rs` "stop after one step", checkpoint-crash shapes | stay: an operator's explicit misbehaviour in the workload | operator | unchanged |

**Does `DriverHooks` survive?** No. Every method is a draw, a choice or a window, and
`buggify_pick!` covers the choices. `Seam` survives as the audit's label; `RunError::SeamCrash`
goes: a crashed process never returns. `BuggifyHooks` shrinks one family per PR.
`DriverTunables` stays: a tunable is configuration, a hint is a decision.

## 3. SimDisk and SimMachine go away

`MachineProcess` keeps its `Process` adapter (the role map needs one) and runs
`run_machine(providers, ProviderDisk::new(..), ..)` with no wrapper.

Library change: `ProviderDisk<P>` implements `MachineDisk` with
`Stores = ProviderStores<P, A>`, `A` from an audit factory the caller passes (`parosd`:
`NoAudit`; sim: `NodeAudit` per journal, as `SimMachineStores::audit` builds it today).
`parosd`'s provisioning `Record` moves into `ProviderDisk::format`; `DirDisk` is deleted.
`run_machine` gains `audit: &A` (`A: Audit`).

| observation today (`paros-sim/src/machine.rs`) | after |
|---|---|
| `note_boot`: empty disk, formed restarts, formatted waits, promised and unvoted | `reachable!` probes in `lifecycle.rs::identity` and `wait.rs`, same messages |
| "no `cell init` lists it restarts and waits" | `Audit::machine_booted(node, &record)`; the sim knows the founders |
| `note_decree`: promise never falls, two ballots meet, one cell per run, several seeds form, a later ballot finishes | `Audit::decree_promised(node, ballot)`, `Audit::cell_formed(node, ballot, &plan)` where `cell_promised`/`cell_formed` are traced; `MachineBoard` moves to `paros_sim::audit::machine`, text kept |
| `provision`: no cell forms without `init`, only a founder, over the founders listed | `Audit::cell_formatting(node, &plan)` before `ledger.format`; the board checks `init_sent` and the layout |
| "formats over journals an unvoted attempt left", "a seed formats its cell's journals" | `reachable!` probes in `wait.rs` around `ledger.format` |
| "a formed machine serves journals", "a store is opened as its machine" | hard `assert!` in `ProviderDisk::stores` and `ProviderStores::open` (library postconditions) |
| "a failed record write stops a machine, which restarts" | `reachable!` where `MachineError::Storage` is built |

Rule: a probe names a path the library walked; an audit callback carries a fact the sim folds
with harness knowledge.

## 4. Migration plan

1. **moonpool PR A: points.** `is_simulated()`, `hint::{reboot, Sink, set_sink, clear_sink}`,
   `buggify_delay!`, `buggify_pick!`, a `rand`-free `buggify_knob!`; `TaskMeta.owner`,
   `CURRENT_OWNER`; the sink and flag installed in `buggify_init`; the recovery-delay knob.
   Tests: a hint in a process schedules the kill and stays pending; in a workload it
   resolves; one seed, two runs, one digest.
2. **moonpool PR B: windows.** `Weight`, `Fragile`, the world's window table cleared on kill,
   attrition's weighted victim draw, the aimed cut, the `sometimes_each` label bucket.
3. **moonpool PR C: probes.** `moonpool-assertions::{reachable!, sometimes!}` over
   `assertion_bool`; `moonpool-sim` re-exports them so both spellings hash to one slot.
4. **Pin advance.** One rev on the eight lines. Proof: build, nextest,
   `cargo xtask sim run-all` saturates as before, canary 300 seeds.
5. **paros PR 1: the machine** (section 0). `moonpool-buggify` and `moonpool-assertions`
   become `paros` deps; the two seams and `delay_boot` become points; `Audit` gains four
   machine callbacks; `ProviderDisk: MachineDisk`; delete `SimDisk`, `SimMachineStores`,
   `DirDisk`, `seam_crash`. Proof: sweep saturates with every moved message reached; hunt
   3,000; canary 300. Record the two seam reachables' per-seed rate before and after.
6. **paros PR 2: the journal commit window.** `fragile` in both syncs; delete
   `world/power.rs` and the quiet-seat exclusion. Proof: the mid-commit reachable's per-seed
   rate before and after; hunt 10,000 (fault model); `injector::judge` still saturates.
7. **paros PR 3: the five driver seams.** Windows, points and the chosen-moment arm in
   `driver/ready.rs`, the matchmaker and the campaign; delete `RunError::SeamCrash`,
   `crash_at`, every sim `SeamCrash` arm, `restart_delay!`. Proof: sweep (per-seam
   `crashed`/`recovered`); hunt 10,000; canary 300.
8. **paros PRs 4..n: one hook family per PR** (resends and elections; sends and mailbox;
   client replies; grid and proxy choices; reconfigurer; GC and held journal). Each: inline
   sites, gate text kept, the method deleted from both traits; sweep saturation, hunt 2,000.
   The last PR deletes `hooks.rs` and rewrites the `adding-a-buggify-site` skill.
9. **Docs.** `AGENTS.md` *Turbulence layers* (prong 1 becomes "hints in `paros`": points,
   windows, chosen moments, probes; `is_simulated` tilts, never decides an outcome), both
   crate maps, `docs/architecture.md` (dated, #246), the book.

## 5. Risks and open questions for Pierre

1. **Process attribution.** The sink must know the calling process; `TaskMeta.owner` touches
   moonpool's executor. The alternative is a handle passed into `run_*`, a hook again.
2. **Rates and weights in production code.** Each site's rate and each window's `Weight` is a
   literal next to the code, as in FDB. A three-step `Weight` or a bare `f64`?
3. **Window granularity.** One window per commit is coarse. A commit could open two
   (`"entries unsynced"`, `"metainfo unsynced"`) so the aimed cut names the step it cut.
   Start with one and split when a gate asks for it.
4. **`reachable!` text ownership.** Probe messages are slot hashes shared with the sim. Moving
   them into `paros` is safe only verbatim. Rule to record: a message in `paros` is never
   reworded; a changed probe is a new message and the old one is deleted in the same PR.
5. **`is_simulated()` discipline.** FDB lets it change timeouts (`storageserver.cpp:3033`) and
   severities. For paros the proposed rule is narrower: tilt rates, cadences and checks only.
   Agree, or allow sim-only shortcuts too?
6. **Recovery window.** Hints rely on moonpool's recovery mode, not paros's time cutoff.
   Confirm in PR 1 that `buggify_enter_recovery` fires at `CHAOS_DURATION_MS` exactly.
