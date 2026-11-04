//! `paros-bench` (#517 (`parosctl bench`), #194 (throughput benchmark),
//! #724 (bench roles), #725 (bench phases)): the load generator. It runs
//! clients against a cell over `paros::client` and prints throughput and
//! latency per call, after `CockroachDB`'s load generator. The cell is the
//! servers (`--servers`), or a cell of N machines the bench starts in its
//! own process (`run --local N`, `local.rs`).
//!
//! The phases are subcommands: `init` fills the journals, `run` measures,
//! `clean` deletes what the bench made (`setup.rs`), and `report` merges
//! the `--json` reports of several bench processes (`report.rs`).
//!
//! The load is roles, not a call mix (`load.rs`): each client is a writer,
//! a tailer or a reader of its journal, and set-leader and truncate run on
//! a schedule:
//!
//! - a writer appends at the tail of its journal (writer `i` to journal
//!   `i % journals`). The writers of one journal share its one writer
//!   through a `paros::client::Pipeline` (#597 (several writes in flight
//!   per journal)): each writer's write is in flight at the same time, at
//!   consecutive positions. A record holds its send time.
//! - a tailer follows its journal from the tail, long-polling, and reads
//!   each record as it lands: `e2e` is the time it sees a record minus the
//!   record's send time, one per record.
//! - a reader reads from the start, a random position or the tail of a
//!   journal it picks for each call (uniform or Zipfian).
//!
//! Every read checks every byte. `--topology` names the journals' shapes
//! (`topology.rs`): `3x1`, the plain majority of three, by default. With
//! several (`3x1,3x2,3x3`), the bench runs each in turn, under the same
//! seed and load, on journals of its own (`bench-3x2-K`), then prints one
//! row per topology and call.
//!
//! The writers and readers run closed loop by default: a call starts when
//! the last one ends. With a rate (`pace.rs`), they run open loop: each
//! call starts at its due time, and its latency counts from that moment, so
//! a slow call delays the ones after it and they count it (no coordinated
//! omission). Every random choice comes from `(seed, client index)`, and
//! the seed is logged. `--ramp` starts the clients one by one and
//! `--warmup` runs on after it; neither counts in the totals. `--backlog`
//! stops the tailers once they end, writes a backlog, then measures the
//! catch-up. `--expect` turns the totals into a pass or a fail
//! (`expect.rs`). `--json` prints one report at the end (`summary.rs`):
//! the numbers and the raw histograms, with the commit, the command line,
//! the CPU count, the kernel and the filesystem of a local cell's stores.

mod expect;
mod histograms;
mod load;
pub mod local;
mod pace;
mod records;
pub mod report;
mod setup;
mod summary;
mod topology;
mod units;

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime};

use clap::Args;
use moonpool_core::{CompioProviders, LocalProviders, RandomProvider};
use paros::client::{Pipeline, Reader, ReaderOutcome, TruncateOutcome, Writer, WriterOutcome};
use paros::core::Value;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use parosctl::support::connect::{Ending, Runtime};
use parosctl::support::output::note;

use expect::Limits;
use histograms::{Call, Calls, Export, Kinds, Registry};
use load::{Load, LoadFlags, Picker, ReadFrom, WriteFlags};
use records::Rng;
use topology::{Topologies, Topology};
use units::{bytes_label, float, parse_duration};

/// The client every phase runs over.
pub type Client = paros::client::Client<CompioProviders>;
type ParosClient = Client;

/// The console prints its header again after this many ticks.
const HEADER_EVERY: u64 = 20;

/// How long a tailer's read waits at the tail for the next record.
const TAIL_WAIT_MS: u64 = 1000;

/// How often a stopped tailer looks whether it may start again.
const TAILER_NAP: Duration = Duration::from_millis(20);

/// The exit code of a run whose `--expect` limits failed.
const LIMITS_FAILED: u8 = 1;

/// The tenant of the bench, and how long its setup may retry.
#[derive(Args, Debug, Clone)]
pub struct TenantFlags {
    /// The tenant to write in, by name; created when missing (and tagged
    /// `paros-bench/created:true`, so that `clean` may delete it).
    #[arg(long, default_value = "bench")]
    tenant: String,
    /// How long the setup (tenant, journals, claims) may retry, in
    /// milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

impl TenantFlags {
    fn patience(&self) -> Duration {
        Duration::from_millis(self.patience_ms)
    }
}

/// The bench's journals: how many, and their shapes.
#[derive(Args, Debug, Clone)]
pub struct JournalFlags {
    #[command(flatten)]
    tenant: TenantFlags,
    /// How many journals per topology: `bench-T-0` to `bench-T-{N-1}`,
    /// created when missing (single writer).
    #[arg(long, default_value = "4", value_parser = clap::value_parser!(u64).range(1..))]
    journals: u64,
    /// The journal topologies, each run in turn on journals of its own:
    /// `ROWSxCOLUMNS`, comma-separated. `3x1` is the plain majority of
    /// three; `3x3` is nine acceptors in three partitions of three.
    #[arg(long, default_value = "3x1")]
    topology: Topologies,
    /// How the stores of a journal the bench creates commit a batch
    /// (#599): `ordered` (two syncs per commit) or `batched` (one). A
    /// journal that exists keeps its own: bench the other mode under
    /// another `--tenant`.
    #[arg(
        long,
        default_value = "ordered",
        value_parser = parosctl::cmd::journal::parse_store_durability
    )]
    store_durability: paros::model::tenant::StoreDurability,
}

/// What `run` and `init` share: the seed and the errors.
#[derive(Args, Debug, Clone)]
pub struct CommonFlags {
    /// The seed of every random choice; drawn and logged when absent.
    #[arg(long)]
    seed: Option<u64>,
    /// Log an error and continue; without it, the first error stops the run.
    #[arg(long)]
    tolerate_errors: bool,
}

/// `paros-bench run`: measure.
#[derive(Args, Debug)]
pub struct RunArgs {
    #[command(flatten)]
    journals: JournalFlags,
    #[command(flatten)]
    load: LoadFlags,
    #[command(flatten)]
    common: CommonFlags,
    /// How long to measure, after the ramp and the warmup (`500ms`, `60s`,
    /// `2m`); `0s` runs until Ctrl-C or `--max-ops`.
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    duration: Duration,
    /// Stop after this many writer and reader calls, counted from the
    /// start.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    max_ops: Option<u64>,
    /// Start the clients one by one over this time; nothing recorded before
    /// it ends counts in the totals.
    #[arg(long, default_value = "0s", value_parser = parse_duration)]
    ramp: Duration,
    /// Run this long after the ramp before the totals start: the
    /// per-second lines still print.
    #[arg(long, default_value = "0s", value_parser = parse_duration)]
    warmup: Duration,
    /// Pass/fail limits on the totals, comma-separated:
    /// `e2e.p50<5ms,write.p99<20ms,throughput>100MiB/s,errors<1`. The
    /// bench prints each with its value, and exits 1 if one fails.
    #[arg(long, default_value = "")]
    expect: Limits,
    /// Start a cell of N machines in this process, on loopback, and run
    /// against it: `--servers` is not needed. The stores go in a new
    /// directory under `--local-dir`, deleted after the run.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..=MAX_LOCAL))]
    pub local: Option<u64>,
    /// Where a local cell's stores go: the filesystem the numbers measure.
    /// The system's temporary directory by default.
    #[arg(long, value_name = "DIR", requires = "local")]
    pub local_dir: Option<PathBuf>,
    /// Write the raw histograms here: one JSON line per second per call.
    #[arg(long, value_name = "FILE")]
    histograms: Option<PathBuf>,
}

/// `paros-bench init`: fill the journals.
#[derive(Args, Debug)]
pub struct InitArgs {
    #[command(flatten)]
    journals: JournalFlags,
    /// Fill each journal until it holds this many records (a journal that
    /// holds them already is left as it is).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    records: u64,
    #[command(flatten)]
    write: WriteFlags,
    #[command(flatten)]
    common: CommonFlags,
}

/// `paros-bench clean`: delete what the bench made.
#[derive(Args, Debug)]
pub struct CleanArgs {
    #[command(flatten)]
    tenant: TenantFlags,
}

/// The most machines `--local` starts: a 3x5 or a 5x3 grid.
const MAX_LOCAL: u64 = 15;

/// Which phase runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Init { records: u64 },
    Run,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Phase::Init { .. } => "init",
            Phase::Run => "run",
        }
    }
}

/// Everything a run (or an init) of one topology needs, resolved.
#[derive(Debug)]
struct Settings {
    phase: Phase,
    journals: JournalFlags,
    load: Load,
    common: CommonFlags,
    duration: Duration,
    ramp: Duration,
    warmup: Duration,
    max_ops: Option<u64>,
    expect: Limits,
    local: Option<u64>,
    histograms: Option<PathBuf>,
}

impl Settings {
    fn of_run(args: RunArgs) -> Result<Self, String> {
        let (load, set) = args.load.resolve()?;
        if let Some(preset) = load.preset {
            note(&format!(
                "preset {}: {}",
                preset.name(),
                if set.is_empty() {
                    "every flag given".to_string()
                } else {
                    set.join(" ")
                }
            ));
        }
        let settings = Self {
            phase: Phase::Run,
            journals: args.journals,
            load,
            common: args.common,
            duration: args.duration,
            ramp: args.ramp,
            warmup: args.warmup,
            max_ops: args.max_ops,
            expect: args.expect,
            local: args.local,
            histograms: args.histograms,
        };
        settings.check()?;
        Ok(settings)
    }

    fn of_init(args: InitArgs) -> Result<Self, String> {
        let (load, _) = LoadFlags::writers_only(args.write).resolve()?;
        if load.writers_per_journal == 0 {
            return Err("init needs a writer per journal".into());
        }
        Ok(Self {
            phase: Phase::Init {
                records: args.records,
            },
            journals: args.journals,
            load,
            common: args.common,
            duration: Duration::ZERO,
            ramp: Duration::ZERO,
            warmup: Duration::ZERO,
            max_ops: None,
            expect: Limits::default(),
            local: None,
            histograms: None,
        })
    }

    /// Refuse a run its arguments cannot hold: a topology wider than the
    /// local cell (placement puts each acceptor on its own machine), or
    /// one histogram file for several topologies.
    fn check(&self) -> Result<(), String> {
        let widest = self.journals.topology.widest();
        if let Some(machines) = self.local
            && widest as u64 > machines
        {
            return Err(format!(
                "--topology needs {widest} machines, and --local starts {machines}"
            ));
        }
        if self.histograms.is_some() && self.journals.topology.all().len() > 1 {
            return Err("--histograms takes one topology".into());
        }
        Ok(())
    }

    /// The writers, tailers and readers of the run.
    fn clients(&self) -> (u64, u64, u64) {
        let n = self.journals.journals;
        let load = &self.load;
        (
            load.writers_per_journal * n,
            load.tailers_per_journal * n,
            load.readers_per_journal * n,
        )
    }

    /// The calls the run makes, in report order.
    fn calls(&self) -> Vec<Call> {
        let load = &self.load;
        Call::ALL
            .into_iter()
            .filter(|call| match call {
                Call::Write => load.writers_per_journal > 0,
                Call::Read => load.readers_per_journal > 0,
                Call::Tail | Call::E2e => load.tailers_per_journal > 0,
                Call::SetLeader => load.churns(),
                Call::Truncate => load.truncates(),
            })
            .collect()
    }

    /// Where the totals start: after the ramp and the warmup.
    fn settled(&self) -> Duration {
        self.ramp + self.warmup
    }
}

/// One journal of the run: its one writer's pipeline, with a client of its
/// own (a leader hint names one journal's leader), the tail its writes
/// reached and the floor a truncation left.
struct Slot {
    journal: setup::Journal,
    key: u64,
    client: ParosClient,
    pipeline: Pipeline,
    tail: Cell<u64>,
    floor: Cell<u64>,
}

/// Why a call failed: what the run ends with, the kind of error (counted
/// per call) and the message.
struct Failure {
    ending: Ending,
    kind: String,
    message: String,
}

impl Failure {
    fn new(ending: Ending, kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            ending,
            kind: kind.into(),
            message: message.into(),
        }
    }

    /// A failure named after an outcome: its variant is the kind.
    fn of_outcome(ending: Ending, outcome: &impl std::fmt::Debug, journal: &str) -> Self {
        let text = format!("{outcome:?}");
        Self::new(ending, kind_of(&text), format!("{journal}: {text}"))
    }
}

/// The kind of an outcome from its `Debug` text: its variant's name
/// (`NotLeader { .. }` is `NotLeader`).
fn kind_of(debug: &str) -> String {
    let name: String = debug
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        "Other".to_string()
    } else {
        name
    }
}

/// Where a `--backlog` stands (#725 (bench phases)).
#[derive(Clone, Debug, PartialEq)]
enum Backlog {
    /// No `--backlog`.
    Off,
    /// Until the ramp and the warmup end.
    Waiting,
    /// The tailers are stopped; the writers wrote `written` bytes so far.
    Filling { written: u64 },
    /// The tailers run again since `since`, each until it reaches its
    /// journal's tail as it was then (`targets`); `left` have not yet.
    Draining {
        since: Instant,
        backlog: u64,
        targets: Vec<u64>,
        left: u64,
    },
    /// Every tailer caught up, after `took`.
    Done { took: Duration, backlog: u64 },
}

/// What every client of one topology's run shares.
struct Shared {
    settings: Rc<Settings>,
    seed: u64,
    topology: Topology,
    /// Print one JSON report, and no tick.
    json: bool,
    slots: Vec<Rc<Slot>>,
    stop: CancellationToken,
    start: Instant,
    /// The first error, when it stopped the run.
    failure: RefCell<Option<Failure>>,
    /// Records a read found wrong.
    mismatches: Cell<u64>,
    /// Writes that arrived before the write in front of them and were sent
    /// again (#597 (several writes in flight per journal)).
    resent: Cell<u64>,
    /// Writer and reader calls so far, for `--max-ops`.
    ops: Cell<u64>,
    /// `init`'s writers still filling.
    filling: Cell<u64>,
    backlog: RefCell<Backlog>,
}

impl Shared {
    /// Count one writer or reader call; stop at `--max-ops`.
    fn count_op(&self) {
        self.ops.set(self.ops.get() + 1);
        if self
            .settings
            .max_ops
            .is_some_and(|max| self.ops.get() >= max)
        {
            self.stop.cancel();
        }
    }

    /// Whether the tailers are stopped for the backlog.
    fn tailers_stopped(&self) -> bool {
        matches!(*self.backlog.borrow(), Backlog::Filling { .. })
    }

    /// A writer wrote `bytes`: while the backlog fills, count them; once
    /// full, start the tailers again.
    fn wrote(&self, bytes: u64) {
        let mut backlog = self.backlog.borrow_mut();
        if let Backlog::Filling { written } = &mut *backlog {
            *written += bytes;
            if *written >= self.settings.load.backlog {
                let (_, tailers, _) = self.settings.clients();
                *backlog = Backlog::Draining {
                    since: Instant::now(),
                    backlog: *written,
                    targets: self.slots.iter().map(|s| s.tail.get()).collect(),
                    left: tailers,
                };
            }
        }
    }

    /// The tail a tailer of journal `slot` must reach while the backlog
    /// drains.
    fn drain_target(&self, slot: usize) -> Option<u64> {
        match &*self.backlog.borrow() {
            Backlog::Draining { targets, .. } => Some(targets[slot]),
            _ => None,
        }
    }

    /// One tailer caught up.
    fn drained(&self) {
        let mut backlog = self.backlog.borrow_mut();
        if let Backlog::Draining {
            since,
            backlog: bytes,
            left,
            ..
        } = &mut *backlog
        {
            *left -= 1;
            if *left == 0 {
                *backlog = Backlog::Done {
                    took: since.elapsed(),
                    backlog: *bytes,
                };
            }
        }
    }
}

/// One topology's totals, for the comparison.
pub struct Measured {
    topology: Topology,
    elapsed: Duration,
    calls: Vec<Call>,
    total: Calls,
    /// Whether every `--expect` limit held.
    passed: bool,
}

/// A checked bench: its settings and its seed, shared by every topology.
pub struct Plan {
    settings: Rc<Settings>,
    seed: u64,
}

impl Plan {
    /// Check `args` and draw the seed when they name none.
    ///
    /// # Errors
    ///
    /// The arguments cannot hold a run.
    pub fn new(runtime: &Runtime, args: RunArgs) -> Result<Self, String> {
        Ok(Self::of(runtime, Settings::of_run(args)?))
    }

    fn of(runtime: &Runtime, settings: Settings) -> Self {
        let seed = settings
            .common
            .seed
            .unwrap_or_else(|| runtime.providers.random().random());
        note(&format!("bench seed={seed}"));
        Self {
            settings: Rc::new(settings),
            seed,
        }
    }

    /// How long the setup may retry.
    pub fn patience(&self) -> Duration {
        self.settings.journals.tenant.patience()
    }

    /// The topologies, in the order given.
    pub fn topologies(&self) -> Vec<Topology> {
        self.settings.journals.topology.all().to_vec()
    }

    /// One topology's run against the cell `client` reaches (`servers`,
    /// their ids): its journals, its clients, its totals.
    ///
    /// # Errors
    ///
    /// What the bench ends with: the setup failed, or a call stopped it.
    pub async fn run_one(
        &self,
        runtime: &Runtime,
        (client, servers): (&ParosClient, &[u64]),
        topology: Topology,
        (json, local): (bool, Option<&Path>),
    ) -> Result<Measured, Ending> {
        let (settings, seed) = (&self.settings, self.seed);
        note(&format!(
            "{} topology {topology}: {}",
            settings.phase.name(),
            topology.label()
        ));
        note(&format!("roles: {}", settings.load.roles()));
        let slots = match prepare(runtime, client, servers, settings, (seed, topology)).await {
            Ok(slots) => slots,
            Err(error) => {
                note(&error);
                return Err(Ending::Unreachable);
            }
        };
        let export = match &settings.histograms {
            Some(path) => match Export::create(path, seed) {
                Ok(export) => Some(export),
                Err(error) => {
                    note(&format!("{}: {error}", path.display()));
                    return Err(Ending::Refused);
                }
            },
            None => None,
        };
        let (writers, _, _) = settings.clients();
        let shared = Rc::new(Shared {
            settings: settings.clone(),
            seed,
            topology,
            json,
            slots,
            stop: CancellationToken::new(),
            start: Instant::now(),
            failure: RefCell::new(None),
            mismatches: Cell::new(0),
            resent: Cell::new(0),
            ops: Cell::new(0),
            filling: Cell::new(writers),
            backlog: RefCell::new(if settings.load.backlog > 0 {
                Backlog::Waiting
            } else {
                Backlog::Off
            }),
        });
        let (elapsed, calls, total) = drive(client.clone(), shared.clone(), export).await;
        if let Some(failure) = shared.failure.borrow_mut().take() {
            note(&failure.message);
            return Err(failure.ending);
        }
        let mismatches = shared.mismatches.get();
        if mismatches > 0 {
            note(&format!("the reads found {mismatches} wrong records"));
            return Err(Ending::Refused);
        }
        let document = report(&shared, local, elapsed, (&calls, &total));
        let verdicts = settings.expect.check(&document);
        for verdict in &verdicts {
            note(&verdict.to_string());
        }
        Ok(Measured {
            topology,
            elapsed,
            calls,
            total,
            passed: verdicts.iter().all(|v| v.passed),
        })
    }
}

/// End a bench whose topologies all ran: with several, and no `--json`,
/// print them side by side. Exit 1 when a limit failed.
pub fn finish(runs: &[Measured], json: bool) -> ExitCode {
    if runs.len() > 1 && !json {
        compare(runs);
    }
    if runs.iter().all(|run| run.passed) {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(LIMITS_FAILED)
    }
}

/// `paros-bench run` against the cell `client` reaches (`servers`, their
/// ids): each topology in turn on the same cell, under the same seed. The
/// first that fails ends the bench. `json` prints one report per topology.
pub async fn run(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    args: RunArgs,
    json: bool,
) -> ExitCode {
    match Plan::new(runtime, args) {
        Ok(plan) => each_topology(runtime, client, servers, &plan, json).await,
        Err(error) => {
            note(&error);
            ExitCode::from(2)
        }
    }
}

/// `paros-bench init`: fill each journal of each topology with
/// `--records` records, as `run`'s writers would write them, under the same
/// rate flags.
pub async fn init(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    args: InitArgs,
    json: bool,
) -> ExitCode {
    match Settings::of_init(args) {
        Ok(settings) => {
            let plan = Plan::of(runtime, settings);
            each_topology(runtime, client, servers, &plan, json).await
        }
        Err(error) => {
            note(&error);
            ExitCode::from(2)
        }
    }
}

async fn each_topology(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    plan: &Plan,
    json: bool,
) -> ExitCode {
    let mut runs = Vec::new();
    for topology in plan.topologies() {
        match plan
            .run_one(runtime, (client, servers), topology, (json, None))
            .await
        {
            Ok(run) => runs.push(run),
            Err(ending) => return ending.into(),
        }
    }
    finish(&runs, json)
}

/// `paros-bench clean`.
pub async fn clean(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    args: CleanArgs,
    json: bool,
) -> ExitCode {
    let name = &args.tenant.tenant;
    let cleaned = match setup::clean(runtime, client, servers, name, args.tenant.patience()).await {
        Ok(cleaned) => cleaned,
        Err(error) => {
            note(&error);
            return Ending::Refused.into();
        }
    };
    let kept = if !cleaned.found {
        format!("no tenant named {name}")
    } else if cleaned.tenant_deleted {
        format!("deleted tenant {name}")
    } else if !cleaned.created_by_bench {
        format!("kept tenant {name}: the bench did not create it")
    } else {
        format!(
            "kept tenant {name}: it holds {} other journals",
            cleaned.others
        )
    };
    if json {
        println!(
            "{}",
            json!({
                "phase": "clean",
                "tenant": name,
                "found": cleaned.found,
                "journals_deleted": cleaned.journals,
                "other_journals": cleaned.others,
                "tenant_deleted": cleaned.tenant_deleted,
            })
        );
    } else {
        println!("deleted {} bench journals; {kept}", cleaned.journals.len());
    }
    ExitCode::SUCCESS
}

/// One row per topology and call: the totals side by side.
fn compare(runs: &[Measured]) {
    assert!(runs.len() > 1, "a comparison needs two runs");
    println!();
    println!("{}", total_header("topology"));
    for run in runs {
        for call in &run.calls {
            let name = format!("{} {}", run.topology, call.name());
            println!("{}", total_row(run.elapsed, &name, &run.total, *call));
        }
    }
}

/// The tenant, the journals, and a claimed writer on each.
async fn prepare(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    settings: &Settings,
    (seed, topology): (u64, Topology),
) -> Result<Vec<Rc<Slot>>, String> {
    let providers = &runtime.providers;
    let flags = &settings.journals;
    let patience = flags.tenant.patience();
    let name = &flags.tenant.tenant;
    let tenant = setup::tenant(providers, client, servers, name, patience).await?;
    let count = usize::try_from(flags.journals).map_err(|e| e.to_string())?;
    let journals = setup::journals(
        runtime,
        client,
        (name, tenant),
        (count, topology, flags.store_durability),
        patience,
    )
    .await?;
    let mut slots = Vec::with_capacity(journals.len());
    for (index, journal) in journals.into_iter().enumerate() {
        let journal_client = client.clone().with_own_leader_hint();
        let mut rng = Rng::of(&[seed, 1, index as u64]);
        let leader_seed = u128::from(rng.next_u64()) << 64 | u128::from(rng.next_u64());
        let pipeline = Pipeline::new(Writer::new(journal.id, leader_seed));
        let tail = if settings.load.writes() {
            setup::claim(&journal_client, &pipeline, &journal, patience).await?;
            pipeline.writer().next_seq()
        } else {
            journal_client
                .journal_state(journal.id, 0)
                .await
                .ok_or_else(|| format!("no server served the state of {}", journal.name))?
                .next_seq
                .0
        };
        slots.push(Rc::new(Slot {
            key: records::journal_key(&journal.name),
            journal,
            client: journal_client,
            pipeline,
            tail: Cell::new(tail),
            floor: Cell::new(0),
        }));
    }
    Ok(slots)
}

/// Start every client and the schedules, tick every second until the run
/// ends, then print the totals (or the report, with `--json`). The time
/// measured, the calls made and their totals.
async fn drive(
    client: ParosClient,
    shared: Rc<Shared>,
    mut export: Option<Export>,
) -> (Duration, Vec<Call>, Calls) {
    let settings = shared.settings.clone();
    let mut registry = Registry::default();
    let start = shared.start;
    let tasks = spawn_clients(&client, &shared, &mut registry);
    let calls = settings.calls();
    let settled = settings.settled();
    let end = (!settings.duration.is_zero()).then(|| settled + settings.duration);
    let mut settling = !settled.is_zero();
    let mut ticker = compio::time::interval_at(
        std::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    let mut ticks = 0_u64;
    let mut ctrl_c = std::pin::pin!(compio::signal::ctrl_c());
    if !settling {
        start_backlog(&shared);
    }
    loop {
        moonpool_core::select! {
            _ = ticker.tick() => {}
            () = shared.stop.cancelled() => break,
            _ = &mut ctrl_c => {
                note("interrupted");
                break;
            }
        }
        let elapsed = start.elapsed();
        let interval = registry.tick();
        if !shared.json {
            if ticks.is_multiple_of(HEADER_EVERY) {
                println!("{}", tick_header());
            }
            // The total counts from the end of the warmup, once it is reset.
            let window = if settling {
                elapsed
            } else {
                elapsed.saturating_sub(settled)
            };
            print_tick(
                &shared,
                (elapsed, window),
                &calls,
                (&interval, &registry.total),
            );
        }
        ticks += 1;
        record(&mut export, elapsed, &calls, &interval);
        if settling && elapsed >= settled {
            settling = false;
            registry.reset();
            start_backlog(&shared);
        }
        if end.is_some_and(|end| elapsed >= end) {
            break;
        }
    }
    shared.stop.cancel();
    drop(tasks);
    let elapsed = start.elapsed();
    let last = registry.tick();
    record(&mut export, elapsed, &calls, &last);
    let measured = if settling {
        elapsed
    } else {
        elapsed.saturating_sub(settled)
    };
    if !shared.json {
        totals(
            measured,
            &calls,
            &registry.total,
            settings.load.writers_per_journal > 0,
        );
        if shared.resent.get() > 0 {
            note(&format!(
                "write: {} arrived early and were sent again",
                shared.resent.get()
            ));
        }
        if let Some(line) = backlog_line(&shared.backlog.borrow()) {
            println!("{line}");
        }
    }
    if let Some(file) = export
        && let Err(error) = file.finish()
    {
        note(&format!("histograms: {error}"));
    }
    (measured, calls, std::mem::take(&mut registry.total))
}

/// One second's lines: a row per call, the target rate, the backlog.
fn print_tick(
    shared: &Shared,
    (elapsed, window): (Duration, Duration),
    calls: &[Call],
    (interval, total): (&Calls, &Calls),
) {
    let settings = &shared.settings;
    let label = if elapsed < settings.ramp {
        " (ramp)"
    } else if elapsed < settings.settled() {
        " (warmup)"
    } else {
        ""
    };
    for call in calls {
        println!(
            "{}",
            tick_row((elapsed, window), *call, interval, total, label)
        );
    }
    if let Some(pace) = &settings.load.pace {
        println!(
            "{:>7.1}s target {:.1} {}",
            elapsed.as_secs_f64(),
            pace.at(elapsed),
            pace.unit.label()
        );
    }
    if let Some(line) = backlog_line(&shared.backlog.borrow()) {
        println!("{:>7.1}s {line}", elapsed.as_secs_f64());
    }
}

/// The backlog starts filling: the tailers stop.
fn start_backlog(shared: &Shared) {
    let mut backlog = shared.backlog.borrow_mut();
    if *backlog == Backlog::Waiting {
        *backlog = Backlog::Filling { written: 0 };
    }
}

/// Where the backlog stands, for the console.
fn backlog_line(backlog: &Backlog) -> Option<String> {
    match backlog {
        Backlog::Off | Backlog::Waiting => None,
        Backlog::Filling { written } => Some(format!(
            "backlog: tailers stopped, {} written",
            bytes_label(float(*written))
        )),
        Backlog::Draining { since, left, .. } => Some(format!(
            "backlog: draining for {:.1}s, {left} tailers behind",
            since.elapsed().as_secs_f64()
        )),
        Backlog::Done { took, backlog } => Some(format!(
            "catch-up: {} drained in {:.2}s ({}/s)",
            bytes_label(float(*backlog)),
            took.as_secs_f64(),
            bytes_label(float(*backlog) / took.as_secs_f64().max(1e-9))
        )),
    }
}

/// Spawn the writers, the tailers, the readers and the schedules, staggered
/// over the ramp; dropping a handle cancels its task, so none outlives the
/// run.
fn spawn_clients(
    client: &ParosClient,
    shared: &Rc<Shared>,
    registry: &mut Registry,
) -> Vec<compio::runtime::JoinHandle<()>> {
    let settings = &shared.settings;
    let (writers, tailers, readers) = settings.clients();
    let all = writers + tailers + readers;
    let journals = settings.journals.journals;
    let mut tasks = Vec::new();
    for index in 0..all {
        let calls = registry.client();
        let client = client.clone().with_own_leader_hint();
        let shared = shared.clone();
        let delay = stagger(settings.ramp, index, all);
        tasks.push(compio::runtime::spawn(async move {
            compio::time::sleep(delay).await;
            let slot = usize::try_from(index % journals).expect("a journal index fits");
            moonpool_core::select! {
                () = shared.stop.cancelled() => {}
                () = role(&client, &shared, index, slot, &calls) => {}
            }
        }));
    }
    let schedules = [
        (settings.load.churns(), Call::SetLeader),
        (settings.load.truncates(), Call::Truncate),
    ];
    for (on, call) in schedules {
        if on {
            let calls = registry.client();
            let shared = shared.clone();
            tasks.push(compio::runtime::spawn(async move {
                moonpool_core::select! {
                    () = shared.stop.cancelled() => {}
                    () = schedule(&shared, call, &calls) => {}
                }
            }));
        }
    }
    tasks
}

/// Client `index`: the writers come first, then the tailers, then the
/// readers.
async fn role(
    client: &ParosClient,
    shared: &Shared,
    index: u64,
    slot: usize,
    calls: &RefCell<Calls>,
) {
    let (writers, tailers, _) = shared.settings.clients();
    if index < writers {
        writer(shared, index, slot, calls).await;
    } else if index < writers + tailers {
        tailer(client, shared, slot, calls).await;
    } else {
        reader(client, shared, index, calls).await;
    }
}

/// Client `index`'s start, of `clients` staggered over `ramp`.
fn stagger(ramp: Duration, index: u64, clients: u64) -> Duration {
    let nanos = ramp.as_nanos() * u128::from(index) / u128::from(clients.max(1));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// A paced client's schedule: its next due time under the rate, if any.
struct Pacer {
    due: Instant,
    /// What one of its calls costs, in the rate's unit when it counts
    /// bytes: its mean record bytes.
    cost: f64,
}

impl Pacer {
    fn new(cost: f64) -> Self {
        Self {
            due: Instant::now(),
            cost,
        }
    }

    /// Wait for the next call's due time; the moment its latency counts
    /// from.
    async fn next(&mut self, shared: &Shared) -> Instant {
        let Some(pace) = &shared.settings.load.pace else {
            return Instant::now();
        };
        let (writers, _, readers) = shared.settings.clients();
        let at = self.due;
        let elapsed = at.saturating_duration_since(shared.start);
        self.due += pace.period(elapsed, writers + readers, self.cost);
        compio::time::sleep_until(at).await;
        at
    }
}

/// The text of the report as one JSON document; printed with `--json`.
fn report(
    shared: &Shared,
    local: Option<&Path>,
    measured: Duration,
    (calls, total): (&[Call], &Calls),
) -> serde_json::Value {
    let settings = &shared.settings;
    let load = &settings.load;
    let flags = &settings.journals;
    let run = json!({
        "phase": settings.phase.name(),
        "seed": shared.seed,
        "target": match local {
            Some(_) => json!({ "local": settings.local }),
            None => json!("servers"),
        },
        "tenant": flags.tenant.tenant,
        "topology": shared.topology.to_string(),
        "acceptors": shared.topology.acceptors(),
        "journals": flags.journals,
        "preset": load.preset.map(load::Preset::name),
        "roles": load.roles(),
        "writers_per_journal": load.writers_per_journal,
        "tailers_per_journal": load.tailers_per_journal,
        "readers_per_journal": load.readers_per_journal,
        "records_per_write": load.records_per_write,
        "records_per_read": load.records_per_read,
        "min_record_bytes": load.min_record_bytes,
        "max_record_bytes": load.max_record_bytes,
        "read_from": load.read_from.name(),
        "journal_distribution": load.journal_distribution.name(),
        "rate": load.pace.map(|pace| json!({
            "unit": pace.unit.label(),
            "max": pace.max,
            "min": pace.moving.map(|m| m.min),
            "period_s": pace.moving.map(|m| m.period.as_secs_f64()),
            "shape": pace.moving.map(|m| m.shape.name()),
        })),
        "set_leader_every_s": load.set_leader_every.as_secs_f64(),
        "truncate_every_s": load.truncate_every.as_secs_f64(),
        "ramp_s": settings.ramp.as_secs_f64(),
        "warmup_s": settings.warmup.as_secs_f64(),
        "backlog_bytes": load.backlog,
        "writes_in_flight_per_journal": load.writers_per_journal,
        "writes_resent": shared.resent.get(),
        "mismatches": shared.mismatches.get(),
    });
    let mut document = summary::document(&run, local, measured, calls, total);
    if let Some(catch_up) = catch_up(&shared.backlog.borrow()) {
        document["catch_up"] = catch_up;
    }
    if shared.json {
        println!("{document}");
    }
    document
}

/// The catch-up in the report: how long the backlog took to drain, and the
/// drain rate.
fn catch_up(backlog: &Backlog) -> Option<serde_json::Value> {
    match backlog {
        Backlog::Off => None,
        Backlog::Done { took, backlog } => Some(json!({
            "done": true,
            "backlog_bytes": backlog,
            "seconds": took.as_secs_f64(),
            "drain_bytes_per_s": float(*backlog) / took.as_secs_f64().max(1e-9),
        })),
        Backlog::Waiting | Backlog::Filling { .. } | Backlog::Draining { .. } => {
            Some(json!({ "done": false }))
        }
    }
}

/// Export one interval; a failed export stops exporting, never the run.
fn record(export: &mut Option<Export>, elapsed: Duration, calls: &[Call], interval: &Calls) {
    if let Some(file) = export.as_mut()
        && let Err(error) = file.tick((elapsed, unix_ms()), calls, interval)
    {
        note(&format!("histograms: {error}"));
        *export = None;
    }
}

/// The total row of every call, then the result row (`write`).
fn totals(measured: Duration, calls: &[Call], total: &Calls, writes: bool) {
    println!();
    println!("{}", total_header("total"));
    for call in calls {
        println!("{}", total_row(measured, call.name(), total, *call));
    }
    if writes {
        println!();
        println!("{}", total_header("result"));
        println!(
            "{}",
            total_row(measured, Call::Write.name(), total, Call::Write)
        );
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// Count a failed `call` by its kind; without `--tolerate-errors`, the
/// first failure stops the run. Whether the caller stops.
fn failed(shared: &Shared, calls: &RefCell<Calls>, call: Call, failure: &Failure) -> bool {
    calls.borrow_mut().error(call, &failure.kind);
    if shared.settings.common.tolerate_errors {
        note(&format!("{}: {}", call.name(), failure.message));
        return false;
    }
    let mut first = shared.failure.borrow_mut();
    if first.is_none() {
        *first = Some(Failure::new(
            failure.ending,
            failure.kind.clone(),
            format!("{} stopped the run: {}", call.name(), failure.message),
        ));
    }
    shared.stop.cancel();
    true
}

/// A writer: append at its journal's tail until stopped, as soon as the
/// last write ends or on the rate's schedule. In `init`, until the journal
/// holds `--records` records.
async fn writer(shared: &Shared, index: u64, slot: usize, calls: &RefCell<Calls>) {
    let load = &shared.settings.load;
    let slot = &shared.slots[slot];
    let mut rng = Rng::of(&[shared.seed, 2, index]);
    let mut pacer = Pacer::new(float(load.records_per_write) * load.mean_record_bytes());
    loop {
        if let Phase::Init { records } = shared.settings.phase
            && slot.tail.get() >= records
        {
            break;
        }
        let began = pacer.next(shared).await;
        match write(shared, slot, &mut rng).await {
            Ok(bytes) => {
                calls
                    .borrow_mut()
                    .record(Call::Write, began.elapsed(), bytes);
                shared.wrote(bytes);
            }
            Err(failure) => {
                if failed(shared, calls, Call::Write, &failure) {
                    return;
                }
            }
        }
        shared.count_op();
    }
    shared.filling.set(shared.filling.get() - 1);
    if shared.filling.get() == 0 {
        shared.stop.cancel();
    }
}

/// A tailer: follow journal `slot` from the tail it had at the start, one
/// long-polled page after the other, every byte checked; each record's
/// `e2e` is the time it is seen minus its send time.
async fn tailer(client: &ParosClient, shared: &Shared, slot_index: usize, calls: &RefCell<Calls>) {
    let slot = &shared.slots[slot_index];
    let mut reader = Reader::new(slot.journal.id, slot.tail.get());
    let mut caught_up = false;
    loop {
        if shared.tailers_stopped() {
            compio::time::sleep(TAILER_NAP).await;
            continue;
        }
        let began = Instant::now();
        let report = client
            .read_any(
                &reader.request(0, TAIL_WAIT_MS),
                client.leader().unwrap_or(0),
            )
            .await;
        match reader.absorb(report.outcome) {
            ReaderOutcome::Records { records, .. } if records.is_empty() => {}
            ReaderOutcome::Records { from, records, .. } => {
                let seen = unix_ns();
                match checked(shared, slot, from, &records) {
                    Ok(bytes) => {
                        let mut calls = calls.borrow_mut();
                        calls.record(Call::Tail, began.elapsed(), bytes);
                        for record in &records {
                            let sent = records::sent_ns(record).unwrap_or(seen);
                            calls.record(
                                Call::E2e,
                                Duration::from_nanos(seen.saturating_sub(sent)),
                                0,
                            );
                        }
                    }
                    Err(failure) => {
                        if failed(shared, calls, Call::Tail, &failure) {
                            return;
                        }
                    }
                }
            }
            // A truncation passed the cursor: it resumed at the floor.
            ReaderOutcome::Gap { .. } => {}
            other => {
                if failed(shared, calls, Call::Tail, &unread(slot, &other)) {
                    return;
                }
            }
        }
        if !caught_up
            && let Some(target) = shared.drain_target(slot_index)
            && reader.cursor() >= target
        {
            caught_up = true;
            shared.drained();
        }
    }
}

/// A reader: read `--records-per-read` records of a journal it picks, from
/// `--read-from`, every byte checked.
async fn reader(client: &ParosClient, shared: &Shared, index: u64, calls: &RefCell<Calls>) {
    let load = &shared.settings.load;
    let journals = shared.slots.len();
    let picker = Picker::new(load.journal_distribution, journals as u64);
    let mut rng = Rng::of(&[shared.seed, 3, index]);
    let mut pacer = Pacer::new(float(load.records_per_read) * load.mean_record_bytes());
    // A `start` cursor per journal.
    let mut cursors = vec![0_u64; journals];
    loop {
        let began = pacer.next(shared).await;
        let at = picker.pick(&mut rng);
        let slot = &shared.slots[at];
        let result = read(client, shared, slot, &mut rng, &mut cursors[at]).await;
        match result {
            Ok(bytes) => calls
                .borrow_mut()
                .record(Call::Read, began.elapsed(), bytes),
            Err(failure) => {
                if failed(shared, calls, Call::Read, &failure) {
                    return;
                }
            }
        }
        shared.count_op();
    }
}

/// Every `--set-leader-every` (or `--truncate-every`), take a new term on
/// (or truncate) each journal, as its writer.
async fn schedule(shared: &Shared, call: Call, calls: &RefCell<Calls>) {
    let load = &shared.settings.load;
    let every = match call {
        Call::SetLeader => load.set_leader_every,
        _ => load.truncate_every,
    };
    let mut ticker = compio::time::interval_at(std::time::Instant::now() + every, every);
    loop {
        ticker.tick().await;
        for slot in &shared.slots {
            let began = Instant::now();
            let result = if call == Call::SetLeader {
                set_leader(shared, slot).await
            } else {
                let up_to = slot.tail.get().saturating_sub(load.truncate_keep);
                if up_to == 0 {
                    continue;
                }
                truncate(shared, slot, up_to).await
            };
            match result {
                Ok(()) => calls.borrow_mut().record(call, began.elapsed(), 0),
                Err(failure) => {
                    if failed(shared, calls, call, &failure) {
                        return;
                    }
                }
            }
        }
    }
}

/// Claim the journal again when another uuid superseded its writer.
async fn owned(shared: &Shared, slot: &Slot) -> Result<(), Failure> {
    if slot.pipeline.owned().is_none() {
        let patience = shared.settings.journals.tenant.patience();
        setup::claim(&slot.client, &slot.pipeline, &slot.journal, patience)
            .await
            .map_err(|e| Failure::new(Ending::Refused, "Claim", e))?;
    }
    Ok(())
}

/// One write of `--records-per-write` records at the journal's next
/// position, as its writer, beside the other writers' writes in flight on
/// it; each record's size drawn, each stamped with its send time. A writer
/// another uuid superseded claims again first. The bytes written.
async fn write(shared: &Shared, slot: &Slot, rng: &mut Rng) -> Result<u64, Failure> {
    let load = &shared.settings.load;
    let retries = slot.client.tunables().retry_budget;
    let mut attempt = 0;
    loop {
        owned(shared, slot).await?;
        let sizes: Vec<usize> = (0..load.records_per_write)
            .map(|_| load.record_size(rng))
            .collect();
        let bytes: u64 = sizes.iter().map(|s| *s as u64).sum();
        let seed = shared.seed;
        let key = slot.key;
        let batch = move |from: u64| {
            let sent = unix_ns();
            (from..)
                .zip(&sizes)
                .map(|(seq, size)| Value(records::record(seed, key, seq, *size, sent)))
                .collect()
        };
        let outcome = match slot.pipeline.write(&slot.client, batch, 0).await {
            Some(sent) => {
                shared
                    .resent
                    .set(shared.resent.get() + u64::from(sent.resent));
                sent.outcome
            }
            None => WriterOutcome::NotOwner,
        };
        match outcome {
            WriterOutcome::Written { seq, count, .. } => {
                slot.tail.set(slot.tail.get().max(seq + count));
                return Ok(bytes);
            }
            // A position another write's failure left unreachable, or a
            // term a set-leader of this run ended while the write was in
            // flight: the records of a new position go there.
            WriterOutcome::Refused { .. }
            | WriterOutcome::Superseded { .. }
            | WriterOutcome::NotOwner
                if attempt < retries =>
            {
                attempt += 1;
            }
            WriterOutcome::Ambiguous => {
                return Err(Failure::new(
                    Ending::Ambiguous,
                    "Ambiguous",
                    "an answer never came: the write may land",
                ));
            }
            WriterOutcome::Unavailable { .. } => {
                return Err(Failure::new(
                    Ending::Unreachable,
                    "Unavailable",
                    "no leader took the write",
                ));
            }
            other => {
                return Err(Failure::of_outcome(
                    Ending::Refused,
                    &other,
                    &slot.journal.name,
                ));
            }
        }
    }
}

/// One set-leader: the journal's writer takes a new term under a fresh
/// uuid, which supersedes its last one and the writes in flight under it.
async fn set_leader(shared: &Shared, slot: &Slot) -> Result<(), Failure> {
    slot.pipeline
        .claim(&slot.client, slot.client.leader().unwrap_or(0), true)
        .await;
    owned(shared, slot).await
}

/// One truncate of the journal below `up_to`, as its writer.
async fn truncate(shared: &Shared, slot: &Slot, up_to: u64) -> Result<(), Failure> {
    owned(shared, slot).await?;
    match slot.pipeline.truncate(&slot.client, up_to, 0).await {
        Some(TruncateOutcome::Applied { .. }) => {
            slot.floor.set(slot.floor.get().max(up_to));
            Ok(())
        }
        Some(TruncateOutcome::Ambiguous) => Err(Failure::new(
            Ending::Ambiguous,
            "Ambiguous",
            "an answer never came: the truncation may land",
        )),
        Some(other) => Err(Failure::of_outcome(
            Ending::Refused,
            &other,
            &slot.journal.name,
        )),
        None => Err(Failure::new(
            Ending::Refused,
            "NotOwner",
            format!("{}: the writer does not lead", slot.journal.name),
        )),
    }
}

/// Check every record of a page read at `from`; the bytes it held.
fn checked(shared: &Shared, slot: &Slot, from: u64, records: &[Vec<u8>]) -> Result<u64, Failure> {
    let mut bytes = 0;
    for (seq, record) in (from..).zip(records) {
        if !records::check(slot.key, seq, record) {
            shared.mismatches.set(shared.mismatches.get() + 1);
            return Err(Failure::new(
                Ending::Refused,
                "Mismatch",
                format!("wrong bytes at {} position {seq}", slot.journal.name),
            ));
        }
        bytes += record.len() as u64;
    }
    Ok(bytes)
}

/// What a read that served no page fails with.
fn unread(slot: &Slot, outcome: &ReaderOutcome) -> Failure {
    match outcome {
        ReaderOutcome::UnknownJournal => Failure::new(
            Ending::Refused,
            "UnknownJournal",
            format!("no server serves {}", slot.journal.name),
        ),
        ReaderOutcome::Denied(denial) => Failure::new(Ending::Refused, "Denied", denial.as_str()),
        _ => Failure::new(
            Ending::Unreachable,
            "Unavailable",
            "no server served the read",
        ),
    }
}

/// Where a reader call reads, of a journal whose records are
/// `[floor, tail)`: `start` at the reader's cursor, `random` at a drawn
/// position, `tail` at the last `limit` records.
fn read_position(
    read_from: ReadFrom,
    (floor, tail): (u64, u64),
    limit: u64,
    cursor: u64,
    rng: &mut Rng,
) -> u64 {
    match read_from {
        ReadFrom::Start => cursor.max(floor),
        ReadFrom::Random if tail > floor => floor + rng.below(tail - floor),
        ReadFrom::Random => floor,
        ReadFrom::Tail => tail.saturating_sub(limit).max(floor),
    }
}

/// One reader call on `slot`, every byte checked. For `start`, the cursor
/// moves past the page, starts again at the first position at the tail,
/// and moves to the floor past a truncation.
async fn read(
    client: &ParosClient,
    shared: &Shared,
    slot: &Slot,
    rng: &mut Rng,
    cursor: &mut u64,
) -> Result<u64, Failure> {
    let load = &shared.settings.load;
    let limit = load.records_per_read;
    let from = read_position(
        load.read_from,
        (slot.floor.get(), slot.tail.get()),
        limit,
        *cursor,
        rng,
    );
    let mut reader = Reader::new(slot.journal.id, from);
    let report = client
        .read_any(&reader.request(limit, 0), client.leader().unwrap_or(0))
        .await;
    match reader.absorb(report.outcome) {
        ReaderOutcome::Records { records, .. } if records.is_empty() => {
            *cursor = 0;
            Ok(0)
        }
        ReaderOutcome::Records { from, records, .. } => {
            *cursor = reader.cursor();
            checked(shared, slot, from, &records)
        }
        // Nothing to check below the floor: a truncation passed it.
        ReaderOutcome::Gap { resumed_at, .. } => {
            slot.floor.set(slot.floor.get().max(resumed_at));
            *cursor = reader.cursor();
            Ok(0)
        }
        other => Err(unread(slot, &other)),
    }
}

/// Milliseconds, with one decimal.
fn ms(nanos: u64) -> String {
    format!("{:.1}", float(nanos) / 1e6)
}

fn tick_header() -> String {
    format!(
        "{:>8} {:>8} {:>14} {:>14} {:>8} {:>8} {:>8} {:>8}  call",
        "elapsed",
        "errors",
        "ops/s(inst)",
        "ops/s(cum)",
        "p50(ms)",
        "p95(ms)",
        "p99(ms)",
        "max(ms)"
    )
}

/// The errors of one call by kind, for the console: ` [NotLeader=2]`.
fn kinds_label(kinds: &Kinds) -> String {
    if kinds.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = kinds.iter().map(|(k, n)| format!("{k}={n}")).collect();
    format!(" [{}]", parts.join(","))
}

fn tick_row(
    (elapsed, window): (Duration, Duration),
    call: Call,
    interval: &Calls,
    total: &Calls,
    label: &str,
) -> String {
    let h = interval.of(call);
    let secs = window.as_secs_f64().max(1e-9);
    format!(
        "{:>7.1}s {:>8} {:>14.1} {:>14.1} {:>8} {:>8} {:>8} {:>8}  {}{}{}",
        elapsed.as_secs_f64(),
        total.errors_of(call),
        float(h.len()),
        float(total.of(call).len()) / secs,
        ms(h.value_at_quantile(0.50)),
        ms(h.value_at_quantile(0.95)),
        ms(h.value_at_quantile(0.99)),
        ms(h.max()),
        call.name(),
        label,
        kinds_label(interval.kinds_of(call)),
    )
}

fn total_header(last: &str) -> String {
    format!(
        "{:>8} {:>8} {:>12} {:>12} {:>9} {:>8} {:>8} {:>8} {:>8} {:>9} {:>8}  {last}",
        "elapsed",
        "errors",
        "ops(total)",
        "ops/s(cum)",
        "MiB/s",
        "avg(ms)",
        "p50(ms)",
        "p90(ms)",
        "p99(ms)",
        "p99.9(ms)",
        "max(ms)"
    )
}

fn total_row(measured: Duration, name: &str, total: &Calls, call: Call) -> String {
    let h = total.of(call);
    let secs = measured.as_secs_f64().max(1e-9);
    format!(
        "{:>7.1}s {:>8} {:>12} {:>12.1} {:>9.2} {:>8} {:>8} {:>8} {:>8} {:>9} {:>8}  {name}{}",
        measured.as_secs_f64(),
        total.errors_of(call),
        h.len(),
        float(h.len()) / secs,
        float(total.bytes_of(call)) / secs / f64::from(1 << 20),
        format!("{:.1}", h.mean() / 1e6),
        ms(h.value_at_quantile(0.50)),
        ms(h.value_at_quantile(0.90)),
        ms(h.value_at_quantile(0.99)),
        ms(h.value_at_quantile(0.999)),
        ms(h.max()),
        kinds_label(total.kinds_of(call)),
    )
}

#[cfg(test)]
mod tests;
