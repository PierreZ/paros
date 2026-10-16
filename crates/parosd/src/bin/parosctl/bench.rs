//! `parosctl bench` (#517 (`parosctl bench`), #194 (throughput benchmark)):
//! the load generator. It runs clients against a cell over `paros::client`
//! and prints throughput and latency per call, after `CockroachDB`'s load
//! generator. The cell is the servers (`--servers`), or a cell of N
//! machines the bench starts in its own process (`--local N`, `local.rs`).
//!
//! The bench owns the loop: the duration, the ramp, Ctrl-C and errors. Each
//! client does one call at a time, drawn by the `--mix` weights:
//!
//! - `write`: `--records-per-call` records at the tail of the client's own
//!   journal (client `i` to journal `i % journals`). Clients that share a
//!   journal share its one writer, so their writes to it go one at a time.
//! - `read-tail`: the last `--records-per-call` records of a random
//!   journal.
//! - `read-start`: the next page of a random journal from a cursor that
//!   starts at its first position and starts again there at the tail.
//! - `set-leader`: the client's journal's writer takes a new term under a
//!   fresh uuid.
//!
//! `truncate` runs on its own schedule (`--truncate-every`): each journal
//! keeps its last `--truncate-keep` records. A read checks every byte.
//!
//! `--topology` names the journals' shapes (`topology.rs`): `3x1`, the
//! plain majority of three, by default. With several (`3x1,3x2,3x3`), the
//! bench runs each in turn, under the same seed and load, on journals of
//! its own (`bench-3x2-K`), then prints one row per topology and call.
//!
//! The clients run closed loop by default: a call starts when the last one
//! ends. With `--rate`, they run open loop: the calls start on a fixed
//! schedule, and a call's latency counts from the moment it was due, so a
//! slow call delays the ones after it and they count it (no coordinated
//! omission). Every random choice comes from `(seed, client index)`, and
//! the seed is logged. `--json` prints one report at the end
//! (`summary.rs`): the numbers with the commit, the command line, the CPU
//! count, the kernel and the filesystem of a local cell's stores.

mod histograms;
pub mod local;
mod mix;
mod records;
mod setup;
mod summary;
mod topology;

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime};

use clap::Args;
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::Value;
use paros::client::{Client, Reader, ReaderOutcome, TruncateOutcome, Writer, WriterOutcome};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::output::note;
use crate::{Ending, Runtime};

use histograms::{Call, Calls, Export, Registry};
use mix::Mix;
use records::{HEADER, Rng};
use topology::{Topologies, Topology};

type ParosClient = Client<TokioProviders>;

/// The console prints its header again after this many ticks.
const HEADER_EVERY: u64 = 20;

/// `parosctl bench`.
#[derive(Args, Debug)]
pub struct BenchArgs {
    /// The tenant to write in, by name; created when missing.
    #[arg(long, default_value = "bench")]
    tenant: String,
    /// How many journals per topology: `bench-T-0` to `bench-T-{N-1}`,
    /// created when missing (single writer).
    #[arg(long, default_value = "4", value_parser = clap::value_parser!(u64).range(1..))]
    journals: u64,
    /// The journal topologies, each run in turn on journals of its own:
    /// `ROWSxCOLUMNS`, comma-separated. `3x1` is the plain majority of
    /// three; `3x3` is nine acceptors in three partitions of three.
    #[arg(long, default_value = "3x1")]
    topology: Topologies,
    /// How many concurrent clients, each one call at a time.
    #[arg(long, default_value = "8", value_parser = clap::value_parser!(u64).range(1..))]
    clients: u64,
    /// How long to run after the ramp (`500ms`, `60s`, `2m`); `0s` runs until
    /// Ctrl-C.
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    duration: Duration,
    /// Start the clients one by one over this time; everything recorded
    /// before it ends is dropped.
    #[arg(long, default_value = "0s", value_parser = parse_duration)]
    ramp: Duration,
    /// The bytes of one record (at least 16: the seed and the position).
    #[arg(long, default_value = "256", value_parser = clap::value_parser!(u64).range(HEADER as u64..))]
    record_size: u64,
    /// The records of one write, and of one read.
    #[arg(long, default_value = "1", value_parser = clap::value_parser!(u64).range(1..))]
    records_per_call: u64,
    /// The calls each client draws, with their weights:
    /// `write=80,read-tail=15,read-start=5`. The calls are `write`,
    /// `read-tail`, `read-start` and `set-leader`; a call with no weight
    /// weighs 1.
    #[arg(long, default_value = "write")]
    mix: Mix,
    /// Run open loop: this many calls per second, over every client
    /// together, each counted from the moment it was due. Without it, each
    /// client calls again as soon as its last call ends (closed loop).
    #[arg(long, value_parser = parse_rate)]
    rate: Option<f64>,
    /// Truncate every journal this often, keeping its last
    /// `--truncate-keep` records (`0s`: never).
    #[arg(long, default_value = "0s", value_parser = parse_duration)]
    truncate_every: Duration,
    /// The records a truncation keeps.
    #[arg(long, default_value = "10000")]
    truncate_keep: u64,
    /// Start a cell of N machines in this process, on loopback, and run
    /// against it: `--servers` is not needed. The stores go in a new
    /// directory under `--local-dir`, deleted after the run.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..=MAX_LOCAL))]
    pub local: Option<u64>,
    /// Where a local cell's stores go: the filesystem the numbers measure.
    /// The system's temporary directory by default.
    #[arg(long, value_name = "DIR", requires = "local")]
    pub local_dir: Option<PathBuf>,
    /// The seed of every random choice; drawn and logged when absent.
    #[arg(long)]
    seed: Option<u64>,
    /// Log an error and continue; without it, the first error stops the run.
    #[arg(long)]
    tolerate_errors: bool,
    /// Write the raw histograms here: one JSON line per second per call.
    #[arg(long, value_name = "FILE")]
    histograms: Option<PathBuf>,
    /// How long the setup (tenant, journals, claims) may retry, in
    /// milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

impl BenchArgs {
    /// Whether the run writes: a call that needs each journal's writer.
    fn writes(&self) -> bool {
        self.mix.has(Call::Write) || self.mix.has(Call::SetLeader) || self.truncates()
    }

    fn truncates(&self) -> bool {
        !self.truncate_every.is_zero()
    }

    /// How long the setup may retry, in milliseconds.
    pub fn patience_ms(&self) -> u64 {
        self.patience_ms
    }

    /// Refuse a run its arguments cannot hold: a topology wider than the
    /// local cell (placement puts each acceptor on its own machine), or
    /// one histogram file for several topologies.
    ///
    /// # Errors
    ///
    /// The refusal, to print.
    pub fn check(&self) -> Result<(), String> {
        let widest = self.topology.widest();
        if let Some(machines) = self.local
            && widest as u64 > machines
        {
            return Err(format!(
                "--topology needs {widest} machines, and --local starts {machines}"
            ));
        }
        if self.histograms.is_some() && self.topology.all().len() > 1 {
            return Err("--histograms takes one topology".into());
        }
        Ok(())
    }
}

/// The most machines `--local` starts: a 3x5 or a 5x3 grid.
const MAX_LOCAL: u64 = 15;

/// A rate in calls per second, above zero.
fn parse_rate(text: &str) -> Result<f64, String> {
    match text.parse::<f64>() {
        Ok(rate) if rate.is_finite() && rate > 0.0 => Ok(rate),
        _ => Err(format!(
            "{text:?}: a rate is a number of calls per second above 0"
        )),
    }
}

/// A duration as `500ms`, `10s` or `2m`.
fn parse_duration(text: &str) -> Result<Duration, String> {
    let (digits, unit) = text
        .find(|c: char| !c.is_ascii_digit())
        .map_or((text, ""), |at| text.split_at(at));
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("{text:?}: a duration is a number and a unit (ms, s, m)"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_secs(value * 60)),
        _ => Err(format!("{text:?}: the unit is ms, s or m")),
    }
}

/// One journal of the run: its one writer, with a client of its own (a
/// leader hint names one journal's leader), and the tail its writes reached.
struct Slot {
    journal: setup::Journal,
    key: u64,
    client: ParosClient,
    writer: Mutex<Writer>,
    tail: Cell<u64>,
}

/// Why a call failed: what the run ends with, and the message.
struct Failure {
    ending: Ending,
    message: String,
}

impl Failure {
    fn new(ending: Ending, message: impl Into<String>) -> Self {
        Self {
            ending,
            message: message.into(),
        }
    }
}

/// What every client of one topology's run shares.
struct Shared {
    args: Rc<BenchArgs>,
    seed: u64,
    topology: Topology,
    /// Print one JSON report, and no tick.
    json: bool,
    slots: Vec<Rc<Slot>>,
    stop: CancellationToken,
    /// The first error, when it stopped the run.
    failure: RefCell<Option<Failure>>,
    /// Records `read-tail` read and found wrong.
    mismatches: Cell<u64>,
}

/// One topology's totals, for the comparison.
pub struct Measured {
    topology: Topology,
    elapsed: Duration,
    calls: Vec<Call>,
    total: Calls,
}

/// A checked bench: its arguments and its seed, shared by every topology.
pub struct Plan {
    args: Rc<BenchArgs>,
    seed: u64,
}

impl Plan {
    /// Check `args` and draw the seed when they name none.
    ///
    /// # Errors
    ///
    /// The arguments cannot hold a run ([`BenchArgs::check`]).
    pub fn new(runtime: &Runtime, args: BenchArgs) -> Result<Self, String> {
        args.check()?;
        let seed = args
            .seed
            .unwrap_or_else(|| runtime.providers.random().random());
        note(&format!("bench seed={seed}"));
        Ok(Self {
            args: Rc::new(args),
            seed,
        })
    }

    /// The topologies, in the order given.
    pub fn topologies(&self) -> Vec<Topology> {
        self.args.topology.all().to_vec()
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
        let (args, seed) = (&self.args, self.seed);
        note(&format!("topology {topology}: {}", topology.label()));
        let patience = Duration::from_millis(args.patience_ms);
        let slots = match prepare(runtime, client, servers, args, (seed, topology), patience).await
        {
            Ok(slots) => slots,
            Err(error) => {
                note(&error);
                return Err(Ending::Unreachable);
            }
        };
        let export = match &args.histograms {
            Some(path) => match Export::create(path, seed) {
                Ok(export) => Some(export),
                Err(error) => {
                    note(&format!("{}: {error}", path.display()));
                    return Err(Ending::Refused);
                }
            },
            None => None,
        };
        let shared = Rc::new(Shared {
            args: args.clone(),
            seed,
            topology,
            json,
            slots,
            stop: CancellationToken::new(),
            failure: RefCell::new(None),
            mismatches: Cell::new(0),
        });
        let (elapsed, calls, total) = tokio::task::LocalSet::new()
            .run_until(drive(client.clone(), shared.clone(), export, local))
            .await;
        let mismatches = shared.mismatches.get();
        if let Some(failure) = shared.failure.borrow_mut().take() {
            note(&failure.message);
            return Err(failure.ending);
        }
        if mismatches > 0 {
            note(&format!("read-tail found {mismatches} wrong records"));
            return Err(Ending::Refused);
        }
        Ok(Measured {
            topology,
            elapsed,
            calls,
            total,
        })
    }
}

/// End a bench whose topologies all ran: with several, and no `--json`,
/// print them side by side.
pub fn finish(runs: &[Measured], json: bool) -> Ending {
    if runs.len() > 1 && !json {
        compare(runs);
    }
    Ending::Success
}

/// `parosctl bench` against the cell `client` reaches (`servers`, their
/// ids): each topology in turn on the same cell, under the same seed. The
/// first that fails ends the bench. `json` prints one report per topology.
pub async fn run(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    args: BenchArgs,
    json: bool,
) -> Ending {
    let plan = match Plan::new(runtime, args) {
        Ok(plan) => plan,
        Err(error) => {
            note(&error);
            return Ending::Refused;
        }
    };
    let mut runs = Vec::new();
    for topology in plan.topologies() {
        match plan
            .run_one(runtime, (client, servers), topology, (json, None))
            .await
        {
            Ok(run) => runs.push(run),
            Err(ending) => return ending,
        }
    }
    finish(&runs, json)
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
    args: &BenchArgs,
    (seed, topology): (u64, Topology),
    patience: Duration,
) -> Result<Vec<Rc<Slot>>, String> {
    let providers = &runtime.providers;
    let tenant = setup::tenant(providers, client, servers, &args.tenant, patience).await?;
    let count = usize::try_from(args.journals).map_err(|e| e.to_string())?;
    let journals = setup::journals(
        runtime,
        client,
        (&args.tenant, tenant),
        (count, topology),
        patience,
    )
    .await?;
    let mut slots = Vec::with_capacity(journals.len());
    for (index, journal) in journals.into_iter().enumerate() {
        let journal_client = client.clone().with_own_leader_hint();
        let mut rng = Rng::of(&[seed, 1, index as u64]);
        let leader_seed = u128::from(rng.next_u64()) << 64 | u128::from(rng.next_u64());
        let mut writer = Writer::new(journal.id, leader_seed);
        let tail = if args.writes() {
            setup::claim(&journal_client, &mut writer, &journal, patience).await?;
            writer.next_seq()
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
            writer: Mutex::new(writer),
            tail: Cell::new(tail),
        }));
    }
    Ok(slots)
}

/// Start the clients and the truncations, tick every second until the run
/// ends, then print the totals (or the report, with `--json`). The time
/// measured, the calls drawn and their totals.
async fn drive(
    client: ParosClient,
    shared: Rc<Shared>,
    mut export: Option<Export>,
    local: Option<&Path>,
) -> (Duration, Vec<Call>, Calls) {
    let args = &shared.args;
    let mut registry = Registry::default();
    let start = Instant::now();
    for index in 0..args.clients {
        let calls = registry.client();
        let client = client.clone().with_own_leader_hint();
        let shared = shared.clone();
        // Stagger the starts over the ramp.
        let delay = stagger(args.ramp, index, args.clients);
        tokio::task::spawn_local(async move {
            tokio::select! {
                () = shared.stop.cancelled() => {}
                () = worker(&client, &shared, index, &calls, delay) => {}
            }
        });
    }
    if args.truncates() {
        let calls = registry.client();
        let shared = shared.clone();
        tokio::task::spawn_local(async move {
            tokio::select! {
                () = shared.stop.cancelled() => {}
                () = truncator(&shared, &calls) => {}
            }
        });
    }
    let calls: Vec<Call> = Call::ALL
        .into_iter()
        .filter(|call| args.mix.has(*call) || (*call == Call::Truncate && args.truncates()))
        .collect();
    let end = (!args.duration.is_zero()).then(|| args.ramp + args.duration);
    let mut ramping = !args.ramp.is_zero();
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    let mut ticks = 0_u64;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        tokio::select! {
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
            // The total counts from the end of the ramp, once it is reset.
            let window = if ramping {
                elapsed
            } else {
                elapsed.saturating_sub(args.ramp)
            };
            for call in &calls {
                println!(
                    "{}",
                    tick_row(
                        (elapsed, window),
                        *call,
                        &interval,
                        &registry.total,
                        elapsed < args.ramp
                    )
                );
            }
        }
        ticks += 1;
        record(&mut export, elapsed, &calls, &interval);
        if ramping && elapsed >= args.ramp {
            ramping = false;
            registry.reset();
        }
        if end.is_some_and(|end| elapsed >= end) {
            break;
        }
    }
    shared.stop.cancel();
    let elapsed = start.elapsed();
    let last = registry.tick();
    record(&mut export, elapsed, &calls, &last);
    let measured = if ramping {
        elapsed
    } else {
        elapsed.saturating_sub(args.ramp)
    };
    report(&shared, local, measured, (&calls, &registry.total));
    if let Some(file) = export
        && let Err(error) = file.finish()
    {
        note(&format!("histograms: {error}"));
    }
    (measured, calls, std::mem::take(&mut registry.total))
}

/// The totals as text, or as one JSON report with `--json`.
fn report(
    shared: &Shared,
    local: Option<&Path>,
    measured: Duration,
    (calls, total): (&[Call], &Calls),
) {
    let args = &shared.args;
    if !shared.json {
        totals(measured, calls, total, args.mix.has(Call::Write));
        return;
    }
    let run = serde_json::json!({
        "seed": shared.seed,
        "target": match local {
            Some(_) => serde_json::json!({ "local": args.local }),
            None => serde_json::json!("servers"),
        },
        "topology": shared.topology.to_string(),
        "acceptors": shared.topology.acceptors(),
        "clients": args.clients,
        "journals": args.journals,
        "record_size": args.record_size,
        "records_per_call": args.records_per_call,
        "rate": args.rate,
        "mismatches": shared.mismatches.get(),
    });
    println!("{}", summary::document(&run, local, measured, calls, total));
}

/// Client `index`'s start, of `clients` staggered over `ramp`.
fn stagger(ramp: Duration, index: u64, clients: u64) -> Duration {
    let nanos = ramp.as_nanos() * u128::from(index) / u128::from(clients);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
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

/// Count a failed `call`; without `--tolerate-errors`, the first failure
/// stops the run. Whether the caller stops.
fn failed(shared: &Shared, calls: &RefCell<Calls>, call: Call, failure: &Failure) -> bool {
    calls.borrow_mut().error(call);
    if shared.args.tolerate_errors {
        note(&format!("{}: {}", call.name(), failure.message));
        return false;
    }
    let mut first = shared.failure.borrow_mut();
    if first.is_none() {
        *first = Some(Failure::new(
            failure.ending,
            format!("{} stopped the run: {}", call.name(), failure.message),
        ));
    }
    shared.stop.cancel();
    true
}

/// One client: wait out its place in the ramp, then call until stopped:
/// as soon as the last call ends, or on the `--rate` schedule.
async fn worker(
    client: &ParosClient,
    shared: &Shared,
    index: u64,
    calls: &RefCell<Calls>,
    delay: Duration,
) {
    tokio::time::sleep(delay).await;
    let args = &shared.args;
    let mut rng = Rng::of(&[shared.seed, 2, index]);
    let journals = usize::try_from(args.journals).expect("a journal count fits");
    let own = usize::try_from(index % args.journals).expect("a journal index fits");
    // Each client's share of the rate: one call every `period`.
    #[allow(
        clippy::cast_precision_loss,
        reason = "a client count is far below 2^52"
    )]
    let period = args
        .rate
        .map(|rate| Duration::from_secs_f64(args.clients as f64 / rate));
    let mut due = Instant::now();
    // A `read-start` cursor per journal.
    let mut cursors = vec![0_u64; journals];
    loop {
        let began = match period {
            Some(period) => {
                let at = due;
                due += period;
                tokio::time::sleep_until(at.into()).await;
                at
            }
            None => Instant::now(),
        };
        let call = args.mix.draw(&mut rng);
        let any = usize::try_from(rng.below(args.journals)).expect("a journal index fits");
        let result = match call {
            Call::Write => write(shared, &shared.slots[own]).await,
            Call::ReadTail => read_tail(client, shared, &shared.slots[any]).await,
            Call::ReadStart => {
                read_start(client, shared, &shared.slots[any], &mut cursors[any]).await
            }
            Call::SetLeader => set_leader(shared, &shared.slots[own]).await,
            Call::Truncate => unreachable!("truncate is never in the mix"),
        };
        match result {
            Ok(bytes) => calls.borrow_mut().record(call, began.elapsed(), bytes),
            Err(failure) => {
                if failed(shared, calls, call, &failure) {
                    return;
                }
            }
        }
    }
}

/// Every `--truncate-every`, truncate each journal below its last
/// `--truncate-keep` records, as its writer.
async fn truncator(shared: &Shared, calls: &RefCell<Calls>) {
    let args = &shared.args;
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + args.truncate_every,
        args.truncate_every,
    );
    loop {
        ticker.tick().await;
        for slot in &shared.slots {
            let up_to = slot.tail.get().saturating_sub(args.truncate_keep);
            if up_to == 0 {
                continue;
            }
            let began = Instant::now();
            match truncate(shared, slot, up_to).await {
                Ok(()) => calls
                    .borrow_mut()
                    .record(Call::Truncate, began.elapsed(), 0),
                Err(failure) => {
                    if failed(shared, calls, Call::Truncate, &failure) {
                        return;
                    }
                }
            }
        }
    }
}

/// The record bytes of one call.
fn call_bytes(args: &BenchArgs) -> u64 {
    args.records_per_call * args.record_size
}

/// Claim the journal again under `writer` when another uuid superseded it.
async fn owned(shared: &Shared, slot: &Slot, writer: &mut Writer) -> Result<(), Failure> {
    if writer.owned().is_none() {
        let patience = Duration::from_millis(shared.args.patience_ms);
        setup::claim(&slot.client, writer, &slot.journal, patience)
            .await
            .map_err(|e| Failure::new(Ending::Refused, e))?;
    }
    Ok(())
}

/// One `write` of `--records-per-call` records at the journal's tail, as
/// its writer. A writer another uuid superseded claims again first.
async fn write(shared: &Shared, slot: &Slot) -> Result<u64, Failure> {
    let args = &shared.args;
    let mut writer = slot.writer.lock().await;
    let size = usize::try_from(args.record_size).expect("a record size fits");
    owned(shared, slot, &mut writer).await?;
    let retries = slot.client.tunables().retry_budget;
    let mut attempt = 0;
    loop {
        let from = writer.next_seq();
        let batch: Vec<Value> = (from..from + args.records_per_call)
            .map(|seq| Value(records::record(shared.seed, slot.key, seq, size)))
            .collect();
        let outcome = writer.write(&slot.client, batch, 0).await;
        match outcome {
            WriterOutcome::Written { seq, count, .. } => {
                slot.tail.set(slot.tail.get().max(seq + count));
                return Ok(call_bytes(args));
            }
            // A stale position, still the owner: the refusal named the
            // tail; the records of the new position go there.
            WriterOutcome::Refused { .. } if writer.owned().is_some() && attempt < retries => {
                attempt += 1;
            }
            WriterOutcome::Ambiguous => {
                return Err(Failure::new(
                    Ending::Ambiguous,
                    "an answer never came: the write may land",
                ));
            }
            WriterOutcome::Unavailable { .. } => {
                return Err(Failure::new(
                    Ending::Unreachable,
                    "no leader took the write",
                ));
            }
            other => {
                return Err(Failure::new(
                    Ending::Refused,
                    format!("{}: {other:?}", slot.journal.name),
                ));
            }
        }
    }
}

/// One `set-leader`: the journal's writer takes a new term under a fresh
/// uuid, which supersedes its last one.
async fn set_leader(shared: &Shared, slot: &Slot) -> Result<u64, Failure> {
    let mut writer = slot.writer.lock().await;
    writer.begin_term();
    owned(shared, slot, &mut writer).await?;
    Ok(0)
}

/// One `truncate` of the journal below `up_to`, as its writer.
async fn truncate(shared: &Shared, slot: &Slot, up_to: u64) -> Result<(), Failure> {
    let mut writer = slot.writer.lock().await;
    owned(shared, slot, &mut writer).await?;
    match writer.truncate(&slot.client, up_to, 0).await {
        Some(TruncateOutcome::Applied { .. }) => Ok(()),
        Some(TruncateOutcome::Ambiguous) => Err(Failure::new(
            Ending::Ambiguous,
            "an answer never came: the truncation may land",
        )),
        other => Err(Failure::new(
            Ending::Refused,
            format!("{}: {other:?}", slot.journal.name),
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
            format!("no server serves {}", slot.journal.name),
        ),
        ReaderOutcome::Denied(denial) => Failure::new(Ending::Refused, denial.as_str()),
        _ => Failure::new(Ending::Unreachable, "no server served the read"),
    }
}

/// One `read-tail`: the last `--records-per-call` records the bench knows
/// of, every byte checked.
async fn read_tail(client: &ParosClient, shared: &Shared, slot: &Slot) -> Result<u64, Failure> {
    let limit = shared.args.records_per_call;
    let from = slot.tail.get().saturating_sub(limit);
    let mut reader = Reader::new(slot.journal.id, from);
    let report = client
        .read_any(&reader.request(limit, 0), client.leader().unwrap_or(0))
        .await;
    match reader.absorb(report.outcome) {
        ReaderOutcome::Records { from, records, .. } => checked(shared, slot, from, &records),
        // Nothing to check below the floor: a truncation passed it.
        ReaderOutcome::Gap { .. } => Ok(0),
        other => Err(unread(slot, &other)),
    }
}

/// One `read-start`: the next `--records-per-call` records from `cursor`,
/// every byte checked; at the tail the cursor starts again at 0, and a
/// truncation moves it to the floor.
async fn read_start(
    client: &ParosClient,
    shared: &Shared,
    slot: &Slot,
    cursor: &mut u64,
) -> Result<u64, Failure> {
    let limit = shared.args.records_per_call;
    let mut reader = Reader::new(slot.journal.id, *cursor);
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
        ReaderOutcome::Gap { .. } => {
            *cursor = reader.cursor();
            Ok(0)
        }
        other => Err(unread(slot, &other)),
    }
}

/// A count as a float, for display only.
#[allow(
    clippy::cast_precision_loss,
    reason = "a printed rate needs no 64-bit precision"
)]
fn float(count: u64) -> f64 {
    count as f64
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

fn tick_row(
    (elapsed, window): (Duration, Duration),
    call: Call,
    interval: &Calls,
    total: &Calls,
    ramping: bool,
) -> String {
    let h = interval.of(call);
    let secs = window.as_secs_f64().max(1e-9);
    format!(
        "{:>7.1}s {:>8} {:>14.1} {:>14.1} {:>8} {:>8} {:>8} {:>8}  {}{}",
        elapsed.as_secs_f64(),
        total.errors_of(call),
        float(h.len()),
        float(total.of(call).len()) / secs,
        ms(h.value_at_quantile(0.50)),
        ms(h.value_at_quantile(0.95)),
        ms(h.value_at_quantile(0.99)),
        ms(h.max()),
        call.name(),
        if ramping { " (ramp)" } else { "" },
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
        "{:>7.1}s {:>8} {:>12} {:>12.1} {:>9.2} {:>8} {:>8} {:>8} {:>8} {:>9} {:>8}  {name}",
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
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{parse_duration, parse_rate};

    #[test]
    fn durations_parse_with_their_unit() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("60s"), Ok(Duration::from_secs(60)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert!(parse_duration("60").is_err());
        assert!(parse_duration("s").is_err());
        assert!(parse_duration("1h").is_err());
    }

    #[test]
    fn a_rate_is_above_zero() {
        assert_eq!(parse_rate("250"), Ok(250.0));
        assert_eq!(parse_rate("0.5"), Ok(0.5));
        assert!(parse_rate("0").is_err());
        assert!(parse_rate("-1").is_err());
        assert!(parse_rate("inf").is_err());
        assert!(parse_rate("fast").is_err());
    }
}
