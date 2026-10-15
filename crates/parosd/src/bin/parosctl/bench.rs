//! `parosctl bench` (#517 (`parosctl bench`)): the load generator. It runs
//! closed-loop clients against a cell over `paros::client` and prints
//! throughput and latency per call, after `CockroachDB`'s load generator.
//!
//! The bench owns the loop: the duration, the ramp, Ctrl-C and errors. Each
//! client does one call at a time, `write` or `read-tail` as
//! `--read-percent` draws. A client writes to its own journal (client `i` to
//! journal `i % journals`); clients that share a journal share its one
//! writer, so their writes to it go one at a time. A `read-tail` reads the
//! last `--records-per-call` records of a random journal and checks every
//! byte. Every random choice comes from `(seed, client index)`, and the
//! seed is logged.
//!
//! These numbers compare two runs on the same machine. They are not a
//! baseline (#194 (throughput benchmark)).

mod histograms;
mod records;
mod setup;

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime};

use clap::Args;
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::Value;
use paros::client::{Client, ReaderOutcome, Writer, WriterOutcome};
use paros::tenant::Desired;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::output::note;
use crate::{Ending, Runtime};

use histograms::{Call, Calls, Export, Registry};
use records::{HEADER, Rng};

type ParosClient = Client<TokioProviders>;

/// The console prints its header again after this many ticks.
const HEADER_EVERY: u64 = 20;

/// `parosctl bench`.
#[derive(Args, Debug)]
pub struct BenchArgs {
    /// The tenant to write in, by name; created when missing.
    #[arg(long, default_value = "bench")]
    tenant: String,
    /// How many journals: `bench-0` to `bench-{N-1}`, created when missing
    /// (single writer).
    #[arg(long, default_value = "4", value_parser = clap::value_parser!(u64).range(1..))]
    journals: u64,
    /// The desired mode of a journal the bench creates.
    #[arg(long, default_value = "double")]
    desired: Desired,
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
    /// The share of calls that are `read-tail`, in percent; the rest are
    /// `write`.
    #[arg(long, default_value = "0", value_parser = clap::value_parser!(u8).range(0..=100))]
    read_percent: u8,
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

/// What every client shares.
struct Shared {
    args: BenchArgs,
    seed: u64,
    slots: Vec<Rc<Slot>>,
    stop: CancellationToken,
    /// The first error, when it stopped the run.
    failure: RefCell<Option<Failure>>,
    /// Records `read-tail` read and found wrong.
    mismatches: Cell<u64>,
}

/// `parosctl bench`.
pub async fn run(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    args: BenchArgs,
) -> Ending {
    let seed = args
        .seed
        .unwrap_or_else(|| runtime.providers.random().random());
    note(&format!("bench seed={seed}"));
    let patience = Duration::from_millis(args.patience_ms);
    let slots = match prepare(runtime, client, servers, &args, seed, patience).await {
        Ok(slots) => slots,
        Err(error) => {
            note(&error);
            return Ending::Unreachable;
        }
    };
    let export = match &args.histograms {
        Some(path) => match Export::create(path, seed) {
            Ok(export) => Some(export),
            Err(error) => {
                note(&format!("{}: {error}", path.display()));
                return Ending::Refused;
            }
        },
        None => None,
    };
    let shared = Rc::new(Shared {
        args,
        seed,
        slots,
        stop: CancellationToken::new(),
        failure: RefCell::new(None),
        mismatches: Cell::new(0),
    });
    let local = tokio::task::LocalSet::new();
    local
        .run_until(drive(client.clone(), shared.clone(), export))
        .await;
    let mismatches = shared.mismatches.get();
    if let Some(failure) = shared.failure.borrow_mut().take() {
        note(&failure.message);
        return failure.ending;
    }
    if mismatches > 0 {
        note(&format!("read-tail found {mismatches} wrong records"));
        return Ending::Refused;
    }
    Ending::Success
}

/// The tenant, the journals, and a claimed writer on each.
async fn prepare(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    args: &BenchArgs,
    seed: u64,
    patience: Duration,
) -> Result<Vec<Rc<Slot>>, String> {
    let providers = &runtime.providers;
    let tenant = setup::tenant(providers, client, servers, &args.tenant, patience).await?;
    let count = usize::try_from(args.journals).map_err(|e| e.to_string())?;
    let journals = setup::journals(
        runtime,
        client,
        (&args.tenant, tenant),
        (count, args.desired),
        patience,
    )
    .await?;
    let mut slots = Vec::with_capacity(journals.len());
    for (index, journal) in journals.into_iter().enumerate() {
        let journal_client = client.clone().with_own_leader_hint();
        let mut rng = Rng::of(&[seed, 1, index as u64]);
        let leader_seed = u128::from(rng.next_u64()) << 64 | u128::from(rng.next_u64());
        let mut writer = Writer::new(journal.id, leader_seed);
        let tail = if args.read_percent < 100 {
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

/// Start the clients, tick every second until the run ends, then print the
/// totals.
async fn drive(client: ParosClient, shared: Rc<Shared>, mut export: Option<Export>) {
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
    let calls: Vec<Call> = Call::ALL
        .into_iter()
        .filter(|call| match call {
            Call::Write => args.read_percent < 100,
            Call::ReadTail => args.read_percent > 0,
        })
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
        if ticks.is_multiple_of(HEADER_EVERY) {
            println!("{}", tick_header());
        }
        ticks += 1;
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
        record(&mut export, elapsed, &interval);
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
    record(&mut export, elapsed, &last);
    let measured = if ramping {
        elapsed
    } else {
        elapsed.saturating_sub(args.ramp)
    };
    totals(measured, &calls, &registry.total, args.read_percent < 100);
    if let Some(file) = export
        && let Err(error) = file.finish()
    {
        note(&format!("histograms: {error}"));
    }
}

/// Client `index`'s start, of `clients` staggered over `ramp`.
fn stagger(ramp: Duration, index: u64, clients: u64) -> Duration {
    let nanos = ramp.as_nanos() * u128::from(index) / u128::from(clients);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// Export one interval; a failed export stops exporting, never the run.
fn record(export: &mut Option<Export>, elapsed: Duration, interval: &Calls) {
    if let Some(file) = export.as_mut()
        && let Err(error) = file.tick(elapsed, unix_ms(), interval)
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

/// One client: wait out its place in the ramp, then call until stopped.
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
    let own = usize::try_from(index % args.journals).expect("a journal index fits");
    loop {
        let read = rng.below(100) < u64::from(args.read_percent);
        let began = Instant::now();
        let (call, result) = if read {
            let slot = usize::try_from(rng.below(args.journals)).expect("a journal index fits");
            (
                Call::ReadTail,
                read_tail(client, shared, &shared.slots[slot]).await,
            )
        } else {
            (Call::Write, write(shared, &shared.slots[own]).await)
        };
        match result {
            Ok(()) => calls.borrow_mut().record(call, began.elapsed()),
            Err(failure) => {
                calls.borrow_mut().error(call);
                if args.tolerate_errors {
                    note(&format!("{}: {}", call.name(), failure.message));
                } else {
                    let mut first = shared.failure.borrow_mut();
                    if first.is_none() {
                        *first = Some(Failure::new(
                            failure.ending,
                            format!("{} stopped the run: {}", call.name(), failure.message),
                        ));
                    }
                    shared.stop.cancel();
                    return;
                }
            }
        }
    }
}

/// One `write` of `--records-per-call` records at the journal's tail, as
/// its writer. A writer another uuid superseded claims again first.
async fn write(shared: &Shared, slot: &Slot) -> Result<(), Failure> {
    let args = &shared.args;
    let mut writer = slot.writer.lock().await;
    let size = usize::try_from(args.record_size).expect("a record size fits");
    if writer.owned().is_none() {
        let patience = Duration::from_millis(args.patience_ms);
        setup::claim(&slot.client, &mut writer, &slot.journal, patience)
            .await
            .map_err(|e| Failure::new(Ending::Refused, e))?;
    }
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
                return Ok(());
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

/// One `read-tail`: the last `--records-per-call` records the bench knows
/// of, every byte checked.
async fn read_tail(client: &ParosClient, shared: &Shared, slot: &Slot) -> Result<(), Failure> {
    let limit = shared.args.records_per_call;
    let from = slot.tail.get().saturating_sub(limit);
    let mut reader = paros::client::Reader::new(slot.journal.id, from);
    let report = client
        .read_any(&reader.request(limit, 0), client.leader().unwrap_or(0))
        .await;
    match reader.absorb(report.outcome) {
        ReaderOutcome::Records { from, records, .. } => {
            for (seq, record) in (from..).zip(&records) {
                if !records::check(slot.key, seq, record) {
                    shared.mismatches.set(shared.mismatches.get() + 1);
                    return Err(Failure::new(
                        Ending::Refused,
                        format!("wrong bytes at {} position {seq}", slot.journal.name),
                    ));
                }
            }
            Ok(())
        }
        // Nothing to check below the floor: the bench never truncates, an
        // operator may.
        ReaderOutcome::Gap { .. } => Ok(()),
        ReaderOutcome::Unavailable => Err(Failure::new(
            Ending::Unreachable,
            "no server served the read",
        )),
        ReaderOutcome::UnknownJournal => Err(Failure::new(
            Ending::Refused,
            format!("no server serves {}", slot.journal.name),
        )),
        ReaderOutcome::Denied(denial) => Err(Failure::new(Ending::Refused, denial.as_str())),
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
        "{:>8} {:>8} {:>12} {:>12} {:>8} {:>8} {:>8} {:>8} {:>8}  {last}",
        "elapsed",
        "errors",
        "ops(total)",
        "ops/s(cum)",
        "avg(ms)",
        "p50(ms)",
        "p95(ms)",
        "p99(ms)",
        "max(ms)"
    )
}

fn total_row(measured: Duration, name: &str, total: &Calls, call: Call) -> String {
    let h = total.of(call);
    format!(
        "{:>7.1}s {:>8} {:>12} {:>12.1} {:>8} {:>8} {:>8} {:>8} {:>8}  {name}",
        measured.as_secs_f64(),
        total.errors_of(call),
        h.len(),
        float(h.len()) / measured.as_secs_f64().max(1e-9),
        format!("{:.1}", h.mean() / 1e6),
        ms(h.value_at_quantile(0.50)),
        ms(h.value_at_quantile(0.95)),
        ms(h.value_at_quantile(0.99)),
        ms(h.max()),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::parse_duration;

    #[test]
    fn durations_parse_with_their_unit() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("60s"), Ok(Duration::from_secs(60)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert!(parse_duration("60").is_err());
        assert!(parse_duration("s").is_err());
        assert!(parse_duration("1h").is_err());
    }
}
