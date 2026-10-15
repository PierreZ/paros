//! **Metrics** (#366, `docs/architecture.md` §3.6): what a machine counts
//! about the calls it serves, for an operator, never for a decision.
//!
//! The library records through moonpool's `PrometheusSource`
//! (`moonpool-prometheus`): a plain `prometheus` registry, provider-free and
//! wasm-safe, that the simulation also reads per machine. `parosd` serves it
//! as Prometheus text (`/metrics`); the simulation hands every machine the
//! source the runner keeps for its address, so a reboot finds its counters
//! again, as a real `/metrics` would on the same host.
//!
//! | metric | kind | labels | where |
//! |---|---|---|---|
//! | `paros_calls_total` | counter | `call`, `tenant`, `journal` | a data-plane call reaches a journal this machine runs |
//! | `paros_call_seconds` | histogram | `call` | the call's answer leaves (a dropped answer is not timed) |
//! | `paros_written_bytes_total` | counter | `tenant`, `journal` | an accepted `Write` is answered |
//! | `paros_read_bytes_total` | counter | `tenant`, `journal` | a `Read` page is answered |
//! | `paros_applied_slot`, `paros_chosen_slot` | gauge | `tenant`, `journal` | every settled batch; the apply lag is their difference |
//! | `paros_settle_seconds` | histogram | | a batch persisted (fsync included), sent and applied |
//! | `paros_machine_*` | gauge | | the last busyness window (#424) |
//! | `paros_machine_phase` | gauge | | 0 starting, 1 idle, 2 serving |
//!
//! Labels name entities by their names (tenant and journal), never by ids;
//! an id is printed in hex only for a journal whose names the machine does
//! not know.
//!
//! **Observation only.** A metric is written after the outcome it counts
//! is decided and is never read by the code that serves: no branch, no
//! draw, no wait depends on it. With metrics off ([`Metrics::off`]) every
//! call is a no-op and the machine serves the same. The simulation draws
//! metrics on or off per seed and judges both with the same oracles, and
//! checks that the counters match the audit's own fold of the calls.
//!
//! Usage per journal ([`Metrics::usage`]) is read back from the same
//! counters: `parosctl status` sums it over the machines of a cell through
//! `Load`. It counts since each machine's process started.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

pub use moonpool_prometheus::PrometheusSource;
use moonpool_prometheus::{
    SimCounter, SimCounterVec, SimGauge, SimGaugeVec, SimHistogram, SimHistogramVec,
};
use paros_core::JournalIdentifier;
use prometheus::HistogramOpts;

use crate::load::Busyness;

/// The bounds of `paros_call_seconds`, in seconds: half a millisecond to
/// ten seconds (a long-poll `Read` waits up to its own bound).
const CALL_BUCKETS: [f64; 14] = [
    0.000_5, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0,
];

/// The bounds of `paros_settle_seconds`, in seconds.
const SETTLE_BUCKETS: [f64; 12] = [
    0.000_1, 0.000_25, 0.000_5, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.5, 1.0,
];

const _: () = assert!(CALL_BUCKETS[0] > 0.0);
const _: () = assert!(SETTLE_BUCKETS[0] > 0.0);

/// A data-plane call, the `call` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CallKind {
    /// A `Write`.
    Write,
    /// A `Read`.
    Read,
    /// A `SetLeader`.
    SetLeader,
    /// A `Truncate`.
    Truncate,
}

impl CallKind {
    /// Every call, in label order.
    pub const ALL: [CallKind; 4] = [
        CallKind::Write,
        CallKind::Read,
        CallKind::SetLeader,
        CallKind::Truncate,
    ];

    /// The stable `call` label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            CallKind::Write => "write",
            CallKind::Read => "read",
            CallKind::SetLeader => "set_leader",
            CallKind::Truncate => "truncate",
        }
    }

    /// The call a label names.
    #[must_use]
    pub fn of_label(label: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|call| call.label() == label)
    }

    fn index(self) -> usize {
        match self {
            CallKind::Write => 0,
            CallKind::Read => 1,
            CallKind::SetLeader => 2,
            CallKind::Truncate => 3,
        }
    }
}

/// Where a machine is in its lifecycle: what `/readyz` answers from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Before its record is read.
    Starting,
    /// It belongs to no cell yet and waits for one (`cell init`, `cell
    /// add-machine`).
    Idle,
    /// A cell member whose node loop runs: its stores opened.
    Serving,
}

impl Phase {
    fn to_u8(self) -> u8 {
        match self {
            Phase::Starting => 0,
            Phase::Idle => 1,
            Phase::Serving => 2,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Phase::Starting,
            1 => Phase::Idle,
            2 => Phase::Serving,
            _ => unreachable!("a phase is stored from Phase::to_u8"),
        }
    }

    /// The phase's name, for people.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Idle => "idle",
            Phase::Serving => "serving",
        }
    }
}

/// What one machine counted about one journal since its process started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalUsage {
    /// The journal.
    pub journal: JournalIdentifier,
    /// Calls that reached the journal here, per [`CallKind::ALL`].
    pub calls: [u64; 4],
    /// Bytes of records in the `Write`s accepted here.
    pub written_bytes: u64,
    /// Bytes of records in the `Read` pages answered here.
    pub read_bytes: u64,
}

impl JournalUsage {
    /// The calls of kind `call`.
    #[must_use]
    pub fn calls_of(&self, call: CallKind) -> u64 {
        self.calls[call.index()]
    }

    /// Every call, of any kind.
    #[must_use]
    pub fn total_calls(&self) -> u64 {
        self.calls.iter().sum()
    }

    /// Add `other`'s counts (another machine's, for the same journal).
    ///
    /// # Panics
    ///
    /// When `other` counts another journal.
    pub fn add(&mut self, other: &JournalUsage) {
        assert_eq!(self.journal, other.journal, "usage adds up per journal");
        for (mine, theirs) in self.calls.iter_mut().zip(other.calls) {
            *mine = mine.saturating_add(theirs);
        }
        self.written_bytes = self.written_bytes.saturating_add(other.written_bytes);
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
    }
}

/// A machine's metrics: cheap to clone, shared by every task of the
/// machine. [`Metrics::off`] (the default) records nothing.
#[derive(Clone, Default)]
pub struct Metrics(Arc<Shared>);

#[derive(Default)]
struct Shared {
    phase: AtomicU8,
    families: Option<Families>,
    /// The journals' handles, created at their first call.
    journals: Mutex<BTreeMap<JournalIdentifier, JournalMetrics>>,
}

/// The registered families of one source.
struct Families {
    source: Arc<PrometheusSource>,
    calls: SimCounterVec,
    latency: SimHistogramVec,
    written: SimCounterVec,
    read: SimCounterVec,
    applied: SimGaugeVec,
    chosen: SimGaugeVec,
    settle: SimHistogram,
    phase: SimGauge,
    cpu_cores: SimGauge,
    machine_cpu: SimGauge,
    disk_busy: SimGauge,
    disk_queue: SimGauge,
}

/// One journal's handles, labelled once with its names.
#[derive(Clone)]
pub(crate) struct JournalMetrics {
    calls: [SimCounter; 4],
    latency: [SimHistogram; 4],
    written: SimCounter,
    read: SimCounter,
    applied: SimGauge,
    chosen: SimGauge,
}

/// A family the source refuses is a bug here: the names, labels and
/// buckets are constants.
fn registered<T>(family: prometheus::Result<T>) -> T {
    family.unwrap_or_else(|error| unreachable!("a paros metric registers: {error}"))
}

impl Families {
    fn register(source: Arc<PrometheusSource>) -> Self {
        let journal = &["tenant", "journal"];
        let calls = registered(source.counter_vec(
            "paros_calls_total",
            "Data-plane calls that reached a journal this machine runs.",
            &["call", "tenant", "journal"],
        ));
        let latency = registered(
            source.histogram_vec(
                HistogramOpts::new(
                    "paros_call_seconds",
                    "Time from a call's arrival to its answer.",
                )
                .buckets(CALL_BUCKETS.to_vec()),
                &["call"],
            ),
        );
        let written = registered(source.counter_vec(
            "paros_written_bytes_total",
            "Bytes of records in the writes this machine accepted.",
            journal,
        ));
        let read = registered(source.counter_vec(
            "paros_read_bytes_total",
            "Bytes of records in the read pages this machine answered.",
            journal,
        ));
        let applied = registered(source.gauge_vec(
            "paros_applied_slot",
            "The slot this machine's journal fold applied up to.",
            journal,
        ));
        let chosen = registered(source.gauge_vec(
            "paros_chosen_slot",
            "The slot this machine knows chosen up to.",
            journal,
        ));
        let settle = registered(source.histogram_with_buckets(
            "paros_settle_seconds",
            "Time a batch takes to persist (fsync included), send and apply.",
            SETTLE_BUCKETS.to_vec(),
        ));
        let phase = registered(source.gauge(
            "paros_machine_phase",
            "The machine's phase: 0 starting, 1 idle, 2 serving.",
        ));
        let cpu_cores = registered(source.gauge(
            "paros_machine_cpu_cores",
            "CPU cores the process used over the last window.",
        ));
        let machine_cpu = registered(source.gauge(
            "paros_machine_cpu_ratio",
            "The whole machine's CPU use over the last window, from 0 to 1.",
        ));
        let disk_busy = registered(source.gauge(
            "paros_machine_disk_busy_ratio",
            "The share of the last window the data disk was busy, from 0 to 1.",
        ));
        let disk_queue = registered(source.gauge(
            "paros_machine_disk_queue_depth",
            "I/Os in flight on the data disk at the last window's end.",
        ));
        Self {
            source,
            calls,
            latency,
            written,
            read,
            applied,
            chosen,
            settle,
            phase,
            cpu_cores,
            machine_cpu,
            disk_busy,
            disk_queue,
        }
    }

    fn journal(&self, tenant: &str, journal: &str) -> JournalMetrics {
        let per_call = |call: CallKind| {
            registered(
                self.calls
                    .with_label_values(&[call.label(), tenant, journal]),
            )
        };
        let latency = |call: CallKind| registered(self.latency.with_label_values(&[call.label()]));
        JournalMetrics {
            calls: CallKind::ALL.map(per_call),
            latency: CallKind::ALL.map(latency),
            written: registered(self.written.with_label_values(&[tenant, journal])),
            read: registered(self.read.with_label_values(&[tenant, journal])),
            applied: registered(self.applied.with_label_values(&[tenant, journal])),
            chosen: registered(self.chosen.with_label_values(&[tenant, journal])),
        }
    }
}

impl Metrics {
    /// Metrics that record nothing: the default.
    #[must_use]
    pub fn off() -> Self {
        Self::default()
    }

    /// Metrics recorded on a source of their own: `parosd`'s.
    #[must_use]
    pub fn on() -> Self {
        Self::new(Arc::new(PrometheusSource::default()))
    }

    /// Metrics recorded on `source`. A source that already holds paros's
    /// families (a machine started again on the same host, in the
    /// simulation) is looked up, never registered twice.
    #[must_use]
    pub fn new(source: Arc<PrometheusSource>) -> Self {
        Self(Arc::new(Shared {
            phase: AtomicU8::new(Phase::Starting.to_u8()),
            families: Some(Families::register(source)),
            journals: Mutex::new(BTreeMap::new()),
        }))
    }

    /// Whether these metrics record.
    #[must_use]
    pub fn is_on(&self) -> bool {
        self.0.families.is_some()
    }

    /// The source they record on, if any.
    #[must_use]
    pub fn source(&self) -> Option<&Arc<PrometheusSource>> {
        self.0.families.as_ref().map(|families| &families.source)
    }

    /// Every metric of the source in the Prometheus text format; empty when
    /// off.
    #[must_use]
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let Some(families) = &self.0.families else {
            return String::new();
        };
        let gathered = families.source.registry().gather();
        let mut text = Vec::new();
        match prometheus::TextEncoder::new().encode(&gathered, &mut text) {
            Ok(()) => String::from_utf8_lossy(&text).into_owned(),
            Err(error) => {
                tracing::warn!(%error, "metrics_unrendered");
                String::new()
            }
        }
    }

    /// The machine's phase, as its lifecycle last set it.
    #[must_use]
    pub fn phase(&self) -> Phase {
        Phase::from_u8(self.0.phase.load(Ordering::Acquire))
    }

    /// Whether the machine is ready (`/readyz`): a cell member whose node
    /// loop runs.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.phase() == Phase::Serving
    }

    /// The machine reached `phase`.
    pub(crate) fn set_phase(&self, phase: Phase) {
        self.0.phase.store(phase.to_u8(), Ordering::Release);
        if let Some(families) = &self.0.families {
            families.phase.set(f64::from(phase.to_u8()));
        }
        assert_eq!(self.phase(), phase, "a phase set is the phase read");
    }

    /// Journal `id`'s handles, labelled with the names `names` gives at
    /// its first call; `None` when off.
    pub(crate) fn journal(
        &self,
        id: JournalIdentifier,
        names: impl FnOnce() -> (String, String),
    ) -> Option<JournalMetrics> {
        let families = self.0.families.as_ref()?;
        let mut journals = self.journals();
        let handles = journals.entry(id).or_insert_with(|| {
            let (tenant, journal) = names();
            families.journal(&tenant, &journal)
        });
        Some(handles.clone())
    }

    /// Journal `id`'s handles, if a call already labelled them.
    fn known(&self, id: JournalIdentifier) -> Option<JournalMetrics> {
        self.0.families.as_ref()?;
        self.journals().get(&id).cloned()
    }

    fn journals(&self) -> std::sync::MutexGuard<'_, BTreeMap<JournalIdentifier, JournalMetrics>> {
        self.0
            .journals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// A batch of journal `id` settled in `took`: its fold applied up to
    /// `applied`, and `chosen` is chosen.
    pub(crate) fn settled(&self, id: JournalIdentifier, applied: u64, chosen: u64, took: Duration) {
        let Some(families) = &self.0.families else {
            return;
        };
        families.settle.observe_duration(took);
        if let Some(journal) = self.known(id) {
            journal.applied.set(slot_f64(applied));
            journal.chosen.set(slot_f64(chosen));
        }
    }

    /// The machine published a busyness window (#424).
    pub(crate) fn busyness(&self, busyness: &Busyness) {
        let Some(families) = &self.0.families else {
            return;
        };
        families.cpu_cores.set(busyness.cpu_cores);
        families.machine_cpu.set(busyness.machine_cpu);
        if let Some(disk) = &busyness.disk {
            families.disk_busy.set(disk.busy);
            families.disk_queue.set(slot_f64(disk.queue_depth));
        }
    }

    /// What this machine counted per journal, in journal order; empty when
    /// off.
    #[must_use]
    pub fn usage(&self) -> Vec<JournalUsage> {
        self.journals()
            .iter()
            .map(|(id, handles)| JournalUsage {
                journal: *id,
                calls: handles.calls.clone().map(|counter| counter.get()),
                written_bytes: handles.written.get(),
                read_bytes: handles.read.get(),
            })
            .collect()
    }
}

/// What one source counted over every journal: the sums of
/// `paros_calls_total` per call and of the two byte counters, read from the
/// registry (#366 (metrics)). The simulation's oracle reads it from each
/// machine's source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    /// Calls per [`CallKind::ALL`].
    pub calls: [u64; 4],
    /// `paros_written_bytes_total`, summed.
    pub written_bytes: u64,
    /// `paros_read_bytes_total`, summed.
    pub read_bytes: u64,
}

impl Totals {
    /// What `source` counted; zero for a source paros never recorded on.
    #[must_use]
    pub fn of(source: &PrometheusSource) -> Self {
        let mut totals = Self::default();
        for family in source.registry().gather() {
            for metric in family.get_metric() {
                let value = count_of(metric.get_counter().get_value());
                match family.name() {
                    "paros_calls_total" => {
                        let call = metric
                            .get_label()
                            .iter()
                            .find(|pair| pair.name() == "call")
                            .and_then(|pair| CallKind::of_label(pair.value()));
                        if let Some(call) = call {
                            totals.calls[call.index()] += value;
                        }
                    }
                    "paros_written_bytes_total" => totals.written_bytes += value,
                    "paros_read_bytes_total" => totals.read_bytes += value,
                    _ => {}
                }
            }
        }
        totals
    }

    /// Count a call of kind `call`.
    pub fn received(&mut self, call: CallKind) {
        self.calls[call.index()] += 1;
    }

    /// Add `other`'s counts.
    pub fn add(&mut self, other: &Totals) {
        for (mine, theirs) in self.calls.iter_mut().zip(other.calls) {
            *mine += theirs;
        }
        self.written_bytes += other.written_bytes;
        self.read_bytes += other.read_bytes;
    }

    /// Every call, of any kind.
    #[must_use]
    pub fn total_calls(&self) -> u64 {
        self.calls.iter().sum()
    }
}

/// A counter's value as a count: counters hold whole numbers.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "an integer counter's value is a whole, non-negative number"
)]
fn count_of(value: f64) -> u64 {
    assert!(value >= 0.0, "a counter is never negative");
    value as u64
}

impl JournalMetrics {
    /// A call of kind `call` reached the journal.
    pub(crate) fn received(&self, call: CallKind) {
        self.calls[call.index()].inc();
    }

    /// The answer to a call of kind `call` left after `took`, with
    /// `written` bytes accepted and `read` bytes served.
    pub(crate) fn answered(&self, call: CallKind, took: Duration, written: u64, read: u64) {
        self.latency[call.index()].observe_duration(took);
        if written > 0 {
            self.written.inc_by(written);
        }
        if read > 0 {
            self.read.inc_by(read);
        }
    }
}

/// A slot or count as a gauge value: exact up to 2^53, which a slot never
/// reaches in practice; a larger one is rounded, for display only.
#[allow(
    clippy::cast_precision_loss,
    reason = "a gauge is for display; slots stay far below 2^53"
)]
fn slot_f64(slot: u64) -> f64 {
    slot as f64
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("on", &self.is_on())
            .field("phase", &self.phase())
            .finish_non_exhaustive()
    }
}

/// Two handles are equal when they are the same machine's: metrics are the
/// machine's, never a value.
impl PartialEq for Metrics {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Metrics {}

#[cfg(test)]
mod tests;
