//! **Metrics** (#366, `docs/architecture/03-6-status.md` §3.6): what a machine counts
//! about the calls it serves, for an operator, never for a decision.
//!
//! The library records in a plain `prometheus` registry, one per
//! [`Metrics::on`], provider-free and wasm-safe. `parosd` serves it as
//! Prometheus text (`/metrics`). The simulation keeps each machine's
//! `Metrics` for its address and hands it back at a reboot, so a reboot
//! finds its counters again, and reads the counters from it
//! ([`Totals::of`]).
//!
//! | metric | kind | labels | where |
//! |---|---|---|---|
//! | `paros_calls_total` | counter | `call`, journal | a data-plane call reaches a journal this machine runs |
//! | `paros_call_answers_total` | counter | `call`, `outcome`, journal | the call's answer leaves (a dropped answer is not counted) |
//! | `paros_call_answer_seconds_total` | counter | `call`, journal | the same answer: the time from the call's arrival |
//! | `paros_written_bytes_total` | counter | journal | an accepted `Write` is answered |
//! | `paros_read_bytes_total` | counter | journal | a `Read` page is answered |
//! | `paros_applied_slot`, `paros_chosen_slot` | gauge | journal | every settled batch; the apply lag is their difference |
//! | `paros_settles_total`, `paros_settle_seconds_total` | counter | | a batch persisted (fsync included), sent and applied |
//! | `paros_machine_*` | gauge | | the last busyness window (#424) |
//! | `paros_machine_load_windows_total` | counter | | a busyness window closes: the heartbeat |
//! | `paros_machine_phase` | gauge | | 0 starting, 1 idle, 2 serving |
//!
//! "journal" is four labels: `tenant` and `journal`, the names, and
//! `tenant_id` and `journal_id`, the ids in hex. A journal whose names the
//! machine does not know is named by its ids in hex. Every label value is
//! bounded: a journal this machine runs, a call, an outcome.
//!
//! There is no histogram: a time is a seconds counter beside its count, so
//! `rate(seconds) / rate(count)` is the mean over any window, and a single
//! slow call is found in the traces. A call received and not answered is
//! `paros_calls_total` less the answers. The outcomes are [`CallOutcome`]: the
//! refusals and the calls this machine did not serve are counted beside
//! the successes.
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

use paros_core::JournalIdentifier;
use prometheus::{
    Counter, CounterVec, Gauge, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

use crate::load::Busyness;

/// The labels that name a journal: its names, then its ids in hex.
const JOURNAL_LABELS: [&str; 4] = ["tenant", "journal", "tenant_id", "journal_id"];

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

/// How a call was answered, the `outcome` label: every success, refusal
/// and call this machine did not serve has its own value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CallOutcome {
    /// Served: a write accepted, a page read, a truncation applied, a
    /// leader set.
    Ok,
    /// A retry of a write accepted before: acked, nothing moved.
    Duplicate,
    /// Judged and refused: a stale writer or a lost compare-and-set.
    Refused,
    /// Refused: shaped for the other writer mode.
    WrongMode,
    /// The position asked for is below the journal's first position.
    Truncated,
    /// Refused at the edge: the batch is over this machine's limits.
    TooLarge,
    /// Refused at the edge: the leader's acceptors fall behind (#360
    /// (disk full)).
    Busy,
    /// Sent to the leader.
    Redirected,
    /// This machine does not run the journal (any more).
    UnknownJournal,
    /// No verdict: the machine could not serve the call in time.
    Unserved,
}

impl CallOutcome {
    /// Every outcome, in label order.
    pub const ALL: [CallOutcome; 10] = [
        CallOutcome::Ok,
        CallOutcome::Duplicate,
        CallOutcome::Refused,
        CallOutcome::WrongMode,
        CallOutcome::Truncated,
        CallOutcome::TooLarge,
        CallOutcome::Busy,
        CallOutcome::Redirected,
        CallOutcome::UnknownJournal,
        CallOutcome::Unserved,
    ];

    /// The stable `outcome` label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            CallOutcome::Ok => "ok",
            CallOutcome::Duplicate => "duplicate",
            CallOutcome::Refused => "refused",
            CallOutcome::WrongMode => "wrong_mode",
            CallOutcome::Truncated => "truncated",
            CallOutcome::TooLarge => "too_large",
            CallOutcome::Busy => "busy",
            CallOutcome::Redirected => "redirected",
            CallOutcome::UnknownJournal => "unknown_journal",
            CallOutcome::Unserved => "unserved",
        }
    }

    /// The outcome a label names.
    #[must_use]
    pub fn of_label(label: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|outcome| outcome.label() == label)
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

/// The registered families of one registry.
struct Families {
    registry: Registry,
    calls: IntCounterVec,
    answers: IntCounterVec,
    answer_seconds: CounterVec,
    written: IntCounterVec,
    read: IntCounterVec,
    applied: IntGaugeVec,
    chosen: IntGaugeVec,
    machine: MachineFamilies,
}

/// The families with no label: the machine's own.
struct MachineFamilies {
    settles: IntCounter,
    settle_seconds: Counter,
    phase: IntGauge,
    windows: IntCounter,
    cpu_cores: Gauge,
    machine_cpu: Gauge,
    disk_busy: Gauge,
    disk_queue: IntGauge,
    scrub_records: IntCounter,
    scrub_bytes: IntCounter,
    scrub_damaged: IntCounter,
    scrub_repaired: IntCounter,
}

/// One journal's handles, labelled once with its names and ids.
#[derive(Clone)]
pub(crate) struct JournalMetrics {
    /// The journal's four label values, in [`JOURNAL_LABELS`] order.
    labels: Arc<[String; 4]>,
    /// The answers family: a series is created at its first outcome.
    answers: IntCounterVec,
    calls: [IntCounter; 4],
    answer_seconds: [Counter; 4],
    written: IntCounter,
    read: IntCounter,
    applied: IntGauge,
    chosen: IntGauge,
}

/// A family the registry refuses is a bug here: the names and labels are
/// constants, and each registry holds paros's families once.
fn registered<T>(family: prometheus::Result<T>) -> T {
    family.unwrap_or_else(|error| unreachable!("a paros metric registers: {error}"))
}

/// `metric`, registered in `registry`.
fn register<T: prometheus::core::Collector + Clone + 'static>(registry: &Registry, metric: T) -> T {
    registered(registry.register(Box::new(metric.clone())));
    metric
}

/// `labels` with the journal's labels after them.
fn with_journal<const N: usize>(labels: [&'static str; N]) -> Vec<&'static str> {
    labels.into_iter().chain(JOURNAL_LABELS).collect()
}

impl Families {
    fn register(registry: Registry) -> Self {
        let counters = |name: &str, help: &str, labels: &[&str]| {
            register(
                &registry,
                registered(IntCounterVec::new(Opts::new(name, help), labels)),
            )
        };
        let slots = |name: &str, help: &str| {
            register(
                &registry,
                registered(IntGaugeVec::new(Opts::new(name, help), &JOURNAL_LABELS)),
            )
        };
        let calls = counters(
            "paros_calls_total",
            "Data-plane calls that reached a journal this machine runs.",
            &with_journal(["call"]),
        );
        let answers = counters(
            "paros_call_answers_total",
            "Answers to data-plane calls, by outcome.",
            &with_journal(["call", "outcome"]),
        );
        let answer_seconds = register(
            &registry,
            registered(CounterVec::new(
                Opts::new(
                    "paros_call_answer_seconds_total",
                    "Seconds from each answered call's arrival to its answer, summed.",
                ),
                &with_journal(["call"]),
            )),
        );
        let written = counters(
            "paros_written_bytes_total",
            "Bytes of records in the writes this machine accepted.",
            &JOURNAL_LABELS,
        );
        let read = counters(
            "paros_read_bytes_total",
            "Bytes of records in the read pages this machine answered.",
            &JOURNAL_LABELS,
        );
        let applied = slots(
            "paros_applied_slot",
            "The slot this machine's journal fold applied up to.",
        );
        let chosen = slots(
            "paros_chosen_slot",
            "The slot this machine knows chosen up to.",
        );
        Self {
            calls,
            answers,
            answer_seconds,
            written,
            read,
            applied,
            chosen,
            machine: MachineFamilies::register(&registry),
            registry,
        }
    }

    /// Every registered family, in registration order.
    fn collectors(&self) -> [&dyn prometheus::core::Collector; 19] {
        let machine = &self.machine;
        [
            &self.calls,
            &self.answers,
            &self.answer_seconds,
            &self.written,
            &self.read,
            &self.applied,
            &self.chosen,
            &machine.settles,
            &machine.settle_seconds,
            &machine.phase,
            &machine.windows,
            &machine.cpu_cores,
            &machine.machine_cpu,
            &machine.disk_busy,
            &machine.disk_queue,
            &machine.scrub_records,
            &machine.scrub_bytes,
            &machine.scrub_damaged,
            &machine.scrub_repaired,
        ]
    }

    fn journal(&self, id: JournalIdentifier, tenant: String, journal: String) -> JournalMetrics {
        let labels = Arc::new([
            tenant,
            journal,
            format!("{:x}", id.tenant.0),
            format!("{:x}", id.journal.0),
        ]);
        let named = labels.each_ref().map(String::as_str);
        let [tenant, journal, tenant_id, journal_id] = named;
        let per_call = |call: CallKind| {
            self.calls
                .with_label_values(&[call.label(), tenant, journal, tenant_id, journal_id])
        };
        let seconds = |call: CallKind| {
            self.answer_seconds.with_label_values(&[
                call.label(),
                tenant,
                journal,
                tenant_id,
                journal_id,
            ])
        };
        JournalMetrics {
            calls: CallKind::ALL.map(per_call),
            answer_seconds: CallKind::ALL.map(seconds),
            written: self.written.with_label_values(&named),
            read: self.read.with_label_values(&named),
            applied: self.applied.with_label_values(&named),
            chosen: self.chosen.with_label_values(&named),
            answers: self.answers.clone(),
            labels,
        }
    }
}

impl MachineFamilies {
    fn register(registry: &Registry) -> Self {
        let counter =
            |name: &str, help: &str| register(registry, registered(IntCounter::new(name, help)));
        let gauge = |name: &str, help: &str| register(registry, registered(Gauge::new(name, help)));
        let settles = counter(
            "paros_settles_total",
            "Batches this machine persisted (fsync included), sent and applied.",
        );
        let settle_seconds = register(
            registry,
            registered(Counter::new(
                "paros_settle_seconds_total",
                "Seconds the settled batches took, summed.",
            )),
        );
        let phase = register(
            registry,
            registered(IntGauge::new(
                "paros_machine_phase",
                "The machine's phase: 0 starting, 1 idle, 2 serving.",
            )),
        );
        let windows = counter(
            "paros_machine_load_windows_total",
            "Busyness windows the machine closed: it stops when sampling stops.",
        );
        let cpu_cores = gauge(
            "paros_machine_cpu_cores",
            "CPU cores the process used over the last window.",
        );
        let machine_cpu = gauge(
            "paros_machine_cpu_ratio",
            "The whole machine's CPU use over the last window, from 0 to 1.",
        );
        let disk_busy = gauge(
            "paros_machine_disk_busy_ratio",
            "The share of the last window the data disk was busy, from 0 to 1.",
        );
        let disk_queue = register(
            registry,
            registered(IntGauge::new(
                "paros_machine_disk_queue_depth",
                "I/Os in flight on the data disk at the last window's end.",
            )),
        );
        // The scrub's own reads (#362 (background scrubbing)).
        let scrub_records = counter(
            "paros_scrub_records_total",
            "Records this machine read back and checked for a scrub.",
        );
        let scrub_bytes = counter(
            "paros_scrub_bytes_total",
            "Payload bytes this machine read back for a scrub.",
        );
        let scrub_damaged = counter(
            "paros_scrub_damaged_total",
            "Records a scrub found damaged on this machine's disk.",
        );
        let scrub_repaired = counter(
            "paros_scrub_repaired_total",
            "Damaged records this machine wrote again from its own copy.",
        );
        Self {
            settles,
            settle_seconds,
            phase,
            windows,
            cpu_cores,
            machine_cpu,
            disk_busy,
            disk_queue,
            scrub_records,
            scrub_bytes,
            scrub_damaged,
            scrub_repaired,
        }
    }
}

impl Metrics {
    /// Metrics that record nothing: the default.
    #[must_use]
    pub fn off() -> Self {
        Self::default()
    }

    /// Metrics recorded in a registry of their own. A machine started
    /// again with the same `Metrics` (the simulation's reboot) keeps
    /// counting from where it stopped.
    #[must_use]
    pub fn on() -> Self {
        Self(Arc::new(Shared {
            phase: AtomicU8::new(Phase::Starting.to_u8()),
            families: Some(Families::register(Registry::new())),
            journals: Mutex::new(BTreeMap::new()),
        }))
    }

    /// Whether these metrics record.
    #[must_use]
    pub fn is_on(&self) -> bool {
        self.0.families.is_some()
    }

    /// The registry they record in, if any.
    #[must_use]
    pub fn registry(&self) -> Option<&Registry> {
        self.0.families.as_ref().map(|families| &families.registry)
    }

    /// Every family these metrics record, series or not yet; empty when
    /// off.
    #[must_use]
    pub fn families(&self) -> Vec<Family> {
        let Some(families) = &self.0.families else {
            return Vec::new();
        };
        let all = families
            .collectors()
            .into_iter()
            .flat_map(prometheus::core::Collector::collect);
        all.map(|family| Family {
            name: family.name().to_string(),
            help: family.help().to_string(),
            kind: match family.get_field_type() {
                prometheus::proto::MetricType::COUNTER => FamilyKind::Counter,
                prometheus::proto::MetricType::GAUGE => FamilyKind::Gauge,
                other => unreachable!("paros records no {other:?}"),
            },
        })
        .collect()
    }

    /// Every metric of the registry in the Prometheus text format; empty
    /// when off.
    #[must_use]
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let Some(families) = &self.0.families else {
            return String::new();
        };
        let gathered = families.registry.gather();
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
            families.machine.phase.set(i64::from(phase.to_u8()));
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
            families.journal(id, tenant, journal)
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
        families.machine.settles.inc();
        families.machine.settle_seconds.inc_by(took.as_secs_f64());
        if let Some(journal) = self.known(id) {
            journal.applied.set(gauge_i64(applied));
            journal.chosen.set(gauge_i64(chosen));
        }
    }

    /// The machine answered a scrub page (#362 (background scrubbing)) of
    /// journal `_id`: what it read, found damaged and repaired.
    pub(crate) fn scrubbed(&self, _id: JournalIdentifier, page: &crate::rpc::ScrubReply) {
        let Some(families) = &self.0.families else {
            return;
        };
        let machine = &families.machine;
        machine.scrub_records.inc_by(page.slots.len() as u64);
        machine.scrub_bytes.inc_by(page.bytes);
        let damaged = page.slots.iter().filter(|s| s.damaged).count() as u64;
        let repaired = page.slots.iter().filter(|s| s.repaired).count() as u64;
        machine.scrub_damaged.inc_by(damaged);
        machine.scrub_repaired.inc_by(repaired);
    }

    /// The machine published a busyness window (#424).
    pub(crate) fn busyness(&self, busyness: &Busyness) {
        let Some(families) = &self.0.families else {
            return;
        };
        families.machine.windows.inc();
        families.machine.cpu_cores.set(busyness.cpu_cores);
        families.machine.machine_cpu.set(busyness.machine_cpu);
        if let Some(disk) = &busyness.disk {
            families.machine.disk_busy.set(disk.busy);
            families.machine.disk_queue.set(gauge_i64(disk.queue_depth));
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

/// A metric family's kind: paros records counters and gauges only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FamilyKind {
    /// Only goes up while the process lives.
    Counter,
    /// Any value: the latest one.
    Gauge,
}

/// One metric family a [`Metrics`] records: what an exporter other than
/// the Prometheus text (`parosd`'s OTLP push) registers once, then reads
/// from [`Metrics::registry`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Family {
    /// The Prometheus name, e.g. `paros_calls_total`.
    pub name: String,
    /// What it counts.
    pub help: String,
    /// Its kind.
    pub kind: FamilyKind,
}

/// What one machine counted over every journal: the sums of
/// `paros_calls_total` and `paros_call_answers_total` per call and of the
/// two byte counters, read from its registry (#366 (metrics)). The
/// simulation's oracle reads it from each machine's metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    /// Calls per [`CallKind::ALL`].
    pub calls: [u64; 4],
    /// Answers per [`CallKind::ALL`], of every outcome.
    pub answers: [u64; 4],
    /// `paros_written_bytes_total`, summed.
    pub written_bytes: u64,
    /// `paros_read_bytes_total`, summed.
    pub read_bytes: u64,
}

impl Totals {
    /// What `metrics` counted; zero when off.
    #[must_use]
    pub fn of(metrics: &Metrics) -> Self {
        let mut totals = Self::default();
        let Some(registry) = metrics.registry() else {
            return totals;
        };
        for family in registry.gather() {
            for metric in family.get_metric() {
                let value = count_of(metric.get_counter().get_value());
                let call = || {
                    metric
                        .get_label()
                        .iter()
                        .find(|pair| pair.name() == "call")
                        .and_then(|pair| CallKind::of_label(pair.value()))
                        .unwrap_or_else(|| unreachable!("a call series names its call"))
                };
                match family.name() {
                    "paros_calls_total" => totals.calls[call().index()] += value,
                    "paros_call_answers_total" => totals.answers[call().index()] += value,
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

    /// Count an answer to a call of kind `call`, `written` bytes accepted
    /// and `read` bytes served.
    pub fn answered(&mut self, call: CallKind, written: u64, read: u64) {
        self.answers[call.index()] += 1;
        self.written_bytes += written;
        self.read_bytes += read;
    }

    /// Add `other`'s counts.
    pub fn add(&mut self, other: &Totals) {
        for (mine, theirs) in self.calls.iter_mut().zip(other.calls) {
            *mine += theirs;
        }
        for (mine, theirs) in self.answers.iter_mut().zip(other.answers) {
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

    /// The answer to a call of kind `call` left after `took` with
    /// `outcome`, `written` bytes accepted and `read` bytes served.
    pub(crate) fn answered(
        &self,
        (call, outcome): (CallKind, CallOutcome),
        took: Duration,
        written: u64,
        read: u64,
    ) {
        let [tenant, journal, tenant_id, journal_id] = self.labels.each_ref().map(String::as_str);
        self.answers
            .with_label_values(&[
                call.label(),
                outcome.label(),
                tenant,
                journal,
                tenant_id,
                journal_id,
            ])
            .inc();
        self.answer_seconds[call.index()].inc_by(took.as_secs_f64());
        if written > 0 {
            self.written.inc_by(written);
        }
        if read > 0 {
            self.read.inc_by(read);
        }
    }
}

/// A slot or count as an integer gauge value; one past `i64::MAX`, which
/// no slot reaches, shows as `i64::MAX`.
fn gauge_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
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
