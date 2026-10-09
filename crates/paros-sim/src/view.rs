//! One seed of the main campaign as data a web page can draw (#307).
//!
//! [`run_chain_seed_view`] runs the same builder as [`crate::run_chain_seed`]
//! and adds two observers: the audit's end-of-run digest and a recorder that
//! copies a few named trace events (the simulator's faults, the cell's
//! formation, leadership changes, chosen slots) into a timeline. Both only
//! observe. The run is the same run, draw for draw, so a page that shows a
//! seed shows what the native hunt runs.
//!
//! The result is plain data with no wall-clock value in it. The same seed
//! must give an equal [`SeedRun`] on every target, native and
//! `wasm32-unknown-unknown` alike; `paros-sim-web` proves this in CI.

use std::cell::Cell;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{Invariant, SIM_FAULT_EVENT_NAME, TraceQuery};

use crate::{DigestSink, RunConfigured, chain_builder};

/// The trace events a [`SeedRun`] keeps, by name. Every other event stays
/// out of the timeline. The list is a choice for readers, not an oracle.
pub const VIEW_EVENTS: [&str; 9] = [
    SIM_FAULT_EVENT_NAME,
    "cell_initialized",
    "cell_formed",
    "machine_serving",
    "election_gap_filled",
    "leadership_resigned",
    "leader_quorum_lost",
    "journal_quarantined",
    "value_chosen",
];

/// One recorded trace event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedEvent {
    /// The simulated time of the event, in milliseconds.
    pub time_ms: u64,
    /// The process or workload that emitted it (its IP), or `sim` for a
    /// fault the simulator injected.
    pub source: String,
    /// The event name: one of [`VIEW_EVENTS`].
    pub name: String,
    /// The event's main detail: the fault kind, the slot, or the cell.
    pub detail: Option<String>,
}

/// One seed of the main campaign, as a page draws it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedRun {
    /// The seed that was run.
    pub seed: u64,
    /// The always-assertion violations: empty when every oracle held.
    pub violations: Vec<String>,
    /// The error of a run that failed without a violation (a panic, a
    /// deadlock), if any.
    pub failure: Option<String>,
    /// The audit's end-of-run digest (see [`crate::chain_seed_digest`]);
    /// `None` when the workload never reached its check.
    pub digest: Option<u64>,
    /// The simulated time the run took, in milliseconds.
    pub simulated_ms: u64,
    /// The simulator events the run processed.
    pub steps: u64,
    /// The recorded events, in the order they were emitted.
    pub events: Vec<SeedEvent>,
}

impl SeedRun {
    /// True when every oracle held and the run finished.
    #[must_use]
    pub fn is_green(&self) -> bool {
        self.violations.is_empty() && self.failure.is_none()
    }
}

/// Run one seed of the main campaign and return it as a [`SeedRun`].
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_chain_seed_view(seed: u64) -> SeedRun {
    let sink: DigestSink = Arc::new(Mutex::new(None));
    let events = Arc::new(Mutex::new(Vec::new()));
    let report = chain_builder(Some(sink.clone()))
        .invariant(Recorder::new(events.clone()))
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run_configured();

    let (failure, simulated_ms, steps) = match report.individual_metrics.first() {
        Some(Ok(metrics)) => (
            None,
            u64::try_from(metrics.simulated_time.as_millis()).unwrap_or(u64::MAX),
            metrics.events_processed,
        ),
        Some(Err(error)) => (Some(error.to_string()), 0, 0),
        None => (Some("the seed did not run".to_owned()), 0, 0),
    };
    let mut events = std::mem::take(&mut *events.lock().unwrap_or_else(PoisonError::into_inner));
    events.sort_by_key(|(seq, _)| *seq);
    let digest = *sink.lock().unwrap_or_else(PoisonError::into_inner);
    SeedRun {
        seed,
        violations: report.assertion_violations,
        failure,
        digest,
        simulated_ms,
        steps,
        events: events.into_iter().map(|(_, event)| event).collect(),
    }
}

/// Copies the [`VIEW_EVENTS`] into a shared list, with their global
/// sequence number so the list can be put back in emission order.
struct Recorder {
    events: Arc<Mutex<Vec<(u64, SeedEvent)>>>,
    cursors: [Cell<usize>; VIEW_EVENTS.len()],
}

impl Recorder {
    fn new(events: Arc<Mutex<Vec<(u64, SeedEvent)>>>) -> Self {
        Self {
            events,
            cursors: Default::default(),
        }
    }
}

impl Invariant for Recorder {
    fn name(&self) -> &'static str {
        "seed_view_recorder"
    }

    fn observe(&self, query: &dyn TraceQuery, _sim_time_ms: u64) {
        let mut events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
        for (name, cursor) in VIEW_EVENTS.iter().zip(&self.cursors) {
            for event in query.since(name, cursor) {
                let detail = ["kind", "slot", "cell"].iter().find_map(|key| {
                    event
                        .str(key)
                        .map(str::to_owned)
                        .or_else(|| event.u64(key).map(|value| value.to_string()))
                });
                events.push((
                    event.seq,
                    SeedEvent {
                        time_ms: event.time_ms,
                        source: event.source,
                        name: event.name,
                        detail,
                    },
                ));
            }
        }
    }

    fn reset(&mut self) {
        self.cursors = Default::default();
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}
