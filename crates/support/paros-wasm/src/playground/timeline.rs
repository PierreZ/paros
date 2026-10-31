//! The timeline: a read-only observer of the events the machines log. It
//! copies a fixed set of named events into the run's view; it never acts
//! on the run.

use std::cell::Cell;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{FieldValue, Invariant, SIM_FAULT_EVENT_NAME, TraceQuery};

use crate::playground::machine::machine_index;
use crate::playground::model::Event;

/// The events the page draws.
pub(crate) const NAMES: &[&str] = &[
    SIM_FAULT_EVENT_NAME,
    "operator_step",
    "machine_formatted",
    "cell_init_adopts_a_plan",
    "cell_initialized",
    "matchmaking_started",
    "matchmaking_completed",
    "machine_starting",
    "machine_serving",
    "machine_admitted",
    "machine_serving_admitted_journals",
    "leader_elected",
    "leadership_resigned",
    "leader_quorum_lost",
    "election_gap_filled",
    "value_chosen",
    "msg_sent",
    "journal_started",
    "client_call",
    "client_answer",
];

/// The recorded events, shared with the run's caller.
pub(crate) type Recorded = Arc<Mutex<Vec<Event>>>;

pub(crate) struct Timeline {
    recorded: Recorded,
    cursors: Vec<Cell<usize>>,
}

impl Timeline {
    pub(crate) fn new(recorded: Recorded) -> Self {
        Self {
            recorded,
            cursors: NAMES.iter().map(|_| Cell::new(0)).collect(),
        }
    }
}

fn text(value: &FieldValue) -> String {
    match value {
        FieldValue::Bool(v) => v.to_string(),
        FieldValue::I64(v) => v.to_string(),
        FieldValue::U64(v) => v.to_string(),
        FieldValue::F64(v) => v.to_string(),
        FieldValue::Str(v) => v.clone(),
    }
}

impl Invariant for Timeline {
    fn name(&self) -> &'static str {
        "sandbox_timeline"
    }

    fn observe(&self, query: &dyn TraceQuery, _sim_time_ms: u64) {
        let mut fresh = Vec::new();
        for (name, cursor) in NAMES.iter().zip(&self.cursors) {
            for event in query.since(name, cursor) {
                fresh.push((
                    event.seq,
                    Event {
                        time_ms: event.time_ms,
                        machine: machine_index(&event.source),
                        name: event.name.clone(),
                        fields: event
                            .fields
                            .iter()
                            .map(|(k, v)| (k.clone(), text(v)))
                            .collect(),
                    },
                ));
            }
        }
        fresh.sort_by_key(|(seq, _)| *seq);
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(fresh.into_iter().map(|(_, event)| event));
    }

    fn reset(&mut self) {
        self.cursors.iter().for_each(|c| c.set(0));
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}
