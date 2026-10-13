//! The sandbox's data: what a run is made of (a [`Config`] and an action
//! log of [`Action`]s) and what it shows ([`RunView`]). Plain serde types,
//! the one contract the browser reads.

use serde::{Deserialize, Serialize};

/// The shape of a universe: everything a run needs beside its actions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The seed of every simulated draw: network latency, disk timing and
    /// every random choice paros makes.
    pub seed: u64,
    /// How many machines run `parosd` (each starts idle, with an empty disk).
    pub machines: usize,
    /// How many of them `init` lists as founding members: machines
    /// `0..founders`.
    pub founders: usize,
    /// How long the run lasts, in simulated milliseconds.
    pub run_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 1,
            machines: 4,
            founders: 3,
            run_ms: 30_000,
        }
    }
}

/// One step of the action log, at a simulated time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    /// When the step starts, in simulated milliseconds. An operator step
    /// starts no earlier than the previous operator step ends.
    pub at_ms: u64,
    /// What it does.
    pub op: Op,
}

/// What a step does. The operator steps call the same client and admin API
/// as `parosctl`; the fault steps act on the simulated world.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Op {
    /// `parosctl init` over the founding members: `cell init` (a
    /// single-decree Paxos over the members), then the fleet steps.
    Init,
    /// Append `value` to the sandbox's journal (created on first use, with
    /// its tenant): a Multi-Paxos write.
    Write {
        /// The record.
        value: String,
    },
    /// `parosctl cell add-machine` for an idle machine.
    Admit {
        /// The machine's index.
        machine: usize,
    },
    /// Crash a machine: its process stops, and its disk keeps only what it
    /// synced.
    Crash {
        /// The machine's index.
        machine: usize,
    },
    /// Start a crashed machine again on its disk.
    Restart {
        /// The machine's index.
        machine: usize,
    },
    /// Cut the network between two machines, both ways.
    Partition {
        /// One machine's index.
        a: usize,
        /// The other machine's index.
        b: usize,
    },
    /// Join two machines cut by a partition again.
    Heal {
        /// One machine's index.
        a: usize,
        /// The other machine's index.
        b: usize,
    },
}

impl Op {
    /// Whether the operator runs this step (the others are faults).
    #[must_use]
    pub fn is_operator(&self) -> bool {
        matches!(self, Op::Init | Op::Write { .. } | Op::Admit { .. })
    }
}

/// What an operator step came to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    /// The step's index in the action log.
    pub index: usize,
    /// When it started and ended, in simulated milliseconds.
    pub start_ms: u64,
    /// When it ended.
    pub end_ms: u64,
    /// Whether it did what it asked.
    pub ok: bool,
    /// What happened, for people.
    pub detail: String,
}

/// One event a machine or the simulator reported, in simulated time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// When it happened, in simulated milliseconds.
    pub time_ms: u64,
    /// The machine that reported it, or `None` for the simulator and the
    /// operator.
    pub machine: Option<usize>,
    /// Its name, as `paros` logs it (`value_chosen`, `msg_sent`, …).
    pub name: String,
    /// Its fields, as text.
    pub fields: Vec<(String, String)>,
}

/// Everything a run shows: replayable from its config and action log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunView {
    /// The config it ran.
    pub config: Config,
    /// The action log it ran.
    pub actions: Vec<Action>,
    /// What each operator step came to, in log order.
    pub steps: Vec<StepResult>,
    /// The events, in the order they happened.
    pub events: Vec<Event>,
    /// Why the run stopped early, when it did.
    pub error: Option<String>,
}
