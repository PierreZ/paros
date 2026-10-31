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
    /// How many machines run `parosd`. Each starts idle, with an empty
    /// disk; `init` forms the cell over the first ones.
    pub machines: usize,
    /// How long the run lasts, in simulated milliseconds.
    pub run_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 1,
            machines: 5,
            run_ms: 30_000,
        }
    }
}

/// One step of the action log, at a simulated time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    /// When the step starts, in simulated milliseconds. A step starts no
    /// earlier than the previous step ends.
    pub at_ms: u64,
    /// What it does.
    pub op: Op,
}

/// What a step does: the same client and admin API as `parosctl`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Op {
    /// `parosctl init` over machines `0..machines`: `cell init` (a
    /// single-decree Paxos over the members), then `universe init`. The
    /// other machines stay idle.
    Init {
        /// How many machines found the cell.
        machines: usize,
    },
    /// `parosctl tenant create`.
    CreateTenant {
        /// The tenant's name.
        tenant: String,
    },
    /// `parosctl journal create`: the tenant coordinator places it.
    CreateJournal {
        /// The tenant's name.
        tenant: String,
        /// The journal's name, inside the tenant.
        journal: String,
        /// Its durability (#523): `machines=F,partitions=P`,
        /// either part optional (`F=1`, `P=1` by default).
        #[serde(default = "default_durability")]
        mode: String,
    },
    /// Append `count` records to `journal` of `tenant`, as its one writer.
    Write {
        /// The tenant's name.
        tenant: String,
        /// The journal's name, inside the tenant.
        journal: String,
        /// How many records.
        count: u32,
    },
    /// Read `count` records of `journal` of `tenant` from its first
    /// position.
    Read {
        /// The tenant's name.
        tenant: String,
        /// The journal's name, inside the tenant.
        journal: String,
        /// How many records.
        count: u32,
    },
    /// Traffic: append `count` records to every journal the log created,
    /// all the journals at the same time, each through its one writer.
    Traffic {
        /// How many records each journal gets.
        count: u32,
    },
    /// `parosctl cell add-machine`: admit idle machine `machine` into
    /// `cell-1`. The cell registers it in its control journal, then tells
    /// it its cell with `Admit`.
    AddMachine {
        /// The machine's index.
        machine: usize,
    },
    /// Crash machine `machine`: moonpool kills its process and drops the
    /// writes its disk did not sync. It stays down until `restart`.
    Crash {
        /// The machine's index.
        machine: usize,
    },
    /// Start machine `machine` again on the disk it had. A machine that
    /// runs is killed and started again in place.
    Restart {
        /// The machine's index.
        machine: usize,
    },
    /// Cut machine `machine` from every other machine and from the client:
    /// moonpool drops each message to or from it until `heal`.
    Isolate {
        /// The machine's index.
        machine: usize,
    },
    /// Join machine `machine` to the network again.
    Heal {
        /// The machine's index.
        machine: usize,
    },
}

impl Op {
    /// The machine a fault names, when the step is a fault.
    #[must_use]
    pub fn fault_target(&self) -> Option<usize> {
        match self {
            Self::Crash { machine }
            | Self::Restart { machine }
            | Self::Isolate { machine }
            | Self::Heal { machine } => Some(*machine),
            _ => None,
        }
    }
}

fn default_durability() -> String {
    "machines=1".into()
}

/// What a step came to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    /// The step's index in the action log.
    pub index: usize,
    /// When it started, in simulated milliseconds.
    pub start_ms: u64,
    /// When it ended.
    pub end_ms: u64,
    /// Whether it did what it asked.
    pub ok: bool,
    /// What happened, for people.
    pub detail: String,
}

/// Where the universe's tenants and journals live after a step, as the
/// admin API reports it: the fleet directory, each tenant's control
/// journal, and an `Inspect` of every journal on every machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// The step it follows, by its index in the action log.
    pub step: usize,
    /// When it was read, in simulated milliseconds.
    pub time_ms: u64,
    /// Each machine's node id (its first six hex digits), by index; `None`
    /// for a machine outside the cell.
    pub machines: Vec<Option<String>>,
    /// Each machine's full node id, in decimal, as `msg_sent` names it
    /// (`node:<id>`); `None` outside the cell.
    pub nodes: Vec<Option<String>>,
    /// Each machine's zone (its failure domain), by index.
    pub zones: Vec<String>,
    /// The machines a `crash` holds down, by index.
    #[serde(default)]
    pub down: Vec<usize>,
    /// The machines an `isolate` cuts from the network, by index.
    #[serde(default)]
    pub isolated: Vec<usize>,
    /// The tenants, the internal ones first.
    pub tenants: Vec<TenantPlace>,
}

/// One tenant and its journals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantPlace {
    /// Its name.
    pub name: String,
    /// Its kind, its `paros/kind` tag (`universe`, `cell` or `users`,
    /// #575 (key:value tags)).
    pub kind: String,
    /// The cell it lives in (the cell's name).
    pub cell: String,
    /// Its journals, its control journal first.
    pub journals: Vec<JournalPlace>,
}

/// One journal and the machines that serve it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalPlace {
    /// Its name.
    pub name: String,
    /// Its identifier, `<tenant>:<journal>` in decimal, as `msg_sent` and
    /// `client_call` events name it.
    pub key: String,
    /// Its acceptors (the configuration in force), by machine index, in
    /// membership order: under a grid, row by row.
    pub members: Vec<usize>,
    /// The quorum system over them.
    pub quorum: Quorum,
    /// The machine that leads it, when one does.
    pub leader: Option<usize>,
    /// Its matchmaker set, by machine index (empty without matchmakers).
    pub matchmakers: Vec<usize>,
    /// The machines that serve it (answered `Inspect` for it).
    pub hosts: Vec<usize>,
    /// The highest position its leader knows chosen.
    pub chosen: Option<u64>,
    /// The acceptor state of each machine that answered `Inspect` for it,
    /// in machine order.
    #[serde(default)]
    pub acceptors: Vec<AcceptorView>,
}

/// A ballot: a round and the node that owns it (its first six hex digits).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BallotView {
    /// The round.
    pub round: u64,
    /// The node, as people read it.
    pub node: String,
}

/// One machine's acceptor and learner state for one journal, as its
/// `Inspect` reports it. A read only: the page shows it, and nothing acts on
/// it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptorView {
    /// The machine, by index.
    pub machine: usize,
    /// Whether the machine is an acceptor of the configuration in force.
    pub member: bool,
    /// The highest ballot it promised (Phase 1).
    pub promised: Option<BallotView>,
    /// The highest slot it holds a vote at, or stands in for one.
    pub watermark: Option<u64>,
    /// The highest slot it knows chosen.
    pub chosen: Option<u64>,
    /// Its compaction floor: the first slot it still holds.
    pub floor: u64,
    /// How many accepted records it holds.
    pub accepted: u64,
    /// The last accepted records (Phase 2), oldest first: slot and ballot.
    pub tail: Vec<(u64, BallotView)>,
}

/// The quorum system of a journal's configuration, as `Inspect` reports it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quorum {
    /// `majority`, `flexible` or `grid`.
    pub system: String,
    /// How many acceptors a Phase-1 quorum holds (elections and reads).
    pub phase1: u64,
    /// How many acceptors a Phase-2 quorum holds (writes).
    pub phase2: u64,
    /// Rows of a grid: each row is a Phase-1 quorum. 0 otherwise.
    pub rows: u64,
    /// Columns of a grid: each column is a Phase-2 quorum, and slot `s`
    /// goes to column `s % cols`. 0 otherwise.
    pub cols: u64,
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
    /// What each step came to, in log order.
    pub steps: Vec<StepResult>,
    /// Where the tenants and journals live, after each step.
    pub topology: Vec<Topology>,
    /// The events, in the order they happened.
    pub events: Vec<Event>,
    /// Why the run stopped early, when it did.
    pub error: Option<String>,
}
