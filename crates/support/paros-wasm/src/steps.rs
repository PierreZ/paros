//! The shape of a step-through trace, shared by the two tools (#811
//! (Paxos flavor pages), #1365 (reshape and seal step-through)): the JSON
//! that `website/site/public/steps.js` and `story.js` draw.
//!
//! A trace is a recorded run of real code: the Paxos pages record
//! `paros-consensus` (`crate::paxos::trace`), the Segments page records the
//! journal roles of `paros-journal` (`crate::journal::seal`). Both fill the
//! same types, so the page draws both with the same script. This module holds
//! the types and the checks that do not depend on the code a run records:
//! the beats of a story ([`story`]), the stages of the steps
//! ([`fill_stages`]) and the assembly of a page ([`assemble`]).

pub mod story;

use serde::{Deserialize, Serialize};

pub use story::{Beat, Choice, Flight, NodeState, Question, Vote};

/// One field of a message: `ballot` = `1.0 (round 1, node 0)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    /// The field's name, in plain words: `ballot`, `slot`, `value`.
    pub name: String,
    /// Its value, as a reader reads it.
    pub value: String,
}

/// The group of steps a step belongs to: "Phase 1 · ballot 1.5".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stage {
    /// A short name, e.g. `Phase 1 · ballot 1.5`. Two steps with the same
    /// title next to each other are one group.
    pub title: String,
    /// One sentence on what the group does.
    pub about: String,
}

/// The trace of one page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    /// The page: the file name under `website/site/src/traces/`.
    pub page: String,
    /// One lane per party, in the order the diagram draws them.
    pub lanes: Vec<Lane>,
    /// The base run first, then the "what if" runs.
    pub scenarios: Vec<Scenario>,
    /// The grid the acceptors sit in, when the configuration is a grid
    /// (#947 (quorum and log in the step-through)): the page draws the
    /// acceptors as rows and columns and lights the row or column that
    /// answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid: Option<Grid>,
}

/// The shape of a grid quorum system.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grid {
    /// How many rows: the acceptors of one column.
    pub rows: u64,
    /// How many columns. A full column votes in Phase 2, and one acceptor
    /// of every column answers Phase 1.
    pub cols: u64,
}

/// Where one acceptor sits in a grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cell {
    /// Its row.
    pub row: u64,
    /// Its column.
    pub column: u64,
}

/// The answers a proposer or a leader holds for one phase of one ballot
/// (#947 (quorum and log in the step-through)): who answered, how many the
/// phase needs, and whether `paros-consensus` says the phase is done.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tally {
    /// `promises` (Phase 1) or `votes` (Phase 2).
    pub what: String,
    /// The lane that counts the answers.
    pub at: String,
    /// The ballot.
    pub ballot: String,
    /// The slot of a Phase 2 vote in a log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u64>,
    /// The lanes that answered, in lane order. A node counts its own answer.
    pub members: Vec<String>,
    /// What the phase needs, in words: `2 of 3`, `one acceptor of every column`.
    pub need: String,
    /// Whether the core says the phase is done: the node leads at this
    /// ballot (Phase 1), or the value is chosen (Phase 2).
    pub quorum: bool,
    /// On a grid, the column of the slot when its votes choose it (Phase 2),
    /// as the core's quorum predicate says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<Line>,
}

/// One row or one column of a grid.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Line {
    /// `row` or `column`.
    pub axis: String,
    /// Which one, from 0.
    pub index: u64,
}

/// One slot of a node's log, for the log strip of a card.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogCell {
    /// The slot number.
    pub slot: u64,
    /// The value, or `no-op` for a slot that a new leader filled.
    pub value: String,
    /// `accepted` (a vote) or `chosen`.
    pub state: String,
    /// Whether the slot holds a control command (a no-op, a truncate).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub control: bool,
}

/// One column of the diagram: a node or a matchmaker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lane {
    /// `n<id>` for a node, `m<id>` for a matchmaker.
    pub id: String,
    /// The column's header.
    pub label: String,
    /// What the party is: `proposer`, `acceptor`, `node` or `matchmaker`.
    pub role: String,
    /// Where the party sits in the grid, when the acceptors form one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell: Option<Cell>,
}

/// One run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scenario {
    /// A stable id, e.g. `base` or `acceptor-down`.
    pub id: String,
    /// The button's text.
    pub label: String,
    /// The first step where this run differs from the base run.
    pub fork: usize,
    /// The state of each lane before the first step, in lane order.
    pub start: Vec<Vec<String>>,
    /// The log of each lane before the first step, in lane order. Empty in
    /// a world of one decree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub start_logs: Vec<Vec<LogCell>>,
    /// The typed state of each lane before the first step, in lane order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub start_nodes: Vec<NodeState>,
    /// The messages on the wire before the first step.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub start_wire: Vec<Flight>,
    /// The steps, in order.
    pub steps: Vec<Step>,
    /// The story of the run: one screen per beat, in order. Empty for a run
    /// that has no story yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub beats: Vec<Beat>,
    /// The lanes of this run, when its world is not the page's: a run on
    /// another level (five nodes on a page of six). `None`: the page's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lanes: Option<Vec<Lane>>,
    /// The grid of this run, when its lanes are its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid: Option<Grid>,
    /// The chapters of the run (#944 (matchmaker runs)): each event (an
    /// election, a write, a crash, a reconfiguration) opens one, so a reader
    /// can jump to it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chapters: Vec<Chapter>,
    /// What the run ends with, read off the world.
    pub outcome: String,
}

/// One chapter of a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chapter {
    /// The event that opens it: "the client writes alpha".
    pub title: String,
    /// The number of the event's step: a jump shows the event as the
    /// current step.
    pub at: usize,
}

/// One step of a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    /// `deliver`, `drop` or `event`.
    pub kind: String,
    /// The sending lane (a message step).
    pub from: Option<String>,
    /// The receiving lane (a message step).
    pub to: Option<String>,
    /// The message (`Prepare 1.5`) or the event (`a3 is down`).
    pub label: String,
    /// The render family of the message: `prepare`, `promise`, `accept`, ….
    pub phase: Option<String>,
    /// Whether the message answers another one.
    pub reply: bool,
    /// The group of steps this step is in: "Phase 1 · ballot 1.5".
    pub stage: Option<Stage>,
    /// One short sentence on what this step did, in plain words.
    pub says: String,
    /// What the engine says this step did, in full: the details.
    pub notes: Vec<String>,
    /// What the message carries: its important fields. Empty for an event.
    pub fields: Vec<Field>,
    /// The message as its sender would say it ("I want to be the leader
    /// with ballot 1.0."). Empty for an event.
    pub voice: String,
    /// The state of each lane after the step, in lane order: a few short
    /// lines per lane ("promise 1.5", "vote alpha @ 1.5").
    pub state: Vec<Vec<String>>,
    /// What is chosen after the step: `alpha`, or `slot 0 = alpha`.
    pub chosen: Vec<String>,
    /// The answers counted so far for the phase this step answers in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tally: Option<Tally>,
    /// The log of each lane after the step, in lane order. Empty in a world
    /// of one decree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<Vec<LogCell>>,
    /// Set when the run broke a rule and two values are chosen for one slot
    /// (#946 (break-the-rule runs)): what the engine saw, in words.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broken: Option<String>,
    /// The id of the message this step delivers or drops: a [`Flight::id`]
    /// of the step before. `None` for an event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<u64>,
    /// The messages on the wire after the step, oldest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wire: Vec<Flight>,
    /// The typed state of each lane after the step, in lane order, with what
    /// the step wrote on each disk.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<NodeState>,
}

/// Build a page's trace on `lanes`: the base run first. A run on the page's
/// own lanes draws them; the fork of every other run is its first step that
/// differs from the base run.
#[must_use]
pub fn assemble(
    page: &str,
    lanes: Vec<Lane>,
    grid: Option<Grid>,
    scenarios: Vec<Scenario>,
) -> Trace {
    story::check_choices(page, &scenarios);
    let mut scenarios = scenarios;
    // A run on the page's own world draws the page's lanes.
    for run in &mut scenarios {
        if run.lanes.as_ref() == Some(&lanes) {
            run.lanes = None;
            run.grid = None;
        }
    }
    if let Some((base, rest)) = scenarios.split_first_mut() {
        for run in rest {
            run.fork = run
                .steps
                .iter()
                .zip(&base.steps)
                .position(|(a, b)| a != b)
                .unwrap_or_else(|| run.steps.len().min(base.steps.len()));
        }
    }
    Trace {
        page: page.to_string(),
        lanes,
        scenarios,
        grid,
    }
}

/// Give every step a stage: a message without its own continues the stage
/// before it, and an event opens the stage after it.
pub fn fill_stages(stages: &mut [Option<Stage>]) {
    let mut last: Option<Stage> = None;
    let mut pending = Vec::new();
    for i in 0..stages.len() {
        match stages[i].clone() {
            Some(stage) => {
                for j in pending.drain(..) {
                    stages[j] = Some(stage.clone());
                }
                last = Some(stage);
            }
            None => pending.push(i),
        }
    }
    // Steps at the end with no stage after them keep the one before.
    for j in pending {
        stages[j].clone_from(&last);
    }
}
