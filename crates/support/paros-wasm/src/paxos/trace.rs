//! Step-through traces for the site's Paxos pages (#811 (Paxos flavor pages)).
//!
//! A trace is a recorded run of the real engine: a [`Game`] on a level's own
//! setup, driven by a short script that chooses messages by what they are and
//! answers every prompt with the answer `paros-core` gives. Each delivered,
//! dropped or copied message becomes one step of a sequence diagram, with the
//! narration the engine derived from that transition and the state of every
//! lane after it. Nothing on the page is drawn from a hand-written trace.
//!
//! A page has one trace: a base run and one or two "what if" runs. A "what
//! if" run shares its first steps with the base run and then does something
//! else (an acceptor goes down, a message is lost, a second proposer runs).
//! [`Scenario::fork`] is the first step where the two differ, so the page can
//! keep the reader's place when they switch.
//!
//! `tests/paxos_traces.rs` writes every trace to `website/site/src/traces/`
//! (committed, diffed in CI).

mod content;
mod pages;
mod words;

pub use content::Field;
pub use pages::{trace_of, traces};
pub use words::Stage;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use paros_core::membership::QuorumSystem;

use serde::{Deserialize, Serialize};

use crate::paxos::action::Action;
use crate::paxos::view::{
    AttemptView, MessageView, NodeFlavour, NodeView, PartyView, QuorumKindView, QuorumSystemView,
    RoleView, WorldView,
};
use crate::paxos::{Game, WorldKind};

/// How many steps one recording may take before it is called a loop.
const BUDGET: usize = 2048;

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
    /// How many rows. A full row answers Phase 1.
    pub rows: u64,
    /// How many columns. A full column votes in Phase 2.
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
/// phase needs, and whether `paros-core` says the phase is done.
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
    /// What the phase needs, in words: `2 of 3`, `a full row`.
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
    /// The steps, in order.
    pub steps: Vec<Step>,
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
}

/// Records one run of a level's setup.
pub struct Recorder {
    game: Game,
    start: Vec<Vec<String>>,
    start_logs: Vec<Vec<LogCell>>,
    steps: Vec<Step>,
    lanes: Vec<Lane>,
    grid: Option<Grid>,
    /// The answers each counter heard, by (counter, phase, ballot, slot).
    heard: BTreeMap<TallyKey, BTreeSet<u64>>,
}

/// Who counts, which phase, which ballot, which slot.
type TallyKey = (String, &'static str, String, Option<u64>);

impl Recorder {
    /// Start a run on the setup of `level_id`.
    ///
    /// # Panics
    ///
    /// When no level has that id.
    #[must_use]
    pub fn new(level_id: &str) -> Self {
        let game = Game::new(level_id).expect("a registered level");
        let world = game.view().world;
        let start = lane_state(&world);
        let start_logs = lane_logs(&world);
        Self {
            lanes: lanes(&world),
            grid: grid_of(&world),
            game,
            start,
            start_logs,
            steps: Vec::new(),
            heard: BTreeMap::new(),
        }
    }

    /// The world as it stands.
    #[must_use]
    pub fn world(&self) -> &WorldKind {
        self.game.world()
    }

    /// Everything in flight.
    #[must_use]
    pub fn wire(&self) -> Vec<MessageView> {
        self.game.view().world.wire
    }

    /// Play one action. A message action becomes a step; an answer to a
    /// prompt only moves the state of the step before it; any other action
    /// is an `event` step labelled `label`.
    ///
    /// # Panics
    ///
    /// When the engine refuses the action: a trace is a run that works.
    pub fn play(&mut self, action: Action, label: &str) -> &mut Self {
        // What the step draws is read before the action takes the message
        // off the wire.
        let carried = match &action {
            Action::Deliver { id } | Action::Duplicate { id, .. } | Action::Drop { id } => self
                .game
                .world()
                .wire()
                .iter()
                .find(|entry| entry.id == *id)
                .map(|entry| content::content(entry, log_world(&self.game.view().world))),
            _ => None,
        };
        let (kind, message, copy_to) = match &action {
            Action::Deliver { id } => ("deliver", self.in_flight(*id), None),
            Action::Duplicate { id, to } => ("deliver", self.in_flight(*id), *to),
            Action::Drop { id } => ("drop", self.in_flight(*id), None),
            _ => ("event", None, None),
        };
        let answer = matches!(action, Action::Answer { .. });
        let what = action.label();
        let before = self.game.view().world;
        self.game
            .act_freely(action)
            .unwrap_or_else(|err| panic!("the trace step {what} was refused: {err}"));
        let after = self.game.view().world;
        let state = lane_state(&after);
        if answer {
            // The answer to a prompt is the core's own answer; what the game
            // says about it is for its player. Only the state moves, and what
            // it changed is said with the step before it.
            let logs = lane_logs(&after);
            let chosen = self.chosen_of(&after);
            let broken = self.broken();
            if let Some(last) = self.steps.last_mut() {
                last.state = state;
                last.logs = logs;
                last.chosen = chosen;
                last.broken = broken;
                let more = words::says_event(&before, &after, "");
                if !more.is_empty() {
                    last.says.push(' ');
                    last.says.push_str(&more);
                }
            }
            return self;
        }
        let notes: Vec<String> = self
            .game
            .narration()
            .iter()
            .filter_map(|event| site_note(&event.text))
            .collect();
        let chosen = self.chosen_of(&after);
        let broken = self.broken();
        let logs = lane_logs(&after);
        let tally = match (&message, kind) {
            (Some(message), "deliver") => self.count(message, &after),
            _ => None,
        };
        let step = match message {
            Some(message) => Step {
                tally,
                logs,
                broken,
                fields: carried
                    .as_ref()
                    .map(|c| c.fields.clone())
                    .unwrap_or_default(),
                voice: carried.map(|c| c.voice).unwrap_or_default(),
                stage: words::stage_of(&message, log_world(&after)),
                says: words::says_message(&before, &after, &message, kind == "drop"),
                chosen,
                kind: kind.to_string(),
                from: Some(lane_of(&message, true)),
                to: Some(copy_to.map_or_else(|| lane_of(&message, false), |to| format!("n{to}"))),
                label: message_label(&message, log_world(&after)),
                phase: Some(message.phase.clone()),
                reply: message.reply,
                notes,
                state,
            },
            None => Step {
                tally: None,
                logs,
                broken,
                stage: None,
                says: words::says_event(&before, &after, label),
                chosen,
                kind: "event".to_string(),
                from: None,
                to: None,
                label: label.to_string(),
                phase: None,
                reply: false,
                fields: Vec::new(),
                voice: String::new(),
                notes,
                state,
            },
        };
        self.push_step(step);
        self
    }

    /// Keep a step. The step that breaks a rule also says so.
    fn push_step(&mut self, mut step: Step) {
        let was_broken = self.steps.last().is_some_and(|last| last.broken.is_some());
        if let Some(broken) = step.broken.clone()
            && !was_broken
        {
            step.says.push(' ');
            step.says.push_str(&broken);
        }
        self.steps.push(step);
    }

    /// What is chosen: every value a quorum chose, so a run that breaks a
    /// rule shows both values.
    fn chosen_of(&self, after: &WorldView) -> Vec<String> {
        match self.game.world() {
            WorldKind::Decree(world) if world.decisions().len() > 1 => {
                let mut values: Vec<String> = Vec::new();
                for (_, command) in world.decisions() {
                    let text = crate::paxos::view::value_text(command);
                    if !values.contains(&text) {
                        values.push(text);
                    }
                }
                values
            }
            _ => words::chosen(after),
        }
    }

    /// The broken rule, if the run broke one.
    fn broken(&self) -> Option<String> {
        match self.game.world() {
            WorldKind::Decree(world) => world.violation().map(str::to_string),
            WorldKind::Log(_) => None,
        }
    }

    /// Count an answer that `message` delivers: a `Promise` or an
    /// `Accepted` to the node that asked. The counter's own answer counts
    /// too (a colocated node promises and votes to itself without a
    /// message). Whether the answers make a quorum is the core's answer,
    /// read off the world after the step.
    fn count(&mut self, message: &MessageView, after: &WorldView) -> Option<Tally> {
        let (what, phase) = match message.kind.as_str() {
            "Promise" => ("promises", "phase1"),
            "Accepted" => ("votes", "phase2"),
            _ => return None,
        };
        if message.to_party != PartyView::Node || message.from_party != PartyView::Node {
            return None;
        }
        let ballot = message.ballot.clone()?;
        let slot = if log_world(after) && phase == "phase2" {
            message.slot
        } else {
            None
        };
        let counter = after.nodes.iter().find(|node| node.id == message.to)?;
        let key = (format!("n{}", message.to), phase, ballot.clone(), slot);
        let heard = self.heard.entry(key).or_default();
        heard.insert(message.from);
        if counter.flavour == NodeFlavour::Colocated && counter.acceptors.contains(&counter.id) {
            let own = match phase {
                "phase1" => counter.promised.as_deref() == Some(ballot.as_str()),
                _ => counter.accepted.iter().any(|record| {
                    Some(record.slot) == slot && record.ballot.as_deref() == Some(ballot.as_str())
                }),
            };
            if own {
                heard.insert(counter.id);
            }
        }
        let heard = heard.clone();
        let quorum = match (phase, counter.flavour) {
            ("phase1", NodeFlavour::Colocated) => {
                counter.role == Some(RoleView::Leader)
                    && counter.ballot.as_deref() == Some(ballot.as_str())
            }
            ("phase1", _) => {
                counter.ballot.as_deref() == Some(ballot.as_str())
                    && matches!(
                        counter.attempt,
                        Some(AttemptView::Phase2 | AttemptView::Won)
                    )
            }
            (_, NodeFlavour::Colocated) => counter
                .accepted
                .iter()
                .any(|record| Some(record.slot) == slot && record.chosen),
            _ => {
                counter.ballot.as_deref() == Some(ballot.as_str())
                    && counter.attempt == Some(AttemptView::Won)
            }
        };
        let lanes = lanes(after);
        let members = lanes
            .iter()
            .filter(|lane| {
                lane.id
                    .strip_prefix('n')
                    .and_then(|id| id.parse::<u64>().ok())
                    .is_some_and(|id| heard.contains(&id))
            })
            .map(|lane| lane.id.clone())
            .collect();
        let line = self.line(phase, message.to, slot, &heard);
        Some(Tally {
            line,
            what: what.to_string(),
            at: format!("n{}", message.to),
            ballot,
            slot,
            members,
            need: self.need(phase, message.to, after),
            quorum,
        })
    }

    /// The grid line that `heard` fills, asked of the counter's own
    /// configuration: a full row for Phase 1, the slot's column for Phase 2.
    fn line(
        &self,
        phase: &str,
        counter: u64,
        slot: Option<u64>,
        heard: &BTreeSet<u64>,
    ) -> Option<Line> {
        let WorldKind::Log(world) = self.game.world() else {
            return None;
        };
        let config = world.node(paros_core::NodeId(counter))?.acceptors();
        let QuorumSystem::Grid { rows, .. } = config.quorum_system() else {
            return None;
        };
        let voters: BTreeSet<paros_core::NodeId> =
            heard.iter().map(|id| paros_core::NodeId(*id)).collect();
        let _ = rows;
        if phase == "phase1" {
            // A Phase-1 quorum of a grid takes acceptors from every column;
            // it is no one line.
            return None;
        }
        let column = config.column_of(paros_core::Slot(slot?))?;
        config
            .has_phase2_quorum_in(&voters, Some(column))
            .then(|| Line {
                axis: "column".to_string(),
                index: u64::try_from(column).unwrap_or(u64::MAX),
            })
    }

    /// What a phase needs, in words, from the quorum system in force at the
    /// counter: the core's quorum sizes, never a count of the page's own.
    fn need(&self, phase: &str, counter: u64, after: &WorldView) -> String {
        let (system, members) = match self.game.world() {
            WorldKind::Decree(world) => (world.system(), world.members()),
            WorldKind::Log(world) => {
                let members = after
                    .nodes
                    .iter()
                    .find(|node| node.id == counter)
                    .map_or(0, |node| node.acceptors.len());
                (world.system(paros_core::NodeId(counter)), members)
            }
        };
        if phase == "phase1" && !after.matchmakers.is_empty() {
            // Matchmaker Paxos: Phase 1 asks every older configuration.
            return "a quorum of every older configuration".to_string();
        }
        match system {
            QuorumSystem::Grid { rows, .. } if phase == "phase1" => {
                match system.phase1_per_column() {
                    Some(1) | None => "one acceptor of every column".to_string(),
                    Some(per) => format!("{per} of the {rows} acceptors of every column"),
                }
            }
            QuorumSystem::Grid { rows, acks, .. } if acks == rows => {
                "the full column of the slot".to_string()
            }
            QuorumSystem::Grid { rows, acks, .. } => {
                format!("{acks} of the {rows} acceptors of the slot's column")
            }
            _ if phase == "phase1" => {
                format!("{} of {members}", system.phase1_quorum_size(members))
            }
            _ => format!("{} of {members}", system.phase2_quorum_size(members)),
        }
    }

    fn in_flight(&self, id: u64) -> Option<MessageView> {
        self.wire().into_iter().find(|message| message.id == id)
    }

    fn prompt_open(&self) -> bool {
        self.game.world().prompt().is_some()
    }

    fn answer_prompt(&mut self) {
        let (id, choice) = {
            let prompt = self.game.world().prompt().expect("a prompt is open");
            (prompt.id, prompt.expected().to_string())
        };
        self.play(Action::Answer { prompt: id, choice }, "");
    }

    /// Deliver every message `keep` accepts, lowest id first, answering every
    /// prompt on the way, until nothing matching is left.
    ///
    /// # Panics
    ///
    /// When the run does not settle.
    pub fn settle(&mut self, keep: impl Fn(&MessageView) -> bool) -> &mut Self {
        for _ in 0..BUDGET {
            if self.prompt_open() {
                self.answer_prompt();
                continue;
            }
            let next = self
                .wire()
                .into_iter()
                .filter(|message| keep(message))
                .map(|message| message.id)
                .min();
            let Some(id) = next else {
                return self;
            };
            self.play(Action::Deliver { id }, "");
        }
        panic!("a trace settles");
    }

    /// Drop every message `hit` accepts.
    ///
    /// # Panics
    ///
    /// When the run does not settle.
    pub fn drop_all(&mut self, hit: impl Fn(&MessageView) -> bool) -> &mut Self {
        for _ in 0..BUDGET {
            let next = self
                .wire()
                .into_iter()
                .filter(|message| hit(message))
                .map(|message| message.id)
                .min();
            let Some(id) = next else {
                return self;
            };
            self.play(Action::Drop { id }, "");
        }
        panic!("a trace drops finitely many messages");
    }

    /// Drop every message `hit` accepts and deliver the rest, until the wire
    /// is empty: the run of a world where `hit` names a dead link.
    ///
    /// # Panics
    ///
    /// When the run does not settle.
    pub fn settle_dropping(&mut self, hit: impl Fn(&MessageView) -> bool) -> &mut Self {
        for _ in 0..BUDGET {
            if self.prompt_open() {
                self.answer_prompt();
                continue;
            }
            let Some(message) = self.wire().into_iter().min_by_key(|message| message.id) else {
                return self;
            };
            if hit(&message) {
                self.play(Action::Drop { id: message.id }, "");
            } else {
                self.play(Action::Deliver { id: message.id }, "");
            }
        }
        panic!("a trace settles");
    }

    /// The recorded run.
    #[must_use]
    pub fn finish(mut self, id: &str, label: &str, outcome: fn(&WorldKind) -> String) -> Scenario {
        let mut stages: Vec<Option<Stage>> =
            self.steps.iter().map(|step| step.stage.clone()).collect();
        words::fill_stages(&mut stages);
        for (step, stage) in self.steps.iter_mut().zip(stages) {
            step.stage = stage;
        }
        let chapters = self
            .steps
            .iter()
            .enumerate()
            .filter(|(_, step)| step.kind == "event")
            .map(|(at, step)| Chapter {
                title: step.label.clone(),
                at: at + 1,
            })
            .collect();
        Scenario {
            chapters,
            id: id.to_string(),
            label: label.to_string(),
            fork: 0,
            outcome: outcome(self.game.world()),
            start: self.start,
            start_logs: self.start_logs,
            lanes: Some(self.lanes),
            grid: self.grid,
            steps: self.steps,
        }
    }
}

/// The lanes of a world, in id order: nodes first, then matchmakers.
#[must_use]
pub fn lanes(world: &WorldView) -> Vec<Lane> {
    let mut lanes: Vec<Lane> = drawn(world)
        .into_iter()
        .map(|node| {
            let (label, role) = match node.flavour {
                NodeFlavour::Proposer => (format!("proposer {}", node.id), "proposer"),
                NodeFlavour::Acceptor => (format!("acceptor {}", node.id), "acceptor"),
                NodeFlavour::Colocated => (format!("node {}", node.id), "node"),
            };
            Lane {
                id: format!("n{}", node.id),
                label,
                role: role.to_string(),
                cell: node.grid_cell.map(|cell| Cell {
                    row: cell.row,
                    column: cell.column,
                }),
            }
        })
        .collect();
    lanes.extend(world.matchmakers.iter().map(|matchmaker| Lane {
        id: format!("m{}", matchmaker.id),
        // A soft hyphen: a narrow card breaks the word in two.
        label: format!("match\u{ad}maker {}", matchmaker.id),
        role: "matchmaker".to_string(),
        cell: None,
    }));
    lanes
}

/// Build a page's trace: the base run first. The fork of every other run is
/// set from its first step that differs from the base run.
///
/// # Panics
///
/// When no level has the id `level_id`.
#[must_use]
pub fn trace(page: &str, level_id: &str, scenarios: Vec<Scenario>) -> Trace {
    let world = Game::new(level_id)
        .expect("a registered level")
        .view()
        .world;
    let lanes = lanes(&world);
    let grid = grid_of(&world);
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

/// The engine narrates for the game, which speaks to its player ("you
/// answer for it", "this game has no partition object"). A page shows the
/// sentences about the protocol only.
fn site_note(text: &str) -> Option<String> {
    let mut kept = String::new();
    let mut sentence = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        sentence.push(c);
        let end = matches!(c, '.' | '?' | '!') && chars.peek().is_none_or(|next| *next == ' ');
        if end || chars.peek().is_none() {
            let words: Vec<String> = sentence
                .split(|c: char| !c.is_alphanumeric())
                .map(str::to_lowercase)
                .collect();
            let to_player = words
                .iter()
                .any(|word| matches!(word.as_str(), "you" | "your" | "game" | "player" | "level"));
            if !to_player && !sentence.trim_end().ends_with('?') {
                kept.push_str(&sentence);
            }
            sentence.clear();
        }
    }
    let kept = kept.trim();
    let mut chars = kept.chars();
    let first = chars.next()?;
    Some(first.to_uppercase().chain(chars).collect())
}

fn lane_of(message: &MessageView, sender: bool) -> String {
    let (id, party) = if sender {
        (message.from, message.from_party)
    } else {
        (message.to, message.to_party)
    };
    match party {
        PartyView::Node => format!("n{id}"),
        PartyView::Matchmaker => format!("m{id}"),
    }
}

/// `Prepare 1.5`, `Accept 1.5 slot 2`: the variant and the numbers a reader
/// follows. The value of an `Accept` is in the step's notes.
fn message_label(message: &MessageView, log: bool) -> String {
    let mut label = message.kind.clone();
    if let Some(ballot) = &message.ballot {
        label.push(' ');
        label.push_str(ballot);
    }
    if let Some(slot) = message.slot
        && log
        && matches!(
            message.kind.as_str(),
            "Accept" | "Accepted" | "Commit" | "Nack"
        )
    {
        let _ = write!(label, " · slot {slot}");
    }
    label
}

/// Whether the world keeps a log (slots), not one decree.
fn log_world(world: &WorldView) -> bool {
    world
        .nodes
        .iter()
        .any(|node| node.flavour == NodeFlavour::Colocated)
}

/// The nodes in lane order: a proposer is drawn left of the acceptors it
/// asks.
fn drawn(world: &WorldView) -> Vec<&NodeView> {
    let mut nodes: Vec<&NodeView> = world.nodes.iter().collect();
    nodes.sort_by_key(|node| node.flavour != NodeFlavour::Proposer);
    nodes
}

/// The grid the acceptors of `world` form, if they form one.
fn grid_of(world: &WorldView) -> Option<Grid> {
    world.nodes.iter().find_map(|node| match node.quorum {
        QuorumSystemView {
            kind: QuorumKindView::Grid,
            rows: Some(rows),
            cols: Some(cols),
            ..
        } => Some(Grid { rows, cols }),
        _ => None,
    })
}

/// The log of each lane, in lane order: empty for a world of one decree,
/// where a card shows the vote in its lines.
fn lane_logs(world: &WorldView) -> Vec<Vec<LogCell>> {
    if !log_world(world) {
        return Vec::new();
    }
    let mut logs: Vec<Vec<LogCell>> = drawn(world)
        .into_iter()
        .map(|node| {
            if !node.alive {
                return Vec::new();
            }
            node.accepted
                .iter()
                .map(|record| LogCell {
                    slot: record.slot,
                    value: if record.control.as_deref() == Some("noop") {
                        "no-op".to_string()
                    } else {
                        record.value.clone()
                    },
                    state: if record.chosen { "chosen" } else { "accepted" }.to_string(),
                    control: record.control.is_some(),
                })
                .collect()
        })
        .collect();
    logs.extend(world.matchmakers.iter().map(|_| Vec::new()));
    logs
}

/// A few short lines per lane: what a reader must see change.
fn lane_state(world: &WorldView) -> Vec<Vec<String>> {
    let mut state: Vec<Vec<String>> = drawn(world).into_iter().map(words::node_lines).collect();
    state.extend(world.matchmakers.iter().map(|matchmaker| {
        if !matchmaker.alive {
            return vec!["down".to_string()];
        }
        let mut lines: Vec<String> = matchmaker
            .registrations
            .iter()
            .map(|registration| {
                let members: Vec<String> =
                    registration.members.iter().map(u64::to_string).collect();
                format!("{} → {}", registration.ballot, members.join(","))
            })
            .collect();
        // The GC watermark: every configuration below it is forgotten.
        if matchmaker.gc_watermark != "0.0" {
            lines.push(format!("forgot < {}", matchmaker.gc_watermark));
        }
        if lines.is_empty() {
            lines.push("no record".to_string());
        }
        lines
    }));
    state
}
