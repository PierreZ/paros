//! Step-through traces for the site's Paxos pages (#811 (Paxos flavor pages)).
//!
//! A trace is a recorded run of the real engine: a [`Game`] on a level's own
//! setup, driven by a short script that chooses messages by what they are.
//! Every decision is the one `paros-consensus` makes. Each delivered,
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
//! A run can also be told as a story (#1146 (recorder for the animated
//! chapters)): its script opens beats, one written sentence each, and every
//! step carries the messages on the wire and each lane's typed state
//! (`story`).
//!
//! `tests/paxos_traces.rs` writes every trace to `website/site/src/traces/`
//! (committed, diffed in CI).

pub(crate) mod compart;
mod content;
mod pages;
mod words;

pub(crate) use crate::steps::story;

pub use crate::steps::{
    Beat, Cell, Chapter, Choice, Field, Flight, Grid, Lane, Line, LogCell, NodeState, Scenario,
    Stage, Step, Tally, Trace, Vote, assemble,
};
pub use pages::{trace_of, traces};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use paros_consensus::membership::QuorumSystem;

use crate::paxos::action::Action;
use crate::paxos::view::{
    AttemptView, MessageView, NodeFlavour, NodeView, PartyView, QuorumKindView, QuorumSystemView,
    RoleView, WorldView,
};
use crate::paxos::{Game, WorldKind};

/// How many steps one recording may take before it is called a loop.
const BUDGET: usize = 2048;

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
    /// The broadcast of each message on the wire: message id → fan id.
    fans: BTreeMap<u64, u64>,
    /// The durable facts of each lane after the last step, in lane order.
    durable: Vec<story::Durable>,
    start_nodes: Vec<NodeState>,
    start_wire: Vec<Flight>,
    beats: story::Beats,
    tour: Option<story::Tour>,
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
        let world = game.world().view();
        let start = lane_state(&world);
        let start_logs = lane_logs(&world);
        let durable = lane_durable(&world);
        let start_nodes = lane_nodes(&world, &durable, &durable);
        let mut fans = BTreeMap::new();
        let start_wire = flights(&world, &mut fans);
        Self {
            lanes: lanes(&world),
            grid: grid_of(&world),
            game,
            start,
            start_logs,
            steps: Vec::new(),
            heard: BTreeMap::new(),
            fans,
            durable,
            start_nodes,
            start_wire,
            beats: story::Beats::default(),
            tour: None,
        }
    }

    /// Mark the run as a tour run: chapter `chapter` of the tour of the
    /// Paxos flavors plays it, under `title`. A tour run tells its flavor in
    /// one world with no restart, and `tests/paxos_tour.rs` holds it to the
    /// rules of the tour (`story::tour_problems`).
    ///
    /// # Panics
    ///
    /// When the run is marked already.
    pub fn tour(&mut self, chapter: &str, title: &str) -> &mut Self {
        assert!(self.tour.is_none(), "a run is in one chapter");
        self.tour = Some(story::Tour {
            chapter: chapter.to_string(),
            title: title.to_string(),
        });
        self
    }

    /// Link the open beat to `run`, the run of the same page that goes
    /// deeper into it ("More on this").
    ///
    /// # Panics
    ///
    /// When no beat is open, or it links a run already.
    pub fn more(&mut self, run: &str) -> &mut Self {
        self.beats.more(run);
        self
    }

    /// Make `lanes` appear at the open beat: the stage draws them from this
    /// beat on, so it starts small and grows.
    ///
    /// # Panics
    ///
    /// When no beat is open, or a lane is named twice.
    pub fn enter(&mut self, lanes: &[&str]) -> &mut Self {
        self.beats.enter(lanes);
        self
    }

    /// Open a beat: one screen of the run's story, with the sentence that
    /// the screen shows. The steps that follow belong to it, up to the next
    /// beat. A beat with no step is an introduction screen.
    ///
    /// # Panics
    ///
    /// When `says` is not one sentence that ends with a full stop.
    pub fn beat(&mut self, says: &str) -> &mut Self {
        self.beats.open(self.steps.len(), says);
        self
    }

    /// End the open beat with a choice: `label` opens the run `run` of the
    /// same page at its beat `beat`.
    ///
    /// # Panics
    ///
    /// When no beat is open.
    pub fn choice(&mut self, label: &str, run: &str, beat: usize) -> &mut Self {
        self.beats.choice(label, run, beat);
        self
    }

    /// Ask the reader a question before the open beat plays: `answers` in
    /// order, `right` the right one from 0, and `why` it is right. The page
    /// shows the question, plays the beat after any answer, then shows `why`.
    /// Pin the right answer to the run with [`Recorder::holds`] after the
    /// steps of the beat.
    ///
    /// # Panics
    ///
    /// When no beat is open, or the question is not well formed (see
    /// `story::Beats::ask`).
    pub fn ask(&mut self, asks: &str, answers: &[&str], right: usize, why: &str) -> &mut Self {
        self.beats.ask(asks, answers, right, why);
        self
    }

    /// Ask the reader what went wrong in the open beat, after it played: the
    /// "find the bug" question of a "Break the rule" run. Pin the answer
    /// with [`Recorder::holds`] after the beat's steps, as for
    /// [`Recorder::ask`].
    pub fn find(&mut self, asks: &str, answers: &[&str], right: usize, why: &str) -> &mut Self {
        self.beats.find(asks, answers, right, why);
        self
    }

    /// Check a fact of the world as it stands. A script calls it after the
    /// steps of a beat that asks a question, so that the right answer is what
    /// `paros-consensus` did and not what the script's author remembers.
    ///
    /// # Panics
    ///
    /// When `fact` is false: the core changed, and the story must change.
    pub fn holds(&mut self, fact: impl Fn(&WorldView) -> bool, says: &str) -> &mut Self {
        assert!(
            fact(&self.game.world().view()),
            "the run no longer shows that {says}"
        );
        self
    }

    /// The world as it stands.
    #[must_use]
    pub fn world(&self) -> &WorldKind {
        self.game.world()
    }

    /// Everything in flight.
    #[must_use]
    pub fn wire(&self) -> Vec<MessageView> {
        self.game.world().view().wire
    }

    /// Play one action. A message action becomes a step; any other action
    /// is an `event` step labelled `label`.
    ///
    /// # Panics
    ///
    /// When the engine refuses the action: a trace is a run that works.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "every script builds its action in the call"
    )]
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
                .map(|entry| content::content(entry, log_world(&self.game.world().view()))),
            _ => None,
        };
        let (kind, message, copy_to) = match &action {
            Action::Deliver { id } => ("deliver", self.in_flight(*id), None),
            Action::Duplicate { id, to } => ("copy", self.in_flight(*id), *to),
            Action::Drop { id } => ("drop", self.in_flight(*id), None),
            _ => ("event", None, None),
        };
        let what = action.label();
        let message_id = message.as_ref().map(|message| message.id);
        let before = self.game.world().view();
        self.game
            .act(&action)
            .unwrap_or_else(|err| panic!("the trace step {what} was refused: {err}"));
        let after = self.game.world().view();
        let state = lane_state(&after);
        let notes: Vec<String> = self
            .game
            .narration()
            .iter()
            .filter_map(|event| site_note(&event.text))
            .collect();
        let chosen = self.chosen_of(&after);
        let broken = self.broken();
        let logs = lane_logs(&after);
        let (tally, repeated) = match (&message, kind) {
            (Some(message), "deliver") => self.count(message, &after),
            _ => (None, false),
        };
        let durable = lane_durable(&after);
        let lane_states = lane_nodes(&after, &self.durable, &durable);
        self.durable = durable;
        let wire = flights(&after, &mut self.fans);
        let step = match message {
            Some(message) => Step {
                tally,
                logs,
                broken,
                message: message_id,
                wire,
                nodes: lane_states,
                fields: carried.as_ref().map_or_default(|c| c.fields.clone()),
                voice: carried.map_or_default(|c| c.voice),
                stage: words::stage_of(&message, log_world(&after)),
                says: message_says(kind, &before, &after, &message, repeated),
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
                message: None,
                wire,
                nodes: lane_states,
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
            WorldKind::Log(_) => two_values(&self.game.world().view()),
        }
    }

    /// Count an answer that `message` delivers: a `Promise` or an
    /// `Accepted` to the node that asked. The counter's own answer counts
    /// too (a colocated node promises and votes to itself without a
    /// message). Whether the answers make a quorum is the core's answer,
    /// read off the world after the step.
    ///
    /// Also says whether the counter had already counted an answer from this
    /// sender for this phase: a copy of a message counts once.
    fn count(&mut self, message: &MessageView, after: &WorldView) -> (Option<Tally>, bool) {
        let (what, phase) = match message.kind.as_str() {
            "Promise" => ("promises", "phase1"),
            "Accepted" => ("votes", "phase2"),
            _ => return (None, false),
        };
        if message.to_party != PartyView::Node || message.from_party != PartyView::Node {
            return (None, false);
        }
        let Some(ballot) = message.ballot.clone() else {
            return (None, false);
        };
        let slot = if log_world(after) && phase == "phase2" {
            message.slot
        } else {
            None
        };
        let Some(counter) = after.nodes.iter().find(|node| node.id == message.to) else {
            return (None, false);
        };
        let key = (format!("n{}", message.to), phase, ballot.clone(), slot);
        let heard = self.heard.entry(key).or_default();
        let repeated = !heard.insert(message.from);
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
        let tally = Some(Tally {
            line,
            what: what.to_string(),
            at: format!("n{}", message.to),
            ballot,
            slot,
            members,
            need: self.need(phase, message.to, after),
            quorum,
        });
        (tally, repeated)
    }

    /// The grid line that `heard` fills, asked of the counter's own
    /// configuration: the slot's column for Phase 2; a Phase-1 quorum of a
    /// grid is no one line, so Phase 1 draws none.
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
        let config = world.node(paros_consensus::NodeId(counter))?.acceptors();
        let QuorumSystem::Grid { rows, .. } = config.quorum_system() else {
            return None;
        };
        let voters: BTreeSet<paros_consensus::NodeId> = heard
            .iter()
            .map(|id| paros_consensus::NodeId(*id))
            .collect();
        let _ = rows;
        if phase == "phase1" {
            // A Phase-1 quorum of a grid takes acceptors from every column;
            // it is no one line.
            return None;
        }
        let column = config.column_of(paros_consensus::Slot(slot?))?;
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
                (world.system(paros_consensus::NodeId(counter)), members)
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
            QuorumSystem::Majority => {
                format!("{} of {members}", system.phase2_quorum_size(members))
            }
        }
    }

    fn in_flight(&self, id: u64) -> Option<MessageView> {
        self.wire().into_iter().find(|message| message.id == id)
    }

    /// Deliver every message `keep` accepts, lowest id first, until nothing
    /// matching is left.
    ///
    /// # Panics
    ///
    /// When the run does not settle.
    pub fn settle(&mut self, keep: impl Fn(&MessageView) -> bool) -> &mut Self {
        for _ in 0..BUDGET {
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
        crate::steps::fill_stages(&mut stages);
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
            start_nodes: self.start_nodes,
            start_wire: self.start_wire,
            beats: self.beats.finish(self.steps.len()),
            lanes: Some(self.lanes),
            grid: self.grid,
            steps: self.steps,
            tour: self.tour,
        }
    }
}

/// One short sentence on a message step: a copy, a loss or a delivery. A
/// `repeated` answer is one the counter already counted.
fn message_says(
    kind: &str,
    before: &WorldView,
    after: &WorldView,
    message: &MessageView,
    repeated: bool,
) -> String {
    if kind == "copy" {
        return words::says_copy(after, message);
    }
    let mut says = words::says_message(before, after, message, kind == "drop");
    if repeated {
        says.push_str(
            " It already counted an answer from this sender for this ballot, so it ignores \
             this copy.",
        );
    }
    says
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
        .world()
        .view();
    assemble(page, lanes(&world), grid_of(&world), scenarios)
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

/// Two nodes of a log world that hold two different values chosen for one
/// slot, in words; `None` while every slot holds one value. Only a "Break
/// the rule" run gets there: the fault comes from outside the core.
fn two_values(world: &WorldView) -> Option<String> {
    let mut chosen: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for node in &world.nodes {
        for record in node.accepted.iter().filter(|record| record.chosen) {
            let values = chosen.entry(record.slot).or_default();
            if !values.contains(&record.value) {
                values.push(record.value.clone());
            }
        }
    }
    chosen
        .into_iter()
        .find(|(_, values)| values.len() > 1)
        .map(|(slot, values)| {
            format!(
                "Two values are chosen for slot {slot}: {}. Paxos says this never happens.",
                values.join(" and ")
            )
        })
}

pub(crate) fn lane_of(message: &MessageView, sender: bool) -> String {
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
pub(crate) fn message_label(message: &MessageView, log: bool) -> String {
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
pub(crate) fn lane_logs(world: &WorldView) -> Vec<Vec<LogCell>> {
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

/// The typed state of each lane, in lane order, as the story draws it: up
/// or down, its role, its ballot and promise, the holes of its log. No step
/// ran, so no lane wrote anything.
pub(crate) fn lane_states(world: &WorldView) -> Vec<NodeState> {
    let durable = lane_durable(world);
    lane_nodes(world, &durable, &durable)
}

/// A few short lines per lane: what a reader must see change.
pub(crate) fn lane_state(world: &WorldView) -> Vec<Vec<String>> {
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

/// The durable facts of each lane, in lane order: what a disk holds.
fn lane_durable(world: &WorldView) -> Vec<story::Durable> {
    let mut durable: Vec<story::Durable> = drawn(world)
        .into_iter()
        .map(|node| match node.flavour {
            // A proposer of one decree keeps nothing on disk.
            NodeFlavour::Proposer => story::Durable {
                alive: node.alive,
                ..story::Durable::default()
            },
            _ => story::Durable {
                alive: node.alive && !node.wiped && !node.retired,
                promise: node
                    .promised
                    .clone()
                    .filter(|promised| promised != words::NO_BALLOT),
                votes: node
                    .accepted
                    .iter()
                    .filter_map(|record| {
                        let ballot = record.ballot.clone()?;
                        Some((record.slot, (ballot, record.value.clone())))
                    })
                    .collect(),
                chosen: node
                    .accepted
                    .iter()
                    .filter(|record| record.chosen)
                    .map(|record| record.slot)
                    .collect(),
                registrations: BTreeMap::new(),
            },
        })
        .collect();
    durable.extend(world.matchmakers.iter().map(|matchmaker| {
        story::Durable {
            alive: matchmaker.alive,
            promise: None,
            votes: BTreeMap::new(),
            chosen: std::collections::BTreeSet::new(),
            registrations: matchmaker
                .registrations
                .iter()
                .map(|registration| {
                    let members: Vec<String> =
                        registration.members.iter().map(u64::to_string).collect();
                    (registration.ballot.clone(), members.join(","))
                })
                .collect(),
        }
    }));
    durable
}

/// The typed state of each lane, in lane order. `was` and `now` are the
/// durable facts before and after the step: what changed is what the step
/// wrote.
fn lane_nodes(world: &WorldView, was: &[story::Durable], now: &[story::Durable]) -> Vec<NodeState> {
    let log = log_world(world);
    let mut nodes: Vec<NodeState> = drawn(world)
        .into_iter()
        .map(|node| {
            let role = match (node.role, node.attempt) {
                (Some(RoleView::Leader), _) => Some("leader"),
                (Some(RoleView::Candidate), _) => Some("candidate"),
                (Some(RoleView::Follower), _) => Some("follower"),
                (None, Some(AttemptView::Idle)) => Some("idle"),
                (None, Some(AttemptView::Phase1)) => Some("phase1"),
                (None, Some(AttemptView::Phase2)) => Some("phase2"),
                (None, Some(AttemptView::Preempted)) => Some("stopped"),
                (None, Some(AttemptView::Won)) => Some("chosen"),
                (None, None) => None,
            };
            let vote = (!log && node.flavour == NodeFlavour::Acceptor)
                .then(|| node.accepted.first())
                .flatten()
                .map(|record| Vote {
                    value: record.value.clone(),
                    ballot: record.ballot.clone().unwrap_or_default(),
                });
            NodeState {
                alive: node.alive,
                role: role.map(str::to_string),
                ballot: node.ballot.clone(),
                promise: node
                    .promised
                    .clone()
                    .filter(|promised| promised != words::NO_BALLOT),
                vote,
                holes: if log && node.alive {
                    story::holes(
                        node.floor.unwrap_or(0),
                        node.accepted.iter().map(|record| record.slot),
                    )
                } else {
                    Vec::new()
                },
                writes: Vec::new(),
                retired: node.retired,
                wiped: node.wiped,
            }
        })
        .collect();
    nodes.extend(world.matchmakers.iter().map(|matchmaker| NodeState {
        alive: matchmaker.alive,
        ..NodeState::default()
    }));
    for ((node, was), now) in nodes.iter_mut().zip(was).zip(now) {
        node.writes = was.writes(now, log);
    }
    nodes
}

/// The messages on the wire, oldest first. A message seen for the first
/// time joins the broadcast of the messages its sender put on the wire in
/// the same action with the same kind, ballot and slot.
fn flights(world: &WorldView, fans: &mut BTreeMap<u64, u64>) -> Vec<Flight> {
    let log = log_world(world);
    let mut opened: BTreeMap<(String, String, Option<String>, Option<u64>), u64> = BTreeMap::new();
    for message in &world.wire {
        if fans.contains_key(&message.id) {
            continue;
        }
        let key = (
            lane_of(message, true),
            message.kind.clone(),
            message.ballot.clone(),
            message.slot,
        );
        let fan = *opened.entry(key).or_insert(message.id);
        fans.insert(message.id, fan);
    }
    fans.retain(|id, _| world.wire.iter().any(|message| message.id == *id));
    world
        .wire
        .iter()
        .map(|message| Flight {
            id: message.id,
            from: lane_of(message, true),
            to: lane_of(message, false),
            phase: message.phase.clone(),
            label: message_label(message, log),
            fan: fans[&message.id],
        })
        .collect()
}
