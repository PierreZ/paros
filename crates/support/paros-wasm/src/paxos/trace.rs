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

mod pages;
mod words;

pub use pages::{trace_of, traces};
pub use words::Stage;

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::paxos::action::Action;
use crate::paxos::view::{MessageView, NodeFlavour, NodeView, PartyView, WorldView};
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
    /// The steps, in order.
    pub steps: Vec<Step>,
    /// What the run ends with, read off the world.
    pub outcome: String,
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
    /// The state of each lane after the step, in lane order: a few short
    /// lines per lane ("promise 1.5", "vote alpha @ 1.5").
    pub state: Vec<Vec<String>>,
    /// What is chosen after the step: `alpha`, or `slot 0 = alpha`.
    pub chosen: Vec<String>,
}

/// Records one run of a level's setup.
pub struct Recorder {
    game: Game,
    start: Vec<Vec<String>>,
    steps: Vec<Step>,
}

impl Recorder {
    /// Start a run on the setup of `level_id`.
    ///
    /// # Panics
    ///
    /// When no level has that id.
    #[must_use]
    pub fn new(level_id: &str) -> Self {
        let game = Game::new(level_id).expect("a registered level");
        let start = lane_state(&game.view().world);
        Self {
            game,
            start,
            steps: Vec::new(),
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
            if let Some(last) = self.steps.last_mut() {
                last.state = state;
                last.chosen = words::chosen(&after);
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
        let chosen = words::chosen(&after);
        let step = match message {
            Some(message) => Step {
                stage: words::stage_of(&message),
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
                stage: None,
                says: words::says_event(&before, &after, label),
                chosen,
                kind: "event".to_string(),
                from: None,
                to: None,
                label: label.to_string(),
                phase: None,
                reply: false,
                notes,
                state,
            },
        };
        self.steps.push(step);
        self
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
        Scenario {
            id: id.to_string(),
            label: label.to_string(),
            fork: 0,
            outcome: outcome(self.game.world()),
            start: self.start,
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
            }
        })
        .collect();
    lanes.extend(world.matchmakers.iter().map(|matchmaker| Lane {
        id: format!("m{}", matchmaker.id),
        // A soft hyphen: a narrow card breaks the word in two.
        label: format!("match\u{ad}maker {}", matchmaker.id),
        role: "matchmaker".to_string(),
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
    let lanes = lanes(
        &Game::new(level_id)
            .expect("a registered level")
            .view()
            .world,
    );
    let mut scenarios = scenarios;
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

/// A few short lines per lane: what a reader must see change.
fn lane_state(world: &WorldView) -> Vec<Vec<String>> {
    let mut state: Vec<Vec<String>> = drawn(world).into_iter().map(words::node_lines).collect();
    state.extend(world.matchmakers.iter().map(|matchmaker| {
        if !matchmaker.alive {
            return vec!["down".to_string()];
        }
        if matchmaker.registrations.is_empty() {
            return vec!["no record".to_string()];
        }
        matchmaker
            .registrations
            .iter()
            .map(|registration| {
                let members: Vec<String> =
                    registration.members.iter().map(u64::to_string).collect();
                format!("{} → {}", registration.ballot, members.join(","))
            })
            .collect()
    }));
    state
}
