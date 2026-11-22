//! The exercises of the Paxos course ("your turn"): a world of
//! `paros-consensus` that the reader drives by hand, with one goal.
//!
//! The reader is the network: each message on the wire can be delivered or
//! dropped. An exercise can also give the reader a few more moves (a proposer
//! opens a ballot, an acceptor loses its disk). The core decides everything
//! else. The goal is a predicate on the world and on the messages the reader
//! delivered, so the page holds no protocol logic: it draws the [`LabView`]
//! and sends the reader's [`Move`]s.
//!
//! A move outside the exercise is refused. A rule that the exercise turns off
//! is a move of the driver that the shipped code refuses (an acceptor that
//! boots on an empty disk), never a change of the core; the exercise says so
//! in `rule_off`. Each exercise has a solution in Rust, and a test plays it:
//! an exercise that the core no longer lets a reader solve fails CI.
//!
//! The engine draws no randomness, so a list of moves is a full replay: undo
//! starts the exercise again and plays every move but the last.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::paxos::action::Action;
use crate::paxos::level::WorldKind;
use crate::paxos::level::common::{crash, open, propose, start_election as elect};
use crate::paxos::trace::compart::desk::{Desk, Verb};
use crate::paxos::trace::{lane_of, lane_state, lanes, message_label};
use crate::paxos::view::{NodeFlavour, RoleView, WorldView};
use crate::paxos::{ActionError, Game};

pub use crate::paxos::trace::Lane;

/// The setup of the single-decree exercises: acceptors 1, 2 and 3,
/// proposers 5 and 8, one decree.
const DECREE: &str = "act1/the-duel";

/// The setup of the log exercises: three nodes, each an acceptor and a
/// replica.
const LOG: &str = "act2/steady-state";

/// The grid: six acceptors in 2 rows and 3 columns.
const GRID: &str = "act4/the-grid";

/// Four nodes (node 3 outside the acceptors at first) and two matchmakers.
const MATCHMAKERS: &str = "act4/reconfigure";

/// Where an exercise runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Setup {
    /// A level of the game's worlds, by its id.
    Level(&'static str),
    /// The compartmentalized deployment: node 0 leads three acceptors, with
    /// two proxy leaders and two replicas ([`Desk`]).
    Compartments,
}

/// A move of an exercise other than deliver and drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Extra {
    /// An action of the game's worlds.
    Act(Action),
    /// A verb of the compartmentalized deployment.
    Compart(Verb),
}

/// How many moves an exercise takes at most: far above any solution, and a
/// bound on the replay of an undo.
const MOVES: usize = 400;

/// One exercise.
pub struct Exercise {
    /// A stable id: the address and the progress use it.
    pub id: &'static str,
    /// The tab's text.
    pub title: &'static str,
    /// The goal, in one or two sentences.
    pub task: &'static str,
    /// The rule this build turns off, in plain words, if any.
    pub rule_off: Option<&'static str>,
    /// What the reader sees when the goal holds: why it matters.
    pub done: &'static str,
    /// Where the exercise runs.
    setup_on: Setup,
    /// The moves played before the reader's first one. They are not the
    /// reader's: undo never takes one back.
    setup: fn(&mut Lab),
    /// The moves other than deliver and drop, with their button text.
    extras: fn() -> Vec<(&'static str, Extra)>,
    /// Whether the goal holds.
    goal: fn(&Lab) -> bool,
    /// A way to reach the goal, played by the tests.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "only the tests play the solutions")
    )]
    solution: fn(&mut Lab),
}

/// Every exercise, in course order.
#[must_use]
pub fn exercises() -> &'static [Exercise] {
    &EXERCISES
}

/// The exercise `id`.
#[must_use]
pub fn exercise(id: &str) -> Option<&'static Exercise> {
    EXERCISES.iter().find(|exercise| exercise.id == id)
}

static EXERCISES: [Exercise; 11] = [
    Exercise {
        id: "vote-not-chosen",
        title: "A vote, not a choice",
        task: "Proposer 5 asks for alpha. Leave a vote for alpha on exactly one acceptor, with \
               no value chosen and no message on the wire.",
        rule_off: None,
        done: "One vote of three is not a choice. A later proposer that hears of this vote must \
               propose alpha, but it is free to choose another value when it does not hear of it.",
        setup_on: Setup::Level(DECREE),
        setup: |lab| lab.act(&open(5, "alpha")),
        extras: Vec::new,
        goal: |lab| {
            let world = lab.view_of_world();
            voters(&world, "alpha") == 1 && world.chosen.is_none() && world.wire.is_empty()
        },
        solution: |lab| {
            lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise");
            lab.deliver(|m| m.kind == "Accept" && m.to == "n1");
            lab.drop_all(|m| m.kind == "Accept");
            lab.settle(|_| true);
        },
    },
    Exercise {
        id: "two-values",
        title: "Two values",
        task: "In this build, an acceptor can lose its disk and answer again. Get two \
               different values chosen.",
        rule_off: Some(
            "An acceptor boots on an empty disk. paros refuses this boot: it is the rule that \
             this exercise turns off.",
        ),
        done: "A majority that forgets its vote is no longer a majority. This is why an \
               acceptor writes to disk before it answers, and why paros refuses to boot a \
               member whose disk is empty.",
        setup_on: Setup::Level(DECREE),
        setup: |lab| lab.act(&open(5, "alpha")),
        extras: || {
            vec![
                (
                    "Proposer 8 opens a ballot for bravo",
                    Extra::Act(open(8, "bravo")),
                ),
                (
                    "Proposer 5 opens a higher ballot",
                    Extra::Act(open(5, "alpha")),
                ),
                (
                    "Acceptor 1 loses its disk",
                    Extra::Act(Action::Wipe { node: 1 }),
                ),
                (
                    "Acceptor 2 loses its disk",
                    Extra::Act(Action::Wipe { node: 2 }),
                ),
                (
                    "Acceptor 3 loses its disk",
                    Extra::Act(Action::Wipe { node: 3 }),
                ),
            ]
        },
        goal: |lab| match lab.game().world() {
            WorldKind::Decree(world) => world.violation().is_some(),
            WorldKind::Log(_) => false,
        },
        solution: |lab| {
            lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise");
            lab.settle(|m| m.kind == "Accept" && m.to != "n3");
            lab.drop_all(|m| m.kind == "Accept");
            lab.settle(|m| m.kind == "Accepted");
            lab.drop_all(|_| true);
            lab.extra("Acceptor 2 loses its disk");
            lab.extra("Proposer 8 opens a ballot for bravo");
            lab.settle(|m| m.to != "n1" && (m.kind == "Prepare" || m.kind == "Promise"));
            lab.drop_all(|m| m.kind == "Prepare");
            lab.settle(|m| m.to != "n1" && (m.kind == "Accept" || m.kind == "Accepted"));
        },
    },
    Exercise {
        id: "duel",
        title: "The duel",
        task: "Two proposers want two values. Make each proposer get two refusals (Nack), \
               with no value chosen.",
        rule_off: None,
        done: "Paxos stays safe with two proposers, but they can block each other with no \
               end. paros keeps one stable leader, and a proposer that loses waits a random \
               time before it tries again.",
        setup_on: Setup::Level(DECREE),
        setup: |_| {},
        extras: || {
            vec![
                (
                    "Proposer 5 opens a ballot for alpha",
                    Extra::Act(open(5, "alpha")),
                ),
                (
                    "Proposer 8 opens a ballot for bravo",
                    Extra::Act(open(8, "bravo")),
                ),
            ]
        },
        goal: |lab| {
            let world = lab.view_of_world();
            world.chosen.is_none() && lab.refusals(5) >= 2 && lab.refusals(8) >= 2
        },
        solution: |lab| {
            lab.extra("Proposer 5 opens a ballot for alpha");
            lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise");
            lab.extra("Proposer 8 opens a ballot for bravo");
            lab.settle(|m| {
                (m.from == "n8" || m.to == "n8") && (m.kind == "Prepare" || m.kind == "Promise")
            });
            lab.settle(|m| m.from == "n5" || m.to == "n5");
            lab.extra("Proposer 5 opens a ballot for alpha");
            lab.settle(|m| {
                (m.from == "n5" || m.to == "n5") && (m.kind == "Prepare" || m.kind == "Promise")
            });
            lab.settle(|m| m.from == "n8" || m.to == "n8");
            lab.extra("Proposer 8 opens a ballot for bravo");
            lab.settle(|m| {
                (m.from == "n8" || m.to == "n8") && (m.kind == "Prepare" || m.kind == "Promise")
            });
            lab.settle(|m| m.from == "n5" || m.to == "n5");
            lab.extra("Proposer 5 opens a ballot for alpha");
            lab.settle(|m| {
                (m.from == "n5" || m.to == "n5") && (m.kind == "Prepare" || m.kind == "Promise")
            });
            lab.settle(|m| m.from == "n8" || m.to == "n8");
        },
    },
    Exercise {
        id: "majority",
        title: "Two of three",
        task: "Proposer 5 asks for alpha. Get alpha chosen while acceptor 3 hears nothing: \
               drop every message to it.",
        rule_off: None,
        done: "Two of three is a majority, so acceptor 3 is not needed. Any two majorities of \
               three share an acceptor: that shared acceptor is how a later ballot finds alpha.",
        setup_on: Setup::Level(DECREE),
        setup: |lab| lab.act(&open(5, "alpha")),
        extras: Vec::new,
        goal: |lab| {
            let world = lab.view_of_world();
            world.chosen.is_some() && lab.delivered_to(3) == 0
        },
        solution: |lab| {
            lab.settle(|m| m.to != "n3");
            lab.drop_all(|m| m.to == "n3");
        },
    },
    Exercise {
        id: "gap",
        title: "Fill the gap",
        task: "Node 0 leads, and alpha is chosen at slot 0. Leave slot 1 with no vote and get a \
               value chosen at slot 2. Then make node 1 lead. What does it put in slot 1?",
        rule_off: None,
        done: "A new leader cannot leave a hole: slot 2 would never be applied. It fills every \
               slot that no promise reports with a Noop, so the log goes on.",
        setup_on: Setup::Level(LOG),
        setup: |lab| {
            lab.act(&elect(0));
            lab.settle(|_| true);
            lab.act(&propose(0, "alpha"));
            lab.settle(|_| true);
        },
        extras: || {
            vec![
                ("The client writes bravo", Extra::Act(propose(0, "bravo"))),
                (
                    "The client writes charlie",
                    Extra::Act(propose(0, "charlie")),
                ),
                ("Node 0 crashes", Extra::Act(crash(0))),
                ("Node 1 starts an election", Extra::Act(elect(1))),
            ]
        },
        goal: |lab| {
            let world = lab.view_of_world();
            chosen_at(&world, 1, "Noop") && chosen_at_any(&world, 2)
        },
        solution: |lab| {
            lab.extra("The client writes bravo");
            lab.drop_all(|m| m.kind == "Accept");
            lab.extra("The client writes charlie");
            lab.settle(|m| m.to != "n0" || m.kind == "Accepted");
            lab.extra("Node 0 crashes");
            lab.extra("Node 1 starts an election");
            lab.settle(|_| true);
        },
    },
    Exercise {
        id: "read",
        title: "Read without the leader",
        task: "alpha is chosen at slot 0. A client reads from node 2. Get the read served while \
               node 0, the leader, gets no message.",
        rule_off: None,
        done: "A leaderless read asks a majority of acceptors for the highest slot that each \
               voted in, and waits until its own log covers it. No leader confirms the read.",
        setup_on: Setup::Level(LOG),
        setup: |lab| {
            lab.act(&elect(0));
            lab.settle(|_| true);
            lab.act(&propose(0, "alpha"));
            lab.settle(|_| true);
        },
        extras: || {
            vec![(
                "A client reads from node 2",
                Extra::Act(Action::QuorumRead {
                    node: 2,
                    client: None,
                }),
            )]
        },
        goal: |lab| {
            let world = lab.view_of_world();
            lab.delivered_to(0) == 0
                && world.clients.iter().any(|client| {
                    client
                        .reads
                        .iter()
                        .any(|read| read.node == 2 && read.served)
                })
        },
        solution: |lab| {
            lab.extra("A client reads from node 2");
            lab.settle(|m| m.to != "n0");
            lab.drop_all(|m| m.to == "n0");
        },
    },
    Exercise {
        id: "grid-column",
        title: "A column down",
        task: "Node 0 leads a grid of 2 rows and 3 columns, and node 3 is down. Get a value \
               chosen anyway.",
        rule_off: None,
        done: "Each slot goes to one column, and a Phase 2 quorum is a full column. A node \
               down stops the slots of its column only: a write to another column is chosen.",
        setup_on: Setup::Level(GRID),
        setup: |lab| {
            lab.act(&elect(0));
            lab.settle(|_| true);
            lab.act(&crash(3));
        },
        extras: || {
            vec![
                ("The client writes alpha", Extra::Act(propose(0, "alpha"))),
                ("The client writes bravo", Extra::Act(propose(0, "bravo"))),
            ]
        },
        goal: |lab| {
            let world = lab.view_of_world();
            world
                .nodes
                .iter()
                .any(|node| node.alive && node.accepted.iter().any(|record| record.chosen))
        },
        solution: |lab| {
            lab.extra("The client writes alpha");
            lab.settle(|_| true);
            lab.extra("The client writes bravo");
            lab.settle(|_| true);
        },
    },
    Exercise {
        id: "old-set",
        title: "The old set counts",
        task: "The operator replaced node 2 with node 3, and node 0 is down. Make node 3 the \
               leader. Can you do it with no message to node 2?",
        rule_off: None,
        done: "The matchmakers report the old set and the new set, and Phase 1 needs a majority \
               of each. Node 2 left the new set, but its promise still counts for the old one.",
        setup_on: Setup::Level(MATCHMAKERS),
        setup: |lab| {
            lab.act(&elect(0));
            lab.settle(|_| true);
            lab.act(&propose(0, "alpha"));
            lab.settle(|_| true);
            lab.act(&Action::Reconfigure {
                node: 0,
                members: vec![0, 1, 3],
                quorum: None,
            });
            lab.settle(|_| true);
            lab.act(&crash(0));
        },
        extras: || vec![("Node 3 starts an election", Extra::Act(elect(3)))],
        goal: |lab| {
            lab.view_of_world()
                .nodes
                .iter()
                .any(|node| node.id == 3 && node.role == Some(RoleView::Leader))
        },
        solution: |lab| {
            lab.extra("Node 3 starts an election");
            lab.settle(|_| true);
        },
    },
    Exercise {
        id: "proxy-down",
        title: "A proxy leader down",
        task: "Proxy leader 1 is down. Slot 1 goes to it (slot mod 2). The client writes bravo: \
               get bravo chosen at slot 1.",
        rule_off: None,
        done: "A proxy leader holds nothing on disk, so its crash loses no value. Node 0 sends \
               the slot again on each beat, then takes it back and sends the Accepts itself. A \
               dead proxy costs a few beats.",
        setup_on: Setup::Compartments,
        setup: |lab| {
            lab.verb(&Verb::Write("alpha"));
            lab.settle(|_| true);
            lab.verb(&Verb::CrashProxy(1));
        },
        extras: || {
            vec![
                (
                    "The client writes bravo",
                    Extra::Compart(Verb::Write("bravo")),
                ),
                ("Node 0 beats", Extra::Compart(Verb::LeaderBeats)),
            ]
        },
        goal: |lab| {
            lab.desk()
                .chosen()
                .iter()
                .any(|line| line == "slot 1 = bravo")
        },
        solution: |lab| {
            lab.extra("The client writes bravo");
            lab.settle(|_| true);
            while !lab.solved() {
                lab.extra("Node 0 beats");
                lab.settle(|_| true);
            }
        },
    },
    Exercise {
        id: "lost-votes",
        title: "Lost votes",
        task: "The client writes alpha, and proxy leader 0 sends the Accepts. Drop at least two \
               votes (Accepted) on their way to proxy leader 0. Then get alpha chosen anyway.",
        rule_off: None,
        done: "A proxy leader that hears no majority sends the same Accepts again on its next \
               beat. An acceptor that already voted answers with the same vote, so a re-send \
               is always safe.",
        setup_on: Setup::Compartments,
        setup: |_| {},
        extras: || {
            vec![
                (
                    "The client writes alpha",
                    Extra::Compart(Verb::Write("alpha")),
                ),
                ("Proxy leader 0 beats", Extra::Compart(Verb::ProxyBeats(0))),
            ]
        },
        goal: |lab| {
            lab.dropped("Accepted", "p0") >= 2
                && lab
                    .desk()
                    .chosen()
                    .iter()
                    .any(|line| line == "slot 0 = alpha")
        },
        solution: |lab| {
            lab.extra("The client writes alpha");
            lab.settle(|m| !(m.kind == "Accepted" && m.to == "p0"));
            lab.drop_all(|m| m.kind == "Accepted" && m.to == "p0");
            lab.extra("Proxy leader 0 beats");
            lab.settle(|_| true);
        },
    },
    Exercise {
        id: "replica-waits",
        title: "A replica waits",
        task: "The client writes alpha (slot 0, proxy leader 0) and bravo (slot 1, proxy leader \
               1). Make replica 0 learn slot 1 before slot 0. Then let it learn both.",
        rule_off: None,
        done: "A replica learns the slots in any order, but it applies them in slot order. It \
               holds slot 1 and waits until slot 0 is chosen too, so every replica applies the \
               same log.",
        setup_on: Setup::Compartments,
        setup: |_| {},
        extras: || {
            vec![
                (
                    "The client writes alpha",
                    Extra::Compart(Verb::Write("alpha")),
                ),
                (
                    "The client writes bravo",
                    Extra::Compart(Verb::Write("bravo")),
                ),
            ]
        },
        goal: |lab| lab.desk().waited(0) && lab.desk().learned(0) >= Some(1),
        solution: |lab| {
            lab.extra("The client writes alpha");
            lab.extra("The client writes bravo");
            lab.settle(|m| !(m.kind == "Commit" && m.to == "r0" && m.slot == Some(0)));
            lab.settle(|_| true);
        },
    },
];

/// Some node holds `value` chosen at `slot`.
fn chosen_at(world: &WorldView, slot: u64, value: &str) -> bool {
    world.nodes.iter().any(|node| {
        node.accepted
            .iter()
            .any(|record| record.slot == slot && record.chosen && record.value == value)
    })
}

/// Some node holds a value chosen at `slot`.
fn chosen_at_any(world: &WorldView, slot: u64) -> bool {
    world.nodes.iter().any(|node| {
        node.accepted
            .iter()
            .any(|record| record.slot == slot && record.chosen)
    })
}

/// How many acceptors voted for `value`. A proposer's card also lists the
/// value it proposes, which is not a vote.
fn voters(world: &WorldView, value: &str) -> usize {
    world
        .nodes
        .iter()
        .filter(|node| node.flavour == NodeFlavour::Acceptor)
        .filter(|node| node.accepted.iter().any(|vote| vote.value == value))
        .count()
}

/// One move of the reader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Move {
    /// Deliver the message `id`.
    Deliver {
        /// The message's id on the wire.
        id: u64,
    },
    /// Lose the message `id`.
    Drop {
        /// The message's id on the wire.
        id: u64,
    },
    /// Deliver every message on the wire, and the ones they send, until the
    /// wire is empty.
    DeliverAll,
    /// The extra move at `index` in [`LabView::extras`].
    Extra {
        /// Its index.
        index: usize,
    },
    /// Take back the last move.
    Undo,
    /// Start the exercise again.
    Reset,
}

/// What the page draws.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabView {
    /// The exercise's id.
    pub id: String,
    /// Its tab text.
    pub title: String,
    /// The goal.
    pub task: String,
    /// The rule the build turns off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_off: Option<String>,
    /// The parties, in order.
    pub lanes: Vec<Lane>,
    /// A few lines of state per lane.
    pub state: Vec<Vec<String>>,
    /// Whether each lane is up.
    pub alive: Vec<bool>,
    /// The messages on the wire.
    pub wire: Vec<WireMessage>,
    /// The other moves, by their button text.
    pub extras: Vec<String>,
    /// The chosen values, as `alpha at 1.5` (one decree) or `slot 0 = alpha`
    /// (a log), in the order they were chosen.
    pub chosen: Vec<String>,
    /// Whether two values are chosen for one decree: the goal of the
    /// exercise that turns a rule off.
    pub two_values: bool,
    /// What the core said about the last move.
    pub said: Vec<String>,
    /// The last move was refused: why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
    /// How many moves the reader made.
    pub moves: usize,
    /// Whether the goal holds.
    pub solved: bool,
    /// Why the goal matters, once it holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub done: Option<String>,
}

/// One message on the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireMessage {
    /// Its id.
    pub id: u64,
    /// The `Message` variant's name, e.g. `Prepare`.
    pub kind: String,
    /// The sending lane.
    pub from: String,
    /// The receiving lane.
    pub to: String,
    /// The render family: `prepare`, `promise`, ….
    pub phase: String,
    /// `Prepare 1.5`.
    pub label: String,
    /// What it says, in a short sentence.
    pub summary: String,
    /// The slot it names, if it names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u64>,
}

/// The world an exercise runs.
enum Engine {
    /// A level of the game's worlds.
    Game(Box<Game>),
    /// The compartmentalized deployment.
    Compart(Box<Desk>),
}

/// One exercise in progress.
pub struct Lab {
    exercise: &'static Exercise,
    engine: Engine,
    /// The reader's moves since the start, for undo.
    moves: Vec<Move>,
    /// The messages the reader delivered, by (variant, receiving lane).
    delivered: BTreeMap<(String, String), usize>,
    /// The messages the reader dropped, by (variant, receiving lane).
    dropped: BTreeMap<(String, String), usize>,
    said: Vec<String>,
    refusal: Option<String>,
}

impl Lab {
    /// Start exercise `id`.
    ///
    /// # Errors
    ///
    /// When no exercise has that id.
    pub fn new(id: &str) -> Result<Self, String> {
        let exercise = exercise(id).ok_or_else(|| format!("there is no exercise {id:?}"))?;
        Ok(Self::start(exercise))
    }

    fn start(exercise: &'static Exercise) -> Self {
        let engine = match exercise.setup_on {
            Setup::Level(level) => Engine::Game(Box::new(
                Game::new(level).expect("an exercise names a registered level"),
            )),
            Setup::Compartments => Engine::Compart(Box::default()),
        };
        let mut lab = Self {
            exercise,
            engine,
            moves: Vec::new(),
            delivered: BTreeMap::new(),
            dropped: BTreeMap::new(),
            said: Vec::new(),
            refusal: None,
        };
        (exercise.setup)(&mut lab);
        lab.moves.clear();
        lab.delivered.clear();
        lab.dropped.clear();
        lab.said = lab.texts();
        lab
    }

    /// Play `action` as part of the setup.
    ///
    /// # Panics
    ///
    /// When the core refuses it: a setup is a run that works.
    fn act(&mut self, action: &Action) {
        self.game_mut()
            .act(action)
            .expect("the setup of an exercise is a run that works");
    }

    /// Play `verb` as part of the setup.
    ///
    /// # Panics
    ///
    /// When the deployment refuses it: a setup is a run that works.
    fn verb(&mut self, verb: &Verb) {
        self.desk_mut()
            .act(verb)
            .expect("the setup of an exercise is a run that works");
    }

    /// Play one move. A refused move changes nothing and says why.
    pub fn play(&mut self, chosen: &Move) {
        match chosen {
            Move::Reset => *self = Self::start(self.exercise),
            Move::Undo => {
                let mut moves = std::mem::take(&mut self.moves);
                moves.pop();
                *self = Self::start(self.exercise);
                for again in &moves {
                    self.play(again);
                }
            }
            other => {
                if self.moves.len() >= MOVES {
                    self.refusal = Some(format!("An exercise takes at most {MOVES} moves."));
                    return;
                }
                match self.apply(other) {
                    Ok(()) => {
                        self.moves.push(other.clone());
                        self.refusal = None;
                        self.said = self.texts();
                    }
                    Err(why) => self.refusal = Some(why),
                }
            }
        }
    }

    fn apply(&mut self, chosen: &Move) -> Result<(), String> {
        match chosen {
            Move::DeliverAll => {
                let mut left = MOVES;
                while let Some(message) = self.wire().into_iter().next() {
                    assert!(left > 0, "a wire empties within {MOVES} deliveries");
                    left -= 1;
                    self.apply(&Move::Deliver { id: message.id })?;
                }
                Ok(())
            }
            Move::Deliver { id } | Move::Drop { id } => {
                let deliver = matches!(chosen, Move::Deliver { .. });
                let message = self
                    .wire()
                    .into_iter()
                    .find(|m| m.id == *id)
                    .ok_or_else(|| format!("No message {id} is on the wire."))?;
                match &mut self.engine {
                    Engine::Game(game) => game
                        .act(&if deliver {
                            Action::Deliver { id: *id }
                        } else {
                            Action::Drop { id: *id }
                        })
                        .map_err(|error: ActionError| error.to_string())?,
                    Engine::Compart(desk) if deliver => desk.deliver(*id)?,
                    Engine::Compart(desk) => desk.lose(*id)?,
                }
                let count = if deliver {
                    &mut self.delivered
                } else {
                    &mut self.dropped
                };
                *count.entry((message.kind, message.to)).or_default() += 1;
                Ok(())
            }
            Move::Extra { index } => {
                let extra = (self.exercise.extras)()
                    .into_iter()
                    .nth(*index)
                    .map(|(_, extra)| extra)
                    .ok_or_else(|| "This exercise has no such move.".to_string())?;
                match (&mut self.engine, extra) {
                    (Engine::Game(game), Extra::Act(action)) => game
                        .act(&action)
                        .map_err(|error: ActionError| error.to_string()),
                    (Engine::Compart(desk), Extra::Compart(verb)) => desk.act(&verb),
                    _ => unreachable!("an exercise's extra moves fit its world"),
                }
            }
            Move::Undo | Move::Reset => unreachable!("handled in play"),
        }
    }

    /// How many `Nack`s the reader delivered to `proposer`.
    #[must_use]
    pub fn refusals(&self, proposer: u64) -> usize {
        self.delivered
            .get(&("Nack".to_string(), format!("n{proposer}")))
            .copied()
            .unwrap_or(0)
    }

    /// How many messages the reader delivered to node `node`.
    #[must_use]
    pub fn delivered_to(&self, node: u64) -> usize {
        let lane = format!("n{node}");
        self.delivered
            .iter()
            .filter(|((_, to), _)| *to == lane)
            .map(|(_, count)| count)
            .sum()
    }

    /// How many `kind` messages the reader dropped on their way to `lane`.
    #[must_use]
    pub fn dropped(&self, kind: &str, lane: &str) -> usize {
        self.dropped
            .get(&(kind.to_string(), lane.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// Whether the goal holds.
    #[must_use]
    pub fn solved(&self) -> bool {
        (self.exercise.goal)(self)
    }

    fn game(&self) -> &Game {
        match &self.engine {
            Engine::Game(game) => game,
            Engine::Compart(_) => unreachable!("this exercise runs a level of the game"),
        }
    }

    fn game_mut(&mut self) -> &mut Game {
        match &mut self.engine {
            Engine::Game(game) => game,
            Engine::Compart(_) => unreachable!("this exercise runs a level of the game"),
        }
    }

    fn desk(&self) -> &Desk {
        match &self.engine {
            Engine::Compart(desk) => desk,
            Engine::Game(_) => unreachable!("this exercise runs the compartments"),
        }
    }

    fn desk_mut(&mut self) -> &mut Desk {
        match &mut self.engine {
            Engine::Compart(desk) => desk,
            Engine::Game(_) => unreachable!("this exercise runs the compartments"),
        }
    }

    fn view_of_world(&self) -> WorldView {
        self.game().world().view()
    }

    /// The messages on the wire, oldest first.
    fn wire(&self) -> Vec<WireMessage> {
        match &self.engine {
            Engine::Game(game) => {
                let log = matches!(game.world(), WorldKind::Log(_));
                game.world()
                    .view()
                    .wire
                    .iter()
                    .map(|message| WireMessage {
                        id: message.id,
                        kind: message.kind.clone(),
                        from: lane_of(message, true),
                        to: lane_of(message, false),
                        phase: message.phase.clone(),
                        label: message_label(message, log),
                        summary: message.summary.clone(),
                        slot: message.slot.filter(|_| log),
                    })
                    .collect()
            }
            Engine::Compart(desk) => desk.wire(),
        }
    }

    /// What the last move did.
    fn texts(&self) -> Vec<String> {
        match &self.engine {
            Engine::Game(game) => game
                .narration()
                .iter()
                .map(|event| event.text.clone())
                .collect(),
            Engine::Compart(desk) => desk.said().to_vec(),
        }
    }

    /// The view the page draws.
    #[must_use]
    pub fn view(&self) -> LabView {
        let (lanes, state, alive, chosen) = match &self.engine {
            Engine::Game(game) => game_parts(game),
            Engine::Compart(desk) => (Desk::lanes(), desk.state(), desk.alive(), desk.chosen()),
        };
        let solved = self.solved();
        LabView {
            id: self.exercise.id.to_string(),
            title: self.exercise.title.to_string(),
            task: self.exercise.task.to_string(),
            rule_off: self.exercise.rule_off.map(str::to_string),
            lanes,
            state,
            alive,
            wire: self.wire(),
            extras: (self.exercise.extras)()
                .into_iter()
                .map(|(label, _)| label.to_string())
                .collect(),
            two_values: match &self.engine {
                Engine::Game(game) => match game.world() {
                    WorldKind::Decree(decree) => decree.decisions().len() > 1,
                    WorldKind::Log(_) => false,
                },
                Engine::Compart(_) => false,
            },
            chosen,
            said: self.said.clone(),
            refused: self.refusal.clone(),
            moves: self.moves.len(),
            solved,
            done: solved.then(|| self.exercise.done.to_string()),
        }
    }

    // ---- what a solution is written in ----------------------------------------

    /// Deliver the first message on the wire that `hit` matches.
    fn deliver(&mut self, hit: impl Fn(&WireMessage) -> bool) {
        let id = self
            .wire()
            .into_iter()
            .find(|m| hit(m))
            .expect("a solution delivers a message that is on the wire")
            .id;
        self.must(&Move::Deliver { id });
    }

    /// Deliver every message that `hit` matches, the new ones too, until none
    /// is left.
    fn settle(&mut self, hit: impl Fn(&WireMessage) -> bool) {
        while let Some(message) = self.wire().into_iter().find(|m| hit(m)) {
            self.must(&Move::Deliver { id: message.id });
        }
    }

    /// Drop every message on the wire that `hit` matches.
    fn drop_all(&mut self, hit: impl Fn(&WireMessage) -> bool) {
        for message in self.wire().into_iter().filter(|m| hit(m)) {
            self.must(&Move::Drop { id: message.id });
        }
    }

    /// Play the extra move whose button says `label`.
    fn extra(&mut self, label: &str) {
        let index = (self.exercise.extras)()
            .iter()
            .position(|(text, _)| *text == label)
            .expect("a solution names an extra move of its exercise");
        self.must(&Move::Extra { index });
    }

    fn must(&mut self, chosen: &Move) {
        self.play(chosen);
        assert!(
            self.refusal.is_none(),
            "{}: the move {chosen:?} is refused: {:?}",
            self.exercise.id,
            self.refusal
        );
    }
}

/// The lanes, their state, which are up, and the chosen values of a game.
fn game_parts(game: &Game) -> (Vec<Lane>, Vec<Vec<String>>, Vec<bool>, Vec<String>) {
    let world = game.world().view();
    let lanes = lanes(&world);
    let alive = lanes
        .iter()
        .map(|lane| {
            world
                .nodes
                .iter()
                .find(|node| format!("n{}", node.id) == lane.id)
                .is_none_or(|node| node.alive)
        })
        .collect();
    let chosen = match game.world() {
        WorldKind::Decree(decree) => decree
            .decisions()
            .iter()
            .map(|(ballot, command)| {
                format!(
                    "{} at {}",
                    crate::paxos::view::show_command(command),
                    crate::paxos::view::show_ballot(*ballot)
                )
            })
            .collect(),
        WorldKind::Log(_) => {
            let mut slots: BTreeMap<u64, String> = BTreeMap::new();
            for node in &world.nodes {
                for record in node.accepted.iter().filter(|record| record.chosen) {
                    slots.insert(record.slot, record.value.clone());
                }
            }
            slots
                .into_iter()
                .map(|(slot, value)| format!("slot {slot} = {value}"))
                .collect()
        }
    };
    (lanes, lane_state(&world), alive, chosen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_exercise_has_a_solution_that_reaches_its_goal() {
        for exercise in exercises() {
            let mut lab = Lab::start(exercise);
            assert!(
                !lab.solved(),
                "{}: the goal does not hold at the start",
                exercise.id
            );
            (exercise.solution)(&mut lab);
            assert!(
                lab.solved(),
                "{}: the solution reaches the goal",
                exercise.id
            );
            let view = lab.view();
            assert!(
                view.solved && view.done.is_some(),
                "{}: the view says so",
                exercise.id
            );
        }
    }

    #[test]
    fn the_exercise_ids_differ() {
        let ids: std::collections::BTreeSet<&str> = exercises().iter().map(|e| e.id).collect();
        assert_eq!(ids.len(), exercises().len());
    }

    #[test]
    fn two_values_are_chosen_only_with_the_rule_off() {
        let lab = {
            let mut lab = Lab::new("two-values").expect("an exercise");
            (lab.exercise.solution)(&mut lab);
            lab
        };
        assert!(lab.view().two_values, "the view says two values are chosen");
        assert_eq!(
            lab.view().chosen.len(),
            2,
            "two decisions: {:?}",
            lab.view().chosen
        );
        assert!(lab.moves.iter().any(|m| matches!(m, Move::Extra { index } if (lab.exercise.extras)()[*index].1 == Extra::Act(Action::Wipe { node: 2 }))),
            "the solution needs the wipe");
    }

    #[test]
    fn the_old_set_needs_node_2() {
        let mut lab = Lab::new("old-set").expect("an exercise");
        lab.extra("Node 3 starts an election");
        lab.settle(|m| m.to != "n2");
        lab.drop_all(|_| true);
        assert!(
            !lab.solved(),
            "with no message to node 2, node 3 cannot lead: {:?}",
            lab.view().said
        );
    }

    #[test]
    fn a_slot_of_a_dead_proxy_needs_the_leader_beats() {
        let mut lab = Lab::new("proxy-down").expect("an exercise");
        lab.extra("The client writes bravo");
        lab.settle(|_| true);
        assert!(!lab.solved(), "with no beat, slot 1 stays open");
        lab.extra("Node 0 beats");
        lab.settle(|_| true);
        assert!(
            !lab.solved(),
            "one beat sends the slot to the dead proxy again"
        );
        lab.extra("Node 0 beats");
        lab.settle(|_| true);
        assert!(
            lab.solved(),
            "the second beat takes the slot back: {:?}",
            lab.view().said
        );
    }

    #[test]
    fn votes_delivered_in_time_do_not_reach_the_goal() {
        let mut lab = Lab::new("lost-votes").expect("an exercise");
        lab.extra("The client writes alpha");
        lab.settle(|_| true);
        assert!(
            lab.desk()
                .chosen()
                .iter()
                .any(|line| line == "slot 0 = alpha")
        );
        assert!(!lab.solved(), "no vote was dropped");
    }

    #[test]
    fn a_replica_that_learns_in_order_never_waits() {
        let mut lab = Lab::new("replica-waits").expect("an exercise");
        lab.extra("The client writes alpha");
        lab.extra("The client writes bravo");
        lab.play(&Move::DeliverAll);
        assert_eq!(
            lab.desk().learned(0),
            Some(1),
            "replica 0 learns both slots"
        );
        assert!(
            !lab.desk().waited(0) && !lab.solved(),
            "in order, it never waits"
        );
    }

    #[test]
    fn a_replica_holds_a_slot_above_its_prefix_and_says_so() {
        let mut lab = Lab::new("replica-waits").expect("an exercise");
        lab.extra("The client writes alpha");
        lab.extra("The client writes bravo");
        lab.settle(|m| !(m.kind == "Commit" && m.to == "r0" && m.slot == Some(0)));
        assert_eq!(lab.desk().learned(0), None, "slot 0 is not learned yet");
        assert!(lab.desk().waited(0));
        assert!(!lab.view().two_values, "two slots are not two values");
        let view = lab.view();
        let r0 = view
            .lanes
            .iter()
            .position(|lane| lane.id == "r0")
            .expect("lane r0");
        assert!(
            view.state[r0]
                .iter()
                .any(|line| line == "holds slot 1, waits"),
            "{:?}",
            view.state[r0]
        );
    }

    #[test]
    fn a_compartments_move_to_a_dead_proxy_is_refused() {
        let mut lab = Lab::new("lost-votes").expect("an exercise");
        lab.play(&Move::Extra { index: 1 });
        assert!(lab.view().refused.is_none(), "an idle proxy may beat");
        let mut lab = Lab::new("proxy-down").expect("an exercise");
        let view = lab.view();
        assert_eq!(view.alive.len(), view.lanes.len());
        let p1 = view
            .lanes
            .iter()
            .position(|lane| lane.id == "p1")
            .expect("lane p1");
        assert!(!view.alive[p1], "proxy leader 1 is down");
        assert!(
            view.lanes.iter().all(|lane| lane.id != "b0"),
            "no batcher lane"
        );
        lab.play(&Move::Deliver { id: 9_999 });
        assert!(lab.view().refused.is_some());
    }

    #[test]
    fn deliver_all_empties_the_wire_in_one_move() {
        let mut lab = Lab::new("majority").expect("an exercise");
        lab.play(&Move::DeliverAll);
        let view = lab.view();
        assert!(view.wire.is_empty(), "the wire is empty");
        assert_eq!(view.moves, 1, "one move");
        assert!(!view.solved, "acceptor 3 heard the ballot");
        assert!(!view.two_values, "one value is chosen");
        lab.play(&Move::Undo);
        assert_eq!(lab.view().moves, 0);
        assert!(!lab.view().wire.is_empty(), "undo puts the wire back");
    }

    #[test]
    fn undo_replays_every_move_but_the_last() {
        let mut lab = Lab::new("vote-not-chosen").expect("an exercise");
        let first = lab.wire()[0].id;
        lab.play(&Move::Deliver { id: first });
        let after_one = lab.view();
        let second = lab.wire()[0].id;
        lab.play(&Move::Drop { id: second });
        assert_eq!(lab.view().moves, 2);
        lab.play(&Move::Undo);
        let undone = lab.view();
        assert_eq!(undone.moves, 1);
        assert_eq!(
            undone.wire.iter().map(|m| m.id).collect::<Vec<_>>(),
            after_one.wire.iter().map(|m| m.id).collect::<Vec<_>>(),
            "the replay gives the same wire"
        );
        lab.play(&Move::Reset);
        assert_eq!(lab.view().moves, 0);
    }

    #[test]
    fn a_refused_move_changes_nothing_and_says_why() {
        let mut lab = Lab::new("vote-not-chosen").expect("an exercise");
        let before = lab.view();
        lab.play(&Move::Deliver { id: 9_999 });
        let after = lab.view();
        assert!(after.refused.is_some(), "the move is refused");
        assert_eq!(after.moves, before.moves);
        lab.play(&Move::Extra { index: 0 });
        assert!(
            lab.view().refused.is_some(),
            "this exercise has no extra move"
        );
    }
}
