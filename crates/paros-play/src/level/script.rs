//! Writing a reference solution without counting message ids by hand.
//!
//! A level's `reference` is a `Vec<Action>` — a flat, replayable list, which is
//! exactly what the tests want and what an "show me the solution" button would
//! play back. Producing that list by hand is another matter: an Act II level is
//! fifty deliveries deep, every id is assigned in the order the engine happens
//! to queue a batch's messages, and one extra `Heartbeat` renumbers the rest.
//!
//! So a reference is *recorded* rather than written. [`Script`] drives a real
//! [`Game`] of the level, choosing messages by what they **are** (a `Prepare`, a
//! slot-1 `Accept`, anything at all) and answering every prompt with the answer
//! `paros-core` itself gives, and hands back the exact list of [`Action`]s it
//! played. The list is still a plain `Vec<Action>`: nothing about the recording
//! survives into the level.
//!
//! Two properties this buys, and they are the reason it is worth a module:
//!
//! - a reference cannot drift out of date when the engine queues one more
//!   message, because it never named an id in the first place;
//! - a reference cannot teach the wrong answer, because
//!   [`Script::answer_prompt`] reads the expected answer off the prompt, which
//!   computed it on a clone of the real role.

use crate::action::Action;
use crate::view::MessageView;
use crate::{Game, GoalStatus};

/// How many steps one recording may take before it is called a loop.
const BUDGET: usize = 4096;

/// A recorder: it plays a level and remembers what it played.
pub(crate) struct Script {
    game: Game,
    actions: Vec<Action>,
}

impl Script {
    /// Start recording `level_id` from its setup.
    pub(crate) fn new(level_id: &str) -> Self {
        Self {
            game: Game::new(level_id).expect("a registered level"),
            actions: Vec::new(),
        }
    }

    /// Play one action and record it.
    pub(crate) fn play(&mut self, action: Action) -> &mut Self {
        let label = action.label();
        self.game
            .act(action.clone())
            .unwrap_or_else(|err| panic!("the reference step {label} was refused: {err}"));
        self.actions.push(action);
        self
    }

    /// Everything in flight, as the wire list shows it.
    pub(crate) fn wire(&self) -> Vec<MessageView> {
        self.game.view().world.wire
    }

    /// The world as it stands, for a recording that has to read a fact off it
    /// — the garbage-collection watermark an operator passes to a `Retire`, for
    /// one. Reading is not playing: nothing here is recorded.
    pub(crate) fn world(&self) -> &crate::WorldKind {
        self.game.world()
    }

    /// Whether a prompt is open.
    fn prompt_open(&self) -> bool {
        self.game.view().prompt.is_some()
    }

    /// Answer the open prompt with the answer `paros-core` itself gives.
    pub(crate) fn answer_prompt(&mut self) -> &mut Self {
        let (id, choice) = {
            let prompt = self.game.world().prompt().expect("a prompt is open");
            (prompt.id, prompt.expected().to_string())
        };
        self.play(Action::Answer { prompt: id, choice })
    }

    /// Deliver the lowest-id message `keep` accepts, if there is one.
    pub(crate) fn deliver_one(&mut self, keep: &impl Fn(&MessageView) -> bool) -> bool {
        let Some(id) = self
            .wire()
            .iter()
            .filter(|message| keep(message))
            .map(|message| message.id)
            .min()
        else {
            return false;
        };
        self.play(Action::Deliver { id });
        true
    }

    /// Deliver every message `keep` accepts, lowest id first, answering every
    /// prompt on the way, until nothing matching is left.
    pub(crate) fn settle(&mut self, keep: impl Fn(&MessageView) -> bool) -> &mut Self {
        for _ in 0..BUDGET {
            if self.prompt_open() {
                self.answer_prompt();
                continue;
            }
            if !self.deliver_one(&keep) {
                return self;
            }
        }
        panic!("a reference recording reaches quiescence");
    }

    /// Deliver everything, answering every prompt.
    pub(crate) fn settle_all(&mut self) -> &mut Self {
        self.settle(|_| true)
    }

    /// Answer every prompt that is open, delivering nothing.
    pub(crate) fn answer_all(&mut self) -> &mut Self {
        for _ in 0..BUDGET {
            if !self.prompt_open() {
                return self;
            }
            self.answer_prompt();
        }
        panic!("a reference recording answers finitely many prompts");
    }

    /// Drop every message `hit` accepts — the partition the player never heals.
    pub(crate) fn drop_all(&mut self, hit: impl Fn(&MessageView) -> bool) -> &mut Self {
        for _ in 0..BUDGET {
            let Some(id) = self
                .wire()
                .iter()
                .filter(|message| hit(message))
                .map(|message| message.id)
                .min()
            else {
                return self;
            };
            self.play(Action::Drop { id });
        }
        panic!("a reference recording drops finitely many messages");
    }

    /// The recorded list, checked to have actually finished the level.
    pub(crate) fn finish(self) -> Vec<Action> {
        assert!(
            matches!(self.game.goal(), GoalStatus::Reached(_)),
            "a reference solution reaches its level's goal, and this one left it at {:?}",
            self.game.goal()
        );
        assert_eq!(
            self.game.mistakes(),
            0,
            "a reference solution answers every prompt correctly"
        );
        self.actions
    }
}

// ---- the predicates a reference reads for ----------------------------------

/// Every message of one wire family (`prepare`, `accept`, `heartbeat`, …).
pub(crate) fn phase(name: &'static str) -> impl Fn(&MessageView) -> bool {
    move |message| message.phase == name
}

/// Every message of one variant (`Prepare`, `Accept`, `Commit`, …).
pub(crate) fn kind(name: &'static str) -> impl Fn(&MessageView) -> bool {
    move |message| message.kind == name
}

/// Every message of one variant naming one slot.
pub(crate) fn kind_at(name: &'static str, slot: u64) -> impl Fn(&MessageView) -> bool {
    move |message| message.kind == name && message.slot == Some(slot)
}

/// Everything addressed to `node`.
pub(crate) fn to(node: u64) -> impl Fn(&MessageView) -> bool {
    move |message| message.to == node
}

/// Everything not addressed to any of `nodes` — the isolation a level builds by
/// simply never delivering.
pub(crate) fn not_to(nodes: &'static [u64]) -> impl Fn(&MessageView) -> bool {
    move |message| !nodes.contains(&message.to)
}
