//! Playing a role: the questions a manual role is asked, and the judge.
//!
//! When a message reaches a node whose role is manual, the world does **not**
//! step it: it parks the message, raises a [`Prompt`], and waits. The prompt
//! shows the state the decision rests on and the choices; the player answers;
//! and the engine compares the answer against what `paros-core` itself would
//! do — computed on a **clone of the role** (`Acceptor`, `Proposer` and
//! `Replica` are all `Clone`, and the node hands them out through
//! `acceptor()` / `proposer()` / `replica()`), so the real node never takes
//! the wrong branch.
//!
//! A wrong answer is therefore never a state the world enters. It costs a
//! mistake and an explanation: the authored consequence, instantiated with
//! this prompt's own ballots and values. That is the only place in the game
//! where a rule is written down twice — once as the core's code, once as the
//! sentence that says what breaks without it — and it is deliberate: the
//! explanation is prose about the core, not a second implementation of it.
//!
//! Every answer is computed **at raise time**. The world is frozen while a
//! prompt is open (the automation pump stops, every other action is refused),
//! so the answer computed then is the answer at answer time.

mod acceptor;
mod matchmaker;
mod proposer;
mod reads;
mod replica;
mod storage;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::auto::AutomationFlag;

pub use storage::RepairCase;

/// Which question a [`Prompt`] asks. Each kind is governed by one
/// [`AutomationFlag`]: the prompt is raised exactly while that flag is off.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum PromptKind {
    /// A `Prepare` arrived. Promise or Nack?
    AcceptorPrepare,
    /// An `Accept` arrived. Accept or Nack?
    AcceptorAccept,
    /// Phase 1 is complete. Which value goes in the `Accept`?
    ProposerValue,
    /// A fresh leadership is recovering a slot. Re-propose, fill, or skip?
    LeaderRecovery,
    /// A slot just became chosen here. Apply it, or hold?
    ReplicaApply,
    /// A batch holds both writes and messages. Sync first or send first?
    PersistOrder,
    /// A `Commit` contradicts a record held at a lower ballot. Keep or take?
    CommitOverwrite,
    /// A read's index is captured and the acks are in. Serve, or wait?
    ReadServe,
    /// A peer's snapshot arrived. What is this node's promise afterwards?
    SnapshotPromise,
    /// A client retried a write. Acked as applied, held in flight, or fresh?
    AckWrite,
    /// A grid leader is about to propose. Which column takes this slot?
    GridColumn,
    /// A quorum read's row has answered. Serve it, or wait?
    QuorumReadServe,
    /// A repair probe holds a faulty slot. Which CTRL case is it?
    RepairVerdict,
    /// A wiped node asks to rejoin. Boot it fresh, or refuse?
    WipedRejoin,
    /// Promises are in and `H_b` names one or more configurations. Is the
    /// cross-configuration Phase 1 complete?
    Phase1Complete,
    /// The matchmakers name a configuration this campaign did not register.
    /// Abandon the campaign, or carry on?
    StaleConfiguration,
    /// A registration for one generation reaches a matchmaker at another.
    /// Serve it, or refuse it?
    GenerationFence,
    /// An operator asks a node to shut down for good. May it?
    MayRetire,
}

impl PromptKind {
    /// The automation flag that answers this prompt when it is on.
    #[must_use]
    pub fn flag(self) -> AutomationFlag {
        match self {
            PromptKind::AcceptorPrepare | PromptKind::AcceptorAccept => {
                AutomationFlag::AcceptorReplies
            }
            PromptKind::CommitOverwrite => AutomationFlag::CommitOverwrite,
            PromptKind::ProposerValue => AutomationFlag::ProposerP2c,
            PromptKind::LeaderRecovery => AutomationFlag::LeaderRecovery,
            PromptKind::ReplicaApply => AutomationFlag::ReplicaApply,
            PromptKind::PersistOrder => AutomationFlag::PersistOrder,
            PromptKind::ReadServe => AutomationFlag::ReadServe,
            PromptKind::SnapshotPromise => AutomationFlag::SnapshotPromise,
            PromptKind::AckWrite => AutomationFlag::AckWrite,
            PromptKind::GridColumn => AutomationFlag::GridColumn,
            PromptKind::QuorumReadServe => AutomationFlag::QuorumReadServe,
            PromptKind::RepairVerdict => AutomationFlag::RepairVerdict,
            PromptKind::WipedRejoin => AutomationFlag::WipedRejoin,
            PromptKind::Phase1Complete => AutomationFlag::Phase1Complete,
            PromptKind::StaleConfiguration => AutomationFlag::StaleConfiguration,
            PromptKind::GenerationFence => AutomationFlag::GenerationFence,
            PromptKind::MayRetire => AutomationFlag::MayRetire,
        }
    }
}

/// Every prompt kind, in the order the levels introduce them. The
/// [`crate::Game`] derives the world's manual set from this, and the tests
/// turn a level's pinned-off flags into the questions it promises to ask.
pub const ALL_PROMPTS: &[PromptKind] = &[
    PromptKind::AcceptorPrepare,
    PromptKind::AcceptorAccept,
    PromptKind::ProposerValue,
    PromptKind::LeaderRecovery,
    PromptKind::ReplicaApply,
    PromptKind::PersistOrder,
    PromptKind::CommitOverwrite,
    PromptKind::ReadServe,
    PromptKind::SnapshotPromise,
    PromptKind::AckWrite,
    PromptKind::GridColumn,
    PromptKind::QuorumReadServe,
    PromptKind::RepairVerdict,
    PromptKind::WipedRejoin,
    PromptKind::Phase1Complete,
    PromptKind::StaleConfiguration,
    PromptKind::GenerationFence,
    PromptKind::MayRetire,
];

/// What a **right** answer confirms, in one clause. The narration says it back
/// so the player reads the rule rather than only "correct".
#[must_use]
pub fn confirmation(kind: PromptKind) -> &'static str {
    match kind {
        PromptKind::AcceptorPrepare => {
            "the acceptor raises its promise on disk first, and the Promise then reports \
             every value that it accepted."
        }
        PromptKind::AcceptorAccept => {
            "the acceptor writes the vote to disk before it sends the Accepted that reports \
             the vote."
        }
        PromptKind::ProposerValue => {
            "this ballot may carry only a value that the promise quorum reported."
        }
        PromptKind::LeaderRecovery => {
            "the report of the promise quorum settles the slot, and it settles the slot no \
             further."
        }
        PromptKind::ReplicaApply => {
            "the application executes the log in order, and it stops at the first hole."
        }
        PromptKind::PersistOrder => {
            "the batch is on disk before the node sends any claim about it."
        }
        PromptKind::CommitOverwrite => {
            "a restart reads back the record that the choosing ballot decided."
        }
        PromptKind::ReadServe => {
            "a node answers a read only with a proof of leadership newer than the read."
        }
        PromptKind::SnapshotPromise => {
            "a snapshot restores the log and not a promise, so the promise only goes up."
        }
        PromptKind::AckWrite => {
            "an ack names a slot that this node executed, and it names nothing else."
        }
        PromptKind::GridColumn => {
            "the slot goes to the column that the configuration computes for it, so every \
             node computes the same column."
        }
        PromptKind::QuorumReadServe => {
            "this node answers a read only after it applies the highest slot that the row \
             reported."
        }
        PromptKind::RepairVerdict => {
            "the reports of the quorum settle the slot, and they settle the slot no further."
        }
        PromptKind::WipedRejoin => {
            "a node that lost its promise does not rejoin, and the acceptor set changes \
             instead."
        }
        PromptKind::Phase1Complete => {
            "Phase 1 is complete only with a quorum of every configuration that the \
             matchmakers named, and not with a quorum of their union."
        }
        PromptKind::StaleConfiguration => {
            "a campaign that registered a superseded acceptor set adopts the set in force and \
             starts again."
        }
        PromptKind::GenerationFence => {
            "a matchmaker answers its own generation, and it refuses every other generation \
             with what it knows."
        }
        PromptKind::MayRetire => {
            "a node retires on evidence that no future leader needs it, and not on a belief \
             about the set in force."
        }
    }
}

/// One answer the player may pick.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Choice {
    /// The stable id an [`crate::Action::Answer`] names.
    pub id: String,
    /// What the button says.
    pub label: String,
}

impl Choice {
    fn new(id: &str, label: impl Into<String>) -> Self {
        Self {
            id: id.to_string(),
            label: label.into(),
        }
    }
}

/// A question the world is waiting on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prompt {
    /// A monotone id, so an answer cannot race a re-render.
    pub id: u64,
    /// Which question.
    pub kind: PromptKind,
    /// The node whose role is being played.
    pub node: u64,
    /// The question, with this prompt's own numbers in it.
    pub question: String,
    /// The state the decision rests on, one fact per line.
    pub state_summary: Vec<String>,
    /// The answers on offer.
    pub choices: Vec<Choice>,
    /// What `paros-core` answers — computed on a clone of the role.
    expected: String,
    /// Per **wrong** choice, the violation it would cause. The right answer
    /// needs no entry: nothing is explained when nothing broke.
    explanations: BTreeMap<String, String>,
    /// The explanation for the last wrong answer, if any.
    pub feedback: Option<String>,
}

/// What answering did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The player matched the core: the world may advance.
    Right,
    /// The player did not: the prompt stays open and `feedback` is set.
    Wrong,
}

impl Prompt {
    /// Whether `choice` is one this prompt offers.
    #[must_use]
    pub fn offers(&self, choice: &str) -> bool {
        self.choices.iter().any(|c| c.id == choice)
    }

    /// Judge `choice`. On [`Verdict::Wrong`] the authored explanation for that
    /// choice becomes [`Prompt::feedback`].
    pub fn judge(&mut self, choice: &str) -> Verdict {
        if choice == self.expected {
            self.feedback = None;
            return Verdict::Right;
        }
        // Every wrong choice is authored; the fallback exists so a future
        // prompt kind cannot ship a silent refusal.
        self.feedback = Some(self.explanations.get(choice).cloned().unwrap_or_else(|| {
            format!(
                "The protocol does not do that here. Its own answer is {:?}.",
                self.expected
            )
        }));
        Verdict::Wrong
    }

    /// The core's own answer — the tests read it to drive a reference solution
    /// without hard-coding a choice per prompt.
    #[must_use]
    pub fn expected(&self) -> &str {
        &self.expected
    }
}
