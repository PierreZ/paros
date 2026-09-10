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

use std::collections::BTreeMap;

use paros_core::acceptor::{AcceptOutcome, PrepareOutcome};
use paros_core::proposer::RecoveryStep;
use paros_core::{Ballot, Command, MatchmakerId, NodeId, Slot};

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::auto::AutomationFlag;
use crate::view::{show_ballot, show_command};

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

    // ---- the eight kinds ---------------------------------------------------

    /// `Prepare(ballot)` arrived at an acceptor whose promise is `promised`.
    ///
    /// Judged by [`paros_core::acceptor::Acceptor::prepare`] on a clone: a
    /// promise is raised (or re-affirmed) for any ballot **not below** the one
    /// held, and refused below it — or refused, without touching the promise,
    /// when the range starts below the compaction floor.
    #[must_use]
    pub fn acceptor_prepare(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        promised: Ballot,
        floor: Slot,
        outcome: PrepareOutcome,
    ) -> Self {
        let b = show_ballot(ballot);
        let p = show_ballot(promised);
        let expected = match outcome {
            PrepareOutcome::Promised { .. } => "promise",
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => "nack",
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "promise".to_string(),
            match outcome {
                PrepareOutcome::BelowFloor => format!(
                    "A promise here lets the candidate propose a new value into slots that \
                     are already chosen. You would promise ballot {b} for the slots from {}, \
                     but this acceptor truncated everything below slot {}. A Promise reports \
                     the values that it accepted in the range that it covers, and those values \
                     are gone. The candidate would read the silence as \"nothing was ever \
                     accepted here\". Refuse the Prepare: a candidate this far behind must \
                     recover the compacted prefix another way.",
                    from_slot.0, floor.0
                ),
                _ => format!(
                    "A promise here lets one slot get two values. Ballot {b} is *below* the \
                     promise {p} that this acceptor holds. A promise is the only fence in \
                     Paxos. After this acceptor promised {p}, its report to the proposer of \
                     {p} covered every lower ballot for the last time. An answer to {b} now \
                     cancels that report, and ballot {b} can collect a majority and choose a \
                     second value for the same slot. One rule answers both questions: refuse \
                     every ballot below the promise that you hold."
                ),
            },
        );
        explanations.insert(
            "nack".to_string(),
            format!(
                "A refusal here costs the cluster an election that it could win. Ballot {b} \
                 is not below the promise {p} that this acceptor holds, so the refusal is not \
                 unsafe. It is a liveness fault. An *equal* ballot is not a special case. Only \
                 one proposer mints a ballot, so ballot {b} twice is that one proposer that \
                 asks again, and the answer does not change. The acceptor must promise, raise \
                 its durable promise to {b} *before* the reply goes out, and report every \
                 value that it accepted from slot {} up.",
                from_slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::AcceptorPrepare,
            node: node.0,
            question: format!("A Prepare at ballot {b} arrived. Promise, or Nack?"),
            state_summary: vec![
                format!("promised ballot: {p}"),
                format!("the Prepare's ballot: {b}"),
                format!("it covers the slots from {}", from_slot.0),
                format!("compaction floor: slot {}", floor.0),
            ],
            choices: vec![
                Choice::new("promise", format!("Promise {b}")),
                Choice::new("nack", format!("Nack, because I promised {p}")),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// `Accept(ballot, slot)` arrived at an acceptor whose promise is
    /// `promised`.
    ///
    /// Judged by [`paros_core::acceptor::Acceptor::admit`], which needs no
    /// clone at all — it takes `&self`. The rule is the same one the `Prepare`
    /// side uses: anything **below** the promise is refused, and a ballot at
    /// or above it is admitted. An equal ballot is the proposer that already
    /// holds this acceptor's promise coming back for its vote.
    #[must_use]
    pub fn acceptor_accept(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        slot: Slot,
        command: &Command,
        promised: Ballot,
        outcome: AcceptOutcome,
    ) -> Self {
        let b = show_ballot(ballot);
        let p = show_ballot(promised);
        let v = show_command(command);
        let expected = match outcome {
            AcceptOutcome::Admitted => "accept",
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => "nack",
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "accept".to_string(),
            format!(
                "A vote here moves a second value one step nearer to a majority at slot {}. \
                 You would vote for {v} at ballot {b} while this acceptor holds a promise at \
                 {p}. The proposer of {p} used that promise as its fence when it ran its \
                 value-selection rule. It learned that this acceptor held nothing newer, and it \
                 possibly chose a value from that report. Refuse the Accept, and tell {b} which \
                 ballot refused it.",
                slot.0
            ),
        );
        explanations.insert(
            "nack".to_string(),
            format!(
                "A refusal here is a liveness fault. Ballot {b} is not below the promise {p}, \
                 so this vote is safe. A proposer that ran Phase 1 at {b} and got the promise \
                 of this acceptor may have its vote in Phase 2 at the same ballot. Only one \
                 proposer mints a ballot, so an equal ballot is that same proposer again. \
                 Accept the value, and look at the two writes and their order. The acceptor \
                 writes the promise at {b} first, and the record for slot {} second. The record \
                 must not reach the disk above the promise that covers it.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::AcceptorAccept,
            node: node.0,
            question: format!(
                "An Accept at ballot {b} for slot {} arrived, and it carries {v}. Accept, or \
                 Nack?",
                slot.0
            ),
            state_summary: vec![
                format!("promised ballot: {p}"),
                format!("the Accept's ballot: {b}"),
                format!("slot: {}", slot.0),
                format!("value: {v}"),
            ],
            choices: vec![
                Choice::new("accept", format!("Accept {v} at {b}")),
                Choice::new("nack", format!("Nack, because I promised {p}")),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// Phase 1 just completed at `ballot`. Which value goes in the `Accept`?
    ///
    /// Judged by [`paros_core::proposer::Proposer::close_phase1`] on a clone:
    /// the highest-ballot value any promise reported wins, and the proposer's
    /// own value is set aside. This is P2c.
    #[must_use]
    pub fn proposer_value(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        own: &Command,
        reported: Option<&(Ballot, Command)>,
    ) -> Self {
        let b = show_ballot(ballot);
        let mine = show_command(own);
        let mut choices = vec![Choice::new("own", format!("Propose my own {mine}"))];
        let mut explanations = BTreeMap::new();
        let expected = if let Some((at, value)) = reported {
            let rv = show_command(value);
            let ra = show_ballot(*at);
            choices.push(Choice::new(
                "reported",
                format!("Propose {rv}, reported at {ra}"),
            ));
            explanations.insert(
                "own".to_string(),
                format!(
                    "This answer can put two chosen values in slot 0. A promise reported {rv} \
                     accepted at ballot {ra}. You cannot separate that report from \"{rv} is \
                     already chosen\", because a majority of accepts at {ra} shares an acceptor \
                     with your promise majority. If you propose {mine} and the cluster already \
                     chose {rv}, slot 0 holds two different chosen values, and Paxos guarantees \
                     that this cannot occur. Adopt the value reported at the highest ballot. \
                     Your own value waits for a later slot or a later ballot.",
                ),
            );
            explanations.insert("reported".to_string(), String::new());
            "reported"
        } else {
            explanations.insert("own".to_string(), String::new());
            "own"
        };
        let mut state_summary = vec![
            format!("won ballot: {b}"),
            format!("the value of my client: {mine}"),
        ];
        state_summary.push(match reported {
            Some((at, value)) => format!(
                "the highest report from the promise quorum: {} at ballot {}",
                show_command(value),
                show_ballot(*at)
            ),
            None => "the promise quorum reported no accepted value".to_string(),
        });
        Self {
            id,
            kind: PromptKind::ProposerValue,
            node: node.0,
            question: format!("Phase 1 at ballot {b} is complete. Which value goes in the Accept?"),
            state_summary,
            choices,
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A fresh leadership is about to recover `slot`.
    ///
    /// Judged by [`paros_core::proposer::Proposer::recovery_next`] on a clone
    /// of the proposer: `Recovered` means re-propose, `Fill` means decide a
    /// `Noop`, and `Undescribed` — a recovery inherited through a cooperative
    /// handoff, which ran no Phase 1 — means skip.
    #[must_use]
    pub fn leader_recovery(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        slot: Slot,
        step: &RecoveryStep<Command>,
    ) -> Self {
        let b = show_ballot(ballot);
        let expected = match step {
            RecoveryStep::Recovered(_) => "repropose",
            RecoveryStep::Fill => "fill_noop",
            RecoveryStep::Undescribed => "skip",
        };
        let reported = match step {
            RecoveryStep::Recovered(command) => format!(
                "the promise quorum reported {} for slot {}",
                show_command(command),
                slot.0
            ),
            RecoveryStep::Fill => {
                format!("the promise quorum reported nothing for slot {}", slot.0)
            }
            RecoveryStep::Undescribed => format!(
                "the handoff from the last leader did not describe slot {}",
                slot.0
            ),
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "repropose".to_string(),
            format!(
                "There is no value to re-propose here. No acceptor reported anything for slot \
                 {}. The next command of your client would be a *new* proposal at a slot below \
                 the frontier that you give fresh commands. That frontier comes from the \
                 accepted log, so a restart passes over the slot again.",
                slot.0
            ),
        );
        explanations.insert(
            "fill_noop".to_string(),
            match step {
                RecoveryStep::Recovered(command) => format!(
                    "A Noop here decides a *different* value at a slot that an earlier ballot \
                     possibly chose, and that is the double-choose. A promise reported {} for \
                     slot {}. The value-selection rule applies to each slot, and this slot has \
                     a value.",
                    show_command(command),
                    slot.0
                ),
                _ => format!(
                    "A Noop here has no permission behind it. Slot {} came from a cooperative \
                     handoff, not from a Phase 1. A handoff runs no Prepare, so no quorum \
                     report supports the claim \"no node chose anything here\", and a Noop fill \
                     needs that report. Skip the slot: the successor re-proposes only the slots \
                     that the last leader described, and an ordinary election covers the rest.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "skip".to_string(),
            match step {
                RecoveryStep::Recovered(command) => format!(
                    "A skip loses {}. No node proposes slot {} again, and the contiguous \
                     chosen prefix of every node stops one slot below it, permanently. \
                     Re-propose the reported value under your own ballot.",
                    show_command(command),
                    slot.0
                ),
                _ => format!(
                    "A skip of slot {} makes the permanent gap. A new proposal always takes the \
                     frontier, and a restart computes the frontier from the accepted log, so no \
                     node proposes this slot again. The chosen prefix then stops one slot below \
                     it on every node, and the reads stop above it. Catch-up cannot help, \
                     because every node holds the same prefix. Your promise quorum reported \
                     nothing here, and quorum intersection makes that silence a permission. \
                     Some member of *every* majority reports a value that is already chosen. \
                     Fill the slot with a Noop.",
                    slot.0
                ),
            },
        );
        Self {
            id,
            kind: PromptKind::LeaderRecovery,
            node: node.0,
            question: format!("You won ballot {b}. What happens to slot {}?", slot.0),
            state_summary: vec![format!("won ballot: {b}"), reported],
            choices: vec![
                Choice::new("repropose", "Re-propose the reported value"),
                Choice::new("fill_noop", "Fill the slot with a Noop"),
                Choice::new("skip", "Do not touch the slot"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A message just made `slot` chosen at this node. Apply it, or hold?
    ///
    /// `applies_now` is the core's own answer, computed by the caller on a
    /// **clone of the replica**: learn the slot, run the contiguous walk, and
    /// see whether the walk surfaced this slot as committed. The rule it
    /// embodies is the contiguity one — the applied prefix has no holes, so a
    /// slot is executed exactly when the walk reaches it — but the answer
    /// comes from [`paros_core::replica::Replica::advance`], not from a
    /// comparison restated here.
    #[must_use]
    pub fn replica_apply(
        id: u64,
        node: NodeId,
        slot: Slot,
        chosen_index: Option<Slot>,
        first_unchosen: Slot,
        applies_now: bool,
    ) -> Self {
        let at = chosen_index.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = if applies_now { "apply" } else { "hold" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "apply".to_string(),
            format!(
                "This answer puts two nodes in different states. Slot {} is chosen, but slot \
                 {} is not, and the state machine must execute the commands in log order. If \
                 you apply {} now, you pass over {}, and you cannot undo that, because the \
                 application already took the effect of the later command. Hold the slot: it \
                 stays recorded as chosen, and the walk applies it when the hole below it \
                 closes.",
                slot.0, first_unchosen.0, slot.0, first_unchosen.0
            ),
        );
        explanations.insert(
            "hold".to_string(),
            format!(
                "A hold stops the log for no reason. Slot {} is the first slot that the \
                 prefix misses, and the prefix ends at {at}. An apply extends the contiguous \
                 prefix by one slot, and possibly by more. The slots above it that were \
                 already chosen become contiguous as well.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::ReplicaApply,
            node: node.0,
            question: format!("Slot {} is chosen here. Apply it now?", slot.0),
            state_summary: vec![
                format!("applied prefix ends at: {at}"),
                format!("first unchosen slot: {}", first_unchosen.0),
                format!("the newly chosen slot: {}", slot.0),
            ],
            choices: vec![
                Choice::new("apply", format!("Apply slot {}", slot.0)),
                Choice::new("hold", "Hold the slot, because the prefix has a hole"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A drained batch holds both durable writes and outbound messages.
    ///
    /// There is nothing to compute: the answer is always "sync first". It is
    /// the persist-before-send edge, and it is the whole reason the `Ready`
    /// handshake exists.
    #[must_use]
    pub fn persist_order(id: u64, node: NodeId, writes: usize, messages: usize) -> Self {
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "send_first".to_string(),
            format!(
                "A send first is the classic loss of data in Paxos. This batch holds {writes} \
                 durable write(s) and {messages} message(s), and every message is a *claim \
                 about the writes*. A Promise says that the promise of the node is now durable \
                 at that ballot. An Accepted says that the node holds the value on disk. If you \
                 send the messages and the node crashes before the disk write, the node \
                 reboots without a promise that it published. It can also reboot without a vote \
                 that a proposer counted toward a majority. It can then accept a lower ballot \
                 that it refused, \
                 and either fault gives one slot two values. Write the batch to disk, then \
                 send it."
            ),
        );
        Self {
            id,
            kind: PromptKind::PersistOrder,
            node: node.0,
            question: "This batch holds writes and messages. Which goes first?".to_string(),
            state_summary: vec![
                format!("durable writes in the batch: {writes}"),
                format!("messages in the batch: {messages}"),
            ],
            choices: vec![
                Choice::new("sync_first", "Write the batch to disk, then send"),
                Choice::new("send_first", "Send, then write the batch to disk"),
            ],
            expected: "sync_first".to_string(),
            explanations,
            feedback: None,
        }
    }

    /// The cluster says `slot` holds `chosen` at `ballot` — through a `Commit`
    /// or a catch-up replay — while this acceptor's record says `held` at a
    /// lower ballot.
    ///
    /// **The answer is a constant, and deliberately so.** There is no clone to
    /// ask: the core has no "keep the old record" state to be in.
    /// [`paros_core::acceptor::Acceptor::record_accepted`] is an upsert by
    /// slot, and the prompt is only ever raised when what arrived was decided
    /// at a *strictly higher* ballot than the record held, so the choosing
    /// ballot always wins and the answer is always `take`. What the prompt
    /// teaches is the consequence of the other answer, which is why the
    /// `keep` branch carries the whole stale-accept-resurrection story and
    /// `take` carries none.
    #[must_use]
    pub fn commit_overwrite(
        id: u64,
        node: NodeId,
        slot: Slot,
        ballot: Ballot,
        chosen: &Command,
        held_at: Ballot,
        held: &Command,
    ) -> Self {
        let b = show_ballot(ballot);
        let hb = show_ballot(held_at);
        let cv = show_command(chosen);
        let hv = show_command(held);
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "keep".to_string(),
            format!(
                "This answer keeps a record that the cluster already contradicted. {cv} is \
                 *chosen* for slot {}, a quorum decided it at ballot {b}, and {hb} is below \
                 that ballot. If the stale record stays on disk, a restart reads it back as the \
                 accepted value of this acceptor. The next promise quorum can then report {hv} \
                 as the highest accepted value, and a new leader re-proposes it over the chosen \
                 {cv}. That fault is the stale-accept resurrection, and the overwrite makes a \
                 restart safe.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::CommitOverwrite,
            node: node.0,
            question: format!(
                "The cluster says that slot {} holds {cv}, decided at {b}. Your record says \
                 {hv} at {hb}. Which record stays on disk?",
                slot.0
            ),
            state_summary: vec![
                format!("slot: {}", slot.0),
                format!("my own record: {hv} at ballot {hb}"),
                format!("the message that arrived: {cv} chosen at ballot {b}"),
            ],
            choices: vec![
                Choice::new("take", format!("Overwrite with {cv} at {b}")),
                Choice::new("keep", format!("Keep {hv} at {hb}")),
            ],
            expected: "take".to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A read at `ctx` captured `index`; an ack just arrived. Serve, or wait?
    ///
    /// Judged by [`paros_core::proposer::Proposer::confirm_reads`] on a clone,
    /// after crediting this ack: a read confirms only once a **Phase-2 quorum**
    /// has acked a beat broadcast at or after the read began *and* the applied
    /// prefix covers the captured index.
    ///
    /// `acks` is the tally **this node's own vote included**. A read round is
    /// seeded with the leader itself, because a leader is an acceptor of its
    /// own configuration and its own state is the first evidence it has. The
    /// card used to print the peer acks alone, and a player who read "1" and
    /// waited for a third was marked wrong for waiting.
    #[must_use]
    // Every argument is one line of the card, and bundling them would only
    // rename them.
    #[allow(clippy::too_many_arguments)]
    pub fn read_serve(
        id: u64,
        node: NodeId,
        ctx: u64,
        index: Option<Slot>,
        acks: usize,
        members: usize,
        chosen_index: Option<Slot>,
        confirmed: bool,
    ) -> Self {
        let at = index.map_or_else(
            || "the empty prefix".to_string(),
            |s| format!("slot {}", s.0),
        );
        let applied =
            chosen_index.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = if confirmed { "serve" } else { "wait" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "serve".to_string(),
            format!(
                "This answer gives the client whatever this node holds now. The acks in hand \
                 are {acks} of {members}, and that count includes the vote of this node. They \
                 must make a Phase-2 quorum of the configuration of this ballot, for a beat \
                 sent at or after the read started. The applied prefix ({applied}) must also \
                 cover {at}. One of those two conditions does not hold. A leader cannot separate \"my followers are slow\" from \"another \
                 node replaced me and commits without me\". A read on that state gives the \
                 client a value older than a write that the cluster acknowledged to another \
                 client. Wait for the acks of the quorum, because they are the proof and they \
                 need no log write."
            ),
        );
        Self {
            id,
            kind: PromptKind::ReadServe,
            node: node.0,
            question: format!(
                "The read of the client (#{ctx}) captured {at}. Serve it, or \
                 wait?"
            ),
            state_summary: vec![
                format!("read index captured: {at}"),
                format!("the acks, with the vote of this node: {acks} of {members}"),
                format!("the applied prefix ends at: {applied}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Serve the read at {at}")),
                Choice::new("wait", "Wait for the ack quorum"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A peer's snapshot arrived at a node stranded below the cluster's floor.
    /// What is its durable promise afterwards?
    ///
    /// Judged on a **clone of the acceptor**, driven exactly as
    /// `ColocatedNode::on_install_snapshot` drives the real one: raise the
    /// promise to the snapshot's ballot only if that ballot is higher, then
    /// [`paros_core::acceptor::Acceptor::install`]. `promised` is what the
    /// clone holds afterwards, and the offered ballots are matched against it
    /// — so the answer is the core's, not a comparison restated here.
    ///
    /// The two choices are the two concrete ballots that differ: the higher of
    /// the pair, and the other one. When the snapshot's ballot is the lower,
    /// picking it is the mistake the whole level exists for — a snapshot
    /// restores the log, never a promise, and a node that forgot a promise it
    /// had already made is free to vote for a ballot it had sworn to refuse.
    #[must_use]
    pub fn snapshot_promise(
        id: u64,
        node: NodeId,
        at: Slot,
        snapshot_ballot: Ballot,
        held: Ballot,
        promised: Ballot,
    ) -> Self {
        let sb = show_ballot(snapshot_ballot);
        let hb = show_ballot(held);
        let higher = held.max(snapshot_ballot);
        let lower = held.min(snapshot_ballot);
        let expected = if promised == higher {
            "higher"
        } else {
            "lower"
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "lower".to_string(),
            format!(
                "This answer lowers the durable promise of this node to {}. A node must not \
                 take back a promise. When it promised {hb}, it told a proposer that every \
                 lower ballot was finished here, and that proposer possibly chose a value from \
                 that answer. A snapshot restores the *log*: the values, the prefix and the \
                 state of the application. It says nothing about promises, and the peer that \
                 sent it does not know what this node promised. Always take the higher of the \
                 two ballots. For the same reason, a node whose disk was *erased* cannot \
                 rejoin: a snapshot cannot give back a promise that the node no longer holds.",
                show_ballot(lower)
            ),
        );
        Self {
            id,
            kind: PromptKind::SnapshotPromise,
            node: node.0,
            question: format!(
                "A snapshot arrived. It covers every slot up to slot {}, and a node took it \
                 under ballot {sb}. You promised {hb}. What is your promise now?",
                at.0
            ),
            state_summary: vec![
                format!("my own durable promise: {hb}"),
                format!("the ballot of the snapshot: {sb}"),
                format!("the snapshot covers every slot up to slot {}", at.0),
            ],
            choices: vec![
                Choice::new("higher", format!("Promise {}", show_ballot(higher))),
                Choice::new("lower", format!("Promise {}", show_ballot(lower))),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A client retried a write. Which of the three honest answers is this?
    ///
    /// Judged on a **clone of the replica**, through the two ledgers the core
    /// itself consults in that order:
    /// [`applied_at`](paros_core::replica::Replica::applied_at) says the
    /// command is inside this node's applied prefix, so the ack may name its
    /// slot; [`inflight_at`](paros_core::replica::Replica::inflight_at) says
    /// it is chosen or in flight at a slot but not executed here yet, so the
    /// client waits on **that** slot; neither says the node has never seen it,
    /// and it takes a fresh one.
    ///
    /// The two tables move together, and that is the whole lesson: ack from
    /// the wrong one and the client is told a write is durable that no node
    /// has applied, or — worse — the retry misses both and the command is
    /// executed twice.
    #[must_use]
    pub fn ack_write(
        id: u64,
        node: NodeId,
        client: u64,
        seq: u64,
        applied_at: Option<Slot>,
        inflight_at: Option<Slot>,
        chosen_index: Option<Slot>,
    ) -> Self {
        let applied =
            chosen_index.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = match (applied_at, inflight_at) {
            (Some(_), _) => "acked",
            (None, Some(_)) => "inflight",
            (None, None) => "fresh",
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "acked".to_string(),
            format!(
                "This answer is the classic early ack. The applied prefix of this node ends \
                 at {applied}, and no slot in it carries write #{seq} for client {client}. The \
                 ack tells the client that its write is durable and readable. The client then \
                 reads at this same node and does not find the write, because \"chosen\" is not \
                 \"applied\". No node executes a slot decided above a hole until the hole \
                 closes."
            ),
        );
        explanations.insert(
            "inflight".to_string(),
            match applied_at {
                Some(slot) => format!(
                    "This answer makes the client wait for a result that it already has. Write \
                     #{seq} is applied here, at slot {}. If a later duplicate of it sits chosen \
                     and unapplied above, that duplicate executes as a no-op, and the reply \
                     never goes out.",
                    slot.0
                ),
                None => format!(
                    "There is no slot to hold the reply on. This node has no record of write \
                     #{seq} in either table: it is not applied, and it is not in flight."
                ),
            },
        );
        explanations.insert(
            "fresh".to_string(),
            match (applied_at, inflight_at) {
                (Some(slot), _) => format!(
                    "This answer executes the command of the client a *second* time. Write \
                     #{seq} is applied here, at slot {}. At-most-once execution exists to stop \
                     that result, and a second execution is worse than an early ack.",
                    slot.0
                ),
                (None, Some(slot)) => format!(
                    "This answer puts the same command in the log twice. Write #{seq} is chosen \
                     at slot {}, or it is still in flight there, and this node has not executed \
                     it yet. The two dedup tables must move together for that reason. If the \
                     command left the in-flight table before the applied table received it, a \
                     retry in that window would miss both tables.",
                    slot.0
                ),
                (None, None) => String::new(),
            },
        );
        let mut choices = vec![
            Choice::new("acked", "Ack the write, because this node applied it"),
            Choice::new(
                "inflight",
                "Hold the reply on the slot that it is in flight at",
            ),
            Choice::new("fresh", "Give the write the next free slot"),
        ];
        choices.retain(|choice| !choice.id.is_empty());
        Self {
            id,
            kind: PromptKind::AckWrite,
            node: node.0,
            question: format!(
                "Client {client} asks again for its write #{seq}. What do you answer?"
            ),
            state_summary: vec![
                format!("the applied prefix ends at: {applied}"),
                format!(
                    "the applied table says: {}",
                    applied_at.map_or_else(
                        || "nothing for this write".to_string(),
                        |s| format!("applied at slot {}", s.0)
                    )
                ),
                format!(
                    "the in-flight table says: {}",
                    inflight_at.map_or_else(
                        || "nothing for this write".to_string(),
                        |s| format!("in flight at slot {}", s.0)
                    )
                ),
            ],
            choices,
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }
}

impl Prompt {
    // ---- the four Act IV kinds ---------------------------------------------

    /// A grid leader is about to propose into `slot`. Which column takes it?
    ///
    /// Judged by [`paros_core::AcceptorConfig::column_of`] on the
    /// configuration in force: the column is `slot % cols`, a pure function of
    /// the slot. `expected` is that answer, and `columns` is how many the grid
    /// has.
    ///
    /// Every wrong choice here is **safe** — every full column of a grid is a
    /// Phase-2 quorum, and every row meets every column — so the explanation
    /// is about who else has to reach the same answer, not about a value being
    /// lost.
    #[must_use]
    pub fn grid_column(id: u64, node: NodeId, slot: Slot, columns: usize, expected: usize) -> Self {
        let mut choices = Vec::new();
        let mut explanations = BTreeMap::new();
        for column in 0..columns {
            choices.push(Choice::new(
                &format!("column_{column}"),
                format!("Column {column}"),
            ));
            if column == expected {
                continue;
            }
            explanations.insert(
                format!("column_{column}"),
                format!(
                    "This answer breaks the agreement about *which* column takes the slot. \
                     Column {column} is a correct Phase-2 quorum, because every full column of \
                     this grid is one and every row meets every column. No message carries the \
                     column. This leader can stop, and another node then re-proposes slot {}. \
                     This leader can also restart and send its own Accept again. Each of them \
                     computes the column from the slot alone, and each gets column {expected}, \
                     because the rule is the slot {} modulo {columns} columns. Select the column \
                     that the rule gives, and every leader addresses the same acceptors.",
                    slot.0, slot.0
                ),
            );
        }
        Self {
            id,
            kind: PromptKind::GridColumn,
            node: node.0,
            question: format!("Slot {} goes to which column?", slot.0),
            state_summary: vec![
                format!("the slot in the proposal: {}", slot.0),
                format!("the columns in this grid: {columns}"),
                "a Phase-2 quorum here is one full column".to_string(),
            ],
            choices,
            expected: format!("column_{expected}"),
            explanations,
            feedback: None,
        }
    }

    /// A quorum read's row has answered: the highest slot any of them has
    /// voted in is `watermark`, and this node has applied up to `applied`.
    ///
    /// Judged on a clone of the node's own
    /// [`QuorumReads`](paros_core::quorum_read::QuorumReads), through
    /// [`serve`](paros_core::quorum_read::QuorumReads::serve) with the
    /// replica's own `covers`: `served` is what the clone did.
    #[must_use]
    pub fn quorum_read_serve(
        id: u64,
        node: NodeId,
        ctx: u64,
        watermark: Option<Slot>,
        applied: Option<Slot>,
        answered: usize,
        served: bool,
    ) -> Self {
        let high =
            watermark.map_or_else(|| "nothing at all".to_string(), |s| format!("slot {}", s.0));
        let here = applied.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = if served { "serve" } else { "wait" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "serve".to_string(),
            format!(
                "This answer gives the client a state older than a write that it already has. \
                 The highest vote of the row is {high}, and this node applied {here}, so the \
                 prefix does not reach the watermark. One acceptor in that row voted in a slot \
                 that this node did not execute. A write acknowledged before the read started \
                 is possibly that slot. Wait: ordinary replication brings the slot here, \
                 and this node answers the read when the prefix covers it."
            ),
        );
        explanations.insert(
            "wait".to_string(),
            format!(
                "A wait gains nothing here, and no leader takes part. This node applied \
                 {here}, and that prefix already covers the highest vote of the row ({high}). A \
                 Phase-2 quorum chose every write acknowledged before this read started, and \
                 that quorum meets the row that this read asked. The maximum of the row is \
                 therefore at or above that write, and this prefix is at or above the maximum."
            ),
        );
        Self {
            id,
            kind: PromptKind::QuorumReadServe,
            node: node.0,
            question: format!("The row has answered read #{ctx}. Serve it, or wait?"),
            state_summary: vec![
                format!("the acceptors that answered: {answered}"),
                format!("the highest slot that any of them voted in: {high}"),
                format!("this node applied: {here}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Serve the read at {high}")),
                Choice::new("wait", "Wait, because the prefix does not reach it"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A leader's repair probe holds `slot`, whose value one acceptor lost.
    /// Which of the three CTRL cases is this, and what may be re-proposed?
    ///
    /// Judged on a **clone of the proposer**: the arriving `Promise` is folded
    /// through
    /// [`fold_probe_promise`](paros_core::proposer::Proposer::fold_probe_promise)
    /// and the probe is resolved through
    /// [`resolve_probe`](paros_core::proposer::Proposer::resolve_probe). A
    /// decision carrying a value is Case 1, a decision carrying none is Case
    /// 2, and no decision at all is Case 3.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn repair_verdict(
        id: u64,
        node: NodeId,
        slot: Slot,
        reported: Option<&Command>,
        faulty_at: Option<Ballot>,
        expected: RepairCase,
    ) -> Self {
        let value = reported.map(show_command);
        let rotted = faulty_at.map_or_else(
            || "a peer reports no damage".to_string(),
            |ballot| {
                format!(
                    "a peer lost its value for slot {}, which it accepted at ballot {}",
                    slot.0,
                    show_ballot(ballot)
                )
            },
        );
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "case1".to_string(),
            match &value {
                Some(value) => format!(
                    "This is the case where the probe re-proposes {value}. You read this text \
                     because you selected another answer."
                ),
                None => format!(
                    "There is no value to re-propose. No acceptor reported a value for slot {}. \
                     The acceptor that voted there lost the value, and every acceptor that \
                     answered reports no vote there.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "case2".to_string(),
            match &value {
                Some(value) => format!(
                    "A Noop here decides a *different* value at a slot that an earlier ballot \
                     possibly chose. A promise reported {value} for slot {}, at a ballot at or \
                     above the damaged record. This ballot may put only that reported value in \
                     slot {}.",
                    slot.0, slot.0
                ),
                None => format!(
                    "A Noop here can overwrite a chosen value. A Noop is safe only after a full \
                     Phase-1 quorum answers and no answer can hide a chosen value. One acceptor \
                     answers \"I voted in that slot and I do not know the value any more\", and \
                     that answer hides what a Noop overwrites. Slot {} stays undecided until \
                     enough of the other acceptors answer.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "case3".to_string(),
            match &value {
                Some(value) => format!(
                    "A wait gains nothing now. The reports hold {value} for slot {}, accepted \
                     at a ballot at or above the damaged record. A value chosen at or below \
                     that ballot is the same value. A value chosen above it left a record on \
                     a member of the quorum that answered. Re-propose the value, and the \
                     damaged acceptor writes it back when it votes.",
                    slot.0
                ),
                None => format!(
                    "A wait gains nothing now. Enough acceptors answered, so no chosen value \
                     can hide behind the damage. A full Phase-1 quorum reported nothing, or a \
                     record no higher than the record that the probe holds. While you wait, \
                     slot {} stays a hole in the prefix of every node.",
                    slot.0
                ),
            },
        );
        let expected_id = match expected {
            RepairCase::ReproposeReported => "case1",
            RepairCase::FillNoop => "case2",
            RepairCase::Wait => "case3",
        };
        Self {
            id,
            kind: PromptKind::RepairVerdict,
            node: node.0,
            question: format!("What may this ballot put in slot {}?", slot.0),
            state_summary: vec![
                rotted,
                format!(
                    "the highest value that any answer reports for slot {}: {}",
                    slot.0,
                    value.clone().unwrap_or_else(|| "none".to_string())
                ),
            ],
            choices: vec![
                Choice::new(
                    "case1",
                    match &value {
                        Some(value) => format!("Re-propose {value}"),
                        None => "Re-propose the reported value".to_string(),
                    },
                ),
                Choice::new("case2", "Decide a Noop"),
                Choice::new("case3", "Wait for more answers"),
            ],
            expected: expected_id.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A node whose disk was erased asks to come back. Boot it fresh, or
    /// refuse?
    ///
    /// **The answer is a constant, and deliberately so.** There is no role to
    /// clone: the store carries no promise to read back, which is the whole
    /// problem. What decides it is the operator's own record that this
    /// identity was provisioned once, and the library refuses such a boot
    /// outright rather than taking a branch in a state machine. The `refuse`
    /// side therefore carries no explanation and the `boot_fresh` side carries
    /// the whole story.
    #[must_use]
    pub fn wiped_rejoin(id: u64, node: NodeId, promised: Ballot) -> Self {
        let held = show_ballot(promised);
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "boot_fresh".to_string(),
            format!(
                "This answer lets one slot get two values. A fresh boot puts this node back \
                 in the pool with an empty promise, and the node promised {held} before. It now \
                 answers a ballot below {held}, because it holds no record of that promise, and \
                 it votes for the value of that ballot. A proposer already ran Phase 1 at \
                 {held}. That proposer learned that this acceptor held nothing newer, and it \
                 possibly chose a value from that answer. A quorum of this node and the \
                 acceptors at the older ballot then chooses a second value for one slot. A \
                 snapshot does not repair that, because a snapshot restores the log and not a \
                 promise, and no peer knows what this node promised. Refuse the boot: the \
                 cluster changes its acceptor set instead, and that change leaves this identity \
                 out of every quorum."
            ),
        );
        Self {
            id,
            kind: PromptKind::WipedRejoin,
            node: node.0,
            question: format!(
                "The disk of node {} is empty, and the node was a member. Boot it fresh, or \
                 refuse the boot?",
                node.0
            ),
            state_summary: vec![
                format!("the last promise that this node made: {held}"),
                "the disk now holds nothing".to_string(),
                "an operator provisioned this identity once".to_string(),
            ],
            choices: vec![
                Choice::new("refuse", "Refuse the boot"),
                Choice::new("boot_fresh", "Boot the node fresh, as a new node"),
            ],
            expected: "refuse".to_string(),
            explanations,
            feedback: None,
        }
    }
}

impl Prompt {
    // ---- the four matchmaker kinds -----------------------------------------

    /// Promises are in, and the matchmakers named `prior` as the
    /// configurations this ballot must cover. Is Phase 1 complete?
    ///
    /// Judged on a **clone of the proposer**: the arriving `Promise` is folded
    /// into it and
    /// [`phase1_won`](paros_core::proposer::Proposer::phase1_won) answers.
    /// That predicate is "every configuration in `H_b` holds a Phase-1 quorum
    /// of its own", never "the union holds one".
    #[must_use]
    pub fn phase1_complete(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        promised: &[NodeId],
        prior: &[Vec<NodeId>],
        complete: bool,
    ) -> Self {
        let b = show_ballot(ballot);
        let held = show_ids(promised);
        let union: Vec<NodeId> = {
            let mut all: Vec<NodeId> = prior.iter().flatten().copied().collect();
            all.sort_unstable();
            all.dedup();
            all
        };
        let named: Vec<String> = prior
            .iter()
            .enumerate()
            .map(|(index, members)| format!("C{index} = {}", show_ids(members)))
            .collect();
        let listed = if named.is_empty() {
            "no configuration at all".to_string()
        } else {
            named.join(", ")
        };
        let expected = if complete { "complete" } else { "open" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "complete".to_string(),
            format!(
                "This answer lets one slot hold two chosen values. The promises in hand are \
                 {held}, and the matchmakers named {listed}. At least one of those \
                 configurations does not hold a Phase-1 quorum of its own. A quorum of the union \
                 {} is not enough. A large set taken mostly from one configuration is a quorum \
                 of the union, and it still misses a Phase-2 quorum of another configuration. A \
                 value that the other configuration chose then stays hidden, and this ballot \
                 proposes a different value. Ask the configuration that is short: Phase 1 needs \
                 a quorum of every configuration, one configuration at a time.",
                show_ids(&union)
            ),
        );
        explanations.insert(
            "open".to_string(),
            format!(
                "A wait gains nothing here. Every configuration that the matchmakers named \
                 ({listed}) already holds a Phase-1 quorum of its own, and the promises are \
                 {held}. A Phase-2 quorum of one of those configurations chose every value that \
                 an earlier ballot chose. A Phase-1 quorum of that same configuration shares an \
                 acceptor with it, so this candidate learned about the value."
            ),
        );
        Self {
            id,
            kind: PromptKind::Phase1Complete,
            node: node.0,
            question: format!("Is Phase 1 at ballot {b} complete?"),
            state_summary: vec![
                format!("the promises held: {held}"),
                format!("the configurations that the matchmakers named: {listed}"),
                "a quorum of every configuration, not a quorum of their union".to_string(),
            ],
            choices: vec![
                Choice::new("complete", "Phase 1 is complete"),
                Choice::new("open", "Phase 1 is still open"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A matchmaker quorum has answered, and its histories name a
    /// reconfiguration to a configuration this ordinary campaign did not
    /// register. Abandon the campaign, or carry on?
    ///
    /// Judged on the core's own
    /// [`Matchmaking`](paros_core::matchmaking::Matchmaking) role, driven with
    /// the same answers the node is given, through
    /// [`stale_belief`](paros_core::matchmaking::Matchmaking::stale_belief).
    #[must_use]
    pub fn stale_configuration(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        believed: &[NodeId],
        effective: Option<(Ballot, &[NodeId])>,
    ) -> Self {
        let b = show_ballot(ballot);
        let mine = show_ids(believed);
        let expected = if effective.is_some() {
            "abandon"
        } else {
            "carry_on"
        };
        let told = match effective {
            Some((at, members)) => format!(
                "an operator changed the acceptor set to {} at ballot {}",
                show_ids(members),
                show_ballot(at)
            ),
            None => "no operator has changed the acceptor set below this ballot".to_string(),
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "carry_on".to_string(),
            match effective {
                Some((at, members)) => format!(
                    "This answer elects a leader under a set that the cluster already replaced, \
                     and it cancels the change of the operator. This campaign registered {mine}, \
                     and the matchmakers report that an operator put {} in force at ballot {}. \
                     Abandon the campaign, adopt {}, and register it at the next round. The \
                     registration of this campaign stays in the registry, and it costs a later \
                     Phase 1 a few extra promises and nothing else.",
                    show_ids(members),
                    show_ballot(at),
                    show_ids(members)
                ),
                None => String::new(),
            },
        );
        explanations.insert(
            "abandon".to_string(),
            format!(
                "This answer costs an election for nothing. The matchmakers report no \
                 operator change below ballot {b}, so {mine} is the set in force, and this \
                 campaign registered the correct set. Only a **reconfiguration** record decides \
                 here. The registry also holds the set that every earlier candidate believed. A \
                 campaign that adopted the newest belief would exchange beliefs with the next \
                 candidate, one round for each election timeout, without end."
            ),
        );
        Self {
            id,
            kind: PromptKind::StaleConfiguration,
            node: node.0,
            question: format!("A matchmaker quorum answered ballot {b}. What do you do now?"),
            state_summary: vec![
                format!("the set that this campaign registered: {mine}"),
                format!("the histories say: {told}"),
            ],
            choices: vec![
                Choice::new("carry_on", "Open Phase 1 with the set that I registered"),
                Choice::new("abandon", "Abandon the campaign and adopt the set in force"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A registration for one generation reaches a matchmaker that holds
    /// another. Serve it, or refuse it?
    ///
    /// Judged on a **clone of the matchmaker**: the clone is stepped with this
    /// very request and its own reply is read back.
    #[must_use]
    pub fn generation_fence(
        id: u64,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        asked: u64,
        held: u64,
        phase: paros_core::MatchmakerPhase,
        refusal: Option<&paros_core::MatchRefusal>,
    ) -> Self {
        let b = show_ballot(ballot);
        let standing = match phase {
            paros_core::MatchmakerPhase::Active => format!("it serves generation {held}"),
            paros_core::MatchmakerPhase::Stopped => {
                format!("it is frozen for generation {held}")
            }
            paros_core::MatchmakerPhase::Inactive => {
                "it serves no generation, because it is a spare".to_string()
            }
            paros_core::MatchmakerPhase::Fresh => "no node ever wrote anything here".to_string(),
        };
        let expected = if refusal.is_some() { "refuse" } else { "serve" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "serve".to_string(),
            format!(
                "This answer writes a registration into a registry that the cluster no longer \
                 reads. The request addresses generation {asked}, and {standing}. The candidate \
                 then learns that its ballot is safe, but the generation that answers every \
                 later campaign holds no record of it. A configuration that a later history \
                 misses is a configuration whose chosen values no campaign asks about. Refuse \
                 the request and report what you hold: the candidate adopts the set that you \
                 name and asks again."
            ),
        );
        explanations.insert(
            "refuse".to_string(),
            format!(
                "This answer costs the candidate an election for nothing. The request \
                 addresses generation {asked}, and this matchmaker serves that generation. \
                 Register ballot {b}, write the registration to disk, and report the \
                 configurations that you hold below it."
            ),
        );
        Self {
            id,
            kind: PromptKind::GenerationFence,
            node: matchmaker.0,
            question: format!(
                "A registration for generation {asked} arrives. Serve it, or refuse it?"
            ),
            state_summary: vec![
                format!("the generation that the request addresses: {asked}"),
                format!("the state of this matchmaker: {standing}"),
                format!("the ballot that it asks to register: {b}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Register {b}")),
                Choice::new("refuse", "Refuse, and report what I hold"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// An operator asks a node to shut down for good, showing a
    /// garbage-collection watermark. May it retire?
    ///
    /// Judged by [`ColocatedNode::may_retire`](paros_core::ColocatedNode::may_retire)
    /// on the node itself: the call takes `&self` and changes nothing, so there
    /// is nothing to clone.
    #[must_use]
    pub fn may_retire(
        id: u64,
        node: NodeId,
        watermark: Ballot,
        effective: Option<Ballot>,
        member: bool,
        leader: bool,
        may: bool,
    ) -> Self {
        let shown = show_ballot(watermark);
        let held = effective.map_or_else(
            || "no floor is in force yet".to_string(),
            |ballot| format!("the floor in force is {}", show_ballot(ballot)),
        );
        let standing = if leader {
            "it is the leader"
        } else if member {
            "it is a member of the acceptor set in force"
        } else {
            "it is not a member of the acceptor set in force"
        };
        let expected = if may { "retire" } else { "refuse" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "retire".to_string(),
            format!(
                "This answer retires the node on a belief. The operator shows the watermark \
                 {shown}, and {held}. The statement \"I am not in the set in force\" is \
                 volatile. This node loses it at every crash, and it comes back with the set \
                 that it was deployed with. An operator that installed a successor \
                 configuration has not collected the old one. A leader can still ask the Phase-1 \
                 quorum of the old configuration, and it must find the promise of this node. \
                 A retirement needs a watermark strictly above every ballot that bound a \
                 configuration naming this node. Only then does a matchmaker quorum durably \
                 refuse every campaign that could ask. Refuse the request, and answer \
                 \"not collected\"."
            ),
        );
        explanations.insert(
            "refuse".to_string(),
            format!(
                "This answer costs the operator a machine that no node uses again. The \
                 watermark {shown} is above every ballot that bound a configuration naming this \
                 node, {standing}, and this node does not lead. A matchmaker quorum wrote that \
                 floor to disk. No future campaign can register below it, and no future leader \
                 can ask this node for a promise."
            ),
        );
        Self {
            id,
            kind: PromptKind::MayRetire,
            node: node.0,
            question: format!("May node {} retire?", node.0),
            state_summary: vec![
                format!("the state of this node: {standing}"),
                format!("the watermark that the operator shows: {shown}"),
                format!("the leader reports: {held}"),
            ],
            choices: vec![
                Choice::new("retire", "Shut down permanently"),
                Choice::new("refuse", "Refuse, because it is not collected"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }
}

/// A list of node ids, as every player-facing sentence names one.
#[must_use]
fn show_ids(ids: &[NodeId]) -> String {
    let ids: Vec<String> = ids.iter().map(|id| id.0.to_string()).collect();
    format!("{{{}}}", ids.join(", "))
}

/// Which CTRL case a repair probe's answers put a faulty slot in — the shape
/// of [`paros_core::proposer::Proposer::resolve_probe`]'s answer, named for
/// the player.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairCase {
    /// Case 1: a value was reported, and this ballot re-proposes it.
    ReproposeReported,
    /// Case 2: a full Phase-1 quorum of qualifying answers reported no value
    /// at all, so the slot may be decided as a `Noop`.
    FillNoop,
    /// Case 3: not enough answers yet. The slot stays undecided.
    Wait,
}
