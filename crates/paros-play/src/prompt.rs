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
            "the promise is raised durably first, and the Promise reports whatever it accepted."
        }
        PromptKind::AcceptorAccept => {
            "the vote is written down before the Accepted that reports it leaves."
        }
        PromptKind::ProposerValue => {
            "a value the promise quorum reported is the only value this ballot may carry."
        }
        PromptKind::LeaderRecovery => {
            "the slot is settled exactly as far as the promise quorum's report licenses."
        }
        PromptKind::ReplicaApply => {
            "the application executes the log in order, and stops at the first hole."
        }
        PromptKind::PersistOrder => "the batch is durable before any claim about it is sent.",
        PromptKind::CommitOverwrite => {
            "the record the choosing ballot decided is what a restart will read back."
        }
        PromptKind::ReadServe => {
            "a read is answered only on a proof of leadership newer than the read itself."
        }
        PromptKind::SnapshotPromise => {
            "a snapshot restores the log and never a promise, so the promise is only ever raised."
        }
        PromptKind::AckWrite => {
            "an ack names a slot this node has really executed, and nothing else does."
        }
        PromptKind::GridColumn => {
            "the slot goes to the column the configuration derives for it, so every node \
             derives the same one."
        }
        PromptKind::QuorumReadServe => {
            "a read is answered only once this node has applied the highest slot the row \
             reported."
        }
        PromptKind::RepairVerdict => {
            "the slot is settled exactly as far as the quorum's reports license, and no further."
        }
        PromptKind::WipedRejoin => {
            "a node that lost its promise does not rejoin, and the acceptor set changes \
             instead."
        }
        PromptKind::Phase1Complete => {
            "Phase 1 is complete only with a quorum of every configuration the matchmakers \
             named, and never with a quorum of their union."
        }
        PromptKind::StaleConfiguration => {
            "a campaign that registered a superseded acceptor set adopts the one in force and \
             starts again."
        }
        PromptKind::GenerationFence => {
            "a matchmaker answers its own generation, and refuses every other one with what it \
             knows."
        }
        PromptKind::MayRetire => {
            "a node retires on evidence that no future leader can need it, never on a belief \
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
                "That is not what the protocol does here: its own answer is {:?}.",
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
                    "You would promise ballot {b} for slots from {}, but this acceptor \
                     truncated everything below slot {}. A Promise reports the values it \
                     accepted in the range it covers; these are gone, so the candidate would \
                     read silence as \"nothing was ever accepted here\" and be free to propose \
                     a fresh value into slots that are already chosen. Refuse instead: a \
                     candidate this far behind must recover the compacted prefix out of band.",
                    from_slot.0, floor.0
                ),
                _ => format!(
                    "Ballot {b} is *below* the promise {p} already held here. A promise is the \
                     only fence Paxos has: having promised {p}, this acceptor's report to \
                     ballot {p}'s proposer was that proposer's *last word* about every lower \
                     ballot. Answering {b} now un-says it — ballot {b} could gather a majority \
                     behind {p}'s back and choose a second value for the same slot. One rule, \
                     both questions: refuse anything below the promise you hold."
                ),
            },
        );
        explanations.insert(
            "nack".to_string(),
            format!(
                "Ballot {b} is not below the promise {p} held here, so refusing it is not \
                 unsafe — it is a liveness bug. Nothing has been promised that {b} would \
                 violate, and refusing it costs the cluster an election it could have won. \
                 (An *equal* ballot is not a puzzle: a ballot is minted by exactly one \
                 proposer, so ballot {b} arriving twice is that one proposer asking again, and \
                 the honest answer is the same answer.) The acceptor promises, raises its \
                 durable promise to {b} *before* the reply leaves, and reports whatever it has \
                 accepted from slot {} on.",
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
                format!("it covers slots from {}", from_slot.0),
                format!("compaction floor: slot {}", floor.0),
            ],
            choices: vec![
                Choice::new("promise", format!("Promise {b}")),
                Choice::new("nack", format!("Nack (I promised {p})")),
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
                "You would vote for {v} at ballot {b} while holding a promise at {p}. That \
                 promise was the fence ballot {p}'s proposer relied on when it ran its own \
                 value-selection rule: it was told this acceptor had nothing newer, and it may \
                 already have chosen a value on the strength of that. A vote at {b} behind {p} \
                 puts a second value one accept closer to a majority for slot {}. Refuse, and \
                 tell {b} which ballot fenced it out.",
                slot.0
            ),
        );
        explanations.insert(
            "nack".to_string(),
            format!(
                "Ballot {b} is not below the promise {p}, so this vote is safe to cast. Refusing \
                 it is a liveness bug: a proposer that ran Phase 1 at {b} and got this \
                 acceptor's promise is entitled to its vote in Phase 2 at the same ballot — a \
                 ballot is minted by one proposer, so \"equal\" always means \"the same \
                 proposer, again\". Accept, and note the two writes and their order: the \
                 promise is re-affirmed at {b} first, then the record for slot {} — the record \
                 must never be durable above the promise that covers it.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::AcceptorAccept,
            node: node.0,
            question: format!(
                "An Accept at ballot {b} for slot {} carrying {v} arrived. Accept, or Nack?",
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
                Choice::new("nack", format!("Nack (I promised {p})")),
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
                    "A promise reported {rv} accepted at ballot {ra}. You cannot tell that \
                     apart from \"{rv} is already chosen\": a majority of accepts at {ra} \
                     shares an acceptor with your promise majority, so the one report you got \
                     is exactly what a chosen value looks like from here. Propose {mine} \
                     instead and, if {rv} really was chosen, slot 0 now holds two different \
                     chosen values — the one thing Paxos promises can never happen. Adopt the \
                     highest-ballot value you were told about; your own value waits for a \
                     later slot, or a later ballot.",
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
            format!("my client's value: {mine}"),
        ];
        state_summary.push(match reported {
            Some((at, value)) => format!(
                "highest report from the promise quorum: {} at ballot {}",
                show_command(value),
                show_ballot(*at)
            ),
            None => "the promise quorum reported nothing accepted".to_string(),
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
                "the predecessor's handoff did not describe slot {} at all",
                slot.0
            ),
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "repropose".to_string(),
            format!(
                "Nothing was reported for slot {}, so there is no value to re-propose. \
                 Inventing your client's next command here would be a *new* proposal at a slot \
                 below the frontier you hand fresh commands out from — and that frontier is \
                 derived from the accepted log, so a restart would step over the slot again.",
                slot.0
            ),
        );
        explanations.insert(
            "fill_noop".to_string(),
            match step {
                RecoveryStep::Recovered(command) => format!(
                    "A promise reported {} for slot {}. Filling a Noop over it decides a \
                     *different* value at a slot some earlier ballot may already have chosen \
                     — the double-choose. The value-selection rule applies per slot, and this \
                     slot has a value.",
                    show_command(command),
                    slot.0
                ),
                _ => format!(
                    "Slot {} came out of a cooperative handoff, not a Phase 1. A handoff runs \
                     no Prepare, so no quorum report licenses the claim \"nobody chose \
                     anything here\" — the licence a Noop fill needs. Skip it: the successor \
                     re-proposes only what the predecessor explicitly described, and an \
                     ordinary election is the fallback for the rest.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "skip".to_string(),
            match step {
                RecoveryStep::Recovered(command) => format!(
                    "Skipping loses {}: nothing would ever propose slot {} again, and every \
                     node's contiguous chosen prefix would freeze one below it — forever. \
                     Re-propose the reported value under your own ballot.",
                    show_command(command),
                    slot.0
                ),
                _ => format!(
                    "Skipping slot {} is exactly the permanent gap. A new proposal only ever \
                     takes the frontier, and a restart recomputes the frontier from the \
                     accepted log, so nothing proposes this slot again: the chosen prefix \
                     freezes one below it cluster-wide, reads are fenced above it, and \
                     commit-replay catch-up cannot help because every node is stuck in the \
                     same place. Your promise quorum reported nothing here, and quorum \
                     intersection turns that into a licence: a value already chosen would have \
                     been reported by some member of *every* majority. Fill a Noop.",
                    slot.0
                ),
            },
        );
        Self {
            id,
            kind: PromptKind::LeaderRecovery,
            node: node.0,
            question: format!("You just won ballot {b}. What happens to slot {}?", slot.0),
            state_summary: vec![format!("won ballot: {b}"), reported],
            choices: vec![
                Choice::new("repropose", "Re-propose the reported value"),
                Choice::new("fill_noop", "Fill the slot with a Noop"),
                Choice::new("skip", "Leave the slot alone"),
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
                "Slot {} is chosen, but slot {} is not, and the state machine has to execute \
                 commands in log order or two nodes end up in different states. Applying {} \
                 now would skip {}, and there is no way back: the application has already \
                 taken the later command's effect. Hold it — it stays recorded as chosen, and \
                 the walk applies it the moment the hole in front of it is filled.",
                slot.0, first_unchosen.0, slot.0, first_unchosen.0
            ),
        );
        explanations.insert(
            "hold".to_string(),
            format!(
                "Slot {} is exactly the first slot the prefix is missing (applied: {at}), so \
                 applying it extends the contiguous prefix by one — and possibly by more, \
                 because slots above it that were already chosen out of order become \
                 contiguous too. Holding it would stall the log for no reason.",
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
                Choice::new("hold", "Hold it: the prefix has a hole"),
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
                "Sending first is the classic Paxos data loss. This batch has {writes} durable \
                 write(s) and {messages} message(s), and every one of those messages is a \
                 *claim about the writes*: a Promise says \"my promise is now durably this \
                 high\", an Accepted says \"this value is durably recorded here\". Send them, \
                 crash before the flush, and the node reboots having forgotten a promise it \
                 published — free to accept a lower ballot it had sworn to refuse — or having \
                 forgotten a vote a proposer already counted toward a majority. Either one \
                 chooses two values for one slot. Flush, then send."
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
                Choice::new("sync_first", "Flush the writes, then send"),
                Choice::new("send_first", "Send, then flush the writes"),
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
                "Keeping {hv} at ballot {hb} keeps a record that is known wrong: {cv} is \
                 *chosen* for slot {}, decided by a quorum at ballot {b}, and {hb} is below \
                 it. Leave the stale record on disk and a restart reads it back as this \
                 acceptor's accepted value; the next election's promise quorum could then \
                 report {hv} as the highest thing anyone accepted, and a fresh leader would \
                 re-propose it over the chosen {cv}. That is the stale-accept resurrection: \
                 the overwrite is not an optimisation, it is what makes restart safe.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::CommitOverwrite,
            node: node.0,
            question: format!(
                "The cluster says slot {} is {cv}, decided at {b}. Your record says {hv} at \
                 {hb}. Which stays on disk?",
                slot.0
            ),
            state_summary: vec![
                format!("slot: {}", slot.0),
                format!("my record: {hv} at ballot {hb}"),
                format!("what arrived: {cv} chosen at ballot {b}"),
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
                "The acks in hand ({acks} of {members}, this node's own vote included) are not a \
                 Phase-2 quorum of this ballot's configuration for a beat broadcast at or after \
                 the read began, or the applied \
                 prefix ({applied}) does not yet cover {at}. Serving now serves whatever this \
                 node happens to hold — and a leader cannot tell \"my followers are slow\" \
                 from \"I was deposed and a newer leader has been committing without me\". A \
                 read served on that state is a read that lies: the client sees a value \
                 older than a write already acknowledged to somebody else. Wait: the quorum's \
                 acks are the proof, and no log write is needed to collect them."
            ),
        );
        Self {
            id,
            kind: PromptKind::ReadServe,
            node: node.0,
            question: format!("The client's read (#{ctx}) captured {at}. Serve it, or wait?"),
            state_summary: vec![
                format!("read index captured: {at}"),
                format!("acks with this node's own vote: {acks} of {members}"),
                format!("applied prefix ends at: {applied}"),
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
                "That lowers this node\'s durable promise to {}. A promise is the one thing a \
                 node may never take back: having promised {hb}, it told some proposer that \
                 every lower ballot was finished here, and that proposer may already have chosen \
                 a value on the strength of it. A snapshot restores the *log* — the values, the \
                 prefix, the application state — and says nothing about promises; the peer that \
                 sent it does not know what this node has sworn. Take the higher of the two, \
                 always. (This is also why a node whose disk was *wiped* can never rejoin: a \
                 snapshot cannot give it back a promise it no longer remembers making.)",
                show_ballot(lower)
            ),
        );
        Self {
            id,
            kind: PromptKind::SnapshotPromise,
            node: node.0,
            question: format!(
                "A snapshot covering everything up to slot {} arrived, taken under ballot {sb}. \
                 You promised {hb}. What is your promise now?",
                at.0
            ),
            state_summary: vec![
                format!("my durable promise: {hb}"),
                format!("the snapshot\'s ballot: {sb}"),
                format!("the snapshot covers everything up to slot {}", at.0),
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
                "Nothing in this node's applied prefix (which ends at {applied}) carries write \
                 #{seq} for client {client}. Acking it anyway is the classic early ack: the \
                 client is told its write is durable and readable, then reads at this very node \
                 a moment later and does not find it — because \"chosen\" is not \"applied\", and \
                 a slot decided above a hole is executed by nobody until the hole closes."
            ),
        );
        explanations.insert(
            "inflight".to_string(),
            match applied_at {
                Some(slot) => format!(
                    "Write #{seq} is already applied here, at slot {}. Parking the reply on an \
                     in-flight slot would make the client wait for something that has already \
                     happened — and if a later duplicate of it is sitting chosen-but-unapplied \
                     somewhere above, that duplicate executes as a no-op and the reply never \
                     fires at all.",
                    slot.0
                ),
                None => format!(
                    "This node has no record of write #{seq} in either table — not applied, not \
                     in flight. There is no slot to park the reply on."
                ),
            },
        );
        explanations.insert(
            "fresh".to_string(),
            match (applied_at, inflight_at) {
                (Some(slot), _) => format!(
                    "Write #{seq} already applied here, at slot {}. Giving it a fresh slot \
                     executes the client\'s command a *second* time — the exact thing \
                     at-most-once execution exists to prevent, and strictly worse than an early \
                     ack.",
                    slot.0
                ),
                (None, Some(slot)) => format!(
                    "Write #{seq} is chosen (or still in flight) at slot {}, it just has not \
                     been executed here yet. Give it a fresh slot and the same command lands \
                     twice in the log. This is exactly why the two dedup tables have to move \
                     together: if \"chosen\" left the in-flight table before \"applied\" \
                     received it, a retry arriving in that window would miss both.",
                    slot.0
                ),
                (None, None) => String::new(),
            },
        );
        let mut choices = vec![
            Choice::new("acked", "Ack it: already applied here"),
            Choice::new("inflight", "Hold the reply on the slot it is in flight at"),
            Choice::new("fresh", "Give it the next free slot"),
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
                format!("applied prefix ends at: {applied}"),
                format!(
                    "the applied ledger says: {}",
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
                    "Column {column} is a perfectly good Phase-2 quorum: every full column of \
                     this grid is one, and every row meets every column, so a value chosen \
                     through any column binds every later ballot. The problem is agreement about \
                     *which* column. Nothing on the wire carries it. If this leader dies and \
                     another node re-proposes slot {}, or if this leader restarts and re-sends \
                     its own Accept, each of them works the column out again from the slot alone \
                     — and each of them gets column {expected}, because the rule is slot \
                     {} modulo {columns} columns. Pick the column the rule gives, and every \
                     incarnation of this leadership addresses the same acceptors.",
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
                format!("the slot being proposed: {}", slot.0),
                format!("columns in this grid: {columns}"),
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
                "The row's highest vote is {high}, and this node has applied {here}. Serving now \
                 answers from a prefix that does not reach the watermark. Some acceptor in that \
                 row voted for a slot this node has not executed, and a write acknowledged before \
                 the read began may be exactly that slot — the client would be shown a state \
                 older than a write it was already promised. Wait: the slot arrives here by \
                 ordinary replication, and the read is answered the moment the prefix covers it."
            ),
        );
        explanations.insert(
            "wait".to_string(),
            format!(
                "This node has applied {here}, which already covers the row's highest vote \
                 ({high}). Every write acknowledged before this read began was chosen by a \
                 Phase-2 quorum, that quorum meets the row this read asked, so the row's maximum \
                 is at or above it — and this prefix is at or above the maximum. Waiting buys \
                 nothing and no leader has to be involved at all."
            ),
        );
        Self {
            id,
            kind: PromptKind::QuorumReadServe,
            node: node.0,
            question: format!("The row has answered read #{ctx}. Serve it, or wait?"),
            state_summary: vec![
                format!("acceptors that answered: {answered}"),
                format!("the highest slot any of them voted in: {high}"),
                format!("this node has applied: {here}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Serve the read at {high}")),
                Choice::new("wait", "Wait: the prefix does not reach it"),
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
                    "a peer lost its value for slot {}, accepted at ballot {}",
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
                    "There is a value to re-propose — {value} — so this is the case where the \
                     probe re-proposes it. (You are reading this because you picked something \
                     else.)"
                ),
                None => format!(
                    "Nobody has reported a value for slot {}. Re-proposing needs a value to \
                     re-propose, and the reports hold none: the acceptor that voted there lost \
                     it, and every acceptor that answered reports no vote there.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "case2".to_string(),
            match &value {
                Some(value) => format!(
                    "A promise reported {value} for slot {}, at a ballot at or above the rotted \
                     record. Deciding a Noop there decides a *different* value at a slot some \
                     earlier ballot may already have chosen. The reported value is the only \
                     thing this ballot may put in slot {}.",
                    slot.0, slot.0
                ),
                None => format!(
                    "A Noop is safe here only when a full Phase-1 quorum has answered and none \
                     of those answers can hide a chosen value. One acceptor's answer is \"I voted \
                     at that slot and I no longer know what for\", and that answer hides exactly \
                     what a Noop would overwrite. Until enough of the others answer, slot {} \
                     stays undecided.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "case3".to_string(),
            match &value {
                Some(value) => format!(
                    "The reports now hold {value} for slot {}, accepted at a ballot at or above \
                     the rotted record. That is enough: a value chosen at or below that ballot is \
                     the same value, and a value chosen above it would have left a record on some \
                     member of the quorum that answered. Re-propose it and the damaged acceptor \
                     writes the value back as it votes.",
                    slot.0
                ),
                None => format!(
                    "Enough acceptors have now answered that no chosen value can be hiding \
                     behind the damage: a full Phase-1 quorum reported either nothing or a record \
                     no higher than what the probe holds. Waiting longer settles nothing, and \
                     slot {} stays a hole in every node's prefix while you do.",
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
                    "the highest value any answer reports for slot {}: {}",
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
                "A fresh boot puts this node back in the pool with an empty promise. It had \
                 promised {held}. It now answers a ballot below {held}, because it has no memory \
                 of refusing one, and it votes for whatever that ballot proposes. Some proposer \
                 already ran Phase 1 at {held}. That proposer was told this acceptor held \
                 nothing newer, and it may have chosen a value on the strength of that answer. A \
                 quorum of this node and the acceptors behind the older ballot then chooses a \
                 second value for one slot. A snapshot does not repair it: a snapshot restores \
                 the log and not a promise, and no peer knows what this node has promised. \
                 Refuse the boot. The cluster changes its acceptor set instead, and that \
                 decision leaves this identity out of every quorum."
            ),
        );
        Self {
            id,
            kind: PromptKind::WipedRejoin,
            node: node.0,
            question: format!(
                "Node {}'s disk is empty, and it was a member. Boot it fresh, or refuse?",
                node.0
            ),
            state_summary: vec![
                format!("the promise this node last made: {held}"),
                "what its disk holds now: nothing at all".to_string(),
                "an operator provisioned this identity once".to_string(),
            ],
            choices: vec![
                Choice::new("refuse", "Refuse the boot"),
                Choice::new("boot_fresh", "Boot it fresh, as a new node"),
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
                "The promises in hand are {held}, and the matchmakers named {listed}. At least \
                 one of those configurations does not hold a Phase-1 quorum of its own. It is not \
                 enough that the promises are a quorum of the union {}: a large set drawn mostly \
                 from one configuration is a quorum of the union and still misses a Phase-2 \
                 quorum of another. A value that other configuration already chose then stays \
                 invisible, this ballot proposes a different one, and one slot holds two chosen \
                 values. Ask the configuration that is short. Phase 1 needs a quorum of every \
                 configuration, one at a time.",
                show_ids(&union)
            ),
        );
        explanations.insert(
            "open".to_string(),
            format!(
                "Every configuration the matchmakers named — {listed} — already holds a Phase-1 \
                 quorum of its own, and the promises are {held}. Waiting longer buys nothing. \
                 Anything an earlier ballot chose was chosen by a Phase-2 quorum of one of those \
                 configurations, and a Phase-1 quorum of that same configuration shares an \
                 acceptor with it, so this candidate has been told about it."
            ),
        );
        Self {
            id,
            kind: PromptKind::Phase1Complete,
            node: node.0,
            question: format!("Is Phase 1 at ballot {b} complete?"),
            state_summary: vec![
                format!("promises held: {held}"),
                format!("configurations the matchmakers named: {listed}"),
                "a quorum of every one of them, never a quorum of their union".to_string(),
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
                    "This campaign registered {mine}, and the matchmakers report that an operator \
                     put {} in force at ballot {}. Carrying on elects a leader under a set the \
                     cluster has already replaced, and that rolls the operator's change back \
                     without anybody asking. Abandon the campaign, adopt {}, and register it at \
                     the next round. The registration this campaign made stays in the registry \
                     and costs a later Phase 1 a few extra promises; it costs nothing else.",
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
                "The matchmakers report no operator change below ballot {b}, so {mine} is the set \
                 in force and this campaign registered the right one. Abandoning it costs an \
                 election for nothing. Only a **reconfiguration** record decides here. The \
                 registry also holds what every earlier candidate merely believed, and a campaign \
                 that adopted the newest belief would swap beliefs with the next candidate, one \
                 round per election timeout, for ever."
            ),
        );
        Self {
            id,
            kind: PromptKind::StaleConfiguration,
            node: node.0,
            question: format!("A matchmaker quorum has answered ballot {b}. What now?"),
            state_summary: vec![
                format!("the set this campaign registered: {mine}"),
                format!("what the histories say: {told}"),
            ],
            choices: vec![
                Choice::new("carry_on", "Open Phase 1 with the set I registered"),
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
                "it serves no generation: it is a spare".to_string()
            }
            paros_core::MatchmakerPhase::Fresh => "nothing has ever been written here".to_string(),
        };
        let expected = if refusal.is_some() { "refuse" } else { "serve" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "serve".to_string(),
            format!(
                "This request addresses generation {asked}, and {standing}. Serving it writes a \
                 registration into a registry the cluster has stopped reading. A candidate would \
                 then be told its ballot is safe, while the generation that answers every later \
                 campaign has never heard of it — and a configuration missing from a later \
                 history is a configuration whose chosen values nobody asks about. Refuse, and \
                 say what you know: the candidate adopts the set you name and asks again."
            ),
        );
        explanations.insert(
            "refuse".to_string(),
            format!(
                "This request addresses generation {asked}, which is exactly the generation this \
                 matchmaker serves. Refusing it costs the candidate an election for nothing. \
                 Register ballot {b}, write it down, and report the configurations you hold below \
                 it."
            ),
        );
        Self {
            id,
            kind: PromptKind::GenerationFence,
            node: matchmaker.0,
            question: format!("A registration for generation {asked} arrives. Serve, or refuse?"),
            state_summary: vec![
                format!("the generation the request addresses: {asked}"),
                format!("where this matchmaker stands: {standing}"),
                format!("the ballot it asks to register: {b}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Register {b}")),
                Choice::new("refuse", "Refuse, and say what I hold"),
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
                "The watermark shown is {shown}, and {held}. Retiring on that is retiring on a \
                 belief. \"I am not in the set in force\" is volatile: this node loses it at every \
                 crash and comes back believing the set it was deployed with. An operator that \
                 installed a successor configuration has not collected the old one — the old \
                 configuration's Phase-1 quorum can still be asked, and a leader that asks it \
                 must find this node's promise. What licenses a retirement is a watermark \
                 strictly above every ballot a configuration naming this node was bound to: only \
                 then does a matchmaker quorum durably refuse every campaign that could ask. \
                 Refuse, and answer \"not collected\"."
            ),
        );
        explanations.insert(
            "refuse".to_string(),
            format!(
                "The watermark {shown} is above every ballot a configuration naming this node was \
                 bound to, {standing}, and it does not lead. A matchmaker quorum wrote that floor \
                 down, so no future campaign can register below it and no future leader can ask \
                 this node for a promise. Refusing costs the operator a machine that will never \
                 be used again."
            ),
        );
        Self {
            id,
            kind: PromptKind::MayRetire,
            node: node.0,
            question: format!("May node {} retire?", node.0),
            state_summary: vec![
                format!("where this node stands: {standing}"),
                format!("the watermark the operator shows: {shown}"),
                format!("what the leader reports: {held}"),
            ],
            choices: vec![
                Choice::new("retire", "Shut down for good"),
                Choice::new("refuse", "Refuse: not collected"),
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
