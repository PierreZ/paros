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
use paros_core::{Ballot, Command, NodeId, Slot};

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
        }
    }
}

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
                "That is not what `paros-core` does here: its own answer is {:?}.",
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
    /// promise is raised for a ballot at or above the one held, and refused
    /// below it — or refused, without touching the promise, when the range
    /// starts below the compaction floor.
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
                    "You would promise ballot {b} after already promising {p}. A promise is the \
                     only fence Paxos has: having promised {p}, this acceptor's report to \
                     ballot {p}'s proposer was that proposer's *last word* about every lower \
                     ballot. Answering {b} now un-says it — ballot {b} could gather a majority \
                     behind {p}'s back and choose a second value for the same slot. The rule is \
                     strict: promise only a ballot at or above the one held."
                ),
            },
        );
        explanations.insert(
            "nack".to_string(),
            format!(
                "Ballot {b} is at or above the promise {p} held here, so refusing it is not \
                 unsafe — it is a liveness bug. Nothing has been promised that {b} would \
                 violate, and refusing it costs the cluster an election it could have won. \
                 The acceptor promises, raises its durable promise to {b} *before* the reply \
                 leaves, and reports whatever it has accepted from slot {} on.",
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
    /// clone at all — it takes `&self`. A vote is admissible at or above the
    /// promise (`>=`, not `>`: accepting at a ballot *is* promising it).
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
                "Ballot {b} is at or above the promise {p}, so this vote is safe to cast — the \
                 test is `>=`, not `>`. Refusing it is a liveness bug: a proposer that ran \
                 Phase 1 at {b} and got this acceptor's promise is entitled to its vote in \
                 Phase 2 at the same ballot. Accept, and note the two writes and their order: \
                 the promise is raised to {b} first, then the record for slot {} — the record \
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
                 Inventing your client's next command here would be a fresh proposal at a \
                 slot below your allocator frontier — and the frontier is derived from the \
                 accepted log, so a restart would step over the slot again.",
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
                    "Skipping slot {} is exactly the permanent gap. `propose` only ever \
                     allocates the frontier, and a restart recomputes the frontier from the \
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
    /// Judged by [`paros_core::replica::Replica`]'s contiguity rule
    /// (`first_unchosen` / `covers`): the applied prefix is contiguous, so a
    /// slot is applied exactly when it is the first unchosen one.
    #[must_use]
    pub fn replica_apply(
        id: u64,
        node: NodeId,
        slot: Slot,
        chosen_index: Option<Slot>,
        first_unchosen: Slot,
    ) -> Self {
        let at = chosen_index.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = if slot == first_unchosen {
            "apply"
        } else {
            "hold"
        };
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
    /// Judged by [`paros_core::acceptor::Acceptor::record_accepted`]'s
    /// upsert-by-slot contract: the choosing ballot wins, and the stale
    /// lower-ballot record is overwritten.
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
    #[must_use]
    pub fn read_serve(
        id: u64,
        node: NodeId,
        ctx: u64,
        index: Option<Slot>,
        acks: usize,
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
                "The acks in hand ({acks}) are not a Phase-2 quorum of this ballot's \
                 configuration for a beat broadcast at or after the read began, or the applied \
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
            question: format!("The read at ctx {ctx} captured {at}. Serve it, or wait?"),
            state_summary: vec![
                format!("read index captured: {at}"),
                format!("heartbeat acks credited to the round: {acks}"),
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
}
