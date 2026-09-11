//! How the matchmaker plane draws: one [`MessageView`] per wire entry, the
//! clause a refusal carries, and which tier each end of the wire belongs to.

use paros_core::{MatchOutcome, NodeId, ReconfigureReply, ReconfigureRequest};

use crate::narration::many;
use crate::view::{MessageView, PartyView, show_ballot};
use crate::world::matchmakers::{show_members, show_set};
use crate::world::{Envelope, InFlight, Party};

/// Render one matchmaker-plane message for the wire list and the stage.
#[must_use]
#[allow(clippy::too_many_lines)]
pub(crate) fn plane_view(entry: &InFlight) -> Option<MessageView> {
    let (kind, phase, ballot, summary, reply) = match &entry.envelope {
        Envelope::Node(_) => return None,
        Envelope::Match(request) => (
            "MatchRequest",
            "match",
            Some(request.ballot),
            format!(
                "Register {} with {}, generation {}",
                show_ballot(request.ballot),
                show_members(request.config.members()),
                request.generation.0
            ),
            false,
        ),
        Envelope::MatchReply(answer) => (
            "MatchReply",
            "match",
            Some(answer.ballot),
            match &answer.outcome {
                MatchOutcome::Registered { history, .. } => format!(
                    "Registered {}: {} below it",
                    show_ballot(answer.ballot),
                    many(history.len(), "configuration")
                ),
                MatchOutcome::Refused(refusal) => {
                    format!("Refused {}: {}", show_ballot(answer.ballot), why(refusal))
                }
            },
            true,
        ),
        Envelope::Gc(request) => (
            "GcRequest",
            "gc",
            Some(request.watermark),
            format!("Raise the watermark to {}", show_ballot(request.watermark)),
            false,
        ),
        Envelope::GcAck(ack) => (
            "GcAck",
            "gc",
            Some(ack.watermark),
            if ack.applied {
                format!("The watermark is now {}", show_ballot(ack.watermark))
            } else {
                format!(
                    "Refused: the watermark stays at {}",
                    show_ballot(ack.watermark)
                )
            },
            true,
        ),
        Envelope::Reconfigure(request) => {
            let (kind, summary) = match request {
                ReconfigureRequest::Stop { generation, .. } => {
                    ("Stop", format!("Freeze generation {}", generation.0))
                }
                ReconfigureRequest::Bootstrap { bootstrap, .. } => (
                    "Bootstrap",
                    format!(
                        "Hold generation {} = {} pending, with {}",
                        bootstrap.set.generation.0,
                        show_set(bootstrap.set.members()),
                        many(bootstrap.history.len(), "registration")
                    ),
                ),
                ReconfigureRequest::DecreePrepare { ballot, .. } => (
                    "DecreePrepare",
                    format!("Prepare {} over the one decree slot", show_ballot(*ballot)),
                ),
                ReconfigureRequest::DecreeAccept {
                    ballot, members, ..
                } => (
                    "DecreeAccept",
                    format!(
                        "Accept {} at {} in the one decree slot",
                        show_set(members),
                        show_ballot(*ballot)
                    ),
                ),
                ReconfigureRequest::Chosen { successor, .. } => (
                    "Chosen",
                    format!(
                        "Generation {} = {} is chosen",
                        successor.generation.0,
                        show_set(successor.members())
                    ),
                ),
            };
            (kind, "reconfigure", None, summary, false)
        }
        Envelope::ReconfigureReply(answer) => {
            let (kind, summary) = match answer {
                ReconfigureReply::Stopped {
                    generation,
                    history,
                    ..
                } => (
                    "Stopped",
                    format!(
                        "Frozen for generation {}, handing over {}",
                        generation.0,
                        many(history.len(), "registration")
                    ),
                ),
                ReconfigureReply::Bootstrapped { set, .. } => (
                    "Bootstrapped",
                    format!("Generation {} is held pending here", set.generation.0),
                ),
                ReconfigureReply::Promised { ballot, vote, .. } => (
                    "Promised",
                    match vote {
                        Some((at, members)) => format!(
                            "Promised {}; it already voted {} at {}",
                            show_ballot(*ballot),
                            show_set(members),
                            show_ballot(*at)
                        ),
                        None => format!(
                            "Promised {}; it has voted for nothing",
                            show_ballot(*ballot)
                        ),
                    },
                ),
                ReconfigureReply::Accepted { ballot, .. } => (
                    "DecreeAccepted",
                    format!("Voted at {}", show_ballot(*ballot)),
                ),
                ReconfigureReply::Nacked { promised, .. } => (
                    "DecreeNack",
                    format!("Refused: it promised {}", show_ballot(*promised)),
                ),
                ReconfigureReply::Learned { activated, at, .. } => (
                    "Learned",
                    if *activated {
                        format!("Generation {} is active here now", at.0)
                    } else {
                        format!("Recorded the successor; it stays at generation {}", at.0)
                    },
                ),
                ReconfigureReply::Refused { current, .. } => (
                    "ReconfigureRefused",
                    format!("Refused: it holds generation {}", current.generation.0),
                ),
            };
            (kind, "reconfigure", None, summary, true)
        }
    };
    Some(MessageView {
        id: entry.id,
        kind: kind.to_string(),
        from: entry.from.number(),
        from_party: entry.from.party_view(),
        to: entry.to.number(),
        to_party: entry.to.party_view(),
        ballot: ballot.map(show_ballot),
        slot: None,
        column: None,
        summary,
        phase: phase.to_string(),
        reply,
        sent_at: entry.sent_at,
    })
}

/// Why a matchmaker refused a registration, in one clause.
#[must_use]
pub(crate) fn why(refusal: &paros_core::MatchRefusal) -> String {
    match refusal {
        paros_core::MatchRefusal::Stale { highest } => format!(
            "it has already registered {}, and a ballot must be above every registered one",
            show_ballot(*highest)
        ),
        paros_core::MatchRefusal::BelowWatermark { watermark } => format!(
            "its watermark is {}, and nothing below a watermark registers again",
            show_ballot(*watermark)
        ),
        paros_core::MatchRefusal::Stopped {
            successor: Some(set),
        } => format!(
            "it is frozen, and its successor is generation {} = {}",
            set.generation.0,
            show_set(set.members())
        ),
        paros_core::MatchRefusal::Stopped { successor: None } => {
            "it is frozen, and no successor is chosen yet".to_string()
        }
        paros_core::MatchRefusal::Generation { current } => format!(
            "it now serves generation {} = {}",
            current.generation.0,
            show_set(current.members())
        ),
        paros_core::MatchRefusal::Inactive => "it serves no generation: it is a spare".to_string(),
    }
}

/// How a party renders on the wire.
impl Party {
    /// The number this party's id carries.
    #[must_use]
    pub fn number(self) -> u64 {
        match self {
            Party::Node(id) => id.0,
            Party::Matchmaker(id) => id.0,
        }
    }

    /// Which tier it belongs to.
    #[must_use]
    pub fn party_view(self) -> PartyView {
        match self {
            Party::Node(_) => PartyView::Node,
            Party::Matchmaker(_) => PartyView::Matchmaker,
        }
    }

    /// The node this party names, if it is one.
    #[must_use]
    pub fn node(self) -> Option<NodeId> {
        match self {
            Party::Node(id) => Some(id),
            Party::Matchmaker(_) => None,
        }
    }
}
