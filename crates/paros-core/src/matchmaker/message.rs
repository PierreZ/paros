//! The matchmaker's **wire contract**: the matchmaking round trip
//! (`MatchRequest` / `MatchReply` and its refusals), the garbage-collection
//! pair (`GcRequest` / `GcAck`), and the five-step generation handover
//! (`ReconfigureRequest` / `ReconfigureReply`).
//!
//! Types only. What a matchmaker *does* with them is
//! [`Matchmaker::step`](super::Matchmaker::step),
//! [`Matchmaker::advance_gc_watermark`](super::Matchmaker::advance_gc_watermark)
//! and the generation machine in [`super::generation`].

use std::collections::BTreeMap;

use super::{MatchmakerPhase, PendingBootstrap, Registration};
use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::types::{Ballot, NodeId};

/// A proposer's matchmaking request: "register `config` for `ballot`, and tell
/// me every configuration registered below it" (the paper's `MatchA`), fenced
/// by the matchmaker generation the proposer believes authoritative.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchRequest {
    /// The requesting proposer.
    pub from: NodeId,
    /// The ballot to register under. One ballot has exactly one proposer, so
    /// `ballot.node` is the identity that keeps matchmakers from disagreeing.
    pub ballot: Ballot,
    /// The acceptor configuration the proposer intends to run `ballot` with.
    pub config: AcceptorConfig,
    /// Whether this is a reconfiguration request (see [`Registration`]).
    pub reconfiguration: bool,
    /// The matchmaker generation the proposer addresses. A matchmaker not
    /// active for exactly this generation refuses with what it knows.
    pub generation: MatchmakerGeneration,
}

impl MatchRequest {
    /// A candidate's request to register its belief `config` under `ballot`
    /// at `generation`.
    #[must_use]
    pub fn new(
        from: NodeId,
        ballot: Ballot,
        config: AcceptorConfig,
        generation: MatchmakerGeneration,
    ) -> Self {
        Self {
            from,
            ballot,
            config,
            reconfiguration: false,
            generation,
        }
    }

    /// A leader's request to register the reconfiguration to `config` under
    /// `ballot` at `generation` (see [`Registration`]).
    #[must_use]
    pub fn reconfigure(
        from: NodeId,
        ballot: Ballot,
        config: AcceptorConfig,
        generation: MatchmakerGeneration,
    ) -> Self {
        Self {
            from,
            ballot,
            config,
            reconfiguration: true,
            generation,
        }
    }

    /// The ledger record this request registers.
    #[must_use]
    pub fn registration(&self) -> Registration {
        Registration {
            config: self.config.clone(),
            reconfiguration: self.reconfiguration,
        }
    }
}

/// Why a matchmaker refused a request. Each variant carries enough for the
/// requester to make progress — the highest registered ballot, the watermark,
/// the set that superseded the one it addressed — exactly as
/// [`crate::Message::Nack`] carries `promised`. A ballot in a refusal is a
/// **diagnostic**, never trusted future-ballot input; a matchmaker set in a
/// refusal is a **chosen** one (a matchmaker only ever names a set it
/// activated or learned chosen), which is why a proposer may adopt it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchRefusal {
    /// The requested ballot is not strictly above every registered ballot
    /// (frankenpaxos: `round <= acceptorGroups.lastKey`), or it names a ballot
    /// already registered with a *different* configuration.
    Stale {
        /// The highest ballot this matchmaker has registered.
        highest: Ballot,
    },
    /// The requested ballot sits below the GC watermark: its configuration was
    /// (or may have been) collected, so nothing below the floor may register.
    BelowWatermark {
        /// The matchmaker's current watermark.
        watermark: Ballot,
    },
    /// The addressed generation is frozen at this matchmaker: a successor is
    /// being chosen, or was chosen (`successor`), and nothing registers for
    /// it again. The proposer adopts the successor if named, else retries
    /// later.
    Stopped {
        /// The chosen successor, if this matchmaker has learned it.
        successor: Option<MatchmakerSet>,
    },
    /// This matchmaker is active for a generation other than the addressed
    /// one: the proposer is stale (adopt `current` if it is higher) or ahead
    /// of a matchmaker that has not activated yet (never adopt a lower one).
    Generation {
        /// The set this matchmaker is active for.
        current: MatchmakerSet,
    },
    /// This matchmaker is not authoritative for any generation: a spare, or
    /// a proposed successor's member whose decree it has not learned.
    Inactive,
}

/// What a matchmaker answered a request with.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchOutcome {
    /// The configuration is registered (durably, once the driver has flushed
    /// the batch this reply travels in — see [`MatchmakerReady`]) and this is
    /// the paper's `MatchB`: every configuration registered at a ballot
    /// **strictly below** the request's and at or above the watermark.
    Registered {
        /// `ballot -> registration` for every registration in
        /// `[gc_watermark, request.ballot)`, in ballot order.
        history: BTreeMap<Ballot, Registration>,
        /// The watermark in force when the history was computed.
        gc_watermark: Ballot,
        /// The **effective configuration** this matchmaker durably holds
        /// (see [`super::MatchmakerHardState::effective`]), whether or not
        /// its record is still in `history`: GC drops the record, never the
        /// scalar, so this is what tells a candidate which acceptor set is
        /// in force after a floor rose over the last reconfiguration.
        effective: Option<(Ballot, AcceptorConfig)>,
    },
    /// The request was refused; nothing was registered.
    Refused(MatchRefusal),
}

/// A matchmaker's answer to one [`MatchRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchReply {
    /// The answering matchmaker.
    pub matchmaker: MatchmakerId,
    /// The requester the reply is addressed to.
    pub to: NodeId,
    /// The request's ballot, echoed.
    pub ballot: Ballot,
    /// The request's generation, echoed.
    pub generation: MatchmakerGeneration,
    /// The answer.
    pub outcome: MatchOutcome,
}

/// A leader's garbage-collection request (the paper's `GarbageA`): raise the
/// watermark of `generation`'s registry to `watermark`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GcRequest {
    /// The requesting leader.
    pub from: NodeId,
    /// The generation addressed.
    pub generation: MatchmakerGeneration,
    /// The floor to raise to — the leader's own ballot.
    pub watermark: Ballot,
}

/// A matchmaker's answer to a [`GcRequest`] (the paper's `GarbageB`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GcAck {
    /// The answering matchmaker.
    pub matchmaker: MatchmakerId,
    /// The request's generation, echoed.
    pub generation: MatchmakerGeneration,
    /// Whether the request was applied at that generation (a matchmaker not
    /// active for it refuses, and `watermark` then names its own floor).
    pub applied: bool,
    /// The durable watermark after the request.
    pub watermark: Ballot,
}

/// A reconfigurer's message to a matchmaker (#125): one step of the
/// stop / bootstrap / decree / publish handover.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ReconfigureRequest {
    /// Freeze `generation` and report its registry (`StopA`).
    Stop {
        /// The requesting node.
        from: NodeId,
        /// The generation to freeze.
        generation: MatchmakerGeneration,
    },
    /// Hand a proposed successor's initial state to one of its members.
    Bootstrap {
        /// The requesting node.
        from: NodeId,
        /// The proposed set, its reconstructed watermark and registry.
        bootstrap: PendingBootstrap,
    },
    /// Phase 1a of the successor decree over `generation`'s matchmakers.
    DecreePrepare {
        /// The requesting node.
        from: NodeId,
        /// The generation whose successor is being decided.
        generation: MatchmakerGeneration,
        /// The decree ballot.
        ballot: Ballot,
    },
    /// Phase 2a of the successor decree: accept `members` as the successor.
    DecreeAccept {
        /// The requesting node.
        from: NodeId,
        /// The generation whose successor is being decided.
        generation: MatchmakerGeneration,
        /// The decree ballot.
        ballot: Ballot,
        /// The proposed successor membership.
        members: Vec<MatchmakerId>,
    },
    /// The successor of `generation` was chosen: a member of `generation`
    /// records it (and freezes, if it had not), a member of the successor
    /// activates its pending bootstrap.
    ///
    /// **A learner notification, not an acceptor decision.** The matchmaker
    /// that receives it does *not* verify that `successor` is what the
    /// decree over `generation` chose — exactly as a Paxos acceptor learning
    /// a `Commit` trusts the proposer that assembled the quorum. The
    /// protocol precondition, on the sender: `Chosen` is emitted only by a
    /// reconfigurer in its `Publishing` phase (entered only on a Phase-2
    /// quorum of `M_generation`), or relayed verbatim by a node that
    /// learned the set from such a publication (a `Stopped { successor }`
    /// or `Generation { current }` refusal, or a `Chosen` step of its own).
    /// Under crash faults with a correct driver that is exactly what
    /// happens; a driver that fabricates a `Chosen` is outside the fault
    /// model, and the core does not defend against it. Every *other* wire
    /// check a learner can make is made: the generation chain
    /// (`successor.generation == generation + 1`), a set that admits the
    /// quorum system, and agreement with the successor already recorded for
    /// that generation — a `Chosen` naming a different one is refused, never
    /// applied and never answered as if it had been learned.
    Chosen {
        /// The requesting node.
        from: NodeId,
        /// The generation succeeded.
        generation: MatchmakerGeneration,
        /// The chosen successor.
        successor: MatchmakerSet,
    },
}

impl ReconfigureRequest {
    /// The requesting node.
    #[must_use]
    pub fn from(&self) -> NodeId {
        match self {
            Self::Stop { from, .. }
            | Self::Bootstrap { from, .. }
            | Self::DecreePrepare { from, .. }
            | Self::DecreeAccept { from, .. }
            | Self::Chosen { from, .. } => *from,
        }
    }
}

/// A matchmaker's answer to one [`ReconfigureRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ReconfigureReply {
    /// `generation` is frozen here (`StopB`): its durable registry, watermark,
    /// and the successor if already learned.
    Stopped {
        /// The answering matchmaker.
        matchmaker: MatchmakerId,
        /// The frozen generation.
        generation: MatchmakerGeneration,
        /// The durable watermark.
        gc_watermark: Ballot,
        /// The durable registry at or above the watermark.
        history: BTreeMap<Ballot, Registration>,
        /// The **effective configuration** this matchmaker durably holds
        /// (see [`super::MatchmakerHardState::effective`]). The
        /// reconstruction takes the maximum over its stop quorum, exactly as
        /// it takes the maximum watermark, so a successor generation
        /// inherits the acceptor set in force even when the record it came
        /// from was collected long ago.
        effective: Option<(Ballot, AcceptorConfig)>,
        /// The chosen successor, if learned.
        successor: Option<MatchmakerSet>,
        /// The highest decree ballot this matchmaker has promised for the
        /// frozen generation: the floor a reconfigurer opens its decree
        /// above. A reconfigurer holds no durable state, so a rebooted
        /// node's fresh incarnation would otherwise reuse the rounds its
        /// earlier one minted — and a ballot must carry one value. Every
        /// promise quorum an earlier decree at this node reached intersects
        /// the stop quorum, so the maximum over the stop quorum is strictly
        /// below no ballot that could ever have been accepted (the handover
        /// model checker's finding, seed 103).
        decree_promised: Ballot,
    },
    /// The bootstrap for `set` is durably pending here.
    Bootstrapped {
        /// The answering matchmaker.
        matchmaker: MatchmakerId,
        /// The proposed set the bootstrap was for.
        set: MatchmakerSet,
    },
    /// Phase 1b: promised `ballot`, reporting the vote held.
    Promised {
        /// The answering matchmaker.
        matchmaker: MatchmakerId,
        /// The generation decided over.
        generation: MatchmakerGeneration,
        /// The promised ballot.
        ballot: Ballot,
        /// The highest-ballot vote held, if any.
        vote: Option<(Ballot, Vec<MatchmakerId>)>,
    },
    /// Phase 2b: accepted `ballot`'s proposal.
    Accepted {
        /// The answering matchmaker.
        matchmaker: MatchmakerId,
        /// The generation decided over.
        generation: MatchmakerGeneration,
        /// The accepted ballot.
        ballot: Ballot,
    },
    /// A decree message at a ballot a higher promise refuses.
    Nacked {
        /// The answering matchmaker.
        matchmaker: MatchmakerId,
        /// The generation decided over.
        generation: MatchmakerGeneration,
        /// The refused ballot.
        ballot: Ballot,
        /// The promise that refused it.
        promised: Ballot,
    },
    /// The chosen successor was learned (recorded, or activated).
    Learned {
        /// The answering matchmaker.
        matchmaker: MatchmakerId,
        /// The generation succeeded.
        generation: MatchmakerGeneration,
        /// Whether this matchmaker activated the successor (it is a member
        /// holding the pending bootstrap) rather than only recording it.
        activated: bool,
    },
    /// The request addressed a generation this matchmaker is not at, or
    /// asked something its phase cannot do: what it knows instead.
    Refused {
        /// The answering matchmaker.
        matchmaker: MatchmakerId,
        /// The set this matchmaker is active or frozen for.
        current: MatchmakerSet,
        /// Where this matchmaker stands.
        phase: MatchmakerPhase,
        /// The successor of `current`, if learned.
        successor: Option<MatchmakerSet>,
    },
}

impl ReconfigureReply {
    /// The answering matchmaker.
    #[must_use]
    pub fn matchmaker(&self) -> MatchmakerId {
        match self {
            Self::Stopped { matchmaker, .. }
            | Self::Bootstrapped { matchmaker, .. }
            | Self::Promised { matchmaker, .. }
            | Self::Accepted { matchmaker, .. }
            | Self::Nacked { matchmaker, .. }
            | Self::Learned { matchmaker, .. }
            | Self::Refused { matchmaker, .. } => *matchmaker,
        }
    }
}
