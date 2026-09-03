//! The matchmaker's **durable state**: its generation phase, the pending
//! bootstraps of a proposed successor, the scalars persisted whole, its static
//! configuration, and the ledger record a registration writes.
//!
//! Everything here is what a reboot reads back through
//! [`RegistryStorage`](super::RegistryStorage); nothing here decides anything.

use std::collections::BTreeMap;

use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::single_decree::DecreeAcceptor;
use crate::types::Ballot;

/// The phase of a matchmaker's current generation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchmakerPhase {
    /// A fresh store: nothing was ever written. Resolved at boot from the
    /// deployment's bootstrap set — a bootstrap member is active for
    /// generation 0, any other matchmaker is inactive (a spare, until a
    /// bootstrap and a decree bring it into a later generation).
    #[default]
    Fresh,
    /// Not authoritative for any generation: a spare, or a member of a
    /// proposed successor whose decree has not been learned yet.
    Inactive,
    /// Serving matchmaking for its generation.
    Active,
    /// Frozen for its generation: registers nothing, keeps voting in the
    /// successor decree, and points late proposers at the successor.
    Stopped,
}

/// A successor generation's initial state, handed to each of its members by
/// the reconfigurer and held **pending** until the decree chooses that set.
/// Stored as one record: it arrives in one message, is replaced whole, and
/// becomes the per-record registry only at activation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PendingBootstrap {
    /// The proposed successor set.
    pub set: MatchmakerSet,
    /// The reconstructed GC watermark (the maximum over the frozen quorum).
    pub gc_watermark: Ballot,
    /// The reconstructed registry (the union over the frozen quorum, at or
    /// above `gc_watermark`).
    pub history: BTreeMap<Ballot, Registration>,
}

/// The small, persisted-whole durable scalars of a matchmaker — the
/// registry's [`crate::HardState`]: the GC watermark, the generation state,
/// and the successor decree's acceptor record. `#[non_exhaustive]` and built
/// through [`Default`] so a field can land without breaking every store.
///
/// The per-ballot registrations are deliberately **not** here: they are
/// persisted one record at a time and read back one record at a time through
/// [`RegistryStorage`], exactly as the accepted log is split from
/// [`crate::HardState`]. See [`RegistryStorage`] for why.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct MatchmakerHardState {
    /// The GC watermark (§3.4): a monotone floor below which no request may
    /// register and below which registrations have been dropped. Raised only
    /// by [`Matchmaker::advance_gc_watermark`] — the leader's GC protocol
    /// (`node/gc.rs`) owns the §3.5 preconditions — and carried forward into
    /// every successor generation. [`Ballot::zero`] is the "nothing
    /// collected" floor.
    pub gc_watermark: Ballot,
    /// The generation `members` and `phase` describe. Generation 0's members
    /// are the deployment's bootstrap set (configuration, never written).
    pub generation: MatchmakerGeneration,
    /// The members of `generation` for a generation this matchmaker
    /// activated (empty at generation 0, whose set is configuration).
    pub members: Vec<MatchmakerId>,
    /// Where this matchmaker stands in `generation`.
    pub phase: MatchmakerPhase,
    /// The chosen successor of `generation`, once learned: what a frozen
    /// matchmaker answers a late proposer with (the discovery chain).
    pub successor: Option<MatchmakerSet>,
    /// This matchmaker's acceptor record in the decree that chooses
    /// `generation`'s successor. Reset at every activation.
    pub decree: DecreeAcceptor<Vec<MatchmakerId>>,
    /// Bootstraps for proposed later generations this matchmaker is a member
    /// of, keyed by the proposed set, inactive until one is chosen.
    pub pending: Vec<PendingBootstrap>,
}

/// A matchmaker's static configuration: its identity and the deployment's
/// bootstrap matchmaker set (generation 0).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchmakerConfig {
    /// This matchmaker's identity.
    pub id: MatchmakerId,
    /// The bootstrap set: the members of generation 0.
    pub bootstrap: Vec<MatchmakerId>,
}

/// One ledger record: the configuration registered under a ballot, and
/// whether registering it was an **operator's reconfiguration** (a leader
/// moving the cluster to a new acceptor set, `RawNode::reconfigure`) rather
/// than a candidate restating the configuration it believed in force.
///
/// **The effective configuration is a registration fact, not a Paxos-chosen
/// value.** A reconfiguration is in force once its flagged record reached a
/// matchmaker quorum — before any Phase 1 or Phase 2 under the new set
/// completes — and stays in force until a higher-ballot flagged record
/// lands. The full contract, with its consequences (what `accepted: true`
/// promises, overlapping reconfigurations, why beliefs never count), is the
/// *effective configuration* section of the leader-side matchmaking module
/// (`node/matchmaking.rs`).
///
/// The flag is what makes the ledger answer "which configuration is in
/// force?" without treating every registration as a fact: an ordinary
/// campaign registers a *belief* (possibly stale, possibly abandoned), and a
/// ledger full of beliefs made "adopt the newest registration" flip-flop
/// between two candidates' beliefs forever. A reconfiguration registration is
/// an explicit request, and requests are monotone by ballot: the
/// highest-ballot one a matchmaker quorum holds is the **effective
/// configuration** — the one every ordinary campaign must register (see
/// `RawNode::on_match_reply`). Once a reconfiguration's matchmaking has
/// completed at a matchmaker quorum, quorum intersection hands that record
/// to every later campaign's matchmaking, so no later ordinary election can
/// reinstate a superseded configuration; before that it may be lost like any
/// proposal that never reached a quorum.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Registration {
    /// The acceptor configuration registered.
    pub config: AcceptorConfig,
    /// Whether this registration is a reconfiguration request.
    pub reconfiguration: bool,
}

impl Registration {
    /// A candidate's belief: the configuration it intends to run with.
    #[must_use]
    pub fn belief(config: AcceptorConfig) -> Self {
        Self {
            config,
            reconfiguration: false,
        }
    }

    /// A reconfiguration request: the configuration a leader moves to.
    #[must_use]
    pub fn reconfiguration(config: AcceptorConfig) -> Self {
        Self {
            config,
            reconfiguration: true,
        }
    }
}
