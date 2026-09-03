//! The **matchmaker**: a per-ballot acceptor-configuration registry (Matchmaker
//! Paxos §3.1–§3.2), as a sans-IO state machine beside [`crate::RawNode`].
//!
//! A matchmaker separates *who are the acceptors* from *what value is chosen*.
//! It keeps a tiny durable map `ballot -> acceptor configuration` plus a GC
//! watermark, and is touched **only** on a round change (a leader change or a
//! reconfiguration), never on the command path. It is the source of truth that
//! lets a new leader discover **every configuration used below its own ballot**,
//! which is what the cross-configuration Phase 1 of #22 rests on.
//!
//! This module is the registry side only: the pure state machine, its durable
//! state, and its request / reply / refusal semantics — driven through the same
//! `step` → `ready` → `advance` shape as [`crate::RawNode`], so the driver's
//! persist-before-reply ordering is structural rather than remembered. It is a
//! **separate handle the caller drives**: [`crate::RawNode`] never steps a
//! matchmaker message, and a cluster deployed without matchmakers never
//! constructs one (see AGENTS.md, *Plain Multi-Paxos is first-class*).
//!
//! # Keying: whole ballots, not bare rounds
//!
//! The paper keys its log by round because rounds are statically partitioned
//! across proposers, so "one proposer owns round `i` and picks one config" is
//! what keeps matchmakers from ever disagreeing about a round's configuration
//! (§3.2). paros does not partition rounds that way: `on_check_leader` campaigns
//! at `max(promised, ballot).round + 1` with the node id as tiebreak, so two
//! nodes legitimately campaign at the *same* round with different ballots. The
//! registry is therefore keyed on the whole [`Ballot`] (`{ round, node }`,
//! totally ordered by `Ord for Ballot`): the ballot's `node` carries the
//! proposer identity, and the paper's no-disagreement property holds for the
//! same reason — **one ballot has exactly one proposer**. Concretely: two
//! candidates at the same round, `{5, node 1}` and `{5, node 2}`, occupy two
//! distinct registry keys (the lower one is a strict ancestor in the other's
//! history), and no path in this module ever keys on a bare round. This is the
//! one place paros diverges from the paper's model, and the tests pin it.
//!
//! # The contract, in one sentence each
//!
//! - **Write-once per ballot.** A ballot registered with configuration `C` is
//!   never observed later with a different configuration, by anyone, ever.
//! - **Registration is strictly monotone.** After registering `b`, a request
//!   at any `b' <= b` is refused, never quietly registered. The one exception
//!   is the *same request again* — `b` with the configuration already
//!   registered under it — which is answered idempotently from the **currently
//!   retained** history, without a second registration. That re-answer is not
//!   a replay of the first reply: GC between the two may have raised the
//!   watermark, so the retry's history can be a strict subset of the original
//!   (never a superset — the registry only ever grows *above* `b`). Whether a
//!   proposer may still act on such a shrunk history is the GC protocol's
//!   contract (§3.4–§3.5), not this state machine's.
//!   Monotonicity is what makes the returned history complete for every later
//!   ballot.
//! - **The history is complete below the request.** The reply for `b` names
//!   every configuration this matchmaker holds at a ballot `< b` and at or
//!   above its watermark. Under-reporting is the bug class the whole protocol
//!   exists to prevent.
//! - **The watermark is monotone** and never lets a request below it register.
//! - **Persist before reply.** A successful reply may only leave once its
//!   registration is fsync-durable — the driver's job, made structural by
//!   [`MatchmakerReady`]'s ordering (writes first, then replies); the exact
//!   analogue of the acceptor's persist-before-`Promise` rule on
//!   [`crate::HardState`].
//!
//! # Generations: the matchmaker set is itself a chosen value (#125)
//!
//! The matchmakers are the source of truth for configurations, so their own
//! membership cannot be a static fact forever: a matchmaker that dies or
//! loses its disk would be unreplaceable. Following §5 of the paper, the
//! matchmaker set carries a **generation**, and generation `g + 1` is chosen
//! by a **single-decree Paxos instance whose acceptors are generation `g`'s
//! matchmakers** ([`crate::DecreeAcceptor`] is the durable half every
//! matchmaker keeps). The handover is stop-the-world, which is acceptable
//! because matchmakers are idle whenever a leader is stable:
//!
//! ```text
//! Stop        a quorum of M_g freezes (durably; a frozen matchmaker registers
//!             nothing for g ever again) and answers with its registry + watermark
//! Bootstrap   the reconfigurer reconstructs (max watermark, union above it) and
//!             hands it to every member of the proposed M_{g+1}, stored *pending*
//! Decree      Phase 1 / Phase 2 over M_g choose the successor set (P2c adopts
//!             a competing proposal already voted, so two reconfigurers never
//!             install two successors)
//! Chosen      every matchmaker told: M_g members record the successor (the
//!             discovery chain), M_{g+1} members activate their pending bootstrap
//! ```
//!
//! Every message is **fenced by generation**: a request naming another
//! generation is refused with what this matchmaker knows (its current set,
//! or its successor), never served. A frozen matchmaker stays alive: it keeps
//! answering `Stop`, votes in the decree, and points late proposers at its
//! successor — "stopped" is a protocol freeze, not a process death.
//!
//! Replacement is also how paros recovers a matchmaker whose durable state is
//! unusable: there is deliberately no matchmaker-specific in-place disk
//! repair. Such a matchmaker is fenced out of the next generation and a fresh
//! one bootstrapped in its place from the surviving quorum's frozen registries.
//!
//! # What this proves, and what it does not
//!
//! Everything above is a property of **one matchmaker**: each registry, taken
//! alone, is write-once, monotone, complete below any ballot it answers, and
//! durable before it answers. The paper's safety argument (§3.3) rests on
//! something more: a proposer collects `MatchB` from `f + 1` of the `2f + 1`
//! matchmakers and runs Phase 1 against the **union** of their histories, and
//! it is the intersection of any two such `f + 1` sets that guarantees no
//! configuration used below the proposer's ballot is missed. That union, the
//! quorum it needs, and the cross-configuration Phase 1 it feeds belong to the
//! leader-side matchmaking phase (`node/matchmaking.rs`); the reconstruction
//! across generations rests on the same intersection (Appendix B: every
//! completed registration reached a quorum of `M_g`, which intersects the
//! frozen quorum the successor was reconstructed from).
//!
//! # Trust boundary of `Chosen`
//!
//! Three of the five reconfiguration messages are *acceptor* decisions the
//! matchmaker makes from its own durable state — `Stop` (freeze), and the
//! decree's `DecreePrepare`/`DecreeAccept` (promise and vote). `Bootstrap`
//! is a durable hand-off of a proposal. `Chosen` is the one **learner
//! notification**: the matchmaker records or activates the successor it is
//! told, without re-deriving the decision, on the precondition that only a
//! proposer holding the decree's Phase-2 quorum (or a node relaying such a
//! publication) emits it. See [`ReconfigureRequest::Chosen`].
//!
//! Trusting is not the same as staying silent. The one contradiction a
//! learner *can* see, it refuses: a `Chosen` for a generation whose
//! successor this matchmaker already recorded, naming a different set, is
//! answered with the ordinary refusal (which carries the recorded
//! successor) and applied to nothing. A wrong relay is precisely what
//! produces that message, and an `activated: false` `Learned` would have
//! made it indistinguishable from a duplicate.

#[cfg(test)]
mod handover_model;
mod reconfigurer;

use std::collections::BTreeMap;

pub use self::reconfigurer::{
    MatchmakerReconfigurer, ReconfigurerPhase, ReconfigurerStep, StartRefusal,
};
pub use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::single_decree::DecreeAcceptor;
use crate::types::{Ballot, NodeId};

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

/// The read-only recovery port of a matchmaker — the registry's
/// [`crate::Storage`], mirrored method for method. The **application**
/// implements it and owns *all* writes; the core only ever *reads back*, once
/// at construction, what the driver has already persisted through its write
/// extension (`paros::MatchmakerStorage`, the `NodeStorage` twin).
///
/// # Why a per-record port and not a state blob
///
/// A matchmaker could be booted from one `(registry map, watermark)` value —
/// the registry is small. It is not, on purpose, because the registry is
/// durable state that **will rot**, and the CTRL recovery story built for the
/// accepted log (Stages 7–8: `docs/analysis/storage/ctrl-multipaxos-restatement.md`)
/// only applies to state the core reads *record by record* through a port the
/// storage layer can classify at its seam:
///
/// - **Detection lives in the write layer's `boot_scan`, per record.** Each
///   registration is one checksummed record with its identity — the ballot —
///   in the checksummed region, so a torn, misdirected or bit-flipped
///   registration is classified *before* any byte reaches this port, exactly
///   as an accepted entry is. A blob would have one checksum for the whole
///   registry and one verdict: crash.
/// - **The tri-state lands here.** CTRL's insight for the log — a record whose
///   *value* is lost but whose *identity* survived must be reported as
///   `faulty`, never as "nothing here" — holds for the registry with the same
///   force: a lost registration answered as "no configuration below `b`"
///   under-reports a history, which is precisely the bug class matchmakers
///   exist to prevent. The repair is not a local one: a matchmaker whose
///   durable state is unusable is **replaced** through a matchmaker-set
///   reconfiguration (the module doc's *Generations*), reconstructed from the
///   surviving quorum — never repaired in place.
/// - **Per-record writes are what make the seams honest.** The driver applies
///   one [`MatchmakerWriteOp`] per record and fsyncs the batch before the
///   reply leaves; a boot that reads records back one by one is the read-side
///   pair of that write ordering, and the audit compares the two.
///
/// Bootstrap and restart are the same path: a fresh matchmaker is an empty
/// port. All methods are infallible: a record that fails its integrity check
/// never reaches the core (the scan withholds it, and crashes or classifies).
pub trait RegistryStorage {
    /// The durable scalars to initialize the matchmaker with. Called once, at
    /// construction.
    fn initial_state(&self) -> MatchmakerHardState;

    /// The record registered under `ballot`, if any — the per-record read,
    /// the twin of [`crate::Storage::accepted`].
    fn registration(&self, ballot: Ballot) -> Option<Registration>;

    /// Every registered ballot in ascending order — the registry's
    /// identities, the twin of the `first_slot..=last_slot` walk. Each names a
    /// record [`Self::registration`] serves.
    fn registered_ballots(&self) -> Vec<Ballot>;
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

/// A single semantic durable write the driver must apply to stable storage
/// and **fsync before** the batch's replies leave — every matchmaker write is
/// safety-critical, so there is no relaxed class here.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchmakerWriteOp {
    /// Register `config` under `ballot`. Append-only: `ballot` is strictly
    /// above every ballot the registry holds.
    Register {
        /// The ballot registered.
        ballot: Ballot,
        /// The record registered under it.
        registration: Registration,
    },
    /// Raise the durable GC watermark to `watermark` and drop every
    /// registration below it. Monotone: never below the current watermark.
    SetGcWatermark(Ballot),
    /// Persist the durable scalars whole (the generation state and the
    /// decree record). The watermark inside equals the durable one.
    SetScalars(MatchmakerHardState),
    /// Replace the registry whole — the activation of a successor generation:
    /// every record dropped, these written, and the scalars (whose watermark
    /// is the reconstructed one) persisted in the same batch.
    InstallRegistry {
        /// The scalars after activation.
        scalars: MatchmakerHardState,
        /// The reconstructed registry.
        registrations: BTreeMap<Ballot, Registration>,
    },
}

/// One batch of matchmaker work, and the compile-time gate enforcing one batch
/// in flight — the matchmaker's [`crate::Ready`].
///
/// # Durability ordering — process the buckets in this order
///
/// 1. **Persist** [`MatchmakerReady::writes`] to stable storage, in order, and
///    fsync them. Every write here is safety-critical.
/// 2. **Send** [`MatchmakerReady::replies`] and
///    [`MatchmakerReady::reconfigure_replies`] — *only after* step 1 is
///    durable. A `Registered` reply published before its registration is on
///    disk is the matchmaker's version of an un-promise: a crash then forgets
///    a configuration the proposer already believes every later leader will
///    be told about. The same holds for a `Stopped` that left before the
///    freeze was durable, and for a decree promise or vote.
/// 3. Call [`MatchmakerReady::advance`] to release the gate.
#[must_use = "a MatchmakerReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct MatchmakerReady<'a> {
    matchmaker: &'a mut Matchmaker,
}

impl MatchmakerReady<'_> {
    /// The durable writes to persist and fsync **first** (step 1), in order.
    #[must_use]
    pub fn writes(&self) -> &[MatchmakerWriteOp] {
        &self.matchmaker.pending_writes
    }

    /// The matchmaking replies to send **after** the writes are durable
    /// (step 2).
    #[must_use]
    pub fn replies(&self) -> &[MatchReply] {
        &self.matchmaker.pending_replies
    }

    /// The reconfiguration replies to send **after** the writes are durable
    /// (step 2).
    #[must_use]
    pub fn reconfigure_replies(&self) -> &[ReconfigureReply] {
        &self.matchmaker.pending_reconfigure_replies
    }

    /// Acknowledge the batch: clears the pending buckets and releases the
    /// unique borrow. Consumes `self` — the guard cannot be reused.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(matchmaker = self.matchmaker.config.id.0)))]
    pub fn advance(self) {
        self.matchmaker.pending_writes.clear();
        self.matchmaker.pending_replies.clear();
        self.matchmaker.pending_reconfigure_replies.clear();
    }
}

/// What a GC request did at this matchmaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcOutcome {
    /// The watermark rose (a durable write is staged).
    Raised,
    /// At or below the floor already in force: nothing changed.
    Unchanged,
    /// Not active for the addressed generation: refused.
    Refused,
}

/// The matchmaker state machine: the registry, the watermark, the generation
/// state, and one pending batch of writes and replies. Pure — no I/O, no
/// clock, no randomness; the driver ([`paros::run_matchmaker`](https://docs.rs/paros))
/// performs every side effect it describes.
#[derive(Clone, Debug)]
pub struct Matchmaker {
    config: MatchmakerConfig,
    hard_state: MatchmakerHardState,
    /// Every registered `ballot -> registration`, strictly increasing in
    /// ballot order (a [`BTreeMap`] keeps the order; the state machine keeps
    /// the "only ever appended above the highest" discipline).
    registry: BTreeMap<Ballot, Registration>,
    pending_writes: Vec<MatchmakerWriteOp>,
    pending_replies: Vec<MatchReply>,
    pending_reconfigure_replies: Vec<ReconfigureReply>,
}

impl Matchmaker {
    /// Boot a matchmaker from its durable storage: read the scalars once, then
    /// walk the registry record by record (a fresh matchmaker is an empty
    /// port). Restart and first boot are the same path, so a rebooted
    /// matchmaker answers exactly as it would have without the crash. A
    /// [`MatchmakerPhase::Fresh`] store resolves against `config.bootstrap`
    /// without a write: a bootstrap member is active for generation 0, any
    /// other matchmaker inactive.
    ///
    /// # Panics
    ///
    /// If the durable state violates the registry contract — a ballot the
    /// walk names but the port cannot serve, a registration below the
    /// watermark, a malformed configuration. That means corrupted storage that
    /// evaded the scan or a broken storage implementation; crashing beats
    /// answering from it.
    #[must_use]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = config.id.0)))]
    pub fn new<S: RegistryStorage>(config: &MatchmakerConfig, storage: &S) -> Self {
        let hard_state = storage.initial_state();
        let mut registry = BTreeMap::new();
        for ballot in storage.registered_ballots() {
            let registration = storage
                .registration(ballot)
                .expect("every registered ballot the walk names has a readable record");
            let previous = registry.insert(ballot, registration);
            assert!(
                previous.is_none(),
                "the registry walk names each ballot once"
            );
        }
        let mut bootstrap = config.bootstrap.clone();
        bootstrap.sort_unstable();
        bootstrap.dedup();
        let matchmaker = Self {
            config: MatchmakerConfig {
                id: config.id,
                bootstrap,
            },
            hard_state,
            registry,
            pending_writes: Vec::new(),
            pending_replies: Vec::new(),
            pending_reconfigure_replies: Vec::new(),
        };
        matchmaker.assert_invariants();
        matchmaker
    }

    /// This matchmaker's identity.
    #[must_use]
    pub fn id(&self) -> MatchmakerId {
        self.config.id
    }

    /// The durable scalars as they stand (including a write not yet flushed
    /// by the driver — the core applies a write the instant it decides it,
    /// exactly as [`crate::RawNode`] does).
    #[must_use]
    pub fn hard_state(&self) -> &MatchmakerHardState {
        &self.hard_state
    }

    /// The registry as it stands, in ballot order (same caveat as
    /// [`Self::hard_state`]).
    #[must_use]
    pub fn registry(&self) -> &BTreeMap<Ballot, Registration> {
        &self.registry
    }

    /// The highest registered ballot, or `None` on an empty registry.
    #[must_use]
    pub fn highest(&self) -> Option<Ballot> {
        self.registry.keys().next_back().copied()
    }

    /// Where this matchmaker stands, with a fresh store resolved against the
    /// bootstrap set.
    #[must_use]
    pub fn phase(&self) -> MatchmakerPhase {
        match self.hard_state.phase {
            MatchmakerPhase::Fresh => {
                if self.config.bootstrap.binary_search(&self.config.id).is_ok() {
                    MatchmakerPhase::Active
                } else {
                    MatchmakerPhase::Inactive
                }
            }
            phase => phase,
        }
    }

    /// The set this matchmaker is active or frozen for: generation 0's
    /// bootstrap set, or the activated one.
    #[must_use]
    pub fn set(&self) -> MatchmakerSet {
        if self.hard_state.generation == MatchmakerGeneration(0)
            && self.hard_state.members.is_empty()
        {
            MatchmakerSet::new(MatchmakerGeneration(0), self.config.bootstrap.clone())
        } else {
            MatchmakerSet {
                generation: self.hard_state.generation,
                members: self.hard_state.members.clone(),
            }
        }
    }

    /// The chosen successor of this matchmaker's generation, if learned.
    #[must_use]
    pub fn successor(&self) -> Option<&MatchmakerSet> {
        self.hard_state.successor.as_ref()
    }

    /// Answer one matchmaking request (the paper's `MatchA` handler, §3.2):
    ///
    /// - not active for the request's generation → [`MatchRefusal::Stopped`]
    ///   (frozen, with the successor if learned), [`MatchRefusal::Generation`]
    ///   (active elsewhere) or [`MatchRefusal::Inactive`];
    /// - below the watermark → [`MatchRefusal::BelowWatermark`];
    /// - not strictly above the highest registered ballot → the same request
    ///   again (the ballot is registered with exactly this configuration) is
    ///   answered idempotently from the currently retained history, with no
    ///   write; anything else is [`MatchRefusal::Stale`];
    /// - otherwise the history of configurations registered in
    ///   `[gc_watermark, ballot)` is computed, `config` is registered under
    ///   `ballot` (a [`MatchmakerWriteOp::Register`] the driver must fsync
    ///   before the reply leaves), and the reply carries that history plus the
    ///   watermark.
    ///
    /// The re-answer is **not** a replay of the first reply. The registry only
    /// ever grows *above* an existing key, so the set of registrations below a
    /// registered ballot never gains a member — but GC can drop members below
    /// a raised watermark. A retry after GC therefore receives the first
    /// answer restricted to the current watermark (a strict subset, with the
    /// higher watermark reported beside it); the GC protocol, not this
    /// handler, is what makes acting on that subset safe.
    ///
    /// # Panics
    ///
    /// If processing exposes a broken internal invariant (a programmer error,
    /// never an operating condition — a stale or below-floor request is a
    /// refusal, not a panic).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, from = request.from.0, round = request.ballot.round)))]
    pub fn step(&mut self, request: MatchRequest) {
        self.assert_invariants();
        let MatchRequest {
            from,
            ballot,
            config,
            reconfiguration,
            generation,
        } = request;
        let registration = Registration {
            config,
            reconfiguration,
        };
        let outcome = if let Some(refusal) = self.generation_refusal(generation) {
            MatchOutcome::Refused(refusal)
        } else if ballot < self.hard_state.gc_watermark {
            MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                watermark: self.hard_state.gc_watermark,
            })
        } else if let Some(highest) = self.highest().filter(|highest| ballot <= *highest) {
            match self.registry.get(&ballot) {
                // The same request again: answered from the retained history
                // (which GC may have shrunk since the first answer),
                // registered once.
                Some(registered) if *registered == registration => MatchOutcome::Registered {
                    history: self.history_below(ballot),
                    gc_watermark: self.hard_state.gc_watermark,
                },
                _ => MatchOutcome::Refused(MatchRefusal::Stale { highest }),
            }
        } else {
            // Compute the history *before* registering, so the request's own
            // configuration never appears in its own answer.
            let history = self.history_below(ballot);
            let previous = self.registry.insert(ballot, registration.clone());
            assert!(
                previous.is_none(),
                "a fresh registration lands on an unregistered ballot"
            );
            assert!(
                self.highest() == Some(ballot),
                "a fresh registration becomes the registry's highest ballot"
            );
            self.pending_writes.push(MatchmakerWriteOp::Register {
                ballot,
                registration,
            });
            MatchOutcome::Registered {
                history,
                gc_watermark: self.hard_state.gc_watermark,
            }
        };
        if let MatchOutcome::Registered { history, .. } = &outcome {
            // Postconditions of a successful answer: the ballot is registered,
            // the history is exactly the window below it, and only an active
            // matchmaker of the addressed generation ever registers.
            assert!(
                self.registry.contains_key(&ballot),
                "a Registered reply names a registered ballot"
            );
            assert!(
                history.keys().all(|b| *b < ballot),
                "a history stays strictly below the ballot it answers"
            );
            assert!(
                history.keys().all(|b| *b >= self.hard_state.gc_watermark),
                "a history never reaches below the watermark"
            );
            assert!(
                self.phase() == MatchmakerPhase::Active && self.set().generation == generation,
                "only an active matchmaker of the addressed generation registers"
            );
        }
        self.pending_replies.push(MatchReply {
            matchmaker: self.config.id,
            to: from,
            ballot,
            generation,
            outcome,
        });
        self.assert_invariants();
    }

    /// The generation fence for a matchmaking or GC request: `None` when
    /// this matchmaker is active for exactly `generation`.
    fn generation_refusal(&self, generation: MatchmakerGeneration) -> Option<MatchRefusal> {
        let current = self.set();
        match self.phase() {
            MatchmakerPhase::Active if current.generation == generation => None,
            MatchmakerPhase::Active => Some(MatchRefusal::Generation { current }),
            MatchmakerPhase::Stopped if current.generation == generation => {
                Some(MatchRefusal::Stopped {
                    successor: self.hard_state.successor.clone(),
                })
            }
            MatchmakerPhase::Stopped => {
                // Frozen for another generation. A proposer *behind* this
                // generation learns the chain link if there is one, else the
                // set in force here — never `Inactive`, which would leave a
                // restarted node (it boots believing the bootstrap
                // generation) with no way to discover the generation it
                // must campaign in while the successor is still undecided
                // (the sweep found exactly that cluster: every campaign
                // refused, no leader for a whole run). A proposer *ahead*
                // of this generation is simply not served here.
                match &self.hard_state.successor {
                    Some(successor) if generation < current.generation.next() => {
                        Some(MatchRefusal::Stopped {
                            successor: Some(successor.clone()),
                        })
                    }
                    None if generation < current.generation => {
                        Some(MatchRefusal::Generation { current })
                    }
                    _ => Some(MatchRefusal::Inactive),
                }
            }
            MatchmakerPhase::Inactive | MatchmakerPhase::Fresh => Some(MatchRefusal::Inactive),
        }
    }

    /// Advance the GC watermark to `watermark` (§3.4: `w = max(w, i)`) for
    /// `generation`, dropping every registration below it, and stage the
    /// durable write. A request at or below the current floor is a no-op
    /// (monotone by construction, never an error); one addressing a
    /// generation this matchmaker is not active for is refused.
    ///
    /// # This is a correctness-critical primitive, not the GC protocol
    ///
    /// Nothing here checks that the collected configurations are no longer
    /// needed. The paper's garbage collection (§3.4–§3.5) is a *protocol*
    /// whose preconditions the leader establishes (`node/gc.rs`: every slot
    /// below its election fence held by a Phase-2 quorum of its own
    /// configuration, nothing chosen below its ballot above the fence) before
    /// it sends `GarbageA`; a floor raised above a configuration some future
    /// proposer still has to contact makes that proposer's history
    /// incomplete, which is a safety violation of the whole protocol — one
    /// this state machine can neither detect nor refuse, because the
    /// knowledge lives with the proposer.
    ///
    /// # Panics
    ///
    /// If raising the floor exposes a broken internal invariant.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, round = watermark.round)))]
    pub fn advance_gc_watermark(
        &mut self,
        generation: MatchmakerGeneration,
        watermark: Ballot,
    ) -> GcOutcome {
        self.assert_invariants();
        if self.generation_refusal(generation).is_some() {
            return GcOutcome::Refused;
        }
        if watermark <= self.hard_state.gc_watermark {
            return GcOutcome::Unchanged;
        }
        self.hard_state.gc_watermark = watermark;
        self.registry = self.registry.split_off(&watermark);
        self.pending_writes
            .push(MatchmakerWriteOp::SetGcWatermark(watermark));
        self.assert_invariants();
        GcOutcome::Raised
    }

    /// Answer one reconfiguration message (the module doc's *Generations*).
    /// Every arm is fenced by generation and phase; a refusal names what this
    /// matchmaker knows so a stale or ahead reconfigurer can adopt or abort.
    ///
    /// # Panics
    ///
    /// If processing exposes a broken internal invariant.
    #[allow(clippy::too_many_lines)]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, from = request.from().0)))]
    pub fn step_reconfigure(&mut self, request: ReconfigureRequest) {
        self.assert_invariants();
        let me = self.config.id;
        let current = self.set();
        let phase = self.phase();
        let refused = |this: &Self| ReconfigureReply::Refused {
            matchmaker: me,
            current: this.set(),
            phase: this.phase(),
            successor: this.hard_state.successor.clone(),
        };
        let reply = match request {
            ReconfigureRequest::Stop { generation, .. } => {
                if current.generation != generation
                    || !matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
                {
                    refused(self)
                } else {
                    if phase == MatchmakerPhase::Active {
                        // The freeze, durable before the answer leaves: a
                        // matchmaker that forgot it stopped and resumed
                        // registering would break the reconstruction the
                        // successor rests on.
                        self.freeze();
                    }
                    ReconfigureReply::Stopped {
                        matchmaker: me,
                        generation,
                        gc_watermark: self.hard_state.gc_watermark,
                        history: self.history_below(Ballot {
                            round: u64::MAX,
                            node: NodeId(u64::MAX),
                        }),
                        successor: self.hard_state.successor.clone(),
                        decree_promised: self.hard_state.decree.promised,
                    }
                }
            }
            ReconfigureRequest::Bootstrap { bootstrap, .. } => {
                let set = bootstrap.set.clone();
                // Wire hygiene: a proposal this matchmaker is not in, one
                // that would not move it forward, or one that cannot admit
                // the quorum system is refused whole. So is a competing
                // proposal for a generation already settled here: once a
                // successor is recorded, nothing else at its generation can
                // ever be chosen, and storing it would keep a whole registry
                // copy durable forever. The refusal names the successor, so
                // the stale reconfigurer adopts it instead.
                let settled = self
                    .hard_state
                    .successor
                    .as_ref()
                    .is_some_and(|s| set.generation <= s.generation && set != *s);
                if !set.contains(me)
                    || set.generation <= current.generation
                    || !set.is_well_formed()
                    || settled
                {
                    refused(self)
                } else {
                    // Keyed by the proposed set: two reconfigurers may
                    // bootstrap this matchmaker into two different proposed
                    // successors of one generation, and only the chosen one
                    // activates. Idempotent for the same set (a resent
                    // bootstrap from the same reconstruction).
                    if let Some(existing) =
                        self.hard_state.pending.iter_mut().find(|p| p.set == set)
                    {
                        if *existing != bootstrap {
                            // A different reconstruction of the same
                            // proposal (two reconfigurers, two frozen
                            // quorums): the later one supersedes, whole.
                            *existing = bootstrap;
                            self.stage_scalars();
                        }
                    } else {
                        self.hard_state.pending.push(bootstrap);
                        self.stage_scalars();
                    }
                    ReconfigureReply::Bootstrapped {
                        matchmaker: me,
                        set,
                    }
                }
            }
            ReconfigureRequest::DecreePrepare {
                generation, ballot, ..
            } => {
                if current.generation != generation
                    || !matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
                {
                    refused(self)
                } else {
                    let before = self.hard_state.decree.promised;
                    match self.hard_state.decree.prepare(ballot) {
                        Ok(vote) => {
                            if self.hard_state.decree.promised != before {
                                self.stage_scalars();
                            }
                            ReconfigureReply::Promised {
                                matchmaker: me,
                                generation,
                                ballot,
                                vote,
                            }
                        }
                        Err(promised) => ReconfigureReply::Nacked {
                            matchmaker: me,
                            generation,
                            ballot,
                            promised,
                        },
                    }
                }
            }
            ReconfigureRequest::DecreeAccept {
                generation,
                ballot,
                members,
                ..
            } => {
                if current.generation != generation
                    || !matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
                {
                    refused(self)
                } else {
                    let mut members = members;
                    members.sort_unstable();
                    members.dedup();
                    match self.hard_state.decree.accept(ballot, members) {
                        Ok(()) => {
                            self.stage_scalars();
                            ReconfigureReply::Accepted {
                                matchmaker: me,
                                generation,
                                ballot,
                            }
                        }
                        Err(promised) => ReconfigureReply::Nacked {
                            matchmaker: me,
                            generation,
                            ballot,
                            promised,
                        },
                    }
                }
            }
            ReconfigureRequest::Chosen {
                generation,
                successor,
                ..
            } => {
                // A learner notification (see the type doc on
                // `ReconfigureRequest::Chosen`): the matchmaker does not
                // re-derive the decision, it applies what a proposer that
                // held the Phase-2 quorum tells it — after the wire checks
                // any learner makes (the generation chain and a set that
                // admits the quorum system).
                let successor = MatchmakerSet::new(successor.generation, successor.members);
                if successor.generation != generation.next() || !successor.is_well_formed() {
                    refused(self)
                } else if current.generation == generation
                    && matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
                {
                    if self
                        .hard_state
                        .successor
                        .as_ref()
                        .is_some_and(|recorded| *recorded != successor)
                    {
                        // Two different successors for one generation: one of
                        // the two publications is wrong. A learner cannot tell
                        // which, but it can refuse to apply the second — the
                        // refusal names the successor recorded, so the
                        // contradiction reaches the sender (and any audit)
                        // instead of disappearing into an ordinary `Learned`.
                        refused(self)
                    } else {
                        // A member of the succeeded generation: record the
                        // chain link (freezing if it had not — once a
                        // successor is chosen the generation is over), then
                        // activate if it is also a member of the successor
                        // holding its bootstrap.
                        let mut changed = false;
                        if self.hard_state.successor.is_none() {
                            if phase == MatchmakerPhase::Active {
                                self.freeze();
                            }
                            self.hard_state.successor = Some(successor.clone());
                            changed = true;
                        }
                        changed |= self.prune_settled_pending(&successor);
                        if changed {
                            self.stage_scalars();
                        }
                        let activated = self.activate(&successor);
                        ReconfigureReply::Learned {
                            matchmaker: me,
                            generation,
                            activated,
                        }
                    }
                } else if successor.generation > current.generation
                    || matches!(phase, MatchmakerPhase::Inactive | MatchmakerPhase::Fresh)
                {
                    // Not a member of the succeeded generation (a spare, or
                    // frozen further back): only an activation can apply —
                    // but the decision settles this matchmaker's losing
                    // bootstraps either way.
                    let pruned = self.prune_settled_pending(&successor);
                    let activated = self.activate(&successor);
                    if activated {
                        ReconfigureReply::Learned {
                            matchmaker: me,
                            generation,
                            activated,
                        }
                    } else {
                        if pruned {
                            self.stage_scalars();
                        }
                        refused(self)
                    }
                } else {
                    refused(self)
                }
            }
        };
        self.pending_reconfigure_replies.push(reply);
        self.assert_invariants();
    }

    /// Freeze the current generation (durable before any reply).
    fn freeze(&mut self) {
        let set = self.set();
        self.hard_state.generation = set.generation;
        self.hard_state.members = set.members;
        self.hard_state.phase = MatchmakerPhase::Stopped;
        self.stage_scalars();
    }

    /// Activate `successor` if this matchmaker is one of its members holding
    /// its pending bootstrap and stands strictly below it: the reconstructed
    /// registry replaces the current one whole, the watermark becomes the
    /// reconstructed one (never lower than the one held — the maximum over a
    /// frozen quorum that this matchmaker, if it was in it, contributed to),
    /// the decree record resets for the new generation, and every pending
    /// bootstrap at or below the new generation is dropped.
    fn activate(&mut self, successor: &MatchmakerSet) -> bool {
        if !successor.contains(self.config.id) || successor.generation <= self.set().generation {
            return false;
        }
        assert!(
            successor.is_well_formed(),
            "an activated matchmaker set admits the matchmaker quorum system"
        );
        let Some(index) = self
            .hard_state
            .pending
            .iter()
            .position(|p| p.set == *successor)
        else {
            return false;
        };
        let bootstrap = self.hard_state.pending.remove(index);
        // The activated watermark is the maximum of the reconstructed one
        // and this matchmaker's own. Both are legitimate floors over the
        // reconstructed registry: the reconstructed one is the maximum over
        // a frozen quorum of `M_g`, and the local one was raised only by a
        // `GarbageA` whose leader had established the §3.5 preconditions —
        // "everything below `i` may be forgotten" is a fact about the
        // cluster, not about the matchmaker that happened to hear it first,
        // so applying it to the reconstruction forgets nothing a future
        // Phase 1 can need. What the local floor can never do is *add*
        // knowledge: the registry installed is exactly the reconstructed
        // history at or above the higher floor, and nothing else.
        let local = self.hard_state.gc_watermark;
        let watermark = bootstrap.gc_watermark.max(local);
        let registry: BTreeMap<Ballot, Registration> = bootstrap
            .history
            .into_iter()
            .filter(|(b, _)| *b >= watermark)
            .collect();
        assert!(
            watermark >= bootstrap.gc_watermark && watermark >= local,
            "an activation never lowers either watermark it inherits"
        );
        assert!(
            registry.keys().all(|b| *b >= watermark),
            "an activated registry holds nothing below the activated watermark"
        );
        self.hard_state.generation = successor.generation;
        self.hard_state.members.clone_from(&successor.members);
        self.hard_state.phase = MatchmakerPhase::Active;
        self.hard_state.successor = None;
        self.hard_state.decree = DecreeAcceptor::default();
        self.hard_state.gc_watermark = watermark;
        self.hard_state
            .pending
            .retain(|p| p.set.generation > successor.generation);
        self.registry = registry.clone();
        // One write for the whole activation: a crash between "registry
        // replaced" and "scalars advanced" would boot a matchmaker answering
        // the wrong generation from the wrong registry.
        self.pending_writes
            .push(MatchmakerWriteOp::InstallRegistry {
                scalars: self.hard_state.clone(),
                registrations: registry,
            });
        true
    }

    /// Drop every pending bootstrap the chosen `successor` settles: a
    /// proposal at or below the successor's generation that is not the
    /// successor itself lost its decree and can never be activated. Keeping
    /// one is not free — the pending list holds a whole reconstructed
    /// registry per proposal and rides inside every later
    /// [`MatchmakerWriteOp::SetScalars`], so an unpruned loser makes every
    /// freeze, promise and vote of every later generation more expensive,
    /// forever. Returns whether anything was dropped.
    fn prune_settled_pending(&mut self, successor: &MatchmakerSet) -> bool {
        let before = self.hard_state.pending.len();
        self.hard_state
            .pending
            .retain(|p| p.set.generation > successor.generation || p.set == *successor);
        self.hard_state.pending.len() != before
    }

    /// Stage a whole-scalars write.
    fn stage_scalars(&mut self) {
        self.pending_writes
            .push(MatchmakerWriteOp::SetScalars(self.hard_state.clone()));
    }

    /// Drain the pending batch. Holds the unique borrow until
    /// [`MatchmakerReady::advance`], so a second `ready()` before `advance()`
    /// is a compile error.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(matchmaker = self.config.id.0)))]
    pub fn ready(&mut self) -> MatchmakerReady<'_> {
        MatchmakerReady { matchmaker: self }
    }

    /// Every registration in `[gc_watermark, ballot)`, in ballot order.
    fn history_below(&self, ballot: Ballot) -> BTreeMap<Ballot, Registration> {
        self.registry
            .range(self.hard_state.gc_watermark..ballot)
            .map(|(b, c)| (*b, c.clone()))
            .collect()
    }

    /// The cross-field checker, called at boot and at every public mutating
    /// entry point: no registration below the watermark, every registration
    /// a well-formed configuration, a frozen or activated generation with a
    /// durable membership, pending bootstraps strictly above it, and no
    /// pending bootstrap a recorded successor has already settled.
    fn assert_invariants(&self) {
        assert!(
            self.registry
                .keys()
                .next()
                .is_none_or(|lowest| *lowest >= self.hard_state.gc_watermark),
            "no registration survives below the gc watermark"
        );
        for registration in self.registry.values() {
            assert!(
                !registration.config.members.is_empty(),
                "a registered configuration names at least one acceptor"
            );
            assert!(
                registration.config.members.windows(2).all(|w| w[0] < w[1]),
                "a registered membership is sorted and deduplicated"
            );
        }
        if self.hard_state.generation > MatchmakerGeneration(0) {
            assert!(
                !self.hard_state.members.is_empty(),
                "an activated generation has a durable membership"
            );
            assert!(
                self.hard_state.phase != MatchmakerPhase::Fresh,
                "an activated generation is never fresh"
            );
        }
        if let Some(successor) = &self.hard_state.successor {
            assert!(
                successor.generation == self.hard_state.generation.next(),
                "a recorded successor is the next generation"
            );
            assert!(
                self.hard_state.phase == MatchmakerPhase::Stopped,
                "a generation with a successor is frozen"
            );
            assert!(
                self.hard_state
                    .pending
                    .iter()
                    .all(|p| p.set.generation > successor.generation || p.set == *successor),
                "a settled generation keeps no pending bootstrap but the successor's"
            );
        }
        let current = self.set().generation;
        assert!(
            self.hard_state
                .pending
                .iter()
                .all(|p| p.set.generation > current && p.set.contains(self.config.id)),
            "a pending bootstrap is for a later generation this matchmaker is a member of"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::QuorumSystem;

    /// Review 3 of #133: a member whose own GC floor sits *above* the
    /// reconstructed one activates with the higher floor, and its registry is
    /// exactly the reconstruction at or above that floor — before and after a
    /// restart from the durable install.
    #[test]
    fn activation_applies_the_higher_of_the_local_and_reconstructed_floors() {
        let cfg =
            |n: u64| AcceptorConfig::new(vec![NodeId(n), NodeId(n + 1)], QuorumSystem::Majority);
        // Matchmaker 0, a member of M_0 = {0, 1, 2}: registers ballots 1..=6
        // and hears a GarbageA raising its floor to ballot 5.
        let mut mm = fresh(0);
        for round in 1..=6 {
            mm.step(MatchRequest::new(
                NodeId(1),
                ballot(round, 1),
                cfg(round),
                G0,
            ));
            mm.ready().advance();
        }
        assert_eq!(mm.advance_gc_watermark(G0, ballot(5, 1)), GcOutcome::Raised);
        mm.ready().advance();
        assert_eq!(
            mm.registry().len(),
            2,
            "ballots 5 and 6 survive the local floor"
        );
        // A reconstruction from *another* frozen quorum ({1, 2}) that never
        // heard the GarbageA: floor at ballot 2, history 2..=6.
        let history: BTreeMap<Ballot, Registration> = (2..=6)
            .map(|round| (ballot(round, 1), Registration::belief(cfg(round))))
            .collect();
        let successor = MatchmakerSet::new(
            MatchmakerGeneration(1),
            vec![MatchmakerId(0), MatchmakerId(3)],
        );
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(9),
            bootstrap: PendingBootstrap {
                set: successor.clone(),
                gc_watermark: ballot(2, 1),
                history,
            },
        });
        mm.ready().advance();
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(9),
            generation: G0,
            successor: successor.clone(),
        });
        let ready = mm.ready();
        let installed = ready.writes().iter().find_map(|op| match op {
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => Some((scalars.gc_watermark, registrations.clone())),
            _ => None,
        });
        ready.advance();
        let (durable_floor, durable_registry) =
            installed.expect("the activation installs the registry");
        let expected: Vec<Ballot> = vec![ballot(5, 1), ballot(6, 1)];
        assert_eq!(mm.set(), successor);
        assert_eq!(
            mm.hard_state().gc_watermark,
            ballot(5, 1),
            "the higher floor wins"
        );
        assert_eq!(mm.registry().keys().copied().collect::<Vec<_>>(), expected);
        assert_eq!(durable_floor, ballot(5, 1));
        assert_eq!(
            durable_registry.keys().copied().collect::<Vec<_>>(),
            expected
        );
        // The restart reads back exactly that.
        let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &TestRegistry::of(&mm));
        assert_eq!(rebooted.set(), successor);
        assert_eq!(rebooted.hard_state().gc_watermark, ballot(5, 1));
        assert_eq!(
            rebooted.registry().keys().copied().collect::<Vec<_>>(),
            expected
        );
        assert_eq!(rebooted.phase(), MatchmakerPhase::Active);
    }

    /// The port over an in-memory registry, for the state-machine tests.
    #[derive(Default)]
    struct TestRegistry {
        hard_state: MatchmakerHardState,
        registry: BTreeMap<Ballot, Registration>,
    }

    impl TestRegistry {
        /// What a reboot of `mm` reads back: its durable scalars and records.
        fn of(mm: &Matchmaker) -> Self {
            Self {
                hard_state: mm.hard_state().clone(),
                registry: mm.registry().clone(),
            }
        }
    }

    impl RegistryStorage for TestRegistry {
        fn initial_state(&self) -> MatchmakerHardState {
            self.hard_state.clone()
        }

        fn registration(&self, ballot: Ballot) -> Option<Registration> {
            self.registry.get(&ballot).cloned()
        }

        fn registered_ballots(&self) -> Vec<Ballot> {
            self.registry.keys().copied().collect()
        }
    }

    const G0: MatchmakerGeneration = MatchmakerGeneration(0);

    fn mmconfig(id: u64, bootstrap: &[u64]) -> MatchmakerConfig {
        MatchmakerConfig {
            id: MatchmakerId(id),
            bootstrap: bootstrap.iter().copied().map(MatchmakerId).collect(),
        }
    }

    fn fresh(id: u64) -> Matchmaker {
        Matchmaker::new(&mmconfig(id, &[0, 1, 2]), &TestRegistry::default())
    }

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn config(members: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            members.iter().map(|n| NodeId(*n)).collect(),
            QuorumSystem::Majority,
        )
    }

    fn request(from: u64, ballot: Ballot, members: &[u64]) -> MatchRequest {
        MatchRequest::new(NodeId(from), ballot, config(members), G0)
    }

    fn drain(mm: &mut Matchmaker) -> (Vec<MatchmakerWriteOp>, Vec<MatchReply>) {
        let ready = mm.ready();
        let writes = ready.writes().to_vec();
        let replies = ready.replies().to_vec();
        ready.advance();
        (writes, replies)
    }

    fn drain_reconfigure(mm: &mut Matchmaker) -> (Vec<MatchmakerWriteOp>, Vec<ReconfigureReply>) {
        let ready = mm.ready();
        let writes = ready.writes().to_vec();
        let replies = ready.reconfigure_replies().to_vec();
        ready.advance();
        (writes, replies)
    }

    fn registered(reply: &MatchReply) -> (&BTreeMap<Ballot, Registration>, Ballot) {
        match &reply.outcome {
            MatchOutcome::Registered {
                history,
                gc_watermark,
            } => (history, *gc_watermark),
            MatchOutcome::Refused(r) => panic!("expected a registration, got {r:?}"),
        }
    }

    fn set(generation: u64, members: &[u64]) -> MatchmakerSet {
        MatchmakerSet::new(
            MatchmakerGeneration(generation),
            members.iter().copied().map(MatchmakerId).collect(),
        )
    }

    /// Review finding P7: a second `Chosen` for one generation naming a
    /// *different* successor is the shape a wrong relay produces. It is
    /// refused — nothing recorded, nothing activated — and the refusal names
    /// the successor this matchmaker holds.
    #[test]
    fn a_chosen_contradicting_the_recorded_successor_is_refused() {
        let mut mm = fresh(0);
        let recorded = set(1, &[0, 1, 2]);
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: recorded.clone(),
        });
        drain_reconfigure(&mut mm);
        assert_eq!(mm.successor(), Some(&recorded));
        // The same publication again is idempotent.
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: recorded.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Learned {
                activated: false,
                ..
            }
        ));
        assert!(writes.is_empty(), "a duplicate publication writes nothing");
        // A contradicting one is not.
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(6),
            generation: G0,
            successor: set(1, &[0, 3, 4]),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(
                &replies[0],
                ReconfigureReply::Refused { successor: Some(s), .. } if *s == recorded
            ),
            "the refusal names the recorded successor, not the contradicting one"
        );
        assert!(
            writes.is_empty(),
            "a contradiction changes no durable state"
        );
        assert_eq!(mm.successor(), Some(&recorded));
        assert_eq!(mm.set(), set(0, &[0, 1, 2]), "and activates nothing");
    }

    /// Review finding P6: a matchmaker bootstrapped into a proposal that
    /// *lost* its decree must not carry that whole reconstructed registry in
    /// its durable scalars forever. The learn path prunes it — here at a
    /// spare, which is outside the chosen set and so never activates, the
    /// branch where nothing else would ever drop it.
    #[test]
    fn a_losing_bootstrap_is_dropped_when_the_successor_is_learned() {
        // Matchmaker 3 is a spare of M_0 = {0, 1, 2}: it is a member of the
        // proposed {0, 1, 3} and of nothing else.
        let mut mm = Matchmaker::new(&mmconfig(3, &[0, 1, 2]), &TestRegistry::default());
        assert_eq!(mm.phase(), MatchmakerPhase::Inactive);
        let losing = set(1, &[0, 1, 3]);
        let mut history = BTreeMap::new();
        history.insert(ballot(1, 1), Registration::belief(config(&[0, 1, 2])));
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: PendingBootstrap {
                set: losing.clone(),
                gc_watermark: Ballot::zero(),
                history: history.clone(),
            },
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "the pending bootstrap is durable");
        assert!(matches!(&replies[0], ReconfigureReply::Bootstrapped { .. }));
        assert_eq!(mm.hard_state().pending.len(), 1);
        // A different set wins generation 1, and this matchmaker is not in it.
        let winner = set(1, &[0, 1, 2]);
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: winner.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(&replies[0], ReconfigureReply::Refused { .. }),
            "a spare outside the chosen set activates nothing"
        );
        assert!(
            mm.hard_state().pending.is_empty(),
            "the losing bootstrap is gone"
        );
        assert!(
            writes
                .iter()
                .any(|w| matches!(w, MatchmakerWriteOp::SetScalars(scalars) if scalars.pending.is_empty())),
            "the prune is durable"
        );
        // And the loser cannot come back: a resent bootstrap for a settled
        // generation is refused. (Here from the other side — a member of the
        // succeeded generation that recorded the successor.)
        let mut member = fresh(0);
        member.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: set(1, &[0, 1, 2]),
        });
        drain_reconfigure(&mut member);
        member.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: PendingBootstrap {
                set: set(1, &[0, 1, 3]),
                gc_watermark: Ballot::zero(),
                history,
            },
        });
        let (writes, replies) = drain_reconfigure(&mut member);
        assert!(matches!(&replies[0], ReconfigureReply::Refused { .. }));
        assert!(writes.is_empty(), "a settled proposal is not stored");
        assert!(member.hard_state().pending.is_empty());
    }

    #[test]
    fn a_fresh_request_registers_and_reports_the_history_strictly_below() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert_eq!(
            writes,
            vec![MatchmakerWriteOp::Register {
                ballot: ballot(1, 1),
                registration: Registration::belief(config(&[0, 1, 2])),
            }]
        );
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].to, NodeId(1));
        assert_eq!(replies[0].generation, G0);
        let (history, watermark) = registered(&replies[0]);
        assert!(history.is_empty(), "the first registration has no past");
        assert_eq!(watermark, Ballot::zero());

        mm.step(request(2, ballot(3, 2), &[1, 2, 3]));
        let (_, replies) = drain(&mut mm);
        let (history, _) = registered(&replies[0]);
        // Exactly the ballots strictly below: `< b`, not `<= b`.
        assert_eq!(
            history.keys().copied().collect::<Vec<_>>(),
            vec![ballot(1, 1)]
        );
        assert_eq!(history[&ballot(1, 1)].config, config(&[0, 1, 2]));
        assert_eq!(mm.highest(), Some(ballot(3, 2)));
    }

    #[test]
    fn a_request_at_or_below_the_highest_is_refused_with_the_highest() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
        drain(&mut mm);
        // Strictly below.
        mm.step(request(2, ballot(4, 9), &[0, 1, 2]));
        // Same round, lower node: below in the total order.
        mm.step(request(0, ballot(5, 0), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty(), "a refusal writes nothing");
        for reply in &replies {
            assert_eq!(
                reply.outcome,
                MatchOutcome::Refused(MatchRefusal::Stale {
                    highest: ballot(5, 1)
                })
            );
        }
        assert_eq!(mm.registry().len(), 1);
    }

    #[test]
    fn the_same_request_again_is_answered_without_a_second_registration() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert_eq!(writes.len(), 2);
        let (first_history, _) = registered(&replies[1]);
        let first_history = first_history.clone();

        mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty(), "a duplicate never re-registers");
        let (history, _) = registered(&replies[0]);
        assert_eq!(*history, first_history, "the re-answer is the first answer");
        assert_eq!(mm.registry().len(), 2);
    }

    #[test]
    fn a_registered_ballot_with_different_bytes_is_refused_write_once() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        drain(&mut mm);
        mm.step(request(1, ballot(1, 1), &[0, 1]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty());
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Stale {
                highest: ballot(1, 1)
            })
        );
        assert_eq!(mm.registry()[&ballot(1, 1)].config, config(&[0, 1, 2]));
    }

    #[test]
    fn a_request_below_the_watermark_is_refused_and_the_history_starts_at_it() {
        let mut mm = fresh(0);
        for round in 1..=4 {
            mm.step(request(1, ballot(round, 1), &[0, 1, 2]));
        }
        drain(&mut mm);
        assert_eq!(mm.advance_gc_watermark(G0, ballot(3, 1)), GcOutcome::Raised);
        assert_eq!(
            mm.advance_gc_watermark(G0, ballot(2, 1)),
            GcOutcome::Unchanged,
            "the floor never lowers"
        );
        assert_eq!(
            mm.advance_gc_watermark(G0, ballot(3, 1)),
            GcOutcome::Unchanged,
            "re-raising is a no-op"
        );
        assert_eq!(
            mm.advance_gc_watermark(MatchmakerGeneration(1), ballot(9, 1)),
            GcOutcome::Refused,
            "another generation's floor is refused"
        );
        let (writes, _) = drain(&mut mm);
        assert_eq!(
            writes,
            vec![MatchmakerWriteOp::SetGcWatermark(ballot(3, 1))]
        );
        assert_eq!(
            mm.registry().keys().copied().collect::<Vec<_>>(),
            vec![ballot(3, 1), ballot(4, 1)],
            "collected registrations are dropped"
        );

        mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
        mm.step(request(1, ballot(6, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                watermark: ballot(3, 1)
            })
        );
        let (history, watermark) = registered(&replies[1]);
        assert_eq!(watermark, ballot(3, 1));
        assert_eq!(
            history.keys().copied().collect::<Vec<_>>(),
            vec![ballot(3, 1), ballot(4, 1)]
        );
    }

    /// The must-fix of #131's review: a retry after GC is answered from the
    /// *retained* history — a strict subset of the first answer, with the
    /// raised watermark beside it — and still registers nothing.
    #[test]
    fn a_retry_after_gc_is_answered_from_the_retained_history() {
        let mut mm = fresh(0);
        for round in 1..=3 {
            mm.step(request(1, ballot(round, 1), &[0, 1, 2]));
        }
        let (_, replies) = drain(&mut mm);
        let (first, watermark) = registered(&replies[2]);
        assert_eq!(
            first.keys().copied().collect::<Vec<_>>(),
            vec![ballot(1, 1), ballot(2, 1)]
        );
        assert_eq!(watermark, Ballot::zero());

        // The first reply is lost; GC moves the floor; the client retries.
        assert_eq!(mm.advance_gc_watermark(G0, ballot(2, 1)), GcOutcome::Raised);
        drain(&mut mm);
        mm.step(request(1, ballot(3, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty(), "a retry never re-registers");
        let (retry, watermark) = registered(&replies[0]);
        assert_eq!(
            retry.keys().copied().collect::<Vec<_>>(),
            vec![ballot(2, 1)],
            "the retry sees the retained window only"
        );
        assert_eq!(watermark, ballot(2, 1), "and the raised floor beside it");
        assert!(
            retry.iter().all(|(b, c)| first.get(b) == Some(c)),
            "a re-answer is a subset of the first answer, never a superset"
        );
    }

    /// The paros-specific keying: two proposers at the *same round* hold two
    /// distinct registry keys, ordered by node, so neither can overwrite the
    /// other and the lower one is an ancestor in the higher one's history. A
    /// registry keyed on the bare round would give them one key.
    #[test]
    fn ballots_at_one_round_from_two_proposers_are_distinct_keys() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
        mm.step(request(2, ballot(5, 2), &[3, 4, 5]));
        let (writes, replies) = drain(&mut mm);
        assert_eq!(writes.len(), 2, "two keys, two registrations");
        let (history, _) = registered(&replies[1]);
        assert_eq!(
            history.get(&ballot(5, 1)).map(|r| &r.config),
            Some(&config(&[0, 1, 2])),
            "the same-round lower ballot is in the higher one's history"
        );
        assert_eq!(mm.registry().len(), 2);
        assert_eq!(mm.registry()[&ballot(5, 1)].config, config(&[0, 1, 2]));
        assert_eq!(mm.registry()[&ballot(5, 2)].config, config(&[3, 4, 5]));

        // The reverse arrival order: the lower node's ballot is stale once
        // the higher node's is registered, and refused — never merged into it.
        let mut mm = fresh(1);
        mm.step(request(2, ballot(5, 2), &[3, 4, 5]));
        mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        assert_eq!(
            replies[1].outcome,
            MatchOutcome::Refused(MatchRefusal::Stale {
                highest: ballot(5, 2)
            })
        );
        assert_eq!(mm.registry().len(), 1);
    }

    #[test]
    fn a_restart_answers_exactly_as_the_original_would_have() {
        let mut mm = fresh(2);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        mm.step(request(2, ballot(2, 2), &[0, 1, 2, 3]));
        drain(&mut mm);
        // A reboot walks the durable records back through the port.
        let mut rebooted = Matchmaker::new(&mmconfig(2, &[0, 1, 2]), &TestRegistry::of(&mm));
        rebooted.step(request(1, ballot(3, 1), &[1, 2, 3]));
        mm.step(request(1, ballot(3, 1), &[1, 2, 3]));
        let (_, expected) = drain(&mut mm);
        let (_, observed) = drain(&mut rebooted);
        assert_eq!(observed, expected);
    }

    #[test]
    #[should_panic(expected = "no registration survives below the gc watermark")]
    fn a_registry_below_its_watermark_refuses_to_boot() {
        let mut store = TestRegistry::default();
        store
            .registry
            .insert(ballot(1, 1), Registration::belief(config(&[0])));
        store.hard_state.gc_watermark = ballot(2, 0);
        let _ = Matchmaker::new(&mmconfig(0, &[0]), &store);
    }

    #[test]
    fn a_configuration_is_normalized_and_sizes_its_quorum() {
        let config = AcceptorConfig::new(
            vec![NodeId(2), NodeId(0), NodeId(2), NodeId(1)],
            QuorumSystem::Majority,
        );
        assert_eq!(config.members, vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(config.quorum_size(), 2);
    }

    // ---- generations (#125) ---------------------------------------------

    /// A fresh store resolves against the bootstrap set: a member is active
    /// for generation 0, a spare is inactive and refuses everything with
    /// `Inactive` — neither writes anything at boot.
    #[test]
    fn a_fresh_matchmaker_resolves_its_phase_from_the_bootstrap_set() {
        let member = fresh(0);
        assert_eq!(member.phase(), MatchmakerPhase::Active);
        assert_eq!(member.set(), set(0, &[0, 1, 2]));
        let mut spare = Matchmaker::new(&mmconfig(7, &[0, 1, 2]), &TestRegistry::default());
        assert_eq!(spare.phase(), MatchmakerPhase::Inactive);
        spare.step(request(1, ballot(1, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut spare);
        assert!(writes.is_empty());
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Inactive)
        );
    }

    /// A request fenced by another generation is refused with the current
    /// set, never served.
    #[test]
    fn a_request_for_another_generation_is_refused_with_the_current_set() {
        let mut mm = fresh(0);
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(1, 1),
            config(&[0, 1, 2]),
            MatchmakerGeneration(3),
        ));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty());
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Generation {
                current: set(0, &[0, 1, 2])
            })
        );
    }

    /// The freeze: a stop is durable before its answer, idempotent, and a
    /// frozen matchmaker registers nothing for its generation ever again —
    /// across a reboot too.
    #[test]
    fn a_stop_freezes_durably_and_is_idempotent() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        drain(&mut mm);
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(5),
            generation: G0,
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(writes.as_slice(), [MatchmakerWriteOp::SetScalars(s)] if s.phase == MatchmakerPhase::Stopped)
        );
        let ReconfigureReply::Stopped {
            generation,
            history,
            successor,
            ..
        } = &replies[0]
        else {
            panic!("expected Stopped, got {:?}", replies[0]);
        };
        assert_eq!(*generation, G0);
        assert_eq!(history.len(), 1);
        assert!(successor.is_none());
        // Idempotent: no second write.
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(6),
            generation: G0,
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(writes.is_empty(), "a re-sent stop writes nothing");
        assert!(matches!(replies[0], ReconfigureReply::Stopped { .. }));
        // Frozen, and still frozen after a reboot.
        for mm in [
            &mut mm.clone(),
            &mut Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &TestRegistry::of(&mm)),
        ] {
            mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
            let (writes, replies) = drain(mm);
            assert!(writes.is_empty());
            assert_eq!(
                replies[0].outcome,
                MatchOutcome::Refused(MatchRefusal::Stopped { successor: None })
            );
        }
        // A stop for another generation is refused with what is known.
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(5),
            generation: MatchmakerGeneration(4),
        });
        let (_, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Refused { current, phase: MatchmakerPhase::Stopped, .. } if *current == set(0, &[0, 1, 2])
        ));
    }

    /// The handover end to end at one matchmaker in both generations: stop,
    /// bootstrap (pending, refused to serve), decree votes (durable), chosen
    /// (activates the pending registry whole, at the reconstructed
    /// watermark), and the new generation serves from the reconstruction.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn a_chosen_successor_activates_its_pending_bootstrap() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        drain(&mut mm);
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(5),
            generation: G0,
        });
        drain_reconfigure(&mut mm);
        let successor = set(1, &[0, 3, 4]);
        let mut history = BTreeMap::new();
        history.insert(ballot(1, 1), Registration::belief(config(&[0, 1, 2])));
        history.insert(ballot(2, 2), Registration::belief(config(&[1, 2, 3])));
        let bootstrap = PendingBootstrap {
            set: successor.clone(),
            gc_watermark: ballot(1, 1),
            history,
        };
        // A bootstrap for a set this matchmaker is not in is refused.
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: PendingBootstrap {
                set: set(1, &[3, 4, 5]),
                ..bootstrap.clone()
            },
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(writes.is_empty());
        assert!(matches!(replies[0], ReconfigureReply::Refused { .. }));
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: bootstrap.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "the pending bootstrap is durable");
        assert!(
            matches!(&replies[0], ReconfigureReply::Bootstrapped { set: s, .. } if *s == successor)
        );
        // Still frozen at generation 0: generation 1 is not served yet.
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(5, 1),
            config(&[0, 1, 2]),
            MatchmakerGeneration(1),
        ));
        let (_, replies) = drain(&mut mm);
        assert!(matches!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Inactive)
        ));
        // The decree over generation 0.
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(5),
            generation: G0,
            ballot: ballot(1, 5),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "a promise is durable");
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Promised { vote: None, .. }
        ));
        mm.step_reconfigure(ReconfigureRequest::DecreeAccept {
            from: NodeId(5),
            generation: G0,
            ballot: ballot(1, 5),
            members: successor.members.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "a vote is durable");
        assert!(matches!(&replies[0], ReconfigureReply::Accepted { .. }));
        // A lower decree ballot is refused with the promise.
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(4),
            generation: G0,
            ballot: ballot(1, 4),
        });
        let (_, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(&replies[0], ReconfigureReply::Nacked { promised, .. } if *promised == ballot(1, 5))
        );
        // A higher one learns the vote (P2c's input).
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(4),
            generation: G0,
            ballot: ballot(2, 4),
        });
        let (_, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(&replies[0], ReconfigureReply::Promised { vote: Some((b, m)), .. } if *b == ballot(1, 5) && *m == successor.members)
        );
        // Chosen: the successor is recorded for generation 0 and, being a
        // member holding the bootstrap, this matchmaker activates it.
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: successor.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Learned {
                activated: true,
                ..
            }
        ));
        assert!(
            matches!(writes.last(), Some(MatchmakerWriteOp::InstallRegistry { scalars, registrations }) if scalars.generation == MatchmakerGeneration(1) && registrations.len() == 2)
        );
        assert_eq!(mm.phase(), MatchmakerPhase::Active);
        assert_eq!(mm.set(), successor);
        assert_eq!(mm.hard_state().gc_watermark, ballot(1, 1));
        assert!(mm.successor().is_none());
        assert_eq!(mm.hard_state().decree, DecreeAcceptor::default());
        // Generation 1 serves from the reconstruction; generation 0 is told
        // the chain link is gone from here (this matchmaker moved on).
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(5, 1),
            config(&[0, 1, 2]),
            MatchmakerGeneration(1),
        ));
        mm.step(request(1, ballot(6, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        let (history, wm) = registered(&replies[0]);
        assert_eq!(history.len(), 2);
        assert_eq!(wm, ballot(1, 1));
        assert_eq!(
            replies[1].outcome,
            MatchOutcome::Refused(MatchRefusal::Generation {
                current: successor.clone()
            })
        );
        // A reboot lands in generation 1 with the installed registry (plus
        // the one registration generation 1 took above).
        let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &TestRegistry::of(&mm));
        assert_eq!(rebooted.set(), successor);
        assert_eq!(rebooted.registry().len(), 3);
    }

    /// A member of the succeeded generation that is *not* in the successor
    /// records the chain link (freezing on the spot if it was still active)
    /// and points late proposers at it forever.
    #[test]
    fn a_departed_matchmaker_answers_with_its_successor() {
        let mut mm = fresh(2);
        let successor = set(1, &[0, 1, 3]);
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: successor.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Learned {
                activated: false,
                ..
            }
        ));
        assert_eq!(writes.len(), 2, "the freeze and the link are durable");
        assert_eq!(mm.phase(), MatchmakerPhase::Stopped);
        assert_eq!(mm.successor(), Some(&successor));
        mm.step(request(1, ballot(9, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Stopped {
                successor: Some(successor.clone())
            })
        );
        // A second, different "chosen" for the same generation is refused
        // (the first record stands; the audit judges the conflict).
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(6),
            generation: G0,
            successor: set(1, &[4, 5, 6]),
        });
        let (writes, _) = drain_reconfigure(&mut mm);
        assert!(writes.is_empty());
        assert_eq!(mm.successor(), Some(&successor));
    }
}
