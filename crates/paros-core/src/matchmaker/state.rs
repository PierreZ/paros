//! The matchmaker's **durable state**: its generation phase, the pending
//! bootstraps of a proposed successor, the scalars persisted whole (the set's
//! generation and every journal's watermark and effective configuration,
//! #190), its static configuration, and the ledger record a registration
//! writes.
//!
//! Everything here is what a reboot reads back through
//! [`RegistryStorage`](super::RegistryStorage); nothing here decides anything.

use std::collections::BTreeMap;

use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::types::{Ballot, JournalKey};

/// This matchmaker's durable **acceptor record** in the successor decree: the
/// promise it made and the vote it cast, the two scalars of Paxos's acceptor
/// half over a one-value log.
///
/// It is the persisted shape only. The decisions are the shared
/// [`Acceptor`](crate::acceptor::Acceptor)'s
/// ([`Matchmaker::decree_acceptor`](super::Matchmaker)), reconstructed over
/// this record for each decree message: a matchmaker is an acceptor of
/// exactly one value, at slot zero, with no compaction floor and no
/// tri-state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DecreeRecord {
    /// The highest ballot promised. Monotone.
    pub promised: Ballot,
    /// The highest-ballot value accepted, if any.
    pub vote: Option<(Ballot, Vec<MatchmakerId>)>,
}

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

/// One journal's registry scalars (#190): its GC watermark and its effective
/// configuration. A matchmaker set serves every journal of its tenant, one
/// registry per journal: the generation is the set's, the watermark and the
/// effective configuration are each journal's own — a set's journals have
/// independent leaders and independent floors.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RegistryScalars {
    /// The journal's GC watermark (§3.4): a monotone floor below which no
    /// request may register and below which registrations were dropped.
    /// Raised only by
    /// [`Matchmaker::advance_gc_watermark`](crate::Matchmaker::advance_gc_watermark),
    /// carried into every successor generation. [`Ballot::zero`] is the
    /// "nothing collected" floor.
    pub gc_watermark: Ballot,
    /// The journal's **effective configuration** and the ballot its
    /// reconfiguration registration was made under: the highest-ballot
    /// flagged registration ([`RegistrationKind::Reconfiguration`]) this
    /// matchmaker has ever accepted for the journal. Monotone in the ballot,
    /// durable before the reply that reports it, carried into every
    /// successor generation — and, unlike the record it is derived from,
    /// **never collected**.
    ///
    /// It exists because the GC watermark and the effective configuration
    /// answer different questions. The watermark says "no future Phase 1
    /// needs a configuration registered below here"; the effective
    /// configuration says "this is the acceptor set in force". A leader's
    /// GC raises the floor to its own ballot, and an *ordinary* leader
    /// registers only a belief, so the floor routinely rises above the last
    /// reconfiguration record — after which, without this scalar, no
    /// campaign's histories named a reconfiguration at all and a node that
    /// rebooted to its bootstrap belief was elected under a superseded
    /// configuration, rolling the whole cluster back.
    pub effective: Option<(Ballot, AcceptorConfig)>,
}

/// One journal's registry as a frozen matchmaker reports it and a
/// reconstruction carries it (#190): its scalars and every registration at
/// or above its watermark (all of them, or a page of them).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RegistrySnapshot {
    /// The journal's GC watermark.
    pub gc_watermark: Ballot,
    /// `ballot -> registration` at or above the watermark.
    pub history: BTreeMap<Ballot, Registration>,
    /// The journal's effective configuration (see
    /// [`RegistryScalars::effective`]).
    pub effective: Option<(Ballot, AcceptorConfig)>,
}

impl RegistrySnapshot {
    /// Fold `other` into this snapshot — the reconstruction's rule (§5),
    /// commutative and idempotent: the maximum watermark, the union of the
    /// histories at or above it, the highest effective configuration.
    /// Returns how many ballots the two held with *different* registrations
    /// (the first one kept stays).
    pub fn merge(&mut self, other: RegistrySnapshot) -> u64 {
        let mut disagreements = 0;
        self.gc_watermark = self.gc_watermark.max(other.gc_watermark);
        for (ballot, registration) in other.history {
            match self.history.entry(ballot) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(registration);
                }
                std::collections::btree_map::Entry::Occupied(slot) => {
                    if *slot.get() != registration {
                        disagreements += 1;
                    }
                }
            }
        }
        let floor = self.gc_watermark;
        self.history.retain(|ballot, _| *ballot >= floor);
        if let Some((ballot, config)) = &other.effective {
            raise_effective(&mut self.effective, *ballot, config);
        }
        disagreements
    }

    /// Its scalars.
    #[must_use]
    pub fn scalars(&self) -> RegistryScalars {
        RegistryScalars {
            gc_watermark: self.gc_watermark,
            effective: self.effective.clone(),
        }
    }
}

/// Where a paged registry transfer resumes (#190): a journal, and the first
/// ballot of it not transferred yet. Journals are walked in order, ballots
/// in order inside each.
pub type RegistryCursor = (JournalKey, Ballot);

/// One page of `registries` starting at `from` (the start when `None`): at
/// most `limit` registrations (a journal with none left still costs one, so
/// its scalars ride the page), each journal in it with its scalars and the
/// part of its history the page covers, and the cursor the next page starts
/// at (`None`: this was the last).
///
/// # Panics
///
/// If `limit` is zero.
#[must_use]
pub fn registry_page(
    registries: &BTreeMap<JournalKey, RegistrySnapshot>,
    from: Option<RegistryCursor>,
    limit: usize,
) -> (
    BTreeMap<JournalKey, RegistrySnapshot>,
    Option<RegistryCursor>,
) {
    assert!(limit > 0, "a registry page carries at least one entry");
    let mut page = BTreeMap::new();
    let mut taken = 0;
    for (journal, snapshot) in registries {
        let start = match from {
            Some((first, _)) if *journal < first => continue,
            Some((first, ballot)) if *journal == first => ballot,
            _ => Ballot::zero(),
        };
        if taken >= limit {
            return (page, Some((*journal, start)));
        }
        let mut entry = RegistrySnapshot {
            gc_watermark: snapshot.gc_watermark,
            history: BTreeMap::new(),
            effective: snapshot.effective.clone(),
        };
        let mut records = snapshot.history.range(start..).peekable();
        if records.peek().is_none() {
            taken += 1;
        }
        while let Some((ballot, registration)) = records.next() {
            entry.history.insert(*ballot, registration.clone());
            taken += 1;
            if taken >= limit {
                if let Some((next, _)) = records.peek() {
                    page.insert(*journal, entry);
                    return (page, Some((*journal, **next)));
                }
                break;
            }
        }
        page.insert(*journal, entry);
    }
    (page, None)
}

/// A successor generation's initial state, handed to each of its members by
/// the reconfigurer and held **pending** until the decree chooses that set:
/// one [`RegistrySnapshot`] per journal of the set (#190). It arrives in
/// pages, each merged in ([`PendingBootstrap::merge`]); it becomes the
/// per-record registries only at activation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PendingBootstrap {
    /// The proposed successor set.
    pub set: MatchmakerSet,
    /// The reconstructed registry of every journal: the maximum watermark
    /// over the frozen quorum, the union of the histories at or above it,
    /// the maximum effective configuration — carried separately from the
    /// history because it is a monotone scalar the GC watermark never
    /// collects.
    pub registries: BTreeMap<JournalKey, RegistrySnapshot>,
}

impl PendingBootstrap {
    /// Fold another page (or another reconstruction) of the same proposed
    /// set in. Safe across reconfigurers: every reconstruction is the union
    /// of a frozen quorum's registries above the maximum watermark, so the
    /// merge of two is the reconstruction over a superset of a quorum —
    /// still complete for every registration that reached a quorum.
    pub fn merge(&mut self, page: PendingBootstrap) {
        for (journal, snapshot) in page.registries {
            self.registries.entry(journal).or_default().merge(snapshot);
        }
    }
}

/// The small, persisted-whole durable scalars of a matchmaker — the
/// registry's [`crate::HardState`]: the generation state, the successor
/// decree's acceptor record, and every journal's [`RegistryScalars`]. `#[non_exhaustive]` and built
/// through [`Default`] so a field can land without breaking every store.
///
/// The per-ballot registrations are deliberately **not** here: they are
/// persisted one record at a time and read back one record at a time through
/// [`RegistryStorage`](crate::RegistryStorage), exactly as the accepted log is split from
/// [`crate::HardState`]. See [`RegistryStorage`](crate::RegistryStorage) for why.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct MatchmakerHardState {
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
    pub decree: DecreeRecord,
    /// Bootstraps for proposed later generations this matchmaker is a member
    /// of, keyed by the proposed set, inactive until one is chosen.
    pub pending: Vec<PendingBootstrap>,
    /// Every journal's registry scalars (#190): its watermark and its
    /// effective configuration. A journal never registered here is absent
    /// (watermark zero, no effective configuration).
    pub registries: BTreeMap<JournalKey, RegistryScalars>,
}

impl MatchmakerHardState {
    /// `journal`'s GC watermark ([`Ballot::zero`] for a journal never
    /// registered here).
    #[must_use]
    pub fn gc_watermark(&self, journal: JournalKey) -> Ballot {
        self.registries
            .get(&journal)
            .map_or(Ballot::zero(), |r| r.gc_watermark)
    }

    /// `journal`'s effective configuration, if any.
    #[must_use]
    pub fn effective(&self, journal: JournalKey) -> Option<&(Ballot, AcceptorConfig)> {
        self.registries
            .get(&journal)
            .and_then(|r| r.effective.as_ref())
    }
}

/// The set `scalars` describes, resolved against the deployment's
/// `bootstrap` membership: generation 0's bootstrap set until a generation
/// is durably activated or frozen.
///
/// Shared, rather than re-derived: [`Matchmaker`] reads it from its own
/// scalars and the handover model checker reads it from a *disk* the
/// matchmaker is not booted from, and two copies of this rule would let a
/// checker agree with a resolution the core does not make.
pub(crate) fn resolved_set(
    scalars: &MatchmakerHardState,
    bootstrap: &[MatchmakerId],
) -> MatchmakerSet {
    if scalars.generation == MatchmakerGeneration(0) && scalars.members.is_empty() {
        MatchmakerSet::new(MatchmakerGeneration(0), bootstrap.to_vec())
    } else {
        MatchmakerSet::new(scalars.generation, scalars.members.clone())
    }
}

/// Where `scalars` stand, resolved the same way: a fresh store makes a
/// bootstrap member active for generation 0 and anyone else a spare.
///
/// `bootstrap` must be sorted (both callers keep it so).
pub(crate) fn resolved_phase(
    scalars: &MatchmakerHardState,
    id: MatchmakerId,
    bootstrap: &[MatchmakerId],
) -> MatchmakerPhase {
    match scalars.phase {
        MatchmakerPhase::Fresh => {
            if bootstrap.binary_search(&id).is_ok() {
                MatchmakerPhase::Active
            } else {
                MatchmakerPhase::Inactive
            }
        }
        phase => phase,
    }
}

/// Raise a held effective configuration to `(ballot, config)` only when
/// `ballot` is **strictly** higher than the one held (a tie keeps the held
/// value). Returns whether it moved.
///
/// The one monotone rule every holder of an effective configuration folds
/// through: the candidate's matchmaking phase, a matchmaker's registration,
/// the handover's reconstruction and a generation's activation.
pub(crate) fn raise_effective(
    held: &mut Option<(Ballot, AcceptorConfig)>,
    ballot: Ballot,
    config: &AcceptorConfig,
) -> bool {
    if held.as_ref().is_none_or(|(newest, _)| ballot > *newest) {
        *held = Some((ballot, config.clone()));
        true
    } else {
        false
    }
}

/// A matchmaker's static configuration: its identity and the deployment's
/// bootstrap matchmaker set (generation 0).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerConfig {
    /// This matchmaker's identity.
    pub id: MatchmakerId,
    /// The bootstrap set: the members of generation 0.
    pub bootstrap: Vec<MatchmakerId>,
}

/// One ledger record: the configuration registered under a ballot, and
/// what [kind](RegistrationKind) it was: an **operator's reconfiguration**
/// (a leader moving the cluster to a new acceptor set,
/// `ColocatedNode::reconfigure`) or a candidate restating the configuration it
/// believed in force.
///
/// **The effective configuration is a registration fact, not a Paxos-chosen
/// value.** A reconfiguration is in force once its record reached a
/// matchmaker quorum — before any Phase 1 or Phase 2 under the new set
/// completes — and stays in force until a higher-ballot flagged record
/// lands. The full contract, with its consequences (what `accepted: true`
/// promises, overlapping reconfigurations, why beliefs never count), is the
/// *effective configuration* section of the leader-side matchmaking module
/// (`node/matchmaking.rs`).
///
/// The kind is what makes the ledger answer "which configuration is in
/// force?" without treating every registration as a fact: an ordinary
/// campaign registers a *belief* (possibly stale, possibly abandoned), and a
/// ledger full of beliefs made "adopt the newest registration" flip-flop
/// between two candidates' beliefs forever. A reconfiguration registration is
/// an explicit request, and requests are monotone by ballot: the
/// highest-ballot one a matchmaker quorum holds is the **effective
/// configuration** — the one every ordinary campaign must register (see
/// `ColocatedNode::on_match_reply`). Once a reconfiguration's matchmaking has
/// completed at a matchmaker quorum, quorum intersection hands that record
/// to every later campaign's matchmaking, so no later ordinary election can
/// reinstate a superseded configuration; before that it may be lost like any
/// proposal that never reached a quorum.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Registration {
    /// The acceptor configuration registered.
    pub config: AcceptorConfig,
    /// What kind of registration this is.
    pub kind: RegistrationKind,
}

/// Why a configuration was registered — the ledger's distinction between a
/// *belief* and a *fact*, and the one thing that decides whether a record
/// can move the effective configuration.
///
/// It is an enum rather than a `bool` because both values are meaningful and
/// neither is the "off" state: a campaign is always one or the other, and
/// `reconfiguration: false` read at a call site says nothing about which
/// (review finding F7 of the core-roles report).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RegistrationKind {
    /// A candidate restating the configuration it *believes* is in force —
    /// possibly stale, possibly abandoned. Never a fact.
    #[default]
    Belief,
    /// An operator's explicit change, through
    /// [`ColocatedNode::reconfigure`](crate::ColocatedNode::reconfigure). Monotone by
    /// ballot, and the only kind the effective configuration is read from.
    Reconfiguration,
}

impl RegistrationKind {
    /// Whether this is an operator's explicit change.
    #[must_use]
    pub fn is_reconfiguration(self) -> bool {
        matches!(self, Self::Reconfiguration)
    }
}

impl Registration {
    /// A candidate's belief: the configuration it intends to run with.
    #[must_use]
    pub fn belief(config: AcceptorConfig) -> Self {
        Self {
            config,
            kind: RegistrationKind::Belief,
        }
    }

    /// A reconfiguration request: the configuration a leader moves to.
    #[must_use]
    pub fn reconfiguration(config: AcceptorConfig) -> Self {
        Self {
            config,
            kind: RegistrationKind::Reconfiguration,
        }
    }
}
