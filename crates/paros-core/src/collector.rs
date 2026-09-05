//! The **collector**: the leader-side garbage-collection tally of Matchmaker
//! Paxos §3.4–§3.5 (#123), as a role beside the acceptor, the proposer and
//! the replica.
//!
//! One leadership, one collector. It holds the two tallies the GC protocol
//! counts — which configured acceptors report a chosen index at or past the
//! election fence (Region 1 of the forgettability condition), and which
//! matchmakers have acked the watermark — plus the floor it made effective
//! and the acceptors that floor retires.
//!
//! What it deliberately does **not** know: whether this node is a leader,
//! whether a recovery or a repair probe is still open, when to re-send a
//! request, or how to build one. Those are couplings between roles and
//! belong to the wiring (`node/gc.rs`), exactly as `node/election.rs` owns
//! the wiring around [`crate::proposer::Proposer`]. The derivation of the
//! condition itself — why *this* is what licenses forgetting a
//! configuration — is written down in that module's doc.

use std::collections::{BTreeMap, BTreeSet};

use crate::matchmaker::GcAck;
use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::types::{Ballot, NodeId, Slot};

/// What one GC ack did, returned by [`crate::ColocatedNode::on_gc_ack`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcStep {
    /// Not for the open campaign: nothing changed.
    Ignored,
    /// One more matchmaker holds the floor; `remaining` more before the
    /// quorum.
    Acked {
        /// Acks still needed.
        remaining: usize,
    },
    /// A matchmaker quorum holds the floor: `watermark` is effective and the
    /// acceptors in `retired` are no longer needed by any future leader.
    Effective {
        /// The floor in force.
        watermark: Ballot,
        /// Members of the prior configurations outside the current one.
        retired: Vec<NodeId>,
    },
}

/// The GC campaign of one leadership (volatile, like every leader tally).
#[derive(Clone, Debug)]
pub struct Collector {
    /// The matchmaker generation the requests address; the ack tally resets
    /// when the leader learns a newer generation.
    generation: MatchmakerGeneration,
    /// The election fence: every slot at or below it was chosen below this
    /// ballot or re-proposed at it. `None` when nothing was ever proposed.
    fence: Option<Slot>,
    /// The members of every configuration in `H_b`.
    prior_members: BTreeSet<NodeId>,
    /// Per configured peer: the chosen index it last acked at this ballot.
    peer_chosen: BTreeMap<NodeId, Slot>,
    /// Whether the requests have been queued (the preconditions held).
    requested: bool,
    /// Matchmakers that acked a watermark at or above this ballot.
    acked_by: BTreeSet<MatchmakerId>,
    /// The floor in force at a matchmaker quorum, and the acceptors it
    /// retires.
    effective: Option<(Ballot, Vec<NodeId>)>,
}

impl Collector {
    /// The campaign of a freshly won leadership at `generation`, judging
    /// Region 1 by `fence` and retiring out of `prior` (`H_b`).
    #[must_use]
    pub fn new(
        generation: MatchmakerGeneration,
        fence: Option<Slot>,
        prior: &[AcceptorConfig],
    ) -> Self {
        Self {
            generation,
            fence,
            prior_members: prior
                .iter()
                .flat_map(|c| c.members().iter().copied())
                .collect(),
            peer_chosen: BTreeMap::new(),
            requested: false,
            acked_by: BTreeSet::new(),
            effective: None,
        }
    }

    /// The election fence Region 1 is judged by.
    #[must_use]
    pub fn fence(&self) -> Option<Slot> {
        self.fence
    }

    /// Whether the requests have been queued.
    #[must_use]
    pub fn requested(&self) -> bool {
        self.requested
    }

    /// The floor made effective at a matchmaker quorum, and the acceptors it
    /// retired — `None` until the quorum holds.
    #[must_use]
    pub fn effective(&self) -> Option<(Ballot, &[NodeId])> {
        self.effective
            .as_ref()
            .map(|(watermark, retired)| (*watermark, retired.as_slice()))
    }

    /// Whether `matchmaker` has already acked the floor — what a re-send
    /// skips.
    #[must_use]
    pub fn acked(&self, matchmaker: MatchmakerId) -> bool {
        self.acked_by.contains(&matchmaker)
    }

    /// Record that the requests for this generation are out.
    pub fn request(&mut self) {
        self.requested = true;
    }

    /// Start the ack tally over at `generation`: acks from a replaced
    /// generation say nothing about the new one's quorum. A no-op once the
    /// floor is effective, or at the generation already addressed.
    pub fn reset_for_generation(&mut self, generation: MatchmakerGeneration) {
        if self.effective.is_none() && self.generation != generation {
            self.requested = false;
            self.acked_by.clear();
            self.generation = generation;
        }
    }

    /// A configured peer reported its chosen index at this ballot (the
    /// monotone half of Region 1's tally).
    pub fn note_chosen(&mut self, from: NodeId, chosen: Option<Slot>) {
        if let Some(chosen) = chosen {
            let entry = self.peer_chosen.entry(from).or_insert(chosen);
            *entry = (*entry).max(chosen);
        }
    }

    /// Whether Region 1 is held: a Phase-2 quorum of `config` reports a
    /// chosen index at or past the fence, so every future Phase-1 quorum of
    /// `config` intersects a node that holds the prefix as its authoritative
    /// accepted record.
    ///
    /// `own` is this node's own `(id, chosen index)` when it is a member of
    /// `config` — the local half of the tally, which arrives by no ack.
    ///
    /// # Panics
    ///
    /// If `config` is not well formed (the quorum tally's own precondition).
    #[must_use]
    pub fn covered(&self, config: &AcceptorConfig, own: Option<(NodeId, Option<Slot>)>) -> bool {
        let reached = |chosen: Option<Slot>| match self.fence {
            None => true,
            Some(fence) => chosen.is_some_and(|c| c >= fence),
        };
        let me = own.map(|(id, _)| id);
        let mut holders: BTreeSet<NodeId> = BTreeSet::new();
        if let Some((id, chosen)) = own
            && reached(chosen)
        {
            holders.insert(id);
        }
        holders.extend(
            config
                .members()
                .iter()
                .filter(|m| Some(**m) != me)
                .filter(|m| reached(self.peer_chosen.get(*m).copied()))
                .copied(),
        );
        config.has_phase2_quorum(&holders)
    }

    /// Fold one matchmaker's GC ack at `ballot` over `config` (the
    /// configuration in force, which decides what the floor retires), against
    /// `matchmakers` — the set whose generation this campaign addressed and
    /// whose quorum makes the floor effective. An ack for another generation,
    /// a lower floor, or one already counted is ignored whole — wire input,
    /// never asserted.
    ///
    /// # Panics
    ///
    /// If `matchmakers` is not well formed (the quorum tally's own
    /// precondition), or if the quorum that makes the floor effective would
    /// retire an acceptor the configuration in force still names.
    pub fn fold_ack(
        &mut self,
        ack: &GcAck,
        matchmakers: &MatchmakerSet,
        ballot: Ballot,
        config: &AcceptorConfig,
    ) -> GcStep {
        let generation = matchmakers.generation;
        if !self.requested
            || self.effective.is_some()
            || ack.generation != generation
            || self.generation != generation
        {
            return GcStep::Ignored;
        }
        // An applied ack, or a refusal whose own floor already sits at or
        // above ours, both mean this matchmaker holds a floor >= ours — and
        // that is all the tally needs. `applied` is deliberately not
        // consulted: a refusal comes from a matchmaker that has moved to a
        // later generation, and it still proves a durable floor at or above
        // `ballot`. The two quorums that matter are both majorities of the
        // generation addressed, so they intersect whatever the refuser now
        // serves.
        if ack.watermark < ballot {
            return GcStep::Ignored;
        }
        if !self.acked_by.insert(ack.matchmaker) {
            return GcStep::Ignored;
        }
        if !matchmakers.has_quorum(&self.acked_by) {
            return GcStep::Acked {
                remaining: matchmakers
                    .quorum_size()
                    .saturating_sub(self.acked_by.len()),
            };
        }
        // Quorum intersection makes the floor effective for every future
        // campaign; the prior configurations' members the current one does
        // not need are retirable.
        let retired: Vec<NodeId> = self
            .prior_members
            .iter()
            .copied()
            .filter(|n| !config.contains(*n))
            .collect();
        self.effective = Some((ballot, retired.clone()));
        GcStep::Effective {
            watermark: ballot,
            retired,
        }
    }
}
