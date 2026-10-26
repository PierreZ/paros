//! The node's **observability counters**: the private [`Counters`], the
//! public snapshot [`NodeCounters`] and the getters behind
//! [`ColocatedNode::counters`].

use super::{ColocatedNode, HandoffCounters, NodeRole};

/// The node's **observability counters**: monotone per incarnation, read by
/// the driver's audit report and by the examples, and never a decision —
/// deleting every one of them leaves the state machine unchanged. Each
/// names a path the simulation proves reached rather than merely present.
/// The handoff's own live in [`HandoffCounters`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Counters {
    /// `CheckQuorum` step-downs: full election-timeout windows without an
    /// ack quorum.
    pub(super) quorum_lost_step_downs: u64,
    /// Campaigns this node declined to open because it is not a member of
    /// the configuration it would register.
    pub(super) non_member_campaigns_skipped: u64,
    /// Leaderships resigned once this node's own reconfiguration removed it
    /// from the acceptor set.
    pub(super) non_member_step_downs: u64,
    /// Election timeouts that found a matchmaking phase still open and
    /// re-sent its requests instead of abandoning the campaign (see
    /// [`ColocatedNode::tick`]).
    pub(super) matchmaking_timeouts: u64,
    /// Recovery-timeout step-downs: a leader resigning because it could not
    /// finish repairing its blocked slots.
    pub(super) repair_step_downs: u64,
    /// Blocked slots resolved as Case 1 (re-proposed from a straggler's
    /// `have`) after the election closed.
    pub(super) repair_case1: u64,
    /// Blocked slots resolved as Case 2 (a full Q1 of `none` assembled from
    /// stragglers; decided `Noop`).
    pub(super) repair_case2: u64,
    /// Undecided holes filled with a [`crate::journal::Control::Noop`] when this node won its
    /// *current* leadership (0 until it wins one, re-set at each election).
    pub(super) election_gap_fills: u64,
    /// Slots a settled leader filled with a [`crate::journal::Control::Noop`] up to a vote
    /// watermark a pre-read reported past its frontier (monotone).
    pub(super) watermark_fills: u64,
    /// Quorum reads not opened for want of a read basis (#260).
    pub(super) quorum_reads_without_basis: u64,
    /// `PreRead`s left unanswered by a node whose belief was still the
    /// bootstrap default (#260).
    pub(super) pre_reads_refused_unheard: u64,
}

/// The repair counters ([`NodeCounters::repair`]): monotone per
/// incarnation, observability only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepairCounters {
    /// Faulty records repaired in place.
    pub repaired: u64,
    /// Blocked slots resolved as Case 1 (re-proposed from a straggler's
    /// `have`) after the election closed.
    pub case1: u64,
    /// Blocked slots resolved as Case 2 (a full Q1 of `none` assembled from
    /// stragglers; decided `Noop`).
    pub case2: u64,
    /// Recovery-timeout step-downs: a leader resigning because it could not
    /// finish repairing its blocked slots.
    pub step_downs: u64,
}

/// The campaign-membership counters ([`NodeCounters::membership`],
/// #122): monotone per incarnation, observability only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MembershipCounters {
    /// Campaigns this node declined to open because it is not a member of
    /// the configuration it would register.
    pub campaigns_skipped: u64,
    /// Leaderships resigned once this node's own reconfiguration removed it
    /// from the acceptor set.
    pub step_downs: u64,
    /// Quorum reads not opened for want of a read basis (#260): no won
    /// leadership heard this incarnation, or the node's belief bound above
    /// its basis.
    pub reads_without_basis: u64,
    /// `PreRead`s left unanswered while this node's belief was still the
    /// bootstrap default (#260).
    pub pre_reads_refused_unheard: u64,
}

/// Every observability counter of a node, in one read
/// ([`ColocatedNode::counters`]): monotone per incarnation unless a field
/// says otherwise, read by the driver's audit report and by the examples,
/// and never a decision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeCounters {
    /// The repair counters (Stage 8).
    pub repair: RepairCounters,
    /// The campaign-membership counters (#122, #260).
    pub membership: MembershipCounters,
    /// The cooperative-handoff counters.
    pub handoff: HandoffCounters,
    /// Undecided holes filled with a [`crate::journal::Control::Noop`] when this node won
    /// its *current* leadership: 0 on a node that has never led, and re-set
    /// at each election it wins.
    pub election_gap_fills: u64,
    /// Slots this node, as a settled leader, filled with a
    /// [`crate::journal::Control::Noop`] because a pre-read reported a vote watermark at or
    /// past its allocator frontier.
    pub watermark_fills: u64,
    /// `CheckQuorum` step-downs (#95): full election-timeout windows
    /// without an ack quorum.
    pub quorum_lost_step_downs: u64,
    /// Election timeouts that re-sent an open matchmaking phase's requests
    /// instead of abandoning the campaign (#120).
    pub matchmaking_timeouts: u64,
    /// The blocked slots the leader's open repair probe still holds (0 when
    /// no probe is open). A gauge, not monotone.
    pub blocked_repairs: usize,
}

impl ColocatedNode {
    /// Monotone campaign-membership counters this incarnation, for the
    /// driver's audit report.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub(super) fn membership_counters(&self) -> MembershipCounters {
        let counters = MembershipCounters {
            campaigns_skipped: self.counters.non_member_campaigns_skipped,
            step_downs: self.counters.non_member_step_downs,
            reads_without_basis: self.counters.quorum_reads_without_basis,
            pre_reads_refused_unheard: self.counters.pre_reads_refused_unheard,
        };
        // Only a matchmaker deployment moves its configuration, so only one
        // ever lacks a read basis or refuses a pre-read for an unheard belief.
        if counters.reads_without_basis > 0 || counters.pre_reads_refused_unheard > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment reads without a basis"
            );
        }
        // A node is outside its configuration only on a deployment that
        // reconfigures: plain Multi-Paxos skips and resigns nothing.
        if counters.campaigns_skipped > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment skips a campaign"
            );
        }
        if counters.step_downs > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment resigns a non-member"
            );
        }
        counters
    }

    /// Election timeouts this incarnation that re-sent an open matchmaking
    /// phase's requests instead of abandoning the campaign. Observability
    /// only (see [`ColocatedNode::tick`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub(super) fn matchmaking_timeouts(&self) -> u64 {
        if self.counters.matchmaking_timeouts > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment matchmakes"
            );
        }
        self.counters.matchmaking_timeouts
    }

    /// How many undecided holes this node filled with a [`crate::journal::Control::Noop`] when it
    /// won its current leadership — 0 on a node that has never led, and re-set at
    /// each election it wins. A read-only observability counter: the driver reads
    /// it on the transition to Leader so a simulation can prove the gap-fill path
    /// is genuinely reached.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub(super) fn election_gap_fills(&self) -> u64 {
        // Every fill took a slot below the frontier.
        assert!(
            self.counters.election_gap_fills <= self.proposer.next_slot().0,
            "gap fills never outnumber the allocated slots"
        );
        self.counters.election_gap_fills
    }

    /// Monotone count of slots this node, as a settled leader, filled with a
    /// [`crate::journal::Control::Noop`] because a pre-read reported a vote watermark at or
    /// past its allocator frontier (`node/quorum_reads.rs`). The driver
    /// reports the delta so a simulation can prove the path is reached.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub(super) fn watermark_fills(&self) -> u64 {
        assert!(
            self.counters.watermark_fills <= self.proposer.next_slot().0,
            "watermark fills never outnumber the allocated slots"
        );
        self.counters.watermark_fills
    }

    /// Monotone count of `CheckQuorum` step-downs (#95) this incarnation: the
    /// times this node, as Leader, spent a full election-timeout window without
    /// hearing an ack quorum and demoted itself. The driver reads the delta per
    /// batch and reports it through its audit port.
    #[must_use]
    pub(super) fn quorum_lost_step_downs(&self) -> u64 {
        self.counters.quorum_lost_step_downs
    }

    /// Monotone cooperative-handoff counters this incarnation (see
    /// [`HandoffCounters`]). The driver reports the delta through its audit
    /// port, so a simulation can prove each handoff and refusal path is
    /// genuinely reached.
    #[must_use]
    pub(super) fn handoff_counters(&self) -> HandoffCounters {
        self.handoff
    }

    /// How many blocked slots the leader's open repair probe still holds (0
    /// when no probe is open): faulty slots the promise quorum resolved neither
    /// as Case 1 (`have`) nor Case 2 (a full Q1 of `none`), still waiting on
    /// stragglers.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub(super) fn blocked_repairs(&self) -> usize {
        let blocked = self.proposer.probe().map_or(0, |p| p.blocked().len());
        if blocked > 0 {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds a repair probe"
            );
        }
        blocked
    }

    /// Every observability counter this incarnation, in one read, for the
    /// driver's audit report.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn counters(&self) -> NodeCounters {
        NodeCounters {
            repair: self.repair_counters(),
            membership: self.membership_counters(),
            handoff: self.handoff_counters(),
            election_gap_fills: self.election_gap_fills(),
            watermark_fills: self.watermark_fills(),
            quorum_lost_step_downs: self.quorum_lost_step_downs(),
            matchmaking_timeouts: self.matchmaking_timeouts(),
            blocked_repairs: self.blocked_repairs(),
        }
    }

    /// Monotone repair counters this incarnation, for the driver's audit
    /// report.
    #[must_use]
    pub(super) fn repair_counters(&self) -> RepairCounters {
        RepairCounters {
            repaired: self.acceptor.faulty_repaired(),
            case1: self.counters.repair_case1,
            case2: self.counters.repair_case2,
            step_downs: self.counters.repair_step_downs,
        }
    }
}
