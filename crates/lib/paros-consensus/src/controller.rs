//! The **journal controller** of version 1 (#1564 (journal controller,
//! version 1), `docs/architecture/04-8-journal-controller.md` §4.8): a
//! pure function that reads the feedback of the deployments of one journal
//! and decides at most one order.
//!
//! - [`View`] holds the last feedback of each node, proxy leader and
//!   matchmaker of the journal, and its age in ticks of the holder of the
//!   view ([`View::tick`]). No clock crosses two processes.
//! - [`evaluate`] reads a view, the spares that the caller gives and a
//!   [`Policy`], and returns a [`Decision`]: [`Decision::Steady`], an
//!   [`Order`], or a [`Decision::Hold`] with its [`HoldReason`]. The first
//!   rule of [`rules`] that applies gives the decision.
//!
//! The controller adds nothing to the protocol: no term, no fence, no
//! message. Each order is a call that a driver already makes:
//! [`Order::Retire`] is `ColocatedNode::check_retirement` and
//! `may_retire`, and [`Order::Reconfigure`] is `ColocatedNode::reconfigure`.
//! Two controllers at the same time are safe; they only waste work
//! (`specs/tla/retire-fence/`, the `two_controllers` checks). The
//! controller sends nothing and persists nothing: a controller that
//! restarts has an empty view, and rule 1 holds until feedback arrives.
//!
//! It uses only the foundations: the feedback types of
//! [`crate::contract::feedback`] and the quorum predicates of
//! [`crate::membership`].

pub mod rules;
pub mod view;

pub use self::rules::evaluate;
pub use self::view::View;

use crate::membership::{AcceptorConfig, FailureDomain};
use crate::types::{Ballot, NodeId};

/// A node that the caller offers to replace a silent member. On a zoned
/// configuration ([`AcceptorConfig::is_zoned`]) a spare replaces only a
/// member of its own domain, so the new configuration keeps the count of
/// each domain (§4.8.2). On a configuration with no zones the domain is not
/// read.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Spare {
    /// The node.
    pub node: NodeId,
    /// Its failure domain, if the caller knows it.
    pub domain: Option<FailureDomain>,
}

/// The thresholds of the rules. Both are counts of the controller's
/// inputs, never of time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Policy {
    /// A feedback is **fresh** while its age is at most this many ticks of
    /// the holder of the view.
    pub max_age: u64,
    /// A member is **silent for the leader** once it missed at least this
    /// many `CheckQuorum` windows in a row. At least one.
    pub silent_windows: u64,
}

/// What [`evaluate`] decides.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Decision {
    /// Nothing to do: the journal is whole.
    Steady,
    /// Carry this order.
    Order(Order),
    /// Do nothing, for this reason.
    Hold(HoldReason),
}

/// An order. The driver of the named node carries it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Order {
    /// The leader `leader`, at `ballot` and with its configuration bound to
    /// `since`, reconfigures to `config` (`ColocatedNode::reconfigure`). The
    /// driver first compares the three with the node: an order for another
    /// leadership is dropped.
    Reconfigure {
        /// The leader that the view saw.
        leader: NodeId,
        /// Its ballot.
        ballot: Ballot,
        /// The ballot its configuration is bound to.
        since: Ballot,
        /// The new configuration.
        config: AcceptorConfig,
    },
    /// The node `node`, outside each configuration in force, retires at
    /// the effective floor `watermark`. The driver calls
    /// `ColocatedNode::check_retirement` on it. Once the node reports its
    /// retire mark checked, the rules give this order no more: the node's
    /// own `may_retire` then decides, at the node.
    Retire {
        /// The node.
        node: NodeId,
        /// The effective GC floor.
        watermark: Ballot,
    },
}

/// Why [`evaluate`] holds.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum HoldReason {
    /// Rule 1: no fresh feedback of a leader.
    NoLeader,
    /// Rule 2: a fresh node or proxy reports a ballot above the leader's.
    LeadershipMoves,
    /// Rule 3: the fresh, active matchmakers are not a majority of their
    /// set, or one reports a configuration in force above the leader's. A
    /// journal of plain Multi-Paxos has no matchmaker, so it always holds
    /// here: such a journal refuses every reconfiguration, and its
    /// controller has no order to give.
    Matchmakers,
    /// Rule 4: the fresh members of the configuration are not a Phase-1
    /// quorum of it.
    Phase1Lost,
    /// A member is silent for the leader and sent no fresh feedback, and
    /// this version of the controller does not replace it.
    MemberSilent {
        /// The silent member.
        node: NodeId,
    },
}
