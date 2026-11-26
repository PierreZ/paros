//! **Feedback**: what each deployment reports about itself, as plain data
//! (#1564 (journal controller, version 1)).
//!
//! Each deployment has one method, `feedback(&self)`, beside its
//! `counters()`: [`crate::node::ColocatedNode::feedback`],
//! [`crate::proxy_leader::ProxyLeader::feedback`],
//! [`crate::replica_node::ReplicaNode::feedback`] and
//! [`crate::matchmaker::Matchmaker::feedback`]. A feedback is not a field of
//! a `Ready`: a `Ready` is the batch to persist before the send, and a
//! feedback is not durable and has no order with the messages. A driver
//! reads it at its own pace and gives it to whoever holds the view, for
//! example a journal controller.
//!
//! A feedback has two parts. The **metrics** are numbers that the role
//! counts. The **insights** are facts that the role concluded itself, as an
//! enum. Each role reports only what it does itself: the leader its
//! `CheckQuorum` window, a proxy leader its Phase 2, a matchmaker its
//! registry. Reading a feedback changes nothing: every field comes from a
//! read-only accessor of the deployment.

use crate::contract::write::RetireMark;
use crate::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet, ProxyId};
use crate::types::{Ballot, NodeId};

/// What a [`crate::node::ColocatedNode`] reports.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeFeedback {
    /// The node.
    pub id: NodeId,
    /// The ballot of the node: its own while it leads or campaigns, else
    /// the highest it follows.
    pub ballot: Ballot,
    /// The node it believes leads, if any.
    pub leader: Option<NodeId>,
    /// The configuration it believes in force.
    pub acceptors: AcceptorConfig,
    /// The ballot that configuration is bound to.
    pub since: Ballot,
    /// Whether that belief is heard (a probe or a leadership), not the
    /// bootstrap default.
    pub heard: bool,
    /// The node pool it may draw a configuration from.
    pub pool: Vec<NodeId>,
    /// Its durable retire mark.
    pub retire_mark: RetireMark,
    /// The GC floor it heard in a leader's beat, if any.
    pub gc_floor_heard: Option<Ballot>,
    /// What only a leader reports: `Some` exactly when the node leads.
    pub leading: Option<LeaderFeedback>,
    /// Metric: the ticks the node took (its own logical clock).
    pub tick: u64,
    /// Metric: the `CheckQuorum` windows that closed with a quorum.
    pub quorum_windows_held: u64,
    /// Metric: the leaderships that ended on a window with no quorum.
    pub quorum_lost_step_downs: u64,
    /// Metric: the matchmaking phases that timed out.
    pub matchmaking_timeouts: u64,
    /// Metric: the bytes of the retained log.
    pub retained_bytes: u64,
    /// The facts the node concluded.
    pub insights: Vec<NodeInsight>,
}

/// What a leader reports beside [`NodeFeedback`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaderFeedback {
    /// Whether the leadership is settled: no election recovery and no
    /// repair probe open.
    pub settled: bool,
    /// The configurations other than its own that are still in force: the
    /// other configurations of `H_b` until the GC floor is effective.
    pub prior: Vec<AcceptorConfig>,
    /// The floor this leadership made effective at a matchmaker quorum,
    /// and the acceptors it found retirable.
    pub gc_effective: Option<(Ballot, Vec<NodeId>)>,
    /// For each member that missed the last `CheckQuorum` window: the
    /// windows in a row it missed. A member with no entry acked the last
    /// window.
    pub missed: Vec<(NodeId, u64)>,
}

/// A fact that a [`crate::node::ColocatedNode`] concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NodeInsight {
    /// The last leadership of this node ended on a `CheckQuorum` window
    /// that closed without a Phase-2 quorum.
    QuorumWindowLost,
    /// The leader saw `node` miss its last `windows` windows in a row.
    MemberSilent {
        /// The member.
        node: NodeId,
        /// The windows in a row it missed.
        windows: u64,
    },
    /// The node is not a member of the configuration it believes in force.
    Outside,
    /// The node heard the floor `floor`, may retire at it on its local
    /// conditions, and the matchmakers have not checked its registrations
    /// yet (`check_retirement`).
    RetirementUnchecked {
        /// The floor it heard.
        floor: Ballot,
    },
    /// The node may retire at the floor `floor` (`may_retire`).
    MayRetire {
        /// The floor it heard.
        floor: Ballot,
    },
}

/// What a [`crate::proxy_leader::ProxyLeader`] reports.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProxyFeedback {
    /// The proxy.
    pub id: ProxyId,
    /// The highest ballot delegated to it: the leadership it works for.
    pub ballot: Ballot,
    /// The node pool it may address.
    pub pool: Vec<NodeId>,
    /// Metric: the rounds open now.
    pub open_rounds: u64,
    /// Metric: the rounds it decided and committed.
    pub decided: u64,
    /// Metric: the rounds it evicted after its retention budget.
    pub expired: u64,
    /// Metric: the `Nack`s it relayed to a leader.
    pub relayed_nacks: u64,
    /// The facts the proxy concluded from its open rounds.
    pub insights: Vec<ProxyInsight>,
}

/// A fact that a [`crate::proxy_leader::ProxyLeader`] concluded from the
/// rounds it holds open. A round counts only once the proxy sent its
/// `Accept` again at least one time: a fresh round waits for nothing yet.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ProxyInsight {
    /// The members of `column` (`None` on a majority) that accepted none
    /// of the resent rounds of that column.
    ColumnSilent {
        /// The column, `None` on a majority.
        column: Option<usize>,
        /// The silent members, in order.
        silent: Vec<NodeId>,
        /// The highest resend count of those rounds.
        resends: u64,
    },
    /// The members of `column` that are not silent are not a Phase-2
    /// quorum of the column: the column cannot decide without a silent
    /// member.
    ColumnBlocked {
        /// The column, `None` on a majority.
        column: Option<usize>,
    },
}

/// What a [`crate::matchmaker::Matchmaker`] reports about one journal.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerFeedback {
    /// The matchmaker.
    pub id: MatchmakerId,
    /// The matchmaker set it serves or last served.
    pub set: MatchmakerSet,
    /// Whether it serves matchmaking for its generation now.
    pub active: bool,
    /// The GC watermark of the journal.
    pub gc_watermark: Ballot,
    /// The effective configuration of the journal and its ballot.
    pub effective: Option<(Ballot, AcceptorConfig)>,
    /// The highest ballot registered for the journal.
    pub highest: Option<Ballot>,
}

/// What a [`crate::replica_node::ReplicaNode`] reports. No rule of the
/// controller of version 1 reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReplicaFeedback {
    /// The node it believes leads, if any.
    pub leader: Option<NodeId>,
    /// Whether it holds a read basis for a leaderless read.
    pub has_read_basis: bool,
    /// Metric: the commands it learned.
    pub learned: u64,
    /// Metric: the catch-up requests it sent.
    pub catch_up_requests: u64,
}
