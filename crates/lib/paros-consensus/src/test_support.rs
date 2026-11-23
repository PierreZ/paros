//! The builders the unit tests share (#730 (paros-consensus rework)): a fresh
//! [`MemStorage`] for a node of a plain cluster, a snapshot of a live
//! node's disk.

use crate::contract::config::Config;
use crate::contract::mem_storage::MemStorage;
use crate::membership::QuorumSystem;
use crate::node::ColocatedNode;
use crate::types::NodeId;

impl MemStorage {
    /// A fresh store for node `id` of a plain Multi-Paxos cluster over
    /// `members`, under a majority quorum.
    pub(crate) fn for_node(id: u64, members: &[u64]) -> Self {
        Self::new(Config {
            peers: members.iter().copied().map(NodeId).collect(),
            quorum_system: QuorumSystem::Majority,
            nodes: Vec::new(),
            matchmakers: Vec::new(),
            matchmaker_pool: Vec::new(),
            proxy_count: 0,
            ..Config::new(NodeId(id), crate::journal::JournalIdentifier::UNSET)
        })
    }

    /// Snapshot a live node's durable state (scalars + accepted log + compaction
    /// floor + faulty entries) into a fresh storage, the way a real driver's
    /// persisted disk would look, for building the "restart from durable
    /// storage" path in tests. A slot is readable, faulty or absent, never
    /// two of them: the faulty entries travel, or a reboot would read a
    /// faulty slot as absent (the fatal mutation of CTRL §5.1.1).
    pub(crate) fn from_node(n: &ColocatedNode) -> Self {
        let faulty: Vec<_> = n
            .acceptor()
            .faulty()
            .iter()
            .map(|(s, b)| (*s, *b))
            .collect();
        assert!(
            faulty
                .iter()
                .all(|(slot, _)| n.acceptor().record(*slot).is_none()),
            "a faulty slot holds no readable record"
        );
        Self {
            hard_state: n.hard_state(),
            accepted: n.acceptor().records().clone(),
            config: n.config().clone(),
            first_slot: n.acceptor().first_slot(),
            faulty,
            sealed: n.replica().journal_base(),
            registration_fence: n.registration_fence(),
            retire_mark: n.retire_mark(),
        }
    }
}
