//! The builders the unit tests share (#730 (paros-core rework)): a fresh
//! [`MemStorage`] for a node of a plain cluster, a snapshot of a live
//! node's disk, and a rotted record.

use crate::contract::config::Config;
use crate::contract::mem_storage::MemStorage;
use crate::membership::QuorumSystem;
use crate::node::ColocatedNode;
use crate::types::{NodeId, Slot};

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
    /// floor) into a fresh storage, the way a real driver's persisted disk would
    /// look, for building the "restart from durable storage" path in tests.
    pub(crate) fn from_node(n: &ColocatedNode) -> Self {
        Self {
            hard_state: n.hard_state(),
            accepted: n.acceptor().records().clone(),
            config: n.config().clone(),
            first_slot: n.acceptor().first_slot(),
            faulty: Vec::new(),
            sealed: n.replica().journal_base(),
        }
    }

    /// Rot the accepted record at `slot`: the value is lost, the identity
    /// `(slot, ballot)` survives — the boot scan's recoverable classification.
    pub(crate) fn rot(&mut self, slot: Slot) {
        let (ballot, _) = self
            .accepted
            .remove(&slot)
            .expect("rot targets a persisted record");
        self.faulty.push((slot, ballot));
    }
}
