//! The parties of a cell: each state machine with its disk.

use paros_consensus::contract::mem_storage::MemStorage;
use paros_consensus::contract::write::AcceptorWrite;
use std::collections::BTreeMap;

use paros_consensus::matchmaker::{
    GcAck, GcRequest, MatchReply, Matchmaker, MatchmakerConfig, MatchmakerHardState, MemRegistry,
};
use paros_consensus::membership::{AcceptorConfig, MatchmakerId, ProxyId};
use paros_consensus::{ColocatedNode, Config, Limits, NodeId, ProxyLeader, ReplicaNode, WriteOp};

use super::Mutant;

/// An acceptor: its disk, and the node when it is up.
pub(super) struct NodeSite {
    pub(super) id: NodeId,
    pub(super) config: Config,
    pub(super) disk: MemStorage,
    pub(super) live: Option<ColocatedNode>,
    /// Every write the disk took, in order: what a mutant rebuilds a
    /// damaged disk from.
    history: Vec<WriteOp>,
}

impl NodeSite {
    pub(super) fn new(id: NodeId, config: Config) -> Self {
        Self {
            id,
            disk: MemStorage::new(config.clone()),
            config,
            live: None,
            history: Vec::new(),
        }
    }

    /// The disk takes one write.
    pub(super) fn persist(&mut self, op: &WriteOp) {
        self.disk.apply(std::slice::from_ref(op));
        self.history.push(op.clone());
    }

    /// The node crashes: its memory is gone, its disk stays, unless a
    /// mutant damages the disk.
    pub(super) fn crash(&mut self, mutant: Option<Mutant>) {
        self.live = None;
        match mutant {
            Some(Mutant::LoseChosenIndex) => self.lose_chosen_index(),
            Some(Mutant::ForgetVotes) => self.forget_votes(),
            _ => {}
        }
    }

    /// [`Mutant::LoseChosenIndex`]: the disk loses its last chosen-index
    /// write, and keeps every other write.
    fn lose_chosen_index(&mut self) {
        let Some(last) = self
            .history
            .iter()
            .rposition(|op| matches!(op, WriteOp::SetChosenIndex(_)))
        else {
            return;
        };
        self.history.remove(last);
        self.rebuild();
    }

    /// [`Mutant::ForgetVotes`]: the disk keeps its promise and its chosen
    /// prefix, and loses every vote above that prefix.
    fn forget_votes(&mut self) {
        let chosen = self.disk.hard_state().chosen_index;
        let kept: Vec<WriteOp> = self
            .history
            .iter()
            .filter(|op| match op {
                WriteOp::Acceptor(AcceptorWrite::AppendAccepted { slot, .. }) => {
                    chosen.is_some_and(|c| *slot <= c)
                }
                _ => true,
            })
            .cloned()
            .collect();
        self.history = kept;
        self.rebuild();
    }

    /// The disk again, from its history.
    fn rebuild(&mut self) {
        let mut disk = MemStorage::new(self.config.clone());
        disk.apply(&self.history);
        self.disk = disk;
    }
}

/// A proxy leader: no disk; it restarts empty.
pub(super) struct ProxySite {
    pub(super) id: ProxyId,
    pub(super) live: Option<ProxyLeader>,
    /// It stays down for the quiet tail.
    pub(super) dead_for_good: bool,
}

impl ProxySite {
    pub(super) fn new(id: ProxyId, config: AcceptorConfig) -> Self {
        Self {
            id,
            live: Some(ProxyLeader::new(id, config)),
            dead_for_good: false,
        }
    }
}

/// A replica: its disk, and the replica when it is up.
pub(super) struct ReplicaSite {
    pub(super) id: NodeId,
    pub(super) disk: MemStorage,
    pub(super) live: Option<ReplicaNode>,
}

impl ReplicaSite {
    pub(super) fn new(id: NodeId, config: Config, limits: Limits) -> Self {
        let disk = MemStorage::new(config);
        let live = Some(ReplicaNode::new(&disk, limits));
        Self { id, disk, live }
    }
}

/// A matchmaker: its registry on disk, and the matchmaker when it is up.
pub(super) struct MatchmakerSite {
    pub(super) id: MatchmakerId,
    config: MatchmakerConfig,
    limits: Limits,
    disk: MemRegistry,
    pub(super) live: Option<Matchmaker>,
}

impl MatchmakerSite {
    pub(super) fn new(id: MatchmakerId, bootstrap: Vec<MatchmakerId>, limits: Limits) -> Self {
        let mut site = Self {
            id,
            config: MatchmakerConfig { id, bootstrap },
            limits,
            disk: MemRegistry::new(MatchmakerHardState::default(), BTreeMap::new()),
            live: None,
        };
        site.boot();
        site
    }

    /// Boot from the registry on disk, when down.
    pub(super) fn boot(&mut self) {
        if self.live.is_none() {
            self.live = Some(Matchmaker::new(&self.config, &self.disk, self.limits));
        }
    }

    /// The registry takes the writes of the batch.
    pub(super) fn persist(&mut self) {
        let Some(matchmaker) = self.live.as_mut() else {
            return;
        };
        let ready = matchmaker.ready();
        for op in ready.writes() {
            self.disk.apply(op);
        }
        drop(ready);
    }

    /// The replies of the batch, once its writes are on disk.
    pub(super) fn take_replies(&mut self) -> Vec<MatchReply> {
        let Some(matchmaker) = self.live.as_mut() else {
            return Vec::new();
        };
        let ready = matchmaker.ready();
        let replies = ready.replies().to_vec();
        ready.advance();
        replies
    }

    /// Raise the GC watermark, persist it, and build the ack.
    pub(super) fn gc(&mut self, request: &GcRequest) -> Option<GcAck> {
        let outcome = self.live.as_mut()?.advance_gc_watermark(
            request.journal,
            request.generation,
            request.watermark,
        );
        self.persist();
        let matchmaker = self.live.as_mut()?;
        let ack = matchmaker.gc_ack(request, outcome);
        matchmaker.ready().advance();
        Some(ack)
    }
}
