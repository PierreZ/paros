//! The parties of a cluster: each state machine with its disk.

use paros_consensus::contract::mem_storage::MemStorage;
use paros_consensus::contract::write::AcceptorWrite;
use std::collections::BTreeMap;

use paros_consensus::matchmaker::{
    GcAck, GcRequest, MatchReply, Matchmaker, MatchmakerConfig, MatchmakerHardState, MemRegistry,
    ReconfigureReply, ReconfigureRequest,
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
    /// Everything that changed the disk, in order: what a crash or a
    /// mutant rebuilds the disk from.
    history: Vec<DiskStep>,
    /// The steps of `history` that an fsync covers: a crash keeps them.
    synced: usize,
}

/// One change of a node's disk.
#[derive(Clone, Debug)]
enum DiskStep {
    /// The disk took a write.
    Write(WriteOp),
    /// The disk lost the value of the record at this slot.
    Rot(paros_consensus::Slot),
}

impl NodeSite {
    pub(super) fn new(id: NodeId, config: Config) -> Self {
        Self {
            id,
            disk: MemStorage::new(config.clone()),
            config,
            live: None,
            history: Vec::new(),
            synced: 0,
        }
    }

    /// The disk takes one write. It is volatile until the next
    /// [`sync`](Self::sync).
    pub(super) fn persist(&mut self, op: &WriteOp) {
        self.disk.apply(std::slice::from_ref(op));
        self.history.push(DiskStep::Write(op.clone()));
    }

    /// An fsync: every step so far survives a crash.
    pub(super) fn sync(&mut self) {
        self.synced = self.history.len();
    }

    /// How many writes no fsync covers yet.
    pub(super) fn unsynced(&self) -> usize {
        self.history.len() - self.synced
    }

    /// The disk loses the value of the record at `slot` and keeps its
    /// identity. A rot is damage, not a write: a crash never undoes it,
    /// and a rebuild rots the record again.
    pub(super) fn rot(&mut self, slot: paros_consensus::Slot) {
        let _ = self.disk.rot(slot);
        self.history.push(DiskStep::Rot(slot));
    }

    /// The node crashes: its memory is gone. The disk keeps every step an
    /// fsync covers and the first `keep` steps after them (`None`: all of
    /// them). The writes apply in order, so a crash keeps a prefix. A
    /// mutant may damage the disk more.
    pub(super) fn crash(&mut self, mutant: Option<Mutant>, keep: Option<usize>) {
        self.live = None;
        if let Some(keep) = keep {
            let tail = self.history.split_off(self.synced);
            let before = tail.len();
            let mut kept = 0;
            for step in tail {
                match step {
                    DiskStep::Rot(_) => self.history.push(step),
                    DiskStep::Write(_) if kept < keep => {
                        kept += 1;
                        self.history.push(step);
                    }
                    DiskStep::Write(_) => {}
                }
            }
            if self.history.len() - self.synced < before {
                self.rebuild();
            }
        }
        match mutant {
            Some(Mutant::LoseChosenIndex) => self.lose_chosen_index(),
            Some(Mutant::ForgetVotes) => self.forget_votes(),
            _ => {}
        }
    }

    /// [`Mutant::LoseChosenIndex`]: the disk loses its last chosen-index
    /// write, even one an fsync covers, and keeps every other write.
    fn lose_chosen_index(&mut self) {
        let Some(last) = self
            .history
            .iter()
            .rposition(|step| matches!(step, DiskStep::Write(WriteOp::SetChosenIndex(_))))
        else {
            return;
        };
        self.history.remove(last);
        if last < self.synced {
            self.synced -= 1;
        }
        self.rebuild();
    }

    /// [`Mutant::ForgetVotes`]: the disk keeps its promise and its chosen
    /// prefix, and loses every vote above that prefix.
    fn forget_votes(&mut self) {
        let chosen = self.disk.hard_state().chosen_index;
        let kept: Vec<DiskStep> = self
            .history
            .iter()
            .filter(|step| match step {
                DiskStep::Write(WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                    slot, ..
                })) => chosen.is_some_and(|c| *slot <= c),
                _ => true,
            })
            .cloned()
            .collect();
        self.history = kept;
        self.synced = self.history.len();
        self.rebuild();
    }

    /// The disk again, from its history.
    fn rebuild(&mut self) {
        let mut disk = MemStorage::new(self.config.clone());
        for step in &self.history {
            match step {
                DiskStep::Write(op) => disk.apply(std::slice::from_ref(op)),
                DiskStep::Rot(slot) => {
                    let _ = disk.rot(*slot);
                }
            }
        }
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

    /// Answer a handover's request: step, persist, then the replies.
    /// `None` when the matchmaker is down.
    pub(super) fn reconfigure(
        &mut self,
        request: ReconfigureRequest,
    ) -> Option<Vec<ReconfigureReply>> {
        self.live.as_mut()?.step_reconfigure(request);
        self.persist();
        let ready = self.live.as_mut()?.ready();
        let replies = ready.reconfigure_replies().to_vec();
        assert!(
            ready.replies().is_empty(),
            "a handover's request gets no matchmaking answer"
        );
        ready.advance();
        Some(replies)
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
