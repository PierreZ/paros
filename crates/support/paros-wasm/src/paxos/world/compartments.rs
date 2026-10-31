//! The compartments: proxy leaders and the replica tier (Compartmentalized
//! Paxos §3.1 and §3.3).
//!
//! This is `crates/lib/paros-core/examples/proxy_leader.rs` and
//! `replica_tier.rs` folded into the log world. A proxy leader and a replica
//! are **two more processes** beside the acceptor pool, each with its own
//! identity space, and each driven through the same `step` → `ready` →
//! `advance` shape as a node. Nothing here decides anything: the proxy is the
//! core's [`ProxyLeader`] and the replica is the core's [`ReplicaNode`].
//!
//! The deployment map is the world's, exactly as it is the driver's in
//! `paros`: an [`Audience::Proxy`](paros_core::contract::Audience::Proxy)
//! resolves to a proxy, a node id that names a replica resolves to that
//! replica, and the learner fan-out (`Audience::Learners`) reaches every
//! replica as well as the pool.

use paros_core::journal::Command;
use paros_core::membership::{AcceptorConfig, ProxyId};
use paros_core::{Limits, Message, NodeId, ProxyLeader, ReplicaNode, Slot};

use crate::paxos::action::{ActionError, ActionErrorCode};
use crate::paxos::narration::{NarrationKind, many, who};
use crate::paxos::view::{GapView, ProxyRoundView, ProxyView, ReplicaView, SlotView, show_ballot};
use crate::paxos::world::{ClientId, Disk, Envelope, InFlight, Party, World};

/// The driver's take-back budget: how many re-delegations a proxy may swallow
/// before the leader runs the round colocated. The same number as the
/// `paros-core` lesson this module replaces.
pub const TAKE_BACK_AFTER: u64 = 2;

/// One round a leader took back from a proxy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TakeBack {
    /// The leader that took it back.
    pub leader: NodeId,
    /// The slot.
    pub slot: Slot,
    /// The proxy it was delegated to.
    pub proxy: ProxyId,
}

/// One proxy leader's process: the core role, or nothing while it is down.
///
/// A proxy keeps nothing durable, so a restart is a fresh role over the
/// deployment's bootstrap configuration.
#[derive(Clone, Debug)]
pub struct ProxyProcess {
    id: ProxyId,
    bootstrap: AcceptorConfig,
    role: Option<ProxyLeader>,
}

impl ProxyProcess {
    /// A running proxy `id` over `bootstrap`.
    #[must_use]
    pub fn new(id: ProxyId, bootstrap: AcceptorConfig) -> Self {
        Self {
            id,
            role: Some(ProxyLeader::new(id, bootstrap.clone())),
            bootstrap,
        }
    }

    /// Its id.
    #[must_use]
    pub fn id(&self) -> ProxyId {
        self.id
    }

    /// The core role, while it runs.
    #[must_use]
    pub fn role(&self) -> Option<&ProxyLeader> {
        self.role.as_ref()
    }

    fn view(&self) -> ProxyView {
        let Some(role) = self.role.as_ref() else {
            return ProxyView {
                id: self.id.0,
                alive: false,
                ballot: None,
                open_rounds: Vec::new(),
                decided: 0,
                refanned: 0,
            };
        };
        let counters = role.counters();
        ProxyView {
            id: self.id.0,
            alive: true,
            ballot: Some(show_ballot(role.ballot())),
            open_rounds: role
                .rounds()
                .by_slot()
                .keys()
                .map(|slot| ProxyRoundView {
                    slot: slot.0,
                    leader: role.delegator(*slot).map_or(0, |leader| leader.0),
                })
                .collect(),
            decided: counters.decided,
            refanned: counters.refanned,
        }
    }
}

/// One replica's process: the core role, its disk, and what it learned.
#[derive(Clone, Debug)]
pub struct ReplicaProcess {
    id: NodeId,
    disk: Disk,
    role: ReplicaNode,
    learned: Vec<(Slot, Command)>,
    caught_up: u64,
}

impl ReplicaProcess {
    /// Its id: a node id outside the acceptor pool.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The core role.
    #[must_use]
    pub fn role(&self) -> &ReplicaNode {
        &self.role
    }

    /// What its walk moved over, in order: the chosen log it learned.
    #[must_use]
    pub fn learned(&self) -> &[(Slot, Command)] {
        &self.learned
    }

    /// How many slots it learned from a catch-up answer.
    #[must_use]
    pub fn caught_up(&self) -> u64 {
        self.caught_up
    }

    fn view(&self) -> ReplicaView {
        let replica = self.role.replica();
        ReplicaView {
            id: self.id.0,
            chosen_index: replica.chosen_index().map(|slot| slot.0),
            chosen_gap: replica.chosen_gap().map(|(hole, highest)| GapView {
                hole: hole.0,
                highest: highest.0,
            }),
            learned: self
                .learned
                .iter()
                .map(|(slot, command)| SlotView::applied_entry(*slot, command))
                .collect(),
            caught_up: self.caught_up,
        }
    }
}

impl World {
    /// Deploy `count` proxy leaders beside the pool, over the configuration
    /// the pool's disks name. The disks must say `proxy_count == count`: the
    /// leader picks a proxy as `slot % proxy_count`.
    ///
    /// # Panics
    ///
    /// If the disks name a different proxy count.
    #[must_use]
    pub fn with_proxies(mut self, count: usize) -> Self {
        let config = self.disks[0].config().clone();
        assert!(
            config.proxy_count == count,
            "the disks name the proxies the world deploys"
        );
        let bootstrap = AcceptorConfig::new(config.peers.clone(), config.quorum_system);
        self.proxies = (0..count)
            .map(|index| ProxyProcess::new(ProxyId(index as u64), bootstrap.clone()))
            .collect();
        self
    }

    /// Deploy one replica per id beside the pool. Each replica runs the
    /// pool's configuration under its own id, which is outside the pool.
    ///
    /// # Panics
    ///
    /// If an id is a member of the pool, or the disks name a different
    /// replica count.
    #[must_use]
    pub fn with_replicas(mut self, ids: &[u64]) -> Self {
        let base = self.disks[0].config().clone();
        assert!(
            base.replica_count == ids.len(),
            "the disks name the replicas the world deploys"
        );
        let writer = self.clients.first().map(|client| client.id);
        self.replicas = ids
            .iter()
            .map(|id| {
                let id = NodeId(*id);
                assert!(!self.pool.contains(&id), "a replica is not an acceptor");
                let mut config = base.clone();
                config.id = id;
                let mut disk = Disk::new(config);
                if let Some(writer) = writer {
                    disk.provision_owner(ClientId(writer.0));
                }
                let role = ReplicaNode::new(&disk, Limits::DEFAULT);
                ReplicaProcess {
                    id,
                    disk,
                    role,
                    learned: Vec::new(),
                    caught_up: 0,
                }
            })
            .collect();
        self
    }

    /// The proxies, in id order.
    #[must_use]
    pub fn proxies(&self) -> &[ProxyProcess] {
        &self.proxies
    }

    /// The running proxy `id`.
    #[must_use]
    pub fn proxy(&self, id: ProxyId) -> Option<&ProxyLeader> {
        self.proxies
            .iter()
            .find(|proxy| proxy.id == id)
            .and_then(ProxyProcess::role)
    }

    /// The replicas, in id order.
    #[must_use]
    pub fn replicas(&self) -> &[ReplicaProcess] {
        &self.replicas
    }

    /// The replica `id`.
    #[must_use]
    pub fn replica(&self, id: NodeId) -> Option<&ReplicaProcess> {
        self.replicas.iter().find(|replica| replica.id == id)
    }

    /// Every round a leader took back from a proxy, in order.
    #[must_use]
    pub fn taken_back(&self) -> &[TakeBack] {
        &self.taken_back
    }

    /// The proxies, as the stage draws them.
    pub(super) fn proxy_views(&self) -> Vec<ProxyView> {
        self.proxies.iter().map(ProxyProcess::view).collect()
    }

    /// The replicas, as the stage draws them.
    pub(super) fn replica_views(&self) -> Vec<ReplicaView> {
        self.replicas.iter().map(ReplicaProcess::view).collect()
    }

    /// The ids of the replica tier.
    pub(super) fn replica_ids(&self) -> Vec<NodeId> {
        self.replicas.iter().map(|replica| replica.id).collect()
    }

    /// The leader's beat on a proxy deployment: take back every round a proxy
    /// swallowed past the budget, and say so.
    pub(super) fn take_back_stalled(&mut self, id: NodeId, index: usize) {
        if self.proxies.is_empty() {
            return;
        }
        let budget = self.take_back_after;
        let taken = self
            .drive(id, index, move |node| node.take_back_delegated(budget))
            .unwrap_or_default();
        for (slot, proxy) in taken {
            self.taken_back.push(TakeBack {
                leader: id,
                slot,
                proxy,
            });
            self.narrate(
                NarrationKind::Accept,
                format!(
                    "{} re-delegated slot {} to proxy {} {} and got no Commit back. It takes \
                     the round back and runs it colocated: it sends the Accept to the column \
                     itself. The classic Phase 2 is always the fallback, so a dead proxy costs \
                     time and never safety.",
                    who(id),
                    slot.0,
                    proxy.0,
                    many(usize::try_from(budget).unwrap_or(usize::MAX), "time")
                ),
            );
        }
    }

    /// Deliver one message to a proxy.
    pub(super) fn deliver_to_proxy(&mut self, entry: InFlight) {
        let (Party::Proxy(to), Envelope::Node(message)) = (entry.to, entry.envelope) else {
            return;
        };
        let Some(index) = self.proxies.iter().position(|proxy| proxy.id == to) else {
            return;
        };
        let kind = message_kind(&message);
        let slot = message_slot(&message);
        let Some(role) = self.proxies[index].role.as_mut() else {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "The {kind} reaches proxy {}, which is not running, so it is discarded.",
                    to.0
                ),
            );
            return;
        };
        let before = role.counters();
        role.step(message);
        let after = role.counters();
        let mark = self.wire.len();
        self.drain_proxy(index);
        let fanned = self.wire.len() - mark;
        let text = if after.decided > before.decided {
            format!(
                "Proxy {} has a Phase-2 quorum for slot {}. The slot is chosen. The proxy \
                 sends the Commit to every learner, the leader too. The leader did not count \
                 one vote.",
                to.0,
                slot.unwrap_or(0)
            )
        } else if after.refanned > before.refanned {
            format!(
                "Proxy {} already holds slot {}. The new delegation refreshes the leader it \
                 names, and the proxy sends the Accept to the column again: {}.",
                to.0,
                slot.unwrap_or(0),
                many(fanned, "message")
            )
        } else if after.delegated > before.delegated {
            format!(
                "Proxy {} takes slot {} from the leader and sends its Accept to the slot's \
                 column: {}. From here, the proxy counts the votes, not the leader.",
                to.0,
                slot.unwrap_or(0),
                many(fanned, "message")
            )
        } else if after.relayed_nacks > before.relayed_nacks {
            format!(
                "Proxy {} relays a Nack for slot {} to the leader that delegated it. A proxy \
                 adopts no ballot: the leader decides what a higher ballot means.",
                to.0,
                slot.unwrap_or(0)
            )
        } else {
            format!("Proxy {} receives the {kind}. Nothing changes.", to.0)
        };
        self.narrate(NarrationKind::Accept, text);
    }

    /// Drain one proxy's batch onto the wire.
    fn drain_proxy(&mut self, index: usize) {
        let pool = self.pool.clone();
        let replicas = self.replica_ids();
        let from = Party::Proxy(self.proxies[index].id);
        let Some(role) = self.proxies[index].role.as_mut() else {
            return;
        };
        let ready = role.ready();
        let mut out: Vec<(Party, Message)> = Vec::new();
        for (audience, message) in ready.messages() {
            for to in audience.resolve_from_proxy(&pool) {
                let party = if replicas.contains(&to) {
                    Party::Replica(to)
                } else {
                    Party::Node(to)
                };
                out.push((party, message.clone()));
            }
            if *audience == paros_core::contract::Audience::Learners {
                for to in &replicas {
                    out.push((Party::Replica(*to), message.clone()));
                }
            }
        }
        ready.advance();
        for (to, message) in out {
            self.send(from, to, Envelope::Node(message));
        }
    }

    /// Deliver one message to a replica.
    pub(super) fn deliver_to_replica(&mut self, entry: InFlight) {
        let (Party::Replica(to), Envelope::Node(message)) = (entry.to, entry.envelope) else {
            return;
        };
        let Some(index) = self.replicas.iter().position(|replica| replica.id == to) else {
            return;
        };
        let kind = message_kind(&message);
        let catch_up = matches!(message, Message::CatchUpResponse { .. });
        let before = self.replicas[index].learned.len();
        self.replicas[index].role.step(message);
        self.drain_replica(index);
        let learned = self.replicas[index].learned.len() - before;
        if catch_up {
            self.replicas[index].caught_up += learned as u64;
        }
        let gap = self.replicas[index].role.replica().chosen_gap();
        let text = match (learned, gap) {
            (0, Some((hole, highest))) => format!(
                "Replica {} receives the {kind}. It knows slot {} is chosen, but slot {} is a \
                 hole. A replica learns in slot order, so it waits.",
                to.0, highest.0, hole.0
            ),
            (0, None) => format!(
                "Replica {} receives the {kind}. It learns nothing new.",
                to.0
            ),
            (n, _) if catch_up => format!(
                "Replica {} learns {} from the catch-up answer of an acceptor. The acceptors \
                 keep the chosen log, so a replica that missed a Commit heals from them.",
                to.0,
                many(n, "slot")
            ),
            (n, _) => format!(
                "Replica {} learns {}. It did not vote. It only learned what the acceptors \
                 chose.",
                to.0,
                many(n, "slot")
            ),
        };
        self.narrate(NarrationKind::Applied, text);
    }

    /// Drain one replica's batch: persist, record what it learned, send.
    fn drain_replica(&mut self, index: usize) {
        let pool = self.pool.clone();
        let replicas = self.replica_ids();
        let me = self.replicas[index].id;
        let replica = &mut self.replicas[index];
        let ready = replica.role.ready();
        let writes = ready.writes().to_vec();
        let committed: Vec<(Slot, Command)> = ready
            .committed()
            .iter()
            .map(|(slot, _, command, _)| (*slot, command.clone()))
            .collect();
        let messages: Vec<(paros_core::contract::Audience, Message)> = ready.messages().to_vec();
        ready.advance();
        for write in &writes {
            replica.disk.apply(write);
        }
        replica.learned.extend(committed);
        for (audience, message) in messages {
            // A replica serves no peer and votes on nothing: it only asks an
            // acceptor for what it missed, and answers a beat.
            for to in audience.resolve(&pool, me) {
                let party = if replicas.contains(&to) {
                    Party::Replica(to)
                } else {
                    Party::Node(to)
                };
                self.send(Party::Replica(me), party, Envelope::Node(message.clone()));
            }
        }
    }

    /// Crash proxy `id`: it holds nothing durable, so everything it held is
    /// gone.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] when there is no such proxy or it is already down.
    pub fn crash_proxy(&mut self, id: ProxyId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.proxy_index(id)?;
        let Some(role) = self.proxies[index].role.take() else {
            return Err(ActionError::new(
                ActionErrorCode::NodeCrashed,
                format!("proxy {} is already crashed", id.0),
            ));
        };
        let open = role.rounds().by_slot().len();
        self.narrate(
            NarrationKind::Crash,
            format!(
                "Proxy {} crashes with {} open. A proxy has no disk and votes on nothing, so \
                 no decision is lost: only the leader can bring those rounds back.",
                id.0,
                many(open, "round")
            ),
        );
        Ok(())
    }

    /// Restart proxy `id`, empty.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] when there is no such proxy or it is running.
    pub fn restart_proxy(&mut self, id: ProxyId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.proxy_index(id)?;
        if self.proxies[index].role.is_some() {
            return Err(ActionError::new(
                ActionErrorCode::NodeAlive,
                format!("proxy {} is running", id.0),
            ));
        }
        let fresh = ProxyLeader::new(id, self.proxies[index].bootstrap.clone());
        self.proxies[index].role = Some(fresh);
        self.narrate(
            NarrationKind::Restart,
            format!(
                "Proxy {} restarts empty. It holds no round until a leader delegates one to it.",
                id.0
            ),
        );
        Ok(())
    }

    fn proxy_index(&self, id: ProxyId) -> Result<usize, ActionError> {
        self.proxies
            .iter()
            .position(|proxy| proxy.id == id)
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no proxy {} in this level", id.0),
                )
            })
    }
}

fn message_kind(message: &Message) -> &'static str {
    match message {
        Message::Accept { .. } => "Accept",
        Message::Accepted { .. } => "Accepted",
        Message::Nack { .. } => "Nack",
        Message::Commit { .. } => "Commit",
        Message::Heartbeat { .. } => "Heartbeat",
        Message::CatchUpResponse { .. } => "catch-up answer",
        _ => "message",
    }
}

fn message_slot(message: &Message) -> Option<u64> {
    match message {
        Message::Accept { slot, .. }
        | Message::Accepted { slot, .. }
        | Message::Commit { slot, .. }
        | Message::Nack { slot, .. } => Some(slot.0),
        _ => None,
    }
}
