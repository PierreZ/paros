//! The **matchmaker plane**: the registry tier, the candidate's matchmaking
//! phase, the garbage-collection floor, and the generation handover.
//!
//! This is `crates/paros-core/examples/matchmaker.rs` with the parts the
//! example hard-codes turned into player choices. The example's
//! `MatchmakerNode` — a [`Matchmaker`] role, the static
//! [`MatchmakerConfig`] it boots with, and a [`MemRegistry`] disk it writes to
//! and reboots from — is [`MatchmakerProcess`] here, and its `deliver_match` /
//! `deliver_reconfigure` are the two methods below: **step, persist, take the
//! reply, advance**, in that order and never another.
//!
//! # Where each piece lives
//!
//! - A **matchmaker** is not a node. It holds a registry and no log, it votes
//!   on no slot, and it has its own identity space
//!   ([`MatchmakerId`]), which is why the wire addresses a
//!   [`Party`] rather than a node id and why crashing one is its
//!   own verb.
//! - The **matchmaking phase** belongs to the candidate. `ColocatedNode` opens
//!   it, queues the requests through `Ready::match_requests`, and folds each
//!   answer through `on_match_reply`; the world only carries the messages.
//! - The **garbage-collection floor** belongs to the leader in the same way
//!   (`Ready::gc_requests`, `on_gc_ack`).
//! - The **reconfigurer** is a *node-side driver object*, not a role of the
//!   core's node: the example holds one beside the pool, and
//!   `crates/paros/src/driver/handover.rs` holds one per driver. So the world
//!   holds one per node, and the node that drives a handover is the node the
//!   player asked.
//!
//! # What the world decides, and what it does not
//!
//! Two decisions here are the **driver's**, exactly as they are in
//! `paros::run_node`, and the game gives them to the player's clock rather than
//! to an ack:
//!
//! - **closing a freeze** ([`paros_core::MatchmakerReconfigurer::close_stop`])
//!   happens on a
//!   beat, not on the ack that first completed the quorum, so a straggler that
//!   arrives in between widens the reconstruction;
//! - **abandoning a stalled handover** happens after
//!   the handover stall timeout beats without progress.
//!
//! Everything else is the core's own answer.

use std::collections::BTreeMap;

use paros_core::{
    AcceptorConfig, Ballot, ColocatedNode, GcAck, GcOutcome, GcRequest, MatchOutcome, MatchReply,
    MatchRequest, MatchStep, Matchmaker, MatchmakerConfig, MatchmakerHardState, MatchmakerId,
    MatchmakerSet, MemRegistry, NodeId, ReconfigureRefusal, ReconfigureReply, ReconfigureRequest,
    ReconfigureResult, ReconfigurerStep, Registration, RegistrationKind, StartRefusal,
};

use crate::action::{ActionError, ActionErrorCode};
use crate::narration::{NarrationKind, many, who};
use crate::prompt::{Prompt, PromptKind};
use crate::view::{MessageView, PartyView, show_ballot};
use crate::world::drain::Paused;
use crate::world::{Envelope, InFlight, Party, World, unknown_node};

/// How the narration names a matchmaker. Nodes are "node 1", matchmakers are
/// "matchmaker 1": two identity spaces, two words.
#[must_use]
pub(crate) fn which(id: MatchmakerId) -> String {
    format!("matchmaker {}", id.0)
}

/// An acceptor set, as every player-facing sentence names one.
#[must_use]
pub(crate) fn show_members(members: &[NodeId]) -> String {
    let ids: Vec<String> = members.iter().map(|id| id.0.to_string()).collect();
    format!("{{{}}}", ids.join(", "))
}

/// A matchmaker set, as every player-facing sentence names one.
#[must_use]
pub(crate) fn show_set(members: &[MatchmakerId]) -> String {
    let ids: Vec<String> = members.iter().map(|id| id.0.to_string()).collect();
    format!("{{{}}}", ids.join(", "))
}

/// One matchmaker's disk: the library's own [`MemRegistry`], plus the two
/// counters that make the flush order visible.
///
/// The **write** half is [`MemRegistry::apply`]; the **read** half is the
/// core's recovery port, so a reboot is `Matchmaker::new(&config, &store)` and
/// nothing else — which is what makes a registration survive a crash here for
/// the same reason it survives one in production.
#[derive(Clone, Debug, Default)]
pub struct RegistryDisk {
    store: MemRegistry,
    /// Writes applied so far.
    writes: usize,
    /// Writes an fsync covers. A reply leaves only while this equals `writes`.
    synced: usize,
}

impl RegistryDisk {
    /// The durable registry.
    #[must_use]
    pub fn store(&self) -> &MemRegistry {
        &self.store
    }

    /// The durable scalars: the watermark, the generation, the phase, the
    /// decree record.
    #[must_use]
    pub fn hard_state(&self) -> &MatchmakerHardState {
        self.store.hard_state()
    }

    /// The registry records, in ballot order.
    #[must_use]
    pub fn registrations(&self) -> &BTreeMap<Ballot, Registration> {
        self.store.registrations()
    }

    /// The fsync. Memory has nothing to flush, so this only records that every
    /// write so far is covered; what matters is **where** it is called.
    fn sync(&mut self) {
        self.synced = self.writes;
    }
}

/// One matchmaker process: the role, the configuration it boots with, and the
/// disk it reboots from. `None` for the role is a crashed matchmaker — its
/// disk survives, exactly as a node's does.
pub struct MatchmakerProcess {
    config: MatchmakerConfig,
    role: Option<Matchmaker>,
    disk: RegistryDisk,
}

impl MatchmakerProcess {
    /// A fresh matchmaker with an empty registry.
    #[must_use]
    pub fn new(id: MatchmakerId, bootstrap: Vec<MatchmakerId>) -> Self {
        Self::seeded(id, bootstrap, BTreeMap::new())
    }

    /// A matchmaker whose registry a level pre-seeded — how a level puts an
    /// earlier leader's registration in place before the player's first move.
    #[must_use]
    pub fn seeded(
        id: MatchmakerId,
        bootstrap: Vec<MatchmakerId>,
        registrations: BTreeMap<Ballot, Registration>,
    ) -> Self {
        let config = MatchmakerConfig { id, bootstrap };
        let disk = RegistryDisk {
            store: MemRegistry::new(MatchmakerHardState::default(), registrations),
            writes: 0,
            synced: 0,
        };
        Self {
            role: Some(Matchmaker::new(&config, disk.store())),
            config,
            disk,
        }
    }

    /// This matchmaker's id.
    #[must_use]
    pub fn id(&self) -> MatchmakerId {
        self.config.id
    }

    /// Whether it is running.
    #[must_use]
    pub fn alive(&self) -> bool {
        self.role.is_some()
    }

    /// The live role, if it is running.
    #[must_use]
    pub fn role(&self) -> Option<&Matchmaker> {
        self.role.as_ref()
    }

    /// Its disk.
    #[must_use]
    pub fn disk(&self) -> &RegistryDisk {
        &self.disk
    }

    /// Drop the volatile role; the registry survives.
    fn crash(&mut self) {
        self.role = None;
    }

    /// Rebuild the role from the disk, through the core's recovery port.
    fn reboot(&mut self) {
        self.role = Some(Matchmaker::new(&self.config, self.disk.store()));
    }

    /// Persist one batch: apply every write, fsync, and only then let the
    /// batch's replies escape.
    ///
    /// # Panics
    ///
    /// If a reply would leave ahead of an unsynced write.
    fn persist(&mut self) {
        let Some(role) = self.role.as_mut() else {
            return;
        };
        let ready = role.ready();
        for op in ready.writes() {
            self.disk.store.apply(op);
            self.disk.writes += 1;
        }
        drop(ready);
        self.disk.sync();
        assert!(
            self.disk.synced == self.disk.writes,
            "no reply leaves ahead of an unsynced write"
        );
    }

    /// Deliver one matchmaking request: step, persist, reply, advance.
    fn deliver_match(&mut self, request: MatchRequest) -> Option<MatchReply> {
        self.role.as_mut()?.step(request);
        self.persist();
        let role = self.role.as_mut()?;
        let ready = role.ready();
        let reply = ready.replies().first().cloned();
        ready.advance();
        reply
    }

    /// Deliver one garbage-collection request: raise the floor, persist, ack.
    fn deliver_gc(&mut self, request: GcRequest) -> Option<GcAck> {
        let outcome = self
            .role
            .as_mut()?
            .advance_gc_watermark(request.generation, request.watermark);
        self.persist();
        let role = self.role.as_ref()?;
        Some(GcAck {
            matchmaker: self.config.id,
            generation: request.generation,
            applied: outcome != GcOutcome::Refused,
            watermark: role.hard_state().gc_watermark,
        })
    }

    /// Deliver one handover step: step, persist, reply, advance.
    fn deliver_reconfigure(&mut self, request: ReconfigureRequest) -> Option<ReconfigureReply> {
        self.role.as_mut()?.step_reconfigure(request);
        self.persist();
        let role = self.role.as_mut()?;
        let ready = role.ready();
        let reply = ready.reconfigure_replies().first().cloned();
        ready.advance();
        reply
    }
}

// ---- rendering ---------------------------------------------------------------

/// Render one matchmaker-plane message for the wire list and the stage.
#[must_use]
#[allow(clippy::too_many_lines)]
pub(crate) fn plane_view(entry: &InFlight) -> Option<MessageView> {
    let (kind, phase, ballot, summary, reply) = match &entry.envelope {
        Envelope::Node(_) => return None,
        Envelope::Match(request) => (
            "MatchRequest",
            "match",
            Some(request.ballot),
            format!(
                "Register {} with {}, generation {}",
                show_ballot(request.ballot),
                show_members(request.config.members()),
                request.generation.0
            ),
            false,
        ),
        Envelope::MatchReply(answer) => (
            "MatchReply",
            "match",
            Some(answer.ballot),
            match &answer.outcome {
                MatchOutcome::Registered { history, .. } => format!(
                    "Registered {}: {} below it",
                    show_ballot(answer.ballot),
                    many(history.len(), "configuration")
                ),
                MatchOutcome::Refused(refusal) => {
                    format!("Refused {}: {}", show_ballot(answer.ballot), why(refusal))
                }
            },
            true,
        ),
        Envelope::Gc(request) => (
            "GcRequest",
            "gc",
            Some(request.watermark),
            format!("Raise the watermark to {}", show_ballot(request.watermark)),
            false,
        ),
        Envelope::GcAck(ack) => (
            "GcAck",
            "gc",
            Some(ack.watermark),
            if ack.applied {
                format!("The watermark is now {}", show_ballot(ack.watermark))
            } else {
                format!(
                    "Refused: the watermark stays at {}",
                    show_ballot(ack.watermark)
                )
            },
            true,
        ),
        Envelope::Reconfigure(request) => {
            let (kind, summary) = match request {
                ReconfigureRequest::Stop { generation, .. } => {
                    ("Stop", format!("Freeze generation {}", generation.0))
                }
                ReconfigureRequest::Bootstrap { bootstrap, .. } => (
                    "Bootstrap",
                    format!(
                        "Hold generation {} = {} pending, with {}",
                        bootstrap.set.generation.0,
                        show_set(bootstrap.set.members()),
                        many(bootstrap.history.len(), "registration")
                    ),
                ),
                ReconfigureRequest::DecreePrepare { ballot, .. } => (
                    "DecreePrepare",
                    format!("Prepare {} over the one decree slot", show_ballot(*ballot)),
                ),
                ReconfigureRequest::DecreeAccept {
                    ballot, members, ..
                } => (
                    "DecreeAccept",
                    format!(
                        "Accept {} at {} in the one decree slot",
                        show_set(members),
                        show_ballot(*ballot)
                    ),
                ),
                ReconfigureRequest::Chosen { successor, .. } => (
                    "Chosen",
                    format!(
                        "Generation {} = {} is chosen",
                        successor.generation.0,
                        show_set(successor.members())
                    ),
                ),
            };
            (kind, "reconfigure", None, summary, false)
        }
        Envelope::ReconfigureReply(answer) => {
            let (kind, summary) = match answer {
                ReconfigureReply::Stopped {
                    generation,
                    history,
                    ..
                } => (
                    "Stopped",
                    format!(
                        "Frozen for generation {}, handing over {}",
                        generation.0,
                        many(history.len(), "registration")
                    ),
                ),
                ReconfigureReply::Bootstrapped { set, .. } => (
                    "Bootstrapped",
                    format!("Generation {} is held pending here", set.generation.0),
                ),
                ReconfigureReply::Promised { ballot, vote, .. } => (
                    "Promised",
                    match vote {
                        Some((at, members)) => format!(
                            "Promised {}; it already voted {} at {}",
                            show_ballot(*ballot),
                            show_set(members),
                            show_ballot(*at)
                        ),
                        None => format!(
                            "Promised {}; it has voted for nothing",
                            show_ballot(*ballot)
                        ),
                    },
                ),
                ReconfigureReply::Accepted { ballot, .. } => (
                    "DecreeAccepted",
                    format!("Voted at {}", show_ballot(*ballot)),
                ),
                ReconfigureReply::Nacked { promised, .. } => (
                    "DecreeNack",
                    format!("Refused: it promised {}", show_ballot(*promised)),
                ),
                ReconfigureReply::Learned { activated, at, .. } => (
                    "Learned",
                    if *activated {
                        format!("Generation {} is active here now", at.0)
                    } else {
                        format!("Recorded the successor; it stays at generation {}", at.0)
                    },
                ),
                ReconfigureReply::Refused { current, .. } => (
                    "ReconfigureRefused",
                    format!("Refused: it holds generation {}", current.generation.0),
                ),
            };
            (kind, "reconfigure", None, summary, true)
        }
    };
    Some(MessageView {
        id: entry.id,
        kind: kind.to_string(),
        from: entry.from.number(),
        from_party: entry.from.party_view(),
        to: entry.to.number(),
        to_party: entry.to.party_view(),
        ballot: ballot.map(show_ballot),
        slot: None,
        column: None,
        summary,
        phase: phase.to_string(),
        reply,
        sent_at: entry.sent_at,
    })
}

/// Why a matchmaker refused a registration, in one clause.
#[must_use]
pub(crate) fn why(refusal: &paros_core::MatchRefusal) -> String {
    match refusal {
        paros_core::MatchRefusal::Stale { highest } => format!(
            "it has already registered {}, and a ballot must be above every registered one",
            show_ballot(*highest)
        ),
        paros_core::MatchRefusal::BelowWatermark { watermark } => format!(
            "its watermark is {}, and nothing below a watermark registers again",
            show_ballot(*watermark)
        ),
        paros_core::MatchRefusal::Stopped {
            successor: Some(set),
        } => format!(
            "it is frozen, and its successor is generation {} = {}",
            set.generation.0,
            show_set(set.members())
        ),
        paros_core::MatchRefusal::Stopped { successor: None } => {
            "it is frozen, and no successor is chosen yet".to_string()
        }
        paros_core::MatchRefusal::Generation { current } => format!(
            "it now serves generation {} = {}",
            current.generation.0,
            show_set(current.members())
        ),
        paros_core::MatchRefusal::Inactive => "it serves no generation: it is a spare".to_string(),
    }
}

// ---- the world's matchmaker plane -------------------------------------------

impl World {
    /// The matchmakers this level deployed, in id order. Empty on a plain
    /// deployment, which is the whole of Act I to Act III.
    #[must_use]
    pub fn matchmakers(&self) -> &[MatchmakerProcess] {
        &self.matchmakers
    }

    /// The matchmaker with `id`, if this level deployed one.
    #[must_use]
    pub fn matchmaker(&self, id: MatchmakerId) -> Option<&MatchmakerProcess> {
        self.matchmakers.iter().find(|m| m.id() == id)
    }

    /// Whether an operator retired `id`: it answered the evidence, shut down,
    /// and it never comes back.
    #[must_use]
    pub fn retired(&self, id: NodeId) -> bool {
        self.index_of(id).is_some_and(|index| self.retired[index])
    }

    /// Every retire request the engine refused for want of evidence:
    /// `(node, the watermark the operator showed)`.
    #[must_use]
    pub fn refused_retires(&self) -> &[(NodeId, Ballot)] {
        &self.refused_retires
    }

    /// The garbage-collection floor `id`'s leadership made effective, and the
    /// acceptors it released. `None` until a matchmaker quorum has acked it.
    #[must_use]
    pub fn gc_effective(&self, id: NodeId) -> Option<(Ballot, Vec<NodeId>)> {
        self.node(id)
            .and_then(ColocatedNode::gc_effective)
            .map(|(watermark, retired)| (watermark, retired.to_vec()))
    }

    /// Every acceptor configuration a node was told about before it opened its
    /// Phase 1 — `H_b`, recorded when the matchmaking phase completed.
    #[must_use]
    pub fn prior_configurations(&self, id: NodeId) -> Vec<AcceptorConfig> {
        self.index_of(id)
            .map(|index| self.campaign_prior[index].clone())
            .unwrap_or_default()
    }

    fn matchmaker_index(&self, id: MatchmakerId) -> Result<usize, ActionError> {
        self.matchmakers
            .iter()
            .position(|m| m.id() == id)
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no matchmaker {} in this level", id.0),
                )
            })
    }

    /// Turn a player's membership and quorum system into a configuration the
    /// core will accept, or refuse it with a reason.
    ///
    /// [`AcceptorConfig::new`] **asserts** an empty membership and one that
    /// does not admit its quorum system, and an assert in wasm is an abort
    /// with no stack. So every one of those is checked here first, and the
    /// player reads a sentence.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the set is not one this cluster can run.
    pub fn compose(
        &self,
        members: &[u64],
        system: paros_core::QuorumSystem,
    ) -> Result<AcceptorConfig, ActionError> {
        if members.is_empty() {
            return Err(ActionError::new(
                ActionErrorCode::BadReach,
                "an acceptor set names at least one acceptor",
            ));
        }
        let mut ids: Vec<NodeId> = members.iter().map(|id| NodeId(*id)).collect();
        ids.sort_unstable();
        ids.dedup();
        for id in &ids {
            if self.index_of(*id).is_none() {
                return Err(unknown_node(*id));
            }
        }
        if !system.admits(ids.len()) {
            return Err(ActionError::new(
                ActionErrorCode::BadReach,
                format!(
                    "{} acceptors do not admit that quorum system: every configuration must have \
                     a Phase-1 quorum and a Phase-2 quorum that meet",
                    ids.len()
                ),
            ));
        }
        Ok(AcceptorConfig::new(ids, system))
    }

    // ---- the operator's matchmaker verbs ------------------------------------

    /// Drop `id`'s volatile role; its registry survives.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn crash_matchmaker(&mut self, id: MatchmakerId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.matchmaker_index(id)?;
        if !self.matchmakers[index].alive() {
            return Err(ActionError::new(
                ActionErrorCode::NodeCrashed,
                format!("matchmaker {} is already crashed", id.0),
            ));
        }
        let held = self.matchmakers[index].disk().registrations().len();
        let watermark = self.matchmakers[index].disk().hard_state().gc_watermark;
        self.matchmakers[index].crash();
        self.narrate(
            NarrationKind::Crash,
            format!(
                "{} crashes. Its registry keeps {} and the watermark {}. A matchmaker answers a \
                 candidate only after the write is on the disk, so everything it ever promised is \
                 still there.",
                which(id),
                many(held, "registration"),
                show_ballot(watermark)
            ),
        );
        Ok(())
    }

    /// Rebuild a crashed matchmaker from its registry.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn restart_matchmaker(&mut self, id: MatchmakerId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.matchmaker_index(id)?;
        if self.matchmakers[index].alive() {
            return Err(ActionError::new(
                ActionErrorCode::NodeAlive,
                format!("matchmaker {} is already running", id.0),
            ));
        }
        self.matchmakers[index].reboot();
        let held = self.matchmakers[index].disk().registrations().len();
        let generation = self.matchmakers[index].disk().hard_state().generation;
        self.narrate(
            NarrationKind::Restart,
            format!(
                "{} restarts from its registry: {}, generation {}. The role is built from the \
                 disk and from nothing else.",
                which(id),
                many(held, "registration"),
                generation.0
            ),
        );
        Ok(())
    }

    /// A client asks the leader at `id` to put `config` in force.
    ///
    /// A reconfiguration is a **round change**: the leader moves to a fresh
    /// ballot registered with the new configuration, and leads again under it
    /// once the cross-configuration Phase 1 completes. Every refusal is an
    /// operating condition the caller retries, and a cluster with no
    /// matchmakers refuses every one of them.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn reconfigure(&mut self, id: NodeId, config: &AcceptorConfig) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let members = show_members(config.members());
        let mark = self.narration.len();
        let requested = config.clone();
        let result = self.observe(id, move |world| {
            let out = world.nodes[index]
                .as_mut()
                .map(|node| node.reconfigure(&requested));
            world.pump(id);
            out
        });
        let text = match result {
            Some(ReconfigureResult::Started(ballot)) => format!(
                "A client asks {} to run with the acceptors {members}. The leader does not edit \
                 the set it has. It opens a fresh ballot, {}, and registers the new set with the \
                 matchmakers under that ballot. A configuration belongs to one ballot, so the \
                 only way to change it is a new ballot. The leader proposes nothing new until it \
                 holds that new ballot.",
                who(id),
                show_ballot(ballot)
            ),
            Some(ReconfigureResult::NotLeader(hint)) => {
                self.narration.truncate(mark);
                return Err(ActionError::new(
                    ActionErrorCode::NotLeader,
                    match hint {
                        Some(leader) => format!(
                            "node {} is not the leader; a reconfiguration goes to node {}",
                            id.0, leader.0
                        ),
                        None => format!(
                            "node {} is not the leader, and it does not know who is",
                            id.0
                        ),
                    },
                ));
            }
            Some(ReconfigureResult::Refused(refusal)) => {
                self.reconfigure_refused(id, refusal, &members);
                let opening = crate::narration::say(
                    NarrationKind::Reconfigure,
                    format!(
                        "A client asks {} to run with the acceptors {members}, and the leader \
                         refuses.",
                        who(id)
                    ),
                );
                self.narration.insert(mark, opening);
                return Ok(());
            }
            None => format!("{} is not running.", who(id)),
        };
        let opening = crate::narration::say(NarrationKind::Reconfigure, text);
        self.narration.insert(mark, opening);
        Ok(())
    }

    /// Say what a refused reconfiguration was refused for.
    fn reconfigure_refused(&mut self, id: NodeId, refusal: ReconfigureRefusal, members: &str) {
        let text = match refusal {
            ReconfigureRefusal::NoMatchmakers => format!(
                "{} names no matchmakers. Plain Multi-Paxos has one fixed acceptor set for the \
                 life of the cluster. There is nowhere to record a second set, so a later leader \
                 could not learn that {members} ever existed. The request is refused, and it is \
                 not queued.",
                who(id)
            ),
            ReconfigureRefusal::Unchanged => {
                format!("{members} is the acceptor set already in force.")
            }
            ReconfigureRefusal::UnknownMember => format!(
                "{members} names a node this deployment cannot reach. A new acceptor must be in \
                 the pool before it can be added to a configuration."
            ),
            ReconfigureRefusal::Malformed => format!(
                "{members} does not admit the quorum system asked for. Every configuration must \
                 have a Phase-1 quorum and a Phase-2 quorum that meet."
            ),
            ReconfigureRefusal::Unsettled => format!(
                "{} still has Phase-1 work open: a slot to settle, a damaged record to repair, or \
                 an application prefix to complete. A reconfiguration moves a settled leadership. \
                 Ask again once the recovery closes.",
                who(id)
            ),
            ReconfigureRefusal::RoundExhausted => {
                "there is no higher ballot left to move to.".to_string()
            }
        };
        self.narrate(NarrationKind::Reconfigure, text);
    }

    /// An operator asks `target` to retire, showing the effective watermark it
    /// read from `id`'s report.
    ///
    /// The evidence is what makes this safe. "I am not in the configuration in
    /// force" is a **belief** — it is volatile, and a reboot puts the node back
    /// on its bootstrap configuration — so a node that answered on that belief
    /// alone could shut down while a configuration it is still needed for is
    /// alive. A watermark strictly above every ballot a configuration naming
    /// this node was bound to is a **fact**: a matchmaker quorum durably
    /// refuses every campaign that could still ask for it.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn retire(
        &mut self,
        id: NodeId,
        target: NodeId,
        watermark: Ballot,
    ) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        self.require_live(id)?;
        let index = self.require_live(target)?;
        let effective = self.gc_effective(id).map(|(ballot, _)| ballot);
        if let Some(prompt) = self.may_retire_prompt(target, index, watermark, effective) {
            self.narrate(
                NarrationKind::Gc,
                format!(
                    "An operator asks {} to shut down permanently. {} You answer for it.",
                    who(target),
                    prompt.question
                ),
            );
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Retire { target, watermark });
            return Ok(());
        }
        self.retire_now(target, watermark);
        Ok(())
    }

    /// Honour or refuse the retirement, once nobody owes an answer for it.
    pub(super) fn retire_now(&mut self, target: NodeId, watermark: Ballot) {
        let Some(index) = self.index_of(target) else {
            return;
        };
        let may = self.nodes[index]
            .as_ref()
            .is_some_and(|node| node.may_retire(watermark));
        if !may {
            self.refused_retires.push((target, watermark));
            self.narrate(
                NarrationKind::Gc,
                format!(
                    "{} refuses to retire, and the reason is \"not collected\". The watermark it \
                     was shown, {}, is not above every ballot a configuration naming this node \
                     was bound to. An installed successor is not a collected predecessor. Some \
                     future leader may still need this node's Phase-1 promise, until a \
                     matchmaker quorum durably refuses those ballots.",
                    who(target),
                    show_ballot(watermark)
                ),
            );
            return;
        }
        self.nodes[index] = None;
        self.armed_seams[index] = None;
        self.retired[index] = true;
        self.narrate(
            NarrationKind::Gc,
            format!(
                "{} retires. It is not in the acceptor set in force, and it does not lead. The \
                 watermark {} sits above every ballot a configuration naming it was bound to. No \
                 future leader can ask it for a promise, so it may stop permanently.",
                who(target),
                show_ballot(watermark)
            ),
        );
        self.settle();
    }

    /// A node drives a handover of the matchmaker set onto `members`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn reconfigure_matchmakers(
        &mut self,
        id: NodeId,
        members: Vec<MatchmakerId>,
    ) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let Some(current) = self
            .node(id)
            .and_then(ColocatedNode::matchmaker_set)
            .cloned()
        else {
            return Err(ActionError::new(
                ActionErrorCode::NoMatchmakers,
                format!(
                    "node {} names no matchmakers, so there is no matchmaker set to replace",
                    id.0
                ),
            ));
        };
        for member in &members {
            if self.matchmaker(*member).is_none() {
                return Err(ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no matchmaker {} in this level", member.0),
                ));
            }
        }
        let proposed = show_set(&members);
        match self.reconfigurers[index].start(&current, members) {
            Ok(()) => {}
            Err(StartRefusal::Busy) => {
                return Err(ActionError::new(
                    ActionErrorCode::HandoverBusy,
                    format!("node {} is already driving a handover", id.0),
                ));
            }
            Err(StartRefusal::Empty) => {
                return Err(ActionError::new(
                    ActionErrorCode::UnknownParty,
                    "a matchmaker set names at least one matchmaker".to_string(),
                ));
            }
        }
        self.narrate(
            NarrationKind::Generation,
            format!(
                "{} starts a handover of the matchmaker set: generation {} = {} is replaced by \
                 {}. The first step freezes the old generation. A frozen matchmaker registers \
                 nothing more, so the copy taken next cannot change while it is read.",
                who(id),
                current.generation.0,
                show_set(current.members()),
                proposed
            ),
        );
        self.queue_reconfigurer(index);
        Ok(())
    }

    /// Re-queue a candidate's open registration toward every matchmaker that
    /// has not answered. Skipping is always safe: a matchmaker answers a
    /// repeated request from its own registry, and a campaign that never
    /// completes is abandoned at the next election timeout.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn resend_matchmaking(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.narrate(
            NarrationKind::Matchmaking,
            format!(
                "{} asks the matchmakers that have not answered again. A matchmaker answers the \
                 same request the same way, and writes nothing a second time.",
                who(id)
            ),
        );
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.resend_matchmaking();
            }
            world.pump(id);
        });
        Ok(())
    }

    /// Re-queue a leader's open garbage-collection request. Skipping is always
    /// safe: a floor that never rises costs unbounded histories, never safety.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn resend_gc(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.narrate(
            NarrationKind::Gc,
            format!(
                "{} asks the matchmakers that have not acked the floor again.",
                who(id)
            ),
        );
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.resend_gc();
            }
            world.pump(id);
        });
        Ok(())
    }

    /// Re-issue a handover's current step, and close a freeze whose quorum has
    /// answered.
    ///
    /// Closing on this cadence rather than on the ack that completed the
    /// quorum is deliberate: a quorum is the floor the reconstruction rests on,
    /// and every straggler that arrives before the close widens it.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn resend_reconfigurer(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        if !self.reconfigurers[index].is_busy() {
            return Err(ActionError::new(
                ActionErrorCode::NoHandover,
                format!("node {} is not driving a handover", id.0),
            ));
        }
        self.reconfigurers[index].resend();
        self.close_freeze(index);
        self.queue_reconfigurer(index);
        Ok(())
    }

    /// One beat of a running handover: the stall clock advances, a freeze whose
    /// quorum answered is closed, and a phase that has made no progress for
    /// the handover stall timeout beats is abandoned.
    ///
    /// The timeout is the **driver's** policy, never a constant inside the
    /// state machine, and it is a level tunable here. Its floor is structural:
    /// a phase must get more beats than one round trip needs, or a handover
    /// that is simply slow is abandoned every time. Zero disables it, which is
    /// what a level that never wants a handover abandoned sets.
    pub(super) fn beat_reconfigurer(&mut self, index: usize) {
        if !self.reconfigurers[index].is_busy() {
            return;
        }
        self.reconfigurers[index].tick();
        self.close_freeze(index);
        let timeout = self.reconfigure_timeout_ticks;
        if timeout > 0 && self.reconfigurers[index].stalled_for() >= timeout {
            let id = self.pool[index];
            if self.reconfigurers[index].abandon() {
                self.narrate(
                    NarrationKind::Generation,
                    format!(
                        "{} gives up the handover it was driving: it made no progress for \
                         {timeout} beats. Nothing is lost. The freeze and the votes are on the \
                         matchmakers' own disks, and the next node that meets the frozen \
                         generation finishes it.",
                        who(id)
                    ),
                );
            }
        }
        self.queue_reconfigurer(index);
    }

    /// Close a freeze whose quorum has answered.
    fn close_freeze(&mut self, index: usize) {
        if !self.reconfigurers[index].stop_quorum_reached() {
            return;
        }
        let Some(reconstruction) = self.reconfigurers[index].close_stop() else {
            return;
        };
        let id = self.pool[index];
        self.narrate(
            NarrationKind::Generation,
            format!(
                "{} closes the freeze. A quorum of the old generation answered. It takes the \
                 highest watermark they reported, and the union of their registries above it: \
                 {}. Every registration that ever completed reached a quorum of the old \
                 generation. Any two quorums share a matchmaker, so nothing can be missing from \
                 that union.",
                who(id),
                many(reconstruction.bootstrap.history.len(), "registration")
            ),
        );
    }

    // ---- the wire ------------------------------------------------------------

    /// Keep the shadow matchmaking phase in step with the node's own.
    ///
    /// A candidate that opens a campaign gets a fresh instance of the core's
    /// [`paros_core::matchmaking::Matchmaking`] role, built from the very
    /// ballot, configuration and kind the node reports. A campaign that closed
    /// takes its shadow with it.
    pub(super) fn sync_matchmaking_shadow(&mut self, index: usize) {
        let open = self.nodes[index]
            .as_ref()
            .and_then(ColocatedNode::matchmaking)
            .map(|(ballot, config, kind)| (ballot, config.clone(), kind));
        match open {
            None => self.matchmaking_shadow[index] = None,
            Some((ballot, config, kind)) => {
                let stale = self.matchmaking_shadow[index]
                    .as_ref()
                    .is_none_or(|shadow| shadow.ballot() != ballot);
                if stale {
                    self.matchmaking_shadow[index] = Some(
                        paros_core::matchmaking::Matchmaking::new(ballot, config, kind),
                    );
                }
            }
        }
    }

    /// Push a running handover's current step onto the wire.
    pub(super) fn queue_reconfigurer(&mut self, index: usize) {
        let id = self.pool[index];
        let ready = self.reconfigurers[index].ready();
        let requests: Vec<(MatchmakerId, ReconfigureRequest)> = ready.requests().to_vec();
        ready.advance();
        for (to, request) in requests {
            self.send(
                Party::Node(id),
                Party::Matchmaker(to),
                Envelope::Reconfigure(request),
            );
        }
    }

    /// Queue one message.
    pub(super) fn send(&mut self, from: Party, to: Party, envelope: Envelope) {
        self.wire.push(InFlight {
            id: self.next_message_id,
            from,
            to,
            envelope,
            sent_at: self.clock,
        });
        self.next_message_id += 1;
    }

    /// Deliver one message to a matchmaker.
    pub(super) fn deliver_to_matchmaker(&mut self, entry: InFlight) {
        let Party::Matchmaker(to) = entry.to else {
            return;
        };
        let Ok(index) = self.matchmaker_index(to) else {
            return;
        };
        if !self.matchmakers[index].alive() {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "The message reaches {}, which is not running, so it is discarded.",
                    which(to)
                ),
            );
            return;
        }
        match entry.envelope {
            Envelope::Match(request) => {
                if let Some(prompt) = self.generation_fence_prompt(to, index, &request) {
                    self.narrate(
                        NarrationKind::Matchmaking,
                        format!(
                            "A registration reaches {}. {} You answer for it, and the real \
                             registry marks the answer.",
                            which(to),
                            prompt.question
                        ),
                    );
                    self.prompt = Some(prompt);
                    self.paused = Some(Paused::Registration {
                        from: entry.from,
                        to,
                        request: Box::new(request),
                    });
                    return;
                }
                self.register_now(entry.from, to, request);
            }
            Envelope::Gc(request) => {
                let ack = self.matchmakers[index].deliver_gc(request);
                if let Some(ack) = ack {
                    self.narrate(
                        NarrationKind::Gc,
                        format!(
                            "{} raises its watermark to {} and writes it down before it answers. \
                             From now on it refuses every campaign below that floor, so no later \
                             history can name a configuration it dropped.",
                            which(to),
                            show_ballot(ack.watermark)
                        ),
                    );
                    self.send(entry.to, entry.from, Envelope::GcAck(ack));
                }
            }
            Envelope::Reconfigure(request) => {
                let reply = self.matchmakers[index].deliver_reconfigure(request);
                if let Some(reply) = reply {
                    self.narrate_handover_reply(to, &reply);
                    self.send(entry.to, entry.from, Envelope::ReconfigureReply(reply));
                }
            }
            Envelope::Node(_)
            | Envelope::MatchReply(_)
            | Envelope::GcAck(_)
            | Envelope::ReconfigureReply(_) => {}
        }
    }

    /// Register for real, once nobody owes an answer for it.
    pub(super) fn register_now(&mut self, from: Party, to: MatchmakerId, request: MatchRequest) {
        let Ok(index) = self.matchmaker_index(to) else {
            return;
        };
        let ballot = request.ballot;
        let members = show_members(request.config.members());
        let reconfiguration = request.kind == RegistrationKind::Reconfiguration;
        let Some(reply) = self.matchmakers[index].deliver_match(request) else {
            return;
        };
        let text = match &reply.outcome {
            MatchOutcome::Registered { history, .. } => format!(
                "{} writes down that ballot {} runs with the acceptors {members}{}. It answers \
                 only after that write. It reports the {} it holds below that ballot. The write \
                 comes first: a registration it forgot would leave a later leader asking the \
                 wrong acceptors.",
                which(to),
                show_ballot(ballot),
                if reconfiguration {
                    " (an operator's change, and not a belief)"
                } else {
                    ""
                },
                many(history.len(), "configuration")
            ),
            MatchOutcome::Refused(refusal) => format!(
                "{} refuses ballot {}: {}.",
                which(to),
                show_ballot(ballot),
                why(refusal)
            ),
        };
        self.narrate(NarrationKind::Matchmaking, text);
        self.send(Party::Matchmaker(to), from, Envelope::MatchReply(reply));
    }

    /// Deliver one matchmaker answer to the node that asked.
    pub(super) fn deliver_matchmaker_reply(&mut self, entry: InFlight) {
        let Party::Node(to) = entry.to else {
            return;
        };
        let Some(index) = self.index_of(to) else {
            return;
        };
        if self.nodes[index].is_none() {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "The answer reaches {}, which is not running, so it is discarded.",
                    who(to)
                ),
            );
            return;
        }
        match entry.envelope {
            Envelope::MatchReply(reply) => {
                if let Some(prompt) = self.stale_configuration_prompt(to, index, &reply) {
                    self.narrate(
                        NarrationKind::Matchmaking,
                        format!(
                            "The matchmakers have answered {}. {} You answer for it.",
                            who(to),
                            prompt.question
                        ),
                    );
                    self.prompt = Some(prompt);
                    self.paused = Some(Paused::MatchReply {
                        node: to,
                        reply: Box::new(reply),
                    });
                    return;
                }
                self.fold_match_reply(to, index, reply);
            }
            Envelope::GcAck(ack) => {
                let step = self.observe(to, move |world| {
                    let out = world.nodes[index].as_mut().map(|node| node.on_gc_ack(&ack));
                    world.pump(to);
                    out
                });
                if let Some(step) = step {
                    self.narrate_gc_step(to, &step);
                }
            }
            Envelope::ReconfigureReply(reply) => {
                let step = self.reconfigurers[index].on_reply(reply);
                self.narrate_reconfigurer_step(to, index, &step);
                self.close_freeze(index);
                self.queue_reconfigurer(index);
            }
            Envelope::Node(_) | Envelope::Match(_) | Envelope::Gc(_) | Envelope::Reconfigure(_) => {
            }
        }
    }

    /// Fold one matchmaker answer for real, once nobody owes an answer for it.
    pub(super) fn fold_match_reply(&mut self, to: NodeId, index: usize, reply: MatchReply) {
        // The shadow is fed exactly what the node is fed, and in the same
        // order, so the `StaleConfiguration` oracle sees the union the node
        // sees.
        if let Some(shadow) = self.matchmaking_shadow[index].as_mut()
            && shadow.ballot() == reply.ballot
        {
            let (matchmaker, answer) =
                paros_core::matchmaking::RegisteredPage::from_reply(reply.clone());
            if let Ok(page) = answer {
                shadow.fold(matchmaker, page);
            }
        }
        let step = self.observe(to, move |world| {
            let out = world.nodes[index]
                .as_mut()
                .map(|node| node.on_match_reply(reply));
            world.pump(to);
            out
        });
        if let Some(step) = step {
            if let MatchStep::Completed { prior, .. } = &step {
                self.campaign_prior[index].clone_from(prior);
            }
            self.narrate_match_step(to, &step);
        }
    }

    // ---- narration -----------------------------------------------------------

    fn narrate_match_step(&mut self, id: NodeId, step: &MatchStep) {
        let text = match step {
            MatchStep::Ignored => return,
            MatchStep::Paged { next } => format!(
                "{} counts nothing yet: that matchmaker's answer is one page of several, and it \
                 asks for the rest from ballot {}.",
                who(id),
                show_ballot(*next)
            ),
            MatchStep::Registered { remaining } => format!(
                "{} counts one registration. {} more must answer before the quorum holds.",
                who(id),
                remaining
            ),
            MatchStep::Completed {
                prior, watermark, ..
            } => {
                let named = if prior.is_empty() {
                    "no configuration at all, so there is nothing for Phase 1 to recover"
                        .to_string()
                } else {
                    format!(
                        "{}: {}",
                        many(prior.len(), "earlier configuration"),
                        prior
                            .iter()
                            .map(|config| show_members(config.members()))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                format!(
                    "A matchmaker quorum has answered {}. The union of their histories, above the \
                     watermark {}, names {named}. Phase 1 may open now, and not before. The \
                     matchmakers prove that no earlier ballot chose a value this candidate \
                     cannot see.",
                    who(id),
                    show_ballot(*watermark)
                )
            }
            MatchStep::Refused(refusal) => format!(
                "{} abandons its campaign: a matchmaker refused it, because {}. A refused \
                 registration does not become a leadership. The next campaign opens at a higher \
                 round.",
                who(id),
                why(refusal)
            ),
            MatchStep::Superseded { set } => format!(
                "{} learns that the matchmaker set has changed: generation {} = {} is \
                 authoritative now. It abandons this campaign and asks the new generation next \
                 time.",
                who(id),
                set.generation.0,
                show_set(set.members())
            ),
            MatchStep::StaleConfiguration { newest } => format!(
                "{} abandons its campaign and adopts the configuration registered at {}. Its own \
                 belief was out of date, because an operator changed the acceptor set while this \
                 node was not listening. A candidate elected under a superseded set would undo \
                 that change, and nobody asked for that.",
                who(id),
                show_ballot(*newest)
            ),
        };
        self.narrate(NarrationKind::Matchmaking, text);
    }

    fn narrate_gc_step(&mut self, id: NodeId, step: &paros_core::GcStep) {
        use paros_core::GcStep;
        let text = match step {
            GcStep::Ignored => return,
            GcStep::Acked { remaining } => format!(
                "{} counts one matchmaker's ack for the floor. {remaining} more must ack before \
                 the floor is in force.",
                who(id)
            ),
            GcStep::Effective { watermark, retired } => format!(
                "The floor {} is in force: a matchmaker quorum wrote it down. Every later \
                 campaign asks a quorum that shares a matchmaker with this one, so no later \
                 history names a configuration below the floor. That releases {}.",
                show_ballot(*watermark),
                if retired.is_empty() {
                    "no acceptor".to_string()
                } else {
                    format!("the acceptors {}", show_members(retired))
                }
            ),
        };
        self.narrate(NarrationKind::Gc, text);
    }

    fn narrate_handover_reply(&mut self, id: MatchmakerId, reply: &ReconfigureReply) {
        let text = match reply {
            ReconfigureReply::Stopped { history, .. } => format!(
                "{} freezes, durably, and hands over its {}. It registers nothing for this \
                 generation ever again. It stays alive: it still votes in the decree, and it \
                 still points a late candidate at the successor.",
                which(id),
                many(history.len(), "registration")
            ),
            ReconfigureReply::Bootstrapped { set, .. } => format!(
                "{} holds the proposed generation {} on its disk, marked pending. It does not \
                 serve it. A set can only be chosen once every member already holds the history \
                 it will be asked about.",
                which(id),
                set.generation.0
            ),
            ReconfigureReply::Promised { ballot, vote, .. } => format!(
                "{} promises {} in the one decree slot, and reports {}. This is Phase 1 of the \
                 single decree, over exactly the roles Act I used.",
                which(id),
                show_ballot(*ballot),
                match vote {
                    Some((at, members)) => format!(
                        "the vote it already holds: {} at {}",
                        show_set(members),
                        show_ballot(*at)
                    ),
                    None => "no vote at all".to_string(),
                }
            ),
            ReconfigureReply::Accepted { ballot, .. } => format!(
                "{} votes for the proposed set at {}, and writes the vote down before it answers. \
                 This is Phase 2 of the single decree.",
                which(id),
                show_ballot(*ballot)
            ),
            ReconfigureReply::Nacked { promised, .. } => format!(
                "{} refuses the decree ballot: it has promised {}.",
                which(id),
                show_ballot(*promised)
            ),
            ReconfigureReply::Learned { activated, at, .. } => {
                if *activated {
                    format!(
                        "{} activates generation {}: the bootstrap it held pending is live, and it \
                         serves matchmaking again.",
                        which(id),
                        at.0
                    )
                } else {
                    format!(
                        "{} records the successor. It stays frozen at generation {} and points \
                         late candidates at the new set.",
                        which(id),
                        at.0
                    )
                }
            }
            ReconfigureReply::Refused { current, .. } => format!(
                "{} refuses the step: it holds generation {}.",
                which(id),
                current.generation.0
            ),
        };
        self.narrate(NarrationKind::Generation, text);
    }

    fn narrate_reconfigurer_step(&mut self, id: NodeId, index: usize, step: &ReconfigurerStep) {
        let text = match step {
            ReconfigurerStep::Ignored => return,
            ReconfigurerStep::Stopped { remaining } => format!(
                "{} counts one freeze. {remaining} more before a quorum of the old generation is \
                 frozen.",
                who(id)
            ),
            ReconfigurerStep::Bootstrapped { remaining } => format!(
                "{} counts one member that holds the proposed set. {remaining} more must hold it: \
                 every member, not a quorum.",
                who(id)
            ),
            ReconfigurerStep::Deciding { ballot } => format!(
                "Every proposed member holds the bootstrap, so the decree opens at ballot {}. The \
                 acceptors of this decree are the matchmakers of the old generation, and the \
                 value is the new set. It is single-decree Paxos over one slot.",
                show_ballot(*ballot)
            ),
            ReconfigurerStep::Promised { remaining } => format!(
                "{} counts one decree promise. {remaining} more before Phase 1 of the decree is \
                 complete.",
                who(id)
            ),
            ReconfigurerStep::Proposing {
                ballot,
                members,
                adopted,
            } => format!(
                "Phase 1 of the decree is complete at {}. {} proposes {}{}",
                show_ballot(*ballot),
                who(id),
                show_set(members),
                if *adopted {
                    ". A matchmaker reported a set it had already voted for, so the value \
                     selection rule takes that set instead. Two operators cannot install two \
                     successors."
                } else {
                    ". No matchmaker reported an earlier vote, so this campaign proposes its own \
                     set."
                }
            ),
            ReconfigurerStep::Accepted { remaining } => format!(
                "{} counts one decree vote. {remaining} more before the set is chosen.",
                who(id)
            ),
            ReconfigurerStep::Chosen { successor } => format!(
                "Generation {} = {} is chosen: a quorum of the old generation voted for it at one \
                 ballot. From here there is exactly one successor, whichever node finishes the \
                 handover.",
                successor.generation.0,
                show_set(successor.members())
            ),
            ReconfigurerStep::Published {
                old_remaining,
                new_remaining,
            } => format!(
                "{} tells both generations. The old set still needs {old_remaining}, the new set \
                 {new_remaining}.",
                who(id)
            ),
            ReconfigurerStep::Done { successor } => {
                self.adopt_set(index, successor);
                format!(
                    "The handover is complete. Generation {} = {} serves matchmaking, and the \
                     members it replaced point every late candidate at it.",
                    successor.generation.0,
                    show_set(successor.members())
                )
            }
            ReconfigurerStep::Preempted { promised, ballot } => format!(
                "Another handover promised {} first, so this decree reopens at {}.",
                show_ballot(*promised),
                show_ballot(*ballot)
            ),
            ReconfigurerStep::Superseded { successor } => {
                self.adopt_set(index, successor);
                format!(
                    "This generation already has a successor: generation {} = {}. {} gives up and \
                     adopts it.",
                    successor.generation.0,
                    show_set(successor.members()),
                    who(id)
                )
            }
        };
        self.narrate(NarrationKind::Generation, text);
    }

    /// The driver's own half of a completed handover: the node that drove it
    /// adopts the set it published, exactly as `paros::run_node` does.
    fn adopt_set(&mut self, index: usize, set: &MatchmakerSet) {
        let id = self.pool[index];
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.learn_matchmakers(set);
            }
            world.pump(id);
        });
    }
}

/// How a party renders on the wire.
impl Party {
    /// The number this party's id carries.
    #[must_use]
    pub fn number(self) -> u64 {
        match self {
            Party::Node(id) => id.0,
            Party::Matchmaker(id) => id.0,
        }
    }

    /// Which tier it belongs to.
    #[must_use]
    pub fn party_view(self) -> PartyView {
        match self {
            Party::Node(_) => PartyView::Node,
            Party::Matchmaker(_) => PartyView::Matchmaker,
        }
    }

    /// The node this party names, if it is one.
    #[must_use]
    pub fn node(self) -> Option<NodeId> {
        match self {
            Party::Node(id) => Some(id),
            Party::Matchmaker(_) => None,
        }
    }
}

/// The prompt kinds the matchmaker plane raises, and the clones they are judged
/// on.
impl World {
    /// The question a registration raises at a matchmaker whose generation is
    /// not the one the request addresses: serve it, or refuse it?
    ///
    /// Judged on a **clone of the matchmaker**: the clone is stepped with this
    /// very request and its own reply is read back, so the answer is the
    /// registry's, not a rule restated here.
    fn generation_fence_prompt(
        &mut self,
        to: MatchmakerId,
        index: usize,
        request: &MatchRequest,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::GenerationFence) {
            return None;
        }
        let role = self.matchmakers[index].role()?;
        let held = role.set().generation;
        let phase = role.phase();
        let mut clone = role.clone();
        clone.step(request.clone());
        let ready = clone.ready();
        let outcome = ready.replies().first().map(|reply| reply.outcome.clone());
        ready.advance();
        let refused = match outcome? {
            MatchOutcome::Registered { .. } => None,
            MatchOutcome::Refused(refusal) => Some(refusal),
        };
        let id = self.take_prompt_id();
        Some(Prompt::generation_fence(
            id,
            to,
            request.ballot,
            request.generation.0,
            held.0,
            phase,
            refused.as_ref(),
        ))
    }

    /// The question a matchmaker quorum's answer raises at a candidate whose
    /// belief is out of date: abandon the campaign, or carry on?
    ///
    /// Judged on a **clone of the candidate's own matchmaking phase**. The
    /// world drives a second [`paros_core::Matchmaking`] with the same
    /// answers the node is given — the role is the core's, but `ColocatedNode`
    /// hands no reference to its own, so this is a shadow rather than a clone
    /// of the node's. See the crate's report: it is the one prompt whose oracle
    /// is not read straight off the node.
    fn stale_configuration_prompt(
        &mut self,
        to: NodeId,
        index: usize,
        reply: &MatchReply,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::StaleConfiguration) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        let matchmakers = node.matchmaker_set()?.clone();
        let (ballot, believed, kind) = node
            .matchmaking()
            .map(|(ballot, config, kind)| (ballot, config.clone(), kind))?;
        if reply.ballot != ballot || kind == RegistrationKind::Reconfiguration {
            return None;
        }
        let shadow = self.matchmaking_shadow[index].as_mut()?;
        let mut clone = shadow.clone();
        let (matchmaker, answer) =
            paros_core::matchmaking::RegisteredPage::from_reply(reply.clone());
        let Ok(page) = answer else {
            return None;
        };
        if clone.fold(matchmaker, page) != paros_core::matchmaking::MatchFold::Registered {
            return None;
        }
        if !clone.quorum_held(&matchmakers) {
            return None;
        }
        let effective = clone.stale_belief();
        let id = self.take_prompt_id();
        Some(Prompt::stale_configuration(
            id,
            to,
            ballot,
            believed.members(),
            effective
                .as_ref()
                .map(|(at, config)| (*at, config.members())),
        ))
    }

    /// The question an operator's retire request raises at the node it names.
    ///
    /// Judged by [`ColocatedNode::may_retire`] on the target itself: the call
    /// takes `&self` and changes nothing, so there is nothing to clone.
    fn may_retire_prompt(
        &mut self,
        target: NodeId,
        index: usize,
        watermark: Ballot,
        effective: Option<Ballot>,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::MayRetire) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        let member = node.is_acceptor();
        let leader = node.is_leader();
        let may = node.may_retire(watermark);
        let id = self.take_prompt_id();
        Some(Prompt::may_retire(
            id, target, watermark, effective, member, leader, may,
        ))
    }
}
