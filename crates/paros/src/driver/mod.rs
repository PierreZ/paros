//! The provider-generic node driver — the `Node` layer that owns the sans-IO
//! [`paros_core::ColocatedNode`] and performs all I/O.
//!
//! Written once over moonpool's `P: Providers` abstraction, so the *same* loop
//! runs in production (`TokioProviders`) and deterministic simulation
//! (`SimProviders`). The sim harness (`paros-sim`) adapts a moonpool `Process`
//! to it; a future `parosd` binary will adapt a tokio `main`.
//!
//! The loop `select`s over {client request, peer message, tick timer, shutdown},
//! feeds the core via `step`/`tick`, and drains every [`paros_core::Ready`] in
//! persist → send → learn → advance order (durable-before-send). It also draws
//! the randomized election timeout from the provider RNG (the core stays
//! dependency-free) and holds each journal call's reply until its slot
//! applies (#204: answered with the journal state machine's verdict),
//! redirecting non-leader proposals.
//!
//! The submodules, one concern each:
//!
//! - [`config`] — the per-node tunables, the constants they default to, the
//!   address parser, and [`RunError`].
//! - [`edge`] — the inbound RPC edge every driver serves from: the
//!   moonpool-rpc runtime the loop polls, and each role's typed inboxes.
//! - [`events`] — the pure helpers that turn a domain value into the stable
//!   field a trace carries.
//! - [`transport`] — the bounded, lossy, keep-newest per-peer mailboxes, the
//!   `Outbound` send handle, and the detached peer-delivery task.
//! - [`calls`] — the journal calls held until their slot applies (#204).
//! - [`log_reads`] — the journal `Read`: its quorum read and its long-poll
//!   (#204).
//! - [`ready`] — the `Ready` handshake's durability pipeline and the held
//!   client replies it answers.
//! - [`reply`] — the one client-reply seam (the drop and duplicate
//!   locations, each drawn exactly once per reply) every driver answers through.
//! - [`matchmaking`] — the matchmaker links, the requests a drained batch hands
//!   the loop, and the reports of what each answer did.
//! - [`handover`] — the driver-side policy around the matchmaker-set handover.
//! - [`operator`] — the operator RPCs answered from the core: compaction,
//!   acceptor-set reconfiguration, retirement and inspection.
//! - [`boot`] — the format-marker check and the (re)boot report.
//! - [`report`] — the post-batch upkeep and its cross-batch delta trackers.
//!
//! `mod.rs` itself holds only [`run_node`], the select loop that wires them,
//! and the per-arm steps that loop shares (`NodeLoop`).

pub(crate) mod boot;
mod calls;
mod config;
pub(crate) mod edge;
pub(crate) mod events;
mod handover;
mod journals;
pub(crate) mod log_reads;
mod matchmaking;
pub(crate) mod operator;
pub(crate) mod ready;
pub(crate) mod reply;
mod report;
mod system;
pub(crate) mod transport;
mod tunables;

pub use config::{BootKind, BootRefusal, DriverTunables, RunError, parse_addr};
pub use events::{command_hash, message_kind, registration_history_hash};
pub use journals::JournalStores;
pub use system::SystemPlan;
pub use tunables::BelowFloor;

use std::collections::{BTreeMap, BTreeSet};

use moonpool_core::{Providers, RandomProvider, SimulationError, SimulationResult, TimeProvider};
use paros_core::{
    ColocatedNode, Control, Delegation, Entry, GcAck, JournalId, JournalIdentifier, MatchRefusal,
    MatchReply, MatchStep, MatchmakerGeneration, MatchmakerId, MatchmakerSet, Message, NodeId,
    NodeRole, Party, ProposeResult, ProxyId, QuorumSystem, ReconfigureReply, ReconfigureRequest,
    ReconfigurerPhase, ReconfigurerStep, Seq, TenantId, Value,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::machine::{ControlJournals, FormedCell};
use crate::rpc::{
    InspectRefusal, InspectTarget, MatchmakerClient, MatchmakersRefusal, ReadAck,
    ReconfigureMatchmakersAck, ReplySender, SetLeaderAck, TruncateAck, WriteAck, leader_from_proto,
    leader_uuid_from_proto,
};
use crate::storage::LogStorage;
use crate::system::{DirectoryEvent, NodeStanding, RegistryEvent, SystemEvent};
use reply::Reply;

use calls::Call;
use edge::{NodeInbox, RpcEdge, edge_reporter};
use events::message_route;
use handover::HandoverDriver;
use journals::{JournalRt, Journals, SingleStore, boot_journal};
use matchmaking::{
    MatchmakerLinks, folded_answer, report_match_step, send_outbox, send_reconfigure_requests,
    surface_matchmaking,
};
use ready::{ClientWaiters, drain_ready, fold_head};
use reply::maybe_duplicate;
use report::{Deltas, HandoffContext, handoff_context, maintain};
use system::{Followed, SystemFollower, follow_local};
use transport::{LaneOpener, Outbound, peer_address};

/// The node loop's fixed context: the handles every arm's **settle tail** needs
/// and none of them change across an incarnation. Bundled so the tail is one
/// call instead of four repeated at every arm, and so the steps the arms share
/// take one handle.
struct NodeLoop<'a, P: Providers, A: Audit> {
    providers: &'a P,
    links: &'a MatchmakerLinks<P>,
    out: &'a Outbound,
    audit: &'a A,
    self_id: u64,
    tunables: DriverTunables,
}

impl<P: Providers, A: Audit> NodeLoop<'_, P, A> {
    /// The **settle tail**: every arm that feeds the core ends here, in this
    /// order — drain the `Ready` batch (persist → send → apply), surface a
    /// matchmaking phase it opened *before* the requests leave, put the
    /// batch's matchmaker-wire requests on the wire, then run the post-batch
    /// upkeep. The order is the contract; the arms differ only in what they
    /// fed the core beforehand.
    ///
    /// # Errors
    ///
    /// Propagates the drain's typed exit ([`RunError`]): a durability-seam
    /// crash or a storage fault the driver decided to crash on.
    async fn settle<S: LogStorage>(
        &self,
        node: &mut ColocatedNode,
        storage: &mut S,
        waiters: &mut ClientWaiters,
        last: &mut Deltas,
    ) -> Result<(), RunError> {
        assert!(
            node.config().id.0 == self.self_id,
            "a node loop settles its own node"
        );
        let promised = node.hard_state().max_promised_ballot;
        let chosen = node.hard_state().chosen_index;
        let mut outbox = drain_ready(node, storage, self.out, waiters, self.audit).await?;
        if !outbox.gc_requests.is_empty() && crate::scenario::withhold_gc() {
            tracing::info!(node = self.self_id, "gc_requests_withheld");
            outbox.gc_requests.clear();
        }
        surface_matchmaking(node, &mut last.matchmaking, self.audit, self.self_id);
        send_outbox(self.providers, self.links, self.audit, self.self_id, outbox);
        maintain(
            node,
            self.providers,
            last,
            waiters,
            self.self_id,
            (
                self.tunables.election_timeout_base,
                self.tunables.election_backoff_doublings,
            ),
            self.audit,
        );
        // The journal fold only grows inside a batch: a journal read
        // long-polling at the tail is re-served here (#204).
        waiters.reads.wake(
            |from, limit, bytes| node.read_log(from, limit, bytes),
            NodeId(self.self_id),
            self.audit,
        );
        // The settle tail persists and applies; it never lowers what it made
        // durable (the core's own marks, restated at the I/O seam).
        assert!(
            node.hard_state().max_promised_ballot >= promised,
            "a settled batch never lowers the promise"
        );
        assert!(
            node.hard_state().chosen_index >= chosen,
            "a settled batch never rewinds the chosen index"
        );
        Ok(())
    }

    /// Answer one client-facing reply through the reply seam
    /// ([`reply::answer`]).
    fn answer<T>(&self, kind: Reply, waiter: ReplySender<T>, ack: T) {
        reply::answer(self.audit, NodeId(self.self_id), kind, waiter, ack);
    }

    /// Put matchmaker-set handover requests on the matchmaker wire.
    fn send_reconfigure(&self, requests: Vec<(MatchmakerId, ReconfigureRequest)>) {
        assert!(
            requests.iter().all(|(_, r)| r.from().0 == self.self_id),
            "a handover request leaves in this node's name"
        );
        send_reconfigure_requests(
            self.providers,
            self.links,
            self.audit,
            self.self_id,
            requests,
        );
    }

    /// Report a matchmaker-set handover this node just started (or is
    /// finishing, `finishing`) from `current` toward `target`, then put its
    /// first requests on the matchmaker wire — in that order.
    fn start_reconfigurer(
        &self,
        handover: &mut HandoverDriver,
        current: &MatchmakerSet,
        target: &[MatchmakerId],
        finishing: bool,
    ) {
        assert!(handover.is_busy(), "a started handover is running");
        assert!(!target.is_empty(), "a handover targets a non-empty set");
        self.audit
            .reconfigurer_started(NodeId(self.self_id), current, target);
        tracing::info!(
            node = self.self_id,
            generation = current.generation.0,
            target = target.len() as u64,
            finishing,
            "reconfigurer_started"
        );
        self.send_reconfigure(handover.take_requests());
    }

    /// The two straggler paths of a handover (#125), taken by whichever node
    /// meets them in a matchmaker's refusal: a registry frozen with no
    /// successor is finished by this node (the reconfigurer's decree adopts
    /// whatever was voted, or re-chooses the same members under a fresh
    /// generation); a member left inactive or behind is told the chosen set
    /// this node already knows.
    fn on_match_refusal(
        &self,
        node: &ColocatedNode,
        handover: &mut HandoverDriver,
        matchmaker: MatchmakerId,
        step: &MatchStep,
    ) {
        let (audit, self_id) = (self.audit, self.self_id);
        match step {
            // Sound to finish *this* node's believed set: a matchmaker
            // answers `Stopped { successor: None }` only when the generation
            // it froze is the one the request named
            // (`Matchmaker::generation_refusal` answers a mismatch with
            // `Generation { current }` or `Inactive` instead), so the
            // generation this node is finishing is exactly the one it
            // believes in force.
            MatchStep::Refused(MatchRefusal::Stopped { successor: None })
                if !handover.is_busy() =>
            {
                if let Some(current) = node.matchmaker_set().cloned()
                    && handover.finish(&current).is_ok()
                {
                    self.start_reconfigurer(handover, &current, current.members(), true);
                }
            }
            MatchStep::Refused(MatchRefusal::Inactive | MatchRefusal::Generation { .. }) => {
                // A refusal is only ever folded on a matchmaker deployment (a
                // plain node ignores every reply), so the believed set is
                // there to republish.
                let set = node.matchmaker_set().cloned();
                let behind = match (step, &set) {
                    (MatchStep::Refused(MatchRefusal::Generation { current }), Some(set)) => {
                        current.generation < set.generation
                    }
                    (_, Some(_)) => true,
                    (_, None) => false,
                };
                if let Some(set) = set
                    && behind
                    && set.generation.0 > 0
                {
                    // A republished successor is a generation this node
                    // learned on a matchmaker deployment, never the bootstrap.
                    assert!(
                        node.config().has_matchmakers(),
                        "only a matchmaker deployment republishes a successor"
                    );
                    audit.successor_republished(NodeId(self_id), matchmaker, &set);
                    tracing::info!(
                        node = self_id,
                        matchmaker = matchmaker.0,
                        generation = set.generation.0,
                        "successor_republished"
                    );
                    let request = ReconfigureRequest::Chosen {
                        from: NodeId(self_id),
                        generation: MatchmakerGeneration(set.generation.0 - 1),
                        successor: set,
                    };
                    self.send_reconfigure(vec![(matchmaker, request)]);
                }
            }
            _ => {}
        }
    }

    /// One tick of the running matchmaker-set handover (#125): its stall
    /// clock, the two ways it is given up, and its re-send — its own cadence
    /// and its own location; a preempted decree reopens only here, so two
    /// dueling reconfigurers are paced by their drivers.
    fn pace_handover(&self, node: &ColocatedNode, handover: &mut HandoverDriver) {
        assert!(
            node.config().has_matchmakers(),
            "a handover is paced only on a matchmaker deployment"
        );
        let (audit, self_id) = (self.audit, self.self_id);
        handover.tick();
        let stall_budget = node
            .election_timeout()
            .saturating_mul(self.tunables.reconfigure_timeout_elections);
        if handover.is_busy()
            && stall_budget != 0
            && handover.stalled_for() >= stall_budget
            && handover.abandon()
        {
            // A phase that no member answers any more (a lost registry, a
            // machine gone) is abandoned: the frozen generation is finished
            // by the next node to meet it, with the members that do answer.
            // Its own gate: the early abandon below reports through the same
            // audit port, so without it this path could be dead unseen.
            moonpool_assertions::reachable!("generation: the stall budget abandons a handover");
            audit.reconfigurer_aborted(NodeId(self_id));
            tracing::info!(node = self_id, "reconfigurer_aborted");
        }
        // The same decision taken early, on the node loop: giving up a
        // handover is always safe (the reconfigurer holds no durable state
        // and the freeze, the bootstrap and the votes are all idempotent or
        // durable elsewhere), and it is what puts a *second* reconfigurer on
        // a half-replaced generation.
        if handover.is_busy() && abandon_early(handover.phase()) && handover.abandon() {
            audit.reconfigurer_aborted(NodeId(self_id));
            tracing::info!(node = self_id, early = true, "reconfigurer_aborted");
        }
        if !handover.resend_due(self.tunables.reconfigurer_resend_ticks) {
            return;
        }
        // The freeze closes here, not on the ack that completed its quorum: a
        // quorum is the floor the reconstruction rests on, and every
        // straggler that answered since widens it — and, for a `finish`, the
        // successor set it proposes (review finding P5).
        if let Some((generation, reconstruction)) = handover.close_stop() {
            let bootstrap = &reconstruction.bootstrap;
            audit.reconfigurer_reconstructed(
                NodeId(self_id),
                generation.0,
                bootstrap,
                reconstruction.disagreements,
            );
            tracing::info!(
                node = self_id,
                generation = generation.0,
                members = bootstrap.set.members().len() as u64,
                registrations = bootstrap.history.len() as u64,
                watermark_round = bootstrap.gc_watermark.round,
                disagreements = reconstruction.disagreements,
                "reconfigurer_reconstructed"
            );
            self.send_reconfigure(handover.take_requests());
        }
        // Skipping a due re-send is always safe: it stretches the
        // stop-the-world window and delays a preempted decree's reopening.
        // Silent in the recovery tail.
        if moonpool_buggify::buggify_fault_with_prob!(0.5) {
            moonpool_assertions::reachable!("generation: the driver skips a due handover re-send");
            tracing::info!(node = self_id, "reconfigurer_resend_skipped");
        } else {
            handover.resend();
            self.send_reconfigure(handover.take_requests());
        }
    }

    /// Give up the leadership, or not, on this tick. Cooperative leader
    /// handoff (`DPaxos`) first: move the existing Phase-2 authority to
    /// another physical node instead of letting an election destroy it and
    /// make the successor rediscover the log through Phase 1. Consulted only
    /// when the core says the leadership is transferable, so a `true` always
    /// has an effect; answering `false` is always safe (a handoff is an
    /// optimization, never a requirement). Offered *before* the resignation
    /// hook: both give up the leadership, and the cooperative one is strictly
    /// the more interesting outcome.
    fn offer_handoff(&self, node: &mut ColocatedNode) {
        let (audit, self_id) = (self.audit, self.self_id);
        let mut handed_off = false;
        if node.can_relinquish() {
            let candidates = node.handoff_candidates();
            if !candidates.is_empty() && initiate_handoff(&handoff_context(node)) {
                let fallback = self.providers.random().random_range(0..candidates.len());
                // Target selection is its own location: it pins the
                // lowest-id candidate instead of the uniform pick, so a
                // seed can concentrate repeated handoffs on one successor
                // (A -> B -> A -> B). Every candidate is valid: the
                // successor checks the transfer itself.
                let target = if moonpool_buggify::buggify_fault_with_prob!(0.5) {
                    moonpool_assertions::reachable!(
                        "a handoff target is chosen by the pinning selector"
                    );
                    candidates[0]
                } else {
                    candidates[fallback]
                };
                if let Some(handoff) = node.relinquish_to(target) {
                    handed_off = true;
                    audit.authority_relinquished(NodeId(self_id), handoff);
                    tracing::info!(
                        node = self_id,
                        to = handoff.to.0,
                        round = handoff.ballot.round,
                        bnode = handoff.ballot.node.0,
                        next_slot = handoff.next_slot.0,
                        decided = handoff.decided,
                        pending = handoff.pending,
                        "authority_relinquished"
                    );
                }
            }
        }
        // A sitting leader resigns on its own, rarely: a rare-but-valid
        // choice (`step_down` is always safe) that makes the cell elect
        // again. Silent in the recovery tail.
        if !handed_off
            && node.role() == NodeRole::Leader
            && moonpool_buggify::buggify_fault_with_prob!(0.004)
        {
            moonpool_assertions::reachable!("the driver voluntarily resigns leadership");
            audit.stepped_down(NodeId(self_id));
            tracing::info!(node = self_id, "leadership_resigned");
            node.step_down();
        }
        // Either way the authority is given up in the same call.
        if handed_off {
            assert!(
                node.role() == NodeRole::Follower,
                "a relinquished authority demotes"
            );
            assert!(!node.can_relinquish(), "an authority is relinquished once");
        }
    }
}

/// What every journal's steps share on this node: the handles a
/// [`NodeLoop`] needs apart from the journal's own audit port.
struct Shared<'a, P: Providers> {
    providers: &'a P,
    links: &'a MatchmakerLinks<P>,
    out: &'a Outbound,
    self_id: u64,
    tunables: DriverTunables,
}

impl<P: Providers> Shared<'_, P> {
    /// The node loop's steps, reporting to `audit` (a journal's port).
    fn with<'b, A: Audit>(&'b self, audit: &'b A) -> NodeLoop<'b, P, A> {
        NodeLoop {
            providers: self.providers,
            links: self.links,
            out: self.out,
            audit,
            self_id: self.self_id,
            tunables: self.tunables,
        }
    }

    /// The settle tail ([`NodeLoop::settle`]) for one journal.
    ///
    /// # Errors
    ///
    /// As [`NodeLoop::settle`].
    async fn settle<S: LogStorage, A: Audit>(
        &self,
        rt: &mut JournalRt<S, A>,
    ) -> Result<(), RunError> {
        let JournalRt {
            node,
            storage,
            audit,
            waiters,
            last,
            ..
        } = rt;
        self.with(audit).settle(node, storage, waiters, last).await
    }

    /// One journal's beat: the core's tick, the re-sends and their BUGGIFY
    /// sites, the
    /// handover's pacing (the one journal of a matchmaker deployment), the
    /// leadership's give-up, the read expiries, the settle tail, and the
    /// stranded-slot report.
    ///
    /// # Errors
    ///
    /// As [`NodeLoop::settle`].
    async fn beat<S: LogStorage, A: Audit>(
        &self,
        rt: &mut JournalRt<S, A>,
        handover: &mut HandoverDriver,
        ticks: u64,
    ) -> Result<(), RunError> {
        let (self_id, tunables) = (self.self_id, self.tunables);
        let JournalRt {
            node,
            storage,
            audit,
            waiters,
            last,
            match_resend,
            gc_resend,
            ..
        } = rt;
        assert!(
            node.config().id.0 == self_id,
            "a beat drives this node's own journal"
        );
        let lp = self.with(&*audit);
        node.tick();
        // Consult each hook and each inline BUGGIFY site only when its
        // decision can have an effect. Both are inert in production.
        if node.has_pending_accepts() {
            // Skipping the re-send is always safe: it is a pure optimization.
            // Silent in the recovery tail, so the tail re-sends honestly.
            if moonpool_buggify::buggify_fault_with_prob!(0.95) {
                moonpool_assertions::reachable!("the driver skips a pending accept re-send");
                tracing::info!(node = self_id, "accept_resend_skipped");
            } else {
                node.resend_pending();
            }
            // Liveness under a dead proxy (#142): a delegated round
            // re-delegated the budget's worth of beats without its `Commit`
            // is taken back and run colocated. The budget is driver policy
            // (`proxy_take_back_resends`, born buggified); the core only
            // counts. A no-op on a deployment without proxies.
            for (slot, proxy) in node.take_back_delegated(tunables.proxy_take_back_resends) {
                audit.delegation_taken_back(NodeId(self_id), slot, proxy);
                tracing::info!(
                    node = self_id,
                    slot = slot.0,
                    proxy = proxy.0,
                    "delegation_taken_back"
                );
            }
        }
        // The open matchmaking request's re-send (#120): paced by
        // `match_resend_ticks`, and its own BUGGIFY location — consulted only
        // when a re-send is due, so a skip always costs a beat. Generous: a
        // skipped beat only stretches an open campaign, and the state worth
        // reaching is the campaign the election timeout abandons
        // mid-matchmaking.
        if match_resend.tick_if(node.matchmaking_pending(), tunables.match_resend_ticks) {
            if moonpool_buggify::buggify_fault_with_prob!(0.5) {
                moonpool_assertions::reachable!(
                    "matchmaking: the driver skips a due matchmaking re-send"
                );
                tracing::info!(node = self_id, "matchmaking_resend_skipped");
            } else {
                node.resend_matchmaking();
            }
        }
        // The open GC request's re-send (#123): its own cadence
        // (`gc_resend_ticks`) and its own BUGGIFY location.
        if gc_resend.tick_if(node.gc_pending(), tunables.gc_resend_ticks) {
            if crate::scenario::withhold_gc() {
                tracing::info!(node = self_id, "gc_requests_withheld");
            } else if moonpool_buggify::buggify_fault_with_prob!(0.5) {
                // Skipping a due re-send is always safe: a skipped beat
                // stretches the window in which a leader deposed before its
                // quorum acks leaves the floor un-raised.
                moonpool_assertions::reachable!(
                    "gc: the driver skips a due garbage-collection re-send"
                );
                tracing::info!(node = self_id, "gc_resend_skipped");
            } else {
                node.resend_gc();
            }
        }
        // The handover belongs to the matchmaker plane, which serves a
        // deployment's one journal; on a plain deployment it stays idle.
        if node.config().has_matchmakers() {
            lp.pace_handover(node, handover);
        }
        lp.offer_handoff(node);
        // Expire the reads whose quorum read is overdue (a row that never
        // answered whole, a fold that has not reached the watermark) and
        // answer the long-polls whose wait ran out. The early expiry, drawn
        // only while a read waits on its confirmation, takes the first exit
        // before the deadline. Kept shy: expiring most parked reads early
        // would stop confirmed reads from ever completing during the chaos
        // window. Silent in the recovery tail; its gate is the audit's
        // `read_expired` (the `early` leg).
        let expire_all =
            waiters.reads.has_confirming() && moonpool_buggify::buggify_fault_with_prob!(0.05);
        waiters.reads.expire(
            |from, limit, bytes| node.read_log(from, limit, bytes),
            ticks,
            tunables.read_retry_ticks,
            expire_all,
            NodeId(self_id),
            &*audit,
        );
        lp.settle(node, storage, waiters, last).await?;
        // Surface a chosen slot stranded above the applied prefix. The
        // `Ready` handshake only ever hands out the *contiguous* prefix, so a
        // hole below a chosen slot is otherwise invisible from outside the
        // core. Re-emitted every tick while it lasts: the oracle reads its
        // persistence past quiescence, not a single instant.
        if let Some((hole, above)) = node.replica().chosen_gap() {
            audit.chosen_gap(NodeId(self_id), hole, above);
            tracing::info!(node = self_id, hole = hole.0, above = above.0, "chosen_gap");
        }
        audit.ticked(NodeId(self_id));
        Ok(())
    }
}

/// Surface a peer message's arrival (mirror of `msg_sent`), so a human reading
/// the trace can pair sends with receives and spot the unmatched ones as
/// network drops.
fn trace_received(self_id: u64, msg: &Message) {
    let kind = message_kind(msg);
    match message_route(msg) {
        Some((from, ballot, Some(slot))) => tracing::info!(
            node = self_id,
            from = %from,
            kind,
            bround = ballot.round,
            bnode = ballot.node.0,
            slot = slot.0,
            "msg_received"
        ),
        // The empty-prefix beat: no slot field, mirroring `msg_sent`.
        Some((from, ballot, None)) => tracing::info!(
            node = self_id,
            from = %from,
            kind,
            bround = ballot.round,
            bnode = ballot.node.0,
            "msg_received"
        ),
        None => tracing::info!(node = self_id, kind, "msg_received"),
    }
}

/// Whether to give up the running handover early, on the node loop. One
/// location per phase, one macro line per arm: a seed that abandons freezes
/// readily must be able to leave decrees alone. `Publishing` is absent: the
/// successor is already chosen there, so giving up loses nothing a
/// straggler's republication does not already cover.
///
/// Always safe: the reconfigurer holds no durable state, and the freeze, the
/// bootstrap and the votes are idempotent or durable elsewhere. It puts a
/// second reconfigurer on a half-replaced generation. Silent in the recovery
/// tail.
fn abandon_early(phase: &ReconfigurerPhase) -> bool {
    let fired = match phase {
        ReconfigurerPhase::Stopping { .. } => moonpool_buggify::buggify_fault_with_prob!(0.02),
        ReconfigurerPhase::Bootstrapping { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.02)
        }
        ReconfigurerPhase::Deciding { .. } => moonpool_buggify::buggify_fault_with_prob!(0.02),
        ReconfigurerPhase::Idle | ReconfigurerPhase::Publishing { .. } => false,
    };
    if fired {
        // BUGGIFY pairing, one gate per location.
        match phase {
            ReconfigurerPhase::Stopping { .. } => moonpool_assertions::reachable!(
                "generation: a handover is abandoned while freezing"
            ),
            ReconfigurerPhase::Bootstrapping { .. } => moonpool_assertions::reachable!(
                "generation: a handover is abandoned while bootstrapping"
            ),
            ReconfigurerPhase::Deciding { .. } => {
                moonpool_assertions::reachable!("generation: a handover is abandoned mid-decree");
            }
            ReconfigurerPhase::Idle | ReconfigurerPhase::Publishing { .. } => {}
        }
    }
    fired
}

/// Whether a transferable leadership hands itself off on this tick. One
/// location per shape of transfer, biased toward the hard states: a handoff
/// that carries unfinished business (an accepted-but-unchosen tail, or a
/// leader still healing a hole) fires an order of magnitude more often than
/// the clean one, which stays armed because it is the common production
/// shape.
///
/// The rates stay near the resignation site's (0.004), and that ceiling
/// matters. A handoff replaces an election, so a high rate makes it the
/// main way leadership moves and starves every campaign that needs a
/// settled cell. `healing` is true for any leader that holds a pipelined
/// slot decided out of order, an ordinary streaming state: at 0.30 it moved
/// leadership every few ticks. Always safe: a failed handoff costs
/// availability, never safety. Silent in the recovery tail.
fn initiate_handoff(ctx: &HandoffContext) -> bool {
    let fired = if ctx.healing {
        moonpool_buggify::buggify_fault_with_prob!(0.03)
    } else if !ctx.settled {
        moonpool_buggify::buggify_fault_with_prob!(0.02)
    } else {
        moonpool_buggify::buggify_fault_with_prob!(0.002)
    };
    if fired {
        // BUGGIFY pairing: each shape fires on some seed, split so that
        // saturation cannot hide one shape behind another's samples.
        if ctx.healing {
            moonpool_assertions::reachable!(
                "a handoff leaves a leader that is still healing a hole"
            );
        } else if ctx.settled {
            moonpool_assertions::reachable!("a handoff leaves a fully settled leader");
        } else {
            moonpool_assertions::reachable!("a handoff carries an accepted-but-unchosen tail");
        }
    }
    fired
}

/// The driver's delegation choice for the proposal about to open (#142):
/// drawn only where it can have an effect (this node leads a deployment
/// with proxies), on the node loop. First "run it colocated?", then "which
/// proxy?", each its own location; [`Delegation::Auto`] otherwise, which is
/// the core's own rule.
fn delegation_choice(node: &ColocatedNode) -> Delegation {
    let count = node.config().proxy_count;
    if count == 0 || !node.is_leader() {
        return Delegation::Auto;
    }
    // The round runs colocated instead, so a proxied deployment's log mixes
    // proxied and colocated slots and the two Phase-2 paths interleave in
    // one leadership. Shy, so most slots still go through a proxy.
    if moonpool_buggify::buggify_fault_with_prob!(0.10) {
        moonpool_assertions::reachable!(
            "proxy: the driver runs a round colocated on a proxied deployment"
        );
        return Delegation::Colocated;
    }
    // The next proxy the modulus would not pick, `(slot + 1) % count`: so
    // consecutive slots land on one proxy, and a handoff successor's
    // re-delegation (which derives `slot % count` afresh) lands on another
    // proxy. Two proxies then fan out the same round: the P2b idempotency
    // the delegation rests on.
    let choice = if count >= 2 && moonpool_buggify::buggify_fault_with_prob!(0.10) {
        moonpool_assertions::reachable!("proxy: the driver overrides a round's proxy");
        let modulus = u64::try_from(count).unwrap_or(u64::MAX);
        Delegation::To(ProxyId(
            node.proposer().next_slot().0.wrapping_add(1) % modulus,
        ))
    } else {
        Delegation::Auto
    };
    // A named proxy is one the deployment has, chosen by its leader.
    if let Delegation::To(proxy) = choice {
        assert!(proxy.is_in(count), "a delegation names a deployed proxy");
        assert!(node.is_leader(), "only a leader delegates");
    }
    choice
}

/// The column of a grid this proposal's Phase 2 goes to instead of the
/// core's `slot % cols` (#141): `(slot + 1) % cols`, the next slot's column,
/// so consecutive slots land on one column and a handoff successor's
/// re-proposal lands on another. Every column is a Phase-2 quorum, so the
/// choice is always valid. Its own location; silent in the recovery tail.
fn phase2_column(slot: paros_core::Slot, cols: usize) -> Option<usize> {
    if cols < 2 || !moonpool_buggify::buggify_fault_with_prob!(0.10) {
        return None;
    }
    // BUGGIFY pairing: the override genuinely fires.
    moonpool_assertions::reachable!("grid: the driver overrides a round's column");
    let modulus = u64::try_from(cols).unwrap_or(u64::MAX);
    let column = usize::try_from(slot.0.wrapping_add(1) % modulus).unwrap_or(0);
    assert!(column < cols, "an overridden column is one the grid has");
    Some(column)
}

/// Whether the next tick waits twice the normal interval. Always safe: every
/// timeout the core owns counts ticks, so a node that ticks slower is a slow
/// node, which the cell must tolerate. Deliberately shy: a node that
/// stretches most ticks runs its protocol clock at half speed, a stalled
/// node rather than a slow one. At this rate a node loses a handful of
/// ticks per chaos window, enough to desynchronize the protocol clocks.
/// Silent in the recovery tail.
fn stretch_tick() -> bool {
    let stretched = moonpool_buggify::buggify_fault_with_prob!(0.05);
    if stretched {
        moonpool_assertions::reachable!("a node stretches its tick interval");
    }
    stretched
}

/// Park `call` on the slot its proposal took (`result`), to be answered
/// when that slot applies, or redirect it at once when this node does not
/// lead.
fn park_call<S, A: Audit, P: Providers>(
    rt: &mut JournalRt<S, A>,
    result: ProposeResult,
    call: Call,
    shared: &Shared<'_, P>,
) {
    match result {
        // A lost redirect is a legal outcome: the client's deadline turns it
        // into a retry elsewhere.
        ProposeResult::NotLeader(hint) => {
            call.no_verdict(hint.map(|n| n.0), &rt.audit, shared.self_id);
        }
        ProposeResult::Accepted(slot) => {
            // A held call waits on a slot this leader just allocated.
            assert!(
                slot < rt.node.proposer().next_slot(),
                "a call waits on an allocated slot"
            );
            rt.waiters.pending.entry(slot).or_default().push(call);
            assert!(
                rt.waiters.pending.contains_key(&slot),
                "a held call is parked on its slot"
            );
        }
    }
}

/// Drive a paros node to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation — only the providers
/// differ) and `S: LogStorage` (the injected durable storage). The loop owns a
/// [`ColocatedNode`], serves the Paros RPC interface, feeds client proposals and
/// peer messages into the core, sends the core's outbound messages to the peers
/// named in `members`, and ticks until `shutdown` fires.
///
/// `members` is the full **node pool** (`NodeId` → address, *including* this
/// node): every node the core may ever address — the bootstrap membership on
/// plain Multi-Paxos, and every spare a reconfiguration could add on a
/// matchmaker deployment. The core addresses each outbound message by
/// `NodeId`, and the driver resolves it here. It must be consistent across
/// the cluster and agree with the `Config` the node read from `storage`.
///
/// `matchmakers` is the matchmaker set (`MatchmakerId` → address), empty on
/// plain Multi-Paxos; it must agree with the `Config`'s matchmaker set. The
/// driver speaks the matchmaker contract only when it is non-empty.
///
/// `proxies` is the deployment map's proxy leaders (`ProxyId` → address,
/// #142), empty on a deployment without proxies; its length must agree with
/// the `Config`'s `proxy_count`. A leader delegates a settled round's Phase 2
/// to the proxy the core names (or the one the driver's proxy override names),
/// and takes it back after `tunables.proxy_take_back_resends` re-delegations
/// without a `Commit`.
///
/// `replicas` is the deployment map's replica tier (#144, `NodeId` →
/// address, empty without one): learners outside the pool that are not
/// acceptors. Every message the core addresses to the learners — a
/// `Commit`, a beat — reaches them beside the pool, and a catch-up answer or
/// a trim point addressed to one reaches its lane. Its length is the
/// `Config`'s `replica_count`.
///
/// `boot` is the operator's claim about `storage` ([`BootKind`], #147): a
/// first boot formats the store (the marker, durably) before the core reads
/// it; an existing member must find the marker, or the driver refuses
/// ([`RunError::Refused`]) — an amnesiac identity never rejoins.
///
/// `tunables` is the driver's per-node transport shape ([`DriverTunables`]):
/// production passes [`DriverTunables::default()`] (the historical constants);
/// the sim harness buggifies it per seed, FDB knob style.
///
/// Every rare-but-valid choice of the driver is an inline BUGGIFY site, inert
/// in production (#294, #318); a simulation harness forces the per-seed ones
/// by name ([`crate::scenario`]).
///
/// `audit` is pure observation: the driver reports every
/// externally meaningful transition to it, and nothing it does can change the
/// run. Production passes [`NoAudit`](crate::NoAudit).
///
/// # Errors
///
/// The exit is typed ([`RunError`]): [`RunError::Storage`] when a [`LogStorage`] call failed and
/// the driver took its fail-stop crash decision — production treats it as a
/// process exit (crash-only), the sim node loop recovers by re-running
/// `run_node` against the surviving durable storage; [`RunError::Refused`] when `boot` and the
/// store's format marker disagree (nothing was written, nothing sent: the
/// identity stays down); [`RunError::Infra`] for genuine
/// provider/infrastructure failures (bind, listen), the only exit that is not
/// a deliberate crash and must propagate.
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len(), matchmakers = matchmakers.len(), proxies = proxies.len(), replicas = replicas.len()))]
// The parameters are the node's complete wiring (providers, storage,
// addressing, tunables, lifecycle, audit) — a bundle would only rename
// the same things.
#[allow(clippy::too_many_arguments)]
pub async fn run_node<P, S, A>(
    providers: P,
    storage: S,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    matchmakers: Vec<(MatchmakerId, String)>,
    proxies: Vec<(ProxyId, String)>,
    replicas: Vec<(NodeId, String)>,
    boot: BootKind,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    S: LogStorage,
    // `Clone + Send + Sync + 'static` because each peer-delivery task carries
    // its own handle to the audit: the bounded-mailbox drops happen inside
    // those tasks, and reporting them (`Audit::dropped_at_mailbox`) is part of
    // the observation contract. Pure observation still holds — the clone
    // shares the same underlying sink.
    A: Audit + Clone + Send + Sync + 'static,
{
    let config = storage.initial_state().1;
    let stores = SingleStore {
        journal: config.journal,
        store: Some((storage, boot)),
        audit: audit.clone(),
    };
    Box::pin(run_journals(
        providers,
        stores,
        config.id,
        local_addr,
        members,
        matchmakers,
        proxies,
        replicas,
        None,
        None,
        tunables,
        shutdown,
    ))
    .await
}

/// [`run_node`] over a **static list of journals** (#188): one
/// [`ColocatedNode`] per journal, each with its own store (from `stores`), its
/// own client waiters and its own audit port, sharing the process, the RPC
/// edge, the peer connections and the tick. Every peer message carries its
/// journal on the `Deliver` envelope and every client call names one; the
/// loop routes each to its journal's node, and nothing crosses journals.
///
/// A storage fault **quarantines** its journal on this node: the journal
/// sends and answers nothing until the loop re-opens it from `stores` after
/// [`DriverTunables::quarantine_ticks`], while the node keeps serving its
/// other journals. A node whose every journal is quarantined at once has
/// nothing left and exits with the fault — for one journal, exactly
/// [`run_node`]'s fail-stop crash. A seam crash is the process dying, for
/// every journal.
///
/// The matchmaker plane, the proxy leaders and the replica tier serve one
/// journal each, the node's first user journal: every other journal of a
/// node — the control journals included — runs plain Multi-Paxos
/// (journal-tagged planes are #190 and #193).
///
/// `node` is this process's node id, named by the caller (no id has a
/// default, §3.8): every journal's [`paros_core::Config`] must carry it, and
/// so must `system`'s plan.
///
/// `system` opts the node into the **system journals** (#189,
/// [`SystemPlan`]): it follows the directory and the node registry (from its
/// own the system journals on a seed, from a seed otherwise), starts a journal
/// the directory creates naming it and stops one it tombstones, opens a lane
/// to every node the registry admits, and refuses a peer message from a node
/// the registry does not have in the pool. Such a node may serve no journal
/// at all (a joiner boots with none) and exits only on a fault. `None` is
/// the static deployment: a fixed list over a fixed pool.
///
/// `formed` is a machine's cell (#246, #277), `None` off a machine: its
/// control journals, which `Inspect` reports, and its vote in the cell
/// decree, which the node keeps answering ([`FormedCell`]).
///
/// # Errors
///
/// As [`run_node`]; [`RunError::Infra`] when several journals are asked of a
/// deployment with matchmakers, proxies or replicas.
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len(), matchmakers = matchmakers.len(), proxies = proxies.len(), replicas = replicas.len()))]
// One cohesive select loop: every arm is a thin feed into the core plus the
// same drain/maintain tail; splitting arms out would only scatter the loop's
// shared state. The parameters are the node's complete wiring.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn run_journals<P, J>(
    providers: P,
    mut stores: J,
    node: NodeId,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    matchmakers: Vec<(MatchmakerId, String)>,
    proxies: Vec<(ProxyId, String)>,
    replicas: Vec<(NodeId, String)>,
    system: Option<SystemPlan>,
    formed: Option<FormedCell>,
    tunables: DriverTunables,
    shutdown: CancellationToken,
) -> Result<(), RunError>
where
    P: Providers,
    J: JournalStores,
{
    let ids = stores.journals();
    if ids.is_empty() && system.is_none() {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "a node serves at least one journal".into(),
        )));
    }
    if system.as_ref().is_some_and(|plan| plan.self_id != node) {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "the system plan names another node".into(),
        )));
    }
    let cell: Option<ControlJournals> = formed.as_ref().map(FormedCell::control_journals);
    // The cell every peer batch carries and every inbound one must name
    // (#216): `0` on a deployment without a cell plan.
    let cell_id = cell.map_or(0, |cell| cell.cell_id);
    assert!(cell.is_none() || cell_id != 0, "a cell plan names its cell");
    // No identifier is fixed (§3.8): the deployment names its control journals —
    // the cell's and the fleet tenant's from the cell plan, the system journals a
    // `SystemPlan` follows — and every other journal is a user's.
    let control: BTreeSet<JournalIdentifier> = cell
        .iter()
        .flat_map(|cell| std::iter::once(cell.cell).chain(cell.fleet))
        .chain(
            system
                .iter()
                .flat_map(|plan| [plan.directory, plan.registry]),
        )
        .filter(|journal| journal.is_set())
        .collect();
    // The node-level audit: what no single journal owns (the edge's
    // rejections, a peer lane's delivery failures, a refused journal id, the
    // system journals' folds), named by the opener rather than borrowed
    // from a journal (#243).
    let node_audit = stores.node_audit();
    // A node exits once it has nothing left to serve — on a static
    // deployment. A node that follows the system journals runs on with none
    // (a joiner waits for the directory to name it) and exits only when a
    // fault took the last one.
    let follows = system.is_some();
    // The journal this node may hold for a scenario (#188,
    // `crate::scenario::HOLD_JOURNAL`): the highest of the user journals it
    // boots serving, when there are several.
    let hold_candidate =
        crate::scenario::hold_candidate(ids.iter().filter(|id| !control.contains(id)));

    // Stage 7 per journal, before the core reads a byte: the boot scan and
    // the format marker (#147). A journal that fails to boot is quarantined
    // (or down for good on a refusal); a node with none left exits.
    let mut journals: Journals<J::Store, J::Audit> = Journals::new(control);
    for &id in &ids {
        open_journal(&providers, &mut stores, &mut journals, id, 0, &tunables).await;
    }
    for journal in journals.take_quarantined() {
        stores.quarantined(journal);
    }
    if journals.exhausted() && (!follows || journals.stranded()) {
        return journals.exit();
    }
    // The matchmaker plane, the proxy leaders and the replica tier serve one
    // journal each — the node's first user journal — so every other journal,
    // the system journals included, must be a plain Multi-Paxos journal
    // (#188; journal-tagged proxies are #193).
    let plane = journals.plane().map(|(journal, _)| *journal);
    let planed = journals
        .live
        .iter()
        .filter(|(journal, _)| Some(**journal) != plane)
        .any(|(_, rt)| {
            let config = rt.node.config();
            config.has_matchmakers() || config.proxy_count > 0 || config.replica_count > 0
        });
    if planed {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "only a node's first journal may name matchmakers, proxies or replicas".into(),
        )));
    }
    // The caller named the node (no id has a default, §3.8); every journal
    // it opened must agree. Caller input, so a mismatch is a refusal.
    if journals.live.values().any(|rt| rt.node.config().id != node) {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "a journal's configuration names another node".into(),
        )));
    }
    let self_id = node.0;

    // Every task spawned by this incarnation must stop when the loop exits,
    // including a durability-seam error that immediately starts a replacement
    // incarnation. This drop guard covers every `?` and return path.
    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    // The RPC edge: a moonpool-rpc runtime listening on this node's address,
    // polled by the loop below. Its handlers are the typed queues the loop
    // selects on, so the loop remains the sole owner of every journal's core.
    let me = Party::Node(NodeId(self_id));
    let mut edge = RpcEdge::listen(&providers, &local_addr, "node", &tunables).await?;
    let mut rpc = NodeInbox::serve(
        &providers,
        &edge,
        &tunables,
        me,
        cell_id,
        edge_reporter(&node_audit, me),
        incarnation_shutdown.clone(),
    )?;

    // The system journals' follower (#189), and the inbox its remote reads
    // answer on. Its sender stays alive with the follower, so the arm below
    // simply never fires on a static deployment.
    let fixed: Vec<NodeId> = members
        .iter()
        .chain(replicas.iter())
        .map(|(id, _)| *id)
        .collect();
    let (mut follower, mut follow_answers, _follow_open) = if let Some(plan) = &system {
        let (follower, inbox) = SystemFollower::new(
            plan,
            edge.handle(),
            fixed,
            &tunables,
            incarnation_shutdown.clone(),
        )?;
        (Some(follower), inbox, None)
    } else {
        let (open, inbox) = mpsc::channel::<Followed>(1);
        (None, inbox, Some(open))
    };
    let rpc_handle = edge.handle().clone();
    // A machine's vote is final: it answers the cell decree from its record
    // while it serves its cell (#277).
    if let Some(formed) = formed {
        formed.serve(&providers, &rpc_handle, incarnation_shutdown.clone())?;
    }

    // The replicas (#144) get a node's lane, as any peer. One lane per peer,
    // carrying every journal (#188: one `Deliver` per peer, a fair lane per
    // journal inside the mailbox).
    let learners: Vec<NodeId> = replicas.iter().map(|(id, _)| *id).collect();
    let lanes = LaneOpener {
        providers: &providers,
        tunables,
        shutdown: incarnation_shutdown.clone(),
        audit: &node_audit,
        from: me,
        cell_id,
    };
    let peer_queues = lanes.open_all(
        edge.handle(),
        "paros-peer-delivery",
        members.into_iter().chain(replicas),
        Party::Node,
    )?;
    // The proxy leaders (#142): one lane each, on the same lossy keep-newest
    // contract as a peer's. Empty on a deployment without proxies.
    let proxy_queues =
        lanes.open_all(edge.handle(), "paros-proxy-delivery", proxies, Party::Proxy)?;

    // The matchmaker links (#120): one client per matchmaker, and the inbox
    // their answers come back through. Empty on plain Multi-Paxos — and a
    // deployment with matchmakers runs one journal, the one they serve.
    let (match_reply_tx, mut match_replies) =
        mpsc::channel::<MatchReply>(tunables.peer_inbox_capacity);
    let matchmaker_clients = matchmakers
        .into_iter()
        .map(|(id, addr)| {
            Ok((
                id,
                MatchmakerClient::new(edge.handle(), peer_address(&addr)?),
            ))
        })
        .collect::<SimulationResult<BTreeMap<_, _>>>()?;
    let (gc_ack_tx, mut gc_acks) = mpsc::channel::<GcAck>(tunables.peer_inbox_capacity);
    let (reconfigure_reply_tx, mut reconfigure_replies) =
        mpsc::channel::<ReconfigureReply>(tunables.peer_inbox_capacity);
    let links = MatchmakerLinks {
        clients: matchmaker_clients,
        replies: match_reply_tx,
        gc_acks: gc_ack_tx,
        reconfigure_replies: reconfigure_reply_tx,
        timeout: tunables.delivery_timeout,
        shutdown: incarnation_shutdown.clone(),
    };
    // The matchmaker-set handover (#125): the reconfigurer plus the two
    // clocks that pace it, idle until a client asks or until this node meets a
    // frozen registry nobody finished replacing.
    let mut handover = HandoverDriver::new(NodeId(self_id));
    // Set by an accepted operator `Retire`: the node exits at its next tick,
    // after the ack had a beat to leave.
    let mut retiring: Option<JournalIdentifier> = None;

    let out = Outbound::new(peer_queues, proxy_queues, learners, me);
    let shared = Shared {
        providers: &providers,
        links: &links,
        out: &out,
        self_id,
        tunables,
    };

    let time = providers.time().clone();
    let mut ticks: u64 = 0;
    // The tick deadline is ABSOLUTE, not a fresh relative sleep per loop
    // iteration: `select!` drops and re-creates its futures every pass, so a
    // relative `sleep(TICK_INTERVAL)` resets whenever any other branch is
    // ready. Under sustained sub-interval traffic (a singleton absorbing every
    // client retry, a reconnect storm) the protocol clock then never advances —
    // no election, no ack, clients retry harder: a self-sustaining starvation
    // loop (seed 3847608256092482294 ticked twice in 81 simulated seconds).
    // With an absolute deadline the sleep is zero-length once the deadline
    // passes and fires regardless of load.
    let mut next_tick = time.now() + tunables.tick_interval;
    loop {
        moonpool_core::select! {
            // The runtime's future is persistent across passes: see `RpcEdge`.
            error = edge.run() => return Err(error.into()),
            Some((req, reply)) = rpc.write.recv() => {
                // A journal `Write` (#204) → the named journal's leader. The
                // leader proposes it into the next slot without judging it:
                // the writer, the position and a retry are all the journal
                // state machine's, at apply. The reply is held until the
                // slot applies and answered with that verdict; a non-leader
                // redirects immediately.
                let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.live.get_mut(&journal) else {
                    if refuse_unknown(&journals, follower.as_ref(), journal, "write", self_id, &node_audit) {
                        shared.with(&node_audit).answer(
                            Reply::Redirect,
                            reply,
                            WriteAck { unknown_journal: true, ..WriteAck::default() },
                        );
                    }
                    continue;
                };
                // The batch limits (#241, §2.7) are judged here, at the edge:
                // a batch over them never reaches consensus.
                if let Some(ack) = calls::batch_too_large(&req.records, &tunables) {
                    tracing::debug!(node = self_id, records = req.records.len(), "write_too_large");
                    shared.with(&node_audit).answer(Reply::Write, reply, ack);
                    continue;
                }
                let entry = Entry {
                    leader: leader_uuid_from_proto(req.leader),
                    seq: Seq(req.seq),
                    records: req.records.into_iter().map(Value).collect(),
                };
                // The column override (#141): drawn only where it can have
                // an effect (this node leads and its configuration is a
                // grid), on the loop. Otherwise the core's `slot % cols`
                // stands.
                let column = match rt.node.acceptors().quorum_system() {
                    QuorumSystem::Grid { cols, .. } if rt.node.is_leader() => {
                        phase2_column(rt.node.proposer().next_slot(), cols)
                    }
                    _ => None,
                };
                // The delegation override (#142), under the same gate: only
                // a leader of a deployment with proxies is asked, first
                // whether to run this round colocated, then which proxy to
                // hand it to; otherwise `Delegation::Auto`, the core's
                // `slot % proxy_count`, stands.
                let delegation = delegation_choice(&rt.node);
                let result = rt.node.propose_in(entry.clone(), column, delegation);
                park_call(rt, result, Call::Write { entry, reply }, &shared);
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.set_leader.recv() => {
                // A journal `SetLeader` (#241): a compare-and-set decided
                // into the log and judged at apply, like a `Write`.
                let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.live.get_mut(&journal) else {
                    if refuse_unknown(&journals, follower.as_ref(), journal, "set_leader", self_id, &node_audit) {
                        shared.with(&node_audit).answer(
                            Reply::Redirect,
                            reply,
                            SetLeaderAck { unknown_journal: true, ..SetLeaderAck::default() },
                        );
                    }
                    continue;
                };
                let new = leader_uuid_from_proto(req.new);
                let old = leader_from_proto(req.old);
                let delegation = delegation_choice(&rt.node);
                let result = rt
                    .node
                    .propose_control_in(Control::SetLeader { new, old }, delegation);
                park_call(rt, result, Call::SetLeader { new, old, reply }, &shared);
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.truncate.recv() => {
                // A journal `Truncate` (#204): decided into the log, judged
                // at apply (fenced by the leader uuid like a `Write`, #228, #241;
                // monotone, clamped to `next_seq`), and every node
                // drops the slots whose records all lie below the new
                // `first_seq` when its walk reaches it. A control journal is
                // truncated like any other (§3.9: its owner checkpoints it,
                // then truncates up to the checkpoint, #230); a follower that
                // reads below the floor jumps to the checkpoint there.
                let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.live.get_mut(&journal) else {
                    if refuse_unknown(&journals, follower.as_ref(), journal, "truncate", self_id, &node_audit) {
                        shared.with(&node_audit).answer(
                            Reply::Redirect,
                            reply,
                            TruncateAck { unknown_journal: true, ..TruncateAck::default() },
                        );
                    }
                    continue;
                };
                let up_to = Seq(req.up_to);
                let leader = leader_uuid_from_proto(req.leader);
                let delegation = delegation_choice(&rt.node);
                let result = rt.node.propose_control_in(
                    Control::Truncate { leader, up_to },
                    delegation,
                );
                park_call(rt, result, Call::Truncate { leader, up_to, reply }, &shared);
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.log_read.recv() => {
                // A journal `Read` (#204), served through the leaderless
                // read (#143, Paxos Quorum Reads) on any node: the core asks
                // a Phase-1 quorum — a row of a grid, the whole
                // configuration otherwise — for their vote watermarks, and
                // the read is served from this node's journal fold once it
                // covers the maximum. Never a redirect: no role is asked
                // for.
                let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.live.get_mut(&journal) else {
                    if refuse_unknown(&journals, follower.as_ref(), journal, "read", self_id, &node_audit) {
                        shared.with(&node_audit).answer(
                            Reply::LogRead,
                            reply,
                            ReadAck { unknown_journal: true, ..ReadAck::default() },
                        );
                    }
                    continue;
                };
                let ctx = rt.waiters.reads.next_ctx();
                // The configuration the core reads over is its read basis
                // (#260), not its belief; without one the core opens nothing
                // and the read is answered unserved by the retry sweep — so
                // neither the row hook nor the audit hears of it.
                let basis = rt.node.read_basis();
                // The row override (the Phase-1 twin of `phase2_column`) is
                // drawn only where it can have an effect: under a grid. Its
                // own location; the replica tier has the other. Otherwise the
                // core's `ctx % rows` stands.
                let row = basis.as_ref().and_then(|b| match b.config.quorum_system() {
                    QuorumSystem::Grid { rows, .. }
                        if rows >= 2 && moonpool_buggify::buggify_fault_with_prob!(0.10) =>
                    {
                        moonpool_assertions::reachable!("grid: the driver overrides a quorum read's row");
                        Some(log_reads::next_row(ctx, rows))
                    }
                    _ => None,
                });
                // The row the core will ask, resolved exactly as it resolves
                // it, for the audit's report of what served it.
                let row = basis.as_ref().and_then(|b| b.config.read_row(ctx, row));
                let opened = fold_head(&rt.node);
                let wait = log_reads::wait_ticks(req.wait_ms, tunables.tick_interval, tunables.read_poll_ticks);
                if basis.is_some() {
                    rt.audit.quorum_read_opened(NodeId(self_id), ctx);
                }
                rt.node.quorum_read_in(ctx, row);
                rt.waiters.reads.park(&req, reply, wait, row, opened);
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((journal, msg)) = rpc.deliver.recv() => {
                // A peer Paxos message → its journal's single input router.
                // The same `paros_core::Message` is sent and received (no
                // DTO). The sender was acknowledged when the message entered
                // the inbox, so nothing here answers it. A message for a
                // journal this node does not run now (quarantined, down, or
                // never served) is dropped: the peer's re-send repairs it.
                // The registry is the pool (#189): a node the fold has not
                // admitted yet is refused before the core sees its message.
                if let Some(f) = &follower
                    && let Some(from) = events::message_sender(&msg)
                    && !f.admits(from)
                {
                    if let Party::Node(from) = from {
                        node_audit.unpooled_message(NodeId(self_id), journal, from);
                    }
                    tracing::info!(node = self_id, journal = %journal, from = %from, "unpooled_message_refused");
                    continue;
                }
                let held = crate::scenario::hold_journal(hold_candidate, journal);
                let Some(rt) = journals.live.get_mut(&journal).filter(|_| !held) else {
                    tracing::info!(node = self_id, journal = %journal, held, "journal_message_dropped");
                    continue;
                };
                trace_received(self_id, &msg);
                // An ack at the inbox, whatever the core makes of it: what
                // refills a leader's `CheckQuorum` window, and what the
                // deposed-leader oracle measures its clock from.
                if let Message::HeartbeatAck { from, ballot, .. } = &msg {
                    rt.audit.heartbeat_ack_received(NodeId(self_id), *from, *ballot);
                }
                // Canary: a Prepare whose from_slot is below our floor is the
                // dangerous "campaign against a truncated acceptor" case. Record it
                // so the sweep can assert the interleaving stays reachable once the
                // acceptor floor guard is in place.
                if let Message::Prepare { from_slot, .. } = &msg
                    && *from_slot < rt.node.acceptor().first_slot()
                {
                    rt.audit.prepare_below_floor(NodeId(self_id), *from_slot, rt.node.acceptor().first_slot());
                    tracing::info!(
                        node = self_id,
                        from_slot = from_slot.0,
                        floor = rt.node.acceptor().first_slot().0,
                        "prepare_below_floor"
                    );
                }
                rt.node.step(msg);
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some(reply) = match_replies.recv() => {
                // A matchmaker's answer to this candidate's registration (#120):
                // fold it into the open matchmaking phase; a quorum closes the
                // phase and opens Phase 1 in the same step. A matchmaker
                // deployment runs one journal.
                let Some((&journal, rt)) = journals.first() else { continue };
                let (matchmaker, ballot) = (reply.matchmaker, reply.ballot);
                // The duplicate seam (the mirror of the matchmaker driver's
                // reply drop): what it tests is the idempotency the
                // registration path claims — a matchmaker already counted
                // never re-opens the quorum.
                maybe_duplicate(NodeId(self_id), Reply::Match, &links.replies, &reply);
                tracing::info!(
                    node = self_id,
                    matchmaker = matchmaker.0,
                    round = ballot.round,
                    registered = matches!(reply.outcome, paros_core::MatchOutcome::Registered { .. }),
                    "match_reply_received"
                );
                let folded = folded_answer(&reply);
                let step = rt.node.on_match_reply(reply);
                report_match_step(&rt.node, &rt.audit, self_id, matchmaker, ballot, folded, &step);
                shared.with(&rt.audit).on_match_refusal(&rt.node, &mut handover, matchmaker, &step);
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some(ack) = gc_acks.recv() => {
                // A matchmaker's answer to this leader's GC request (#123):
                // fold it; a quorum makes the floor effective and names the
                // retirable acceptors (reported in the step).
                let Some((&journal, rt)) = journals.first() else { continue };
                maybe_duplicate(NodeId(self_id), Reply::GcAck, &links.gc_acks, &ack);
                let step = rt.node.on_gc_ack(&ack);
                rt.audit.gc_step(NodeId(self_id), ack.matchmaker, &ack, &step);
                tracing::info!(
                    node = self_id,
                    matchmaker = ack.matchmaker.0,
                    generation = ack.generation.0,
                    applied = ack.applied,
                    round = ack.watermark.round,
                    step = ?step,
                    "gc_ack_received"
                );
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some(reply) = reconfigure_replies.recv() => {
                // A matchmaker's answer to this node's handover step (#125).
                let Some((&journal, rt)) = journals.first() else { continue };
                let matchmaker = reply.matchmaker();
                maybe_duplicate(
                    NodeId(self_id),
                    Reply::MatchmakerReconfigure,
                    &links.reconfigure_replies,
                    &reply,
                );
                let step = handover.on_reply(reply.clone());
                rt.audit.reconfigurer_step(NodeId(self_id), matchmaker, &reply, &step);
                tracing::info!(
                    node = self_id,
                    matchmaker = matchmaker.0,
                    reply = events::reconfigure_reply_kind(&reply),
                    step = ?step,
                    "reconfigurer_step"
                );
                // The chosen set is authoritative the instant it is chosen —
                // this node adopts it before its publication completes.
                if let ReconfigurerStep::Chosen { successor }
                | ReconfigurerStep::Done { successor }
                | ReconfigurerStep::Superseded { successor } = &step
                {
                    rt.node.learn_matchmakers(successor);
                }
                if let ReconfigurerStep::Preempted { .. } = &step {
                    let ticks = providers
                        .random()
                        .random_range(1..tunables.reconfigure_backoff_max_ticks.max(1) + 1);
                    handover.back_off(ticks);
                    rt.audit.reconfigurer_backoff(NodeId(self_id), ticks);
                    tracing::info!(node = self_id, ticks, "reconfigurer_backoff");
                }
                shared.with(&rt.audit).send_reconfigure(handover.take_requests());
                let outcome = shared.settle(rt).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.reconfigure_matchmakers.recv() => {
                // No settle tail: this arm drives the reconfigurer, never the
                // core, so there is no `Ready` batch to drain.
                // A matchmaker-set reconfiguration (#125): any node may drive
                // it. Refusable like every operator request; a started
                // handover runs to completion on this node's own cadence.
                let Some((_, rt)) = journals.first() else { continue };
                let lp = shared.with(&rt.audit);
                let target: Vec<MatchmakerId> = req.members.iter().copied().map(MatchmakerId).collect();
                let refusal = operator::reconfigure_matchmakers(&rt.node, &mut handover, &target, |m| {
                    links.clients.contains_key(m)
                })
                .err();
                let accepted = refusal.is_none();
                let generation = rt.node.matchmaker_set().map_or(0, |set| set.generation.0);
                if let Some(current) = rt.node.matchmaker_set()
                    && accepted
                {
                    lp.start_reconfigurer(&mut handover, current, &target, false);
                }
                rt.audit.reconfigure_matchmakers_acked(NodeId(self_id), refusal);
                let label = refusal.map_or("", MatchmakersRefusal::label);
                tracing::info!(node = self_id, accepted, refusal = label, "reconfigure_matchmakers_acked");
                lp.answer(
                    Reply::ReconfigureMatchmakers,
                    reply,
                    ReconfigureMatchmakersAck {
                        accepted,
                        refusal: label.to_string(),
                        generation: accepted.then_some(generation),
                    },
                );
            }
            Some((req, reply)) = rpc.retire.recv() => {
                // No settle tail: this arm only reads the core and arms a flag
                // the tick arm acts on, so it produces no `Ready` batch. A
                // retirement is a matchmaker-plane act, so the node's first
                // journal's: it leaves that journal, and keeps serving the
                // others (#188).
                let Some((&journal, rt)) = journals.first() else { continue };
                let ack = operator::retire(&rt.node, &rt.audit, self_id, &req);
                if ack.accepted {
                    retiring = Some(journal);
                }
                shared.with(&rt.audit).answer(Reply::Retire, reply, ack);
            }
            Some((req, reply)) = rpc.reconfigure.recv() => {
                // An acceptor reconfiguration (#122): a matchmaker-deployment
                // act, so the node's one journal; a plain deployment's first
                // journal refuses it (`no_matchmakers`).
                let Some((&journal, rt)) = journals.first() else { continue };
                let ack = operator::reconfigure(&mut rt.node, &rt.audit, self_id, &req);
                let outcome = shared.settle(rt).await;
                // A lost reconfiguration ack is ambiguous to the client, which
                // re-asks; a started reconfiguration stands (a retry is refused
                // as `not_leader` while it runs, then `unchanged` once done).
                shared.with(&node_audit).answer(Reply::Reconfigure, reply, ack);
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.inspect.recv() => {
                // No settle tail: an inspect is a pure read of the core, so it
                // produces no `Ready` batch. It names its journal or asks for
                // the node alone (#243): no identifier has a default, so an unset
                // one is refused, and so is a journal not live here — the
                // node's own facts are answered either way.
                let facts = operator::node_facts(self_id, cell.as_ref());
                let answer = match req.target() {
                    Ok(InspectTarget::Node) => facts,
                    Ok(InspectTarget::Journal(journal)) => journals.live.get(&journal).map_or_else(
                        || facts.refused(InspectRefusal::UnknownJournal),
                        |rt| operator::inspect(&rt.node, cell.as_ref()),
                    ),
                    Err(refusal) => facts.refused(refusal),
                };
                let _ = reply.send(answer);
            }
            Some(answer) = follow_answers.recv() => {
                // A seed's answer to this node's follow read of a system
                // journal (#189): fold it and apply what moved. The next read
                // opens on the next tick.
                let Some(f) = follower.as_mut() else { continue };
                let journal = answer.journal();
                let events = f.fold_remote(answer);
                let checkpoints = if journal == f.registry_key() { f.take_checkpoints() } else { Vec::new() };
                let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, rpc: &rpc_handle, audit: &node_audit };
                sys.apply(f, journal, events, checkpoints).await;
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                // Pacing, not a protocol bound: every timeout the core owns is
                // counted in ticks, so a node that waits twice as long between
                // ticks is exactly a slow node. Stretching desynchronizes the
                // cluster's protocol clocks — a shape moonpool's clock skew
                // reaches only for the wall clock, never for the tick counter
                // the election and read-round timers actually run on.
                next_tick = time.now() + if stretch_tick() { tunables.tick_interval * 2 } else { tunables.tick_interval };
                if let Some(journal) = retiring.take() {
                    // The operator's decommissioning takes effect: this
                    // identity leaves the journal and never comes back to it;
                    // a node left with no journal ends.
                    if let Some(rt) = journals.live.get(&journal) {
                        rt.audit.retired(NodeId(self_id));
                    }
                    tracing::info!(node = self_id, journal = %journal, "retired");
                    journals.park(journal, None);
                    if journals.exhausted() && (!follows || journals.stranded()) {
                        return journals.exit();
                    }
                }
                ticks += 1;
                // Tell the opener about every journal a storage fault took
                // since the last beat: it may know the store is gone for good.
                for journal in journals.take_quarantined() {
                    stores.quarantined(journal);
                }
                // A quarantined journal whose time is up re-opens from its
                // store — a restart of that journal alone.
                let due = journals.due(ticks, tunables.quarantine_ticks);
                for &journal in &due {
                    open_journal(&providers, &mut stores, &mut journals, journal, ticks, &tunables).await;
                }
                // A re-opened journal booted from `Config::pool`: it admits
                // the registry's pool again (#189).
                if let Some(f) = follower.as_ref().filter(|_| !due.is_empty()) {
                    admit_pool(&mut journals, f);
                }
                // Every live journal's beat, in id order.
                let live: Vec<JournalIdentifier> = journals.live.keys().copied().collect();
                for journal in live {
                    let Some(rt) = journals.live.get_mut(&journal) else { continue };
                    if crate::scenario::hold_journal(hold_candidate, journal) {
                        rt.audit.journal_held(NodeId(self_id));
                        tracing::info!(node = self_id, journal = %journal, "journal_held");
                        continue;
                    }
                    let outcome = shared.beat(rt, &mut handover, ticks).await;
                    journals.fold(journal, outcome, ticks, self_id)?;
                }
                // The system journals (#189): fold what this node's own
                // the system journals chose, apply it, and keep a follow read
                // open against a seed for the ones it does not run.
                if let Some(f) = follower.as_mut() {
                    for (journal, events) in follow_local(f, &journals) {
                        let checkpoints = if journal == f.registry_key() { f.take_checkpoints() } else { Vec::new() };
                        let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, rpc: &rpc_handle, audit: &node_audit };
                        sys.apply(f, journal, events, checkpoints).await;
                    }
                    f.poll_remote(&providers, |journal| journals.live.contains_key(&journal));
                }
                if journals.exhausted() && (!follows || journals.stranded()) {
                    return journals.exit();
                }
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

/// What applying the system journals' folds (#189) touches: the node's
/// journals and their opener, the lanes, and the node-level audit.
struct SystemCtx<'a, 'l, P: Providers, J: JournalStores> {
    stores: &'a mut J,
    journals: &'a mut Journals<J::Store, J::Audit>,
    now: u64,
    tunables: &'a DriverTunables,
    out: &'a Outbound,
    lanes: &'a LaneOpener<'l, P, J::Audit>,
    rpc: &'a moonpool_rpc::RpcHandle<P>,
    audit: &'a J::Audit,
}

/// An event is tagged by the journal whose fold produced it: a registry
/// event by the registry, a directory event by any other system journal.
fn assert_event_source(
    registry: JournalIdentifier,
    journal: JournalIdentifier,
    event: &SystemEvent,
) {
    match event {
        SystemEvent::Registry(_) => {
            assert!(
                journal == registry,
                "a registry event comes from the registry"
            );
        }
        SystemEvent::Directory(_) => {
            assert!(
                journal != registry,
                "a directory event never comes from the registry"
            );
        }
    }
}

impl<P: Providers, J: JournalStores> SystemCtx<'_, '_, P, J> {
    /// Apply what `journal`'s fold moved, in position order: start a created
    /// journal naming this node, stop a tombstoned one, open a lane to an
    /// admitted node, and stop every user journal on this node's own
    /// retirement. Each event is reported first, once.
    #[tracing::instrument(level = "debug", skip_all, fields(node = follower.self_id().0, journal = %journal, events = events.len()))]
    async fn apply(
        &mut self,
        follower: &SystemFollower<P>,
        journal: JournalIdentifier,
        events: Vec<(u64, SystemEvent)>,
        checkpoints: Vec<system::FoldedCheckpoint>,
    ) {
        let me = follower.self_id();
        self.report_checkpoints(follower, journal, checkpoints);
        // Whether the pool moved or a journal opened: every live journal
        // then admits the registry's pool (`ColocatedNode::extend_pool`,
        // refused on a journal that cannot reconfigure).
        let mut admit = false;
        for (seq, event) in events {
            assert_event_source(follower.registry_key(), journal, &event);
            self.audit.system_folded(me, journal, seq, &event);
            tracing::info!(node = me.0, journal = %journal, seq, event = ?event, "system_folded");
            match event {
                SystemEvent::Directory(DirectoryEvent::Created {
                    id, config, mode, ..
                }) => {
                    // A created journal lives in the directory's tenant
                    // (#235): the directory is that tenant's control journal.
                    let id = JournalIdentifier::new(journal.tenant, id);
                    // A stateless machine never takes acceptor work (#211):
                    // a configuration naming one runs without it.
                    if !config.members().contains(&me)
                        || follower.is_tombstoned(id)
                        || self.journals.serves(id)
                        || !follower.takes_storage_work()
                    {
                        continue;
                    }
                    let journal_config = paros_core::Config {
                        peers: config.members().to_vec(),
                        quorum_system: config.quorum_system(),
                        writer_mode: mode,
                        ..paros_core::Config::new(me, id)
                    };
                    if !self.stores.create(id, journal_config) {
                        continue;
                    }
                    open_journal(
                        self.lanes.providers,
                        self.stores,
                        self.journals,
                        id,
                        self.now,
                        self.tunables,
                    )
                    .await;
                    if self.journals.live.contains_key(&id) {
                        self.audit.journal_started(me, id);
                        tracing::info!(node = me.0, journal = %id, "journal_started");
                        admit = true;
                    }
                }
                SystemEvent::Directory(DirectoryEvent::Deleted { id }) => {
                    let id = JournalIdentifier::new(journal.tenant, id);
                    if self.journals.serves(id) {
                        self.journals.park(id, None);
                        self.audit.journal_stopped(me, id);
                        tracing::info!(node = me.0, journal = %id, "journal_stopped");
                    }
                    self.stores.delete(id);
                }
                SystemEvent::Registry(
                    RegistryEvent::Registered { id, .. } | RegistryEvent::Reregistered { id, .. },
                ) if id == me => {
                    self.join_spares(follower).await;
                    admit = true;
                }
                SystemEvent::Registry(
                    RegistryEvent::Registered { id, addr, .. }
                    | RegistryEvent::Reregistered { id, addr, .. },
                ) => {
                    self.admit_peer(me, id, &addr);
                    admit = true;
                }
                SystemEvent::Registry(RegistryEvent::Retired { id }) if id == me => {
                    self.retire_self(me);
                }
                // A checkpoint (#230): the fold holds the registry it names,
                // restored or verified. Everything it says is applied again —
                // a fold restored from it never saw the entries below it.
                SystemEvent::Registry(RegistryEvent::Checkpoint { .. }) => {
                    let registry = follower.registry().clone();
                    for (id, node) in registry.nodes() {
                        match node.standing {
                            NodeStanding::Retired if id == me => self.retire_self(me),
                            NodeStanding::Retired => {}
                            // A draining node takes no new work.
                            NodeStanding::Registered if id == me => {
                                self.join_spares(follower).await;
                            }
                            NodeStanding::Draining if id == me => {}
                            _ => self.admit_peer(me, id, &node.addr),
                        }
                    }
                    admit = true;
                }
                _ => {}
            }
        }
        if admit {
            admit_pool(self.journals, follower);
        }
    }
}

impl<P: Providers, J: JournalStores> SystemCtx<'_, '_, P, J> {
    /// Report each checkpoint `journal`'s fold met, in position order.
    fn report_checkpoints(
        &self,
        follower: &SystemFollower<P>,
        journal: JournalIdentifier,
        checkpoints: Vec<system::FoldedCheckpoint>,
    ) {
        let me = follower.self_id();
        for (seq, verified, state) in checkpoints {
            assert!(
                journal == follower.registry_key(),
                "only the registry is checkpointed"
            );
            assert!(
                state.next_seq() <= seq + 1,
                "a checkpoint's registry holds no position past it"
            );
            self.audit
                .checkpoint_folded(me, journal, seq, verified, &state);
            tracing::info!(node = me.0, journal = %journal, seq, ?verified, "checkpoint_folded");
        }
    }

    /// Node `id`, registered at `addr`, is in the pool: open its peer lane
    /// (once) and report the admission.
    fn admit_peer(&mut self, me: NodeId, id: NodeId, addr: &str) {
        // This node's own registration joins spares; it is never a peer.
        assert!(id != me, "a node never admits itself as a peer");
        if !self.out.has_peer(id) {
            match peer_address(addr) {
                Ok(addr) => {
                    let lane =
                        self.lanes
                            .open(self.rpc, "paros-peer-delivery", addr, Party::Node(id));
                    self.out.add_peer(id, lane);
                }
                Err(error) => {
                    tracing::warn!(node = me.0, admitted = id.0, %error, "registered_address_unusable");
                }
            }
        }
        self.audit.pool_admitted(me, id);
        tracing::info!(node = me.0, admitted = id.0, "pool_admitted");
    }

    /// This node's own retirement: every user journal it serves stops.
    fn retire_self(&mut self, me: NodeId) {
        let served: Vec<JournalIdentifier> = self
            .journals
            .live
            .keys()
            .copied()
            .filter(|journal| !self.journals.is_control(*journal))
            .collect();
        for journal in served {
            self.journals.park(journal, None);
            self.audit.journal_stopped(me, journal);
            tracing::info!(node = me.0, journal = %journal, "journal_stopped");
        }
        // A retired node serves its control journals and nothing else.
        assert!(
            self.journals
                .live
                .keys()
                .all(|j| self.journals.is_control(*j)),
            "a retired node runs no user journal"
        );
    }

    /// This node is in the pool now (#189): it joins every journal a
    /// reconfiguration may pull it into, as a spare — its own identity, the
    /// pool the registry has admitted.
    async fn join_spares(&mut self, follower: &SystemFollower<P>) {
        let me = follower.self_id();
        if !follower.takes_storage_work() {
            return;
        }
        for template in follower.spares() {
            let journal = template.journal;
            if self.journals.serves(journal) {
                continue;
            }
            let mut nodes = follower.pool();
            nodes.push(me);
            nodes.sort_unstable();
            nodes.dedup();
            let config = paros_core::Config {
                id: me,
                nodes,
                ..template.clone()
            };
            if !self.stores.create(journal, config) {
                continue;
            }
            open_journal(
                self.lanes.providers,
                self.stores,
                self.journals,
                journal,
                self.now,
                self.tunables,
            )
            .await;
            if self.journals.live.contains_key(&journal) {
                self.audit.journal_started(me, journal);
                tracing::info!(node = me.0, journal = %journal, "spare_joined");
            }
        }
    }
}

/// Every live journal admits the registry's pool (#189): a registered node
/// becomes one it follows, counts and answers. Refused by the core on a
/// journal that cannot reconfigure; retired nodes stay in (the pool is
/// grow-only) and are kept out at the edge instead.
fn admit_pool<P: Providers, S, A>(journals: &mut Journals<S, A>, follower: &SystemFollower<P>) {
    let pool = follower.pool();
    for rt in journals.live.values_mut() {
        rt.node.extend_pool(&pool);
        // A journal that can reconfigure now admits the whole registry pool.
        if rt.node.config().has_matchmakers() {
            assert!(
                pool.iter().all(|n| rt.node.pool().binary_search(n).is_ok()),
                "a reconfigurable journal admits the registry's pool"
            );
        }
    }
}

/// Whether a call naming `journal` — not live on this node — is refused as
/// unknown (`true`: answer `unknown_journal`, reported through the audit) or
/// silently left unanswered (`false`: the node serves the journal but it is
/// quarantined or down here, and the client's deadline retries elsewhere).
fn refuse_unknown<S, A: Audit, N: Audit, P: Providers>(
    journals: &Journals<S, A>,
    follower: Option<&SystemFollower<P>>,
    journal: JournalIdentifier,
    call: &'static str,
    self_id: u64,
    audit: &N,
) -> bool {
    // A journal the directory tombstoned (#189) is unknown from here on,
    // whatever this node still holds of it.
    let tombstoned = follower.is_some_and(|f| f.is_tombstoned(journal));
    if journal.is_set() && journals.serves(journal) && !tombstoned {
        // Unavailable, not unknown: a journal this node serves but cannot
        // answer right now is quarantined or down, never live.
        assert!(
            !journals.live.contains_key(&journal),
            "a live journal is never refused"
        );
        tracing::info!(
            node = self_id,
            journal = %journal,
            call,
            "journal_unavailable"
        );
        return false;
    }
    audit.journal_refused(NodeId(self_id), journal, call);
    tracing::info!(node = self_id, journal = %journal, call, "journal_refused");
    true
}

/// Open `journal`'s store and boot it into `journals` (at boot, or when its
/// quarantine is over at tick `now`): live on success, back in quarantine on
/// a storage fault, down for good when the store refuses to open.
async fn open_journal<P: Providers, J: JournalStores>(
    providers: &P,
    stores: &mut J,
    journals: &mut Journals<J::Store, J::Audit>,
    journal: JournalIdentifier,
    now: u64,
    tunables: &DriverTunables,
) {
    let audit = stores.audit(journal);
    let Some((storage, boot)) = stores.open(journal).await else {
        tracing::info!(journal = %journal, "journal_down");
        journals.park(journal, None);
        return;
    };
    match boot_journal(providers, storage, boot, audit, tunables).await {
        Ok(rt) => {
            journals.insert(journal, rt);
            stores.opened(journal);
        }
        Err(fault @ RunError::Storage(_)) => journals.requarantine(journal, now, fault),
        Err(fault) => journals.park(journal, Some(fault)),
    }
    // Whatever happened, the journal now sits in exactly one state.
    assert!(
        journals.serves(journal),
        "an opened journal is live, quarantined or down"
    );
}
