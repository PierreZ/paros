//! The node loop's handles: [`NodeLoop`], the fixed context of every arm's settle tail, and [`Shared`], what every journal's steps share.

use moonpool_core::{LocalTimeProvider, RandomProvider};
use paros_core::journal::{Control, TenantId};
use paros_core::matchmaker::{MatchRefusal, ReconfigureReply, ReconfigureRequest};
use paros_core::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem};
use paros_core::node::{MatchStep, NodeRole};
use paros_core::{ColocatedNode, Message, NodeId};
use paros_net::ReplySender;
use paros_net::runtime::ParosProviders;
use paros_observe::audit::Audit;
use paros_observe::metrics::Metrics;
use paros_storage::LogStorage;
use paros_storage::boot::RunError;
use paros_tunables::DriverTunables;

use super::calls::Call;
use super::edge::RpcEdge;
use super::flight::{Flight, Input};
use super::handover::{HandoverDriver, TenantAudits};
use super::journals::{JournalRt, JournalStores, Journals};
use super::matchmaking::{
    MatchmakerLinks, send_outbox, send_reconfigure_requests, surface_matchmaking,
};
use super::ready::{ClientWaiters, Taken, finish_ready, fold_head, take_ready};
use super::reply::Reply;
use super::report::{Deltas, draw_election_timeout, handoff_context, maintain};
use super::transport::Outbound;
use crate::node::batch::{Phase, hold, park_call};
use crate::node::control::read_only_refuses;
use crate::node::draws::{
    abandon_early, contest_overwrite, delegation_choice, initiate_handoff, trace_received,
};
use crate::node::grid::{fast_column, fast_row, move_stuck_rounds, phase2_column};
use crate::node::{flight, log_reads, reply};

/// The node loop's fixed context: the handles every arm's **settle tail** needs
/// and none of them change across an incarnation. Bundled so the tail is one
/// call instead of four repeated at every arm, and so the steps the arms share
/// take one handle.
pub(crate) struct NodeLoop<'a, P: ParosProviders, A: Audit> {
    pub(crate) providers: &'a P,
    pub(crate) links: &'a MatchmakerLinks<P>,
    pub(crate) out: &'a Outbound,
    pub(crate) audit: &'a A,
    pub(crate) self_id: u64,
    /// This node's own address, as its shard declarations name it.
    pub(crate) own_addr: &'a str,
    pub(crate) tunables: DriverTunables,
}

impl<P: ParosProviders, A: Audit> NodeLoop<'_, P, A> {
    /// How a shard declaration names the acceptor `id` (#760 (moonpool
    /// owns fault placement)): by the address its lane dials, this node by
    /// its own. A member with no lane here is named by its id, an address
    /// the simulator does not know.
    pub(crate) fn replica(&self, id: NodeId) -> moonpool_buggify::shard::Replica {
        let address = if id.0 == self.self_id {
            Some(self.own_addr.to_string())
        } else {
            self.out.dialed(id).map(|addr| addr.as_str().to_string())
        };
        moonpool_buggify::shard::Replica::new(address.unwrap_or_else(|| format!("node-{}", id.0)))
    }

    /// The **settle tail**'s second half, once the batch [`take_ready`] took
    /// is durable: finish the batch (send → apply), surface a matchmaking
    /// phase it opened *before* the requests leave, put the batch's
    /// matchmaker-wire requests on the wire, run the post-batch upkeep, and
    /// on a tick the read expiry: `expiry` carries the tick count and
    /// whether every confirming read expires early. The expired long-polls
    /// are answered from the fold the batch reported, before the next page
    /// is released. The order is the contract; the arms differ only in what
    /// they fed the core beforehand.
    async fn finish(
        &self,
        node: &mut ColocatedNode,
        waiters: &mut ClientWaiters,
        last: &mut Deltas,
        taken: Taken,
        expiry: Option<(u64, bool)>,
    ) {
        assert!(
            node.config().id.0 == self.self_id,
            "a node loop settles its own node"
        );
        let (promised, chosen) = (taken.promised_before, taken.chosen_before);
        let mut outbox = finish_ready(node, self.out, waiters, self.audit, taken).await;
        let reported = fold_head(node);
        if !outbox.gc_requests.is_empty() && paros_observe::scenario::withhold_gc() {
            tracing::info!(node = self.self_id, "gc_requests_withheld");
            outbox.gc_requests.clear();
        }
        surface_matchmaking(node, &mut last.matchmaking, self.audit, self.self_id);
        let tenant = node.config().journal.tenant;
        send_outbox(
            self.providers,
            self.links,
            self.audit,
            (self.self_id, tenant),
            outbox,
        );
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
        super::shards::declare(node, &mut last.shard, &|id| self.replica(id));
        // The journal fold only grows inside a batch: a journal read
        // long-polling at the tail is re-served here (#204).
        // Nothing between the drain and here walks the prefix: the reads
        // below see the fold the batch reported.
        assert!(
            fold_head(node) == reported,
            "a node serves reads from the fold its batch reported"
        );
        waiters.reads.wake(
            |from, limit, bytes| node.read_log(from, limit, bytes),
            NodeId(self.self_id),
            self.audit,
        );
        if let Some((ticks, expire_all)) = expiry {
            waiters.reads.expire(
                |from, limit, bytes| node.read_log(from, limit, bytes),
                ticks,
                self.tunables.read_retry_ticks,
                expire_all,
                NodeId(self.self_id),
                self.audit,
            );
        }
        assert!(
            fold_head(node) == reported,
            "serving reads never moves the fold"
        );
        // The batch is now durable, sent and applied, and every read this
        // settle answered saw the fold it reported. Only at this boundary may
        // the core materialize the next bounded Ready page. Released before
        // the reads (inside the drain, as before #550 (replica serves an
        // unreported fold page)), the core folds the page at once while its
        // writes and its report wait for the next batch, and a read parked at
        // the tail is served slots the node neither persisted nor reported
        // (witness 12015710127796105877 on PR #552: a node served next_seq 5
        // with its fold at slot 5 and its report at slot 3).
        node.advance_recovery();
        assert!(
            fold_head(node) >= reported,
            "releasing the next page never rewinds the fold"
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
    }

    /// Answer one client-facing reply through the reply seam
    /// ([`reply::answer`]).
    pub(crate) fn answer<T>(&self, kind: Reply, waiter: ReplySender<T>, ack: T) {
        reply::answer(self.audit, NodeId(self.self_id), kind, waiter, ack);
    }

    /// Put handover requests for `tenant`'s matchmaker set on the
    /// matchmaker wire.
    pub(crate) fn send_reconfigure(
        &self,
        tenant: TenantId,
        requests: Vec<(MatchmakerId, ReconfigureRequest)>,
    ) {
        assert!(
            requests.iter().all(|(_, r)| r.from().0 == self.self_id),
            "a handover request leaves in this node's name"
        );
        send_reconfigure_requests(
            self.providers,
            self.links,
            self.audit,
            (self.self_id, tenant),
            requests,
        );
    }

    /// Report a matchmaker-set handover this node just started (or is
    /// finishing, `finishing`) from `current` toward `target`, then put its
    /// first requests on the matchmaker wire — in that order.
    pub(crate) fn start_reconfigurer(
        &self,
        (tenant, handover): (TenantId, &mut HandoverDriver),
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
            tenant = tenant.0,
            generation = current.generation.0,
            target = target.len() as u64,
            finishing,
            "reconfigurer_started"
        );
        self.send_reconfigure(tenant, handover.take_requests());
    }

    /// The two straggler paths of a handover (#125), taken by whichever node
    /// meets them in a matchmaker's refusal: a registry frozen with no
    /// successor is finished by this node (the reconfigurer's decree adopts
    /// whatever was voted, or re-chooses the same members under a fresh
    /// generation); a member left inactive or behind is told the chosen set
    /// this node already knows.
    pub(crate) fn on_match_refusal(
        &self,
        node: &ColocatedNode,
        handover: &mut HandoverDriver,
        matchmaker: MatchmakerId,
        step: &MatchStep,
    ) {
        let tenant = node.config().journal.tenant;
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
                    self.start_reconfigurer((tenant, handover), &current, current.members(), true);
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
                    self.republish(tenant, matchmaker, set);
                }
            }
            _ => {}
        }
    }

    /// Tell `matchmaker`, a straggler, the chosen `set` this node knows: the
    /// `Chosen` link from the generation before it.
    fn republish(&self, tenant: TenantId, matchmaker: MatchmakerId, set: MatchmakerSet) {
        // Generation 0 is the bootstrap: no decree chose it.
        assert!(
            set.generation.0 > 0,
            "only a chosen successor is republished"
        );
        let (audit, self_id) = (self.audit, self.self_id);
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
        self.send_reconfigure(tenant, vec![(matchmaker, request)]);
    }

    /// A member of the generation this node's handover replaces answered
    /// from an earlier generation (#496): it never learned that the set
    /// being replaced was chosen. Its freeze can then never count, so a
    /// handover that needs it waits for good, and no other path tells it:
    /// a campaign meets it only when its refusal comes first. Witness
    /// 16692607731545838330 of #496: generation 1 was `{1, 2, 3, 4}`, only
    /// matchmakers 3 and 4 learned it, and every finish of it was refused
    /// by 1 and 2 from generation 0.
    pub(crate) fn on_handover_straggler(
        &self,
        tenant: TenantId,
        handover: &HandoverDriver,
        reply: &ReconfigureReply,
    ) {
        let ReconfigureReply::Refused {
            matchmaker,
            current,
            ..
        } = reply
        else {
            return;
        };
        let Some(old) = handover.old() else { return };
        if current.generation >= old.generation || !old.contains(*matchmaker) {
            return;
        }
        // A handover replaces the set its node believes in force: the
        // bootstrap, or a set a decree chose. Only the second has a link.
        if old.generation.0 == 0 {
            return;
        }
        moonpool_assertions::reachable!(
            "generation: a handover tells a behind member the set it replaces"
        );
        self.republish(tenant, *matchmaker, old.clone());
    }

    /// One tick of the running matchmaker-set handover (#125): its stall
    /// clock, the two ways it is given up, and its re-send — its own cadence
    /// and its own location; a preempted decree reopens only here, so two
    /// dueling reconfigurers are paced by their drivers.
    pub(crate) fn pace_handover(&self, node: &ColocatedNode, handover: &mut HandoverDriver) {
        assert!(
            node.config().has_matchmakers(),
            "a handover is paced only on a matchmaker deployment"
        );
        let (audit, self_id) = (self.audit, self.self_id);
        let tenant = node.config().journal.tenant;
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
        // BUGGIFY (#496): close a freeze the tick its quorum answered, with
        // no round left for the stragglers. Valid (the quorum is all a
        // reconstruction rests on), and a `finish` then proposes exactly the
        // quorum: a thinner successor set, which tolerates fewer losses.
        // Silent in the recovery tail.
        let hasty =
            handover.stop_quorum_reached() && moonpool_buggify::buggify_fault_with_prob!(0.5);
        if hasty {
            moonpool_assertions::reachable!("generation: a freeze closes on its first quorum");
        }
        if !hasty && !handover.resend_due(self.tunables.reconfigurer_resend_ticks) {
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
                tenant = tenant.0,
                members = bootstrap.set.members().len() as u64,
                journals = bootstrap.registries.len() as u64,
                registrations = bootstrap
                    .registries
                    .values()
                    .map(|registry| registry.history.len())
                    .sum::<usize>() as u64,
                disagreements = reconstruction.disagreements,
                "reconfigurer_reconstructed"
            );
            self.send_reconfigure(tenant, handover.take_requests());
        }
        if hasty {
            // No re-send was due: the bootstrap just went out.
            return;
        }
        // Skipping a due re-send is always safe: it stretches the
        // stop-the-world window and delays a preempted decree's reopening.
        // Silent in the recovery tail.
        if moonpool_buggify::buggify_fault_with_prob!(0.5) {
            moonpool_assertions::reachable!("generation: the driver skips a due handover re-send");
            tracing::info!(node = self_id, "reconfigurer_resend_skipped");
        } else {
            handover.resend();
            self.send_reconfigure(tenant, handover.take_requests());
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
        // again. Silent in the recovery tail. A leader that holds delegated
        // rounds has its own named location too (`RESIGN_DELEGATING`, #341):
        // it campaigns again at once, so its next leadership's delegations
        // meet the rounds a proxy still holds for this one.
        if !handed_off && node.role() == NodeRole::Leader {
            let again =
                !node.delegated_rounds().is_empty() && paros_observe::scenario::resign_delegating();
            if again || moonpool_buggify::buggify_fault_with_prob!(0.004) {
                moonpool_assertions::reachable!("the driver voluntarily resigns leadership");
                audit.stepped_down(NodeId(self_id));
                tracing::info!(node = self_id, "leadership_resigned");
                let promised = node.acceptor().promised();
                node.step_down();
                if again {
                    // Any node may campaign at any time, at any fresh round.
                    node.campaign_above(promised);
                }
            }
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

/// The audit ports of every journal of `tenant` that booted here naming
/// matchmakers (#190), live or not: the ports its set's handover reports to.
/// A quarantined or held journal still hears its tenant's handover, since
/// its matchmakers' registries move with it.
pub(crate) fn tenant_audits<S, J: JournalStores>(
    journals: &Journals<S, J::Audit>,
    stores: &J,
    tenant: TenantId,
) -> TenantAudits<J::Audit> {
    let audits = TenantAudits(
        journals
            .matchmade(tenant)
            .map(|journal| stores.audit(journal))
            .collect(),
    );
    assert!(
        audits.0.len() <= journals.matchmade(tenant).count(),
        "a tenant's ports are its matchmade journals'"
    );
    audits
}

/// What every journal's steps share on this node: the handles a
/// [`NodeLoop`] needs apart from the journal's own audit port.
pub(crate) struct Shared<'a, P: ParosProviders> {
    pub(crate) providers: &'a P,
    pub(crate) links: &'a MatchmakerLinks<P>,
    pub(crate) out: &'a Outbound,
    pub(crate) edge: &'a RpcEdge<P>,
    pub(crate) self_id: u64,
    /// This node's own address, as its shard declarations name it.
    pub(crate) own_addr: &'a str,
    pub(crate) tunables: DriverTunables,
    /// The machine's metrics (#366 (metrics)); off outside a machine.
    pub(crate) metrics: &'a Metrics,
}

impl<P: ParosProviders> Shared<'_, P> {
    /// The node loop's steps, reporting to `audit` (a journal's port).
    pub(crate) fn with<'b, A: Audit>(&'b self, audit: &'b A) -> NodeLoop<'b, P, A> {
        NodeLoop {
            providers: self.providers,
            links: self.links,
            out: self.out,
            audit,
            self_id: self.self_id,
            own_addr: self.own_addr,
            tunables: self.tunables,
        }
    }

    /// The settle tail for one journal (#613 (settle off the node loop)):
    /// take the core's batch, then either finish it at once (no durable
    /// write) or launch its flush and return, the journal flying until
    /// [`Shared::landed`] finishes it. Called only on a journal that does
    /// not fly: a flying journal's inputs wait in its backlog.
    pub(crate) async fn settle<S, A>(&self, rt: &mut JournalRt<S, A>)
    where
        S: LogStorage + 'static,
        A: Audit + Clone + Send + Sync + 'static,
    {
        assert!(
            rt.backlog.is_empty(),
            "a journal that does not fly holds nothing back"
        );
        self.take(rt).await;
    }

    /// Take the core's batch: finish it at once (no durable write) or
    /// launch its flush. [`Shared::settle`], and the backlog's own settles
    /// ([`Shared::step_backlog`]), which may leave inputs held behind the
    /// flight it launches.
    async fn take<S, A>(&self, rt: &mut JournalRt<S, A>)
    where
        S: LogStorage + 'static,
        A: Audit + Clone + Send + Sync + 'static,
    {
        assert!(rt.flight.is_none(), "a flying journal takes no other batch");
        let began = self.providers.time().now();
        let taken = take_ready(&mut rt.node, self.out, &rt.audit).await;
        let expiry = rt.expiry.take();
        if taken.writes.is_empty() {
            self.with(&rt.audit)
                .finish(&mut rt.node, &mut rt.waiters, &mut rt.last, taken, expiry)
                .await;
            self.settled(rt, began);
            return;
        }
        let Some(storage) = rt.storage.take() else {
            unreachable!("a journal that does not fly holds its store");
        };
        rt.flight = Some(Flight::launch(
            storage,
            taken,
            (&rt.audit, self.self_id),
            expiry,
            began,
        ));
        assert!(
            rt.storage.is_none(),
            "a flying journal's store is in its flush"
        );
    }

    /// Record a finished batch's metrics (observation only, #366 (metrics)).
    fn settled<S, A>(&self, rt: &JournalRt<S, A>, began: std::time::Duration) {
        let applied = fold_head(&rt.node).map_or(0, |slot| slot.0 + 1);
        let chosen = rt
            .node
            .replica()
            .chosen_index()
            .map_or(0, |slot| slot.0 + 1);
        let took = self.providers.time().now().saturating_sub(began);
        self.metrics
            .settled(rt.node.config().journal, applied, chosen, took);
    }

    /// Step `input` into the journal's core and settle, or hold it back
    /// while the journal's batch flies (#613 (settle off the node loop)). A
    /// full backlog waits for the flight here first.
    ///
    /// # Errors
    ///
    /// The landed flush's fault ([`Shared::land`]).
    pub(crate) async fn input<S, A>(
        &self,
        rt: &mut JournalRt<S, A>,
        input: Input,
    ) -> Result<(), RunError>
    where
        S: LogStorage + 'static,
        A: Audit + Clone + Send + Sync + 'static,
    {
        if rt.flight.is_some() && rt.backlog.len() < flight::BACKLOG {
            hold(&mut rt.backlog, input);
            assert!(
                rt.backlog.len() <= flight::BACKLOG,
                "a backlog stays bounded"
            );
            return Ok(());
        }
        self.land(rt).await?;
        assert!(rt.flight.is_none(), "a landed journal flies nothing");
        self.apply(rt, input);
        self.settle(rt).await;
        Ok(())
    }

    /// Wait here for the journal's flight, if one flies, and finish it
    /// ([`Shared::landed`]); the runtime runs through the wait. For the
    /// rare inputs that do not wait in the backlog, and a full backlog.
    ///
    /// # Errors
    ///
    /// The flush's storage fault, or the runtime's exit.
    pub(crate) async fn land<S, A>(&self, rt: &mut JournalRt<S, A>) -> Result<(), RunError>
    where
        S: LogStorage + 'static,
        A: Audit + Clone + Send + Sync + 'static,
    {
        // The backlog's batches may fly in turn: wait for each, so the
        // caller's input steps after every input held before it.
        while let Some(flight) = rt.flight.take() {
            let (storage, done) = self.edge.alongside(flight.flush).await?;
            let landing = flight::Landing {
                storage,
                done,
                taken: flight.taken,
                expiry: flight.expiry,
                began: flight.began,
            };
            self.landed(rt, landing).await?;
        }
        assert!(rt.flight.is_none(), "a landed journal flies nothing");
        assert!(rt.backlog.is_empty(), "a landed journal holds nothing back");
        Ok(())
    }

    /// Finish a landed flight: give the store back, finish the batch, then
    /// step and settle the inputs held back meanwhile
    /// ([`Shared::step_backlog`]).
    ///
    /// # Errors
    ///
    /// The flush's storage fault: the batch is not finished, nothing it
    /// held leaves.
    pub(crate) async fn landed<S, A>(
        &self,
        rt: &mut JournalRt<S, A>,
        landing: flight::Landing<S>,
    ) -> Result<(), RunError>
    where
        S: LogStorage + 'static,
        A: Audit + Clone + Send + Sync + 'static,
    {
        assert!(rt.flight.is_none(), "a landing ends the journal's flight");
        assert!(rt.storage.is_none(), "a landing gives the store back");
        rt.storage = Some(landing.storage);
        landing.done?;
        if rt.back_from_full {
            // A store a full disk made read-only wrote again (#360 (disk
            // full)): the outcome the read-only state exists for.
            rt.back_from_full = false;
            moonpool_assertions::reachable!("disk full: a store went read-only and came back");
        }
        self.with(&rt.audit)
            .finish(
                &mut rt.node,
                &mut rt.waiters,
                &mut rt.last,
                landing.taken,
                landing.expiry,
            )
            .await;
        self.settled(rt, landing.began);
        self.step_backlog(rt).await;
        Ok(())
    }

    /// Step the inputs held back during a flight and settle them, as few
    /// batches as the core's phase allows (group commit): the inputs of one
    /// phase share one batch, and an input that moves the phase
    /// ([`Phase`]) ends its batch. Every per-batch observer
    /// (`surface_matchmaking`, `maintain`) reads the core once, after the
    /// batch, and reports what it sees under the core's ballot, so a batch
    /// holds at most one phase change, as one input per batch did before
    /// #613 (settle off the node loop). Witnesses on PR #639 (sync
    /// hand-off): 4250574494756084282, a beat opened a campaign at round 33
    /// and a later message of the same batch abandoned it, so its requests
    /// left unsurfaced; 12321567722382338258, a re-asked campaign was
    /// reported under a ballot a later input had moved. Stops when the
    /// backlog is empty, or when a batch flies: the rest waits for it.
    async fn step_backlog<S, A>(&self, rt: &mut JournalRt<S, A>)
    where
        S: LogStorage + 'static,
        A: Audit + Clone + Send + Sync + 'static,
    {
        while rt.flight.is_none() && !rt.backlog.is_empty() {
            let mut held = std::mem::take(&mut rt.backlog).into_iter();
            let mut stepped = 0_usize;
            for input in held.by_ref() {
                let before = Phase::of(&rt.node);
                self.apply(rt, input);
                stepped += 1;
                if Phase::of(&rt.node) != before {
                    break;
                }
            }
            rt.backlog = held.collect();
            assert!(stepped > 0, "a batch of the backlog steps an input");
            if stepped > 1 {
                moonpool_assertions::reachable!(
                    "driver: one flush carries the inputs that waited on the last one"
                );
            }
            self.take(rt).await;
        }
        assert!(
            rt.backlog.is_empty() || rt.flight.is_some(),
            "a journal holds inputs back only while it flies"
        );
    }

    /// Step one input into the journal's core (no settle).
    pub(crate) fn apply<S, A>(&self, rt: &mut JournalRt<S, A>, input: Input)
    where
        A: Audit + Clone,
    {
        assert!(rt.flight.is_none(), "nothing steps a flying journal's core");
        let self_id = self.self_id;
        match input {
            Input::Propose(call) => {
                let delegation =
                    delegation_choice(&rt.node, |proxy| self.out.proxy_queues.contains_key(&proxy));
                let result = match &call {
                    Call::Write { entry, .. } => {
                        // The column override (#141): drawn only where it
                        // can have an effect (this node leads and its
                        // configuration is a grid), on the loop. Otherwise
                        // the core's `slot % cols` stands.
                        let column = match rt.node.acceptors().quorum_system() {
                            QuorumSystem::Grid { cols, .. } if rt.node.is_leader() => {
                                let slot = rt.node.proposer().next_slot();
                                // Else a leader prefers a column with no
                                // acceptor the cell holds slow (#361 (slow
                                // machines)).
                                phase2_column(slot, cols).or_else(|| {
                                    fast_column(rt.node.acceptors(), slot, |id| {
                                        rt.slow.contains(&id)
                                    })
                                })
                            }
                            _ => None,
                        };
                        rt.node.propose_in(entry.clone(), column, delegation)
                    }
                    Call::SetLeader { new, old, .. } => rt.node.propose_control_in(
                        Control::SetLeader {
                            new: *new,
                            old: *old,
                        },
                        delegation,
                    ),
                    Call::Truncate { leader, up_to, .. } => rt.node.propose_control_in(
                        Control::Truncate {
                            leader: *leader,
                            up_to: *up_to,
                        },
                        delegation,
                    ),
                };
                park_call(rt, result, call, self);
            }
            Input::Read(reads) => {
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
                    QuorumSystem::Grid { rows, acks, .. }
                        if rows >= 2
                            && acks == rows
                            && moonpool_buggify::buggify_fault_with_prob!(0.10) =>
                    {
                        moonpool_assertions::reachable!(
                            "grid: the driver overrides a quorum read's row"
                        );
                        Some(log_reads::next_row(ctx, rows))
                    }
                    // Else a row with no acceptor the cell holds slow (#361
                    // (slow machines)).
                    _ => fast_row(&b.config, ctx, |id| rt.slow.contains(&id)),
                });
                // The row the core will ask, resolved exactly as it resolves
                // it, for the audit's report of what served it.
                let row = basis.as_ref().and_then(|b| b.config.read_row(ctx, row));
                let opened = fold_head(&rt.node);
                if basis.is_some() {
                    rt.audit.quorum_read_opened(NodeId(self_id), ctx);
                }
                rt.node.quorum_read_in(ctx, row);
                rt.waiters.reads.park(
                    reads,
                    log_reads::ReadLimits::of(&self.tunables),
                    row,
                    opened,
                );
            }
            Input::Deliver(msg) => {
                trace_received(self_id, &msg);
                // Canary: a Prepare whose from_slot is below our floor is the
                // dangerous "campaign against a truncated acceptor" case.
                // Record it so the sweep can assert the interleaving stays
                // reachable once the acceptor floor guard is in place.
                if let Message::Prepare { from_slot, .. } = &msg
                    && *from_slot < rt.node.acceptor().first_slot()
                {
                    let floor = rt.node.acceptor().first_slot();
                    rt.audit
                        .prepare_below_floor(NodeId(self_id), *from_slot, floor);
                    tracing::info!(
                        node = self_id,
                        from_slot = from_slot.0,
                        floor = floor.0,
                        "prepare_below_floor"
                    );
                }
                if rt.read_only.is_some() && read_only_refuses(&msg) {
                    moonpool_assertions::reachable!(
                        "disk full: a read-only store refuses a Phase-2 vote"
                    );
                    tracing::debug!(node = self_id, reason = "disk_full", "phase2_refused");
                    return;
                }
                contest_overwrite(&mut rt.node, &mut rt.contested, &msg, self_id);
                rt.node.step(msg);
            }
            Input::Tick(ticks) => {
                self.read_only_beat(rt, ticks);
                self.beat_step(rt, ticks);
            }
        }
    }

    /// A read-only journal's beat (#360 (disk full)): it leaves read-only
    /// once `read_only_ticks` passed, with a fresh election timeout, and
    /// until then it never campaigns (a timeout a rival's `Prepare` asked
    /// for is set back to 0 here, before the core's tick).
    fn read_only_beat<S, A: Audit + Clone>(&self, rt: &mut JournalRt<S, A>, ticks: u64) {
        let Some(since) = rt.read_only else {
            return;
        };
        let self_id = self.self_id;
        if ticks.saturating_sub(since) >= self.tunables.read_only_ticks.max(1) {
            rt.read_only = None;
            rt.back_from_full = true;
            let timeout =
                draw_election_timeout(self.providers, self_id, self.tunables.election_timeout_base);
            rt.node.set_election_timeout(timeout);
            assert!(
                rt.node.election_timeout() > 0,
                "a writable journal campaigns again"
            );
            rt.audit.election_timeout_set(NodeId(self_id), timeout);
            moonpool_assertions::reachable!("disk full: a read-only store becomes writable");
            tracing::info!(node = self_id, reason = "disk_full", "journal_writable");
            return;
        }
        // It re-opened a follower, its timeout is held at 0 before every
        // core tick and a handoff to it is refused: it cannot lead.
        if rt.node.election_timeout() != 0 {
            rt.node.set_election_timeout(0);
        }
        assert!(!rt.node.is_leader(), "a read-only journal does not lead");
    }

    /// One journal's beat, stepped into its core (#613 (settle off the node
    /// loop): a beat of a flying journal waits in its backlog): the core's
    /// tick, the re-sends and their BUGGIFY sites, the leadership's give-up,
    /// the read expiry it owes the next settle, and the stranded-slot
    /// report. The tenant's handover is paced apart ([`NodeLoop::pace_handover`]),
    /// at the tick itself.
    fn beat_step<S, A: Audit + Clone>(&self, rt: &mut JournalRt<S, A>, ticks: u64) {
        let (self_id, tunables) = (self.self_id, self.tunables);
        let JournalRt {
            node,
            audit,
            waiters,
            match_resend,
            gc_resend,
            expiry,
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
            // Liveness under a stuck acceptor partition (#533 (move a stuck
            // round)): a grid round re-sent the budget's worth of beats
            // without a decision moves to another column. The budget is
            // driver policy (`partition_move_resends`, born buggified); the
            // core only counts and moves.
            move_stuck_rounds(node, tunables.partition_move_resends, self_id);
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
            if paros_observe::scenario::withhold_gc() {
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
        lp.offer_handoff(node);
        // Expire the reads whose quorum read is overdue (a row that never
        // answered whole, a fold that has not reached the watermark) and
        // answer the long-polls whose wait ran out. The early expiry, drawn
        // only while a read waits on its confirmation, takes the first exit
        // before the deadline. Kept shy: expiring most parked reads early
        // would stop confirmed reads from ever completing during the chaos
        // window. Silent in the recovery tail; its gate is the audit's
        // `read_expired` (the `early` leg).
        // They are answered in the settle, from the fold its batch reported,
        // before the next page is released (#550 (replica serves an
        // unreported fold page)).
        let expire_all =
            waiters.reads.has_confirming() && moonpool_buggify::buggify_fault_with_prob!(0.05);
        // A beat replaces a waiting expiry: the later tick's deadline holds.
        let owed = expiry.is_some_and(|(_, all)| all);
        *expiry = Some((ticks, expire_all || owed));
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
    }
}
