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

mod book;
pub(crate) mod boot;
mod calls;
mod config;
pub(crate) mod edge;
pub(crate) mod events;
mod flight;
mod handover;
mod journals;
pub(crate) mod log_reads;
mod matchmaking;
mod metered;
pub(crate) mod operator;
pub(crate) mod ready;
pub(crate) mod reply;
mod report;
pub(crate) mod scrub;
mod stop;
mod system;
pub(crate) mod transport;
mod tunables;

pub use config::{
    BootKind, BootRefusal, DriverTunables, LOAD_INTERVAL_FLOOR, MAX_READ_RECORDS, RunError,
    parse_addr,
};
pub use events::{command_hash, message_kind, registration_history_hash};
pub use journals::JournalStores;
pub use system::ControlPlan;
pub use tunables::BelowFloor;

use std::collections::{BTreeMap, BTreeSet};

use moonpool_core::{Providers, RandomProvider, SimulationError, SimulationResult, TimeProvider};
use paros_core::{
    ColocatedNode, Control, Delegation, Entry, Fingerprint, GcAck, JournalId, JournalIdentifier,
    MatchRefusal, MatchReply, MatchStep, MatchmakerGeneration, MatchmakerId, MatchmakerSet,
    Message, NodeId, NodeRole, Party, ProposeResult, ProxyId, QuorumSystem, ReconfigureReply,
    ReconfigureRequest, ReconfigurerPhase, ReconfigurerStep, Seq, Slot, TenantId, Value,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::machine::{CellMachine, ControlJournals};
use crate::metrics::{CallKind, Metrics};
use crate::rpc::{
    InspectRefusal, InspectTarget, MatchmakerClient, MatchmakersRefusal, ReadAck,
    ReconfigureMatchmakersAck, ReplySender, RetireAck, RetireRefusal, SetLeaderAck, TruncateAck,
    WriteAck, leader_from_proto, leader_uuid_from_proto,
};
use crate::storage::LogStorage;
use crate::system::{NodeStanding, RegistryEvent, SystemEvent};
use crate::tenant::TenantEvent;
use crate::transport::Transport;
use reply::Reply;

use calls::Call;
use edge::{NodeInbox, RpcEdge, edge_reporter};
use events::message_route;
use flight::{Flight, Input};
use handover::{HandoverDriver, TenantAudits};
use journals::{JournalRt, Journals, SingleStore, boot_journal};
use matchmaking::{
    MatchmakerLinks, folded_answer, report_match_step, send_outbox, send_reconfigure_requests,
    surface_matchmaking,
};
use metered::{Meter, journal_names};
use ready::{ClientWaiters, Taken, finish_ready, fold_head, take_ready};
use reply::maybe_duplicate;
use report::{Deltas, HandoffContext, draw_election_timeout, handoff_context, maintain};
use system::{ControlFollower, Followed, follow_local};
use transport::{LaneOpener, Outbound, peer_target};

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
        if !outbox.gc_requests.is_empty() && crate::scenario::withhold_gc() {
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
    fn answer<T>(&self, kind: Reply, waiter: ReplySender<T>, ack: T) {
        reply::answer(self.audit, NodeId(self.self_id), kind, waiter, ack);
    }

    /// Put handover requests for `tenant`'s matchmaker set on the
    /// matchmaker wire.
    fn send_reconfigure(
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
    fn start_reconfigurer(
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
    fn on_match_refusal(
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
    fn on_handover_straggler(
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
    fn pace_handover(&self, node: &ColocatedNode, handover: &mut HandoverDriver) {
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
            let again = !node.delegated_rounds().is_empty() && crate::scenario::resign_delegating();
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
fn tenant_audits<S, J: JournalStores>(
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
struct Shared<'a, P: Providers> {
    providers: &'a P,
    links: &'a MatchmakerLinks<P>,
    out: &'a Outbound,
    edge: &'a RpcEdge<P>,
    self_id: u64,
    tunables: DriverTunables,
    /// The machine's metrics (#366 (metrics)); off outside a machine.
    metrics: &'a Metrics,
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

    /// The settle tail for one journal (#613 (settle off the node loop)):
    /// take the core's batch, then either finish it at once (no durable
    /// write) or launch its flush and return, the journal flying until
    /// [`Shared::landed`] finishes it. Called only on a journal that does
    /// not fly: a flying journal's inputs wait in its backlog.
    async fn settle<S, A>(&self, rt: &mut JournalRt<S, A>)
    where
        S: LogStorage + Send + 'static,
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
        S: LogStorage + Send + 'static,
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
    async fn input<S, A>(&self, rt: &mut JournalRt<S, A>, input: Input) -> Result<(), RunError>
    where
        S: LogStorage + Send + 'static,
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
    async fn land<S, A>(&self, rt: &mut JournalRt<S, A>) -> Result<(), RunError>
    where
        S: LogStorage + Send + 'static,
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
    async fn landed<S, A>(
        &self,
        rt: &mut JournalRt<S, A>,
        landing: flight::Landing<S>,
    ) -> Result<(), RunError>
    where
        S: LogStorage + Send + 'static,
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
        S: LogStorage + Send + 'static,
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
    fn apply<S, A>(&self, rt: &mut JournalRt<S, A>, input: Input)
    where
        A: Audit + Clone,
    {
        assert!(rt.flight.is_none(), "nothing steps a flying journal's core");
        let self_id = self.self_id;
        match input {
            Input::Propose(call) => {
                let delegation = delegation_choice(&rt.node);
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
            Input::Read { req, reply } => {
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
                    &req,
                    reply,
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

/// Campaign before an `Accept` overwrites a different value this node
/// accepted at a lower ballot, or not (#376). The `Accept` proves that a
/// later leadership chose another value for the slot without this node's
/// copy. Campaigning first is a rare-but-valid choice (any node may campaign
/// at any time, at any fresh round; `ColocatedNode::campaign_above`): the raised promise refuses
/// the `Accept`, and the campaign's Phase 1 reads both values for the slot,
/// this node's own and the later one, where P2c must keep the higher ballot
/// (the gate "phase1: a promise quorum reports two different values for one
/// slot and P2c keeps the higher ballot"). Without it the `Accept` erases
/// the lower copy before any Phase 1 reads it, and the gate fired on about
/// 0.25% of seeds.
///
/// A plain choice, not a disruptive one: it costs one election, and only
/// when two values for one slot already exist, so it stays on in the
/// recovery tail, where most of a run's Phase-2 traffic is.
///
/// One contest per `(slot, held value, incoming value)` (#396). Under a
/// grid the campaign's Phase 1 closes on the contester's row, which need
/// not hold the other column-mate's value: P2c then keeps the contester's
/// own value, and the other column-mate contests back. Unbounded, two
/// column-mates of a `Grid{2,2}` dueled from round 12 to round 1652 and the
/// slot never chose (a liveness loss; safety held). The second overwriting
/// `Accept` of the same pair is taken as plain Paxos takes it.
fn contest_overwrite(
    node: &mut ColocatedNode,
    contested: &mut BTreeSet<(Slot, u64, u64)>,
    msg: &Message,
    self_id: u64,
) {
    let Message::Accept {
        ballot,
        slot,
        command,
        ..
    } = msg
    else {
        return;
    };
    if node.role() == NodeRole::Leader {
        return;
    }
    let Some(key) = node
        .acceptor()
        .record(*slot)
        .filter(|(held, value)| held < ballot && value != command)
        .map(|(_, value)| (*slot, value.fingerprint(), command.fingerprint()))
    else {
        return;
    };
    if contested.contains(&key) {
        moonpool_assertions::reachable!("an acceptor declines a repeat contest of one overwrite");
        tracing::info!(
            node = self_id,
            slot = slot.0,
            round = ballot.round,
            "overwrite_contest_declined"
        );
        return;
    }
    if !moonpool_buggify::buggify_with_prob!(1.0) {
        return;
    }
    let floor = node.acceptor().first_slot();
    contested.retain(|(contested_slot, _, _)| *contested_slot >= floor);
    contested.insert(key);
    assert!(
        contested.contains(&key),
        "a contest is recorded before it runs"
    );
    moonpool_assertions::reachable!(
        "an acceptor campaigns before an Accept overwrites a different value"
    );
    tracing::info!(
        node = self_id,
        slot = slot.0,
        round = ballot.round,
        "overwrite_contested"
    );
    let promised = node.acceptor().promised();
    node.campaign_above(*ballot);
    assert!(
        node.acceptor().promised() >= promised,
        "a contest never lowers the promise"
    );
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
/// tail. The freeze and the decree last a few ticks each, so their rates are
/// far above the bootstrap's (#484 (silent driver fault sites)): at 0.02 the
/// freeze's fired on 5 of 810 hunt seeds and the decree's on none; at 0.25,
/// with the early-handover scenario on twice as many seeds, on 15 and 6 of
/// 1,000.
fn abandon_early(phase: &ReconfigurerPhase) -> bool {
    let fired = match phase {
        ReconfigurerPhase::Stopping { .. } => moonpool_buggify::buggify_fault_with_prob!(0.25),
        ReconfigurerPhase::Bootstrapping { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.02)
        }
        ReconfigurerPhase::Deciding { .. } => moonpool_buggify::buggify_fault_with_prob!(0.25),
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
    // The handoff-storm scenario's location (#484 (silent driver fault
    // sites)): drawn after the shape's own, so the shape keeps its gate.
    let fired = fired || crate::scenario::hand_off();
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

/// The most doublings of a moved round's budget (#533 (move a stuck
/// round)): a round that moved `m` times waits
/// `after * 2^min(m, MOVE_BACKOFF_CAP)` re-sends before its next move. The
/// cap keeps the wait finite, so a round in a column that stays stuck
/// still moves. At the cap, production's budget (20 beats) becomes 1,280
/// beats. Not a tunable: the liveness argument of [`move_budget`] rests on
/// it.
const MOVE_BACKOFF_CAP: u64 = 6;
const _: () = assert!(
    MOVE_BACKOFF_CAP >= 1,
    "a moved round waits longer than a new one"
);
const _: () = assert!(
    MOVE_BACKOFF_CAP < 63,
    "the back-off shift stays inside a u64"
);

/// The re-sends a round that moved `moves` times waits before its next
/// move: `after` for a round that never moved, doubled for each move up to
/// [`MOVE_BACKOFF_CAP`] doublings, saturating.
///
/// Why: a move resets the round's tally, so the new column must collect
/// all its acks inside the budget. Under load the `Accept` flood makes the
/// round trip longer than a budget of one or two re-sends. Then the round
/// flips between columns without end, and no column decides it. Red on
/// main (CI run 38823114601, `replay-recipe 7816651028622460903
/// 216:14501162423016305769` at 629aa62): a `Grid { rows: 3, cols: 2 }`
/// leader with a budget of 1 to 3 moved its rounds 9,844 times, and the
/// apply walk stalled at slot 14 of 19 decided. With the doubling the same
/// recipe is green: each new column gets a longer window, so after a few
/// moves the round trip fits.
fn move_budget(after: u64, moves: u64) -> u64 {
    let shift = moves.min(MOVE_BACKOFF_CAP);
    assert!(shift <= MOVE_BACKOFF_CAP, "the back-off is capped");
    let budget = after.saturating_mul(1_u64 << shift);
    assert!(
        budget >= after,
        "a moved round never waits less than a new one"
    );
    budget
}

/// Move every colocated grid round re-sent at least its budget without a
/// decision to another column (#533 (move a stuck round)): the next column
/// round-robin, or on a BUGGIFY decision the one after it, so a slot may
/// skip a column. The budget is `after` for a round that never moved, and
/// it doubles with each move ([`move_budget`]). A no-op on a node that is
/// not the leader and under a majority or a flexible split. Drawn on the
/// node loop.
fn move_stuck_rounds(node: &mut ColocatedNode, after: u64, self_id: u64) {
    let QuorumSystem::Grid { cols, .. } = node.acceptors().quorum_system() else {
        return;
    };
    if cols < 2 || !node.is_leader() {
        return;
    }
    let stuck: Vec<(paros_core::Slot, usize)> = node
        .proposer()
        .rounds()
        .iter()
        .filter(|(_, round)| round.proxy().is_none() && round.resends() >= after)
        .filter(|(_, round)| {
            let due = round.resends() >= move_budget(after, round.moves());
            if !due {
                // Only a moved round has a budget above `after`.
                assert!(
                    round.moves() > 0,
                    "only a moved round waits past the budget"
                );
                moonpool_assertions::reachable!("grid: the driver backs off a moved round");
            }
            due
        })
        .filter_map(|(slot, round)| round.column().map(|column| (*slot, column)))
        .collect();
    for (slot, column) in stuck {
        let step = if cols >= 3 && moonpool_buggify::buggify_fault_with_prob!(0.5) {
            moonpool_assertions::reachable!(
                "grid: the driver skips a column when it moves a round"
            );
            2
        } else {
            1
        };
        let next = (column + step) % cols;
        assert!(next != column, "a stuck round moves to another column");
        if node.move_round(slot, next) {
            moonpool_assertions::reachable!("grid: the driver moves a stuck round");
            tracing::info!(
                node = self_id,
                slot = slot.0,
                from = column,
                to = next,
                "round_moved"
            );
        }
    }
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

/// The members of `node`'s configuration the cell holds slow (#361 (slow
/// machines)), from the registry fold: empty outside a grid, where no
/// column or row choice reads them, and without a control plan.
fn slow_members(
    node: &ColocatedNode,
    registry: Option<&crate::system::Registry>,
) -> BTreeSet<NodeId> {
    let (QuorumSystem::Grid { .. }, Some(registry)) = (node.acceptors().quorum_system(), registry)
    else {
        return BTreeSet::new();
    };
    let slow: BTreeSet<NodeId> = node
        .acceptors()
        .members()
        .iter()
        .copied()
        .filter(|id| registry.is_slow(*id))
        .collect();
    assert!(
        slow.iter().all(|id| node.acceptors().contains(*id)),
        "a slow member is a member"
    );
    slow
}

/// The column of a grid this leader's Phase 2 goes to when the core's own
/// column holds an acceptor the cell holds slow (#361 (slow machines)) and
/// another column holds none: the first such column after the core's. Every
/// column is a Phase-2 quorum, so the choice is always valid; it only
/// prefers fast acceptors. `None` keeps the core's choice.
fn fast_column(
    config: &paros_core::AcceptorConfig,
    slot: paros_core::Slot,
    is_slow: impl Fn(NodeId) -> bool,
) -> Option<usize> {
    let QuorumSystem::Grid { cols, .. } = config.quorum_system() else {
        return None;
    };
    let own = config.column_of(slot)?;
    let holds_slow = |column: usize| {
        config
            .phase2_addressees(Some(column))
            .into_iter()
            .any(&is_slow)
    };
    if !holds_slow(own) {
        return None;
    }
    let column = (1..cols)
        .map(|step| (own + step) % cols)
        .find(|column| !holds_slow(*column))?;
    assert!(column != own, "a fast column is another column");
    assert!(column < cols, "a fast column is one the grid has");
    moonpool_assertions::reachable!("grid: a round's column avoids a slow acceptor");
    Some(column)
}

/// The row of a grid a quorum read asks when the core's own row holds an
/// acceptor the cell holds slow (#361 (slow machines)) and another row
/// holds none: the twin of [`fast_column`], under the same condition as the
/// row override (a full column, so every row is a read quorum).
fn fast_row(
    config: &paros_core::AcceptorConfig,
    ctx: u64,
    slow: impl Fn(NodeId) -> bool,
) -> Option<usize> {
    let QuorumSystem::Grid { rows, acks, .. } = config.quorum_system() else {
        return None;
    };
    if rows < 2 || acks != rows {
        return None;
    }
    let own = config.row_of(ctx)?;
    let holds_slow = |row: usize| config.phase1_addressees(Some(row)).into_iter().any(&slow);
    if !holds_slow(own) {
        return None;
    }
    let row = (1..rows)
        .map(|step| (own + step) % rows)
        .find(|row| !holds_slow(*row))?;
    assert!(row != own, "a fast row is another row");
    assert!(
        config.admits_read_row(Some(row)),
        "a fast row is a read row"
    );
    moonpool_assertions::reachable!("grid: a quorum read's row avoids a slow acceptor");
    Some(row)
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

/// What the per-batch observers read of a core (#613 (settle off the node
/// loop)): an input that moves any of it ends a group-commit batch
/// ([`Shared::step_backlog`]). The role, ballot and leader (`maintain`'s
/// role report and election clock), the open matchmaking campaign and
/// membership probe (`surface_matchmaking`), and the counters `maintain`
/// reports under the core's ballot. Inputs that move none of it (the
/// steady traffic of one leadership) still share one flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Phase {
    role: NodeRole,
    ballot: paros_core::Ballot,
    leader: Option<NodeId>,
    campaign: Option<paros_core::Ballot>,
    probe: Option<paros_core::Ballot>,
    reasks: u64,
    quorum_lost: u64,
    watermark_fills: u64,
    repair: paros_core::RepairCounters,
    handoff: paros_core::HandoffCounters,
    generation: u64,
    needs_timeout: bool,
}

impl Phase {
    fn of(node: &ColocatedNode) -> Self {
        let phase = Self {
            role: node.role(),
            ballot: node.ballot(),
            leader: node.leader(),
            campaign: node
                .matchmaking_role()
                .map(paros_core::matchmaking::Matchmaking::ballot),
            probe: node
                .membership_probe()
                .map(paros_core::matchmaking::MembershipProbe::ballot),
            reasks: node.matchmaking_timeouts(),
            quorum_lost: node.quorum_lost_step_downs(),
            watermark_fills: node.watermark_fills(),
            repair: node.repair_counters(),
            handoff: node.handoff_counters(),
            generation: node.matchmaker_set().map_or(0, |set| set.generation.0),
            needs_timeout: node.needs_election_timeout(),
        };
        // A probe and a campaign never coexist (`surface_matchmaking`).
        assert!(
            phase.campaign.is_none() || phase.probe.is_none(),
            "a phase holds a campaign or a probe, never both"
        );
        phase
    }
}

/// Hold `input` back while its journal's batch flies (#613 (settle off the
/// node loop)): in arrival order, except that a beat replaces the one
/// already waiting (a slow node's clock, never a burst of ticks).
fn hold(backlog: &mut Vec<Input>, input: Input) {
    let before = backlog.len();
    if let Input::Tick(ticks) = input
        && let Some(waiting) = backlog.iter_mut().find_map(|held| match held {
            Input::Tick(at) => Some(at),
            _ => None,
        })
    {
        assert!(*waiting <= ticks, "a later beat replaces an earlier one");
        *waiting = ticks;
        assert!(backlog.len() == before, "a replaced beat holds no more");
        return;
    }
    backlog.push(input);
    assert!(
        backlog.len() == before + 1,
        "a held input waits in the backlog"
    );
}

/// Park `call` on the slot its proposal took (`result`), with the ballot
/// it was proposed under, to be answered when that slot applies, or
/// redirect it at once when this node does not lead.
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
            // The proposal's ballot: only a decision at it is this call's
            // (`answer_applied_calls`).
            let ballot = rt.node.ballot();
            assert!(rt.node.is_leader(), "only a leader parks a call");
            rt.waiters
                .pending
                .entry(slot)
                .or_default()
                .push((ballot, call));
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
    transport: Transport,
    shutdown: CancellationToken,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    S: LogStorage + Send + 'static,
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
        transport,
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
/// The matchmaker plane, the proxy leaders and the replica tier are shared
/// by every journal that names them (#190, #193): each journal's
/// [`paros_core::Config`] says whether it delegates to proxies and teaches
/// replicas, and the messages carry its identifier.
///
/// `node` is this process's node id, named by the caller (no id has a
/// default, §3.8): every journal's [`paros_core::Config`] must carry it, and
/// so must `system`'s plan.
///
/// `system` opts the node into the **control journals** (#189, #210,
/// [`ControlPlan`]): it follows the cell control journal (the node registry)
/// and the control journal of every tenant the cell hosts (its own copy where
/// it serves one, from a seed otherwise), starts a journal a tenant creates
/// naming it and stops one it tombstones, opens a lane to every node the
/// registry admits, and refuses a peer message from a node the registry does
/// not have in the pool. Such a node may serve no journal
/// at all (a joiner boots with none) and exits only on a fault. `None` is
/// the static deployment: a fixed list over a fixed pool.
///
/// `machine` is a machine of a cell (#246, #277, #474), `None` off a
/// machine: its control journals, which `Inspect` reports, and what it
/// answers beside its journals — a founding member its vote in the cell
/// decree ([`crate::machine::FormedCell`]), an admitted machine its admission
/// ([`crate::machine::AdmittedMachine`]).
///
/// # Errors
///
/// As [`run_node`].
///
/// # Panics
///
/// When a founding member's plan names a founding member `members` has no
/// lane to (a programmer error: a machine passes its plan's members).
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
    system: Option<ControlPlan>,
    machine: Option<CellMachine>,
    tunables: DriverTunables,
    transport: Transport,
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
    let cell: Option<ControlJournals> = machine.as_ref().map(CellMachine::control_journals);
    // The machine's metrics (#366 (metrics)): off on a node outside a
    // machine.
    let metrics: Metrics = machine
        .as_ref()
        .map(|machine| machine.facts().metrics.clone())
        .unwrap_or_default();
    // The cell every peer batch carries and every inbound one must name
    // (#216): `0` on a deployment without a cell plan.
    let cell_id = cell.map_or(0, |cell| cell.cell_id);
    assert!(cell.is_none() || cell_id != 0, "a cell plan names its cell");
    // No identifier is fixed (§3.8): the deployment names its control journals —
    // the cell's and the fleet tenant's from the cell plan, the system journals a
    // `ControlPlan` follows — and every other journal is a user's.
    let control: BTreeSet<JournalIdentifier> = cell
        .iter()
        .flat_map(|cell| {
            std::iter::once(cell.cell)
                .chain(cell.fleet)
                .chain(cell.election)
        })
        .chain(system.iter().map(|plan| plan.cell))
        .filter(|journal| journal.is_set())
        .collect();
    // The node-level audit: what no single journal owns (the edge's
    // rejections, a peer lane's delivery failures, a refused journal id, the
    // system journals' folds), named by the opener rather than borrowed
    // from a journal (#243).
    let node_audit = stores.node_audit();
    // A node exits once it has nothing left to serve — on a static
    // deployment. A node that follows the system journals runs on with none
    // (a joiner waits for a tenant to name it) and exits only when a
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
    let mut journals: Journals<J::Store, J::Audit> =
        Journals::new(control).preferring(ids.first().copied());
    for &id in &ids {
        open_journal(&providers, &mut stores, &mut journals, id, 0, &tunables).await;
    }
    for journal in journals.take_quarantined() {
        stores.quarantined(journal);
    }
    if journals.exhausted() && !journals.waits_for_space() && (!follows || journals.stranded()) {
        return journals.exit();
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
    // The clean stop (#560 (clean Ctrl-C shutdown)): cancelled when
    // `shutdown` fires, while the loop still serves, and with the
    // incarnation. The tasks that hold a leadership of their own (the cell
    // coordinator) hand it on when it fires.
    let stopping = incarnation_shutdown.child_token();
    // Who the stop's log lines name: the machine's name, never an id (the
    // names rule), or the node on a deployment outside a machine.
    let stop_name: String = machine.as_ref().map_or_else(
        || format!("node {}", node.0),
        |machine| machine.facts().name.clone(),
    );

    // The RPC edge: a moonpool-rpc runtime listening on this node's address,
    // polled by the loop below. Its handlers are the typed queues the loop
    // selects on, so the loop remains the sole owner of every journal's core.
    let me = Party::Node(NodeId(self_id));
    let edge = RpcEdge::listen(&providers, &local_addr, "node", &tunables, &transport).await?;
    // A founding member's machine interface (#404 (static vs dynamic
    // endpoints)): a dynamic group serving `Deliver` for this incarnation,
    // published in its `IdentifyAck` and kept in the registry by `node_id`.
    // It lives as long as this loop; a static deployment serves none.
    let machine_group = machine
        .as_ref()
        .map(|_| {
            edge.handle()
                .register_group::<crate::rpc::methods::MachineInterface>(
                    moonpool_rpc::AccessClass::Public,
                )
                .map_err(|e| {
                    RunError::Infra(SimulationError::InvalidState(format!(
                        "register the machine interface: {e}"
                    )))
                })
        })
        .transpose()?;
    // The group's reference as `Inspect` hands it out (#404): a frontend
    // learns the reference of each server it inspects.
    let machine_interface: Vec<u8> = machine_group
        .as_ref()
        .map(|group| crate::rpc::published(&group.interface_ref()))
        .unwrap_or_default();
    assert_eq!(
        machine_interface.is_empty(),
        machine_group.is_none(),
        "only a machine publishes an interface"
    );
    let machine = machine.map(|mut machine| {
        if let Some(group) = &machine_group {
            let facts = machine.facts().serving(edge.handle()).with_interface(group);
            *machine.facts_mut() = facts;
        }
        machine
    });
    let mut rpc = NodeInbox::serve(
        &providers,
        &edge,
        &tunables,
        me,
        cell_id,
        edge_reporter(&node_audit, me),
        incarnation_shutdown.clone(),
        machine_group.as_ref(),
    )?;

    // The system journals' follower (#189), and the inbox its remote reads
    // answer on. Its sender stays alive with the follower, so the arm below
    // simply never fires on a static deployment.
    let fixed: Vec<NodeId> = members
        .iter()
        .chain(replicas.iter())
        .map(|(id, _)| *id)
        .collect();
    // A machine dials its peers by their advertised addresses, names
    // resolved at dial time (#257): its control-journal seeds too.
    let names = machine
        .as_ref()
        .map_or_else(crate::Names::literal, |machine| {
            machine.facts().names.clone()
        });
    let (mut follower, mut follow_answers, _follow_open) = if let Some(plan) = &system {
        let (follower, inbox) = ControlFollower::new(
            plan,
            edge.handle(),
            &names,
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
    // What the stores host beside the journals on this edge: a founding
    // member's matchmaker sets (#190).
    stores.host_on_edge(
        &providers,
        &rpc_handle,
        machine_group.as_ref(),
        (&tunables, cell_id),
        incarnation_shutdown.clone(),
    )?;
    // A machine's vote is final: it answers the cell decree from its record
    // while it serves its cell (#277). A founding member's peers are dialed
    // where the registry fold says (#349), from the plan's addresses on.
    let mut peer_book = match &machine {
        Some(CellMachine::Founded { cell, .. }) => Some(book::PeerBook::new(cell)),
        _ => None,
    };
    // A founding member that came back at another address than the one its
    // cell knows (#390) holds its journals' clocks until its registration
    // ends. Its peers send to the address they know, so it hears none of
    // them, and its campaigns only raise their promises above the sitting
    // leader's: the cell control journal that would register it stalls.
    // Held, it still answers every message that reaches it.
    let mut unregistered: Option<watch::Receiver<bool>> = None;
    // Whether the cell coordinator's task ended (#560 (clean Ctrl-C
    // shutdown)): a clean stop waits for it to hand its term on. `None` on
    // a node that runs none.
    let mut coordinator_ended: Option<watch::Receiver<bool>> = None;
    // The instant since which the registry's pool is owed to the live journals
    // (#189), and whether this incarnation holds it a while (#387,
    // `crate::scenario::ADMIT_LATE`; drawn only where a registry is followed).
    let mut admission_owed: Option<std::time::Duration> = None;
    let late_admitter = system.is_some() && crate::scenario::late_admitter();
    // The books an admitted machine's cache is offered (#505 (stranded
    // admitted machine)): its follow's folds, and a book its cell
    // coordinator handed it when its fold was stuck.
    let mut offered_books = None;
    if let Some(CellMachine::Admitted { machine, cache }) = machine.clone() {
        offered_books = Some(cache.subscribe());
        // An admitted machine answers its admission beside the journals
        // placed on it (#474), follows the registry for its cache (#211)
        // and registers the address it advertises now (#349).
        machine.serve_beside(
            &providers,
            &rpc_handle,
            &tunables,
            cache,
            incarnation_shutdown.clone(),
        )?;
    }
    if let Some(CellMachine::Founded {
        cell: formed,
        cache,
    }) = machine
    {
        // Every founding member answers the cell's views (#399).
        crate::machine::views::spawn(
            &providers,
            &rpc_handle,
            &formed,
            &tunables,
            incarnation_shutdown.clone(),
        )?;
        // It asks the coordinator to register the address it advertises
        // now (#349): the cell may know another one.
        if let Some(election) = formed
            .plan
            .election
            .is_set()
            .then_some(formed.plan.election)
        {
            // A lone founding member is its own quorum: it needs no peer.
            let moved = formed.plan.members.len() > 1
                && formed
                    .founders()
                    .iter()
                    .any(|(id, addr)| *id == formed.facts.node_id && *addr != formed.facts.addr);
            let ended = crate::machine::register::spawn(
                &providers,
                &rpc_handle,
                crate::machine::register::Registration {
                    facts: formed.facts.serving(&rpc_handle),
                    cell_id: formed.plan.cell_id,
                    election,
                    book: crate::machine::with_own(
                        &formed.founders(),
                        formed.facts.node_id,
                        &formed.facts.addr,
                    ),
                },
                &tunables,
                incarnation_shutdown.clone(),
            );
            unregistered = moved.then_some(ended);
        }
        // Every founding member campaigns for the cell coordinator (#240):
        // a moved one once its registration ended (#506 (renamed founder
        // not registered)).
        coordinator_ended = Some(crate::machine::coordinator::spawn(
            &providers,
            &rpc_handle,
            &formed,
            &tunables,
            node_audit.call_observer(),
            unregistered.clone(),
            (incarnation_shutdown.clone(), stopping.clone()),
        ));
        formed.serve(
            &providers,
            &rpc_handle,
            &tunables,
            cache.subscribe(),
            incarnation_shutdown.clone(),
        )?;
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
        names: names.clone(),
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
    // their answers come back through, each tagged with its tenant (#190).
    // Empty on plain Multi-Paxos.
    let (match_reply_tx, mut match_replies) =
        mpsc::channel::<(TenantId, MatchReply)>(tunables.peer_inbox_capacity);
    let matchmaker_clients = matchmakers
        .into_iter()
        .map(|(id, addr)| {
            Ok((
                id,
                MatchmakerClient::named(edge.handle(), names.clone(), peer_target(&addr)?),
            ))
        })
        .collect::<SimulationResult<BTreeMap<_, _>>>()?;
    let (gc_ack_tx, mut gc_acks) = mpsc::channel::<(TenantId, GcAck)>(tunables.peer_inbox_capacity);
    let (reconfigure_reply_tx, mut reconfigure_replies) =
        mpsc::channel::<(TenantId, ReconfigureReply)>(tunables.peer_inbox_capacity);
    let links = MatchmakerLinks {
        clients: std::sync::Mutex::new(matchmaker_clients),
        replies: match_reply_tx,
        gc_acks: gc_ack_tx,
        reconfigure_replies: reconfigure_reply_tx,
        timeout: tunables.delivery_timeout,
        shutdown: incarnation_shutdown.clone(),
        cell_id,
    };
    // The matchmaker-set handovers (#125), one per tenant (#190): the
    // reconfigurer plus the two clocks that pace it, idle until a client asks
    // or until this node meets a frozen registry nobody finished replacing.
    let mut handovers: BTreeMap<TenantId, HandoverDriver> = BTreeMap::new();
    // A handover pages its bootstrap by the matchmakers' page (#439).
    let registry_page = tunables.registry_page;
    // Set by an accepted operator `Retire`: the node exits at its next tick,
    // after the ack had a beat to leave.
    let mut retiring: Option<JournalIdentifier> = None;

    let out = Outbound::new(peer_queues, proxy_queues, learners, me);
    let shared = Shared {
        providers: &providers,
        links: &links,
        out: &out,
        edge: &edge,
        self_id,
        tunables,
        metrics: &metrics,
    };
    let time = providers.time().clone();
    let meter = Meter {
        metrics: &metrics,
        time: &time,
        audit: &node_audit,
        node: NodeId(self_id),
    };

    // The clean stop in progress, once `shutdown` fired (#560 (clean Ctrl-C
    // shutdown)).
    let mut halt: Option<stop::Stop> = None;
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
            (journal, landing) = journals.next_landing() => {
                // A journal's flush landed (#613 (settle off the node loop)):
                // finish its batch, step what it held back, and settle that
                // as the next batch.
                let Some(rt) = journals.live.get_mut(&journal) else { continue };
                if let Err(fault) = shared.landed(rt, landing).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                }
            }
            Some((req, reply)) = rpc.write.recv() => {
                // A journal `Write` (#204) → the named journal's leader. The
                // leader proposes it into the next slot without judging it:
                // the writer, the position and a retry are all the journal
                // state machine's, at apply. The reply is held until the
                // slot applies and answered with that verdict; a non-leader
                // redirects immediately.
                if halt.is_some() {
                    // A stopping machine takes no new call (#560 (clean
                    // Ctrl-C shutdown)): retry elsewhere.
                    stop::refused("write");
                    shared.with(&node_audit).answer(
                        Reply::Redirect,
                        reply,
                        WriteAck { stopping: true, ..WriteAck::default() },
                    );
                    continue;
                }
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
                let reply = meter.call(
                    journal,
                    || journal_names(follower.as_ref(), cell.as_ref(), journal),
                    (CallKind::Write, metered::request_bytes(&req.records)),
                    reply,
                    metered::write_answer,
                );
                // The batch limits (#241, §2.7) are judged here, at the edge:
                // a batch over them never reaches consensus.
                if let Some(ack) = calls::batch_too_large(&req.records, &tunables) {
                    tracing::debug!(node = self_id, records = req.records.len(), "write_too_large");
                    shared.with(&node_audit).answer(Reply::Write, reply, ack);
                    continue;
                }
                // Admission control (#360 (disk full)): a leader whose
                // acceptors fall behind answers `busy`, and the client
                // retries the same write.
                if let Some(ack) = calls::busy(&rt.node, &tunables) {
                    moonpool_assertions::reachable!("admission: a leader answers a write busy");
                    tracing::debug!(node = self_id, journal = %journal, "write_busy");
                    shared.with(&node_audit).answer(Reply::Redirect, reply, ack);
                    continue;
                }
                let entry = Entry {
                    leader: leader_uuid_from_proto(req.leader),
                    seq: Seq(req.seq),
                    records: req.records.into_iter().map(Value).collect(),
                };
                rt.slow = slow_members(
                    &rt.node,
                    follower.as_ref().map(system::ControlFollower::registry),
                );
                let outcome = shared.input(rt, Input::Propose(Call::Write { entry, reply })).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.set_leader.recv() => {
                // A journal `SetLeader` (#241): a compare-and-set decided
                // into the log and judged at apply, like a `Write`.
                if halt.is_some() {
                    stop::refused("set_leader");
                    shared.with(&node_audit).answer(
                        Reply::Redirect,
                        reply,
                        SetLeaderAck { stopping: true, ..SetLeaderAck::default() },
                    );
                    continue;
                }
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
                let reply = meter.call(
                    journal,
                    || journal_names(follower.as_ref(), cell.as_ref(), journal),
                    (CallKind::SetLeader, 0),
                    reply,
                    metered::set_leader_answer,
                );
                let new = leader_uuid_from_proto(req.new);
                let old = leader_from_proto(req.old);
                let outcome = shared.input(rt, Input::Propose(Call::SetLeader { new, old, reply })).await;
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
                if halt.is_some() {
                    stop::refused("truncate");
                    shared.with(&node_audit).answer(
                        Reply::Redirect,
                        reply,
                        TruncateAck { stopping: true, ..TruncateAck::default() },
                    );
                    continue;
                }
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
                let reply = meter.call(
                    journal,
                    || journal_names(follower.as_ref(), cell.as_ref(), journal),
                    (CallKind::Truncate, 0),
                    reply,
                    metered::truncate_answer,
                );
                let up_to = Seq(req.up_to);
                let leader = leader_uuid_from_proto(req.leader);
                let outcome = shared.input(rt, Input::Propose(Call::Truncate { leader, up_to, reply })).await;
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
                if halt.is_some() {
                    stop::refused("read");
                    shared.with(&node_audit).answer(
                        Reply::ReadUnserved,
                        reply,
                        ReadAck { stopping: true, ..ReadAck::default() },
                    );
                    continue;
                }
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
                let reply = meter.call(
                    journal,
                    || journal_names(follower.as_ref(), cell.as_ref(), journal),
                    (CallKind::Read, 0),
                    reply,
                    metered::read_answer,
                );
                rt.slow = slow_members(
                    &rt.node,
                    follower.as_ref().map(system::ControlFollower::registry),
                );
                let outcome = shared.input(rt, Input::Read { req, reply }).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((journal, msg)) = rpc.deliver.recv() => {
                // A peer Paxos message → its journal's single input router.
                // The same `paros_core::Message` is sent and received (no
                // DTO). The sender was acknowledged when the message entered
                // the inbox, so nothing here answers it. A message for a
                // journal this node does not run now (quarantined, down, or
                // never served) is dropped: the peer's re-send repairs it.
                // A node sends in its own name only (`Outbound::resolve`
                // asserts a fan-out never addresses its sender), so a
                // message in this node's own name is a transport echo: a
                // pooled peer id the registry still maps to this machine's
                // address, a wiped machine's old id whose successor took
                // the address (#246, #423). The core would follow it (a
                // leader that steps its own beat demotes itself), so it is
                // dropped whole before the core sees it. Witness
                // 3102364801525731724 on f8e629f (PR #679 (clean Ctrl-C shutdown)): "placement: a journal
                // with a machine to go to leaves a draining one after
                // chaos".
                if events::message_sender(&msg) == Some(Party::Node(NodeId(self_id))) {
                    moonpool_assertions::reachable!(
                        "node: a message in this node's own name is ignored"
                    );
                    tracing::info!(node = self_id, journal = %journal, kind = events::message_kind(&msg), "self_message_ignored");
                    continue;
                }
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
                // An ack at the inbox, whatever the core makes of it: what
                // refills a leader's `CheckQuorum` window, and what the
                // deposed-leader oracle measures its clock from.
                if let Message::HeartbeatAck { from, ballot, .. } = &msg {
                    rt.audit.heartbeat_ack_received(NodeId(self_id), *from, *ballot);
                }
                // A stopping node's handed-off leadership moved once its
                // successor beats (#560 (clean Ctrl-C shutdown)).
                if let (Some(halt), Message::Heartbeat { from, ballot, .. }) = (halt.as_mut(), &msg)
                    && halt.heard(journal, *from, *ballot)
                {
                    moonpool_assertions::reachable!("stop: a successor took a stopping leader's journal");
                    tracing::info!(machine = %stop_name, journal = %journal, to = from.0, "stop_handoff_taken");
                }
                let outcome = shared.input(rt, Input::Deliver(msg)).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((tenant, reply)) = match_replies.recv() => {
                // A matchmaker's answer to this candidate's registration (#120):
                // fold it into the open matchmaking phase of the journal it
                // echoes (#190); a quorum closes the phase and opens Phase 1
                // in the same step.
                let journal = JournalIdentifier::new(tenant, reply.journal);
                let audits = tenant_audits(&journals, &stores, tenant);
                let Some(rt) = journals.live.get_mut(&journal) else { continue };
                if let Err(fault) = shared.land(rt).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                let (matchmaker, ballot) = (reply.matchmaker, reply.ballot);
                // The duplicate seam (the mirror of the matchmaker driver's
                // reply drop): what it tests is the idempotency the
                // registration path claims — a matchmaker already counted
                // never re-opens the quorum.
                maybe_duplicate(NodeId(self_id), Reply::Match, &links.replies, &(tenant, reply.clone()));
                tracing::info!(
                    node = self_id,
                    matchmaker = matchmaker.0,
                    round = ballot.round,
                    registered = matches!(reply.outcome, paros_core::MatchOutcome::Registered { .. }),
                    "match_reply_received"
                );
                let folded = folded_answer(&reply);
                let step = rt.node.on_match_reply(reply);
                report_match_step(&rt.audit, self_id, matchmaker, ballot, folded, &step);
                let handover = handovers.entry(tenant).or_insert_with(|| HandoverDriver::new(NodeId(self_id), registry_page));
                shared.with(&audits).on_match_refusal(&rt.node, handover, matchmaker, &step);
                // A campaign a chosen successor set superseded asks that set
                // at once, not one election timeout later: its candidate
                // already adopted the set, and the old one never answers it
                // again. Without it, a journal waits an election timeout for
                // every generation a handover chains (witness
                // 137495999355562437 on PR #626 (Accepts before own sync): a
                // one-member journal on a draining machine waited three
                // ~10 s timeouts after chaos, and its move missed the settle).
                if matches!(step, MatchStep::Superseded { .. }) {
                    moonpool_assertions::reachable!(
                        "matchmaking: a superseded campaign asks the new set at once"
                    );
                    rt.node.campaign_above(ballot);
                }
                shared.settle(rt).await;
            }
            Some((tenant, ack)) = gc_acks.recv() => {
                // A matchmaker's answer to this leader's GC request (#123):
                // fold it into the journal it echoes (#190); a quorum makes
                // the floor effective and names the retirable acceptors
                // (reported in the step).
                let journal = JournalIdentifier::new(tenant, ack.journal);
                let Some(rt) = journals.live.get_mut(&journal) else { continue };
                if let Err(fault) = shared.land(rt).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                maybe_duplicate(NodeId(self_id), Reply::GcAck, &links.gc_acks, &(tenant, ack.clone()));
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
                shared.settle(rt).await;
            }
            Some((tenant, reply)) = reconfigure_replies.recv() => {
                // A matchmaker's answer to this node's handover step of
                // `tenant`'s set (#125, #190).
                let Some(handover) = handovers.get_mut(&tenant) else { continue };
                let audits = tenant_audits(&journals, &stores, tenant);
                let matchmaker = reply.matchmaker();
                maybe_duplicate(
                    NodeId(self_id),
                    Reply::MatchmakerReconfigure,
                    &links.reconfigure_replies,
                    &(tenant, reply.clone()),
                );
                shared.with(&audits).on_handover_straggler(tenant, handover, &reply);
                let step = handover.on_reply(reply.clone());
                audits.reconfigurer_step(NodeId(self_id), matchmaker, &reply, &step);
                tracing::info!(
                    node = self_id,
                    tenant = tenant.0,
                    matchmaker = matchmaker.0,
                    reply = events::reconfigure_reply_kind(&reply),
                    step = ?step,
                    "reconfigurer_step"
                );
                if let ReconfigurerStep::Preempted { .. } = &step {
                    let ticks = providers
                        .random()
                        .random_range(1..tunables.reconfigure_backoff_max_ticks.max(1) + 1);
                    handover.back_off(ticks);
                    audits.reconfigurer_backoff(NodeId(self_id), ticks);
                    tracing::info!(node = self_id, ticks, "reconfigurer_backoff");
                }
                shared.with(&audits).send_reconfigure(tenant, handover.take_requests());
                // The chosen set is authoritative the instant it is chosen —
                // every journal of the tenant adopts it before its
                // publication completes.
                if let ReconfigurerStep::Chosen { successor }
                | ReconfigurerStep::Done { successor }
                | ReconfigurerStep::Superseded { successor } = &step
                {
                    let served: Vec<JournalIdentifier> = journals
                        .live
                        .iter()
                        .filter(|(journal, rt)| journal.tenant == tenant && rt.node.config().has_matchmakers())
                        .map(|(journal, _)| *journal)
                        .collect();
                    for journal in served {
                        let Some(rt) = journals.live.get_mut(&journal) else { continue };
                        if let Err(fault) = shared.land(rt).await {
                            journals.fold(journal, Err(fault), ticks, self_id)?;
                            continue;
                        }
                        rt.node.learn_matchmakers(successor);
                        shared.settle(rt).await;
                    }
                }
            }
            Some((req, reply)) = rpc.reconfigure_matchmakers.recv() => {
                // No settle tail: this arm drives the reconfigurer, never the
                // core, so there is no `Ready` batch to drain.
                // A matchmaker-set reconfiguration (#125): any node may drive
                // it. Refusable like every operator request; a started
                // handover runs to completion on this node's own cadence.
                // It hands over the set of the tenant the request names
                // (#501 (matchmaker sets off the founders)), through a
                // journal of that tenant this node serves. No tenant has a
                // default (#439 (operator calls name their journal)): an
                // unset one, or one this node serves no journal of, is
                // refused `unknown_tenant`; a tenant whose journals run
                // without matchmakers, `no_matchmakers`.
                let asked = TenantId(req.tenant);
                let serves = asked.0 != 0 && journals.live.keys().any(|journal| journal.tenant == asked);
                let journal = journals
                    .live
                    .iter()
                    .find(|(journal, rt)| asked.0 != 0 && journal.tenant == asked && rt.node.config().has_matchmakers())
                    .map(|(journal, _)| *journal);
                let Some(journal) = journal else {
                    let refusal = if serves {
                        MatchmakersRefusal::NoMatchmakers
                    } else {
                        moonpool_assertions::reachable!("operator: a matchmaker handover names a tenant the node does not serve");
                        MatchmakersRefusal::UnknownTenant
                    };
                    tracing::info!(node = self_id, accepted = false, refusal = refusal.label(), "reconfigure_matchmakers_acked");
                    shared.with(&node_audit).answer(
                        Reply::ReconfigureMatchmakers,
                        reply,
                        ReconfigureMatchmakersAck {
                            accepted: false,
                            refusal: refusal.label().to_string(),
                            generation: None,
                        },
                    );
                    continue;
                };
                let tenant = journal.tenant;
                assert!(tenant == asked, "a handover runs for the tenant asked");
                assert!(tenant.0 != 0, "a handover names its tenant");
                let audits = tenant_audits(&journals, &stores, tenant);
                let Some(rt) = journals.live.get(&journal) else { continue };
                let lp = shared.with(&audits);
                let handover = handovers.entry(tenant).or_insert_with(|| HandoverDriver::new(NodeId(self_id), registry_page));
                let target: Vec<MatchmakerId> = req.members.iter().copied().map(MatchmakerId).collect();
                let refusal = operator::reconfigure_matchmakers(&rt.node, handover, &target, |m| {
                    links.knows(*m)
                })
                .err();
                let accepted = refusal.is_none();
                let generation = rt.node.matchmaker_set().map_or(0, |set| set.generation.0);
                if let Some(current) = rt.node.matchmaker_set()
                    && accepted
                {
                    lp.start_reconfigurer((tenant, handover), current, &target, false);
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
                // retirement names the journal it leaves (#474): it leaves
                // that journal, and keeps serving the others (#188). No
                // journal has a default (#439 (operator calls name their
                // journal)): an unset one, or one not live here, is refused.
                let asked = JournalIdentifier::new(TenantId(req.tenant), paros_core::JournalId(req.journal));
                let Some((&journal, rt)) = journals.named(asked) else {
                    moonpool_assertions::reachable!("operator: a retirement names a journal the node does not serve");
                    let refusal = RetireRefusal::UnknownJournal;
                    let ack = RetireAck { accepted: false, refusal: refusal.label().to_string() };
                    shared.with(&node_audit).answer(Reply::Retire, reply, ack);
                    continue;
                };
                let ack = operator::retire(&rt.node, &rt.audit, self_id, &req);
                if ack.accepted {
                    retiring = Some(journal);
                }
                shared.with(&rt.audit).answer(Reply::Retire, reply, ack);
            }
            Some((req, reply)) = rpc.reconfigure.recv() => {
                // An acceptor reconfiguration (#122): a matchmaker-deployment
                // act on the journal it names (#474); a plain journal refuses
                // it (`no_matchmakers`). No journal has a default (#439
                // (operator calls name their journal)): an unset one, or one
                // not live here, is refused `unknown_journal`.
                let asked = JournalIdentifier::new(TenantId(req.tenant), paros_core::JournalId(req.journal));
                let Some((&journal, rt)) = journals.named(asked) else {
                    moonpool_assertions::reachable!("operator: a reconfiguration names a journal the node does not serve");
                    shared.with(&node_audit).answer(Reply::Reconfigure, reply, operator::unknown_journal());
                    continue;
                };
                if let Err(fault) = shared.land(rt).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                let ack = operator::reconfigure(&mut rt.node, &rt.audit, self_id, &req);
                shared.settle(rt).await;
                // A lost reconfiguration ack is ambiguous to the client, which
                // re-asks; a started reconfiguration stands (a retry is refused
                // as `not_leader` while it runs, then `unchanged` once done).
                shared.with(&node_audit).answer(Reply::Reconfigure, reply, ack);
            }
            Some((req, reply)) = rpc.scrub.recv() => {
                // A scrub page (#362 (background scrubbing)): read back and
                // checked here; a damaged record goes to the core, and the
                // settle tail makes its rewrite durable before the answer
                // leaves. No journal has a default: an unset one, or one not
                // live here, is refused.
                let asked = JournalIdentifier::new(TenantId(req.tenant), paros_core::JournalId(req.journal));
                let Some((&journal, rt)) = journals.named(asked) else {
                    let _ = reply.send(scrub::refused("unknown_journal"));
                    continue;
                };
                // The store comes back from a flying batch first (#613
                // (settle off the node loop)): the read-back needs it.
                if let Err(fault) = shared.land(rt).await {
                    let _ = reply.send(scrub::refused("storage_fault"));
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                let Some(storage) = rt.storage.as_mut() else {
                    unreachable!("a landed journal holds its store");
                };
                let mut served = scrub::serve(&mut rt.node, storage, &req).await;
                if served.repairs > 0 {
                    // The answer waits for the rewrite's flush: a slot is
                    // reported repaired only once it is durable.
                    shared.settle(rt).await;
                    let outcome = shared.land(rt).await;
                    if outcome.is_err() {
                        // The rewrite did not land: the store's next
                        // append of the slot writes it again.
                        for slot in &mut served.reply.slots {
                            slot.repaired = false;
                        }
                    }
                    journals.fold(journal, outcome, ticks, self_id)?;
                }
                metrics.scrubbed(journal, &served.reply);
                let _ = reply.send(served.reply);
            }
            Some((req, reply)) = rpc.inspect.recv() => {
                // No settle tail: an inspect is a pure read of the core, so it
                // produces no `Ready` batch. It names its journal or asks for
                // the node alone (#243): no identifier has a default, so an unset
                // one is refused, and so is a journal not live here — the
                // node's own facts are answered either way.
                let facts = operator::node_facts(self_id, cell.as_ref());
                let mut answer = match req.target() {
                    Ok(InspectTarget::Node) => facts,
                    Ok(InspectTarget::Journal(journal)) => journals.live.get(&journal).map_or_else(
                        || facts.refused(InspectRefusal::UnknownJournal),
                        |rt| operator::inspect(&rt.node, cell.as_ref()),
                    ),
                    Err(refusal) => facts.refused(refusal),
                };
                answer.machine_interface.clone_from(&machine_interface);
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
                let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, links: &links, rpc: &rpc_handle, audit: &node_audit };
                let (mut admit, owed) = sys.apply(f, journal, events, checkpoints).await;
                if owed && sys.place(f).await {
                    admit = true;
                }
                if admit {
                    admission_owed.get_or_insert(time.now());
                }
                admit_owed(&mut journals, f, &mut admission_owed, late_admitter, time.now());
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
                    if journals.exhausted() && !journals.waits_for_space() && (!follows || journals.stranded()) {
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
                    if journals.live.contains_key(&journal) {
                        // A re-open after a storage fault boots on what the
                        // failed sync left in the file image, durable or
                        // not, until the journal's next sync (#348).
                        let hinted = moonpool_buggify::hint!("journal re-opened, not synced", 0.5);
                        if hinted.strike() == moonpool_buggify::hint::Strike::Killed {
                            moonpool_assertions::reachable!(
                                "the driver crashes right after a journal re-opens"
                            );
                        }
                        hinted.await;
                    }
                }
                // A re-opened journal booted from `Config::pool`: it admits
                // the registry's pool again (#189).
                if let Some(f) = follower.as_ref().filter(|_| !due.is_empty()) {
                    admit_pool(&mut journals, f);
                }
                // A moved founding member's clocks wait for its registration
                // (#390).
                if unregistered.as_ref().is_some_and(|ended| *ended.borrow()) {
                    moonpool_assertions::reachable!(
                        "machine: a moved founding member resumes its journals once registered"
                    );
                    tracing::info!(node = self_id, "moved_member_registered");
                    unregistered = None;
                }
                // Every live journal's beat, in id order.
                let live: Vec<JournalIdentifier> = if unregistered.is_some() {
                    Vec::new()
                } else {
                    journals.live.keys().copied().collect()
                };
                // Each tenant's handover is paced once per tick, on the
                // first of its matchmaker journals that beats (#190).
                let mut paced: BTreeSet<TenantId> = BTreeSet::new();
                for journal in live {
                    let audits = tenant_audits(&journals, &stores, journal.tenant);
                    let Some(rt) = journals.live.get_mut(&journal) else { continue };
                    if crate::scenario::hold_journal(hold_candidate, journal) {
                        rt.audit.journal_held(NodeId(self_id));
                        tracing::info!(node = self_id, journal = %journal, "journal_held");
                        continue;
                    }
                    // The handover belongs to the tenant's matchmaker set; on
                    // a plain journal it stays idle. Paced at the tick, flying
                    // or not: it only reads the core.
                    if rt.node.config().has_matchmakers() && paced.insert(journal.tenant) {
                        let handover = handovers.entry(journal.tenant).or_insert_with(|| HandoverDriver::new(NodeId(self_id), registry_page));
                        shared.with(&audits).pace_handover(&rt.node, handover);
                    }
                    let outcome = shared.input(rt, Input::Tick(ticks)).await;
                    journals.fold(journal, outcome, ticks, self_id)?;
                }
                // The peer book (#349): a peer the registry moved is dialed
                // at its registered address from its lane's next batch on.
                if let Some(book) = peer_book.as_mut() {
                    let (moved, cache) = book.follow(&journals);
                    for (peer, (addr, interface)) in moved {
                        let moved = out.readdress(peer, addr.clone(), interface.clone());
                        assert!(moved, "every founding member but this one has a lane");
                        // A founding member hosts its cell's matchmaker sets
                        // (#190): dialed at its new address too.
                        if let Some(link) = links.client(MatchmakerId(peer.0)) {
                            link.readdress(addr.clone(), &interface);
                        }
                        tracing::info!(node = self_id, peer = peer.0, %addr, "peer_book_moved");
                    }
                    // The durable cached registry fold (#211), written off
                    // the loop.
                    if let Some(cache) = cache {
                        stores.cache_registry(cache);
                    }
                }
                // An admitted machine's newest book (#505 (stranded admitted
                // machine)): every seed it moved is dialed there from the
                // follower's next read on, and so is its lane.
                if let (Some(books), Some(f)) = (offered_books.as_mut(), follower.as_mut())
                    && books.has_changed().unwrap_or(false)
                {
                    let offered = books.borrow_and_update().clone();
                    for (peer, addr) in offered.map(|book| f.adopt(book.position, &book.machines)).unwrap_or_default() {
                        let interface = f.registry().get(peer).map(|node| node.machine_interface.clone()).unwrap_or_default();
                        out.readdress(peer, addr.clone(), interface.clone());
                        if let Some(link) = links.client(MatchmakerId(peer.0)) {
                            link.readdress(addr.clone(), &interface);
                        }
                        moonpool_assertions::reachable!("driver: an admitted machine dials a seed from an adopted book");
                        tracing::info!(node = self_id, peer = peer.0, %addr, "seed_book_moved");
                    }
                }
                // The system journals (#189): fold what this node's own
                // the system journals chose, apply it, and keep a follow read
                // open against a seed for the ones it does not run.
                if let Some(f) = follower.as_mut() {
                    // Every fold this tick reports its events before any
                    // journal is placed (#474): a placement reads them all.
                    let mut placing = false;
                    for (journal, events) in follow_local(f, &journals) {
                        let checkpoints = if journal == f.registry_key() { f.take_checkpoints() } else { Vec::new() };
                        let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, links: &links, rpc: &rpc_handle, audit: &node_audit };
                        let (admit, owed) = sys.apply(f, journal, events, checkpoints).await;
                        placing |= owed;
                        if admit {
                            admission_owed.get_or_insert(time.now());
                        }
                    }
                    if placing {
                        let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, links: &links, rpc: &rpc_handle, audit: &node_audit };
                        if sys.place(f).await {
                            admission_owed.get_or_insert(time.now());
                        }
                    }
                    // A held admission lands once its hold is over (#387).
                    admit_owed(&mut journals, f, &mut admission_owed, late_admitter, time.now());
                    if !crate::scenario::lag_follow() {
                        f.poll_remote(&providers, |journal| journals.live.contains_key(&journal));
                    }
                }
                if journals.exhausted() && !journals.waits_for_space() && (!follows || journals.stranded()) {
                    return journals.exit();
                }
                if let Some(halt) = halt.as_mut() {
                    // A clean stop's beat (#560 (clean Ctrl-C shutdown)):
                    // hand every leadership it can on, then end once
                    // nothing is open, or once the bound ran out.
                    halt.beat();
                    let leaders: Vec<JournalIdentifier> = journals
                        .live
                        .iter()
                        .filter(|(_, rt)| rt.node.can_relinquish())
                        .map(|(journal, _)| *journal)
                        .collect();
                    for journal in leaders {
                        let Some(rt) = journals.live.get_mut(&journal) else { continue };
                        if let Err(fault) = shared.land(rt).await {
                            journals.fold(journal, Err(fault), ticks, self_id)?;
                            continue;
                        }
                        if let Some(handoff) = stop::hand_on(&providers, &mut rt.node) {
                            rt.audit.authority_relinquished(NodeId(self_id), handoff);
                            halt.handed(journal, handoff.to, handoff.ballot);
                            tracing::info!(machine = %stop_name, journal = %journal, to = handoff.to.0, "stop_handoff_sent");
                        }
                        shared.settle(rt).await;
                    }
                    let coordinator = coordinator_ended.as_ref().is_none_or(|ended| *ended.borrow());
                    let open = halt.open(&journals, coordinator);
                    let now = time.now();
                    if open.is_none() || halt.expired(now) {
                        let (handed, taken) = halt.handoffs();
                        let took_ms = u64::try_from(halt.took(now).as_millis()).unwrap_or(u64::MAX);
                        match open {
                            None => {
                                moonpool_assertions::reachable!("stop: a machine stopped cleanly");
                                tracing::info!(machine = %stop_name, handed, taken, took_ms, "stop_completed");
                            }
                            Some(open) => {
                                moonpool_assertions::reachable!("stop: a machine's stop ran out of time");
                                tracing::warn!(machine = %stop_name, step = open.label(), handed, taken, took_ms, "stop_timed_out");
                            }
                        }
                        return Ok(());
                    }
                }
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled(), if halt.is_none() => {
                // The operator's stop (#560 (clean Ctrl-C shutdown)): the
                // loop serves on, refuses new calls, and ends at a beat
                // once nothing is open.
                stopping.cancel();
                halt = Some(stop::Stop::begin(time.now(), &tunables));
                tracing::info!(machine = %stop_name, within_ms = u64::try_from(tunables.shutdown_within.as_millis()).unwrap_or(u64::MAX), "stop_started");
            }
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
    links: &'a MatchmakerLinks<P>,
    rpc: &'a moonpool_rpc::RpcHandle<P>,
    audit: &'a J::Audit,
}

/// Whether a node outside a journal's bootstrap membership starts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seat {
    /// Only a member does: a tenant control journal, served by the founding
    /// members.
    Member,
    /// Any node does, as a spare: a journal placed on it (#474).
    Spare,
}

/// An event is tagged by the journal whose fold produced it: a registry
/// event by the registry, a tenant event by a tenant control journal.
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
        SystemEvent::Tenant(_) => {
            assert!(
                journal != registry,
                "a tenant event never comes from the registry"
            );
            assert!(
                journal.tenant != registry.tenant,
                "a tenant event never comes from the cell tenant"
            );
        }
    }
}

impl<P: Providers, J: JournalStores> SystemCtx<'_, '_, P, J> {
    /// Apply what `journal`'s fold moved, in position order: start a created
    /// journal naming this node, stop a tombstoned one, open a lane to an
    /// admitted node, and stop every user journal on this node's own
    /// retirement. Each event is reported first, once. Whether the
    /// registry's pool is owed to the live journals ([`admit_owed`]), and
    /// whether the journals placed here are owed a [`SystemCtx::place`]
    /// (#474), which the caller runs once every fold of the round reported
    /// its events.
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(level = "debug", skip_all, fields(node = follower.self_id().0, journal = %journal, events = events.len()))]
    async fn apply(
        &mut self,
        follower: &ControlFollower<P>,
        journal: JournalIdentifier,
        events: Vec<(u64, SystemEvent)>,
        checkpoints: Vec<system::FoldedCheckpoint>,
    ) -> (bool, bool) {
        let me = follower.self_id();
        self.report_checkpoints(follower, journal, checkpoints);
        // Whether the pool moved or a journal opened: every live journal
        // then admits the registry's pool (`ColocatedNode::extend_pool`,
        // refused on a journal that cannot reconfigure).
        let mut admit = false;
        // Whether a booking or a created journal moved: the journals placed
        // here are started and stopped after the events (#474).
        let mut place = false;
        for (seq, event) in events {
            assert_event_source(follower.registry_key(), journal, &event);
            self.audit.system_folded(me, journal, seq, &event);
            tracing::info!(node = me.0, journal = %journal, seq, event = ?event, "system_folded");
            match event {
                // A created journal starts where the registry books its
                // acceptor slots (#474): placed below, once both folds hold
                // it, whichever comes first.
                SystemEvent::Tenant(TenantEvent::Created { id, .. }) => {
                    // A created journal lives in the tenant whose control
                    // journal created it (#210): never in another one.
                    let id = JournalIdentifier::new(journal.tenant, id);
                    assert!(
                        id.tenant == journal.tenant,
                        "a journal is created inside its own tenant"
                    );
                    place = true;
                }
                SystemEvent::Tenant(TenantEvent::Deleted { id, .. }) => {
                    let id = JournalIdentifier::new(journal.tenant, id);
                    self.stop(me, id);
                }
                // A checkpoint of a tenant control journal: stop every
                // deleted journal; the live ones are placed below.
                SystemEvent::Tenant(TenantEvent::Checkpoint { .. }) => {
                    let deleted: Vec<JournalId> = follower
                        .tenant(journal.tenant)
                        .map(|t| {
                            t.journals()
                                .filter(|(_, j)| j.deleted_at.is_some())
                                .map(|(id, _)| id)
                                .collect()
                        })
                        .unwrap_or_default();
                    for id in deleted {
                        self.stop(me, JournalIdentifier::new(journal.tenant, id));
                    }
                    place = true;
                }
                // A slot booked or released here (#474): placed below.
                SystemEvent::Registry(
                    RegistryEvent::Booked { node, .. } | RegistryEvent::Released { node, .. },
                ) if node == me => place = true,
                // A grant that books a slot here (#476 (tenant footprint)):
                // a set's slots come as one grant, with no other event.
                SystemEvent::Registry(RegistryEvent::Granted { ref slots, .. })
                    if slots.iter().any(|slot| slot.node == me) =>
                {
                    place = true;
                }
                // The cell hosts a tenant (#210): every founding member
                // serves its control journal (the placement until #212).
                SystemEvent::Registry(RegistryEvent::TenantHosted { tenant, control }) => {
                    let id = JournalIdentifier::new(tenant, control);
                    let founders = follower.founders().to_vec();
                    if self
                        .start(
                            follower,
                            id,
                            (&founders, &[], 0),
                            (
                                paros_core::QuorumSystem::Majority,
                                paros_core::WriterMode::Single,
                                crate::tenant::StoreDurability::Ordered,
                            ),
                            Seat::Member,
                        )
                        .await
                    {
                        admit = true;
                    }
                }
                // The cell dropped a tenant: every journal of it stops here.
                SystemEvent::Registry(RegistryEvent::TenantDropped { tenant }) => {
                    self.stop_tenant(me, tenant);
                }
                SystemEvent::Registry(
                    RegistryEvent::Registered { id, .. } | RegistryEvent::Reregistered { id, .. },
                ) if id == me => {
                    self.join_spares(follower).await;
                    admit = true;
                }
                // A registered machine registered again, maybe at another
                // address (a rename, #349): a lane opened at the old one
                // follows it, matchmaker link included (#454 (control plane
                // meets chaos): witness 17483207099981818289, an admitted
                // machine renamed in chaos whose set handover never reached
                // it). A founding member that moved before its first
                // registration landed moves with a `Registered` (#543): an
                // admitted machine opened its lane from the admission's book,
                // at the plan's address, and that lane follows it too.
                // Seed 15535649749772960988 (recipe
                // 2208855:8827221011598423148) on fba843a: the joiner's lane
                // to a moved founder dialed its dead address for the whole
                // run, so a `1x4` grid journal never held a Phase-1 quorum
                // again and a chosen slot stayed unapplied.
                SystemEvent::Registry(
                    RegistryEvent::Reregistered { id, addr, .. }
                    | RegistryEvent::Registered { id, addr, .. },
                ) => {
                    let interface = follower
                        .registry()
                        .get(id)
                        .map(|node| node.machine_interface.clone())
                        .unwrap_or_default();
                    self.readdress_peer(me, id, &addr, &interface);
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
                    let founders = follower.founders().to_vec();
                    for tenant in registry.hosted() {
                        let Some(hosted) = registry.hosted_tenant(tenant) else {
                            continue;
                        };
                        let id = JournalIdentifier::new(tenant, hosted.control);
                        self.start(
                            follower,
                            id,
                            (&founders, &[], 0),
                            (
                                paros_core::QuorumSystem::Majority,
                                paros_core::WriterMode::Single,
                                crate::tenant::StoreDurability::Ordered,
                            ),
                            Seat::Member,
                        )
                        .await;
                    }
                    let served: Vec<TenantId> =
                        self.journals.live.keys().map(|j| j.tenant).collect();
                    for tenant in served.into_iter().filter(|t| registry.dropped(*t)) {
                        self.stop_tenant(me, tenant);
                    }
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
                    place = true;
                }
                _ => {}
            }
        }
        (admit, place)
    }
}

impl<P: Providers, J: JournalStores> SystemCtx<'_, '_, P, J> {
    /// Start journal `id` on this node, bootstrapped over `members` under
    /// `quorum_system`, written in `writer` mode, its store committing under
    /// `store` (#599 (durability mode per journal)) — unless the journal is
    /// gone for good, already served here, this machine takes no storage
    /// work (a `stateless` machine never serves a journal, #211), or this
    /// node is no member and `seat` takes members only. A node outside
    /// `members` joins as a spare (#474): its own id beside the bootstrap
    /// membership, which a reconfiguration then names; only a journal with
    /// matchmakers can. The configuration depends on the arguments alone, so
    /// every start of the journal here opens its store under the same one.
    /// Whether it started.
    async fn start(
        &mut self,
        follower: &ControlFollower<P>,
        id: JournalIdentifier,
        (members, domains, cell_id): (&[NodeId], &[paros_core::FailureDomain], u64),
        (quorum_system, writer, store): (
            paros_core::QuorumSystem,
            paros_core::WriterMode,
            crate::tenant::StoreDurability,
        ),
        seat: Seat,
    ) -> bool {
        assert!(
            domains.is_empty() || domains.len() == members.len(),
            "a started journal labels every member or none"
        );
        let me = follower.self_id();
        let member = members.contains(&me);
        let spare = !member && seat == Seat::Spare && !follower.matchmakers().is_empty();
        if !(member || spare)
            || follower.is_tombstoned(id)
            || self.journals.serves(id)
            || !follower.takes_storage_work()
        {
            return false;
        }
        let nodes = if spare {
            moonpool_assertions::reachable!("machine: a placed journal starts as a spare");
            let mut nodes = members.to_vec();
            nodes.push(me);
            nodes.sort_unstable();
            nodes.dedup();
            nodes
        } else {
            Vec::new()
        };
        let journal_config = paros_core::Config {
            peers: members.to_vec(),
            // The placed configuration's labels (#215 (zones)), bound to
            // the journal's bootstrap like its membership.
            peer_domains: domains.to_vec(),
            cell_id,
            quorum_system,
            nodes,
            writer_mode: writer,
            // Born with its tenant's matchmaker set (#190).
            matchmakers: follower.matchmakers().to_vec(),
            ..paros_core::Config::new(me, id)
        };
        assert!(
            journal_config.pool().contains(&me),
            "a started journal's pool holds this node"
        );
        if !self.stores.create(id, journal_config, store.journal()) {
            return false;
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
        let started = self.journals.live.contains_key(&id);
        if started {
            self.audit.journal_started(me, id);
            tracing::info!(node = me.0, journal = %id, "journal_started");
        }
        started
    }

    /// Start and stop the journals placed on this node (#474): every live
    /// journal of a hosted tenant the registry books an acceptor slot of
    /// here runs here, and every other journal such a tenant created stops
    /// here, its store kept: a later booking starts it again from there.
    /// Whether a journal started.
    async fn place(&mut self, follower: &ControlFollower<P>) -> bool {
        let me = follower.self_id();
        let booked: BTreeSet<JournalIdentifier> = follower
            .registry()
            .bookings()
            .filter(|(_, b)| b.node == me && b.role == crate::system::Role::Acceptor)
            .filter_map(|(_, b)| match b.target {
                crate::system::BookingTarget::Journal(journal) => Some(journal),
                crate::system::BookingTarget::Set { .. }
                | crate::system::BookingTarget::Tenant { .. } => None,
            })
            .collect();
        // The matchmaker sets booked here (#501 (matchmaker sets off the
        // founders)): the host opens a set of a later generation only then.
        let sets: BTreeSet<TenantId> = follower
            .registry()
            .bookings()
            .filter(|(_, b)| b.node == me && b.role == crate::system::Role::Matchmaker)
            // A footprint's matchmaker slot (#476) reserves room only.
            .filter(|(_, b)| matches!(b.target, crate::system::BookingTarget::Set { .. }))
            .map(|(_, b)| b.target.tenant())
            .collect();
        self.stores.book_sets(sets);
        // A journal its tenant created (live or deleted) that is no longer
        // booked here stops: its booking was released. A store a booking
        // released while this machine was down counts too: no journal of
        // this incarnation knows it, but the machine still holds it.
        let provisioned: Vec<JournalIdentifier> = self
            .stores
            .provisioned()
            .into_iter()
            .filter(|journal| !self.journals.released(*journal))
            .collect();
        let held: BTreeSet<JournalIdentifier> = self.journals.known().chain(provisioned).collect();
        let unbooked: Vec<JournalIdentifier> = held
            .into_iter()
            .filter(|journal| !self.journals.is_control(*journal) && !booked.contains(journal))
            .filter(|journal| {
                follower
                    .tenant(journal.tenant)
                    .is_some_and(|t| t.get(journal.journal).is_some())
            })
            .collect();
        for journal in unbooked {
            moonpool_assertions::reachable!("machine: a released booking stops its journal");
            self.journals.forget(journal);
            self.audit.journal_stopped(me, journal);
            tracing::info!(node = me.0, journal = %journal, "journal_released");
        }
        let mut started = false;
        for journal in booked {
            let Some(created) = follower
                .tenant(journal.tenant)
                .and_then(|t| t.get(journal.journal))
                .filter(|j| j.deleted_at.is_none())
                .cloned()
            else {
                continue;
            };
            let systems = (
                created.config.quorum_system(),
                created.writer,
                created.store,
            );
            if self
                .start(
                    follower,
                    journal,
                    (
                        created.config.members(),
                        created.config.domains(),
                        created.config.cell(),
                    ),
                    systems,
                    Seat::Spare,
                )
                .await
            {
                if !follower.founders().contains(&me) {
                    moonpool_assertions::reachable!(
                        "machine: an admitted machine serves a journal"
                    );
                }
                started = true;
            }
        }
        started
    }

    /// Stop journal `id` here for good, and forget its store.
    fn stop(&mut self, me: NodeId, id: JournalIdentifier) {
        if self.journals.serves(id) {
            self.journals.park(id, None);
            self.audit.journal_stopped(me, id);
            tracing::info!(node = me.0, journal = %id, "journal_stopped");
        }
        self.stores.delete(id);
    }

    /// Stop every journal of `tenant` here, its control journal included.
    fn stop_tenant(&mut self, me: NodeId, tenant: TenantId) {
        let served: Vec<JournalIdentifier> = self
            .journals
            .live
            .keys()
            .copied()
            .filter(|j| j.tenant == tenant)
            .collect();
        for id in served {
            self.stop(me, id);
        }
    }

    /// Report each checkpoint `journal`'s fold met, in position order.
    fn report_checkpoints(
        &self,
        follower: &ControlFollower<P>,
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

    /// Node `id` registered again at `addr`: its lane, if this node has
    /// one, and its matchmaker link dial `addr` from their next batch on.
    fn readdress_peer(&mut self, me: NodeId, id: NodeId, addr: &str, interface: &[u8]) {
        assert!(id != me, "a node never readdresses itself");
        if !self.out.has_peer(id) {
            return;
        }
        match peer_target(addr) {
            Ok(target) => {
                let moved = self.out.readdress(id, target.clone(), interface.to_vec());
                assert!(moved, "a peer this node holds has a lane");
                if let Some(link) = self.links.client(MatchmakerId(id.0)) {
                    link.readdress(target, interface);
                }
                moonpool_assertions::reachable!("driver: a registered peer is readdressed");
            }
            Err(error) => {
                tracing::warn!(node = me.0, peer = id.0, %error, "registered_address_unusable");
            }
        }
    }

    /// Node `id`, registered at `addr`, is in the pool: open its peer lane
    /// (once) and report the admission.
    fn admit_peer(&mut self, me: NodeId, id: NodeId, addr: &str) {
        // This node's own registration joins spares; it is never a peer.
        assert!(id != me, "a node never admits itself as a peer");
        if !self.out.has_peer(id) {
            match peer_target(addr) {
                Ok(addr) => {
                    // A machine the registry admits may host a matchmaker
                    // set of a later generation (#501 (matchmaker sets off
                    // the founders)): this node links to it too.
                    self.links.add(
                        MatchmakerId(id.0),
                        MatchmakerClient::named(self.rpc, self.lanes.names.clone(), addr.clone()),
                    );
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
    async fn join_spares(&mut self, follower: &ControlFollower<P>) {
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
            // A spare of the static plan: ordered, like every journal no
            // tenant created.
            if !self
                .stores
                .create(journal, config, crate::journal::Durability::Ordered)
            {
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

/// Admit the registry's pool into every live journal when an admission is
/// owed, unless this node holds it late ([`crate::scenario::ADMIT_LATE`],
/// #387): then it stays owed, and a later call admits it.
fn admit_owed<P: Providers, S, A>(
    journals: &mut Journals<S, A>,
    follower: &ControlFollower<P>,
    owed: &mut Option<std::time::Duration>,
    late: bool,
    now: std::time::Duration,
) {
    let Some(since) = *owed else { return };
    if crate::scenario::admit_held(late, since, now) {
        return;
    }
    admit_pool(journals, follower);
    *owed = None;
    assert!(owed.is_none(), "an admission made is no longer owed");
}

/// Every live journal admits the registry's pool (#189): a registered node
/// becomes one it follows, counts and answers. Refused by the core on a
/// journal that cannot reconfigure; retired nodes stay in (the pool is
/// grow-only) and are kept out at the edge instead.
fn admit_pool<P: Providers, S, A>(journals: &mut Journals<S, A>, follower: &ControlFollower<P>) {
    let pool = follower.pool();
    // In a cell, a machine's matchmaker is named by its node id: every
    // registered machine may host a set of a later generation (#501
    // (matchmaker sets off the founders)).
    let matchmakers: Vec<MatchmakerId> = pool.iter().map(|n| MatchmakerId(n.0)).collect();
    for rt in journals.live.values_mut() {
        rt.node.extend_pool(&pool);
        rt.node.extend_matchmaker_pool(&matchmakers);
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
    follower: Option<&ControlFollower<P>>,
    journal: JournalIdentifier,
    call: &'static str,
    self_id: u64,
    audit: &N,
) -> bool {
    // A journal its tenant tombstoned (#189, #210) is unknown from here on,
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

/// A journal a full disk quarantined re-opens **read-only** (#360 (disk
/// full), decided on 2026-10-18: a state of the live journal, not a longer
/// quarantine, so the node keeps its Phase-1 promises and its reads): from
/// the fault's tick `since` for `DriverTunables::read_only_ticks`, so the
/// quarantine before the re-open counts (a full disk holds a journal
/// unwritable for the longer of the two, not their sum), it votes in Phase 1,
/// serves reads, refuses every Phase-2 vote ([`read_only_refuses`]) and
/// never campaigns (an election timeout of 0 never fires). A write that
/// still finds the disk full quarantines it again.
fn enter_read_only<S, A: Audit>(
    rt: &mut JournalRt<S, A>,
    journal: JournalIdentifier,
    since: u64,
    now: u64,
) {
    assert!(
        rt.read_only.is_none(),
        "a re-opened journal starts writable"
    );
    assert!(
        since <= now,
        "a full disk took the journal before it re-opens"
    );
    // A re-opened core is a follower with no campaign open.
    assert!(!rt.node.is_leader(), "a re-opened journal does not lead");
    rt.read_only = Some(since);
    rt.back_from_full = false;
    rt.node.set_election_timeout(0);
    assert!(
        rt.node.election_timeout() == 0,
        "a read-only journal never campaigns"
    );
    moonpool_assertions::reachable!("disk full: a store re-opens read-only");
    let node = rt.node.config().id.0;
    tracing::warn!(node, journal = %journal, reason = "disk_full", "journal_read_only");
}

/// Whether a read-only journal (#360 (disk full)) refuses `msg`: a Phase-2
/// vote (`Accept`), the records a catch-up would write, and a handoff that
/// would make it lead. A refused message is dropped, as a lost one: the
/// leader's re-send reaches the node once it is writable again, and a
/// quorum of other acceptors decides meanwhile. Phase 1 (`Prepare`) and
/// reads go on.
fn read_only_refuses(msg: &Message) -> bool {
    matches!(
        msg,
        Message::Accept { .. } | Message::CatchUpResponse { .. } | Message::Relinquish { .. }
    )
}

/// Open `journal`'s store and boot it into `journals` (at boot, or when its
/// quarantine is over at tick `now`): live on success, back in quarantine on
/// a storage fault (its open's too, [`JournalStores::open_fault`]), down for
/// good when the store refuses to open.
async fn open_journal<P: Providers, J: JournalStores>(
    providers: &P,
    stores: &mut J,
    journals: &mut Journals<J::Store, J::Audit>,
    journal: JournalIdentifier,
    now: u64,
    tunables: &DriverTunables,
) {
    // A batch the journal's last runtime left flying lands first: one
    // handle writes a store at a time (#613 (settle off the node loop)).
    journals.land_orphan(journal).await;
    let audit = stores.audit(journal);
    let Some((storage, boot)) = stores.open(journal).await else {
        if let Some(fault) = stores.open_fault(journal) {
            assert!(
                matches!(fault, RunError::Storage(_)),
                "an open quarantines only on a storage fault"
            );
            moonpool_assertions::reachable!(
                "driver: a journal's open is quarantined on a storage fault"
            );
            tracing::info!(journal = %journal, "journal_open_quarantined");
            journals.requarantine(journal, now, fault);
            return;
        }
        tracing::info!(journal = %journal, "journal_down");
        journals.park(journal, None);
        return;
    };
    match boot_journal(providers, storage, boot, audit, tunables).await {
        Ok(mut rt) => {
            if let Some(since) = journals.take_full(journal) {
                enter_read_only(&mut rt, journal, since, now);
            }
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
