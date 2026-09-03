//! The provider-generic node driver — the `Node` layer that owns the sans-IO
//! [`paros_core::RawNode`] and performs all I/O.
//!
//! Written once over moonpool's `P: Providers` abstraction, so the *same* loop
//! runs in production (`TokioProviders`) and deterministic simulation
//! (`SimProviders`). The sim harness (`paros-sim`) adapts a moonpool `Process`
//! to it; a future `parosd` binary will adapt a tokio `main`.
//!
//! The loop `select`s over {client request, peer message, tick timer, shutdown},
//! feeds the core via `step`/`tick`, and drains every [`paros_core::Ready`] in
//! persist → send → apply → advance order (durable-before-send). It also draws
//! the randomized election timeout from the provider RNG (the core stays
//! dependency-free) and holds each client reply until its slot commits
//! (ack-on-commit), redirecting non-leader proposals.
//!
//! The submodules, one concern each:
//!
//! - [`config`] — the per-node tunables, the constants they default to, the
//!   shared gRPC shapes, the scope guard, the address parser, and [`RunError`].
//! - [`events`] — the `EV_*` tracing names and the pure helpers that turn a
//!   domain value into the stable field a trace carries.
//! - [`transport`] — the bounded, lossy, keep-newest per-peer mailboxes, the
//!   `Outbound` send handle, and the detached peer-delivery task.
//! - [`snap_repair`] — the snapshot-point custody tally and chunk-repair pull.
//! - [`ready`] — the `Ready` handshake's durability pipeline and the held
//!   client replies it answers.
//! - [`matchmaking`] — the matchmaker links, the requests a drained batch hands
//!   the loop, and the reports of what each answer did.
//! - [`handover`] — the driver-side policy around the matchmaker-set handover.
//! - [`boot`] — the (re)boot replay of durable state.
//! - [`report`] — the post-batch upkeep and its cross-batch delta trackers.
//!
//! `mod.rs` itself holds only [`run_node`], the select loop that wires them.

mod boot;
mod config;
mod events;
mod handover;
mod matchmaking;
mod ready;
mod report;
mod snap_repair;
mod transport;

pub use config::{DriverTunables, RunError, parse_addr};
pub(crate) use config::{accept_and_serve, grpc_keep_alive};
pub use events::{
    EV_APPLIED, EV_AUTHORITY_INSTALLED, EV_AUTHORITY_RELINQUISHED, EV_BOOTED, EV_CHOSEN,
    EV_CHOSEN_GAP, EV_CLIENT_REPLY_DROPPED, EV_COMPACTED, EV_CRASHED, EV_DUPLICATE_SUPPRESSED,
    EV_ELECTION_TIMEOUT_EXTREME, EV_GAP_FILLED, EV_HANDOFF_FENCE_EXPIRED, EV_HANDOFF_REFUSED,
    EV_LEADER, EV_LEADERSHIP_RESIGNED, EV_MSG_RECV, EV_MSG_SENT, EV_NODE_STATE, EV_NODE_TICK,
    EV_PERSIST, EV_PREPARE_BELOW_FLOOR, EV_PROPOSE_DEDUP_ACK, EV_QUORUM_LOST, EV_RECOVERED,
    EV_RESEND_SKIPPED, EV_SEND_DROPPED, EV_SEND_DUPLICATED, EV_SNAPSHOT_INSTALLED,
    EV_SNAPSHOT_MID_ELECTION, EV_SNAPSHOT_OFFERED, EV_STORAGE_FAULT, EV_SYNCED, command_hash,
};
pub use handover::RECONFIGURE_TIMEOUT_ELECTIONS;

use std::collections::BTreeMap;
use std::sync::Arc;

use moonpool_core::{
    Detach, NetworkProvider, Providers, RandomProvider, SimulationError, SimulationResult,
    TaskProvider, TcpListenerTrait, TimeProvider,
};
use moonpool_hyper::{H2Server, H2ServerConfig, ReconnectingChannel};
use paros_core::{
    AcceptorConfig, ClientId, ClientSeq, Control, GcAck, MatchRefusal, MatchReply, MatchStep,
    MatchmakerGeneration, MatchmakerId, Message, NodeId, NodeRole, ProposeResult, QuorumSystem,
    RawNode, ReadIndexResult, ReconfigureRefusal, ReconfigureReply, ReconfigureRequest,
    ReconfigureResult, ReconfigurerStep, Slot, StartRefusal, Value,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::grpc::{
    CompactAck, InspectReply, ParosInternalClient, ParosInternalServer, ParosMatchmakerClient,
    ParosServer, ProposeAck, ReadAck, ReconfigureAck, ReconfigureMatchmakersAck, RetireAck,
    RpcInbox, common, rpc_channel,
};
use crate::hooks::{DriverHooks, Reply};
use crate::storage::NodeStorage;

use boot::replay_boot_state;
use config::{OnDrop, grpc_channel_config};
use events::{message_kind, message_route};
use handover::HandoverDriver;
use matchmaking::{
    MatchmakerLinks, report_match_step, send_outbox, send_reconfigure_requests, surface_matchmaking,
};
use ready::{ClientWaiters, drain_ready, storage_fault_crash};
use report::{Deltas, draw_election_timeout, handoff_context, maintain};
use snap_repair::{
    SnapRepair, handle_snap_chunk_request, handle_snap_chunk_response, snap_repair_tick,
};
use transport::{Outbound, PeerMailbox, PeerQueues, run_peer_delivery};

/// The node loop's fixed context: the handles every arm's **settle tail** needs
/// and none of them change across an incarnation. Bundled so the tail is one
/// call instead of four repeated at every arm.
struct NodeLoop<'a, P: Providers, H: DriverHooks, A: Audit> {
    providers: &'a P,
    links: &'a MatchmakerLinks<P>,
    out: &'a Outbound,
    hooks: &'a H,
    audit: &'a A,
    self_id: u64,
    election_base: u64,
}

impl<P: Providers, H: DriverHooks, A: Audit> NodeLoop<'_, P, H, A> {
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
    fn settle<S: NodeStorage>(
        &self,
        node: &mut RawNode,
        storage: &mut S,
        waiters: &mut ClientWaiters,
        last: &mut Deltas,
    ) -> Result<(), RunError> {
        let outbox = drain_ready(node, storage, self.out, waiters, self.hooks, self.audit)?;
        surface_matchmaking(node, &mut last.matchmaking, self.audit, self.self_id);
        send_outbox(self.providers, self.links, self.audit, self.self_id, outbox);
        maintain(
            node,
            self.providers,
            last,
            waiters,
            self.self_id,
            self.election_base,
            self.hooks,
            self.audit,
        );
        Ok(())
    }
}

/// Drive a paros node to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation — only the providers
/// differ) and `S: NodeStorage` (the injected durable storage). The loop owns a
/// [`RawNode`], serves the Paros gRPC interface, feeds client proposals and
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
/// `tunables` is the driver's per-node transport shape ([`DriverTunables`]):
/// production passes [`DriverTunables::default()`] (the historical constants);
/// the sim harness buggifies it per seed, FDB knob style.
///
/// `hooks` controls the driver-level crash seams and rare-but-valid policy
/// alternatives. Production passes [`NoHooks`](crate::NoHooks), whose default
/// methods are inert.
///
/// `audit` is the pure-observation mirror of `hooks`: the driver reports every
/// externally meaningful transition to it, and nothing it does can change the
/// run. Production passes [`NoAudit`](crate::NoAudit).
///
/// # Errors
///
/// The exit is typed ([`RunError`]): [`RunError::SeamCrash`] when `hooks` fires
/// at a durability seam (the caller recovers by re-running `run_node` with
/// fresh storage); [`RunError::Storage`] when a [`NodeStorage`] call failed and
/// the driver took its fail-stop crash decision — production treats it as a
/// process exit (crash-only), the sim node loop recovers through the same
/// restart path as a seam crash; [`RunError::Infra`] for genuine
/// provider/infrastructure failures (bind, listen), the only exit that is not
/// a deliberate crash and must propagate.
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len(), matchmakers = matchmakers.len()))]
// One cohesive select loop: every arm is a thin feed into the core plus the
// same drain/maintain tail; splitting arms out would only scatter the loop's
// shared state. The parameters are the node's complete wiring (providers,
// storage, addressing, tunables, lifecycle, hooks, audit) — a bundle would
// only rename the same eight things.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn run_node<P, S, H, A>(
    providers: P,
    mut storage: S,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    matchmakers: Vec<(MatchmakerId, String)>,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    S: NodeStorage,
    // Deliberately *not* `Send + 'static`, unlike the audit below. Every hook
    // is consulted from the node loop, never from a spawned task, and keeping
    // the bound this narrow is what *enforces* that: `hooks` arrives as a
    // borrow, so it cannot be captured by a `spawn_task` future, and a future
    // attempt to consult a hook from a detached task is a compile error rather
    // than a determinism bug found on CI months later. See [`PeerMailbox`].
    H: DriverHooks,
    // `Clone + Send + Sync + 'static` because each peer-delivery task carries
    // its own handle to the audit: the bounded-mailbox drops happen inside
    // those tasks, and reporting them (`Audit::dropped_at_mailbox`) is part of
    // the observation contract. Pure observation still holds — the clone
    // shares the same underlying sink.
    A: Audit + Clone + Send + Sync + 'static,
{
    // Stage 7: verify and classify every durable record BEFORE anything else —
    // in particular before `RawNode::new` reads the store — so no corrupted
    // bytes ever cross into protocol logic. A detected mismatch is the same
    // deliberate crash decision as any other storage fault: typed on the
    // audit, then `RunError::Storage` unwinds the incarnation. The scan itself
    // may only discard a crash-truncatable tail or repair a `HardState` copy
    // from its twin (see [`NodeStorage::boot_scan`]); it never truncates on a
    // corruption verdict.
    let boot_id = storage.initial_state().1.id.0;
    storage
        .boot_scan()
        .map_err(|e| storage_fault_crash(audit, boot_id, e))?;

    // Every task spawned by this incarnation must stop when `run_node` exits,
    // including a durability-seam error that immediately starts a replacement
    // incarnation. This drop guard covers every `?` and return path.
    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    let listener = providers
        .network()
        .bind(&local_addr)
        .await
        .map_err(|e| SimulationError::InvalidState(format!("node gRPC listener: {e}")))?;

    // The sans-IO core, bootstrapped from durable storage.
    let mut node = RawNode::new(&storage);
    let self_id = node.config().id.0;

    replay_boot_state(&mut node, &mut storage, self_id, hooks, audit)?;

    // Tonic handlers run as h2 request tasks and forward into these typed
    // queues. The loop remains the sole owner of RawNode. The edge's
    // integrity rejections are reported through the audit like every other
    // externally meaningful transition (observation only: the closure
    // returns nothing and the edge's answer does not depend on it).
    let on_reject: crate::grpc::OnReject = {
        let audit = audit.clone();
        Arc::new(move |kind| audit.edge_rejected(NodeId(self_id), kind))
    };
    let (rpc_service, mut rpc): (_, RpcInbox) = rpc_channel(
        tunables.client_inbox_capacity,
        tunables.peer_inbox_capacity,
        on_reject,
    );
    let grpc_service = tonic::service::Routes::new(ParosServer::new(rpc_service.clone()))
        .add_service(ParosInternalServer::new(rpc_service))
        .prepare();
    let grpc_server = H2Server::new(&providers).with_config(H2ServerConfig {
        keep_alive: Some(grpc_keep_alive(&tunables)),
        vectored_writes: true,
    });

    // Validate every origin before starting reconnecting-channel tasks. Once
    // channels exist, there are no fallible setup steps before their drop guard
    // is installed.
    let peers = members
        .into_iter()
        .map(|(id, addr)| {
            let origin = http::Uri::try_from(format!("http://{addr}"))
                .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
            Ok((id, addr, origin))
        })
        .collect::<SimulationResult<Vec<_>>>()?;

    let mut peer_channels = Vec::with_capacity(peers.len());
    let peer_queues = peers
        .into_iter()
        .map(|(id, addr, origin)| {
            let channel =
                ReconnectingChannel::new(&providers, addr, grpc_channel_config(&tunables));
            peer_channels.push(channel.clone());
            let client = ParosInternalClient::with_origin(channel, origin);
            let regular = PeerMailbox::new(tunables.peer_queue_capacity);
            let snapshot = PeerMailbox::new(tunables.snapshot_queue_capacity);
            providers
                .task()
                .spawn_task(
                    "paros-grpc-peer-delivery",
                    run_peer_delivery(
                        client.clone(),
                        providers.time().clone(),
                        incarnation_shutdown.clone(),
                        regular.clone(),
                        tunables,
                        audit.clone(),
                        self_id,
                        id,
                    ),
                )
                .detach();
            providers
                .task()
                .spawn_task(
                    "paros-grpc-snapshot-delivery",
                    run_peer_delivery(
                        client,
                        providers.time().clone(),
                        incarnation_shutdown.clone(),
                        snapshot.clone(),
                        tunables,
                        audit.clone(),
                        self_id,
                        id,
                    ),
                )
                .detach();
            (id, PeerQueues { regular, snapshot })
        })
        .collect::<BTreeMap<_, _>>();

    // The matchmaker links (#120): one reconnecting channel per matchmaker,
    // and the inbox their answers come back through. Empty on plain
    // Multi-Paxos.
    let (match_reply_tx, mut match_replies) =
        mpsc::channel::<MatchReply>(tunables.peer_inbox_capacity);
    let matchmaker_clients = matchmakers
        .into_iter()
        .map(|(id, addr)| {
            let origin = http::Uri::try_from(format!("http://{addr}"))
                .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
            let channel =
                ReconnectingChannel::new(&providers, addr, grpc_channel_config(&tunables));
            peer_channels.push(channel.clone());
            Ok((id, ParosMatchmakerClient::with_origin(channel, origin)))
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
    let mut retiring = false;

    // `close` is terminal and shared by every clone held by tonic clients. It
    // cancels connect/backoff/keepalive work immediately when this incarnation
    // exits, including simulated durability crashes that return via `?`.
    let _peer_channel_guard = OnDrop::new(move || {
        for channel in peer_channels {
            channel.close();
        }
    });

    // One reconnecting h2 channel per peer; cloned generated clients multiplex
    // concurrent RPCs over that shared connection.
    let out = Outbound {
        peer_queues,
        self_id,
    };
    let loop_ctx = NodeLoop {
        providers: &providers,
        links: &links,
        out: &out,
        hooks,
        audit,
        self_id,
        election_base: tunables.election_timeout_base,
    };

    // The held client replies: proposals keyed by slot (ack-on-commit), reads
    // keyed by their read-index ctx.
    let mut waiters = ClientWaiters::default();
    let mut next_read_ctx: u64 = 0;
    // The snapshot-point repair layer (#101): the boot scan's rotted-chunk
    // classification arms the chunk pull; everything else fills in per tick.
    let mut snap = SnapRepair::default();
    for (at, chunk) in storage.faulty_snap_chunks() {
        snap.pending.entry(at.0).or_default().insert(chunk);
    }
    for (&at, chunks) in &snap.pending {
        let count = u64::try_from(chunks.len()).unwrap_or(u64::MAX);
        audit.snap_chunks_reported(NodeId(self_id), Slot(at), count);
        tracing::info!(node = self_id, at, chunks = count, "snap_chunks_reported");
    }
    // Seed the first randomized election timeout (jitter from the driver's RNG).
    let first_timeout = draw_election_timeout(
        &providers,
        hooks,
        audit,
        self_id,
        tunables.election_timeout_base,
    );
    node.set_election_timeout(first_timeout);
    audit.election_timeout_set(NodeId(self_id), first_timeout);
    let mut last = Deltas {
        role: node.role(),
        duplicates: node.duplicates_suppressed(),
        quorum_lost: node.quorum_lost_step_downs(),
        repair: node.repair_counters(),
        handoff: node.handoff_counters(),
        membership: node.membership_counters(),
        matchmaking: None,
        matchmaking_timeouts: node.matchmaking_timeouts(),
        matchmaker_generation: node.matchmaker_set().generation.0,
    };
    // Ticks since the open matchmaking request was last (re-)sent.
    let mut match_resend_elapsed: u64 = 0;
    // Ticks since the open GC request was last (re-)sent. The running
    // handover's own clocks live in `handover`.
    let mut gc_resend_elapsed: u64 = 0;

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
            accepted = listener.accept() => {
                let (stream, addr) = accepted
                    .map_err(|e| SimulationError::InvalidState(format!("gRPC accept: {e}")))?;
                let connection = grpc_server.serve_connection_with_shutdown(
                    stream,
                    grpc_service.clone(),
                    incarnation_shutdown.clone().cancelled_owned(),
                );
                accept_and_serve(&providers, "paros-grpc-server", "node", addr, connection);
            }
            Some((req, reply)) = rpc.propose.recv() => {
                // A client value → the leader (deduplicated by (client, seq)). The
                // reply is held until the slot commits (ack-on-commit); a non-leader
                // redirects immediately.
                let seq = req.seq;
                let client = req.client;
                match node.propose(ClientId(req.client), ClientSeq(req.seq), Value(req.command)) {
                    ProposeResult::NotLeader(hint) => {
                        // A lost redirect is a legal outcome: the client's
                        // deadline turns it into a retry elsewhere.
                        if hooks.drop_client_reply(Reply::ProposeRedirect) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ProposeRedirect);
                            tracing::info!(node = self_id, reply = "propose_redirect", "client_reply_dropped");
                        } else {
                            let _ = reply.send(ProposeAck { seq, leader: hint.map(|n| n.0), committed: false, slot: None });
                        }
                    }
                    ProposeResult::Accepted(slot) | ProposeResult::Duplicate(slot) => {
                        waiters.pending.entry(slot).or_default().push((client, seq, reply));
                    }
                    ProposeResult::Chosen(slot) => {
                        // Already inside this node's applied prefix before this
                        // call, so the ack fires immediately — and it *names* the
                        // slot, exactly like the ack-on-commit path. A committed
                        // ack that named nothing was unfalsifiable: the client was
                        // told "applied" with no way for an oracle to check the
                        // claim against the applied prefix.
                        audit.client_acked(NodeId(self_id), client, seq, slot, storage.applied_slot(), true);
                        tracing::info!(node = self_id, slot = slot.0, "propose_dedup_ack");
                        if hooks.drop_client_reply(Reply::ProposeDedup) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ProposeDedup);
                            tracing::info!(node = self_id, reply = "propose_dedup", "client_reply_dropped");
                        } else {
                            let _ = reply.send(ProposeAck { seq, leader: Some(self_id), committed: true, slot: Some(slot.0) });
                        }
                    }
                }
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
            }
            Some((req, reply)) = rpc.read.recv() => {
                // A client read via read-index: the leader captures its applied
                // watermark, confirms it is still leader with a heartbeat-ack
                // quorum round (no log write), and the reply is parked until the
                // confirmed `ReadState` surfaces after apply — a deposed or
                // freshly elected leader can no longer serve a stale watermark.
                // A non-leader redirects immediately.
                let seq = req.seq;
                match node.read_index(next_read_ctx) {
                    ReadIndexResult::NotLeader(hint) => {
                        if hooks.drop_client_reply(Reply::ReadRedirect) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ReadRedirect);
                            tracing::info!(node = self_id, reply = "read_redirect", "client_reply_dropped");
                        } else {
                            let _ = reply.send(ReadAck { seq, leader: hint.map(|n| n.0), committed: false, read_index: None });
                        }
                    }
                    ReadIndexResult::Pending => {
                        waiters.pending_reads.insert(next_read_ctx, (seq, ticks, reply));
                        next_read_ctx += 1;
                    }
                }
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
            }
            Some((msg, reply)) = rpc.deliver.recv() => {
                // A peer Paxos message → the core's single input router. The same
                // `paros_core::Message` is sent and received (no DTO). Surface the
                // arrival (mirror of `msg_sent`) so the demo can pair sends with
                // receives and mark the unmatched ones as network drops.
                let kind = message_kind(&msg);
                match message_route(&msg) {
                    Some((from, config_id, ballot, Some(slot))) => tracing::info!(
                        node = self_id,
                        from = from.0,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        slot = slot.0,
                        config_id = config_id.0,
                        "msg_received"
                    ),
                    // The empty-prefix beat: no slot field, mirroring `msg_sent`.
                    Some((from, config_id, ballot, None)) => tracing::info!(
                        node = self_id,
                        from = from.0,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        config_id = config_id.0,
                        "msg_received"
                    ),
                    None => {
                        if let Some(config_id) = msg.config_id() {
                            tracing::info!(
                                node = self_id,
                                kind,
                                config_id = config_id.0,
                                "msg_received"
                            );
                        } else {
                            tracing::info!(node = self_id, kind, "msg_received");
                        }
                    }
                }
                // Canary: a Prepare whose from_slot is below our floor is the
                // dangerous "campaign against a truncated acceptor" case. Record it
                // so the sweep can assert the interleaving stays reachable once the
                // acceptor floor guard is in place.
                if let Message::Prepare { from_slot, .. } = &msg
                    && *from_slot < node.first_slot()
                {
                    audit.prepare_below_floor(NodeId(self_id), *from_slot, node.first_slot());
                    tracing::info!(
                        node = self_id,
                        from_slot = from_slot.0,
                        floor = node.first_slot().0,
                        "prepare_below_floor"
                    );
                }
                // Snapshot-repair traffic is driver-terminal (#101): handled
                // here, never stepped into the core — consensus state must
                // not depend on snapshot custody.
                // A snap-repair message naming a foreign configuration is
                // ignored (guarded, never asserted — wire input): custody and
                // chunk bytes are only meaningful within one configuration.
                let snap_handled = match &msg {
                    Message::SnapAck {
                        config_id,
                        from,
                        at_index,
                    } => {
                        // Custody counts toward the coupling quorum only from
                        // members of the active configuration.
                        if *config_id == node.hard_state().config_id
                            && node.is_leader()
                            && node.acceptors().contains(*from)
                        {
                            snap.acks.entry(*at_index).or_default().insert(*from);
                        }
                        true
                    }
                    Message::SnapChunkRequest {
                        config_id,
                        from,
                        at_index,
                        chunks,
                    } => {
                        if *config_id == node.hard_state().config_id {
                            handle_snap_chunk_request(
                                &node, &storage, &out, hooks, audit, *from, *at_index, chunks,
                            );
                        }
                        true
                    }
                    Message::SnapChunkResponse {
                        config_id,
                        from,
                        at_index,
                        chunks,
                    } => {
                        // Pool-checked: only a pooled node's chunk bytes are
                        // installed (a replica outside the active
                        // configuration holds the same decided point).
                        if *config_id == node.hard_state().config_id
                            && node.config().pool().contains(from)
                        {
                            let at = *at_index;
                            let chunks = chunks.clone();
                            handle_snap_chunk_response(
                                &mut node,
                                &mut storage,
                                &mut snap,
                                hooks,
                                audit,
                                self_id,
                                at,
                                &chunks,
                            )?;
                        }
                        true
                    }
                    _ => false,
                };
                if !snap_handled {
                    node.step(msg);
                }
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
                let _ = reply.send(());
            }
            Some(reply) = match_replies.recv() => {
                // A matchmaker's answer to this candidate's registration (#120):
                // fold it into the open matchmaking phase; a quorum closes the
                // phase and opens Phase 1 in the same step.
                let (matchmaker, ballot) = (reply.matchmaker, reply.ballot);
                tracing::info!(
                    node = self_id,
                    matchmaker = matchmaker.0,
                    round = ballot.round,
                    registered = matches!(reply.outcome, paros_core::MatchOutcome::Registered { .. }),
                    "match_reply_received"
                );
                let step = node.on_match_reply(reply);
                report_match_step(&node, audit, self_id, matchmaker, ballot, &step);
                // The two straggler paths of a handover (#125), taken by
                // whichever node meets them: a registry frozen with no
                // successor is finished by this node (the reconfigurer's
                // decree adopts whatever was voted, or re-chooses the same
                // members under a fresh generation); a member left inactive
                // or behind is told the chosen set this node already knows.
                match &step {
                    // Sound to finish *this* node's believed set: a
                    // matchmaker answers `Stopped { successor: None }` only
                    // when the generation it froze is the one the request
                    // named (`Matchmaker::generation_refusal` answers a
                    // mismatch with `Generation { current }` or `Inactive`
                    // instead), so the generation this node is finishing is
                    // exactly the one it believes in force.
                    MatchStep::Refused(MatchRefusal::Stopped { successor: None }) if !handover.is_busy() => {
                        let current = node.matchmaker_set().clone();
                        if handover.finish(&current).is_ok() {
                            audit.reconfigurer_started(NodeId(self_id), &current, &current.members);
                            tracing::info!(
                                node = self_id,
                                generation = current.generation.0,
                                target = current.members.len() as u64,
                                finishing = true,
                                "reconfigurer_started"
                            );
                            send_reconfigure_requests(&providers, &links, audit, self_id, handover.take_requests());
                        }
                    }
                    MatchStep::Refused(MatchRefusal::Inactive | MatchRefusal::Generation { .. }) => {
                        let set = node.matchmaker_set().clone();
                        let behind = match &step {
                            MatchStep::Refused(MatchRefusal::Generation { current }) => current.generation < set.generation,
                            _ => true,
                        };
                        if behind && set.generation.0 > 0 {
                            audit.successor_republished(NodeId(self_id), matchmaker, &set);
                            tracing::info!(node = self_id, matchmaker = matchmaker.0, generation = set.generation.0, "successor_republished");
                            let request = ReconfigureRequest::Chosen {
                                from: NodeId(self_id),
                                generation: MatchmakerGeneration(set.generation.0 - 1),
                                successor: set,
                            };
                            send_reconfigure_requests(&providers, &links, audit, self_id, vec![(matchmaker, request)]);
                        }
                    }
                    _ => {}
                }
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
            }
            Some(ack) = gc_acks.recv() => {
                // A matchmaker's answer to this leader's GC request (#123):
                // fold it; a quorum makes the floor effective and names the
                // retirable acceptors (reported in the step).
                let step = node.on_gc_ack(&ack);
                audit.gc_step(NodeId(self_id), ack.matchmaker, &ack, &step);
                tracing::info!(
                    node = self_id,
                    matchmaker = ack.matchmaker.0,
                    generation = ack.generation.0,
                    applied = ack.applied,
                    round = ack.watermark.round,
                    step = ?step,
                    "gc_ack_received"
                );
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
            }
            Some(reply) = reconfigure_replies.recv() => {
                // A matchmaker's answer to this node's handover step (#125).
                let matchmaker = reply.matchmaker();
                let step = handover.on_reply(reply.clone());
                audit.reconfigurer_step(NodeId(self_id), matchmaker, &reply, &step);
                tracing::info!(
                    node = self_id,
                    matchmaker = matchmaker.0,
                    reply = crate::matchmaker::reconfigure_reply_kind(&reply),
                    step = ?step,
                    "reconfigurer_step"
                );
                // The chosen set is authoritative the instant it is chosen —
                // this node adopts it before its publication completes.
                if let ReconfigurerStep::Chosen { successor }
                | ReconfigurerStep::Done { successor }
                | ReconfigurerStep::Superseded { successor } = &step
                {
                    node.learn_matchmakers(successor);
                }
                if let ReconfigurerStep::Preempted { .. } = &step {
                    let base = tunables.election_timeout_base.max(1);
                    let ticks = providers.random().random_range(1..base * 2 + 1);
                    handover.back_off(ticks);
                    audit.reconfigurer_backoff(NodeId(self_id), ticks);
                    tracing::info!(node = self_id, ticks, "reconfigurer_backoff");
                }
                send_reconfigure_requests(&providers, &links, audit, self_id, handover.take_requests());
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
            }
            Some((req, reply)) = rpc.reconfigure_matchmakers.recv() => {
                // No settle tail: this arm drives the reconfigurer, never the
                // core, so there is no `Ready` batch to drain.
                // A matchmaker-set reconfiguration (#125): any node may drive
                // it. Refusable like every operator request; a started
                // handover runs to completion on this node's own cadence.
                let target: Vec<MatchmakerId> = req.members.iter().copied().map(MatchmakerId).collect();
                let refusal = if !node.config().has_matchmakers() {
                    "no_matchmakers"
                } else if target.is_empty() {
                    "empty"
                } else if !target.iter().all(|m| links.clients.contains_key(m)) {
                    "unknown_matchmaker"
                } else {
                    match handover.start(node.matchmaker_set(), target.clone()) {
                        Ok(()) => "",
                        Err(StartRefusal::Busy) => "busy",
                        Err(StartRefusal::Empty) => "empty",
                        Err(StartRefusal::Malformed) => "malformed",
                    }
                };
                let generation = node.matchmaker_set().generation.0;
                if refusal.is_empty() {
                    audit.reconfigurer_started(NodeId(self_id), node.matchmaker_set(), &target);
                    tracing::info!(
                        node = self_id,
                        generation,
                        target = target.len() as u64,
                        finishing = false,
                        "reconfigurer_started"
                    );
                    send_reconfigure_requests(&providers, &links, audit, self_id, handover.take_requests());
                }
                audit.reconfigure_matchmakers_acked(NodeId(self_id), refusal);
                tracing::info!(node = self_id, accepted = refusal.is_empty(), refusal, "reconfigure_matchmakers_acked");
                if hooks.drop_client_reply(Reply::ReconfigureMatchmakers) {
                    audit.client_reply_dropped(NodeId(self_id), Reply::ReconfigureMatchmakers);
                    tracing::info!(node = self_id, reply = "reconfigure_matchmakers", "client_reply_dropped");
                } else {
                    let _ = reply.send(ReconfigureMatchmakersAck {
                        accepted: refusal.is_empty(),
                        refusal: refusal.to_string(),
                        generation: refusal.is_empty().then_some(generation),
                    });
                }
            }
            Some((_req, reply)) = rpc.retire.recv() => {
                // No settle tail: this arm only reads the core and arms a flag
                // the tick arm acts on, so it produces no `Ready` batch.
                // Operator decommissioning (#123): a node a leader's GC named
                // retirable is shut down for good. Refused while this node is
                // a member of the configuration it believes in force, or
                // leads: "retirable" is never "still needed".
                let accepted = node.config().has_matchmakers() && !node.is_acceptor() && !node.is_leader();
                audit.retire_acked(NodeId(self_id), accepted);
                tracing::info!(node = self_id, accepted, "retire_acked");
                if accepted {
                    retiring = true;
                }
                if hooks.drop_client_reply(Reply::Retire) {
                    audit.client_reply_dropped(NodeId(self_id), Reply::Retire);
                    tracing::info!(node = self_id, reply = "retire", "client_reply_dropped");
                } else {
                    let _ = reply.send(RetireAck {
                        accepted,
                        refusal: if accepted { String::new() } else { "member".to_string() },
                    });
                }
            }
            Some((req, reply)) = rpc.reconfigure.recv() => {
                // An online reconfiguration (#122): the leader moves to a fresh
                // ballot registered with the new acceptor set. Refusable like
                // `Compact` — a non-leader redirects, a plain deployment
                // refuses outright, and an unsettled leadership asks the
                // client to retry.
                let members: Vec<NodeId> = req.members.iter().copied().map(NodeId).collect();
                let result = if members.is_empty() {
                    ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember)
                } else {
                    node.reconfigure(&AcceptorConfig::new(members.clone(), QuorumSystem::Majority))
                };
                audit.reconfigure_acked(NodeId(self_id), &members, result);
                let (accepted, refusal, round) = match result {
                    ReconfigureResult::Started(ballot) => (true, "", Some(ballot.round)),
                    ReconfigureResult::NotLeader(_) => (false, "not_leader", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::NoMatchmakers) => (false, "no_matchmakers", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::Unchanged) => (false, "unchanged", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember) => (false, "unknown_member", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::Malformed) => (false, "malformed", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::Unsettled) => (false, "unsettled", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::RoundExhausted) => (false, "round_exhausted", None),
                };
                let leader = match result {
                    ReconfigureResult::NotLeader(hint) => hint.map(|n| n.0),
                    _ => Some(self_id),
                };
                tracing::info!(
                    node = self_id,
                    members = members.len() as u64,
                    accepted,
                    refusal,
                    round = round.unwrap_or(0),
                    "reconfigure_acked"
                );
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
                // A lost reconfiguration ack is ambiguous to the client, which
                // re-asks; a started reconfiguration stands (a retry is refused
                // as `not_leader` while it runs, then `unchanged` once done).
                if hooks.drop_client_reply(Reply::Reconfigure) {
                    audit.client_reply_dropped(NodeId(self_id), Reply::Reconfigure);
                    tracing::info!(node = self_id, reply = "reconfigure", "client_reply_dropped");
                } else {
                    let _ = reply.send(ReconfigureAck { leader, accepted, refusal: refusal.to_string(), round });
                }
            }
            Some((req, reply)) = rpc.compact.recv() => {
                // The application permits dropping the log prefix up to `up_to`.
                // Only the leader admits it: it proposes a `Truncate` control
                // command into the next slot, decided by ordinary Paxos and
                // forwarded to every node, each of which truncates lazily when it
                // applies that slot. A non-leader redirects (like `propose`).
                //
                // The coupling rule (#101, CTRL §3.5): a `Truncate{up_to}` is
                // proposed only once a quorum holds the decided snapshot at
                // (or past) `up_to` — that is what makes chunk repair sound
                // once the log below the floor is gone. A request no decided
                // point covers first seeds a `Snap` marker and answers
                // `accepted: false`; the client's retry finds the point once
                // the quorum's custody advertisements land. Proposal-side
                // policy only — the acceptor paths stay fully opaque.
                let ack = if node.is_leader() {
                    // The quorum question goes through the configuration in
                    // force, never a raw count: an ack from a node the current
                    // acceptor set no longer names does not witness custody.
                    let covered = snap
                        .acks
                        .iter()
                        .filter(|(_, holders)| node.acceptors().has_quorum(holders))
                        .map(|(&point, _)| point)
                        .max();
                    let propose_marker = |node: &mut RawNode, snap: &mut SnapRepair| {
                        if snap.marker_pending.is_none()
                            && let ProposeResult::Accepted(slot) = node.propose_snap_marker()
                        {
                            snap.marker_pending = Some(slot);
                            tracing::info!(node = self_id, at = slot.0, "snap_marker_proposed");
                        }
                    };
                    if let Some(point) = covered {
                        let up_to = Slot(req.up_to.min(point.0));
                        // Honest ack: `accepted: true` only when the Truncate
                        // proposal was actually admitted. `propose_control`
                        // can refuse (a step-down raced this request), and the
                        // client's retry handles `accepted: false` exactly
                        // like the coupling refusal below.
                        let proposed = matches!(
                            node.propose_control(Control::Truncate { up_to }),
                            ProposeResult::Accepted(_)
                        );
                        tracing::info!(
                            node = self_id,
                            requested = req.up_to,
                            up_to = up_to.0,
                            point = point.0,
                            accepted = proposed,
                            "truncate_coupled_to_snap_point"
                        );
                        if req.up_to > point.0 {
                            // The request outruns the covered prefix: seed the
                            // next point so a later compact can go further.
                            propose_marker(&mut node, &mut snap);
                        }
                        CompactAck {
                            leader: Some(self_id),
                            accepted: proposed,
                            first_slot: node.first_slot().0,
                        }
                    } else {
                        propose_marker(&mut node, &mut snap);
                        CompactAck {
                            leader: Some(self_id),
                            accepted: false,
                            first_slot: node.first_slot().0,
                        }
                    }
                } else {
                    CompactAck {
                        leader: node.leader().map(|n| n.0),
                        accepted: false,
                        first_slot: node.first_slot().0,
                    }
                };
                audit.compact_acked(NodeId(self_id), ack.accepted);
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
                // A lost compaction ack is ambiguous to the client, which
                // re-asks; the marker/Truncate it may have seeded stands.
                if hooks.drop_client_reply(Reply::Compact) {
                    audit.client_reply_dropped(NodeId(self_id), Reply::Compact);
                    tracing::info!(node = self_id, reply = "compact", "client_reply_dropped");
                } else {
                    let _ = reply.send(ack);
                }
            }
            Some((_req, reply)) = rpc.inspect.recv() => {
                // No settle tail: an inspect is a pure read of the core and the
                // store, so it produces no `Ready` batch.
                let since = node.acceptors_since();
                let set = node.matchmaker_set();
                let (gc_watermark, retirable) = node.gc_effective().map_or((None, Vec::new()), |(w, retired)| {
                    (
                        Some(common::Ballot { round: w.round, node: w.node.0 }),
                        retired.iter().map(|n| n.0).collect(),
                    )
                });
                let _ = reply.send(InspectReply {
                    chosen_index: node.hard_state().chosen_index.map(|slot| slot.0),
                    first_slot: node.first_slot().0,
                    snapshot: storage.snapshot(),
                    members: node.acceptors().members.iter().map(|n| n.0).collect(),
                    config_ballot: Some(common::Ballot { round: since.round, node: since.node.0 }),
                    leader: node.is_leader(),
                    matchmaker_generation: set.generation.0,
                    matchmakers: set.members.iter().map(|m| m.0).collect(),
                    retirable,
                    gc_watermark,
                });
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                // Pacing, not a protocol bound: every timeout the core owns is
                // counted in ticks, so a node that waits twice as long between
                // ticks is exactly a slow node. Stretching desynchronizes the
                // cluster's protocol clocks — a shape moonpool's clock skew
                // reaches only for the wall clock, never for the tick counter
                // the election and read-round timers actually run on.
                next_tick = time.now()
                    + if hooks.stretch_tick_interval() { tunables.tick_interval * 2 } else { tunables.tick_interval };
                if retiring {
                    // The operator's decommissioning takes effect: this
                    // incarnation ends and never comes back as this identity.
                    audit.retired(NodeId(self_id));
                    tracing::info!(node = self_id, "retired");
                    return Ok(());
                }
                node.tick();
                // Consult each hook only when its decision can have an effect.
                // Production's hooks are false; simulation gives each decision
                // an independent BUGGIFY location.
                if node.has_pending_accepts() {
                    if hooks.skip_accept_resend() {
                        audit.resend_skipped(NodeId(self_id));
                        tracing::info!(node = self_id, "accept_resend_skipped");
                    } else {
                        node.resend_pending();
                    }
                }
                // The open matchmaking request's re-send (#120): paced by
                // `match_resend_ticks`, and its own BUGGIFY location — consulted
                // only when a re-send is due, so a skip always costs a beat.
                if node.matchmaking_pending() {
                    match_resend_elapsed += 1;
                    if match_resend_elapsed >= tunables.match_resend_ticks.max(1) {
                        match_resend_elapsed = 0;
                        if hooks.skip_matchmaking_resend() {
                            audit.matchmaking_resend_skipped(NodeId(self_id));
                            tracing::info!(node = self_id, "matchmaking_resend_skipped");
                        } else {
                            node.resend_matchmaking();
                        }
                    }
                } else {
                    match_resend_elapsed = 0;
                }
                // The open GC request's re-send (#123): the same cadence as
                // matchmaking, its own BUGGIFY location.
                if node.gc_pending() {
                    gc_resend_elapsed += 1;
                    if gc_resend_elapsed >= tunables.match_resend_ticks.max(1) {
                        gc_resend_elapsed = 0;
                        if hooks.skip_gc_resend() {
                            audit.gc_resend_skipped(NodeId(self_id));
                            tracing::info!(node = self_id, "gc_resend_skipped");
                        } else {
                            node.resend_gc();
                        }
                    }
                } else {
                    gc_resend_elapsed = 0;
                }
                // The running handover's re-send (#125): the same cadence,
                // its own location; a preempted decree reopens only here, so
                // two dueling reconfigurers are paced by their drivers.
                handover.tick();
                let stall_budget = node.election_timeout().saturating_mul(RECONFIGURE_TIMEOUT_ELECTIONS);
                if handover.is_busy()
                    && stall_budget != 0
                    && handover.stalled_for() >= stall_budget
                    && handover.abandon()
                {
                    // A phase that no member answers any more (a lost
                    // registry, a machine gone) is abandoned: the frozen
                    // generation is finished by the next node to meet it,
                    // with the members that do answer.
                    audit.reconfigurer_aborted(NodeId(self_id));
                    tracing::info!(node = self_id, "reconfigurer_aborted");
                }
                if handover.resend_due(tunables.match_resend_ticks) {
                    if hooks.skip_reconfigurer_resend() {
                        audit.reconfigurer_resend_skipped(NodeId(self_id));
                        tracing::info!(node = self_id, "reconfigurer_resend_skipped");
                    } else {
                        handover.resend();
                        send_reconfigure_requests(&providers, &links, audit, self_id, handover.take_requests());
                    }
                }
                // Cooperative leader handoff (`DPaxos`): move the existing
                // Phase-2 authority to another physical node instead of letting
                // an election destroy it and make the successor rediscover the
                // log through Phase 1. Consulted only when the core says the
                // leadership is transferable, so a `true` always has an effect;
                // answering `false` is always safe (a handoff is an
                // optimization, never a requirement). Offered *before* the
                // resignation hook: both give up the leadership, and the
                // cooperative one is strictly the more interesting outcome.
                let mut handed_off = false;
                if node.can_relinquish() {
                    let candidates = node.handoff_candidates();
                    if !candidates.is_empty() {
                        let ctx = handoff_context(&node, candidates.len());
                        if hooks.initiate_handoff(ctx) {
                            let fallback = providers.random().random_range(0..candidates.len());
                            let target = hooks
                                .handoff_target(&candidates)
                                .filter(|t| candidates.contains(t))
                                .unwrap_or(candidates[fallback]);
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
                }
                if !handed_off && node.role() == NodeRole::Leader && hooks.resign_leadership() {
                    audit.stepped_down(NodeId(self_id));
                    tracing::info!(node = self_id, "leadership_resigned");
                    node.step_down();
                }
                // Snapshot-point repair upkeep (#101): custody advertisement,
                // the leader's coupling tally, and the chunk-repair pull.
                snap_repair_tick(&node, &storage, &out, hooks, audit, &mut snap);
                ticks += 1;
                // Expire parked reads whose confirmation is overdue (lost acks, a
                // minority-partitioned leader that never steps down): answer a
                // retry redirect while the client still has deadline left. A late
                // core confirmation finds the ctx gone and is ignored. The
                // early-expiry hook (consulted only while reads are parked)
                // takes the same exit before the deadline.
                let expire_all = !waiters.pending_reads.is_empty() && hooks.expire_parked_read_early();
                // `(ctx, early)`: `early` marks a read the hook expired while
                // its deadline still had ticks left — the audit keeps the two
                // exits apart.
                let overdue: Vec<(u64, bool)> = waiters.pending_reads
                    .iter()
                    .filter_map(|(ctx, (_, parked_at, _))| {
                        let by_deadline = ticks.saturating_sub(*parked_at) > tunables.read_retry_ticks;
                        (expire_all || by_deadline).then_some((*ctx, !by_deadline))
                    })
                    .collect();
                for (ctx, early) in overdue {
                    if let Some((seq, _, waiter)) = waiters.pending_reads.remove(&ctx) {
                        audit.read_expired(NodeId(self_id), early);
                        if hooks.drop_client_reply(Reply::ReadRedirect) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ReadRedirect);
                            tracing::info!(node = self_id, reply = "read_redirect", "client_reply_dropped");
                        } else {
                            let _ = waiter.send(ReadAck { seq, leader: Some(self_id), committed: false, read_index: None });
                        }
                    }
                }
                loop_ctx.settle(&mut node, &mut storage, &mut waiters, &mut last)?;
                // Surface a chosen slot stranded above the applied prefix. The
                // `Ready` handshake only ever hands out the *contiguous* prefix, so
                // a hole below a chosen slot is otherwise invisible from outside the
                // core. Re-emitted every tick while it lasts: the oracle reads its
                // persistence past quiescence, not a single instant.
                if let Some((hole, above)) = node.chosen_gap() {
                    audit.chosen_gap(NodeId(self_id), hole, above);
                    tracing::info!(node = self_id, hole = hole.0, above = above.0, "chosen_gap");
                }
                audit.ticked(NodeId(self_id));
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}
