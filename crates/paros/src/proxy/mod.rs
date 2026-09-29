//! The provider-generic **proxy leader driver** (#142, Compartmentalized
//! Paxos §3.1) — the I/O layer that owns a sans-IO
//! [`paros_core::ProxyLeader`], the third driver beside
//! [`run_node`](crate::run_node) and [`run_matchmaker`](crate::run_matchmaker).
//!
//! Written once over moonpool's `P: Providers`, so the *same* loop runs in
//! production and deterministic simulation; the harness adapts a moonpool
//! `Process` to it exactly as it adapts the node. The loop serves the node
//! contract's **Phase-2 subset** — it receives the leader's delegated
//! `Accept`s and the acceptors' `Accepted`s and `Nack`s through the same
//! `Deliver` lane a node does — feeds each one into the core, and puts every
//! batch the core produces on the wire through the same lossy keep-newest
//! per-peer mailboxes the node driver sends through: the `Accept` fanned out
//! to a column, the `Commit` to every learner, a relayed `Nack` to the
//! delegating leader. On each beat it first evicts the rounds the core has
//! re-fanned-out `DriverTunables::proxy_round_resends` times without an
//! answer ([`ProxyLeader::expire_stale`] — bounded retention: a slot every
//! acceptor compacted past is never answered, and an eviction is not a
//! decision), then asks the core to re-fan-out the rest
//! ([`ProxyLeader::resend_pending`]; the driver's [`DriverHooks`] may skip
//! a beat).
//!
//! **Nothing here is durable.** A proxy has no storage seam, no boot scan,
//! no format marker and no crash seam: it reboots empty, and the leader's
//! next re-delegation rebuilds every round it still needs. The only typed
//! exit is [`RunError::Infra`].
//!
//! A deployment whose `Config::proxy_count` is zero never runs this loop;
//! the node driver then delegates nothing and the wire carries exactly the
//! plain deployment's messages.

use std::collections::BTreeMap;

use moonpool_core::{Providers, SimulationResult, TimeProvider};
use paros_core::{
    AcceptorConfig, Audience, Ballot, Message, NodeId, Party, ProxyId, ProxyLeader, Slot,
};
use tokio_util::sync::CancellationToken;

use crate::audit::{Audit, DelegationOutcome};
use crate::driver::edge::{RpcEdge, edge_reporter};
use crate::driver::events::{command_hash, message_kind, message_route};
use crate::driver::transport::{LaneOpener, Outbound, PeerQueues, peer_address, send_messages};
use crate::driver::{DriverTunables, RunError};
use crate::hooks::DriverHooks;
use crate::rpc::{serve_deliveries, well_known};

/// A proxy leader's deployment data: its identity in the proxy namespace and
/// the bootstrap acceptor configuration it fans out to until a delegation
/// teaches it a later one (a proxy takes part in no Phase 1 and hears no
/// beat, so on a matchmaker deployment the delegation carries `C_b`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyConfig {
    /// This proxy's identity: `ProxyId(0..proxy_count)` of the deployment.
    pub id: ProxyId,
    /// The bootstrap acceptor configuration (the node's `Config::peers`
    /// under its quorum system).
    pub acceptors: AcceptorConfig,
}

/// The coordinates of a delegated `Accept` the report needs once the core
/// has consumed the message: its leader, ballot, slot and command hash.
#[derive(Clone, Copy)]
struct DelegatedAccept {
    leader: NodeId,
    ballot: Ballot,
    slot: Slot,
    vhash: u64,
}

impl DelegatedAccept {
    /// Read the report's coordinates off `msg` before it is stepped; `None`
    /// for anything but an `Accept`.
    fn of(msg: &Message) -> Option<Self> {
        let Message::Accept {
            leader,
            ballot,
            slot,
            command,
            ..
        } = msg
        else {
            return None;
        };
        Some(Self {
            leader: *leader,
            ballot: *ballot,
            slot: *slot,
            vhash: command_hash(command),
        })
    }
}

/// Report what one delegated `Accept` did at the proxy, read off the core's
/// own counters before and after the step.
fn report_delegation<A: Audit>(
    audit: &A,
    id: ProxyId,
    before: paros_core::proxy_leader::ProxyCounters,
    after: paros_core::proxy_leader::ProxyCounters,
    accept: Option<DelegatedAccept>,
) {
    let Some(DelegatedAccept {
        leader,
        ballot,
        slot,
        vhash,
    }) = accept
    else {
        return;
    };
    if after.superseded > before.superseded {
        let count = after.superseded - before.superseded;
        audit.proxy_rounds_superseded(id, count);
        tracing::info!(
            proxy = id.0,
            round = ballot.round,
            bnode = ballot.node.0,
            count,
            "proxy_rounds_superseded"
        );
    }
    let outcome = if after.delegated > before.delegated {
        DelegationOutcome::Opened
    } else if after.refanned > before.refanned {
        DelegationOutcome::Refanned
    } else {
        DelegationOutcome::Ignored
    };
    audit.proxy_delegated(id, leader, slot, ballot, vhash, outcome);
    tracing::info!(
        proxy = id.0,
        from = leader.0,
        round = ballot.round,
        bnode = ballot.node.0,
        slot = slot.0,
        vhash,
        outcome = ?outcome,
        "proxy_delegated"
    );
}

/// Run the [`ProxyReady`](paros_core::ProxyReady) handshake once: report
/// every message the batch carries at the instant it is about to leave —
/// each fan-out, each decision, each relayed refusal — resolve every
/// audience through the deployment map as a proxy sends
/// ([`Audience::resolve_from_proxy`]: a proxy is nobody's peer, so the
/// leader hears its own delegation's fan-out when it sits in the column;
/// a `Commit` to the learners reaches the deployment's replicas too),
/// and send. Nothing to persist: the batch is messages alone.
#[tracing::instrument(level = "trace", skip_all, fields(proxy = proxy.id().0))]
fn drain<H: DriverHooks, A: Audit>(
    proxy: &mut ProxyLeader,
    pool: &[NodeId],
    out: &Outbound,
    hooks: &H,
    audit: &A,
) {
    let id = proxy.id();
    let ready = proxy.ready();
    let mut messages: Vec<(Party, Message)> = Vec::new();
    for (audience, msg) in ready.messages() {
        let addressees = out.resolve(audience, pool);
        match (audience, msg) {
            (
                Audience::AcceptorsOf { column, .. },
                Message::Accept {
                    leader,
                    ballot,
                    slot,
                    command,
                    ..
                },
            ) => {
                let vhash = command_hash(command);
                audit.proxy_fanned_out(
                    id,
                    *leader,
                    *slot,
                    *ballot,
                    vhash,
                    *column,
                    addressees.len(),
                );
                tracing::info!(
                    proxy = id.0,
                    leader = leader.0,
                    round = ballot.round,
                    bnode = ballot.node.0,
                    slot = slot.0,
                    vhash,
                    column = column.map_or(-1_i64, |c| i64::try_from(c).unwrap_or(i64::MAX)),
                    addressees = addressees.len() as u64,
                    "proxy_fanned_out"
                );
            }
            (
                Audience::Learners,
                Message::Commit {
                    ballot,
                    slot,
                    command,
                    ..
                },
            ) => {
                let vhash = command_hash(command);
                audit.proxy_decided(id, *slot, *ballot, vhash);
                tracing::info!(
                    proxy = id.0,
                    round = ballot.round,
                    bnode = ballot.node.0,
                    slot = slot.0,
                    vhash,
                    "proxy_decided"
                );
            }
            (Audience::Node(leader), Message::Nack { ballot, slot, .. }) => {
                audit.proxy_nack_relayed(id, *leader, *slot, *ballot);
                tracing::info!(
                    proxy = id.0,
                    leader = leader.0,
                    round = ballot.round,
                    bnode = ballot.node.0,
                    slot = slot.0,
                    "proxy_nack_relayed"
                );
            }
            _ => {}
        }
        messages.extend(
            addressees
                .into_iter()
                .map(|to| (Party::Node(to), msg.clone())),
        );
    }
    ready.advance();
    send_messages(out, hooks, audit, messages);
}

/// Drive a paros proxy leader to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation). The loop owns a
/// [`ProxyLeader`] built from `config`, serves the node contract's Phase-2
/// subset on `local_addr`, and sends to the acceptors and learners named in
/// `members` — the full **node pool** (`NodeId` → address), the same list
/// every node is given, so a fan-out to a column and a `Commit` to every
/// learner resolve through the same deployment map. `replicas` is the
/// deployment's replica tier (#144, `NodeId` → address, outside the pool;
/// empty without one): a `Commit` reaches them beside the pool. `tunables` supplies the
/// tick cadence, the retention budget (`proxy_round_resends`), the RPC
/// keep-alive and the mailbox shape; `hooks` and
/// `audit` are the provider-generic seams every driver in this crate takes,
/// with the proxy's own beat location ([`DriverHooks::skip_proxy_resend`])
/// and the send seam's per-message drop and duplicate locations.
///
/// # Errors
///
/// [`RunError::Infra`] for a genuine provider/infrastructure failure (bind,
/// accept, a bad address). A proxy has no storage and no crash seam, so no
/// other exit exists.
#[tracing::instrument(level = "debug", skip_all, fields(proxy = config.id.0, local_addr = %local_addr, members = members.len()))]
// The parameters are the proxy's complete wiring; a bundle would only rename
// them. The loop is one select over the contract's one inbox and the beat.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_proxy<P, H, A>(
    providers: P,
    local_addr: String,
    config: ProxyConfig,
    members: Vec<(NodeId, String)>,
    replicas: Vec<(NodeId, String)>,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    // Not `Send + 'static`, as on `run_node`: a hook is consulted from this
    // loop and never from a spawned task.
    H: DriverHooks,
    A: Audit + Clone + Send + Sync + 'static,
{
    let id = config.id;
    let me = Party::Proxy(id);

    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    // The edge serves the `Deliver` lane alone: nothing else a node serves
    // is a proxy leader's, and a call to it is refused `EndpointNotFound`.
    let mut edge = RpcEdge::listen(&providers, &local_addr, "proxy", &tunables).await?;
    let mut inbox = serve_deliveries(
        &providers,
        edge.handle(),
        tunables.peer_inbox_capacity,
        me,
        edge_reporter(audit, me),
        incarnation_shutdown.clone(),
    )?;

    // The sans-IO core: empty, over the bootstrap configuration. Every boot
    // is a first boot — there is nothing to recover — and it is reported so
    // an oracle can tie a rebooted proxy to the leader's re-delegations.
    let mut proxy = ProxyLeader::new(id, config.acceptors);
    audit.proxy_booted(id, proxy.acceptors());
    tracing::info!(
        proxy = id.0,
        members = proxy.acceptors().members().len() as u64,
        "proxy_booted"
    );

    // One lane per node of the pool — the acceptors the fan-outs reach and
    // the learners the `Commit`s reach are the same list — and one per
    // replica, which the `Commit`s reach too.
    let pool: Vec<NodeId> = members.iter().map(|(id, _)| *id).collect();
    let learners: Vec<NodeId> = replicas.iter().map(|(id, _)| *id).collect();
    let lanes = LaneOpener {
        providers: &providers,
        tunables,
        shutdown: incarnation_shutdown.clone(),
        audit,
        from: me,
    };
    let peer_queues = members
        .into_iter()
        .chain(replicas)
        .map(|(node, addr)| {
            let client = well_known(edge.handle(), peer_address(&addr)?);
            let regular = lanes.open(
                "paros-proxy-fanout",
                client,
                Party::Node(node),
                tunables.peer_queue_capacity,
            );
            Ok((node, PeerQueues { regular }))
        })
        .collect::<SimulationResult<BTreeMap<_, _>>>()?;
    let out = Outbound {
        peer_queues,
        proxy_queues: BTreeMap::new(),
        learners,
        sender: me,
    };

    let time = providers.time().clone();
    // An absolute tick deadline, for the reason `run_node` gives at its own
    // loop; the runtime's persistent future lives in the edge.
    let mut next_tick = time.now() + tunables.tick_interval;

    loop {
        moonpool_core::select! {
            error = edge.run() => return Err(error.into()),
            Some(msg) = inbox.recv() => {
                // A delegated `Accept`, an `Accepted`, a `Nack` → the core's
                // single input router; anything else is not a proxy's to
                // hear and the core ignores it.
                let kind = message_kind(&msg);
                if let Some((from, ballot, Some(slot))) = message_route(&msg) {
                    tracing::info!(
                        proxy = id.0,
                        from = %from,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        slot = slot.0,
                        "msg_received"
                    );
                } else {
                    tracing::info!(proxy = id.0, kind, "msg_received");
                }
                let accept = DelegatedAccept::of(&msg);
                let before = proxy.counters();
                proxy.step(msg);
                report_delegation(audit, id, before, proxy.counters(), accept);
                drain(&mut proxy, &pool, &out, hooks, audit);
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                next_tick = time.now() + tunables.tick_interval;
                // Bounded retention first: a round re-fanned-out the
                // budget's worth of beats without an answer is evicted (a
                // compacted slot is never answered; an eviction decides
                // nothing and the leader's re-delegation reopens a round it
                // still needs). Driver policy (`proxy_round_resends`, born
                // buggified); the core only counts.
                for slot in proxy.expire_stale(tunables.proxy_round_resends) {
                    audit.proxy_round_expired(id, slot);
                    tracing::info!(proxy = id.0, slot = slot.0, "proxy_round_expired");
                }
                // The beat: re-fan-out every open round. Consulted only
                // with rounds open, so a skip always costs a beat; skipping
                // is always safe (the leader's take-back is the liveness).
                if proxy.has_pending_accepts() {
                    if hooks.skip_proxy_resend() {
                        audit.proxy_resend_skipped(id);
                        tracing::info!(proxy = id.0, "proxy_resend_skipped");
                    } else {
                        proxy.resend_pending();
                        drain(&mut proxy, &pool, &out, hooks, audit);
                    }
                }
                tracing::info!(proxy = id.0, "proxy_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}
