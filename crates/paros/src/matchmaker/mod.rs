//! The provider-generic **matchmaker driver** — the I/O layer that owns a
//! sans-IO [`paros_core::Matchmaker`], the twin of [`run_node`](crate::run_node)
//! for the registry role.
//!
//! Written once over moonpool's `P: Providers`, so the *same* loop runs in
//! production and deterministic simulation; the harness adapts a moonpool
//! `Process` to it exactly as it adapts the node. The loop serves the
//! matchmaker gRPC contract, feeds each request into the core, and drains every
//! [`MatchmakerReady`](paros_core::MatchmakerReady) in **persist → fsync →
//! reply** order: a `Registered` reply leaves only once its registration is
//! durable, the registry's version of the acceptor's persist-before-`Promise`
//! rule. The registry is read back through the core's
//! [`RegistryStorage`](paros_core::RegistryStorage) port and written through
//! [`MatchmakerStorage`] — the node's `Storage` / `NodeStorage` split,
//! mirrored so the log's CTRL recovery applies to the registry (see
//! [`storage`]).
//!
//! A cluster deployed without matchmakers never runs this loop; the node
//! driver does not know it exists.

mod storage;

use moonpool_core::{
    Detach, NetworkProvider, Providers, SimulationError, TaskProvider, TcpListenerTrait,
};
use moonpool_hyper::{H2Server, H2ServerConfig, KeepAlive};
use paros_core::{
    AcceptorConfig, Ballot, MatchOutcome, MatchReply, Matchmaker, MatchmakerId, MatchmakerWriteOp,
    Registration,
};
use tokio_util::sync::CancellationToken;

use crate::audit::{Audit, StorageFaultDecision};
use crate::driver::{DriverTunables, RunError};
use crate::grpc::{MatchmakerInbox, ParosMatchmakerServer, matchmaker_channel};
use crate::hooks::{DriverHooks, Reply, Seam};
use crate::storage::StorageError;

pub use storage::{MatchmakerStorage, MemMatchmakerStorage, matchmaker_storage_contract_suite};

/// A stable digest of an acceptor configuration (FNV-1a over the sorted
/// membership and the quorum system), emitted on the audit callbacks so a
/// checker can compare configurations by equality without carrying them. The
/// same function on both ends: the driver hashes what it persists and what it
/// replies, an observer hashes what it sees on the wire.
#[must_use]
pub fn config_hash(config: &AcceptorConfig) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut fold = |bytes: &[u8]| {
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    fold(&(config.members.len() as u64).to_le_bytes());
    for member in &config.members {
        fold(&member.0.to_le_bytes());
    }
    fold(&[match config.quorum_system {
        paros_core::QuorumSystem::Majority => 0_u8,
    }]);
    h
}

/// Map a [`StorageError`] into the driver's deliberate crash decision, typed on
/// the audit at the instant it is made (the matchmaker twin of the node
/// driver's storage-fault crash).
fn storage_fault_crash<A: Audit>(audit: &A, id: MatchmakerId, e: StorageError) -> RunError {
    audit.matchmaker_storage_fault(id, &e, StorageFaultDecision::Crash);
    tracing::warn!(matchmaker = id.0, error = %e, decision = "crash", "matchmaker_storage_fault");
    RunError::Storage(e)
}

/// Run the [`MatchmakerReady`](paros_core::MatchmakerReady) handshake once:
/// persist the batch's writes, fsync them, report them, and hand back the
/// replies the caller may now send. The two crash seams sit exactly where a
/// real crash would matter: before the fsync (the batch is lost whole, no
/// reply was sent) and after it (the batch is durable, the reply never
/// leaves).
///
/// The order — writes, fsync, *then* replies — is the persist-before-reply
/// rule (invariant 1 of #119): a `Registered` reply that left before its
/// registration reached the disk would, across a crash at the seam between
/// the two, name a configuration the restarted matchmaker no longer holds —
/// a later leader's history would then silently omit it. The audit judges the
/// rule at the reply (`match_replied` must find the registration already
/// folded durable) and again at every restart (`matchmaker_recovered` must
/// read back every durable registration), and the two crash seams below are
/// the BUGGIFY locations that make both crash windows likely.
#[tracing::instrument(level = "trace", skip_all, fields(matchmaker = matchmaker.id().0))]
fn drain<S, H, A>(
    matchmaker: &mut Matchmaker,
    storage: &mut S,
    hooks: &H,
    audit: &A,
) -> Result<Vec<MatchReply>, RunError>
where
    S: MatchmakerStorage,
    H: DriverHooks,
    A: Audit,
{
    let id = matchmaker.id();
    let ready = matchmaker.ready();
    let writes = ready.writes().to_vec();
    let replies = ready.replies().to_vec();
    ready.advance();

    // 1. Persist every write, in order.
    for op in &writes {
        match op {
            MatchmakerWriteOp::Register {
                ballot,
                registration,
            } => storage
                .register(*ballot, registration)
                .map_err(|e| storage_fault_crash(audit, id, e))?,
            MatchmakerWriteOp::SetGcWatermark(watermark) => storage
                .set_gc_watermark(*watermark)
                .map_err(|e| storage_fault_crash(audit, id, e))?,
        }
    }
    if !writes.is_empty() {
        // Crash seam: staged but not flushed — the batch dies whole, and no
        // reply has been handed out yet.
        if hooks.crash_at(Seam::MatchBeforeSync) {
            audit.matchmaker_crashed(id, Seam::MatchBeforeSync);
            tracing::info!(
                matchmaker = id.0,
                seam = "match_before_sync",
                "matchmaker_crashed"
            );
            return Err(RunError::SeamCrash(Seam::MatchBeforeSync));
        }
        storage
            .sync()
            .map_err(|e| storage_fault_crash(audit, id, e))?;
        // Durable now — report the truthful persisted state.
        for op in &writes {
            match op {
                MatchmakerWriteOp::Register {
                    ballot,
                    registration,
                } => {
                    audit.match_registered(id, *ballot, registration);
                    tracing::info!(
                        matchmaker = id.0,
                        round = ballot.round,
                        bnode = ballot.node.0,
                        members = registration.config.members.len() as u64,
                        reconfiguration = registration.reconfiguration,
                        config = config_hash(&registration.config),
                        "match_registered"
                    );
                }
                MatchmakerWriteOp::SetGcWatermark(watermark) => {
                    audit.gc_watermark_raised(id, *watermark);
                    tracing::info!(
                        matchmaker = id.0,
                        round = watermark.round,
                        bnode = watermark.node.0,
                        "gc_watermark_raised"
                    );
                }
            }
        }
    }
    // 2. Crash seam: durable, but the reply never leaves. Only meaningful when
    //    there is a reply to lose.
    if !replies.is_empty() && hooks.crash_at(Seam::MatchAfterSyncBeforeReply) {
        audit.matchmaker_crashed(id, Seam::MatchAfterSyncBeforeReply);
        tracing::info!(
            matchmaker = id.0,
            seam = "match_after_sync_before_reply",
            "matchmaker_crashed"
        );
        return Err(RunError::SeamCrash(Seam::MatchAfterSyncBeforeReply));
    }
    Ok(replies)
}

/// Report one reply at the instant it leaves.
fn report_reply<A: Audit>(audit: &A, reply: &MatchReply) {
    let id = reply.matchmaker;
    match &reply.outcome {
        MatchOutcome::Registered {
            history,
            gc_watermark,
        } => {
            let history: Vec<(Ballot, Registration)> = history
                .iter()
                .map(|(ballot, registration)| (*ballot, registration.clone()))
                .collect();
            audit.match_replied(id, reply.to, reply.ballot, &history, *gc_watermark);
            tracing::info!(
                matchmaker = id.0,
                to = reply.to.0,
                round = reply.ballot.round,
                bnode = reply.ballot.node.0,
                history = history.len() as u64,
                watermark_round = gc_watermark.round,
                "match_replied"
            );
        }
        MatchOutcome::Refused(refusal) => {
            audit.match_refused(id, reply.to, reply.ballot, *refusal);
            tracing::info!(
                matchmaker = id.0,
                to = reply.to.0,
                round = reply.ballot.round,
                bnode = reply.ballot.node.0,
                reason = ?refusal,
                "match_refused"
            );
        }
    }
}

/// Drive a paros matchmaker to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation) and
/// `S: MatchmakerStorage` (the injected durable registry). The loop owns a
/// [`paros_core::Matchmaker`], serves the matchmaker gRPC contract on
/// `local_addr`, and answers each request only once its registration is
/// fsync-durable. `tunables` supplies the h2 keep-alive and inbox shape (the
/// matchmaker has no tick and no peers); `hooks` and `audit` are the same
/// provider-generic seams the node driver takes, with the matchmaker's own
/// crash locations ([`Seam::MatchBeforeSync`],
/// [`Seam::MatchAfterSyncBeforeReply`]) and reply-drop location
/// ([`Reply::Match`]).
///
/// # Errors
///
/// The exit is typed exactly like [`run_node`](crate::run_node)'s:
/// [`RunError::SeamCrash`] for a hook-injected crash at a durability seam (the
/// caller re-runs against the surviving storage), [`RunError::Storage`] for a
/// fail-stop storage fault, [`RunError::Infra`] for a genuine
/// provider/infrastructure failure.
///
/// # Panics
///
/// If the core breaks its one-request-one-reply contract (a programmer error,
/// never an operating condition).
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = id.0, local_addr = %local_addr))]
// The parameters are the matchmaker's complete wiring; a bundle would only
// rename them.
#[allow(clippy::too_many_arguments)]
pub async fn run_matchmaker<P, S, H, A>(
    providers: P,
    mut storage: S,
    local_addr: String,
    id: MatchmakerId,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    S: MatchmakerStorage,
    H: DriverHooks,
    A: Audit + Clone + Send + Sync + 'static,
{
    // Verify the store before the core reads it (the node driver's rule).
    storage
        .boot_scan()
        .map_err(|e| storage_fault_crash(audit, id, e))?;

    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    let listener = providers
        .network()
        .bind(&local_addr)
        .await
        .map_err(|e| SimulationError::InvalidState(format!("matchmaker gRPC listener: {e}")))?;

    // The sans-IO core, bootstrapped from durable storage through the
    // read-only port (scalars once, then record by record); re-report the
    // recovered registry so the oracles see this incarnation's belief.
    let mut matchmaker = Matchmaker::new(id, &storage);
    let recovered: Vec<(Ballot, Registration)> = matchmaker
        .registry()
        .iter()
        .map(|(ballot, registration)| (*ballot, registration.clone()))
        .collect();
    let watermark = matchmaker.hard_state().gc_watermark;
    audit.matchmaker_recovered(id, &recovered, watermark);
    tracing::info!(
        matchmaker = id.0,
        registrations = recovered.len() as u64,
        watermark_round = watermark.round,
        "matchmaker_booted"
    );

    let (service, mut inbox): (_, MatchmakerInbox) =
        matchmaker_channel(tunables.client_inbox_capacity);
    let grpc_service = tonic::service::Routes::new(ParosMatchmakerServer::new(service)).prepare();
    let grpc_server = H2Server::new(&providers).with_config(H2ServerConfig {
        keep_alive: Some(KeepAlive {
            interval: tunables.keep_alive_interval,
            timeout: tunables.keep_alive_timeout,
            while_idle: false,
        }),
        vectored_writes: true,
    });

    loop {
        moonpool_core::select! {
            accepted = listener.accept() => {
                let (stream, addr) = accepted
                    .map_err(|e| SimulationError::InvalidState(format!("matchmaker gRPC accept: {e}")))?;
                let connection = grpc_server.serve_connection_with_shutdown(
                    stream,
                    grpc_service.clone(),
                    incarnation_shutdown.clone().cancelled_owned(),
                );
                providers.task().spawn_task("paros-matchmaker-grpc-server", async move {
                    if let Err(error) = connection.await {
                        tracing::warn!(%addr, %error, "matchmaker gRPC connection ended");
                    }
                }).detach();
            }
            Some((request, reply)) = inbox.requests.recv() => {
                // One request, one batch, one reply: the core answers every
                // request it is stepped, and the drain hands the reply out
                // only once the batch is durable.
                matchmaker.step(request);
                let mut replies = drain(&mut matchmaker, &mut storage, hooks, audit)?;
                let answer = replies.pop();
                assert!(
                    answer.is_some() && replies.is_empty(),
                    "one matchmaking request yields exactly one reply"
                );
                if let Some(answer) = answer {
                    report_reply(audit, &answer);
                    // A lost reply is a legal outcome: the registration stands
                    // and the requester's retry is the same request again,
                    // answered from the retained history.
                    if hooks.drop_client_reply(Reply::Match) {
                        audit.match_reply_dropped(id);
                        tracing::info!(matchmaker = id.0, reply = "match", "match_reply_dropped");
                    } else {
                        let _ = reply.send(answer);
                    }
                }
            }
            Some(((from, watermark), reply)) = inbox.collects.recv() => {
                // The GC *primitive*, not the GC protocol: raise the floor (a
                // no-op at or below the current one), persist it, and only
                // then acknowledge with the floor in force. Nothing here
                // establishes that the dropped configurations are no longer
                // needed — see `Matchmaker::advance_gc_watermark`: the
                // caller owns the paper's §3.5 preconditions.
                let raised = matchmaker.advance_gc_watermark(watermark);
                tracing::info!(
                    matchmaker = id.0,
                    from = from.0,
                    round = watermark.round,
                    raised,
                    "garbage_collect_requested"
                );
                let replies = drain(&mut matchmaker, &mut storage, hooks, audit)?;
                assert!(replies.is_empty(), "a garbage-collect request yields no match reply");
                let _ = reply.send((id, matchmaker.hard_state().gc_watermark));
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{NodeId, QuorumSystem};

    #[test]
    fn config_hash_distinguishes_membership_and_is_order_independent() {
        let a = AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        );
        let b = AcceptorConfig::new(
            vec![NodeId(2), NodeId(1), NodeId(0)],
            QuorumSystem::Majority,
        );
        let c = AcceptorConfig::new(vec![NodeId(0), NodeId(1)], QuorumSystem::Majority);
        assert_eq!(config_hash(&a), config_hash(&b));
        assert_ne!(config_hash(&a), config_hash(&c));
    }
}
