//! Chain-of-Blocks client workload.
//!
//! One client of one journal, speaking the four calls of #204: `Write`,
//! `Read`, `Truncate` and `SetLeader`. Every client is an **owner** — it
//! claims the journal with `SetLeader`, writes at the position its claim
//! answered, and is fenced the moment another owner claims — or a
//! **reader**, which only reads (each journal's first client is always an
//! owner, so every journal is written). Every client folds the journal it reads into a
//! `ChainState` (`fold.rs`).

use std::collections::BTreeSet;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use moonpool_sim::{
    RandomProvider, SimContext, SimulationError, SimulationResult, TimeProvider, Workload,
    assert_always, assert_reachable, assert_sometimes, assert_sometimes_greater_than,
    buggify_with_prob, swarm_op_enabled,
};
use paros::client::{
    ClaimOutcome, MatchmakersRefusal, ReadOutcome, ReconfigureMatchmakersOutcome,
    ReconfigureOutcome, Resolution, Retarget, RetireOutcome, SetLeaderOutcome, TruncateOutcome,
    WriteOptions, WriteOutcome, Writer, WriterOutcome,
};
use paros::{
    Entry, JournalIdentifier, LeaderUuid, QuorumSystem, ReconfigureRefusal, RetireRequest,
    TenantId, Truncate, WireQuorumSystem, leader_uuid_to_proto, quorum_system_from_proto,
};

use crate::audit::{ClientHistory, audit_world_for, check_run};
use crate::chain::{hash_text, trace_truncate};
use crate::client::{ChainClient, ClientRuntime, client_rpc_config};

mod config;
mod fleet;
mod fold;
mod foreign;
mod multi;
mod owner;
mod races;
mod reads;
mod reconfigure;
mod rpc;
pub(crate) mod system;
mod truncate;
mod write;

use crate::{CHAOS_DURATION_MS, DigestSink};
use config::{
    ADMIT, BOOK_CAPACITY, CHECK_TAIL, CHECKPOINT, CREATE_JOURNAL, ChainConfig, DELETE_JOURNAL,
    DRAIN_NODE, DUAL_SUBMIT, DUP_WRITE, ELECTION, FLEET_INIT, MATCH_GC, MATCHMAKE, OP_COUNT, PAUSE,
    QUORUM_READ, READ, READ_INDEX, READ_STATE, RECONFIGURE, RECONFIGURE_MATCHMAKERS, REGISTER_NODE,
    RETIRE, RETIRE_NODE, SET_LEADER, TENANT, TRUNCATE, TRUNCATE_STORM, VIEW, WRITE,
    WRITE_TO_NON_LEADER, weighted_index,
};
pub(crate) use config::{MAX_BATCH_RECORDS, MAX_LARGE_COMMAND_BYTES};
use reads::{SETTLE, judge_read, tail};
use reconfigure::{
    MATCHMAKER_SHAPES, RECONFIGURE_SHAPES, REMOVING_SHAPES, ROTATE_SHAPE, compose_reconfiguration,
    ledger_answer, ledger_request, live_candidates, malformed_system,
};
use rpc::{CallLog, judged_truncate, judged_write, read_once, within};
use truncate::{absorb_truncate, fence};
use write::{Submission, WrittenCommand};

/// Fresh leader seeds for a client's library writers (#241): every session,
/// checkpointer and claim a workload starts leads under uuids of its own, so
/// a well-behaved client never reinstates a uuid that led before.
#[derive(Debug)]
pub(super) struct LeaderSeeds {
    base: u128,
    drawn: std::sync::atomic::AtomicU64,
}

impl LeaderSeeds {
    pub(super) fn new(base: u128) -> Self {
        Self {
            base,
            drawn: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The next seed, never handed out before.
    pub(super) fn next(&self) -> u128 {
        let k = self
            .drawn
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        paros::client::leader_uuid(self.base, k).0
    }
}

/// Where a client sends its next attempt after a redirect, a transport error,
/// or an ambiguous outcome (the library's [`Retarget`]). Drawn per step, so a
/// seed can be a client that always follows the hint, one that stubbornly
/// re-asks the same node (the dedup path on the node that may have
/// committed the abandoned attempt), or one that walks the ring. Two bits
/// of `draw` pick it; the hint-following default keeps half the mass so the
/// ordinary client stays the common shape.
fn retarget_from_draw(draw: u64) -> Retarget {
    match draw % 4 {
        0 | 1 => Retarget::FollowHint,
        2 => Retarget::SameNode,
        _ => Retarget::NextNode,
    }
}

/// Sticky per-run coverage facts for the adversarial operations — a *flag
/// set*, not a state machine: one independent bit per gate, each flipped once
/// at its own transition (the `crate::audit` flag-set waiver).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct AdversarialCoverage {
    duplicate_reproposed: bool,
    duplicate_across_leader_change: bool,
    dual_submitted: bool,
    compact_storm_modes: [bool; 3],
    payload_classes: [bool; 4],
    /// A `READ` step ran.
    read_executed: bool,
    /// A `SET_LEADER` step ran, and one won.
    set_leader_executed: bool,
    claim_won: bool,
    /// A superseded writer's write was refused, naming the generation that
    /// fenced it.
    fenced: bool,
    /// A superseded writer reinstated its old uuid (`reinstate_pct`).
    reinstated: bool,
    /// Race 2 (#205): a retry that crossed an ownership change was answered
    /// from the log, or refused as superseded.
    retry_acked_across_claim: bool,
    retry_superseded: bool,
    /// Race 1 (#205): a claim landed inside an owner's burst — writes ahead
    /// of it written, the rest fenced by the generation it minted.
    burst_fenced: bool,
    /// Race 3 (#205): a read raced past by a truncation was refused, and the
    /// reader resumed at the floor the refusal named.
    reader_resumed: bool,
    /// One flag per [`RECONFIGURE_SHAPES`] entry: the shape was requested and
    /// the leader started it.
    reconfigure_started: [bool; 5],
    /// A deployment without matchmakers refused a reconfiguration outright.
    reconfigure_refused_plain: bool,
    /// One flag per [`MATCHMAKER_SHAPES`] entry: the shape was requested and
    /// a node started the handover.
    reconfigure_matchmakers_started: [bool; 4],
    /// A node accepted a retirement and the world parked the identity.
    retired: bool,
    /// A node refused a retirement (it was a member again by the time the
    /// request landed).
    retire_refused: bool,
    /// A refused retirement handed the parked identity back to the world.
    retire_released: bool,
}

/// Factory-created stateful test driver. Its model is the client's own
/// history, never a second implementation of Paxos.
///
/// The history is keyed by the client's own operation number — never by the
/// payload hash: two distinct writes can legitimately carry identical bytes,
/// and hash-keying would alias their outcomes ("never use hashes as
/// identities"). The payload hash rides along as data, for the traces.
pub(crate) struct ChainWorkload {
    external_digests_compared: bool,
    adversarial: AdversarialCoverage,
    /// This client's own record of what it asked for and what came back —
    /// the linearizability history checked in `check()`. The client is the
    /// only party that knows its own program order.
    history: ClientHistory,
    /// Where to publish the audit's end-of-run digest (the determinism proof).
    digest: Option<DigestSink>,
    /// The journal this client writes and reads (#188), and the run's
    /// plan (set in `setup`).
    journal: JournalIdentifier,
    plan: Option<crate::shape::JournalPlan>,
    /// This client's id (set in `setup`): its identity as an owner.
    client_id: u64,
    /// Every attempt at this client's journal, logged at the RPC seam
    /// (set in `run`), handed to the history at `check()`.
    calls: Option<CallLog>,
}

impl ChainWorkload {
    pub(crate) fn new(digest: Option<DigestSink>) -> Self {
        Self {
            external_digests_compared: false,
            adversarial: AdversarialCoverage::default(),
            history: ClientHistory::default(),
            digest,
            journal: JournalIdentifier::UNSET,
            plan: None,
            client_id: 0,
            calls: None,
        }
    }

    fn enabled_operations() -> Vec<u8> {
        let enabled: Vec<u8> = (0..OP_COUNT).filter(|op| swarm_op_enabled(*op)).collect();
        if enabled.is_empty() {
            (0..OP_COUNT).collect()
        } else {
            enabled
        }
    }

    fn choose_operation(config: &ChainConfig, enabled: &[u8], draw: u64) -> u8 {
        let total = enabled
            .iter()
            .map(|operation| config.weight(*operation))
            .sum::<u64>();
        let mut ticket = draw % total.max(1);
        for operation in enabled {
            let weight = config.weight(*operation);
            if ticket < weight {
                return *operation;
            }
            ticket -= weight;
        }
        enabled[0]
    }
}

/// Claim `journal` (#204) through the library ([`ChainClient::claim`]):
/// read where it stands — a quorum read any node serves, moving on to the
/// next node while one goes unserved (a leader whose own reads cannot
/// confirm, a slow link to its row, must not also fence every claim sent
/// its way — witness seed 13376948288886643991, where two owners' claims
/// read at such a leader for the whole recovery tail) — then `SetLeader`
/// against the leader read, asked of `nodes[target]`, under `uuid`.
///
/// A claim against its own uuid would supersede this client's own
/// leadership: with a request timeout under the claim's answer, every claim
/// won and every answer was lost, and the client re-claimed forever, one
/// term a claim (witness seed 3544251723324122292, #205: generations 1–58
/// all its own, no write in 60 s) — the library adopts a read naming `uuid`
/// as `Owned`. A writer that wants a new term asks under a new uuid
/// ([`Writer::claim`] with `fresh`, the races of #205).
async fn claim(
    nodes: &ChainClient,
    journal: JournalIdentifier,
    target: usize,
    uuid: LeaderUuid,
) -> ClaimOutcome {
    let outcome = nodes.claim(journal, uuid, target).await;
    assert_always!(
        outcome != ClaimOutcome::Malformed,
        "chain: a node answers a well-formed journal state"
    );
    assert_always!(
        outcome != ClaimOutcome::UnknownJournal,
        "chain: a node serves the journal the client names",
        {
            "node" => nodes.id_of(target),
            "tenant" => journal.tenant.0,
            "journal" => journal.journal.0
        }
    );
    outcome
}

#[async_trait]
impl Workload for ChainWorkload {
    fn name(&self) -> &'static str {
        "chain-client"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn setup(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        tail(ctx.state())
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .registered += 1;
        // The run's journals (#188): drawn once per seed by whoever asks
        // first, the same for every node and client; clients are spread over
        // them round-robin.
        let has_matchmakers = !crate::roles::deployment(ctx.topology())
            .matchmakers()
            .is_empty();
        let plan = crate::shape::journals(ctx.state());
        if has_matchmakers && plan.ids.len() > 1 {
            // #201: several journals beside the matchmaker plane, the
            // composition PR #199 had withheld (a cause; the outcomes are
            // the journal board's gates).
            assert_reachable!("journal: a matchmaker seed runs more than one journal");
        }
        self.client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        self.journal = plan.for_client(ctx.client_id());
        self.plan = Some(plan);
        // Every client folds its journal from the start, so the trim fence
        // holds every trim back until this client has folded past it.
        fold::register(ctx.state(), self.journal, self.client_id);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The seed's deployment map: the acceptor pool this client proposes
        // to.
        let deployment = crate::roles::deployment(ctx.topology());
        let servers = deployment.acceptors().to_vec();
        if servers.is_empty() {
            return Err(SimulationError::InvalidState(
                "chain workload has no server".into(),
            ));
        }

        let config = ChainConfig::for_timeline();
        // Membership as protocol data (#122): whether this seed deploys
        // matchmakers (the opt-in for reconfiguration), and the floor no
        // configuration this client asks for goes below. On a plain seed
        // every request is refused unread, so any set at all may be asked
        // for — the point there is the refusal.
        let has_matchmakers = !deployment.matchmakers().is_empty();
        let config_floor = if has_matchmakers {
            crate::shape::config_floor(servers.len(), true)
        } else {
            1
        };
        // The run's quorum-system policy (#140): what every successor this
        // client composes runs under, at the successor's own size. Drawn by
        // whoever asked first — a node or this client — and the same for
        // both.
        let policy = crate::shape::quorum_policy(ctx.state(), servers.len());
        // The matchmaker pool's address book and the floor no matchmaker set
        // this client asks for goes below (#125): the bootstrap set's size,
        // capped at three — the smallest set that keeps a quorum after the
        // one registry loss the world permits.
        let matchmaker_ips = deployment.matchmakers().to_vec();
        let matchmaker_floor = if has_matchmakers {
            crate::shape::matchmaker_floor(
                crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_ips.len()).len(),
            )
        } else {
            1
        };
        // The run's own client-only RPC runtime, stopped when `runtime`
        // drops on any exit path.
        let runtime = ClientRuntime::start(
            ctx,
            client_rpc_config(
                Duration::from_millis(config.connect_timeout_ms),
                Duration::from_millis(config.keep_alive_interval_ms),
                Duration::from_millis(config.keep_alive_timeout_ms),
            ),
        )?;
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let journal = self.journal;
        // Every attempt at this client's journal, logged at the RPC seam:
        // the library client reports each one to it (#221).
        let log = CallLog::new(journal, client_id, ctx.time().clone());
        self.calls = Some(log.clone());
        let observer: Arc<dyn paros::client::CallObserver> = Arc::new(log.clone());
        let server_count = servers.len();
        // The library client this workload drives every journal call
        // through (#221): the genesis pool at its rank, then the joiners
        // (#189) — a reconfiguration may make one a member and then the
        // leader, and a client that cannot reach its leader cannot append at
        // all. Every draw the client makes stays over the genesis pool (the
        // rotation); only a leader a reply names routes to a joiner.
        let node_ids: Vec<(u64, String)> = servers
            .iter()
            .enumerate()
            .map(|(rank, ip)| (rank as u64, ip.clone()))
            .chain(
                deployment
                    .joiners()
                    .iter()
                    .enumerate()
                    .map(|(rank, ip)| (crate::roles::joiner_node_id(rank).0, ip.clone())),
            )
            .collect();
        let nodes = runtime
            .chain_client(ctx, &node_ids, config.tunables())?
            .with_observer(observer.clone())
            .rotating_over(server_count);
        // The replica tier (#144): never proposed to, only probed — a replica
        // applies the same log, so the settle tail waits for it and the
        // live-read comparison judges it beside every acceptor. Empty on a
        // seed without replicas.
        // The replica tier serves the default journal alone (#188).
        let main = crate::shape::identifiers(ctx.state()).main;
        let replica_ips: Vec<(u64, String)> = if self.journal == main {
            deployment
                .replicas()
                .iter()
                .enumerate()
                .map(|(rank, ip)| {
                    let id = crate::roles::replica_node_id(paros::ReplicaId(rank as u64));
                    (id.0, ip.clone())
                })
                .collect()
        } else {
            Vec::new()
        };
        let replica_count = replica_ips.len();
        // Every process that serves a journal `Read`: the nodes, then the
        // replicas (a fold's rotation, `Fold::read_to_tail`).
        let readers = runtime
            .chain_client(
                ctx,
                &node_ids[..server_count]
                    .iter()
                    .cloned()
                    .chain(replica_ips)
                    .collect::<Vec<_>>(),
                config.tunables(),
            )?
            .with_observer(observer);

        let operations = Self::enabled_operations();
        tracing::info!(?config, "chain_config");
        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        self.history.set_client(client_id);
        let audit = audit_world_for(ctx.state(), journal);
        let now_ms = {
            let time = time.clone();
            move || u64::try_from(time.now().as_millis()).unwrap_or(u64::MAX)
        };
        let request_timeout = Duration::from_millis(config.request_timeout_ms);
        let read_timeout = Duration::from_millis(config.read_timeout_ms);
        let mut next_op = 0_u64;
        // A joiner this client just registered: the next step grows a
        // configuration onto it (the `REGISTER_NODE` arm).
        let mut reconfigure_next = false;
        // The wiped-founder scenario (#246): client 0 runs `init` first, so
        // the cell decree runs inside the chaos window.
        let mut init_first = self.client_id == 0 && crate::shape::wiped_founder(ctx.state());
        let mut successful_after_ambiguity = false;
        // The library's decisions as outcomes (#221): a write redirected
        // and written at the leader, an ambiguous write the session settled,
        // a superseded writer that stopped.
        let mut redirected_written = false;
        let mut ambiguity_resolved = false;
        let mut superseded_stopped = false;
        // An unanswered write at the hinted leader dropped the hint, and
        // the write after it reached a leader.
        let mut hint_dropped = false;
        let mut dropped_hint_written = false;
        let mut written = Vec::<WrittenCommand>::new();
        // The highest tail a read of this client was served (`None` before
        // any): this client runs one operation at a time, so a later read
        // starts after an earlier one completed, and linearizability demands
        // its tail never move backwards.
        let mut last_read_tail: Option<u64> = None;
        // This client's fold of the journal (#186): the application this
        // client is, and its tailing cursor — a position (#204) — where its
        // tailing reads start, only ever moved forward.
        let mut fold = fold::Fold::new(journal);
        // Owner or reader (#204): a journal's first client always writes, so
        // every journal has a writer; any other client may be a reader for
        // the whole run. Clients are dealt to journals round-robin, so the
        // first `ids.len()` ids are each journal's first — a reader-only
        // journal would have nothing for its tail to converge on.
        let journal_count = self.plan.as_ref().map_or(1, |plan| plan.ids.len().max(1));
        let reader = config.reader && usize::try_from(client_id).unwrap_or(0) >= journal_count;
        // A multi-writer journal (#241) has no owner: every client of it
        // that writes appends, with no claim and no fence (`multi`).
        let multi_writer = audit.mode() == paros::WriterMode::Multi;
        // Its leader uuids derive from a random seed of its own (#241).
        let mut writer = Writer::new(journal, ctx.random().random::<u128>());
        // The system-journal operations (#189), and whether the run runs
        // the system journals at all (a seed that does not must refuse them).
        // The fleet operations (#229): the fleet tenant and the cell's tenant list.
        let mut fleet_ops = fleet::FleetOps::new(
            ctx,
            &deployment,
            runtime.connector(ctx, config.tunables()),
            Duration::from_millis(config.init_patience_ms),
            client_id,
            LeaderSeeds::new(ctx.random().random::<u128>()),
            (config.fleet_kill_delay_ms, config.fleet_kill_down_ms),
        )?;
        let mut system_ops = system::SystemOps::new(
            &deployment,
            crate::shape::identifiers(ctx.state()),
            crate::shape::system_journals(ctx.state()),
            &crate::shape::joiner_machines(ctx.state(), deployment.joiners().len()),
            (client_id, LeaderSeeds::new(ctx.random().random::<u128>())),
            request_timeout,
        );

        // The one-attempt write the races and the misbehaviours make
        // (`rpc`), bound to this client's journal.
        let write_once = |target: usize, entry: &Entry, abandon: bool| {
            rpc::write_once(&nodes, journal, target, entry, abandon, false)
        };
        // One truncation request as the trace tells it: the `Truncate` it
        // asks for under the writer `fence` it carries (#228; `None` sends
        // nothing — a writer that owns no generation), clamped below every
        // folding client's cursor (the fold fence, `fold`), then whether the
        // leader applied it.
        let truncator = nodes.with_tunables(config.truncate_tunables());
        let truncate_once = |target: usize, request: Truncate| {
            let truncator = truncator.clone();
            async move { judged_truncate(truncator.truncate(&request, target).await) }
        };
        let truncate_traced = |target: usize, fence: Option<LeaderUuid>, up_to: u64| {
            let attempt = fence.and_then(|leader| {
                fold::clamp(ctx.state(), journal, up_to).map(|up_to| {
                    trace_truncate(leader, up_to);
                    let request = Truncate {
                        journal: journal.journal.0,
                        tenant: journal.tenant.0,
                        up_to,
                        leader: Some(leader_uuid_to_proto(leader)),
                    };
                    (up_to, truncate_once(target, request))
                })
            });
            async move {
                let (up_to, attempt) = attempt?;
                let outcome = attempt.await;
                if matches!(outcome, TruncateOutcome::Applied { .. }) {
                    tracing::info!(up_to, "chain_compact_accepted");
                }
                Some(outcome)
            }
        };
        let reconfigurer = nodes.with_tunables(config.reconfigure_tunables());
        let matchmaker_reconfigurer = nodes.with_tunables(config.matchmakers_tunables());

        // An owner claims the journal first (#204): read where it stands and
        // `SetLeader` against it. Losing is a valid start — another owner
        // won, and this client's writes are fenced until it claims again.
        // A claim the cluster leaves unresolved is re-asked within the
        // owner's patience (`claim_patience_ms`), the next server along.
        let mut remove_next = false;
        if !reader && !multi_writer {
            let mut first =
                usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
            let patience = time.now() + Duration::from_millis(config.claim_patience_ms);
            loop {
                let outcome = claim(&nodes, journal, first, writer.uuid()).await;
                writer.claimed(&outcome);
                match outcome {
                    ClaimOutcome::Won { .. } => {
                        // An owner who reconfigures first (see
                        // `reconfigure_after_claim`): the main journal's
                        // alone, on a deployment that honors one.
                        remove_next = (config.reconfigure_after_claim
                            || crate::shape::departed_straggler(ctx.state()))
                            && has_matchmakers
                            && journal == main;
                        break;
                    }
                    ClaimOutcome::WrongMode { .. } => {
                        owner_never_of_wrong_mode();
                        break;
                    }
                    ClaimOutcome::Lost { .. }
                    | ClaimOutcome::Owned { .. }
                    | ClaimOutcome::UnknownJournal
                    | ClaimOutcome::Malformed => break,
                    ClaimOutcome::Redirect { leader } => {
                        first = leader
                            .and_then(|id| nodes.index_of(id))
                            .unwrap_or((first + 1) % server_count);
                    }
                    ClaimOutcome::Unread | ClaimOutcome::Ambiguous => {
                        first = (first + 1) % server_count;
                    }
                }
                if time.now() >= patience
                    || shutdown.is_cancelled()
                    || time
                        .sleep(Duration::from_millis(config.retry_backoff_ms))
                        .await
                        .is_err()
                {
                    break;
                }
                assert_reachable!("chain: an owner re-asks its opening claim within its patience");
            }
        }

        // Start with a small concurrent batch when writes are enabled. This
        // is honest client pipelining (#204: `Write` is pipelineable): it
        // lets Phase-2 rounds overlap a driver beat, making the optional
        // re-send decision and a later election gap observable without
        // fabricating or filtering protocol messages. The batches take
        // consecutive positions; one that reaches the leader out of order is
        // refused and names the position the journal stood at.
        if operations.contains(&WRITE) && !reader && !multi_writer {
            let mut primer = Vec::with_capacity(config.pipeline_depth);
            let mut ahead = writer;
            let mut raw = 0;
            for _ in 0..config.pipeline_depth {
                // One draw per primer entry shapes its payload class, its
                // bytes, and its first target — every combination is a valid
                // client.
                raw = ctx.random().random::<u64>();
                let primer_target =
                    usize::try_from((raw >> 2) % u64::try_from(server_count).unwrap_or(1))
                        .unwrap_or(0);
                let submission =
                    self.submit(&audit, &config, ahead, &mut next_op, raw, raw, now_ms());
                ahead.advance_to(ahead.next_seq() + submission.entry.count());
                let target = nodes.leader().unwrap_or(primer_target);
                primer.push((submission, target));
            }
            // Race 1 (#205): the owner's own claim, in the middle of the
            // burst.
            let race = buggify_with_prob!(0.25).then(|| {
                let delay = raw % (config.burst_claim_delay_ms + 1);
                (Duration::from_millis(delay), nodes.leader().unwrap_or(0))
            });
            self.burst(
                ctx,
                &nodes,
                &config,
                primer,
                race,
                (&mut writer, &mut written),
            )
            .await;
            if config.compaction && writer.next_seq() > 0 {
                let fallback =
                    usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
                let outcome = truncate_traced(
                    nodes.leader().unwrap_or(fallback),
                    fence(&writer),
                    writer.next_seq(),
                )
                .await;
                absorb_truncate(&mut writer, outcome.as_ref());
            }
        }

        // Static stability (#247): on its own location, client 0 of a
        // system-journal run holds the seed — the one node hosting the
        // registry — down for
        // `parent_hold_ms` of the chaos window, while every tenant journal
        // keeps committing without it (the journal board's gate).
        let parent_seed = (client_id == 0 && crate::shape::system_journals(ctx.state()))
            .then(|| servers[0].clone());
        let mut parent_until: Option<Duration> = None;
        let mut parent_held_once = false;
        let journal_board = crate::audit::journals::journal_board(ctx.state());
        for _step in 0..config.steps {
            if shutdown.is_cancelled() {
                break;
            }
            if let Some(ip) = &parent_seed {
                if parent_until.is_some_and(|until| time.now() >= until) {
                    crate::lifecycle::restart(ctx, ip).await;
                    crate::audit::journals::lock(&journal_board).release_parent();
                    parent_until = None;
                } else if parent_until.is_none()
                    && !parent_held_once
                    && time.now() < Duration::from_millis(CHAOS_DURATION_MS)
                    && buggify_with_prob!(0.1)
                {
                    assert_reachable!("static: the seed hosting the control journals is held down");
                    crate::lifecycle::crash(ctx, ip).await;
                    crate::audit::journals::lock(&journal_board).hold_parent(0);
                    parent_until = Some(time.now() + Duration::from_millis(config.parent_hold_ms));
                    parent_held_once = true;
                }
            }

            // Exactly six provider draws per logical step, independent of the
            // swarm mask and payload length. `raw_policy` shapes this step's
            // client policies: which node it asks first, how it retargets
            // after a redirect, where a duplicate goes, how far it compacts.
            let raw_op = ctx.random().random::<u64>();
            let raw_target = ctx.random().random::<u64>();
            let raw_class = ctx.random().random::<u64>();
            let raw_payload = ctx.random().random::<u64>();
            let raw_pause = ctx.random().random::<u64>();
            let raw_policy = ctx.random().random::<u64>();
            let after_register = std::mem::take(&mut reconfigure_next);
            // The owner's removal stays its next operation until a request
            // leaves (an inspect the chaos swallows, a shape nothing admits),
            // and for the chaos window only: the tail runs the drawn mix.
            // On a departed-straggler seed it stays armed into the late
            // outage's window (`crate::world::late_outage`): the owner's
            // claim usually lands after the chaos window.
            let deadline = if crate::shape::departed_straggler(ctx.state()) {
                crate::world::late_outage::late_deadline()
            } else {
                Duration::from_millis(CHAOS_DURATION_MS)
            };
            if time.now() >= deadline {
                remove_next = false;
            }
            let after_claim = remove_next;
            let op = if after_register {
                assert_reachable!("system: a client reconfigures right after registering a joiner");
                RECONFIGURE
            } else if after_claim {
                RECONFIGURE
            } else if std::mem::take(&mut init_first) {
                assert_reachable!("init: an operator runs init first on a wiped-founder seed");
                FLEET_INIT
            } else {
                Self::choose_operation(&config, &operations, raw_op)
            };
            // The matchmaker plane — an acceptor or matchmaker
            // reconfiguration, a retirement — belongs to the default journal
            // (#188): a client of another journal pauses instead.
            let op = if journal != main
                && matches!(op, RECONFIGURE | RECONFIGURE_MATCHMAKERS | RETIRE)
            {
                PAUSE
            } else if reader
                && matches!(
                    op,
                    WRITE
                        | WRITE_TO_NON_LEADER
                        | DUP_WRITE
                        | DUAL_SUBMIT
                        | SET_LEADER
                        | TRUNCATE
                        | TRUNCATE_STORM
                )
            {
                // A reader (#204) never writes, claims or truncates: it
                // reads instead.
                READ
            } else {
                op
            };
            let target =
                usize::try_from(raw_target % u64::try_from(server_count).unwrap_or(1)).unwrap_or(0);
            let retarget = retarget_from_draw(raw_policy);
            // One step in eight ignores the leader hint outright: a write to
            // whoever `target` is, which after a turnover is the *old*
            // leader — the stale-hint edge `WRITE_TO_NON_LEADER` reaches only
            // deliberately.
            let ignore_hint = (raw_policy >> 2) % 8 == 0;

            // The cross-tenant attack (#247), its own location: a write, a
            // truncation or a claim sent under an identifier that is not this
            // journal's — another tenant's journal, or an identifier nobody serves
            // — must be refused, and never reach the other journal.
            if matches!(op, WRITE | TRUNCATE | SET_LEADER) && buggify_with_prob!(0.1) {
                let journals = self
                    .plan
                    .as_ref()
                    .map(|plan| plan.ids.clone())
                    .unwrap_or_default();
                foreign::attack(
                    ctx,
                    &nodes,
                    (op, &writer),
                    (journal, &journals),
                    (target, raw_payload),
                    request_timeout,
                )
                .await;
            }

            // A multi-writer journal (#241): the write family appends,
            // truncates open and claims a journal that must refuse it.
            if multi_writer
                && matches!(
                    op,
                    WRITE
                        | WRITE_TO_NON_LEADER
                        | DUP_WRITE
                        | DUAL_SUBMIT
                        | SET_LEADER
                        | TRUNCATE
                        | TRUNCATE_STORM
                )
            {
                let trim_to = if matches!(op, TRUNCATE | TRUNCATE_STORM)
                    && config.compaction
                    && raw_pause % config.compact_every == 0
                {
                    fold.read_to_tail(ctx, &audit, &readers, target, client_id, config.read_limit)
                        .await;
                    fold::clamp(ctx.state(), journal, fold.cursor())
                } else {
                    None
                };
                let step = multi::Step {
                    ctx,
                    nodes: &nodes,
                    log: &log,
                    audit: &audit,
                    config: &config,
                    journal,
                    target,
                    server_count,
                    retarget,
                    draws: (raw_class, raw_payload),
                    trim_to,
                };
                self.multi_step(&step, op, &mut next_op, &mut written).await;
                continue;
            }
            // The mode confusion on a single-writer journal (#241), its own
            // location: an unfenced write, refused as of the wrong mode.
            if op == WRITE && buggify_with_prob!(0.03) {
                assert_reachable!("chain: an unfenced write meets a single-writer journal");
                multi::unfenced_write_refused(
                    ctx,
                    (&nodes, &audit),
                    journal,
                    target,
                    request_timeout,
                )
                .await;
            }
            // The same confusion for a truncation (#339), its own location:
            // an unfenced `Truncate`, refused as of the wrong mode.
            if op == TRUNCATE && buggify_with_prob!(0.15) {
                assert_reachable!("chain: an unfenced truncate meets a single-writer journal");
                let up_to = fold::clamp(ctx.state(), journal, writer.next_seq()).unwrap_or(0);
                multi::unfenced_truncate_refused(
                    ctx,
                    &nodes,
                    journal,
                    (target, up_to),
                    request_timeout,
                )
                .await;
            }
            // An owner's own misbehaviours (#339), each its own location: a
            // write ahead of the journal and a claim of the term it leads.
            if op == WRITE && writer.owned().is_some() && buggify_with_prob!(0.2) {
                assert_reachable!("chain: an owner writes ahead of the journal");
                owner::ahead_write_refused(
                    ctx,
                    (&nodes, &audit),
                    (journal, &mut writer),
                    (target, raw_payload),
                    request_timeout,
                )
                .await;
            }
            if op == SET_LEADER && writer.owned().is_some() && buggify_with_prob!(0.25) {
                assert_reachable!("chain: an owner claims the term it leads");
                owner::own_term_refused(
                    ctx,
                    &nodes,
                    (journal, &mut writer),
                    target,
                    request_timeout,
                )
                .await;
            }

            match op {
                WRITE | WRITE_TO_NON_LEADER => {
                    // Race 1 (#205), mid-run: an owner pipelines a burst at
                    // consecutive positions, and on a second coin claims the
                    // journal again while it is in flight. Its entries are
                    // spread off the step's draws, so the step still draws
                    // six times.
                    if op == WRITE && writer.owned().is_some() && buggify_with_prob!(0.10) {
                        assert_reachable!("chain: an owner pipelines a burst of writes mid-run");
                        let via = nodes.leader().unwrap_or(target);
                        let mut ahead = writer;
                        let mut burst = Vec::with_capacity(config.pipeline_depth);
                        for k in 0..config.pipeline_depth as u64 {
                            let spread = crate::chain::splitmix(raw_payload ^ k);
                            let submission = self.submit(
                                &audit,
                                &config,
                                ahead,
                                &mut next_op,
                                raw_class.wrapping_add(k),
                                spread,
                                now_ms(),
                            );
                            ahead.advance_to(ahead.next_seq() + submission.entry.count());
                            burst.push((submission, via));
                        }
                        let race = buggify_with_prob!(0.5).then(|| {
                            assert_reachable!("chain: a claim races a mid-run burst");
                            let delay = raw_pause % (config.burst_claim_delay_ms + 1);
                            (Duration::from_millis(delay), via)
                        });
                        self.burst(
                            ctx,
                            &nodes,
                            &config,
                            burst,
                            race,
                            (&mut writer, &mut written),
                        )
                        .await;
                        continue;
                    }
                    let submission = self.submit(
                        &audit,
                        &config,
                        writer,
                        &mut next_op,
                        raw_class,
                        raw_payload,
                        now_ms(),
                    );
                    let chosen_target = if op == WRITE_TO_NON_LEADER {
                        nodes.leader().map_or(target, |leader| {
                            if server_count > 1 {
                                (leader + 1 + target % (server_count - 1)) % server_count
                            } else {
                                leader
                            }
                        })
                    } else if ignore_hint {
                        target
                    } else {
                        nodes.leader().unwrap_or(target)
                    };
                    // Honest ambiguity: abandon the client observation, never
                    // falsify a server acknowledgement. The identical write
                    // is retried below.
                    // Race 2 (#205): this attempt's timeout is shorter than
                    // its ack (`ack_race_timeout_ms`), so the owner gives up
                    // on a write that may still land.
                    let ack_race =
                        op == WRITE && writer.owned().is_some() && buggify_with_prob!(0.25);
                    #[allow(clippy::cast_precision_loss)]
                    let abandon = !ack_race
                        && time.now() < Duration::from_millis(CHAOS_DURATION_MS)
                        && buggify_with_prob!(config.abandon_pct as f64 / 100.0);
                    if abandon {
                        // BUGGIFY pairing: the deliberate mid-flight
                        // abandonment (the honest-ambiguity generator) fires.
                        assert_reachable!("chain: a client abandons an in-flight observation");
                    }
                    let result = if ack_race {
                        self.ack_race(
                            ctx,
                            &nodes,
                            &config,
                            &submission,
                            chosen_target,
                            &mut writer,
                        )
                        .await
                    } else {
                        // The library's write (#221): the identical write,
                        // following redirects (a `WRITE_TO_NON_LEADER` stops
                        // at the first) inside one request deadline.
                        let request = writer.request(&submission.entry);
                        log.open_write(submission.op);
                        let report = nodes
                            .write(
                                &request,
                                chosen_target,
                                WriteOptions {
                                    retarget,
                                    stop_at_redirect: op != WRITE,
                                    abandon_first_after: abandon
                                        .then_some(Duration::from_millis(10)),
                                },
                            )
                            .await;
                        let result = judged_write(
                            report.outcome,
                            false,
                            nodes.id_of(report.server),
                            writer.journal(),
                        );
                        if report.redirects > 0 && matches!(result, WriteOutcome::Written { .. }) {
                            redirected_written = true;
                        }
                        let result = if matches!(result, WriteOutcome::Ambiguous) {
                            tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_proposal_ambiguous");
                            // Settle it (#204: the journal answers the
                            // identical write from the log): read the
                            // position back, then re-send it byte for byte —
                            // by policy, back to the node that may have
                            // committed the abandoned attempt, or on to the
                            // hinted leader / the next node.
                            let retry_target = nodes.retarget(
                                retarget,
                                chosen_target,
                                nodes.leader().map(|leader| nodes.id_of(leader)),
                            );
                            // An impatient operator (#204's retry edge,
                            // its own BUGGIFY location): the identical
                            // write re-sent at once, before any read-back,
                            // which a journal that committed the first must
                            // answer from the log. `resolve` reads back
                            // first, so without this the retry that meets a
                            // committed write is all but never sent.
                            let resent = if crate::shape::lost_verdict(ctx.state())
                                || buggify_with_prob!(0.5)
                            {
                                assert_reachable!(
                                    "client: an ambiguous write is re-sent before any read-back"
                                );
                                let again = nodes
                                    .write_attempt(retry_target, request.clone(), None)
                                    .await;
                                match judged_write(
                                    again,
                                    false,
                                    nodes.id_of(retry_target),
                                    writer.journal(),
                                ) {
                                    WriteOutcome::Written { seq, count, .. } => {
                                        Some(Resolution::Written { seq, count })
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            };
                            let resolved = match resent {
                                Some(resolution) => paros::client::ResolveReport {
                                    resolution,
                                    by_read_back: false,
                                },
                                None => nodes.resolve(&request, retry_target, retarget).await,
                            };
                            if resolved.by_read_back {
                                assert_reachable!(
                                    "client: a read-back alone proves an ambiguous write fenced"
                                );
                            }
                            match resolved.resolution {
                                Resolution::Written { seq, count } => {
                                    successful_after_ambiguity = true;
                                    ambiguity_resolved = true;
                                    WriteOutcome::Written {
                                        seq,
                                        count,
                                        duplicate: true,
                                    }
                                }
                                Resolution::NotWritten { state } => {
                                    ambiguity_resolved = true;
                                    WriteOutcome::Refused { state }
                                }
                                Resolution::Truncated { state } => {
                                    WriteOutcome::Truncated { state }
                                }
                                Resolution::Unresolved => WriteOutcome::Ambiguous,
                            }
                        } else {
                            result
                        };
                        log.close_write();
                        result
                    };
                    match result {
                        WriteOutcome::Written { seq, count, .. } => {
                            writer.advance_to(seq + count);
                            self.record_written(&submission, seq, count, now_ms());
                            self.adversarial.payload_classes[submission.payload_class] = true;
                            written.push(submission.written(
                                seq,
                                count,
                                nodes.leader().unwrap_or(chosen_target),
                            ));
                            if config.compaction
                                && submission.op.is_multiple_of(config.compact_every)
                            {
                                // How far to ask: everything written, a
                                // partial prefix below it, or past it (a
                                // truncation is clamped to `next_seq`).
                                let end = seq + count;
                                let up_to = match (raw_policy >> 5) % 4 {
                                    0 => end.saturating_sub((raw_policy >> 7) % (end + 1)),
                                    1 => end + 1 + (raw_policy >> 7) % 8,
                                    _ => end,
                                };
                                let outcome = truncate_traced(
                                    nodes.leader().unwrap_or(chosen_target),
                                    fence(&writer),
                                    up_to,
                                )
                                .await;
                                absorb_truncate(&mut writer, outcome.as_ref());
                            }
                        }
                        WriteOutcome::Refused { state } | WriteOutcome::Truncated { state } => {
                            self.history.record_write_failed(submission.op);
                            if writer.owned().is_none()
                                && state
                                    .leader
                                    .is_some_and(|leader| leader != submission.entry.leader)
                            {
                                self.adversarial.fenced = true;
                            }
                            writer.learn(&state);
                            tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_command_rejected");
                        }
                        WriteOutcome::Redirect { leader } => {
                            nodes.observe_leader(leader);
                            self.history.record_write_failed(submission.op);
                        }
                        WriteOutcome::TooLarge { .. } => {
                            assert_reachable!(
                                "chain: a batch over a node's limits is refused at the edge"
                            );
                            self.history.record_write_failed(submission.op);
                        }
                        WriteOutcome::WrongMode { .. } => {
                            owner_never_of_wrong_mode();
                            self.history.record_write_failed(submission.op);
                        }
                        WriteOutcome::UnknownJournal
                        | WriteOutcome::Malformed
                        | WriteOutcome::Ambiguous => {
                            self.history.record_write_failed(submission.op);
                        }
                    }
                }
                SET_LEADER => {
                    if !self.adversarial.set_leader_executed {
                        assert_reachable!("chain: set-leader operation executes");
                        self.adversarial.set_leader_executed = true;
                    }
                    let via = nodes.leader().unwrap_or(target);
                    // The deliberate misbehaviour (§2.3, decided on
                    // 2026-10-09): a superseded writer reinstates the uuid
                    // it last led with, which the journal does not refuse —
                    // it trusts its clients to draw fresh ones. Its stale
                    // writes then land; the journal's guarantees hold all
                    // the same.
                    let former = writer.fence();
                    if writer.owned().is_none()
                        && former != writer.uuid()
                        && raw_policy % 100 < config.reinstate_pct
                    {
                        assert_reachable!("chain: a superseded writer reinstates its old uuid");
                        if let Some(state) = nodes.journal_state(journal, via).await {
                            let outcome =
                                nodes.set_leader(journal, former, state.leader, via).await;
                            if matches!(outcome, SetLeaderOutcome::Won { .. }) {
                                self.adversarial.reinstated = true;
                            }
                        }
                        continue;
                    }
                    let outcome = claim(&nodes, journal, via, writer.uuid()).await;
                    writer.claimed(&outcome);
                    match outcome {
                        ClaimOutcome::Won { state } => {
                            self.adversarial.claim_won = true;
                            tracing::info!(
                                leader = %writer.uuid(),
                                next_seq = state.next_seq.0,
                                "chain_claim_won"
                            );
                        }
                        ClaimOutcome::Lost { state } | ClaimOutcome::Owned { state } => {
                            tracing::info!(next_seq = state.next_seq.0, "chain_claim_lost");
                        }
                        _ => {}
                    }
                }
                DUP_WRITE => {
                    if let Some(current_leader) = nodes.leader() {
                        let candidates = written
                            .iter()
                            .filter(|command| command.node != current_leader)
                            .collect::<Vec<_>>();
                        if candidates.is_empty() {
                            continue;
                        }
                        let index = usize::try_from(
                            raw_payload % u64::try_from(candidates.len()).unwrap_or(1),
                        )
                        .unwrap_or(0);
                        let command = (*candidates[index]).clone();
                        // Where the retry goes: the current leader, the node
                        // that originally answered it (a possibly demoted
                        // node), or anyone.
                        let duplicate_target = match (raw_policy >> 3) % 4 {
                            0 | 1 => current_leader,
                            2 => command.node % server_count,
                            _ => target,
                        };
                        tracing::info!(
                            cmd = %hash_text(command.cmd_hash),
                            seq = command.seq,
                            original_node = command.node,
                            target = duplicate_target,
                            "chain_duplicate_reproposed"
                        );
                        if !self.adversarial.duplicate_reproposed {
                            assert_reachable!("chain: duplicate reproposal executes");
                            self.adversarial.duplicate_reproposed = true;
                        }
                        let result = within(
                            ctx,
                            request_timeout,
                            WriteOutcome::Ambiguous,
                            write_once(duplicate_target, &command.entry, false),
                        )
                        .await;
                        match result {
                            WriteOutcome::Written { seq, duplicate, .. } => {
                                // A write already in the journal is answered
                                // from the log, at the position it holds.
                                assert_always!(
                                    duplicate && seq == command.seq,
                                    "chain: duplicate committed ack preserves its slot",
                                    {
                                        "original_seq" => command.seq,
                                        "observed_seq" => seq,
                                        "target" => duplicate_target,
                                    }
                                );
                                if !self.adversarial.duplicate_across_leader_change {
                                    assert_reachable!(
                                        "chain: duplicate suppression observed after leader change"
                                    );
                                    self.adversarial.duplicate_across_leader_change = true;
                                }
                            }
                            WriteOutcome::Refused { state } => {
                                assert_always!(
                                    false,
                                    "chain: a retried write is never refused",
                                    { "seq" => command.seq, "next_seq" => state.next_seq.0 }
                                );
                            }
                            WriteOutcome::Redirect { leader } => nodes.observe_leader(leader),
                            // A node with smaller limits than the first
                            // attempt's refuses the identical retry.
                            WriteOutcome::WrongMode { .. } => owner_never_of_wrong_mode(),
                            WriteOutcome::TooLarge { .. }
                            | WriteOutcome::Truncated { .. }
                            | WriteOutcome::UnknownJournal
                            | WriteOutcome::Malformed
                            | WriteOutcome::Ambiguous => {}
                        }
                    }
                }
                DUAL_SUBMIT => {
                    if server_count > 1 && time.now() < Duration::from_millis(CHAOS_DURATION_MS) {
                        let submission = self.submit(
                            &audit,
                            &config,
                            writer,
                            &mut next_op,
                            raw_class,
                            raw_payload,
                            now_ms(),
                        );
                        let second_target = (target
                            + 1
                            + usize::try_from(
                                raw_pause % u64::try_from(server_count - 1).unwrap_or(1),
                            )
                            .unwrap_or(0))
                            % server_count;
                        tracing::info!(
                            cmd = %hash_text(submission.cmd_hash),
                            first = target,
                            second = second_target,
                            "chain_dual_submitted"
                        );
                        if !self.adversarial.dual_submitted {
                            assert_reachable!("chain: dual-submit operation executes");
                            self.adversarial.dual_submitted = true;
                        }
                        let targets = [target, second_target];
                        let attempts = targets.iter().map(|target| {
                            within(
                                ctx,
                                request_timeout,
                                WriteOutcome::Ambiguous,
                                write_once(*target, &submission.entry, false),
                            )
                        });
                        let results = join_all(attempts).await;
                        let mut committed: Option<(u64, u64, usize)> = None;
                        let mut refused: Option<paros::JournalView> = None;
                        for (attempt_target, result) in targets.into_iter().zip(results) {
                            match result {
                                WriteOutcome::Written { seq, count, .. } => {
                                    if let Some((original, _, _)) = committed {
                                        assert_always!(
                                            seq == original,
                                            "chain: dual-submit committed slots agree",
                                            {
                                                "original_seq" => original,
                                                "observed_seq" => seq,
                                                "target" => attempt_target,
                                            }
                                        );
                                    } else {
                                        committed = Some((seq, count, attempt_target));
                                    }
                                }
                                WriteOutcome::Refused { state }
                                | WriteOutcome::Truncated { state } => {
                                    refused = Some(state);
                                }
                                WriteOutcome::Redirect { leader } => nodes.observe_leader(leader),
                                WriteOutcome::WrongMode { .. } => owner_never_of_wrong_mode(),
                                WriteOutcome::TooLarge { .. }
                                | WriteOutcome::UnknownJournal
                                | WriteOutcome::Malformed
                                | WriteOutcome::Ambiguous => {}
                            }
                        }
                        if let Some((seq, count, ack_target)) = committed {
                            writer.advance_to(seq + count);
                            self.record_written(&submission, seq, count, now_ms());
                            self.adversarial.payload_classes[submission.payload_class] = true;
                            written.push(submission.written(seq, count, ack_target));
                        } else {
                            self.history.record_write_failed(submission.op);
                            if let Some(state) = refused {
                                writer.learn(&state);
                            }
                        }
                    }
                }
                TRUNCATE => {
                    if config.compaction && raw_pause % config.compact_every == 0 {
                        // Fold first: the fence holds a truncation below this
                        // client's own cursor too.
                        fold.read_to_tail(
                            ctx,
                            &audit,
                            &readers,
                            target,
                            client_id,
                            config.read_limit,
                        )
                        .await;
                        // Everything this client has read is what it may
                        // drop: its fold's cursor, or its own writes' end
                        // when it wrote past what it read.
                        let up_to = writer.next_seq().max(fold.cursor());
                        // The owner truncates under its own fence (#228). A
                        // superseded owner sends nothing — or, as the
                        // deliberate misbehaviour, its old uuid, which the
                        // journal must refuse.
                        let stale = writer.owned().is_none()
                            && writer.fence() != writer.uuid()
                            && raw_policy % 100 < config.stale_truncate_pct;
                        let fence = if stale {
                            assert_reachable!("chain: a superseded owner sends a stale truncate");
                            Some(writer.fence())
                        } else {
                            fence(&writer)
                        };
                        let outcome =
                            truncate_traced(nodes.leader().unwrap_or(target), fence, up_to).await;
                        absorb_truncate(&mut writer, outcome.as_ref());
                    }
                }
                TRUNCATE_STORM => {
                    let base = writer.next_seq().max(fold.cursor());
                    // A storm is the owner's (#228): a writer that owns no
                    // generation sends none.
                    if let (true, Some(leader)) = (config.compaction && base > 0, fence(&writer)) {
                        let first_mode = usize::try_from(raw_pause % 3).unwrap_or(0);
                        for attempt in 0..config.compact_storm_attempts {
                            let mode = (first_mode + attempt) % 3;
                            let (mode_name, up_to, request_target) = match mode {
                                // Far past the journal's tail: a truncation
                                // is clamped to `next_seq` at apply (#204),
                                // and the fence below turns it into the
                                // furthest truncation every folding client
                                // allows.
                                0 => (
                                    "overask",
                                    base.saturating_add(10_000 + raw_payload % 10_000),
                                    nodes.leader().unwrap_or(target),
                                ),
                                1 if server_count > 1 && nodes.leader().is_some() => {
                                    let leader = nodes.leader().unwrap_or(target) % server_count;
                                    let offset = 1 + usize::try_from(
                                        (raw_target + u64::try_from(attempt).unwrap_or(0))
                                            % u64::try_from(server_count - 1).unwrap_or(1),
                                    )
                                    .unwrap_or(0);
                                    ("follower", base, (leader + offset) % server_count)
                                }
                                2 if nodes.hint().stale.is_some()
                                    && nodes.hint().stale != nodes.leader() =>
                                {
                                    ("stale-leader", base, nodes.hint().stale.unwrap_or(target))
                                }
                                _ => continue,
                            };
                            let Some(up_to) = fold::clamp(ctx.state(), journal, up_to) else {
                                continue;
                            };
                            trace_truncate(leader, up_to);
                            tracing::info!(
                                up_to,
                                target = request_target,
                                mode = mode_name,
                                attempt,
                                "chain_compact_storm_request"
                            );
                            if !self.adversarial.compact_storm_modes[mode] {
                                match mode {
                                    0 => {
                                        assert_reachable!("chain: compact-storm overask executes");
                                    }
                                    1 => {
                                        assert_reachable!(
                                            "chain: compact-storm follower request executes"
                                        );
                                    }
                                    2 => {
                                        assert_reachable!(
                                            "chain: compact-storm stale-leader request executes"
                                        );
                                    }
                                    _ => unreachable!("compact storm mode is modulo three"),
                                }
                                self.adversarial.compact_storm_modes[mode] = true;
                            }
                            let request = Truncate {
                                journal: journal.journal.0,
                                tenant: journal.tenant.0,
                                up_to,
                                leader: Some(leader_uuid_to_proto(leader)),
                            };
                            match truncate_once(request_target, request).await {
                                TruncateOutcome::Applied { state } => {
                                    nodes.observe_leader_at(request_target);
                                    tracing::info!(
                                        up_to,
                                        first_seq = state.first_seq.0,
                                        "chain_compact_accepted"
                                    );
                                }
                                TruncateOutcome::Redirect { leader } => {
                                    nodes.observe_leader(leader);
                                }
                                // Superseded mid-storm: the rest of the
                                // storm is refused alike, and the writer
                                // learns it from its next write.
                                TruncateOutcome::WrongMode { .. } => owner_never_of_wrong_mode(),
                                TruncateOutcome::Refused { .. }
                                | TruncateOutcome::UnknownJournal
                                | TruncateOutcome::Malformed
                                | TruncateOutcome::Ambiguous => {}
                            }
                        }
                    }
                }
                READ => {
                    if !self.adversarial.read_executed {
                        assert_reachable!("chain: journal-read operation executes");
                        self.adversarial.read_executed = true;
                    }
                    // Where the read starts, from the class draw: the
                    // tailing cursor (most often — the reader that long-polls
                    // at the tail and meets `first_seq` as the journal
                    // moves), this client's own last written position, the
                    // journal's start, or far past any tail (a long-poll
                    // answered empty).
                    //
                    // Race 3 (#205): a reader at a lagging cursor — at or
                    // below this client's fold, outside the trim fence — while
                    // this client truncates to everything it folded. The
                    // slot order decides whether the page or the truncation
                    // comes first; a reader refused as truncated resumes at
                    // the floor the refusal names.
                    let racing = buggify_with_prob!(0.15);
                    let tailing = !racing && raw_class % 4 < 2;
                    let from = if racing {
                        raw_payload % (fold.cursor() + 1)
                    } else {
                        match raw_class % 4 {
                            0 | 1 => fold.cursor(),
                            2 => written.last().map_or(0, |w| w.seq),
                            _ => {
                                if raw_class & (1 << 8) != 0 {
                                    0
                                } else {
                                    fold.cursor().saturating_add(1 << 20)
                                }
                            }
                        }
                    };
                    let race_up_to = writer.next_seq().max(fold.cursor());
                    let race_fence = fence(&writer);
                    // Any node or replica serves a journal read.
                    let span = server_count + replica_count;
                    let mut drawn = usize::try_from(raw_target >> 32).unwrap_or(0) % span.max(1);
                    // A client naming a journal this deployment does not
                    // serve (the unset identifier, or another tenant's) must be
                    // refused, never answered from the wrong journal.
                    let stray = !racing && buggify_with_prob!(0.05);
                    let named = if stray {
                        assert_reachable!("chain: a client asks for a journal nobody serves");
                        if raw_policy & 1 == 0 {
                            JournalIdentifier::UNSET
                        } else {
                            // An identifier nobody serves (#235): this journal's
                            // id in a tenant no run draws (`u64::MAX` is
                            // outside every draw) — the right journal id
                            // under the wrong tenant must be refused too.
                            JournalIdentifier::new(TenantId(u64::MAX), journal.journal)
                        }
                    } else {
                        journal
                    };
                    let op_id = next_op;
                    next_op += 1;
                    self.history.record_read_issued(op_id, now_ms());
                    // One read, retried at the next server while its
                    // quorum read goes unserved, inside one deadline.
                    let deadline =
                        time.now() + read_timeout + Duration::from_millis(config.read_wait_ms);
                    let mut attempts = 0_u64;
                    let read = async {
                        loop {
                            let remaining = deadline.saturating_sub(time.now());
                            if remaining.is_zero() || shutdown.is_cancelled() {
                                break ReadOutcome::Ambiguous;
                            }
                            attempts += 1;
                            let call = read_once(
                                &readers,
                                drawn,
                                named,
                                from,
                                config.read_limit,
                                config.read_wait_ms,
                            );
                            let answer = within(ctx, remaining, ReadOutcome::Ambiguous, call).await;
                            if answer.is_served() || answer == ReadOutcome::UnknownJournal {
                                break answer;
                            }
                            drawn = (drawn + 1) % span.max(1);
                        }
                    };
                    let truncation = async {
                        if racing && !reader && config.compaction {
                            assert_reachable!("chain: a truncation races a reader's cursor");
                            let _ = truncate_traced(
                                nodes.leader().unwrap_or(target),
                                race_fence,
                                race_up_to,
                            )
                            .await;
                        }
                    };
                    let (answer, ()) = futures::join!(read, truncation);
                    // Race 3's reader (#205): a cursor at `from`, folding
                    // the answer — a truncation moves it to the floor the
                    // refusal names and reports the gap (the library's
                    // `Reader`).
                    let mut race_reader = paros::client::Reader::new(journal, from);
                    let gap = (racing && matches!(answer, ReadOutcome::Truncated { .. }))
                        .then(|| race_reader.absorb(answer.clone()));
                    // Only an answer is judged: one that never came (the unset
                    // id is refused at the edge, as a transport error) or went
                    // unserved is ambiguous, never assumed.
                    let answered = answer.is_served() || answer == ReadOutcome::UnknownJournal;
                    match answer {
                        answer if stray && answered => {
                            assert_always!(
                                answer == ReadOutcome::UnknownJournal,
                                "chain: a read naming another journal is refused",
                                { "journal" => named }
                            );
                            self.history.record_read_failed(op_id);
                        }
                        answer if answered => {
                            assert_always!(
                                answer != ReadOutcome::UnknownJournal,
                                "chain: a node serves the journal the client names",
                                {
                                    "node" => readers.id_of(drawn),
                                    "tenant" => named.tenant.0,
                                    "journal" => named.journal.0
                                }
                            );
                            judge_read(&audit, from, &answer, &written);
                            let tail = answer
                                .state()
                                .and_then(|state| state.next_seq.0.checked_sub(1));
                            // Per-client monotonicity: this client's reads
                            // never observe a shrinking journal.
                            assert_always!(
                                tail >= last_read_tail,
                                "chain: a client's read states never move backwards",
                                {
                                    "previous" => crate::signed_watermark(last_read_tail),
                                    "observed" => crate::signed_watermark(tail),
                                }
                            );
                            last_read_tail = last_read_tail.max(tail);
                            if answer.is_served() {
                                self.history
                                    .record_read_ack(op_id, tail, attempts, now_ms());
                            } else {
                                self.history.record_read_failed(op_id);
                            }
                            if tailing {
                                // A tailing page folds into this client's
                                // state and moves its cursor forward.
                                fold.absorb(&audit, ctx.state(), client_id, from, &answer);
                            }
                        }
                        // Unserved (its quorum read did not confirm in
                        // time), or no answer: ambiguous, never assumed.
                        _ => self.history.record_read_failed(op_id),
                    }
                    // The raced reader resumes at the floor it was refused
                    // below — or, truncated again, at the next floor.
                    if let Some(paros::client::ReaderOutcome::Gap { .. }) = gap {
                        for k in 0..span.clamp(1, 4) {
                            let resume_at = race_reader.cursor();
                            let call = read_once(
                                &readers,
                                (drawn + k) % span.max(1),
                                journal,
                                resume_at,
                                config.read_limit,
                                0,
                            );
                            let answer =
                                within(ctx, read_timeout, ReadOutcome::Ambiguous, call).await;
                            if answer.is_served() {
                                judge_read(&audit, resume_at, &answer, &written);
                            }
                            if let paros::client::ReaderOutcome::Records { .. } =
                                race_reader.absorb(answer)
                            {
                                self.adversarial.reader_resumed = true;
                                break;
                            }
                        }
                    }
                }
                READ_STATE => {
                    // Fold to the tail through any node or replica: the
                    // application's state is this client's fold (#186).
                    let span = server_count + replica_count;
                    let drawn = usize::try_from(raw_target >> 32).unwrap_or(0) % span.max(1);
                    fold.read_to_tail(ctx, &audit, &readers, drawn, client_id, config.read_limit)
                        .await;
                    tracing::info!(
                        index = fold.state().applied_count,
                        state = %hash_text(fold.state().chain_hash),
                        "chain_state_read"
                    );
                }
                PAUSE => {
                    let delay = 1 + raw_pause % config.pause_ms;
                    moonpool_sim::select! {
                        _ = time.sleep(Duration::from_millis(delay)) => {}
                        () = shutdown.cancelled() => {}
                    }
                }
                // Retired ids (see the constants): no-ops that keep the
                // alphabet stable.
                MATCHMAKE | MATCH_GC | READ_INDEX | QUORUM_READ | CHECK_TAIL => {}
                RECONFIGURE => {
                    // Read the configuration in force from the hinted leader
                    // (or the step's target): every node learns it from the
                    // ballot's `Prepare`, so a stale answer only makes the
                    // request refused (`unchanged`, `unknown_member`) — an
                    // operating condition, never a wrong state.
                    let probe_target = nodes.leader().unwrap_or(target);
                    let in_force = nodes.inspect(probe_target, journal).await.map(|reply| {
                        let wire = WireQuorumSystem {
                            quorum_system: reply.quorum_system,
                            phase1_quorum: reply.phase1_quorum,
                            phase2_quorum: reply.phase2_quorum,
                            rows: reply.rows,
                            cols: reply.cols,
                        };
                        (reply.members, quorum_system_from_proto(&wire).ok())
                    });
                    let members = in_force.as_ref().map(|(members, _)| members.clone());
                    let system_in_force = in_force.and_then(|(_, system)| system);
                    // The owner's first operation after its claim (see
                    // `reconfigure_after_claim`) starts the shape ring at
                    // one that moves a member out — never `grow`.
                    // On a departed-straggler seed it is a `rotate`, whole
                    // when the spares allow: the successor's fresh members
                    // never held the departed members' slots, so the newest
                    // configuration alone may hold a quorum of `none`
                    // answers where the prior one does not, the sub-shape
                    // only the cross-configuration Phase 1 decides (#267).
                    let scenario_rotation =
                        after_claim && crate::shape::departed_straggler(ctx.state());
                    let drawn = if scenario_rotation {
                        ROTATE_SHAPE
                    } else if after_claim {
                        REMOVING_SHAPES[usize::try_from(raw_class % 4).unwrap_or(0)]
                    } else {
                        weighted_index(&config.reconfigure_shape_weights, raw_class)
                    };
                    let leader_id = nodes.leader().map(|l| nodes.id_of(l));
                    // The successor draws from the live pool: an identity the
                    // run lost for good (wiped, retired, corruption-parked)
                    // is never asked for, and is the first one moved out.
                    let mut live = live_candidates(
                        &servers,
                        &crate::world::parked_nodes(ctx.state(), journal),
                    );
                    // The joiners the node registry admitted (#189): a
                    // successor may pull one in. Read here, before the
                    // composition and its ledger entry, which take no await
                    // between them — a retirement reserved in the meantime
                    // is re-checked at the ledger.
                    let joinable = system_ops.joinable(ctx, &nodes, raw_payload).await;
                    live.extend(joinable.iter().copied());
                    // The adversarial draw (R5): compose from *every* rank
                    // instead, so the request may name an identity the run
                    // lost for good. A well-behaved operator would not, and
                    // the protocol must survive one who does. What it must
                    // never be asked for is an *unwinnable* configuration —
                    // one whose live members cannot form a quorum — so every
                    // composition, adversarial or not, is filtered on that
                    // and the shape ring falls through to one that holds.
                    // This binds the ordinary path too: `grow` keeps the set
                    // in force whole, so growing onto a spare from a
                    // configuration that already carried a dead member left
                    // one live of two (hunt seed 11169765483580423663); the
                    // ring now reaches `replace`/`shrink` instead, which move
                    // the dead identity out — the composer's documented job.
                    let all_ranks: Vec<u64> =
                        (0..u64::try_from(server_count).unwrap_or(0)).collect();
                    let adversarial_members = buggify_with_prob!(0.10);
                    // A whole-set rotation (#173): the successor off the
                    // spares alone. The ring's random start lands there only
                    // by luck, and it is the shape that leaves every
                    // rebooted member outside the bootstrap belief.
                    let whole_rotation = scenario_rotation || buggify_with_prob!(0.5);
                    // The successor's quorum system (#140, #141): the seed's
                    // policy at the successor's own size — or, on a flexible
                    // or a grid seed, a coin that composes a *majority*
                    // successor instead, so the cross-configuration Phase 1
                    // asks two different systems their own predicates. Only
                    // that direction: a majority successor always sits
                    // inside the copy budget a flexible or a grid policy was
                    // sized for (its tolerated loss is never below theirs),
                    // while a split or a grid on a majority seed would not,
                    // so a majority seed never composes one. A grid policy
                    // switches on its own too, at every size no layout
                    // tiles (`QuorumPolicy::system`).
                    let switch_to_majority = matches!(
                        policy,
                        crate::shape::QuorumPolicy::Flexible { .. }
                            | crate::shape::QuorumPolicy::Grid { .. }
                    ) && buggify_with_prob!(0.25);
                    let successor_system = |n: usize| {
                        if switch_to_majority {
                            QuorumSystem::Majority
                        } else {
                            policy.system(n)
                        }
                    };
                    // A live quorum of *both* phases under the successor's
                    // own system: Phase 1 must complete against it (it is in
                    // `H_b` from then on) and Phase 2 must decide under it.
                    // Asked of the membership boundary, never a count. On a
                    // grid, Phase 2 is asked of **every** column: each slot
                    // is decided by its own column (`column_of`), so one
                    // live column decides only its own slots, and a column
                    // with a member lost for good freezes the rest — the
                    // leader's recovery never closes and no later
                    // reconfiguration can move the dead member out (#198).
                    let keeps_live_quorum = |next: &[u64]| {
                        let live_members: BTreeSet<u64> =
                            next.iter().filter(|m| live.contains(m)).copied().collect();
                        let system = successor_system(next.len());
                        let columns: BTreeSet<usize> = (0..next.len() as u64)
                            .filter_map(|slot| system.column_of(paros::Slot(slot)))
                            .collect();
                        let phase2 = if columns.is_empty() {
                            system.is_phase2_quorum(next, &live_members)
                        } else {
                            columns.iter().all(|column| {
                                system.is_phase2_quorum_in(next, &live_members, Some(*column))
                            })
                        };
                        system.is_phase1_quorum(next, &live_members) && phase2
                    };
                    // Most shapes need a spare, which most seeds do not have:
                    // walk the shape ring from the draw so an impossible
                    // shape falls through to the next one instead of making
                    // the whole step a silent no-op.
                    let compose_from = |candidates: &[u64]| {
                        members.as_deref().and_then(|members| {
                            (0..RECONFIGURE_SHAPES.len()).find_map(|k| {
                                let shape = (drawn + k) % RECONFIGURE_SHAPES.len();
                                compose_reconfiguration(
                                    shape,
                                    members,
                                    candidates,
                                    config_floor,
                                    leader_id,
                                    raw_payload,
                                    whole_rotation,
                                )
                                .filter(|(_, _, next)| keeps_live_quorum(next))
                                .map(|(observed, name, next)| (shape, observed, name, next))
                            })
                        })
                    };
                    // A registered joiner (#189) is one spare among the
                    // pool's, and growing onto it is the rare step the
                    // system board's "joins a journal's configuration
                    // through Reconfigure" gate waits on: on a coin of the
                    // step's policy draw, a step with one joinable draws the
                    // new member from the joiners alone (the members in
                    // force stay candidates), falling back to the whole
                    // live pool when no shape holds — always on the step
                    // right after a registration. A composition policy,
                    // like the shape draw — a per-seed BUGGIFY activation
                    // left the CI sweep's 1,024 seeds short of the gate.
                    let prefer_joiner =
                        !joinable.is_empty() && (after_register || (raw_policy >> 11) % 2 == 0);
                    let joiner_first: Vec<u64> = live
                        .iter()
                        .copied()
                        .filter(|n| {
                            joinable.contains(n)
                                || members.as_deref().is_some_and(|m| m.contains(n))
                        })
                        .collect();
                    let composed = if adversarial_members {
                        compose_from(&all_ranks).or_else(|| compose_from(&live))
                    } else if prefer_joiner {
                        let onto_joiner = compose_from(&joiner_first);
                        if onto_joiner.is_some() {
                            assert_reachable!(
                                "reconfiguration: the composer draws a successor's new member from the registered joiners"
                            );
                        }
                        onto_joiner.or_else(|| compose_from(&live))
                    } else {
                        compose_from(&live)
                    };
                    if let Some((shape, observed, name, next)) = composed {
                        assert_always!(
                            keeps_live_quorum(&next),
                            "reconfiguration: a requested configuration keeps a live quorum",
                            {
                                "members" => next.len() as u64,
                                "live" => next.iter().filter(|m| live.contains(m)).count() as u64
                            }
                        );
                        if next.iter().any(|m| !live.contains(m)) {
                            assert_reachable!(
                                "reconfiguration: a requested configuration names an identity lost for good"
                            );
                        }
                        if shape != drawn {
                            assert_reachable!(
                                "reconfiguration: the drawn shape is impossible and the step falls through"
                            );
                        }
                        if after_claim && REMOVING_SHAPES.contains(&shape) {
                            // BUGGIFY pairing: the owner's first operation
                            // after its claim moves a member out.
                            assert_reachable!(
                                "reconfiguration: an owner's first operation after its claim removes a member"
                            );
                        }
                        let disjoint = members
                            .as_deref()
                            .is_some_and(|in_force| in_force.iter().all(|m| !next.contains(m)));
                        if disjoint {
                            assert_reachable!(
                                "reconfiguration: a successor acceptor set shares no member with its predecessor"
                            );
                        }
                        let mut system = successor_system(next.len());
                        if system_in_force.is_some_and(|in_force| {
                            std::mem::discriminant(&in_force) != std::mem::discriminant(&system)
                        }) {
                            assert_reachable!(
                                "reconfiguration: the client composes a successor under a different quorum system"
                            );
                        }
                        // The adversarial half (R5's spirit): an operator who
                        // names a quorum system the membership does not admit
                        // must be refused at the wire, never crash the node.
                        // The workload judges "malformed" with its own
                        // arithmetic, never the library's `admits` (#269: a
                        // mutant of `admits` also silenced the request).
                        let malformed = buggify_with_prob!(0.05)
                            .then(|| malformed_system(next.len(), ctx.random().random::<u64>()))
                            .flatten();
                        if let Some(bad) = malformed {
                            system = bad;
                        }
                        let malformed = malformed.is_some();
                        tracing::info!(shape = name, members = ?next, ?system, "chain_reconfigure_request");
                        remove_next = false;
                        // The operators' ledger (#198): filed before the
                        // request leaves, answered below; a retirement reads
                        // it (`StorageWorld::retire`).
                        let ledger_id = ledger_request(ctx.state(), &next);
                        let outcome = reconfigurer.reconfigure(&next, system, probe_target).await;
                        tracing::info!(shape = name, outcome = ?outcome, "chain_reconfigure_outcome");
                        ledger_answer(ctx.state(), ledger_id, &outcome);
                        match outcome {
                            ReconfigureOutcome::Started { leader, .. } => {
                                // The AGENTS.md rule, client-visible: a
                                // deployment without matchmakers never honors
                                // a reconfiguration.
                                assert_always!(
                                    has_matchmakers,
                                    "reconfiguration: a deployment without matchmakers never accepts a reconfiguration",
                                    { "shape" => name }
                                );
                                assert_always!(
                                    !malformed,
                                    "reconfiguration: a configuration that does not admit its quorum system is never started",
                                    { "shape" => name }
                                );
                                self.adversarial.reconfigure_started[observed] = true;
                                nodes.observe_leader(leader);
                                // A rare-but-valid operator act (#173):
                                // reboot every member of the configuration
                                // just installed. Each loses its belief in
                                // force and boots to the bootstrap one, so a
                                // successor disjoint from the bootstrap is a
                                // cluster whose members all believe they are
                                // outside the configuration in force, and
                                // whose non-members know better but do not
                                // lead. A clean reboot keeps every disk. A
                                // successor sharing no member with its
                                // predecessor is the shape that leaves no
                                // rebooted member inside the default, so it
                                // is the one the location leans on.
                                if buggify_with_prob!(if disjoint { 0.9 } else { 0.25 }) {
                                    let _ = time
                                        .sleep(Duration::from_millis(
                                            config.reboot_successor_delay_ms,
                                        ))
                                        .await;
                                    assert_reachable!(
                                        "reconfiguration: the client reboots every member of the configuration it installed"
                                    );
                                    for member in &next {
                                        if let Some(ip) = usize::try_from(*member)
                                            .ok()
                                            .and_then(|rank| servers.get(rank))
                                        {
                                            crate::lifecycle::restart(ctx, ip).await;
                                        }
                                    }
                                }
                            }
                            ReconfigureOutcome::Refused { leader, refusal } => {
                                if refusal == ReconfigureRefusal::NoMatchmakers {
                                    assert_always!(
                                        !has_matchmakers,
                                        "reconfiguration: only a deployment without matchmakers refuses for lack of them",
                                        { "shape" => name }
                                    );
                                    self.adversarial.reconfigure_refused_plain = true;
                                }
                                if refusal == ReconfigureRefusal::Malformed {
                                    assert_always!(
                                        malformed,
                                        "reconfiguration: only a configuration that does not admit its quorum system is refused as malformed",
                                        { "shape" => name }
                                    );
                                    assert_reachable!(
                                        "reconfiguration: a configuration that does not admit its quorum system is refused"
                                    );
                                }
                                adopt_plane_leader(&nodes, has_matchmakers, leader);
                            }
                            ReconfigureOutcome::NotLeader { leader }
                            | ReconfigureOutcome::Unrecognized { leader } => {
                                adopt_plane_leader(&nodes, has_matchmakers, leader);
                            }
                            ReconfigureOutcome::Ambiguous => {}
                        }
                    }
                }
                RECONFIGURE_MATCHMAKERS => {
                    // Any node may drive a matchmaker handover, and every
                    // node learns the authoritative set: read it from the
                    // step's target and ask that same node. A stale answer
                    // only makes the handover superseded or refused — an
                    // operating condition, never a wrong state.
                    let current: Option<(u64, Vec<u64>)> = nodes
                        .inspect(target, journal)
                        .await
                        .map(|reply| (reply.matchmaker_generation, reply.matchmakers));
                    let drawn_slot = weighted_index(&config.matchmaker_shape_weights, raw_class);
                    let candidates = live_candidates(
                        &matchmaker_ips,
                        &crate::world::parked_matchmakers(ctx.state()),
                    );
                    let request = if has_matchmakers {
                        // The same shape ring as the acceptor composer: a
                        // matchmaker set at its floor admits no shrink and a
                        // full bootstrap leaves no spare, so a fixed shape
                        // would make the step a no-op for the whole run.
                        current.as_ref().and_then(|(_, members)| {
                            (0..MATCHMAKER_SHAPES.len()).find_map(|k| {
                                let slot = (drawn_slot + k) % MATCHMAKER_SHAPES.len();
                                compose_reconfiguration(
                                    MATCHMAKER_SHAPES[slot],
                                    members,
                                    &candidates,
                                    matchmaker_floor,
                                    None,
                                    raw_payload,
                                    false,
                                )
                                .map(|(observed, name, next)| {
                                    // The observed shape's own slot: a
                                    // rotation that came out a shrink is
                                    // gated as the shrink it is.
                                    let observed_slot = MATCHMAKER_SHAPES
                                        .iter()
                                        .position(|s| *s == observed)
                                        .unwrap_or(slot);
                                    (slot, observed_slot, name, next)
                                })
                            })
                        })
                    } else {
                        // Plain Multi-Paxos: the request is sent anyway, and
                        // the point is the refusal.
                        current
                            .is_some()
                            .then_some((drawn_slot, drawn_slot, "plain", vec![0]))
                    };
                    if let Some((shape_slot, observed_slot, name, next)) = request {
                        if shape_slot != drawn_slot {
                            assert_reachable!(
                                "reconfiguration: the drawn shape is impossible and the step falls through"
                            );
                        }
                        if current
                            .as_ref()
                            .is_some_and(|(_, in_force)| in_force.iter().all(|m| !next.contains(m)))
                        {
                            assert_reachable!(
                                "generation: a successor matchmaker set shares no member with its predecessor"
                            );
                        }
                        tracing::info!(shape = name, members = ?next, "chain_reconfigure_matchmakers_request");
                        let outcome = matchmaker_reconfigurer
                            .reconfigure_matchmakers(&next, target)
                            .await;
                        tracing::info!(shape = name, outcome = ?outcome, "chain_reconfigure_matchmakers_outcome");
                        match outcome {
                            ReconfigureMatchmakersOutcome::Started { generation } => {
                                assert_always!(
                                    has_matchmakers,
                                    "generation: a deployment without matchmakers never accepts a matchmaker reconfiguration",
                                    { "shape" => name }
                                );
                                // The node may have learned a newer generation
                                // between the read and the request (a handover
                                // completed in between): the set it starts
                                // from is its own, and the client's stale
                                // composition is what a rotate through the
                                // pool looks like — an operating condition.
                                tracing::info!(
                                    shape = name,
                                    generation,
                                    "chain_reconfigure_matchmakers_started"
                                );
                                self.adversarial.reconfigure_matchmakers_started[observed_slot] =
                                    true;
                            }
                            ReconfigureMatchmakersOutcome::Refused(refusal) => {
                                assert_always!(
                                    (refusal == MatchmakersRefusal::NoMatchmakers) != has_matchmakers,
                                    "generation: only a deployment without matchmakers refuses for lack of them",
                                    { "shape" => name, "refusal" => format!("{refusal:?}") }
                                );
                            }
                            ReconfigureMatchmakersOutcome::Ambiguous => {}
                        }
                    }
                }
                RETIRE => {
                    // Only a leader reports what its effective floor retired;
                    // a follower answers an empty list and the step is a
                    // no-op.
                    let probe_target = nodes.leader().unwrap_or(target);
                    // The retirable list, the configuration in force and the
                    // effective GC watermark come from the *same* reply: the
                    // world can hold the protocol to "a retirable node is
                    // outside C_b", and the node itself refuses the request
                    // unless the watermark proves every configuration it was
                    // a member of is forgotten (#123).
                    let inspected = nodes.inspect(probe_target, journal).await;
                    let (retirable, in_force, gc_watermark) = inspected
                        .map(|reply| (reply.retirable, reply.members, reply.gc_watermark))
                        .unwrap_or_default();
                    assert_always!(
                        retirable.is_empty() || has_matchmakers,
                        "gc: a deployment without matchmakers never names a retirable node"
                    );
                    let parked = crate::world::parked_nodes(ctx.state(), journal);
                    let live = |id: &u64| {
                        usize::try_from(*id)
                            .ok()
                            .filter(|i| *i < server_count && !parked.contains(&servers[*i]))
                    };
                    // The stale member (#165), its own location: a member of
                    // the configuration the floor kept whose *own* belief
                    // does not name it — it never heard that configuration,
                    // or rebooted to its bootstrap belief and has not heard a
                    // beat since. The window is narrow, so a blind aim almost
                    // never lands in it; this operator asks every member at
                    // once what it believes (one request timeout for all) and
                    // aims at the first that does not know it is one. The
                    // node must still refuse (`stale`). None found: the
                    // ordinary draw below, so the retirement mix is kept.
                    let mut stale_member = None;
                    if gc_watermark.is_some() && buggify_with_prob!(0.25) {
                        assert_reachable!(
                            "gc: an operator probes the members' beliefs before a retirement"
                        );
                        let candidates: Vec<usize> = in_force.iter().filter_map(live).collect();
                        let beliefs =
                            join_all(candidates.iter().map(|i| nodes.inspect(*i, journal))).await;
                        stale_member = candidates.iter().zip(beliefs).find_map(|(i, reply)| {
                            let own = u64::try_from(*i).unwrap_or(u64::MAX);
                            reply
                                .is_some_and(|reply| !reply.members.contains(&own))
                                .then_some(*i)
                        });
                        if stale_member.is_some() {
                            assert_reachable!(
                                "gc: a retirement is aimed at a member whose belief does not name it"
                            );
                        }
                    }
                    // The adversarial aim (R5): send the retirement to a node
                    // the *same* reply names as a current member instead of a
                    // retirable one. A well-behaved operator would not; the
                    // node must refuse it (`member`, or `leader` when it is
                    // the sitting one), so the world reservation is skipped
                    // for this draw — nothing is parked, and a refusal has
                    // nothing to release.
                    let aim_at_member = stale_member.is_some() || buggify_with_prob!(0.10);
                    let pool: &[u64] = if aim_at_member { &in_force } else { &retirable };
                    let victims: Vec<usize> = pool.iter().filter_map(live).collect();
                    if aim_at_member && !victims.is_empty() {
                        assert_reachable!("gc: a retirement is aimed at a current member");
                    }
                    if !victims.is_empty() {
                        let victim = stale_member.unwrap_or(
                            victims[usize::try_from(
                                raw_payload % u64::try_from(victims.len()).unwrap_or(1),
                            )
                            .unwrap_or(0)],
                        );
                        // The racing operator (#198), its own location: a
                        // reconfiguration that puts the victim back, asked
                        // for just before the retirement — the order two
                        // uncoordinated clients produced (the re-add is
                        // registered, and on its way to the victim, when the
                        // victim accepts its retirement). The operators'
                        // ledger must withhold the retirement; without it a
                        // grid successor is installed with a member dead for
                        // good.
                        if !aim_at_member
                            && has_matchmakers
                            && !in_force.is_empty()
                            && buggify_with_prob!(0.25)
                        {
                            let mut readd = in_force.clone();
                            readd.push(u64::try_from(victim).unwrap_or(u64::MAX));
                            readd.sort_unstable();
                            readd.dedup();
                            assert_reachable!(
                                "gc: an operator asks to re-add a node just before retiring it"
                            );
                            let ledger_id = ledger_request(ctx.state(), &readd);
                            let system = policy.system(readd.len());
                            let outcome =
                                reconfigurer.reconfigure(&readd, system, probe_target).await;
                            ledger_answer(ctx.state(), ledger_id, &outcome);
                        }
                        // Park the identity first, under the dead-node budget
                        // (a retirement is one more way to lose every copy a
                        // node holds); a restart of a parked identity exits
                        // at boot, so an ambiguous ack can never bring it
                        // back. Refused by the budget: the step is a no-op.
                        let reserved = if aim_at_member {
                            // No reservation: the target is a member, the
                            // node refuses, and parking it would remove a
                            // live acceptor the protocol still names.
                            true
                        } else {
                            let world = crate::world::storage_world(ctx.state());
                            let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
                            guard.retire(
                                &servers[victim],
                                u64::try_from(victim).unwrap_or(u64::MAX),
                                &in_force,
                                gc_watermark.map_or(0, |w| w.round),
                            )
                        };
                        if reserved {
                            tracing::info!(node = victim as u64, "chain_retire_request");
                            let outcome =
                                nodes.retire(victim, RetireRequest { gc_watermark }).await;
                            tracing::info!(node = victim as u64, outcome = ?outcome, "chain_retire_outcome");
                            match outcome {
                                RetireOutcome::Retired => self.adversarial.retired = true,
                                RetireOutcome::Refused(_) => {
                                    // Refused means the node is a member of
                                    // the configuration in force, is the
                                    // leader, or no effective floor sits
                                    // above its membership fence: it is still
                                    // live, so the pre-emptive park must be
                                    // undone or the harness has removed a
                                    // member outside the protocol. Only ever
                                    // on an explicit refusal — an ambiguous
                                    // ack may have been honored.
                                    self.adversarial.retire_refused = true;
                                    let world = crate::world::storage_world(ctx.state());
                                    let released = world
                                        .lock()
                                        .unwrap_or_else(PoisonError::into_inner)
                                        .release_retirement(
                                            &servers[victim],
                                            u64::try_from(victim).unwrap_or(u64::MAX),
                                        );
                                    self.adversarial.retire_released |= released;
                                }
                                // Ambiguous: the park stands for good (an
                                // honored retirement must never come back),
                                // so the audit excuses the identity now
                                // rather than at a boot that may never come.
                                RetireOutcome::Ambiguous if !aim_at_member => audit
                                    .note_retired_parked(u64::try_from(victim).unwrap_or(u64::MAX)),
                                RetireOutcome::Ambiguous => {}
                            }
                        }
                    }
                }
                CREATE_JOURNAL => {
                    fleet_ops
                        .create_journal(
                            ctx,
                            config.tunables().checkpoint_policy(),
                            (raw_class, raw_payload),
                        )
                        .await;
                }
                DELETE_JOURNAL => {
                    fleet_ops
                        .delete_journal(ctx, config.tunables().checkpoint_policy(), raw_payload)
                        .await;
                }
                REGISTER_NODE => {
                    // An operator who registers a node usually adds it next
                    // (#189): the client's next step grows a configuration
                    // onto the joiner it just registered.
                    // On a lagging-fold seed the reconfiguration follows
                    // whatever the swarm mask: the scenario's ingredients
                    // come together (`crate::shape::lagging_fold`).
                    reconfigure_next = system_ops
                        .registry_step(ctx, &nodes, None, raw_payload)
                        .await
                        && journal == main
                        && (operations.contains(&RECONFIGURE)
                            || crate::shape::lagging_fold(ctx.state()));
                }
                DRAIN_NODE => {
                    let _ = system_ops
                        .registry_step(
                            ctx,
                            &nodes,
                            Some(paros::system::NodeStanding::Registered),
                            raw_payload,
                        )
                        .await;
                }
                RETIRE_NODE => {
                    let _ = system_ops
                        .registry_step(
                            ctx,
                            &nodes,
                            Some(paros::system::NodeStanding::Draining),
                            raw_payload,
                        )
                        .await;
                }
                CHECKPOINT => {
                    system_ops
                        .checkpoint(
                            ctx,
                            &nodes,
                            config.tunables().checkpoint_policy(),
                            raw_payload,
                        )
                        .await;
                }
                BOOK_CAPACITY => system_ops.book(ctx, &nodes, raw_payload).await,
                FLEET_INIT => {
                    fleet_ops
                        .init(ctx, config.tunables().checkpoint_policy(), raw_payload)
                        .await;
                }
                TENANT => {
                    fleet_ops
                        .tenant(
                            ctx,
                            config.tunables().checkpoint_policy(),
                            (raw_class, raw_payload),
                        )
                        .await;
                }
                ADMIT => {
                    fleet_ops
                        .admit(ctx, config.tunables().checkpoint_policy(), raw_payload)
                        .await;
                }
                ELECTION => {
                    fleet_ops
                        .elect(
                            ctx,
                            config.tunables().checkpoint_policy(),
                            config.election_tunables(),
                            raw_payload,
                        )
                        .await;
                }
                VIEW => fleet_ops.view(ctx, raw_payload).await,
                _ => unreachable!("operation IDs are bounded by OP_COUNT"),
            }
        }

        if let (Some(ip), Some(_)) = (&parent_seed, parent_until.take()) {
            crate::lifecycle::restart(ctx, ip).await;
            crate::audit::journals::lock(&journal_board).release_parent();
        }
        assert_sometimes!(
            successful_after_ambiguity,
            "chain: ambiguous proposal is reconciled as committed"
        );
        assert_sometimes!(
            self.adversarial.duplicate_across_leader_change,
            "journal: a retried write is acked from the log across a leader change"
        );
        // The three races of #205, by their outcomes.
        assert_sometimes!(
            self.adversarial.retry_superseded,
            "journal: a retry after an ownership change is refused as superseded"
        );
        assert_sometimes!(
            self.adversarial.retry_acked_across_claim,
            "journal: a retry across an ownership change is acked from the log"
        );
        assert_sometimes!(
            self.adversarial.burst_fenced,
            "journal: a claim fences the rest of an owner's burst"
        );
        assert_sometimes!(
            self.adversarial.reader_resumed,
            "journal: a reader hits Truncated and resumes above the floor"
        );
        if self.adversarial.claim_won {
            assert_reachable!("chain: a client claims the journal mid-run");
        }
        if self.adversarial.reinstated {
            assert_reachable!("chain: a reinstated uuid leads again");
        }
        if self.adversarial.fenced {
            assert_reachable!("chain: a superseded writer learns the generation that fenced it");
        }
        if self.adversarial.reconfigure_started[0] {
            assert_reachable!("reconfiguration: the client grows the acceptor set onto a spare");
        }
        if self.adversarial.reconfigure_started[1] {
            assert_reachable!("reconfiguration: the client shrinks the acceptor set");
        }
        if self.adversarial.reconfigure_started[2] {
            assert_reachable!("reconfiguration: the client replaces one acceptor with a spare");
        }
        if self.adversarial.reconfigure_started[3] {
            assert_reachable!(
                "reconfiguration: the client removes the leader from the acceptor set"
            );
        }
        if self.adversarial.reconfigure_started[4] {
            assert_reachable!("reconfiguration: the client rotates the whole acceptor set");
        }
        if self.adversarial.reconfigure_refused_plain {
            assert_reachable!(
                "reconfiguration: a deployment without matchmakers refuses a reconfiguration"
            );
        }
        if self.adversarial.reconfigure_matchmakers_started[0] {
            assert_reachable!("generation: the client grows the matchmaker set onto a spare");
        }
        if self.adversarial.reconfigure_matchmakers_started[1] {
            assert_reachable!("generation: the client shrinks the matchmaker set");
        }
        if self.adversarial.reconfigure_matchmakers_started[2] {
            assert_reachable!("generation: the client replaces one matchmaker with a spare");
        }
        if self.adversarial.reconfigure_matchmakers_started[3] {
            assert_reachable!("generation: the client rotates the whole matchmaker set");
        }
        if self.adversarial.retired {
            assert_reachable!("gc: the client retires an acceptor the effective floor released");
        }
        if self.adversarial.retire_refused {
            assert_reachable!("gc: a retirement is refused by a node that is a member again");
        }
        if self.adversarial.retire_released {
            assert_reachable!("gc: a refused retirement releases the parked identity");
        }
        if self.adversarial.payload_classes[0] {
            assert_reachable!("chain: an empty payload is acknowledged");
        }
        if self.adversarial.payload_classes[1] {
            assert_reachable!("chain: a one-byte payload is acknowledged");
        }
        if self.adversarial.payload_classes[2] {
            assert_reachable!("chain: a boundary-sized payload is acknowledged");
        }
        if self.adversarial.payload_classes[3] {
            assert_reachable!("chain: a large payload is acknowledged");
        }

        // Everything that injects faults stops at the cutoff: paros' own driver
        // hooks and storage-fault layer by their own clock, and Moonpool's
        // network/storage/block families plus the partitions in force through
        // recovery mode. What survives is the *damage* — closed connections,
        // degraded pair latency, accumulated clock skew, rotted records, a node
        // still down its restart delay. Everything from here to
        // `recovery_budget_ms` is therefore an explicit quiet tail on live
        // replicas: election, `Accept` re-send, gap fill, catch-up, snapshot
        // transfer and chunk repair get real fault-free simulated time, and
        // convergence is judged only at its end.
        let cutoff = Duration::from_millis(CHAOS_DURATION_MS);
        if time.now() < cutoff {
            time.sleep(cutoff.checked_sub(time.now()).unwrap())
                .await
                .ok();
        }
        // The fleet's control plane in the recovery tail (#247): this
        // operator finishes the operation it stopped in, and the last one to
        // do so — every fleet writer is quiet then — judges the final folds
        // of the fleet directory and the cell, and every live node's registry fold.
        fleet_ops
            .settle(
                ctx,
                config.tunables().checkpoint_policy(),
                Duration::from_millis(config.retry_backoff_ms.max(10)),
            )
            .await;
        let last_to_settle = {
            let tail = tail(ctx.state());
            let mut guard = tail.lock().unwrap_or_else(PoisonError::into_inner);
            guard.fleet_settled += 1;
            guard.fleet_settled == guard.registered
        };
        if last_to_settle {
            let journals = self
                .plan
                .as_ref()
                .map(|plan| plan.ids.clone())
                .unwrap_or_default();
            // A node down for good — every journal it serves parked — follows
            // nothing; every joiner follows the registry whatever it stands.
            let expected: Vec<u64> = servers
                .iter()
                .enumerate()
                .filter(|(_, ip)| {
                    !journals
                        .iter()
                        .all(|j| crate::world::parked_nodes(ctx.state(), *j).contains(*ip))
                })
                .map(|(rank, _)| rank as u64)
                .chain(
                    (0..deployment.joiners().len())
                        .map(|rank| crate::roles::joiner_node_id(rank).0),
                )
                .collect();
            fleet_ops.final_check(ctx, &nodes, &expected).await;
        }
        // Every client folds to the tail as the chaos window closes (#205):
        // a client whose program never drew a fold holds the trim fence at
        // zero, which refused every truncation of the run — the reason the
        // trim-point reach collapsed once positions replaced slots. With
        // every cursor past zero, the tail truncation below can raise the
        // floor past a node or replica still down from the chaos window, and
        // the one coming back jumps to the trim point.
        fold.read_to_tail(
            ctx,
            &audit,
            &readers,
            usize::try_from(client_id).unwrap_or(0) % readers.server_count(),
            client_id,
            config.read_limit,
        )
        .await;
        // A replica held down across the tail (#205), its own location: an
        // operator stops a replica as the chaos window closes and starts it
        // again only once this owner's recovery batch and tail truncation
        // went by, so it comes back below the floor every acceptor raised
        // without it — the jump to the trim point a replica exists to
        // survive, which attrition alone reached once in a thousand runs
        // (its restarts mostly land before any truncation of the tail).
        let held_replica = (journal == crate::shape::identifiers(ctx.state()).main
            && client_id == 0)
            .then(|| deployment.replicas())
            .filter(|replicas| !replicas.is_empty())
            .filter(|_| buggify_with_prob!(0.5))
            .map(|replicas| replicas[0].clone());
        if let Some(ip) = &held_replica {
            assert_reachable!("chain: a replica is held down across the tail truncation");
            crate::lifecycle::crash(ctx, ip).await;
        }
        // The applied count the tail must move past (the audit tracks the
        // applied *slot*; the count is one past it).
        // One snapshot per journal (#357): the first client to get here takes
        // it, and every sibling on the journal reads it.
        let pre_tail_count = {
            let own = audit.cluster_applied_max().map_or(0, |slot| slot + 1);
            let tail = tail(ctx.state());
            let mut guard = tail.lock().unwrap_or_else(PoisonError::into_inner);
            *guard.snapshots.entry(journal).or_insert(own)
        };

        // A small recovery batch proves post-chaos forward progress and gives
        // the state frontier useful depth even when the swarmed operation mask
        // suppressed writes during the turbulent prefix. An owner writes it,
        // claiming the journal again whenever a verdict says another owner
        // holds it (#204); a reader has nothing to write, and its progress is
        // its fold at the end.
        let recovery_deadline = time.now() + Duration::from_millis(config.recovery_budget_ms);
        let mut recovery_acked = 0_u64;
        // Once a node's batch limits refuse a recovery write (#241), the
        // writer splits: every later recovery write is one small record,
        // which every limit's floor admits (`paros_sim::shape`).
        let mut split = false;
        let one_small = ChainConfig {
            batch_records: 1,
            large_command_bytes: config.command_bytes,
            ..config
        };
        let first = usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
        let mut target = nodes.leader().unwrap_or(first) % server_count;
        // A multi-writer journal's recovery batch (#241): one small record
        // a write, each sent until it is written, the open truncation
        // before the last.
        if multi_writer && !reader {
            for k in 0..config.recovery_proposals {
                let trim_to = if k + 1 == config.recovery_proposals
                    && recovery_acked > 0
                    && config.compaction
                    && !shutdown.is_cancelled()
                {
                    fold.read_to_tail(ctx, &audit, &readers, target, client_id, config.read_limit)
                        .await;
                    fold::clamp(ctx.state(), journal, fold.cursor())
                } else {
                    None
                };
                let raw = ctx.random().random::<u64>();
                let step = multi::Step {
                    ctx,
                    nodes: &nodes,
                    log: &log,
                    audit: &audit,
                    config: &one_small,
                    journal,
                    target,
                    server_count,
                    retarget: Retarget::FollowHint,
                    draws: (2, raw),
                    trim_to,
                };
                if trim_to.is_some() {
                    self.multi_step(&step, TRUNCATE, &mut next_op, &mut written)
                        .await;
                }
                if !self
                    .append_until_written(&step, &mut next_op, &mut written, recovery_deadline)
                    .await
                {
                    break;
                }
                recovery_acked = recovery_acked.saturating_add(1);
                target = (target + 1) % server_count;
            }
        }
        for k in 0..config.recovery_proposals {
            if reader || multi_writer {
                break;
            }
            // The tail truncation, before the batch's last write: an owner
            // that wrote its batch drops everything every client has folded
            // (the fence's clamp; it folds its own batch first, since the
            // fence holds a truncation below its own cursor too). Before the
            // last write, never after it: the chosen prefix is contiguous,
            // so that write's ack means the truncation's slot was decided
            // too. Issued last, an attempt that lingered past its answer (a
            // delegated round taken back beats later) was decided after
            // every client had judged the run converged, one slot past a
            // node the run then ended on (witness seed 11017340697535666646,
            // #205's 10k hunt: decided 2.8 s after its call gave up).
            if k + 1 == config.recovery_proposals
                && recovery_acked > 0
                && config.compaction
                && !shutdown.is_cancelled()
            {
                fold.read_to_tail(ctx, &audit, &readers, target, client_id, config.read_limit)
                    .await;
                let outcome =
                    truncate_traced(target, fence(&writer), writer.next_seq().max(fold.cursor()))
                        .await;
                absorb_truncate(&mut writer, outcome.as_ref());
            }
            let raw = ctx.random().random::<u64>();
            let mut acknowledged = false;
            // The write being retried: one operation for as long as its
            // generation and position still stand. A retry is the same write
            // (#204); re-submitting its bytes as a new operation would record
            // the log's `Duplicate` answer as a second write invoked after
            // reads that already saw the first.
            let mut pending: Option<Submission> = None;
            while time.now() < recovery_deadline && !shutdown.is_cancelled() {
                // The library's writer session (#221): claim when it owns
                // nothing, write as the owner, and stop — send nothing —
                // the moment a newer owner supersedes it.
                if writer.owned().is_none() {
                    match claim(&nodes, journal, target, writer.uuid()).await {
                        outcome @ (ClaimOutcome::Won { .. }
                        | ClaimOutcome::Lost { .. }
                        | ClaimOutcome::Owned { .. }) => {
                            writer.claimed(&outcome);
                        }
                        ClaimOutcome::Redirect { leader } => {
                            target = leader
                                .and_then(|id| nodes.index_of(id))
                                .unwrap_or((target + 1) % server_count);
                        }
                        ClaimOutcome::WrongMode { .. } => owner_never_of_wrong_mode(),
                        ClaimOutcome::UnknownJournal
                        | ClaimOutcome::Malformed
                        | ClaimOutcome::Unread
                        | ClaimOutcome::Ambiguous => {
                            target = (target + 1) % server_count;
                        }
                    }
                    if writer.owned().is_none() {
                        time.sleep(Duration::from_millis(config.retry_backoff_ms))
                            .await
                            .ok();
                        continue;
                    }
                }
                let submission = match pending.take() {
                    Some(retry)
                        if writer.owned() == Some(retry.entry.leader)
                            && retry.entry.seq.0 == writer.next_seq() =>
                    {
                        retry
                    }
                    _ => {
                        let shape = if split { &one_small } else { &config };
                        self.submit(&audit, shape, writer, &mut next_op, raw, raw, now_ms())
                    }
                };
                log.open_write(submission.op);
                let hinted = nodes.leader().is_some();
                let outcome = writer.write_entry(&nodes, &submission.entry, target).await;
                log.close_write();
                if hinted && nodes.leader().is_none() && outcome == WriterOutcome::Ambiguous {
                    hint_dropped = true;
                }
                match outcome {
                    WriterOutcome::Written {
                        seq,
                        count,
                        resolved,
                        ..
                    } => {
                        recovery_acked = recovery_acked.saturating_add(1);
                        acknowledged = true;
                        ambiguity_resolved |= resolved;
                        dropped_hint_written |= hint_dropped;
                        hint_dropped = false;
                        let via = nodes.leader().unwrap_or(target);
                        self.record_written(&submission, seq, count, now_ms());
                        written.push(submission.written(seq, count, via));
                        break;
                    }
                    WriterOutcome::Superseded { state } => {
                        // The writer owns nothing now: its next round
                        // claims before it sends anything again.
                        self.history.record_write_failed(submission.op);
                        assert_always!(
                            writer.owned().is_none()
                                && state.leader != Some(submission.entry.leader),
                            "client: a superseded writer owns nothing"
                        );
                        superseded_stopped = true;
                    }
                    WriterOutcome::NotWritten { .. } => {
                        self.history.record_write_failed(submission.op);
                        ambiguity_resolved = true;
                    }
                    WriterOutcome::TooLarge { .. } => {
                        // Nothing was proposed: the batch is dropped, not
                        // retried, and the next one is split.
                        assert_reachable!("chain: a recovery batch over a node's limits is split");
                        self.history.record_write_failed(submission.op);
                        split = true;
                        time.sleep(Duration::from_millis(config.retry_backoff_ms))
                            .await
                            .ok();
                        continue;
                    }
                    WriterOutcome::WrongMode { .. } => {
                        owner_never_of_wrong_mode();
                        self.history.record_write_failed(submission.op);
                    }
                    WriterOutcome::Refused { .. }
                    | WriterOutcome::Truncated { .. }
                    | WriterOutcome::NotOwner => {
                        self.history.record_write_failed(submission.op);
                    }
                    WriterOutcome::Unavailable { leader } => {
                        self.history.record_write_failed(submission.op);
                        // A leader outside the genesis pool (#189: a joiner
                        // a reconfiguration pulled in) has no client here.
                        target = leader
                            .and_then(|id| nodes.index_of(id))
                            .unwrap_or((target + 1) % server_count);
                    }
                    WriterOutcome::UnknownJournal => {
                        self.history.record_write_failed(submission.op);
                        let journal = writer.journal();
                        assert_always!(
                            false,
                            "chain: a node serves the journal the client names",
                            {
                                "node" => nodes.id_of(target),
                                "tenant" => journal.tenant.0,
                                "journal" => journal.journal.0
                            }
                        );
                    }
                    WriterOutcome::Ambiguous => {
                        self.history.record_write_failed(submission.op);
                        target = (target + 1) % server_count;
                    }
                }
                pending = Some(submission);
                time.sleep(Duration::from_millis(config.retry_backoff_ms))
                    .await
                    .ok();
            }
            if !acknowledged {
                break;
            }
        }
        if let Some(ip) = &held_replica {
            crate::lifecycle::restart(ctx, ip).await;
        }
        // The library client's decisions (#221), by their outcomes — the
        // recovery batch is the writer session's own path.
        assert_sometimes!(
            redirected_written,
            "client: a redirected write is written at the leader"
        );
        assert_sometimes!(
            ambiguity_resolved,
            "client: an ambiguous write is resolved by a read-back"
        );
        assert_sometimes!(
            superseded_stopped,
            "client: a superseded writer stops writing"
        );
        assert_sometimes!(
            dropped_hint_written,
            "client: a write after a dropped leader hint is written"
        );
        let tail = tail(ctx.state());
        {
            let mut guard = tail.lock().unwrap_or_else(PoisonError::into_inner);
            guard.done_proposing += 1;
            if guard.done_proposing == guard.registered && guard.all_quiet_at.is_none() {
                guard.all_quiet_at = Some(time.now());
            }
        }
        // The convergence claim needs every client quiet, so its budget runs
        // from the first moment every client is (#177), and never ends before
        // this client's own recovery deadline. While a sibling is still in its
        // operation program there is no deadline yet: every program is finite
        // (its operations time out, its recovery batch has its own deadline).
        // The threshold, `recovery_budget_ms`, is unchanged; only where it is
        // measured from moved.
        let budget = Duration::from_millis(config.recovery_budget_ms);
        let convergence_deadline = || -> Option<Duration> {
            tail.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .all_quiet_at
                .map(|at| (at + budget).max(recovery_deadline))
        };
        let in_budget = || convergence_deadline().is_none_or(|deadline| time.now() < deadline);

        let mut converged = false;
        // The last probe, `(node, answer)` per live node in node order — the
        // node id travels with its answer from the read through the settle
        // decision to the red-path print, so a parked node dropping out of the
        // live set can never shift the blame onto its neighbour. An answer is
        // one past the node's contiguous chosen prefix (#186: the prefix *is*
        // a node's state; there is no application behind it to compare).
        let mut last_probe: Vec<(usize, Option<u64>)> = Vec::new();
        // `(since, end)`: when the cluster was first seen converged at `end`,
        // reset whenever a probe disagrees.
        let mut stable: Option<(Duration, u64)> = None;
        while in_budget() && !shutdown.is_cancelled() {
            // A node terminally parked by a detected corruption (Stage 7's
            // detect ⇒ crash baseline) never answers again — the availability
            // cost the dead-node budget bounds. Convergence is demanded of
            // every *live* node; the parked set's unavailability is separately
            // asserted as explained (audit + storage gates).
            let parked = crate::world::parked_nodes(ctx.state(), journal);
            // A replica is probed after the acceptors, numbered past them
            // (`server_count + rank`); its disk is never parked.
            let live: Vec<usize> = (0..server_count)
                .filter(|i| !parked.contains(&servers[*i]))
                .chain(server_count..server_count + replica_count)
                .collect();
            let mut observed: Vec<(usize, u64)> = Vec::with_capacity(live.len());
            let mut unanswered = false;
            for &node in &live {
                // `readers` holds the nodes, then the replicas: index `node`.
                let end = readers
                    .inspect(node, journal)
                    .await
                    .map(|reply| reply.chosen_index.map_or(0, |c| c + 1));
                let Some(end) = end else {
                    unanswered = true;
                    break;
                };
                observed.push((node, end));
            }
            last_probe = live
                .iter()
                .map(|&node| {
                    let answer = observed
                        .iter()
                        .find(|(observed_node, _)| *observed_node == node)
                        .map(|(_, end)| *end);
                    (node, answer)
                })
                .collect();
            let all_quiet = {
                let guard = tail.lock().unwrap_or_else(PoisonError::into_inner);
                guard.done_proposing == guard.registered
            };
            match (!unanswered).then(|| observed.first().copied()).flatten() {
                Some((_, reference))
                    if all_quiet
                        && reference > pre_tail_count
                        && observed.iter().all(|(_, end)| *end == reference) =>
                {
                    if !self.external_digests_compared {
                        assert_reachable!(
                            "chain: external replica digests are compared after chaos"
                        );
                        self.external_digests_compared = true;
                    }
                    match stable {
                        Some((since, end)) if end == reference => {
                            if time.now().saturating_sub(since) >= SETTLE {
                                converged = true;
                                break;
                            }
                        }
                        _ => stable = Some((time.now(), reference)),
                    }
                }
                _ => stable = None,
            }
            time.sleep(Duration::from_millis(config.probe_interval_ms))
                .await
                .ok();
        }
        // #188: this journal converged; the run ends only once every journal
        // a client appends to has, so wait (to the same deadline) for the
        // siblings — a client whose journal is quiet keeps the run alive for
        // one still settling.
        if converged {
            let appended_to: BTreeSet<JournalIdentifier> = self
                .plan
                .as_ref()
                .map(|plan| {
                    plan.ids
                        .iter()
                        .take(ctx.client_count().max(1))
                        .copied()
                        .collect()
                })
                .unwrap_or_default();
            tail.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .converged
                .insert(journal);
            while in_budget() && !shutdown.is_cancelled() {
                let all = appended_to.is_subset(
                    &tail
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .converged,
                );
                if all {
                    break;
                }
                time.sleep(Duration::from_millis(config.probe_interval_ms))
                    .await
                    .ok();
            }
        }
        // #188: a journal no client appends to is judged by client 0 at the
        // end (`check`) on an empty history, so client 0 also waits, to the
        // same deadline, for each to converge on its acceptors. A storage
        // fault can quarantine an idle journal on a node until
        // `quarantine_ticks` past the chaos window; the final claim must find
        // it re-opened and caught up, and nothing else keeps the run alive
        // for it (witness 12223320876494641875: an idle journal quarantined at
        // 3.8 s for 80 ticks, the run over at 9 s, red before, green after).
        if converged && client_id == 0 {
            let idle: Vec<JournalIdentifier> = self
                .plan
                .as_ref()
                .map(|plan| plan.ids.iter().skip(ctx.client_count()).copied().collect())
                .unwrap_or_default();
            for idle in idle {
                let mut stable: Option<(Duration, u64)> = None;
                while in_budget() && !shutdown.is_cancelled() {
                    let parked = crate::world::parked_nodes(ctx.state(), idle);
                    let mut ends: Vec<u64> = Vec::with_capacity(server_count);
                    let mut unanswered = false;
                    for node in (0..server_count).filter(|i| !parked.contains(&servers[*i])) {
                        let Some(reply) = readers.inspect(node, idle).await else {
                            unanswered = true;
                            break;
                        };
                        ends.push(reply.chosen_index.map_or(0, |c| c + 1));
                    }
                    let agreed = !unanswered && ends.windows(2).all(|w| w[0] == w[1]);
                    match (agreed, ends.first().copied(), stable) {
                        (true, Some(end), Some((since, held))) if held == end => {
                            if time.now().saturating_sub(since) >= SETTLE {
                                break;
                            }
                        }
                        (true, Some(end), _) => stable = Some((time.now(), end)),
                        _ => stable = None,
                    }
                    time.sleep(Duration::from_millis(config.probe_interval_ms))
                        .await
                        .ok();
                }
            }
        }
        // The converged cluster, read the way a journal client reads it: one
        // last fold from this client's cursor to the tail, so every client's
        // fold meets every other's on the entries they share.
        if converged && let Some(&(node, _)) = last_probe.first() {
            fold.read_to_tail(ctx, &audit, &readers, node, client_id, config.read_limit)
                .await;
            tracing::info!(
                index = fold.state().applied_count,
                state = %hash_text(fold.state().chain_hash),
                "chain_state_read"
            );
        }
        fold.leave(ctx.state(), client_id);

        // Availability oracle (issue #19 E): the budget bounds storage faults a
        // priori, and this independently re-derives — from world state, never
        // from the budget's own bookkeeping — whether an unavailable run is
        // *explainable* by the injected faults (a quorum of clean copies
        // genuinely missing). Under the per-record budget no run is excusable,
        // so an unavailable run with clean quorums everywhere is a real
        // liveness bug, named as such beside the convergence failure.
        // A run a sibling client ended (it saw the cluster converged once every
        // client was quiet) cuts this client's own observation short; the
        // audit's final-convergence claim is the arbiter for that run.
        let ended_by_sibling = !converged && shutdown.is_cancelled();
        let storage = crate::world::storage_fault_stats(ctx.state(), journal);
        // A slot an outage left unrecoverable, or with every clean copy out
        // of reach (#263), is waited on for good: the run's liveness is
        // excused, never its safety.
        let unrecoverable = audit.loss_excuses_liveness();
        assert_always!(
            converged || ended_by_sibling || unrecoverable || !storage.clean_quorum_everywhere,
            "chain: an unavailable run is explained by injected storage faults"
        );
        // Liveness under the budget: faults were injected and the cluster
        // still served and converged (invariant 4 — up to f failures,
        // fail-stop storage faults included, keep the cluster available).
        assert_sometimes!(
            storage.injected > 0 && converged,
            "storage: a run injects storage faults and still converges"
        );
        // The CTRL availability trade, measured: a corruption-parked node
        // stays down (detect ⇒ crash) while the live quorum still converges.
        let corruption = crate::world::corruption_stats(ctx.state(), journal);
        assert_sometimes!(
            corruption.parked > 0 && converged,
            "storage: a corruption-parked node stays down and the cluster converges"
        );
        if !(((recovery_acked > 0 || reader) && converged) || ended_by_sibling || unrecoverable) {
            // Which leg failed: the cluster, or this owner's recovery writes.
            eprintln!(
                "chain run RED: client {client_id} converged={converged} recovery_acked={recovery_acked} reader={reader} owned={:?} next_seq={}",
                writer.owned(),
                writer.next_seq()
            );
            // Failure diagnostic (fires only on the red path): which node is
            // stuck, and where, by real node id (the parked nodes are absent,
            // not renumbered). `None` = the node did not answer the inspect
            // probe inside its timeout. The seed's buggified shape is printed
            // too — a knob at its extreme is one of the things that can
            // produce a red.
            let parked_now = crate::world::parked_nodes(ctx.state(), journal);
            eprintln!(
                "chain convergence FAILED at t={}ms (deadline {:?}ms, pre_tail_count {}): per-node chosen ends = {:?}",
                time.now().as_millis(),
                convergence_deadline().map(|deadline| deadline.as_millis()),
                pre_tail_count,
                last_probe,
            );
            eprintln!("  CONFIG {config:?}");
            eprintln!("  PROBE parked={parked_now:?} servers={server_count}");
            eprintln!("  AUDIT {}", audit.diagnostics());
            let journals = self
                .plan
                .as_ref()
                .map(|plan| plan.ids.clone())
                .unwrap_or_default();
            eprintln!("  JOURNAL {journal} of {journals:?}");
            for other in &journals {
                eprintln!(
                    "  AUDIT[{}] {}",
                    other,
                    crate::audit::audit_world_for(ctx.state(), *other).diagnostics()
                );
            }
        }
        assert_always!(
            ((recovery_acked > 0 || reader) && converged) || ended_by_sibling || unrecoverable,
            "chain: cluster converged after chaos"
        );
        assert_sometimes_greater_than!(
            audit.cluster_applied_max().map_or(0, |slot| slot + 1),
            8_u64,
            "chain: applied index watermark"
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn check(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The two perspectives, and nothing else: the client's own history
        // (linearizability over what it was told), and the audit's fold of
        // every driver transition (safety, restart, and the one liveness claim).
        if let Some(calls) = &self.calls {
            self.history.set_attempts(calls.take());
        }
        // The linearizability search waits for every client of the journal.
        let clients = self.plan.as_ref().map_or(1, |plan| {
            (0..ctx.client_count())
                .filter(|client| plan.for_client(*client) == self.journal)
                .count()
        });
        let mut digest = check_run(ctx.state(), self.journal, &self.history, clients);
        // A journal no client appends to (#188: more journals than clients)
        // is judged by client 0, on an empty history: its audit's safety
        // oracles ran all along, and its final claim holds too.
        if self.client_id == 0
            && let Some(plan) = &self.plan
        {
            for idle in plan.ids.iter().skip(ctx.client_count()) {
                digest ^= check_run(ctx.state(), *idle, &ClientHistory::default(), 0);
            }
        }
        // The control journals' histories (#247): every client's library
        // calls at the registry, at the control journals of the cell the
        // machines formed (#246) and at every tenant control journal a
        // machine folded (#210), searched once (by client 0, after every
        // run) against the journal model.
        if self.client_id == 0 {
            let identifiers = crate::shape::identifiers(ctx.state());
            let system = crate::shape::system_journals(ctx.state()).then_some(identifiers.registry);
            let tenants =
                crate::audit::tenants::lock(&crate::audit::tenants::tenant_board(ctx.state()))
                    .controls();
            let formed = crate::machine::formed_cell(ctx.state());
            let cell = formed
                .map(|cell| [Some(cell.cell), cell.fleet])
                .into_iter()
                .flatten()
                .flatten();
            let take = |journal| {
                std::mem::take(
                    &mut *rpc::control_attempts(ctx.state(), journal)
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner),
                )
            };
            for journal in system.into_iter().chain(cell).chain(tenants) {
                crate::audit::check_control_history(take(journal), paros::WriterMode::Single);
            }
            // The cell's election journal (#240): every candidate's
            // campaigns, renewals, resignations and truncations, unfenced.
            if let Some(election) = formed.and_then(|cell| cell.election) {
                crate::audit::check_control_history(take(election), paros::WriterMode::Multi);
            }
        }
        if let Some(sink) = &self.digest {
            *sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(digest);
        }
        // (Every acked slot being inside the applied prefix is the audit's
        // final claim, judged once over every client's history.)
        Ok(())
    }
}

/// An owner fences every call it makes of its single-writer journal: a
/// wrong-mode refusal (#241) there is a bug.
fn owner_never_of_wrong_mode() {
    assert_always!(
        false,
        "chain: an owner's call is never refused as of the wrong mode"
    );
}

/// Adopt the leader a `Reconfigure` reply named as this client's journal's
/// leader hint — only where that reply speaks for this journal. A
/// `Reconfigure` names no journal: a node answers it from its *plane*
/// journal (`Journals::plane`), which on a matchmaker deployment is the
/// journal whose configuration names the matchmakers (the main one), but on
/// a plain deployment is the node's first live user journal in id order —
/// with system journals, possibly a journal created at runtime and led by a
/// joiner that never serves this one. Adopting that
/// leader sends this journal's next `Write` to a node that answers
/// `UnknownJournal` (seed 10308963497620992383: node 4's plane was a
/// created journal led by joiner 100).
fn adopt_plane_leader(nodes: &ChainClient, has_matchmakers: bool, leader: Option<u64>) {
    if has_matchmakers {
        nodes.observe_leader(leader);
    }
}
