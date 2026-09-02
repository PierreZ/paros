//! Chain-of-Blocks client workload.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use moonpool_hyper::ReconnectingChannel;
use moonpool_sim::{
    RandomProvider, SimContext, SimulationError, SimulationResult, TimeProvider, Workload,
    assert_always, assert_reachable, assert_sometimes, assert_sometimes_greater_than, buggify_knob,
    buggify_with_prob, swarm_op_enabled,
};
use paros::{
    Command, Compact, Control, InspectRequest, ParosClient, ParosInternalClient, Propose, Read,
    Reconfigure, Slot, parse_addr, proposal_checksum,
};

use crate::audit::{ClientHistory, audit_world, check_run};
use crate::chain::{ChainState, command_hash, hash_text, user_command_hash};
use crate::{CHAOS_DURATION_MS, DigestSink};

const PROPOSE: u8 = 0;
const PROPOSE_TO_NON_LEADER: u8 = 1;
const COMPACT: u8 = 2;
const READ_STATE: u8 = 3;
const PAUSE: u8 = 4;
const DUP_REPROPOSE: u8 = 5;
const DUAL_SUBMIT: u8 = 6;
const COMPACT_STORM: u8 = 7;
/// The PUBLIC read-index RPC (the driver's leadership-confirmed linearizable
/// read), as opposed to [`READ_STATE`]'s internal inspect probe.
const READ_INDEX: u8 = 8;
/// **Retired.** Once a client-side stand-in for the leader's matchmaking
/// phase (#119); superseded by the real phase in `paros_core::RawNode`
/// (#120), which a client must not race — a client-minted registration above
/// the leader's round would refuse every campaign. The id stays reserved so
/// the alphabet's ids never shift; the operation is a no-op.
const MATCHMAKE: u8 = 9;
/// **Retired** with [`MATCHMAKE`]: raising the GC watermark from a client is
/// unsafe once leaders depend on the registry (the GC protocol is #123). A
/// no-op that keeps its id.
const MATCH_GC: u8 = 10;
/// A client-requested **online reconfiguration** (#122): read the acceptor
/// set in force from a node, compose a new one — grow onto a spare, shrink,
/// replace one member with a spare, remove the leader itself, or rotate the
/// whole set through the pool — and ask the leader. On a deployment without
/// matchmakers the request is still sent, and must be refused.
const RECONFIGURE: u8 = 11;
const OP_COUNT: u8 = 12;

/// The reconfiguration shapes, by `raw_class` draw (see [`RECONFIGURE`]).
const RECONFIGURE_SHAPES: [&str; 5] = ["grow", "shrink", "replace", "remove-leader", "rotate"];

/// Per-timeline client shape — every field is a `buggify_knob!` (AGENTS.md,
/// prong 2): the default is production's ordinary client, and an activated seed
/// draws one extreme. Each knob documents its floor: the extreme is a valid
/// configuration that keeps the run winnable, never a defeat of it.
///
/// No knob here carries a pairing gate. The location's own firing is the
/// proof, and a per-knob `reachable` would only spend assertion slots.
#[derive(Clone, Copy, Debug)]
struct ChainConfig {
    /// Swarm steps after the primer. Floor 0: the primer and the recovery
    /// batch still commit, so a run whose whole chaos-window history is the
    /// primer (slot 0 alone, at depth 1) is the #56 boundary, not a dead run.
    steps: u64,
    /// Ordinary payload size. Floor 1 byte; ceiling far under the 3 MiB
    /// delivery batch cap.
    command_bytes: usize,
    /// Large payload size. Ceiling 16 KiB, still far under the batch cap.
    large_command_bytes: usize,
    /// Per-request client deadline. Floor 350 ms sits *below* the election
    /// timeout, so every leader change turns into an ambiguous outcome and the
    /// retry/dedup surface saturates; that is a valid client, not a stall.
    request_timeout_ms: u64,
    /// Idle between ops in a `PAUSE` step. Floor 1 ms.
    pause_ms: u64,
    /// One compaction ping every N acked proposals. Floor 1 (every ack).
    compact_every: u64,
    /// Whether this client ever asks for compaction. The off extreme keeps
    /// the chosen prefix uncompacted for the whole run, so catch-up never has
    /// to go through a snapshot — the other half of the recovery surface.
    compaction: bool,
    /// Concurrent proposals in the primer batch. Floor 1: a sequential start.
    pipeline_depth: usize,
    /// Requests per compaction storm. Floor 1.
    compact_storm_attempts: usize,
    /// The recovery tail, an order of magnitude past the 4 s chaos window and
    /// past the longest attrition restart (5 s after swarm rescaling) plus
    /// the below-floor snapshot recovery it forces. **Never below 45 s**.
    recovery_budget_ms: u64,
    /// Proposals in the post-chaos recovery batch. Floor 1: convergence
    /// needs at least one commit past the pre-tail watermark.
    recovery_proposals: u64,
    /// Percent chance a chaos-window proposal abandons its first attempt
    /// mid-flight (honest ambiguity, retried under the same identity).
    /// Ceiling 60: every abandoned attempt is retried, so no rate stalls.
    abandon_pct: u64,
    /// Idle between a redirect and the next attempt. Floor 0 (tight loop
    /// bounded by the request deadline).
    redirect_sleep_ms: u64,
    /// Idle between recovery-batch retries. Floor 0, same bound.
    retry_backoff_ms: u64,
    /// Convergence probe cadence. Floor 10 ms: the probe is one inspect RPC
    /// per live node, and the tail is tens of seconds.
    probe_interval_ms: u64,
    /// Beat between compaction re-asks at the same leader (the #101 coupling
    /// answers the first ask with `accepted: false` while the marker decides).
    /// Floor 10 ms.
    compact_beat_ms: u64,
    /// Compaction re-asks per operation. Floor 1.
    compact_attempts: u8,
    /// Reconfiguration re-asks per operation (following `not_leader`
    /// redirects, or an `unsettled` leader a beat later). Floor 1.
    reconfigure_attempts: u8,
    /// The client channel's connect timeout. Floor 250 ms: one round trip
    /// over the default cross-datacenter link; a shorter one never connects.
    connect_timeout_ms: u64,
    /// The client channel's h2 PING interval. Floor 250 ms (same bound), and
    /// below the shortest request deadline so a half-open connection is
    /// caught within one request.
    keep_alive_interval_ms: u64,
    /// How long a PING may go unanswered. Floor 250 ms: a timeout under the
    /// round trip replaces a healthy stream on every ping.
    keep_alive_timeout_ms: u64,
    /// Per-operation weights of the swarm alphabet, one knob each so a seed
    /// can be storm-heavy and read-starved at once. Floor 0 for any single
    /// weight (the alphabet's total is guarded, and an all-zero draw falls
    /// back to the first enabled op).
    weights: [u64; OP_COUNT as usize],
}

impl ChainConfig {
    fn for_timeline() -> Self {
        Self {
            steps: buggify_knob!(32_u64, 0_u64..65_u64),
            command_bytes: buggify_knob!(64_usize, 1_usize..257_usize),
            large_command_bytes: buggify_knob!(4096_usize, 512_usize..16_385_usize),
            request_timeout_ms: buggify_knob!(1500_u64, 350_u64..3001_u64),
            pause_ms: buggify_knob!(75_u64, 1_u64..501_u64),
            compact_every: buggify_knob!(4_u64, 1_u64..9_u64),
            compaction: buggify_knob!(1_u64, 0_u64..1_u64) == 1,
            pipeline_depth: buggify_knob!(8_usize, 1_usize..17_usize),
            compact_storm_attempts: buggify_knob!(6_usize, 1_usize..13_usize),
            recovery_budget_ms: buggify_knob!(60_000_u64, 45_000_u64..90_001_u64),
            recovery_proposals: buggify_knob!(12_u64, 1_u64..25_u64),
            abandon_pct: buggify_knob!(15_u64, 0_u64..61_u64),
            redirect_sleep_ms: buggify_knob!(10_u64, 0_u64..101_u64),
            retry_backoff_ms: buggify_knob!(25_u64, 0_u64..201_u64),
            probe_interval_ms: buggify_knob!(50_u64, 10_u64..251_u64),
            compact_beat_ms: buggify_knob!(60_u64, 10_u64..301_u64),
            compact_attempts: buggify_knob!(4_u8, 1_u8..9_u8),
            reconfigure_attempts: buggify_knob!(4_u8, 1_u8..9_u8),
            connect_timeout_ms: buggify_knob!(1000_u64, 250_u64..3001_u64),
            keep_alive_interval_ms: buggify_knob!(2000_u64, 250_u64..5001_u64),
            keep_alive_timeout_ms: buggify_knob!(1000_u64, 250_u64..3001_u64),
            // PROPOSE, NON_LEADER, COMPACT, READ, PAUSE, DUP, DUAL, STORM, READ_IDX,
            // MATCHMAKE (retired), MATCH_GC (retired), RECONFIGURE
            weights: [
                buggify_knob!(20_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(9_u64, 0_u64..41_u64),
                buggify_knob!(16_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(11_u64, 0_u64..41_u64),
                buggify_knob!(11_u64, 0_u64..41_u64),
                buggify_knob!(13_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                0,
                0,
                // Each accepted reconfiguration stalls the cluster for one
                // matchmaking round trip plus one Phase 1; a run that draws
                // the ceiling is a cluster that reconfigures more often than
                // it commits, which is still a valid (slow) client.
                buggify_knob!(6_u64, 0_u64..41_u64),
            ],
        }
    }

    fn weight(&self, operation: u8) -> u64 {
        self.weights[usize::from(operation)]
    }
}

/// Where a client sends its next attempt after a redirect, a transport error,
/// or an ambiguous outcome. Drawn per step, so a seed can be a client that
/// always follows the hint, one that stubbornly re-asks the same node (the
/// dedup path on the node that may have committed the abandoned attempt), or
/// one that walks the ring.
#[derive(Clone, Copy, Debug)]
enum Retarget {
    FollowHint,
    SameNode,
    NextNode,
}

impl Retarget {
    /// Two bits of `draw` pick the policy; the hint-following default keeps
    /// half the mass so the ordinary client stays the common shape.
    fn from_draw(draw: u64) -> Self {
        match draw % 4 {
            0 | 1 => Self::FollowHint,
            2 => Self::SameNode,
            _ => Self::NextNode,
        }
    }

    fn next(self, current: usize, hinted: Option<u64>, server_count: usize) -> usize {
        let hint = hinted
            .and_then(|id| usize::try_from(id).ok())
            .filter(|node| *node < server_count);
        match self {
            Self::FollowHint => hint.unwrap_or((current + 1) % server_count),
            Self::SameNode => current,
            Self::NextNode => (current + 1) % server_count,
        }
    }
}

#[derive(Clone, Debug)]
enum Outcome {
    Acked { seq: u64 },
    Rejected { seq: u64 },
    Ambiguous { seq: u64 },
}

impl Outcome {
    fn seq(&self) -> u64 {
        match self {
            Self::Acked { seq } | Self::Rejected { seq } | Self::Ambiguous { seq } => *seq,
        }
    }
}

enum ProposalResult {
    Acked { leader: Option<u64>, slot: u64 },
    Rejected { leader: Option<u64> },
    Ambiguous,
}

enum CompactResult {
    Accepted { leader: Option<u64> },
    Rejected { leader: Option<u64> },
    Ambiguous,
}

/// The terminal outcome of one reconfiguration operation.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ReconfigureResult {
    Started {
        leader: Option<u64>,
        round: u64,
    },
    Refused {
        leader: Option<u64>,
        refusal: String,
    },
    Ambiguous,
}

/// Compose the acceptor set a [`RECONFIGURE`] step asks for, from the set in
/// force (`members`, node ids into a pool of `pool` nodes) and the step's
/// shape draw. `floor` is the smallest configuration the run may put in
/// force (`crate::shape::config_floor`, the size the storage world's copy
/// budget is computed over). `None` when the shape is impossible here (no
/// spare to grow onto, nothing above the floor to shrink); the step is then
/// a no-op.
fn compose_reconfiguration(
    shape: usize,
    members: &[u64],
    pool: u64,
    floor: usize,
    leader: Option<u64>,
    draw: u64,
) -> Option<(&'static str, Vec<u64>)> {
    let mut current: Vec<u64> = members.to_vec();
    current.sort_unstable();
    current.dedup();
    if current.is_empty() || pool == 0 {
        return None;
    }
    let spares: Vec<u64> = (0..pool).filter(|n| !current.contains(n)).collect();
    let pick = |len: usize| usize::try_from(draw % u64::try_from(len).unwrap_or(1)).unwrap_or(0);
    let mut next = current.clone();
    let name = RECONFIGURE_SHAPES[shape % RECONFIGURE_SHAPES.len()];
    match name {
        "grow" => {
            if spares.is_empty() {
                return None;
            }
            next.push(spares[pick(spares.len())]);
        }
        "shrink" => {
            if current.len() <= floor {
                return None;
            }
            next.remove(pick(current.len()));
        }
        "replace" => {
            if spares.is_empty() {
                return None;
            }
            next[pick(current.len())] = spares[pick(spares.len())];
        }
        "remove-leader" => {
            let leader = leader?;
            if current.len() <= floor || !current.contains(&leader) {
                return None;
            }
            next.retain(|n| *n != leader);
        }
        _ => {
            // "rotate": every member steps `shift` ranks through the pool —
            // a mostly or wholly disjoint successor when spares allow it.
            let shift = 1 + draw % pool.max(2).saturating_sub(1);
            next = current.iter().map(|n| (n + shift) % pool).collect();
        }
    }
    next.sort_unstable();
    next.dedup();
    if next == current || next.len() < floor {
        return None;
    }
    Some((name, next))
}

#[derive(Clone)]
struct AckedCommand {
    seq: u64,
    payload: Vec<u8>,
    cmd_hash: u64,
    slot: u64,
    node: usize,
}

const TAIL_KEY: &str = "paros-chain-tail";

/// How long the cluster must stay converged and unchanged before the run is
/// over. One observation of "every live node equal" is not the end of the
/// tail: the leader can still decide a follow-up control command (a `Snap`
/// marker's `Truncate`, a gap fill) a few beats later, and the audit's final
/// claim would then catch the followers one slot behind. A second's worth of
/// ticks covers those follow-ups. **Never buggified**: this is the definition
/// of the tail's end, not a shape the run takes.
const SETTLE: Duration = Duration::from_secs(1);

/// The replicas of one full probe (every live node answered, all at the same
/// applied count) whose digest disagrees with the lowest-id replica's. Each
/// entry keeps its **real node id**: the probe skips parked nodes, so the
/// position in the answer list is not the node, and a diagnostic that used it
/// would blame the wrong replica whenever a lower-id node was parked.
fn divergent_replicas(observed: &[(usize, ChainState)]) -> Vec<(usize, ChainState)> {
    let Some(&(_, reference)) = observed.first() else {
        return Vec::new();
    };
    observed
        .iter()
        .skip(1)
        .filter(|(_, state)| state.chain_hash != reference.chain_hash)
        .copied()
        .collect()
}

/// `node=<id> count=<n> state=<digest>` per replica, for a detail map.
fn describe_replicas(replicas: &[(usize, ChainState)]) -> String {
    replicas
        .iter()
        .map(|(node, state)| {
            format!(
                "node={node} count={} state={}",
                state.applied_count,
                hash_text(state.chain_hash)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The run's shared tail bookkeeping, one per iteration: how many clients the
/// run has and how many have finished proposing. Convergence is only called
/// once *every* client is quiet — the first client to see it ends the run, and
/// its siblings, cut short by that shutdown, defer to the audit's final claim.
#[derive(Default)]
struct Tail {
    registered: usize,
    done_proposing: usize,
}

fn tail(state: &moonpool_sim::StateHandle) -> Arc<Mutex<Tail>> {
    if let Some(tail) = state.get::<Arc<Mutex<Tail>>>(TAIL_KEY) {
        return tail;
    }
    let tail = Arc::new(Mutex::new(Tail::default()));
    state.publish(TAIL_KEY, tail.clone());
    tail
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
    read_index_executed: bool,
    read_index_committed: bool,
    /// One flag per [`RECONFIGURE_SHAPES`] entry: the shape was requested and
    /// the leader started it.
    reconfigure_started: [bool; 5],
    /// A deployment without matchmakers refused a reconfiguration outright.
    reconfigure_refused_plain: bool,
}

struct OnDrop<F: FnOnce()> {
    action: Option<F>,
}

impl<F: FnOnce()> OnDrop<F> {
    fn new(action: F) -> Self {
        Self {
            action: Some(action),
        }
    }
}

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

/// Factory-created stateful test driver. Its model contains outcomes, never a
/// second implementation of Paxos.
///
/// Both models are keyed by the request's own identity — its `seq` (this
/// workload is one client, so `seq` is the `(client, seq)` identity) — never
/// by the payload hash: two distinct requests can legitimately carry
/// identical bytes, and hash-keying would alias their outcomes ("never use
/// hashes as identities"). The payload hash rides along as data, for the
/// applied-trace joins.
pub(crate) struct ChainWorkload {
    /// Per `seq`: the payload's `cmd_hash` and the terminal client outcome.
    outcomes: BTreeMap<u64, (u64, Outcome)>,
    issued_count: u64,
    external_digests_compared: bool,
    adversarial: AdversarialCoverage,
    /// This client's own record of what it asked for and what came back —
    /// the linearizability history checked in `check()`. The client is the
    /// only party that knows its own program order.
    history: ClientHistory,
    /// Where to publish the audit's end-of-run digest (the determinism proof).
    digest: Option<DigestSink>,
}

impl ChainWorkload {
    pub(crate) fn new(digest: Option<DigestSink>) -> Self {
        Self {
            outcomes: BTreeMap::new(),
            issued_count: 0,
            external_digests_compared: false,
            adversarial: AdversarialCoverage::default(),
            history: ClientHistory::default(),
            digest,
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

    fn update_leader_hint(
        current: &mut Option<usize>,
        stale: &mut Option<usize>,
        observed: Option<u64>,
        server_count: usize,
    ) {
        let next = observed
            .and_then(|id| usize::try_from(id).ok())
            .filter(|node| *node < server_count);
        if let (Some(previous), Some(next)) = (*current, next)
            && previous != next
        {
            *stale = Some(previous);
        }
        *current = next;
    }

    fn payload(class: u64, ordinary: usize, large: usize, mut seed: u64) -> Vec<u8> {
        let len = match class % 4 {
            0 => 0,
            1 => 1,
            2 => ordinary,
            _ => large,
        };
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            // Local xorshift expands one provider draw without making the
            // explorer's RNG-call count depend on payload size.
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            bytes.push(seed.to_le_bytes()[0]);
        }
        bytes
    }
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

        let endpoints = servers
            .iter()
            .map(|ip| {
                let addr = parse_addr(ip)?;
                let origin = http::Uri::try_from(format!("http://{addr}"))
                    .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
                Ok((addr, origin))
            })
            .collect::<SimulationResult<Vec<_>>>()?;
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
        let channel_config = crate::client_channel_config(
            Duration::from_millis(config.connect_timeout_ms),
            Duration::from_millis(config.keep_alive_interval_ms),
            Duration::from_millis(config.keep_alive_timeout_ms),
        );
        let mut public_clients = Vec::with_capacity(endpoints.len());
        let mut internal_clients = Vec::with_capacity(endpoints.len());
        let mut channels = Vec::with_capacity(endpoints.len());
        for (addr, origin) in endpoints {
            let channel = ReconnectingChannel::new(ctx.providers(), addr, channel_config.clone());
            public_clients.push(ParosClient::with_origin(channel.clone(), origin.clone()));
            internal_clients.push(ParosInternalClient::with_origin(channel.clone(), origin));
            channels.push(channel);
        }
        let _channel_guard = OnDrop::new(move || {
            for channel in channels {
                channel.close();
            }
        });

        let operations = Self::enabled_operations();
        tracing::info!(?config, "chain_config");
        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        self.history.set_client(client_id);
        let audit = audit_world(ctx.state());
        let now_ms = {
            let time = time.clone();
            move || u64::try_from(time.now().as_millis()).unwrap_or(u64::MAX)
        };
        let server_count = public_clients.len();
        let mut next_seq = 0_u64;
        let mut leader_hint: Option<usize> = None;
        let mut stale_leader_hint: Option<usize> = None;
        let mut max_acked_slot: Option<u64> = None;
        let mut successful_after_ambiguity = false;
        let mut live_states = BTreeMap::<u64, u64>::new();
        let mut acked_commands = Vec::<AckedCommand>::new();
        // The highest read-index watermark this client has observed committed
        // (`None` is the empty applied prefix, ordered below `Some(0)`). This
        // client runs one operation at a time, so a later committed read
        // starts after an earlier one completed: linearizability demands its
        // watermark never move backwards.
        let mut last_read_frontier: Option<u64> = None;

        let propose_once = |target: usize, seq: u64, payload: Vec<u8>, abandon: bool| {
            let mut client = public_clients[target].clone();
            let time = time.clone();
            async move {
                let call = client.propose(Propose {
                    client: client_id,
                    seq,
                    checksum: proposal_checksum(client_id, seq, &payload),
                    command: payload,
                });
                if abandon {
                    moonpool_sim::select! {
                        response = call => match response {
                            Ok(response) => {
                                let ack = response.into_inner();
                                assert_always!(ack.seq == seq, "chain: proposal ack echoes request");
                                if ack.committed {
                                    ProposalResult::Acked {
                                        leader: ack.leader,
                                        slot: ack.slot.unwrap_or_default(),
                                    }
                                } else {
                                    ProposalResult::Rejected { leader: ack.leader }
                                }
                            }
                            Err(_) => ProposalResult::Ambiguous,
                        },
                        _ = time.sleep(Duration::from_millis(10)) => ProposalResult::Ambiguous,
                    }
                } else {
                    match call.await {
                        Ok(response) => {
                            let ack = response.into_inner();
                            assert_always!(ack.seq == seq, "chain: proposal ack echoes request");
                            if ack.committed {
                                ProposalResult::Acked {
                                    leader: ack.leader,
                                    slot: ack.slot.unwrap_or_default(),
                                }
                            } else {
                                ProposalResult::Rejected { leader: ack.leader }
                            }
                        }
                        Err(_) => ProposalResult::Ambiguous,
                    }
                }
            }
        };
        let compact_once = |target: usize, up_to: u64| {
            let clients = public_clients.clone();
            let time = time.clone();
            async move {
                let mut client = clients[target].clone();
                // The #101 coupling makes compaction a two-phase dance: the
                // first ask usually seeds the `Snap` marker and answers
                // `accepted: false`; once a quorum advertises the decided
                // point, a retry gets the `Truncate` proposed. A few
                // beat-spaced retries at the same leader complete the dance
                // within one workload operation, keeping truncation pressure
                // (and everything downstream of raised floors) at its
                // pre-coupling cadence.
                let mut attempt_target = target;
                for _attempt in 0..config.compact_attempts {
                    let outcome = moonpool_sim::select! {
                        response = client.compact(Compact { up_to }) => match response {
                            Ok(response) => {
                                let ack = response.into_inner();
                                if ack.accepted {
                                    CompactResult::Accepted { leader: ack.leader }
                                } else {
                                    CompactResult::Rejected { leader: ack.leader }
                                }
                            }
                            Err(_) => CompactResult::Ambiguous,
                        },
                        _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => CompactResult::Ambiguous,
                    };
                    match outcome {
                        CompactResult::Rejected { leader: Some(next) }
                            if usize::try_from(next).is_ok_and(|next| next == attempt_target) =>
                        {
                            // Same leader, not yet coupled: give the marker a
                            // beat to decide and the custody acks to land.
                            if time
                                .sleep(Duration::from_millis(config.compact_beat_ms))
                                .await
                                .is_err()
                            {
                                return outcome;
                            }
                        }
                        CompactResult::Rejected { leader: Some(next) } => {
                            let Ok(next) = usize::try_from(next) else {
                                return outcome;
                            };
                            attempt_target = next;
                            client = clients[attempt_target % clients.len()].clone();
                        }
                        terminal => return terminal,
                    }
                }
                CompactResult::Ambiguous
            }
        };
        let reconfigure_once = |target: usize, members: Vec<u64>| {
            let clients = public_clients.clone();
            let time = time.clone();
            async move {
                let mut attempt_target = target % clients.len();
                let mut client = clients[attempt_target].clone();
                for _attempt in 0..config.reconfigure_attempts {
                    let request = Reconfigure {
                        members: members.clone(),
                    };
                    let outcome = moonpool_sim::select! {
                        response = client.reconfigure(request) => match response {
                            Ok(response) => {
                                let ack = response.into_inner();
                                if ack.accepted {
                                    ReconfigureResult::Started { leader: ack.leader, round: ack.round.unwrap_or(0) }
                                } else {
                                    ReconfigureResult::Refused { leader: ack.leader, refusal: ack.refusal }
                                }
                            }
                            Err(_) => ReconfigureResult::Ambiguous,
                        },
                        _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ReconfigureResult::Ambiguous,
                    };
                    match &outcome {
                        // A redirect: follow the hint. Every other refusal is
                        // terminal for this operation — `unsettled` included,
                        // after one beat at the same leader.
                        ReconfigureResult::Refused {
                            leader: Some(next),
                            refusal,
                        } if refusal == "not_leader" => {
                            let Ok(next) = usize::try_from(*next) else {
                                return outcome;
                            };
                            attempt_target = next % clients.len();
                            client = clients[attempt_target].clone();
                        }
                        ReconfigureResult::Refused { refusal, .. } if refusal == "unsettled" => {
                            if time
                                .sleep(Duration::from_millis(config.compact_beat_ms))
                                .await
                                .is_err()
                            {
                                return outcome;
                            }
                        }
                        _ => return outcome,
                    }
                }
                ReconfigureResult::Ambiguous
            }
        };

        // Start with a small concurrent batch when proposals are enabled. This
        // is honest client pipelining: it lets Phase-2 rounds overlap a driver
        // beat, making the optional re-send decision and a later election gap
        // observable without fabricating or filtering protocol messages.
        if operations.contains(&PROPOSE) {
            let mut primer = Vec::with_capacity(config.pipeline_depth);
            for _ in 0..config.pipeline_depth {
                let seq = next_seq;
                next_seq = next_seq.saturating_add(1);
                // One draw per primer entry shapes its payload class, its
                // bytes, and its first target — every combination is a valid
                // client.
                let raw = ctx.random().random::<u64>();
                let payload_class = usize::try_from(raw % 4).unwrap_or(0);
                let primer_target =
                    usize::try_from((raw >> 2) % u64::try_from(server_count).unwrap_or(1))
                        .unwrap_or(0);
                let payload = Self::payload(
                    raw % 4,
                    config.command_bytes,
                    config.large_command_bytes,
                    raw,
                );
                let cmd_hash = user_command_hash(&payload);
                audit.note_submitted(cmd_hash);
                self.history.record_write_issued(seq, now_ms());
                tracing::info!(
                    cmd = %hash_text(cmd_hash),
                    seq,
                    bytes = payload.len() as u64,
                    "chain_command_submitted"
                );
                primer.push((seq, payload, payload_class, cmd_hash, primer_target));
            }
            let results = join_all(primer.iter().map(|(seq, payload, _, _, target)| {
                let attempt = propose_once(*target, *seq, payload.clone(), false);
                let time = time.clone();
                async move {
                    moonpool_sim::select! {
                        result = attempt => result,
                        _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ProposalResult::Ambiguous,
                    }
                }
            }))
            .await;
            for ((seq, payload, payload_class, cmd_hash, target), result) in
                primer.into_iter().zip(results)
            {
                match result {
                    ProposalResult::Acked { leader, slot } => {
                        Self::update_leader_hint(
                            &mut leader_hint,
                            &mut stale_leader_hint,
                            leader,
                            server_count,
                        );
                        max_acked_slot = Some(max_acked_slot.map_or(slot, |max| max.max(slot)));
                        self.outcomes
                            .insert(seq, (cmd_hash, Outcome::Acked { seq }));
                        self.history.record_write_ack(seq, Some(slot), now_ms());
                        tracing::info!(
                            cmd = %hash_text(cmd_hash),
                            seq,
                            slot,
                            "chain_command_acked"
                        );
                        if let Some(node) = leader {
                            tracing::info!(
                                client_id,
                                seq_id = seq,
                                slot,
                                node,
                                "client_acknowledged"
                            );
                        }
                        acked_commands.push(AckedCommand {
                            seq,
                            payload,
                            cmd_hash,
                            slot,
                            node: leader_hint.unwrap_or(target),
                        });
                        self.adversarial.payload_classes[payload_class] = true;
                    }
                    ProposalResult::Rejected { leader } => {
                        Self::update_leader_hint(
                            &mut leader_hint,
                            &mut stale_leader_hint,
                            leader,
                            server_count,
                        );
                        self.outcomes
                            .insert(seq, (cmd_hash, Outcome::Rejected { seq }));
                        tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_command_rejected");
                    }
                    ProposalResult::Ambiguous => {
                        self.outcomes
                            .insert(seq, (cmd_hash, Outcome::Ambiguous { seq }));
                        tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_proposal_ambiguous");
                    }
                }
            }
            if let Some(up_to) = max_acked_slot.filter(|_| config.compaction) {
                let control = Command::Control(Control::Truncate { up_to: Slot(up_to) });
                tracing::info!(
                    cmd = %hash_text(command_hash(&control)),
                    up_to,
                    "chain_control_submitted"
                );
                let fallback =
                    usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
                if matches!(
                    compact_once(leader_hint.unwrap_or(fallback), up_to).await,
                    CompactResult::Accepted { .. }
                ) {
                    tracing::info!(up_to, "chain_compact_accepted");
                }
            }
        }

        for _step in 0..config.steps {
            if shutdown.is_cancelled() {
                break;
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
            let op = Self::choose_operation(&config, &operations, raw_op);
            let target =
                usize::try_from(raw_target % u64::try_from(server_count).unwrap_or(1)).unwrap_or(0);
            let retarget = Retarget::from_draw(raw_policy);
            // One step in eight ignores the leader hint outright: a proposal
            // to whoever `target` is, which after a turnover is the *old*
            // leader — the stale-hint edge `PROPOSE_TO_NON_LEADER` reaches only
            // deliberately.
            let ignore_hint = (raw_policy >> 2) % 8 == 0;

            match op {
                PROPOSE | PROPOSE_TO_NON_LEADER => {
                    let seq = next_seq;
                    next_seq = next_seq.saturating_add(1);
                    let payload_class = usize::try_from(raw_class % 4).unwrap_or(0);
                    let payload = Self::payload(
                        raw_class,
                        config.command_bytes,
                        config.large_command_bytes,
                        raw_payload,
                    );
                    let cmd_hash = user_command_hash(&payload);
                    audit.note_submitted(cmd_hash);
                    self.history.record_write_issued(seq, now_ms());
                    tracing::info!(
                        cmd = %hash_text(cmd_hash),
                        seq,
                        bytes = payload.len() as u64,
                        "chain_command_submitted"
                    );

                    let chosen_target = if op == PROPOSE_TO_NON_LEADER {
                        leader_hint.map_or(target, |leader| {
                            if server_count > 1 {
                                (leader + 1 + target % (server_count - 1)) % server_count
                            } else {
                                leader
                            }
                        })
                    } else if ignore_hint {
                        target
                    } else {
                        leader_hint.unwrap_or(target)
                    };
                    // Honest ambiguity: abandon the client observation, never
                    // falsify a server acknowledgement. The identical identity
                    // is retried below.
                    #[allow(clippy::cast_precision_loss)]
                    let abandon = time.now() < Duration::from_millis(CHAOS_DURATION_MS)
                        && buggify_with_prob!(config.abandon_pct as f64 / 100.0);
                    if abandon {
                        // BUGGIFY pairing: the deliberate mid-flight
                        // abandonment (the honest-ambiguity generator) fires.
                        assert_reachable!("chain: a client abandons an in-flight observation");
                    }
                    let proposal_deadline =
                        time.now() + Duration::from_millis(config.request_timeout_ms);
                    let mut attempt_target = chosen_target;
                    let mut first_attempt = true;
                    let result = loop {
                        let remaining = proposal_deadline.saturating_sub(time.now());
                        if remaining.is_zero() {
                            break ProposalResult::Ambiguous;
                        }
                        let attempt = moonpool_sim::select! {
                            result = propose_once(
                                attempt_target,
                                seq,
                                payload.clone(),
                                abandon && first_attempt,
                            ) => result,
                            _ = time.sleep(remaining) => ProposalResult::Ambiguous,
                            () = shutdown.cancelled() => ProposalResult::Ambiguous,
                        };
                        first_attempt = false;
                        match attempt {
                            ProposalResult::Rejected { leader }
                                if op == PROPOSE && time.now() < proposal_deadline =>
                            {
                                attempt_target =
                                    retarget.next(attempt_target, leader, server_count);
                                time.sleep(Duration::from_millis(config.redirect_sleep_ms))
                                    .await
                                    .ok();
                            }
                            terminal => break terminal,
                        }
                    };
                    let result = if matches!(result, ProposalResult::Ambiguous) {
                        tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_proposal_ambiguous");
                        self.outcomes
                            .insert(seq, (cmd_hash, Outcome::Ambiguous { seq }));
                        // The reconciling retry: by policy, back to the node
                        // that may have committed the abandoned attempt (the
                        // dedup path on the committing node), or on to the
                        // hinted leader / the next node.
                        let retry_target = retarget.next(
                            chosen_target,
                            leader_hint.and_then(|node| u64::try_from(node).ok()),
                            server_count,
                        );
                        let reconciled = moonpool_sim::select! {
                            result = propose_once(retry_target, seq, payload.clone(), false) => result,
                            _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ProposalResult::Ambiguous,
                            () = shutdown.cancelled() => ProposalResult::Ambiguous,
                        };
                        if matches!(reconciled, ProposalResult::Acked { .. }) {
                            successful_after_ambiguity = true;
                        }
                        reconciled
                    } else {
                        result
                    };

                    match result {
                        ProposalResult::Acked { leader, slot } => {
                            Self::update_leader_hint(
                                &mut leader_hint,
                                &mut stale_leader_hint,
                                leader,
                                server_count,
                            );
                            max_acked_slot = Some(max_acked_slot.map_or(slot, |max| max.max(slot)));
                            self.outcomes
                                .insert(seq, (cmd_hash, Outcome::Acked { seq }));
                            self.history.record_write_ack(seq, Some(slot), now_ms());
                            tracing::info!(
                                cmd = %hash_text(cmd_hash),
                                seq,
                                slot,
                                "chain_command_acked"
                            );
                            if let Some(node) = leader {
                                tracing::info!(
                                    client_id,
                                    seq_id = seq,
                                    slot,
                                    node,
                                    "client_acknowledged"
                                );
                            }
                            acked_commands.push(AckedCommand {
                                seq,
                                payload: payload.clone(),
                                cmd_hash,
                                slot,
                                node: leader_hint.unwrap_or(chosen_target),
                            });
                            self.adversarial.payload_classes[payload_class] = true;
                            if config.compaction && seq.is_multiple_of(config.compact_every) {
                                // How far to ask: the just-acked slot, a
                                // partial prefix below it, or one past it (a
                                // refusal is a legal answer to any of them).
                                let up_to = match (raw_policy >> 5) % 4 {
                                    0 => slot.saturating_sub((raw_policy >> 7) % (slot + 1)),
                                    1 => slot + 1 + (raw_policy >> 7) % 8,
                                    _ => slot,
                                };
                                let control =
                                    Command::Control(Control::Truncate { up_to: Slot(up_to) });
                                tracing::info!(
                                    cmd = %hash_text(command_hash(&control)),
                                    up_to,
                                    "chain_control_submitted"
                                );
                                if matches!(
                                    compact_once(leader_hint.unwrap_or(chosen_target), up_to).await,
                                    CompactResult::Accepted { .. }
                                ) {
                                    tracing::info!(up_to, "chain_compact_accepted");
                                }
                            }
                        }
                        ProposalResult::Rejected { leader } => {
                            Self::update_leader_hint(
                                &mut leader_hint,
                                &mut stale_leader_hint,
                                leader,
                                server_count,
                            );
                            self.outcomes
                                .insert(seq, (cmd_hash, Outcome::Rejected { seq }));
                            self.history.record_write_failed(seq);
                            tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_command_rejected");
                        }
                        ProposalResult::Ambiguous => {
                            self.outcomes
                                .insert(seq, (cmd_hash, Outcome::Ambiguous { seq }));
                            self.history.record_write_failed(seq);
                        }
                    }
                }
                DUP_REPROPOSE => {
                    if let Some(current_leader) = leader_hint {
                        let candidates = acked_commands
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
                        // Where the duplicate goes: the current leader (the
                        // dedup fast path), the node that originally acked it
                        // (dedup at a possibly demoted node), or anyone.
                        let duplicate_target = match (raw_policy >> 3) % 4 {
                            0 | 1 => current_leader,
                            2 => command.node % server_count,
                            _ => target,
                        };
                        tracing::info!(
                            cmd = %hash_text(command.cmd_hash),
                            seq = command.seq,
                            original_slot = command.slot,
                            original_node = command.node,
                            target = duplicate_target,
                            "chain_duplicate_reproposed"
                        );
                        if !self.adversarial.duplicate_reproposed {
                            assert_reachable!("chain: duplicate reproposal executes");
                            self.adversarial.duplicate_reproposed = true;
                        }
                        let result = moonpool_sim::select! {
                            result = propose_once(
                                duplicate_target,
                                command.seq,
                                command.payload,
                                false,
                            ) => result,
                            _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ProposalResult::Ambiguous,
                            () = shutdown.cancelled() => ProposalResult::Ambiguous,
                        };
                        match result {
                            ProposalResult::Acked { leader, slot } => {
                                self.history
                                    .record_write_ack(command.seq, Some(slot), now_ms());
                                assert_always!(
                                    slot == command.slot,
                                    "chain: duplicate committed ack preserves its slot",
                                    {
                                        "original_slot" => command.slot,
                                        "observed_slot" => slot,
                                        "target" => duplicate_target,
                                    }
                                );
                                if !self.adversarial.duplicate_across_leader_change {
                                    assert_reachable!(
                                        "chain: duplicate suppression observed after leader change"
                                    );
                                    self.adversarial.duplicate_across_leader_change = true;
                                }
                                Self::update_leader_hint(
                                    &mut leader_hint,
                                    &mut stale_leader_hint,
                                    leader,
                                    server_count,
                                );
                            }
                            ProposalResult::Rejected { leader } => {
                                Self::update_leader_hint(
                                    &mut leader_hint,
                                    &mut stale_leader_hint,
                                    leader,
                                    server_count,
                                );
                            }
                            ProposalResult::Ambiguous => {}
                        }
                    }
                }
                DUAL_SUBMIT => {
                    if server_count > 1 && time.now() < Duration::from_millis(CHAOS_DURATION_MS) {
                        let seq = next_seq;
                        next_seq = next_seq.saturating_add(1);
                        let payload_class = usize::try_from(raw_class % 4).unwrap_or(0);
                        let payload = Self::payload(
                            raw_class,
                            config.command_bytes,
                            config.large_command_bytes,
                            raw_payload,
                        );
                        let cmd_hash = user_command_hash(&payload);
                        audit.note_submitted(cmd_hash);
                        self.history.record_write_issued(seq, now_ms());
                        tracing::info!(
                            cmd = %hash_text(cmd_hash),
                            seq,
                            bytes = payload.len() as u64,
                            "chain_command_submitted"
                        );
                        let second_target = (target
                            + 1
                            + usize::try_from(
                                raw_pause % u64::try_from(server_count - 1).unwrap_or(1),
                            )
                            .unwrap_or(0))
                            % server_count;
                        tracing::info!(
                            cmd = %hash_text(cmd_hash),
                            seq,
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
                            let attempt = propose_once(*target, seq, payload.clone(), false);
                            let time = time.clone();
                            let shutdown = shutdown.clone();
                            async move {
                                moonpool_sim::select! {
                                    result = attempt => result,
                                    _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ProposalResult::Ambiguous,
                                    () = shutdown.cancelled() => ProposalResult::Ambiguous,
                                }
                            }
                        });
                        let results = join_all(attempts).await;
                        let mut committed: Option<(u64, Option<u64>, usize)> = None;
                        let mut rejected = 0_usize;
                        let mut redirect = None;
                        for (attempt_target, result) in targets.into_iter().zip(results) {
                            match result {
                                ProposalResult::Acked { leader, slot } => {
                                    if let Some((original_slot, _, _)) = committed {
                                        assert_always!(
                                            slot == original_slot,
                                            "chain: dual-submit committed slots agree",
                                            {
                                                "original_slot" => original_slot,
                                                "observed_slot" => slot,
                                                "target" => attempt_target,
                                            }
                                        );
                                    } else {
                                        committed = Some((slot, leader, attempt_target));
                                    }
                                }
                                ProposalResult::Rejected { leader } => {
                                    rejected += 1;
                                    redirect = redirect.or(leader);
                                }
                                ProposalResult::Ambiguous => {}
                            }
                        }

                        if let Some((slot, leader, ack_target)) = committed {
                            Self::update_leader_hint(
                                &mut leader_hint,
                                &mut stale_leader_hint,
                                leader,
                                server_count,
                            );
                            max_acked_slot =
                                Some(max_acked_slot.map_or(slot, |maximum| maximum.max(slot)));
                            self.outcomes
                                .insert(seq, (cmd_hash, Outcome::Acked { seq }));
                            self.history.record_write_ack(seq, Some(slot), now_ms());
                            tracing::info!(
                                cmd = %hash_text(cmd_hash),
                                seq,
                                slot,
                                "chain_command_acked"
                            );
                            if let Some(node) = leader {
                                tracing::info!(
                                    client_id,
                                    seq_id = seq,
                                    slot,
                                    node,
                                    "client_acknowledged"
                                );
                            }
                            acked_commands.push(AckedCommand {
                                seq,
                                payload,
                                cmd_hash,
                                slot,
                                node: leader_hint.unwrap_or(ack_target),
                            });
                            self.adversarial.payload_classes[payload_class] = true;
                        } else if rejected == targets.len() {
                            Self::update_leader_hint(
                                &mut leader_hint,
                                &mut stale_leader_hint,
                                redirect,
                                server_count,
                            );
                            self.outcomes
                                .insert(seq, (cmd_hash, Outcome::Rejected { seq }));
                            tracing::info!(
                                cmd = %hash_text(cmd_hash),
                                seq,
                                "chain_command_rejected"
                            );
                        } else {
                            self.outcomes
                                .insert(seq, (cmd_hash, Outcome::Ambiguous { seq }));
                            tracing::info!(
                                cmd = %hash_text(cmd_hash),
                                seq,
                                "chain_proposal_ambiguous"
                            );
                        }
                    }
                }
                COMPACT => {
                    if let Some(up_to) = max_acked_slot
                        && config.compaction
                        && raw_pause % config.compact_every == 0
                    {
                        let control = Command::Control(Control::Truncate { up_to: Slot(up_to) });
                        let cmd_hash = command_hash(&control);
                        tracing::info!(
                            cmd = %hash_text(cmd_hash),
                            up_to,
                            "chain_control_submitted"
                        );
                        let target = leader_hint.unwrap_or(target);
                        let accepted = compact_once(target, up_to).await;
                        if matches!(accepted, CompactResult::Accepted { .. }) {
                            tracing::info!(up_to, "chain_compact_accepted");
                        }
                    }
                }
                COMPACT_STORM => {
                    if let Some(base) = max_acked_slot.filter(|_| config.compaction) {
                        let first_mode = usize::try_from(raw_pause % 3).unwrap_or(0);
                        for attempt in 0..config.compact_storm_attempts {
                            let mode = (first_mode + attempt) % 3;
                            let (mode_name, up_to, request_target) = match mode {
                                0 => (
                                    "overask",
                                    base.saturating_add(10_000 + raw_payload % 10_000),
                                    leader_hint.unwrap_or(target),
                                ),
                                1 if server_count > 1 && leader_hint.is_some() => {
                                    let leader = leader_hint.unwrap_or(target) % server_count;
                                    let offset = 1 + usize::try_from(
                                        (raw_target + u64::try_from(attempt).unwrap_or(0))
                                            % u64::try_from(server_count - 1).unwrap_or(1),
                                    )
                                    .unwrap_or(0);
                                    ("follower", base, (leader + offset) % server_count)
                                }
                                2 if stale_leader_hint.is_some()
                                    && stale_leader_hint != leader_hint =>
                                {
                                    ("stale-leader", base, stale_leader_hint.unwrap_or(target))
                                }
                                _ => continue,
                            };
                            let control =
                                Command::Control(Control::Truncate { up_to: Slot(up_to) });
                            let cmd_hash = command_hash(&control);
                            tracing::info!(
                                cmd = %hash_text(cmd_hash),
                                up_to,
                                "chain_control_submitted"
                            );
                            tracing::info!(
                                cmd = %hash_text(cmd_hash),
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

                            let first = compact_once(request_target, up_to).await;
                            let result = match first {
                                CompactResult::Rejected {
                                    leader: Some(redirect),
                                } if usize::try_from(redirect).ok().is_some_and(|node| {
                                    node < server_count && node != request_target
                                }) =>
                                {
                                    let redirect =
                                        usize::try_from(redirect).unwrap_or(request_target);
                                    compact_once(redirect, up_to).await
                                }
                                terminal => terminal,
                            };
                            match result {
                                CompactResult::Accepted { leader } => {
                                    Self::update_leader_hint(
                                        &mut leader_hint,
                                        &mut stale_leader_hint,
                                        leader,
                                        server_count,
                                    );
                                    tracing::info!(up_to, "chain_compact_accepted");
                                }
                                CompactResult::Rejected { leader } => {
                                    Self::update_leader_hint(
                                        &mut leader_hint,
                                        &mut stale_leader_hint,
                                        leader,
                                        server_count,
                                    );
                                }
                                CompactResult::Ambiguous => {}
                            }
                        }
                    }
                }
                READ_INDEX => {
                    // The public linearizable read: the driver captures the
                    // leader's applied watermark, confirms leadership with a
                    // heartbeat-ack quorum round, and only then answers. A
                    // timeout is Ambiguous — nothing is recorded or assumed.
                    let seq = next_seq;
                    next_seq = next_seq.saturating_add(1);
                    if !self.adversarial.read_index_executed {
                        assert_reachable!("chain: read-index operation executes");
                        self.adversarial.read_index_executed = true;
                    }
                    self.history.record_read_issued(seq, now_ms());
                    let read_deadline =
                        time.now() + Duration::from_millis(config.request_timeout_ms);
                    let mut attempt_target = leader_hint.unwrap_or(target) % server_count;
                    let mut attempts: u64 = 0;
                    let outcome = loop {
                        let remaining = read_deadline.saturating_sub(time.now());
                        if remaining.is_zero() || shutdown.is_cancelled() {
                            break None;
                        }
                        attempts += 1;
                        let mut client = public_clients[attempt_target].clone();
                        let attempt = moonpool_sim::select! {
                            response = client.read(Read { client: client_id, seq }) =>
                                response.ok().map(tonic::Response::into_inner),
                            _ = time.sleep(remaining) => None,
                            () = shutdown.cancelled() => None,
                        };
                        match attempt {
                            Some(ack) => {
                                assert_always!(
                                    ack.seq == seq,
                                    "chain: read-index ack echoes request"
                                );
                                if ack.committed {
                                    break Some(ack.read_index);
                                }
                                // Redirect, by this step's policy, inside
                                // the same deadline.
                                attempt_target =
                                    retarget.next(attempt_target, ack.leader, server_count);
                            }
                            // Transport error: same policy, no hint.
                            None => {
                                attempt_target = retarget.next(attempt_target, None, server_count);
                            }
                        }
                        if time
                            .sleep(Duration::from_millis(config.redirect_sleep_ms))
                            .await
                            .is_err()
                        {
                            break None;
                        }
                    };
                    if let Some(watermark) = outcome {
                        self.history
                            .record_read_ack(seq, watermark, attempts, now_ms());
                        tracing::info!(
                            client_id,
                            seq_id = seq,
                            read_index = watermark
                                .map_or(-1_i64, |wm| { i64::try_from(wm).unwrap_or(i64::MAX) }),
                            "chain_read_index_acked"
                        );
                        // Per-client monotonicity: this client's committed
                        // reads never observe a shrinking applied frontier.
                        assert_always!(
                            watermark >= last_read_frontier,
                            "chain: a client's read-index watermarks never move backwards",
                            {
                                "previous" => last_read_frontier
                                    .map_or(-1_i64, |wm| i64::try_from(wm).unwrap_or(i64::MAX)),
                                "observed" => watermark
                                    .map_or(-1_i64, |wm| i64::try_from(wm).unwrap_or(i64::MAX)),
                            }
                        );
                        last_read_frontier = last_read_frontier.max(watermark);
                        // Read-your-writes: every write this client saw acked
                        // completed before this read began, so the confirmed
                        // frontier must cover the highest acked slot.
                        if let Some(acked) = max_acked_slot {
                            assert_always!(
                                watermark.is_some_and(|wm| wm >= acked),
                                "chain: a read-index ack covers the client's acked writes",
                                {
                                    "max_acked_slot" => acked,
                                    "observed" => watermark
                                        .map_or(-1_i64, |wm| i64::try_from(wm).unwrap_or(i64::MAX)),
                                }
                            );
                        }
                        self.adversarial.read_index_committed = true;
                    } else {
                        // Ambiguous per convention: a timed-out read carries
                        // no constraint and is never assumed to have missed.
                        self.history.record_read_failed(seq);
                        tracing::info!(client_id, seq_id = seq, "chain_read_index_ambiguous");
                    }
                }
                READ_STATE => {
                    let mut client = internal_clients[target].clone();
                    if let Some(state) = moonpool_sim::select! {
                        response = client.inspect(InspectRequest {}) => response
                            .ok()
                            .and_then(|response| ChainState::decode(&response.into_inner().snapshot).ok()),
                        _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => None,
                        () = shutdown.cancelled() => None,
                    } {
                        let prior = live_states.insert(state.applied_count, state.chain_hash);
                        assert_always!(
                            prior.is_none_or(|hash| hash == state.chain_hash),
                            "chain: live reads agree at equal count"
                        );
                        tracing::info!(
                            index = state.applied_count,
                            state = %hash_text(state.chain_hash),
                            "chain_state_read"
                        );
                    }
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
                MATCHMAKE | MATCH_GC => {}
                RECONFIGURE => {
                    // Read the configuration in force from the hinted leader
                    // (or the step's target): every node learns it from the
                    // ballot's `Prepare`, so a stale answer only makes the
                    // request refused (`unchanged`, `unknown_member`) — an
                    // operating condition, never a wrong state.
                    let probe_target = leader_hint.unwrap_or(target);
                    let mut probe = internal_clients[probe_target].clone();
                    let members = moonpool_sim::select! {
                        response = probe.inspect(InspectRequest {}) => response
                            .ok()
                            .map(|response| response.into_inner().members),
                        _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => None,
                        () = shutdown.cancelled() => None,
                    };
                    let shape = usize::try_from(raw_class % 5).unwrap_or(0);
                    let leader_id = leader_hint.and_then(|l| u64::try_from(l).ok());
                    let pool = u64::try_from(server_count).unwrap_or(0);
                    if let Some((name, next)) = members.as_deref().and_then(|members| {
                        compose_reconfiguration(
                            shape,
                            members,
                            pool,
                            config_floor,
                            leader_id,
                            raw_payload,
                        )
                    }) {
                        tracing::info!(shape = name, members = ?next, "chain_reconfigure_request");
                        let outcome = reconfigure_once(probe_target, next).await;
                        tracing::info!(shape = name, outcome = ?outcome, "chain_reconfigure_outcome");
                        match outcome {
                            ReconfigureResult::Started { leader, .. } => {
                                // The AGENTS.md rule, client-visible: a
                                // deployment without matchmakers never honors
                                // a reconfiguration.
                                assert_always!(
                                    has_matchmakers,
                                    "reconfiguration: a deployment without matchmakers never accepts a reconfiguration",
                                    { "shape" => name }
                                );
                                self.adversarial.reconfigure_started[shape] = true;
                                Self::update_leader_hint(
                                    &mut leader_hint,
                                    &mut stale_leader_hint,
                                    leader,
                                    server_count,
                                );
                            }
                            ReconfigureResult::Refused { leader, refusal } => {
                                if refusal == "no_matchmakers" {
                                    assert_always!(
                                        !has_matchmakers,
                                        "reconfiguration: only a deployment without matchmakers refuses for lack of them",
                                        { "shape" => name }
                                    );
                                    self.adversarial.reconfigure_refused_plain = true;
                                }
                                Self::update_leader_hint(
                                    &mut leader_hint,
                                    &mut stale_leader_hint,
                                    leader,
                                    server_count,
                                );
                            }
                            ReconfigureResult::Ambiguous => {}
                        }
                    }
                }
                _ => unreachable!("operation IDs are bounded by OP_COUNT"),
            }
        }

        assert_sometimes!(
            successful_after_ambiguity,
            "chain: ambiguous proposal is reconciled as committed"
        );
        assert_sometimes!(
            self.adversarial.duplicate_across_leader_change,
            "a duplicate is suppressed across a leader change"
        );
        if self.adversarial.dual_submitted {
            assert_reachable!("chain: concurrent dual-submit is exercised");
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
        if self.adversarial.compact_storm_modes[0] {
            assert_reachable!("chain: compact-storm overask is exercised");
        }
        if self.adversarial.compact_storm_modes[1] {
            assert_reachable!("chain: compact-storm follower targeting is exercised");
        }
        if self.adversarial.compact_storm_modes[2] {
            assert_reachable!("chain: compact-storm stale-leader targeting is exercised");
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
        assert_sometimes!(
            self.adversarial.read_index_committed,
            "chain: a committed read-index observes the applied frontier"
        );

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
        // The applied count the tail must move past (the audit tracks the
        // applied *slot*; the count is one past it).
        let pre_tail_count = audit.cluster_applied_max().map_or(0, |slot| slot + 1);

        // A small recovery batch proves post-chaos forward progress and gives
        // the state frontier useful depth even when the swarmed operation mask
        // suppressed proposals during the turbulent prefix.
        let recovery_deadline = time.now() + Duration::from_millis(config.recovery_budget_ms);
        let mut recovery_acked = 0_u64;
        let first = usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
        let mut target = leader_hint.unwrap_or(first) % server_count;
        for _ in 0..config.recovery_proposals {
            let seq = next_seq;
            next_seq = next_seq.saturating_add(1);
            let raw = ctx.random().random::<u64>();
            let payload = Self::payload(
                raw % 4,
                config.command_bytes,
                config.large_command_bytes,
                raw,
            );
            let cmd_hash = user_command_hash(&payload);
            audit.note_submitted(cmd_hash);
            self.history.record_write_issued(seq, now_ms());
            tracing::info!(
                cmd = %hash_text(cmd_hash),
                seq,
                bytes = payload.len() as u64,
                "chain_command_submitted"
            );
            let mut acknowledged = false;
            while time.now() < recovery_deadline && !shutdown.is_cancelled() {
                let result = moonpool_sim::select! {
                    result = propose_once(target, seq, payload.clone(), false) => result,
                    _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ProposalResult::Ambiguous,
                    () = shutdown.cancelled() => ProposalResult::Ambiguous,
                };
                match result {
                    ProposalResult::Acked { leader, slot } => {
                        recovery_acked = recovery_acked.saturating_add(1);
                        acknowledged = true;
                        self.outcomes
                            .insert(seq, (cmd_hash, Outcome::Acked { seq }));
                        self.history.record_write_ack(seq, Some(slot), now_ms());
                        tracing::info!(
                            cmd = %hash_text(cmd_hash),
                            seq,
                            slot,
                            "chain_command_acked"
                        );
                        if let Some(node) = leader {
                            tracing::info!(
                                client_id,
                                seq_id = seq,
                                slot,
                                node,
                                "client_acknowledged"
                            );
                            target = usize::try_from(node).unwrap_or(target) % server_count;
                        }
                        break;
                    }
                    ProposalResult::Rejected { leader } => {
                        target = leader
                            .and_then(|id| usize::try_from(id).ok())
                            .unwrap_or((target + 1) % server_count);
                    }
                    ProposalResult::Ambiguous => target = (target + 1) % server_count,
                }
                time.sleep(Duration::from_millis(config.retry_backoff_ms))
                    .await
                    .ok();
            }
            if !acknowledged {
                break;
            }
        }
        self.issued_count = next_seq;
        let tail = tail(ctx.state());
        tail.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .done_proposing += 1;

        let mut converged = false;
        // The last probe, `(node, answer)` per live node in node order — the
        // node id travels with its state from the read through the settle
        // decision to the red-path print, so a parked node dropping out of the
        // live set can never shift the blame onto its neighbour.
        let mut last_probe: Vec<(usize, Option<ChainState>)> = Vec::new();
        // `(since, state)`: when the cluster was first seen converged at
        // `state`, reset whenever a probe disagrees.
        let mut stable: Option<(Duration, ChainState)> = None;
        while time.now() < recovery_deadline && !shutdown.is_cancelled() {
            // A node terminally parked by a detected corruption (Stage 7's
            // detect ⇒ crash baseline) never answers again — the availability
            // cost the dead-node budget bounds. Convergence is demanded of
            // every *live* node; the parked set's unavailability is separately
            // asserted as explained (audit + storage gates).
            let parked = crate::world::parked_nodes(ctx.state());
            let live: Vec<usize> = (0..server_count)
                .filter(|i| !parked.contains(&servers[*i]))
                .collect();
            let mut observed: Vec<(usize, ChainState)> = Vec::with_capacity(live.len());
            let mut unanswered = false;
            for &node in &live {
                let mut client = internal_clients[node].clone();
                let state = moonpool_sim::select! {
                    response = client.inspect(InspectRequest {}) => response
                        .ok()
                        .and_then(|response| ChainState::decode(&response.into_inner().snapshot).ok()),
                    _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => None,
                    () = shutdown.cancelled() => None,
                };
                let Some(state) = state else {
                    unanswered = true;
                    break;
                };
                observed.push((node, state));
            }
            last_probe = live
                .iter()
                .map(|&node| {
                    let answer = observed
                        .iter()
                        .find(|(observed_node, _)| *observed_node == node)
                        .map(|(_, state)| *state);
                    (node, answer)
                })
                .collect();
            // This is deliberately independent of `command_applied`: these are
            // live RPC reads of each application's opaque snapshot. A driver or
            // trace bug cannot manufacture agreement here. Different counts may
            // be ordinary catch-up; equal counts with different digests are an
            // immediate state-machine-safety violation.
            if let Some((reference_node, reference)) =
                (!unanswered).then(|| observed.first().copied()).flatten()
            {
                let equal_count = observed
                    .iter()
                    .all(|(_, state)| state.applied_count == reference.applied_count);
                if equal_count {
                    if !self.external_digests_compared {
                        assert_reachable!(
                            "chain: external replica digests are compared after chaos"
                        );
                        self.external_digests_compared = true;
                    }
                    let divergent = divergent_replicas(&observed);
                    assert_always!(
                        divergent.is_empty(),
                        "chain: live reads agree at equal count",
                        {
                            "reference_node" => reference_node,
                            "applied_count" => reference.applied_count,
                            "expected_state" => hash_text(reference.chain_hash),
                            "divergent" => describe_replicas(&divergent),
                        }
                    );
                    let all_quiet = {
                        let guard = tail.lock().unwrap_or_else(PoisonError::into_inner);
                        guard.done_proposing == guard.registered
                    };
                    if all_quiet
                        && reference.applied_count > pre_tail_count
                        && observed.iter().all(|(_, state)| *state == reference)
                    {
                        match stable {
                            Some((since, state)) if state == reference => {
                                if time.now().saturating_sub(since) >= SETTLE {
                                    converged = true;
                                    break;
                                }
                            }
                            _ => stable = Some((time.now(), reference)),
                        }
                    } else {
                        stable = None;
                    }
                } else {
                    stable = None;
                }
            } else {
                stable = None;
            }
            time.sleep(Duration::from_millis(config.probe_interval_ms))
                .await
                .ok();
        }

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
        let storage = crate::world::storage_fault_stats(ctx.state());
        assert_always!(
            converged || ended_by_sibling || !storage.clean_quorum_everywhere,
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
        let corruption = crate::world::corruption_stats(ctx.state());
        assert_sometimes!(
            corruption.parked > 0 && converged,
            "storage: a corruption-parked node stays down and the cluster converges"
        );
        if !converged && !ended_by_sibling {
            // Failure diagnostic (fires only on the red path): which node is
            // stuck, and where, by real node id (the parked nodes are absent,
            // not renumbered). `None` = the node did not answer the inspect
            // probe inside its timeout. The seed's buggified shape is printed
            // too — a knob at its extreme is one of the things that can
            // produce a red.
            let parked_now = crate::world::parked_nodes(ctx.state());
            eprintln!(
                "chain convergence FAILED at t={}ms (deadline {}ms, pre_tail_count {}): per-node states = {:?}",
                time.now().as_millis(),
                recovery_deadline.as_millis(),
                pre_tail_count,
                last_probe,
            );
            eprintln!("  CONFIG {config:?}");
            eprintln!("  PROBE parked={parked_now:?} servers={server_count}");
            eprintln!("  AUDIT {}", audit.diagnostics());
            for ip in &servers {
                if let Some(probe) = crate::world::corpus_disk_probe(ctx.state(), ip) {
                    eprintln!(
                        "  DISK {ip}: floor={} applied={} snap_point={:?} faulty_chunks={:?} clean_slots={}..={}",
                        probe.floor,
                        probe.applied_count,
                        probe.snap_point,
                        probe.faulty_chunks,
                        probe.clean_slots.first().copied().unwrap_or(0),
                        probe.clean_slots.last().copied().unwrap_or(0),
                    );
                }
            }
        }
        assert_always!(
            (recovery_acked > 0 && converged) || ended_by_sibling,
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
        let digest = check_run(ctx.state(), &self.history);
        if let Some(sink) = &self.digest {
            *sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(digest);
        }
        // (Every acked slot being inside the applied prefix is the audit's
        // final claim, judged once over every client's history.)
        assert_always!(
            self.outcomes
                .iter()
                .all(|(seq, (_, outcome))| *seq == outcome.seq() && *seq < self.issued_count),
            "chain: retained outcome model is internally valid"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(applied_count: u64, chain_hash: u64) -> ChainState {
        ChainState {
            applied_count,
            chain_hash,
            ..ChainState::default()
        }
    }

    /// The shape composer, pinned at the mechanism: each shape moves the set
    /// the way its name says, never below the floor, never onto a node outside
    /// the pool, and never to the set already in force.
    #[test]
    fn reconfiguration_shapes_respect_the_floor_and_the_pool() {
        let members = [1_u64, 2, 3];
        let grow = compose_reconfiguration(0, &members, 5, 3, Some(1), 7).unwrap();
        assert_eq!(grow.0, "grow");
        assert_eq!(grow.1.len(), 4);
        assert!(grow.1.iter().all(|n| *n < 5));
        assert!(
            compose_reconfiguration(0, &[0, 1, 2], 3, 3, None, 0).is_none(),
            "no spare"
        );
        assert!(
            compose_reconfiguration(1, &members, 5, 3, None, 0).is_none(),
            "at the floor"
        );
        let shrink = compose_reconfiguration(1, &[0, 1, 2, 3], 5, 3, None, 2).unwrap();
        assert_eq!((shrink.0, shrink.1.len()), ("shrink", 3));
        let replace = compose_reconfiguration(2, &members, 5, 3, None, 1).unwrap();
        assert_eq!(replace.0, "replace");
        assert_eq!(replace.1.len(), 3);
        assert_ne!(replace.1, members.to_vec());
        assert!(
            compose_reconfiguration(3, &members, 5, 3, Some(1), 0).is_none(),
            "removing the leader at the floor is refused"
        );
        let removed = compose_reconfiguration(3, &[0, 1, 2, 3], 5, 3, Some(2), 0).unwrap();
        assert_eq!(
            (removed.0, removed.1.clone()),
            ("remove-leader", vec![0, 1, 3])
        );
        assert!(
            compose_reconfiguration(3, &[0, 1, 2, 3], 5, 3, None, 0).is_none(),
            "no leader known"
        );
        let rotate = compose_reconfiguration(4, &[0, 1, 2], 6, 3, None, 2).unwrap();
        assert_eq!((rotate.0, rotate.1.clone()), ("rotate", vec![3, 4, 5]));
        assert!(
            compose_reconfiguration(4, &[0, 1, 2], 3, 3, None, 0).is_none(),
            "a rotation through a pool with no spare is the same set"
        );
    }

    /// The identity regression: node 1 is parked and absent from the probe,
    /// node 3 diverges. The report must name node 3 — the positional answer
    /// (index 2 of the live list) would have blamed node 2, which agrees.
    #[test]
    fn a_divergent_replica_is_named_by_its_node_id_not_its_position() {
        let observed = vec![
            (0, state(7, 0xaa)),
            (2, state(7, 0xaa)),
            (3, state(7, 0xbb)),
        ];
        let divergent = divergent_replicas(&observed);
        assert_eq!(divergent.len(), 1);
        assert_eq!(divergent[0].0, 3);
        assert_eq!(divergent[0].1.chain_hash, 0xbb);
        assert!(describe_replicas(&divergent).starts_with("node=3 count=7 "));
    }

    #[test]
    fn agreeing_replicas_report_no_divergence() {
        let observed = vec![(0, state(3, 0x11)), (2, state(3, 0x11))];
        assert!(divergent_replicas(&observed).is_empty());
        assert!(divergent_replicas(&[]).is_empty());
    }
}
