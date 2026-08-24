//! Chain-of-Blocks client workload.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use moonpool_hyper::ReconnectingChannel;
use moonpool_sim::{
    RandomProvider, SimContext, SimulationError, SimulationResult, TimeProvider, TraceQuery,
    Workload, assert_always, assert_sometimes, assert_sometimes_greater_than, buggify_knob,
    buggify_with_prob, swarm_op_enabled,
};
use paros::{
    Command, Compact, Control, InspectRequest, ParosClient, ParosInternalClient, Propose, Slot,
    parse_addr,
};

use crate::CHAOS_DURATION_MS;
use crate::audit::{GateScope, audit_world};
use crate::chain::{ChainState, command_hash, hash_text, user_command_hash};

const PROPOSE: u8 = 0;
const PROPOSE_TO_NON_LEADER: u8 = 1;
const COMPACT: u8 = 2;
const READ_STATE: u8 = 3;
const PAUSE: u8 = 4;
const OP_COUNT: u8 = 5;

const EV_APPLIED: &str = "command_applied";

#[derive(Clone, Copy)]
struct ChainConfig {
    steps: u64,
    command_bytes: usize,
    large_command_bytes: usize,
    request_timeout_ms: u64,
    pause_ms: u64,
    compact_every: u64,
    pipeline_depth: usize,
    recovery_budget_ms: u64,
}

impl ChainConfig {
    fn for_timeline() -> Self {
        Self {
            steps: buggify_knob!(32_u64, 8_u64..65_u64),
            command_bytes: buggify_knob!(64_usize, 1_usize..257_usize),
            large_command_bytes: buggify_knob!(4096_usize, 512_usize..16_385_usize),
            request_timeout_ms: buggify_knob!(1500_u64, 350_u64..3001_u64),
            pause_ms: buggify_knob!(75_u64, 1_u64..501_u64),
            compact_every: buggify_knob!(4_u64, 1_u64..9_u64),
            pipeline_depth: buggify_knob!(8_usize, 4_usize..17_usize),
            recovery_budget_ms: buggify_knob!(60_000_u64, 45_000_u64..90_001_u64),
        }
    }
}

#[derive(Clone, Debug)]
enum Outcome {
    Acked { seq: u64, slot: u64 },
    Rejected { seq: u64 },
    Ambiguous { seq: u64 },
}

impl Outcome {
    fn seq(&self) -> u64 {
        match self {
            Self::Acked { seq, .. } | Self::Rejected { seq } | Self::Ambiguous { seq } => *seq,
        }
    }
}

enum ProposalResult {
    Acked { leader: Option<u64>, slot: u64 },
    Rejected { leader: Option<u64> },
    Ambiguous,
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
#[derive(Default)]
pub(crate) struct ChainWorkload {
    outcomes: BTreeMap<u64, Outcome>,
    submitted: BTreeSet<u64>,
    final_state: Option<ChainState>,
    issued_count: u64,
    safety_only: bool,
}

impl ChainWorkload {
    pub(crate) fn network_safety() -> Self {
        Self {
            safety_only: true,
            ..Self::default()
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

    fn payload(class: u64, ordinary: usize, large: usize, mut seed: u64) -> Vec<u8> {
        let len = match class % 4 {
            0 => 0,
            1 => 1,
            2 => 1 + usize::try_from(seed % u64::try_from(ordinary).unwrap_or(1)).unwrap_or(0),
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

    async fn setup(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The network-swarm safety axis has no quiet recovery tail — provider
        // faults outlive `chaos_duration` in the pinned Moonpool revision — so
        // it must never make the quiescence-gated liveness claim.
        if !self.safety_only {
            audit_world(ctx.state()).enable_liveness_checks();
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = ctx.topology().all_process_ips().to_vec();
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
        let mut public_clients = Vec::with_capacity(endpoints.len());
        let mut internal_clients = Vec::with_capacity(endpoints.len());
        let mut channels = Vec::with_capacity(endpoints.len());
        for (addr, origin) in endpoints {
            let channel =
                ReconnectingChannel::new(ctx.providers(), addr, crate::client_channel_config());
            public_clients.push(ParosClient::with_origin(channel.clone(), origin.clone()));
            internal_clients.push(ParosInternalClient::with_origin(channel.clone(), origin));
            channels.push(channel);
        }
        let _channel_guard = OnDrop::new(move || {
            for channel in channels {
                channel.close();
            }
        });

        let config = ChainConfig::for_timeline();
        let operations = Self::enabled_operations();
        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let server_count = public_clients.len();
        let mut next_seq = 0_u64;
        let mut leader_hint: Option<usize> = None;
        let mut max_acked_slot: Option<u64> = None;
        let mut successful_after_ambiguity = false;
        let mut live_states = BTreeMap::<u64, u64>::new();

        let propose_once = |target: usize, seq: u64, payload: Vec<u8>, abandon: bool| {
            let mut client = public_clients[target].clone();
            let time = time.clone();
            async move {
                let call = client.propose(Propose {
                    client: client_id,
                    seq,
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
            let mut client = public_clients[target].clone();
            let time = time.clone();
            async move {
                moonpool_sim::select! {
                    response = client.compact(Compact { up_to }) => response
                        .ok()
                        .is_some_and(|response| response.into_inner().accepted),
                    _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => false,
                }
            }
        };

        // Start with a small concurrent batch when proposals are enabled. This
        // is honest client pipelining: it lets Phase-2 rounds overlap a driver
        // beat, making the optional re-send decision and a later election gap
        // observable without fabricating or filtering protocol messages.
        if operations.contains(&PROPOSE) {
            let mut primer = Vec::with_capacity(config.pipeline_depth);
            for offset in 0..config.pipeline_depth {
                let seq = next_seq;
                next_seq = next_seq.saturating_add(1);
                let raw = ctx.random().random::<u64>();
                let payload = Self::payload(
                    u64::try_from(offset).unwrap_or(0).saturating_add(1),
                    config.command_bytes,
                    config.large_command_bytes,
                    raw,
                );
                let cmd_hash = user_command_hash(&payload);
                self.submitted.insert(cmd_hash);
                tracing::info!(
                    cmd = %hash_text(cmd_hash),
                    seq,
                    bytes = payload.len() as u64,
                    "chain_command_submitted"
                );
                primer.push((seq, payload, cmd_hash, offset % server_count));
            }
            let results = join_all(primer.iter().map(|(seq, payload, _, target)| {
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
            for ((seq, _, cmd_hash, _), result) in primer.into_iter().zip(results) {
                match result {
                    ProposalResult::Acked { leader, slot } => {
                        leader_hint = leader.and_then(|id| usize::try_from(id).ok());
                        max_acked_slot = Some(max_acked_slot.map_or(slot, |max| max.max(slot)));
                        self.outcomes.insert(cmd_hash, Outcome::Acked { seq, slot });
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
                    }
                    ProposalResult::Rejected { leader } => {
                        leader_hint = leader.and_then(|id| usize::try_from(id).ok());
                        self.outcomes.insert(cmd_hash, Outcome::Rejected { seq });
                        tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_command_rejected");
                    }
                    ProposalResult::Ambiguous => {
                        self.outcomes.insert(cmd_hash, Outcome::Ambiguous { seq });
                        tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_proposal_ambiguous");
                    }
                }
            }
            if let Some(up_to) = max_acked_slot {
                let control = Command::Control(Control::Truncate { up_to: Slot(up_to) });
                tracing::info!(
                    cmd = %hash_text(command_hash(&control)),
                    up_to,
                    "chain_control_submitted"
                );
                if compact_once(leader_hint.unwrap_or(0), up_to).await {
                    tracing::info!(up_to, "chain_compact_accepted");
                }
            }
        }

        let steps = if self.safety_only {
            config.steps.min(16)
        } else {
            config.steps
        };
        for _step in 0..steps {
            if shutdown.is_cancelled() {
                break;
            }

            // Exactly five provider draws per logical step, independent of the
            // swarm mask and payload length.
            let raw_op = ctx.random().random::<u64>();
            let raw_target = ctx.random().random::<u64>();
            let raw_class = ctx.random().random::<u64>();
            let raw_payload = ctx.random().random::<u64>();
            let raw_pause = ctx.random().random::<u64>();
            let op_span = u64::try_from(operations.len()).unwrap_or(1);
            let op = operations[usize::try_from(raw_op % op_span).unwrap_or(0)];
            let target =
                usize::try_from(raw_target % u64::try_from(server_count).unwrap_or(1)).unwrap_or(0);

            match op {
                PROPOSE | PROPOSE_TO_NON_LEADER => {
                    let seq = next_seq;
                    next_seq = next_seq.saturating_add(1);
                    let payload = Self::payload(
                        raw_class,
                        config.command_bytes,
                        config.large_command_bytes,
                        raw_payload,
                    );
                    let cmd_hash = user_command_hash(&payload);
                    self.submitted.insert(cmd_hash);
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
                    } else {
                        leader_hint.unwrap_or(target)
                    };
                    // Honest ambiguity: abandon the client observation, never
                    // falsify a server acknowledgement. The identical identity
                    // is retried below.
                    let abandon = time.now() < Duration::from_millis(CHAOS_DURATION_MS)
                        && buggify_with_prob!(0.15);
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
                                attempt_target = leader
                                    .and_then(|id| usize::try_from(id).ok())
                                    .unwrap_or((attempt_target + 1) % server_count);
                                time.sleep(Duration::from_millis(10)).await.ok();
                            }
                            terminal => break terminal,
                        }
                    };
                    let result = if matches!(result, ProposalResult::Ambiguous) {
                        tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_proposal_ambiguous");
                        self.outcomes.insert(cmd_hash, Outcome::Ambiguous { seq });
                        let retry_target =
                            leader_hint.unwrap_or((chosen_target + 1) % server_count);
                        let reconciled = moonpool_sim::select! {
                            result = propose_once(retry_target, seq, payload, false) => result,
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
                            leader_hint = leader.and_then(|id| usize::try_from(id).ok());
                            max_acked_slot = Some(max_acked_slot.map_or(slot, |max| max.max(slot)));
                            self.outcomes.insert(cmd_hash, Outcome::Acked { seq, slot });
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
                            if seq.is_multiple_of(config.compact_every) {
                                let control =
                                    Command::Control(Control::Truncate { up_to: Slot(slot) });
                                tracing::info!(
                                    cmd = %hash_text(command_hash(&control)),
                                    up_to = slot,
                                    "chain_control_submitted"
                                );
                                if compact_once(leader_hint.unwrap_or(chosen_target), slot).await {
                                    tracing::info!(up_to = slot, "chain_compact_accepted");
                                }
                            }
                        }
                        ProposalResult::Rejected { leader } => {
                            leader_hint = leader.and_then(|id| usize::try_from(id).ok());
                            self.outcomes.insert(cmd_hash, Outcome::Rejected { seq });
                            tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_command_rejected");
                        }
                        ProposalResult::Ambiguous => {
                            self.outcomes.insert(cmd_hash, Outcome::Ambiguous { seq });
                        }
                    }
                }
                COMPACT => {
                    if let Some(up_to) = max_acked_slot
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
                        if accepted {
                            tracing::info!(up_to, "chain_compact_accepted");
                        }
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
                _ => unreachable!("operation IDs are bounded by OP_COUNT"),
            }
        }

        assert_sometimes!(
            successful_after_ambiguity,
            "chain: ambiguous proposal is reconciled as committed"
        );

        if self.safety_only {
            let observation_end =
                Duration::from_millis(CHAOS_DURATION_MS).saturating_add(Duration::from_secs(8));
            if time.now() < observation_end {
                time.sleep(observation_end.saturating_sub(time.now()))
                    .await
                    .ok();
            }
            self.issued_count = next_seq;
            let applied_hashes: BTreeSet<String> = ctx
                .observability()
                .snapshot(EV_APPLIED)
                .iter()
                .filter_map(|event| event.str("cmd").map(str::to_owned))
                .collect();
            for (cmd_hash, outcome) in &self.outcomes {
                if matches!(outcome, Outcome::Acked { .. }) {
                    assert_always!(
                        applied_hashes.contains(&hash_text(*cmd_hash)),
                        "chain: every acknowledged command was applied"
                    );
                }
            }
            return Ok(());
        }

        // Driver perturbations and attrition stop at the cutoff. Provider-level
        // turbulence may continue, so recovery is retry-based rather than a claim
        // of a perfectly fault-free network.
        let cutoff = Duration::from_millis(CHAOS_DURATION_MS);
        if time.now() < cutoff {
            time.sleep(cutoff.checked_sub(time.now()).unwrap())
                .await
                .ok();
        }
        let pre_tail_count = ctx
            .observability()
            .snapshot(EV_APPLIED)
            .iter()
            .filter_map(|event| event.u64("index"))
            .max()
            .unwrap_or(0);

        // A small recovery batch proves post-chaos forward progress and gives
        // the state frontier useful depth even when the swarmed operation mask
        // suppressed proposals during the turbulent prefix.
        let recovery_deadline = time.now() + Duration::from_millis(config.recovery_budget_ms);
        let mut recovery_acked = 0_u64;
        let mut target = leader_hint.unwrap_or(0) % server_count;
        for recovery_offset in 0..12_u64 {
            let seq = next_seq;
            next_seq = next_seq.saturating_add(1);
            let payload = Self::payload(
                2,
                config.command_bytes,
                config.large_command_bytes,
                seq ^ recovery_offset ^ 0xa5a5_5a5a,
            );
            let cmd_hash = user_command_hash(&payload);
            self.submitted.insert(cmd_hash);
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
                        self.outcomes.insert(cmd_hash, Outcome::Acked { seq, slot });
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
                time.sleep(Duration::from_millis(25)).await.ok();
            }
            if !acknowledged {
                break;
            }
        }
        self.issued_count = next_seq;

        let mut converged = false;
        let mut last_probe: Vec<Option<ChainState>> = Vec::new();
        while time.now() < recovery_deadline && !shutdown.is_cancelled() {
            let mut observed = Vec::with_capacity(server_count);
            for client in &internal_clients {
                let mut client = client.clone();
                let state = moonpool_sim::select! {
                    response = client.inspect(InspectRequest {}) => response
                        .ok()
                        .and_then(|response| ChainState::decode(&response.into_inner().snapshot).ok()),
                    _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => None,
                    () = shutdown.cancelled() => None,
                };
                if let Some(state) = state {
                    observed.push(state);
                } else {
                    break;
                }
            }
            last_probe = (0..server_count)
                .map(|i| observed.get(i).copied())
                .collect();
            if observed.len() == server_count
                && observed
                    .first()
                    .is_some_and(|first| first.applied_count > pre_tail_count)
                && observed.windows(2).all(|pair| pair[0] == pair[1])
            {
                self.final_state = observed.first().copied();
                converged = true;
                break;
            }
            time.sleep(Duration::from_millis(50)).await.ok();
        }

        if !converged {
            // Failure diagnostic (fires only on the red path): which node is
            // stuck, and where. `None` = the node did not answer the inspect
            // probe inside its timeout.
            let q = ctx.observability();
            let last_applied: BTreeMap<u64, u64> = q
                .snapshot(EV_APPLIED)
                .iter()
                .filter_map(|e| Some((e.u64("node")?, e.time_ms)))
                .fold(BTreeMap::new(), |mut m, (n, t)| {
                    let entry = m.entry(n).or_insert(0);
                    *entry = (*entry).max(t);
                    m
                });
            let count_by_node = |name: &str| -> BTreeMap<u64, usize> {
                q.snapshot(name).iter().filter_map(|e| e.u64("node")).fold(
                    BTreeMap::new(),
                    |mut m, n| {
                        *m.entry(n).or_insert(0) += 1;
                        m
                    },
                )
            };
            eprintln!(
                "chain convergence FAILED at t={}ms (deadline {}ms, pre_tail_count {}): \
                 per-node states = {:?}; last command_applied per node = {:?}; \
                 boots per node = {:?}; seam crashes per node = {:?}",
                time.now().as_millis(),
                recovery_deadline.as_millis(),
                pre_tail_count,
                last_probe,
                last_applied,
                count_by_node("booted"),
                count_by_node("crashed"),
            );
        }
        assert_always!(
            recovery_acked > 0 && converged,
            "chain: cluster converged after chaos"
        );
        let applied_hashes: BTreeSet<String> = ctx
            .observability()
            .snapshot(EV_APPLIED)
            .iter()
            .filter_map(|event| event.str("cmd").map(str::to_owned))
            .collect();
        for (cmd_hash, outcome) in &self.outcomes {
            if matches!(outcome, Outcome::Acked { .. }) {
                assert_always!(
                    applied_hashes.contains(&hash_text(*cmd_hash)),
                    "chain: every acknowledged command was applied"
                );
            }
        }
        if let Some(state) = self.final_state {
            assert_sometimes_greater_than!(
                state.applied_count,
                8_u64,
                "chain: applied index watermark"
            );
        }
        Ok(())
    }

    async fn check(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let scope = if self.safety_only {
            GateScope::SafetyOnly
        } else {
            GateScope::Full
        };
        audit_world(ctx.state()).check_gates(scope);
        assert_always!(
            self.outcomes
                .values()
                .all(|outcome| outcome.seq() < self.issued_count),
            "chain: retained outcome model is internally valid"
        );
        if !self.safety_only {
            assert_always!(
                self.final_state
                    .is_some_and(|state| self.outcomes.values().all(|outcome| {
                        match outcome {
                            Outcome::Acked { slot, .. } => *slot < state.applied_count,
                            Outcome::Rejected { .. } | Outcome::Ambiguous { .. } => true,
                        }
                    })),
                "chain: retained acknowledged slots are valid"
            );
        }
        Ok(())
    }
}
