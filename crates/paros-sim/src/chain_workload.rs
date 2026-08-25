//! Chain-of-Blocks client workload.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use moonpool_hyper::ReconnectingChannel;
use moonpool_sim::{
    RandomProvider, SimContext, SimulationError, SimulationResult, TimeProvider, TraceQuery,
    Workload, assert_always, assert_reachable, assert_sometimes, assert_sometimes_greater_than,
    buggify_knob, buggify_with_prob, sim::config_random_f64, swarm_op_enabled,
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
const DUP_REPROPOSE: u8 = 5;
const DUAL_SUBMIT: u8 = 6;
const COMPACT_STORM: u8 = 7;
const OP_COUNT: u8 = 8;

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
    compact_storm_attempts: usize,
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
            compact_storm_attempts: buggify_knob!(6_usize, 3_usize..13_usize),
            recovery_budget_ms: buggify_knob!(60_000_u64, 45_000_u64..90_001_u64),
        }
    }
}

#[derive(Clone, Copy)]
enum WeightProfile {
    ReadHeavy,
    WriteHeavy,
    Mixed,
}

impl WeightProfile {
    fn for_timeline() -> Self {
        let draw = config_random_f64();
        if draw < 1.0 / 3.0 {
            Self::ReadHeavy
        } else if draw < 2.0 / 3.0 {
            Self::WriteHeavy
        } else {
            Self::Mixed
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::ReadHeavy => "read-heavy",
            Self::WriteHeavy => "write-heavy",
            Self::Mixed => "mixed",
        }
    }

    fn weight(self, operation: u8) -> u64 {
        let weights = match self {
            // PROPOSE, NON_LEADER, COMPACT, READ, PAUSE, DUP, DUAL, STORM
            Self::ReadHeavy => [10, 5, 4, 52, 12, 5, 5, 7],
            Self::WriteHeavy => [30, 12, 8, 8, 4, 14, 14, 10],
            Self::Mixed => [20, 10, 9, 16, 10, 11, 11, 13],
        };
        weights[usize::from(operation)]
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

enum CompactResult {
    Accepted { leader: Option<u64> },
    Rejected { leader: Option<u64> },
    Ambiguous,
}

#[derive(Clone)]
struct AckedCommand {
    seq: u64,
    payload: Vec<u8>,
    cmd_hash: u64,
    slot: u64,
    node: usize,
}

#[derive(Default)]
struct AdversarialCoverage {
    duplicate_reproposed: bool,
    duplicate_across_leader_change: bool,
    dual_submitted: bool,
    compact_storm_modes: [bool; 3],
    payload_classes: [bool; 4],
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
    external_digests_compared: bool,
    adversarial: AdversarialCoverage,
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

    fn choose_operation(profile: WeightProfile, enabled: &[u8], draw: u64) -> u8 {
        let total = enabled
            .iter()
            .map(|operation| profile.weight(*operation))
            .sum::<u64>();
        let mut ticket = draw % total.max(1);
        for operation in enabled {
            let weight = profile.weight(*operation);
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
        let weight_profile = WeightProfile::for_timeline();
        tracing::info!(profile = weight_profile.name(), "chain_weight_profile");
        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let server_count = public_clients.len();
        let mut next_seq = 0_u64;
        let mut leader_hint: Option<usize> = None;
        let mut stale_leader_hint: Option<usize> = None;
        let mut max_acked_slot: Option<u64> = None;
        let mut successful_after_ambiguity = false;
        let mut live_states = BTreeMap::<u64, u64>::new();
        let mut acked_commands = Vec::<AckedCommand>::new();

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
                let payload_class = offset.saturating_add(1) % 4;
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
                primer.push((seq, payload, payload_class, cmd_hash, offset % server_count));
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
                if matches!(
                    compact_once(leader_hint.unwrap_or(0), up_to).await,
                    CompactResult::Accepted { .. }
                ) {
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
            let op = Self::choose_operation(weight_profile, &operations, raw_op);
            let target =
                usize::try_from(raw_target % u64::try_from(server_count).unwrap_or(1)).unwrap_or(0);

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
                            acked_commands.push(AckedCommand {
                                seq,
                                payload: payload.clone(),
                                cmd_hash,
                                slot,
                                node: leader_hint.unwrap_or(chosen_target),
                            });
                            self.adversarial.payload_classes[payload_class] = true;
                            if seq.is_multiple_of(config.compact_every) {
                                let control =
                                    Command::Control(Control::Truncate { up_to: Slot(slot) });
                                tracing::info!(
                                    cmd = %hash_text(command_hash(&control)),
                                    up_to = slot,
                                    "chain_control_submitted"
                                );
                                if matches!(
                                    compact_once(leader_hint.unwrap_or(chosen_target), slot).await,
                                    CompactResult::Accepted { .. }
                                ) {
                                    tracing::info!(up_to = slot, "chain_compact_accepted");
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
                            self.outcomes.insert(cmd_hash, Outcome::Rejected { seq });
                            tracing::info!(cmd = %hash_text(cmd_hash), seq, "chain_command_rejected");
                        }
                        ProposalResult::Ambiguous => {
                            self.outcomes.insert(cmd_hash, Outcome::Ambiguous { seq });
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
                        let duplicate_target = current_leader;
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
                        self.submitted.insert(cmd_hash);
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
                            self.outcomes.insert(cmd_hash, Outcome::Rejected { seq });
                            tracing::info!(
                                cmd = %hash_text(cmd_hash),
                                seq,
                                "chain_command_rejected"
                            );
                        } else {
                            self.outcomes.insert(cmd_hash, Outcome::Ambiguous { seq });
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
                    if let Some(base) = max_acked_slot {
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
                                    let leader = leader_hint.unwrap_or(0) % server_count;
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
        if !self.safety_only {
            assert_sometimes!(
                matches!(weight_profile, WeightProfile::ReadHeavy),
                "chain: read-heavy operation weights are selected"
            );
            assert_sometimes!(
                matches!(weight_profile, WeightProfile::WriteHeavy),
                "chain: write-heavy operation weights are selected"
            );
            assert_sometimes!(
                matches!(weight_profile, WeightProfile::Mixed),
                "chain: mixed operation weights are selected"
            );
            assert_sometimes!(
                self.adversarial.duplicate_across_leader_change,
                "a duplicate is suppressed across a leader change"
            );
            assert_sometimes!(
                self.adversarial.dual_submitted,
                "chain: concurrent dual-submit is exercised"
            );
            assert_sometimes!(
                self.adversarial.compact_storm_modes[0],
                "chain: compact-storm overask is exercised"
            );
            assert_sometimes!(
                self.adversarial.compact_storm_modes[1],
                "chain: compact-storm follower targeting is exercised"
            );
            assert_sometimes!(
                self.adversarial.compact_storm_modes[2],
                "chain: compact-storm stale-leader targeting is exercised"
            );
            assert_sometimes!(
                self.adversarial.payload_classes[0],
                "chain: an empty payload is acknowledged"
            );
            assert_sometimes!(
                self.adversarial.payload_classes[1],
                "chain: a one-byte payload is acknowledged"
            );
            assert_sometimes!(
                self.adversarial.payload_classes[2],
                "chain: a boundary-sized payload is acknowledged"
            );
            assert_sometimes!(
                self.adversarial.payload_classes[3],
                "chain: a large payload is acknowledged"
            );
        }

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
                    if !applied_hashes.contains(&hash_text(*cmd_hash)) {
                        let q = ctx.observability();
                        eprintln!(
                            "ACK-NOT-APPLIED: outcome={:?} t={}ms; dedup_acks={} crashes={} boots={} applied_events={} nodes={}",
                            outcome,
                            time.now().as_millis(),
                            q.len("propose_dedup_ack"),
                            q.len("crashed"),
                            q.len("booted"),
                            q.len(EV_APPLIED),
                            server_count,
                        );
                    }
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
            // This is deliberately independent of `command_applied`: these are
            // live RPC reads of each application's opaque snapshot. A driver or
            // trace bug cannot manufacture agreement here. Different counts may
            // be ordinary catch-up; equal counts with different digests are an
            // immediate state-machine-safety violation.
            if observed.len() == server_count {
                let reference = observed[0];
                let equal_count = observed
                    .iter()
                    .all(|state| state.applied_count == reference.applied_count);
                if equal_count {
                    if !self.external_digests_compared {
                        assert_reachable!(
                            "chain: external replica digests are compared after chaos"
                        );
                        self.external_digests_compared = true;
                    }
                    for (node, state) in observed.iter().enumerate().skip(1) {
                        assert_always!(
                            state.chain_hash == reference.chain_hash,
                            "chain: live reads agree at equal count",
                            {
                                "node" => node,
                                "applied_count" => state.applied_count,
                                "expected_state" => hash_text(reference.chain_hash),
                                "observed_state" => hash_text(state.chain_hash),
                            }
                        );
                    }
                    if reference.applied_count > pre_tail_count
                        && observed.iter().all(|state| *state == reference)
                    {
                        self.final_state = Some(reference);
                        converged = true;
                        break;
                    }
                }
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
            eprintln!(
                "  ticks={} leader_elected={} prepares_sent={} msg_sent={} msg_received={} check_leader_evts={}",
                q.len("node_tick"),
                q.len("leader_elected"),
                q.snapshot("msg_sent")
                    .iter()
                    .filter(|e| e.str("kind") == Some("prepare"))
                    .count(),
                q.len("msg_sent"),
                q.len("msg_received"),
                q.len("election_timeout_extreme"),
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
