//! Dedicated lifecycle choreography for below-floor snapshot recovery.
//!
//! This workload deliberately owns only the client-side sequence. Process
//! lifecycle remains Moonpool's built-in attrition surface: the workload
//! observes the resulting `sim_fault` events and drives the two survivors while
//! the selected victim is down.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use moonpool_hyper::ReconnectingChannel;
use moonpool_sim::{
    SIM_FAULT_EVENT_NAME, SimContext, SimulationError, SimulationResult, TimeProvider, TraceEvent,
    TraceQuery, Workload, assert_always, assert_reachable, assert_sometimes,
};
use paros::{
    Command, Compact, Control, InspectRequest, ParosClient, ParosInternalClient, Propose, Slot,
    parse_addr,
};

use crate::chain::{ChainState, command_hash, hash_text, user_command_hash};

const CLUSTER_SIZE: usize = 3;
const PRIME_PROPOSALS: u64 = 3;
const SURVIVOR_PROPOSALS: u64 = 12;
const RPC_TIMEOUT: Duration = Duration::from_secs(1);
const PRIME_BUDGET: Duration = Duration::from_secs(15);
const SURVIVOR_BUDGET: Duration = Duration::from_secs(20);
const RECOVERY_BUDGET: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

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

/// One factory-created client that forces a real kill → truncate → restart
/// sequence on a fixed three-node cluster.
#[derive(Default)]
pub(crate) struct SnapshotRecoveryWorkload {
    baseline: Option<ChainState>,
    next_seq: u64,
    completed: bool,
}

impl SnapshotRecoveryWorkload {
    fn invalid(message: impl Into<String>) -> SimulationError {
        SimulationError::InvalidState(message.into())
    }

    fn servers(ctx: &SimContext) -> SimulationResult<Vec<String>> {
        let mut servers = ctx.topology().all_process_ips().to_vec();
        servers.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
        servers.dedup();
        let valid = servers.len() == CLUSTER_SIZE;
        assert_always!(
            valid,
            "chain choreography: lifecycle axis has exactly three nodes",
            { "nodes" => servers.len() }
        );
        if !valid {
            return Err(Self::invalid(format!(
                "snapshot choreography expected {CLUSTER_SIZE} nodes, got {}",
                servers.len()
            )));
        }
        Ok(servers)
    }

    fn payload(seq: u64) -> Vec<u8> {
        let mut seed = seq ^ 0x9e37_79b9_7f4a_7c15;
        let mut payload = Vec::with_capacity(64);
        for _ in 0..64 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            payload.push(seed.to_le_bytes()[0]);
        }
        payload
    }

    async fn wait_event<F>(
        ctx: &SimContext,
        name: &str,
        deadline: Duration,
        predicate: F,
    ) -> SimulationResult<TraceEvent>
    where
        F: Fn(&TraceEvent) -> bool,
    {
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            if let Some(event) = ctx
                .observability()
                .snapshot(name)
                .into_iter()
                .find(&predicate)
            {
                return Ok(event);
            }
            ctx.time()
                .sleep(POLL_INTERVAL)
                .await
                .map_err(|error| Self::invalid(format!("event wait failed: {error}")))?;
        }
        assert_always!(
            false,
            "chain choreography: every required lifecycle event arrives",
            { "event" => name }
        );
        Err(Self::invalid(format!(
            "timed out waiting for choreography event {name}"
        )))
    }

    fn victim_prefix(ctx: &SimContext, victim: u64, before_seq: u64) -> Option<u64> {
        ctx.observability()
            .snapshot("command_applied")
            .into_iter()
            .filter(|event| event.seq < before_seq && event.u64("node") == Some(victim))
            .filter_map(|event| event.u64("index"))
            .max()
    }
}

#[async_trait]
impl Workload for SnapshotRecoveryWorkload {
    fn name(&self) -> &'static str {
        "snapshot-recovery-client"
    }

    #[allow(clippy::too_many_lines)]
    async fn setup(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = Self::servers(ctx)?;
        let endpoints = servers
            .iter()
            .map(|ip| {
                let addr = parse_addr(ip)?;
                let origin = http::Uri::try_from(format!("http://{addr}"))
                    .map_err(|error| Self::invalid(format!("bad gRPC origin: {error}")))?;
                Ok((addr, origin))
            })
            .collect::<SimulationResult<Vec<_>>>()?;
        let mut public_clients = Vec::with_capacity(CLUSTER_SIZE);
        let mut internal_clients = Vec::with_capacity(CLUSTER_SIZE);
        let mut channels = Vec::with_capacity(CLUSTER_SIZE * 2);
        for (addr, origin) in endpoints {
            let public_channel = ReconnectingChannel::new(
                ctx.providers(),
                addr.clone(),
                crate::client_channel_config(),
            );
            let internal_channel =
                ReconnectingChannel::new(ctx.providers(), addr, crate::client_channel_config());
            public_clients.push(ParosClient::with_origin(
                public_channel.clone(),
                origin.clone(),
            ));
            internal_clients.push(ParosInternalClient::with_origin(
                internal_channel.clone(),
                origin,
            ));
            channels.push(public_channel);
            channels.push(internal_channel);
        }
        let _channel_guard = OnDrop::new(move || {
            for channel in channels {
                channel.close();
            }
        });

        let time = ctx.time().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let deadline = time.now() + PRIME_BUDGET;
        let mut target = 0_usize;
        while self.next_seq < PRIME_PROPOSALS {
            let seq = self.next_seq;
            let payload = Self::payload(seq);
            let cmd_hash = user_command_hash(&payload);
            tracing::info!(
                cmd = %hash_text(cmd_hash),
                seq,
                bytes = payload.len() as u64,
                "chain_command_submitted"
            );
            let mut acknowledged = None;
            while time.now() < deadline {
                let mut client = public_clients[target].clone();
                let response = moonpool_sim::select! {
                    response = client.propose(Propose {
                        client: client_id,
                        seq,
                        command: payload.clone(),
                    }) => response.ok().map(tonic::Response::into_inner),
                    _ = time.sleep(RPC_TIMEOUT) => None,
                };
                if let Some(ack) = response {
                    assert_always!(
                        ack.seq == seq,
                        "chain choreography: proposal ack echoes request",
                        { "expected_seq" => seq, "observed_seq" => ack.seq }
                    );
                    if ack.committed {
                        acknowledged = ack.slot;
                        if let Some(leader) = ack.leader.and_then(|node| usize::try_from(node).ok())
                        {
                            target = leader % CLUSTER_SIZE;
                        }
                        break;
                    }
                    target = ack
                        .leader
                        .and_then(|node| usize::try_from(node).ok())
                        .map_or((target + 1) % CLUSTER_SIZE, |leader| leader % CLUSTER_SIZE);
                } else {
                    target = (target + 1) % CLUSTER_SIZE;
                }
            }
            let Some(slot) = acknowledged else {
                assert_always!(
                    false,
                    "chain choreography: setup primes a nonzero application state",
                    { "acked" => self.next_seq, "required" => PRIME_PROPOSALS }
                );
                return Err(Self::invalid("setup could not prime the chain"));
            };
            tracing::info!(cmd = %hash_text(cmd_hash), seq, slot, "chain_command_acked");
            self.next_seq = self.next_seq.saturating_add(1);
        }

        let mut last_states = Vec::new();
        while time.now() < deadline {
            let mut states = Vec::with_capacity(CLUSTER_SIZE);
            for client in &internal_clients {
                let mut client = client.clone();
                let state = moonpool_sim::select! {
                    response = client.inspect(InspectRequest {}) => response
                        .ok()
                        .and_then(|response| ChainState::decode(&response.into_inner().snapshot).ok()),
                    _ = time.sleep(RPC_TIMEOUT) => None,
                };
                let Some(state) = state else {
                    break;
                };
                states.push(state);
            }
            last_states = states.clone();
            if states.len() == CLUSTER_SIZE
                && states[0].applied_count >= PRIME_PROPOSALS
                && states.iter().all(|state| *state == states[0])
            {
                self.baseline = Some(states[0]);
                assert_reachable!("chain choreography: setup reaches an exact nonzero baseline");
                return Ok(());
            }
            time.sleep(POLL_INTERVAL).await.map_err(|error| {
                Self::invalid(format!("setup convergence wait failed: {error}"))
            })?;
        }

        assert_always!(
            false,
            "chain choreography: setup replicas converge exactly before attrition",
            { "observed_nodes" => last_states.len() }
        );
        Err(Self::invalid("setup replicas did not converge exactly"))
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = Self::servers(ctx)?;
        let baseline = self
            .baseline
            .ok_or_else(|| Self::invalid("snapshot choreography has no setup baseline"))?;
        let time = ctx.time().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);

        let fault_deadline = time.now() + Duration::from_secs(6);
        let graceful = Self::wait_event(ctx, SIM_FAULT_EVENT_NAME, fault_deadline, |event| {
            event.str("kind") == Some("process_graceful_shutdown")
        })
        .await?;
        let victim_ip = graceful
            .str("ip")
            .ok_or_else(|| Self::invalid("graceful-shutdown event has no victim IP"))?
            .to_owned();
        let victim = servers
            .iter()
            .position(|ip| ip == &victim_ip)
            .ok_or_else(|| Self::invalid(format!("unknown attrition victim {victim_ip}")))?;
        let victim_node = u64::try_from(victim).unwrap_or(u64::MAX);
        let force_kill = Self::wait_event(ctx, SIM_FAULT_EVENT_NAME, fault_deadline, |event| {
            event.seq > graceful.seq
                && event.str("kind") == Some("process_force_kill")
                && event.str("ip") == Some(victim_ip.as_str())
        })
        .await?;
        let lifecycle_ordered = graceful.seq < force_kill.seq;
        assert_always!(
            lifecycle_ordered,
            "chain choreography: graceful shutdown precedes force kill",
            { "graceful_seq" => graceful.seq, "kill_seq" => force_kill.seq, "victim" => victim_node }
        );
        if !lifecycle_ordered {
            return Err(Self::invalid("force kill preceded graceful shutdown"));
        }

        let expected_prefix = baseline
            .applied_slot()
            .ok_or_else(|| Self::invalid("setup baseline is empty"))?
            .0;
        let victim_prefix =
            Self::victim_prefix(ctx, victim_node, force_kill.seq).unwrap_or(expected_prefix);
        let primed_before_kill = victim_prefix >= expected_prefix;
        assert_always!(
            primed_before_kill,
            "chain choreography: victim has the primed prefix at force kill",
            { "victim" => victim_node, "expected_prefix" => expected_prefix, "victim_prefix" => victim_prefix }
        );
        if !primed_before_kill {
            return Err(Self::invalid("victim lost the primed baseline before kill"));
        }

        let survivors: Vec<usize> = (0..CLUSTER_SIZE).filter(|node| *node != victim).collect();
        let endpoints = servers
            .iter()
            .map(|ip| {
                let addr = parse_addr(ip)?;
                let origin = http::Uri::try_from(format!("http://{addr}"))
                    .map_err(|error| Self::invalid(format!("bad gRPC origin: {error}")))?;
                Ok((addr, origin))
            })
            .collect::<SimulationResult<Vec<_>>>()?;
        let mut public_clients = Vec::with_capacity(CLUSTER_SIZE);
        let mut internal_clients = Vec::with_capacity(CLUSTER_SIZE);
        let mut channels = Vec::with_capacity(CLUSTER_SIZE * 2);
        for (addr, origin) in endpoints {
            let public_channel = ReconnectingChannel::new(
                ctx.providers(),
                addr.clone(),
                crate::client_channel_config(),
            );
            let internal_channel =
                ReconnectingChannel::new(ctx.providers(), addr, crate::client_channel_config());
            public_clients.push(ParosClient::with_origin(
                public_channel.clone(),
                origin.clone(),
            ));
            internal_clients.push(ParosInternalClient::with_origin(
                internal_channel.clone(),
                origin,
            ));
            channels.push(public_channel);
            channels.push(internal_channel);
        }
        let _channel_guard = OnDrop::new(move || {
            for channel in channels {
                channel.close();
            }
        });

        let survivor_deadline = time.now() + SURVIVOR_BUDGET;
        let mut target_pos = 0_usize;
        let mut committed = 0_u64;
        let mut max_acked_slot = None::<u64>;
        let mut leader_hint = None::<usize>;
        while committed < SURVIVOR_PROPOSALS && time.now() < survivor_deadline {
            let seq = self.next_seq;
            let payload = Self::payload(seq);
            let cmd_hash = user_command_hash(&payload);
            tracing::info!(
                cmd = %hash_text(cmd_hash),
                seq,
                bytes = payload.len() as u64,
                "chain_command_submitted"
            );
            let mut acknowledged = None;
            while time.now() < survivor_deadline {
                let target = leader_hint
                    .filter(|leader| *leader != victim)
                    .unwrap_or(survivors[target_pos % survivors.len()]);
                let mut client = public_clients[target].clone();
                let response = moonpool_sim::select! {
                    response = client.propose(Propose {
                        client: client_id,
                        seq,
                        command: payload.clone(),
                    }) => response.ok().map(tonic::Response::into_inner),
                    _ = time.sleep(RPC_TIMEOUT) => None,
                };
                match response {
                    Some(ack) => {
                        assert_always!(
                            ack.seq == seq,
                            "chain choreography: proposal ack echoes request",
                            { "expected_seq" => seq, "observed_seq" => ack.seq }
                        );
                        if ack.committed {
                            acknowledged = ack.slot;
                            leader_hint = ack
                                .leader
                                .and_then(|node| usize::try_from(node).ok())
                                .filter(|node| *node != victim);
                            break;
                        }
                        leader_hint = ack
                            .leader
                            .and_then(|node| usize::try_from(node).ok())
                            .filter(|node| *node != victim);
                    }
                    None => leader_hint = None,
                }
                target_pos = (target_pos + 1) % survivors.len();
            }
            let Some(slot) = acknowledged else {
                break;
            };
            tracing::info!(cmd = %hash_text(cmd_hash), seq, slot, "chain_command_acked");
            max_acked_slot = Some(max_acked_slot.map_or(slot, |current| current.max(slot)));
            committed = committed.saturating_add(1);
            self.next_seq = self.next_seq.saturating_add(1);
        }
        let enough_survivor_work = committed >= SURVIVOR_PROPOSALS
            && max_acked_slot.is_some_and(|slot| slot > victim_prefix);
        assert_always!(
            enough_survivor_work,
            "chain choreography: survivors commit twelve unique proposals past the victim",
            {
                "committed" => committed,
                "required" => SURVIVOR_PROPOSALS,
                "victim_prefix" => victim_prefix,
                "max_acked_slot" => max_acked_slot.unwrap_or(0)
            }
        );
        if !enough_survivor_work {
            return Err(Self::invalid("survivors did not advance far enough"));
        }
        let compact_to = max_acked_slot.expect("checked above");
        let control = Command::Control(Control::Truncate {
            up_to: Slot(compact_to),
        });
        tracing::info!(
            cmd = %hash_text(command_hash(&control)),
            up_to = compact_to,
            "chain_control_submitted"
        );
        let control_seq = ctx
            .observability()
            .snapshot("chain_control_submitted")
            .into_iter()
            .map(|event| event.seq)
            .max()
            .unwrap_or(force_kill.seq);

        let mut compact_accepted = false;
        while time.now() < survivor_deadline {
            let target = leader_hint
                .filter(|leader| *leader != victim)
                .unwrap_or(survivors[target_pos % survivors.len()]);
            let mut client = public_clients[target].clone();
            let response = moonpool_sim::select! {
                response = client.compact(Compact { up_to: compact_to }) => {
                    response.ok().map(tonic::Response::into_inner)
                },
                _ = time.sleep(RPC_TIMEOUT) => None,
            };
            if let Some(ack) = response {
                if ack.accepted {
                    compact_accepted = true;
                    break;
                }
                leader_hint = ack
                    .leader
                    .and_then(|node| usize::try_from(node).ok())
                    .filter(|node| *node != victim);
            } else {
                leader_hint = None;
            }
            target_pos = (target_pos + 1) % survivors.len();
        }
        assert_always!(
            compact_accepted,
            "chain choreography: a survivor accepts compaction past the victim",
            { "compact_to" => compact_to, "victim_prefix" => victim_prefix }
        );
        if !compact_accepted {
            return Err(Self::invalid("no survivor accepted compaction"));
        }
        tracing::info!(up_to = compact_to, "chain_compact_accepted");

        let restart_deadline = time.now() + RECOVERY_BUDGET;
        let mut survivor_floors = BTreeMap::<u64, (u64, u64)>::new();
        let restart = loop {
            for event in ctx.observability().snapshot("compacted") {
                let Some(node) = event.u64("node") else {
                    continue;
                };
                if event.seq > control_seq
                    && survivors
                        .iter()
                        .any(|survivor| u64::try_from(*survivor).ok() == Some(node))
                    && event
                        .u64("first")
                        .is_some_and(|first| first > victim_prefix)
                {
                    survivor_floors.insert(node, (event.u64("first").unwrap_or(0), event.seq));
                }
            }
            let maybe_restart = ctx
                .observability()
                .snapshot(SIM_FAULT_EVENT_NAME)
                .into_iter()
                .find(|event| {
                    event.seq > force_kill.seq
                        && event.str("kind") == Some("process_restart")
                        && event.str("ip") == Some(victim_ip.as_str())
                });
            if let Some(event) = maybe_restart {
                break event;
            }
            if time.now() >= restart_deadline || ctx.shutdown().is_cancelled() {
                return Err(Self::invalid("victim did not restart"));
            }
            time.sleep(POLL_INTERVAL)
                .await
                .map_err(|error| Self::invalid(format!("restart wait failed: {error}")))?;
        };
        let both_compacted_before_restart = survivor_floors.len() == survivors.len()
            && survivor_floors.values().all(|(_, seq)| *seq < restart.seq);
        assert_always!(
            both_compacted_before_restart,
            "chain choreography: both survivors compact past the victim before restart",
            {
                "survivors_compacted" => survivor_floors.len(),
                "required" => survivors.len(),
                "victim_prefix" => victim_prefix,
                "restart_seq" => restart.seq
            }
        );
        if !both_compacted_before_restart {
            return Err(Self::invalid("restart preceded survivor compaction"));
        }

        let post_restart_deadline = time.now() + RECOVERY_BUDGET;
        let boot = Self::wait_event(ctx, "booted", post_restart_deadline, |event| {
            event.seq > restart.seq && event.u64("node") == Some(victim_node)
        })
        .await?;
        let driver_snapshot =
            Self::wait_event(ctx, "snapshot_installed", post_restart_deadline, |event| {
                event.seq > restart.seq
                    && event.u64("node") == Some(victim_node)
                    && event
                        .u64("chosen_index")
                        .is_some_and(|index| index >= compact_to)
            })
            .await?;
        let app_snapshot = Self::wait_event(
            ctx,
            "chain_snapshot_installed",
            post_restart_deadline,
            |event| {
                event.seq > restart.seq
                    && event.u64("node") == Some(victim_node)
                    && event.u64("index").is_some_and(|index| index >= compact_to)
            },
        )
        .await?;

        // Both snapshot facts must follow restart, but their relative order is
        // intentionally unconstrained: driver and application storage emit from
        // different layers of the same install batch.
        let compact_seq_max = survivor_floors
            .values()
            .map(|(_, seq)| *seq)
            .max()
            .unwrap_or(0);
        let globally_ordered = force_kill.seq < compact_seq_max
            && compact_seq_max < restart.seq
            && restart.seq < boot.seq
            && restart.seq < driver_snapshot.seq
            && restart.seq < app_snapshot.seq;
        assert_always!(
            globally_ordered,
            "chain choreography: lifecycle evidence is globally ordered",
            {
                "kill_seq" => force_kill.seq,
                "compact_seq" => compact_seq_max,
                "restart_seq" => restart.seq,
                "boot_seq" => boot.seq,
                "driver_snapshot_seq" => driver_snapshot.seq,
                "app_snapshot_seq" => app_snapshot.seq
            }
        );
        if !globally_ordered {
            return Err(Self::invalid("lifecycle evidence was out of order"));
        }

        let final_deadline = time.now() + RECOVERY_BUDGET;
        let mut final_states = Vec::with_capacity(CLUSTER_SIZE);
        let final_equal = loop {
            final_states.clear();
            for client in &internal_clients {
                let mut client = client.clone();
                let state = moonpool_sim::select! {
                    response = client.inspect(InspectRequest {}) => response
                        .ok()
                        .and_then(|response| ChainState::decode(&response.into_inner().snapshot).ok()),
                    _ = time.sleep(RPC_TIMEOUT) => None,
                };
                let Some(state) = state else {
                    break;
                };
                final_states.push(state);
            }
            let equal = final_states.len() == CLUSTER_SIZE
                && final_states.iter().all(|state| *state == final_states[0])
                && final_states[0].applied_count > baseline.applied_count
                && final_states[0]
                    .applied_slot()
                    .is_some_and(|slot| slot.0 >= compact_to);
            if equal || time.now() >= final_deadline || ctx.shutdown().is_cancelled() {
                break equal;
            }
            time.sleep(POLL_INTERVAL).await.map_err(|error| {
                Self::invalid(format!("final convergence wait failed: {error}"))
            })?;
        };
        assert_always!(
            final_equal,
            "chain choreography: external replica states agree past the baseline",
            {
                "observed_nodes" => final_states.len(),
                "baseline_count" => baseline.applied_count,
                "final_count" => final_states.first().map_or(0, |state| state.applied_count),
                "final_hash" => final_states.first().map_or(0, |state| state.chain_hash),
                "compact_to" => compact_to
            }
        );
        if !final_equal {
            return Err(Self::invalid("final external replica states did not agree"));
        }

        self.completed = true;
        assert_reachable!("chain choreography: lifecycle reaches snapshot recovery");
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        assert_sometimes!(
            self.completed,
            "chain choreography: lifecycle completes in one axis run"
        );
        Ok(())
    }
}
