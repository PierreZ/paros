//! The CTRL evaluation corpus (#113): enumerated, analytically-derived
//! recovery cases beside the coverage-guided sweep.
//!
//! CTRL §5.1 evaluates *targeted*, not random: enumerate which copies of which
//! slots are faulty, derive recoverable-vs-unrecoverable from the mask alone,
//! and demand exactly Correct on one side and `CorrectlyUnavailable` (WAITED,
//! never fabricated) on the other. The swarm's per-boot rot sites cover the
//! same territory probabilistically; this corpus is the analytic evidence that
//! catches the bug shape probability cannot — an oracle excuse that is too
//! generous never fails a random run, but fails an enumerated case whose
//! ground truth says "this mask is recoverable" (or "this mask must wait").
//!
//! Three case families, all on a fixed three-node cluster with scripted
//! lifecycle (no swarm chaos — every fault is a targeted injection):
//!
//! - [`E1MaskWorkload`]: a short fully-replicated decided prefix, every
//!   application snapshot rotted (so the log is the only custody), then a
//!   per-slot × per-node corruption mask over the decided records. A slot with
//!   ≥ 1 clean copy must converge intact; a slot with 0 clean copies must be
//!   waited on, never fabricated. The derivation cross-checks the world's
//!   `unrecoverable_slots` ground truth.
//! - [`BareQuorumWorkload`]: one slot decided by a bare quorum while the third
//!   node is down, then both holders' copies rotted — the last-copy-gone shape
//!   whose Phase-1 tally is `faulty, faulty, none`: exactly CTRL §5.1.1's
//!   mutation-(b) target (a sub-Q1 count of `none` must never no-op fill a
//!   chosen slot).
//! - [`SnapshotLifecycleWorkload`]: the §5.1.2 compound — log-only,
//!   snapshotted, and snapshotted-and-truncated nodes in one scripted run,
//!   reaching all four snapshot-recovery paths: local re-replay at floor 0,
//!   whole-blob `InstallSnapshot` under a truncated log, the below-floor
//!   `Prepare` refusal, and the truncated-past-everyone WAIT.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use moonpool_hyper::ReconnectingChannel;
use moonpool_sim::{
    RandomProvider, SimContext, SimulationError, SimulationResult, TimeProvider, TraceEvent,
    TraceQuery, Workload, assert_always, assert_sometimes,
};
use paros::{
    Command, Compact, Control, Entry, InspectRequest, ParosClient, ParosInternalClient, Propose,
    Slot, Value, parse_addr, proposal_checksum, snap_chunk_count,
};

use crate::chain::{ChainState, command_hash, hash_text, user_command_hash};
use crate::node::{
    corpus_corrupt_entry, corpus_corrupt_snap_chunk, corpus_corrupt_snapshot, corpus_disk_probe,
    corpus_hold_node, corpus_release_node, corpus_restart_node, unrecoverable_slots,
};

/// Fixed corpus cluster size. The mask grid and the analytic derivation both
/// assume it; the workloads assert the topology matches.
pub(crate) const CORPUS_NODES: usize = 3;
/// Decided-prefix length the E1 mask covers: 3 nodes × 3 slots = a 9-bit mask
/// space of 512 cases, exhaustively enumerable by the hunt axis and densely
/// sampled by the canonical nextest set.
pub(crate) const CORPUS_SLOTS: u64 = 3;
/// The full E1 mask space (`2^(CORPUS_NODES * CORPUS_SLOTS)` = 512).
pub(crate) const CORPUS_MASK_SPACE: u16 = 512;
const _: () = assert!(
    CORPUS_MASK_SPACE as u128 == 1_u128 << (CORPUS_NODES as u128 * CORPUS_SLOTS as u128),
    "the mask space covers exactly the node x slot grid"
);

const RPC_TIMEOUT: Duration = Duration::from_secs(1);
const PRIME_BUDGET: Duration = Duration::from_secs(30);
const OUTCOME_BUDGET: Duration = Duration::from_secs(90);
/// How long an unrecoverable case must *hold* its wait after first reaching it
/// before the run believes nothing will be fabricated late.
const WAIT_SETTLE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Where an E1 run's mask comes from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MaskSource {
    /// An explicit mask (the canonical nextest cases; bit index
    /// `node * CORPUS_SLOTS + slot`).
    Fixed(u16),
    /// Drawn from the run's seeded RNG (the hunt axis's dense sampling).
    Seeded,
}

fn invalid(message: impl Into<String>) -> SimulationError {
    SimulationError::InvalidState(message.into())
}

fn sorted_servers(ctx: &SimContext) -> SimulationResult<Vec<String>> {
    let mut servers = ctx.topology().all_process_ips().to_vec();
    servers.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
    servers.dedup();
    let valid = servers.len() == CORPUS_NODES;
    assert_always!(
        valid,
        "corpus: the corpus axis runs exactly three nodes",
        { "nodes" => servers.len() }
    );
    if !valid {
        return Err(invalid(format!(
            "corpus expected {CORPUS_NODES} nodes, got {}",
            servers.len()
        )));
    }
    Ok(servers)
}

/// A deterministic per-seq payload (same shape as the lifecycle choreography's).
fn payload(seq: u64) -> Vec<u8> {
    let mut state = seq ^ 0x517c_c1b7_2722_0a95;
    let mut bytes = Vec::with_capacity(48);
    for _ in 0..48 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push(state.to_le_bytes()[0]);
    }
    bytes
}

/// Fold the expected chain states for a command sequence: `expected[i]` is the
/// application state after the first `i` commands. This is the corpus's own
/// analytic model of the register — computed from what it proposed, never read
/// back from the cluster.
fn expected_states(commands: &[Command]) -> Vec<ChainState> {
    let mut states = vec![ChainState::default()];
    for command in commands {
        let previous = *states.last().expect("seeded with the initial state");
        states.push(previous.apply(command).next);
    }
    states
}

fn user_command(client: u64, seq: u64, bytes: Vec<u8>) -> Command {
    Command::User(Entry {
        client: paros::ClientId(client),
        seq: paros::ClientSeq(seq),
        value: Value(bytes),
    })
}

/// The corpus's client bundle: one public + one internal gRPC client per node.
struct CorpusClients {
    public: Vec<ParosClient<ReconnectingChannel<moonpool_sim::SimProviders, tonic::body::Body>>>,
    internal: Vec<
        ParosInternalClient<ReconnectingChannel<moonpool_sim::SimProviders, tonic::body::Body>>,
    >,
    channels: Vec<ReconnectingChannel<moonpool_sim::SimProviders, tonic::body::Body>>,
}

impl Drop for CorpusClients {
    /// Closing is idempotent and shared by every clone: the channels' connect,
    /// backoff, and keep-alive tasks stop on every exit path from a workload.
    fn drop(&mut self) {
        for channel in &self.channels {
            channel.close();
        }
    }
}

impl CorpusClients {
    fn connect(ctx: &SimContext, servers: &[String]) -> SimulationResult<Self> {
        let mut public = Vec::with_capacity(servers.len());
        let mut internal = Vec::with_capacity(servers.len());
        let mut channels = Vec::with_capacity(servers.len());
        for ip in servers {
            let addr = parse_addr(ip)?;
            let origin = http::Uri::try_from(format!("http://{addr}"))
                .map_err(|e| invalid(format!("bad gRPC origin: {e}")))?;
            let channel =
                ReconnectingChannel::new(ctx.providers(), addr, crate::client_channel_config());
            public.push(ParosClient::with_origin(channel.clone(), origin.clone()));
            internal.push(ParosInternalClient::with_origin(channel.clone(), origin));
            channels.push(channel);
        }
        Ok(Self {
            public,
            internal,
            channels,
        })
    }

    /// Propose `(seq, bytes)` until some node commits it, rotating targets and
    /// following leader hints. Returns the committed slot, or `None` at the
    /// deadline.
    async fn propose_until_acked(
        &self,
        ctx: &SimContext,
        client_id: u64,
        seq: u64,
        bytes: &[u8],
        exclude: Option<usize>,
        deadline: Duration,
    ) -> Option<u64> {
        let time = ctx.time();
        let count = self.public.len();
        let mut target = (usize::try_from(seq).unwrap_or(0)) % count;
        loop {
            if time.now() >= deadline || ctx.shutdown().is_cancelled() {
                return None;
            }
            if Some(target) == exclude {
                target = (target + 1) % count;
                continue;
            }
            let mut client = self.public[target].clone();
            let response = moonpool_sim::select! {
                response = client.propose(Propose {
                    client: client_id,
                    seq,
                    checksum: proposal_checksum(client_id, seq, bytes),
                    command: bytes.to_vec(),
                }) => response.ok().map(tonic::Response::into_inner),
                _ = time.sleep(RPC_TIMEOUT) => None,
            };
            match response {
                Some(ack) if ack.committed => return ack.slot,
                Some(ack) => {
                    target = ack
                        .leader
                        .and_then(|node| usize::try_from(node).ok())
                        .filter(|node| *node < count && Some(*node) != exclude)
                        .unwrap_or((target + 1) % count);
                }
                None => target = (target + 1) % count,
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
    }

    /// Ask for a decided `Truncate{up_to}` until a leader accepts it.
    async fn compact_until_accepted(
        &self,
        ctx: &SimContext,
        up_to: u64,
        exclude: Option<usize>,
        deadline: Duration,
    ) -> bool {
        let control = Command::Control(Control::Truncate { up_to: Slot(up_to) });
        tracing::info!(
            cmd = %hash_text(command_hash(&control)),
            up_to,
            "chain_control_submitted"
        );
        let time = ctx.time();
        let count = self.public.len();
        let mut target = 0_usize;
        loop {
            if time.now() >= deadline || ctx.shutdown().is_cancelled() {
                return false;
            }
            if Some(target) == exclude {
                target = (target + 1) % count;
                continue;
            }
            let mut client = self.public[target].clone();
            let response = moonpool_sim::select! {
                response = client.compact(Compact { up_to }) => {
                    response.ok().map(tonic::Response::into_inner)
                }
                _ = time.sleep(RPC_TIMEOUT) => None,
            };
            match response {
                Some(ack) if ack.accepted => return true,
                Some(ack) => {
                    target = ack
                        .leader
                        .and_then(|node| usize::try_from(node).ok())
                        .filter(|node| *node < count && Some(*node) != exclude)
                        .unwrap_or((target + 1) % count);
                }
                None => target = (target + 1) % count,
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
    }

    /// One live application-state read from node `i` (`None` on timeout).
    async fn inspect(&self, ctx: &SimContext, i: usize) -> Option<ChainState> {
        let time = ctx.time();
        let mut client = self.internal[i].clone();
        moonpool_sim::select! {
            response = client.inspect(InspectRequest {}) => response
                .ok()
                .and_then(|response| ChainState::decode(&response.into_inner().snapshot).ok()),
            _ = time.sleep(RPC_TIMEOUT) => None,
        }
    }

    /// Wait until every live node's inspected state equals `want` (`true`), or
    /// the deadline passes (`false`).
    async fn wait_all_at(&self, ctx: &SimContext, want: &ChainState, deadline: Duration) -> bool {
        let time = ctx.time();
        loop {
            if ctx.shutdown().is_cancelled() {
                return false;
            }
            let mut all = true;
            for i in 0..self.internal.len() {
                match self.inspect(ctx, i).await {
                    Some(state) if state == *want => {}
                    _ => {
                        all = false;
                        break;
                    }
                }
            }
            if all {
                return true;
            }
            if time.now() >= deadline {
                return false;
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
    }

    /// Assert every node *stays* exactly at `want` for `hold`: any progress
    /// past it would be a fabricated value for a lost slot.
    async fn hold_all_at(&self, ctx: &SimContext, want: &ChainState, hold: Duration) -> bool {
        let time = ctx.time();
        let until = time.now() + hold;
        while time.now() < until && !ctx.shutdown().is_cancelled() {
            for i in 0..self.internal.len() {
                if let Some(state) = self.inspect(ctx, i).await
                    && state != *want
                {
                    eprintln!(
                        "CORPUS-DIAG hold deviation: node {i} at ({}, {:016x}), want ({}, {:016x}), t={}ms",
                        state.applied_count,
                        state.chain_hash,
                        want.applied_count,
                        want.chain_hash,
                        time.now().as_millis(),
                    );
                    return false;
                }
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
        true
    }
}

/// Wait until every node's durable world record shows the fully replicated,
/// fully applied prefix (`slots` clean everywhere, application at `want`).
async fn wait_replicated(
    ctx: &SimContext,
    servers: &[String],
    slots: &BTreeSet<u64>,
    want: &ChainState,
    deadline: Duration,
) -> bool {
    let time = ctx.time();
    loop {
        if ctx.shutdown().is_cancelled() {
            return false;
        }
        let all = servers.iter().all(|ip| {
            corpus_disk_probe(ctx.state(), ip).is_some_and(|probe| {
                slots.is_subset(&probe.clean_slots)
                    && probe.applied_count == want.applied_count
                    && probe.chain_hash == want.chain_hash
            })
        });
        if all {
            return true;
        }
        if time.now() >= deadline {
            return false;
        }
        time.sleep(POLL_INTERVAL).await.ok();
    }
}

/// Wait for one matching trace event (the lifecycle compound's evidence gates).
async fn wait_event<F>(
    ctx: &SimContext,
    name: &str,
    deadline: Duration,
    predicate: F,
) -> Option<TraceEvent>
where
    F: Fn(&TraceEvent) -> bool,
{
    let time = ctx.time();
    loop {
        if let Some(event) = ctx
            .observability()
            .snapshot(name)
            .into_iter()
            .find(&predicate)
        {
            return Some(event);
        }
        if time.now() >= deadline || ctx.shutdown().is_cancelled() {
            return None;
        }
        time.sleep(POLL_INTERVAL).await.ok();
    }
}

/// Prime `count` sequential user commands (seqs `0..count`), asserting each
/// decides its expected slot, and return the committed command list.
async fn prime_prefix(
    ctx: &SimContext,
    clients: &CorpusClients,
    client_id: u64,
    count: u64,
    exclude: Option<usize>,
    seq_base: u64,
    slot_base: u64,
) -> SimulationResult<Vec<Command>> {
    let deadline = ctx.time().now() + PRIME_BUDGET;
    let mut commands = Vec::new();
    for offset in 0..count {
        let seq = seq_base + offset;
        let bytes = payload(seq);
        tracing::info!(
            cmd = %hash_text(user_command_hash(&bytes)),
            seq,
            bytes = bytes.len() as u64,
            "chain_command_submitted"
        );
        let Some(slot) = clients
            .propose_until_acked(ctx, client_id, seq, &bytes, exclude, deadline)
            .await
        else {
            assert_always!(
                false,
                "corpus: priming decides its full prefix inside the budget",
                { "seq" => seq }
            );
            return Err(invalid("corpus priming timed out"));
        };
        // The analytic model needs to know exactly which slot holds which
        // command; a quiet scripted cluster decides them contiguously.
        assert_always!(
            slot == slot_base + offset,
            "corpus: priming decides the expected contiguous slots",
            { "seq" => seq, "slot" => slot, "expected" => slot_base + offset }
        );
        commands.push(user_command(client_id, seq, bytes));
    }
    Ok(commands)
}

// --- the E1 mask workload -----------------------------------------------------

/// E1-style per-slot × per-node corruption masks over a fully replicated
/// decided prefix (see the module doc).
pub(crate) struct E1MaskWorkload {
    source: MaskSource,
    recovered_intact: bool,
    waited_unrecoverable: bool,
}

impl E1MaskWorkload {
    pub(crate) fn new(source: MaskSource) -> Self {
        Self {
            source,
            recovered_intact: false,
            waited_unrecoverable: false,
        }
    }
}

#[async_trait]
impl Workload for E1MaskWorkload {
    fn name(&self) -> &'static str {
        "corpus-e1-mask"
    }

    #[allow(clippy::too_many_lines)] // one linear scripted case: prime → inject → derive → judge
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = sorted_servers(ctx)?;
        let clients = CorpusClients::connect(ctx, &servers)?;
        let time = ctx.time().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);

        // Phase 1: prime and fully replicate the decided prefix.
        let commands = prime_prefix(ctx, &clients, client_id, CORPUS_SLOTS, None, 0, 0).await?;
        let expected = expected_states(&commands);
        let full = expected[commands.len()];
        let slots: BTreeSet<u64> = (0..CORPUS_SLOTS).collect();
        let replicated =
            wait_replicated(ctx, &servers, &slots, &full, time.now() + PRIME_BUDGET).await;
        assert_always!(
            replicated,
            "corpus: priming replicates and applies the full prefix everywhere"
        );
        if !replicated {
            return Err(invalid("corpus priming did not replicate"));
        }

        // Phase 2: derive the mask and inject it — atomically with the
        // restarts (no await between them), so no flush can heal a mark first.
        let mask = match self.source {
            MaskSource::Fixed(mask) => mask % CORPUS_MASK_SPACE,
            MaskSource::Seeded => {
                u16::try_from(ctx.random().random::<u64>() % u64::from(CORPUS_MASK_SPACE))
                    .unwrap_or(0)
            }
        };
        tracing::info!(mask, "corpus_mask_selected");
        let mut derived_unrecoverable: BTreeSet<u64> = BTreeSet::new();
        // Every snapshot is rotted first: the decided log is the only custody
        // left, so the mask alone decides recoverability.
        for (n, ip) in servers.iter().enumerate() {
            corpus_corrupt_snapshot(ctx.state(), ip, u64::try_from(n).unwrap_or(u64::MAX));
        }
        for slot in 0..CORPUS_SLOTS {
            let mut corrupted = 0_usize;
            for (n, ip) in servers.iter().enumerate() {
                let bit = u16::try_from(n).unwrap_or(0) * u16::try_from(CORPUS_SLOTS).unwrap_or(0)
                    + u16::try_from(slot).unwrap_or(0);
                if mask & (1_u16 << bit) != 0 {
                    let landed = corpus_corrupt_entry(
                        ctx.state(),
                        ip,
                        u64::try_from(n).unwrap_or(u64::MAX),
                        slot,
                    );
                    assert_always!(
                        landed,
                        "corpus: a mask injection lands on a clean replicated record",
                        { "slot" => slot, "node" => n }
                    );
                    corrupted += 1;
                }
            }
            if corrupted == servers.len() {
                derived_unrecoverable.insert(slot);
            }
        }
        // The cross-check this corpus exists for: the analytic derivation and
        // the world's independently computed ground truth must agree exactly.
        let ground_truth = unrecoverable_slots(ctx.state());
        assert_always!(
            ground_truth == derived_unrecoverable,
            "corpus: the analytic mask derivation matches the world's unrecoverable ground truth",
            {
                "mask" => mask,
                "derived" => derived_unrecoverable.len(),
                "world" => ground_truth.len()
            }
        );
        for ip in &servers {
            corpus_restart_node(ctx.state(), ip);
        }

        // Phase 3: judge the analytically derived outcome over live RPC reads.
        let deadline = time.now() + OUTCOME_BUDGET;
        if let Some(&lost) = derived_unrecoverable.iter().next() {
            // CorrectlyUnavailable: every node recovers exactly the prefix
            // below the first lost slot and then WAITS — no fabrication, ever.
            let held_state = expected[usize::try_from(lost).unwrap_or(0)];
            let reached = clients.wait_all_at(ctx, &held_state, deadline).await;
            let held = reached && clients.hold_all_at(ctx, &held_state, WAIT_SETTLE).await;
            if !(reached && held) {
                // Failure diagnostic (fires only on the red path): each node's
                // live state and durable evidence at the moment of judgment.
                for (n, ip) in servers.iter().enumerate() {
                    let live = clients.inspect(ctx, n).await;
                    let probe = corpus_disk_probe(ctx.state(), ip);
                    eprintln!(
                        "CORPUS-DIAG node {n}: live={:?} clean_slots={:?} floor={:?} applied={:?}",
                        live.map(|s| (s.applied_count, s.chain_hash)),
                        probe.as_ref().map(|p| p.clean_slots.clone()),
                        probe.as_ref().map(|p| p.floor),
                        probe.as_ref().map(|p| (p.applied_count, p.chain_hash)),
                    );
                }
                eprintln!(
                    "CORPUS-DIAG mask={mask:#011b} derived={derived_unrecoverable:?} world={:?} expected_hold=({}, {:016x})",
                    unrecoverable_slots(ctx.state()),
                    held_state.applied_count,
                    held_state.chain_hash,
                );
            }
            assert_always!(
                reached,
                "corpus: an unrecoverable mask holds every node at the last recoverable prefix",
                { "mask" => mask, "lost_slot" => lost }
            );
            assert_always!(
                held,
                "corpus: an unrecoverable mask never fabricates past a lost slot",
                { "mask" => mask, "lost_slot" => lost }
            );
            self.waited_unrecoverable = reached && held;
        } else {
            // Correct: every slot kept a clean copy, so the cluster must
            // converge back to the exact pre-injection state.
            let converged = clients.wait_all_at(ctx, &full, deadline).await;
            assert_always!(
                converged,
                "corpus: a recoverable mask converges to the pre-injection state",
                { "mask" => mask }
            );
            self.recovered_intact = converged;
        }
        drop(clients);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        assert_sometimes!(
            self.recovered_intact,
            "corpus: a recoverable corruption mask converges intact"
        );
        assert_sometimes!(
            self.waited_unrecoverable,
            "corpus: an unrecoverable corruption mask waits without fabricating"
        );
        Ok(())
    }
}

// --- the bare-quorum lost-slot case -------------------------------------------

/// One slot decided by a bare quorum while the third node is down, then both
/// holders' copies (and their application snapshots) rotted: the CTRL
/// `faulty, faulty, none` tally. The cluster must WAIT at the lost slot — the
/// deterministic target of §5.1.1's mutation (b), where a sub-Q1 `none` count
/// no-op fills the chosen slot and fabricates history.
pub(crate) struct BareQuorumWorkload {
    waited: bool,
}

impl BareQuorumWorkload {
    pub(crate) fn new() -> Self {
        Self { waited: false }
    }
}

#[async_trait]
impl Workload for BareQuorumWorkload {
    fn name(&self) -> &'static str {
        "corpus-bare-quorum"
    }

    #[allow(clippy::too_many_lines)] // one linear scripted case
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = sorted_servers(ctx)?;
        let clients = CorpusClients::connect(ctx, &servers)?;
        let time = ctx.time().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let absent = CORPUS_NODES - 1;

        // Phase 1: two fully replicated slots.
        let mut commands = prime_prefix(ctx, &clients, client_id, 2, None, 0, 0).await?;
        let expected2 = expected_states(&commands)[2];
        let replicated = wait_replicated(
            ctx,
            &servers,
            &(0..2).collect(),
            &expected2,
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            replicated,
            "corpus: priming replicates and applies the full prefix everywhere"
        );
        if !replicated {
            drop(clients);
            return Err(invalid("bare-quorum priming did not replicate"));
        }

        // Phase 2: hold the third node down; decide slot 2 on the bare quorum.
        corpus_hold_node(ctx.state(), &servers[absent]);
        let survivors: Vec<String> = servers[..absent].to_vec();
        commands.extend(prime_prefix(ctx, &clients, client_id, 1, Some(absent), 2, 2).await?);
        let expected3 = expected_states(&commands)[3];
        let survivors_hold = wait_replicated(
            ctx,
            &survivors,
            &(0..3).collect(),
            &expected3,
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            survivors_hold,
            "corpus: the bare quorum holds and applies the extra slot",
            { "slot" => 2_u64 }
        );

        // Phase 3 (atomic with the restarts): rot both holders' slot-2 copies
        // and their snapshots. The value now exists nowhere readable — the
        // third node honestly reports `none` (it never accepted the slot).
        for (n, ip) in survivors.iter().enumerate() {
            let node = u64::try_from(n).unwrap_or(u64::MAX);
            corpus_corrupt_snapshot(ctx.state(), ip, node);
            let landed = corpus_corrupt_entry(ctx.state(), ip, node, 2);
            assert_always!(
                landed,
                "corpus: a mask injection lands on a clean replicated record",
                { "slot" => 2_u64, "node" => n }
            );
        }
        let ground_truth = unrecoverable_slots(ctx.state());
        let derived: BTreeSet<u64> = [2].into_iter().collect();
        assert_always!(
            ground_truth == derived,
            "corpus: the analytic mask derivation matches the world's unrecoverable ground truth",
            { "world" => ground_truth.len() }
        );
        for ip in &survivors {
            corpus_restart_node(ctx.state(), ip);
        }
        corpus_release_node(ctx.state(), &servers[absent]);

        // Phase 4: every node — the `none` reporter included — must settle at
        // the two-slot prefix and WAIT at slot 2. A `Noop` fill here (the
        // §5.1.1-(b) mutation) would advance the count past 2 and go red.
        let deadline = time.now() + OUTCOME_BUDGET;
        let reached = clients.wait_all_at(ctx, &expected2, deadline).await;
        assert_always!(
            reached,
            "corpus: an unrecoverable mask holds every node at the last recoverable prefix",
            { "lost_slot" => 2_u64 }
        );
        let held = clients.hold_all_at(ctx, &expected2, WAIT_SETTLE).await;
        assert_always!(
            held,
            "corpus: an unrecoverable mask never fabricates past a lost slot",
            { "lost_slot" => 2_u64 }
        );
        self.waited = reached && held;
        drop(clients);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        assert_sometimes!(
            self.waited,
            "corpus: a bare-quorum lost slot is correctly waited on"
        );
        Ok(())
    }
}

// --- the §5.1.2 snapshot-lifecycle compound -----------------------------------

/// The §5.1.2 compound run (see the module doc): all four snapshot-recovery
/// paths in one scripted scenario.
pub(crate) struct SnapshotLifecycleWorkload {
    completed: bool,
}

impl SnapshotLifecycleWorkload {
    pub(crate) fn new() -> Self {
        Self { completed: false }
    }
}

#[async_trait]
impl Workload for SnapshotLifecycleWorkload {
    fn name(&self) -> &'static str {
        "corpus-snapshot-lifecycle"
    }

    #[allow(clippy::too_many_lines)] // one linear scripted compound scenario
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = sorted_servers(ctx)?;
        let clients = CorpusClients::connect(ctx, &servers)?;
        let time = ctx.time().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let state = ctx.state();

        // Phase A: five decided, fully replicated slots.
        let mut commands = prime_prefix(ctx, &clients, client_id, 5, None, 0, 0).await?;
        let state5 = expected_states(&commands)[5];
        let replicated = wait_replicated(
            ctx,
            &servers,
            &(0..5).collect(),
            &state5,
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            replicated,
            "corpus: priming replicates and applies the full prefix everywhere"
        );
        if !replicated {
            drop(clients);
            return Err(invalid("lifecycle priming did not replicate"));
        }

        // Phase B — path 1, local re-replay at floor 0: rot one node's
        // snapshot (making it log-only) and restart it; the boot scan resets
        // the application and the local log rebuilds the exact state.
        corpus_corrupt_snapshot(state, &servers[2], 2);
        corpus_restart_node(state, &servers[2]);
        let replayed = wait_replicated(
            ctx,
            &servers[2..],
            &(0..5).collect(),
            &state5,
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            replayed,
            "corpus: a log-only node replays its exact state from its local log"
        );

        // Phase C: hold node 0 down; the survivors decide three more slots and
        // a Truncate past all of them, raising both floors.
        corpus_hold_node(state, &servers[0]);
        commands.extend(prime_prefix(ctx, &clients, client_id, 3, Some(0), 5, 5).await?);
        let compacted = clients
            .compact_until_accepted(ctx, 7, Some(0), time.now() + PRIME_BUDGET)
            .await;
        assert_always!(
            compacted,
            "corpus: a survivor accepts compaction past the held-down node"
        );
        // Under the #101 coupling, compaction decides two commands: the Snap
        // marker at slot 8 (the decided snapshot point that must cover the
        // truncation), then the Truncate at slot 9.
        commands.push(Command::Control(Control::Snap { at_index: Slot(8) }));
        commands.push(Command::Control(Control::Truncate { up_to: Slot(7) }));
        let full_state = expected_states(&commands)[10];
        let floors_raised = {
            let deadline = time.now() + PRIME_BUDGET;
            loop {
                let both = servers[1..]
                    .iter()
                    .all(|ip| corpus_disk_probe(state, ip).is_some_and(|probe| probe.floor == 8));
                if both {
                    break true;
                }
                if time.now() >= deadline || ctx.shutdown().is_cancelled() {
                    break false;
                }
                time.sleep(POLL_INTERVAL).await.ok();
            }
        };
        assert_always!(
            floors_raised,
            "corpus: both survivors truncate past the held-down node"
        );
        if !floors_raised {
            drop(clients);
            return Err(invalid("survivors did not truncate"));
        }

        // Phase D: hold both survivors; rot the held node's snapshot and
        // release it alone. It re-replays its retained log locally (floor 0)
        // and campaigns unanswered, its ballot and promise climbing.
        corpus_hold_node(state, &servers[1]);
        corpus_hold_node(state, &servers[2]);
        corpus_corrupt_snapshot(state, &servers[0], 0);
        corpus_release_node(state, &servers[0]);
        time.sleep(Duration::from_secs(4)).await.ok();

        // Phase E — paths 2 and 3: release the survivors. The lone node's
        // next campaign prepares from slot 5, below both floors — refused by
        // the floor guard (path 3) — while its campaign catch-up probe draws
        // the whole-blob InstallSnapshot that heals it (path 2).
        let release_seq = ctx
            .observability()
            .snapshot(paros::EV_PREPARE_BELOW_FLOOR)
            .into_iter()
            .map(|event| event.seq)
            .max()
            .unwrap_or(0);
        corpus_release_node(state, &servers[1]);
        corpus_release_node(state, &servers[2]);
        let refusal = wait_event(
            ctx,
            paros::EV_PREPARE_BELOW_FLOOR,
            time.now() + OUTCOME_BUDGET,
            |event| event.seq > release_seq && event.u64("node").is_some_and(|node| node >= 1),
        )
        .await;
        assert_always!(
            refusal.is_some(),
            "corpus: a below-floor campaign is refused by a truncated acceptor"
        );
        let installed = wait_event(
            ctx,
            "snapshot_installed",
            time.now() + OUTCOME_BUDGET,
            |event| {
                event.u64("node") == Some(0)
                    && event.u64("chosen_index").is_some_and(|index| index >= 8)
            },
        )
        .await;
        assert_always!(
            installed.is_some(),
            "corpus: a below-floor node recovers by whole-blob snapshot install"
        );
        let healed = clients
            .wait_all_at(ctx, &full_state, time.now() + OUTCOME_BUDGET)
            .await;
        assert_always!(
            healed,
            "corpus: the lifecycle cluster converges after the snapshot heal"
        );
        if !healed {
            drop(clients);
            return Err(invalid("cluster did not converge after the install"));
        }

        // Phase F — path 4, truncated past everyone: every node now sits above
        // a raised floor; rot every live snapshot AND every chunk of every
        // retained decided point (the survivors hold the point at slot 8 —
        // without rotting it too, #101's local point restore would rescue
        // them), then restart everyone atomically. The folded prefix has no
        // custody left anywhere — the whole cluster must wait at applied
        // count 0, fabricating nothing.
        for (n, ip) in servers.iter().enumerate() {
            let node = u64::try_from(n).unwrap_or(u64::MAX);
            corpus_corrupt_snapshot(state, ip, node);
            if let Some(probe) = corpus_disk_probe(state, ip)
                && probe.snap_point.is_some()
            {
                let mut chunk = 0_u32;
                while corpus_corrupt_snap_chunk(state, ip, node, chunk) {
                    chunk += 1;
                }
            }
        }
        let ground_truth = unrecoverable_slots(state);
        let folded: BTreeSet<u64> = (0..8).collect();
        assert_always!(
            folded.is_subset(&ground_truth),
            "corpus: the truncated-past-everyone fold is unrecoverable ground truth",
            { "world" => ground_truth.len() }
        );
        for ip in &servers {
            corpus_restart_node(state, ip);
        }
        let waiting = clients
            .wait_all_at(ctx, &ChainState::default(), time.now() + OUTCOME_BUDGET)
            .await;
        assert_always!(
            waiting,
            "corpus: a truncated cluster with rotted snapshots resets and waits"
        );
        let held = clients
            .hold_all_at(ctx, &ChainState::default(), WAIT_SETTLE)
            .await;
        assert_always!(
            held,
            "corpus: the truncated-past-everyone wait never fabricates state"
        );
        self.completed = waiting && held;
        drop(clients);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        assert_sometimes!(
            self.completed,
            "corpus: the snapshot-lifecycle compound reaches all four recovery paths"
        );
        Ok(())
    }
}

// --- the #101 per-chunk mask corpus -------------------------------------------

/// Where a chunk-mask run's mask comes from (bit index
/// `node * chunk_count + chunk` over the decided point's blob).
#[derive(Clone, Copy, Debug)]
pub(crate) enum ChunkMaskSource {
    /// An explicit mask (the canonical nextest cases).
    Fixed(u32),
    /// Drawn from the run's seeded RNG (the hunt axis's dense sampling).
    Seeded,
}

/// Per-chunk corruption masks over the retained decided snapshot point (#101):
/// a fully replicated prefix is compacted through the Snap/Truncate coupling,
/// every node retains the byte-identical point, and the mask rots chunks per
/// node. A chunk with ≥ 1 clean copy anywhere must be repaired back to clean
/// on every holder (chunk repair is the only heal — the live application
/// states stay healthy, so no whole-blob path runs); a chunk with 0 clean
/// copies must stay faulty on every holder, never fabricated, while the
/// cluster itself stays fully available (the live states are custody). With
/// `rot_live_node0`, node 0's live snapshot is rotted too, driving the
/// point-restore / whole-blob race on top of the chunk repair.
pub(crate) struct ChunkMaskWorkload {
    source: ChunkMaskSource,
    rot_live_node0: bool,
    repaired_clean: bool,
    unassemblable_held: bool,
}

impl ChunkMaskWorkload {
    pub(crate) fn new(source: ChunkMaskSource, rot_live_node0: bool) -> Self {
        Self {
            source,
            rot_live_node0,
            repaired_clean: false,
            unassemblable_held: false,
        }
    }
}

#[async_trait]
impl Workload for ChunkMaskWorkload {
    fn name(&self) -> &'static str {
        "corpus-chunk-mask"
    }

    #[allow(clippy::too_many_lines)] // one linear scripted case
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = sorted_servers(ctx)?;
        let clients = CorpusClients::connect(ctx, &servers)?;
        let time = ctx.time().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let state = ctx.state();

        // Phase 1: six decided slots, then compaction through the coupling —
        // the Snap marker at slot 6, the Truncate at slot 7, floor 6, and the
        // byte-identical decided point retained on every node.
        let mut commands = prime_prefix(ctx, &clients, client_id, 6, None, 0, 0).await?;
        // Full replication before compaction: every node must hold and apply
        // the whole prefix, so no node is below the floor when the coupling's
        // Snap + Truncate decide — all three then record the identical point
        // themselves (the mask assumes three holders).
        let primed = expected_states(&commands)[6];
        let replicated = wait_replicated(
            ctx,
            &servers,
            &(0..6).collect(),
            &primed,
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            replicated,
            "corpus: priming replicates and applies the full prefix everywhere"
        );
        if !replicated {
            drop(clients);
            return Err(invalid("chunk corpus priming did not replicate"));
        }
        let compacted = clients
            .compact_until_accepted(ctx, 5, None, time.now() + PRIME_BUDGET)
            .await;
        assert_always!(
            compacted,
            "corpus: compaction is accepted once the point is quorum-held"
        );
        commands.push(Command::Control(Control::Snap { at_index: Slot(6) }));
        commands.push(Command::Control(Control::Truncate { up_to: Slot(5) }));
        let states = expected_states(&commands);
        let full = states[8];
        let point_state = states[7];
        let chunk_count = snap_chunk_count(point_state.encode().len());
        assert_always!(
            chunk_count == 5,
            "corpus: the chunk grid matches the decided point's blob",
            { "chunks" => chunk_count }
        );
        let settled = {
            let deadline = time.now() + PRIME_BUDGET;
            loop {
                let all = servers.iter().all(|ip| {
                    corpus_disk_probe(state, ip).is_some_and(|probe| {
                        probe.floor == 6
                            && probe.snap_point == Some(6)
                            && probe.faulty_chunks.is_empty()
                            && probe.applied_count == full.applied_count
                            && probe.chain_hash == full.chain_hash
                    })
                });
                if all {
                    break true;
                }
                if time.now() >= deadline || ctx.shutdown().is_cancelled() {
                    break false;
                }
                time.sleep(POLL_INTERVAL).await.ok();
            }
        };
        if !settled {
            // Failure diagnostic (fires only on the red path).
            for (n, ip) in servers.iter().enumerate() {
                let probe = corpus_disk_probe(state, ip);
                eprintln!(
                    "CORPUS-DIAG chunk settle node {n}: floor={:?} point={:?} applied={:?} want=({}, {:016x})",
                    probe.as_ref().map(|p| p.floor),
                    probe.as_ref().map(|p| p.snap_point),
                    probe.as_ref().map(|p| (p.applied_count, p.chain_hash)),
                    full.applied_count,
                    full.chain_hash,
                );
            }
            for event in ctx.observability().snapshot("command_applied") {
                if event.u64("node") == Some(1) {
                    eprintln!(
                        "CORPUS-DIAG applied idx={:?} kind={:?} cmd={:?}",
                        event.u64("index"),
                        event.str("kind"),
                        event.str("cmd"),
                    );
                }
            }
        }
        assert_always!(
            settled,
            "corpus: every node retains the decided point before the chunk mask"
        );
        if !settled {
            drop(clients);
            return Err(invalid("chunk corpus did not settle before injection"));
        }

        // Phase 2: derive and inject the chunk mask, atomically with the
        // restarts whose boot scans classify it.
        let space: u32 = 1 << (u32::try_from(servers.len()).unwrap_or(3) * chunk_count);
        let mask = match self.source {
            ChunkMaskSource::Fixed(mask) => mask % space,
            ChunkMaskSource::Seeded => {
                u32::try_from(ctx.random().random::<u64>() % u64::from(space)).unwrap_or(0)
            }
        };
        tracing::info!(mask, "corpus_chunk_mask_selected");
        let mut unassemblable: BTreeSet<u32> = BTreeSet::new();
        for chunk in 0..chunk_count {
            let mut rotted = 0_usize;
            for (n, ip) in servers.iter().enumerate() {
                let bit = u32::try_from(n).unwrap_or(0) * chunk_count + chunk;
                if mask & (1_u32 << bit) != 0 {
                    let landed = corpus_corrupt_snap_chunk(
                        state,
                        ip,
                        u64::try_from(n).unwrap_or(u64::MAX),
                        chunk,
                    );
                    assert_always!(
                        landed,
                        "corpus: a chunk mask injection lands on a clean chunk",
                        { "node" => n, "chunk" => chunk }
                    );
                    rotted += 1;
                }
            }
            if rotted == servers.len() {
                unassemblable.insert(chunk);
            }
        }
        if self.rot_live_node0 {
            corpus_corrupt_snapshot(state, &servers[0], 0);
        }
        // Cross-check: chunk rot alone never strands a slot — the live
        // application states (and, with one live rot, the two healthy peers)
        // remain custody, so the world's ground truth must stay empty.
        let ground_truth = unrecoverable_slots(state);
        assert_always!(
            ground_truth.is_empty(),
            "corpus: chunk rot alone leaves every slot recoverable",
            { "world" => ground_truth.len() }
        );
        for ip in &servers {
            corpus_restart_node(state, ip);
        }

        // Phase 3: judge. Assemblable chunks must heal back to clean on every
        // holder (chunk repair is the only path — live states stay healthy);
        // unassemblable chunks must stay faulty everywhere, never fabricated;
        // and the cluster converges to the full state either way.
        let deadline = time.now() + OUTCOME_BUDGET;
        let converged = clients.wait_all_at(ctx, &full, deadline).await;
        assert_always!(
            converged,
            "corpus: the cluster stays available under chunk rot",
            { "mask" => mask }
        );
        let repaired = {
            loop {
                let healed = servers.iter().all(|ip| {
                    corpus_disk_probe(state, ip).is_some_and(|probe| {
                        probe
                            .faulty_chunks
                            .iter()
                            .all(|chunk| unassemblable.contains(chunk))
                    })
                });
                if healed {
                    break true;
                }
                if time.now() >= deadline || ctx.shutdown().is_cancelled() {
                    break false;
                }
                time.sleep(POLL_INTERVAL).await.ok();
            }
        };
        assert_always!(
            repaired,
            "corpus: every assemblable chunk is repaired from a peer",
            { "mask" => mask }
        );
        // The settle hold: nothing may resolve an unassemblable chunk — a
        // late "repair" of a chunk with zero clean copies would be fabricated
        // bytes (the write-side identity assert is the second line of
        // defense).
        let held = {
            let until = time.now() + WAIT_SETTLE;
            let mut ok = true;
            while time.now() < until && !ctx.shutdown().is_cancelled() {
                for (n, ip) in servers.iter().enumerate() {
                    let bits_for_node = |chunk: u32| {
                        let bit = u32::try_from(n).unwrap_or(0) * chunk_count + chunk;
                        mask & (1_u32 << bit) != 0
                    };
                    let still_faulty = corpus_disk_probe(state, ip).is_some_and(|probe| {
                        unassemblable
                            .iter()
                            .filter(|chunk| bits_for_node(**chunk))
                            .all(|chunk| probe.faulty_chunks.contains(chunk))
                    });
                    if !still_faulty {
                        ok = false;
                    }
                }
                if !ok {
                    break;
                }
                time.sleep(POLL_INTERVAL).await.ok();
            }
            ok
        };
        assert_always!(
            held,
            "corpus: a chunk with no clean copy is never fabricated",
            { "mask" => mask }
        );
        self.repaired_clean = converged && repaired && (mask != 0 || self.rot_live_node0);
        self.unassemblable_held = held && !unassemblable.is_empty();
        drop(clients);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        assert_sometimes!(
            self.repaired_clean,
            "corpus: a chunk mask heals through per-chunk repair"
        );
        assert_sometimes!(
            self.unassemblable_held,
            "corpus: an unassemblable chunk is correctly left faulty"
        );
        Ok(())
    }
}
