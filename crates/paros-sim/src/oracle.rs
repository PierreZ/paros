//! The oracle harness: invariants that read the simulation trace.
//!
//! - [`TimelineRecorder`] reconstructs the animation [`RunResult`] from the
//!   standard `client_*` events (the wasm demo and native runner consume it).
//! - [`ClientLivenessOracle`] wires the `assert_*` contract macros off the same
//!   event stream — a worked example of moonpool's oracle harness.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{
    Invariant, SIM_FAULT_EVENT_NAME, TraceEvent, TraceQuery, assert_always, assert_reachable,
    assert_sometimes, assert_sometimes_all, assert_sometimes_each,
};
use paros::{
    EV_APPLIED, EV_BOOTED, EV_CHOSEN, EV_CHOSEN_GAP, EV_COMPACTED, EV_CRASHED,
    EV_ELECTION_TIMEOUT_EXTREME, EV_GAP_FILLED, EV_LEADER, EV_LEADERSHIP_RESIGNED, EV_MSG_RECV,
    EV_MSG_SENT, EV_NODE_STATE, EV_NODE_TICK, EV_PERSIST, EV_PREPARE_BELOW_FLOOR, EV_RECOVERED,
    EV_RESEND_SKIPPED, EV_SEND_DROPPED, EV_SNAPSHOT_INSTALLED, EV_SNAPSHOT_MID_ELECTION,
    EV_SNAPSHOT_OFFERED, EV_SYNCED,
};
use serde::Serialize;

/// Standard transport-client observability events (same names as moonpool's
/// transport workloads, so tooling is workload-agnostic).
const EV_ISSUED: &str = "client_issued";
const EV_ACKED: &str = "client_acknowledged";
const EV_FAILED: &str = "client_failed";
/// Client read events — the history the [`LinearizabilityOracle`] checks.
const EV_READ_ISSUED: &str = "client_read_issued";
const EV_READ_ACKED: &str = "client_read_acknowledged";
const EV_READ_FAILED: &str = "client_read_failed";
/// Per-run client workload mode (`"sequential"` / `"pipelined"` / `"quiet"`),
/// emitted once by `paros_sim::workload::ProposeClient` so
/// [`LinearizabilityOracle`] can tell which per-seq ordering guarantee this run's
/// history satisfies. `"quiet"` is a single proposal, so its history is trivially
/// sequential and takes the same checks.
const EV_WORKLOAD_MODE: &str = "client_workload_mode";

/// Node A — the client.
const NODE_A: u8 = 0;
/// Node B — the contacted paros node.
const NODE_B: u8 = 1;
/// Minimum displayed flight time for a dropped leg, so it is always visible.
const MIN_DROP_SPAN_MS: u64 = 50;

/// How a leg of a round trip resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// Delivered over the network.
    Delivered,
    /// Timed out / never acknowledged.
    Dropped,
}

/// One message leg crossing the simulated network.
#[derive(Debug, Clone, Serialize)]
pub struct Shot {
    /// Workload client this leg belongs to (0 when a single client runs).
    pub client: u64,
    /// Request sequence number this leg belongs to (unique per client only).
    pub seq: u64,
    /// Node that sent this message (0 = A/client, 1 = B/node).
    pub from: u8,
    /// Node the message travels to (0 = A/client, 1 = B/node).
    pub to: u8,
    /// Simulated time the message left `from`, in milliseconds.
    pub depart_ms: u64,
    /// Simulated time the message reached `to`, in milliseconds.
    pub arrive_ms: u64,
    /// In-flight latency, in milliseconds.
    pub latency_ms: u64,
    /// Whether this leg was delivered or dropped.
    pub outcome: Outcome,
}

/// One inter-node Paxos message (Prepare/Promise/Accept/Accepted/Nack/Commit),
/// reconstructed by pairing a send with its matching receive. This is the
/// protocol timeline the single-decree visualization animates — distinct from the
/// client-level [`Shot`] above (whose `from`/`to` of 0/1 mean client/node).
#[derive(Debug, Clone, Serialize)]
pub struct ProtocolShot {
    /// Message kind: `prepare`, `promise`, `accept`, `accepted`, `nack`, `commit`.
    pub kind: String,
    /// Ballot round this message carries.
    pub bround: u64,
    /// Ballot node (proposer) this message carries.
    pub bnode: u64,
    /// Log slot this message concerns (always 0 in single-decree).
    pub slot: u64,
    /// Sending node id (`0..CLUSTER_SIZE`).
    pub from: u8,
    /// Receiving node id (`0..CLUSTER_SIZE`).
    pub to: u8,
    /// Simulated time the message left `from`, in milliseconds.
    pub depart_ms: u64,
    /// Simulated time it reached `to` (synthesized for a drop), in milliseconds.
    pub arrive_ms: u64,
    /// In-flight latency, in milliseconds.
    pub latency_ms: u64,
    /// Whether a matching receive was found (delivered) or not (dropped).
    pub outcome: Outcome,
}

/// A snapshot of one node's durable state at a point in time, from `node_state`
/// events. Drives the per-node promised-ballot label and accepted-value marker.
#[derive(Debug, Clone, Serialize)]
pub struct NodeStateShot {
    /// Simulated time this state was observed, in milliseconds.
    pub time_ms: u64,
    /// The node whose state this is.
    pub node: u64,
    /// Promised-ballot round.
    pub pround: u64,
    /// Promised-ballot node (proposer).
    pub pbnode: u64,
    /// Whether the node has an accepted value (slot 0).
    pub has_accepted: bool,
    /// Accepted-ballot round (meaningful only when `has_accepted`).
    pub around: u64,
    /// Accepted-ballot node (meaningful only when `has_accepted`).
    pub abnode: u64,
    /// Hash of the accepted value (meaningful only when `has_accepted`).
    pub vhash: u64,
}

/// A "this node learned a chosen value" marker, from `value_chosen` events.
/// Drives the chosen glow.
#[derive(Debug, Clone, Serialize)]
pub struct ChosenShot {
    /// Simulated time the value was learned, in milliseconds.
    pub time_ms: u64,
    /// The node that applied the chosen value.
    pub node: u64,
    /// The slot that was chosen (always 0 in single-decree).
    pub slot: u64,
    /// Hash of the chosen value.
    pub vhash: u64,
}

/// A "this node became leader" marker, from `leader_elected` events. Drives the
/// leader badge and the multi-decree election animation.
#[derive(Debug, Clone, Serialize)]
pub struct LeaderShot {
    /// Simulated time the node took leadership, in milliseconds.
    pub time_ms: u64,
    /// The node that became leader.
    pub node: u64,
    /// The ballot round it leads at.
    pub round: u64,
}

/// A "this node advanced its applied (contiguous chosen) prefix" marker, from
/// `log_applied` events. Drives the per-node committed-prefix boundary.
#[derive(Debug, Clone, Serialize)]
pub struct AppliedShot {
    /// Simulated time the slot was applied, in milliseconds.
    pub time_ms: u64,
    /// The node that applied the slot.
    pub node: u64,
    /// The slot just applied (the applied prefix grows one slot at a time).
    pub slot: u64,
}

/// A "this node crashed at a durability seam" marker, from `crashed` events (a
/// `buggify`-injected seam crash). Drives the crash icon and pins the animation's
/// persist/send-seam marker to the node that died on it.
#[derive(Debug, Clone, Serialize)]
pub struct CrashShot {
    /// Simulated time the node crashed, in milliseconds.
    pub time_ms: u64,
    /// The node that crashed.
    pub node: u64,
    /// Which seam it died on: `"before_sync"` (whole un-synced batch lost) or
    /// `"after_sync_before_send"` (durable, but the batch's messages never left).
    pub seam: String,
}

/// A "this node came back up" marker: a boot that follows an earlier boot of the
/// same node (its first boot is the initial start, not a restart). Derived from
/// `booted` events. Drives the node re-lighting and rejoining after a crash gap.
#[derive(Debug, Clone, Serialize)]
pub struct RestartShot {
    /// Simulated time the node restarted, in milliseconds.
    pub time_ms: u64,
    /// The node that restarted.
    pub node: u64,
}

/// A "this node flushed a `Ready` batch" marker, from `synced` events. Drives the
/// persist/send-seam tick: filled when the batch was fsync'd (`sync`), hollow for
/// a relaxed (chosen-index-only) write.
#[derive(Debug, Clone, Serialize)]
pub struct SyncShot {
    /// Simulated time the batch was flushed, in milliseconds.
    pub time_ms: u64,
    /// The node that flushed.
    pub node: u64,
    /// Whether the batch required an fsync-before-send (`MustSync::Sync`).
    pub sync: bool,
    /// Number of write ops in the batch.
    pub writes: u64,
}

/// A "this node read a durable accepted record back on (re)boot" marker, from
/// `recovered` events. Drives the durable-state badge that survives the crash gap:
/// the accepted value the node still holds after coming back.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveredShot {
    /// Simulated time the record was recovered (the boot instant), in ms.
    pub time_ms: u64,
    /// The node that recovered the record.
    pub node: u64,
    /// The slot the record belongs to.
    pub slot: u64,
    /// Hash of the recovered accepted value.
    pub vhash: u64,
}

/// The full result of one seeded run: every message leg plus headline counters
/// the UI shows alongside the animation.
#[derive(Debug, Clone, Serialize)]
pub struct RunResult {
    /// The seed this run used.
    pub seed: u64,
    /// Number of paros nodes this seed drew (cluster size is per-seed config).
    pub nodes: usize,
    /// Number of proposals observed.
    pub requests: u32,
    /// Every message leg exchanged, in time order.
    pub shots: Vec<Shot>,
    /// The inter-node Paxos protocol exchange, in send order.
    pub protocol: Vec<ProtocolShot>,
    /// Per-node durable-state snapshots, in observation order.
    pub node_states: Vec<NodeStateShot>,
    /// Chosen-value markers, in observation order.
    pub chosen: Vec<ChosenShot>,
    /// Leadership-takeover markers, in observation order (multi-decree).
    pub leaders: Vec<LeaderShot>,
    /// Applied-prefix advancement markers, in observation order (multi-decree).
    pub applied: Vec<AppliedShot>,
    /// Seam-crash markers (a node died at the persist/send seam), in time order.
    pub crashes: Vec<CrashShot>,
    /// Restart markers (a node came back after a crash), in time order.
    pub restarts: Vec<RestartShot>,
    /// Batch-flush (`MustSync`) markers, in time order — the persist/send seam.
    pub syncs: Vec<SyncShot>,
    /// Durable accepted records read back on (re)boot, in time order — the durable
    /// state that survives a crash.
    pub recovered: Vec<RecoveredShot>,
    /// Proposals that completed successfully.
    pub delivered: u32,
    /// Proposals dropped / timed out.
    pub dropped: u32,
    /// Logical-clock ticks the cluster advanced through.
    pub ticks: u64,
    /// Slowest successful round trip, in simulated milliseconds.
    pub longest_rtt_ms: u64,
    /// Total simulated time elapsed, in milliseconds.
    pub sim_duration_ms: u64,
}

impl RunResult {
    /// An empty result, used only if the run produced no observable events.
    fn empty(seed: u64) -> Self {
        Self {
            seed,
            nodes: 0,
            requests: 0,
            shots: Vec::new(),
            protocol: Vec::new(),
            node_states: Vec::new(),
            chosen: Vec::new(),
            leaders: Vec::new(),
            applied: Vec::new(),
            crashes: Vec::new(),
            restarts: Vec::new(),
            syncs: Vec::new(),
            recovered: Vec::new(),
            delivered: 0,
            dropped: 0,
            ticks: 0,
            longest_rtt_ms: 0,
            sim_duration_ms: 0,
        }
    }
}

/// Raw timeline the recorder accumulates from the trace.
#[derive(Default)]
pub(crate) struct RecorderData {
    /// `((client_id, seq_id), sim_time_ms)` for each issued proposal.
    issued: Vec<((u64, u64), u64)>,
    /// `((client_id, seq_id), sim_time_ms)` for each acknowledged proposal.
    acked: Vec<((u64, u64), u64)>,
    /// `((client_id, seq_id), sim_time_ms)` for each failed proposal.
    failed: Vec<((u64, u64), u64)>,
    /// Number of logical-clock ticks observed.
    ticks: u64,
}

/// Pull `((client_id, seq_id), time_ms)` pairs for every event named `name`.
/// `client_id` defaults to 0 for events predating the multi-client workload.
fn collect_seq(q: &dyn TraceQuery, name: &str) -> Vec<((u64, u64), u64)> {
    q.snapshot(name)
        .into_iter()
        .filter_map(|e| {
            Some((
                (e.u64("client_id").unwrap_or(0), e.u64("seq_id")?),
                e.time_ms,
            ))
        })
        .collect()
}

/// A workload-agnostic recorder. As an [`Invariant`] it sees the whole trace
/// after each step; it snapshots the standard client events + tick count into
/// shared state the driver reads once the run completes.
pub(crate) struct TimelineRecorder {
    data: Arc<Mutex<RecorderData>>,
}

impl TimelineRecorder {
    pub(crate) fn new(data: Arc<Mutex<RecorderData>>) -> Self {
        Self { data }
    }
}

impl Invariant for TimelineRecorder {
    fn name(&self) -> &'static str {
        "timeline_recorder"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let mut d = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        d.issued = collect_seq(q, EV_ISSUED);
        d.acked = collect_seq(q, EV_ACKED);
        d.failed = collect_seq(q, EV_FAILED);
        d.ticks = u64::try_from(q.len(EV_NODE_TICK)).unwrap_or(u64::MAX);
    }
}

/// One captured inter-node message leg (a send or a receive), before sends and
/// receives are paired into a [`ProtocolShot`]. `from`/`to` are always the
/// sender/receiver node ids, whichever side recorded it.
#[derive(Clone)]
struct RawLeg {
    time_ms: u64,
    from: u8,
    to: u8,
    kind: String,
    bround: u64,
    bnode: u64,
    slot: u64,
}

/// Raw protocol timeline the [`ProtocolRecorder`] accumulates from the trace:
/// the inter-node sends and receives (paired later) plus the node-state and
/// chosen streams (used as-is).
#[derive(Default)]
pub(crate) struct ProtocolData {
    sends: Vec<RawLeg>,
    recvs: Vec<RawLeg>,
    node_states: Vec<NodeStateShot>,
    chosen: Vec<ChosenShot>,
    leaders: Vec<LeaderShot>,
    applied: Vec<AppliedShot>,
    cluster: BTreeSet<u64>,
    snapshots: Vec<(u64, u64, u64)>,
}

/// Pull the ballot-carrying message legs named `name`. `self_field` names the
/// trace field holding *this* leg's own node id (`node` for both sends and
/// receives); `peer_field` names the other endpoint (`to` for sends, `from` for
/// receives). Legs missing the ballot/slot fields (the tick self-events) are
/// skipped, leaving only the six Paxos kinds.
fn collect_legs(q: &dyn TraceQuery, name: &str, self_is_from: bool) -> Vec<RawLeg> {
    q.snapshot(name)
        .into_iter()
        .filter_map(|e| {
            let kind = e.str("kind")?.to_string();
            let this = u8::try_from(e.u64("node")?).ok()?;
            let peer = u8::try_from(e.u64(if self_is_from { "to" } else { "from" })?).ok()?;
            let (from, to) = if self_is_from {
                (this, peer)
            } else {
                (peer, this)
            };
            Some(RawLeg {
                time_ms: e.time_ms,
                from,
                to,
                kind,
                bround: e.u64("bround")?,
                bnode: e.u64("bnode")?,
                slot: e.u64("slot")?,
            })
        })
        .collect()
}

/// Pull the per-node durable-state snapshots from the `node_state` stream.
fn collect_node_states(q: &dyn TraceQuery) -> Vec<NodeStateShot> {
    q.snapshot(EV_NODE_STATE)
        .into_iter()
        .filter_map(|e| {
            let has_accepted = e.bool("has_accepted").unwrap_or(false);
            Some(NodeStateShot {
                time_ms: e.time_ms,
                node: e.u64("node")?,
                pround: e.u64("pround")?,
                pbnode: e.u64("pbnode")?,
                has_accepted,
                around: if has_accepted { e.u64("around")? } else { 0 },
                abnode: if has_accepted { e.u64("abnode")? } else { 0 },
                vhash: if has_accepted { e.u64("vhash")? } else { 0 },
            })
        })
        .collect()
}

/// Pull the chosen-value markers from the `value_chosen` stream.
fn collect_chosen(q: &dyn TraceQuery) -> Vec<ChosenShot> {
    q.snapshot(EV_CHOSEN)
        .into_iter()
        .filter_map(|e| {
            Some(ChosenShot {
                time_ms: e.time_ms,
                node: e.u64("node")?,
                slot: e.u64("slot")?,
                vhash: e.u64("vhash")?,
            })
        })
        .collect()
}

/// Pull the leadership-takeover markers from the `leader_elected` stream.
fn collect_leaders(q: &dyn TraceQuery) -> Vec<LeaderShot> {
    q.snapshot(EV_LEADER)
        .into_iter()
        .filter_map(|e| {
            Some(LeaderShot {
                time_ms: e.time_ms,
                node: e.u64("node")?,
                round: e.u64("round")?,
            })
        })
        .collect()
}

/// Pull the applied-prefix advancement markers from the `log_applied` stream.
fn collect_applied(q: &dyn TraceQuery) -> Vec<AppliedShot> {
    q.snapshot(EV_APPLIED)
        .into_iter()
        .filter_map(|e| {
            Some(AppliedShot {
                time_ms: e.time_ms,
                node: e.u64("node")?,
                slot: e.u64("slot")?,
            })
        })
        .collect()
}

/// Pull the seam-crash markers from the `crashed` stream.
fn collect_crashes(q: &dyn TraceQuery) -> Vec<CrashShot> {
    q.snapshot(EV_CRASHED)
        .into_iter()
        .filter_map(|e| {
            Some(CrashShot {
                time_ms: e.time_ms,
                node: e.u64("node")?,
                seam: e.str("seam").unwrap_or("unknown").to_string(),
            })
        })
        .collect()
}

/// Derive the restart markers from the `booted` stream: a node's *first* boot is
/// its initial start, so only the second and later boots are restarts. Both
/// streams arrive in capture (time) order, so a per-node "seen once" set suffices.
fn collect_restarts(q: &dyn TraceQuery) -> Vec<RestartShot> {
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    q.snapshot(EV_BOOTED)
        .into_iter()
        .filter_map(|e| {
            let node = e.u64("node")?;
            if seen.insert(node) {
                return None; // first boot for this node — not a restart
            }
            Some(RestartShot {
                time_ms: e.time_ms,
                node,
            })
        })
        .collect()
}

/// Pull the batch-flush (`MustSync`) markers from the `synced` stream.
fn collect_syncs(q: &dyn TraceQuery) -> Vec<SyncShot> {
    q.snapshot(EV_SYNCED)
        .into_iter()
        .filter_map(|e| {
            Some(SyncShot {
                time_ms: e.time_ms,
                node: e.u64("node")?,
                sync: e.bool("sync").unwrap_or(false),
                writes: e.u64("writes").unwrap_or(0),
            })
        })
        .collect()
}

/// Pull the durable accepted records read back on (re)boot from the `recovered`
/// stream.
fn collect_recovered(q: &dyn TraceQuery) -> Vec<RecoveredShot> {
    q.snapshot(EV_RECOVERED)
        .into_iter()
        .filter_map(|e| {
            Some(RecoveredShot {
                time_ms: e.time_ms,
                node: e.u64("node")?,
                slot: e.u64("slot")?,
                vhash: e.u64("vhash")?,
            })
        })
        .collect()
}

/// Raw recovery timeline the [`RecoveryRecorder`] accumulates: the crash/restart
/// events, the per-batch flush (`MustSync`) markers, and the durable records read
/// back on (re)boot. All four are used as-is by `build_result`.
#[derive(Default)]
pub(crate) struct RecoveryData {
    crashes: Vec<CrashShot>,
    restarts: Vec<RestartShot>,
    syncs: Vec<SyncShot>,
    recovered: Vec<RecoveredShot>,
}

/// The recovery-timeline recorder: mirrors [`ProtocolRecorder`], but captures the
/// durability streams the crash/recovery visualization needs — seam crashes,
/// restarts, batch flushes, and durable read-backs. Kept separate from the
/// protocol recorder so each recorder owns one concern.
pub(crate) struct RecoveryRecorder {
    data: Arc<Mutex<RecoveryData>>,
}

impl RecoveryRecorder {
    pub(crate) fn new(data: Arc<Mutex<RecoveryData>>) -> Self {
        Self { data }
    }
}

impl Invariant for RecoveryRecorder {
    fn name(&self) -> &'static str {
        "recovery_recorder"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let mut d = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        d.crashes = collect_crashes(q);
        d.restarts = collect_restarts(q);
        d.syncs = collect_syncs(q);
        d.recovered = collect_recovered(q);
    }
}

/// The protocol-timeline recorder: mirrors [`TimelineRecorder`], but captures the
/// inter-node Paxos messages and the node-state / chosen streams the single-decree
/// visualization needs (the client recorder above stays focused on client events).
pub(crate) struct ProtocolRecorder {
    data: Arc<Mutex<ProtocolData>>,
}

impl ProtocolRecorder {
    pub(crate) fn new(data: Arc<Mutex<ProtocolData>>) -> Self {
        Self { data }
    }
}

impl Invariant for ProtocolRecorder {
    fn name(&self) -> &'static str {
        "protocol_recorder"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let mut d = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        d.sends = collect_legs(q, EV_MSG_SENT, true);
        d.recvs = collect_legs(q, EV_MSG_RECV, false);
        d.node_states = collect_node_states(q);
        d.chosen = collect_chosen(q);
        d.leaders = collect_leaders(q);
        d.applied = collect_applied(q);
        d.cluster = q
            .snapshot(EV_BOOTED)
            .iter()
            .filter_map(|event| event.u64("node"))
            .collect();
        d.snapshots = collect_snapshots(q);
    }
}

/// Assert convergence from the completed deterministic run, when no future
/// leader change can invalidate a provisional quiescence decision.
pub(crate) fn assert_final_convergence(data: &ProtocolData) {
    let mut prefixes: BTreeMap<u64, u64> = BTreeMap::new();
    for applied in &data.applied {
        prefixes
            .entry(applied.node)
            .and_modify(|prefix| *prefix = (*prefix).max(applied.slot))
            .or_insert(applied.slot);
    }
    for &(_, node, chosen_index) in &data.snapshots {
        prefixes
            .entry(node)
            .and_modify(|prefix| *prefix = (*prefix).max(chosen_index))
            .or_insert(chosen_index);
    }
    let Some(cluster_max) = prefixes.values().copied().max() else {
        return;
    };
    for node in &data.cluster {
        assert_eq!(
            prefixes.get(node).copied(),
            Some(cluster_max),
            "every node converges to the cluster's chosen prefix at the end of the settle tail"
        );
    }
}

/// Pair each send with the earliest unmatched receive sharing its route, in send
/// order. A paired send is `Delivered` (its receive's time is the arrival); an
/// unpaired send is one the network `Dropped`. Deterministic: the trace is
/// captured in deterministic order and the pairing is a stable FIFO over it.
#[allow(clippy::type_complexity)]
fn build_protocol(
    data: &ProtocolData,
) -> (
    Vec<ProtocolShot>,
    Vec<NodeStateShot>,
    Vec<ChosenShot>,
    Vec<LeaderShot>,
    Vec<AppliedShot>,
) {
    let mut sends: Vec<&RawLeg> = data.sends.iter().collect();
    sends.sort_by_key(|s| s.time_ms); // stable: ties keep capture order

    let mut recv_used = vec![false; data.recvs.len()];
    let mut protocol = Vec::with_capacity(sends.len());

    for s in sends {
        let matched = data.recvs.iter().enumerate().find(|(i, r)| {
            !recv_used[*i]
                && r.from == s.from
                && r.to == s.to
                && r.kind == s.kind
                && r.bround == s.bround
                && r.bnode == s.bnode
                && r.slot == s.slot
                && r.time_ms >= s.time_ms
        });

        let (outcome, arrive_ms) = match matched {
            Some((i, r)) => {
                recv_used[i] = true;
                (Outcome::Delivered, r.time_ms)
            }
            None => (Outcome::Dropped, s.time_ms.saturating_add(MIN_DROP_SPAN_MS)),
        };

        protocol.push(ProtocolShot {
            kind: s.kind.clone(),
            bround: s.bround,
            bnode: s.bnode,
            slot: s.slot,
            from: s.from,
            to: s.to,
            depart_ms: s.time_ms,
            arrive_ms,
            latency_ms: arrive_ms.saturating_sub(s.time_ms),
            outcome,
        });
    }

    (
        protocol,
        data.node_states.clone(),
        data.chosen.clone(),
        data.leaders.clone(),
        data.applied.clone(),
    )
}

/// Liveness oracle: wires the `assert_*` contract macros off the standard client
/// event stream. A worked example of the oracle harness — safety oracles for the
/// real protocol arrive in Stage 2.
pub(crate) struct ClientLivenessOracle;

impl Invariant for ClientLivenessOracle {
    fn name(&self) -> &'static str {
        "client_liveness"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let issued = q.len(EV_ISSUED);
        let acked = q.len(EV_ACKED);
        let failed = q.len(EV_FAILED);

        // A terminal event is only ever recorded for a proposal that was issued.
        assert_always!(
            acked + failed <= issued,
            "no proposal is acked/failed before it is issued"
        );
        // With no chaos a proposal does come back — a "sometimes" + "reachable".
        assert_sometimes!(acked > 0, "at least one proposal is acknowledged");
        if acked > 0 {
            assert_reachable!("a client proposal is acknowledged");
        }
    }
}

/// This run's per-client workload modes (`paros_sim::workload::ProposeClient`
/// draws one per client instance, from the sim config RNG). Sometimes-gated
/// here so the sweep proves every mode rotates. Returns the set of clients
/// that drew `Pipelined` (no per-client program order to linearize against).
fn observe_workload_mode(q: &dyn TraceQuery) -> BTreeSet<u64> {
    let mode_events = q.snapshot(EV_WORKLOAD_MODE);
    let sequential = mode_events
        .iter()
        .any(|e| e.str("mode") == Some("sequential"));
    let pipelined_clients: BTreeSet<u64> = mode_events
        .iter()
        .filter(|e| e.str("mode") == Some("pipelined"))
        .map(|e| e.u64("client_id").unwrap_or(0))
        .collect();
    let pipelined = !pipelined_clients.is_empty();
    assert_sometimes!(sequential, "a run uses the sequential client workload mode");
    if sequential {
        assert_reachable!("a run uses the sequential client workload mode");
    }
    assert_sometimes!(pipelined, "a run uses the pipelined client workload mode");
    if pipelined {
        assert_reachable!("a run uses the pipelined client workload mode");
    }
    // The one-decision-then-idle mode: the only run shape whose chosen prefix
    // stops at slot 0, and therefore the only one in which an empty prefix can be
    // told apart from a prefix of exactly slot 0 (see [`ConvergenceOracle`]).
    let quiet = mode_events.iter().any(|e| e.str("mode") == Some("quiet"));
    assert_sometimes!(quiet, "a run uses the quiet single-decision workload mode");
    if quiet {
        assert_reachable!("a cluster decides one slot and then idles");
    }
    pipelined_clients
}

/// One committed operation's real-time span: first issue to first committed
/// ack, in simulated milliseconds. Two spans sharing a boundary millisecond are
/// treated as *concurrent* (no precedence edge), which can only drop — never
/// fabricate — a real-time constraint, so the checker stays sound at trace
/// granularity.
#[derive(Clone, Copy)]
struct OpSpan {
    inv: u64,
    resp: u64,
}

impl OpSpan {
    fn before(self, other: OpSpan) -> bool {
        self.resp < other.inv
    }
}

/// Cap on the committed-operation history the interval checker walks pairwise.
/// The current workloads stay far below it (a few dozen operations per run);
/// the cap only bounds the `O(n^2)` walk if a future workload explodes.
const LIN_HISTORY_CAP: usize = 512;

/// The committed client history, keyed by `(client_id, seq_id)`. A watermark is
/// `Option<u64>`: an absent `read_index` field is the empty applied prefix, and
/// `None < Some(0)` is exactly the watermark order.
struct LinHistory {
    /// Acked writes with a known slot (program order within one client).
    write_slot: BTreeMap<(u64, u64), u64>,
    /// Committed reads and their observed watermark.
    read_wm: BTreeMap<(u64, u64), Option<u64>>,
    /// Committed writes as real-time spans with their slot (`None` for a
    /// defensive slotless ack, which still forbids the empty prefix later).
    writes: Vec<(OpSpan, Option<u64>)>,
    /// Committed reads as real-time spans with their watermark.
    reads: Vec<(OpSpan, Option<u64>)>,
}

/// Fold the client trace into a [`LinHistory`]. First issue and first
/// committed ack win when an event repeats.
fn collect_lin_history(q: &dyn TraceQuery) -> LinHistory {
    let keyed = |e: &TraceEvent| Some((e.u64("client_id").unwrap_or(0), e.u64("seq_id")?));
    let mut write_slot: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    let mut write_resp: BTreeMap<(u64, u64), (u64, Option<u64>)> = BTreeMap::new();
    for e in q.snapshot(EV_ACKED) {
        let Some(key) = keyed(&e) else { continue };
        write_resp.entry(key).or_insert((e.time_ms, e.u64("slot")));
        if let Some(slot) = e.u64("slot") {
            write_slot.entry(key).or_insert(slot);
        }
    }
    let mut read_wm: BTreeMap<(u64, u64), Option<u64>> = BTreeMap::new();
    let mut read_resp: BTreeMap<(u64, u64), (u64, Option<u64>)> = BTreeMap::new();
    for e in q.snapshot(EV_READ_ACKED) {
        let Some(key) = keyed(&e) else { continue };
        read_wm.entry(key).or_insert(e.u64("read_index"));
        read_resp
            .entry(key)
            .or_insert((e.time_ms, e.u64("read_index")));
    }
    let mut write_inv: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for e in q.snapshot(EV_ISSUED) {
        let Some(key) = keyed(&e) else { continue };
        write_inv.entry(key).or_insert(e.time_ms);
    }
    let mut read_inv: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for e in q.snapshot(EV_READ_ISSUED) {
        let Some(key) = keyed(&e) else { continue };
        read_inv.entry(key).or_insert(e.time_ms);
    }
    let writes = write_resp
        .iter()
        .filter_map(|(key, &(resp, slot))| {
            let inv = *write_inv.get(key)?;
            Some((OpSpan { inv, resp }, slot))
        })
        .collect();
    let reads = read_resp
        .iter()
        .filter_map(|(key, &(resp, wm))| {
            let inv = *read_inv.get(key)?;
            Some((OpSpan { inv, resp }, wm))
        })
        .collect();
    LinHistory {
        write_slot,
        read_wm,
        writes,
        reads,
    }
}

/// The full checker: disclosed-order linearizability over real time. Committed
/// writes pin to their slot, committed reads to their watermark; the induced
/// order is a valid linearization iff it agrees with every real-time
/// precedence edge. Four pairwise checks, complete for this register (see the
/// [`LinearizabilityOracle`] doc), bounded by [`LIN_HISTORY_CAP`].
fn check_disclosed_order(h: &LinHistory) {
    if h.writes.len() + h.reads.len() > LIN_HISTORY_CAP {
        return;
    }
    // L1 — the log order of two committed writes agrees with their real-time
    // order.
    for (i, &(w1, s1)) in h.writes.iter().enumerate() {
        for &(w2, s2) in &h.writes[i + 1..] {
            let (Some(s1), Some(s2)) = (s1, s2) else {
                continue;
            };
            if w1.before(w2) {
                assert_always!(
                    s1 < s2,
                    "two real-time-ordered committed writes land in log order"
                );
            } else if w2.before(w1) {
                assert_always!(
                    s2 < s1,
                    "two real-time-ordered committed writes land in log order"
                );
            }
        }
    }
    // L2 — a committed read observes every write that completed before it
    // began (a slotless committed ack still forbids the empty prefix). L3 — a
    // write invoked after a committed read lands above that read's watermark.
    for &(r, wm) in &h.reads {
        for &(w, slot) in &h.writes {
            if w.before(r) {
                let observed = match slot {
                    Some(s) => wm >= Some(s),
                    None => wm.is_some(),
                };
                assert_always!(
                    observed,
                    "a committed read observes every write completed before it began"
                );
            } else if r.before(w)
                && let Some(s) = slot
            {
                assert_always!(
                    Some(s) > wm,
                    "a write invoked after a committed read lands above its watermark"
                );
            }
        }
    }
    // L4 — watermarks of real-time-ordered committed reads never move
    // backwards.
    for (i, &(r1, wm1)) in h.reads.iter().enumerate() {
        for &(r2, wm2) in &h.reads[i + 1..] {
            if r1.before(r2) {
                assert_always!(
                    wm2 >= wm1,
                    "real-time-ordered committed reads observe monotone watermarks"
                );
            } else if r2.before(r1) {
                assert_always!(
                    wm1 >= wm2,
                    "real-time-ordered committed reads observe monotone watermarks"
                );
            }
        }
    }
}

/// The sequential fast path for one non-pipelined client: program order (seq)
/// is real-time order within the client even where timestamps tie, so C1-C3
/// are strictly stronger than the interval checks for its operations.
fn check_sequential_client(client: u64, h: &LinHistory) {
    let span = (client, 0)..=(client, u64::MAX);
    // C1 — a committed read observes every write acked before it began: read
    // `k` starts after write `j`'s ack for every `j <= k`, so its watermark
    // covers the running max acked slot (two-pointer over seq).
    let mut max_acked_slot: Option<u64> = None;
    let mut writes = h.write_slot.range(span.clone()).peekable();
    for (&(_, rk), &wm) in h.read_wm.range(span.clone()) {
        while let Some(&(&(_, wj), &slot)) = writes.peek() {
            if wj > rk {
                break;
            }
            max_acked_slot = max_acked_slot.max(Some(slot));
            writes.next();
        }
        assert_always!(
            wm >= max_acked_slot,
            "a committed read's watermark covers every write acked before it began"
        );
    }

    // C2 — this client's reads do not overlap, so their watermarks never move
    // backwards.
    let mut prev: Option<u64> = None;
    for (_, &wm) in h.read_wm.range(span.clone()) {
        assert_always!(wm >= prev, "committed-read watermarks never move backwards");
        prev = prev.max(wm);
    }

    // C3 — a write issued after a committed read must land above that read's
    // watermark (a slot at or below it would place the write inside the prefix
    // the read already observed). Guards against an inflated / speculative
    // watermark.
    let mut max_read_wm: Option<u64> = None;
    let mut reads = h.read_wm.range(span.clone()).peekable();
    for (&(_, wj), &slot) in h.write_slot.range(span) {
        while let Some(&(&(_, rk), &wm)) = reads.peek() {
            if rk >= wj {
                break;
            }
            max_read_wm = max_read_wm.max(wm);
            reads.next();
        }
        if let Some(i) = max_read_wm {
            assert_always!(
                slot > i,
                "a write issued after a committed read lands above its watermark"
            );
        }
    }
}

/// The linearizability oracle — the client-history checker (the king oracle:
/// every later stage asserts client-observed correctness through it). The
/// register under check is the **applied log prefix**: an acked write is a
/// state transition at its committed `slot`, and a committed read observes the
/// watermark `read_index`.
///
/// **The full checker (multi-client, pipelined included).** A Wing & Gong /
/// Porcupine search backtracks over candidate linearization orders; here the
/// consensus log *discloses* every linearization point — a committed write
/// linearizes at its slot, a committed read at its watermark — so the search
/// collapses to its verification half: the disclosed order is a valid
/// linearization iff it is consistent with real-time. That is four pairwise
/// interval checks over committed operations (see `observe`), valid for any
/// number of concurrent clients and any per-client mode, bounded by
/// [`LIN_HISTORY_CAP`]. Precedence across operations comes from event
/// timestamps, with same-millisecond boundaries treated as concurrent.
///
/// **The sequential fast path.** A client in a non-pipelined mode issues each
/// op only after the previous op's terminal event, so within that client
/// program order (seq numbers) is real-time order even when the events share a
/// millisecond. C1-C3 keep that stronger per-client check, now keyed by
/// `client_id`.
///
/// Failed / timed-out operations enter no constraint: a timed-out write may
/// still commit later, so it is deliberately unconstrained (`Ambiguous`, never
/// assumed aborted). Every *committed* ack does enter the checks, the dedup
/// fast path included: it used to ack with no slot and so fell outside
/// `write_slot` entirely, and that exemption was the hole the early-ack bug
/// lived in (see [`AppliedAckOracle`]). A committed ack that (defensively)
/// names no slot still constrains later reads: their watermark can no longer
/// be the empty prefix.
pub(crate) struct LinearizabilityOracle;

impl Invariant for LinearizabilityOracle {
    fn name(&self) -> &'static str {
        "linearizability"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        // A terminal read event is only ever recorded for a read that was issued.
        let issued = q.len(EV_READ_ISSUED);
        let acked = q.len(EV_READ_ACKED);
        let failed = q.len(EV_READ_FAILED);
        assert_always!(
            acked + failed <= issued,
            "no read is acked/failed before it is issued"
        );

        let h = collect_lin_history(q);
        let pipelined_clients = observe_workload_mode(q);
        check_disclosed_order(&h);
        // --- The sequential fast path, per client: program order is real-time
        // order within a non-pipelined client even where timestamps tie, so
        // C1-C3 stay strictly stronger than L1-L4 for those clients.
        let history_clients: BTreeSet<u64> = h
            .write_slot
            .keys()
            .chain(h.read_wm.keys())
            .map(|&(c, _)| c)
            .collect();
        for &client in &history_clients {
            if !pipelined_clients.contains(&client) {
                check_sequential_client(client, &h);
            }
        }
        // Coverage gates (`UntilCoverageStable` only saturates once these fire).
        let multi_client = history_clients.len() > 1;
        assert_sometimes!(
            multi_client,
            "a run drives concurrent clients against one register"
        );
        if multi_client {
            assert_reachable!("a run drives concurrent clients against one register");
        }
        let concurrent_read_write = h
            .reads
            .iter()
            .any(|&(r, _)| h.writes.iter().any(|&(w, _)| !w.before(r) && !r.before(w)));
        assert_sometimes!(
            concurrent_read_write,
            "a linearizable read commits concurrently with a conflicting write"
        );
        if concurrent_read_write {
            assert_reachable!("a linearizable read commits concurrently with a conflicting write");
        }
        assert_sometimes!(!h.read_wm.is_empty(), "a linearizable read commits");
        if !h.read_wm.is_empty() {
            assert_reachable!("a linearizable read commits");
        }
        let multi_slot = h.read_wm.values().any(|wm| *wm >= Some(1));
        assert_sometimes!(multi_slot, "a committed read observes a multi-slot prefix");
        if multi_slot {
            assert_reachable!("a committed read observes a multi-slot prefix");
        }
        // A read served after leadership changed hands (the window where a
        // naive local read goes stale): the first `leader_elected` at a round
        // different from the initial one marks the change.
        let mut first_round: Option<u64> = None;
        let mut leader_change_ms: Option<u64> = None;
        for e in q.snapshot(EV_LEADER) {
            let Some(round) = e.u64("round") else {
                continue;
            };
            match first_round {
                None => first_round = Some(round),
                Some(r) if round != r => {
                    leader_change_ms = Some(e.time_ms);
                    break;
                }
                Some(_) => {}
            }
        }
        let read_after_change = leader_change_ms
            .is_some_and(|t| q.snapshot(EV_READ_ACKED).iter().any(|e| e.time_ms > t));
        assert_sometimes!(read_after_change, "a read commits after a leader change");
        if read_after_change {
            assert_reachable!("a read commits after a leader change");
        }
        let retried = q
            .snapshot(EV_READ_ACKED)
            .iter()
            .any(|e| e.u64("attempts").is_some_and(|a| a > 1));
        assert_sometimes!(retried, "a read is retried across nodes before committing");
        if retried {
            assert_reachable!("a read is retried across nodes before committing");
        }
    }
}

/// The ack oracle: **a committed write ack never names a slot the acking node
/// had not already applied.** `committed = true` is the promise that the write
/// is in the register this project defines — the *applied* log prefix — so an
/// ack that outruns the acking node's own apply is a client-visible
/// linearizability violation on its own, with no later read needed to expose
/// it. A subsequent read at the same node legitimately returns a watermark
/// below the "applied" write.
///
/// This is the check the trace could not carry until every committed ack named
/// a slot. The dedup fast path (`paros::ProposeResult::Chosen`) used to ack with
/// no slot at all, and both the workload and [`LinearizabilityOracle`] were
/// explicitly told to skip slotless acks — an exemption exactly the size of the
/// bug it was hiding. Now the fast path names its slot too, so both this oracle
/// and C1 cover it.
///
/// Two events, joined on `(node, slot)`: `client_acknowledged` carries the slot
/// and the node that answered, `log_applied` carries the node and the slot it
/// just folded into its contiguous prefix. Unlike C1-C3 this holds for *both*
/// workload modes — it is a per-ack local claim, not a program-order argument,
/// so `Pipelined` is checked too.
pub(crate) struct AppliedAckOracle;

impl Invariant for AppliedAckOracle {
    fn name(&self) -> &'static str {
        "ack_after_apply"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        // Earliest time each slot entered each node's applied prefix. First
        // wins: a restart replays `log_applied` for the recovered prefix, and
        // the original apply is the moment that matters.
        let mut applied_at: BTreeMap<(u64, u64), u64> = BTreeMap::new();
        for e in q.snapshot(EV_APPLIED) {
            let (Some(node), Some(idx)) = (e.u64("node"), e.u64("applied_index")) else {
                continue;
            };
            applied_at.entry((node, idx)).or_insert(e.time_ms);
        }
        let mut checked = 0_usize;
        for e in q.snapshot(EV_ACKED) {
            // A failed/slotless ack constrains nothing (and after the
            // un-blindfold a committed one always carries both fields).
            let (Some(slot), Some(node)) = (e.u64("slot"), e.u64("node")) else {
                continue;
            };
            checked += 1;
            let applied_first = applied_at
                .get(&(node, slot))
                .is_some_and(|t| *t <= e.time_ms);
            assert_always!(
                applied_first,
                "a committed write ack names a slot the acking node had already applied"
            );
        }
        // Coverage gate: the join above actually had acks to check.
        assert_sometimes!(
            checked > 0,
            "a committed write ack is checked against the acking node's applied prefix"
        );
        if checked > 0 {
            assert_reachable!(
                "a committed write ack is checked against the acking node's applied prefix"
            );
        }
    }
}

/// The Paxos safety oracle — the heart of the project. Reads the driver's
/// protocol events ([`EV_CHOSEN`], [`EV_NODE_STATE`], [`EV_PERSIST`],
/// [`EV_MSG_SENT`]) and asserts the single-decree safety invariants on every
/// step: at most one value chosen per slot, a monotone promised ballot, never an
/// accept above the promise, and — the two the #67 arc added — at most one
/// command *proposed* per `(ballot, slot)` and at most one *accepted* per
/// `(slot, ballot)`.
pub(crate) struct SafetyOracle;

impl Invariant for SafetyOracle {
    fn name(&self) -> &'static str {
        "paxos_safety"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        // Invariant 1 (the crown jewel): at most one value is ever chosen per
        // slot — across the whole cluster.
        let mut chosen_value: BTreeMap<u64, u64> = BTreeMap::new();
        let mut any_chosen = false;
        for e in q.snapshot(EV_CHOSEN) {
            let (Some(slot), Some(vhash)) = (e.u64("slot"), e.u64("vhash")) else {
                continue;
            };
            any_chosen = true;
            if let Some(prev) = chosen_value.insert(slot, vhash) {
                assert_always!(prev == vhash, "at most one value is ever chosen for a slot");
            }
        }
        // Liveness reachability: a value does get chosen (gates `UntilCoverageStable`).
        assert_sometimes!(any_chosen, "a value is eventually chosen");
        if any_chosen {
            assert_reachable!("a value is chosen");
        }

        // Invariant 2 (per-node, in capture/time order): a node's promised ballot
        // is monotonic — it never decreases, including across a restart (the boot
        // re-emits the recovered promise as a `node_state`).
        let mut last_promised: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
        for e in q.snapshot(EV_NODE_STATE) {
            let Some(node) = e.u64("node") else { continue };
            let (Some(pr), Some(pn)) = (e.u64("pround"), e.u64("pbnode")) else {
                continue;
            };
            let promised = (pr, pn);
            if let Some(prev) = last_promised.insert(node, promised) {
                assert_always!(promised >= prev, "a node's promised ballot never decreases");
            }
        }

        // Invariant 3 (per-slot, across the whole log — not just slot 0): a node
        // never persists an accept above the ballot it has promised. Each `persist`
        // event carries the accepted ballot and the node's promised ballot at the
        // time of the write.
        for e in q.snapshot(EV_PERSIST) {
            let (Some(ar), Some(an)) = (e.u64("around"), e.u64("abnode")) else {
                continue;
            };
            let (Some(pr), Some(pn)) = (e.u64("pround"), e.u64("pbnode")) else {
                continue;
            };
            assert_always!(
                (ar, an) <= (pr, pn),
                "a node's accepted ballot never exceeds its promised ballot"
            );
        }

        // Invariant 4 (the Phase-2 half of P2b, checked *on the wire*): a ballot
        // proposes at most one command per slot. A ballot names its own proposer
        // (`Ballot.node`), so exactly one node ever sends `Accept`s at it, and that
        // node holds one `Proposing` per slot — two different commands under one
        // `(ballot, slot)` means the proposer allocated a slot it already had in
        // flight, which destroys the highest-ballot-per-slot value selection a new
        // leader's Phase 1 depends on (two records at the *same* ballot, no rule to
        // pick between them).
        //
        // This is the check #67's arc needs and the only one that can see it: the
        // anomaly is upstream of `persist` and `value_chosen` alike, so a
        // double-allocation an acceptor quorum happens to reject leaves no trace at
        // all in Invariants 1-3. Reading `msg_sent` rather than `msg_received` is
        // deliberate — it indicts the *proposer*, not the network.
        let mut proposed: BTreeMap<(u64, u64, u64), u64> = BTreeMap::new();
        for e in q.snapshot(EV_MSG_SENT) {
            if e.str("kind") != Some("accept") {
                continue;
            }
            let (Some(br), Some(bn), Some(slot), Some(vhash)) = (
                e.u64("bround"),
                e.u64("bnode"),
                e.u64("slot"),
                e.u64("vhash"),
            ) else {
                continue;
            };
            if let Some(prev) = proposed.insert((br, bn, slot), vhash) {
                assert_always!(
                    prev == vhash,
                    "one ballot proposes at most one command for a slot"
                );
            }
        }
        // Coverage gate: the check above is only as good as the field it reads.
        // An `accept` whose `vhash` never made it onto the trace is skipped
        // silently by the destructuring above, and the invariant would be
        // vacuously true forever. Saturation has to see it fire.
        assert_sometimes!(
            !proposed.is_empty(),
            "a proposed command is checked against its ballot's other proposals"
        );
        if !proposed.is_empty() {
            assert_reachable!("a proposed command is checked against its ballot's other proposals");
        }

        // Invariant 5 (the durable mirror of 4): at most one command is ever
        // *accepted* for one `(slot, ballot)` anywhere in the cluster. A chosen
        // value is re-recorded at its choosing ballot (`mark_chosen`), so the
        // legitimate paths all write the same command per `(slot, ballot)`; two
        // different ones would mean an acceptor quorum had ratified a
        // double-allocation, which is Invariant 4's failure carried all the way to
        // disk.
        let mut accepted: BTreeMap<(u64, u64, u64), u64> = BTreeMap::new();
        for e in q.snapshot(EV_PERSIST) {
            let (Some(slot), Some(ar), Some(an), Some(vhash)) = (
                e.u64("slot"),
                e.u64("around"),
                e.u64("abnode"),
                e.u64("vhash"),
            ) else {
                continue;
            };
            if let Some(prev) = accepted.insert((slot, ar, an), vhash) {
                assert_always!(
                    prev == vhash,
                    "at most one command is ever accepted for one (slot, ballot)"
                );
            }
        }
    }
}

/// The truncation events (`compacted`), time-ordered, as `(time_ms, node, first)`
/// where `first` is the new compaction floor (the first slot still retained).
/// `snapshot` yields events in capture (time) order.
fn collect_compactions(q: &dyn TraceQuery) -> Vec<(u64, u64, u64)> {
    q.snapshot(EV_COMPACTED)
        .iter()
        .filter_map(|e| Some((e.time_ms, e.u64("node")?, e.u64("first")?)))
        .collect()
}

/// The snapshot-install events (`snapshot_installed`), time-ordered, as
/// `(time_ms, node, chosen_index)` where `chosen_index` is the commit index the
/// snapshot brought the node up to. Used to admit the applied-index jump an
/// install performs (a below-floor node jumps straight to the chosen prefix) and
/// to prove below-floor recovery is actually exercised.
fn collect_snapshots(q: &dyn TraceQuery) -> Vec<(u64, u64, u64)> {
    q.snapshot(EV_SNAPSHOT_INSTALLED)
        .iter()
        .filter_map(|e| Some((e.time_ms, e.u64("node")?, e.u64("chosen_index")?)))
        .collect()
}

/// Recovery oracle (safety only): a node's durable state survives a crash intact.
/// After a restart, the state the core rebuilds from storage must not contradict
/// what the node persisted before the crash — a durable accepted `(slot -> value)`
/// never changes across the restart seam. (The promised-ballot-never-lowers half
/// is covered by [`SafetyOracle`] invariant 2, since the boot re-emits the
/// recovered promise as a `node_state`.) Convergence of a *lagging* restarted node
/// is a Stage-5 concern, not this oracle's.
pub(crate) struct RecoveryOracle;

impl Invariant for RecoveryOracle {
    fn name(&self) -> &'static str {
        "recovery_safety"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        // `persist` records the latest durably-accepted value hash per (node,
        // slot); `recovered` is what a node read back from storage on a (re)boot.
        // A recovered value must match what the node last persisted for that slot
        // before the boot (a synced accept is never lost or altered by a crash).
        //
        // Both streams arrive in capture (time) order, so merge them with a two-
        // pointer walk: before checking a recovery at time `t`, fold in every
        // persist at time `<= t` (same-instant persists come from the pre-crash
        // life; the rebooted life's first persist is strictly later).
        let persists = q.snapshot(EV_PERSIST);
        let recovers = q.snapshot(EV_RECOVERED);
        let mut persisted: BTreeMap<(u64, u64), u64> = BTreeMap::new();
        let mut pi = 0;
        for r in &recovers {
            let (Some(node), Some(slot), Some(vhash)) =
                (r.u64("node"), r.u64("slot"), r.u64("vhash"))
            else {
                continue;
            };
            while pi < persists.len() && persists[pi].time_ms <= r.time_ms {
                let p = &persists[pi];
                if let (Some(pn), Some(ps), Some(pv)) =
                    (p.u64("node"), p.u64("slot"), p.u64("vhash"))
                {
                    persisted.insert((pn, ps), pv);
                }
                pi += 1;
            }
            if let Some(&prev) = persisted.get(&(node, slot)) {
                assert_always!(
                    prev == vhash,
                    "a restart never changes a pre-crash accepted value for a slot"
                );
            }
        }
    }
}

/// No-gaps oracle: each node's applied (contiguous chosen) prefix advances one
/// slot at a time, starting at slot 0 — it never skips a slot. Reads the
/// `log_applied` stream the driver emits as the commit index moves.
///
/// Restart-tolerant: after a crash a node re-drives the apply of its durable
/// committed prefix (the boot re-emits `log_applied` for `first_slot..=chosen_index`),
/// so a node may *replay* already-applied slots (`idx` at or below the frontier).
/// A replay is idempotent and allowed; only a *forward skip* past the frontier is
/// a real gap. The first-ever apply must be slot 0, unless the node has truncated
/// its prefix: a compacted node's boot replay resumes at its compaction floor, so
/// a forward jump is admitted only when it lands exactly on that floor.
pub(crate) struct NoGapsOracle;

impl Invariant for NoGapsOracle {
    fn name(&self) -> &'static str {
        "log_no_gaps"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        // Per node, the frontier is the next slot expected to be newly applied.
        // Track each node's durable compaction floor over time, folding in every
        // truncation at or before the current applied event (two-pointer merge,
        // both streams in capture order), so a legitimate boot-replay jump to the
        // floor is distinguished from a real gap.
        let compactions = collect_compactions(q);
        let snapshots = collect_snapshots(q);
        let mut ci = 0;
        let mut si = 0;
        let mut floor: BTreeMap<u64, u64> = BTreeMap::new();
        // Every slot a node jumped to via a snapshot install; a forward jump to any
        // of them is legal (the folded prefix is in that snapshot). A node can
        // install more than one snapshot in a single drain (two peers each serve
        // it), so this is a set, not just the latest landing.
        let mut snap_landings: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        let mut max_applied = 0_u64;
        for e in q.snapshot(EV_APPLIED) {
            let (Some(node), Some(idx)) = (e.u64("node"), e.u64("applied_index")) else {
                continue;
            };
            while ci < compactions.len() && compactions[ci].0 <= e.time_ms {
                let (_, cn, cf) = compactions[ci];
                let f = floor.entry(cn).or_insert(0);
                *f = (*f).max(cf);
                ci += 1;
            }
            while si < snapshots.len() && snapshots[si].0 <= e.time_ms {
                let (_, sn, sc) = snapshots[si];
                snap_landings.entry(sn).or_default().insert(sc);
                si += 1;
            }
            max_applied = max_applied.max(idx);
            let next = frontier.entry(node).or_insert(0);
            if idx == *next {
                // Advancing the frontier by exactly one (no gap).
                *next += 1;
            } else if idx > *next {
                // A forward jump is only legal at the node's compaction floor (a
                // boot replay of a truncated log resumes there, not at slot 0) or
                // at a snapshot install (the folded prefix jumps straight to the
                // snapshot's chosen index).
                let at_floor = idx == floor.get(&node).copied().unwrap_or(0);
                let at_snapshot = snap_landings
                    .get(&node)
                    .is_some_and(|landings| landings.contains(&idx));
                assert_always!(
                    at_floor || at_snapshot,
                    "a node's applied prefix advances one slot at a time (a forward jump only at the compaction floor or a snapshot install)"
                );
                *next = idx + 1;
            }
            // else idx < *next: an idempotent restart replay, allowed.
        }
        // The log is multi-slot (a stable leader streamed past slot 0).
        assert_sometimes!(max_applied >= 2, "a multi-slot prefix is applied");
        if max_applied >= 2 {
            assert_reachable!("a multi-slot log prefix is applied");
        }
    }
}

/// Leadership oracle: a node's leadership ballots strictly increase (it never
/// becomes leader again at a round at or below one it already led), a fresh
/// leader never holds a promise above the ballot it just won, and elections do
/// happen. Reads the `leader_elected` stream.
///
/// Note: two nodes *can* lead the same round with different ballots (a ballot is
/// `(round, node)`, ordered by node id) under a partition — that is safe, because
/// quorum intersection lets only the higher ballot commit (prefix agreement,
/// asserted by [`SafetyOracle`]). So "≤1 leader per ballot" is structural (the
/// ballot carries the node); what is worth asserting is the genuinely-true
/// per-node monotonicity, which catches a node re-leading at a stale ballot.
///
/// The promise check is #67's detector, and it is deliberately placed at the
/// *instant of victory*: winning means having promised your own campaign ballot
/// and heard nothing higher, so `won >= promised` should be an identity there. It
/// fails only for a Candidate that raised its promise without dropping its
/// campaign — which `mark_chosen` and `on_install_snapshot` both do, being the
/// two promise-raising paths that skip `become_follower` — and then won on a
/// `Promise` that was in flight before the raise. A tick later the same state is
/// no longer distinguishable from a sitting leader legitimately learning a
/// higher-ballot commit, which is why nothing downstream can see it. The
/// consequences (`next_slot` and the read fence both landing below slots the
/// promise quorum reported) are what [`SafetyOracle`]'s per-ballot proposal check
/// and [`LinearizabilityOracle`]'s C1 would catch *if* the stale leader ever got
/// a quorum to answer it.
pub(crate) struct LeadershipOracle;

impl Invariant for LeadershipOracle {
    fn name(&self) -> &'static str {
        "leadership"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let mut last_round: BTreeMap<u64, u64> = BTreeMap::new();
        let mut any = false;
        let mut checked = 0_usize;
        for e in q.snapshot(EV_LEADER) {
            let (Some(node), Some(round)) = (e.u64("node"), e.u64("round")) else {
                continue;
            };
            any = true;
            if let Some(prev) = last_round.insert(node, round) {
                assert_always!(
                    round > prev,
                    "a node's leadership ballots strictly increase"
                );
            }
            // #67: the ballot won vs. the promise held at that instant.
            let (Some(bn), Some(pr), Some(pn)) = (e.u64("bnode"), e.u64("pround"), e.u64("pbnode"))
            else {
                continue;
            };
            checked += 1;
            assert_always!(
                (round, bn) >= (pr, pn),
                "a fresh leader has not promised a ballot above the one it won"
            );
        }
        // Coverage gate on the #67 check itself: it reads three fields the
        // `leader_elected` event has to carry, and a missing one would skip it
        // silently, leaving the invariant vacuously true. Saturation has to see it
        // actually compare something.
        assert_sometimes!(
            checked > 0,
            "a fresh leader's promise is checked against the ballot it won"
        );
        if checked > 0 {
            assert_reachable!("a fresh leader's promise is checked against the ballot it won");
        }
        assert_sometimes!(any, "a leader is elected");
        if any {
            assert_reachable!("a leader is elected");
        }
    }
}

/// Progress / liveness oracle: the dueling-proposer livelock is fixed, so under
/// eventual synchrony a stable leader streams several slots, and under chaos
/// leadership turns over and the cluster recovers. These are `sometimes` +
/// `reachable` gates: the `UntilCoverageStable` sweep only saturates once they
/// fire, so a saturated sweep (no `convergence_timeout`) is the proof of progress.
pub(crate) struct ProgressOracle;

impl Invariant for ProgressOracle {
    fn name(&self) -> &'static str {
        "progress_liveness"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let max_applied = q
            .snapshot(EV_APPLIED)
            .into_iter()
            .filter_map(|e| e.u64("applied_index"))
            .max()
            .unwrap_or(0);
        let rounds: BTreeSet<u64> = q
            .snapshot(EV_LEADER)
            .into_iter()
            .filter_map(|e| e.u64("round"))
            .collect();

        assert_sometimes!(max_applied >= 3, "a stable leader streams several slots");
        if max_applied >= 3 {
            assert_reachable!("the chosen prefix advances under a stable leader");
        }
        assert_sometimes!(
            rounds.len() >= 2,
            "leadership turns over and the cluster recovers"
        );
        if rounds.len() >= 2 {
            assert_reachable!("leadership turns over (re-election)");
        }
    }
}

/// Grace (ms) after chaos ends — and after the cluster's chosen prefix last grew,
/// and after a node's most recent (re)boot — before that node is required to have
/// converged. It must cover a lagging follower's catch-up round trip; below it,
/// follower lag (or a just-rebooted node re-establishing its prefix) is a
/// legitimate transient, not a violation.
const CONVERGENCE_GRACE_MS: u64 = 3_000;

/// Whether the run has genuinely settled, the shared gate every liveness oracle
/// hangs its `assert_always` off: chaos is over, the cluster's chosen prefix has
/// not grown for [`CONVERGENCE_GRACE_MS`] (`prefix_grew_ms` is when it last did),
/// and leadership has not changed hands for just as long (a lagging node can
/// neither pull from nor push to a leader that keeps changing under it).
///
/// Before this gate, lag and holes are legitimate transients and nothing is
/// asserted; after it, they are real liveness failures.
fn quiesced(q: &dyn TraceQuery, sim_time_ms: u64, prefix_grew_ms: u64) -> bool {
    let last_leader_ms = q
        .snapshot(EV_LEADER)
        .iter()
        .map(|e| e.time_ms)
        .max()
        .unwrap_or(0);
    sim_time_ms > crate::CHAOS_DURATION_MS + CONVERGENCE_GRACE_MS
        && sim_time_ms.saturating_sub(prefix_grew_ms) > CONVERGENCE_GRACE_MS
        && sim_time_ms.saturating_sub(last_leader_ms) > CONVERGENCE_GRACE_MS
}

/// How recently a node must have reported a `chosen_gap` for it to count as
/// *still open*. The driver re-emits the event every tick (50 ms) for as long as
/// the gap exists, so ten ticks is a generous "it is still there" window while
/// still excluding one that opened and healed earlier in the settle tail.
const GAP_STILL_OPEN_MS: u64 = 500;

/// The cluster's applied high-water mark and the time it was last raised, from the
/// `log_applied` stream (which arrives in capture/time order). `None` when nothing
/// has been applied anywhere yet.
fn cluster_applied_max(q: &dyn TraceQuery) -> Option<(u64, u64)> {
    let mut max: Option<(u64, u64)> = None;
    for e in q.snapshot(EV_APPLIED) {
        let Some(idx) = e.u64("applied_index") else {
            continue;
        };
        if max.is_none_or(|(m, _)| idx > m) {
            max = Some((idx, e.time_ms));
        }
    }
    max
}

/// Convergence oracle (the #18 liveness deliverable): once chaos has quiesced,
/// every *live* node's chosen prefix catches up to the cluster maximum — the log
/// converges. This is the invariant no prior oracle sees: safety oracles check
/// that nodes never *disagree* on a slot, but a follower that missed both the
/// `Accept` and the `Commit` for a decided slot keeps a permanent **hole** (the
/// leader only re-sends `Accept`s for still-pending slots, and the follower path
/// used to ignore the heartbeat's `commit`). Commit-replay catch-up
/// (`paros_core`) closes that hole; this oracle is what makes the closure
/// observable — red on the unfixed code, green once catch-up lands.
///
/// During the run this oracle records coverage for lag and the hard below-floor
/// snapshot path. The actual liveness assertion runs after the deterministic
/// simulation completes in [`assert_final_convergence`]. A mid-run
/// `assert_always!` cannot express eventual convergence: it permanently records
/// a transient failure even if a later leader change immediately heals the node.
///
/// It reads the **empty** prefix as its own state (`None`, not `Some(0)`), which
/// is what lets it see the #56 boundary at all: a node that has applied nothing
/// emits no `log_applied`, so it is invisible in the applied stream and has to be
/// found through the boot registry instead. Such a node next to a cluster whose
/// prefix is exactly slot 0 is the divergence a `0`-initialised oracle reads as
/// "converged" — the same sentinel confusion the heartbeat watermark used to
/// carry on the wire. The run shape that exhibits it is a cluster that decides
/// *one* slot and then goes quiet, which is what `crate::workload`'s quiet mode
/// exists to produce.
pub(crate) struct ConvergenceOracle;

impl Invariant for ConvergenceOracle {
    fn name(&self) -> &'static str {
        "convergence"
    }

    fn observe(&self, q: &dyn TraceQuery, sim_time_ms: u64) {
        // Per-node chosen-prefix high-water mark, plus the cluster maximum and the
        // time it was last raised (the applied index equals the slot; the driver
        // only emits it walking the *contiguous* chosen prefix, so a node's max is
        // its prefix length − 1).
        //
        // Both maxima are `Option`s, and that is the point rather than a detail: a
        // node that has applied *nothing* emits no `log_applied` at all, so an
        // empty prefix (`None`) is not `Some(0)` and must not be folded into one.
        // Reading a bare `0` for both is the same sentinel confusion #56 found on
        // the wire — the leader's heartbeat watermark encoded "nothing chosen" as
        // `Slot(0)`, indistinguishable from "slot 0 chosen" — and an oracle that
        // shares it can never go red on it.
        let mut per_node_max: BTreeMap<u64, u64> = BTreeMap::new();
        let mut cluster_max: Option<u64> = None;
        let mut cluster_max_time = 0_u64;
        let mut lagged: BTreeSet<u64> = BTreeSet::new();
        let applied = q.snapshot(EV_APPLIED);
        if applied.is_empty() {
            return;
        }
        // Single time-ordered pass: track each node's running max and the cluster
        // max, marking any node that is ever strictly behind the cluster max (it
        // fell into a hole). `snapshot` yields events in capture (time) order.
        for e in &applied {
            let (Some(node), Some(idx)) = (e.u64("node"), e.u64("applied_index")) else {
                continue;
            };
            if cluster_max.is_none_or(|m| idx > m) {
                cluster_max = Some(idx);
                cluster_max_time = e.time_ms;
            }
            let m = per_node_max.entry(node).or_insert(0);
            *m = (*m).max(idx);
            for (&n, &nm) in &per_node_max {
                if Some(nm) < cluster_max {
                    lagged.insert(n);
                }
            }
        }
        // Nothing carried an `applied_index`: no cluster prefix to converge to.
        let Some(cluster_max) = cluster_max else {
            return;
        };

        // Coverage: a node that fell behind and then reached the cluster max —
        // proof the catch-up path actually healed a hole (not merely that nothing
        // ever broke). Fires on the fixed code; drives sweep saturation.
        let recovered = cluster_max > 0
            && lagged
                .iter()
                .any(|n| per_node_max.get(n).copied() == Some(cluster_max));
        assert_sometimes!(
            recovered,
            "a lagging node catches up to the cluster's chosen prefix"
        );
        if recovered {
            assert_reachable!("a lagging node converges via catch-up");
        }

        // Each node's most recent lifecycle event: `booted` (up) vs `crashed` (a
        // seam crash — down until a later boot). Used to require convergence only
        // of nodes that are up and have been stable for the grace window, so a
        // node crashed or just-rebooted in the settle tail is not falsely flagged
        // (its prefix is re-established from durable storage on boot).
        let mut last_life: BTreeMap<u64, (u64, bool)> = BTreeMap::new();
        for (name, is_boot) in [(EV_BOOTED, true), (EV_CRASHED, false)] {
            for e in q.snapshot(name) {
                if let Some(node) = e.u64("node") {
                    let slot = last_life.entry(node).or_insert((0, is_boot));
                    if e.time_ms >= slot.0 {
                        *slot = (e.time_ms, is_boot);
                    }
                }
            }
        }
        let stable_up = |node: u64| -> bool {
            match last_life.get(&node) {
                Some(&(t, true)) => sim_time_ms.saturating_sub(t) > CONVERGENCE_GRACE_MS,
                Some(&(_, false)) => false, // most recent event is a crash → down
                None => true,               // applied but no lifecycle event recorded
            }
        };

        // Quiescence gate (shared with [`GapFillOracle`]): only once the cluster
        // has genuinely settled is a lagging live node a real convergence failure
        // rather than a node still catching up under a settling cluster.
        if !quiesced(q, sim_time_ms, cluster_max_time) {
            return;
        }

        // Each node's final durable compaction floor (the max over its truncation
        // events). A lagging node whose next needed slot has been truncated on
        // *every* peer cannot catch up through commit-replay: no live peer can
        // serve it a contiguous range. That case used to be *exempt* from
        // convergence (recovering it was the application's out-of-band job).
        // Snapshot transfer now recovers it through paros, so convergence is
        // demanded of every stable live node; the below-floor case is kept as a
        // reachability gate (proof the hard path was actually exercised) rather
        // than an escape hatch.
        let mut final_floor: BTreeMap<u64, u64> = BTreeMap::new();
        for (_t, node, first) in collect_compactions(q) {
            let f = final_floor.entry(node).or_insert(0);
            *f = (*f).max(first);
        }

        // Every node this run ever brought up, not just the ones that applied
        // something: a node whose prefix is still empty is exactly the case the
        // applied stream cannot show, because it emits no event to be seen in.
        let mut cluster: BTreeSet<u64> = q
            .snapshot(EV_BOOTED)
            .iter()
            .filter_map(|e| e.u64("node"))
            .collect();
        cluster.extend(per_node_max.keys().copied());

        for &node in &cluster {
            if !stable_up(node) {
                continue;
            }
            // `None` = this node has applied nothing at all. Under #56's heartbeat
            // encoding a follower that missed only slot 0's `Commit` sat here
            // forever — the beat advertised a bare `Slot(0)` it could not tell from
            // "the leader has nothing", so it never pulled, and no other healing
            // path is open (the leader re-sends `Accept`s only for slots still in
            // flight, the reverse push needs a strictly higher local prefix, and
            // healthy beats keep the election timer from firing). A `0`-sentinel
            // oracle reads that node as "converged to slot 0"; this one does not.
            let prefix = per_node_max.get(&node).copied();
            if prefix == Some(cluster_max) {
                continue;
            }
            let next_needed = prefix.map_or(0, |m| m + 1);
            let below_all_floors = per_node_max.keys().any(|&p| p != node)
                && per_node_max
                    .keys()
                    .filter(|&&p| p != node)
                    .all(|&p| next_needed < final_floor.get(&p).copied().unwrap_or(0));
            if below_all_floors {
                // Reached the hard case: a node below every peer's floor, which
                // only snapshot transfer can heal. Fire the reachability gate, then
                // still demand convergence (the fix must actually work).
                assert_reachable!(
                    "a node fell below every peer's compaction floor (recovers via snapshot transfer)"
                );
            }
        }
    }
}

/// Gap-fill oracle: an election never strands the log behind an **undecided
/// hole**. Once the cluster has quiesced, no node may still be holding a slot it
/// knows is chosen above its own applied prefix.
///
/// This is the invariant no prior oracle sees, and the reason a wedged cluster
/// looks like *silence* rather than a violation. A candidate re-proposes only the
/// slots its promise quorum reported accepted; a slot that reached the old leader
/// alone, while a *later* slot reached the promise quorum, is neither recovered nor
/// re-allocated — `next_slot` jumps past it. From then on:
///
/// - `advance_chosen_index` freezes the applied prefix one below the hole,
///   cluster-wide and forever, while higher slots keep being chosen;
/// - the fresh-leader read fence sits above the hole, so no read ever confirms —
///   parked reads just hang to the driver timeout;
/// - commit-replay catch-up cannot heal it: every node's chosen prefix is frozen
///   below the hole, so no peer has anything to replay.
///
/// Every one of those consequences is *quiet*: the safety oracles stay green (no
/// node disagrees about any slot), [`ConvergenceOracle`] stays green (every node
/// is frozen at the *same* prefix, so they have all "converged"), and the client
/// merely times out. The `chosen_gap` event the driver emits each tick is what
/// makes the wedge visible; asserting it does not survive quiescence is what turns
/// it into a failure.
///
/// A gap itself is perfectly ordinary — pipelining leaves several slots undecided,
/// and a follower that missed one `Commit` holds one until catch-up runs. So the
/// assertion is gated twice over. First on [`quiesced`], exactly like
/// [`ConvergenceOracle`]: nothing is asserted until chaos ended, the prefix stopped
/// growing, and leadership settled. Second, and this is what keeps the two oracles
/// from overlapping, only a hole **no node has applied** counts. A lagging node's
/// gap sits below the cluster's applied high-water mark — some peer has that slot
/// and catch-up can serve it, which is precisely [`ConvergenceOracle`]'s subject
/// and a known-slow path. The election hole is the other thing entirely: the hole
/// is *above* the cluster maximum, because every node is frozen at the same place
/// and nobody has the slot to give.
pub(crate) struct GapFillOracle;

impl Invariant for GapFillOracle {
    fn name(&self) -> &'static str {
        "election_gap_fill"
    }

    fn observe(&self, q: &dyn TraceQuery, sim_time_ms: u64) {
        // Coverage: the fill path actually ran. Bare `assert_reachable!` rather than
        // `assert_sometimes!`, following [`TruncationOracle`]'s below-floor Prepare
        // gate: a fill happens only when a slot reached the old leader *alone* below
        // a later slot that reached the promise quorum, which is a rare
        // interleaving. Demanding it on a plateau of seeds would stall saturation
        // forever; demanding it at least once across exploration is the honest bar.
        if !q.snapshot(EV_GAP_FILLED).is_empty() {
            assert_reachable!("a new leader gap-fills a hole its promise quorum never reported");
        }

        // A run that applied nothing anywhere is degenerate — the client never got
        // a value in — and `ProgressOracle` owns that case.
        let Some((cluster_max, prefix_grew_ms)) = cluster_applied_max(q) else {
            return;
        };
        if !quiesced(q, sim_time_ms, prefix_grew_ms) {
            return;
        }
        // Still open *right now*, not merely seen at some point in the tail: the
        // driver re-emits the gap every tick, so a node that still holds one has
        // reported it within the last few ticks. Reading the whole grace window
        // instead would flag a gap that opened and healed earlier in the tail — a
        // node rebooting out of the chaos window does exactly that.
        //
        // And only a hole above the cluster's applied maximum: below it the slot
        // exists on some peer and catch-up can serve it, which is a lagging node
        // ([`ConvergenceOracle`]'s subject), not a hole nothing can fill.
        let since = sim_time_ms.saturating_sub(GAP_STILL_OPEN_MS);
        let wedged = q
            .snapshot(EV_CHOSEN_GAP)
            .iter()
            .any(|e| e.time_ms > since && e.u64("hole").is_some_and(|hole| hole > cluster_max));
        assert_always!(
            !wedged,
            "a quiesced cluster holds no chosen slot above its applied prefix (an election left an undecided hole)"
        );
    }
}

/// Cursor-based Chain-of-Blocks state-machine oracle. It observes only public
/// application facts; it does not share the state transition implementation.
pub(crate) struct ChainAgreement {
    submitted_cursor: Cell<usize>,
    control_cursor: Cell<usize>,
    snapshot_cursor: Cell<usize>,
    applied_cursor: Cell<usize>,
    submitted: RefCell<BTreeMap<String, u64>>,
    state_by_index: RefCell<BTreeMap<u64, String>>,
    command_by_index: RefCell<BTreeMap<u64, String>>,
    node_index: RefCell<BTreeMap<u64, u64>>,
    network_guidance: bool,
}

impl ChainAgreement {
    pub(crate) fn new() -> Self {
        Self {
            submitted_cursor: Cell::new(0),
            control_cursor: Cell::new(0),
            snapshot_cursor: Cell::new(0),
            applied_cursor: Cell::new(0),
            submitted: RefCell::new(BTreeMap::new()),
            state_by_index: RefCell::new(BTreeMap::new()),
            command_by_index: RefCell::new(BTreeMap::new()),
            node_index: RefCell::new(BTreeMap::new()),
            network_guidance: false,
        }
    }

    pub(crate) fn network() -> Self {
        Self {
            network_guidance: true,
            ..Self::new()
        }
    }

    fn fault_regime(q: &dyn TraceQuery, before_seq: u64) -> i64 {
        q.snapshot(SIM_FAULT_EVENT_NAME)
            .iter()
            .filter(|event| event.seq < before_seq)
            .filter_map(|event| event.str("kind"))
            .map(|kind| {
                if kind.contains("storage") {
                    2
                } else if kind.contains("process") {
                    3
                } else {
                    1
                }
            })
            .next_back()
            .unwrap_or(0)
    }
}

impl Default for ChainAgreement {
    fn default() -> Self {
        Self::new()
    }
}

impl Invariant for ChainAgreement {
    fn name(&self) -> &'static str {
        "chain_agreement"
    }

    #[allow(clippy::too_many_lines)]
    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let mut submitted = self.submitted.borrow_mut();
        for event in q.since("chain_command_submitted", &self.submitted_cursor) {
            if let Some(command) = event.str("cmd") {
                submitted.entry(command.to_owned()).or_insert(event.seq);
            }
        }
        for event in q.since("chain_control_submitted", &self.control_cursor) {
            if let Some(command) = event.str("cmd") {
                submitted.entry(command.to_owned()).or_insert(event.seq);
            }
        }

        // Snapshot and ordinary apply streams must be merged by global trace
        // sequence; separate cursors alone would lose their causal ordering.
        let mut transitions = q
            .since("chain_snapshot_installed", &self.snapshot_cursor)
            .into_iter()
            .map(|event| (event.seq, true, event))
            .chain(
                q.since("command_applied", &self.applied_cursor)
                    .into_iter()
                    .map(|event| (event.seq, false, event)),
            )
            .collect::<Vec<_>>();
        transitions.sort_by_key(|(seq, _, _)| *seq);

        let mut states = self.state_by_index.borrow_mut();
        let mut commands = self.command_by_index.borrow_mut();
        let mut per_node = self.node_index.borrow_mut();
        for (_, snapshot, event) in transitions {
            let (Some(node), Some(index), Some(state)) =
                (event.u64("node"), event.u64("index"), event.str("state"))
            else {
                continue;
            };
            if snapshot {
                let monotone = per_node
                    .get(&node)
                    .is_none_or(|previous| index >= *previous);
                assert_always!(monotone, "chain: applies are contiguous per node");
                per_node.insert(node, index);
                let prior = states.entry(index).or_insert_with(|| state.to_owned());
                assert_always!(
                    prior == state,
                    "chain: one state per applied index",
                    {
                        "node" => node,
                        "index" => index,
                        "expected_state" => prior,
                        "observed_state" => state,
                    }
                );
                continue;
            }

            let expected = per_node.get(&node).copied().unwrap_or(0).saturating_add(1);
            assert_always!(index == expected, "chain: applies are contiguous per node");
            per_node.insert(node, index);

            let Some(command) = event.str("cmd") else {
                continue;
            };
            let prior_command = commands.entry(index).or_insert_with(|| command.to_owned());
            let prior_state = states.entry(index).or_insert_with(|| state.to_owned());
            assert_always!(
                prior_command == command && prior_state == state,
                "chain: one state per applied index",
                {
                    "node" => node,
                    "index" => index,
                    "expected_command" => prior_command,
                    "observed_command" => command,
                    "expected_state" => prior_state,
                    "observed_state" => state,
                }
            );

            let kind = event.str("kind").unwrap_or("unknown");
            let proposed = kind == "noop"
                || submitted
                    .get(command)
                    .is_some_and(|submitted_seq| *submitted_seq < event.seq);
            assert_always!(
                proposed,
                "chain: applied command was proposed",
                {
                    "node" => node,
                    "index" => index,
                    "kind" => kind,
                    "command" => command,
                }
            );

            let latest_leader = q
                .snapshot(EV_LEADER)
                .into_iter()
                .rfind(|leader| leader.seq < event.seq)
                .and_then(|leader| leader.u64("node"));
            let role = i64::from(latest_leader == Some(node));
            let fault = Self::fault_regime(q, event.seq);
            let floor = q
                .snapshot(EV_COMPACTED)
                .into_iter()
                .filter(|compact| compact.seq < event.seq && compact.u64("node") == Some(node))
                .filter_map(|compact| compact.u64("first"))
                .max();
            let floor_relation = match (floor, event.u64("slot")) {
                (Some(first), Some(slot)) if slot >= first => 1,
                (Some(_), Some(_)) => 2,
                (None, _) | (Some(_), None) => 0,
            };
            assert_sometimes_each!(
                "chain: state frontier",
                [("role", role), ("fault", fault), ("floor", floor_relation)],
                [("applied_count", index)]
            );
        }
        drop(per_node);
        drop(commands);
        drop(states);
        drop(submitted);

        let leaders = q.snapshot(EV_LEADER);
        let acknowledgements = q.snapshot("chain_command_acked");
        let leader_changed = leaders.len() >= 2;
        let acknowledged_after_change = leaders
            .get(1)
            .is_some_and(|changed| acknowledgements.iter().any(|ack| ack.seq > changed.seq));
        if self.network_guidance {
            let noop_applied = q
                .snapshot("command_applied")
                .iter()
                .any(|event| event.str("kind") == Some("noop"));
            if noop_applied {
                assert_reachable!("chain: noop gap fill is applied");
            }
        } else {
            assert_sometimes!(
                acknowledged_after_change,
                "chain: proposal succeeds after leader change"
            );
            assert_sometimes!(
                !q.snapshot("chain_compact_accepted").is_empty()
                    && !q.snapshot(EV_COMPACTED).is_empty(),
                "chain: compact takes effect"
            );
            assert_sometimes!(
                !q.snapshot("chain_snapshot_installed").is_empty(),
                "chain: node recovers through snapshot install"
            );
            let old_leader_gone = !q.snapshot(EV_LEADERSHIP_RESIGNED).is_empty()
                || !q.snapshot(EV_CRASHED).is_empty();
            assert_sometimes_all!(
                "chain: failover completed",
                [
                    ("old leader gone", old_leader_gone),
                    ("new leader elected", leader_changed),
                    ("client acknowledged", acknowledged_after_change),
                ]
            );
        }
    }

    fn reset(&mut self) {
        self.submitted_cursor.set(0);
        self.control_cursor.set(0);
        self.snapshot_cursor.set(0);
        self.applied_cursor.set(0);
        self.submitted.get_mut().clear();
        self.state_by_index.get_mut().clear();
        self.command_by_index.get_mut().clear();
        self.node_index.get_mut().clear();
    }
}

/// Snapshot oracle (coverage): the below-floor recovery *mechanism* is actually
/// exercised. The [`ConvergenceOracle`] demands the liveness (a below-floor node
/// reaches the cluster prefix); this proves the path that heals it — an opaque
/// snapshot install — really fires, so the red→green result is not vacuous. The
/// install's promise-never-lowers safety is covered by [`SafetyOracle`]
/// invariant 2 (the boot/`node_state` monotonic-promise check).
pub(crate) struct SnapshotOracle;

impl Invariant for SnapshotOracle {
    fn name(&self) -> &'static str {
        "snapshot"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let snapshots = collect_snapshots(q);
        let installed = !snapshots.is_empty();
        assert_sometimes!(
            installed,
            "a below-floor node recovers via snapshot transfer"
        );
        if installed {
            assert_reachable!("a snapshot was installed to recover a below-floor node");
        }

        // #88 reachability: a snapshot lands during a live election — the
        // window in which `on_install_snapshot` can raise a candidate's
        // promise past the ballot it is campaigning at (the stale-ballot route
        // the `try_become_leader` guard closes). The driver detects the exact
        // condition (`role == Candidate` while an `InstallSnapshot` write
        // persists) and traces [`EV_SNAPSHOT_MID_ELECTION`]; the win-at-a-
        // stale-ballot *bug* itself is what [`LeadershipOracle`] detects.
        let mid_election = !q.snapshot(EV_SNAPSHOT_MID_ELECTION).is_empty();
        assert_sometimes!(mid_election, "a snapshot lands during a live election");
        if mid_election {
            assert_reachable!("a snapshot lands during a live election");
        }
    }
}

/// Driver-hook oracle (coverage only): the driver's durability seams and
/// rare-but-valid policy decisions are actually taken on some seeds.
///
/// It asserts no new safety property; the consequences are already covered by
/// prefix agreement, no gaps, convergence, snapshot recovery, and gap fill. It
/// proves the hooks are still connected: perturbations that stopped firing would
/// leave the sweep looking green while quietly testing less, which is precisely
/// how #54 stayed invisible for 1500 seeds before it.
pub(crate) struct DriverHookOracle;

impl Invariant for DriverHookOracle {
    fn name(&self) -> &'static str {
        "driver_hooks"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let crashes = q.snapshot(EV_CRASHED);
        let after_sync_crashed = crashes
            .iter()
            .any(|event| event.str("seam") == Some("after_sync_before_send"));
        let before_sync_crashed = crashes
            .iter()
            .any(|event| event.str("seam") == Some("before_sync"));
        let snapshot_offered = !q.snapshot(EV_SNAPSHOT_OFFERED).is_empty();
        let shortest_timeout = !q.snapshot(EV_ELECTION_TIMEOUT_EXTREME).is_empty();
        let skipped = !q.snapshot(EV_RESEND_SKIPPED).is_empty();
        let resigned = !q.snapshot(EV_LEADERSHIP_RESIGNED).is_empty();
        assert_sometimes!(
            after_sync_crashed,
            "the driver crashes after sync and before sending a batch"
        );
        assert_sometimes!(
            before_sync_crashed,
            "the driver crashes before syncing a staged batch"
        );
        assert_sometimes!(
            snapshot_offered,
            "a snapshot offer enters the driver's common outbound path"
        );
        assert_sometimes!(
            shortest_timeout,
            "the driver selects the shortest valid election timeout"
        );
        if after_sync_crashed {
            assert_reachable!("the driver crashes after sync and before sending a batch");
        }
        if snapshot_offered {
            assert_reachable!("the driver queues a snapshot offer before the send seam");
        }
        if shortest_timeout {
            assert_reachable!("the driver selects the shortest valid election timeout");
        }
        assert_sometimes!(skipped, "the driver skips a pending accept re-send");
        assert_sometimes!(resigned, "the driver voluntarily resigns leadership");
        if skipped {
            assert_reachable!("the driver skips a pending accept re-send");
        }
        if resigned {
            assert_reachable!("the driver voluntarily resigns leadership");
        }
        // The send-seam per-message loss locations (#80/#88 reachability): an
        // isolated `Accept` vanishing is the chosen-gap wedge ingredient; a
        // lost `Prepare`/`Promise` stretches an election open.
        let send_drops = q.snapshot(EV_SEND_DROPPED);
        let dropped_accept = send_drops
            .iter()
            .any(|event| event.str("kind") == Some("accept"));
        let dropped_election = send_drops
            .iter()
            .any(|event| matches!(event.str("kind"), Some("prepare" | "promise" | "nack")));
        assert_sometimes!(
            dropped_accept,
            "the driver drops one isolated accept at the send seam"
        );
        if dropped_accept {
            assert_reachable!("the driver drops one isolated accept at the send seam");
        }
        assert_sometimes!(
            dropped_election,
            "the driver drops an election message at the send seam"
        );
        if dropped_election {
            assert_reachable!("the driver drops an election message at the send seam");
        }
    }
}

/// Truncation oracle: the log stays bounded, and nothing below a node's durable
/// compaction floor is ever persisted or recovered again (safety of truncation),
/// while the compaction path and the dangerous below-floor Prepare interleaving
/// are actually exercised (coverage).
///
/// The floor for each `(node, slot)` event is folded from the [`EV_COMPACTED`]
/// stream with a two-pointer merge (both streams in capture order), matching the
/// pattern [`RecoveryOracle`] uses for `persist`/`recovered`.
pub(crate) struct TruncationOracle;

impl TruncationOracle {
    /// For each event in `events` (each carrying `node`/`slot`), assert its slot
    /// is at or above the node's durable compaction floor established *strictly
    /// before* that event's time.
    ///
    /// The floor is folded from compactions with time `<` the event's time, not
    /// `<=`: a node may legitimately persist a late re-accept of a slot in the same
    /// simulated millisecond it then compacts that slot away (the accept happened
    /// first, in-core, guarded by `record_accepted`'s floor check). Counting a
    /// same-instant compaction against such a persist would be a false positive. A
    /// real resurrection (a persist below a floor made durable at an earlier
    /// instant) is still caught, since the in-core floor only ever rises.
    fn assert_above_floor(
        q: &dyn TraceQuery,
        compactions: &[(u64, u64, u64)],
        event: &'static str,
        msg: &'static str,
    ) {
        let mut ci = 0;
        let mut floor: BTreeMap<u64, u64> = BTreeMap::new();
        for e in q.snapshot(event) {
            let (Some(node), Some(slot)) = (e.u64("node"), e.u64("slot")) else {
                continue;
            };
            while ci < compactions.len() && compactions[ci].0 < e.time_ms {
                let (_, cn, cf) = compactions[ci];
                let f = floor.entry(cn).or_insert(0);
                *f = (*f).max(cf);
                ci += 1;
            }
            assert_always!(slot >= floor.get(&node).copied().unwrap_or(0), msg);
        }
    }
}

impl Invariant for TruncationOracle {
    fn name(&self) -> &'static str {
        "log_truncation"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        let compactions = collect_compactions(q);

        // Safety: a node never persists an accept, nor recovers a record on boot,
        // below its own durable floor. The truncated prefix is genuinely gone, so
        // the log stays bounded by the floor across restarts.
        Self::assert_above_floor(
            q,
            &compactions,
            EV_PERSIST,
            "a node never persists an accept below its compaction floor",
        );
        Self::assert_above_floor(
            q,
            &compactions,
            EV_RECOVERED,
            "a truncated record is never recovered on boot (the log stays bounded)",
        );

        // Coverage: compaction actually happens (the workload drives it every run).
        let compacted = compactions.iter().any(|&(_, _, first)| first > 0);
        assert_sometimes!(compacted, "the log is compacted (truncation happens)");
        if compacted {
            assert_reachable!("a node truncates its log prefix behind the chosen index");
        }

        // Coverage: the dangerous below-floor Prepare interleaving is exercised, so
        // the guard that refuses it stays under test. Rare (only a lagging node
        // below a compacted peer's floor triggers it), so reachable-only: it must
        // be hit at least once across exploration, not on every seed.
        if !q.snapshot(EV_PREPARE_BELOW_FLOOR).is_empty() {
            assert_reachable!("a candidate prepares below a peer's compaction floor");
        }
    }
}

/// Turn the recorded timeline into the animation [`RunResult`]: match each issued
/// proposal to its acknowledgement (delivered) or failure (dropped), and
/// synthesize the legs of every round trip.
/// Pair each issued proposal with its terminal event into the client-leg
/// [`Shot`]s the demo animates, returning `(shots, delivered, dropped,
/// longest_rtt_ms)`.
fn build_shots(
    issued: &[((u64, u64), u64)],
    ack: &BTreeMap<(u64, u64), u64>,
    fail: &BTreeMap<(u64, u64), u64>,
) -> (Vec<Shot>, u32, u32, u64) {
    let mut shots = Vec::new();
    let mut delivered = 0_u32;
    let mut dropped = 0_u32;
    let mut longest_rtt_ms = 0_u64;

    for ((client, seq), issue_ms) in issued.iter().copied() {
        if let Some(&ack_ms) = ack.get(&(client, seq)) {
            delivered += 1;
            let rtt = ack_ms.saturating_sub(issue_ms);
            longest_rtt_ms = longest_rtt_ms.max(rtt);
            let mid_ms = issue_ms.saturating_add(rtt / 2);
            shots.push(Shot {
                client,
                seq,
                from: NODE_A,
                to: NODE_B,
                depart_ms: issue_ms,
                arrive_ms: mid_ms,
                latency_ms: mid_ms.saturating_sub(issue_ms),
                outcome: Outcome::Delivered,
            });
            shots.push(Shot {
                client,
                seq,
                from: NODE_B,
                to: NODE_A,
                depart_ms: mid_ms,
                arrive_ms: ack_ms,
                latency_ms: ack_ms.saturating_sub(mid_ms),
                outcome: Outcome::Delivered,
            });
        } else {
            dropped += 1;
            let end_ms = fail.get(&(client, seq)).copied().unwrap_or(issue_ms);
            let span = end_ms.saturating_sub(issue_ms).max(MIN_DROP_SPAN_MS);
            shots.push(Shot {
                client,
                seq,
                from: NODE_A,
                to: NODE_B,
                depart_ms: issue_ms,
                arrive_ms: issue_ms.saturating_add(span),
                latency_ms: span,
                outcome: Outcome::Dropped,
            });
        }
    }
    (shots, delivered, dropped, longest_rtt_ms)
}

pub(crate) fn build_result(
    seed: u64,
    data: &RecorderData,
    proto: &ProtocolData,
    rec: &RecoveryData,
) -> RunResult {
    let (protocol, node_states, chosen, leaders, applied) = build_protocol(proto);
    let nodes = proto
        .cluster
        .iter()
        .max()
        .map_or(0, |&m| usize::try_from(m).unwrap_or(0) + 1);
    let crashes = rec.crashes.clone();
    let restarts = rec.restarts.clone();
    let syncs = rec.syncs.clone();
    let recovered = rec.recovered.clone();

    if data.issued.is_empty() {
        return RunResult {
            nodes,
            protocol,
            node_states,
            chosen,
            leaders,
            applied,
            crashes,
            restarts,
            syncs,
            recovered,
            ..RunResult::empty(seed)
        };
    }

    let ack: BTreeMap<(u64, u64), u64> = data.acked.iter().copied().collect();
    let fail: BTreeMap<(u64, u64), u64> = data.failed.iter().copied().collect();
    let mut issued = data.issued.clone();
    issued.sort_by_key(|&(_, t)| t);
    let (shots, delivered, dropped, longest_rtt_ms) = build_shots(&issued, &ack, &fail);

    // The animation spans the latest of any observable event: a client leg, a
    // protocol leg, a node-state change, a chosen marker, or a crash/restart —
    // the crash/recovery tail must not be truncated.
    let sim_duration_ms = shots
        .iter()
        .map(|s| s.arrive_ms)
        .chain(protocol.iter().map(|s| s.arrive_ms))
        .chain(node_states.iter().map(|s| s.time_ms))
        .chain(chosen.iter().map(|s| s.time_ms))
        .chain(leaders.iter().map(|s| s.time_ms))
        .chain(applied.iter().map(|s| s.time_ms))
        .chain(crashes.iter().map(|s| s.time_ms))
        .chain(restarts.iter().map(|s| s.time_ms))
        .chain(recovered.iter().map(|s| s.time_ms))
        .max()
        .unwrap_or(0);

    RunResult {
        seed,
        nodes,
        requests: u32::try_from(issued.len()).unwrap_or(u32::MAX),
        shots,
        protocol,
        node_states,
        chosen,
        leaders,
        applied,
        crashes,
        restarts,
        syncs,
        recovered,
        delivered,
        dropped,
        ticks: data.ticks,
        longest_rtt_ms,
        sim_duration_ms,
    }
}
