//! The trace tier: the **demo-data recorders** plus the Chain application
//! oracle.
//!
//! Protocol correctness no longer lives here. Every safety, liveness and
//! coverage check the driver can report moved to the **audit port**
//! (`crate::audit`), where each transition is folded into O(1) incremental
//! state instead of re-scanning a growing event stream, and the client-visible
//! history moved into the workloads that own it. What is left reads the trace
//! for two reasons that genuinely need it:
//!
//! - [`TimelineRecorder`] / [`ProtocolRecorder`] / [`RecoveryRecorder`]
//!   reconstruct the animation [`RunResult`] the wasm demo and native runner
//!   render. They assert nothing.
//! - [`ChainAgreement`] checks the *application*'s state machine, whose
//!   transitions the storage layer emits as trace facts (and whose
//!   `sometimes_each` frontier joins them against the simulator's own fault
//!   stream, which no driver callback can see).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{
    Invariant, SIM_FAULT_EVENT_NAME, TraceEvent, TraceQuery, assert_always, assert_reachable,
    assert_sometimes, assert_sometimes_all, assert_sometimes_each,
};
use paros::{
    EV_APPLIED, EV_BOOTED, EV_CHOSEN, EV_COMPACTED, EV_CRASHED, EV_LEADER, EV_LEADERSHIP_RESIGNED,
    EV_MSG_RECV, EV_MSG_SENT, EV_NODE_STATE, EV_NODE_TICK, EV_RECOVERED, EV_SYNCED,
};
use serde::Serialize;

/// Standard transport-client observability events (same names as moonpool's
/// transport workloads, so tooling is workload-agnostic).
const EV_ISSUED: &str = "client_issued";
const EV_ACKED: &str = "client_acknowledged";
const EV_FAILED: &str = "client_failed";

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

/// Which trace stream one merged event came from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ChainStream {
    /// `leader_elected` — feeds the per-transition role dimension.
    Leader,
    /// A simulator fault record — feeds the fault-regime dimension.
    Fault,
    /// `compacted` — feeds the per-node floor dimension.
    Compacted,
    /// `chain_command_acked` — feeds the post-failover acknowledgement gate.
    Acked,
    /// `chain_snapshot_installed` — an application state jump.
    Snapshot,
    /// `snapshot_reset_for_recovery` — Stage 8: a corrupted application
    /// snapshot was reset, so the node's applied index legally restarts from
    /// zero (a local log replay, or a wait for a peer `InstallSnapshot`).
    Reset,
    /// `command_applied` — an ordinary application transition.
    Applied,
}

/// Cursor-based Chain-of-Blocks state-machine oracle. It observes only public
/// application facts; it does not share the state transition implementation.
///
/// This is the one checker that still reads the trace, and deliberately so: the
/// facts it needs are the *application*'s (`command_applied` /
/// `chain_snapshot_installed`, which the sim storage layer emits as it flushes),
/// and its `sometimes_each` frontier joins them against the simulator's own
/// fault stream — neither of which any driver-side audit callback can see.
///
/// Everything it reads is consumed through a **cursor**, and every "the latest
/// X before this transition" lookup is maintained incrementally by merging the
/// auxiliary streams into the same seq-ordered walk. One `observe` therefore
/// costs the events that arrived since the last one, not a re-scan of the run.
pub(crate) struct ChainAgreement {
    submitted_cursor: Cell<usize>,
    control_cursor: Cell<usize>,
    snapshot_cursor: Cell<usize>,
    applied_cursor: Cell<usize>,
    leader_cursor: Cell<usize>,
    fault_cursor: Cell<usize>,
    compacted_cursor: Cell<usize>,
    acked_cursor: Cell<usize>,
    reset_cursor: Cell<usize>,
    checksum_rejected_cursor: Cell<usize>,
    submitted: RefCell<BTreeMap<String, u64>>,
    state_by_index: RefCell<BTreeMap<u64, String>>,
    command_by_index: RefCell<BTreeMap<u64, String>>,
    node_index: RefCell<BTreeMap<u64, u64>>,
    /// The most recent `leader_elected`'s node, as of the transition being
    /// walked (`None` when no election has happened yet, or the event carried
    /// no node).
    latest_leader: Cell<Option<u64>>,
    /// The regime of the most recent fault: 0 none, 2 storage, 3 process,
    /// 1 anything else.
    fault_regime: Cell<i64>,
    /// Per node, the highest compaction floor established so far.
    floor_by_node: RefCell<BTreeMap<u64, u64>>,
    /// How many `leader_elected` events have been consumed.
    leaders_seen: Cell<usize>,
    /// Trace seq of the *second* election — the leader change acks are compared
    /// against.
    second_leader_seq: Cell<Option<u64>>,
    /// Highest `chain_command_acked` seq consumed so far.
    max_ack_seq: Cell<Option<u64>>,
    /// A `Noop` gap fill reached the application.
    noop_applied: Cell<bool>,
    network_guidance: bool,
}

impl ChainAgreement {
    pub(crate) fn new() -> Self {
        Self {
            submitted_cursor: Cell::new(0),
            control_cursor: Cell::new(0),
            snapshot_cursor: Cell::new(0),
            applied_cursor: Cell::new(0),
            leader_cursor: Cell::new(0),
            fault_cursor: Cell::new(0),
            compacted_cursor: Cell::new(0),
            acked_cursor: Cell::new(0),
            reset_cursor: Cell::new(0),
            checksum_rejected_cursor: Cell::new(0),
            submitted: RefCell::new(BTreeMap::new()),
            state_by_index: RefCell::new(BTreeMap::new()),
            command_by_index: RefCell::new(BTreeMap::new()),
            node_index: RefCell::new(BTreeMap::new()),
            latest_leader: Cell::new(None),
            fault_regime: Cell::new(0),
            floor_by_node: RefCell::new(BTreeMap::new()),
            leaders_seen: Cell::new(0),
            second_leader_seq: Cell::new(None),
            max_ack_seq: Cell::new(None),
            noop_applied: Cell::new(false),
            network_guidance: false,
        }
    }

    pub(crate) fn network() -> Self {
        Self {
            network_guidance: true,
            ..Self::new()
        }
    }

    fn fault_regime(kind: &str) -> i64 {
        if kind.contains("storage") {
            2
        } else if kind.contains("process") {
            3
        } else {
            1
        }
    }

    /// Fold the client's submission streams in; `submitted` maps a command to
    /// the trace seq at which it was first proposed.
    fn ingest_submitted(&self, q: &dyn TraceQuery) {
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
    }

    /// Every stream this oracle joins, merged by global trace sequence. The
    /// application transitions *must* be ordered against each other (separate
    /// cursors alone would lose their causal ordering), and the auxiliary
    /// streams ride along so each transition sees exactly the leader, fault and
    /// floor state established strictly before it.
    fn merged(&self, q: &dyn TraceQuery) -> Vec<(ChainStream, TraceEvent)> {
        let mut merged: Vec<(ChainStream, TraceEvent)> = Vec::new();
        for (stream, name, cursor) in [
            (ChainStream::Leader, EV_LEADER, &self.leader_cursor),
            (ChainStream::Fault, SIM_FAULT_EVENT_NAME, &self.fault_cursor),
            (ChainStream::Compacted, EV_COMPACTED, &self.compacted_cursor),
            (
                ChainStream::Acked,
                "chain_command_acked",
                &self.acked_cursor,
            ),
            (
                ChainStream::Snapshot,
                "chain_snapshot_installed",
                &self.snapshot_cursor,
            ),
            (
                ChainStream::Reset,
                "snapshot_reset_for_recovery",
                &self.reset_cursor,
            ),
            (
                ChainStream::Applied,
                "command_applied",
                &self.applied_cursor,
            ),
        ] {
            merged.extend(q.since(name, cursor).into_iter().map(|e| (stream, e)));
        }
        merged.sort_by_key(|(_, event)| event.seq);
        merged
    }

    /// Update the running join state from one auxiliary event.
    fn observe_auxiliary(&self, stream: ChainStream, event: &TraceEvent) {
        match stream {
            ChainStream::Leader => {
                self.latest_leader.set(event.u64("node"));
                let seen = self.leaders_seen.get() + 1;
                self.leaders_seen.set(seen);
                if seen == 2 {
                    self.second_leader_seq.set(Some(event.seq));
                }
            }
            ChainStream::Fault => {
                if let Some(kind) = event.str("kind") {
                    self.fault_regime.set(Self::fault_regime(kind));
                }
            }
            ChainStream::Compacted => {
                if let (Some(node), Some(first)) = (event.u64("node"), event.u64("first")) {
                    let mut floors = self.floor_by_node.borrow_mut();
                    let slot = floors.entry(node).or_insert(first);
                    *slot = (*slot).max(first);
                }
            }
            ChainStream::Acked => {
                let seq = self
                    .max_ack_seq
                    .get()
                    .map_or(event.seq, |m| m.max(event.seq));
                self.max_ack_seq.set(Some(seq));
            }
            ChainStream::Snapshot | ChainStream::Reset | ChainStream::Applied => {}
        }
    }

    /// Stage 8: a corrupted application snapshot was reset for recovery. The
    /// node's applied index legally restarts from zero — the replay that
    /// follows re-derives the *same* per-index states (the agreement check
    /// keeps holding), it merely re-walks them.
    fn observe_reset(&self, event: &TraceEvent) {
        let Some(node) = event.u64("node") else {
            return;
        };
        self.node_index.borrow_mut().remove(&node);
    }

    /// A snapshot install jumps the application state; it need not be
    /// contiguous, but it must never regress and must agree at its index.
    fn observe_snapshot(&self, event: &TraceEvent) {
        let (Some(node), Some(index), Some(state)) =
            (event.u64("node"), event.u64("index"), event.str("state"))
        else {
            return;
        };
        let mut per_node = self.node_index.borrow_mut();
        let monotone = per_node
            .get(&node)
            .is_none_or(|previous| index >= *previous);
        assert_always!(monotone, "chain: applies are contiguous per node");
        per_node.insert(node, index);
        let mut states = self.state_by_index.borrow_mut();
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
    }

    /// One ordinary application transition: contiguous locally, agreeing
    /// cluster-wide at its index, and only ever a command somebody proposed.
    fn observe_applied(&self, event: &TraceEvent) {
        if event.str("kind") == Some("noop") {
            self.noop_applied.set(true);
        }
        let (Some(node), Some(index), Some(state)) =
            (event.u64("node"), event.u64("index"), event.str("state"))
        else {
            return;
        };
        let mut per_node = self.node_index.borrow_mut();
        let expected = per_node.get(&node).copied().unwrap_or(0).saturating_add(1);
        assert_always!(index == expected, "chain: applies are contiguous per node");
        per_node.insert(node, index);
        drop(per_node);

        let Some(command) = event.str("cmd") else {
            return;
        };
        let mut commands = self.command_by_index.borrow_mut();
        let mut states = self.state_by_index.borrow_mut();
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
        drop(states);
        drop(commands);

        let kind = event.str("kind").unwrap_or("unknown");
        let proposed = kind == "noop"
            || self
                .submitted
                .borrow()
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

        let role = i64::from(self.latest_leader.get() == Some(node));
        let floor = self.floor_by_node.borrow().get(&node).copied();
        let floor_relation = match (floor, event.u64("slot")) {
            (Some(first), Some(slot)) if slot >= first => 1,
            (Some(_), Some(_)) => 2,
            (None, _) | (Some(_), None) => 0,
        };
        assert_sometimes_each!(
            "chain: state frontier",
            [
                ("role", role),
                ("fault", self.fault_regime.get()),
                ("floor", floor_relation)
            ],
            [("applied_count", index)]
        );
    }

    /// The campaign-level coverage gates. Every "did this ever happen" question
    /// is an O(1) stream length or a running flag, so the tail costs nothing per
    /// pump.
    fn check_campaign_gates(&self, q: &dyn TraceQuery) {
        let leader_changed = self.leaders_seen.get() >= 2;
        let acknowledged_after_change = self
            .second_leader_seq
            .get()
            .is_some_and(|changed| self.max_ack_seq.get().is_some_and(|ack| ack > changed));
        if self.network_guidance {
            if self.noop_applied.get() {
                assert_reachable!("chain: noop gap fill is applied");
            }
            return;
        }
        assert_sometimes!(
            acknowledged_after_change,
            "chain: proposal succeeds after leader change"
        );
        assert_sometimes!(
            q.len("chain_compact_accepted") != 0 && q.len(EV_COMPACTED) != 0,
            "chain: compact takes effect"
        );
        assert_sometimes!(
            q.len("chain_snapshot_installed") != 0,
            "chain: node recovers through snapshot install"
        );
        let old_leader_gone = q.len(EV_LEADERSHIP_RESIGNED) != 0 || q.len(EV_CRASHED) != 0;
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

impl Default for ChainAgreement {
    fn default() -> Self {
        Self::new()
    }
}

impl Invariant for ChainAgreement {
    fn name(&self) -> &'static str {
        "chain_agreement"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        self.ingest_submitted(q);
        for _ in q.since("proposal_checksum_rejected", &self.checksum_rejected_cursor) {
            assert_reachable!("chain: an invalid proposal checksum is rejected");
        }
        for (stream, event) in self.merged(q) {
            match stream {
                ChainStream::Snapshot => self.observe_snapshot(&event),
                ChainStream::Reset => self.observe_reset(&event),
                ChainStream::Applied => self.observe_applied(&event),
                aux => self.observe_auxiliary(aux, &event),
            }
        }
        self.check_campaign_gates(q);
    }

    fn reset(&mut self) {
        self.submitted_cursor.set(0);
        self.control_cursor.set(0);
        self.snapshot_cursor.set(0);
        self.applied_cursor.set(0);
        self.leader_cursor.set(0);
        self.fault_cursor.set(0);
        self.compacted_cursor.set(0);
        self.acked_cursor.set(0);
        self.reset_cursor.set(0);
        self.checksum_rejected_cursor.set(0);
        self.submitted.get_mut().clear();
        self.state_by_index.get_mut().clear();
        self.command_by_index.get_mut().clear();
        self.node_index.get_mut().clear();
        self.floor_by_node.get_mut().clear();
        self.latest_leader.set(None);
        self.fault_regime.set(0);
        self.leaders_seen.set(0);
        self.second_leader_seq.set(None);
        self.max_ack_seq.set(None);
        self.noop_applied.set(false);
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
