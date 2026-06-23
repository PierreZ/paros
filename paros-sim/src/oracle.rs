//! The oracle harness: invariants that read the simulation trace.
//!
//! - [`TimelineRecorder`] reconstructs the animation [`RunResult`] from the
//!   standard `client_*` events (the wasm demo and native runner consume it).
//! - [`ClientLivenessOracle`] wires the `assert_*` contract macros off the same
//!   event stream — a worked example of moonpool's oracle harness.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{Invariant, TraceQuery, assert_always, assert_reachable, assert_sometimes};
use serde::Serialize;

/// Standard transport-client observability events (same names as moonpool's
/// transport workloads, so tooling is workload-agnostic).
const EV_ISSUED: &str = "client_issued";
const EV_ACKED: &str = "client_acknowledged";
const EV_FAILED: &str = "client_failed";
/// Node logical-clock tick event (emitted by the driver).
const EV_TICK: &str = "node_tick";

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
    /// Request sequence number this leg belongs to.
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

/// The full result of one seeded run: every message leg plus headline counters
/// the UI shows alongside the animation.
#[derive(Debug, Clone, Serialize)]
pub struct RunResult {
    /// The seed this run used.
    pub seed: u64,
    /// Number of proposals observed.
    pub requests: u32,
    /// Every message leg exchanged, in time order.
    pub shots: Vec<Shot>,
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
            requests: 0,
            shots: Vec::new(),
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
    /// `(seq_id, sim_time_ms)` for each issued proposal.
    issued: Vec<(u64, u64)>,
    /// `(seq_id, sim_time_ms)` for each acknowledged proposal.
    acked: Vec<(u64, u64)>,
    /// `(seq_id, sim_time_ms)` for each failed proposal.
    failed: Vec<(u64, u64)>,
    /// Number of logical-clock ticks observed.
    ticks: u64,
}

/// Pull `(seq_id, time_ms)` pairs for every event named `name`.
fn collect_seq(q: &dyn TraceQuery, name: &str) -> Vec<(u64, u64)> {
    q.snapshot(name)
        .into_iter()
        .filter_map(|e| Some((e.u64("seq_id")?, e.time_ms)))
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
        d.ticks = u64::try_from(q.len(EV_TICK)).unwrap_or(u64::MAX);
    }
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

/// Turn the recorded timeline into the animation [`RunResult`]: match each issued
/// proposal to its acknowledgement (delivered) or failure (dropped), and
/// synthesize the legs of every round trip.
pub(crate) fn build_result(seed: u64, data: &RecorderData) -> RunResult {
    if data.issued.is_empty() {
        return RunResult::empty(seed);
    }

    let ack: HashMap<u64, u64> = data.acked.iter().copied().collect();
    let fail: HashMap<u64, u64> = data.failed.iter().copied().collect();
    let mut issued = data.issued.clone();
    issued.sort_by_key(|&(_, t)| t);

    let mut shots = Vec::new();
    let mut delivered = 0_u32;
    let mut dropped = 0_u32;
    let mut longest_rtt_ms = 0_u64;

    for (seq, issue_ms) in issued.iter().copied() {
        if let Some(&ack_ms) = ack.get(&seq) {
            delivered += 1;
            let rtt = ack_ms.saturating_sub(issue_ms);
            longest_rtt_ms = longest_rtt_ms.max(rtt);
            let mid_ms = issue_ms.saturating_add(rtt / 2);
            shots.push(Shot {
                seq,
                from: NODE_A,
                to: NODE_B,
                depart_ms: issue_ms,
                arrive_ms: mid_ms,
                latency_ms: mid_ms.saturating_sub(issue_ms),
                outcome: Outcome::Delivered,
            });
            shots.push(Shot {
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
            let end_ms = fail.get(&seq).copied().unwrap_or(issue_ms);
            let span = end_ms.saturating_sub(issue_ms).max(MIN_DROP_SPAN_MS);
            shots.push(Shot {
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

    let sim_duration_ms = shots.iter().map(|s| s.arrive_ms).max().unwrap_or(0);

    RunResult {
        seed,
        requests: u32::try_from(issued.len()).unwrap_or(u32::MAX),
        shots,
        delivered,
        dropped,
        ticks: data.ticks,
        longest_rtt_ms,
        sim_duration_ms,
    }
}
