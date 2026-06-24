//! The oracle harness: invariants that read the simulation trace.
//!
//! - [`TimelineRecorder`] reconstructs the animation [`RunResult`] from the
//!   standard `client_*` events (the wasm demo and native runner consume it).
//! - [`ClientLivenessOracle`] wires the `assert_*` contract macros off the same
//!   event stream — a worked example of moonpool's oracle harness.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{Invariant, TraceQuery, assert_always, assert_reachable, assert_sometimes};
use paros::{EV_CHOSEN, EV_MSG_RECV, EV_MSG_SENT, EV_NODE_STATE, EV_NODE_TICK};
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
    /// The inter-node Paxos protocol exchange, in send order.
    pub protocol: Vec<ProtocolShot>,
    /// Per-node durable-state snapshots, in observation order.
    pub node_states: Vec<NodeStateShot>,
    /// Chosen-value markers, in observation order.
    pub chosen: Vec<ChosenShot>,
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
            protocol: Vec::new(),
            node_states: Vec::new(),
            chosen: Vec::new(),
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
    }
}

/// Pair each send with the earliest unmatched receive sharing its route, in send
/// order. A paired send is `Delivered` (its receive's time is the arrival); an
/// unpaired send is one the network `Dropped`. Deterministic: the trace is
/// captured in deterministic order and the pairing is a stable FIFO over it.
fn build_protocol(data: &ProtocolData) -> (Vec<ProtocolShot>, Vec<NodeStateShot>, Vec<ChosenShot>) {
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

    (protocol, data.node_states.clone(), data.chosen.clone())
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

/// The Paxos safety oracle — the heart of the project. Reads the driver's
/// protocol events ([`EV_CHOSEN`], [`EV_NODE_STATE`]) and asserts the three
/// single-decree safety invariants on every step.
pub(crate) struct SafetyOracle;

impl Invariant for SafetyOracle {
    fn name(&self) -> &'static str {
        "paxos_safety"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        // Invariant 1 (the crown jewel): at most one value is ever chosen per
        // slot — across the whole cluster.
        let mut chosen_value: HashMap<u64, u64> = HashMap::new();
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

        // Invariants 2 & 3 are per-node, reconstructed from the node-state stream
        // in capture (time) order.
        let mut last_promised: HashMap<u64, (u64, u64)> = HashMap::new();
        for e in q.snapshot(EV_NODE_STATE) {
            let Some(node) = e.u64("node") else { continue };
            let (Some(pr), Some(pn)) = (e.u64("pround"), e.u64("pbnode")) else {
                continue;
            };
            let promised = (pr, pn);

            // Invariant 2: a node's promised ballot is monotonic (never decreases).
            if let Some(prev) = last_promised.insert(node, promised) {
                assert_always!(promised >= prev, "a node's promised ballot never decreases");
            }

            // Invariant 3: a node never accepts above its promised ballot (i.e.
            // it only accepts ballots it has promised — never below the promise).
            if e.bool("has_accepted") == Some(true)
                && let (Some(ar), Some(an)) = (e.u64("around"), e.u64("abnode"))
            {
                assert_always!(
                    (ar, an) <= promised,
                    "a node's accepted ballot never exceeds its promised ballot"
                );
            }
        }
    }
}

/// Turn the recorded timeline into the animation [`RunResult`]: match each issued
/// proposal to its acknowledgement (delivered) or failure (dropped), and
/// synthesize the legs of every round trip.
pub(crate) fn build_result(seed: u64, data: &RecorderData, proto: &ProtocolData) -> RunResult {
    let (protocol, node_states, chosen) = build_protocol(proto);

    if data.issued.is_empty() {
        return RunResult {
            protocol,
            node_states,
            chosen,
            ..RunResult::empty(seed)
        };
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

    // The animation spans the latest of any observable event: a client leg, a
    // protocol leg, a node-state change, or a chosen marker.
    let sim_duration_ms = shots
        .iter()
        .map(|s| s.arrive_ms)
        .chain(protocol.iter().map(|s| s.arrive_ms))
        .chain(node_states.iter().map(|s| s.time_ms))
        .chain(chosen.iter().map(|s| s.time_ms))
        .max()
        .unwrap_or(0);

    RunResult {
        seed,
        requests: u32::try_from(issued.len()).unwrap_or(u32::MAX),
        shots,
        protocol,
        node_states,
        chosen,
        delivered,
        dropped,
        ticks: data.ticks,
        longest_rtt_ms,
        sim_duration_ms,
    }
}
