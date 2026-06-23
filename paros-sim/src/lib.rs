//! `paros-sim` — the deterministic-simulation driver, Paxos workloads, and
//! oracles, backed by moonpool.
//!
//! This crate wraps the sans-IO [`paros_core::RawNode`] in moonpool's
//! deterministic providers and transport (the async driver: step/tick/Ready/
//! advance ↔ I/O, durable-before-send), and defines the workloads and
//! `Invariant`s once so both the native runner and the wasm demo reuse them. It
//! is kept wasm-safe (`default-features = false` drops moonpool's native
//! providers + fork explorer).
//!
//! Stage 1 stands up the harness with **no protocol yet**: an empty cluster
//! advances logical time, acknowledges client proposals, and replays
//! bit-identically from a seed. [`run_seed`] is the single entry point both the
//! native runner and the browser demo call.

mod node;
mod oracle;
mod storage;
mod workload;

pub use node::{NodeProcess, Propose, ProposeAck};
pub use oracle::{Outcome, RunResult, Shot};
pub use storage::MemStorage;

use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::runner::builder::ProcessCount;
use moonpool_sim::{SimulationBuilder, WorkloadCount};

use crate::oracle::{ClientLivenessOracle, RecorderData, TimelineRecorder, build_result};
use crate::workload::ProposeClient;

// --- Tuning knobs ------------------------------------------------------------

/// Number of proposals the client sends.
pub(crate) const REQUESTS: u32 = 12;
/// Per-proposal client deadline, in simulated milliseconds.
pub(crate) const TIMEOUT_MS: u64 = 700;
/// Gap between proposals, in simulated milliseconds, so node ticks interleave.
pub(crate) const GAP_MS: u64 = 20;
/// How often each node advances its logical clock, in simulated milliseconds.
pub(crate) const TICK_INTERVAL_MS: u64 = 50;
/// Number of paros nodes in the cluster.
pub(crate) const CLUSTER_SIZE: usize = 3;

/// Run one deterministic seed of the Stage-1 simulation and return its timeline.
/// The same seed always produces the same [`RunResult`].
#[must_use]
pub fn run_seed(seed: u64) -> RunResult {
    let data = Arc::new(Mutex::new(RecorderData::default()));
    let _report = SimulationBuilder::new()
        .processes(ProcessCount::Fixed(CLUSTER_SIZE), || Box::new(NodeProcess))
        .workloads(WorkloadCount::Fixed(1), |_| Box::new(ProposeClient))
        .invariant(TimelineRecorder::new(data.clone()))
        .invariant(ClientLivenessOracle)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run();

    let data = data.lock().unwrap_or_else(PoisonError::into_inner);
    build_result(seed, &data)
}

/// Run one seed and serialize the [`RunResult`] to JSON. Serializing a plain data
/// struct cannot fail, but on the off chance it does the error is returned as a
/// small JSON object instead of panicking.
#[must_use]
pub fn run_seed_json(seed: u64) -> String {
    serde_json::to_string(&run_seed(seed))
        .unwrap_or_else(|e| format!("{{\"error\":\"serialize failed: {e}\"}}"))
}

#[cfg(test)]
mod tests {
    use paros_core::{Ballot, Message, NodeId, Slot, Value};

    /// One representative of every `Message` variant.
    fn every_variant() -> Vec<Message> {
        let ballot = Ballot {
            round: 7,
            node: NodeId(3),
        };
        vec![
            Message::Prepare {
                from: NodeId(1),
                ballot,
                slot: Slot(5),
            },
            Message::Promise {
                from: NodeId(1),
                ballot,
                slot: Slot(5),
                accepted: Some((ballot, Value(vec![9, 9]))),
            },
            Message::Accept {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
                value: Value(vec![1, 2, 3]),
            },
            Message::Accepted {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
            },
            Message::Nack {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
            },
            Message::Commit {
                from: NodeId(0),
                ballot,
                slot: Slot(6),
                value: Value(vec![4]),
            },
            Message::CheckLeader { from: NodeId(0) },
            Message::Heartbeat { from: NodeId(0) },
        ]
    }

    /// The driver puts `paros_core::Message` on the wire directly (no DTO): every
    /// variant must serde round-trip losslessly.
    #[test]
    fn message_serde_round_trips() {
        for msg in every_variant() {
            let json = serde_json::to_string(&msg).expect("serialize");
            let back: Message = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(msg, back, "serde round-trip must be lossless for {msg:?}");
        }
    }
}
