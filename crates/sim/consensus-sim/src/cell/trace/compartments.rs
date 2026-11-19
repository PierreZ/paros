//! The events of a cell walk as a trace of
//! `specs/tla/compartments/CompartmentsTrace.tla`: `MultiPaxosTrace` and
//! the read replica of `specs/tla/compartments/Compartments.tla` (#1231
//! (traced walks reach the rules)).
//!
//! A read replica is recorded by the driver loop of the cell
//! (`Cell::drain_replica`): a `WriteOp::Learned` (`ReplicaLearn`), a floor
//! it wrote (`ReplicaFloor`), each slot its walk folded (`ReplicaApply`)
//! and each read it served (`ReplicaServe`); at a boot (`ReplicaBoot`,
//! the fold after `ReplicaNode::new`) and at the start of a read
//! (`ReplicaRead`). The acceptors' answers (`ReadAck`) and the writes that
//! completed (`Applied`) are the events of `reads.rs`.

use moonpool_tla::Value;
use paros_consensus::{NodeId, WriteOp};

use super::{CellEvent, Cmd, Flavor, multi_paxos};
use crate::trace::{Trace, node};

/// The replica steps of `CompartmentsTrace`.
pub(super) fn keeps(event: &CellEvent) -> bool {
    matches!(
        event,
        CellEvent::ReplicaLearn { .. }
            | CellEvent::ReplicaApply { .. }
            | CellEvent::ReplicaFloor { .. }
            | CellEvent::ReplicaBoot { .. }
            | CellEvent::ReplicaRead { .. }
            | CellEvent::ReplicaServe { .. }
            | CellEvent::ReadAck { .. }
            | CellEvent::Applied { .. }
    )
}

/// The events of one cell walk of `flavor` as a trace of
/// `CompartmentsTrace.tla`.
pub(super) fn trace(flavor: &Flavor, events: &[CellEvent]) -> Trace {
    let mut trace = multi_paxos::trace(flavor.acceptors, flavor.quorum, events);
    trace.define(
        "TraceReplicas",
        Value::Set(
            (0..flavor.replicas)
                .map(|i| node(NodeId(crate::cell::REPLICA_BASE + i)))
                .collect(),
        ),
    );
    let ctxs = events
        .iter()
        .filter_map(|event| match event {
            CellEvent::ReplicaRead { ctx, .. } => {
                Some(Value::Int(i64::try_from(*ctx).expect("a token")))
            }
            _ => None,
        })
        .collect();
    trace.define("TraceCtxs", Value::Set(ctxs));
    trace
}

/// The steps of the writes of one batch of `replica` that come before the
/// slots the batch folds (`before`: what it learned and a jump to a trim
/// point, which the walk folds from), or after them (a compaction, which
/// follows the walk).
pub(in crate::cell) fn replica_writes(
    replica: NodeId,
    writes: &[WriteOp],
    before: bool,
) -> Vec<CellEvent> {
    writes
        .iter()
        .filter_map(|op| match op {
            WriteOp::Learned {
                slot,
                ballot,
                command,
            } if before => Some(CellEvent::ReplicaLearn {
                replica,
                slot: *slot,
                ballot: *ballot,
                cmd: Cmd::of(command),
            }),
            WriteOp::TrimmedTo { point: floor, .. } if before => Some(CellEvent::ReplicaFloor {
                replica,
                floor: *floor,
                jump: true,
            }),
            WriteOp::Truncate { first: floor, .. } if !before => Some(CellEvent::ReplicaFloor {
                replica,
                floor: *floor,
                jump: false,
            }),
            _ => None,
        })
        .collect()
}
