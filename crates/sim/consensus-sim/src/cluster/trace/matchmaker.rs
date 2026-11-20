//! The events of a cluster walk that `specs/tla/node/MatchmakerTrace.tla`
//! keeps: matchmaking, Phase 1 over every configuration of `H_b`, Phase 2
//! in `C_b` and the GC, on the flavors with matchmakers.

use super::ClusterEvent;

/// The steps of `Matchmaker.tla` and the log steps of the trace spec. A
/// step down changes nothing in it: a leader that stepped down asks for
/// no GC, and its rounds stay open as a message the code never delivers.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::Select { .. }
            | ClusterEvent::Register { .. }
            | ClusterEvent::Prepare { .. }
            | ClusterEvent::Promise { .. }
            | ClusterEvent::Win { .. }
            | ClusterEvent::Propose { .. }
            | ClusterEvent::Accept { .. }
            | ClusterEvent::Record { .. }
            | ClusterEvent::Learn { .. }
            | ClusterEvent::Restart { .. }
            | ClusterEvent::GcAsk { .. }
            | ClusterEvent::GcAck { .. }
    )
}

