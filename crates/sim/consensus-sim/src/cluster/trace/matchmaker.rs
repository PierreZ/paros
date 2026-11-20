//! The events of a cluster walk as a trace of
//! `specs/tla/matchmaker/MatchmakerTrace.tla`: matchmaking, Phase 1 over
//! every configuration of `H_b`, Phase 2 in `C_b` and the GC, on the
//! flavors with matchmakers.

use std::collections::BTreeSet;

use moonpool_tla::Value;
use paros_consensus::membership::MatchmakerId;
use paros_consensus::{Ballot, NodeId};

use super::{ClusterEvent, Sizes, encode};
use crate::trace::{Ranks, Trace, int, node};

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

