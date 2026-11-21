//! The events of a cluster walk as a trace of the implementation model of
//! `ColocatedNode`, `specs/tla/node/` (#1306 (trace specs extend the
//! checked model)): the log, the handoff, the rounds of the proxy leaders,
//! matchmaking, the GC, the retirement, the floors and the faulty entries
//! of CTRL, the read replica and the leaderless reads. Every trace spec of
//! that folder extends `NodeTrace.tla`, the binding of these events to the
//! actions of `Node.tla`, and reads the same constants: this module writes
//! them all.

use std::collections::BTreeSet;

use moonpool_tla::Value;
use paros_consensus::membership::{MatchmakerId, ProxyId, QuorumSystem};
use paros_consensus::{Ballot, NodeId};

use super::{ClusterEvent, Flavor, Sizes, encode};
use crate::trace::{Ranks, Trace, int, node};

/// Every ballot that a walk names: a campaign registers a ballot before it
/// prepares it, and a ballot that never prepared is in the histories.
fn ballots(events: &[ClusterEvent]) -> BTreeSet<Ballot> {
    events
        .iter()
        .filter_map(|event| match event {
            ClusterEvent::Select { ballot, .. }
            | ClusterEvent::Register { ballot, .. }
            | ClusterEvent::Prepare { ballot, .. }
            | ClusterEvent::GcAsk { ballot }
            | ClusterEvent::GcAck { ballot, .. }
            | ClusterEvent::Basis { ballot, .. }
            | ClusterEvent::ReadStart { ballot, .. }
            | ClusterEvent::ReadAck {
                fence: Some(ballot),
                ..
            } => Some(*ballot),
            _ => None,
        })
        .collect()
}

/// The members of `config` in the order of its grid: a configuration
/// keeps its members sorted (`AcceptorConfig::members`), and its grid is
/// row-major over them, so each set has one layout.
fn layout(config: &[NodeId]) -> Value {
    assert!(
        config.is_sorted(),
        "a configuration lists its members in order"
    );
    Value::Seq(config.iter().map(|id| node(*id)).collect())
}

/// The events of one cluster walk of `flavor` as a trace of a spec of
/// `specs/tla/node/`.
pub(super) fn trace(flavor: &Flavor, events: &[ClusterEvent]) -> Trace {
    let ballots = ballots(events);
    let ranks = Ranks::new(ballots.iter().copied());
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    let pool: Vec<NodeId> = (0..flavor.acceptors + flavor.spares).map(NodeId).collect();
    let mut configs = BTreeSet::new();
    let mut layouts = BTreeSet::from([layout(&pool)]);
    let mut ctxs = BTreeSet::new();
    for event in events {
        match event {
            ClusterEvent::Select { config, .. } => {
                configs.insert(encode::config(config));
                layouts.insert(layout(config));
            }
            ClusterEvent::ReadStart { ctx, .. }
            | ClusterEvent::ReplicaRead { ctx, .. }
            | ClusterEvent::ReadAck { ctx, .. } => {
                ctxs.insert(Value::Int(i64::try_from(*ctx).expect("a token")));
            }
            _ => {}
        }
        encode::write(&mut trace, &ranks, 0, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        Value::Set(pool.iter().map(|id| node(*id)).collect()),
    );
    trace.define(
        "TraceMatchmakers",
        Value::Set(
            (0..flavor.matchmakers)
                .map(|id| encode::matchmaker(MatchmakerId(id)))
                .collect(),
        ),
    );
    trace.define("TraceConfigs", Value::Set(configs.into_iter().collect()));
    trace.define("TraceLayouts", Value::Set(layouts.into_iter().collect()));
    trace.define(
        "TraceReplicas",
        Value::Set(
            (0..flavor.replicas)
                .map(|i| node(NodeId(crate::cluster::REPLICA_BASE + i)))
                .collect(),
        ),
    );
    trace.define("TraceCtxs", Value::Set(ctxs.into_iter().collect()));
    trace.define(
        "TraceProxies",
        Value::Set(
            (0..flavor.proxies)
                .map(|id| encode::proxy(ProxyId(id)))
                .collect(),
        ),
    );
    trace.define("TraceCommands", sizes.commands(false));
    trace.define("TraceNB", ranks.count());
    trace.define("TraceNS", int(sizes.slots.max(1)));
    trace.define(
        "TraceOwner",
        Value::Seq(ballots.iter().map(|ballot| node(ballot.node)).collect()),
    );
    let (cols, acks) = match flavor.quorum {
        QuorumSystem::Majority => (0, 0),
        QuorumSystem::Grid { cols, acks, .. } => (cols, acks),
    };
    trace.define("TraceCols", int(cols));
    trace.define("TraceAcks", int(acks));
    trace
}
