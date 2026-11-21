//! The rules of each trace spec that the checked walks must meet (#1231
//! (traced walks reach the rules)).
//!
//! A trace spec judges only the steps a walk takes. A walk that never fills
//! a gap with a `Noop` leaves the gap-fill guard of the spec unjudged, and
//! TLC accepts it with nothing to say. So each spec names the steps that
//! some checked walk must show: a [`Rule`] is met by one walk, and
//! `tests/traces.rs` fails when no checked walk of a flavor meets a rule of
//! its spec. An empty trace meets no rule.

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::{Ballot, NodeId};

use super::{ClusterEvent, Cmd, Spec};

/// A step of a trace spec that some checked walk must take.
#[derive(Clone, Copy, Debug)]
pub struct Rule {
    /// What the step is, in the words of the spec.
    pub name: &'static str,
    met: fn(&[ClusterEvent]) -> bool,
}

impl Rule {
    /// Whether the kept events of one walk take this step.
    #[must_use]
    pub fn met(&self, events: &[ClusterEvent]) -> bool {
        (self.met)(events)
    }
}

/// Every log spec: Phase 1 with a vote to recover, Phase 2, the learn, the
/// frontier above slot 0, the `Noop` gap fill and the record of a learn.
const LOG: &[Rule] = &[
    Rule {
        name: "a promise reports a vote",
        met: |events| {
            events.iter().any(|e| {
                matches!(e, ClusterEvent::Promise { votes, .. } | ClusterEvent::Page { votes, .. }
                    if !votes.is_empty())
            })
        },
    },
    Rule {
        name: "a leader wins with a frontier above slot 0",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Win { frontier, .. } if frontier.0 > 0))
        },
    },
    Rule {
        name: "three ballots win in one walk",
        met: |events| {
            events
                .iter()
                .filter(|e| matches!(e, ClusterEvent::Win { .. }))
                .count()
                >= 3
        },
    },
    Rule {
        name: "a leader fills a gap with a Noop",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Propose { cmd: Cmd::Noop, .. }))
        },
    },
    Rule {
        name: "an acceptor votes",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Accept { .. }))
        },
    },
    Rule {
        name: "a node learns a slot chosen",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Learn { .. }))
        },
    },
    Rule {
        name: "a learn records the chosen vote below the promise",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Record { .. }))
        },
    },
];

/// The log specs that keep the restarts.
const RESTART: Rule = Rule {
    name: "a node boots again from its disk",
    met: |events| {
        events
            .iter()
            .any(|e| matches!(e, ClusterEvent::Restart { .. }))
    },
};

/// `LeaderHandoffTrace`: a handoff with a pending slot, and its install.
const HANDOFF: &[Rule] = &[
    Rule {
        name: "a leader hands on a tail with a pending slot",
        met: |events| {
            events.iter().any(
                |e| matches!(e, ClusterEvent::Relinquish { pending, .. } if !pending.is_empty()),
            )
        },
    },
    Rule {
        name: "a successor installs the ballot it was handed",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Install { .. }))
        },
    },
];

/// `ProxyLeaderTrace`: a proxy's commit, a commit of a proxy that crashed
/// before and lost its rounds, and a round a proxy closes on a `Nack` or an
/// eviction.
const PROXY: &[Rule] = &[
    Rule {
        name: "a proxy commits a round",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Commit { .. }))
        },
    },
    Rule {
        name: "a proxy commits a round after a crash",
        met: |events| {
            let mut crashed = BTreeSet::new();
            events.iter().any(|e| match e {
                ClusterEvent::Crash { proxy } => {
                    crashed.insert(*proxy);
                    false
                }
                ClusterEvent::Commit { proxy, .. } => crashed.contains(proxy),
                _ => false,
            })
        },
    },
    Rule {
        name: "a proxy closes a round with no decision",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Close { .. }))
        },
    },
];

/// `MatchmakerTrace`: a reconfiguration, Phase 1 over two configurations
/// of `H_b`, and a GC request whose guard reads a learned prefix.
const MATCHMAKER: &[Rule] = &[
    Rule {
        name: "a leader selects a second configuration",
        met: |events| {
            events
                .iter()
                .filter_map(|e| match e {
                    ClusterEvent::Select { config, .. } => Some(config),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
                .len()
                >= 2
        },
    },
    Rule {
        name: "a candidate prepares over two configurations",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Prepare { prior, .. } if prior.len() >= 2))
        },
    },
    Rule {
        name: "a leader asks for GC above frontier 0",
        met: |events| {
            let mut frontier: BTreeMap<Ballot, u64> = BTreeMap::new();
            events.iter().any(|e| match e {
                ClusterEvent::Win {
                    ballot,
                    frontier: f,
                    ..
                } => {
                    frontier.insert(*ballot, f.0);
                    false
                }
                ClusterEvent::GcAsk { ballot } => frontier.get(ballot).is_some_and(|f| *f > 0),
                _ => false,
            })
        },
    },
    Rule {
        name: "a matchmaker acks a GC request",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::GcAck { .. }))
        },
    },
];

/// `MatchmakerGridTrace`: a win on a Phase-1 quorum of a grid that is no
/// majority of its configuration (on a `2 × 2` full grid, one acceptor of
/// each column), and a proxy's `Commit` under a configuration that a
/// reconfiguration selected.
const GRID: &[Rule] = &[
    Rule {
        name: "a proxy commits under a second configuration",
        met: |events| {
            let mut first = None;
            let mut config: BTreeMap<Ballot, &Vec<NodeId>> = BTreeMap::new();
            events.iter().any(|e| match e {
                ClusterEvent::Select { ballot, config: c } => {
                    first.get_or_insert(c);
                    config.insert(*ballot, c);
                    false
                }
                ClusterEvent::Commit { ballot, .. } => {
                    config.get(ballot).is_some_and(|c| Some(*c) != first)
                }
                _ => false,
            })
        },
    },
    Rule {
        name: "a ballot wins on half of its grid",
        met: |events| {
            let mut config: BTreeMap<Ballot, usize> = BTreeMap::new();
            let mut promised: BTreeMap<Ballot, BTreeSet<NodeId>> = BTreeMap::new();
            events.iter().any(|e| match e {
                ClusterEvent::Select { ballot, config: c } => {
                    config.insert(*ballot, c.len());
                    false
                }
                ClusterEvent::Promise {
                    acceptor, ballot, ..
                } => {
                    promised.entry(*ballot).or_default().insert(*acceptor);
                    false
                }
                ClusterEvent::Win { ballot, .. } => config.get(ballot).is_some_and(|n| {
                    // The owner promises its own ballot when it prepares.
                    let mut quorum = promised.get(ballot).cloned().unwrap_or_default();
                    quorum.insert(ballot.node);
                    2 * quorum.len() <= *n
                }),
                _ => false,
            })
        },
    },
];

/// `ReadsTrace`: the read basis of a beat, its fence, the reader's own
/// answer, a read that must see a completed write, and an answer whose
/// read fence ends the read.
const READS: &[Rule] = &[
    Rule {
        name: "a node learns its read basis from a beat",
        met: |events| {
            events.iter().any(
                |e| matches!(e, ClusterEvent::Basis { node, ballot, .. } if *node != ballot.node),
            )
        },
    },
    Rule {
        name: "a read basis carries a fence",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Basis { fence: Some(_), .. }))
        },
    },
    Rule {
        name: "a reader answers its own read",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::ReadStart { own: Some(_), .. }))
        },
    },
    Rule {
        name: "a read after a completed write is served",
        met: |events| {
            let mut applied = false;
            let mut after = BTreeSet::new();
            events.iter().any(|e| match e {
                ClusterEvent::Applied { .. } => {
                    applied = true;
                    false
                }
                ClusterEvent::ReadStart { ctx, .. } if applied => {
                    after.insert(*ctx);
                    false
                }
                ClusterEvent::ReadServe { ctx, .. } => after.contains(ctx),
                _ => false,
            })
        },
    },
    Rule {
        name: "an answer above the read basis ends the read",
        met: |events| {
            let mut basis = BTreeMap::new();
            let mut fences = BTreeMap::new();
            events.iter().any(|e| match e {
                ClusterEvent::ReadStart { ctx, ballot, .. } => {
                    basis.insert(*ctx, *ballot);
                    false
                }
                ClusterEvent::ReadAck {
                    acceptor,
                    ctx,
                    fence: Some(fence),
                    ..
                } => {
                    fences.insert((*acceptor, *ctx), *fence);
                    false
                }
                ClusterEvent::ReadFold { acceptor, ctx, .. } => fences
                    .get(&(*acceptor, *ctx))
                    .zip(basis.get(ctx))
                    .is_some_and(|(fence, basis)| fence > basis),
                _ => false,
            })
        },
    },
];

/// The rules of the compaction floor of `truncation/`.
const TRUNCATION: &[Rule] = &[
    Rule {
        name: "a node compacts its log",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Compact { .. }))
        },
    },
    Rule {
        name: "a compaction keeps a record at or above first_seq",
        met: |events| {
            events.iter().enumerate().any(|(i, e)| {
                let ClusterEvent::Compact { floor, .. } = e else {
                    return false;
                };
                let folds = events[..i].iter().filter_map(|e| match e {
                    ClusterEvent::Fold { slot, high, first } => Some((*slot, *high, *first)),
                    _ => None,
                });
                let first = folds.clone().next_back().map_or(0, |(_, _, first)| first);
                folds
                    .into_iter()
                    .any(|(slot, high, _)| slot >= *floor && high > first)
            })
        },
    },
    Rule {
        name: "a node jumps to the trim point of a peer",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Trim { .. }))
        },
    },
    Rule {
        name: "an acceptor promises after it compacted",
        met: |events| {
            events.iter().enumerate().any(|(i, e)| {
                let ClusterEvent::Compact { node, .. } = e else {
                    return false;
                };
                events[i..].iter().any(|e| {
                    matches!(e, ClusterEvent::Promise { acceptor, .. } | ClusterEvent::Page { acceptor, .. }
                        if acceptor == node)
                })
            })
        },
    },
];

/// The rules of the read replica of `compartments/`.
const COMPARTMENTS: &[Rule] = &[
    Rule {
        name: "a replica learns a slot",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::ReplicaLearn { .. }))
        },
    },
    Rule {
        name: "a replica folds a slot",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::ReplicaApply { .. }))
        },
    },
    Rule {
        name: "a replica boots again and folds its prefix again",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::ReplicaBoot { from, .. } if from.0 > 0))
        },
    },
    Rule {
        name: "a replica moves its floor",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::ReplicaFloor { .. }))
        },
    },
    Rule {
        name: "a replica serves a read opened after a completed write",
        met: |events| {
            events.iter().enumerate().any(|(i, e)| {
                let ClusterEvent::ReplicaRead { ctx, .. } = e else {
                    return false;
                };
                events[..i]
                    .iter()
                    .any(|e| matches!(e, ClusterEvent::Applied { .. }))
                    && events[i..]
                        .iter()
                        .any(|e| matches!(e, ClusterEvent::ReplicaServe { ctx: c, .. } if c == ctx))
            })
        },
    },
];

/// The rules of the faulty entries of CTRL of `truncation/`.
const FAULTS: &[Rule] = &[
    Rule {
        name: "a boot reads a faulty record",
        met: |events| events.iter().any(|e| matches!(e, ClusterEvent::Rot { .. })),
    },
    Rule {
        name: "a promise reports a faulty record",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Page { faulty, .. } if !faulty.is_empty()))
        },
    },
    Rule {
        name: "a promise comes in pages",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Page { next: Some(_), .. }))
        },
    },
    Rule {
        name: "a leader wins over a faulty report",
        met: |events| {
            let mut faulty = BTreeSet::new();
            events.iter().any(|e| match e {
                ClusterEvent::Page {
                    ballot, faulty: f, ..
                } if !f.is_empty() => {
                    faulty.insert(*ballot);
                    false
                }
                ClusterEvent::Win { ballot, .. } => faulty.contains(ballot),
                _ => false,
            })
        },
    },
];

/// The rules of `RetireTrace`.
const RETIRE: &[Rule] = &[
    Rule {
        name: "a node retires",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Retire { .. }))
        },
    },
    Rule {
        name: "a retired node rejoins",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::Rejoin { .. }))
        },
    },
    Rule {
        name: "a node that rejoined promises",
        met: |events| {
            let mut back = BTreeSet::new();
            events.iter().any(|e| match e {
                ClusterEvent::Rejoin { node } => {
                    back.insert(*node);
                    false
                }
                ClusterEvent::Promise { acceptor, .. } => back.contains(acceptor),
                _ => false,
            })
        },
    },
];

/// The rules of `HandoverTrace`.
const HANDOVER: &[Rule] = &[
    Rule {
        name: "a matchmaker freezes",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MmStop { .. }))
        },
    },
    Rule {
        name: "a bootstrap carries a registration",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MmBoot { registry, .. } if !registry.is_empty()))
        },
    },
    Rule {
        name: "a matchmaker votes in the successor decree",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MmVote { .. }))
        },
    },
    Rule {
        name: "a matchmaker activates a later generation",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MmActivate { .. }))
        },
    },
    Rule {
        name: "a matchmaker registers after a handover",
        met: |events| {
            let mut active = BTreeSet::new();
            events.iter().any(|e| match e {
                ClusterEvent::MmActivate { matchmaker, .. } => {
                    active.insert(*matchmaker);
                    false
                }
                ClusterEvent::Register { matchmaker, .. } => active.contains(matchmaker),
                _ => false,
            })
        },
    },
    Rule {
        name: "two handovers in one walk",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MmActivate { set, .. } if set.generation.0 >= 2))
        },
    },
];

impl Spec {
    /// The steps of this spec that some checked walk must take.
    #[must_use]
    pub fn rules(self) -> Vec<Rule> {
        if self == Self::Handover {
            // The handover is the matchmakers' own: no log step is its.
            return HANDOVER.to_vec();
        }
        let mut rules = LOG.to_vec();
        match self {
            Self::Handover => {}
            Self::MultiPaxos => rules.push(RESTART),
            Self::LeaderHandoff => {
                rules.push(RESTART);
                rules.extend_from_slice(HANDOFF);
            }
            Self::ProxyLeader => rules.extend_from_slice(PROXY),
            Self::Matchmaker => {
                rules.push(RESTART);
                rules.extend_from_slice(MATCHMAKER);
            }
            Self::MatchmakerGrid => {
                rules.push(RESTART);
                rules.extend_from_slice(MATCHMAKER);
                rules.extend_from_slice(GRID);
            }
            Self::Reads => {
                rules.push(RESTART);
                rules.extend_from_slice(MATCHMAKER);
                rules.extend_from_slice(READS);
            }
            Self::Truncation => {
                rules.push(RESTART);
                rules.extend_from_slice(TRUNCATION);
            }
            Self::Compartments => {
                rules.push(RESTART);
                rules.extend_from_slice(COMPARTMENTS);
            }
            Self::Faults => {
                rules.push(RESTART);
                rules.extend_from_slice(FAULTS);
            }
            Self::Retire => {
                rules.push(RESTART);
                rules.extend_from_slice(MATCHMAKER);
                rules.extend_from_slice(RETIRE);
            }
        }
        rules
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_trace_meets_no_rule() {
        for spec in [
            Spec::MultiPaxos,
            Spec::LeaderHandoff,
            Spec::ProxyLeader,
            Spec::Matchmaker,
            Spec::MatchmakerGrid,
            Spec::Reads,
            Spec::Truncation,
            Spec::Compartments,
            Spec::Faults,
            Spec::Retire,
            Spec::Handover,
        ] {
            assert!(spec.rules().iter().all(|rule| !rule.met(&[])), "{spec:?}");
        }
    }
}
