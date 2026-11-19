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

use paros_consensus::Ballot;

use super::{CellEvent, Cmd, Spec};

/// A step of a trace spec that some checked walk must take.
#[derive(Clone, Copy, Debug)]
pub struct Rule {
    /// What the step is, in the words of the spec.
    pub name: &'static str,
    met: fn(&[CellEvent]) -> bool,
}

impl Rule {
    /// Whether the kept events of one walk take this step.
    #[must_use]
    pub fn met(&self, events: &[CellEvent]) -> bool {
        (self.met)(events)
    }
}

/// Every log spec: Phase 1 with a vote to recover, Phase 2, the learn, the
/// frontier above slot 0, the `Noop` gap fill and the record of a learn.
const LOG: &[Rule] = &[
    Rule {
        name: "a promise reports a vote",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, CellEvent::Promise { votes, .. } if !votes.is_empty()))
        },
    },
    Rule {
        name: "a leader wins with a frontier above slot 0",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, CellEvent::Win { frontier, .. } if frontier.0 > 0))
        },
    },
    Rule {
        name: "three ballots win in one walk",
        met: |events| {
            events
                .iter()
                .filter(|e| matches!(e, CellEvent::Win { .. }))
                .count()
                >= 3
        },
    },
    Rule {
        name: "a leader fills a gap with a Noop",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, CellEvent::Propose { cmd: Cmd::Noop, .. }))
        },
    },
    Rule {
        name: "an acceptor votes",
        met: |events| events.iter().any(|e| matches!(e, CellEvent::Accept { .. })),
    },
    Rule {
        name: "a node learns a slot chosen",
        met: |events| events.iter().any(|e| matches!(e, CellEvent::Learn { .. })),
    },
    Rule {
        name: "a learn records the chosen vote below the promise",
        met: |events| events.iter().any(|e| matches!(e, CellEvent::Record { .. })),
    },
];

/// The log specs that keep the restarts.
const RESTART: Rule = Rule {
    name: "a node boots again from its disk",
    met: |events| {
        events
            .iter()
            .any(|e| matches!(e, CellEvent::Restart { .. }))
    },
};

/// `LeaderHandoffTrace`: a handoff with a pending slot, and its install.
const HANDOFF: &[Rule] = &[
    Rule {
        name: "a leader hands on a tail with a pending slot",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, CellEvent::Relinquish { pending, .. } if !pending.is_empty()))
        },
    },
    Rule {
        name: "a successor installs the ballot it was handed",
        met: |events| {
            events
                .iter()
                .any(|e| matches!(e, CellEvent::Install { .. }))
        },
    },
];

/// `ProxyLeaderTrace`: a proxy's commit, and a commit of a proxy that
/// crashed before and lost its rounds.
const PROXY: &[Rule] = &[
    Rule {
        name: "a proxy commits a round",
        met: |events| events.iter().any(|e| matches!(e, CellEvent::Commit { .. })),
    },
    Rule {
        name: "a proxy commits a round after a crash",
        met: |events| {
            let mut crashed = BTreeSet::new();
            events.iter().any(|e| match e {
                CellEvent::Crash { proxy } => {
                    crashed.insert(*proxy);
                    false
                }
                CellEvent::Commit { proxy, .. } => crashed.contains(proxy),
                _ => false,
            })
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
                    CellEvent::Select { config, .. } => Some(config),
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
                .any(|e| matches!(e, CellEvent::Prepare { prior, .. } if prior.len() >= 2))
        },
    },
    Rule {
        name: "a leader asks for GC above frontier 0",
        met: |events| {
            let mut frontier: BTreeMap<Ballot, u64> = BTreeMap::new();
            events.iter().any(|e| match e {
                CellEvent::Win {
                    ballot,
                    frontier: f,
                    ..
                } => {
                    frontier.insert(*ballot, f.0);
                    false
                }
                CellEvent::GcAsk { ballot } => frontier.get(ballot).is_some_and(|f| *f > 0),
                _ => false,
            })
        },
    },
    Rule {
        name: "a matchmaker acks a GC request",
        met: |events| events.iter().any(|e| matches!(e, CellEvent::GcAck { .. })),
    },
];

impl Spec {
    /// The steps of this spec that some checked walk must take.
    #[must_use]
    pub fn rules(self) -> Vec<Rule> {
        let mut rules = LOG.to_vec();
        match self {
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
        ] {
            assert!(spec.rules().iter().all(|rule| !rule.met(&[])), "{spec:?}");
        }
    }
}
