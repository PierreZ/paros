//! The proxy leader's **feedback** ([`ProxyFeedback`]): its counters and
//! what its open rounds show about each column (#1564 (journal controller,
//! version 1)). Read-only.

use std::collections::{BTreeMap, BTreeSet};

use crate::contract::feedback::{ProxyFeedback, ProxyInsight};
use crate::types::NodeId;

use super::ProxyLeader;

/// What the resent rounds of one column show: the members that accepted
/// none of them, and the highest resend count.
struct ColumnWatch {
    silent: BTreeSet<NodeId>,
    resends: u64,
}

impl ProxyLeader {
    /// What this proxy reports about itself, as plain data
    /// ([`crate::contract::feedback`]).
    ///
    /// The insights come from the rounds it holds open and sent again at
    /// least one time. For each column of such rounds, a member is
    /// **silent** when it accepted none of them ([`ProxyInsight::ColumnSilent`]),
    /// and the column is **blocked** when the members that are not silent
    /// are not a Phase-2 quorum of it ([`ProxyInsight::ColumnBlocked`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn feedback(&self) -> ProxyFeedback {
        let config = self.acceptors();
        let mut columns: BTreeMap<Option<usize>, ColumnWatch> = BTreeMap::new();
        for round in self.rounds().by_slot().values() {
            if round.resends() == 0 {
                continue;
            }
            let accepted = round
                .accepted_by()
                .expect("a proxy folds its own rounds: each is colocated here");
            let column = round.column();
            let unanswered: BTreeSet<NodeId> = config
                .phase2_addressees(column)
                .into_iter()
                .filter(|id| !accepted.contains(id))
                .collect();
            let watch = columns.entry(column).or_insert_with(|| ColumnWatch {
                silent: unanswered.clone(),
                resends: 0,
            });
            watch.silent = watch.silent.intersection(&unanswered).copied().collect();
            watch.resends = watch.resends.max(round.resends());
        }
        let mut insights = Vec::new();
        for (column, watch) in columns {
            assert!(watch.resends > 0, "only a resent round is watched");
            if watch.silent.is_empty() {
                continue;
            }
            let heard: BTreeSet<NodeId> = config
                .phase2_addressees(column)
                .into_iter()
                .filter(|id| !watch.silent.contains(id))
                .collect();
            let blocked = !config.has_phase2_quorum_in(&heard, column);
            insights.push(ProxyInsight::ColumnSilent {
                column,
                silent: watch.silent.into_iter().collect(),
                resends: watch.resends,
            });
            if blocked {
                insights.push(ProxyInsight::ColumnBlocked { column });
            }
        }
        let counters = self.counters();
        let feedback = ProxyFeedback {
            id: self.id(),
            ballot: self.ballot(),
            pool: self.pool().to_vec(),
            open_rounds: u64::try_from(self.rounds().by_slot().len())
                .expect("the open rounds fit in a u64"),
            decided: counters.decided,
            expired: counters.expired,
            relayed_nacks: counters.relayed_nacks,
            insights,
        };
        assert!(
            feedback.insights.iter().all(|insight| match insight {
                ProxyInsight::ColumnSilent { silent, .. } =>
                    silent.iter().all(|id| config.contains(*id)),
                ProxyInsight::ColumnBlocked { .. } => true,
            }),
            "a silent member is a member of the proxy's configuration"
        );
        feedback
    }
}
