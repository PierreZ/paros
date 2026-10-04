//! The matchmaker set's fan-out (#190): one set serves every journal of its
//! tenant, one registry per journal, while the oracles live in one audit
//! world per journal. A report about one journal's registry (a
//! registration, a watermark raise, a reply) goes to that journal's world;
//! a report about the set (a generation's scalars, a boot, an activation, a
//! freeze page, a handover step or reconstruction) goes to every journal's
//! world, projected onto its journal — so each world's
//! [`MatchmakerAudit`](super::matchmaker::MatchmakerAudit) judges the whole
//! handover through the registry it owns.
//!
//! Three gates live here because they are cross-journal by nature: a
//! handover that carried more than one journal's registry, a campaign that
//! completed in one journal while another of the same node was still
//! matchmaking, and one journal's floor raised above another journal's
//! already-raised floor at the same matchmaker.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::assert_sometimes;
use paros::{Ballot, JournalKey, MatchmakerId, NodeId, Registration};

use super::world::{AuditWorld, audit_world_for};
use crate::shape::JournalPlan;

const SET_BOARD_KEY: &str = "paros-set-board";

/// Every journal the set serves, each with its audit world, in journal
/// order. Empty on a port outside a matchmaker deployment.
pub(crate) type SetWorlds = Arc<[(JournalKey, Arc<AuditWorld>)]>;

/// The run's cross-journal matchmaking record: which `(node, journal)`
/// campaigns are open, for the gate that wants two journals of one node
/// matchmaking at once.
#[derive(Default)]
pub(crate) struct SetBoard {
    open: BTreeSet<(u64, JournalKey)>,
}

impl SetBoard {
    /// `node` opened a campaign in `journal`.
    pub(crate) fn opened(&mut self, node: NodeId, journal: JournalKey) {
        self.open.insert((node.0, journal));
    }

    /// `node`'s campaign in `journal` ended (closed, refused, abandoned, or
    /// the node rebooted).
    pub(crate) fn closed(&mut self, node: NodeId, journal: JournalKey) {
        self.open.remove(&(node.0, journal));
    }

    /// `node`'s campaign in `journal` completed: the gate's outcome is
    /// whether another journal of the node was mid-matchmaking.
    pub(crate) fn completed(&mut self, node: NodeId, journal: JournalKey, journals: usize) {
        self.closed(node, journal);
        if journals > 1 {
            let concurrent = self
                .open
                .range((node.0, JournalKey::UNSET)..)
                .take_while(|(n, _)| *n == node.0)
                .next()
                .is_some();
            assert_sometimes!(
                concurrent,
                "matchmaking: a campaign completes while another journal of its node is matchmaking"
            );
        }
    }
}

/// The run's matchmaking board, published once per iteration.
pub(crate) fn set_board(state: &moonpool_sim::StateHandle) -> Arc<Mutex<SetBoard>> {
    crate::state::published(state, SET_BOARD_KEY, SetBoard::default)
}

/// The journals a matchmaker deployment's set serves (#190): every journal
/// of the plan in the deployment's tenant — the default journal's — each
/// with its audit world. A journal of another tenant runs plain
/// Multi-Paxos, outside the set.
pub(crate) fn set_worlds(state: &moonpool_sim::StateHandle, plan: &JournalPlan) -> SetWorlds {
    plan.ids
        .iter()
        .copied()
        .filter(|journal| journal.tenant == JournalKey::default().tenant)
        .map(|journal| (journal, audit_world_for(state, journal)))
        .collect()
}

/// Lock the board, through a poisoned lock too (a panic elsewhere already
/// failed the run).
pub(crate) fn lock(board: &Mutex<SetBoard>) -> std::sync::MutexGuard<'_, SetBoard> {
    board.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One journal's registry out of a per-journal map (empty when the journal
/// holds none here), borrowed: a fan-out never copies a registry.
pub(crate) fn registry_of(
    registries: &BTreeMap<JournalKey, BTreeMap<Ballot, Registration>>,
    journal: JournalKey,
) -> &BTreeMap<Ballot, Registration> {
    static EMPTY: BTreeMap<Ballot, Registration> = BTreeMap::new();
    registries.get(&journal).unwrap_or(&EMPTY)
}

/// The activation gate: a handover carried more than one journal's
/// registry, on a set serving several.
pub(crate) fn activation_gate(
    registries: &BTreeMap<JournalKey, BTreeMap<Ballot, Registration>>,
    journals: usize,
) {
    if journals > 1 {
        let carried = registries.values().filter(|r| !r.is_empty()).count();
        assert_sometimes!(
            carried > 1,
            "generation: a handover carries more than one journal's registry"
        );
    }
}

/// The floor gate: `matchmaker` raised `journal`'s watermark to
/// `watermark` while another journal of the set already held a raised
/// floor strictly below it there — each journal's leader raises its own,
/// and one journal's GC never drags another's floor along.
pub(crate) fn watermark_gate(
    worlds: &[(JournalKey, Arc<AuditWorld>)],
    matchmaker: MatchmakerId,
    journal: JournalKey,
    watermark: Ballot,
) {
    if worlds.len() > 1 {
        let independent = worlds
            .iter()
            .filter(|(other, _)| *other != journal)
            .any(|(_, world)| {
                let floor = world.matchmaker_watermark(matchmaker);
                floor > Ballot::zero() && floor < watermark
            });
        assert_sometimes!(
            independent,
            "gc: one journal's watermark rises without another's"
        );
    }
}
