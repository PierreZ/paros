//! A journal's replication, declared with moonpool's `shard!` (#760
//! (moonpool owns fault placement)): which acceptors hold the journal, and
//! what it needs from them to make progress and to keep its chosen slots.
//! The simulator keeps every destructive fault inside these needs; in
//! production no sink is installed and nothing here runs past one
//! thread-local read.
//!
//! The leader declares, because only it knows the configurations in force
//! (`ColocatedNode::configs_in_force`): the one it runs, and every other
//! configuration of `H_b` until its GC floor is effective. Those others are
//! a **move** (`current` = the prior ones, `next` = the one in force), which
//! ends on `current` = the one in force once the floor is effective. An
//! operator reconfiguration starts a move the same way, from the
//! configuration in force to the requested one, before its campaign opens.
//!
//! A configuration's needs come from its quorum system: a value chosen by a
//! Phase-2 quorum is found again by every Phase-1 quorum, so the journal
//! keeps its slots while a Phase-1 quorum of intact copies remains
//! (`durable`), and makes progress while a Phase-1 quorum and a Phase-2
//! quorum are up (`progress`). A grid's partitions map to `All` and `Any`
//! of one need per partition.
//!
//! A tenant's **matchmaker set** is a shard too (`matchmakers/<tenant>`):
//! each member's registry is one whole copy, and the set keeps its
//! registrations, and makes progress, while a majority is intact and up. A
//! node declares the set its journals believe in force, and a handover it
//! drives is a move to the members it names. Moonpool keeps one view per
//! process, so the node declares once per tenant, never per journal.

use moonpool_buggify::shard;
use moonpool_buggify::shard::{Need, Replica, Replication};
use paros_core::journal::TenantId;
use paros_core::membership::{AcceptorConfig, MatchmakerId, QuorumSystem};
use paros_core::{ColocatedNode, NodeId};

/// What a journal declared last: the `current` and `next` it named.
pub(crate) type Declared = Option<(Replication, Option<Replication>)>;

/// The shard name of the journal `node` serves: `journal/<tenant>/<journal>`.
fn shard_name(node: &ColocatedNode) -> String {
    format!("journal/{}", node.config().journal)
}

/// Declare `node`'s replication if it changed since `last`: a leader's
/// configurations in force. A non-leader declares nothing. `replica` names
/// a member by the address its lane dials.
pub(crate) fn declare(
    node: &ColocatedNode,
    last: &mut Declared,
    replica: &dyn Fn(NodeId) -> Replica,
) {
    if !shard::is_active() {
        return;
    }
    let Some((prior, current)) = node.configs_in_force() else {
        return;
    };
    let now = replication(current, replica);
    let declared = if prior.is_empty() {
        (now, None)
    } else {
        (combined(&prior, replica), Some(now))
    };
    if last.as_ref() == Some(&declared) {
        return;
    }
    let name = shard_name(node);
    match declared.1.clone() {
        None => shard!(name, current = declared.0.clone()),
        Some(next) => shard!(name, current = declared.0.clone(), next = next),
    }
    *last = Some(declared);
}

/// Declare an operator reconfiguration's move from `old` to the one `node`
/// now registers, before its campaign's first request leaves.
pub(crate) fn declare_move(
    node: &ColocatedNode,
    old: &AcceptorConfig,
    last: &mut Declared,
    replica: &dyn Fn(NodeId) -> Replica,
) {
    if !shard::is_active() {
        return;
    }
    let Some(new) = node.matchmaking_role().map(|m| m.config().clone()) else {
        return;
    };
    if &new == old {
        return;
    }
    let declared = (replication(old, replica), Some(replication(&new, replica)));
    if last.as_ref() == Some(&declared) {
        return;
    }
    let next = declared.1.clone().unwrap_or_else(|| declared.0.clone());
    shard!(shard_name(node), current = declared.0.clone(), next = next);
    *last = Some(declared);
}

/// What a matchmaker set declared last from this process: the members its
/// `current` and `next` named.
pub(crate) type DeclaredSet = Option<(Vec<MatchmakerId>, Option<Vec<MatchmakerId>>)>;

/// Declare `tenant`'s matchmaker set if it changed since `last`: `current`
/// the members this node believes in force, `next` the members a handover
/// it drives moves the set to. `replica` names a matchmaker by the address
/// its link dials.
pub(crate) fn declare_matchmakers(
    tenant: TenantId,
    current: &[MatchmakerId],
    next: Option<Vec<MatchmakerId>>,
    last: &mut DeclaredSet,
    replica: &dyn Fn(MatchmakerId) -> Replica,
) {
    if !shard::is_active() {
        return;
    }
    let next = next.filter(|next| next.as_slice() != current && !next.is_empty());
    let declared = (current.to_vec(), next);
    if last.as_ref() == Some(&declared) {
        return;
    }
    let name = paros_storage::journal::matchmaker_shard(tenant);
    let now = set_replication(&declared.0, replica);
    match &declared.1 {
        None => shard!(name, current = now),
        Some(next) => shard!(name, current = now, next = set_replication(next, replica)),
    }
    *last = Some(declared);
}

/// A matchmaker set's replication: a majority of its members, intact to
/// keep the registrations and up to make progress (matchmaker quorums are
/// majorities only).
pub(crate) fn set_replication(
    members: &[MatchmakerId],
    replica: &dyn Fn(MatchmakerId) -> Replica,
) -> Replication {
    let replicas: Vec<Replica> = members.iter().map(|id| replica(*id)).collect();
    let majority = Need::majority(replicas.clone());
    Replication {
        replicas,
        progress: majority.clone(),
        durable: majority,
    }
}

/// One configuration's replication.
pub(crate) fn replication(
    config: &AcceptorConfig,
    replica: &dyn Fn(NodeId) -> Replica,
) -> Replication {
    let members: Vec<Replica> = config.members().iter().map(|id| replica(*id)).collect();
    let (phase1, phase2) = needs(config.quorum_system(), &members);
    Replication {
        replicas: members,
        progress: Need::All(vec![phase1.clone(), phase2]),
        durable: phase1,
    }
}

/// Several configurations as one replication: every one's needs.
fn combined(configs: &[&AcceptorConfig], replica: &dyn Fn(NodeId) -> Replica) -> Replication {
    let each: Vec<Replication> = configs.iter().map(|c| replication(c, replica)).collect();
    if let [one] = each.as_slice() {
        return one.clone();
    }
    let mut replicas: Vec<Replica> = Vec::new();
    for r in each.iter().flat_map(|r| r.replicas.iter()) {
        if !replicas.contains(r) {
            replicas.push(r.clone());
        }
    }
    Replication {
        replicas,
        progress: Need::All(each.iter().map(|r| r.progress.clone()).collect()),
        durable: Need::All(each.iter().map(|r| r.durable.clone()).collect()),
    }
}

/// A quorum system's Phase-1 and Phase-2 needs over `members`, the sorted
/// membership.
fn needs(system: QuorumSystem, members: &[Replica]) -> (Need, Need) {
    let n = members.len();
    match system {
        QuorumSystem::Grid { rows, cols, acks } => {
            // Member `i` is in column `i % cols` (row-major layout).
            let column = |c: usize| -> Vec<Replica> {
                members
                    .iter()
                    .skip(c)
                    .step_by(cols.max(1))
                    .cloned()
                    .collect()
            };
            let phase1 = (rows + 1).saturating_sub(acks);
            (
                Need::All(
                    (0..cols)
                        .map(|c| Need::AtLeast(phase1, column(c)))
                        .collect(),
                ),
                Need::Any((0..cols).map(|c| Need::AtLeast(acks, column(c))).collect()),
            )
        }
        system => (
            Need::AtLeast(system.phase1_quorum_size(n), members.to_vec()),
            Need::AtLeast(system.phase2_quorum_size(n), members.to_vec()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(id: NodeId) -> Replica {
        Replica::new(format!("10.0.0.{}", id.0))
    }

    #[test]
    fn a_majority_needs_a_majority_up_and_intact() {
        let config = AcceptorConfig::new(
            vec![NodeId(1), NodeId(2), NodeId(3)],
            QuorumSystem::Majority,
        );
        let r = replication(&config, &named);
        let up = |ids: &'static [u8]| {
            move |r: &Replica| ids.iter().any(|i| r.0 == format!("10.0.0.{i}"))
        };
        assert!(r.durable.holds(&up(&[1, 2])));
        assert!(!r.durable.holds(&up(&[1])));
        assert!(r.progress.holds(&up(&[2, 3])));
    }

    #[test]
    fn partitions_need_a_majority_of_every_partition_to_keep_data() {
        let members: Vec<NodeId> = (1..=6).map(NodeId).collect();
        let config = AcceptorConfig::new(
            members,
            QuorumSystem::Grid {
                rows: 3,
                cols: 2,
                acks: 2,
            },
        );
        let r = replication(&config, &named);
        // Columns: {1, 3, 5} and {2, 4, 6}.
        let ok = |ids: &'static [u8]| {
            move |r: &Replica| ids.iter().any(|i| r.0 == format!("10.0.0.{i}"))
        };
        assert!(r.durable.holds(&ok(&[1, 3, 2, 4])));
        assert!(!r.durable.holds(&ok(&[1, 3, 5, 2])));
        assert!(r.progress.holds(&ok(&[1, 3, 2, 4])));
    }

    fn matchmaker(id: MatchmakerId) -> Replica {
        Replica::new(format!("10.0.2.{}", id.0))
    }

    fn up(ids: &'static [u64]) -> impl Fn(&Replica) -> bool {
        move |r: &Replica| ids.iter().any(|i| r.0 == format!("10.0.2.{i}"))
    }

    #[test]
    fn a_matchmaker_set_keeps_its_registrations_on_a_majority() {
        let members: Vec<MatchmakerId> = (1..=5).map(MatchmakerId).collect();
        let r = set_replication(&members, &matchmaker);
        assert_eq!(r.replicas.len(), 5);
        assert!(r.durable.holds(&up(&[1, 2, 3])));
        assert!(!r.durable.holds(&up(&[4, 5])));
        assert!(r.progress.holds(&up(&[3, 4, 5])));
        assert!(!r.progress.holds(&up(&[1, 2])));
    }

    #[test]
    fn a_matchmaker_set_of_one_or_two_spares_no_member() {
        let two = set_replication(&[MatchmakerId(1), MatchmakerId(2)], &matchmaker);
        assert!(!two.durable.holds(&up(&[1])));
        assert!(two.durable.holds(&up(&[1, 2])));
        let one = set_replication(&[MatchmakerId(1)], &matchmaker);
        assert!(!one.durable.holds(&up(&[])));
    }

    #[test]
    fn the_matchmaker_shard_names_its_tenant() {
        assert_eq!(
            paros_storage::journal::matchmaker_shard(TenantId(7)),
            "matchmakers/7"
        );
    }
}
