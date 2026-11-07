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
//!
//! A handover's **successor** is a shard of its own
//! (`matchmakers/<tenant>/successor`, [`successor_shard`]), declared by the
//! matchmakers, not the nodes: the decree binds the set to a successor from
//! the first vote on (a later proposer adopts the vote it finds, Paxos),
//! and a node's `next` is only the move it drives, dropped when it abandons.
//! Each matchmaker declares the successor its durable record binds it to
//! ([`bound_successor`]): the chosen set it learned, else the set it voted
//! for; a majority of that set intact and up. A matchmaker names a member
//! by its id ([`matchmaker_name`]): the simulator maps the name to the
//! member's process. Witness: sweep recipe `13484684413026399249
//! 36749:4964286755810217066` on e82e86a (#685 (no founders)): the cell
//! tenant's set of three was handed onto one member, its reconfigurer was
//! killed with the successor voted at one matchmaker, the finishers stalled
//! on a full disk for 70 s and declared no move, attrition wiped the voted
//! member, and the decree chose the wiped id after chaos: no journal of the
//! cell closed a probe again. Green with the successor declared: the gate
//! turns that wipe into a crash.
//!
//! A node declares a set only once a matchmaker quorum answered it in this
//! incarnation ([`belief_heard`]). Every incarnation boots believing the
//! bootstrap set, and a set a handover replaced is a stale belief: declared,
//! it would add a need the set it replaced can no longer meet, after the
//! gate placed its faults on the set in force (witness
//! 10575918452901073899 on #848 (a torn matchmaker registry drops its set
//! alone): a restarted node declared the five-member bootstrap set of a
//! set moved to three).

use moonpool_buggify::shard;
use moonpool_buggify::shard::{Need, Replica, Replication};
use paros_core::journal::TenantId;
use paros_core::matchmaker::MatchmakerHardState;
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

/// The shard name of `tenant`'s successor generation:
/// `matchmakers/<tenant>/successor`. Its own name, because the simulator
/// keeps one view per process and shard, and a machine's node and
/// matchmaker share a process: under the set's name the node's next
/// declaration would replace the matchmaker's.
pub(crate) fn successor_shard(tenant: TenantId) -> String {
    format!(
        "{}/successor",
        paros_storage::journal::matchmaker_shard(tenant)
    )
}

/// The replica name of matchmaker `id` where no link names its address:
/// `matchmaker-<id>:0`. The port is none; the simulator resolves the host
/// to the member's process through its gate table, and an unknown host
/// counts as intact.
pub(crate) fn matchmaker_name(id: MatchmakerId) -> Replica {
    Replica::new(format!("matchmaker-{}:0", id.0))
}

/// The successor a matchmaker's durable record binds its set to: the
/// chosen set it learned, else the set it voted for in the decree. `None`
/// while neither: a fresh or a frozen-and-unvoted generation, or an
/// activated one.
pub(crate) fn bound_successor(state: &MatchmakerHardState) -> Option<Vec<MatchmakerId>> {
    let bound = state
        .successor
        .as_ref()
        .map(|successor| successor.members().to_vec())
        .or_else(|| {
            state
                .decree
                .vote
                .as_ref()
                .map(|(_, members)| members.clone())
        });
    assert!(
        bound.as_ref().is_none_or(|members| !members.is_empty()),
        "a bound successor names a matchmaker"
    );
    bound
}

/// What a matchmaker declared last for its successor shard: the members,
/// or `None` for gone.
pub(crate) type DeclaredSuccessor = Option<Option<Vec<MatchmakerId>>>;

/// Declare `tenant`'s successor shard if `successor` changed since `last`:
/// a majority of its members intact and up, or gone once no successor
/// binds the set any more. A shard never declared is not declared gone.
pub(crate) fn declare_successor(
    tenant: TenantId,
    successor: Option<Vec<MatchmakerId>>,
    last: &mut DeclaredSuccessor,
) {
    if !shard::is_active() {
        return;
    }
    if last.as_ref() == Some(&successor) || (last.is_none() && successor.is_none()) {
        return;
    }
    let name = successor_shard(tenant);
    match &successor {
        Some(members) => {
            shard!(name, current = set_replication(members, &matchmaker_name));
        }
        None => {
            shard!(name);
        }
    }
    *last = Some(successor);
}

/// Whether `step` shows that a matchmaker quorum of the node's believed set
/// answered it: a campaign's matchmaking completed, or a membership probe
/// closed. Only then is the belief a fact the node may declare.
pub(crate) fn belief_heard(step: &paros_core::node::MatchStep) -> bool {
    use paros_core::node::MatchStep;
    matches!(
        step,
        MatchStep::Completed { .. } | MatchStep::ProbeClosed { .. }
    )
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
    fn only_a_quorum_answer_makes_a_belief_heard() {
        use paros_core::node::MatchStep;
        assert!(belief_heard(&MatchStep::Completed {
            prior: Vec::new(),
            watermark: paros_core::types::Ballot::default(),
            registered_by: 2,
            disagreements: 0,
        }));
        assert!(!belief_heard(&MatchStep::ProbeAnswered));
        assert!(!belief_heard(&MatchStep::Registered { remaining: 1 }));
        assert!(!belief_heard(&MatchStep::Ignored));
        assert!(!belief_heard(&MatchStep::UnknownMember));
    }

    /// A sink that keeps every declaration, for the tests below.
    struct Kept(std::rc::Rc<std::cell::RefCell<Vec<shard::Declaration>>>);

    impl shard::Sink for Kept {
        fn declare(&self, declaration: shard::Declaration) {
            self.0.borrow_mut().push(declaration);
        }
        fn caught_up(&self, _shard: &str, _replica: &Replica) {}
        fn intact(&self, _shard: &str) {}
    }

    #[test]
    fn a_matchmaker_is_bound_to_the_chosen_set_else_to_its_vote() {
        use paros_core::membership::{MatchmakerGeneration, MatchmakerSet};
        use paros_core::types::Ballot;
        let mut state = MatchmakerHardState::default();
        assert_eq!(bound_successor(&state), None);
        state.decree.vote = Some((Ballot::default(), vec![MatchmakerId(1)]));
        assert_eq!(bound_successor(&state), Some(vec![MatchmakerId(1)]));
        state.successor = Some(MatchmakerSet::new(
            MatchmakerGeneration(1),
            vec![MatchmakerId(2), MatchmakerId(3)],
        ));
        assert_eq!(
            bound_successor(&state),
            Some(vec![MatchmakerId(2), MatchmakerId(3)])
        );
    }

    #[test]
    fn a_successor_is_declared_once_and_declared_gone_once_unbound() {
        let kept = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        shard::set_sink(Box::new(Kept(kept.clone())));
        let mut last = DeclaredSuccessor::None;
        let tenant = TenantId(7);
        // Nothing bound, nothing declared: never-declared is not gone.
        declare_successor(tenant, None, &mut last);
        assert!(kept.borrow().is_empty());
        declare_successor(tenant, Some(vec![MatchmakerId(1)]), &mut last);
        declare_successor(tenant, Some(vec![MatchmakerId(1)]), &mut last);
        assert_eq!(kept.borrow().len(), 1);
        let first = kept.borrow()[0].clone();
        assert_eq!(first.name, "matchmakers/7/successor");
        let current = first.current.expect("a bound successor is a replication");
        assert!(!current.durable.holds(&|_| false));
        assert!(current.durable.holds(&|r| r.address() == "matchmaker-1:0"));
        assert!(first.next.is_none());
        declare_successor(tenant, None, &mut last);
        declare_successor(tenant, None, &mut last);
        assert_eq!(kept.borrow().len(), 2);
        assert!(kept.borrow()[1].current.is_none());
        shard::clear_sink();
    }

    #[test]
    fn the_matchmaker_shard_names_its_tenant() {
        assert_eq!(
            paros_storage::journal::matchmaker_shard(TenantId(7)),
            "matchmakers/7"
        );
        assert_eq!(successor_shard(TenantId(7)), "matchmakers/7/successor");
        assert_eq!(matchmaker_name(MatchmakerId(7)).address(), "matchmaker-7:0");
    }
}
