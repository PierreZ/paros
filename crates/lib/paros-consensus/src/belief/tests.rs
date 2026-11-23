//! The follower rule and the read basis of [`Belief`], one rule per test.

use super::{Belief, Judgement};
use crate::membership::{AcceptorConfig, QuorumSystem};
use crate::quorum_read::ReadBasis;
use crate::types::{Ballot, NodeId, Slot};

impl Belief {
    /// The fields, for a test that breaks the invariants of a deployment
    /// on purpose.
    pub(crate) fn tamper(
        &mut self,
    ) -> (
        &mut AcceptorConfig,
        &mut Ballot,
        &mut Option<ReadBasis<NodeId>>,
    ) {
        (&mut self.config, &mut self.since, &mut self.basis)
    }
}

fn majority(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        QuorumSystem::Majority,
    )
}

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn pool(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

#[test]
fn a_matchmaker_belief_learns_a_newer_configuration_from_the_pool() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    let pool = pool(&[0, 1, 2, 3]);
    let judged = belief.learn(ballot(1, 0), Some(majority(&[1, 2, 3])), &pool);
    assert_eq!(judged, Judgement::Learn);
    assert_eq!(belief.config(), &majority(&[1, 2, 3]));
    assert_eq!(belief.since(), ballot(1, 0));
}

#[test]
fn a_ballot_not_above_the_belief_teaches_nothing() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    let pool = pool(&[0, 1, 2, 3]);
    let _ = belief.learn(ballot(2, 0), Some(majority(&[1, 2, 3])), &pool);
    for stale in [ballot(2, 0), ballot(1, 9)] {
        let judged = belief.learn(stale, Some(majority(&[0, 1, 2])), &pool);
        assert_eq!(judged, Judgement::NotNewer);
    }
    assert_eq!(belief.config(), &majority(&[1, 2, 3]));
    assert_eq!(belief.since(), ballot(2, 0));
}

#[test]
fn a_configuration_outside_the_pool_is_ignored_whole() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    let judged = belief.learn(ballot(1, 0), Some(majority(&[1, 2, 7])), &pool(&[0, 1, 2]));
    assert_eq!(judged, Judgement::OutsidePool);
    assert_eq!(belief.config(), &majority(&[0, 1, 2]));
    assert_eq!(belief.since(), Ballot::zero());
}

#[test]
fn a_plain_belief_never_moves() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), false);
    let judged = belief.learn(ballot(5, 1), Some(majority(&[0, 1])), &pool(&[0, 1, 2]));
    assert_eq!(judged, Judgement::Plain);
    assert_eq!(belief.config(), &majority(&[0, 1, 2]));
    assert_eq!(belief.since(), Ballot::zero());
    assert!(!belief.has_matchmakers());
}

#[test]
fn a_message_with_no_configuration_teaches_nothing() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    assert_eq!(
        belief.learn(ballot(1, 0), None, &pool(&[0, 1, 2])),
        Judgement::Absent
    );
    assert_eq!(belief.since(), Ballot::zero());
}

#[test]
fn bind_moves_the_belief_with_no_rule_even_below_it() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    belief.bind(majority(&[1, 2, 3]), ballot(4, 1));
    belief.bind(majority(&[0, 1, 2]), ballot(2, 0));
    assert_eq!(belief.config(), &majority(&[0, 1, 2]));
    assert_eq!(belief.since(), ballot(2, 0));
}

#[test]
#[should_panic(expected = "only a matchmaker deployment moves its configuration")]
fn a_plain_belief_refuses_a_bind() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), false);
    belief.bind(majority(&[0, 1]), ballot(1, 0));
}

#[test]
fn a_plain_read_basis_is_the_static_configuration_with_no_fence() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), false);
    let pool = pool(&[0, 1, 2]);
    assert!(!belief.learn_read_basis(&majority(&[0, 1, 2]), ballot(1, 0), Some(Slot(3)), &pool));
    let basis = belief
        .read_basis(&pool)
        .expect("a plain basis is always there");
    assert_eq!(basis.config, majority(&[0, 1, 2]));
    assert_eq!(basis.since, Ballot::zero());
    assert_eq!(basis.fence, None);
    assert!(belief.stored_basis().is_none());
}

#[test]
fn a_matchmaker_read_basis_moves_forward_only_and_from_the_pool_only() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    let pool = pool(&[0, 1, 2, 3]);
    assert!(
        belief.read_basis(&pool).is_none(),
        "no basis before a won leadership"
    );
    assert!(belief.learn_read_basis(&majority(&[0, 1, 2]), ballot(2, 0), Some(Slot(4)), &pool));
    // The same ballot again is learned: a later beat of one leadership.
    assert!(belief.learn_read_basis(&majority(&[0, 1, 2]), ballot(2, 0), Some(Slot(5)), &pool));
    assert!(!belief.learn_read_basis(&majority(&[1, 2, 3]), ballot(1, 3), None, &pool));
    assert!(!belief.learn_read_basis(&majority(&[1, 2, 9]), ballot(3, 1), None, &pool));
    let basis = belief
        .read_basis(&pool)
        .expect("a won leadership was heard");
    assert_eq!(basis.since, ballot(2, 0));
    assert_eq!(basis.fence, Some(Slot(5)));
    assert_eq!(belief.stored_basis(), Some(&basis));
}

#[test]
fn a_belief_kept_by_its_rules_holds_its_invariants() {
    let pool = pool(&[0, 1, 2, 3]);
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    belief.assert_invariants(&pool);
    let _ = belief.learn(ballot(1, 0), Some(majority(&[1, 2, 3])), &pool);
    assert!(belief.learn_read_basis(&majority(&[1, 2, 3]), ballot(1, 0), None, &pool));
    belief.assert_invariants(&pool);
    Belief::new(majority(&[0, 1, 2]), false).assert_invariants(&pool);
}

#[test]
#[should_panic(expected = "a plain belief is bound to no ballot")]
fn a_plain_belief_bound_to_a_ballot_breaks_the_invariants() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), false);
    *belief.tamper().1 = ballot(1, 0);
    belief.assert_invariants(&pool(&[0, 1, 2]));
}

#[test]
#[should_panic(expected = "a plain belief stores no read basis")]
fn a_plain_belief_with_a_basis_breaks_the_invariants() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), false);
    *belief.tamper().2 = Some(ReadBasis {
        config: majority(&[0, 1, 2]),
        since: ballot(1, 0),
        fence: None,
    });
    belief.assert_invariants(&pool(&[0, 1, 2]));
}

#[test]
#[should_panic(expected = "a belief's configuration is drawn from the pool")]
fn a_configuration_outside_the_pool_breaks_the_invariants() {
    let belief = Belief::new(majority(&[0, 1, 2]), true);
    belief.assert_invariants(&pool(&[0, 1]));
}

#[test]
#[should_panic(expected = "a belief's read basis is drawn from the pool")]
fn a_basis_outside_the_pool_breaks_the_invariants() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    *belief.tamper().2 = Some(ReadBasis {
        config: majority(&[0, 1, 9]),
        since: ballot(1, 0),
        fence: None,
    });
    belief.assert_invariants(&pool(&[0, 1, 2]));
}

#[test]
#[should_panic(expected = "a read basis is a won ballot's, never the bootstrap's")]
fn a_basis_at_the_zero_ballot_breaks_the_invariants() {
    let mut belief = Belief::new(majority(&[0, 1, 2]), true);
    *belief.tamper().2 = Some(ReadBasis {
        config: majority(&[0, 1, 2]),
        since: Ballot::zero(),
        fence: None,
    });
    belief.assert_invariants(&pool(&[0, 1, 2]));
}
