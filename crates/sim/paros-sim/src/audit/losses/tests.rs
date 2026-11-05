//! The storage-loss oracles of [`AuditState`] on hand-built plans: a loss
//! an outage planned stays clean, and an unplanned or impossible one goes
//! red.

use paros::core::NodeId;
use paros::core::membership::{AcceptorConfig, QuorumSystem};

use crate::audit::state::AuditState;
use crate::audit::testing::red;

fn three() -> AuditState {
    AuditState {
        bootstrap: Some(AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        )),
        ..AuditState::default()
    }
}

#[test]
fn an_outage_damages_only_holders_of_the_slot() {
    let mut state = three();
    assert!(!red(|| state.note_outage_loss(4, &[0, 1], &[1])));
    assert!(red(|| state.note_outage_loss(5, &[0, 1], &[2])));
    let planned = &state.losses.planned[&4];
    assert!(planned.holders.contains(&0) && planned.damaged.contains(&1));
}

#[test]
fn a_lost_copy_is_one_an_outage_planned() {
    let mut state = three();
    assert!(red(|| state.note_copy_lost(1, 4)));
    state.note_outage_loss(4, &[0, 1, 2], &[1]);
    assert!(!red(|| state.note_copy_lost(1, 4)));
    let planned = &state.losses.planned[&4];
    assert!(planned.lost.contains(&1));
    assert!(planned.shape.is_some(), "every planned loss landed");
}

#[test]
fn a_faulty_copy_of_a_planned_slot_is_lost_and_a_learners_never() {
    let mut state = three();
    state.note_outage_loss(4, &[0, 1, 2], &[0, 1]);
    state.note_faulty_copies(2, &[4, 9]);
    assert!(state.losses.planned[&4].lost.contains(&2));
    assert!(!state.losses.planned.contains_key(&9), "an unplanned slot");
    state.replicas.insert(0);
    state.note_faulty_copies(0, &[4]);
    assert!(!state.losses.planned[&4].lost.contains(&0));
}

#[test]
fn an_unrecoverable_slot_is_never_accepted_again() {
    let mut state = three();
    assert!(
        !red(|| state.loss_accepted(0, 4, 7)),
        "a run with no planned loss judges nothing"
    );
    state.note_outage_loss(4, &[0, 1, 2], &[1]);
    state.note_copy_lost(1, 4);
    assert!(!red(|| state.loss_accepted(1, 4, 7)));
    assert!(
        !state.losses.planned[&4].lost.contains(&1),
        "a rewritten copy is lost no longer"
    );
    state.losses.unrecoverable.insert(4);
    assert!(red(|| state.loss_accepted(2, 4, 7)));
}
