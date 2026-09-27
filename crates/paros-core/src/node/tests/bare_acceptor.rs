//! The **bare acceptor** (#144): a `ColocatedNode` constructed with
//! `Application::Shed` votes, learns and keeps the chosen prefix exactly as a
//! colocated node does, and hands the application nothing.

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::state::Application;

/// A three-node cluster whose node 2 is a bare acceptor.
fn cluster_with_a_bare_acceptor() -> [ColocatedNode; 3] {
    let members = [0, 1, 2];
    std::array::from_fn(|i| {
        let mut storage = TestStorage::new(i as u64, &members);
        if i == 2 {
            storage.config.application = Application::Shed;
        }
        ColocatedNode::new(&storage)
    })
}

/// [`deliver_all`], also collecting every `committed` batch each node
/// surfaced, by node index.
fn deliver_collecting(
    nodes: &mut [ColocatedNode],
    mut queue: Vec<(NodeId, Message)>,
    committed: &mut [Vec<(Slot, Command)>],
) {
    while let Some((to, msg)) = queue.pop() {
        let idx = nodes
            .iter()
            .position(|n| n.config().id == to)
            .expect("addressed to a member");
        nodes[idx].step(msg);
        let (sent, applied) = drain_with(&mut nodes[idx], |ready| ready.committed().to_vec());
        committed[idx].extend(applied);
        queue.extend(sent);
    }
}

#[test]
fn a_bare_acceptor_learns_the_prefix_and_applies_nothing() {
    let mut nodes = cluster_with_a_bare_acceptor();
    make_leader(&mut nodes, 0);
    let mut committed = vec![Vec::new(), Vec::new(), Vec::new()];
    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let r = nodes[0].propose(ClientId(1), ClientSeq(seq), val(b));
        assert!(matches!(r, ProposeResult::Accepted(_)));
        let (q, applied) = drain_with(&mut nodes[0], |ready| ready.committed().to_vec());
        committed[0].extend(applied);
        deliver_collecting(&mut nodes, q, &mut committed);
    }
    // The bare acceptor holds the learner's state: the durable chosen index,
    // the chosen values, the authoritative records and the ledger.
    assert_eq!(nodes[2].hard_state().chosen_index, Some(Slot(2)));
    for (slot, b) in [(0, 10), (1, 20), (2, 30)] {
        assert_eq!(chosen_at(&nodes[2], slot), Some(val(b)));
        assert!(nodes[2].acceptor().record(Slot(slot)).is_some());
    }
    assert_eq!(
        nodes[2].replica().applied_at(ClientId(1), ClientSeq(2)),
        Some(Slot(1)),
        "the ledger is the walk's, not the application's"
    );
    // ... and applied nothing, while the colocated nodes applied every slot.
    assert!(
        committed[2].is_empty(),
        "a bare acceptor never emits committed"
    );
    assert_eq!(committed[0].len(), 3);
    assert_eq!(committed[1].len(), 3);
}

#[test]
fn a_bare_acceptor_leads_and_truncates_on_a_decided_truncate() {
    let mut nodes = cluster_with_a_bare_acceptor();
    make_leader(&mut nodes, 2);
    let mut committed = vec![Vec::new(), Vec::new(), Vec::new()];
    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let _ = nodes[2].propose(ClientId(1), ClientSeq(seq), val(b));
        let q = drain(&mut nodes[2]);
        deliver_collecting(&mut nodes, q, &mut committed);
    }
    // A retried identity is answered from the ledger's fast path, as on any
    // leader: chosen-ness is a learner fact, not an application one.
    assert!(matches!(
        nodes[2].propose(ClientId(1), ClientSeq(2), val(20)),
        ProposeResult::Chosen(Slot(1))
    ));
    let r = nodes[2].propose_control(Control::Truncate { up_to: Slot(1) });
    assert!(matches!(r, ProposeResult::Accepted(Slot(3))));
    let q = drain(&mut nodes[2]);
    deliver_collecting(&mut nodes, q, &mut committed);
    // Applying the decided `Truncate` compacted the bare acceptor's log as it
    // did every colocated node's.
    assert_eq!(nodes[2].acceptor().first_slot(), Slot(2));
    assert_eq!(nodes[0].acceptor().first_slot(), Slot(2));
    assert!(committed[2].is_empty());
    assert_eq!(
        committed[0].last().map(|(s, _)| *s),
        Some(Slot(3)),
        "a colocated follower applied the control slot"
    );
}

#[test]
#[should_panic(expected = "only a node that runs the application opens an application repair")]
fn a_bare_acceptor_never_opens_an_application_repair() {
    let mut nodes = cluster_with_a_bare_acceptor();
    nodes[2].open_app_repair(Slot(0));
}
