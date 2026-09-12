//! Phase-2 delegation to a proxy leader (#142).
//!
//! The model checker (`proxy_model`) owns correctness; these tests pin the
//! *shape* of the wiring — what a delegation puts on the wire, what the
//! leader keeps, how a round comes back — so a regression names the rule
//! it broke.

use super::{
    ClientId, ClientSeq, ColocatedNode, Delegation, Message, NO_CHECK_QUORUM, NodeId, NodeRole,
    Party, ProposeResult, Slot, TestStorage, deliver_all, drain, make_leader, node, ucmd, val,
};
use crate::membership::ProxyId;
use crate::message::Audience;
use crate::proxy_leader::ProxyLeader;
use crate::types::command_fingerprint;

/// A three-node plain deployment with `proxies` proxy leaders.
fn proxied_node(id: u64, proxies: usize) -> ColocatedNode {
    let mut storage = TestStorage::new(id, &[0, 1, 2]);
    storage.config.proxy_count = proxies;
    ColocatedNode::new(&storage)
}

fn proxied_cluster(proxies: usize) -> [ColocatedNode; 3] {
    let mut nodes = [
        proxied_node(0, proxies),
        proxied_node(1, proxies),
        proxied_node(2, proxies),
    ];
    make_leader(&mut nodes, 0);
    nodes
}

/// The messages a node queued this batch, audience and all.
fn raw(n: &mut ColocatedNode) -> Vec<(Audience, Message)> {
    let ready = n.ready();
    let out = ready.messages().to_vec();
    ready.advance();
    n.advance_recovery();
    out
}

/// A deployment without proxies delegates nothing, whatever the driver
/// asks: the plain path is the `None` arm, message for message.
#[test]
fn a_plain_deployment_never_delegates() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    assert!(matches!(
        nodes[0].propose_in(ClientId(1), ClientSeq(1), val(1), None, Delegation::Auto),
        ProposeResult::Accepted(Slot(0))
    ));
    assert!(nodes[0].delegated_rounds().is_empty());
    let out = raw(&mut nodes[0]);
    assert!(out.iter().all(|(audience, m)| {
        !matches!(audience, Audience::Proxy(_))
            && !matches!(
                m,
                Message::Accept {
                    reply_to: Party::Proxy(_),
                    ..
                }
            )
    }));
    assert!(matches!(
        out.iter()
            .find(|(_, m)| matches!(m, Message::Accept { .. })),
        Some((
            Audience::AcceptorsOf { .. },
            Message::Accept {
                reply_to: Party::Node(NodeId(0)),
                leader: NodeId(0),
                config: None,
                ..
            }
        ))
    ));
}

/// A delegated round: one `Accept` to the proxy with the proxy as the reply
/// party and the leader as the hint, a record in the leader's own log, no
/// vote in its tally, and the round closed by the proxy's `Commit`.
#[test]
fn a_delegated_round_is_handed_to_the_proxy_and_closed_by_its_commit() {
    let mut nodes = proxied_cluster(2);
    let ballot = nodes[0].ballot();
    assert!(matches!(
        nodes[0].propose(ClientId(1), ClientSeq(1), val(1)),
        ProposeResult::Accepted(Slot(0))
    ));
    assert_eq!(nodes[0].delegated_rounds(), vec![(Slot(0), ProxyId(0))]);
    assert_eq!(
        nodes[0].acceptor().record(Slot(0)),
        Some(&(ballot, ucmd(1, 1, 1))),
        "the allocator is durable: the leader records the round it delegated"
    );
    let out = raw(&mut nodes[0]);
    assert_eq!(
        out,
        vec![(
            Audience::Proxy(ProxyId(0)),
            Message::Accept {
                reply_to: Party::Proxy(ProxyId(0)),
                leader: NodeId(0),
                ballot,
                slot: Slot(0),
                command: ucmd(1, 1, 1),
                config: None,
            }
        )],
        "a delegation is the one Accept, to the proxy, and nothing else"
    );
    // A stray Accepted reaching the leader for its delegated round is not
    // counted: the votes are the proxy's.
    nodes[0].step(Message::Accepted {
        from: NodeId(1),
        ballot,
        slot: Slot(0),
        vhash: command_fingerprint(&ucmd(1, 1, 1)),
    });
    nodes[0].step(Message::Accepted {
        from: NodeId(2),
        ballot,
        slot: Slot(0),
        vhash: command_fingerprint(&ucmd(1, 1, 1)),
    });
    assert!(
        nodes[0].replica().chosen_at(Slot(0)).is_none(),
        "a delegated round never decides at the leader"
    );
    // The proxy's Commit closes it.
    nodes[0].step(Message::Commit {
        from: Party::Proxy(ProxyId(0)),
        ballot,
        slot: Slot(0),
        command: ucmd(1, 1, 1),
    });
    assert!(nodes[0].proposer().rounds().is_empty());
    assert!(nodes[0].delegated_rounds().is_empty());
    assert_eq!(nodes[0].replica().chosen_index(), Some(Slot(0)));
}

/// The driver's overrides: a named proxy, or colocated on a proxied
/// deployment; a proxy the deployment does not have is a programmer error.
#[test]
fn the_driver_may_name_the_proxy_or_run_the_round_colocated() {
    let mut nodes = proxied_cluster(3);
    assert!(matches!(
        nodes[0].propose_in(
            ClientId(1),
            ClientSeq(1),
            val(1),
            None,
            Delegation::To(ProxyId(2))
        ),
        ProposeResult::Accepted(Slot(0))
    ));
    assert_eq!(nodes[0].delegated_rounds(), vec![(Slot(0), ProxyId(2))]);
    assert!(matches!(
        nodes[0].propose_in(
            ClientId(1),
            ClientSeq(2),
            val(2),
            None,
            Delegation::Colocated
        ),
        ProposeResult::Accepted(Slot(1))
    ));
    assert_eq!(nodes[0].delegated_rounds(), vec![(Slot(0), ProxyId(2))]);
    let out = raw(&mut nodes[0]);
    let accepts: Vec<&Audience> = out
        .iter()
        .filter(|(_, m)| matches!(m, Message::Accept { .. }))
        .map(|(a, _)| a)
        .collect();
    assert!(matches!(accepts[0], Audience::Proxy(ProxyId(2))));
    assert!(matches!(accepts[1], Audience::AcceptorsOf { .. }));
}

#[test]
#[should_panic(expected = "a delegated round names a proxy of the deployment")]
fn naming_a_proxy_the_deployment_lacks_is_a_programmer_error() {
    let mut nodes = proxied_cluster(2);
    let _ = nodes[0].propose_in(
        ClientId(1),
        ClientSeq(1),
        val(1),
        None,
        Delegation::To(ProxyId(2)),
    );
}

/// A re-send re-delegates; after the budget the leader takes the round
/// back and runs it colocated, deciding it from the acceptors' own
/// `Accepted`s — the liveness under a dead proxy.
#[test]
fn a_stalled_delegation_is_taken_back_and_decided_colocated() {
    let mut nodes = proxied_cluster(1);
    let ballot = nodes[0].ballot();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(1), val(1));
    let _ = raw(&mut nodes[0]); // the delegation, lost with the dead proxy
    for _ in 0..2 {
        nodes[0].resend_pending();
        let out = raw(&mut nodes[0]);
        assert!(
            out.iter()
                .all(|(a, m)| matches!(a, Audience::Proxy(ProxyId(0)))
                    && matches!(
                        m,
                        Message::Accept {
                            reply_to: Party::Proxy(ProxyId(0)),
                            ..
                        }
                    )),
            "a re-send of a delegated round is a re-delegation"
        );
    }
    nodes[0].take_back_delegated(3);
    assert_eq!(
        nodes[0].delegated_rounds(),
        vec![(Slot(0), ProxyId(0))],
        "under budget: still delegated"
    );
    nodes[0].take_back_delegated(2);
    assert!(nodes[0].delegated_rounds().is_empty(), "taken back");
    assert!(nodes[0].proposer().rounds().contains_key(&Slot(0)));
    let out = raw(&mut nodes[0]);
    assert!(
        matches!(
            out.as_slice(),
            [(
                Audience::AcceptorsOf { .. },
                Message::Accept {
                    reply_to: Party::Node(NodeId(0)),
                    leader: NodeId(0),
                    ..
                }
            )]
        ),
        "the taken-back round fans out colocated"
    );
    let to_send: Vec<(NodeId, Message)> = out
        .into_iter()
        .flat_map(|(a, m)| {
            a.resolve(&[NodeId(0), NodeId(1), NodeId(2)], NodeId(0))
                .into_iter()
                .map(move |to| (to, m.clone()))
        })
        .collect();
    deliver_all(&mut nodes, to_send);
    for n in &nodes {
        assert_eq!(n.replica().chosen_at(Slot(0)), Some(&ucmd(1, 1, 1)));
    }
    assert_eq!(nodes[0].ballot(), ballot, "no election was needed");
}

/// A handoff successor re-delegates every inherited pending round with
/// `leader` naming itself.
#[test]
fn a_handoff_successor_redelegates_its_inherited_rounds() {
    let mut nodes = proxied_cluster(2);
    let ballot = nodes[0].ballot();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(1), val(1));
    let _ = raw(&mut nodes[0]); // the delegation is lost
    let receipt = nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    assert_eq!(receipt.pending, 1, "the delegated round travels as pending");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[1].is_leader());
    assert_eq!(nodes[1].delegated_rounds(), vec![(Slot(0), ProxyId(0))]);
    nodes[1].resend_pending();
    let out = raw(&mut nodes[1]);
    assert!(
        matches!(
            out.as_slice(),
            [(
                Audience::Proxy(ProxyId(0)),
                Message::Accept {
                    reply_to: Party::Proxy(ProxyId(0)),
                    leader: NodeId(1),
                    ..
                }
            )]
        ),
        "the re-delegation names the successor as the leader hint"
    );
    if let [(_, Message::Accept { ballot: b, .. })] = out.as_slice() {
        assert_eq!(*b, ballot, "under the same ballot, no second Phase 1");
    }
}

/// An election's recovery and its gap fills are never proxied: the
/// successor holds the delegated round's record from the proxy's fan-out,
/// its Phase 1 reports it, and the P2c re-proposal runs colocated.
#[test]
fn a_fresh_leaderships_recovery_is_never_delegated() {
    let mut nodes = proxied_cluster(1);
    let ballot = nodes[0].ballot();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(1), val(1));
    let delegation = raw(&mut nodes[0]);
    let mut proxy = ProxyLeader::new(ProxyId(0), nodes[0].acceptors().clone());
    for (_, m) in delegation {
        proxy.step(m);
    }
    // The fan-out reaches the acceptors; their `Accepted`s never reach the
    // proxy, so nothing is decided and no `Commit` leaves.
    let fanned: Vec<(NodeId, Message)> = proxy
        .ready()
        .messages()
        .iter()
        .flat_map(|(a, m)| {
            a.resolve_from_proxy(&[NodeId(0), NodeId(1), NodeId(2)])
                .into_iter()
                .map(move |to| (to, m.clone()))
        })
        .collect();
    for (to, m) in fanned {
        nodes[usize::try_from(to.0).expect("index")].step(m);
        let _ = raw(&mut nodes[usize::try_from(to.0).expect("index")]);
    }
    assert_eq!(
        nodes[1].acceptor().record(Slot(0)),
        Some(&(ballot, ucmd(1, 1, 1)))
    );
    nodes[0].step_down();
    let _ = raw(&mut nodes[0]);
    // Node 1 campaigns: its own record seeds the recovery.
    nodes[1].set_election_timeout(1);
    nodes[1].tick();
    let q = drain(&mut nodes[1]);
    deliver_all(&mut nodes, q);
    assert!(nodes[1].is_leader());
    nodes[1].set_election_timeout(NO_CHECK_QUORUM);
    assert!(
        nodes[1].delegated_rounds().is_empty(),
        "the P2c re-proposal of slot 0 ran colocated"
    );
    nodes[1].advance_recovery();
    let q = drain(&mut nodes[1]);
    deliver_all(&mut nodes, q);
    for n in &nodes {
        assert_eq!(n.replica().chosen_at(Slot(0)), Some(&ucmd(1, 1, 1)));
    }
    assert!(nodes[1].ballot() > ballot, "a fresh ballot, not a handoff");
}

/// A `Nack` a proxy relays deposes the leader by the ordinary rule.
#[test]
fn a_relayed_nack_deposes_the_delegating_leader() {
    let mut nodes = proxied_cluster(1);
    let ballot = nodes[0].ballot();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(1), val(1));
    let _ = raw(&mut nodes[0]);
    let mut proxy = ProxyLeader::new(ProxyId(0), nodes[0].acceptors().clone());
    proxy.step(Message::Accept {
        reply_to: Party::Proxy(ProxyId(0)),
        leader: NodeId(0),
        ballot,
        slot: Slot(0),
        command: ucmd(1, 1, 1),
        config: None,
    });
    let _ = proxy.ready().messages().to_vec();
    proxy.ready().advance();
    proxy.step(Message::Nack {
        from: NodeId(2),
        ballot,
        slot: Slot(0),
    });
    let relayed = proxy.ready().messages().to_vec();
    assert_eq!(relayed.len(), 1);
    let (audience, nack) = relayed.into_iter().next().expect("one relay");
    assert_eq!(audience, Audience::Node(NodeId(0)));
    nodes[0].step(nack);
    assert_eq!(nodes[0].role(), NodeRole::Follower);
}
