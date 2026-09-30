//! The runtime pool (#189), pinned at the mechanism: `extend_pool` is
//! grow-only and refused on plain Multi-Paxos, a reconfiguration may name a
//! node only once the pool admits it, and a candidate whose matchmakers name
//! a node its pool has not admitted abandons the campaign instead of acting
//! on it — then completes once the pool catches up.

use super::*;

/// Plain Multi-Paxos: the pool is the membership, and it never moves.
#[test]
fn a_plain_pool_never_moves() {
    let mut n = node(0, &[0, 1, 2]);
    assert!(!n.extend_pool(&[NodeId(7)]));
    assert_eq!(n.pool(), &[NodeId(0), NodeId(1), NodeId(2)]);
}

/// A matchmaker deployment's pool grows, idempotently, and a reconfiguration
/// naming a node is refused until the pool admits it.
#[test]
fn a_reconfiguration_names_a_node_only_once_the_pool_admits_it() {
    let pool = [0, 1, 2, 3, 4];
    let mut nodes = [
        deployed_node(0, &[0, 1, 2], &pool, 1),
        deployed_node(1, &[0, 1, 2], &pool, 1),
        deployed_node(2, &[0, 1, 2], &pool, 1),
        deployed_node(3, &[0, 1, 2], &pool, 1),
        deployed_node(4, &[0, 1, 2], &pool, 1),
    ];
    let mut mms = registries(1);
    campaign(&mut nodes[0]);
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    nodes[0].set_election_timeout(NO_CHECK_QUORUM);
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    let joined = cfg(&[0, 1, 2, 7]);
    assert_eq!(
        nodes[0].reconfigure(&joined),
        ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember)
    );
    assert!(nodes[0].extend_pool(&[NodeId(7)]));
    assert!(
        !nodes[0].extend_pool(&[NodeId(7)]),
        "admitting twice is a no-op"
    );
    assert!(nodes[0].pool().contains(&NodeId(7)));
    assert!(matches!(
        nodes[0].reconfigure(&joined),
        ReconfigureResult::Started(_)
    ));
    // The reconfiguration registers `C_new` with the registry.
    let requests = drain_match_requests(&mut nodes[0]);
    let _ = matchmake(&mut mms, requests);

    // Node 1 never heard of node 7: every campaign whose matchmakers name
    // `C_new` is abandoned, never acted on.
    let mut unknown = false;
    for _ in 0..4 {
        campaign(&mut nodes[1]);
        let requests = drain_match_requests(&mut nodes[1]);
        for reply in matchmake(&mut mms, requests) {
            let step = nodes[1].on_match_reply(reply);
            assert!(
                !matches!(
                    step,
                    MatchStep::Completed { .. } | MatchStep::StaleConfiguration { .. }
                ),
                "a campaign never acts on a configuration naming an unknown node: {step:?}"
            );
            unknown |= step == MatchStep::UnknownMember;
        }
        let _ = drain(&mut nodes[1]);
        if unknown {
            break;
        }
    }
    assert!(unknown, "the unknown member abandons the campaign");
    assert!(!nodes[1].matchmaking_pending());

    // Once the pool admits node 7, the next campaign adopts `C_new`.
    assert!(nodes[1].extend_pool(&[NodeId(7)]));
    let mut adopted = false;
    for _ in 0..4 {
        campaign(&mut nodes[1]);
        let requests = drain_match_requests(&mut nodes[1]);
        for reply in matchmake(&mut mms, requests) {
            let step = nodes[1].on_match_reply(reply);
            assert_ne!(step, MatchStep::UnknownMember);
            adopted |= matches!(step, MatchStep::StaleConfiguration { .. });
        }
        let _ = drain(&mut nodes[1]);
        if adopted {
            break;
        }
    }
    assert!(
        adopted,
        "the caught-up candidate adopts the effective configuration"
    );
    assert_eq!(nodes[1].acceptors(), &joined);
}
