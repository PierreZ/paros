//! Wire hygiene (#344): input that no honest peer sends.
//!
//! The simulation runs honest peers only, and its fault model has no bit
//! flips, so a check that refuses a malformed message is never reached
//! there. These tests are what catches a mutant that makes such a check
//! more permissive (`.cargo/mutants.toml`). Each test feeds one malformed
//! shape to the check that must refuse it, next to the well-formed twin it
//! must take, so the test is not vacuous.

use std::collections::BTreeMap;

use super::{
    Ballot, MatchOutcome, MatchReply, MatchStep, MatchmakerGeneration, MatchmakerId, MemStorage,
    Message, NodeId, Party, Registration, Slot, ballot, campaign, cfg, deployed_node, drain,
    drain_match_requests, ucmd,
};
use crate::contract::limits::Limits;
use crate::contract::limits::REGISTRY_PAGE;
use crate::contract::message::Audience;
use crate::journal::Command;
use crate::matchmaker::RegistrationKind;
use crate::matchmaking::{MatchFold, Matchmaking, RegisteredPage};
use crate::membership::{MAX_ROLE_COUNT, ProxyId};
use crate::node::ColocatedNode;

// ---------------------------------------------------------------------------
// `Matchmaking::accepts`: the shape and cursor of a registry page.
// ---------------------------------------------------------------------------

/// A campaign at round 1000, so every page below sits under its ballot.
fn phase() -> Matchmaking {
    Matchmaking::new(ballot(1000, 0), cfg(&[0, 1, 2]), RegistrationKind::Belief)
}

/// Beliefs naming `[0, 1, 2]` at each of `rounds`.
fn beliefs(rounds: impl IntoIterator<Item = u64>) -> BTreeMap<Ballot, Registration> {
    rounds
        .into_iter()
        .map(|round| (ballot(round, 1), Registration::belief(cfg(&[0, 1, 2]))))
        .collect()
}

/// A page from `from` holding `history`, continued at `next`.
fn page(
    from: Ballot,
    history: BTreeMap<Ballot, Registration>,
    next: Option<Ballot>,
) -> RegisteredPage {
    RegisteredPage {
        from_ballot: from,
        history,
        next_from_ballot: next,
        gc_watermark: Ballot::zero(),
        effective: None,
    }
}

const MM: MatchmakerId = MatchmakerId(0);

/// A page longer than `REGISTRY_PAGE` is refused; a full page is taken.
#[test]
fn a_page_longer_than_the_ceiling_is_ignored() {
    let ceiling = u64::try_from(REGISTRY_PAGE).expect("a page size fits a round");
    let mut m = phase();
    assert_eq!(
        m.fold(MM, page(Ballot::zero(), beliefs(1..=ceiling + 1), None)),
        MatchFold::Ignored
    );
    assert_eq!(
        m.fold(MM, page(Ballot::zero(), beliefs(1..=ceiling), None)),
        MatchFold::Registered
    );
}

/// A registration below the page's own start is refused; the same entry at
/// the start is taken.
#[test]
fn a_page_entry_below_its_start_is_ignored() {
    let mut m = phase();
    assert_eq!(
        m.fold(MM, page(ballot(5, 1), beliefs([4, 6]), None)),
        MatchFold::Ignored
    );
    assert_eq!(
        m.fold(MM, page(ballot(5, 1), beliefs([5, 6]), None)),
        MatchFold::Registered
    );
}

/// A page with a cursor but no registration is refused: a matchmaker's page
/// size is at least one (#338).
#[test]
fn a_continued_page_without_a_registration_is_ignored() {
    let mut m = phase();
    assert_eq!(
        m.fold(
            MM,
            page(Ballot::zero(), BTreeMap::new(), Some(ballot(9, 1)))
        ),
        MatchFold::Ignored
    );
    // Not even a cursor at or below the start rescues it.
    assert_eq!(
        m.fold(MM, page(ballot(9, 1), BTreeMap::new(), Some(ballot(9, 1)))),
        MatchFold::Ignored
    );
    assert_eq!(
        m.fold(MM, page(Ballot::zero(), beliefs([3]), Some(ballot(9, 1)))),
        MatchFold::Paged(ballot(9, 1))
    );
}

/// A cursor at the page's last registration would ask for that registration
/// twice: refused. A cursor above it is taken.
#[test]
fn a_cursor_at_the_last_registration_is_ignored() {
    let mut m = phase();
    assert_eq!(
        m.fold(
            MM,
            page(Ballot::zero(), beliefs([3, 4]), Some(ballot(4, 1)))
        ),
        MatchFold::Ignored
    );
    assert_eq!(
        m.fold(
            MM,
            page(Ballot::zero(), beliefs([3, 4]), Some(ballot(4, 2)))
        ),
        MatchFold::Paged(ballot(4, 2))
    );
}

/// After a first page, the next one must start at the cursor it named, or
/// above it exactly at the sender's raised watermark (a GC raise collected
/// the cursor). Any other start is refused.
#[test]
fn a_later_page_starts_at_its_cursor_or_at_a_raised_watermark() {
    let cursor = ballot(9, 1);
    let first = || page(Ballot::zero(), beliefs([3]), Some(cursor));
    // Below the cursor: refused.
    let mut m = phase();
    assert_eq!(m.fold(MM, first()), MatchFold::Paged(cursor));
    assert_eq!(
        m.fold(MM, page(ballot(8, 1), beliefs([8]), None)),
        MatchFold::Ignored
    );
    // Above the cursor, but not at the watermark the page reports: refused.
    let raised = ballot(20, 1);
    let mut above = page(raised, beliefs([21]), None);
    above.gc_watermark = ballot(19, 1);
    assert_eq!(m.fold(MM, above.clone()), MatchFold::Ignored);
    // Above the cursor at the reported watermark: taken.
    above.gc_watermark = raised;
    assert_eq!(m.fold(MM, above), MatchFold::Registered);
    // At the cursor: taken, whatever the watermark.
    let mut m = phase();
    assert_eq!(m.fold(MM, first()), MatchFold::Paged(cursor));
    let mut at = page(cursor, beliefs([10]), None);
    at.gc_watermark = ballot(2, 1);
    assert_eq!(m.fold(MM, at), MatchFold::Registered);
}

/// A matchmaker whose complete answer is counted has nothing more to say:
/// a second page from it, even a well-formed one, is refused.
#[test]
fn a_page_from_a_registered_matchmaker_is_ignored() {
    let mut m = phase();
    assert_eq!(
        m.fold(MM, page(Ballot::zero(), beliefs([3]), None)),
        MatchFold::Registered
    );
    assert_eq!(
        m.fold(MM, page(Ballot::zero(), beliefs([4]), None)),
        MatchFold::Ignored
    );
    assert_eq!(m.registered(), 1);
    assert!(!m.history().contains_key(&ballot(4, 1)));
}

// ---------------------------------------------------------------------------
// `ProxyId::is_in` and `rank_is_in`: an `Accept`'s reply address.
// ---------------------------------------------------------------------------

/// A follower of a three-node deployment with one proxy leader.
fn proxied_follower() -> ColocatedNode {
    let mut storage = MemStorage::for_node(1, &[0, 1, 2]);
    storage.config.proxy_count = 1;
    ColocatedNode::new(&storage, Limits::DEFAULT)
}

fn accept(reply_to: Party) -> Message {
    Message::Accept {
        reply_to,
        leader: NodeId(0),
        ballot: ballot(1, 0),
        slot: Slot(0),
        command: ucmd(1, 1, 1),
        config: None,
    }
}

fn accepted(msgs: &[(NodeId, Message)]) -> bool {
    msgs.iter()
        .any(|(_, m)| matches!(m, Message::Accepted { .. }))
}

/// An `Accept` whose reply address names a proxy rank past the cap is
/// refused whole: no promise, no vote. A rank under the cap is answered,
/// past the count this node knows too: the count is the configuration's,
/// bound to a ballot this acceptor may not know yet (#754 (counts off the
/// format marker)).
#[test]
fn an_accept_replying_to_a_proxy_past_the_cap_is_ignored() {
    let mut n = proxied_follower();
    n.step(accept(Party::Proxy(ProxyId(MAX_ROLE_COUNT as u64))));
    assert!(
        !accepted(&drain(&mut n)),
        "a proxy past the cap gets no vote"
    );
    assert_eq!(n.acceptor.promised(), Ballot::zero());
    n.step(accept(Party::Proxy(ProxyId(u64::MAX))));
    assert!(!accepted(&drain(&mut n)));
    assert_eq!(n.acceptor.promised(), Ballot::zero());

    n.step(accept(Party::Proxy(ProxyId(1))));
    let _ = drain(&mut n);
    assert_eq!(
        n.acceptor.promised(),
        ballot(1, 0),
        "a proxy past the count but under the cap is"
    );
}

/// The rank arithmetic itself, at its boundaries: a rank names one of
/// `count` peers only below `count`, and nothing in an empty tier.
#[test]
fn a_proxy_rank_is_in_only_below_the_count() {
    assert!(ProxyId(0).is_in(1));
    assert!(!ProxyId(1).is_in(1));
    assert!(ProxyId(2).is_in(3));
    assert!(!ProxyId(3).is_in(3));
    assert!(!ProxyId(0).is_in(0));
    assert!(!ProxyId(u64::MAX).is_in(usize::MAX));
}

// ---------------------------------------------------------------------------
// `MatchmakerSet::contains`: a reply from a stranger matchmaker.
// ---------------------------------------------------------------------------

/// A matchmaking reply from a matchmaker outside the deployment's set is
/// refused, however well-formed; the same reply from a member is folded.
#[test]
fn a_match_reply_from_a_stranger_matchmaker_is_ignored() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    let b = n.ballot();
    let _ = drain_match_requests(&mut n);
    let reply = |mm| MatchReply {
        matchmaker: MatchmakerId(mm),
        to: NodeId(0),
        journal: crate::journal::JournalId::UNSET,
        ballot: b,
        generation: MatchmakerGeneration(0),
        outcome: MatchOutcome::Registered {
            from_ballot: Ballot::zero(),
            history: BTreeMap::new(),
            next_from_ballot: None,
            gc_watermark: Ballot::zero(),
            effective: None,
        },
    };
    assert!(!n.matchmaker_set().expect("a set").contains(MatchmakerId(3)));
    assert_eq!(n.on_match_reply(reply(3)), MatchStep::Ignored);
    assert_eq!(n.on_match_reply(reply(7)), MatchStep::Ignored);
    assert_eq!(
        n.on_match_reply(reply(0)),
        MatchStep::Registered { remaining: 1 }
    );
}

// ---------------------------------------------------------------------------
// The sender of a learn, of history and of a trim point: a pooled node, or a
// proxy of the deployment for a `Commit`.
// ---------------------------------------------------------------------------

/// A plain follower of `{0, 1, 2}` with nothing learned.
fn follower() -> ColocatedNode {
    let storage = MemStorage::for_node(1, &[0, 1, 2]);
    ColocatedNode::new(&storage, Limits::DEFAULT)
}

fn commit(from: Party, round: u64) -> Message {
    Message::Commit {
        from,
        ballot: ballot(round, 0),
        slot: Slot(0),
        command: ucmd(1, 1, 1),
    }
}

/// A `Commit` from outside the pool, or from a proxy rank past the cap,
/// teaches nothing: a learn raises the durable promise to the wire ballot,
/// and one misrouted `Commit` at the top ballot would end every later
/// campaign. The same `Commit` from a pooled node, or a proxy, is learned.
#[test]
fn a_commit_from_a_stranger_is_ignored() {
    let mut n = follower();
    for stranger in [
        Party::Node(NodeId(9)),
        Party::Proxy(ProxyId(MAX_ROLE_COUNT as u64)),
    ] {
        n.step(commit(stranger, u64::MAX));
        assert_eq!(n.acceptor.promised(), Ballot::zero(), "{stranger:?}");
        assert!(n.replica.chosen().is_empty(), "{stranger:?}");
    }
    n.step(commit(Party::Proxy(ProxyId(0)), 3));
    assert_eq!(n.acceptor.promised(), ballot(3, 0));
    n.step(commit(Party::Node(NodeId(0)), 3));
    assert_eq!(n.replica.chosen().len(), 1);
}

/// A `CatchUpResponse` from outside the pool teaches nothing; the same
/// page from a pooled peer is learned.
#[test]
fn a_catch_up_page_from_a_stranger_is_ignored() {
    let mut n = follower();
    let entries: BTreeMap<Slot, (Ballot, crate::journal::Command)> =
        [(Slot(0), (ballot(7, 0), ucmd(1, 1, 1)))]
            .into_iter()
            .collect();
    n.step(Message::CatchUpResponse {
        from: NodeId(9),
        entries: entries.clone(),
    });
    assert_eq!(n.acceptor.promised(), Ballot::zero());
    assert!(n.replica.chosen().is_empty());
    n.step(Message::CatchUpResponse {
        from: NodeId(2),
        entries,
    });
    assert_eq!(n.acceptor.promised(), ballot(7, 0));
    assert_eq!(n.replica.chosen().len(), 1);
}

/// A trim point from outside the pool moves no floor, even as the answer
/// to a request; the same point from a pooled peer does.
#[test]
fn a_trim_point_from_a_stranger_is_ignored() {
    let mut n = follower();
    let trimmed = |from| Message::TrimmedTo {
        from,
        point: Slot(4),
        state: crate::journal::state::JournalState::default(),
    };
    let _ = n.catch_up_request(Slot(0));
    n.step(trimmed(NodeId(9)));
    assert_eq!(n.acceptor.first_slot(), Slot(0));
    n.step(trimmed(NodeId(0)));
    assert_eq!(n.acceptor.first_slot(), Slot(4));
}

/// A pooled peer's trim point that answers no request of this node moves
/// no floor: the node asks that peer from its first unchosen slot, and
/// takes the point when it answers the request (#1100 (contract and
/// assertions)).
#[test]
fn an_unasked_trim_point_makes_the_node_ask() {
    let mut n = follower();
    let trimmed = Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(4),
        state: crate::journal::state::JournalState::default(),
    };
    n.step(trimmed.clone());
    assert_eq!(
        n.acceptor.first_slot(),
        Slot(0),
        "an unasked point is not taken"
    );
    let asked: Vec<_> = n
        .ready()
        .messages()
        .iter()
        .filter(|(to, m)| {
            *to == Audience::Node(NodeId(0))
                && matches!(m, Message::CatchUpRequest { from_slot, .. } if *from_slot == Slot(0))
        })
        .cloned()
        .collect();
    assert_eq!(asked.len(), 1, "the node asks the peer that sent the point");
    let _ = drain(&mut n);
    n.step(trimmed);
    assert_eq!(n.acceptor.first_slot(), Slot(4), "the answer is taken");
    // The jump answered every open request: the same point again is
    // stale, and a higher one unasked.
    n.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(8),
        state: crate::journal::state::JournalState::default(),
    });
    assert_eq!(
        n.acceptor.first_slot(),
        Slot(4),
        "a jump closes the request"
    );
}

/// A request is open only while the node misses the slot it asked for: once
/// the node learns that range by entries, the request is answered, and a
/// later trim point is unasked again. The node asks from the first slot it
/// now misses, and takes no jump.
#[test]
fn a_request_answered_by_entries_closes_the_trim_point_gate() {
    let mut n = follower();
    let _ = n.catch_up_request(Slot(0));
    let _ = drain(&mut n);
    let entries: BTreeMap<Slot, (crate::types::Ballot, Command)> = (0..3)
        .map(|s| {
            (
                Slot(s),
                (
                    ballot(7, 0),
                    Command::Control(crate::journal::Control::Noop),
                ),
            )
        })
        .collect();
    n.step(Message::CatchUpResponse {
        from: NodeId(2),
        entries,
    });
    assert_eq!(n.catch_up_need(), Slot(3), "the node holds slots 0 to 2");
    let _ = drain(&mut n);
    n.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(8),
        state: crate::journal::state::JournalState::default(),
    });
    assert_eq!(
        n.acceptor.first_slot(),
        Slot(0),
        "an answered request does not open the gate"
    );
    let asked = drain(&mut n).into_iter().any(|(to, m)| {
        to == NodeId(0)
            && matches!(m, Message::CatchUpRequest { from_slot, .. } if from_slot == Slot(3))
    });
    assert!(asked, "the node asks from the first slot it misses");
}

/// A catch-up page longer than the bound a correct server cuts it at is
/// ignored whole; a page at the bound is learned.
#[test]
fn a_catch_up_page_over_the_bound_is_ignored() {
    use crate::contract::limits::CATCHUP_BATCH;
    let page = |len: usize| -> BTreeMap<Slot, (crate::types::Ballot, Command)> {
        (0..len as u64)
            .map(|s| {
                (
                    Slot(s),
                    (
                        ballot(7, 0),
                        Command::Control(crate::journal::Control::Noop),
                    ),
                )
            })
            .collect()
    };
    let mut n = follower();
    n.step(Message::CatchUpResponse {
        from: NodeId(2),
        entries: page(CATCHUP_BATCH + 1),
    });
    assert!(
        n.replica.chosen().is_empty(),
        "an over-long page teaches nothing"
    );
    n.step(Message::CatchUpResponse {
        from: NodeId(2),
        entries: page(CATCHUP_BATCH),
    });
    assert_eq!(n.replica.chosen().len(), CATCHUP_BATCH);
}

/// A catch-up request from outside the pool is not answered; the same
/// request from a pooled peer is.
#[test]
fn a_catch_up_request_from_a_stranger_is_not_answered() {
    let mut n = follower();
    n.step(commit(Party::Node(NodeId(0)), 1));
    let _ = drain(&mut n);
    n.step(Message::CatchUpRequest {
        from: NodeId(9),
        from_slot: Slot(0),
    });
    assert!(drain(&mut n).is_empty(), "a stranger is served nothing");
    n.step(Message::CatchUpRequest {
        from: NodeId(2),
        from_slot: Slot(0),
    });
    assert!(
        drain(&mut n)
            .iter()
            .any(|(to, m)| *to == NodeId(2) && matches!(m, Message::CatchUpResponse { .. })),
        "a pooled peer is served"
    );
}

/// A message in the node's own name is a transport echo, and `step` drops
/// it for every variant: a leader's own beat does not depose it, and its
/// own `Promise` does not reach the campaign (which asserts it never pages
/// itself).
#[test]
fn a_message_in_the_nodes_own_name_is_dropped() {
    let mut nodes = super::cluster::<3>();
    super::make_leader(&mut nodes, 0);
    let ballot = nodes[0].ballot();
    let echoes = [
        Message::Heartbeat {
            from: NodeId(0),
            ballot: crate::types::Ballot {
                round: ballot.round + 1,
                node: NodeId(0),
            },
            commit: None,
            config: None,
            fence: None,
            gc_floor: None,
        },
        Message::Promise {
            from: NodeId(0),
            ballot,
            from_slot: Slot(0),
            accepted: BTreeMap::new(),
            faulty: BTreeMap::new(),
            next_from_slot: Some(Slot(1)),
            prepared: None,
        },
        Message::TrimmedTo {
            from: NodeId(0),
            point: Slot(9),
            state: crate::journal::state::JournalState::default(),
        },
    ];
    for echo in echoes {
        let hard = nodes[0].hard_state();
        nodes[0].step(echo);
        assert!(nodes[0].is_leader(), "an echo deposes nobody");
        assert_eq!(nodes[0].ballot(), ballot);
        assert_eq!(nodes[0].hard_state(), hard, "an echo moves nothing durable");
        assert!(drain(&mut nodes[0]).is_empty(), "an echo is not answered");
    }
    assert_eq!(nodes[0].acceptor.first_slot(), Slot(0));
}

/// `Message::sender` names the party each variant comes from: the reply
/// address of a request, the `from` of a reply.
#[test]
fn the_sender_of_a_request_is_its_reply_address() {
    let prepare = Message::Prepare {
        reply_to: NodeId(2),
        ballot: ballot(1, 2),
        from_slot: Slot(0),
        config: None,
    };
    assert_eq!(prepare.sender(), Party::Node(NodeId(2)));
    assert_eq!(
        accept(Party::Proxy(ProxyId(1))).sender(),
        Party::Proxy(ProxyId(1))
    );
    assert_eq!(
        commit(Party::Proxy(ProxyId(0)), 1).sender(),
        Party::Proxy(ProxyId(0))
    );
    let ack = Message::HeartbeatAck {
        from: NodeId(4),
        ballot: ballot(1, 0),
        chosen: None,
    };
    assert_eq!(ack.sender(), Party::Node(NodeId(4)));
}
