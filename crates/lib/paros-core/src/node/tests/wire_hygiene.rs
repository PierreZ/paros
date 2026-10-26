//! Wire hygiene (#344): input that no honest peer sends.
//!
//! The simulation runs honest peers only, and its fault model has no bit
//! flips, so a check that refuses a malformed message is never reached
//! there: the mutation hunt (#269) cannot kill an edit that makes such a
//! check more permissive. Each test below feeds one malformed shape to the
//! check that must refuse it, next to the well-formed twin it must take, so
//! the test is not vacuous. `.cargo/mutants.toml` excludes the edits these
//! tests pin, naming this file.

use std::collections::BTreeMap;

use super::{
    Ballot, MatchOutcome, MatchReply, MatchStep, MatchmakerGeneration, MatchmakerId, MemStorage,
    Message, NodeId, Party, Registration, Slot, ballot, campaign, cfg, deployed_node, drain,
    drain_match_requests, ucmd,
};
use crate::contract::limits::Limits;
use crate::contract::limits::REGISTRY_PAGE;
use crate::matchmaker::RegistrationKind;
use crate::matchmaking::{MatchFold, Matchmaking, RegisteredPage};
use crate::membership::ProxyId;
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

/// An `Accept` whose reply address names a proxy the deployment does not
/// have is refused whole: no promise, no vote. The deployed proxy's twin is
/// answered.
#[test]
fn an_accept_replying_to_an_undeployed_proxy_is_ignored() {
    let mut n = proxied_follower();
    n.step(accept(Party::Proxy(ProxyId(1))));
    assert!(
        !accepted(&drain(&mut n)),
        "an undeployed proxy gets no vote"
    );
    assert_eq!(n.acceptor.promised(), Ballot::zero());
    n.step(accept(Party::Proxy(ProxyId(u64::MAX))));
    assert!(!accepted(&drain(&mut n)));
    assert_eq!(n.acceptor.promised(), Ballot::zero());

    n.step(accept(Party::Proxy(ProxyId(0))));
    let _ = drain(&mut n);
    assert_eq!(n.acceptor.promised(), ballot(1, 0), "the deployed proxy is");
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
