use super::*;
use crate::membership::QuorumSystem;
use crate::types::NodeId;

/// The watermark is the oracle that a raised floor was reported by a
/// matchmaker: a raised watermark with no page folded is a state that no
/// fold reaches, and the check must name it.
#[test]
#[should_panic(expected = "a raised watermark was reported")]
fn a_raised_watermark_that_no_matchmaker_reported_breaks_the_check() {
    let config = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(2)],
        QuorumSystem::Majority,
    );
    let ballot = Ballot {
        round: 5,
        node: NodeId(0),
    };
    let mut m = Matchmaking::new(ballot, config, RegistrationKind::Belief);
    m.watermark = Ballot {
        round: 1,
        node: NodeId(1),
    };
    let _ = m.watermark();
}

fn majority(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        QuorumSystem::Majority,
    )
}

fn b(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(0),
    }
}

fn belief(config: &AcceptorConfig) -> Registration {
    Registration {
        config: config.clone(),
        kind: RegistrationKind::Belief,
    }
}

/// A complete page from `from` with the given history and watermark.
fn page(from: Ballot, history: &[(Ballot, Registration)], watermark: Ballot) -> RegisteredPage {
    RegisteredPage {
        from_ballot: from,
        history: history.iter().cloned().collect(),
        next_from_ballot: None,
        gc_watermark: watermark,
        effective: None,
    }
}

fn phase() -> Matchmaking {
    Matchmaking::new(b(10), majority(&[0, 1, 2]), RegistrationKind::Belief)
}

/// A whole reply decodes to its sender and its page.
#[test]
fn a_reply_decodes_to_its_sender_and_page() {
    let reply = MatchReply {
        matchmaker: MatchmakerId(4),
        to: NodeId(0),
        ballot: b(10),
        generation: crate::membership::MatchmakerGeneration(0),
        outcome: MatchOutcome::Registered {
            from_ballot: b(1),
            history: BTreeMap::new(),
            next_from_ballot: None,
            gc_watermark: b(1),
            effective: None,
        },
    };
    let (from, decoded) = RegisteredPage::from_reply(reply);
    assert_eq!(from, MatchmakerId(4));
    assert_eq!(decoded, Some(Ok(page(b(1), &[], b(1)))));
}

/// A registered matchmaker that still owes a page is a state no fold
/// reaches: the check must name it.
#[test]
#[should_panic(expected = "a registered matchmaker owes no further page")]
fn a_registered_matchmaker_that_owes_a_page_breaks_the_check() {
    let mut m = phase();
    m.registered_by.insert(MatchmakerId(0));
    m.page_next.insert(MatchmakerId(0), b(1));
    m.assert_invariants();
}

/// A fresh phase reports the zero watermark and no disagreement, without
/// tripping its own checks.
#[test]
fn a_fresh_phase_reports_nothing() {
    let m = phase();
    assert_eq!(m.watermark(), Ballot::zero());
    assert_eq!(m.disagreements(), 0);
}

/// A disagreement with an empty union is a state no fold reaches.
#[test]
#[should_panic(expected = "a disagreement lies in the union")]
fn a_disagreement_outside_the_union_breaks_the_check() {
    let mut m = phase();
    m.disagreements = 1;
    let _ = m.disagreements();
}

/// Two matchmakers that name two configurations at one ballot are one
/// disagreement; both configurations stay in the union.
#[test]
fn two_configurations_at_one_ballot_are_one_disagreement() {
    let mut m = phase();
    let x = majority(&[0, 1, 2]);
    let y = majority(&[1, 2, 3]);
    assert_eq!(
        m.fold(MatchmakerId(0), page(b(1), &[(b(5), belief(&x))], b(1))),
        MatchFold::Registered
    );
    assert_eq!(
        m.fold(MatchmakerId(1), page(b(1), &[(b(5), belief(&y))], b(1))),
        MatchFold::Registered
    );
    assert_eq!(m.disagreements(), 1);
    assert_eq!(m.history()[&b(5)].len(), 2);
}

/// A later page must start at the cursor owed, or above it at the sender's
/// own watermark; above the cursor at any other ballot it is refused.
#[test]
fn a_later_page_above_its_cursor_counts_only_at_the_watermark() {
    let x = majority(&[0, 1, 2]);
    let first = RegisteredPage {
        next_from_ballot: Some(b(3)),
        ..page(b(1), &[(b(2), belief(&x))], b(1))
    };
    let mut m = phase();
    assert_eq!(
        m.fold(MatchmakerId(0), first.clone()),
        MatchFold::Paged(b(3))
    );
    assert_eq!(
        m.fold(MatchmakerId(0), page(b(4), &[], b(1))),
        MatchFold::Ignored,
        "above the cursor, not at the watermark"
    );
    assert_eq!(
        m.fold(MatchmakerId(0), page(b(2), &[], b(1))),
        MatchFold::Ignored,
        "below the cursor"
    );
    assert_eq!(
        m.fold(MatchmakerId(0), page(b(4), &[], b(4))),
        MatchFold::Registered,
        "a GC raise collected the cursor"
    );
    let mut m = phase();
    assert_eq!(m.fold(MatchmakerId(0), first), MatchFold::Paged(b(3)));
    assert_eq!(
        m.fold(MatchmakerId(0), page(b(3), &[], b(1))),
        MatchFold::Registered,
        "at the cursor"
    );
}

/// A cut page's cursor lies strictly above its last registration: a cursor
/// at the last one, or a cut page with no registration, is refused.
#[test]
fn a_cut_page_names_a_cursor_above_its_last_registration() {
    let x = majority(&[0, 1, 2]);
    let at_last = RegisteredPage {
        next_from_ballot: Some(b(2)),
        ..page(b(1), &[(b(2), belief(&x))], b(1))
    };
    let empty = RegisteredPage {
        next_from_ballot: Some(b(2)),
        ..page(b(1), &[], b(1))
    };
    let mut m = phase();
    assert_eq!(m.fold(MatchmakerId(0), at_last), MatchFold::Ignored);
    assert_eq!(m.fold(MatchmakerId(0), empty), MatchFold::Ignored);
}

/// A configuration named at the campaign's own ballot is covered by Phase 1
/// whatever its owner testifies: it is not abandonable.
#[test]
fn a_configuration_named_at_the_ballot_is_not_abandonable() {
    let y = majority(&[1, 2, 3]);
    let z = majority(&[2, 3, 4]);
    let mut m = phase();
    assert_eq!(
        m.fold(
            MatchmakerId(0),
            page(b(1), &[(b(5), belief(&z)), (b(10), belief(&y))], b(1))
        ),
        MatchFold::Registered
    );
    assert_eq!(m.abandonable(), vec![(z, vec![b(5)])]);
}

/// `MembershipProbe::fold` at the role (#907 (core unit tests)): each
/// matchmaker counts once, the effective configuration is the one at the
/// highest ballot heard and never moves back, and a matchmaker quorum of
/// answers holds the probe.
#[test]
fn a_probe_folds_each_matchmaker_once_and_keeps_the_highest_fact() {
    use crate::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
    let set = MatchmakerSet::new(
        MatchmakerGeneration(0),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    );
    let mut probe = MembershipProbe::new(b(4), majority(&[0, 1, 2]));
    assert_eq!(probe.ballot(), b(4));
    assert!(probe.effective().is_none());
    assert!(probe.fold(MatchmakerId(0), Some((b(3), majority(&[1, 2, 3])))));
    assert!(
        !probe.fold(MatchmakerId(0), Some((b(9), majority(&[3, 4, 5])))),
        "a second answer of one matchmaker is ignored whole"
    );
    assert_eq!(probe.effective(), Some(&(b(3), majority(&[1, 2, 3]))));
    assert!(!probe.quorum_held(&set), "one answer of three is no quorum");
    assert!(probe.fold(MatchmakerId(1), Some((b(2), majority(&[0, 1, 2])))));
    assert_eq!(
        probe.effective(),
        Some(&(b(3), majority(&[1, 2, 3]))),
        "a lower fact never moves the probe back"
    );
    assert!(probe.quorum_held(&set));
    assert_eq!(probe.answered_by(), 2);
    assert_eq!(probe.unanswered(&set), vec![MatchmakerId(2)]);
    assert!(probe.fold(MatchmakerId(2), None), "an empty answer counts");
    assert_eq!(probe.answered_by(), 3);
}
