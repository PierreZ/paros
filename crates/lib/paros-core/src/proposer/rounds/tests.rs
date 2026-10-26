use super::*;
use crate::membership::QuorumSystem;
use crate::types::{Command, Entry, LeaderUuid, NodeId, Seq, Value, command_fingerprint};

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn cmd(seq: u64) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(seq),
        records: vec![Value(seq.to_le_bytes().to_vec())],
    })
}

/// One command per (slot, ballot) (P2b, #317): a vote at the round's own
/// ballot for another value is a broken invariant, never ignored.
#[test]
#[should_panic(expected = "an Accepted at an open round's ballot names the round's command")]
fn a_same_ballot_vote_for_another_value_panics() {
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    rounds.open(Slot(5), ballot(1, 0), cmd(1), Some(NodeId(0)), None);
    let _ = rounds.fold_accepted(NodeId(1), ballot(1, 0), Slot(5), 0);
}

/// The tally stands on its own: opened, folded, decided and closed
/// with no proposer around it — the shape the proxy leader embeds.
#[test]
fn a_standalone_tally_decides_on_a_configuration_quorum() {
    let config = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(2)],
        QuorumSystem::Majority,
    );
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    assert!(rounds.is_empty());
    rounds.open(Slot(5), ballot(1, 0), cmd(1), Some(NodeId(0)), None);
    assert_eq!(rounds.column(Slot(5)), Some(None));
    assert!(rounds.decided(Slot(5), &config).is_none());
    assert!(
        !rounds.fold_accepted(
            NodeId(1),
            ballot(2, 0),
            Slot(5),
            command_fingerprint(&cmd(1))
        ),
        "a vote at another ballot never counts"
    );
    assert!(rounds.fold_accepted(
        NodeId(1),
        ballot(1, 0),
        Slot(5),
        command_fingerprint(&cmd(1))
    ));
    assert_eq!(
        rounds.decided(Slot(5), &config),
        Some((ballot(1, 0), cmd(1)))
    );
    assert!(rounds.is_open_at(Slot(5), ballot(1, 0)));
    assert!(!rounds.is_open_at(Slot(5), ballot(2, 0)));
    assert!(!rounds.is_open_at(Slot(6), ballot(1, 0)));
    rounds.close(Slot(5));
    assert!(rounds.is_empty());
    assert_eq!(rounds.column(Slot(5)), None);
}

/// The re-send cursor is the tally's own: a page walks from the cursor
/// up, wraps around, and `clear` resets it with the rounds.
#[test]
fn the_resend_page_is_fair_and_dies_with_the_tally() {
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    for slot in 0..3 {
        rounds.open(Slot(slot), ballot(1, 0), cmd(slot), None, None);
    }
    let first = rounds.resend_page(RESEND_BATCH);
    assert_eq!(
        first.iter().map(|p| p.slot).collect::<Vec<_>>(),
        vec![Slot(0), Slot(1), Slot(2)]
    );
    assert_eq!(first[0].column, None);
    rounds.retain_from(Slot(1));
    assert_eq!(rounds.by_slot().len(), 2);
    rounds.clear();
    assert!(rounds.is_empty());
    assert!(rounds.resend_page(RESEND_BATCH).is_empty());
}

/// A delegated round is remembered, never counted and never decided
/// here; its re-send is a counted re-delegation, and a take-back turns
/// it into an ordinary colocated round seeded with the opener's own vote.
#[test]
fn a_delegated_round_is_the_proxys_until_taken_back() {
    let config = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(2)],
        QuorumSystem::Majority,
    );
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    rounds.open_delegated(Slot(3), ballot(1, 0), cmd(3), None, ProxyId(1));
    let round = rounds.by_slot().get(&Slot(3)).expect("open");
    assert_eq!(round.proxy(), Some(ProxyId(1)));
    assert!(round.accepted_by().is_none());
    assert!(
        !rounds.fold_accepted(
            NodeId(1),
            ballot(1, 0),
            Slot(3),
            command_fingerprint(&cmd(3))
        ),
        "a delegated round counts no vote here"
    );
    assert!(rounds.decided(Slot(3), &config).is_none());
    assert!(rounds.is_open_at(Slot(3), ballot(1, 0)));
    assert!(rounds.stalled_delegations(1).is_empty());
    assert!(rounds.stalled(1).is_empty());
    let page = rounds.resend_page(RESEND_BATCH);
    assert_eq!(page[0].proxy, Some(ProxyId(1)));
    assert_eq!(rounds.by_slot()[&Slot(3)].resends(), 1);
    assert_eq!(rounds.stalled_delegations(1), vec![Slot(3)]);
    assert!(rounds.stalled_delegations(2).is_empty());
    assert_eq!(rounds.stalled(1), vec![Slot(3)]);
    assert!(rounds.take_back(Slot(3), Some(NodeId(0))));
    assert!(
        !rounds.take_back(Slot(3), None),
        "a colocated round is not taken back"
    );
    assert!(
        rounds.stalled_delegations(0).is_empty(),
        "a taken-back round is no delegation"
    );
    assert_eq!(
        rounds.stalled(1),
        vec![Slot(3)],
        "the re-send count is the round's, whatever its custody"
    );
    assert!(rounds.fold_accepted(
        NodeId(1),
        ballot(1, 0),
        Slot(3),
        command_fingerprint(&cmd(3))
    ));
    assert_eq!(
        rounds.decided(Slot(3), &config),
        Some((ballot(1, 0), cmd(3))),
        "the opener's own vote plus one is the majority"
    );
    assert_eq!(rounds.resend_page(RESEND_BATCH)[0].proxy, None);
}
