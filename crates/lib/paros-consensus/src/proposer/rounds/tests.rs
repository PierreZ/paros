use super::*;
use crate::journal::{Command, Entry, LeaderUuid, Seq, command_fingerprint};
use crate::membership::QuorumSystem;
use crate::types::{NodeId, Value};

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
        records: vec![Value::from(seq.to_le_bytes().to_vec())],
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
    assert!(
        rounds.resend_page(RESEND_BATCH).is_empty(),
        "a cleared tally resends nothing"
    );
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
    assert!(
        rounds.stalled_delegations(1).is_empty(),
        "a fresh delegation is not stalled"
    );
    assert!(rounds.stalled(1).is_empty(), "a fresh round is not stalled");
    let page = rounds.resend_page(RESEND_BATCH);
    assert_eq!(page[0].proxy, Some(ProxyId(1)));
    assert_eq!(rounds.by_slot()[&Slot(3)].resends(), 1);
    assert_eq!(rounds.stalled_delegations(1), vec![Slot(3)]);
    assert!(
        rounds.stalled_delegations(2).is_empty(),
        "one resend is below a stall of two"
    );
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

/// A short page wraps around from the lowest round, and never past its
/// limit.
#[test]
fn a_wrapped_resend_page_keeps_its_limit() {
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    for slot in 0..4 {
        rounds.open(Slot(slot), ballot(1, 0), cmd(slot), None, None);
    }
    let first = rounds.resend_page(3);
    assert_eq!(
        first.iter().map(|p| p.slot).collect::<Vec<_>>(),
        vec![Slot(0), Slot(1), Slot(2)]
    );
    let second = rounds.resend_page(2);
    assert_eq!(
        second.iter().map(|p| p.slot).collect::<Vec<_>>(),
        vec![Slot(3), Slot(0)],
        "one round from the cursor, one wrapped"
    );
}

/// `all_at` holds only when every open round runs at the ballot, and
/// `close_below` keeps a round at the ballot itself.
#[test]
fn rounds_below_a_ballot_close_and_rounds_at_it_stay() {
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    rounds.open(Slot(1), ballot(1, 0), cmd(1), None, None);
    rounds.open(Slot(2), ballot(2, 0), cmd(2), None, None);
    assert!(!rounds.all_at(ballot(2, 0)));
    assert_eq!(rounds.close_below(ballot(2, 0)), vec![Slot(1)]);
    assert!(rounds.all_at(ballot(2, 0)));
    assert!(rounds.is_open_at(Slot(2), ballot(2, 0)));
}

/// A move to the column a round already runs against is no move; a move to
/// another column is counted once.
#[test]
fn a_move_to_the_same_column_is_refused() {
    let grid = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
        QuorumSystem::grid(2, 2),
    );
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    rounds.open(Slot(1), ballot(1, 0), cmd(1), None, Some(0));
    assert_eq!(rounds.by_slot()[&Slot(1)].moves(), 0);
    assert!(!rounds.move_to(&grid, Slot(1), Some(0), None));
    assert_eq!(rounds.by_slot()[&Slot(1)].moves(), 0);
    assert!(rounds.move_to(&grid, Slot(1), Some(1), None));
    assert!(rounds.move_to(&grid, Slot(1), Some(0), None));
    assert_eq!(rounds.by_slot()[&Slot(1)].moves(), 2);
}

/// `fold_accepted_in` at the role (#907 (core unit tests)): the addressee
/// guard counts a vote only from the round's own column of the
/// configuration. A vote from another column, from outside the
/// configuration, or for a slot with no round never counts, and only the
/// round's full column decides it.
#[test]
fn a_vote_counts_only_from_the_rounds_column() {
    // A 2 x 2 grid, members in row order: column 1 holds nodes 1 and 3.
    let config = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
        QuorumSystem::grid(2, 2),
    );
    let vhash = command_fingerprint(&cmd(1));
    let mut rounds: Rounds<NodeId, Command> = Rounds::new();
    rounds.open(Slot(5), ballot(1, 0), cmd(1), None, Some(1));
    assert!(
        !rounds.fold_accepted_in(&config, NodeId(0), ballot(1, 0), Slot(5), vhash),
        "a vote from another column is not the round's"
    );
    assert!(
        !rounds.fold_accepted_in(&config, NodeId(9), ballot(1, 0), Slot(5), vhash),
        "a vote from outside the configuration never counts"
    );
    assert!(
        !rounds.fold_accepted_in(&config, NodeId(1), ballot(1, 0), Slot(6), vhash),
        "a vote for a slot with no round never counts"
    );
    assert!(rounds.fold_accepted_in(&config, NodeId(1), ballot(1, 0), Slot(5), vhash));
    assert!(
        rounds.decided(Slot(5), &config).is_none(),
        "one vote of a full column does not decide"
    );
    assert!(rounds.fold_accepted_in(&config, NodeId(3), ballot(1, 0), Slot(5), vhash));
    assert_eq!(
        rounds.decided(Slot(5), &config),
        Some((ballot(1, 0), cmd(1))),
        "the full column decides"
    );
}
