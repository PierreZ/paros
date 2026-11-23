use super::*;
use crate::journal::{Entry, LeaderUuid, Seq};
use crate::membership::QuorumSystem;
use crate::proposer::Round;
use crate::types::Value;

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

fn majority(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        QuorumSystem::Majority,
    )
}

fn delegation(proxy: u64, leader: u64, b: Ballot, slot: u64, command: Command) -> Message {
    Message::Accept {
        reply_to: Party::Proxy(ProxyId(proxy)),
        leader: NodeId(leader),
        ballot: b,
        slot: Slot(slot),
        command,
        config: None,
    }
}

fn accepted(from: u64, b: Ballot, slot: u64, command: &Command) -> Message {
    Message::Accepted {
        from: NodeId(from),
        ballot: b,
        slot: Slot(slot),
        vhash: command_fingerprint(command),
    }
}

fn drain(proxy: &mut ProxyLeader) -> Vec<(Audience, Message)> {
    let ready = proxy.ready();
    let out = ready.messages().to_vec();
    ready.advance();
    out
}

/// The whole contract on one round: a delegation fans out to the column
/// with the proxy as the reply party and the leader as the hint, votes
/// from the column decide it, the `Commit` names the proxy, and a
/// re-delegation of the closed round is ignored.
#[test]
fn a_delegated_round_is_fanned_out_folded_and_committed() {
    let mut proxy = ProxyLeader::new(ProxyId(1), majority(&[0, 1, 2]), Limits::DEFAULT);
    proxy.step(delegation(1, 0, ballot(3, 0), 7, cmd(7)));
    let out = drain(&mut proxy);
    assert_eq!(out.len(), 1);
    let (audience, msg) = &out[0];
    assert_eq!(
        *audience,
        Audience::AcceptorsOf {
            config: majority(&[0, 1, 2]),
            column: None
        }
    );
    assert_eq!(
        *msg,
        Message::Accept {
            reply_to: Party::Proxy(ProxyId(1)),
            leader: NodeId(0),
            ballot: ballot(3, 0),
            slot: Slot(7),
            command: cmd(7),
            config: None,
        }
    );
    assert_eq!(proxy.delegator(Slot(7)), Some(NodeId(0)));
    // A stray vote from outside the configuration never counts.
    proxy.step(accepted(9, ballot(3, 0), 7, &cmd(7)));
    proxy.step(accepted(2, ballot(3, 0), 7, &cmd(7)));
    assert!(drain(&mut proxy).is_empty(), "one vote is not a majority");
    proxy.step(accepted(0, ballot(3, 0), 7, &cmd(7)));
    let out = drain(&mut proxy);
    assert_eq!(
        out,
        vec![(
            Audience::Learners,
            Message::Commit {
                from: Party::Proxy(ProxyId(1)),
                ballot: ballot(3, 0),
                slot: Slot(7),
                command: cmd(7),
            }
        )]
    );
    assert!(proxy.rounds().is_empty());
    assert_eq!(proxy.counters().decided, 1);
    // The closed round is remembered: a late re-delegation is ignored.
    proxy.step(delegation(1, 0, ballot(3, 0), 7, cmd(7)));
    assert!(
        drain(&mut proxy).is_empty(),
        "a late re-delegation of a closed round is ignored"
    );
    assert_eq!(proxy.counters().ignored, 1);
}

/// A re-delegation re-fans-out with the *new* leader hint (a handoff
/// successor), a lower ballot is ignored, a higher one replaces the
/// round, and a delegation for another proxy is not this proxy's.
#[test]
fn re_delegations_refresh_the_leader_and_ballots_order_the_rounds() {
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), Limits::DEFAULT);
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    drain(&mut proxy);
    proxy.step(delegation(0, 1, ballot(3, 0), 4, cmd(4)));
    let out = drain(&mut proxy);
    assert!(
        matches!(&out[0].1, Message::Accept { leader, .. } if *leader == NodeId(1)),
        "the re-fan-out names the successor"
    );
    assert_eq!(proxy.counters().refanned, 1);
    proxy.step(delegation(0, 2, ballot(2, 2), 4, cmd(9)));
    assert!(drain(&mut proxy).is_empty(), "a stale ballot is ignored");
    proxy.step(delegation(0, 0, ballot(3, 0), 6, cmd(6)));
    drain(&mut proxy);
    proxy.step(delegation(0, 2, ballot(5, 2), 4, cmd(9)));
    assert_eq!(
        proxy.counters().superseded,
        2,
        "a higher ballot closes every round of the lower one"
    );
    assert_eq!(proxy.ballot(), ballot(5, 2));
    assert!(!proxy.rounds().by_slot().contains_key(&Slot(6)));
    assert_eq!(
        proxy.rounds().by_slot().get(&Slot(4)).map(Round::ballot),
        Some(ballot(5, 2))
    );
    drain(&mut proxy);
    proxy.step(delegation(0, 0, ballot(3, 0), 6, cmd(6)));
    assert!(
        drain(&mut proxy).is_empty(),
        "a delegation from the superseded leadership is ignored"
    );
    proxy.step(delegation(1, 2, ballot(5, 2), 5, cmd(5)));
    assert!(
        drain(&mut proxy).is_empty(),
        "a delegation for another proxy is not folded"
    );
    assert!(!proxy.rounds().by_slot().contains_key(&Slot(5)));
}

/// Bounded retention: a round nobody answers — the case is a slot every
/// acceptor compacted past, whose `Accept` is ignored without a reply —
/// is re-fanned-out the budget's worth of beats and then evicted with
/// no `Commit` and no `done` entry, so a later delegation of the slot
/// reopens it fresh instead of being ignored as decided. The scenario
/// the eviction exists for: a delegation delayed on the wire until
/// after the leader took the round back, decided it colocated and the
/// cluster truncated past it, released and ticked under the same
/// ballot, used to be re-fanned-out forever.
#[test]
fn an_unanswered_round_is_evicted_after_its_budget() {
    let limits = Limits {
        proxy_retention: 3,
        ..Limits::DEFAULT
    };
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), limits);
    assert_eq!(proxy.limits(), limits);
    // The late delegation of a slot the acceptors will never answer.
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    assert_eq!(drain(&mut proxy).len(), 1);
    for beat in 1..=3 {
        assert!(proxy.tick().is_empty(), "under budget at beat {beat}");
        let out = drain(&mut proxy);
        assert_eq!(out.len(), 1, "the round is re-fanned-out on beat {beat}");
        assert!(matches!(out[0].1, Message::Accept { .. }));
    }
    assert_eq!(proxy.rounds().by_slot()[&Slot(4)].resends(), 3);
    assert!(
        proxy.expire_stale(4).is_empty(),
        "a budget above the re-fan-outs evicts nothing"
    );
    assert_eq!(proxy.tick(), vec![Slot(4)]);
    assert!(!proxy.has_pending_accepts());
    assert_eq!(proxy.delegator(Slot(4)), None);
    assert!(drain(&mut proxy).is_empty(), "an eviction emits nothing");
    assert_eq!(proxy.counters().expired, 1);
    assert_eq!(proxy.counters().decided, 0);
    // Not a decision: the same delegation, under the same ballot, opens
    // the round again (a leader that still needs it re-delegates), and
    // a vote can still decide it.
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    assert_eq!(proxy.counters().delegated, 2);
    assert_eq!(proxy.counters().ignored, 0);
    assert_eq!(drain(&mut proxy).len(), 1);
    proxy.step(accepted(1, ballot(3, 0), 4, &cmd(4)));
    proxy.step(accepted(2, ballot(3, 0), 4, &cmd(4)));
    assert!(
        matches!(
            drain(&mut proxy).as_slice(),
            [(Audience::Learners, Message::Commit { .. })]
        ),
        "a reopened round decides as any round does"
    );
}

/// A refusal closes the round and reaches the leader that delegated it.
#[test]
fn a_nack_is_relayed_to_the_delegating_leader() {
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), Limits::DEFAULT);
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    drain(&mut proxy);
    proxy.step(Message::Nack {
        from: NodeId(2),
        ballot: ballot(3, 0),
        slot: Slot(4),
    });
    assert_eq!(
        drain(&mut proxy),
        vec![(
            Audience::Node(NodeId(0)),
            Message::Nack {
                from: NodeId(2),
                ballot: ballot(3, 0),
                slot: Slot(4),
            }
        )]
    );
    assert!(proxy.rounds().is_empty());
    assert_eq!(proxy.counters().relayed_nacks, 1);
}

/// A matchmaker deployment's delegation carries `C_b`: the proxy adopts
/// it, fans out to its column, and closes the rounds of the leadership
/// it superseded.
#[test]
fn a_delegation_teaches_the_proxy_its_configuration() {
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), Limits::DEFAULT);
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    drain(&mut proxy);
    let grid = AcceptorConfig::new((0..4).map(NodeId).collect(), QuorumSystem::grid(2, 2));
    proxy.step(Message::Accept {
        reply_to: Party::Proxy(ProxyId(0)),
        leader: NodeId(3),
        ballot: ballot(4, 3),
        slot: Slot(9),
        command: cmd(9),
        config: Some(grid.clone()),
    });
    assert_eq!(proxy.acceptors(), &grid);
    assert!(
        !proxy.rounds().by_slot().contains_key(&Slot(4)),
        "the old leadership's round is closed"
    );
    let out = drain(&mut proxy);
    assert_eq!(
        out[0].0,
        Audience::AcceptorsOf {
            config: grid,
            column: Some(1)
        }
    );
    assert!(
        matches!(&out[0].1, Message::Accept { config: None, .. }),
        "the fan-out carries no configuration"
    );
}

/// A proxy batch is empty until a delegation queues a fan-out, and empty
/// again after `advance`.
#[test]
fn a_proxy_batch_is_empty_until_a_delegation() {
    let mut proxy = ProxyLeader::new(ProxyId(1), majority(&[0, 1, 2]), Limits::DEFAULT);
    let ready = proxy.ready();
    assert!(ready.is_empty(), "a fresh proxy owes nothing");
    ready.advance();
    proxy.step(delegation(1, 0, ballot(3, 0), 7, cmd(7)));
    let ready = proxy.ready();
    assert!(!ready.is_empty(), "the fan-out is queued");
    ready.advance();
    let ready = proxy.ready();
    assert!(ready.is_empty(), "an advanced batch is gone");
    ready.advance();
}

/// The invariant check is the oracle of every proxy test: a delegator kept
/// for a slot with no open round is a state that no call reaches, and the
/// check must name it.
#[test]
#[should_panic(expected = "a proxy remembers a delegator only for an open round")]
fn a_delegator_without_an_open_round_breaks_the_proxy_invariants() {
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), Limits::DEFAULT);
    proxy.delegators.insert(Slot(3), NodeId(0));
    proxy.assert_invariants();
}

/// Each ignored delegation counts once: one addressed to another proxy,
/// one below the ballot the proxy works for.
#[test]
fn every_ignored_delegation_is_counted() {
    let mut proxy = ProxyLeader::new(ProxyId(1), majority(&[0, 1, 2]), Limits::DEFAULT);
    assert_eq!(proxy.id(), ProxyId(1));
    proxy.step(delegation(2, 0, ballot(3, 0), 7, cmd(7)));
    assert_eq!(proxy.counters().ignored, 1);
    assert!(!proxy.has_pending_accepts());
    proxy.step(delegation(1, 0, ballot(3, 0), 7, cmd(7)));
    assert!(proxy.has_pending_accepts());
    proxy.step(delegation(1, 0, ballot(2, 0), 8, cmd(8)));
    assert_eq!(proxy.counters().ignored, 2);
    assert_eq!(proxy.counters().delegated, 1);
}

/// A refusal from outside the configuration, or for a round that is not
/// open at that ballot, is not relayed.
#[test]
fn only_a_member_s_refusal_of_an_open_round_is_relayed() {
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), Limits::DEFAULT);
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    drain(&mut proxy);
    proxy.step(Message::Nack {
        from: NodeId(9),
        ballot: ballot(3, 0),
        slot: Slot(4),
    });
    proxy.step(Message::Nack {
        from: NodeId(2),
        ballot: ballot(3, 0),
        slot: Slot(5),
    });
    assert_eq!(drain(&mut proxy), Vec::new());
    assert!(proxy.has_pending_accepts(), "the round is still open");
    assert_eq!(proxy.counters().relayed_nacks, 0);
}

/// The proxy remembers the last `DONE_MEMORY` slots it decided, and no
/// more: the oldest of them is still ignored on a re-delegation.
#[test]
fn the_closed_round_memory_holds_exactly_its_bound() {
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), Limits::DEFAULT);
    let b = ballot(3, 0);
    let rounds = u64::try_from(DONE_MEMORY).expect("small bound") + 1;
    for slot in 0..rounds {
        proxy.step(delegation(0, 0, b, slot, cmd(slot)));
        proxy.step(accepted(0, b, slot, &cmd(slot)));
        proxy.step(accepted(1, b, slot, &cmd(slot)));
        drain(&mut proxy);
    }
    assert_eq!(proxy.counters().decided, rounds);
    // Slot 1 is the oldest remembered; slot 0 is forgotten.
    proxy.step(delegation(0, 0, b, 1, cmd(1)));
    assert!(drain(&mut proxy).is_empty(), "slot 1 is remembered");
    assert_eq!(proxy.counters().ignored, 1);
    proxy.step(delegation(0, 0, b, 0, cmd(0)));
    assert_eq!(drain(&mut proxy).len(), 1, "slot 0 is forgotten");
}

/// The beat re-fans-out one page of the open rounds, the page the proxy's
/// `Limits` name, and a round past no budget stays open: the default keeps
/// every round until it closes.
#[test]
fn the_beat_re_fans_out_one_page() {
    let limits = Limits {
        resend_page: 1,
        ..Limits::DEFAULT
    };
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), limits);
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    proxy.step(delegation(0, 0, ballot(3, 0), 5, cmd(5)));
    let _ = drain(&mut proxy);
    for _ in 0..10 {
        assert!(
            proxy.tick().is_empty(),
            "the default retention evicts nothing"
        );
        let out = drain(&mut proxy);
        assert_eq!(out.len(), 1, "one round per page");
        assert!(matches!(out[0].1, Message::Accept { .. }));
    }
    assert!(proxy.has_pending_accepts());
    assert_eq!(proxy.counters().expired, 0);
}

/// A proxy takes its limits as every deployment does: checked at
/// construction.
#[test]
#[should_panic(expected = "a proxy keeps a round for at least one re-fan-out")]
fn a_proxy_refuses_invalid_limits() {
    let _ = ProxyLeader::new(
        ProxyId(0),
        majority(&[0, 1, 2]),
        Limits {
            proxy_retention: 0,
            ..Limits::DEFAULT
        },
    );
}

/// Two delegations at one `(slot, ballot)` with two commands break P2b
/// (one ballot, one proposer): a detected safety violation, so the proxy
/// stops on it (decision 30 keeps such an assertion). A handoff successor
/// re-delegates the inherited command at the same ballot, which the proxy
/// folds: `re_delegations_refresh_the_leader_and_ballots_order_the_rounds`.
#[test]
#[should_panic(expected = "a re-delegation at the open round's ballot carries the round's command")]
fn two_commands_at_one_ballot_and_slot_stop_the_proxy() {
    let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]), Limits::DEFAULT);
    proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
    proxy.step(delegation(0, 1, ballot(3, 0), 4, cmd(5)));
}

/// On a grid the proxy fans a round out to the slot's column alone, and
/// counts only the votes of that column (#907 (core unit tests)): a vote
/// from another column is not the round's, and the full column decides.
#[test]
fn a_grid_round_is_fanned_out_and_folded_in_its_column() {
    // A 2 x 2 grid, members in row order: slot 7 is column 1, nodes 1 and 3.
    let grid = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
        QuorumSystem::grid(2, 2),
    );
    let mut proxy = ProxyLeader::new(ProxyId(0), grid.clone(), Limits::DEFAULT);
    proxy.step(delegation(0, 0, ballot(3, 0), 7, cmd(7)));
    let out = drain(&mut proxy);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].0,
        Audience::AcceptorsOf {
            config: grid,
            column: Some(1)
        },
        "a grid round goes to its slot's column"
    );
    proxy.step(accepted(0, ballot(3, 0), 7, &cmd(7)));
    proxy.step(accepted(2, ballot(3, 0), 7, &cmd(7)));
    assert!(
        drain(&mut proxy).is_empty(),
        "the votes of another column never decide the round"
    );
    proxy.step(accepted(1, ballot(3, 0), 7, &cmd(7)));
    assert!(
        drain(&mut proxy).is_empty(),
        "half a column does not decide"
    );
    proxy.step(accepted(3, ballot(3, 0), 7, &cmd(7)));
    let out = drain(&mut proxy);
    assert!(
        matches!(out.as_slice(), [(Audience::Learners, Message::Commit { slot, .. })] if *slot == Slot(7)),
        "the full column decides"
    );
}
