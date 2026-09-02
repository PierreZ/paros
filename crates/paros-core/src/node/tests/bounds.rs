//! Resource bounds of the public `RawNode` API (issue #96): Promise paging,
//! bounded leader recovery, bounded chosen-prefix release, Nack round
//! isolation, and Accepted fingerprints. Every scenario here is deterministic
//! and drives only public methods; the simulation campaign remains responsible
//! for emergent cluster interleavings.

use std::collections::BTreeMap;

use super::{
    Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, LEADER_RECOVERY_BATCH, Message,
    NodeId, NodeRole, PROMISE_BATCH, ProposeResult, RawNode, Slot, TestStorage, Value,
    command_fingerprint,
};

const SUFFIX_LEN: u64 = 2 * PROMISE_BATCH as u64 + 2;

fn command(slot: u64) -> Command {
    Command::User(Entry {
        client: ClientId(7),
        seq: ClientSeq(slot),
        value: Value(slot.to_le_bytes().to_vec()),
    })
}

type ReadyOutput = (Vec<(NodeId, Message)>, Option<(usize, usize, usize)>);

fn take_ready(node: &mut RawNode) -> ReadyOutput {
    let ready = node.ready();
    let messages = ready.messages().to_vec();
    let recovery = ready.recovery_batch();
    ready.advance();
    (messages, recovery)
}

fn candidate(id: u64) -> (RawNode, Ballot) {
    let mut node = RawNode::new(&TestStorage::new(id, &[0, 1, 2]));
    node.set_election_timeout(1);
    node.tick();
    let ballot = node.ballot();
    let _ = take_ready(&mut node);
    (node, ballot)
}

/// A long accepted suffix is served in `PROMISE_BATCH`-sized Promise pages,
/// each continuation is requested exactly, only the terminal page completes the
/// election, and the leader's recovery of that suffix is paged the same way.
// One linear choreography: the recovery paging depends on the election the
// promise paging completes, so splitting it would scatter the scenario.
#[allow(clippy::too_many_lines)]
#[test]
fn promise_suffix_and_leader_recovery_are_paged() {
    let (mut candidate, ballot) = candidate(1);

    let mut storage = TestStorage::new(0, &[0, 1, 2]);
    for slot in 0..SUFFIX_LEN {
        storage
            .accepted
            .insert(Slot(slot), (Ballot::zero(), command(slot)));
    }
    let mut acceptor = RawNode::new(&storage);
    let mut cursor = Slot(0);
    let mut pages = 0_usize;
    let mut entries_seen = 0_usize;
    let first_recovery;

    loop {
        acceptor.step(Message::Prepare {
            config_id: ConfigId::default(),
            from: NodeId(1),
            ballot,
            from_slot: cursor,
        });
        let (messages, _) = take_ready(&mut acceptor);
        let Some(Message::Promise {
            from,
            ballot: promised,
            from_slot,
            accepted,
            next_from_slot,
            ..
        }) = messages
            .into_iter()
            .map(|(_, message)| message)
            .find(|message| matches!(message, Message::Promise { .. }))
        else {
            panic!("the bounded suffix choreography receives a Promise page");
        };
        assert!(
            accepted.len() <= PROMISE_BATCH,
            "a Promise carries at most one bounded suffix chunk ({})",
            accepted.len()
        );
        pages += 1;
        entries_seen += accepted.len();

        candidate.step(Message::Promise {
            faulty: BTreeMap::new(),
            config_id: ConfigId::default(),
            from,
            ballot: promised,
            from_slot,
            accepted,
            next_from_slot,
        });
        let (candidate_messages, recovery) = take_ready(&mut candidate);
        if let Some(next) = next_from_slot {
            let Some(Message::Prepare {
                from,
                ballot: continuation_ballot,
                from_slot,
                ..
            }) = candidate_messages
                .into_iter()
                .map(|(_, message)| message)
                .find(|message| matches!(message, Message::Prepare { .. }))
            else {
                panic!("a partial Promise page requests its exact continuation");
            };
            assert!(
                from == NodeId(1) && continuation_ballot == ballot && from_slot == next,
                "a partial Promise page requests its exact continuation"
            );
            cursor = next;
        } else {
            first_recovery = recovery;
            break;
        }
    }

    assert!(
        pages > 1 && entries_seen == usize::try_from(SUFFIX_LEN).expect("fits"),
        "a Promise suffix continues across bounded pages"
    );
    assert_eq!(
        candidate.role(),
        NodeRole::Leader,
        "only a terminal Promise page completes the election quorum"
    );

    let mut recovery = first_recovery;
    let mut recovery_pages = 0_usize;
    loop {
        let Some((started, _gap_fills, remaining)) = recovery else {
            panic!("leader recovery exposes its bounded Ready chunk");
        };
        assert!(
            started <= LEADER_RECOVERY_BATCH,
            "a leader starts at most one bounded recovery chunk per Ready ({started})"
        );
        recovery_pages += 1;
        if remaining == 0 {
            break;
        }
        candidate.advance_recovery();
        recovery = take_ready(&mut candidate).1;
    }
    assert!(
        recovery_pages > 1,
        "a leader recovery suffix continues across bounded Ready batches"
    );
}

/// A gap fill can unlock a long suffix the node already learned chosen out of
/// order. That contiguous release is recovery work too: it must not turn one
/// Accepted ack into an unbounded Ready write batch.
#[test]
fn gap_fill_releases_the_chosen_prefix_in_bounded_chunks() {
    let mut gapped = RawNode::new(&TestStorage::new(0, &[0, 1, 2]));
    let prior = Ballot {
        round: 1,
        node: NodeId(1),
    };
    for slot in 1..SUFFIX_LEN {
        gapped.step(Message::Commit {
            config_id: ConfigId::default(),
            from: NodeId(1),
            ballot: prior,
            slot: Slot(slot),
            command: command(slot),
        });
        let _ = take_ready(&mut gapped);
    }
    gapped.set_election_timeout(1);
    gapped.tick();
    let gap_ballot = gapped.ballot();
    let _ = take_ready(&mut gapped);
    gapped.step(Message::Promise {
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: gap_ballot,
        from_slot: Slot(0),
        accepted: BTreeMap::new(),
        next_from_slot: None,
    });
    let _ = take_ready(&mut gapped);
    let noop = Command::Control(Control::Noop);
    gapped.step(Message::Accepted {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: gap_ballot,
        slot: Slot(0),
        vhash: command_fingerprint(&noop),
    });
    let mut prefix_pages = 0_usize;
    loop {
        let ready = gapped.ready();
        assert!(
            ready.writes().len() <= LEADER_RECOVERY_BATCH + 1,
            "a gap fill releases at most one bounded chosen-prefix write chunk ({})",
            ready.writes().len()
        );
        prefix_pages += 1;
        ready.advance();
        if gapped.hard_state().chosen_index == Some(Slot(SUFFIX_LEN - 1)) {
            break;
        }
        gapped.advance_recovery();
    }
    assert!(
        prefix_pages > 1,
        "a chosen-prefix continuation drains across bounded Ready batches"
    );
}

/// A Nack's `promised` hint never selects a future campaign round, and a
/// same-ballot continuation closes a different stale campaign.
#[test]
fn nack_hints_and_stale_campaigns_are_isolated() {
    let (mut nacked, rejected) = candidate(2);
    nacked.step(Message::Nack {
        config_id: ConfigId::default(),
        from: NodeId(0),
        ballot: rejected,
        promised: Ballot {
            round: u64::MAX,
            node: NodeId(0),
        },
        slot: Slot(0),
    });
    let _ = take_ready(&mut nacked);
    nacked.set_election_timeout(1);
    nacked.tick();
    assert_eq!(
        nacked.ballot().round,
        rejected.round + 1,
        "a Nack wire hint does not select a future campaign round"
    );

    let (mut stale, stale_ballot) = candidate(0);
    let learned = Ballot {
        round: stale_ballot.round + 1,
        node: NodeId(1),
    };
    stale.step(Message::Commit {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: learned,
        slot: Slot(0),
        command: command(0),
    });
    let _ = take_ready(&mut stale);
    stale.step(Message::Prepare {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: learned,
        from_slot: Slot(1),
    });
    assert_eq!(
        stale.role(),
        NodeRole::Follower,
        "a same-ballot continuation closes a different stale campaign"
    );
}

/// An `Accepted` is credited only when its fingerprint names the admitted
/// command, identity included.
#[test]
fn accepted_fingerprints_include_identity() {
    let (mut proposer, proposal_ballot) = candidate(0);
    proposer.step(Message::Promise {
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: proposal_ballot,
        from_slot: Slot(0),
        accepted: BTreeMap::new(),
        next_from_slot: None,
    });
    let _ = take_ready(&mut proposer);
    let ProposeResult::Accepted(slot) =
        proposer.propose(ClientId(9), ClientSeq(1), Value(vec![1, 2, 3]))
    else {
        panic!("leader rejected the proposal");
    };
    let admitted = Command::User(Entry {
        client: ClientId(9),
        seq: ClientSeq(1),
        value: Value(vec![1, 2, 3]),
    });
    let admitted_hash = command_fingerprint(&admitted);
    assert_ne!(
        command_fingerprint(&command(0)),
        admitted_hash,
        "command fingerprints include identity"
    );
    proposer.step(Message::Accepted {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: proposal_ballot,
        slot,
        vhash: admitted_hash ^ 1,
    });
    assert!(
        proposer.hard_state().chosen_index.is_none(),
        "a mismatched Accepted fingerprint is not credited"
    );
    proposer.step(Message::Accepted {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: proposal_ballot,
        slot,
        vhash: admitted_hash,
    });
    assert_eq!(
        proposer.hard_state().chosen_index,
        Some(slot),
        "a matching Accepted fingerprint can complete its quorum"
    );
}
