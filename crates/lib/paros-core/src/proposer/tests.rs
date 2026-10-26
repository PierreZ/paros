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

fn config(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().map(|n| NodeId(*n)).collect(),
        QuorumSystem::Majority,
    )
}

/// Fold an empty terminal `Promise` page from `from` at `ballot(1, 0)`.
fn fold_empty(p: &mut Proposer<NodeId, Command>, from: u64, from_slot: Slot) -> PromiseFold {
    p.fold_promise(
        NodeId(from),
        ballot(1, 0),
        from_slot,
        BTreeMap::new(),
        BTreeMap::new(),
        None,
    )
}

/// Fold an empty terminal `Promise` page from `from` into the probe.
fn fold_empty_probe(
    p: &mut Proposer<NodeId, Command>,
    from: u64,
    at: Ballot,
    from_slot: Slot,
) -> PromiseFold {
    p.fold_probe_promise(
        NodeId(from),
        at,
        from_slot,
        &BTreeMap::new(),
        &BTreeMap::new(),
        None,
    )
}

fn opened(prior: Vec<AcceptorConfig>) -> Proposer<NodeId, Command> {
    let mut p = Proposer::new();
    let mut expected: Vec<NodeId> = prior
        .iter()
        .flat_map(|c| c.members().iter().copied())
        .chain([NodeId(1), NodeId(2)])
        .filter(|n| *n != NodeId(0))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    let targets = p.open_phase1(
        Campaign {
            me: Some(NodeId(0)),
            ballot: ballot(1, 0),
            config: config(&[0, 1, 2]),
            prior,
            from_slot: Slot(0),
        },
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert_eq!(
        targets, expected,
        "Phase 1 addresses the union of the prior configurations and C_b"
    );
    p
}

/// Phase 1 completes only with a quorum of *every* prior configuration,
/// never of their union (#121).
#[test]
fn phase1_needs_a_quorum_of_every_prior_configuration() {
    let mut p = opened(vec![config(&[0, 1, 2]), config(&[2, 3, 4])]);
    assert_eq!(fold_empty(&mut p, 1, Slot(0)), PromiseFold::Answered);
    // {0, 1, 3} is a majority of the union's five members, yet the
    // second configuration holds only one promise (node 3): the union
    // rule would wrongly complete Phase 1 here.
    assert_eq!(fold_empty(&mut p, 3, Slot(0)), PromiseFold::Answered);
    assert!(!p.phase1_won(ballot(1, 0)), "quorum(union) is not the rule");
    assert_eq!(fold_empty(&mut p, 4, Slot(0)), PromiseFold::Answered);
    assert!(p.phase1_won(ballot(1, 0)));
    assert!(
        !p.phase1_won(ballot(2, 1)),
        "a campaign below the node's own promise never wins"
    );
}

/// The merge keeps the highest-ballot report per slot (P2c) and the
/// closed outcome carries it.
#[test]
fn promise_merge_is_p2c() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut low = BTreeMap::new();
    low.insert(Slot(0), (ballot(0, 1), cmd(1)));
    let mut high = BTreeMap::new();
    high.insert(Slot(0), (ballot(0, 2), cmd(2)));
    p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(0),
        high,
        BTreeMap::new(),
        None,
    );
    p.fold_promise(NodeId(2), ballot(1, 0), Slot(0), low, BTreeMap::new(), None);
    assert!(p.phase1_won(ballot(1, 0)));
    let out = p.close_phase1(|_| false);
    assert_eq!(out.recovered.get(&Slot(0)), Some(&(ballot(0, 2), cmd(2))));
    assert_eq!(out.highest_reported, Some(Slot(0)));
    assert!(out.blocked.is_empty());
    assert!(p.probe().is_none());
    assert!(p.election().is_none());
}

/// A page at the wrong cursor, or from a sender already counted, is
/// ignored; a continuation names the next cursor.
#[test]
fn promise_pages_are_cursor_checked() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let full: BTreeMap<Slot, (Ballot, Command)> = (0..PROMISE_BATCH as u64)
        .map(|s| (Slot(s), (ballot(0, 1), cmd(s))))
        .collect();
    assert_eq!(
        p.fold_promise(
            NodeId(1),
            ballot(1, 0),
            Slot(0),
            full,
            BTreeMap::new(),
            Some(Slot(PROMISE_BATCH as u64)),
        ),
        PromiseFold::Continue(Slot(PROMISE_BATCH as u64))
    );
    assert_eq!(
        fold_empty(&mut p, 1, Slot(3)),
        PromiseFold::Ignored,
        "a page at the wrong cursor is ignored"
    );
    assert_eq!(
        fold_empty(&mut p, 1, Slot(PROMISE_BATCH as u64)),
        PromiseFold::Answered
    );
    assert_eq!(
        fold_empty(&mut p, 1, Slot(0)),
        PromiseFold::Ignored,
        "a counted sender is not merged twice"
    );
}

/// A faulty report above the best `have` blocks the slot (Case 3) until
/// a straggler's answer lets the probe decide it (Case 1).
/// Review finding C8: the probe pages a straggler's `Promise` through the
/// same [`PromiseTally`] the election does, and only the election half was
/// tested. A page at the wrong cursor is ignored, the continuation is
/// tracked per straggler, and a straggler that answered completely is
/// never merged twice.
#[test]
fn probe_promise_pages_are_cursor_checked() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(0), ballot(0, 2));
    p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(0),
        BTreeMap::new(),
        faulty,
        None,
    );
    let out = p.close_phase1(|_| false);
    assert!(out.blocked.contains(&Slot(0)));
    assert_eq!(
        p.probe()
            .expect("the probe opens for the blocked slot")
            .requery(NodeId(0))
            .from_slot,
        Slot(0),
        "the probe inherits the election's first slot"
    );
    // A full first page from the straggler leaves it mid-suffix.
    let full: BTreeMap<Slot, (Ballot, Command)> = (0..PROMISE_BATCH as u64)
        .map(|s| (Slot(s), (ballot(0, 1), cmd(s))))
        .collect();
    assert_eq!(
        p.fold_probe_promise(
            NodeId(2),
            ballot(1, 0),
            Slot(0),
            &full,
            &BTreeMap::new(),
            Some(Slot(PROMISE_BATCH as u64)),
        ),
        PromiseFold::Continue(Slot(PROMISE_BATCH as u64))
    );
    assert_eq!(
        p.probe().expect("still open").requery(NodeId(0)).to,
        vec![NodeId(2)],
        "a straggler mid-suffix is still a straggler"
    );
    assert_eq!(
        fold_empty_probe(&mut p, 2, ballot(1, 0), Slot(3)),
        PromiseFold::Ignored,
        "a page at the wrong cursor is ignored"
    );
    assert_eq!(
        fold_empty_probe(&mut p, 2, ballot(2, 0), Slot(PROMISE_BATCH as u64)),
        PromiseFold::Ignored,
        "a page at another ballot is ignored"
    );
    // The terminal page decides the blocked slot from the first page's
    // `have` and closes the probe.
    assert_eq!(
        fold_empty_probe(&mut p, 2, ballot(1, 0), Slot(PROMISE_BATCH as u64)),
        PromiseFold::Answered
    );
    assert_eq!(
        p.resolve_probe(),
        vec![ProbeDecision {
            slot: Slot(0),
            command: Some(cmd(0)),
        }]
    );
    assert!(p.probe().is_none());
}

#[test]
fn faulty_report_blocks_until_the_probe_decides() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(0), ballot(0, 2));
    p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(0),
        BTreeMap::new(),
        faulty,
        None,
    );
    assert!(p.phase1_won(ballot(1, 0)));
    let out = p.close_phase1(|_| false);
    assert!(
        out.blocked.contains(&Slot(0)),
        "a faulty report with no have blocks"
    );
    let probe = p.probe().expect("the probe opens for the blocked slot");
    assert_eq!(probe.requery(NodeId(0)).to, vec![NodeId(2)]);
    assert!(p.resolve_probe().is_empty(), "Case 3: wait");
    let mut have = BTreeMap::new();
    have.insert(Slot(0), (ballot(0, 2), cmd(7)));
    assert_eq!(
        p.fold_probe_promise(
            NodeId(2),
            ballot(1, 0),
            Slot(0),
            &have,
            &BTreeMap::new(),
            None
        ),
        PromiseFold::Answered
    );
    let decisions = p.resolve_probe();
    assert_eq!(
        decisions,
        vec![ProbeDecision {
            slot: Slot(0),
            command: Some(cmd(7)),
        }]
    );
    assert!(
        p.probe().is_none(),
        "the last blocked slot closes the probe"
    );
}

/// A round decides on a quorum of *its configuration*; a vote from a
/// non-member never counts toward it.
#[test]
fn rounds_decide_on_a_configuration_quorum() {
    let mut p = Proposer::new();
    let c = config(&[0, 1, 2]);
    p.open_round(Slot(5), ballot(1, 0), cmd(1), Some(NodeId(0)), None);
    assert!(p.decided(Slot(5), &c).is_none());
    assert!(p.fold_accepted(
        NodeId(1),
        ballot(1, 0),
        Slot(5),
        command_fingerprint(&cmd(1))
    ));
    assert_eq!(p.decided(Slot(5), &c), Some((ballot(1, 0), cmd(1))));
    assert!(p.supersedes(ballot(1, 0), Slot(5)));
    assert!(!p.supersedes(ballot(2, 0), Slot(5)));
    p.close_round(Slot(5));
    assert!(p.rounds().is_empty());
}

/// The recovery pump fills undescribed slots only under a Phase-1-backed
/// policy.
#[test]
fn recovery_policy_decides_the_fill() {
    let mut p: Proposer<NodeId, Command> = Proposer::new();
    let mut recovered = BTreeMap::new();
    recovered.insert(Slot(1), cmd(1));
    p.open_recovery(
        recovered.clone(),
        BTreeSet::new(),
        Slot(0),
        Slot(2),
        RecoveryPolicy::Phase1Backed,
    );
    assert_eq!(p.recovery_next(), Some((Slot(0), RecoveryStep::Fill)));
    assert_eq!(
        p.recovery_next(),
        Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
    );
    assert_eq!(p.recovery_next(), None);
    assert_eq!(p.recovery_remaining(), 0);
    p.close_drained_recovery();
    assert!(p.recovery().is_none());

    p.open_recovery(
        recovered,
        BTreeSet::new(),
        Slot(0),
        Slot(2),
        RecoveryPolicy::Inherited,
    );
    assert_eq!(
        p.recovery_next(),
        Some((Slot(0), RecoveryStep::Undescribed))
    );
    assert_eq!(
        p.recovery_next(),
        Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
    );
    p.close_drained_recovery();
    assert!(p.recovery().is_none());
}
