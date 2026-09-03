#[allow(clippy::wildcard_imports)]
use super::*;

#[test]
fn follower_resets_election_clock_on_leader_traffic() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    // Arm node 1 with a timeout of 3 ticks. It ages two ticks (still Follower),
    // then leader contact (a streamed Accept) resets its clock, so two more
    // ticks still leave it a Follower. Without the reset it would have hit 3.
    nodes[1].set_election_timeout(3);
    nodes[1].tick();
    nodes[1].tick();
    assert_eq!(
        nodes[1].role(),
        NodeRole::Follower,
        "two ticks is under the timeout"
    );
    let r = nodes[0].propose(ClientId(1), ClientSeq(1), val(9));
    assert!(matches!(r, ProposeResult::Accepted(_)));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    nodes[1].tick();
    nodes[1].tick();
    assert_eq!(
        nodes[1].role(),
        NodeRole::Follower,
        "leader contact reset the clock, so it still has not timed out"
    );
}

#[test]
fn a_leader_without_an_ack_quorum_steps_down_after_its_window() {
    // Pins the #95 CheckQuorum contract after its sim red→green (23 zombie
    // seeds, e.g. 901969623722906706): an isolated leader must not stay
    // Leader past an ack-quorum-less election-timeout window.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    nodes[0].set_election_timeout(3);
    // Tick without ever delivering the beats (a fully partitioned leader):
    // the first window may have been pre-credited by `make_leader`'s
    // delivered beat, so demotion lands within two windows at the latest.
    let mut demoted_at = None;
    for i in 0..10 {
        nodes[0].tick();
        let _ = drain(&mut nodes[0]);
        if !nodes[0].is_leader() {
            demoted_at = Some(i);
            break;
        }
    }
    let at = demoted_at.expect("an isolated leader demotes itself (CheckQuorum)");
    assert!(at <= 6, "within two ack windows, demoted at tick {at}");
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(nodes[0].quorum_lost_step_downs(), 1);
    assert!(
        nodes[0].needs_election_timeout(),
        "the demoted leader re-enters the ordinary election path"
    );
}

#[test]
fn a_leader_hearing_acks_keeps_leadership_across_windows() {
    // The healthy half of CheckQuorum: every delivered beat is acked by both
    // followers, so the window refills each time it closes and leadership is
    // never disturbed.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    nodes[0].set_election_timeout(2);
    for _ in 0..8 {
        nodes[0].tick();
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q); // beats out, acks back, window credited
        assert!(nodes[0].is_leader(), "a reachable leader never demotes");
    }
    assert_eq!(nodes[0].quorum_lost_step_downs(), 0);
}

#[test]
fn voluntary_step_down_resigns_and_drops_the_volatile_leadership_state() {
    // The public [`RawNode::step_down`] half of the same property: a leader that
    // resigns of its own accord (no deposing Prepare, no crash) keeps every
    // durable commitment — the promised ballot and the accepted log — and drops
    // exactly the volatile leadership state: the in-flight Phase-2 rounds and any
    // unconfirmed read-index round. This is the primitive the simulation drives to
    // make a never-re-sent hole permanent (#54) and to create election churn.
    let mut nodes = cluster_with_three_chosen();
    let promise_before = nodes[0].hard_state().max_promised_ballot;
    let ballot_before = nodes[0].ballot();

    nodes[0].propose(ClientId(9), ClientSeq(1), val(90));
    let _ = drain(&mut nodes[0]);
    let _ = nodes[0].read_index(1);
    let _ = drain(&mut nodes[0]);
    // Snapshot the log *after* the proposal self-accepted into slot 3: that
    // accept is durable and must survive the resignation too.
    let log_before = nodes[0].accepted().clone();
    assert!(
        !nodes[0].proposer.rounds().is_empty(),
        "a Phase-2 round is in flight"
    );
    assert_eq!(nodes[0].read_rounds.len(), 1, "a read round is pending");

    nodes[0].step_down();

    assert!(!nodes[0].is_leader(), "the leader resigned");
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(
        nodes[0].leader(),
        None,
        "it resigned rather than handing over, so it knows no leader"
    );
    assert!(
        nodes[0].proposer.rounds().is_empty(),
        "the volatile in-flight rounds go with the leadership — this is what makes \
         a hole below a decided slot permanent"
    );
    assert!(
        nodes[0].read_rounds.is_empty(),
        "unconfirmed read rounds die with the leadership"
    );
    assert!(
        nodes[0].needs_election_timeout(),
        "it asks the driver for a fresh randomized election timeout"
    );
    assert_eq!(
        nodes[0].hard_state().max_promised_ballot,
        promise_before,
        "the durable promise never regresses on a step-down"
    );
    assert_eq!(
        nodes[0].ballot(),
        ballot_before,
        "and its operating ballot is unchanged"
    );
    assert_eq!(
        *nodes[0].accepted(),
        log_before,
        "the accepted log is durable state, untouched"
    );

    // Idempotent, and a no-op on a node that never led.
    nodes[0].step_down();
    nodes[1].step_down();
    assert!(!nodes[1].is_leader());
    assert!(
        drain(&mut nodes[1]).is_empty(),
        "a non-leader resigns silently"
    );
}

#[test]
fn a_round_the_driver_never_re_sends_stalls_until_one_call_heals_it() {
    // The [`RawNode::resend_pending`] contract, both halves. A driver that beats
    // but never calls it leaves a round whose first `Accept` was lost pending
    // forever — the cluster is *safe* (the slot is simply undecided) but the
    // contiguous chosen prefix is frozen below it. A single call decides it, which
    // is what proves the stall was the skipped re-send and nothing else.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0: healthy.
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: every `Accept` is lost, so the round is pending on the leader alone.
    nodes[0].propose(ClientId(1), ClientSeq(2), val(20));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, msg| {
        !matches!(msg, Message::Accept { .. })
    });

    // Beat for a while, and never re-send. The beats keep the followers from
    // campaigning, so nothing else can heal the slot either.
    for _ in 0..5 {
        nodes[0].tick();
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
    assert_eq!(
        chosen_at(&nodes[1], 1),
        None,
        "no re-send, so the follower never even saw slot 1"
    );
    assert_eq!(
        nodes[0].hard_state().chosen_index,
        Some(Slot(0)),
        "and the leader's own prefix is frozen one below the pending slot"
    );

    // One call, and the round completes on the very next exchange.
    nodes[0].resend_pending();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(
        chosen_at(&nodes[1], 1),
        Some(val(20)),
        "a single re-send is enough: skipping was the only thing holding it"
    );
    assert_eq!(
        nodes[0].hard_state().chosen_index,
        Some(Slot(1)),
        "the prefix walks past it"
    );
    assert!(
        !nodes[0].has_pending_accepts(),
        "the hook is no longer consulted once the round decides"
    );
}

#[test]
fn a_step_down_makes_a_never_re_sent_hole_permanent_until_the_noop_fill() {
    // The #54 arc built entirely from the two public decisions the simulation
    // perturbs — no crash, no packet-loss emulation at the protocol layer. The
    // driver skips the re-send of slot 1 (safe, pure optimization loss) while slot
    // 2 decides normally, and then the leader resigns (also safe). The volatile
    // `proposer` map goes with the leadership, so nothing will ever re-propose
    // slot 1 — and a promise quorum that never saw it steps clean over it. The
    // `Control::Noop` gap fill is the only thing that closes it.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0: healthy.
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: its `Accept`s are lost and the driver never re-sends it.
    nodes[0].propose(ClientId(1), ClientSeq(2), val(20));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, msg| {
        !matches!(msg, Message::Accept { .. })
    });

    // Slot 2: reaches node 1, so the {0,1} quorum decides it — an *undecided* slot
    // now sits below a *decided* one.
    nodes[0].propose(ClientId(1), ClientSeq(3), val(30));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, msg| {
        !(matches!(msg, Message::Accept { .. }) && to == NodeId(2))
    });
    assert_eq!(chosen_at(&nodes[1], 2), Some(val(30)));

    // The leader resigns. Slot 1 lived only in its volatile `proposer` map.
    nodes[0].step_down();
    assert!(
        nodes[0].proposer.rounds().is_empty(),
        "the only record that slot 1 was still being proposed is gone"
    );

    // Nodes 1 and 2 elect; neither ever saw slot 1, so `Election::recovered` holds
    // slot 2 alone and `next_slot` jumps over the hole.
    nodes[1].set_election_timeout(1);
    nodes[1].tick();
    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));
    assert!(nodes[1].is_leader());
    assert_eq!(
        nodes[1].election_gap_fills(),
        1,
        "the new leader found the hole the quorum never reported and filled it"
    );

    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));
    assert_eq!(
        nodes[1].hard_state().chosen_index,
        Some(Slot(2)),
        "the frozen prefix walks past the filled hole"
    );
    assert_eq!(nodes[1].chosen_gap(), None, "nothing is stranded any more");
}
