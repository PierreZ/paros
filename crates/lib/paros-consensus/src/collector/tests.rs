use super::*;
use crate::membership::QuorumSystem;

fn config(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        QuorumSystem::Majority,
    )
}

fn set(generation: u64) -> MatchmakerSet {
    MatchmakerSet::new(
        MatchmakerGeneration(generation),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    )
}

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(0),
    }
}

fn ack(matchmaker: u64, generation: u64, watermark: Ballot) -> GcAck {
    GcAck {
        matchmaker: MatchmakerId(matchmaker),
        generation: MatchmakerGeneration(generation),
        applied: true,
        watermark,
        newer: None,
        stopped: false,
    }
}

/// The fence the collector was built with is the one it reports: the GC
/// wiring judges Region 1 by it.
#[test]
fn the_fence_is_the_one_the_campaign_gave() {
    let gc = Collector::new(MatchmakerGeneration(0), Some(Slot(7)), &[]);
    assert_eq!(gc.fence(), Some(Slot(7)));
    let open = Collector::new(MatchmakerGeneration(0), None, &[]);
    assert_eq!(open.fence(), None);
}

/// An effective floor that was never requested is a state no fold reaches:
/// the check must name it.
#[test]
#[should_panic(expected = "an effective floor was requested first")]
fn an_effective_floor_that_was_never_requested_breaks_the_check() {
    let mut gc = Collector::new(MatchmakerGeneration(0), None, &[]);
    gc.effective = Some((ballot(1), Vec::new()));
    gc.assert_invariants();
}

/// An ack counted before the requests went out breaks the check.
#[test]
#[should_panic(expected = "no ack is counted before the requests")]
fn an_ack_before_the_requests_breaks_the_check() {
    let mut gc = Collector::new(MatchmakerGeneration(0), None, &[]);
    gc.acked_by.insert(MatchmakerId(0));
    gc.assert_invariants();
}

/// A floor that retires a node `H_b` never named breaks the check.
#[test]
#[should_panic(expected = "a floor retires only members of H_b")]
fn a_floor_that_retires_a_stranger_breaks_the_check() {
    let mut gc = Collector::new(MatchmakerGeneration(0), None, &[config(&[0, 1, 2])]);
    gc.requested = true;
    gc.effective = Some((ballot(1), vec![NodeId(9)]));
    gc.assert_invariants();
}

/// A new generation starts the tally over: the requests must go out again
/// and the acks of the replaced generation count for nothing.
#[test]
fn a_new_generation_starts_the_tally_over() {
    let mut gc = Collector::new(MatchmakerGeneration(0), None, &[]);
    gc.request();
    let b = ballot(3);
    let c = config(&[0, 1, 2]);
    assert_eq!(
        gc.fold_ack(&ack(0, 0, b), &set(0), b, &c),
        GcStep::Acked { remaining: 1 }
    );
    gc.reset_for_generation(MatchmakerGeneration(1));
    assert!(!gc.requested(), "the new generation is asked again");
    assert_eq!(
        gc.fold_ack(&ack(1, 1, b), &set(1), b, &c),
        GcStep::Ignored,
        "no ack counts before the requests to the new generation"
    );
    gc.request();
    assert_eq!(
        gc.fold_ack(&ack(1, 1, b), &set(1), b, &c),
        GcStep::Acked { remaining: 1 },
        "the ack of the replaced generation is not carried over"
    );
}

/// An ack whose generation matches the set but not the generation the
/// tally addressed is ignored: the tally counts one generation only.
#[test]
fn an_ack_for_a_set_the_tally_did_not_address_is_ignored() {
    let mut gc = Collector::new(MatchmakerGeneration(0), None, &[]);
    gc.request();
    let b = ballot(3);
    let c = config(&[0, 1, 2]);
    for m in 0..3 {
        assert_eq!(
            gc.fold_ack(&ack(m, 1, b), &set(1), b, &c),
            GcStep::Ignored,
            "matchmaker {m}"
        );
    }
    assert!(gc.effective().is_none());
}
