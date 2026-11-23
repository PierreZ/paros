//! The client walks: every seed passes every oracle, the campaign reaches
//! every site, and each oracle catches its mutant.

use journal_sim::campaign::{campaign, try_campaign};
use journal_sim::client::{
    ClientMutant, FLOOR, LED, ONCE, PAGE, REFUSED, TAIL, WRITTEN, reach, walk,
};

/// Two clients, a lossy network, seals, held and lost answers.
#[test]
fn the_client_walk() {
    campaign(&walk(ClientMutant::None, 300), reach::ALL);
}

/// The seeds of #1495 (a late copy of `SetLeader` reinstates a uuid): with one
/// value for every multi-writer mode, a late copy of `SetLeader(u, mode)`
/// brought `u` back, and a fenced write reported refused landed (100603);
/// on 100033 the client's tail also went back below the tail of a `Below`.
#[test]
fn the_seeds_of_a_late_set_leader_copy() {
    for seed in [100_603, 100_033] {
        campaign(&walk(ClientMutant::None, 1).first_seed(seed), &[]);
    }
}

fn caught(mutant: ClientMutant, oracle: &str) {
    let text = try_campaign(&walk(mutant, 300), &[]).expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn once_catches_a_resend_placed_at_the_tail() {
    caught(ClientMutant::ResendAtTail, ONCE);
}

#[test]
fn refused_catches_a_timeout_taken_as_a_refusal() {
    caught(ClientMutant::LostAsFenced, REFUSED);
}

#[test]
fn written_catches_a_resolve_that_always_says_written() {
    caught(ClientMutant::ResolveWritten, WRITTEN);
}

#[test]
fn led_catches_a_forged_refusal() {
    caught(ClientMutant::ForgedRefusal, LED);
}

#[test]
fn page_catches_a_late_read() {
    caught(ClientMutant::PageLate, PAGE);
}

#[test]
fn tail_catches_a_short_view() {
    caught(ClientMutant::ShortView, TAIL);
}

#[test]
fn floor_catches_a_floor_asked_of_the_oldest_segment() {
    caught(ClientMutant::FloorFromOldest, FLOOR);
}
