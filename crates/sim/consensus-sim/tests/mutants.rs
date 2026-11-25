//! The controls: each oracle finds the bug it exists to catch. A mutant
//! plants one bug in the driver of the walk (never in the core), and the
//! campaign must fail with that oracle's message.

use consensus_sim::campaign::try_campaign;
use consensus_sim::cluster::{Cluster, Flavor, Mutant};
use consensus_sim::oracle::journal::JOURNAL_FOLD;
use consensus_sim::oracle::ledger::{
    COMMIT_BACKED, LEARNED_CHOSEN, ONE_PROPOSAL, ONE_VALUE, PERSIST_BEFORE_SEND, PREFIX_SHRINKS,
};
use consensus_sim::oracle::reads::READ_SEES_WRITES;
use moonpool_walk::Walk;

/// Run `flavor` with `mutant` over at most `seeds` seeds, and expect a
/// failure that names `oracle`.
fn caught(flavor: Flavor, mutant: Mutant, seeds: u64, oracle: &str) {
    let flavor = flavor.with(mutant);
    let walk = Walk::<Cluster>::new()
        .seeds(seeds)
        .env_prefix(None)
        .model(move |cx| Cluster::build(flavor.clone(), cx));
    match try_campaign(&walk, &[]) {
        Ok(coverage) => panic!(
            "{mutant:?} survived {seeds} seeds; {oracle} did not fire\n{}",
            coverage.report
        ),
        Err(failure) => assert!(
            failure.contains(oracle),
            "{mutant:?} was caught, but not by `{oracle}`:\n{failure}"
        ),
    }
}

#[test]
fn skipping_the_persist_is_caught_by_persist_before_send() {
    caught(
        Flavor::multi_paxos(),
        Mutant::SkipPersist,
        5,
        PERSIST_BEFORE_SEND,
    );
}

#[test]
fn forgetting_votes_is_caught_by_one_value_per_slot() {
    caught(Flavor::multi_paxos(), Mutant::ForgetVotes, 400, ONE_VALUE);
}

#[test]
fn a_lost_chosen_index_is_caught_by_the_prefix_oracle() {
    caught(
        Flavor::multi_paxos(),
        Mutant::LoseChosenIndex,
        50,
        PREFIX_SHRINKS,
    );
}

#[test]
fn an_early_commit_is_caught_by_the_commit_oracle() {
    caught(Flavor::multi_paxos(), Mutant::EarlyCommit, 5, COMMIT_BACKED);
}

#[test]
fn a_corrupt_commit_is_caught_by_the_learned_oracle() {
    caught(
        Flavor::replicas(),
        Mutant::CorruptCommit,
        20,
        LEARNED_CHOSEN,
    );
}

#[test]
fn a_corrupt_trim_jump_is_caught_by_the_journal_rules() {
    caught(
        Flavor::multi_paxos_five(),
        Mutant::CorruptTrimJump,
        400,
        JOURNAL_FOLD,
    );
}

#[test]
fn a_corrupt_segment_in_a_trim_jump_is_caught_by_the_journal_rules() {
    caught(
        Flavor::segments(),
        Mutant::CorruptTrimSegment,
        400,
        JOURNAL_FOLD,
    );
}

#[test]
fn a_local_read_is_caught_by_the_read_oracle() {
    caught(Flavor::replicas(), Mutant::LocalRead, 100, READ_SEES_WRITES);
}

#[test]
fn no_resend_is_caught_by_the_liveness_judge() {
    caught(
        Flavor::multi_paxos(),
        Mutant::NoResend,
        100,
        "the model did not converge",
    );
}

#[test]
fn a_decree_acceptor_that_forgets_its_promise_is_caught_by_one_value() {
    use consensus_sim::decree::{DecreeModel, DecreeMutant};
    use paros_consensus::decree::Phase1Rule;
    let walk = Walk::<DecreeModel>::new()
        .seeds(3_000)
        .crash_weights(12, 12)
        .env_prefix(None)
        .model(|_| DecreeModel::build(Phase1Rule::Quorum, DecreeMutant::ForgetPromise));
    let failure = try_campaign(&walk, &[]).expect_err("the mutant is caught");
    assert!(failure.contains(ONE_VALUE), "{failure}");
}

#[test]
fn a_site_longer_than_the_table_keeps_is_refused() {
    use consensus_sim::batcher::BatchModel;
    let walk = Walk::<BatchModel>::new().seeds(1).env_prefix(None);
    let long = "batcher walk: a site name that runs on past the sixty-three bytes of a slot";
    let failure = try_campaign(&walk, &[long]).expect_err("a long site is refused");
    assert!(failure.contains("longer than"), "{failure}");
}

#[test]
fn a_rewound_handoff_is_caught_by_one_proposal_per_ballot() {
    caught(Flavor::handoff(), Mutant::RewindHandoff, 400, ONE_PROPOSAL);
}
