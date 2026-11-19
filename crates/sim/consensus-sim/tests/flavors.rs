//! One walk per Paxos flavor of `paros-consensus`: the correct core passes
//! every seed, and each flavor reaches the cases it exists for.

use consensus_sim::campaign::campaign;
use consensus_sim::cell::{Cell, Flavor, reach};
use moonpool_walk::Walk;

/// Seeds per flavor: a seed takes a few milliseconds in a debug build.
const SEEDS: u64 = 60;

/// The sites every flavor of the cell reaches.
const COMMON: &[&str] = &[
    reach::CHOSEN,
    reach::NEW_LEADER,
    reach::CRASH_BEFORE_PERSIST,
    reach::CRASH_BEFORE_SEND,
    reach::REBOOT,
    reach::WRITE_APPLIED,
    reach::SET_LEADER,
    reach::WRITE_AFTER_GAP,
    reach::DUPLICATE,
    reach::FENCED,
];

/// Walk `flavor`; every site of [`COMMON`] and of `extra` must pass.
fn walk(flavor: Flavor, extra: &[&str]) {
    walk_seeds(flavor, extra, SEEDS);
}

/// [`walk`] over `seeds` seeds.
fn walk_seeds(flavor: Flavor, extra: &[&str], seeds: u64) {
    let walk = Walk::<Cell>::new()
        .seeds(seeds)
        .model(move |cx| Cell::build(flavor.clone(), cx));
    let expected: Vec<&str> = COMMON.iter().chain(extra).copied().collect();
    campaign(&walk, &expected);
}

#[test]
fn multi_paxos() {
    walk(Flavor::multi_paxos(), &[reach::LOST_UNSYNCED]);
}

#[test]
fn multi_paxos_on_five_with_truncation() {
    walk(
        Flavor::multi_paxos_five(),
        &[
            reach::COMPACTED,
            reach::TRUNCATE_MOVED,
            reach::RETRY,
            reach::LOST_UNSYNCED,
        ],
    );
}

#[test]
fn leader_handoff() {
    walk(Flavor::handoff(), &[reach::HANDOFF, reach::LOST_UNSYNCED]);
}

#[test]
fn grid_quorums() {
    walk(
        Flavor::grid(2, 2),
        &[reach::NAMED_COLUMN, reach::NODE_READ, reach::LOST_UNSYNCED],
    );
}

#[test]
fn grid_partitions() {
    walk(
        Flavor::partitions(),
        &[
            reach::NAMED_COLUMN,
            reach::MOVED_ROUND,
            reach::LOST_UNSYNCED,
        ],
    );
}

#[test]
fn proxy_leaders() {
    walk(
        Flavor::proxies(),
        &[
            reach::PROXY_COMMIT,
            reach::TAKE_BACK,
            reach::PROXY_CRASH,
            reach::HANDOFF,
            reach::LOST_UNSYNCED,
        ],
    );
}

#[test]
fn proxy_leaders_on_a_grid() {
    walk(
        Flavor::proxies_on_grid(),
        &[reach::PROXY_COMMIT, reach::TAKE_BACK, reach::NAMED_COLUMN],
    );
}

#[test]
fn replicas() {
    walk(
        Flavor::replicas(),
        &[
            reach::REPLICA_APPLIED,
            reach::REPLICA_READ,
            reach::COMPACTED,
            reach::TRUNCATE_MOVED,
            reach::LOST_UNSYNCED,
        ],
    );
}

#[test]
fn leaderless_reads() {
    walk(Flavor::reads(), &[reach::NODE_READ, reach::LOST_UNSYNCED]);
}

#[test]
fn compartments() {
    walk(
        Flavor::compartments(),
        &[
            reach::PROXY_COMMIT,
            reach::REPLICA_APPLIED,
            reach::REPLICA_READ,
            reach::NODE_READ,
            reach::HANDOFF,
            reach::COMPACTED,
            reach::TRUNCATE_MOVED,
            reach::LOST_UNSYNCED,
        ],
    );
}

#[test]
fn single_decree() {
    use consensus_sim::decree::{DecreeModel, DecreeMutant, reach as decree};
    use paros_consensus::decree::Phase1Rule;
    for rule in [Phase1Rule::Quorum, Phase1Rule::EveryAcceptor] {
        let walk = Walk::<DecreeModel>::new()
            .seeds(SEEDS * 2)
            .model(move |_| DecreeModel::build(rule, DecreeMutant::None));
        campaign(
            &walk,
            &[
                decree::CHOSEN,
                decree::ADOPTED,
                decree::PREEMPTED,
                decree::LATE_PROPOSER,
            ],
        );
    }
}

#[test]
fn batcher() {
    use consensus_sim::batcher::{BatchModel, reach as batch};
    let walk = Walk::<BatchModel>::new().seeds(SEEDS * 2);
    campaign(
        &walk,
        &[batch::MERGED, batch::SCATTERED, batch::ALONE, batch::TWICE],
    );
}

#[test]
fn matchmakers_and_reconfiguration() {
    walk(
        Flavor::matchmakers(),
        &[
            reach::RECONFIGURED,
            reach::CHOSEN_RECONFIGURED,
            reach::GC_ACKED,
        ],
    );
}

#[test]
fn reconfigured_compartments() {
    walk(
        Flavor::reconfigured_compartments(),
        &[
            reach::RECONFIGURED,
            reach::CHOSEN_RECONFIGURED,
            reach::PROXY_COMMIT,
            reach::REPLICA_APPLIED,
            reach::NODE_READ,
        ],
    );
}

#[test]
fn disk_faults() {
    walk(
        Flavor::disk_faults(),
        &[
            reach::ROT_CHOSEN,
            reach::SCRUBBED,
            reach::COMPACTED,
            reach::LOST_UNSYNCED,
        ],
    );
}

#[test]
fn disk_faults_on_three() {
    walk(
        Flavor::disk_faults_three(),
        &[
            reach::ROT_CHOSEN,
            reach::ROT_SPLIT_FLOOR,
            reach::SCRUBBED,
            reach::COMPACTED,
        ],
    );
}

#[test]
fn retirement_and_rejoin() {
    walk(
        Flavor::retirement(),
        &[
            reach::RECONFIGURED,
            reach::GC_ACKED,
            reach::RETIRED,
            reach::REBOOKED,
            reach::ADMITTED,
        ],
    );
}

#[test]
fn matchmaker_set_handover() {
    walk(
        Flavor::handover(),
        &[reach::RECONFIGURED, reach::GC_ACKED, reach::HANDED_OVER],
    );
}

/// Seeds of the lagging-reads walk. The walk is the control of #1228
/// (stale read by a lagging reader): with the reader gated on its belief
/// ballot (the code before #1228), seeds 67, 71 and 99 serve a stale read
/// and `READ_SEES_WRITES` fails.
const LAGGING_SEEDS: u64 = 120;

#[test]
fn reads_on_lagging_pools() {
    walk_seeds(
        Flavor::lagging_reads(),
        &[
            reach::RECONFIGURED,
            reach::ADMITTED,
            reach::NODE_READ,
            reach::LAGGING_READER_REFUSED,
        ],
        LAGGING_SEEDS,
    );
}
