//! The differential check of `paros-consensus` (#1101 (one Belief role, one
//! copy of the retained log)): a change that says it changes no behaviour
//! leaves every batch of the core the same.
//!
//! The core is sans-IO, so the old code is the oracle. Each flavor walks a
//! fixed set of seeds, and the cluster folds every batch that a node, a
//! proxy leader or a replica hands its driver into one digest
//! ([`Cluster::ready_digest`]): the messages, the writes, the learned and
//! served entries, in order. `tests/differential.txt` holds the digests of
//! the code before the change. A pull request that changes behaviour on
//! purpose writes the file again (`DIFFERENTIAL_WRITE=1`) and names the
//! change; any other difference is a bug.

use std::fmt::Write as _;
use std::path::PathBuf;

use consensus_sim::cluster::{Cluster, Flavor};
use moonpool_walk::Walk;

/// Seeds per flavor: enough runs to reach each flavor's rare paths, few
/// enough for the debug `test` job.
const SEEDS: u64 = 12;

/// Every flavor of `tests/flavors.rs`, by name.
fn flavors() -> Vec<(&'static str, Flavor)> {
    vec![
        ("multi-paxos", Flavor::multi_paxos()),
        ("multi-paxos-five", Flavor::multi_paxos_five()),
        ("handoff", Flavor::handoff()),
        ("grid", Flavor::grid(2, 2)),
        ("partitions", Flavor::partitions()),
        ("proxies", Flavor::proxies()),
        ("proxies-on-grid", Flavor::proxies_on_grid()),
        ("replicas", Flavor::replicas()),
        ("reads", Flavor::reads()),
        ("compartments", Flavor::compartments()),
        ("matchmakers", Flavor::matchmakers()),
        (
            "reconfigured-compartments",
            Flavor::reconfigured_compartments(),
        ),
        ("reconfiguring", Flavor::reconfiguring()),
        ("disk-faults", Flavor::disk_faults()),
        ("disk-faults-3", Flavor::disk_faults_three()),
        ("retirement", Flavor::retirement()),
        ("lagging-reads", Flavor::lagging_reads()),
        ("compaction", Flavor::compaction()),
        ("compartments-compaction", Flavor::compartments_compacting()),
        ("retiring", Flavor::retiring()),
        ("handover", Flavor::handover()),
        ("segments", Flavor::segments()),
        ("segment-replicas", Flavor::segment_replicas()),
        ("segment-reseat", Flavor::segment_reseat()),
        ("healing", Flavor::healing()),
        ("compartments-paged", Flavor::compartments().paged()),
        ("reconfiguring-paged", Flavor::reconfiguring().paged()),
    ]
}

/// One line per flavor and seed: `<flavor> <seed> <digest>`.
fn digests() -> String {
    let mut out = String::new();
    for (name, flavor) in flavors() {
        let walk = Walk::<Cluster>::new().model(move |cx| Cluster::build(flavor.clone(), cx));
        for seed in 1..=SEEDS {
            let run = walk
                .replay(seed)
                .unwrap_or_else(|failure| panic!("{name} seed {seed} fails: {failure:?}"));
            writeln!(out, "{name} {seed} {:016x}", run.model.ready_digest())
                .expect("a String takes every write");
        }
    }
    out
}

fn recorded() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/differential.txt")
}

#[test]
fn every_batch_matches_the_recorded_core() {
    let now = digests();
    let path = recorded();
    if std::env::var_os("DIFFERENTIAL_WRITE").is_some() {
        std::fs::write(&path, &now).expect("the digests are written");
        return;
    }
    let before = std::fs::read_to_string(&path).expect("tests/differential.txt is committed");
    let changed: Vec<String> = before
        .lines()
        .zip(now.lines())
        .filter(|(old, new)| old != new)
        .map(|(old, new)| format!("was {old}, is {new}"))
        .collect();
    assert!(
        changed.is_empty() && before.lines().count() == now.lines().count(),
        "the core's batches changed on {} runs (a change of behaviour; write the file \
         again with DIFFERENTIAL_WRITE=1 only for a change the pull request names):\n{}",
        changed.len(),
        changed.join("\n")
    );
}
