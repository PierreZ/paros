use super::Config;
use crate::journal::state::WriterMode;
use crate::journal::{JournalId, JournalIdentifier, TenantId};
use crate::membership::{FailureDomain, QuorumSystem};
use crate::types::NodeId;

fn nodes(ids: impl IntoIterator<Item = u64>) -> Vec<NodeId> {
    ids.into_iter().map(NodeId).collect()
}

fn three_peers() -> Config {
    let mut config = Config::new(NodeId(1), JournalIdentifier::UNSET);
    config.peers = nodes([3, 1, 2]);
    config
}

/// A plain bootstrap is the peers under the quorum system, sorted, with
/// the cell and the role counts of the configuration. It has no labels.
#[test]
fn a_plain_bootstrap_carries_the_peers_cell_and_counts() {
    let mut config = three_peers();
    config.quorum_system = QuorumSystem::Flexible { q1: 2, q2: 2 };
    config.cell_id = 9;
    config.proxy_count = 2;
    config.replica_count = 3;
    let bootstrap = config.bootstrap();
    assert_eq!(bootstrap.members(), nodes([1, 2, 3]));
    assert_eq!(
        bootstrap.quorum_system(),
        QuorumSystem::Flexible { q1: 2, q2: 2 }
    );
    assert_eq!(bootstrap.cell(), 9);
    assert_eq!(bootstrap.proxy_count(), 2);
    assert_eq!(bootstrap.replica_count(), 3);
    assert!(!bootstrap.is_zoned());
    assert_eq!(bootstrap.domain_of(NodeId(1)), None);
}

/// A labeled bootstrap gives each peer the label at its own position in
/// `peer_domains`. The sort of the members does not move a label to
/// another peer.
#[test]
fn a_labeled_bootstrap_keeps_each_peer_with_its_label() {
    let mut config = three_peers();
    config.peer_domains = vec![
        FailureDomain::new("", "c"),
        FailureDomain::new("", "a"),
        FailureDomain::new("", "b"),
    ];
    config.cell_id = 4;
    config.proxy_count = 1;
    let bootstrap = config.bootstrap();
    assert!(bootstrap.is_zoned());
    assert_eq!(bootstrap.members(), nodes([1, 2, 3]));
    assert_eq!(
        bootstrap.domain_of(NodeId(3)),
        Some(&FailureDomain::new("", "c"))
    );
    assert_eq!(
        bootstrap.domain_of(NodeId(1)),
        Some(&FailureDomain::new("", "a"))
    );
    assert_eq!(
        bootstrap.domain_of(NodeId(2)),
        Some(&FailureDomain::new("", "b"))
    );
    assert_eq!(bootstrap.cell(), 4);
    assert_eq!(bootstrap.proxy_count(), 1);
    assert_eq!(bootstrap.replica_count(), 0);
}

/// A configuration labels all its bootstrap peers or none of them. A label
/// list of a different length is refused.
#[test]
#[should_panic(expected = "a configuration labels every bootstrap member or none")]
fn a_bootstrap_with_a_missing_label_is_refused() {
    let mut config = three_peers();
    config.peer_domains = vec![FailureDomain::new("", "a")];
    let _ = config.bootstrap();
}

/// The format marker records no role count. All other fields stay the
/// same, so that a change of a count does not refuse a restart.
#[test]
fn the_recorded_configuration_drops_only_the_counts() {
    let mut config = Config::new(NodeId(2), JournalIdentifier::new(TenantId(5), JournalId(6)));
    config.peers = nodes([1, 2, 3]);
    config.nodes = nodes([1, 2, 3, 4]);
    config.writer_mode = WriterMode::Multi;
    config.cell_id = 7;
    config.proxy_count = 3;
    config.replica_count = 2;

    let recorded = config.recorded();
    assert_eq!(recorded.proxy_count, 0);
    assert_eq!(recorded.replica_count, 0);
    assert!(!recorded.bootstrap().has_proxies());
    assert_eq!(
        recorded,
        Config {
            proxy_count: 0,
            replica_count: 0,
            ..config.clone()
        }
    );
    // Two configurations that differ only in their counts record the same.
    let mut other = config.clone();
    other.proxy_count = 1;
    other.replica_count = 0;
    assert_eq!(other.recorded(), recorded);
}
