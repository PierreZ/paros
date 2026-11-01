//! One module per drawn scenario: each one is a seed's fault plan, an
//! injector or a moment the run reaches, and the gates it must fire.

pub(crate) mod bare_outage;
pub(crate) mod cold_rot;
pub(crate) mod disk_full;
pub(crate) mod downed_founder;
pub(crate) mod gone_machine;
pub(crate) mod lagging_acceptor;
pub(crate) mod lagging_replica;
pub(crate) mod late_outage;
pub(crate) mod moved_founder;
pub(crate) mod replaced_founder;
pub(crate) mod silent_machine;
pub(crate) mod slow_machine;
pub(crate) mod split_floor;
pub(crate) mod thin_sets;
pub(crate) mod wiped_founder;
pub(crate) mod zone_outage;

use std::net::IpAddr;

use moonpool_sim::Shards;
use moonpool_sim::shard::Replica;

/// Take shards out of moonpool's shard gate before a scenario wipes the
/// founder at `target` past what its journals declare they keep (#760
/// (moonpool owns fault placement)): the gate would turn the wipe into a
/// crash. Every journal shard leaves the gate, as all shards did before
/// matchmaker sets were shards; a matchmaker set leaves it only when it
/// lists the target ([`leaves_gate`]).
pub(crate) fn switch_off_shards_of(shards: &Shards, target: &str) {
    let Ok(target) = target.parse::<IpAddr>() else {
        return;
    };
    for name in shards.names() {
        let replicas = shards
            .get(&name)
            .map(|shard| shard.replicas())
            .unwrap_or_default();
        if leaves_gate(&name, &replicas, target) {
            shards.switch_off(&name);
        }
    }
}

/// Whether the shard `name` over `replicas` leaves the gate for a wipe of
/// `target`. A matchmaker set the target is not in stays in it: the gate
/// is what keeps a majority of its registries clean, now that the
/// harness's matchmaker ledger is gone. Witness 3 (CI's sim-wasm seeds on
/// the #760 slice 2 branch): with every shard off, crashes tore a second
/// registry of a set that a wipe had already cost one member. A journal
/// leaves the gate whether it lists the target or not. Witness
/// 15259921327496077211 (a PR #834 sweep): with only the target's journals
/// off, the cell's election did not settle in the recovery tail.
fn leaves_gate(name: &str, replicas: &[Replica], target: IpAddr) -> bool {
    let set = name.starts_with(MATCHMAKER_SHARD_PREFIX);
    !set || lists(replicas, target)
}

/// The prefix of a matchmaker set's shard name
/// (`paros::storage::journal::matchmaker_shard`).
const MATCHMAKER_SHARD_PREFIX: &str = "matchmakers/";

/// Whether `replicas` names the process at `target`: an IP, or an
/// `ip:port` (the book holds them resolved).
fn lists(replicas: &[Replica], target: IpAddr) -> bool {
    replicas.iter().any(|replica| {
        let address = replica.address();
        address.parse::<IpAddr>().ok().or_else(|| {
            address
                .parse::<std::net::SocketAddr>()
                .ok()
                .map(|at| at.ip())
        }) == Some(target)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shard_lists_its_replicas_by_address_with_or_without_a_port() {
        let replicas = vec![Replica::new("10.0.3.1:4500"), Replica::new("10.0.3.2")];
        let ip = |s: &str| s.parse::<IpAddr>().expect("ip");
        assert!(lists(&replicas, ip("10.0.3.1")));
        assert!(lists(&replicas, ip("10.0.3.2")));
        assert!(!lists(&replicas, ip("10.0.3.3")));
        assert!(!lists(&[], ip("10.0.3.1")));
    }

    #[test]
    fn a_wipe_takes_every_journal_and_only_its_own_matchmaker_sets_out_of_the_gate() {
        let target = "10.0.3.1".parse::<IpAddr>().expect("ip");
        let with = vec![Replica::new("10.0.3.1:4500"), Replica::new("10.0.3.2:4500")];
        let without = vec![Replica::new("10.0.3.2:4500"), Replica::new("10.0.3.3:4500")];
        assert!(leaves_gate("journal/1/2", &with, target));
        assert!(leaves_gate("journal/1/2", &without, target));
        assert!(leaves_gate("matchmakers/7", &with, target));
        assert!(!leaves_gate("matchmakers/7", &without, target));
        assert_eq!(
            paros::storage::journal::matchmaker_shard(paros::core::journal::TenantId(7)),
            format!("{MATCHMAKER_SHARD_PREFIX}7")
        );
    }
}
