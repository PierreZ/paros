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

/// Take out of moonpool's shard gate every shard that lists `target`, so
/// a scenario's wipe of it goes past what those shards declare they keep
/// (#760 (moonpool owns fault placement)): the gate would turn the wipe
/// into a crash. Every other shard stays in the gate, so a matchmaker set
/// the target is not in keeps a majority of clean registries. Witness 3
/// (CI's sim-wasm seeds on the #760 slice 2 branch): with every shard
/// off, crashes tore a second registry of a set that a wipe had already
/// cost one member.
pub(crate) fn switch_off_shards_of(shards: &Shards, target: &str) {
    let Ok(target) = target.parse::<IpAddr>() else {
        return;
    };
    for name in shards.names() {
        if shards
            .get(&name)
            .is_some_and(|shard| lists(&shard.replicas(), target))
        {
            shards.switch_off(&name);
        }
    }
}

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
}
