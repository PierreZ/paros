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
