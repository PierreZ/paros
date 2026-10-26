//! The observe layer: what a run reports about itself. The [`audit::Audit`]
//! port the simulation's oracles implement, a machine's
//! [`metrics::Metrics`], and the named BUGGIFY locations
//! ([`scenario`]). It changes no behaviour.

pub mod audit;
pub mod events;
pub mod metrics;
pub mod reply;
pub mod scenario;
