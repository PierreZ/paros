//! The net layer: what every role shares to talk. Addresses, the transport
//! and its sessions, the RPC edge and its runtime configuration, and the
//! peer lanes.

pub mod address;
pub mod clients;
pub mod inbound;
pub mod runtime;
#[cfg(feature = "tls")]
pub mod tls;
pub mod transport;

pub use clients::{MatchmakerClient, NodeClient, Pass, in_group, published, well_known};
pub use inbound::{
    EdgeRejection, Inbound, OnReject, ReplySender, rpc_config, serve_deliveries, serve_well_known,
};
