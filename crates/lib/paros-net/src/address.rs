//! The address parser the simulation dials with.

use std::net::SocketAddr;

use moonpool_core::{SimulationError, SimulationResult};

/// Parse an IP (which may lack a port) into a socket-address string, defaulting to
/// port 4500 (the moonpool sim convention). The simulation's parser: it takes
/// literal addresses only, and `parosd` resolves its hostnames itself, once,
/// at startup (#209), before any driver sees an address.
///
/// # Errors
///
/// Returns an error if `ip` is not a parseable network address.
///
/// # Panics
///
/// If an assertion on its own invariants, preconditions or postconditions
/// fails: a programmer error, never an operating condition.
pub fn parse_addr(ip: &str) -> SimulationResult<String> {
    let addr_str = if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4500")
    };
    assert!(addr_str.contains(':'), "an address always names its port");
    assert!(addr_str.contains(':'), "an address always names its port");
    addr_str
        .parse::<SocketAddr>()
        .map(|addr| addr.to_string())
        .map_err(|e| SimulationError::InvalidState(format!("bad addr: {e}")))
}
