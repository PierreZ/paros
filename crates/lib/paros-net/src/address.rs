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
pub fn parse_addr(ip: &str) -> SimulationResult<String> {
    let addr_str = if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4500")
    };
    addr_str
        .parse::<SocketAddr>()
        .map(|addr| addr.to_string())
        .map_err(|e| SimulationError::InvalidState(format!("bad addr: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bare IP takes port 4500; an address with a port keeps it; text that
    /// is no socket address is refused.
    #[test]
    fn a_bare_ip_takes_the_sim_port() {
        assert_eq!(parse_addr("10.0.0.1").ok(), Some("10.0.0.1:4500".into()));
        assert_eq!(parse_addr("10.0.0.1:7").ok(), Some("10.0.0.1:7".into()));
        assert_eq!(parse_addr("[::1]:9").ok(), Some("[::1]:9".into()));
        assert!(parse_addr("node-0").is_err(), "a name is not a literal");
        assert!(parse_addr("::1").is_err(), "an IPv6 literal needs its port");
    }
}
