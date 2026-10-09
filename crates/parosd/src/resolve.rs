//! Hostname resolution (#209), shared by `parosd` and `parosctl`: an
//! address book entry is `HOST:PORT`, where the host is a literal IP or a
//! name (a Compose service), and the port is always explicit — the
//! simulator's default port 4500 is not a production convention.
//!
//! A name is resolved **once, at startup**, in the configuration layer,
//! never inside a driver: the drivers (and the simulation, which hands them
//! literal addresses) see socket addresses only. A peer whose address
//! changes later is reached again after a restart of the process that
//! resolved it.

use std::net::{SocketAddr, ToSocketAddrs};

/// Check that `addr` is `HOST:PORT` (a literal socket address, or a
/// non-empty host and a port), without resolving anything.
///
/// # Errors
///
/// A description of what is malformed.
pub fn check_shape(addr: &str) -> Result<(), String> {
    if addr.parse::<SocketAddr>().is_ok() {
        return Ok(());
    }
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| format!("expected HOST:PORT, got {addr:?} (the port is required)"))?;
    if host.is_empty() || host.contains(':') {
        return Err(format!("expected HOST:PORT, got {addr:?}"));
    }
    port.parse::<u16>()
        .map_err(|e| format!("bad port in {addr:?}: {e}"))?;
    Ok(())
}

/// Resolve `addr` (`HOST:PORT`) to one socket address: a literal as it is,
/// a name to its first IPv4 address, or its first address when it has no
/// IPv4 one.
///
/// # Errors
///
/// `addr` is malformed, or the name does not resolve (yet).
pub fn resolve(addr: &str) -> Result<SocketAddr, String> {
    check_shape(addr)?;
    if let Ok(literal) = addr.parse::<SocketAddr>() {
        return Ok(literal);
    }
    let resolved: Vec<SocketAddr> = addr
        .to_socket_addrs()
        .map_err(|e| format!("cannot resolve {addr:?}: {e}"))?
        .collect();
    resolved
        .iter()
        .find(|a| a.is_ipv4())
        .or_else(|| resolved.first())
        .copied()
        .ok_or_else(|| format!("{addr:?} resolves to no address"))
}

/// Resolve `addr` (`HOST:PORT`) to **every** socket address it names, in
/// resolution order without duplicates: a name that stands for several
/// machines (a Compose network alias, #216) yields them all. One family
/// only — the IPv4 addresses when there is any, else the IPv6 ones — the
/// family [`resolve`] picks for a listen address: a name like `localhost`
/// that resolves to `127.0.0.1` and `::1` is one machine listening on one
/// of them, never two members.
///
/// # Errors
///
/// `addr` is malformed, or the name does not resolve (yet).
#[allow(
    dead_code,
    reason = "parosctl's (its servers and founding members); parosd resolves one listen address"
)]
pub fn resolve_all(addr: &str) -> Result<Vec<SocketAddr>, String> {
    check_shape(addr)?;
    if let Ok(literal) = addr.parse::<SocketAddr>() {
        return Ok(vec![literal]);
    }
    let mut resolved: Vec<SocketAddr> = Vec::new();
    for found in addr
        .to_socket_addrs()
        .map_err(|e| format!("cannot resolve {addr:?}: {e}"))?
    {
        if !resolved.contains(&found) {
            resolved.push(found);
        }
    }
    if resolved.iter().any(SocketAddr::is_ipv4) {
        resolved.retain(SocketAddr::is_ipv4);
    }
    if resolved.is_empty() {
        return Err(format!("{addr:?} resolves to no address"));
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_and_names_resolve_and_a_port_is_required() {
        assert_eq!(
            resolve("127.0.0.1:4500"),
            Ok("127.0.0.1:4500".parse().expect("literal"))
        );
        assert_eq!(
            resolve("[::1]:4500"),
            Ok("[::1]:4500".parse().expect("literal"))
        );
        let local = resolve("localhost:4501").expect("localhost resolves");
        assert!(local.ip().is_loopback(), "{local}");
        assert_eq!(local.port(), 4501);
        assert!(check_shape("node-0:4500").is_ok());
        assert!(check_shape("node-0").is_err(), "no port");
        assert!(check_shape(":4500").is_err(), "no host");
        assert!(check_shape("node-0:http").is_err(), "a named port");
        assert!(resolve("no-such-host.invalid:4500").is_err());
        assert_eq!(
            resolve_all("127.0.0.1:4500"),
            Ok(vec!["127.0.0.1:4500".parse().expect("literal")])
        );
        // One machine, one address: never its IPv4 and its IPv6 address both.
        let local = resolve_all("localhost:4501").expect("localhost resolves");
        assert_eq!(
            local,
            vec![resolve("localhost:4501").expect("localhost resolves")]
        );
    }
}
