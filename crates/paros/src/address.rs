//! Machine addresses (#257): what a machine advertises and what its peers
//! dial.
//!
//! A machine binds its listen address and **advertises** another one
//! (`docs/architecture.md` §3.2), as `FoundationDB`'s `public_address` and
//! `CockroachDB`'s `--advertise-addr`. An advertised [`Address`] is
//! `HOST:PORT`, where the host is a literal IP or a name (a Compose service,
//! a DNS record). It is kept as written in the cell plan, an admission and
//! the registry, and it is resolved **at dial time** through [`Names`]: a
//! machine whose IP changes behind a stable name heals with no write. A
//! dialer that fails resolves the name again on its next attempt.
//!
//! [`Names`] is the one resolver every dialer of a machine shares: the
//! operating system's lookup in `parosd` and `parosctl`, a table the
//! simulation edits in a run (moonpool's `ScriptedResolver`). A literal
//! address never reaches the resolver.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use moonpool_core::Resolver;

/// A machine's address, `HOST:PORT`: a literal socket address or a name
/// and a port. Literal addresses are kept in their canonical form, names
/// in lower case, so two spellings of one address compare equal.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address(String);

impl Address {
    /// `text` as an address.
    ///
    /// # Errors
    ///
    /// What is malformed: no port, an empty host, a port that is not a
    /// number, or an IPv6 host without brackets.
    pub fn parse(text: &str) -> Result<Self, String> {
        if let Ok(literal) = text.parse::<SocketAddr>() {
            return Ok(Self(literal.to_string()));
        }
        let (host, port) = text
            .rsplit_once(':')
            .ok_or_else(|| format!("expected HOST:PORT, got {text:?} (the port is required)"))?;
        if host.is_empty() || host.contains(':') || host.contains(char::is_whitespace) {
            return Err(format!("expected HOST:PORT, got {text:?}"));
        }
        port.parse::<u16>()
            .map_err(|e| format!("bad port in {text:?}: {e}"))?;
        Ok(Self(format!("{}:{port}", host.to_ascii_lowercase())))
    }

    /// The literal socket address, when the host is an IP.
    #[must_use]
    pub fn literal(&self) -> Option<SocketAddr> {
        self.0.parse().ok()
    }

    /// The address as text, `HOST:PORT`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Self {
        Self(addr.to_string())
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl core::str::FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::parse(text)
    }
}

type Lookup = dyn Fn(String) -> Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send>>
    + Send
    + Sync;

/// The resolver every dialer of a machine shares (a name to its socket
/// addresses), erased so the drivers stay generic over the providers alone.
/// Clones share it.
#[derive(Clone)]
pub struct Names {
    lookup: Option<Arc<Lookup>>,
}

impl Names {
    /// Names over `resolver`.
    #[must_use]
    pub fn new<R: Resolver>(resolver: R) -> Self {
        let lookup: Arc<Lookup> = Arc::new(move |target: String| {
            let resolver = resolver.clone();
            Box::pin(async move { resolver.resolve(&target).await })
        });
        Self {
            lookup: Some(lookup),
        }
    }

    /// No resolver: literal addresses only, and every name fails to
    /// resolve. A deployment whose addresses are all literal uses it.
    #[must_use]
    pub fn literal() -> Self {
        Self { lookup: None }
    }

    /// Resolve `address` to one socket address: a literal as it is, a name
    /// to its first IPv4 address, or its first address when it has no IPv4
    /// one.
    ///
    /// # Errors
    ///
    /// The name does not resolve (yet), or there is no resolver.
    pub async fn resolve(&self, address: &Address) -> io::Result<SocketAddr> {
        if let Some(literal) = address.literal() {
            return Ok(literal);
        }
        let Some(lookup) = &self.lookup else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no resolver for {address}"),
            ));
        };
        let found = lookup(address.as_str().to_string()).await?;
        found
            .iter()
            .find(|a| a.is_ipv4())
            .or_else(|| found.first())
            .copied()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{address} resolves to no address"),
                )
            })
    }
}

impl fmt::Debug for Names {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Names")
            .field("resolver", &self.lookup.is_some())
            .finish()
    }
}

/// Two `Names` are equal when they are the same resolver (or both have none).
impl PartialEq for Names {
    fn eq(&self, other: &Self) -> bool {
        match (&self.lookup, &other.lookup) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for Names {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_are_canonical_and_names_keep_their_port() {
        assert_eq!(
            Address::parse("10.0.0.1:4500").map(|a| a.literal()),
            Ok(Some("10.0.0.1:4500".parse().expect("literal")))
        );
        assert_eq!(
            Address::parse("[::1]:4500").map(|a| a.to_string()),
            Ok("[::1]:4500".into())
        );
        assert_eq!(
            Address::parse("Node1:4500").map(|a| a.to_string()),
            Ok("node1:4500".into())
        );
        assert_eq!(Address::parse("node1:4500").map(|a| a.literal()), Ok(None));
        assert!(Address::parse("node1").is_err(), "no port");
        assert!(Address::parse(":4500").is_err(), "no host");
        assert!(Address::parse("node1:http").is_err(), "a named port");
        assert!(Address::parse("::1:4500").is_err(), "IPv6 needs brackets");
    }

    #[test]
    fn literal_names_resolve_literals_only() {
        let names = Names::literal();
        let literal = Address::parse("10.0.0.1:4500").expect("an address");
        let resolved = futures::executor::block_on(names.resolve(&literal));
        assert_eq!(resolved.ok(), literal.literal());
        let name = Address::parse("node1:4500").expect("an address");
        assert!(futures::executor::block_on(names.resolve(&name)).is_err());
        assert_eq!(names, Names::literal());
    }
}
