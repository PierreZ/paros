//! TLS for paros (feature `tls`): the certificate layout every process of a
//! universe shares, and the [`Transport`] a process builds from its
//! certificate files.
//!
//! **One authority, one layout.** A universe has one certificate authority.
//! Every leaf it signs names its holder with a SPIFFE-style identity,
//! `spiffe://<universe>/<role>/<name>` ([`identity`]), as its first name.
//! A leaf that serves (a machine, a frontend) also proves
//! [`SERVER_NAME`], the one DNS name every client expects: machines are
//! dialed by address, and an address changes (#257), so a client checks
//! the authority and the shared name, never the address.
//!
//! **Roles** ([`Role`]): a machine and a frontend accept and dial
//! ([`Usage::Both`]); an admin and a client only dial ([`Usage::Client`]).
//! The role is in the identity, so a later policy can tell an operator
//! from a tenant's client (#192 (the frontend)). This module authorizes
//! nothing: every endpoint stays public, and mutual TLS decides only who
//! may open a session ([`ClientCertificates`]).
//!
//! **The same code in the simulation.** `parosctl tls` issues the files
//! with [`Authority`] and [`leaf`]; the simulation issues its certificates
//! with the same functions from seeded keys ([`KeySeed::Bytes`]), so a
//! certificate fault site ([`Flaw`]) is one the shipped issuer has too.

use std::str::FromStr;
use std::time::Duration;

use moonpool_core::TimeProvider;
pub use moonpool_rpc::security::tls::pki::{
    Authority, Flaw, Issued, KeySeed, LeafSpec, SubjectName, Usage, Validity, read_certificates,
    read_key,
};
pub use moonpool_rpc::security::tls::rustls;
pub use moonpool_rpc::security::tls::{
    CertificateDer, CertificateNames, ClientAuth, PrivateKeyDer, TlsError,
};
use moonpool_rpc::security::tls::{ServerNames, Tls, TlsClient, TlsServer};
pub use moonpool_rpc::security::{FixedUtc, UtcClock, UtcTime};
// The host's wall clock: moonpool has none on `wasm32-unknown-unknown`, where
// only a simulated process (with its `ProviderUtc`) runs (#307 (paros-sim in
// wasm)).
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use moonpool_rpc::security::SystemUtc;

use crate::transport::Transport;

/// The DNS name every serving leaf proves, and every client expects.
pub const SERVER_NAME: &str = "paros";

/// The identity scheme: a SPIFFE ID.
const SCHEME: &str = "spiffe://";

/// What a certificate's holder is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// A `parosd` machine: it serves the cell and dials its peers.
    Machine,
    /// A `paros-frontend`: it serves clients and dials the machines.
    Frontend,
    /// An operator's `parosctl`: it dials only.
    Admin,
    /// An application's client: it dials only.
    Client,
}

impl Role {
    /// Every role, in display order.
    pub const ALL: [Role; 4] = [Role::Machine, Role::Frontend, Role::Admin, Role::Client];

    /// What a leaf for this role is for.
    #[must_use]
    pub fn usage(self) -> Usage {
        match self {
            Self::Machine | Self::Frontend => Usage::Both,
            Self::Admin | Self::Client => Usage::Client,
        }
    }

    /// The role's word in an identity.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Machine => "machine",
            Self::Frontend => "frontend",
            Self::Admin => "admin",
            Self::Client => "client",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Role {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|role| role.as_str() == text)
            .ok_or_else(|| format!("unknown role {text:?}: use machine, frontend, admin or client"))
    }
}

/// The identity of `name`, a `role` of `universe`:
/// `spiffe://<universe>/<role>/<name>`.
#[must_use]
pub fn identity(universe: &str, role: Role, name: &str) -> String {
    format!("{SCHEME}{universe}/{role}/{name}")
}

/// The parts of an identity [`identity`] made: `(universe, role, name)`.
#[must_use]
pub fn parse_identity(identity: &str) -> Option<(&str, Role, &str)> {
    let rest = identity.strip_prefix(SCHEME)?;
    let mut parts = rest.splitn(3, '/');
    let universe = parts.next().filter(|part| !part.is_empty())?;
    let role = parts.next()?.parse().ok()?;
    let name = parts.next().filter(|part| !part.is_empty())?;
    Some((universe, role, name))
}

/// What to issue for `name`, a `role` of `universe`: its identity first,
/// then [`SERVER_NAME`] when the role serves, then `extra` (the DNS names
/// and addresses an operator also dials it by).
#[must_use]
pub fn leaf(
    universe: &str,
    role: Role,
    name: &str,
    extra: &[SubjectName],
    validity: Validity,
    seed: KeySeed,
) -> LeafSpec {
    let usage = role.usage();
    let mut names = vec![SubjectName::Uri(identity(universe, role, name))];
    if usage.serves() {
        names.push(SubjectName::Dns(SERVER_NAME.to_string()));
    }
    names.extend(
        extra
            .iter()
            .filter(|extra| **extra != SubjectName::Dns(SERVER_NAME.to_string()))
            .cloned(),
    );
    LeafSpec {
        names,
        validity,
        usage,
        seed,
    }
}

/// Whether a serving process asks its callers for certificates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClientCertificates {
    /// Ask for none: server-authenticated TLS. Any caller that trusts the
    /// authority opens a session.
    Off,
    /// Verify a certificate when the caller presents one; admit a caller
    /// without one.
    Optional,
    /// Require a certificate the authority signed (mutual TLS): a caller
    /// without one is refused at the handshake. The default.
    #[default]
    Required,
}

impl ClientCertificates {
    /// Every mode.
    pub const ALL: [ClientCertificates; 3] = [Self::Off, Self::Optional, Self::Required];

    /// The mode's word in a setting.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Optional => "optional",
            Self::Required => "required",
        }
    }

    fn client_auth(self, roots: &[CertificateDer<'static>]) -> ClientAuth {
        match self {
            Self::Off => ClientAuth::Off,
            Self::Optional => ClientAuth::Optional(roots.to_vec()),
            Self::Required => ClientAuth::Required(roots.to_vec()),
        }
    }
}

impl std::fmt::Display for ClientCertificates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ClientCertificates {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|mode| mode.as_str() == text)
            .ok_or_else(|| format!("unknown mode {text:?}: use off, optional or required"))
    }
}

/// A process's certificate and the authority it trusts.
pub struct Material {
    /// Its chain, leaf first.
    pub chain: Vec<CertificateDer<'static>>,
    /// The chain's private key.
    pub key: PrivateKeyDer<'static>,
    /// The authorities it trusts: the universe's.
    pub roots: Vec<CertificateDer<'static>>,
}

impl std::fmt::Debug for Material {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Material")
            .field("chain", &self.chain.len())
            .field("roots", &self.roots.len())
            .finish_non_exhaustive()
    }
}

impl Clone for Material {
    fn clone(&self) -> Self {
        Self {
            chain: self.chain.clone(),
            key: self.key.clone_key(),
            roots: self.roots.clone(),
        }
    }
}

impl Material {
    /// Read the PEM files' contents: the chain, its key and the authority.
    ///
    /// # Errors
    ///
    /// [`TlsError`] when a file holds no certificate or no key.
    pub fn from_pem(chain: &[u8], key: &[u8], authority: &[u8]) -> Result<Self, TlsError> {
        let material = Self {
            chain: read_certificates(chain)?,
            key: read_key(key)?,
            roots: read_certificates(authority)?,
        };
        if material.chain.is_empty() {
            return Err(TlsError("the certificate file holds no certificate".into()));
        }
        if material.roots.is_empty() {
            return Err(TlsError("the authority file holds no certificate".into()));
        }
        Ok(material)
    }

    /// `issued`, trusting `authority`.
    #[must_use]
    pub fn issued(issued: &Issued, authority: &Authority) -> Self {
        Self {
            chain: issued.chain.clone(),
            key: issued.key(),
            roots: vec![authority.certificate().clone()],
        }
    }
}

/// The client side every paros process dials with: trust `roots`, expect
/// [`SERVER_NAME`], present `certificate` when one is given.
///
/// # Errors
///
/// [`TlsError`] without a usable root or with an unusable certificate.
pub fn client(
    roots: &[CertificateDer<'static>],
    certificate: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    clock: impl UtcClock,
) -> Result<TlsClient, TlsError> {
    let client = TlsClient::new(roots.iter().cloned(), clock)?
        .with_server_names(ServerNames::Fixed(SERVER_NAME.to_string()));
    match certificate {
        Some((chain, key)) => client.with_certificate(chain, key),
        None => Ok(client),
    }
}

/// The transport of a process that serves and dials (a machine, a
/// frontend): it serves `material`'s chain, asks callers for certificates
/// as `callers` says, and presents the same chain when it dials.
///
/// # Errors
///
/// [`TlsError`] when the chain, the key or the authority cannot be used.
pub fn serving(
    material: &Material,
    callers: ClientCertificates,
    clock: impl UtcClock + Clone,
) -> Result<Transport, TlsError> {
    let server = TlsServer::new(
        material.chain.clone(),
        material.key.clone_key(),
        clock.clone(),
    )?
    .with_client_auth(callers.client_auth(&material.roots))?;
    let client = client(
        &material.roots,
        Some((material.chain.clone(), material.key.clone_key())),
        clock,
    )?;
    Ok(Transport::Tls(Tls::new(client, server)))
}

/// The transport of a process that only dials (an operator, a client):
/// trust `roots`, present `certificate` when one is given.
///
/// # Errors
///
/// [`TlsError`] without a usable root or with an unusable certificate.
pub fn dialing(
    roots: &[CertificateDer<'static>],
    certificate: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    clock: impl UtcClock,
) -> Result<Transport, TlsError> {
    Ok(Transport::Tls(Tls::client(client(
        roots,
        certificate,
        clock,
    )?)))
}

/// A process's three PEM files, each with the name of the setting that
/// gave it, so an error names what to fix: what `parosd`,
/// `paros-frontend` and `parosctl` read.
///
/// A serving process takes all three files or none: with none, its
/// sessions are plaintext; with only some, the missing ones are an error,
/// so a typo never silently turns TLS off. A dialing process needs the
/// authority alone to verify the servers, and its certificate and key only
/// when the servers ask for one.
#[derive(Clone, Copy, Debug)]
pub struct Files<'a> {
    /// The certificate chain file, leaf first, and its setting's name.
    pub cert: (&'a str, Option<&'a std::path::Path>),
    /// The private key file, and its setting's name.
    pub key: (&'a str, Option<&'a std::path::Path>),
    /// The authority's certificate file, and its setting's name.
    pub ca: (&'a str, Option<&'a std::path::Path>),
}

fn read_file((setting, path): (&str, &std::path::Path)) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|error| format!("{setting} ({}): {error}", path.display()))
}

impl Files<'_> {
    /// The material, or `None` when no file is set.
    ///
    /// # Errors
    ///
    /// A file set without the other two, a file that cannot be read, or one
    /// that holds no certificate or no key.
    pub fn material(&self) -> Result<Option<Material>, String> {
        match (self.cert, self.key, self.ca) {
            ((_, None), (_, None), (_, None)) => Ok(None),
            ((cert_name, Some(cert)), (key_name, Some(key)), (ca_name, Some(ca))) => {
                Material::from_pem(
                    &read_file((cert_name, cert))?,
                    &read_file((key_name, key))?,
                    &read_file((ca_name, ca))?,
                )
                .map(Some)
                .map_err(|error| format!("{cert_name}: {error}"))
            }
            _ => {
                let missing: Vec<&str> = [self.cert, self.key, self.ca]
                    .into_iter()
                    .filter_map(|(name, path)| path.is_none().then_some(name))
                    .collect();
                Err(format!(
                    "TLS needs {}, {} and {} together: {} missing",
                    self.cert.0,
                    self.key.0,
                    self.ca.0,
                    missing.join(" and ")
                ))
            }
        }
    }

    /// The transport of a process that serves and dials: TLS with the
    /// files, asking callers for certificates as `callers` says, validated
    /// at the host's time; plaintext with no file.
    ///
    /// # Errors
    ///
    /// As [`Files::material`], or material TLS cannot use.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub fn serving(&self, callers: ClientCertificates) -> Result<Transport, String> {
        match self.material()? {
            None => Ok(Transport::Plaintext),
            Some(material) => serving(&material, callers, SystemUtc)
                .map_err(|error| format!("{}: {error}", self.cert.0)),
        }
    }

    /// The transport of a process that only dials: TLS once the authority
    /// is set, presenting the certificate when it and its key are set,
    /// validated at the host's time; plaintext with no file.
    ///
    /// # Errors
    ///
    /// A certificate without its key or the reverse, a certificate without
    /// the authority, a file that cannot be read, or material TLS cannot
    /// use.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub fn dialing(&self) -> Result<Transport, String> {
        let (ca_name, ca) = self.ca;
        let Some(ca) = ca else {
            return match (self.cert.1, self.key.1) {
                (None, None) => Ok(Transport::Plaintext),
                _ => Err(format!(
                    "{ca_name} is not set: a certificate needs the authority to verify the servers"
                )),
            };
        };
        let roots = read_certificates(&read_file((ca_name, ca))?)
            .map_err(|error| format!("{ca_name}: {error}"))?;
        if roots.is_empty() {
            return Err(format!("{ca_name}: no certificate"));
        }
        let certificate = match (self.cert, self.key) {
            ((_, None), (_, None)) => None,
            ((cert_name, Some(cert)), (key_name, Some(key))) => Some((
                read_certificates(&read_file((cert_name, cert))?)
                    .map_err(|error| format!("{cert_name}: {error}"))?,
                read_key(&read_file((key_name, key))?)
                    .map_err(|error| format!("{key_name}: {error}"))?,
            )),
            ((cert_name, _), (key_name, _)) => {
                return Err(format!("{cert_name} and {key_name} go together"));
            }
        };
        dialing(&roots, certificate, SystemUtc).map_err(|error| format!("{}: {error}", self.cert.0))
    }
}

/// A UTC clock read from a provider's time: `epoch` (the wall clock at the
/// provider's zero) plus [`TimeProvider::now`]. What a simulated process
/// validates certificates with, as the frontend checks tokens (#192 (the
/// frontend)); production uses [`SystemUtc`].
#[derive(Clone, Debug)]
pub struct ProviderUtc<T> {
    epoch: Duration,
    time: T,
}

impl<T: TimeProvider> ProviderUtc<T> {
    /// `epoch` plus `time`'s reading.
    #[must_use]
    pub fn new(epoch: Duration, time: T) -> Self {
        Self { epoch, time }
    }
}

impl<T: TimeProvider> UtcClock for ProviderUtc<T> {
    fn now_utc(&self) -> Option<UtcTime> {
        Some(UtcTime::from_unix_seconds(
            self.epoch.saturating_add(self.time.now()).as_secs(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_906_502_400;

    fn year() -> Validity {
        Validity::starting(UtcTime::from_unix_seconds(NOW - 60), 365 * 86_400)
    }

    #[test]
    fn identities_round_trip() {
        for role in Role::ALL {
            let id = identity("prod", role, "machine-1");
            assert_eq!(parse_identity(&id), Some(("prod", role, "machine-1")));
            assert_eq!(role.as_str().parse::<Role>(), Ok(role));
        }
        assert_eq!(parse_identity("paros://tenant/journal"), None);
        assert_eq!(parse_identity("spiffe://prod/owner/x"), None);
        assert_eq!(parse_identity("spiffe://prod/admin/"), None);
        for mode in ClientCertificates::ALL {
            assert_eq!(mode.as_str().parse::<ClientCertificates>(), Ok(mode));
        }
        assert!("both".parse::<ClientCertificates>().is_err());
    }

    #[test]
    fn a_serving_leaf_proves_the_shared_name() {
        let extra = [
            SubjectName::Dns("parosd-1".into()),
            SubjectName::Dns(SERVER_NAME.into()),
        ];
        let spec = leaf(
            "prod",
            Role::Machine,
            "parosd-1",
            &extra,
            year(),
            KeySeed::Random,
        );
        assert_eq!(spec.usage, Usage::Both);
        assert_eq!(
            spec.names,
            vec![
                SubjectName::Uri("spiffe://prod/machine/parosd-1".into()),
                SubjectName::Dns(SERVER_NAME.into()),
                SubjectName::Dns("parosd-1".into()),
            ]
        );
        let admin = leaf("prod", Role::Admin, "ops", &[], year(), KeySeed::Random);
        assert_eq!(admin.usage, Usage::Client);
        assert_eq!(
            admin.names,
            vec![SubjectName::Uri("spiffe://prod/admin/ops".into())]
        );
    }

    #[test]
    fn issued_files_build_both_transports() {
        let authority =
            Authority::new("prod authority", year(), KeySeed::Bytes([7; 32])).expect("authority");
        let issued = authority
            .issue(&leaf(
                "prod",
                Role::Machine,
                "m",
                &[],
                year(),
                KeySeed::Bytes([8; 32]),
            ))
            .expect("leaf");
        let material = Material::from_pem(
            issued.chain_pem().as_bytes(),
            issued.key_pem().as_bytes(),
            authority.certificate_pem().as_bytes(),
        )
        .expect("pem");
        assert_eq!(material.chain.len(), 2);
        let clock = FixedUtc::new(UtcTime::from_unix_seconds(NOW));
        for mode in ClientCertificates::ALL {
            assert!(serving(&material, mode, clock.clone()).is_ok_and(|t| t.is_encrypted()));
        }
        assert!(dialing(&material.roots, None, clock).is_ok());
        assert!(Material::from_pem(b"", b"", b"").is_err());
    }

    #[test]
    fn files_go_together() {
        let none = Files {
            cert: ("CERT", None),
            key: ("KEY", None),
            ca: ("CA", None),
        };
        assert!(
            none.serving(ClientCertificates::Required)
                .is_ok_and(|t| !t.is_encrypted())
        );
        assert!(none.dialing().is_ok_and(|t| !t.is_encrypted()));
        let path = std::path::Path::new("/nonexistent/paros.crt");
        let partial = Files {
            cert: ("CERT", Some(path)),
            ..none
        };
        let error = partial.material().expect_err("a partial set is refused");
        assert!(error.contains("KEY and CA missing"), "{error}");
        let error = partial
            .dialing()
            .expect_err("a certificate needs the authority");
        assert!(error.contains("CA is not set"), "{error}");
    }
}
