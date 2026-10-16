//! The machine's configuration (#196): environment variables, validated at
//! startup. Every `parosd` is started the same way — there is no role and
//! no identity to pass, since `node_id` is minted at format (#225):
//!
//! | variable | meaning |
//! |---|---|
//! | `PAROS_LISTEN` | `HOST:PORT` this machine binds; may be a wildcard (`0.0.0.0:4500`) |
//! | `PAROS_ADVERTISE` | `HOST:PORT` its peers and clients dial; `PAROS_LISTEN` by default, required when that is a wildcard (#257) |
//! | `PAROS_DATA_DIR` | where its identity and its stores live |
//! | `PAROS_CLASS` | `storage` or `stateless` (fixed at format) |
//! | `PAROS_CAPACITY` | its capacity, in role slots (default 32): one slot holds one acceptor of one journal |
//! | `PAROS_FAILURE_DOMAIN` | its failure domain label (default empty) |
//! | `PAROS_STORE_LAYOUT` | `default` (64 MiB segments) or `small` (laptops, tests) |
//! | `PAROS_TLS_CERT` | its certificate chain, PEM (`parosctl tls`); with the next two, every session is TLS |
//! | `PAROS_TLS_KEY` | the certificate's private key, PEM |
//! | `PAROS_TLS_CA` | the universe's authority, PEM: the peers and callers it trusts |
//! | `PAROS_TLS_CLIENT_AUTH` | `required` (mutual TLS, the default), `optional` or `off`: what it asks of a caller |
//! | `PAROS_ROOT_PUBLIC_KEY` | the root public key files it pins while idle, comma-separated (#245 (Biscuit tokens)): `cell init` and `cell add-machine` then need an `admin` token; none trusts the network |
//! | `PAROS_HTTP_LISTEN` | `HOST:PORT` of its HTTP endpoint (#366 (metrics)): `/metrics` (Prometheus text), `/healthz` and `/readyz`; none by default |
//! | `PAROS_OTLP_ENDPOINT` | an OpenTelemetry collector's OTLP/gRPC URL (`http://collector:4317`): spans and metrics are pushed there; none by default |
//! | `PAROS_OTLP_TRACES_ENDPOINT`, `PAROS_OTLP_METRICS_ENDPOINT` | one signal's collector, in place of `PAROS_OTLP_ENDPOINT` |
//! | `PAROS_OTLP_INTERVAL_MS` | how often the metrics are pushed (default 10000, at least 100) |
//! | `PAROS_<FIELD>[_MS]` | one override per `DriverTunables` field ([`crate::tunables`]) |
//!
//! Each variable has the matching `--flag`. An unknown `PAROS_*` variable
//! is an error, so a typo never silently keeps a default.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use paros::machine::{AdminPin, Class};
use paros::tls::{ClientCertificates, Files};
use paros::{JournalStoreConfig, Transport};
use paros_authz_biscuit::{BiscuitAuthz, KeyRing, RootPublicKey};

/// The variables this module reads.
const VARIABLES: [&str; 18] = [
    "PAROS_HTTP_LISTEN",
    "PAROS_OTLP_ENDPOINT",
    "PAROS_OTLP_TRACES_ENDPOINT",
    "PAROS_OTLP_METRICS_ENDPOINT",
    "PAROS_OTLP_INTERVAL_MS",
    "PAROS_NAME",
    "PAROS_LISTEN",
    "PAROS_ADVERTISE",
    "PAROS_DATA_DIR",
    "PAROS_CLASS",
    "PAROS_CAPACITY",
    "PAROS_FAILURE_DOMAIN",
    "PAROS_STORE_LAYOUT",
    "PAROS_TLS_CERT",
    "PAROS_TLS_KEY",
    "PAROS_TLS_CA",
    "PAROS_TLS_CLIENT_AUTH",
    "PAROS_ROOT_PUBLIC_KEY",
];

/// The store layout a machine runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Layout {
    /// The CLSTORE layout: 64 MiB segments.
    Default,
    /// 256 KiB segments and frequent checkpoints, for tests and laptops.
    Small,
}

impl Layout {
    /// The store configuration.
    #[must_use]
    pub fn config(self) -> JournalStoreConfig {
        match self {
            Layout::Default => JournalStoreConfig::default(),
            Layout::Small => JournalStoreConfig::small(),
        }
    }
}

/// The paros daemon: one uniform binary any machine runs (#196).
#[derive(Parser, Debug)]
#[command(name = "parosd", version, about)]
pub struct Settings {
    /// `HOST:PORT` this machine binds, resolved once at start; may be a
    /// wildcard (`0.0.0.0:4500`).
    #[arg(long, env = "PAROS_LISTEN")]
    pub listen: String,
    /// `HOST:PORT` its peers and clients dial (#257): a literal or a name,
    /// kept as a name and resolved by whoever dials it. `PAROS_LISTEN` by
    /// default; required when `PAROS_LISTEN` is a wildcard.
    #[arg(long, env = "PAROS_ADVERTISE")]
    pub advertise: Option<String>,
    /// Where its identity (`machine`) and its stores live.
    #[arg(long, env = "PAROS_DATA_DIR")]
    pub data_dir: PathBuf,
    /// `storage` or `stateless`, fixed at format.
    #[arg(long, env = "PAROS_CLASS", default_value = "storage", value_parser = parse_class)]
    pub class: Class,
    /// Its capacity, in placement units.
    #[arg(long, env = "PAROS_CAPACITY", default_value = "32")]
    pub capacity: u64,
    /// Its failure domain label.
    #[arg(long, env = "PAROS_FAILURE_DOMAIN", default_value = "")]
    pub failure_domain: String,
    /// Its name, for people (#399): what `parosctl` prints and takes. The
    /// host of `PAROS_ADVERTISE` by default. A label, never its identity: it
    /// may change across starts.
    #[arg(long, env = "PAROS_NAME")]
    pub name: Option<String>,
    /// `HOST:PORT` of its HTTP endpoint (#366 (metrics)): `/metrics` in
    /// the Prometheus text format, `/healthz` (the process is alive) and
    /// `/readyz` (a cell member whose stores opened). None by default.
    #[arg(long, env = "PAROS_HTTP_LISTEN")]
    pub http_listen: Option<String>,
    /// An OpenTelemetry collector's OTLP/gRPC URL (#366 (metrics)): the
    /// spans and the metrics are pushed there. None by default.
    #[arg(long, env = "PAROS_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
    /// The collector the spans go to, in place of `PAROS_OTLP_ENDPOINT`.
    #[arg(long, env = "PAROS_OTLP_TRACES_ENDPOINT")]
    pub otlp_traces_endpoint: Option<String>,
    /// The collector the metrics go to, in place of `PAROS_OTLP_ENDPOINT`.
    #[arg(long, env = "PAROS_OTLP_METRICS_ENDPOINT")]
    pub otlp_metrics_endpoint: Option<String>,
    /// How often the metrics are pushed, in milliseconds.
    #[arg(long, env = "PAROS_OTLP_INTERVAL_MS", default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(100..))]
    pub otlp_interval_ms: u64,
    /// The store layout.
    #[arg(
        long,
        env = "PAROS_STORE_LAYOUT",
        value_enum,
        default_value = "default"
    )]
    pub layout: Layout,
    /// Its certificate chain, PEM, leaf first (`parosctl tls`). With the
    /// key and the authority, every session it serves and dials is TLS.
    #[arg(long, env = "PAROS_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    /// The certificate's private key, PEM.
    #[arg(long, env = "PAROS_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
    /// The universe's authority, PEM: the peers and callers it trusts.
    #[arg(long, env = "PAROS_TLS_CA")]
    pub tls_ca: Option<PathBuf>,
    /// What it asks of a caller: `required` (mutual TLS), `optional` or
    /// `off` (server-authenticated TLS).
    #[arg(long, env = "PAROS_TLS_CLIENT_AUTH", default_value = "required", value_parser = parse_client_auth)]
    pub tls_client_auth: ClientCertificates,
    /// The root public key files it pins while idle (#245 (Biscuit
    /// tokens)): the calls that put it in a cell then need an `admin`
    /// token signed by one of them. None: the network is the trust
    /// boundary.
    #[arg(long, env = "PAROS_ROOT_PUBLIC_KEY", value_delimiter = ',')]
    pub root_public_key: Vec<PathBuf>,
}

impl Settings {
    /// The collector the spans go to, if any.
    pub fn traces_endpoint(&self) -> Option<&str> {
        self.otlp_traces_endpoint
            .as_deref()
            .or(self.otlp_endpoint.as_deref())
    }

    /// The collector the metrics go to, if any.
    pub fn metrics_endpoint(&self) -> Option<&str> {
        self.otlp_metrics_endpoint
            .as_deref()
            .or(self.otlp_endpoint.as_deref())
    }

    /// The name this machine gives its telemetry before it knows its
    /// advertised address: `PAROS_NAME`, else `PAROS_ADVERTISE`, else
    /// `PAROS_LISTEN`, as written.
    pub fn instance(&self) -> &str {
        self.name
            .as_deref()
            .or(self.advertise.as_deref())
            .unwrap_or(&self.listen)
    }

    /// Its transport: TLS with the three files, plaintext with none.
    ///
    /// # Errors
    ///
    /// Only some of the files, or a file TLS cannot use.
    pub fn transport(&self) -> Result<Transport, String> {
        Files {
            cert: ("PAROS_TLS_CERT", self.tls_cert.as_deref()),
            key: ("PAROS_TLS_KEY", self.tls_key.as_deref()),
            ca: ("PAROS_TLS_CA", self.tls_ca.as_deref()),
        }
        .serving(self.tls_client_auth)
    }
}

impl Settings {
    /// Its bootstrap pin: an [`AdminPin`] over the root public keys it
    /// names, at the wall clock `epoch`; open when it names none.
    ///
    /// # Errors
    ///
    /// A key file that cannot be read or holds no key, or two keys under
    /// one key id.
    pub fn pin(&self, epoch: std::time::Duration) -> Result<AdminPin, String> {
        if self.root_public_key.is_empty() {
            return Ok(AdminPin::open());
        }
        let mut keys = Vec::with_capacity(self.root_public_key.len());
        for path in &self.root_public_key {
            let text = std::fs::read_to_string(path)
                .map_err(|error| format!("PAROS_ROOT_PUBLIC_KEY {}: {error}", path.display()))?;
            keys.push(
                RootPublicKey::from_file(&text).map_err(|error| {
                    format!("PAROS_ROOT_PUBLIC_KEY {}: {error}", path.display())
                })?,
            );
        }
        let ring = KeyRing::new(keys).map_err(|error| format!("PAROS_ROOT_PUBLIC_KEY: {error}"))?;
        Ok(AdminPin::new(Arc::new(BiscuitAuthz::new(ring)), epoch))
    }
}

fn parse_client_auth(text: &str) -> Result<ClientCertificates, String> {
    text.parse()
}

fn parse_class(text: &str) -> Result<Class, String> {
    text.parse().map_err(str::to_string)
}

/// Refuse a `PAROS_*` variable nobody reads, among `names`.
///
/// # Errors
///
/// The first unknown variable, with what is known.
pub fn check_unknown(names: impl IntoIterator<Item = String>) -> Result<(), String> {
    let tunables = crate::tunables::variables();
    for name in names {
        if name.starts_with("PAROS_")
            && !VARIABLES.contains(&name.as_str())
            && !tunables.contains(&name)
        {
            return Err(format!(
                "unknown variable {name}: parosd reads {} and one PAROS_<FIELD>[_MS] per driver \
                 tunable",
                VARIABLES.join(", ")
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_paros_variable_is_refused_and_others_pass() {
        let ok = [
            "PAROS_LISTEN",
            "PAROS_ADVERTISE",
            "PAROS_TICK_INTERVAL_MS",
            "RUST_LOG",
            "HOME",
        ];
        assert_eq!(check_unknown(ok.map(String::from)), Ok(()));
        let typo = check_unknown(["PAROS_LISTN".to_string()]);
        assert!(typo.is_err_and(|e| e.contains("PAROS_LISTN")));
        assert!(
            check_unknown(["PAROS_ID".to_string()]).is_err(),
            "PAROS_ID is gone"
        );
        assert!(
            check_unknown(["PAROS_SEEDS".to_string()]).is_err(),
            "PAROS_SEEDS is gone"
        );
    }
}
