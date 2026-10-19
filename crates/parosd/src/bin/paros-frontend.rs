//! `paros-frontend` — the frontend (#192 (the frontend),
//! `docs/architecture/03-5-frontend-and-resolver.md` §3.5): the stateless entry role in front of a
//! cell's machines, over moonpool's `TokioProviders`.
//!
//! A client reaches a frontend for its data, never a machine. The
//! frontend checks each call's Biscuit token against the root public keys
//! it trusts, resolves the call's names (`paros://<tenant>/<journal>`) to
//! the ids the machines know, and forwards the call and its answer. The
//! frontend is the library's, `paros::frontend::run_frontend`: the same
//! code the deterministic simulation runs. This binary holds only its
//! Tokio wiring and configuration. It holds no key and no state.
//!
//! The root public keys it trusts are the universe entry's (#245 (Biscuit
//! tokens), `parosctl universe key`), read with the universe directory.
//! The key files it is configured with rule only while it has read none,
//! or the universe holds none. Without them, it refuses every call until
//! it read the universe entry's keys.
//!
//! | variable | meaning |
//! |---|---|
//! | `PAROS_FRONTEND_LISTEN` | `HOST:PORT` the frontend binds |
//! | `PAROS_FRONTEND_CELL` | the cell's founding members, `HOST:PORT,…` (a Compose alias stands for each machine behind it) |
//! | `PAROS_FRONTEND_ROOT_PUBLIC_KEY` | the root public keys it trusts until it read the universe entry's, comma-separated, each a key line or a key file's path (`parosctl key generate`); optional |
//! | `PAROS_FRONTEND_TIMEOUT_MS` | one forwarded attempt's timeout (default 5000) |
//! | `PAROS_FRONTEND_TLS_CERT` | its certificate chain, PEM (`parosctl tls`, role `frontend`); with the next two, every session is TLS, to its clients and to the machines |
//! | `PAROS_FRONTEND_TLS_KEY` | the certificate's private key, PEM |
//! | `PAROS_FRONTEND_TLS_CA` | the universe's authority, PEM |
//! | `PAROS_FRONTEND_TLS_CLIENT_AUTH` | `required` (the default), `optional` or `off`: what it asks of a client |
//!
//! The machines and the frontend make the same choice: a frontend with
//! TLS dials TLS machines, and presents its own certificate to them.
//!
//! Until placement (#212) a frontend fronts the founding members, which
//! serve every journal; until `Resolve` (#216) a client is configured with
//! its frontends' addresses.
//!
//! | exit | meaning |
//! |---|---|
//! | 0 | shut down on `SIGTERM` / `SIGINT` |
//! | 1 | the runtime, the bind or the listener failed |
//! | 2 | an invalid configuration |

#[path = "../resolve.rs"]
mod resolve;
#[path = "../root_key.rs"]
mod root_key;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::Parser;
use moonpool_core::{TokioProviders, TokioResolver};
use paros::client::ClientTunables;
use paros::frontend::{FrontendSettings, run_frontend};
use paros::tls::{ClientCertificates, Files};
use paros::{Address, DriverTunables, Names, NoAudit};
use paros_authz_biscuit::{BiscuitAuthz, KeyRing, RootPublicKey, since_epoch};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// The paros frontend: checks, resolves and forwards a cell's journal calls.
#[derive(Parser, Debug)]
#[command(name = "paros-frontend", version, about)]
struct Settings {
    /// `HOST:PORT` the frontend binds.
    #[arg(long, env = "PAROS_FRONTEND_LISTEN")]
    listen: String,
    /// The cell's founding members, `HOST:PORT,…`.
    #[arg(long, env = "PAROS_FRONTEND_CELL", value_delimiter = ',')]
    cell: Vec<String>,
    /// The root public keys it trusts until it read the universe entry's,
    /// comma-separated, each a key line or a key file's path.
    #[arg(long, env = "PAROS_FRONTEND_ROOT_PUBLIC_KEY", value_delimiter = ',', value_parser = root_key::parse)]
    root_public_key: Vec<RootPublicKey>,
    /// One forwarded attempt's timeout, in milliseconds.
    #[arg(long, env = "PAROS_FRONTEND_TIMEOUT_MS", default_value = "5000")]
    timeout_ms: u64,
    /// Its certificate chain, PEM, leaf first. With the key and the
    /// authority, every session is TLS.
    #[arg(long, env = "PAROS_FRONTEND_TLS_CERT")]
    tls_cert: Option<std::path::PathBuf>,
    /// The certificate's private key, PEM.
    #[arg(long, env = "PAROS_FRONTEND_TLS_KEY")]
    tls_key: Option<std::path::PathBuf>,
    /// The universe's authority, PEM.
    #[arg(long, env = "PAROS_FRONTEND_TLS_CA")]
    tls_ca: Option<std::path::PathBuf>,
    /// What it asks of a client: `required` (mutual TLS), `optional` or
    /// `off`.
    #[arg(long, env = "PAROS_FRONTEND_TLS_CLIENT_AUTH", default_value = "required", value_parser = parse_client_auth)]
    tls_client_auth: ClientCertificates,
}

fn parse_client_auth(text: &str) -> Result<ClientCertificates, String> {
    text.parse()
}

fn main() -> ExitCode {
    let settings = Settings::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn,paros_frontend=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let configured = match configure(&settings) {
        Ok(configured) => configured,
        Err(error) => {
            eprintln!("paros-frontend: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("paros-frontend: cannot start the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(Box::pin(run(configured)))
}

/// The frontend's settings and the ring it trusts, from the configuration.
fn configure(settings: &Settings) -> Result<(FrontendSettings, KeyRing), String> {
    let listen = resolve::resolve(&settings.listen)
        .map_err(|error| format!("PAROS_FRONTEND_LISTEN: {error}"))?;
    let mut cell: Vec<Address> = Vec::new();
    for entry in &settings.cell {
        cell.extend(
            resolve::expand(entry).map_err(|error| format!("PAROS_FRONTEND_CELL: {error}"))?,
        );
    }
    if cell.is_empty() {
        return Err("PAROS_FRONTEND_CELL names no machine".into());
    }
    if settings.root_public_key.is_empty() {
        tracing::info!("frontend_trusts_the_universe_entry_keys_only");
    }
    let ring = KeyRing::new(settings.root_public_key.iter().cloned())
        .map_err(|error| error.to_string())?;
    let transport = Files {
        cert: ("PAROS_FRONTEND_TLS_CERT", settings.tls_cert.as_deref()),
        key: ("PAROS_FRONTEND_TLS_KEY", settings.tls_key.as_deref()),
        ca: ("PAROS_FRONTEND_TLS_CA", settings.tls_ca.as_deref()),
    }
    .serving(settings.tls_client_auth)?;
    let client = ClientTunables {
        request_timeout: Duration::from_millis(settings.timeout_ms.max(1)),
        read_timeout: Duration::from_millis(settings.timeout_ms.max(1)),
        ..ClientTunables::default()
    };
    Ok((
        FrontendSettings {
            listen,
            cell,
            names: Names::new(TokioResolver::new()),
            epoch: since_epoch(SystemTime::now()),
            tunables: DriverTunables::production(),
            transport,
            client,
        },
        ring,
    ))
}

async fn run((settings, ring): (FrontendSettings, KeyRing)) -> ExitCode {
    // The epoch is read as the providers start: the frontend's wall clock
    // is `epoch + time.now()`.
    let providers = TokioProviders::new();
    let settings = FrontendSettings {
        epoch: since_epoch(SystemTime::now()),
        ..settings
    };
    let ran = run_frontend(
        providers,
        settings,
        Arc::new(BiscuitAuthz::new(ring)),
        NoAudit,
        shutdown_on_signal(),
    )
    .await;
    match ran {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("paros-frontend: {error}");
            ExitCode::FAILURE
        }
    }
}

/// A cancellation token fired on `SIGTERM` or `SIGINT`: the frontend's
/// clean stop (#560 (clean Ctrl-C shutdown)), which refuses new calls and
/// answers the ones it took. A second signal exits at once (code 130).
fn shutdown_on_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let fired = token.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("frontend_signal_shutdown");
        fired.cancel();
        wait_for_signal().await;
        eprintln!("paros-frontend: a second signal: stopping at once");
        std::process::exit(130);
    });
    token
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }
        Err(_) => {
            tokio::signal::ctrl_c().await.ok();
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    tokio::signal::ctrl_c().await.ok();
}
