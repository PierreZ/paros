//! `paros-frontend` — the frontend (#192 (the frontend),
//! `docs/architecture.md` §3.5): the stateless entry role in front of a
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
//! | variable | meaning |
//! |---|---|
//! | `PAROS_FRONTEND_LISTEN` | `HOST:PORT` the frontend binds |
//! | `PAROS_FRONTEND_CELL` | the cell's founding members, `HOST:PORT,…` (a Compose alias stands for each machine behind it) |
//! | `PAROS_FRONTEND_ROOT_PUBLIC_KEY` | the root public key files it trusts, comma-separated (`parosctl key generate`) |
//! | `PAROS_FRONTEND_TIMEOUT_MS` | one forwarded attempt's timeout (default 5000) |
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

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::Parser;
use moonpool_core::{TokioProviders, TokioResolver};
use paros::client::ClientTunables;
use paros::frontend::{FrontendSettings, run_frontend};
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
    /// The root public key files it trusts, comma-separated.
    #[arg(long, env = "PAROS_FRONTEND_ROOT_PUBLIC_KEY", value_delimiter = ',')]
    root_public_key: Vec<std::path::PathBuf>,
    /// One forwarded attempt's timeout, in milliseconds.
    #[arg(long, env = "PAROS_FRONTEND_TIMEOUT_MS", default_value = "5000")]
    timeout_ms: u64,
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
    let mut keys = Vec::with_capacity(settings.root_public_key.len());
    for path in &settings.root_public_key {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        keys.push(
            RootPublicKey::from_file(&text)
                .map_err(|error| format!("{}: {error}", path.display()))?,
        );
    }
    if keys.is_empty() {
        return Err(
            "PAROS_FRONTEND_ROOT_PUBLIC_KEY names no key: every call would be refused".into(),
        );
    }
    let ring = KeyRing::new(keys).map_err(|error| error.to_string())?;
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

/// A cancellation token fired on `SIGTERM` or `SIGINT`.
fn shutdown_on_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let fired = token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        fired.cancel();
    });
    token
}
