//! `parosd` — the paros daemon (#196): **one uniform binary** every machine
//! runs, over moonpool's `TokioProviders` and the journal stores on a real
//! filesystem. `parosd` serves; the client is `parosctl` (#220), over
//! `paros::client`.
//!
//! A machine starts with its listen and advertised addresses, its data
//! directory, its class, capacity and failure domain — environment
//! variables, validated at startup ([`settings`]). It binds its listen
//! address and publishes its advertised one (#257): the cell plan and the
//! registry hold what it advertises, and its peers resolve that name each
//! time they dial it, so a machine whose IP changes across a restart is
//! reached again. There is no role to pick, no identity to pass and
//! no peer to name:
//!
//! 1. **Format, once.** On an empty data directory the machine mints its
//!    `node_id` at random (#225) and records it (`MachineRecord`). A
//!    directory that holds stores but no identity lost it, and is refused.
//! 2. **Wait.** Until it belongs to a cell, it serves the machine contract
//!    (`paros::machine::wait_for_cell`): `Identify`, the cell decree's two
//!    phases as an acceptor, and `CellInit` as its proposer (#277). It never
//!    forms a cell on its own (#216).
//! 3. **Serve.** A formed machine runs `paros::run_journals` over its
//!    cell's plan: the cell control and election journals and the fleet
//!    tenant's — every identifier drawn at `cell init`, none fixed — plain
//!    Multi-Paxos over the founding members, and every journal its tenants
//!    create through their coordinator (#210). Every start after formation is an existing member's
//!    ([`BootKind::ExistingMember`]), so a lost store is refused as amnesia.
//!
//! The lifecycle is the library's, `paros::machine::run_machine` (#246): the
//! same code the deterministic simulation runs. This binary holds only its
//! Tokio wiring, the `PAROS_*` configuration, name resolution and the exit
//! codes. Its disk is the library's `ProviderDisk` over Tokio's filesystem,
//! rooted at the data directory: the record (`<data-dir>/machine`) and the
//! stores (`<data-dir>/journals/<tenant>/<journal>/`).
//!
//! | exit | outcome | what the operator does |
//! |---|---|---|
//! | 0 | shut down on `SIGTERM` / `SIGINT` | nothing |
//! | 75 (`EX_TEMPFAIL`) | [`RunError::Storage`] or [`MachineError::Storage`]: the fail-stop crash on a storage fault | restart: the next boot recovers from what the disk holds |
//! | 78 (`EX_CONFIG`) | [`RunError::Refused`]: the store or the identity disagrees with the configuration | do **not** restart: resolve it (amnesia, an edited configuration, a class change) |
//! | 1 | [`RunError::Infra`]: bind, listen, address | fix the environment |
//! | 2 | an invalid configuration | fix the variables |

mod resolve;
mod settings;
mod tunables;

use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use moonpool_core::{TokioProviders, TokioResolver, TokioStorageProvider};
use paros::machine::{MachineAddresses, MachineError, MachineSettings, ProviderDisk};
use paros::{Address, BootRefusal, Names, NoAudit, RunError};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::settings::Settings;

/// `EX_TEMPFAIL`: a storage fault crashed the process; restart it.
const EXIT_RESTART: u8 = 75;
/// `EX_CONFIG`: the boot was refused; an operator must act.
const EXIT_REFUSED: u8 = 78;

/// How long a start waits for its names to resolve (#209): a Compose
/// service's peers may still be starting.
const RESOLVE_PATIENCE: Duration = Duration::from_secs(30);
/// How often an unresolved name is asked again.
const RESOLVE_RETRY: Duration = Duration::from_millis(500);

fn main() -> ExitCode {
    let settings = Settings::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn,parosd=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    if let Err(error) = settings::check_unknown(std::env::vars().map(|(name, _)| name)) {
        eprintln!("parosd: {error}");
        return ExitCode::from(2);
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("parosd: cannot start the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(Box::pin(run(settings)))
}

/// Run the machine: the library's lifecycle on this data directory.
async fn run(settings: Settings) -> ExitCode {
    let tunables = match tunables::from_env() {
        Ok(tunables) => tunables,
        Err(error) => return invalid(&error),
    };
    let addresses = match addresses(&settings) {
        Ok(addresses) => addresses,
        Err(error) => return invalid(&error),
    };
    let machine = MachineSettings {
        class: settings.class,
        capacity: settings.capacity,
        failure_domain: settings.failure_domain.clone(),
    };
    let disk = ProviderDisk::new(
        TokioStorageProvider::new(),
        settings.data_dir.to_string_lossy(),
        settings.layout.config(),
    );
    let ran = Box::pin(paros::machine::run_machine(
        TokioProviders::new(),
        disk,
        |_| NoAudit,
        &machine,
        addresses,
        Names::new(TokioResolver::new()),
        tunables,
        shutdown_on_signal(),
    ))
    .await;
    match ran {
        Ok(()) => {
            tracing::info!("parosd_stopped");
            ExitCode::SUCCESS
        }
        Err(MachineError::Invalid(error)) => invalid(&error),
        Err(MachineError::Refused(error)) => {
            eprintln!("parosd: {}: {error}", settings.data_dir.display());
            ExitCode::from(EXIT_REFUSED)
        }
        Err(MachineError::Storage(error)) => {
            tracing::error!(%error, "parosd_storage_failed");
            ExitCode::from(EXIT_RESTART)
        }
        Err(MachineError::Run(error)) => exit(&error),
    }
}

/// The machine's two addresses (#257): `PAROS_LISTEN` resolved once (it
/// binds where it is), `PAROS_ADVERTISE` checked and kept as written.
fn addresses(settings: &Settings) -> Result<MachineAddresses, String> {
    let advertise = settings
        .advertise
        .as_deref()
        .map(|advertise| {
            Address::parse(advertise).map_err(|error| format!("PAROS_ADVERTISE: {error}"))
        })
        .transpose()?;
    let listen = patiently(|| resolve::resolve(&settings.listen))
        .map_err(|error| format!("PAROS_LISTEN: {error}"))?;
    MachineAddresses::new(listen, advertise)
}

/// `resolve` until it answers or [`RESOLVE_PATIENCE`] runs out.
fn patiently<T>(resolve: impl Fn() -> Result<T, String>) -> Result<T, String> {
    let deadline = std::time::Instant::now() + RESOLVE_PATIENCE;
    loop {
        match resolve() {
            Ok(found) => return Ok(found),
            Err(error) if std::time::Instant::now() >= deadline => return Err(error),
            Err(_) => std::thread::sleep(RESOLVE_RETRY),
        }
    }
}

/// An invalid configuration (exit 2).
fn invalid(error: &str) -> ExitCode {
    eprintln!("parosd: {error}");
    ExitCode::from(2)
}

fn exit(error: &RunError) -> ExitCode {
    eprintln!("parosd: {error}");
    match error {
        RunError::Storage(_) => ExitCode::from(EXIT_RESTART),
        RunError::Refused(refusal) => {
            eprintln!("parosd: {}", remedy(*refusal));
            ExitCode::from(EXIT_REFUSED)
        }
        RunError::Infra(_) => ExitCode::FAILURE,
    }
}

/// What the operator does about a refusal.
fn remedy(refusal: BootRefusal) -> &'static str {
    match refusal {
        BootRefusal::Amnesia => {
            "a journal store of this machine's cell carries no format marker: its disk was \
             lost. A lost store never rejoins; wipe the machine's data directory to start it \
             as a new machine, and heal the cell by reconfiguration"
        }
        BootRefusal::AlreadyFormatted => {
            "this store is already formatted: the machine record and the stores disagree"
        }
        BootRefusal::ConfigMismatch => {
            "the store was formatted under another configuration (the boot_config_mismatch \
             event above names both); membership changes go through reconfiguration, never \
             through the configuration"
        }
    }
}

/// A cancellation token fired on `SIGTERM` or `SIGINT`: the drivers' shutdown.
fn shutdown_on_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let fired = token.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("parosd_signal_shutdown");
        fired.cancel();
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
        Err(error) => {
            tracing::warn!(%error, "sigterm_handler_unavailable");
            tokio::signal::ctrl_c().await.ok();
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    tokio::signal::ctrl_c().await.ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(args: &[&str]) -> Settings {
        let mut command = vec!["parosd", "--data-dir", "/tmp/unused"];
        command.extend_from_slice(args);
        Settings::try_parse_from(command).expect("valid arguments")
    }

    #[test]
    fn the_advertised_address_defaults_to_a_listen_address_that_is_not_a_wildcard() {
        let plain = addresses(&settings(&["--listen", "127.0.0.1:4500"])).expect("valid");
        assert_eq!(plain.listen, "127.0.0.1:4500".parse().expect("literal"));
        assert_eq!(plain.advertise.to_string(), "127.0.0.1:4500");
        let named = addresses(&settings(&[
            "--listen",
            "0.0.0.0:4500",
            "--advertise",
            "Node1:4500",
        ]))
        .expect("valid");
        assert!(named.listen.ip().is_unspecified());
        assert_eq!(named.advertise.to_string(), "node1:4500", "kept as a name");
        let refused = addresses(&settings(&["--listen", "0.0.0.0:4500"]));
        assert!(refused.is_err_and(|e| e.contains("PAROS_ADVERTISE")));
        let malformed = addresses(&settings(&[
            "--listen",
            "0.0.0.0:4500",
            "--advertise",
            "node1",
        ]));
        assert!(malformed.is_err_and(|e| e.contains("PAROS_ADVERTISE")));
        let wildcard = addresses(&settings(&[
            "--listen",
            "0.0.0.0:4500",
            "--advertise",
            "0.0.0.0:4500",
        ]));
        assert!(wildcard.is_err_and(|e| e.contains("PAROS_ADVERTISE")));
    }
}
