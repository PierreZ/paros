//! `parosd` — the paros daemon (#196): **one uniform binary** every machine
//! runs, over moonpool's `TokioProviders` and the journal stores on a real
//! filesystem. `parosd` serves; the client is `parosctl` (#220), over
//! `paros::client`.
//!
//! A machine starts with its listen address, its data directory, its class,
//! capacity and failure domain, and its rendezvous join list — environment
//! variables, validated at startup ([`settings`]). There is no role to pick
//! and no identity to pass:
//!
//! 1. **Format, once.** On an empty data directory the machine mints its
//!    `node_id` at random (#225) and records it ([`machine_record`]). A
//!    directory that holds stores but no identity lost it, and is refused.
//! 2. **Wait.** Until it belongs to a cell, it serves the machine contract
//!    (`paros::machine::wait_for_cell`): `Identify`, and — on a seed of
//!    class `storage`, one its own join list names — `Init` and
//!    `FormCell`. It never forms a cell on its own (#216).
//! 3. **Serve.** A formed machine runs `paros::run_journals` over its
//!    cell's plan: the cell control journal and the static assignment that
//!    stands in for placement until M9 (#212), plain Multi-Paxos over the
//!    seeds. Every start after formation is an existing member's
//!    ([`BootKind::ExistingMember`]), so a lost store is refused as amnesia.
//!
//! The drivers are the library's provider-generic ones — the same code the
//! deterministic simulation runs — and the stores are `paros::journal`'s.
//! The machine phase (steps 1 and 2) is not yet in the simulation (#216).
//!
//! | exit | outcome | what the operator does |
//! |---|---|---|
//! | 0 | shut down on `SIGTERM` / `SIGINT` | nothing |
//! | 75 (`EX_TEMPFAIL`) | [`RunError::Storage`]: the fail-stop crash on a storage fault | restart: the next boot recovers from what the disk holds |
//! | 78 (`EX_CONFIG`) | [`RunError::Refused`]: the store or the identity disagrees with the configuration | do **not** restart: resolve it (amnesia, an edited configuration, a class change) |
//! | 1 | [`RunError::Infra`]: bind, listen, address | fix the environment |
//! | 2 | an invalid configuration | fix the variables |

mod machine_record;
mod record;
mod resolve;
mod settings;
mod stores;
mod tunables;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::client::bootstrap::TOY_JOURNAL;
use paros::machine::{CellPlan, MachineFacts};
use paros::{BootRefusal, DriverTunables, JournalKey, NoHooks, NodeId, RunError};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::machine_record::{DirLedger, MachineRecord, journal_config};
use crate::settings::Settings;
use crate::stores::DirStores;

/// `EX_TEMPFAIL`: a storage fault crashed the process; restart it.
const EXIT_RESTART: u8 = 75;
/// `EX_CONFIG`: the boot was refused; an operator must act.
const EXIT_REFUSED: u8 = 78;

/// How long a start waits for its names to resolve (#209): a Compose
/// service's peers may still be starting.
const RESOLVE_PATIENCE: Duration = Duration::from_secs(30);
/// How often an unresolved name is asked again.
const RESOLVE_RETRY: Duration = Duration::from_millis(500);

/// How a start ended before any driver ran.
enum Stop {
    /// The configuration is invalid (exit 2).
    Invalid(String),
    /// The data directory disagrees with the configuration (exit 78).
    Refused(String),
}

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

/// Format if needed, wait for a cell if needed, then serve it.
async fn run(settings: Settings) -> ExitCode {
    let providers = TokioProviders::new();
    let tunables = match tunables::from_env() {
        Ok(tunables) => tunables,
        Err(error) => return stopped(Stop::Invalid(error)),
    };
    let record = match identity(&providers, &settings) {
        Ok(record) => record,
        Err(stop) => return stopped(stop),
    };
    let facts = match facts(&settings, &record) {
        Ok(facts) => facts,
        Err(stop) => return stopped(stop),
    };
    tracing::info!(
        node = facts.node_id.0,
        addr = %facts.addr,
        seeds = facts.seeds.len(),
        class = facts.class.as_str(),
        "parosd_starting"
    );
    let shutdown = shutdown_on_signal();
    let plan = if let Some(plan) = record.formed() {
        plan.clone()
    } else {
        let mut ledger = DirLedger {
            data_dir: settings.data_dir.clone(),
            layout: settings.layout.config(),
            record,
        };
        let waited = paros::machine::wait_for_cell(
            providers.clone(),
            &facts,
            &[TOY_JOURNAL],
            &mut ledger,
            &tunables,
            shutdown.clone(),
        )
        .await;
        match waited {
            Ok(Some(plan)) => plan,
            Ok(None) => {
                tracing::info!("parosd_stopped");
                return ExitCode::SUCCESS;
            }
            Err(error) => return exit(&error),
        }
    };
    serve(
        providers,
        &settings,
        facts.node_id,
        plan,
        tunables,
        shutdown,
    )
    .await
}

/// The machine's identity: read, or minted on an empty data directory.
fn identity(providers: &TokioProviders, settings: &Settings) -> Result<MachineRecord, Stop> {
    let dir = &settings.data_dir;
    let read =
        MachineRecord::read(dir).map_err(|e| Stop::Refused(format!("machine record: {e}")))?;
    let Some(mut record) = read else {
        // A directory with stores and no identity lost it: never a new
        // machine on top of an old one's stores.
        if dir.join("journals").exists() || record::Record::read(dir).ok().flatten().is_some() {
            return Err(Stop::Refused(format!(
                "{}: stores without a machine record — this machine lost its identity \
                 (amnesia); wipe the directory to start a new machine, which never rejoins \
                 as the old one",
                dir.display()
            )));
        }
        let rendezvous = settings.rendezvous.clone().ok_or_else(|| {
            Stop::Invalid("PAROS_RENDEZVOUS is required on a machine's first start".into())
        })?;
        let node_id = loop {
            let id: u64 = providers.random().random();
            if id != 0 {
                break NodeId(id);
            }
        };
        let record = MachineRecord {
            node_id,
            class: settings.class,
            capacity: settings.capacity,
            failure_domain: settings.failure_domain.clone(),
            rendezvous,
            plan: None,
        };
        record
            .write(dir)
            .map_err(|e| Stop::Refused(format!("machine record: {e}")))?;
        tracing::info!(node = node_id.0, "machine_formatted");
        return Ok(record);
    };
    if record.class != settings.class {
        return Err(Stop::Refused(format!(
            "this machine was formatted as {}, not {}: a class is fixed at format",
            record.class.as_str(),
            settings.class.as_str()
        )));
    }
    let mut changed =
        record.capacity != settings.capacity || record.failure_domain != settings.failure_domain;
    record.capacity = settings.capacity;
    record.failure_domain.clone_from(&settings.failure_domain);
    if let Some(rendezvous) = &settings.rendezvous
        && *rendezvous != record.rendezvous
    {
        record.rendezvous.clone_from(rendezvous);
        changed = true;
    }
    if changed {
        record
            .write(dir)
            .map_err(|e| Stop::Refused(format!("machine record: {e}")))?;
    }
    Ok(record)
}

/// What the machine knows of itself: its address and its seeds, resolved
/// once, here (#209), retried while a Compose peer starts.
fn facts(settings: &Settings, record: &MachineRecord) -> Result<MachineFacts, Stop> {
    let addr = patiently(|| resolve::resolve(&settings.listen)).map_err(Stop::Invalid)?;
    let mut seeds: Vec<SocketAddr> = Vec::new();
    for entry in record
        .rendezvous
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
    {
        for seed in patiently(|| resolve::resolve_all(entry)).map_err(Stop::Invalid)? {
            if !seeds.contains(&seed) {
                seeds.push(seed);
            }
        }
    }
    if seeds.is_empty() {
        return Err(Stop::Invalid("the rendezvous names no seed".into()));
    }
    Ok(MachineFacts {
        node_id: record.node_id,
        class: record.class,
        capacity: record.capacity,
        failure_domain: record.failure_domain.clone(),
        addr,
        seeds,
    })
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

/// Serve the cell's journals until shutdown.
async fn serve(
    providers: TokioProviders,
    settings: &Settings,
    node_id: NodeId,
    plan: CellPlan,
    tunables: DriverTunables,
    shutdown: CancellationToken,
) -> ExitCode {
    let genesis: BTreeMap<JournalKey, paros::Config> = plan
        .journals
        .iter()
        .map(|&journal| (journal, journal_config(&plan, node_id, journal)))
        .collect();
    let stores = match DirStores::load(
        node_id.0,
        settings.data_dir.clone(),
        settings.layout.config(),
        genesis,
    )
    .await
    {
        Ok(stores) => stores,
        Err(error) => return stopped(Stop::Refused(error)),
    };
    let addr = plan
        .members
        .iter()
        .find(|(id, _)| *id == node_id)
        .map_or_else(|| settings.listen.clone(), |(_, addr)| addr.to_string());
    let book: Vec<(NodeId, String)> = plan
        .members
        .iter()
        .map(|(id, addr)| (*id, addr.to_string()))
        .collect();
    tracing::info!(node = node_id.0, cell = plan.cell_id, %addr, "parosd_serving");
    let ran = paros::run_journals(
        providers,
        stores,
        addr,
        book,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
        tunables,
        shutdown,
        &NoHooks,
    )
    .await;
    match ran {
        Ok(()) => {
            tracing::info!("parosd_stopped");
            ExitCode::SUCCESS
        }
        Err(error) => exit(&error),
    }
}

fn stopped(stop: Stop) -> ExitCode {
    match stop {
        Stop::Invalid(error) => {
            eprintln!("parosd: {error}");
            ExitCode::from(2)
        }
        Stop::Refused(error) => {
            eprintln!("parosd: {error}");
            ExitCode::from(EXIT_REFUSED)
        }
    }
}

fn exit(error: &RunError) -> ExitCode {
    eprintln!("parosd: {error}");
    match error {
        RunError::Storage(_) => ExitCode::from(EXIT_RESTART),
        RunError::Refused(refusal) => {
            eprintln!("parosd: {}", remedy(*refusal));
            ExitCode::from(EXIT_REFUSED)
        }
        RunError::Infra(_) | RunError::SeamCrash(_) => ExitCode::FAILURE,
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
