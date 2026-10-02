//! `parosd` — the paros daemon (#206): every role of a deployment over
//! moonpool's `TokioProviders` and the journal stores on a real
//! filesystem. `parosd` serves; the client is `parosctl` (#220), over
//! `paros::client`.
//!
//! The drivers it runs are the library's provider-generic ones — the same
//! code the deterministic simulation runs over `SimProviders` — and the
//! stores are `paros::journal`'s, the same code the simulation runs over
//! the simulated disk. What this binary adds is only what a process needs:
//! argument parsing, a tracing subscriber, the data directory, signals, and
//! an exit code per way a driver can stop:
//!
//! | exit | driver outcome | what the operator does |
//! |---|---|---|
//! | 0 | shut down on `SIGTERM` / `SIGINT` | nothing |
//! | 75 (`EX_TEMPFAIL`) | [`RunError::Storage`]: the fail-stop crash on a storage fault | restart: the next boot recovers from what the disk holds |
//! | 78 (`EX_CONFIG`) | [`RunError::Refused`]: the boot claim or the configuration disagrees with the store | do **not** restart: resolve the claim (amnesia, a formatted store, an edited configuration) |
//! | 1 | [`RunError::Infra`]: bind, listen, address | fix the environment |
//!
//! Provisioning is its own command (#208): `parosd provision <role>` formats
//! every store of an identity, writes the provisioning record and exits;
//! every ordinary start is an existing member's ([`BootKind::ExistingMember`]),
//! so a start never formats and a wiped volume is refused as amnesia.

mod deployment;
mod record;
mod stores;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use moonpool_core::TokioProviders;
use paros::{
    BootKind, BootRefusal, DriverTunables, JournalId, JournalMatchmakerStorage, JournalStorage,
    JournalStoreConfig, MatchmakerId, NoAudit, NoHooks, NodeId, Provisioned, ProxyId, RunError,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::deployment::Deployment;
use crate::record::Record;
use crate::stores::{DirStores, journal_dir, matchmaker_dir, path_str, replica_dir};

/// The paros daemon.
#[derive(Parser, Debug)]
#[command(name = "parosd", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run an acceptor node: every journal of the deployment.
    Node(ServerArgs),
    /// Run a matchmaker (the deployment must name matchmakers).
    Matchmaker(ServerArgs),
    /// Run a replica: the deployment's journal, learned, never voted on.
    Replica(ServerArgs),
    /// Run a proxy leader (stateless: no data directory, no boot claim).
    Proxy(ProxyArgs),
    /// Provision an identity, once: format every store it keeps, record
    /// it, and exit. Never part of a start.
    #[command(subcommand)]
    Provision(Provision),
}

/// The roles that keep stores, each provisioned once.
#[derive(Subcommand, Debug)]
enum Provision {
    /// Format an acceptor node's stores: one per journal of the deployment.
    Node(ServerArgs),
    /// Format a matchmaker's registry.
    Matchmaker(ServerArgs),
    /// Format a replica's log.
    Replica(ServerArgs),
}

/// The store layout a server runs.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Layout {
    /// The CLSTORE layout: 64 MiB segments.
    Default,
    /// 256 KiB segments and frequent checkpoints, for tests and laptops.
    Small,
}

impl Layout {
    fn config(self) -> JournalStoreConfig {
        match self {
            Layout::Default => JournalStoreConfig::default(),
            Layout::Small => JournalStoreConfig::small(),
        }
    }
}

#[derive(clap::Args, Debug)]
struct ServerArgs {
    /// This process's id in its role's list of the deployment.
    #[arg(long)]
    id: u64,
    /// Where this process keeps its stores. A start finds them formatted
    /// by `parosd provision`, or refuses: a store without its format marker
    /// is amnesia, and a lost disk never rejoins.
    #[arg(long, env = "PAROS_DATA_DIR")]
    data_dir: PathBuf,
    /// The store layout.
    #[arg(long, value_enum, default_value = "default")]
    layout: Layout,
    #[command(flatten)]
    deployment: Deployment,
}

impl ServerArgs {
    /// The provisioning record a start finds: it must name this identity
    /// when present. A missing one is not refused here — the stores'
    /// markers judge the start, and name what is missing.
    fn check_record(&self, role: &str) -> Result<(), String> {
        match Record::read(&self.data_dir) {
            Ok(Some(record)) => record.check(role, self.id),
            Ok(None) => {
                tracing::warn!(data_dir = %self.data_dir.display(), "parosd_unprovisioned");
                Ok(())
            }
            Err(error) => Err(format!("provisioning record: {error}")),
        }
    }
}

#[derive(clap::Args, Debug)]
struct ProxyArgs {
    /// This proxy's id (`0..n`).
    #[arg(long)]
    id: u64,
    #[command(flatten)]
    deployment: Deployment,
}

/// `EX_TEMPFAIL`: a storage fault crashed the process; restart it.
const EXIT_RESTART: u8 = 75;
/// `EX_CONFIG`: the boot was refused; an operator must act.
const EXIT_REFUSED: u8 = 78;

fn main() -> ExitCode {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn,parosd=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
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
    runtime.block_on(run(cli.command))
}

async fn run(command: Command) -> ExitCode {
    match command {
        // Boxed: a driver's future holds every arm's state of its loop.
        Command::Node(args) => Box::pin(serve("node", run_node(args))).await,
        Command::Matchmaker(args) => Box::pin(serve("matchmaker", run_matchmaker(args))).await,
        Command::Replica(args) => Box::pin(serve("replica", run_replica(args))).await,
        Command::Proxy(args) => Box::pin(serve("proxy", run_proxy(args))).await,
        Command::Provision(role) => Box::pin(serve("provision", provision(role))).await,
    }
}

/// Run one role until it stops, and map how it stopped to an exit code.
async fn serve(
    role: &str,
    driver: impl Future<Output = Result<Result<(), RunError>, String>>,
) -> ExitCode {
    match driver.await {
        Err(invalid) => {
            eprintln!("parosd {role}: {invalid}");
            ExitCode::from(2)
        }
        Ok(Ok(())) => {
            tracing::info!(role, "parosd_stopped");
            ExitCode::SUCCESS
        }
        Ok(Err(error)) => {
            eprintln!("parosd {role}: {error}");
            match error {
                RunError::Storage(_) => ExitCode::from(EXIT_RESTART),
                RunError::Refused(refusal) => {
                    eprintln!("parosd {role}: {}", remedy(refusal));
                    ExitCode::from(EXIT_REFUSED)
                }
                RunError::Infra(_) | RunError::SeamCrash(_) => ExitCode::FAILURE,
            }
        }
    }
}

/// What the operator does about a refusal.
fn remedy(refusal: BootRefusal) -> &'static str {
    match refusal {
        BootRefusal::Amnesia => {
            "this identity's store carries no format marker: its disk was lost, or it was \
             never provisioned (run `parosd provision` once, before its first start). A lost \
             identity never rejoins; replace it by reconfiguration"
        }
        BootRefusal::AlreadyFormatted => {
            "this identity is already provisioned (its data directory carries the provisioning \
             record): start it with `parosd <role>`, or point --data-dir at an empty directory"
        }
        BootRefusal::ConfigMismatch => {
            "the store was formatted under another configuration (the boot_config_mismatch \
             event above names both): restore the deployment it was provisioned with; \
             membership changes go through reconfiguration, never through the configuration"
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

async fn run_node(args: ServerArgs) -> Result<Result<(), RunError>, String> {
    let d = &args.deployment;
    d.validate()?;
    let id = NodeId(args.id);
    let addr = Deployment::addr_of(&d.nodes, "node", args.id)?;
    let stores = DirStores::load(
        args.id,
        args.data_dir.clone(),
        args.layout.config(),
        genesis(d, id),
    )
    .await?;
    tracing::info!(node = id.0, %addr, data_dir = %args.data_dir.display(), "parosd_node_starting");
    Ok(paros::run_journals(
        TokioProviders::new(),
        stores,
        addr,
        d.node_book(),
        d.matchmaker_book(),
        d.proxy_book(),
        d.replica_book(),
        None,
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
    )
    .await)
}

async fn run_matchmaker(args: ServerArgs) -> Result<Result<(), RunError>, String> {
    let d = &args.deployment;
    d.validate()?;
    let addr = Deployment::addr_of(&d.matchmakers, "matchmaker", args.id)?;
    args.check_record("matchmaker")?;
    let providers = TokioProviders::new();
    let storage = JournalMatchmakerStorage::new(
        moonpool_core::Providers::storage(&providers).clone(),
        path_str(&matchmaker_dir(&args.data_dir)),
        args.layout.config(),
    );
    tracing::info!(matchmaker = args.id, %addr, "parosd_matchmaker_starting");
    Ok(paros::run_matchmaker(
        providers,
        storage,
        BootKind::ExistingMember,
        addr,
        d.matchmaker_config(MatchmakerId(args.id)),
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
        &NoAudit,
    )
    .await)
}

async fn run_replica(args: ServerArgs) -> Result<Result<(), RunError>, String> {
    let d = &args.deployment;
    d.validate()?;
    let addr = Deployment::addr_of(&d.replicas, "replica", args.id)?;
    args.check_record("replica")?;
    let providers = TokioProviders::new();
    let storage = JournalStorage::new(
        moonpool_core::Providers::storage(&providers).clone(),
        path_str(&replica_dir(&args.data_dir)),
        d.replica_config(NodeId(args.id)),
        args.layout.config(),
    );
    tracing::info!(replica = args.id, %addr, "parosd_replica_starting");
    Ok(paros::run_replica(
        providers,
        storage,
        addr,
        d.node_book(),
        BootKind::ExistingMember,
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
        &NoAudit,
    )
    .await)
}

async fn run_proxy(args: ProxyArgs) -> Result<Result<(), RunError>, String> {
    let d = &args.deployment;
    d.validate()?;
    let addr = Deployment::addr_of(&d.proxies, "proxy", args.id)?;
    tracing::info!(proxy = args.id, %addr, "parosd_proxy_starting");
    Ok(paros::run_proxy(
        TokioProviders::new(),
        addr,
        d.proxy_config(ProxyId(args.id)),
        d.node_book(),
        d.replica_book(),
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
        &NoAudit,
    )
    .await)
}

/// Node `id`'s genesis journals and their configurations, in id order.
fn genesis(d: &Deployment, id: NodeId) -> BTreeMap<JournalId, paros::Config> {
    d.journals
        .iter()
        .map(|&journal| (JournalId(journal), d.node_config(id, JournalId(journal))))
        .collect()
}

/// `parosd provision <role>` (#208): format every store of the identity,
/// then write the provisioning record. A data directory that carries a
/// record was provisioned already and is refused; one without a record
/// resumes an interrupted provisioning from what its disk holds.
async fn provision(role: Provision) -> Result<Result<(), RunError>, String> {
    let (name, args) = match &role {
        Provision::Node(args) => ("node", args),
        Provision::Matchmaker(args) => ("matchmaker", args),
        Provision::Replica(args) => ("replica", args),
    };
    let d = &args.deployment;
    d.validate()?;
    let book = match role {
        Provision::Node(_) => &d.nodes,
        Provision::Matchmaker(_) => &d.matchmakers,
        Provision::Replica(_) => &d.replicas,
    };
    Deployment::addr_of(book, name, args.id)?;
    match Record::read(&args.data_dir) {
        Ok(None) => {}
        Ok(Some(_)) => return Ok(Err(RunError::Refused(BootRefusal::AlreadyFormatted))),
        Err(error) => return Err(format!("provisioning record: {error}")),
    }
    let provider = moonpool_core::TokioStorageProvider::new();
    let layout = args.layout.config();
    let mut outcomes = Vec::new();
    let mut journals = std::collections::BTreeSet::new();
    match role {
        Provision::Node(_) => {
            for (journal, config) in genesis(d, NodeId(args.id)) {
                let mut store = JournalStorage::new(
                    provider.clone(),
                    path_str(&journal_dir(&args.data_dir, journal)),
                    config,
                    layout,
                );
                match paros::provision_store(&mut store).await {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(error) => return Ok(Err(error)),
                }
                journals.insert(journal);
            }
        }
        Provision::Matchmaker(_) => {
            let mut store = JournalMatchmakerStorage::new(
                provider,
                path_str(&matchmaker_dir(&args.data_dir)),
                layout,
            );
            let config = d.matchmaker_config(MatchmakerId(args.id));
            match paros::provision_matchmaker_store(&mut store, &config).await {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) => return Ok(Err(error)),
            }
        }
        Provision::Replica(_) => {
            let mut store = JournalStorage::new(
                provider,
                path_str(&replica_dir(&args.data_dir)),
                d.replica_config(NodeId(args.id)),
                layout,
            );
            match paros::provision_store(&mut store).await {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) => return Ok(Err(error)),
            }
        }
    }
    let record = Record {
        role: name.into(),
        id: args.id,
        journals,
    };
    record
        .write(&args.data_dir)
        .map_err(|e| format!("provisioning record: {e}"))?;
    let resumed = outcomes
        .iter()
        .filter(|o| **o == Provisioned::Resumed)
        .count();
    println!(
        "provisioned {name} {}: {} stores formatted, {resumed} already formatted by an \
         interrupted run",
        args.id,
        outcomes.len() - resumed
    );
    Ok(Ok(()))
}
