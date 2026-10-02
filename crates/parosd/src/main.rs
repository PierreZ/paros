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

mod deployment;
mod stores;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use moonpool_core::TokioProviders;
use paros::{
    BootKind, BootRefusal, DriverTunables, JournalId, JournalMatchmakerStorage, JournalStorage,
    JournalStoreConfig, MatchmakerId, NoAudit, NoHooks, NodeId, ProxyId, RunError,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::deployment::Deployment;
use crate::stores::{DirStores, matchmaker_dir, path_str, replica_dir};

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
    /// Where this process keeps its stores.
    #[arg(long, env = "PAROS_DATA_DIR")]
    data_dir: PathBuf,
    /// The operator's claim that this identity has never been provisioned:
    /// its stores are formatted before anything else. Without it the stores
    /// must already carry their format marker, and a store that does not is
    /// refused as amnesia — a lost disk never rejoins.
    #[arg(long)]
    first_boot: bool,
    /// The store layout.
    #[arg(long, value_enum, default_value = "default")]
    layout: Layout,
    #[command(flatten)]
    deployment: Deployment,
}

impl ServerArgs {
    fn boot(&self) -> BootKind {
        if self.first_boot {
            BootKind::FirstBoot
        } else {
            BootKind::ExistingMember
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
            "this identity's store carries no format marker: its disk was lost (or this is a \
             first boot: pass --first-boot exactly once). A lost identity never rejoins; \
             replace it by reconfiguration"
        }
        BootRefusal::AlreadyFormatted => {
            "--first-boot on a store that is already formatted: drop --first-boot to restart \
             this identity, or point --data-dir at an empty directory"
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
    let genesis: BTreeMap<JournalId, paros::Config> = d
        .journals
        .iter()
        .map(|&journal| (JournalId(journal), d.node_config(id, JournalId(journal))))
        .collect();
    let stores = DirStores::new(
        args.data_dir.clone(),
        args.layout.config(),
        genesis,
        args.boot(),
    );
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
        args.boot(),
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
        args.boot(),
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
