//! `parosd`: one role of a paros deployment, over Tokio.
//!
//! ```text
//! parosd node       --id 0 --data-dir DIR [--first-boot] <topology>
//! parosd matchmaker --id 0 --data-dir DIR [--first-boot] <topology>
//! parosd proxy      --id 0 <topology>
//! parosd replica    --id 1000 --data-dir DIR [--first-boot] <topology>
//! ```
//!
//! Every flag has a `PAROS_*` environment variable; list-valued ones take a
//! comma-separated list. See the crate README for a laptop walkthrough.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use moonpool_core::{TokioProviders, TokioStorageProvider};
use paros::{
    BootKind, DriverTunables, JournalId, JournalMatchmakerStorage, JournalStorage,
    JournalStoreConfig, MatchmakerId, NoAudit, NoHooks, NodeId, RunError, run_journals,
    run_matchmaker, run_proxy, run_replica,
};
use parosd::exit;
use parosd::stores::{DirStores, journal_dir};
use parosd::topology::{Entry, Topology};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "parosd", version, about = "Run one role of a paros deployment")]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// An acceptor node: every journal of the topology, each on its own
    /// store under the data directory.
    Node {
        #[command(flatten)]
        me: Identity,
        #[command(flatten)]
        disk: Disk,
        #[command(flatten)]
        topology: TopologyArgs,
    },
    /// A matchmaker: the configuration registry of a deployment that names
    /// matchmakers.
    Matchmaker {
        #[command(flatten)]
        me: Identity,
        #[command(flatten)]
        disk: Disk,
        #[command(flatten)]
        topology: TopologyArgs,
    },
    /// A proxy leader: the deployment journal's Phase-2 fan-out. Nothing
    /// durable.
    Proxy {
        #[command(flatten)]
        me: Identity,
        #[command(flatten)]
        topology: TopologyArgs,
    },
    /// A replica: the deployment journal's chosen log, read from by clients;
    /// it never votes.
    Replica {
        #[command(flatten)]
        me: Identity,
        #[command(flatten)]
        disk: Disk,
        #[command(flatten)]
        topology: TopologyArgs,
    },
}

#[derive(Args)]
struct Identity {
    /// This process's identity in its role's address book.
    #[arg(long, env = "PAROS_ID")]
    id: u64,
    /// The address to listen on; defaults to this process's address in the
    /// book (what its peers dial).
    #[arg(long, env = "PAROS_LISTEN")]
    listen: Option<std::net::SocketAddr>,
}

#[derive(Args)]
struct Disk {
    /// Where this process's stores live (one directory per journal).
    #[arg(long, env = "PAROS_DATA_DIR")]
    data_dir: PathBuf,
    /// The operator's claim that this identity has never been provisioned:
    /// the stores are formatted on this boot. Without it every store must
    /// already carry its format marker, and one that does not is refused as
    /// amnesiac. Provisioning replaces this flag (#208).
    #[arg(long, env = "PAROS_FIRST_BOOT")]
    first_boot: bool,
}

impl Disk {
    fn boot(&self) -> BootKind {
        if self.first_boot {
            BootKind::FirstBoot
        } else {
            BootKind::ExistingMember
        }
    }
}

#[derive(Args)]
struct TopologyArgs {
    /// The acceptor pool, `ID=IP:PORT` each.
    #[arg(
        long = "node",
        env = "PAROS_NODES",
        value_delimiter = ',',
        required = true
    )]
    nodes: Vec<Entry>,
    /// The bootstrap configuration's node ids, when not the whole pool
    /// (matchmaker deployments only).
    #[arg(long = "bootstrap", env = "PAROS_BOOTSTRAP", value_delimiter = ',')]
    bootstrap: Vec<u64>,
    /// The matchmakers, `ID=IP:PORT` each (none: plain Multi-Paxos).
    #[arg(long = "matchmaker", env = "PAROS_MATCHMAKERS", value_delimiter = ',')]
    matchmakers: Vec<Entry>,
    /// The proxy leaders, `ID=IP:PORT` each, in `ProxyId` order.
    #[arg(long = "proxy", env = "PAROS_PROXIES", value_delimiter = ',')]
    proxies: Vec<Entry>,
    /// The replicas, `ID=IP:PORT` each, in `ReplicaId` order; ids outside
    /// the node pool.
    #[arg(long = "replica", env = "PAROS_REPLICAS", value_delimiter = ',')]
    replicas: Vec<Entry>,
    /// The journals every node serves; the first is the one the
    /// matchmakers, proxies and replicas serve.
    #[arg(
        long = "journal",
        env = "PAROS_JOURNALS",
        value_delimiter = ',',
        default_value = "128"
    )]
    journals: Vec<u64>,
}

impl TopologyArgs {
    fn topology(self) -> Result<Topology, String> {
        let topology = Topology {
            nodes: self.nodes,
            bootstrap: self.bootstrap,
            matchmakers: self.matchmakers,
            proxies: self.proxies,
            replicas: self.replicas,
            journals: self.journals.into_iter().map(JournalId).collect(),
        };
        topology.validate()?;
        Ok(topology)
    }
}

/// Where `me` listens: its override, or its address in `book`.
fn listen_addr(me: &Identity, role: &str, book: &[Entry]) -> Result<String, String> {
    match me.listen {
        Some(addr) => Ok(addr.to_string()),
        None => Topology::address_of(book, me.id)
            .ok_or_else(|| format!("{role} {} is not in its address book", me.id)),
    }
}

/// A cancellation token `SIGTERM` and `SIGINT` cancel: the driver's
/// shutdown signal, so a stopped process ends its loop and drops its
/// listener instead of dying mid-batch.
fn shutdown_on_signal() -> CancellationToken {
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    tokio::spawn(async move {
        let terminate = async {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{SignalKind, signal};
                match signal(SignalKind::terminate()) {
                    Ok(mut sigterm) => {
                        sigterm.recv().await;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "no SIGTERM handler");
                        std::future::pending::<()>().await;
                    }
                }
            }
            #[cfg(not(unix))]
            std::future::pending::<()>().await;
        };
        tokio::select! {
            () = terminate => tracing::info!("sigterm"),
            _ = tokio::signal::ctrl_c() => tracing::info!("sigint"),
        }
        token.cancel();
    });
    shutdown
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,parosd=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let name = match &cli.role {
        Role::Node { .. } => "node",
        Role::Matchmaker { .. } => "matchmaker",
        Role::Proxy { .. } => "proxy",
        Role::Replica { .. } => "replica",
    };
    match run(cli.role).await {
        Ok(result) => exit::report(name, &result),
        Err(usage) => {
            tracing::error!(role = name, error = %usage, "parosd_usage");
            eprintln!("parosd {name}: {usage}");
            ExitCode::from(exit::USAGE)
        }
    }
}

/// The outcome of a role that started: its driver's result. `Err` is a
/// usage error found before anything ran.
type Started = Result<Result<(), RunError>, String>;

/// Run one role to its end.
async fn run(role: Role) -> Started {
    match role {
        Role::Node { me, disk, topology } => node(me, disk, topology.topology()?).await,
        Role::Matchmaker { me, disk, topology } => matchmaker(me, disk, topology.topology()?).await,
        Role::Proxy { me, topology } => proxy(me, topology.topology()?).await,
        Role::Replica { me, disk, topology } => replica(me, disk, topology.topology()?).await,
    }
}

async fn node(me: Identity, disk: Disk, topology: Topology) -> Started {
    if !topology.nodes.iter().any(|n| n.id == me.id) {
        return Err(format!("node {} is not in --node", me.id));
    }
    let addr = listen_addr(&me, "node", &topology.nodes)?;
    let stores = DirStores::new(
        disk.data_dir.clone(),
        topology.node_configs(NodeId(me.id)),
        JournalStoreConfig::default(),
        disk.boot(),
    );
    tracing::info!(id = me.id, %addr, data_dir = %disk.data_dir.display(), "parosd_node");
    Ok(run_journals(
        TokioProviders::new(),
        stores,
        addr,
        topology.node_book(),
        topology.matchmaker_book(),
        topology.proxy_book(),
        topology.replica_book(),
        None,
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
    )
    .await)
}

async fn matchmaker(me: Identity, disk: Disk, topology: Topology) -> Started {
    if !topology.matchmakers.iter().any(|m| m.id == me.id) {
        return Err(format!("matchmaker {} is not in --matchmaker", me.id));
    }
    let addr = listen_addr(&me, "matchmaker", &topology.matchmakers)?;
    let dir = disk.data_dir.join("registry");
    let storage = JournalMatchmakerStorage::new(
        TokioStorageProvider::new(),
        dir.to_string_lossy().into_owned(),
        JournalStoreConfig::default(),
    );
    tracing::info!(id = me.id, %addr, dir = %dir.display(), "parosd_matchmaker");
    Ok(run_matchmaker(
        TokioProviders::new(),
        storage,
        disk.boot(),
        addr,
        topology.matchmaker_config(MatchmakerId(me.id)),
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
        &NoAudit,
    )
    .await)
}

async fn proxy(me: Identity, topology: Topology) -> Started {
    let rank = topology
        .proxy_rank(me.id)
        .ok_or_else(|| format!("proxy {} is not in --proxy", me.id))?;
    let addr = listen_addr(&me, "proxy", &topology.proxies)?;
    tracing::info!(id = me.id, %addr, "parosd_proxy");
    Ok(run_proxy(
        TokioProviders::new(),
        addr,
        topology.proxy_config(rank),
        topology.node_book(),
        topology.replica_book(),
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
        &NoAudit,
    )
    .await)
}

async fn replica(me: Identity, disk: Disk, topology: Topology) -> Started {
    // `validate` keeps replica ids out of the pool, so a named replica never
    // reaches the core's boot assert that it is not an acceptor.
    if !topology.replicas.iter().any(|r| r.id == me.id) {
        return Err(format!("replica {} is not in --replica", me.id));
    }
    let addr = listen_addr(&me, "replica", &topology.replicas)?;
    let dir = journal_dir(&disk.data_dir, topology.deployment_journal());
    let storage = JournalStorage::new(
        TokioStorageProvider::new(),
        dir.to_string_lossy().into_owned(),
        topology.deployment_config(NodeId(me.id)),
        JournalStoreConfig::default(),
    );
    tracing::info!(id = me.id, %addr, dir = %dir.display(), "parosd_replica");
    Ok(run_replica(
        TokioProviders::new(),
        storage,
        addr,
        topology.node_book(),
        disk.boot(),
        DriverTunables::default(),
        shutdown_on_signal(),
        &NoHooks,
        &NoAudit,
    )
    .await)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn the_default_journal_is_the_first_user_journal() {
        let cli =
            Cli::try_parse_from(["parosd", "proxy", "--id", "0", "--node", "0=127.0.0.1:4500"])
                .expect("parses");
        let Role::Proxy { topology, .. } = cli.role else {
            panic!("a proxy");
        };
        assert_eq!(topology.journals, vec![JournalId::FIRST_USER.0]);
    }
}
