//! `parosctl` — the paros CLI (#220), over `paros::client` (#221).
//!
//! The split follows etcd's (`etcd`, `etcdctl`, `clientv3`): `parosd`
//! serves, `parosctl` is the client, and `paros::client` is the library
//! both the CLI and the deterministic simulation's workload drive. The CLI
//! holds **no client policy of its own** — which server to ask, what a
//! redirect means, that a retry is the identical write, that a timeout is
//! ambiguous, how a writer claims a journal and how a reader resumes after
//! a truncation are all the library's — and adds only what a command line
//! needs: arguments, a Tokio runtime, output and exit codes.
//!
//! | exit | meaning |
//! |---|---|
//! | 0 | success |
//! | 3 | answered, and not what was asked: refused, lost, not served |
//! | 4 | ambiguous: a write (or another mutation) may or may not have happened |
//! | 5 | no server answered: nothing was decided |
//! | 2 | bad arguments (clap's own) |

mod commands;
mod init;
mod journal;
mod output;
#[path = "../../resolve.rs"]
mod resolve;
mod tenant;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use moonpool_core::TokioProviders;
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use paros::client::{Client, ClientTunables, bootstrap};

use crate::output::Printer;

/// The paros CLI: journal calls and operator calls against a deployment.
#[derive(Parser, Debug)]
#[command(name = "parosctl", version, about)]
struct Cli {
    #[command(flatten)]
    global: Global,
    #[command(subcommand)]
    command: Command,
}

/// Options every command takes.
#[derive(Args, Debug)]
struct Global {
    /// The servers to ask, comma-separated: `HOST:PORT` — a name that
    /// resolves to several machines (the seeds' rendezvous name) stands for
    /// them all, and each server's node id is learned from its own
    /// `Inspect` — or `ID=HOST:PORT` to name the id outright. A host is an
    /// IP or a name, resolved once, at startup.
    #[arg(long, env = "PAROSCTL_SERVERS", value_delimiter = ',', global = true)]
    servers: Vec<ServerArg>,
    /// Print JSON, one document per answer, instead of text.
    #[arg(long, global = true)]
    json: bool,
    /// How long one call waits for its answer, in milliseconds.
    #[arg(long, default_value = "5000", global = true)]
    timeout_ms: u64,
}

/// One server entry: an explicit node id, or the addresses a name resolves
/// to.
#[derive(Clone, Debug)]
struct ServerArg {
    id: Option<u64>,
    addrs: Vec<SocketAddr>,
}

impl FromStr for ServerArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('=') {
            Some((id, addr)) => Ok(Self {
                id: Some(
                    id.trim()
                        .parse()
                        .map_err(|e| format!("bad id in {s:?}: {e}"))?,
                ),
                addrs: vec![
                    resolve::resolve(addr.trim())
                        .map_err(|e| format!("bad address in {s:?}: {e}"))?,
                ],
            }),
            None => Ok(Self {
                id: None,
                addrs: resolve::resolve_all(s.trim())
                    .map_err(|e| format!("bad address in {s:?}: {e}"))?,
            }),
        }
    }
}

impl Global {
    /// Every address named, in order, without duplicates.
    fn addrs(&self) -> Vec<SocketAddr> {
        let mut addrs = Vec::new();
        for addr in self.servers.iter().flat_map(|s| s.addrs.iter().copied()) {
            if !addrs.contains(&addr) {
                addrs.push(addr);
            }
        }
        addrs
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Form the cell over its seeds and register it in the fleet (#196,
    /// #216, #229): sent to the first server, a waiting seed; then the first
    /// cell coordinator claims the cell control journal and registers the
    /// cell in meta. Refused on an initialized cell; a re-run resumes.
    Init(init::InitArgs),
    /// A call to a formed cell.
    #[command(flatten)]
    Cell(CellCommand),
}

/// The calls to a formed cell, each through a client of its servers.
#[derive(Subcommand, Debug)]
enum CellCommand {
    /// Write records at the journal's tail, claiming it first if needed.
    Write(commands::WriteArgs),
    /// Read records from a position.
    Read(commands::ReadArgs),
    /// Follow the journal until interrupted.
    Tail(commands::TailArgs),
    /// Truncate the journal below a position.
    Truncate(commands::TruncateArgs),
    /// Compare-and-swap the journal's writer.
    SetLeader(commands::SetLeaderArgs),
    /// Show each server's view: leader, ballot, configuration, chosen
    /// index, floor, GC watermark and what it may retire.
    Inspect(commands::InspectArgs),
    /// Ask the leader to change the acceptor set.
    Reconfigure(commands::ReconfigureArgs),
    /// Retire a node the GC floor released.
    Retire(commands::RetireArgs),
    /// The fleet's tenants, through meta (#229).
    #[command(subcommand)]
    Tenant(tenant::TenantCommand),
    /// A tenant's journals, through its control journal (#210).
    #[command(subcommand)]
    Journal(journal::JournalCommand),
}

/// How a command ended, as an exit code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ending {
    /// Done.
    Success,
    /// Answered, and not what was asked.
    Refused,
    /// May or may not have happened.
    Ambiguous,
    /// Nobody answered: nothing happened.
    Unreachable,
}

impl From<Ending> for ExitCode {
    fn from(ending: Ending) -> Self {
        match ending {
            Ending::Success => ExitCode::SUCCESS,
            Ending::Refused => ExitCode::from(3),
            Ending::Ambiguous => ExitCode::from(4),
            Ending::Unreachable => ExitCode::from(5),
        }
    }
}

/// The client-only RPC runtime, driven on a task of its own for the life
/// of the process.
struct Runtime {
    providers: TokioProviders,
    rpc: RpcHandle<TokioProviders>,
}

fn runtime() -> Result<Runtime, String> {
    let config = RpcConfig {
        max_frame_bytes: paros::MAX_FRAME_BYTES,
        ..RpcConfig::default()
    };
    let providers = TokioProviders::new();
    let (driver, rpc) = RpcDriver::client_only(providers.clone(), config)
        .map_err(|e| format!("client RPC runtime: {e}"))?;
    tokio::spawn(async move {
        let error = driver.run().await;
        tracing::warn!(%error, "client RPC runtime failed");
    });
    Ok(Runtime { providers, rpc })
}

/// The library client of `servers` (id and address each).
fn client(
    runtime: &Runtime,
    servers: &[(u64, SocketAddr)],
    timeout: Duration,
) -> Client<TokioProviders> {
    let tunables = ClientTunables {
        request_timeout: timeout,
        read_timeout: timeout,
        ..ClientTunables::default()
    };
    Client::connect(&runtime.providers, &runtime.rpc, servers, tunables)
}

/// The servers `global` names, each with its node id: an explicit one, or
/// the one its own `Inspect` reports (#196: ids are random). A server that
/// does not answer is left out.
async fn servers(runtime: &Runtime, global: &Global) -> Vec<(u64, SocketAddr)> {
    let mut known = Vec::new();
    let mut unknown = Vec::new();
    for server in &global.servers {
        match server.id {
            Some(id) => known.push((id, server.addrs[0])),
            None => unknown.extend(server.addrs.iter().copied()),
        }
    }
    let found =
        bootstrap::discover(&runtime.providers, &runtime.rpc, &unknown, global.timeout()).await;
    known.extend(found);
    known
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    if cli.global.servers.is_empty() {
        eprintln!("parosctl: no servers: pass --servers or set PAROSCTL_SERVERS");
        return ExitCode::FAILURE;
    }
    let runtime = match runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("parosctl: {error}");
            return ExitCode::FAILURE;
        }
    };
    let out = Printer::new(cli.global.json);
    let command = match cli.command {
        Command::Init(args) => {
            let timeout = cli.global.timeout();
            let addrs = cli.global.addrs();
            let connect = |servers: &[(u64, SocketAddr)]| client(&runtime, servers, timeout);
            return init::run(
                &runtime.providers,
                &runtime.rpc,
                &addrs,
                connect,
                &out,
                args,
            )
            .await
            .into();
        }
        Command::Cell(command) => command,
    };
    let servers = servers(&runtime, &cli.global).await;
    if servers.is_empty() {
        eprintln!("parosctl: no server answered with its node id");
        return Ending::Unreachable.into();
    }
    let client = client(&runtime, &servers, cli.global.timeout());
    let ending = match command {
        CellCommand::Write(args) => commands::write(&client, &out, args).await,
        CellCommand::Read(args) => commands::read(&client, &out, args).await,
        CellCommand::Tail(args) => commands::tail(&client, &out, args).await,
        CellCommand::Truncate(args) => commands::truncate(&client, &out, args).await,
        CellCommand::SetLeader(args) => commands::set_leader(&client, &out, args).await,
        CellCommand::Inspect(args) => commands::inspect(&client, &out, args).await,
        CellCommand::Reconfigure(args) => commands::reconfigure(&client, &out, args).await,
        CellCommand::Retire(args) => commands::retire(&client, &out, args).await,
        CellCommand::Tenant(command) => {
            tenant::run(&runtime.providers, &client, &out, command).await
        }
        CellCommand::Journal(command) => {
            journal::run(&runtime.providers, &client, &out, command).await
        }
    };
    ending.into()
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }
}
