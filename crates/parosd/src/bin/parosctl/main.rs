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
mod output;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use moonpool_core::TokioProviders;
use moonpool_rpc::{RpcConfig, RpcDriver};
use paros::client::{Client, ClientTunables};

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
    /// The servers to ask, comma-separated: `ID=HOST:PORT` (the node id a
    /// leader hint names it by) or `HOST:PORT` (its position in the list).
    #[arg(long, env = "PAROSCTL_SERVERS", value_delimiter = ',', global = true)]
    servers: Vec<ServerArg>,
    /// Print JSON, one document per answer, instead of text.
    #[arg(long, global = true)]
    json: bool,
    /// How long one call waits for its answer, in milliseconds.
    #[arg(long, default_value = "5000", global = true)]
    timeout_ms: u64,
}

/// One server: its node id and address.
#[derive(Clone, Debug)]
struct ServerArg {
    id: Option<u64>,
    addr: SocketAddr,
}

impl FromStr for ServerArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id, addr) = match s.split_once('=') {
            Some((id, addr)) => (
                Some(
                    id.trim()
                        .parse()
                        .map_err(|e| format!("bad id in {s:?}: {e}"))?,
                ),
                addr,
            ),
            None => (None, s),
        };
        let addr = paros::parse_addr(addr.trim())
            .map_err(|e| format!("bad address in {s:?}: {e}"))?
            .parse()
            .map_err(|e| format!("bad address in {s:?}: {e}"))?;
        Ok(Self { id, addr })
    }
}

#[derive(Subcommand, Debug)]
enum Command {
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

/// The library client of `global.servers`, over a client-only RPC runtime
/// driven on a task of its own for the life of the process.
fn connect(global: &Global) -> Result<Client<TokioProviders>, String> {
    if global.servers.is_empty() {
        return Err("no servers: pass --servers or set PAROSCTL_SERVERS".into());
    }
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
    let servers: Vec<(u64, SocketAddr)> = global
        .servers
        .iter()
        .zip(0_u64..)
        .map(|(server, position)| (server.id.unwrap_or(position), server.addr))
        .collect();
    let timeout = Duration::from_millis(global.timeout_ms);
    let tunables = ClientTunables {
        request_timeout: timeout,
        read_timeout: timeout,
        ..ClientTunables::default()
    };
    Ok(Client::connect(&providers, &rpc, &servers, tunables))
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
    let client = match connect(&cli.global) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("parosctl: {error}");
            return ExitCode::FAILURE;
        }
    };
    let out = Printer::new(cli.global.json);
    let ending = match cli.command {
        Command::Write(args) => commands::write(&client, &out, args).await,
        Command::Read(args) => commands::read(&client, &out, args).await,
        Command::Tail(args) => commands::tail(&client, &out, args).await,
        Command::Truncate(args) => commands::truncate(&client, &out, args).await,
        Command::SetLeader(args) => commands::set_leader(&client, &out, args).await,
        Command::Inspect(args) => commands::inspect(&client, &out, args).await,
        Command::Reconfigure(args) => commands::reconfigure(&client, &out, args).await,
        Command::Retire(args) => commands::retire(&client, &out, args).await,
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
