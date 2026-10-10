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

mod cell;
mod commands;
mod fleet;
mod init;
mod journal;
mod key;
mod names;
mod output;
#[path = "../../resolve.rs"]
mod resolve;
mod token;

use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use moonpool_core::{TokioProviders, TokioResolver};
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use paros::client::{Client, ClientTunables, bootstrap};
use paros::{Address, Names};

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
    /// resolves to several machines (a Compose alias) stands for
    /// them all, and each server's node id is learned from its own
    /// `Inspect` — or `ID=HOST:PORT` to name the id outright (its 16 hex
    /// digits). A host is an IP or a name; a name of one machine is
    /// resolved each time it is dialed (#257).
    #[arg(long, env = "PAROSCTL_SERVERS", value_delimiter = ',', global = true)]
    servers: Vec<ServerArg>,
    /// Print JSON, one document per answer, instead of text.
    #[arg(long, global = true)]
    json: bool,
    /// How long one call waits for its answer, in milliseconds.
    #[arg(long, default_value = "5000", global = true)]
    timeout_ms: u64,
}

/// One server entry: an explicit node id, or the addresses a name stands
/// for.
#[derive(Clone, Debug)]
struct ServerArg {
    id: Option<u64>,
    addrs: Vec<Address>,
}

impl FromStr for ServerArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('=') {
            Some((id, addr)) => Ok(Self {
                id: Some(
                    names::parse_node_id(id.trim()).map_err(|e| format!("bad id in {s:?}: {e}"))?,
                ),
                addrs: vec![
                    Address::parse(addr.trim())
                        .map_err(|e| format!("bad address in {s:?}: {e}"))?,
                ],
            }),
            None => Ok(Self {
                id: None,
                addrs: resolve::expand(s.trim())
                    .map_err(|e| format!("bad address in {s:?}: {e}"))?,
            }),
        }
    }
}

impl Global {
    /// Every address named, in order, without duplicates.
    fn addrs(&self) -> Vec<Address> {
        let mut addrs = Vec::new();
        for addr in self.servers.iter().flat_map(|s| s.addrs.iter().cloned()) {
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
    /// Form the cell over its founding members with `cell init`'s decree
    /// (#196, #216, #277); then `init` claims the cell control journal under a
    /// leader uuid of its own, and the fleet steps register the cell in the fleet directory
    /// (#229). Refused on an initialized fleet; a re-run resumes.
    Init(init::InitArgs),
    /// Cell administration (#216): `cell add-machine <addr>` admits an idle
    /// machine into the cell of the servers.
    #[command(name = "cell")]
    CellAdmin(cell::CellArgs),
    /// Root key pairs for Biscuit tokens, offline (#400): `key generate`,
    /// `key show`.
    Key(key::KeyArgs),
    /// Biscuit tokens, offline (#400): `token mint` with a root key,
    /// `token derive` (narrower, macaroon style, no key), `token inspect`.
    Token(token::TokenArgs),
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
    /// Create, delete and list tenants through the fleet directory (#229).
    Tenant(fleet::TenantArgs),
    /// Create, delete and list a tenant's journals through its control
    /// journal and the tenant coordinator (#210).
    Journal(journal::JournalArgs),
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
    /// The operating system's resolver: names are resolved as they are
    /// dialed (#257).
    names: Names,
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
    Ok(Runtime {
        providers,
        rpc,
        names: Names::new(TokioResolver::new()),
    })
}

/// The library client of `servers` (id and address each).
fn client(
    runtime: &Runtime,
    servers: &[(u64, Address)],
    timeout: Duration,
) -> Client<TokioProviders> {
    let tunables = ClientTunables {
        request_timeout: timeout,
        read_timeout: timeout,
        ..ClientTunables::default()
    };
    Client::connect_named(
        &runtime.providers,
        &runtime.rpc,
        &runtime.names,
        servers,
        tunables,
    )
}

/// The servers `global` names, each with its node id: an explicit one, or
/// the one its own `Inspect` reports (#196: ids are random). A server that
/// does not answer is left out.
async fn servers(runtime: &Runtime, global: &Global) -> Vec<(u64, Address)> {
    let mut known = Vec::new();
    let mut unknown = Vec::new();
    for server in &global.servers {
        match server.id {
            Some(id) => known.push((id, server.addrs[0].clone())),
            None => unknown.extend(server.addrs.iter().cloned()),
        }
    }
    let found = bootstrap::discover(
        &runtime.providers,
        &runtime.rpc,
        &runtime.names,
        &unknown,
        global.timeout(),
    )
    .await;
    known.extend(found);
    known
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    // The key and token commands are offline: no server, no runtime.
    let out = Printer::new(cli.global.json);
    match cli.command {
        Command::Key(args) => key::run(&out, args).into(),
        Command::Token(args) => token::run(&out, args).into(),
        command => match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(online(cli.global, command)),
            Err(error) => {
                eprintln!("parosctl: runtime: {error}");
                ExitCode::FAILURE
            }
        },
    }
}

/// The commands that talk to servers.
async fn online(global: Global, command: Command) -> ExitCode {
    let cli = Cli { global, command };
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
            let members = match args.members(&cli.global.addrs()) {
                Ok(members) => members,
                Err(error) => {
                    eprintln!("parosctl: {error}");
                    return ExitCode::FAILURE;
                }
            };
            let connect = |servers: &[(u64, Address)]| client(&runtime, servers, timeout);
            return init::run(
                &runtime.providers,
                &runtime.rpc,
                &runtime.names,
                &members,
                connect,
                &out,
                &args,
            )
            .await
            .into();
        }
        Command::CellAdmin(args) => {
            let servers = servers(&runtime, &cli.global).await;
            if servers.is_empty() {
                eprintln!("parosctl: no server answered with its node id");
                return Ending::Unreachable.into();
            }
            let client = client(&runtime, &servers, cli.global.timeout());
            return cell::run(
                &runtime.providers,
                &runtime.rpc,
                &runtime.names,
                &client,
                &servers,
                &out,
                args,
            )
            .await
            .into();
        }
        Command::Cell(command) => command,
        Command::Key(_) | Command::Token(_) => unreachable!("offline, handled in main"),
    };
    let servers = servers(&runtime, &cli.global).await;
    if servers.is_empty() {
        eprintln!("parosctl: no server answered with its node id");
        return Ending::Unreachable.into();
    }
    let client = client(&runtime, &servers, cli.global.timeout());
    let ending = match command {
        CellCommand::Write(args) => commands::write(&runtime.providers, &client, &out, args).await,
        CellCommand::Read(args) => commands::read(&client, &out, args).await,
        CellCommand::Tail(args) => commands::tail(&client, &out, args).await,
        CellCommand::Truncate(args) => {
            commands::truncate(&runtime.providers, &client, &out, args).await
        }
        CellCommand::SetLeader(args) => {
            commands::set_leader(&runtime.providers, &client, &out, args).await
        }
        CellCommand::Inspect(args) => commands::inspect(&client, &out, args).await,
        CellCommand::Reconfigure(args) => commands::reconfigure(&client, &out, args).await,
        CellCommand::Retire(args) => commands::retire(&client, &out, args).await,
        CellCommand::Tenant(args) => {
            let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
            fleet::run(&runtime.providers, &client, &ids, &out, args).await
        }
        CellCommand::Journal(args) => {
            journal::run(
                &runtime.providers,
                &runtime.rpc,
                &runtime.names,
                &client,
                &out,
                args,
            )
            .await
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
