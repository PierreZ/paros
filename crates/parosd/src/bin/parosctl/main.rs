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
mod entry;
mod fleet;
mod init;
mod journal;
mod key;
mod labels;
mod names;
mod output;
#[path = "../../resolve.rs"]
mod resolve;
mod token;
mod views;

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
    /// Ask a view as tenant `NAME`, not as an admin (#399): the cell shows
    /// that tenant's own spread only. Until tokens (#245), every caller is
    /// an admin and may narrow itself so.
    #[arg(long, value_name = "NAME", global = true)]
    as_tenant: Option<String>,
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
    /// Cells (#216, #399): `cell list`, `cell show [<cell>]`, and `cell
    /// add-machine <addr>`, which admits an idle machine into the cell of
    /// the servers.
    #[command(name = "cell")]
    CellAdmin(cell::CellArgs),
    /// Which references serve a tenant (#216): the cell, and its machines
    /// where the registry places them. Any machine of a cell answers.
    Resolve(entry::ResolveArgs),
    /// Root key pairs for Biscuit tokens, offline (#400): `key generate`,
    /// `key show`.
    Key(key::KeyArgs),
    /// Biscuit tokens, offline (#400): `token mint` with a root key,
    /// `token derive` (narrower, macaroon style, no key), `token inspect`.
    Token(token::TokenArgs),
    /// The cell's machines (#399): `machine list`, `machine show <name>`.
    Machine(views::MachineArgs),
    /// Who holds which role now (#399): the coordinators, every journal's
    /// acceptors and matchmakers, and the bookings; of the cell, a tenant
    /// or a machine.
    Roles(views::RolesArgs),
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
    /// Tenants (#229, #399): `tenant list`, `tenant show <tenant>`, and
    /// `tenant create|delete` through the universe directory.
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

/// Whether `command` is a view (#399): one request to the servers' cell,
/// no node ids.
fn is_view(command: &Command) -> bool {
    match command {
        Command::Machine(_) | Command::Roles(_) => true,
        Command::CellAdmin(args) => {
            !matches!(args.command, cell::CellAdminCommand::AddMachine { .. })
        }
        Command::Cell(CellCommand::Tenant(args)) => matches!(
            args.command,
            fleet::TenantCommand::List | fleet::TenantCommand::Show { .. }
        ),
        _ => false,
    }
}

/// Whether `command` prints machines, cells, tenants or journals: it
/// names them from one admin view first (#399).
fn prints_names(command: &CellCommand) -> bool {
    matches!(
        command,
        CellCommand::Inspect(_)
            | CellCommand::Reconfigure(_)
            | CellCommand::Retire(_)
            | CellCommand::Tenant(_)
            | CellCommand::Journal(_)
    )
}

/// A view (#399): one request to the servers' cell.
async fn run_view(asker: &views::Asker<'_>, out: &Printer, command: Command) -> Ending {
    match command {
        Command::Machine(args) => views::machine(asker, out, args).await,
        Command::Roles(args) => views::roles_cmd(asker, out, args).await,
        Command::CellAdmin(args) => match args.command {
            cell::CellAdminCommand::List => views::cell_list(asker, out).await,
            cell::CellAdminCommand::Show { cell } => {
                views::cell_show(asker, out, cell.as_deref()).await
            }
            cell::CellAdminCommand::AddMachine { .. } => unreachable!("not a view"),
        },
        Command::Cell(CellCommand::Tenant(args)) => match args.command {
            fleet::TenantCommand::Show { name } => views::tenant_show(asker, out, &name).await,
            _ => views::tenant_list(asker, out).await,
        },
        _ => unreachable!("only the views"),
    }
}

/// The view asker over `global`'s servers, in its `--as-tenant` scope.
fn asker<'a>(runtime: &'a Runtime, global: &Global) -> views::Asker<'a> {
    views::Asker {
        providers: &runtime.providers,
        rpc: &runtime.rpc,
        names: &runtime.names,
        servers: global.addrs(),
        scope: match &global.as_tenant {
            Some(tenant) => paros::view::Scope::Tenant(tenant.as_bytes().to_vec()),
            None => paros::view::Scope::Admin,
        },
        timeout: global.timeout(),
    }
}

/// `parosctl resolve`: the servers are the entry endpoint.
async fn resolve_tenant(
    runtime: &Runtime,
    global: &Global,
    out: &Printer,
    args: entry::ResolveArgs,
) -> ExitCode {
    let entry = global.addrs();
    entry::run(
        &runtime.providers,
        &runtime.rpc,
        &runtime.names,
        &entry,
        global.timeout(),
        out,
        args,
    )
    .await
    .into()
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
    let asker = asker(&runtime, &cli.global);
    if is_view(&cli.command) {
        return run_view(&asker, &out, cli.command).await.into();
    }
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
            return init::run(&members, connect, &out, &asker, &args)
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
            let labels = labels::Labels::fetch(&asker).await;
            return cell::run(&asker, &client, &servers, &labels, &out, args)
                .await
                .into();
        }
        Command::Resolve(args) => return resolve_tenant(&runtime, &cli.global, &out, args).await,
        Command::Cell(command) => command,
        Command::Key(_) | Command::Token(_) => unreachable!("offline, handled in main"),
        Command::Machine(_) | Command::Roles(_) => unreachable!("views, handled above"),
    };
    let servers = servers(&runtime, &cli.global).await;
    if servers.is_empty() {
        eprintln!("parosctl: no server answered with its node id");
        return Ending::Unreachable.into();
    }
    let client = client(&runtime, &servers, cli.global.timeout());
    // Names for what the command prints (#399): one admin view.
    let labels = if prints_names(&command) {
        labels::Labels::fetch(&asker).await
    } else {
        labels::Labels::default()
    };
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
        CellCommand::Inspect(args) => commands::inspect(&client, &labels, &out, args).await,
        CellCommand::Reconfigure(args) => commands::reconfigure(&client, &labels, &out, args).await,
        CellCommand::Retire(args) => commands::retire(&client, &labels, &out, args).await,
        CellCommand::Tenant(args) => {
            let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
            fleet::run(&runtime.providers, &client, &ids, &labels, &out, args).await
        }
        CellCommand::Journal(args) => {
            journal::run(
                &runtime.providers,
                &runtime.rpc,
                &runtime.names,
                &client,
                &labels,
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
