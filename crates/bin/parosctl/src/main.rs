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

use std::process::ExitCode;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use paros::model::address::Address;
use parosctl::cmd::{
    cell, data, init, journal, key, machine, resolve, roles, status, store, tenant, tls, token,
    universe,
};
use parosctl::support::connect::{
    Ending, Global, Runtime, asker, client, connect, runtime, servers,
};
use parosctl::support::labels::Labels;
use parosctl::support::output::Printer;
use parosctl::support::view::Asker;

/// The paros CLI: journal calls and operator calls against a deployment.
#[derive(Parser, Debug)]
#[command(name = "parosctl", version, about)]
struct Cli {
    #[command(flatten)]
    global: Global,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Form the cell over its founding members with `cell init`'s decree
    /// (#196, #216, #277); then the universe steps register the cell in the
    /// universe directory (#229), and `init` reads the cell control journal
    /// and waits for the cell coordinator to join the cell (#513). Refused
    /// on an initialized universe; a re-run resumes.
    Init(init::InitArgs),
    /// Cells (#216, #399): `cell list`, `cell show [<cell>]`, and `cell
    /// add-machine <addr>`, which admits an idle machine into the cell of
    /// the servers.
    #[command(name = "cell")]
    CellAdmin(cell::CellArgs),
    /// Which references serve a tenant (#216): the cell, and its machines
    /// where the registry places them. Any machine of a cell answers.
    Resolve(resolve::ResolveArgs),
    /// Root key pairs for Biscuit tokens, offline (#400): `key generate`,
    /// `key show`.
    Key(key::KeyArgs),
    /// Biscuit tokens, offline (#400): `token mint` with a root key,
    /// `token derive` (narrower, macaroon style, no key), `token inspect`.
    Token(token::TokenArgs),
    /// The universe entry (#245 (Biscuit tokens)): `universe key list`,
    /// `universe key add <file.public>`, `universe key remove <label>`, the
    /// root public keys every token is checked against.
    Universe(universe::UniverseArgs),
    /// TLS certificates, offline: `tls init` writes the authority and a
    /// certificate per machine, frontend and admin in one step; `tls ca`,
    /// `tls issue`, `tls show`, `tls verify`.
    Tls(tls::TlsArgs),
    /// A machine's disk, offline (#365): `store inspect <data-dir>` reads
    /// the record, every journal store and every matchmaker set without
    /// starting `parosd`, and never writes to it.
    Store(store::StoreArgs),
    /// The cell's machines (#399): `machine list`, `machine show <name>`.
    Machine(machine::MachineArgs),
    /// Who holds which role now (#399): the coordinators, every journal's
    /// acceptors and matchmakers, and the slots each machine reserves; of the cell, a tenant
    /// or a machine.
    Roles(roles::RolesArgs),
    /// How the universe, the cell or a tenant stands (#191 (status)):
    /// desired, available and current, each journal's word (healthy,
    /// degraded, unavailable) and the slots per class.
    Status(status::StatusArgs),
    /// A call to a formed cell.
    #[command(flatten)]
    Cell(CellCommand),
}

/// The calls to a formed cell, each through a client of its servers.
#[derive(Subcommand, Debug)]
enum CellCommand {
    /// Write records at the journal's tail, claiming it first if needed.
    Write(data::WriteArgs),
    /// Read records from a position.
    Read(data::ReadArgs),
    /// Follow the journal until interrupted.
    Tail(data::TailArgs),
    /// Truncate the journal below a position.
    Truncate(data::TruncateArgs),
    /// Compare-and-swap the journal's writer.
    SetLeader(data::SetLeaderArgs),
    /// Show each server's view: leader, ballot, configuration, chosen
    /// index, floor, GC watermark and what it may retire.
    Inspect(data::InspectArgs),
    /// Ask the leader to change the acceptor set.
    Reconfigure(data::ReconfigureArgs),
    /// Retire a node the GC floor released.
    Retire(data::RetireArgs),
    /// Tenants (#229, #399): `tenant list`, `tenant show <tenant>`, and
    /// `tenant create|delete` through the universe directory.
    Tenant(tenant::TenantArgs),
    /// Create, delete and list a tenant's journals through its control
    /// journal and the tenant coordinator (#210).
    Journal(journal::JournalArgs),
    /// FDB's `configure` (#214 (tenant modes)): a new desired mode for a
    /// tenant's journal (`--journal`) or for its default; the tenant
    /// coordinator applies a journal's mode by reconfiguration.
    Configure(journal::ConfigureArgs),
}

/// Whether `command` is a journal call a frontend serves.
fn through_frontend(command: &CellCommand) -> bool {
    matches!(
        command,
        CellCommand::Write(_)
            | CellCommand::Read(_)
            | CellCommand::Tail(_)
            | CellCommand::Truncate(_)
            | CellCommand::SetLeader(_)
    ) || matches!(command, CellCommand::Journal(args) if args.is_tag())
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
        )
        .with_writer(std::io::stderr)
        .init();
    // The help cites no issue (#399): people read it.
    let matches = parosctl::support::help::for_people(Cli::command()).get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    // The key, token, TLS and store commands are offline: no server, no
    // client runtime.
    let out = Printer::new(cli.global.json);
    match cli.command {
        Command::Key(args) => key::run(&out, args).into(),
        Command::Token(args) => token::run(&out, args).into(),
        Command::Tls(args) => tls::run(&out, args).into(),
        Command::Store(args) => store::run(&out, args).into(),
        command => match compio::runtime::Runtime::new() {
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
        Command::Machine(_) | Command::Roles(_) | Command::Status(_) => true,
        Command::CellAdmin(args) => !matches!(
            args.command,
            cell::CellAdminCommand::AddMachine { .. }
                | cell::CellAdminCommand::DrainMachine { .. }
                | cell::CellAdminCommand::RemoveMachine { .. }
                | cell::CellAdminCommand::Maintenance { .. }
                | cell::CellAdminCommand::TagMachine { .. }
                | cell::CellAdminCommand::Tag { .. }
                | cell::CellAdminCommand::Configure { .. }
        ),
        Command::Cell(CellCommand::Tenant(args)) => matches!(
            args.command,
            tenant::TenantCommand::List { .. } | tenant::TenantCommand::Show { .. }
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
async fn run_view(asker: &Asker<'_>, out: &Printer, command: Command) -> Ending {
    match command {
        Command::Machine(args) => machine::machine(asker, out, args).await,
        Command::Roles(args) => roles::roles_cmd(asker, out, args).await,
        Command::Status(args) => status::run(asker, out, args).await,
        Command::CellAdmin(args) => match args.command {
            cell::CellAdminCommand::List { selector } => {
                cell::cell_list(asker, out, selector.as_deref()).await
            }
            cell::CellAdminCommand::Show { cell } => {
                cell::cell_show(asker, out, cell.as_deref()).await
            }
            cell::CellAdminCommand::AddMachine { .. }
            | cell::CellAdminCommand::DrainMachine { .. }
            | cell::CellAdminCommand::RemoveMachine { .. }
            | cell::CellAdminCommand::Maintenance { .. }
            | cell::CellAdminCommand::TagMachine { .. }
            | cell::CellAdminCommand::Tag { .. }
            | cell::CellAdminCommand::Configure { .. } => unreachable!("not a view"),
        },
        Command::Cell(CellCommand::Tenant(args)) => match args.command {
            tenant::TenantCommand::Show { name } => tenant::tenant_show(asker, out, &name).await,
            tenant::TenantCommand::List { selector } => {
                tenant::tenant_list(asker, out, selector.as_deref()).await
            }
            _ => unreachable!("not a view"),
        },
        _ => unreachable!("only the views"),
    }
}

/// `parosctl resolve`: the servers are the entry endpoint.
async fn resolve_tenant(
    runtime: &Runtime,
    global: &Global,
    out: &Printer,
    mut args: resolve::ResolveArgs,
) -> ExitCode {
    let entry = global.addrs();
    args.token = match global.token() {
        Ok(token) => token,
        Err(error) => {
            eprintln!("parosctl: {error}");
            return ExitCode::FAILURE;
        }
    };
    resolve::run(
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

/// The admin commands that write the cell or the universe directory under
/// an `admin` token (#245 (Biscuit tokens)): `init`, `cell`, `universe`.
async fn administer(
    runtime: &Runtime,
    global: &Global,
    asker: &Asker<'_>,
    out: &Printer,
    command: Command,
) -> ExitCode {
    let token = match global.token() {
        Ok(token) => token.unwrap_or_default(),
        Err(error) => {
            eprintln!("parosctl: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Command::Init(args) = command {
        let timeout = global.timeout();
        let members = match args.members(&global.addrs()) {
            Ok(members) => members,
            Err(error) => {
                eprintln!("parosctl: {error}");
                return ExitCode::FAILURE;
            }
        };
        let connect = |servers: &[(u64, Address)]| client(runtime, servers, timeout);
        return init::run(&members, connect, out, asker, &args, token)
            .await
            .into();
    }
    let servers = servers(runtime, global).await;
    if servers.is_empty() {
        eprintln!("parosctl: no server answered with its node id");
        return Ending::Unreachable.into();
    }
    let client = client(runtime, &servers, global.timeout());
    match command {
        Command::CellAdmin(args) => {
            let labels = Labels::fetch(asker).await;
            cell::run(asker, &client, &servers, &labels, out, args, token)
                .await
                .into()
        }
        Command::Universe(args) => {
            let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
            universe::run(asker, &client, &ids, out, args).await.into()
        }
        _ => unreachable!("only the admin commands are administered"),
    }
}

/// The commands that talk to servers.
async fn online(global: Global, command: Command) -> ExitCode {
    let cli = Cli { global, command };
    let fronted = !cli.global.frontends.is_empty()
        && matches!(&cli.command, Command::Cell(command) if through_frontend(command));
    if cli.global.servers.is_empty() && !fronted {
        eprintln!("parosctl: no servers: pass --servers or set PAROSCTL_SERVERS");
        return ExitCode::FAILURE;
    }
    let runtime = match runtime(&cli.global) {
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
        command @ (Command::Init(_) | Command::CellAdmin(_) | Command::Universe(_)) => {
            return administer(&runtime, &cli.global, &asker, &out, command).await;
        }
        Command::Resolve(args) => return resolve_tenant(&runtime, &cli.global, &out, args).await,
        Command::Cell(command) => command,
        Command::Key(_) | Command::Token(_) | Command::Tls(_) | Command::Store(_) => {
            unreachable!("offline, handled in main")
        }
        Command::Machine(_) | Command::Roles(_) | Command::Status(_) => {
            unreachable!("views, handled above")
        }
    };
    let (servers, client) = match connect(&runtime, &cli.global, fronted).await {
        Ok(connected) => connected,
        Err(code) => return code,
    };
    // Names for what the command prints (#399): one admin view.
    let labels = if prints_names(&command) {
        Labels::fetch(&asker).await
    } else {
        Labels::default()
    };
    let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
    let ending = match command {
        CellCommand::Write(args) => data::write(&runtime.providers, &client, &out, args).await,
        CellCommand::Read(args) => data::read(&client, &out, args).await,
        CellCommand::Tail(args) => data::tail(&client, &out, args).await,
        CellCommand::Truncate(args) => {
            data::truncate(&runtime.providers, &client, &out, args).await
        }
        CellCommand::SetLeader(args) => {
            data::set_leader(&runtime.providers, &client, &out, args).await
        }
        CellCommand::Inspect(args) => data::inspect((&client, &asker), &labels, &out, args).await,
        CellCommand::Reconfigure(args) => data::reconfigure(&client, &labels, &out, args).await,
        CellCommand::Retire(args) => data::retire(&client, &labels, &out, args).await,
        CellCommand::Tenant(args) => {
            tenant::run(&runtime.providers, &client, &ids, &labels, &out, args).await
        }
        CellCommand::Journal(args) if fronted => {
            journal::tag_through_frontends(&runtime.providers, &client, &out, args).await
        }
        CellCommand::Configure(args) => {
            journal::configure(
                &runtime.providers,
                &runtime.rpc,
                &runtime.names,
                &client,
                &out,
                args,
            )
            .await
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

    /// A subcommand flag named like a global one panics when the command
    /// line is read, which `debug_assert` does not see: `configure
    /// --frontends 1` did, beside the global `--frontends HOST:PORT`.
    #[test]
    fn a_tenant_frontend_count_reads_beside_the_global_frontends() {
        use clap::Parser as _;
        let cli = Cli::try_parse_from([
            "parosctl",
            "--frontends",
            "a:4500",
            "configure",
            "--tenant",
            "acme",
            "--frontend-count",
            "1",
        ]);
        assert!(cli.is_ok());
    }

    /// `journal create` and `configure` both read `--layout` and the scale
    /// counts (§3.4).
    #[test]
    fn create_and_configure_read_a_layout() {
        use clap::Parser as _;
        for line in [
            &[
                "parosctl",
                "journal",
                "create",
                "acme",
                "orders",
                "--layout",
                "compartmentalized",
                "--proxies",
                "3",
            ][..],
            &[
                "parosctl",
                "configure",
                "--tenant",
                "acme",
                "--journal",
                "orders",
                "--layout",
                "colocated",
            ][..],
        ] {
            assert!(Cli::try_parse_from(line).is_ok(), "{line:?}");
        }
        assert!(
            Cli::try_parse_from([
                "parosctl", "journal", "create", "acme", "orders", "--layout", "multi",
            ])
            .is_err()
        );
    }
}
