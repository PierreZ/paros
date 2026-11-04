//! `paros-bench` — the paros load generator (#517, #194 (throughput
//! benchmark), #724 (bench roles), #725 (bench phases)): writers, tailers
//! and readers on the journals of a tenant, closed or open loop, against
//! the servers or a local cell (`run --local N`); throughput and latency
//! per call each second, then the totals.
//!
//! The phases are subcommands: `init` fills the journals, `run` measures,
//! `clean` deletes what the bench made, `report` merges the `--json`
//! reports of several bench processes. They take the options `parosctl`
//! takes (`--servers`, `--timeout-ms`, the TLS options, `--json`) and their
//! own (`bench.rs`). The exit codes are `parosctl`'s, and 1 when a
//! `--expect` limit fails.

mod bench;

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use parosctl::support::connect::{Ending, Global, Runtime, client, connect, runtime};
use parosctl::support::output::note;

/// The paros load generator.
#[derive(Parser, Debug)]
#[command(name = "paros-bench", version, about)]
struct Cli {
    #[command(flatten)]
    global: Global,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Fill the bench journals with records, before a run.
    Init(Box<bench::InitArgs>),
    /// Measure: writers, tailers and readers on the bench journals.
    Run(Box<bench::RunArgs>),
    /// Delete the bench journals, and the tenant when the bench created it.
    Clean(bench::CleanArgs),
    /// Merge the `run --json` reports of several bench processes.
    Report(bench::report::ReportArgs),
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
    if let Command::Report(args) = cli.command {
        return report(&args, cli.global.json);
    }
    match compio::runtime::Runtime::new() {
        Ok(runtime) => runtime.block_on(online(cli.global, cli.command)),
        Err(error) => {
            eprintln!("paros-bench: runtime: {error}");
            ExitCode::FAILURE
        }
    }
}

/// `paros-bench report`: no cell needed.
fn report(args: &bench::report::ReportArgs, json: bool) -> ExitCode {
    let merged = match bench::report::read(&args.files).and_then(|r| bench::report::merge(&r)) {
        Ok(merged) => merged,
        Err(error) => {
            eprintln!("paros-bench: {error}");
            return Ending::Refused.into();
        }
    };
    if json {
        println!("{merged}");
    } else {
        bench::report::print(&merged);
    }
    let verdicts = args.expect.check(&merged);
    for verdict in &verdicts {
        note(&verdict.to_string());
    }
    if verdicts.iter().all(|v| v.passed) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// A phase against the servers `global` names, or `run` on a local cell.
async fn online(global: Global, command: Command) -> ExitCode {
    if let Command::Run(args) = &command
        && let Some(machines) = args.local
    {
        let Command::Run(args) = command else {
            unreachable!("matched above")
        };
        return local(&global, *args, machines).await;
    }
    if global.servers.is_empty() {
        eprintln!("paros-bench: no servers: pass --servers or set PAROSCTL_SERVERS");
        return ExitCode::FAILURE;
    }
    let runtime = match runtime(&global) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("paros-bench: {error}");
            return ExitCode::FAILURE;
        }
    };
    let (servers, client) = match connect(&runtime, &global, false).await {
        Ok(connected) => connected,
        Err(code) => return code,
    };
    let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
    phase(&runtime, &client, &ids, command, global.json).await
}

/// Run `command` against the cell `client` reaches.
async fn phase(
    runtime: &Runtime,
    client: &bench::Client,
    ids: &[u64],
    command: Command,
    json: bool,
) -> ExitCode {
    match command {
        Command::Init(args) => bench::init(runtime, client, ids, *args, json).await,
        Command::Run(args) => bench::run(runtime, client, ids, *args, json).await,
        Command::Clean(args) => bench::clean(runtime, client, ids, args, json).await,
        Command::Report(_) => unreachable!("report needs no cell"),
    }
}

/// `paros-bench run --local N` (#194 (throughput benchmark)): for each
/// topology, start a fresh cell of N machines in this process, form it,
/// run the bench against it, then stop it and delete its stores. A fresh
/// cell per topology keeps one topology's journals out of the next one's
/// numbers.
async fn local(global: &Global, args: bench::RunArgs, machines: u64) -> ExitCode {
    let runtime = match runtime(global) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("paros-bench: {error}");
            return ExitCode::FAILURE;
        }
    };
    let parent = args.local_dir.clone().unwrap_or_else(std::env::temp_dir);
    let plan = match bench::Plan::new(&runtime, args) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("paros-bench: {error}");
            return ExitCode::from(2);
        }
    };
    let patience: Duration = plan.patience();
    let mut runs = Vec::new();
    for topology in plan.topologies() {
        let cell = match bench::local::start(machines, &parent) {
            Ok(cell) => cell,
            Err(error) => {
                eprintln!("paros-bench: {error}");
                return ExitCode::FAILURE;
            }
        };
        note(&format!(
            "local cell of {machines} machines, stores in {}",
            cell.dir.display()
        ));
        let ran = if let Err(error) = cell.form(&runtime, patience).await {
            note(&error);
            Err(Ending::Unreachable)
        } else {
            match cell.servers(&runtime, global.timeout(), patience).await {
                Ok(servers) => {
                    let client = client(&runtime, &servers, global.timeout());
                    let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
                    let dir = cell.dir.clone();
                    plan.run_one(
                        &runtime,
                        (&client, &ids),
                        topology,
                        (global.json, Some(&dir)),
                    )
                    .await
                }
                Err(error) => {
                    note(&error);
                    Err(Ending::Unreachable)
                }
            }
        };
        if let Err(error) = cell.stop() {
            note(&format!("local cell: {error}"));
        }
        match ran {
            Ok(run) => runs.push(run),
            Err(ending) => return ending.into(),
        }
    }
    bench::finish(&runs, global.json)
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::Cli;

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn the_phases_are_subcommands() {
        for args in [
            &["paros-bench", "run", "--local", "3", "--duration", "5s"][..],
            &["paros-bench", "--json", "run", "--preset", "pub-sub"],
            &["paros-bench", "init", "--records", "100"],
            &["paros-bench", "clean", "--tenant", "t"],
            &["paros-bench", "report", "a.json", "b.json"],
        ] {
            assert!(Cli::try_parse_from(args).is_ok(), "{args:?}");
        }
        assert!(Cli::try_parse_from(["paros-bench", "report"]).is_err());
        assert!(Cli::try_parse_from(["paros-bench", "clean", "--local", "3"]).is_err());
    }
}
