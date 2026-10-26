//! `paros-bench` — the paros load generator (#517, #194 (throughput
//! benchmark)): clients writing and reading the journals of a tenant,
//! closed or open loop, against the servers or a local cell (`--local N`);
//! throughput and latency per call each second, then the totals.
//!
//! It takes the options `parosctl` takes (`--servers`, `--timeout-ms`, the
//! TLS options, `--json`) and the bench's own (`bench.rs`). The exit codes
//! are `parosctl`'s.

mod bench;

use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use parosctl::support::connect::{Ending, Global, client, connect, runtime};
use parosctl::support::output::note;

/// The paros load generator.
#[derive(Parser, Debug)]
#[command(name = "paros-bench", version, about)]
struct Cli {
    #[command(flatten)]
    global: Global,
    #[command(flatten)]
    args: bench::BenchArgs,
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
    match compio::runtime::Runtime::new() {
        Ok(runtime) => runtime.block_on(online(cli.global, cli.args)),
        Err(error) => {
            eprintln!("paros-bench: runtime: {error}");
            ExitCode::FAILURE
        }
    }
}

/// The bench against the servers `global` names, or a local cell.
async fn online(global: Global, args: bench::BenchArgs) -> ExitCode {
    if let Some(machines) = args.local {
        return local(&global, args, machines).await;
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
    bench::run(&runtime, &client, &ids, args, global.json)
        .await
        .into()
}

/// `paros-bench --local N` (#194 (throughput benchmark)): for each
/// topology, start a fresh cell of N machines in this process, form it, run
/// the bench against it, then stop it and delete its stores. A fresh cell
/// per topology keeps one topology's journals out of the next one's numbers.
async fn local(global: &Global, args: bench::BenchArgs, machines: u64) -> ExitCode {
    let runtime = match runtime(global) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("paros-bench: {error}");
            return ExitCode::FAILURE;
        }
    };
    let parent = args.local_dir.clone().unwrap_or_else(std::env::temp_dir);
    let patience = Duration::from_millis(args.patience_ms());
    let plan = match bench::Plan::new(&runtime, args) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("paros-bench: {error}");
            return ExitCode::from(2);
        }
    };
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
    bench::finish(&runs, global.json).into()
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
