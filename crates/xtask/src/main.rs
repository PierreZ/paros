//! Build automation for paros: the sancov-instrumented simulation runner
//! (mirrors moonpool's `xtask`) and the mutation hunt (`cargo xtask mutants`,
//! #269).
//!
//! The runner machinery (`run_binaries`) sets `SANCOV_CRATES` and a separate
//! `--target-dir target/sancov` so cargo doesn't serve a cached
//! non-instrumented build, and builds in release mode: a debug build runs the
//! whole simulation many times slower. `SIM_BINARIES` lists the
//! deterministic-simulation binaries to drive under coverage.

use std::collections::BTreeSet;
use std::process::{self, Command};
use std::time::Instant;

/// A simulation binary with its name and the crates to instrument with sancov.
struct SimBinary {
    name: &'static str,
    sancov_crates: &'static str,
}

impl SimBinary {
    /// Display name without the `sim-` prefix.
    fn display_name(&self) -> &str {
        self.name.strip_prefix("sim-").unwrap_or(self.name)
    }
}

/// Registry of simulation binaries instrumented for coverage-guided runs.
const SIM_BINARIES: &[SimBinary] = &[SimBinary {
    name: "sim-paros-chain",
    // The shipped library is the system under test: the sans-IO state machine plus
    // the provider-generic driver. NOT `paros_sim` — that is the test harness
    // (oracles, workload, fault world), and instrumenting it would inflate the edge
    // denominator and misdirect coverage-guided exploration onto harness code.
    sancov_crates: "paros_core,paros",
}];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(std::string::String::as_str) {
        Some("sim") => sim_dispatch(&args[1..]),
        Some("mutants") => mutants(&args[1..]),
        Some("help" | "--help" | "-h") | None => print_usage(),
        Some(cmd) => {
            eprintln!("unknown command: {cmd}");
            print_usage();
            process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("Usage: cargo xtask <command>");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  sim       Simulation binary management");
    eprintln!("  mutants   Mutation-test paros-core with the simulation as the test");
    eprintln!();
    eprintln!("Run 'cargo xtask sim --help' for simulation subcommands.");
}

fn sim_dispatch(args: &[String]) {
    match args.first().map(std::string::String::as_str) {
        Some("list") => sim_list(&args[1..]),
        Some("run") => sim_run(&args[1..]),
        Some("run-all") => run_binaries(&filter_binaries(&[]), &[]),
        Some("help" | "--help" | "-h") | None => sim_help(),
        Some(cmd) => {
            eprintln!("unknown sim subcommand: {cmd}");
            sim_help();
            process::exit(1);
        }
    }
}

fn sim_help() {
    eprintln!("Usage: cargo xtask sim <subcommand>");
    eprintln!();
    eprintln!("Subcommands:");
    eprintln!("  list [filter...]     List simulation binaries");
    eprintln!("  run <filter...>      Run binaries matching filter(s)");
    eprintln!("  run-all              Run all simulation binaries");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  cargo xtask sim list");
    eprintln!("  cargo xtask sim run-all");
}

fn mutants_help() {
    eprintln!("Usage: cargo xtask mutants [--seeds N] [cargo-mutants args...]");
    eprintln!();
    eprintln!("Runs cargo-mutants over the paros-core modules .cargo/mutants.toml scopes,");
    eprintln!("with a fixed-seed hunt of the main campaign as the test (#269): a mutant is");
    eprintln!("caught when the hunt reports a violation. --seeds sets the hunt's seed count");
    eprintln!("(default: paros_sim::MUTANT_SEEDS); every other argument goes to cargo-mutants.");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  cargo xtask mutants --list");
    eprintln!("  cargo xtask mutants --seeds 100 --file crates/paros-core/src/acceptor.rs");
    eprintln!("  cargo xtask mutants --shard 3/16 --jobs 2");
}

/// A mutant's hunt may take this many times the unmutated hunt before
/// cargo-mutants stops it as a timeout: a mutant that livelocks the
/// simulation counts as caught, and is not left to run for the job's lifetime.
const MUTANT_TIMEOUT_MULTIPLIER: u64 = 3;

/// The floor on that timeout, in seconds, for a tiny `--seeds`.
const MUTANT_TIMEOUT_FLOOR_SECS: u64 = 60;

/// The unmutated hunt, as `cargo test` arguments: the test cargo-mutants runs
/// for each mutant (`.cargo/mutants.toml`).
const MUTANT_HUNT: &[&str] = &[
    "test",
    "--profile",
    "release",
    "--package",
    "paros-sim-runner",
    "--features",
    "mutants",
    "--test",
    "mutants",
];

/// `cargo xtask mutants`: `cargo mutants` with the hunt's seed count in
/// `PAROS_MUTANT_SEEDS`. The scope, the test target and the release profile
/// live in `.cargo/mutants.toml`.
///
/// cargo-mutants' own baseline builds the mutated package rather than the
/// configured `test_package`, which has no `mutants` test, so this runs the
/// baseline itself: the unmutated hunt must be clean, and its time sizes the
/// per-mutant timeout passed with `--baseline skip`. A listing (`--list`,
/// `--list-files`) or an explicit `--baseline`/`--timeout` skips it.
fn mutants(args: &[String]) {
    if args
        .first()
        .is_some_and(|a| matches!(a.as_str(), "help" | "--help" | "-h"))
    {
        mutants_help();
        return;
    }
    let mut seeds = None;
    let mut forwarded = Vec::new();
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        if arg == "--seeds" {
            let Some(n) = rest.next().and_then(|n| n.parse::<u64>().ok()) else {
                eprintln!("--seeds needs a seed count");
                process::exit(2);
            };
            seeds = Some(n);
        } else {
            forwarded.push(arg.clone());
        }
    }
    let own_baseline = !forwarded.iter().any(|a| {
        a.starts_with("--list")
            || a.starts_with("--baseline")
            || a.starts_with("--timeout")
            || a == "-t"
    });
    let mut cmd = Command::new("cargo");
    cmd.arg("mutants");
    if let Some(n) = seeds {
        // Inherited by cargo-mutants' test runs.
        cmd.env("PAROS_MUTANT_SEEDS", n.to_string());
    }
    if own_baseline {
        let timeout = mutant_baseline(seeds) * MUTANT_TIMEOUT_MULTIPLIER;
        let timeout = timeout.max(MUTANT_TIMEOUT_FLOOR_SECS);
        eprintln!("per-mutant test timeout: {timeout}s");
        cmd.args(["--baseline", "skip", "--timeout", &timeout.to_string()]);
    }
    cmd.args(&forwarded);
    match cmd.status() {
        Ok(status) => process::exit(status.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("cargo mutants failed to launch ({e}): is cargo-mutants on the PATH?");
            process::exit(1);
        }
    }
}

/// Build and run the unmutated hunt; exit unless it is clean. Returns the
/// run's wall time in whole seconds, the build excluded.
fn mutant_baseline(seeds: Option<u64>) -> u64 {
    eprintln!("--- mutants: the unmutated baseline hunt ---");
    let hunt = || {
        let mut cmd = Command::new("cargo");
        cmd.args(MUTANT_HUNT);
        if let Some(n) = seeds {
            cmd.env("PAROS_MUTANT_SEEDS", n.to_string());
        }
        cmd
    };
    let build = hunt().arg("--no-run").status();
    if !build.is_ok_and(|s| s.success()) {
        eprintln!("the baseline hunt failed to build");
        process::exit(4);
    }
    let start = Instant::now();
    let run = hunt().status();
    let elapsed = start.elapsed();
    if !run.is_ok_and(|s| s.success()) {
        eprintln!("the unmutated hunt is red: no mutant can be judged against it");
        process::exit(4);
    }
    eprintln!("baseline hunt clean in {}", fmt_duration(elapsed));
    elapsed.as_secs().max(1)
}

/// Format a duration as a human-readable string.
fn fmt_duration(d: std::time::Duration) -> String {
    let total_ms = d.as_millis();
    if total_ms < 1000 {
        format!("{total_ms}ms")
    } else if total_ms < 60_000 {
        format!("{:.1}s", d.as_secs_f64())
    } else {
        let mins = d.as_secs() / 60;
        let secs = d.as_secs() % 60;
        format!("{mins}m {secs:02}s")
    }
}

/// The registered binaries matching any filter (all of them when there is
/// none); exits when nothing matches.
fn filter_binaries(filters: &[String]) -> Vec<&'static SimBinary> {
    let binaries: Vec<_> = SIM_BINARIES
        .iter()
        .filter(|b| filters.is_empty() || filters.iter().any(|f| b.name.contains(f.as_str())))
        .collect();
    if binaries.is_empty() {
        eprintln!("No binaries match filters: {filters:?}");
        process::exit(1);
    }
    binaries
}

fn sim_list(args: &[String]) {
    for bin in filter_binaries(args) {
        println!("{}", bin.display_name());
    }
}

fn sim_run(args: &[String]) {
    // Split on "--" to separate filter args from binary args.
    let (filter_args, binary_args) = match args.iter().position(|a| a == "--") {
        Some(pos) => (&args[..pos], &args[pos + 1..]),
        None => (args, [].as_slice()),
    };

    if filter_args.is_empty() {
        eprintln!("error: 'run' requires at least one filter argument");
        eprintln!();
        eprintln!("Usage: cargo xtask sim run <filter...> [-- <binary-args...>]");
        eprintln!("       cargo xtask sim run-all    (to run all binaries)");
        process::exit(1);
    }

    run_binaries(&filter_binaries(filter_args), binary_args);
}

/// Path under the sancov target dir where we stamp the active instrumentation set.
const SANCOV_STAMP: &str = "target/sancov/.sancov-crates";

/// Make `target/sancov` reflect `sancov_crates` before building.
///
/// `SANCOV_CRATES` is not part of cargo's fingerprint (that is why we use a
/// separate target dir at all), so changing *which* crates are instrumented does
/// not invalidate the cached, differently-instrumented artifacts — cargo would
/// silently serve a stale build. We stamp the active whitelist; when it changes we
/// `cargo clean` only the crates whose membership flipped (the symmetric
/// difference), so they rebuild with (or without) instrumentation and everything
/// else is left cached.
fn ensure_instrumentation_fresh(sancov_crates: &str) {
    let stamp = std::path::Path::new(SANCOV_STAMP);
    let prev = std::fs::read_to_string(stamp).unwrap_or_default();
    if prev == sancov_crates {
        return;
    }

    // Crate names use underscores in `SANCOV_CRATES`; cargo package specs use the
    // hyphenated package name. Normalize before diffing/cleaning.
    let to_pkgs = |s: &str| -> BTreeSet<String> {
        s.split(',')
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .map(|c| c.replace('_', "-"))
            .collect()
    };
    let flipped: Vec<String> = to_pkgs(&prev)
        .symmetric_difference(&to_pkgs(sancov_crates))
        .cloned()
        .collect();

    if !flipped.is_empty() {
        eprintln!(
            "SANCOV_CRATES changed ({prev:?} -> {sancov_crates:?}); cleaning {flipped:?} so they \
             rebuild with the right instrumentation"
        );
        let mut clean = Command::new("cargo");
        clean.args(["clean", "--release", "--target-dir", "target/sancov"]);
        for pkg in &flipped {
            clean.args(["-p", pkg]);
        }
        let _ = clean.status();
    }

    if let Some(dir) = stamp.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(stamp, sancov_crates);
}

fn run_binaries(binaries: &[&SimBinary], extra_args: &[String]) {
    eprintln!(
        "Running {} simulation binaries (sancov enabled)",
        binaries.len()
    );
    eprintln!();

    let total_start = Instant::now();
    let mut passed = Vec::new();
    let mut failed = Vec::new();

    for bin in binaries {
        eprintln!("--- {} ---", bin.display_name());
        ensure_instrumentation_fresh(bin.sancov_crates);
        let bin_start = Instant::now();

        let mut cmd = Command::new("cargo");
        cmd.args(["run", "--release", "--bin", bin.name]);

        cmd.env("SANCOV_CRATES", bin.sancov_crates);
        // Use a separate target dir so cargo doesn't serve a cached
        // non-instrumented build (SANCOV_CRATES isn't in cargo's fingerprint).
        cmd.args(["--target-dir", "target/sancov"]);

        if !extra_args.is_empty() {
            cmd.arg("--");
            cmd.args(extra_args);
        }

        let name = bin.display_name();
        let elapsed = || fmt_duration(bin_start.elapsed());
        match cmd.status() {
            Ok(status) if status.success() => {
                eprintln!("--- {name} --- ({})\n", elapsed());
                passed.push(name);
            }
            Ok(status) => {
                let code = status.code().unwrap_or(-1);
                eprintln!("{name}: exited with code {code} ({})\n", elapsed());
                failed.push(name);
            }
            Err(e) => {
                eprintln!("{name}: failed to launch: {e}\n");
                failed.push(name);
            }
        }
    }

    // Summary
    let total_elapsed = total_start.elapsed();
    eprintln!("=== Summary ===");
    eprintln!(
        "{} passed, {} failed, {} total ({})",
        passed.len(),
        failed.len(),
        binaries.len(),
        fmt_duration(total_elapsed),
    );
    if !failed.is_empty() {
        eprintln!("Failed:");
        for name in &failed {
            eprintln!("  {name}");
        }
        process::exit(1);
    }
}
