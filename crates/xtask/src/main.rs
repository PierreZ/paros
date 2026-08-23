//! Build automation for paros. Currently hosts the sancov-instrumented
//! simulation runner. Mirrors moonpool's `xtask`.
//!
//! The runner machinery (`run_binaries`) sets `SANCOV_CRATES` and a separate
//! `--target-dir target/sancov` so cargo doesn't serve a cached
//! non-instrumented build. `SIM_BINARIES` lists the deterministic-simulation
//! binaries to drive under coverage.

use std::collections::HashSet;
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
    name: "paros-sim-runner",
    // The shipped library is the system under test: the sans-IO state machine plus
    // the provider-generic driver. NOT `paros_sim` — that is the test harness
    // (oracles, workload, viz serde), and instrumenting it would inflate the edge
    // denominator and misdirect coverage-guided exploration onto harness code.
    sancov_crates: "paros_core,paros",
}];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(std::string::String::as_str) {
        Some("sim") => sim_dispatch(&args[1..]),
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
    eprintln!("  sim   Simulation binary management");
    eprintln!();
    eprintln!("Run 'cargo xtask sim --help' for simulation subcommands.");
}

fn sim_dispatch(args: &[String]) {
    match args.first().map(std::string::String::as_str) {
        Some("list") => sim_list(&args[1..]),
        Some("run") => sim_run(&args[1..]),
        Some("run-all") => sim_run_all(),
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

fn filter_binaries(filters: &[String]) -> Vec<&'static SimBinary> {
    if filters.is_empty() {
        SIM_BINARIES.iter().collect()
    } else {
        SIM_BINARIES
            .iter()
            .filter(|b| filters.iter().any(|f| b.name.contains(f.as_str())))
            .collect()
    }
}

fn sim_list(args: &[String]) {
    let binaries = filter_binaries(args);

    if binaries.is_empty() {
        if args.is_empty() {
            eprintln!("(no sim binaries registered yet — Stage 1 adds the first)");
            return;
        }
        eprintln!("No binaries match filters: {args:?}");
        process::exit(1);
    }

    for bin in &binaries {
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

    let binaries = filter_binaries(filter_args);

    if binaries.is_empty() {
        eprintln!("No binaries match filters: {filter_args:?}");
        process::exit(1);
    }

    run_binaries(&binaries, binary_args);
}

fn sim_run_all() {
    let binaries: Vec<&SimBinary> = SIM_BINARIES.iter().collect();
    if binaries.is_empty() {
        eprintln!("(no sim binaries registered yet — Stage 1 adds the first)");
        return;
    }
    run_binaries(&binaries, &[]);
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
    let to_pkgs = |s: &str| -> HashSet<String> {
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
        clean.args(["clean", "--target-dir", "target/sancov"]);
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
        cmd.args(["run", "--bin", bin.name]);

        cmd.env("SANCOV_CRATES", bin.sancov_crates);
        // Use a separate target dir so cargo doesn't serve a cached
        // non-instrumented build (SANCOV_CRATES isn't in cargo's fingerprint).
        cmd.args(["--target-dir", "target/sancov"]);

        if !extra_args.is_empty() {
            cmd.arg("--");
            cmd.args(extra_args);
        }

        match cmd.status() {
            Ok(status) if status.success() => {
                eprintln!(
                    "--- {} --- ({})\n",
                    bin.display_name(),
                    fmt_duration(bin_start.elapsed())
                );
                passed.push(bin.display_name());
            }
            Ok(status) => {
                let code = status.code().unwrap_or(-1);
                eprintln!(
                    "{}: exited with code {code} ({})\n",
                    bin.display_name(),
                    fmt_duration(bin_start.elapsed())
                );
                failed.push(bin.display_name());
            }
            Err(e) => {
                eprintln!("{}: failed to launch: {e}\n", bin.display_name());
                failed.push(bin.display_name());
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
