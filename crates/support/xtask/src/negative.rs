//! The negative hunt (#742 (mutant per excuse)): hand-written mutants that
//! break paros on purpose, each of which the simulation must catch, as
//! `FoundationDB`'s `tests/negative` do.
//!
//! Each oracle excuse (`paros_sim::excuses`) switches a check off, and some
//! re-derive a rule of the system under test. If the excuse is wider than
//! the rule, a real bug hides behind it and the run stays green. A mutant
//! here breaks paros in the area its excuse covers, and the hunt
//! (`paros-sim-runner`'s `mutants` test target) must go red on a seed where
//! that excuse fired. A mutant with no excuse covers a piece of the
//! harness's fault ledger instead (#760 (moonpool owns fault placement)):
//! it must stay red once moonpool's gate replaces the ledger.
//!
//! A mutant is one file in [`MUTANT_DIR`], `<name>.mutant`:
//!
//! ```text
//! # Why this mutant, what it breaks (comment lines).
//! excuse: loss: unrecoverable slot
//! expect: caught
//! file: crates/lib/paros-core/src/proposer.rs
//! --- find
//! <text that occurs exactly once in the file>
//! --- replace
//! <its replacement>
//! --- end
//! ```
//!
//! `excuse` is an excuse's name as the excuse table prints it, or `none`.
//! `expect` is `caught` (the default), `red elsewhere`, `survives #N`: a
//! known survivor, with the issue that tracks it, or `either #N`: a mutant
//! the fixed-seed hunt catches on some heads and not on others, because
//! its verdict moves with the draws, until issue N makes it caught on
//! every head. `red elsewhere` fits an
//! excuse the harness derives from its own ground truth alone (a wipe, a
//! lost cell): a mutant cannot widen such an excuse, so the proof is that
//! the oracle bites on the seeds right outside it. `file` applies to the blocks after it; a
//! mutant may edit several places. A find text that is missing, or occurs
//! more than once, makes the mutant **stale**: the code moved, and the
//! mutant must follow it.
//!
//! The verdict of a mutant is one of: caught (red on a seed where its
//! excuse fired, or anywhere for a ledger mutant), red elsewhere (red only
//! on seeds where its excuse did not fire), survived (the hunt stayed
//! green), unviable (it does not build), timeout, or stale. The command
//! exits non-zero when any verdict differs from its expectation.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command};
use std::time::{Duration, Instant};

use super::{MUTANT_HUNT, MUTANT_INCREMENTAL, fmt_duration};

/// Where the mutants live, from the workspace root.
const MUTANT_DIR: &str = "crates/sim/paros-sim-runner/negative";

/// Where the verdicts go, from the workspace root: one line per mutant in
/// `verdicts.txt`, and each hunt's output in `<name>.log`.
const OUT_DIR: &str = "negative.out";

/// The lines of each hunt's log the command repeats on its own output.
const TAIL_LINES: usize = 12;

/// Seeds per mutant when no `--seeds` is given: more than the mutation
/// hunt's, as an excuse fires on a share of the seeds only.
const DEFAULT_SEEDS: u64 = 600;

/// How long one mutant's hunt may run, when no `--timeout` is given. A
/// known survivor runs every seed: `heal-machine-free-one-short` took
/// 39 min on a CI runner, then passed 45 min once #821 (control plane mode)
/// made the runs longer (PR #834 (moonpool owns fault placement)). A bound,
/// not a target: the job's own limit is 340 min for a shard of a few mutants.
const DEFAULT_TIMEOUT: Duration = Duration::from_mins(90);

/// One edit of a mutant: `find` occurs exactly once in `file`.
struct Edit {
    file: PathBuf,
    find: String,
    replace: String,
}

/// One mutant file, parsed.
struct Mutant {
    name: String,
    /// The excuse it must be caught under; `None` for a ledger mutant.
    excuse: Option<String>,
    expect: Expect,
    edits: Vec<Edit>,
}

#[derive(Clone, PartialEq, Eq)]
enum Expect {
    Caught,
    RedElsewhere,
    /// A known survivor, with the issue that tracks it.
    Survives(String),
    /// Caught or survived, as the draws fall, until the issue makes it
    /// caught on every head.
    Either(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Caught,
    RedElsewhere,
    Survived,
    Unviable,
    Timeout,
    Stale,
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Verdict::Caught => "caught",
            Verdict::RedElsewhere => "red elsewhere",
            Verdict::Survived => "survived",
            Verdict::Unviable => "unviable",
            Verdict::Timeout => "timeout",
            Verdict::Stale => "stale",
        }
    }

    fn meets(self, expect: &Expect) -> bool {
        match expect {
            Expect::Caught => self == Verdict::Caught,
            Expect::RedElsewhere => matches!(self, Verdict::Caught | Verdict::RedElsewhere),
            Expect::Survives(_) => matches!(self, Verdict::Survived | Verdict::RedElsewhere),
            Expect::Either(_) => matches!(
                self,
                Verdict::Caught | Verdict::Survived | Verdict::RedElsewhere
            ),
        }
    }
}

fn help() {
    eprintln!(
        "Usage: cargo xtask negative [--seeds N] [--shard K/N] [--timeout SECS] [--list] [NAME...]"
    );
    eprintln!();
    eprintln!("Applies each hand-written mutant of {MUTANT_DIR} in turn, runs the hunt,");
    eprintln!("and restores the tree (#742 (mutant per excuse)). A mutant with an excuse");
    eprintln!("is caught only on a seed where that excuse fired. NAME filters by name.");
    eprintln!("Verdicts go to {OUT_DIR}/verdicts.txt; the exit code is non-zero when a");
    eprintln!("verdict differs from the mutant's `expect`.");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  cargo xtask negative --list");
    eprintln!("  cargo xtask negative --seeds 200 heal-machine-free-one-short");
    eprintln!("  cargo xtask negative --shard 1/4");
}

/// `cargo xtask negative`.
pub(super) fn negative(args: &[String]) {
    let mut seeds = DEFAULT_SEEDS;
    let mut timeout = DEFAULT_TIMEOUT;
    let mut shard = (0_usize, 1_usize);
    let mut list = false;
    let mut filters = Vec::new();
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "help" | "--help" | "-h" => {
                help();
                return;
            }
            "--list" => list = true,
            "--seeds" => seeds = number(rest.next(), "--seeds needs a seed count"),
            "--timeout" => {
                timeout = Duration::from_secs(number(rest.next(), "--timeout needs seconds"));
            }
            "--shard" => {
                let parsed = rest.next().and_then(|s| {
                    let (k, n) = s.split_once('/')?;
                    Some((k.parse().ok()?, n.parse().ok()?))
                });
                match parsed {
                    Some((k, n)) if k < n => shard = (k, n),
                    _ => fail("--shard needs K/N with K < N"),
                }
            }
            name => filters.push(name.to_string()),
        }
    }
    let mutants: Vec<Mutant> = load()
        .into_iter()
        .filter(|m| filters.is_empty() || filters.iter().any(|f| m.name.contains(f.as_str())))
        .enumerate()
        .filter(|(i, _)| i % shard.1 == shard.0)
        .map(|(_, m)| m)
        .collect();
    if list {
        for m in &mutants {
            println!("{}  [{}]", m.name, m.excuse.as_deref().unwrap_or("ledger"));
        }
        return;
    }
    if mutants.is_empty() {
        eprintln!("no mutant to run");
        return;
    }
    fs::create_dir_all(OUT_DIR).unwrap_or_else(|e| fail(&format!("{OUT_DIR}: {e}")));
    let mut summary = String::new();
    let mut unmet = 0;
    for mutant in &mutants {
        let start = Instant::now();
        let verdict = run(mutant, seeds, timeout);
        let met = verdict.meets(&mutant.expect);
        if !met {
            unmet += 1;
        }
        let expect = match &mutant.expect {
            Expect::Caught => "caught".to_string(),
            Expect::RedElsewhere => "red elsewhere".to_string(),
            Expect::Survives(issue) => format!("survives {issue}"),
            Expect::Either(issue) => format!("either {issue}"),
        };
        let line = format!(
            "{}\t{}\t{}\t{}\t{}",
            mutant.name,
            mutant.excuse.as_deref().unwrap_or("ledger"),
            verdict.label(),
            expect,
            if met { "ok" } else { "UNEXPECTED" },
        );
        eprintln!(
            "--- negative: {line} ({}) ---",
            fmt_duration(start.elapsed())
        );
        let _ = writeln!(summary, "{line}");
    }
    let path = Path::new(OUT_DIR).join("verdicts.txt");
    fs::write(&path, &summary).unwrap_or_else(|e| fail(&format!("{}: {e}", path.display())));
    print!("{summary}");
    if unmet > 0 {
        eprintln!("{unmet} mutant(s) not as expected");
        process::exit(1);
    }
}

fn number<T: std::str::FromStr>(arg: Option<&String>, message: &str) -> T {
    arg.and_then(|s| s.parse().ok())
        .unwrap_or_else(|| fail(message))
}

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    process::exit(2);
}

/// Every mutant of [`MUTANT_DIR`], by name.
fn load() -> Vec<Mutant> {
    let mut paths: Vec<PathBuf> = fs::read_dir(MUTANT_DIR)
        .unwrap_or_else(|e| fail(&format!("{MUTANT_DIR}: {e} (run from the workspace root)")))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|ext| ext == "mutant"))
        .collect();
    paths.sort();
    paths.iter().map(|p| parse(p)).collect()
}

/// Parse one mutant file (see the module doc).
fn parse(path: &Path) -> Mutant {
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_else(|| fail(&format!("{}: no name", path.display())))
        .to_string();
    let text =
        fs::read_to_string(path).unwrap_or_else(|e| fail(&format!("{}: {e}", path.display())));
    let bad = |what: &str| -> ! { fail(&format!("{}: {what}", path.display())) };
    let mut excuse = None;
    let mut expect = Expect::Caught;
    let mut file: Option<PathBuf> = None;
    let mut edits = Vec::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if line == "--- find" {
            let Some(file) = file.clone() else {
                bad("a `--- find` before any `file:`")
            };
            let find = block(&mut lines, "--- replace").unwrap_or_else(|| bad("no `--- replace`"));
            let replace = block(&mut lines, "--- end").unwrap_or_else(|| bad("no `--- end`"));
            edits.push(Edit {
                file,
                find,
                replace,
            });
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            bad(&format!("not a `key: value` line: {line:?}"))
        };
        let value = value.trim();
        match key {
            "excuse" => excuse = (value != "none").then(|| value.to_string()),
            "expect" => {
                expect = match value.split_once(' ') {
                    None if value == "caught" => Expect::Caught,
                    _ if value == "red elsewhere" => Expect::RedElsewhere,
                    Some(("survives", issue)) => Expect::Survives(issue.trim().to_string()),
                    Some(("either", issue)) => Expect::Either(issue.trim().to_string()),
                    _ => bad(&format!(
                        "expect is `caught`, `red elsewhere`, `survives #N` or `either #N`, not {value:?}"
                    )),
                };
            }
            "file" => file = Some(PathBuf::from(value)),
            _ => bad(&format!("unknown key {key:?}")),
        }
    }
    if edits.is_empty() {
        bad("no edit");
    }
    Mutant {
        name,
        excuse,
        expect,
        edits,
    }
}

/// The lines up to `end`, joined, with the trailing newline of the last
/// one; `None` when `end` never comes.
fn block<'a>(lines: &mut impl Iterator<Item = &'a str>, end: &str) -> Option<String> {
    let mut text = String::new();
    for line in lines.by_ref() {
        if line == end {
            return Some(text);
        }
        text.push_str(line);
        text.push('\n');
    }
    None
}

/// The files a mutant edited, with their original text: put back on drop,
/// so a hunt that panics or a mutant that does not build leaves the tree
/// clean.
struct Applied(Vec<(PathBuf, String)>);

impl Drop for Applied {
    fn drop(&mut self) {
        for (file, original) in &self.0 {
            if let Err(e) = fs::write(file, original) {
                eprintln!("could not restore {}: {e}", file.display());
            }
        }
    }
}

/// Apply every edit of `mutant`; `None` when one is stale.
fn apply(mutant: &Mutant) -> Option<Applied> {
    let mut applied = Applied(Vec::new());
    for edit in &mutant.edits {
        let current = if applied.0.iter().any(|(f, _)| *f == edit.file) {
            fs::read_to_string(&edit.file).ok()?
        } else {
            let original = fs::read_to_string(&edit.file).ok()?;
            applied.0.push((edit.file.clone(), original.clone()));
            original
        };
        if current.matches(edit.find.as_str()).count() != 1 {
            eprintln!(
                "{}: the find text of {} occurs {} times, not once",
                mutant.name,
                edit.file.display(),
                current.matches(edit.find.as_str()).count()
            );
            return None;
        }
        if fs::write(&edit.file, current.replacen(&edit.find, &edit.replace, 1)).is_err() {
            return None;
        }
    }
    Some(applied)
}

/// Apply `mutant`, build and run the hunt, restore the tree.
fn run(mutant: &Mutant, seeds: u64, timeout: Duration) -> Verdict {
    eprintln!("--- negative: {} ---", mutant.name);
    let Some(applied) = apply(mutant) else {
        return Verdict::Stale;
    };
    let hunt = || {
        let mut cmd = Command::new("cargo");
        cmd.args(MUTANT_HUNT);
        cmd.env(MUTANT_INCREMENTAL.0, MUTANT_INCREMENTAL.1);
        cmd.env("PAROS_MUTANT_SEEDS", seeds.to_string());
        match &mutant.excuse {
            Some(excuse) => cmd.env("PAROS_MUTANT_EXCUSE", excuse),
            None => cmd.env_remove("PAROS_MUTANT_EXCUSE"),
        };
        cmd
    };
    if !hunt().arg("--no-run").status().is_ok_and(|s| s.success()) {
        return Verdict::Unviable;
    }
    let log = Path::new(OUT_DIR).join(format!("{}.log", mutant.name));
    let Ok(out) = fs::File::create(&log) else {
        fail(&format!("{}: cannot create", log.display()));
    };
    let err = out.try_clone().unwrap_or_else(|e| fail(&format!("{e}")));
    let Ok(child) = hunt().stdout(out).stderr(err).spawn() else {
        return Verdict::Unviable;
    };
    let verdict = wait(child, timeout, mutant.excuse.is_some(), &log);
    drop(applied);
    // The hunt's last lines carry its evidence: the CI log shows them.
    let text = fs::read_to_string(&log).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    for line in &lines[lines.len().saturating_sub(TAIL_LINES)..] {
        eprintln!("  | {line}");
    }
    verdict
}

/// Wait for the hunt, at most `timeout`, and judge it: green is a
/// survivor; red is caught when the hunt's log says so (cargo hides the
/// hunt's own exit code), or, for a ledger mutant, red at all (see
/// `paros-sim-runner/tests/mutants.rs`).
fn wait(mut child: Child, timeout: Duration, excused: bool, log: &Path) -> Verdict {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Verdict::Survived,
            Ok(Some(_)) => {
                let text = fs::read_to_string(log).unwrap_or_default();
                return if text.contains("NEGATIVE: caught") || !excused {
                    Verdict::Caught
                } else {
                    Verdict::RedElsewhere
                };
            }
            Ok(None) if start.elapsed() < timeout => {
                std::thread::sleep(Duration::from_secs(1));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Verdict::Timeout;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Expect, Verdict};

    #[test]
    fn an_either_mutant_accepts_caught_and_survived_but_not_a_broken_run() {
        let either = Expect::Either("#773".to_string());
        assert!(Verdict::Caught.meets(&either));
        assert!(Verdict::Survived.meets(&either));
        assert!(Verdict::RedElsewhere.meets(&either));
        for broken in [Verdict::Unviable, Verdict::Timeout, Verdict::Stale] {
            assert!(!broken.meets(&either), "{}", broken.label());
        }
        // The strict expectations are unchanged.
        assert!(!Verdict::Survived.meets(&Expect::Caught));
        assert!(!Verdict::Caught.meets(&Expect::Survives("#773".to_string())));
    }
}
