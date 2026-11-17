//! The negative hunt (#742 (mutant per excuse), step 1.5 of the rework,
//! #1094 (injected liveness bugs)): hand-written mutants that turn on a
//! known bug, each of which the simulation must catch, as `FoundationDB`'s
//! `tests/negative` do. The command first runs the unmutated hunt
//! (`paros-sim-runner`'s `mutants` test target) over the same seeds and
//! records its reds, the *baseline*. A mutant is caught by a red the
//! baseline does not have on that seed, of one of its causes
//! (`paros_sim::mutant_hunt`): main is red on a share of the seeds while
//! the causes of phase 1 are open, so a red alone shows nothing.
//!
//! A mutant is one file in [`MUTANT_DIR`], `<name>.mutant`:
//!
//! ```text
//! # Why this mutant, what it breaks (comment lines).
//! expect: caught
//! cause: <the message of an oracle that must catch it>
//! file: crates/lib/paros-core/src/proposer.rs
//! --- find
//! <text that occurs exactly once in the file>
//! --- replace
//! <its replacement>
//! --- end
//! ```
//!
//! `expect` is `caught` (the default) or `survives #N`: a known survivor,
//! with the issue that tracks it. The baseline makes a verdict stable on a
//! head: a red that is not the mutant's does not catch it.
//! `cause` may come several times: a red of any of them catches the
//! mutant. A mutant with no `cause` is caught by any new red.
//! `file` applies to the blocks after it; a mutant may edit several
//! places. A find text that is missing, or occurs more than once, makes
//! the mutant **stale**: the code moved, and the mutant must follow it.
//!
//! The verdict of a mutant is one of: caught (a new red of a cause),
//! survived (none), unviable (it does not build, or the hunt broke), timeout, or
//! stale. The command exits non-zero when any verdict differs from its
//! expectation.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command};
use std::time::{Duration, Instant};

use super::{MUTANT_HUNT, MUTANT_INCREMENTAL, fmt_duration};

/// Where the mutants live, from the workspace root.
const MUTANT_DIR: &str = "crates/sim/paros-sim-runner/negative";

/// Where the verdicts go, from the workspace root: one line per mutant in
/// `verdicts.txt`, each hunt's output in `<name>.log`, and the baseline's
/// reds in `baseline.txt`.
const OUT_DIR: &str = "negative.out";

/// The lines of each hunt's log the command repeats on its own output.
const TAIL_LINES: usize = 12;

/// Seeds per mutant when no `--seeds` is given: more than the mutation
/// hunt's, as a mutant's state comes on a share of the seeds only.
const DEFAULT_SEEDS: u64 = 600;

/// How long one mutant's hunt may run, when no `--timeout` is given. A
/// known survivor runs every seed: a heal survivor (since deleted with the
/// role slots it mutated, #837 (remove role slots)) took 39 min on a CI
/// runner, then passed 45 min once #821 (control plane mode)
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
    expect: Expect,
    causes: Vec<String>,
    edits: Vec<Edit>,
}

#[derive(Clone, PartialEq, Eq)]
enum Expect {
    Caught,
    /// A known survivor, with the issue that tracks it.
    Survives(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Caught,
    Survived,
    Unviable,
    Timeout,
    Stale,
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Verdict::Caught => "caught",
            Verdict::Survived => "survived",
            Verdict::Unviable => "unviable",
            Verdict::Timeout => "timeout",
            Verdict::Stale => "stale",
        }
    }

    fn meets(self, expect: &Expect) -> bool {
        match expect {
            Expect::Caught => self == Verdict::Caught,
            Expect::Survives(_) => self == Verdict::Survived,
        }
    }
}

fn help() {
    eprintln!(
        "Usage: cargo xtask negative [--seeds N] [--shard K/N] [--timeout SECS] [--baseline FILE | --baseline-only] [--list] [NAME...]"
    );
    eprintln!();
    eprintln!("Runs the unmutated hunt and records its reds (or reads them from");
    eprintln!("--baseline FILE, recorded over the same seeds), then applies each");
    eprintln!("hand-written mutant of {MUTANT_DIR} in turn, runs the hunt, and restores");
    eprintln!("the tree. A red the baseline does not have, of one of the mutant's");
    eprintln!("causes, catches the mutant. NAME filters by name. --baseline-only");
    eprintln!("records the baseline and runs no mutant (CI shares it between shards).");
    eprintln!("Verdicts go to {OUT_DIR}/verdicts.txt; the exit code is non-zero when a");
    eprintln!("verdict differs from the mutant's `expect`.");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  cargo xtask negative --list");
    eprintln!("  cargo xtask negative --seeds 200 heal-maintenance-never-ends");
    eprintln!("  cargo xtask negative --shard 1/4");
}

/// `cargo xtask negative`.
pub(super) fn negative(args: &[String]) {
    let mut seeds = DEFAULT_SEEDS;
    let mut timeout = DEFAULT_TIMEOUT;
    let mut shard = (0_usize, 1_usize);
    let mut list = false;
    let mut known: Option<PathBuf> = None;
    let mut baseline_only = false;
    let mut filters = Vec::new();
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "help" | "--help" | "-h" => {
                help();
                return;
            }
            "--list" => list = true,
            "--baseline-only" => baseline_only = true,
            "--baseline" => {
                let Some(path) = rest.next() else {
                    fail("--baseline needs a file")
                };
                known = Some(PathBuf::from(path));
            }
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
            println!("{}", m.name);
        }
        return;
    }
    fs::create_dir_all(OUT_DIR).unwrap_or_else(|e| fail(&format!("{OUT_DIR}: {e}")));
    if baseline_only {
        record_baseline(&Path::new(OUT_DIR).join("baseline.txt"), seeds);
        return;
    }
    if mutants.is_empty() {
        eprintln!("no mutant to run");
        return;
    }
    let known = known.unwrap_or_else(|| {
        let path = Path::new(OUT_DIR).join("baseline.txt");
        record_baseline(&path, seeds);
        path
    });
    let known =
        fs::canonicalize(&known).unwrap_or_else(|e| fail(&format!("{}: {e}", known.display())));
    judge_all(&mutants, seeds, timeout, &known);
}

/// Run every mutant against the baseline's reds at `known`, write the
/// verdicts, and exit non-zero when one is not as expected.
fn judge_all(mutants: &[Mutant], seeds: u64, timeout: Duration, known: &Path) {
    let mut summary = String::new();
    let mut unmet = 0;
    for mutant in mutants {
        let start = Instant::now();
        let verdict = run(mutant, seeds, timeout, known);
        let met = verdict.meets(&mutant.expect);
        if !met {
            unmet += 1;
        }
        let expect = match &mutant.expect {
            Expect::Caught => "caught".to_string(),
            Expect::Survives(issue) => format!("survives {issue}"),
        };
        let line = format!(
            "{}\t{}\t{}\t{}",
            mutant.name,
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
    let mut expect = Expect::Caught;
    let mut causes = Vec::new();
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
            "expect" => {
                expect = match value.split_once(' ') {
                    None if value == "caught" => Expect::Caught,
                    Some(("survives", issue)) => Expect::Survives(issue.trim().to_string()),
                    _ => bad(&format!(
                        "expect is `caught` or `survives #N`, not {value:?}"
                    )),
                };
            }
            "cause" if !value.is_empty() => causes.push(value.to_string()),
            "file" => file = Some(PathBuf::from(value)),
            _ => bad(&format!("unknown key {key:?}")),
        }
    }
    if edits.is_empty() {
        bad("no edit");
    }
    Mutant {
        name,
        expect,
        causes,
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

/// Build and run the unmutated hunt over `seeds`, and record its reds at
/// `path` (`PAROS_MUTANT_RECORD`); exit when it does not run.
fn record_baseline(path: &Path, seeds: u64) {
    eprintln!("--- negative: the unmutated baseline hunt over {seeds} seeds ---");
    // The hunt runs in its package's directory: an absolute path.
    let path = std::env::current_dir().map_or_else(|e| fail(&format!("{e}")), |dir| dir.join(path));
    let mut cmd = Command::new("cargo");
    cmd.args(MUTANT_HUNT);
    cmd.env(MUTANT_INCREMENTAL.0, MUTANT_INCREMENTAL.1);
    cmd.env("PAROS_MUTANT_SEEDS", seeds.to_string());
    cmd.env("PAROS_MUTANT_RECORD", &path);
    if !cmd.status().is_ok_and(|s| s.success()) {
        fail("the baseline hunt did not run");
    }
    let reds = fs::read_to_string(&path).map_or(0, |t| t.lines().count());
    eprintln!("baseline: {reds} reds in {}", path.display());
}

/// Apply `mutant`, build and run the hunt against the baseline's reds at
/// `known`, restore the tree.
fn run(mutant: &Mutant, seeds: u64, timeout: Duration, known: &Path) -> Verdict {
    eprintln!("--- negative: {} ---", mutant.name);
    let Some(applied) = apply(mutant) else {
        return Verdict::Stale;
    };
    let hunt = || {
        let mut cmd = Command::new("cargo");
        cmd.args(MUTANT_HUNT);
        cmd.env(MUTANT_INCREMENTAL.0, MUTANT_INCREMENTAL.1);
        cmd.env("PAROS_MUTANT_SEEDS", seeds.to_string());
        cmd.env("PAROS_MUTANT_BASELINE", known);
        cmd.env("PAROS_MUTANT_CAUSES", mutant.causes.join("\n"));
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
    let verdict = wait(child, timeout);
    drop(applied);
    // The hunt's last lines carry its evidence: the CI log shows them.
    let text = fs::read_to_string(&log).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    for line in &lines[lines.len().saturating_sub(TAIL_LINES)..] {
        eprintln!("  | {line}");
    }
    verdict
}

/// Wait for the hunt, at most `timeout`, and judge it: a hunt that exits 0
/// found no red of the mutant's, and one that exits 1 did (see
/// `paros-sim-runner/tests/mutants.rs`).
fn wait(mut child: Child, timeout: Duration) -> Verdict {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Verdict::Survived,
            Ok(Some(status)) if status.code() == Some(1) => return Verdict::Caught,
            // The hunt itself broke (a bad baseline file, a panic outside
            // the simulation): no verdict on the mutant.
            Ok(Some(_)) => return Verdict::Unviable,
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
    use super::{Expect, Verdict, parse};

    #[test]
    fn a_mutant_names_its_causes() {
        let dir = std::env::temp_dir().join(format!("negative-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temporary directory");
        let path = dir.join("two-causes.mutant");
        std::fs::write(
            &path,
            "# why\nexpect: caught\ncause: heal: one\ncause: sets: two\nfile: a.rs\n--- find\nx\n--- replace\ny\n--- end\n",
        )
        .expect("a mutant file");
        let mutant = parse(&path);
        assert_eq!(mutant.name, "two-causes");
        assert!(mutant.expect == Expect::Caught);
        assert_eq!(mutant.causes, ["heal: one", "sets: two"]);
        assert_eq!(mutant.edits.len(), 1);
        assert_eq!(mutant.edits[0].find, "x\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_verdict_meets_only_its_own_expectation() {
        let survives = Expect::Survives("#1177".to_string());
        assert!(Verdict::Caught.meets(&Expect::Caught));
        assert!(Verdict::Survived.meets(&survives));
        assert!(!Verdict::Survived.meets(&Expect::Caught));
        assert!(!Verdict::Caught.meets(&survives));
        for broken in [Verdict::Unviable, Verdict::Timeout, Verdict::Stale] {
            assert!(!broken.meets(&Expect::Caught), "{}", broken.label());
            assert!(!broken.meets(&survives), "{}", broken.label());
        }
    }
}
