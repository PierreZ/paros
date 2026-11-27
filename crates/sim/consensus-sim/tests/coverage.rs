//! Coverage-driven simulation (#1199 (coverage-driven DST)): every flavor
//! walks with exploration on, and the campaign judges the union of what
//! they reached against every `probe!` site of `paros-consensus`.
//!
//! A run that finds something new (the first pass of a site, or more
//! covered edges of the core when `scripts/sancov-rustc.sh` instruments it)
//! is branched: its children replay it up to that step and draw a new
//! schedule (moonpool-walk's `Walk::explore`). The judge is over all the
//! walks of this one test, not over one flavor: a probe that no walk
//! reaches fails the gate. There is no list of excused probes: a probe that
//! no walk reaches gets a walk move or a steer in the same PR.
//!
//! Each walk runs in its own process (this test binary again, with the
//! ignored test [`one_walk`]), several at a time. A process has its own
//! `SanitizerCoverage` counters, so the edges that steer a walk are the
//! edges of that walk alone, and a walk branches the same way however many
//! walks run beside it.
//!
//! CI splits the walks over several machines. A shard (`COVERAGE_SHARD=i/n`)
//! runs the walks whose index in [`walks`] is `i` modulo `n`, and saves what
//! each reached in `COVERAGE_SAVE`. The judge (`COVERAGE_FROM`) reads the
//! saved walks of every shard, runs none, and judges their union as one run
//! of every walk would be judged: it fails when a walk is missing. A walk
//! runs the same way in a shard as in one run of all, so the union is the
//! same.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use consensus_sim::campaign::{Coverage, try_campaign};
use consensus_sim::cluster::{Cluster, Flavor};
use consensus_sim::decree::{DecreeModel, DecreeMutant};
use consensus_sim::probes::core_probes;
use moonpool_walk::{Model, Walk};
use paros_consensus::decree::Phase1Rule;

/// Root seeds of each walk. `MOONPOOL_WALK_SEEDS` overrides it.
const SEEDS: u64 = 300;
/// The runs each root seed may add by branching. `MOONPOOL_WALK_EXPLORE`
/// overrides it.
const EXPLORE: u64 = 10;

/// The walk that [`one_walk`] runs, by its name in [`walks`].
const WALK_VAR: &str = "COVERAGE_WALK";
/// The file where [`one_walk`] writes what its walk reached.
const OUT_VAR: &str = "COVERAGE_OUT";
/// How many walk processes run at once. The default is the number of
/// CPUs.
const JOBS_VAR: &str = "COVERAGE_JOBS";
/// The shard of the walks that this run walks, as `i/n`.
const SHARD_VAR: &str = "COVERAGE_SHARD";
/// The directory where a shard saves what each of its walks reached.
const SAVE_VAR: &str = "COVERAGE_SAVE";
/// The directory that the judge reads the saved walks of every shard from.
const FROM_VAR: &str = "COVERAGE_FROM";

/// Every flavor of the cluster, with its own pages and with the smallest
/// pages, each with the name its report prints.
fn flavors() -> Vec<(String, Flavor)> {
    let full = vec![
        Flavor::multi_paxos(),
        Flavor::multi_paxos_five(),
        Flavor::handoff(),
        Flavor::grid(2, 2),
        Flavor::partitions(),
        Flavor::proxies(),
        Flavor::proxies_on_grid(),
        Flavor::replicas(),
        Flavor::reads(),
        Flavor::compartments(),
        Flavor::matchmakers(),
        Flavor::reconfigured_compartments(),
        Flavor::disk_faults(),
        Flavor::disk_faults_three(),
        Flavor::retirement(),
        Flavor::handover(),
        Flavor::lagging_reads(),
        Flavor::segments(),
        Flavor::segment_replicas(),
        Flavor::healing(),
    ];
    let paged = [
        Flavor::multi_paxos_five(),
        Flavor::compartments(),
        Flavor::matchmakers(),
        Flavor::reconfigured_compartments(),
        Flavor::disk_faults(),
        Flavor::disk_faults_three(),
        Flavor::retirement(),
        Flavor::handover(),
        Flavor::lagging_reads(),
    ]
    .map(Flavor::paged);
    let full = full.into_iter().map(|f| (f.name.to_owned(), f));
    let paged = paged
        .into_iter()
        .map(|f| (format!("{} (paged)", f.name), f));
    full.chain(paged).collect()
}

/// The decree walks, one for each `Phase1Rule`, and the batcher.
const OTHER_WALKS: &[&str] = &["decree Quorum", "decree EveryAcceptor", "batcher"];

/// The name of every walk of the campaign.
fn walks() -> Vec<String> {
    flavors()
        .into_iter()
        .map(|(name, _)| name)
        .chain(OTHER_WALKS.iter().map(|name| (*name).to_owned()))
        .collect()
}

/// An exploring walk: probes and, when instrumented, the core's edges.
fn explore<M: Model>(walk: Walk<M>) -> Walk<M> {
    walk.seeds(SEEDS)
        .explore(EXPLORE)
        .edge_coverage(moonpool_explorer::sancov_edges_covered_live)
}

/// What one walk reached: each site with its passes, each move the walk
/// may make and never made, and its line of the report.
#[derive(Default)]
struct Reached {
    sites: Vec<(String, u64)>,
    unused: Vec<String>,
    report: String,
}

impl Reached {
    /// `allowed` says whether the walk may make its action of that index:
    /// a move the flavor switched off is not a gap.
    fn new(name: &str, coverage: &Coverage, allowed: &dyn Fn(usize) -> bool) -> Self {
        let explored = coverage
            .report
            .to_string()
            .lines()
            .find(|line| line.starts_with("explored:"))
            .unwrap_or("not explored")
            .to_owned();
        let reached = coverage.sites.iter().filter(|(_, hits)| *hits > 0).count();
        let counts = coverage
            .report
            .actions
            .iter()
            .zip(&coverage.report.stats.actions);
        let unused = counts
            .enumerate()
            .filter(|(index, (_, count))| **count == 0 && allowed(*index))
            .map(|(_, (action, _))| action.clone())
            .collect();
        let edges = if moonpool_explorer::sancov_is_available() {
            format!(
                "; {} of {} core edges covered",
                moonpool_explorer::sancov_edges_covered_live(),
                moonpool_explorer::sancov_edge_count()
            )
        } else {
            String::new()
        };
        Self {
            sites: coverage.sites.clone(),
            unused,
            report: format!("{name}: {reached} sites; {explored}{edges}"),
        }
    }

    /// One line for each item, with a tab after its kind. A site name
    /// holds no tab or line break: the assertion table keeps a
    /// `probe!` message, which is one line of prose.
    fn encode(&self) -> String {
        let mut text = format!("report\t{}\n", self.report);
        for (site, hits) in &self.sites {
            assert!(
                !site.contains(['\t', '\n']),
                "a site name is one line: {site:?}"
            );
            let _ = writeln!(text, "site\t{hits}\t{site}");
        }
        for action in &self.unused {
            let _ = writeln!(text, "unused\t{action}");
        }
        text
    }

    fn decode(text: &str) -> Self {
        let mut reached = Self::default();
        for line in text.lines() {
            let (kind, rest) = line.split_once('\t').expect("a kind, then a tab");
            match kind {
                "report" => rest.clone_into(&mut reached.report),
                "site" => {
                    let (hits, site) = rest.split_once('\t').expect("hits, then the site");
                    let hits = hits.parse().expect("a count of passes");
                    reached.sites.push((site.to_owned(), hits));
                }
                "unused" => reached.unused.push(rest.to_owned()),
                other => panic!("an unknown line kind {other:?}"),
            }
        }
        reached
    }
}

/// Run the walk called `name` in this process.
fn run(name: &str) -> Reached {
    let walk_result = |result: Result<Coverage, String>| {
        result.unwrap_or_else(|failure| panic!("{name}: {failure}"))
    };
    if let Some((_, flavor)) = flavors().into_iter().find(|(n, _)| n == name) {
        let allows = flavor.clone();
        let allowed = move |index: usize| {
            <Cluster as Model>::actions()
                .get(index)
                .is_some_and(|action| allows.allows(*action))
        };
        let walk =
            explore(Walk::<Cluster>::new()).model(move |cx| Cluster::build(flavor.clone(), cx));
        return Reached::new(name, &walk_result(try_campaign(&walk, &[])), &allowed);
    }
    let coverage = match name {
        "decree Quorum" | "decree EveryAcceptor" => {
            let rule = if name == "decree Quorum" {
                Phase1Rule::Quorum
            } else {
                Phase1Rule::EveryAcceptor
            };
            let walk = explore(Walk::<DecreeModel>::new())
                .model(move |_| DecreeModel::build(rule, DecreeMutant::None));
            try_campaign(&walk, &[])
        }
        "batcher" => try_campaign(
            &explore(Walk::<consensus_sim::batcher::BatchModel>::new()),
            &[],
        ),
        other => panic!("no walk is called {other:?}"),
    };
    Reached::new(name, &walk_result(coverage), &|_| true)
}

/// One walk of the campaign, in a process of its own: the walk that
/// `COVERAGE_WALK` names, with what it reached written to `COVERAGE_OUT`.
/// [`every_core_probe_is_reached`] runs it; alone, it does nothing.
#[test]
#[ignore = "one walk of the coverage gate, run by every_core_probe_is_reached"]
fn one_walk() {
    let (Some(name), Some(out)) = (std::env::var_os(WALK_VAR), std::env::var_os(OUT_VAR)) else {
        return;
    };
    let name = name.into_string().expect("a walk name is UTF-8");
    let reached = run(&name);
    std::fs::write(&out, reached.encode()).expect("write what the walk reached");
}

/// A walk process that runs.
struct Running {
    name: String,
    child: Child,
    out: PathBuf,
    log: PathBuf,
}

/// Start the walk `name` in a new process of this test binary.
fn start(name: &str, index: usize, dir: &Path) -> Running {
    let out = dir.join(format!("{index}.reached"));
    let log = dir.join(format!("{index}.log"));
    let log_file = std::fs::File::create(&log).expect("create the walk's log");
    let child = Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            "--ignored",
            "--exact",
            "one_walk",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(WALK_VAR, name)
        .env(OUT_VAR, &out)
        .stdout(Stdio::from(log_file.try_clone().expect("share the log")))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("start a walk process");
    Running {
        name: name.to_owned(),
        child,
        out,
        log,
    }
}

/// Wait for `running`, and read what it reached.
fn finish(mut running: Running) -> Reached {
    let status = running.child.wait().expect("wait for a walk process");
    let log = std::fs::read_to_string(&running.log).unwrap_or_default();
    assert!(
        status.success(),
        "the walk {} failed ({status}):\n{log}",
        running.name
    );
    let text = std::fs::read_to_string(&running.out).unwrap_or_else(|error| {
        panic!("the walk {} wrote nothing ({error}):\n{log}", running.name)
    });
    Reached::decode(&text)
}

/// Run the walks of [`walks`] at `indices`, `jobs` processes at a time, in
/// that order. Each comes back with its index.
fn run_walks(indices: &[usize], jobs: usize) -> Vec<(usize, Reached)> {
    let dir = std::env::temp_dir().join(format!("paros-coverage-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the campaign's directory");
    let names = walks();
    let mut done = Vec::new();
    let mut running: Vec<(usize, Running)> = Vec::new();
    let mut next = indices.iter();
    let mut pending = next.next();
    while pending.is_some() || !running.is_empty() {
        while let Some(&index) = pending
            && running.len() < jobs
        {
            running.push((index, start(&names[index], index, &dir)));
            pending = next.next();
        }
        // Wait for the oldest: the walks are not of one length, but the
        // pool stays full while the oldest runs.
        let (index, oldest) = running.remove(0);
        done.push((index, finish(oldest)));
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(done.len(), indices.len(), "every walk of the run ran");
    done
}

/// The shard `i/n` that `COVERAGE_SHARD` names, if any.
fn shard() -> Option<(usize, usize)> {
    let text = std::env::var(SHARD_VAR).ok()?;
    let (i, n) = text.split_once('/').expect("COVERAGE_SHARD is i/n");
    let i: usize = i.parse().expect("the shard index is a count");
    let n: usize = n.parse().expect("the shard count is a count");
    assert!(n > 0, "at least one shard");
    assert!(i < n, "the shard index is below the shard count");
    Some((i, n))
}

/// The file where a shard saves the walk at `index` of [`walks`].
fn saved(dir: &Path, index: usize) -> PathBuf {
    dir.join(format!("walk-{index}.reached"))
}

/// What every walk of [`walks`] reached, as the shards saved it in `dir`.
fn load(dir: &Path) -> Vec<Reached> {
    (0..walks().len())
        .map(|index| {
            let path = saved(dir, index);
            let text = std::fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!(
                    "no shard saved the walk {} ({}: {error})",
                    walks()[index],
                    path.display()
                )
            });
            Reached::decode(&text)
        })
        .collect()
}

#[test]
fn every_core_probe_is_reached() {
    let jobs = std::env::var(JOBS_VAR).map_or_else(
        |_| std::thread::available_parallelism().map_or(1, std::num::NonZero::get),
        |jobs| jobs.parse().expect("COVERAGE_JOBS is a count"),
    );
    assert!(jobs > 0, "at least one walk runs at a time");
    let walked = if let Some(dir) = std::env::var_os(FROM_VAR) {
        assert!(
            shard().is_none(),
            "the judge runs no walk: COVERAGE_FROM takes no COVERAGE_SHARD"
        );
        load(Path::new(&dir))
    } else if let Some((i, n)) = shard() {
        // A shard walks its part and saves it; the judge of the union is
        // the run with `COVERAGE_FROM`, once every shard is done.
        let dir = std::env::var_os(SAVE_VAR).expect("a shard saves its walks in COVERAGE_SAVE");
        let dir = Path::new(&dir);
        std::fs::create_dir_all(dir).expect("create the shard's directory");
        let indices: Vec<usize> = (0..walks().len()).filter(|index| index % n == i).collect();
        assert!(!indices.is_empty(), "every shard walks at least one walk");
        for (index, reached) in run_walks(&indices, jobs) {
            eprintln!("{}", reached.report);
            std::fs::write(saved(dir, index), reached.encode())
                .expect("save what the walk reached");
        }
        return;
    } else {
        let indices: Vec<usize> = (0..walks().len()).collect();
        run_walks(&indices, jobs)
            .into_iter()
            .map(|(_, reached)| reached)
            .collect()
    };
    assert_eq!(walked.len(), walks().len(), "every walk reported");

    let mut hits: BTreeMap<&str, u64> = BTreeMap::new();
    let mut errors = Vec::new();
    let mut report = String::new();
    for (name, reached) in walks().iter().zip(&walked) {
        for (site, count) in &reached.sites {
            *hits.entry(site.as_str()).or_default() += count;
        }
        for action in &reached.unused {
            errors.push(format!("a walk never made the move {name}: {action}"));
        }
        let _ = writeln!(report, "{}", reached.report);
    }
    let probes = core_probes();
    let hits_of = |key: &str| hits.get(key).copied().unwrap_or(0);
    for probe in &probes {
        if hits_of(probe.key()) == 0 {
            errors.push(format!(
                "no walk reached it: {} ({})",
                probe.message, probe.file
            ));
        }
    }
    let reached = probes.iter().filter(|p| hits_of(p.key()) > 0).count();
    // The rarest probes reached: the margin the gate has left.
    let mut rarest: Vec<(u64, &str)> = probes
        .iter()
        .map(|p| (hits_of(p.key()), p.message.as_str()))
        .filter(|(count, _)| *count > 0)
        .collect();
    rarest.sort_unstable();
    for (count, message) in rarest.iter().take(5) {
        let _ = writeln!(report, "rare: {count} passes: {message}");
    }
    let report = format!(
        "{reached} of {} core probes reached\n{report}",
        probes.len()
    );
    eprintln!("{report}");
    assert!(errors.is_empty(), "{}\n{report}", errors.join("\n"));
}
