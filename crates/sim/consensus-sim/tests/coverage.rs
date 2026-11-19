//! Coverage-driven simulation (#1199 (coverage-driven DST)): every flavor
//! walks with exploration on, and the campaign judges the union of what
//! they reached against every `probe!` site of `paros-consensus`.
//!
//! A run that finds something new (the first pass of a site, or more
//! covered edges of the core when `scripts/sancov-rustc.sh` instruments it)
//! is branched: its children replay it up to that step and draw a new
//! schedule (moonpool-walk's `Walk::explore`). The judge is over all the
//! walks of this one test, not over one flavor: a probe that no flavor
//! reaches fails, unless [`GAPS`] names it with the walk move it waits for.
//! A named probe that the default campaign reaches fails too, so the list
//! only shrinks. A campaign with another budget (`MOONPOOL_WALK_SEEDS`,
//! `MOONPOOL_WALK_EXPLORE`) or with edge coverage on branches differently:
//! there, a named probe that is reached is only reported.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use consensus_sim::campaign::{Coverage, try_campaign};
use consensus_sim::cell::{Cell, Flavor};
use consensus_sim::probes::core_probes;
use moonpool_walk::{Model, Walk};

/// Root seeds of each walk.
const SEEDS: u64 = 10;
/// The runs each root seed may add by branching.
const EXPLORE: u64 = 5;

/// The probes no walk reaches yet, each group with the move of the walk it
/// waits for. The follow-up is #1225 (consensus-sim: walk moves for the
/// unreached probes).
const GAPS: &[(&str, &[&str])] = &[
    (
        "disk faults: no walk rots a record on a disk (faulty entries, holes, scrub, repair)",
        &[
            "scrub: a damaged record is written again from the node's own copy",
            "acceptor: an Accept inside the chosen prefix heals a faulty chosen record",
            "phase1: a full promise page mixes readable and faulty entries",
            "boot: the fold stops at a hole under the chosen prefix",
            "catch-up: a replay stops at the server's own faulty chosen record",
            "catch-up: a trim-point jump resolves a repair probe's blocked slot",
            "repair: a leader's probe pages a straggler's late Promise",
        ],
    ),
    (
        "retirement and rejoin: no walk retires a node or books it again",
        &[
            "matchmaker: a registration naming a retired node is refused",
            "matchmaker: a rejoin clears a retired node",
            "matchmaker: a retirement check of an earlier incarnation is refused",
            "matchmaker: a retirement check records the node as retired",
            "gc: a retirement asks the matchmakers which registrations name the node",
            "rejoin: a retirement check takes a rejoin's place",
            "gc: a retirement check asks the matchmakers that did not agree",
            "gc: every matchmaker agreed the node may retire",
            "matchmaking: a campaign meets a matchmaker that refuses a retired node",
            "matchmaking: a reconfiguration naming a retired node is abandoned",
            "rejoin: a rejoin asks the matchmakers that did not take it",
            "rejoin: a running journal booked again rejoins",
            "rejoin: every matchmaker took the rejoin",
        ],
    ),
    (
        "matchmaker set handover: no walk runs MatchmakerReconfigurer",
        &[
            "handover: a bootstrap continues on a later page",
            "handover: a frozen registry spans several pages",
            "handover: a finish thins an even count of answerers to an odd successor",
            "handover: a reconfigurer reads a frozen registry over several pages",
            "handover: a reconfigurer sends a bootstrap over several pages",
            "gc: a refused ack meets a frozen generation",
            "gc: a refused ack teaches the successor",
            "node: a campaign asks the pool outside its set",
            "node: a campaign learns a later set from outside its set",
            "node: a late refusal moves the matchmaker belief",
            "node: a probe asks the pool outside its set",
            "node: a probe learns a later set from outside its set",
        ],
    ),
    (
        "configurations outside the pool: a reconfiguration of the walk names only nodes of the pool",
        &[
            "learning: a configuration outside the pool is ignored",
            "matchmaking: a campaign meets a configuration outside the pool",
            "matchmaking: a membership probe meets a configuration outside the pool",
        ],
    ),
    (
        "not explained yet: the cell makes these moves, but no run reached the branch",
        &[
            "propose: a leadership a higher ballot passed refuses a proposal",
            "matchmaking: a campaign adopts a reconfiguration its won leadership ran",
            "matchmaking: a campaign refutes a reconfiguration a late probe answer named",
            "matchmaking: a late probe answer names a refuted reconfiguration",
            "phase2: a leader moves a stuck round to another column",
            "phase2: a leader whose promise moved past its ballot opens a round it cannot record",
            "phase2: a stuck round whose slot is chosen elsewhere is not moved",
            "phase2: a moved round is chosen",
            "quorum read: a read waits on its basis fence above the row's watermark",
        ],
    ),
];

/// Every flavor of the cell, with full pages and with the smallest pages.
fn flavors() -> Vec<Flavor> {
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
    ];
    let paged = [
        Flavor::multi_paxos_five(),
        Flavor::compartments(),
        Flavor::matchmakers(),
        Flavor::reconfigured_compartments(),
    ]
    .map(Flavor::paged);
    full.into_iter().chain(paged).collect()
}

/// An exploring walk: probes and, when instrumented, the core's edges.
fn explore<M: Model>(walk: Walk<M>) -> Walk<M> {
    walk.seeds(SEEDS)
        .explore(EXPLORE)
        .edge_coverage(moonpool_explorer::sancov_edges_covered_live)
}

/// What every walk of the campaign reached.
#[derive(Default)]
struct Union {
    hits: BTreeMap<String, u64>,
    report: String,
    unused: Vec<String>,
}

impl Union {
    /// Take what one walk reached. `allowed` says whether the walk may make
    /// its action of that index: a move the flavor switched off is not a gap.
    fn absorb(&mut self, name: &str, coverage: &Coverage, allowed: &dyn Fn(usize) -> bool) {
        for (site, hits) in &coverage.sites {
            *self.hits.entry(site.clone()).or_default() += hits;
        }
        let explored = coverage
            .report
            .to_string()
            .lines()
            .find(|line| line.starts_with("explored:"))
            .unwrap_or("not explored")
            .to_owned();
        let reached = coverage.sites.iter().filter(|(_, hits)| *hits > 0).count();
        let _ = writeln!(self.report, "{name}: {reached} sites; {explored}");
        // Every move the walk may make, it made.
        let counts = coverage
            .report
            .actions
            .iter()
            .zip(&coverage.report.stats.actions);
        for (index, (action, count)) in counts.enumerate() {
            if *count == 0 && allowed(index) {
                self.unused.push(format!("{name}: {action}"));
            }
        }
    }

    fn hits(&self, site: &str) -> u64 {
        self.hits.get(site).copied().unwrap_or(0)
    }
}

fn run<M: Model>(union: &mut Union, name: &str, walk: &Walk<M>, allowed: &dyn Fn(usize) -> bool) {
    let coverage = try_campaign(walk, &[]).unwrap_or_else(|failure| panic!("{name}: {failure}"));
    union.absorb(name, &coverage, allowed);
}

#[test]
fn every_core_probe_is_reached_or_named() {
    let mut union = Union::default();
    for flavor in flavors() {
        let name = if flavor.limits == paros_consensus::Limits::DEFAULT {
            flavor.name.to_owned()
        } else {
            format!("{} (paged)", flavor.name)
        };
        let allows = flavor.clone();
        let allowed = move |index: usize| {
            <Cell as Model>::actions()
                .get(index)
                .is_some_and(|action| allows.allows(*action))
        };
        let walk = explore(Walk::<Cell>::new()).model(move |cx| Cell::build(flavor.clone(), cx));
        run(&mut union, &name, &walk, &allowed);
    }
    {
        use consensus_sim::decree::{DecreeModel, DecreeMutant};
        use paros_consensus::decree::Phase1Rule;
        for rule in [Phase1Rule::Quorum, Phase1Rule::EveryAcceptor] {
            let walk = explore(Walk::<DecreeModel>::new())
                .model(move |_| DecreeModel::build(rule, DecreeMutant::None));
            run(&mut union, &format!("decree {rule:?}"), &walk, &|_| true);
        }
        let walk = explore(Walk::<consensus_sim::batcher::BatchModel>::new());
        run(&mut union, "batcher", &walk, &|_| true);
    }

    let edges = moonpool_explorer::sancov_is_available();
    let default_budget = ["MOONPOOL_WALK_SEEDS", "MOONPOOL_WALK_EXPLORE"]
        .iter()
        .all(|name| std::env::var_os(name).is_none());
    let strict = default_budget && !edges;
    let mut notes = Vec::new();
    let named: BTreeMap<&str, &str> = GAPS
        .iter()
        .flat_map(|(why, probes)| probes.iter().map(move |probe| (*probe, *why)))
        .collect();
    let probes = core_probes();
    let mut errors = Vec::new();
    let mut gaps: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for probe in &probes {
        let reached = union.hits(probe.key()) > 0;
        match (reached, named.get(probe.message.as_str())) {
            (true, Some(_)) => {
                let line = format!(
                    "reached, but GAPS still names it: {} ({})",
                    probe.message, probe.file
                );
                if strict {
                    errors.push(line);
                } else {
                    notes.push(line);
                }
            }
            (true, None) => {}
            (false, Some(why)) => gaps.entry(why).or_default().push(&probe.message),
            (false, None) => errors.push(format!(
                "no walk reached it: {} ({})",
                probe.message, probe.file
            )),
        }
    }
    for name in named.keys() {
        if !probes.iter().any(|probe| probe.message == *name) {
            errors.push(format!("GAPS names a probe the core does not have: {name}"));
        }
    }
    for unused in &union.unused {
        errors.push(format!("a walk never made the move {unused}"));
    }

    let reached = probes.iter().filter(|p| union.hits(p.key()) > 0).count();
    let mut report = format!(
        "{reached} of {} core probes reached{}\n{}",
        probes.len(),
        if edges {
            format!(
                "; {} of {} core edges covered",
                moonpool_explorer::sancov_edges_covered_live(),
                moonpool_explorer::sancov_edge_count()
            )
        } else {
            String::new()
        },
        union.report
    );
    for (why, list) in &gaps {
        let _ = writeln!(report, "unreached, {why}:");
        for probe in list {
            let _ = writeln!(report, "  {probe}");
        }
    }
    for note in &notes {
        let _ = writeln!(report, "{note}");
    }
    eprintln!("{report}");
    assert!(errors.is_empty(), "{}\n{report}", errors.join("\n"));
}
