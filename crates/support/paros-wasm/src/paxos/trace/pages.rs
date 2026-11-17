//! The five pages' runs. Each run is a short script on a level's own setup;
//! the messages, the notes and the lane states are the engine's.

use crate::paxos::WorldKind;
use crate::paxos::action::{Action, BallotSpec};
use crate::paxos::level::common::{crash, open, propose, start_election as elect};
use crate::paxos::trace::{Recorder, Scenario, Trace, trace};
use crate::paxos::view::{MessageView, PartyView};
use paros_core::NodeId;

mod multi_paxos;
mod quorums;
mod single_decree;

use multi_paxos::multi_paxos;
use quorums::quorums;
use single_decree::single_decree;

/// The script that records one page's trace.
type Record = fn() -> Trace;

/// The pages, in site order, and the script that records each.
const PAGES: &[(&str, Record)] = &[
    ("single-decree", single_decree),
    ("multi-paxos", multi_paxos),
    ("matchmaker", matchmaker),
    ("compartmentalized", compartmentalized),
    ("quorums", quorums),
];

/// Every page's trace, in site order.
#[must_use]
pub fn traces() -> Vec<Trace> {
    PAGES.iter().map(|(_, record)| record()).collect()
}

/// The trace of one page, recorded now; `None` for an unknown page.
#[must_use]
pub fn trace_of(page: &str) -> Option<Trace> {
    PAGES
        .iter()
        .find(|(name, _)| *name == page)
        .map(|(_, record)| record())
}

// ---- shorthands --------------------------------------------------------------

pub(super) fn any(_: &MessageView) -> bool {
    true
}

pub(super) fn kind(name: &'static str) -> impl Fn(&MessageView) -> bool {
    move |message| message.kind == name
}

/// Every message to or from node `node` (not a matchmaker: the two id spaces
/// are different).
pub(super) fn touches(node: u64) -> impl Fn(&MessageView) -> bool {
    move |message| {
        (message.to == node && message.to_party == PartyView::Node)
            || (message.from == node && message.from_party == PartyView::Node)
    }
}

/// The value the single-decree world chose, or that it chose none.
pub(super) fn decree_outcome(world: &WorldKind) -> String {
    if let Some(broken) = world.decree().and_then(|decree| decree.violation()) {
        return broken.to_string();
    }
    match world.view().chosen {
        Some(chosen) => format!("{} is chosen at ballot {}.", chosen.value, chosen.ballot),
        None => "No value is chosen. The run waits for a majority.".to_string(),
    }
}

/// What the live nodes of a log world know is chosen, grouped by node, and
/// where each group's log starts after a truncation.
pub(super) fn log_outcome(world: &WorldKind) -> String {
    let view = world.view();
    let mut groups: Vec<(Vec<u64>, String)> = Vec::new();
    for node in view.nodes.iter().filter(|node| node.alive) {
        let chosen: Vec<String> = node
            .accepted
            .iter()
            .filter(|record| record.chosen)
            .map(|record| format!("slot {} = {}", record.slot, record.value))
            .collect();
        let mut known = chosen.join(", ");
        if let Some(floor) = node.floor.filter(|floor| *floor > 0) {
            known = if known.is_empty() {
                format!("dropped every slot below slot {floor}")
            } else {
                format!("dropped every slot below slot {floor}, then {known}")
            };
        } else if known.is_empty() {
            known = "no chosen slot".to_string();
        }
        match groups.iter_mut().find(|(_, held)| *held == known) {
            Some((ids, _)) => ids.push(node.id),
            None => groups.push((vec![node.id], known)),
        }
    }
    let lines: Vec<String> = groups
        .into_iter()
        .map(|(ids, known)| {
            let ids: Vec<String> = ids.iter().map(u64::to_string).collect();
            let who = if ids.len() == 1 {
                format!("Node {}", ids[0])
            } else {
                format!("Nodes {}", ids.join(", "))
            };
            format!("{who}: {known}.")
        })
        .collect();
    lines.join(" ")
}

// ---- Matchmaker Paxos --------------------------------------------------------

fn matchmaker() -> Trace {
    const LEVEL: &str = "act4/reconfigure";

    fn start(run: &mut Recorder) {
        run.play(elect(0), "node 0 starts an election").settle(any);
        run.play(propose(0, "alpha"), "the client writes alpha")
            .settle(any);
    }

    let mut base = Recorder::new(LEVEL);
    start(&mut base);
    base.play(
        Action::Reconfigure {
            node: 0,
            members: vec![0, 1, 2, 3],
            quorum: None,
        },
        "the operator adds node 3 to the acceptors",
    )
    .settle(any);
    base.play(propose(0, "bravo"), "the client writes bravo")
        .settle(any);

    let mut crashed = Recorder::new(LEVEL);
    start(&mut crashed);
    crashed.play(crash(0), "node 0 crashes");
    crashed
        .play(elect(1), "node 1 starts an election")
        .settle_dropping(touches(0));
    crashed
        .play(propose(1, "bravo"), "the client writes bravo")
        .settle_dropping(touches(0));

    trace(
        "matchmaker",
        LEVEL,
        vec![
            base.finish("base", "No fault", log_outcome),
            crashed.finish(
                "leader-crash",
                "What if the leader crashes and node 1 takes over?",
                log_outcome,
            ),
        ]
        .into_iter()
        .chain(matchmaker_changes(LEVEL))
        .collect(),
    )
}

/// The Matchmaker Paxos runs that change the acceptors (#944 (matchmaker
/// runs)), and the run that skips the matchmakers.
fn matchmaker_changes(level: &str) -> Vec<Scenario> {
    fn start(run: &mut Recorder) {
        run.play(elect(0), "node 0 starts an election").settle(any);
        run.play(propose(0, "alpha"), "the client writes alpha")
            .settle(any);
    }

    // Replace an acceptor: node 2 leaves and node 3 joins. Then the leader
    // crashes, and node 3 campaigns: its Phase 1 must cover the old set and
    // the new one.
    let mut replace = Recorder::new(level);
    start(&mut replace);
    replace
        .play(
            Action::Reconfigure {
                node: 0,
                members: vec![0, 1, 3],
                quorum: None,
            },
            "the operator replaces node 2 with node 3",
        )
        .settle(any);
    replace.play(crash(0), "node 0 crashes");
    replace
        .play(elect(3), "node 3 starts an election")
        .settle_dropping(touches(0));
    replace
        .play(propose(3, "bravo"), "the client writes bravo")
        .settle_dropping(touches(0));

    // Remove an acceptor, wait until a matchmaker quorum forgets the old
    // set, then retire the node.
    let mut retire = Recorder::new("act4/garbage-collection");
    start(&mut retire);
    retire
        .play(
            Action::Reconfigure {
                node: 0,
                members: vec![0, 1, 2],
                quorum: None,
            },
            "the operator removes node 3",
        )
        .settle(any);
    let mut watermark = None;
    for _ in 0..12 {
        watermark = retire
            .world()
            .log()
            .and_then(|log| log.gc_effective(NodeId(0)))
            .map(|(ballot, _)| ballot);
        if watermark.is_some() {
            break;
        }
        retire
            .play(Action::Tick { node: 0 }, "the leader's clock ticks")
            .settle(any);
    }
    let watermark = watermark.expect("a matchmaker quorum holds the floor");
    let retire_node_3 = || Action::Retire {
        node: 0,
        target: 3,
        gc_watermark: Some(BallotSpec {
            round: watermark.round,
            node: watermark.node.0,
        }),
    };
    // Node 3 first asks the matchmakers which registrations name it
    // (#1002 (retire fence gaps)), then the operator retires it again.
    retire.play(retire_node_3(), "the operator asks node 3 to retire");
    retire.settle(any);
    retire.play(retire_node_3(), "the operator retires node 3");
    retire.settle(any);

    // Break the rule: proposer 8 gets a new set of acceptors with no
    // matchmaker, so its Phase 1 asks only the new set.
    let mut skipped = Recorder::new("act1/the-duel");
    skipped
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| {
            !(message.kind == "Accept" && message.to == 3) && message.kind != "Commit"
        })
        .drop_all(|message| message.kind == "Accept" || message.kind == "Commit");
    skipped.play(
        Action::Reconfigure {
            node: 8,
            members: vec![3],
            quorum: None,
        },
        "proposer 8 moves to acceptor 3 alone, with no matchmaker",
    );
    skipped
        .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo")
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"));

    vec![
        replace.finish(
            "replace",
            "Replace an acceptor, then lose the leader",
            log_outcome,
        ),
        retire.finish(
            "retire",
            "Remove an acceptor, forget the old set, retire it",
            log_outcome,
        ),
        skipped.finish(
            "break-the-rule",
            "Break the rule: skip the matchmakers",
            decree_outcome,
        ),
    ]
}

// ---- Compartmentalized Paxos: the handoff ------------------------------------

fn compartmentalized() -> Trace {
    // The handoff keeps its own run, on the colocated world of its level.
    let mut handoff = Recorder::new("act4/the-handoff");
    handoff
        .play(elect(0), "node 0 starts an election")
        .settle(any);
    handoff
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(any);
    handoff
        .play(
            Action::Relinquish { node: 0, to: 1 },
            "node 0 hands the leadership to node 1",
        )
        .settle(any);
    handoff
        .play(propose(1, "bravo"), "the client writes bravo")
        .settle(any);
    super::compart::trace(handoff.finish(
        "handoff",
        "Move the leader with no election",
        log_outcome,
    ))
}
