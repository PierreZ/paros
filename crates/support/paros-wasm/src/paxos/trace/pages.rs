//! The five pages' runs. Each run is a short script on a level's own setup;
//! the messages, the notes and the lane states are the engine's.

use crate::paxos::WorldKind;
use crate::paxos::action::{Action, BallotSpec, Phase};
use crate::paxos::level::common::{crash, open, propose, start_election as elect};
use crate::paxos::trace::{Recorder, Scenario, Trace, trace};
use crate::paxos::view::{MessageView, PartyView};
use paros_core::NodeId;

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

fn any(_: &MessageView) -> bool {
    true
}

fn kind(name: &'static str) -> impl Fn(&MessageView) -> bool {
    move |message| message.kind == name
}

/// Every message to or from node `node` (not a matchmaker: the two id spaces
/// are different).
fn touches(node: u64) -> impl Fn(&MessageView) -> bool {
    move |message| {
        (message.to == node && message.to_party == PartyView::Node)
            || (message.from == node && message.from_party == PartyView::Node)
    }
}

/// The value the single-decree world chose, or that it chose none.
fn decree_outcome(world: &WorldKind) -> String {
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
fn log_outcome(world: &WorldKind) -> String {
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

// ---- single decree -----------------------------------------------------------

fn single_decree() -> Trace {
    const LEVEL: &str = "act1/the-duel";

    let mut base = Recorder::new(LEVEL);
    base.play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(any);

    let mut down = Recorder::new(LEVEL);
    down.play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle_dropping(touches(3));

    // Proposer 8 starts after a1 voted for alpha. Its Phase 1 reaches a1, so it
    // must propose alpha, not its own value.
    let mut second = Recorder::new(LEVEL);
    second
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.phase == "prepare" || message.phase == "promise");
    second
        .settle(|message| message.kind == "Accept" && message.to == 1)
        .settle(kind("Accepted"));
    second.play(
        open(8, "bravo"),
        "proposer 8 opens a higher ballot for bravo",
    );
    let eight = |message: &MessageView| message.from == 8 || message.to == 8;
    second
        .settle(move |message| {
            eight(message) && (message.phase == "prepare" || message.phase == "promise")
        })
        .settle(any);

    trace(
        "single-decree",
        LEVEL,
        vec![
            base.finish("base", "No fault", decree_outcome),
            down.finish(
                "acceptor-down",
                "What if acceptor 3 is down?",
                decree_outcome,
            ),
            second.finish(
                "second-proposer",
                "What if a second proposer starts?",
                decree_outcome,
            ),
        ]
        .into_iter()
        .chain(single_decree_faults(LEVEL))
        .collect(),
    )
}

/// The single-decree runs with a fault: a duel, a restart, lost votes, and
/// the run that breaks the rule.
fn single_decree_faults(level: &str) -> Vec<Scenario> {
    // The duel: each proposer's Phase 1 stops the other's Phase 2. Neither
    // value is chosen until one proposer waits.
    let mut duel = Recorder::new(level);
    duel.play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.phase == "prepare" || message.phase == "promise");
    duel.play(
        open(8, "bravo"),
        "proposer 8 opens a higher ballot for bravo",
    );
    let from_or_to = |id: u64| move |message: &MessageView| message.from == id || message.to == id;
    duel.settle(move |message| {
        from_or_to(8)(message) && (message.phase == "prepare" || message.phase == "promise")
    });
    // The Accepts of proposer 5 meet promises for 1.8: refused.
    duel.settle(|message| message.to == 5 || message.from == 5);
    duel.play(open(5, "alpha"), "proposer 5 opens a higher ballot again");
    duel.settle(|message| {
        (message.from == 5 || message.to == 5)
            && (message.phase == "prepare" || message.phase == "promise")
    });
    // Now the Accepts of proposer 8 are refused. Proposer 8 waits, and
    // proposer 5 finishes.
    duel.settle(|message| message.to == 8 || message.from == 8);
    duel.settle(any);

    // An acceptor crashes after its vote. Its disk keeps the vote, and the
    // next proposer hears of it.
    let mut restart = Recorder::new(level);
    restart
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.phase == "prepare" || message.phase == "promise");
    restart
        .settle(|message| message.kind == "Accept" && message.to == 1)
        .drop_all(|message| message.kind == "Accept");
    restart.play(Action::Crash { node: 1 }, "acceptor 1 crashes");
    restart.drop_all(kind("Accepted"));
    restart.play(Action::Restart { node: 1 }, "acceptor 1 restarts");
    restart.play(open(8, "bravo"), "proposer 8 opens a ballot for bravo");
    restart.settle(any);

    // Messages are lost: proposer 5 does not know that alpha is chosen. It
    // tries again at a higher ballot and finds alpha in the promises.
    let mut lost = Recorder::new(level);
    lost.play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.phase != "accepted" && message.phase != "commit");
    lost.drop_all(kind("Accepted"));
    lost.play(
        open(5, "alpha"),
        "proposer 5 tries again at a higher ballot",
    );
    lost.settle(any);

    // Break the rule: an acceptor loses its disk and answers again. paros
    // refuses this boot; the run shows why.
    let mut wiped = Recorder::new(level);
    wiped
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.phase == "prepare" || message.phase == "promise");
    wiped
        .settle(|message| message.kind == "Accept" && message.to != 3)
        .drop_all(|message| message.kind == "Accept")
        .settle(kind("Accepted"))
        .drop_all(kind("Commit"));
    wiped.play(Action::Wipe { node: 2 }, "acceptor 2 loses its disk");
    wiped.play(
        Action::SetReach {
            phase: Phase::One,
            nodes: vec![2, 3],
        },
        "Phase 1 now reaches only acceptors 2 and 3",
    );
    wiped.play(
        Action::SetReach {
            phase: Phase::Two,
            nodes: vec![2, 3],
        },
        "Phase 2 now reaches only acceptors 2 and 3",
    );
    wiped.play(open(8, "bravo"), "proposer 8 opens a ballot for bravo");
    wiped
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"));

    vec![
        duel.finish(
            "duel",
            "What if two proposers block each other?",
            decree_outcome,
        ),
        restart.finish(
            "acceptor-restart",
            "What if an acceptor crashes after its vote?",
            decree_outcome,
        ),
        lost.finish(
            "lost-message",
            "What if the votes are lost?",
            decree_outcome,
        ),
        wiped.finish(
            "break-the-rule",
            "Break the rule: an acceptor forgets its vote",
            decree_outcome,
        ),
    ]
}

// ---- Multi-Paxos -------------------------------------------------------------

fn multi_paxos() -> Trace {
    const LEVEL: &str = "act2/steady-state";

    fn start(run: &mut Recorder) {
        run.play(elect(0), "node 0 starts an election").settle(any);
        run.play(propose(0, "alpha"), "the client writes alpha")
            .settle(any);
    }

    let mut base = Recorder::new(LEVEL);
    start(&mut base);
    base.play(propose(0, "bravo"), "the client writes bravo")
        .settle(any);

    // The leader crashes after one follower accepted bravo. The next leader
    // finds that vote in Phase 1 and finishes the slot.
    let mut crashed = Recorder::new(LEVEL);
    start(&mut crashed);
    crashed.play(propose(0, "bravo"), "the client writes bravo");
    crashed
        .settle(|message| message.kind == "Accept" && message.to == 1)
        .drop_all(|message| message.kind == "Accept");
    crashed.play(crash(0), "node 0 crashes");
    crashed.drop_all(touches(0));
    crashed
        .play(elect(1), "node 1 starts an election")
        .settle_dropping(touches(0));

    // The gap: slot 0 is lost with the leader, slot 1 is chosen. The next
    // leader hears of no vote for slot 0 and fills it with a no-op.
    let mut gap = Recorder::new("act2/the-permanent-gap");
    start(&mut gap);
    gap.play(propose(0, "bravo"), "the client writes bravo");
    gap.play(propose(0, "charlie"), "the client writes charlie");
    gap.drop_all(|message| message.kind == "Accept" && message.slot == Some(1))
        .settle(any);
    gap.play(crash(0), "node 0 crashes");
    gap.play(elect(1), "node 1 starts an election")
        .settle_dropping(touches(0));

    // A node is down while the others write. When it is back, the leader's
    // heartbeat tells it what is chosen, and it asks for the slots it lacks.
    let mut lagging = Recorder::new(LEVEL);
    lagging
        .play(elect(0), "node 0 starts an election")
        .settle(any);
    lagging.play(crash(2), "node 2 crashes");
    lagging
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle_dropping(touches(2));
    lagging
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(2));
    lagging.play(Action::Restart { node: 2 }, "node 2 restarts");
    lagging
        .play(Action::Tick { node: 0 }, "the leader's clock ticks")
        .settle(any);

    // Truncate is a command in the log: the leader proposes it, the nodes
    // apply it, and the slots below it go.
    let mut truncate = Recorder::new("act3/truncate-by-consensus");
    start(&mut truncate);
    truncate
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle(any);
    truncate
        .play(
            Action::Compact { node: 0, up_to: 8 },
            "the client truncates the journal",
        )
        .settle(any);

    // A leaderless read: node 1 asks a quorum for the highest slot each one
    // voted in, and answers once it applied that far.
    let mut read = Recorder::new(LEVEL);
    start(&mut read);
    read.play(
        Action::QuorumRead {
            node: 1,
            client: Some(7),
        },
        "a client reads from node 1",
    )
    .settle(any);

    trace(
        "multi-paxos",
        LEVEL,
        vec![
            base.finish("base", "No fault", log_outcome),
            crashed.finish(
                "leader-crash",
                "What if the leader crashes during a write?",
                log_outcome,
            ),
            gap.finish(
                "gap",
                "What if a slot is lost with the leader?",
                log_outcome,
            ),
            lagging.finish("lagging", "What if a node falls behind?", log_outcome),
            truncate.finish("truncate", "Truncate the journal", log_outcome),
            read.finish("read", "Read without the leader", log_outcome),
        ],
    )
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
    retire.play(
        Action::Retire {
            node: 0,
            target: 3,
            gc_watermark: Some(BallotSpec {
                round: watermark.round,
                node: watermark.node.0,
            }),
        },
        "the operator retires node 3",
    );
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
    const LEVEL: &str = "act4/the-handoff";

    fn start(run: &mut Recorder) {
        run.play(elect(0), "node 0 starts an election").settle(any);
        run.play(propose(0, "alpha"), "the client writes alpha")
            .settle(any);
    }

    let mut base = Recorder::new(LEVEL);
    start(&mut base);
    base.play(
        Action::Relinquish { node: 0, to: 1 },
        "node 0 hands the leadership to node 1",
    )
    .settle(any);
    base.play(propose(1, "bravo"), "the client writes bravo")
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
        "compartmentalized",
        LEVEL,
        vec![
            base.finish("base", "No fault", log_outcome),
            crashed.finish(
                "leader-crash",
                "What if the leader crashes instead?",
                log_outcome,
            ),
        ],
    )
}

// ---- quorums: the grid -------------------------------------------------------

fn quorums() -> Trace {
    const LEVEL: &str = "act4/the-grid";

    fn start(run: &mut Recorder) {
        run.play(elect(0), "node 0 starts an election").settle(any);
    }

    let mut base = Recorder::new(LEVEL);
    start(&mut base);
    base.play(propose(0, "alpha"), "the client writes alpha")
        .settle(any);
    base.play(propose(0, "bravo"), "the client writes bravo")
        .settle(any);

    // A node of the column of slot 0 is down: the column is not complete, so
    // slot 0 is not chosen, even with five of six nodes up.
    // Then the other columns go on, and the slot of the broken column waits
    // until node 3 is back and the leader sends its Accept again.
    let mut down = Recorder::new(LEVEL);
    start(&mut down);
    down.play(crash(3), "node 3 crashes");
    down.play(propose(0, "alpha"), "the client writes alpha")
        .settle_dropping(touches(3));
    down.play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(3));
    down.play(Action::Restart { node: 3 }, "node 3 restarts");
    down.play(
        Action::ResendPending { node: 0 },
        "the leader sends its open Accepts again",
    )
    .settle(any);

    trace(
        "quorums",
        LEVEL,
        vec![
            base.finish("base", "Grid: a row, then a column", log_outcome),
            down.finish(
                "column-down",
                "What if a node of the column is down?",
                log_outcome,
            ),
        ]
        .into_iter()
        .chain(quorum_shapes())
        .collect(),
    )
}

/// One run per other quorum shape (#948 (quorum shapes and picker)), and
/// the run with two quorums that do not cross.
fn quorum_shapes() -> Vec<Scenario> {
    // A majority: 3 of 5 in both phases.
    let mut majority = Recorder::new("act2/the-read-that-lies");
    majority
        .play(elect(0), "node 0 starts an election")
        .settle(any);
    majority
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(any);

    // A flexible split over four acceptors: 3 answer Phase 1, 2 vote in
    // Phase 2. Proposer 8's Phase 1 meets the vote of proposer 5.
    let mut flexible = Recorder::new("act4/flexible-quorums");
    flexible.play(
        Action::SetReach {
            phase: Phase::Two,
            nodes: vec![1, 2],
        },
        "Phase 2 now reaches acceptors 1 and 2",
    );
    flexible
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(any);
    flexible.play(
        Action::SetReach {
            phase: Phase::One,
            nodes: vec![2, 3, 4],
        },
        "Phase 1 now reaches acceptors 2, 3 and 4",
    );
    flexible
        .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo")
        .settle(any);

    // Break the rule: proposer 8 counts with a quorum that does not cross
    // the quorum that chose alpha.
    let mut crossed = Recorder::new("act4/flexible-quorums");
    crossed.play(
        Action::SetReach {
            phase: Phase::Two,
            nodes: vec![1, 2],
        },
        "Phase 2 now reaches acceptors 1 and 2",
    );
    crossed
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"));
    crossed.play(
        Action::Reconfigure {
            node: 8,
            members: vec![3, 4],
            quorum: None,
        },
        "proposer 8 counts its quorums in acceptors 3 and 4 only",
    );
    crossed.play(
        Action::SetReach {
            phase: Phase::Two,
            nodes: vec![3, 4],
        },
        "Phase 2 now reaches acceptors 3 and 4",
    );
    crossed
        .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo")
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"));

    vec![
        majority.finish("majority", "Majority: 3 of 5", log_outcome),
        flexible.finish("flexible", "Flexible: 3 of 4, then 2 of 4", decree_outcome),
        crossed.finish(
            "break-the-rule",
            "Break the rule: two quorums that do not cross",
            decree_outcome,
        ),
    ]
}
