//! The five pages' runs. Each run is a short script on a level's own setup;
//! the messages, the notes and the lane states are the engine's.

use crate::paxos::WorldKind;
use crate::paxos::action::Action;
use crate::paxos::trace::{Recorder, Trace, trace};
use crate::paxos::view::{MessageView, PartyView};

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

const CLIENT: u64 = 7;

fn open(proposer: u64, value: &str) -> Action {
    Action::OpenBallot {
        proposer,
        value: value.to_string(),
    }
}

fn elect(node: u64) -> Action {
    Action::StartElection { node }
}

fn propose(node: u64, value: &str) -> Action {
    Action::Propose {
        node,
        client: CLIENT,
        value: value.to_string(),
        column: None,
    }
}

fn crash(node: u64) -> Action {
    Action::Crash { node }
}

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
    match world.view().chosen {
        Some(chosen) => format!("{} is chosen at ballot {}.", chosen.value, chosen.ballot),
        None => "No value is chosen. The run waits for a majority.".to_string(),
    }
}

/// What the live nodes of a log world know is chosen, grouped by node.
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
        let chosen = chosen.join(", ");
        match groups.iter_mut().find(|(_, known)| *known == chosen) {
            Some((ids, _)) => ids.push(node.id),
            None => groups.push((vec![node.id], chosen)),
        }
    }
    let lines: Vec<String> = groups
        .into_iter()
        .map(|(ids, chosen)| {
            let ids: Vec<String> = ids.iter().map(u64::to_string).collect();
            let who = if ids.len() == 1 {
                format!("Node {}", ids[0])
            } else {
                format!("Nodes {}", ids.join(", "))
            };
            if chosen.is_empty() {
                format!("{who} know no chosen slot.")
            } else {
                format!("{who} know: {chosen}.")
            }
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
            down.finish("acceptor-down", "What if a3 is down?", decree_outcome),
            second.finish(
                "second-proposer",
                "What if a second proposer starts?",
                decree_outcome,
            ),
        ],
    )
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
        ],
    )
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
    let mut down = Recorder::new(LEVEL);
    start(&mut down);
    down.play(crash(3), "node 3 crashes");
    down.play(propose(0, "alpha"), "the client writes alpha")
        .settle_dropping(touches(3));

    trace(
        "quorums",
        LEVEL,
        vec![
            base.finish("base", "No fault", log_outcome),
            down.finish(
                "column-down",
                "What if a node of the column is down?",
                log_outcome,
            ),
        ],
    )
}
