//! The quorums chapter (#1149 (quorums chapter)): the runs of
//! `/paxos/quorums/`, each told as a story in beats.
//!
//! The story: a majority, then the one rule (a Phase 1 quorum meets every
//! Phase 2 quorum), then the grid, where the two shapes differ, then the
//! partitions of `QuorumSystem::partitions`. The base run ends with a screen of
//! choices; every other run ends with a way back to it. There is no flexible
//! run: step 0.12 of the rework deleted `QuorumSystem::Flexible` (decision 27).

use super::{any, decree_outcome, kind, log_outcome, touches, voted};
use crate::paxos::action::{Action, Phase};
use crate::paxos::level::common::{crash, open, propose, start_election as elect};
use crate::paxos::trace::{Recorder, Scenario, Trace, trace};
use crate::paxos::view::{MessageView, RoleView, WorldView};

/// Some live node holds `value` chosen at `slot`.
fn chosen_at(slot: u64, value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world.nodes.iter().filter(|node| node.alive).any(|node| {
            node.accepted
                .iter()
                .any(|record| record.slot == slot && record.chosen && record.value == value)
        })
    }
}

/// No live node holds a chosen record at `slot`.
fn open_slot(slot: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world.nodes.iter().filter(|node| node.alive).all(|node| {
            node.accepted
                .iter()
                .all(|record| record.slot != slot || !record.chosen)
        })
    }
}

/// Node `id` leads.
fn leads(id: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .nodes
            .iter()
            .any(|node| node.id == id && node.role == Some(RoleView::Leader))
    }
}

/// The setup: six acceptors in a grid of 2 rows and 3 columns.
const LEVEL: &str = "act4/the-grid";

/// The beat of the base run that lists the other runs.
const TURN: usize = 6;

/// From now on, both phases reach only acceptors `a` and `b`.
fn reach(run: &mut Recorder, a: u64, b: u64) {
    for (phase, name) in [(Phase::One, 1), (Phase::Two, 2)] {
        run.play(
            Action::SetReach {
                phase,
                nodes: vec![a, b],
            },
            &format!("Phase {name} now reaches only acceptors {a} and {b}"),
        );
    }
}

/// Open a run: node 0 wins an election, as in the first story.
fn lead(run: &mut Recorder) {
    run.beat("Node 0 wins an election, as in the first story.")
        .play(elect(0), "node 0 starts an election")
        .settle(any);
}

/// End a run: a screen that goes back to the list of runs.
fn back(run: &mut Recorder, says: &str) {
    run.beat(says).choice("Choose another case", "base", TURN);
}

pub(super) fn quorums() -> Trace {
    trace(
        "quorums",
        LEVEL,
        vec![
            base(),
            majority(),
            break_the_rule(),
            election(),
            column_down(),
            stuck_column(),
            two_shapes(),
            partitions(),
            tour_majority(),
            tour_grids(),
        ],
    )
}

/// One node of `nodes` sends or receives `message`.
fn touches_any(nodes: &'static [u64]) -> impl Fn(&MessageView) -> bool {
    move |message| nodes.iter().any(|node| touches(*node)(message))
}

/// A `kind` message to one node of `nodes`.
fn kind_to(kind: &'static str, nodes: &'static [u64]) -> impl Fn(&MessageView) -> bool {
    move |message| message.kind == kind && nodes.contains(&message.to)
}

/// The tour run of chapter 2, "Who must answer": five nodes and one shape,
/// a majority. Three of five answer each phase; the node that two majorities
/// share carries the vote to the next ballot; two nodes down and the writes
/// go on; a third down and a write waits, with nothing lost.
fn tour_majority() -> Scenario {
    let mut run = Recorder::new("act2/the-read-that-lies");
    run.tour("majority", "Who must answer");
    run.beat("Five nodes, and each one is an acceptor. Each phase waits for a majority: 3 of 5.")
        .beat("Node 0 starts an election. Its Prepares to nodes 3 and 4 are lost.")
        .play(elect(0), "node 0 starts an election")
        .drop_all(kind_to("Prepare", &[3, 4]))
        .beat("Nodes 1 and 2 promise. With its own promise, node 0 has 3 of 5, so it leads.")
        .settle(any)
        .holds(leads(0), "node 0 leads with 3 promises of 5");
    run.beat("A client writes alpha at slot 0. The Accepts to nodes 3 and 4 are lost too.")
        .play(propose(0, "alpha"), "the client writes alpha")
        .drop_all(kind_to("Accept", &[3, 4]))
        .beat("Nodes 0, 1 and 2 vote: 3 of 5. alpha is chosen, and only these three nodes know it.")
        .settle_dropping(touches_any(&[3, 4]))
        .holds(chosen_at(0, "alpha"), "alpha is chosen at slot 0");
    run.beat("Now nodes 0 and 1 crash. Of the three nodes that voted, only node 2 is up.")
        .play(crash(0), "node 0 crashes")
        .play(crash(1), "node 1 crashes");
    run.beat("Node 4 starts an election. Nodes 2, 3 and 4 are up: just a majority.")
        .play(elect(4), "node 4 starts an election")
        .settle(|message| message.kind == "Prepare" && ![0, 1].contains(&message.to))
        .drop_all(touches_any(&[0, 1]))
        .beat("Two majorities of five always share a node. Here it is node 2, and its promise reports alpha.")
        .more("break-the-rule")
        .settle(|message| message.kind == "Promise")
        .holds(leads(4), "node 4 leads with nodes 2 and 3");
    run.beat(
        "So node 4 proposes alpha again at slot 0. Nodes 2, 3 and 4 vote, and alpha stays chosen.",
    )
    .settle_dropping(touches_any(&[0, 1]))
    .holds(chosen_at(0, "alpha"), "alpha is still chosen at slot 0");
    run.beat("Two nodes of five are down, and writes go on. A client writes bravo at slot 1.")
        .play(propose(4, "bravo"), "the client writes bravo")
        .settle_dropping(touches_any(&[0, 1]))
        .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1");
    run.beat("Now node 3 crashes too. Only nodes 2 and 4 are up: 2 of 5, less than a majority.")
        .play(crash(3), "node 3 crashes");
    run.beat("A client writes charlie. Only nodes 2 and 4 can vote, so charlie waits.")
        .play(propose(4, "charlie"), "the client writes charlie")
        .settle_dropping(touches_any(&[0, 1, 3]))
        .holds(open_slot(2), "slot 2 waits for a third vote");
    run.beat("Nothing is lost. Node 3 restarts, and node 4 sends its open Accept again.")
        .play(Action::Restart { node: 3 }, "node 3 restarts")
        .play(
            Action::ResendPending { node: 4 },
            "node 4 sends its open Accepts again",
        )
        .beat("Node 3 votes, and charlie is chosen. A majority of 5 nodes survives 2 failures.")
        .more("majority")
        .settle_dropping(touches_any(&[0, 1]))
        .holds(chosen_at(2, "charlie"), "charlie is chosen at slot 2");
    run.finish("tour-majority", "Who must answer", log_outcome)
}

/// The tour run of chapter 5, "Grids": the same six acceptors as a grid of
/// 2 rows and 3 columns. One node of every column elects; one full column
/// writes; a node down stops its column only, and the leader moves the slot.
fn tour_grids() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.tour("grids", "Grids");
    run.beat("Six acceptors stand in a grid of 2 rows and 3 columns. A majority would be 4 of 6.")
        .beat("The grid has two shapes. To elect: one node of every column. To write: one full column.")
        .beat("Node 0 starts an election. Its Prepares to nodes 3, 4 and 5 are lost.")
        .play(elect(0), "node 0 starts an election")
        .drop_all(kind_to("Prepare", &[3, 4, 5]))
        .beat("Nodes 1 and 2 promise. With node 0, that is one node of each column, so node 0 leads.")
        .more("election")
        .settle(any)
        .holds(leads(0), "node 0 leads with one node of each column");
    run.beat("Slot 0 belongs to column 0: nodes 0 and 3. A client writes alpha, and node 3 votes.")
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(kind("Accept"))
        .beat("The full column voted, so alpha is chosen. Two acceptors of six did the work.")
        .settle(any)
        .holds(chosen_at(0, "alpha"), "alpha is chosen at slot 0");
    run.beat("Why is it safe? A full column holds one node of every election quorum.")
        .beat("So a later leader always hears from a node of the column that chose alpha.")
        .more("two-shapes");
    run.beat("Now node 4 crashes. It is one of the two nodes of column 1.")
        .play(crash(4), "node 4 crashes");
    run.beat("Slot 1 belongs to column 1. A client writes bravo, but node 4 cannot vote, so bravo waits.")
        .more("column-down")
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(4))
        .holds(open_slot(1), "slot 1 waits for node 4");
    run.beat("The leader moves slot 1 to column 2: the same ballot and the same value, to nodes 2 and 5.")
        .more("stuck-column")
        .play(
            Action::MoveRound {
                node: 0,
                slot: 1,
                column: 2,
            },
            "the leader moves slot 1 to column 2",
        )
        .settle_dropping(touches(4))
        .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1");
    run.beat("A node down stops the slots of its column only. The other columns go on.")
        .beat("Partitions are another shape: a majority of one partition writes, a majority of each elects.")
        .more("partitions")
        .beat("A grid writes to few acceptors. The price: each election must ask every column.");
    run.finish("tour-grids", "Grids", log_outcome)
}

/// The base run: node 0 is elected by one node of each column, then each
/// write goes to one column.
fn base() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat(
        "Six acceptors stand in a grid of 2 rows and 3 columns. A majority would need 4 of 6 \
         for each write. The grid uses one shape to elect and another shape to write.",
    )
    .beat("Node 0 starts an election. Phase 1 needs a promise from one acceptor of every column.")
    .play(elect(0), "node 0 starts an election")
    .settle(kind("Prepare"))
    .beat("The promises cover the three columns, so node 0 is the leader.")
    .settle(any)
    .beat(
        "Slot 0 belongs to column 0: nodes 0 and 3. Node 0 votes for itself and sends the \
         Accept to node 3 only.",
    )
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle(kind("Accept"))
    .beat("The full column voted, so alpha is chosen. Two acceptors of six did the work.")
    .settle(any)
    .beat("Slot 1 belongs to column 1: nodes 1 and 4. A second write touches two other nodes.")
    .play(propose(0, "bravo"), "the client writes bravo")
    .settle(any)
    .beat("Now choose a case.")
    .choice("Compare: a majority of 5", "majority", 0)
    .choice("Two quorums that do not meet", "break-the-rule", 0)
    .choice("Elect with half the nodes down", "election", 0)
    .choice("A node of a column is down", "column-down", 0)
    .choice("Move a stuck slot to another column", "stuck-column", 0)
    .choice("The leader dies after a write", "two-shapes", 0)
    .choice("Partitions", "partitions", 0);
    run.finish(
        "base",
        "Grid: one of each column, then a column",
        log_outcome,
    )
}

/// A majority of five: the one shape for both phases.
fn majority() -> Scenario {
    let mut run = Recorder::new("act2/the-read-that-lies");
    run.beat(
        "Five acceptors and one shape for both phases: 3 of 5. Any two majorities share an \
         acceptor.",
    )
    .play(elect(0), "node 0 starts an election")
    .settle(kind("Prepare"))
    .beat("Three promises of five elect node 0.")
    .settle(any)
    .beat("Each write needs 3 votes of 5 too. A majority pays the same price on each write.")
    .ask(
        "Five acceptors, majority quorums. How many acceptors can be down while the journal \
         still elects and writes?",
        &["One", "Two", "Three"],
        1,
        "Three of five is a majority, so two can be down. Every write still touches three \
         acceptors.",
    )
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle(any)
    .holds(chosen_at(0, "alpha"), "alpha is chosen at slot 0");
    back(
        &mut run,
        "A majority survives 2 failures of 5, but every write touches 3 acceptors.",
    );
    run.finish("majority", "Majority: 3 of 5", log_outcome)
}

/// Break the rule: two proposers count their quorums in two halves that
/// share no acceptor, and two values are chosen.
fn break_the_rule() -> Scenario {
    let mut run = Recorder::new("act4/four-acceptors");
    run.beat(
        "The one rule: every Phase 1 quorum must share an acceptor with every Phase 2 quorum. \
         Here it is broken. Proposer 5 counts its quorums in acceptors 1 and 2 only.",
    )
    .play(
        Action::Reconfigure {
            node: 5,
            members: vec![1, 2],
            quorum: None,
        },
        "proposer 5 counts its quorums in acceptors 1 and 2 only",
    );
    reach(&mut run, 1, 2);
    run.beat("Proposer 5 gets alpha chosen by acceptors 1 and 2.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"));
    run.beat("Proposer 8 counts its quorums in acceptors 3 and 4 only.")
        .play(
            Action::Reconfigure {
                node: 8,
                members: vec![3, 4],
                quorum: None,
            },
            "proposer 8 counts its quorums in acceptors 3 and 4 only",
        );
    reach(&mut run, 3, 4);
    run.beat(
        "Its Phase 1 asks acceptors 3 and 4. Neither one voted, so proposer 8 never hears of \
         alpha.",
    )
    .ask(
        "alpha is chosen by acceptors 1 and 2. Proposer 8 asks only acceptors 3 and 4 for a \
         promise. Does it hear of alpha?",
        &[
            "Yes: alpha is chosen",
            "No: its quorum shares no acceptor with the quorum that chose alpha",
        ],
        1,
        "A proposer learns only what its own quorum reports. Two quorums that share no \
         acceptor cannot see each other.",
    )
    .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo")
    .settle(kind("Prepare"))
    .settle(kind("Promise"))
    .beat("Proposer 8 gets bravo chosen. Two values are chosen.")
    .find(
        "Two values are chosen. What broke Paxos?",
        &[
            "Proposer 8 proposed bravo after a Phase 1 with no vote",
            "The two proposers counted quorums that share no acceptor",
            "Acceptors 3 and 4 voted for bravo",
        ],
        1,
        "Each proposer followed Paxos inside its own half. The rule that broke is between the \
         halves: every Phase 1 quorum must share an acceptor with every Phase 2 quorum.",
    )
    .settle(|message| message.kind != "Commit")
    .drop_all(kind("Commit"))
    .holds(voted(1, "alpha"), "acceptor 1 keeps its vote for alpha")
    .holds(voted(3, "bravo"), "acceptor 3 votes for bravo")
    .holds(voted(4, "bravo"), "acceptor 4 votes for bravo");
    back(
        &mut run,
        "Two quorums that share no acceptor cannot see each other. Every shape in paros keeps \
         the rule.",
    );
    run.finish(
        "break-the-rule",
        "Break the rule: two quorums that do not meet",
        decree_outcome,
    )
}

/// An election on the grid is not a full row: one node of each column is
/// enough, so it finishes with three of six nodes down.
fn election() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    let down = |message: &MessageView| [1, 2, 3].into_iter().any(|node| touches(node)(message));
    run.beat("Nodes 1, 2 and 3 crash. Half of the grid is down.")
        .play(crash(1), "node 1 crashes")
        .play(crash(2), "node 2 crashes")
        .play(crash(3), "node 3 crashes");
    run.beat(
        "Node 0 starts an election. Its Prepare reaches nodes 4 and 5 only. No row is complete.",
    )
    .play(elect(0), "node 0 starts an election")
    .drop_all(down)
    .settle(|message| message.kind != "Promise")
    .beat(
        "Node 0 holds column 0, node 4 column 1 and node 5 column 2. One node of each column is \
         a Phase 1 quorum, so node 0 is the leader.",
    )
    .ask(
        "Nodes 1, 2 and 3 are down. Nodes 0, 4 and 5 promise: one node of each column, and no \
         full row. Is node 0 elected?",
        &[
            "No: Phase 1 needs a full row",
            "Yes: one node of each column is enough",
        ],
        1,
        "A write needs a full column. Every full column holds one node of the election \
         quorum, so one node of each column is enough to see every write.",
    )
    .settle(any)
    .holds(leads(0), "node 0 is elected with half of the grid down");
    back(
        &mut run,
        "The election finished with 3 of 6 nodes down. A write still needs a full column, and \
         each column has a node down now.",
    );
    run.finish(
        "election",
        "Elect with one node of each column",
        log_outcome,
    )
}

/// Node 3 is down: column 0 cannot choose slot 0, and the other columns go
/// on. Slot 0 waits until node 3 is back.
fn column_down() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat("Node 3 crashes. It is one of the two nodes of column 0.")
        .play(crash(3), "node 3 crashes");
    run.beat("Slot 0 goes to column 0. Node 3 cannot vote, so alpha is not chosen.")
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle_dropping(touches(3));
    run.beat("Slot 1 goes to column 1. Its column is complete, so bravo is chosen.")
        .ask(
            "Node 3 of column 0 is down, so slot 0 is not chosen. Slot 1 goes to column 1. Is \
             bravo chosen?",
            &[
                "No: slot 0 must be chosen first",
                "Yes: column 1 is complete",
            ],
            1,
            "Each slot has its own vote. The log applies in order, so slot 1 waits to be \
             applied, but it is chosen.",
        )
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(3))
        .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1")
        .holds(open_slot(0), "slot 0 is still open");
    run.beat(
        "Node 3 restarts, and the leader sends its open Accept again. Column 0 votes, and alpha \
         is chosen.",
    )
    .play(Action::Restart { node: 3 }, "node 3 restarts")
    .play(
        Action::ResendPending { node: 0 },
        "the leader sends its open Accepts again",
    )
    .settle(any);
    back(
        &mut run,
        "One node down stops the slots of its column only. The log applies in order, so slot \
         1 waited for slot 0.",
    );
    run.finish(
        "column-down",
        "What if a node of the column is down?",
        log_outcome,
    )
}

/// Node 3 stays down: the leader moves slot 0 to column 1, with the same
/// ballot and the same value.
fn stuck_column() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat("Node 3 crashes, and it does not come back.")
        .play(crash(3), "node 3 crashes");
    run.beat("Slot 0 goes to column 0. Without node 3, alpha is not chosen.")
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle_dropping(touches(3));
    run.beat(
        "The leader moves slot 0 to column 1: the same ballot and the same value, now sent to \
         nodes 1 and 4.",
    )
    .play(
        Action::MoveRound {
            node: 0,
            slot: 0,
            column: 1,
        },
        "the leader moves slot 0 to column 1",
    )
    .settle_dropping(touches(3));
    back(
        &mut run,
        "This is safe. A slot has one value in one ballot, so the old votes do no harm, and \
         every Phase 1 quorum meets column 1 too.",
    );
    run.finish(
        "stuck-column",
        "Move a stuck slot to another column",
        log_outcome,
    )
}

/// The price of two shapes: the value is on column 0 only. The leader dies,
/// and the next leader finds the value because its Phase 1 needs a node of
/// column 0.
fn two_shapes() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat("Column 0, nodes 0 and 3, votes for alpha at slot 0.")
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"));
    run.beat(
        "Node 0 crashes before it tells the others. Only node 3 still holds the vote for alpha.",
    )
    .play(crash(0), "node 0 crashes");
    run.beat(
        "Node 1 starts an election. Its Phase 1 needs a node of column 0, and node 3 is the \
         only one left.",
    )
    .ask(
        "Only node 3 still holds the vote for alpha. Node 1 starts an election. Must its \
         Phase 1 hear from node 3?",
        &[
            "No: any three nodes are enough",
            "Yes: it needs a node of column 0, and node 3 is the only one left",
        ],
        1,
        "An election needs one node of every column. A full column may have chosen a value, \
         so the election must ask that column.",
    )
    .play(elect(1), "node 1 starts an election")
    .drop_all(touches(0))
    .settle(|message| message.kind != "Accept");
    run.beat(
        "Node 3 reports its vote, so node 1 proposes alpha again at slot 0. Node 3 votes, but \
         column 0 is not complete: node 0 is down.",
    )
    .settle_dropping(touches(0));
    run.beat("Node 1 moves slot 0 to column 1, and alpha is chosen again, not lost.")
        .play(
            Action::MoveRound {
                node: 1,
                slot: 0,
                column: 1,
            },
            "node 1 moves slot 0 to column 1",
        )
        .settle_dropping(touches(0));
    back(
        &mut run,
        "This is the price of two shapes. A write touches one column, so an election must ask \
         every column.",
    );
    run.finish("two-shapes", "The leader dies after a write", log_outcome)
}

/// Two partitions of three acceptors: a majority of one partition chooses,
/// a majority of every partition elects.
fn partitions() -> Scenario {
    let mut run = Recorder::new("act4/partitions");
    run.beat(
        "A journal can be split into partitions. Here: two partitions of three acceptors. \
         Partition 0 is nodes 0, 2 and 4. Partition 1 is nodes 1, 3 and 5.",
    )
    .beat("Node 4 crashes.")
    .play(crash(4), "node 4 crashes");
    run.beat(
        "Node 0 starts an election. Phase 1 needs a majority of every partition: 2 of 3, two \
         times.",
    )
    .play(elect(0), "node 0 starts an election")
    .settle_dropping(touches(4));
    run.beat(
        "Slot 0 goes to partition 0. Two votes of three choose alpha, so node 4 is not missed.",
    )
    .ask(
        "Node 4 of partition 0 is down. Slot 0 goes to partition 0. Can alpha be chosen?",
        &[
            "No: partition 0 lost a node",
            "Yes: 2 of its 3 nodes are a majority of the partition",
        ],
        1,
        "A write needs a majority of one partition. Each partition survives one failure.",
    )
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle_dropping(touches(4))
    .holds(chosen_at(0, "alpha"), "alpha is chosen at slot 0");
    run.beat("Slot 1 goes to partition 1, and 2 of its 3 nodes choose bravo.")
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(4));
    back(
        &mut run,
        "Each partition survives one failure. More partitions spread the writes, and each \
         election asks all of them.",
    );
    run.finish(
        "partitions",
        "Partitions: 2 of 3, one or every partition",
        log_outcome,
    )
}
