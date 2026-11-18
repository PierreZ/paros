//! The quorums chapter (#1149 (quorums chapter)): the runs of
//! `/paxos/quorums/`, each told as a story in beats.
//!
//! The story: a majority, then the one rule (a Phase 1 quorum meets every
//! Phase 2 quorum), then the grid, where the two shapes differ, then the
//! partitions of `QuorumSystem::partitions`. The base run ends with a screen of
//! choices; every other run ends with a way back to it. There is no flexible
//! run: step 0.12 of the rework deleted `QuorumSystem::Flexible` (decision 27).

use super::{any, decree_outcome, kind, log_outcome, touches};
use crate::paxos::action::{Action, Phase};
use crate::paxos::level::common::{crash, open, propose, start_election as elect};
use crate::paxos::trace::{Recorder, Scenario, Trace, trace};
use crate::paxos::view::MessageView;

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
        ],
    )
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
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle(any);
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
    .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo")
    .settle(kind("Prepare"))
    .settle(kind("Promise"))
    .beat("Proposer 8 gets bravo chosen. Two values are chosen.")
    .settle(|message| message.kind != "Commit")
    .drop_all(kind("Commit"));
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
    .settle(any);
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
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(3));
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
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle_dropping(touches(4));
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
