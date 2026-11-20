//! The Matchmaker Paxos chapter: the runs of `/paxos/matchmaker/`, each told
//! as a story in beats.
//!
//! The story: a leader registers its ballot and its acceptors at the
//! matchmakers before Phase 1, a change of the acceptors is a new ballot,
//! the next leader's Phase 1 covers every configuration the matchmakers
//! report, and an acceptor retires only once no future leader can need it.
//! The base run ends with a screen of choices; every other run ends with a
//! way back to it.

use super::{any, decree_outcome, kind, log_outcome, touches};
use crate::paxos::action::{Action, BallotSpec};
use crate::paxos::level::common::{crash, open, propose, start_election as elect};
use crate::paxos::trace::{Recorder, Scenario, Trace, trace};
use crate::paxos::view::{MessageView, PartyView, RoleView, WorldView};
use paros_consensus::NodeId;

/// The setup: four nodes (node 3 outside the acceptors at first) and two
/// matchmakers.
const LEVEL: &str = "act4/reconfigure";

/// The beat of the base run that lists the other runs.
const TURN: usize = 6;

/// A matchmaking message: the probe, the registration and their answers.
fn matchmaking(message: &MessageView) -> bool {
    message.kind == "MatchRequest" || message.kind == "MatchReply"
}

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

/// A `Prepare` to node `to` is on the wire.
fn prepare_to(to: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world.wire.iter().any(|message| {
            message.kind == "Prepare" && message.to == to && message.to_party == PartyView::Node
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

/// Acceptor `id` of the single-decree world voted for `value`.
fn voted(id: u64, value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .nodes
            .iter()
            .any(|node| node.id == id && node.accepted.iter().any(|record| record.value == value))
    }
}

/// End a run: a screen that goes back to the list of runs.
fn back(run: &mut Recorder, says: &str) {
    run.beat(says).choice("Choose another case", "base", TURN);
}

/// Open a run: node 0 leads at ballot 2.0 and alpha is chosen at slot 0, as
/// in the first story.
fn lead(run: &mut Recorder) {
    run.beat("Node 0 leads at ballot 2.0, and alpha is chosen at slot 0, as in the first story.")
        .play(elect(0), "node 0 starts an election")
        .settle(any)
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(any);
}

pub(super) fn matchmaker() -> Trace {
    trace(
        "matchmaker",
        LEVEL,
        vec![base(), leader_crash(), replace(), retire(), skipped()],
    )
}

/// The base run: the matchmakers before Phase 1, then a change of the
/// acceptors as a new ballot.
fn base() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat(
        "Two matchmakers keep which acceptors each ballot uses. Nodes 0, 1 and 2 are the \
         acceptors. Node 3 waits outside.",
    )
    .beat(
        "Node 0 first asks the matchmakers which acceptors are in force. Then it registers \
         ballot 2.0 with acceptors 0, 1 and 2 at both of them.",
    )
    .ask(
        "Node 0 wants to lead. What must it do before it sends a Prepare?",
        &[
            "Nothing: it sends the Prepare at once",
            "Register its ballot and its acceptors at a majority of the matchmakers",
            "Ask every acceptor which acceptors are in force",
        ],
        1,
        "The matchmakers keep the acceptors of each ballot. A new leader learns the older \
         configurations from them, so its Phase 1 asks the right acceptors.",
    )
    .play(elect(0), "node 0 starts an election")
    .settle(matchmaking)
    .holds(
        prepare_to(1),
        "the Prepare leaves only after the matchmakers answered",
    )
    .beat(
        "Phase 1 runs on acceptors 0, 1 and 2. Once it wins, node 0 tells the matchmakers to \
         forget the configurations below ballot 2.0.",
    )
    .settle(any)
    .beat("alpha is chosen at slot 0 by two votes of three.")
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle(any)
    .beat(
        "The operator adds node 3. A configuration never changes in place: node 0 registers a \
         new ballot, 3.0, with acceptors 0, 1, 2 and 3, and runs Phase 1 again.",
    )
    .ask(
        "The operator adds node 3 to the acceptors. What does node 0 do?",
        &[
            "It sends node 3 the log and goes on at ballot 2.0",
            "It registers a new ballot with the four acceptors, then runs Phase 1 again",
            "It waits until a matchmaker tells node 3",
        ],
        1,
        "A change of the acceptors is a change of round. The configuration is bound to its \
         ballot, so the leader opens ballot 3.0 with the new set.",
    )
    .play(
        Action::Reconfigure {
            node: 0,
            members: vec![0, 1, 2, 3],
            quorum: None,
        },
        "the operator adds node 3 to the acceptors",
    )
    .settle(matchmaking)
    .holds(prepare_to(3), "the Prepare of ballot 3.0 goes to node 3")
    .settle(any)
    .beat("bravo goes to slot 1. With four acceptors, it needs three votes.")
    .play(propose(0, "bravo"), "the client writes bravo")
    .settle(any)
    .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1")
    .beat("Now choose a case.")
    .choice("The leader crashes", "leader-crash", 0)
    .choice("Replace an acceptor, then lose the leader", "replace", 0)
    .choice("Remove an acceptor and retire it", "retire", 0)
    .choice("Break the rule: skip the matchmakers", "break-the-rule", 0);
    run.finish("base", "No fault", log_outcome)
}

/// The leader crashes; node 1 asks the matchmakers, then takes over.
fn leader_crash() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat(
        "Node 0 crashes. Node 1 registers ballot 3.1 at the matchmakers, and they report the \
         acceptors of ballot 2.0: nodes 0, 1 and 2.",
    )
    .ask(
        "Node 0 crashed. Which acceptors must node 1 ask in Phase 1?",
        &[
            "Acceptors 0, 1 and 2, the set the matchmakers report",
            "All four nodes",
            "Only the matchmakers",
        ],
        0,
        "The matchmakers report the acceptors of every ballot they still remember. Here that \
         is one set, and a majority of it is enough.",
    )
    .play(crash(0), "node 0 crashes")
    .play(elect(1), "node 1 starts an election")
    .settle_dropping(touches(0))
    .holds(leads(1), "node 1 leads with promises from nodes 1 and 2");
    run.beat("bravo is chosen at slot 1 by nodes 1 and 2.")
        .play(propose(1, "bravo"), "the client writes bravo")
        .settle_dropping(touches(0))
        .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1");
    back(
        &mut run,
        "A crash of the leader costs one round of matchmaking and one Phase 1, as in \
         Multi-Paxos.",
    );
    run.finish(
        "leader-crash",
        "What if the leader crashes and node 1 takes over?",
        log_outcome,
    )
}

/// Replace an acceptor, then lose the leader: the next leader's Phase 1
/// covers the old set and the new one.
fn replace() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat(
        "The operator replaces node 2 with node 3. Node 0 registers ballot 3.0 with acceptors \
         0, 1 and 3, and runs Phase 1.",
    )
    .play(
        Action::Reconfigure {
            node: 0,
            members: vec![0, 1, 3],
            quorum: None,
        },
        "the operator replaces node 2 with node 3",
    )
    .settle(any);
    run.beat(
        "Node 0 crashes. Node 3 campaigns at ballot 4.3. The matchmakers report two sets: 0, \
         1, 2 for ballot 2.0 and 0, 1, 3 for ballot 3.0.",
    )
    .ask(
        "The matchmakers report two sets of acceptors. Whose promises does node 3 need?",
        &[
            "A majority of the new set only",
            "A majority of each set",
            "A majority of all four nodes",
        ],
        1,
        "A value may be chosen in either set, so Phase 1 needs a quorum of every set the \
         matchmakers report. Nodes 1 and 2 cover the old set, and nodes 1 and 3 the new one.",
    )
    .play(crash(0), "node 0 crashes")
    .play(elect(3), "node 3 starts an election")
    .settle_dropping(touches(0))
    .holds(leads(3), "node 3 leads with promises from nodes 1 and 2");
    run.beat("bravo is chosen at slot 1 by nodes 1 and 3.")
        .play(propose(3, "bravo"), "the client writes bravo")
        .settle_dropping(touches(0))
        .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1");
    back(
        &mut run,
        "Node 2 is out of the new set, but its promise still counts for the old one. Removed \
         is not shut down.",
    );
    run.finish(
        "replace",
        "Replace an acceptor, then lose the leader",
        log_outcome,
    )
}

/// Remove an acceptor, wait until a matchmaker quorum forgets the old set,
/// then retire the node.
fn retire() -> Scenario {
    let mut run = Recorder::new("act4/garbage-collection");
    lead(&mut run);
    run.beat("The operator removes node 3. Node 0 registers ballot 3.0 with acceptors 0, 1 and 2.")
        .play(
            Action::Reconfigure {
                node: 0,
                members: vec![0, 1, 2],
                quorum: None,
            },
            "the operator removes node 3",
        )
        .settle(any);
    run.beat(
        "The leader beats. Once ballot 3.0 holds, it tells the matchmakers to forget the \
         configurations below 3.0, and they write that down.",
    );
    let mut watermark = None;
    for _ in 0..12 {
        watermark = run
            .world()
            .log()
            .and_then(|log| log.gc_effective(NodeId(0)))
            .map(|(ballot, _)| ballot);
        if watermark.is_some() {
            break;
        }
        run.play(Action::Tick { node: 0 }, "the leader's clock ticks")
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
    // Node 3 first asks the matchmakers which registrations name it (#1002
    // (retire fence gaps)), then the operator retires it again.
    run.beat(
        "Node 3 asks the matchmakers whether any registration still names it. None does, so \
         the operator retires it.",
    )
    .ask(
        "When can node 3 shut down for good?",
        &[
            "As soon as the operator removes it",
            "Once a matchmaker quorum forgot every configuration that names it",
            "Never: an acceptor stays forever",
        ],
        1,
        "While the matchmakers remember a set that names node 3, a future leader can need its \
         promise. Once they forget it, no leader can ask.",
    )
    .play(retire_node_3(), "the operator asks node 3 to retire")
    .settle(any)
    .play(retire_node_3(), "the operator retires node 3")
    .settle(any)
    .holds(
        |world| world.nodes.iter().any(|node| node.id == 3 && !node.alive),
        "node 3 is retired",
    );
    back(
        &mut run,
        "Forget first, then retire: an acceptor leaves only when no leader can need it again.",
    );
    run.finish(
        "retire",
        "Remove an acceptor, forget the old set, retire it",
        log_outcome,
    )
}

/// Break the rule: proposer 8 gets a new set of acceptors with no
/// matchmaker, so its Phase 1 asks only the new set.
fn skipped() -> Scenario {
    let mut run = Recorder::new("act1/the-duel");
    run.beat(
        "Proposer 5 gets alpha chosen by acceptors 1 and 2. Acceptor 3 never votes, and the \
         Commits are lost.",
    )
    .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
    .settle(|message| !(message.kind == "Accept" && message.to == 3) && message.kind != "Commit")
    .drop_all(|message| message.kind == "Accept" || message.kind == "Commit");
    run.beat(
        "Proposer 8 moves to acceptor 3 alone, and asks no matchmaker. paros refuses this \
         change. Here it is allowed, to show why.",
    )
    .play(
        Action::Reconfigure {
            node: 8,
            members: vec![3],
            quorum: None,
        },
        "proposer 8 moves to acceptor 3 alone, with no matchmaker",
    );
    run.beat("Proposer 8 opens ballot 1.8 for bravo on acceptor 3 alone.")
        .ask(
            "alpha is chosen by acceptors 1 and 2. Proposer 8 asks only acceptor 3. Can it \
             choose bravo?",
            &[
                "No: its Phase 1 finds the vote for alpha",
                "Yes: acceptor 3 never voted, so Phase 1 finds nothing",
                "No: acceptor 3 refuses a new set of acceptors",
            ],
            1,
            "With no matchmaker, ballot 1.8 does not know the acceptors of ballot 1.5. Its \
             Phase 1 meets no vote for alpha, so two values are chosen.",
        )
        .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo")
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"))
        .holds(voted(3, "bravo"), "acceptor 3 votes for bravo")
        .holds(voted(1, "alpha"), "acceptor 1 keeps its vote for alpha");
    back(
        &mut run,
        "The matchmakers are how a new ballot learns the old acceptors. Skip them, and two \
         majorities need not meet.",
    );
    run.finish(
        "break-the-rule",
        "Break the rule: skip the matchmakers",
        decree_outcome,
    )
}
