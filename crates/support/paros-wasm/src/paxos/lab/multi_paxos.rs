//! The exercises of Multi-Paxos: lesson 3, fill the gap (the pilot of the guided exercise), and lesson 4, a read without the leader.

use super::Exercise;
use super::Extra;
use super::LOG;
use super::Setup;
use super::chosen_at;
use super::chosen_at_any;
use crate::paxos::action::Action;
use crate::paxos::level::common::crash;
use crate::paxos::level::common::propose;
use crate::paxos::level::common::start_election as elect;

pub(super) const GAP: Exercise = Exercise {
    id: "gap",
    title: "Fill the gap",
    task: "Node 0 leads, and alpha is chosen at slot 0. Leave slot 1 with no vote and get a \
           value chosen at slot 2. Then make node 1 lead. What does it put in slot 1?",
    rule_off: None,
    done: "A new leader cannot leave a hole: slot 2 would never be applied. It fills every \
           slot that no promise reports with a Noop, so the log goes on.",
    setup_on: Setup::Level(LOG),
    setup: |lab| {
        lab.act(&elect(0));
        lab.settle(|_| true);
        lab.act(&propose(0, "alpha"));
        lab.settle(|_| true);
    },
    extras: || {
        vec![
            ("The client writes bravo", Extra::Act(propose(0, "bravo"))),
            (
                "The client writes charlie",
                Extra::Act(propose(0, "charlie")),
            ),
            ("Node 0 crashes", Extra::Act(crash(0))),
            ("Node 1 starts an election", Extra::Act(elect(1))),
        ]
    },
    goal: |lab| {
        let world = lab.view_of_world();
        chosen_at(&world, 1, "Noop") && chosen_at_any(&world, 2)
    },
    solution: |lab| {
        lab.extra("The client writes bravo");
        lab.drop_all(|m| m.kind == "Accept");
        lab.extra("The client writes charlie");
        lab.settle(|m| m.to != "n0" || m.kind == "Accepted");
        lab.extra("Node 0 crashes");
        lab.extra("Node 1 starts an election");
        lab.settle(|_| true);
    },
};

pub(super) const READ: Exercise = Exercise {
    id: "read",
    title: "Read without the leader",
    task: "alpha is chosen at slot 0. A client reads from node 2. Get the read served while \
           node 0, the leader, gets no message.",
    rule_off: None,
    done: "A leaderless read asks a majority of acceptors for the highest slot that each \
           voted in, and waits until its own log covers it. No leader confirms the read.",
    setup_on: Setup::Level(LOG),
    setup: |lab| {
        lab.act(&elect(0));
        lab.settle(|_| true);
        lab.act(&propose(0, "alpha"));
        lab.settle(|_| true);
    },
    extras: || {
        vec![(
            "A client reads from node 2",
            Extra::Act(Action::QuorumRead {
                node: 2,
                client: None,
            }),
        )]
    },
    goal: |lab| {
        let world = lab.view_of_world();
        lab.delivered_to(0) == 0
            && world.clients.iter().any(|client| {
                client
                    .reads
                    .iter()
                    .any(|read| read.node == 2 && read.served)
            })
    },
    solution: |lab| {
        lab.extra("A client reads from node 2");
        lab.settle(|m| m.to != "n0");
        lab.drop_all(|m| m.to == "n0");
    },
};
