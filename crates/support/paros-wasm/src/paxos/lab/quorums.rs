//! The exercises of the quorums page: lesson 2, two of three, and lesson 5, a grid column.

use super::DECREE;
use super::Exercise;
use super::Extra;
use super::GRID;
use super::Setup;
use crate::paxos::level::common::crash;
use crate::paxos::level::common::open;
use crate::paxos::level::common::propose;
use crate::paxos::level::common::start_election as elect;

pub(super) const MAJORITY: Exercise = Exercise {
    id: "majority",
    title: "Two of three",
    task: "Proposer 5 asks for alpha. Get alpha chosen while acceptor 3 hears nothing: \
           drop every message to it.",
    rule_off: None,
    done: "Two of three is a majority, so acceptor 3 is not needed. Any two majorities of \
           three share an acceptor: that shared acceptor is how a later ballot finds alpha.",
    setup_on: Setup::Level(DECREE),
    setup: |lab| lab.act(&open(5, "alpha")),
    extras: Vec::new,
    goal: |lab| {
        let world = lab.view_of_world();
        world.chosen.is_some() && lab.delivered_to(3) == 0
    },
    solution: |lab| {
        lab.settle(|m| m.to != "n3");
        lab.drop_all(|m| m.to == "n3");
    },
};

pub(super) const GRID_COLUMN: Exercise = Exercise {
    id: "grid-column",
    title: "A column down",
    task: "Node 0 leads a grid of 2 rows and 3 columns, and node 3 is down. Get a value \
           chosen anyway.",
    rule_off: None,
    done: "Each slot goes to one column, and a Phase 2 quorum is a full column. A node \
           down stops the slots of its column only: a write to another column is chosen.",
    setup_on: Setup::Level(GRID),
    setup: |lab| {
        lab.act(&elect(0));
        lab.settle(|_| true);
        lab.act(&crash(3));
    },
    extras: || {
        vec![
            ("The client writes alpha", Extra::Act(propose(0, "alpha"))),
            ("The client writes bravo", Extra::Act(propose(0, "bravo"))),
        ]
    },
    goal: |lab| {
        let world = lab.view_of_world();
        world
            .nodes
            .iter()
            .any(|node| node.alive && node.accepted.iter().any(|record| record.chosen))
    },
    solution: |lab| {
        lab.extra("The client writes alpha");
        lab.settle(|_| true);
        lab.extra("The client writes bravo");
        lab.settle(|_| true);
    },
};
