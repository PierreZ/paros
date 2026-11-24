//! The exercise of lesson 7, Matchmaker Paxos: the old acceptor set.

use super::Extra;
use super::MATCHMAKERS;
use super::Setup;
use super::{Exercise, NO_PARTS, NONE};
use crate::paxos::action::Action;
use crate::paxos::level::common::crash;
use crate::paxos::level::common::propose;
use crate::paxos::level::common::start_election as elect;
use crate::paxos::view::RoleView;

pub(super) const OLD_SET: Exercise = Exercise {
    id: "old-set",
    title: "The old set counts",
    task: "The operator replaced node 2 with node 3, and node 0 is down. Make node 3 the \
           leader. Can you do it with no message to node 2?",
    rule_off: None,
    done: "The matchmakers report the old set and the new set, and Phase 1 needs a majority \
           of each. Node 2 left the new set, but its promise still counts for the old one.",
    setup_on: Setup::Level(MATCHMAKERS),
    setup: |lab| {
        lab.act(&elect(0));
        lab.settle(|_| true);
        lab.act(&propose(0, "alpha"));
        lab.settle(|_| true);
        lab.act(&Action::Reconfigure {
            node: 0,
            members: vec![0, 1, 3],
            quorum: None,
        });
        lab.settle(|_| true);
        lab.act(&crash(0));
    },
    intro: NONE,
    parts: NO_PARTS,
    guide: NONE,
    extras: || vec![("Node 3 starts an election", Extra::Act(elect(3)))],
    goal: |lab| {
        lab.view_of_world()
            .nodes
            .iter()
            .any(|node| node.id == 3 && node.role == Some(RoleView::Leader))
    },
    solution: |lab| {
        lab.extra("Node 3 starts an election");
        lab.settle(|_| true);
    },
};
