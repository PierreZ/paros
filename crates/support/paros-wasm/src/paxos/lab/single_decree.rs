//! The exercises of lesson 1, single-decree Paxos: a vote is not a choice, two values with the disk rule off, and the duel.

use super::DECREE;
use super::Extra;
use super::Setup;
use super::voters;
use super::{Exercise, NO_PARTS, NONE};
use crate::paxos::action::Action;
use crate::paxos::level::WorldKind;
use crate::paxos::level::common::open;

pub(super) const VOTE_NOT_CHOSEN: Exercise = Exercise {
    id: "vote-not-chosen",
    title: "A vote, not a choice",
    task: "Proposer 5 asks for alpha. Leave a vote for alpha on exactly one acceptor, with \
           no value chosen and no message on the wire.",
    rule_off: None,
    done: "One vote of three is not a choice. A later proposer that hears of this vote must \
           propose alpha, but it is free to choose another value when it does not hear of it.",
    setup_on: Setup::Level(DECREE),
    setup: |lab| lab.act(&open(5, "alpha")),
    intro: NONE,
    parts: NO_PARTS,
    guide: NONE,
    extras: Vec::new,
    goal: |lab| {
        let world = lab.view_of_world();
        voters(&world, "alpha") == 1 && world.chosen.is_none() && world.wire.is_empty()
    },
    solution: |lab| {
        lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise");
        lab.deliver(|m| m.kind == "Accept" && m.to == "n1");
        lab.drop_all(|m| m.kind == "Accept");
        lab.settle(|_| true);
    },
};

pub(super) const TWO_VALUES: Exercise = Exercise {
    id: "two-values",
    title: "Two values",
    task: "In this build, an acceptor can lose its disk and answer again. Get two \
           different values chosen.",
    rule_off: Some(
        "An acceptor boots on an empty disk. paros refuses this boot: it is the rule that \
         this exercise turns off.",
    ),
    done: "A majority that forgets its vote is no longer a majority. This is why an \
           acceptor writes to disk before it answers, and why paros refuses to boot a \
           member whose disk is empty.",
    setup_on: Setup::Level(DECREE),
    setup: |lab| lab.act(&open(5, "alpha")),
    intro: NONE,
    parts: NO_PARTS,
    guide: NONE,
    extras: || {
        vec![
            (
                "Proposer 8 opens a ballot for bravo",
                Extra::Act(open(8, "bravo")),
            ),
            (
                "Proposer 5 opens a higher ballot",
                Extra::Act(open(5, "alpha")),
            ),
            (
                "Acceptor 1 loses its disk",
                Extra::Act(Action::Wipe { node: 1 }),
            ),
            (
                "Acceptor 2 loses its disk",
                Extra::Act(Action::Wipe { node: 2 }),
            ),
            (
                "Acceptor 3 loses its disk",
                Extra::Act(Action::Wipe { node: 3 }),
            ),
        ]
    },
    goal: |lab| match lab.game().world() {
        WorldKind::Decree(world) => world.violation().is_some(),
        WorldKind::Log(_) => false,
    },
    solution: |lab| {
        lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise");
        lab.settle(|m| m.kind == "Accept" && m.to != "n3");
        lab.drop_all(|m| m.kind == "Accept");
        lab.settle(|m| m.kind == "Accepted");
        lab.drop_all(|_| true);
        lab.extra("Acceptor 2 loses its disk");
        lab.extra("Proposer 8 opens a ballot for bravo");
        lab.settle(|m| m.to != "n1" && (m.kind == "Prepare" || m.kind == "Promise"));
        lab.drop_all(|m| m.kind == "Prepare");
        lab.settle(|m| m.to != "n1" && (m.kind == "Accept" || m.kind == "Accepted"));
    },
};

pub(super) const DUEL: Exercise = Exercise {
    id: "duel",
    title: "The duel",
    task: "Two proposers want two values. Make each proposer get two refusals (Nack), \
           with no value chosen.",
    rule_off: None,
    done: "Paxos stays safe with two proposers, but they can block each other with no \
           end. paros keeps one stable leader, and a proposer that loses waits a random \
           time before it tries again.",
    setup_on: Setup::Level(DECREE),
    setup: |_| {},
    intro: NONE,
    parts: NO_PARTS,
    guide: NONE,
    extras: || {
        vec![
            (
                "Proposer 5 opens a ballot for alpha",
                Extra::Act(open(5, "alpha")),
            ),
            (
                "Proposer 8 opens a ballot for bravo",
                Extra::Act(open(8, "bravo")),
            ),
        ]
    },
    goal: |lab| {
        let world = lab.view_of_world();
        world.chosen.is_none() && lab.refusals(5) >= 2 && lab.refusals(8) >= 2
    },
    solution: |lab| {
        lab.extra("Proposer 5 opens a ballot for alpha");
        lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise");
        lab.extra("Proposer 8 opens a ballot for bravo");
        lab.settle(|m| {
            (m.from == "n8" || m.to == "n8") && (m.kind == "Prepare" || m.kind == "Promise")
        });
        lab.settle(|m| m.from == "n5" || m.to == "n5");
        lab.extra("Proposer 5 opens a ballot for alpha");
        lab.settle(|m| {
            (m.from == "n5" || m.to == "n5") && (m.kind == "Prepare" || m.kind == "Promise")
        });
        lab.settle(|m| m.from == "n8" || m.to == "n8");
        lab.extra("Proposer 8 opens a ballot for bravo");
        lab.settle(|m| {
            (m.from == "n8" || m.to == "n8") && (m.kind == "Prepare" || m.kind == "Promise")
        });
        lab.settle(|m| m.from == "n5" || m.to == "n5");
        lab.extra("Proposer 5 opens a ballot for alpha");
        lab.settle(|m| {
            (m.from == "n5" || m.to == "n5") && (m.kind == "Prepare" || m.kind == "Promise")
        });
        lab.settle(|m| m.from == "n8" || m.to == "n8");
    },
};
