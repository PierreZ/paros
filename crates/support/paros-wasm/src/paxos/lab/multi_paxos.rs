//! The exercises of Multi-Paxos: lesson 3, fill the gap (the pilot of the guided exercise), and lesson 4, a read without the leader.

use super::Extra;
use super::LOG;
use super::Setup;
use super::chosen_at;
use super::chosen_at_any;
use super::{Exercise, Lab, Move, NO_PARTS, NONE, Part, Step, WireMessage};
use crate::paxos::action::Action;
use crate::paxos::level::common::crash;
use crate::paxos::level::common::propose;
use crate::paxos::level::common::start_election as elect;
use crate::paxos::view::RoleView;

/// The extra moves of `gap`, by their index in its list.
const BRAVO: usize = 0;
const CHARLIE: usize = 1;
const CRASH: usize = 2;
const ELECT: usize = 3;

/// The reader wrote `value` (extra move `index`).
fn wrote(lab: &Lab, index: usize) -> bool {
    lab.moves().contains(&Move::Extra { index })
}

/// Node 1 or node 2 voted for a client value (not a `Noop`) at slot 1. The
/// vote of node 0, the leader that wrote it, does not count: node 0 crashes.
fn client_vote_at_1(lab: &Lab) -> bool {
    lab.view_of_world()
        .nodes
        .iter()
        .filter(|node| node.id != 0)
        .any(|node| {
            node.accepted
                .iter()
                .any(|vote| vote.slot == 1 && vote.value != "Noop")
        })
}

/// An `Accept` of node 0 for slot 1 is on the wire.
fn accept_at_1(message: &WireMessage) -> bool {
    message.kind == "Accept" && message.from == "n0" && message.slot == Some(1)
}

/// Node `id` is up and leads.
fn leads(lab: &Lab, id: u64) -> bool {
    lab.view_of_world()
        .nodes
        .iter()
        .any(|node| node.id == id && node.alive && node.role == Some(RoleView::Leader))
}

/// Node `id` is up.
fn up(lab: &Lab, id: u64) -> bool {
    lab.view_of_world()
        .nodes
        .iter()
        .any(|node| node.id == id && node.alive)
}

/// The first message on the wire that `hit` matches, to deliver.
fn deliver_first(lab: &Lab, hit: impl Fn(&WireMessage) -> bool) -> Option<Move> {
    lab.wire()
        .into_iter()
        .find(|m| hit(m))
        .map(|m| Move::Deliver { id: m.id })
}

const GAP_INTRO: &[Step] = &[
    Step {
        says: "Node 0 starts an election. Nodes 1 and 2 promise, so node 0 leads.",
        play: |lab| {
            lab.act(&elect(0));
            lab.settle(|_| true);
        },
    },
    Step {
        says: "A client writes alpha. Node 0 puts it at slot 0, the nodes vote, and alpha is \
               chosen.",
        play: |lab| {
            lab.act(&propose(0, "alpha"));
            lab.settle(|_| true);
        },
    },
];

const GAP_PARTS: &[Part] = &[
    Part {
        label: "bravo is at slot 1, with no vote from node 1 or 2",
        holds: |lab| {
            wrote(lab, BRAVO) && !client_vote_at_1(lab) && !lab.wire().iter().any(accept_at_1)
        },
        idea: "A value is not chosen when its Accepts never arrive: only the leader votes for it.",
        plan: "Write bravo. Node 0 puts it at slot 1. Then drop each Accept of slot 1.",
        next: |lab| {
            if !wrote(lab, BRAVO) {
                return Some(Move::Extra { index: BRAVO });
            }
            lab.wire()
                .into_iter()
                .find(accept_at_1)
                .map(|m| Move::Drop { id: m.id })
        },
        dead: client_vote_at_1,
        dead_says: "Node 1 or node 2 voted for bravo at slot 1, so a new leader will find bravo. \
                    Undo the move that delivered its Accept.",
    },
    Part {
        label: "A value is chosen at slot 2",
        holds: |lab| chosen_at_any(&lab.view_of_world(), 2),
        idea: "A slot after the hole can still be chosen: it needs votes from a majority.",
        plan: "Write charlie. Node 0 puts it at slot 2. Deliver its Accepts and the votes, but \
               no Accept of slot 1.",
        next: |lab| {
            if !wrote(lab, CHARLIE) {
                return Some(Move::Extra { index: CHARLIE });
            }
            deliver_first(lab, |m| !accept_at_1(m))
        },
        dead: |_| false,
        dead_says: "",
    },
    Part {
        label: "Node 1 leads",
        holds: |lab| leads(lab, 1),
        idea: "A new leader runs Phase 1 at a higher ballot, and a majority must promise.",
        plan: "Crash node 0. Then node 1 starts an election: deliver its messages.",
        next: |lab| {
            if up(lab, 0) {
                return Some(Move::Extra { index: CRASH });
            }
            if !wrote(lab, ELECT) {
                return Some(Move::Extra { index: ELECT });
            }
            deliver_first(lab, |_| true)
        },
        dead: |_| false,
        dead_says: "",
    },
    Part {
        label: "Slot 1 is filled",
        holds: |lab| chosen_at(&lab.view_of_world(), 1, "Noop"),
        idea: "The promises report each vote. A new leader proposes each reported value again, \
               and puts a Noop in each slot below its frontier that no promise reports.",
        plan: "Deliver the messages of node 1 until slot 1 is chosen.",
        next: |lab| deliver_first(lab, |_| true),
        dead: |_| false,
        dead_says: "",
    },
];

const GAP_GUIDE: &[Step] = &[
    Step {
        says: "Write bravo. Node 0 puts it at slot 1 and sends an Accept to each node.",
        play: |lab| lab.extra("The client writes bravo"),
    },
    Step {
        says: "Drop each Accept of slot 1. Only node 0 votes for bravo, so bravo is not chosen.",
        play: |lab| lab.drop_all(accept_at_1),
    },
    Step {
        says: "Write charlie. Node 0 puts it at slot 2. Two votes are a majority, so charlie \
               is chosen at slot 2.",
        play: |lab| {
            lab.extra("The client writes charlie");
            lab.settle(|m| m.to != "n0" || m.kind == "Accepted");
        },
    },
    Step {
        says: "Node 0 crashes, with the only vote for bravo. Slot 1 is not chosen, so no node \
               can apply slot 2.",
        play: |lab| lab.extra("Node 0 crashes"),
    },
    Step {
        says: "Node 1 starts an election. The promises report charlie at slot 2 and no vote at \
               slot 1, so node 1 puts a Noop at slot 1.",
        play: |lab| {
            lab.extra("Node 1 starts an election");
            lab.settle(|_| true);
        },
    },
];

pub(super) const GAP: Exercise = Exercise {
    id: "gap",
    title: "Fill the gap",
    task: "Node 0 leads, and alpha is chosen at slot 0. Put bravo at slot 1 with no vote from \
           node 1 or 2, and get a value chosen at slot 2. Then make node 1 lead. What does it \
           put in slot 1?",
    rule_off: None,
    done: "A new leader cannot leave a hole: slot 2 would never be applied. It fills every \
           slot that no promise reports with a Noop, so the log goes on.",
    setup_on: Setup::Level(LOG),
    setup: |_| {},
    intro: GAP_INTRO,
    parts: GAP_PARTS,
    guide: GAP_GUIDE,
    extras: || {
        let extras = vec![
            ("The client writes bravo", Extra::Act(propose(0, "bravo"))),
            (
                "The client writes charlie",
                Extra::Act(propose(0, "charlie")),
            ),
            ("Node 0 crashes", Extra::Act(crash(0))),
            ("Node 1 starts an election", Extra::Act(elect(1))),
        ];
        // The indexes that the parts name.
        assert_eq!(extras[BRAVO].0, "The client writes bravo");
        assert_eq!(extras[CHARLIE].0, "The client writes charlie");
        assert_eq!(extras[CRASH].0, "Node 0 crashes");
        assert_eq!(extras[ELECT].0, "Node 1 starts an election");
        extras
    },
    goal: |lab| GAP_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in GAP_GUIDE {
            (step.play)(lab);
        }
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
    intro: NONE,
    parts: NO_PARTS,
    guide: NONE,
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
