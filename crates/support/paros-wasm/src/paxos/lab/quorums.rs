//! The exercises of the quorums page: lesson 2, two of three, and lesson 5, a
//! grid column. Both are guided: an intro, the goal as a checklist with
//! hints and dead ends, and a guide that "Show me how" plays.

use super::DECREE;
use super::Extra;
use super::GRID;
use super::Setup;
use super::{Exercise, Lab, Move, Part, Step, WireMessage};
use crate::paxos::action::Action;
use crate::paxos::level::common::crash;
use crate::paxos::level::common::open;
use crate::paxos::level::common::propose;
use crate::paxos::level::common::start_election as elect;
use crate::paxos::view::AttemptView;

// ---- two of three -----------------------------------------------------------

/// A `kind` message on its way to acceptor 1 or acceptor 2.
fn to_one_or_two(message: &WireMessage, kind: &str) -> bool {
    message.kind == kind && (message.to == "n1" || message.to == "n2")
}

/// A `kind` message from acceptor 1 or acceptor 2 on its way to proposer 5.
fn from_one_or_two(message: &WireMessage, kind: &str) -> bool {
    message.kind == kind && message.to == "n5" && (message.from == "n1" || message.from == "n2")
}

/// The first message on the wire that `hit` matches, to deliver.
fn deliver_first(lab: &Lab, hit: impl Fn(&WireMessage) -> bool) -> Option<Move> {
    lab.wire()
        .into_iter()
        .find(|m| hit(m))
        .map(|m| Move::Deliver { id: m.id })
}

/// Proposer 5 heard promises from a majority: it is in Phase 2, or done.
fn promised(lab: &Lab) -> bool {
    lab.view_of_world().nodes.iter().any(|node| {
        node.id == 5 && matches!(node.attempt, Some(AttemptView::Phase2 | AttemptView::Won))
    })
}

/// Acceptor 3 got no message.
fn three_hears_nothing(lab: &Lab) -> bool {
    lab.delivered_to(3) == 0
}

/// How many `kind` messages to each lane of `lanes` the reader dropped.
fn dropped(lab: &Lab, kind: &str, lanes: &[&str]) -> usize {
    lanes.iter().map(|lane| lab.dropped(kind, lane)).sum()
}

const MAJORITY_INTRO: &[Step] = &[Step {
    says: "Proposer 5 opens a ballot for alpha. It sends a Prepare to acceptors 1, 2 and 3.",
    play: |lab| lab.act(&open(5, "alpha")),
}];

/// The proposer sends each message once, so a lost one is lost for good.
/// While acceptor 3 hears nothing, every `Promise` and every `Accepted` to
/// proposer 5 comes from acceptor 1 or acceptor 2, and the proposer needs
/// both of them. When acceptor 3 heard a message, the last part names that
/// dead end.
const MAJORITY_PARTS: &[Part] = &[
    Part {
        label: "Proposer 5 has promises from acceptors 1 and 2",
        holds: promised,
        idea: "Two of three is a majority. Phase 1 needs promises from two acceptors, not three.",
        plan: "Deliver the Prepares to acceptors 1 and 2, then their two promises.",
        next: |lab| {
            deliver_first(lab, |m| to_one_or_two(m, "Prepare"))
                .or_else(|| deliver_first(lab, |m| from_one_or_two(m, "Promise")))
        },
        dead: |lab| {
            let lost = dropped(lab, "Prepare", &["n1", "n2"]) + dropped(lab, "Promise", &["n5"]);
            (three_hears_nothing(lab) && lost > 0).then_some(
                "A Prepare to acceptor 1 or 2, or its promise, is lost, and proposer 5 sends \
                 no other. Without acceptor 3, no majority can promise. Undo that move.",
            )
        },
    },
    Part {
        label: "Acceptors 1 and 2 vote, and alpha is chosen",
        holds: |lab| lab.view_of_world().chosen.is_some(),
        idea: "Phase 2 needs a majority too: two votes of three choose a value.",
        plan: "Deliver the Accepts to acceptors 1 and 2, then their two votes.",
        next: |lab| {
            deliver_first(lab, |m| to_one_or_two(m, "Accept"))
                .or_else(|| deliver_first(lab, |m| from_one_or_two(m, "Accepted")))
        },
        dead: |lab| {
            let lost = dropped(lab, "Accept", &["n1", "n2"]) + dropped(lab, "Accepted", &["n5"]);
            (three_hears_nothing(lab) && lost > 0).then_some(
                "An Accept to acceptor 1 or 2, or its vote, is lost, and proposer 5 sends no \
                 other. Without acceptor 3, no majority can vote. Undo that move.",
            )
        },
    },
    Part {
        label: "Acceptor 3 hears nothing",
        holds: three_hears_nothing,
        idea: "Leave each message to acceptor 3 on the wire, or drop it.",
        plan: "Never deliver a message to acceptor 3.",
        next: |_| None,
        dead: |lab| {
            (!three_hears_nothing(lab))
                .then_some("A message reached acceptor 3. Undo the move that delivered it.")
        },
    },
];

const MAJORITY_GUIDE: &[Step] = &[
    Step {
        says: "Deliver the Prepares to acceptors 1 and 2. Each one writes its promise and \
               answers.",
        play: |lab| {
            lab.deliver(|m| m.kind == "Prepare" && m.to == "n1");
            lab.deliver(|m| m.kind == "Prepare" && m.to == "n2");
        },
    },
    Step {
        says: "Deliver the two promises. Two of three is a majority, so proposer 5 sends its \
               Accepts.",
        play: |lab| lab.settle(|m| from_one_or_two(m, "Promise")),
    },
    Step {
        says: "Deliver the Accepts to acceptors 1 and 2, then their votes. Two votes of three \
               choose alpha.",
        play: |lab| {
            lab.settle(|m| to_one_or_two(m, "Accept"));
            lab.settle(|m| from_one_or_two(m, "Accepted"));
        },
    },
    Step {
        says: "Drop each message to acceptor 3. It never heard of alpha, and alpha is chosen.",
        play: |lab| lab.drop_all(|m| m.to == "n3"),
    },
];

pub(super) const MAJORITY: Exercise = Exercise {
    id: "majority",
    title: "Two of three",
    task: "Proposer 5 asks for alpha. Get alpha chosen while acceptor 3 hears nothing.",
    rule_off: None,
    done: "Two of three is a majority, so acceptor 3 is not needed. Any two majorities of \
           three share an acceptor: that shared acceptor is how a later ballot finds alpha.",
    setup_on: Setup::Level(DECREE),
    setup: |_| {},
    intro: MAJORITY_INTRO,
    parts: MAJORITY_PARTS,
    guide: MAJORITY_GUIDE,
    extras: Vec::new,
    goal: |lab| MAJORITY_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in MAJORITY_GUIDE {
            (step.play)(lab);
        }
    },
};

// ---- a column down ----------------------------------------------------------

/// The extra moves of `grid-column`, by their index in its list.
const ALPHA: usize = 0;
const BRAVO: usize = 1;
const RESEND: usize = 2;

/// The client's writes, in order, by the index of their extra move. Node 0
/// leads for the whole exercise (no other node can campaign), so the n-th
/// write takes slot n.
fn writes(lab: &Lab) -> Vec<usize> {
    lab.moves()
        .iter()
        .filter_map(|m| match m {
            Move::Extra { index } if *index == ALPHA || *index == BRAVO => Some(*index),
            _ => None,
        })
        .collect()
}

/// A live node holds bravo chosen at slot 1.
fn bravo_chosen_at_1(lab: &Lab) -> bool {
    lab.view_of_world().nodes.iter().any(|node| {
        node.alive
            && node
                .accepted
                .iter()
                .any(|record| record.slot == 1 && record.chosen && record.value == "bravo")
    })
}

const GRID_INTRO: &[Step] = &[
    Step {
        says: "Node 0 starts an election. The promises cover every column, so node 0 leads.",
        play: |lab| {
            lab.act(&elect(0));
            lab.settle(|_| true);
        },
    },
    Step {
        says: "Node 3 crashes. It is one of the two nodes of column 0.",
        play: |lab| lab.act(&crash(3)),
    },
];

/// Slot 0 belongs to column 0 and slot 1 to column 1. The writes take the
/// slots in order, so a write in the wrong order cannot be taken back. A lost
/// message of slot 1 is no dead end: the leader can send its open Accepts
/// again.
const GRID_PARTS: &[Part] = &[
    Part {
        label: "alpha is written at slot 0, in column 0",
        holds: |lab| writes(lab).first() == Some(&ALPHA),
        idea: "Each slot belongs to one column. Slot 0 belongs to column 0: nodes 0 and 3.",
        plan: "Write alpha first. It takes slot 0.",
        next: |lab| {
            writes(lab)
                .is_empty()
                .then_some(Move::Extra { index: ALPHA })
        },
        dead: |lab| {
            (writes(lab).first() == Some(&BRAVO)).then_some(
                "bravo took slot 0, and a slot keeps its value. Undo that write, and write \
                 alpha first.",
            )
        },
    },
    Part {
        label: "bravo is chosen at slot 1, in column 1",
        holds: bravo_chosen_at_1,
        idea: "Node 3 stops only the slots of column 0. Slot 1 belongs to column 1, nodes 1 \
               and 4, and both are up.",
        plan: "Write bravo. Deliver its Accepts to nodes 1 and 4, then their votes.",
        next: |lab| {
            if writes(lab).len() < 2 {
                return Some(Move::Extra { index: BRAVO });
            }
            deliver_first(lab, |m| m.slot == Some(1) && m.to != "n3")
                .or(Some(Move::Extra { index: RESEND }))
        },
        dead: |lab| {
            (writes(lab).get(1) == Some(&ALPHA)).then_some(
                "alpha took slot 1 too, and a slot keeps its value. Undo that write, and \
                 write bravo second.",
            )
        },
    },
];

const GRID_GUIDE: &[Step] = &[
    Step {
        says: "Write alpha. Slot 0 belongs to column 0, and node 3 is down, so alpha waits.",
        play: |lab| lab.extra("The client writes alpha"),
    },
    Step {
        says: "Write bravo. Slot 1 belongs to column 1: nodes 1 and 4, and both are up.",
        play: |lab| lab.extra("The client writes bravo"),
    },
    Step {
        says: "Deliver the Accepts of slot 1 and the votes. The full column voted, so bravo is \
               chosen while slot 0 waits.",
        play: |lab| lab.settle(|m| m.slot == Some(1) && m.to != "n3"),
    },
];

pub(super) const GRID_COLUMN: Exercise = Exercise {
    id: "grid-column",
    title: "A column down",
    task: "Node 0 leads a grid of 2 rows and 3 columns, and node 3 is down. Write alpha at \
           slot 0, then get bravo chosen at slot 1.",
    rule_off: None,
    done: "Each slot goes to one column, and a Phase 2 quorum is a full column. A node \
           down stops the slots of its column only: a write to another column is chosen.",
    setup_on: Setup::Level(GRID),
    setup: |_| {},
    intro: GRID_INTRO,
    parts: GRID_PARTS,
    guide: GRID_GUIDE,
    extras: || {
        let extras = vec![
            ("The client writes alpha", Extra::Act(propose(0, "alpha"))),
            ("The client writes bravo", Extra::Act(propose(0, "bravo"))),
            (
                "Node 0 sends its open Accepts again",
                Extra::Act(Action::ResendPending { node: 0 }),
            ),
        ];
        // The indexes that the parts name.
        assert_eq!(extras[ALPHA].0, "The client writes alpha");
        assert_eq!(extras[BRAVO].0, "The client writes bravo");
        assert_eq!(extras[RESEND].0, "Node 0 sends its open Accepts again");
        extras
    },
    goal: |lab| GRID_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in GRID_GUIDE {
            (step.play)(lab);
        }
    },
};
