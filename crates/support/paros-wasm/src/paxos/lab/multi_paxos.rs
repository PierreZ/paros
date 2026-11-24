//! The exercises of Multi-Paxos, both guided: lesson 3, fill the gap (the
//! pilot of the guided exercise), and lesson 4, a read without the leader
//! that must not meet an open slot.

use super::Extra;
use super::LOG;
use super::Setup;
use super::chosen_at;
use super::chosen_at_any;
use super::{Exercise, Lab, Move, Part, Step, WireMessage};
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

/// Node 0 leads, and node 1 or node 2 is up with no promise above node 0's
/// ballot: a write of node 0 can still get a majority of votes.
fn leads_live(lab: &Lab) -> bool {
    let world = lab.view_of_world();
    let Some(own) = world
        .nodes
        .iter()
        .find(|node| node.id == 0 && node.alive && node.role == Some(RoleView::Leader))
        .and_then(|node| node.ballot.as_deref())
    else {
        return false;
    };
    world.nodes.iter().any(|node| {
        node.id != 0
            && node.alive
            && node
                .promised
                .as_deref()
                .is_none_or(|p| ballot(p) <= ballot(own))
    })
}

/// A ballot `round.node` as a pair that orders as ballots do.
fn ballot(text: &str) -> (u64, u64) {
    let (round, node) = text.split_once('.').unwrap_or((text, "0"));
    (round.parse().unwrap_or(0), node.parse().unwrap_or(0))
}

/// An `Accept` of a slot that `hit` matches is on the wire to node 1 or
/// node 2, and its receiver is up and has promised no higher ballot:
/// delivered, it is a vote.
fn accept_live(lab: &Lab, hit: impl Fn(u64) -> bool) -> Option<u64> {
    let world = lab.view_of_world();
    world
        .wire
        .iter()
        .find(|m| {
            let Some(sent) = m.ballot.as_deref() else {
                return false;
            };
            m.kind == "Accept"
                && m.slot.is_some_and(&hit)
                && m.to != 0
                && world.nodes.iter().any(|node| {
                    node.id == m.to
                        && node.alive
                        && node
                            .promised
                            .as_deref()
                            .is_none_or(|p| ballot(p) <= ballot(sent))
                })
        })
        .map(|m| m.id)
}

/// Node 1 or node 2 holds a vote at slot 2 or above, or a live `Accept` of
/// such a slot is on its way to one of them: slot 2 can still be chosen with
/// no new write. A new leader proposes a reported vote at slot 2 again, and
/// fills slot 2 with a `Noop` when a vote above it sets its frontier.
fn slot_2_alive(lab: &Lab) -> bool {
    let voted = lab
        .view_of_world()
        .nodes
        .iter()
        .filter(|node| node.id != 0)
        .any(|node| node.accepted.iter().any(|vote| vote.slot >= 2));
    voted || accept_live(lab, |slot| slot >= 2).is_some()
}

/// Node 0 holds a vote for bravo at slot 1.
fn bravo_at_1(lab: &Lab) -> bool {
    lab.view_of_world().nodes.iter().any(|node| {
        node.id == 0
            && node
                .accepted
                .iter()
                .any(|vote| vote.slot == 1 && vote.value == "bravo")
    })
}

/// Node 0 leads with a live ballot, and its next free slot passes `hit`.
fn next_write(lab: &Lab, hit: impl Fn(u64) -> bool) -> bool {
    leads_live(lab)
        && lab
            .view_of_world()
            .nodes
            .iter()
            .any(|node| node.id == 0 && node.next_slot.is_some_and(&hit))
}

/// The next write lands at `slot`.
fn writes_at(lab: &Lab, slot: u64) -> bool {
    next_write(lab, |next| next == slot)
}

/// A write can still land at `slot`, now or after the writes below it.
fn can_write_at(lab: &Lab, slot: u64) -> bool {
    next_write(lab, |next| next <= slot)
}

/// The moves that make node 1 lead with no word from node 0: node 0
/// crashes, node 1 opens a ballot after the crash (an election from before
/// can carry node 0's promise, and with it bravo), then the messages of
/// nodes 1 and 2 are delivered; a ballot whose messages were lost is opened
/// again.
fn elect_on(lab: &Lab) -> Option<Move> {
    if up(lab, 0) {
        return Some(Move::Extra { index: CRASH });
    }
    let last = |index| {
        lab.moves()
            .iter()
            .rposition(|m| *m == Move::Extra { index })
    };
    let fresh = matches!((last(ELECT), last(CRASH)), (Some(elect), Some(crash)) if elect > crash);
    if !fresh {
        return Some(Move::Extra { index: ELECT });
    }
    deliver_first(lab, |m| m.to != "n0" && m.from != "n0").or(Some(Move::Extra { index: ELECT }))
}

const GAP_PARTS: &[Part] = &[
    Part {
        label: "bravo is at slot 1, with no vote from node 1 or 2",
        holds: |lab| {
            bravo_at_1(lab) && !client_vote_at_1(lab) && !lab.wire().iter().any(accept_at_1)
        },
        idea: "A value is not chosen when its Accepts never arrive: only the leader votes for it.",
        plan: "Write bravo. Node 0 puts it at slot 1. Then drop each Accept of slot 1.",
        next: |lab| {
            if !bravo_at_1(lab) {
                return writes_at(lab, 1).then_some(Move::Extra { index: BRAVO });
            }
            lab.wire()
                .into_iter()
                .find(accept_at_1)
                .map(|m| Move::Drop { id: m.id })
        },
        dead: |lab| {
            if client_vote_at_1(lab) {
                Some(
                    "Node 1 or node 2 voted for a value at slot 1, so a new leader will find \
                     it. Undo the move that delivered its Accept.",
                )
            } else if !bravo_at_1(lab) && !can_write_at(lab, 1) {
                Some(
                    "No write can put bravo at slot 1 now: slot 1 holds another value, or node 0 \
                     no longer leads. Undo to before that.",
                )
            } else {
                None
            }
        },
    },
    Part {
        label: "A value is chosen at slot 2",
        holds: |lab| chosen_at_any(&lab.view_of_world(), 2),
        idea: "A slot after the hole can still be chosen: it needs votes from a majority. A new \
               leader proposes again each vote that a promise reports.",
        plan: "Write charlie. Node 0 puts it at slot 2. Deliver its Accepts and the votes, but \
               no Accept of slot 1.",
        next: |lab| {
            if writes_at(lab, 2) {
                return Some(Move::Extra { index: CHARLIE });
            }
            // The votes of slot 2 first, before a new ballot outdates them.
            if let Some(id) = accept_live(lab, |slot| slot == 2) {
                return Some(Move::Deliver { id });
            }
            // Then the rest of slot 2: its votes reach node 0.
            if let Some(deliver) = deliver_first(lab, |m| m.slot == Some(2)) {
                return Some(deliver);
            }
            // A vote above slot 2 sets a new leader's frontier there.
            if let Some(id) = accept_live(lab, |slot| slot > 2) {
                return Some(Move::Deliver { id });
            }
            if slot_2_alive(lab) {
                // A new leader proposes the votes again, and fills the rest.
                elect_on(lab)
            } else if leads_live(lab) {
                // Slot 2 is lost: a write above it gives a new leader a
                // frontier there.
                Some(Move::Extra { index: CHARLIE })
            } else {
                None
            }
        },
        dead: |lab| {
            if !leads_live(lab) && !slot_2_alive(lab) {
                Some(
                    "No node but node 0 holds a vote at slot 2 or above, and node 0 can no \
                     longer write. Undo the moves that lost those votes.",
                )
            } else {
                None
            }
        },
    },
    Part {
        label: "Node 1 leads",
        holds: |lab| leads(lab, 1),
        idea: "A new leader runs Phase 1 at a higher ballot, and a majority must promise.",
        plan: "Crash node 0. Then node 1 starts an election: deliver its messages.",
        next: elect_on,
        dead: |_| None,
    },
    Part {
        label: "Slot 1 is filled",
        holds: |lab| chosen_at(&lab.view_of_world(), 1, "Noop"),
        idea: "The promises report each vote. A new leader proposes each reported value again, \
               and puts a Noop in each slot below its frontier that no promise reports.",
        plan: "Deliver the messages of node 1 until slot 1 is chosen.",
        next: elect_on,
        dead: |_| None,
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

/// The extra move of `read`.
const READ_AT_2: usize = 0;

/// The reads of node 2, as `served` flags.
fn reads_at_2(lab: &Lab) -> Vec<bool> {
    lab.view_of_world()
        .clients
        .iter()
        .flat_map(|client| client.reads.iter())
        .filter(|read| read.node == 2)
        .map(|read| read.served)
        .collect()
}

/// Some read of node 2 is served.
fn read_served(lab: &Lab) -> bool {
    reads_at_2(lab).contains(&true)
}

/// Node `id` holds slot 1 chosen.
fn knows_chosen_1(lab: &Lab, id: u64) -> bool {
    lab.view_of_world().nodes.iter().any(|node| {
        node.id == id
            && node
                .accepted
                .iter()
                .any(|vote| vote.slot == 1 && vote.chosen)
    })
}

/// How many messages of `kind` the reader delivered to `lane`.
fn delivered(lab: &Lab, kind: &str, lane: &str) -> usize {
    lab.delivered
        .get(&(kind.to_string(), lane.to_string()))
        .copied()
        .unwrap_or(0)
}

/// A vote of node 1 or node 2 for slot 1, on its way to node 0.
fn vote_to_0(message: &WireMessage) -> bool {
    message.kind == "Accepted" && message.to == "n0" && message.slot == Some(1)
}

/// The Commit of node 0 to node 2.
fn commit_to_2(message: &WireMessage) -> bool {
    message.kind == "Commit" && message.from == "n0" && message.to == "n2"
}

/// A question of node 2's read, or an answer to it.
fn read_message(message: &WireMessage) -> bool {
    (message.kind == "PreRead" && message.from == "n2")
        || (message.kind == "PreReadAck" && message.to == "n2")
}

const READ_INTRO: &[Step] = &[
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
    Step {
        says: "A client writes bravo. Node 0 puts it at slot 1, and nodes 1 and 2 vote. Their \
               votes are still on the wire to node 0.",
        play: |lab| {
            lab.act(&propose(0, "bravo"));
            lab.settle(|m| m.kind == "Accept");
        },
    },
];

const READ_PARTS: &[Part] = &[
    Part {
        label: "A client reads from node 2",
        holds: |lab| !reads_at_2(lab).is_empty(),
        idea: "Any node can serve a read. It need not lead.",
        plan: "Press the move that makes a client read from node 2.",
        next: |_| Some(Move::Extra { index: READ_AT_2 }),
        dead: |_| None,
    },
    Part {
        label: "Node 2 hears how high another node voted",
        holds: |lab| read_served(lab) || delivered(lab, "PreReadAck", "n2") > 0,
        idea: "Node 2 counts its own answer and needs one more: two of three is a majority. \
               Each node answers with the highest slot that it voted in.",
        plan: "Deliver the question of node 2 to node 1, then the answer of node 1.",
        next: |lab| {
            deliver_first(lab, |m| read_message(m) && (m.to == "n1" || m.from == "n1"))
                .or_else(|| deliver_first(lab, read_message))
                .or(Some(Move::Extra { index: READ_AT_2 }))
        },
        dead: |_| None,
    },
    Part {
        label: "Node 0 learns that bravo is chosen",
        holds: |lab| knows_chosen_1(lab, 0),
        idea: "The answers say slot 1, and node 2 has not applied slot 1: bravo may be chosen. \
               The read waits until node 2 knows.",
        plan: "Deliver a vote for bravo to node 0. Its own vote and one more are a majority.",
        next: |lab| deliver_first(lab, vote_to_0),
        dead: |lab| {
            (!knows_chosen_1(lab, 0) && !lab.wire().iter().any(vote_to_0)).then_some(
                "Every vote for bravo on its way to node 0 is lost, so node 0 never learns \
                 that bravo is chosen, and node 2 never applies slot 1. Undo the move that \
                 dropped a vote.",
            )
        },
    },
    Part {
        label: "Node 2 learns that bravo is chosen",
        holds: |lab| knows_chosen_1(lab, 2),
        idea: "The leader tells the others with a Commit.",
        plan: "Deliver the Commit of node 0 to node 2.",
        next: |lab| deliver_first(lab, commit_to_2),
        dead: |lab| {
            (knows_chosen_1(lab, 0)
                && !knows_chosen_1(lab, 2)
                && !lab.wire().iter().any(commit_to_2))
            .then_some(
                "The Commit to node 2 is lost, and with no clock, no heartbeat tells node 2 \
                 again. Undo the move that dropped it.",
            )
        },
    },
    Part {
        label: "Node 2 serves the read",
        holds: read_served,
        idea: "Node 2 serves a read once it applied every slot up to the highest answer.",
        plan: "Deliver the questions and the answers of the read. If they are lost, read again.",
        next: |lab| deliver_first(lab, read_message).or(Some(Move::Extra { index: READ_AT_2 })),
        dead: |_| None,
    },
];

const READ_GUIDE: &[Step] = &[
    Step {
        says: "A client reads from node 2. Node 2 voted at slot 1, so its own answer says slot \
               1. It asks nodes 0 and 1 how high they voted.",
        play: |lab| lab.extra("A client reads from node 2"),
    },
    Step {
        says: "Deliver the question to node 1, then its answer: slot 1. Node 2 has not applied \
               slot 1, and bravo may be chosen, so the read waits.",
        play: |lab| {
            lab.deliver(|m| m.kind == "PreRead" && m.from == "n2" && m.to == "n1");
            lab.deliver(|m| m.kind == "PreReadAck" && m.from == "n1" && m.to == "n2");
        },
    },
    Step {
        says: "Deliver the vote of node 1 to node 0. Two votes of three choose bravo, and node \
               0 sends a Commit.",
        play: |lab| lab.deliver(|m| vote_to_0(m) && m.from == "n1"),
    },
    Step {
        says: "Deliver the Commit to node 2. Node 2 applies bravo at slot 1, so now it serves \
               the read.",
        play: |lab| lab.deliver(commit_to_2),
    },
];

pub(super) const READ: Exercise = Exercise {
    id: "read",
    title: "A read that waits",
    task: "alpha is chosen at slot 0. Nodes 1 and 2 voted for bravo at slot 1, and node 0, the \
           leader, has not heard yet. A client reads from node 2. Get the read served.",
    rule_off: None,
    done: "A leaderless read asks a majority of acceptors for the highest slot that each \
           voted in, and waits until its own node applied that far. No leader serves the read. \
           But a vote at slot 1 means bravo may be chosen, so the read waits until node 2 \
           learns that slot 1 is chosen.",
    setup_on: Setup::Level(LOG),
    setup: |_| {},
    intro: READ_INTRO,
    parts: READ_PARTS,
    guide: READ_GUIDE,
    extras: || {
        let extras = vec![(
            "A client reads from node 2",
            Extra::Act(Action::QuorumRead {
                node: 2,
                client: None,
            }),
        )];
        // The index that the parts name.
        assert_eq!(extras[READ_AT_2].0, "A client reads from node 2");
        extras
    },
    goal: |lab| READ_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in READ_GUIDE {
            (step.play)(lab);
        }
    },
};
