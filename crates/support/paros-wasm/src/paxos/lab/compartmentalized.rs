//! The exercises of lesson 6, Compartmentalized Paxos: a proxy leader that
//! stops, lost votes, a replica that waits. Each one is guided: a checklist,
//! three hints per line and a guide that "Show me how" plays.
//!
//! Every predicate here reads the deployment ([`Desk`]): what the roles of
//! `paros-consensus` hold and what is on the wire. A dead end is a line that
//! no move of the exercise can make hold again.
//!
//! [`Desk`]: crate::paxos::trace::compart::desk::Desk

use super::Extra;
use super::Setup;
use super::{Exercise, Lab, Move, NONE, Part, Step, WireMessage};
use crate::paxos::trace::compart::desk::Verb;

/// The first message on the wire that `hit` matches, to deliver.
fn deliver_first(lab: &Lab, hit: impl Fn(&WireMessage) -> bool) -> Option<Move> {
    lab.wire()
        .into_iter()
        .find(|m| hit(m))
        .map(|m| Move::Deliver { id: m.id })
}

/// The first message on the wire that `hit` matches, to drop.
fn drop_first(lab: &Lab, hit: impl Fn(&WireMessage) -> bool) -> Option<Move> {
    lab.wire()
        .into_iter()
        .find(|m| hit(m))
        .map(|m| Move::Drop { id: m.id })
}

/// Whether a message of `kind` for `slot` goes from `from` to `to` (a lane,
/// or a prefix of lanes: `n` for every node).
fn is(m: &WireMessage, kind: &str, slot: u64, from: &str, to: &str) -> bool {
    m.kind == kind && m.slot == Some(slot) && m.from.starts_with(from) && m.to.starts_with(to)
}

/// How many messages on the wire `hit` matches.
fn on_wire(lab: &Lab, hit: impl Fn(&WireMessage) -> bool) -> usize {
    lab.wire().iter().filter(|m| hit(m)).count()
}

/// Whether the acceptors hold `value` chosen at `slot`.
fn chosen(lab: &Lab, slot: u64, value: &str) -> bool {
    let line = format!("slot {slot} = {value}");
    lab.desk().chosen().contains(&line)
}

// ---- A proxy leader down ----------------------------------------------------

/// The extra moves of `proxy-down`, by their index in its list.
const BRAVO: usize = 0;
const LEADER_BEATS: usize = 1;

/// Node 0 gave slot 1 to bravo: the first write after alpha.
fn bravo_has_slot_1(lab: &Lab) -> bool {
    lab.desk().written(1)
}

const PROXY_DOWN_INTRO: &[Step] = &[
    Step {
        says: "A client writes alpha. Node 0 gives slot 0 to proxy leader 0, and a majority \
               of the acceptors chooses alpha.",
        play: |lab| {
            lab.verb(&Verb::Write("alpha"));
            lab.settle(|_| true);
        },
    },
    Step {
        says: "Proxy leader 1 crashes. It holds nothing on disk, so nothing is lost with it.",
        play: |lab| lab.verb(&Verb::CrashProxy(1)),
    },
];

const PROXY_DOWN_PARTS: &[Part] = &[
    Part {
        label: "bravo gets slot 1, which goes to proxy leader 1",
        holds: bravo_has_slot_1,
        idea: "Node 0 gives each write the next free slot, and slot s goes to proxy leader s \
               mod 2.",
        plan: "Write bravo. Node 0 gives it slot 1 and hands it to proxy leader 1.",
        next: |_| Some(Move::Extra { index: BRAVO }),
        dead: |_| None,
    },
    Part {
        label: "Node 0 takes slot 1 back",
        holds: |lab| lab.desk().taken_back(1),
        idea: "A leader counts the beats on which a proxy leader did not answer. After a few, \
               it takes the slot back.",
        plan: "Let node 0 beat until it takes slot 1 back from proxy leader 1.",
        next: |_| {
            Some(Move::Extra {
                index: LEADER_BEATS,
            })
        },
        dead: |_| None,
    },
    Part {
        label: "bravo is chosen at slot 1",
        holds: |lab| chosen(lab, 1, "bravo"),
        idea: "A slot taken back is node 0's own round: it sends the Accepts and counts the \
               votes itself.",
        plan: "Deliver the Accepts of node 0 and the votes. If a message is lost, node 0 \
               sends it again on its next beat.",
        next: |lab| {
            deliver_first(lab, |_| true).or(Some(Move::Extra {
                index: LEADER_BEATS,
            }))
        },
        dead: |_| None,
    },
];

const PROXY_DOWN_GUIDE: &[Step] = &[
    Step {
        says: "Write bravo. Node 0 gives it slot 1 and hands it to proxy leader 1. The proxy \
               is down, so the message is lost.",
        play: |lab| {
            lab.extra("The client writes bravo");
            lab.settle(|_| true);
        },
    },
    Step {
        says: "Node 0 beats. Slot 1 is not chosen, so it sends the slot to proxy leader 1 \
               again. No answer comes.",
        play: |lab| {
            lab.extra("Node 0 beats");
            lab.settle(|_| true);
        },
    },
    Step {
        says: "Node 0 beats again. The proxy did not answer twice, so node 0 takes slot 1 back \
               and sends the Accepts itself. bravo is chosen.",
        play: |lab| {
            while !lab.desk().taken_back(1) {
                lab.extra("Node 0 beats");
            }
            lab.settle(|_| true);
        },
    },
];

pub(super) const PROXY_DOWN: Exercise = Exercise {
    id: "proxy-down",
    title: "A proxy leader down",
    task: "Proxy leader 1 is down. Slot 1 goes to it (slot mod 2). The client writes bravo: \
           get bravo chosen at slot 1.",
    rule_off: None,
    done: "A proxy leader holds nothing on disk, so its crash loses no value. Node 0 sends \
           the slot again on each beat, then takes it back and sends the Accepts itself. A \
           dead proxy costs a few beats.",
    setup_on: Setup::Compartments,
    setup: |_| {},
    intro: PROXY_DOWN_INTRO,
    parts: PROXY_DOWN_PARTS,
    guide: PROXY_DOWN_GUIDE,
    extras: || {
        let extras = vec![
            (
                "The client writes bravo",
                Extra::Compart(Verb::Write("bravo")),
            ),
            ("Node 0 beats", Extra::Compart(Verb::LeaderBeats)),
        ];
        // The indexes that the parts name.
        assert_eq!(extras[BRAVO].0, "The client writes bravo");
        assert_eq!(extras[LEADER_BEATS].0, "Node 0 beats");
        extras
    },
    goal: |lab| PROXY_DOWN_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in PROXY_DOWN_GUIDE {
            (step.play)(lab);
        }
    },
};

// ---- Lost votes -------------------------------------------------------------

/// The extra moves of `lost-votes`, by their index in its list.
const ALPHA: usize = 0;
const PROXY_0_BEATS: usize = 1;

/// The votes of slot 0 to proxy leader 0 that were lost.
fn votes_lost(lab: &Lab) -> usize {
    lab.desk().lost("Accepted", "p0", 0)
}

/// A vote of slot 0 on its way to proxy leader 0.
fn vote_to_p0(m: &WireMessage) -> bool {
    is(m, "Accepted", 0, "n", "p0")
}

/// An Accept of slot 0 from proxy leader 0 on its way to an acceptor.
fn accept_of_p0(m: &WireMessage) -> bool {
    is(m, "Accept", 0, "p0", "n")
}

/// Node 0 hands slot 0 to proxy leader 0.
fn handoff_0(m: &WireMessage) -> bool {
    is(m, "Accept", 0, "n0", "p0")
}

/// Proxy leader 0 still counts slot 0, or will once it gets the slot: it
/// sends the Accepts again on each of its beats, so more votes come.
fn p0_will_count(lab: &Lab) -> bool {
    !lab.desk().written(0) || lab.desk().counts(0, 0) || on_wire(lab, handoff_0) > 0
}

const LOST_VOTES_PARTS: &[Part] = &[
    Part {
        label: "Two votes for slot 0 are lost on their way to proxy leader 0",
        holds: |lab| votes_lost(lab) >= 2,
        idea: "A proxy leader counts the votes of its slots. A vote that is lost never counts.",
        plan: "Write alpha and deliver the Accepts of proxy leader 0. Drop each vote on its way \
               back to proxy leader 0.",
        next: |lab| {
            if !lab.desk().written(0) {
                return Some(Move::Extra { index: ALPHA });
            }
            drop_first(lab, vote_to_p0)
                .or_else(|| deliver_first(lab, |m| accept_of_p0(m) || handoff_0(m)))
                .or_else(|| {
                    lab.desk().counts(0, 0).then_some(Move::Extra {
                        index: PROXY_0_BEATS,
                    })
                })
        },
        dead: |lab| {
            let more = on_wire(lab, vote_to_p0) + on_wire(lab, accept_of_p0);
            if votes_lost(lab) + more >= 2 || p0_will_count(lab) {
                None
            } else if lab.desk().lost("Accept", "p0", 0) > 0 {
                Some(
                    "The hand-off of slot 0 to proxy leader 0 was lost, so no Accept and no vote \
                     of slot 0 comes. Node 0 does not beat here. Undo that move.",
                )
            } else {
                Some(
                    "Proxy leader 0 heard a majority and closed slot 0, so no vote for slot 0 \
                     comes again. Undo to before the votes reached it.",
                )
            }
        },
    },
    Part {
        label: "alpha is chosen at slot 0",
        holds: |lab| chosen(lab, 0, "alpha"),
        idea: "A proxy leader that hears no majority sends the same Accepts again on its beat. \
               An acceptor that voted answers with the same vote.",
        plan: "Let proxy leader 0 beat, and deliver its Accepts, the votes and the Commits.",
        next: |lab| {
            deliver_first(lab, |m| {
                is(m, "Commit", 0, "p0", "n") || accept_of_p0(m) || vote_to_p0(m) || handoff_0(m)
            })
            .or_else(|| {
                lab.desk().counts(0, 0).then_some(Move::Extra {
                    index: PROXY_0_BEATS,
                })
            })
            .or_else(|| (!lab.desk().written(0)).then_some(Move::Extra { index: ALPHA }))
        },
        dead: |lab| {
            let commit = on_wire(lab, |m| is(m, "Commit", 0, "p0", "n")) > 0;
            (!chosen(lab, 0, "alpha") && !commit && !p0_will_count(lab)).then_some(
                "No proxy leader counts slot 0 now, and no Commit of slot 0 is on its way to an \
                 acceptor. Node 0 does not beat here. Undo the move that lost it.",
            )
        },
    },
];

const LOST_VOTES_GUIDE: &[Step] = &[
    Step {
        says: "Write alpha. Node 0 hands slot 0 to proxy leader 0, which sends an Accept to \
               each acceptor. Each acceptor votes.",
        play: |lab| {
            lab.extra("The client writes alpha");
            lab.settle(|m| !vote_to_p0(m));
        },
    },
    Step {
        says: "Drop each vote on its way to proxy leader 0. The proxy hears no majority, so \
               it decides nothing.",
        play: |lab| lab.drop_all(vote_to_p0),
    },
    Step {
        says: "Proxy leader 0 beats and sends the same Accepts again. Each acceptor answers \
               with its vote, and alpha is chosen.",
        play: |lab| {
            lab.extra("Proxy leader 0 beats");
            lab.settle(|_| true);
        },
    },
];

pub(super) const LOST_VOTES: Exercise = Exercise {
    id: "lost-votes",
    title: "Lost votes",
    task: "The client writes alpha, and proxy leader 0 sends the Accepts. Drop at least two \
           votes (Accepted) on their way to proxy leader 0. Then get alpha chosen anyway.",
    rule_off: None,
    done: "A proxy leader that hears no majority sends the same Accepts again on its next \
           beat. An acceptor that already voted answers with the same vote, so a re-send \
           is always safe.",
    setup_on: Setup::Compartments,
    setup: |_| {},
    intro: NONE,
    parts: LOST_VOTES_PARTS,
    guide: LOST_VOTES_GUIDE,
    extras: || {
        let extras = vec![
            (
                "The client writes alpha",
                Extra::Compart(Verb::Write("alpha")),
            ),
            ("Proxy leader 0 beats", Extra::Compart(Verb::ProxyBeats(0))),
        ];
        assert_eq!(extras[ALPHA].0, "The client writes alpha");
        assert_eq!(extras[PROXY_0_BEATS].0, "Proxy leader 0 beats");
        extras
    },
    goal: |lab| LOST_VOTES_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in LOST_VOTES_GUIDE {
            (step.play)(lab);
        }
    },
};

// ---- A replica waits --------------------------------------------------------

/// The extra moves of `replica-waits`, by their index in its list.
const WRITE_ALPHA: usize = 0;
const WRITE_BRAVO: usize = 1;
const P0_BEATS: usize = 2;
const P1_BEATS: usize = 3;

/// A message that moves `slot` on: its hand-off to its proxy leader, an
/// Accept, a vote, or a Commit to an acceptor or to replica 0. A Commit to
/// replica 1 is not one.
fn moves_slot(m: &WireMessage, slot: u64) -> bool {
    m.slot == Some(slot) && m.to != "r1"
}

/// Replica 0 can still learn `slot`: it knows it, a Commit of it is on its
/// way to replica 0, its proxy leader still counts it (or will, once it gets
/// the slot), or node 0 has not given it to a write yet.
fn can_learn(lab: &Lab, slot: u64) -> bool {
    let desk = lab.desk();
    let proxy = slot % 2;
    desk.knows(0, slot)
        || !desk.written(slot)
        || desk.counts(proxy, slot)
        || on_wire(lab, |m| {
            is(m, "Commit", slot, "p", "r0") || is(m, "Accept", slot, "n0", &format!("p{proxy}"))
        }) > 0
}

/// The move that writes the next value: alpha takes slot 0, bravo slot 1.
fn next_write(lab: &Lab) -> Move {
    let index = if lab.desk().written(0) {
        WRITE_BRAVO
    } else {
        WRITE_ALPHA
    };
    Move::Extra { index }
}

/// The move that moves `slot` on toward replica 0, if one does.
fn push_slot(lab: &Lab, slot: u64) -> Option<Move> {
    let desk = lab.desk();
    if !desk.written(slot) {
        return Some(next_write(lab));
    }
    if desk.knows(0, slot) {
        return None;
    }
    deliver_first(lab, |m| moves_slot(m, slot)).or_else(|| {
        let index = if slot.is_multiple_of(2) {
            P0_BEATS
        } else {
            P1_BEATS
        };
        desk.counts(slot % 2, slot).then_some(Move::Extra { index })
    })
}

const REPLICA_WAITS_PARTS: &[Part] = &[
    Part {
        label: "Replica 0 learns slot 1 before slot 0",
        holds: |lab| lab.desk().held(0, 1),
        idea: "Slot 0 and slot 1 go to two proxy leaders. Each one sends its Commit when its \
               own slot is chosen, in any order.",
        plan: "Write alpha and bravo. Move slot 1 through proxy leader 1 to replica 0, and hold \
               the messages of slot 0.",
        next: |lab| push_slot(lab, 1),
        dead: |lab| {
            if lab.desk().knows(0, 0) {
                Some(
                    "Replica 0 learned slot 0 first. Undo to before the Commit of slot 0 \
                     reached it.",
                )
            } else if !can_learn(lab, 1) {
                Some(
                    "Slot 1 can no longer reach replica 0: its Commit or its hand-off was lost, \
                     and nothing sends it again. Undo that move.",
                )
            } else {
                None
            }
        },
    },
    Part {
        label: "Replica 0 learns slots 0 and 1",
        holds: |lab| lab.desk().learned(0) >= Some(1),
        idea: "A replica applies the slots in slot order. It holds slot 1 until slot 0 is \
               chosen too.",
        plan: "Now move slot 0 through proxy leader 0, and deliver its Commit to replica 0.",
        next: |lab| push_slot(lab, 0).or_else(|| push_slot(lab, 1)),
        dead: |lab| {
            (!can_learn(lab, 0) || !can_learn(lab, 1)).then_some(
                "A slot can no longer reach replica 0: its Commit or its hand-off was lost, and \
                 nothing sends it again. Undo that move.",
            )
        },
    },
];

const REPLICA_WAITS_GUIDE: &[Step] = &[
    Step {
        says: "Write alpha and bravo. Slot 0 goes to proxy leader 0, and slot 1 to proxy \
               leader 1.",
        play: |lab| {
            lab.extra("The client writes alpha");
            lab.extra("The client writes bravo");
        },
    },
    Step {
        says: "Move slot 1 first. Proxy leader 1 hears a majority and sends its Commit. \
               Replica 0 learns slot 1 and holds it: slot 0 is not chosen yet.",
        play: |lab| lab.settle(|m| m.slot == Some(1)),
    },
    Step {
        says: "Now move slot 0. Replica 0 learns it and applies both slots, in slot order.",
        play: |lab| lab.settle(|_| true),
    },
];

pub(super) const REPLICA_WAITS: Exercise = Exercise {
    id: "replica-waits",
    title: "A replica waits",
    task: "The client writes alpha (slot 0, proxy leader 0) and bravo (slot 1, proxy leader \
           1). Make replica 0 learn slot 1 before slot 0. Then let it learn both.",
    rule_off: None,
    done: "A replica learns the slots in any order, but it applies them in slot order. It \
           holds slot 1 and waits until slot 0 is chosen too, so every replica applies the \
           same log.",
    setup_on: Setup::Compartments,
    setup: |_| {},
    intro: NONE,
    parts: REPLICA_WAITS_PARTS,
    guide: REPLICA_WAITS_GUIDE,
    extras: || {
        let extras = vec![
            (
                "The client writes alpha",
                Extra::Compart(Verb::Write("alpha")),
            ),
            (
                "The client writes bravo",
                Extra::Compart(Verb::Write("bravo")),
            ),
            ("Proxy leader 0 beats", Extra::Compart(Verb::ProxyBeats(0))),
            ("Proxy leader 1 beats", Extra::Compart(Verb::ProxyBeats(1))),
        ];
        assert_eq!(extras[WRITE_ALPHA].0, "The client writes alpha");
        assert_eq!(extras[WRITE_BRAVO].0, "The client writes bravo");
        assert_eq!(extras[P0_BEATS].0, "Proxy leader 0 beats");
        assert_eq!(extras[P1_BEATS].0, "Proxy leader 1 beats");
        extras
    },
    goal: |lab| REPLICA_WAITS_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in REPLICA_WAITS_GUIDE {
            (step.play)(lab);
        }
    },
};
