//! The exercises of lesson 6, Compartmentalized Paxos: a proxy leader that stops, lost votes, a replica that waits.

use super::Extra;
use super::Setup;
use super::{Exercise, NO_PARTS, NONE};
use crate::paxos::trace::compart::desk::Verb;

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
    setup: |lab| {
        lab.verb(&Verb::Write("alpha"));
        lab.settle(|_| true);
        lab.verb(&Verb::CrashProxy(1));
    },
    intro: NONE,
    parts: NO_PARTS,
    guide: NONE,
    extras: || {
        vec![
            (
                "The client writes bravo",
                Extra::Compart(Verb::Write("bravo")),
            ),
            ("Node 0 beats", Extra::Compart(Verb::LeaderBeats)),
        ]
    },
    goal: |lab| {
        lab.desk()
            .chosen()
            .iter()
            .any(|line| line == "slot 1 = bravo")
    },
    solution: |lab| {
        lab.extra("The client writes bravo");
        lab.settle(|_| true);
        while !lab.solved() {
            lab.extra("Node 0 beats");
            lab.settle(|_| true);
        }
    },
};

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
    parts: NO_PARTS,
    guide: NONE,
    extras: || {
        vec![
            (
                "The client writes alpha",
                Extra::Compart(Verb::Write("alpha")),
            ),
            ("Proxy leader 0 beats", Extra::Compart(Verb::ProxyBeats(0))),
        ]
    },
    goal: |lab| {
        lab.dropped("Accepted", "p0") >= 2
            && lab
                .desk()
                .chosen()
                .iter()
                .any(|line| line == "slot 0 = alpha")
    },
    solution: |lab| {
        lab.extra("The client writes alpha");
        lab.settle(|m| !(m.kind == "Accepted" && m.to == "p0"));
        lab.drop_all(|m| m.kind == "Accepted" && m.to == "p0");
        lab.extra("Proxy leader 0 beats");
        lab.settle(|_| true);
    },
};

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
    parts: NO_PARTS,
    guide: NONE,
    extras: || {
        vec![
            (
                "The client writes alpha",
                Extra::Compart(Verb::Write("alpha")),
            ),
            (
                "The client writes bravo",
                Extra::Compart(Verb::Write("bravo")),
            ),
        ]
    },
    goal: |lab| lab.desk().waited(0) && lab.desk().learned(0) >= Some(1),
    solution: |lab| {
        lab.extra("The client writes alpha");
        lab.extra("The client writes bravo");
        lab.settle(|m| !(m.kind == "Commit" && m.to == "r0" && m.slot == Some(0)));
        lab.settle(|_| true);
    },
};
