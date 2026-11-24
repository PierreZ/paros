//! The exercises of lesson 1, single-decree Paxos: a vote is not a choice, two values with the
//! disk rule off, and the duel. Each one is guided: an intro, the goal as a checklist with
//! hints and dead ends, and a guide that "Show me how" plays.
//!
//! The world is `act1/the-duel`: acceptors 1, 2 and 3, proposers 5 and 8. A proposer's next
//! ballot is its own last round plus one (`DecreeWorld::open_ballot`), so a proposer that is
//! behind climbs one round per open.

use super::DECREE;
use super::Extra;
use super::Setup;
use super::ballot;
use super::voters;
use super::{Exercise, Lab, Move, NONE, Part, Step};
use crate::paxos::action::Action;
use crate::paxos::level::common::open;
use crate::paxos::view::{MessageView, NodeFlavour, WorldView};

/// The acceptors of the world.
const ACCEPTORS: [u64; 3] = [1, 2, 3];

/// The lane of node `id`, as a wire message names it.
fn lane(id: u64) -> String {
    format!("n{id}")
}

/// The promise of acceptor `id`, as a pair that orders as ballots do.
fn promise_of(world: &WorldView, id: u64) -> Option<(u64, u64)> {
    world
        .nodes
        .iter()
        .find(|node| node.id == id && node.flavour == NodeFlavour::Acceptor)
        .and_then(|node| node.promised.as_deref())
        .map(ballot)
}

/// `message` is a `Prepare` or an `Accept` to an acceptor whose promise is
/// above its ballot: delivered, it gets a `Nack`.
fn refused(world: &WorldView, message: &MessageView) -> bool {
    let (Some(sent), true) = (
        message.ballot.as_deref().map(ballot),
        message.kind == "Prepare" || message.kind == "Accept",
    ) else {
        return false;
    };
    promise_of(world, message.to).is_some_and(|promise| promise > sent)
}

/// `message` is an `Accept` that its acceptor takes: delivered, it is a
/// vote.
fn votes(world: &WorldView, message: &MessageView) -> bool {
    message.kind == "Accept" && ACCEPTORS.contains(&message.to) && !refused(world, message)
}

/// The first message on the wire that `hit` matches.
fn first(world: &WorldView, hit: impl Fn(&MessageView) -> bool) -> Option<&MessageView> {
    world.wire.iter().find(|m| hit(m))
}

/// A Phase 1 message.
fn phase1(message: &MessageView) -> bool {
    message.kind == "Prepare" || message.kind == "Promise"
}

// ---- A vote, not a choice ----

/// Proposer 5 is in Phase 2: an `Accept` is on the wire, an acceptor voted,
/// or the reader dropped an `Accept`. One proposer runs one ballot here, so
/// once it sends its Accepts it sends no more messages of Phase 1.
fn asks_for_votes(lab: &Lab) -> bool {
    let world = lab.view_of_world();
    world.wire.iter().any(|m| m.kind == "Accept")
        || voters(&world, "alpha") > 0
        || ACCEPTORS
            .iter()
            .any(|id| lab.dropped("Accept", &lane(*id)) > 0)
}

/// How many promises proposer 5 can still get: the ones it got, the ones on
/// the wire, and one for each `Prepare` on the wire. Each acceptor promises
/// ballot 1.5 at most once, and no message is copied here.
fn promises_left(lab: &Lab) -> usize {
    let world = lab.view_of_world();
    let got = lab
        .delivered
        .get(&("Promise".to_string(), lane(5)))
        .copied()
        .unwrap_or(0);
    let on_wire = world.wire.iter().filter(|m| phase1(m)).count();
    got + on_wire
}

const VOTE_INTRO: &[Step] = &[Step {
    says: "Proposer 5 opens ballot 1.5 for alpha. Its Prepares are on the wire, one to each \
           acceptor.",
    play: |lab| lab.act(&open(5, "alpha")),
}];

const VOTE_PARTS: &[Part] = &[
    Part {
        label: "Proposer 5 asks for votes",
        holds: asks_for_votes,
        idea: "A proposer asks for votes only with the promises of a majority: two of three.",
        plan: "Deliver the Prepares, then the Promises, until proposer 5 sends its Accepts.",
        next: |lab| {
            let world = lab.view_of_world();
            first(&world, phase1).map(|m| Move::Deliver { id: m.id })
        },
        dead: |lab| {
            (!asks_for_votes(lab) && promises_left(lab) < 2).then_some(
                "Two messages of Phase 1 are lost, so proposer 5 can never get two promises. \
                 Undo a lost message.",
            )
        },
    },
    Part {
        label: "Exactly one acceptor votes for alpha",
        holds: |lab| voters(&lab.view_of_world(), "alpha") == 1,
        idea: "An acceptor votes when an Accept reaches it. One Accept gives one vote.",
        plan: "Deliver one Accept, for example the one to acceptor 1.",
        next: |lab| {
            let world = lab.view_of_world();
            first(&world, |m| m.kind == "Accept" && m.to == 1)
                .or_else(|| first(&world, |m| m.kind == "Accept"))
                .map(|m| Move::Deliver { id: m.id })
        },
        dead: |lab| {
            let world = lab.view_of_world();
            let voted = voters(&world, "alpha");
            if voted >= 2 {
                Some(
                    "Two acceptors voted for alpha. Two of three are a majority, so alpha is \
                     chosen. Undo the second vote.",
                )
            } else if voted == 0
                && asks_for_votes(lab)
                && !world.wire.iter().any(|m| m.kind == "Accept")
            {
                Some(
                    "Every Accept is lost, and proposer 5 sends no more. Undo to keep one \
                     Accept.",
                )
            } else {
                None
            }
        },
    },
    Part {
        label: "No value is chosen, and the wire is empty",
        holds: |lab| {
            let world = lab.view_of_world();
            !chosen(lab) && world.wire.is_empty()
        },
        idea: "A second vote would make a majority. The other Accepts must never arrive.",
        plan: "Drop the other Accepts. Then deliver what is left.",
        next: |lab| {
            let world = lab.view_of_world();
            first(&world, |m| m.kind == "Accept")
                .map(|m| Move::Drop { id: m.id })
                .or_else(|| world.wire.first().map(|m| Move::Deliver { id: m.id }))
        },
        dead: |_| None,
    },
];

const VOTE_GUIDE: &[Step] = &[
    Step {
        says: "Deliver the Prepares and the Promises. Two promises are a majority, so proposer \
               5 sends an Accept to each acceptor.",
        play: |lab| lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise"),
    },
    Step {
        says: "Deliver one Accept, to acceptor 1. It writes its vote for alpha to disk.",
        play: |lab| lab.deliver(|m| m.kind == "Accept" && m.to == "n1"),
    },
    Step {
        says: "Drop the two other Accepts. One vote of three is not a majority, so alpha is not \
               chosen.",
        play: |lab| lab.drop_all(|m| m.kind == "Accept"),
    },
    Step {
        says: "Deliver the rest. The Accepted of acceptor 1 reaches proposer 5, which still waits \
               for a majority.",
        play: |lab| lab.settle(|_| true),
    },
];

pub(super) const VOTE_NOT_CHOSEN: Exercise = Exercise {
    id: "vote-not-chosen",
    title: "A vote, not a choice",
    task: "Proposer 5 asks for alpha. Leave a vote for alpha on exactly one acceptor, with \
           no value chosen and no message on the wire.",
    rule_off: None,
    done: "One vote of three is not a choice. A later proposer that hears of this vote must \
           propose alpha, but it is free to choose another value when it does not hear of it.",
    setup_on: Setup::Level(DECREE),
    setup: |_| {},
    intro: VOTE_INTRO,
    parts: VOTE_PARTS,
    guide: VOTE_GUIDE,
    extras: Vec::new,
    goal: |lab| VOTE_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in VOTE_GUIDE {
            (step.play)(lab);
        }
    },
};

// ---- Two values ----

/// The extra moves of `two-values`, by their index in its list.
const OPEN_8: usize = 0;
const OPEN_5: usize = 1;
/// The index of "Acceptor `id` loses its disk".
fn wipe(id: u64) -> usize {
    usize::try_from(id + 1).expect("an acceptor id is small")
}

/// The value chosen first, if any.
fn first_value(world: &WorldView) -> Option<String> {
    world.chosen.as_ref().map(|chosen| chosen.value.clone())
}

/// The acceptors that hold a vote for `value`.
fn holders(world: &WorldView, value: &str) -> Vec<u64> {
    world
        .nodes
        .iter()
        .filter(|node| node.flavour == NodeFlavour::Acceptor)
        .filter(|node| node.accepted.iter().any(|vote| vote.value == value))
        .map(|node| node.id)
        .collect()
}

/// A majority of the acceptors holds no vote for the value chosen first, or
/// a second value is chosen already: once two values are chosen, the
/// exercise is done, whatever the acceptors hold after it.
fn majority_forgot(lab: &Lab) -> bool {
    let world = lab.view_of_world();
    two_chosen(lab) || first_value(&world).is_some_and(|value| holders(&world, &value).len() < 2)
}

/// Two different values are chosen.
fn two_chosen(lab: &Lab) -> bool {
    lab.game().world().violation().is_some()
}

const TWO_INTRO: &[Step] = &[Step {
    says: "Proposer 5 opens ballot 1.5 for alpha. Its Prepares are on the wire, one to each \
           acceptor.",
    play: |lab| lab.act(&open(5, "alpha")),
}];

const TWO_PARTS: &[Part] = &[
    Part {
        label: "A proposer learns that a value is chosen",
        holds: |lab| lab.view_of_world().chosen.is_some(),
        idea: "A value is chosen when a majority votes for it at one ballot.",
        plan: "Deliver every message. When the wire is empty and no value is chosen, open a \
               higher ballot of proposer 5.",
        next: |lab| {
            if lab.view_of_world().wire.is_empty() {
                Some(Move::Extra { index: OPEN_5 })
            } else {
                Some(Move::DeliverAll)
            }
        },
        dead: |_| None,
    },
    Part {
        label: "A majority holds no vote for it",
        holds: majority_forgot,
        idea: "Phase 1 finds a chosen value through the votes on the disks. Without the disk \
               rule, a disk can be lost.",
        plan: "Wipe the disk of an acceptor that holds a vote for the chosen value, until only \
               one holds it.",
        next: |lab| {
            let world = lab.view_of_world();
            let value = first_value(&world)?;
            holders(&world, &value)
                .first()
                .map(|id| Move::Extra { index: wipe(*id) })
        },
        dead: |_| None,
    },
    Part {
        label: "A proposer learns that a second value is chosen",
        holds: two_chosen,
        idea: "A proposer that hears of no vote is free to propose its own value.",
        plan: "Open a ballot for the other value. Keep its messages away from the acceptor that \
               still holds the first value, and deliver the rest.",
        next: |lab| {
            let world = lab.view_of_world();
            let value = first_value(&world)?;
            let other = if value == "alpha" { 8 } else { 5 };
            let held = holders(&world, &value);
            // Lose what the other proposer sends, and every message to or
            // from the acceptor that holds the first value: its promise
            // would report it.
            if let Some(m) = first(&world, |m| {
                held.contains(&m.to) || held.contains(&m.from) || (m.from != other && m.to != other)
            }) {
                return Some(Move::Drop { id: m.id });
            }
            if let Some(m) = world.wire.first() {
                return Some(Move::Deliver { id: m.id });
            }
            Some(Move::Extra {
                index: if other == 8 { OPEN_8 } else { OPEN_5 },
            })
        },
        dead: |_| None,
    },
];

const TWO_GUIDE: &[Step] = &[
    Step {
        says: "Deliver the Prepares and the Promises of proposer 5. It holds ballot 1.5.",
        play: |lab| lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise"),
    },
    Step {
        says: "Deliver the Accepts to acceptors 1 and 2, and drop the one to acceptor 3. Two \
               votes of three: alpha is chosen.",
        play: |lab| {
            lab.settle(|m| m.kind == "Accept" && m.to != "n3");
            lab.drop_all(|m| m.kind == "Accept");
        },
    },
    Step {
        says: "Deliver the votes to proposer 5, and drop its Commits: no acceptor learns that \
               alpha is chosen.",
        play: |lab| {
            lab.settle(|m| m.kind == "Accepted");
            lab.drop_all(|_| true);
        },
    },
    Step {
        says: "Acceptor 2 loses its disk. It starts again with no promise and no vote. paros \
               refuses this boot; here the rule is off.",
        play: |lab| lab.extra("Acceptor 2 loses its disk"),
    },
    Step {
        says: "Proposer 8 opens ballot 1.8 for bravo. Drop its Prepare to acceptor 1, the only \
               acceptor that still holds alpha.",
        play: |lab| {
            lab.extra("Proposer 8 opens a ballot for bravo");
            lab.settle(|m| m.to != "n1" && (m.kind == "Prepare" || m.kind == "Promise"));
            lab.drop_all(|m| m.kind == "Prepare");
        },
    },
    Step {
        says: "Acceptors 2 and 3 report no vote, so proposer 8 asks for bravo. Two votes of \
               three: bravo is chosen too.",
        play: |lab| lab.settle(|m| m.to != "n1" && (m.kind == "Accept" || m.kind == "Accepted")),
    },
];

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
    setup: |_| {},
    intro: TWO_INTRO,
    parts: TWO_PARTS,
    guide: TWO_GUIDE,
    extras: || {
        let extras = vec![
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
        ];
        // The indexes that the parts name.
        assert_eq!(extras[OPEN_8].0, "Proposer 8 opens a ballot for bravo");
        assert_eq!(extras[OPEN_5].0, "Proposer 5 opens a higher ballot");
        for id in ACCEPTORS {
            assert_eq!(extras[wipe(id)].0, format!("Acceptor {id} loses its disk"));
        }
        extras
    },
    goal: |lab| TWO_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in TWO_GUIDE {
            (step.play)(lab);
        }
    },
};

// ---- The duel ----

/// The extra moves of `duel`: the index of "Proposer `id` opens a ballot".
fn opens(id: u64) -> usize {
    usize::from(id != 5)
}

/// The other proposer.
fn rival(id: u64) -> u64 {
    if id == 5 { 8 } else { 5 }
}

/// A value is chosen: two acceptors voted for it at one ballot, whether or
/// not a proposer learned it.
fn chosen(lab: &Lab) -> bool {
    lab.game()
        .world()
        .decree()
        .is_some_and(crate::paxos::world::decree::DecreeWorld::chosen_by_votes)
}

/// The value is chosen: no move takes it back.
fn chosen_says(lab: &Lab) -> Option<&'static str> {
    chosen(lab).then_some(
        "Two acceptors voted for one value at one ballot, so it is chosen, and a chosen value \
         stays. Undo the move that gave the second vote.",
    )
}

/// The move that gets proposer `id` a refusal, with no vote on the way: a
/// vote can make a majority, and a chosen value ends the duel.
///
/// Each move is the first of these that applies: deliver a `Nack` to `id`;
/// deliver a message of `id` that its acceptor refuses; drop an `Accept` of
/// the rival that its acceptor would take; when an `Accept` of `id` would
/// be taken, raise that acceptor's promise above it with a `Prepare` of the
/// rival (the rival opens again until its ballot is high enough); deliver
/// any other message that gives no vote; with the wire empty, `id` opens a
/// ballot.
fn refusal_for(lab: &Lab, id: u64) -> Move {
    let world = lab.view_of_world();
    let other = rival(id);
    let deliver = |m: &MessageView| Move::Deliver { id: m.id };
    if let Some(m) = first(&world, |m| m.kind == "Nack" && m.to == id) {
        return deliver(m);
    }
    if let Some(m) = first(&world, |m| m.from == id && refused(&world, m)) {
        return deliver(m);
    }
    if let Some(m) = first(&world, |m| m.from == other && votes(&world, m)) {
        return Move::Drop { id: m.id };
    }
    if let Some(accept) = first(&world, |m| m.from == id && votes(&world, m)) {
        let above = accept.ballot.as_deref().map(ballot);
        let raise = first(&world, |m| {
            m.kind == "Prepare"
                && m.from == other
                && m.to == accept.to
                && m.ballot.as_deref().map(ballot) > above
                && !refused(&world, m)
        });
        return raise.map_or(
            Move::Extra {
                index: opens(other),
            },
            deliver,
        );
    }
    if let Some(m) = first(&world, |m| m.kind != "Accept" || refused(&world, m)) {
        return deliver(m);
    }
    // Every message gives a vote or a refusal, and every vote and every
    // refusal of either proposer is handled above: the wire is empty.
    assert!(world.wire.is_empty(), "every message is handled above");
    Move::Extra { index: opens(id) }
}

const DUEL_PARTS: &[Part] = &[
    Part {
        label: "Proposer 5 gets two refusals",
        holds: |lab| lab.refusals(5) >= 2,
        idea: "An acceptor refuses a ballot below its promise, and answers Nack.",
        plan: "Let proposer 8 get promises for a higher ballot. Then deliver the messages of \
               proposer 5.",
        next: |lab| Some(refusal_for(lab, 5)),
        dead: chosen_says,
    },
    Part {
        label: "Proposer 8 gets two refusals",
        holds: |lab| lab.refusals(8) >= 2,
        idea: "A refused proposer opens a higher ballot, and its promises refuse the other one.",
        plan: "Let proposer 5 open again, above proposer 8, and get promises. Then deliver the \
               messages of proposer 8.",
        next: |lab| Some(refusal_for(lab, 8)),
        dead: chosen_says,
    },
    Part {
        label: "No value is chosen",
        holds: |lab| !chosen(lab),
        idea: "Two votes at one ballot choose a value.",
        plan: "Never deliver an Accept that its acceptor takes.",
        next: |_| None,
        dead: chosen_says,
    },
];

const DUEL_GUIDE: &[Step] = &[
    Step {
        says: "Proposer 5 opens ballot 1.5. Deliver its Prepares and Promises: it now asks for \
               votes.",
        play: |lab| {
            lab.extra("Proposer 5 opens a ballot for alpha");
            lab.settle(|m| m.kind == "Prepare" || m.kind == "Promise");
        },
    },
    Step {
        says: "Proposer 8 opens ballot 1.8. Deliver its Prepares and Promises: each acceptor now \
               promises 1.8.",
        play: |lab| {
            lab.extra("Proposer 8 opens a ballot for bravo");
            lab.settle(|m| {
                (m.from == "n8" || m.to == "n8") && (m.kind == "Prepare" || m.kind == "Promise")
            });
        },
    },
    Step {
        says: "Deliver the Accepts of proposer 5. Each acceptor promised 1.8, so each one \
               refuses ballot 1.5: proposer 5 gets the Nacks.",
        play: |lab| lab.settle(|m| m.from == "n5" || m.to == "n5"),
    },
    Step {
        says: "Proposer 5 opens ballot 2.5, above 1.8. Deliver its Prepares and Promises.",
        play: |lab| {
            lab.extra("Proposer 5 opens a ballot for alpha");
            lab.settle(|m| {
                (m.from == "n5" || m.to == "n5") && (m.kind == "Prepare" || m.kind == "Promise")
            });
        },
    },
    Step {
        says: "Deliver the Accepts of proposer 8. Each acceptor promised 2.5, so proposer 8 gets \
               the Nacks. Nothing is chosen.",
        play: |lab| lab.settle(|m| m.from == "n8" || m.to == "n8"),
    },
];

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
    parts: DUEL_PARTS,
    guide: DUEL_GUIDE,
    extras: || {
        let extras = vec![
            (
                "Proposer 5 opens a ballot for alpha",
                Extra::Act(open(5, "alpha")),
            ),
            (
                "Proposer 8 opens a ballot for bravo",
                Extra::Act(open(8, "bravo")),
            ),
        ];
        // The indexes that the parts name.
        assert_eq!(extras[opens(5)].0, "Proposer 5 opens a ballot for alpha");
        assert_eq!(extras[opens(8)].0, "Proposer 8 opens a ballot for bravo");
        extras
    },
    goal: |lab| DUEL_PARTS.iter().all(|part| (part.holds)(lab)),
    solution: |lab| {
        for step in DUEL_GUIDE {
            (step.play)(lab);
        }
    },
};
