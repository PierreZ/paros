//! The tour run of Matchmaker Paxos (tour plan §5.5): chapter 7, "Change the
//! acceptors". One world with no restart of the world, and the narrator
//! makes each change and each fault in it: the matchmakers before Phase 1, a
//! new set of acceptors as a new ballot, the cut of the log into segments
//! with a `Roll` (`docs/architecture/04-7-segments.md` §4.7), the crash of
//! the leader, a Phase 1 over the old set and the new one, and the
//! retirement of the acceptor that left.

use super::{LEVEL, chosen_at, leads, matchmaking, prepare_to};
use crate::paxos::action::{Action, BallotSpec};
use crate::paxos::level::common::{crash, propose, start_election as elect};
use crate::paxos::trace::pages::{any, log_outcome, touches};
use crate::paxos::trace::{Recorder, Scenario};
use crate::paxos::view::{SegmentView, WorldView};
use paros_consensus::NodeId;

/// Node `id` is up and its tail segment is `id` from `start`.
fn in_segment(node: u64, id: u64, start: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world.nodes.iter().any(|view| {
            view.id == node && view.alive && view.segment == Some(SegmentView { id, start })
        })
    }
}

/// An `Accept` for `slot` is on the wire.
fn accept_for(slot: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .wire
            .iter()
            .any(|message| message.kind == "Accept" && message.slot == Some(slot))
    }
}

/// Chapter 7: the matchmakers, a change of the acceptors, two rolls of the
/// log, the leader's crash, and the retirement of node 2.
pub(super) fn tour() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.tour("matchmakers", "Change the acceptors");
    run.beat(
        "Nodes 0, 1 and 2 are the acceptors. Two matchmakers keep which acceptors each ballot \
         uses. Node 3 waits outside.",
    )
    .beat(
        "To lead, node 0 first registers ballot 2.0 with acceptors 0, 1 and 2 at the matchmakers.",
    )
    .more("base")
    .play(elect(0), "node 0 starts an election")
    .settle(matchmaking)
    .holds(
        prepare_to(1),
        "the Prepare leaves only after the matchmakers answered",
    )
    .beat("Only now does node 0 send its Prepare. Two promises of three: node 0 leads.")
    .settle(any)
    .holds(leads(0), "node 0 leads")
    .beat("A client writes alpha. Two votes of three choose it at slot 0.")
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle(any)
    .holds(chosen_at(0, "alpha"), "alpha is chosen at slot 0")
    .beat("Now node 2 must leave, and node 3 takes its place.")
    .beat(
        "A set never changes in place. Node 0 registers a new ballot, 3.0, with acceptors 0, 1 \
         and 3.",
    )
    .more("replace")
    .play(
        Action::Reconfigure {
            node: 0,
            members: vec![0, 1, 3],
            quorum: None,
        },
        "the operator replaces node 2 with node 3",
    )
    .settle(matchmaking)
    .holds(prepare_to(3), "the Prepare of ballot 3.0 goes to node 3")
    .beat("Phase 1 runs again, and node 0 leads at ballot 3.0 with acceptors 0, 1 and 3.")
    .settle(any)
    .holds(leads(0), "node 0 leads again")
    .beat("The new set chooses bravo at slot 1.")
    .play(propose(0, "bravo"), "the client writes bravo")
    .settle(any)
    .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1");
    segments(&mut run);
    crash_and_retire(&mut run);
    run.finish("tour", "Change the acceptors", log_outcome)
}

/// The cut: a `Roll` is a command of the log, the leader does not wait for
/// it, and every node ends the segment at the same slot.
fn segments(run: &mut Recorder) {
    run.beat("The log grows for ever. So the leader can cut it into segments, each a range of slots.")
        .beat("A client asks node 0 to roll. The Roll is a command: node 0 proposes it at slot 2, like a write.")
        .play(Action::Roll { node: 0 }, "the client asks node 0 to roll")
        .holds(accept_for(2), "the Roll goes to slot 2")
        .beat("Node 0 does not wait for the Roll. It proposes charlie at slot 3 at once.")
        .play(propose(0, "charlie"), "the client writes charlie")
        .holds(accept_for(3), "charlie goes to slot 3")
        .beat(
            "Both are chosen. Each node applies the Roll at slot 2: segment 0 ends there, and \
             segment 1 starts at slot 3.",
        )
        .settle(any)
        .holds(chosen_at(3, "charlie"), "charlie is chosen at slot 3")
        .holds(
            |world| [0, 1, 3].iter().all(|&id| in_segment(id, 1, 3)(world)),
            "nodes 0, 1 and 3 are in segment 1 from slot 3",
        )
        .beat("Paxos chose one value at slot 2, so every node ends segment 0 at the same slot.");
}

/// The leader crashes, node 1 takes over with a Phase 1 over both sets and
/// rolls again, then the old set is forgotten and node 2 retires.
fn crash_and_retire(run: &mut Recorder) {
    run.beat("Now node 0 crashes.")
        .more("leader-crash")
        .play(crash(0), "node 0 crashes")
        .beat(
            "Node 1 registers ballot 4.1. The matchmakers report two sets: 0, 1, 2 for ballot 2.0 \
             and 0, 1, 3 for ballot 3.0.",
        )
        .play(elect(1), "node 1 starts an election")
        .settle_dropping(touches(0))
        .holds(leads(1), "node 1 leads")
        .beat(
            "Phase 1 needs a majority of each set. Node 2 is not in the new set, but its promise still \
             counts for the old one.",
        )
        .beat("Node 1 takes its segment from its own chosen log: segment 1, from slot 3.")
        .holds(in_segment(1, 1, 3), "node 1 is in segment 1 from slot 3")
        .beat("Node 1 rolls again, at slot 4. Segment 1 ends there, and segment 2 starts at slot 5.")
        .play(Action::Roll { node: 1 }, "the client asks node 1 to roll")
        .settle_dropping(touches(0))
        .holds(
            |world| [1, 3].iter().all(|&id| in_segment(id, 2, 5)(world)),
            "nodes 1 and 3 are in segment 2 from slot 5",
        );
    run.beat("Node 1 beats. Its ballot holds, so the matchmakers forget the sets below it.");
    let mut watermark = None;
    for _ in 0..12 {
        watermark = run
            .world()
            .log()
            .and_then(|log| log.gc_effective(NodeId(1)))
            .map(|(ballot, _)| ballot);
        if watermark.is_some() {
            break;
        }
        run.play(Action::Tick { node: 1 }, "the leader's clock ticks")
            .settle_dropping(touches(0));
    }
    let watermark = watermark.expect("a matchmaker quorum holds the floor");
    let retire_node_2 = || Action::Retire {
        node: 1,
        target: 2,
        gc_watermark: Some(BallotSpec {
            round: watermark.round,
            node: watermark.node.0,
        }),
    };
    run.beat("No set names node 2 now, so it retires.")
        .play(retire_node_2(), "the operator asks node 2 to retire")
        .settle_dropping(touches(0))
        .play(retire_node_2(), "the operator retires node 2")
        .settle_dropping(touches(0))
        .holds(
            |world| world.nodes.iter().any(|node| node.id == 2 && !node.alive),
            "node 2 is retired",
        )
        .beat("A new set is a new ballot, and a cut is a command of the log.")
        .more("break-the-rule");
}
