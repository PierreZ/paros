//! The Multi-Paxos chapter (#1150 (Multi-Paxos chapter)): the runs of
//! `/paxos/multi-paxos/`, each told as a story in beats.
//!
//! The story: one election covers the whole log, then a stable leader skips
//! Phase 1, then decide and apply, then lost messages, then a leader crash
//! and what the next leader finds, then the log's own commands (truncate)
//! and reads. The base run ends with a screen of choices; every other run
//! ends with a way back to it.

use super::{any, kind, log_outcome, touches};
use crate::paxos::action::{Action, Seam};
use crate::paxos::level::common::{crash, propose, start_election as elect};
use crate::paxos::trace::{Recorder, Scenario, Trace, trace};
use crate::paxos::view::{RoleView, WorldView};

/// The setup: three nodes, each an acceptor and a replica.
const LEVEL: &str = "act2/steady-state";

/// The beat of the base run that lists the other runs.
const TURN: usize = 7;

/// Some live node holds `value` chosen at `slot`.
fn chosen_at(slot: u64, value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world.nodes.iter().filter(|node| node.alive).any(|node| {
            node.accepted
                .iter()
                .any(|record| record.slot == slot && record.chosen && record.value == value)
        })
    }
}

/// No node but `except` holds a record of `value`, at any slot. A leader
/// writes its own vote before it sends, so the leader's disk is left out.
fn only_on(except: u64, value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .nodes
            .iter()
            .filter(|node| node.id != except)
            .all(|node| node.accepted.iter().all(|record| record.value != value))
    }
}

/// A message of kind `name` is on the wire.
fn on_wire(name: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| world.wire.iter().any(|message| message.kind == name)
}

/// Node `id` leads.
fn leads(id: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .nodes
            .iter()
            .any(|node| node.id == id && node.role == Some(RoleView::Leader))
    }
}

/// No node leads.
fn no_leader(world: &WorldView) -> bool {
    world
        .nodes
        .iter()
        .all(|node| node.role != Some(RoleView::Leader))
}

/// End a run: a screen that goes back to the list of runs.
fn back(run: &mut Recorder, says: &str) {
    run.beat(says).choice("Choose another fault", "base", TURN);
}

/// Open a run: node 0 is elected and alpha is chosen at slot 0, as in the
/// first story.
fn lead(run: &mut Recorder) {
    run.beat("Node 0 leads, and alpha is chosen at slot 0, as in the first story.")
        .play(elect(0), "node 0 starts an election")
        .settle(any)
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(any);
}

pub(super) fn multi_paxos() -> Trace {
    trace(
        "multi-paxos",
        LEVEL,
        vec![
            base(),
            leader_crash(),
            gap(),
            lost_commit(),
            lagging(),
            seam(Seam::BeforeSync),
            seam(Seam::AfterSyncBeforeSend),
            truncate(),
            read(),
            stale_nack(),
            stale_promise(),
            recovery_crash(),
            trimmed(),
            handoff(),
            handoff_lost(),
            read_waits(),
        ],
    )
}

/// The base run: one election, then two writes with no Phase 1.
fn base() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat(
        "Three nodes keep one log. Each node is an acceptor and a replica, and one node leads.",
    )
    .beat(
        "Node 0 starts an election with ballot 1.0. One Prepare covers every slot of the log, \
         from slot 0 up.",
    )
    .play(elect(0), "node 0 starts an election")
    .settle(kind("Prepare"))
    .beat("While it campaigns, node 0 also asks the others for the chosen slots it lacks.")
    .settle(kind("CatchUpRequest"))
    .beat("Two promises of three: node 0 now leads every slot with ballot 1.0.")
    .settle(any)
    .beat(
        "The client writes alpha. The leader already holds a ballot for every slot, so it \
         skips Phase 1 and sends an Accept for slot 0.",
    )
    .play(propose(0, "alpha"), "the client writes alpha")
    .settle(kind("Accept"))
    .beat("Two votes of three choose alpha. The leader tells the others with a Commit.")
    .settle(any)
    .beat("bravo goes to slot 1 with the same ballot: one round trip for each write.")
    .ask(
        "Node 0 leads with ballot 1.0, and alpha is chosen at slot 0. The client writes bravo. \
         What does the leader send first?",
        &[
            "A Prepare for slot 1",
            "An Accept for slot 1, at ballot 1.0",
            "A Commit for slot 0",
        ],
        1,
        "One Phase 1 covered every slot from slot 0 up. A stable leader skips Phase 1, so each \
         write costs one round trip.",
    )
    .play(propose(0, "bravo"), "the client writes bravo")
    .holds(
        |world| on_wire("Accept")(world) && !on_wire("Prepare")(world),
        "the leader sends an Accept and no Prepare",
    )
    .settle(any)
    .holds(chosen_at(1, "bravo"), "bravo is chosen at slot 1")
    .beat("Now choose what goes wrong.")
    .choice("The leader crashes during a write", "leader-crash", 0)
    .choice("A slot is lost with the leader", "gap", 0)
    .choice("The Commits are lost", "lost-commit", 0)
    .choice("A node falls behind", "lagging", 0)
    .choice(
        "The leader stops before its disk write",
        "seam-before-sync",
        0,
    )
    .choice(
        "The leader stops after its disk write",
        "seam-after-sync",
        0,
    )
    .choice("Truncate the journal", "truncate", 0)
    .choice("Read without the leader", "read", 0)
    .choice("An old leader writes", "stale-nack", 0)
    .choice("An old leader gets a Prepare", "stale-promise", 0)
    .choice("The new leader crashes too", "recovery-crash", 0)
    .choice("A node misses a truncation", "trimmed", 0)
    .choice("Hand the leadership on", "handoff", 0)
    .choice("The handoff is lost", "handoff-lost", 0)
    .choice("A read waits for a slot", "read-waits", 0);
    run.finish("base", "No fault", log_outcome)
}

/// The leader crashes after one follower voted for bravo. The next leader
/// finds that vote and proposes bravo again.
fn leader_crash() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat(
        "The client writes bravo. Only node 1 gets the Accept, and node 0 crashes before it \
         hears the vote.",
    )
    .play(propose(0, "bravo"), "the client writes bravo")
    .settle(|message| message.kind == "Accept" && message.to == 1)
    .drop_all(kind("Accept"))
    .play(crash(0), "node 0 crashes")
    .drop_all(touches(0));
    run.beat("Node 1 starts an election with ballot 2.1, and node 2 promises.")
        .play(elect(1), "node 1 starts an election")
        .drop_all(touches(0))
        .settle(|message| message.kind != "Accept");
    run.beat(
        "Node 1 holds a vote for bravo at slot 1 itself. A new leader proposes the vote it \
         finds, so bravo is proposed again at ballot 2.1 and chosen.",
    )
    .ask(
        "Only node 1 voted for bravo at slot 1, then the leader died. Node 1 is elected. What \
         does it put in slot 1?",
        &["bravo", "A no-op", "Nothing: slot 1 stays empty"],
        0,
        "Phase 1 reports every vote. One vote may be part of a choice that the new leader \
         cannot see, so it proposes bravo again.",
    )
    .settle_dropping(touches(0))
    .holds(
        chosen_at(1, "bravo"),
        "bravo is chosen at slot 1 at the new ballot",
    );
    back(
        &mut run,
        "A vote that a quorum may have chosen is never lost: the next Phase 1 finds it.",
    );
    run.finish(
        "leader-crash",
        "What if the leader crashes during a write?",
        log_outcome,
    )
}

/// Slot 1 is lost with the leader while slot 2 is chosen. The next leader
/// fills slot 1 with a no-op.
fn gap() -> Scenario {
    let mut run = Recorder::new("act2/the-permanent-gap");
    lead(&mut run);
    run.beat(
        "The client writes bravo and charlie. Many slots can be open at once. The Accepts for \
         slot 1 are lost, and charlie is chosen at slot 2.",
    )
    .play(propose(0, "bravo"), "the client writes bravo")
    .play(propose(0, "charlie"), "the client writes charlie")
    .drop_all(|message| message.kind == "Accept" && message.slot == Some(1))
    .settle(any);
    run.beat("Node 0 crashes. No node holds a vote for slot 1.")
        .play(crash(0), "node 0 crashes");
    run.beat(
        "Node 1 is elected. No promise reports a vote for slot 1, but slot 2 is chosen above \
         it.",
    )
    .play(elect(1), "node 1 starts an election")
    .drop_all(touches(0))
    .settle(|message| message.kind != "Accept");
    run.beat(
        "Node 1 fills slot 1 with a no-op. Without it, no node could apply past slot 1, \
         because the log applies in order.",
    )
    .ask(
        "Slot 2 holds charlie, chosen. No promise reports a vote for slot 1. What does the new \
         leader put in slot 1?",
        &[
            "bravo, the value that the client sent",
            "A no-op",
            "Nothing: it waits for the client",
        ],
        1,
        "The new leader never heard of bravo. A no-op closes the hole, so every node can apply \
         up to charlie.",
    )
    .settle_dropping(touches(0))
    .holds(chosen_at(1, "Noop"), "a no-op is chosen at slot 1")
    .holds(
        only_on(0, "bravo"),
        "bravo is on no disk but the dead leader's",
    );
    back(
        &mut run,
        "bravo is lost: it was never chosen, so the client must write it again.",
    );
    run.finish(
        "gap",
        "What if a slot is lost with the leader?",
        log_outcome,
    )
}

/// The Commits are lost. The leader never sends them again: its next
/// heartbeat carries how far the log is chosen, and the followers pull.
fn lost_commit() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Node 0 leads, as in the first story.")
        .play(elect(0), "node 0 starts an election")
        .settle(any);
    run.beat("The client writes alpha, and two votes choose it. Every Commit is lost on the way.")
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"));
    run.beat(
        "The leader does not send a Commit again. Its next heartbeat says how far the log is \
         chosen.",
    )
    .ask(
        "Every Commit for alpha is lost. How do the followers learn that alpha is chosen?",
        &[
            "The leader sends the Commit again",
            "The next heartbeat says how far the log is chosen",
            "They never learn it",
        ],
        1,
        "The heartbeat carries the commit index. A follower that sees a gap asks for the slots \
         that it lacks.",
    )
    .play(Action::Tick { node: 0 }, "the leader's clock ticks")
    .holds(
        |world| on_wire("Heartbeat")(world) && !on_wire("Commit")(world),
        "the leader sends a heartbeat and no Commit",
    )
    .settle(kind("Heartbeat"));
    run.beat(
        "Each follower sees that it lacks a chosen slot and asks the leader for it. An answer \
         to a heartbeat also tells the leader that the follower is alive.",
    )
    .settle(any);
    back(
        &mut run,
        "A lost Commit costs time, never a value: the vote is on the disks, and the heartbeat \
         closes the gap.",
    );
    run.finish("lost-commit", "What if the Commits are lost?", log_outcome)
}

/// Node 2 is down while the others write. When it is back, the leader's
/// heartbeat tells it what is chosen, and it asks for the slots it lacks.
fn lagging() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Node 0 leads, as in the first story.")
        .play(elect(0), "node 0 starts an election")
        .settle(any);
    run.beat("Node 2 crashes. Nodes 0 and 1 still make a majority, and choose alpha and bravo.")
        .play(crash(2), "node 2 crashes")
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle_dropping(touches(2))
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(2));
    run.beat("Node 2 restarts. Its disk has nothing for slots 0 and 1.")
        .play(Action::Restart { node: 2 }, "node 2 restarts");
    run.beat("The leader's heartbeat says how far the log is chosen. Node 2 is behind.")
        .play(Action::Tick { node: 0 }, "the leader's clock ticks")
        .settle(|message| message.kind != "CatchUpRequest");
    run.beat("Node 2 asks the leader for the chosen slots it lacks, and the leader sends them.")
        .settle(any);
    back(
        &mut run,
        "A node that falls behind catches up from the chosen slots. It never votes again on a \
         slot that is chosen.",
    );
    run.finish("lagging", "What if a node falls behind?", log_outcome)
}

/// The leader is armed to stop at a seam of its next disk write, and the
/// client writes bravo. Before the sync, nothing is left; after the sync,
/// its own vote is left on its disk, and nobody else knows of it.
fn seam(seam: Seam) -> Scenario {
    let before = seam == Seam::BeforeSync;
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat(if before {
        "The client writes bravo. Node 0 stops before its disk write: nothing is written, and \
         nothing is sent."
    } else {
        "The client writes bravo. Node 0 writes its own vote for bravo to disk, then stops \
         before it sends the Accepts."
    })
    .play(
        Action::CrashAt { node: 0, seam },
        if before {
            "node 0 will stop before its next disk write"
        } else {
            "node 0 will stop after its next disk write"
        },
    )
    .play(propose(0, "bravo"), "the client writes bravo")
    .drop_all(touches(0));
    run.beat("Node 1 is elected. Nodes 1 and 2 never heard of bravo, so no promise reports it.")
        .ask(
            if before {
                "Node 0 stopped before its disk write. Nodes 1 and 2 elect node 1. Can bravo \
                 still be chosen at slot 1?"
            } else {
                "Node 0 wrote its vote for bravo, then stopped before it sent anything. Nodes 1 \
                 and 2 elect node 1. Can bravo still be chosen at slot 1?"
            },
            &[
                "Yes: the leader accepted it",
                "No: no promise of the new quorum reports it",
            ],
            1,
            "The new quorum is nodes 1 and 2, and neither heard of bravo. A vote on one disk, or \
             no vote at all, is not a choice.",
        )
        .play(elect(1), "node 1 starts an election")
        .settle_dropping(touches(0));
    run.beat("The client writes charlie, and node 1 puts it in slot 1.")
        .play(propose(1, "charlie"), "the client writes charlie")
        .settle_dropping(touches(0))
        .holds(chosen_at(1, "charlie"), "charlie is chosen at slot 1");
    run.beat(if before {
        "Node 0 restarts with the disk it had before bravo. The heartbeat brings it the slots \
         it lacks."
    } else {
        "Node 0 restarts with a vote for bravo at slot 1. One vote of three chose nothing, and \
         the heartbeat brings the chosen charlie."
    })
    .play(Action::Restart { node: 0 }, "node 0 restarts")
    .play(Action::Tick { node: 1 }, "the leader's clock ticks")
    .settle(any);
    back(
        &mut run,
        if before {
            "A stop before the sync leaves no trace. The client never got an answer, so it \
             writes bravo again."
        } else {
            "A vote on one disk is not a choice. Only a quorum of votes chooses a value."
        },
    );
    if before {
        run.finish(
            "seam-before-sync",
            "What if the leader stops before its disk write?",
            log_outcome,
        )
    } else {
        run.finish(
            "seam-after-sync",
            "What if the leader stops after its disk write?",
            log_outcome,
        )
    }
}

/// Truncate is a command in the log: the leader proposes it, the nodes
/// apply it, and the slots below it go.
fn truncate() -> Scenario {
    let mut run = Recorder::new("act3/truncate-by-consensus");
    lead(&mut run);
    run.beat("bravo is chosen at slot 1.")
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle(any);
    run.beat(
        "The client truncates the journal. A truncation is a command in the log too: it goes \
         to slot 2 and needs a quorum of votes.",
    )
    .play(
        Action::Compact { node: 0, up_to: 8 },
        "the client truncates the journal",
    )
    .settle(|message| message.kind != "Commit");
    run.beat(
        "Each node drops the slots below the truncation when it applies slot 2. No node takes \
         a snapshot.",
    )
    .settle(any);
    back(
        &mut run,
        "A truncation is decided like a write, so every node drops the same slots.",
    );
    run.finish("truncate", "Truncate the journal", log_outcome)
}

/// A leaderless read: node 1 asks a quorum for the highest slot each one
/// voted in, and answers once it applied that far.
fn read() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat(
        "A client reads from node 1, not from the leader. Node 1 asks each node for the \
         highest slot that it voted in.",
    )
    .ask(
        "A client reads from node 1, a follower. What does node 1 do first?",
        &[
            "It sends the read to the leader",
            "It answers from its own log at once",
            "It asks a quorum how far they voted",
        ],
        2,
        "A read never goes through the leader, and a node alone can be behind. A quorum's \
         answers say how far the node must apply before it answers.",
    )
    .play(
        Action::QuorumRead {
            node: 1,
            client: Some(7),
        },
        "a client reads from node 1",
    )
    .holds(on_wire("PreRead"), "node 1 asks the nodes")
    .settle(kind("PreRead"));
    run.beat(
        "A quorum of answers tells node 1 how far it must apply. It applied slot 0, so it \
         answers the read at once.",
    )
    .settle(any);
    back(
        &mut run,
        "A read never goes through the leader, and paros has no leases.",
    );
    run.finish("read", "Read without the leader", log_outcome)
}

/// Open a run where node 0 is cut off after alpha, and nodes 1 and 2 elect
/// node 1.
fn cut_off(run: &mut Recorder) {
    lead(run);
    run.beat("Node 0 is cut off. Nodes 1 and 2 elect node 1 with ballot 2.1.")
        .play(elect(1), "node 1 starts an election")
        .settle_dropping(touches(0));
}

/// An old leader that missed an election writes: each acceptor refuses
/// with a Nack, and the old leader steps down.
fn stale_nack() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    cut_off(&mut run);
    run.beat(
        "Node 0 is back on the network. It still thinks that it leads, and the client writes \
         bravo to it with ballot 1.0.",
    )
    .play(propose(0, "bravo"), "the client writes bravo to node 0")
    .settle(kind("Accept"));
    run.beat(
        "Both acceptors promised ballot 2.1, so each one refuses with a Nack. Node 0 learns \
         that it does not lead, and bravo is not chosen.",
    )
    .ask(
        "Node 0 still thinks that it leads with ballot 1.0. Nodes 1 and 2 promised ballot 2.1. \
         What happens to the Accepts for bravo?",
        &[
            "Nodes 1 and 2 vote, and bravo is chosen",
            "Nodes 1 and 2 refuse, and node 0 stops leading",
        ],
        1,
        "A promise refuses every lower ballot. The Nack tells node 0 that a higher ballot \
         exists.",
    )
    .settle(any)
    .holds(only_on(0, "bravo"), "nodes 1 and 2 did not vote for bravo")
    .holds(|world| !leads(0)(world), "node 0 stops leading");
    back(
        &mut run,
        "A Nack carries no ballot. The old leader steps down, and a value from the wire never \
         picks its next ballot.",
    );
    run.finish("stale-nack", "What if an old leader writes?", log_outcome)
}

/// A leader gets a Prepare for a higher ballot: its own promise passes its
/// ballot, and it steps down.
fn stale_promise() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat(
        "Node 1 starts an election with ballot 2.1, and node 0 gets the Prepare. Node 0 is an \
         acceptor too, so it promises 2.1.",
    )
    .play(elect(1), "node 1 starts an election")
    .settle(kind("Prepare"));
    run.beat(
        "Its own promise is now above its ballot 1.0, so node 0 stops leading. Node 1 gets \
         its promises and leads.",
    )
    .settle(any);
    back(
        &mut run,
        "A leader is also an acceptor. When it promises a higher ballot, it cannot use its \
         old one.",
    );
    run.finish(
        "stale-promise",
        "What if an old leader gets a Prepare?",
        log_outcome,
    )
}

/// The new leader crashes in the middle of its recovery, and a third
/// leader recovers both ballots.
fn recovery_crash() -> Scenario {
    let five = |run: &mut Recorder| {
        run.beat("Five nodes. Node 0 leads, and alpha is chosen at slot 0.")
            .play(elect(0), "node 0 starts an election")
            .settle(any)
            .play(propose(0, "alpha"), "the client writes alpha")
            .settle(any);
    };
    let mut run = Recorder::new("act2/the-read-that-lies");
    five(&mut run);
    run.beat("The client writes bravo. Only node 1 votes for it, then node 0 crashes.")
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle(|message| message.kind == "Accept" && message.to == 1)
        .drop_all(touches(0))
        .play(crash(0), "node 0 crashes");
    run.beat(
        "Node 1 is elected with ballot 2.1. It holds the vote for bravo, so its recovery \
         proposes bravo again at slot 1.",
    )
    .play(elect(1), "node 1 starts an election")
    .drop_all(touches(0))
    .settle(|message| message.kind != "Accept");
    run.beat("Only node 2 gets that Accept before node 1 crashes. The recovery is half done.")
        .settle(|message| message.kind == "Accept" && message.to == 2)
        .play(crash(1), "node 1 crashes")
        .drop_all(|message| touches(0)(message) || touches(1)(message));
    run.beat(
        "Node 2 is elected with ballot 3.2. Its own vote for bravo at 2.1 is the highest vote \
         it finds, so it proposes bravo.",
    )
    .play(elect(2), "node 2 starts an election")
    .drop_all(|message| touches(0)(message) || touches(1)(message))
    .settle(|message| message.kind != "Accept");
    run.beat("Nodes 2, 3 and 4 vote, and bravo is chosen at slot 1.")
        .settle_dropping(|message| touches(0)(message) || touches(1)(message));
    back(
        &mut run,
        "A recovery is ordinary Paxos. A leader that crashes in the middle leaves votes, and \
         the next Phase 1 finds them.",
    );
    run.finish(
        "recovery-crash",
        "What if the new leader crashes too?",
        log_outcome,
    )
}

/// A node misses a truncation. The leader does not send it the slots
/// below the floor: it sends a jump (`TrimmedTo`).
fn trimmed() -> Scenario {
    let mut run = Recorder::new("act3/truncate-by-consensus");
    run.beat("Node 0 leads. Node 2 crashes.")
        .play(elect(0), "node 0 starts an election")
        .settle(any)
        .play(crash(2), "node 2 crashes");
    run.beat("Nodes 0 and 1 choose alpha, bravo, then a truncation at slot 2.")
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle_dropping(touches(2))
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle_dropping(touches(2))
        .play(
            Action::Compact { node: 0, up_to: 8 },
            "the client truncates the journal",
        )
        .settle_dropping(touches(2));
    run.beat(
        "Node 2 restarts, and the heartbeat tells it that it is behind. It asks for the \
         slots it lacks.",
    )
    .play(Action::Restart { node: 2 }, "node 2 restarts")
    .play(Action::Tick { node: 0 }, "the leader's clock ticks")
    .settle(|message| message.kind != "TrimmedTo");
    run.beat(
        "Those slots are gone. The leader sends a jump: the floor and the state of the \
         journal, with no bytes and no ballot. Node 2 keeps its promise.",
    )
    .settle(any);
    back(
        &mut run,
        "paros takes no snapshot. A node far behind jumps to the floor, and the bytes below it \
         are gone for good.",
    );
    run.finish(
        "trimmed",
        "What if a node misses a truncation?",
        log_outcome,
    )
}

/// The leader hands its leadership to node 1 with the same ballot and no
/// Phase 1.
fn handoff() -> Scenario {
    let mut run = Recorder::new("act4/the-handoff");
    lead(&mut run);
    run.beat(
        "Node 0 hands the leadership to node 1. It stops leading first, and the Relinquish \
         names node 1 and carries the ballot.",
    )
    .play(
        Action::Relinquish { node: 0, to: 1 },
        "node 0 hands the leadership to node 1",
    )
    .settle(any);
    run.beat("Node 1 leads with the same ballot. It writes bravo with no Phase 1.")
        .play(propose(1, "bravo"), "the client writes bravo")
        .settle(any);
    back(
        &mut run,
        "Only the node that won a ballot can hand it on, and only once.",
    );
    run.finish("handoff", "Move the leader with no election", log_outcome)
}

/// The Relinquish is lost: no node leads until an election.
fn handoff_lost() -> Scenario {
    let mut run = Recorder::new("act4/the-handoff");
    lead(&mut run);
    run.beat("Node 0 stops leading and sends the Relinquish to node 1. It is lost.")
        .play(
            Action::Relinquish { node: 0, to: 1 },
            "node 0 hands the leadership to node 1",
        )
        .drop_all(kind("Relinquish"));
    run.beat("No node leads now. The clock of node 1 runs until its election timeout.")
        .ask(
            "The Relinquish is lost. Node 0 already stopped leading. What happens next?",
            &[
                "Node 0 leads again",
                "No node leads until an election timeout",
                "Node 1 leads with the old ballot",
            ],
            1,
            "Node 0 gave up its ballot before it sent the Relinquish, so it cannot take it \
             back. A lost handoff costs one election, never a value.",
        )
        .holds(no_leader, "no node leads");
    for _ in 0..5 {
        run.play(Action::Tick { node: 1 }, "the clock of node 1 ticks")
            .settle(|message| message.kind != "Promise");
    }
    run.beat("Node 1 gets its promises and leads with a new ballot.")
        .settle(any);
    back(
        &mut run,
        "A failed handoff costs time, never a value: the next election takes over.",
    );
    run.finish("handoff-lost", "What if the handoff is lost?", log_outcome)
}

/// A read waits: a quorum says that slot 1 has votes, and node 1 answers
/// only when it applied slot 1.
fn read_waits() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    lead(&mut run);
    run.beat("The client writes bravo, and both followers vote. The leader has not heard yet.")
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle(kind("Accept"));
    run.beat(
        "A client reads from node 1. The answers say that a node voted up to slot 1, and node \
         1 has not applied slot 1, so the read waits.",
    )
    .ask(
        "Both followers voted for bravo at slot 1, but no Commit arrived yet. A client reads \
         from node 1. What does node 1 do?",
        &[
            "It answers now, without bravo",
            "It waits until it applies slot 1",
            "It refuses the read",
        ],
        1,
        "bravo may be chosen already. A read that skips it could miss a write that finished \
         before the read began.",
    )
    .play(
        Action::QuorumRead {
            node: 1,
            client: Some(7),
        },
        "a client reads from node 1",
    )
    .settle(|message| message.kind == "PreRead" || message.kind == "PreReadAck");
    run.beat("The votes reach the leader, and its Commit tells node 1. Node 1 answers the read.")
        .settle(any);
    back(
        &mut run,
        "A read sees every write that a quorum may have chosen before the read began.",
    );
    run.finish(
        "read-waits",
        "What if a read meets an open slot?",
        log_outcome,
    )
}
