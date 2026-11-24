//! The two tour runs of Multi-Paxos (tour plan §5.3): chapter 3, "The log
//! and its leader", and chapter 4, "Reads". Each run tells its chapter in
//! one world with no restart of the world, and the narrator makes each fault
//! in that world. Chapter 3 has the world of the `gap` run and of the `gap`
//! exercise at its centre: bravo is lost at slot 1, charlie is chosen at
//! slot 2, and the next leader fills slot 1 with a `Noop`.

use super::{LEVEL, chosen_at, leads, no_leader, on_wire, only_on};
use crate::paxos::action::Action;
use crate::paxos::level::common::{crash, propose, start_election as elect};
use crate::paxos::trace::pages::{any, kind, log_outcome, touches};
use crate::paxos::trace::{Recorder, Scenario};
use crate::paxos::view::{RoleView, WorldView};

/// The client of the level.
const CLIENT: u64 = 7;

/// Node `id` is up, has applied `slot`, and has applied nothing above it.
fn applied_to(id: u64, slot: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world.nodes.iter().any(|node| {
            node.id == id
                && node.alive
                && node.applied.iter().any(|s| s.slot == slot)
                && node.applied.iter().all(|s| s.slot <= slot)
        })
    }
}

/// Node `id` is up and follows.
fn follows(id: u64) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .nodes
            .iter()
            .any(|node| node.id == id && node.alive && node.role == Some(RoleView::Follower))
    }
}

/// Node `id` has promised `ballot`.
fn promised(id: u64, ballot: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .nodes
            .iter()
            .any(|node| node.id == id && node.promised.as_deref() == Some(ballot))
    }
}

/// Node `id` holds `value` chosen at `slot`.
fn holds_chosen(id: u64, slot: u64, value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world.nodes.iter().any(|node| {
            node.id == id
                && node
                    .accepted
                    .iter()
                    .any(|record| record.slot == slot && record.chosen && record.value == value)
        })
    }
}

/// The reads of node `id`, as `served` flags.
fn reads_of(world: &WorldView, id: u64) -> Vec<bool> {
    world
        .clients
        .iter()
        .flat_map(|client| client.reads.iter())
        .filter(|read| read.node == id)
        .map(|read| read.served)
        .collect()
}

/// Chapter 3: one election for the whole log, a write with no Phase 1, the
/// heartbeat, a hole at slot 1, the leader's crash, the `Noop` of the next
/// leader, and the old leader that comes back, follows and catches up.
pub(super) fn the_log() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.tour("the-log", "The log and its leader");
    run.beat("Three nodes keep one log, a row of slots. Each node is an acceptor and a replica.")
        .beat(
            "Node 0 starts an election with ballot 1.0. One Prepare covers every slot of the log.",
        )
        .play(elect(0), "node 0 starts an election")
        .settle(kind("Prepare"))
        .beat("Two promises of three: node 0 now leads every slot with ballot 1.0.")
        .settle(any)
        .holds(leads(0), "node 0 leads")
        .beat("A client writes alpha. The leader skips Phase 1 and sends an Accept for slot 0.")
        .more("base")
        .play(propose(0, "alpha"), "the client writes alpha")
        .holds(
            |world| on_wire("Accept")(world) && !on_wire("Prepare")(world),
            "the leader sends an Accept and no Prepare",
        )
        .settle(kind("Accept"))
        .beat("Two votes of three choose alpha. A Commit tells the others.")
        .settle(any)
        .holds(chosen_at(0, "alpha"), "alpha is chosen at slot 0")
        .beat("The heartbeat of the leader also says how far the log is chosen.")
        .more("lost-commit")
        .play(Action::Tick { node: 0 }, "the leader's clock ticks")
        .holds(on_wire("Heartbeat"), "the leader sends a heartbeat")
        .settle(any)
        .beat("The client writes bravo and charlie. Many slots can be open at once.")
        .play(propose(0, "bravo"), "the client writes bravo")
        .play(propose(0, "charlie"), "the client writes charlie")
        .beat("Now the Accepts of slot 1 are lost. Only node 0 votes for bravo.")
        .drop_all(|message| message.kind == "Accept" && message.slot == Some(1))
        .beat(
            "Two votes choose charlie at slot 2, and the Commit to node 1 is lost. Slot 1 is \
             open, so no node applies slot 2.",
        )
        .settle(|message| !(message.kind == "Commit" && message.to == 1))
        .drop_all(|message| message.kind == "Commit" && message.to == 1)
        .holds(
            |world| !holds_chosen(1, 2, "charlie")(world),
            "node 1 does not know that charlie is chosen",
        )
        .holds(chosen_at(2, "charlie"), "charlie is chosen at slot 2")
        .holds(
            |world| (0..3).all(|id| applied_to(id, 0)(world)),
            "every node applied slot 0 and nothing above it",
        );
    after_the_crash(&mut run);
    run.finish("tour-log", "The log and its leader", log_outcome)
}

/// The second half of chapter 3: node 0 crashes, node 1 closes the hole,
/// and node 0 comes back, follows and catches up.
fn after_the_crash(run: &mut Recorder) {
    run
        .beat("Now node 0 crashes, with the only vote for bravo.")
        .more("gap")
        .play(crash(0), "node 0 crashes")
        .beat(
            "Node 1 starts an election with ballot 2.1. The promises report charlie at slot 2 \
             and no vote at slot 1.",
        )
        .play(elect(1), "node 1 starts an election")
        .drop_all(touches(0))
        .settle(|message| message.kind != "Accept")
        .holds(
            |world| {
                world.wire.iter().any(|m| {
                    m.kind == "Accept" && m.slot == Some(2) && m.ballot.as_deref() == Some("2.1")
                })
            },
            "node 1 proposes charlie again at ballot 2.1",
        )
        .beat("Node 1 proposes charlie again and fills slot 1 with a no-op. The log has no hole.")
        .settle_dropping(touches(0))
        .holds(chosen_at(1, "Noop"), "a no-op is chosen at slot 1")
        .holds(
            chosen_at(2, "charlie"),
            "charlie stays chosen at slot 2",
        )
        .holds(
            |world| applied_to(1, 2)(world) && applied_to(2, 2)(world),
            "nodes 1 and 2 applied up to slot 2",
        )
        .beat("bravo was never chosen, so it is lost. The client must write it again.")
        .holds(
            only_on(0, "bravo"),
            "bravo is on no disk but the dead leader's",
        )
        .beat("Node 0 restarts as a follower. It reads its promise of ballot 1.0 and its votes back from its disk.")
        .play(Action::Restart { node: 0 }, "node 0 restarts")
        .holds(follows(0), "node 0 boots as a follower")
        .holds(promised(0, "1.0"), "node 0 keeps its promise of ballot 1.0")
        .beat("The heartbeat of node 1 reaches node 0. Node 0 sees that it lacks chosen slots, and asks for them.")
        .more("lagging")
        .play(Action::Tick { node: 1 }, "the leader's clock ticks")
        .holds(on_wire("Heartbeat"), "node 1 sends a heartbeat")
        .settle(kind("Heartbeat"))
        .holds(
            |world| {
                world
                    .wire
                    .iter()
                    .any(|m| m.kind == "CatchUpRequest" && m.from == 0)
            },
            "node 0 asks for the slots that it lacks",
        )
        .beat("The answer raises the promise of node 0 to ballot 2.1, and node 0 catches up.")
        .settle(any)
        .holds(promised(0, "2.1"), "node 0 promised ballot 2.1")
        .holds(holds_chosen(0, 1, "Noop"), "node 0 learns the no-op at slot 1")
        .holds(applied_to(0, 2), "node 0 applied up to slot 2")
        .beat("One election covers every slot, and a new leader closes every hole.");
}

/// Chapter 4: a read that asks a quorum and not the leader, a read that
/// waits for an open slot, and a read with the leader down.
pub(super) fn reads() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.tour("reads", "Reads");
    run.beat("Node 0 leads, and alpha is chosen at slot 0.")
        .play(elect(0), "node 0 starts an election")
        .settle(any)
        .play(propose(0, "alpha"), "the client writes alpha")
        .settle(any)
        .beat("A client reads from node 1. Node 1 asks each acceptor how high it voted.")
        .more("read")
        .play(
            Action::QuorumRead {
                node: 1,
                client: Some(CLIENT),
            },
            "a client reads from node 1",
        )
        .holds(on_wire("PreRead"), "node 1 asks the nodes")
        .settle(kind("PreRead"))
        .beat("The answers say slot 0. Node 1 applied slot 0, so it serves the read at once.")
        .settle(any)
        .holds(
            |world| reads_of(world, 1) == [true],
            "node 1 served the read",
        )
        .beat("Node 0 answered as an acceptor, like node 2. No leader confirms a read, and paros has no leases.")
        .beat("The client writes bravo. Both followers vote, but the leader has not heard yet.")
        .play(propose(0, "bravo"), "the client writes bravo")
        .settle(kind("Accept"))
        .beat("A client reads from node 2. Both answers say slot 1.")
        .play(
            Action::QuorumRead {
                node: 2,
                client: Some(CLIENT),
            },
            "a client reads from node 2",
        )
        .settle(|message| message.kind == "PreRead" || message.kind == "PreReadAck")
        .beat("bravo may be chosen already, and node 2 has not applied slot 1. The read waits.")
        .more("read-waits")
        .holds(
            |world| reads_of(world, 2) == [false],
            "the read of node 2 waits",
        )
        .beat("The votes reach the leader, and its Commit tells node 2. Node 2 applies bravo and serves the read.")
        .settle(any)
        .holds(
            |world| reads_of(world, 2) == [true],
            "node 2 served the read",
        )
        .holds(applied_to(2, 1), "node 2 applied slot 1")
        .beat("Now node 0 crashes. No node leads.")
        .play(crash(0), "node 0 crashes")
        .holds(no_leader, "no node leads")
        .beat("A client reads from node 1 again. Nodes 1 and 2 are a majority, so the read needs no leader at all.")
        .play(
            Action::QuorumRead {
                node: 1,
                client: Some(CLIENT),
            },
            "a client reads from node 1",
        )
        .settle_dropping(touches(0))
        .holds(
            |world| reads_of(world, 1) == [true, true],
            "node 1 served its second read with node 0 down",
        )
        .beat("A read sees every write that a quorum may have chosen before the read began.");
    run.finish("tour-reads", "Reads", log_outcome)
}
