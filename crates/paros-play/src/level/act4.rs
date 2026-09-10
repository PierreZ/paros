//! Act IV — the parts of paros the book does not write down.
//!
//! Act I to Act III build one shape: a majority answers Phase 1, a majority
//! votes in Phase 2, one leader talks to everybody, and a disk either survives
//! a crash or the node does not come back. Act IV takes each of those apart.
//!
//! The first four levels change **who counts**. A flexible split makes the two
//! phases different sizes. A grid makes them different *shapes*. A quorum read
//! turns the Phase-1 side into a read path that needs no leader. A cooperative
//! handoff moves a leadership without an election. The last two levels change
//! **what a disk may lose**: one record, which the cluster repairs in place,
//! and the whole disk, which it does not.
//!
//! Every reference solution here is recorded, not written: a private `Script`
//! plays the level and answers every prompt with the answer `paros-core`
//! itself gives.

use paros_core::{Command, Config, NodeId, QuorumSystem, Slot};

use crate::action::{Action, ActionKind, Phase};
use crate::auto::AutomationFlag;
use crate::level::script::{Script, kind, kind_at};
use crate::level::{GoalStatus, Level, WorldKind};
use crate::view::{MessageView, show_command};
use crate::world::decree::DecreeWorld;
use crate::world::{Disk, World};

/// Act IV's levels, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &FLEXIBLE_QUORUMS,
        &THE_GRID,
        &QUORUM_READS,
        &THE_HANDOFF,
        // part two: matchmakers — levels 24 to 27 (`act4/matchmaking`,
        // `act4/reconfigure`, `act4/garbage-collection`,
        // `act4/matchmaker-generations`) are appended here, in play order,
        // together with the matchmaker plane they need. The two levels below
        // stay last: they are numbered 28 and 29 in the plan.
        &FAULTY_RECORDS,
        &THE_WIPED_NODE,
    ]
}

/// The client every Act IV level gives the player.
const CLIENT: u64 = 7;

/// The election timeout every Act IV node starts with, in ticks.
const TIMEOUT: u64 = 5;

/// The four acceptors of the flexible-quorum level, and the two proposers that
/// compete over them.
const FOUR_ACCEPTORS: &[u64] = &[1, 2, 3, 4];

/// The acceptor grid every grid level lays out: two rows of three.
const GRID: QuorumSystem = QuorumSystem::Grid { rows: 2, cols: 3 };

/// Every prompt-governing flag. A level removes exactly the one it teaches.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::SnapshotPromise,
    AutomationFlag::AckWrite,
    AutomationFlag::GridColumn,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::RepairVerdict,
    AutomationFlag::WipedRejoin,
];

/// Every role but the column a grid slot goes to.
const NO_GRID_COLUMN: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::SnapshotPromise,
    AutomationFlag::AckWrite,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::RepairVerdict,
    AutomationFlag::WipedRejoin,
];

/// Every role but serving a leaderless read.
const NO_QUORUM_READ_SERVE: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::SnapshotPromise,
    AutomationFlag::AckWrite,
    AutomationFlag::GridColumn,
    AutomationFlag::RepairVerdict,
    AutomationFlag::WipedRejoin,
];

/// Every role but settling a damaged slot.
const NO_REPAIR_VERDICT: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::SnapshotPromise,
    AutomationFlag::AckWrite,
    AutomationFlag::GridColumn,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::WipedRejoin,
];

/// Every role but the answer a wiped node's boot gets.
const NO_WIPED_REJOIN: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::SnapshotPromise,
    AutomationFlag::AckWrite,
    AutomationFlag::GridColumn,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::RepairVerdict,
];

/// The convenience toggles the log-world levels offer.
const TOGGLES: &[AutomationFlag] = &[
    AutomationFlag::DeliverReplies,
    AutomationFlag::DeliverHeartbeats,
];

/// The one toggle a level that pins beats off may still offer.
const REPLIES_ONLY: &[AutomationFlag] = &[AutomationFlag::DeliverReplies];

// ---- worlds -----------------------------------------------------------------

fn peers(size: u64) -> Vec<NodeId> {
    (0..size).map(NodeId).collect()
}

fn config(id: NodeId, size: u64, system: QuorumSystem) -> Config {
    Config {
        id,
        peers: peers(size),
        quorum_system: system,
        ..Config::default()
    }
}

/// A cluster of `size` fresh nodes under `system`, with one client.
fn fresh(size: u64, system: QuorumSystem) -> WorldKind {
    let disks = peers(size)
        .into_iter()
        .map(|id| Disk::new(config(id, size, system)))
        .collect();
    WorldKind::Log(Box::new(World::from_disks(disks, &[CLIENT], TIMEOUT)))
}

// ---- reading the world for a goal -------------------------------------------

fn log_world(world: &WorldKind) -> Option<&World> {
    world.log()
}

/// What a node's application has executed, in order.
fn applied(world: &WorldKind, node: u64) -> Vec<String> {
    log_world(world)
        .and_then(|world| world.disk(NodeId(node)))
        .map(|disk| {
            disk.applied()
                .iter()
                .map(|(_, command)| show_command(command))
                .collect()
        })
        .unwrap_or_default()
}

/// The text inside a client command, for a goal that has to name a value.
fn text(command: &Command) -> String {
    command
        .user()
        .map(|entry| String::from_utf8_lossy(&entry.value.0).into_owned())
        .unwrap_or_default()
}

/// The value the single-decree world holds, if it holds one.
fn chosen_text(world: &WorldKind) -> Option<String> {
    world
        .decree()
        .and_then(|decree| decree.chosen().map(|(_, command)| text(command)))
}

// ---- action shorthands ------------------------------------------------------

fn open(proposer: u64, value: &str) -> Action {
    Action::OpenBallot {
        proposer,
        value: value.to_string(),
    }
}

fn reach(phase: Phase, nodes: &[u64]) -> Action {
    Action::SetReach {
        phase,
        nodes: nodes.to_vec(),
    }
}

fn start_election(node: u64) -> Action {
    Action::StartElection { node }
}

fn propose(node: u64, value: &str) -> Action {
    Action::Propose {
        node,
        client: CLIENT,
        value: value.to_string(),
        column: None,
    }
}

fn quorum_read(node: u64) -> Action {
    Action::QuorumRead {
        node,
        client: Some(CLIENT),
    }
}

fn relinquish(node: u64, to: u64) -> Action {
    Action::Relinquish { node, to }
}

fn crash(node: u64) -> Action {
    Action::Crash { node }
}

fn restart(node: u64) -> Action {
    Action::Restart { node }
}

fn tick(node: u64) -> Action {
    Action::Tick { node }
}

fn resend(node: u64) -> Action {
    Action::ResendPending { node }
}

/// Everything addressed to `node`.
fn to(node: u64) -> impl Fn(&MessageView) -> bool {
    move |message| message.to == node
}

// ---- 20. flexible quorums ---------------------------------------------------

const FLEXIBLE_ACTIONS: &[ActionKind] = &[
    ActionKind::OpenBallot,
    ActionKind::SetReach,
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::SetAutomation,
];

/// `act4/flexible-quorums`.
pub static FLEXIBLE_QUORUMS: Level = Level {
    id: "act4/flexible-quorums",
    act: 4,
    title: "Flexible quorums",
    briefing: "\
Act I told you that any two majorities share an acceptor, and that this one \
fact carries the whole safety argument. Read the argument again and you find \
something smaller is enough. Phase 1 must learn about every value that Phase 2 \
may have chosen. Nothing needs two Phase-1 quorums to share an acceptor. \
Nothing needs two Phase-2 quorums to share one either. Only the two phases \
must meet.

Write that as arithmetic and you get `q1 + q2 > n`. Four acceptors give you a \
choice the majority hides: this level runs `q1 = 3` and `q2 = 2`. A value is \
now chosen by **two** acceptors, and every later election must collect **three** \
promises. The trade is the point. The steady state gets cheaper and more \
tolerant, because a write needs two answers instead of three. The next \
election gets dearer, because it needs three answers instead of three of four \
being enough at two.

You control the reach of each phase: which acceptors a `Prepare` gets to, and \
which acceptors an `Accept` gets to. Get `\"alpha\"` chosen with two acceptors. \
Then run a second ballot with three, and watch it come back with the same \
value. Try to pick three acceptors that miss both voters. Three plus two is \
five, and there are only four acceptors, so no such set exists.",
    field_guide: "play.html",
    symbols: &[
        "QuorumSystem::Flexible",
        "QuorumSystem::cross_intersects",
        "AcceptorConfig::has_phase1_quorum",
        "AcceptorConfig::has_phase2_quorum",
        "docs/references/papers — Howard, Malkhi, Spiegelman, Flexible Paxos",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: REPLIES_ONLY,
    unlocks: &[],
    allowed_actions: FLEXIBLE_ACTIONS,
    setup: || {
        WorldKind::Decree(Box::new(DecreeWorld::with_system(
            FOUR_ACCEPTORS,
            &[5, 8],
            &std::collections::BTreeMap::new(),
            None,
            QuorumSystem::Flexible { q1: 3, q2: 2 },
        )))
    },
    goal: |world| {
        let Some(decree) = world.decree() else {
            return GoalStatus::Open("This level runs in the single-decree world.".to_string());
        };
        let Some(chosen) = chosen_text(world) else {
            return GoalStatus::Open(
                "Get a value chosen. Two acceptors are a Phase-2 quorum here.".to_string(),
            );
        };
        let first = decree.completed_phase1().first().map(|c| c.ballot);
        let later = decree
            .completed_phase1()
            .iter()
            .find(|campaign| Some(campaign.ballot) > first);
        match later {
            Some(campaign) if text(&campaign.proposed) == chosen => GoalStatus::Reached(format!(
                "Two acceptors chose {chosen:?}, and three had to be asked to find it again. \
                 Ballot {}.{} reached {:?}, and every set of three acceptors here contains one \
                 of the two that voted. That is the whole of `q1 + q2 > n`.",
                campaign.ballot.round,
                campaign.ballot.node.0,
                campaign.reach.iter().map(|n| n.0).collect::<Vec<_>>()
            )),
            Some(campaign) => GoalStatus::Failed(format!(
                "A campaign proposed {:?} over the chosen {chosen:?}.",
                text(&campaign.proposed)
            )),
            None => GoalStatus::Open(format!(
                "{chosen:?} is chosen. Now run a second ballot and see what its promises report."
            )),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Set the Phase-1 reach to three acceptors before you open the second ballot. Two \
             acceptors are not enough to complete Phase 1 here, whichever two you pick."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act4/flexible-quorums");
        // Phase 1 asks three acceptors; Phase 2 asks two of them.
        script.play(reach(Phase::One, &[1, 2, 3]));
        script.play(reach(Phase::Two, &[1, 2]));
        script.play(open(5, "alpha")).settle_all();
        // A second ballot, through three acceptors that include neither voter
        // — which is impossible, so it includes one of them.
        script.play(reach(Phase::One, &[2, 3, 4]));
        script.play(open(8, "bravo")).settle_all();
        script.finish()
    },
};

// ---- 21. the grid -----------------------------------------------------------

const GRID_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/the-grid`.
pub static THE_GRID: Level = Level {
    id: "act4/the-grid",
    act: 4,
    title: "The grid",
    briefing: "\
A quorum does not have to be a count at all. Lay six acceptors out in two rows \
of three. Call any whole **row** a Phase-1 quorum and any whole **column** a \
Phase-2 quorum. A row and a column of one grid always cross in exactly one \
cell, so the two phases meet by geometry. No arithmetic is involved, and \
nothing about the safety argument changes.

What this buys is throughput. Each slot goes to **one column**, so each \
acceptor sees a third of the writes and the acceptor tier scales with the \
number of columns. What it costs is that failure now depends on *which* \
acceptor is down, not how many. One dead acceptor leaves its column short, and \
that column's slots wait. The column a slot uses is `slot` modulo the number \
of columns. That is a rule, not a message: nothing on the wire carries the \
column, so a restarted leader and a successor both work out the same one.

Get two commands chosen. You choose the column for each, and the grid marks \
your answer. Then watch the rule do its own work: send a copy of slot 0's \
`Accept` to a node outside slot 0's column. That node is a member of the \
configuration and it votes, honestly and safely. Its vote counts for nothing, \
because a Phase-2 quorum here is a whole column and it is in a different one.",
    field_guide: "play.html",
    symbols: &[
        "QuorumSystem::Grid",
        "QuorumSystem::column_of",
        "AcceptorConfig::has_phase2_quorum_in",
        "ColocatedNode::propose_in",
        "docs/references/papers — Whittaker et al., Compartmentalized Paxos §3.2",
    ],
    automation_on: NO_GRID_COLUMN,
    pinned_off: &[AutomationFlag::GridColumn],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::GridColumn],
    allowed_actions: GRID_ACTIONS,
    setup: || fresh(6, GRID),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let Some(leader) = log.leader() else {
            return GoalStatus::Open("Elect a leader. A whole row answers Phase 1.".to_string());
        };
        let Some(node) = log.node(leader) else {
            return GoalStatus::Open("The leader is not running.".to_string());
        };
        let columns: Vec<usize> = [Slot(0), Slot(1)]
            .iter()
            .filter_map(|slot| node.acceptors().column_of(*slot))
            .collect();
        let everywhere = (0..6).all(|id| applied(world, id).len() >= 2);
        let stray = log
            .node(NodeId(4))
            .is_some_and(|node| node.acceptor().record(Slot(0)).is_some())
            && node.acceptors().column_of(Slot(0)).is_some_and(|column| {
                !node
                    .acceptors()
                    .is_phase2_addressee(NodeId(4), Some(column))
            });
        match (everywhere, columns.as_slice(), stray) {
            (true, [first, second], true) if first != second => GoalStatus::Reached(format!(
                "Slot 0 was decided by column {first} and slot 1 by column {second}, and all six \
                 nodes applied both. Node 4 holds slot 0's value too, and its vote for that slot \
                 counted for nothing: it is not in column {first}."
            )),
            (true, [first, second], false) if first != second => GoalStatus::Open(format!(
                "Both slots are chosen, on columns {first} and {second}. Now send a copy of slot \
                 0's Accept to a node outside column {first}, and watch the tally."
            )),
            _ => GoalStatus::Open(
                "Get two commands chosen and applied on all six nodes.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "The column is the slot number modulo the number of columns. This grid has three \
             columns."
                .to_string(),
        ),
        _ => Some(
            "Slot 0 goes to column 0, slot 1 to column 1, slot 2 to column 2, slot 3 back to \
             column 0. Any column is safe; only this one is the column everybody else derives."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act4/the-grid");
        script.play(start_election(0)).settle_all();
        // Slot 0: the player names its column, then the column votes. The
        // copy that goes to node 4 is the level's own demonstration.
        script.play(propose(0, "alpha")).answer_all();
        let stray = script
            .wire()
            .iter()
            .find(|message| message.kind == "Accept" && message.slot == Some(0))
            .map(|message| message.id)
            .expect("slot 0's Accept is in flight");
        script.play(Action::Duplicate {
            id: stray,
            to: Some(4),
        });
        script.settle(|message| message.kind == "Accept" && message.to == 4);
        script.settle(|message| message.kind == "Accepted" && message.from == 4);
        script.settle_all();
        // Slot 1 lands on the next column round-robin.
        script.play(propose(0, "bravo")).answer_all();
        script.settle_all();
        script.finish()
    },
};

// ---- 22. quorum reads -------------------------------------------------------

const QUORUM_READ_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::QuorumRead,
    ActionKind::ResendPending,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/quorum-reads`.
pub static QUORUM_READS: Level = Level {
    id: "act4/quorum-reads",
    act: 4,
    title: "Quorum reads",
    briefing: "\
The grid took the writes off the leader and left the reads on it. A read-index \
read asks the leader to prove it still leads, and that proof costs a round of \
beats and acks on the one node the grid was trying to relieve. There is a \
better question to ask, and it does not involve the leader at all.

Ask a **row** — a Phase-1 quorum — one thing each: what is the highest slot you \
have voted in? Take the largest answer. Then have any replica serve the read as \
soon as it has applied that slot. The argument is the same intersection you \
already know. A write acknowledged before this read began was chosen by a whole \
column. The row you asked crosses that column. So one of the acceptors that \
answered has voted that slot, and the largest answer is at or above it. There \
is no clock here, and no lease: this level refuses exactly the assumption that \
clocks agree.

The waiting is the interesting half. An acceptor raises its watermark when it \
**votes**, not when a slot is chosen, so a slot the leader started and did not \
finish raises it too. A read that lands on such a watermark waits for the slot \
to arrive. That costs the reader time. It does not cost the client a stale \
answer. You get one of those here: read at a follower while a slot sits \
half decided, and say whether the read may be served.",
    field_guide: "play.html",
    symbols: &[
        "ColocatedNode::quorum_read",
        "Acceptor::vote_watermark",
        "QuorumReads::serve",
        "Message::PreRead",
        "Replica::covers",
        "docs/references/papers — Whittaker et al., Compartmentalized Paxos §3.4",
    ],
    automation_on: NO_QUORUM_READ_SERVE,
    pinned_off: &[
        AutomationFlag::QuorumReadServe,
        AutomationFlag::DeliverHeartbeats,
    ],
    unlocked: REPLIES_ONLY,
    unlocks: &[AutomationFlag::QuorumReadServe],
    allowed_actions: QUORUM_READ_ACTIONS,
    setup: || fresh(6, GRID),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Err(detail) = log.linearizable() {
            return GoalStatus::Failed(detail);
        }
        if log.beats_broadcast() > 0 {
            return GoalStatus::Failed(
                "A leader broadcast a beat. This level answers its read without one.".to_string(),
            );
        }
        let served: Vec<(NodeId, Option<Slot>)> = log
            .reads()
            .into_iter()
            .filter(|(_, _, served)| *served)
            .map(|(node, index, _)| (node, index))
            .collect();
        let leader = log.leader();
        match served.first() {
            Some((node, index)) if Some(*node) != leader => GoalStatus::Reached(format!(
                "Node {} answered the read at {}, and it does not lead. No beat was broadcast, \
                 the leader opened no read round, and the history is linearizable.",
                node.0,
                index.map_or_else(
                    || "the empty prefix".to_string(),
                    |s| format!("slot {}", s.0)
                )
            )),
            Some((node, _)) => GoalStatus::Open(format!(
                "Node {} served the read, and it is the leader. Ask a follower instead: the \
                 point is that any replica can answer.",
                node.0
            )),
            None => GoalStatus::Open(
                "Ask a follower for a read, and decide when it may be answered.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Compare two numbers on the card: the highest slot the row has voted in, and the \
             slot this node has applied up to."
                .to_string(),
        ),
        _ => Some(
            "One acceptor in the row voted for a slot that is not chosen yet. This node has not \
             applied it. Wait, and let the leader re-send its Accept."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act4/quorum-reads");
        script.play(start_election(0)).settle_all();
        // Slot 0 goes to column 0 = {0, 3}. Node 3 votes; the answer is lost,
        // so the slot is voted for and chosen nowhere.
        script.play(propose(0, "alpha"));
        script.settle(kind_at("Accept", 0));
        script.drop_all(kind("Accepted"));
        // Node 4 is a follower in row 1 = {3, 4, 5}. Node 3 answers with slot
        // 0, and node 4 has applied nothing.
        script.play(quorum_read(4)).settle_all();
        script.answer_all();
        // The leader re-sends, the column completes, and the waiting read is
        // answered out of the batch that applies the slot.
        script.play(resend(0)).settle_all();
        script.finish()
    },
};

// ---- 23. the handoff --------------------------------------------------------

const HANDOFF_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Relinquish,
    ActionKind::SetAutomation,
];

/// `act4/the-handoff`.
pub static THE_HANDOFF: Level = Level {
    id: "act4/the-handoff",
    act: 4,
    title: "The handoff",
    briefing: "\
An election destroys a leadership and builds a new one. The successor picks a \
higher ballot, runs Phase 1, and rediscovers the log from a promise quorum. \
That is the right tool when the old leader is gone. It is a waste when the old \
leader is alive and simply wants to move — during a rolling restart, for \
example, or when an operator moves the leadership closer to its clients.

So hand the authority over instead. The outgoing leader sends the ballot, the \
next free slot, and the tail below it, split into the slots it knows are \
chosen and the slots whose Phase 2 is still open. The two exactly cover the \
range, and that is what lets the successor skip Phase 1: it may re-propose \
what it was told about, and there is nothing else in the range to be told \
about. Gap filling stays **off** here. A `Noop` needs a promise quorum's \
report to license it, and this successor collected none.

The dangerous case is a replayed message, so read the rule that closes it. \
Only the node that **won** a ballot may hand it on. Suppose a successor could \
pass it further. A delayed copy of the first message then arrives at that \
successor again. Every check passes — it is the named addressee and its \
promise still matches — so it installs the same authority a second time, \
beside the node that is already using it. Two nodes then hand out the same \
slots under one ballot. One hop costs an election. The alternative costs \
safety. Hand the leadership over, get a command chosen under it, and then try \
to hand it on again.",
    field_guide: "play.html",
    symbols: &[
        "ColocatedNode::relinquish_to",
        "ColocatedNode::can_relinquish",
        "Message::Relinquish",
        "LeadershipOrigin::Handoff",
        "RecoveryPolicy::Inherited",
        "docs/analysis/consensus/dpaxos-leader-handoff.md",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: HANDOFF_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let Some(handoff) = log.handoffs().first().copied() else {
            return GoalStatus::Open(
                "Get a command chosen, then hand the leadership to a peer.".to_string(),
            );
        };
        let Some(node) = log.node(handoff.to) else {
            return GoalStatus::Open("The successor is not running.".to_string());
        };
        if !node.is_leader() {
            return GoalStatus::Open(format!(
                "Node {} was offered the authority. Deliver the message that carries it.",
                handoff.to.0
            ));
        }
        if node.ballot() != handoff.ballot {
            return GoalStatus::Failed(format!(
                "Node {} leads at a different ballot: the authority was not inherited, it was \
                 won again.",
                handoff.to.0
            ));
        }
        // A command chosen **under the successor** is one at or above the
        // frontier it was handed: everything below that came from the
        // predecessor's own leadership.
        let under_successor = node.replica().chosen_index() >= Some(handoff.next_slot);
        let refusal = log.handoff_refusal(handoff.to);
        match (under_successor, refusal) {
            (true, Some(_)) => GoalStatus::Reached(format!(
                "Node {} leads at ballot {}.{}, which node {} won. No election ran, and slot {} \
                 was chosen under the inherited authority. Node {} may not hand it on: an \
                 authority moves once, and a second hop needs a durable record that paros does \
                 not keep.",
                handoff.to.0,
                handoff.ballot.round,
                handoff.ballot.node.0,
                handoff.from.0,
                handoff.next_slot.0,
                handoff.to.0
            )),
            (false, _) => GoalStatus::Open(format!(
                "Node {} holds the authority. Get a command chosen under it.",
                handoff.to.0
            )),
            (true, None) => GoalStatus::Failed(format!(
                "Node {} is allowed to hand the authority on. One hop is the rule.",
                handoff.to.0
            )),
        }
    },
    hint: |world, mistakes| {
        let handed = log_world(world).is_some_and(|log| !log.handoffs().is_empty());
        (mistakes > 0 || handed).then(|| {
            "Ask the successor to hand the leadership on. The refusal names the rule, and the \
             briefing explains the replayed message it closes."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act4/the-handoff");
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).settle_all();
        // The authority moves, under the same ballot and with no Prepare.
        script.play(relinquish(0, 1)).settle_all();
        script.play(propose(1, "bravo")).settle_all();
        script.finish()
    },
};

// ---- 28. faulty records -----------------------------------------------------

const FAULTY_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::ResendPending,
    ActionKind::Crash,
    ActionKind::Restart,
    ActionKind::Corrupt,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/faulty-records`.
pub static FAULTY_RECORDS: Level = Level {
    id: "act4/faulty-records",
    act: 4,
    title: "Faulty records",
    briefing: "\
Disks lose things one block at a time. A node comes back and one accepted \
record no longer reads: the value is gone, and the slot number and the ballot \
survive beside it. The node has three honest answers about that slot, not two. \
It voted and holds the value. It did not vote. Or it voted and no longer knows \
for what.

The third answer must stay its own answer. A node that turns it into \"I did \
not vote\" makes the mistake this level exists for. A candidate that hears silence from a \
whole quorum concludes that nothing was chosen there and decides something \
else. If the lost value was already chosen, that slot now holds two values. So \
a damaged record is reported as damaged, and a candidate that receives one \
cannot settle the slot from that report alone.

What settles it is more answers. The leader keeps asking the acceptors that \
have not replied, and each reply moves the slot into one of three cases. Some \
acceptor reports a value at a ballot at or above the damaged record: the \
leader re-proposes that value, and the damaged acceptor writes it back as it \
votes. A whole Phase-1 quorum reports nothing that could hide a chosen value: \
the leader decides a `Noop`. Neither yet: the leader waits. Damage one record \
here, bring the node back, and say which case each report puts the slot in.",
    field_guide: "play.html",
    symbols: &[
        "Storage::faulty_entries",
        "Acceptor::faulty",
        "Proposer::fold_probe_promise",
        "Proposer::resolve_probe",
        "docs/analysis/storage/ctrl-multipaxos-restatement.md",
    ],
    automation_on: NO_REPAIR_VERDICT,
    pinned_off: &[AutomationFlag::RepairVerdict],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::RepairVerdict],
    allowed_actions: FAULTY_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let damaged = (0..3)
            .map(NodeId)
            .find(|id| !log.faulty_records(*id).is_empty());
        let repaired = (0..3)
            .map(NodeId)
            .any(|id| log.disk(id).is_some_and(|disk| disk.has_applied(Slot(0))));
        let leader = log.leader();
        let blocked = leader.map_or(0, |id| log.blocked_repairs(id));
        let value = log
            .node(NodeId(2))
            .and_then(|node| node.replica().chosen_at(Slot(0)).map(show_command));
        match (damaged, blocked, repaired, value) {
            (None, 0, true, Some(value)) if value == "\"alpha\"" => GoalStatus::Reached(
                "Slot 0 holds \"alpha\", the value the damaged acceptor had voted for and could \
                 no longer read. The record is readable again on every node, because the \
                 acceptor wrote it back as it voted for the leader's proposal."
                    .to_string(),
            ),
            (None, 0, true, Some(value)) => GoalStatus::Failed(format!(
                "Slot 0 holds {value}. The repair re-proposed a value nobody had reported for \
                 that slot."
            )),
            (Some(id), _, _, _) => GoalStatus::Open(format!(
                "Node {} still holds a record it cannot read. Elect a leader and let it collect \
                 enough answers to settle that slot.",
                id.0
            )),
            _ => GoalStatus::Open(
                "Damage one accepted record, bring the node back, and elect a leader.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the card's second line. It says what the answers hold for that slot, and it \
             is the only thing this ballot may put there."
                .to_string(),
        ),
        _ => Some(
            "One acceptor reports the value it accepted, at a ballot at or above the damaged \
             record. Re-propose that value: a `Noop` would decide something else."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act4/faulty-records");
        script.play(start_election(0)).settle_all();
        // One command reaches node 2 alone, and its answer is lost: accepted
        // at one node, chosen nowhere, and node 1 does not hear of it.
        script.play(propose(0, "alpha"));
        script.drop_all(kind("Accept"));
        script.play(resend(0));
        script.settle(|message| message.kind == "Accept" && message.to == 2);
        script
            .drop_all(|message| matches!(message.kind.as_str(), "Accept" | "Accepted" | "Commit"));
        // The whole cluster goes down, and node 2's record rots.
        script.play(crash(0)).play(crash(1)).play(crash(2));
        script.play(Action::Corrupt { node: 2, slot: 0 });
        script.play(restart(2)).play(restart(1));
        // Node 1 and node 2 elect a leader. Node 2 reports slot 0 as damaged,
        // so the election cannot settle it.
        script.play(start_election(1)).settle_all();
        // Node 0 comes back holding the value, and the leader asks it.
        script.play(restart(0));
        script.play(tick(1)).settle_all();
        script.answer_all();
        script.settle_all();
        script.finish()
    },
};

// ---- 29. the wiped node -----------------------------------------------------

const WIPE_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Wipe,
    ActionKind::Restart,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/the-wiped-node`.
pub static THE_WIPED_NODE: Level = Level {
    id: "act4/the-wiped-node",
    act: 4,
    title: "The wiped node",
    briefing: "\
A crash costs a node everything it held in memory and nothing it wrote down. \
That is why paros keeps leadership in memory and promises on disk: the crash \
is an abdication, and the promise comes back. Losing the **disk** is a \
different failure, and it has a different answer.

A promise is the one thing a node may not take back. Having promised a ballot, \
it told some proposer that every lower ballot was finished there, and that \
proposer may have chosen a value on the strength of it. A node that boots with \
an empty disk has no memory of that promise. It answers a lower ballot, votes \
for whatever that ballot proposes, and a quorum built behind the older ballot \
chooses a second value for a slot that already has one. A snapshot does not \
help. A snapshot restores the log, and the peer that sends it does not know \
what this node has sworn.

So a node that lost its disk does not rejoin, and the library refuses the boot \
rather than trusting an operator to remember. Every store carries a marker \
that says it has been formatted. An operator's own record says the identity \
was provisioned once. A store that was provisioned and no longer carries its \
marker is a lost disk, and that is the refusal. Erase a disk here, try to \
bring it back, and keep the survivors deciding without it. What heals the \
cluster is a change of the acceptor set, which is the next act's business.",
    field_guide: "play.html",
    symbols: &[
        "NodeStorage::is_formatted",
        "BootKind::ExistingMember",
        "BootRefusal::Amnesia",
        "Acceptor::promised",
        "docs/analysis/play/game-plan.md",
    ],
    automation_on: NO_WIPED_REJOIN,
    pinned_off: &[AutomationFlag::WipedRejoin],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::WipedRejoin],
    allowed_actions: WIPE_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Some(node) = log.promise_regressed() {
            return GoalStatus::Failed(format!(
                "node {}'s durable promise came back lower than a promise it had already made.",
                node.0
            ));
        }
        let Some((wiped, promised)) = log.refused_boots().first().copied() else {
            let erased = (0..3).map(NodeId).find(|id| {
                log.disk(*id)
                    .is_some_and(|disk| disk.provisioned() && !disk.is_formatted())
            });
            return match erased {
                Some(id) => GoalStatus::Open(format!(
                    "Node {}'s disk is empty. Try to bring it back.",
                    id.0
                )),
                None => GoalStatus::Open("Erase one node's disk.".to_string()),
            };
        };
        if log.node(wiped).is_some() {
            return GoalStatus::Failed(format!(
                "node {} is running again on an empty disk.",
                wiped.0
            ));
        }
        let survivors: usize = (0..3)
            .map(NodeId)
            .filter(|id| *id != wiped)
            .map(|id| applied(world, id.0).len())
            .max()
            .unwrap_or(0);
        if survivors < 2 {
            return GoalStatus::Open(format!(
                "Node {} stays out. Get one more command chosen without it.",
                wiped.0
            ));
        }
        GoalStatus::Reached(format!(
            "Node {} is refused, and the two survivors chose another command without it. Its \
             disk no longer holds the promise {}.{} it made, and nothing in the cluster can give \
             that promise back. The acceptor set has to change instead.",
            wiped.0, promised.round, promised.node.0
        ))
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "An empty disk and a new disk look the same from inside the node. What tells them \
             apart is the operator's own record that this identity was provisioned once."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act4/the-wiped-node");
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).settle_all();
        // The disk goes, and with it the promise node 2 had made.
        script.play(Action::Wipe { node: 2 });
        script.play(restart(2)).answer_all();
        // Two of three is still a quorum, so the cluster carries on.
        script.play(propose(0, "bravo"));
        script.settle(|message| message.to != 2);
        script.drop_all(to(2));
        script.finish()
    },
};
