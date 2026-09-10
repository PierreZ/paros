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

use paros_core::{Command, Config, MatchmakerId, NodeId, QuorumSystem, Slot};

use crate::action::{Action, ActionKind, BallotSpec, Phase};
use crate::auto::AutomationFlag;
use crate::level::script::{Script, kind, kind_at, phase};
use crate::level::{GoalStatus, Level, WorldKind};
use crate::view::{MessageView, show_command};
use crate::world::decree::DecreeWorld;
use crate::world::matchmakers::MatchmakerProcess;
use crate::world::{Disk, World};

/// Act IV's levels, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &FLEXIBLE_QUORUMS,
        &THE_GRID,
        &QUORUM_READS,
        &THE_HANDOFF,
        // Part two: the matchmaker plane. The two levels below stay last —
        // they are numbered 28 and 29 in the plan.
        &MATCHMAKING,
        &RECONFIGURE,
        &GARBAGE_COLLECTION,
        &MATCHMAKER_GENERATIONS,
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
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
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
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
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
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
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
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
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
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
];

/// Every role but judging a cross-configuration Phase 1.
const NO_PHASE1_COMPLETE: &[AutomationFlag] = &[
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
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
];

/// Every role but fencing a matchmaker generation.
const NO_GENERATION_FENCE: &[AutomationFlag] = &[
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
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::MayRetire,
];

/// Every role but answering a retire request.
const NO_MAY_RETIRE: &[AutomationFlag] = &[
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
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
];

/// Every role but the two a reconfiguration teaches: judging a
/// cross-configuration Phase 1, and abandoning a stale belief.
const NO_PHASE1_NOR_STALE: &[AutomationFlag] = &[
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
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
];

/// The convenience toggles the log-world levels offer.
const TOGGLES: &[AutomationFlag] = &[
    AutomationFlag::DeliverReplies,
    AutomationFlag::DeliverHeartbeats,
];

/// The one toggle a level that pins beats off may still offer.
const REPLIES_ONLY: &[AutomationFlag] = &[AutomationFlag::DeliverReplies];

/// The toggles a matchmaker level offers once the player has earned the
/// matchmaker pump.
const MATCHMAKER_TOGGLES: &[AutomationFlag] = &[
    AutomationFlag::DeliverReplies,
    AutomationFlag::DeliverHeartbeats,
    AutomationFlag::DeliverMatchmakerReplies,
];

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

/// A cluster that **names matchmakers**: `pool` is every node that may ever be
/// an acceptor, `bootstrap` the acceptor set in force before any ballot was
/// registered, `matchmakers` the registry tier of generation 0, and `spares`
/// the matchmakers a generation handover may pull in.
///
/// A node outside `bootstrap` is a spare acceptor: it is addressable, it
/// answers Phase 1 for the ballots it takes part in, and it becomes an acceptor
/// only when a reconfiguration names it.
fn deployed(pool: &[u64], bootstrap: &[u64], matchmakers: &[u64], spares: &[u64]) -> WorldKind {
    let nodes: Vec<NodeId> = pool.iter().copied().map(NodeId).collect();
    let members: Vec<NodeId> = bootstrap.iter().copied().map(NodeId).collect();
    let set: Vec<MatchmakerId> = matchmakers.iter().copied().map(MatchmakerId).collect();
    let mut all = set.clone();
    all.extend(spares.iter().copied().map(MatchmakerId));
    all.sort_unstable();
    let disks: Vec<Disk> = nodes
        .iter()
        .map(|id| {
            Disk::new(Config {
                id: *id,
                peers: members.clone(),
                quorum_system: QuorumSystem::Majority,
                nodes: nodes.clone(),
                matchmakers: set.clone(),
                matchmaker_pool: all.clone(),
            })
        })
        .collect();
    let processes = all
        .iter()
        .map(|id| MatchmakerProcess::new(*id, set.clone()))
        .collect();
    WorldKind::Log(Box::new(
        World::from_disks(disks, &[CLIENT], TIMEOUT).with_matchmakers(processes),
    ))
}

/// What a node's application has executed, with the protocol's own control
/// commands taken out: what a level means when it says "a command".
fn commands(world: &WorldKind, node: u64) -> Vec<String> {
    applied(world, node)
        .into_iter()
        .filter(|command| {
            !command.starts_with("Noop")
                && !command.starts_with("Truncate")
                && !command.starts_with("Snap")
        })
        .collect()
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
which acceptors an `Accept` gets to. Get `alpha` chosen with two acceptors. \
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
                "Two acceptors chose {chosen}, and three had to be asked to find it again. \
                 Ballot {}.{} reached {:?}, and every set of three acceptors here contains one \
                 of the two that voted. That is the whole of `q1 + q2 > n`.",
                campaign.ballot.round,
                campaign.ballot.node.0,
                campaign.reach.iter().map(|n| n.0).collect::<Vec<_>>()
            )),
            Some(campaign) => GoalStatus::Failed(format!(
                "A campaign proposed {} over the chosen {chosen}.",
                text(&campaign.proposed)
            )),
            None => GoalStatus::Open(format!(
                "{chosen} is chosen. Now run a second ballot and see what its promises report."
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

// ---- 24. matchmaking --------------------------------------------------------

const MATCHMAKING_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Crash,
    ActionKind::Restart,
    ActionKind::CrashMatchmaker,
    ActionKind::RestartMatchmaker,
    ActionKind::ResendMatchmaking,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/matchmaking`.
pub static MATCHMAKING: Level = Level {
    id: "act4/matchmaking",
    act: 4,
    title: "Matchmaking",
    briefing: "\
Every level so far kept one thing fixed: the acceptors never changed. Let \
them change and the safety argument breaks in a way that is easy to miss. \
Say the acceptors are `{0, 1, 2}` and a leader gets a slot chosen with nodes \
0 and 1, then dies before it tells anybody. An operator moves the cluster to \
`{2, 3, 4}`. A new leader asks that set, hears from nodes 3 and 4, and both \
report nothing — truthfully, because neither was there. It decides something \
else, and one slot holds two values.

So a new leader must ask a Phase-1 quorum of **every** acceptor set that may \
still hold a value it has not seen. That raises a question: how does a \
candidate learn which sets existed? A separate small service answers it. A \
**matchmaker** keeps one durable map from a ballot to an acceptor set. It \
holds no log, it votes on no slot, and nothing on the command path asks it \
anything.

A candidate registers `(my ballot, my acceptor set)` with a quorum of \
matchmakers before it sends one `Prepare`. Each of them answers with every \
set it holds below that ballot. A quorum is enough, and the reason is the \
intersection you already know. Every earlier ballot registered with a quorum \
of the same matchmakers before it could accept anything, and two quorums \
share a matchmaker. So the union of the answers names every set an earlier \
ballot could have chosen under.

Elect node 0, get a command chosen, then take node 0 away and elect node 1. \
Node 1's matchmakers will tell it about node 0's set, and you say whether \
its Phase 1 is complete.",
    field_guide: "play.html",
    symbols: &[
        "ColocatedNode::on_match_reply",
        "Ready::match_requests",
        "Matchmaking",
        "MatchStep::Completed",
        "Proposer::phase1_won",
        "docs/references/papers — Whittaker et al., Matchmaker Paxos §3",
    ],
    automation_on: NO_PHASE1_COMPLETE,
    pinned_off: &[
        AutomationFlag::Phase1Complete,
        AutomationFlag::DeliverMatchmakerReplies,
    ],
    unlocked: TOGGLES,
    unlocks: &[
        AutomationFlag::Phase1Complete,
        AutomationFlag::DeliverMatchmakerReplies,
    ],
    allowed_actions: MATCHMAKING_ACTIONS,
    setup: || deployed(&[0, 1, 2], &[0, 1, 2], &[0, 1], &[]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let Some(leader) = log.leader() else {
            return GoalStatus::Open(
                "Elect a leader. A candidate asks the matchmakers first.".to_string(),
            );
        };
        let prior = log.prior_configurations(leader);
        if prior.is_empty() {
            return GoalStatus::Open(format!(
                "Node {} leads, and the matchmakers told it nothing came before. Get a command \
                 chosen, then take node {} away and elect somebody else.",
                leader.0, leader.0
            ));
        }
        let executed = commands(world, leader.0);
        if executed.len() < 2 {
            return GoalStatus::Open(format!(
                "Node {} was elected across a set the matchmakers named. Get one more command \
                 chosen under it.",
                leader.0
            ));
        }
        GoalStatus::Reached(format!(
            "Node {} leads, and it was elected across the acceptor set the matchmakers reported \
             for the earlier ballot. It has executed {} commands. No Prepare left before a \
             matchmaker quorum answered, and Phase 1 closed only once that set held a quorum of \
             its own.",
            leader.0,
            executed.len()
        ))
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the card's first two lines: who has promised, and which sets the matchmakers \
             named."
                .to_string(),
        ),
        _ => Some(
            "One set is named, and two of its three acceptors have promised. Two of three is a \
             Phase-1 quorum of that set."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act4/matchmaking");
        // The first campaign: the matchmakers report that nothing came before,
        // so Phase 1 has nothing to recover and closes at once.
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).settle_all();
        // Node 0 goes, and node 1 campaigns. Its matchmakers name node 0's set.
        script.play(crash(0));
        script.play(start_election(1)).settle_all();
        script.play(propose(1, "bravo")).settle_all();
        script.finish()
    },
};

// ---- 25. reconfigure --------------------------------------------------------

const RECONFIGURE_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Reconfigure,
    ActionKind::Crash,
    ActionKind::Restart,
    ActionKind::ResendMatchmaking,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/reconfigure`.
pub static RECONFIGURE: Level = Level {
    id: "act4/reconfigure",
    act: 4,
    title: "Reconfigure",
    briefing: "\
An acceptor set belongs to a ballot, and it is never edited. Edit it under a \
live ballot and every quorum count in flight changes meaning. The \
matchmakers' map from a ballot to a set becomes a lie as well. So there is \
exactly one way to change the acceptors. The leader picks a **fresh ballot** \
and registers the new set with it: a reconfiguration is a round change.

That has a price and a shape. The price is a stall. The leader abandons its \
open rounds and admits no new command. It leads again after one matchmaking \
round trip and one Phase 1. The shape is that the new ballot's Phase 1 must \
cover the **old** set, which the matchmakers now report. Its Phase 2 \
addresses the new set alone.

A joining node is sent the `Prepare` too, so it promises the ballot and \
learns the set before any `Accept` reaches it. A removed node keeps \
answering Phase 1 for the ballots it took part in: removed is not shut down. \
(A cluster with no matchmakers refuses a reconfiguration outright — there is \
nowhere to record a second set, so plain Multi-Paxos keeps one for life.)

Grow the cluster onto the spare, node 3. Then take the leader away and elect \
somebody else. That campaign is told about **two** sets, the one before the \
change and the one after, and it must hold a Phase-1 quorum of each. A \
quorum of everything the two sets name together is not the same claim, and \
the card will ask you which one Phase 1 needs.",
    field_guide: "play.html",
    symbols: &[
        "ColocatedNode::reconfigure",
        "ReconfigureResult",
        "ReconfigureRefusal::NoMatchmakers",
        "Election::covered",
        "Registration::reconfiguration",
        "docs/references/papers — Whittaker et al., Matchmaker Paxos §4.2",
    ],
    automation_on: NO_PHASE1_NOR_STALE,
    pinned_off: &[
        AutomationFlag::Phase1Complete,
        AutomationFlag::StaleConfiguration,
    ],
    unlocked: MATCHMAKER_TOGGLES,
    unlocks: &[AutomationFlag::StaleConfiguration],
    allowed_actions: RECONFIGURE_ACTIONS,
    setup: || deployed(&[0, 1, 2, 3], &[0, 1, 2], &[0, 1], &[]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let Some(leader) = log.leader() else {
            return GoalStatus::Open("Elect a leader, then grow onto node 3.".to_string());
        };
        let Some(node) = log.node(leader) else {
            return GoalStatus::Open("The leader is not running.".to_string());
        };
        let members = node.acceptors().members().len();
        if members < 4 {
            return GoalStatus::Open(format!(
                "Node {} leads a set of {members}. Ask it to run with the acceptors 0, 1, 2 and 3.",
                leader.0
            ));
        }
        let prior = log.prior_configurations(leader);
        if prior.len() < 2 {
            return GoalStatus::Open(format!(
                "The new set is in force. Now take node {} away and elect somebody else: that \
                 campaign is told about both sets.",
                leader.0
            ));
        }
        if !commands(world, 3).iter().any(|command| command == "bravo") {
            return GoalStatus::Open(
                "Get one more command chosen under the new set, and let node 3 execute it."
                    .to_string(),
            );
        }
        GoalStatus::Reached(format!(
            "Node {} leads a set of four at ballot {}, which is above the ballot the set of three \
             was bound to. Node 3 promised that ballot before any Accept reached it, and it has \
             executed the command chosen under the new set. The campaign that elected node {} had \
             to hold a Phase-1 quorum of both sets, not of the {} acceptors they name together.",
            leader.0,
            crate::view::show_ballot(node.acceptors_since()),
            leader.0,
            prior
                .iter()
                .flat_map(|config| config.members().iter().copied())
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        ))
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Count the promises against each named set on its own, not against the two of them \
             together."
                .to_string(),
        ),
        _ => Some(
            "The set of three needs two of its own members to have promised. Check that one \
             first."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act4/reconfigure");
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).settle_all();
        // Node 2 is away for the change, so it will come back believing the
        // set of three — a belief this cluster has already replaced.
        script.play(crash(2));
        // The set grows onto the spare. Phase 1 covers the set of three.
        script
            .play(Action::Reconfigure {
                node: 0,
                members: vec![0, 1, 2, 3],
                quorum: None,
            })
            .settle_all();
        script.play(propose(0, "bravo")).settle_all();
        // Node 2 comes back on its bootstrap belief and campaigns on it. The
        // matchmakers report the change, so the campaign is abandoned; the
        // next one registers the set in force and is told about both.
        script.play(restart(2)).settle_all();
        script.play(crash(0));
        script.play(start_election(2)).settle_all();
        script.play(start_election(2)).settle_all();
        script.play(propose(2, "charlie")).settle_all();
        script.finish()
    },
};

// ---- 26. garbage collection -------------------------------------------------

const GC_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Reconfigure,
    ActionKind::Retire,
    ActionKind::ResendGc,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/garbage-collection`.
pub static GARBAGE_COLLECTION: Level = Level {
    id: "act4/garbage-collection",
    act: 4,
    title: "Garbage collection",
    briefing: "\
Every reconfiguration adds a set the matchmakers must keep, and every later \
Phase 1 must cover every set they report. Left alone, elections get slower \
for ever and a removed machine can never be switched off. So a set has to \
become forgettable — and the condition for that is exact. A set may be \
forgotten only when **no future leader can need its Phase-1 quorum to learn \
a value its Phase-2 quorum may have chosen**.

A leader can prove that about the log it holds. Above its election fence, \
its own Phase 1 already reported that nothing was accepted, so no older set \
is relevant there. Between the fence and its applied prefix, everything was \
re-proposed or filled under its own ballot and set. Below that, a node that \
learns a slot chosen writes the value down as its accepted record. A member \
of the current set therefore answers a later Phase 1 with it.

So the condition is this. The leadership must be settled, and a **Phase-2 \
quorum of the current set must report a chosen index at or past the fence**. The leader \
then asks the matchmakers to raise their floor to its own ballot. The floor \
is in force only once a matchmaker **quorum** has written it down.

Only then may a removed acceptor be switched off, and the request must carry \
the floor as evidence. \"I am not in the set in force\" is a belief this node \
forgets at every crash. A floor above every ballot a set naming it was bound \
to is a fact. Take the cluster down to three acceptors, and beat until the \
floor is in force. Then answer for node 3 twice: once with no evidence, once \
with the watermark the leader reports.",
    field_guide: "play.html",
    symbols: &[
        "ColocatedNode::gc_effective",
        "ColocatedNode::may_retire",
        "GcStep::Effective",
        "Ready::gc_requests",
        "docs/analysis/consensus/matchmaker-gc-and-generations.md",
    ],
    automation_on: NO_MAY_RETIRE,
    pinned_off: &[AutomationFlag::MayRetire],
    unlocked: MATCHMAKER_TOGGLES,
    unlocks: &[AutomationFlag::MayRetire],
    allowed_actions: GC_ACTIONS,
    setup: || deployed(&[0, 1, 2, 3], &[0, 1, 2, 3], &[0, 1], &[]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let refused = !log.refused_retires().is_empty();
        if !log.retired(NodeId(3)) {
            let floor = log.leader().and_then(|leader| log.gc_effective(leader));
            return match floor {
                Some((watermark, retirable)) if retirable.contains(&NodeId(3)) => {
                    GoalStatus::Open(format!(
                        "The floor {} is in force and it releases node 3. Ask node 3 to retire, \
                         and show it that watermark.",
                        crate::view::show_ballot(watermark)
                    ))
                }
                _ => GoalStatus::Open(
                    "Take node 3 out of the acceptor set, then beat until a matchmaker quorum \
                     holds the floor."
                        .to_string(),
                ),
            };
        }
        if !refused {
            return GoalStatus::Failed(
                "Node 3 retired, and no request was ever refused for want of evidence. The point \
                 of this level is the refusal."
                    .to_string(),
            );
        }
        GoalStatus::Reached(
            "Node 3 is retired, and the first request — the one that carried no watermark — was \
             refused with \"not collected\". An installed successor set is not a collected \
             predecessor: what licenses the shutdown is a floor a matchmaker quorum wrote down, \
             above every ballot a set naming node 3 was bound to."
                .to_string(),
        )
    },
    hint: |world, mistakes| {
        let effective = log_world(world)
            .and_then(|log| log.leader().and_then(|leader| log.gc_effective(leader)));
        (mistakes > 0).then(|| match effective {
            Some(_) => "The leader reports a floor now. Compare it with the ballots node 3's own \
                        sets were bound to."
                .to_string(),
            None => "No floor is in force yet. A request with no evidence behind it is refused."
                .to_string(),
        })
    },
    reference: || {
        let mut script = Script::new("act4/garbage-collection");
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).settle_all();
        // Node 3 leaves the acceptor set. It stays running, and it keeps
        // answering Phase 1 for the ballots it took part in.
        script
            .play(Action::Reconfigure {
                node: 0,
                members: vec![0, 1, 2],
                quorum: None,
            })
            .settle_all();
        // No floor yet: the request carries no evidence and is refused.
        script
            .play(Action::Retire {
                node: 0,
                target: 3,
                gc_watermark: None,
            })
            .answer_all();
        // Beat until a matchmaker quorum holds the floor.
        let mut watermark = None;
        for _ in 0..12 {
            watermark = script
                .world()
                .log()
                .and_then(|log| log.gc_effective(NodeId(0)))
                .map(|(ballot, _)| ballot);
            if watermark.is_some() {
                break;
            }
            script.play(tick(0)).settle_all();
        }
        let watermark = watermark.expect("a matchmaker quorum holds the floor");
        script
            .play(Action::Retire {
                node: 0,
                target: 3,
                gc_watermark: Some(BallotSpec {
                    round: watermark.round,
                    node: watermark.node.0,
                }),
            })
            .answer_all();
        script.finish()
    },
};

// ---- 27. matchmaker generations ---------------------------------------------

const GENERATIONS_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::ReconfigureMatchmakers,
    ActionKind::ResendReconfigurer,
    ActionKind::CrashMatchmaker,
    ActionKind::RestartMatchmaker,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act4/matchmaker-generations`.
pub static MATCHMAKER_GENERATIONS: Level = Level {
    id: "act4/matchmaker-generations",
    act: 4,
    title: "Matchmaker generations",
    briefing: "\
The matchmakers are now the source of truth about membership. Their own \
membership cannot be frozen for ever: one of them loses a disk and must be \
replaced. Asking a higher tier of matchmakers for the matchmakers never \
bottoms out. The answer is that the matchmaker set carries a **generation**. \
Generation `g + 1` is chosen by single-decree Paxos whose acceptors are the \
members of generation `g`. That decree is Act I again — the same proposer, \
the same acceptor, one slot — with a list of matchmakers as its value.

The handover has five steps, and their order is the argument. **Stop**: a \
quorum of the old generation freezes, durably. A frozen matchmaker registers \
nothing more, so the copy taken next is a still picture.

**Reconstruct**: take the highest floor the frozen members report and the \
union of their registries above it. **Bootstrap**: every proposed member \
writes that reconstruction down, marked pending. **Decide**: the decree \
chooses one successor, so two operators racing cannot install two. \
**Publish**: the old members record the link and point stragglers at it; the \
new members activate what they held pending.

Every message names the generation it is for, and a matchmaker answers only \
its own. Replace matchmaker 2 by matchmaker 3. A campaign that started \
before the handover still has a registration in flight for the old \
generation. When that registration reaches a frozen matchmaker, you say what \
the matchmaker does with it.",
    field_guide: "play.html",
    symbols: &[
        "MatchmakerReconfigurer",
        "MatchmakerSet",
        "Matchmaker::step_reconfigure",
        "MatchRefusal::Stopped",
        "ColocatedNode::learn_matchmakers",
        "docs/analysis/consensus/matchmaker-gc-and-generations.md",
    ],
    automation_on: NO_GENERATION_FENCE,
    pinned_off: &[AutomationFlag::GenerationFence],
    unlocked: MATCHMAKER_TOGGLES,
    unlocks: &[AutomationFlag::GenerationFence],
    allowed_actions: GENERATIONS_ACTIONS,
    setup: || deployed(&[0, 1, 2], &[0, 1, 2], &[0, 1, 2], &[3]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let active: Vec<u64> = log
            .matchmakers()
            .iter()
            .filter(|process| {
                process.role().is_some_and(|role| {
                    role.set().generation.0 == 1
                        && role.phase() == paros_core::MatchmakerPhase::Active
                })
            })
            .map(|process| process.id().0)
            .collect();
        if active.len() < 3 {
            return GoalStatus::Open(format!(
                "Generation 1 is active at {} of its three members. Drive the handover: stop, \
                 bootstrap, decide, publish.",
                active.len()
            ));
        }
        let registered = log.pool().iter().copied().find(|id| {
            log.node(*id).is_some_and(|node| {
                node.matchmaker_set()
                    .is_some_and(|set| set.generation.0 == 1)
            }) && !log.prior_configurations(*id).is_empty()
        });
        let Some(leader) = log.leader() else {
            return GoalStatus::Open(
                "Generation 1 serves. Now elect a leader through it.".to_string(),
            );
        };
        if registered.is_none()
            || log
                .node(leader)
                .and_then(|node| node.matchmaker_set().map(|set| set.generation.0))
                != Some(1)
        {
            return GoalStatus::Open(
                "Elect a leader that registers its ballot with generation 1.".to_string(),
            );
        }
        if commands(world, leader.0).is_empty() {
            return GoalStatus::Open(format!(
                "Node {} leads through generation 1. Get a command chosen.",
                leader.0
            ));
        }
        GoalStatus::Reached(format!(
            "Generation 1 = {active:?} serves matchmaking at every member, the matchmaker it \
             replaced stays alive to point late candidates at it, node {} was elected through the \
             new generation, and a command is chosen under that leadership. One successor was \
             chosen, by a decree over the generation it replaced.",
            leader.0
        ))
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the card's second line: it says which generation this matchmaker holds, and \
             whether it is frozen."
                .to_string(),
        ),
        _ => Some(
            "This matchmaker is frozen for the generation the request names. A frozen matchmaker \
             registers nothing more; it answers with the successor it knows."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act4/matchmaker-generations");
        script.play(start_election(0)).settle_all();
        // Node 1 opens a campaign against generation 0 and its registrations
        // stay in flight: this is the straggler the fence will refuse.
        script.play(start_election(1));
        // The handover runs while they wait.
        script.play(Action::ReconfigureMatchmakers {
            node: 0,
            members: vec![0, 1, 3],
        });
        for _ in 0..10 {
            script.settle(phase("reconfigure"));
            script.play(tick(0));
        }
        script.settle(phase("reconfigure"));
        // The straggler reaches the matchmaker that was left behind.
        script.settle(|message| message.kind == "MatchRequest" && message.to == 2);
        script.settle(|message| message.kind == "MatchReply");
        // Node 1 adopts the new generation and campaigns through it.
        script.play(start_election(1)).settle_all();
        script.play(propose(1, "alpha")).settle_all();
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
            (None, 0, true, Some(value)) if value == "alpha" => GoalStatus::Reached(
                "Slot 0 holds alpha, the value the damaged acceptor had voted for and could \
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
