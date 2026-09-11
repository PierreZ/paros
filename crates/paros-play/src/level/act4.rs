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

/// How many beats a matchmaker-set handover may make no progress for before
/// the node driving it gives it up, on the level that teaches the handover.
///
/// Its floor is structural: a phase must get more beats than one round trip
/// needs, or a handover that is merely slow is given up every time. Four is
/// three beats above the one beat a freeze or a bootstrap answer takes here.
const HANDOVER_STALL: u64 = 4;

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
///
/// `stall` is how many beats a matchmaker-set handover may make no progress
/// for before the node driving it gives up. It is **driver policy**, never a
/// constant inside the state machine, so every level names its own; `0`
/// disables it, which is what a level that never wants a handover abandoned
/// asks for.
fn deployed(
    pool: &[u64],
    bootstrap: &[u64],
    matchmakers: &[u64],
    spares: &[u64],
    stall: u64,
) -> WorldKind {
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
        World::from_disks(disks, &[CLIENT], TIMEOUT)
            .with_matchmakers(processes)
            .with_reconfigure_timeout(stall),
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
Act I showed that any two majorities share an acceptor, and that this fact \
gives the safety argument. Read the argument again, because a smaller \
condition is sufficient. Phase 1 must learn every value that Phase 2 possibly \
chose. Two Phase-1 quorums do not need to share an acceptor. Two Phase-2 \
quorums do not need to share one either. Only the two phases must meet.

Write that condition as arithmetic and you get `q1 + q2 > n`. Four acceptors \
then give you a choice that a majority does not offer: this level runs \
`q1 = 3` and `q2 = 2`. **Two** acceptors now choose a value, and every later election must \
collect **three** promises. That trade is the subject of this level. The \
steady state costs less and tolerates more, because a write needs two answers, \
not three. The next election costs more, because it needs three answers while \
a write needs only two.

You control the reach of each phase. The reach names the acceptors that a \
`Prepare` reaches, and the acceptors that an `Accept` reaches. Get `alpha` \
chosen with two acceptors. Then run a second ballot with three acceptors, and \
look at the value that comes back. Try to select three acceptors that contain \
neither voter. Three plus two is five, and there are only four acceptors, so \
no such set exists.",
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
                "Two acceptors chose {chosen}, and three acceptors had to answer to find it \
                 again. Ballot {}.{} reached {:?}. Every set of three acceptors here contains \
                 one of the two that voted, and that is what `q1 + q2 > n` says.",
                campaign.ballot.round,
                campaign.ballot.node.0,
                campaign.reach.iter().map(|n| n.0).collect::<Vec<_>>()
            )),
            Some(campaign) => GoalStatus::Failed(format!(
                "A campaign proposed {}, but the cluster already chose {chosen}.",
                text(&campaign.proposed)
            )),
            None => GoalStatus::Open(format!(
                "{chosen} is chosen. Now run a second ballot, and read what its promises \
                 report."
            )),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Set the Phase-1 reach to three acceptors before you open the second ballot. Two \
             acceptors cannot complete Phase 1 here, whichever two you select."
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
A quorum is not always a count. Put six acceptors in two rows of three. Call \
any whole **row** a Phase-1 quorum, and call any whole **column** a Phase-2 \
quorum. A row and a column of one grid always cross at exactly one cell, so \
the two phases meet by geometry. The rule uses no arithmetic, and the safety \
argument does not change.

The grid gives more throughput. Each slot goes to **one column**, so each \
acceptor gets a third of the writes, and more columns give more capacity. The \
cost is that a failure now depends on *which* acceptor is down, not on how \
many. One dead acceptor leaves its column incomplete, and the slots of that \
column wait. The column of a slot is `slot` modulo the number of columns. That \
column is a rule, not a message: no message carries the column, so a restarted \
leader and a successor compute the same one.

Get two commands chosen. You select the column for each command, and the grid \
marks your answer. Then look at the rule itself: send a copy of the `Accept` \
for slot 0 to a node outside the column of slot 0. That node is a member of \
the configuration, and it votes correctly and safely. Its vote counts for \
nothing, because a Phase-2 quorum here is a whole column, and that node is in \
another column.",
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
                "Column {first} decided slot 0, column {second} decided slot 1, and all six \
                 nodes applied both slots. Node 4 also holds the value of slot 0. Its vote for \
                 that slot counted for nothing, because it is not in column {first}."
            )),
            (true, [first, second], false) if first != second => GoalStatus::Open(format!(
                "Both slots are chosen, on columns {first} and {second}. Now send a copy of \
                 the Accept for slot 0 to a node outside column {first}, and look at the tally."
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
            "Slot 0 goes to column 0, slot 1 to column 1, slot 2 to column 2, and slot 3 goes \
             to column 0 again. Every column is safe, but only this column is the one that every \
             other node computes."
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
The grid moved the writes away from the leader, but the reads stay on the \
leader. A read-index read asks the leader to prove that it still leads. That \
proof costs one round of beats and acks on the node that the grid must \
protect. A better question exists, and it does not use the leader.

Ask a **row**, which is a Phase-1 quorum, one question each: what is the \
highest slot that you voted in? Take the largest answer, and let any replica \
serve the read after it applies that slot. The argument is the intersection \
that you already know. A whole column chose every write acknowledged before \
this read started, and the row that you asked crosses that column. One of the \
acceptors that answered voted in that slot, so the largest answer is at or \
above it. This level uses no clock and no lease, and it refuses the assumption \
that clocks agree.

The wait is the second half of the rule. An acceptor raises its watermark when \
it **votes**, not when a slot becomes chosen. A slot that the leader started \
and did not finish therefore raises the watermark too. A read that meets such \
a watermark waits for the slot. That wait costs the reader time, and it does \
not give the client an old answer. This level gives you that case: read at a \
follower while a slot is not yet decided, and say whether the cluster may \
serve the read.",
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
        // A read the cluster could answer at once teaches nothing: the wait is
        // the half of the rule this level exists for. So the read must reach at
        // least the slot of a write the client already holds an ack for, which
        // is the write a stale answer would lose.
        let acked = log.highest_acked_slot();
        match served.first() {
            Some((node, index)) if Some(*node) != leader && *index >= acked && acked.is_some() => {
                GoalStatus::Reached(format!(
                    "Node {} answered the read at {}, and it does not lead. No node sent a \
                     beat, and the leader opened no read round. The client already holds an ack \
                     for {}, and the answer is at or above that slot. The history is \
                     linearizable.",
                    node.0,
                    index.map_or_else(
                        || "the empty prefix".to_string(),
                        |s| format!("slot {}", s.0)
                    ),
                    acked.map_or_else(
                        || "the empty prefix".to_string(),
                        |s| format!("slot {}", s.0)
                    )
                ))
            }
            Some((node, _)) if Some(*node) == leader => GoalStatus::Open(format!(
                "Node {} served the read, and it is the leader. Ask a follower instead, \
                 because any replica can answer.",
                node.0
            )),
            Some((node, index)) => GoalStatus::Open(format!(
                "Node {} answered the read at {}, and the client holds no ack at or below that \
                 slot. Get a command chosen and acknowledged first. Then ask for the read.",
                node.0,
                index.map_or_else(
                    || "the empty prefix".to_string(),
                    |s| format!("slot {}", s.0)
                )
            )),
            None => GoalStatus::Open(
                "Ask a follower for a read. Then decide when the follower may answer it."
                    .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Compare two numbers on the card: the highest slot that the row voted in, and the \
             last slot that this node applied."
                .to_string(),
        ),
        _ => Some(
            "One acceptor in the row voted in a slot that is not chosen yet, and this node did \
             not apply that slot. Wait, and let the leader send its Accept again."
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
An election destroys a leadership and builds a new one. The successor selects \
a higher ballot, runs Phase 1, and learns the log from a promise quorum. That \
method is correct when the old leader is gone. It costs too much when the old \
leader is alive and only wants to move. A rolling restart is one example, and \
an operator that moves the leadership nearer to its clients is another.

Move the authority instead. The old leader sends the ballot, the next free \
slot, and the tail below that slot. It splits the tail into the slots that it \
knows are chosen and the slots whose Phase 2 is still open. The two parts \
cover the range exactly, so the successor can omit Phase 1. It re-proposes the \
slots that the message described, and the range holds nothing else. The gap \
fill stays **off** here, because a `Noop` needs a report from a promise \
quorum, and this successor collected none.

A replayed message is the unsafe case, so read the rule that prevents it: only \
the node that **won** a ballot may pass it on. Suppose that a successor could \
pass the ballot further, and a delayed copy of the first message reaches that \
successor again. Every check passes, because the message names that node and \
its promise still matches. The node installs the same authority a second time, \
beside the node that already uses it. Two nodes then give out the same slots \
under one ballot, so one extra hop costs safety while one hop costs only an \
election. Move the leadership, get a command chosen under it, and then try to \
move it again.",
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
                "Get a command chosen. Then move the leadership to a peer.".to_string(),
            );
        };
        let Some(node) = log.node(handoff.to) else {
            return GoalStatus::Open("The successor is not running.".to_string());
        };
        if !node.is_leader() {
            return GoalStatus::Open(format!(
                "The old leader offered the authority to node {}. Deliver the message that \
                 carries it.",
                handoff.to.0
            ));
        }
        if node.ballot() != handoff.ballot {
            return GoalStatus::Failed(format!(
                "Node {} leads at a different ballot. It did not inherit the authority. It \
                 won a new one.",
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
                "Node {} leads at ballot {}.{}, which node {} won. No election ran, and the \
                 cluster chose slot {} under the inherited authority. Node {} must not pass the \
                 authority on. An authority moves once, and a second hop needs a durable record \
                 that paros does not keep.",
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
                "Node {} can pass the authority on. The rule allows one hop only.",
                handoff.to.0
            )),
        }
    },
    hint: |world, mistakes| {
        let handed = log_world(world).is_some_and(|log| !log.handoffs().is_empty());
        (mistakes > 0 || handed).then(|| {
            "Ask the successor to pass the leadership on. The refusal names the rule, and the \
             briefing explains the replayed message that the rule prevents."
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
Every level until now kept the acceptors fixed, and if the acceptors change, \
the safety argument breaks in a way that is easy to miss. Take the acceptors \
`{0, 1, 2}`. A leader gets a slot chosen with nodes 0 and 1, and then it stops \
before it tells any other node. An operator moves the cluster to `{2, 3, 4}`. \
A new leader asks that set and hears from nodes 3 and 4, and both report \
nothing. Their reports are correct, because neither node was a member before, \
so the new leader decides another value and the slot holds two values.

A new leader must therefore ask a Phase-1 quorum of **every** acceptor set \
that possibly holds a value that it has not seen. One question follows: how \
does a candidate learn which sets existed? A small separate service answers \
that question. A **matchmaker** keeps one durable map from a ballot to an \
acceptor set. It holds no log, it votes on no slot, and no message on the \
command path asks it anything.

A candidate registers its own ballot and its own acceptor set with a quorum \
of matchmakers. It does that before it sends one `Prepare`. Each matchmaker \
answers with every set that it holds below that ballot. A quorum is \
sufficient, for the intersection reason that you know. Every earlier ballot \
registered with a quorum of the same matchmakers before any acceptor voted, \
and two quorums share a matchmaker. The union of the answers therefore names \
every set that an earlier ballot could use.

Elect node 0 and get a command chosen. Then remove node 0 and elect node 1. \
The matchmakers tell node 1 about the set of node 0. You then say whether its \
Phase 1 is complete.",
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
    setup: || deployed(&[0, 1, 2], &[0, 1, 2], &[0, 1], &[], 0),
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
                "Node {} leads, and the matchmakers reported no earlier set. Get a command \
                 chosen. Then remove node {} and elect another node.",
                leader.0, leader.0
            ));
        }
        let executed = commands(world, leader.0);
        if executed.len() < 2 {
            return GoalStatus::Open(format!(
                "The cluster elected node {} across a set that the matchmakers named. Get one \
                 more command chosen under it.",
                leader.0
            ));
        }
        GoalStatus::Reached(format!(
            "Node {} leads, and the cluster elected it across the acceptor set that the \
             matchmakers reported for the earlier ballot. It executed {} commands. No Prepare \
             went out before a matchmaker quorum answered. Phase 1 closed only after that set \
             held a quorum of its own.",
            leader.0,
            executed.len()
        ))
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the first two lines of the card: which nodes promised, and which sets the \
             matchmakers named."
                .to_string(),
        ),
        _ => Some(
            "The matchmakers named one set, and two of its three acceptors promised. Two of \
             three acceptors are a Phase-1 quorum of that set."
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
An acceptor set belongs to one ballot, and no node edits that set. If you \
edit it under a live ballot, every open quorum count changes its meaning. The \
map that the matchmakers hold from a ballot to a set also becomes wrong. Only \
one method changes the acceptors. The leader selects a **fresh ballot** and \
registers the new set with that ballot, so a reconfiguration is a round \
change.

That method has a cost and a shape. The cost is a stall: the leader abandons \
its open rounds and admits no new command. It leads again after one \
matchmaking round trip and one Phase 1. The shape is that the Phase 1 of the \
new ballot must cover the **old** set, which the matchmakers now report. Its \
Phase 2 addresses the new set alone.

The leader sends the `Prepare` to a node that joins as well. That node \
therefore promises the ballot and learns the set before any `Accept` reaches \
it. A node that leaves the set still answers Phase 1 for the ballots that it \
took part in. A removed node is not a node that is off. A cluster with no \
matchmakers refuses a reconfiguration. It has no place to record a second \
set, so plain Multi-Paxos keeps one set permanently.

Grow the cluster onto the spare node, node 3. Then remove the leader and \
elect another node. The matchmakers tell that campaign about **two** sets: \
the set before the change and the set after it. The campaign must hold a \
Phase-1 quorum of each set. A quorum of every node that the two sets name \
together is a different claim. The card asks you which claim Phase 1 needs.",
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
    setup: || deployed(&[0, 1, 2, 3], &[0, 1, 2], &[0, 1], &[], 0),
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
                "Node {} leads a set of {members}. Ask it to run with the acceptors 0, 1, 2 \
                 and 3.",
                leader.0
            ));
        }
        let prior = log.prior_configurations(leader);
        if prior.len() < 2 {
            return GoalStatus::Open(format!(
                "The new set is in force. Now remove node {} and elect another node. The \
                 matchmakers tell that campaign about both sets.",
                leader.0
            ));
        }
        // Which command that is belongs to the client, not to this level: the
        // last value it asked for is the one the new set decided.
        let latest = log.proposed_values().last().cloned();
        let executed = latest
            .as_ref()
            .is_some_and(|value| commands(world, 3).iter().any(|command| command == value));
        if !executed {
            return GoalStatus::Open(
                "Get one more command chosen under the new set, and let node 3 execute it."
                    .to_string(),
            );
        }
        GoalStatus::Reached(format!(
            "Node {} leads a set of four at ballot {}, which is above the ballot that bound \
             the set of three. Node 3 promised that ballot before any Accept reached it, and it \
             executed the command chosen under the new set. The campaign that elected node {} \
             had to hold a Phase-1 quorum of both sets. A quorum of the {} acceptors that they \
             name together is not enough.",
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
            "The set of three needs a promise from two of its own members. Check that set \
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
Every reconfiguration adds a set that the matchmakers must keep, and every \
later Phase 1 must cover every set that they report. Without a rule, the \
elections get slower and slower, and an operator cannot stop a removed \
machine. A set must therefore become forgettable, and the condition is exact. \
A set may be forgotten only when **no future leader needs its Phase-1 quorum \
to learn a value that its Phase-2 quorum possibly chose**.

A leader can prove that condition about the log that it holds. Above its \
election fence, its own Phase 1 reported that no acceptor accepted anything, \
so no older set matters there. Between the fence and its applied prefix, the \
leader re-proposed or filled every slot under its own ballot and set. Below \
the prefix, a node that learns that a slot is chosen writes the value as its \
accepted record. A member of the current set therefore answers a later Phase \
1 with that value.

The condition is therefore this. The leadership must be settled. A **Phase-2 \
quorum of the current set must report a chosen index at or above the \
fence**. The leader then asks the matchmakers to raise their floor to its own \
ballot. The floor is in force only after a matchmaker **quorum** writes it to \
disk.

Only then may an operator stop a removed acceptor, and the request must carry \
the floor as evidence. The statement \"I am not in the set in force\" is a \
belief, and the node loses that belief at every crash. A floor above every \
ballot that named this node in a set is a fact. Take the cluster down to \
three acceptors, and beat until the floor is in force. Then answer for node 3 \
twice: once with no evidence, and once with the watermark that the leader \
reports.",
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
    setup: || deployed(&[0, 1, 2, 3], &[0, 1, 2, 3], &[0, 1], &[], 0),
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
                        "The floor {} is in force, and it releases node 3. Ask node 3 to \
                         retire, and give it that watermark.",
                        crate::view::show_ballot(watermark)
                    ))
                }
                _ => GoalStatus::Open(
                    "Take node 3 out of the acceptor set. Then beat until a matchmaker quorum \
                     holds the floor."
                        .to_string(),
                ),
            };
        }
        if !refused {
            return GoalStatus::Failed(
                "Node 3 retired, and the leader refused no request for a lack of evidence. \
                 This level is about that refusal."
                    .to_string(),
            );
        }
        // The evidence itself, and not only the outcome: the watermark the
        // shutdown rested on must be a floor a leadership really reports. A
        // number that no leader reports is not evidence, whatever it is above.
        let Some((_, watermark)) = log
            .retirements()
            .iter()
            .copied()
            .find(|(id, _)| *id == NodeId(3))
        else {
            return GoalStatus::Failed(
                "Node 3 is retired, and the world recorded no watermark for it.".to_string(),
            );
        };
        if !log.reports_gc_floor(watermark) {
            return GoalStatus::Failed(format!(
                "Node 3 retired on the watermark {}, and no leader reports that floor. An \
                 operator reads a floor from a leader that made it effective.",
                crate::view::show_ballot(watermark)
            ));
        }
        GoalStatus::Reached(format!(
            "Node 3 is retired, on the floor {}. The leader reports that floor, and a \
             matchmaker quorum wrote it to disk. The first request carried no such evidence, and \
             node 3 refused it with \"not collected\". An installed successor set does not mean \
             a collected predecessor. That floor is above every ballot that bound a set that \
             names node 3.",
            crate::view::show_ballot(watermark)
        ))
    },
    hint: |world, mistakes| {
        let effective = log_world(world)
            .and_then(|log| log.leader().and_then(|leader| log.gc_effective(leader)));
        (mistakes > 0).then(|| match effective {
            Some(_) => "The leader now reports a floor. Compare that floor with the ballots \
                        that bound the sets which named node 3."
                .to_string(),
            None => "No floor is in force yet. A node refuses a request that carries no \
                     evidence."
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
The matchmakers now hold the true membership. Their own membership cannot \
stay fixed permanently, because one matchmaker loses a disk and needs a \
replacement. A higher tier of matchmakers for the matchmakers has no end. The \
answer is that the matchmaker set carries a **generation**. Single-decree \
Paxos chooses generation `g + 1`, and its acceptors are the members of \
generation `g`. That decree is Act I again: the same proposer, the same \
acceptor and one slot, with a list of matchmakers as its value.

The handover has five steps, and their order is the argument. **Stop**: a \
quorum of the old generation freezes its state on disk. A frozen matchmaker \
registers nothing more, so the next copy does not change while the handover \
reads it.

**Reconstruct**: take the highest floor that the frozen members report, and \
the union of their registries above that floor. **Bootstrap**: every proposed \
member writes that reconstruction to disk and marks it pending. **Decide**: \
the decree chooses one successor, so two operators that compete cannot \
install two sets. **Publish**: the old members record the link and send late \
proposers to it, and the new members activate the pending set.

Every message names its generation, and a matchmaker answers only its own \
generation. Replace matchmaker 2 with matchmaker 3. A campaign that started \
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
    setup: || deployed(&[0, 1, 2], &[0, 1, 2], &[0, 1, 2], &[3], HANDOVER_STALL),
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
                 bootstrap, decide and publish.",
                active.len()
            ));
        }
        // The node that registered through generation 1 must be the leader
        // itself. Another node's completed campaign proves nothing about this
        // leadership: `H_b` is per campaign, and a campaign that a node
        // abandoned or lost took its own registration with it.
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
        if registered != Some(leader) {
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
            "Generation 1 = {active:?} serves matchmaking at every member. The matchmaker that \
             it replaced stays up, and it sends late candidates to the new generation. The \
             cluster elected node {} through the new generation, and a command is chosen under \
             that leadership. A decree over the generation that it replaced chose one successor.",
            leader.0
        ))
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the second line of the card. It says which generation this matchmaker holds, \
             and whether the matchmaker is frozen."
                .to_string(),
        ),
        _ => Some(
            "This matchmaker is frozen for the generation that the request names. A frozen \
             matchmaker registers nothing more, and it answers with the successor that it \
             knows."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act4/matchmaker-generations");
        script.play(start_election(0)).settle_all();
        // Node 1 opens a campaign against generation 0 and its registrations
        // stay in flight: this is the straggler the fence will refuse.
        script.play(start_election(1));
        // Matchmaker 3 is down, and a set is bootstrapped at **every** member
        // it names, so this handover can never leave that step. The node
        // driving it gives it up after the stall timeout, and it must: a
        // proposed member that never answers would otherwise hold a busy
        // refusal for the rest of the run. Nothing is lost — the freeze is on
        // the old members' own disks.
        script.play(Action::CrashMatchmaker { matchmaker: 3 });
        script.play(Action::ReconfigureMatchmakers {
            node: 0,
            members: vec![0, 1, 3],
        });
        for _ in 0..8 {
            script.settle(phase("reconfigure"));
            script.play(tick(0));
        }
        // Matchmaker 3 comes back, and the handover is asked for again. It
        // re-freezes members that are already frozen, which changes nothing,
        // and this time every proposed member holds the bootstrap.
        script.play(Action::RestartMatchmaker { matchmaker: 3 });
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
A disk loses data one block at a time. A node comes back, and one accepted \
record does not read: the value is gone, but the slot number and the ballot \
are still there. The node has three correct answers about that slot, not two. \
It voted and it holds the value. It did not vote. It voted, and it does not \
know the value any more.

The third answer must stay a separate answer. A node that changes it into \"I \
did not vote\" makes the mistake that this level shows. A candidate that hears \
nothing from a whole quorum decides that nothing was chosen there, and it \
decides another value. If the cluster already chose the lost value, that slot \
now holds two values. A node therefore reports a damaged record as damaged, \
and a candidate that receives one cannot settle the slot from that report \
alone.

More answers settle the slot: the leader keeps asking the acceptors that did \
not reply, and each reply puts the slot into one of three cases. First: an \
acceptor reports a value at a ballot at or above the damaged record, so the \
leader re-proposes that value. The damaged acceptor then writes the value \
back when it votes. Second: a whole Phase-1 quorum reports nothing that can \
hide a chosen value, so the leader decides a `Noop`. Third: neither case \
holds yet, so the leader waits. Damage one record here, start the node again, \
and say which case each report gives the slot.",
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
        // The value the damaged record held is the one the client asked for
        // first. The level reads it back rather than naming it: a repair is
        // correct when the slot holds *the value that was voted for*, and any
        // other value in that slot is a second value for one slot.
        let voted = log.proposed_values().first().cloned();
        match (damaged, blocked, repaired, value) {
            (None, 0, true, Some(value)) if Some(&value) == voted.as_ref() => {
                GoalStatus::Reached(format!(
                    "Slot 0 holds {value}. The damaged acceptor voted for that value, and then \
                     it could not read the value. Every node can read the record again, because \
                     the acceptor wrote the value back when it voted for the proposal of the \
                     leader."
                ))
            }
            (None, 0, true, Some(value)) => GoalStatus::Failed(format!(
                "Slot 0 holds {value}. The repair re-proposed a value that no acceptor \
                 reported for that slot."
            )),
            (Some(id), _, _, _) => GoalStatus::Open(format!(
                "Node {} still holds a record that it cannot read. Elect a leader, and let it \
                 collect enough answers to settle that slot.",
                id.0
            )),
            _ => GoalStatus::Open(
                "Damage one accepted record, start the node again, and elect a leader.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the second line of the card. It says what the answers hold for that slot, \
             and this ballot may put nothing else there."
                .to_string(),
        ),
        _ => Some(
            "One acceptor reports the value that it accepted, at a ballot at or above the \
             damaged record. Re-propose that value, because a `Noop` decides another value."
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
A crash costs a node every item in memory, and no item on disk. paros \
therefore keeps the leadership in memory and the promises on disk. The crash \
is an abdication, and the promise comes back. A lost **disk** is a different \
failure, and it has a different answer.

A node must not take back a promise. When it promises a ballot, it tells a \
proposer that every lower ballot is finished there, and that proposer \
possibly chose a value from that answer. A node that boots with an empty disk \
holds no record of that promise. It answers a lower ballot and votes for the \
value of that ballot. A quorum at the older ballot then chooses a second \
value for a slot that already holds one. A snapshot does not help, because a \
snapshot restores the log and the peer that sends it does not know what this \
node promised.

A node that lost its disk therefore does not rejoin: the library refuses the \
boot, and it does not depend on the memory of an operator. Every store \
carries a marker that says that an operator formatted it. The record of the \
operator says that this identity was provisioned once. A store that was \
provisioned and no longer carries its marker is a lost disk, and the library \
refuses that boot. Erase a disk here, try to start the node again, and let \
the other nodes keep deciding. A change of the acceptor set heals the \
cluster, and the next levels cover that change.",
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
                "The durable promise of node {} came back lower than a promise that it \
                 already made.",
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
                    "The disk of node {} is empty. Try to start the node again.",
                    id.0
                )),
                None => GoalStatus::Open("Erase the disk of one node.".to_string()),
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
                "Node {} stays out of the cluster. Get one more command chosen without it.",
                wiped.0
            ));
        }
        GoalStatus::Reached(format!(
            "The library refuses node {}, and the other two nodes chose another command \
             without it. Its disk no longer holds the promise {}.{} that it made, and no node in \
             the cluster can give that promise back. The acceptor set must change instead.",
            wiped.0, promised.round, promised.node.0
        ))
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "An empty disk and a new disk look the same inside the node. Only the record of \
             the operator separates them, because it says that this identity was provisioned \
             once."
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
