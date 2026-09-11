//! Act III — truncation, snapshots, and reads.
//!
//! Six levels over the **log world**, and one question runs through all of
//! them: what does a node know, and what is it entitled to *say*? Act II built
//! a log that grows. This act makes it shrink — which creates a node nothing
//! can replay to — and then makes it answer questions, which is where the gap
//! between "chosen", "applied" and "acknowledged" stops being pedantry and
//! starts being the difference between a correct database and a lying one.
//!
//! The order is the order the mechanisms depend on each other: truncation
//! first (it is what strands a node), then the snapshot that rescues it, then
//! the read path — the confirmation round, the fresh-leader fence, the
//! client-visible property all of it exists for — and finally the write ack,
//! which is the other half of that property.
//!
//! Every reference solution here is **recorded**, not written: a private
//! `Script` plays the level, choosing messages by what they are rather than by
//! id and answering every prompt with the answer `paros-core` itself gives.

use paros_core::{Config, NodeId, QuorumSystem, Slot};

use crate::action::{Action, ActionKind};
use crate::auto::AutomationFlag;
use crate::level::script::{Script, kind, to};
use crate::level::{GoalStatus, Level, WorldKind};
use crate::view::{MessageView, show_command};
use crate::world::{Disk, RetryAnswer, World};

/// Act III's levels, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &TRUNCATE_BY_CONSENSUS,
        &THE_STRANDED_NODE,
        &READ_INDEX,
        &THE_FRESH_LEADER_TRAP,
        &LINEARIZABLE_OR_NOT,
        &CHOSEN_IS_NOT_APPLIED,
    ]
}

/// The client every single-client Act III level gives the player.
const CLIENT: u64 = 7;

/// The second client, for the linearizability level. Two clients is the
/// smallest history in which "before" and "after" are not the same party's
/// program order.
const OTHER: u64 = 8;

/// The election timeout every Act III node starts with, in ticks.
const TIMEOUT: u64 = 5;

/// Every role answered for the player. A level removes exactly the one it
/// teaches.
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
];

/// Every role but the one a level teaches. Spelled out per level, as Act II
/// does: a helper that removed one entry could not be a `const`, and a level's
/// automation set is data.
const NO_SNAPSHOT_PROMISE: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::AckWrite,
];

/// Every role but serving a read.
const NO_READ_SERVE: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::SnapshotPromise,
    AutomationFlag::AckWrite,
];

/// Every role but answering a client's retry.
const NO_ACK_WRITE: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::SnapshotPromise,
];

/// The convenience toggles every Act III level offers.
const TOGGLES: &[AutomationFlag] = &[
    AutomationFlag::DeliverReplies,
    AutomationFlag::DeliverHeartbeats,
];

// ---- worlds -----------------------------------------------------------------

fn peers(size: u64) -> Vec<NodeId> {
    (0..size).map(NodeId).collect()
}

fn config(id: NodeId, size: u64) -> Config {
    Config {
        id,
        peers: peers(size),
        quorum_system: QuorumSystem::Majority,
        ..Config::default()
    }
}

/// A cluster of `size` fresh nodes with `clients` clients.
fn fresh(size: u64, clients: &[u64]) -> WorldKind {
    let disks = peers(size)
        .into_iter()
        .map(|id| Disk::new(config(id, size)))
        .collect();
    WorldKind::Log(Box::new(World::from_disks(disks, clients, TIMEOUT)))
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

/// A watermark as the goals write it.
fn at(slot: Option<Slot>) -> String {
    slot.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0))
}

/// Every node's compaction floor, deduplicated — one entry means one
/// cluster-wide floor.
fn distinct_floors(world: &WorldKind) -> Vec<u64> {
    let Some(log) = log_world(world) else {
        return Vec::new();
    };
    let mut floors: Vec<u64> = log.floors().iter().map(|(_, first)| first.0).collect();
    floors.sort_unstable();
    floors.dedup();
    floors
}

// ---- action shorthands ------------------------------------------------------

fn start_election(node: u64) -> Action {
    Action::StartElection { node }
}

fn propose(client: u64, node: u64, value: &str) -> Action {
    Action::Propose {
        node,
        client,
        value: value.to_string(),
        column: None,
    }
}

fn compact(node: u64, up_to: u64) -> Action {
    Action::Compact { node, up_to }
}

fn read_index(client: u64, node: u64) -> Action {
    Action::ReadIndex {
        node,
        client: Some(client),
    }
}

fn retry(client: u64, node: u64, seq: u64) -> Action {
    Action::Retry { node, client, seq }
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

/// The Phase-2 traffic of one slot: its `Accept`s, its `Accepted`s and the
/// `Commit`s that report the decision.
fn slot_traffic(slot: u64) -> impl Fn(&MessageView) -> bool {
    move |message| {
        matches!(message.kind.as_str(), "Accept" | "Accepted" | "Commit")
            && message.slot == Some(slot)
    }
}

/// Everything that is not Phase-2 traffic — the elections, the beats, the
/// catch-up, the snapshots.
fn not_phase2(message: &MessageView) -> bool {
    !matches!(message.kind.as_str(), "Accept" | "Accepted" | "Commit")
}

// ---- 14. truncate by consensus ----------------------------------------------

const TRUNCATE_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Compact,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act3/truncate-by-consensus`.
pub static TRUNCATE_BY_CONSENSUS: Level = Level {
    id: "act3/truncate-by-consensus",
    act: 3,
    title: "Truncate by consensus",
    briefing: "\
A log that only grows fills the disk, so a real system removes the old prefix. \
One method is wrong: each node prunes its own log when it wants to. Two nodes \
then disagree about how far they pruned. A `Prepare` from a slow proposer then \
reaches a peer that deleted the slot in the question. That peer answers \
\"nothing accepted there\" when the true answer is \"I do not know any more\". Two \
values can then get chosen for one slot.

paros therefore makes the floor a **decided value**. A log slot holds a \
`Command`, and a command is either the opaque bytes of the client or one control \
command of paros. `Truncate{up_to}` is one of those control commands. The leader \
proposes it, and the acceptors vote for it as they vote for a client value. \
Every node drops its prefix when it applies that slot. The result is one \
cluster-wide floor, carried by ordinary replication, with no separate broadcast \
and no separate agreement protocol.

The leader enforces a coupling rule here, and that rule is the reason for the \
two steps of this level. Below the floor the entries are gone on every disk, so \
only a **snapshot** can recover a node that was away. A snapshot that no node \
holds recovers no node. The leader therefore proposes a `Truncate` only after a \
quorum holds a decided snapshot point that covers it. If you ask for a \
compaction before that point exists, the leader refuses you and seeds a snapshot \
point, and your second request succeeds. Look at the floor on every node: each \
floor moves when that node applies the decision.",
    field_guide: "truncation-and-snapshots.html",
    symbols: &[
        "Control::Truncate",
        "Control::Snap",
        "ColocatedNode::propose_control",
        "ColocatedNode::compact",
        "WriteOp::Truncate",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: TRUNCATE_ACTIONS,
    setup: || fresh(3, &[CLIENT]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let refused = log.compacts().iter().any(|outcome| !outcome.accepted);
        let accepted = log.compacts().iter().any(|outcome| outcome.accepted);
        let floors = distinct_floors(world);
        let stranded = log.stranded();
        if !stranded.is_empty() {
            return GoalStatus::Failed(format!(
                "node {} is below the floor of the cluster, and every disk deleted the slots \
                 that it still needs.",
                stranded[0].0
            ));
        }
        match (refused, accepted, floors.as_slice()) {
            (_, true, [first]) if *first > 0 => GoalStatus::Reached(format!(
                "The floor of every node is slot {first}, and no node is stranded. No message \
                 sent that number. Each node computed it when it applied the same decided \
                 command, at the same place in the same log."
            )),
            (false, _, _) => GoalStatus::Open(
                "Ask the leader to compact the log. Look closely at the first answer.".to_string(),
            ),
            (_, false, _) => GoalStatus::Open(
                "The leader refused you and seeded a snapshot point. Get that point decided, \
                 then ask again."
                    .to_string(),
            ),
            (_, _, floors) => GoalStatus::Open(format!(
                "The floors are still {floors:?}. Deliver the decision to every node. Each \
                 node truncates when it *applies* that slot, not when the leader proposes it."
            )),
        }
    },
    hint: |world, mistakes| {
        let refused = log_world(world)
            .is_some_and(|log| log.compacts().iter().any(|outcome| !outcome.accepted));
        (mistakes > 0 || refused).then(|| {
            "A refusal is not a failure here. The leader does not drop a prefix that no \
             quorum can replace with a snapshot, so it seeds a snapshot point. Deliver that \
             decision, then ask to compact again."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act3/truncate-by-consensus");
        script.play(start_election(0)).settle_all();
        script
            .play(propose(CLIENT, 0, "alpha"))
            .play(propose(CLIENT, 0, "bravo"))
            .settle_all();
        // No decided snapshot point exists yet, so this is refused — and the
        // refusal seeds the marker that makes the retry work.
        script.play(compact(0, 8)).settle_all();
        script.play(compact(0, 8)).settle_all();
        script.finish()
    },
};

// ---- 15. the stranded node ---------------------------------------------------

const STRANDED_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Compact,
    ActionKind::Crash,
    ActionKind::Restart,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act3/the-stranded-node`.
pub static THE_STRANDED_NODE: Level = Level {
    id: "act3/the-stranded-node",
    act: 3,
    title: "The stranded node",
    briefing: "\
Truncation creates a node that no ordinary message can help. Crash node 2, and \
let the cluster decide more slots and truncate past its position. Then start \
node 2 again: it needs slots that no disk holds. Catch-up replays what a peer \
still holds, and it cannot replay what every node deleted. The acceptors are \
also right to refuse a `Prepare` about a truncated range. Such a peer would \
report \"nothing accepted\" when the true answer is \"I do not know any more\", and \
the floor guard prevents that report.

paros performs one kind of state transfer, and it is the answer here. When a \
peer sees a catch-up request below its floor, it offers a **snapshot**. The \
snapshot is the opaque application state at the chosen prefix of that peer. The \
application produced those bytes, and paros sends them without a read of any \
byte. The receiver moves its chosen prefix to the boundary of the snapshot, \
compacts everything below it, and installs the state.

One rule is the subject of this level: a snapshot restores the **log** — the \
values, the prefix and the state of the application. It says nothing about \
**promises**, and the peer that sent it does not know what this node promised. \
The node that installs the snapshot therefore keeps the *higher* of its own \
promise and the ballot of the snapshot. Node 2 comes back, hears no leader, and \
campaigns. That campaign pulls the snapshot, and it raises the promise above the \
ballot of the prefix. The game then asks you for its promise, and a wrong answer \
lets the node vote for a ballot that it refused.",
    field_guide: "truncation-and-snapshots.html",
    symbols: &[
        "Message::InstallSnapshot",
        "Ready::snapshot_offers",
        "Acceptor::install",
        "WriteOp::InstallSnapshot",
    ],
    automation_on: NO_SNAPSHOT_PROMISE,
    pinned_off: &[AutomationFlag::SnapshotPromise],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::SnapshotPromise],
    allowed_actions: STRANDED_ACTIONS,
    setup: || fresh(3, &[CLIENT]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Some(node) = log.promise_regressed() {
            return GoalStatus::Failed(format!(
                "The durable promise of node {} came back lower than a promise that it \
                 already made. A snapshot restores the log, not a promise.",
                node.0
            ));
        }
        let healed = applied(world, 2);
        let leader_log = applied(world, 0);
        let floor = log.disk(NodeId(2)).map_or(0, |disk| disk.floor().0);
        if leader_log.is_empty() {
            return GoalStatus::Open(
                "Get some commands chosen through the two nodes that are up.".to_string(),
            );
        }
        if floor == 0 {
            return GoalStatus::Open(
                "Start node 2 again, and let it find that it is below the floor. It campaigns \
                 when it hears no leader, and that campaign asks its peers for the range that \
                 it misses."
                    .to_string(),
            );
        }
        if healed == leader_log {
            GoalStatus::Reached(format!(
                "Node 2 is back with the whole prefix ({}), and it kept its promise. No peer \
                 replayed those slots, because they do not exist any more. A peer gave node 2 \
                 the state of the application and the boundary slot.",
                healed.join(", ")
            ))
        } else {
            GoalStatus::Open(format!(
                "Node 2 executed {healed:?}, and the cluster executed {leader_log:?}."
            ))
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Compare the two ballots on the card. One of them is a promise that this node \
             made, and no other node knows about it."
                .to_string(),
        ),
        _ => Some(
            "Keep the higher of the two ballots. A node must not take back a promise, and the \
             peer that sent the snapshot does not know what this node promised."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act3/the-stranded-node");
        // Node 2 is away for the whole of the cluster's progress.
        script.play(crash(2));
        script.play(start_election(0)).settle_all();
        script
            .play(propose(CLIENT, 0, "alpha"))
            .play(propose(CLIENT, 0, "bravo"))
            .settle_all();
        // Seed a snapshot point, then truncate past node 2's position.
        script.play(compact(0, 8)).settle_all();
        script.play(compact(0, 8)).settle_all();
        // Node 2 comes back and campaigns: it has heard from no leader, and the
        // campaign broadcasts the catch-up request that finds it below the
        // floor. Its Prepares are dropped — this level is not about an
        // election, and node 2 is in no state to win one.
        script.play(restart(2));
        for _ in 0..TIMEOUT {
            script.play(tick(2));
        }
        script.drop_all(kind("Prepare"));
        script.settle(kind("CatchUpRequest"));
        script.settle(kind("InstallSnapshot"));
        script.answer_all();
        script.finish()
    },
};

// ---- 16. read-index ----------------------------------------------------------

const READ_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::ReadIndex,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act3/read-index`.
pub static READ_INDEX: Level = Level {
    id: "act3/read-index",
    act: 3,
    title: "Read-index",
    briefing: "\
One correct read needs no new protocol: propose a no-op through ordinary \
consensus, and answer at its slot. The commit proves that the proposer was the \
leader at that moment, at a quorum. That read also costs a log slot, one flush \
on every acceptor and a full round trip, for each read. A system with many reads \
would write nothing useful to its disks.

Read-index keeps the proof and removes the write. The slot of the no-op was \
never important; only the evidence of current leadership was, and a heartbeat \
carries that evidence. **Capture** the applied watermark as the read index. \
**Confirm** the leadership: send a beat, and collect the acks of a quorum. \
**Serve** the read after the applied prefix covers the captured index. Quorum \
intersection does the rest: a higher ballot that committed anything holds a \
promise quorum, and one member of our ack quorum refuses our ballot.

One detail carries the safety, and this level puts it in your hands. An ack \
counts only if it names the current ballot of the leader. It must also name a \
beat sequence at or after the beat that was sent when the read started. An ack \
to an older beat proves nothing: the follower possibly sent it and then promised \
a higher ballot elsewhere, so you would read the past. One such stale ack is in \
flight here, from a beat sent before the client asked. Deliver it, and decide \
whether it is proof.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "ColocatedNode::read_index",
        "Proposer::open_read",
        "Proposer::credit_read_ack",
        "Proposer::confirm_reads",
        "ReadState",
    ],
    automation_on: NO_READ_SERVE,
    pinned_off: &[AutomationFlag::ReadServe, AutomationFlag::DeliverHeartbeats],
    unlocked: &[AutomationFlag::DeliverReplies],
    unlocks: &[AutomationFlag::ReadServe],
    allowed_actions: READ_ACTIONS,
    setup: || fresh(3, &[CLIENT]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Err(detail) = log.linearizable() {
            return GoalStatus::Failed(detail);
        }
        let acked = log.highest_acked_slot();
        let served: Vec<Option<Slot>> = log
            .reads()
            .into_iter()
            .filter(|(_, _, served)| *served)
            .map(|(_, index, _)| index)
            .collect();
        match served.first() {
            Some(index) if *index >= acked && acked.is_some() => GoalStatus::Reached(format!(
                "The cluster served the read at {}, at or above the last acknowledged write \
                 ({}). The read cost one round of beats and no byte of log.",
                at(*index),
                at(acked)
            )),
            Some(index) => GoalStatus::Open(format!(
                "The cluster served a read at {}, but the level asks for an acknowledged \
                 write below it first.",
                at(*index)
            )),
            None => GoalStatus::Open(
                "Get a command chosen. Then ask for a read, and decide when the proof is \
                 complete."
                    .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Look at the beat that the ack names. The number of acks is not the question."
                .to_string(),
        ),
        _ => Some(
            "An ack to a beat sent *before* the read started proves nothing, because the \
             follower can promise a higher ballot after it sends the ack. Wait for an ack to \
             the beat that the read itself caused."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act3/read-index");
        script.play(start_election(0)).settle_all();
        script.play(propose(CLIENT, 0, "alpha")).settle_all();
        // A beat *before* the read: its ack is the stale one.
        script.play(tick(0));
        script.settle(|message| message.kind == "Heartbeat");
        // The read captures the watermark and beats again.
        script.play(read_index(CLIENT, 0));
        // The stale ack lands first and proves nothing.
        script.settle(|message| message.kind == "HeartbeatAck" && message.sent_at == 1);
        script.answer_all();
        // Now the beat the read itself triggered, and its ack.
        script.settle_all();
        script.finish()
    },
};

// ---- 17. the fresh-leader trap ----------------------------------------------

const TRAP_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::ReadIndex,
    ActionKind::Crash,
    ActionKind::Restart,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act3/the-fresh-leader-trap`.
pub static THE_FRESH_LEADER_TRAP: Level = Level {
    id: "act3/the-fresh-leader-trap",
    act: 3,
    title: "The fresh-leader trap",
    briefing: "\
The confirmation round is not enough, and that is the difficult half of the \
read path. A leader that just won an election holds a valid quorum. Every ack \
that it collects is real, at its own current ballot, at this moment. Its applied \
prefix can still miss writes that the last leader acknowledged to a client. \
Election recovery re-proposes those slots, and until they decide again, the \
local state of the new leader does not hold those writes.

Capture and confirm alone would serve that old watermark with a fresh quorum, \
and the client would lose a write that the cluster called durable. Raft commits \
a no-op in the new term before it serves a read, but paros waits instead. At the \
moment that it wins, a leader records a **read floor**: the highest slot that \
its promise quorum reported. By quorum intersection that floor is at or above \
every write that an earlier leader acknowledged. A read captures the *maximum* \
of the applied watermark and that floor. It confirms only when the ack quorum is \
complete and the applied prefix covers the captured index.

In this level one other node accepted one command from the old leader. The old \
leader then stopped before the decision came back, so nothing is chosen and no \
node applied anything. The Phase 1 of the new leader finds that value and must \
re-propose it. Ask for a read while the recovery is still in flight: the acks \
arrive, but the answer is still no. Then let the slot decide again, and the read \
completes on its own, in the batch that applied the slot.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "Proposer::read_floor",
        "Proposer::confirm_reads",
        "Replica::covers",
        "RecoveryStep::Recovered",
    ],
    automation_on: NO_READ_SERVE,
    pinned_off: &[AutomationFlag::ReadServe],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: TRAP_ACTIONS,
    setup: || fresh(3, &[CLIENT]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Err(detail) = log.linearizable() {
            return GoalStatus::Failed(detail);
        }
        let served: Vec<Option<Slot>> = log
            .reads()
            .into_iter()
            .filter(|(_, _, served)| *served)
            .map(|(_, index, _)| index)
            .collect();
        // The inherited value is whatever the client asked for, and the level
        // reads it back from the history rather than naming it here.
        let executed = applied(world, 1);
        let asked = log.proposed_values();
        let recovered = !asked.is_empty() && asked.iter().all(|value| executed.contains(value));
        match (served.first(), recovered) {
            (Some(index), true) => GoalStatus::Reached(format!(
                "The leader refused the read while the recovered slot was still in flight. It \
                 served the read at {} after the slot decided again. The quorum was not the \
                 missing part. The applied prefix was.",
                at(*index)
            )),
            (None, _) => GoalStatus::Open(
                "Ask the new leader for a read, and answer for it. Then let its recovered slot \
                 finish."
                    .to_string(),
            ),
            (Some(_), false) => GoalStatus::Open(
                "The cluster served the read, but no node executed the inherited value."
                    .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "The acks are not the question. Compare the index that the read captured with the \
             applied prefix below it."
                .to_string(),
        ),
        _ => Some(
            "The read floor of a new leader is the highest slot that its promise quorum \
             reported. That slot is above every slot that the leader applied. Wait, because \
             the read completes by itself when the recovered slot decides."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act3/the-fresh-leader-trap");
        script.play(start_election(0)).settle_all();
        // One command reaches exactly one other node, and its Accepted never
        // comes back: accepted somewhere, chosen nowhere.
        script.play(propose(CLIENT, 0, "alpha"));
        script.settle(|message| message.kind == "Accept" && message.to == 1);
        script.drop_all(|message| matches!(message.kind.as_str(), "Accept" | "Accepted"));
        // The leadership dies with the round that would have re-sent it.
        script.play(crash(0));
        // Node 1 campaigns with node 2. Its Phase 1 finds the value at node 1
        // itself, so its read floor sits above everything it has applied.
        script.play(start_election(1));
        script.settle(not_phase2);
        // The read: quorum in hand, applied prefix still empty.
        script.play(read_index(CLIENT, 1));
        script.settle(|message| matches!(message.kind.as_str(), "Heartbeat" | "HeartbeatAck"));
        script.answer_all();
        // Now let the recovered slot decide; the read fires with the batch that
        // applies it.
        script.settle_all();
        script.finish()
    },
};

// ---- 18. linearizable or not ------------------------------------------------

const HISTORY_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::ReadIndex,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act3/linearizable-or-not`.
pub static LINEARIZABLE_OR_NOT: Level = Level {
    id: "act3/linearizable-or-not",
    act: 3,
    title: "Linearizable or not",
    briefing: "\
Every mechanism in this act serves one client-visible property, and this level \
produces it and judges it. **Linearizable** means that every operation takes \
effect at one instant between the request and the answer. The register under \
observation here is the applied log prefix. A write adds to that prefix at its \
committed slot, and a read observes its watermark.

The log gives a total order to the writes, so a check of a recorded history \
needs no search. Three conditions over the program order of the clients are the \
whole test. One: a committed read observes every write acknowledged before the \
read started. Two: a watermark does not go down across two reads that do not \
overlap. Three: a write issued after a committed read takes a slot *above* the \
watermark of that read. Operations that never completed constrain nothing, so a \
write that timed out may still commit later, and that result is not a violation.

This level has two clients and one leader change. Get a write acknowledged under \
the old leadership, and elect a new leader without the knowledge of the old one. \
Then read across the change: the read of the second client must observe the \
write of the first client, even though a different node answers it. **Then try \
to break the property:** ask the replaced leader, which still believes that it \
leads, for a read. It collects nothing, because every follower that promised the \
new ballot refuses to ack the old ballot. That read stays open for the rest of \
the level, and the protocol is correct to refuse a read that it cannot prove.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "ReadState",
        "Proposer::confirm_reads",
        "Proposer::credit_read_ack",
        "ColocatedNode::read_index",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: HISTORY_ACTIONS,
    setup: || fresh(3, &[CLIENT, OTHER]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Err(detail) = log.linearizable() {
            return GoalStatus::Failed(format!("The history is not linearizable: {detail}"));
        }
        let history = log.history();
        let acked_writes: Vec<_> = history
            .iter()
            .filter(|op| op.write && op.completed.is_some())
            .collect();
        // A read that completed *after* a write acknowledged at another node
        // completed, and observed it: the read across the leader change.
        let across = history.iter().any(|read| {
            !read.write
                && read.completed.is_some()
                && acked_writes.iter().any(|write| {
                    write.node != read.node
                        && write.completed < Some(read.started)
                        && write.at <= read.at
                        && write.at.is_some()
                })
        });
        let refused = history
            .iter()
            .filter(|op| !op.write && op.completed.is_none())
            .count();
        match (across, refused) {
            (true, 1..) => GoalStatus::Reached(format!(
                "The history is linearizable. A read at one node observed a write that \
                 another node acknowledged before the read started, across a leader change. {} \
                 read that the protocol cannot prove is still open, and the protocol must keep \
                 it open.",
                if refused == 1 {
                    "one".to_string()
                } else {
                    refused.to_string()
                }
            )),
            (false, _) => GoalStatus::Open(
                "Get a write acknowledged. Change the leadership without the knowledge of the \
                 old leader. Then read at the new leader."
                    .to_string(),
            ),
            (true, 0) => GoalStatus::Open(
                "Now ask the replaced leader for a read as well, and look at what it cannot \
                 do."
                .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "The read of the replaced leader must stay open. Deliver its beats, and look at \
             which nodes ack them. A follower that promised the newer ballot does not ack."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act3/linearizable-or-not");
        script.play(start_election(0)).settle_all();
        // Client 7's write is acknowledged under the old leadership.
        script.play(propose(CLIENT, 0, "alpha")).settle_all();
        // Node 1 takes the leadership with node 2; node 0 hears none of it and
        // goes on believing it leads.
        script.play(start_election(1));
        script.settle(to(2));
        script.settle(|message| message.to == 1);
        script.drop_all(to(0));
        // The deposed leader is asked for a read it will never be able to prove.
        script.play(read_index(CLIENT, 0));
        script.drop_all(to(0));
        script.settle(to(2));
        script.drop_all(to(0));
        // Client 8 writes and then reads at the leader that really leads.
        script.play(propose(OTHER, 1, "bravo"));
        script.settle(|message| message.to != 0);
        script.drop_all(to(0));
        script.play(read_index(OTHER, 1));
        script.settle(|message| message.to != 0);
        script.drop_all(to(0));
        script.finish()
    },
};

// ---- 19. chosen is not applied ----------------------------------------------

const RETRY_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Retry,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act3/chosen-is-not-applied`.
pub static CHOSEN_IS_NOT_APPLIED: Level = Level {
    id: "act3/chosen-is-not-applied",
    act: 3,
    title: "Chosen is not applied",
    briefing: "\
The read half of linearizability depends on the write half. The rule \"a read \
observes every write **acknowledged** before it started\" is worth something only \
if the ack means what it says. One word in the middle of the log has two \
meanings.

A slot is **chosen** when a quorum votes for it. That fact belongs to the \
cluster and it is permanent. The leader pipelines, so slot 6 can become chosen \
while slot 5 is still open. A slot is **applied** when this node gives it to its \
state machine, and that step is strictly in order. Slot 6 therefore waits for \
slot 5. Between those two moments the command is decided and not executed, and a \
node that acked it would promise the client an unreadable result.

A client retry arrives in that window, and the leader removes duplicates by \
`(client, seq)`. The leader keeps **two** tables: the applied commands, and the \
commands in flight. If you answer from the applied table while the command is \
only chosen, you ack a write that no node executed. If you answer that you did \
not see the command, the cluster gives a decided command a second slot and \
executes it twice. The two tables move together: the identity moves inside the \
in-flight table onto the chosen slot, and only the apply walk writes the applied \
one.

In this level the second write of the client is chosen above a hole. The client \
asks twice: once while the hole is open, and once after it closes. Answer for \
the leader both times.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "Replica::applied_at",
        "Replica::inflight_at",
        "Replica::track_inflight",
        "ProposeResult::Duplicate",
        "ProposeResult::Chosen",
    ],
    automation_on: NO_ACK_WRITE,
    pinned_off: &[AutomationFlag::AckWrite],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::AckWrite],
    allowed_actions: RETRY_ACTIONS,
    setup: || fresh(3, &[CLIENT]),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let executed = applied(world, 0);
        // At most once, for every value the client asked for: which values
        // those are is the client's business, and the history reports them.
        let asked = log.proposed_values();
        if let Some(twice) = asked
            .iter()
            .find(|value| executed.iter().filter(|command| command == value).count() > 1)
        {
            return GoalStatus::Failed(format!(
                "The cluster executed the command {twice} twice. A retry that misses both dedup \
                 tables gets a fresh slot, and the command runs a second time."
            ));
        }
        let all_executed = !asked.is_empty()
            && asked
                .iter()
                .all(|value| executed.iter().any(|command| command == value));
        let held = log
            .retries()
            .iter()
            .any(|outcome| matches!(outcome.answer, RetryAnswer::InFlight(_)));
        let acked = log
            .retries()
            .iter()
            .any(|outcome| matches!(outcome.answer, RetryAnswer::Applied(_)));
        if log
            .retries()
            .iter()
            .any(|outcome| matches!(outcome.answer, RetryAnswer::Fresh(_)))
        {
            return GoalStatus::Failed(
                "A retry got a fresh slot. The leader did not recognise a command that it \
                 already held, so the write of the client is now in the log twice."
                    .to_string(),
            );
        }
        match (all_executed, held, acked) {
            (true, true, true) => GoalStatus::Reached(format!(
                "The cluster executed the command exactly once ({}). The leader answered both \
                 retries from the table that knew where the command was. It held the retry \
                 while the hole was open. It acknowledged the retry after the hole closed. \
                 \"Chosen\" and \"applied\" are two different facts, and a leader may acknowledge \
                 only one of them.",
                executed.join(", ")
            )),
            (true, false, _) => GoalStatus::Open(
                "Let the client ask again *while* its command is chosen above the hole. That \
                 window is the subject of this level."
                    .to_string(),
            ),
            (true, true, false) => {
                GoalStatus::Open("Now close the hole, and let the client ask again.".to_string())
            }
            _ => GoalStatus::Open(
                "Get both commands chosen, but not in order, so the second command waits above \
                 a hole. Then answer the retries of the client."
                    .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the two lines on the card. One table knows where the command *is*. The \
             other table knows that the cluster *ran* it."
                .to_string(),
        ),
        _ => Some(
            "While the hole is open, the command is chosen and not executed. It is in the \
             in-flight table, not in the applied table, so the client waits on that slot. After \
             the hole closes, the command is in the applied table, and the leader may \
             acknowledge it."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act3/chosen-is-not-applied");
        script.play(start_election(0)).settle_all();
        script
            .play(propose(CLIENT, 0, "alpha"))
            .play(propose(CLIENT, 0, "bravo"));
        // Slot 1 decides while slot 0 is still open: chosen above a hole.
        script.settle(slot_traffic(1));
        // The retry lands in the window. Chosen, not applied.
        script.play(retry(CLIENT, 0, 2)).answer_all();
        // Close the hole, then ask once more.
        script.settle_all();
        script.play(retry(CLIENT, 0, 2)).answer_all();
        script.settle_all();
        script.finish()
    },
};
