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
A log that only grows is a disk that eventually fills, so a real system throws \
the old prefix away. The obvious way to do that is the wrong one: let each node \
prune whenever it likes. Two nodes then disagree about how far they have pruned, \
and a `Prepare` from a lagging proposer lands on a peer that deleted exactly the \
slot it is asking about — a peer that answers \"nothing accepted there\" when the \
truth is \"I no longer know\". That is how two values get chosen for one slot.

So paros makes the floor a **decided value**. A log slot holds a `Command`, and a \
command is either the client's opaque bytes or one of paros's own control \
commands — and `Truncate{up_to}` is one of those. It is proposed by the leader, \
voted on by acceptors that cannot tell it apart from a client value, and every \
node drops its prefix **when it applies that slot**. One cluster-wide floor, \
forwarded by ordinary replication, with no separate broadcast and no agreement \
protocol of its own.

There is a coupling rule the leader will enforce on you here, and it is the \
reason this level has two steps. Past the floor the entries are gone \
*everywhere*, so the only thing left to rescue a node that was away is a \
**snapshot** — and a snapshot nobody holds rescues nobody. So the leader \
proposes a `Truncate` only once a quorum holds a decided snapshot point covering \
it. Ask to compact before there is one and you will be refused: the leader seeds \
a snapshot point instead, and your retry goes through. Watch the floors move on \
every node, one at a time, as each one applies the decision.",
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
                "node {} sits below the cluster's floor: the slots it still needs have been \
                 deleted everywhere.",
                stranded[0].0
            ));
        }
        match (refused, accepted, floors.as_slice()) {
            (_, true, [first]) if *first > 0 => GoalStatus::Reached(format!(
                "Every node's floor is slot {first}, and nobody is stranded. Nothing broadcast \
                 that number: each node computed it by applying the same decided command, in the \
                 same place in the same log."
            )),
            (false, _, _) => GoalStatus::Open(
                "Ask the leader to compact. The first answer is the interesting one.".to_string(),
            ),
            (_, false, _) => GoalStatus::Open(
                "The leader refused and seeded a snapshot point. Get that decided, then ask again."
                    .to_string(),
            ),
            (_, _, floors) => GoalStatus::Open(format!(
                "The floors are still {floors:?}. Deliver the decision to every node — each one \
                 truncates when it *applies* that slot, not when the leader proposed it."
            )),
        }
    },
    hint: |world, mistakes| {
        let refused = log_world(world)
            .is_some_and(|log| log.compacts().iter().any(|outcome| !outcome.accepted));
        (mistakes > 0 || refused).then(|| {
            "A refusal is not a failure here. The leader will not drop a prefix no quorum can \
             replace with a snapshot, so it seeds a snapshot point instead — deliver that \
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
Truncation creates a node nothing can help. Crash node 2, let the cluster keep \
deciding *and* keep truncating past its position, and bring it back: it needs \
slots that no longer exist on any disk anywhere. Catch-up replays what a peer \
still holds; it cannot replay what everybody deleted. And the acceptors are \
right to refuse — a peer that answered a `Prepare` about a range it has \
truncated would be reporting \"nothing accepted\" when the truth is \"I no longer \
know\", which is exactly the lie the floor guard exists to prevent.

The one piece of state transfer paros performs is the answer. When a peer sees a \
catch-up request that falls below its floor it offers a **snapshot** instead: the \
opaque application state at its own chosen prefix, which the application \
produced and which paros ships without ever reading a byte of. The receiver \
jumps its chosen prefix to the snapshot's boundary, compacts everything below \
it, and installs the state.

And then there is the line the whole level is about. A snapshot restores the \
**log** — the values, the prefix, the application's state. It says nothing about \
**promises**, and the peer that sent it has no idea what this node has sworn. So \
the installing node keeps the *higher* of its own promise and the snapshot's \
ballot, never the snapshot's alone. Node 2 comes back, hears from no leader, and \
campaigns — which is what pulls the snapshot, and which is also why its promise \
is now above the ballot the snapshot's prefix was decided under. You will be \
asked what its promise is afterwards. Get it wrong and the node is free to vote \
for a ballot it had already sworn to refuse.",
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
                "node {}'s durable promise came back lower than a promise it had already made. A \
                 snapshot restores the log, never a promise.",
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
                "Bring node 2 back and let it discover it is below the floor. It campaigns when \
                 it hears from no leader, and that campaign is what asks its peers for the range \
                 it is missing."
                    .to_string(),
            );
        }
        if healed == leader_log {
            GoalStatus::Reached(format!(
                "Node 2 is back with the whole prefix restored ({}) and its promise intact. \
                 Nothing replayed those slots to it — they do not exist any more. It was handed \
                 the application's state and told where the boundary was.",
                healed.join(", ")
            ))
        } else {
            GoalStatus::Open(format!(
                "Node 2 has executed {healed:?} and the cluster has executed {leader_log:?}."
            ))
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Compare the two ballots on the card. One of them is a promise this node made and \
             nobody else knows about."
                .to_string(),
        ),
        _ => Some(
            "Keep the higher of the two. A promise is the only thing a node may never take back, \
             and the peer that sent the snapshot has no idea what this node has sworn."
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
There is a correct read that needs no new protocol at all: propose a no-op \
through ordinary consensus and answer at its slot. The commit proves the \
proposer was leader *now*, at a quorum. It also costs a log slot, an fsync on \
every acceptor and a full round trip — per read. A read-heavy system would spend \
its disks writing nothing.

Read-index keeps the proof and drops the write, because the no-op's slot never \
mattered — only the evidence of current leadership did, and a heartbeat can \
carry that for free. Three moves: **capture** the applied watermark as the read \
index; **confirm** by broadcasting a beat and collecting acks from a quorum; \
**serve** once the applied prefix covers the captured index. Quorum \
intersection does the rest — if a higher ballot had committed anything before \
this read began, a quorum had promised that ballot, and at least one member of \
any quorum answering us would have refused ours.

The detail that carries the safety, and the one this level puts in your hands: \
an ack only counts if it echoes the leader's current ballot **and a beat \
sequence at or after the one broadcast when the read began**. An ack to an older \
beat proves nothing at all — the follower may have sent it and *then* promised a \
higher ballot somewhere else, and you would be reading the past. There is one \
such stale ack in flight here, from a beat sent before the client ever asked. \
Deliver it and decide whether it is proof.",
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
                "The read is served at {}, at or above the last acknowledged write ({}). It cost \
                 one round of beats and not one byte of log.",
                at(*index),
                at(acked)
            )),
            Some(index) => GoalStatus::Open(format!(
                "A read was served at {}, but the level wants a write acknowledged under it \
                 first.",
                at(*index)
            )),
            None => GoalStatus::Open(
                "Get a command chosen, then ask for a read and decide when the proof is complete."
                    .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => {
            Some("Look at which beat the ack echoes, not at how many acks there are.".to_string())
        }
        _ => Some(
            "An ack to a beat sent *before* the read began proves nothing: the follower could \
             have promised a higher ballot in between. Wait for an ack to the beat the read \
             itself triggered."
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
The confirmation round is not enough, and this is the sneaky half of the read \
path. A leader that *just* won an election holds a perfectly valid quorum — \
every ack it collects is real, at its own current ballot, right now. And its \
applied prefix can still be missing writes the previous leader acknowledged to a \
client. Election recovery re-proposes those slots, and until they re-decide, the \
new leader's local state is behind the truth it is about to be asked for.

Capture-and-confirm alone would serve that stale watermark with a fresh quorum, \
and the client would see a write it had already been told was durable simply \
vanish. Raft's answer is to commit a no-op in the new term before serving any \
read. paros waits instead: at the moment it wins, a leader records a **read \
floor** — the highest slot its promise quorum reported, which by quorum \
intersection is at or above every write any earlier leader acknowledged. A read \
captures the *maximum* of the applied watermark and that floor, and confirms \
only when **both** things are true: the ack quorum is in, and the applied prefix \
covers the captured index.

Here the old leader got one command accepted by one other node and then died \
before the decision came back, so nothing is chosen and nobody has applied \
anything. The new leader's Phase 1 finds that value and must re-propose it. Ask \
for a read while its recovery is still in flight: the acks will be there, the \
answer is still no. Then let the slot re-decide and watch the read fire on its \
own, out of the very batch that applied it.",
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
        let recovered = applied(world, 1)
            .iter()
            .any(|command| command == "\"alpha\"");
        match (served.first(), recovered) {
            (Some(index), true) => GoalStatus::Reached(format!(
                "The read was refused while the recovered slot was still in flight, and served \
                 at {} once it re-decided. The quorum was never the missing piece — the applied \
                 prefix was.",
                at(*index)
            )),
            (None, _) => GoalStatus::Open(
                "Ask the fresh leader for a read, answer for it, and then let its recovered slot \
                 finish."
                    .to_string(),
            ),
            (Some(_), false) => GoalStatus::Open(
                "The read was served, but the inherited value has not been executed.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "The acks are not the question. Compare the index the read captured with the applied \
             prefix underneath it."
                .to_string(),
        ),
        _ => Some(
            "A fresh leader's read floor sits at the highest slot its promise quorum reported, \
             which is above anything it has applied yet. Wait: the read fires by itself when the \
             recovered slot decides."
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
Every mechanism in this act exists for one client-visible property, and this \
level is where you produce it and have it judged. **Linearizable** means every \
operation appears to take effect atomically at some instant between when it was \
asked and when it was answered. The register under observation here is the \
applied log prefix: a write appends to it at its committed slot, and a read \
observes its watermark.

Because the log totally orders the writes, checking a recorded history needs no \
search at all — three conditions over the clients' own program order are the \
whole test. One: a committed read observes every write acknowledged before it \
began. Two: watermarks never move backwards across reads that do not overlap. \
Three: a write issued after a committed read lands *above* that read's \
watermark. Operations that never completed constrain nothing; a timed-out write \
may still commit later, and that is not a violation of anything.

Two clients here, and one leader change in the middle. Get a write acknowledged \
under the old leadership, elect a new leader behind the old one's back, and read \
across the change: the second client's read must see the first client's write, \
even though the node answering it was not the node that took it. **Then try to \
break it.** Ask the deposed leader — which still believes it leads — for a read \
of its own. It will collect nothing, because every follower that promised the \
new ballot refuses to ack the old one, and its read will sit unanswered for the \
rest of the level. That is the protocol declining to certify a read it cannot \
prove, and it is the correct thing for it to do forever.",
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
                "The history checks out. A read at one node observed a write acknowledged at \
                 another before it began, across a leader change — and {} read the protocol \
                 could not prove is still sitting unanswered, which is the only honest thing to \
                 do with it.",
                if refused == 1 {
                    "one".to_string()
                } else {
                    refused.to_string()
                }
            )),
            (false, _) => GoalStatus::Open(
                "Get a write acknowledged, change the leadership behind the old leader's back, \
                 and read at the new leader."
                    .to_string(),
            ),
            (true, 0) => GoalStatus::Open(
                "Now ask the deposed leader for a read too, and watch what it cannot do."
                    .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "The deposed leader's read is supposed to hang. Deliver its beats and see who acks \
             them: a follower that has promised the newer ballot will not."
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
The read half of linearizability leans on the write half: \"a read observes \
every write **acknowledged** before it began\" is only worth something if the \
ack means what it says. And there is one word in the middle of the log that \
means two different things.

A slot is **chosen** when a quorum has voted for it. That is a fact about the \
cluster, it is permanent, and — because the leader pipelines — it can become \
true at slot 6 while slot 5 is still open. A slot is **applied** when this node \
has handed it to its state machine, which happens strictly in order, so slot 6 \
waits for slot 5. Between those two moments the command is decided and \
unexecuted, and a node that acked it as done would be promising a client \
something no node can read back yet.

That window is exactly where a client retry lands. A retry is deduplicated by \
`(client, seq)`, and the leader keeps **two** tables: what it has applied, and \
what it has in flight. Answer from the applied table when the command is only \
chosen and you have acked a write nobody executed. Answer \"never seen it\" and \
you give an already-decided command a second slot — duplicate execution, which \
is strictly worse. So the two tables move together: when a slot is learned \
chosen, the identity moves *within* the in-flight table onto that slot, and only \
the contiguous apply walk ever writes the applied one. Here the client's second \
write is chosen above a hole. It will ask twice — once while the hole is open, \
once after it closes — and you answer for the leader both times.",
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
        let bravo = executed
            .iter()
            .filter(|command| *command == "\"bravo\"")
            .count();
        if bravo > 1 {
            return GoalStatus::Failed(
                "\"bravo\" was executed twice. A retry that misses both dedup tables gets a \
                 fresh slot, and the command runs again."
                    .to_string(),
            );
        }
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
                "A retry was given a fresh slot: the leader had never heard of a command it was \
                 already holding, and the client's write is now in the log twice."
                    .to_string(),
            );
        }
        match (bravo, held, acked) {
            (1, true, true) => GoalStatus::Reached(format!(
                "The command was executed exactly once ({}), and both retries were answered from \
                 the table that actually knew where it was — held while the hole was open, \
                 acknowledged once it closed. \"Chosen\" and \"applied\" are two different facts, \
                 and only one of them may be acknowledged.",
                executed.join(", ")
            )),
            (1, false, _) => GoalStatus::Open(
                "Have the client ask again *while* its command is chosen above the hole — that \
                 is the window the level is about."
                    .to_string(),
            ),
            (1, true, false) => {
                GoalStatus::Open("Now close the hole and let the client ask once more.".to_string())
            }
            _ => GoalStatus::Open(
                "Get both commands chosen — out of order, so the second one waits above a hole \
                 — and answer the client's retries."
                    .to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the two lines on the card. One table knows where the command *is*; the other \
             knows it has been *run*."
                .to_string(),
        ),
        _ => Some(
            "While the hole is open the command is chosen and unexecuted: it is in the in-flight \
             table, not the applied one, so the client waits on that slot. Once the hole closes \
             it is in the applied table and may be acknowledged."
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
