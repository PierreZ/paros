//! Act II — a replicated log.
//!
//! Seven levels over the **log world** ([`crate::world::World`]): three (once,
//! five) `ColocatedNode`s, one client, disks that survive a crash, and the
//! clock the player ticks. Act I chose one value; here the same two phases run
//! over a log of slots, one leader claims the whole suffix with a single
//! `Prepare`, and everything that can go wrong between "durable" and "sent"
//! becomes a move the player makes on purpose.
//!
//! The order is the order the mechanisms depend on each other: the durability
//! edge first, then the log and its holes, then the leader that streams it,
//! then the three failures that only a leader change can produce — a hole
//! nobody will ever fill, a record a restart would resurrect, and a read
//! answered by a leader that has already been replaced.
//!
//! Every reference solution here is **recorded**, not written: a private
//! `Script` plays the level, choosing messages by what they are rather than by
//! id and answering every prompt with the answer `paros-core` itself gives, and
//! the flat `Vec<Action>` it hands back is the level's `reference`.

use std::collections::BTreeMap;

use paros_core::{
    Ballot, ClientId, ClientSeq, Command, Config, Entry, NodeId, QuorumSystem, Slot, Value,
};

use crate::action::{Action, ActionKind, Seam};
use crate::auto::AutomationFlag;
use crate::level::script::{Script, kind, kind_at, not_to, phase, to};
use crate::level::{GoalStatus, Level, WorldKind};
use crate::view::{MessageView, show_command};
use crate::world::{Disk, World};

/// Act II's levels, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &PERSIST_BEFORE_SEND,
        &A_LOG_OF_DECISIONS,
        &ELECT_A_LEADER,
        &STEADY_STATE,
        &THE_PERMANENT_GAP,
        &WHAT_SURVIVES_A_CRASH,
        &THE_READ_THAT_LIES,
    ]
}

/// The one client every Act II level gives the player.
const CLIENT: u64 = 7;

/// The election timeout every Act II node starts with, in ticks. Long enough
/// that a level's own ticks are deliberate, short enough that
/// [`Action::StartElection`] is not the only way to campaign.
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
];

/// The convenience toggle every Act II level offers.
const TOGGLES: &[AutomationFlag] = &[AutomationFlag::DeliverReplies];

// ---- worlds -----------------------------------------------------------------

/// The configuration of a `size`-node cluster under a majority.
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

/// A cluster of `size` fresh nodes.
fn fresh(size: u64) -> WorldKind {
    let disks = peers(size)
        .into_iter()
        .map(|id| Disk::new(config(id, size)))
        .collect();
    WorldKind::Log(Box::new(World::from_disks(disks, &[CLIENT], TIMEOUT)))
}

/// A cluster whose disks already carry a history: every node promised
/// `promised`, and `records` says what each one accepted.
fn with_history(
    size: u64,
    promised: Ballot,
    records: &BTreeMap<u64, BTreeMap<Slot, (Ballot, Command)>>,
) -> WorldKind {
    let disks = peers(size)
        .into_iter()
        .map(|id| {
            Disk::seeded(
                config(id, size),
                promised,
                records.get(&id.0).cloned().unwrap_or_default(),
                None,
            )
        })
        .collect();
    WorldKind::Log(Box::new(World::from_disks(disks, &[CLIENT], TIMEOUT)))
}

/// A command an earlier leadership left on a disk. Its client id is **not**
/// the level's client: a seeded command and a fresh proposal must not collide
/// in the at-most-once ledger, or the leader would answer the new proposal
/// with the old slot.
fn carried_over(text: &str) -> Command {
    Command::User(Entry {
        client: ClientId(9),
        seq: ClientSeq(1),
        value: Value(text.as_bytes().to_vec()),
    })
}

/// One node's seeded log.
fn one_record(slot: u64, ballot: Ballot, text: &str) -> BTreeMap<Slot, (Ballot, Command)> {
    let mut records = BTreeMap::new();
    records.insert(Slot(slot), (ballot, carried_over(text)));
    records
}

/// A watermark as the goals write it.
fn at(slot: Option<Slot>) -> String {
    slot.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0))
}

/// The ballot an earlier leadership ran at: `round`, minted by node `node`.
fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
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

/// Every node's id, whether it is running or not.
fn pool(world: &WorldKind) -> Vec<u64> {
    log_world(world).map_or_else(Vec::new, |world| {
        world.pool().iter().map(|id| id.0).collect()
    })
}

/// The nodes that are not running.
fn crashed(world: &WorldKind) -> Vec<u64> {
    let Some(log) = log_world(world) else {
        return Vec::new();
    };
    log.pool()
        .iter()
        .filter(|id| log.node(**id).is_none())
        .map(|id| id.0)
        .collect()
}

/// Whether every node has executed exactly `expected`, in order.
fn everyone_applied(world: &WorldKind, expected: &[&str]) -> Result<(), String> {
    let want: Vec<String> = expected.iter().map(|text| (*text).to_string()).collect();
    for node in pool(world) {
        let got = applied(world, node);
        if got != want {
            return Err(format!(
                "node {node} executed {got:?}, but the level asks for {want:?}"
            ));
        }
    }
    Ok(())
}

/// Whether some node is holding accepted records it has executed none of — the
/// shape of a cluster that voted for everything and was told about nothing.
fn holding_undecided(world: &WorldKind) -> bool {
    let Some(log) = log_world(world) else {
        return false;
    };
    log.pool().iter().any(|id| {
        log.disk(*id)
            .is_some_and(|disk| !disk.records().is_empty() && disk.applied().is_empty())
    })
}

/// Both durability seams, and which nodes they cut.
fn seams(world: &WorldKind) -> (bool, bool) {
    let Some(log) = log_world(world) else {
        return (false, false);
    };
    (
        log.seams_fired()
            .iter()
            .any(|(_, seam)| *seam == Seam::BeforeSync),
        log.seams_fired()
            .iter()
            .any(|(_, seam)| *seam == Seam::AfterSyncBeforeSend),
    )
}

// ---- action shorthands ------------------------------------------------------

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

fn crash(node: u64) -> Action {
    Action::Crash { node }
}

fn restart(node: u64) -> Action {
    Action::Restart { node }
}

fn crash_at(node: u64, seam: Seam) -> Action {
    Action::CrashAt { node, seam }
}

fn tick(node: u64) -> Action {
    Action::Tick { node }
}

fn read_index(node: u64) -> Action {
    Action::ReadIndex {
        node,
        client: Some(CLIENT),
    }
}

/// The Phase-2 traffic of one slot: its `Accept`s, its `Accepted`s and the
/// `Commit`s that report the decision. Deliberately not "everything naming this
/// slot": a `Heartbeat`'s slot is its commit watermark, not a proposal.
fn slot_traffic(slot: u64) -> impl Fn(&MessageView) -> bool {
    move |message| {
        matches!(message.kind.as_str(), "Accept" | "Accepted" | "Commit")
            && message.slot == Some(slot)
    }
}

// ---- 7. persist before send -------------------------------------------------

const SEAM_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Crash,
    ActionKind::CrashAt,
    ActionKind::Restart,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act2/persist-before-send`.
pub static PERSIST_BEFORE_SEND: Level = Level {
    id: "act2/persist-before-send",
    act: 2,
    title: "Persist before send",
    briefing: "\
Every message that a node sends about itself is a claim about its disk. A \
`Promise` says that the promise of the node is now durable at that ballot or \
higher. An `Accepted` says that the node holds that value on disk. A proposer \
counts those claims toward a quorum and then treats the slot as decided. If a \
claim is not durable, the cluster loses a decision, not a message.

For that reason a node produces its output in **batches**. One batch holds a \
set of durable writes and a set of messages, in a fixed order. Write the batch \
to disk first, and send the messages second. If you send first, a node that \
crashes in that window forgets a promise that it published, or a vote that a \
proposer counted. A node that forgot a promise can then vote for a ballot that \
it refused. Either fault lets one slot get two values.

The game asks you for the order on every batch. You then cut one batch in two, \
and the two seams give different results. `crash before sync` discards the \
whole batch: it writes nothing and it sends nothing. That case is always safe, \
because the disk does not change and the node told no other node anything. \
`crash after sync, before send` keeps the writes and loses the messages, so the \
node holds a promise that no other node knows about. The restart keeps that \
promise, so restart both nodes and make sure that no promise goes down.",
    field_guide: "restart-safety.html",
    symbols: &["Ready", "Ready::advance", "HardState", "WriteOp"],
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::LeaderRecovery,
        AutomationFlag::ReadServe,
    ],
    pinned_off: &[AutomationFlag::PersistOrder],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::PersistOrder],
    allowed_actions: SEAM_ACTIONS,
    setup: || fresh(3),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Some(node) = log.promise_regressed() {
            return GoalStatus::Failed(format!(
                "The durable promise of node {} is below a promise that it already made. No \
                 part of the protocol is safe after that.",
                node.0
            ));
        }
        let (before, after) = seams(world);
        let chosen = log.pool().iter().any(|id| {
            log.disk(*id)
                .is_some_and(|d| d.hard_state().chosen_index.is_some())
        });
        let down = crashed(world);
        match (before, after, chosen, down.is_empty()) {
            (true, true, true, true) => GoalStatus::Reached(
                "A value is chosen, you cut both seams, and every node came back with every \
                 promise that it made. The batch that reached the disk but not the wire is the \
                 safe one. A node may know more than the cluster, but it must not know less."
                    .to_string(),
            ),
            (_, _, _, false) => GoalStatus::Open(format!(
                "Restart node {} and look at what it reads back.",
                down[0]
            )),
            (false, _, _, _) => GoalStatus::Open(
                "Cut a batch before it reaches the disk. Arm `crash before sync`. Then deliver a \
                 message that makes the node write."
                    .to_string(),
            ),
            (_, false, _, _) => GoalStatus::Open(
                "Cut a batch after it reaches the disk and before it goes out. That seam leaves \
                 a promise that no other node knows about."
                    .to_string(),
            ),
            (_, _, false, _) => GoalStatus::Open(
                "Now get a value chosen through the nodes that are up.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "The question is not which order is faster. Ask what the message claims if the node \
             stops one moment after it sends the message."
                .to_string(),
        ),
        _ => Some(
            "Write the batch to disk, then send it. A `Promise` is a statement about the disk, \
             so the disk must be correct first."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act2/persist-before-send");
        // The candidate's own batch raises its promise and carries the
        // Prepares: the first place the question is asked.
        script.play(start_election(0)).answer_all();
        // Node 1 dies with the promise durable and the Promise never sent.
        script
            .play(crash_at(1, Seam::AfterSyncBeforeSend))
            .settle(|message| message.kind == "Prepare" && message.to == 1)
            .play(restart(1));
        // Node 2 answers properly, and its Promise elects node 0.
        script
            .settle(|message| message.kind == "Prepare" && message.to == 2)
            .settle(kind("Promise"));
        script.play(propose(0, "alpha")).answer_all();
        // Node 2 dies before the flush: nothing written, nothing sent.
        script
            .play(crash_at(2, Seam::BeforeSync))
            .settle(|message| message.kind == "Accept" && message.to == 2);
        script
            .settle(|message| message.kind == "Accept" && message.to == 1)
            .settle(kind("Accepted"))
            .play(restart(2))
            .settle_all();
        script.finish()
    },
};

// ---- 8. a log of decisions --------------------------------------------------

const LOG_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act2/a-log-of-decisions`.
pub static A_LOG_OF_DECISIONS: Level = Level {
    id: "act2/a-log-of-decisions",
    act: 2,
    title: "A log of decisions",
    briefing: "\
One decision is not a database. A replicated state machine needs a **sequence** \
of decisions. Multi-Paxos therefore runs the same protocol at every **slot** of \
a log, and the application executes the chosen commands in slot order. Each \
slot runs on its own, so slot 2 does not wait for slot 1. A leader sends the \
`Accept` messages for several slots at the same time. That method is \
pipelining, and it makes the log fast, but the slots then complete in any order.

That behaviour separates two words. A slot is **chosen** when a quorum votes \
for it. That fact belongs to the cluster, it is permanent, and it can occur at \
any slot at any time. A slot is **applied** when this node gives it to its \
state machine. That step is local and strictly in order, because two nodes that \
execute the same commands in a different order hold two different databases. A \
node therefore keeps a contiguous *applied prefix*, and a slot chosen above a \
hole waits.

Propose three commands. Then deliver the votes for slot 2 before the votes for \
slot 1. Answer for the replica each time that a slot becomes chosen. Do not \
apply the later slot early. You cannot undo that mistake, because the \
application already ran the command.",
    field_guide: "replicated-log.html",
    symbols: &[
        "Replica::chosen_index",
        "Replica::first_unchosen",
        "Replica::chosen_gap",
        "Slot",
    ],
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::LeaderRecovery,
        AutomationFlag::PersistOrder,
        AutomationFlag::ReadServe,
    ],
    pinned_off: &[AutomationFlag::ReplicaApply],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::ReplicaApply],
    allowed_actions: LOG_ACTIONS,
    setup: || fresh(3),
    goal: |world| match everyone_applied(world, &["alpha", "bravo", "charlie"]) {
        Ok(()) => GoalStatus::Reached(
            "Every node executed the same three commands in the same order. One command was \
             chosen before the slot below it, and every node executed it after that slot."
                .to_string(),
        ),
        Err(detail) => GoalStatus::Open(format!(
            "Get all three commands chosen and applied on every node, in slot order. {detail}"
        )),
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Compare the slot that became chosen with the first slot that the node still \
             misses. The two are sometimes different."
                .to_string(),
        ),
        _ => Some(
            "A node applies a slot only when that slot is its first unchosen slot. A node holds \
             every slot above a hole. It records the slot as chosen, and it executes the slot \
             when the hole closes."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act2/a-log-of-decisions");
        script.play(start_election(0)).settle_all();
        script
            .play(propose(0, "alpha"))
            .play(propose(0, "bravo"))
            .play(propose(0, "charlie"));
        // Slot 0 decides and applies. Then slot 2 decides over a hole at slot
        // 1, and has to wait for it.
        script
            .settle(slot_traffic(0))
            .settle(slot_traffic(2))
            .settle(slot_traffic(1))
            .settle_all();
        script.finish()
    },
};

// ---- 9. elect a leader ------------------------------------------------------

const ELECTION_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act2/elect-a-leader`.
pub static ELECT_A_LEADER: Level = Level {
    id: "act2/elect-a-leader",
    act: 2,
    title: "Elect a leader",
    briefing: "\
Phase 1 at every slot costs two round trips for each command. It also lets two \
proposers compete at each slot. Multi-Paxos removes both costs. It elects one \
**leader**, and that leader runs Phase 1 once for every slot above a start \
slot. The `from_slot` field in a `Prepare` names that start slot. Phase 1 names \
no value, so one short message can claim a log suffix of any length.

The reply carries the work. A `Promise` reports **every** value that the \
acceptor accepted at or above that slot. One exchange therefore tells the new \
leader the state of the log that the last leader left. Some of those slots are \
already chosen and some are not, and the new leader cannot separate the two. A \
report from one acceptor looks the same as an already chosen value. The \
value-selection rule of Act I therefore applies to each slot: re-propose the \
reported value under your own ballot before you propose your own command.

One node in this level still holds a value from a leadership that ended. It \
accepted the value, and then that leader stopped. Tick a follower until its \
election timer fires. Run its Phase 1 through the node that holds the value. \
Settle the inherited slot before you propose anything new. Then give the \
cluster a fresh command, and look at the cost: one round trip, not two.",
    field_guide: "stable-leader.html",
    symbols: &[
        "Message::Prepare",
        "Election::recovered",
        "Proposer::open_recovery",
        "RecoveryStep::Recovered",
    ],
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::PersistOrder,
        AutomationFlag::ReadServe,
    ],
    pinned_off: &[AutomationFlag::LeaderRecovery],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::LeaderRecovery],
    allowed_actions: ELECTION_ACTIONS,
    setup: || {
        let mut records = BTreeMap::new();
        records.insert(1, one_record(0, ballot(1, 2), "carried-over"));
        with_history(3, ballot(1, 2), &records)
    },
    goal: |world| match everyone_applied(world, &["carried-over", "fresh"]) {
        Ok(()) => GoalStatus::Reached(
            "The cluster decided the inherited value under the new ballot before it proposed \
             anything new. The fresh command took a slot above it. Phase 1 ran once, for the \
             whole suffix."
                .to_string(),
        ),
        Err(detail) => GoalStatus::Open(format!(
            "Settle the slot that the promise quorum reported. Then get a fresh command chosen. \
             {detail}"
        )),
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Read the report that the Promise gave for that slot before you answer. The client \
             of the new leader does not change that answer."
                .to_string(),
        ),
        _ => Some(
            "A slot that a Promise describes is possibly already chosen, and you cannot tell. \
             Re-propose the reported value under your ballot. Your own command waits for a slot \
             above it."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act2/elect-a-leader");
        // Tick node 0 until its election timer runs out — there is no "campaign
        // now" verb, in the game or in the core. Its Phase 1 must then reach
        // node 1, the only node that knows anything.
        for _ in 0..TIMEOUT {
            script.play(tick(0));
        }
        script
            .settle(|message| message.kind == "Prepare" && message.to == 1)
            .settle(kind("Promise"));
        // The recovery question, then the inherited slot is decided at the new
        // ballot, and only then a fresh command.
        script.settle_all();
        script.play(propose(0, "fresh")).settle_all();
        script.finish()
    },
};

// ---- 10. steady state -------------------------------------------------------

const STREAM_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::ResendPending,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act2/steady-state`.
pub static STEADY_STATE: Level = Level {
    id: "act2/steady-state",
    act: 2,
    title: "Steady state",
    briefing: "\
With a leader in place the protocol reaches its lowest cost: one round trip for \
each command. The leader gives the command the next free slot. It sends \
`Accept` and counts the votes, and the slot is chosen. It sends no `Prepare`, \
because the ballot that it won already covers every slot that it uses. Lamport \
says that this cost is not only low, it is *optimal*. Phase 2 alone is the \
smallest cost that any fault-tolerant agreement algorithm can reach.

The leader also does not wait. Propose three commands, and the leader opens \
three rounds at the same time. Each round decides when its own quorum answers. \
The followers do **not** learn that a slot is decided, because the votes go to \
the leader and the decision occurs there. The leader therefore adds its commit \
watermark to the `Heartbeat` that it already sends to hold its position. That \
costs no extra message and no extra round trip, and a follower learns how far \
the log is settled.

Drop the commit messages. The followers then hold three accepted values and an \
empty applied prefix. Tick the leader once and deliver the beat. All three \
commands then execute together. After the whole cluster catches up, the game \
delivers the heartbeats for you. That automation is the first one, and every \
later level uses it.",
    field_guide: "stable-leader.html",
    symbols: &[
        "Message::Heartbeat",
        "ColocatedNode::propose",
        "Proposer::next_slot",
        "ColocatedNode::resend_pending",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::DeliverHeartbeats],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::DeliverHeartbeats],
    allowed_actions: STREAM_ACTIONS,
    setup: || fresh(3),
    goal: |world| match everyone_applied(world, &["alpha", "bravo", "charlie"]) {
        Ok(()) => GoalStatus::Reached(
            "Three commands took three round trips. The followers learned the result from a \
             watermark on a beat that the leader sent anyway. The game now delivers the \
             heartbeats for you."
                .to_string(),
        ),
        Err(detail) => GoalStatus::Open(format!(
            "Propose all three commands and get them executed on every node. {detail}"
        )),
    },
    // This level asks no question, so there are no mistakes to count: the hint
    // watches the world instead, and appears exactly when a node is holding
    // votes it has not been told the fate of.
    hint: |world, mistakes| {
        (mistakes > 1 || holding_undecided(world)).then(|| {
            "The votes go to the leader, so the decision occurs there and no message tells the \
             followers. A follower learns the result from the commit index on the next beat. \
             Tick the leader, then deliver the beat."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act2/steady-state");
        script.play(start_election(0)).settle_all();
        script
            .play(propose(0, "alpha"))
            .play(propose(0, "bravo"))
            .play(propose(0, "charlie"));
        // Three rounds in flight at once; the leader decides all three.
        script.settle(kind("Accept")).settle(kind("Accepted"));
        // The followers are told nothing directly: the beat carries it.
        script.drop_all(kind("Commit"));
        script.play(tick(0)).settle(phase("heartbeat")).settle_all();
        script.finish()
    },
};

// ---- 11. the permanent gap --------------------------------------------------

const GAP_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Crash,
    ActionKind::Restart,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act2/the-permanent-gap`.
pub static THE_PERMANENT_GAP: Level = Level {
    id: "act2/the-permanent-gap",
    act: 2,
    title: "The permanent gap",
    briefing: "\
Pipelining has one failure that no other part of the protocol repairs. The \
leader sends the `Accept` messages for slot 0 and slot 1 together. The messages \
for slot 0 reach only the leader itself. The messages for slot 1 reach a \
quorum, so slot 1 is chosen. The leader then crashes, and its round map is \
volatile, so the leadership takes the map with it. For that reason this level \
makes you crash the leader, and no node sends the `Accept` for slot 0 again.

Now count what the next leader can see. Its promise quorum excludes the dead \
leader, so **no** `Promise` names slot 0. The first free slot comes from the \
accepted log, so the next leader passes over the hole. A restart computes the \
same first free slot, so no node proposes slot 0 again. A hole is not a local \
fault: the applied prefix of every node stops one slot below it, permanently. \
Higher slots become chosen but do not execute, the reads stop below the hole, \
and no peer can replay the missing slot.

The new leader must re-propose every value that a `Promise` reported. It has a \
second duty as well: it must fill every slot below its frontier that **no** \
`Promise` described, with its own `Noop`. That fill is safe for the reason that \
Phase 1 exists. A Phase-2 quorum accepted any value already chosen at that \
slot. That quorum shares a member with this promise quorum, so a `Promise` \
reports the value. Silence from a full quorum is not ignorance; it is \
permission to fill the slot.",
    field_guide: "stable-leader.html",
    symbols: &[
        "Control::Noop",
        "RecoveryStep::Fill",
        "Replica::chosen_gap",
        "ColocatedNode::election_gap_fills",
    ],
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::PersistOrder,
        AutomationFlag::ReadServe,
    ],
    pinned_off: &[AutomationFlag::LeaderRecovery],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: GAP_ACTIONS,
    setup: || fresh(3),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        if let Some((hole, highest)) = log
            .pool()
            .iter()
            .filter_map(|id| log.node(*id))
            .find_map(|node| node.replica().chosen_gap())
        {
            return GoalStatus::Open(format!(
                "Slot {} is chosen and slot {} is not, so the applied prefix stops below the \
                 hole. Only a new leadership can propose slot {} again.",
                highest.0, hole.0, hole.0
            ));
        }
        let executed = applied(world, 1);
        // `Noop` is the protocol's own control command, so the level may name
        // it. The client's values are the client's, so they are read back from
        // the history rather than written down here.
        let filled = executed.iter().any(|command| command == "Noop");
        let asked = log.proposed_values();
        let chosen = !asked.is_empty() && asked.iter().all(|value| executed.contains(value));
        match (filled, chosen) {
            (true, true) => GoalStatus::Reached(format!(
                "No node holds a hole, and the cluster executed the commands of the client: {}. \
                 No client asked for the Noop. Quorum intersection makes the Noop the proof \
                 that the slot was free.",
                executed.join(", ")
            )),
            (true, false) => GoalStatus::Open(
                "The Noop filled the hole. The cluster did not choose the command that was lost \
                 with the old leader, so the client must ask again."
                    .to_string(),
            ),
            (false, _) => GoalStatus::Open(
                "Let a new leadership account for every slot below its frontier.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Ask what permits each answer. A re-proposal needs a value that an acceptor \
             reported. A skip needs another node that proposes the slot later."
                .to_string(),
        ),
        _ => Some(
            "No acceptor reported that slot, and the silence of a full promise quorum shows \
             that nothing was chosen there. Fill the slot with a Noop, so the prefix can move \
             past it."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act2/the-permanent-gap");
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).play(propose(0, "bravo"));
        // Slot 0's Accepts are lost for good — not merely late. A copy still in
        // flight would land before the next ballot fenced it out, and then the
        // promise quorum would report slot 0 after all.
        script.drop_all(kind_at("Accept", 0)).settle_all();
        // The leadership dies with its round map, so nothing re-sends slot 0.
        script.play(crash(0)).play(start_election(1)).settle_all();
        // The client's first command was never chosen: it asks again.
        script.play(propose(1, "alpha")).settle_all();
        script.finish()
    },
};

// ---- 12. what survives a crash ----------------------------------------------

const RESTART_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Crash,
    ActionKind::CrashAt,
    ActionKind::Restart,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act2/what-survives-a-crash`.
pub static WHAT_SURVIVES_A_CRASH: Level = Level {
    id: "act2/what-survives-a-crash",
    act: 2,
    title: "What survives a crash",
    briefing: "\
A node holds two kinds of state: the disk and the volatile state. The volatile \
state is the role, the ballot, the open rounds and the reads that the node \
owes. The process loses that state at a crash, so a crash is an abdication and \
the node writes no fence to make one. The disk holds the promise and the \
accepted records, and the safety argument of the protocol uses only the disk. \
Crash a node at any step of a decision, and then restart it. It comes back as a \
follower, and it holds every promise that it made.

One fault breaks that rule, and it is not a lost write. A node in this level \
holds an accepted value from an old ballot, and the new leader left it out of \
the promise quorum. The cluster therefore chose another value at that slot, and \
the decision reaches the node as a `Commit` or a catch-up. The record of the \
node disagrees with that decision at a *lower* ballot. If the node keeps that \
record, a restart reports it as the accepted value, and the next promise quorum \
can report it as the highest. A new leader then re-proposes it over a chosen \
value: that fault is the stale-accept resurrection, and the overwrite makes a \
restart safe.

Play the side of the acceptor. Crash and restart the node at every step. When \
the disagreement arrives, decide which record the disk keeps.",
    field_guide: "restart-safety.html",
    symbols: &[
        "Acceptor::record_accepted",
        "HardState::max_promised_ballot",
        "Message::Commit",
        "Message::CatchUpResponse",
    ],
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::LeaderRecovery,
        AutomationFlag::PersistOrder,
        AutomationFlag::ReadServe,
    ],
    pinned_off: &[AutomationFlag::CommitOverwrite],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::CommitOverwrite],
    allowed_actions: RESTART_ACTIONS,
    setup: || {
        let mut records = BTreeMap::new();
        records.insert(2, one_record(0, ballot(1, 2), "stale-value"));
        with_history(3, ballot(1, 2), &records)
    },
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
        let down = crashed(world);
        if let Some(node) = down.first() {
            return GoalStatus::Open(format!(
                "Restart node {node} and read its disk back. That is the whole question."
            ));
        }
        let wrong: Vec<u64> = log
            .pool()
            .iter()
            .filter(|id| {
                log.disk(**id).is_none_or(|disk| {
                    disk.records()
                        .get(&Slot(0))
                        .is_none_or(|(_, command)| show_command(command) != "fresh")
                })
            })
            .map(|id| id.0)
            .collect();
        if wrong.is_empty() {
            GoalStatus::Reached(
                "Every disk holds the chosen value at slot 0. Every promise came back at \
                 least as high as it was before the crash. The stale record is gone, so the \
                 next election cannot bring it back."
                    .to_string(),
            )
        } else {
            GoalStatus::Open(format!(
                "Get the chosen value onto every disk at slot 0. Node(s) {wrong:?} still hold \
                 another value."
            ))
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Ask what the next promise quorum reports if this node is a member of it. Then ask \
             what a new leader does with that report."
                .to_string(),
        ),
        _ => Some(
            "The ballot that chose the value is higher than the ballot of the record here. The \
             higher ballot decides, so overwrite the record. If you keep it, a restart brings \
             back a value that no node chose."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act2/what-survives-a-crash");
        // A crash before anything was promised loses nothing.
        script
            .play(start_election(0))
            .play(crash(1))
            .play(restart(1));
        // Node 0's promise quorum is itself and node 1 — node 2, which holds
        // the old value, is never asked.
        script
            .settle(|message| message.kind == "Prepare" && message.to == 1)
            .settle(kind("Promise"));
        script.play(propose(0, "fresh"));
        // A crash straight after the vote: the record is durable, so the
        // Accepted it already sent is still true.
        script
            .settle(|message| message.kind == "Accept" && message.to == 1)
            .play(crash(1))
            .play(restart(1))
            .settle(kind("Accepted"));
        // Now the decision reaches the node holding the stale record.
        script.settle(|message| message.kind == "Commit" && message.to == 2);
        script.play(crash(2)).play(restart(2)).settle_all();
        script.finish()
    },
};

// ---- 13. the read that lies -------------------------------------------------

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

/// `act2/the-read-that-lies`.
pub static THE_READ_THAT_LIES: Level = Level {
    id: "act2/the-read-that-lies",
    act: 2,
    title: "The read that lies",
    briefing: "\
A write is safe because a quorum voted for it. A read changes nothing, so no \
node votes on it, and a read has no quorum. For that reason a read is easy to \
get wrong. The simple answer is that the leader holds the whole log and answers \
from memory. But leadership is a **belief**, and a node cannot check that belief \
on its own. No message tells a leader that another node replaced it, so a quiet \
follower and a newer ballot look the same to it.

A read must therefore prove the leadership at the moment of the question, and \
the proof needs no log write. Capture the watermark that the read must observe, \
and send a beat to every node. Wait for a **Phase-2 quorum** to ack *that* beat, \
not an older one. A quorum of acks to a beat sent after the read started shows \
that no other ballot decided anything. Any quorum that decided a value shares a \
member with this quorum. Answer the read after the applied prefix covers the \
captured watermark.

This level has five nodes. Another node replaced the leader, and neither the old \
leader nor one follower knows that. The old leader collects exactly one ack, so \
its own vote plus that ack is two of five. Decide whether two of five is enough. \
Then ask the real leader the same question and look at the complete proof. The \
level checks one rule: a read must not observe less than a write that the \
cluster already acknowledged.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "ColocatedNode::read_index",
        "Proposer::open_read",
        "Proposer::confirm_reads",
        "ReadState",
    ],
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::LeaderRecovery,
        AutomationFlag::PersistOrder,
    ],
    pinned_off: &[AutomationFlag::ReadServe],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::ReadServe],
    allowed_actions: READ_ACTIONS,
    setup: || fresh(5),
    goal: |world| {
        let Some(log) = log_world(world) else {
            return GoalStatus::Open("This level runs in the replicated-log world.".to_string());
        };
        let reads = log.reads();
        let acked = log.highest_acked_slot();
        let served: Vec<(NodeId, Option<Slot>)> = reads
            .iter()
            .filter(|(_, _, served)| *served)
            .map(|(node, index, _)| (*node, *index))
            .collect();
        if let Some((node, index)) = served
            .iter()
            .find(|(_, index)| index.map(|s| s.0) < acked.map(|s| s.0))
        {
            return GoalStatus::Failed(format!(
                "A read that node {} served observed {}, but the cluster had already \
                 acknowledged a write at {}. That read gave a wrong answer.",
                node.0,
                at(*index),
                at(acked)
            ));
        }
        let unserved = reads.iter().filter(|(_, _, served)| !*served).count();
        match (served.len(), unserved) {
            (0, _) => GoalStatus::Open(
                "Ask for a linearizable read. Then answer for the leader that must prove its \
                 leadership."
                    .to_string(),
            ),
            (_, 0) => GoalStatus::Open(
                "Ask the replaced leader for a read as well. The answer to look at is the \
                 answer that it must not give."
                    .to_string(),
            ),
            (_, _) => GoalStatus::Reached(format!(
                "The cluster served one read, at or above the last acknowledged write ({}). \
                 One read still waits at a node that cannot prove that it leads. That node must \
                 keep the read open.",
                at(acked)
            )),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Count the acks against the configuration, not against the nodes that answered. \
             Two of five is not a quorum of five."
                .to_string(),
        ),
        _ => Some(
            "A node serves a read on a quorum of acks to a beat sent *after* the read \
             started. It must also wait until the applied prefix covers the captured \
             watermark. With less proof, the leader answers from a belief."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act2/the-read-that-lies");
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).settle_all();
        // Node 1 campaigns and wins with nodes 2 and 3. Node 0 and node 4 hear
        // none of it, so both still believe node 0 leads.
        script
            .play(start_election(1))
            .settle(|message| message.kind == "Prepare" && (message.to == 2 || message.to == 3))
            .settle(not_to(&[0, 4]));
        script.drop_all(to(0));
        // The deposed leader is asked for a read, and collects one ack.
        script
            .play(read_index(0))
            .settle(|message| message.kind == "Heartbeat" && message.to == 4)
            .settle(|message| message.kind == "HeartbeatAck" && message.to == 0);
        script.answer_all().drop_all(to(0));
        // The leader that really leads is asked the same question.
        script
            .play(read_index(1))
            .settle(|message| message.kind == "Heartbeat" && (message.to == 2 || message.to == 3))
            .settle(|message| message.kind == "HeartbeatAck" && message.to == 1);
        script.answer_all();
        script.finish()
    },
};
