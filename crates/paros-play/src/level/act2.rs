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
    let want: Vec<String> = expected.iter().map(|text| format!("{text:?}")).collect();
    for node in pool(world) {
        let got = applied(world, node);
        if got != want {
            return Err(format!(
                "node {node} has executed {got:?}, and the level wants {want:?}"
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
Everything a node says about itself is a claim about its disk. A `Promise` says \
\"my promise is now durably at least this high\"; an `Accepted` says \"this value \
is durably recorded here\". A proposer counts those claims toward a quorum and \
then treats the slot as decided — so a claim that turns out not to be durable is \
not a lost message, it is a decision unmade.

That is why a node's output arrives in **batches**: a set of durable writes and a \
set of messages, produced together, with a fixed order between them. Flush first, \
send second. Get it backwards and a node that crashes in the window reboots \
having forgotten a promise it published — free to vote for a ballot it had sworn \
to refuse — or having forgotten a vote a proposer already counted. Either one \
chooses two values for one slot.

You will be asked the order on every batch, and then you will cut a batch in half \
on purpose. `crash before sync` throws the whole batch away: nothing written, \
nothing sent, and the disk is exactly what it was — always safe, because nobody \
was ever told anything. `crash after sync, before send` keeps the writes and \
loses the messages: the node now holds a promise **nobody in the cluster has ever \
heard about**, and when it reboots it still holds it. That asymmetry is the \
whole rule. Restart both nodes and check: no promise ever goes backwards.",
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
                "node {}'s durable promise is below a promise it had already made. Nothing in \
                 the protocol survives that.",
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
                "A value is chosen, both seams were cut, and every node came back holding every \
                 promise it had made. The batch that was flushed and never sent is the safe \
                 half: a node may know more than the cluster, never less."
                    .to_string(),
            ),
            (_, _, _, false) => GoalStatus::Open(format!(
                "Restart node {} and see what it reads back.",
                down[0]
            )),
            (false, _, _, _) => GoalStatus::Open(
                "Cut a batch before its flush: arm `crash before sync` and deliver something that \
                 makes the node write."
                    .to_string(),
            ),
            (_, false, _, _) => GoalStatus::Open(
                "Cut a batch after its flush and before its send: that is the one that leaves a \
                 promise nobody heard about."
                    .to_string(),
            ),
            (_, _, false, _) => {
                GoalStatus::Open("Now get a value chosen through the survivors.".to_string())
            }
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "The question is never which is faster. Ask what the message would be claiming if \
             the node died a moment after sending it."
                .to_string(),
        ),
        _ => Some(
            "Flush the writes, then send. A `Promise` is a statement about the disk, so the disk \
             has to be true first."
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
of them, so Multi-Paxos runs the same protocol independently at every **slot** of \
a log, and the application executes the chosen commands in slot order. \
Independently is the important word: slot 2 does not wait for slot 1. A leader \
streams `Accept`s for several slots at once — that is pipelining, and it is what \
makes the log fast — so the slots come back in whatever order the network feels \
like.

Which splits one word into two. A slot is **chosen** when a quorum has voted for \
it: that is a fact about the cluster, it is permanent, and it can happen at any \
slot at any time. A slot is **applied** when this node has handed it to its state \
machine: that is local, and it is strictly in order, because two nodes that \
executed the same commands in different orders are two different databases. So a \
node keeps a contiguous *applied prefix*, and a slot chosen above a hole simply \
waits.

Propose three commands, then deliver slot 2's votes before slot 1's, and answer \
for the replica each time a slot becomes chosen. Applying the later slot early is \
not a mistake you can take back: the application has already run the command.",
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
            "Every node executed the same three commands in the same order — including the one \
             that was chosen before the slot in front of it, and executed after it."
                .to_string(),
        ),
        Err(detail) => GoalStatus::Open(format!(
            "Get all three commands chosen and applied everywhere, in slot order. {detail}"
        )),
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Compare the slot that just became chosen with the first slot the node is still \
             missing. They are only sometimes the same."
                .to_string(),
        ),
        _ => Some(
            "A slot is applied exactly when it is the first unchosen one. Anything above a hole \
             is held — recorded as chosen, executed later, the moment the hole closes."
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
Running Phase 1 for every slot would cost two round trips per command and let two \
proposers collide on each one. Multi-Paxos gets rid of both by electing one \
**leader** that runs Phase 1 exactly once — and not for a slot, for *every slot \
from here on*. That is what the `from_slot` in a `Prepare` means, and it is why \
one short message can claim an unbounded suffix of the log: Phase 1 never \
mentions a value, so there is nothing slot-specific in it to repeat.

The reply is what makes it work. A `Promise` reports **everything** the acceptor \
has accepted at or above that slot, so a single exchange tells the new leader the \
whole state of the log the previous one left behind. Some of those slots may \
already be chosen and some may not, and — this is the part that catches people — \
the new leader cannot tell which. A value reported by one acceptor is exactly \
what an already-chosen value looks like from here, so the value-selection rule \
from Act I applies per slot: re-propose what you were told, under your own \
ballot, before you propose anything of your own.

One node here still holds a value from a leadership that ended: it accepted it, \
and then that leader vanished. Tick a follower until its election timer fires, \
run its Phase 1 through the node that holds the value, and settle the inherited \
slot before you stream anything new. Then give the cluster a fresh command and \
watch it cost one round trip instead of two.",
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
            "The inherited value was decided under the new ballot before anything new was \
             streamed, and the fresh command landed above it. Phase 1 ran once, for the whole \
             suffix."
                .to_string(),
        ),
        Err(detail) => GoalStatus::Open(format!(
            "Settle the slot the promise quorum reported, then get a fresh command chosen. \
             {detail}"
        )),
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Read what the Promise reported for that slot before you answer. The new leader's \
             own client has nothing to do with it."
                .to_string(),
        ),
        _ => Some(
            "A slot a Promise described may already be chosen — you cannot tell. Re-propose the \
             reported value under your ballot; your own command waits for a slot above it."
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
With a leader in place the protocol collapses to its cheapest possible shape: one \
round trip per command. The leader hands the command the next free slot, sends \
`Accept`, counts votes, and the slot is chosen — no `Prepare`, because the ballot \
it won already covers every slot it will ever use. Lamport's note on this is not \
that it is fast but that it is *optimal*: Phase 2 alone is the minimum cost any \
fault-tolerant agreement algorithm can have.

It does not wait, either. Propose three commands and the leader opens three \
rounds at once; each decides when its own quorum answers. What the followers do \
**not** get from that is the news that a slot was decided — the votes go to the \
leader, and the decision happens there. So the leader piggybacks its commit \
watermark on the `Heartbeat` it was already sending to hold its position: no \
extra message, no extra round trip, and a follower learns how far the log is \
settled as a side effect of being told the leader is alive.

Drop the commit messages and watch the followers stay stuck with three accepted \
values and an empty applied prefix. Then tick the leader once, deliver the beat, \
and watch all three execute at once. When the whole cluster has caught up, \
heartbeat delivery stops being your job: it is the first thing the game automates \
for you, and every later level assumes it.",
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
            "Three commands, three round trips, and the followers learned the whole thing from \
             a watermark riding a beat the leader was sending anyway. Heartbeat delivery is \
             yours to automate from here on."
                .to_string(),
        ),
        Err(detail) => GoalStatus::Open(format!(
            "Stream all three commands and get them executed on every node. {detail}"
        )),
    },
    // This level asks no question, so there are no mistakes to count: the hint
    // watches the world instead, and appears exactly when a node is holding
    // votes it has not been told the fate of.
    hint: |world, mistakes| {
        (mistakes > 1 || holding_undecided(world)).then(|| {
            "The votes go to the leader, so the decision happens there and the followers are \
             told nothing. A follower finds out from the commit index on the next beat — tick \
             the leader, then deliver the beat."
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
Pipelining has a failure mode that nothing else in the protocol repairs. The \
leader streams the `Accept`s for slot 0 and slot 1 together; slot 0's reach \
nobody but the leader itself, slot 1's reach a quorum, and slot 1 is chosen. Then \
the leader crashes. Its round map was volatile — it dies with the leadership, \
which is exactly why this level makes you *crash* the leader rather than cut it \
off — so nobody re-sends slot 0's `Accept`s, ever.

Now count what the next leader can see. Its promise quorum excludes the dead \
leader, so **no** `Promise` mentions slot 0; and `next_slot`, the first slot it \
will hand out, is derived from the accepted log, so it steps straight over the \
hole. Nothing will ever propose slot 0 again — not after a restart either, \
because a reboot recomputes `next_slot` the same way. And a hole is not a local \
blemish: every node's contiguous applied prefix stops one below it, cluster-wide \
and forever. Higher slots keep being chosen and never execute, reads are fenced \
below the hole, and catch-up cannot help, because every node is stuck at the same \
place and no peer has anything to replay.

So the new leader has a second duty beside re-proposing what it was told: it must \
fill every slot below its frontier that **no** `Promise` described, with a `Noop` \
of its own. That is safe for precisely the reason Phase 1 exists. A value already \
chosen there was accepted by a Phase-2 quorum; that quorum shares a member with \
this promise quorum; so it would have been reported. Silence from a full quorum \
is not ignorance — it is a licence.",
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
                "Slot {} is chosen and slot {} is not, so the applied prefix is frozen below it. \
                 Nothing will propose slot {} unless a new leadership does.",
                highest.0, hole.0, hole.0
            ));
        }
        let executed = applied(world, 1);
        let filled = executed.iter().any(|command| command == "Noop");
        let alpha = executed.iter().any(|command| command == "\"alpha\"");
        let bravo = executed.iter().any(|command| command == "\"bravo\"");
        match (filled, alpha && bravo) {
            (true, true) => GoalStatus::Reached(format!(
                "No hole anywhere, and the client's commands are executed: {}. The Noop is not a \
                 value anybody wanted — it is the proof that the slot was free, bought by quorum \
                 intersection.",
                executed.join(", ")
            )),
            (true, false) => GoalStatus::Open(
                "The hole is filled. The command that was lost with the old leader was never \
                 chosen, though: the client has to ask again."
                    .to_string(),
            ),
            (false, _) => GoalStatus::Open(
                "Get a new leadership to account for every slot below its frontier.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Ask what licenses each answer. Re-proposing needs a value somebody reported; \
             leaving the slot alone needs somebody who will propose it later."
                .to_string(),
        ),
        _ => Some(
            "Nobody reported that slot, and a full promise quorum's silence means nothing was \
             ever chosen there. Fill it with a Noop so the prefix can move past it."
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
A node is two things: a disk and a mind. The mind — the role, the ballot it \
operates under, the rounds it has in flight, the reads it owes an answer to — is \
volatile and dies with the process, and that is deliberate: it means a crash is \
an abdication, and no fence has to be written to make one. The disk is the \
promise and the accepted records, and it is the only thing the protocol's safety \
argument ever depended on. Crash a node at any step of a decision and restart it: \
it comes back a follower who remembers exactly what it swore.

There is one way to break that, and it is not losing a write. It is keeping the \
wrong one. Here a node has been holding an accepted value from an old ballot and \
was left out of the new leader's promise quorum, so the cluster went on and chose \
something else at that slot. When the decision finally reaches it — as a `Commit`, \
or replayed by a catch-up — its own record contradicts it at a *lower* ballot. \
Keep that record and the disk is now a lie: after a restart the node reports it as \
its accepted value, the next election's promise quorum can report it as the \
highest anybody accepted, and a fresh leader would dutifully re-propose it **over \
a value that is already chosen**. That is the stale-accept resurrection, and \
overwriting is not an optimization — it is what makes a restart safe.

Play the acceptor's side of that. Crash and restart around every step, and when \
the contradiction arrives, decide which record the disk keeps.",
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
                "node {}'s durable promise came back lower than a promise it had already made.",
                node.0
            ));
        }
        let down = crashed(world);
        if let Some(node) = down.first() {
            return GoalStatus::Open(format!(
                "Restart node {node} and read its disk back — that is the whole question."
            ));
        }
        let wrong: Vec<u64> = log
            .pool()
            .iter()
            .filter(|id| {
                log.disk(**id).is_none_or(|disk| {
                    disk.records()
                        .get(&Slot(0))
                        .is_none_or(|(_, command)| show_command(command) != "\"fresh\"")
                })
            })
            .map(|id| id.0)
            .collect();
        if wrong.is_empty() {
            GoalStatus::Reached(
                "Every disk holds the chosen value at slot 0, every promise came back at least \
                 as high as it went down, and the stale record that would have been resurrected \
                 by the next election is gone."
                    .to_string(),
            )
        } else {
            GoalStatus::Open(format!(
                "Get the chosen value onto every disk at slot 0 — node(s) {wrong:?} still hold \
                 something else."
            ))
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Ask what the next election's promise quorum would report if this node were in it, \
             and what a fresh leader would do with that report."
                .to_string(),
        ),
        _ => Some(
            "The ballot that chose the value is higher than the ballot of the record held here. \
             The choosing ballot wins: overwrite, or a restart resurrects a value nobody chose."
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
A write is safe because a quorum voted for it. A read has no quorum — it changes \
nothing, so there is nothing to vote on — and that is exactly why it is the \
easiest thing in the system to get wrong. The tempting answer is that the leader \
just knows: it has the whole log, so let it answer from memory. But leadership is \
a **belief**, not a fact a node can check locally. Nothing tells a leader it has \
been replaced. From the inside, \"my followers are quiet\" and \"a newer ballot has \
been committing without me for a minute\" are the same silence.

So a read has to prove leadership at the moment it is asked, and the proof costs \
no log write at all: capture the watermark the read must observe, broadcast a \
beat, and wait for a **Phase-2 quorum** to ack *that* beat — not an older one. A \
quorum of acks to a beat sent after the read began means no other ballot could \
have been committing behind this node's back, because any quorum that decided \
something shares a member with this one. Then, once the applied prefix covers the \
captured watermark, answer.

Here there are five nodes, a leader that has been replaced without noticing, and \
one follower that has not heard the news either. The old leader will collect \
exactly one ack — its own vote plus one is two of five — and it will feel like \
progress. Decide whether that is enough, then ask the real leader the same \
question and watch the proof complete. The rule the level is checking is the only \
one that matters to a client: a read never goes behind a write that was already \
acknowledged.",
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
                "A read served by node {} observed {} while a write had already been \
                 acknowledged at {}. That read lied.",
                node.0,
                at(*index),
                at(acked)
            ));
        }
        let unserved = reads.iter().filter(|(_, _, served)| !*served).count();
        match (served.len(), unserved) {
            (0, _) => GoalStatus::Open(
                "Ask for a linearizable read and answer for the leader that has to prove itself."
                    .to_string(),
            ),
            (_, 0) => GoalStatus::Open(
                "Ask the deposed leader for a read as well: the interesting answer is the one it \
                 must not give."
                    .to_string(),
            ),
            (_, _) => GoalStatus::Reached(format!(
                "One read was served, at or above the last acknowledged write ({}), and one is \
                 still waiting at a node that cannot prove it leads — which is the correct thing \
                 for it to do forever.",
                at(acked)
            )),
        }
    },
    hint: |_world, mistakes| match mistakes {
        0..=1 => None,
        2..=3 => Some(
            "Count the acks against the configuration, not against the nodes that answered. Two \
             of five is not a quorum of five."
                .to_string(),
        ),
        _ => Some(
            "A read is served on a quorum of acks to a beat sent *after* the read began, and \
             only once the applied prefix covers the watermark it captured. Anything less and \
             the leader is answering from a belief."
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
