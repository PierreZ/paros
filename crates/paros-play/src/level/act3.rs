//! Act III — truncation, the trim point, and reads.
//!
//! Five levels over the **log world**, and one question runs through all of
//! them: what does a node know, and what is it entitled to *say*? Act II built
//! a log that grows. This act makes it shrink — which creates a node nothing
//! can replay to — and then makes it answer questions, which is where the gap
//! between "chosen", "applied" and "acknowledged" stops being pedantry and
//! starts being the difference between a correct database and a lying one.
//!
//! The order is the order the mechanisms depend on each other: truncation
//! first (it is what strands a node), then the trim-point jump that rescues it, then
//! the read path — the quorum read at a fresh leader, the client-visible
//! property it exists for — and finally the write ack,
//! which is the other half of that property.
//!
//! Every reference solution here is **recorded**, not written: a private
//! `Script` plays the level, choosing messages by what they are rather than by
//! id and answering every prompt with the answer `paros-core` itself gives.

use paros_core::{NodeId, QuorumSystem, Slot};

use crate::action::{Action, ActionKind};
use crate::auto::AutomationFlag;
use crate::level::common::{
    CLIENT, REPLIES_AND_BEATS, TIMEOUT, accepted, applied, crash, fresh, is_phase2, on_log,
    propose_as, quorum_read_as, restart, slot_traffic, start_election, tick,
};
use crate::level::script::{Script, kind, to};
use crate::level::{GoalStatus, Level, WorldKind};
use crate::narration::at;
use crate::world::RetryAnswer;

/// Act III's levels, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &TRUNCATE_BY_CONSENSUS,
        &THE_STRANDED_NODE,
        &THE_FRESH_LEADER_TRAP,
        &LINEARIZABLE_OR_NOT,
        &CHOSEN_IS_NOT_APPLIED,
    ]
}

/// The second client, for the linearizability level. Two clients is the
/// smallest history in which "before" and "after" are not the same party's
/// program order.
const OTHER: u64 = 8;

/// Every role answered for the player. A level pins off the ones it teaches
/// (`pinned_off`), which keeps them off whatever this list says.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::TrimPoint,
    AutomationFlag::AckWrite,
];

// ---- reading the world for a goal -------------------------------------------

/// Every node's compaction floor, deduplicated — one entry means one
/// cluster-wide floor.
fn distinct_floors(world: &WorldKind) -> Vec<u64> {
    let Some(log) = world.log() else {
        return Vec::new();
    };
    let mut floors: Vec<u64> = log.floors().iter().map(|(_, first)| first.0).collect();
    floors.sort_unstable();
    floors.dedup();
    floors
}

// ---- action shorthands ------------------------------------------------------

fn compact(node: u64, up_to: u64) -> Action {
    Action::Compact { node, up_to }
}

fn retry(client: u64, node: u64, seq: u64) -> Action {
    Action::Retry { node, client, seq }
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

Each node drops only what it has itself chosen: the `Truncate` names the last \
slot it permits dropping, and a node clamps that to its own chosen prefix. Ask \
the leader to compact, and deliver the decision. Look at the floor on every \
node: each floor moves when that node applies the decision, and never when the \
leader proposes it.",
    field_guide: "truncation-and-snapshots.html",
    symbols: &[
        "Control::Truncate",
        "ColocatedNode::propose_control",
        "ColocatedNode::compact",
        "WriteOp::Truncate",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[],
    allowed_actions: TRUNCATE_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
    goal: |world| {
        on_log(world, |log| {
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
            match (accepted, floors.as_slice()) {
                (true, [first]) if *first > 0 => GoalStatus::Reached(format!(
                    "The floor of every node is slot {first}, and no node is stranded. No message \
                 sent that number. Each node computed it when it applied the same decided \
                 command, at the same place in the same log."
                )),
                (false, _) => GoalStatus::Open(
                    "Get some values chosen, then ask the leader to compact the log.".to_string(),
                ),
                (_, floors) => GoalStatus::Open(format!(
                    "The floors are still {floors:?}. Deliver the decision to every node. Each \
                 node truncates when it *applies* that slot, not when the leader proposes it."
                )),
            }
        })
    },
    hint: |world, mistakes| {
        let asked = world
            .log()
            .is_some_and(|log| log.compacts().iter().any(|outcome| outcome.accepted));
        (mistakes > 0 || asked).then(|| {
            "The Truncate is a slot like any other. Deliver its Accepts and its Commit, and \
             every node drops its prefix when it applies that slot."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act3/truncate-by-consensus");
        script.play(start_election(0)).settle_all();
        script
            .play(propose_as(CLIENT, 0, "alpha"))
            .play(propose_as(CLIENT, 0, "bravo"))
            .settle_all();
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

The answer is that the node does not need those slots at all. Everything below \
a floor is **chosen**: a decided `Truncate` put the floor there, and a node \
truncates only what it has chosen. When a peer sees a catch-up request below its \
floor, it answers with its **trim point**: \"my log starts at this slot\". The \
node jumps there. It raises its own floor to that slot, counts every slot below \
it as chosen, and asks for the rest by ordinary catch-up. No bytes travel. The \
application folds the chosen values into its own state, and paros runs no \
application.

One rule is the subject of this level: a trim point says where the **log** \
starts, and it says nothing about **promises**. It carries no ballot, and the \
peer that sent it does not know what this node promised. The node that jumps \
keeps the promise it made, exactly as it was. Node 2 comes back, hears no \
leader, and campaigns. That campaign asks its peers for the range it misses, and \
a peer answers with its trim point. The game then asks you what node 2 does, and \
a wrong answer either waits forever for slots that no disk holds or lets the \
node vote for a ballot that it refused.",
    field_guide: "truncation-and-snapshots.html",
    symbols: &[
        "Message::TrimmedTo",
        "WriteOp::TrimmedTo",
        "Acceptor::trim_to",
        "Replica::trim_to",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::TrimPoint],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[AutomationFlag::TrimPoint],
    allowed_actions: STRANDED_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
    goal: |world| {
        on_log(world, |log| {
            if let Some(node) = log.promise_regressed() {
                return GoalStatus::Failed(format!(
                    "The durable promise of node {} came back lower than a promise that it \
                 already made. A trim point says where the log starts, not what a node promised.",
                    node.0
                ));
            }
            let leader_log = applied(world, 0);
            if leader_log.is_empty() {
                return GoalStatus::Open(
                    "Get some commands chosen through the two nodes that are up.".to_string(),
                );
            }
            let cluster_floor = log.disk(NodeId(0)).map_or(0, |disk| disk.floor().0);
            if cluster_floor == 0 {
                return GoalStatus::Open(
                    "Ask the leader to compact the log past node 2's position.".to_string(),
                );
            }
            let floor = log.disk(NodeId(2)).map_or(0, |disk| disk.floor().0);
            let chosen = |node: u64| {
                log.disk(NodeId(node))
                    .and_then(|disk| disk.hard_state().chosen_index)
                    .map(|slot| slot.0)
            };
            if floor == 0 {
                return GoalStatus::Open(
                    "Start node 2 again, and let it find that it is below the floor. It campaigns \
                 when it hears no leader, and that campaign asks its peers for the range that \
                 it misses."
                        .to_string(),
                );
            }
            if floor == cluster_floor && chosen(2) >= chosen(0) {
                GoalStatus::Reached(format!(
                    "Node 2's log starts at slot {floor}, like every other node's, and it kept its \
                 promise. No peer replayed the slots below it, because they do not exist any \
                 more. A peer told node 2 where its log starts, and everything below that slot \
                 is chosen."
                ))
            } else {
                GoalStatus::Open(format!(
                    "Node 2's floor is slot {floor}, and the cluster's is slot {cluster_floor}."
                ))
            }
        })
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Everything below the peer's trim point is chosen, and no disk holds it any more. \
             What is left to wait for?"
                .to_string(),
        ),
        _ => Some(
            "Jump to the trim point and keep your promise. A node must not take back a \
             promise, and a trim point carries no ballot."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act3/the-stranded-node");
        // Node 2 is away for the whole of the cluster's progress.
        script.play(crash(2));
        script.play(start_election(0)).settle_all();
        script
            .play(propose_as(CLIENT, 0, "alpha"))
            .play(propose_as(CLIENT, 0, "bravo"))
            .settle_all();
        // Truncate past node 2's position.
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
        script.settle(kind("TrimmedTo"));
        script.answer_all();
        script.finish()
    },
};

// ---- 16. the fresh-leader trap ----------------------------------------------

const TRAP_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::QuorumRead,
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
A leader that just won an election holds a valid quorum, and it can still be \
behind. Its applied prefix can miss a value that the last leader started. \
Election recovery re-proposes that slot, and until the slot decides again, the \
local state of the new leader does not hold the value. A read that the new \
leader answers from its own state can therefore miss a write.

A quorum read does not trust the leader. It asks a **Phase-1 quorum** for the \
highest slot that each acceptor voted in. An acceptor raises that number when \
it **votes**, not when a slot becomes chosen. The acceptor that holds the \
inherited value reports its slot, so the read waits for that slot. The read \
costs the client time, and it does not give the client an old answer. Raft \
commits a no-op in the new term before it serves a read; paros waits for the \
slot instead, and no leader takes part.

In this level one other node accepted one command from the old leader. The old \
leader then stopped before the decision came back, so nothing is chosen and no \
node applied anything. The Phase 1 of the new leader finds that value and must \
re-propose it. Ask the new leader for a read while the recovery is still in \
flight: the quorum answers, but the answer is still no. Then let the slot \
decide again, and the read completes on its own, in the batch that applied the \
slot.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "ColocatedNode::quorum_read",
        "Acceptor::vote_watermark",
        "Replica::covers",
        "RecoveryStep::Recovered",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::QuorumReadServe],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[],
    allowed_actions: TRAP_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
    goal: |world| {
        on_log(world, |log| {
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
                "The new leader held the read while the recovered slot was still in flight. It \
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
        })
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "The number of answers is not the question. Compare the highest slot that the \
             quorum voted in with the applied prefix of this node."
                .to_string(),
        ),
        _ => Some(
            "An acceptor voted in the inherited slot, so the quorum reports that slot. This \
             node did not apply it yet. Wait, because the read completes by itself when the \
             recovered slot decides."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act3/the-fresh-leader-trap");
        script.play(start_election(0)).settle_all();
        // One command reaches exactly one other node, and its Accepted never
        // comes back: accepted somewhere, chosen nowhere.
        script.play(propose_as(CLIENT, 0, "alpha"));
        script.settle(|message| message.kind == "Accept" && message.to == 1);
        script.drop_all(|message| matches!(message.kind.as_str(), "Accept" | "Accepted"));
        // The leadership dies with the round that would have re-sent it.
        script.play(crash(0));
        // Node 1 campaigns with node 2. Its Phase 1 finds the value at node 1
        // itself, and it re-proposes it.
        script.play(start_election(1));
        script.settle(|message| !is_phase2(message));
        // The read: node 1 voted in the inherited slot, so the quorum reports
        // it, and node 1 has applied nothing yet.
        script.play(quorum_read_as(CLIENT, 1));
        script.settle(|message| matches!(message.kind.as_str(), "PreRead" | "PreReadAck"));
        script.answer_all();
        // Now let the recovered slot decide; the read is served with the batch
        // that applies it.
        script.settle_all();
        script.finish()
    },
};

// ---- 17. linearizable or not ------------------------------------------------

const HISTORY_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::QuorumRead,
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
leads, for a read. The partition stops every answer to it, so it cannot get the \
answers of a Phase-1 quorum. That read stays open for the rest of the level, and \
the protocol is correct to hold a read that it cannot prove.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "ReadState",
        "ColocatedNode::quorum_read",
        "QuorumReads::serve",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[],
    allowed_actions: HISTORY_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT, OTHER]),
    goal: |world| {
        on_log(world, |log| {
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
        })
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "The read of the replaced leader must stay open. Look at the answers to its \
             question: the partition drops each one, so it never holds a Phase-1 quorum."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act3/linearizable-or-not");
        script.play(start_election(0)).settle_all();
        // Client 7's write is acknowledged under the old leadership.
        script.play(propose_as(CLIENT, 0, "alpha")).settle_all();
        // Node 1 takes the leadership with node 2; node 0 hears none of it and
        // goes on believing it leads.
        script.play(start_election(1));
        script.settle(to(2));
        script.settle(|message| message.to == 1);
        script.drop_all(to(0));
        // The deposed leader is asked for a read it will never be able to
        // prove: every answer to it is dropped.
        script.play(quorum_read_as(CLIENT, 0));
        script.drop_all(to(0));
        script.settle(to(2));
        script.drop_all(to(0));
        // Client 8 writes and then reads at the leader that really leads.
        script.play(propose_as(OTHER, 1, "bravo"));
        script.settle(|message| message.to != 0);
        script.drop_all(to(0));
        script.play(quorum_read_as(OTHER, 1));
        script.settle(|message| message.to != 0);
        script.drop_all(to(0));
        script.finish()
    },
};

// ---- 18. chosen is not applied ----------------------------------------------

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
while slot 5 is still open. A slot is **applied** when this node folds it into \
the journal, and that step is strictly in order. Slot 6 therefore waits for \
slot 5. Between those two moments the write is decided and not judged, and a \
node that acked it would promise the client an unreadable result.

A client retry is the **same write** sent again: the same writer, the same \
position, the same bytes. The leader keeps no table of what it has seen. It \
gives the retry the next free slot like any write, and the journal answers it \
when that slot folds. By then the original has folded below it, so the \
position holds exactly this write and the retry is a duplicate: acked, and \
nothing moves. At-most-once is a property of the log.

In this level the second write of the client is chosen above a hole. The client \
asks twice: once while the hole is open, and once after it closes. Say, both \
times, what the journal answers as far as the leader has folded it.",
    field_guide: "linearizable-reads.html",
    symbols: &[
        "JournalState::apply",
        "Outcome::Duplicate",
        "Replica::accepted_at",
        "Replica::outcome_at",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::AckWrite],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[AutomationFlag::AckWrite],
    allowed_actions: RETRY_ACTIONS,
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
    goal: |world| {
        on_log(world, |log| {
            let executed = accepted(world, 0);
            // At most once, for every value the client asked for: which values
            // those are is the client's business, and the history reports them.
            let asked = log.proposed_values();
            if let Some(twice) = asked
                .iter()
                .find(|value| executed.iter().filter(|command| command == value).count() > 1)
            {
                return GoalStatus::Failed(format!(
                    "The journal accepted the write {twice} twice. A retry of a write the journal \
                 holds must fold as a duplicate."
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
            match (all_executed, held, acked) {
                (true, true, true) => GoalStatus::Reached(format!(
                    "The journal accepted each write exactly once ({}). The first retry came while \
                 the hole was open: the fold had not reached its position, so it waited in its \
                 own slot and folded as a duplicate. The second came after the hole closed, \
                 and the log already held it. \"Chosen\" and \"applied\" are two different \
                 facts, and a journal answers only from the second.",
                    executed.join(", ")
                )),
                (true, false, _) => GoalStatus::Open(
                    "Let the client ask again *while* its write is chosen above the hole. That \
                 window is the subject of this level."
                        .to_string(),
                ),
                (true, true, false) => GoalStatus::Open(
                    "Now close the hole, and let the client ask again.".to_string(),
                ),
                _ => GoalStatus::Open(
                    "Get both writes chosen, but not in order, so the second waits above a hole. \
                 Then answer the retries of the client."
                        .to_string(),
                ),
            }
        })
    },
    hint: |_world, mistakes| match mistakes {
        0 => None,
        1..=2 => Some(
            "Read the journal's next position on the card, and compare it with the position \
             the retry asks for."
                .to_string(),
        ),
        _ => Some(
            "While the hole is open, the fold stops below the retried position: nothing can be \
             said yet, and the retry waits for its slot. After the hole closes, the position \
             holds exactly this write, and the journal acks it as a duplicate."
                .to_string(),
        ),
    },
    reference: || {
        let mut script = Script::new("act3/chosen-is-not-applied");
        script.play(start_election(0)).settle_all();
        script
            .play(propose_as(CLIENT, 0, "alpha"))
            .play(propose_as(CLIENT, 0, "bravo"));
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
