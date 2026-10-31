//! Act V — moving and splitting the leader (Compartmentalized Paxos).
//!
//! Act IV changed who counts. Act V changes **who does the work**. A classic
//! Multi-Paxos leader sequences every command and also broadcasts it: it sends
//! each `Accept`, counts each `Accepted`, and announces each `Commit`. The
//! acceptors vote on the log and they are also its only learners. Act V takes
//! those jobs apart, one process per job, as Whittaker et al. do in
//! *Scaling Replicated State Machines with Compartmentalization* (2021).
//!
//! - `act5/proxy-leaders`: the leader only sequences; proxy leaders broadcast.
//! - `act5/the-dead-proxy`: a proxy dies with a round, and the leader takes it
//!   back.
//! - `act5/handoff-through-a-proxy`: the leadership moves while a proxy holds a
//!   round, and the successor re-delegates it to the same proxy.
//! - `act5/the-replica-tier`: replicas learn the log and never vote.
//!
//! These levels replace the `proxy_leader` and `replica_tier` lessons that
//! `paros-core` once carried as examples. Every reference is recorded by a
//! `Script`, as in the other acts.

use paros_core::membership::QuorumSystem;
use paros_core::{Config, NodeId, Slot};

use crate::paxos::action::{Action, ActionKind};
use crate::paxos::auto::AutomationFlag;
use crate::paxos::level::common::{
    CLIENT, REPLIES_AND_BEATS, REPLIES_ONLY, TIMEOUT, on_log, peers, propose, start_election, tick,
};
use crate::paxos::level::script::{Script, kind};
use crate::paxos::level::{GoalStatus, Level, WorldKind};
use crate::paxos::view::{MessageView, PartyView, show_command};
use crate::paxos::world::{Disk, World};

/// Act V's levels, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &PROXY_LEADERS,
        &THE_DEAD_PROXY,
        &HANDOFF_THROUGH_A_PROXY,
        &THE_REPLICA_TIER,
    ]
}

/// The acceptor grid the proxy levels lay out: two rows of three.
const GRID: QuorumSystem = QuorumSystem::grid(2, 3);

/// How many proxy leaders the proxy levels deploy.
const PROXIES: usize = 2;

/// The acceptors of the replica level, and the replicas beside them.
const ACCEPTORS: u64 = 3;
const REPLICAS: &[u64] = &[10, 11];

/// Every prompt-governing flag: Act V teaches deployments, not a role's
/// answer, so every role answers for itself.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::TrimPoint,
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

// ---- worlds -----------------------------------------------------------------

/// Six nodes in a `2 × 3` grid and two proxy leaders beside them.
fn proxied() -> WorldKind {
    let disks = peers(6)
        .into_iter()
        .map(|id| {
            Disk::new(Config {
                peers: peers(6),
                quorum_system: GRID,
                proxy_count: PROXIES,
                ..Config::new(id, paros_core::journal::JournalIdentifier::UNSET)
            })
        })
        .collect();
    WorldKind::Log(Box::new(
        World::from_disks(disks, &[CLIENT], TIMEOUT).with_proxies(PROXIES),
    ))
}

/// Three acceptors under a majority, and two replicas beside them.
fn tiered() -> WorldKind {
    let disks = peers(ACCEPTORS)
        .into_iter()
        .map(|id| {
            Disk::new(Config {
                peers: peers(ACCEPTORS),
                replica_count: REPLICAS.len(),
                ..Config::new(id, paros_core::journal::JournalIdentifier::UNSET)
            })
        })
        .collect();
    WorldKind::Log(Box::new(
        World::from_disks(disks, &[CLIENT], TIMEOUT).with_replicas(REPLICAS),
    ))
}

// ---- reading the world for a goal -------------------------------------------

/// The client's writes chosen at every node of the pool, in slot order — or
/// `None` while some node has not learned them all.
fn writes_everywhere(log: &World, at_least: usize) -> Option<Vec<String>> {
    let mut agreed: Option<Vec<String>> = None;
    for id in log.pool() {
        let disk = log.disk(*id)?;
        let writes: Vec<String> = disk
            .applied()
            .iter()
            .map(|(_, command)| show_command(command))
            .filter(|command| !command.starts_with("Noop"))
            .collect();
        if writes.len() < at_least {
            return None;
        }
        match &agreed {
            None => agreed = Some(writes),
            Some(first) if *first == writes => {}
            Some(_) => return None,
        }
    }
    agreed
}

/// Whether a message is addressed to a proxy leader.
fn to_proxy(message: &MessageView) -> bool {
    message.to_party == PartyView::Proxy
}

// ---- 30. proxy leaders ------------------------------------------------------

const PROXY_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::ResendPending,
    ActionKind::CrashProxy,
    ActionKind::RestartProxy,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act5/proxy-leaders`.
pub static PROXY_LEADERS: Level = Level {
    id: "act5/proxy-leaders",
    act: 5,
    title: "The leader only sequences",
    briefing: "\
A Multi-Paxos leader does two jobs for each command. It **sequences**: it \
selects the slot. That job is serial, and it is cheap. It also \
**broadcasts**: it sends the `Accept` to each acceptor of the column, counts \
the `Accepted`s, and sends the `Commit` to each learner. That job grows with \
each acceptor and each learner, and it all goes through one process. So a \
classic leader becomes slower when you add acceptors.

Only the first job must belong to the leader. Here, two **proxy leaders** \
stand beside a grid of six acceptors. The leader still selects the slot and \
still holds the ballot. Then it gives the same `Accept` to one proxy and is \
done until the `Commit` comes back. The proxy sends the `Accept` to the \
column, counts the votes, and announces the `Commit`. The proxy votes on \
nothing and orders nothing, so it cannot break agreement. The proxy of a slot \
is `slot % proxies`, a rule and not a message.

Elect a leader and get three commands chosen through the proxies. Look at \
the wire: for each command, the leader sends one message and receives one \
message, however wide the grid is.",
    field_guide: "play.html",
    symbols: &[
        "ProxyLeader",
        "ProxyId::of",
        "Delegation::Auto",
        "ColocatedNode::propose_in",
        "docs/references/papers — Whittaker et al., Compartmentalized Paxos §3.1",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[],
    allowed_actions: PROXY_ACTIONS,
    setup: proxied,
    goal: |world| {
        on_log(world, |log| {
            if log.leader().is_none() {
                return GoalStatus::Open(
                    "Elect a leader. A whole row answers Phase 1.".to_string(),
                );
            }
            let decided: Vec<u64> = log
                .proxies()
                .iter()
                .map(|proxy| proxy.role().map_or(0, |role| role.counters().decided))
                .collect();
            let total: u64 = decided.iter().sum();
            match writes_everywhere(log, 3) {
                Some(_) if total >= 3 && decided.iter().all(|count| *count > 0) => {
                    GoalStatus::Reached(format!(
                        "All six nodes learned three commands. The proxies decided {total} \
                         slots ({} and {}), and the leader counted no vote. For each slot it \
                         sent one Accept to a proxy and learned the Commit back.",
                        decided[0], decided[1]
                    ))
                }
                Some(_) => GoalStatus::Open(
                    "The commands are chosen, but not all through the proxies. A proxy count \
                     of zero is the classic leader; here, each slot goes to proxy slot % 2."
                        .to_string(),
                ),
                None => GoalStatus::Open(
                    "Get three commands chosen and learned on all six nodes.".to_string(),
                ),
            }
        })
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Deliver the Accept that goes to a proxy first. The proxy then sends one Accept \
             to each node of the slot's column."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act5/proxy-leaders");
        script.play(start_election(1)).settle_all();
        for value in ["alpha", "bravo", "charlie"] {
            script.play(propose(1, value)).settle_all();
        }
        script.finish()
    },
};

// ---- 31. the dead proxy -----------------------------------------------------

/// `act5/the-dead-proxy`.
pub static THE_DEAD_PROXY: Level = Level {
    id: "act5/the-dead-proxy",
    act: 5,
    title: "The dead proxy",
    briefing: "\
A proxy leader keeps nothing durable and votes on nothing. So when a proxy \
dies, no decision is lost. But a round that the proxy held stops: nobody \
counts its votes and nobody announces its `Commit`.

The leader is responsible for liveness. On each beat it delegates each open \
round again, to the same proxy. After a budget of re-delegations without a \
`Commit`, the leader **takes the round back** and runs it colocated: it sends \
the `Accept` to the column itself, as a classic leader does. The budget is \
driver policy, not a rule of the protocol. Here the budget is two.

Get one command chosen. Then crash the proxy of the next slot before its \
delegation arrives, and propose the next command. Re-send the leader's \
pending rounds twice, then tick the leader. The leader takes the round back, \
and the slot is chosen while the proxy is still down.",
    field_guide: "play.html",
    symbols: &[
        "ColocatedNode::take_back_delegated",
        "ColocatedNode::delegated_rounds",
        "ColocatedNode::resend_pending",
        "ProxyLeader::expire_stale",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[],
    allowed_actions: PROXY_ACTIONS,
    setup: proxied,
    goal: |world| {
        on_log(world, |log| {
            let Some(taken) = log.taken_back().first().copied() else {
                return GoalStatus::Open(
                    "Crash the proxy of the next slot, propose a command, and make the leader \
                     take the round back."
                        .to_string(),
                );
            };
            let down = log.proxy(taken.proxy).is_none();
            let chosen = log.pool().iter().all(|id| {
                log.node(*id)
                    .is_some_and(|node| node.replica().chosen_at(taken.slot).is_some())
            });
            match (chosen, down) {
                (true, true) => GoalStatus::Reached(format!(
                    "Node {} took slot {} back from proxy {} and ran it colocated. All six \
                     nodes learned the slot, and proxy {} is still down. Liveness under a dead \
                     proxy is the leader's job, and the fallback is the classic Phase 2.",
                    taken.leader.0, taken.slot.0, taken.proxy.0, taken.proxy.0
                )),
                (true, false) => GoalStatus::Reached(format!(
                    "Node {} took slot {} back from proxy {} and ran it colocated, and the \
                     slot is chosen everywhere.",
                    taken.leader.0, taken.slot.0, taken.proxy.0
                )),
                (false, _) => GoalStatus::Open(format!(
                    "Node {} took slot {} back. Deliver its Accepts to the column, and the \
                     votes back to the leader.",
                    taken.leader.0, taken.slot.0
                )),
            }
        })
    },
    hint: |world, mistakes| {
        let delegated = world.log().is_some_and(|log| {
            log.leader()
                .and_then(|id| log.node(id))
                .is_some_and(|node| !node.delegated_rounds().is_empty())
        });
        (mistakes > 0 || delegated).then(|| {
            "Use \"re-send pending\" on the leader two times, then tick the leader. The tick \
             is the beat that takes the round back."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act5/the-dead-proxy");
        script.play(start_election(1)).settle_all();
        script.play(propose(1, "alpha")).settle_all();
        script.play(propose(1, "bravo"));
        let proxy = script
            .wire()
            .iter()
            .find(|message| message.kind == "Accept" && to_proxy(message))
            .map(|message| message.to)
            .expect("the leader delegated the slot");
        script.play(Action::CrashProxy { proxy }).settle_all();
        for _ in 0..2 {
            script.play(Action::ResendPending { node: 1 }).settle_all();
        }
        script.play(tick(1)).settle_all();
        script.finish()
    },
};

// ---- 32. handoff through a proxy --------------------------------------------

const HANDOFF_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Relinquish,
    ActionKind::ResendPending,
    ActionKind::CrashProxy,
    ActionKind::RestartProxy,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act5/handoff-through-a-proxy`.
pub static HANDOFF_THROUGH_A_PROXY: Level = Level {
    id: "act5/handoff-through-a-proxy",
    act: 5,
    title: "A handoff through a proxy",
    briefing: "\
A cooperative handoff moves a leadership without an election: the old leader \
sends its ballot, its next free slot and its open rounds to one peer. On a \
proxy deployment an open round can be at a proxy when the leadership moves.

The successor needs no message about the proxy. The proxy of a slot is \
`slot % proxies`, so the successor computes the same proxy, and it \
delegates the inherited round there again under the **same ballot**. The \
proxy still holds the round. It refreshes the leader that it names, sends the \
`Accept` again, and finishes the round. Nothing is decided twice, because one \
ballot still has one proposer.

Get one command chosen. Propose a second command and deliver its delegation \
to the proxy, but keep the `Accept`s that the proxy sends in flight. Move the \
leadership to node 4, deliver the `Relinquish`, and then deliver the rest.",
    field_guide: "play.html",
    symbols: &[
        "ColocatedNode::relinquish_to",
        "ProxyLeader::delegator",
        "ProxyCounters::refanned",
        "LeadershipOrigin::Handoff",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: REPLIES_ONLY,
    unlocks: &[],
    allowed_actions: HANDOFF_ACTIONS,
    setup: proxied,
    goal: |world| {
        on_log(world, |log| {
            let Some(handoff) = log.handoffs().first().copied() else {
                return GoalStatus::Open(
                    "Get a command chosen, open a second one at a proxy, and move the \
                     leadership to node 4."
                        .to_string(),
                );
            };
            let Some(node) = log.node(handoff.to) else {
                return GoalStatus::Open("The successor is not running.".to_string());
            };
            if !node.is_leader() {
                return GoalStatus::Open(format!(
                    "Deliver the Relinquish to node {}.",
                    handoff.to.0
                ));
            }
            if node.ballot() != handoff.ballot {
                return GoalStatus::Failed(format!(
                    "Node {} leads at a ballot of its own. It did not inherit the authority.",
                    handoff.to.0
                ));
            }
            let refanned: u64 = log
                .proxies()
                .iter()
                .map(|proxy| proxy.role().map_or(0, |role| role.counters().refanned))
                .sum();
            let inherited = Slot(handoff.next_slot.0.saturating_sub(1));
            let chosen = log.pool().iter().all(|id| {
                log.node(*id)
                    .is_some_and(|node| node.replica().chosen_at(inherited).is_some())
            });
            match (refanned > 0, chosen) {
                (true, true) => GoalStatus::Reached(format!(
                    "Node {} leads under ballot {}.{}, which node {} won. It delegated slot {} \
                     again to the proxy that held it, and the proxy finished the round. All \
                     six nodes learned the slot.",
                    handoff.to.0,
                    handoff.ballot.round,
                    handoff.ballot.node.0,
                    handoff.from.0,
                    inherited.0
                )),
                (false, _) => GoalStatus::Open(
                    "The handoff went through, but no proxy held the round when it moved. \
                     Open the second command at its proxy first, then move the leadership."
                        .to_string(),
                ),
                (true, false) => GoalStatus::Open(format!(
                    "The proxy has the round again. Deliver the votes, so that slot {} is \
                     chosen.",
                    inherited.0
                )),
            }
        })
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Deliver only the Accept that goes to the proxy. Keep the Accepts that the proxy \
             sends in flight, then move the leadership."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act5/handoff-through-a-proxy");
        script.play(start_election(1)).settle_all();
        script.play(propose(1, "alpha")).settle_all();
        script.play(propose(1, "bravo"));
        script.settle(|message| message.kind == "Accept" && to_proxy(message));
        script.play(Action::Relinquish { node: 1, to: 4 });
        script.settle(kind("Relinquish"));
        script.settle_all();
        script.finish()
    },
};

// ---- 33. the replica tier ---------------------------------------------------

const REPLICA_ACTIONS: &[ActionKind] = &[
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::Tick,
    ActionKind::StartElection,
    ActionKind::Propose,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

/// `act5/the-replica-tier`.
pub static THE_REPLICA_TIER: Level = Level {
    id: "act5/the-replica-tier",
    act: 5,
    title: "The replica tier",
    briefing: "\
Until now each node was an acceptor and also a learner. That couples two \
things. An extra acceptor makes each quorum bigger, so it makes the protocol \
**slower**. An extra learner only adds a process that reads the chosen log, \
so it adds read capacity for free. On one process they cannot grow apart.

Here, three acceptors vote, learn and keep the chosen log. Beside them, two \
**replicas** learn the log from the `Commit`s and never vote. A replica \
learns in slot order. When it misses a `Commit`, it waits at the hole, and \
the next beat of the leader shows it that it is behind. It then asks an \
acceptor for the missing range. The acceptors keep the chosen log for this \
reason: a lagging replica heals from them.

Get three commands chosen. Drop the `Commit` of the second command to \
replica 11, and get the third command chosen. Then tick the leader, and let \
replica 11 catch up.",
    field_guide: "play.html",
    symbols: &[
        "ReplicaNode",
        "Message::CatchUpRequest",
        "AcceptorConfig::reply_owner",
        "docs/references/papers — Whittaker et al., Compartmentalized Paxos §3.3",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: REPLIES_AND_BEATS,
    unlocks: &[],
    allowed_actions: REPLICA_ACTIONS,
    setup: tiered,
    goal: |world| {
        on_log(world, |log| {
            let Some(acceptors) = writes_everywhere(log, 3) else {
                return GoalStatus::Open(
                    "Get three commands chosen by the three acceptors.".to_string(),
                );
            };
            let learned: Vec<Vec<String>> = log
                .replicas()
                .iter()
                .map(|replica| {
                    replica
                        .learned()
                        .iter()
                        .map(|(_, command)| show_command(command))
                        .filter(|command| !command.starts_with("Noop"))
                        .collect()
                })
                .collect();
            let caught_up = log
                .replica(NodeId(11))
                .map_or(0, crate::paxos::world::ReplicaProcess::caught_up);
            let same = learned.iter().all(|writes| *writes == acceptors);
            match (same, caught_up > 0) {
                (true, true) => GoalStatus::Reached(format!(
                    "Both replicas learned the same {} commands as the acceptors, in the same \
                     order. Replica 11 missed a Commit and learned {} from an acceptor. The \
                     replicas voted on nothing.",
                    acceptors.len(),
                    if caught_up == 1 {
                        "one slot".to_string()
                    } else {
                        format!("{caught_up} slots")
                    }
                )),
                (true, false) => GoalStatus::Open(
                    "Both replicas learned the log, but none of them caught up. Drop one \
                     Commit to replica 11 and try again."
                        .to_string(),
                ),
                (false, _) => GoalStatus::Open(
                    "A replica is behind the acceptors. Tick the leader: its beat tells the \
                     replica how far the log is chosen."
                        .to_string(),
                ),
            }
        })
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "A replica that waits at a hole asks an acceptor for the missing range after the \
             leader's next Heartbeat."
                .to_string()
        })
    },
    reference: || {
        let mut script = Script::new("act5/the-replica-tier");
        script.play(start_election(0)).settle_all();
        script.play(propose(0, "alpha")).settle_all();
        script.play(propose(0, "bravo"));
        script.settle(|message| message.kind != "Commit");
        script.drop_all(|message| {
            message.kind == "Commit" && message.to_party == PartyView::Replica && message.to == 11
        });
        script.settle_all();
        script.play(propose(0, "charlie")).settle_all();
        script.play(tick(0)).settle_all();
        script.finish()
    },
};
