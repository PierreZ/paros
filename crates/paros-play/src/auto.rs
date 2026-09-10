//! Automation as reward: which decisions the engine makes for the player, and
//! the deterministic pump that makes them.
//!
//! Every flag stands for one decision the player learned to make by hand. A
//! flag that is **off** makes the corresponding [`crate::prompt::PromptKind`]
//! manual — the world parks the message and raises the prompt — and the two
//! delivery flags plus [`AutomationFlag::ResendPending`] additionally run a
//! *pump* after every action. A level pins a flag off when it needs the
//! decision made by hand for teaching, whatever the player unlocked.
//!
//! The pump order is fixed and documented on [`pump`], because replay must be
//! bit-exact: the action log plus the flag set is the whole state.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::level::WorldKind;

/// One automated decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum AutomationFlag {
    /// The acceptor answers `Prepare` and `Accept` on its own (one rule for
    /// both: refuse anything below the promise held).
    AcceptorReplies,
    /// The acceptor replaces a stale lower-ballot record with what the
    /// choosing ballot decided, on its own.
    CommitOverwrite,
    /// A candidate applies the P2c value-selection rule on its own.
    ProposerP2c,
    /// The replica decides on its own whether a newly chosen slot extends the
    /// contiguous prefix.
    ReplicaApply,
    /// A fresh leader recovers and gap-fills on its own.
    LeaderRecovery,
    /// The driver flushes a batch before sending it on its own.
    PersistOrder,
    /// A leader serves a confirmed read on its own.
    ReadServe,
    /// A node installing a peer's snapshot keeps the higher of its own promise
    /// and the snapshot's ballot, on its own.
    SnapshotPromise,
    /// A leader answers a client's retry from its two dedup tables, on its own.
    AckWrite,
    /// Heartbeats and their acks are delivered without being clicked.
    DeliverHeartbeats,
    /// `Promise`, `Accepted` and `Nack` are delivered without being clicked.
    DeliverReplies,
    /// A leader re-sends its pending `Accept`s on every tick.
    ResendPending,
}

/// Every flag, in the order the view lists them.
pub const ALL_FLAGS: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
    AutomationFlag::SnapshotPromise,
    AutomationFlag::AckWrite,
    AutomationFlag::DeliverHeartbeats,
    AutomationFlag::DeliverReplies,
    AutomationFlag::ResendPending,
];

impl AutomationFlag {
    /// A short label for the toggle.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            AutomationFlag::AcceptorReplies => "acceptor replies",
            AutomationFlag::CommitOverwrite => "overwrite a stale record",
            AutomationFlag::ProposerP2c => "proposer P2c",
            AutomationFlag::ReplicaApply => "replica apply",
            AutomationFlag::LeaderRecovery => "leader recovery",
            AutomationFlag::PersistOrder => "persist before send",
            AutomationFlag::ReadServe => "serve reads",
            AutomationFlag::SnapshotPromise => "promise across a snapshot",
            AutomationFlag::AckWrite => "answer a client retry",
            AutomationFlag::DeliverHeartbeats => "deliver heartbeats",
            AutomationFlag::DeliverReplies => "deliver replies",
            AutomationFlag::ResendPending => "re-send pending accepts",
        }
    }
}

/// Which flags this level offers, which are on, and which it pins off.
///
/// `unlocked` is what the level exposes as a toggle — the frontend's progress
/// store decides what a player has earned and the level declares the rest;
/// the engine only enforces that a toggle is unlocked and not pinned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Automation {
    /// The flags this level offers as toggles.
    pub unlocked: BTreeSet<AutomationFlag>,
    /// The flags currently on.
    pub on: BTreeSet<AutomationFlag>,
    /// The flags the level forbids turning on.
    pub pinned_off: BTreeSet<AutomationFlag>,
}

impl Automation {
    /// Whether `flag` is on.
    #[must_use]
    pub fn is_on(&self, flag: AutomationFlag) -> bool {
        self.on.contains(&flag)
    }

    /// Whether `flag` is pinned off by the level.
    #[must_use]
    pub fn is_pinned_off(&self, flag: AutomationFlag) -> bool {
        self.pinned_off.contains(&flag)
    }
}

/// How many deliveries one pump may make before the engine calls it a
/// non-terminating loop. Generous: a level's whole wire is a handful of
/// messages, and the bound only exists so a mistake fails loudly instead of
/// hanging the browser.
const PUMP_BUDGET: usize = 4096;

/// Run every enabled pump after a player action, to quiescence.
///
/// The order is fixed, and it is the order a real driver's loop would take
/// them in:
///
/// 1. Nothing runs while a prompt is open — the player owes an answer, and an
///    automated delivery would move the world under the question.
/// 2. `DeliverHeartbeats`: deliver the lowest-id `Heartbeat` / `HeartbeatAck`
///    on the wire.
/// 3. `DeliverReplies`: deliver the lowest-id `Promise` / `Accepted` / `Nack`.
/// 4. Back to 1 until no delivery was made.
///
/// `ResendPending` is not a pump: re-sending happens on a tick, so the world
/// does it inside [`crate::world::World::tick`] when the flag is on (see
/// [`crate::world::WorldPolicy`]).
///
/// # Panics
///
/// If the pump does not reach quiescence within its delivery budget — a
/// programmer error in the pump's own predicates, never a player-reachable
/// state.
pub fn pump(world: &mut WorldKind, automation: &Automation) {
    let beats = automation.is_on(AutomationFlag::DeliverHeartbeats);
    let replies = automation.is_on(AutomationFlag::DeliverReplies);
    if !beats && !replies {
        return;
    }
    let mut budget = PUMP_BUDGET;
    loop {
        if world.prompt().is_some() {
            return;
        }
        let Some(id) = world.next_auto_delivery(beats, replies) else {
            return;
        };
        assert!(budget > 0, "the automation pump reaches quiescence");
        budget -= 1;
        // A delivery the pump chose off the wire cannot be refused: the id came
        // from the wire this instant and no prompt is open.
        let _ = world.deliver(id);
    }
}
