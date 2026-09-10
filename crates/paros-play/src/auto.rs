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
    /// A grid leader addresses each slot to its own column, on its own.
    GridColumn,
    /// A node serves a completed quorum read on its own.
    QuorumReadServe,
    /// A leader's repair probe settles a damaged slot on its own.
    RepairVerdict,
    /// The engine refuses a wiped node's boot on its own.
    WipedRejoin,
    /// A candidate decides on its own whether its cross-configuration Phase 1
    /// is complete.
    Phase1Complete,
    /// A candidate abandons a stale belief on its own.
    StaleConfiguration,
    /// A matchmaker fences a request from another generation on its own.
    GenerationFence,
    /// The engine decides on its own whether a node may retire.
    MayRetire,
    /// Heartbeats and their acks are delivered without being clicked.
    DeliverHeartbeats,
    /// `Promise`, `Accepted` and `Nack` are delivered without being clicked.
    DeliverReplies,
    /// Matchmaker requests and their answers are delivered without being
    /// clicked.
    DeliverMatchmakerReplies,
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
    AutomationFlag::GridColumn,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::RepairVerdict,
    AutomationFlag::WipedRejoin,
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
    AutomationFlag::DeliverHeartbeats,
    AutomationFlag::DeliverReplies,
    AutomationFlag::DeliverMatchmakerReplies,
    AutomationFlag::ResendPending,
];

impl AutomationFlag {
    /// A short label for the toggle.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            AutomationFlag::AcceptorReplies => "answer a Prepare and an Accept",
            AutomationFlag::CommitOverwrite => "overwrite a stale record",
            AutomationFlag::ProposerP2c => "apply the P2c rule",
            AutomationFlag::ReplicaApply => "apply a chosen slot",
            AutomationFlag::LeaderRecovery => "recover and fill the gaps",
            AutomationFlag::PersistOrder => "persist the batch before the send",
            AutomationFlag::ReadServe => "serve a confirmed read",
            AutomationFlag::SnapshotPromise => "keep the higher promise",
            AutomationFlag::AckWrite => "answer a client retry",
            AutomationFlag::GridColumn => "address a slot to its column",
            AutomationFlag::QuorumReadServe => "serve a quorum read",
            AutomationFlag::RepairVerdict => "settle a damaged slot",
            AutomationFlag::WipedRejoin => "refuse a wiped node",
            AutomationFlag::Phase1Complete => "judge a Phase 1 across two configurations",
            AutomationFlag::StaleConfiguration => "abandon a stale belief",
            AutomationFlag::GenerationFence => "fence a matchmaker generation",
            AutomationFlag::MayRetire => "answer a retire request",
            AutomationFlag::DeliverHeartbeats => "deliver the heartbeats",
            AutomationFlag::DeliverReplies => "deliver the replies",
            AutomationFlag::DeliverMatchmakerReplies => "deliver the matchmaker messages",
            AutomationFlag::ResendPending => "re-send the pending Accepts",
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
/// 4. `DeliverMatchmakerReplies`: deliver the lowest-id matchmaker-plane
///    message — a registration, a garbage-collection request, a handover step,
///    or any of their answers.
/// 5. Back to 1 until no delivery was made.
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
    let matchmaker = automation.is_on(AutomationFlag::DeliverMatchmakerReplies);
    if !beats && !replies && !matchmaker {
        return;
    }
    let mut budget = PUMP_BUDGET;
    loop {
        if world.prompt().is_some() {
            return;
        }
        let Some(id) = world.next_auto_delivery(beats, replies, matchmaker) else {
            return;
        };
        assert!(budget > 0, "the automation pump reaches quiescence");
        budget -= 1;
        // A delivery the pump chose off the wire cannot be refused: the id came
        // from the wire this instant and no prompt is open.
        let _ = world.deliver(id);
    }
}
