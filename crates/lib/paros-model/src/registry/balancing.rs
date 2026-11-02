//! What the cell control journal records of its balancer (#687 (rebalance a
//! cell), [`crate::balance`]): whether it is paused, the move in flight and
//! the last moves that ended.
//!
//! The registry is the balancer's one store (Pierre, 2026-11-01): no journal
//! of its own. A move writes two entries more than a move off a draining
//! machine, `BalanceStarted` and, when it gives up, `BalanceAbandoned`. A move
//! ends by itself when the fold releases its booking, so a finished move
//! writes nothing more. A later coordinator term reads the move in flight
//! here and goes on with it.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;

use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_wire::system as wire;

/// How many ended moves the registry keeps: what `cell balance` shows.
pub const ENDED_KEPT: usize = 8;

/// Why the balancer moves an acceptor (#687).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BalanceReason {
    /// Its machine hosts more roles than another of its zone.
    Roles,
    /// Its machine is busier than another of its zone (#424 (busyness
    /// metrics)).
    Busy,
}

impl BalanceReason {
    /// Its wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            BalanceReason::Roles => "roles",
            BalanceReason::Busy => "busy",
        }
    }

    pub(crate) fn parse(text: &str) -> Result<Self, &'static str> {
        match text {
            "roles" => Ok(BalanceReason::Roles),
            "busy" => Ok(BalanceReason::Busy),
            _ => Err("a balance move names an unknown reason"),
        }
    }
}

impl fmt::Display for BalanceReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One balance move (#687): acceptor booking `booking` of `journal` leaves
/// `from` for `to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BalanceMove {
    /// The booking that leaves.
    pub booking: u64,
    /// Its journal.
    pub journal: JournalIdentifier,
    /// The machine it leaves.
    pub from: NodeId,
    /// The machine it goes to.
    pub to: NodeId,
    /// Why.
    pub reason: BalanceReason,
    /// The registry position it started at.
    pub started: u64,
}

/// How a balance move ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BalanceOutcome {
    /// Its booking was released: the journal moved.
    Done,
    /// The balancer gave it up.
    Abandoned,
}

/// A balance move that ended, at registry position `at`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EndedMove {
    /// The move.
    pub moved: BalanceMove,
    /// How it ended.
    pub outcome: BalanceOutcome,
    /// The position it ended at.
    pub at: u64,
}

/// What the registry holds of the balancer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BalanceState {
    paused: bool,
    requests: BTreeSet<u64>,
    moving: Option<BalanceMove>,
    ended: VecDeque<EndedMove>,
}

impl BalanceState {
    /// Whether an operator paused the balancer.
    #[must_use]
    pub fn paused(&self) -> bool {
        self.paused
    }

    /// Whether balance request `request` was applied.
    #[must_use]
    pub fn applied(&self, request: u64) -> bool {
        self.requests.contains(&request)
    }

    /// The move in flight.
    #[must_use]
    pub fn moving(&self) -> Option<&BalanceMove> {
        self.moving.as_ref()
    }

    /// The last moves that ended, oldest first, at most [`ENDED_KEPT`].
    pub fn ended(&self) -> impl Iterator<Item = &EndedMove> {
        self.ended.iter()
    }

    /// Whether it holds nothing: a checkpoint then writes no state.
    pub(crate) fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Apply request `request`. `false` when it was applied already.
    pub(crate) fn set(&mut self, request: u64, paused: bool) -> bool {
        if !self.requests.insert(request) {
            return false;
        }
        self.paused = paused;
        assert!(self.applied(request), "an applied request is recorded");
        true
    }

    /// Start `moved`. The caller checked the booking.
    pub(crate) fn start(&mut self, moved: BalanceMove) {
        assert!(self.moving.is_none(), "one balance move at a time");
        assert!(!self.paused, "a paused balancer starts no move");
        assert_ne!(moved.from, moved.to, "a balance move changes machines");
        self.moving = Some(moved);
    }

    /// End the move in flight of `booking` as `outcome`, at `at`. `false`
    /// when no move of that booking is in flight.
    pub(crate) fn end(&mut self, booking: u64, outcome: BalanceOutcome, at: u64) -> bool {
        let Some(moved) = self.moving.filter(|m| m.booking == booking) else {
            return false;
        };
        self.moving = None;
        self.ended.push_back(EndedMove { moved, outcome, at });
        while self.ended.len() > ENDED_KEPT {
            self.ended.pop_front();
        }
        assert!(
            self.ended.len() <= ENDED_KEPT,
            "the ended moves are bounded"
        );
        true
    }

    pub(crate) fn to_wire(&self) -> Option<wire::BalanceState> {
        if self.is_empty() {
            return None;
        }
        Some(wire::BalanceState {
            paused: self.paused,
            requests: self.requests.iter().copied().collect(),
            moving: self.moving.map(|m| move_to_wire(m, None)),
            ended: self
                .ended
                .iter()
                .map(|e| move_to_wire(e.moved, Some((e.outcome, e.at))))
                .collect(),
        })
    }

    pub(crate) fn from_wire(state: wire::BalanceState) -> Result<Self, &'static str> {
        let moving = state.moving.as_ref().map(move_from_wire).transpose()?;
        if moving.is_some_and(|(_, ended)| ended.is_some()) {
            return Err("a balance move in flight has not ended");
        }
        let mut ended = VecDeque::new();
        for record in &state.ended {
            let (moved, end) = move_from_wire(record)?;
            let (outcome, at) = end.ok_or("an ended balance move names its end")?;
            ended.push_back(EndedMove { moved, outcome, at });
        }
        if ended.len() > ENDED_KEPT {
            return Err("a balance state keeps too many ended moves");
        }
        // A pause stops new moves only: one in flight goes on, so a paused
        // state may hold one.
        let moving = moving.map(|(moved, _)| moved);
        Ok(Self {
            paused: state.paused,
            requests: state.requests.into_iter().collect(),
            moving,
            ended,
        })
    }
}

fn move_to_wire(m: BalanceMove, end: Option<(BalanceOutcome, u64)>) -> wire::BalanceMoveState {
    wire::BalanceMoveState {
        booking: m.booking,
        tenant: m.journal.tenant.0,
        journal: m.journal.journal.0,
        from: m.from.0,
        to: m.to.0,
        reason: m.reason.as_str().into(),
        started: m.started,
        ended: end.map_or(0, |(_, at)| at),
        abandoned: end.is_some_and(|(o, _)| o == BalanceOutcome::Abandoned),
    }
}

type MoveRecord = (BalanceMove, Option<(BalanceOutcome, u64)>);

fn move_from_wire(m: &wire::BalanceMoveState) -> Result<MoveRecord, &'static str> {
    let moved = BalanceMove {
        booking: m.booking,
        journal: JournalIdentifier::new(TenantId(m.tenant), JournalId(m.journal)),
        from: NodeId(m.from),
        to: NodeId(m.to),
        reason: BalanceReason::parse(&m.reason)?,
        started: m.started,
    };
    if moved.from == moved.to {
        return Err("a balance move changes machines");
    }
    let end = match (m.ended, m.abandoned) {
        (0, false) => None,
        (0, true) => return Err("an abandoned balance move names its end"),
        (at, true) => Some((BalanceOutcome::Abandoned, at)),
        (at, false) => Some((BalanceOutcome::Done, at)),
    };
    Ok((moved, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn moved(booking: u64) -> BalanceMove {
        BalanceMove {
            booking,
            journal: JournalIdentifier::new(TenantId(7), JournalId(booking)),
            from: NodeId(1),
            to: NodeId(2),
            reason: BalanceReason::Roles,
            started: 3,
        }
    }

    #[test]
    fn a_request_applies_once() {
        let mut state = BalanceState::default();
        assert!(state.set(5, true));
        assert!(state.paused());
        assert!(!state.set(5, false), "a repeat changes nothing");
        assert!(state.paused());
        assert!(state.set(6, false));
        assert!(!state.paused());
    }

    #[test]
    fn a_move_ends_once_and_the_ended_moves_are_bounded() {
        let mut state = BalanceState::default();
        for booking in 1..=10 {
            state.start(moved(booking));
            assert!(
                !state.end(booking + 100, BalanceOutcome::Done, 9),
                "not this move"
            );
            assert!(state.end(booking, BalanceOutcome::Done, booking + 10));
            assert!(
                !state.end(booking, BalanceOutcome::Done, 9),
                "ended already"
            );
        }
        assert_eq!(state.ended().count(), ENDED_KEPT);
        assert_eq!(state.ended().next().map(|e| e.moved.booking), Some(3));
        assert!(state.moving().is_none());
    }

    #[test]
    fn the_state_round_trips_through_its_wire_form() {
        assert_eq!(BalanceState::default().to_wire(), None);
        let mut state = BalanceState::default();
        state.start(moved(1));
        assert!(state.end(1, BalanceOutcome::Abandoned, 4));
        state.start(moved(2));
        assert!(state.set(9, true));
        let wire = state.to_wire().expect("a state that holds something");
        assert_eq!(BalanceState::from_wire(wire), Ok(state));
    }
}
