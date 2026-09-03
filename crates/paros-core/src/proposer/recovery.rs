//! The **bounded recovery** a fresh leadership drains: the slots its Phase 1
//! recovered, the holes its [`RecoveryPolicy`] licenses it to fill, and the
//! cursor that hands them out one page at a time.

use std::collections::{BTreeMap, BTreeSet};

use super::Proposer;
use crate::types::{Command, Slot};

/// What licenses a fresh leadership to fill a slot its recovery does not
/// name: an explicit policy, never a flag, because the two answers rest on
/// two different safety arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPolicy {
    /// The recovery came out of a won Phase 1. Quorum intersection guarantees
    /// a value already chosen at an unreported slot would have been reported,
    /// so an unreported slot is genuinely free and is filled with a
    /// [`Control::Noop`] (the election gap fill).
    Phase1Backed,
    /// The recovery was inherited through a cooperative handoff, which ran
    /// **no** Phase 1: there is no quorum report behind it, so a slot the
    /// predecessor did not explicitly describe is simply skipped — filling
    /// it could overwrite a value chosen under an older ballot.
    Inherited,
}

/// Bounded continuation for a leadership's recovered suffix.
#[derive(Clone, Debug)]
pub struct Recovery {
    /// Highest-ballot command reported for each retained slot.
    pub(super) recovered: BTreeMap<Slot, Command>,
    /// Slots the Phase-1 tally could not decide (Case 3: wait): neither
    /// re-proposed nor no-op-filled by the pump; the open [`RepairProbe`]
    /// resolves them as stragglers answer.
    pub(super) blocked: BTreeSet<Slot>,
    /// Next slot to recover or fill.
    pub(super) cursor: Slot,
    /// One past the highest slot covered by the recovery.
    pub(super) end: Slot,
    /// Whether an undescribed slot may be filled.
    pub(super) policy: RecoveryPolicy,
}

impl Recovery {
    /// The policy this recovery runs under.
    #[must_use]
    pub fn policy(&self) -> RecoveryPolicy {
        self.policy
    }

    /// Whether `slot` is blocked on the repair probe (Case 3: wait).
    #[must_use]
    pub fn is_blocked(&self, slot: Slot) -> bool {
        self.blocked.contains(&slot)
    }

    /// How many slots the cursor has still to sweep.
    #[must_use]
    pub fn remaining(&self) -> usize {
        usize::try_from(self.end.0.saturating_sub(self.cursor.0)).unwrap_or(usize::MAX)
    }
}

/// One step of a recovery pump ([`Proposer::recovery_next`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryStep {
    /// The recovery names a command for this slot: re-propose it (P2c).
    Recovered(Command),
    /// Nobody reported the slot and the policy is [`RecoveryPolicy::Phase1Backed`]:
    /// fill it with a [`Control::Noop`].
    Fill,
    /// Nobody described the slot and the policy is [`RecoveryPolicy::Inherited`]:
    /// skip it.
    Undescribed,
}

impl Proposer {
    // ---- recovery -----------------------------------------------------------

    /// The open recovery continuation, if any.
    #[must_use]
    pub fn recovery(&self) -> Option<&Recovery> {
        self.recovery.as_ref()
    }

    /// Open the bounded recovery of `[cursor, end)`: `recovered` names the
    /// command per slot, `blocked` the slots the repair probe owns, and
    /// `policy` what an undescribed slot means.
    ///
    /// # Panics
    ///
    /// If a recovery is already open, or an inherited recovery blocks a slot
    /// (a handoff ran no Phase 1, so nothing could have been blocked by one).
    pub fn open_recovery(
        &mut self,
        recovered: BTreeMap<Slot, Command>,
        blocked: BTreeSet<Slot>,
        cursor: Slot,
        end: Slot,
        policy: RecoveryPolicy,
    ) {
        assert!(
            self.recovery.is_none(),
            "one recovery continuation per leadership"
        );
        assert!(
            policy == RecoveryPolicy::Phase1Backed || blocked.is_empty(),
            "an inherited recovery blocks no slot"
        );
        self.recovery = Some(Recovery {
            recovered,
            blocked,
            cursor,
            end,
            policy,
        });
    }

    /// Advance the recovery cursor one slot and say what the pump does with
    /// it. `None` when no recovery is open or its range is drained.
    pub fn recovery_next(&mut self) -> Option<(Slot, RecoveryStep)> {
        let recovery = self.recovery.as_mut()?;
        if recovery.cursor >= recovery.end {
            return None;
        }
        let slot = recovery.cursor;
        recovery.cursor = Slot(recovery.cursor.0.saturating_add(1));
        let step = match recovery.recovered.remove(&slot) {
            Some(command) => RecoveryStep::Recovered(command),
            // Only a Phase-1-backed recovery may invent a value for a slot
            // nobody reported (see `RecoveryPolicy`).
            None => match recovery.policy {
                RecoveryPolicy::Phase1Backed => RecoveryStep::Fill,
                RecoveryPolicy::Inherited => RecoveryStep::Undescribed,
            },
        };
        Some((slot, step))
    }

    /// Whether the open recovery holds `slot` blocked on the repair probe.
    #[must_use]
    pub fn recovery_blocked(&self, slot: Slot) -> bool {
        self.recovery
            .as_ref()
            .is_some_and(|recovery| recovery.is_blocked(slot))
    }

    /// How many slots the open recovery has still to sweep (0 when none).
    #[must_use]
    pub fn recovery_remaining(&self) -> usize {
        self.recovery.as_ref().map_or(0, Recovery::remaining)
    }

    /// Close the recovery once its cursor swept the whole range.
    ///
    /// # Panics
    ///
    /// If a drained recovery left a recovered slot at or past its cursor
    /// unvisited (consumed entries are removed as the cursor passes them;
    /// what survives is only the below-range residue the caller's prefix
    /// heal already handled).
    pub fn close_drained_recovery(&mut self) {
        if self.recovery_remaining() != 0 {
            return;
        }
        if let Some(recovery) = self.recovery.take() {
            assert!(
                recovery.cursor >= recovery.end,
                "a closed recovery drained its range"
            );
            assert!(
                recovery.recovered.range(recovery.cursor..).next().is_none(),
                "a closed recovery leaves no recovered slot unvisited"
            );
        }
    }
}
