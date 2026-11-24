//! One **register operation** (§J3.4; `specs/tla/cas-register/`,
//! `specs/tla/segment-records/`, `Read` and `Write`).
//!
//! An operation at generation `op`:
//!
//! 1. reads at a majority of one matchmaker generation and takes the value
//!    of the highest write;
//! 2. checks the guard of its [`Change`] on that value
//!    ([`RegisterValue::apply`]); a refusal ends it;
//! 3. writes the new value at the same generation, and is **done** when a
//!    majority took the write.
//!
//! **Act on a write seen done, never on a read** (#1275,
//! `segment-records/` control `drop_read`): only [`CallStep::Done`] says
//! that the value holds. A read can show a write that only a minority took.
//! So the operation writes even when its change leaves the value as it is
//! (a reshape that the register already took, a [`Change::Confirm`]): the
//! write makes the value done.
//!
//! A member that took a later read or write refuses the write
//! ([`CallStep::Conflict`]); a member of a later generation names it
//! ([`CallStep::Moved`]). The caller then opens a new operation above. A
//! write that a minority took may still be seen by a later read, so a caller
//! that retries reads the register again and applies its change to what it
//! finds.

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::Ballot;
use paros_consensus::membership::{MatchmakerId, MatchmakerSet};

use super::message::{Reply, Request, Written};
use super::value::{Change, ChangeRefusal, RegisterValue};

/// Where an operation stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallPhase {
    /// Reading: the answers so far.
    Reading(BTreeMap<MatchmakerId, Written>),
    /// Writing `value`: the members that took it.
    Writing {
        /// The value the read gave.
        seen: RegisterValue,
        /// The new value.
        value: RegisterValue,
        /// The members that took the write.
        acks: BTreeSet<MatchmakerId>,
    },
    /// Done: a majority took the write.
    Done(RegisterValue),
    /// Ended without a done write: refused, in conflict or moved.
    Ended,
}

/// What one reply did to an operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallStep {
    /// Nothing that the caller acts on.
    Pending,
    /// The read is done at a majority: the operation now writes `value`
    /// over `seen`.
    Write {
        /// The value of the highest write the read saw.
        seen: RegisterValue,
        /// The value it writes.
        value: RegisterValue,
    },
    /// A majority took the write: `value` holds.
    Done(RegisterValue),
    /// The guard refused the change on the value the read saw.
    Refused {
        /// Why.
        refusal: ChangeRefusal,
        /// The value the read saw. It may be a write that a minority took:
        /// never act on it.
        seen: RegisterValue,
    },
    /// A later operation read or wrote at a member: open a new operation
    /// above `above`.
    Conflict {
        /// The highest generation the member named.
        above: Ballot,
    },
    /// The set hands over: run the operation again at `set`, or wait while
    /// `set` is `None` (the handover is not chosen yet).
    Moved {
        /// The later set, once a member knows it.
        set: Option<MatchmakerSet>,
    },
}

/// One operation on the register.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterCall {
    op: Ballot,
    set: MatchmakerSet,
    change: Change,
    phase: CallPhase,
}

impl RegisterCall {
    /// Operation `op` of `change`, at the matchmaker set `set`.
    #[must_use]
    pub fn new(op: Ballot, set: MatchmakerSet, change: Change) -> Self {
        Self {
            op,
            set,
            change,
            phase: CallPhase::Reading(BTreeMap::new()),
        }
    }

    /// The operation generation.
    #[must_use]
    pub fn op(&self) -> Ballot {
        self.op
    }

    /// The set the operation runs at.
    #[must_use]
    pub fn set(&self) -> &MatchmakerSet {
        &self.set
    }

    /// The change it asks.
    #[must_use]
    pub fn change(&self) -> &Change {
        &self.change
    }

    /// Where it stands.
    #[must_use]
    pub fn phase(&self) -> &CallPhase {
        &self.phase
    }

    /// The requests of the phase in flight, to the members that did not
    /// answer it: what the caller sends, and sends again on a timeout.
    #[must_use]
    pub fn requests(&self) -> Vec<(MatchmakerId, Request)> {
        let generation = self.set.generation;
        match &self.phase {
            CallPhase::Reading(answers) => self
                .set
                .members()
                .iter()
                .filter(|member| !answers.contains_key(member))
                .map(|member| {
                    (
                        *member,
                        Request::Read {
                            generation,
                            op: self.op,
                        },
                    )
                })
                .collect(),
            CallPhase::Writing { value, acks, .. } => self
                .set
                .members()
                .iter()
                .filter(|member| !acks.contains(member))
                .map(|member| {
                    (
                        *member,
                        Request::Write {
                            generation,
                            op: self.op,
                            value: value.clone(),
                        },
                    )
                })
                .collect(),
            CallPhase::Done(_) | CallPhase::Ended => Vec::new(),
        }
    }

    /// Fold the reply of member `from`.
    ///
    /// # Panics
    ///
    /// When two members hold different values at one write generation (one
    /// operation writes one value), or an assertion on its own
    /// postconditions fails.
    pub fn on_reply(&mut self, from: MatchmakerId, reply: Reply) -> CallStep {
        if !self.set.contains(from) {
            return CallStep::Pending;
        }
        match reply {
            Reply::Value { op, written } if op == self.op => self.on_value(from, written),
            Reply::Wrote { op } if op == self.op => self.on_wrote(from),
            Reply::Conflict { op, read, written } if op == self.op => {
                if matches!(self.phase, CallPhase::Writing { .. }) {
                    self.phase = CallPhase::Ended;
                    CallStep::Conflict {
                        above: read.max(written),
                    }
                } else {
                    CallStep::Pending
                }
            }
            refused @ Reply::Refused { .. } => {
                if matches!(self.phase, CallPhase::Done(_) | CallPhase::Ended) {
                    return CallStep::Pending;
                }
                let later = refused.later_set(self.set.generation);
                if later.is_some() {
                    self.phase = CallPhase::Ended;
                }
                CallStep::Moved { set: later }
            }
            _ => CallStep::Pending,
        }
    }

    fn on_value(&mut self, from: MatchmakerId, written: Written) -> CallStep {
        let CallPhase::Reading(answers) = &mut self.phase else {
            return CallStep::Pending;
        };
        answers.insert(from, written);
        let voters: BTreeSet<MatchmakerId> = answers.keys().copied().collect();
        if !self.set.has_quorum(&voters) {
            return CallStep::Pending;
        }
        let top = Written::highest(answers.values()).clone();
        match top.value.apply(&self.change) {
            Ok(value) => {
                self.phase = CallPhase::Writing {
                    seen: top.value.clone(),
                    value: value.clone(),
                    acks: BTreeSet::new(),
                };
                CallStep::Write {
                    seen: top.value,
                    value,
                }
            }
            Err(refusal) => {
                self.phase = CallPhase::Ended;
                CallStep::Refused {
                    refusal,
                    seen: top.value,
                }
            }
        }
    }

    fn on_wrote(&mut self, from: MatchmakerId) -> CallStep {
        let CallPhase::Writing { value, acks, .. } = &mut self.phase else {
            return CallStep::Pending;
        };
        acks.insert(from);
        if !self.set.has_quorum(acks) {
            return CallStep::Pending;
        }
        let value = value.clone();
        self.phase = CallPhase::Done(value.clone());
        CallStep::Done(value)
    }
}

#[cfg(test)]
mod tests;
