//! **Held answers** (`docs/journal/02-data-plane.md` §J2.2; decided by Pierre
//! on 2026-11-20, #1232 (held writes)).
//!
//! The consensus leader holds the answer of each command it proposed until
//! the slot is applied. After a leader change, the slot of a deposed leader
//! can choose another command or a `Noop`. So **a held answer belongs to the
//! command, not to the slot**: the leader answers a held command from the
//! outcome of its slot only when the command applied there is the one it
//! proposed there. Else it answers `Unknown`. It may answer `Unknown` at any
//! time (it steps down, it stops). It never answers `NotLeader` for a
//! command that holds a slot: the caller would send it again, and both
//! copies can land.
//!
//! TLC checks the rule in `specs/tla/compartments/` (`AckedChosen`; the
//! control `held_slot` loses an acked write, the control `held_notleader`
//! lands a write two times). A single writer resolves an `Unknown` with a
//! resend at the same `expected_seq`: the fold judges the position.

use std::collections::BTreeMap;

use paros_consensus::Slot;

use crate::command::Command;
use crate::fold::Answer;

/// What the leader tells the caller of a held command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// The slot applied this command: its answer.
    Answer(Answer),
    /// The fate of the command is unknown to this leader: the slot chose
    /// another command, or the leader let its held commands go.
    Unknown,
}

/// The commands a leader proposed and holds the answers of, by slot, each
/// with the caller's `token`.
#[derive(Clone, Debug)]
pub struct Held<T> {
    slots: BTreeMap<Slot, (T, Command)>,
}

impl<T> Default for Held<T> {
    fn default() -> Self {
        Self {
            slots: BTreeMap::new(),
        }
    }
}

impl<T> Held<T> {
    /// No held command.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn new() -> Self {
        let held = Self::default();
        assert!(held.is_empty(), "a new hold is empty");
        held
    }

    /// How many commands are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether no command is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Hold the answer of `command`, which the leader proposed at `slot` for
    /// `token`.
    ///
    /// # Panics
    ///
    /// When the leader already holds a command at `slot`: a leader proposes
    /// one command at each slot of its ballot.
    pub fn hold(&mut self, slot: Slot, token: T, command: Command) {
        let before = self.len();
        let previous = self.slots.insert(slot, (token, command));
        assert!(
            previous.is_none(),
            "a leader proposes one command at a slot"
        );
        assert!(self.len() == before + 1, "a hold adds one command");
    }

    /// The slot `slot` applied `applied` with `answer`. Returns the reply to
    /// the caller of the command held there, if any: the answer when the
    /// slot applied the command the leader proposed, else `Unknown`.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    pub fn applied(
        &mut self,
        slot: Slot,
        applied: &Command,
        answer: &Answer,
    ) -> Option<(T, Reply)> {
        let before = self.len();
        let (token, proposed) = self.slots.remove(&slot)?;
        assert!(
            self.len() + 1 == before,
            "an applied slot releases one hold"
        );
        let reply = if proposed == *applied {
            Reply::Answer(answer.clone())
        } else {
            Reply::Unknown
        };
        assert!(
            !self.slots.contains_key(&slot),
            "an applied slot holds nothing after it"
        );
        Some((token, reply))
    }

    /// Let every held command go, each answered `Unknown`: the leader steps
    /// down or stops. In slot order.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    pub fn release_all(&mut self) -> Vec<(T, Reply)> {
        let released: Vec<(T, Reply)> = std::mem::take(&mut self.slots)
            .into_values()
            .map(|(token, _)| (token, Reply::Unknown))
            .collect();
        assert!(self.is_empty(), "a released hold is empty");
        assert!(
            released.iter().all(|(_, reply)| *reply == Reply::Unknown),
            "a released command is answered Unknown"
        );
        released
    }
}

#[cfg(test)]
mod tests;
