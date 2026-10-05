//! The non-interference oracles (#188): what several journals on the same
//! nodes owe each other.
//!
//! Every per-journal oracle already runs on the journal's own
//! [`AuditWorld`](super::AuditWorld) (one per journal,
//! `audit_world_for`), so safety, the clients' folds, convergence and the
//! storage gates are keyed by journal without knowing it. What no single
//! world can see is the *relation* between journals, and that lives here, on
//! one board every journal's audit port shares:
//!
//! - **safety** — every slot of journal `j` holds only a command appended to
//!   `j` (a message or a slot that crossed journals would apply another
//!   journal's identity), and a journal a storage fault quarantined on a node
//!   sends nothing from that node until it re-opens;
//! - **liveness** — a journal keeps committing while a sibling on the same
//!   nodes is held for the chaos window (`DriverHooks::hold_journal`) or
//!   still recovering from the hold, and a node keeps running its other
//!   journals' protocol (it sends their beats, votes and acks) while one is
//!   quarantined.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{StateHandle, assert_reachable, assert_sometimes};
use paros::JournalIdentifier;

use crate::shape::JournalPlan;

const JOURNAL_BOARD_KEY: &str = "paros-journal-board";

/// The run's cross-journal facts, shared by every journal's audit port.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)] // sticky, independent gate facts
pub(crate) struct JournalBoard {
    /// The run serves more than one journal.
    multi: bool,
    /// The journal held on every node for the chaos window, if any.
    held: Option<JournalIdentifier>,
    /// `(node, journal)` pairs quarantined right now.
    quarantined: BTreeSet<(u64, JournalIdentifier)>,
    /// A journal committed while a sibling was held or not yet caught up.
    committed_while_held: bool,
    /// The held journal applied a slot after its hold ended: it caught up.
    held_caught_up: bool,
    /// A node applied a slot of one journal while another of its journals
    /// was quarantined.
    served_while_quarantined: bool,
    /// Some journal was ever quarantined on some node.
    quarantined_ever: bool,
}

/// The run's [`JournalBoard`] (`crate::state::published`).
pub(crate) fn journal_board(state: &StateHandle) -> Arc<Mutex<JournalBoard>> {
    crate::state::published(state, JOURNAL_BOARD_KEY, JournalBoard::default)
}

impl JournalBoard {
    /// Record the run's journal plan (idempotent: every node arms the same).
    pub(crate) fn arm(&mut self, plan: &JournalPlan) {
        self.multi = plan.is_multi();
        self.held = plan.held;
    }

    /// Whether the run serves more than one journal.
    pub(crate) fn is_multi(&self) -> bool {
        self.multi
    }

    /// `journal` was quarantined on `node`.
    pub(crate) fn quarantine(&mut self, node: u64, journal: JournalIdentifier) {
        self.quarantined.insert((node, journal));
        self.quarantined_ever = true;
    }

    /// `journal` booted on `node` (at a process boot or a re-open).
    pub(crate) fn reopened(&mut self, node: u64, journal: JournalIdentifier) {
        self.quarantined.remove(&(node, journal));
    }

    /// Whether `journal` is quarantined on `node` right now.
    pub(crate) fn is_quarantined(&self, node: u64, journal: JournalIdentifier) -> bool {
        self.quarantined.contains(&(node, journal))
    }

    /// `node` applied a slot of `journal`; `in_chaos` says the chaos window
    /// (the hold) is still open. The held journal stays stalled past the
    /// window until it re-elects and commits again, so a sibling's commit
    /// counts until then: commits inside the 4 s window alone are rare
    /// (a run's first leaders are still being elected).
    pub(crate) fn applied(&mut self, journal: JournalIdentifier, in_chaos: bool) {
        let Some(held) = self.held else {
            return;
        };
        if held == journal {
            self.held_caught_up |= !in_chaos;
        } else if in_chaos || !self.held_caught_up {
            if !self.committed_while_held {
                assert_reachable!("journal: a journal commits while a sibling is held");
            }
            self.committed_while_held = true;
        }
    }

    /// `node` sent a message of `journal`: it is running that journal's
    /// protocol, which counts as serving it while a sibling is quarantined
    /// on the same node.
    pub(crate) fn sent(&mut self, node: u64, journal: JournalIdentifier) {
        if self
            .quarantined
            .iter()
            .any(|(n, j)| *n == node && *j != journal)
        {
            self.served_while_quarantined = true;
        }
    }

    /// The liveness gates, once per run. Each is evaluated only on a run
    /// whose cause fired, so a seed that never held or quarantined a journal
    /// never counts against them.
    pub(crate) fn check_gates(&self) {
        if !self.multi {
            return;
        }
        if self.held.is_some() {
            assert_sometimes!(
                self.committed_while_held,
                "journal: a journal keeps committing while a sibling on its nodes is held"
            );
        }
        if self.quarantined_ever {
            assert_sometimes!(
                self.served_while_quarantined,
                "journal: a node serves its other journals while one is quarantined"
            );
        }
    }
}

/// Lock the board.
pub(crate) fn lock(board: &Mutex<JournalBoard>) -> std::sync::MutexGuard<'_, JournalBoard> {
    board.lock().unwrap_or_else(PoisonError::into_inner)
}
