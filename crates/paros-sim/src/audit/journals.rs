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
//!   quarantined;
//! - **static stability** (#247) — a tenant journal keeps committing while
//!   its parent, the control plane (meta, the cell's control journal, the
//!   directory, all on the seed), is held down: the seed is killed for a
//!   stretch of the chaos window, and some other node applies a tenant
//!   slot meanwhile.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{StateHandle, assert_reachable, assert_sometimes};
use paros::JournalKey;

use crate::shape::JournalPlan;

const JOURNAL_BOARD_KEY: &str = "paros-journal-board";

/// The run's cross-journal facts, shared by every journal's audit port.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)] // sticky, independent gate facts
pub(crate) struct JournalBoard {
    /// The run serves more than one journal.
    multi: bool,
    /// The journal held on every node for the chaos window, if any.
    held: Option<JournalKey>,
    /// `(node, journal)` pairs quarantined right now.
    quarantined: BTreeSet<(u64, JournalKey)>,
    /// A journal committed while a sibling was held or not yet caught up.
    committed_while_held: bool,
    /// The held journal applied a slot after its hold ended: it caught up.
    held_caught_up: bool,
    /// A node applied a slot of one journal while another of its journals
    /// was quarantined.
    served_while_quarantined: bool,
    /// Some journal was ever quarantined on some node.
    quarantined_ever: bool,
    /// The run's tenant journals (the plan's).
    tenants: BTreeSet<JournalKey>,
    /// The node hosting the control journals, while it is held down
    /// (#247): cleared when it is released or boots again.
    parent_held: Option<u64>,
    /// The parent was ever held.
    parent_held_ever: bool,
    /// Another node applied a tenant journal's slot while the parent was
    /// held.
    committed_while_parent_held: bool,
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
        self.tenants = plan.ids.iter().copied().collect();
    }

    /// `node`, which hosts the control journals, is held down (#247).
    pub(crate) fn hold_parent(&mut self, node: u64) {
        self.parent_held = Some(node);
        self.parent_held_ever = true;
    }

    /// The parent hold is over.
    pub(crate) fn release_parent(&mut self) {
        self.parent_held = None;
    }

    /// `node` applied a slot of `journal`: a tenant commit while the
    /// parent is held, when another node applies a tenant journal's slot.
    pub(crate) fn applied_under_parent(&mut self, node: u64, journal: JournalKey) {
        if self.parent_held.is_some_and(|parent| parent != node) && self.tenants.contains(&journal)
        {
            if !self.committed_while_parent_held {
                assert_reachable!("static: a tenant journal commits while the seed is held down");
            }
            self.committed_while_parent_held = true;
        }
    }

    /// Whether the run serves more than one journal.
    pub(crate) fn is_multi(&self) -> bool {
        self.multi
    }

    /// `journal` was quarantined on `node`.
    pub(crate) fn quarantine(&mut self, node: u64, journal: JournalKey) {
        self.quarantined.insert((node, journal));
        self.quarantined_ever = true;
    }

    /// `journal` booted on `node` (at a process boot or a re-open).
    pub(crate) fn reopened(&mut self, node: u64, journal: JournalKey) {
        self.quarantined.remove(&(node, journal));
        // A held parent that boots again (attrition restarted it early) is
        // no longer held.
        if self.parent_held == Some(node) {
            self.parent_held = None;
        }
    }

    /// Whether `journal` is quarantined on `node` right now.
    pub(crate) fn is_quarantined(&self, node: u64, journal: JournalKey) -> bool {
        self.quarantined.contains(&(node, journal))
    }

    /// `node` applied a slot of `journal`; `in_chaos` says the chaos window
    /// (the hold) is still open. The held journal stays stalled past the
    /// window until it re-elects and commits again, so a sibling's commit
    /// counts until then: commits inside the 4 s window alone are rare
    /// (a run's first leaders are still being elected).
    pub(crate) fn applied(&mut self, journal: JournalKey, in_chaos: bool) {
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
    pub(crate) fn sent(&mut self, node: u64, journal: JournalKey) {
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
        if self.parent_held_ever {
            assert_sometimes!(
                self.committed_while_parent_held,
                "static: a tenant commits while its parent is held"
            );
        }
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
