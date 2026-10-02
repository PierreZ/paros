//! The reader (#204): a cursor into one journal, paged forward with `Read`.
//!
//! A reader below the journal's floor is answered `truncated`, naming the
//! floor. The reader resumes there and **reports the gap** — the positions
//! it will never read — as [`ReaderOutcome::Gap`]: a reader that skipped it
//! silently would hand its caller a fold with a hole it cannot see.

use moonpool_core::Providers;
use paros_core::{JournalKey, JournalState};

use super::Client;
use super::outcome::ReadOutcome;
use crate::rpc::Read;

/// What one step of a [`Reader`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReaderOutcome {
    /// Records from `from` up (possibly none: a read at the tail), and the
    /// state they were served from. The cursor moved past them.
    Records {
        /// The position of the first record.
        from: u64,
        /// The records, in position order.
        records: Vec<Vec<u8>>,
        /// The journal state the page was served from.
        state: JournalState,
    },
    /// The cursor was below the journal's floor: the positions
    /// `[from, resumed_at)` are gone, and the cursor resumed at
    /// `resumed_at`.
    Gap {
        /// The cursor before the jump.
        from: u64,
        /// The floor the answer named, where the cursor is now.
        resumed_at: u64,
        /// The journal state that named it.
        state: JournalState,
    },
    /// No server served the page.
    Unavailable,
    /// The server asked does not serve the journal.
    UnknownJournal,
}

/// A cursor into one journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reader {
    journal: JournalKey,
    cursor: u64,
}

impl Reader {
    /// A reader of `journal` from position `cursor`.
    #[must_use]
    pub fn new(journal: JournalKey, cursor: u64) -> Self {
        Self { journal, cursor }
    }

    /// The journal it reads.
    #[must_use]
    pub fn journal(&self) -> JournalKey {
        self.journal
    }

    /// The next position it reads.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Whether the cursor is at (or past) the tail `state` names.
    #[must_use]
    pub fn at_tail(&self, state: &JournalState) -> bool {
        self.cursor >= state.next_seq.0
    }

    /// The `Read` of the next page: `limit` records at most (`0`: the
    /// server's page size), long-polling `wait_ms` at the tail.
    #[must_use]
    pub fn request(&self, limit: u64, wait_ms: u64) -> Read {
        Read {
            journal: self.journal.journal.0,
            tenant: self.journal.tenant.0,
            from_seq: self.cursor,
            limit,
            wait_ms,
        }
    }

    /// Fold the answer to a read made at the cursor: a page moves the
    /// cursor past its records, a truncation moves it to the floor and is a
    /// [`ReaderOutcome::Gap`]. The cursor never moves back.
    pub fn absorb(&mut self, outcome: ReadOutcome) -> ReaderOutcome {
        match outcome {
            ReadOutcome::Page {
                from,
                records,
                state,
            } => {
                self.cursor = self.cursor.max(from + records.len() as u64);
                ReaderOutcome::Records {
                    from,
                    records,
                    state,
                }
            }
            ReadOutcome::Truncated { state } => {
                let from = self.cursor;
                self.cursor = self.cursor.max(state.first_seq.0);
                ReaderOutcome::Gap {
                    from,
                    resumed_at: self.cursor,
                    state,
                }
            }
            ReadOutcome::UnknownJournal => ReaderOutcome::UnknownJournal,
            ReadOutcome::Unserved | ReadOutcome::Malformed | ReadOutcome::Ambiguous => {
                ReaderOutcome::Unavailable
            }
        }
    }

    /// Read the next page — the client's `page_size` records, long-polling
    /// its `wait_ms` at the tail — from server `first` on (see
    /// [`Client::read_any`]), and fold it.
    pub async fn next<P: Providers>(&mut self, client: &Client<P>, first: usize) -> ReaderOutcome {
        let tunables = client.tunables();
        let request = self.request(tunables.page_size, tunables.wait_ms);
        let report = client.read_any(&request, first).await;
        self.absorb(report.outcome)
    }
}
