//! The client's fold of the journal (#186) and the trim fence that keeps it
//! foldable.
//!
//! paros runs no application: this client *is* the application. It reads the
//! journal from its cursor and folds every user entry into a [`ChainState`],
//! reporting each step to the audit (`AuditWorld::fold_applied`), which checks
//! that every client folds the same entry to the same state. A fold needs
//! every entry from the start, so a trim must never overtake a folding
//! client: every client registers its cursor in the run's shared fence, and
//! a trim is clamped below the lowest registered cursor. A client that is
//! done proposing leaves the fence; a trim may then overtake it, and it
//! stops folding.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{SimContext, StateHandle, assert_always};
use paros::{JournalId, Read, ReadAck, encode_records};

use super::rpc::within;
use crate::audit::AuditWorld;
use crate::chain::{ChainState, user_command_hash};
use crate::client::SimClient;

const FENCE_KEY: &str = "paros-chain-trim-fence";

/// The pages a fold-to-tail reads at most before it stops: a bound, not a
/// target — the next fold resumes from where this one stopped.
const FOLD_PAGES: usize = 64;

/// Every folding client's cursor, by client id: the next LSN it has not
/// folded.
#[derive(Default)]
struct Fence {
    cursors: BTreeMap<u64, u64>,
}

/// `journal`'s fence (#188: each journal's trims wait only for the clients
/// folding that journal).
fn fence(state: &StateHandle, journal: JournalId) -> Arc<Mutex<Fence>> {
    crate::state::published(
        state,
        &crate::state::journal_key(FENCE_KEY, journal),
        Fence::default,
    )
}

/// Register `client` in the fence at the log's start.
pub(super) fn register(state: &StateHandle, journal: JournalId, client: u64) {
    fence(state, journal)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .cursors
        .insert(client, 0);
}

/// Take `client` out of the fence: trims no longer wait for it.
pub(super) fn release(state: &StateHandle, journal: JournalId, client: u64) {
    fence(state, journal)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .cursors
        .remove(&client);
}

/// The highest `Truncate { up_to }` the fence allows at most `up_to`:
/// every slot at or below it folded by every registered client. `None`
/// while some registered client has folded nothing.
pub(super) fn clamp(state: &StateHandle, journal: JournalId, up_to: u64) -> Option<u64> {
    let guard = fence(state, journal);
    let guard = guard.lock().unwrap_or_else(PoisonError::into_inner);
    match guard.cursors.values().min() {
        None => Some(up_to),
        Some(&lowest) => lowest.checked_sub(1).map(|fenced| up_to.min(fenced)),
    }
}

/// One client's fold: the state after every user entry below `cursor`.
pub(super) struct Fold {
    /// The journal this client folds.
    journal: JournalId,
    state: ChainState,
    cursor: u64,
    /// Whether the client is still in the fence. Once it left, a trim may
    /// overtake the cursor and the fold stops (`detached`).
    fenced: bool,
    detached: bool,
}

impl Fold {
    pub(super) fn new(journal: JournalId) -> Self {
        Self {
            journal,
            state: ChainState::default(),
            cursor: 0,
            fenced: true,
            detached: false,
        }
    }

    /// The next LSN this client has not folded — its tailing reads start
    /// here.
    pub(super) fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The state after every entry folded so far.
    pub(super) fn state(&self) -> ChainState {
        self.state
    }

    /// Leave the fence (see [`release`]).
    pub(super) fn leave(&mut self, state: &StateHandle, client: u64) {
        self.fenced = false;
        release(state, self.journal, client);
    }

    /// Fold one page read from `from` (already judged). A page from anywhere
    /// but the cursor folds nothing; a trim answer at the cursor ends the
    /// fold — which a fenced client never meets.
    pub(super) fn absorb(
        &mut self,
        audit: &AuditWorld,
        state: &StateHandle,
        client: u64,
        from: u64,
        ack: &ReadAck,
    ) {
        if from != self.cursor || ack.unknown_journal {
            return;
        }
        let next = ack.trimmed_to.unwrap_or(ack.next_lsn);
        assert_always!(
            next >= self.cursor,
            "chain: a client's journal-read cursor never moves backwards",
            { "cursor" => self.cursor, "next" => next }
        );
        if let Some(trim) = ack.trimmed_to {
            assert_always!(
                !self.fenced,
                "chain: a client's fold is never trimmed out from under it",
                { "client" => client, "cursor" => self.cursor, "trim" => trim }
            );
            self.detached = true;
        } else if !self.detached {
            for entry in &ack.entries {
                let value = encode_records(&entry.records);
                self.state = self.state.fold(entry.lsn, &value);
                audit.fold_applied(
                    client,
                    entry.lsn,
                    user_command_hash(&value),
                    self.state.chain_hash,
                );
            }
        }
        self.cursor = self.cursor.max(next);
        if let Some(cursor) = fence(state, self.journal)
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cursors
            .get_mut(&client)
        {
            *cursor = (*cursor).max(self.cursor);
        }
    }

    /// Read from the cursor until the serving node has nothing more, folding
    /// every page. Returns whether the read reached the node's committed end
    /// (`false` on a timeout, a refusal or the page bound).
    pub(super) async fn read_to_tail(
        &mut self,
        ctx: &SimContext,
        audit: &AuditWorld,
        via: &SimClient,
        client: u64,
        max_bytes: u64,
        timeout: Duration,
    ) -> bool {
        for _ in 0..FOLD_PAGES {
            if self.detached {
                return false;
            }
            let from = self.cursor;
            let request = Read {
                journal: self.journal.0,
                from_lsn: from,
                max_bytes,
            };
            let call = async { via.read(&request).await.ok() };
            let Some(ack) = within(ctx, timeout, None, call).await else {
                return false;
            };
            if ack.unknown_journal {
                return false;
            }
            let stuck = ack.trimmed_to.is_none() && ack.entries.is_empty() && ack.next_lsn == from;
            super::judge_read(audit, client, from, &ack, &[]);
            self.absorb(audit, ctx.state(), client, from, &ack);
            if ack.trimmed_to.is_none() && ack.next_lsn >= ack.committed_end {
                return true;
            }
            // A page that moved nothing (a damaged slot the node cannot
            // serve yet) ends this fold; the next one retries.
            if stuck {
                return false;
            }
        }
        false
    }
}
