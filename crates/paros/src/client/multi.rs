//! The multi-writer calls (#241, `docs/architecture.md` §2.4): a journal
//! anyone with access appends to. It has no leader, so a request carries no
//! leader uuid and no position: the journal assigns the position at apply.
//!
//! There is no deduplication. A write whose answer never came may still
//! land, and a retry may land a second time: delivery is at-least-once, and
//! the writers own that. So the library never re-sends a multi-writer write
//! on its own after an ambiguous answer: [`Client::write`] follows redirects
//! and returns, and the caller decides whether to send the batch again.
//!
//! A call shaped for the other mode is refused as `WrongMode` and moves
//! nothing: a single-writer client that meets a multi-writer journal never
//! writes unfenced, and the reverse.

use paros_core::{Entry, JournalIdentifier, LeaderUuid, Seq, Value};

use crate::rpc::{Truncate, Write};

/// The [`Entry`] a multi-writer write decides: the unset leader uuid and no
/// position (the journal assigns one).
///
/// # Panics
///
/// If `records` is empty: a write carries at least one record.
#[must_use]
pub fn append_entry(records: Vec<Vec<u8>>) -> Entry {
    assert!(!records.is_empty(), "a multi-writer write carries records");
    let entry = Entry {
        leader: LeaderUuid::UNSET,
        seq: Seq(0),
        records: records.into_iter().map(Value).collect(),
    };
    assert!(
        !entry.leader.is_set(),
        "a multi-writer write names no leader"
    );
    entry
}

/// The `Write(batch)` request of a multi-writer journal: no leader uuid, no
/// position.
///
/// # Panics
///
/// If `records` is empty: a write carries at least one record.
#[must_use]
pub fn append_request(journal: JournalIdentifier, records: Vec<Vec<u8>>) -> Write {
    assert!(!records.is_empty(), "a multi-writer write carries records");
    let request = Write {
        journal: journal.journal.0,
        tenant: journal.tenant.0,
        leader: None,
        seq: 0,
        records,
    };
    assert!(
        request.leader.is_none(),
        "a multi-writer write names no leader"
    );
    request
}

/// The `Truncate(up_to_seq)` request of a multi-writer journal: anyone may
/// send it, so it names no leader uuid.
#[must_use]
pub fn open_truncate_request(journal: JournalIdentifier, up_to: u64) -> Truncate {
    let request = Truncate {
        journal: journal.journal.0,
        tenant: journal.tenant.0,
        up_to,
        leader: None,
    };
    assert!(
        request.leader.is_none(),
        "a multi-writer truncation names no leader"
    );
    request
}
