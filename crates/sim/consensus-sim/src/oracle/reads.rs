//! The read oracle: a leaderless read sees every write that completed
//! before it started.
//!
//! A write completed when a party applied its slot (the driver acks the
//! client from [`Ready::committed`](paros_consensus::Ready::committed)). A
//! read starts when the driver calls `quorum_read`, and it is answered
//! when its [`ReadState`](paros_consensus::quorum_read::ReadState) surfaces
//! with a read index. Linearizability needs the index at or past the
//! highest slot that some party applied before the read started (Charapko,
//! Ailijiang and Demirbas, *Linearizable Quorum Reads in Paxos*, the rule
//! paros's `quorum_read.rs` follows). The oracle reads only what the
//! driver saw: no call to the core.

use std::collections::BTreeMap;

use paros_consensus::Slot;

/// The oracle's message.
pub const READ_SEES_WRITES: &str = "a read sees every write that completed before it started";

/// The open reads of one run.
#[derive(Debug, Default)]
pub struct Reads {
    /// The highest slot any party applied so far.
    applied: Option<Slot>,
    /// Per read token, the highest applied slot when the read started.
    open: BTreeMap<u64, Option<Slot>>,
    /// Reads answered.
    answered: u64,
}

impl Reads {
    /// A party applied `slot`: the write in it completed.
    pub fn applied(&mut self, slot: Slot) {
        self.applied = self.applied.max(Some(slot));
    }

    /// A read with token `ctx` starts now.
    pub fn started(&mut self, ctx: u64) {
        self.open.insert(ctx, self.applied);
    }

    /// The read `ctx` is answered at `index`. A token answered twice, or by
    /// two parties, is judged against the start it recorded first. An
    /// answer to a read that never started is a failure: the oracle judges
    /// every answer and excuses none.
    ///
    /// # Panics
    ///
    /// If the read never started, or if the index is below a write that
    /// completed before the read started.
    pub fn answered(&mut self, who: &str, ctx: u64, index: Option<Slot>) {
        let floor = *self.open.get(&ctx).unwrap_or_else(|| {
            panic!("{READ_SEES_WRITES}: {who} answered read {ctx}, which never started")
        });
        assert!(
            index >= floor,
            "{READ_SEES_WRITES}: {who} answered read {ctx} at {index:?}, slot {floor:?} completed before it started"
        );
        self.answered += 1;
    }

    /// How many reads were answered.
    #[must_use]
    pub fn answered_count(&self) -> u64 {
        self.answered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_below_a_completed_write_is_red() {
        let mut reads = Reads::default();
        reads.applied(Slot(3));
        reads.started(7);
        reads.applied(Slot(5));
        reads.answered("node 0", 7, Some(Slot(3)));
        assert_eq!(reads.answered_count(), 1);
        reads.started(8);
        let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reads.answered("node 1", 8, Some(Slot(4)));
        }))
        .expect_err("the oracle fires");
        let message = error.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(message.contains(READ_SEES_WRITES), "{message}");
    }

    #[test]
    fn a_read_on_an_empty_log_may_answer_none() {
        let mut reads = Reads::default();
        reads.started(1);
        reads.answered("node 0", 1, None);
        assert_eq!(reads.answered_count(), 1);
    }

    #[test]
    #[should_panic(expected = "which never started")]
    fn an_answer_to_a_read_that_never_started_fails() {
        let mut reads = Reads::default();
        reads.answered("node 0", 99, None);
    }
}
