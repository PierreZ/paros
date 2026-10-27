//! The client's tunables ([`ClientTunables`]) and the checkpoint policy
//! they name ([`CheckpointPolicy`]).

use std::time::Duration;

/// The client's tunables, one plain field each so a harness can push any
/// one of them to an extreme on its own (prong 2 of the turbulence
/// doctrine). Each documents its floor: the smallest value that is still a
/// working client, slower but never stuck.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientTunables {
    /// How long one call (a write with the redirects it follows, a claim's
    /// `SetLeader`, a truncation or reconfiguration attempt) waits for its
    /// answer. Floor: one decided-and-applied slot's round trip — a deadline
    /// under it abandons every write and every retry of it.
    pub request_timeout: Duration,
    /// How long one `Read` attempt waits, on top of its `wait_ms`. Floor:
    /// one quorum read's confirmation window — under it every read is
    /// abandoned, and a claim starts with a read.
    pub read_timeout: Duration,
    /// Redirects a call follows before it gives up with the redirect it
    /// last got (a truncation's or reconfiguration's asks in all). Floor 1.
    pub redirect_limit: u32,
    /// Idle between a redirect and the next attempt. Floor 0: a tight loop,
    /// bounded by the call's deadline.
    pub redirect_backoff: Duration,
    /// Re-asks of a call that may succeed later at the same place: an
    /// unsettled leader, a busy matchmaker reconfigurer, the identical
    /// re-sends [`Client::resolve`] makes. Floor 1.
    pub retry_budget: u32,
    /// Idle between those re-asks. Floor 0.
    pub retry_backoff: Duration,
    /// The records a `Read` page asks for (`0`: the server's page size).
    /// Floor 1 when set: a reader that walks one record per call.
    pub page_size: u64,
    /// How long a `Read` at the tail lets the server wait for a record.
    /// Floor 0: answered at once, empty when nothing is past the cursor.
    pub wait_ms: u64,
    /// A [`checkpoint::Checkpointer`] checkpoints once the entries since its
    /// last checkpoint reach this many times the state's size: the extra
    /// writes are capped at `1/k`. Floor 1: a checkpoint per state's worth
    /// of entries.
    pub checkpoint_factor: u32,
    /// ... or once this long has passed since its last checkpoint, with any
    /// entry since. Floor 0: a checkpoint after every entry.
    pub checkpoint_interval: Duration,
    /// The most state bytes in one checkpoint `Chunk` record (#353).
    /// Floor 1: a chunk per byte. Keep it well under the nodes' batch bytes:
    /// a chunk no batch holds fails every checkpoint.
    pub checkpoint_chunk_bytes: usize,
    /// The most checkpoint run records in one `Write`. Floor 1: a batch per
    /// record. A node's `TooLarge` lowers it for the rest of the run.
    pub checkpoint_batch_records: usize,
    /// The most checkpoint run record bytes in one `Write`. Floor 1: a
    /// batch per record. A node's `TooLarge` lowers it for the rest of the
    /// run.
    pub checkpoint_batch_bytes: usize,
}

impl Default for ClientTunables {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            redirect_limit: 8,
            redirect_backoff: Duration::from_millis(20),
            retry_budget: 4,
            retry_backoff: Duration::from_millis(100),
            page_size: 0,
            wait_ms: 0,
            checkpoint_factor: 4,
            checkpoint_interval: Duration::from_mins(1),
            checkpoint_chunk_bytes: 8 << 10,
            checkpoint_batch_records: 64,
            checkpoint_batch_bytes: 256 << 10,
        }
    }
}

impl ClientTunables {
    /// The checkpoint policy these tunables name.
    #[must_use]
    pub fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy {
            factor: self.checkpoint_factor,
            interval: self.checkpoint_interval,
            chunk_bytes: self.checkpoint_chunk_bytes,
            batch_records: self.checkpoint_batch_records,
            batch_bytes: self.checkpoint_batch_bytes,
        }
    }
}

/// When an owner checkpoints, from [`ClientTunables`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointPolicy {
    /// Checkpoint once the entries since the last checkpoint reach this many
    /// times the state's size (so checkpoints cost at most `1/k` extra
    /// writes). Floor 1.
    pub factor: u32,
    /// Checkpoint once this long has passed since the last one, with any
    /// entry since. Floor 0: after every entry.
    pub interval: Duration,
    /// The most state bytes in one `Chunk` record. Floor 1: a chunk per
    /// byte, many records but a valid run. Keep it well under the server's
    /// batch bytes, or no batch holds a chunk.
    pub chunk_bytes: usize,
    /// The most run records in one `Write`. Floor 1: a batch per record.
    /// A node that answers `TooLarge` lowers it for the rest of the run.
    pub batch_records: usize,
    /// The most run record bytes in one `Write` (a batch always takes one
    /// record). Floor 1. A node that answers `TooLarge` lowers it for the
    /// rest of the run.
    pub batch_bytes: usize,
}

impl CheckpointPolicy {
    /// How many of `records` the next batch takes under `limits` (records,
    /// bytes): at least one, then as many as fit.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn batch_len(records: &[Vec<u8>], (max_records, max_bytes): (usize, usize)) -> usize {
        assert!(!records.is_empty(), "a batch takes a record");
        let mut bytes = 0;
        let mut taken = 0;
        for record in records.iter().take(max_records.max(1)) {
            if taken > 0 && bytes + record.len() > max_bytes {
                break;
            }
            bytes += record.len();
            taken += 1;
        }
        assert!(taken >= 1, "a batch always takes one record");
        taken
    }
}
