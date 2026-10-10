//! The chain client's writes: the `WRITE`, `WRITE_TO_NON_LEADER` and
//! `DUP_WRITE` operations, and how a write is drawn, issued and recorded.

use paros::client::Writer;
use paros::{Command, Entry, Value, command_hash};

use super::ChainWorkload;
use super::config::ChainConfig;
use crate::audit::AuditWorld;
use crate::chain::{hash_text, user_command_hash};

/// One write this client issued: its entry, the payload class its bytes
/// were drawn from, and its client-side operation number.
pub(super) struct Submission {
    pub(super) op: u64,
    pub(super) entry: Entry,
    pub(super) payload_class: usize,
    pub(super) cmd_hash: u64,
}

impl Submission {
    /// The write, written at `[seq, seq + count)` through `node`.
    pub(super) fn written(&self, seq: u64, count: u64, node: usize) -> WrittenCommand {
        WrittenCommand {
            entry: self.entry.clone(),
            seq,
            count,
            cmd_hash: self.cmd_hash,
            node,
        }
    }
}

/// A write this client saw written.
#[derive(Clone)]
pub(super) struct WrittenCommand {
    pub(super) entry: Entry,
    pub(super) seq: u64,
    pub(super) count: u64,
    pub(super) cmd_hash: u64,
    pub(super) node: usize,
}

impl ChainWorkload {
    /// Issue the next write from the caller's `class` and `seed` draws (it
    /// draws nothing itself): allocate its operation number, build its
    /// batch at the writer's position under the writer's generation, and
    /// record the submission with the audit, the history and the trace.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn submit(
        &mut self,
        audit: &AuditWorld,
        config: &ChainConfig,
        writer: Writer,
        next_op: &mut u64,
        class: u64,
        seed: u64,
        now_ms: u64,
    ) -> Submission {
        let records = Self::draw_records(audit, config, class, seed);
        // An owner's write; a superseded writer's is the deliberate
        // misbehaviour (#204: under its old uuid, which the journal must
        // refuse unless a reinstatement made it lead again).
        let entry = writer.stale_entry(records);
        self.issue(audit, entry, next_op, class, now_ms)
    }

    /// The records of one write: `1..=batch_records` of them, their bytes
    /// drawn from `class` and `seed`, each registered with the audit.
    pub(super) fn draw_records(
        audit: &AuditWorld,
        config: &ChainConfig,
        class: u64,
        seed: u64,
    ) -> Vec<Value> {
        let count = 1 + (seed >> 48) % config.batch_records.max(1);
        let records: Vec<Value> = (0..count)
            .map(|k| {
                Value(Self::payload(
                    class,
                    config.command_bytes,
                    config.large_command_bytes,
                    seed.wrapping_add(k.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
                ))
            })
            .collect();
        for record in &records {
            audit.note_submitted(user_command_hash(&record.0));
        }
        records
    }

    /// Issue `entry` as this client's next write operation.
    pub(super) fn issue(
        &mut self,
        audit: &AuditWorld,
        entry: Entry,
        next_op: &mut u64,
        class: u64,
        now_ms: u64,
    ) -> Submission {
        let op = *next_op;
        *next_op = next_op.saturating_add(1);
        let payload_class = usize::try_from(class % 4).unwrap_or(0);
        let cmd_hash = command_hash(&Command::Write(entry.clone()));
        // The non-interference oracle's ground truth (#188): this write
        // belongs to this client's journal and to no other.
        audit.note_appended(cmd_hash);
        self.history.record_write_issued(op, now_ms);
        tracing::info!(
            cmd = %hash_text(cmd_hash),
            op,
            seq = entry.seq.0,
            leader = %entry.leader,
            records = entry.count(),
            "chain_command_submitted"
        );
        Submission {
            op,
            entry,
            payload_class,
            cmd_hash,
        }
    }

    /// Record a write seen written at `[seq, seq + count)` in the history and
    /// the trace.
    pub(super) fn record_written(
        &mut self,
        submission: &Submission,
        seq: u64,
        count: u64,
        now_ms: u64,
    ) {
        let last = (seq + count).checked_sub(1);
        self.history.record_write_ack(submission.op, last, now_ms);
        tracing::info!(
            cmd = %hash_text(submission.cmd_hash),
            op = submission.op,
            seq,
            count,
            "chain_command_acked"
        );
    }

    fn payload(class: u64, ordinary: usize, large: usize, mut seed: u64) -> Vec<u8> {
        let len = match class % 4 {
            0 => 0,
            1 => 1,
            2 => ordinary,
            _ => large,
        };
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            // Local xorshift expands one provider draw without making the
            // explorer's RNG-call count depend on payload size.
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            bytes.push(seed.to_le_bytes()[0]);
        }
        bytes
    }
}
