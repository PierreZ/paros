//! Batches at a node's edge limits (#663 (batches at the edge limits)).
//!
//! A `TooLarge` names the answering node's limits (`max_records`,
//! `max_bytes`). On a coin, the owner then sends that node a batch exactly
//! at one limit, which the edge must let through, and then one record or
//! one byte past it, which the edge must refuse. Without this no write ever
//! sat on the `<=` of `paros::roles::node::calls::batch_too_large`: the
//! workload's batches are at most four records of 16 KiB, far under the
//! default limits.
//!
//! An owner's own write rarely meets a `TooLarge`: both a multi-record
//! batch and a node's small limits must be drawn. So on its own BUGGIFY
//! location an owner first sends a batch of empty records one past the
//! largest record limit any node draws: every node that serves the journal
//! refuses it and names its limits (#663).
//!
//! Both writes are ordinary owner writes through the history: issued,
//! noted with the audit, and judged by the linearizability search.

use moonpool_sim::{assert_always, assert_reachable, buggify_with_prob};
use paros::client::{WriteOutcome, Writer};
use paros::{DriverTunables, Value};

use crate::audit::AuditWorld;
use crate::chain::user_command_hash;

use super::rpc::{within, write_once};
use super::write::WrittenCommand;
use super::{ChainWorkload, Step};

/// The limit a probe sits on.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Limit {
    /// Exactly `max_records` records, few bytes.
    Records,
    /// Exactly `max_bytes` bytes, an empty record next to large ones.
    Bytes,
}

/// The records of a batch at `limit` (`over`: one record or one byte past
/// it), their bytes drawn from `seed`.
fn records(
    limit: Limit,
    (max_records, max_bytes): (u64, u64),
    over: bool,
    seed: u64,
) -> Vec<Value> {
    let fill = |len: u64, k: u64| {
        let len = usize::try_from(len).unwrap_or(0);
        Value(ChainWorkload::payload(
            3,
            0,
            len,
            seed.wrapping_add(k.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
        ))
    };
    let records = match limit {
        Limit::Records => {
            // One record of a few bytes, the rest empty: the byte limit's
            // floor (16 KiB) always admits it.
            let count = max_records + u64::from(over);
            (0..count)
                .map(|k| fill(u64::from(k == 0), k))
                .collect::<Vec<_>>()
        }
        Limit::Bytes => {
            // Up to four records, the first empty when there are several,
            // the others sharing exactly `max_bytes`.
            let count = max_records.min(1 + (seed >> 40) % 4).max(1);
            let sized = count.saturating_sub(1).max(1);
            let empty = count - sized;
            let share = max_bytes / sized;
            (0..count)
                .map(|k| {
                    if k < empty {
                        fill(0, k)
                    } else if k + 1 == count {
                        fill(max_bytes - share * (sized - 1) + u64::from(over), k)
                    } else {
                        fill(share, k)
                    }
                })
                .collect()
        }
    };
    let bytes = records
        .iter()
        .map(|record| record.0.len() as u64)
        .sum::<u64>();
    let count = records.len() as u64;
    match limit {
        Limit::Records => {
            assert_always!(
                count == max_records + u64::from(over) && bytes <= max_bytes,
                "chain: a record-limit probe is sized at the limit"
            );
        }
        Limit::Bytes => {
            assert_always!(
                count <= max_records && bytes == max_bytes + u64::from(over),
                "chain: a byte-limit probe is sized at the limit"
            );
        }
    }
    records
}

impl ChainWorkload {
    /// After `server` refused a batch as over `limits`: on a coin, probe
    /// them ([`Self::probe_edges`]).
    pub(super) async fn edge_limits(
        &mut self,
        step: &Step<'_>,
        (writer, written, next_op): (&mut Writer, &mut Vec<WrittenCommand>, &mut u64),
        server: usize,
        limits: (u64, u64),
        now_ms: &(impl Fn() -> u64 + Sync),
    ) {
        if writer.owned().is_none() || !buggify_with_prob!(0.5) {
            return;
        }
        self.probe_edges(step, (writer, written, next_op), server, limits, now_ms)
            .await;
    }

    /// An owner's batch of empty records one past every node's record
    /// limit, sent to `server`: refused `TooLarge` by a node that serves the
    /// journal, never written; its limits are then probed.
    #[tracing::instrument(level = "debug", skip_all, fields(server = server))]
    pub(super) async fn oversized_write(
        &mut self,
        step: &Step<'_>,
        (writer, written, next_op): (&mut Writer, &mut Vec<WrittenCommand>, &mut u64),
        server: usize,
        now_ms: &(impl Fn() -> u64 + Sync),
    ) {
        assert_always!(
            writer.owned().is_some(),
            "chain: only an owner sends an oversized batch"
        );
        // The largest record limit a node runs: the default, and the
        // production profile `parosd` ships (`shape.rs` draws below both).
        let most = DriverTunables::default()
            .max_batch_records
            .max(DriverTunables::production().max_batch_records);
        let count = usize::try_from(most + 1).unwrap_or(usize::MAX);
        let over = self
            .edge_write(
                step,
                (&mut *writer, next_op),
                server,
                vec![Value(Vec::new()); count],
                now_ms,
            )
            .await;
        assert_always!(
            !matches!(over.1, WriteOutcome::Written { .. }),
            "chain: an oversized batch is never written",
            { "records" => most + 1 }
        );
        self.edge_failed(writer, &over.0, &over.1, step);
        if let WriteOutcome::TooLarge {
            max_records,
            max_bytes,
        } = over.1
        {
            assert_reachable!("chain: an oversized batch names a node's limits");
            self.probe_edges(
                step,
                (writer, written, next_op),
                server,
                (max_records, max_bytes),
                now_ms,
            )
            .await;
        }
    }

    /// A batch exactly at one of `server`'s `limits` (never `TooLarge`
    /// from that node), then one past it (never written).
    #[tracing::instrument(level = "debug", skip_all, fields(server = server))]
    async fn probe_edges(
        &mut self,
        step: &Step<'_>,
        (writer, written, next_op): (&mut Writer, &mut Vec<WrittenCommand>, &mut u64),
        server: usize,
        limits: (u64, u64),
        now_ms: &(impl Fn() -> u64 + Sync),
    ) {
        let Step {
            ctx,
            raw_class,
            raw_payload,
            ..
        } = *step;
        // A batch at the byte limit only where the run's stores hold it: a
        // node whose limit was not capped (none in this harness) would stall.
        let fits = limits.1 <= crate::shape::entry_capacity(ctx.state());
        // A batch at the record limit holds one byte: its encoding, a few
        // bytes a record, fits the entry log's floor (`ENTRY_BLOCKS_FLOOR`)
        // even at the default 1024 records.
        let limit = if fits && raw_class & 1 == 1 {
            Limit::Bytes
        } else {
            Limit::Records
        };
        assert_reachable!("chain: an owner probes a node's edge limits");
        let at = self
            .edge_write(
                step,
                (&mut *writer, next_op),
                server,
                records(limit, limits, false, raw_payload),
                now_ms,
            )
            .await;
        assert_always!(
            !matches!(at.1, WriteOutcome::TooLarge { .. }),
            "chain: a batch at a node's limits passes its edge",
            { "max_records" => limits.0, "max_bytes" => limits.1 }
        );
        // An answer past the edge (a verdict, a redirect): the limit
        // admitted the batch. The node need not lead the journal.
        if !matches!(
            at.1,
            WriteOutcome::TooLarge { .. } | WriteOutcome::Ambiguous
        ) {
            if limit == Limit::Bytes {
                assert_reachable!("chain: a batch exactly at a node's byte limit passes its edge");
            } else {
                assert_reachable!(
                    "chain: a batch exactly at a node's record limit passes its edge"
                );
            }
        }
        match at.1 {
            WriteOutcome::Written { seq, count, .. } => {
                writer.advance_to(seq + count);
                self.record_written(&at.0, seq, count, now_ms());
                written.push(at.0.written(seq, count, server));
            }
            outcome => self.edge_failed(writer, &at.0, &outcome, step),
        }
        let past = self
            .edge_write(
                step,
                (&mut *writer, next_op),
                server,
                records(limit, limits, true, raw_payload.rotate_left(17)),
                now_ms,
            )
            .await;
        assert_always!(
            !matches!(past.1, WriteOutcome::Written { .. }),
            "chain: a batch past a node's limits is never written",
            { "max_records" => limits.0, "max_bytes" => limits.1 }
        );
        if matches!(past.1, WriteOutcome::TooLarge { .. }) {
            assert_reachable!("chain: a batch one past a node's limits is refused at its edge");
        }
        self.edge_failed(writer, &past.0, &past.1, step);
    }

    /// Issue `records` as the owner's next write and send it to `server`
    /// once, no redirect followed.
    async fn edge_write(
        &mut self,
        step: &Step<'_>,
        (writer, next_op): (&mut Writer, &mut u64),
        server: usize,
        records: Vec<Value>,
        now_ms: &(impl Fn() -> u64 + Sync),
    ) -> (super::write::Submission, WriteOutcome) {
        register(step.audit, &records);
        let entry = writer.stale_entry(records);
        let submission = self.issue(step.audit, entry, next_op, step.raw_class, now_ms());
        writer.opened(&submission.entry);
        let outcome = within(
            step.ctx,
            step.request_timeout,
            WriteOutcome::Ambiguous,
            write_once(
                step.nodes,
                writer.journal(),
                server,
                &submission.entry,
                false,
                false,
            ),
        )
        .await;
        (submission, outcome)
    }

    /// A probe write that was not written: failed in the history, and any
    /// state it names learned.
    fn edge_failed(
        &mut self,
        writer: &mut Writer,
        submission: &super::write::Submission,
        outcome: &WriteOutcome,
        step: &Step<'_>,
    ) {
        if matches!(outcome, WriteOutcome::Written { .. }) {
            return;
        }
        self.history.record_write_failed(submission.op);
        match outcome {
            WriteOutcome::Refused { state } | WriteOutcome::Truncated { state } => {
                writer.learn(state);
            }
            WriteOutcome::Redirect { leader } => step.nodes.observe_leader(*leader),
            WriteOutcome::WrongMode { .. } => super::owner_never_of_wrong_mode(),
            _ => {}
        }
    }
}

/// Register each record with the audit before it leaves.
fn register(audit: &AuditWorld, records: &[Value]) {
    for record in records {
        audit.note_submitted(user_command_hash(&record.0));
    }
}
