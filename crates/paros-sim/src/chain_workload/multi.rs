//! The multi-writer journal's operations (#241, `docs/architecture.md` §2.4)
//! and the mode confusion on both kinds of journal.
//!
//! A multi-writer journal has no leader: every client of it appends, with
//! no claim, no fence and no position, and anyone truncates. So on such a
//! journal the write family of the op alphabet maps here: `WRITE` and
//! `WRITE_TO_NON_LEADER` append one batch (the library follows redirects),
//! `DUP_WRITE` sends the identical batch twice, `DUAL_SUBMIT` sends it to
//! two nodes at once, `TRUNCATE` and `TRUNCATE_STORM` truncate open, and
//! `SET_LEADER` claims a journal that must refuse it as of the wrong mode.
//! An ambiguous append is sent again on a coin: delivery is at-least-once,
//! and the journal model's gate asks for a batch that lands twice.
//!
//! The mode confusion, each on its own BUGGIFY location: a fenced write sent
//! to a multi-writer journal and an unfenced one sent to a single-writer
//! journal, both refused as of the wrong mode and never applied.
//!
//! Every attempt goes through the library client, so the linearizability
//! search judges it against the multi-writer model.

use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable, buggify_with_prob};
use paros::client::multi::{append_entry, append_request, open_truncate_request};
use paros::client::{Retarget, SetLeaderOutcome, TruncateOutcome, WriteOptions, WriteOutcome};
use paros::{Command, Entry, JournalIdentifier, LeaderUuid, Seq, Value, command_hash};

use super::rpc::{CallLog, judged_truncate, judged_write, set_leader_once, within, write_once};
use super::{
    ChainConfig, ChainWorkload, DUAL_SUBMIT, DUP_WRITE, SET_LEADER, Submission, TRUNCATE,
    TRUNCATE_STORM, WrittenCommand,
};
use crate::audit::AuditWorld;
use crate::client::ChainClient;

/// What one multi-writer step works with: the client, its journal, the
/// step's target and draws.
pub(super) struct Step<'a> {
    pub(super) ctx: &'a SimContext,
    pub(super) nodes: &'a ChainClient,
    pub(super) log: &'a CallLog,
    pub(super) audit: &'a AuditWorld,
    pub(super) config: &'a ChainConfig,
    pub(super) journal: JournalIdentifier,
    pub(super) target: usize,
    /// The genesis pool the client's own draws rotate over: the joiners
    /// after it are reached only through a leader a reply names.
    pub(super) server_count: usize,
    pub(super) retarget: Retarget,
    /// The step's payload class and bytes draws.
    pub(super) draws: (u64, u64),
    /// The highest position every folding client has folded past, for an
    /// open truncation (`fold::clamp` already applied); `None` sends none.
    pub(super) trim_to: Option<u64>,
}

impl ChainWorkload {
    /// One step of the write family on a multi-writer journal.
    #[tracing::instrument(level = "debug", skip_all, fields(op = op, journal = %step.journal))]
    pub(super) async fn multi_step(
        &mut self,
        step: &Step<'_>,
        op: u8,
        next_op: &mut u64,
        written: &mut Vec<WrittenCommand>,
    ) {
        match op {
            SET_LEADER => claim_refused(step).await,
            TRUNCATE | TRUNCATE_STORM => truncate_open(step).await,
            _ if buggify_with_prob!(0.05) => {
                assert_reachable!("chain: a fenced write meets a multi-writer journal");
                fenced_write_refused(step).await;
            }
            _ => {
                let submission = self.submit_append(step, next_op);
                match op {
                    DUP_WRITE => {
                        for _ in 0..2 {
                            self.append(step, &submission, written).await;
                        }
                    }
                    DUAL_SUBMIT if step.server_count > 1 => {
                        self.dual_append(step, &submission, written).await;
                    }
                    _ => {
                        let landed = self.append(step, &submission, written).await;
                        if !landed && buggify_with_prob!(0.5) {
                            assert_reachable!(
                                "chain: an unanswered multi-writer write is sent again"
                            );
                            self.append(step, &submission, written).await;
                        }
                    }
                }
            }
        }
    }

    /// The recovery tail's write (#241): one batch, sent again after every
    /// answer that is no verdict until it is written or `deadline` passes;
    /// whether it was written. A re-send may land the batch twice: the
    /// journal promises at-least-once, and the search allows it.
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %step.journal))]
    pub(super) async fn append_until_written(
        &mut self,
        step: &Step<'_>,
        next_op: &mut u64,
        written: &mut Vec<WrittenCommand>,
        deadline: Duration,
    ) -> bool {
        let submission = self.submit_append(step, next_op);
        let time = step.ctx.time();
        while time.now() < deadline && !step.ctx.shutdown().is_cancelled() {
            if self.append(step, &submission, written).await {
                return true;
            }
            time.sleep(Duration::from_millis(step.config.retry_backoff_ms))
                .await
                .ok();
        }
        false
    }

    /// Draw one batch for a multi-writer journal: the records of an
    /// ordinary write, under no leader and no position.
    fn submit_append(&mut self, step: &Step<'_>, next_op: &mut u64) -> Submission {
        let (class, seed) = step.draws;
        let records = Self::draw_records(step.audit, step.config, class, seed);
        let entry = append_entry(records.into_iter().map(|record| record.0).collect());
        let now = u64::try_from(step.ctx.time().now().as_millis()).unwrap_or(u64::MAX);
        self.issue(step.audit, entry, next_op, class, now)
    }

    /// Append `submission` through the library's write, redirects followed;
    /// whether it was written.
    async fn append(
        &mut self,
        step: &Step<'_>,
        submission: &Submission,
        written: &mut Vec<WrittenCommand>,
    ) -> bool {
        let records = submission
            .entry
            .records
            .iter()
            .map(|r| r.0.clone())
            .collect();
        let request = append_request(step.journal, records);
        let first = step.nodes.leader().unwrap_or(step.target);
        step.log.open_write(submission.op);
        let report = step
            .nodes
            .write(
                &request,
                first,
                WriteOptions {
                    retarget: step.retarget,
                    ..WriteOptions::default()
                },
            )
            .await;
        step.log.close_write();
        let node = step.nodes.id_of(report.server);
        let outcome = judged_write(report.outcome, false, node, step.journal);
        self.absorb_append(step, submission, &outcome, report.server, written)
    }

    /// The identical batch to two nodes at once: each verdict a position
    /// of its own.
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %step.journal))]
    async fn dual_append(
        &mut self,
        step: &Step<'_>,
        submission: &Submission,
        written: &mut Vec<WrittenCommand>,
    ) {
        // On the genesis pool only: a joiner serves no plan journal.
        let count = step.server_count;
        let other = (step.target + 1) % count;
        let send = |target: usize| {
            let attempt = write_once(
                step.nodes,
                step.journal,
                target,
                &submission.entry,
                false,
                false,
            );
            within(
                step.ctx,
                Duration::from_millis(step.config.request_timeout_ms),
                WriteOutcome::Ambiguous,
                attempt,
            )
        };
        let (a, b) = futures::join!(send(step.target), send(other));
        let mut seqs = Vec::new();
        for (outcome, target) in [(a, step.target), (b, other)] {
            if let WriteOutcome::Written { seq, .. } = &outcome {
                seqs.push(*seq);
            }
            self.absorb_append(step, submission, &outcome, target, written);
        }
        if let [first, second] = seqs[..] {
            assert_reachable!("chain: a multi-writer write sent to two nodes lands twice");
            assert_always!(
                first != second,
                "chain: two multi-writer writes never share a position",
                { "seq" => first }
            );
        }
    }

    /// Fold one multi-writer write verdict back: written records are this
    /// client's to find in every read that covers them; a journal that
    /// refuses a batch with records breaks the mode.
    fn absorb_append(
        &mut self,
        step: &Step<'_>,
        submission: &Submission,
        outcome: &WriteOutcome,
        target: usize,
        written: &mut Vec<WrittenCommand>,
    ) -> bool {
        match *outcome {
            WriteOutcome::Written {
                seq,
                count,
                duplicate,
            } => {
                assert_always!(
                    !duplicate,
                    "chain: a multi-writer write is never answered from the log"
                );
                step.nodes.observe_leader_at(target);
                let now = u64::try_from(step.ctx.time().now().as_millis()).unwrap_or(u64::MAX);
                self.record_written(submission, seq, count, now);
                written.push(submission.written(seq, count, target));
                true
            }
            WriteOutcome::Refused { .. }
            | WriteOutcome::Truncated { .. }
            | WriteOutcome::WrongMode { .. } => {
                assert_always!(
                    false,
                    "chain: a multi-writer journal accepts every unfenced write"
                );
                self.history.record_write_failed(submission.op);
                false
            }
            WriteOutcome::TooLarge { .. } => {
                assert_reachable!("chain: a batch over a node's limits is refused at the edge");
                self.history.record_write_failed(submission.op);
                false
            }
            WriteOutcome::Redirect { leader } => {
                step.nodes.observe_leader(leader);
                self.history.record_write_failed(submission.op);
                false
            }
            WriteOutcome::UnknownJournal | WriteOutcome::Malformed | WriteOutcome::Ambiguous => {
                self.history.record_write_failed(submission.op);
                false
            }
        }
    }
}

/// A claim of a journal that has no leader: refused as of the wrong mode,
/// or no verdict.
#[tracing::instrument(level = "debug", skip_all, fields(journal = %step.journal))]
async fn claim_refused(step: &Step<'_>) {
    let uuid = LeaderUuid(u128::from(step.draws.1) | 1);
    let ask = set_leader_once(step.nodes, step.journal, step.target, (uuid, None), false);
    let outcome = within(
        step.ctx,
        Duration::from_millis(step.config.request_timeout_ms),
        SetLeaderOutcome::Ambiguous,
        ask,
    )
    .await;
    if let SetLeaderOutcome::WrongMode { state } = &outcome {
        assert_reachable!("chain: a multi-writer journal refuses a claim");
        assert_always!(
            state.leader.is_none(),
            "chain: a multi-writer journal names no leader"
        );
    }
    assert_always!(
        !matches!(
            outcome,
            SetLeaderOutcome::Won { .. } | SetLeaderOutcome::Lost { .. }
        ),
        "chain: a multi-writer journal never judges a claim"
    );
}

/// A truncation anyone may send, below every folding client's cursor.
#[tracing::instrument(level = "debug", skip_all, fields(journal = %step.journal))]
async fn truncate_open(step: &Step<'_>) {
    let Some(up_to) = step.trim_to else {
        return;
    };
    let request = open_truncate_request(step.journal, up_to);
    let truncator = step.nodes.with_tunables(step.config.truncate_tunables());
    let first = step.nodes.leader().unwrap_or(step.target);
    let outcome = judged_truncate(truncator.truncate(&request, first).await);
    match outcome {
        TruncateOutcome::Applied { state } => {
            assert_reachable!("chain: anyone truncates a multi-writer journal");
            assert_always!(
                state.first_seq.0 >= up_to.min(state.next_seq.0),
                "chain: an open truncation raises the floor to where it asked",
                { "up_to" => up_to, "first_seq" => state.first_seq.0 }
            );
        }
        TruncateOutcome::Refused { .. } | TruncateOutcome::WrongMode { .. } => {
            assert_always!(
                false,
                "chain: a multi-writer journal never refuses a truncation"
            );
        }
        TruncateOutcome::Redirect { .. }
        | TruncateOutcome::UnknownJournal
        | TruncateOutcome::Malformed
        | TruncateOutcome::Ambiguous => {}
    }
}

/// A write under a leader uuid to a journal that has none: refused as of
/// the wrong mode, or no verdict. One attempt, no redirect followed.
#[tracing::instrument(level = "debug", skip_all, fields(journal = %step.journal))]
async fn fenced_write_refused(step: &Step<'_>) {
    let entry = Entry {
        leader: LeaderUuid(u128::from(step.draws.1) | 1),
        seq: Seq(0),
        records: vec![Value(b"fenced".to_vec())],
    };
    // Sent to this journal: a slot of it may hold the write (refused).
    step.audit
        .note_appended(command_hash(&Command::Write(entry.clone())));
    let attempt = write_once(step.nodes, step.journal, step.target, &entry, false, false);
    let outcome = within(
        step.ctx,
        Duration::from_millis(step.config.request_timeout_ms),
        WriteOutcome::Ambiguous,
        attempt,
    )
    .await;
    assert_always!(
        !matches!(
            outcome,
            WriteOutcome::Written { .. }
                | WriteOutcome::Refused { .. }
                | WriteOutcome::Truncated { .. }
        ),
        "chain: a fenced write to a multi-writer journal is refused as of the wrong mode"
    );
}

/// The mode confusion on a single-writer journal (#241): an unfenced write,
/// refused as of the wrong mode, or no verdict. One attempt, no redirect
/// followed.
#[tracing::instrument(level = "debug", skip_all, fields(journal = %journal))]
pub(super) async fn unfenced_write_refused(
    ctx: &SimContext,
    (nodes, audit): (&ChainClient, &AuditWorld),
    journal: JournalIdentifier,
    target: usize,
    timeout: Duration,
) {
    let entry = append_entry(vec![b"unfenced".to_vec()]);
    // Sent to this journal: a slot of it may hold the write (refused).
    audit.note_appended(command_hash(&Command::Write(entry.clone())));
    let attempt = write_once(nodes, journal, target, &entry, false, false);
    let outcome = within(ctx, timeout, WriteOutcome::Ambiguous, attempt).await;
    if matches!(outcome, WriteOutcome::WrongMode { .. }) {
        assert_reachable!("chain: a single-writer journal refuses an unfenced write");
    }
    assert_always!(
        !matches!(
            outcome,
            WriteOutcome::Written { .. }
                | WriteOutcome::Refused { .. }
                | WriteOutcome::Truncated { .. }
        ),
        "chain: an unfenced write to a single-writer journal is refused as of the wrong mode"
    );
}
