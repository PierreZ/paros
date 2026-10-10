//! The chain client's writes: the `WRITE`, `WRITE_TO_NON_LEADER` and
//! `DUP_WRITE` operations, and how a write is drawn, issued and recorded.

use std::future::Future;
use std::time::Duration;

use moonpool_sim::{TimeProvider, assert_always, assert_reachable, buggify_with_prob};
use paros::client::{Resolution, TruncateOutcome, WriteOptions, WriteOutcome, Writer};
use paros::{Command, Entry, LeaderUuid, Value, command_hash};

use crate::CHAOS_DURATION_MS;
use crate::audit::AuditWorld;
use crate::chain::{hash_text, user_command_hash};

use super::config::{ChainConfig, WRITE, WRITE_TO_NON_LEADER};
use super::rpc::{judged_write, within};
use super::{ChainWorkload, Step, absorb_truncate, fence, owner_never_of_wrong_mode};

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

impl ChainWorkload {
    /// One `WRITE` or `WRITE_TO_NON_LEADER` step (#204): the owner's next
    /// write through the library, its ambiguity settled, and an optional
    /// compaction after it. The flags are the run's library outcomes:
    /// written after an ambiguity, an ambiguity resolved, a redirect written.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn write_step<T, F>(
        &mut self,
        step: &Step<'_>,
        (writer, written, next_op): (&mut Writer, &mut Vec<WrittenCommand>, &mut u64),
        (successful_after_ambiguity, ambiguity_resolved, redirected_written): (
            &mut bool,
            &mut bool,
            &mut bool,
        ),
        now_ms: &(impl Fn() -> u64 + Sync),
        truncate_traced: &T,
    ) where
        T: Fn(usize, Option<LeaderUuid>, u64) -> F + Sync,
        F: Future<Output = Option<TruncateOutcome>> + Send,
    {
        let Step {
            audit,
            config,
            ctx,
            ignore_hint,
            log,
            nodes,
            op,
            raw_class,
            raw_pause,
            raw_payload,
            raw_policy,
            retarget,
            server_count,
            target,
            time,
            ..
        } = *step;
        // Race 1 (#205), mid-run: an owner pipelines a burst at
        // consecutive positions, and on a second coin claims the
        // journal again while it is in flight. Its entries are
        // spread off the step's draws, so the step still draws
        // six times.
        if op == WRITE && writer.owned().is_some() && buggify_with_prob!(0.10) {
            assert_reachable!("chain: an owner pipelines a burst of writes mid-run");
            let via = nodes.leader().unwrap_or(target);
            let mut ahead = *writer;
            let mut burst = Vec::with_capacity(config.pipeline_depth);
            for k in 0..config.pipeline_depth as u64 {
                let spread = crate::chain::splitmix(raw_payload ^ k);
                let submission = self.submit(
                    audit,
                    config,
                    ahead,
                    &mut *next_op,
                    raw_class.wrapping_add(k),
                    spread,
                    now_ms(),
                );
                ahead.advance_to(ahead.next_seq() + submission.entry.count());
                burst.push((submission, via));
            }
            let race = buggify_with_prob!(0.5).then(|| {
                assert_reachable!("chain: a claim races a mid-run burst");
                let delay = raw_pause % (config.burst_claim_delay_ms + 1);
                (Duration::from_millis(delay), via)
            });
            self.burst(
                ctx,
                nodes,
                config,
                burst,
                race,
                (&mut *writer, &mut *written),
            )
            .await;
            return;
        }
        let submission = self.submit(
            audit,
            config,
            *writer,
            &mut *next_op,
            raw_class,
            raw_payload,
            now_ms(),
        );
        let chosen_target = if op == WRITE_TO_NON_LEADER {
            nodes.leader().map_or(target, |leader| {
                if server_count > 1 {
                    (leader + 1 + target % (server_count - 1)) % server_count
                } else {
                    leader
                }
            })
        } else if ignore_hint {
            target
        } else {
            nodes.leader().unwrap_or(target)
        };
        // Honest ambiguity: abandon the client observation, never
        // falsify a server acknowledgement. The identical write
        // is retried below.
        // Race 2 (#205): this attempt's timeout is shorter than
        // its ack (`ack_race_timeout_ms`), so the owner gives up
        // on a write that may still land.
        let ack_race = op == WRITE && writer.owned().is_some() && buggify_with_prob!(0.25);
        #[allow(clippy::cast_precision_loss)]
        let abandon = !ack_race
            && time.now() < Duration::from_millis(CHAOS_DURATION_MS)
            && buggify_with_prob!(config.abandon_pct as f64 / 100.0);
        if abandon {
            // BUGGIFY pairing: the deliberate mid-flight
            // abandonment (the honest-ambiguity generator) fires.
            assert_reachable!("chain: a client abandons an in-flight observation");
        }
        let result = if ack_race {
            self.ack_race(ctx, nodes, config, &submission, chosen_target, &mut *writer)
                .await
        } else {
            // The library's write (#221): the identical write,
            // following redirects (a `WRITE_TO_NON_LEADER` stops
            // at the first) inside one request deadline.
            let request = writer.request(&submission.entry);
            log.open_write(submission.op);
            let report = nodes
                .write(
                    &request,
                    chosen_target,
                    WriteOptions {
                        retarget,
                        stop_at_redirect: op != WRITE,
                        abandon_first_after: abandon.then_some(Duration::from_millis(10)),
                    },
                )
                .await;
            let result = judged_write(
                report.outcome,
                false,
                nodes.id_of(report.server),
                writer.journal(),
            );
            if report.redirects > 0 && matches!(result, WriteOutcome::Written { .. }) {
                *redirected_written = true;
            }
            let result = if matches!(result, WriteOutcome::Ambiguous) {
                tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_proposal_ambiguous");
                // Settle it (#204: the journal answers the
                // identical write from the log): read the
                // position back, then re-send it byte for byte —
                // by policy, back to the node that may have
                // committed the abandoned attempt, or on to the
                // hinted leader / the next node.
                let retry_target = nodes.retarget(
                    retarget,
                    chosen_target,
                    nodes.leader().map(|leader| nodes.id_of(leader)),
                );
                // An impatient operator (#204's retry edge,
                // its own BUGGIFY location): the identical
                // write re-sent at once, before any read-back,
                // which a journal that committed the first must
                // answer from the log. `resolve` reads back
                // first, so without this the retry that meets a
                // committed write is all but never sent.
                let resent = if crate::shape::lost_verdict(ctx.state()) || buggify_with_prob!(0.5) {
                    assert_reachable!("client: an ambiguous write is re-sent before any read-back");
                    let again = nodes
                        .write_attempt(retry_target, request.clone(), None)
                        .await;
                    match judged_write(again, false, nodes.id_of(retry_target), writer.journal()) {
                        WriteOutcome::Written { seq, count, .. } => {
                            Some(Resolution::Written { seq, count })
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                let resolved = match resent {
                    Some(resolution) => paros::client::ResolveReport {
                        resolution,
                        by_read_back: false,
                    },
                    None => nodes.resolve(&request, retry_target, retarget).await,
                };
                if resolved.by_read_back {
                    assert_reachable!("client: a read-back alone proves an ambiguous write fenced");
                }
                match resolved.resolution {
                    Resolution::Written { seq, count } => {
                        *successful_after_ambiguity = true;
                        *ambiguity_resolved = true;
                        WriteOutcome::Written {
                            seq,
                            count,
                            duplicate: true,
                        }
                    }
                    Resolution::NotWritten { state } => {
                        *ambiguity_resolved = true;
                        WriteOutcome::Refused { state }
                    }
                    Resolution::Truncated { state } => WriteOutcome::Truncated { state },
                    Resolution::Unresolved => WriteOutcome::Ambiguous,
                }
            } else {
                result
            };
            log.close_write();
            result
        };
        match result {
            WriteOutcome::Written { seq, count, .. } => {
                writer.advance_to(seq + count);
                self.record_written(&submission, seq, count, now_ms());
                self.adversarial.payload_classes[submission.payload_class] = true;
                written.push(submission.written(
                    seq,
                    count,
                    nodes.leader().unwrap_or(chosen_target),
                ));
                if config.compaction && submission.op.is_multiple_of(config.compact_every) {
                    // How far to ask: everything written, a
                    // partial prefix below it, or past it (a
                    // truncation is clamped to `next_seq`).
                    let end = seq + count;
                    let up_to = match (raw_policy >> 5) % 4 {
                        0 => end.saturating_sub((raw_policy >> 7) % (end + 1)),
                        1 => end + 1 + (raw_policy >> 7) % 8,
                        _ => end,
                    };
                    let outcome = truncate_traced(
                        nodes.leader().unwrap_or(chosen_target),
                        fence(writer),
                        up_to,
                    )
                    .await;
                    absorb_truncate(&mut *writer, outcome.as_ref());
                }
            }
            WriteOutcome::Refused { state } | WriteOutcome::Truncated { state } => {
                self.history.record_write_failed(submission.op);
                if writer.owned().is_none()
                    && state
                        .leader
                        .is_some_and(|leader| leader != submission.entry.leader)
                {
                    self.adversarial.fenced = true;
                }
                writer.learn(&state);
                tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_command_rejected");
            }
            WriteOutcome::Redirect { leader } => {
                nodes.observe_leader(leader);
                self.history.record_write_failed(submission.op);
            }
            WriteOutcome::TooLarge { .. } => {
                assert_reachable!("chain: a batch over a node's limits is refused at the edge");
                self.history.record_write_failed(submission.op);
            }
            WriteOutcome::WrongMode { .. } => {
                owner_never_of_wrong_mode();
                self.history.record_write_failed(submission.op);
            }
            WriteOutcome::Denied(_)
            | WriteOutcome::UnknownJournal
            | WriteOutcome::Malformed
            | WriteOutcome::Ambiguous => {
                self.history.record_write_failed(submission.op);
            }
        }
    }

    /// One `DUP_WRITE` step: a write this client saw written, re-sent byte for
    /// byte to a node other than the leader, which must answer it from the log.
    pub(super) async fn dup_write_step<W, F>(
        &mut self,
        step: &Step<'_>,
        written: &[WrittenCommand],
        write_once: &W,
    ) where
        W: Fn(usize, &Entry, bool) -> F + Sync,
        F: Future<Output = WriteOutcome> + Send,
    {
        let Step {
            ctx,
            nodes,
            raw_payload,
            raw_policy,
            request_timeout,
            server_count,
            target,
            ..
        } = *step;
        if let Some(current_leader) = nodes.leader() {
            let candidates = written
                .iter()
                .filter(|command| command.node != current_leader)
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                return;
            }
            let index = usize::try_from(raw_payload % u64::try_from(candidates.len()).unwrap_or(1))
                .unwrap_or(0);
            let command = (*candidates[index]).clone();
            // Where the retry goes: the current leader, the node
            // that originally answered it (a possibly demoted
            // node), or anyone.
            let duplicate_target = match (raw_policy >> 3) % 4 {
                0 | 1 => current_leader,
                2 => command.node % server_count,
                _ => target,
            };
            tracing::info!(
                cmd = %hash_text(command.cmd_hash),
                seq = command.seq,
                original_node = command.node,
                target = duplicate_target,
                "chain_duplicate_reproposed"
            );
            if !self.adversarial.duplicate_reproposed {
                assert_reachable!("chain: duplicate reproposal executes");
                self.adversarial.duplicate_reproposed = true;
            }
            let result = within(
                ctx,
                request_timeout,
                WriteOutcome::Ambiguous,
                write_once(duplicate_target, &command.entry, false),
            )
            .await;
            match result {
                WriteOutcome::Written { seq, duplicate, .. } => {
                    // A write already in the journal is answered
                    // from the log, at the position it holds.
                    assert_always!(
                        duplicate && seq == command.seq,
                        "chain: duplicate committed ack preserves its slot",
                        {
                            "original_seq" => command.seq,
                            "observed_seq" => seq,
                            "target" => duplicate_target,
                        }
                    );
                    if !self.adversarial.duplicate_across_leader_change {
                        assert_reachable!(
                            "chain: duplicate suppression observed after leader change"
                        );
                        self.adversarial.duplicate_across_leader_change = true;
                    }
                }
                WriteOutcome::Refused { state } => {
                    assert_always!(
                        false,
                        "chain: a retried write is never refused",
                        { "seq" => command.seq, "next_seq" => state.next_seq.0 }
                    );
                }
                WriteOutcome::Redirect { leader } => nodes.observe_leader(leader),
                // A node with smaller limits than the first
                // attempt's refuses the identical retry.
                WriteOutcome::WrongMode { .. } => owner_never_of_wrong_mode(),
                WriteOutcome::TooLarge { .. }
                | WriteOutcome::Truncated { .. }
                | WriteOutcome::Denied(_)
                | WriteOutcome::UnknownJournal
                | WriteOutcome::Malformed
                | WriteOutcome::Ambiguous => {}
            }
        }
    }
}
