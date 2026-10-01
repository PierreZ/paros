//! The three races of `docs/architecture.md` §6 (#205), as the owner runs
//! them: a claim racing its own pipelined burst ([`ChainWorkload::burst`],
//! race 1) and a write whose timeout is shorter than its ack, retried
//! across the owner's re-claim ([`ChainWorkload::ack_race`], race 2). Race
//! 3 — a truncation racing a reader's cursor — is the `READ` step's own
//! shape. Every outcome is judged by the linearizability search; the
//! flags here only feed the gates.

use std::time::Duration;

use futures::future::join_all;
use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable};
use paros::ClientId;

use super::rpc::{self, CallLog, SetLeaderResult, WriteResult, within};
use super::{
    ChainConfig, ChainWorkload, LeaderHint, Routes, Submission, Writer, WrittenCommand, claim,
};
use crate::chain::hash_text;
use crate::client::SimClient;

impl ChainWorkload {
    /// Send `burst` — writes at consecutive positions, each to its target —
    /// pipelined (#204: `Write` is pipelineable), and fold the verdicts into
    /// the writer. Overlapping Phase-2 rounds make the optional re-send and a
    /// later election gap observable without fabricating a message; a write
    /// that reaches the leader out of order is refused and names where the
    /// journal stood.
    ///
    /// With `race` (a delay and a node to ask), **race 1 of #205**: this
    /// owner claims the journal again while its burst is in flight. The
    /// writes whose slots the claim lands behind are fenced — refused,
    /// naming the generation the claim minted — and the ones ahead of it
    /// are written; which is which is the slot order's alone, and the
    /// linearizability check judges both halves.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn burst(
        &mut self,
        ctx: &SimContext,
        clients: &[SimClient],
        log: &CallLog,
        config: &ChainConfig,
        burst: Vec<(Submission, usize)>,
        race: Option<(Duration, usize)>,
        routes: Routes,
        (writer, hint, written): (&mut Writer, &mut LeaderHint, &mut Vec<WrittenCommand>),
    ) {
        let time = ctx.time().clone();
        let journal = self.journal;
        let me = self.client_id;
        let timeout = Duration::from_millis(config.request_timeout_ms);
        let read_timeout = Duration::from_millis(config.read_timeout_ms);
        let sends = join_all(burst.iter().map(|(submission, target)| {
            let attempt = rpc::write_once(
                clients,
                log,
                &time,
                journal,
                *target,
                &submission.entry,
                false,
                false,
            );
            within(ctx, timeout, WriteResult::Ambiguous, attempt)
        }));
        let claimed = async {
            let (delay, via) = race?;
            assert_reachable!("chain: a claim races an owner's pipelined burst");
            time.sleep(delay).await.ok()?;
            claim(
                ctx,
                clients,
                log,
                journal,
                via % clients.len(),
                me,
                (read_timeout, timeout),
            )
            .await
        };
        let (results, claimed) = futures::join!(sends, claimed);
        let mut next = writer.next_seq;
        let (mut landed, mut fenced) = (false, false);
        for ((submission, target), result) in burst.into_iter().zip(results) {
            match result {
                WriteResult::Written { seq, count, .. } => {
                    hint.observe(u64::try_from(target).ok(), routes);
                    next = next.max(seq + count);
                    landed = true;
                    let now = u64::try_from(time.now().as_millis()).unwrap_or(u64::MAX);
                    self.record_written(&submission, seq, count, now);
                    self.adversarial.payload_classes[submission.payload_class] = true;
                    written.push(submission.written(seq, count, target));
                }
                WriteResult::Refused { state } | WriteResult::Truncated { state } => {
                    self.history.record_write_failed(submission.op);
                    fenced |= state.generation.0 > submission.entry.generation.0;
                    if state.owner == Some(ClientId(me)) {
                        next = next.max(state.next_seq.0);
                    } else {
                        writer.learn(me, &state);
                    }
                    tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_command_rejected");
                }
                WriteResult::Redirect { leader } => {
                    hint.observe(leader, routes);
                    self.history.record_write_failed(submission.op);
                }
                WriteResult::Ambiguous => {
                    self.history.record_write_failed(submission.op);
                    tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_proposal_ambiguous");
                }
            }
        }
        writer.next_seq = next;
        match claimed {
            Some(SetLeaderResult::Won { state }) => {
                writer.won(&state);
                // A fenced write names a state at or past the claim's.
                writer.next_seq = writer.next_seq.max(next);
                self.adversarial.burst_fenced |= landed && fenced;
            }
            Some(SetLeaderResult::Lost { state }) => writer.learn(me, &state),
            Some(SetLeaderResult::Redirect { leader }) => hint.observe(leader, routes),
            Some(SetLeaderResult::Ambiguous) | None => {}
        }
    }

    /// **Race 2 of #205**: `submission`'s first attempt times out before its
    /// ack can come back (`ack_race_timeout_ms`), so the write may still
    /// land; the owner, not knowing, claims the journal again, and only then
    /// retries the same write — under the generation it was built with. The
    /// retry crosses the ownership change its own claim made: answered from
    /// the log when the first attempt landed ahead of the claim, refused as
    /// superseded when it did not.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn ack_race(
        &mut self,
        ctx: &SimContext,
        clients: &[SimClient],
        log: &CallLog,
        config: &ChainConfig,
        submission: &Submission,
        target: usize,
        routes: Routes,
        (writer, hint): (&mut Writer, &mut LeaderHint),
    ) -> WriteResult {
        let time = ctx.time().clone();
        let journal = self.journal;
        let me = self.client_id;
        let timeout = Duration::from_millis(config.request_timeout_ms);
        let read_timeout = Duration::from_millis(config.read_timeout_ms);
        let send = |target: usize| {
            rpc::write_once(
                clients,
                log,
                &time,
                journal,
                target,
                &submission.entry,
                false,
                false,
            )
        };
        let short = Duration::from_millis(config.ack_race_timeout_ms);
        let first = within(ctx, short, WriteResult::Ambiguous, send(target)).await;
        if !matches!(first, WriteResult::Ambiguous) {
            return first;
        }
        assert_reachable!("chain: a write's timeout is shorter than its ack");
        let moved = match claim(
            ctx,
            clients,
            log,
            journal,
            target,
            me,
            (read_timeout, timeout),
        )
        .await
        {
            Some(SetLeaderResult::Won { state }) => {
                writer.won(&state);
                true
            }
            // Another owner's claim overtook this one: the ownership
            // changed all the same.
            Some(SetLeaderResult::Lost { state }) => {
                writer.learn(me, &state);
                true
            }
            Some(SetLeaderResult::Redirect { leader }) => {
                hint.observe(leader, routes);
                false
            }
            Some(SetLeaderResult::Ambiguous) | None => false,
        };
        let retry = within(
            ctx,
            timeout,
            WriteResult::Ambiguous,
            send(hint.current.unwrap_or(target)),
        )
        .await;
        if moved {
            match &retry {
                WriteResult::Written { duplicate, .. } => {
                    assert_always!(
                        *duplicate,
                        "journal: a write retried across an ownership change is never accepted anew"
                    );
                    self.adversarial.retry_acked_across_claim = true;
                }
                WriteResult::Refused { state }
                    if state.generation.0 > submission.entry.generation.0 =>
                {
                    self.adversarial.retry_superseded = true;
                }
                _ => {}
            }
        }
        retry
    }
}
