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
use paros::client::{ClaimOutcome, WriteOutcome, Writer};

use super::rpc::{self, within};
use super::{ChainConfig, ChainWorkload, Submission, WrittenCommand, claim};
use crate::chain::hash_text;
use crate::client::ChainClient;

impl ChainWorkload {
    /// Send `burst` — writes at consecutive positions, each to its target —
    /// pipelined (#204: `Write` is pipelineable), and fold the verdicts into
    /// the writer. Overlapping Phase-2 rounds make the optional re-send and a
    /// later election gap observable without fabricating a message; a write
    /// that reaches the leader out of order is refused and names where the
    /// journal stood.
    ///
    /// With `race` (a delay and a node to ask), **race 1 of #205**: this
    /// owner claims the journal again, under its next uuid, while its burst
    /// is in flight. The writes whose slots the claim lands behind are
    /// fenced — refused, naming the uuid the claim installed — and the ones ahead of it
    /// are written; which is which is the slot order's alone, and the
    /// linearizability check judges both halves.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn burst(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        config: &ChainConfig,
        burst: Vec<(Submission, usize)>,
        race: Option<(Duration, usize)>,
        (writer, written): (&mut Writer, &mut Vec<WrittenCommand>),
    ) {
        let time = ctx.time().clone();
        let journal = self.journal;
        let timeout = Duration::from_millis(config.request_timeout_ms);
        // The uuid the racing claim asks under: the writer's next term.
        let mut racer = *writer;
        racer.begin_term();
        let next_term = racer.uuid();
        // A raced burst is spread over the claim's span (`burst_spacing_ms`),
        // so the claim lands inside it rather than behind every write.
        let spacing = if race.is_some() {
            Duration::from_millis(config.burst_spacing_ms)
        } else {
            Duration::ZERO
        };
        let sends = join_all(burst.iter().zip(0_u32..).map(|((submission, target), k)| {
            let time = time.clone();
            async move {
                if !spacing.is_zero() {
                    time.sleep(spacing * k).await.ok();
                }
                let attempt =
                    rpc::write_once(nodes, journal, *target, &submission.entry, false, false);
                within(ctx, timeout, WriteOutcome::Ambiguous, attempt).await
            }
        }));
        let claimed = async {
            let (delay, via) = race?;
            assert_reachable!("chain: a claim races an owner's pipelined burst");
            time.sleep(delay).await.ok()?;
            Some(claim(nodes, journal, via % nodes.server_count(), next_term).await)
        };
        let (results, claimed) = futures::join!(sends, claimed);
        let mut next = writer.next_seq();
        let (mut landed, mut fenced) = (false, false);
        for ((submission, target), result) in burst.into_iter().zip(results) {
            match result {
                WriteOutcome::Written { seq, count, .. } => {
                    nodes.observe_leader_at(target);
                    next = next.max(seq + count);
                    landed = true;
                    let now = u64::try_from(time.now().as_millis()).unwrap_or(u64::MAX);
                    self.record_written(&submission, seq, count, now);
                    self.adversarial.payload_classes[submission.payload_class] = true;
                    written.push(submission.written(seq, count, target));
                }
                WriteOutcome::Refused { state } | WriteOutcome::Truncated { state } => {
                    self.history.record_write_failed(submission.op);
                    fenced |= state
                        .leader
                        .is_some_and(|leader| leader != submission.entry.leader);
                    if state.leader == Some(submission.entry.leader)
                        || state.leader == Some(next_term)
                    {
                        next = next.max(state.next_seq.0);
                    } else {
                        writer.learn(&state);
                    }
                    tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_command_rejected");
                }
                WriteOutcome::Redirect { leader } => {
                    nodes.observe_leader(leader);
                    self.history.record_write_failed(submission.op);
                }
                WriteOutcome::UnknownJournal
                | WriteOutcome::Malformed
                | WriteOutcome::Ambiguous => {
                    self.history.record_write_failed(submission.op);
                    tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_proposal_ambiguous");
                }
            }
        }
        writer.advance_to(next);
        match claimed {
            Some(ClaimOutcome::Won { state }) => {
                writer.begin_term();
                writer.won(&state);
                // A fenced write names a state at or past the claim's.
                writer.advance_to(next);
                self.adversarial.burst_fenced |= landed && fenced;
            }
            Some(outcome @ ClaimOutcome::Owned { .. }) => {
                writer.begin_term();
                writer.claimed(&outcome);
            }
            Some(outcome) => {
                writer.claimed(&outcome);
            }
            None => {}
        }
    }

    /// **Race 2 of #205**: `submission`'s first attempt times out before its
    /// ack can come back (`ack_race_timeout_ms`), so the write may still
    /// land; the owner, not knowing, claims the journal again, and only then
    /// retries the same write — under the uuid it was built with. The
    /// retry crosses the ownership change its own claim made: answered from
    /// the log when the first attempt landed ahead of the claim, refused as
    /// superseded when it did not.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn ack_race(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        config: &ChainConfig,
        submission: &Submission,
        target: usize,
        writer: &mut Writer,
    ) -> WriteOutcome {
        let journal = self.journal;
        let timeout = Duration::from_millis(config.request_timeout_ms);
        let send = |target: usize| {
            rpc::write_once(nodes, journal, target, &submission.entry, false, false)
        };
        let short = Duration::from_millis(config.ack_race_timeout_ms);
        let first = within(ctx, short, WriteOutcome::Ambiguous, send(target)).await;
        if !matches!(first, WriteOutcome::Ambiguous) {
            return first;
        }
        assert_reachable!("chain: a write's timeout is shorter than its ack");
        writer.begin_term();
        let moved = match claim(nodes, journal, target, writer.uuid()).await {
            ClaimOutcome::Won { state } => {
                writer.won(&state);
                true
            }
            // Another owner's claim overtook this one: the ownership
            // changed all the same.
            // Already the leader: an earlier claim of its own won. The
            // leadership changed only if that claim installed another uuid
            // than the one this write was built under.
            ClaimOutcome::Owned { state } => {
                writer.learn(&state);
                state.leader != Some(submission.entry.leader)
            }
            ClaimOutcome::Lost { state } => {
                writer.learn(&state);
                true
            }
            ClaimOutcome::Redirect { .. }
            | ClaimOutcome::UnknownJournal
            | ClaimOutcome::Malformed
            | ClaimOutcome::Unread
            | ClaimOutcome::Ambiguous => false,
        };
        let retry = within(
            ctx,
            timeout,
            WriteOutcome::Ambiguous,
            send(nodes.leader().unwrap_or(target)),
        )
        .await;
        if moved {
            match &retry {
                WriteOutcome::Written { duplicate, .. } => {
                    assert_always!(
                        *duplicate,
                        "journal: a write retried across an ownership change is never accepted anew"
                    );
                    self.adversarial.retry_acked_across_claim = true;
                }
                WriteOutcome::Refused { state }
                    if state
                        .leader
                        .is_some_and(|leader| leader != submission.entry.leader) =>
                {
                    self.adversarial.retry_superseded = true;
                }
                _ => {}
            }
        }
        retry
    }
}
