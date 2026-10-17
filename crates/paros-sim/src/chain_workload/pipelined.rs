//! An owner's writes through the library's pipeline (#597 (several writes in
//! flight per journal)): `paros::client::Pipeline`, the path `parosctl
//! bench` runs, several writes in flight at consecutive positions. On a coin
//! the owner sends them last position first, so most arrive before the write
//! in front of them, wait, and are sent again; on another, it claims the
//! journal again while they are in flight, so a new term fences the rest.
//! Every attempt goes through the library client, and the linearizability
//! search judges each one.

use std::time::Duration;

use futures::future::join_all;
use moonpool_sim::{SimContext, TimeProvider, assert_reachable, buggify_with_prob};
use paros::client::{Pipeline, Writer, WriterOutcome};

use super::{ChainWorkload, Step, WrittenCommand, owner_never_of_wrong_mode};
use crate::chain::hash_text;

impl ChainWorkload {
    /// Write `pipeline_depth` batches through a [`Pipeline`] over `writer`,
    /// fold every outcome into the history, and take the pipeline's writer
    /// back. The batches are drawn off the step's `raw_payload` and
    /// `raw_class`, so the step draws as often as any other.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn pipelined(
        &mut self,
        step: &Step<'_>,
        (writer, written, next_op): (&mut Writer, &mut Vec<WrittenCommand>, &mut u64),
        now_ms: &(impl Fn() -> u64 + Sync),
    ) {
        let Step {
            audit,
            config,
            ctx,
            nodes,
            raw_class,
            raw_pause,
            raw_payload,
            target,
            ..
        } = *step;
        assert_reachable!("chain: an owner pipelines writes through the library");
        let via = nodes.leader().unwrap_or(target);
        let pipeline = Pipeline::new(*writer);
        let mut opened = Vec::with_capacity(config.pipeline_depth);
        for k in 0..config.pipeline_depth as u64 {
            let class = raw_class.wrapping_add(k);
            let spread = crate::chain::splitmix(raw_payload ^ k);
            let records = Self::draw_records(audit, config, class, spread);
            let Some(ticket) = pipeline.open(|_| records).await else {
                break;
            };
            let submission = self.issue(audit, ticket.entry().clone(), next_op, class, now_ms());
            opened.push((submission, ticket));
        }
        // The caller chooses when each write leaves: last position first
        // makes the writes arrive out of order. A plain choice off the
        // step's draws, not a fault: every seed that pipelines makes it.
        let reverse = raw_class.rotate_left(17) & 1 == 1;
        if reverse {
            assert_reachable!("chain: an owner sends a pipeline last position first");
        }
        let spacing = Duration::from_millis(config.burst_spacing_ms.max(1));
        let race = buggify_with_prob!(0.4).then(|| {
            assert_reachable!("chain: a claim races a library pipeline");
            Duration::from_millis(raw_pause % (config.burst_claim_delay_ms + 1))
        });
        let count = u32::try_from(opened.len()).unwrap_or(u32::MAX);
        let time = ctx.time().clone();
        let pipeline = &pipeline;
        let sends = join_all(opened.iter().zip(0_u32..).map(|((_, ticket), k)| {
            let time = time.clone();
            let ticket = ticket.clone();
            let order = if reverse { count - 1 - k } else { k };
            async move {
                time.sleep(spacing * order).await.ok();
                pipeline.send(nodes, ticket, via).await
            }
        }));
        let claimed = async {
            let delay = race?;
            time.sleep(delay).await.ok()?;
            Some(pipeline.claim(nodes, via, true).await)
        };
        let (results, _claimed) = futures::join!(sends, claimed);
        self.fold_pipelined(ctx, opened, results, (via, written));
        *writer = pipeline.writer();
    }

    /// Fold a pipeline's outcomes into the history, the trace and the run's
    /// flags, in position order.
    fn fold_pipelined(
        &mut self,
        ctx: &SimContext,
        opened: Vec<(super::Submission, paros::client::Ticket)>,
        results: Vec<paros::client::Pipelined>,
        (via, written): (usize, &mut Vec<WrittenCommand>),
    ) {
        for ((submission, _), sent) in opened.into_iter().zip(results) {
            match sent.outcome {
                WriterOutcome::Written { seq, count, .. } => {
                    let now = u64::try_from(ctx.time().now().as_millis()).unwrap_or(u64::MAX);
                    self.record_written(&submission, seq, count, now);
                    self.adversarial.payload_classes[submission.payload_class] = true;
                    self.adversarial.pipeline_resent_written |= sent.resent;
                    written.push(submission.written(seq, count, via));
                }
                WriterOutcome::WrongMode { .. } => {
                    owner_never_of_wrong_mode();
                    self.history.record_write_failed(submission.op);
                }
                WriterOutcome::Ambiguous | WriterOutcome::Unavailable { .. } => {
                    self.history.record_write_failed(submission.op);
                    tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_proposal_ambiguous");
                }
                _ => {
                    self.history.record_write_failed(submission.op);
                    tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_command_rejected");
                }
            }
        }
    }
}
