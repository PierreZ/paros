//! The chain client's truncations: the `TRUNCATE` and `TRUNCATE_STORM`
//! operations, and the writer fence they are sent under (#228).

use std::future::Future;

use moonpool_sim::assert_reachable;
use paros::client::{TruncateOutcome, Writer};
use paros::{LeaderUuid, Truncate, leader_uuid_to_proto};

use crate::chain::trace_truncate;

use super::fold::{self, Fold};
use super::{ChainWorkload, Step, owner_never_of_wrong_mode};

/// The writer fence an owner truncates under (#228): the uuid it leads
/// with, or `None` when it leads no term (it sends nothing).
pub(super) fn fence(writer: &Writer) -> Option<LeaderUuid> {
    writer.owned()
}

/// Fold a truncation's verdict back into the writer: a refusal names the
/// writer in force, so a superseded owner stops.
pub(super) fn absorb_truncate(writer: &mut Writer, outcome: Option<&TruncateOutcome>) {
    if let Some(outcome) = outcome {
        writer.absorb_truncate(outcome);
    }
}

impl ChainWorkload {
    /// One `TRUNCATE` step (#228): an owner truncates under its writer fence,
    /// clamped below every folding client's cursor.
    pub(super) async fn truncate_step<T, F>(
        &mut self,
        step: &Step<'_>,
        (writer, fold): (&mut Writer, &mut Fold),
        truncate_traced: &T,
    ) where
        T: Fn(usize, Option<LeaderUuid>, u64) -> F + Sync,
        F: Future<Output = Option<TruncateOutcome>> + Send,
    {
        let Step {
            audit,
            client_id,
            config,
            ctx,
            nodes,
            raw_pause,
            raw_policy,
            readers,
            target,
            ..
        } = *step;
        if config.compaction && raw_pause % config.compact_every == 0 {
            // Fold first: the fence holds a truncation below this
            // client's own cursor too.
            fold.read_to_tail(ctx, audit, readers, target, client_id, config.read_limit)
                .await;
            // Everything this client has read is what it may
            // drop: its fold's cursor, or its own writes' end
            // when it wrote past what it read.
            let up_to = writer.next_seq().max(fold.cursor());
            // The owner truncates under its own fence (#228). A
            // superseded owner sends nothing — or, as the
            // deliberate misbehaviour, its old uuid, which the
            // journal must refuse.
            let stale = writer.owned().is_none()
                && writer.fence() != writer.uuid()
                && raw_policy % 100 < config.stale_truncate_pct;
            let fence = if stale {
                assert_reachable!("chain: a superseded owner sends a stale truncate");
                Some(writer.fence())
            } else {
                fence(writer)
            };
            let outcome = truncate_traced(nodes.leader().unwrap_or(target), fence, up_to).await;
            absorb_truncate(&mut *writer, outcome.as_ref());
        }
    }

    /// One `TRUNCATE_STORM` step: a burst of truncations at the leader, a
    /// follower and a stale leader.
    pub(super) async fn truncate_storm_step<O, F>(
        &mut self,
        step: &Step<'_>,
        (writer, fold): (&mut Writer, &mut Fold),
        truncate_once: &O,
    ) where
        O: Fn(usize, Truncate) -> F + Sync,
        F: Future<Output = TruncateOutcome> + Send,
    {
        let Step {
            config,
            ctx,
            journal,
            nodes,
            raw_pause,
            raw_payload,
            raw_target,
            server_count,
            target,
            ..
        } = *step;
        let base = writer.next_seq().max(fold.cursor());
        // A storm is the owner's (#228): a writer that owns no
        // generation sends none.
        if let (true, Some(leader)) = (config.compaction && base > 0, fence(writer)) {
            let first_mode = usize::try_from(raw_pause % 3).unwrap_or(0);
            for attempt in 0..config.compact_storm_attempts {
                let mode = (first_mode + attempt) % 3;
                let (mode_name, up_to, request_target) = match mode {
                    // Far past the journal's tail: a truncation
                    // is clamped to `next_seq` at apply (#204),
                    // and the fence below turns it into the
                    // furthest truncation every folding client
                    // allows.
                    0 => (
                        "overask",
                        base.saturating_add(10_000 + raw_payload % 10_000),
                        nodes.leader().unwrap_or(target),
                    ),
                    1 if server_count > 1 && nodes.leader().is_some() => {
                        let leader = nodes.leader().unwrap_or(target) % server_count;
                        let offset = 1 + usize::try_from(
                            (raw_target + u64::try_from(attempt).unwrap_or(0))
                                % u64::try_from(server_count - 1).unwrap_or(1),
                        )
                        .unwrap_or(0);
                        ("follower", base, (leader + offset) % server_count)
                    }
                    2 if nodes.hint().stale.is_some() && nodes.hint().stale != nodes.leader() => {
                        ("stale-leader", base, nodes.hint().stale.unwrap_or(target))
                    }
                    _ => continue,
                };
                let Some(up_to) = fold::clamp(ctx.state(), journal, up_to) else {
                    continue;
                };
                trace_truncate(leader, up_to);
                tracing::info!(
                    up_to,
                    target = request_target,
                    mode = mode_name,
                    attempt,
                    "chain_compact_storm_request"
                );
                if !self.adversarial.compact_storm_modes[mode] {
                    match mode {
                        0 => {
                            assert_reachable!("chain: compact-storm overask executes");
                        }
                        1 => {
                            assert_reachable!("chain: compact-storm follower request executes");
                        }
                        2 => {
                            assert_reachable!("chain: compact-storm stale-leader request executes");
                        }
                        _ => unreachable!("compact storm mode is modulo three"),
                    }
                    self.adversarial.compact_storm_modes[mode] = true;
                }
                let request = Truncate {
                    journal: journal.journal.0,
                    tenant: journal.tenant.0,
                    up_to,
                    leader: Some(leader_uuid_to_proto(leader)),
                };
                match truncate_once(request_target, request).await {
                    TruncateOutcome::Applied { state } => {
                        nodes.observe_leader_at(request_target);
                        tracing::info!(
                            up_to,
                            first_seq = state.first_seq.0,
                            "chain_compact_accepted"
                        );
                    }
                    TruncateOutcome::Redirect { leader } => {
                        nodes.observe_leader(leader);
                    }
                    // Superseded mid-storm: the rest of the
                    // storm is refused alike, and the writer
                    // learns it from its next write.
                    TruncateOutcome::WrongMode { .. } => owner_never_of_wrong_mode(),
                    TruncateOutcome::Refused { .. }
                    | TruncateOutcome::Denied(_)
                    | TruncateOutcome::UnknownJournal
                    | TruncateOutcome::Malformed
                    | TruncateOutcome::Ambiguous => {}
                }
            }
        }
    }
}
