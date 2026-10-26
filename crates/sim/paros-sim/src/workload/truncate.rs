//! The chain client's truncations: the `TRUNCATE` and `TRUNCATE_STORM`
//! operations, and the writer fence they are sent under (#228).

use std::future::Future;

use moonpool_sim::{assert_always, assert_reachable};
use paros::client::{TruncateOutcome, Writer};
use paros::core::LeaderUuid;
use paros::wire::Truncate;
use paros::wire::leader_uuid_to_proto;

use crate::workload::chain::trace_truncate;

use super::config::{MID_BATCH_SHAPE, TRUNCATE_SHAPES, weighted_index};
use super::fold::{self, Fold};
use super::write::WrittenCommand;
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

/// The floor a `TRUNCATE` step asks for (#659 (partial and mid-batch
/// truncation)), by its shape (an index into [`TRUNCATE_SHAPES`]): `floor`
/// is the highest `first_seq` the client learned and `fence` the most the
/// trim fence allows. A shape that finds nothing to aim at asks for
/// `fence`. A floor of 0 sends nothing.
fn truncate_floor(
    shape: usize,
    (floor, fence): (u64, u64),
    written: &[WrittenCommand],
    draw: u64,
) -> u64 {
    match TRUNCATE_SHAPES[shape % TRUNCATE_SHAPES.len()] {
        "uniform" => {
            let low = floor.min(fence);
            low + draw % (fence - low + 1)
        }
        // Strictly inside the last batch of several records this client
        // wrote, below the fence.
        "mid-batch" => written
            .iter()
            .rev()
            .find(|own| own.count > 1 && own.seq + 1 < fence)
            .map_or(fence, |own| {
                let inside = own.count.min(fence - own.seq) - 1;
                own.seq + 1 + draw % inside
            }),
        "floor" => floor.min(fence),
        "backward" => floor.min(fence).saturating_sub(1),
        _ => fence,
    }
}

/// Judge a truncation the journal applied at `first` (its `first_seq`
/// after it), asked up to `up_to` when the client knew the floor at
/// `before` (#659): a floor never moves back, and the gates for a floor
/// inside a batch and a backward ask.
fn judge_floor(first: u64, (before, up_to): (u64, u64), written: &[WrittenCommand]) {
    assert_always!(
        first >= before,
        "chain: a truncation never lowers the journal's floor",
        { "first_seq" => first, "before" => before, "up_to" => up_to }
    );
    if up_to < before {
        assert_reachable!("chain: a backward truncation leaves the floor");
    }
    if written
        .iter()
        .any(|own| own.seq < first && first < own.seq + own.count)
    {
        assert_reachable!("chain: a truncation floor lands inside a batch");
    }
}

impl ChainWorkload {
    /// One `TRUNCATE` step (#228): an owner truncates under its writer fence,
    /// clamped below every folding client's cursor. The floor it asks for is
    /// a shape drawn per step (#659 (partial and mid-batch truncation)).
    pub(super) async fn truncate_step<T, F>(
        &mut self,
        step: &Step<'_>,
        (writer, fold, written): (&mut Writer, &mut Fold, &[WrittenCommand]),
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
            journal,
            nodes,
            raw_class,
            raw_pause,
            raw_payload,
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
            let base = writer.next_seq().max(fold.cursor());
            let before = fold.floor();
            // A client that wrote a batch of several records aims inside
            // it on three steps in ten, whatever the seed's weights: the
            // shape needs that batch below the fence, which the weights
            // alone seldom line up (#659). A plain choice off the step's
            // draws, not a BUGGIFY site: a site's location is active on a
            // quarter of the seeds only, and the few steps that hold such a
            // batch then never met it in a whole sweep.
            let several = written.iter().any(|own| own.count > 1);
            let shape = if several && raw_policy.rotate_left(23) % 10 < 3 {
                assert_reachable!("chain: a truncation aims inside a batch the client wrote");
                MID_BATCH_SHAPE
            } else {
                weighted_index(&config.truncate_shape_weights, raw_class)
            };
            let up_to = fold::clamp(ctx.state(), journal, base).map_or(base, |fence| {
                truncate_floor(shape, (before, fence), written, raw_payload)
            });
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
            if let Some(TruncateOutcome::Applied { state }) = &outcome {
                let first = state.first_seq.0;
                judge_floor(first, (before, up_to), written);
                fold.learn_floor(first);
            }
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
