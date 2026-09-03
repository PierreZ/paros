//! The boot replay: on every (re)boot the core rebuilt its volatile state from
//! durable storage, and this re-emits that recovered belief for the oracles,
//! walks the retained chosen prefix back through the application, and opens the
//! application repair when the prefix cannot be replayed locally.

use paros_core::{AcceptorConfig, Ballot, ColocatedNode, Command, Control, NodeId, Slot};

use crate::audit::{Audit, Deployment};
use crate::hooks::{DriverHooks, Seam};
use crate::storage::NodeStorage;

use super::config::RunError;
use super::events::command_hash;
use super::ready::storage_fault_crash;

/// On (re)boot the core rebuilt its volatile state from durable storage. Re-emit
/// that recovered state so the oracles see this node's post-restart belief: the
/// recovered promised ballot (`node_state`, feeding the monotonic-promise check
/// across the restart seam), each recovered accepted record (`recovered`, feeding
/// the recovery oracle's "a restart never changes a pre-crash accepted value"
/// check), and each rebuilt chosen entry (`value_chosen`, feeding
/// at-most-one-value-chosen). The apply replay (`log_applied`) covers a crash
/// between "`chosen_index` durable" and "apply side-effects done"; it is
/// idempotent (the chosen index is the applied index). A compacted node's
/// accepted log starts at its floor, so the replay naturally covers only the
/// retained prefix. A clean first boot has empty scalars/log, so this is a near
/// no-op.
// One linear boot replay: report → walk → repair; splitting it would scatter
// the ordering contract between the three.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id))]
pub(crate) fn replay_boot_state<S: NodeStorage, H: DriverHooks, A: Audit>(
    node: &mut ColocatedNode,
    storage: &mut S,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError> {
    // Mark this incarnation coming up. The recovery recorder turns every `booted`
    // after a node's first into a *restart* event for the animation.
    tracing::info!(node = self_id, "booted");

    let promised = node.hard_state().max_promised_ballot;
    tracing::info!(
        node = self_id,
        pround = promised.round,
        pbnode = promised.node.0,
        "node_state"
    );
    // One typed report of the whole recovered belief: the promise plus every
    // durable accepted record read back. Built once so the audit sees the boot
    // as a single transition, matching the `recovered` trace stream.
    let mut records: Vec<(Slot, Ballot, u64)> = Vec::with_capacity(node.acceptor().records().len());
    for (slot, (ballot, command)) in node.acceptor().records() {
        let vhash = command_hash(command);
        records.push((*slot, *ballot, vhash));
        tracing::info!(
            node = self_id,
            slot = slot.0,
            around = ballot.round,
            abnode = ballot.node.0,
            vhash,
            "recovered"
        );
    }
    // Stage 8: surface the scan's recoverable classification *before* the
    // recovered-state report — the audit's explained-divergence rule keys on
    // it (a recovered log may omit a persisted record only after a detected
    // corruption crash or a reported-faulty event).
    let faulty: Vec<(Slot, Ballot)> = node
        .acceptor()
        .faulty()
        .iter()
        .map(|(slot, ballot)| (*slot, *ballot))
        .collect();
    if !faulty.is_empty() {
        audit.faulty_reported(NodeId(self_id), &faulty);
        for (slot, ballot) in &faulty {
            tracing::info!(
                node = self_id,
                slot = slot.0,
                around = ballot.round,
                abnode = ballot.node.0,
                "faulty_reported"
            );
        }
    }
    // The recovered chosen index and the configured cluster size travel with
    // the boot report: the index anchors the cross-restart chosen-prefix
    // checks, the size lets a checker do quorum arithmetic without guessing
    // the topology from partial boot observations.
    let deployment = Deployment {
        bootstrap: AcceptorConfig::new(node.config().peers.clone(), node.config().quorum_system),
        pool: node.config().pool().to_vec(),
        matchmakers: node.config().matchmakers.clone(),
        matchmaker_pool: node.config().matchmaker_pool().to_vec(),
    };
    audit.recovered(
        NodeId(self_id),
        promised,
        node.hard_state().chosen_index,
        &deployment,
        &records,
    );
    let mut replayed_application = false;
    let mut replayed_snap_points: Vec<Slot> = Vec::new();
    let mut repair_from: Option<Slot> = None;
    if let Some(ci) = node.hard_state().chosen_index {
        let applied_slot = storage.applied_slot();
        let floor = node.acceptor().first_slot();
        let resume = applied_slot.map_or(Slot(0), |a| Slot(a.0.saturating_add(1)));
        if resume < floor {
            // The application prefix stops below the compaction floor (the
            // snapshot state was lost): the log cannot replay the missing
            // range — only a peer's InstallSnapshot can. Open the repair and
            // apply nothing; consensus keeps serving every slot it can read.
            repair_from = Some(resume);
        } else {
            for s in floor.0..=ci.0 {
                let slot = Slot(s);
                let record = node.acceptor().records().get(&slot);
                let Some((_b, stored)) = record else {
                    if applied_slot.is_some_and(|applied| slot <= applied) {
                        // The record rotted but its effect is already durable
                        // in the application state, which is the authority for
                        // its own prefix; the reported-faulty event explains
                        // the emission gap to the oracles.
                        continue;
                    }
                    // A chosen record this node cannot read, not yet applied:
                    // the replay stops here — contiguity is the contract — and
                    // the repair pump re-emits the healed range via catch-up.
                    repair_from = Some(slot);
                    break;
                };
                // A #94 duplicate slot replays exactly as the live walk applied
                // it: a no-op. The core re-derived `duplicate_slots` from the
                // sealed sessions + the retained log in `ColocatedNode::new`, so the
                // substitution is deterministic across the restart.
                let noop = Command::Control(Control::Noop);
                let command = if node.replica().duplicate_slots().contains(&slot) {
                    &noop
                } else {
                    stored
                };
                if applied_slot.is_none_or(|applied| slot > applied) {
                    storage
                        .apply(ci, slot, command)
                        .map_err(|e| storage_fault_crash(audit, self_id, e))?;
                    // A freshly replayed `Snap` marker re-captures its decided
                    // point (#101): the application state at this walk instant
                    // is the boundary state, exactly as in the live apply
                    // loop. An already-applied marker is skipped — its point
                    // flushed with the same batch as its apply.
                    if let Command::Control(Control::Snap { .. }) = command {
                        storage
                            .record_snapshot(slot)
                            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
                        replayed_snap_points.push(slot);
                    }
                    replayed_application = true;
                }
                let vhash = command_hash(command);
                audit.applied(
                    NodeId(self_id),
                    slot,
                    vhash,
                    match command {
                        Command::User(e) => Some((e.client.0, e.seq.0)),
                        Command::Control(_) => None,
                    },
                );
                tracing::info!(node = self_id, slot = slot.0, vhash, "value_chosen");
                tracing::info!(
                    node = self_id,
                    slot = slot.0,
                    applied_index = slot.0,
                    "log_applied"
                );
            }
        }
    }
    if replayed_application {
        // Crash seam: the replayed prefix is staged but not yet flushed. The
        // next incarnation replays the same prefix from the same durable
        // state, so this is the idempotence of the boot replay itself under
        // test — the one seam a crash *between* batches can never reach,
        // because it sits before the first batch.
        if hooks.crash_at(Seam::AfterBootReplayBeforeSync) {
            audit.crashed(NodeId(self_id), Seam::AfterBootReplayBeforeSync);
            tracing::info!(
                node = self_id,
                seam = "after_boot_replay_before_sync",
                "crashed"
            );
            return Err(RunError::SeamCrash(Seam::AfterBootReplayBeforeSync));
        }
        storage
            .sync(paros_core::MustSync::Sync)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }
    for at in &replayed_snap_points {
        audit.snap_recorded(NodeId(self_id), *at);
        tracing::info!(node = self_id, at = at.0, "snap_recorded");
    }
    if let Some(from) = repair_from {
        let below_floor = from < node.acceptor().first_slot();
        node.open_app_repair(from);
        audit.app_repair_started(NodeId(self_id), from, below_floor);
        tracing::info!(
            node = self_id,
            from = from.0,
            below_floor,
            "app_repair_started"
        );
    }
    Ok(())
}
