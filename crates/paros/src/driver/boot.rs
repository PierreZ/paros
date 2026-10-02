//! The boot report: on every (re)boot the core rebuilt its volatile state from
//! durable storage, and this re-emits that recovered belief for the oracles —
//! and the format-marker check (#147) that runs before the core reads a byte.
//! There is no application to replay (#186): the chosen prefix a node
//! recovered is its whole state.

use paros_core::{AcceptorConfig, Ballot, ColocatedNode, NodeId, Slot};

use crate::audit::{Audit, Deployment};
use crate::storage::LogStorage;

use super::config::{BootKind, BootRefusal, RunError};
use super::events::command_hash;
use super::ready::{report_applied, storage_fault_crash};

/// #147: judge the operator's claim against the store's format marker,
/// before the core reads a byte. The marker is what makes "a wiped identity
/// never rejoins" a property of the library rather than of whoever runs it:
/// an empty-but-openable store is indistinguishable from a first boot to
/// `ColocatedNode::new`, so the refusal has to happen here, on the claim. A
/// first boot formats the store durably first — the marker lands on disk no
/// later than the first promise, which is the ordering the refusal relies on.
///
/// #207: the marker records the [`Config`](paros_core::Config) it was written
/// under, and an existing member is refused when the operator now hands it
/// another one ([`BootRefusal::ConfigMismatch`]). The bootstrap membership,
/// the quorum system and the counts are safety inputs the core reads once,
/// at construction: an edited configuration file must never change them
/// across a restart — a node that silently switched from a majority to a
/// flexible split, or to a membership missing a peer, would count quorums
/// its peers do not.
///
/// # Errors
///
/// [`RunError::Refused`] when the claim and the marker disagree, or the
/// marker names another configuration (nothing was written);
/// [`RunError::Storage`] when formatting the store failed.
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id))]
pub(crate) async fn check_format_marker<S: LogStorage, A: Audit>(
    storage: &mut S,
    boot: BootKind,
    self_id: u64,
    audit: &A,
) -> Result<(), RunError> {
    let (_, operator) = storage.initial_state();
    let refusal = match (boot, storage.formatted_config()) {
        (BootKind::ExistingMember, Some(formatted)) if formatted == operator => return Ok(()),
        (BootKind::ExistingMember, Some(formatted)) => {
            tracing::error!(
                node = self_id,
                formatted = ?formatted,
                operator = ?operator,
                "boot_config_mismatch"
            );
            BootRefusal::ConfigMismatch
        }
        (BootKind::FirstBoot, None) => {
            storage
                .format(&operator)
                .await
                .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            storage
                .sync(paros_core::MustSync::Sync)
                .await
                .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            tracing::info!(node = self_id, "store_formatted");
            return Ok(());
        }
        (BootKind::ExistingMember, None) => BootRefusal::Amnesia,
        (BootKind::FirstBoot, Some(_)) => BootRefusal::AlreadyFormatted,
    };
    audit.boot_refused(NodeId(self_id), refusal);
    tracing::warn!(node = self_id, refusal = refusal.label(), "boot_refused");
    Err(RunError::Refused(refusal))
}

/// On (re)boot the core rebuilt its volatile state from durable storage. Re-emit
/// that recovered state so the oracles see this node's post-restart belief: the
/// recovered promised ballot (`node_state`, feeding the monotonic-promise check
/// across the restart seam), each recovered accepted record (`recovered`, feeding
/// the recovery oracle's "a restart never changes a pre-crash accepted value"
/// check), and the recovered chosen index, which is the node's whole walked
/// prefix: nothing is replayed, because no application sits behind it
/// (#186). A clean first boot has empty scalars/log, so this is a near no-op.
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id))]
pub(crate) fn report_boot_state<A: Audit>(node: &ColocatedNode, self_id: u64, audit: &A) {
    // Mark this incarnation coming up (every `booted` after a node's first is
    // a restart).
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
        replica_count: node.config().replica_count,
    };
    audit.recovered(
        NodeId(self_id),
        promised,
        node.hard_state().chosen_index,
        &deployment,
        &records,
    );
    // The recovered chosen prefix, re-reported as walked, in slot order: a
    // chosen slot's record is its authoritative accepted record, and the
    // prefix *is* this node's state (#186). A slot made durable and chosen
    // just before a crash at `AfterSyncBeforeSend` was never reported walked
    // by this node, and on a one-member journal no other node reports it
    // either (#189: the system journals on one seed; witness
    // 13093924963020097181) — a later dedup ack would then name a slot no
    // report ever applied. A replay of a slot already reported is
    // idempotent to every oracle.
    if let Some(chosen) = node.hard_state().chosen_index {
        let mut next = node.acceptor().first_slot();
        for (slot, (_, command)) in node.acceptor().records().range(..=chosen) {
            if *slot != next {
                break;
            }
            report_applied(
                audit,
                self_id,
                *slot,
                command,
                node.replica().outcome_at(*slot),
            );
            next = Slot(slot.0 + 1);
        }
    }
}
