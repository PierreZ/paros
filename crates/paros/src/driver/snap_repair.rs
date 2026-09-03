//! The driver's snapshot-point repair layer (#101, CTRL §3.5): the custody
//! tally a leader couples `Truncate` to, and the per-tick chunk pull that
//! heals a node's own rotted snapshot point from its peers. Volatile
//! per-incarnation state — consensus never depends on snapshot custody.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{Message, NodeId, RawNode, Slot, Value};

use crate::audit::Audit;
use crate::hooks::{DriverHooks, Seam};
use crate::storage::NodeStorage;

use super::config::RunError;
use super::ready::storage_fault_crash;
use super::transport::{Outbound, send_messages};

/// The driver's **snapshot-point repair layer** (#101, CTRL §3.5). Volatile
/// per-incarnation state beside the sans-IO core: consensus never depends on
/// snapshot custody, so all of this lives in the driver.
///
/// - Every node advertises its latest recorded decided snapshot point to the
///   leader once per tick ([`Message::SnapAck`]); the leader's set-based tally
///   is what gates the `Truncate` coupling rule (truncation is proposed only
///   once a quorum holds the covering point).
/// - A node whose boot scan reported rotted chunks of its retained point
///   pulls them from peers once per tick ([`Message::SnapChunkRequest`]);
///   peers answer chunks they hold clean, stay silent about what they lack,
///   and answer a point they have advanced past with the whole-blob
///   [`Message::InstallSnapshot`] fallback.
#[derive(Default)]
pub(crate) struct SnapRepair {
    /// Leader tally: decided snapshot point → nodes advertising custody of
    /// it. Points only ever advance, so a stale entry is still a sound
    /// coupling witness (any retained point at or past `up_to` covers a
    /// `Truncate{up_to}`). Pruned below the compaction floor each tick: a
    /// point the floor already passed can license no further truncation, and
    /// without the prune the map grew one entry per decided marker for the
    /// life of the incarnation.
    pub(crate) acks: BTreeMap<Slot, BTreeSet<NodeId>>,
    /// This node's rotted chunks of its retained point, awaiting peer repair.
    pub(crate) pending: BTreeMap<u64, BTreeSet<u32>>,
    /// A `Snap` marker this leadership proposed and is still waiting to see
    /// quorum custody for — dedupes marker proposals across compact retries.
    pub(crate) marker_pending: Option<Slot>,
}

/// Answer one peer's chunk request (see [`SnapRepair`]): chunks of the shared
/// point, silence for what this node lacks, or the whole-blob advanced
/// fallback — guarded exactly like a snapshot offer (the served state must
/// cover the advertised boundary).
// The repair layer's full context is exactly these handles; bundling them
// would only rename the same eight things.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0, to = to.0, at = at.0, chunks = chunks.len()))]
pub(crate) fn handle_snap_chunk_request<S, H, A>(
    node: &RawNode,
    storage: &S,
    out: &Outbound,
    hooks: &H,
    audit: &A,
    to: NodeId,
    at: Slot,
    chunks: &[u32],
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let me = NodeId(out.self_id);
    match storage.latest_snap_point() {
        Some(point) if point == at => {
            let served: Vec<(u32, Value)> = chunks
                .iter()
                .filter_map(|chunk| {
                    let bytes = storage.read_snap_chunk(at, *chunk)?;
                    // Silence about a chunk this node holds is the same
                    // answer as silence about one it lacks; the requester
                    // re-asks every tick. Consulted only for a chunk that
                    // would otherwise be served.
                    if hooks.withhold_snap_chunk(to) {
                        audit.snap_chunk_withheld(me, to);
                        tracing::info!(node = me.0, at = at.0, chunk, "snap_chunk_withheld");
                        return None;
                    }
                    Some((*chunk, Value(bytes)))
                })
                .collect();
            if !served.is_empty() {
                send_messages(
                    out,
                    hooks,
                    audit,
                    vec![(
                        to,
                        Message::SnapChunkResponse {
                            config_id: node.hard_state().config_id,
                            from: me,
                            at_index: at,
                            chunks: served,
                        },
                    )],
                );
            }
        }
        Some(point) if point > at => {
            // The advanced whole-blob fallback: this node no longer retains
            // the requested point, so it serves its current snapshot instead,
            // under the same guard as any snapshot offer — the opaque bytes
            // must describe exactly the boundary the message names.
            let Some(ci) = node.hard_state().chosen_index else {
                return;
            };
            if node.app_repair().is_some() || storage.applied_slot() != Some(ci) {
                return;
            }
            let ballot = node
                .accepted()
                .get(&ci)
                .map_or(node.hard_state().max_promised_ballot, |(b, _)| *b);
            audit.snap_advanced_fallback(me, to);
            tracing::info!(node = out.self_id, to = to.0, "snap_advanced_fallback");
            send_messages(
                out,
                hooks,
                audit,
                vec![(
                    to,
                    Message::InstallSnapshot {
                        config_id: node.hard_state().config_id,
                        from: me,
                        ballot,
                        chosen_index: ci,
                        snapshot: Value(storage.snapshot()),
                        sessions: node.session_ledger(),
                    },
                )],
            );
        }
        // A point this node does not hold answers nothing: absence carries no
        // information (CTRL Figure 6 Box B), and a node *behind* the requested
        // point has nothing sound to say about it either.
        _ => {}
    }
}

/// Install repaired chunks from one peer's response, flush them durably, and
/// — once the point is whole again — restore the application from it if the
/// live state was lost below the floor (re-pointing the core's repair pump at
/// the floor so the retained suffix re-emits in order).
// The repair layer's full context is exactly these handles (see
// `handle_snap_chunk_request`); bundling them would only rename them.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id, at = at.0, chunks = chunks.len()))]
pub(crate) fn handle_snap_chunk_response<S, H, A>(
    node: &mut RawNode,
    storage: &mut S,
    snap: &mut SnapRepair,
    hooks: &H,
    audit: &A,
    self_id: u64,
    at: Slot,
    chunks: &[(u32, Value)],
) -> Result<(), RunError>
where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let Some(pending) = snap.pending.get(&at.0).cloned() else {
        return Ok(());
    };
    let mut installed = 0_u64;
    let mut bytes = 0_u64;
    // The store's own verdict on the *point*: whether it is whole now. The
    // driver's optimism is not a substitute — a write the store refuses (a
    // point it no longer retains, a payload whose length or bytes do not
    // match the decided state) installs nothing, and crossing the chunk off
    // `pending` anyway dropped the point's entry and left it permanently
    // incomplete for the rest of the incarnation.
    let mut clean = false;
    for (chunk, payload) in chunks {
        if !pending.contains(chunk) {
            continue;
        }
        clean = storage
            .write_snap_chunk(at, *chunk, &payload.0)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        installed += 1;
        bytes += u64::try_from(payload.0.len()).unwrap_or(u64::MAX);
    }
    if installed == 0 {
        return Ok(());
    }
    // Crash seam: the repaired chunks are staged but not yet flushed — the
    // only durable-write pipeline outside `drain_ready`'s seam machinery. A
    // crash here loses the staged installs whole; the reboot's scan still
    // reports the chunks faulty and the per-tick pull re-runs the repair.
    if hooks.crash_at(Seam::BeforeChunkSync) {
        audit.crashed(NodeId(self_id), Seam::BeforeChunkSync);
        tracing::info!(node = self_id, seam = "before_chunk_sync", "crashed");
        return Err(RunError::SeamCrash(Seam::BeforeChunkSync));
    }
    // Flush the chunk installs durably before reporting them (and before the
    // restore below stages the recovered application state).
    storage
        .sync(paros_core::MustSync::Sync)
        .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    let blob_bytes = storage.snap_chunk_count(at).map_or(0, |count| {
        u64::from(count) * crate::storage::SNAP_CHUNK_BYTES as u64
    });
    audit.snap_chunk_repaired(NodeId(self_id), at, installed, bytes, blob_bytes);
    tracing::info!(
        node = self_id,
        at = at.0,
        chunks = installed,
        bytes,
        blob_bytes,
        "snap_chunk_repaired"
    );
    let complete = clean;
    if complete {
        snap.pending.remove(&at.0);
    } else {
        // Not whole: re-derive what the store still classifies faulty for
        // this point rather than crossing off what this response happened to
        // carry. Re-asking for a chunk already installed is free — the write
        // is idempotent and the pull runs once a tick — while forgetting one
        // the store never took costs the point for the incarnation.
        let still_faulty: BTreeSet<u32> = storage
            .faulty_snap_chunks()
            .into_iter()
            .filter(|(point, _)| *point == at)
            .map(|(_, chunk)| chunk)
            .collect();
        if still_faulty.is_empty() {
            snap.pending.remove(&at.0);
        } else {
            snap.pending.insert(at.0, still_faulty);
        }
        // Every chunk the point still lacked arrived in this response and
        // every write returned `Ok`, yet the store does not call the point
        // whole: it refused at least one of them.
        if pending
            .iter()
            .all(|chunk| chunks.iter().any(|(offered, _)| offered == chunk))
        {
            audit.snap_chunk_rejected(NodeId(self_id), at);
            tracing::info!(node = self_id, at = at.0, "snap_chunk_rejected");
        }
    }
    if complete
        && let Some(point) = storage
            .restore_from_snap_point()
            .map_err(|e| storage_fault_crash(audit, self_id, e))?
    {
        // Crash seam: the application restore is staged (the chunks above are
        // already durable) but its fsync has not happened. A crash here loses
        // the staged restore only; the reboot lands below the floor with a
        // clean point and recovers through a peer's `InstallSnapshot` instead.
        if hooks.crash_at(Seam::AfterChunkRestoreBeforeSync) {
            audit.crashed(NodeId(self_id), Seam::AfterChunkRestoreBeforeSync);
            tracing::info!(
                node = self_id,
                seam = "after_chunk_restore_before_sync",
                "crashed"
            );
            return Err(RunError::SeamCrash(Seam::AfterChunkRestoreBeforeSync));
        }
        storage
            .sync(paros_core::MustSync::Sync)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        audit.snap_point_restored(NodeId(self_id), point);
        tracing::info!(node = self_id, at = point.0, "snap_point_restored");
        // The application jumped to the point (= floor - 1); re-point the
        // core's repair pump at the floor so the retained decided suffix
        // re-emits in order through the ordinary committed seam.
        if node.app_repair().is_some() {
            node.open_app_repair(node.first_slot());
        }
    }
    Ok(())
}

/// Per-tick snapshot-repair upkeep (see [`SnapRepair`]): custody
/// advertisement toward the leader, the leader's own tally and marker
/// bookkeeping, and the chunk-repair pull.
#[tracing::instrument(level = "trace", skip_all, fields(node = node.config().id.0))]
pub(crate) fn snap_repair_tick<S, H, A>(
    node: &RawNode,
    storage: &S,
    out: &Outbound,
    hooks: &H,
    audit: &A,
    snap: &mut SnapRepair,
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let me = NodeId(out.self_id);
    let latest = storage.latest_snap_point();
    // A point the compaction floor already passed can license no further
    // truncation, so it is no longer a witness worth keeping.
    snap.acks.retain(|point, _| *point >= node.first_slot());
    if node.is_leader() {
        // The leader is its own first custodian, and a marker stops being
        // outstanding once a quorum advertises the point it created.
        if let Some(point) = latest {
            snap.acks.entry(point).or_default().insert(me);
        }
        // The one boundary every quorum question crosses: the configuration
        // in force decides, so an ack from a node it no longer names stops
        // counting the moment a reconfiguration takes effect.
        if let Some(marker) = snap.marker_pending
            && snap
                .acks
                .get(&marker)
                .is_some_and(|holders| node.acceptors().has_phase2_quorum(holders))
        {
            snap.marker_pending = None;
        }
    } else {
        snap.marker_pending = None;
        if let (Some(point), Some(leader)) = (latest, node.leader())
            && leader != me
        {
            // The advertisement is due: consult the pacing hook only now, when
            // skipping has an observable effect (a lost beat of the leader's
            // custody tally, re-sent next tick).
            if hooks.skip_snap_advertisement() {
                tracing::info!(node = out.self_id, "snap_advertisement_skipped");
            } else {
                send_messages(
                    out,
                    hooks,
                    audit,
                    vec![(
                        leader,
                        Message::SnapAck {
                            config_id: node.hard_state().config_id,
                            from: me,
                            at_index: point,
                        },
                    )],
                );
            }
        }
    }
    // The chunk pull: once per tick, ask every peer for the still-missing
    // chunks of the retained point. Pending chunks of a point this store has
    // advanced past are obsolete — the newer point covers everything.
    snap.pending
        .retain(|at, chunks| latest == Some(Slot(*at)) && !chunks.is_empty());
    if let Some((&at, chunks)) = snap.pending.iter().next() {
        // The pull is due: consult the pacing hook only now (skipping delays
        // the repair one beat; the pull re-issues every tick it is due).
        if hooks.skip_chunk_pull() {
            tracing::info!(node = out.self_id, "chunk_pull_skipped");
            return;
        }
        let wanted: Vec<u32> = chunks.iter().copied().collect();
        // Every pooled node is a replica that may hold the point.
        let requests: Vec<(NodeId, Message)> = node
            .config()
            .pool()
            .iter()
            .filter(|peer| **peer != me)
            .map(|peer| {
                (
                    *peer,
                    Message::SnapChunkRequest {
                        config_id: node.hard_state().config_id,
                        from: me,
                        at_index: Slot(at),
                        chunks: wanted.clone(),
                    },
                )
            })
            .collect();
        send_messages(out, hooks, audit, requests);
    }
}
