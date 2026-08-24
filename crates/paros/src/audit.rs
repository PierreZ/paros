//! The **audit port**: the driver's provider-generic observation seam.
//!
//! [`Audit`] is the mirror image of [`DriverHooks`](crate::DriverHooks). Hooks
//! *perturb* the driver — they answer "should I take this rare-but-valid
//! alternative?" and the driver's behavior changes with the answer. The audit
//! only *observes*: the driver reports every externally meaningful state
//! transition, typed, at the instant it happens, and nothing it returns (it
//! returns nothing) can influence the run. Deleting every audit call must leave
//! the shipped program bit-identical.
//!
//! Each callback fires **after** the transition it reports is real: a durable
//! write after its fsync, an apply after the application saw it, a send beside
//! the transmit. They sit exactly where the driver's `tracing` events already
//! are — the trace stays for humans and the wasm demo, while correctness
//! checking moves here, where an implementation can fold each transition into
//! O(1) incremental state instead of re-scanning a growing event stream.
//!
//! Production passes [`NoAudit`]; every method defaults to a no-op.

use paros_core::{Ballot, Message, NodeId, Slot};

use crate::hooks::Seam;

/// Provider-generic observation port for [`run_node`](crate::run_node).
///
/// Pure observation: implementations must not influence the driver (that is
/// [`DriverHooks`](crate::DriverHooks)' job) and must not block — a callback
/// runs inline on the node loop.
#[allow(unused_variables)]
pub trait Audit {
    /// This node durably raised its promised ballot (after the fsync).
    fn promised(&self, node: NodeId, ballot: Ballot) {}

    /// This node durably accepted `command` (hashed to `vhash`) at `ballot`
    /// for `slot`. `promised` is the node's promise at the time of the write,
    /// so the never-accept-above-promise invariant is checkable per slot.
    fn accepted(&self, node: NodeId, slot: Slot, ballot: Ballot, promised: Ballot, vhash: u64) {}

    /// This node durably advanced its chosen index.
    fn chosen_index(&self, node: NodeId, index: Slot) {}

    /// This node durably truncated its log prefix; `first` is the new
    /// compaction floor (the first slot still retained).
    fn truncated(&self, node: NodeId, first: Slot) {}

    /// This node installed an opaque application snapshot from a peer, jumping
    /// its chosen prefix to `chosen_index` and adopting `ballot`.
    fn snapshot_installed(&self, node: NodeId, chosen_index: Slot, ballot: Ballot) {}

    /// This node applied the chosen command at `slot` (hashed to `vhash`),
    /// advancing its contiguous applied prefix.
    fn applied(&self, node: NodeId, slot: Slot, vhash: u64) {}

    /// This node handed `msg` to the transport, addressed to `to`. Reports the
    /// core's outbound decision even when the network later drops it.
    fn sent(&self, node: NodeId, to: NodeId, msg: &Message) {}

    /// This node became leader at `won`, holding `promised` at that instant and
    /// having filled `gap_fills` undecided holes with no-ops.
    fn elected(&self, node: NodeId, won: Ballot, promised: Ballot, gap_fills: u64) {}

    /// The driver asked this leader to resign, and it did.
    fn stepped_down(&self, node: NodeId) {}

    /// This node holds a chosen slot above its contiguous applied prefix:
    /// `hole` is the first slot missing, `above` the highest chosen slot past
    /// it. Reported once per tick for as long as the gap lasts.
    fn chosen_gap(&self, node: NodeId, hole: Slot, above: Slot) {}

    /// This node answered a client proposal with a *committed* ack naming
    /// `slot`; `applied` is the node's own applied prefix at that instant.
    /// `dedup` marks the fast-path ack of a retry whose request was already
    /// chosen (`ProposeResult::Chosen`), as opposed to the ack-on-commit path.
    #[allow(clippy::fn_params_excessive_bools)]
    fn client_acked(
        &self,
        node: NodeId,
        client: u64,
        seq: u64,
        slot: Slot,
        applied: Option<Slot>,
        dedup: bool,
    ) {
    }

    /// This node answered a client read with a confirmed watermark (`None` is
    /// the empty applied prefix, which is not slot 0).
    fn read_confirmed(&self, node: NodeId, index: Option<Slot>) {}

    /// This node (re)booted, having rebuilt volatile state from durable
    /// storage: its recovered promise plus every `(slot, ballot, vhash)`
    /// accepted record it read back.
    fn recovered(&self, node: NodeId, promised: Ballot, accepted: &[(Slot, Ballot, u64)]) {}

    /// This node crashed at a durability `seam` inside a `Ready` batch.
    fn crashed(&self, node: NodeId, seam: Seam) {}

    /// This node dropped one outbound message at the send seam (hook-decided
    /// per-message loss, indistinguishable from network loss to the peers).
    fn dropped_at_send(&self, node: NodeId, to: NodeId, msg: &Message) {}

    /// The driver deliberately sent this one outbound message twice
    /// ([`DriverHooks::duplicate_outgoing`](crate::DriverHooks)).
    fn duplicated_at_send(&self, node: NodeId, to: NodeId, msg: &Message) {}

    /// The driver deliberately dropped this one client-facing reply after the
    /// server state advanced ([`DriverHooks::drop_client_reply`](crate::DriverHooks)).
    fn client_reply_dropped(&self, node: NodeId, reply: crate::hooks::Reply) {}

    /// A snapshot install persisted while this node was a live Candidate — the
    /// #88 window (`on_install_snapshot` deliberately does not touch the
    /// election, so the campaign stays open across the install).
    fn snapshot_mid_election(&self, node: NodeId) {}

    /// This node materialized `offers` snapshot transfers into the common
    /// outbound path (reported before the after-sync/before-send seam).
    fn snapshot_offered(&self, node: NodeId, offers: u64) {}

    /// This node deliberately skipped re-sending its pending `Accept`s.
    fn resend_skipped(&self, node: NodeId) {}

    /// This node selected the shortest valid election timeout.
    fn election_timeout_extreme(&self, node: NodeId, ticks: u64) {}

    /// This node received a `Prepare` below its own compaction floor — the
    /// "campaign against a truncated acceptor" interleaving.
    fn prepare_below_floor(&self, node: NodeId, from_slot: Slot, floor: Slot) {}
}

/// Inert production audit: every observation is dropped.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoAudit;

impl Audit for NoAudit {}
