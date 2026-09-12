//! The node's **read wiring**, and the back half the two read tallies share.
//!
//! Two tallies serve a linearizable read: the leader's read-index rounds
//! ([`crate::proposer::Authority`], confirmed by a Phase-2 quorum of beat
//! acks in creation order) and the leaderless quorum reads
//! ([`crate::quorum_read::QuorumReads`], confirmed by a whole row's vote
//! watermarks, wired in `node/quorum_reads.rs`). Their front halves differ
//! by design — what a confirmation *is* — and their back halves are one
//! thing: a confirmed read waits for the replica's chosen prefix to cover
//! its index, surfaces as a [`ReadState`] through the same
//! [`crate::Ready::read_states`], and is dropped silently after the same
//! window when it cannot complete. That back half lives here, once:
//! [`READ_TTL_TICKS`], [`ColocatedNode::serve_reads`] and
//! [`ColocatedNode::tick_reads`].

use super::{ColocatedNode, NodeRole, ReadState, Slot};

/// Ticks a pending read — a read-index round waiting for its ack quorum, a
/// quorum read waiting for its row to answer whole or for the replica to
/// cover the index it settled on — may wait before the node
/// garbage-collects it (lost acks, an unreachable quorum or row). Dropped
/// silently: a read carries no durable obligation, and the driver owns the
/// client reply (its retry sweep answers first, well inside this window).
/// A watermark raised by an accept that never decided needs the next
/// leader's gap fill to be covered, which is why the window is not shorter
/// than an election.
pub(super) const READ_TTL_TICKS: u64 = 20;

impl ColocatedNode {
    /// Hand the proposer this node's active configuration and chosen prefix
    /// and queue every read-index round they confirm
    /// ([`crate::proposer::Proposer::confirm_reads`], where the Phase-2
    /// argument behind a read lives).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn try_confirm_reads(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        let confirmed = self
            .proposer
            .confirm_reads(&self.acceptors, self.replica.chosen_index());
        self.surface_reads(confirmed);
    }

    /// Hand the quorum-read tally the replica's answer and queue every read
    /// it serves: a read confirmed at `index` surfaces once the chosen
    /// prefix covers it.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn serve_quorum_reads(&mut self) {
        let replica = &self.replica;
        let served = self.quorum_reads.serve(|index| replica.covers(index));
        self.surface_reads(served);
    }

    /// Advance both tallies against what this node now knows — the acks it
    /// credited, the prefix it applied — and surface every read they serve.
    /// Called wherever the answer can change for either: the chosen prefix
    /// advanced, a tick.
    pub(super) fn serve_reads(&mut self) {
        self.try_confirm_reads();
        self.serve_quorum_reads();
    }

    /// Per-tick upkeep for both tallies: drop the reads that outlived
    /// [`READ_TTL_TICKS`], then serve what the prefix may have covered
    /// since. No re-broadcast is needed for the live read-index rounds:
    /// every leader tick already broadcasts a fresh, higher-seq beat whose
    /// acks confirm all older pending rounds.
    pub(super) fn tick_reads(&mut self) {
        let now = self.tick_count;
        self.proposer.expire_reads(now, READ_TTL_TICKS);
        self.quorum_reads.expire(now, READ_TTL_TICKS);
        self.serve_reads();
    }

    /// The one seam a served read crosses to the driver: a `(ctx, index)`
    /// pair from either tally becomes a [`ReadState`] in
    /// [`crate::Ready::read_states`], in the order the tally served it.
    fn surface_reads(&mut self, served: Vec<(u64, Option<Slot>)>) {
        self.pending_read_states.extend(
            served
                .into_iter()
                .map(|(ctx, index)| ReadState { ctx, index }),
        );
    }
}
