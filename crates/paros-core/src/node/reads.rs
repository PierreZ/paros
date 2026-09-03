use super::{BTreeSet, NodeId, NodeRole, RawNode, ReadState, Slot};

/// Ticks a pending read-index round may wait for its ack quorum before the
/// leader garbage-collects it (lost acks, an unreachable quorum). Dropped
/// silently: the round carries no durable obligation, and the driver owns the
/// client reply (its retry sweep answers first, well inside this window).
pub(super) const READ_ROUND_TTL_TICKS: u64 = 20;

/// Volatile state of one in-flight read-index round (leader only).
pub(super) struct ReadRound {
    /// The driver-supplied correlation token.
    pub(super) ctx: u64,
    /// The captured read index: `max(chosen_index, read_floor)` at capture time.
    pub(super) index: Option<Slot>,
    /// The beat sequence an ack must answer (at or after) to credit this round:
    /// the heartbeat broadcast when the round began. Later beats' acks count
    /// too, so one ack can confirm every older pending round.
    pub(super) required_seq: u64,
    /// Peers (incl. self) that acked a qualifying beat at the round's ballot.
    pub(super) acked_by: BTreeSet<NodeId>,
    /// Tick the round was created on, for TTL garbage collection.
    pub(super) created_tick: u64,
}

impl RawNode {
    /// Confirm the eligible prefix of pending read rounds, in creation order: a
    /// round resolves once a quorum (incl. self) acked a qualifying beat AND the
    /// chosen prefix covers the round's index (the fresh-leader fence resolves
    /// here, via [`RawNode::advance_chosen_index`]). Confirmability is monotone
    /// in creation order — a later round's index and required seq are both at or
    /// above an earlier one's — so scanning the front suffices.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn try_confirm_reads(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        while let Some(round) = self.read_rounds.first() {
            let confirmed = self.acceptors.has_quorum(&round.acked_by)
                && self.replica.chosen_index() >= round.index;
            if !confirmed {
                break;
            }
            let round = self.read_rounds.remove(0);
            self.pending_read_states.push(ReadState {
                ctx: round.ctx,
                index: round.index,
            });
        }
    }
}
