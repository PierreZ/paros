use super::{NodeRole, RawNode, ReadState};

/// Ticks a pending read-index round may wait for its ack quorum before the
/// leader garbage-collects it (lost acks, an unreachable quorum). Dropped
/// silently: the round carries no durable obligation, and the driver owns the
/// client reply (its retry sweep answers first, well inside this window).
pub(super) const READ_ROUND_TTL_TICKS: u64 = 20;

impl RawNode {
    /// Hand the proposer this node's active configuration and chosen prefix
    /// and queue every read round they confirm
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
        self.pending_read_states.extend(
            confirmed
                .into_iter()
                .map(|(ctx, index)| ReadState { ctx, index }),
        );
    }
}
