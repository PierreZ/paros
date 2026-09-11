//! The node's **standing-authority wiring**: `CheckQuorum` (#95), the
//! per-tick re-proof that a Phase-2 quorum of the leader's configuration can
//! still reach it. The [`Proposer`](crate::proposer::Proposer) counts the
//! window and answers whether it holds; this module owns the policy — how
//! long a window may run, and that an empty one resigns.

use super::{ColocatedNode, NodeRole};

impl ColocatedNode {
    /// `CheckQuorum` (#95): a leader must re-prove, once per election
    /// timeout, that an ack quorum can still reach it. Without this, an
    /// idle leader cut off from its quorum stays Leader forever — its
    /// election clock is frozen (the election branch of
    /// [`ColocatedNode::tick`] runs only for non-leaders), below-promise
    /// beats are ignored unacked rather than Nacked, and an idle leader
    /// emits no `Accept`s whose Nack could demote it — while it keeps
    /// admitting proposals into a stale suffix for the whole partition,
    /// feeding #94's double-apply. The window is the same length as the
    /// election timeout (etcd-raft's `CheckQuorum`), so a demoted leader's
    /// peers are already eligible to campaign by the time it steps down.
    /// Every beat is acked by every reachable follower each tick, so a
    /// healthy leader trivially refills the window.
    ///
    /// A **Phase-2** quorum, for the reason spelled out at the read fence
    /// (`node/reads.rs`): a leader's authority is the claim that no later
    /// ballot has decided behind it, which every future Phase-1 quorum's
    /// intersection with this ack set rules out. On a delegated round the
    /// window is fed by `HeartbeatAck` alone: a proxy's votes never reach
    /// this node's tally (`node/phase2.rs`).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn tick_check_quorum(&mut self) {
        if self.role != NodeRole::Leader || self.election_timeout == 0 {
            return;
        }
        if self.proposer.tick_authority() < self.election_timeout {
            return;
        }
        if self.proposer.authority_holds(&self.acceptors) {
            let me = self.config.id;
            self.proposer
                .renew_authority(self.is_acceptor().then_some(me));
        } else {
            self.counters.quorum_lost_step_downs += 1;
            self.become_follower(None);
        }
    }
}
