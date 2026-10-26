//! The proxy leader's reports (§3.1, #142), and the leader's side of a delegation.

use moonpool_sim::{TimeProvider, assert_reachable};
use paros::core::{Ballot, Message, NodeId, Party, ProxyId, Slot};
use paros::roles::node::command_hash;

use super::NodeAudit;

impl<T: TimeProvider> NodeAudit<T> {
    pub(super) fn proxy_sent(&self, proxy: ProxyId, to: NodeId, msg: &Message) {
        self.count_sent(msg);
        let from = Party::Proxy(proxy);
        match msg {
            // The fan-out carries the leader's command to the ballot's own
            // acceptors: *whom* and *what* are judged exactly as the leader's
            // colocated `Accept` is; *who* is the leader the hint names, whose
            // authority the delegation already exercised.
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } => {
                let vhash = command_hash(command);
                let mut st = self.state();
                st.observe_accept_send(from, to.0, *ballot);
                st.observe_proposal(from, *ballot, slot.0, vhash);
            }
            Message::Commit { .. } => self.observe_durable_send(from, msg),
            _ => {}
        }
    }

    pub(super) fn delegation_taken_back(&self, _node: NodeId, _slot: Slot, _proxy: ProxyId) {
        self.state().delegation_taken_back = true;
    }

    // The proxy's own paths — a reboot, a re-fan-out, an ignored or
    // superseded delegation, a relayed `Nack` — are reported through the
    // port but not gated here: the model checker proves each in-core, and
    // the slot budget (512 per campaign process) is spent on outcomes.
    #[allow(clippy::too_many_arguments, reason = "the `Audit` hook's signature")]
    pub(super) fn proxy_fanned_out(
        &self,
        _proxy: ProxyId,
        leader: NodeId,
        slot: Slot,
        ballot: Ballot,
        _vhash: u64,
        _column: Option<usize>,
        _addressees: usize,
    ) {
        // Every leader hint a round's fan-outs ever named: a second one is
        // a handoff successor's re-delegation refreshing it.
        self.state()
            .fanout_leaders
            .entry((slot.0, ballot.round, ballot.node.0))
            .or_default()
            .insert(leader.0);
    }

    pub(super) fn proxy_decided(&self, proxy: ProxyId, slot: Slot, ballot: Ballot, vhash: u64) {
        self.state()
            .observe_proxy_decision(proxy.0, slot.0, ballot, vhash);
    }

    pub(super) fn proxy_round_expired(&self, _proxy: ProxyId, _slot: Slot) {
        let mut st = self.state();
        reach_once!(
            st.proxy_round_expired,
            "proxy: a proxy evicts a round nobody answers"
        );
    }
}
