//! The network's reports: drops, duplicates, failed deliveries and edge rejections.

use moonpool_sim::{TimeProvider, assert_reachable};
use paros::core::contract::Party;
use paros::core::{Message, NodeId};
use paros::net::EdgeRejection;

use super::NodeAudit;

impl<T: TimeProvider> NodeAudit<T> {
    pub(super) fn dropped_at_send(&self, _from: Party, _to: Party, msg: &Message) {
        let mut st = self.state();
        match msg {
            Message::Accept { .. } => {
                reach_once!(
                    st.dropped_accept,
                    "the driver drops one isolated accept at the send seam"
                );
            }
            Message::Prepare { .. } | Message::Promise { .. } | Message::Nack { .. } => {
                reach_once!(
                    st.dropped_election,
                    "the driver drops an election message at the send seam"
                );
            }
            Message::Commit { .. } => {
                reach_once!(
                    st.dropped_commit,
                    "the driver drops a commit at the send seam"
                );
            }
            Message::Accepted { .. } => {
                reach_once!(
                    st.dropped_accepted,
                    "the driver drops an accepted ack at the send seam"
                );
            }
            Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => {
                reach_once!(
                    st.dropped_heartbeat,
                    "the driver drops a heartbeat at the send seam"
                );
            }
            Message::TrimmedTo { .. } | Message::CatchUpResponse { .. } => {
                reach_once!(
                    st.dropped_repair,
                    "the driver drops a repair message at the send seam"
                );
            }
            Message::CatchUpRequest { .. } => {
                reach_once!(
                    st.dropped_catchup_request,
                    "the driver drops a catch-up request at the send seam"
                );
            }
            // The whole cooperative handoff, lost in one message: the outgoing
            // leader has already stepped down and the successor never starts,
            // so this must cost availability only — an ordinary Phase 1 is the
            // documented fallback, and the liveness checks are what prove it.
            Message::Relinquish { .. } => {
                reach_once!(
                    st.dropped_relinquish,
                    "the driver drops a relinquishment at the send seam"
                );
            }
            _ => {}
        }
    }

    pub(super) fn duplicated_at_send(&self, _from: Party, _to: Party, msg: &Message) {
        let mut st = self.state();
        reach_once!(
            st.duplicated_any,
            "the driver duplicates a message at the send seam"
        );
        // The quorum-counting kinds are the point of the location: a
        // duplicate of one of these must never fabricate a quorum.
        if matches!(
            msg,
            Message::Promise { .. } | Message::Accepted { .. } | Message::HeartbeatAck { .. }
        ) {
            reach_once!(
                st.duplicated_quorum_kind,
                "the driver duplicates a quorum-counting message at the send seam"
            );
        }
        // The other kinds (a commit, the repair and snap-repair planes, a
        // catch-up request) share the family gate above: every one of them
        // is an idempotency claim the `always` checks judge, and the hook
        // keeps one location per kind regardless. A re-delivered handoff
        // keeps its own — it must be a no-op at its addressee, never an
        // allocator rewind, and refused everywhere else; the uniqueness
        // oracle above is what keeps that honest.
        if matches!(msg, Message::Relinquish { .. }) {
            reach_once!(
                st.duplicated_relinquish,
                "the driver duplicates a relinquishment at the send seam"
            );
        }
    }

    pub(super) fn client_reply_dropped(&self, _node: NodeId, reply: paros::observe::reply::Reply) {
        let mut st = self.state();
        if matches!(
            reply,
            paros::observe::reply::Reply::Redirect
                | paros::observe::reply::Reply::ReadUnserved
                | paros::observe::reply::Reply::Truncate
                | paros::observe::reply::Reply::Reconfigure
                | paros::observe::reply::Reply::ReconfigureMatchmakers
                | paros::observe::reply::Reply::Retire
        ) {
            // Nothing committed behind these: the client sees a deadline and
            // retries blind. No dedup edge to track, only the reach.
            reach_once!(
                st.redirect_dropped,
                "a redirect or compaction reply is dropped at the reply seam"
            );
            return;
        }
        reach_once!(
            st.reply_dropped,
            "a committed client reply is dropped at the reply seam"
        );
        if matches!(reply, paros::observe::reply::Reply::Write) {
            st.propose_reply_dropped = true;
        }
        if matches!(reply, paros::observe::reply::Reply::LogRead) {
            reach_once!(
                st.read_reply_dropped,
                "a confirmed read reply is dropped at the reply seam"
            );
        }
    }

    pub(super) fn dropped_at_mailbox(&self, _from: Party, _to: Party, _kind: &'static str) {
        let mut st = self.state();
        reach_once!(st.mailbox_dropped, "mailbox overflow dropped a message");
    }

    pub(super) fn delivery_failed(&self, _from: Party, _to: Party) {
        let mut st = self.state();
        st.delivery_failures += 1;
        reach_once!(st.delivery_failed, "a peer delivery RPC fails or times out");
    }

    pub(super) fn edge_rejected(&self, _at: Party, kind: EdgeRejection) {
        let mut st = self.state();
        if kind == EdgeRejection::ForeignCell {
            // Another cell's peer, not a corrupted request (#216): its gate
            // is the edge's own inline reachable.
            return;
        }
        // A corrupted request needs a damaged frame, and the campaign masks
        // `BitFlip` (wire integrity is the transport's): no gate, the count
        // stays for the run's diagnostics (#446 (a gate that never fires
        // cannot fail the sweep)).
        st.edge_rejections += 1;
    }
}
