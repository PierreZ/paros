//! The node's **quorum-read wiring** (#143, Compartmentalized Paxos §3.4):
//! how a `PreRead` on the wire reaches the [`Acceptor`]'s vote watermark,
//! how a `PreReadAck` reaches the [`QuorumReads`] tally, and how a completed
//! read waits on the [`Replica`]'s "applied at or past this index" before it
//! surfaces through the same [`Ready::read_states`] the leader's read-index
//! path uses (that shared back half — the window, the serving, the
//! surfacing — is `node/reads.rs`). The components decide; this module
//! builds the messages and keeps the reads consistent with the
//! configuration the node believes in.
//!
//! Every node runs it — leader, follower, spare — and no path here touches
//! the leader's authority, its beats or its acks: the read-index path is
//! untouched and a plain deployment's `Heartbeat` / `HeartbeatAck` are
//! byte-for-byte what they were.
//!
//! [`Acceptor`]: crate::acceptor::Acceptor
//! [`Replica`]: crate::replica::Replica
//! [`Ready::read_states`]: crate::Ready::read_states
//! [`QuorumReads`]: crate::quorum_read::QuorumReads

use super::{ColocatedNode, Message, NodeId, NodeRole, Slot};
use crate::quorum_read::PreReadFold;
use crate::types::Ballot;
use crate::{Command, Control, Delegation, ProposeResult};

impl ColocatedNode {
    /// **Leaderless read** entry point, on any node (#143, Compartmentalized
    /// Paxos §3.4): ask the row [`AcceptorConfig::row_of`] derives for `ctx`
    /// — under the configuration this node believes in force — for their
    /// vote watermarks, settle on the maximum once the row answered whole,
    /// and surface a [`ReadState`](super::ReadState) carrying `ctx` through
    /// [`Ready::read_states`] once this node's chosen prefix covers it
    /// ([`Replica::covers`]). The driver's "wait until applied, then serve"
    /// path is then exactly the read-index one.
    ///
    /// **No clock anywhere.** The read is linearizable by quorum
    /// intersection alone (the argument is on [`crate::quorum_read`]): every
    /// write acked before the read began was chosen by a Phase-2 quorum that
    /// the read's Phase-1 quorum meets, so the maximum watermark is at or
    /// past it. A read that cannot complete — a row that never answers
    /// whole, a watermark the replica never reaches, a configuration that
    /// moves underneath it — surfaces nothing and is dropped after
    /// `READ_TTL_TICKS` (the window both read tallies share, `node/reads.rs`);
    /// the driver owns the client-facing timeout, and the client records it
    /// *ambiguous*, never aborted.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition): a driver token is opened at most once.
    ///
    /// [`AcceptorConfig::row_of`]: crate::AcceptorConfig::row_of
    /// [`Ready::read_states`]: crate::Ready::read_states
    /// [`Replica::covers`]: crate::replica::Replica::covers
    pub fn quorum_read(&mut self, ctx: u64) {
        self.quorum_read_in(ctx, None);
    }

    /// [`ColocatedNode::quorum_read`] with the **row named by the caller**
    /// (`row: Some(r)`), where `quorum_read` takes the default
    /// [`AcceptorConfig::row_of`] — the Phase-1 twin of
    /// [`ColocatedNode::propose_in`]'s column. Every row of a grid is a
    /// Phase-1 quorum that meets every column, so any row is a valid choice;
    /// a row the configuration in force does not have (any row under a
    /// majority or a flexible split, one past a grid's last) falls back to
    /// the default ([`AcceptorConfig::read_row`]). Which row to ask is the
    /// driver's rare-but-valid decision, never the core's.
    ///
    /// # Panics
    ///
    /// As [`ColocatedNode::quorum_read`].
    ///
    /// [`AcceptorConfig::row_of`]: crate::AcceptorConfig::row_of
    /// [`AcceptorConfig::read_row`]: crate::AcceptorConfig::read_row
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, ctx)))]
    pub fn quorum_read_in(&mut self, ctx: u64, row: Option<usize>) {
        let me = self.config.id;
        // The reader is its own first answer when it sits in the row: its
        // watermark is a fact its durable log holds, exactly what a peer's
        // ack would claim.
        let row = self.acceptors.read_row(ctx, row);
        let own = self
            .acceptors
            .is_phase1_addressee(me, row)
            .then(|| (me, self.acceptor.vote_watermark()));
        let addressees = self.quorum_reads.open(
            ctx,
            row,
            self.acceptors.clone(),
            self.acceptors_since,
            self.tick_count,
            own,
        );
        for to in addressees {
            self.send(to, Message::PreRead { reply_to: me, ctx });
        }
        // A one-node row (a single-node cluster) is its own quorum: serve in
        // this same batch.
        self.serve_quorum_reads();
        self.assert_invariants();
    }

    /// Acceptor: answer a `PreRead` with this node's vote watermark. No
    /// durable write and no promise moves — the ack claims only "I have
    /// voted this high", which the durable log already holds. Answered by
    /// every pooled node, member of the reader's configuration or not (the
    /// reader's tally counts only its row; acceptor guards are pool-based),
    /// to a pooled reader or a replica (#144: a replica reads the same way).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = reply_to.0, ctx)))]
    pub(super) fn on_pre_read(&mut self, reply_to: NodeId, ctx: u64) {
        // Wire hygiene: an ack never answers an arbitrary id — a node of the
        // pool, or, on a deployment with a replica tier, a replica (its id is
        // outside the pool by construction; the driver routes it through the
        // deployment map and drops an address it does not know).
        if !self.in_pool(reply_to) && !self.config.has_replicas() {
            return;
        }
        let writes_at_entry = self.pending_writes.len();
        let config_since = self.wire_config_since();
        self.send(
            reply_to,
            Message::PreReadAck {
                from: self.config.id,
                ctx,
                watermark: self.acceptor.vote_watermark(),
                config_since,
            },
        );
        // Negative space: a watermark answer is a pure reply.
        assert!(
            self.pending_writes.len() == writes_at_entry,
            "a pre-read answer queues no durable write"
        );
    }

    /// Reader: fold an acceptor's watermark into the read at `ctx`, and
    /// serve whatever that completes.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, ctx)))]
    pub(super) fn on_pre_read_ack(
        &mut self,
        from: NodeId,
        ctx: u64,
        watermark: Option<Slot>,
        config_since: Option<Ballot>,
    ) {
        // Wire hygiene, then the row guard: the tally counts only the
        // addressees of the read's row, over the configuration the read was
        // opened against (a stray answer from outside it is never folded —
        // the mirror of `on_accepted`'s column guard).
        if !self.in_pool(from) {
            return;
        }
        self.fill_to_watermark(watermark);
        let Some(read) = self.quorum_reads.get(ctx) else {
            return;
        };
        if !read.config().is_phase1_addressee(from, read.row()) {
            return;
        }
        match self.quorum_reads.fold(ctx, from, watermark, config_since) {
            PreReadFold::Ignored | PreReadFold::Superseded => {}
            PreReadFold::Counted => self.serve_quorum_reads(),
        }
    }

    /// Leader: an acceptor voted at `watermark`, at or past this leader's
    /// allocator frontier — a vote from an earlier ballot its Phase-1 quorum
    /// did not include. Every quorum read that meets that acceptor waits for
    /// the slot to be chosen (#204: every `Read` is a quorum read), and on an
    /// idle log nothing else ever proposes there: the clients' claims start
    /// with a read, so reads and writes would wait on each other forever.
    /// A settled leader therefore proposes a [`Control::Noop`] into every
    /// slot up to the watermark. That is an ordinary proposal at the
    /// frontier — exactly what [`ColocatedNode::propose_control`] opens, and
    /// safe for the same reason: the Phase 1 this ballot completed covered
    /// every slot past the frontier and reported nothing chosen there. An
    /// unsettled leader leaves it to its recovery; the read retries.
    ///
    /// Red→green: hunt seed 9253735150370401629 (acceptors 1 and 4 held
    /// votes at slots 4–7 outside the leader's promise quorum; no read that
    /// asked either of them was ever served, no claim was ever sent, and the
    /// run ended with four slots chosen).
    ///
    /// [`Control::Noop`]: crate::Control::Noop
    fn fill_to_watermark(&mut self, watermark: Option<Slot>) {
        let Some(watermark) = watermark else {
            return;
        };
        if self.role != NodeRole::Leader
            || !self.leadership_settled()
            || watermark < self.proposer.next_slot()
        {
            return;
        }
        while self.proposer.next_slot() <= watermark {
            let filled =
                self.open_proposal(None, Delegation::Auto, Command::Control(Control::Noop));
            assert!(
                matches!(filled, ProposeResult::Accepted(_)),
                "a leader's fill opens a proposal"
            );
            self.counters.watermark_fills += 1;
        }
        assert!(
            self.proposer.next_slot() > watermark,
            "a watermark fill leaves the frontier past the watermark"
        );
    }
}
