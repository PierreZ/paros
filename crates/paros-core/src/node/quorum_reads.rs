//! The node's **read wiring** (#143, Compartmentalized Paxos §3.4): how a
//! `PreRead` on the wire reaches the [`Acceptor`]'s vote watermark, how a
//! `PreReadAck` reaches the [`QuorumReads`] tally, and how a completed read
//! waits on the [`Replica`]'s "applied at or past this index" before it
//! surfaces through [`Ready::read_states`]. The components decide; this
//! module builds the messages and keeps the reads consistent with the
//! configuration the node believes in.
//!
//! Every node runs it — leader, follower, spare — and no path here touches
//! the leader's authority, its beats or its acks. It is the only read path:
//! the leader's read-index rounds retired (#243).
//!
//! [`Acceptor`]: crate::acceptor::Acceptor
//! [`Replica`]: crate::replica::Replica
//! [`Ready::read_states`]: crate::Ready::read_states
//! [`QuorumReads`]: crate::quorum_read::QuorumReads

use super::{BeliefSource, ColocatedNode, Message, NodeId, NodeRole, ReadState, Slot};
use crate::membership::QuorumSystem;
use crate::quorum_read::PreReadFold;
use crate::types::Ballot;
use crate::{Command, Control, Delegation, ProposeResult};

/// Ticks a pending quorum read — waiting for its row to answer whole, or for
/// the replica to cover the index it settled on — may wait before the node
/// garbage-collects it (lost acks, an unreachable row). Dropped silently: a
/// read carries no durable obligation, and the driver owns the client reply
/// (its retry sweep answers first, well inside this window). A watermark
/// raised by an accept that never decided needs the next leader's gap fill
/// to be covered, which is why the window is not shorter than an election.
pub(crate) const READ_TTL_TICKS: u64 = 20;

// A read must outlive the tick it opened in, or no row could ever answer it.
const _: () = assert!(READ_TTL_TICKS > 0);

impl ColocatedNode {
    /// **Leaderless read** entry point, on any node (#143, Compartmentalized
    /// Paxos §3.4): ask the row [`AcceptorConfig::row_of`] derives for `ctx`
    /// — under the configuration this node believes in force — for their
    /// vote watermarks, settle on the maximum once the row answered whole,
    /// and surface a [`ReadState`](super::ReadState) carrying `ctx` through
    /// [`Ready::read_states`] once this node's chosen prefix covers it
    /// ([`Replica::covers`]).
    ///
    /// **No clock anywhere.** The read is linearizable by quorum
    /// intersection alone (the argument is on [`crate::quorum_read`]): every
    /// write acked before the read began was chosen by a Phase-2 quorum that
    /// the read's Phase-1 quorum meets, so the maximum watermark is at or
    /// past it. A read that cannot complete — a row that never answers
    /// whole, a watermark the replica never reaches, a configuration that
    /// moves underneath it — surfaces nothing and is dropped after
    /// `READ_TTL_TICKS`;
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
        let marks = self.durable_marks();
        // No basis, no read (#260): the read opens nothing, and the driver's
        // retry sweep answers it unserved — the client asks again, here or
        // elsewhere, once a won leadership's beat has been heard.
        let Some(basis) = self.read_basis() else {
            probe!(
                reachable,
                "quorum read: a node without a read basis opens no read"
            );
            self.counters.quorum_reads_without_basis += 1;
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment reads without a basis"
            );
            self.assert_marks_monotone(marks);
            self.assert_invariants();
            return;
        };
        // The reader is its own first answer when it sits in the row: its
        // watermark is a fact its durable log holds, exactly what a peer's
        // ack would claim.
        let row = basis.config.read_row(ctx, row);
        // A grid reads one row; a majority or a flexible split the whole
        // membership (#269).
        assert!(
            row.is_some() == matches!(basis.config.quorum_system(), QuorumSystem::Grid { .. }),
            "a read names a row exactly under a grid"
        );
        let own = basis
            .config
            .is_phase1_addressee(me, row)
            .then(|| (me, self.acceptor.vote_watermark()));
        // Read from the addressee list, apart from the predicate above.
        assert!(
            own.is_none() || basis.config.phase1_addressees(row).contains(&me),
            "a reader answers itself only from inside its row"
        );
        let config = basis.config.clone();
        let addressees = self
            .quorum_reads
            .open(ctx, row, basis, self.tick_count, own);
        // The row's other members, and nobody outside the configuration.
        assert!(
            !addressees.contains(&me),
            "a reader never pre-reads itself over the wire"
        );
        assert!(
            addressees.iter().all(|to| config.contains(*to)),
            "a pre-read addresses only members of the configuration"
        );
        for to in addressees {
            self.send(to, Message::PreRead { reply_to: me, ctx });
        }
        // A one-node row (a single-node cluster) is its own quorum: serve in
        // this same batch.
        self.serve_quorum_reads();
        // A read touches no durable state.
        self.assert_marks_monotone(marks);
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
        // An answer carries this node's configuration ballot, which a reader
        // reads as "no campaign above the read's basis reached me" (#260).
        // A rebooted node's belief is the bootstrap default bound to no
        // ballot, whatever it promised before the crash: it answers nothing
        // until its membership probe (or a leader) has told it what is in
        // force, or a read could complete over a promise it forgot.
        if self.config.has_matchmakers() && self.belief_source == BeliefSource::Bootstrap {
            probe!(
                reachable,
                "quorum read: a rebooted node answers no pre-read before it heard"
            );
            self.counters.pre_reads_refused_unheard += 1;
            // Negative space: an unheard belief is bound to no ballot, the
            // very thing an answer would have misreported.
            assert!(
                self.acceptors_since == crate::types::Ballot::zero(),
                "an unheard belief is bound to no ballot"
            );
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
        // Wire hygiene; the row guard is the tally's own (`QuorumReads::fold`
        // ignores an answer from outside the read's row, the mirror of
        // `on_accepted`'s column guard).
        if !self.in_pool(from) {
            return;
        }
        self.fill_to_watermark(watermark);
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

    /// Hand the quorum-read tally the replica's answer and queue every read
    /// it serves: a read confirmed at `index` surfaces once the chosen
    /// prefix covers it. Called wherever the answer can change: an ack
    /// folded, the chosen prefix advanced, a tick.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn serve_quorum_reads(&mut self) {
        let replica = &self.replica;
        let served = self.quorum_reads.serve(|index| replica.covers(index));
        // The apply condition: a read surfaces only once the fold covers the
        // index its row reported.
        assert!(
            served.iter().all(|(_, index)| self.replica.covers(*index)),
            "a served read's index is covered by the fold"
        );
        let queued = self.pending_read_states.len();
        let count = served.len();
        self.pending_read_states.extend(
            served
                .into_iter()
                .map(|(ctx, index)| ReadState { ctx, index }),
        );
        assert!(
            self.pending_read_states.len() == queued + count,
            "every served read surfaces once"
        );
    }

    /// Per-tick upkeep: drop the reads that outlived the node's read window
    /// (at least [`READ_TTL_TICKS`], #386), then serve what the prefix may
    /// have covered since.
    pub(super) fn tick_quorum_reads(&mut self) {
        let now = self.tick_count;
        self.quorum_reads.expire(now, self.read_window());
        self.serve_quorum_reads();
    }
}
