//! **Quorum reads** (#143, Compartmentalized Paxos §3.4, *Paxos Quorum
//! Reads*): a linearizable read that never touches the leader and never
//! reads a clock.
//!
//! A reader asks a **Phase-1** quorum of acceptors — one row of a grid, the
//! whole membership under a majority or a flexible split — for their *vote
//! watermarks* ([`crate::acceptor::Acceptor::vote_watermark`]: the highest
//! slot each has voted in), takes the **maximum**, and has any one replica
//! serve the read once that replica has applied that index
//! ([`crate::replica::Replica::covers`]). This module is the tally: a
//! [`QuorumRead`] folds the answers of one row and completes on
//! [`AcceptorConfig::has_phase1_quorum_in`]; [`QuorumReads`] holds every
//! read a node has open, bounded by a TTL. It reads no wire and knows no role — the node's
//! `node/quorum_reads.rs` is the wiring that feeds it and acts on its
//! answers, on **any** node: leader, follower, spare.
//!
//! # Safety, written down
//!
//! A write acked before the read began was chosen by a full Phase-2 quorum
//! `Q2` of the configuration in force at some slot `s`. The read's Phase-1
//! quorum `Q1` intersects `Q2` ([`crate::QuorumSystem::cross_intersects`] —
//! under a grid, a row meets every column in one cell), so some acceptor in
//! `Q1` voted `s` and its watermark is at least `s`; the maximum over `Q1`
//! is therefore at least `s`, and the replica serves only after applying
//! `s`. Every write acked before the read is thus visible to it, which is
//! the read half of linearizability (§3.5's case analysis); the write half
//! is the leader's, unchanged. **No clock anywhere**: the paper's read
//! leases (§9) assume clock synchrony, and this rung refuses exactly that.
//!
//! The liveness trade is the mirror image: a watermark raised by an accept
//! that never decided (a slot the leader started and abandoned) makes the
//! replica wait for the next leader's gap fill, and the read expires
//! [`Ambiguous`](crate::ColocatedNode::quorum_read) at the client. A
//! learner-only record raises the watermark too (paros records a chosen
//! value as the authoritative accepted record), which is conservative in the
//! same direction — a longer wait, never a stale answer.
//!
//! # One configuration at a time
//!
//! The argument above is made within **one configuration**: the row asked
//! and the column that chose the write are of the same grid. A read is
//! therefore bound to the configuration it was opened against (the tally
//! snapshots it, as a round records its column), and a node **abandons**
//! every open read the moment it learns a newer configuration — the row it
//! asked need not intersect the successor's columns. A `PreReadAck` also
//! carries the answering node's configuration ballot on a matchmaker
//! deployment, so a read whose row already knows of a successor abandons on
//! the first such answer rather than completing over a superseded belief
//! (under a majority or a flexible split every two Phase-1 quorums
//! intersect, so some row member always knows). What remains is a grid row
//! *wholly* unaware of a completed successor configuration: the reader
//! rediscovers it from the next `Prepare` or `Heartbeat`, and until then a
//! read it serves is judged by the client-history linearizability oracle —
//! the chain campaign's `QUORUM_READ` operation, served through the
//! driver's `QuorumRead` RPC (the sweep's finding, not this module's
//! claim).
//!
//! # What is deliberately not here (§3.6)
//!
//! Sequentially and eventually consistent reads are client-side watermark
//! bookkeeping over the same `Read` — a client that remembers the highest
//! index it has written or read and asks a replica for at least that — and
//! involve no acceptor at all. They are workload-only, judged by the client
//! history's linearizability search (#205), and are not built in the core.

use std::collections::{BTreeMap, BTreeSet};

use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Slot};

/// What folding a [`crate::Message::PreReadAck`] did to a read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreReadFold {
    /// No read open at that token, or the sender already answered.
    Ignored,
    /// The answer was counted toward the read's row.
    Counted,
    /// The answer named a configuration newer than the one the read was
    /// opened against: the read is abandoned, whole.
    Superseded,
}

/// Where one quorum read stands.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Stage {
    /// Collecting watermarks from the row.
    Tallying,
    /// The row answered: the read index is the maximum watermark, and the
    /// read waits for the replica to cover it.
    Confirmed {
        /// The maximum watermark over the row (`None`: nobody voted yet).
        index: Option<Slot>,
    },
}

/// One in-flight quorum read: the row it was addressed to, the
/// configuration it is judged over, and the watermarks folded so far.
#[derive(Clone, Debug)]
pub struct QuorumRead<Id> {
    /// The reader's correlation token.
    ctx: u64,
    /// The row this read was addressed to ([`AcceptorConfig::row_of`]);
    /// `None` when the quorum system names no row.
    row: Option<usize>,
    /// The configuration the read was opened against and is judged over —
    /// snapshotted, as a round records its column, so a belief that moves
    /// underneath the read cannot change what quorum it needs.
    config: AcceptorConfig<Id>,
    /// The ballot `config` was bound to at the reader; an answer naming a
    /// higher one supersedes the read.
    config_since: Ballot,
    /// The watermark each answering acceptor reported.
    watermarks: BTreeMap<Id, Option<Slot>>,
    /// Tick the read was opened on, for TTL garbage collection.
    created_tick: u64,
    stage: Stage,
}

impl<Id: Copy + Ord> QuorumRead<Id> {
    /// The reader's correlation token.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn ctx(&self) -> u64 {
        // A read's row is one its configuration has.
        assert!(
            self.config.admits_read_row(self.row),
            "a read asks a row of its configuration"
        );
        self.ctx
    }

    /// The row this read was addressed to (`None`: no row).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn row(&self) -> Option<usize> {
        assert!(
            self.config.admits_read_row(self.row),
            "a read asks a row of its configuration"
        );
        self.row
    }

    /// The configuration the read is judged over.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn config(&self) -> &AcceptorConfig<Id> {
        assert!(
            self.config.is_well_formed(),
            "a read is judged over a well-formed configuration"
        );
        &self.config
    }

    /// The acceptors that have answered, with the watermark each reported.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn watermarks(&self) -> &BTreeMap<Id, Option<Slot>> {
        // The row guard's read-back: every counted watermark is the row's.
        assert!(
            self.watermarks
                .keys()
                .all(|id| self.config.is_phase1_addressee(*id, self.row)),
            "every counted watermark comes from the read's row"
        );
        &self.watermarks
    }

    /// The read index, once the row answered: the maximum watermark over
    /// the quorum. `None` while still tallying.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn confirmed_index(&self) -> Option<Option<Slot>> {
        let confirmed = match self.stage {
            Stage::Tallying => None,
            Stage::Confirmed { index } => Some(index),
        };
        // A confirmed read's tally is frozen: its index is still the maximum
        // of the watermarks that confirmed it.
        if let Some(index) = confirmed {
            assert!(
                index == self.max_watermark(),
                "a confirmed index is the row's maximum"
            );
        }
        confirmed
    }

    /// The maximum watermark over the answers so far — what the read index
    /// becomes the moment the row is whole.
    fn max_watermark(&self) -> Option<Slot> {
        let max = self.watermarks.values().copied().flatten().max();
        assert!(
            self.watermarks.values().all(|w| *w <= max),
            "the read index covers every watermark the row reported"
        );
        max
    }
}

/// Every quorum read a node has open, in creation order (see the module
/// doc). Generic over the acceptor identity like every tally in the crate.
#[derive(Clone, Debug)]
pub struct QuorumReads<Id> {
    reads: Vec<QuorumRead<Id>>,
}

impl<Id> Default for QuorumReads<Id> {
    fn default() -> Self {
        let reads = Self { reads: Vec::new() };
        assert!(reads.reads.is_empty(), "a default tally holds no read");
        reads
    }
}

impl<Id: Copy + Ord> QuorumReads<Id> {
    /// No read open.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn new() -> Self {
        let reads = Self::default();
        assert!(reads.reads.is_empty(), "a fresh tally holds no read");
        reads
    }

    /// The open reads, in creation order.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn pending(&self) -> &[QuorumRead<Id>] {
        // Every token is opened at most once.
        assert!(
            self.reads
                .iter()
                .enumerate()
                .all(|(i, r)| self.reads[..i].iter().all(|o| o.ctx != r.ctx)),
            "a read token is open at most once"
        );
        &self.reads
    }

    /// Whether no read is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.reads.is_empty()
    }

    /// Open a read at `ctx` against `row` of `config` (bound to the reader
    /// at `config_since`) — the row [`AcceptorConfig::row_of`] derives for
    /// `ctx`, or one the caller chose ([`AcceptorConfig::read_row`]) — seeded
    /// with the reader's own watermark when it is itself an addressee of
    /// that row (`own: Some((me, watermark))`). Returns the addressees the
    /// caller sends `PreRead` to — the row minus the reader itself.
    ///
    /// # Panics
    ///
    /// If a read is already open at `ctx` (the driver's token is unique per
    /// read, so a second open at one token is a programmer error), or if
    /// `row` is not a row of `config`'s quorum system (a row under a
    /// majority or a flexible split, or one past a grid's last).
    pub fn open(
        &mut self,
        ctx: u64,
        row: Option<usize>,
        config: AcceptorConfig<Id>,
        config_since: Ballot,
        created_tick: u64,
        own: Option<(Id, Option<Slot>)>,
    ) -> Vec<Id> {
        assert!(
            self.reads.iter().all(|r| r.ctx != ctx),
            "a quorum read token is opened at most once"
        );
        assert!(
            config.admits_read_row(row),
            "a quorum read is opened against a row of its configuration"
        );
        let mut watermarks = BTreeMap::new();
        let mut addressees = config.phase1_addressees(row);
        if let Some((me, watermark)) = own {
            assert!(
                config.is_phase1_addressee(me, row),
                "a reader seeds its own watermark only as an addressee of the row"
            );
            watermarks.insert(me, watermark);
            addressees.retain(|id| *id != me);
        }
        let open = self.reads.len();
        self.reads.push(QuorumRead {
            ctx,
            row,
            config,
            config_since,
            watermarks,
            created_tick,
            stage: Stage::Tallying,
        });
        // The reader never addresses itself, and the read is tallying.
        if let Some((me, _)) = own {
            assert!(!addressees.contains(&me), "a reader never pre-reads itself");
        }
        assert!(
            self.reads.len() == open + 1,
            "opening adds exactly one read"
        );
        addressees
    }

    /// Fold `from`'s watermark into the read at `ctx`. The row guard is the
    /// tally's own, as the Phase-2 column guard is
    /// ([`crate::proposer::Proposer::fold_accepted_in`]): an answer from
    /// outside the read's row, over the configuration the read was opened
    /// against ([`AcceptorConfig::is_phase1_addressee`]), is ignored before
    /// anything else, so it can neither be counted nor abandon the read. A
    /// sender already counted is ignored. `config_since` is the
    /// configuration ballot the answer named (`None` on a plain
    /// deployment): one above the read's abandons it — the row asked need
    /// not intersect the successor's columns.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn fold(
        &mut self,
        ctx: u64,
        from: Id,
        watermark: Option<Slot>,
        config_since: Option<Ballot>,
    ) -> PreReadFold {
        let Some(position) = self.reads.iter().position(|r| r.ctx == ctx) else {
            return PreReadFold::Ignored;
        };
        if !self.reads[position]
            .config
            .is_phase1_addressee(from, self.reads[position].row)
        {
            return PreReadFold::Ignored;
        }
        if config_since.is_some_and(|since| since > self.reads[position].config_since) {
            self.reads.remove(position);
            return PreReadFold::Superseded;
        }
        let read = &mut self.reads[position];
        if read.stage != Stage::Tallying || read.watermarks.contains_key(&from) {
            return PreReadFold::Ignored;
        }
        read.watermarks.insert(from, watermark);
        assert!(
            read.watermarks.get(&from) == Some(&watermark),
            "a counted watermark is the one reported"
        );
        assert!(read.stage == Stage::Tallying, "only a tallying read counts");
        PreReadFold::Counted
    }

    /// Abandon every read opened against a configuration bound below
    /// `config_since`: the reader learned a newer configuration, and a read
    /// over the superseded one may never complete.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn abandon_superseded(&mut self, config_since: Ballot) {
        self.reads.retain(|r| r.config_since >= config_since);
        assert!(
            self.reads.iter().all(|r| r.config_since >= config_since),
            "no read opened under a superseded configuration survives"
        );
    }

    /// Advance every read: a tallying read whose row is whole (a Phase-1
    /// quorum of its configuration **in its row**) confirms at the maximum
    /// watermark, and a confirmed read whose index `covered` — the replica's
    /// answer, "applied at or past this index" — is served. Returns the
    /// served `(ctx, index)` pairs in creation order and drops them.
    ///
    /// # Panics
    ///
    /// If a vote behind a confirmation came from outside the read's row:
    /// [`QuorumReads::fold`]'s row guard refuses any other sender, restated
    /// here so the quorum predicate is never fed an id that is not one of
    /// the row's.
    pub fn serve(&mut self, covered: impl Fn(Option<Slot>) -> bool) -> Vec<(u64, Option<Slot>)> {
        let mut served = Vec::new();
        self.reads.retain_mut(|read| {
            if read.stage == Stage::Tallying {
                let answered: BTreeSet<Id> = read.watermarks.keys().copied().collect();
                if !read.config.has_phase1_quorum_in(&answered, read.row) {
                    return true;
                }
                assert!(
                    answered
                        .iter()
                        .all(|id| read.config.is_phase1_addressee(*id, read.row)),
                    "every watermark behind a quorum read comes from the read's row"
                );
                read.stage = Stage::Confirmed {
                    index: read.max_watermark(),
                };
            }
            let Stage::Confirmed { index } = read.stage else {
                unreachable!("a read past the tally is confirmed");
            };
            if !covered(index) {
                return true;
            }
            served.push((read.ctx, index));
            false
        });
        // A served read leaves the tally: it surfaces exactly once.
        assert!(
            served
                .iter()
                .all(|(ctx, _)| self.reads.iter().all(|r| r.ctx != *ctx)),
            "a served read is no longer pending"
        );
        served
    }

    /// Drop every read older than `ttl` ticks at `now` (a row that never
    /// answered whole, a watermark the replica never reached). Dropped
    /// silently: the read carries no
    /// durable obligation, and the driver owns the client reply.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn expire(&mut self, now: u64, ttl: u64) {
        self.reads
            .retain(|r| now.saturating_sub(r.created_tick) <= ttl);
        assert!(
            self.reads
                .iter()
                .all(|r| now.saturating_sub(r.created_tick) <= ttl),
            "no read outlives its window"
        );
        assert!(
            self.reads.iter().all(|r| r.created_tick <= now),
            "no read was opened in the future"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::QuorumSystem;
    use crate::types::NodeId;

    fn grid() -> AcceptorConfig {
        AcceptorConfig::new(
            (1..=6).map(NodeId).collect(),
            QuorumSystem::Grid { rows: 2, cols: 3 },
        )
    }

    fn ballot(round: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(0),
        }
    }

    /// The tally: a read addressed to row 1 (`ctx % 2`) completes on that
    /// row alone — a row that is not a column — at the maximum watermark,
    /// and is served only once the replica covers it.
    #[test]
    fn a_read_completes_on_its_row_at_the_maximum_watermark() {
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        // Node 5 is the reader, in row 1 = {4, 5, 6}: it seeds its own
        // watermark and asks the other two.
        let addressees = reads.open(
            1,
            Some(1),
            grid(),
            ballot(0),
            0,
            Some((NodeId(5), Some(Slot(2)))),
        );
        assert_eq!(addressees, vec![NodeId(4), NodeId(6)]);
        assert_eq!(reads.pending()[0].row(), Some(1));
        assert!(reads.serve(|_| true).is_empty(), "one answer is no row");
        assert_eq!(
            reads.fold(1, NodeId(4), Some(Slot(3)), None),
            PreReadFold::Counted
        );
        assert_eq!(
            reads.fold(1, NodeId(4), Some(Slot(9)), None),
            PreReadFold::Ignored,
            "an acceptor answers once"
        );
        assert_eq!(
            reads.fold(2, NodeId(6), Some(Slot(3)), None),
            PreReadFold::Ignored,
            "no read at that token"
        );
        assert!(reads.serve(|_| true).is_empty(), "two of three is no row");
        assert_eq!(
            reads.fold(1, NodeId(6), None, None),
            PreReadFold::Counted,
            "a never-voted acceptor still answers"
        );
        // The row is whole: confirmed at max(2, 3, nothing) = 3, but the
        // replica has not applied slot 3 yet.
        assert!(reads.serve(|index| index <= Some(Slot(2))).is_empty());
        assert_eq!(reads.pending()[0].confirmed_index(), Some(Some(Slot(3))));
        assert_eq!(
            reads.fold(1, NodeId(5), Some(Slot(7)), None),
            PreReadFold::Ignored,
            "a confirmed read folds nothing more"
        );
        assert_eq!(
            reads.serve(|index| index <= Some(Slot(3))),
            vec![(1, Some(Slot(3)))]
        );
        assert!(reads.is_empty());
    }

    /// The row guard is the tally's: an answer from outside the read's row
    /// is never counted, and a newer configuration it names abandons
    /// nothing — so a caller that skips its own guard cannot feed `serve` a
    /// watermark from outside the row.
    #[test]
    fn an_answer_from_outside_the_row_is_ignored() {
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        // Row 1 = {4, 5, 6}; node 1 is in row 0.
        let _ = reads.open(1, Some(1), grid(), ballot(1), 0, None);
        assert_eq!(
            reads.fold(1, NodeId(1), Some(Slot(9)), None),
            PreReadFold::Ignored,
            "a row-0 acceptor is not an addressee of row 1"
        );
        assert_eq!(
            reads.fold(1, NodeId(1), Some(Slot(9)), Some(ballot(2))),
            PreReadFold::Ignored,
            "an answer from outside the row abandons nothing"
        );
        assert_eq!(reads.pending().len(), 1);
        assert!(reads.pending()[0].watermarks().is_empty());
        for id in [4, 5, 6] {
            assert_eq!(
                reads.fold(1, NodeId(id), Some(Slot(2)), None),
                PreReadFold::Counted
            );
        }
        assert_eq!(reads.serve(|_| true), vec![(1, Some(Slot(2)))]);
    }

    /// A row of acceptors that never voted confirms at the empty index,
    /// which any replica covers — the read of an empty log.
    #[test]
    fn an_unvoted_row_confirms_at_the_empty_index() {
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        let majority = AcceptorConfig::new((0..3).map(NodeId).collect(), QuorumSystem::Majority);
        let addressees = reads.open(4, None, majority, ballot(0), 0, Some((NodeId(0), None)));
        assert_eq!(addressees, vec![NodeId(1), NodeId(2)]);
        assert_eq!(reads.fold(4, NodeId(1), None, None), PreReadFold::Counted);
        assert_eq!(reads.serve(|index| index.is_none()), vec![(4, None)]);
    }

    /// A configuration that moves — at the reader, or reported by a row
    /// member — abandons the read: the row asked need not intersect the
    /// successor's columns.
    #[test]
    fn a_newer_configuration_abandons_the_read() {
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        let _ = reads.open(0, Some(0), grid(), ballot(1), 0, None);
        assert_eq!(
            reads.fold(0, NodeId(1), Some(Slot(1)), Some(ballot(1))),
            PreReadFold::Counted,
            "the read's own configuration ballot is not newer"
        );
        assert_eq!(
            reads.fold(0, NodeId(2), Some(Slot(1)), Some(ballot(2))),
            PreReadFold::Superseded
        );
        assert!(reads.is_empty());
        let _ = reads.open(2, Some(0), grid(), ballot(1), 0, None);
        let _ = reads.open(3, Some(1), grid(), ballot(3), 0, None);
        reads.abandon_superseded(ballot(3));
        assert_eq!(reads.pending().len(), 1);
        assert_eq!(reads.pending()[0].ctx(), 3);
    }

    /// The TTL: a read that never completes is dropped, and a read that
    /// completed but whose index the replica never reached is dropped too.
    #[test]
    fn a_read_expires_by_ttl() {
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        let _ = reads.open(0, Some(0), grid(), ballot(0), 5, None);
        let _ = reads.open(1, Some(1), grid(), ballot(0), 9, None);
        reads.expire(15, 8);
        assert_eq!(reads.pending().len(), 1);
        assert_eq!(reads.pending()[0].ctx(), 1);
        reads.expire(30, 8);
        assert!(reads.is_empty());
    }

    #[test]
    #[should_panic(expected = "a quorum read token is opened at most once")]
    fn a_token_is_opened_at_most_once() {
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        let _ = reads.open(0, Some(0), grid(), ballot(0), 0, None);
        let _ = reads.open(0, Some(0), grid(), ballot(0), 0, None);
    }

    /// A named row: any row of the grid is honoured, a row past the last
    /// or one under a majority falls back to `ctx % rows` / the whole
    /// membership (`read_row`), and the tally refuses a row its
    /// configuration does not have.
    #[test]
    fn a_named_read_row_is_honoured_or_falls_back() {
        let grid = grid();
        assert_eq!(
            grid.read_row(4, Some(1)),
            Some(1),
            "a named row is honoured"
        );
        assert_eq!(
            grid.read_row(4, None),
            Some(0),
            "the default row is ctx % rows"
        );
        assert_eq!(
            grid.read_row(5, Some(2)),
            Some(1),
            "a row past the last falls back"
        );
        let majority = AcceptorConfig::new((1..=3).map(NodeId).collect(), QuorumSystem::Majority);
        assert_eq!(
            majority.read_row(4, Some(0)),
            None,
            "a majority names no row"
        );
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        // Row 1 of the 2x3 grid is {4, 5, 6}, although ctx 4 defaults to row 0.
        let addressees = reads.open(4, Some(1), grid, ballot(0), 0, None);
        assert_eq!(addressees, vec![NodeId(4), NodeId(5), NodeId(6)]);
    }

    #[test]
    #[should_panic(expected = "a quorum read is opened against a row of its configuration")]
    fn a_row_the_configuration_lacks_is_refused() {
        let mut reads: QuorumReads<NodeId> = QuorumReads::new();
        let _ = reads.open(0, Some(2), grid(), ballot(0), 0, None);
    }
}
