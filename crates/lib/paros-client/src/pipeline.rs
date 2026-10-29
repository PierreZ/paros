//! Several writes in flight on one single-writer journal (#597 (several
//! writes in flight per journal)): one [`Writer`]'s leadership, shared by
//! the callers that write through it, each write sent without waiting for
//! the answer of the write before it.
//!
//! A `Write` names its position (`docs/architecture/02-data-plane.md` §2.2), and the
//! journal judges it at apply: accepted only at `next_seq`. A leader that
//! receives the writes in order puts them in consecutive slots, and every
//! one is accepted. The pipeline keeps the order and at-most-once rules of
//! a single-writer journal with three rules:
//!
//! - **Consecutive positions.** [`Pipeline::open`] gives each write the
//!   position after the last one it gave, under the uuid the writer leads
//!   with. It never gives a position twice while a write at it is in
//!   flight under that uuid.
//! - **A write that arrived early waits, then is sent again unchanged.** A
//!   write that reached the leader before the write in front of it is
//!   refused, and the view names a `next_seq` below its position under its
//!   own uuid. It waits until every write in front of it settled. When
//!   the journal's tail reached its position, it sends the identical write
//!   again (a retry is the same write; the log answers it). When a write in
//!   front of it failed, its position is never reached: it is not written
//!   (its only verdict was a refusal), and it returns that refusal.
//! - **A failure stops the pipeline until it drains.** A write that is not
//!   written (refused, fenced, ambiguous, abandoned) leaves a hole that
//!   every later position depends on. No new write opens until the writes
//!   in flight settle; then the next one opens at the tail the writer
//!   knows. A claim that wins a new term opens at the tail its view names at
//!   once: the old term's writes in flight are fenced or already counted.
//! - **Writes sent together merge into one write** (#709 (merge fenced
//!   writes)). A send waits one turn of the executor, so the sends beside
//!   it get ready too. Then the first ready write takes the ready writes at
//!   the positions after it, under the same uuid, into one write at its own
//!   position: one slot, not one slot per write. The journal judges the
//!   merged write as one write, and each write gets its own range back. A
//!   verdict that is not a write (a refusal, a fence, an ambiguity) is the
//!   verdict of every write it holds. A merged write over a leader's limits
//!   (`TooLarge`) comes apart: each write is sent alone, and never merged
//!   again. The merge is here, not in a batcher: the log deduplicates a
//!   re-send only when it is the identical write, and only the pipeline
//!   re-sends the merged write unchanged (an early re-send, or
//!   [`Client::resolve`] of an ambiguous one). A batcher that merged
//!   again a write a caller re-sent alone would make the journal refuse a
//!   write it holds.
//!
//! The library draws no randomness and sends nothing on its own: each write
//! is the caller's [`Pipeline::send`], and the first server is the
//! caller's choice, as for [`Writer::write`]. [`Lanes`] is the pure state;
//! the unit tests pin it.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use paros_core::Value;
use paros_core::journal::{Entry, JournalIdentifier, JournalView, LeaderUuid, Seq};
use paros_net::runtime::ParosProviders;
use tokio::sync::watch;

use super::Client;
use super::outcome::{ClaimOutcome, TruncateOutcome};
use super::writer::{Writer, WriterOutcome};

/// One journal's writer, shared by every write in flight on it. `Send` and
/// `Sync`: callers share it by reference or in an `Arc`.
pub struct Pipeline {
    lanes: Mutex<Lanes>,
    /// Bumped after every change a waiting write may care about.
    settled: watch::Sender<u64>,
}

/// A write [`Pipeline::open`] gave a position: send it with
/// [`Pipeline::send`]. Every opened ticket must be sent: an unsent one holds
/// its position, and after a failure the pipeline waits for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ticket {
    id: u64,
    entry: Entry,
}

impl Ticket {
    /// The write: the uuid, the position and the records it carries.
    #[must_use]
    pub fn entry(&self) -> &Entry {
        &self.entry
    }
}

/// What [`Pipeline::send`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pipelined {
    /// The write's outcome, as [`Writer::write_entry`] gives it.
    pub outcome: WriterOutcome,
    /// The write arrived before the write in front of it, waited, and was
    /// sent again unchanged.
    pub resent: bool,
    /// The write went out merged with the writes beside it, in one write
    /// (#709 (merge fenced writes)).
    pub merged: bool,
}

/// The most records one merged write carries (#709 (merge fenced
/// writes)): a quarter of a leader's default `max_batch_records`. A leader
/// with lower limits answers `TooLarge`, and the writes go apart.
pub const MERGE_RECORDS: usize = 256;

/// The most record bytes one merged write carries: a quarter of a leader's
/// default `max_batch_bytes`.
pub const MERGE_BYTES: usize = 256 * 1024;

const _: () = assert!(MERGE_RECORDS >= 2 && MERGE_BYTES >= 1);

/// One turn of the executor: pending once, woken at once. The sends
/// polled in the same turn get ready before the first one leads.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Pipeline {
    /// A pipeline over `writer`'s leadership.
    #[must_use]
    pub fn new(writer: Writer) -> Self {
        Self {
            lanes: Mutex::new(Lanes::new(writer)),
            settled: watch::Sender::new(0),
        }
    }

    fn lanes(&self) -> MutexGuard<'_, Lanes> {
        self.lanes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Tell every waiting write that something settled.
    fn wake(&self) {
        self.settled
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    /// The writer's belief now: its uuid, its leadership, the tail it
    /// knows. A copy.
    #[must_use]
    pub fn writer(&self) -> Writer {
        self.lanes().writer
    }

    /// The journal it writes.
    #[must_use]
    pub fn journal(&self) -> JournalIdentifier {
        self.writer().journal()
    }

    /// The uuid it believes leads the journal, when it believes it leads.
    #[must_use]
    pub fn owned(&self) -> Option<LeaderUuid> {
        self.writer().owned()
    }

    /// How many writes are in flight now.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.lanes().flight.len()
    }

    /// Give the next write its position: `build` makes its records from the
    /// position. `None` when the writer leads no term (claim first). After
    /// a failure it waits until the writes in flight settle.
    pub async fn open(&self, build: impl FnOnce(u64) -> Vec<Value>) -> Option<Ticket> {
        loop {
            let mut settled = self.settled.subscribe();
            {
                let mut lanes = self.lanes();
                match lanes.admit() {
                    Admit::At(seq) => return Some(lanes.open(build(seq))),
                    Admit::NotOwner => return None,
                    Admit::Drain => {}
                }
            }
            // The sender lives in `self`: `changed` returns when a write
            // settles.
            if settled.changed().await.is_err() {
                return None;
            }
        }
    }

    /// Send `ticket` to the believed leader (or `first`), following
    /// redirects and settling an ambiguous answer ([`Writer::write_entry`]).
    /// A write that arrived before the write in front of it waits for it,
    /// then is sent again unchanged, or returns its refusal when that write
    /// failed. The verdict is folded into the shared writer.
    ///
    /// # Panics
    ///
    /// If `ticket` is not in flight on this pipeline (sent twice, or
    /// opened on another one).
    #[tracing::instrument(level = "debug", skip_all, fields(seq = ticket.entry.seq.0, records = ticket.entry.count()))]
    pub async fn send<P: ParosProviders>(
        &self,
        client: &Client<P>,
        ticket: Ticket,
        first: usize,
    ) -> Pipelined {
        // First, before any await: a send dropped mid-flight still settles
        // its ticket.
        let mut guard = Abandon {
            pipeline: self,
            id: Some(ticket.id),
        };
        assert!(
            self.lanes().holds(ticket.id),
            "a ticket is sent while it is in flight"
        );
        self.lanes().ready(ticket.id);
        // The sends beside it get ready in this turn (#709 (merge fenced
        // writes)).
        YieldOnce(false).await;
        let mut resent = false;
        // Lead a write, or wait for the merged write that carries it.
        let mut entry = loop {
            let mut settled = self.settled.subscribe();
            let lead = self.lanes().lead(ticket.id);
            match lead {
                Lead::Send(entry) => break entry,
                Lead::Done(outcome) => {
                    guard.id = None;
                    return Pipelined {
                        outcome,
                        resent,
                        merged: true,
                    };
                }
                Lead::Wait => {
                    if settled.changed().await.is_err() {
                        return Pipelined {
                            outcome: WriterOutcome::Ambiguous,
                            resent,
                            merged: true,
                        };
                    }
                }
            }
        };
        let merged = entry != ticket.entry;
        if merged {
            moonpool_assertions::reachable!(
                "client: a pipeline merges consecutive writes into one write"
            );
        }
        loop {
            let mut writer = self.writer();
            let outcome = writer.write_entry(client, &entry, first).await;
            let folded = self.lanes().fold(ticket.id, outcome);
            self.wake();
            match folded {
                Folded::Done(outcome) => {
                    guard.id = None;
                    return Pipelined {
                        outcome,
                        resent,
                        merged,
                    };
                }
                Folded::Apart => {
                    // Over a leader's limits: its own write, alone.
                    entry = ticket.entry.clone();
                    continue;
                }
                Folded::Early => {}
            }
            loop {
                let mut settled = self.settled.subscribe();
                let resumed = self.lanes().resume(ticket.id);
                match resumed {
                    Resumed::Send(again) => {
                        // Paired with `open`: a re-send is the identical
                        // write.
                        assert!(again == entry, "a pipelined write is sent again unchanged");
                        moonpool_assertions::reachable!(
                            "client: a pipelined write that arrived early is sent again"
                        );
                        resent = true;
                        break;
                    }
                    Resumed::Done(outcome) => {
                        self.wake();
                        guard.id = None;
                        return Pipelined {
                            outcome,
                            resent,
                            merged,
                        };
                    }
                    Resumed::Wait => {
                        if settled.changed().await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }

    /// [`Pipeline::open`], then [`Pipeline::send`]: `None` when the writer
    /// leads no term.
    pub async fn write<P: ParosProviders>(
        &self,
        client: &Client<P>,
        build: impl FnOnce(u64) -> Vec<Value>,
        first: usize,
    ) -> Option<Pipelined> {
        let ticket = self.open(build).await?;
        Some(self.send(client, ticket, first).await)
    }

    /// Claim the journal under the writer's uuid ([`Writer::claim`]);
    /// `fresh` takes a new term first, which fences the writes in flight
    /// under the old one. A won claim opens the next write at the tail its
    /// view names.
    #[tracing::instrument(level = "debug", skip_all, fields(fresh))]
    pub async fn claim<P: ParosProviders>(
        &self,
        client: &Client<P>,
        first: usize,
        fresh: bool,
    ) -> ClaimOutcome {
        let (journal, (uuid, led)) = {
            let mut lanes = self.lanes();
            (lanes.writer.journal(), lanes.begin_claim(fresh))
        };
        let outcome = if led {
            client.reclaim(journal, uuid, first).await
        } else {
            client.claim(journal, uuid, first).await
        };
        self.lanes().claimed(uuid, &outcome);
        self.wake();
        outcome
    }

    /// Truncate the journal below `up_to` as its leader
    /// ([`Writer::truncate`]); `None` when it leads no term. A refusal is
    /// folded into the shared writer.
    #[tracing::instrument(level = "debug", skip_all, fields(up_to))]
    pub async fn truncate<P: ParosProviders>(
        &self,
        client: &Client<P>,
        up_to: u64,
        first: usize,
    ) -> Option<TruncateOutcome> {
        let mut writer = self.writer();
        let leader = writer.owned()?;
        let outcome = writer.truncate(client, up_to, first).await?;
        if let TruncateOutcome::Refused { state } = &outcome {
            self.lanes().observe(leader, state);
            self.wake();
        }
        Some(outcome)
    }
}

/// Settles a ticket whose send was dropped before its verdict: the write
/// may still land, so the pipeline treats it as a failure.
struct Abandon<'a> {
    pipeline: &'a Pipeline,
    id: Option<u64>,
}

impl Drop for Abandon<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.pipeline.lanes().abandon(id);
            self.pipeline.wake();
        }
    }
}

/// Whether a new write may open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admit {
    /// At this position.
    At(u64),
    /// Not now: the writer leads no term.
    NotOwner,
    /// Not yet: a write failed, and the writes in flight must settle first.
    Drain,
}

/// What a verdict did to its write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Folded {
    /// Settled with this outcome.
    Done(WriterOutcome),
    /// Arrived before the write in front of it: it waits.
    Early,
    /// A merged write over a leader's limits came apart (#709 (merge
    /// fenced writes)): each write it held is sent alone.
    Apart,
}

/// What a ready write does (#709 (merge fenced writes)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Lead {
    /// Send this write: its own, or merged with the ready writes after it.
    Send(Entry),
    /// A merged write of another ticket carries it.
    Wait,
    /// The merged write that carried it settled with this outcome.
    Done(WriterOutcome),
}

/// Where a write's send stands (#709 (merge fenced writes)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// Opened; its send has not started.
    Opened,
    /// Its send started, and no write took it yet.
    Ready,
    /// Its own send leads it, alone or merged.
    Sending,
    /// The merged write of this ticket carries it.
    Carried(u64),
}

/// What a waiting write does next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Resumed {
    /// Send this identical write again: the tail reached its position.
    Send(Entry),
    /// A write in front of it is still in flight.
    Wait,
    /// Settled: the tail can never reach its position.
    Done(WriterOutcome),
}

/// One write in flight.
#[derive(Clone, Debug)]
struct InFlight {
    id: u64,
    entry: Entry,
    /// The refusal of a write that arrived early, while it waits.
    early: Option<JournalView>,
    /// Where its send stands.
    stage: Stage,
    /// Sent alone from now on: a merged write that held it was over a
    /// leader's limits.
    alone: bool,
    /// The tickets its merged write carries after its own, in position
    /// order; empty when it goes alone.
    carries: Vec<u64>,
    /// Its send was dropped while a merged write carried it.
    dropped: bool,
}

/// The pipeline's pure state: the shared writer, the next position it
/// gives, and the writes in flight.
#[derive(Clone, Debug)]
pub(crate) struct Lanes {
    pub(crate) writer: Writer,
    /// The position the next write opens at, while the writer leads.
    ahead: u64,
    /// A write under the uuid the writer leads with failed: no write opens
    /// until the writes in flight settle.
    broken: bool,
    next_id: u64,
    flight: Vec<InFlight>,
    /// The verdicts of carried writes, until their own send takes them.
    done: BTreeMap<u64, WriterOutcome>,
}

/// The view an outcome names, when it names one.
fn view_of(outcome: &WriterOutcome) -> Option<&JournalView> {
    match outcome {
        WriterOutcome::Superseded { state }
        | WriterOutcome::Refused { state }
        | WriterOutcome::Truncated { state }
        | WriterOutcome::WrongMode { state }
        | WriterOutcome::NotWritten { state } => Some(state),
        _ => None,
    }
}

/// `entry`'s share of a merged write's `outcome`: its own range of a
/// written one, the same verdict otherwise.
fn part_of(outcome: &WriterOutcome, entry: &Entry) -> WriterOutcome {
    match outcome {
        WriterOutcome::Written {
            duplicate,
            resolved,
            ..
        } => WriterOutcome::Written {
            seq: entry.seq.0,
            count: entry.count(),
            duplicate: *duplicate,
            resolved: *resolved,
        },
        other => other.clone(),
    }
}

/// `entry`'s record bytes.
fn bytes_of(entry: &Entry) -> usize {
    entry
        .records
        .iter()
        .map(|record| record.0.len())
        .fold(0, usize::saturating_add)
}

impl Lanes {
    pub(crate) fn new(writer: Writer) -> Self {
        let lanes = Self {
            ahead: writer.next_seq(),
            writer,
            broken: false,
            next_id: 0,
            flight: Vec::new(),
            done: BTreeMap::new(),
        };
        lanes.assert_invariants();
        lanes
    }

    /// The writes in flight hold distinct tickets; under the uuid the writer
    /// leads with, each ends at or below the next position; a failure is
    /// pending only while a write is in flight.
    fn assert_invariants(&self) {
        for (k, write) in self.flight.iter().enumerate() {
            assert!(
                self.flight[k + 1..]
                    .iter()
                    .all(|other| other.id != write.id),
                "every write in flight holds its own ticket"
            );
            assert!(write.id < self.next_id, "a ticket is one the pipeline gave");
            if self.writer.owned() == Some(write.entry.leader) {
                assert!(
                    write.entry.seq.0 + write.entry.count() <= self.ahead,
                    "a write in flight lies below the next position"
                );
            }
        }
        for write in &self.flight {
            if let Stage::Carried(by) = write.stage {
                assert!(
                    self.flight
                        .iter()
                        .any(|other| other.id == by && other.stage == Stage::Sending),
                    "a carried write rides a merged write in flight"
                );
            }
            assert!(
                write.carries.is_empty() || write.stage == Stage::Sending,
                "only a write being sent carries others"
            );
        }
        assert!(
            !self.broken || !self.flight.is_empty(),
            "a failure stops the pipeline only while writes are in flight"
        );
    }

    /// The tickets in flight, in ticket order.
    #[cfg(test)]
    pub(crate) fn in_flight_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.flight.iter().map(|write| write.id).collect();
        ids.sort_unstable();
        ids
    }

    pub(crate) fn holds(&self, id: u64) -> bool {
        self.flight.iter().any(|write| write.id == id)
    }

    fn at(&mut self, id: u64) -> &mut InFlight {
        self.flight
            .iter_mut()
            .find(|write| write.id == id)
            .expect("a settled write is in flight")
    }

    /// Whether a new write may open, and where.
    pub(crate) fn admit(&self) -> Admit {
        if self.writer.owned().is_none() {
            Admit::NotOwner
        } else if self.broken {
            Admit::Drain
        } else {
            Admit::At(self.ahead)
        }
    }

    /// Open a write of `records` at the next position.
    ///
    /// # Panics
    ///
    /// If [`Lanes::admit`] admits none (a programmer error).
    pub(crate) fn open(&mut self, records: Vec<Value>) -> Ticket {
        let Admit::At(seq) = self.admit() else {
            panic!("a write opens only at an admitted position");
        };
        let leader = self.writer.owned().expect("an admitted write has a leader");
        assert!(
            seq >= self.writer.next_seq(),
            "a write opens at or past the tail"
        );
        let entry = Entry {
            leader,
            seq: Seq(seq),
            records,
        };
        self.writer.opened(&entry);
        self.ahead = seq + entry.count();
        let id = self.next_id;
        self.next_id += 1;
        self.flight.push(InFlight {
            id,
            entry: entry.clone(),
            early: None,
            stage: Stage::Opened,
            alone: false,
            carries: Vec::new(),
            dropped: false,
        });
        self.assert_invariants();
        Ticket { id, entry }
    }

    /// Settle `id`: off the flight; once none is in flight, the next write
    /// opens at the tail the writer knows.
    fn remove(&mut self, id: u64) -> InFlight {
        let at = self
            .flight
            .iter()
            .position(|write| write.id == id)
            .expect("a settled write is in flight");
        let write = self.flight.swap_remove(at);
        if self.flight.is_empty() {
            self.ahead = self.writer.next_seq();
            self.broken = false;
        }
        assert!(!self.holds(id), "a settled write leaves the flight");
        self.assert_invariants();
        write
    }

    /// Move the known tail forward to `next`, and the next position with
    /// it: a tail past every position given (a write that landed after it
    /// was given up) is where the next write opens.
    fn forward(&mut self, next: u64) {
        self.writer.advance_to(next);
        self.ahead = self.ahead.max(self.writer.next_seq());
        assert!(
            self.ahead >= self.writer.next_seq(),
            "the next position is at or past the known tail"
        );
    }

    /// Learn from a view a verdict of a write under `leader` named. A view
    /// of a term the writer left teaches nothing; one naming `leader` only
    /// moves the tail forward (the writes behind may have moved it
    /// further); one naming another leader supersedes the writer.
    pub(crate) fn observe(&mut self, leader: LeaderUuid, state: &JournalView) {
        if self.writer.owned() != Some(leader) {
            return;
        }
        if state.leader == Some(leader) {
            self.forward(state.next_seq.0);
        } else {
            self.writer.learn(state);
            assert!(
                self.writer.owned().is_none(),
                "another leader supersedes the writer"
            );
        }
        self.assert_invariants();
    }

    /// Fold `id`'s outcome.
    pub(crate) fn fold(&mut self, id: u64, outcome: WriterOutcome) -> Folded {
        let entry = self.sent_entry(id);
        let merged = !self.get(id).carries.is_empty();
        let current = self.writer.owned() == Some(entry.leader);
        if let Some(state) = view_of(&outcome) {
            self.observe(entry.leader, state);
        }
        match outcome {
            // A merged write over a leader's limits proposed nothing: its
            // writes go apart, each alone.
            WriterOutcome::TooLarge { .. } if merged => {
                self.apart(id);
                Folded::Apart
            }
            WriterOutcome::Written { seq, count, .. } => {
                assert!(
                    !merged || (seq == entry.seq.0 && count == entry.count()),
                    "a merged write is written whole, at its own position"
                );
                self.forward(seq + count);
                Folded::Done(self.settle(id, &outcome))
            }
            // Refused under its own uuid at a tail below its position: the
            // write in front of it was not applied yet.
            WriterOutcome::Refused { state }
                if current
                    && self.writer.owned() == Some(entry.leader)
                    && state.leader == Some(entry.leader)
                    && state.next_seq < entry.seq =>
            {
                self.at(id).early = Some(state);
                self.assert_invariants();
                Folded::Early
            }
            outcome => {
                if current {
                    self.broken = true;
                }
                Folded::Done(self.settle(id, &outcome))
            }
        }
    }

    /// What the early write `id` does next.
    ///
    /// # Panics
    ///
    /// If `id` did not arrive early (a programmer error).
    pub(crate) fn resume(&mut self, id: u64) -> Resumed {
        let write = self.get(id).clone();
        let Some(early) = write.early else {
            panic!("only a write that arrived early waits");
        };
        let leader = write.entry.leader;
        let ahead_of_it = self
            .flight
            .iter()
            .any(|other| other.entry.leader == leader && other.entry.seq < write.entry.seq);
        if ahead_of_it {
            return Resumed::Wait;
        }
        let current = self.writer.owned() == Some(leader);
        if current && self.writer.next_seq() >= write.entry.seq.0 {
            self.at(id).early = None;
            return Resumed::Send(self.sent_entry(id));
        }
        // The writes in front of it settled and the tail stops short of it
        // (or its term ended): its one verdict is the refusal, and the
        // verdict of every write it carries.
        if current {
            self.broken = true;
        }
        Resumed::Done(self.settle(id, &WriterOutcome::Refused { state: early }))
    }

    /// Settle `id`, whose send was dropped: it may still land.
    pub(crate) fn abandon(&mut self, id: u64) {
        if !self.holds(id) {
            return;
        }
        if matches!(self.get(id).stage, Stage::Carried(_)) {
            // The merged write that carries it goes on; its verdict has no
            // one to take it.
            self.at(id).dropped = true;
            self.assert_invariants();
            return;
        }
        if self.writer.owned() == Some(self.get(id).entry.leader) {
            self.broken = true;
        }
        // The writes it carries may land with it.
        self.settle(id, &WriterOutcome::Ambiguous);
    }

    /// Start `id`'s send: it is ready to lead, or to be carried (#709
    /// (merge fenced writes)).
    ///
    /// # Panics
    ///
    /// If its send already started (a programmer error).
    pub(crate) fn ready(&mut self, id: u64) {
        let write = self.at(id);
        assert!(write.stage == Stage::Opened, "a write's send starts once");
        write.stage = Stage::Ready;
        self.assert_invariants();
    }

    /// What the ready write `id` sends (#709 (merge fenced writes)): the
    /// verdict of the merged write that carried it, a wait while one
    /// carries it, or its write, merged with the ready writes at the
    /// positions after it under the same uuid, within [`MERGE_RECORDS`] and
    /// [`MERGE_BYTES`]. An empty write is never merged: it holds no
    /// position of its own.
    ///
    /// # Panics
    ///
    /// If `id` is not ready (a programmer error).
    pub(crate) fn lead(&mut self, id: u64) -> Lead {
        if let Some(outcome) = self.done.remove(&id) {
            return Lead::Done(outcome);
        }
        let write = self.at(id);
        match write.stage {
            Stage::Carried(_) => return Lead::Wait,
            Stage::Ready => write.stage = Stage::Sending,
            Stage::Opened | Stage::Sending => panic!("only a ready write leads"),
        }
        let leader = write.entry.leader;
        let alone = write.alone || write.entry.records.is_empty();
        let mut end = write.entry.seq.0 + write.entry.count();
        let mut records = write.entry.records.len();
        let mut bytes = bytes_of(&write.entry);
        loop {
            if alone {
                break;
            }
            let next = self.flight.iter_mut().find(|other| {
                other.stage == Stage::Ready
                    && !other.alone
                    && !other.entry.records.is_empty()
                    && other.entry.leader == leader
                    && other.entry.seq.0 == end
                    && records + other.entry.records.len() <= MERGE_RECORDS
                    && bytes.saturating_add(bytes_of(&other.entry)) <= MERGE_BYTES
            });
            let Some(other) = next else {
                break;
            };
            other.stage = Stage::Carried(id);
            end += other.entry.count();
            records += other.entry.records.len();
            bytes = bytes.saturating_add(bytes_of(&other.entry));
            let member = other.id;
            self.at(id).carries.push(member);
        }
        self.assert_invariants();
        Lead::Send(self.sent_entry(id))
    }

    fn get(&self, id: u64) -> &InFlight {
        self.flight
            .iter()
            .find(|write| write.id == id)
            .expect("a write is in flight")
    }

    /// The write `id`'s send carries: its own entry, then the records of
    /// each write it carries, in turn.
    pub(crate) fn sent_entry(&self, id: u64) -> Entry {
        let write = self.get(id);
        let mut entry = write.entry.clone();
        for member in &write.carries {
            entry
                .records
                .extend(self.get(*member).entry.records.iter().cloned());
        }
        entry
    }

    /// Settle `id` and every write it carries with `outcome`: a write's
    /// range is its own part of a written one. Returns `id`'s own verdict;
    /// the others wait in `done` for their sends.
    fn settle(&mut self, id: u64, outcome: &WriterOutcome) -> WriterOutcome {
        let carries = std::mem::take(&mut self.at(id).carries);
        let own = self.get(id).entry.clone();
        let mut at = own.seq.0 + own.count();
        for member in carries {
            let write = self.remove(member);
            assert!(
                write.stage == Stage::Carried(id) && write.entry.seq.0 == at,
                "a merged write carries the writes at the positions after its own"
            );
            at += write.entry.count();
            if !write.dropped {
                self.done.insert(member, part_of(outcome, &write.entry));
            }
        }
        self.remove(id);
        part_of(outcome, &own)
    }

    /// Take `id`'s merged write apart: each write it carried sends itself
    /// alone, and none merges again. A write whose send was dropped is
    /// never sent: the merged write proposed nothing, so it is not
    /// written, and the pipeline drains as after a failure.
    fn apart(&mut self, id: u64) {
        let carries = std::mem::take(&mut self.at(id).carries);
        self.at(id).alone = true;
        let leader = self.get(id).entry.leader;
        for member in carries {
            if self.get(member).dropped {
                if self.writer.owned() == Some(leader) {
                    self.broken = true;
                }
                self.remove(member);
            } else {
                let write = self.at(member);
                write.stage = Stage::Ready;
                write.alone = true;
            }
        }
        self.assert_invariants();
    }

    /// Start a claim: `fresh` spends the uuid the writer leads with. The
    /// uuid the claim asks for, and whether that uuid already leads (a
    /// re-claim, [`Client::reclaim`]: a uuid never leads twice).
    pub(crate) fn begin_claim(&mut self, fresh: bool) -> (LeaderUuid, bool) {
        if fresh {
            self.writer.begin_term();
            assert!(
                self.writer.owned().is_none(),
                "a fresh claim leads no term yet"
            );
        }
        self.assert_invariants();
        (self.writer.uuid(), self.writer.owned().is_some())
    }

    /// Fold the outcome of a claim under `uuid`. A claim a later one
    /// overtook teaches nothing; a writer that already leads under `uuid` (a
    /// sibling's claim won first) only moves its tail forward; a won claim
    /// opens the next write at its view's tail.
    pub(crate) fn claimed(&mut self, uuid: LeaderUuid, outcome: &ClaimOutcome) {
        if self.writer.uuid() != uuid {
            return;
        }
        let view = match outcome {
            ClaimOutcome::Won { state }
            | ClaimOutcome::Lost { state }
            | ClaimOutcome::Owned { state } => Some(state),
            _ => None,
        };
        if self.writer.owned() == Some(uuid) {
            if let Some(state) = view {
                self.observe(uuid, state);
            }
            return;
        }
        self.writer.claimed(outcome);
        if self.writer.owned() == Some(uuid) {
            // The term's first write: nothing is in flight under it.
            assert!(
                self.flight.iter().all(|write| write.entry.leader != uuid),
                "a new term has no write in flight"
            );
            self.ahead = self.writer.next_seq();
            self.broken = false;
        }
        self.assert_invariants();
    }
}
