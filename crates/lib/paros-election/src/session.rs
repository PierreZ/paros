//! A **member** of the store: one session over one journal client
//! (`docs/architecture/04-roles.md` §4.4, "The coordination store").
//!
//! [`Member`] has the shape of the journal client it owns
//! (`paros_journal::client`): calls in, requests out, answers in. The
//! program sends each [`Request`] of [`Member::take_requests`] to the
//! journal and gives each answer back with [`Member::answer`]. It gives the
//! time and each draw (a session id, a jitter) as values: the member reads
//! no clock and draws no random number. The member keeps nothing on disk:
//! its state is the journal. A member that stops comes back as a new
//! member, with a fresh session id.
//!
//! What a member does:
//!
//! - **It tails the journal** and folds every record into its [`Tree`]
//!   ([`Member::tick`] asks the next page). Each change of the fold is an
//!   [`Event::Changed`]: a watch is a filter on these, and no change
//!   between two events is lost. A member below the floor of the journal
//!   starts again from the checkpoint at the floor.
//! - **It sends one entry of its session at a time**, in the order the
//!   program asked for them, and settles each one only from its own fold
//!   (TLC: `SettledTrue` of `specs/tla/coord/`): the journal client may land
//!   a copy two times, and the fold applies the first one only.
//! - **It renews its session**: with nothing to send for
//!   [`Config::heartbeat`], it sends an empty transaction.
//! - **It expires a silent session**: a session whose last position did not
//!   move for [`Config::timeout`] plus the jitter of the tick, on the time
//!   that the program gives, gets `Expire(session, last)`. The fold applies
//!   it only if the session's last entry is still at `last`. An early
//!   expiry costs availability, never safety.
//! - **It takes checkpoints and trims** only when the program asks for
//!   them ([`Member::checkpoint`], [`Member::trim`]); a trim goes only to a
//!   checkpoint that its fold took. **Open**: which member does it, and
//!   when.
//!
//! **Precondition: only the store truncates its journal.** The journal is
//! multi-writer (§J2.4), so anyone with access may `Truncate`, and the
//! journal cannot refuse it. A record at the floor that is not a checkpoint
//! the fold took stops the member: [`Event::Unreadable`].

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use paros_journal::client::{CallId, Client, Outcome, Request, RequestId, Response};
use paros_journal::{JournalView, Leader, LeaderUuid, Seq};

use crate::codec;
use crate::entry::{Entry, SessionId, Term, Txn};
use crate::path::Path;
use crate::tree::{Change, Fate, Tree, TxnError};

/// The numbers a member works with, on the program's time unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// The most records of one read.
    pub page: u64,
    /// The time with nothing sent after which the session sends a
    /// heartbeat.
    pub heartbeat: u64,
    /// The time with no new entry of a session after which a member
    /// expires it. It is longer than `heartbeat`. **Open**: the values
    /// (`ZooKeeper`'s default session timeout is two to twenty ticks).
    pub timeout: u64,
}

/// The id of one transaction that the program asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub u64);

/// How a transaction of the session ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Settled {
    /// It applied.
    Applied,
    /// It failed: a check or an operation. Its `op_seq` is used; nothing
    /// changed.
    Failed(TxnError),
    /// It is folded below the checkpoint that the member started from: it
    /// took its `op_seq`, and the member cannot tell whether it applied.
    /// Read the tree.
    Unknown,
    /// The session expired before the fold reached it: it will never
    /// apply.
    Expired,
}

/// What the program learns from the member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A transaction of the session ended.
    Settled {
        /// The transaction.
        txn: TxnId,
        /// How it ended.
        settled: Settled,
    },
    /// One change of the fold, by the record at `seq`.
    Changed {
        /// The position of the record.
        seq: Seq,
        /// The change.
        change: Change,
    },
    /// The session expired: every transaction still open ends
    /// [`Settled::Expired`]. The program opens a new session with a fresh
    /// id to go on.
    SessionExpired(SessionId),
    /// A checkpoint that this member wrote landed and the fold took it at
    /// `seq`, or did not.
    Checkpoint {
        /// Where it landed, when the fold took it.
        taken: Option<Seq>,
    },
    /// The journal answered a call that is not a read or an entry: a
    /// `SetLeader` of [`Member::set_mode`] or a `Truncate` of
    /// [`Member::trim`].
    Journal(Outcome),
    /// The journal refused an entry of this member: the journal is not
    /// multi-writer, or the entry is too large. The entry stays in flight;
    /// [`Member::resend`] sends it again.
    Refused(Outcome),
    /// The record at the floor of the journal is not a checkpoint that the
    /// fold takes: something other than the store truncated the journal.
    /// The member stops reading.
    Unreadable(Seq),
}

/// Where the session is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// No session: [`Member::open`] opens one.
    Closed,
    /// The `Open` of the session is in flight.
    Opening(SessionId),
    /// The fold shows the session live.
    Live(SessionId),
    /// The fold shows the session expired.
    Expired(SessionId),
}

/// What a call of the client is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Purpose {
    Entry,
    Expire,
    Checkpoint,
    Journal,
    Read,
}

/// The entry of the session in flight.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Flight {
    txn: Option<TxnId>,
    entry: Entry,
}

/// A member of the store.
#[derive(Clone, Debug)]
pub struct Member {
    config: Config,
    client: Client,
    calls: BTreeMap<CallId, Purpose>,
    tree: Option<Tree>,
    /// Where the next read starts when there is no tree: 0, or the floor
    /// of the journal.
    start: Seq,
    reading: bool,
    unreadable: bool,
    state: State,
    next_op: u64,
    next_txn: u64,
    /// The transactions that wait, each with its id (none for a heartbeat).
    queue: VecDeque<(Option<TxnId>, Txn)>,
    flight: Option<Flight>,
    /// The checkpoints this member wrote, by read position.
    written: BTreeSet<Seq>,
    now: u64,
    /// The time the session's last entry settled, or it opened.
    renewed: u64,
    /// For each live session, the last position the member saw and the
    /// time it first saw it.
    watched: BTreeMap<SessionId, (Seq, u64)>,
    /// The expiries sent, so a member sends one per `last`.
    expiring: BTreeSet<(SessionId, Seq)>,
    events: Vec<Event>,
}

impl Member {
    /// A member with no session, that has read nothing yet.
    ///
    /// # Panics
    ///
    /// When `config.timeout` is not above `config.heartbeat`, or the page
    /// is empty: a bug of the program.
    #[must_use]
    pub fn new(config: Config) -> Self {
        assert!(config.page > 0, "a read asks for at least one record");
        assert!(
            config.timeout > config.heartbeat,
            "a session renews itself before others expire it"
        );
        let member = Self {
            config,
            client: Client::new(),
            calls: BTreeMap::new(),
            tree: None,
            start: Seq(0),
            reading: false,
            unreadable: false,
            state: State::Closed,
            next_op: 1,
            next_txn: 0,
            queue: VecDeque::new(),
            flight: None,
            written: BTreeSet::new(),
            now: 0,
            renewed: 0,
            watched: BTreeMap::new(),
            expiring: BTreeSet::new(),
            events: Vec::new(),
        };
        member.assert_invariants();
        member
    }

    /// Make the journal multi-writer, under the fresh tag `tag`, if nobody
    /// set its leader yet: `SetLeader(multi(tag), Unset)`. The store needs a
    /// multi-writer journal. The answer is an [`Event::Journal`].
    pub fn set_mode(&mut self, tag: LeaderUuid) -> CallId {
        let call = self.client.set_leader(Leader::multi(tag), Leader::Unset);
        self.calls.insert(call, Purpose::Journal);
        call
    }

    /// Open the session `id`, drawn fresh by the program, and apply `txn`
    /// in the same step (the latch join of a [`crate::recipes::Latch`],
    /// for example).
    ///
    /// # Panics
    ///
    /// When the member has a session that did not expire, or `id` is zero.
    pub fn open(&mut self, id: SessionId, txn: Txn) -> TxnId {
        assert!(id.is_set(), "a session id is not zero");
        assert!(
            matches!(self.state, State::Closed | State::Expired(_)),
            "a member opens one session at a time"
        );
        assert!(
            self.flight.is_none(),
            "nothing is in flight with no session"
        );
        let txn_id = self.new_txn();
        self.state = State::Opening(id);
        self.next_op = 1;
        self.renewed = self.now;
        self.send(Flight {
            txn: Some(txn_id),
            entry: Entry::Open { session: id, txn },
        });
        txn_id
    }

    /// Ask for the transaction `txn` of the session. It leaves when the
    /// entry before it settled.
    ///
    /// # Panics
    ///
    /// When the member has no session that is opening or live.
    pub fn submit(&mut self, txn: Txn) -> TxnId {
        assert!(
            matches!(self.state, State::Opening(_) | State::Live(_)),
            "a transaction belongs to a session"
        );
        let id = self.new_txn();
        self.queue.push_back((Some(id), txn));
        self.send_next();
        id
    }

    /// Send the entry in flight again, after an [`Event::Refused`]. The
    /// fold applies one copy only.
    pub fn resend(&mut self) {
        if let Some(flight) = self.flight.take() {
            self.send(flight);
        }
    }

    /// Write a checkpoint of the fold at its read position. The answer is an
    /// [`Event::Checkpoint`] once the fold reaches it. Returns `false` when
    /// the member has no fold yet.
    pub fn checkpoint(&mut self) -> bool {
        let Some(tree) = &self.tree else {
            return false;
        };
        let at = tree.next();
        let entry = Entry::Checkpoint {
            at,
            tree: Box::new(tree.clone()),
        };
        let call = self.client.write(vec![codec::encode(&entry)]);
        self.calls.insert(call, Purpose::Checkpoint);
        self.written.insert(at);
        true
    }

    /// Truncate the journal up to `at`, which is a checkpoint that the
    /// fold took. The answer is an [`Event::Journal`]. Returns `None` when
    /// `at` is not one: a trim anywhere else breaks the fold of every new
    /// member (TLC: control `trim_any`).
    pub fn trim(&mut self, at: Seq) -> Option<CallId> {
        let taken = self.tree.as_ref()?.checkpoints().contains(&at);
        if !taken {
            return None;
        }
        let call = self.client.truncate(at);
        self.calls.insert(call, Purpose::Journal);
        Some(call)
    }

    /// The time is `now`: read the next page, renew the session, expire the
    /// sessions that are silent for the timeout plus `jitter`.
    ///
    /// # Panics
    ///
    /// When `now` goes back: a bug of the program.
    pub fn tick(&mut self, now: u64, jitter: u64) {
        assert!(now >= self.now, "the time of the program does not go back");
        self.now = now;
        self.read_next();
        if let State::Live(_) = self.state
            && self.flight.is_none()
            && self.queue.is_empty()
            && now - self.renewed >= self.config.heartbeat
        {
            self.queue.push_back((None, Txn::default()));
            self.send_next();
        }
        self.expire_silent(jitter);
        self.assert_invariants();
    }

    /// The requests to send now.
    #[must_use]
    pub fn take_requests(&mut self) -> Vec<Request> {
        self.client.take_requests()
    }

    /// The `response` to request `id`.
    ///
    /// # Panics
    ///
    /// When the response is of the wrong kind for its request (a bug of the
    /// program, as for the journal client), or a rule of the member breaks.
    pub fn answer(&mut self, id: RequestId, response: Response) {
        self.client.answer(id, response);
        for (call, outcome) in self.client.take_outcomes() {
            let purpose = self.calls.remove(&call).expect("every call has a purpose");
            self.outcome(purpose, outcome);
        }
        self.assert_invariants();
    }

    /// What the program learned since the last call.
    #[must_use]
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    /// The fold, once the member read the start of the journal.
    #[must_use]
    pub fn tree(&self) -> Option<&Tree> {
        self.tree.as_ref()
    }

    /// The position the fold started from: 0, or the floor of the journal
    /// when the member started from the checkpoint there.
    #[must_use]
    pub fn start(&self) -> Seq {
        self.start
    }

    /// Where the session is.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// The term at which this member's session holds the turn of `parent`,
    /// in its fold. An order out of the journal carries this term, and the
    /// receiver checks it in the same write as the effect. Read it with
    /// [`Member::quiet`]: a holder that saw none of its entries land for
    /// part of the timeout may already be expired (Curator's `SUSPENDED`).
    #[must_use]
    pub fn holds(&self, parent: &Path) -> Option<Term> {
        let State::Live(id) = self.state else {
            return None;
        };
        let turn = self.tree.as_ref()?.turn(parent)?;
        (turn.holder == id).then_some(turn.term)
    }

    /// The time since the session's last entry settled, or it opened.
    #[must_use]
    pub fn quiet(&self) -> u64 {
        self.now - self.renewed
    }

    /// Whether the member has a call or a transaction open.
    #[must_use]
    pub fn idle(&self) -> bool {
        self.flight.is_none() && self.queue.is_empty() && self.calls.is_empty()
    }

    /// Check the member's rules.
    ///
    /// # Panics
    ///
    /// When a rule does not hold: a bug of the member.
    pub fn assert_invariants(&self) {
        assert!(
            self.flight.is_some() || self.queue.is_empty() || !matches!(self.state, State::Live(_)),
            "a live session sends its next transaction when nothing is in flight"
        );
        assert!(
            !matches!(self.state, State::Closed | State::Expired(_)) || self.flight.is_none(),
            "nothing is in flight with no session"
        );
        assert!(self.renewed <= self.now, "the session renewed in the past");
        if let Some(flight) = &self.flight {
            let (session, op_seq) = flight.entry.of_session().expect("an entry of the session");
            assert!(
                Some(session) == self.session(),
                "the entry is of this session"
            );
            assert!(
                op_seq < self.next_op || op_seq == 0,
                "the op_seq is used once"
            );
        }
    }

    fn session(&self) -> Option<SessionId> {
        match self.state {
            State::Closed => None,
            State::Opening(id) | State::Live(id) | State::Expired(id) => Some(id),
        }
    }

    fn new_txn(&mut self) -> TxnId {
        self.next_txn += 1;
        TxnId(self.next_txn)
    }

    fn send(&mut self, flight: Flight) {
        let call = self.client.write(vec![codec::encode(&flight.entry)]);
        self.calls.insert(call, Purpose::Entry);
        self.flight = Some(flight);
    }

    /// The next transaction of the queue, when nothing is in flight and the
    /// session is live.
    fn send_next(&mut self) {
        let State::Live(session) = self.state else {
            return;
        };
        if self.flight.is_some() {
            return;
        }
        let Some((txn_id, txn)) = self.queue.pop_front() else {
            return;
        };
        let op_seq = self.next_op;
        self.next_op += 1;
        self.send(Flight {
            txn: txn_id,
            entry: Entry::Txn {
                session,
                op_seq,
                txn,
            },
        });
    }

    fn read_next(&mut self) {
        if self.reading || self.unreadable {
            return;
        }
        let from = self.tree.as_ref().map_or(self.start, Tree::next);
        let call = self.client.read(from, self.config.page);
        self.calls.insert(call, Purpose::Read);
        self.reading = true;
    }

    fn outcome(&mut self, purpose: Purpose, outcome: Outcome) {
        match purpose {
            Purpose::Read => {
                self.reading = false;
                match outcome {
                    Outcome::Page { from, records, .. } => {
                        let full = records.len() as u64 == self.config.page;
                        self.fold(from, &records);
                        if full {
                            self.read_next();
                        }
                    }
                    Outcome::Truncated(view) => self.below_floor(view),
                    other => unreachable!("a read ends with a page: {other:?}"),
                }
            }
            Purpose::Entry => {
                if !matches!(outcome, Outcome::Written { .. }) {
                    self.events.push(Event::Refused(outcome));
                }
            }
            Purpose::Expire | Purpose::Checkpoint => {}
            Purpose::Journal => self.events.push(Event::Journal(outcome)),
        }
    }

    /// The member's next position is below the floor: start again from the
    /// checkpoint at the floor.
    fn below_floor(&mut self, view: JournalView) {
        self.tree = None;
        self.start = view.first_seq;
        self.watched.clear();
    }

    /// Fold the records of a page from `from`.
    fn fold(&mut self, from: Seq, records: &[paros_journal::Record]) {
        let expected = self.tree.as_ref().map_or(self.start, Tree::next);
        if from != expected || records.is_empty() {
            return;
        }
        if self.tree.is_none() {
            let first = codec::decode(&records[0]);
            let tree = if from == Seq(0) {
                Some(Tree::new())
            } else {
                Tree::from_checkpoint(from, first.as_ref())
            };
            let Some(tree) = tree else {
                self.unreadable = true;
                self.events.push(Event::Unreadable(from));
                return;
            };
            self.tree = Some(tree);
            self.after_jump();
        }
        for (offset, record) in records.iter().enumerate() {
            let seq = Seq(from.0 + offset as u64);
            let entry = codec::decode(record);
            let tree = self.tree.as_mut().expect("the fold has a start");
            let (fate, changes) = tree.apply(seq, entry.as_ref());
            self.learn(seq, entry.as_ref(), &fate, changes);
        }
        self.send_next();
    }

    /// After a start from a checkpoint: the entry in flight may be folded
    /// below it, and the session may be gone.
    fn after_jump(&mut self) {
        let tree = self.tree.as_ref().expect("a fold");
        let Some(id) = self.session() else {
            return;
        };
        if tree.expired(id) {
            self.end_session(id);
            return;
        }
        let Some(row) = tree.session(id).copied() else {
            return;
        };
        if let State::Opening(_) = self.state {
            self.state = State::Live(id);
        }
        if let Some(flight) = &self.flight {
            let (_, op_seq) = flight.entry.of_session().expect("an entry of the session");
            if row.op_seq >= op_seq {
                let txn = flight.txn;
                self.flight = None;
                self.settle(txn, Settled::Unknown);
            }
        }
    }

    fn learn(&mut self, seq: Seq, entry: Option<&Entry>, fate: &Fate, changes: Vec<Change>) {
        let own = self.session();
        let mine = |s: &SessionId| Some(*s) == own;
        match fate {
            Fate::Opened { session, result } if mine(session) => {
                if self
                    .flight
                    .as_ref()
                    .is_some_and(|f| f.entry.of_session() == Some((*session, 0)))
                {
                    let txn = self.flight.take().and_then(|f| f.txn);
                    self.state = State::Live(*session);
                    self.settle(txn, settled(result));
                }
            }
            Fate::Done {
                session,
                op_seq,
                result,
            } if mine(session) => {
                if self
                    .flight
                    .as_ref()
                    .is_some_and(|f| f.entry.of_session() == Some((*session, *op_seq)))
                {
                    let txn = self.flight.take().and_then(|f| f.txn);
                    self.settle(txn, settled(result));
                }
            }
            Fate::Checkpoint { taken } => {
                if let Some(Entry::Checkpoint { at, .. }) = entry
                    && self.written.remove(at)
                {
                    self.events.push(Event::Checkpoint {
                        taken: taken.then_some(seq),
                    });
                }
            }
            _ => {}
        }
        for change in changes {
            if let Change::Expired(session) = &change
                && mine(session)
            {
                self.end_session(*session);
            }
            self.events.push(Event::Changed { seq, change });
        }
    }

    fn settle(&mut self, txn: Option<TxnId>, settled: Settled) {
        self.renewed = self.now;
        if let Some(txn) = txn {
            self.events.push(Event::Settled { txn, settled });
        }
    }

    fn end_session(&mut self, id: SessionId) {
        if let Some(flight) = self.flight.take()
            && let Some(txn) = flight.txn
        {
            self.events.push(Event::Settled {
                txn,
                settled: Settled::Expired,
            });
        }
        for (txn, _) in std::mem::take(&mut self.queue) {
            let Some(txn) = txn else { continue };
            self.events.push(Event::Settled {
                txn,
                settled: Settled::Expired,
            });
        }
        self.state = State::Expired(id);
        self.events.push(Event::SessionExpired(id));
    }

    fn expire_silent(&mut self, jitter: u64) {
        let Some(tree) = &self.tree else {
            return;
        };
        let own = self.session();
        let live: BTreeMap<SessionId, Seq> = tree
            .sessions()
            .filter(|(s, _)| Some(*s) != own)
            .map(|(s, row)| (s, row.last))
            .collect();
        self.watched.retain(|s, _| live.contains_key(s));
        self.expiring.retain(|(s, _)| live.contains_key(s));
        let mut due = Vec::new();
        for (session, last) in live {
            let seen = self.watched.entry(session).or_insert((last, self.now));
            if seen.0 != last {
                *seen = (last, self.now);
            } else if self.now - seen.1 >= self.config.timeout + jitter
                && self.expiring.insert((session, last))
            {
                due.push(Entry::Expire { session, last });
            }
        }
        for entry in due {
            let call = self.client.write(vec![codec::encode(&entry)]);
            self.calls.insert(call, Purpose::Expire);
        }
    }
}

fn settled(result: &Result<(), TxnError>) -> Settled {
    match result {
        Ok(()) => Settled::Applied,
        Err(error) => Settled::Failed(*error),
    }
}

#[cfg(test)]
mod tests;
