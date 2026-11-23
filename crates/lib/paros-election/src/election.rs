//! The state machine of one actor of an election over one journal.
//!
//! The steps (`docs/architecture/03-3-coordinators-and-placement.md` §3.3,
//! the model `specs/tla/election/`):
//!
//! - **Watch.** A candidate reads the tail of the journal every
//!   [`Config::poll`]. Each read gives a view: the leader value and the tail.
//!   The candidate folds the election records it reads (the last one names
//!   the holder, its term and its interface: [`Event::Holder`]).
//! - **Challenge.** The holder is silent when the candidate sees the same
//!   leader and the same tail for one [`Config::lease`], on the clock the
//!   program passes. A journal with no leader has no holder. The candidate
//!   then waits a jitter that the program draws, and claims if the holder
//!   is still silent. A claim that is too early deposes a live holder: it
//!   costs availability, never safety.
//! - **Win.** One compare-and-set, `SetLeader(new, old)`, with a fresh uuid
//!   that the program draws as `new` and the leader of the last view as
//!   `old`. The loser learns the leader from the refusal ([`Event::Lost`]).
//!   The client settles a lost answer: a refusal after a resend that names
//!   `new` is a win.
//! - **Number.** The winner reads the journal up to the tail that the answer
//!   of its `SetLeader` names. Its term number is the number of the last
//!   election record there, plus one: the count of the terms in the fold. No
//!   election record lands between the win and that tail, since only the
//!   winner's uuid may write then.
//! - **Hold.** The first write of the term is the term record. Once it is
//!   written, the actor leads ([`Event::Leading`]). Each later write under
//!   the uuid is a renewal: the program's writes ([`Election::write`]) and,
//!   when it is idle for [`Config::renew`], a renewal record.
//! - **Lose.** The holder stops at its first refused write
//!   ([`Event::Deposed`]). An answer counts only for the uuid its write was
//!   sent under: the answer to a write of an earlier term never stops or
//!   publishes the current one.
//! - **Hand on.** `SetLeader(next, me)` with a fresh uuid. On a win, the
//!   program gives the [`Handoff`] to the next actor, which adopts it
//!   ([`Election::adopt`]) and writes the term record of the next number.
//! - **Mode.** The election never claims a journal whose leader is a
//!   multi-writer mode ([`Event::MultiWriter`]).
//!
//! The election keeps nothing on disk: its state is in the journal. An
//! actor that restarts is a new incarnation, with a new `Election`, and
//! draws new uuids. It owns one [`Client`] and makes no message of its
//! own: the program sends the client's [`Request`]s and gives back their
//! [`Response`]s.
//!
//! **The fence of a term is not here.** Each receiver of an order checks
//! the term number in the same write as the effect (the takeover rules of
//! `docs/architecture/04-roles.md` §4.4). [`Event::Leading`] comes only
//! once the term record is in the journal, so no two holders send orders
//! under one number (`OneOrderer` of the model).

use std::collections::{BTreeMap, BTreeSet};

use paros_journal::client::{CallId, Client, Outcome, Request, RequestId, Response};
use paros_journal::{Answer, JournalView, Leader, LeaderUuid, Record, Seq};

use crate::config::Config;
use crate::record::{ElectionRecord, Kind, Term};

/// The draws of the program: the election reads no randomness.
pub trait Draws {
    /// A fresh uuid: never drawn before by any actor of the journal
    /// (`docs/journal/02-data-plane.md` §J2.3). The election also refuses a
    /// uuid that it saw, and draws again.
    fn uuid(&mut self) -> LeaderUuid;
    /// A jitter in `0..=max`.
    fn jitter(&mut self, max: u64) -> u64;
}

/// What a holder that handed its term on gives the next actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Handoff {
    /// The uuid that the hand on installed. The next actor writes under it.
    pub uuid: LeaderUuid,
    /// The number of the next term: the handing holder's, plus one.
    pub term: Term,
    /// The tail of the journal after the hand on: the next actor's first
    /// write goes there.
    pub next_seq: Seq,
}

/// The id of one write that the program asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteId(pub u64);

/// What the election tells the program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The actor leads term `term` under `uuid`: its term record is in the
    /// journal. Orders that it sends carry `term`.
    Leading {
        /// The number of the term.
        term: Term,
        /// The uuid the actor writes under.
        uuid: LeaderUuid,
    },
    /// A term that the actor won stopped at a refused write (or a refused
    /// hand on). [`Event::Leading`] may not have come for it.
    Deposed {
        /// The number of the term.
        term: Term,
    },
    /// The program stopped the actor's term ([`Election::resign`]).
    Stopped {
        /// The number of the term.
        term: Term,
    },
    /// The actor's claim lost the compare-and-set; `leader` is the leader
    /// value the journal holds.
    Lost {
        /// The leader value in the refusal.
        leader: Leader,
    },
    /// The fold read a new election record: the holder that it names.
    Holder {
        /// The number of the holder's term.
        term: Term,
        /// The uuid the holder writes under.
        uuid: LeaderUuid,
        /// The holder's interface.
        interface: Vec<u8>,
    },
    /// The journal is in a multi-writer mode: the election does not claim
    /// it while it stays so.
    MultiWriter,
    /// The hand on won: give `0` to the next actor.
    HandedOn(Handoff),
    /// A write of the program is in the journal.
    Wrote {
        /// The write.
        id: WriteId,
        /// The position of its first record.
        seq: Seq,
        /// How many records.
        count: u64,
    },
    /// A write of the program is not in the journal, or its fate is
    /// unknown: its term stopped.
    NotWritten {
        /// The write.
        id: WriteId,
    },
}

/// Why the election refused a call of the program.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The actor does not lead: no term, or its term record is not written.
    NotLeading,
    /// A write of the actor is in flight: one at a time.
    Busy,
    /// A record carries the key of the election record.
    Reserved,
}

/// Where the actor is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Not a candidate.
    Standby,
    /// A candidate: it watches the holder. `claim_at` is the time it claims
    /// at once it found the holder silent.
    Watching { claim_at: Option<u64> },
    /// The `SetLeader` of a claim is out.
    Claiming { uuid: LeaderUuid, call: CallId },
    /// The claim won: the actor reads up to `until` for its number.
    Folding { uuid: LeaderUuid, until: Seq },
    /// The term record is out.
    Publishing {
        uuid: LeaderUuid,
        term: Term,
        call: CallId,
    },
    /// The actor leads; `write` is its write in flight.
    Leading {
        uuid: LeaderUuid,
        term: Term,
        write: Option<CallId>,
    },
    /// The `SetLeader` of a hand on is out.
    Handing {
        term: Term,
        next: LeaderUuid,
        call: CallId,
    },
}

/// Why a call of the client was asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Purpose {
    Claim,
    Publish,
    Renew,
    User(WriteId),
    Hand,
}

/// A call of the client: why, and the uuid it was asked under (the new
/// uuid for a claim).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Asked {
    purpose: Purpose,
    uuid: LeaderUuid,
}

/// The last view a read gave, and since when the actor sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sample {
    leader: Leader,
    next: Seq,
    since: u64,
}

/// One actor of an election over one journal.
#[derive(Clone, Debug)]
pub struct Election {
    config: Config,
    interface: Vec<u8>,
    client: Client,
    phase: Phase,
    now: u64,
    calls: BTreeMap<CallId, Asked>,
    /// The read in flight: its call and its first position.
    read: Option<(CallId, Seq)>,
    /// When the last read was asked.
    read_at: Option<u64>,
    /// The next position to fold.
    cursor: Seq,
    /// The last election record folded.
    last: Option<ElectionRecord>,
    sample: Option<Sample>,
    multi: bool,
    /// The uuids this actor drew.
    used: BTreeSet<LeaderUuid>,
    /// When the actor last sent a write of its term.
    wrote_at: u64,
    next_write: u64,
    events: Vec<Event>,
}

impl Election {
    /// An actor that publishes `interface` when it leads, at time `now`. It
    /// is not a candidate until [`Election::campaign`].
    ///
    /// # Panics
    ///
    /// When `config` cannot hold a term ([`Config::check`]).
    #[must_use]
    pub fn new(config: Config, interface: Vec<u8>, now: u64) -> Self {
        config.check();
        let election = Self {
            config,
            interface,
            client: Client::new(),
            phase: Phase::Standby,
            now,
            calls: BTreeMap::new(),
            read: None,
            read_at: None,
            cursor: Seq(0),
            last: None,
            sample: None,
            multi: false,
            used: BTreeSet::new(),
            wrote_at: now,
            next_write: 0,
            events: Vec::new(),
        };
        election.assert_invariants();
        election
    }

    /// Become a candidate. A candidate that leads already stays so.
    pub fn campaign(&mut self) {
        if self.phase == Phase::Standby {
            self.phase = Phase::Watching { claim_at: None };
        }
        self.assert_invariants();
    }

    /// Stop being a candidate. A holder stops its term
    /// ([`Event::Stopped`]) and writes no more renewals; another candidate
    /// claims after one lease. The answers to calls in flight are ignored.
    pub fn resign(&mut self) {
        if let Some(term) = self.term() {
            self.events.push(Event::Stopped { term });
        }
        self.phase = Phase::Standby;
        self.assert_invariants();
    }

    /// The time is `now`: read the tail, claim, or renew when it is due.
    ///
    /// # Panics
    ///
    /// When `now` is before the time of the last tick: the clock of the
    /// program goes back.
    pub fn tick<D: Draws>(&mut self, now: u64, draws: &mut D) {
        assert!(now >= self.now, "the clock of the program never goes back");
        self.now = now;
        self.maybe_read();
        match self.phase {
            Phase::Watching { claim_at } => self.watch(claim_at, draws),
            Phase::Leading {
                uuid,
                term,
                write: None,
            } if now.saturating_sub(self.wrote_at) >= self.config.renew => {
                self.renew(uuid, term);
            }
            _ => {}
        }
        self.assert_invariants();
    }

    /// The `response` to the client's request `id`.
    pub fn answer(&mut self, id: RequestId, response: Response) {
        self.client.answer(id, response);
        for (call, outcome) in self.client.take_outcomes() {
            if self.read.is_some_and(|(read, _)| read == call) {
                self.on_read(outcome);
            } else if let Some(asked) = self.calls.remove(&call) {
                self.on_call(call, asked, outcome);
            } else {
                unreachable!("the election asked every call of its client");
            }
        }
        self.assert_invariants();
    }

    /// Write `records` under the actor's term: a renewal too.
    ///
    /// # Errors
    ///
    /// [`Refusal::NotLeading`] when the actor does not lead,
    /// [`Refusal::Busy`] while a write of it is in flight, and
    /// [`Refusal::Reserved`] when a record carries the key of the election
    /// record.
    pub fn write(&mut self, records: Vec<Record>) -> Result<WriteId, Refusal> {
        let Phase::Leading { uuid, term, write } = self.phase else {
            return Err(Refusal::NotLeading);
        };
        if write.is_some() {
            return Err(Refusal::Busy);
        }
        if records.iter().any(ElectionRecord::reserved) {
            return Err(Refusal::Reserved);
        }
        let id = WriteId(self.next_write);
        self.next_write += 1;
        let call = self.client.write(records);
        self.ask(call, Purpose::User(id), uuid);
        self.wrote_at = self.now;
        self.phase = Phase::Leading {
            uuid,
            term,
            write: Some(call),
        };
        self.assert_invariants();
        Ok(id)
    }

    /// Hand the term on: `SetLeader(next, me)` with a fresh uuid. On a win
    /// comes [`Event::HandedOn`]; the actor stops either way and watches
    /// again.
    ///
    /// # Errors
    ///
    /// [`Refusal::NotLeading`] when the actor does not lead, and
    /// [`Refusal::Busy`] while a write of it is in flight.
    pub fn hand_on<D: Draws>(&mut self, draws: &mut D) -> Result<(), Refusal> {
        let Phase::Leading { uuid, term, write } = self.phase else {
            return Err(Refusal::NotLeading);
        };
        if write.is_some() {
            return Err(Refusal::Busy);
        }
        let next = self.fresh(draws);
        let call = self
            .client
            .set_leader(Leader::uuid(next), Leader::uuid(uuid));
        self.ask(call, Purpose::Hand, uuid);
        self.phase = Phase::Handing { term, next, call };
        self.assert_invariants();
        Ok(())
    }

    /// Take the term that a holder handed on to this actor. The actor takes
    /// it only when it has no claim or term of its own and no call in
    /// flight; else the handed term has no holder, and a candidate claims
    /// the next one after a lease. Returns whether it took it.
    pub fn adopt(&mut self, handoff: Handoff) -> bool {
        if !matches!(self.phase, Phase::Standby | Phase::Watching { .. })
            || !self.client.calls_idle()
            || !handoff.uuid.is_set()
            || handoff.term.0 == 0
        {
            return false;
        }
        self.used.insert(handoff.uuid);
        self.client.adopt(handoff.uuid, handoff.next_seq);
        self.publish(handoff.uuid, handoff.term);
        self.assert_invariants();
        true
    }

    /// The requests to send now, in the order the client made them.
    #[must_use]
    pub fn take_requests(&mut self) -> Vec<Request> {
        self.client.take_requests()
    }

    /// The events since the last call, in order.
    #[must_use]
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    /// The term the actor leads and its uuid, once its term record is in
    /// the journal.
    #[must_use]
    pub fn leading(&self) -> Option<(Term, LeaderUuid)> {
        match self.phase {
            Phase::Leading { term, uuid, .. } => Some((term, uuid)),
            _ => None,
        }
    }

    /// Whether the actor is a candidate with no claim and no term: it may
    /// adopt a handed term.
    #[must_use]
    pub fn watching(&self) -> bool {
        matches!(self.phase, Phase::Watching { .. })
    }

    /// The last election record the actor read.
    #[must_use]
    pub fn holder(&self) -> Option<&ElectionRecord> {
        self.last.as_ref()
    }

    /// The journal client the election drives.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Check the invariants of the election.
    ///
    /// # Panics
    ///
    /// When an invariant does not hold: a bug of the election.
    pub fn assert_invariants(&self) {
        self.client.assert_invariants();
        let own_calls = self
            .calls
            .values()
            .filter(|a| !matches!(a.purpose, Purpose::User(_)))
            .count();
        assert!(own_calls <= 1, "the election has one call of its own out");
        match self.phase {
            Phase::Claiming { uuid, call } => {
                assert!(self.used.contains(&uuid), "a claim names a drawn uuid");
                assert!(self.calls.contains_key(&call), "the claim is out");
            }
            Phase::Publishing { call, term, .. } => {
                assert!(term.0 > 0, "a term has a number");
                assert!(self.calls.contains_key(&call), "the term record is out");
            }
            Phase::Leading { term, write, .. } => {
                assert!(term.0 > 0, "a term has a number");
                assert!(
                    write.is_none_or(|call| self.calls.contains_key(&call)),
                    "the write in flight is a call"
                );
            }
            Phase::Handing { call, term, next } => {
                assert!(term.0 > 0, "a term has a number");
                assert!(self.used.contains(&next), "a hand on names a drawn uuid");
                assert!(self.calls.contains_key(&call), "the hand on is out");
            }
            Phase::Standby | Phase::Watching { .. } | Phase::Folding { .. } => {}
        }
        if let Some((_, from)) = self.read {
            assert!(from == self.cursor, "a read starts at the cursor");
        }
    }

    /// The term the actor won and has not lost, if any.
    fn term(&self) -> Option<Term> {
        match self.phase {
            Phase::Publishing { term, .. }
            | Phase::Leading { term, .. }
            | Phase::Handing { term, .. } => Some(term),
            _ => None,
        }
    }

    fn ask(&mut self, call: CallId, purpose: Purpose, uuid: LeaderUuid) {
        let previous = self.calls.insert(call, Asked { purpose, uuid });
        assert!(previous.is_none(), "a call id names one call");
    }

    /// A uuid that no actor drew: the program's draw, unless the election
    /// saw it.
    fn fresh<D: Draws>(&mut self, draws: &mut D) -> LeaderUuid {
        for _ in 0..16 {
            let uuid = draws.uuid();
            let seen = !uuid.is_set()
                || self.used.contains(&uuid)
                || self.sample.is_some_and(|s| s.leader == Leader::Uuid(uuid))
                || self.last.as_ref().is_some_and(|r| r.uuid == uuid);
            if !seen {
                self.used.insert(uuid);
                return uuid;
            }
        }
        panic!("the draws of the program gave 16 uuids in a row that the election saw");
    }

    /// Whether the actor reads the journal in its phase.
    fn reads(&self) -> bool {
        matches!(
            self.phase,
            Phase::Watching { .. } | Phase::Claiming { .. } | Phase::Folding { .. }
        )
    }

    /// Ask a read of the next records, when one is due: at once while the
    /// actor folds to its tail, else once a poll.
    fn maybe_read(&mut self) {
        if self.read.is_some() || !self.reads() {
            return;
        }
        let folding = matches!(self.phase, Phase::Folding { .. });
        let due = self
            .read_at
            .is_none_or(|at| self.now.saturating_sub(at) >= self.config.poll);
        if folding || due {
            let call = self.client.read(self.cursor, self.config.page);
            self.read = Some((call, self.cursor));
            self.read_at = Some(self.now);
        }
    }

    /// The candidate's step: claim once the holder was silent for a lease
    /// and the jitter passed.
    fn watch<D: Draws>(&mut self, claim_at: Option<u64>, draws: &mut D) {
        let Some(sample) = self.sample else {
            return;
        };
        let due = match sample.leader {
            Leader::Unset => true,
            Leader::Multi(_) => false,
            Leader::Uuid(_) => self.now.saturating_sub(sample.since) >= self.config.lease,
        };
        if !due {
            self.phase = Phase::Watching { claim_at: None };
            return;
        }
        let at = claim_at.unwrap_or_else(|| self.now + draws.jitter(self.config.jitter));
        if self.now < at {
            self.phase = Phase::Watching { claim_at: Some(at) };
            return;
        }
        let uuid = self.fresh(draws);
        let call = self.client.set_leader(Leader::uuid(uuid), sample.leader);
        self.ask(call, Purpose::Claim, uuid);
        self.phase = Phase::Claiming { uuid, call };
    }

    fn renew(&mut self, uuid: LeaderUuid, term: Term) {
        let record = ElectionRecord {
            kind: Kind::Renewal,
            term,
            uuid,
            interface: self.interface.clone(),
        };
        let call = self.client.write(vec![record.encode()]);
        self.ask(call, Purpose::Renew, uuid);
        self.wrote_at = self.now;
        self.phase = Phase::Leading {
            uuid,
            term,
            write: Some(call),
        };
    }

    /// Send the term record of `term` under `uuid`.
    fn publish(&mut self, uuid: LeaderUuid, term: Term) {
        let record = ElectionRecord {
            kind: Kind::Term,
            term,
            uuid,
            interface: self.interface.clone(),
        };
        let call = self.client.write(vec![record.encode()]);
        self.ask(call, Purpose::Publish, uuid);
        self.wrote_at = self.now;
        self.phase = Phase::Publishing { uuid, term, call };
    }

    /// The claim won and the fold reached its tail: the number is the last
    /// record's, plus one.
    fn finish_fold(&mut self) {
        let Phase::Folding { uuid, until } = self.phase else {
            unreachable!("only a fold to the tail finishes");
        };
        assert!(self.cursor >= until, "the fold reached the tail of the win");
        let term = self.last.as_ref().map_or(Term(0), |r| r.term).next();
        self.publish(uuid, term);
    }

    /// The view of a read or an answer: the holder is silent while it stays.
    fn observe(&mut self, view: JournalView) {
        let same = self
            .sample
            .is_some_and(|s| s.leader == view.leader && s.next == view.next_seq);
        if !same {
            self.sample = Some(Sample {
                leader: view.leader,
                next: view.next_seq,
                since: self.now,
            });
        }
        let multi = view.leader.is_multi();
        if multi && !self.multi {
            self.events.push(Event::MultiWriter);
        }
        self.multi = multi;
    }

    /// A refusal or a stop shows a holder that just changed, or a view that
    /// lagged: the candidate gives the holder a lease from now.
    fn restart_lease(&mut self) {
        if let Some(sample) = &mut self.sample {
            sample.since = self.now;
        }
        self.phase = Phase::Watching { claim_at: None };
    }

    fn fold_record(&mut self, seq: Seq, record: &Record) {
        if let Phase::Folding { until, .. } = self.phase
            && seq >= until
        {
            self.finish_fold();
        }
        let Some(found) = ElectionRecord::decode(record) else {
            return;
        };
        let new = self
            .last
            .as_ref()
            .is_none_or(|last| last.term != found.term || last.uuid != found.uuid);
        if new {
            self.events.push(Event::Holder {
                term: found.term,
                uuid: found.uuid,
                interface: found.interface.clone(),
            });
        }
        self.last = Some(found);
    }

    fn on_read(&mut self, outcome: Outcome) {
        let (_, asked_from) = self.read.take().expect("a read is out");
        match outcome {
            Outcome::Page {
                from,
                records,
                view,
                ..
            } => {
                assert!(from == asked_from, "a page starts where its read asked");
                for (i, record) in records.iter().enumerate() {
                    let seq = Seq(from.0 + i as u64);
                    self.fold_record(seq, record);
                    self.cursor = Seq(seq.0 + 1);
                }
                self.observe(view);
            }
            Outcome::Truncated(view) => {
                // The records below the floor are gone: the fold goes on
                // from the floor.
                self.cursor = self.cursor.max(view.first_seq);
                self.observe(view);
            }
            // A read ends with a page or below the floor; anything else is
            // data that tells nothing of the tail.
            _ => {}
        }
        if let Phase::Folding { until, .. } = self.phase
            && self.cursor >= until
        {
            self.finish_fold();
        }
        let behind = self.sample.is_some_and(|s| self.cursor < s.next);
        if behind || matches!(self.phase, Phase::Folding { .. }) {
            // Catch up at once: the tail is ahead of the fold.
            self.read_at = None;
        }
        self.maybe_read();
    }

    fn on_call(&mut self, call: CallId, asked: Asked, outcome: Outcome) {
        match asked.purpose {
            Purpose::Claim => self.on_claim(call, asked, outcome),
            Purpose::Publish => {
                let Phase::Publishing {
                    uuid,
                    term,
                    call: out,
                } = self.phase
                else {
                    return;
                };
                if out != call || uuid != asked.uuid {
                    return;
                }
                if matches!(outcome, Outcome::Written { .. }) {
                    self.phase = Phase::Leading {
                        uuid,
                        term,
                        write: None,
                    };
                    self.events.push(Event::Leading { term, uuid });
                } else {
                    self.events.push(Event::Deposed { term });
                    self.restart_lease();
                }
            }
            Purpose::Renew | Purpose::User(_) => {
                let written = match (&outcome, asked.purpose) {
                    (Outcome::Written { seq, count }, Purpose::User(id)) => {
                        self.events.push(Event::Wrote {
                            id,
                            seq: *seq,
                            count: *count,
                        });
                        true
                    }
                    (_, Purpose::User(id)) => {
                        self.events.push(Event::NotWritten { id });
                        false
                    }
                    (written, _) => matches!(written, Outcome::Written { .. }),
                };
                let Phase::Leading { uuid, term, write } = self.phase else {
                    return;
                };
                if write != Some(call) || uuid != asked.uuid {
                    return;
                }
                if written {
                    self.phase = Phase::Leading {
                        uuid,
                        term,
                        write: None,
                    };
                } else {
                    self.events.push(Event::Deposed { term });
                    self.restart_lease();
                }
            }
            Purpose::Hand => {
                let Phase::Handing {
                    term,
                    next,
                    call: out,
                } = self.phase
                else {
                    return;
                };
                if out != call {
                    return;
                }
                if let Outcome::LeaderSet(view) = outcome {
                    self.events.push(Event::HandedOn(Handoff {
                        uuid: next,
                        term: term.next(),
                        next_seq: view.next_seq,
                    }));
                    self.observe(view);
                } else {
                    self.events.push(Event::Deposed { term });
                }
                self.restart_lease();
            }
        }
    }

    fn on_claim(&mut self, call: CallId, asked: Asked, outcome: Outcome) {
        let Phase::Claiming { uuid, call: out } = self.phase else {
            return;
        };
        if out != call || uuid != asked.uuid {
            return;
        }
        match outcome {
            Outcome::LeaderSet(view) => {
                assert!(
                    view.leader == Leader::Uuid(uuid),
                    "the client reports a won SetLeader with its uuid"
                );
                self.observe(view);
                self.phase = Phase::Folding {
                    uuid,
                    until: view.next_seq,
                };
                if self.cursor >= view.next_seq {
                    self.finish_fold();
                } else {
                    self.maybe_read();
                }
            }
            Outcome::LeaderRefused(view) => {
                self.events.push(Event::Lost {
                    leader: view.leader,
                });
                self.observe(view);
                self.restart_lease();
            }
            other => {
                // The journal refused the call itself: nothing applied.
                let leader = match other {
                    Outcome::Refused(
                        Answer::Taken(view)
                        | Answer::Fenced(view)
                        | Answer::WrongMode(view)
                        | Answer::Empty(view),
                    ) => view.leader,
                    _ => self.sample.map_or(Leader::Unset, |s| s.leader),
                };
                self.events.push(Event::Lost { leader });
                self.restart_lease();
            }
        }
    }
}

#[cfg(test)]
mod tests;
