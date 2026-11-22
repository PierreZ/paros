//! The **journal client** (#1331 (the pure journal client),
//! `docs/journal/02-data-plane.md` §J2): the state machine that a writer or
//! a reader of one journal runs, with no network, no clock and no
//! randomness (Pierre, 2026-11-19: parosd handles the network).
//!
//! The program asks the client for one of the four calls ([`Client::write`],
//! [`Client::read`], [`Client::truncate`], [`Client::set_leader`]). The
//! client gives back [`Request`]s to send, the program sends each one and
//! gives its [`Response`] back with [`Client::answer`], and the client gives
//! the [`Outcome`] of each call with [`Client::take_outcomes`]. The client
//! settles every ambiguous answer itself:
//!
//! - **Exactly-once for a single writer** (§J2.2). A write in single-writer
//!   mode names its position, `expected_seq`. After an answer that does not
//!   tell the fate of the write (`Unknown`, a lost reply, a leader that holds
//!   no slot for it, `Early`), the client sends the identical write again,
//!   at the same `expected_seq`: the fold judges the position, and answers
//!   `Duplicate` when the first copy is there (#1232 (held writes)).
//! - **`Below`** (#1301 (a resent write across a seal)): the position is in
//!   a sealed segment. The client sends `Resolve(uuid, expected_seq, write)`
//!   to a replica of that segment (#1320). Written ends the call. Not covered
//!   asks again. Not written sends the write again at the tail that the
//!   `Below` named, under the same uuid, and the journal judges it. With
//!   fresh uuids and one call in flight, another write takes the position
//!   only once the writer was deposed, so that resend is answered `Fenced`
//!   (or `WrongMode`) and the call ends refused (`journal-sim`'s client walk
//!   reaches it).
//! - **`Truncated`** resolves nothing: the position is below the trim floor,
//!   and the call ends with [`Outcome::Truncated`], fate unknown.
//! - **At-least-once in multi-writer mode** (§J2.4). An unfenced write has
//!   no position, so after an ambiguous answer the client sends it again,
//!   and it can land two times. The writers own that.
//!
//! **One call in flight.** The client sends one write, truncate or
//! `SetLeader` at a time, in the order the program asked for them: the next
//! one leaves only when the call before it ended. So a single writer's
//! position is always the tail that the journal answered last, and a resend
//! never races a later write. Reads do not wait: each read is its own
//! request. Several writes in flight (the pipeline of #597) is not built.
//!
//! **The uuid is the caller's** (§J2.3). The client draws no random number:
//! the program draws a fresh uuid for each claim and passes it to
//! [`Client::set_leader`]. Once a `SetLeader` that names a uuid is applied,
//! every write of the client is fenced by it, until the next `SetLeader` of
//! the client. A client that never won a `SetLeader` writes unfenced: the
//! journal accepts that only while it is multi-writer.
//!
//! **A lost `SetLeader` settles itself.** A resend of a `SetLeader(new, old)`
//! whose first copy applied is refused, because `new` already leads. Uuids
//! are fresh, so a refusal whose view names `new` as the leader after a
//! resend means that the first copy won: the client answers
//! [`Outcome::LeaderSet`] with that view.
//!
//! **Record headers are opaque** (#1299 (record headers)). The client sends
//! the records as the program gave them, with their headers, and never reads
//! a header.
//!
//! **What a peer sends is data.** An answer that does not fit the call (a
//! `LeaderSet` for a write) ends the call with [`Outcome::Refused`]; it is
//! never a panic. A [`Response`] of the wrong kind for its request (a page
//! for a write) is a bug of the program that drives the client, and panics.

use std::collections::{BTreeMap, VecDeque};

use paros_consensus::journal::{LeaderUuid, Seq};

use crate::command::{Command, Fence, Leader, Record, Write};
use crate::fold::{Answer, JournalView};
use crate::held::Reply;
use crate::resolve::ResolveAnswer;

/// The id of one call that the program asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallId(pub u64);

/// The id of one request that the client asked the program to send. Every
/// copy of a call has its own id: the answer of an older copy is ignored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub u64);

/// A request for the program to send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Send `command` to the journal's consensus leader.
    Command {
        /// The id of this copy.
        id: RequestId,
        /// The command.
        command: Command,
    },
    /// Send `Resolve(uuid, expected_seq, records)` to a replica of the
    /// sealed segment that holds `expected_seq` (§J2.2, #1320).
    Resolve {
        /// The id of this copy.
        id: RequestId,
        /// The uuid of the write.
        uuid: LeaderUuid,
        /// The position of the write.
        expected_seq: Seq,
        /// The records of the write.
        records: Vec<Record>,
    },
    /// Send `Read(from, limit)` to a server of the journal (§J2.5).
    Read {
        /// The id of this copy.
        id: RequestId,
        /// The first position to read.
        from: Seq,
        /// The most records of the page.
        limit: u64,
    },
}

impl Request {
    /// The id of this copy.
    #[must_use]
    pub fn id(&self) -> RequestId {
        match self {
            Self::Command { id, .. } | Self::Resolve { id, .. } | Self::Read { id, .. } => *id,
        }
    }
}

/// The answer of a `Read` (§J2.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadAnswer {
    /// The committed records from the position asked, in order, and the
    /// journal's view.
    Page {
        /// The records.
        records: Vec<Record>,
        /// The view of the journal.
        view: JournalView,
    },
    /// The position asked is below `first_seq`.
    Truncated(JournalView),
    /// This server cannot give the page; ask another server.
    Unserved,
}

/// What came back for a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    /// The leader's reply to a [`Request::Command`].
    Reply(Reply),
    /// The role asked is not a leader that writes: the command holds no
    /// slot.
    NotLeader,
    /// No answer came: the request or its answer was lost, or the program
    /// stopped waiting.
    Lost,
    /// The replica's answer to a [`Request::Resolve`].
    Resolved(ResolveAnswer),
    /// The server's answer to a [`Request::Read`].
    Read(ReadAnswer),
}

/// How a call ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The write is in the journal, at `[seq, seq + count)`. Final.
    Written {
        /// The position of the first record.
        seq: Seq,
        /// How many records.
        count: u64,
    },
    /// The call is refused for ever: the answer of the journal (`Fenced`,
    /// `Taken`, `WrongMode`, `Empty`, `Reserved`, or an answer that does not
    /// fit the call). A refused write is not in the journal.
    Refused(Answer),
    /// The position is below the trim floor. For a write the fate is
    /// unknown; a read has no record there.
    Truncated(JournalView),
    /// The `SetLeader` applied: the view after it.
    LeaderSet(JournalView),
    /// The `SetLeader` lost the compare-and-set: the view it was judged
    /// against.
    LeaderRefused(JournalView),
    /// The `Truncate` applied: the view after it.
    Trimmed(JournalView),
    /// A page of the journal from `from`.
    Page {
        /// The position of the first record of the page.
        from: Seq,
        /// The records.
        records: Vec<Record>,
        /// The view of the journal.
        view: JournalView,
    },
}

/// How the client writes now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Unfenced writes: the client won no `SetLeader` that names a uuid, or
    /// its last one made the journal multi-writer.
    Open,
    /// Fenced writes under `uuid`, the next one at `next`.
    Fenced {
        /// The uuid the client writes under.
        uuid: LeaderUuid,
        /// The position of the client's next write.
        next: Seq,
    },
}

/// A call that waits for its turn.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Write(Vec<Record>),
    Truncate(Seq),
    SetLeader {
        new: Option<LeaderUuid>,
        old: Leader,
    },
}

/// Where the call in flight is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Stage {
    /// A copy of `command` is out.
    Sent { command: Command, copies: u32 },
    /// The write `write` was answered `Below` with `view`; a `Resolve` is
    /// out.
    Resolving { write: Write, view: JournalView },
}

/// The call in flight.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Flight {
    call: CallId,
    request: RequestId,
    stage: Stage,
}

/// A read in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Reading {
    call: CallId,
    from: Seq,
    limit: u64,
}

/// The client of one journal.
#[derive(Clone, Debug)]
pub struct Client {
    mode: Mode,
    next_call: u64,
    next_request: u64,
    waiting: VecDeque<(CallId, Call)>,
    flight: Option<Flight>,
    reads: BTreeMap<RequestId, Reading>,
    outbox: Vec<Request>,
    outcomes: Vec<(CallId, Outcome)>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    /// A client that writes unfenced until it wins a `SetLeader` that names
    /// a uuid.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn new() -> Self {
        let client = Self {
            mode: Mode::Open,
            next_call: 0,
            next_request: 0,
            waiting: VecDeque::new(),
            flight: None,
            reads: BTreeMap::new(),
            outbox: Vec::new(),
            outcomes: Vec::new(),
        };
        client.assert_invariants();
        assert!(client.idle(), "a new client has nothing in flight");
        client
    }

    /// How the client writes now.
    #[must_use]
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Whether the client has no call and no read in flight or waiting.
    #[must_use]
    pub fn idle(&self) -> bool {
        self.flight.is_none() && self.waiting.is_empty() && self.reads.is_empty()
    }

    /// Write `records` as one write: fenced at the client's next position
    /// in [`Mode::Fenced`], unfenced in [`Mode::Open`]. The mode is the one
    /// when the write leaves, after the calls asked before it.
    pub fn write(&mut self, records: Vec<Record>) -> CallId {
        self.ask(Call::Write(records))
    }

    /// `Truncate(up_to)`: fenced by the client's uuid in [`Mode::Fenced`],
    /// unfenced in [`Mode::Open`].
    pub fn truncate(&mut self, up_to: Seq) -> CallId {
        self.ask(Call::Truncate(up_to))
    }

    /// `SetLeader(new, old)`: a compare-and-set of the leader value.
    /// `new = None` makes the journal multi-writer. `new` is a fresh uuid
    /// that the program drew (§J2.3).
    pub fn set_leader(&mut self, new: Option<LeaderUuid>, old: Leader) -> CallId {
        self.ask(Call::SetLeader { new, old })
    }

    /// `Read(from, limit)`. It leaves at once, beside the call in flight.
    ///
    /// # Panics
    ///
    /// When `limit` is zero: a read of no record is a bug of the program.
    pub fn read(&mut self, from: Seq, limit: u64) -> CallId {
        assert!(limit > 0, "a read asks for at least one record");
        let call = self.new_call();
        let reading = Reading { call, from, limit };
        self.send_read(reading);
        assert!(
            self.reads.values().any(|r| r.call == call),
            "the read is in flight"
        );
        call
    }

    /// The requests to send now, in the order the client made them.
    #[must_use]
    pub fn take_requests(&mut self) -> Vec<Request> {
        std::mem::take(&mut self.outbox)
    }

    /// The calls that ended, in the order they ended.
    #[must_use]
    pub fn take_outcomes(&mut self) -> Vec<(CallId, Outcome)> {
        std::mem::take(&mut self.outcomes)
    }

    /// The `response` to request `id`. The answer of a copy that the client
    /// no longer waits for is ignored: a later copy is out, or its call
    /// ended.
    ///
    /// # Panics
    ///
    /// When `response` is of the wrong kind for the request: a page for a
    /// write, a reply for a read. That is a bug of the program that drives
    /// the client.
    pub fn answer(&mut self, id: RequestId, response: Response) {
        self.assert_invariants();
        if let Some(reading) = self.reads.remove(&id) {
            self.answer_read(reading, response);
        } else if self.flight.as_ref().is_some_and(|f| f.request == id) {
            let flight = self.flight.take().expect("the call in flight");
            match flight.stage {
                Stage::Sent { command, copies } => {
                    self.answer_command(flight.call, command, copies, response);
                }
                Stage::Resolving { write, view } => {
                    self.answer_resolve(flight.call, write, view, &response);
                }
            }
            self.start_next();
        }
        self.assert_invariants();
    }

    /// Check the client's invariants.
    ///
    /// # Panics
    ///
    /// When an invariant does not hold: a bug of the client.
    pub fn assert_invariants(&self) {
        assert!(
            self.flight.is_some() || self.waiting.is_empty(),
            "a call waits only behind a call in flight"
        );
        if let Some(flight) = &self.flight {
            assert!(
                flight.request.0 < self.next_request,
                "the request in flight has a given id"
            );
            assert!(
                !self.reads.contains_key(&flight.request),
                "a request id names one request"
            );
            assert!(flight.call.0 < self.next_call, "the call has a given id");
            if let Stage::Sent { copies, .. } = &flight.stage {
                assert!(*copies > 0, "a sent call has a copy out");
            }
        }
        assert!(
            self.reads.keys().all(|id| id.0 < self.next_request),
            "every read has a given id"
        );
    }

    fn new_call(&mut self) -> CallId {
        let call = CallId(self.next_call);
        self.next_call += 1;
        assert!(self.next_call > call.0, "call ids only grow");
        call
    }

    fn new_request(&mut self) -> RequestId {
        let id = RequestId(self.next_request);
        self.next_request += 1;
        assert!(self.next_request > id.0, "request ids only grow");
        id
    }

    fn ask(&mut self, call: Call) -> CallId {
        let id = self.new_call();
        self.waiting.push_back((id, call));
        if self.flight.is_none() {
            self.start_next();
        }
        assert!(self.flight.is_some(), "a call is in flight after an ask");
        self.assert_invariants();
        id
    }

    /// Send the next waiting call, if no call is in flight.
    fn start_next(&mut self) {
        if self.flight.is_some() {
            return;
        }
        let Some((call, waiting)) = self.waiting.pop_front() else {
            return;
        };
        let command = match waiting {
            Call::Write(records) => Command::Write(Write {
                fence: match self.mode {
                    Mode::Open => Fence::Open,
                    Mode::Fenced { uuid, next } => Fence::Leader {
                        uuid,
                        expected_seq: next,
                    },
                },
                records,
            }),
            Call::Truncate(up_to) => Command::Truncate {
                fence: match self.mode {
                    Mode::Open => None,
                    Mode::Fenced { uuid, .. } => Some(uuid),
                },
                up_to,
            },
            Call::SetLeader { new, old } => Command::SetLeader { new, old },
        };
        self.send(call, command, 1);
        assert!(self.flight.is_some(), "the call is in flight");
    }

    /// Send copy `copies` of `command` for `call`.
    fn send(&mut self, call: CallId, command: Command, copies: u32) {
        assert!(self.flight.is_none(), "one call in flight");
        assert!(copies > 0, "a copy counts from one");
        let request = self.new_request();
        self.outbox.push(Request::Command {
            id: request,
            command: command.clone(),
        });
        self.flight = Some(Flight {
            call,
            request,
            stage: Stage::Sent { command, copies },
        });
    }

    /// Send `command` again, after `copies` copies.
    fn resend(&mut self, call: CallId, command: Command, copies: u32) {
        self.send(call, command, copies + 1);
    }

    fn send_read(&mut self, reading: Reading) {
        let id = self.new_request();
        self.outbox.push(Request::Read {
            id,
            from: reading.from,
            limit: reading.limit,
        });
        let previous = self.reads.insert(id, reading);
        assert!(previous.is_none(), "a request id names one read");
    }

    fn send_resolve(&mut self, call: CallId, write: Write, view: JournalView) {
        let Fence::Leader { uuid, expected_seq } = write.fence else {
            unreachable!("only a fenced write is answered Below");
        };
        let request = self.new_request();
        self.outbox.push(Request::Resolve {
            id: request,
            uuid,
            expected_seq,
            records: write.records.clone(),
        });
        self.flight = Some(Flight {
            call,
            request,
            stage: Stage::Resolving { write, view },
        });
    }

    fn end(&mut self, call: CallId, outcome: Outcome) {
        assert!(
            self.outcomes.iter().all(|(c, _)| *c != call),
            "a call ends once"
        );
        self.outcomes.push((call, outcome));
    }

    /// Follow `view` while the client writes under the uuid it names: the
    /// tail of the journal is the client's next position.
    fn follow(&mut self, view: JournalView) {
        if let Mode::Fenced { uuid, next } = &mut self.mode
            && view.leader == Leader::Uuid(*uuid)
        {
            *next = view.next_seq;
        }
    }

    fn answer_command(&mut self, call: CallId, command: Command, copies: u32, response: Response) {
        let answer = match response {
            Response::Reply(Reply::Answer(answer)) => answer,
            Response::Reply(Reply::Unknown) | Response::NotLeader | Response::Lost => {
                // The fate is unknown, or the command holds no slot: the
                // identical command again. A fenced write is judged at its
                // position; an unfenced one can land two times.
                self.resend(call, command, copies);
                return;
            }
            Response::Resolved(_) | Response::Read(_) => {
                panic!("a command is answered by a reply: {response:?}")
            }
        };
        match command {
            Command::Write(write) => self.answer_write(call, write, copies, answer),
            Command::SetLeader { new, .. } => self.answer_set_leader(call, new, copies, answer),
            Command::Truncate { .. } => {
                let outcome = match answer {
                    Answer::Trimmed(view) => Outcome::Trimmed(view),
                    other => Outcome::Refused(other),
                };
                self.end(call, outcome);
            }
            Command::Noop | Command::Upgrade { .. } => {
                unreachable!("the client sends no Noop and no Upgrade")
            }
        }
    }

    fn answer_write(&mut self, call: CallId, write: Write, copies: u32, answer: Answer) {
        match answer {
            Answer::Written { seq, count } | Answer::Duplicate { seq, count } => {
                if let (Mode::Fenced { uuid, next }, Fence::Leader { uuid: fence, .. }) =
                    (&mut self.mode, write.fence)
                    && *uuid == fence
                {
                    *next = Seq(seq.0 + count);
                }
                self.end(call, Outcome::Written { seq, count });
            }
            Answer::Early(_) => self.resend(call, Command::Write(write), copies),
            Answer::Below(view) => self.send_resolve(call, write, view),
            Answer::Truncated(view) => self.end(call, Outcome::Truncated(view)),
            refused @ (Answer::Taken(view)
            | Answer::Fenced(view)
            | Answer::WrongMode(view)
            | Answer::Empty(view)) => {
                self.follow(view);
                self.end(call, Outcome::Refused(refused));
            }
            other => self.end(call, Outcome::Refused(other)),
        }
    }

    fn answer_set_leader(
        &mut self,
        call: CallId,
        new: Option<LeaderUuid>,
        copies: u32,
        answer: Answer,
    ) {
        let won = match &answer {
            Answer::LeaderSet(view) => Some(*view),
            // A resend whose first copy won: `new` leads, and uuids are
            // fresh, so no other call installed it.
            Answer::LeaderRefused(view)
                if copies > 1
                    && new.is_none_or(LeaderUuid::is_set)
                    && view.leader == Leader::from_new(new) =>
            {
                Some(*view)
            }
            _ => None,
        };
        let outcome = match (won, answer) {
            (Some(view), _) => {
                self.mode = match new {
                    Some(uuid) => Mode::Fenced {
                        uuid,
                        next: view.next_seq,
                    },
                    None => Mode::Open,
                };
                Outcome::LeaderSet(view)
            }
            (None, Answer::LeaderRefused(view)) => Outcome::LeaderRefused(view),
            (None, other) => Outcome::Refused(other),
        };
        self.end(call, outcome);
    }

    fn answer_resolve(
        &mut self,
        call: CallId,
        write: Write,
        view: JournalView,
        response: &Response,
    ) {
        match *response {
            Response::Resolved(ResolveAnswer::Written { seq, count }) => {
                self.follow(JournalView {
                    next_seq: Seq(seq.0 + count),
                    ..view
                });
                self.end(call, Outcome::Written { seq, count });
            }
            Response::Resolved(ResolveAnswer::NotWritten) => {
                // Not in the sealed segment, for ever: the same records
                // again, at the tail that the `Below` named.
                let Fence::Leader { uuid, .. } = write.fence else {
                    unreachable!("only a fenced write is answered Below");
                };
                let again = Write {
                    fence: Fence::Leader {
                        uuid,
                        expected_seq: view.next_seq,
                    },
                    records: write.records,
                };
                self.send(call, Command::Write(again), 1);
            }
            Response::Resolved(ResolveAnswer::Truncated) => {
                self.end(call, Outcome::Truncated(view));
            }
            Response::Resolved(ResolveAnswer::NotCovered)
            | Response::NotLeader
            | Response::Lost => {
                self.send_resolve(call, write, view);
            }
            Response::Reply(_) | Response::Read(_) => {
                panic!("a Resolve is answered by a replica: {response:?}")
            }
        }
    }

    fn answer_read(&mut self, reading: Reading, response: Response) {
        match response {
            Response::Read(ReadAnswer::Page { records, view }) => self.end(
                reading.call,
                Outcome::Page {
                    from: reading.from,
                    records,
                    view,
                },
            ),
            Response::Read(ReadAnswer::Truncated(view)) => {
                self.end(reading.call, Outcome::Truncated(view));
            }
            Response::Read(ReadAnswer::Unserved) | Response::NotLeader | Response::Lost => {
                self.send_read(reading);
            }
            Response::Reply(_) | Response::Resolved(_) => {
                panic!("a read is answered by a page: {response:?}")
            }
        }
    }
}

#[cfg(test)]
mod tests;
