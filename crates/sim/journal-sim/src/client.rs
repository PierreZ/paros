//! The pure journal client under the walk (#1331 (the pure journal
//! client), `docs/journal/02-data-plane.md` §J2).
//!
//! Two programs, each with one [`Client`] of `paros_journal::client`, use
//! one journal. They claim it with fresh uuids (and depose each other),
//! make it multi-writer and back, write batches of records with opaque
//! headers, truncate and read. Every request and every answer goes through
//! the walk's mailbox: lost, repeated and reordered. The journal answers
//! from the fold of its segment in force (`paros_journal::JournalState`),
//! and the walk seals that segment, so a resend across a seal is answered
//! `Below` and settled with `Resolve` at the sealed segment. A command can
//! apply and lose its answer, apply and be answered `Unknown` (a deposed
//! leader held it), take no slot and be answered `Unknown` (its slot chose a
//! `Noop`) or `NotLeader`. A replica can lag (`Resolve` not covered) and a
//! read can be unserved. A program that waits too long gives its client
//! `Lost`.
//!
//! The oracles judge what each client reports against the whole log of the
//! journal, which the walk keeps itself (each record by a unique id in its
//! payload). They read no state of the client:
//!
//! - [`WRITTEN`]: a write reported written holds its positions, with its
//!   records in order.
//! - [`ONCE`]: a fenced write lands at most once, whatever the resends
//!   (exactly-once for a single writer, §J2.2).
//! - [`REFUSED`]: a fenced write reported refused is never in the log. (An
//!   unfenced write is at-least-once: a copy refused `WrongMode` says
//!   nothing of an older copy still in flight, §J2.4.)
//! - [`LED`]: a `SetLeader` reported set applied: a copy of that call won
//!   (a uuid or a multi tag is drawn once, so one `SetLeader` names it).
//! - [`TAIL`]: the first copy of a fenced write of the uuid that leads
//!   names the journal's tail: a sole writer's client follows the tail, so
//!   it never writes below it or above it.
//! - [`PAGE`]: a page holds the log's records at its positions.
//! - [`FLOOR`]: a page holds no record below the trim floor that the
//!   journal had when its read was asked. The journal answers each read
//!   from the segment that holds its position, and a sealed segment folds
//!   only up to its seal, so it still serves records that a later
//!   `Truncate` cut (§J3.2): the client must take its floor from the
//!   segment in force.
//!
//! The liveness claim: once the faults stop, every call of every client
//! ends.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};
use paros_journal::client::{CallId, Client, Outcome, ReadAnswer, Request, RequestId, Response};
use paros_journal::resolve::ResolveAnswer;
use paros_journal::{
    Answer, BoundaryState, Command, Fence, Header, Headers, JournalState, JournalView, Leader,
    LeaderUuid, Record, Reply, Seq, Write, resolve,
};

/// Oracle: a write reported written is in the log at its positions.
pub const WRITTEN: &str = "client walk: a write reported written holds its positions";
/// Oracle: a fenced write lands at most once.
pub const ONCE: &str = "client walk: a fenced write lands at most once";
/// Oracle: a fenced write reported refused is never in the log.
pub const REFUSED: &str = "client walk: a fenced write reported refused is never in the log";
/// Oracle: a `SetLeader` reported set applied.
pub const LED: &str = "client walk: a SetLeader reported set applied";
/// Oracle: a sole writer's new write names the tail.
pub const TAIL: &str = "client walk: a leading writer's new write names the tail";
/// Oracle: a page holds the log's records.
pub const PAGE: &str = "client walk: a page holds the log's records at its positions";
/// Oracle: a page holds no record below the floor of the journal.
pub const FLOOR: &str = "client walk: a page holds no record below the floor of the journal";

/// The coverage sites of the client walk.
pub mod reach {
    /// A resend answered `Duplicate` ended the write as written.
    pub const DUPLICATE: &str = "client walk: a resend answered Duplicate";
    /// A `Below` resolved written.
    pub const RESOLVED_WRITTEN: &str = "client walk: a Below resolved written";
    /// A `Below` resolved not written, sent again at the tail and refused:
    /// with fresh uuids, another write took the position only after the
    /// writer was deposed.
    pub const RESOLVED_AGAIN: &str = "client walk: a Below not written, refused at the tail";
    /// A write ended `Truncated`.
    pub const TRUNCATED: &str = "client walk: a write ended Truncated";
    /// A fenced write of a deposed writer refused.
    pub const DEPOSED: &str = "client walk: a deposed writer's write refused";
    /// A resent `SetLeader` refused because its first copy won.
    pub const LOST_WIN: &str = "client walk: a SetLeader resend refused, its first copy won";
    /// An unfenced write that landed two times.
    pub const TWICE: &str = "client walk: an unfenced write landed two times";
    /// A batch of several records written.
    pub const BATCH: &str = "client walk: a batch of several records written";
    /// A page read.
    pub const PAGE: &str = "client walk: a page read";
    /// A read below the floor.
    pub const READ_TRUNCATED: &str = "client walk: a read below the floor";
    /// A read below the floor of the segment in force, at a position that
    /// the sealed segment which holds it still serves.
    pub const FLOOR_REFUSED: &str = "client walk: a read below the floor its holder kept";
    /// A truncate applied.
    pub const TRIMMED: &str = "client walk: a truncate applied";
    /// A leading writer's new write after the journal answered one of its
    /// writes `Truncated`.
    pub const AFTER_TRUNCATED: &str = "client walk: a leader's new write after a Truncated";
    /// A write written in the third segment.
    pub const THIRD: &str = "client walk: a write written in the third segment";

    /// Every site.
    pub const ALL: &[&str] = &[
        DUPLICATE,
        RESOLVED_WRITTEN,
        RESOLVED_AGAIN,
        TRUNCATED,
        DEPOSED,
        LOST_WIN,
        TWICE,
        BATCH,
        PAGE,
        READ_TRUNCATED,
        FLOOR_REFUSED,
        TRIMMED,
        THIRD,
        AFTER_TRUNCATED,
    ];
}

/// A bug planted in the program that drives the clients, or in the
/// journal it talks to: never in the client.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClientMutant {
    /// No bug.
    #[default]
    None,
    /// The journal places a resent fenced write at its tail, not at the
    /// position it names. [`ONCE`] catches it.
    ResendAtTail,
    /// The program takes a timeout as a refusal. [`REFUSED`] catches it.
    LostAsFenced,
    /// The replica answers every `Resolve` written. [`WRITTEN`] catches it.
    ResolveWritten,
    /// The program shows a refused `SetLeader` resend as naming its uuid.
    /// [`LED`] catches it.
    ForgedRefusal,
    /// The server reads one position late. [`PAGE`] catches it.
    PageLate,
    /// The journal answers a `SetLeader` that applied with a view one
    /// position short. [`TAIL`] catches it.
    ShortView,
    /// The program asks the floor of the oldest segment, not of the segment
    /// in force. [`FLOOR`] catches it.
    FloorFromOldest,
}

/// A message in the walk's mailbox.
#[derive(Clone, Debug)]
pub enum ClientMsg {
    /// A request of client `0` to the journal.
    Request(usize, Request),
    /// The answer of request `1` to client `0`.
    Response(usize, RequestId, Response),
}

/// The moves of the client walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientAction {
    /// A client claims the journal with a fresh uuid.
    Claim,
    /// A client makes the journal multi-writer.
    ToMulti,
    /// A client writes a batch.
    Write,
    /// A client truncates.
    Truncate,
    /// A client reads.
    Read,
    /// The segment in force is sealed.
    Seal,
    /// A program stops waiting for one request: `Lost`.
    Timeout,
    /// A very late copy of an old command reaches the journal; nobody
    /// waits for its answer.
    Replay,
}

/// The clients of the walk.
const CLIENTS: usize = 2;
/// The most segments of one walk.
const SEGMENTS: usize = 6;
/// The most calls a program has open at once.
const OPEN_CALLS: usize = 4;

/// A call that a program asked for, as the walk knows it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Asked {
    Write(Vec<u64>),
    SetLeader(Leader),
    Truncate,
    /// A read of `limit` records from `from`, asked when the floor of the
    /// journal was `floor`.
    Read {
        from: Seq,
        limit: u64,
        floor: Seq,
    },
}

/// A segment of the journal: its fold and the fenced writes it accepted.
#[derive(Clone, Debug)]
struct Segment {
    base: BoundaryState,
    fold: JournalState,
    positions: BTreeMap<Seq, Write>,
}

impl Segment {
    fn new(base: BoundaryState) -> Self {
        Self {
            base,
            fold: JournalState::from_boundary(base),
            positions: BTreeMap::new(),
        }
    }
}

/// A program and its client.
#[derive(Clone, Debug)]
struct Program {
    client: Client,
    calls: BTreeMap<CallId, Asked>,
}

/// The client walk.
pub struct ClientModel {
    mutant: ClientMutant,
    segments: Vec<Segment>,
    programs: Vec<Program>,
    /// The whole log: each position's record id and record.
    log: BTreeMap<u64, (u64, Record)>,
    /// How many times each record id is in the log.
    landed: BTreeMap<u64, u32>,
    /// The record ids of every fenced write the journal saw.
    fenced: BTreeSet<u64>,
    /// The record ids of the fenced writes reported refused.
    refused: BTreeSet<u64>,
    /// The ids (uuids and multi tags, drawn from one counter) that a
    /// `SetLeader` installed.
    installed: BTreeSet<u128>,
    /// The requests whose answer a program still waits for, and whether
    /// each is a command.
    waiting: BTreeMap<(usize, RequestId), bool>,
    /// The fenced writes the journal already saw, by record ids.
    placed: BTreeSet<Vec<u64>>,
    /// The writes answered `Below` and not resolved yet, by record ids.
    belows: BTreeSet<Vec<u64>>,
    /// The writes resolved not written.
    again: BTreeSet<Vec<u64>>,
    /// The last commands sent, for late copies.
    sent: Vec<(usize, Command)>,
    /// Whether the command applied now is a late copy that nobody waits
    /// for.
    replaying: bool,
    /// For each client, whether the journal answered a write of the uuid
    /// that leads `Truncated` since its last new write.
    truncated: Vec<bool>,
    /// The last id drawn: a uuid or a multi tag.
    uuids: u128,
    /// The leader value of the last `SetLeader` asked for.
    drawn: Leader,
    /// The last record id drawn.
    ids: u64,
}

impl Model for ClientModel {
    type Msg = ClientMsg;
    type Node = ();
    type Action = ClientAction;
    type Event = ();

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(ClientMutant::None, cx)
    }

    fn actions() -> &'static [ClientAction] {
        &[
            ClientAction::Claim,
            ClientAction::ToMulti,
            ClientAction::Write,
            ClientAction::Truncate,
            ClientAction::Read,
            ClientAction::Seal,
            ClientAction::Timeout,
            ClientAction::Replay,
        ]
    }

    fn weight(action: ClientAction, phase: Phase) -> u32 {
        match (phase, action) {
            (_, ClientAction::Timeout)
            | (Phase::Chaos, ClientAction::Seal | ClientAction::Replay) => 2,
            (Phase::Chaos, ClientAction::Write) => 12,
            (Phase::Chaos, ClientAction::Claim | ClientAction::Read) => 3,
            (Phase::Chaos, _) => 1,
            _ => 0,
        }
    }

    fn act(&mut self, action: ClientAction, cx: &mut Cx<'_, Self>) {
        if action == ClientAction::Timeout {
            self.timeout(cx);
            return;
        }
        if action == ClientAction::Replay {
            // On one move in two, the last truncate sent under the leader
            // value of the journal: a late copy of it applies, and can cut a
            // write that the leader resends.
            let leader = match self.current().fold.leader() {
                Leader::Uuid(uuid) => Some(uuid),
                _ => None,
            };
            let last_truncate = self
                .sent
                .iter()
                .rev()
                .find(|(_, c)| matches!(c, Command::Truncate { fence, .. } if *fence == leader))
                .cloned();
            let late = if cx.rng().chance(1, 2) && last_truncate.is_some() {
                last_truncate
            } else {
                cx.rng().pick(&self.sent).cloned()
            };
            if let Some((index, command)) = late {
                self.replaying = true;
                let _ = self.apply(index, &command);
                self.replaying = false;
            }
            return;
        }
        if action == ClientAction::Seal {
            if self.segments.len() < SEGMENTS {
                let boundary = self.current().fold.boundary();
                self.segments.push(Segment::new(boundary));
            }
            return;
        }
        let index = cx.rng().index(CLIENTS).expect("clients");
        if self.programs[index].calls.len() >= OPEN_CALLS {
            return;
        }
        let leader = self.current().fold.leader();
        let next = self.current().fold.next_seq().0;
        let (call, asked) = match action {
            ClientAction::Claim | ClientAction::ToMulti => {
                // A uuid or a multi tag, fresh: the program draws it (§J2.3,
                // §J2.4.1).
                self.uuids += 1;
                let id = LeaderUuid(self.uuids);
                let new = if action == ClientAction::Claim {
                    Leader::uuid(id)
                } else {
                    Leader::multi(id)
                };
                self.drawn = new;
                let old = self.old_leader(leader, cx);
                let client = &mut self.programs[index].client;
                (client.set_leader(new, old), Asked::SetLeader(new))
            }
            ClientAction::Write => {
                let (ids, records) = self.records(cx);
                let client = &mut self.programs[index].client;
                (client.write(records), Asked::Write(ids))
            }
            ClientAction::Truncate => {
                // On one call in two, one past the tail: a late copy of it
                // then truncates a write sent after it.
                let up_to = if cx.rng().chance(1, 2) {
                    Seq(next + 1)
                } else {
                    Seq(cx.rng().below(next + 2))
                };
                let client = &mut self.programs[index].client;
                (client.truncate(up_to), Asked::Truncate)
            }
            ClientAction::Read => {
                let from = Seq(cx.rng().below(next + 2));
                let limit = 1 + cx.rng().below(4);
                let floor = self.current().fold.first_seq();
                let client = &mut self.programs[index].client;
                (client.read(from, limit), Asked::Read { from, limit, floor })
            }
            ClientAction::Seal | ClientAction::Timeout | ClientAction::Replay => {
                unreachable!("handled above")
            }
        };
        self.programs[index].calls.insert(call, asked);
        self.flush(index, cx);
    }

    fn deliver(&mut self, message: ClientMsg, cx: &mut Cx<'_, Self>) {
        match message {
            ClientMsg::Request(index, request) => self.serve(index, request, cx),
            ClientMsg::Response(index, id, response) => {
                self.waiting.remove(&(index, id));
                self.programs[index].client.answer(id, response);
                self.flush(index, cx);
            }
        }
    }

    fn check(&mut self) {
        for id in &self.refused {
            assert!(
                !self.landed.contains_key(id),
                "{REFUSED}: record {id} is in the log"
            );
        }
    }

    fn converged(&self) -> Result<(), String> {
        let open: usize = self.programs.iter().map(|p| p.calls.len()).sum();
        if open == 0 {
            assert!(
                self.programs.iter().all(|p| p.client.idle()),
                "a client with no open call is idle"
            );
            Ok(())
        } else {
            Err(format!("{open} calls have not ended"))
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} segments, {} records, view {:?}, waiting {:?}",
            self.segments.len(),
            self.log.len(),
            self.current().fold.view(),
            self.waiting
        )
    }
}

impl ClientModel {
    /// The walk with `mutant` planted. The fresh journal is unset, or
    /// multi-writer on one seed in four.
    pub fn build(mutant: ClientMutant, cx: &mut Cx<'_, Self>) -> Self {
        // A multi-writer fresh journal takes the tag of its configuration:
        // the first id of the walk.
        let fresh = if cx.rng().chance(1, 4) {
            Leader::multi(LeaderUuid(1))
        } else {
            Leader::Unset
        };
        let uuids = u128::from(fresh.is_multi());
        Self {
            mutant,
            segments: vec![Segment::new(BoundaryState::fresh(fresh))],
            programs: vec![
                Program {
                    client: Client::new(),
                    calls: BTreeMap::new(),
                };
                CLIENTS
            ],
            log: BTreeMap::new(),
            landed: BTreeMap::new(),
            fenced: BTreeSet::new(),
            refused: BTreeSet::new(),
            installed: BTreeSet::new(),
            sent: Vec::new(),
            replaying: false,
            truncated: vec![false; CLIENTS],
            waiting: BTreeMap::new(),
            placed: BTreeSet::new(),
            belows: BTreeSet::new(),
            again: BTreeSet::new(),
            uuids,
            drawn: fresh,
            ids: 0,
        }
    }

    fn current(&self) -> &Segment {
        self.segments.last().expect("a journal has a segment")
    }

    fn current_mut(&mut self) -> &mut Segment {
        self.segments.last_mut().expect("a journal has a segment")
    }

    /// The leader value a client names as `old`: the journal's, or on one
    /// call in four a stale one.
    fn old_leader(&self, leader: Leader, cx: &mut Cx<'_, Self>) -> Leader {
        if cx.rng().chance(3, 4) || self.uuids == 0 {
            leader
        } else {
            match cx.rng().below(3) {
                0 => Leader::Unset,
                // A mode named by the id drawn before the call: often an
                // earlier mode, sometimes the one in force.
                1 => Leader::multi(LeaderUuid(self.uuids.saturating_sub(1).max(1))),
                _ => Leader::uuid(LeaderUuid(
                    1 + u128::from(cx.rng().below(u64::try_from(self.uuids).expect("few"))),
                )),
            }
        }
    }

    /// A batch of one to three records, each with a fresh id in its
    /// payload, some with an opaque client header.
    fn records(&mut self, cx: &mut Cx<'_, Self>) -> (Vec<u64>, Vec<Record>) {
        let count = 1 + cx.rng().below(3);
        let mut ids = Vec::new();
        let mut records = Vec::new();
        for _ in 0..count {
            self.ids += 1;
            ids.push(self.ids);
            let client = if cx.rng().chance(1, 3) {
                vec![Header {
                    key: b"k".to_vec(),
                    value: self.ids.to_le_bytes().to_vec(),
                }]
            } else {
                Vec::new()
            };
            records.push(Record {
                headers: Headers {
                    leader_time: None,
                    client,
                },
                payload: self.ids.to_le_bytes().to_vec(),
            });
        }
        (ids, records)
    }

    fn id_of(record: &Record) -> u64 {
        u64::from_le_bytes(record.payload[..8].try_into().expect("an id payload"))
    }

    fn ids_of(records: &[Record]) -> Vec<u64> {
        records.iter().map(Self::id_of).collect()
    }

    /// Send what client `index` asks, and judge what it reports.
    fn flush(&mut self, index: usize, cx: &mut Cx<'_, Self>) {
        for request in self.programs[index].client.take_requests() {
            let command = matches!(request, Request::Command { .. });
            self.waiting.insert((index, request.id()), command);
            cx.send(ClientMsg::Request(index, request));
        }
        for (call, outcome) in self.programs[index].client.take_outcomes() {
            let asked = self.programs[index]
                .calls
                .remove(&call)
                .expect("an outcome for a call the program asked");
            self.judge(index, &asked, &outcome);
        }
    }

    /// A program stops waiting for one request.
    fn timeout(&mut self, cx: &mut Cx<'_, Self>) {
        let waiting: Vec<(usize, RequestId)> = self.waiting.keys().copied().collect();
        let Some(&(index, id)) = cx.rng().pick(&waiting) else {
            return;
        };
        let command = self
            .waiting
            .remove(&(index, id))
            .expect("a waiting request");
        let response = if command && self.mutant == ClientMutant::LostAsFenced {
            Response::Reply(Reply::Answer(Answer::Fenced(self.current().fold.view())))
        } else {
            Response::Lost
        };
        self.programs[index].client.answer(id, response);
        self.flush(index, cx);
    }

    /// The journal serves `request` of client `index`.
    fn serve(&mut self, index: usize, request: Request, cx: &mut Cx<'_, Self>) {
        let chaos = cx.phase() == Phase::Chaos;
        match request {
            Request::Command { id, command } => {
                self.sent.push((index, command.clone()));
                if self.sent.len() > 8 {
                    self.sent.remove(0);
                }
                let fate = if chaos { cx.rng().below(10) } else { 0 };
                let response = match fate {
                    // The slot chose a `Noop`: the command holds no slot.
                    6 => Response::Reply(Reply::Unknown),
                    7 => Response::NotLeader,
                    _ => {
                        let answer = self.apply(index, &command);
                        match fate {
                            8 => Response::Reply(Reply::Unknown),
                            // The answer is lost; the program times out.
                            9 => return,
                            _ => Response::Reply(Reply::Answer(self.forge(answer))),
                        }
                    }
                };
                cx.send(ClientMsg::Response(index, id, response));
            }
            Request::Resolve {
                id,
                uuid,
                expected_seq,
                records,
            } => {
                let lagging = chaos && cx.rng().chance(1, 5);
                let answer = if lagging {
                    ResolveAnswer::NotCovered
                } else if self.mutant == ClientMutant::ResolveWritten {
                    ResolveAnswer::Written {
                        seq: expected_seq,
                        count: records.len() as u64,
                    }
                } else {
                    self.resolve(uuid, expected_seq, &records)
                };
                if matches!(answer, ResolveAnswer::NotWritten) {
                    self.again.insert(Self::ids_of(&records));
                }
                cx.send(ClientMsg::Response(index, id, Response::Resolved(answer)));
            }
            Request::Floor { id } => {
                let sealed = self.segments.len() - 1;
                let answer = if chaos && cx.rng().chance(1, 6) {
                    ReadAnswer::Unserved
                } else if self.mutant == ClientMutant::FloorFromOldest {
                    ReadAnswer::Page {
                        records: Vec::new(),
                        view: self.segments[0].fold.view(),
                        sealed: false,
                    }
                } else if let Some(stale) = cx.rng().index(sealed).filter(|_| cx.rng().chance(1, 4))
                {
                    // The program routed the copy on a register value that a
                    // seal made stale: a replica of a sealed segment answers
                    // at its tail.
                    ReadAnswer::Page {
                        records: Vec::new(),
                        view: self.segments[stale].fold.view(),
                        sealed: true,
                    }
                } else {
                    ReadAnswer::Page {
                        records: Vec::new(),
                        view: self.current().fold.view(),
                        sealed: false,
                    }
                };
                cx.send(ClientMsg::Response(index, id, Response::Read(answer)));
            }
            Request::Read { id, from, limit } => {
                let answer = if chaos && cx.rng().chance(1, 6) {
                    ReadAnswer::Unserved
                } else {
                    self.read(from, limit)
                };
                cx.send(ClientMsg::Response(index, id, Response::Read(answer)));
            }
        }
    }

    /// [`ClientMutant::ForgedRefusal`]: a refused `SetLeader` names the
    /// last leader value drawn. [`ClientMutant::ShortView`]: a won
    /// `SetLeader` names a tail one short.
    fn forge(&self, answer: Answer) -> Answer {
        match answer {
            Answer::LeaderRefused(view)
                if self.mutant == ClientMutant::ForgedRefusal && self.drawn != Leader::Unset =>
            {
                Answer::LeaderRefused(JournalView {
                    leader: self.drawn,
                    ..view
                })
            }
            Answer::LeaderSet(view)
                if self.mutant == ClientMutant::ShortView && view.next_seq.0 > 0 =>
            {
                Answer::LeaderSet(JournalView {
                    next_seq: Seq(view.next_seq.0 - 1),
                    ..view
                })
            }
            other => other,
        }
    }

    /// The command is chosen at the next slot of the segment in force, and
    /// folded.
    fn apply(&mut self, index: usize, command: &Command) -> Answer {
        let mut command = command.clone();
        if let Command::Write(write) = &mut command
            && let Fence::Leader { uuid, expected_seq } = &mut write.fence
        {
            let ids = Self::ids_of(&write.records);
            self.fenced.extend(ids.iter().copied());
            let fold = self.current().fold;
            let first = self.placed.insert(ids);
            if first && fold.leader() == Leader::Uuid(*uuid) {
                assert!(
                    *expected_seq == fold.next_seq(),
                    "{TAIL}: a new write of {uuid:?} at {expected_seq:?}, the tail is {:?}",
                    fold.next_seq()
                );
                sometimes!(self.truncated[index], reach::AFTER_TRUNCATED);
                self.truncated[index] = false;
            }
            if self.mutant == ClientMutant::ResendAtTail && !first {
                *expected_seq = fold.next_seq();
            }
        }
        let segments = self.segments.len();
        let segment = self.current_mut();
        let answer = segment
            .fold
            .apply(&command, |seq| segment.positions.get(&seq));
        match (&answer, &command) {
            (Answer::Written { seq, count }, Command::Write(write)) => {
                if matches!(write.fence, Fence::Leader { .. }) {
                    segment.positions.insert(*seq, write.clone());
                }
                assert!(*count == write.count(), "a write takes its count");
                for (i, record) in write.records.iter().enumerate() {
                    let id = Self::id_of(record);
                    let position = seq.0 + i as u64;
                    self.log.insert(position, (id, record.clone()));
                    let landed = self.landed.entry(id).or_insert(0);
                    *landed += 1;
                    assert!(
                        *landed == 1 || !self.fenced.contains(&id),
                        "{ONCE}: record {id} landed {landed} times"
                    );
                    sometimes!(*landed > 1, reach::TWICE);
                }
                sometimes!(segments >= 3, reach::THIRD);
            }
            (
                Answer::LeaderSet(_),
                Command::SetLeader {
                    new: Leader::Uuid(id) | Leader::Multi(id),
                    ..
                },
            ) => {
                self.installed.insert(id.0);
            }
            (
                Answer::Fenced(_) | Answer::Taken(_) | Answer::WrongMode(_),
                Command::Write(write),
            ) => {
                sometimes!(
                    self.again.contains(&Self::ids_of(&write.records)),
                    reach::RESOLVED_AGAIN
                );
            }
            (Answer::Truncated(view), Command::Write(write)) => {
                if let Fence::Leader { uuid, .. } = write.fence
                    && view.leader == Leader::Uuid(uuid)
                    && !self.replaying
                {
                    self.truncated[index] = true;
                }
            }
            (Answer::Duplicate { .. }, _) => sometimes!(true, reach::DUPLICATE),
            (Answer::LeaderRefused(view), Command::SetLeader { new, .. }) => {
                sometimes!(
                    *new != Leader::Unset && view.leader == *new,
                    reach::LOST_WIN
                );
            }
            (Answer::Below(_), Command::Write(write)) => {
                self.belows.insert(Self::ids_of(&write.records));
            }
            _ => {}
        }
        answer
    }

    /// A replica of the sealed segment that holds `expected_seq` answers.
    fn resolve(&self, uuid: LeaderUuid, expected_seq: Seq, records: &[Record]) -> ResolveAnswer {
        let sealed = &self.segments[..self.segments.len() - 1];
        let Some(segment) = sealed
            .iter()
            .find(|s| s.base.next_seq <= expected_seq && expected_seq < s.fold.next_seq())
        else {
            return ResolveAnswer::NotCovered;
        };
        resolve(&segment.fold, uuid, expected_seq, records, |seq| {
            segment.positions.get(&seq)
        })
    }

    /// The segment that holds `from` answers: a sealed segment folds only up
    /// to its seal, so its floor misses every later `Truncate`.
    fn read(&self, from: Seq, limit: u64) -> ReadAnswer {
        let holder = self.holder(from);
        let fold = &self.segments[holder].fold;
        let view = fold.view();
        if from < fold.first_seq() {
            return ReadAnswer::Truncated(view);
        }
        let start = if self.mutant == ClientMutant::PageLate {
            from.0 + 1
        } else {
            from.0
        };
        let end = (from.0 + limit).min(fold.next_seq().0);
        let records = (start..end)
            .map(|seq| {
                let record = self.log.get(&seq).expect("a position below next_seq");
                (Seq(seq), record.1.clone())
            })
            .collect();
        ReadAnswer::Page {
            records,
            view,
            sealed: holder + 1 < self.segments.len() && end == fold.next_seq().0,
        }
    }

    /// The index of the segment whose range holds `seq`: a sealed one, or
    /// the segment in force for a position at or above its start.
    fn holder(&self, seq: Seq) -> usize {
        self.segments
            .iter()
            .position(|s| s.base.next_seq <= seq && seq < s.fold.next_seq())
            .unwrap_or(self.segments.len() - 1)
    }

    /// The oracles on one reported outcome.
    fn judge(&mut self, index: usize, asked: &Asked, outcome: &Outcome) {
        match (asked, outcome) {
            (Asked::Write(ids), Outcome::Written { seq, count }) => {
                assert!(
                    *count == ids.len() as u64,
                    "{WRITTEN}: {ids:?} reported {count} records"
                );
                for (i, id) in ids.iter().enumerate() {
                    let at = self.log.get(&(seq.0 + i as u64)).map(|(id, _)| *id);
                    assert!(
                        at == Some(*id),
                        "{WRITTEN}: {ids:?} at {seq:?}, the log holds {at:?}"
                    );
                }
                sometimes!(ids.len() > 1, reach::BATCH);
                sometimes!(self.belows.contains(ids), reach::RESOLVED_WRITTEN);
            }
            (Asked::Write(ids), Outcome::Refused(answer)) => {
                if ids.iter().any(|id| self.fenced.contains(id)) {
                    for id in ids {
                        assert!(
                            !self.landed.contains_key(id),
                            "{REFUSED}: record {id} refused {answer:?} is in the log"
                        );
                    }
                    self.refused.extend(ids.iter().copied());
                }
                sometimes!(matches!(answer, Answer::Fenced(_)), reach::DEPOSED);
            }
            (Asked::Write(_), Outcome::Truncated(_)) => sometimes!(true, reach::TRUNCATED),
            (Asked::SetLeader(new), Outcome::LeaderSet(view)) => {
                assert!(
                    view.leader == *new,
                    "{LED}: SetLeader({new:?}) reported set with {view:?}"
                );
                // Ids are fresh, so a copy of this call applied iff its id
                // was installed.
                let applied = match new {
                    Leader::Uuid(id) | Leader::Multi(id) => self.installed.contains(&id.0),
                    Leader::Unset => false,
                };
                assert!(
                    applied,
                    "{LED}: SetLeader({new:?}) of client {index} reported set, no copy applied"
                );
            }
            (Asked::Truncate, Outcome::Trimmed(_)) => sometimes!(true, reach::TRIMMED),
            (Asked::Read { limit, floor, .. }, Outcome::Page { from, records, .. }) => {
                assert!(records.len() as u64 <= *limit, "{PAGE}: over the limit");
                assert!(
                    records.is_empty() || from >= floor,
                    "{FLOOR}: a page from {from:?}, the floor was {floor:?} when the read was asked"
                );
                for (i, record) in records.iter().enumerate() {
                    let at = self.log.get(&(from.0 + i as u64)).map(|(_, r)| r);
                    assert!(
                        at == Some(record),
                        "{PAGE}: {record:?} at {}, the log holds {at:?}",
                        from.0 + i as u64
                    );
                }
                sometimes!(!records.is_empty(), reach::PAGE);
            }
            (Asked::Read { from, .. }, Outcome::Truncated(_)) => {
                sometimes!(true, reach::READ_TRUNCATED);
                sometimes!(
                    *from >= self.segments[self.holder(*from)].fold.first_seq(),
                    reach::FLOOR_REFUSED
                );
            }
            _ => {}
        }
    }
}

/// The client walk with `mutant` planted, over `seeds` seeds.
#[must_use]
pub fn walk(mutant: ClientMutant, seeds: u64) -> Walk<ClientModel> {
    Walk::<ClientModel>::new()
        .seeds(seeds)
        .chaos_steps(300)
        .env_prefix(None)
        .first_seed(crate::campaign::first_seed())
        .model(move |cx| ClientModel::build(mutant, cx))
}
