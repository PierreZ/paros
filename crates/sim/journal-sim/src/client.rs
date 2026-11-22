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
//! - [`LED`]: a `SetLeader` reported set installed its leader.
//! - [`PAGE`]: a page holds the log's records at its positions.
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
/// Oracle: a `SetLeader` reported set installed its leader.
pub const LED: &str = "client walk: a SetLeader reported set installed its leader";
/// Oracle: a page holds the log's records.
pub const PAGE: &str = "client walk: a page holds the log's records at its positions";

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
    /// A truncate applied.
    pub const TRIMMED: &str = "client walk: a truncate applied";
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
        TRIMMED,
        THIRD,
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
    SetLeader(Option<LeaderUuid>),
    Truncate,
    Read { limit: u64 },
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
    /// The leader values that a `SetLeader` installed.
    installed: Vec<Leader>,
    /// The requests whose answer a program still waits for, and whether
    /// each is a command.
    waiting: BTreeMap<(usize, RequestId), bool>,
    /// The fenced writes the journal already placed, for
    /// [`ClientMutant::ResendAtTail`].
    placed: BTreeSet<Vec<u64>>,
    /// The writes answered `Below` and not resolved yet, by record ids.
    belows: BTreeSet<Vec<u64>>,
    /// The writes resolved not written.
    again: BTreeSet<Vec<u64>>,
    /// The last uuid drawn.
    uuids: u128,
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
        ]
    }

    fn weight(action: ClientAction, phase: Phase) -> u32 {
        match (phase, action) {
            (_, ClientAction::Timeout) | (Phase::Chaos, ClientAction::Seal) => 2,
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
            ClientAction::Claim => {
                self.uuids += 1;
                let uuid = LeaderUuid(self.uuids);
                let old = self.old_leader(leader, cx);
                let client = &mut self.programs[index].client;
                (
                    client.set_leader(Some(uuid), old),
                    Asked::SetLeader(Some(uuid)),
                )
            }
            ClientAction::ToMulti => {
                let old = self.old_leader(leader, cx);
                let client = &mut self.programs[index].client;
                (client.set_leader(None, old), Asked::SetLeader(None))
            }
            ClientAction::Write => {
                let (ids, records) = self.records(cx);
                let client = &mut self.programs[index].client;
                (client.write(records), Asked::Write(ids))
            }
            ClientAction::Truncate => {
                let up_to = Seq(cx.rng().below(next + 2));
                let client = &mut self.programs[index].client;
                (client.truncate(up_to), Asked::Truncate)
            }
            ClientAction::Read => {
                let from = Seq(cx.rng().below(next + 2));
                let limit = 1 + cx.rng().below(4);
                let client = &mut self.programs[index].client;
                (client.read(from, limit), Asked::Read { limit })
            }
            ClientAction::Seal | ClientAction::Timeout => unreachable!("handled above"),
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
        let fresh = if cx.rng().chance(1, 4) {
            Leader::Multi
        } else {
            Leader::Unset
        };
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
            installed: vec![fresh],
            waiting: BTreeMap::new(),
            placed: BTreeSet::new(),
            belows: BTreeSet::new(),
            again: BTreeSet::new(),
            uuids: 0,
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
                1 => Leader::Multi,
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
            self.judge(&asked, &outcome);
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
                let fate = if chaos { cx.rng().below(10) } else { 0 };
                let response = match fate {
                    // The slot chose a `Noop`: the command holds no slot.
                    6 => Response::Reply(Reply::Unknown),
                    7 => Response::NotLeader,
                    _ => {
                        let answer = self.apply(&command);
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
    /// uuid it asked for.
    fn forge(&self, answer: Answer) -> Answer {
        match answer {
            Answer::LeaderRefused(view)
                if self.mutant == ClientMutant::ForgedRefusal && self.uuids > 0 =>
            {
                Answer::LeaderRefused(JournalView {
                    leader: Leader::uuid(LeaderUuid(self.uuids)),
                    ..view
                })
            }
            other => other,
        }
    }

    /// The command is chosen at the next slot of the segment in force, and
    /// folded.
    fn apply(&mut self, command: &Command) -> Answer {
        let mut command = command.clone();
        if let Command::Write(write) = &mut command
            && let Fence::Leader { expected_seq, .. } = &mut write.fence
        {
            let ids = Self::ids_of(&write.records);
            self.fenced.extend(ids.iter().copied());
            if self.mutant == ClientMutant::ResendAtTail && !self.placed.insert(ids) {
                *expected_seq = self.current().fold.next_seq();
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
            (Answer::LeaderSet(view), _) => {
                self.installed.push(view.leader);
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
            (Answer::Duplicate { .. }, _) => sometimes!(true, reach::DUPLICATE),
            (Answer::LeaderRefused(view), Command::SetLeader { new, .. }) => {
                sometimes!(
                    new.is_some_and(LeaderUuid::is_set) && view.leader == Leader::from_new(*new),
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

    fn read(&self, from: Seq, limit: u64) -> ReadAnswer {
        let fold = &self.current().fold;
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
                self.log
                    .get(&seq)
                    .expect("a position below next_seq")
                    .1
                    .clone()
            })
            .collect();
        ReadAnswer::Page { records, view }
    }

    /// The oracles on one reported outcome.
    fn judge(&mut self, asked: &Asked, outcome: &Outcome) {
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
                    view.leader == Leader::from_new(*new) && self.installed.contains(&view.leader),
                    "{LED}: SetLeader({new:?}) reported set with {view:?}"
                );
            }
            (Asked::Truncate, Outcome::Trimmed(_)) => sometimes!(true, reach::TRIMMED),
            (Asked::Read { limit }, Outcome::Page { from, records, .. }) => {
                assert!(records.len() as u64 <= *limit, "{PAGE}: over the limit");
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
            (Asked::Read { .. }, Outcome::Truncated(_)) => {
                sometimes!(true, reach::READ_TRUNCATED);
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
        .model(move |cx| ClientModel::build(mutant, cx))
}
