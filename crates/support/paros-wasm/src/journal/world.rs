//! The journal world of the fold lab: three writers and a chain of
//! segments, each folded by `paros-journal`'s [`JournalState`].
//!
//! The world is a driver. It builds the commands a client sends, hands each
//! one to [`JournalState::apply`] (or to [`resolve()`]) and keeps what the
//! core answers. It judges nothing itself: every answer, and every move of
//! `next_seq`, `first_seq` and the leader, is the core's.
//!
//! What the world adds is what a client and a replica would hold around the
//! core: each writer's belief (the last view it heard), its last write, the
//! records each segment took, and the seal. No consensus runs here: each
//! call is one decided slot, applied at once. Network, disks and time are
//! not in the world.

use std::collections::BTreeMap;

use paros_journal::{
    Answer, BoundaryState, Command, Fence, JournalState, JournalView, Leader, LeaderUuid, Record,
    ResolveAnswer, Seq, Write, resolve,
};
use serde::Serialize;

/// The names of the three writers.
pub const NAMES: [&str; 3] = ["A", "B", "C"];

/// How many segments a run may hold: a bound on the seals, far above any
/// exercise.
const SEGMENTS: usize = 8;

/// One call a writer (or the operator, for a seal) makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Call {
    /// `SetLeader(writer, believed leader)`.
    SetLeader(usize),
    /// `SetLeader(None, believed leader)`: multi-writer.
    SetMulti(usize),
    /// A new fenced write of one record at the writer's believed `next_seq`.
    Write(usize),
    /// The same, and the answer is lost on its way back.
    WriteLost(usize),
    /// A new unfenced write of one record.
    WriteOpen(usize),
    /// The writer's last write, sent again unchanged.
    Resend(usize),
    /// `Truncate` up to the writer's believed `next_seq`: fenced by its
    /// uuid, or unfenced when it believes the journal is multi-writer.
    Truncate(usize),
    /// `Read` of the tail: the writer learns the view of the segment in
    /// force.
    Read(usize),
    /// `Resolve` of the writer's last write, at a replica of the segment
    /// that holds its position.
    Resolve(usize),
    /// The segment in force seals; the next one folds from its boundary
    /// state.
    Seal,
}

impl Call {
    /// The writer that makes the call, if any.
    #[must_use]
    pub fn writer(self) -> Option<usize> {
        match self {
            Self::SetLeader(w)
            | Self::SetMulti(w)
            | Self::Write(w)
            | Self::WriteLost(w)
            | Self::WriteOpen(w)
            | Self::Resend(w)
            | Self::Truncate(w)
            | Self::Read(w)
            | Self::Resolve(w) => Some(w),
            Self::Seal => None,
        }
    }

    /// A stable name for the page and its tests: `A:write`, `seal`.
    ///
    /// # Panics
    ///
    /// Never: every call but a seal has a writer.
    #[must_use]
    pub fn key(self) -> String {
        let verb = match self {
            Self::SetLeader(_) => "set-leader",
            Self::SetMulti(_) => "set-multi",
            Self::Write(_) => "write",
            Self::WriteLost(_) => "write-lost",
            Self::WriteOpen(_) => "write-open",
            Self::Resend(_) => "resend",
            Self::Truncate(_) => "truncate",
            Self::Read(_) => "read",
            Self::Resolve(_) => "resolve",
            Self::Seal => return "seal".to_string(),
        };
        format!("{}:{verb}", NAMES[self.writer().expect("a writer's call")])
    }
}

/// What one call gave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The fold's answer to a decided command.
    Applied(Answer),
    /// The answer of a `Resolve`.
    Resolved(ResolveAnswer),
    /// A `Read`: the view of the segment in force.
    Read(JournalView),
    /// A seal: the boundary state the next segment folds from.
    Sealed(BoundaryState),
}

/// One call and its outcome, as the history keeps it.
#[derive(Clone, Debug)]
pub struct Event {
    /// The call.
    pub call: Call,
    /// What the client sent, in the words of §J2: `Write(A, 0, [a1])`.
    pub sent: String,
    /// What the core gave.
    pub outcome: Outcome,
    /// Whether the answer was lost on its way back to the writer.
    pub lost: bool,
}

/// One writer: its uuid, the view it last heard, and its last write.
#[derive(Clone, Debug)]
struct Writer {
    uuid: LeaderUuid,
    belief: JournalView,
    last: Option<Write>,
    /// How many new writes it made: the payload of the next is `a<n+1>`.
    made: u32,
}

/// One segment: its fold, the writes it took by their first position, and
/// whether it is sealed.
#[derive(Clone, Debug)]
struct Segment {
    state: JournalState,
    writes: BTreeMap<Seq, Write>,
    sealed: bool,
}

impl Segment {
    fn from(boundary: BoundaryState) -> Self {
        Self {
            state: JournalState::from_boundary(boundary),
            writes: BTreeMap::new(),
            sealed: false,
        }
    }
}

/// The world: three writers and a chain of segments.
#[derive(Clone, Debug)]
pub struct Journal {
    writers: Vec<Writer>,
    segments: Vec<Segment>,
    events: Vec<Event>,
}

impl Default for Journal {
    fn default() -> Self {
        Self::new()
    }
}

impl Journal {
    /// A fresh journal, single-writer with no leader yet, and three writers
    /// that heard its first view.
    #[must_use]
    pub fn new() -> Self {
        let first = Segment::from(BoundaryState::fresh(Leader::Unset));
        let view = first.state.view();
        let writers = (0..NAMES.len())
            .map(|i| Writer {
                uuid: uuid_of(i),
                belief: view,
                last: None,
                made: 0,
            })
            .collect();
        Self {
            writers,
            segments: vec![first],
            events: Vec::new(),
        }
    }

    /// Make `call`.
    ///
    /// # Errors
    ///
    /// A sentence when the call cannot be made: no write to send again, a
    /// last write with no fence for `Resolve`, too many segments.
    ///
    /// # Panics
    ///
    /// When the core's own assertions fail: a bug, never a move.
    pub fn call(&mut self, call: Call) -> Result<&Event, String> {
        let event = match call {
            Call::Seal => self.seal()?,
            Call::Read(w) => {
                let view = self.in_force().state.view();
                self.writers[w].belief = view;
                Event {
                    call,
                    sent: "Read(tail)".to_string(),
                    outcome: Outcome::Read(view),
                    lost: false,
                }
            }
            Call::Resolve(w) => self.resolve(w)?,
            Call::SetLeader(w) | Call::SetMulti(w) => {
                let new = matches!(call, Call::SetLeader(_)).then_some(self.writers[w].uuid);
                let old = self.writers[w].belief.leader;
                let sent = format!(
                    "SetLeader({}, {})",
                    new.map_or("None".to_string(), name_of),
                    show_leader(old)
                );
                self.apply(call, sent, &Command::SetLeader { new, old }, false)
            }
            Call::Write(w) | Call::WriteLost(w) | Call::WriteOpen(w) => {
                let writer = &mut self.writers[w];
                writer.made += 1;
                let payload = format!("{}{}", NAMES[w].to_lowercase(), writer.made);
                let fence = if matches!(call, Call::WriteOpen(_)) {
                    Fence::Open
                } else {
                    Fence::Leader {
                        uuid: writer.uuid,
                        expected_seq: writer.belief.next_seq,
                    }
                };
                let write = Write {
                    fence,
                    records: vec![Record::new(payload.into_bytes())],
                };
                writer.last = Some(write.clone());
                let sent = show_write(&write);
                self.apply(
                    call,
                    sent,
                    &Command::Write(write),
                    matches!(call, Call::WriteLost(_)),
                )
            }
            Call::Resend(w) => {
                let write = self.writers[w]
                    .last
                    .clone()
                    .ok_or_else(|| format!("{} has no write to send again.", NAMES[w]))?;
                let sent = show_write(&write);
                self.apply(call, sent, &Command::Write(write), false)
            }
            Call::Truncate(w) => {
                let writer = &self.writers[w];
                let fence = (writer.belief.leader != Leader::Multi).then_some(writer.uuid);
                let up_to = writer.belief.next_seq;
                let sent = format!(
                    "Truncate({}, {})",
                    fence.map_or("None".to_string(), name_of),
                    up_to.0
                );
                self.apply(call, sent, &Command::Truncate { fence, up_to }, false)
            }
        };
        self.events.push(event);
        Ok(self.events.last().expect("an event was just pushed"))
    }

    /// Hand one command to the fold of the segment in force.
    fn apply(&mut self, call: Call, sent: String, command: &Command, lost: bool) -> Event {
        let segment = self.segments.last_mut().expect("a journal has a segment");
        let Segment { state, writes, .. } = segment;
        let answer = state.apply(command, |seq| writes.get(&seq));
        if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, command) {
            // The segment keeps the write as the client sent it, at the
            // position the fold gave it (an unfenced write names none).
            if let Fence::Leader { expected_seq, .. } = write.fence {
                assert!(expected_seq == *seq, "a fenced write lands at its position");
            }
            assert!(!writes.contains_key(seq), "a position takes one write");
            writes.insert(*seq, write.clone());
        }
        let w = call.writer().expect("an applied call has a writer");
        if !lost {
            self.hear(w, &answer, command);
        }
        Event {
            call,
            sent,
            outcome: Outcome::Applied(answer),
            lost,
        }
    }

    /// What the writer learns from an answer it heard.
    fn hear(&mut self, w: usize, answer: &Answer, command: &Command) {
        let writer = &mut self.writers[w];
        match answer {
            Answer::Written { seq, count } | Answer::Duplicate { seq, count } => {
                writer.belief.next_seq = writer.belief.next_seq.max(Seq(seq.0 + count));
                if let Command::Write(Write {
                    fence: Fence::Leader { uuid, .. },
                    ..
                }) = command
                {
                    writer.belief.leader = Leader::Uuid(*uuid);
                }
            }
            Answer::Taken(view)
            | Answer::Fenced(view)
            | Answer::Early(view)
            | Answer::Truncated(view)
            | Answer::Below(view)
            | Answer::WrongMode(view)
            | Answer::Empty(view)
            | Answer::LeaderSet(view)
            | Answer::LeaderRefused(view)
            | Answer::Trimmed(view)
            | Answer::Reserved(view) => writer.belief = *view,
            Answer::Noop => {}
        }
    }

    fn seal(&mut self) -> Result<Event, String> {
        if self.segments.len() >= SEGMENTS {
            return Err(format!("This run holds at most {SEGMENTS} segments."));
        }
        let ending = self.segments.last_mut().expect("a journal has a segment");
        ending.sealed = true;
        let boundary = ending.state.boundary();
        let next = Segment::from(boundary);
        assert!(
            next.state.boundary().follows(&boundary),
            "a new segment follows the sealed one"
        );
        self.segments.push(next);
        Ok(Event {
            call: Call::Seal,
            sent: format!("seal segment {}", self.segments.len() - 1),
            outcome: Outcome::Sealed(boundary),
            lost: false,
        })
    }

    fn resolve(&self, w: usize) -> Result<Event, String> {
        let write = self.writers[w]
            .last
            .as_ref()
            .ok_or_else(|| format!("{} has no write to resolve.", NAMES[w]))?;
        let Fence::Leader { uuid, expected_seq } = write.fence else {
            return Err(format!(
                "{}'s last write has no fence and no position: Resolve needs both.",
                NAMES[w]
            ));
        };
        // A replica of the segment that holds the position: the last
        // segment that starts at or below it.
        let segment = self
            .segments
            .iter()
            .rev()
            .find(|segment| segment.state.start_seq() <= expected_seq)
            .unwrap_or(&self.segments[0]);
        let answer = resolve(&segment.state, uuid, expected_seq, &write.records, |seq| {
            segment.writes.get(&seq)
        });
        Ok(Event {
            call: Call::Resolve(w),
            sent: format!(
                "Resolve({}, {}, [{}])",
                name_of(uuid),
                expected_seq.0,
                payloads(write)
            ),
            outcome: Outcome::Resolved(answer),
            lost: false,
        })
    }

    // ---- what the lab reads --------------------------------------------------

    fn in_force(&self) -> &Segment {
        self.segments.last().expect("a journal has a segment")
    }

    /// The view of the segment in force.
    #[must_use]
    pub fn view(&self) -> JournalView {
        self.in_force().state.view()
    }

    /// Every call so far, in order.
    #[must_use]
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Whether writer `w` heard `hit` (an answer that was not lost).
    #[must_use]
    pub fn heard(&self, w: usize, hit: impl Fn(&Outcome) -> bool) -> bool {
        self.events
            .iter()
            .any(|e| !e.lost && e.call.writer() == Some(w) && hit(&e.outcome))
    }

    /// The label of a call in the current state: `A writes a2 at 1`.
    #[must_use]
    pub fn label(&self, call: Call) -> String {
        let Some(w) = call.writer() else {
            return format!("Seal segment {}", self.segments.len());
        };
        let name = NAMES[w];
        let writer = &self.writers[w];
        let next = format!("{}{}", name.to_lowercase(), writer.made + 1);
        let at = writer.belief.next_seq.0;
        let last = writer.last.as_ref().map(show_write);
        match call {
            Call::SetLeader(_) => format!(
                "{name}: SetLeader({name}, {})",
                show_leader(writer.belief.leader)
            ),
            Call::SetMulti(_) => format!(
                "{name}: SetLeader(None, {})",
                show_leader(writer.belief.leader)
            ),
            Call::Write(_) => format!("{name} writes {next} at {at}"),
            Call::WriteLost(_) => format!("{name} writes {next} at {at}; the answer is lost"),
            Call::WriteOpen(_) => format!("{name} writes {next} with no fence"),
            Call::Resend(_) => match last {
                Some(sent) => format!("{name} sends {sent} again"),
                None => format!("{name} sends its last write again"),
            },
            Call::Truncate(_) => format!("{name} truncates up to {at}"),
            Call::Read(_) => format!("{name} reads the tail"),
            Call::Resolve(_) => format!("{name} asks Resolve for its last write"),
            Call::Seal => unreachable!("a seal has no writer"),
        }
    }

    /// The world as the page draws it.
    #[must_use]
    pub fn render(&self) -> WorldView {
        let view = self.view();
        WorldView {
            journal: show_view(view),
            writers: self
                .writers
                .iter()
                .enumerate()
                .map(|(i, writer)| WriterView {
                    name: NAMES[i].to_string(),
                    believes: show_view(writer.belief),
                    last: writer.last.as_ref().map(show_write),
                })
                .collect(),
            segments: self
                .segments
                .iter()
                .enumerate()
                .map(|(i, segment)| SegmentView {
                    n: i + 1,
                    start: segment.state.start_seq().0,
                    next: segment.state.next_seq().0,
                    sealed: segment.sealed,
                    records: segment
                        .writes
                        .iter()
                        .flat_map(|(seq, write)| {
                            write.records.iter().enumerate().map(move |(k, record)| {
                                let at = seq.0 + k as u64;
                                RecordView {
                                    seq: at,
                                    text: String::from_utf8_lossy(&record.payload).into_owned(),
                                    trimmed: at < view.first_seq.0,
                                }
                            })
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// The page's picture of the world.
#[derive(Clone, Debug, Serialize)]
pub struct WorldView {
    /// The view of the segment in force.
    pub journal: ViewText,
    /// Each writer.
    pub writers: Vec<WriterView>,
    /// Each segment, oldest first.
    pub segments: Vec<SegmentView>,
}

/// A journal view in words.
#[derive(Clone, Debug, Serialize)]
pub struct ViewText {
    /// `A`, `None` (multi-writer) or `unset`.
    pub leader: String,
    /// `next_seq`.
    pub next_seq: u64,
    /// `first_seq`.
    pub first_seq: u64,
}

/// One writer.
#[derive(Clone, Debug, Serialize)]
pub struct WriterView {
    /// `A`.
    pub name: String,
    /// The view the writer last heard.
    pub believes: ViewText,
    /// Its last write, as sent.
    pub last: Option<String>,
}

/// One segment.
#[derive(Clone, Debug, Serialize)]
pub struct SegmentView {
    /// 1 for the first.
    pub n: usize,
    /// Its first position.
    pub start: u64,
    /// Its `next_seq`.
    pub next: u64,
    /// Whether it is sealed.
    pub sealed: bool,
    /// The records it took.
    pub records: Vec<RecordView>,
}

/// One record.
#[derive(Clone, Debug, Serialize)]
pub struct RecordView {
    /// Its position.
    pub seq: u64,
    /// Its payload.
    pub text: String,
    /// Whether it is below `first_seq`.
    pub trimmed: bool,
}

/// Writer `i`'s uuid: a fixed, set value, so a run is the same each time.
fn uuid_of(i: usize) -> LeaderUuid {
    let uuid = LeaderUuid(0xA + i as u128);
    assert!(uuid.is_set(), "a writer's uuid is set");
    uuid
}

/// The name of the writer that holds `uuid`.
fn name_of(uuid: LeaderUuid) -> String {
    (0..NAMES.len())
        .find(|&i| uuid_of(i) == uuid)
        .map_or_else(|| uuid.to_string(), |i| NAMES[i].to_string())
}

/// A leader value in words.
#[must_use]
pub fn show_leader(leader: Leader) -> String {
    match leader {
        Leader::Unset => "unset".to_string(),
        Leader::Multi => "None".to_string(),
        Leader::Uuid(uuid) => name_of(uuid),
    }
}

fn show_view(view: JournalView) -> ViewText {
    ViewText {
        leader: show_leader(view.leader),
        next_seq: view.next_seq.0,
        first_seq: view.first_seq.0,
    }
}

fn payloads(write: &Write) -> String {
    write
        .records
        .iter()
        .map(|record| String::from_utf8_lossy(&record.payload).into_owned())
        .collect::<Vec<_>>()
        .join(", ")
}

/// A write as §J2 names it: `Write(A, 0, [a1])`, or `Write([b1])` unfenced.
fn show_write(write: &Write) -> String {
    match write.fence {
        Fence::Leader { uuid, expected_seq } => format!(
            "Write({}, {}, [{}])",
            name_of(uuid),
            expected_seq.0,
            payloads(write)
        ),
        Fence::Open => format!("Write([{}])", payloads(write)),
    }
}
