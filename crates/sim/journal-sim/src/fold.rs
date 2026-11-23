//! The journal fold under the walk (#1324 (paros-journal)): the clients of
//! one journal send commands, each command is chosen in slot order, and the
//! replica of the segment in force folds it with
//! [`paros_journal::JournalState`]. The walk seals the segment in force: the
//! next segment folds from its boundary state, so a resend across a seal is
//! answered `Below`, and the client settles it with `Resolve` at the replica
//! of the old segment. The replica crashes and refolds its segment from the
//! boundary state when it restarts.
//!
//! The clients: three writers, each with its own uuid once it won a
//! `SetLeader`. A writer writes at the position it believes next, resends
//! one of its writes unchanged (with a new leader time), writes again at an
//! old position, and keeps writing under a uuid that a later `SetLeader`
//! deposed. Any client switches the journal to multi-writer and back,
//! truncates fenced or unfenced, and writes unfenced.
//!
//! The oracles, each written from `docs/journal/02-data-plane.md` and
//! judged against [`Truth`], the fold of the whole log:
//!
//! - [`FOLD`]: each answer is the truth's verdict on the slot, or `Below`
//!   where the truth says `Duplicate` or taken (`AckTrue` of
//!   `journal-state/`).
//! - [`VIEW`]: the fold of the segment in force is the truth's state
//!   (`OneFold`).
//! - [`RESOLVE`]: a `Resolve` answers written where the truth said
//!   `Duplicate`, not written where it said taken, or `Truncated` below the
//!   journal's floor (`ResolveTrue`).
//! - [`REFOLD`]: a replica that restarts folds its segment to the state it
//!   had.
//!
//! The liveness claim: once the faults stop, every `Below` is resolved.

use std::collections::BTreeMap;

use moonpool_walk::{Cx, Model, Phase, sometimes};
use paros_journal::resolve::ResolveAnswer;
use paros_journal::{
    Answer, BoundaryState, Command, Fence, Header, Headers, JournalState, Leader, LeaderUuid,
    Record, Seq, Write, resolve,
};

use crate::oracle::{Truth, Verdict, label};

/// Oracle: each answer is the answer of the whole log, or `Below`.
pub const FOLD: &str = "each answer is the whole log's, or Below for a resend across a seal";
/// Oracle: the fold is the fold of the whole log.
pub const VIEW: &str = "the fold of the segment in force is the fold of the whole log";
/// Oracle: a `Resolve` answers the fate that the whole log gives.
pub const RESOLVE: &str = "a Resolve answers the fate that the whole log gives";
/// Oracle: a refold gives the state the replica had.
pub const REFOLD: &str = "a replica that restarts refolds its segment to the same state";

/// The coverage sites of the fold walk.
pub mod reach {
    /// A resend across a seal answered `Below`.
    pub const BELOW: &str = "fold walk: a resend across a seal answered Below";
    /// A `Resolve` answered written.
    pub const RESOLVED_WRITTEN: &str = "fold walk: a Resolve answered written";
    /// A `Resolve` answered not written.
    pub const RESOLVED_NOT: &str = "fold walk: a Resolve answered not written";
    /// A resend answered `Duplicate`.
    pub const DUPLICATE: &str = "fold walk: a resend answered Duplicate";
    /// A resend with another leader time answered `Duplicate`.
    pub const DUPLICATE_TIME: &str = "fold walk: a resend with a new leader time is a Duplicate";
    /// A write at a taken position.
    pub const TAKEN: &str = "fold walk: a write at a taken position";
    /// A write above the next position.
    pub const EARLY: &str = "fold walk: a write above the next position is early";
    /// A write of a uuid that does not lead.
    pub const FENCED: &str = "fold walk: a write of a uuid that does not lead is fenced";
    /// A call shaped for the other mode.
    pub const WRONG_MODE: &str = "fold walk: a call shaped for the other mode";
    /// A write below the floor.
    pub const TRUNCATED: &str = "fold walk: a write below the floor is truncated";
    /// A truncate raised the floor.
    pub const TRIMMED: &str = "fold walk: a truncate raised the floor";
    /// `SetLeader(None)` applied.
    pub const TO_MULTI: &str = "fold walk: SetLeader(None) made the journal multi-writer";
    /// A `SetLeader` from multi-writer applied.
    pub const TO_SINGLE: &str = "fold walk: a SetLeader made it single-writer again";
    /// A write applied in the third segment.
    pub const THIRD: &str = "fold walk: a write applied in the third segment";
    /// A writer deposed in an earlier segment fenced in a later one.
    pub const MOVED: &str = "fold walk: a writer deposed before a seal is fenced after it";
    /// A replica refolded a segment that took a command.
    pub const REFOLDED: &str = "fold walk: a replica refolded a segment that took a command";
    /// A write of several records applied (batches only).
    pub const BATCH: &str = "fold walk: a write of several records applied";
    /// An empty write refused (extras only).
    pub const EMPTY: &str = "fold walk: an empty write is refused";
    /// A reserved command applied (extras only).
    pub const RESERVED: &str = "fold walk: an Upgrade is reserved";

    /// The sites of every fold walk.
    pub const ALL: &[&str] = &[
        BELOW,
        RESOLVED_WRITTEN,
        RESOLVED_NOT,
        DUPLICATE,
        DUPLICATE_TIME,
        TAKEN,
        EARLY,
        FENCED,
        WRONG_MODE,
        TRUNCATED,
        TRIMMED,
        TO_MULTI,
        TO_SINGLE,
        THIRD,
        MOVED,
        REFOLDED,
    ];
    /// The sites of a walk with batches and extras.
    pub const WIDE: &[&str] = &[BATCH, EMPTY, RESERVED];
}

/// What the walk's clients may send, and how many segments it may seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    /// Writes of several records (the model's write holds one record, so a
    /// traced walk has none).
    pub batches: bool,
    /// Empty writes and `Upgrade` commands (not in the model).
    pub extras: bool,
    /// The most segments of one walk: the walk stops sealing at this count.
    pub segments: usize,
}

impl Shape {
    /// What a traced walk sends: one record a write, nothing the model does
    /// not have.
    pub const TRACED: Self = Self {
        batches: false,
        extras: false,
        segments: 4,
    };
    /// Everything.
    pub const WIDE: Self = Self {
        batches: true,
        extras: true,
        segments: 4,
    };
    /// A long traced walk, for the live trace check (`tests/live.rs`): what
    /// [`TRACED`](Self::TRACED) sends, over many segments, so that the
    /// segments sealed early are still asked when the walk is far past them.
    pub const LONG: Self = Self {
        segments: 200,
        ..Self::TRACED
    };
}

/// A bug planted in the driver of the fold walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FoldMutant {
    /// No bug.
    #[default]
    None,
    /// The next segment folds from the base of the sealed one, not from its
    /// boundary state. [`VIEW`] catches it.
    StaleBoundary,
    /// A replica that restarts refolds its state but not its log of writes,
    /// so a resend is taken. [`FOLD`] catches it.
    LostLog,
    /// A replica that restarts refolds all but its last command. [`REFOLD`]
    /// catches it.
    RefoldShort,
    /// The client sends `Resolve` to the replica of the segment in force and
    /// takes "not covered" as not written. [`RESOLVE`] catches it.
    ResolveInForce,
    /// The driver answers `Below` for a write taken in the segment in force.
    /// The trace control: the walk turns off the leg of [`FOLD`] that judges
    /// the position of a `Below` and the leg of [`RESOLVE`] that needs a
    /// sealed segment to hold it, so only the trace check refuses it. A walk
    /// with no mutant judges both legs.
    BelowInSegment,
    /// The same bug as [`FoldMutant::BelowInSegment`], with every oracle on.
    /// [`FOLD`] catches it.
    BelowJudged,
}

/// One step of the fold walk, for `specs/tla/journal-state/FoldTrace.tla`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FoldEvent {
    /// The replica applied `command`: its answer, and its state after it.
    Apply {
        /// The command of the slot.
        command: Command,
        /// The answer, by name ([`label`]).
        answer: Verdict,
        /// The leader value, `next_seq` and `first_seq` after the step.
        state: (Leader, u64, u64),
    },
    /// The segment in force was sealed; the next one starts from this
    /// boundary state.
    Seal {
        /// The boundary state.
        state: (Leader, u64, u64),
    },
    /// The replica of sealed segment `segment` (from 1) answered the
    /// `Resolve` of `write`.
    Resolve {
        /// The sealed segment asked, from 1.
        segment: usize,
        /// The write resolved.
        write: Write,
        /// The answer.
        answer: ResolveAnswer,
    },
}

/// What the events port of the fold walk carries (rule 5 of
/// `docs/journal/04-roles.md` §J4.2, `Events`): the fresh journal first,
/// then each event in the order the walk records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reported {
    /// The leader value of the fresh journal.
    Fresh(Leader),
    /// One event.
    Event(FoldEvent),
}

/// The events port of a fold walk.
pub type Port = std::sync::mpsc::SyncSender<Reported>;

/// The message of a walk that stops because its live trace check stopped.
pub const STOPPED: &str = "the live trace check stopped the walk";

/// The moves of the fold walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoldAction {
    /// A writer claims the journal with a fresh uuid.
    Claim,
    /// A writer writes at the position it believes next.
    Write,
    /// A writer resends one of its writes.
    Resend,
    /// A writer writes something new at an old position.
    Stale,
    /// A client writes unfenced.
    Open,
    /// A client makes the journal multi-writer.
    ToMulti,
    /// A client truncates.
    Truncate,
    /// The leader fills a slot with a `Noop`.
    Noop,
    /// The segment in force is sealed.
    Seal,
    /// A client resolves a `Below`.
    Resolve,
    /// An empty write or an `Upgrade` (extras only).
    Extra,
}

/// The writers of the walk.
const WRITERS: usize = 3;

/// A writer.
#[derive(Clone, Debug, Default)]
struct Writer {
    uuid: Option<LeaderUuid>,
    next: u64,
    /// Its last fenced writes, for resends.
    sent: Vec<Write>,
}

/// A segment and its replica.
#[derive(Clone, Debug)]
struct Segment {
    base: BoundaryState,
    /// The replica's fold, `None` while it is down.
    fold: Option<JournalState>,
    /// The fenced writes the fold accepted, by position.
    log: BTreeMap<Seq, Write>,
    /// The commands chosen in this segment, in slot order.
    chosen: Vec<Command>,
    /// The truth's verdict on each chosen command.
    verdicts: Vec<Verdict>,
    /// How many of them the replica applied.
    applied: usize,
    /// The state before the crash, for [`REFOLD`].
    before_crash: Option<JournalState>,
}

impl Segment {
    fn new(base: BoundaryState) -> Self {
        Self {
            base,
            fold: Some(JournalState::from_boundary(base)),
            log: BTreeMap::new(),
            chosen: Vec::new(),
            verdicts: Vec::new(),
            applied: 0,
            before_crash: None,
        }
    }
}

/// The fold walk.
pub struct FoldModel {
    shape: Shape,
    /// The events port: each recorded event also goes there, for a live
    /// trace check. `None` for a walk checked after it ends.
    port: Option<Port>,
    mutant: FoldMutant,
    truth: Truth,
    segments: Vec<Segment>,
    writers: Vec<Writer>,
    /// The last uuid drawn.
    uuids: u128,
    /// The leader value the clients last saw.
    seen: Leader,
    /// The fenced writes answered `Below`, with the truth's verdict.
    belows: Vec<(Write, Verdict)>,
    /// The uuids deposed, with the segment where it happened.
    deposed: BTreeMap<u128, usize>,
}

impl Model for FoldModel {
    type Msg = ();
    type Node = ();
    type Action = FoldAction;
    type Event = FoldEvent;

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(Shape::WIDE, FoldMutant::None, cx)
    }

    fn actions() -> &'static [FoldAction] {
        &[
            FoldAction::Claim,
            FoldAction::Write,
            FoldAction::Resend,
            FoldAction::Stale,
            FoldAction::Open,
            FoldAction::ToMulti,
            FoldAction::Truncate,
            FoldAction::Noop,
            FoldAction::Seal,
            FoldAction::Resolve,
            FoldAction::Extra,
        ]
    }

    fn weight(action: FoldAction, phase: Phase) -> u32 {
        match (action, phase) {
            (FoldAction::Resolve, _) => 6,
            (_, Phase::Chaos) => match action {
                FoldAction::Write => 10,
                FoldAction::Resend | FoldAction::Claim => 5,
                FoldAction::Seal => 3,
                _ => 2,
            },
            _ => 0,
        }
    }

    fn act(&mut self, action: FoldAction, cx: &mut Cx<'_, Self>) {
        match action {
            FoldAction::Claim => self.claim(cx),
            FoldAction::Write => self.write(cx, false),
            FoldAction::Stale => self.write(cx, true),
            FoldAction::Resend => self.resend(cx),
            FoldAction::Open => {
                let records = self.records(cx);
                self.choose(
                    Command::Write(Write {
                        fence: Fence::Open,
                        records,
                    }),
                    cx,
                );
            }
            FoldAction::ToMulti => {
                let old = self.old_leader(cx);
                self.choose(Command::SetLeader { new: None, old }, cx);
            }
            FoldAction::Truncate => self.truncate(cx),
            FoldAction::Noop => {
                self.choose(Command::Noop, cx);
            }
            FoldAction::Seal => self.seal(cx),
            FoldAction::Resolve => self.resolve(cx),
            FoldAction::Extra => {
                if self.shape.extras {
                    let command = if cx.rng().chance(1, 2) {
                        Command::Upgrade {
                            release: paros_journal::Release(2),
                        }
                    } else {
                        let fence = self.fence(cx).unwrap_or(Fence::Open);
                        Command::Write(Write {
                            fence,
                            records: Vec::new(),
                        })
                    };
                    self.choose(command, cx);
                }
            }
        }
    }

    fn deliver(&mut self, _message: (), _cx: &mut Cx<'_, Self>) {}

    fn nodes(&self) -> Vec<()> {
        vec![()]
    }

    fn is_up(&self, _node: &()) -> bool {
        self.current().fold.is_some()
    }

    fn crash(&mut self, _node: &(), _cx: &mut Cx<'_, Self>) {
        let segment = self.current_mut();
        if let Some(fold) = segment.fold.take() {
            segment.before_crash = Some(fold);
        }
    }

    fn restart(&mut self, _node: &(), cx: &mut Cx<'_, Self>) {
        if self.current().fold.is_some() {
            return;
        }
        let mutant = self.mutant;
        let segment = self.current_mut();
        let before = segment
            .before_crash
            .take()
            .expect("a down replica has a state");
        let upto = if mutant == FoldMutant::RefoldShort {
            segment.applied.saturating_sub(1)
        } else {
            segment.applied
        };
        let mut fold = JournalState::from_boundary(segment.base);
        let mut log = BTreeMap::new();
        for command in &segment.chosen[..upto] {
            let answer = fold.apply(command, |seq| log.get(&seq));
            if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, command) {
                log.insert(*seq, write.clone());
            }
        }
        assert!(
            fold == before,
            "{REFOLD}: {fold:?} after a restart, {before:?} before"
        );
        sometimes!(segment.applied > 0, reach::REFOLDED);
        if mutant == FoldMutant::LostLog {
            segment.log.clear();
        } else {
            segment.log = log;
        }
        segment.fold = Some(fold);
        self.catch_up(cx);
    }

    fn check(&mut self) {
        let segment = self.current();
        if let Some(fold) = &segment.fold
            && segment.applied == segment.chosen.len()
        {
            assert!(
                (fold.leader(), fold.next_seq().0, fold.first_seq().0)
                    == (self.truth.leader, self.truth.next, self.truth.first),
                "{VIEW}: the fold {fold:?}, the truth {:?}",
                self.truth
            );
        }
    }

    fn converged(&self) -> Result<(), String> {
        if self.belows.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{} writes answered Below are not resolved",
                self.belows.len()
            ))
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} segments, truth {:?}, belows {}",
            self.segments.len(),
            self.truth,
            self.belows.len()
        )
    }
}

impl FoldModel {
    /// The walk with `shape` and `mutant` planted. The fresh journal is
    /// unset, or multi-writer on one seed in four.
    pub fn build(shape: Shape, mutant: FoldMutant, cx: &mut Cx<'_, Self>) -> Self {
        let fresh = if cx.rng().chance(1, 4) {
            Leader::Multi
        } else {
            Leader::Unset
        };
        Self {
            shape,
            port: None,
            mutant,
            truth: Truth::new(fresh),
            segments: vec![Segment::new(BoundaryState::fresh(fresh))],
            writers: vec![Writer::default(); WRITERS],
            uuids: 0,
            seen: fresh,
            belows: Vec::new(),
            deposed: BTreeMap::new(),
        }
    }

    /// The walk of [`build`](Self::build), with an events port: it reports
    /// its fresh journal, then each event as it records it, so that a live
    /// trace check judges the walk while it runs.
    ///
    /// # Panics
    ///
    /// When the port is closed: the live check stopped, and the walk stops
    /// with it.
    pub fn build_reported(
        shape: Shape,
        mutant: FoldMutant,
        port: Port,
        cx: &mut Cx<'_, Self>,
    ) -> Self {
        let mut model = Self::build(shape, mutant, cx);
        assert!(
            port.send(Reported::Fresh(model.fresh())).is_ok(),
            "{STOPPED}"
        );
        model.port = Some(port);
        model
    }

    /// Record `event`, and report it on the port.
    fn emit(&self, event: FoldEvent, cx: &mut Cx<'_, Self>) {
        if let Some(port) = &self.port {
            assert!(
                port.send(Reported::Event(event.clone())).is_ok(),
                "{STOPPED}"
            );
        }
        cx.record(event);
    }

    /// The leader value of the fresh journal, by its name in a trace.
    #[must_use]
    pub fn fresh(&self) -> Leader {
        self.segments[0].base.leader
    }

    fn current(&self) -> &Segment {
        self.segments.last().expect("a journal has a segment")
    }

    fn current_mut(&mut self) -> &mut Segment {
        self.segments.last_mut().expect("a journal has a segment")
    }

    fn records(&self, cx: &mut Cx<'_, Self>) -> Vec<Record> {
        let count = if self.shape.batches {
            1 + cx.rng().below(3)
        } else {
            1
        };
        (0..count)
            .map(|_| {
                let id = u8::try_from(1 + cx.rng().below(3)).expect("small");
                Record {
                    headers: Headers {
                        leader_time: cx.rng().chance(1, 2).then(|| cx.rng().below(1000)),
                        client: if id == 3 {
                            vec![Header {
                                key: b"k".to_vec(),
                                value: vec![id],
                            }]
                        } else {
                            Vec::new()
                        },
                    },
                    payload: vec![id],
                }
            })
            .collect()
    }

    /// A leader value a client names as `old`: the one it saw, or on one
    /// call in four an older one.
    fn old_leader(&self, cx: &mut Cx<'_, Self>) -> Leader {
        if cx.rng().chance(3, 4) || self.uuids == 0 {
            self.seen
        } else {
            match cx.rng().below(3) {
                0 => Leader::Unset,
                1 => Leader::Multi,
                _ => Leader::uuid(LeaderUuid(
                    1 + u128::from(
                        cx.rng()
                            .below(u64::try_from(self.uuids).expect("few uuids")),
                    ),
                )),
            }
        }
    }

    /// The fence of a random writer that holds a uuid.
    fn fence(&self, cx: &mut Cx<'_, Self>) -> Option<Fence> {
        let index = cx.rng().index(WRITERS)?;
        let writer = &self.writers[index];
        writer.uuid.map(|uuid| Fence::Leader {
            uuid,
            expected_seq: Seq(writer.next),
        })
    }

    fn claim(&mut self, cx: &mut Cx<'_, Self>) {
        let index = cx.rng().index(WRITERS).expect("writers");
        self.uuids += 1;
        let uuid = LeaderUuid(self.uuids);
        let old = self.old_leader(cx);
        if let Some(Answer::LeaderSet(view)) = self.choose(
            Command::SetLeader {
                new: Some(uuid),
                old,
            },
            cx,
        ) {
            let writer = &mut self.writers[index];
            writer.uuid = Some(uuid);
            writer.next = view.next_seq.0;
        }
    }

    fn write(&mut self, cx: &mut Cx<'_, Self>, stale: bool) {
        let index = cx.rng().index(WRITERS).expect("writers");
        let Some(uuid) = self.writers[index].uuid else {
            return;
        };
        let next = self.writers[index].next;
        let at = if stale {
            if next == 0 {
                return;
            }
            cx.rng().below(next)
        } else if cx.rng().chance(1, 10) {
            next + 1
        } else {
            next
        };
        let write = Write {
            fence: Fence::Leader {
                uuid,
                expected_seq: Seq(at),
            },
            records: self.records(cx),
        };
        let answer = self.choose(Command::Write(write.clone()), cx);
        let writer = &mut self.writers[index];
        match answer {
            Some(Answer::Written { seq, count }) => {
                writer.next = seq.0 + count;
                writer.sent.push(write);
                if writer.sent.len() > 4 {
                    writer.sent.remove(0);
                }
            }
            Some(
                Answer::Taken(view)
                | Answer::Fenced(view)
                | Answer::Early(view)
                | Answer::Truncated(view)
                | Answer::Below(view),
            ) => writer.next = view.next_seq.0,
            _ => {}
        }
    }

    fn resend(&mut self, cx: &mut Cx<'_, Self>) {
        let index = cx.rng().index(WRITERS).expect("writers");
        let Some(pick) = cx.rng().index(self.writers[index].sent.len()) else {
            return;
        };
        let mut write = self.writers[index].sent[pick].clone();
        let mut new_time = false;
        for record in &mut write.records {
            let time = Some(1000 + cx.rng().below(1000));
            new_time |= record.headers.leader_time != time;
            record.headers.leader_time = time;
        }
        let answer = self.choose(Command::Write(write), cx);
        sometimes!(
            new_time && matches!(answer, Some(Answer::Duplicate { .. })),
            reach::DUPLICATE_TIME
        );
    }

    fn truncate(&mut self, cx: &mut Cx<'_, Self>) {
        let fence = if cx.rng().chance(1, 2) {
            None
        } else {
            match self.fence(cx) {
                Some(Fence::Leader { uuid, .. }) => Some(uuid),
                _ => None,
            }
        };
        let up_to = Seq(cx.rng().below(self.truth.next + 2));
        self.choose(Command::Truncate { fence, up_to }, cx);
    }

    /// `command` is chosen at the next slot; the replica folds it if it is
    /// up. Returns the answer when the replica answered at once.
    fn choose(&mut self, command: Command, cx: &mut Cx<'_, Self>) -> Option<Answer> {
        let verdict = self.truth.apply(&command);
        let segment = self.current_mut();
        segment.chosen.push(command);
        segment.verdicts.push(verdict);
        self.catch_up(cx)
    }

    /// The replica applies every chosen slot it has not applied. Returns
    /// the answer of the last.
    fn catch_up(&mut self, cx: &mut Cx<'_, Self>) -> Option<Answer> {
        let mut last = None;
        loop {
            let segment = self.segments.last_mut().expect("a segment");
            let fold = segment.fold.as_mut()?;
            if segment.applied == segment.chosen.len() {
                return last;
            }
            let command = segment.chosen[segment.applied].clone();
            let verdict = segment.verdicts[segment.applied];
            let mut answer = fold.apply(&command, |seq| segment.log.get(&seq));
            if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, &command) {
                segment.log.insert(*seq, write.clone());
            }
            if matches!(
                self.mutant,
                FoldMutant::BelowInSegment | FoldMutant::BelowJudged
            ) && let Answer::Taken(view) = answer
            {
                answer = Answer::Below(view);
            }
            segment.applied += 1;
            let state = (fold.leader(), fold.next_seq().0, fold.first_seq().0);
            let name = label(&answer);
            // `Below` is the answer of a fenced write whose position is in an
            // older segment (§J2.2): its verdict in the whole log is a
            // duplicate or a taken position, below the base of this segment.
            let below_base = self.mutant == FoldMutant::BelowInSegment
                || matches!(
                    &command,
                    Command::Write(write) if matches!(
                        write.fence,
                        Fence::Leader { expected_seq, .. } if expected_seq < segment.base.next_seq
                    )
                );
            assert!(
                name == verdict
                    || (name == "below" && below_base && (verdict == "dup" || verdict == "taken")),
                "{FOLD}: {command:?} answered {answer:?}, the whole log says {verdict}"
            );
            self.observe(&command, &answer);
            self.emit(
                FoldEvent::Apply {
                    command: command.clone(),
                    answer: name,
                    state,
                },
                cx,
            );
            if name == "below"
                && let Command::Write(write) = command
            {
                self.belows.push((write, verdict));
            }
            last = Some(answer);
        }
    }

    /// The facts of one answer: what the clients learn, and the sites.
    fn observe(&mut self, command: &Command, answer: &Answer) {
        let segments = self.segments.len();
        match answer {
            Answer::LeaderSet(view) => {
                sometimes!(view.leader == Leader::Multi, reach::TO_MULTI);
                if let Command::SetLeader { old, .. } = command {
                    sometimes!(*old == Leader::Multi, reach::TO_SINGLE);
                    if let Leader::Uuid(uuid) = old {
                        self.deposed.entry(uuid.0).or_insert(segments);
                    }
                }
                self.seen = view.leader;
            }
            Answer::Trimmed(view) => sometimes!(view.first_seq.0 > 0, reach::TRIMMED),
            Answer::Written { count, .. } => {
                sometimes!(*count > 1, reach::BATCH);
                sometimes!(segments >= 3, reach::THIRD);
            }
            Answer::Duplicate { .. } => sometimes!(true, reach::DUPLICATE),
            Answer::Taken(_) => sometimes!(true, reach::TAKEN),
            Answer::Early(_) => sometimes!(true, reach::EARLY),
            Answer::Fenced(_) => {
                sometimes!(true, reach::FENCED);
                if let Command::Write(Write {
                    fence: Fence::Leader { uuid, .. },
                    ..
                }) = command
                {
                    sometimes!(
                        self.deposed.get(&uuid.0).is_some_and(|at| *at < segments),
                        reach::MOVED
                    );
                }
            }
            Answer::WrongMode(_) => sometimes!(true, reach::WRONG_MODE),
            Answer::Truncated(_) => sometimes!(true, reach::TRUNCATED),
            Answer::Below(_) => sometimes!(true, reach::BELOW),
            Answer::Empty(_) => sometimes!(true, reach::EMPTY),
            Answer::Reserved(_) => sometimes!(true, reach::RESERVED),
            Answer::LeaderRefused(_) | Answer::Noop => {}
        }
    }

    fn seal(&mut self, cx: &mut Cx<'_, Self>) {
        if self.segments.len() >= self.shape.segments {
            return;
        }
        let segment = self.current();
        let Some(fold) = segment.fold else {
            return;
        };
        if segment.applied < segment.chosen.len() {
            return;
        }
        let boundary = if self.mutant == FoldMutant::StaleBoundary {
            segment.base
        } else {
            fold.boundary()
        };
        self.emit(
            FoldEvent::Seal {
                state: (boundary.leader, boundary.next_seq.0, boundary.first_seq.0),
            },
            cx,
        );
        self.segments.push(Segment::new(boundary));
        self.check();
    }

    fn resolve(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(pick) = cx.rng().index(self.belows.len()) else {
            return;
        };
        let (write, verdict) = self.belows[pick].clone();
        let Fence::Leader { uuid, expected_seq } = write.fence else {
            unreachable!("only a fenced write is answered Below");
        };
        let sealed = self.segments.len() - 1;
        let asked = if self.mutant == FoldMutant::ResolveInForce {
            Some(sealed)
        } else {
            (0..sealed).find(|&index| {
                let segment = &self.segments[index];
                let fold = segment.fold.expect("a sealed segment's replica is up");
                segment.base.next_seq <= expected_seq && expected_seq < fold.next_seq()
            })
        };
        // FOLD lets a `Below` only below the base of the segment in force, and
        // the sealed segments cover every position below it.
        let Some(index) = asked else {
            assert!(
                self.mutant == FoldMutant::BelowInSegment,
                "{RESOLVE}: {write:?} was answered Below, but no sealed segment holds it"
            );
            // The trace control: the client reads the tail instead.
            self.belows.remove(pick);
            return;
        };
        let segment = &self.segments[index];
        let fold = segment.fold.expect("a sealed segment's replica is up");
        let mut answer = resolve(&fold, uuid, expected_seq, &write.records, |seq| {
            segment.log.get(&seq)
        });
        if self.mutant == FoldMutant::ResolveInForce && answer == ResolveAnswer::NotCovered {
            answer = ResolveAnswer::NotWritten;
        }
        let truth = match answer {
            ResolveAnswer::Written { .. } => verdict == "dup",
            ResolveAnswer::NotWritten => verdict == "taken",
            ResolveAnswer::Truncated => expected_seq.0 < self.truth.first,
            ResolveAnswer::NotCovered => false,
        };
        assert!(
            truth,
            "{RESOLVE}: {write:?} resolved {answer:?}, the whole log says {verdict}"
        );
        sometimes!(
            matches!(answer, ResolveAnswer::Written { .. }),
            reach::RESOLVED_WRITTEN
        );
        sometimes!(answer == ResolveAnswer::NotWritten, reach::RESOLVED_NOT);
        self.emit(
            FoldEvent::Resolve {
                segment: index + 1,
                write,
                answer,
            },
            cx,
        );
        self.belows.remove(pick);
    }
}
