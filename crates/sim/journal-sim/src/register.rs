//! The journal's matchmaker register under the walk (#1325 (the matchmaker
//! register as an optional role), `docs/journal/03-segments-and-records.md`
//! §J3.4): register operations and handovers of the matchmaker set at the
//! same time, with lost, late and repeated messages and machines that crash.
//!
//! The machines: X, Y and Z are the first set (generation 0); a handover
//! moves the register to Y, Z and W (generation 1), or, in the wide shape,
//! a second handover races it to X, Y and W. Each machine runs one
//! [`RegisterMember`]; the walk persists its state before a reply leaves
//! when the step says so, and a machine restarts from what it persisted.
//!
//! Two shapes:
//!
//! - [`Shape::TRACED`] follows `specs/tla/segment-records/`: the record of
//!   one segment starts open, sealed or retired, and a fixed list of
//!   operations of the model's kinds runs on it (two seals that race, the
//!   retire, the offload mark and a replica's confirm of it, two
//!   elections, a reshape of the controller of term 1). A read is one step
//!   at a majority of the members that serve. X is the one machine that
//!   crashes, for good. TLC checks the walk against `RegisterTrace.tla`.
//! - [`Shape::WIDE`] starts from a new journal and runs any change on what
//!   the callers last saw, retries an operation that a later one beat or
//!   that the handover moved, races two handovers, and crashes and
//!   restarts every machine.
//!
//! The oracles, written from §J3.4 and `segment-records/`, share no code
//! with the register:
//!
//! - [`CARRIED`]: a write that is done is never lost: each majority that
//!   serves its generation or a later one returns it or a later value, and
//!   the done values, in the order of their generations, never move back
//!   (`Carried`, `NoBackward`, `SealOnce`, `ReshapeFenced`).
//! - [`ACTED`]: a step that acted on a record (the old acceptors retired, a
//!   replica dropped its copy) acted on a value that every serving majority
//!   holds (`RetireKept`, `DropKept`).
//!
//! The liveness claim: once the faults stop, every operation and every
//! handover ends.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};
use paros_journal::register::{
    Ballot, CallPhase, CallStep, Change, HandoverPhase, HandoverStep, MatchmakerGeneration,
    MatchmakerId, MatchmakerSet, MemberState, RegisterCall, RegisterHandover, RegisterMember,
    RegisterValue, Reply, Request, Successor,
};
use paros_journal::segment::{OpeningCall, SegmentId, SegmentRecord, SegmentState};
use paros_journal::{BoundaryState, Grid, Leader, NodeId, RELEASE, Seq, Slot};

pub mod order;

use order::{at_or_after, holds};

/// Oracle: no done write is lost, and the done writes never move back.
pub const CARRIED: &str = "a done register write is held by every later serving majority";
/// Oracle: a step acts only on a value that every serving majority holds.
pub const ACTED: &str = "a step acts on a register value that every serving majority holds";

/// The coverage sites of the register walk.
pub mod reach {
    /// A write done at the first set.
    pub const DONE_OLD: &str = "register walk: a write done at the first set";
    /// A write done at the next set.
    pub const DONE_NEW: &str = "register walk: a write done at the next set";
    /// The guard refused a change.
    pub const REFUSED: &str = "register walk: the guard refused a change";
    /// A later operation beat a write.
    pub const CONFLICT: &str = "register walk: a later operation beat a write";
    /// The handover chose the next set.
    pub const CHOSEN: &str = "register walk: the handover chose the next set";
    /// A member of the next set serves.
    pub const ACTIVATED: &str = "register walk: a member of the next set serves";
    /// The old acceptors retired on a done write.
    pub const RETIRED: &str = "register walk: the old acceptors retired on a done write";
    /// A replica dropped its copy on a done write.
    pub const DROPPED: &str = "register walk: a replica dropped its copy on a done write";
    /// A machine crashed.
    pub const CRASHED: &str = "register walk: a machine crashed";
    /// A frozen member refused an operation (wide only).
    pub const MOVED: &str = "register walk: an operation moved to the next set";
    /// A handover lost to the other one (wide only).
    pub const SUPERSEDED: &str = "register walk: a handover lost to the other one";
    /// A reshape of a deposed controller refused (wide only).
    pub const DEPOSED: &str = "register walk: a reshape of a deposed controller refused";
    /// A machine restarted from its disk (wide only).
    pub const RESTARTED: &str = "register walk: a machine restarted from its disk";
    /// A segment trimmed (wide only).
    pub const TRIMMED: &str = "register walk: a segment trimmed";

    /// The sites of every register walk.
    pub const ALL: &[&str] = &[
        DONE_OLD, DONE_NEW, REFUSED, CONFLICT, CHOSEN, ACTIVATED, RETIRED, DROPPED, CRASHED,
    ];
    /// The sites of the wide walk.
    pub const WIDE: &[&str] = &[MOVED, SUPERSEDED, DEPOSED, RESTARTED, TRIMMED];
}

/// What the walk does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    /// The model's shape: one segment's record, the model's kinds, atomic
    /// reads, one handover, X crashes for good.
    pub traced: bool,
}

impl Shape {
    /// What TLC checks.
    pub const TRACED: Self = Self { traced: true };
    /// Everything.
    pub const WIDE: Self = Self { traced: false };
}

/// A bug planted in the driver of the register walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RegisterMutant {
    /// No bug.
    #[default]
    None,
    /// The driver sends a member's reply before it persists the step, so a
    /// crash loses a write the member acknowledged. [`CARRIED`] catches it.
    SendBeforePersist,
    /// The driver acts on a retire or an offload mark that its operation
    /// only read, before the write is done. [`ACTED`] catches it.
    ActOnRead,
    /// The driver tells the handover that W stored the proposed state, and
    /// never sends it. The oracles pass it (the chosen state still reaches
    /// W), and TLC refuses the decree: the trace control.
    ForgedBootstrap,
}

/// The kinds of operation of `segment-records/`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A first leader seals the segment at slot 1.
    Seal1,
    /// Another first leader seals it at slot 2.
    Seal2,
    /// The leader retires it.
    Retire,
    /// The offloader marks it.
    Offload,
    /// A replica confirms the mark before it drops its copy.
    Drop,
    /// A controller candidate raises the term.
    Elect,
    /// The controller of term 1 opens a segment.
    Reshape1,
}

impl Kind {
    /// The name of the kind in the model.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Seal1 => "seal1",
            Self::Seal2 => "seal2",
            Self::Retire => "retire",
            Self::Offload => "offload",
            Self::Drop => "drop",
            Self::Elect => "elect",
            Self::Reshape1 => "reshape1",
        }
    }
}

/// Where the traced record starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Start {
    /// Segment 1 in force, segment 2 pending.
    Open,
    /// Segment 1 sealed at slot 1 with its boundary state, segment 2 in
    /// force.
    Sealed,
    /// Segment 1 retired.
    Retired,
}

impl Start {
    /// The kinds whose guard in the code is the guard of the model from
    /// this start.
    fn kinds(self) -> &'static [Kind] {
        match self {
            Self::Open => &[Kind::Seal1, Kind::Seal2, Kind::Elect],
            Self::Sealed => &[Kind::Retire, Kind::Elect],
            Self::Retired => &[Kind::Offload, Kind::Drop, Kind::Elect, Kind::Reshape1],
        }
    }
}

/// One step of the register walk, for `RegisterTrace.tla`. The values are
/// the record of segment 1 as the model writes it ([`Abstract`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterEvent {
    /// Operation `i` read at `q` of generation `g`.
    Read {
        /// The operation, from 1.
        i: usize,
        /// The generation.
        g: u64,
        /// The majority.
        q: Vec<MatchmakerId>,
        /// The guard refused the change.
        refused: bool,
        /// The value the read saw.
        seen: Abstract,
    },
    /// Member `m` took the write of operation `i`.
    Write {
        /// The operation, from 1.
        i: usize,
        /// The member.
        m: MatchmakerId,
        /// The value written.
        value: Abstract,
    },
    /// The old acceptors retired.
    Retire,
    /// A replica dropped its copy.
    Drop,
    /// Member `m` froze.
    Stop(MatchmakerId),
    /// The handover built `successor` from the frozen majority `q`.
    Close {
        /// The frozen majority.
        q: Vec<MatchmakerId>,
        /// The successor state.
        successor: AbstractSuccessor,
    },
    /// Member `m` stored `successor`.
    Bootstrap(MatchmakerId, AbstractSuccessor),
    /// The decree chose `successor`.
    Decide(AbstractSuccessor),
    /// Member `m` serves the next generation.
    Activate(MatchmakerId),
    /// Machine `m` is down.
    Crash(MatchmakerId),
}

/// The record of segment 1 as `segment-records/` writes it: the state (0
/// open, 1 sealed, 2 retired, 3 offloaded), the seal end, the term and the
/// count of reshapes.
pub type Abstract = (u64, u64, u64, u64);

/// A successor state: the generation of its write, the value, and its read
/// generation.
pub type AbstractSuccessor = (u64, Abstract, u64);

/// The callers of a wide walk: the most operations in flight at once.
const CALLERS: usize = 3;

/// The done writes before a wide walk hands the set over.
const HANDOVER_AFTER: usize = 8;

/// The machines: X, Y, Z, W.
const POOL: [u64; 4] = [1, 2, 3, 4];

/// The name of a machine in the model.
///
/// # Panics
///
/// When `id` is not a machine of the walk.
#[must_use]
pub fn name(id: MatchmakerId) -> &'static str {
    match id.0 {
        1 => "X",
        2 => "Y",
        3 => "Z",
        4 => "W",
        _ => panic!("the walk has four machines"),
    }
}

fn set(generation: u64, members: &[u64]) -> MatchmakerSet {
    MatchmakerSet::new(
        MatchmakerGeneration(generation),
        members.iter().copied().map(MatchmakerId).collect(),
    )
}

/// The record of segment 1 of `value`, as the model writes it.
///
/// # Panics
///
/// When the value has no segment 1, or segment 1 is in a state the model
/// does not have.
#[must_use]
pub fn abstraction(value: &RegisterValue) -> Abstract {
    let record = value.segment(SegmentId(1)).expect("a traced record");
    let state = match record.state() {
        SegmentState::InForce => 0,
        SegmentState::Sealed => 1,
        SegmentState::Retired => 2,
        SegmentState::Offloaded => 3,
        other => panic!("the model has no state {other:?}"),
    };
    let cfg = value.segments().len() - 2;
    (
        state,
        record.end().map_or(0, |end| end.0),
        value.term().number,
        u64::try_from(cfg).expect("few segments"),
    )
}

fn abstract_successor(successor: &Successor) -> AbstractSuccessor {
    (
        successor.written.op.round,
        abstraction(&successor.written.value),
        successor.read.round,
    )
}

fn opening(term: u64, expected: Option<u64>) -> OpeningCall {
    OpeningCall {
        term,
        expected: expected.map(SegmentId),
        grid: Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("a v1 grid"),
        replicas: vec![NodeId(10), NodeId(11), NodeId(12)],
    }
}

fn boundary() -> BoundaryState {
    BoundaryState {
        next_seq: Seq(3),
        first_seq: Seq(0),
        leader: Leader::Multi,
    }
}

/// The value of the traced record at `start`.
fn start_value(start: Start) -> RegisterValue {
    let mut changes = vec![
        Change::Reshape {
            opening: opening(0, None),
            release: RELEASE,
        },
        Change::Reshape {
            opening: opening(0, Some(1)),
            release: RELEASE,
        },
    ];
    if start != Start::Open {
        changes.push(Change::Seal {
            segment: SegmentId(1),
            end: Slot(1),
        });
        changes.push(Change::Boundary {
            segment: SegmentId(1),
            state: boundary(),
        });
    }
    if start == Start::Retired {
        changes.push(Change::Retire {
            segment: SegmentId(1),
            holders: 2,
            unrepaired_damage: false,
        });
    }
    changes
        .iter()
        .try_fold(RegisterValue::new(), |value, change| value.apply(change))
        .expect("the start of a traced record")
}

/// The change of a traced kind.
fn change_of(kind: Kind) -> Change {
    let one = SegmentId(1);
    match kind {
        Kind::Seal1 => Change::Seal {
            segment: one,
            end: Slot(1),
        },
        Kind::Seal2 => Change::Seal {
            segment: one,
            end: Slot(2),
        },
        Kind::Retire => Change::Retire {
            segment: one,
            holders: 2,
            unrepaired_damage: false,
        },
        Kind::Offload => Change::Offload { segment: one },
        Kind::Drop => Change::Confirm {
            segment: one,
            state: SegmentState::Offloaded,
        },
        Kind::Elect => Change::Elect { holder: NodeId(7) },
        Kind::Reshape1 => Change::Reshape {
            opening: opening(1, Some(2)),
            release: RELEASE,
        },
    }
}

/// Who sent a request, and gets its reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Caller {
    /// Operation `i`, from 0.
    Op(usize),
    /// Handover `h`, from 0.
    Handover(usize),
}

/// A message of the walk.
#[derive(Clone, Debug)]
pub enum RegisterMsg {
    /// A request to a member.
    Request {
        /// The member.
        to: MatchmakerId,
        /// The sender.
        from: Caller,
        /// The request.
        request: Request,
    },
    /// A member's reply.
    Reply {
        /// The caller.
        to: Caller,
        /// The member.
        from: MatchmakerId,
        /// The reply.
        reply: Reply,
    },
}

/// The moves of the register walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisterAction {
    /// An operation starts.
    Begin,
    /// A handover starts.
    Handover,
    /// Every caller sends its requests again; an operation that ended
    /// without a done write runs again (wide only).
    Resend,
}

/// What a step acted on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Acted {
    /// The old acceptors of a segment retired.
    Retired(SegmentId),
    /// A replica dropped its copy of a segment.
    Dropped(SegmentId),
}

/// The state of an operation.
#[derive(Clone, Debug)]
enum Status {
    /// Not started (traced) or waiting to run again (wide).
    Idle,
    /// In flight.
    Running(Box<RegisterCall>),
    /// A majority took its write.
    Done,
    /// It ended with no done write, for good.
    Ended,
}

#[derive(Clone, Debug)]
struct Operation {
    /// The model's kind (traced).
    kind: Option<Kind>,
    change: Change,
    status: Status,
}

/// A machine.
#[derive(Clone, Debug)]
struct Machine {
    disk: MemberState,
    live: Option<RegisterMember>,
}

/// A handover role.
#[derive(Clone, Debug)]
struct Handover {
    role: RegisterHandover,
    target: Vec<MatchmakerId>,
    /// The durable decree round.
    round: u64,
    started: bool,
    ended: bool,
}

/// The register walk.
pub struct RegisterModel {
    shape: Shape,
    mutant: RegisterMutant,
    start: Start,
    machines: BTreeMap<MatchmakerId, Machine>,
    ops: Vec<Operation>,
    handovers: Vec<Handover>,
    /// The next operation generation (wide).
    next_round: u64,
    /// What the callers last saw (wide).
    belief: RegisterValue,
    /// The done writes: their generation, their set's generation and their
    /// value.
    done: Vec<(Ballot, MatchmakerGeneration, RegisterValue)>,
    acted: Vec<Acted>,
    /// The chosen next set.
    chosen: Option<MatchmakerSet>,
}

/// The register walk of `shape` with `mutant` planted, over `seeds` seeds
/// of `steps` chaos steps. At most one machine is down at a time: the set
/// of three survives one failure, and a fault outside that bends nothing
/// (root `AGENTS.md`, "bend, do not break").
#[must_use]
pub fn walk(shape: Shape, mutant: RegisterMutant, seeds: u64, steps: u64) -> Walk<RegisterModel> {
    Walk::<RegisterModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .max_down(1)
        .env_prefix(None)
        .model(move |cx| RegisterModel::build(shape, mutant, cx))
}

impl Model for RegisterModel {
    type Msg = RegisterMsg;
    type Node = MatchmakerId;
    type Action = RegisterAction;
    type Event = RegisterEvent;

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(Shape::WIDE, RegisterMutant::None, cx)
    }

    fn actions() -> &'static [RegisterAction] {
        &[
            RegisterAction::Begin,
            RegisterAction::Handover,
            RegisterAction::Resend,
        ]
    }

    fn weight(action: RegisterAction, phase: Phase) -> u32 {
        match (action, phase) {
            (RegisterAction::Resend, Phase::Chaos) => 2,
            (RegisterAction::Begin, Phase::Chaos) => 6,
            (RegisterAction::Resend, _) | (RegisterAction::Handover, Phase::Chaos) => 1,
            _ => 0,
        }
    }

    fn act(&mut self, action: RegisterAction, cx: &mut Cx<'_, Self>) {
        match action {
            RegisterAction::Begin => {
                if self.shape.traced {
                    self.begin_traced(cx);
                } else {
                    self.begin_wide(cx);
                }
            }
            RegisterAction::Handover => self.start_handover(cx),
            RegisterAction::Resend => self.resend(cx),
        }
    }

    fn deliver(&mut self, message: RegisterMsg, cx: &mut Cx<'_, Self>) {
        match message {
            RegisterMsg::Request { to, from, request } => {
                if let Some(reply) = self.step_member(to, request, cx) {
                    cx.send(RegisterMsg::Reply {
                        to: from,
                        from: to,
                        reply,
                    });
                }
            }
            RegisterMsg::Reply { to, from, reply } => match to {
                Caller::Op(i) => self.op_reply(i, from, reply, cx),
                Caller::Handover(h) => self.handover_reply(h, from, reply, cx),
            },
        }
    }

    fn nodes(&self) -> Vec<MatchmakerId> {
        if self.shape.traced {
            vec![MatchmakerId(1)]
        } else {
            POOL.iter().copied().map(MatchmakerId).collect()
        }
    }

    fn is_up(&self, node: &MatchmakerId) -> bool {
        self.machines[node].live.is_some()
    }

    fn crash(&mut self, node: &MatchmakerId, cx: &mut Cx<'_, Self>) {
        if self.shape.traced && self.stop_waits_on(*node) {
            // The model's `Close` reads a frozen majority that is up: a
            // traced walk keeps a member up while its stop report waits.
            return;
        }
        let machine = self.machines.get_mut(node).expect("a machine");
        if machine.live.take().is_some() {
            sometimes!(true, reach::CRASHED);
            cx.record(RegisterEvent::Crash(*node));
        }
    }

    fn restart(&mut self, node: &MatchmakerId, _cx: &mut Cx<'_, Self>) {
        if self.shape.traced {
            // The model's crash is for good.
            return;
        }
        let machine = self.machines.get_mut(node).expect("a machine");
        if machine.live.is_none() {
            machine.live = Some(RegisterMember::new(*node, machine.disk.clone()));
            sometimes!(true, reach::RESTARTED);
        }
    }

    fn check(&mut self) {
        self.check_carried();
        self.check_acted();
    }

    fn converged(&self) -> Result<(), String> {
        let running: Vec<usize> = self
            .ops
            .iter()
            .enumerate()
            .filter(|(_, op)| matches!(op.status, Status::Running(_)))
            .map(|(i, _)| i)
            .collect();
        let waiting = if self.shape.traced {
            0
        } else {
            self.ops
                .iter()
                .filter(|op| matches!(op.status, Status::Idle))
                .count()
        };
        let handovers = self
            .handovers
            .iter()
            .filter(|h| h.started && !h.ended)
            .count();
        if running.is_empty() && waiting == 0 && handovers == 0 {
            Ok(())
        } else {
            Err(format!(
                "operations in flight {running:?}, waiting {waiting}, handovers in flight {handovers}"
            ))
        }
    }

    fn calm(&mut self, cx: &mut Cx<'_, Self>) {
        for node in POOL.map(MatchmakerId) {
            self.restart(&node, cx);
        }
    }

    fn describe(&self) -> String {
        format!(
            "start {:?}, {} operations, {} done, chosen {:?}",
            self.start,
            self.ops.len(),
            self.done.len(),
            self.chosen
        )
    }
}

impl RegisterModel {
    /// The walk with `shape` and `mutant` planted.
    ///
    /// # Panics
    ///
    /// Never: the draw of the start has three choices.
    pub fn build(shape: Shape, mutant: RegisterMutant, cx: &mut Cx<'_, Self>) -> Self {
        let first = set(0, &[1, 2, 3]);
        let starts = [Start::Open, Start::Sealed, Start::Retired];
        let start = starts[cx.rng().index(starts.len()).expect("starts")];
        let value = if shape.traced {
            start_value(start)
        } else {
            RegisterValue::new()
        };
        let machines = POOL
            .iter()
            .map(|&id| {
                let id = MatchmakerId(id);
                let mut disk = MemberState::first(id, first.clone());
                disk.written.value = value.clone();
                let live = Some(RegisterMember::new(id, disk.clone()));
                (id, Machine { disk, live })
            })
            .collect();
        let ops = if shape.traced {
            let kinds = start.kinds();
            let count = 2 + cx.rng().below(2);
            (0..count)
                .map(|_| {
                    let kind = kinds[cx.rng().index(kinds.len()).expect("kinds")];
                    Operation {
                        kind: Some(kind),
                        change: change_of(kind),
                        status: Status::Idle,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        let mut handovers = vec![Handover {
            role: RegisterHandover::new(NodeId(9), 0),
            target: [2, 3, 4].map(MatchmakerId).to_vec(),
            round: 0,
            started: false,
            ended: false,
        }];
        if !shape.traced {
            handovers.push(Handover {
                role: RegisterHandover::new(NodeId(8), 0),
                target: [1, 2, 4].map(MatchmakerId).to_vec(),
                round: 0,
                started: false,
                ended: false,
            });
        }
        Self {
            shape,
            mutant,
            start,
            machines,
            ops,
            handovers,
            next_round: 1,
            belief: value,
            done: Vec::new(),
            acted: Vec::new(),
            chosen: None,
        }
    }

    /// The model's operations, by kind (a traced walk).
    #[must_use]
    pub fn kinds(&self) -> Vec<Kind> {
        self.ops.iter().filter_map(|op| op.kind).collect()
    }

    /// The record value the traced walk starts from, as the model writes it.
    #[must_use]
    pub fn start(&self) -> Abstract {
        abstraction(&start_value(self.start))
    }

    /// The set of generation `g`: the first set, or the chosen next set.
    fn set_of(&self, g: u64) -> Option<MatchmakerSet> {
        match g {
            0 => Some(set(0, &[1, 2, 3])),
            _ => self.chosen.clone().filter(|s| s.generation.0 == g),
        }
    }

    /// The majorities of generation `g` whose members all serve it.
    fn serving(&self, g: u64) -> Vec<BTreeSet<MatchmakerId>> {
        let Some(set) = self.set_of(g) else {
            return Vec::new();
        };
        let members = set.members();
        let mut majorities = Vec::new();
        for mask in 1u32..(1 << members.len()) {
            let q: BTreeSet<MatchmakerId> = members
                .iter()
                .enumerate()
                .filter(|(k, _)| mask & (1 << k) != 0)
                .map(|(_, m)| *m)
                .collect();
            let serve = q.iter().all(|m| {
                self.machines[m]
                    .live
                    .as_ref()
                    .is_some_and(|member| member.serves(set.generation))
            });
            if 2 * q.len() > members.len() && serve {
                majorities.push(q);
            }
        }
        majorities
    }

    /// The highest write that a read at `q` returns.
    fn top(&self, q: &BTreeSet<MatchmakerId>) -> (Ballot, &RegisterValue) {
        q.iter()
            .map(|m| {
                let state = self.machines[m]
                    .live
                    .as_ref()
                    .expect("a serving member")
                    .state();
                (state.written.op, &state.written.value)
            })
            .max_by_key(|(op, _)| *op)
            .expect("a majority")
    }

    fn check_carried(&self) {
        for (op, generation, value) in &self.done {
            for g in generation.0..=1 {
                for q in self.serving(g) {
                    let (top_op, top) = self.top(&q);
                    assert!(
                        top_op >= *op && at_or_after(top, value),
                        "{CARRIED}: the write of {op:?} at generation {} is lost: \
                         generation {g} majority {q:?} returns {top_op:?} {top:?}",
                        generation.0
                    );
                }
            }
        }
        let mut ordered: Vec<&(Ballot, MatchmakerGeneration, RegisterValue)> =
            self.done.iter().collect();
        ordered.sort_by_key(|(op, _, _)| *op);
        for pair in ordered.windows(2) {
            assert!(
                at_or_after(&pair[1].2, &pair[0].2),
                "{CARRIED}: the done write of {:?} moves back from the one of {:?}",
                pair[1].0,
                pair[0].0
            );
        }
    }

    fn check_acted(&self) {
        for acted in &self.acted {
            for g in 0..=1 {
                for q in self.serving(g) {
                    let (_, top) = self.top(&q);
                    let (segment, least) = match acted {
                        Acted::Retired(segment) => (*segment, SegmentState::Retired),
                        Acted::Dropped(segment) => (*segment, SegmentState::Offloaded),
                    };
                    assert!(
                        holds(top, segment, least),
                        "{ACTED}: {acted:?}, but generation {g} majority {q:?} returns {top:?}"
                    );
                }
            }
        }
    }

    /// Whether a handover waits on the stop report of `node` to close.
    fn stop_waits_on(&self, node: MatchmakerId) -> bool {
        self.handovers.iter().any(|h| {
            matches!(h.role.phase(), HandoverPhase::Stopping { stopped, .. } if stopped.contains_key(&node))
        })
    }

    /// Member `to` answers `request`, if it is up. The driver persists the
    /// step before the reply leaves.
    fn step_member(
        &mut self,
        to: MatchmakerId,
        request: Request,
        cx: &mut Cx<'_, Self>,
    ) -> Option<Reply> {
        let mutant = self.mutant;
        let machine = self.machines.get_mut(&to).expect("a machine");
        let member = machine.live.as_mut()?;
        let before = member.state().clone();
        let write = match &request {
            Request::Write { op, .. } => Some(*op),
            _ => None,
        };
        let step = member.step(request);
        let after = member.state().clone();
        if step.persist && mutant == RegisterMutant::SendBeforePersist && cx.rng().chance(1, 3) {
            // The bug: the reply leaves, then the machine dies before the
            // step reaches its disk.
            machine.live = None;
            cx.record(RegisterEvent::Crash(to));
        } else if step.persist {
            machine.disk = after.clone();
        }
        // The model's events of the member.
        if let (Some(op), Reply::Wrote { .. }) = (write, &step.reply)
            && before.written.op != op
            && self.shape.traced
        {
            cx.record(RegisterEvent::Write {
                i: usize::try_from(op.round).expect("an index"),
                m: to,
                value: abstraction(&after.written.value),
            });
        }
        if before.phase != after.phase || before.set != after.set {
            if after.set.generation > before.set.generation {
                sometimes!(true, reach::ACTIVATED);
                cx.record(RegisterEvent::Activate(to));
            } else {
                cx.record(RegisterEvent::Stop(to));
            }
        }
        if let (Some(staged), true) = (
            &after.staged,
            before.staged != after.staged && self.shape.traced,
        ) {
            cx.record(RegisterEvent::Bootstrap(to, abstract_successor(staged)));
        }
        Some(step.reply)
    }

    /// A traced operation reads at a serving majority, in one step, and
    /// sends its write.
    fn begin_traced(&mut self, cx: &mut Cx<'_, Self>) {
        let idle: Vec<usize> = (0..self.ops.len())
            .filter(|&i| matches!(self.ops[i].status, Status::Idle))
            .collect();
        let Some(pick) = cx.rng().index(idle.len()) else {
            return;
        };
        let i = idle[pick];
        let g = u64::from(self.chosen.is_some() && cx.rng().chance(2, 3));
        // A smallest majority: the code's read ends at the first majority.
        let majorities: Vec<BTreeSet<MatchmakerId>> = self
            .serving(g)
            .into_iter()
            .filter(|q| q.len() == 2)
            .collect();
        let Some(pick) = cx.rng().index(majorities.len()) else {
            return;
        };
        let q = majorities[pick].clone();
        let set = self.set_of(g).expect("a serving set");
        let op = Ballot {
            round: u64::try_from(i + 1).expect("an index"),
            node: NodeId(1),
        };
        let mut call = RegisterCall::new(op, set.clone(), self.ops[i].change.clone());
        let mut outcome = CallStep::Pending;
        for m in &q {
            let reply = self
                .step_member(
                    *m,
                    Request::Read {
                        generation: set.generation,
                        op,
                    },
                    cx,
                )
                .expect("a serving member is up");
            outcome = call.on_reply(*m, reply);
        }
        let (refused, seen) = match &outcome {
            CallStep::Write { seen, .. } => (false, seen.clone()),
            CallStep::Refused { seen, .. } => (true, seen.clone()),
            other => panic!("a read at a serving majority ends: {other:?}"),
        };
        cx.record(RegisterEvent::Read {
            i: i + 1,
            g,
            q: q.into_iter().collect(),
            refused,
            seen: abstraction(&seen),
        });
        self.ops[i].status = Status::Running(Box::new(call));
        self.after_step(i, outcome, cx);
    }

    /// A wide operation: a change on what the callers last saw.
    fn begin_wide(&mut self, cx: &mut Cx<'_, Self>) {
        let running = self
            .ops
            .iter()
            .filter(|op| matches!(op.status, Status::Running(_)))
            .count();
        if self.ops.len() >= 40 || running >= CALLERS {
            return;
        }
        let change = self.wide_change(cx);
        self.ops.push(Operation {
            kind: None,
            change,
            status: Status::Idle,
        });
        let i = self.ops.len() - 1;
        self.run(i, cx);
    }

    fn wide_change(&self, cx: &mut Cx<'_, Self>) -> Change {
        let belief = &self.belief;
        let newest = belief.newest().map(SegmentRecord::id);
        let term = belief.term().number;
        let pick = |cx: &mut Cx<'_, Self>| {
            let index = cx.rng().index(belief.segments().len())?;
            belief.segments().keys().copied().nth(index)
        };
        if cx.rng().chance(4, 5)
            && let Some(change) = self.next_in_life(cx)
        {
            return change;
        }
        match cx.rng().below(9) {
            0 => Change::Elect {
                holder: NodeId(5 + cx.rng().below(2)),
            },
            1 | 2 => {
                // Sometimes a deposed controller, or a stale expectation.
                let term = if cx.rng().chance(1, 4) {
                    term.saturating_sub(1)
                } else {
                    term
                };
                let expected = if cx.rng().chance(1, 5) {
                    newest.and_then(|SegmentId(n)| n.checked_sub(1).map(SegmentId))
                } else {
                    newest
                };
                Change::Reshape {
                    opening: opening(term, expected.map(|id| id.0)),
                    release: RELEASE,
                }
            }
            3 => {
                let segment = belief.in_force().map_or(SegmentId(1), SegmentRecord::id);
                Change::Seal {
                    segment,
                    end: Slot(cx.rng().below(4)),
                }
            }
            4 => Change::Boundary {
                segment: pick(cx).unwrap_or(SegmentId(1)),
                state: boundary(),
            },
            5 => Change::Retire {
                segment: pick(cx).unwrap_or(SegmentId(1)),
                holders: 1 + usize::try_from(cx.rng().below(3)).expect("small"),
                unrepaired_damage: cx.rng().chance(1, 6),
            },
            6 => Change::Offload {
                segment: pick(cx).unwrap_or(SegmentId(1)),
            },
            7 => Change::Trim {
                segment: pick(cx).unwrap_or(SegmentId(1)),
                first_seq: Seq(cx.rng().below(5)),
            },
            _ => Change::Confirm {
                segment: pick(cx).unwrap_or(SegmentId(1)),
                state: SegmentState::Offloaded,
            },
        }
    }

    /// The next step of a segment's life on what the callers saw: the
    /// steer that brings a wide walk to a retire, a drop and a trim.
    fn next_in_life(&self, cx: &mut Cx<'_, Self>) -> Option<Change> {
        let belief = &self.belief;
        if belief.term().number == 0 {
            return Some(Change::Elect { holder: NodeId(5) });
        }
        let newest = belief.newest().map(SegmentRecord::id);
        let reshape = Change::Reshape {
            opening: opening(belief.term().number, newest.map(|id| id.0)),
            release: RELEASE,
        };
        if newest.is_none() {
            return Some(reshape);
        }
        let steps: Vec<Change> = belief
            .segments()
            .values()
            .filter_map(|record| {
                let segment = record.id();
                match record.state() {
                    SegmentState::Pending => belief.in_force().map(|r| Change::Seal {
                        segment: r.id(),
                        end: Slot(r.start().map_or(0, |s| s.0) + cx.rng().below(3)),
                    }),
                    SegmentState::InForce => (Some(segment) == newest).then(|| reshape.clone()),
                    SegmentState::Sealed if record.boundary().is_none() => Some(Change::Boundary {
                        segment,
                        state: boundary(),
                    }),
                    SegmentState::Sealed => Some(Change::Retire {
                        segment,
                        holders: 2,
                        unrepaired_damage: false,
                    }),
                    SegmentState::Retired => Some(Change::Offload { segment }),
                    SegmentState::Offloaded if cx.rng().chance(1, 2) => Some(Change::Confirm {
                        segment,
                        state: SegmentState::Offloaded,
                    }),
                    SegmentState::Offloaded => Some(Change::Trim {
                        segment,
                        first_seq: Seq(3),
                    }),
                    SegmentState::Trimmed => None,
                }
            })
            .collect();
        let pick = cx.rng().index(steps.len())?;
        steps.into_iter().nth(pick)
    }

    /// Run wide operation `i` at a new generation, at the newest set the
    /// callers know.
    fn run(&mut self, i: usize, cx: &mut Cx<'_, Self>) {
        let op = Ballot {
            round: self.next_round,
            node: NodeId(1),
        };
        self.next_round += 1;
        let set = self.chosen.clone().unwrap_or_else(|| set(0, &[1, 2, 3]));
        let call = RegisterCall::new(op, set, self.ops[i].change.clone());
        Self::send_call(i, &call, cx);
        self.ops[i].status = Status::Running(Box::new(call));
    }

    fn send_call(i: usize, call: &RegisterCall, cx: &mut Cx<'_, Self>) {
        for (to, request) in call.requests() {
            cx.send(RegisterMsg::Request {
                to,
                from: Caller::Op(i),
                request,
            });
        }
    }

    fn op_reply(&mut self, i: usize, from: MatchmakerId, reply: Reply, cx: &mut Cx<'_, Self>) {
        let Status::Running(call) = &mut self.ops[i].status else {
            return;
        };
        if self.shape.traced && matches!(call.phase(), CallPhase::Reading(_)) {
            return;
        }
        let step = call.on_reply(from, reply);
        self.after_step(i, step, cx);
    }

    /// What the driver does on one step of operation `i`.
    fn after_step(&mut self, i: usize, step: CallStep, cx: &mut Cx<'_, Self>) {
        let Status::Running(call) = &self.ops[i].status else {
            return;
        };
        let call = call.clone();
        match step {
            CallStep::Pending => {}
            CallStep::Moved { set } => {
                if set.is_some() {
                    sometimes!(true, reach::MOVED);
                    self.ops[i].status = self.ended();
                }
            }
            CallStep::Write { seen, value } => {
                self.see(&seen);
                if self.mutant == RegisterMutant::ActOnRead {
                    self.act_on(i, &value, cx);
                }
                Self::send_call(i, &call, cx);
            }
            CallStep::Done(value) => {
                let generation = call.set().generation;
                sometimes!(generation.0 == 0, reach::DONE_OLD);
                sometimes!(generation.0 == 1, reach::DONE_NEW);
                sometimes!(matches!(call.change(), Change::Trim { .. }), reach::TRIMMED);
                self.done.push((call.op(), generation, value.clone()));
                self.see(&value);
                self.ops[i].status = Status::Done;
                if self.mutant != RegisterMutant::ActOnRead {
                    self.act_on(i, &value, cx);
                }
            }
            CallStep::Refused { refusal, seen } => {
                sometimes!(true, reach::REFUSED);
                sometimes!(
                    matches!(refusal, paros_journal::register::ChangeRefusal::Term { held, asked } if asked < held),
                    reach::DEPOSED
                );
                self.see(&seen);
                self.ops[i].status = Status::Ended;
            }
            CallStep::Conflict { above } => {
                sometimes!(true, reach::CONFLICT);
                self.next_round = self.next_round.max(above.round + 1);
                self.ops[i].status = self.ended();
            }
        }
    }

    /// An operation that ended with no done write: for good in a traced
    /// walk, to run again in a wide one.
    fn ended(&self) -> Status {
        if self.shape.traced {
            Status::Ended
        } else {
            Status::Idle
        }
    }

    fn see(&mut self, value: &RegisterValue) {
        if !self.shape.traced {
            self.belief = value.clone();
        }
    }

    /// The step that acts on the write of operation `i`: the old acceptors
    /// retire, a replica drops its copy.
    fn act_on(&mut self, i: usize, value: &RegisterValue, cx: &mut Cx<'_, Self>) {
        let acted = match self.ops[i].change {
            Change::Retire { segment, .. } => Acted::Retired(segment),
            Change::Offload { segment }
            | Change::Confirm {
                segment,
                state: SegmentState::Offloaded,
            } => Acted::Dropped(segment),
            _ => return,
        };
        if self.acted.contains(&acted) {
            return;
        }
        assert!(
            holds(
                value,
                match acted {
                    Acted::Retired(s) | Acted::Dropped(s) => s,
                },
                match acted {
                    Acted::Retired(_) => SegmentState::Retired,
                    Acted::Dropped(_) => SegmentState::Offloaded,
                }
            ),
            "the value acted on holds the step"
        );
        self.acted.push(acted);
        match acted {
            Acted::Retired(_) => {
                sometimes!(true, reach::RETIRED);
                cx.record(RegisterEvent::Retire);
            }
            Acted::Dropped(_) => {
                sometimes!(true, reach::DROPPED);
                cx.record(RegisterEvent::Drop);
            }
        }
    }

    fn start_handover(&mut self, cx: &mut Cx<'_, Self>) {
        if !self.shape.traced && self.done.len() < HANDOVER_AFTER {
            // A handover stops the writes until it ends: a wide walk lets
            // the life of a segment go some way first.
            return;
        }
        let Some(h) = cx.rng().index(self.handovers.len()) else {
            return;
        };
        let handover = &mut self.handovers[h];
        if handover.started {
            return;
        }
        handover.started = true;
        handover
            .role
            .start(set(0, &[1, 2, 3]), handover.target.clone())
            .expect("a new handover starts");
        self.send_handover(h, cx);
    }

    fn send_handover(&self, h: usize, cx: &mut Cx<'_, Self>) {
        for (to, request) in self.handovers[h].role.requests() {
            cx.send(RegisterMsg::Request {
                to,
                from: Caller::Handover(h),
                request,
            });
        }
    }

    fn handover_reply(
        &mut self,
        h: usize,
        from: MatchmakerId,
        reply: Reply,
        cx: &mut Cx<'_, Self>,
    ) {
        if self.handovers[h].ended {
            return;
        }
        let stopping: Option<Vec<MatchmakerId>> = match self.handovers[h].role.phase() {
            HandoverPhase::Stopping { stopped, .. } => {
                Some(stopped.keys().copied().chain([from]).collect())
            }
            _ => None,
        };
        let step = self.handovers[h].role.on_reply(from, reply);
        match step {
            HandoverStep::Pending => return,
            HandoverStep::Closed(successor) => {
                let mut q = stopping.expect("a close ends the stop");
                q.sort_unstable();
                q.dedup();
                cx.record(RegisterEvent::Close {
                    q,
                    successor: abstract_successor_or_any(&successor, self.shape),
                });
                if self.mutant == RegisterMutant::ForgedBootstrap {
                    let forged = Reply::Bootstrapped {
                        successor: paros_journal::register::Fingerprint::fingerprint(&successor),
                    };
                    let _ = self.handovers[h].role.on_reply(MatchmakerId(4), forged);
                }
            }
            HandoverStep::Round(round) => {
                let handover = &mut self.handovers[h];
                assert!(round > handover.round, "a decree round is new");
                handover.round = round;
            }
            HandoverStep::Chosen(successor) => {
                sometimes!(true, reach::CHOSEN);
                if let Some(chosen) = &self.chosen {
                    assert!(*chosen == successor.set, "one next set is chosen");
                }
                self.chosen = Some(successor.set.clone());
                cx.record(RegisterEvent::Decide(abstract_successor_or_any(
                    &successor, self.shape,
                )));
            }
            HandoverStep::Done(successor) => {
                assert!(
                    self.chosen.as_ref() == Some(&successor.set),
                    "a handover ends on the chosen set"
                );
                self.handovers[h].ended = true;
                return;
            }
            HandoverStep::Superseded(_) => {
                sometimes!(true, reach::SUPERSEDED);
                self.handovers[h].ended = true;
                return;
            }
        }
        self.send_handover(h, cx);
    }

    fn resend(&mut self, cx: &mut Cx<'_, Self>) {
        for h in 0..self.handovers.len() {
            if self.handovers[h].started && !self.handovers[h].ended {
                self.send_handover(h, cx);
            }
        }
        let mut again = false;
        for i in 0..self.ops.len() {
            match &self.ops[i].status {
                Status::Running(call) => {
                    let call = call.clone();
                    if !(self.shape.traced && matches!(call.phase(), CallPhase::Reading(_))) {
                        Self::send_call(i, &call, cx);
                    }
                }
                Status::Idle if !self.shape.traced && !again => {
                    // One operation runs again at each resend: a backoff, so
                    // that two retries do not beat each other for ever.
                    again = true;
                    self.run(i, cx);
                }
                _ => {}
            }
        }
    }
}

/// The successor as the model writes it; a wide walk records no trace.
fn abstract_successor_or_any(successor: &Successor, shape: Shape) -> AbstractSuccessor {
    if shape.traced {
        abstract_successor(successor)
    } else {
        (
            successor.written.op.round,
            (0, 0, 0, 0),
            successor.read.round,
        )
    }
}
