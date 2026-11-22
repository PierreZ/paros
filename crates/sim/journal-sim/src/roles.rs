//! The journal roles under the walk (#1326 (the journal roles over the
//! core), `docs/journal/04-roles.md` §J4.1): acceptors, leaders and the
//! replicas of each segment exchange messages over a network that loses,
//! repeats and reorders them, across a chain of segments that the grid
//! controller opens with reshapes.
//!
//! The deployment: a pool of six acceptors (1 to 6). The first segment's
//! grid is one column of 1, 2 and 3; each reshape draws a new grid from the
//! pool, one column of three, or in the wide shape on one reshape in two
//! two columns of three. Segment `k` has its own replica set, `10k + 1` to
//! `10k + 3`. Three proposers (101 to 103) campaign: each campaign is a
//! new ballot, bound to the newest segment of the register (the
//! matchmakers' binding, abstracted as one counter). A proposer runs one
//! leader: a new campaign stops its earlier one, which answers `Unknown`;
//! a leader that crashes takes no more steps, and its callers time out. A
//! live leader of the newest segment is deposed only in the chaos phase.
//!
//! The driver keeps the flight rule (§J4.2): an acceptor's or a replica's
//! batch becomes durable at a `Flush` move, a send waits for the batch it
//! names, and a crash loses the batches in flight and the sends that wait
//! on them. The register is atomic: a change queued by a role is done or
//! refused at a `Register` move, and every role then observes the value.
//! The walk retires a sealed segment whose boundary state is written once
//! `F + 1` replicas of its set hold it (the leader's retire is a later rule
//! group of #1326).
//!
//! The clients: three writers write, resend, write at old positions,
//! switch the journal to multi-writer and back, and truncate, as in the
//! fold walk. Each client has one call in flight; a call answered `Unknown`
//! is sent again; a client learns the leader and the next position from
//! every view it is answered; a `Below` is settled with `Resolve` at a
//! replica of an older segment.
//!
//! The oracles, written from `docs/journal/` and `specs/tla/journal-state/`,
//! share no code with the roles:
//!
//! - [`AGREE`]: one command is chosen at each slot (`Agreement`).
//! - [`SEALED`]: a grid chooses nothing at or above the end of its sealed
//!   segment (`SealedFrozen`).
//! - [`FOLD`]: each answer of a replica is the whole log's, or `Below` for a
//!   resend across a seal (`AckTrue`).
//! - [`VIEW`]: each replica's fold is the fold of the whole log at its front
//!   (`OneFold`).
//! - [`BOUNDARY`]: a boundary state written in the register is the fold of
//!   the whole log at the end of its segment (`RecordTrue`).
//! - [`RESOLVE`]: a `Resolve` answers the fate that the whole log gives
//!   (`ResolveTrue`).
//!
//! The liveness claim: once the faults stop, a leader of the newest
//! segment writes, every chosen slot is applied by every replica of its
//! segment, every sealed segment has its boundary state, and every `Below`
//! is resolved.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};

use paros_journal::register::{Change, RegisterValue};
use paros_journal::resolve::ResolveAnswer;
use paros_journal::roles::acceptor::AcceptorDisk;
use paros_journal::roles::replica::ReplicaDisk;
use paros_journal::roles::{
    AcceptorEffect, AcceptorWrite, Campaign, JournalAcceptor, JournalLeader, LeaderEffect,
    LeaderPhase, Message, PROMISE_BATCH, PersistId, Proposed, ReplicaEffect, ReplicaOp,
    SegmentReplica,
};
use paros_journal::segment::{OpeningCall, SegmentId, SegmentRecord, SegmentState};
use paros_journal::{
    Answer, BoundaryState, Command, Fence, Grid, JournalView, Leader, LeaderUuid, NodeId, RELEASE,
    Record, Reply, Seq, Slot, Write,
};

use crate::oracle::{Truth, Verdict, label};

/// Oracle: one command is chosen at each slot.
pub const AGREE: &str = "one command is chosen at each slot";
/// Oracle: a sealed grid chooses nothing at or above its end.
pub const SEALED: &str = "a grid chooses nothing at or above the end of its sealed segment";
/// Oracle: each answer is the whole log's, or `Below`.
pub const FOLD: &str =
    "each replica answer is the whole log's, or Below for a resend across a seal";
/// Oracle: each replica's fold is the whole log's.
pub const VIEW: &str = "each replica's fold is the fold of the whole log at its front";
/// Oracle: a boundary write is the whole log's state at the end.
pub const BOUNDARY: &str = "a boundary state is the fold of the whole log at the segment end";
/// Oracle: a `Resolve` answers the fate that the whole log gives.
pub const RESOLVE: &str = "a Resolve at a replica answers the fate that the whole log gives";

/// The coverage sites of the roles walk.
pub mod reach {
    /// A leader sealed a segment.
    pub const SEALED: &str = "roles walk: a first leader sealed the segment before";
    /// A first leader lost the seal to another.
    pub const SEAL_LOST: &str = "roles walk: a first leader lost the seal to another";
    /// A `Noop` of the gap fill chosen below the start of a segment.
    pub const GAP_NOOP: &str = "roles walk: a Noop filled a slot below the segment start";
    /// A vote of an old grid chosen again in a new grid.
    pub const REPROPOSED: &str = "roles walk: an old grid's vote was chosen again in a new one";
    /// A write applied in the third segment.
    pub const THIRD: &str = "roles walk: a write applied in the third segment";
    /// A boundary state written.
    pub const BOUNDARY: &str = "roles walk: a replica wrote the boundary state";
    /// A segment retired.
    pub const RETIRED: &str = "roles walk: a sealed segment retired";
    /// A replica took slots from a peer.
    pub const PULLED: &str = "roles walk: a replica took a slot from a peer";
    /// A leader stopped on a higher promise.
    pub const NACKED: &str = "roles walk: a leader stopped on a higher promise";
    /// A held command answered `Unknown`.
    pub const UNKNOWN: &str = "roles walk: a held command was answered Unknown";
    /// A resend across a seal answered `Below`.
    pub const BELOW: &str = "roles walk: a resend across a seal answered Below";
    /// A `Resolve` answered written.
    pub const RESOLVED: &str = "roles walk: a Resolve answered written";
    /// A resend answered `Duplicate`.
    pub const DUPLICATE: &str = "roles walk: a resend answered Duplicate";
    /// A truncate raised the floor.
    pub const TRIMMED: &str = "roles walk: a truncate raised the floor";
    /// `SetLeader(None)` applied.
    pub const TO_MULTI: &str = "roles walk: SetLeader(None) made the journal multi-writer";
    /// A Phase 1 took a promise in more than one page.
    pub const PAGED: &str = "roles walk: a promise came in more than one page";
    /// An acceptor restarted from its disk with a vote.
    pub const RESTARTED: &str = "roles walk: an acceptor restarted from its disk with a vote";
    /// A slot chosen in a grid of two columns (wide only).
    pub const COLUMNS: &str = "roles walk: a slot chosen in a grid of two columns";

    /// The sites of every roles walk.
    pub const ALL: &[&str] = &[
        SEALED, SEAL_LOST, GAP_NOOP, REPROPOSED, THIRD, BOUNDARY, RETIRED, PULLED, NACKED, UNKNOWN,
        BELOW, RESOLVED, DUPLICATE, TRIMMED, TO_MULTI, RESTARTED,
    ];
    /// The sites of the wide walk.
    pub const WIDE: &[&str] = &[COLUMNS, PAGED];
}

/// What the walk does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    /// Grids of two columns, and writes of many slots (promises in pages).
    pub wide: bool,
}

impl Shape {
    /// One column for each grid.
    pub const NARROW: Self = Self { wide: false };
    /// Two columns on some reshapes, and promises in pages of two.
    pub const WIDE: Self = Self { wide: true };

    /// The entries of one promise page.
    fn promise_page(self) -> usize {
        if self.wide { 2 } else { PROMISE_BATCH }
    }
}

/// A bug planted in the driver of the roles walk, never in the roles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RolesMutant {
    /// No bug.
    #[default]
    None,
    /// An acceptor restarts with an empty disk. [`AGREE`] catches it.
    WipeDisk,
    /// A campaign binds its ballot to the segment before a sealed one, as
    /// if it were still in force. [`SEALED`] catches it.
    StaleBinding,
    /// The driver turns a replica's `Duplicate` into a taken position.
    /// [`FOLD`] catches it.
    DuplicateAsTaken,
    /// A replica of a segment takes the boundary state of the segment two
    /// before (the fresh journal for the second). [`VIEW`] catches it.
    StaleBase,
    /// The boundary write carries the base of the segment, not its fold at
    /// the end. [`BOUNDARY`] catches it.
    StaleBoundary,
    /// The client asks the replica of the segment in force to resolve, and
    /// takes "not covered" as not written. [`RESOLVE`] catches it.
    ResolveInForce,
    /// The trace control: the driver runs a replica's boundary write before
    /// the batch it waits on is durable. The write is the fold of chosen
    /// slots, so every oracle passes; `RolesTrace.tla` refuses it, as
    /// `BoundWrite` needs every slot of the segment on the replica's disk.
    BoundaryBeforeDurable,
}

/// One step of the roles walk that changes the state of
/// `specs/tla/journal-state/Journal.tla`: each is one action of the model
/// (`crate::trace::roles`). A ballot is its round; an acceptor, a replica
/// and a segment are their numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RolesEvent {
    /// The controller opened segment `g` (`Reshape`).
    Reshape {
        /// The new segment.
        g: u64,
        /// Its acceptors.
        grid: Vec<u64>,
    },
    /// A campaign of ballot `b` bound to the newest segment, which writes
    /// from `from` (`Register`).
    Register {
        /// The ballot.
        b: u64,
        /// The first slot its leader writes.
        from: u64,
    },
    /// Acceptor `a` raised its promise to `b` and queued the write
    /// (`Promise`).
    Promise {
        /// The acceptor.
        a: u64,
        /// The ballot.
        b: u64,
    },
    /// Acceptor `a` voted `b` at `s` and queued the write (`Accept`).
    Accept {
        /// The acceptor.
        a: u64,
        /// The ballot.
        b: u64,
        /// The slot.
        s: u64,
    },
    /// Acceptor `a` made its first queued batch durable (`Flush`).
    Flush {
        /// The acceptor.
        a: u64,
    },
    /// Acceptor `a` crashed: its queue and its memory are lost (`Crash`).
    Crash {
        /// The acceptor.
        a: u64,
    },
    /// The leader of `b` closed its Phase 1 on the complete reports of
    /// `q`, with the seal end `end` when it is a first leader (`Phase1`).
    Phase1 {
        /// The ballot.
        b: u64,
        /// The acceptors whose reports it read.
        q: Vec<u64>,
        /// The end it writes, for a first leader.
        end: Option<u64>,
    },
    /// The register took the seal end of the leader of `b` (`SealWrite`).
    Seal {
        /// The ballot.
        b: u64,
    },
    /// The leader of `b` writes from `start`, the start of its segment
    /// (`Open`).
    Open {
        /// The ballot.
        b: u64,
        /// The start of its segment.
        start: u64,
    },
    /// The leader of `b` proposed `command` at `s` (`Propose`).
    Propose {
        /// The ballot.
        b: u64,
        /// The slot.
        s: u64,
        /// The command.
        command: Command,
    },
    /// Replica `r` took its base, durably (`TakeBase`).
    Base {
        /// The replica.
        r: u64,
    },
    /// Replica `r` applied `command` at slot `s`, durably, and answered
    /// `res` (`Apply`).
    Apply {
        /// The replica.
        r: u64,
        /// The slot.
        s: u64,
        /// The command.
        command: Command,
        /// The answer, by its name in the model.
        res: &'static str,
    },
    /// The register took the boundary state of replica `r` (`BoundWrite`).
    Boundary {
        /// The replica.
        r: u64,
    },
    /// Segment `p` retired (`Retire`).
    Retire {
        /// The segment.
        p: u64,
    },
    /// Replica `r` answered `res` to the `Resolve` of the write at `s`
    /// (`ResolveBelow`).
    Resolve {
        /// The slot of the write answered `Below`.
        s: u64,
        /// The replica.
        r: u64,
        /// The answer, by its name in the model.
        res: &'static str,
    },
}

/// One message on the wire.
#[derive(Clone, Debug)]
pub struct Envelope {
    from: NodeId,
    to: NodeId,
    message: Message,
}

/// The moves of the roles walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolesAction {
    /// A proposer campaigns with a new ballot.
    Campaign,
    /// A writer claims the journal with a fresh uuid.
    Claim,
    /// A writer writes at the position it believes next.
    Write,
    /// A writer resends one of its writes.
    Resend,
    /// A writer writes something new at an old position.
    Stale,
    /// A client makes the journal multi-writer, or writes unfenced.
    Multi,
    /// A client truncates.
    Truncate,
    /// The grid controller opens the next segment.
    Reshape,
    /// The walk retires a sealed segment.
    Retire,
    /// The register runs one queued change.
    Register,
    /// A role's batch becomes durable.
    Flush,
    /// A role's periodic step.
    Tick,
    /// A client resolves a `Below`.
    Resolve,
}

/// A role with a disk, under the flight rule.
#[derive(Clone, Debug)]
struct Durable<Role, Disk, Op> {
    role: Option<Role>,
    disk: Disk,
    batches: VecDeque<(PersistId, Vec<Op>)>,
    held: Vec<(PersistId, Envelope)>,
    /// The register changes that wait on a batch.
    changes: Vec<(PersistId, Change)>,
}

impl<Role, Disk, Op> Durable<Role, Disk, Op> {
    fn new(role: Role, disk: Disk) -> Self {
        Self {
            role: Some(role),
            disk,
            batches: VecDeque::new(),
            held: Vec::new(),
            changes: Vec::new(),
        }
    }

    fn crash(&mut self) {
        self.role = None;
        self.batches.clear();
        self.held.clear();
        self.changes.clear();
    }
}

type AcceptorNode = Durable<JournalAcceptor, AcceptorDisk, paros_journal::roles::AcceptorOp>;
type ReplicaNode = Durable<SegmentReplica, ReplicaDisk, ReplicaOp>;

/// Who queued a register change.
#[derive(Clone, Copy, Debug)]
enum Origin {
    /// A leader, by its node and its ballot's round.
    Leader(NodeId, u64),
    Replica(NodeId),
}

/// A writer.
#[derive(Clone, Debug, Default)]
struct Writer {
    uuid: Option<LeaderUuid>,
    /// The uuid of this writer's last claim, whose answer may be lost.
    claimed: Option<LeaderUuid>,
    next: u64,
    sent: Vec<Write>,
}

/// A client call in flight.
#[derive(Clone, Debug)]
struct Call {
    writer: Option<usize>,
    ballot: paros_journal::register::Ballot,
    slot: Slot,
    command: Command,
}

/// The acceptor pool.
const POOL: u64 = 6;

/// The rounds and the commits a leader re-sends on one tick: a page small
/// against the walk's mailbox.
const RESEND_PAGE: usize = 4;
/// The proposers.
const PROPOSERS: [u64; 3] = [101, 102, 103];
/// The writers.
const WRITERS: usize = 3;
/// The most segments of one walk.
const MAX_SEGMENTS: u64 = 4;

/// The roles walk.
pub struct RolesModel {
    shape: Shape,
    mutant: RolesMutant,
    initial: Leader,
    register: RegisterValue,
    queue: Vec<(Origin, Change)>,
    acceptors: BTreeMap<NodeId, AcceptorNode>,
    replicas: BTreeMap<NodeId, ReplicaNode>,
    leaders: BTreeMap<NodeId, JournalLeader<u64>>,
    /// The segment each ballot is bound to.
    bound: BTreeMap<(u64, NodeId), SegmentId>,
    round: u64,
    /// Every slot some leader saw chosen: its command and the ballot.
    chosen: BTreeMap<Slot, (Command, u64, NodeId)>,
    /// The truth over the chosen prefix: its state before each slot, and
    /// its verdict on each slot.
    truth: Truth,
    states: Vec<(Leader, u64, u64)>,
    verdicts: Vec<Verdict>,
    writers: Vec<Writer>,
    seen: Leader,
    calls: BTreeMap<u64, Call>,
    tokens: u64,
    uuids: u128,
    /// Writes answered `Below`, with their slot.
    belows: Vec<(Write, Slot)>,
    /// The calls answered `Unknown`, which their client sends again.
    unknown: Vec<(Option<usize>, Command)>,
    /// The command each ballot proposed at each slot, by round. A ballot
    /// proposes one command at a slot: the trace records the first `Accept`
    /// of each `(round, slot)`, and the model's `Accept` votes that proposal.
    proposed: BTreeMap<(u64, Slot), Command>,
    /// The durable front each replica's `Apply` events reached.
    traced: BTreeMap<NodeId, Slot>,
}

/// The walk of `shape` with `mutant` planted.
#[must_use]
pub fn walk(shape: Shape, mutant: RolesMutant, seeds: u64, steps: u64) -> Walk<RolesModel> {
    Walk::<RolesModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .env_prefix(None)
        .model(move |cx| RolesModel::build(shape, mutant, cx))
}

impl Model for RolesModel {
    type Msg = Envelope;
    type Node = NodeId;
    type Action = RolesAction;
    type Event = RolesEvent;

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(Shape::WIDE, RolesMutant::None, cx)
    }

    fn actions() -> &'static [RolesAction] {
        &[
            RolesAction::Campaign,
            RolesAction::Claim,
            RolesAction::Write,
            RolesAction::Resend,
            RolesAction::Stale,
            RolesAction::Multi,
            RolesAction::Truncate,
            RolesAction::Reshape,
            RolesAction::Retire,
            RolesAction::Register,
            RolesAction::Flush,
            RolesAction::Tick,
            RolesAction::Resolve,
        ]
    }

    fn weight(action: RolesAction, phase: Phase) -> u32 {
        match (action, phase) {
            (
                RolesAction::Campaign
                | RolesAction::Register
                | RolesAction::Flush
                | RolesAction::Tick
                | RolesAction::Resolve
                | RolesAction::Retire,
                Phase::Settle,
            ) => 1,
            (_, Phase::Settle) => 0,
            (RolesAction::Flush, _) => 30,
            (RolesAction::Register, _) => 10,
            (RolesAction::Tick, _) => 6,
            (RolesAction::Resolve, _) => 4,
            (_, Phase::Quiet) => match action {
                RolesAction::Campaign | RolesAction::Retire => 1,
                _ => 0,
            },
            (RolesAction::Write, _) => 12,
            (RolesAction::Resend, _) => 5,
            (
                RolesAction::Claim
                | RolesAction::Reshape
                | RolesAction::Retire
                | RolesAction::Stale,
                _,
            ) => 2,
            (RolesAction::Campaign | RolesAction::Multi | RolesAction::Truncate, _) => 1,
        }
    }

    fn act(&mut self, action: RolesAction, cx: &mut Cx<'_, Self>) {
        match action {
            RolesAction::Campaign => self.campaign(cx),
            RolesAction::Claim => self.claim(cx),
            RolesAction::Write => self.write(cx, false),
            RolesAction::Stale => self.write(cx, true),
            RolesAction::Resend => self.resend(cx),
            RolesAction::Multi => self.multi(cx),
            RolesAction::Truncate => self.truncate(cx),
            RolesAction::Reshape => self.reshape(cx),
            RolesAction::Retire => self.retire(cx),
            RolesAction::Register => self.register_step(cx),
            RolesAction::Flush => self.flush(cx),
            RolesAction::Tick => self.tick(cx),
            RolesAction::Resolve => self.resolve(cx),
        }
    }

    fn deliver(&mut self, envelope: Envelope, cx: &mut Cx<'_, Self>) {
        let Envelope { from, to, message } = envelope;
        if self.acceptors.contains_key(&to) {
            // A retired grid's acceptors stop: only an acceptor of a grid
            // that is not retired takes a step (`Active` of the model).
            if !self.active(to) {
                return;
            }
            let node = self.acceptors.get_mut(&to).expect("an acceptor");
            if let Some(role) = &mut node.role {
                let effects = role.step(from, message);
                for effect in &effects {
                    if let AcceptorEffect::Persist { ops, .. } = effect {
                        cx.record(Self::queued(to, ops));
                    }
                }
                self.acceptor_effects(to, effects, cx);
            }
        } else if self.replicas.contains_key(&to) {
            let pulled =
                matches!(message, Message::Commit { .. }) && self.replicas.contains_key(&from);
            let node = self.replicas.get_mut(&to).expect("a replica");
            if let Some(role) = &mut node.role {
                let before = role.front();
                let effects = role.step(from, message);
                sometimes!(pulled && role.front() > before, reach::PULLED);
                self.replica_effects(to, effects, cx);
            }
        } else if let Some(leader) = self.leaders.get_mut(&to) {
            let nack = matches!(message, Message::Nack { .. });
            sometimes!(
                matches!(&message, Message::Promise { page, .. } if page.next.is_some()),
                reach::PAGED
            );
            let before = leader.phase().clone();
            let effects = leader.step(from, message);
            sometimes!(
                nack && *leader.phase() == LeaderPhase::Stopped,
                reach::NACKED
            );
            if before == LeaderPhase::Phase1 {
                let b = leader.ballot().round;
                let q = leader.phase1().complete().iter().map(|n| n.0).collect();
                match *leader.phase() {
                    LeaderPhase::Sealing { end } => cx.record(RolesEvent::Phase1 {
                        b,
                        q,
                        end: Some(end.0),
                    }),
                    LeaderPhase::Open { start } => {
                        cx.record(RolesEvent::Phase1 { b, q, end: None });
                        cx.record(RolesEvent::Open { b, start: start.0 });
                    }
                    LeaderPhase::Phase1 | LeaderPhase::Stopped => {}
                }
            }
            self.leader_effects(to, effects, cx);
        }
    }

    fn nodes(&self) -> Vec<NodeId> {
        self.acceptors
            .keys()
            .chain(self.replicas.keys())
            .copied()
            .chain(PROPOSERS.iter().map(|p| NodeId(*p)))
            .collect()
    }

    fn is_up(&self, node: &NodeId) -> bool {
        if let Some(acceptor) = self.acceptors.get(node) {
            acceptor.role.is_some()
        } else if let Some(replica) = self.replicas.get(node) {
            replica.role.is_some()
        } else {
            true
        }
    }

    fn crash(&mut self, node: &NodeId, cx: &mut Cx<'_, Self>) {
        if let Some(acceptor) = self.acceptors.get_mut(node) {
            if acceptor.role.is_some() {
                cx.record(RolesEvent::Crash { a: node.0 });
            }
            acceptor.crash();
        } else if let Some(replica) = self.replicas.get_mut(node) {
            replica.crash();
        } else if let Some(leader) = self.leaders.remove(node) {
            // The callers of a crashed leader time out: the fate of each
            // call is unknown.
            let lost: Vec<u64> = self
                .calls
                .iter()
                .filter(|(_, call)| call.ballot == leader.ballot())
                .map(|(token, _)| *token)
                .collect();
            for token in lost {
                self.reply(token, Reply::Unknown);
            }
        }
    }

    fn restart(&mut self, node: &NodeId, cx: &mut Cx<'_, Self>) {
        if let Some(acceptor) = self.acceptors.get_mut(node) {
            if acceptor.role.is_some() {
                return;
            }
            if self.mutant == RolesMutant::WipeDisk {
                acceptor.disk = AcceptorDisk::default();
            }
            sometimes!(!acceptor.disk.votes.is_empty(), reach::RESTARTED);
            acceptor.role = Some(JournalAcceptor::recover(
                *node,
                acceptor.disk.clone(),
                self.shape.promise_page(),
            ));
        } else if let Some(replica) = self.replicas.get(node) {
            if replica.role.is_some() {
                return;
            }
            let segment = Self::segment_of_replica(*node);
            let set = Self::set_of(segment);
            let role =
                SegmentReplica::recover(*node, segment, set, self.initial, replica.disk.clone());
            self.replicas.get_mut(node).expect("a replica").role = Some(role);
            self.observe_replica(*node, cx);
        }
    }

    fn check(&mut self) {
        for (node, replica) in &self.replicas {
            let Some(role) = &replica.role else { continue };
            let (Some(state), Some(front)) = (role.state(), role.front()) else {
                continue;
            };
            let at = usize::try_from(front.0).expect("a small slot");
            let truth = self.states.get(at);
            assert!(
                truth == Some(&(state.leader(), state.next_seq().0, state.first_seq().0)),
                "{VIEW}: replica {node:?} at front {front:?} has {state:?}, the whole log {truth:?}"
            );
        }
    }

    fn converged(&self) -> Result<(), String> {
        if !self.queue.is_empty() {
            return Err("register changes are queued".into());
        }
        if self.acceptors.values().any(|a| !a.batches.is_empty())
            || self.replicas.values().any(|r| !r.batches.is_empty())
        {
            return Err("batches are in flight".into());
        }
        let newest = self.register.newest().expect("a segment");
        if !self.leaders.values().any(|leader| {
            leader.segment() == newest.id() && matches!(leader.phase(), LeaderPhase::Open { .. })
        }) {
            return Err(format!("no leader writes in {:?}", newest.id()));
        }
        let frontier = Slot(u64::try_from(self.verdicts.len()).expect("few slots"));
        if self
            .chosen
            .keys()
            .next_back()
            .is_some_and(|s| *s >= frontier)
        {
            return Err(format!("the chosen log has a hole at {frontier:?}"));
        }
        for record in self.register.segments().values() {
            if record.state() == SegmentState::Sealed && record.boundary().is_none() {
                return Err(format!("{:?} has no boundary state", record.id()));
            }
            if record.state() >= SegmentState::Retired || record.state() == SegmentState::Pending {
                continue;
            }
            let end = record.end().unwrap_or(frontier);
            for node in record.replicas() {
                let role = self.replicas[node].role.as_ref();
                if role.is_some_and(|r| r.front() != Some(end)) {
                    return Err(format!("replica {node:?} is not at {end:?}"));
                }
            }
        }
        if !self.belows.is_empty() {
            return Err(format!(
                "{} Below answers are not resolved",
                self.belows.len()
            ));
        }
        Ok(())
    }

    fn describe(&self) -> String {
        format!(
            "register {:?}\nchosen to {} slots, truth {:?}\nleaders {:?}",
            self.register
                .segments()
                .values()
                .map(|r| (r.id(), r.state(), r.start(), r.end()))
                .collect::<Vec<_>>(),
            self.verdicts.len(),
            self.truth,
            self.leaders
                .values()
                .map(|l| (l.ballot(), l.segment(), l.phase().clone()))
                .collect::<Vec<_>>()
        )
    }
}

impl RolesModel {
    /// The walk of `shape` with `mutant` planted. The fresh journal is
    /// unset, or multi-writer on one seed in four.
    ///
    /// # Panics
    ///
    /// When the first segment cannot open.
    pub fn build(shape: Shape, mutant: RolesMutant, cx: &mut Cx<'_, Self>) -> Self {
        let initial = if cx.rng().chance(1, 4) {
            Leader::Multi
        } else {
            Leader::Unset
        };
        let acceptors = (1..=POOL)
            .map(|id| {
                let node = NodeId(id);
                let disk = AcceptorDisk::default();
                (
                    node,
                    Durable::new(
                        JournalAcceptor::recover(node, disk.clone(), shape.promise_page()),
                        disk,
                    ),
                )
            })
            .collect();
        let mut model = Self {
            shape,
            mutant,
            initial,
            register: RegisterValue::new(),
            queue: Vec::new(),
            acceptors,
            replicas: BTreeMap::new(),
            leaders: BTreeMap::new(),
            bound: BTreeMap::new(),
            round: 0,
            chosen: BTreeMap::new(),
            truth: Truth::new(initial),
            states: vec![(initial, 0, 0)],
            verdicts: Vec::new(),
            writers: vec![Writer::default(); WRITERS],
            seen: initial,
            calls: BTreeMap::new(),
            tokens: 0,
            uuids: 0,
            belows: Vec::new(),
            unknown: Vec::new(),
            proposed: BTreeMap::new(),
            traced: BTreeMap::new(),
        };
        model.register = model
            .register
            .apply(&Change::Elect { holder: NodeId(0) })
            .expect("the first election");
        let grid = Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("a grid");
        model.open_segment(grid, cx);
        model
    }

    fn set_of(segment: SegmentId) -> Vec<NodeId> {
        (1..=3).map(|i| NodeId(10 * segment.0 + i)).collect()
    }

    fn segment_of_replica(node: NodeId) -> SegmentId {
        SegmentId(node.0 / 10)
    }

    /// The reshape call of the controller: the next segment with `grid`.
    fn open_segment(&mut self, grid: Grid, cx: &mut Cx<'_, Self>) {
        let newest = self.register.newest().map(SegmentRecord::id);
        let id = SegmentId(newest.map_or(1, |SegmentId(n)| n + 1));
        let replicas = Self::set_of(id);
        let change = Change::Reshape {
            opening: OpeningCall {
                term: self.register.term().number,
                expected: newest,
                grid,
                replicas: replicas.clone(),
            },
            release: RELEASE,
        };
        let Change::Reshape { opening, .. } = &change else {
            unreachable!("the change is a reshape");
        };
        let grid: Vec<u64> = opening
            .grid
            .columns()
            .iter()
            .flatten()
            .map(|n| n.0)
            .collect();
        let Ok(value) = self.register.apply(&change) else {
            return;
        };
        self.register = value;
        if id.0 >= 2 {
            cx.record(RolesEvent::Reshape { g: id.0, grid });
        }
        for node in &replicas {
            let disk = ReplicaDisk::default();
            let role =
                SegmentReplica::recover(*node, id, replicas.clone(), self.initial, disk.clone());
            self.replicas.insert(*node, Durable::new(role, disk));
        }
        self.observe_all(cx);
    }

    fn reshape(&mut self, cx: &mut Cx<'_, Self>) {
        let newest = self.register.newest().expect("a segment");
        if newest.id().0 >= MAX_SEGMENTS {
            return;
        }
        let mut pool: Vec<NodeId> = (1..=POOL).map(NodeId).collect();
        // A draw of the pool's order.
        for i in (1..pool.len()).rev() {
            let j = usize::try_from(cx.rng().below(u64::try_from(i + 1).expect("small")))
                .expect("small");
            pool.swap(i, j);
        }
        let columns = if self.shape.wide && cx.rng().chance(1, 2) {
            vec![pool[..3].to_vec(), pool[3..].to_vec()]
        } else {
            vec![pool[..3].to_vec()]
        };
        let grid = Grid::new(1, columns).expect("a v1 grid");
        self.open_segment(grid, cx);
    }

    /// The walk's retire: a sealed segment whose boundary state is written,
    /// once `F + 1` replicas of its set hold every slot of it on their disk.
    fn retire(&mut self, cx: &mut Cx<'_, Self>) {
        let sealed: Vec<SegmentId> = self
            .register
            .segments()
            .values()
            .filter(|r| r.state() == SegmentState::Sealed && r.boundary().is_some())
            .map(SegmentRecord::id)
            .collect();
        for segment in sealed {
            let end = self.register.segment(segment).and_then(SegmentRecord::end);
            let holders = Self::set_of(segment)
                .iter()
                .filter(|node| end.is_some() && self.disk_front(**node) == end)
                .count();
            if let Ok(value) = self.register.apply(&Change::Retire {
                segment,
                holders,
                unrepaired_damage: false,
            }) {
                self.register = value;
                cx.record(RolesEvent::Retire { p: segment.0 });
                sometimes!(true, reach::RETIRED);
            }
        }
    }

    /// A proposer campaigns: a new ballot, bound to the newest segment.
    fn campaign(&mut self, cx: &mut Cx<'_, Self>) {
        let newest = self.register.newest().expect("a segment").id();
        // A live leader of the newest segment is deposed only in the chaos
        // phase, on one campaign in eight: a walk that deposes every leader
        // soon answers most calls `Unknown`.
        // A Phase 1 that reads a segment retired since its campaign waits
        // for acceptors that stopped: the election timeout gives up on it.
        let live = self.leaders.values().any(|l| {
            l.segment() == newest
                && *l.phase() != LeaderPhase::Stopped
                && !(*l.phase() == LeaderPhase::Phase1
                    && l.segments().iter().any(|read| {
                        self.register
                            .segment(read.id)
                            .is_some_and(|r| r.state() >= SegmentState::Retired)
                    }))
        });
        if live && (cx.phase() != Phase::Chaos || !cx.rng().chance(1, 8)) {
            return;
        }
        let proposer = NodeId(*cx.rng().pick(&PROPOSERS).expect("proposers"));
        self.round += 1;
        let ballot = paros_journal::register::Ballot {
            round: self.round,
            node: proposer,
        };
        let mut value = self.register.clone();
        let mut segment = newest;
        if self.mutant == RolesMutant::StaleBinding
            && let Some(stale) = self.unseal(newest)
        {
            value = stale;
            segment = SegmentId(newest.0 - 1);
        }
        let from = self.applied_from(&value, segment);
        self.bound.insert((ballot.round, ballot.node), segment);
        let Some((leader, effects)) = JournalLeader::campaign(
            Campaign {
                ballot,
                segment,
                from,
            },
            &value,
        ) else {
            return;
        };
        cx.record(RolesEvent::Register {
            b: ballot.round,
            from: from.0,
        });
        // A proposer runs one leader: its earlier one stops, and answers
        // its callers `Unknown`.
        if let Some(mut old) = self.leaders.insert(proposer, leader) {
            let stopped = old.stop();
            self.leader_effects(proposer, stopped, cx);
        }
        self.leader_effects(proposer, effects, cx);
    }

    /// The mutant `StaleBinding`: the register as it was before the seal
    /// of `newest`'s predecessor, when that one is sealed.
    fn unseal(&self, newest: SegmentId) -> Option<RegisterValue> {
        let before = self.register.segment(SegmentId(newest.0.checked_sub(1)?))?;
        if before.state() != SegmentState::Sealed {
            return None;
        }
        let mut segments: BTreeMap<SegmentId, SegmentRecord> = self
            .register
            .segments()
            .iter()
            .filter(|(id, _)| **id < before.id())
            .map(|(id, r)| (*id, r.clone()))
            .collect();
        let open = SegmentRecord::from_parts(
            before.id(),
            before.release(),
            before.opening().clone(),
            before.replicas().to_vec(),
            SegmentState::InForce,
            before.start(),
            None,
            None,
        )
        .ok()?;
        segments.insert(before.id(), open);
        Some(RegisterValue::from_parts(self.register.term(), segments))
    }

    /// The first slot a leader of `segment` reads: the applied prefix of the
    /// oldest segment that is not retired, on the disks of its replicas.
    fn applied_from(&self, value: &RegisterValue, segment: SegmentId) -> Slot {
        let oldest = value
            .segments()
            .values()
            .find(|r| r.state() < SegmentState::Retired && r.id() <= segment)
            .expect("a segment is read");
        let start = oldest.start().unwrap_or(Slot(0));
        oldest
            .replicas()
            .iter()
            .filter_map(|node| self.disk_front(*node))
            .chain(std::iter::once(start))
            .max()
            .expect("a start")
    }

    /// The front of replica `node`'s fold on its disk: the slots it holds
    /// in order from the start of its segment. The replicas of the first
    /// segment start at slot 0 with the fresh journal; another replica has
    /// a front once its base is durable.
    fn disk_front(&self, node: NodeId) -> Option<Slot> {
        let disk = &self.replicas[&node].disk;
        let start = match disk.base {
            Some((_, start)) => start,
            None if Self::segment_of_replica(node) == SegmentId(1) => Slot(0),
            None => return None,
        };
        let mut front = start;
        while disk.chosen.contains_key(&front) {
            front = Slot(front.0 + 1);
        }
        Some(front)
    }

    /// Whether acceptor `node` is in the grid of a segment that is not
    /// retired.
    fn active(&self, node: NodeId) -> bool {
        self.register.segments().values().any(|record| {
            record.state() < SegmentState::Retired
                && record.grid().columns().iter().flatten().any(|n| *n == node)
        })
    }

    /// The event of an acceptor's queued batch: a vote, or a promise alone.
    fn queued(node: NodeId, ops: &[paros_journal::roles::AcceptorOp]) -> RolesEvent {
        let votes: Vec<_> = ops
            .iter()
            .filter_map(|op| match op {
                AcceptorWrite::AppendAccepted { slot, ballot, .. } => Some((*slot, *ballot)),
                AcceptorWrite::SetPromise(_) => None,
            })
            .collect();
        // The model's `Accept` queues one vote: a batch with two would trace
        // only one of them.
        assert!(votes.len() <= 1, "a batch holds one vote at most: {ops:?}");
        let vote = votes.first().copied();
        match (vote, ops) {
            (Some((slot, ballot)), _) => RolesEvent::Accept {
                a: node.0,
                b: ballot.round,
                s: slot.0,
            },
            (None, [AcceptorWrite::SetPromise(ballot)]) => RolesEvent::Promise {
                a: node.0,
                b: ballot.round,
            },
            (None, _) => unreachable!("a batch is a vote or one promise: {ops:?}"),
        }
    }

    fn leader_effects(
        &mut self,
        node: NodeId,
        effects: Vec<LeaderEffect<u64>>,
        cx: &mut Cx<'_, Self>,
    ) {
        for effect in effects {
            match effect {
                LeaderEffect::Send { to, message } => {
                    if let Message::Commit { slot, command, .. } = &message {
                        let ballot = self.leaders[&node].ballot();
                        self.commit_seen(*slot, command, (ballot.round, ballot.node));
                    }
                    if let Message::Accept {
                        ballot,
                        slot,
                        command,
                    } = &message
                    {
                        match self.proposed.entry((ballot.round, *slot)) {
                            Entry::Occupied(first) => assert_eq!(
                                first.get(),
                                command,
                                "ballot {} proposed two commands at slot {}",
                                ballot.round,
                                slot.0
                            ),
                            Entry::Vacant(entry) => {
                                entry.insert(command.clone());
                                cx.record(RolesEvent::Propose {
                                    b: ballot.round,
                                    s: slot.0,
                                    command: command.clone(),
                                });
                            }
                        }
                    }
                    cx.send(Envelope {
                        from: node,
                        to,
                        message,
                    });
                }
                LeaderEffect::Register(change) => {
                    let round = self.leaders[&node].ballot().round;
                    self.queue.push((Origin::Leader(node, round), change));
                }
                LeaderEffect::Reply { token, reply } => self.reply(token, reply),
            }
        }
    }

    fn acceptor_effects(
        &mut self,
        node: NodeId,
        effects: Vec<AcceptorEffect>,
        cx: &mut Cx<'_, Self>,
    ) {
        let acceptor = self.acceptors.get_mut(&node).expect("an acceptor");
        for effect in effects {
            match effect {
                AcceptorEffect::Persist { id, ops } => acceptor.batches.push_back((id, ops)),
                AcceptorEffect::Send { to, message, after } => {
                    let envelope = Envelope {
                        from: node,
                        to,
                        message,
                    };
                    match after {
                        Some(id) if acceptor.disk.durable.is_none_or(|d| d < id) => {
                            acceptor.held.push((id, envelope));
                        }
                        _ => cx.send(envelope),
                    }
                }
            }
        }
    }

    fn replica_effects(
        &mut self,
        node: NodeId,
        effects: Vec<ReplicaEffect>,
        cx: &mut Cx<'_, Self>,
    ) {
        for effect in effects {
            match effect {
                ReplicaEffect::Persist { id, ops } => {
                    self.replicas
                        .get_mut(&node)
                        .expect("a replica")
                        .batches
                        .push_back((id, ops));
                }
                ReplicaEffect::Send {
                    to,
                    mut message,
                    after,
                } => {
                    if let Message::Applied { slot, answer, .. } = &mut message {
                        if self.mutant == RolesMutant::DuplicateAsTaken
                            && let Answer::Duplicate { .. } = answer
                        {
                            let view = self.replicas[&node]
                                .role
                                .as_ref()
                                .and_then(|r| r.state())
                                .expect("a replica that answers folds")
                                .view();
                            *answer = Answer::Taken(view);
                        }
                        self.judge(*slot, answer);
                    }
                    let envelope = Envelope {
                        from: node,
                        to,
                        message,
                    };
                    let replica = self.replicas.get_mut(&node).expect("a replica");
                    match after {
                        Some(id) if replica.disk.durable.is_none_or(|d| d < id) => {
                            replica.held.push((id, envelope));
                        }
                        _ => cx.send(envelope),
                    }
                }
                ReplicaEffect::Register { change, after } => {
                    let replica = self.replicas.get_mut(&node).expect("a replica");
                    match after {
                        Some(id)
                            if replica.disk.durable.is_none_or(|d| d < id)
                                && self.mutant != RolesMutant::BoundaryBeforeDurable =>
                        {
                            replica.changes.push((id, change));
                        }
                        _ => self.queue.push((Origin::Replica(node), change)),
                    }
                }
            }
        }
    }

    /// A leader saw `slot` chosen: the oracles [`AGREE`] and [`SEALED`],
    /// and the truth over the chosen prefix.
    fn commit_seen(&mut self, slot: Slot, command: &Command, ballot: (u64, NodeId)) {
        if let Some((held, ..)) = self.chosen.get(&slot) {
            assert!(
                held == command,
                "{AGREE}: slot {slot:?} chose {held:?} and {command:?}"
            );
            return;
        }
        let segment = self.bound[&ballot];
        if let Some(end) = self.register.segment(segment).and_then(SegmentRecord::end) {
            assert!(
                slot < end,
                "{SEALED}: slot {slot:?} chosen at {ballot:?} in {segment:?}, sealed at {end:?}"
            );
        }
        if let Some(start) = self
            .register
            .segment(segment)
            .and_then(SegmentRecord::start)
            && slot < start
        {
            sometimes!(*command == Command::Noop, reach::GAP_NOOP);
            sometimes!(*command != Command::Noop, reach::REPROPOSED);
        }
        let columns = self
            .register
            .segment(segment)
            .map_or(1, |r| r.grid().columns().len());
        sometimes!(columns == 2, reach::COLUMNS);
        self.chosen
            .insert(slot, (command.clone(), ballot.0, ballot.1));
        while let Some((command, ..)) = self.chosen.get(&Slot(
            u64::try_from(self.verdicts.len()).expect("few slots"),
        )) {
            let command = command.clone();
            let verdict = self.truth.apply(&command);
            self.verdicts.push(verdict);
            self.states
                .push((self.truth.leader, self.truth.next, self.truth.first));
        }
    }

    /// The oracle [`FOLD`] on a replica's answer at `slot`.
    fn judge(&self, slot: Slot, answer: &Answer) {
        let at = usize::try_from(slot.0).expect("a small slot");
        let verdict = self.verdicts.get(at).copied();
        let name = label(answer);
        assert!(
            Some(name) == verdict || (name == "below" && matches!(verdict, Some("dup" | "taken"))),
            "{FOLD}: slot {slot:?} answered {answer:?}, the whole log says {verdict:?}"
        );
        let segment = self.segment_holding(slot);
        sometimes!(
            name == "ok" && segment.is_some_and(|s| s.0 >= 3),
            reach::THIRD
        );
    }

    fn segment_holding(&self, slot: Slot) -> Option<SegmentId> {
        self.register
            .segments()
            .values()
            .rev()
            .find(|r| r.start().is_some_and(|s| s <= slot))
            .map(SegmentRecord::id)
    }

    /// One queued register change runs.
    fn register_step(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(index) = cx.rng().index(self.queue.len()) else {
            return;
        };
        let (origin, mut change) = self.queue.remove(index);
        if let (RolesMutant::StaleBoundary, Change::Boundary { segment, state }) =
            (self.mutant, &mut change)
            && let Some(base) = Self::set_of(*segment)
                .iter()
                .find_map(|node| self.replicas[node].disk.base)
        {
            *state = base.0;
        }
        match self.register.apply(&change) {
            Ok(value) => {
                match (&change, origin) {
                    (Change::Boundary { segment, state }, _) => {
                        let end = value.segment(*segment).and_then(SegmentRecord::end);
                        let at = end.map(|e| usize::try_from(e.0).expect("small"));
                        let truth = at.and_then(|at| self.states.get(at));
                        assert!(
                            truth == Some(&(state.leader, state.next_seq.0, state.first_seq.0)),
                            "{BOUNDARY}: {segment:?} wrote {state:?}, the whole log at {end:?} is {truth:?}"
                        );
                        sometimes!(true, reach::BOUNDARY);
                        if let Origin::Replica(node) = origin {
                            cx.record(RolesEvent::Boundary { r: node.0 });
                        }
                    }
                    (Change::Seal { segment, end }, _) => {
                        for (slot, (_, round, node)) in &self.chosen {
                            assert!(
                                *slot < *end || self.bound[&(*round, *node)] != *segment,
                                "{SEALED}: {segment:?} sealed at {end:?} chose slot {slot:?}"
                            );
                        }
                        sometimes!(true, reach::SEALED);
                        if let Origin::Leader(_, b) = origin {
                            cx.record(RolesEvent::Seal { b });
                        }
                    }
                    _ => {}
                }
                self.register = value;
                self.observe_all(cx);
            }
            Err(_) => {
                if let Origin::Leader(node, round) = origin
                    && let Some(leader) = self.leaders.get_mut(&node)
                    && leader.ballot().round == round
                {
                    let sealing = matches!(leader.phase(), LeaderPhase::Sealing { .. });
                    let effects = leader.refused();
                    sometimes!(sealing, reach::SEAL_LOST);
                    self.leader_effects(node, effects, cx);
                }
            }
        }
    }

    fn observe_all(&mut self, cx: &mut Cx<'_, Self>) {
        let leaders: Vec<NodeId> = self.leaders.keys().copied().collect();
        for node in leaders {
            let value = self.register.clone();
            let leader = self.leaders.get_mut(&node).expect("a leader");
            let sealing = matches!(leader.phase(), LeaderPhase::Sealing { .. });
            let effects = leader.observe(&value);
            sometimes!(
                sealing && *leader.phase() == LeaderPhase::Stopped,
                reach::SEAL_LOST
            );
            if let (true, LeaderPhase::Open { start }) = (sealing, leader.phase()) {
                cx.record(RolesEvent::Open {
                    b: leader.ballot().round,
                    start: start.0,
                });
            }
            self.leader_effects(node, effects, cx);
        }
        let replicas: Vec<NodeId> = self.replicas.keys().copied().collect();
        for node in replicas {
            self.observe_replica(node, cx);
        }
    }

    fn observe_replica(&mut self, node: NodeId, cx: &mut Cx<'_, Self>) {
        let value = if self.mutant == RolesMutant::StaleBase {
            self.stale_base(Self::segment_of_replica(node))
        } else {
            self.register.clone()
        };
        let Some(role) = self.replicas.get_mut(&node).and_then(|r| r.role.as_mut()) else {
            return;
        };
        let effects = role.observe(&value);
        self.replica_effects(node, effects, cx);
    }

    /// The mutant `StaleBase`: the register with the boundary state of the
    /// segment before `segment` replaced by the one two before.
    fn stale_base(&self, segment: SegmentId) -> RegisterValue {
        let Some(before) = segment
            .0
            .checked_sub(1)
            .and_then(|n| self.register.segment(SegmentId(n)))
        else {
            return self.register.clone();
        };
        let Some(boundary) = before.boundary() else {
            return self.register.clone();
        };
        let stale = before
            .id()
            .0
            .checked_sub(1)
            .and_then(|n| self.register.segment(SegmentId(n)))
            .and_then(SegmentRecord::boundary)
            .unwrap_or(BoundaryState::fresh(self.initial));
        if stale == boundary {
            return self.register.clone();
        }
        let mut segments = self.register.segments().clone();
        let record = SegmentRecord::from_parts(
            before.id(),
            before.release(),
            before.opening().clone(),
            before.replicas().to_vec(),
            before.state(),
            before.start(),
            before.end(),
            Some(stale),
        )
        .expect("the same record with an older boundary");
        segments.insert(before.id(), record);
        RegisterValue::from_parts(self.register.term(), segments)
    }

    /// A batch of a role becomes durable, and the sends and the register
    /// changes that wait on it go.
    fn flush(&mut self, cx: &mut Cx<'_, Self>) {
        let nodes: Vec<NodeId> = self
            .acceptors
            .iter()
            .filter(|(_, a)| !a.batches.is_empty())
            .map(|(n, _)| *n)
            .chain(
                self.replicas
                    .iter()
                    .filter(|(_, r)| !r.batches.is_empty())
                    .map(|(n, _)| *n),
            )
            .collect();
        let picked: Vec<NodeId> = if cx.phase() == Phase::Settle {
            nodes
        } else {
            cx.rng().pick(&nodes).copied().into_iter().collect()
        };
        for node in picked {
            let queued = self
                .acceptors
                .get(&node)
                .map_or_else(|| self.replicas[&node].batches.len(), |a| a.batches.len());
            let count = if cx.phase() == Phase::Settle {
                queued
            } else {
                1 + usize::try_from(cx.rng().below(u64::try_from(queued).expect("few batches")))
                    .expect("few batches")
            };
            for _ in 0..count {
                if self.acceptors.contains_key(&node) {
                    self.flush_acceptor(node, cx);
                } else {
                    self.flush_replica(node, cx);
                }
            }
        }
    }

    fn flush_acceptor(&mut self, node: NodeId, cx: &mut Cx<'_, Self>) {
        let acceptor = self.acceptors.get_mut(&node).expect("an acceptor");
        let (id, ops) = acceptor.batches.pop_front().expect("a batch");
        acceptor.disk.apply(id, &ops);
        cx.record(RolesEvent::Flush { a: node.0 });
        let (go, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut acceptor.held)
            .into_iter()
            .partition(|(held, _)| *held <= id);
        acceptor.held = keep;
        for (_, envelope) in go {
            cx.send(envelope);
        }
    }

    fn flush_replica(&mut self, node: NodeId, cx: &mut Cx<'_, Self>) {
        let replica = self.replicas.get_mut(&node).expect("a replica");
        let (id, ops) = replica.batches.pop_front().expect("a batch");
        replica.disk.apply(id, &ops);
        let segment = Self::segment_of_replica(node);
        if segment.0 >= 2 && ops.iter().any(|op| matches!(op, ReplicaOp::Base { .. })) {
            cx.record(RolesEvent::Base { r: node.0 });
        }
        // Each slot the fold on the disk passed: one `Apply`, with the
        // answer of the fold.
        if let Some(front) = self.disk_front(node) {
            let role = self.replicas[&node]
                .role
                .as_ref()
                .expect("a replica with batches runs");
            let start = role.start().unwrap_or(Slot(0));
            let from = self.traced.get(&node).copied().unwrap_or(start);
            for slot in from.0..front.0 {
                let answer = role
                    .answer(Slot(slot))
                    .expect("the fold in memory holds every slot of the disk");
                cx.record(RolesEvent::Apply {
                    r: node.0,
                    s: slot,
                    command: role.chosen()[&Slot(slot)].clone(),
                    res: label(answer),
                });
            }
            self.traced.insert(node, front.max(from));
        }
        let replica = self.replicas.get_mut(&node).expect("a replica");
        let (go, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut replica.held)
            .into_iter()
            .partition(|(held, _)| *held <= id);
        replica.held = keep;
        let (changes, wait): (Vec<_>, Vec<_>) = std::mem::take(&mut replica.changes)
            .into_iter()
            .partition(|(held, _)| *held <= id);
        replica.changes = wait;
        for (_, envelope) in go {
            cx.send(envelope);
        }
        for (_, change) in changes {
            self.queue.push((Origin::Replica(node), change));
        }
    }

    fn tick(&mut self, cx: &mut Cx<'_, Self>) {
        let all = cx.phase() == Phase::Settle;
        let leaders: Vec<NodeId> = self.leaders.keys().copied().collect();
        for node in leaders {
            if all || cx.rng().chance(2, 3) {
                let effects = self
                    .leaders
                    .get_mut(&node)
                    .expect("a leader")
                    .tick(RESEND_PAGE);
                self.leader_effects(node, effects, cx);
            }
        }
        let replicas: Vec<NodeId> = self.replicas.keys().copied().collect();
        for node in replicas {
            if !(all || cx.rng().chance(1, 3)) {
                continue;
            }
            let Some(role) = self.replicas[&node].role.as_ref() else {
                continue;
            };
            let effects = role.tick();
            self.replica_effects(node, effects, cx);
        }
    }

    /// The open leader with the highest ballot.
    fn writer_leader(&self) -> Option<NodeId> {
        self.leaders
            .iter()
            .filter(|(_, l)| matches!(l.phase(), LeaderPhase::Open { .. }))
            .max_by_key(|(_, l)| l.ballot())
            .map(|(n, _)| *n)
    }

    fn propose(&mut self, writer: Option<usize>, command: Command, cx: &mut Cx<'_, Self>) {
        // A client has one call in flight: a writer waits for its answer,
        // and the unfenced calls of the walk are at most two.
        let busy = match writer {
            Some(index) => self.calls.values().any(|call| call.writer == Some(index)),
            None => {
                self.calls
                    .values()
                    .filter(|call| call.writer.is_none())
                    .count()
                    >= 2
            }
        };
        if busy {
            return;
        }
        let Some(node) = self.writer_leader() else {
            return;
        };
        self.tokens += 1;
        let token = self.tokens;
        let leader = self.leaders.get_mut(&node).expect("a leader");
        let ballot = leader.ballot();
        let (proposed, effects) = leader.propose(token, command.clone());
        if let Proposed::Slot(slot) = proposed {
            self.calls.insert(
                token,
                Call {
                    writer,
                    ballot,
                    slot,
                    command,
                },
            );
        }
        self.leader_effects(node, effects, cx);
    }

    fn records(cx: &mut Cx<'_, Self>) -> Vec<Record> {
        let id = u8::try_from(1 + cx.rng().below(3)).expect("small");
        vec![Record::new(vec![id])]
    }

    fn claim(&mut self, cx: &mut Cx<'_, Self>) {
        // A writer that leads is fenced on one claim in four: a walk that
        // fences every writer soon writes nothing.
        if self.writers.iter().any(|w| w.uuid.is_some()) && !cx.rng().chance(1, 4) {
            return;
        }
        let index = cx.rng().index(WRITERS).expect("writers");
        self.uuids += 1;
        let old = if cx.rng().chance(3, 4) {
            self.seen
        } else {
            Leader::Unset
        };
        let command = Command::SetLeader {
            new: Some(LeaderUuid(self.uuids)),
            old,
        };
        self.writers[index].claimed = Some(LeaderUuid(self.uuids));
        self.propose(Some(index), command, cx);
    }

    fn write(&mut self, cx: &mut Cx<'_, Self>, stale: bool) {
        // A writer that believes it leads writes; one that does not would
        // only be fenced.
        let leading: Vec<usize> = (0..WRITERS)
            .filter(|i| self.writers[*i].uuid.is_some())
            .collect();
        let Some(&index) = cx.rng().pick(&leading) else {
            return;
        };
        let uuid = self.writers[index].uuid.expect("a leading writer");
        let next = self.writers[index].next;
        let at = if stale {
            if next == 0 {
                return;
            }
            cx.rng().below(next)
        } else {
            next
        };
        let records = if self.shape.wide && cx.rng().chance(1, 3) {
            (0..3).map(|i| Record::new(vec![i])).collect()
        } else {
            Self::records(cx)
        };
        let write = Write {
            fence: Fence::Leader {
                uuid,
                expected_seq: Seq(at),
            },
            records,
        };
        self.propose(Some(index), Command::Write(write), cx);
    }

    /// A client sends again a call whose fate is unknown, or a write it
    /// knows was written.
    fn resend(&mut self, cx: &mut Cx<'_, Self>) {
        if let Some(pick) = cx.rng().index(self.unknown.len())
            && cx.rng().chance(2, 3)
        {
            let (writer, command) = self.unknown.remove(pick);
            self.propose(writer, command, cx);
            return;
        }
        let index = cx.rng().index(WRITERS).expect("writers");
        let Some(write) = cx.rng().pick(&self.writers[index].sent).cloned() else {
            return;
        };
        self.propose(Some(index), Command::Write(write), cx);
    }

    fn multi(&mut self, cx: &mut Cx<'_, Self>) {
        let command = if cx.rng().chance(1, 2) {
            Command::SetLeader {
                new: None,
                old: self.seen,
            }
        } else {
            Command::Write(Write {
                fence: Fence::Open,
                records: Self::records(cx),
            })
        };
        self.propose(None, command, cx);
    }

    fn truncate(&mut self, cx: &mut Cx<'_, Self>) {
        let fence = cx
            .rng()
            .index(WRITERS)
            .and_then(|i| self.writers[i].uuid)
            .filter(|_| cx.rng().chance(1, 2));
        let up_to = Seq(cx.rng().below(self.truth.next + 2));
        self.propose(None, Command::Truncate { fence, up_to }, cx);
    }

    /// A reply to the caller of `token`.
    fn reply(&mut self, token: u64, reply: Reply) {
        let Some(call) = self.calls.remove(&token) else {
            return;
        };
        let answer = match reply {
            Reply::Unknown => {
                sometimes!(true, reach::UNKNOWN);
                self.unknown.push((call.writer, call.command));
                if self.unknown.len() > 8 {
                    self.unknown.remove(0);
                }
                return;
            }
            Reply::Answer(answer) => answer,
        };
        self.judge(call.slot, &answer);
        if let Some(view) = view_of(&answer) {
            self.learn(view);
        }
        match (&answer, &call.command) {
            (Answer::LeaderSet(view), Command::SetLeader { .. }) => {
                sometimes!(view.leader == Leader::Multi, reach::TO_MULTI);
            }
            (Answer::Trimmed(view), _) => sometimes!(view.first_seq.0 > 0, reach::TRIMMED),
            (Answer::Written { seq, count }, Command::Write(write)) => {
                if let Some(index) = call.writer {
                    let writer = &mut self.writers[index];
                    writer.next = writer.next.max(seq.0 + count);
                    writer.sent.push(write.clone());
                    if writer.sent.len() > 4 {
                        writer.sent.remove(0);
                    }
                }
            }
            (Answer::Duplicate { .. }, _) => sometimes!(true, reach::DUPLICATE),
            (Answer::Below(_), Command::Write(write)) => {
                sometimes!(true, reach::BELOW);
                self.belows.push((write.clone(), call.slot));
            }
            (Answer::Taken(view) | Answer::Early(view) | Answer::Truncated(view), _) => {
                if let Some(index) = call.writer {
                    let writer = &mut self.writers[index];
                    writer.next = writer.next.max(view.next_seq.0);
                }
            }
            _ => {}
        }
    }

    /// A client learns from a view: the leader it names, and whether a
    /// writer still leads. A late view may be older than the last one;
    /// the walk only loses a write by it, never a check.
    fn learn(&mut self, view: &JournalView) {
        self.seen = view.leader;
        for writer in &mut self.writers {
            if writer
                .uuid
                .is_some_and(|uuid| view.leader != Leader::Uuid(uuid))
            {
                writer.uuid = None;
            }
            if writer.uuid.is_some() {
                writer.next = writer.next.max(view.next_seq.0);
            }
            if let Some(claimed) = writer.claimed
                && view.leader == Leader::Uuid(claimed)
                && writer.uuid.is_none()
            {
                writer.uuid = Some(claimed);
                writer.next = view.next_seq.0;
            }
        }
    }

    /// A client resolves a `Below` at a replica of an older segment.
    fn resolve(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(pick) = cx.rng().index(self.belows.len()) else {
            return;
        };
        let (write, slot) = self.belows[pick].clone();
        let Fence::Leader { uuid, expected_seq } = write.fence else {
            unreachable!("only a fenced write is answered Below");
        };
        let in_force = self.register.in_force().map(SegmentRecord::id);
        let mut answer = None;
        let mut by = None;
        for (node, replica) in &self.replicas {
            let segment = Self::segment_of_replica(*node);
            let asked = if self.mutant == RolesMutant::ResolveInForce {
                Some(segment) == in_force
            } else {
                Some(segment) != in_force
            };
            let Some(role) = replica.role.as_ref().filter(|_| asked) else {
                continue;
            };
            // The answer goes once the batch it waits on is durable: a
            // replica with a batch in flight answers later.
            let (found, after) = role.resolve(uuid, expected_seq, &write.records);
            if after.is_some_and(|id| replica.disk.durable.is_none_or(|d| d < id)) {
                continue;
            }
            if found != ResolveAnswer::NotCovered || self.mutant == RolesMutant::ResolveInForce {
                answer = Some(found);
                by = Some(*node);
                break;
            }
        }
        let Some(mut answer) = answer else {
            return;
        };
        if self.mutant == RolesMutant::ResolveInForce && answer == ResolveAnswer::NotCovered {
            answer = ResolveAnswer::NotWritten;
        }
        let verdict = self.verdicts[usize::try_from(slot.0).expect("small")];
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
            reach::RESOLVED
        );
        let res = match answer {
            ResolveAnswer::Written { .. } => "dup",
            ResolveAnswer::NotWritten => "taken",
            ResolveAnswer::Truncated => "trunc",
            ResolveAnswer::NotCovered => "notcovered",
        };
        cx.record(RolesEvent::Resolve {
            s: slot.0,
            r: by.expect("a replica answered").0,
            res,
        });
        self.belows.remove(pick);
    }

    /// The leader of the fresh journal.
    #[must_use]
    pub fn fresh(&self) -> Leader {
        self.initial
    }

    /// The nodes that were ever leaders, for a test.
    #[must_use]
    pub fn proposers() -> BTreeSet<NodeId> {
        PROPOSERS.iter().map(|p| NodeId(*p)).collect()
    }
}

/// The view an answer names, if any.
fn view_of(answer: &Answer) -> Option<&JournalView> {
    match answer {
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
        | Answer::Reserved(view) => Some(view),
        Answer::Written { .. } | Answer::Duplicate { .. } | Answer::Noop => None,
    }
}

thread_local! {}
