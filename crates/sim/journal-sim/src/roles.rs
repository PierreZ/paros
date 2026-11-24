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
    AcceptorEffect, AcceptorOp, AcceptorWrite, Campaign, JournalAcceptor, JournalLeader,
    JournalProxy, LeaderEffect, LeaderPhase, Message, PROMISE_BATCH, PersistId, Proposed,
    ProxyEffect, ReadAnswer, ReplicaEffect, ReplicaOp, SegmentReplica,
};
use paros_journal::segment::{OpeningCall, SegmentId, SegmentRecord, SegmentState};
use paros_journal::{
    Answer, BoundaryState, Command, Fence, Grid, JournalView, Leader, LeaderUuid, NodeId, RELEASE,
    Record, Reply, Seq, Slot, Write,
};

use crate::oracle::{Truth, Verdict, as_written, label};

/// Oracle: a read serves the records of the whole log, and a page of a
/// segment that no seal ended covers every slot chosen before the read
/// started (`ServedChosen`, `ReadFresh` of
/// `specs/tla/experiments/segments/reads/`).
pub const READ: &str = "a read serves the whole log's records, and an open segment's page covers what was chosen before";
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
/// Oracle: a leader retires a segment only once `F + 1` replicas of its
/// set hold every slot of it on their disk (`retire_f`).
pub const RETIRE: &str = "a segment retires only with F + 1 replicas of its set holding it on disk";

/// Oracle: a segment is trimmed only once it is sealed and the decided
/// floor of the whole log is at or above its last `seq` (§J3.2,
/// `trim_intent`, `trim_open` of `specs/tla/segment-truncate/`).
pub const TRIM: &str = "a segment is trimmed only once the decided floor covers its last seq";
/// Oracle: a read serves no record below the floor that the whole log had
/// when the read started (§J3.2, `holder` of `specs/tla/segment-truncate/`).
pub const FLOOR: &str = "a read serves no record below the floor of the whole log";

/// Oracle: every record of a chosen write carries one leader time, the
/// time of a proposal of that write (§J2.8): the leader stamps it at
/// propose, in the chosen value, and no replica stamps at apply.
pub const STAMPED: &str =
    "every record of a chosen write carries the leader time of a proposal of it";

/// Oracle: a proxy leader commits a slot only once `F + 1` acceptors of the
/// column of the round hold a vote at the round's ballot or above on their
/// disk (`specs/tla/proxy-leader/`).
pub const PROXY: &str =
    "a proxy commits a slot only once F + 1 acceptors of its column voted it on disk";

/// An acceptor drops the votes of a segment's slots only below a point
/// that `F + 1` replicas of the segment's set compacted to on their disk,
/// and every slot it drops is chosen (§J3.2, `AccCompact` and `DropChosen`
/// of `specs/tla/journal-compact/`).
pub const COMPACT: &str =
    "an acceptor drops a vote only below the compaction point of F + 1 replicas of its set";

/// Oracle: a CTRL repair writes the command that the slot chose (§J5.3,
/// `AccRepair` of `specs/tla/journal-ctrl/`): the repaired vote is the
/// chosen command, at or above the ballot of the leader that chose it.
pub const REPAIRED: &str = "a repaired vote holds the chosen command at or above its ballot";

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
    /// A `SetLeader` of a multi-writer mode applied.
    pub const TO_MULTI: &str = "roles walk: a SetLeader made the journal multi-writer";
    /// A Phase 1 took a promise in more than one page.
    pub const PAGED: &str = "roles walk: a promise came in more than one page";
    /// An acceptor restarted from its disk with a vote.
    pub const RESTARTED: &str = "roles walk: an acceptor restarted from its disk with a vote";
    /// A slot chosen by a proxy leader.
    pub const PROXIED: &str = "roles walk: a proxy leader chose a slot";
    /// A slot chosen by a proxy leader after a proxy crashed.
    pub const PROXY_CRASHED: &str = "roles walk: a slot chosen after a proxy leader crashed";
    /// A read of a segment that no seal ended served a page.
    pub const READ_OPEN: &str = "roles walk: a read of an open segment served a page";
    /// A read met an acceptor that knows a later segment.
    pub const READ_SUPERSEDED: &str = "roles walk: a read met a later segment";
    /// A leader refused a proposal with its window of answers full.
    pub const BUSY: &str = "roles walk: a full window refused a proposal";
    /// A read of a sealed segment served its last records.
    pub const READ_SEALED: &str = "roles walk: a read reached the end of a sealed segment";
    /// An answer to a read of an earlier incarnation of a replica reached
    /// the replica after its restart.
    pub const READ_STALE_ACK: &str = "roles walk: a stale read answer reached a restarted replica";
    /// A retired segment trimmed under the floor.
    pub const SEGMENT_TRIMMED: &str = "roles walk: a floor over a retired segment trimmed it";
    /// A read of an older segment refused below the floor of the segment
    /// in force.
    pub const FLOOR_REFUSED: &str = "roles walk: a read met the floor of the segment in force";
    /// A slot chosen in a grid of two columns (wide only).
    pub const COLUMNS: &str = "roles walk: a slot chosen in a grid of two columns";
    /// A replica dropped its log below its compaction point (§J3.2).
    pub const COMPACTED: &str = "roles walk: a replica compacted its log";
    /// An acceptor dropped the votes of a segment that `F + 1` replicas
    /// compacted.
    pub const DROPPED: &str = "roles walk: an acceptor dropped votes";
    /// A laggard jumped to a peer's compaction point.
    pub const JUMPED: &str = "roles walk: a laggard jumped to a peer's trim";
    /// A fold that jumped applied a slot below its trim floor, with no
    /// answer.
    pub const BLIND: &str = "roles walk: a blind fold applied a slot";

    /// An acceptor booted with a damaged vote.
    pub const ROTTED: &str = "roles walk: an acceptor booted with a damaged vote";
    /// A leader's Phase 1 left a slot blocked by a faulty vote.
    pub const BLOCKED: &str = "roles walk: a faulty vote blocked a slot";
    /// A late answer decided a blocked slot.
    pub const PROBED: &str = "roles walk: a late answer decided a blocked slot";
    /// A late answer dropped a blocked slot, and the leader skipped it.
    pub const SKIPPED: &str = "roles walk: a late answer dropped a blocked slot";
    /// A replica gave a blocked slot its chosen command.
    pub const LEARNED: &str = "roles walk: a replica gave a blocked slot its command";
    /// An acceptor repaired its faulty vote.
    pub const REPAIRED: &str = "roles walk: an acceptor repaired its faulty vote";

    /// The sites of every roles walk.
    pub const ALL: &[&str] = &[
        SEALED,
        SEAL_LOST,
        GAP_NOOP,
        REPROPOSED,
        THIRD,
        BOUNDARY,
        RETIRED,
        PULLED,
        NACKED,
        UNKNOWN,
        BELOW,
        RESOLVED,
        DUPLICATE,
        TRIMMED,
        TO_MULTI,
        RESTARTED,
        PROXIED,
        PROXY_CRASHED,
        READ_OPEN,
        READ_SUPERSEDED,
        READ_SEALED,
        READ_STALE_ACK,
        BUSY,
        SEGMENT_TRIMMED,
        FLOOR_REFUSED,
        COMPACTED,
        DROPPED,
        JUMPED,
        BLIND,
        ROTTED,
        BLOCKED,
        PROBED,
        SKIPPED,
        LEARNED,
        REPAIRED,
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

/// The answers a leader holds at most: the walk's writers have five calls
/// in flight at most, so a window of four refuses some.
const WINDOW: usize = 5;

/// A bug planted in the driver of the roles walk, never in the roles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RolesMutant {
    /// No bug.
    #[default]
    None,
    /// An acceptor restarts with an empty disk. [`AGREE`] catches it.
    WipeDisk,
    /// The driver hands a leader, as the done value of its bind, the
    /// register before the seal of the newest segment's predecessor: the
    /// ballot is bound to the sealed segment as if it were still in force.
    /// [`SEALED`] catches it.
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
    /// The driver turns a replica's `Taken` into `Below`, also for a
    /// position inside the segment. [`FOLD`] catches it.
    BelowInSegment,
    /// The driver turns every page of a read into `Unserved`. [`READ`]
    /// catches it, once the faults stop.
    ReadUnserved,
    /// The driver drops the last record of each page of two records or
    /// more. [`READ`] catches it:
    /// a short page ends before the segment's records end.
    ShortPage,
    /// The trace control: the driver runs a replica's boundary write before
    /// the batch it waits on is durable. The write is the fold of chosen
    /// slots, so every oracle passes; `RolesTrace.tla` refuses it, as
    /// `BoundWrite` needs every slot of the segment on the replica's disk.
    BoundaryBeforeDurable,
    /// The driver turns a replica's "not full" answer to `Holding` into
    /// "full", so the leader counts a replica that lags. [`RETIRE`] catches
    /// it.
    HoldsLie,
    /// The driver gives a proxy leader each `Accepted` once from every
    /// acceptor of the pool, so the proxy counts votes nobody cast.
    /// [`PROXY`] catches it.
    ForgedAccepted,
    /// The driver gives a replica each `PreReadAck` with the fence of the
    /// reader's own segment, so a read never meets a later segment.
    /// [`READ`] catches it.
    ReadNoFence,
    /// The driver gives each acceptor an `Accept` whose write lost its
    /// leader time. [`STAMPED`] catches it.
    UnstampedAccept,
    /// The driver trims a sealed segment on the intent to truncate: at the
    /// proposal of a `Truncate`, before it is decided. [`TRIM`] catches it.
    TrimIntent,
    /// The reader of an older segment takes the floor of the holder, not
    /// of the segment in force. [`FLOOR`] catches it.
    FloorFromHolder,
    /// The driver gives a leader each `CompactedTo` once from every replica
    /// of its segment, so the leader drops votes on one report.
    /// [`COMPACT`] catches it.
    ForgedCompacted,
    /// The driver gives a leader each promise page without its dropped
    /// ranges, so Phase 1 takes a slot whose votes were dropped for an
    /// empty one. [`AGREE`] catches it.
    DroppedHidden,
    /// The driver gives each acceptor a `Repair` that carries a `Noop` in
    /// place of the chosen command. [`REPAIRED`] catches it.
    RepairForged,
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
        /// Its acceptors, column by column.
        grid: Vec<Vec<u64>>,
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
    /// Acceptor `a` sent again its promise at `b`, which its disk holds,
    /// with no write: the answer to a `Prepare` it already promised, sent
    /// once its last batch is durable (`Repromise`).
    Repromise {
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
    /// `res`, or `None` when its fold was blind (`Apply`).
    Apply {
        /// The replica.
        r: u64,
        /// The slot.
        s: u64,
        /// The command.
        command: Command,
        /// The answer, by its name in the model; `None` for a blind fold.
        res: Option<&'static str>,
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
    /// The register took the trim of segment `p` that replica `r` wrote
    /// (`Trim`).
    Trim {
        /// The segment.
        p: u64,
        /// The replica.
        r: u64,
    },
    /// Replica `r` dropped its copy of a trimmed segment (`Drop`).
    Drop {
        /// The replica.
        r: u64,
    },
    /// Acceptor `a` dropped the votes of segment `p` below `x` and queued
    /// the write (`AccCompact`).
    Compact {
        /// The acceptor.
        a: u64,
        /// The segment.
        p: u64,
        /// The slot below which the votes go.
        x: u64,
    },
    /// Replica `r` dropped its log below `slot`, durably (`RepCompact`).
    CompactReplica {
        /// The replica.
        r: u64,
        /// The compaction point.
        slot: u64,
    },
    /// Replica `r` jumped to the compaction point `slot` of a peer, whose
    /// trim floor is `floor`, durably (`JumpFrom`).
    Jump {
        /// The replica.
        r: u64,
        /// The compaction point.
        slot: u64,
        /// Its trim floor.
        floor: u64,
    },
    /// Acceptor `a` booted with its vote at `s` damaged: the value is lost,
    /// the ballot stays (`Rot`).
    Rot {
        /// The acceptor.
        a: u64,
        /// The slot.
        s: u64,
    },
    /// A late answer decided the blocked slot `s` of the leader of `b` on
    /// the qualifying answers `q` (`ProbeDecide`).
    Probe {
        /// The ballot.
        b: u64,
        /// The slot.
        s: u64,
        /// The qualifying answers.
        q: Vec<u64>,
    },
    /// The complete answer of acceptor `a` dropped the blocked slot `s` of
    /// the leader of `b`, which proposes nothing there (`ProbeSkip`).
    Skip {
        /// The ballot.
        b: u64,
        /// The slot.
        s: u64,
        /// The acceptor whose answer dropped the slot.
        a: u64,
    },
    /// A replica gave the blocked slot `s` of the leader of `b` its chosen
    /// command (`LearnChosen`).
    Learn {
        /// The ballot.
        b: u64,
        /// The slot.
        s: u64,
        /// The chosen command.
        command: Command,
    },
    /// Acceptor `a` queued the repair of its faulty vote at `s` with
    /// `command`, chosen at ballot `c` (`AccRepair`).
    Repair {
        /// The acceptor.
        a: u64,
        /// The slot.
        s: u64,
        /// The chosen command.
        command: Command,
        /// The ballot that chose it.
        c: u64,
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
    /// The register runs one queued change.
    Register,
    /// A role's batch becomes durable.
    Flush,
    /// A role's periodic step.
    Tick,
    /// A client resolves a `Below`.
    Resolve,
    /// A client reads at a replica.
    Read,
    /// A replica learns the register value it missed.
    Observe,
    /// An acceptor boots and finds one of its votes damaged.
    Rot,
}

/// One open read of the walk's reader.
#[derive(Clone, Copy, Debug)]
struct OpenRead {
    /// The replica asked.
    node: NodeId,
    /// The first position.
    from: u64,
    /// The most records.
    limit: usize,
    /// The length of the chosen prefix when the read started.
    need: usize,
    /// The floor of the whole log when the reader started.
    floor: u64,
    /// For a read of the floor: the replica of the older segment that the
    /// reader asks next, with the first position and the limit.
    then: Option<(NodeId, u64, usize)>,
    /// The read started after the faults stopped: the walk asks again
    /// until one such read is served.
    settled: bool,
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

type AcceptorNode = Durable<JournalAcceptor, AcceptorDisk, AcceptorOp>;
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

/// The steer of [`RolesModel::late`]: one column holds a faulty vote at a
/// slot. A rot arms it; once a silent member dropped the slot, the next
/// campaign takes it. Its promises to that `campaign` wait for the `silent` members, the dropper among
/// them, so the members that answer are the faulty one and a quorum less
/// one that kept the slot with no vote at the faulty ballot or above: the
/// tally cannot decide the slot and blocks it, and the dropper's late
/// answer drops it. The `kept` members, the faulty one and those that
/// answer, take no `Compact` meanwhile, so their answers still read the
/// slot.
#[derive(Clone, Debug)]
struct Late {
    campaign: Option<(NodeId, paros_journal::register::Ballot)>,
    faulty: NodeId,
    slot: Slot,
    silent: BTreeSet<NodeId>,
    kept: BTreeSet<NodeId>,
}

/// The acceptor pool.
const POOL: u64 = 6;

/// The rounds and the commits a leader re-sends on one tick: a page small
/// against the walk's mailbox.
const RESEND_PAGE: usize = 4;
/// The proposers.
const PROPOSERS: [u64; 3] = [101, 102, 103];
/// The proxy leaders: a campaign delegates to them on one seed in two.
const PROXIES: [u64; 3] = [201, 202, 203];
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
    /// The proxy leaders; `None` while one is down.
    proxies: BTreeMap<NodeId, Option<JournalProxy>>,
    /// Whether some proxy leader crashed.
    proxy_crashed: bool,
    /// The segment each ballot is bound to: the newest segment of the
    /// register when its bind was done.
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
    /// The `Below` answers that a `Resolve` settled: the client may ask
    /// `Resolve` again (it reads and writes nothing), and every covering
    /// answer is judged again.
    settled_belows: Vec<(Write, Slot)>,
    /// The writer whose last call was answered `Written`: a client that
    /// lost that answer resends the write at once, to the leader that
    /// applied it, and the fold answers `Duplicate`.
    echo: Option<usize>,
    /// The calls answered `Unknown`, which their client sends again.
    unknown: Vec<(Option<usize>, Command)>,
    /// The command each ballot proposed at each slot, by round. A ballot
    /// proposes one command at a slot: the trace records the first `Accept`
    /// of each `(round, slot)`, and the model's `Accept` votes that proposal.
    proposed: BTreeMap<(u64, Slot), Command>,
    /// The durable front each replica's `Apply` events reached.
    traced: BTreeMap<NodeId, Slot>,
    /// The highest vote that a compaction dropped from each acceptor's disk,
    /// or whose value a rot damaged, by acceptor and slot: the oracle
    /// [`PROXY`] counts it as on the disk (a damaged vote keeps its ballot,
    /// §J5.3).
    dropped_votes: BTreeMap<(NodeId, Slot), paros_journal::register::Ballot>,
    /// The `Repair`s the network holds: the rot move steers to a vote above
    /// one of them and then lets it go, so a late repair meets a faulty
    /// vote of a later ballot.
    late_repairs: Vec<Envelope>,
    /// The answer of each slot a replica applied in memory and that is not
    /// traced yet, taken after each step: a later compaction drops it from
    /// the replica. `None` for a slot its blind fold applied.
    applied: BTreeMap<NodeId, BTreeMap<Slot, Option<Answer>>>,
    /// The records of the whole log, by position.
    records: BTreeMap<u64, Record>,
    /// The open reads, by replica and the reader's id.
    reads: BTreeMap<(NodeId, u64), OpenRead>,
    /// The replicas that dropped their copy.
    dropped: BTreeSet<NodeId>,
    /// The command of each proposal, as its client wrote it, by its time:
    /// its call's token.
    stamps: BTreeMap<u64, Command>,
    /// The reads of an older segment whose floor read was not served, with
    /// the floor when the reader started: the reader asks the floor again
    /// later.
    waiting: Vec<(NodeId, u64, usize, u64)>,
    /// The read answers that wait on a batch of their replica.
    held_reads: BTreeMap<NodeId, Vec<(PersistId, u64, ReadAnswer)>>,
    /// The last read id of each replica's reader. A crash of the replica
    /// starts its reader again at 1, as a program that restarts with it:
    /// the ids of the reads repeat across the incarnations of a replica.
    read_ids: BTreeMap<NodeId, u64>,
    /// The boot of the reads of each replica's incarnation, from its
    /// `PreRead`s; a crash forgets it.
    read_boots: BTreeMap<NodeId, PersistId>,
    /// The replica that hears the register late, if any.
    slow: Option<NodeId>,
    /// The replica that loses every message until a peer compacts to a
    /// point whose fold is blind, or its segment is no longer in force, if
    /// any: it
    /// lags behind that point, so its next pull meets a `TrimmedTo` and its
    /// fold from there is blind.
    deaf: Option<NodeId>,
    /// The peers of the deaf replica that compacted to a blind point since
    /// it went deaf.
    blinded: BTreeSet<NodeId>,
    /// A read that started after the faults stopped was served a page.
    settled_served: bool,
    /// A campaign whose Phase 1 the walk steers to a block that a late
    /// answer drops (`SKIPPED`): the acceptors whose promises wait until
    /// its Phase 1 closed, and the acceptors whose `Compact`s wait.
    late: Option<Late>,
    /// The promises that wait for `late`.
    held_promises: Vec<Envelope>,
    /// The `Compact`s that wait for `late`.
    held_compacts: Vec<Envelope>,
    /// The promises sent again with no write that wait on a batch of their
    /// acceptor: the acceptor, the batch, the leader and the ballot. Each is
    /// a `Repromise` event once it goes.
    repromises: Vec<(NodeId, PersistId, NodeId, u64)>,
}

/// The walk of `shape` with `mutant` planted.
#[must_use]
pub fn walk(shape: Shape, mutant: RolesMutant, seeds: u64, steps: u64) -> Walk<RolesModel> {
    Walk::<RolesModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .env_prefix(None)
        .first_seed(crate::campaign::first_seed())
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
            RolesAction::Register,
            RolesAction::Flush,
            RolesAction::Tick,
            RolesAction::Resolve,
            RolesAction::Read,
            RolesAction::Observe,
            RolesAction::Rot,
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
                | RolesAction::Read
                | RolesAction::Write
                | RolesAction::Observe,
                Phase::Settle,
            ) => 1,
            (_, Phase::Settle) => 0,
            (RolesAction::Flush, _) => 30,
            (RolesAction::Register, _) => 10,
            (RolesAction::Tick, _) => 6,
            (RolesAction::Resolve | RolesAction::Read | RolesAction::Observe, _) => 4,
            (_, Phase::Quiet) => match action {
                RolesAction::Campaign => 1,
                _ => 0,
            },
            (RolesAction::Write, _) => 12,
            (RolesAction::Resend, _) => 5,
            (
                RolesAction::Claim
                | RolesAction::Reshape
                | RolesAction::Stale
                | RolesAction::Truncate,
                _,
            ) => 2,
            (RolesAction::Campaign | RolesAction::Multi | RolesAction::Rot, _) => 1,
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
            RolesAction::Register => self.register_step(cx),
            RolesAction::Flush => self.flush(cx),
            RolesAction::Tick => self.tick(cx),
            RolesAction::Resolve => self.resolve(cx),
            RolesAction::Read => self.read(cx),
            RolesAction::Rot => self.rot(cx),
            RolesAction::Observe => {
                let nodes: Vec<NodeId> = self
                    .replicas
                    .keys()
                    .copied()
                    .filter(|node| self.slow != Some(*node))
                    .collect();
                if let Some(pick) = cx.rng().index(nodes.len()) {
                    self.observe_replica(nodes[pick], cx);
                }
            }
        }
    }

    fn deliver(&mut self, envelope: Envelope, cx: &mut Cx<'_, Self>) {
        if self.hold_late(&envelope) {
            return;
        }
        let Envelope { from, to, message } = envelope;
        // A deaf replica hears again once its segment is no longer the
        // segment in force.
        if self.deaf.is_some_and(|deaf| {
            self.register.in_force().map(SegmentRecord::id) != Some(Self::segment_of_replica(deaf))
        }) {
            self.deaf = None;
        }
        if self.deaf == Some(to) && cx.phase() != Phase::Settle {
            return;
        }
        if self.acceptors.contains_key(&to) {
            // A retired grid's acceptors stop: only an acceptor of a grid
            // that is not retired takes a step (`Active` of the model).
            if !self.active(to) {
                return;
            }
            let mut message = message;
            if let Message::Accept {
                command: Command::Write(write),
                ..
            } = &mut message
                && self.mutant == RolesMutant::UnstampedAccept
            {
                for record in &mut write.records {
                    record.headers.leader_time = None;
                }
            }
            if let Message::Repair { command, .. } = &mut message
                && self.mutant == RolesMutant::RepairForged
            {
                *command = Command::Noop;
            }
            let repair = match &message {
                Message::Repair {
                    ballot,
                    slot,
                    command,
                } => Some((*slot, command.clone(), ballot.round)),
                _ => None,
            };
            let node = self.acceptors.get_mut(&to).expect("an acceptor");
            if let Some(role) = &mut node.role {
                let effects = role.step(from, message);
                for effect in &effects {
                    if let AcceptorEffect::Persist { ops, .. } = effect {
                        match &repair {
                            Some((slot, command, c)) => cx.record(RolesEvent::Repair {
                                a: to.0,
                                s: slot.0,
                                command: command.clone(),
                                c: *c,
                            }),
                            None => cx.record(Self::queued(to, ops)),
                        }
                    }
                }
                let wrote = effects
                    .iter()
                    .any(|e| matches!(e, AcceptorEffect::Persist { .. }));
                if let (Some((slot, ..)), true) = (&repair, wrote) {
                    sometimes!(true, reach::REPAIRED);
                    let vote = role.vote(*slot).cloned();
                    self.judge_repair(to, *slot, vote.as_ref());
                }
                self.acceptor_effects(to, effects, cx);
            }
        } else if self.replicas.contains_key(&to) {
            self.deliver_to_replica(from, to, message, cx);
        } else if let Some(Some(proxy)) = self.proxies.get_mut(&to) {
            let forged = match message {
                Message::Accepted { .. } if self.mutant == RolesMutant::ForgedAccepted => {
                    (1..=POOL).map(NodeId).collect()
                }
                _ => Vec::new(),
            };
            let mut effects = proxy.step(from, message.clone());
            for node in forged {
                effects.extend(proxy.step(node, message.clone()));
            }
            self.proxy_effects(to, effects, cx);
        } else if self.leaders.contains_key(&to) {
            self.deliver_to_leader(from, to, message, cx);
            self.release_late(false, cx);
        }
    }

    fn nodes(&self) -> Vec<NodeId> {
        self.acceptors
            .keys()
            .chain(self.replicas.keys())
            .copied()
            .chain(PROPOSERS.iter().map(|p| NodeId(*p)))
            .chain(PROXIES.iter().map(|p| NodeId(*p)))
            .collect()
    }

    fn is_up(&self, node: &NodeId) -> bool {
        if let Some(acceptor) = self.acceptors.get(node) {
            acceptor.role.is_some()
        } else if let Some(replica) = self.replicas.get(node) {
            replica.role.is_some()
        } else if let Some(proxy) = self.proxies.get(node) {
            proxy.is_some()
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
            self.applied.remove(node);
            // The reads of a replica that crashed get no answer: their
            // client times out.
            self.held_reads.remove(node);
            self.reads.retain(|(asked, _), _| asked != node);
            self.read_ids.remove(node);
            self.read_boots.remove(node);
        } else if let Some(proxy) = self.proxies.get_mut(node) {
            self.proxy_crashed |= proxy.as_ref().is_some_and(|p| p.open() > 0);
            *proxy = None;
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
        } else if let Some(proxy) = self.proxies.get_mut(node)
            && proxy.is_none()
        {
            *proxy = Some(JournalProxy::new(*node));
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
        // A read after the faults stop is served.
        if !self.settled_served {
            return Err(format!(
                "{READ}: no read after the faults stopped was served"
            ));
        }
        if !self.queue.is_empty() {
            return Err(format!("register changes are queued: {:?}", self.queue));
        }
        if self.acceptors.values().any(|a| !a.batches.is_empty())
            || self.replicas.values().any(|r| !r.batches.is_empty())
        {
            return Err("batches are in flight".into());
        }
        let newest = self.register.newest().expect("a segment");
        if !self.leaders.values().any(|leader| {
            leader.segment() == Some(newest.id())
                && matches!(leader.phase(), LeaderPhase::Open { .. })
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
        // Every call is answered: a deposed leader stops (a proxy tells it
        // too) and answers what it holds `Unknown`.
        if !self.calls.is_empty() {
            return Err(format!("calls are not answered: {:?}", self.calls.keys()));
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
    /// The acceptors that share a column of some grid with `node`: a
    /// slot's votes and drops are judged among them.
    fn column_peers(&self, node: NodeId) -> BTreeSet<NodeId> {
        self.register
            .segments()
            .values()
            .flat_map(|record| record.grid().columns().iter())
            .filter(|column| column.contains(&node))
            .flatten()
            .copied()
            .filter(|n| *n != node)
            .collect()
    }

    /// Whether `envelope` waits for `late`: a promise of a silent member
    /// to the steered campaign, or a `Compact` of a kept member.
    fn hold_late(&mut self, envelope: &Envelope) -> bool {
        let Some(late) = &self.late else {
            return false;
        };
        if late.kept.contains(&envelope.to) && matches!(envelope.message, Message::Compact { .. }) {
            self.held_compacts.push(envelope.clone());
            return true;
        }
        if late.silent.contains(&envelope.from)
            && late.campaign.is_some_and(|(leader, b)| {
                envelope.to == leader
                    && matches!(&envelope.message, Message::Promise { ballot, .. } if *ballot == b)
            })
        {
            self.held_promises.push(envelope.clone());
            return true;
        }
        false
    }

    /// Once the steered campaign's Phase 1 closed, or the campaign is
    /// gone, or the faults stopped (an armed steer with no campaign yet
    /// waits for one), the held messages arrive: the promises
    /// are late answers. `force` releases them in any case.
    fn release_late(&mut self, force: bool, cx: &mut Cx<'_, Self>) {
        let Some(late) = &self.late else {
            return;
        };
        // A leader that still binds has not asked yet: its Phase 1 is
        // ahead, and the silent members wait for it.
        let open = late.campaign.is_none_or(|(leader, ballot)| {
            self.leaders.get(&leader).is_some_and(|l| {
                l.ballot() == ballot
                    && matches!(l.phase(), LeaderPhase::Binding | LeaderPhase::Phase1)
            })
        });
        if open && !force && cx.phase() != Phase::Settle {
            return;
        }
        self.late = None;
        for envelope in std::mem::take(&mut self.held_promises) {
            if self.leaders.contains_key(&envelope.to) {
                self.deliver(envelope, cx);
            }
        }
        for envelope in std::mem::take(&mut self.held_compacts) {
            self.deliver(envelope, cx);
        }
    }

    /// The steers that [`Late`] can take now: for each column of a segment
    /// that is not retired with a faulty vote at a slot (that one member
    /// dropped, when `dropped`), the members that stay silent and the
    /// members that are kept.
    /// Each acceptor is judged as it answers now: its role's memory, which
    /// runs ahead of its disk while a batch is in flight.
    fn late_steers(&self, dropped: bool) -> Vec<Late> {
        let up = |n: &NodeId| self.acceptors.get(n).is_some_and(|a| a.role.is_some());
        let drops = |n: &NodeId, slot: Slot| {
            self.acceptors[n].role.as_ref().is_some_and(|role| {
                role.dropped()
                    .iter()
                    .any(|(start, below)| *start <= slot && slot < *below)
            })
        };
        let mut steers = Vec::new();
        for record in self.register.segments().values() {
            if record.state() >= SegmentState::Retired {
                continue;
            }
            for column in record.grid().columns() {
                for faulty in column.iter().filter(|n| up(n)) {
                    for (slot, ballot) in &self.acceptors[faulty].disk.faulty {
                        // The column of the segment that the faulty vote's
                        // ballot is bound to: the one a Phase 1 tallies.
                        if self.bound.get(&(ballot.round, ballot.node)) != Some(&record.id())
                            || drops(faulty, *slot)
                            || (dropped
                                && !column
                                    .iter()
                                    .any(|x| x != faulty && up(x) && drops(x, *slot)))
                        {
                            continue;
                        }
                        // The members that kept the slot below the faulty
                        // ballot; a quorum less one of them answers with
                        // the faulty member.
                        let keeping: Vec<NodeId> = column
                            .iter()
                            .filter(|w| {
                                *w != faulty
                                    && up(w)
                                    && !drops(w, *slot)
                                    && self.acceptors[w]
                                        .role
                                        .as_ref()
                                        .and_then(|role| role.vote(*slot))
                                        .is_none_or(|(b, _)| b < ballot)
                            })
                            .copied()
                            .collect();
                        let answering = column.len() / 2;
                        if keeping.len() < answering {
                            continue;
                        }
                        let kept: BTreeSet<NodeId> = keeping[..answering]
                            .iter()
                            .copied()
                            .chain([*faulty])
                            .collect();
                        let silent: BTreeSet<NodeId> = column
                            .iter()
                            .filter(|n| !kept.contains(n))
                            .copied()
                            .collect();
                        if silent.is_empty() {
                            continue;
                        }
                        steers.push(Late {
                            campaign: None,
                            faulty: *faulty,
                            slot: *slot,
                            silent,
                            kept,
                        });
                    }
                }
            }
        }
        steers
    }

    /// A message to replica `to`.
    fn deliver_to_replica(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: Message,
        cx: &mut Cx<'_, Self>,
    ) {
        let mut message = message;
        if let Message::PreReadAck { segment, bound, .. } = &mut message
            && self.mutant == RolesMutant::ReadNoFence
        {
            *bound = *segment;
        }
        if let Message::PreReadAck { read, .. } = &message {
            sometimes!(
                self.read_boots
                    .get(&to)
                    .is_some_and(|boot| read.boot < *boot),
                reach::READ_STALE_ACK
            );
        }
        let pulled = matches!(message, Message::Commit { .. }) && self.replicas.contains_key(&from);
        let node = self.replicas.get_mut(&to).expect("a replica");
        if let Some(role) = &mut node.role {
            let before = role.front();
            let effects = role.step(from, message);
            sometimes!(pulled && role.front() > before, reach::PULLED);
            self.replica_effects(to, effects, cx);
        }
    }

    /// A message to the leader of proposer `to`.
    fn deliver_to_leader(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: Message,
        cx: &mut Cx<'_, Self>,
    ) {
        let leader = self.leaders.get_mut(&to).expect("a leader");
        let mut message = message;
        if let Message::Promise { page, .. } = &mut message
            && self.mutant == RolesMutant::DroppedHidden
        {
            page.dropped.clear();
        }
        let forged = match &message {
            Message::CompactedTo { segment, .. } if self.mutant == RolesMutant::ForgedCompacted => {
                Self::set_of(*segment)
            }
            _ => Vec::new(),
        };
        let nack = matches!(message, Message::Nack { .. });
        sometimes!(
            matches!(&message, Message::Promise { page, .. } if page.next.is_some()),
            reach::PAGED
        );
        let before = leader.phase().clone();
        let blocked: Vec<Slot> = leader.blocked().to_vec();
        let learned = matches!(message, Message::Learned { .. });
        let mut effects = leader.step(from, message.clone());
        for node in forged {
            effects.extend(leader.step(node, message.clone()));
        }
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
                LeaderPhase::Binding | LeaderPhase::Phase1 | LeaderPhase::Stopped => {}
            }
            sometimes!(!leader.blocked().is_empty(), reach::BLOCKED);
        }
        let b = leader.ballot().round;
        for slot in blocked.iter().filter(|s| !leader.blocked().contains(s)) {
            if learned {
                sometimes!(true, reach::LEARNED);
                let Message::Learned { command, .. } = &message else {
                    unreachable!("a learn")
                };
                cx.record(RolesEvent::Learn {
                    b,
                    s: slot.0,
                    command: command.clone(),
                });
            } else if let Some(a) = leader.phase1().dropped_by(*slot) {
                sometimes!(true, reach::SKIPPED);
                cx.record(RolesEvent::Skip {
                    b,
                    s: slot.0,
                    a: a.0,
                });
            } else {
                sometimes!(true, reach::PROBED);
                let q = leader
                    .phase1()
                    .qualifying(*slot)
                    .iter()
                    .map(|n| n.0)
                    .collect();
                cx.record(RolesEvent::Probe { b, s: slot.0, q });
            }
        }
        self.leader_effects(to, effects, cx);
    }

    /// The walk of `shape` with `mutant` planted. The fresh journal is
    /// unset, or multi-writer on one seed in four.
    ///
    /// # Panics
    ///
    /// When the first segment cannot open.
    pub fn build(shape: Shape, mutant: RolesMutant, cx: &mut Cx<'_, Self>) -> Self {
        // A multi-writer fresh journal takes the tag of its configuration:
        // the first id of the walk. Uuids and tags come from one counter.
        let initial = if cx.rng().chance(1, 4) {
            Leader::multi(LeaderUuid(1))
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
            proxies: PROXIES
                .iter()
                .map(|p| (NodeId(*p), Some(JournalProxy::new(NodeId(*p)))))
                .collect(),
            proxy_crashed: false,
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
            uuids: u128::from(initial.is_multi()),
            belows: Vec::new(),
            settled_belows: Vec::new(),
            echo: None,
            unknown: Vec::new(),
            proposed: BTreeMap::new(),
            traced: BTreeMap::new(),
            applied: BTreeMap::new(),
            dropped_votes: BTreeMap::new(),
            late_repairs: Vec::new(),
            records: BTreeMap::new(),
            reads: BTreeMap::new(),
            stamps: BTreeMap::new(),
            dropped: BTreeSet::new(),
            waiting: Vec::new(),
            held_reads: BTreeMap::new(),
            read_ids: BTreeMap::new(),
            read_boots: BTreeMap::new(),
            slow: None,
            deaf: None,
            blinded: BTreeSet::new(),
            settled_served: false,
            late: None,
            held_promises: Vec::new(),
            held_compacts: Vec::new(),
            repromises: Vec::new(),
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
        let grid: Vec<Vec<u64>> = opening
            .grid
            .columns()
            .iter()
            .map(|column| column.iter().map(|n| n.0).collect())
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
        // The controller mostly waits while a replica is deaf: a reshape
        // ends the segment before its peers compact to a blind point.
        if self.deaf.is_some() && !cx.rng().chance(1, 8) {
            return;
        }
        let mut pool: Vec<NodeId> = (1..=POOL).map(NodeId).collect();
        // A draw of the pool's order.
        for i in (1..pool.len()).rev() {
            let j = usize::try_from(cx.rng().below(u64::try_from(i + 1).expect("small")))
                .expect("small");
            pool.swap(i, j);
        }
        // On three reshapes in four, the new grid leaves out the acceptors of
        // the grid in force where it can: their vote watermarks stay at the
        // seal, so a read at the replica that hears the register late is
        // served, and only the fence of the later segment refuses it.
        if cx.rng().chance(3, 4)
            && let Some(in_force) = self.register.in_force()
        {
            let old: BTreeSet<NodeId> = in_force
                .grid()
                .columns()
                .iter()
                .flatten()
                .copied()
                .collect();
            pool.sort_by_key(|node| old.contains(node));
        }
        let columns = if self.shape.wide && cx.rng().chance(1, 2) {
            vec![pool[..3].to_vec(), pool[3..].to_vec()]
        } else {
            vec![pool[..3].to_vec()]
        };
        let grid = Grid::new(1, columns).expect("a v1 grid");
        // At each reshape, one replica of the segment in force hears the
        // register late: it learns a value only when it restarts or the
        // faults stop, so a read there can meet the next segment before the
        // replica knows the seal.
        self.slow = self
            .register
            .in_force()
            .map(SegmentRecord::id)
            .and_then(|in_force| {
                let set = Self::set_of(in_force);
                cx.rng().index(set.len()).map(|pick| set[pick])
            });
        self.open_segment(grid, cx);
    }

    /// A proposer campaigns: a new ballot, bound to the newest segment.
    fn campaign(&mut self, cx: &mut Cx<'_, Self>) {
        self.release_late(false, cx);
        // A steer whose faulty vote is gone is dropped.
        if let Some(l) = &self.late
            && l.campaign.is_none()
            && !self.acceptors[&l.faulty].disk.faulty.contains_key(&l.slot)
        {
            self.release_late(true, cx);
        }
        let newest = self.register.newest().expect("a segment").id();
        // A live leader of the newest segment is deposed only in the chaos
        // phase, on one campaign in eight: a walk that deposes every leader
        // soon answers most calls `Unknown`.
        // A Phase 1 that reads a segment retired since its bind waits
        // for acceptors that stopped: the election timeout gives up on it.
        let alive = |l: &JournalLeader<u64>| {
            l.segment().is_none_or(|segment| segment == newest)
                && *l.phase() != LeaderPhase::Stopped
                && !(*l.phase() == LeaderPhase::Phase1
                    && l.segments().iter().any(|read| {
                        self.register
                            .segment(read.id)
                            .is_some_and(|r| r.state() >= SegmentState::Retired)
                    }))
        };
        let live = self.leaders.values().any(alive);
        // Steer: while a first leader of the newest segment waits on its
        // seal, one campaign in two starts a second one on another
        // proposer. Its Phase 1 may read a vote the first one missed, so the
        // two seal ends differ, and the leader whose seal comes second sees
        // the end of the other.
        let sealing: Vec<NodeId> = self
            .leaders
            .iter()
            .filter(|(_, l)| {
                l.segment() == Some(newest) && matches!(l.phase(), LeaderPhase::Sealing { .. })
            })
            .map(|(node, _)| *node)
            .collect();
        let second = !sealing.is_empty() && cx.phase() == Phase::Chaos && cx.rng().chance(1, 2);
        // Steer: while a repair is late, one campaign in two deposes the
        // leader and reads from the start of the oldest segment, so it
        // proposes the repaired slot again at a later ballot.
        let again =
            !self.late_repairs.is_empty() && cx.phase() == Phase::Chaos && cx.rng().chance(1, 2);
        // Steer: a rot armed a late answer; the next campaign takes it.
        let armed = cx.phase() == Phase::Chaos
            && self.late.as_ref().is_some_and(|l| {
                l.campaign.is_none()
                    && l.silent.iter().any(|x| {
                        self.acceptors[x].role.as_ref().is_some_and(|role| {
                            role.dropped()
                                .iter()
                                .any(|(start, below)| *start <= l.slot && l.slot < *below)
                        })
                    })
            });
        let second = second || again || armed;
        // After the faults stop, a new campaign tells the acceptors that
        // started again their belief: until a `Prepare` does, an acceptor
        // answers no read (§J3.3), and no read can be served.
        // That campaign then runs to its end: a second one while the first
        // binds or runs its Phase 1 would depose it before its `Prepare`
        // lands, round after round.
        let running = self
            .leaders
            .values()
            .any(|l| alive(l) && matches!(l.phase(), LeaderPhase::Binding | LeaderPhase::Phase1));
        let unread =
            cx.phase() == Phase::Settle && !self.settled_served && !running && self.reads_blind();
        if live && !second && !unread && (cx.phase() != Phase::Chaos || !cx.rng().chance(1, 8)) {
            return;
        }
        let others: Vec<u64> = PROPOSERS
            .iter()
            .copied()
            .filter(|p| !second || !sealing.contains(&NodeId(*p)))
            .collect();
        let proposer = NodeId(*cx.rng().pick(&others).expect("proposers"));
        self.round += 1;
        let ballot = paros_journal::register::Ballot {
            round: self.round,
            node: proposer,
        };
        let value = self.register.clone();
        // One campaign in three reads from the start of the oldest segment
        // that is not retired, below the slots its acceptors may have
        // dropped: their promises name the dropped ranges. The applied
        // prefix only grows, so a first slot read now stays valid at the
        // bind, which may see a later segment.
        // An acceptor that dropped a slot under another's faulty vote
        // answers this campaign late, on one campaign in two; the campaign
        // reads the oldest segment, so its Phase 1 reads the slot.
        let late = if armed {
            self.late.take()
        } else if self.late.is_none() && cx.phase() == Phase::Chaos {
            let steers = self.late_steers(true);
            cx.rng()
                .pick(&steers)
                .cloned()
                .filter(|_| cx.rng().chance(1, 2))
        } else {
            None
        };
        let from = if again || late.is_some() || cx.rng().chance(1, 3) {
            Self::oldest_read(&value, newest).start().unwrap_or(Slot(0))
        } else {
            self.applied_from(&value, newest)
        };
        let (mut leader, effects) = JournalLeader::campaign(Campaign { ballot, from });
        if cx.rng().chance(1, 2) {
            leader.delegate_to(PROXIES.iter().map(|p| NodeId(*p)).collect());
        }
        // A proposer runs one leader: its earlier one stops, and answers
        // its callers `Unknown`.
        if let Some(mut old) = self.leaders.insert(proposer, leader) {
            let stopped = old.stop();
            self.leader_effects(proposer, stopped, cx);
        }
        if let Some(mut late) = late {
            self.release_late(true, cx);
            late.campaign = Some((proposer, ballot));
            self.late = Some(late);
        }
        self.leader_effects(proposer, effects, cx);
    }

    /// The done bind of `ballot`, the leader at `node`: the walk records the
    /// segment it is bound to, the newest of `value`, and the leader takes
    /// `value` and starts Phase 1. With the mutant `StaleBinding` the leader
    /// is given the value before the seal of the newest segment's
    /// predecessor instead, with its bind: a driver that hands a leader a
    /// stale value, which binds it to the sealed segment.
    fn bind(
        &mut self,
        node: NodeId,
        ballot: paros_journal::register::Ballot,
        value: &RegisterValue,
        cx: &mut Cx<'_, Self>,
    ) {
        let newest = value.newest().expect("a segment").id();
        let mut given = value.clone();
        let mut segment = newest;
        if self.mutant == RolesMutant::StaleBinding
            && let Some(stale) = self.unseal(newest)
        {
            given = stale
                .apply(&Change::Bind { ballot })
                .expect("a stale value has no bind");
            segment = SegmentId(newest.0 - 1);
        }
        self.bound.insert((ballot.round, ballot.node), segment);
        let Some(leader) = self.leaders.get_mut(&node) else {
            return;
        };
        if leader.ballot() != ballot || *leader.phase() != LeaderPhase::Binding {
            return;
        }
        let effects = leader.observe(&given);
        assert_eq!(
            leader.segment(),
            Some(segment),
            "a done bind binds the leader to the newest segment"
        );
        // The leader reads from the first slot of its campaign, raised to
        // the start of its oldest segment when a retire landed before the
        // bind.
        cx.record(RolesEvent::Register {
            b: ballot.round,
            from: leader.from().0,
        });
        self.leader_effects(node, effects, cx);
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
            before.heals(),
            SegmentState::InForce,
            before.start(),
            None,
            None,
        )
        .ok()?;
        segments.insert(before.id(), open);
        Some(RegisterValue::from_parts(self.register.term(), segments))
    }

    /// The oldest segment that a leader of `segment` reads: the oldest that
    /// is not retired.
    fn oldest_read(value: &RegisterValue, segment: SegmentId) -> &SegmentRecord {
        value
            .segments()
            .values()
            .find(|r| r.state() < SegmentState::Retired && r.id() <= segment)
            .expect("a segment is read")
    }

    /// The first slot a leader of `segment` reads: the applied prefix of the
    /// oldest segment that is not retired, on the disks of its replicas.
    fn applied_from(&self, value: &RegisterValue, segment: SegmentId) -> Slot {
        let oldest = Self::oldest_read(value, segment);
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
        let mut front = disk.trim.map_or(start, |trim| trim.slot);
        while disk.chosen.contains_key(&front) {
            front = Slot(front.0 + 1);
        }
        Some(front)
    }

    /// Whether acceptor `node` is in the grid of a segment that is not
    /// retired.
    /// Whether a column of the grid in force has no majority of acceptors
    /// that know a belief: a leaderless read there waits for a `Prepare`.
    fn reads_blind(&self) -> bool {
        let Some(record) = self.register.in_force() else {
            return false;
        };
        record.grid().columns().iter().any(|column| {
            let knowing = column
                .iter()
                .filter(|node| {
                    self.acceptors[node]
                        .role
                        .as_ref()
                        .is_some_and(|role| role.bound().is_some())
                })
                .count();
            2 * knowing <= column.len()
        })
    }

    fn active(&self, node: NodeId) -> bool {
        self.register.segments().values().any(|record| {
            record.state() < SegmentState::Retired
                && record.grid().columns().iter().flatten().any(|n| *n == node)
        })
    }

    /// The event of an acceptor's queued batch: a vote, or a promise alone.
    fn queued(node: NodeId, ops: &[AcceptorOp]) -> RolesEvent {
        if let [AcceptorOp::Compact { segment, below, .. }] = ops {
            return RolesEvent::Compact {
                a: node.0,
                p: segment.0,
                x: below.0,
            };
        }
        let votes: Vec<_> = ops
            .iter()
            .filter_map(|op| match op {
                AcceptorOp::Write(AcceptorWrite::AppendAccepted { slot, ballot, .. }) => {
                    Some((*slot, *ballot))
                }
                AcceptorOp::Write(AcceptorWrite::SetPromise(_)) => None,
                AcceptorOp::Compact { .. } => {
                    unreachable!("a compaction is a batch of its own: {ops:?}")
                }
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
            (None, [AcceptorOp::Write(AcceptorWrite::SetPromise(ballot))]) => RolesEvent::Promise {
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
                    // One `Repair` in two is late: the network holds it
                    // until a rot steers to it, a tick lets it go, or the
                    // faults stop.
                    if matches!(message, Message::Repair { .. })
                        && cx.phase() != Phase::Settle
                        && cx.rng().chance(1, 2)
                    {
                        self.late_repairs.push(Envelope {
                            from: node,
                            to,
                            message,
                        });
                        continue;
                    }
                    if let Message::Commit { slot, command, .. } = &message {
                        let ballot = self.leaders[&node].ballot();
                        self.commit_seen(*slot, command, (ballot.round, ballot.node));
                    }
                    if let Message::Accept {
                        ballot,
                        slot,
                        command,
                    }
                    | Message::Delegate {
                        ballot,
                        slot,
                        command,
                        ..
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
                // The walk's acceptors of a retired segment stop on their
                // own: the walk asks no step of them after the retire.
                LeaderEffect::Retired { .. } => {}
            }
        }
        // A writer whose call was just answered `Written` lost the answer on
        // one call in two, and resends the write to the leader that applied
        // it.
        if self.echo.is_some() && cx.rng().chance(1, 2) {
            self.echo(cx);
        }
    }

    fn proxy_effects(&mut self, node: NodeId, effects: Vec<ProxyEffect>, cx: &mut Cx<'_, Self>) {
        // The ballot of each slot this step chose, from the `Chosen` it
        // tells the leader.
        let chosen: BTreeMap<Slot, paros_journal::register::Ballot> = effects
            .iter()
            .filter_map(|effect| match effect.message {
                Message::Chosen { ballot, slot } => Some((slot, ballot)),
                _ => None,
            })
            .collect();
        for ProxyEffect { to, message } in effects {
            match &message {
                Message::Accept {
                    ballot,
                    slot,
                    command,
                } => assert_eq!(
                    self.proposed.get(&(ballot.round, *slot)),
                    Some(command),
                    "a proxy sends the command its leader delegated"
                ),
                Message::Commit { slot, command, .. } => {
                    let ballot = chosen[slot];
                    self.judge_proxy(*slot, ballot);
                    sometimes!(true, reach::PROXIED);
                    sometimes!(self.proxy_crashed, reach::PROXY_CRASHED);
                    self.commit_seen(*slot, command, (ballot.round, ballot.node));
                }
                _ => {}
            }
            cx.send(Envelope {
                from: node,
                to,
                message,
            });
        }
    }

    /// The oracle [`PROXY`]: `F + 1` acceptors of the column of `slot` in
    /// the grid of `ballot`'s segment hold a vote at `ballot` or above on
    /// their disk.
    fn judge_proxy(&self, slot: Slot, ballot: paros_journal::register::Ballot) {
        let segment = self.bound[&(ballot.round, ballot.node)];
        let grid = self
            .register
            .segment(segment)
            .expect("a bound segment is in the register")
            .grid();
        let columns = grid.columns();
        let count = u64::try_from(columns.len()).expect("few columns");
        let column = &columns[usize::try_from(slot.0 % count).expect("small")];
        // A vote that a compaction dropped from the disk was on it.
        let voted = column
            .iter()
            .filter(|node| {
                self.acceptors[node]
                    .disk
                    .votes
                    .get(&slot)
                    .map(|(b, _)| *b)
                    .max(self.dropped_votes.get(&(**node, slot)).copied())
                    .is_some_and(|b| b >= ballot)
            })
            .count();
        assert!(
            voted > column.len() / 2,
            "{PROXY}: slot {slot:?} committed at {ballot:?} with {voted} votes on disk in {column:?}"
        );
    }

    fn acceptor_effects(
        &mut self,
        node: NodeId,
        effects: Vec<AcceptorEffect>,
        cx: &mut Cx<'_, Self>,
    ) {
        for effect in &effects {
            if let AcceptorEffect::Persist { ops, .. } = effect {
                for op in ops {
                    if let AcceptorOp::Compact {
                        segment,
                        start,
                        below,
                    } = op
                    {
                        self.judge_compact(*segment, *start, *below);
                    }
                }
            }
        }
        // A promise in a step that writes nothing is one sent again.
        let wrote = effects
            .iter()
            .any(|e| matches!(e, AcceptorEffect::Persist { .. }));
        let acceptor = self.acceptors.get_mut(&node).expect("an acceptor");
        for effect in effects {
            match effect {
                AcceptorEffect::Persist { id, ops } => acceptor.batches.push_back((id, ops)),
                AcceptorEffect::Send { to, message, after } => {
                    let again = match &message {
                        Message::Promise { ballot, .. } if !wrote => Some(ballot.round),
                        _ => None,
                    };
                    let envelope = Envelope {
                        from: node,
                        to,
                        message,
                    };
                    match after {
                        Some(id) if acceptor.disk.durable.is_none_or(|d| d < id) => {
                            acceptor.held.push((id, envelope));
                            if let Some(b) = again {
                                self.repromises.push((node, id, to, b));
                            }
                        }
                        _ => {
                            if let Some(b) = again {
                                cx.record(RolesEvent::Repromise { a: node.0, b });
                            }
                            cx.send(envelope);
                        }
                    }
                }
            }
        }
    }

    /// The mutants that change a message of replica `node` before it goes.
    fn plant_in_message(&self, node: NodeId, message: &mut Message) {
        if let Message::Holds { full, .. } = message
            && self.mutant == RolesMutant::HoldsLie
        {
            *full = true;
        }
        if let Message::Applied { answer, .. } = message {
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
            if self.mutant == RolesMutant::BelowInSegment
                && let Answer::Taken(view) = answer
            {
                *answer = Answer::Below(*view);
            }
        }
    }

    fn replica_effects(
        &mut self,
        node: NodeId,
        effects: Vec<ReplicaEffect>,
        cx: &mut Cx<'_, Self>,
    ) {
        self.take_answers(node);
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
                    if let Message::PreRead { read, .. } = &message {
                        self.read_boots.insert(node, read.boot);
                    }
                    self.plant_in_message(node, &mut message);
                    if let Message::Applied { slot, answer, .. } = &message {
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
                                && !(self.mutant == RolesMutant::BoundaryBeforeDurable
                                    && matches!(change, Change::Boundary { .. })) =>
                        {
                            replica.changes.push((id, change));
                        }
                        _ => self.queue.push((Origin::Replica(node), change)),
                    }
                }
                ReplicaEffect::Read {
                    read,
                    mut answer,
                    after,
                } => {
                    if self.mutant == RolesMutant::ReadUnserved
                        && matches!(answer, ReadAnswer::Page { .. })
                    {
                        answer = ReadAnswer::Unserved;
                    }
                    if self.mutant == RolesMutant::ShortPage
                        && let ReadAnswer::Page { records, .. } = &mut answer
                        && records.len() >= 2
                    {
                        records.pop();
                    }
                    let replica = &self.replicas[&node];
                    match after {
                        Some(id) if replica.disk.durable.is_none_or(|d| d < id) => {
                            self.held_reads
                                .entry(node)
                                .or_default()
                                .push((id, read, answer));
                        }
                        _ => self.judge_read(node, read, &answer, cx),
                    }
                }
                // A replica of a trimmed segment drops its copy: it answers
                // no read and takes no step after it. Its batches in flight
                // still reach the disk, as the driver keeps them for a
                // dropped role (`Stop::Dropped`), and then the copy goes
                // (`Drop` of the model).
                ReplicaEffect::Drop => {
                    assert!(
                        self.register
                            .segment(Self::segment_of_replica(node))
                            .is_some_and(|r| r.state() == SegmentState::Trimmed),
                        "the roles walk drops only the copies of a trimmed segment: {node:?}"
                    );
                    while !self.replicas[&node].batches.is_empty() {
                        self.flush_replica(node, cx);
                    }
                    // A replica that restarts after its drop drops again:
                    // the model's copy went once.
                    if self.dropped.insert(node) {
                        cx.record(RolesEvent::Drop { r: node.0 });
                    }
                }
                ReplicaEffect::Copied { .. } => {
                    unreachable!("the roles walk has no heal: {node:?}")
                }
            }
        }
    }

    /// The oracle [`COMPACT`] on a compaction of `segment` from `start` to
    /// `below`.
    fn judge_compact(&self, segment: SegmentId, start: Slot, below: Slot) {
        sometimes!(true, reach::DROPPED);
        let compacted = Self::set_of(segment)
            .iter()
            .filter(|node| {
                self.replicas
                    .get(node)
                    .and_then(|r| r.disk.trim)
                    .is_some_and(|trim| trim.slot >= below)
            })
            .count();
        assert!(
            compacted >= 2,
            "{COMPACT}: segment {segment:?} dropped below {below:?} with {compacted} replicas compacted there"
        );
        for slot in (start.0..below.0).map(Slot) {
            assert!(
                self.chosen.contains_key(&slot),
                "{COMPACT}: slot {slot:?} of segment {segment:?} dropped and not chosen"
            );
        }
    }

    /// The answers of the slots replica `node` applied in memory since the
    /// last step, before a compaction drops them.
    fn take_answers(&mut self, node: NodeId) {
        let Some(role) = self.replicas[&node].role.as_ref() else {
            return;
        };
        let (Some(start), Some(front)) = (role.start(), role.front()) else {
            return;
        };
        let applied = self.applied.entry(node).or_default();
        let traced = self.traced.get(&node).copied().unwrap_or(start);
        let from = applied
            .keys()
            .next_back()
            .map_or(traced, |last| Slot(last.0 + 1))
            .max(traced);
        for slot in (from.0..front.0).map(Slot) {
            applied.insert(slot, role.answer(slot).cloned());
        }
    }

    /// A leader saw `slot` chosen: the oracles [`AGREE`] and [`SEALED`],
    /// and the truth over the chosen prefix.
    fn commit_seen(&mut self, slot: Slot, command: &Command, ballot: (u64, NodeId)) {
        if let Command::Write(write) = command {
            let time = write.records.first().and_then(|r| r.headers.leader_time);
            assert!(
                time.and_then(|t| self.stamps.get(&t)) == Some(&as_written(command))
                    && write.records.iter().all(|r| r.headers.leader_time == time),
                "{STAMPED}: slot {slot:?} chose {write:?}, the proposal of its time was {:?}",
                time.and_then(|t| self.stamps.get(&t))
            );
        }
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
            let next = self.truth.next;
            let verdict = self.truth.apply(&command);
            if let Command::Write(write) = &command
                && self.truth.next > next
            {
                for (at, record) in (next..).zip(&write.records) {
                    self.records.insert(at, record.clone());
                }
            }
            self.verdicts.push(verdict);
            self.states
                .push((self.truth.leader, self.truth.next, self.truth.first));
        }
    }

    /// The oracle [`REPAIRED`] on the vote that acceptor `node` holds at
    /// `slot` after a repair.
    fn judge_repair(
        &self,
        node: NodeId,
        slot: Slot,
        vote: Option<&(paros_journal::register::Ballot, Command)>,
    ) {
        let chosen = self.chosen.get(&slot);
        assert!(
            chosen.is_some_and(|(command, round, _)| {
                vote.is_some_and(|(ballot, held)| held == command && ballot.round >= *round)
            }),
            "{REPAIRED}: acceptor {node:?} holds {vote:?} at {slot:?}, the slot chose {chosen:?}"
        );
    }

    /// Whether a replica applied `slot` on its disk and still holds it
    /// (`Holds` of `specs/tla/journal-state/`).
    fn learned(&self, slot: Slot) -> bool {
        self.replicas.iter().any(|(node, replica)| {
            let start = match replica.disk.base {
                Some((_, start)) => start,
                None if Self::segment_of_replica(*node) == SegmentId(1) => Slot(0),
                None => return false,
            };
            let compacted = replica.disk.trim.map_or(Slot(0), |trim| trim.slot);
            !self.dropped.contains(node)
                && start <= slot
                && compacted <= slot
                && self.traced.get(node).is_some_and(|front| slot < *front)
        })
    }

    /// An up acceptor boots and finds a vote of its disk damaged (CTRL,
    /// §J5.3): the value is lost and the ballot stays. The walk damages a
    /// vote only inside the contract (`Spare` of `specs/tla/journal-ctrl/`):
    /// a replica applied the slot, or the slot chose another command, so
    /// the value is not lost for good.
    fn rot(&mut self, cx: &mut Cx<'_, Self>) {
        let candidates = self.rot_candidates();
        // Steer: one rot in two damages a vote above a late repair.
        let late = self.late_rot(&candidates);
        let steered = !late.is_empty() && cx.rng().chance(1, 2);
        // Half of the rots take a vote at a slot that another acceptor
        // dropped and that a third acceptor never voted at that ballot or
        // above: a later Phase 1 on the rotted acceptor and the third one
        // blocks the slot, and the dropper's late answer drops it
        // (`SKIPPED`).
        let drops = |a: &AcceptorNode, slot: &Slot| {
            a.disk
                .dropped
                .iter()
                .any(|(start, below)| start <= slot && slot < below)
        };
        let under: Vec<_> = candidates
            .iter()
            .filter(|(node, slot, ballot)| {
                let peers = self.column_peers(*node);
                let dropper = self
                    .acceptors
                    .iter()
                    .find(|(other, a)| peers.contains(other) && drops(a, slot));
                dropper.is_some_and(|(x, _)| {
                    self.acceptors.iter().any(|(w, a)| {
                        peers.contains(w)
                            && w != x
                            && a.role.is_some()
                            && !drops(a, slot)
                            && a.disk.votes.get(slot).is_none_or(|(b, _)| b < ballot)
                    })
                })
            })
            .copied()
            .collect();
        let pool = if steered {
            &late
        } else if !under.is_empty() && cx.rng().chance(1, 2) {
            &under
        } else {
            &candidates
        };
        let Some(&(node, slot, _)) = cx.rng().pick(pool) else {
            return;
        };
        self.rot_at(node, slot, steered, cx);
        // Steer: a rot that leaves a faulty vote at a slot that a member of
        // its column dropped arms a late answer for the next campaign.
        if cx.phase() == Phase::Chaos && self.late.is_none() {
            let steers: Vec<Late> = self
                .late_steers(false)
                .into_iter()
                .filter(|late| late.faulty == node && late.slot == slot)
                .collect();
            self.late = cx.rng().pick(&steers).cloned();
        }
    }

    /// The votes a rot may damage, with their ballots: up acceptors only.
    fn rot_candidates(&self) -> Vec<(NodeId, Slot, paros_journal::register::Ballot)> {
        self.acceptors
            .iter()
            .filter(|(_, acceptor)| acceptor.role.is_some())
            .flat_map(|(node, acceptor)| {
                acceptor
                    .disk
                    .votes
                    .iter()
                    .map(move |(slot, vote)| (*node, *slot, vote.clone()))
            })
            .filter(|(node, slot, (_, command))| {
                self.learned(*slot)
                    || self
                        .chosen
                        .get(slot)
                        .is_some_and(|(chosen, ..)| chosen != command)
                    || self.held_by_quorum(*node, *slot)
                    || self.unchoosable(*node, *slot)
            })
            .map(|(node, slot, (ballot, _))| (node, slot, ballot))
            .collect()
    }

    /// Whether no ballot can choose the value of `node`'s vote at `slot`
    /// (`Unchoosable` of `specs/tla/journal-ctrl/`): a majority of the
    /// column of `slot` in the grid that the vote's ballot is bound to,
    /// without `node`, has no vote at `slot` on disk or in a batch, kept
    /// `slot`, and promised above the vote's ballot on disk. Every quorum of
    /// that column meets it, so no quorum votes at that ballot, and the
    /// value is not needed. The walk keeps the rot inside `within_f`, so the
    /// tally can still decide the slot.
    fn unchoosable(&self, node: NodeId, slot: Slot) -> bool {
        let Some((ballot, _)) = self.acceptors[&node].disk.votes.get(&slot) else {
            return false;
        };
        if !self.within_f(node, slot) {
            return false;
        }
        let Some(record) = self
            .bound
            .get(&(ballot.round, ballot.node))
            .and_then(|segment| self.register.segment(*segment))
        else {
            return false;
        };
        let columns = record.grid().columns();
        let index = usize::try_from(slot.0 % u64::try_from(columns.len()).expect("few"))
            .expect("a column index");
        let column = &columns[index];
        let empty = column
            .iter()
            .filter(|x| **x != node)
            .filter(|x| {
                let acceptor = &self.acceptors[*x];
                let pending = acceptor
                    .batches
                    .iter()
                    .flat_map(|(_, ops)| ops)
                    .any(|op| match op {
                        AcceptorOp::Write(AcceptorWrite::AppendAccepted { slot: at, .. }) => {
                            *at == slot
                        }
                        AcceptorOp::Compact { start, below, .. } => {
                            (*start..*below).contains(&slot)
                        }
                        AcceptorOp::Write(_) => false,
                    });
                !pending
                    && !acceptor.disk.votes.contains_key(&slot)
                    && !acceptor.disk.faulty.contains_key(&slot)
                    && !acceptor
                        .disk
                        .dropped
                        .iter()
                        .any(|(start, below)| (*start..*below).contains(&slot))
                    && acceptor.disk.promised > *ballot
            })
            .count();
        2 * empty > column.len()
    }

    /// Whether a damaged vote of `node` at `slot` keeps at most `F` damaged
    /// votes at `slot` in each column of each grid that is not retired. A
    /// slot that no replica holds is decided only by the tally, which needs
    /// a majority of a column that answers without a damaged vote above the
    /// highest readable one: CTRL tolerates `F` faulty acceptors of
    /// `2F + 1` (§J5.3). The model allows more, and checks safety over them.
    fn within_f(&self, node: NodeId, slot: Slot) -> bool {
        self.register
            .segments()
            .values()
            .filter(|record| record.state() < SegmentState::Retired)
            .all(|record| {
                record.grid().columns().iter().all(|column| {
                    let faulty = column
                        .iter()
                        .filter(|x| {
                            **x == node || self.acceptors[*x].disk.faulty.contains_key(&slot)
                        })
                        .count();
                    faulty <= column.len() / 2
                })
            })
    }

    /// Whether a majority of the grid of a segment that is not retired,
    /// without `node`, holds on disk the command chosen at `slot`, each
    /// vote bound to that segment and at or above the ballot that chose it
    /// (the third arm of `Spare` in `specs/tla/journal-ctrl/`): the value
    /// stays on disk elsewhere, so `node`'s vote may rot, inside `within_f`.
    fn held_by_quorum(&self, node: NodeId, slot: Slot) -> bool {
        let Some((command, round, by)) = self.chosen.get(&slot) else {
            return false;
        };
        if !self.within_f(node, slot) {
            return false;
        }
        let chose = paros_journal::register::Ballot {
            round: *round,
            node: *by,
        };
        self.register
            .segments()
            .values()
            .filter(|record| record.state() < SegmentState::Retired)
            .any(|record| {
                let grid: Vec<NodeId> = record.grid().columns().iter().flatten().copied().collect();
                let holders = grid
                    .iter()
                    .filter(|x| **x != node)
                    .filter(|x| {
                        self.acceptors[x]
                            .disk
                            .votes
                            .get(&slot)
                            .is_some_and(|(b, c)| {
                                c == command
                                    && *b >= chose
                                    && self.bound.get(&(b.round, b.node)) == Some(&record.id())
                            })
                    })
                    .count();
                2 * holders > grid.len()
            })
    }

    /// The `candidates` above a late repair of their acceptor at their slot:
    /// a rot there, then the repair, makes the repair meet a faulty vote of
    /// a later ballot, which the repair keeps.
    fn late_rot(
        &self,
        candidates: &[(NodeId, Slot, paros_journal::register::Ballot)],
    ) -> Vec<(NodeId, Slot, paros_journal::register::Ballot)> {
        candidates
            .iter()
            .filter(|(node, slot, ballot)| {
                self.late_repairs.iter().any(|e| {
                    e.to == *node
                        && matches!(e.message, Message::Repair { ballot: b, slot: s, .. }
                            if s == *slot && b < *ballot)
                })
            })
            .copied()
            .collect()
    }

    /// The acceptor `node` boots and finds its vote at `slot` damaged. With
    /// `release`, the network then lets the late repairs of that vote go.
    fn rot_at(&mut self, node: NodeId, slot: Slot, release: bool, cx: &mut Cx<'_, Self>) {
        let acceptor = self.acceptors.get_mut(&node).expect("an acceptor");
        acceptor.crash();
        let (ballot, _) = acceptor.disk.votes.remove(&slot).expect("a vote");
        acceptor.disk.faulty.insert(slot, ballot);
        let held = self.dropped_votes.entry((node, slot)).or_insert(ballot);
        *held = (*held).max(ballot);
        acceptor.role = Some(JournalAcceptor::recover(
            node,
            acceptor.disk.clone(),
            self.shape.promise_page(),
        ));
        sometimes!(true, reach::ROTTED);
        cx.record(RolesEvent::Rot {
            a: node.0,
            s: slot.0,
        });
        if release {
            self.release_repairs(cx, |e| {
                e.to == node && matches!(e.message, Message::Repair { slot: s, .. } if s == slot)
            });
        }
    }

    /// The oracle [`FOLD`] on a replica's answer at `slot`.
    fn judge(&self, slot: Slot, answer: &Answer) {
        let at = usize::try_from(slot.0).expect("a small slot");
        let verdict = self.verdicts.get(at).copied();
        let name = label(answer);
        let segment = self.segment_holding(slot);
        // `Below` is the answer of a fenced write whose position lies below
        // the first position of the segment that holds its slot: the whole
        // log's next position at the segment's first slot.
        let below_base = || {
            let Some((Command::Write(write), ..)) = self.chosen.get(&slot) else {
                return false;
            };
            let Fence::Leader { expected_seq, .. } = write.fence else {
                return false;
            };
            let start = segment
                .and_then(|id| self.register.segment(id))
                .and_then(SegmentRecord::start)
                .expect("the segment of a slot a replica applied has a start");
            let at = usize::try_from(start.0).expect("a small slot");
            expected_seq.0 < self.states[at].1
        };
        assert!(
            Some(name) == verdict
                || (name == "below" && matches!(verdict, Some("dup" | "taken")) && below_base()),
            "{FOLD}: slot {slot:?} answered {answer:?}, the whole log says {verdict:?}"
        );
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
        // Steer: a seal waits on three register steps in four while a later
        // first leader of the same segment runs its Phase 1, so the two
        // leaders seal together and one sees the end of the other.
        if let (Origin::Leader(_, round), Change::Seal { .. }) = &self.queue[index]
            && cx.phase() == Phase::Chaos
            && self.leaders.values().any(|l| {
                l.ballot().round > *round
                    && *l.phase() == LeaderPhase::Phase1
                    && l.segment() == Some(self.register.newest().expect("a segment").id())
            })
            && cx.rng().chance(3, 4)
        {
            return;
        }
        let (origin, mut change) = self.queue.remove(index);
        if let (RolesMutant::StaleBoundary, Change::Boundary { segment, state }) =
            (self.mutant, &mut change)
            && let Some(base) = Self::set_of(*segment)
                .iter()
                .find_map(|node| self.replicas[node].disk.base)
        {
            *state = base.0;
        }
        self.judge_claim(&change);
        match self.register.apply(&change) {
            Ok(value) => {
                match (&change, origin) {
                    // `judge_claim` judged the state against the segment's
                    // end, which the change does not move.
                    (Change::Boundary { .. }, _) => {
                        sometimes!(true, reach::BOUNDARY);
                        if let Origin::Replica(node) = origin {
                            cx.record(RolesEvent::Boundary { r: node.0 });
                        }
                    }
                    (Change::Retire { segment, .. }, Origin::Leader(_, b)) => {
                        let end = value.segment(*segment).and_then(SegmentRecord::end);
                        let holders = Self::set_of(*segment)
                            .iter()
                            .filter(|node| end.is_some() && self.disk_front(**node) == end)
                            .count();
                        assert!(
                            holders > Self::set_of(*segment).len() / 2,
                            "{RETIRE}: {segment:?} retired by ballot {b} with {holders} holders on disk"
                        );
                        cx.record(RolesEvent::Retire { p: segment.0 });
                        sometimes!(true, reach::RETIRED);
                    }
                    (Change::Trim { segment, .. }, _) => {
                        let last = self
                            .register
                            .segment(*segment)
                            .and_then(SegmentRecord::boundary)
                            .map(|b| b.next_seq.0);
                        assert!(
                            last.is_some_and(|last| self.truth.first >= last),
                            "{TRIM}: {segment:?} trimmed with its last seq {last:?}, the decided floor is {}",
                            self.truth.first
                        );
                        if let Origin::Replica(node) = origin {
                            cx.record(RolesEvent::Trim {
                                p: segment.0,
                                r: node.0,
                            });
                        }
                        sometimes!(true, reach::SEGMENT_TRIMMED);
                    }
                    (Change::Bind { ballot }, Origin::Leader(node, _)) => {
                        self.bind(node, *ballot, &value, cx);
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

    /// [`BOUNDARY`] and [`TRIM`] on a replica's change before the register
    /// takes it: a change that the register then refuses is still the
    /// replica's claim, and is judged as if done. BOUNDARY reads the end
    /// from the register: a replica writes a boundary only once the register
    /// holds the segment's end (`Replica::boundary` needs a full copy), so
    /// the end is there whenever a replica claims one.
    fn judge_claim(&self, change: &Change) {
        match change {
            Change::Boundary { segment, state } => {
                let end = self.register.segment(*segment).and_then(SegmentRecord::end);
                let at = end.map(|e| usize::try_from(e.0).expect("small"));
                let truth = at.and_then(|at| self.states.get(at));
                assert!(
                    truth == Some(&(state.leader, state.next_seq.0, state.first_seq.0)),
                    "{BOUNDARY}: {segment:?} asked {state:?}, the whole log at {end:?} is {truth:?}"
                );
            }
            Change::Trim { segment, .. } => {
                let last = self
                    .register
                    .segment(*segment)
                    .and_then(SegmentRecord::boundary)
                    .map(|b| b.next_seq.0);
                assert!(
                    last.is_none_or(|last| self.truth.first >= last),
                    "{TRIM}: {segment:?} asked a trim with its last seq {last:?}, the decided floor is {}",
                    self.truth.first
                );
            }
            _ => {}
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
        // A replica may miss a value for a while: it learns it on a tick
        // or an `Observe` move, so a read can meet a later segment before
        // its replica knows the seal.
        let all = cx.phase() == Phase::Settle;
        let replicas: Vec<NodeId> = self.replicas.keys().copied().collect();
        for node in replicas {
            if all || (self.slow != Some(node) && cx.rng().chance(2, 3)) {
                self.observe_replica(node, cx);
            }
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
            before.heals(),
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
        // Steer: once a vote above a late repair of its slot may rot, it
        // rots at the next flush (its acceptor reboots), and the repair
        // arrives.
        if cx.phase() == Phase::Chaos && !self.late_repairs.is_empty() {
            let late = self.late_rot(&self.rot_candidates());
            if let Some(&(at, slot, _)) = cx.rng().pick(&late) {
                self.rot_at(at, slot, true, cx);
            }
        }
    }

    fn flush_acceptor(&mut self, node: NodeId, cx: &mut Cx<'_, Self>) {
        let acceptor = self.acceptors.get_mut(&node).expect("an acceptor");
        let (id, ops) = acceptor.batches.pop_front().expect("a batch");
        for op in &ops {
            if let AcceptorOp::Compact { start, below, .. } = op {
                for (slot, (ballot, _)) in acceptor.disk.votes.range(*start..*below) {
                    let held = self.dropped_votes.entry((node, *slot)).or_insert(*ballot);
                    *held = (*held).max(*ballot);
                }
            }
        }
        acceptor.disk.apply(id, &ops);
        cx.record(RolesEvent::Flush { a: node.0 });
        let (go, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut acceptor.held)
            .into_iter()
            .partition(|(held, _)| *held <= id);
        acceptor.held = keep;
        // The promises sent again that waited on this batch go now, with the
        // disk as the batch left it.
        let (again, wait): (Vec<_>, Vec<_>) = std::mem::take(&mut self.repromises)
            .into_iter()
            .partition(|(a, held, ..)| *a == node && *held <= id);
        self.repromises = wait;
        for (_, held, to, b) in again {
            let goes = go.iter().any(|(h, envelope)| {
                *h == held
                    && envelope.to == to
                    && matches!(envelope.message, Message::Promise { ballot, .. } if ballot.round == b)
            });
            if goes {
                cx.record(RolesEvent::Repromise { a: node.0, b });
            }
        }
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
        for op in &ops {
            let ReplicaOp::Compact(trim) = op else {
                continue;
            };
            // The deaf replica hears again once every peer of its set
            // compacted to a blind point: no peer serves the slots below
            // it, so its next pull meets a `TrimmedTo`.
            if trim.state.first_seq() < trim.floor
                && let Some(deaf) = self.deaf
                && deaf != node
                && Self::segment_of_replica(deaf) == segment
            {
                self.blinded.insert(node);
                if Self::set_of(segment)
                    .iter()
                    .all(|peer| *peer == deaf || self.blinded.contains(peer))
                {
                    self.deaf = None;
                }
            }
            let start = self.replicas[&node]
                .role
                .as_ref()
                .and_then(SegmentReplica::start)
                .expect("a replica compacts after its base");
            let traced = self.traced.get(&node).copied().unwrap_or(start);
            if trim.slot > traced {
                // Above the durable front: a jump to a peer's point.
                sometimes!(true, reach::JUMPED);
                cx.record(RolesEvent::Jump {
                    r: node.0,
                    slot: trim.slot.0,
                    floor: trim.floor.0,
                });
                self.traced.insert(node, trim.slot);
                if let Some(applied) = self.applied.get_mut(&node) {
                    applied.retain(|slot, _| *slot >= trim.slot);
                }
            } else {
                sometimes!(true, reach::COMPACTED);
                cx.record(RolesEvent::CompactReplica {
                    r: node.0,
                    slot: trim.slot.0,
                });
            }
        }
        // Each slot the fold on the disk passed: one `Apply`, with the
        // answer of the fold.
        if let Some(front) = self.disk_front(node) {
            let role = self.replicas[&node]
                .role
                .as_ref()
                .expect("a replica with batches runs");
            // A replica that has not observed its base yet folds nothing:
            // its slots on the disk wait for the base.
            let start = role.start().unwrap_or(Slot(0));
            let front = front.min(role.front().unwrap_or(start));
            let from = self.traced.get(&node).copied().unwrap_or(start);
            let applied = self.applied.entry(node).or_default();
            for slot in from.0..front.0 {
                let answer = applied
                    .remove(&Slot(slot))
                    .expect("the walk took the answer of every slot the replica applied");
                sometimes!(answer.is_none(), reach::BLIND);
                cx.record(RolesEvent::Apply {
                    r: node.0,
                    s: slot,
                    command: self.replicas[&node].disk.chosen[&Slot(slot)].clone(),
                    res: answer.as_ref().map(label),
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
        let (answers, wait): (Vec<_>, Vec<_>) = self
            .held_reads
            .remove(&node)
            .unwrap_or_default()
            .into_iter()
            .partition(|(held, ..)| *held <= id);
        self.held_reads.insert(node, wait);
        for (_, read, answer) in answers {
            self.judge_read(node, read, &answer, cx);
        }
    }

    /// The network lets the late repairs that `pick` names go.
    fn release_repairs(&mut self, cx: &mut Cx<'_, Self>, pick: impl Fn(&Envelope) -> bool) {
        let (go, keep): (Vec<Envelope>, Vec<Envelope>) = std::mem::take(&mut self.late_repairs)
            .into_iter()
            .partition(|e| pick(e));
        self.late_repairs = keep;
        for envelope in go {
            cx.send(envelope);
        }
    }

    fn tick(&mut self, cx: &mut Cx<'_, Self>) {
        let all = cx.phase() == Phase::Settle;
        // A late repair arrives on one tick in 64, and every one once the
        // faults stop.
        if all || cx.rng().chance(1, 64) {
            self.release_repairs(cx, |_| true);
        }
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
        let proxies: Vec<NodeId> = self.proxies.keys().copied().collect();
        for node in proxies {
            if !(all || cx.rng().chance(1, 3)) {
                continue;
            }
            let Some(Some(proxy)) = self.proxies.get_mut(&node) else {
                continue;
            };
            let effects = proxy.tick(RESEND_PAGE);
            self.proxy_effects(node, effects, cx);
        }
        let replicas: Vec<NodeId> = self.replicas.keys().copied().collect();
        for node in replicas {
            if !(all || cx.rng().chance(1, 3)) {
                continue;
            }
            if all || self.slow != Some(node) {
                self.observe_replica(node, cx);
            }
            let Some(role) = self.replicas.get_mut(&node).and_then(|r| r.role.as_mut()) else {
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
        // and the unfenced calls of the walk are at most two, or three in a
        // burst. So the calls in flight are at most one more than the
        // leader's window, and a leader whose window is full answers a
        // burst busy now and then.
        let waits = if let Some(index) = writer {
            self.calls.values().any(|call| call.writer == Some(index))
        } else {
            let limit = if cx.rng().chance(1, 8) { 3 } else { 2 };
            self.calls
                .values()
                .filter(|call| call.writer.is_none())
                .count()
                >= limit
        };
        if waits {
            return;
        }
        let Some(node) = self.writer_leader() else {
            return;
        };
        self.tokens += 1;
        let token = self.tokens;
        let leader = self.leaders.get_mut(&node).expect("a leader");
        let ballot = leader.ballot();
        // The walk's clock is the count of its calls: each proposal has its
        // own time.
        self.stamps.insert(token, as_written(&command));
        let (proposed, effects) = leader.propose(token, command.clone(), token, WINDOW);
        sometimes!(proposed == Proposed::Busy, reach::BUSY);
        // A busy call holds no slot: its client may send it again, as a
        // new call.
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
            new: Leader::uuid(LeaderUuid(self.uuids)),
            old,
        };
        self.writers[index].claimed = Some(LeaderUuid(self.uuids));
        self.propose(Some(index), command, cx);
    }

    fn write(&mut self, cx: &mut Cx<'_, Self>, stale: bool) {
        // After the faults stop, a writer writes until a read is served: a
        // read waits for the slot of the highest vote it hears, and a vote
        // of a deposed leader above the chosen prefix is decided only when
        // the leader proposes that slot.
        if cx.phase() == Phase::Settle {
            if self.settled_served {
                return;
            }
            // With no writer that believes it leads, a writer claims.
            if self.writers.iter().all(|w| w.uuid.is_none()) {
                self.claim(cx);
                return;
            }
        }
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

    /// The resend of [`Self::echo`]'s write, once its writer is free and a
    /// leader writes. Returns whether it sent it.
    fn echo(&mut self, cx: &mut Cx<'_, Self>) -> bool {
        let Some(index) = self.echo else {
            return false;
        };
        if self.calls.values().any(|call| call.writer == Some(index))
            || self.writer_leader().is_none()
        {
            return false;
        }
        self.echo = None;
        let Some(write) = self.writers[index].sent.last().cloned() else {
            return false;
        };
        self.propose(Some(index), Command::Write(write), cx);
        true
    }

    fn resend(&mut self, cx: &mut Cx<'_, Self>) {
        if self.echo(cx) {
            return;
        }
        if let Some(pick) = cx.rng().index(self.unknown.len())
            && cx.rng().chance(2, 3)
        {
            let (writer, command) = self.unknown.remove(pick);
            self.propose(writer, command, cx);
            return;
        }
        let index = cx.rng().index(WRITERS).expect("writers");
        // Half of the resends take a write at or above the floor, which a
        // `Truncate` did not cut, and at or above the start of the segment
        // the register knows last, which no seal put below: its resend
        // answers `Duplicate`.
        let start = self
            .register
            .segments()
            .values()
            .filter_map(|r| r.boundary().map(|b| b.next_seq.0))
            .max()
            .unwrap_or(0);
        let first = self.truth.first.max(start);
        let kept: Vec<Write> = self.writers[index]
            .sent
            .iter()
            .filter(|w| matches!(w.fence, Fence::Leader { expected_seq, .. } if expected_seq.0 >= first))
            .cloned()
            .collect();
        let from = if !kept.is_empty() && cx.rng().chance(1, 2) {
            &kept
        } else {
            &self.writers[index].sent
        };
        let Some(write) = cx.rng().pick(from).cloned() else {
            return;
        };
        self.propose(Some(index), Command::Write(write), cx);
    }

    fn multi(&mut self, cx: &mut Cx<'_, Self>) {
        let command = if cx.rng().chance(1, 2) {
            self.uuids += 1;
            Command::SetLeader {
                new: Leader::multi(LeaderUuid(self.uuids)),
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

    /// The replicas of `segment` that may go deaf: never the replica that
    /// hears the register late, since its reads are how a read meets a
    /// later segment.
    fn hearing(&self, segment: SegmentId) -> Vec<NodeId> {
        Self::set_of(segment)
            .into_iter()
            .filter(|node| self.slow != Some(*node))
            .collect()
    }

    /// One replica of the segment in force goes deaf, then the clients send
    /// two calls: an open write, which holds a slot in either mode, and a
    /// write that the cut can fall inside. Whether a replica went deaf.
    fn plant_deaf(&mut self, cx: &mut Cx<'_, Self>) -> bool {
        // Only with a leader that writes, and a client that can write.
        let can_write = self.truth.leader.is_multi()
            || (0..WRITERS).any(|i| {
                self.writers[i].uuid.is_some()
                    && !self.calls.values().any(|call| call.writer == Some(i))
            });
        if self.deaf.is_some() || self.writer_leader().is_none() || !can_write {
            return false;
        }
        let Some(in_force) = self.register.in_force().map(SegmentRecord::id) else {
            return false;
        };
        let set = self.hearing(in_force);
        let Some(deaf) = cx.rng().pick(&set).copied() else {
            return false;
        };
        self.deaf = Some(deaf);
        self.blinded.clear();
        let records = Self::records(cx);
        self.propose(
            None,
            Command::Write(Write {
                fence: Fence::Open,
                records,
            }),
            cx,
        );
        if self.truth.leader.is_multi() {
            let records = Self::records(cx);
            self.propose(
                None,
                Command::Write(Write {
                    fence: Fence::Open,
                    records,
                }),
                cx,
            );
        } else {
            self.write(cx, false);
        }
        true
    }

    /// The writes in flight, by slot, with the least end each can take:
    /// the position its fence asks for, or for an open write in a
    /// multi-writer mode the records of the open writes in flight up to
    /// it above the next position, plus its records.
    fn ends_in_flight(&self) -> Vec<(Slot, u64)> {
        let mut calls: Vec<&Call> = self.calls.values().collect();
        calls.sort_by_key(|call| call.slot);
        let mut open = self.truth.next;
        calls
            .into_iter()
            .filter_map(|call| {
                let Command::Write(Write { fence, records }) = &call.command else {
                    return None;
                };
                let count = u64::try_from(records.len()).expect("few records");
                match fence {
                    Fence::Leader { expected_seq, .. } => Some((call.slot, expected_seq.0 + count)),
                    Fence::Open if self.truth.leader.is_multi() => {
                        open += count;
                        Some((call.slot, open))
                    }
                    Fence::Open => None,
                }
            })
            .collect()
    }

    fn truncate(&mut self, cx: &mut Cx<'_, Self>) {
        // One truncate in four follows writes at once, while they are still
        // in flight, and one replica of the segment in force loses its
        // messages from just before them: it misses a slot below the write
        // that the cut can fall inside.
        let planted = cx.rng().chance(1, 4) && self.plant_deaf(cx);
        let (first, next) = (self.truth.first, self.truth.next);
        // The ends of the writes in flight: their slots lie below the slot
        // of a `Truncate` proposed now.
        let ends = self.ends_in_flight();
        let asked = ends.iter().map(|(_, end)| *end).fold(next, u64::max);
        let low = next.max(first + 1);
        let (fence, up_to, deafen) = if asked > low
            && self.truth.leader != Leader::Unset
            && (planted || cx.rng().chance(1, 2))
        {
            // The writer in force truncates at or below the position of a
            // write in flight, and one replica of the segment in force loses
            // its messages from now on. Once the write is chosen below the
            // `Truncate`, its record is at or above the floor, so the peers
            // compact to its slot, below the `Truncate`, with a blind fold;
            // the deaf replica never got the write, so it jumps there and
            // folds blind until it applies the `Truncate`.
            let fence = match self.truth.leader {
                Leader::Uuid(uuid) => Some(uuid),
                Leader::Unset | Leader::Multi(_) => None,
            };
            (fence, Seq(low + cx.rng().below(asked - low)), true)
        } else {
            let fence = cx
                .rng()
                .index(WRITERS)
                .and_then(|i| self.writers[i].uuid)
                .filter(|_| cx.rng().chance(1, 2));
            (fence, Seq(cx.rng().below(next + 2)), false)
        };
        let tokens = self.tokens;
        self.propose(None, Command::Truncate { fence, up_to }, cx);
        // Only a `Truncate` that holds a slot cuts the segment: a busy
        // leader refused it.
        let held = self.tokens > tokens && self.calls.contains_key(&self.tokens);
        if planted && !(deafen && held) {
            self.deaf = None;
        }
        if deafen
            && held
            && self.deaf.is_none()
            && let Some(in_force) = self.register.in_force().map(SegmentRecord::id)
        {
            // The slot of a write in flight that ends above the floor: the
            // peers compact to it, with a blind fold. A replica whose front
            // is below that slot lags behind the blind point, so it jumps
            // there; a replica at the slot or above pulls the slots instead.
            let cut = ends
                .iter()
                .filter(|(_, end)| *end > up_to.0)
                .map(|(slot, _)| *slot)
                .min();
            let set: Vec<NodeId> = self
                .hearing(in_force)
                .into_iter()
                .filter(|node| {
                    cut.is_some_and(|slot| {
                        self.replicas[node]
                            .role
                            .as_ref()
                            .and_then(SegmentReplica::front)
                            .is_none_or(|front| front < slot)
                    })
                })
                .collect();
            self.deaf = cx.rng().pick(&set).copied();
            self.blinded.clear();
        }
        if self.mutant == RolesMutant::TrimIntent
            && let Some(in_force) = self
                .register
                .segments()
                .values()
                .rev()
                .find(|r| r.state() == SegmentState::InForce)
        {
            let node = Self::set_of(in_force.id())[0];
            let covered: Vec<SegmentId> = self
                .register
                .segments()
                .values()
                .filter(|r| r.boundary().is_some_and(|b| b.next_seq <= up_to))
                .map(SegmentRecord::id)
                .collect();
            for segment in covered {
                self.queue.push((
                    Origin::Replica(node),
                    Change::Trim {
                        segment,
                        first_seq: up_to,
                    },
                ));
            }
        }
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
                sometimes!(view.leader.is_multi(), reach::TO_MULTI);
            }
            (Answer::Trimmed(view), _) => sometimes!(view.first_seq.0 > 0, reach::TRIMMED),
            (Answer::Written { seq, count }, Command::Write(write)) => {
                if let Some(index) = call.writer {
                    let writer = &mut self.writers[index];
                    writer.next = writer.next.max(seq.0 + count);
                    writer.sent.push(write.clone());
                    self.echo = Some(index);
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

    /// A client reads at one replica that runs, from a position at most two
    /// past the whole log's next.
    fn read(&mut self, cx: &mut Cx<'_, Self>) {
        // After the faults stop, reads go on until one is served.
        if cx.phase() == Phase::Settle && self.settled_served {
            return;
        }
        let nodes: Vec<NodeId> = self
            .replicas
            .iter()
            .filter(|(_, r)| r.role.is_some())
            .map(|(n, _)| *n)
            .collect();
        let Some(pick) = cx.rng().index(nodes.len()) else {
            return;
        };
        // Half of the reads go to the replica that hears the register late,
        // where a read meets a later segment; three in four once its
        // segment is sealed, where a stale read shows.
        let late = self.slow.filter(|slow| nodes.contains(slow)).map(|slow| {
            let sealed = self
                .register
                .segment(Self::segment_of_replica(slow))
                .and_then(SegmentRecord::end)
                .is_some();
            (slow, if sealed { 3 } else { 2 })
        });
        let node = match late {
            Some((slow, odds)) if cx.rng().chance(odds, 4) => slow,
            _ => nodes[pick],
        };
        // Half of the reads are at the tail, where a stale read shows.
        let from = if cx.rng().chance(1, 2) {
            self.truth.next
        } else {
            cx.rng().below(self.truth.next + 2)
        };
        let limit = usize::try_from(cx.rng().below(4) + 1).expect("small");
        if !self.waiting.is_empty() && cx.rng().chance(1, 2) {
            let (node, from, limit, floor) = self.waiting.remove(0);
            self.route_read(node, from, limit, floor, cx);
        } else {
            let floor = self.truth.first;
            self.route_read(node, from, limit, floor, cx);
        }
    }

    /// A replica of a segment older than the one in force folds only up to
    /// its seal: the reader first takes the floor from a replica of the
    /// segment in force, with a read at its tail (§J3.2).
    fn route_read(
        &mut self,
        node: NodeId,
        from: u64,
        limit: usize,
        floor: u64,
        cx: &mut Cx<'_, Self>,
    ) {
        if self.replicas[&node].role.is_none() {
            return;
        }
        let segment = Self::segment_of_replica(node);
        let in_force = self
            .register
            .segments()
            .values()
            .rev()
            .find(|r| r.state() == SegmentState::InForce)
            .map(SegmentRecord::id)
            .filter(|f| *f > segment && self.mutant != RolesMutant::FloorFromHolder);
        let Some(in_force) = in_force else {
            self.start_read(node, from, limit, floor, None, cx);
            return;
        };
        let set: Vec<NodeId> = Self::set_of(in_force)
            .into_iter()
            .filter(|n| self.replicas[n].role.is_some())
            .collect();
        match cx.rng().index(set.len()) {
            Some(i) => {
                let next = self.truth.next;
                self.start_read(set[i], next, 1, floor, Some((node, from, limit)), cx);
            }
            None => self.waiting.push((node, from, limit, floor)),
        }
    }

    /// Send the read to `node`, and judge its answer when it comes.
    fn start_read(
        &mut self,
        node: NodeId,
        from: u64,
        limit: usize,
        floor: u64,
        then: Option<(NodeId, u64, usize)>,
        cx: &mut Cx<'_, Self>,
    ) {
        let ids = self.read_ids.entry(node).or_default();
        *ids += 1;
        let read = *ids;
        let need = self.states.len() - 1;
        self.reads.insert(
            (node, read),
            OpenRead {
                node,
                from,
                limit,
                need,
                floor,
                then,
                settled: cx.phase() == Phase::Settle,
            },
        );
        let role = self
            .replicas
            .get_mut(&node)
            .and_then(|r| r.role.as_mut())
            .expect("a replica that runs");
        let effects = role.read(read, Seq(from), limit);
        self.replica_effects(node, effects, cx);
    }

    /// The oracle [`READ`] on the answer of `read`.
    fn judge_read(&mut self, at: NodeId, read: u64, answer: &ReadAnswer, cx: &mut Cx<'_, Self>) {
        let Some(OpenRead {
            node,
            from,
            limit,
            need,
            floor,
            then,
            settled,
        }) = self.reads.remove(&(at, read))
        else {
            return;
        };
        self.settled_served |= settled && matches!(answer, ReadAnswer::Page { .. });
        self.judge_answer(read, node, (from, limit), (need, floor), answer);
        // The floor read gave the floor of the segment in force: the reader
        // asks the older segment only at or above it.
        let Some((target, from, limit)) = then else {
            return;
        };
        let view = match answer {
            ReadAnswer::Page { view, .. }
            | ReadAnswer::Below(view)
            | ReadAnswer::Truncated(view) => *view,
            ReadAnswer::Superseded(_) | ReadAnswer::Unserved => {
                self.waiting.push((target, from, limit, floor));
                return;
            }
        };
        if from < view.first_seq.0 {
            let answer = ReadAnswer::Truncated(view);
            self.judge_answer(read, target, (from, limit), (need, floor), &answer);
            sometimes!(true, reach::FLOOR_REFUSED);
        } else if self.replicas[&target].role.is_some() {
            self.start_read(target, from, limit, floor, None, cx);
        }
    }

    /// The oracles [`READ`] and [`FLOOR`] on one answer.
    /// The read asked `from` and `limit`; `need` is the length of the
    /// chosen prefix when it started, and `floor` the floor of the whole
    /// log when its reader started.
    fn judge_answer(
        &mut self,
        read: u64,
        node: NodeId,
        (from, limit): (u64, usize),
        (need, floor): (usize, u64),
        answer: &ReadAnswer,
    ) {
        let segment = Self::segment_of_replica(node);
        match answer {
            ReadAnswer::Page {
                records,
                view,
                sealed,
            } => {
                for (seq, record) in records {
                    assert!(
                        self.records.get(&seq.0) == Some(record),
                        "{READ}: read {read} at {node:?} served {record:?} at {seq:?}, the whole log has {:?}",
                        self.records.get(&seq.0)
                    );
                }
                assert!(
                    records.first().is_none_or(|(seq, _)| seq.0 >= floor),
                    "{FLOOR}: read {read} at {node:?} served from {:?}, the floor was {floor} when its reader started",
                    records.first().map(|(seq, _)| seq)
                );
                assert!(
                    records.first().is_none_or(|(seq, _)| seq.0 == from),
                    "{READ}: read {read} from {from} served from {:?}",
                    records.first()
                );
                // A short page ends where the segment's records end: at the
                // view's next position, or at the end of a sealed segment.
                if records.len() < limit {
                    let end = records.last().map_or(from, |(seq, _)| seq.0 + 1);
                    let next = if *sealed {
                        self.register
                            .segment(segment)
                            .and_then(SegmentRecord::boundary)
                            .map_or(view.next_seq.0, |b| b.next_seq.0)
                    } else {
                        view.next_seq.0
                    };
                    assert!(
                        end == next || (records.is_empty() && from >= next),
                        "{READ}: read {read} from {from} at {node:?} gave a short page to {end}, its records end at {next}"
                    );
                }
                if !sealed && records.len() < limit {
                    // The view of a page at the tail is the fold of the
                    // whole log at a slot at or past the chosen prefix
                    // when the read started.
                    let fresh = self.states[need..].contains(&(
                        view.leader,
                        view.next_seq.0,
                        view.first_seq.0,
                    ));
                    assert!(
                        fresh,
                        "{READ}: read {read} of {segment:?} at {node:?} served {view:?}, the whole log was at slot {need}, {:?}, when it started",
                        self.states[need]
                    );
                    sometimes!(!records.is_empty(), reach::READ_OPEN);
                }
                sometimes!(*sealed, reach::READ_SEALED);
            }
            ReadAnswer::Truncated(_) => assert!(
                from < self.truth.first,
                "{READ}: read {read} from {from} answered Truncated, the floor is {}",
                self.truth.first
            ),
            ReadAnswer::Below(view) => {
                // The first position of the segment: the next of the
                // boundary state of the segment before, 0 for the first.
                let start = segment
                    .0
                    .checked_sub(1)
                    .and_then(|before| self.register.segment(SegmentId(before)))
                    .map_or(Some(0), |before| before.boundary().map(|b| b.next_seq.0));
                assert!(
                    start.is_some_and(|start| from < start),
                    "{READ}: read {read} from {from} at {segment:?} answered Below with {view:?}, the segment starts at {start:?}"
                );
            }
            ReadAnswer::Superseded(bound) => {
                assert!(
                    *bound > segment && self.register.segment(*bound).is_some(),
                    "{READ}: read {read} of {segment:?} superseded by {bound:?}"
                );
                sometimes!(true, reach::READ_SUPERSEDED);
            }
            ReadAnswer::Unserved => {}
        }
    }

    fn resolve(&mut self, cx: &mut Cx<'_, Self>) {
        // With no `Below` open, the client asks again for one it settled, so
        // a `Resolve` meets the replicas as they are now (a blind fold below
        // the trim floor, a dropped copy).
        let again = self.belows.is_empty();
        let Some(pick) = cx.rng().index(if again {
            self.settled_belows.len()
        } else {
            self.belows.len()
        }) else {
            return;
        };
        let (write, slot) = if again {
            self.settled_belows[pick].clone()
        } else {
            self.belows[pick].clone()
        };
        let Fence::Leader { uuid, expected_seq } = write.fence else {
            unreachable!("only a fenced write is answered Below");
        };
        let in_force = self.register.in_force().map(SegmentRecord::id);
        // The client asks every replica, and takes the answer of one that
        // covers the write, drawn at random: a replica that cannot answer
        // (no base yet, a blind fold below the trim floor, a dropped copy)
        // is asked too. A replica of the segment in force covers a position
        // below its `first_seq` only (`Truncated`), which is the one answer
        // left once the old segment is trimmed and its copies dropped.
        let mut covered = Vec::new();
        for (node, replica) in &self.replicas {
            let segment = Self::segment_of_replica(*node);
            let asked = self.mutant != RolesMutant::ResolveInForce || Some(segment) == in_force;
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
                covered.push((found, *node));
            }
        }
        if self.mutant == RolesMutant::ResolveInForce {
            for (answer, _) in &mut covered {
                if *answer == ResolveAnswer::NotCovered {
                    *answer = ResolveAnswer::NotWritten;
                }
            }
        }
        // Every answer that covers the write is judged, not only the one
        // that the client takes.
        let verdict = self.verdicts[usize::try_from(slot.0).expect("small")];
        for (answer, node) in &covered {
            let truth = match answer {
                ResolveAnswer::Written { .. } => verdict == "dup",
                ResolveAnswer::NotWritten => verdict == "taken",
                ResolveAnswer::Truncated => expected_seq.0 < self.truth.first,
                ResolveAnswer::NotCovered => false,
            };
            assert!(
                truth,
                "{RESOLVE}: {write:?} resolved {answer:?} at replica {node:?}, the whole log says {verdict}"
            );
        }
        let Some((answer, by)) = cx.rng().pick(&covered).copied() else {
            return;
        };
        if again {
            return;
        }
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
            r: by.0,
            res,
        });
        self.settled_belows.push(self.belows.remove(pick));
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
