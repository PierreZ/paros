//! The whole journal on the in-memory IO of `paros-journal-io` (#1445
//! (journal-sim: the whole journal on the in-memory IO),
//! `docs/journal/07-verification.md` §J7.4).
//!
//! **The deployment.** Nine machines in three zones (`a`: 1 to 3, `b`: 4
//! to 6, `c`: 7 to 9). Each machine is one process: one generic [`Driver`]
//! with its disk, its transport through the walk's mailbox, the shared
//! clock and the shared register ([`MemRegister`]). Each process serves the
//! roles of **two journals** at once: the acceptors, the segment replicas
//! and the leaders of each. Journal 1 starts with its acceptors on 1, 4 and
//! 7 and its replicas on 2, 5 and 8; journal 2 with its acceptors on 2, 5
//! and 8 and its replicas on 3, 6 and 9: one column of three, one member in
//! each zone (`F = 1`, the zone rule). A role is a node of its journal, and
//! node `n` runs on machine `n % 10`: each new role on a machine takes the
//! next free node of that machine (`n + 10`).
//!
//! **The control.** Each journal has two grid controllers on two machines,
//! each run by a `paros_journal_control::ControlDriver`. Before a step of
//! the controllers, every machine that is up publishes its report to them
//! (a beat, and the segment of each leader on it that writes), so a machine
//! not heard for a tick is suspect and down after [`DOWN_AFTER`] ticks. The
//! walk is the driver's two ports: the register (a request or its reply may
//! be lost), and the cell: the zones, the candidates of the host's placement
//! (the next free node of each machine that is up and serves no member of
//! the segment in force), the matchmakers (three machines, one in each
//! zone) and one token of the rate limiter that the two journals share.
//! Journal 1 starts with a holder that runs on no machine, journal 2 with no
//! holder. The walk delivers the driver's copy and abandon asks to the new
//! replicas, and each complete copy back to the drivers. The
//! walk is the host's placement: it
//! serves each acceptor and replica that the register names on its machine,
//! and campaigns a leader of the newest segment when none writes. The
//! leader retires a sealed segment once `F + 1` replicas of its set hold it
//! on their disk (§J3.1), as in [`crate::roles`].
//!
//! **The faults.** The walk crashes and restarts machines; a zone goes out
//! (every machine of it crashes at once); a disk fails a commit inside the
//! contract (before or after its write); the network loses, repeats and
//! reorders messages. A crash drops the driver and keeps the disk; a
//! restart is a new driver on the same disk, and placement serves its roles
//! again. A machine that stays down long enough is down for the controller,
//! which heals the grid and the replica set with a reshape, or holds and
//! alerts when no placement meets the zone rule (a zone out).
//!
//! **The clients.** One writer for each journal claims it with `SetLeader`,
//! writes one record at each position with `expected_seq`, writes at an old
//! position now and then, and sends a call again when its answer is lost,
//! `Unknown` or `NotLeader`. A resend across a seal answered `Below` is
//! settled with `Resolve` at a replica of an older segment.
//!
//! **The oracles** share no code with the roles, the driver or `decide`:
//!
//! - [`AGREE`]: no two `Commit`s on the wire name two commands at one slot
//!   of a journal.
//! - [`SEALED`]: no `Commit` of a segment is at or above the end of that
//!   segment once it is sealed.
//! - [`FLIGHT`]: an acceptor's `Accepted` or `Promise` leaves only once the
//!   vote or the promise it reports is on its disk.
//! - [`ANSWER`]: each answer is the whole log's ([`Truth`]) at a slot of the
//!   command, or `Below` for a resend across a seal; each `Resolve` gives the
//!   fate that the whole log gives.
//! - [`ONCE`]: no record is written twice in the whole log (exactly-once
//!   with `expected_seq`).
//! - [`ACKED`]: each acknowledged write holds its position in the whole log
//!   (no lost acked write).
//! - [`ALERTS`]: each alert of the controller is true of the machines as
//!   they are, not as the view says.
//! - [`HEAL_HELD`]: each node that a done heal added to the set of a
//!   retired segment holds every slot of it on its disk while the set
//!   names it, but the slots below its compaction point, each of which
//!   wrote only records below the trim floor that the log decided.
//!
//! **The liveness claim** (goal 2, the heal bound: the settle budget of the
//! walk): once the faults stop, every machine is up again; each journal has
//! a controller that holds the register's term with no call in flight,
//! every older segment is retired, a leader of the newest segment writes,
//! every call is answered and judged, every `Below` is resolved, and every
//! replica of the newest segment applied the whole log.
//!
//! **The heal of a retired segment** (#1314): when a member of a retired
//! segment's set is down for the controller, the controller heals the set.
//! The walk asks the new replica's driver to copy ([`Driver::join`]) and
//! to drop an abandoned copy ([`Driver::abandon`]); each driver reads the
//! register first, so it judges the ask on a value done after the ask was
//! sent. In the chaos phase an ask may be lost, and the network may hold
//! it and give it again later ([`reach::LATE_ASK`]). A complete copy
//! reaches every live controller of the journal
//! ([`reach::COPIED`]), and the heal's compare-and-set runs on the register
//! like a reshape ([`reach::REREPLICATED`]).
//!
//! **A machine lost for good** ([`WholeAction::Lose`]): once in a run, a
//! machine that is not a matchmaker goes down and never comes back. The
//! liveness claim then asks for the heal too: no grid or replica set of the
//! segment in force, and no replica set of a retired segment, names a node
//! of the lost machine ([`reach::LOST_HEALED`]). A matchmaker lost for good
//! needs a handover of the matchmaker set, which this walk does not run.
//! A zone lost for good breaks the contract (`F = 1`): no placement meets
//! the zone rule, and the controller holds and alerts.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::rc::Rc;

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};

use paros_journal::register::{Ballot, Change, MatchmakerGeneration, RegisterValue};
use paros_journal::roles::CopyId;
use paros_journal::roles::{Campaign, LeaderPhase, Message, PROMISE_BATCH};
use paros_journal::segment::{OpeningCall, SegmentRecord, SegmentState};
use paros_journal::{
    Answer, Command, Fence, Grid, Leader, LeaderUuid, NodeId, RELEASE, Record, Reply,
    ResolveAnswer, SegmentId, Seq, Slot, Write,
};
use paros_journal_control::{
    Action, Alert, Ask, Cell, ControlConfig, ControlDriver, ControlRegister, FailureDomain,
    MatchmakerId, MatchmakerSet, Outcome, Polled, ProcessId, Report, Tunables, Windows,
};
use paros_journal_io::mem::{ManualClock, MemRegister};
use paros_journal_io::storage::Storage as _;
use paros_journal_io::{
    Address, Assignment, CallId, CallOutcome, Clock, Driver, Envelope, HostConfig, JournalId,
    Notice, Op, Register,
};

use crate::backend::{Disk, Wire};
use crate::oracle::{Truth, as_written, label};

/// Oracle: one command is chosen at each slot of a journal.
pub const AGREE: &str = "whole journal: no two Commits name two commands at one slot";
/// Oracle: a sealed segment chooses nothing at or above its end.
pub const SEALED: &str = "whole journal: no Commit of a sealed segment is at or above its end";
/// Oracle: the flight rule.
pub const FLIGHT: &str =
    "whole journal: an Accepted or a Promise leaves only once its state is on the disk";
/// Oracle: each answer and each `Resolve` is the whole log's.
pub const ANSWER: &str = "whole journal: each answer is the whole log's at a slot of the command";
/// Oracle: exactly-once.
pub const ONCE: &str = "whole journal: no record is written twice in the whole log";
/// Oracle: no lost acked write.
pub const ACKED: &str = "whole journal: each acked write holds its position in the whole log";
/// Oracle: each alert of the controller is true.
pub const ALERTS: &str = "whole journal: each controller alert is true of the machines";
/// Oracle: the journals share one token, so at most one reshapes or heals
/// at a time.
pub const BOUNDED_IN_FLIGHT: &str =
    "whole journal: at most one journal reshapes or heals at a time under the shared token";
/// Oracle: a node that a heal added to a retired set holds the whole
/// segment on its disk while the set names it.
pub const HEAL_HELD: &str =
    "whole journal: a node a heal added holds the whole segment on its disk";

/// The coverage sites of the whole journal walk.
pub mod reach {
    /// One commit held the batches of two journals.
    pub const TWO_JOURNALS: &str = "whole journal: one commit held the batches of two journals";
    /// A commit failed and stopped its roles.
    pub const FAILED: &str = "whole journal: a failed commit stopped its roles";
    /// An acceptor read a vote back at a restart.
    pub const RESTARTED: &str = "whole journal: an acceptor read its votes back at a restart";
    /// Every machine of a zone was down at once.
    pub const ZONE_OUT: &str = "whole journal: every machine of a zone was down at once";
    /// A controller took the term from another controller.
    pub const TAKEOVER: &str = "whole journal: a controller took the term from another";
    /// A heal reshape of a controller was done.
    pub const HEALED: &str = "whole journal: a controller's heal reshape was done";
    /// A new replica reported its complete copy.
    pub const COPIED: &str = "whole journal: a new replica reported its complete copy";
    /// A copy or abandon ask arrived again, late.
    pub const LATE_ASK: &str = "whole journal: a copy or abandon ask arrived again, late";
    /// A heal of a retired segment's replica set was done.
    pub const REREPLICATED: &str = "whole journal: a heal of a retired segment's set was done";
    /// A heal reshape was done with its reply lost.
    pub const HEAL_LOST: &str = "whole journal: a heal was done and its reply was lost";
    /// A first leader sealed the segment before.
    pub const SEALED: &str = "whole journal: a first leader sealed the segment before";
    /// A sealed segment retired.
    pub const RETIRED: &str = "whole journal: a sealed segment retired";
    /// A write was chosen in a segment that a heal opened.
    pub const HEALED_WRITE: &str = "whole journal: a write chosen in a segment a heal opened";
    /// A node that a heal added holds a compacted copy of its segment.
    pub const HEALED_TRIM: &str = "whole journal: a healed replica holds a compacted copy";
    /// The controller held a heal and alerted that no placement exists.
    pub const UNPLACEABLE: &str = "whole journal: the controller alerted no placement";
    /// A controller that runs read that another took its term, and stopped.
    pub const DEPOSED: &str = "whole journal: a running controller saw it was deposed";
    /// A heal waited for the token that the journals share.
    pub const NO_TOKEN: &str = "whole journal: a heal waited for the shared token";
    /// The controller asked to re-replicate a sealed segment.
    pub const REREPLICATE: &str = "whole journal: the controller asked a re-replication";
    /// A call sent again was answered `Duplicate`.
    pub const DUPLICATE: &str = "whole journal: a call sent again was answered Duplicate";
    /// A call was answered `Unknown`.
    pub const UNKNOWN: &str = "whole journal: a call was answered Unknown";
    /// A resend across a seal was answered `Below`.
    pub const BELOW: &str = "whole journal: a resend across a seal was answered Below";
    /// A `Resolve` gave a fate.
    pub const RESOLVED: &str = "whole journal: a Resolve at an older replica gave a fate";
    /// A machine was lost for good.
    pub const LOST: &str = "whole journal: a machine was lost for good";
    /// A heal replaced a replica of a retired segment on the lost machine.
    pub const LOST_HEALED: &str = "whole journal: a heal replaced a replica on a lost machine";

    /// Every site of the walk.
    pub const ALL: &[&str] = &[
        TWO_JOURNALS,
        FAILED,
        RESTARTED,
        ZONE_OUT,
        TAKEOVER,
        DEPOSED,
        HEALED,
        HEAL_LOST,
        SEALED,
        RETIRED,
        HEALED_WRITE,
        HEALED_TRIM,
        UNPLACEABLE,
        NO_TOKEN,
        REREPLICATE,
        COPIED,
        REREPLICATED,
        LATE_ASK,
        DUPLICATE,
        UNKNOWN,
        BELOW,
        RESOLVED,
        LOST,
        LOST_HEALED,
    ];
}

/// A bug planted in the walk's backends, client, host signals or
/// placement, never in the code under test.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WholeMutant {
    /// No bug.
    #[default]
    None,
    /// The disk says a commit is done and writes nothing. [`FLIGHT`]
    /// catches it.
    LyingDisk,
    /// A machine restarts with an empty disk. [`AGREE`] catches it.
    WipeDisk,
    /// The client takes a `Taken` as written. [`ANSWER`] catches it.
    TakenAsWritten,
    /// The client sends an acked write again, at the next position.
    /// [`ONCE`] catches it.
    WriteTwice,
    /// The client keeps an acked write one position too high. [`ACKED`]
    /// catches it.
    AckOffByOne,
    /// The wire moves a `Commit` of a sealed segment to the segment's end.
    /// [`SEALED`] catches it.
    CommitAtEnd,
    /// The walk drops the reports of a machine once it was ever down, so
    /// the controllers keep it down. [`ALERTS`] catches it.
    StaleLiveness,
    /// The cell gives a token to every journal. [`BOUNDED_IN_FLIGHT`]
    /// catches it.
    NoToken,
    /// The walk reports a new replica's copy complete as soon as it asks
    /// the copy. [`HEAL_HELD`] catches it.
    EarlyCopied,
}

/// The weights of the walk's crash and restart steps: a machine comes
/// back faster than another goes down, so a journal writes between faults.
const CRASH: (u32, u32) = (1, 4);
const JOURNALS: [JournalId; 2] = [JournalId(1), JournalId(2)];
const MACHINES: [u64; 9] = [1, 2, 3, 4, 5, 6, 7, 8, 9];
/// The machines of the matchmakers, one in each zone.
const MATCHMAKERS: [u64; 3] = [1, 4, 7];
const TICK: u64 = 1_000;
/// A machine down for this many ticks is down for the controller; before,
/// it is suspect.
pub const DOWN_AFTER: u64 = 4;
const CONFIG: HostConfig = HostConfig {
    tick: TICK,
    resend_page: 4,
};
const TUNABLES: Tunables = Tunables {
    stale_after: 10 * TICK,
    min_reshape_interval: 6 * TICK,
    stall_window: 80 * TICK,
};
/// The numbers of the controllers' drivers: a machine not heard for a tick
/// is suspect, and down after [`DOWN_AFTER`] ticks.
const CONTROL: ControlConfig = ControlConfig {
    tunables: TUNABLES,
    windows: Windows {
        suspect_after: TICK,
        down_after: DOWN_AFTER * TICK,
    },
};
/// The nodes of the leaders: `LEADERS + 10 * round + machine`.
const LEADERS: u64 = 100_000;
/// The nodes of the controllers: `CONTROLLERS + machine`.
const CONTROLLERS: u64 = 1_000_000;
/// A leader that does not write after this many ticks is replaced.
const STUCK: u64 = 30;
/// The uuid of each journal's writer.
const A: LeaderUuid = LeaderUuid(0xA);

/// The machine that serves `node`.
fn machine_of(node: NodeId) -> u64 {
    node.0 % 10
}

/// The zone of `machine`.
fn zone_of(machine: u64) -> u64 {
    (machine - 1) / 3
}

fn domain(machine: u64) -> FailureDomain {
    let az = ["a", "b", "c"][usize::try_from(zone_of(machine)).expect("three zones")];
    FailureDomain::new("r", az)
}

/// [`reach::TWO_JOURNALS`] on each commit.
fn two_journals(ops: &[Op]) {
    let journals: BTreeSet<JournalId> = ops.iter().map(|op| op.key.address.journal).collect();
    sometimes!(journals.len() >= 2, reach::TWO_JOURNALS);
}

/// The truth over a chosen prefix: for each slot the position before it
/// and the verdict; the payload at each position written; the fold.
type Judged = (Vec<(u64, &'static str)>, BTreeMap<u64, Vec<u8>>, Truth);

type Proc = Driver<Disk, Wire, ManualClock, MemRegister, ()>;

/// One machine: its driver while it is up, its disk and its placed roles
/// always.
struct Machine {
    driver: Option<Proc>,
    disk: Disk,
    inbox: Rc<RefCell<VecDeque<Envelope>>>,
    outbox: Rc<RefCell<Vec<Envelope>>>,
    /// The acceptors and replicas that placement put here.
    placed: Vec<Assignment>,
    /// It was ever down (for the mutant `StaleLiveness`).
    ever_down: bool,
}

/// One grid controller: its machine, and its driver while the machine is
/// up.
struct Ctl {
    machine: u64,
    driver: Option<ControlDriver>,
}

impl Ctl {
    fn node(&self) -> NodeId {
        NodeId(CONTROLLERS + self.machine)
    }
}

/// The writer's call in flight.
#[derive(Clone, Debug)]
struct Call {
    machine: u64,
    id: CallId,
    command: Command,
    resent: bool,
}

/// One journal, as the walk sees it.
struct Journal {
    id: JournalId,
    round: u64,
    /// The segment each leader is bound to, by its node.
    bound: BTreeMap<NodeId, SegmentId>,
    /// When each leader started.
    started: BTreeMap<NodeId, u64>,
    /// Every command a `Commit` on the wire named, by slot.
    chosen: BTreeMap<Slot, Command>,
    /// When the walk first saw each segment, and each seal.
    opened_at: BTreeMap<SegmentId, u64>,
    sealed_at: BTreeMap<SegmentId, u64>,
    /// The segments that a controller's reshape opened.
    healed: BTreeSet<SegmentId>,
    /// The nodes that a done heal added to the set of a retired segment.
    added: BTreeSet<(SegmentId, NodeId)>,
    /// The last time each machine was up when the walk published the
    /// reports to the journal's controllers: the truth of [`ALERTS`].
    heard: BTreeMap<u64, u64>,
    ctls: Vec<Ctl>,
    claimed: bool,
    next: u64,
    texts: u64,
    call: Option<Call>,
    again: Option<Command>,
    /// The answers not judged yet are from `judged` on.
    answers: Vec<(Command, Answer)>,
    judged: usize,
    /// The acked writes: the position and the payload.
    acked: Vec<(u64, Vec<u8>)>,
    /// The writes answered `Below`, to resolve.
    belows: Vec<Write>,
}

/// The whole journal walk.
pub struct WholeModel {
    mutant: WholeMutant,
    clock: ManualClock,
    register: MemRegister,
    machines: BTreeMap<u64, Machine>,
    journals: Vec<Journal>,
    /// The copy and abandon asks that the network still holds: each may
    /// arrive again, late ([`WholeAction::Late`]).
    late: Vec<(usize, Ask)>,
    /// The journal that holds the one token of the cell's rate limiter.
    token: Option<usize>,
    /// The controllers whose reports are lost until a time, by journal and
    /// controller.
    muted: BTreeMap<(usize, usize), u64>,
    /// The machine lost for good, once one is.
    lost: Option<u64>,
}

/// The walk with `mutant` planted.
#[must_use]
pub fn walk(mutant: WholeMutant, seeds: u64, steps: u64) -> Walk<WholeModel> {
    Walk::<WholeModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .env_prefix(None)
        .crash_weights(CRASH.0, CRASH.1)
        .max_down(3)
        .deliveries_per_step(12, 12)
        .model(move |cx| WholeModel::build(mutant, cx))
}

/// The moves of the walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WholeAction {
    /// A machine runs one round of its driver.
    Poll,
    /// The clock moves one tick.
    Tick,
    /// A journal gets a new leader, when none writes.
    Campaign,
    /// A writer calls.
    Write,
    /// A controller takes one step.
    Control,
    /// A writer resolves a `Below`.
    Resolve,
    /// The next commit of a machine fails.
    Fault,
    /// Every machine of a zone crashes.
    ZoneOut,
    /// A copy or abandon ask that the network held arrives again, late.
    Late,
    /// The reports of one controller are lost for a while: the other
    /// controller of its journal takes the term from a holder that runs.
    Mute,
    /// A machine that is not a matchmaker is lost for good.
    Lose,
}

impl Model for WholeModel {
    type Msg = Envelope;
    type Node = u64;
    type Action = WholeAction;
    type Event = ();

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(WholeMutant::None, cx)
    }

    fn actions() -> &'static [WholeAction] {
        &[
            WholeAction::Poll,
            WholeAction::Tick,
            WholeAction::Campaign,
            WholeAction::Write,
            WholeAction::Control,
            WholeAction::Resolve,
            WholeAction::Fault,
            WholeAction::ZoneOut,
            WholeAction::Late,
            WholeAction::Mute,
            WholeAction::Lose,
        ]
    }

    fn weight(action: WholeAction, phase: Phase) -> u32 {
        match (action, phase) {
            (
                WholeAction::Fault
                | WholeAction::ZoneOut
                | WholeAction::Late
                | WholeAction::Mute
                | WholeAction::Lose,
                Phase::Chaos,
            ) => 1,
            (
                WholeAction::Fault
                | WholeAction::ZoneOut
                | WholeAction::Late
                | WholeAction::Mute
                | WholeAction::Lose,
                _,
            ) => 0,
            (_, Phase::Settle) => 1,
            (WholeAction::Poll, _) => 120,
            (WholeAction::Tick | WholeAction::Campaign, _) => 8,
            (WholeAction::Write, _) => 10,
            (WholeAction::Control, _) => 4,
            (WholeAction::Resolve, _) => 2,
        }
    }

    fn act(&mut self, action: WholeAction, cx: &mut Cx<'_, Self>) {
        match action {
            WholeAction::Poll => self.poll(cx),
            WholeAction::Tick => self.clock.advance(TICK),
            WholeAction::Campaign => self.each(cx, Self::campaign),
            WholeAction::Write => self.each(cx, Self::write),
            WholeAction::Control => self.each(cx, Self::control),
            WholeAction::Resolve => self.each(cx, Self::resolve),
            WholeAction::Fault => self.fault(cx),
            WholeAction::ZoneOut => self.zone_out(cx),
            WholeAction::Late => self.late(cx),
            WholeAction::Mute => self.mute(cx),
            WholeAction::Lose => self.lose(cx),
        }
        self.place();
        self.judge_heals();
    }

    fn deliver(&mut self, envelope: Envelope, _cx: &mut Cx<'_, Self>) {
        let machine = &self.machines[&machine_of(envelope.to)];
        if machine.driver.is_some() {
            machine.inbox.borrow_mut().push_back(envelope);
        }
    }

    fn nodes(&self) -> Vec<u64> {
        MACHINES.to_vec()
    }

    fn is_up(&self, node: &u64) -> bool {
        self.machines[node].driver.is_some()
    }

    fn crash(&mut self, node: &u64, _cx: &mut Cx<'_, Self>) {
        self.down(*node);
    }

    fn restart(&mut self, node: &u64, _cx: &mut Cx<'_, Self>) {
        if self.machines[node].driver.is_some() || self.lost == Some(*node) {
            return;
        }
        if self.mutant == WholeMutant::WipeDisk {
            self.machines[node].disk.wipe();
        }
        self.start(*node);
        let driver = self.machines[node].driver.as_ref().expect("started");
        let voted = driver
            .host()
            .addresses()
            .filter_map(|a| driver.host().acceptor(a))
            .any(|a| a.vote(Slot(0)).is_some());
        sometimes!(voted, reach::RESTARTED);
        let now = self.now();
        for journal in &mut self.journals {
            for ctl in &mut journal.ctls {
                if ctl.machine == *node {
                    ctl.driver = Some(ControlDriver::new(
                        ctl.node(),
                        ProcessId(ctl.node().0),
                        CONTROL,
                        now,
                    ));
                }
            }
        }
    }

    fn check(&mut self) {
        for j in 0..self.journals.len() {
            self.judge(j);
        }
    }

    fn converged(&self) -> Result<(), String> {
        if let Some(m) = MACHINES
            .iter()
            .find(|m| self.machines[m].driver.is_none() && self.lost != Some(**m))
        {
            return Err(format!("machine {m} is down"));
        }
        for (j, journal) in self.journals.iter().enumerate() {
            self.converged_one(j, journal)
                .map_err(|why| format!("journal {}: {why}", journal.id.0))?;
        }
        Ok(())
    }

    fn finish(&mut self) {
        for j in 0..self.journals.len() {
            self.judge(j);
            let journal = &self.journals[j];
            assert!(
                journal.judged == journal.answers.len(),
                "every answer is judged at the end"
            );
        }
    }

    fn describe(&self) -> String {
        let mut text = String::new();
        for journal in &self.journals {
            let value = self.register.value(journal.id).expect("a register");
            let _ = writeln!(
                text,
                "journal {}: chosen to {:?}, term {:?}, segments {:?}, {} answers judged of {}, \
                 call {:?}, again {:?}, belows {}",
                journal.id.0,
                frontier(&journal.chosen),
                value.term(),
                value
                    .segments()
                    .values()
                    .map(|r| (
                        r.id().0,
                        r.state(),
                        r.grid().columns().to_vec(),
                        r.replicas().to_vec()
                    ))
                    .collect::<Vec<_>>(),
                journal.judged,
                journal.answers.len(),
                journal.call,
                journal.again,
                journal.belows.len()
            );
        }
        let down: Vec<u64> = MACHINES
            .into_iter()
            .filter(|m| self.machines[m].driver.is_none())
            .collect();
        let _ = write!(text, "machines down: {down:?}");
        text
    }
}

/// The first slot that no `Commit` named yet.
fn frontier(chosen: &BTreeMap<Slot, Command>) -> Slot {
    let mut slot = Slot(0);
    while chosen.contains_key(&slot) {
        slot = Slot(slot.0 + 1);
    }
    slot
}

/// The payload of a one-record write.
fn payload(write: &Write) -> Vec<u8> {
    write
        .records
        .iter()
        .flat_map(|r| r.payload.clone())
        .collect()
}

impl WholeModel {
    /// The walk with `mutant` planted: segment 1 of each journal open on
    /// three zones, every machine up.
    ///
    /// # Panics
    ///
    /// When a first segment cannot open.
    pub fn build(mutant: WholeMutant, _cx: &mut Cx<'_, Self>) -> Self {
        let register = MemRegister::new();
        let mut journals = Vec::new();
        for (i, id) in JOURNALS.into_iter().enumerate() {
            let shift = u64::try_from(i).expect("two journals");
            let acceptors: Vec<NodeId> = [1, 4, 7].map(|m| NodeId(m + shift)).to_vec();
            let replicas: Vec<NodeId> = [2, 5, 8].map(|m| NodeId(m + shift)).to_vec();
            // Journal 1 was created by a creator that holds the term and
            // is gone (node 0 runs on no machine): its controllers take the
            // term once its process is down. Journal 2 was created with no
            // election: its controllers find no holder.
            let value = if i == 0 {
                RegisterValue::new()
                    .apply(&Change::Elect { holder: NodeId(0) })
                    .expect("an election")
            } else {
                RegisterValue::new()
            };
            let value = value
                .apply(&Change::Reshape {
                    opening: OpeningCall {
                        term: value.term().number,
                        expected: None,
                        grid: Grid::new(1, vec![acceptors]).expect("a grid"),
                        replicas,
                    },
                    release: RELEASE,
                })
                .expect("segment 1 opens");
            register.create(id, value);
            let ctls = [3 + 3 * shift, 6 + 3 * shift]
                .map(|m| (m - 1) % 9 + 1)
                .map(|machine| Ctl {
                    machine,
                    driver: Some(ControlDriver::new(
                        NodeId(CONTROLLERS + machine),
                        ProcessId(CONTROLLERS + machine),
                        CONTROL,
                        0,
                    )),
                })
                .into_iter()
                .collect();
            journals.push(Journal {
                id,
                round: 0,
                bound: BTreeMap::new(),
                started: BTreeMap::new(),
                chosen: BTreeMap::new(),
                opened_at: BTreeMap::new(),
                sealed_at: BTreeMap::new(),
                healed: BTreeSet::new(),
                added: BTreeSet::new(),
                heard: BTreeMap::new(),
                ctls,
                claimed: false,
                next: 0,
                texts: 0,
                call: None,
                again: None,
                answers: Vec::new(),
                judged: 0,
                acked: Vec::new(),
                belows: Vec::new(),
            });
        }
        let mut model = Self {
            mutant,
            clock: ManualClock::new(),
            register,
            machines: BTreeMap::new(),
            journals,
            late: Vec::new(),
            token: None,
            muted: BTreeMap::new(),
            lost: None,
        };
        for m in MACHINES {
            model.machines.insert(
                m,
                Machine {
                    driver: None,
                    disk: Disk::new(two_journals),
                    inbox: Rc::default(),
                    outbox: Rc::default(),
                    placed: Vec::new(),
                    ever_down: false,
                },
            );
            model.start(m);
        }
        model.place();
        model
    }

    /// What the run did, for each journal: the answers its writer got,
    /// and the segments its register opened.
    #[must_use]
    pub fn progress(&self) -> Vec<(usize, usize)> {
        (0..self.journals.len())
            .map(|j| {
                (
                    self.journals[j].answers.len(),
                    self.value(j).segments().len(),
                )
            })
            .collect()
    }

    fn now(&self) -> u64 {
        self.clock.now().0
    }

    fn value(&self, j: usize) -> RegisterValue {
        self.register
            .value(self.journals[j].id)
            .expect("each journal has a register")
    }

    fn value_of(&self, id: JournalId) -> RegisterValue {
        self.register
            .value(id)
            .expect("each journal has a register")
    }

    fn up(&self, machine: u64) -> bool {
        self.machines[&machine].driver.is_some()
    }

    /// Run `step` for one journal (each journal in the settle phase).
    fn each(&mut self, cx: &mut Cx<'_, Self>, step: fn(&mut Self, usize, &mut Cx<'_, Self>)) {
        if cx.phase() == Phase::Settle {
            for j in 0..self.journals.len() {
                step(self, j, cx);
            }
        } else {
            let j = cx.rng().index(self.journals.len()).expect("journals");
            step(self, j, cx);
        }
    }

    /// A new driver for `machine` on its disk: it serves its placed roles
    /// again.
    fn start(&mut self, m: u64) {
        let machine = self.machines.get_mut(&m).expect("a machine");
        let wire = Wire {
            inbox: Rc::clone(&machine.inbox),
            outbox: Rc::clone(&machine.outbox),
            disk: machine.disk.clone(),
            flight: FLIGHT,
        };
        let mut driver = Driver::new(
            CONFIG,
            machine.disk.clone(),
            wire,
            self.clock.clone(),
            self.register.clone(),
            (),
        );
        for assignment in &machine.placed {
            driver.assign(assignment.clone());
        }
        machine.driver = Some(driver);
    }

    /// `machine` goes down: its driver, its leaders and its controllers are
    /// gone; its disk and its placed roles stay.
    fn down(&mut self, m: u64) {
        let machine = self.machines.get_mut(&m).expect("a machine");
        if machine.driver.take().is_none() {
            return;
        }
        machine.inbox.borrow_mut().clear();
        machine.outbox.borrow_mut().clear();
        machine.ever_down = true;
        for journal in &mut self.journals {
            for ctl in &mut journal.ctls {
                if ctl.machine == m {
                    ctl.driver = None;
                }
            }
            if journal.call.as_ref().is_some_and(|c| c.machine == m) {
                // The caller times out: the fate of its call is unknown.
                let call = journal.call.take().expect("a call");
                journal.again = Some(call.command);
            }
        }
    }

    /// The truth of a machine, for the oracle [`ALERTS`] of journal `j`:
    /// down, and not up at a report of the journal's controllers for long
    /// enough to be replaced.
    fn truly_gone(&self, j: usize, machine: u64) -> bool {
        let heard = self.journals[j].heard.get(&machine).copied().unwrap_or(0);
        !self.up(machine) && self.now() - heard >= DOWN_AFTER * TICK
    }

    /// Every node of journal `j` that the register names.
    fn named(value: &RegisterValue) -> BTreeSet<NodeId> {
        value
            .segments()
            .values()
            .flat_map(|r| {
                r.grid()
                    .columns()
                    .iter()
                    .flatten()
                    .chain(r.replicas())
                    .copied()
            })
            .collect()
    }

    /// The machines that serve a member of the segment in force.
    fn in_force_machines(value: &RegisterValue) -> BTreeSet<u64> {
        value.in_force().map_or_else(BTreeSet::new, |r| {
            r.grid()
                .columns()
                .iter()
                .flatten()
                .chain(r.replicas())
                .map(|n| machine_of(*n))
                .collect()
        })
    }

    /// The candidates of the host's placement for journal `j`: the next
    /// free node of each machine that is up, in machine order, first the
    /// machines that serve no member of the segment in force, then the
    /// others. A heal puts a member on a machine that serves another group
    /// of the journal only when no free machine meets the zone rule (a
    /// machine lost for good can leave a zone with no free machine); the
    /// zone rule of each group keeps one member of it on each machine.
    fn candidates(&self, value: &RegisterValue) -> Vec<NodeId> {
        let named = Self::named(value);
        let busy = Self::in_force_machines(value);
        let (free, shared): (Vec<u64>, Vec<u64>) = MACHINES
            .into_iter()
            .filter(|m| self.up(*m))
            .partition(|m| !busy.contains(m));
        free.into_iter()
            .chain(shared)
            .map(|m| {
                let mut node = m;
                while named.contains(&NodeId(node)) {
                    node += 10;
                }
                NodeId(node)
            })
            .collect()
    }

    /// Placement: every acceptor and replica that a register names is
    /// served on its machine; the walk notes each new segment and seal.
    fn place(&mut self) {
        let now = self.now();
        for j in 0..self.journals.len() {
            let value = self.value(j);
            let id = self.journals[j].id;
            let mut wanted: Vec<Assignment> = Vec::new();
            for record in value.segments().values() {
                for node in record.grid().columns().iter().flatten() {
                    wanted.push(Assignment::Acceptor {
                        address: Address::new(id, *node),
                        page: PROMISE_BATCH,
                    });
                }
                if record.state() < SegmentState::Trimmed {
                    for node in record.replicas() {
                        wanted.push(Assignment::Replica {
                            address: Address::new(id, *node),
                            segment: record.id(),
                            set: record.replicas().to_vec(),
                            initial: Leader::Unset,
                        });
                    }
                }
                let journal = &mut self.journals[j];
                journal.opened_at.entry(record.id()).or_insert(now);
                if record.state() >= SegmentState::Sealed
                    && !journal.sealed_at.contains_key(&record.id())
                {
                    journal.sealed_at.insert(record.id(), now);
                    sometimes!(true, reach::SEALED);
                }
            }
            for assignment in wanted {
                let machine = self
                    .machines
                    .get_mut(&machine_of(assignment.address().node))
                    .expect("a machine");
                if machine
                    .placed
                    .iter()
                    .any(|a| a.address() == assignment.address())
                {
                    continue;
                }
                if let Some(driver) = machine.driver.as_mut() {
                    driver.assign(assignment.clone());
                }
                machine.placed.push(assignment);
            }
        }
    }

    /// The leaders of journal `j` on machines that are up, with their
    /// machine.
    fn leaders(&self, j: usize) -> Vec<(u64, NodeId)> {
        let id = self.journals[j].id;
        let mut leaders = Vec::new();
        for (m, machine) in &self.machines {
            let Some(driver) = machine.driver.as_ref() else {
                continue;
            };
            for address in driver.host().addresses() {
                if address.journal == id && driver.host().leader(address).is_some() {
                    leaders.push((*m, address.node));
                }
            }
        }
        leaders
    }

    /// The leader of journal `j` that writes, with the highest ballot.
    fn open_leader(&self, j: usize) -> Option<(u64, NodeId)> {
        let id = self.journals[j].id;
        self.leaders(j)
            .into_iter()
            .filter(|(m, node)| {
                self.machines[m]
                    .driver
                    .as_ref()
                    .and_then(|d| d.host().leader(Address::new(id, *node)))
                    .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
            })
            .max_by_key(|(_, node)| node.0 / 10)
    }

    /// The applied prefix of the oldest segment that is not retired: the
    /// first slot a new leader reads.
    fn applied_from(&self, j: usize, value: &RegisterValue) -> Slot {
        let id = self.journals[j].id;
        let oldest = value
            .segments()
            .values()
            .find(|r| r.state() < SegmentState::Retired)
            .expect("a segment is read");
        let start = oldest.start().unwrap_or(Slot(0));
        oldest
            .replicas()
            .iter()
            .filter_map(|node| {
                self.machines[&machine_of(*node)]
                    .driver
                    .as_ref()?
                    .host()
                    .replica(Address::replica(id, *node, oldest.id()))?
                    .front()
            })
            .chain(std::iter::once(start))
            .max()
            .expect("a start")
    }

    /// Poll one machine (every machine in the settle phase), then move its
    /// sends into the mailbox and read its replies.
    fn poll(&mut self, cx: &mut Cx<'_, Self>) {
        let up: Vec<u64> = MACHINES.into_iter().filter(|m| self.up(*m)).collect();
        let picked: Vec<u64> = if cx.phase() == Phase::Settle {
            up
        } else {
            cx.rng().pick(&up).copied().into_iter().collect()
        };
        for m in picked {
            let machine = self.machines.get_mut(&m).expect("a machine");
            let Some(driver) = machine.driver.as_mut() else {
                continue;
            };
            driver.poll();
            for (address, why) in driver.take_stopped() {
                if why == paros_journal_io::Stop::Failed {
                    sometimes!(true, reach::FAILED);
                    // Placement serves again a role that a failed commit
                    // stopped.
                    if let Some(assignment) = machine.placed.iter().find(|a| a.address() == address)
                    {
                        driver.assign(assignment.clone());
                    }
                }
            }
            let replies = driver.take_replies();
            let notices = driver.take_notices();
            self.flush(m, cx);
            for reply in replies {
                self.on_reply(m, reply.address.journal, reply.call, reply.outcome);
            }
            for notice in notices {
                self.on_notice(&notice, cx);
            }
        }
    }

    /// A new replica's complete copy goes to every live controller of its
    /// journal: only the one whose heal runs that copy counts it.
    fn on_notice(&mut self, notice: &Notice, cx: &mut Cx<'_, Self>) {
        let Notice::Copied {
            address,
            segment,
            copy,
        } = notice
        else {
            // A leader's done retire: the acceptors of the retired segment
            // stop when placement no longer names them.
            sometimes!(true, reach::RETIRED);
            return;
        };
        let Some(j) = self
            .journals
            .iter()
            .position(|journal| journal.id == address.journal)
        else {
            return;
        };
        sometimes!(true, reach::COPIED);
        for c in 0..self.journals[j].ctls.len() {
            self.copied(j, c, *segment, *copy, cx);
        }
    }

    /// Move the sends of `machine` into the mailbox, through the oracles of
    /// the wire.
    fn flush(&mut self, m: u64, cx: &mut Cx<'_, Self>) {
        let sent: Vec<Envelope> = self.machines[&m].outbox.borrow_mut().drain(..).collect();
        for mut envelope in sent {
            if self.mutant == WholeMutant::CommitAtEnd {
                self.commit_at_end(&mut envelope);
            }
            self.on_wire(&envelope);
            cx.send(envelope);
        }
    }

    /// The planted bug [`WholeMutant::CommitAtEnd`]: a `Commit` of a
    /// sealed segment leaves at the segment's end.
    fn commit_at_end(&self, envelope: &mut Envelope) {
        let Message::Commit { segment, slot, .. } = &mut envelope.message else {
            return;
        };
        let j = self.journal_index(envelope.journal);
        if let Some(end) = self.value(j).segment(*segment).and_then(SegmentRecord::end) {
            *slot = end;
        }
    }

    fn journal_index(&self, id: JournalId) -> usize {
        self.journals
            .iter()
            .position(|j| j.id == id)
            .expect("a journal of the walk")
    }

    /// [`AGREE`] and [`SEALED`] on every `Commit` that leaves.
    fn on_wire(&mut self, envelope: &Envelope) {
        let Message::Commit {
            segment,
            slot,
            command,
        } = &envelope.message
        else {
            return;
        };
        let j = self.journal_index(envelope.journal);
        let value = self.value(j);
        if let Some(end) = value.segment(*segment).and_then(SegmentRecord::end) {
            assert!(
                *slot < end,
                "{SEALED}: journal {} slot {slot:?} of {segment:?} committed, sealed at {end:?}",
                envelope.journal.0
            );
        }
        let journal = &mut self.journals[j];
        sometimes!(
            journal.healed.contains(segment) && matches!(command, Command::Write(_)),
            reach::HEALED_WRITE
        );
        let held = journal
            .chosen
            .entry(*slot)
            .or_insert_with(|| command.clone());
        assert!(
            held == command,
            "{AGREE}: journal {} slot {slot:?} chose {held:?} and {command:?}",
            envelope.journal.0
        );
    }

    fn on_reply(&mut self, m: u64, id: JournalId, call: CallId, outcome: CallOutcome) {
        let j = self.journal_index(id);
        let mutant = self.mutant;
        let journal = &mut self.journals[j];
        let Some(call) = journal.call.take_if(|c| c.machine == m && c.id == call) else {
            return;
        };
        let answer = match outcome {
            CallOutcome::NotLeader => {
                journal.again = Some(call.command);
                return;
            }
            CallOutcome::Reply(Reply::Unknown) => {
                sometimes!(true, reach::UNKNOWN);
                journal.again = Some(call.command);
                return;
            }
            CallOutcome::Reply(Reply::Answer(answer)) => answer,
        };
        sometimes!(
            call.resent && matches!(answer, Answer::Duplicate { .. }),
            reach::DUPLICATE
        );
        let answer = match (mutant, answer, &call.command) {
            (
                WholeMutant::TakenAsWritten,
                Answer::Taken(_),
                Command::Write(Write {
                    fence: Fence::Leader { expected_seq, .. },
                    records,
                }),
            ) => Answer::Written {
                seq: *expected_seq,
                count: records.len() as u64,
            },
            (_, answer, _) => answer,
        };
        match (&answer, &call.command) {
            (
                Answer::Written { seq, count } | Answer::Duplicate { seq, count },
                Command::Write(write),
            ) => {
                journal.next = journal.next.max(seq.0 + count);
                let at = if mutant == WholeMutant::AckOffByOne {
                    seq.0 + 1
                } else {
                    seq.0
                };
                journal.acked.push((at, payload(write)));
                if mutant == WholeMutant::WriteTwice && !call.resent {
                    journal.again = Some(Command::Write(Write {
                        fence: Fence::Leader {
                            uuid: A,
                            expected_seq: Seq(seq.0 + count),
                        },
                        records: write.records.clone(),
                    }));
                }
            }
            (Answer::Below(view), Command::Write(write)) => {
                sometimes!(true, reach::BELOW);
                journal.next = journal.next.max(view.next_seq.0);
                journal.belows.push(write.clone());
            }
            (
                Answer::LeaderSet(view)
                | Answer::LeaderRefused(view)
                | Answer::Taken(view)
                | Answer::Early(view)
                | Answer::Fenced(view),
                _,
            ) => {
                journal.claimed |= view.leader == Leader::Uuid(A);
                journal.next = journal.next.max(view.next_seq.0);
            }
            _ => {}
        }
        journal.answers.push((call.command, answer));
    }

    /// A new leader of the newest segment of journal `j` on a machine that
    /// is up, when none writes (and now and then in the chaos phase).
    fn campaign(&mut self, j: usize, cx: &mut Cx<'_, Self>) {
        let value = self.value(j);
        let newest = value.newest().expect("a segment").id();
        let now = self.now();
        let id = self.journals[j].id;
        let live = self.leaders(j).into_iter().any(|(m, node)| {
            let leader = self.machines[&m]
                .driver
                .as_ref()
                .and_then(|d| d.host().leader(Address::new(id, node)))
                .expect("a leader");
            leader.segment() == newest
                && *leader.phase() != LeaderPhase::Stopped
                && (matches!(leader.phase(), LeaderPhase::Open { .. })
                    || now - self.journals[j].started[&node] < STUCK * TICK)
        });
        if live && (cx.phase() != Phase::Chaos || !cx.rng().chance(1, 32)) {
            return;
        }
        let up: Vec<u64> = MACHINES.into_iter().filter(|m| self.up(*m)).collect();
        let Some(m) = cx.rng().pick(&up).copied() else {
            return;
        };
        let from = self.applied_from(j, &value);
        let journal = &mut self.journals[j];
        journal.round += 1;
        let node = NodeId(LEADERS + 10 * journal.round + m);
        journal.bound.insert(node, newest);
        journal.started.insert(node, now);
        let older: Vec<NodeId> = journal.started.keys().copied().collect();
        let machine = self.machines.get_mut(&m).expect("a machine");
        let driver = machine.driver.as_mut().expect("up");
        // A machine runs one leader of a journal: its earlier ones stop.
        for old in older {
            let address = Address::new(id, old);
            if old != node && driver.host().leader(address).is_some() {
                driver.unassign(address);
            }
        }
        let mut replies = driver.take_replies();
        driver.assign(Assignment::Leader {
            address: Address::new(id, node),
            proxies: Vec::new(),
            campaign: Campaign {
                ballot: Ballot {
                    round: self.journals[j].round,
                    node,
                },
                segment: newest,
                from,
            },
        });
        replies.extend(driver.take_replies());
        self.flush(m, cx);
        for reply in replies {
            self.on_reply(m, reply.address.journal, reply.call, reply.outcome);
        }
    }

    /// The writer of journal `j` calls the leader that writes.
    fn write(&mut self, j: usize, cx: &mut Cx<'_, Self>) {
        if self.journals[j].call.is_some() {
            return;
        }
        let Some((m, node)) = self.open_leader(j) else {
            return;
        };
        let journal = &mut self.journals[j];
        let (command, resent) = if let Some(command) = journal.again.take() {
            (command, true)
        } else if cx.phase() == Phase::Settle && journal.claimed {
            return;
        } else if !journal.claimed {
            (
                Command::SetLeader {
                    new: Leader::uuid(A),
                    old: Leader::Unset,
                },
                false,
            )
        } else {
            journal.texts += 1;
            let at = if cx.rng().chance(1, 8) {
                journal.next.saturating_sub(1)
            } else {
                journal.next
            };
            let command = Command::Write(Write {
                fence: Fence::Leader {
                    uuid: A,
                    expected_seq: Seq(at),
                },
                records: vec![Record::new(
                    [journal.id.0.to_be_bytes(), journal.texts.to_be_bytes()].concat(),
                )],
            });
            (command, false)
        };
        let id = journal.id;
        let driver = self
            .machines
            .get_mut(&m)
            .and_then(|machine| machine.driver.as_mut())
            .expect("up");
        let call = driver.call(Address::new(id, node), command.clone());
        self.journals[j].call = Some(Call {
            machine: m,
            id: call,
            command,
            resent,
        });
        let replies = driver.take_replies();
        self.flush(m, cx);
        for reply in replies {
            self.on_reply(m, reply.address.journal, reply.call, reply.outcome);
        }
    }

    /// One step of the controllers of journal `j` (each controller in the
    /// settle phase, else one at random). First every machine that is up
    /// publishes its report to every controller of the journal that is up:
    /// a beat, and the segment of each leader on it that writes.
    fn control(&mut self, j: usize, cx: &mut Cx<'_, Self>) {
        self.publish(j, cx.phase() == Phase::Chaos);
        let alive: Vec<usize> = (0..self.journals[j].ctls.len())
            .filter(|c| self.journals[j].ctls[*c].driver.is_some())
            .collect();
        let picked: Vec<usize> = if cx.phase() == Phase::Settle {
            alive
        } else {
            cx.rng().pick(&alive).copied().into_iter().collect()
        };
        for c in picked {
            self.control_one(j, c, cx);
        }
    }

    /// The reports of every machine that is up, and of every controller of
    /// journal `j` that is up, to every controller of the journal that is
    /// up. In the chaos phase a muted controller's report is lost
    /// ([`WholeAction::Mute`]). The planted bug
    /// [`WholeMutant::StaleLiveness`] drops the reports of a machine that
    /// was ever down.
    fn publish(&mut self, j: usize, chaos: bool) {
        let now = self.now();
        let id = self.journals[j].id;
        let mut reports = Vec::new();
        for (c, ctl) in self.journals[j].ctls.iter().enumerate() {
            let muted = chaos && self.muted.get(&(j, c)).is_some_and(|until| now < *until);
            if let Some(driver) = ctl.driver.as_ref()
                && !muted
            {
                reports.push(driver.report());
            }
        }
        for (m, machine) in &self.machines {
            let Some(driver) = machine.driver.as_ref() else {
                continue;
            };
            self.journals[j].heard.insert(*m, now);
            if self.mutant == WholeMutant::StaleLiveness && machine.ever_down {
                continue;
            }
            let process = ProcessId(*m);
            reports.push(Report::Beat { process });
            for address in driver.host().addresses() {
                if address.journal != id {
                    continue;
                }
                if let Some(leader) = driver.host().leader(address)
                    && matches!(leader.phase(), LeaderPhase::Open { .. })
                {
                    reports.push(Report::Writes {
                        process,
                        segment: leader.segment(),
                    });
                }
            }
        }
        for ctl in &mut self.journals[j].ctls {
            if let Some(driver) = ctl.driver.as_mut() {
                for report in &reports {
                    driver.publish(now, *report);
                }
            }
        }
    }

    /// The ports of controller `c` of journal `j`: the register, lossy in
    /// the chaos phase, and the walk's placement and token.
    fn ports<'a, 'b>(
        &self,
        j: usize,
        cx: &'a mut Cx<'b, Self>,
    ) -> (WalkRegister<'a, 'b>, WalkCell) {
        let id = self.journals[j].id;
        let value = self.value(j);
        let register = WalkRegister {
            register: self.register.clone(),
            id,
            chaos: cx.phase() == Phase::Chaos,
            cx,
            ran: Vec::new(),
        };
        let cell = WalkCell {
            j,
            candidates: self.candidates(&value),
            matchmakers_live: MATCHMAKERS
                .iter()
                .zip(1..)
                .filter(|(m, _)| self.up(**m))
                .map(|(_, id)| MatchmakerId(id))
                .collect(),
            token: self.token,
            any_token: self.mutant == WholeMutant::NoToken,
        };
        (register, cell)
    }

    /// One step of controller `c` of journal `j`, on a machine that is up.
    fn control_one(&mut self, j: usize, c: usize, cx: &mut Cx<'_, Self>) {
        let now = self.now();
        let holder = self.value(j).term().holder;
        let mut driver = self.journals[j].ctls[c].driver.take().expect("up");
        let (mut register, mut cell) = self.ports(j, cx);
        let polled = driver.poll(now, &mut register, &mut cell);
        let ran = std::mem::take(&mut register.ran);
        drop(register);
        self.token = cell.token;
        let me = driver.controller().me();
        let value = self.value(j);
        sometimes!(
            driver.controller().term() == Some(value.term().number)
                && value.term().holder == Some(me)
                && holder.is_some_and(|h| h != me && h.0 >= CONTROLLERS),
            reach::TAKEOVER
        );
        self.journals[j].ctls[c].driver = Some(driver);
        self.ran(j, &ran);
        self.polled(j, c, polled, cx);
    }

    /// Note the register operations that a controller of journal `j` ran.
    fn ran(&mut self, j: usize, ran: &[(Change, Outcome, bool)]) {
        let value = self.value(j);
        for (change, outcome, done) in ran {
            match change {
                Change::Reshape { opening, release } => {
                    assert_eq!(*release, RELEASE, "a reshape at this release");
                    let opened = value
                        .segments()
                        .values()
                        .find(|r| r.opening() == opening)
                        .map(SegmentRecord::id);
                    if let Some(segment) = opened {
                        sometimes!(true, reach::HEALED);
                        sometimes!(*done && *outcome == Outcome::NoAnswer, reach::HEAL_LOST);
                        self.journals[j].healed.insert(segment);
                    }
                }
                Change::Rereplicate {
                    segment,
                    add,
                    remove,
                    ..
                } => {
                    if *done {
                        sometimes!(true, reach::REREPLICATED);
                        sometimes!(self.lost == Some(machine_of(*remove)), reach::LOST_HEALED);
                        self.journals[j].added.insert((*segment, *add));
                    }
                }
                Change::Elect { .. } => {}
                other => {
                    panic!("a controller asks an election, a reshape or a heal, not {other:?}")
                }
            }
        }
    }

    /// Run what one step of controller `c` of journal `j` gives: judge its
    /// alerts, and send its asks to the new replicas.
    fn polled(&mut self, j: usize, c: usize, polled: Polled, cx: &mut Cx<'_, Self>) {
        if let Some(decision) = &polled.decision {
            let value = self.value(j);
            sometimes!(decision.action == Action::Stop, reach::DEPOSED);
            self.judge_alerts(j, &value, &decision.alerts);
            sometimes!(
                decision.action == Action::Wait(paros_journal_control::Wait::NoToken),
                reach::NO_TOKEN
            );
        }
        for ask in polled.sends {
            match ask {
                Ask::Act(action) => {
                    // The walk damages no record (a disk fault is an I/O
                    // error), so no decision has a damaged copy to repair,
                    // and the role runs a re-replication as a heal.
                    panic!("the controller asked {action:?}, but no record is damaged");
                }
                ask @ Ask::Copy { .. } => {
                    sometimes!(true, reach::REREPLICATE);
                    self.send_ask(j, c, &ask, cx);
                }
                ask @ Ask::Abandon { .. } => self.send_ask(j, c, &ask, cx),
                other => {
                    panic!("a driver sends only a copy, an abandon or a repair, not {other:?}")
                }
            }
        }
        self.judge_in_flight();
    }

    /// The new replica of `copy` reports its complete copy to controller
    /// `c` of journal `j`.
    fn copied(
        &mut self,
        j: usize,
        c: usize,
        segment: SegmentId,
        copy: CopyId,
        cx: &mut Cx<'_, Self>,
    ) {
        let now = self.now();
        let Some(mut driver) = self.journals[j].ctls[c].driver.take() else {
            return;
        };
        let (mut register, mut cell) = self.ports(j, cx);
        let polled = driver.copied(now, segment, copy, &mut register, &mut cell);
        let ran = std::mem::take(&mut register.ran);
        drop(register);
        self.token = cell.token;
        self.journals[j].ctls[c].driver = Some(driver);
        self.ran(j, &ran);
        self.polled(j, c, polled, cx);
    }

    /// A copy or abandon ask of controller `c` goes to the new replica's
    /// driver. In the chaos phase the network may lose it, and may hold a
    /// copy of it that arrives again later ([`WholeAction::Late`]).
    fn send_ask(&mut self, j: usize, c: usize, ask: &Ask, cx: &mut Cx<'_, Self>) {
        if cx.phase() == Phase::Chaos {
            if cx.rng().chance(1, 4) {
                self.late.push((j, ask.clone()));
            }
            if cx.rng().chance(1, 10) {
                return;
            }
        }
        self.deliver_ask(j, ask);
        if self.mutant == WholeMutant::EarlyCopied
            && let Ask::Copy { segment, copy, .. } = &ask
        {
            self.copied(j, c, *segment, *copy, cx);
        }
    }

    /// The new replica's driver takes the ask, when its machine is up.
    fn deliver_ask(&mut self, j: usize, ask: &Ask) {
        let id = self.journals[j].id;
        let (node, address) = match ask {
            Ask::Copy { segment, node, .. } | Ask::Abandon { segment, node, .. } => {
                (*node, Address::replica(id, *node, *segment))
            }
            _ => unreachable!("only a copy or an abandon is held"),
        };
        let Some(driver) = self
            .machines
            .get_mut(&machine_of(node))
            .and_then(|machine| machine.driver.as_mut())
        else {
            return;
        };
        match ask {
            Ask::Copy { set, copy, .. } => {
                driver.join(address, set.clone(), Leader::Unset, *copy);
            }
            Ask::Abandon { copy, .. } => driver.abandon(address, *copy),
            _ => unreachable!("only a copy or an abandon is held"),
        }
    }

    /// A held copy or abandon ask arrives again; the network keeps it or
    /// not.
    fn late(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(at) = cx.rng().index(self.late.len()) else {
            return;
        };
        let (j, ask) = if cx.rng().chance(1, 2) {
            self.late.swap_remove(at)
        } else {
            self.late[at].clone()
        };
        sometimes!(true, reach::LATE_ASK);
        self.deliver_ask(j, &ask);
    }

    /// [`HEAL_HELD`]: each node that a done heal added to the set of a
    /// retired segment holds every slot of the segment on its disk while
    /// the set names it, but the slots below its `Trim`: the compaction
    /// drops a slot only when every record it wrote is below the trim
    /// floor, the floor is one that the chosen log decided, and the fold of
    /// the `Trim` is the fold of the chosen log at its slot.
    fn judge_heals(&self) {
        for (j, journal) in self.journals.iter().enumerate() {
            let value = self.value_of(journal.id);
            for (segment, node) in &journal.added {
                let Some(record) = value.segment(*segment) else {
                    continue;
                };
                if record.state() != SegmentState::Retired || !record.replicas().contains(node) {
                    continue;
                }
                let start = record.start().unwrap_or(Slot(0));
                let end = record.end().expect("a retired segment has an end");
                let address = Address::replica(journal.id, *node, *segment);
                let entries = self.machines[&machine_of(*node)]
                    .disk
                    .store
                    .borrow_mut()
                    .scan(address)
                    .expect("a scan of memory");
                let disk = paros_journal_io::layout::decode_replica(address, &entries)
                    .expect("a replica disk the walk never damages here");
                let below = disk
                    .trim
                    .as_ref()
                    .map_or(start, |trim| trim.slot.max(start));
                let missing: Vec<u64> = (below.0..end.0)
                    .filter(|s| !disk.chosen.contains_key(&Slot(*s)))
                    .collect();
                assert!(
                    missing.is_empty(),
                    "{HEAL_HELD}: {node:?} of {segment:?} misses the slots {missing:?}"
                );
                let Some(trim) = disk.trim.as_ref().filter(|trim| trim.slot > start) else {
                    continue;
                };
                sometimes!(true, reach::HEALED_TRIM);
                let (states, _, truth) = self.truth(j);
                // The fold that the healed node starts from at its trim is
                // the fold of the chosen log before the trim's slot.
                let mut before = Truth::new(Leader::Unset);
                for slot in 0..trim.slot.0 {
                    before.apply(&journal.chosen[&Slot(slot)]);
                }
                assert!(
                    (
                        trim.state.leader(),
                        trim.state.next_seq().0,
                        trim.state.first_seq().0
                    ) == (before.leader, before.next, before.first),
                    "{HEAL_HELD}: {node:?} of {segment:?} trims at {:?} with the fold {:?}, \
                     the chosen log has {:?} there",
                    trim.slot,
                    trim.state.view(),
                    (before.leader, before.next, before.first)
                );
                assert!(
                    trim.floor.0 <= truth.first,
                    "{HEAL_HELD}: {node:?} of {segment:?} trims at the floor {:?}, above the \
                     decided floor {}",
                    trim.floor,
                    truth.first
                );
                let dropped = (0u64..)
                    .zip(&states)
                    .filter(|(slot, _)| (start.0..trim.slot.0).contains(slot));
                for (slot, (before, verdict)) in dropped {
                    let reaches = match (journal.chosen.get(&Slot(slot)), *verdict) {
                        (Some(Command::Write(write)), "ok") => {
                            before + write.records.len() as u64 > trim.floor.0
                        }
                        _ => false,
                    };
                    assert!(
                        !reaches,
                        "{HEAL_HELD}: {node:?} of {segment:?} dropped slot {slot}, which wrote \
                         a record at or above its trim floor {:?}",
                        trim.floor
                    );
                }
            }
        }
    }

    /// [`BOUNDED_IN_FLIGHT`]: at most one journal has an expensive action
    /// in flight: a reshape that its retire has not ended (a pending or a
    /// sealed segment), or a heal of the controller that holds the
    /// register's term. The two journals share one token.
    fn judge_in_flight(&self) {
        let busy: Vec<u64> =
            self.journals
                .iter()
                .filter(|journal| {
                    let value = self.value_of(journal.id);
                    let term = value.term();
                    let healing = journal.ctls.iter().any(|ctl| {
                        ctl.driver.as_ref().is_some_and(|d| {
                            term.holder == Some(d.controller().me())
                                && d.controller().term() == Some(term.number)
                                && d.controller().heal().is_some()
                        })
                    });
                    healing
                        || value.segments().values().any(|r| {
                            matches!(r.state(), SegmentState::Pending | SegmentState::Sealed)
                        })
                })
                .map(|journal| journal.id.0)
                .collect();
        assert!(
            busy.len() <= 1,
            "{BOUNDED_IN_FLIGHT}: the journals {busy:?} reshape or heal at once"
        );
    }

    /// [`ALERTS`]: each alert is true of the machines as they are. The
    /// grids of this walk are one column of three with `F = 1`, so each
    /// truth is a count over the machines.
    fn judge_alerts(&self, j: usize, value: &RegisterValue, alerts: &BTreeSet<Alert>) {
        let in_force = value.in_force().expect("a segment in force");
        let members: Vec<NodeId> = in_force
            .grid()
            .columns()
            .iter()
            .flatten()
            .copied()
            .collect();
        let up = members.iter().filter(|n| self.up(machine_of(**n))).count();
        let gone: Vec<NodeId> = members
            .iter()
            .copied()
            .filter(|n| self.truly_gone(j, machine_of(*n)))
            .collect();
        let kept_zones: BTreeSet<u64> = members
            .iter()
            .filter(|n| !gone.contains(n))
            .map(|n| zone_of(machine_of(*n)))
            .collect();
        let kept = members.len() - gone.len();
        // The zones where a candidate can take the place of a member gone:
        // every machine that is up is a candidate.
        let free_zones: BTreeSet<u64> = MACHINES
            .into_iter()
            .filter(|m| self.up(*m))
            .map(zone_of)
            .filter(|z| !kept_zones.contains(z))
            .collect();
        let placeable = kept_zones.len() == kept && free_zones.len() >= gone.len();
        let matchmakers_up = MATCHMAKERS.iter().filter(|m| self.up(**m)).count();
        // Whether a set a heal may replace (the set in force, and each
        // sealed or retired set) has a replica gone and no placement that
        // keeps one replica in each zone: its kept replicas share a zone,
        // or too few zones have a free machine.
        let replicas_stuck = value
            .segments()
            .values()
            .filter(|r| {
                matches!(
                    r.state(),
                    SegmentState::InForce | SegmentState::Sealed | SegmentState::Retired
                )
            })
            .any(|r| {
                let (gone, kept): (Vec<NodeId>, Vec<NodeId>) = r
                    .replicas()
                    .iter()
                    .partition(|n| self.truly_gone(j, machine_of(**n)));
                let kept_zones: BTreeSet<u64> =
                    kept.iter().map(|n| zone_of(machine_of(*n))).collect();
                let free_zones = MACHINES
                    .into_iter()
                    .filter(|m| self.up(*m))
                    .map(zone_of)
                    .filter(|z| !kept_zones.contains(z))
                    .collect::<BTreeSet<u64>>()
                    .len();
                !gone.is_empty() && (kept_zones.len() < kept.len() || free_zones < gone.len())
            });
        let now = self.now();
        let journal = &self.journals[j];
        for alert in alerts {
            let truth = match alert {
                Alert::ControlFrozen => matchmakers_up < 2,
                Alert::MatchmakersAtRisk => matchmakers_up == 2,
                Alert::Unplaceable => !placeable,
                Alert::DataLost | Alert::WritesDegraded | Alert::ElectionBlocked => false,
                Alert::Stopped => up < 2,
                Alert::ReshapeStalled => value.segments().values().any(|r| {
                    r.state() == SegmentState::Sealed
                        && journal
                            .sealed_at
                            .get(&r.id())
                            .is_some_and(|at| now - at > TUNABLES.stall_window)
                }),
                Alert::ReplicasUnplaceable => replicas_stuck,
            };
            if *alert == Alert::Unplaceable {
                sometimes!(true, reach::UNPLACEABLE);
            }
            assert!(
                truth,
                "{ALERTS}: journal {} alerts {alert:?}; machines down {:?}",
                journal.id.0,
                MACHINES
                    .into_iter()
                    .filter(|m| !self.up(*m))
                    .collect::<Vec<_>>()
            );
        }
    }

    /// The writer of journal `j` resolves a `Below` at a replica of an
    /// older segment; [`ANSWER`] judges the fate.
    fn resolve(&mut self, j: usize, _cx: &mut Cx<'_, Self>) {
        let Some(write) = self.journals[j].belows.first().cloned() else {
            return;
        };
        let Fence::Leader { uuid, expected_seq } = write.fence else {
            unreachable!("only a fenced write is answered Below");
        };
        let value = self.value(j);
        let id = self.journals[j].id;
        let in_force = value.in_force().map(SegmentRecord::id);
        let mut answers = Vec::new();
        for record in value.segments().values() {
            if Some(record.id()) == in_force {
                continue;
            }
            for node in record.replicas() {
                let found = self.machines[&machine_of(*node)]
                    .driver
                    .as_ref()
                    .and_then(|d| d.host().replica(Address::replica(id, *node, record.id())))
                    // The walk reads the replica's answer to judge its fate; the
                    // persist id it waits on matters only to a driver that sends it.
                    .map(|r| r.resolve(uuid, expected_seq, &write.records).0);
                if let Some(found) = found
                    && found != ResolveAnswer::NotCovered
                {
                    answers.push(found);
                }
            }
        }
        if answers.is_empty() {
            return;
        }
        let Some(verdicts) = self.verdicts_of(j, &Command::Write(write.clone())) else {
            return;
        };
        for answer in answers {
            let truth = match answer {
                ResolveAnswer::Written { .. } => verdicts.iter().any(|(_, v)| *v == "dup"),
                ResolveAnswer::NotWritten => verdicts.iter().any(|(_, v)| *v == "taken"),
                ResolveAnswer::Truncated | ResolveAnswer::NotCovered => false,
            };
            assert!(
                truth,
                "{ANSWER}: journal {} {write:?} resolved {answer:?}, the whole log gives {verdicts:?}",
                id.0
            );
        }
        sometimes!(true, reach::RESOLVED);
        self.journals[j].belows.remove(0);
    }

    /// The truth over the chosen prefix of journal `j`: for each slot, the
    /// position before it and the verdict, and the position of each record
    /// written.
    fn truth(&self, j: usize) -> Judged {
        let journal = &self.journals[j];
        let mut truth = Truth::new(Leader::Unset);
        let mut states = Vec::new();
        let mut positions = BTreeMap::new();
        for slot in 0..frontier(&journal.chosen).0 {
            let command = &journal.chosen[&Slot(slot)];
            let before = truth.next;
            let verdict = truth.apply(command);
            if let (Command::Write(write), "ok") = (command, verdict) {
                for (i, record) in write.records.iter().enumerate() {
                    positions.insert(before + i as u64, record.payload.clone());
                }
            }
            states.push((before, verdict));
        }
        (states, positions, truth)
    }

    /// The verdicts of the slots that hold `command`, `None` when none
    /// does yet.
    fn verdicts_of(&self, j: usize, command: &Command) -> Option<Vec<(u64, &'static str)>> {
        let (states, ..) = self.truth(j);
        let journal = &self.journals[j];
        let at: Vec<(u64, &'static str)> = states
            .iter()
            .enumerate()
            .filter(|(slot, _)| {
                as_written(&journal.chosen[&Slot(*slot as u64)]) == as_written(command)
            })
            .map(|(_, s)| *s)
            .collect();
        (!at.is_empty()).then_some(at)
    }

    /// [`ANSWER`], [`ONCE`] and [`ACKED`] over the chosen prefix of
    /// journal `j`.
    fn judge(&mut self, j: usize) {
        let (states, positions, truth) = self.truth(j);
        let journal = &self.journals[j];
        let mut judged = journal.judged;
        while let Some((command, answer)) = journal.answers.get(judged) {
            let at: Vec<(u64, &str)> = states
                .iter()
                .enumerate()
                .filter(|(slot, _)| {
                    as_written(&journal.chosen[&Slot(*slot as u64)]) == as_written(command)
                })
                .map(|(_, s)| *s)
                .collect();
            if at.is_empty() {
                break;
            }
            let name = label(answer);
            let fits = at.iter().any(|(before, verdict)| {
                (*verdict == name
                    && match answer {
                        Answer::Written { seq, .. } => seq.0 == *before,
                        _ => true,
                    })
                    || (name == "below" && matches!(*verdict, "dup" | "taken"))
            });
            assert!(
                fits,
                "{ANSWER}: journal {} {command:?} answered {answer:?}, the whole log gives {at:?}",
                journal.id.0
            );
            judged += 1;
        }
        let mut seen: BTreeSet<&Vec<u8>> = BTreeSet::new();
        for payload in positions.values() {
            assert!(
                seen.insert(payload),
                "{ONCE}: journal {} wrote {payload:?} twice",
                journal.id.0
            );
        }
        for (seq, payload) in &journal.acked {
            assert!(
                positions.get(seq) == Some(payload),
                "{ACKED}: journal {} acked {payload:?} at {seq}, the log holds {:?} (next {})",
                journal.id.0,
                positions.get(seq),
                truth.next
            );
        }
        self.journals[j].judged = judged;
    }

    fn converged_one(&self, j: usize, journal: &Journal) -> Result<(), String> {
        let value = self.value(j);
        let holder = value.term().holder;
        let holding = journal.ctls.iter().any(|ctl| {
            Some(ctl.node()) == holder
                && ctl.driver.as_ref().is_some_and(|d| {
                    d.controller().term() == Some(value.term().number)
                        && d.controller().call().is_none()
                })
        });
        if !holding {
            return Err("no controller holds the term with no call in flight".into());
        }
        let newest = value.newest().expect("a segment");
        if newest.state() != SegmentState::InForce {
            return Err(format!("{:?} is {:?}", newest.id(), newest.state()));
        }
        if let Some(lost) = self.lost {
            let on_lost = |nodes: &mut dyn Iterator<Item = &NodeId>| {
                nodes
                    .copied()
                    .filter(|n| machine_of(*n) == lost)
                    .collect::<Vec<_>>()
            };
            let grid = on_lost(&mut newest.grid().columns().iter().flatten());
            let replicas = on_lost(&mut newest.replicas().iter());
            if !grid.is_empty() || !replicas.is_empty() {
                return Err(format!(
                    "{:?} names {grid:?} and {replicas:?} on the lost machine {lost}",
                    newest.id()
                ));
            }
            for record in value.segments().values() {
                let named = on_lost(&mut record.replicas().iter());
                if record.state() == SegmentState::Retired && !named.is_empty() {
                    return Err(format!(
                        "retired {:?} names {named:?} on the lost machine {lost}",
                        record.id()
                    ));
                }
            }
        }
        if let Some(old) = value
            .segments()
            .values()
            .find(|r| r.id() != newest.id() && r.state() < SegmentState::Retired)
        {
            return Err(format!("{:?} is {:?}", old.id(), old.state()));
        }
        let Some((_, leader)) = self.open_leader(j) else {
            return Err("no leader writes".into());
        };
        if journal.bound.get(&leader) != Some(&newest.id()) {
            return Err("the leader that writes is not of the newest segment".into());
        }
        if journal.call.is_some() || journal.again.is_some() || !journal.claimed {
            return Err("the writer has a call to finish".into());
        }
        if !journal.belows.is_empty() {
            return Err(format!("{} Below not resolved", journal.belows.len()));
        }
        if journal.judged < journal.answers.len() {
            return Err(format!(
                "{} answers not judged",
                journal.answers.len() - journal.judged
            ));
        }
        let end = frontier(&journal.chosen);
        let (_, _, truth) = self.truth(j);
        for node in newest.replicas() {
            let replica = self.machines[&machine_of(*node)]
                .driver
                .as_ref()
                .and_then(|d| {
                    d.host()
                        .replica(Address::replica(journal.id, *node, newest.id()))
                });
            let front = replica.and_then(paros_journal::roles::SegmentReplica::front);
            if front != Some(end) {
                return Err(format!(
                    "replica {} is at {front:?}, the log at {end:?}",
                    node.0
                ));
            }
            let next = replica
                .and_then(|r| r.state())
                .map(paros_journal::JournalState::next_seq);
            if next != Some(Seq(truth.next)) {
                return Err(format!(
                    "replica {} folds to {next:?}, the whole log to {}",
                    node.0, truth.next
                ));
            }
        }
        Ok(())
    }

    /// Arm a commit fault on a machine.
    fn fault(&mut self, cx: &mut Cx<'_, Self>) {
        let m = *cx.rng().pick(&MACHINES).expect("machines");
        let disk = &self.machines[&m].disk;
        if self.mutant == WholeMutant::LyingDisk {
            disk.arm_lie();
        } else {
            disk.arm(cx.rng().chance(1, 2));
        }
    }

    /// The reports of one controller are lost for longer than the down
    /// window.
    fn mute(&mut self, cx: &mut Cx<'_, Self>) {
        let j = cx.rng().index(self.journals.len()).expect("journals");
        let c = cx
            .rng()
            .index(self.journals[j].ctls.len())
            .expect("controllers");
        let until = self.now() + (DOWN_AFTER + 2) * TICK;
        self.muted.insert((j, c), until);
    }

    /// Once in a run, a machine that is not a matchmaker is lost for good:
    /// it goes down and never comes back.
    fn lose(&mut self, cx: &mut Cx<'_, Self>) {
        if self.lost.is_some() || !cx.rng().chance(1, 4) {
            return;
        }
        let others: Vec<u64> = MACHINES
            .into_iter()
            .filter(|m| !MATCHMAKERS.contains(m))
            .collect();
        let m = *cx.rng().pick(&others).expect("machines");
        self.lost = Some(m);
        if self.up(m) {
            self.down(m);
            cx.crashed(&m);
        }
        sometimes!(true, reach::LOST);
    }

    /// Every machine of one zone crashes.
    fn zone_out(&mut self, cx: &mut Cx<'_, Self>) {
        let zone = cx.rng().below(3);
        for m in MACHINES {
            if zone_of(m) == zone && self.up(m) {
                self.down(m);
                cx.crashed(&m);
            }
        }
        sometimes!(true, reach::ZONE_OUT);
    }
}

/// The register as a controller of the walk reaches it: in the chaos phase
/// a request (1 in 10) or a reply (1 in 8) is lost. It notes each operation
/// with its outcome and whether the write took place.
struct WalkRegister<'a, 'b> {
    register: MemRegister,
    id: JournalId,
    chaos: bool,
    cx: &'a mut Cx<'b, WholeModel>,
    ran: Vec<(Change, Outcome, bool)>,
}

impl ControlRegister for WalkRegister<'_, '_> {
    fn read(&mut self) -> Option<RegisterValue> {
        self.register.value(self.id)
    }

    fn run(&mut self, change: &Change) -> Outcome {
        if self.chaos && self.cx.rng().chance(1, 10) {
            self.ran.push((change.clone(), Outcome::NoAnswer, false));
            return Outcome::NoAnswer;
        }
        let result = self.register.change(self.id, change);
        let done = result.is_ok();
        let outcome = if self.chaos && self.cx.rng().chance(1, 8) {
            Outcome::NoAnswer
        } else {
            match result {
                Ok(value) => Outcome::Done(value),
                Err(refusal) => Outcome::Refused(refusal),
            }
        };
        self.ran.push((change.clone(), outcome.clone(), done));
        outcome
    }
}

/// The walk's placement and the cell's one token, for journal `j`.
struct WalkCell {
    j: usize,
    candidates: Vec<NodeId>,
    matchmakers_live: BTreeSet<MatchmakerId>,
    token: Option<usize>,
    /// The planted bug [`WholeMutant::NoToken`].
    any_token: bool,
}

impl Cell for WalkCell {
    fn process_of(&self, node: NodeId) -> Option<ProcessId> {
        // A controller is a process of its own, on a stateless machine
        // (§J6.1); a role runs in the process of its machine.
        if node.0 >= CONTROLLERS {
            return Some(ProcessId(node.0));
        }
        let machine = machine_of(node);
        MACHINES.contains(&machine).then_some(ProcessId(machine))
    }

    fn zone(&self, process: ProcessId) -> Option<FailureDomain> {
        Some(domain(process.0))
    }

    fn candidates(&mut self, _value: &RegisterValue) -> Vec<NodeId> {
        self.candidates.clone()
    }

    fn matchmakers(&self) -> (MatchmakerSet, BTreeSet<MatchmakerId>) {
        (
            MatchmakerSet::new(MatchmakerGeneration(0), (1..=3).map(MatchmakerId).collect()),
            self.matchmakers_live.clone(),
        )
    }

    fn token_free(&self) -> bool {
        self.any_token || self.token.is_none_or(|holder| holder == self.j)
    }

    fn take_token(&mut self) {
        if self.token.is_none() {
            self.token = Some(self.j);
        }
    }

    fn give_back(&mut self) {
        if self.token == Some(self.j) {
            self.token = None;
        }
    }
}
