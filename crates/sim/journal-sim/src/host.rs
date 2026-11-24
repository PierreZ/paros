//! The journal on the generic driver of `paros-journal-io` under the walk
//! (#1329 (paros-journal-io), `docs/journal/04-roles.md` §J4.2).
//!
//! Three processes run one [`Driver`] each, with the backends of the walk:
//! a disk that survives a crash and fails some commits inside the contract
//! (a failed commit may or may not have written its batch), a transport
//! whose messages go through the walk's mailbox (lost, repeated and
//! reordered), a clock that the walk moves, and the journal's register on
//! three matchmakers ([`QuorumRegister`]): process `p` hosts matchmaker
//! `p`, which is down while the process is; a change can lose its request
//! or its reply, reach one member only, or wait in flight while other
//! operations run, and a read repairs a write that a minority took.
//! Process `p` serves acceptor `p` and replica `10 + p` of segment 1, and,
//! after a campaign on it, leader `100 + p`. A crash drops the driver and
//! keeps the disk; a restart is a new driver on the same disk, and the
//! roles read their records back. A role that a failed commit stopped is
//! assigned again, as placement would.
//!
//! One writer claims the journal, writes, and sends a call again when its
//! answer is lost or `Unknown`. A reader reads at the replica of a process
//! with [`Driver::read`]. A restarted process gets a new driver, whose read
//! ids start again at 1, while the answers to the reads of the driver
//! before are still on the wire.
//!
//! The oracles share no code with the driver or the roles:
//!
//! - [`AGREE`]: no two `Commit`s on the wire name two commands at one slot.
//! - [`FLIGHT`]: an acceptor's `Accepted` leaves only once a vote at least
//!   as new is on its disk, and a `Promise` only once a promise at least as
//!   high is (the flight rule, rule 1 of §J4.2).
//! - [`ANSWER`]: each answer that a leader gives through the driver is the
//!   whole log's at a slot that chose the command ([`Truth`]).
//! - [`READ`]: each page of a leaderless read holds the whole log's records
//!   at their positions, and its view covers every answer that the writer
//!   had when the read started (§J2.5).
//!
//! The liveness claim: once the faults stop, a leader writes, every call
//! is answered and judged, every replica applied the whole log, and every
//! read is answered.
//!
//! The segments, the seal and the clients of many writers are the roles
//! walk's ([`crate::roles`]); this walk judges the driver and the `Host`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};

use paros_journal::register::{Ballot, Change, MatchmakerId, RegisterValue};
use paros_journal::roles::PersistId;
use paros_journal::roles::{Campaign, LeaderPhase, Message};
use paros_journal::{
    Answer, Command, Fence, Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, ReadAnswer,
    Record, Reply, SegmentId, Seq, Slot, Write,
};
use paros_journal_io::mem::ManualClock;
use paros_journal_io::{
    Address, Assignment, CallId, CallOutcome, Driver, Envelope, HostConfig, JournalId, Key, Kind,
    Op,
};

use crate::backend::{Disk, DiskFault, Fault, Landed, LossyRegister, Lost, QuorumRegister, Wire};

use crate::oracle::{Truth, as_written, label};

/// Oracle: one command is chosen at each slot.
pub const AGREE: &str = "driver walk: no two Commits name two commands at one slot";
/// Oracle: the flight rule.
pub const FLIGHT: &str = "driver walk: a reply leaves only once the state it reports is durable";
/// Oracle: each answer is the whole log's.
pub const ANSWER: &str = "driver walk: each answer is the whole log's at a slot of the command";
/// Oracle: a read page holds the whole log's records and covers what the
/// writer knew when the read started.
pub const READ: &str =
    "driver walk: a read page holds the whole log's records and covers every answer before it";

/// The coverage sites of the driver walk.
pub mod reach {
    /// One commit held the batches of two roles (group commit).
    pub const GROUPED: &str = "driver walk: one commit held the batches of two roles";
    /// A commit wrote nothing (`Full`) and stopped its roles.
    pub const FAILED: &str = "driver walk: a failed commit stopped its roles";
    /// A commit of unknown outcome had written its batch, and its process
    /// stopped.
    pub const WRITTEN_HALTED: &str =
        "driver walk: an Unknown commit wrote, and its process stopped";
    /// A commit of unknown outcome had written nothing, and its process
    /// stopped.
    pub const UNWRITTEN_HALTED: &str = "driver walk: an Unknown commit wrote nothing and halted";
    /// A scan of unknown outcome stopped its process.
    pub const SCAN_HALTED: &str = "driver walk: an Unknown scan stopped its process";
    /// A scan read nothing (`Full`): its role stopped unreadable, and
    /// placement served it again.
    pub const UNREADABLE: &str = "driver walk: a failed scan stopped its role, served again";
    /// A list read nothing (`Full`) at a boot, and was asked again.
    pub const LIST_AGAIN: &str = "driver walk: a failed list at a boot was asked again";
    /// A role whose disk lost a record stopped for good at its restart.
    pub const LOST: &str = "driver walk: a role whose disk lost a record stopped for good";
    /// A process restarted, and an acceptor read a vote back.
    pub const RESTARTED: &str = "driver walk: an acceptor read its votes back at a restart";
    /// A campaign deposed an open leader.
    pub const DEPOSED: &str = "driver walk: a campaign deposed an open leader";
    /// A call sent again was answered `Duplicate`.
    pub const DUPLICATE: &str = "driver walk: a call sent again was answered Duplicate";
    /// A call was answered `Unknown`.
    pub const UNKNOWN: &str = "driver walk: a call was answered Unknown";
    /// A send waited on a batch that was not durable yet.
    pub const HELD: &str = "driver walk: a reply waited on a batch in the group";
    /// A leader answered a call busy.
    pub const BUSY: &str = "driver walk: a leader answered a call busy";
    /// A read served a page with a record.
    pub const READ_SERVED: &str = "driver walk: a read served a page with a record";
    /// An answer to a read of a driver before a restart reached the new
    /// driver of the process, with the id of a read of the new driver.
    pub const STALE_READ_ACK: &str = "driver walk: a stale read answer reached a restarted process";
    /// A register change reached one matchmaker only, and no answer came.
    pub const REGISTER_HALF: &str = "driver walk: a register write reached one matchmaker only";
    /// A register change waited in flight while other operations ran.
    pub const REGISTER_LATE: &str = "driver walk: a register write waited in flight";
    /// A register write in flight landed and was done.
    pub const REGISTER_LATE_DONE: &str = "driver walk: a register write in flight landed done";
    /// A register read repaired a write that a minority took.
    pub const REGISTER_REPAIRED: &str = "driver walk: a register read repaired a minority write";

    /// Every site of the walk.
    pub const ALL: &[&str] = &[
        GROUPED,
        FAILED,
        WRITTEN_HALTED,
        UNWRITTEN_HALTED,
        SCAN_HALTED,
        UNREADABLE,
        LIST_AGAIN,
        LOST,
        RESTARTED,
        DEPOSED,
        DUPLICATE,
        UNKNOWN,
        HELD,
        READ_SERVED,
        STALE_READ_ACK,
        BUSY,
        REGISTER_HALF,
        REGISTER_LATE,
        REGISTER_LATE_DONE,
        REGISTER_REPAIRED,
    ];
}

/// A bug planted in the walk's backends or client, never in the driver.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HostMutant {
    /// No bug.
    #[default]
    None,
    /// The disk says a commit is done and writes nothing. [`FLIGHT`]
    /// catches it.
    LyingDisk,
    /// A process restarts with an empty disk. [`AGREE`] catches it.
    WipeDisk,
    /// The client takes a `Taken` (a write at a position that holds
    /// another write) as written. [`ANSWER`] catches it.
    TakenAsWritten,
    /// The reader takes each record of a page one position later.
    /// [`READ`] catches it.
    PageShift,
}

const J: JournalId = JournalId(7);
const PROCESSES: [u64; 3] = [1, 2, 3];
const TICK: u64 = 1_000;
const CONFIG: HostConfig = HostConfig {
    tick: TICK,
    resend_page: 4,
    // One answer held: a call sent again while the first is still held is
    // busy.
    window: 1,
};
const A: LeaderUuid = LeaderUuid(0xA);

/// [`reach::GROUPED`] on each commit.
fn grouped(ops: &[Op]) {
    let roles: BTreeSet<Address> = ops.iter().map(|op| op.key.address).collect();
    sometimes!(roles.len() >= 2, reach::GROUPED);
}

type Proc = Driver<Disk, Wire, ManualClock, LossyRegister, ()>;

/// The ways a register change can go unanswered.
const FAULTS: [Fault; 4] = [Fault::Request, Fault::Reply, Fault::Half, Fault::Late];

/// The register sites on each change that got no answer.
fn lost_change(lost: Lost) {
    sometimes!(lost == Lost::Half, reach::REGISTER_HALF);
    sometimes!(lost == Lost::Late, reach::REGISTER_LATE);
}

/// [`reach::REGISTER_REPAIRED`] on each read that repaired a write a
/// minority took.
fn repaired() {
    sometimes!(true, reach::REGISTER_REPAIRED);
}

/// One process: its driver while it is up, its disk always.
struct Process {
    driver: Option<Proc>,
    disk: Disk,
    /// The register as this process reaches it.
    register: LossyRegister,
    inbox: Rc<RefCell<VecDeque<Envelope>>>,
    outbox: Rc<RefCell<Vec<Envelope>>>,
}

/// The writer's call in flight.
#[derive(Clone, Debug)]
struct Call {
    process: u64,
    id: CallId,
    command: Command,
    resent: bool,
}

/// A read in flight: the process, and the writer's next position when it
/// started.
#[derive(Clone, Copy, Debug)]
struct OpenRead {
    from: u64,
    need: u64,
}

/// The driver walk.
pub struct HostModel {
    mutant: HostMutant,
    clock: ManualClock,
    register: QuorumRegister,
    processes: BTreeMap<u64, Process>,
    round: u64,
    /// Every command a `Commit` on the wire named, by slot.
    chosen: BTreeMap<Slot, Command>,
    claimed: bool,
    next: u64,
    texts: u64,
    call: Option<Call>,
    /// A command whose answer was lost or `Unknown`: the writer sends it
    /// again.
    again: Option<Command>,
    /// The answers not judged yet: the command, the answer.
    answers: Vec<(Command, Answer)>,
    judged: usize,
    /// The reads in flight, by process and the id its driver gave.
    reads: BTreeMap<(u64, CallId), OpenRead>,
    /// The pages not judged yet: the read, and its records and view.
    pages: Vec<(OpenRead, ReadAnswer)>,
    /// The boot of the reads of each replica's incarnation, from its
    /// `PreRead`s on the wire; a crash forgets it.
    read_boots: BTreeMap<NodeId, PersistId>,
    /// The `PreReadAck`s that the network holds back: the move
    /// [`HostAction::Late`] delivers them, often after a restart of their
    /// replica. A late message is inside the contract of the transport.
    late: Vec<Envelope>,
    /// The process whose acceptor's disk lost a record: one at most, so a
    /// majority of the acceptors still runs.
    lost: Option<u64>,
}

/// The walk with `mutant` planted.
#[must_use]
pub fn walk(mutant: HostMutant, seeds: u64, steps: u64) -> Walk<HostModel> {
    Walk::<HostModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .env_prefix(None)
        .first_seed(crate::campaign::first_seed())
        .model(move |cx| HostModel::build(mutant, cx))
}

/// The moves of the driver walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostAction {
    /// A process runs one round of its driver.
    Poll,
    /// The clock moves one tick.
    Tick,
    /// A process runs a new leader.
    Campaign,
    /// The writer calls.
    Write,
    /// The reader reads at the replica of a process.
    Read,
    /// The network delivers the `PreReadAck`s that it held back.
    Late,
    /// The next commit of a process fails, or its next register change
    /// goes unanswered.
    Fault,
    /// One register write in flight lands at the members that are up.
    Land,
}

impl Model for HostModel {
    type Msg = Envelope;
    type Node = u64;
    type Action = HostAction;
    type Event = ();

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(HostMutant::None, cx)
    }

    fn actions() -> &'static [HostAction] {
        &[
            HostAction::Poll,
            HostAction::Tick,
            HostAction::Campaign,
            HostAction::Write,
            HostAction::Read,
            HostAction::Late,
            HostAction::Fault,
            HostAction::Land,
        ]
    }

    fn weight(action: HostAction, phase: Phase) -> u32 {
        match (action, phase) {
            (HostAction::Fault | HostAction::Campaign, Phase::Chaos)
            | (HostAction::Late | HostAction::Land, _) => 2,
            (HostAction::Fault, _) | (HostAction::Read, Phase::Settle) => 0,
            (HostAction::Poll | HostAction::Tick | HostAction::Write, Phase::Settle)
            | (HostAction::Campaign, _) => 1,
            (HostAction::Poll, _) => 30,
            (HostAction::Tick, _) => 5,
            (HostAction::Write, _) => 10,
            (HostAction::Read, _) => 6,
        }
    }

    fn act(&mut self, action: HostAction, cx: &mut Cx<'_, Self>) {
        match action {
            HostAction::Poll => self.poll(cx),
            HostAction::Tick => self.clock.advance(TICK),
            HostAction::Campaign => self.campaign(cx),
            HostAction::Write => self.write(cx),
            HostAction::Read => self.read(cx),
            HostAction::Late => self.late(cx),
            HostAction::Fault => self.fault(cx),
            HostAction::Land => self.land(cx),
        }
    }

    fn deliver(&mut self, envelope: Envelope, _cx: &mut Cx<'_, Self>) {
        if let Message::PreReadAck { read, .. } = &envelope.message {
            sometimes!(
                self.read_boots
                    .get(&envelope.to)
                    .is_some_and(|boot| read.boot < *boot),
                reach::STALE_READ_ACK
            );
        }
        let process = Self::process_of(envelope.to);
        if let Some(process) = self.processes.get(&process)
            && process.driver.is_some()
        {
            process.inbox.borrow_mut().push_back(envelope);
        }
    }

    fn nodes(&self) -> Vec<u64> {
        PROCESSES.to_vec()
    }

    fn is_up(&self, node: &u64) -> bool {
        self.processes[node].driver.is_some()
    }

    fn crash(&mut self, node: &u64, _cx: &mut Cx<'_, Self>) {
        let process = self.processes.get_mut(node).expect("a process");
        process.driver = None;
        self.register.set_up(Self::matchmaker(*node), false);
        process.inbox.borrow_mut().clear();
        process.outbox.borrow_mut().clear();
        if self.call.as_ref().is_some_and(|c| c.process == *node) {
            // The caller times out: the fate of its call is unknown.
            let call = self.call.take().expect("a call");
            self.again = Some(call.command);
        }
        // The reader of a crashed process times out.
        self.reads.retain(|(p, _), _| p != node);
        self.read_boots.remove(&NodeId(10 + node));
    }

    fn restart(&mut self, node: &u64, _cx: &mut Cx<'_, Self>) {
        if self.processes[node].driver.is_some() {
            return;
        }
        if self.mutant == HostMutant::WipeDisk {
            self.processes[node].disk.wipe();
        }
        self.start(*node);
        let driver = self.processes[node].driver.as_ref().expect("started");
        let acceptor = driver.host().acceptor(Self::acceptor(*node));
        sometimes!(
            acceptor.is_some_and(|a| a.vote(Slot(1)).is_some()),
            reach::RESTARTED
        );
    }

    fn check(&mut self) {
        self.judge();
    }

    fn converged(&self) -> Result<(), String> {
        if self.register.in_flight() > 0 {
            return Err(format!(
                "{} register writes are in flight",
                self.register.in_flight()
            ));
        }
        if self.call.is_some() || self.again.is_some() {
            return Err("the writer has a call in flight".into());
        }
        if !self.claimed {
            return Err("the writer has not claimed the journal".into());
        }
        if self.judged < self.answers.len() {
            return Err(format!(
                "{} answers are not judged",
                self.answers.len() - self.judged
            ));
        }
        if !self.reads.is_empty() || !self.pages.is_empty() {
            return Err(format!(
                "{} reads are open and {} pages are not judged",
                self.reads.len(),
                self.pages.len()
            ));
        }
        if self.open_leader().is_none() {
            return Err("no leader writes".into());
        }
        let frontier = self.frontier();
        for p in PROCESSES {
            let front = self.processes[&p]
                .driver
                .as_ref()
                .and_then(|d| d.host().replica(Self::replica(p)))
                .and_then(paros_journal::roles::SegmentReplica::front);
            if front != Some(frontier) {
                return Err(format!(
                    "replica {} is at {front:?}, the log at {frontier:?}",
                    10 + p
                ));
            }
        }
        Ok(())
    }

    fn describe(&self) -> String {
        format!(
            "chosen to {:?}, {} answers judged of {}, call {:?}, again {:?}",
            self.frontier(),
            self.judged,
            self.answers.len(),
            self.call,
            self.again
        )
    }
}

impl HostModel {
    /// The walk with `mutant` planted: segment 1 open, every process up.
    ///
    /// # Panics
    ///
    /// When the first segment cannot open.
    pub fn build(mutant: HostMutant, cx: &mut Cx<'_, Self>) -> Self {
        let register = QuorumRegister::new(|_| {}, repaired, cx.rng().next_u64());
        let value = RegisterValue::new()
            .apply(&Change::Elect { holder: NodeId(0) })
            .expect("an election");
        let value = value
            .apply(&Change::Reshape {
                opening: OpeningCall {
                    term: value.term().number,
                    expected: None,
                    grid: Grid::new(1, vec![PROCESSES.map(NodeId).to_vec()]).expect("a grid"),
                    replicas: PROCESSES.map(|p| NodeId(10 + p)).to_vec(),
                },
                release: RELEASE,
            })
            .expect("segment 1 opens");
        register.create(J, value);
        let mut model = Self {
            mutant,
            clock: ManualClock::new(),
            register,
            processes: BTreeMap::new(),
            round: 0,
            chosen: BTreeMap::new(),
            claimed: false,
            next: 0,
            texts: 0,
            call: None,
            again: None,
            answers: Vec::new(),
            judged: 0,
            reads: BTreeMap::new(),
            pages: Vec::new(),
            read_boots: BTreeMap::new(),
            late: Vec::new(),
            lost: None,
        };
        for p in PROCESSES {
            model.processes.insert(
                p,
                Process {
                    driver: None,
                    disk: Disk::new(grouped),
                    register: LossyRegister::new(model.register.clone(), lost_change),
                    inbox: Rc::default(),
                    outbox: Rc::default(),
                },
            );
            model.start(p);
        }
        model
    }

    fn acceptor(p: u64) -> Address {
        Address::new(J, NodeId(p))
    }

    fn replica(p: u64) -> Address {
        Address::new(J, NodeId(10 + p))
    }

    fn leader(p: u64) -> Address {
        Address::new(J, NodeId(100 + p))
    }

    /// The matchmaker that process `p` hosts.
    fn matchmaker(p: u64) -> MatchmakerId {
        MatchmakerId(p)
    }

    fn process_of(node: NodeId) -> u64 {
        node.0 % 10
    }

    fn assignment(address: Address) -> Assignment {
        if address.node.0 < 10 {
            Assignment::Acceptor {
                address,
                page: paros_journal::roles::PROMISE_BATCH,
            }
        } else {
            Assignment::Replica {
                address,
                segment: SegmentId(1),
                set: PROCESSES.map(|p| NodeId(10 + p)).to_vec(),
                initial: Leader::Unset,
            }
        }
    }

    /// A new driver for process `p` on its disk: it serves the acceptor and
    /// the replica again.
    fn start(&mut self, p: u64) {
        self.register.set_up(Self::matchmaker(p), true);
        let process = self.processes.get_mut(&p).expect("a process");
        let wire = Wire {
            inbox: Rc::clone(&process.inbox),
            outbox: Rc::clone(&process.outbox),
            disk: process.disk.clone(),
            flight: FLIGHT,
        };
        let mut driver = Driver::new(
            CONFIG,
            process.disk.clone(),
            wire,
            self.clock.clone(),
            process.register.clone(),
            (),
        );
        // A process that boots lists its disk first. A list that read
        // nothing (`Full`) may be asked again.
        let listed = match driver.on_disk() {
            Err(paros_journal_io::StorageError::Full) => {
                sometimes!(true, reach::LIST_AGAIN);
                driver.on_disk()
            }
            listed => listed,
        };
        listed.expect("a list asked again after a list that read nothing reads the disk");
        driver.assign(Self::assignment(Self::acceptor(p)));
        driver.assign(Self::assignment(Self::replica(p)));
        process.driver = Some(driver);
    }

    fn up(&self) -> Vec<u64> {
        PROCESSES
            .into_iter()
            .filter(|p| self.processes[p].driver.is_some())
            .collect()
    }

    fn open_leader(&self) -> Option<u64> {
        PROCESSES.into_iter().find(|p| {
            self.processes[p]
                .driver
                .as_ref()
                .and_then(|d| d.host().leader(Self::leader(*p)))
                .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
        })
    }

    fn frontier(&self) -> Slot {
        let mut slot = Slot(0);
        while self.chosen.contains_key(&slot) {
            slot = Slot(slot.0 + 1);
        }
        slot
    }

    /// Poll one process (every process in the settle phase), then move its
    /// sends into the mailbox and read its replies.
    fn poll(&mut self, cx: &mut Cx<'_, Self>) {
        let up = self.up();
        let picked: Vec<u64> = if cx.phase() == Phase::Settle {
            up
        } else {
            cx.rng().pick(&up).copied().into_iter().collect()
        };
        for p in picked {
            let process = self.processes.get_mut(&p).expect("a process");
            let Some(driver) = process.driver.as_mut() else {
                continue;
            };
            let busy = driver.poll();
            sometimes!(
                busy && !process.outbox.borrow().is_empty() && process.inbox.borrow().is_empty(),
                reach::HELD
            );
            let stopped = driver.take_stopped();
            let replies = driver.take_replies();
            let reads = driver.take_reads();
            // Placement serves again a role that a commit stopped when it
            // wrote nothing, or a scan when it read nothing. A role whose disk
            // lost a record stops for good.
            for (address, why) in &stopped {
                match why {
                    paros_journal_io::Stop::Failed => {
                        sometimes!(true, reach::FAILED);
                        driver.assign(Self::assignment(*address));
                    }
                    paros_journal_io::Stop::Unreadable => {
                        assert_eq!(
                            process.disk.take_scan_failed(),
                            Some(paros_journal_io::StorageError::Full),
                            "a role stops unreadable only on a scan that read nothing"
                        );
                        sometimes!(true, reach::UNREADABLE);
                        driver.assign(Self::assignment(*address));
                    }
                    paros_journal_io::Stop::Lost => sometimes!(true, reach::LOST),
                    _ => {}
                }
            }
            // A commit of unknown outcome stops the process: what it sent
            // and answered before the commit goes out, then the walk crashes
            // it, and a restart reads the disk again.
            let halted = driver.halted();
            if halted {
                if process.disk.take_scan_failed() == Some(paros_journal_io::StorageError::Unknown)
                {
                    sometimes!(true, reach::SCAN_HALTED);
                } else {
                    let wrote = process.disk.wrote_unknown();
                    sometimes!(wrote, reach::WRITTEN_HALTED);
                    sometimes!(!wrote, reach::UNWRITTEN_HALTED);
                }
            }
            let sent: Vec<Envelope> = process.outbox.borrow_mut().drain(..).collect();
            for envelope in sent {
                self.on_wire(&envelope);
                // The network holds back half of the read answers.
                if matches!(envelope.message, Message::PreReadAck { .. })
                    && cx.phase() == Phase::Chaos
                    && cx.rng().chance(1, 2)
                {
                    self.late.push(envelope);
                } else {
                    cx.send(envelope);
                }
            }
            for reply in replies {
                self.on_reply(p, reply.call, reply.outcome);
            }
            for reply in reads {
                self.on_read(p, reply.read, reply.answer);
            }
            if halted {
                self.crash(&p, cx);
            }
        }
    }

    /// The reader's answer to the read `id` of process `p`: a page waits
    /// for [`READ`], any other answer ends the read.
    fn on_read(&mut self, p: u64, id: CallId, answer: ReadAnswer) {
        let Some(open) = self.reads.remove(&(p, id)) else {
            return;
        };
        if let ReadAnswer::Page {
            mut records,
            view,
            sealed,
        } = answer
        {
            if self.mutant == HostMutant::PageShift {
                for (seq, _) in &mut records {
                    *seq = Seq(seq.0 + 1);
                }
            }
            self.pages.push((
                open,
                ReadAnswer::Page {
                    records,
                    view,
                    sealed,
                },
            ));
        }
    }

    /// A read at the replica of a process that is up, at the writer's next
    /// position or below it.
    fn read(&mut self, cx: &mut Cx<'_, Self>) {
        let up = self.up();
        let Some(p) = cx.rng().pick(&up).copied() else {
            return;
        };
        let from = if cx.rng().chance(1, 2) {
            self.next
        } else {
            cx.rng().below(self.next + 1)
        };
        let process = self.processes.get_mut(&p).expect("a process");
        let driver = process.driver.as_mut().expect("up");
        let replica = Address::replica(J, NodeId(10 + p), SegmentId(1));
        let id = driver.read(replica, Seq(from), 4);
        self.reads.insert(
            (p, id),
            OpenRead {
                from,
                need: self.next,
            },
        );
    }

    /// The network delivers each read answer that it held back, with odds
    /// of one half in the chaos phase. A late message is inside the contract
    /// of the transport, and an answer held across a restart of its replica
    /// reaches the next incarnation.
    fn late(&mut self, cx: &mut Cx<'_, Self>) {
        let chaos = cx.phase() == Phase::Chaos;
        for envelope in std::mem::take(&mut self.late) {
            if !chaos || cx.rng().chance(1, 2) {
                cx.send(envelope);
            } else {
                self.late.push(envelope);
            }
        }
    }

    /// [`AGREE`] on every `Commit` that leaves, and the boot of each
    /// replica's reads.
    fn on_wire(&mut self, envelope: &Envelope) {
        if let Message::PreRead { read, .. } = &envelope.message {
            self.read_boots.insert(envelope.from, read.boot);
        }
        if let Message::Commit { slot, command, .. } = &envelope.message {
            let held = self.chosen.entry(*slot).or_insert_with(|| command.clone());
            assert!(
                held == command,
                "{AGREE}: slot {slot:?} chose {held:?} and {command:?}"
            );
        }
    }

    fn on_reply(&mut self, p: u64, id: CallId, outcome: CallOutcome) {
        let Some(call) = self.call.take_if(|c| c.process == p && c.id == id) else {
            return;
        };
        match outcome {
            CallOutcome::NotLeader => self.again = Some(call.command),
            CallOutcome::Busy => {
                sometimes!(true, reach::BUSY);
                self.again = Some(call.command);
            }
            CallOutcome::Reply(Reply::Unknown) => {
                sometimes!(true, reach::UNKNOWN);
                self.again = Some(call.command);
            }
            CallOutcome::Reply(Reply::Answer(answer)) => {
                sometimes!(
                    call.resent && matches!(answer, Answer::Duplicate { .. }),
                    reach::DUPLICATE
                );
                let answer = match (self.mutant, answer, &call.command) {
                    (
                        HostMutant::TakenAsWritten,
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
                match &answer {
                    Answer::Written { seq, count } | Answer::Duplicate { seq, count } => {
                        self.next = self.next.max(seq.0 + count);
                    }
                    Answer::LeaderSet(view)
                    | Answer::LeaderRefused(view)
                    | Answer::Taken(view)
                    | Answer::Early(view)
                    | Answer::Fenced(view) => {
                        self.claimed |= view.leader == Leader::Uuid(A);
                        self.next = self.next.max(view.next_seq.0);
                    }
                    _ => {}
                }
                self.answers.push((call.command, answer));
            }
        }
    }

    /// A new leader on a process that is up, at a new ballot.
    fn campaign(&mut self, cx: &mut Cx<'_, Self>) {
        let open = self.open_leader();
        if cx.phase() != Phase::Chaos && open.is_some() {
            return;
        }
        let up = self.up();
        let Some(p) = cx.rng().pick(&up).copied() else {
            return;
        };
        sometimes!(open.is_some(), reach::DEPOSED);
        self.round += 1;
        let address = Self::leader(p);
        let process = self.processes.get_mut(&p).expect("a process");
        let driver = process.driver.as_mut().expect("up");
        driver.unassign(address);
        let mut lost = driver.take_replies();
        driver
            .transport_mut()
            .inbox
            .borrow_mut()
            .retain(|e| e.to != address.node);
        driver.assign(Assignment::Leader {
            address,
            proxies: Vec::new(),
            campaign: Campaign {
                ballot: Ballot {
                    round: self.round,
                    node: address.node,
                },
                from: Slot(0),
            },
        });
        lost.extend(driver.take_replies());
        let sent: Vec<Envelope> = process.outbox.borrow_mut().drain(..).collect();
        for envelope in sent {
            self.on_wire(&envelope);
            cx.send(envelope);
        }
        for reply in lost {
            self.on_reply(p, reply.call, reply.outcome);
        }
    }

    /// The writer's next call, to a leader that writes.
    fn write(&mut self, cx: &mut Cx<'_, Self>) {
        if self.call.is_some() {
            // The caller times out while its call is held, and sends it
            // again: the fate of the first call is unknown, and a leader
            // whose window is full answers the second busy.
            if cx.phase() != Phase::Chaos || !cx.rng().chance(1, 4) {
                return;
            }
            let call = self.call.take().expect("a call");
            self.again = Some(call.command);
        }
        let Some(p) = self.open_leader() else {
            return;
        };
        let (command, resent) = if let Some(command) = self.again.take() {
            (command, true)
        } else if cx.phase() == Phase::Settle && self.claimed {
            return;
        } else if !self.claimed {
            (
                Command::SetLeader {
                    new: Leader::uuid(A),
                    old: Leader::Unset,
                },
                false,
            )
        } else {
            self.texts += 1;
            let at = if cx.rng().chance(1, 8) {
                self.next.saturating_sub(1)
            } else {
                self.next
            };
            let command = Command::Write(Write {
                fence: Fence::Leader {
                    uuid: A,
                    expected_seq: Seq(at),
                },
                records: vec![Record::new(self.texts.to_be_bytes().to_vec())],
            });
            (command, false)
        };
        let process = self.processes.get_mut(&p).expect("a process");
        let driver = process.driver.as_mut().expect("up");
        let id = driver.call(Self::leader(p), command.clone());
        self.call = Some(Call {
            process: p,
            id,
            command,
            resent,
        });
        let replies = driver.take_replies();
        let sent: Vec<Envelope> = process.outbox.borrow_mut().drain(..).collect();
        for envelope in sent {
            self.on_wire(&envelope);
            cx.send(envelope);
        }
        for reply in replies {
            self.on_reply(p, reply.call, reply.outcome);
        }
    }

    /// Arm a commit fault on a process.
    fn fault(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(p) = cx.rng().pick(&PROCESSES).copied() else {
            return;
        };
        let disk = &self.processes[&p].disk;
        if self.mutant == HostMutant::LyingDisk {
            disk.arm_lie();
        } else if self.lost.is_none()
            && self.processes[&p].driver.is_none()
            && cx.rng().chance(1, 2)
        {
            // The disk of a process that is down loses its acceptor's
            // promise: the restart finds the loss, and the acceptor stops for
            // good.
            disk.lose(Key::new(Self::acceptor(p), Kind::Promise, 0));
            self.lost = Some(p);
        } else if cx.rng().chance(1, 3) {
            let fault = *cx.rng().pick(&FAULTS).expect("four ways");
            self.processes[&p].register.arm(fault);
        } else {
            disk.arm(*cx.rng().pick(&DiskFault::ALL).expect("six faults"));
        }
    }

    /// One register write in flight lands, at the members that are up.
    fn land(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(pick) = cx.rng().index(self.register.in_flight()) else {
            return;
        };
        let landed = self.register.land(pick);
        sometimes!(landed == Landed::Done, reach::REGISTER_LATE_DONE);
    }

    /// [`ANSWER`]: judge each answer whose command the chosen prefix holds.
    fn judge(&mut self) {
        let frontier = self.frontier();
        let mut truth = Truth::new(Leader::Unset);
        let mut states: Vec<(u64, &'static str, &Command)> = Vec::new();
        let mut records: BTreeMap<u64, Record> = BTreeMap::new();
        for slot in 0..frontier.0 {
            let command = &self.chosen[&Slot(slot)];
            let before = truth.next;
            let verdict = truth.apply(command);
            if let (Command::Write(write), "ok") = (command, verdict) {
                for (at, record) in (before..).zip(&write.records) {
                    records.insert(at, record.clone());
                }
            }
            states.push((before, verdict, command));
        }
        while let Some((command, answer)) = self.answers.get(self.judged) {
            let at: Vec<&(u64, &str, &Command)> = states
                .iter()
                .filter(|(_, _, c)| as_written(c) == as_written(command))
                .collect();
            if at.is_empty() {
                break;
            }
            let fits = at.iter().any(|(before, verdict, _)| {
                *verdict == label(answer)
                    && match answer {
                        Answer::Written { seq, .. } => seq.0 == *before,
                        _ => true,
                    }
            });
            assert!(
                fits,
                "{ANSWER}: {command:?} answered {answer:?}, the whole log gives {:?}",
                at.iter().map(|(b, v, _)| (*b, *v)).collect::<Vec<_>>()
            );
            self.judged += 1;
        }
        self.judge_pages(&records);
    }

    /// [`READ`]: each page holds the whole log's records at their
    /// positions, and its view covers the writer's next position when the
    /// read started.
    fn judge_pages(&mut self, records: &BTreeMap<u64, Record>) {
        for (open, answer) in std::mem::take(&mut self.pages) {
            let ReadAnswer::Page {
                records: page,
                view,
                ..
            } = &answer
            else {
                unreachable!("only pages wait");
            };
            assert!(
                view.next_seq.0 >= open.need,
                "{READ}: a read from {} served {view:?}, the writer had position {} when it started",
                open.from,
                open.need
            );
            sometimes!(!page.is_empty(), reach::READ_SERVED);
            for (seq, record) in page {
                assert!(
                    seq.0 >= open.from,
                    "{READ}: a read from {} served position {seq:?}",
                    open.from
                );
                let whole = records.get(&seq.0).map(|r| (&r.payload, &r.headers.client));
                assert!(
                    whole == Some((&record.payload, &record.headers.client)),
                    "{READ}: a read served {record:?} at {seq:?}, the whole log has {whole:?}"
                );
            }
        }
    }
}
