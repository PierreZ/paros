//! The helpers that the examples of `paros-journal-io` share: the roles of
//! one journal, the register of its first segment, the client calls and
//! the loop that drives the processes.
//!
//! This is Cargo's place for code shared by examples: a file
//! `examples/common.rs` would be built as an example of its own.

#![allow(dead_code)]

use paros_journal::client::{CallId as ClientCall, Client, Outcome, Request, RequestId, Response};
use paros_journal::register::{Ballot, Change, RegisterValue};
use paros_journal::resolve::ResolveAnswer;
use paros_journal::roles::{Campaign, LeaderPhase};
use paros_journal::{
    Answer, Command, Fence, Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Record, Reply,
    SegmentId, Seq, Slot, Write,
};
use paros_journal_io::mem::{Endpoint, ManualClock, MemNetwork, MemRegister, MemStorage};
use paros_journal_io::{
    Address, Assignment, CallId, CallOutcome, CallReply, Clock, Driver, Events, HostConfig,
    JournalId, Register, Storage, Transport,
};

/// The journal of the examples.
pub const JOURNAL: JournalId = JournalId(1);
/// The nanoseconds between two periodic steps.
pub const TICK: u64 = 1_000_000;
/// The acceptors of every grid: one column of three (`F = 1`).
pub const ACCEPTORS: [u64; 3] = [1, 2, 3];
/// The writer.
pub const A: LeaderUuid = LeaderUuid(0xA);

/// The role `node` of the journal.
pub fn at(node: u64) -> Address {
    Address::new(JOURNAL, NodeId(node))
}

/// The replica set of segment `id`: `10 * id + 1 ..= 10 * id + 3`.
pub fn replicas(id: u64) -> Vec<NodeId> {
    (1..=3).map(|i| NodeId(10 * id + i)).collect()
}

/// The register after the first election of the controller.
pub fn elected() -> RegisterValue {
    RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("the first election of the controller")
}

/// The controller's reshape that opens segment `id` after `expected`.
pub fn reshape(value: &RegisterValue, id: u64) -> Change {
    let expected = (id > 1).then(|| SegmentId(id - 1));
    Change::Reshape {
        opening: OpeningCall {
            term: value.term().number,
            expected,
            grid: Grid::new(1, vec![ACCEPTORS.map(NodeId).to_vec()]).expect("a v1 grid"),
            replicas: replicas(id),
        },
        release: RELEASE,
    }
}

/// The assignment of an acceptor.
pub fn acceptor(node: u64) -> Assignment {
    Assignment::Acceptor {
        address: at(node),
        page: 16,
    }
}

/// The assignment of a replica of segment `id`.
pub fn replica(node: u64, id: u64) -> Assignment {
    Assignment::Replica {
        address: at(node),
        segment: SegmentId(id),
        set: replicas(id),
        initial: Leader::Unset,
    }
}

/// The assignment of a leader at `round`, bound to segment `id`, that
/// reads from `from`.
pub fn leader(node: u64, round: u64, id: u64, from: Slot) -> Assignment {
    Assignment::Leader {
        address: at(node),
        proxies: Vec::new(),
        campaign: Campaign {
            ballot: Ballot {
                round,
                node: NodeId(node),
            },
            segment: SegmentId(id),
            from,
        },
    }
}

/// Whether the leader at `node` writes.
pub fn open<S, T, C, R, E>(driver: &Driver<S, T, C, R, E>, node: u64) -> bool
where
    S: Storage,
    T: Transport,
    C: Clock,
    R: Register,
    E: Events,
{
    driver
        .host()
        .leader(at(node))
        .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
}

/// A single-writer write of `text` by writer A at `seq`.
pub fn write(seq: u64, text: &str) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: A,
            expected_seq: Seq(seq),
        },
        records: vec![Record::new(text.as_bytes().to_vec())],
    })
}

/// Writer A takes the journal.
pub fn claim() -> Command {
    Command::SetLeader {
        new: Some(A),
        old: Leader::Unset,
    }
}

/// The answer of a write of one record at `seq`.
pub fn written(seq: u64) -> CallOutcome {
    CallOutcome::Reply(Reply::Answer(Answer::Written {
        seq: Seq(seq),
        count: 1,
    }))
}

/// The answer of `call` among `replies`, if it came.
pub fn reply_of(replies: &[CallReply], call: CallId) -> Option<&CallOutcome> {
    replies.iter().find(|r| r.call == call).map(|r| &r.outcome)
}

/// Print one step of an example: what was asked, and what came back.
pub fn show(what: &str, answer: &impl std::fmt::Debug) {
    println!("  {what:<46} -> {answer:?}");
}

/// One process that serves every role of the journal, on the backends of
/// `paros_journal_io::mem`: three acceptors and the replicas of segment 1
/// at first, one leader at a time.
pub struct OneProcess {
    /// The driver.
    pub process: Driver<MemStorage, Endpoint, ManualClock, MemRegister, ()>,
    /// The clock, which the loop moves one tick at a time.
    pub clock: ManualClock,
    /// The journal's register, shared with the driver.
    pub register: MemRegister,
}

impl OneProcess {
    /// Segment 1 opens, with leader `leader` at round 1.
    pub fn new(leader_node: u64) -> Self {
        let clock = ManualClock::new();
        let register = MemRegister::new();
        register.create(JOURNAL, elected());
        let mut journal = Self {
            process: Driver::new(
                HostConfig {
                    tick: TICK,
                    resend_page: 16,
                },
                MemStorage::new(),
                MemNetwork::new().endpoint(),
                clock.clone(),
                register.clone(),
                (),
            ),
            clock,
            register,
        };
        journal.open_segment(1);
        for node in 1..=3 {
            journal.assign(acceptor(node));
            journal.assign(replica(10 + node, 1));
        }
        journal.assign(leader(leader_node, 1, 1, Slot(0)));
        journal
    }

    /// Serve `role`.
    pub fn assign(&mut self, role: Assignment) {
        self.process.transport_mut().route(role.address());
        self.process.assign(role);
    }

    /// The controller opens segment `id`.
    pub fn open_segment(&mut self, id: u64) {
        let value = self.register.read(JOURNAL).expect("the register");
        self.register
            .change(JOURNAL, &reshape(&value, id))
            .expect("the reshape");
    }

    /// The controller opens segment `id`, the old leader `old` goes, and
    /// leader `new` at round `id` seals the segment before.
    pub fn reshape(&mut self, id: u64, old: u64, new: u64) {
        self.open_segment(id);
        for node in replicas(id) {
            self.assign(replica(node.0, id));
        }
        self.process.unassign(at(old));
        // The new leader reads from the applied prefix of the segment before.
        let from = replicas(id - 1)
            .iter()
            .filter_map(|node| self.process.host().replica(at(node.0))?.front())
            .max()
            .expect("a replica of the segment before");
        self.assign(leader(new, id, id, from));
    }

    /// Poll until the process has no work: the answers of the calls.
    pub fn settle(&mut self) -> Vec<CallReply> {
        while self.process.poll() {}
        self.process.take_replies()
    }

    /// The fold of replica `node`.
    pub fn fold(&self, node: u64) -> paros_journal::JournalState {
        *self
            .process
            .host()
            .replica(at(node))
            .and_then(|r| r.state())
            .expect("a replica with its base")
    }
}

/// A program of one writer: a [`Client`] of `paros_journal::client`, and
/// the glue that sends its requests to the process and gives it back the
/// answers. This glue is all a program writes: the client settles every
/// ambiguous answer itself.
pub struct Program {
    /// The client.
    pub client: Client,
    /// The leader the program sends commands to.
    pub leader: u64,
    /// How many `Resolve` calls the program sent.
    pub resolves: usize,
    /// The commands in flight: the driver's call id, and the client's
    /// request id.
    calls: Vec<(CallId, RequestId)>,
    outcomes: Vec<(ClientCall, Outcome)>,
}

impl Program {
    /// A program whose commands go to leader `leader`.
    pub fn new(leader: u64) -> Self {
        Self {
            client: Client::new(),
            leader,
            resolves: 0,
            calls: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    /// Send what the client asks. A command goes to the leader. A `Resolve`
    /// goes to the replicas of the segments in turn, here in the same
    /// process, until one covers the position.
    pub fn send(&mut self, journal: &mut OneProcess) {
        for request in self.client.take_requests() {
            match request {
                Request::Command { id, command } => {
                    let call = journal.process.call(at(self.leader), command);
                    self.calls.push((call, id));
                }
                Request::Resolve {
                    id,
                    uuid,
                    expected_seq,
                    records,
                } => {
                    self.resolves += 1;
                    let answer = (1..)
                        .map_while(|segment| {
                            let node = replicas(segment)[0].0;
                            journal.process.host().replica(at(node))
                        })
                        // A program sends after `settle`, when the process
                        // has no work left: the batch an answer waits on is
                        // durable.
                        .map(|r| r.resolve(uuid, expected_seq, &records).0)
                        .find(|a| *a != ResolveAnswer::NotCovered)
                        .unwrap_or(ResolveAnswer::NotCovered);
                    self.client.answer(id, Response::Resolved(answer));
                }
                Request::Floor { .. } | Request::Read { .. } => {
                    unreachable!("these examples do not read")
                }
            }
        }
        self.take();
    }

    /// Give the client the answers of its calls among `replies`.
    pub fn receive(&mut self, replies: &[CallReply]) {
        for reply in replies {
            if let Some(i) = self.calls.iter().position(|(c, _)| *c == reply.call) {
                let (_, id) = self.calls.remove(i);
                let response = match &reply.outcome {
                    CallOutcome::Reply(reply) => Response::Reply(reply.clone()),
                    CallOutcome::NotLeader => Response::NotLeader,
                };
                self.client.answer(id, response);
            }
        }
        self.take();
    }

    /// The program stops waiting for its commands in flight: their answers
    /// are lost.
    pub fn lose(&mut self) {
        for (_, id) in std::mem::take(&mut self.calls) {
            self.client.answer(id, Response::Lost);
        }
        self.take();
    }

    fn take(&mut self) {
        self.outcomes.extend(self.client.take_outcomes());
    }

    /// The outcome of `call`, once it ended.
    pub fn outcome(&mut self, call: ClientCall) -> Option<Outcome> {
        let i = self.outcomes.iter().position(|(c, _)| *c == call)?;
        Some(self.outcomes.remove(i).1)
    }

    /// Send, poll and tick until `call` ends: its outcome.
    pub fn run(&mut self, journal: &mut OneProcess, call: ClientCall) -> Outcome {
        for _ in 0..100 {
            self.send(journal);
            let replies = journal.settle();
            self.receive(&replies);
            if let Some(outcome) = self.outcome(call) {
                return outcome;
            }
            journal.clock.advance(TICK);
        }
        panic!("{call:?} did not end in 100 ticks");
    }
}
