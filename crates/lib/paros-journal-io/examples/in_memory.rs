//! **A journal on three processes, on the traits of `paros-journal-io`.**
//!
//! Run it: `cargo run -p paros-journal-io --example in_memory`
//!
//! The journal roles are pure state machines. A program runs them with the
//! generic [`Driver`], and brings the four traits that it needs. Here each
//! one is a backend in memory from `paros_journal_io::mem`, written with the
//! standard library alone, with no runtime crate:
//!
//! - **Storage** ([`MemStorage`]): one for each process. The driver writes
//!   the roles' batches in the format of `paros_journal_io::layout`, and a
//!   role that starts reads its records back.
//! - **Transport** ([`MemNetwork`]): one endpoint for each process. Every
//!   message goes through it, between two roles of one process too.
//! - **Clock** ([`ManualClock`]): the example moves it one tick at a time.
//! - **Register** ([`MemRegister`]): the journal's compare-and-set register.
//!
//! The driver keeps the **flight rule** (`docs/journal/04-roles.md` §J4.2):
//! it commits the batches of a round in one write, and sends a message only
//! once the batch that it waits on is durable.
//!
//! # What the run shows
//!
//! 1. The controller opens segment 1: one column of three acceptors and
//!    three replicas, one of each on each process. Leader L1 runs Phase 1.
//! 2. Writer A takes the journal and writes two records.
//! 3. Process 2 crashes. A write is chosen at acceptors 1 and 3, `F + 1` of
//!    the column. Process 2 starts again: a new driver reads its acceptor
//!    and its replica back from its storage.
//! 4. The controller opens segment 2. Leader L2, on process 3, runs Phase 1
//!    over both segments and seals segment 1 at one above the highest vote
//!    it saw. The replicas of segment 1 write its boundary state, and the
//!    replicas of segment 2 fold from it.
//! 5. Writer A writes again, in segment 2, at the next position.
//!
//! Each step asserts its result, so CI runs this example as a test.

mod common;

use paros_journal::register::Ballot;
use paros_journal::roles::{JournalLeader, LeaderPhase};
use paros_journal::{Answer, Leader, NodeId, Reply, SegmentId, Seq, Slot};
use paros_journal_io::mem::{Endpoint, ManualClock, MemNetwork, MemRegister, MemStorage};
use paros_journal_io::{
    Assignment, CallId, CallOutcome, CallReply, Driver, HostConfig, Register, Storage,
};

use common::{
    A, JOURNAL, TICK, acceptor, at, claim, elected, leader, open, replica, replicas, reply_of,
    reshape, show, write, written,
};

type Process = Driver<MemStorage, Endpoint, ManualClock, MemRegister, ()>;

const CONFIG: HostConfig = HostConfig {
    tick: TICK,
    resend_page: 16,
    window: 64,
};
/// Leader L1 on process 1, then leader L2 on process 3.
const L1: u64 = 100;
const L2: u64 = 200;

/// Three processes on one network, one clock and one register.
struct Cluster {
    network: MemNetwork,
    clock: ManualClock,
    register: MemRegister,
    processes: Vec<Option<Process>>,
    replies: Vec<CallReply>,
}

impl Cluster {
    fn new() -> Self {
        let register = MemRegister::new();
        register.create(JOURNAL, elected());
        Self {
            network: MemNetwork::new(),
            clock: ManualClock::new(),
            register,
            processes: vec![None, None, None],
            replies: Vec::new(),
        }
    }

    /// Process `i` starts on `storage`, and serves `roles`.
    fn start(&mut self, i: usize, storage: MemStorage, roles: Vec<Assignment>) {
        let mut process = Driver::new(
            CONFIG,
            storage,
            self.network.endpoint(),
            self.clock.clone(),
            self.register.clone(),
            (),
        );
        for role in roles {
            process.transport_mut().route(role.address());
            process.assign(role);
        }
        self.processes[i] = Some(process);
    }

    fn process(&mut self, i: usize) -> &mut Process {
        self.processes[i].as_mut().expect("a running process")
    }

    /// Process `i` also serves `role`.
    fn assign(&mut self, i: usize, role: Assignment) {
        let process = self.process(i);
        process.transport_mut().route(role.address());
        process.assign(role);
    }

    /// Process `i` crashes: its driver is gone, its storage stays.
    fn crash(&mut self, i: usize) -> MemStorage {
        let process = self.processes[i].take().expect("a running process");
        process.into_parts().0
    }

    /// The controller opens the next segment.
    fn open_segment(&mut self, id: u64) {
        let value = self.register.read(JOURNAL).expect("the register");
        self.register
            .change(JOURNAL, &reshape(&value, id))
            .expect("the reshape");
    }

    /// Poll every process until none works, then move the clock one tick,
    /// until `done` holds.
    fn run_until(&mut self, what: &str, done: impl Fn(&Self) -> bool) {
        for _ in 0..100 {
            loop {
                let mut busy = false;
                for process in self.processes.iter_mut().flatten() {
                    busy |= process.poll();
                    self.replies.extend(process.take_replies());
                }
                if !busy {
                    break;
                }
            }
            if done(self) {
                return;
            }
            self.clock.advance(TICK);
        }
        panic!("{what}: not reached in 100 ticks");
    }

    /// The writer's call to the leader `node` on process `i`, and its
    /// answer.
    fn call(&mut self, i: usize, node: u64, command: paros_journal::Command) -> CallOutcome {
        let call: CallId = self.process(i).call(at(node), command);
        self.run_until("the call answers", |c| reply_of(&c.replies, call).is_some());
        let outcome = reply_of(&self.replies, call).expect("a reply").clone();
        self.replies.retain(|r| r.call != call);
        outcome
    }

    fn replica_front(&self, i: usize, node: u64) -> Option<Slot> {
        self.processes[i]
            .as_ref()?
            .host()
            .replica(at(node))?
            .front()
    }
}

fn main() {
    let mut cluster = Cluster::new();
    open_first(&mut cluster);
    write_first(&mut cluster);
    crash_and_restart(&mut cluster);
    seal(&mut cluster);
    write_second(&mut cluster);
    println!("ok: four records, two segments, one process restarted from its own storage.");
}

/// Step 1: segment 1 opens, and leader L1 runs Phase 1.
fn open_first(cluster: &mut Cluster) {
    println!("1. Segment 1 opens on three processes; leader L1 runs Phase 1.");
    cluster.open_segment(1);
    for i in 0..3 {
        let node = i as u64 + 1;
        cluster.start(
            i,
            MemStorage::new(),
            vec![acceptor(node), replica(10 + node, 1)],
        );
    }
    cluster.assign(0, leader(L1, 1, Slot(0)));
    cluster.run_until("L1 opens", |c| {
        c.processes[0].as_ref().is_some_and(|p| open(p, L1))
    });
    let phase = cluster
        .process(0)
        .host()
        .leader(at(L1))
        .map(|l| l.phase().clone());
    show("L1 phase", &phase);
    assert_eq!(phase, Some(LeaderPhase::Open { start: Slot(0) }));
}

/// Step 2: writer A takes the journal and writes two records.
fn write_first(cluster: &mut Cluster) {
    println!("2. Writer A takes the journal and writes.");
    let set = cluster.call(0, L1, claim());
    show("SetLeader(A, Unset)", &set);
    assert!(matches!(
        set,
        CallOutcome::Reply(Reply::Answer(Answer::LeaderSet(_)))
    ));
    for seq in 0..2 {
        let answer = cluster.call(0, L1, write(seq, &format!("a{seq}")));
        show(&format!("Write(A, {seq})"), &answer);
        assert_eq!(answer, written(seq));
    }
}

/// Step 3: process 2 crashes, a write is chosen without it, and it starts
/// again from its storage.
fn crash_and_restart(cluster: &mut Cluster) {
    println!("3. Process 2 crashes; F + 1 of the column still choose.");
    let mut storage = cluster.crash(1);
    let answer = cluster.call(0, L1, write(2, "a2"));
    show("Write(A, 2) with process 2 down", &answer);
    assert_eq!(answer, written(2));

    let records = storage.scan(at(2)).expect("a scan").len();
    show("records of acceptor 2 on its storage", &records);
    cluster.start(1, storage, vec![acceptor(2), replica(12, 1)]);
    let acceptor = cluster
        .process(1)
        .host()
        .acceptor(at(2))
        .expect("acceptor 2");
    let votes: Vec<Slot> = (0..4)
        .map(Slot)
        .filter(|s| acceptor.vote(*s).is_some())
        .collect();
    show(
        "acceptor 2 reads its storage again",
        &(acceptor.promised(), &votes),
    );
    assert_eq!(
        acceptor.promised(),
        Ballot {
            round: 1,
            node: NodeId(L1)
        }
    );
    assert_eq!(
        votes,
        [Slot(0), Slot(1), Slot(2)],
        "slot 3 was chosen while process 2 was down"
    );
}

/// Step 4: segment 2 opens, and leader L2 seals segment 1.
fn seal(cluster: &mut Cluster) {
    println!("4. Segment 2 opens; leader L2 seals segment 1.");
    cluster.open_segment(2);
    for i in 0..3 {
        cluster.assign(i, replica(21 + i as u64, 2));
    }
    cluster.process(0).unassign(at(L1));
    // L2 reads from the applied prefix of segment 1.
    let from = (0..3)
        .filter_map(|i| cluster.replica_front(i, 11 + i as u64))
        .max()
        .expect("a replica of segment 1");
    cluster.assign(2, leader(L2, 2, from));
    cluster.run_until("L2 opens and segment 2 folds", |c| {
        c.processes[2].as_ref().is_some_and(|p| open(p, L2))
            && (0..3).all(|i| c.replica_front(i, 21 + i as u64).is_some())
    });
    let bound = cluster
        .process(2)
        .host()
        .leader(at(L2))
        .and_then(JournalLeader::segment);
    show("L2 bound to", &bound);
    assert_eq!(
        bound,
        Some(SegmentId(2)),
        "the bind takes the newest segment"
    );
    let value = cluster.register.value(JOURNAL).expect("the register");
    let record = value.segment(SegmentId(1)).expect("segment 1");
    let end = record.end().expect("segment 1 is sealed");
    show("segment 1 sealed at", &end);
    show("boundary of segment 1", &record.boundary());
    assert_eq!(end, Slot(4), "one above the highest vote, slot 3");
    assert!(record.boundary().is_some(), "the replicas wrote it");
}

/// Step 5: writer A writes in segment 2, at the next position.
fn write_second(cluster: &mut Cluster) {
    println!("5. Writer A writes again, now in segment 2.");
    let answer = cluster.call(2, L2, write(3, "a3"));
    show("Write(A, 3)", &answer);
    assert_eq!(answer, written(3));
    let set = replicas(2);
    for (i, node) in set.iter().enumerate() {
        let replica = cluster.processes[i]
            .as_ref()
            .and_then(|p| p.host().replica(at(node.0)))
            .expect("a replica of segment 2");
        let state = replica.state().expect("a replica with its base");
        assert_eq!(state.next_seq(), Seq(4));
        assert_eq!(state.leader(), Leader::Uuid(A));
    }
}
