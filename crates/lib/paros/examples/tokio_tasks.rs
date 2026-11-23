//! **A journal on three tokio tasks, through the `paros` crate alone.**
//!
//! Run it: `cargo run -p paros --example tokio_tasks`
//!
//! paros has no runtime. A program brings one, and implements the traits
//! of `paros::journal_io` on it. This example does that with tokio, in
//! about a hundred lines, with no network:
//!
//! - **Transport** (`Channels`): one tokio channel for each task. A send
//!   puts the message, a Rust value, on the channel of the task that serves
//!   the receiver, and wakes that task. Nothing is encoded.
//! - **Clock** (`TokioClock`): the nanoseconds since the start, from
//!   `tokio::time::Instant`.
//! - **Storage** and **register**: the in-memory backends of
//!   `paros::journal_io::mem`. Each task has its own storage; the three
//!   tasks share one register.
//!
//! The traits require no `Send`, and the in-memory backends are not `Send`.
//! So the tasks run on one thread, in a `tokio::task::LocalSet`. Each task
//! owns one [`Driver`]: it polls the driver until the driver has no work,
//! then sleeps until a message wakes it or the driver's timer is due.
//!
//! # What the run shows
//!
//! 1. Three tasks start, one for each process: an acceptor and a replica of
//!    segment 1 on each, and the leader on task 1.
//! 2. The leader runs Phase 1 over the channels.
//! 3. Writer A, on task 1, takes the journal and writes three records.
//! 4. Every replica, on its own task, folds the three writes.
//!
//! Each step asserts its result, so CI runs this example as a test.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::LocalSet;
use tokio::time::Instant;

use paros::journal::register::{Ballot, Change, RegisterValue};
use paros::journal::roles::{Campaign, LeaderPhase};
use paros::journal::{
    Answer, Command, Fence, Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Record, Reply,
    SegmentId, Seq, Slot, Write,
};
use paros::journal_io::mem::{MemRegister, MemStorage};
use paros::journal_io::{
    Address, Assignment, CallOutcome, Clock, Driver, Envelope, HostConfig, JournalId, Register,
    Time, Transport,
};

const JOURNAL: JournalId = JournalId(1);
const TICK: u64 = 1_000_000;
const ACCEPTORS: [u64; 3] = [1, 2, 3];
const REPLICAS: [u64; 3] = [11, 12, 13];
const LEADER: u64 = 100;
const A: LeaderUuid = LeaderUuid(0xA);

type Process = Driver<MemStorage, Channels, TokioClock, MemRegister, ()>;

fn at(node: u64) -> Address {
    Address::new(JOURNAL, NodeId(node))
}

/// The channel of one task, and the signal that wakes it.
#[derive(Clone)]
struct Inbox {
    sender: UnboundedSender<Envelope>,
    wake: Rc<Notify>,
}

/// The transport of one task: its own channel, and the routes from each
/// role to the inbox of the task that serves it.
struct Channels {
    routes: Rc<RefCell<BTreeMap<Address, Inbox>>>,
    receiver: UnboundedReceiver<Envelope>,
}

impl Transport for Channels {
    fn send(&mut self, envelope: Envelope) {
        if let Some(inbox) = self.routes.borrow().get(&envelope.receiver()) {
            // A send to a task that stopped is a lost message: the roles
            // send again on their timers.
            let _ = inbox.sender.send(envelope);
            inbox.wake.notify_one();
        }
    }

    fn receive(&mut self) -> Option<Envelope> {
        self.receiver.try_recv().ok()
    }
}

/// The time since the start of the program.
#[derive(Clone, Copy)]
struct TokioClock {
    start: Instant,
}

impl Clock for TokioClock {
    fn now(&self) -> Time {
        Time(u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }
}

/// What a task needs besides its driver: its wake signal and the clock.
struct Task {
    wake: Rc<Notify>,
    clock: TokioClock,
}

impl Task {
    /// Poll the driver until it has no work, then sleep until a message or
    /// the driver's timer, until `done` holds.
    async fn run_until(&self, process: &mut Process, mut done: impl FnMut(&mut Process) -> bool) {
        loop {
            while process.poll() {}
            if done(process) {
                return;
            }
            let due = process
                .timer()
                .map_or(TICK, |t| t.0.saturating_sub(self.clock.now().0).max(1));
            tokio::select! {
                () = self.wake.notified() => {}
                () = tokio::time::sleep(Duration::from_nanos(due)) => {}
            }
        }
    }

    /// The writer's call to the leader, and its answer.
    async fn call(&self, process: &mut Process, command: Command) -> CallOutcome {
        let id = process.call(at(LEADER), command);
        let mut answer = None;
        self.run_until(process, |p| {
            answer = answer
                .take()
                .or_else(|| p.take_replies().into_iter().find(|r| r.call == id));
            answer.is_some()
        })
        .await;
        answer.expect("a reply").outcome
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    LocalSet::new().run_until(run()).await;
}

async fn run() {
    let clock = TokioClock {
        start: Instant::now(),
    };
    println!("1. Three tasks start: an acceptor and a replica on each, the leader on task 1.");
    let mut tasks = start(clock);

    // Tasks 2 and 3 run until their replica has folded the three writes,
    // then hand their driver back. Task 1 is the one below.
    let (mut first, first_task, _) = tasks.remove(0);
    let others: Vec<_> = tasks
        .into_iter()
        .map(|(mut process, task, node)| {
            tokio::task::spawn_local(async move {
                task.run_until(&mut process, |p| folded(p, node)).await;
                process
            })
        })
        .collect();

    println!("2. The leader runs Phase 1 over the channels.");
    first_task
        .run_until(&mut first, |p| {
            p.host()
                .leader(at(LEADER))
                .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
        })
        .await;

    println!("3. Writer A takes the journal and writes three records.");
    write(&first_task, &mut first).await;
    println!("4. Every replica, on its own task, folds the three writes.");
    // The writes are done at a quorum, so a replica may still be one message
    // behind. Task 1 serves its roles until tasks 2 and 3 are done.
    first_task
        .run_until(&mut first, |p| {
            folded(p, REPLICAS[0]) && others.iter().all(tokio::task::JoinHandle::is_finished)
        })
        .await;
    let mut processes = vec![first];
    for handle in others {
        processes.push(handle.await.expect("a task that ends"));
    }
    for (i, process) in processes.iter().enumerate() {
        let node = REPLICAS[i];
        let state = process
            .host()
            .replica(at(node))
            .and_then(|r| r.state())
            .expect("a replica with its base");
        show(
            &format!("replica {node} on task {}", i + 1),
            &(state.next_seq(), state.leader()),
        );
        assert_eq!(state.next_seq(), Seq(3));
        assert_eq!(state.leader(), Leader::Uuid(A));
    }
    println!("ok: three tasks, one journal, three records, no network.");
}

/// One driver for each task, its roles assigned and routed, with the task's
/// wake signal and the node of its replica.
fn start(clock: TokioClock) -> Vec<(Process, Task, u64)> {
    let register = open_segment();
    let routes = Rc::new(RefCell::new(BTreeMap::new()));
    let mut tasks = Vec::new();
    for i in 0..3 {
        let (sender, receiver) = unbounded_channel();
        let wake = Rc::new(Notify::new());
        let inbox = Inbox {
            sender,
            wake: wake.clone(),
        };
        let mut roles = vec![
            Assignment::Acceptor {
                address: at(ACCEPTORS[i]),
                page: 16,
            },
            Assignment::Replica {
                address: at(REPLICAS[i]),
                segment: SegmentId(1),
                set: REPLICAS.map(NodeId).to_vec(),
                initial: Leader::Unset,
            },
        ];
        if i == 0 {
            roles.push(leader());
        }
        for role in &roles {
            routes.borrow_mut().insert(role.address(), inbox.clone());
        }
        let mut process = Driver::new(
            HostConfig {
                tick: TICK,
                resend_page: 16,
            },
            MemStorage::new(),
            Channels {
                routes: routes.clone(),
                receiver,
            },
            clock,
            register.clone(),
            (),
        );
        for role in roles {
            process.assign(role);
        }
        tasks.push((process, Task { wake, clock }, REPLICAS[i]));
    }
    tasks
}

/// Writer A takes the journal and writes three records, one at a time.
async fn write(task: &Task, process: &mut Process) {
    let set = task
        .call(
            process,
            Command::SetLeader {
                new: Leader::uuid(A),
                old: Leader::Unset,
            },
        )
        .await;
    show("SetLeader(A, Unset)", &set);
    assert!(matches!(
        set,
        CallOutcome::Reply(Reply::Answer(Answer::LeaderSet(_)))
    ));
    for seq in 0..3 {
        let write = Command::Write(Write {
            fence: Fence::Leader {
                uuid: A,
                expected_seq: Seq(seq),
            },
            records: vec![Record::new(format!("record {seq}").into_bytes())],
        });
        let answer = task.call(process, write).await;
        show(&format!("Write(A, expected_seq {seq})"), &answer);
        assert_eq!(
            answer,
            CallOutcome::Reply(Reply::Answer(Answer::Written {
                seq: Seq(seq),
                count: 1
            }))
        );
    }
}

/// Whether the replica `node` of this process has folded the three writes.
fn folded(process: &mut Process, node: u64) -> bool {
    process
        .host()
        .replica(at(node))
        .and_then(|r| r.state())
        .is_some_and(|s| s.next_seq() == Seq(3))
}

/// The journal's register: the controller is elected, then it opens
/// segment 1 on one column of three acceptors.
fn open_segment() -> MemRegister {
    let mut register = MemRegister::new();
    let elected = RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("the first election of the controller");
    register.create(JOURNAL, elected.clone());
    let reshape = Change::Reshape {
        opening: OpeningCall {
            term: elected.term().number,
            expected: None,
            grid: Grid::new(1, vec![ACCEPTORS.map(NodeId).to_vec()]).expect("a v1 grid"),
            replicas: REPLICAS.map(NodeId).to_vec(),
        },
        release: RELEASE,
    };
    register.change(JOURNAL, &reshape).expect("the reshape");
    register
}

/// The leader of round 1, bound to segment 1.
fn leader() -> Assignment {
    Assignment::Leader {
        address: at(LEADER),
        proxies: Vec::new(),
        campaign: Campaign {
            ballot: Ballot {
                round: 1,
                node: NodeId(LEADER),
            },
            segment: SegmentId(1),
            from: Slot(0),
        },
    }
}

/// Print one step: what was asked, and what came back.
fn show(what: &str, answer: &impl std::fmt::Debug) {
    println!("  {what:<46} -> {answer:?}");
}
