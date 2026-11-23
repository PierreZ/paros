//! A host whose roles are not journal roles: a toy coordinator with a disk
//! and two workers with none, on the generic host and driver of
//! `paros-host` (#1410 (split the generic host of actors)).
//!
//! The coordinator owns a term. Each time it starts, it writes a term one
//! higher than the term on its disk, and every order it sends waits for
//! that write (the flight rule): a worker never sees a term that a crash
//! can undo. A worker runs the task of an order and answers `Done` with
//! the term; the coordinator reports a task finished only for an answer of
//! its own term. The run crashes the process (a drop of the driver) and
//! starts it again on the same disk: the new term is higher, and a late
//! answer of the old term is refused.
//!
//! The family is a few lines of types, the roles are one enum with the
//! `Role` trait, and the backends are written by hand. Nothing here names
//! a journal.
//!
//!     nix develop --command cargo run -p paros-host --example coordinator

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use paros_host::{
    Clock, Driver, Effect, Family, Host, Input, Ports, Role, Start, Storage, StorageError, Time,
    Transport,
};

/// An actor of the process: an opaque address, not a journal key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Actor {
    Coordinator,
    Worker(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Body {
    Order { term: u64, task: u64 },
    Done { term: u64, task: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Msg {
    from: Actor,
    to: Actor,
    body: Body,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Assignment {
    Coordinator { workers: Vec<u8> },
    Worker(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum In {
    Msg {
        from: Actor,
        body: Body,
    },
    /// A client asks the coordinator to run `task`.
    Run {
        task: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Out {
    Finished { task: u64, by: Actor },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Why {
    Unassigned,
    Unreadable,
    Failed,
}

/// The family of the example's roles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cell;

/// One record of the coordinator's disk: the term written by batch `batch`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TermRecord {
    batch: u64,
    term: u64,
}

impl Family for Cell {
    type Address = Actor;
    type Envelope = Msg;
    type Batch = u64;
    type Op = TermRecord;
    type Records = Vec<TermRecord>;
    type Assignment = Assignment;
    type Input = In;
    type Output = Out;
    type Stop = Why;

    fn address(assignment: &Assignment) -> Actor {
        match assignment {
            Assignment::Coordinator { .. } => Actor::Coordinator,
            Assignment::Worker(n) => Actor::Worker(*n),
        }
    }

    fn route(msg: Msg) -> (Actor, In) {
        (
            msg.to,
            In::Msg {
                from: msg.from,
                body: msg.body,
            },
        )
    }

    fn durable(records: &Vec<TermRecord>) -> Option<u64> {
        records.last().map(|record| record.batch)
    }

    fn overlay(mut records: Vec<TermRecord>, ops: &[TermRecord]) -> Vec<TermRecord> {
        records.extend(ops.iter().cloned());
        records
    }

    fn unassigned() -> Why {
        Why::Unassigned
    }

    fn unreadable() -> Why {
        Why::Unreadable
    }

    fn failed() -> Why {
        Why::Failed
    }
}

/// The roles of the example.
#[derive(Debug)]
enum CellRole {
    Coordinator {
        term: u64,
        /// The batch that wrote `term`: every order waits on it.
        batch: u64,
        workers: Vec<u8>,
        next: usize,
    },
    Worker,
}

impl Role for CellRole {
    type Family = Cell;
    type Shared = ();

    fn start((): &(), _: Time, assignment: Assignment) -> Start<Self> {
        match assignment {
            // The coordinator has a disk: it reads its term first.
            Assignment::Coordinator { .. } => Start::Recover,
            Assignment::Worker(_) => Start::Running(CellRole::Worker, Vec::new()),
        }
    }

    fn recover(
        (): &(),
        _: Time,
        address: Actor,
        assignment: Assignment,
        records: &Vec<TermRecord>,
    ) -> Result<(Self, Vec<Effect<Cell>>), Why> {
        let Assignment::Coordinator { workers } = assignment else {
            unreachable!("a worker has no disk")
        };
        let last = records
            .last()
            .map_or(TermRecord { batch: 0, term: 0 }, Clone::clone);
        // A new term, one higher than any term on the disk, in a new batch.
        let record = TermRecord {
            batch: last.batch + 1,
            term: last.term + 1,
        };
        let role = CellRole::Coordinator {
            term: record.term,
            batch: record.batch,
            workers,
            next: 0,
        };
        let write = Effect::Persist {
            address,
            id: record.batch,
            ops: vec![record],
        };
        Ok((role, vec![write]))
    }

    fn step(&mut self, (): &(), _: Time, address: Actor, input: In) -> Vec<Effect<Cell>> {
        match (self, input) {
            (
                CellRole::Coordinator {
                    term,
                    batch,
                    workers,
                    next,
                },
                In::Run { task },
            ) => {
                let worker = workers[*next % workers.len()];
                *next += 1;
                // The order waits for the write of its term.
                vec![Effect::Send {
                    sender: address,
                    envelope: Msg {
                        from: address,
                        to: Actor::Worker(worker),
                        body: Body::Order { term: *term, task },
                    },
                    after: Some(*batch),
                }]
            }
            (
                CellRole::Coordinator { term, .. },
                In::Msg {
                    from,
                    body: Body::Done { term: t, task },
                },
            ) => {
                if t == *term {
                    vec![Effect::Output {
                        address,
                        output: Out::Finished { task, by: from },
                        after: None,
                    }]
                } else {
                    // An answer of another term is refused: data, not a panic.
                    Vec::new()
                }
            }
            (
                CellRole::Worker,
                In::Msg {
                    from,
                    body: Body::Order { term, task },
                },
            ) => {
                vec![Effect::Send {
                    sender: address,
                    envelope: Msg {
                        from: address,
                        to: from,
                        body: Body::Done { term, task },
                    },
                    after: None,
                }]
            }
            _ => Vec::new(),
        }
    }

    fn tick(&mut self, (): &(), _: Time, _: Actor) -> Vec<Effect<Cell>> {
        Vec::new()
    }

    fn stop(&mut self, _: Actor, _: &Why) -> Vec<Effect<Cell>> {
        Vec::new()
    }
}

/// A disk in memory that a crash keeps.
#[derive(Clone, Default)]
struct Disk(Rc<RefCell<Vec<TermRecord>>>);

impl Storage<Cell> for Disk {
    fn commit(&mut self, ops: &[TermRecord]) -> Result<(), StorageError> {
        self.0.borrow_mut().extend(ops.iter().cloned());
        Ok(())
    }

    fn scan(&mut self, address: Actor) -> Result<Vec<TermRecord>, StorageError> {
        assert_eq!(
            address,
            Actor::Coordinator,
            "only the coordinator has a disk"
        );
        Ok(self.0.borrow().clone())
    }
}

/// One process: every message goes through the queue, a local one too.
#[derive(Default)]
struct Wire {
    queue: VecDeque<Msg>,
    log: Vec<Msg>,
}

impl Transport<Cell> for Wire {
    fn send(&mut self, msg: Msg) {
        self.log.push(msg.clone());
        self.queue.push_back(msg);
    }

    fn receive(&mut self) -> Option<Msg> {
        self.queue.pop_front()
    }
}

struct Stopped;

impl Clock for Stopped {
    fn now(&self) -> Time {
        Time(0)
    }
}

/// The finished tasks, as the ports take them.
#[derive(Default)]
struct Finished(BTreeMap<u64, Actor>);

impl Ports<Host<CellRole>> for Finished {
    fn output(&mut self, _: Actor, output: Out) -> Vec<Input<Cell>> {
        let Out::Finished { task, by } = output;
        self.0.insert(task, by);
        Vec::new()
    }
}

type Proc = Driver<Host<CellRole>, Disk, Wire, Stopped, Finished, ()>;

fn process(disk: &Disk) -> Proc {
    let mut driver = Driver::new(
        Host::new(1_000, ()),
        disk.clone(),
        Wire::default(),
        Stopped,
        Finished::default(),
        (),
    );
    driver.input(Input::Assign(Assignment::Coordinator {
        workers: vec![1, 2],
    }));
    driver.input(Input::Assign(Assignment::Worker(1)));
    driver.input(Input::Assign(Assignment::Worker(2)));
    driver
}

fn run(driver: &mut Proc, task: u64) {
    driver.input(Input::Deliver {
        address: Actor::Coordinator,
        input: In::Run { task },
    });
}

fn settle(driver: &mut Proc) {
    while driver.poll() {}
}

fn main() {
    let disk = Disk::default();
    let mut driver = process(&disk);
    run(&mut driver, 7);
    run(&mut driver, 8);
    // The term is not durable yet: the orders wait for it.
    assert!(
        driver.transport_mut().log.is_empty(),
        "no order before its term is durable"
    );
    settle(&mut driver);
    println!("term 1 on disk: {:?}", disk.0.borrow());
    assert_eq!(disk.0.borrow().last().map(|r| r.term), Some(1));
    assert_eq!(
        driver.ports().0,
        BTreeMap::from([(7, Actor::Worker(1)), (8, Actor::Worker(2))]),
        "each task finished once, by the worker it went to"
    );

    // A crash: the driver is dropped, the disk stays.
    drop(driver);
    let mut driver = process(&disk);
    settle(&mut driver);
    println!("after a restart: {:?}", disk.0.borrow());
    assert_eq!(
        disk.0.borrow().last().map(|r| r.term),
        Some(2),
        "a new term, one higher"
    );

    // A late answer of term 1 arrives: the coordinator of term 2 refuses it.
    driver.transport_mut().send(Msg {
        from: Actor::Worker(1),
        to: Actor::Coordinator,
        body: Body::Done { term: 1, task: 9 },
    });
    settle(&mut driver);
    assert!(
        driver.ports().0.is_empty(),
        "an answer of an old term finishes nothing"
    );

    run(&mut driver, 10);
    settle(&mut driver);
    assert_eq!(driver.ports().0, BTreeMap::from([(10, Actor::Worker(1))]));
    println!("finished: {:?}", driver.ports().0);
    println!("ok: a role that is not a journal role runs on the generic host");
}
