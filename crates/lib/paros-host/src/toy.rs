//! A toy family for the tests of the host and the driver: a counter with a
//! disk, which persists each bump, tells its peers and reports its value at
//! each fire of its timer, and a watcher with no disk, which reports each
//! higher value, ends at a limit, and sets one timer whose jitter is a
//! random draw. It is not a journal role, so the tests show that the host
//! knows none.

use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use crate::family::Family;
use crate::host::Effect;
use crate::io::{Clock, Entropy, Storage, StorageError, Transport};
use crate::role::{Role, Start};
use crate::time::Time;

/// The value at which a watcher ends by its own step.
pub const LIMIT: u64 = 3;

/// The period of a counter's timer, and the fixed part of a watcher's.
pub const TICK: u64 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Toy;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Note {
    pub from: u32,
    pub to: u32,
    pub value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op {
    pub address: u32,
    pub batch: u64,
    pub value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Assignment {
    Counter { address: u32, peers: Vec<u32> },
    Watcher { address: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum In {
    Bump,
    /// The counter ends by its own step.
    End,
    Note {
        from: u32,
        value: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Out {
    Bumped {
        value: u64,
    },
    Seen {
        value: u64,
    },
    /// A periodic report that the driver drops when its batch is not
    /// durable yet.
    Report {
        value: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stop {
    Unassigned,
    Unreadable,
    Failed,
    Damaged,
    Done,
}

impl Family for Toy {
    type Address = u32;
    type Envelope = Note;
    type Batch = u64;
    type Op = Op;
    type Records = Vec<(u64, u64)>;
    type Assignment = Assignment;
    type Input = In;
    type Output = Out;
    type Stop = Stop;

    fn address(assignment: &Assignment) -> u32 {
        match assignment {
            Assignment::Counter { address, .. } | Assignment::Watcher { address } => *address,
        }
    }

    fn route(envelope: Note) -> (u32, In) {
        (
            envelope.to,
            In::Note {
                from: envelope.from,
                value: envelope.value,
            },
        )
    }

    fn durable(records: &Vec<(u64, u64)>) -> Option<u64> {
        records.last().map(|(batch, _)| *batch)
    }

    fn overlay(records: Vec<(u64, u64)>, ops: &[Op]) -> Vec<(u64, u64)> {
        let mut map: BTreeMap<u64, u64> = records.into_iter().collect();
        for op in ops {
            map.insert(op.batch, op.value);
        }
        map.into_iter().collect()
    }

    fn holds(output: &Out) -> bool {
        !matches!(output, Out::Report { .. })
    }

    fn keeps_batches(why: &Stop) -> bool {
        *why == Stop::Done
    }

    fn unassigned() -> Stop {
        Stop::Unassigned
    }

    fn unreadable() -> Stop {
        Stop::Unreadable
    }

    fn failed() -> Stop {
        Stop::Failed
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToyRole {
    Counter {
        value: u64,
        /// The last batch emitted, durable or not.
        last: u64,
        peers: Vec<u32>,
        ended: bool,
    },
    Watcher {
        seen: u64,
        /// The jitter of its timer, once the driver drew it.
        jitter: Option<u64>,
        /// Whether its timer fired.
        woke: bool,
    },
}

impl Role for ToyRole {
    type Family = Toy;
    /// The values below which a counter refuses to start (a stand-in for a
    /// register value that the code above the host gives).
    type Shared = u64;

    fn start(_shared: &u64, _now: Time, assignment: Assignment) -> Start<Self> {
        match assignment {
            Assignment::Counter { .. } => Start::Recover,
            Assignment::Watcher { address } => Start::Running(
                ToyRole::Watcher {
                    seen: 0,
                    jitter: None,
                    woke: false,
                },
                vec![Effect::Draw { address }],
            ),
        }
    }

    fn recover(
        shared: &u64,
        now: Time,
        address: u32,
        assignment: Assignment,
        records: &Vec<(u64, u64)>,
    ) -> Result<(Self, Vec<Effect<Toy>>), Stop> {
        let Assignment::Counter { peers, .. } = assignment else {
            unreachable!("a watcher has no disk")
        };
        let value = records.last().map_or(0, |(_, value)| *value);
        if value < *shared {
            return Err(Stop::Damaged);
        }
        let last = records.last().map_or(0, |(batch, _)| *batch);
        Ok((
            ToyRole::Counter {
                value,
                last,
                peers,
                ended: false,
            },
            vec![Effect::SetTimer {
                address,
                at: now.after(TICK),
            }],
        ))
    }

    fn step(&mut self, _shared: &u64, _now: Time, address: u32, input: In) -> Vec<Effect<Toy>> {
        match (self, input) {
            (ToyRole::Counter { ended, .. }, In::End) => {
                *ended = true;
                Vec::new()
            }
            (
                ToyRole::Counter {
                    value, last, peers, ..
                },
                In::Bump,
            ) => {
                *value += 1;
                *last += 1;
                let mut out = vec![Effect::Persist {
                    address,
                    id: *last,
                    ops: vec![Op {
                        address,
                        batch: *last,
                        value: *value,
                    }],
                }];
                for to in peers.iter() {
                    out.push(Effect::Send {
                        sender: address,
                        envelope: Note {
                            from: address,
                            to: *to,
                            value: *value,
                        },
                        after: Some(*last),
                    });
                }
                out.push(Effect::Output {
                    address,
                    output: Out::Bumped { value: *value },
                    after: Some(*last),
                });
                out
            }
            (ToyRole::Watcher { seen, .. }, In::Note { value, .. }) if value > *seen => {
                *seen = value;
                vec![Effect::Output {
                    address,
                    output: Out::Seen { value },
                    after: None,
                }]
            }
            _ => Vec::new(),
        }
    }

    fn fire(&mut self, _shared: &u64, now: Time, address: u32) -> Vec<Effect<Toy>> {
        match self {
            ToyRole::Counter { value, last, .. } => {
                let mut out = Vec::new();
                if *last > 0 {
                    out.push(Effect::Output {
                        address,
                        output: Out::Report { value: *value },
                        after: Some(*last),
                    });
                }
                out.push(Effect::SetTimer {
                    address,
                    at: now.after(TICK),
                });
                out
            }
            ToyRole::Watcher { woke, .. } => {
                *woke = true;
                Vec::new()
            }
        }
    }

    fn drawn(&mut self, _shared: &u64, now: Time, address: u32, value: u64) -> Vec<Effect<Toy>> {
        let ToyRole::Watcher { jitter, .. } = self else {
            unreachable!("a counter asks no draw")
        };
        assert!(jitter.is_none(), "a watcher asks one draw");
        *jitter = Some(value % TICK);
        vec![Effect::SetTimer {
            address,
            at: now.after(TICK + value % TICK),
        }]
    }

    fn stop(&mut self, _address: u32, _why: &Stop) -> Vec<Effect<Toy>> {
        Vec::new()
    }

    fn done(&self) -> Option<Stop> {
        match self {
            ToyRole::Watcher { seen, .. } if *seen >= LIMIT => Some(Stop::Done),
            ToyRole::Counter { ended: true, .. } => Some(Stop::Done),
            _ => None,
        }
    }
}

/// A disk in memory, shared by its copies; a test can make the next commit
/// fail, and the next scan.
#[derive(Clone, Debug, Default)]
pub struct Disk {
    pub records: Rc<std::cell::RefCell<BTreeMap<(u32, u64), u64>>>,
    pub fail_commit: Rc<Cell<bool>>,
    pub fail_scan: Rc<Cell<bool>>,
}

impl Storage<Toy> for Disk {
    fn commit(&mut self, ops: &[Op], _sync: bool) -> Result<(), StorageError> {
        if self.fail_commit.replace(false) {
            return Err(StorageError("planted".into()));
        }
        let mut records = self.records.borrow_mut();
        for op in ops {
            records.insert((op.address, op.batch), op.value);
        }
        Ok(())
    }

    fn scan(&mut self, address: u32) -> Result<Vec<(u64, u64)>, StorageError> {
        if self.fail_scan.replace(false) {
            return Err(StorageError("planted".into()));
        }
        Ok(self
            .records
            .borrow()
            .range((address, 0)..=(address, u64::MAX))
            .map(|((_, batch), value)| (*batch, *value))
            .collect())
    }
}

/// A network that delivers to the one process, in order.
#[derive(Clone, Debug, Default)]
pub struct Wire {
    pub queue: Rc<std::cell::RefCell<VecDeque<Note>>>,
}

impl Transport<Toy> for Wire {
    fn send(&mut self, envelope: Note) {
        self.queue.borrow_mut().push_back(envelope);
    }

    fn receive(&mut self) -> Option<Note> {
        self.queue.borrow_mut().pop_front()
    }
}

/// A clock that a test moves.
#[derive(Clone, Debug, Default)]
pub struct Hand(pub Rc<Cell<u64>>);

impl Clock for Hand {
    fn now(&self) -> Time {
        Time(self.0.get())
    }
}

/// A random source that gives 7, 14, 21, ...: each draw is new, and a test
/// knows it.
#[derive(Clone, Debug, Default)]
pub struct Dice(pub u64);

impl Entropy for Dice {
    fn draw(&mut self) -> u64 {
        self.0 += 7;
        self.0
    }
}
