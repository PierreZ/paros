//! Tests of the driver: a whole journal in one process and over three
//! processes, the flight rule on every send, a commit that fails, a crash
//! and a restart from the records, and the events port.

use std::cell::RefCell;
use std::rc::Rc;

use paros_journal::register::{Ballot, RegisterValue};
use paros_journal::roles::{Campaign, LeaderPhase, Message};
use paros_journal::{
    Answer, Fence, Grid, JournalView, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Record,
    Reply, SegmentId, SegmentRecord, SegmentState, Seq, Slot, Write,
};

use super::*;
use crate::events::Event;
use crate::host::Effect;
use crate::layout::{self, Entry, Key, Kind, Op};
use crate::mem::{Endpoint, ManualClock, MemNetwork, MemRegister, MemStorage};
use crate::register::Register;
use crate::storage::StorageError;
use crate::wire::Envelope;

const J: JournalId = JournalId(1);
const TICK: u64 = 1_000;
const CONFIG: HostConfig = HostConfig {
    tick: TICK,
    resend_page: 4,
    window: 64,
};
const LEADER: Address = Address {
    journal: J,
    node: NodeId(100),
    segment: None,
};
const ACCEPTORS: [u64; 3] = [1, 2, 3];
const REPLICAS: [u64; 3] = [11, 12, 13];
const A: LeaderUuid = LeaderUuid(0xA);

/// A storage that the test and the transport also read.
#[derive(Clone, Default)]
struct Shared(Rc<RefCell<MemStorage>>);

impl Storage<Journal> for Shared {
    fn commit(&mut self, ops: &[Op]) -> Result<(), StorageError> {
        self.0.borrow_mut().commit(ops)
    }

    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        self.0.borrow_mut().scan(address)
    }

    fn list(&mut self) -> Result<Vec<Address>, StorageError> {
        self.0.borrow_mut().list()
    }
}

/// A transport that checks the flight rule on every send: an acceptor's
/// `Accepted` leaves only once its vote is on the disk, and a `Promise`
/// only once a promise at least as high is.
struct Checked {
    endpoint: Endpoint,
    disk: Shared,
    sent: Rc<RefCell<u64>>,
}

impl Checked {
    fn on_disk(&self, key: Key) -> Option<Vec<u8>> {
        let mut disk = self.disk.0.borrow_mut();
        let entries = disk.scan(key.address).expect("a scan");
        entries.into_iter().find_map(|(k, entry)| match entry {
            Entry::Value(bytes) if k == key => Some(bytes),
            _ => None,
        })
    }
}

fn ballot_bytes(ballot: Ballot) -> Vec<u8> {
    let mut bytes = ballot.round.to_be_bytes().to_vec();
    bytes.extend(ballot.node.0.to_be_bytes());
    bytes
}

impl Transport<Journal> for Checked {
    fn send(&mut self, envelope: Envelope) {
        match &envelope.message {
            Message::Accepted { ballot, slot } => {
                let vote = self
                    .on_disk(Key::new(envelope.sender(), Kind::Vote, slot.0))
                    .expect("an Accepted leaves once its vote is durable");
                assert!(
                    vote[..16] >= ballot_bytes(*ballot)[..],
                    "a vote at least as new as the reply's is durable"
                );
            }
            Message::Promise { ballot, .. } => {
                let promise = self
                    .on_disk(Key::new(envelope.sender(), Kind::Promise, 0))
                    .expect("a Promise leaves once its promise is durable");
                assert!(promise >= ballot_bytes(*ballot), "the promise is durable");
            }
            _ => {}
        }
        *self.sent.borrow_mut() += 1;
        self.endpoint.send(envelope);
    }

    fn receive(&mut self) -> Option<Envelope> {
        self.endpoint.receive()
    }
}

type TestDriver = Driver<Shared, Checked, ManualClock, MemRegister, Vec<Event>>;

/// The world of a test: one network, one clock, one register.
struct World {
    network: MemNetwork,
    clock: ManualClock,
    register: MemRegister,
    sent: Rc<RefCell<u64>>,
    /// The replies that a test has not looked at yet.
    replies: RefCell<Vec<CallReply>>,
}

impl World {
    fn new() -> Self {
        let register = MemRegister::new();
        let value = RegisterValue::new()
            .apply(&Change::Elect { holder: NodeId(0) })
            .expect("an election");
        let value = value
            .apply(&Change::Reshape {
                opening: OpeningCall {
                    term: value.term().number,
                    expected: None,
                    grid: Grid::new(1, vec![ACCEPTORS.map(NodeId).to_vec()]).expect("a grid"),
                    replicas: REPLICAS.map(NodeId).to_vec(),
                },
                release: RELEASE,
            })
            .expect("a reshape");
        register.create(J, value);
        Self {
            network: MemNetwork::new(),
            clock: ManualClock::new(),
            register,
            sent: Rc::default(),
            replies: RefCell::default(),
        }
    }

    /// A process that serves `nodes` of the journal, on `disk`.
    fn process(&self, disk: &Shared, nodes: &[u64]) -> TestDriver {
        let endpoint = self.network.endpoint();
        for node in nodes {
            endpoint.route(Address::new(J, NodeId(*node)));
        }
        let transport = Checked {
            endpoint,
            disk: disk.clone(),
            sent: Rc::clone(&self.sent),
        };
        let mut driver = Driver::new(
            CONFIG,
            disk.clone(),
            transport,
            self.clock.clone(),
            self.register.clone(),
            Vec::new(),
        );
        assign(&mut driver, nodes);
        driver
    }

    /// Poll every process until none works, then move the clock one tick;
    /// stop when `done` holds.
    fn run(&self, drivers: &mut [TestDriver], what: &str, done: impl Fn(&[TestDriver]) -> bool) {
        for _ in 0..100 {
            while drivers.iter_mut().fold(false, |busy, d| d.poll() | busy) {}
            if done(drivers) {
                return;
            }
            self.clock.advance(TICK);
        }
        panic!("{what}: not reached in 100 ticks");
    }
}

/// Serve the roles `nodes`: an acceptor, a replica or the leader.
fn assign(driver: &mut TestDriver, nodes: &[u64]) {
    for node in nodes {
        let address = Address::new(J, NodeId(*node));
        driver.assign(match node {
            1..=3 => Assignment::Acceptor { address, page: 16 },
            11..=13 => Assignment::Replica {
                address,
                segment: SegmentId(1),
                set: REPLICAS.map(NodeId).to_vec(),
                initial: Leader::Unset,
            },
            _ => Assignment::Leader {
                address,
                proxies: Vec::new(),
                campaign: Campaign {
                    ballot: Ballot {
                        round: 1,
                        node: address.node,
                    },
                    from: Slot(0),
                },
            },
        });
    }
}

fn open(drivers: &[TestDriver]) -> bool {
    drivers.iter().any(|d| {
        d.host()
            .leader(LEADER)
            .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
    })
}

fn write(seq: u64, text: &str) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: A,
            expected_seq: Seq(seq),
        },
        records: vec![Record::new(text.as_bytes().to_vec())],
    })
}

fn claim() -> Command {
    Command::SetLeader {
        new: Leader::uuid(A),
        old: Leader::Unset,
    }
}

/// Run until the call has its reply, and return it.
fn answer(world: &World, drivers: &mut [TestDriver], at: usize, call: CallId) -> CallOutcome {
    for _ in 0..100 {
        while drivers.iter_mut().fold(false, |busy, d| d.poll() | busy) {}
        let mut replies = world.replies.borrow_mut();
        replies.extend(drivers[at].take_replies());
        if let Some(reply) = replies.iter().find(|r| r.call == call) {
            return reply.outcome.clone();
        }
        drop(replies);
        world.clock.advance(TICK);
    }
    panic!("call {call}: no reply in 100 ticks");
}

fn written(seq: u64) -> CallOutcome {
    CallOutcome::Reply(Reply::Answer(Answer::Written {
        seq: Seq(seq),
        count: 1,
    }))
}

#[test]
fn a_journal_in_one_process_writes() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    let reply = answer(&world, &mut drivers, 0, set);
    assert!(
        matches!(
            reply,
            CallOutcome::Reply(Reply::Answer(Answer::LeaderSet(_)))
        ),
        "{reply:?}"
    );
    let w0 = drivers[0].call(LEADER, write(0, "a0"));
    let w1 = drivers[0].call(LEADER, write(1, "a1"));
    assert_eq!(answer(&world, &mut drivers, 0, w0), written(0));
    assert_eq!(answer(&world, &mut drivers, 0, w1), written(1));
    assert!(*world.sent.borrow() > 0, "the checked transport saw sends");
    for node in REPLICAS {
        let replica = drivers[0]
            .host()
            .replica(Address::new(J, NodeId(node)))
            .expect("a replica");
        assert!(replica.front() >= Some(Slot(3)), "replica {node} applied");
    }
}

#[test]
fn a_journal_over_three_processes_writes() {
    let world = World::new();
    let disks: Vec<Shared> = (0..3).map(|_| Shared::default()).collect();
    let mut drivers = vec![
        world.process(&disks[0], &[1, 11, 100]),
        world.process(&disks[1], &[2, 12]),
        world.process(&disks[2], &[3, 13]),
    ];
    world.run(&mut drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    assert!(matches!(
        answer(&world, &mut drivers, 0, set),
        CallOutcome::Reply(Reply::Answer(Answer::LeaderSet(_)))
    ));
    let w0 = drivers[0].call(LEADER, write(0, "a0"));
    assert_eq!(answer(&world, &mut drivers, 0, w0), written(0));
    for (i, disk) in disks.iter().enumerate() {
        let acceptor = Address::new(J, NodeId(ACCEPTORS[i]));
        let entries = disk.0.borrow_mut().scan(acceptor).expect("a scan");
        assert!(
            entries.iter().any(|(k, _)| k.kind == Kind::Promise),
            "each process holds its own acceptor's records"
        );
        let other = Address::new(J, NodeId(ACCEPTORS[(i + 1) % 3]));
        assert_eq!(disk.0.borrow_mut().scan(other).expect("a scan"), Vec::new());
    }
}

#[test]
fn a_failed_commit_stops_its_roles_and_they_recover() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    answer(&world, &mut drivers, 0, set);
    drivers[0].take_stopped();

    // The leader's Accepts reach the acceptors in the next poll; their
    // votes go into one commit, which fails.
    let w0 = drivers[0].call(LEADER, write(0, "a0"));
    disk.0.borrow_mut().fail_next_commit(StorageError::Full);
    drivers[0].poll();
    let stopped = drivers[0].take_stopped();
    let failed: Vec<u64> = stopped
        .iter()
        .filter(|(_, why)| *why == Stop::Failed)
        .map(|(a, _)| a.node.0)
        .collect();
    assert_eq!(
        failed,
        ACCEPTORS.to_vec(),
        "every acceptor of the commit stops"
    );
    for node in ACCEPTORS {
        assert!(!drivers[0].host().serves(Address::new(J, NodeId(node))));
    }
    assert!(
        drivers[0].take_replies().is_empty(),
        "no vote of the failed commit was sent"
    );

    // Placement serves them again: they read their records and the leader's
    // re-sends choose the write.
    assign(&mut drivers[0], &ACCEPTORS);
    world.replies.borrow_mut().clear();
    assert_eq!(answer(&world, &mut drivers, 0, w0), written(0));
}

#[test]
fn a_new_driver_serves_the_roles_again_from_their_records() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    answer(&world, &mut drivers, 0, set);
    let w0 = drivers[0].call(LEADER, write(0, "a0"));
    assert_eq!(answer(&world, &mut drivers, 0, w0), written(0));
    let promised = drivers[0]
        .host()
        .acceptor(Address::new(J, NodeId(1)))
        .expect("an acceptor")
        .promised();

    // A crash: the driver is gone, the disk stays. A new leader at a higher
    // ballot runs Phase 1 over what the acceptors read back.
    let (disk_back, ..) = drivers.pop().expect("a driver").into_parts();
    world.replies.borrow_mut().clear();
    let mut drivers = vec![world.process(&disk_back, &[1, 2, 3, 11, 12, 13])];
    for node in ACCEPTORS {
        let acceptor = drivers[0]
            .host()
            .acceptor(Address::new(J, NodeId(node)))
            .expect("the acceptor recovered");
        assert_eq!(acceptor.promised(), promised, "acceptor {node}");
        assert!(acceptor.vote(Slot(1)).is_some(), "the vote of the write");
    }
    let next = Address::new(J, NodeId(101));
    drivers[0].assign(Assignment::Leader {
        address: next,
        proxies: Vec::new(),
        campaign: Campaign {
            ballot: Ballot {
                round: 2,
                node: next.node,
            },
            from: Slot(0),
        },
    });
    drivers[0].transport_mut().endpoint.route(next);
    world.run(&mut drivers, "the new leader opens", |d| {
        d[0].host()
            .leader(next)
            .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
    });
    let w1 = drivers[0].call(next, write(1, "a1"));
    assert_eq!(answer(&world, &mut drivers, 0, w1), written(1));
}

#[test]
fn the_events_port_reports_every_step_once_its_batches_are_durable() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let events = drivers[0].events();
    assert!(
        matches!(events[0].input, Input::Observe { journal: J, .. }),
        "the first input is the register value"
    );
    let assigned = events
        .iter()
        .filter(|e| matches!(e.input, Input::Assign(_)))
        .count();
    assert_eq!(assigned, 7, "each assignment is an input");
    assert!(
        events.windows(2).all(|pair| pair[0].now <= pair[1].now),
        "the steps are in time order"
    );
    // Every batch in a reported step is on the disk.
    for event in events {
        for effect in &event.effects {
            if let Effect::Persist { address, ops, .. } = effect {
                let entries = disk.0.borrow_mut().scan(*address).expect("a scan");
                let last = ops.last().expect("a durable record");
                assert!(entries.iter().any(|(k, _)| *k == last.key));
            }
        }
    }
    assert!(drivers[0].timer().is_some(), "the timer is armed");
}

#[test]
fn poll_says_when_it_worked_and_the_timer_is_the_roles() {
    let world = World::new();
    let disk = Shared::default();
    let acceptor = world.process(&disk, &[1]);
    assert_eq!(acceptor.timer(), None, "an acceptor sets no timer");
    let mut driver = world.process(&Shared::default(), &[11]);
    assert_eq!(
        driver.timer(),
        Some(Time(TICK)),
        "the replica set its timer one tick after its start at time 0"
    );
    while driver.poll() {}
    assert!(!driver.poll(), "nothing to do");
    // Only the timer.
    world.clock.advance(TICK);
    assert!(driver.poll(), "the timer fired");
    assert_eq!(driver.timer(), Some(Time(2 * TICK)), "armed again");
    assert!(!driver.poll());
    // Only a new register value.
    let mut register = world.register.clone();
    register
        .change(J, &Change::Elect { holder: NodeId(9) })
        .expect("an election");
    assert!(driver.poll(), "a new register value");
    assert!(!driver.poll());
}

#[test]
fn call_ids_grow_and_a_write_needs_no_tick() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    assert_eq!(set, 1, "the ids of a driver grow from 1");
    answer(&world, &mut drivers, 0, set);
    // With no tick, every batch is made durable and every held send goes
    // in the round of its commit: the write is answered.
    let at = drivers[0].timer();
    let w0 = drivers[0].call(LEADER, write(0, "a0"));
    assert_eq!(w0, 2);
    while drivers[0].poll() {}
    assert_eq!(drivers[0].timer(), at, "no timer fired");
    assert_eq!(
        reply_of(&drivers[0].take_replies(), w0),
        Some(written(0)),
        "the write is answered within its rounds"
    );
}

fn reply_of(replies: &[CallReply], call: CallId) -> Option<CallOutcome> {
    replies
        .iter()
        .find(|r| r.call == call)
        .map(|r| r.outcome.clone())
}

#[test]
fn unassign_stops_a_leader_and_answers_its_calls_unknown() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    drivers[0].unassign(LEADER);
    assert_eq!(drivers[0].take_stopped(), vec![(LEADER, Stop::Unassigned)]);
    assert_eq!(
        reply_of(&drivers[0].take_replies(), set),
        Some(CallOutcome::Reply(Reply::Unknown))
    );
    assert!(drivers[0].host().leader(LEADER).is_none());
}

#[test]
fn a_role_unassigned_before_its_commit_writes_nothing() {
    let world = World::new();
    let disk = Shared::default();
    // A fresh replica takes its base when it is served: a batch in the
    // group, and the write of the boundary state held on it.
    let mut driver = world.process(&disk, &[11]);
    driver.unassign(Address::new(J, NodeId(11)));
    assert_eq!(
        driver.take_stopped(),
        vec![(
            Address::replica(J, NodeId(11), SegmentId(1)),
            Stop::Unassigned
        )]
    );
    assign(&mut driver, &[12]);
    driver.poll();
    let base_of_11 = driver.events().iter().any(|event| {
        event.effects.iter().any(|effect| {
            matches!(effect, Effect::Persist { address, .. } if address.node == NodeId(11))
        })
    });
    assert!(
        base_of_11,
        "the step that took the dropped batch is reported"
    );
    let records = |node| {
        disk.0
            .borrow_mut()
            .scan(Address::replica(J, NodeId(node), SegmentId(1)))
            .expect("a scan")
    };
    assert_eq!(
        records(11),
        Vec::new(),
        "the unassigned replica wrote nothing"
    );
    assert_ne!(records(12), Vec::new(), "the other replica took its base");
}

#[test]
fn a_restarted_acceptor_answers_with_no_write_at_its_promise() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let (disk_back, ..) = drivers.pop().expect("a driver").into_parts();
    // The same ballot again: each acceptor promised it already, so its
    // Promise writes nothing and waits on the durable batch it read back.
    let mut drivers = vec![world.process(&disk_back, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens again", open);
}

/// Segment 2 opens, and a second leader of it runs on the process of
/// `drivers`, with segment 1's replicas at slot `from`; `before` runs just
/// before its campaign. Gives the leader and `from`.
fn second_leader(
    world: &World,
    drivers: &mut [TestDriver],
    before: impl FnOnce(&World),
) -> (Address, Slot) {
    world.run(drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    answer(world, drivers, 0, set);

    let mut register = world.register.clone();
    let value = register.read(J).expect("the register");
    register
        .change(
            J,
            &Change::Reshape {
                opening: OpeningCall {
                    term: value.term().number,
                    expected: Some(SegmentId(1)),
                    grid: Grid::new(1, vec![ACCEPTORS.map(NodeId).to_vec()]).expect("a grid"),
                    replicas: [21, 22, 23].map(NodeId).to_vec(),
                },
                release: RELEASE,
            },
        )
        .expect("segment 2 opens");
    drivers[0].unassign(LEADER);
    let next = Address::new(J, NodeId(101));
    for node in [21, 22, 23] {
        let address = Address::new(J, NodeId(node));
        drivers[0].transport_mut().endpoint.route(address);
        drivers[0].assign(Assignment::Replica {
            address,
            segment: SegmentId(2),
            set: [21, 22, 23].map(NodeId).to_vec(),
            initial: Leader::Unset,
        });
    }
    drivers[0].transport_mut().endpoint.route(next);
    let from = drivers[0]
        .host()
        .replica(Address::new(J, NodeId(11)))
        .and_then(paros_journal::roles::SegmentReplica::front)
        .expect("a front");
    before(world);
    drivers[0].assign(Assignment::Leader {
        address: next,
        proxies: Vec::new(),
        campaign: Campaign {
            ballot: Ballot {
                round: 2,
                node: next.node,
            },
            from,
        },
    });
    world.run(drivers, "the second leader opens", |d| {
        d[0].host()
            .leader(next)
            .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
    });
    (next, from)
}

#[test]
fn a_second_leader_seals_the_first_segment_through_the_register() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    let (_, from) = second_leader(&world, &mut drivers, |_| {});
    let value = world.register.value(J).expect("the register");
    let first = value.segment(SegmentId(1)).expect("segment 1");
    assert_eq!(first.end(), Some(from), "sealed one above the last vote");
    world.run(&mut drivers, "the boundary is written", |_| {
        world
            .register
            .value(J)
            .and_then(|v| v.segment(SegmentId(1)).and_then(SegmentRecord::boundary))
            .is_some()
    });
    assert_eq!(
        drivers[0].host().value(J),
        world.register.value(J).as_ref(),
        "the Host holds the value of the last done write"
    );
}

/// The world, with segment 2 open and segment 1 sealed at 0 and retired.
fn retired_world() -> World {
    let mut world = World::new();
    let value = world.register.value(J).expect("a register");
    for change in [
        Change::Reshape {
            opening: OpeningCall {
                term: value.term().number,
                expected: Some(SegmentId(1)),
                grid: Grid::new(1, vec![ACCEPTORS.map(NodeId).to_vec()]).expect("a grid"),
                replicas: vec![NodeId(21), NodeId(22), NodeId(23)],
            },
            release: RELEASE,
        },
        Change::Seal {
            segment: SegmentId(1),
            end: Slot(0),
        },
        Change::Boundary {
            segment: SegmentId(1),
            state: paros_journal::BoundaryState::fresh(Leader::Unset),
        },
        Change::Retire {
            segment: SegmentId(1),
            holders: 2,
            unrepaired_damage: false,
        },
    ] {
        world
            .register
            .change(J, &change)
            .expect("the change is done");
    }
    world
}

const NEW: Address = Address {
    journal: J,
    node: NodeId(20),
    segment: Some(SegmentId(1)),
};

fn copy_of(term: u64, number: u64) -> paros_journal::roles::CopyId {
    paros_journal::roles::CopyId { term, number }
}

fn joining_on(disk: &Shared) -> Option<paros_journal::roles::CopyId> {
    let entries = disk.0.borrow_mut().scan(NEW).expect("a scan of memory");
    layout::decode_replica(NEW, &entries)
        .expect("the records decode")
        .joining
}

#[test]
fn an_abandoned_copy_ends_on_the_disk() {
    let world = retired_world();
    let disk = Shared::default();
    let mut driver = world.process(&disk, &[]);
    driver.join(
        NEW,
        REPLICAS.map(NodeId).to_vec(),
        Leader::Unset,
        copy_of(2, 1),
    );
    driver.poll();
    assert_eq!(joining_on(&disk), Some(copy_of(2, 1)));
    driver.abandon(NEW, copy_of(2, 1));
    driver.poll();
    assert!(!driver.host().serves(NEW), "the replica dropped its copy");
    assert_eq!(joining_on(&disk), None, "the end of the copy is durable");
}

#[test]
fn a_copy_asked_again_before_the_commit_reads_the_last_writes() {
    let world = retired_world();
    let disk = Shared::default();
    let mut driver = world.process(&disk, &[]);
    driver.join(
        NEW,
        REPLICAS.map(NodeId).to_vec(),
        Leader::Unset,
        copy_of(2, 1),
    );
    driver.poll();
    // The abandon and a later, lower ask come before the next commit: the
    // ask reads the abandon's write, so the ended copy does not come back,
    // and the new batches follow the abandon's.
    driver.abandon(NEW, copy_of(2, 1));
    driver.join(
        NEW,
        REPLICAS.map(NodeId).to_vec(),
        Leader::Unset,
        copy_of(1, 7),
    );
    assert_eq!(
        driver
            .host()
            .replica(NEW)
            .and_then(paros_journal::roles::SegmentReplica::joining),
        Some(copy_of(1, 7))
    );
    driver.poll();
    assert_eq!(joining_on(&disk), Some(copy_of(1, 7)));
}

#[test]
fn a_complete_copy_is_a_notice_once_its_batch_is_durable() {
    // Segment 1 ends at slot 0: the new replica holds the whole segment as
    // soon as it has its base.
    let world = retired_world();
    let disk = Shared::default();
    let mut driver = world.process(&disk, &[]);
    driver.join(
        NEW,
        REPLICAS.map(NodeId).to_vec(),
        Leader::Unset,
        copy_of(2, 1),
    );
    let mut notices = Vec::new();
    for _ in 0..10 {
        while driver.poll() {}
        notices.extend(driver.take_notices());
        world.clock.advance(TICK);
    }
    assert!(
        notices.contains(&Notice::Copied {
            address: NEW,
            segment: SegmentId(1),
            copy: copy_of(2, 1),
        }),
        "the copy is reported: {notices:?}"
    );
    assert_eq!(joining_on(&disk), Some(copy_of(2, 1)));
    assert!(driver.take_notices().is_empty(), "a take empties the port");
}

/// Run until the read has its answer, and return it.
fn read_answer(world: &World, drivers: &mut [TestDriver], read: CallId) -> ReadAnswer {
    for _ in 0..100 {
        while drivers.iter_mut().fold(false, |busy, d| d.poll() | busy) {}
        if let Some(reply) = drivers[0].take_reads().into_iter().find(|r| r.read == read) {
            return reply.answer;
        }
        world.clock.advance(TICK);
    }
    panic!("read {read}: no answer in 100 ticks");
}

#[test]
fn a_read_at_a_replica_serves_the_records_after_a_quorum_of_watermarks() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    world.run(&mut drivers, "the leader opens", open);
    let set = drivers[0].call(LEADER, claim());
    answer(&world, &mut drivers, 0, set);
    let w0 = drivers[0].call(LEADER, write(0, "a0"));
    // The read leaves beside the write: its answer may wait on the batch
    // of the replica that holds the write.
    let replica = Address::replica(J, NodeId(11), SegmentId(1));
    let read = drivers[0].read(replica, Seq(0), 8);
    assert!(read > w0, "reads and calls share one counter");
    let answer_of_read = read_answer(&world, &mut drivers, read);
    let ReadAnswer::Page {
        records,
        view,
        sealed,
    } = answer_of_read
    else {
        panic!("not a page: {answer_of_read:?}");
    };
    assert!(!sealed);
    assert!(view.next_seq <= Seq(1));
    assert!(
        records
            .iter()
            .all(|(seq, record)| *seq == Seq(0) && record.payload == b"a0".to_vec())
    );
    assert_eq!(answer(&world, &mut drivers, 0, w0), written(0));
    let again = drivers[0].read(replica, Seq(0), 8);
    let ReadAnswer::Page { records, .. } = read_answer(&world, &mut drivers, again) else {
        panic!("not a page");
    };
    assert_eq!(records.len(), 1, "the write is chosen before this read");
}

#[test]
fn a_read_at_a_role_that_stops_is_unserved() {
    let world = World::new();
    let disk = Shared::default();
    // The acceptors are not served: the read waits for its watermarks.
    let mut drivers = [world.process(&disk, &[11])];
    let replica = Address::replica(J, NodeId(11), SegmentId(1));
    while drivers[0].poll() {}
    let read = drivers[0].read(replica, Seq(0), 8);
    while drivers[0].poll() {}
    assert!(
        drivers[0].take_reads().is_empty(),
        "no quorum of watermarks"
    );
    drivers[0].unassign(Address::new(J, NodeId(11)));
    while drivers[0].poll() {}
    assert_eq!(
        drivers[0].take_reads(),
        vec![ReadReply {
            address: replica,
            read,
            answer: ReadAnswer::Unserved,
        }]
    );
    let unserved = drivers[0].read(replica, Seq(0), 8);
    assert_eq!(
        drivers[0].take_reads(),
        vec![ReadReply {
            address: replica,
            read: unserved,
            answer: ReadAnswer::Unserved,
        }],
        "a role that is not served answers at once"
    );
}

#[test]
fn a_read_that_a_stop_drops_is_unserved_and_a_dropped_notice_is_lost() {
    let mut ports = JournalPorts::new(MemRegister::new());
    let replica = Address::replica(J, NodeId(11), SegmentId(1));
    let view = JournalView {
        leader: Leader::Unset,
        next_seq: Seq(0),
        first_seq: Seq(0),
    };
    // The page waited on a batch of its stopped replica: the fold it read
    // may hold a batch that is lost, so the reader learns only `Unserved`.
    ports.dropped(
        replica,
        Output::ReadReply {
            read: 7,
            answer: ReadAnswer::Truncated(view),
        },
    );
    assert_eq!(
        ports.reads,
        vec![ReadReply {
            address: replica,
            read: 7,
            answer: ReadAnswer::Unserved,
        }]
    );
    ports.dropped(
        replica,
        Output::Retired {
            segment: SegmentId(1),
        },
    );
    assert_eq!(ports.reads.len(), 1);
    assert_eq!(ports.replies, Vec::new());
    assert_eq!(ports.notices, Vec::new());
}

#[test]
fn a_seal_whose_reply_is_lost_opens_the_leader_from_the_done_value() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    // The seal of the second leader takes place, and its reply is lost: the
    // leader learns the done seal from the register, and opens.
    let (_, from) = second_leader(&world, &mut drivers, |world| {
        world.register.lose_next(true);
    });
    let value = world.register.value(J).expect("the register");
    let first = value.segment(SegmentId(1)).expect("segment 1");
    assert_eq!(first.end(), Some(from), "the lost seal took place");
}

#[test]
fn a_seal_whose_request_is_lost_takes_place_later() {
    let world = World::new();
    let disk = Shared::default();
    let mut drivers = vec![world.process(&disk, &[1, 2, 3, 11, 12, 13, 100])];
    let (_, from) = second_leader(&world, &mut drivers, |world| {
        world.register.lose_next(false);
    });
    let value = world.register.value(J).expect("the register");
    let first = value.segment(SegmentId(1)).expect("segment 1");
    assert_eq!(first.end(), Some(from), "the leader sends the seal again");
}

/// Whether segment 1 of the register is trimmed.
fn first_trimmed(world: &World) -> bool {
    world
        .register
        .value(J)
        .and_then(|v| v.segment(SegmentId(1)).map(SegmentRecord::state))
        == Some(SegmentState::Trimmed)
}

#[test]
fn a_trim_whose_request_is_lost_runs_again_at_the_next_timer() {
    // A replica asks the trim of a retired segment once for each value it
    // observes, so only the driver asks a lost trim again: no new value
    // comes while the trim waits.
    let world = retired_world();
    let disk = Shared::default();
    let mut driver = world.process(&disk, &[]);
    let address = Address::new(J, NodeId(21));
    driver.transport_mut().endpoint.route(address);
    world.register.lose_next(false);
    driver.assign(Assignment::Replica {
        address,
        segment: SegmentId(2),
        set: [21, 22, 23].map(NodeId).to_vec(),
        initial: Leader::Unset,
    });
    while driver.poll() {}
    assert!(!first_trimmed(&world), "the first request is lost");
    world.run(&mut [driver], "the trim runs again", |_| {
        first_trimmed(&world)
    });
}

#[test]
fn a_change_with_no_answer_of_a_role_that_stopped_is_not_run_again() {
    let world = retired_world();
    let disk = Shared::default();
    let mut driver = world.process(&disk, &[]);
    let address = Address::new(J, NodeId(21));
    driver.transport_mut().endpoint.route(address);
    world.register.lose_next(false);
    let replica = Assignment::Replica {
        address,
        segment: SegmentId(2),
        set: [21, 22, 23].map(NodeId).to_vec(),
        initial: Leader::Unset,
    };
    driver.assign(replica.clone());
    while driver.poll() {}
    assert!(!first_trimmed(&world), "the first request is lost");
    // The role stops, and a new one starts at its address: the change of the
    // role that stopped is not the new role's.
    driver.unassign(address);
    while driver.poll() {}
    assert!(
        driver.inner.ports().unanswered.is_empty(),
        "a stop drops the changes of the role"
    );
    driver.assign(replica);
    while driver.poll() {}
    assert!(first_trimmed(&world), "the new role asks its own trim");
}
