use std::cell::RefCell;
use std::rc::Rc;

use super::*;
use crate::host::{Host, Input};
use crate::toy::{Assignment, Disk, Hand, In, Note, Out, Stop, Toy, ToyRole, Wire};

const TICK: u64 = 10;

/// Ports that keep every output, and every output that a stop dropped.
#[derive(Default)]
struct Kept {
    outputs: Vec<(u32, Out)>,
    dropped: Vec<(u32, Out)>,
    /// Inputs that the next poll gives the host.
    next: Vec<Input<Toy>>,
}

impl Ports<Host<ToyRole>> for Kept {
    fn poll(&mut self, _machine: &Host<ToyRole>) -> Vec<Input<Toy>> {
        std::mem::take(&mut self.next)
    }

    fn output(&mut self, address: u32, output: Out) -> Vec<Input<Toy>> {
        self.outputs.push((address, output));
        Vec::new()
    }

    fn dropped(&mut self, address: u32, output: Out) {
        self.dropped.push((address, output));
    }
}

type Events = Rc<RefCell<Vec<Event<Input<Toy>, Toy>>>>;

struct Shared(Events);

impl crate::ports::Events<Host<ToyRole>> for Shared {
    fn record(&mut self, event: Event<Input<Toy>, Toy>) {
        self.0.borrow_mut().push(event);
    }
}

type Proc = Driver<Host<ToyRole>, Disk, Wire, Hand, Kept, Shared>;

fn proc(disk: &Disk, clock: &Hand) -> (Proc, Events) {
    let events: Events = Rc::default();
    let driver = Driver::new(
        Host::new(TICK, 0),
        disk.clone(),
        Wire::default(),
        clock.clone(),
        Kept::default(),
        Shared(events.clone()),
    );
    (driver, events)
}

fn bump(driver: &mut Proc) {
    driver.input(Input::Deliver {
        address: 1,
        input: In::Bump,
    });
}

fn start(driver: &mut Proc) {
    driver.input(Input::Assign(Assignment::Counter {
        address: 1,
        peers: vec![2],
    }));
    driver.input(Input::Assign(Assignment::Watcher { address: 2 }));
}

#[test]
fn a_send_and_an_output_wait_for_the_commit_of_their_batch() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, events) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    bump(&mut driver);
    assert!(
        driver.ports().outputs.is_empty(),
        "nothing goes before the commit"
    );
    assert!(
        disk.records.borrow().is_empty(),
        "the batches wait in the group"
    );
    // The persist steps are not reported while their batches are in flight.
    let reported = events.borrow().len();
    assert!(driver.poll());
    assert_eq!(
        disk.records
            .borrow()
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<_>>(),
        vec![((1, 1), 1), ((1, 2), 2)],
        "the two batches commit in one write"
    );
    assert_eq!(
        driver.ports().outputs,
        vec![(1, Out::Bumped { value: 1 }), (1, Out::Bumped { value: 2 })]
    );
    assert!(events.borrow().len() > reported);
    // The held notes reach the watcher at the next round.
    assert!(driver.poll());
    assert_eq!(
        driver.ports().outputs[2..],
        [(2, Out::Seen { value: 1 }), (2, Out::Seen { value: 2 })]
    );
    assert!(!driver.poll(), "a quiet round does no work");
}

#[test]
fn a_failed_commit_stops_the_role_and_drops_what_waited_on_it() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, events) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    disk.fail_commit.set(true);
    assert!(driver.poll());
    assert_eq!(driver.take_stopped(), vec![(1, Stop::Failed)]);
    assert_eq!(driver.ports().dropped, vec![(1, Out::Bumped { value: 1 })]);
    assert_eq!(driver.ports().outputs, Vec::new());
    assert!(
        driver.transport_mut().queue.borrow().is_empty(),
        "the note never goes"
    );
    // Every step is reported once its batch is settled, failed too.
    let last = events.borrow().last().cloned().expect("a step");
    assert_eq!(last.input, Input::Failed { address: 1 });
}

#[test]
fn a_role_that_ends_keeps_its_batches_and_a_restart_reads_them_before_the_commit() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    driver.input(Input::Deliver {
        address: 1,
        input: In::End,
    });
    assert_eq!(driver.take_stopped(), vec![(1, Stop::Done)]);
    // The batch stays in the group: a start in the same round reads it on
    // top of the disk, and goes on above it.
    driver.input(Input::Assign(Assignment::Counter {
        address: 1,
        peers: vec![2],
    }));
    assert!(matches!(
        driver.machine().role(1),
        Some(ToyRole::Counter {
            value: 1,
            last: 1,
            ..
        })
    ));
    bump(&mut driver);
    assert!(driver.poll());
    assert_eq!(
        disk.records
            .borrow()
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<_>>(),
        vec![((1, 1), 1), ((1, 2), 2)]
    );
    // A new driver on the same disk recovers the last value.
    let (mut again, _) = proc(&disk, &clock);
    start(&mut again);
    assert!(matches!(
        again.machine().role(1),
        Some(ToyRole::Counter {
            value: 2,
            last: 2,
            ..
        })
    ));
}

#[test]
fn a_restart_inside_one_round_overlays_the_group_on_the_scan() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    driver.input(Input::Unassign(1));
    // The stop took the batch out of the group: it is lost, as in a crash.
    driver.input(Input::Assign(Assignment::Counter {
        address: 1,
        peers: vec![2],
    }));
    assert!(matches!(
        driver.machine().role(1),
        Some(ToyRole::Counter { value: 0, .. })
    ));
    bump(&mut driver);
    assert!(driver.poll());
    assert_eq!(
        disk.records
            .borrow()
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<_>>(),
        vec![((1, 1), 1)]
    );
}

#[test]
fn an_unreadable_disk_stops_the_role_that_starts() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    disk.fail_scan.set(true);
    start(&mut driver);
    assert_eq!(driver.take_stopped(), vec![(1, Stop::Unreadable)]);
    assert!(driver.machine().serves(2));
}

#[test]
fn an_output_that_does_not_wait_is_dropped_before_its_batch_is_durable() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    start(&mut driver);
    assert_eq!(driver.timer(), Some(Time(TICK)));
    bump(&mut driver);
    clock.0.set(TICK);
    // The timer fires before the commit: the report names a batch that is
    // not durable, so the driver drops it.
    assert!(driver.poll());
    assert!(
        !driver
            .ports()
            .outputs
            .iter()
            .any(|(_, out)| matches!(out, Out::Report { .. }))
    );
    assert_eq!(driver.timer(), Some(Time(2 * TICK)));
    clock.0.set(2 * TICK);
    assert!(driver.poll());
    assert!(
        driver
            .ports()
            .outputs
            .contains(&(1, Out::Report { value: 1 }))
    );
}

#[test]
fn the_ports_give_inputs_at_each_round() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    start(&mut driver);
    driver.ports_mut().next.push(Input::Message(Note {
        from: 1,
        to: 2,
        value: 1,
    }));
    assert!(driver.poll());
    assert_eq!(driver.ports().outputs, vec![(2, Out::Seen { value: 1 })]);
    let (_, _, _, ports, _) = driver.into_parts();
    assert_eq!(ports.dropped, Vec::new());
}

#[test]
fn a_stop_drops_only_the_held_outputs_of_its_role() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    // Two outputs that wait on a batch of their role, as inside a round.
    driver.held_outputs.push((1, 1, Out::Bumped { value: 7 }));
    driver.held_outputs.push((1, 3, Out::Bumped { value: 8 }));
    driver.on_stopped(1, Stop::Unassigned);
    assert_eq!(driver.ports().dropped, vec![(1, Out::Bumped { value: 7 })]);
    assert_eq!(
        driver.held_outputs,
        vec![(1, 3, Out::Bumped { value: 8 })],
        "the output of a role that runs still waits on its batch"
    );
    assert_eq!(driver.take_stopped(), vec![(1, Stop::Unassigned)]);
    driver.held_outputs.clear();
}

#[test]
fn a_role_that_ends_keeps_no_durable_entry_after_its_last_commit() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    driver.input(Input::Deliver {
        address: 1,
        input: In::End,
    });
    assert_eq!(driver.take_stopped(), vec![(1, Stop::Done)]);
    // The batch of the role that ended reaches the disk, but the role is
    // gone: a role at its address later starts from its own read.
    assert!(driver.poll());
    assert_eq!(disk.records.borrow().len(), 1, "the last batch is durable");
    assert!(
        !driver.durable.contains_key(&1),
        "a role that ended keeps no durable entry"
    );
}
