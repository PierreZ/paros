use std::cell::RefCell;
use std::rc::Rc;

use super::*;
use crate::host::{Host, Input};
use crate::toy::{Assignment, Dice, Disk, Hand, In, Note, Out, Stop, TICK, Toy, ToyRole, Wire};

/// Ports that keep every output, and every output that a stop dropped.
#[derive(Default)]
struct Kept {
    outputs: Vec<(u32, Out)>,
    dropped: Vec<(u32, Out)>,
    /// Inputs that the next poll gives the host.
    next: Vec<Input<Toy>>,
    /// Inputs that the next output gives the host.
    on_output: Vec<Input<Toy>>,
}

impl Ports<Host<ToyRole>> for Kept {
    fn poll(&mut self, _machine: &Host<ToyRole>) -> Vec<Input<Toy>> {
        std::mem::take(&mut self.next)
    }

    fn output(&mut self, address: u32, output: Out) -> Vec<Input<Toy>> {
        self.outputs.push((address, output));
        std::mem::take(&mut self.on_output)
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

type Proc = Driver<Host<ToyRole>, Disk, Wire, Hand, Kept, Shared, Dice>;

fn proc(disk: &Disk, clock: &Hand) -> (Proc, Events) {
    let events: Events = Rc::default();
    let driver = Driver::new(
        Host::new(0),
        disk.clone(),
        Wire::default(),
        clock.clone(),
        Kept::default(),
        Shared(events.clone()),
    )
    .with_entropy(Dice::default());
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
    disk.fail_commit.set(Some(StorageError::Full));
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
    disk.fail_scan.set(Some(StorageError::Full));
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
    // The counter's next fire, and the watcher's timer with its draw of 7.
    assert_eq!(driver.timer(), Some(Time(TICK + 7)));
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

#[test]
fn a_recorded_run_replays_to_the_same_effects() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, events) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    assert!(driver.poll());
    for at in [TICK, TICK + 7, 2 * TICK, 3 * TICK] {
        clock.0.set(at);
        driver.poll();
    }
    bump(&mut driver);
    assert!(driver.poll());
    let recorded = events.borrow().clone();
    assert!(
        recorded
            .iter()
            .any(|e| matches!(e.input, Input::Drawn { .. })),
        "the run draws"
    );
    assert!(
        recorded
            .iter()
            .any(|e| e.input == Input::TimerFired { address: 2 }),
        "the watcher's own timer fires"
    );
    assert!(
        recorded
            .iter()
            .filter(|e| e.input == Input::TimerFired { address: 1 })
            .count()
            >= 2,
        "the counter's timer fires again"
    );
    // A new host, with no disk, no clock and no random source: the recorded
    // inputs alone give the same effects, step by step.
    let mut again: Host<ToyRole> = Host::new(0);
    for event in &recorded {
        assert_eq!(
            again.step(event.now, event.input.clone()),
            event.effects,
            "the step of {:?} at {:?}",
            event.input,
            event.now
        );
    }
    assert_eq!(again.role(1), driver.machine().role(1));
    assert_eq!(again.role(2), driver.machine().role(2));
}

#[test]
fn a_timer_set_again_inside_the_round_of_fires_is_kept() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    for address in [1, 3] {
        driver.input(Input::Assign(Assignment::Counter {
            address,
            peers: Vec::new(),
        }));
    }
    bump(&mut driver);
    assert!(driver.poll());
    // Both timers are due at TICK. The report of counter 1 restarts counter
    // 3, which sets a new timer before its own old fire comes.
    clock.0.set(TICK);
    driver.ports_mut().on_output = vec![
        Input::Unassign(3),
        Input::Assign(Assignment::Counter {
            address: 3,
            peers: Vec::new(),
        }),
    ];
    assert!(driver.poll());
    assert_eq!(driver.machine().deadline(3), Some(Time(2 * TICK)));
    assert_eq!(
        driver.timers.get(&3),
        Some(&Time(2 * TICK)),
        "the driver keeps the timer that the host keeps"
    );
}

#[test]
fn a_disk_lists_each_role_with_records_fresh_and_after_a_restart() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    assert_eq!(driver.on_disk(), Ok(Vec::new()), "a fresh disk lists none");
    start(&mut driver);
    bump(&mut driver);
    assert!(driver.poll());
    // The watcher keeps no record: only the counter is on the disk.
    assert_eq!(driver.on_disk(), Ok(vec![1]));
    drop(driver);
    let (mut again, _) = proc(&disk, &clock);
    assert_eq!(again.on_disk(), Ok(vec![1]));
    assert!(!again.halted());
}

#[test]
fn a_list_that_read_nothing_may_be_asked_again_and_a_lost_list_stops_the_process() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    disk.fail_list.set(Some(StorageError::Full));
    assert_eq!(driver.on_disk(), Err(StorageError::Full));
    assert!(
        !driver.halted(),
        "a list that read nothing may be asked again"
    );
    assert_eq!(driver.on_disk(), Ok(Vec::new()));
    disk.fail_list.set(Some(StorageError::Lost));
    assert_eq!(driver.on_disk(), Err(StorageError::Lost));
    assert!(driver.halted(), "a list that lost a role cannot be trusted");
    assert_eq!(driver.on_disk(), Err(StorageError::Unknown));
}

#[test]
fn a_lost_record_stops_its_role_for_good_and_the_list_still_names_it() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    assert!(driver.poll());
    drop(driver);
    disk.lose(1);
    assert!(disk.records.borrow().is_empty(), "the one record is gone");
    let (mut again, _) = proc(&disk, &clock);
    // Faulty, not empty: the list names the role whose record is gone.
    assert_eq!(again.on_disk(), Ok(vec![1]));
    start(&mut again);
    assert_eq!(again.take_stopped(), vec![(1, Stop::Lost)]);
    assert!(!again.machine().serves(1));
    assert!(again.machine().serves(2), "the other role runs");
    // Placement asks again: the scan reports the loss again, so the role
    // never starts from what the loss left.
    again.input(Input::Assign(Assignment::Counter {
        address: 1,
        peers: vec![2],
    }));
    assert_eq!(again.take_stopped(), vec![(1, Stop::Lost)]);
    assert!(!again.halted());
}

#[test]
fn a_commit_that_finds_a_lost_record_stops_the_process_and_no_role_for_good() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    disk.fail_commit.set(Some(StorageError::Lost));
    assert!(driver.poll());
    // The commit cannot name the role that lost a record: no role stops for
    // good, and the scans of the next process name it.
    assert!(driver.halted());
    assert_eq!(driver.take_stopped(), Vec::new());
    assert_eq!(driver.ports().dropped, vec![(1, Out::Bumped { value: 1 })]);
    assert_eq!(driver.ports().outputs, Vec::new());
    assert!(driver.transport_mut().queue.borrow().is_empty());
}

#[test]
fn a_halt_reports_the_steps_that_are_settled_and_steps_nothing_more() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, events) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    assert!(driver.poll());
    // The note of value 1 waits in the transport. In the next round the
    // watcher sees it (an output with no batch, out at once), then a bump
    // from the ports writes a batch whose commit is `Unknown`, and the
    // round's later inputs never step.
    driver.ports_mut().next = vec![
        Input::Deliver {
            address: 1,
            input: In::Bump,
        },
        Input::Deliver {
            address: 1,
            input: In::Bump,
        },
    ];
    disk.fail_commit.set(Some(StorageError::Unknown));
    assert!(driver.poll());
    assert!(driver.halted());
    assert!(
        driver
            .ports()
            .outputs
            .contains(&(2, Out::Seen { value: 1 }))
    );
    let recorded = events.borrow();
    assert!(
        recorded.iter().any(|e| matches!(
            e.input,
            Input::Message(Note {
                from: 1,
                to: 2,
                value: 1
            })
        )),
        "the step whose output went out is reported"
    );
    assert!(
        !recorded.iter().any(|e| e
            .effects
            .iter()
            .any(|f| matches!(f, Effect::Persist { id: 2.., .. }))),
        "no step that waits on the unknown batch is reported"
    );
}

#[test]
fn an_unknown_commit_stops_the_process() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, events) = proc(&disk, &clock);
    start(&mut driver);
    bump(&mut driver);
    disk.fail_commit.set(Some(StorageError::Unknown));
    assert!(driver.poll());
    assert!(driver.halted());
    assert_eq!(
        driver.take_stopped(),
        Vec::new(),
        "no role stops: the process does"
    );
    assert_eq!(driver.ports().dropped, vec![(1, Out::Bumped { value: 1 })]);
    assert_eq!(driver.ports().outputs, Vec::new());
    assert!(
        driver.transport_mut().queue.borrow().is_empty(),
        "the note never goes"
    );
    assert_eq!(driver.timer(), None);
    // The starts are settled and reported; the bump, whose batch is
    // unknown, is not.
    let recorded = events.borrow();
    assert!(recorded.iter().any(|e| matches!(e.input, Input::Assign(_))));
    assert!(
        !recorded.iter().any(|e| e
            .effects
            .iter()
            .any(|f| matches!(f, Effect::Persist { .. }))),
        "a step whose batch is unknown is not reported"
    );
    drop(recorded);
    // A halted driver does nothing more.
    bump(&mut driver);
    assert!(!driver.poll());
    assert!(disk.records.borrow().is_empty());
    // A new process reads the disk: this backend wrote nothing.
    let (mut again, _) = proc(&disk, &clock);
    start(&mut again);
    assert!(matches!(
        again.machine().role(1),
        Some(ToyRole::Counter { value: 0, .. })
    ));
}

#[test]
fn an_unknown_scan_stops_the_process_at_once() {
    let (disk, clock) = (Disk::default(), Hand::default());
    let (mut driver, _) = proc(&disk, &clock);
    disk.fail_scan.set(Some(StorageError::Unknown));
    start(&mut driver);
    assert!(driver.halted());
    assert!(!driver.machine().serves(2), "no input steps after the halt");
    assert_eq!(driver.take_stopped(), Vec::new());
}

#[test]
fn each_fault_has_one_verdict() {
    assert_eq!(StorageError::Full.verdict(), Verdict::Again);
    assert_eq!(StorageError::Lost.verdict(), Verdict::ForGood);
    assert_eq!(StorageError::Unknown.verdict(), Verdict::Halt);
    assert_eq!(
        StorageError::Lost.to_string(),
        "storage: the disk lost a record of the role"
    );
    assert_eq!(
        StorageError::Full.to_string(),
        "storage: the call did nothing"
    );
    assert_eq!(
        StorageError::Unknown.to_string(),
        "storage: the backend does not know what the call did"
    );
}
