use super::*;
use crate::toy::{Assignment, In, LIMIT, Note, Out, Stop, Toy, ToyRole};

const TICK: u64 = 10;

fn host() -> Host<ToyRole> {
    Host::new(TICK, 0)
}

fn counter(address: u32, peers: Vec<u32>) -> Input<Toy> {
    Input::Assign(Assignment::Counter { address, peers })
}

fn watcher(address: u32) -> Input<Toy> {
    Input::Assign(Assignment::Watcher { address })
}

#[test]
fn a_role_with_no_disk_runs_at_once_and_the_first_assign_arms_the_timer() {
    let mut host = host();
    let effects = host.step(Time(5), watcher(2));
    assert_eq!(effects, vec![Effect::SetTimer { at: Time(15) }]);
    assert!(host.serves(2));
    assert_eq!(host.role(2), Some(&ToyRole::Watcher { seen: 0 }));
    // A second role does not arm the timer again.
    assert_eq!(host.step(Time(6), watcher(3)), Vec::new());
}

#[test]
fn a_role_with_a_disk_reads_its_records_first_and_loses_its_inputs_until_then() {
    let mut host = host();
    let effects = host.step(Time(0), counter(1, vec![2]));
    assert_eq!(
        effects,
        vec![
            Effect::SetTimer { at: Time(TICK) },
            Effect::Read { address: 1 }
        ]
    );
    assert!(host.serves(1));
    assert!(host.role(1).is_none(), "a role that recovers does not run");
    assert!(host.pending(1).is_some());
    let lost = host.step(
        Time(1),
        Input::Deliver {
            address: 1,
            input: In::Bump,
        },
    );
    assert_eq!(lost, Vec::new(), "a role that recovers loses its inputs");
    let effects = host.step(
        Time(2),
        Input::Recovered {
            address: 1,
            records: vec![(1, 4), (2, 7)],
        },
    );
    assert_eq!(effects, Vec::new());
    assert_eq!(
        host.role(1),
        Some(&ToyRole::Counter {
            value: 7,
            last: 2,
            peers: vec![2],
            ended: false,
        })
    );
    // A late read of a role that runs changes nothing.
    let late = host.step(
        Time(3),
        Input::Recovered {
            address: 1,
            records: Vec::new(),
        },
    );
    assert_eq!(late, Vec::new());
    assert!(matches!(
        host.role(1),
        Some(ToyRole::Counter { value: 7, .. })
    ));
}

#[test]
fn an_address_takes_one_role_and_placement_may_say_it_again() {
    let mut host = host();
    host.step(Time(0), watcher(2));
    assert_eq!(host.step(Time(1), watcher(2)), Vec::new());
    assert_eq!(host.step(Time(1), counter(2, vec![])), Vec::new());
    assert_eq!(host.role(2), Some(&ToyRole::Watcher { seen: 0 }));
}

#[test]
fn records_that_refuse_a_start_stop_the_role() {
    let mut host: Host<ToyRole> = Host::new(TICK, 9);
    host.step(Time(0), counter(1, vec![]));
    let effects = host.step(
        Time(1),
        Input::Recovered {
            address: 1,
            records: vec![(1, 4)],
        },
    );
    assert_eq!(
        effects,
        vec![Effect::Stopped {
            address: 1,
            why: Stop::Damaged,
        }]
    );
    assert!(!host.serves(1));
}

#[test]
fn an_unreadable_disk_stops_only_a_role_that_recovers() {
    let mut host = host();
    host.step(Time(0), counter(1, vec![]));
    host.step(Time(0), watcher(2));
    assert_eq!(
        host.step(Time(1), Input::Unreadable { address: 1 }),
        vec![Effect::Stopped {
            address: 1,
            why: Stop::Unreadable,
        }]
    );
    assert_eq!(
        host.step(Time(1), Input::Unreadable { address: 2 }),
        Vec::new()
    );
    assert!(host.serves(2));
}

#[test]
fn a_message_goes_to_the_role_it_names_and_a_role_ends_by_its_own_step() {
    let mut host = host();
    host.step(Time(0), watcher(2));
    for value in 1..LIMIT {
        let effects = host.step(
            Time(value),
            Input::Message(Note {
                from: 1,
                to: 2,
                value,
            }),
        );
        assert_eq!(
            effects,
            vec![Effect::Output {
                address: 2,
                output: Out::Seen { value },
                after: None,
            }]
        );
    }
    // A message for a role that the process does not serve is lost.
    let lost = Input::Message(Note {
        from: 1,
        to: 9,
        value: 1,
    });
    assert_eq!(host.step(Time(5), lost), Vec::new());
    let effects = host.step(
        Time(6),
        Input::Message(Note {
            from: 1,
            to: 2,
            value: LIMIT,
        }),
    );
    assert_eq!(
        effects,
        vec![
            Effect::Output {
                address: 2,
                output: Out::Seen { value: LIMIT },
                after: None,
            },
            Effect::Stopped {
                address: 2,
                why: Stop::Done,
            },
        ]
    );
    assert!(!host.serves(2));
}

#[test]
fn the_timer_steps_every_role_that_runs_and_arms_again() {
    let mut host = host();
    host.step(Time(0), counter(1, vec![]));
    host.step(
        Time(0),
        Input::Recovered {
            address: 1,
            records: vec![(3, 5)],
        },
    );
    host.step(Time(0), watcher(2));
    let effects = host.step(Time(10), Input::TimerFired);
    assert_eq!(
        effects,
        vec![
            Effect::Output {
                address: 1,
                output: Out::Report { value: 5 },
                after: Some(3),
            },
            Effect::SetTimer { at: Time(20) },
        ]
    );
}

#[test]
fn an_unassign_stops_the_role_and_a_failed_commit_stops_it_too() {
    let mut host = host();
    host.step(Time(0), watcher(2));
    host.step(Time(0), watcher(3));
    assert_eq!(
        host.step(Time(1), Input::Unassign(2)),
        vec![Effect::Stopped {
            address: 2,
            why: Stop::Unassigned,
        }]
    );
    assert_eq!(host.step(Time(1), Input::Unassign(2)), Vec::new());
    assert_eq!(
        host.step(Time(1), Input::Failed { address: 3 }),
        vec![Effect::Stopped {
            address: 3,
            why: Stop::Failed,
        }]
    );
    assert_eq!(host.addresses().count(), 0);
}

#[test]
fn the_shared_state_is_what_every_start_reads() {
    let mut host = host();
    *host.shared_mut() = 8;
    assert_eq!(*host.shared(), 8);
    host.step(Time(0), counter(1, vec![]));
    let effects = host.step(
        Time(1),
        Input::Recovered {
            address: 1,
            records: vec![(1, 8)],
        },
    );
    assert_eq!(effects, Vec::new(), "a value at the floor starts");
}

#[test]
#[should_panic(expected = "the timer moves time forward")]
fn a_tick_of_zero_is_refused() {
    let _ = Host::<ToyRole>::new(0, 0);
}
