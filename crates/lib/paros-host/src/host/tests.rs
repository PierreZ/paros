use super::*;
use crate::toy::{Assignment, In, LIMIT, Note, Out, Stop, TICK, Toy, ToyRole};

fn host() -> Host<ToyRole> {
    Host::new(0)
}

/// A watcher that has not seen a value, nor its draw.
const FRESH: ToyRole = ToyRole::Watcher {
    seen: 0,
    jitter: None,
    woke: false,
};

fn counter(address: u32, peers: Vec<u32>) -> Input<Toy> {
    Input::Assign(Assignment::Counter { address, peers })
}

fn watcher(address: u32) -> Input<Toy> {
    Input::Assign(Assignment::Watcher { address })
}

#[test]
fn a_role_with_no_disk_runs_at_once_and_its_draw_is_an_input() {
    let mut host = host();
    let effects = host.step(Time(5), watcher(2));
    assert_eq!(effects, vec![Effect::Draw { address: 2 }]);
    assert!(host.serves(2));
    assert_eq!(host.role(2), Some(&FRESH));
    assert_eq!(host.deadline(2), None, "no timer before the draw");
    // The drawn value sets the jitter of the role's timer.
    let effects = host.step(
        Time(6),
        Input::Drawn {
            address: 2,
            value: 13,
        },
    );
    assert_eq!(
        effects,
        vec![Effect::SetTimer {
            address: 2,
            at: Time(6 + TICK + 3),
        }]
    );
    assert_eq!(host.deadline(2), Some(Time(6 + TICK + 3)));
    // A draw for a role that the host does not serve is lost.
    assert_eq!(
        host.step(
            Time(7),
            Input::Drawn {
                address: 9,
                value: 1
            }
        ),
        Vec::new()
    );
}

#[test]
fn a_role_with_a_disk_reads_its_records_first_and_loses_its_inputs_until_then() {
    let mut host = host();
    let effects = host.step(Time(0), counter(1, vec![2]));
    assert_eq!(effects, vec![Effect::Read { address: 1 }]);
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
    assert_eq!(
        effects,
        vec![Effect::SetTimer {
            address: 1,
            at: Time(2 + TICK),
        }],
        "the role sets its timer when it starts"
    );
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
    assert_eq!(host.role(2), Some(&FRESH));
}

#[test]
fn records_that_refuse_a_start_stop_the_role() {
    let mut host: Host<ToyRole> = Host::new(9);
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
fn a_timer_fires_its_role_alone_and_only_once_due() {
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
    host.step(
        Time(0),
        Input::Drawn {
            address: 2,
            value: 4,
        },
    );
    assert_eq!(host.deadline(1), Some(Time(TICK)));
    assert_eq!(host.deadline(2), Some(Time(TICK + 4)));
    // A fire before the deadline, or for a role with no timer, changes
    // nothing.
    assert_eq!(
        host.step(Time(TICK - 1), Input::TimerFired { address: 1 }),
        Vec::new()
    );
    assert_eq!(
        host.step(Time(TICK), Input::TimerFired { address: 9 }),
        Vec::new()
    );
    let effects = host.step(Time(TICK), Input::TimerFired { address: 1 });
    assert_eq!(
        effects,
        vec![
            Effect::Output {
                address: 1,
                output: Out::Report { value: 5 },
                after: Some(3),
            },
            Effect::SetTimer {
                address: 1,
                at: Time(2 * TICK),
            },
        ]
    );
    assert_eq!(
        host.role(2),
        Some(&ToyRole::Watcher {
            seen: 0,
            jitter: Some(4),
            woke: false,
        }),
        "the fire of a role reaches no other role"
    );
    assert_eq!(
        host.step(Time(TICK + 4), Input::TimerFired { address: 2 }),
        Vec::new()
    );
    assert_eq!(host.deadline(2), None, "a timer that fired is gone");
    assert!(matches!(
        host.role(2),
        Some(ToyRole::Watcher { woke: true, .. })
    ));
    // The timer fired once: a second fire is stale.
    host.step(Time(TICK + 5), Input::TimerFired { address: 2 });
    assert_eq!(host.deadline(2), None);
}

#[test]
fn a_stop_takes_the_timer_of_its_role() {
    let mut host = host();
    host.step(Time(0), counter(1, vec![]));
    host.step(
        Time(0),
        Input::Recovered {
            address: 1,
            records: Vec::new(),
        },
    );
    assert_eq!(host.deadline(1), Some(Time(TICK)));
    host.step(Time(1), Input::Unassign(1));
    assert_eq!(host.deadline(1), None);
    // A late fire of a stopped role is lost.
    assert_eq!(
        host.step(Time(TICK), Input::TimerFired { address: 1 }),
        Vec::new()
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
    assert_eq!(effects.len(), 1, "a value at the floor starts");
}

#[test]
fn a_draw_that_the_role_did_not_ask_for_is_lost() {
    let mut host = host();
    host.step(Time(0), watcher(2));
    // The watcher stops before its draw comes, and a counter takes the
    // address: the value is not the counter's.
    host.step(Time(1), Input::Unassign(2));
    host.step(Time(1), counter(2, vec![]));
    host.step(
        Time(1),
        Input::Recovered {
            address: 2,
            records: Vec::new(),
        },
    );
    let late = Input::Drawn {
        address: 2,
        value: 5,
    };
    assert_eq!(host.step(Time(2), late.clone()), Vec::new());
    // A watcher gets one value for its one ask; a second is lost.
    host.step(Time(3), watcher(4));
    let first = Input::Drawn {
        address: 4,
        value: 5,
    };
    assert_eq!(host.step(Time(3), first.clone()).len(), 1);
    assert_eq!(host.step(Time(4), first), Vec::new());
}
