use super::*;

const WINDOWS: Windows = Windows {
    suspect_after: 10,
    down_after: 30,
};
const A: ProcessId = ProcessId(1);
const B: ProcessId = ProcessId(2);

#[test]
fn a_process_never_heard_ages_from_the_start() {
    let signals = Signals::new(100);
    assert_eq!(signals.liveness(A, 100, WINDOWS), Liveness::Up);
    assert_eq!(signals.liveness(A, 109, WINDOWS), Liveness::Up);
    assert_eq!(signals.liveness(A, 110, WINDOWS), Liveness::Suspect);
    assert_eq!(signals.liveness(A, 129, WINDOWS), Liveness::Suspect);
    assert_eq!(signals.liveness(A, 130, WINDOWS), Liveness::Down);
    assert_eq!(signals.signal_age(130), 30);
}

#[test]
fn a_report_makes_its_process_up_again() {
    let mut signals = Signals::new(0);
    signals.publish(50, Report::Beat { process: A });
    assert_eq!(signals.liveness(A, 59, WINDOWS), Liveness::Up);
    assert_eq!(signals.liveness(A, 60, WINDOWS), Liveness::Suspect);
    assert_eq!(signals.liveness(A, 80, WINDOWS), Liveness::Down);
    // The other process is not heard: it ages from the start.
    assert_eq!(signals.liveness(B, 50, WINDOWS), Liveness::Down);
    signals.publish(80, Report::Beat { process: A });
    assert_eq!(signals.liveness(A, 80, WINDOWS), Liveness::Up);
}

#[test]
fn the_signal_age_is_the_time_since_the_newest_report() {
    let mut signals = Signals::new(0);
    assert_eq!(signals.signal_age(7), 7);
    signals.publish(5, Report::Beat { process: A });
    signals.publish(9, Report::Beat { process: B });
    assert_eq!(signals.signal_age(9), 0);
    assert_eq!(signals.signal_age(20), 11);
}

#[test]
fn a_leader_writes_inside_the_suspicion_window_only() {
    let mut signals = Signals::new(0);
    let segment = SegmentId(2);
    assert!(!signals.writes(segment, 0, WINDOWS));
    signals.publish(
        20,
        Report::Writes {
            process: A,
            segment,
        },
    );
    assert_eq!(signals.liveness(A, 20, WINDOWS), Liveness::Up);
    assert!(signals.writes(segment, 29, WINDOWS));
    assert!(!signals.writes(segment, 30, WINDOWS));
    assert!(!signals.writes(SegmentId(1), 20, WINDOWS));
    // A beat of the same process does not say its leader still writes.
    signals.publish(35, Report::Beat { process: A });
    assert!(!signals.writes(segment, 35, WINDOWS));
}

#[test]
fn a_report_names_its_process() {
    assert_eq!(Report::Beat { process: A }.process(), A);
    assert_eq!(
        Report::Writes {
            process: B,
            segment: SegmentId(1)
        }
        .process(),
        B
    );
}

#[test]
#[should_panic(expected = "the controller's clock never goes back")]
fn a_report_before_the_newest_panics() {
    let mut signals = Signals::new(0);
    signals.publish(10, Report::Beat { process: A });
    signals.publish(9, Report::Beat { process: B });
}

#[test]
#[should_panic(expected = "a report after the start")]
fn a_report_before_the_start_panics() {
    let mut signals = Signals::new(10);
    signals.publish(9, Report::Beat { process: A });
}

#[test]
#[should_panic(expected = "a process is suspect before it is down")]
fn windows_that_end_before_they_start_panic() {
    Windows {
        suspect_after: 5,
        down_after: 4,
    }
    .assert_valid();
}

#[test]
#[should_panic(expected = "a process just heard is up")]
fn an_empty_suspicion_window_panics() {
    Windows {
        suspect_after: 0,
        down_after: 4,
    }
    .assert_valid();
}

#[test]
fn equal_windows_skip_the_suspect_state() {
    let windows = Windows {
        suspect_after: 10,
        down_after: 10,
    };
    windows.assert_valid();
    let signals = Signals::new(0);
    assert_eq!(signals.liveness(A, 9, windows), Liveness::Up);
    assert_eq!(signals.liveness(A, 10, windows), Liveness::Down);
}
