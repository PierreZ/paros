//! A fleet or cell operation run to its end (`fleet::run_to_end`): each
//! written step is kept in order, an interrupted step is taken again while
//! the patience lasts past the last written step, and a run that keeps
//! writing past its bound ends going round. A request to a coordinator is
//! asked again the same way (`coordinator`).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Duration;

use crate::fleet::{FleetRefusal, Interrupted, Run, Stage, Step, run_to_end};
use crate::rig::Rig;
use crate::rig::cell::{Cell, ids};

fn not_joined() -> Step<u64> {
    Step::Interrupted(Interrupted::NotJoined { journal: ids(1, 2) })
}

/// Run the scripted `steps` (one per take; past the script, interrupted)
/// with `patience` and `max_steps`: the run, how many steps it took and
/// the simulated time it spent.
fn run(steps: Vec<Step<u64>>, patience: Duration, max_steps: usize) -> (Run<u64>, usize, Duration) {
    let cell = Cell::default();
    Rig::new().run_with(&cell, &[1], |client| async move {
        let script = RefCell::new(VecDeque::from(steps));
        let taken = RefCell::new(0);
        let start = client.now();
        let run = run_to_end(&client, patience, max_steps, async || {
            *taken.borrow_mut() += 1;
            script.borrow_mut().pop_front().unwrap_or_else(not_joined)
        })
        .await;
        (run, taken.into_inner(), client.now().saturating_sub(start))
    })
}

#[test]
fn a_run_keeps_its_written_steps_in_order_and_its_last() {
    let (run, taken, _) = run(
        vec![
            Step::Advanced(Stage::FormFleet),
            Step::Done {
                result: 7,
                last: Some(Stage::AddCell),
            },
        ],
        Duration::ZERO,
        8,
    );
    assert_eq!(taken, 2);
    assert_eq!(run.steps, vec![Stage::FormFleet, Stage::AddCell]);
    assert_eq!(
        run.outcome,
        Step::Done {
            result: 7,
            last: Some(Stage::AddCell)
        }
    );
}

#[test]
fn a_run_that_found_its_end_names_its_last_written_step() {
    let (run, _, _) = run(
        vec![
            Step::Advanced(Stage::FormFleet),
            Step::Done {
                result: 7,
                last: None,
            },
        ],
        Duration::ZERO,
        8,
    );
    assert_eq!(
        run.outcome,
        Step::Done {
            result: 7,
            last: Some(Stage::FormFleet)
        },
        "the last step written on the way"
    );
}

#[test]
fn an_interrupted_step_is_taken_again_while_the_patience_lasts() {
    let (run, taken, spent) = run(
        vec![
            not_joined(),
            Step::Done {
                result: 1,
                last: None,
            },
        ],
        Duration::from_secs(1),
        8,
    );
    assert_eq!(taken, 2);
    assert!(matches!(run.outcome, Step::Done { result: 1, .. }));
    assert!(run.steps.is_empty());
    assert!(spent > Duration::ZERO, "a pause between the takes");
}

#[test]
fn an_interrupted_step_past_the_patience_ends_the_run() {
    let (run, taken, spent) = run(Vec::new(), Duration::from_millis(500), 8);
    assert_eq!(run.outcome, not_joined());
    assert!(taken > 2, "taken again while the patience lasts");
    assert!(spent >= Duration::from_millis(500), "{spent:?}");
}

#[test]
fn a_written_step_restarts_the_patience() {
    // Interrupted for two thirds of the patience, a written step, then
    // interrupted as long again: past the first patience, the run goes on
    // to its end.
    let patience = Duration::from_millis(300);
    let waits = || (0..20).map(|_| not_joined());
    let steps: Vec<Step<u64>> = waits()
        .chain([Step::Advanced(Stage::FormFleet)])
        .chain(waits())
        .chain([Step::Done {
            result: 3,
            last: None,
        }])
        .collect();
    let (run, taken, spent) = run(steps, patience, 8);
    assert_eq!(taken, 42);
    assert_eq!(run.steps, vec![Stage::FormFleet]);
    assert!(matches!(run.outcome, Step::Done { result: 3, .. }));
    assert!(spent > patience, "{spent:?}");
}

#[test]
fn a_refused_step_ends_the_run_at_once() {
    let refused = Step::Refused(FleetRefusal::NotInitialized);
    let (run, taken, _) = run(
        vec![Step::Advanced(Stage::FormFleet), refused.clone()],
        Duration::from_secs(1),
        8,
    );
    assert_eq!(taken, 2);
    assert_eq!(run.outcome, refused);
    assert_eq!(run.steps, vec![Stage::FormFleet]);
}

#[test]
fn a_run_that_keeps_writing_ends_going_round() {
    let steps = (0..10).map(|_| Step::Advanced(Stage::AddCell)).collect();
    let (run, taken, _) = run(steps, Duration::ZERO, 4);
    assert_eq!(taken, 4);
    assert_eq!(
        run.outcome,
        Step::Interrupted(Interrupted::GoingRound { steps: 4 })
    );
    assert_eq!(run.steps.len(), 4);
}

mod coordinator {
    //! A request to a coordinator (`journals::until_decided`): asked again
    //! while its answer is retryable and the patience lasts, `not
    //! coordinator` while no coordinator published one.

    use std::cell::RefCell;
    use std::time::Duration;

    use moonpool_rpc::{AccessClass, Endpoint, EndpointToken, Incarnation, InterfaceRef};
    use paros_core::Value;
    use paros_core::journal::{Command, Entry, LeaderUuid, Seq, WriterMode};
    use paros_wire::methods::CoordinatorInterface;

    use crate::election::{Candidate, ElectionRecord};
    use crate::journals::until_decided;
    use crate::rig::Rig;
    use crate::rig::cell::{Cell, ids};

    /// A cell whose election journal names a coordinator when `published`.
    fn cell(published: bool) -> Cell {
        let cell = Cell::led_by(1);
        cell.create(ids(1, 9), WriterMode::Multi);
        if published {
            let endpoint = Endpoint::new(
                "10.0.0.1:4500".parse().expect("an address"),
                Incarnation::from_raw(7),
                EndpointToken::from_parts(3, 1),
            );
            let reference =
                InterfaceRef::<CoordinatorInterface>::new(endpoint, AccessClass::Public);
            let campaign = ElectionRecord::Campaign {
                term: 1,
                candidate: Candidate {
                    id: 1,
                    interface: "10.0.0.1:4500".into(),
                    reference: paros_net::published(&reference),
                },
                uuid: LeaderUuid(5),
            };
            cell.apply(
                ids(1, 9),
                &Command::Write(Entry {
                    leader: LeaderUuid::UNSET,
                    seq: Seq(0),
                    records: vec![Value(campaign.encode())],
                }),
            );
        }
        cell
    }

    /// Ask with `answers` in turn (past them, retryable; an answer under 10
    /// is retryable): the answer and the asks, and the timeouts the asks
    /// were given.
    fn ask(published: bool, answers: Vec<u8>, patience: Duration) -> ((u8, u32), Vec<Duration>) {
        let cell = cell(published);
        Rig::new().run_on(&cell, &[1], |client, providers| async move {
            let answers = RefCell::new(answers.into_iter());
            let timeouts = RefCell::new(Vec::new());
            let decided = until_decided(
                &providers,
                &client,
                ids(1, 9),
                patience,
                || 0,
                |answer| *answer < 10,
                async |_, timeout| {
                    timeouts.borrow_mut().push(timeout);
                    answers.borrow_mut().next().unwrap_or(1)
                },
            )
            .await;
            (decided, timeouts.into_inner())
        })
    }

    #[test]
    fn a_decided_answer_ends_the_request_at_once() {
        let (decided, timeouts) = ask(true, vec![1, 2, 30], Duration::from_secs(10));
        assert_eq!(decided, (30, 3));
        assert_eq!(timeouts, vec![crate::rig::tunables().request_timeout; 3]);
    }

    #[test]
    fn a_retryable_answer_past_the_patience_is_the_answer() {
        let (decided, timeouts) = ask(true, Vec::new(), Duration::from_millis(500));
        assert_eq!(decided.0, 1);
        assert!(decided.1 > 1, "asked again while the patience lasted");
        assert_eq!(timeouts.len(), decided.1 as usize);
    }

    #[test]
    fn no_published_coordinator_is_not_coordinator_and_asks_nothing() {
        let (decided, timeouts) = ask(false, vec![30], Duration::from_millis(500));
        assert_eq!(decided.0, 0);
        assert!(decided.1 > 1);
        assert!(timeouts.is_empty());
    }
}
