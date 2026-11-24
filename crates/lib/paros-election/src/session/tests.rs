use paros_journal::client::{Request, RequestId, Response};
use paros_journal::{
    Answer, BoundaryState, Command, JournalState, Leader, LeaderUuid, ReadAnswer, Record, Reply,
    Seq,
};

use super::{Config, Event, Member, Settled, State};
use crate::entry::{Op, SessionId, Txn};
use crate::path::Path;
use crate::recipes::Latch;

const CONFIG: Config = Config {
    page: 4,
    heartbeat: 3,
    timeout: 10,
};

/// A journal in one segment: the fold of `paros_journal` and its records.
struct Journal {
    state: JournalState,
    records: Vec<Record>,
}

impl Journal {
    fn new() -> Self {
        Self {
            state: JournalState::from_boundary(BoundaryState::fresh(Leader::Unset)),
            records: Vec::new(),
        }
    }

    fn serve(&mut self, request: Request) -> (RequestId, Response) {
        let view = self.state.view();
        match request {
            Request::Command { id, command } => {
                let answer = self.state.apply(&command, |_| None);
                if let (Answer::Written { .. }, Command::Write(write)) = (&answer, &command) {
                    self.records.extend(write.records.iter().cloned());
                }
                (id, Response::Reply(Reply::Answer(answer)))
            }
            Request::Floor { id } => (
                id,
                Response::Read(ReadAnswer::Page {
                    records: vec![],
                    view,
                    sealed: false,
                }),
            ),
            Request::Read { id, from, limit } => {
                if from < view.first_seq {
                    return (id, Response::Read(ReadAnswer::Truncated(view)));
                }
                let records = (from.0..view.next_seq.0)
                    .take(usize::try_from(limit).unwrap())
                    .map(|s| (Seq(s), self.records[usize::try_from(s).unwrap()].clone()))
                    .collect();
                (
                    id,
                    Response::Read(ReadAnswer::Page {
                        records,
                        view,
                        sealed: false,
                    }),
                )
            }
            Request::Resolve { .. } => unreachable!("the store writes unfenced"),
        }
    }
}

/// Serve every request of `members` that is not held, until none is left.
fn pump(journal: &mut Journal, members: &mut [&mut Member]) {
    loop {
        let mut any = false;
        for member in members.iter_mut() {
            for request in member.take_requests() {
                any = true;
                let (id, response) = journal.serve(request);
                member.answer(id, response);
            }
        }
        if !any {
            return;
        }
    }
}

/// Tick every member at `now`, then serve them.
fn tick(journal: &mut Journal, now: u64, members: &mut [&mut Member]) {
    for member in members.iter_mut() {
        member.tick(now, 0);
    }
    pump(journal, members);
}

fn latch() -> Latch {
    Latch::new(Path::new("/election").unwrap())
}

fn joined(member: &mut Member, id: u64) {
    let txn = Txn::ops(latch().join(id.to_le_bytes().to_vec()));
    member.open(SessionId(id), txn);
}

fn settled(events: &[Event]) -> Vec<Settled> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Settled { settled, .. } => Some(*settled),
            _ => None,
        })
        .collect()
}

#[test]
fn three_members_elect_one_holder_and_agree() {
    let mut journal = Journal::new();
    let (mut a, mut b, mut c) = (
        Member::new(CONFIG),
        Member::new(CONFIG),
        Member::new(CONFIG),
    );
    a.set_mode(LeaderUuid(0x3A));
    pump(&mut journal, &mut [&mut a]);
    assert!(matches!(a.take_events()[..], [Event::Journal(_)]));
    joined(&mut a, 1);
    joined(&mut b, 2);
    joined(&mut c, 3);
    for now in 0..3 {
        tick(&mut journal, now, &mut [&mut a, &mut b, &mut c]);
    }
    for member in [&a, &b, &c] {
        assert!(matches!(member.state(), State::Live(_)));
        assert_eq!(member.tree(), a.tree(), "every member folds the same tree");
    }
    let parent = latch().parent().clone();
    let held: Vec<_> = [&a, &b, &c].iter().map(|m| m.holds(&parent)).collect();
    assert!(held[0].is_some() && held[1].is_none() && held[2].is_none());
    assert_eq!(settled(&a.take_events()), [Settled::Applied]);
}

#[test]
fn a_silent_holder_is_expired_and_its_late_guarded_write_fails() {
    let mut journal = Journal::new();
    let (mut a, mut b) = (Member::new(CONFIG), Member::new(CONFIG));
    a.set_mode(LeaderUuid(0x3A));
    joined(&mut a, 1);
    pump(&mut journal, &mut [&mut a]);
    joined(&mut b, 2);
    tick(&mut journal, 0, &mut [&mut a, &mut b]);
    tick(&mut journal, 1, &mut [&mut a, &mut b]);
    let parent = latch().parent().clone();
    let term = a.holds(&parent).expect("A holds the latch");
    // A sends a guarded write, and then stops: its request is held.
    a.submit(latch().guarded(
        term,
        vec![Op::Ensure {
            path: Path::new("/acted").unwrap(),
        }],
    ));
    let held = a.take_requests();
    let mut now = 2;
    while b.holds(&parent).is_none() {
        assert!(now < 40, "B takes over after the timeout");
        tick(&mut journal, now, &mut [&mut b]);
        now += 1;
    }
    let new = b.holds(&parent).unwrap();
    assert!(new > term, "the term grows");
    // A's held write lands now: the fold voids it.
    for request in held {
        let (id, response) = journal.serve(request);
        a.answer(id, response);
    }
    let _ = a.take_events();
    tick(&mut journal, now, &mut [&mut a, &mut b]);
    let events = a.take_events();
    assert!(events.contains(&Event::SessionExpired(SessionId(1))));
    assert_eq!(settled(&events), [Settled::Expired]);
    assert!(
        b.tree()
            .unwrap()
            .node(&Path::new("/acted").unwrap())
            .is_none()
    );
    assert_eq!(a.tree(), b.tree());
}

#[test]
fn a_member_below_the_floor_starts_from_the_checkpoint() {
    let mut journal = Journal::new();
    let mut a = Member::new(CONFIG);
    a.set_mode(LeaderUuid(0x3A));
    joined(&mut a, 1);
    for now in 0..8 {
        tick(&mut journal, now, &mut [&mut a]);
    }
    assert!(a.checkpoint());
    pump(&mut journal, &mut [&mut a]);
    tick(&mut journal, 8, &mut [&mut a]);
    let taken = a
        .take_events()
        .into_iter()
        .find_map(|e| match e {
            Event::Checkpoint { taken } => Some(taken),
            _ => None,
        })
        .expect("the checkpoint landed");
    let at = taken.expect("nothing landed between the read and the checkpoint");
    assert!(
        a.trim(Seq(at.0 + 1)).is_none(),
        "a trim goes to a checkpoint only"
    );
    a.trim(at).expect("a checkpoint the fold took");
    pump(&mut journal, &mut [&mut a]);
    let mut late = Member::new(CONFIG);
    for now in 9..12 {
        tick(&mut journal, now, &mut [&mut a, &mut late]);
    }
    assert_eq!(late.tree(), a.tree(), "a new member folds the same tree");
}

#[test]
fn a_floor_that_is_not_a_checkpoint_stops_the_member() {
    let mut journal = Journal::new();
    let mut a = Member::new(CONFIG);
    a.set_mode(LeaderUuid(0x3A));
    joined(&mut a, 1);
    for now in 0..5 {
        tick(&mut journal, now, &mut [&mut a]);
    }
    assert!(journal.records.len() > 1, "a heartbeat is at position 1");
    // Another client truncates the journal: the store's precondition breaks.
    let cut = Command::Truncate {
        fence: None,
        up_to: Seq(1),
    };
    assert!(matches!(
        journal.state.apply(&cut, |_| None),
        Answer::Trimmed(_)
    ));
    let mut late = Member::new(CONFIG);
    tick(&mut journal, 5, &mut [&mut late]);
    tick(&mut journal, 6, &mut [&mut late]);
    assert_eq!(late.take_events(), [Event::Unreadable(Seq(1))]);
    assert!(late.tree().is_none());
}

#[test]
fn an_entry_below_the_checkpoint_of_an_expired_session_ends_unknown() {
    let mut journal = Journal::new();
    let (mut a, mut b) = (Member::new(CONFIG), Member::new(CONFIG));
    a.set_mode(LeaderUuid(0x3A));
    joined(&mut a, 1);
    pump(&mut journal, &mut [&mut a]);
    tick(&mut journal, 0, &mut [&mut a]);
    assert!(matches!(a.state(), State::Live(_)));
    let _ = a.take_events();
    // A's write lands, and A reads nothing more.
    let acted = Path::new("/acted").unwrap();
    let txn = a.submit(Txn::ops(vec![Op::Ensure {
        path: acted.clone(),
    }]));
    pump(&mut journal, &mut [&mut a]);
    // B expires the silent session, checkpoints and trims.
    let mut now = 1;
    while b.tree().is_none_or(|t| !t.expired(SessionId(1))) {
        assert!(now < 40, "B expires A after the timeout");
        tick(&mut journal, now, &mut [&mut b]);
        now += 1;
    }
    assert!(b.checkpoint());
    pump(&mut journal, &mut [&mut b]);
    tick(&mut journal, now, &mut [&mut b]);
    let at = b
        .take_events()
        .into_iter()
        .find_map(|e| match e {
            Event::Checkpoint { taken } => taken,
            _ => None,
        })
        .expect("the fold took the checkpoint");
    b.trim(at).expect("a checkpoint the fold took");
    pump(&mut journal, &mut [&mut b]);
    // A restarts from the checkpoint: its write applied before the expiry.
    tick(&mut journal, now + 1, &mut [&mut a]);
    tick(&mut journal, now + 2, &mut [&mut a]);
    assert!(
        a.tree().unwrap().node(&acted).is_some(),
        "the write applied"
    );
    let events = a.take_events();
    assert!(events.contains(&Event::Jumped(at)));
    assert!(events.contains(&Event::Settled {
        txn,
        settled: Settled::Unknown
    }));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::SessionExpired(_)))
            .count(),
        1
    );
    assert!(matches!(a.state(), State::Expired(_)));
}
