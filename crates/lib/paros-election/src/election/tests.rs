use paros_journal::client::ReadAnswer;
use paros_journal::{BoundaryState, Command, Fence, JournalState, Reply, Write};

use super::*;

const MS: u64 = 1_000_000;

/// A journal in memory: one segment, every request answered at once.
struct Journal {
    fold: JournalState,
    log: Vec<Record>,
    positions: BTreeMap<Seq, Write>,
}

impl Journal {
    fn new(leader: Leader) -> Self {
        Self {
            fold: JournalState::from_boundary(BoundaryState::fresh(leader)),
            log: Vec::new(),
            positions: BTreeMap::new(),
        }
    }

    fn serve(&mut self, request: &Request) -> Response {
        match request {
            Request::Command { command, .. } => {
                let answer = self.fold.apply(command, |seq| self.positions.get(&seq));
                if let (paros_journal::Answer::Written { seq, .. }, Command::Write(write)) =
                    (&answer, command)
                {
                    if matches!(write.fence, Fence::Leader { .. }) {
                        self.positions.insert(*seq, write.clone());
                    }
                    self.log.extend(write.records.iter().cloned());
                }
                Response::Reply(Reply::Answer(answer))
            }
            Request::Floor { .. } => Response::Read(ReadAnswer::Page {
                records: Vec::new(),
                view: self.fold.view(),
                sealed: false,
            }),
            Request::Read { from, limit, .. } => {
                let end = (from.0 + limit).min(self.log.len() as u64);
                let records = (from.0..end)
                    .map(|seq| (Seq(seq), self.log[usize::try_from(seq).unwrap()].clone()))
                    .collect();
                Response::Read(ReadAnswer::Page {
                    records,
                    view: self.fold.view(),
                    sealed: false,
                })
            }
            Request::Resolve { .. } => unreachable!("one segment: nothing is below"),
        }
    }
}

/// Draws from a counter: uuids 1, 2, 3, ... and no jitter.
struct Counter(u128);

impl Draws for Counter {
    fn uuid(&mut self) -> LeaderUuid {
        self.0 += 1;
        LeaderUuid(self.0)
    }
    fn jitter(&mut self, _max: u64) -> u64 {
        0
    }
}

fn config() -> Config {
    Config {
        lease: 100 * MS,
        poll: 10 * MS,
        renew: 20 * MS,
        jitter: 5 * MS,
        page: 2,
    }
}

/// Drive `actor` until it has nothing to send.
fn pump(actor: &mut Election, journal: &mut Journal) {
    loop {
        let requests = actor.take_requests();
        if requests.is_empty() {
            return;
        }
        for request in requests {
            let response = journal.serve(&request);
            actor.answer(request.id(), response);
        }
    }
}

fn step(actor: &mut Election, journal: &mut Journal, now: u64, draws: &mut Counter) {
    actor.tick(now, draws);
    pump(actor, journal);
}

fn leading_term(events: &[Event]) -> Option<Term> {
    events.iter().find_map(|e| match e {
        Event::Leading { term, .. } => Some(*term),
        _ => None,
    })
}

#[test]
fn a_lone_candidate_takes_term_one_and_renews() {
    let mut journal = Journal::new(Leader::Unset);
    let mut draws = Counter(0);
    let mut actor = Election::new(config(), b"a".to_vec(), 0);
    actor.campaign();
    step(&mut actor, &mut journal, 0, &mut draws);
    step(&mut actor, &mut journal, MS, &mut draws);
    let events = actor.take_events();
    assert_eq!(leading_term(&events), Some(Term(1)));
    assert_eq!(actor.leading(), Some((Term(1), LeaderUuid(1))));
    let before = journal.log.len();
    step(&mut actor, &mut journal, 30 * MS, &mut draws);
    assert_eq!(journal.log.len(), before + 1, "an idle holder renews");
    let renewal = ElectionRecord::decode(journal.log.last().unwrap()).unwrap();
    assert_eq!(renewal.kind, Kind::Renewal);
    assert_eq!(renewal.term, Term(1));
}

#[test]
fn a_candidate_waits_a_lease_of_silence_then_takes_the_next_number() {
    let mut journal = Journal::new(Leader::Unset);
    let mut draws = Counter(0);
    let mut a = Election::new(config(), b"a".to_vec(), 0);
    let mut b = Election::new(config(), b"b".to_vec(), 0);
    a.campaign();
    step(&mut a, &mut journal, 0, &mut draws);
    step(&mut a, &mut journal, MS, &mut draws);
    assert!(a.leading().is_some());
    b.campaign();
    // While a renews, b never claims.
    let mut now = MS;
    for _ in 0..30 {
        now += 10 * MS;
        step(&mut a, &mut journal, now, &mut draws);
        step(&mut b, &mut journal, now, &mut draws);
    }
    assert!(b.leading().is_none(), "a live holder keeps its term");
    let seen = b.take_events();
    assert!(
        seen.iter().any(
            |e| matches!(e, Event::Holder { term: Term(1), interface, .. } if interface == b"a")
        ),
        "b read the holder: {seen:?}"
    );
    // a stops; after a lease b wins term 2.
    for _ in 0..30 {
        now += 10 * MS;
        step(&mut b, &mut journal, now, &mut draws);
    }
    assert_eq!(leading_term(&b.take_events()), Some(Term(2)));
    // a's next write is refused: it stops at once.
    let _ = a.write(vec![Record::new(b"late".to_vec())]).unwrap();
    pump(&mut a, &mut journal);
    let events = a.take_events();
    assert!(
        events.contains(&Event::Deposed { term: Term(1) }),
        "{events:?}"
    );
    assert!(a.leading().is_none());
}

#[test]
fn the_election_never_claims_a_multi_writer_journal() {
    let mut journal = Journal::new(Leader::multi(LeaderUuid(99)));
    let mut draws = Counter(0);
    let mut actor = Election::new(config(), b"a".to_vec(), 0);
    actor.campaign();
    for now in 0..50 {
        step(&mut actor, &mut journal, now * 10 * MS, &mut draws);
    }
    let events = actor.take_events();
    assert_eq!(events, vec![Event::MultiWriter], "reported once");
    assert_eq!(draws.0, 0, "no claim");
}

#[test]
fn a_hand_on_gives_the_next_number_to_the_next_actor() {
    let mut journal = Journal::new(Leader::Unset);
    let mut draws = Counter(0);
    let mut a = Election::new(config(), b"a".to_vec(), 0);
    let mut b = Election::new(config(), b"b".to_vec(), 0);
    a.campaign();
    step(&mut a, &mut journal, 0, &mut draws);
    step(&mut a, &mut journal, MS, &mut draws);
    let _ = a.take_events();
    a.hand_on(&mut draws).unwrap();
    pump(&mut a, &mut journal);
    let handoff = a
        .take_events()
        .into_iter()
        .find_map(|e| match e {
            Event::HandedOn(h) => Some(h),
            _ => None,
        })
        .expect("the hand on won");
    assert_eq!(handoff.term, Term(2));
    assert!(a.leading().is_none());
    assert!(b.adopt(handoff));
    pump(&mut b, &mut journal);
    assert_eq!(leading_term(&b.take_events()), Some(Term(2)));
    assert_eq!(b.leading(), Some((Term(2), handoff.uuid)));
}

#[test]
fn the_program_may_not_write_an_election_record_or_two_at_once() {
    let mut journal = Journal::new(Leader::Unset);
    let mut draws = Counter(0);
    let mut actor = Election::new(config(), b"a".to_vec(), 0);
    assert_eq!(
        actor.write(vec![Record::new(vec![1])]),
        Err(Refusal::NotLeading)
    );
    actor.campaign();
    step(&mut actor, &mut journal, 0, &mut draws);
    step(&mut actor, &mut journal, MS, &mut draws);
    let forged = ElectionRecord {
        kind: Kind::Term,
        term: Term(9),
        uuid: LeaderUuid(9),
        interface: Vec::new(),
    }
    .encode();
    assert_eq!(actor.write(vec![forged]), Err(Refusal::Reserved));
    let id = actor.write(vec![Record::new(vec![1])]).unwrap();
    assert_eq!(actor.write(vec![Record::new(vec![2])]), Err(Refusal::Busy));
    pump(&mut actor, &mut journal);
    assert!(
        actor
            .take_events()
            .iter()
            .any(|e| matches!(e, Event::Wrote { id: w, .. } if *w == id))
    );
}
