//! Three actors elect one leader over a journal in memory. The leader
//! stops; after one lease of silence another actor wins the next term, and
//! the old leader's next write is refused.
//!
//! The journal is the pure fold of `paros-journal`, one segment, with every
//! request answered at once: the program that drives the elections is the
//! loop below. Time is a number that the loop moves.

use std::collections::BTreeMap;

use paros_election::{Config, Draws, Election, Event, Term};
use paros_journal::client::{ReadAnswer, Request, Response};
use paros_journal::{
    Answer, BoundaryState, Command, Fence, JournalState, Leader, LeaderUuid, Record, Reply, Seq,
    Write,
};

const MS: u64 = 1_000_000;

/// A journal in memory.
struct Journal {
    fold: JournalState,
    log: Vec<Record>,
    positions: BTreeMap<Seq, Write>,
}

impl Journal {
    fn serve(&mut self, request: &Request) -> Response {
        match request {
            Request::Command { command, .. } => {
                let answer = self.fold.apply(command, |seq| self.positions.get(&seq));
                if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, command) {
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
                    .map(|seq| {
                        let at = usize::try_from(seq).expect("a small journal");
                        (Seq(seq), self.log[at].clone())
                    })
                    .collect();
                Response::Read(ReadAnswer::Page {
                    records,
                    view: self.fold.view(),
                    sealed: false,
                })
            }
            Request::Resolve { .. } => unreachable!("one segment: no write is below"),
        }
    }
}

/// The program's draws: uuids from a counter, and a jitter from a small
/// generator so that the actors do not claim at once.
struct Program {
    uuids: u128,
    state: u64,
}

impl Draws for Program {
    fn uuid(&mut self) -> LeaderUuid {
        self.uuids += 1;
        LeaderUuid(self.uuids)
    }

    fn jitter(&mut self, max: u64) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (self.state >> 33) % (max + 1)
    }
}

/// Tick each running actor at `now` and serve its requests.
fn round(
    actors: &mut [Election],
    running: &[bool],
    journal: &mut Journal,
    now: u64,
    program: &mut Program,
) {
    for (actor, _) in actors.iter_mut().zip(running).filter(|(_, run)| **run) {
        actor.tick(now, program);
        loop {
            let requests = actor.take_requests();
            if requests.is_empty() {
                break;
            }
            for request in requests {
                let response = journal.serve(&request);
                actor.answer(request.id(), response);
            }
        }
    }
}

fn leaders(actors: &[Election]) -> Vec<(usize, Term)> {
    actors
        .iter()
        .enumerate()
        .filter_map(|(i, a)| a.leading().map(|(term, _)| (i, term)))
        .collect()
}

fn main() {
    let config = Config {
        lease: 200 * MS,
        poll: 20 * MS,
        renew: 50 * MS,
        jitter: 40 * MS,
        page: 16,
    };
    let mut journal = Journal {
        fold: JournalState::from_boundary(BoundaryState::fresh(Leader::Unset)),
        log: Vec::new(),
        positions: BTreeMap::new(),
    };
    let mut program = Program { uuids: 0, state: 7 };
    let mut actors: Vec<Election> = ["a", "b", "c"]
        .iter()
        .map(|name| {
            let mut actor = Election::new(config, format!("{name}:7000").into_bytes(), 0);
            actor.campaign();
            actor
        })
        .collect();
    let mut running = vec![true; 3];

    // The three actors race; one compare-and-set wins term 1.
    let mut now = 0;
    for _ in 0..50 {
        now += 10 * MS;
        round(&mut actors, &running, &mut journal, now, &mut program);
    }
    let first = leaders(&actors);
    assert_eq!(first.len(), 1, "one leader: {first:?}");
    let (old, term) = first[0];
    assert_eq!(term, Term(1));
    println!("actor {old} leads term {}", term.0);

    // The leader writes under its term: each write is a renewal.
    let id = actors[old]
        .write(vec![Record::new(b"set x = 1".to_vec())])
        .expect("the leader writes");
    round(&mut actors, &running, &mut journal, now, &mut program);
    assert!(
        actors[old]
            .take_events()
            .iter()
            .any(|e| matches!(e, Event::Wrote { id: w, .. } if *w == id))
    );

    // The leader stops. After one lease of silence, another actor wins.
    running[old] = false;
    for _ in 0..60 {
        now += 10 * MS;
        round(&mut actors, &running, &mut journal, now, &mut program);
    }
    let next: Vec<(usize, Term)> = leaders(&actors)
        .into_iter()
        .filter(|(i, _)| *i != old)
        .collect();
    assert_eq!(next.len(), 1, "one new leader: {next:?}");
    let (new, term) = next[0];
    assert_eq!(term, Term(2), "the next number");
    println!("actor {new} leads term {}", term.0);

    // The others read the new holder from the journal.
    let watcher = (0..3)
        .find(|i| *i != old && *i != new)
        .expect("a third actor");
    let holder = actors[watcher].holder().expect("the watcher read a holder");
    assert_eq!(holder.term, Term(2));
    assert_eq!(
        holder.interface,
        format!("{}:7000", ["a", "b", "c"][new]).into_bytes()
    );

    // The old leader comes back: its first write is refused and it stops.
    let _ = actors[old].write(vec![Record::new(b"set x = 2".to_vec())]);
    running[old] = true;
    round(&mut actors, &running, &mut journal, now, &mut program);
    let events = actors[old].take_events();
    assert!(
        events.contains(&Event::Deposed { term: Term(1) }),
        "{events:?}"
    );
    assert!(actors[old].leading().is_none());
    println!("actor {old} stopped at its first refused write");
}
