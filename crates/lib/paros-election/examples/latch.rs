//! **A leader latch over a journal: three members elect a leader, the
//! leader stops, and another takes over at a higher term.**
//!
//! Run it: `cargo run -p paros-election --example latch`
//!
//! The store owns one journal in multi-writer mode. Every member appends
//! to it and runs the same fold over it (`paros_election::Tree`): the
//! sessions, the nodes, and the turn of each latch. The journal arbitrates
//! nothing. Each member here is a `Member`, which owns one journal client;
//! the loop of `common` serves their requests from a journal in memory.
//!
//! # What the run shows
//!
//! 1. A makes the journal multi-writer. A, B and C open their sessions
//!    and join the latch at `/election`, each with its address. A's entry
//!    lands first, so A holds the latch, at the term of that position.
//! 2. Every member folds the same tree. A writes its address to `/leader`
//!    with a transaction guarded by the latch at its term.
//! 3. A stops. Its last guarded write is still in flight. B and C see no
//!    entry of A's session for the timeout and expire it; the fold deletes
//!    A's node, and B takes the latch at a higher term.
//! 4. A worker keeps the highest term it saw, on its disk. It takes B's
//!    order and then refuses A's late order: the term fence outside the
//!    journal.
//! 5. A's late guarded write lands: the fold voids it, because A no longer
//!    holds the latch. `/leader` still names B.
//! 6. B writes a checkpoint and trims the journal to it. A new member D
//!    starts from the checkpoint and folds the same tree.
//!
//! Each step asserts its result, so CI runs this example as a test.

mod common;

use paros_election::recipes::Latch;
use paros_election::{Config, Event, Member, Op, Path, SessionId, Settled, Term, Txn};
use paros_journal::LeaderUuid;

use common::{Journal, pump, tick};

const CONFIG: Config = Config {
    page: 16,
    heartbeat: 3,
    timeout: 10,
};

/// A worker that takes orders from the leader, with the term fence: it
/// keeps the highest term it saw, and refuses an order below it, in the
/// same write as the effect.
struct Worker {
    seen: Option<Term>,
    done: Vec<&'static str>,
}

impl Worker {
    fn order(&mut self, term: Term, what: &'static str) -> bool {
        if self.seen.is_some_and(|seen| term < seen) {
            return false;
        }
        self.seen = Some(term);
        self.done.push(what);
        true
    }
}

fn leader_path() -> Path {
    Path::new("/leader").expect("a path")
}

/// The transaction that writes `address` to `/leader`, guarded by `latch`
/// at `term`.
fn write_leader(latch: &Latch, term: Term, address: &str) -> Txn {
    latch.guarded(
        term,
        vec![
            Op::Ensure {
                path: leader_path(),
            },
            Op::SetData {
                path: leader_path(),
                data: address.as_bytes().to_vec(),
                version: None,
            },
        ],
    )
}

fn settled(events: &[Event]) -> Vec<Settled> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Settled { settled, .. } => Some(*settled),
            _ => None,
        })
        .collect()
}

/// The run: the journal, the latch and the members.
struct Run {
    latch: Latch,
    parent: Path,
    journal: Journal,
    a: Member,
    b: Member,
    c: Member,
}

fn main() {
    let latch = Latch::new(Path::new("/election").expect("a path"));
    let mut run = Run {
        parent: latch.parent().clone(),
        latch,
        journal: Journal::new(),
        a: Member::new(CONFIG),
        b: Member::new(CONFIG),
        c: Member::new(CONFIG),
    };
    let first = elect(&mut run);
    let mut worker = Worker {
        seen: None,
        done: Vec::new(),
    };
    assert!(worker.order(first, "A: rebalance"));
    let (second, now) = take_over(&mut run, first, &mut worker);
    start_from_checkpoint(&mut run, second, now);
    println!("Done: one holder for each term, and every member agrees.");
}

/// Steps 1 and 2: the election, and the leader's first guarded write.
fn elect(run: &mut Run) -> Term {
    let Run {
        latch,
        parent,
        journal,
        a,
        b,
        c,
    } = run;
    println!("1. A makes the journal multi-writer; A, B and C join the latch.");
    a.set_mode(LeaderUuid(0x3A));
    pump(journal, &mut [a]);
    for (member, id, address) in [
        (&mut *a, 1, "10.0.0.1"),
        (&mut *b, 2, "10.0.0.2"),
        (&mut *c, 3, "10.0.0.3"),
    ] {
        let join = Txn::ops(latch.join(address.as_bytes().to_vec()));
        member.open(SessionId(id), join);
        pump(journal, &mut [member]);
    }
    tick(journal, 0, &mut [a, b, c]);
    let first = a.holds(parent).expect("A's entry landed first");
    println!("   A holds the latch at term {}", first.0.0);
    assert!(b.holds(parent).is_none() && c.holds(parent).is_none());

    println!("2. Every member folds the same tree; A writes /leader under its term.");
    assert_eq!(a.tree(), b.tree());
    assert_eq!(b.tree(), c.tree());
    a.submit(write_leader(latch, first, "10.0.0.1"));
    tick(journal, 1, &mut [a, b, c]);
    assert!(settled(&a.take_events()).contains(&Settled::Applied));
    let tree = b.tree().expect("B folded the journal");
    assert_eq!(
        tree.node(&leader_path()).expect("/leader").data,
        b"10.0.0.1"
    );
    first
}

/// Steps 3 to 5: A stops, B takes over, the fences hold. Gives B's term
/// and the time.
fn take_over(run: &mut Run, first: Term, worker: &mut Worker) -> (Term, u64) {
    let Run {
        latch,
        parent,
        journal,
        a,
        b,
        c,
    } = run;
    println!("3. A stops, with a guarded write in flight; B and C expire it.");
    a.submit(write_leader(latch, first, "10.0.0.1 (late)"));
    let late = a.take_requests();
    let mut now = 2;
    while b.holds(parent).is_none() {
        assert!(now < 30, "B takes over after the timeout");
        tick(journal, now, &mut [b, c]);
        now += 1;
    }
    let second = b.holds(parent).expect("B holds the latch");
    println!("   at time {now}, B holds the latch at term {}", second.0.0);
    assert!(second > first, "the term grows");
    let tree = c.tree().expect("C folded the journal");
    assert!(tree.expired(SessionId(1)), "A's session left a tombstone");
    assert_eq!(latch.candidates(tree).len(), 2, "A's node went with it");
    b.submit(write_leader(latch, second, "10.0.0.2"));
    tick(journal, now, &mut [b, c]);

    println!("4. The worker takes B's order, then refuses A's late order.");
    assert!(worker.order(second, "B: rebalance"));
    assert!(!worker.order(first, "A: late rebalance"));
    assert_eq!(worker.done, ["A: rebalance", "B: rebalance"]);

    println!("5. A's late guarded write lands: the fold voids it.");
    for request in late {
        let (id, response) = journal.serve(request);
        a.answer(id, response);
    }
    let _ = a.take_events();
    tick(journal, now, &mut [a, b, c]);
    tick(journal, now, &mut [a, b, c]);
    let events = a.take_events();
    assert!(events.contains(&Event::SessionExpired(SessionId(1))));
    assert_eq!(settled(&events), [Settled::Expired]);
    let tree = b.tree().expect("B folded the journal");
    assert_eq!(
        tree.node(&leader_path()).expect("/leader").data,
        b"10.0.0.2"
    );
    assert_eq!(
        a.tree(),
        b.tree(),
        "A folds the same tree, and learns it lost"
    );
    (second, now)
}

/// Step 6: a checkpoint, a trim, and a new member that starts from it.
fn start_from_checkpoint(run: &mut Run, second: Term, now: u64) {
    let Run {
        latch,
        journal,
        b,
        c,
        ..
    } = run;
    println!("6. B writes a checkpoint and trims; D starts from it.");
    assert!(b.checkpoint());
    tick(journal, now, &mut [b, c]);
    let taken = b
        .take_events()
        .into_iter()
        .find_map(|event| match event {
            Event::Checkpoint { taken } => Some(taken),
            _ => None,
        })
        .expect("the checkpoint landed")
        .expect("nothing landed between B's read and the checkpoint");
    b.trim(taken).expect("a checkpoint the fold took");
    pump(journal, &mut [b]);
    assert_eq!(journal.first(), taken);
    println!(
        "   the journal keeps {} of its {} records",
        journal.len() - taken.0,
        journal.len()
    );
    let mut d = Member::new(CONFIG);
    tick(journal, now + 1, &mut [b, c, &mut d]);
    tick(journal, now + 2, &mut [b, c, &mut d]);
    assert_eq!(
        d.tree(),
        b.tree(),
        "D folds the same tree from the checkpoint"
    );
    let holder = latch.holder(d.tree().expect("D folded"));
    assert_eq!(holder.map(|t| t.term), Some(second));
}
