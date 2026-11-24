//! The code that the examples share: a journal in one process, with no
//! network, and the loop that serves the members' requests.
//!
//! The journal is the fold of `paros_journal` over one segment, and its
//! records: what a replica of the segment in force serves. It answers each
//! request at once, so the examples show the store, not the roles that
//! choose the log (the examples of `paros-journal-io` show those).

use paros_election::Member;
use paros_journal::client::{Request, RequestId, Response};
use paros_journal::{
    Answer, BoundaryState, Command, JournalState, Leader, ReadAnswer, Record, Reply, Seq,
};

/// A journal in one segment.
pub struct Journal {
    state: JournalState,
    records: Vec<Record>,
}

impl Journal {
    /// A fresh journal: nobody set its leader yet.
    pub fn new() -> Self {
        Self {
            state: JournalState::from_boundary(BoundaryState::fresh(Leader::Unset)),
            records: Vec::new(),
        }
    }

    /// How many records the journal holds, the trimmed ones too.
    pub fn len(&self) -> u64 {
        self.records.len() as u64
    }

    /// The first position a reader may start at.
    pub fn first(&self) -> Seq {
        self.state.first_seq()
    }

    /// The answer to one request.
    pub fn serve(&mut self, request: Request) -> (RequestId, Response) {
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
                    .take(usize::try_from(limit).expect("a page fits in memory"))
                    .map(|s| {
                        let index = usize::try_from(s).expect("a position fits in memory");
                        (Seq(s), self.records[index].clone())
                    })
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

/// Serve every request of `members` until none is left.
pub fn pump(journal: &mut Journal, members: &mut [&mut Member]) {
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
pub fn tick(journal: &mut Journal, now: u64, members: &mut [&mut Member]) {
    for member in members.iter_mut() {
        member.tick(now, 0);
    }
    pump(journal, members);
}
