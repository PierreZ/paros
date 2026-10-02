//! The writer session (#204): a client's belief about its own ownership of
//! one journal — the generation it owns and the position it writes next —
//! and the calls that keep that belief honest.
//!
//! Every verdict corrects the belief: a refusal names the journal's writer
//! and next position, so a wrong belief costs a refused write, never a
//! wrong one. A writer another owner superseded **stops**: its
//! [`Writer::write`] sends nothing until its caller claims again — the
//! journal would refuse the write anyway, and a writer that kept trying
//! would only be asking the journal to fence it again.

use moonpool_core::Providers;
use paros_core::{ClientId, Entry, Generation, JournalId, JournalState, Seq, Value};

use super::outcome::{ClaimOutcome, TruncateOutcome, WriteOutcome};
use super::{Client, Resolution, WriteOptions};
use crate::rpc::{Truncate, Write};

/// What a writer learned from a journal state a verdict named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Learned {
    /// The state names this writer the owner: adopted whole.
    Owner,
    /// It names another owner, and this writer believed it owned the
    /// journal: it has been superseded and stops.
    Superseded,
    /// It names another owner; this writer did not believe it owned it.
    NotOwner,
}

/// What [`Writer::write`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriterOutcome {
    /// The journal holds the batch at `[seq, seq + count)`.
    Written {
        /// The first record's position.
        seq: u64,
        /// The records the batch holds.
        count: u64,
        /// Answered from the log as a write it already held.
        duplicate: bool,
        /// The first answer was ambiguous, and [`Client::resolve`] settled
        /// it as written.
        resolved: bool,
    },
    /// Nothing was sent: this writer owns no generation (it never claimed,
    /// or it was superseded). Claim first.
    NotOwner,
    /// Another owner's generation fenced this write: the writer stops.
    Superseded {
        /// The journal state naming the new owner.
        state: JournalState,
    },
    /// Refused for another reason (a position that is not the next one);
    /// the writer's position is corrected from `state`.
    Refused {
        /// The journal state the write was judged against.
        state: JournalState,
    },
    /// The position is below the journal's floor.
    Truncated {
        /// The journal state the write was judged against.
        state: JournalState,
    },
    /// The first answer was ambiguous and the journal proved the write is
    /// not in it, and never will be.
    NotWritten {
        /// The journal state that proves it.
        state: JournalState,
    },
    /// No server gave a verdict (`leader` is the last hint).
    Unavailable {
        /// The leader the last answer named.
        leader: Option<u64>,
    },
    /// The server asked does not serve the journal.
    UnknownJournal,
    /// Still unknown after the resolution budget: the write may land.
    Ambiguous,
}

/// One client's ownership of one journal. `Copy`: a caller that pipelines
/// takes a copy, advances it per write, and folds the verdicts back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Writer {
    journal: JournalId,
    owner: ClientId,
    /// The generation it believes it owns (`None`: it does not).
    owned: Option<u64>,
    /// The last generation it owned.
    last: u64,
    /// The position it writes next.
    next_seq: u64,
}

/// The `Write` request carrying `entry` to `journal`.
#[must_use]
pub fn write_request(journal: JournalId, entry: &Entry) -> Write {
    Write {
        journal: journal.0,
        generation: entry.generation.0,
        owner: entry.owner.0,
        seq: entry.seq.0,
        records: entry.records.iter().map(|r| r.0.clone()).collect(),
    }
}

impl Writer {
    /// A writer of `journal` as client `owner`, owning nothing yet.
    #[must_use]
    pub fn new(journal: JournalId, owner: u64) -> Self {
        Self {
            journal,
            owner: ClientId(owner),
            owned: None,
            last: 0,
            next_seq: 0,
        }
    }

    /// The journal it writes.
    #[must_use]
    pub fn journal(&self) -> JournalId {
        self.journal
    }

    /// The client it writes as.
    #[must_use]
    pub fn owner(&self) -> u64 {
        self.owner.0
    }

    /// The generation it believes it owns.
    #[must_use]
    pub fn owned(&self) -> Option<u64> {
        self.owned
    }

    /// The generation its next write names: the one it owns, or the last
    /// one it owned (what [`Writer::stale_entry`] writes under).
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.owned.unwrap_or(self.last)
    }

    /// The position it writes next.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Move the next position up to `next` (never back).
    pub fn advance_to(&mut self, next: u64) {
        self.next_seq = self.next_seq.max(next);
    }

    /// Learn from a state a verdict named. A state naming this client the
    /// owner (a claim whose answer was lost, a position it had wrong) is
    /// adopted whole; any other owner supersedes it.
    pub fn learn(&mut self, state: &JournalState) -> Learned {
        if state.owner == Some(self.owner) {
            self.owned = Some(state.generation.0);
            self.last = state.generation.0;
            self.next_seq = state.next_seq.0;
            Learned::Owner
        } else if self.owned.take().is_some() {
            Learned::Superseded
        } else {
            Learned::NotOwner
        }
    }

    /// A claim won: own `state`'s generation and continue at its position.
    pub fn won(&mut self, state: &JournalState) {
        self.owned = Some(state.generation.0);
        self.last = state.generation.0;
        self.next_seq = state.next_seq.0;
    }

    /// Fold a claim's outcome into the belief; what it learned, when the
    /// claim named a state.
    pub fn claimed(&mut self, outcome: &ClaimOutcome) -> Option<Learned> {
        match outcome {
            ClaimOutcome::Won { state } => {
                self.won(state);
                Some(Learned::Owner)
            }
            ClaimOutcome::Lost { state } | ClaimOutcome::Owned { state } => Some(self.learn(state)),
            _ => None,
        }
    }

    /// The write of `records` at its next position under the generation it
    /// owns; `None` when it owns none.
    #[must_use]
    pub fn entry(&self, records: Vec<Value>) -> Option<Entry> {
        self.owned.map(|generation| Entry {
            generation: Generation(generation),
            owner: self.owner,
            seq: Seq(self.next_seq),
            records,
        })
    }

    /// **Deliberate misbehaviour, for a harness:** the write of `records`
    /// at its next position under the generation it last owned, whether or
    /// not it still owns it. A superseded writer's write, which the journal
    /// must refuse; never what [`Writer::write`] sends.
    #[must_use]
    pub fn stale_entry(&self, records: Vec<Value>) -> Entry {
        Entry {
            generation: Generation(self.generation()),
            owner: self.owner,
            seq: Seq(self.next_seq),
            records,
        }
    }

    /// The fenced `Truncate` (#228) of this writer's journal below `up_to`,
    /// under the generation it owns; `None` when it owns none.
    #[must_use]
    pub fn truncate_request(&self, up_to: u64) -> Option<Truncate> {
        self.owned.map(|generation| Truncate {
            journal: self.journal.0,
            up_to,
            generation,
            owner: self.owner.0,
        })
    }

    /// **Deliberate misbehaviour, for a harness:** the `Truncate` below
    /// `up_to` under the generation it last owned, whether or not it still
    /// owns it. A superseded owner's truncation, which the journal must
    /// refuse; never what [`Writer::truncate`] sends.
    #[must_use]
    pub fn stale_truncate_request(&self, up_to: u64) -> Truncate {
        Truncate {
            journal: self.journal.0,
            up_to,
            generation: self.generation(),
            owner: self.owner.0,
        }
    }

    /// Fold a truncation's verdict into the belief: a refusal names the
    /// current writer. What it learned, when the verdict named a state.
    pub fn absorb_truncate(&mut self, outcome: &TruncateOutcome) -> Option<Learned> {
        match outcome {
            TruncateOutcome::Refused { state } => Some(self.learn(state)),
            _ => None,
        }
    }

    /// Truncate the journal below `up_to` as its owner (#228): the request
    /// carries this writer's own fence, to the believed leader (or
    /// `first`), following redirects. `None` when it owns no generation:
    /// nothing is sent. A refusal is folded back (a superseded writer
    /// stops).
    pub async fn truncate<P: Providers>(
        &mut self,
        client: &Client<P>,
        up_to: u64,
        first: usize,
    ) -> Option<TruncateOutcome> {
        let request = self.truncate_request(up_to)?;
        let start = client.leader().unwrap_or(first);
        let outcome = client.truncate(&request, start).await;
        self.absorb_truncate(&outcome);
        Some(outcome)
    }

    /// The `Write` request carrying `entry` to this writer's journal.
    #[must_use]
    pub fn request(&self, entry: &Entry) -> Write {
        write_request(self.journal, entry)
    }

    /// Fold a write's verdict into the belief: a written batch moves the
    /// position past it, a refusal names the state to learn. What it
    /// learned, when the verdict named a state.
    pub fn absorb(&mut self, outcome: &WriteOutcome) -> Option<Learned> {
        match outcome {
            WriteOutcome::Written { seq, count, .. } => {
                self.advance_to(seq + count);
                None
            }
            WriteOutcome::Refused { state } | WriteOutcome::Truncated { state } => {
                Some(self.learn(state))
            }
            _ => None,
        }
    }

    /// Claim the journal (see [`Client::claim`]) and fold the outcome.
    pub async fn claim<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fresh: bool,
    ) -> ClaimOutcome {
        let outcome = client.claim(self.journal, self.owner.0, first, fresh).await;
        self.claimed(&outcome);
        outcome
    }

    /// Write `records` at the tail as the owner: see [`Writer::write_entry`].
    /// Sends nothing when it owns no generation.
    pub async fn write<P: Providers>(
        &mut self,
        client: &Client<P>,
        records: Vec<Value>,
        first: usize,
    ) -> WriterOutcome {
        let Some(entry) = self.entry(records) else {
            return WriterOutcome::NotOwner;
        };
        self.write_entry(client, &entry, first).await
    }

    /// Write `entry` — built by [`Writer::entry`], or a retry of one whose
    /// generation and position still stand — to the believed leader (or
    /// `first`), following redirects; an ambiguous answer is settled by
    /// [`Client::resolve`] before this returns. Sends nothing when the
    /// writer does not own `entry`'s generation: a superseded writer stops.
    pub async fn write_entry<P: Providers>(
        &mut self,
        client: &Client<P>,
        entry: &Entry,
        first: usize,
    ) -> WriterOutcome {
        if self.owned != Some(entry.generation.0) || entry.owner != self.owner {
            return WriterOutcome::NotOwner;
        }
        let request = self.request(entry);
        let start = client.leader().unwrap_or(first);
        let report = client.write(&request, start, WriteOptions::default()).await;
        let learned = self.absorb(&report.outcome);
        match report.outcome {
            WriteOutcome::Written {
                seq,
                count,
                duplicate,
            } => WriterOutcome::Written {
                seq,
                count,
                duplicate,
                resolved: false,
            },
            WriteOutcome::Refused { state } if learned == Some(Learned::Superseded) => {
                WriterOutcome::Superseded { state }
            }
            WriteOutcome::Refused { state } => WriterOutcome::Refused { state },
            WriteOutcome::Truncated { state } => WriterOutcome::Truncated { state },
            WriteOutcome::Redirect { leader } => WriterOutcome::Unavailable { leader },
            WriteOutcome::UnknownJournal => WriterOutcome::UnknownJournal,
            WriteOutcome::Malformed => WriterOutcome::Unavailable { leader: None },
            WriteOutcome::Ambiguous => {
                let resolved = client
                    .resolve(&request, report.server, super::Retarget::FollowHint)
                    .await;
                match resolved.resolution {
                    Resolution::Written { seq, count } => {
                        self.advance_to(seq + count);
                        WriterOutcome::Written {
                            seq,
                            count,
                            duplicate: true,
                            resolved: true,
                        }
                    }
                    Resolution::NotWritten { state } => {
                        if self.learn(&state) == Learned::Superseded {
                            WriterOutcome::Superseded { state }
                        } else {
                            WriterOutcome::NotWritten { state }
                        }
                    }
                    Resolution::Truncated { state } => {
                        self.learn(&state);
                        WriterOutcome::Truncated { state }
                    }
                    Resolution::Unresolved => WriterOutcome::Ambiguous,
                }
            }
        }
    }
}
