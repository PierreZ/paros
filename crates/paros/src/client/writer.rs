//! The writer session (#204, #241): a client's belief about its own
//! leadership of one journal — the leader uuid it leads under and the
//! position it writes next — and the calls that keep that belief honest.
//!
//! Every verdict corrects the belief: a refusal names the journal's leader
//! and next position, so a wrong belief costs a refused write, never a
//! wrong one. A writer another leader superseded **stops**: its
//! [`Writer::write`] sends nothing until its caller claims again — the
//! journal would refuse the write anyway, and a writer that kept trying
//! would only be asking the journal to fence it again.
//!
//! **Leader uuids** (§2.3): one per leadership term, never per process. The
//! library draws no randomness, so the caller hands each writer a random
//! 128-bit seed and the writer derives its uuids from it, one per term
//! ([`leader_uuid`]): a writer that led and was superseded claims again
//! under the next one, which fences its own older in-flight writes. A claim
//! whose answer was lost is asked again under the same uuid, and a journal
//! already naming it is adopted, never claimed twice.

use moonpool_core::Providers;
use paros_core::{Entry, JournalIdentifier, JournalView, LeaderUuid, Seq, Value};

use super::outcome::{ClaimOutcome, TruncateOutcome, WriteOutcome};
use super::{Client, Resolution, WriteOptions};
use crate::rpc::{Truncate, Write};

/// What a writer learned from a journal state a verdict named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Learned {
    /// The view names this writer's uuid the leader: adopted whole.
    Owner,
    /// It names another leader, and this writer believed it led the
    /// journal: it has been superseded and stops.
    Superseded,
    /// It names another leader; this writer did not believe it led.
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
    /// Nothing was sent: this writer leads no term (it never claimed, or
    /// it was superseded). Claim first.
    NotOwner,
    /// Another leader uuid fenced this write: the writer stops.
    Superseded {
        /// The journal view naming the new leader.
        state: JournalView,
    },
    /// Refused for another reason (a position that is not the next one);
    /// the writer's position is corrected from `state`.
    Refused {
        /// The journal state the write was judged against.
        state: JournalView,
    },
    /// The position is below the journal's floor.
    Truncated {
        /// The journal state the write was judged against.
        state: JournalView,
    },
    /// The first answer was ambiguous and the journal proved the write is
    /// not in it, and never will be.
    NotWritten {
        /// The journal state that proves it.
        state: JournalView,
    },
    /// No server gave a verdict (`leader` is the last hint).
    Unavailable {
        /// The leader the last answer named.
        leader: Option<u64>,
    },
    /// The server asked does not serve the journal.
    UnknownJournal,
    /// The batch is over the answering node's limits (#241): nothing was
    /// written, and the writer's position is unchanged. Split the batch.
    TooLarge {
        /// The most records the node accepts in one write.
        max_records: u64,
        /// The most record bytes the node accepts in one write.
        max_bytes: u64,
    },
    /// Still unknown after the resolution budget: the write may land.
    Ambiguous,
}

/// The `k`th leader uuid of a writer seeded with `seed` (#241): a fixed
/// mixing of the two (splitmix64 on each half), never the unset uuid. A
/// random seed makes every derived uuid as random as one drawn directly; two
/// writers collide only if their seeds do.
#[must_use]
pub fn leader_uuid(seed: u128, k: u64) -> LeaderUuid {
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    let half = |bits: u128| u64::try_from(bits & u128::from(u64::MAX)).unwrap_or_default();
    let hi = mix(half(seed >> 64) ^ k);
    let lo = mix(half(seed) ^ k.rotate_left(32));
    // The unset uuid never leads: a derivation that lands on it moves on.
    LeaderUuid(((u128::from(hi) << 64) | u128::from(lo)).max(1))
}

/// One client's leadership of one journal. `Copy`: a caller that pipelines
/// takes a copy, advances it per write, and folds the verdicts back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Writer {
    journal: JournalIdentifier,
    /// The caller's seed every uuid of this writer derives from.
    seed: u128,
    /// How many uuids it has spent: `mine` is the `terms`th.
    terms: u64,
    /// The uuid it claims and writes under in its current (or next) term.
    mine: LeaderUuid,
    /// Whether `mine` leads the journal, as far as it knows.
    owned: bool,
    /// The last uuid it led under, once superseded (what
    /// [`Writer::stale_entry`] writes under).
    last: Option<LeaderUuid>,
    /// The position it writes next.
    next_seq: u64,
}

/// The `Write` request carrying `entry` to `journal`.
#[must_use]
pub fn write_request(journal: JournalIdentifier, entry: &Entry) -> Write {
    Write {
        journal: journal.journal.0,
        tenant: journal.tenant.0,
        leader: Some(crate::rpc::leader_uuid_to_proto(entry.leader)),
        seq: entry.seq.0,
        records: entry.records.iter().map(|r| r.0.clone()).collect(),
    }
}

impl Writer {
    /// A writer of `journal` whose leader uuids derive from the caller's
    /// random `seed`, leading nothing yet.
    #[must_use]
    pub fn new(journal: JournalIdentifier, seed: u128) -> Self {
        Self {
            journal,
            seed,
            terms: 0,
            mine: leader_uuid(seed, 0),
            owned: false,
            last: None,
            next_seq: 0,
        }
    }

    /// A writer of `journal` whose first uuid is `uuid` itself — an
    /// operator naming the uuid to lead under (`parosctl --leader`); its
    /// later terms derive from it as from a seed.
    ///
    /// # Panics
    ///
    /// If `uuid` is the unset uuid, which never leads.
    #[must_use]
    pub fn with_uuid(journal: JournalIdentifier, uuid: LeaderUuid) -> Self {
        assert!(uuid.is_set(), "the unset uuid never leads");
        Self {
            mine: uuid,
            ..Self::new(journal, uuid.0)
        }
    }

    /// The journal it writes.
    #[must_use]
    pub fn journal(&self) -> JournalIdentifier {
        self.journal
    }

    /// The uuid it claims and writes under: the one it leads with, or the
    /// one its next claim asks for.
    #[must_use]
    pub fn uuid(&self) -> LeaderUuid {
        self.mine
    }

    /// The uuid it believes leads the journal, when it believes it leads.
    #[must_use]
    pub fn owned(&self) -> Option<LeaderUuid> {
        self.owned.then_some(self.mine)
    }

    /// The uuid a stale write names: the one it leads with, or the last one
    /// it led with (what [`Writer::stale_entry`] writes under).
    #[must_use]
    pub fn fence(&self) -> LeaderUuid {
        if self.owned {
            self.mine
        } else {
            self.last.unwrap_or(self.mine)
        }
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

    /// Spend the current uuid: it led (or may have), so the next claim asks
    /// under a fresh one.
    fn next_term(&mut self) {
        self.last = Some(self.mine);
        self.terms += 1;
        self.mine = leader_uuid(self.seed, self.terms);
        assert!(
            Some(self.mine) != self.last,
            "a writer's next uuid is not its last"
        );
    }

    /// Spend the uuid it leads with, on purpose: it leads nothing until its
    /// next claim, which asks under its next uuid and supersedes its own
    /// term. A writer that leads nothing keeps the unspent uuid it has.
    pub fn begin_term(&mut self) {
        if self.owned {
            self.owned = false;
            self.next_term();
        }
    }

    /// Learn from a view a verdict named. A view naming this writer's uuid
    /// the leader (a claim whose answer was lost, a position it had wrong)
    /// is adopted whole; any other leader supersedes it, and its next claim
    /// asks under a fresh uuid.
    pub fn learn(&mut self, state: &JournalView) -> Learned {
        if state.leader == Some(self.mine) {
            self.owned = true;
            self.next_seq = state.next_seq.0;
            Learned::Owner
        } else if self.owned {
            self.owned = false;
            self.next_term();
            Learned::Superseded
        } else {
            Learned::NotOwner
        }
    }

    /// A claim won: lead under its uuid and continue at its position.
    ///
    /// # Panics
    ///
    /// If `state` names another leader (a programmer error: a won claim
    /// names its own uuid).
    pub fn won(&mut self, state: &JournalView) {
        assert!(
            state.leader == Some(self.mine),
            "a won claim names the writer's uuid"
        );
        self.owned = true;
        self.next_seq = state.next_seq.0;
    }

    /// Fold a claim's outcome into the belief; what it learned, when the
    /// claim named a state.
    pub fn claimed(&mut self, outcome: &ClaimOutcome) -> Option<Learned> {
        match outcome {
            // A won claim names this writer's uuid; a reply that names
            // another (a server's answer, never trusted to panic on) is
            // learned like any other view.
            ClaimOutcome::Won { state } if state.leader == Some(self.mine) => {
                self.won(state);
                Some(Learned::Owner)
            }
            ClaimOutcome::Won { state }
            | ClaimOutcome::Lost { state }
            | ClaimOutcome::Owned { state } => Some(self.learn(state)),
            _ => None,
        }
    }

    /// The write of `records` at its next position under the uuid it leads
    /// with; `None` when it leads no term.
    #[must_use]
    pub fn entry(&self, records: Vec<Value>) -> Option<Entry> {
        self.owned().map(|leader| Entry {
            leader,
            seq: Seq(self.next_seq),
            records,
        })
    }

    /// **Deliberate misbehaviour, for a harness:** the write of `records`
    /// at its next position under the uuid it last led with, whether or not
    /// it still leads. A superseded writer's write, which the journal must
    /// refuse; never what [`Writer::write`] sends.
    #[must_use]
    pub fn stale_entry(&self, records: Vec<Value>) -> Entry {
        Entry {
            leader: self.fence(),
            seq: Seq(self.next_seq),
            records,
        }
    }

    /// The fenced `Truncate` (#228) of this writer's journal below `up_to`,
    /// under the uuid it leads with; `None` when it leads no term.
    #[must_use]
    pub fn truncate_request(&self, up_to: u64) -> Option<Truncate> {
        self.owned().map(|leader| Truncate {
            journal: self.journal.journal.0,
            tenant: self.journal.tenant.0,
            up_to,
            leader: Some(crate::rpc::leader_uuid_to_proto(leader)),
        })
    }

    /// **Deliberate misbehaviour, for a harness:** the `Truncate` below
    /// `up_to` under the uuid it last led with, whether or not it still
    /// leads. A superseded leader's truncation, which the journal must
    /// refuse; never what [`Writer::truncate`] sends.
    #[must_use]
    pub fn stale_truncate_request(&self, up_to: u64) -> Truncate {
        Truncate {
            journal: self.journal.journal.0,
            tenant: self.journal.tenant.0,
            up_to,
            leader: Some(crate::rpc::leader_uuid_to_proto(self.fence())),
        }
    }

    /// Fold a truncation's verdict into the belief: a refusal names the
    /// current leader. What it learned, when the verdict named a state.
    pub fn absorb_truncate(&mut self, outcome: &TruncateOutcome) -> Option<Learned> {
        match outcome {
            TruncateOutcome::Refused { state } => Some(self.learn(state)),
            _ => None,
        }
    }

    /// Truncate the journal below `up_to` as its leader (#228): the request
    /// carries this writer's own fence, to the believed leader (or
    /// `first`), following redirects. `None` when it leads no term:
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

    /// Claim the journal under this writer's uuid (see [`Client::claim`])
    /// and fold the outcome. `fresh` asks for a new term on purpose: a
    /// writer that leads spends its uuid first, so it supersedes itself.
    pub async fn claim<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fresh: bool,
    ) -> ClaimOutcome {
        if fresh {
            self.begin_term();
        }
        let outcome = client.claim(self.journal, self.mine, first).await;
        self.claimed(&outcome);
        outcome
    }

    /// Write `records` at the tail as the leader: see [`Writer::write_entry`].
    /// Sends nothing when it leads no term.
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
    /// uuid and position still stand — to the believed leader (or `first`),
    /// following redirects; an ambiguous answer is settled by
    /// [`Client::resolve`] before this returns. Sends nothing when the
    /// writer does not lead under `entry`'s uuid: a superseded writer stops.
    pub async fn write_entry<P: Providers>(
        &mut self,
        client: &Client<P>,
        entry: &Entry,
        first: usize,
    ) -> WriterOutcome {
        if self.owned() != Some(entry.leader) {
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
            WriteOutcome::TooLarge {
                max_records,
                max_bytes,
            } => WriterOutcome::TooLarge {
                max_records,
                max_bytes,
            },
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
