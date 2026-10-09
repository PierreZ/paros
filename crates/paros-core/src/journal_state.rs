//! The **journal state machine** (#204, #241): one per journal, judged at
//! apply.
//!
//! A journal exposes four calls — `Write`, `Read`, `Truncate`, `SetLeader`
//! (`docs/architecture.md`, section 2) — and every rule behind them is
//! judged here, when the replica's contiguous walk reaches the slot that
//! decided the call, in slot order, on every node alike. The state is four
//! scalars, [`JournalState`]: the current leader uuid, the hidden term
//! counter, the next position `next_seq` and the first retained position
//! `first_seq`. A client sees the [`JournalView`]: the same without the term.
//!
//! - **`Write(leader_uuid, seq, batch)`** ([`Command::Write`]) is accepted
//!   iff `leader_uuid` is the current leader and `seq == next_seq`; a batch of
//!   `n` records then occupies `[seq, seq + n)`. A retry with `seq <
//!   next_seq` that is *exactly* the write accepted at `seq` — same leader
//!   uuid, same records — is an idempotent ack ([`Outcome::Duplicate`]);
//!   anything else below `next_seq` is refused, and below `first_seq` it is
//!   [`Outcome::Truncated`]. The log is the deduplication table: there is no
//!   per-client session ledger and nothing to expire.
//! - **`SetLeader(new_uuid, old_uuid)`** ([`Control::SetLeader`]) is a pure
//!   compare-and-set: it wins iff `old_uuid` is the current leader (`None` on
//!   a journal that never had one), and the term then rises by one. It is
//!   refused when `new_uuid` is the current leader or the unset uuid; a uuid
//!   that led before wins again (decided on 2026-10-09: the journal trusts
//!   its clients to draw fresh uuids, `docs/architecture.md` §2.3). No lease,
//!   no clock.
//! - **`Truncate(leader_uuid, up_to_seq)`** ([`Control::Truncate`], #228) is
//!   fenced like a `Write`: it is accepted iff `leader_uuid` is current, and
//!   then raises `first_seq` to `up_to_seq`, clamped to `next_seq`. Monotone.
//!   A superseded or foreign caller is refused ([`Outcome::TruncateRefused`])
//!   and nothing moves: anyone holding the tenant could otherwise truncate to
//!   a position that is not the leader's checkpoint.
//!
//! A refusal is answered in place and names the view it was judged against,
//! so a leader learns where the journal is (the next position) or that it
//! was superseded (another leader uuid). Refusals, `Noop`s, control commands
//! and leadership changes consume a slot and no position: positions are
//! dense.
//!
//! The term (#241, §2.3) is the core's own: raised by every won `SetLeader`,
//! carried by the store's sealed state and a trim-point jump, and never
//! answered in a data-plane reply (only an operator's `Inspect` shows it).
//!
//! The state machine is pure: it reads the command and, for a retry, the
//! write accepted at the retried position (handed in as a lookup, so the
//! role never holds the log), and returns the [`Outcome`]. The replica
//! (`crate::replica`) owns the fold — where it stands, the positions index,
//! the outcomes a driver acks from — and the truncation it implies for the
//! log's slots.
//!
//! Hard `assert!`s throughout (AGENTS.md, *Assertion doctrine*): the state's
//! own ordering (`first_seq <= next_seq`) and its monotonicity are pinned at
//! every transition.

use crate::types::{Command, Control, Entry, LeaderUuid, Seq};

/// The per-journal control state (#204, #241): who may write, and where the
/// journal's dense positions stand. Folded from the log in slot order; the
/// same prefix folds to the same state on every node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JournalState {
    /// The leader uuid that may write in the current term, `None` until the
    /// first `SetLeader` (term zero is led by nobody).
    pub leader: Option<LeaderUuid>,
    /// The hidden term counter (§2.3): raised by every won `SetLeader`.
    /// Never answered to a client ([`JournalState::view`] drops it).
    pub term: u64,
    /// The position the next accepted record takes.
    pub next_seq: Seq,
    /// The first position a reader may start at: every record below it was
    /// truncated.
    pub first_seq: Seq,
}

/// What a client learns of a journal's state (#241): the [`JournalState`]
/// without its hidden term. Every refusal and every reply names one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JournalView {
    /// The current leader uuid, `None` while the journal never had one.
    pub leader: Option<LeaderUuid>,
    /// The position the next accepted record takes.
    pub next_seq: Seq,
    /// The first position a reader may start at.
    pub first_seq: Seq,
}

/// What applying one decided slot did to the journal (#204). A driver answers
/// the call that proposed the slot from it; the refusals carry the view they
/// were judged against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The write was accepted: its records occupy `[seq, seq + count)`.
    Accepted {
        /// The first record's position.
        seq: Seq,
        /// How many records.
        count: u64,
    },
    /// The write is a retry of the one accepted at `seq`: acked, and nothing
    /// moved.
    Duplicate {
        /// The position the original was accepted at.
        seq: Seq,
        /// How many records it holds.
        count: u64,
    },
    /// The write was refused: a stale or foreign leader uuid, a position
    /// that is not the next one, a retry whose records differ from what was
    /// accepted there, or an empty batch. The view names the current leader
    /// and the next position.
    Refused(JournalView),
    /// The write asked for a position below `first_seq`: the records there
    /// are gone, so whether it was accepted is unknowable here. The leader
    /// treats it as ambiguous and reads the tail.
    Truncated(JournalView),
    /// The `SetLeader` won: the view after it (the new leader, the next
    /// position it continues from).
    Leader(JournalView),
    /// The `SetLeader` lost: its `old` is not the current leader, or its
    /// `new` already leads. The view names the current leader.
    LeaderRefused(JournalView),
    /// The `Truncate` applied: the view after it (`first_seq` raised, or
    /// left where a higher truncation already put it).
    Trimmed(JournalView),
    /// The `Truncate` was refused (#228): its leader uuid is not the current
    /// one. Nothing moved; the view names the current leader.
    TruncateRefused(JournalView),
    /// A `Noop`: nothing moved.
    Noop,
}

impl JournalState {
    /// What a client learns of this state: everything but the term.
    #[must_use]
    pub fn view(&self) -> JournalView {
        JournalView {
            leader: self.leader,
            next_seq: self.next_seq,
            first_seq: self.first_seq,
        }
    }

    /// Apply one decided `command`. `accepted_at(seq)` answers "which write
    /// was accepted with its first record at `seq`?" for a retry below
    /// `next_seq`; it is only asked for a position at or above `first_seq`.
    ///
    /// # Panics
    ///
    /// If a transition breaks the state's own ordering or monotonicity (a
    /// programmer error, never an operating condition).
    pub fn apply<'a>(
        &mut self,
        command: &Command,
        accepted_at: impl Fn(Seq) -> Option<&'a Entry>,
    ) -> Outcome {
        self.assert_invariants();
        let before = *self;
        let outcome = match command {
            Command::Write(entry) => self.apply_write(entry, accepted_at),
            Command::Control(Control::SetLeader { new, old }) => self.apply_set_leader(*new, *old),
            Command::Control(Control::Truncate { leader, up_to }) => {
                self.apply_truncate(*leader, *up_to)
            }
            Command::Control(Control::Noop) => Outcome::Noop,
        };
        // Monotone in every scalar but the leader, and the leader moves only
        // with the term.
        assert!(self.term >= before.term, "a journal's term never decreases");
        assert!(
            self.next_seq >= before.next_seq,
            "a journal's next position never decreases"
        );
        assert!(
            self.first_seq >= before.first_seq,
            "a journal's first position never decreases"
        );
        if self.leader != before.leader {
            assert!(
                self.term > before.term,
                "a journal's leader changes only with its term"
            );
        }
        // Negative space, per outcome: a refusal, a duplicate and a `Noop`
        // move nothing; each accepted transition moves exactly its own
        // scalars.
        match &outcome {
            Outcome::Refused(_)
            | Outcome::Truncated(_)
            | Outcome::LeaderRefused(_)
            | Outcome::TruncateRefused(_)
            | Outcome::Duplicate { .. }
            | Outcome::Noop => {
                assert!(*self == before, "a refused or no-op command moves nothing");
            }
            Outcome::Accepted { seq, count } => {
                assert!(
                    *seq == before.next_seq,
                    "a write is accepted at the next position"
                );
                assert!(*count > 0, "an accepted write carries records");
                assert!(
                    self.next_seq.0 == seq.0 + count,
                    "a write advances by its records"
                );
                assert!(self.term == before.term, "a write never moves the term");
                assert!(
                    self.first_seq == before.first_seq,
                    "a write never truncates"
                );
            }
            Outcome::Leader(after) => {
                assert!(
                    *after == self.view(),
                    "a won SetLeader reports the state after it"
                );
                assert!(
                    self.term == before.term + 1,
                    "a won SetLeader raises the term by one"
                );
                assert!(
                    self.leader != before.leader,
                    "a won SetLeader changes the leader"
                );
                assert!(
                    self.next_seq == before.next_seq,
                    "a SetLeader moves no position"
                );
            }
            Outcome::Trimmed(after) => {
                assert!(
                    *after == self.view(),
                    "a truncation reports the state after it"
                );
                assert!(
                    self.next_seq == before.next_seq,
                    "a truncation never moves next_seq"
                );
                assert!(self.term == before.term, "a truncation keeps the leader");
            }
        }
        // The refusals name the view they were judged against.
        if let Outcome::Refused(judged)
        | Outcome::Truncated(judged)
        | Outcome::LeaderRefused(judged)
        | Outcome::TruncateRefused(judged) = &outcome
        {
            assert!(
                *judged == before.view(),
                "a refusal names the state it was judged against"
            );
        }
        self.assert_invariants();
        outcome
    }

    fn apply_write<'a>(
        &mut self,
        entry: &Entry,
        accepted_at: impl Fn(Seq) -> Option<&'a Entry>,
    ) -> Outcome {
        if entry.seq < self.first_seq {
            return Outcome::Truncated(self.view());
        }
        if entry.seq < self.next_seq {
            // A retry is answered from the log itself: exactly the write
            // accepted at this position — same leader uuid, same records — is
            // an ack; anything else is a write that lost the position.
            return match accepted_at(entry.seq) {
                Some(original) if original == entry => {
                    assert!(
                        entry.seq.0 + entry.count() <= self.next_seq.0,
                        "an accepted write lies below the next position"
                    );
                    Outcome::Duplicate {
                        seq: entry.seq,
                        count: entry.count(),
                    }
                }
                _ => Outcome::Refused(self.view()),
            };
        }
        let current = self.is_current(entry.leader);
        if !current || entry.seq != self.next_seq || entry.records.is_empty() {
            return Outcome::Refused(self.view());
        }
        assert!(entry.count() > 0, "an accepted write carries records");
        let seq = self.next_seq;
        self.next_seq = Seq(seq.0 + entry.count());
        assert!(
            self.next_seq > seq,
            "an accepted write advances the next position"
        );
        Outcome::Accepted {
            seq,
            count: entry.count(),
        }
    }

    fn apply_set_leader(&mut self, new: LeaderUuid, old: Option<LeaderUuid>) -> Outcome {
        // The unset uuid never leads, and the current leader cannot win its
        // own term again.
        if old != self.leader || !new.is_set() || Some(new) == self.leader {
            return Outcome::LeaderRefused(self.view());
        }
        let before = self.term;
        self.term += 1;
        self.leader = Some(new);
        assert!(
            self.term > before,
            "a won SetLeader moves past the term it was judged in"
        );
        assert!(self.is_current(new), "the new leader is the current one");
        Outcome::Leader(self.view())
    }

    fn apply_truncate(&mut self, leader: LeaderUuid, up_to: Seq) -> Outcome {
        if !self.is_current(leader) {
            return Outcome::TruncateRefused(self.view());
        }
        let before = self.first_seq;
        self.first_seq = self.first_seq.max(up_to.min(self.next_seq));
        assert!(
            self.first_seq >= before,
            "a truncation never lowers first_seq"
        );
        assert!(
            self.first_seq <= self.next_seq,
            "a truncation is clamped to next_seq"
        );
        Outcome::Trimmed(self.view())
    }

    /// Whether `leader` is the journal's current leader uuid: the fence a
    /// `Write` and a `Truncate` are both judged against.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_current(&self, leader: LeaderUuid) -> bool {
        let current = self.leader == Some(leader);
        // Paired with `assert_invariants`: a current leader exists only from
        // the first term on, and is never the unset uuid.
        if current {
            assert!(self.term > 0, "a current leader holds a term");
            assert!(leader.is_set(), "the unset uuid never leads");
        }
        current
    }

    /// The state's own ordering.
    ///
    /// # Panics
    ///
    /// If `first_seq` passed `next_seq`, or a journal with a term has no
    /// leader (or the reverse), or the unset uuid leads.
    pub fn assert_invariants(&self) {
        assert!(
            self.first_seq <= self.next_seq,
            "a journal's first position never passes its next one"
        );
        assert!(
            self.leader.is_some() == (self.term > 0),
            "a journal has a leader exactly from its first term"
        );
        assert!(
            self.leader.is_none_or(LeaderUuid::is_set),
            "the unset uuid never leads"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{JournalState, Outcome};
    use crate::types::{Command, Control, Entry, LeaderUuid, Seq, Value};

    fn write(leader: u128, seq: u64, records: &[&[u8]]) -> Command {
        Command::Write(Entry {
            leader: LeaderUuid(leader),
            seq: Seq(seq),
            records: records.iter().map(|r| Value(r.to_vec())).collect(),
        })
    }

    fn truncate(leader: u128, up_to: u64) -> Command {
        Command::Control(Control::Truncate {
            leader: LeaderUuid(leader),
            up_to: Seq(up_to),
        })
    }

    fn set_leader(new: u128, old: Option<u128>) -> Command {
        Command::Control(Control::SetLeader {
            new: LeaderUuid(new),
            old: old.map(LeaderUuid),
        })
    }

    /// Apply `commands` in order against a log that remembers each accepted
    /// write, as the replica's positions index does.
    fn fold(commands: &[Command]) -> (JournalState, Vec<Outcome>) {
        let mut state = JournalState::default();
        let mut accepted: Vec<Entry> = Vec::new();
        let mut outcomes = Vec::new();
        for command in commands {
            let outcome = state.apply(command, |seq| accepted.iter().find(|e| e.seq == seq));
            if let (Outcome::Accepted { .. }, Command::Write(entry)) = (&outcome, command) {
                accepted.push(entry.clone());
            }
            outcomes.push(outcome);
        }
        (state, outcomes)
    }

    #[test]
    fn nobody_writes_before_the_first_set_leader() {
        let (state, outcomes) = fold(&[write(1, 0, &[b"a"])]);
        assert!(matches!(outcomes[0], Outcome::Refused(_)));
        assert_eq!(state.next_seq, Seq(0));
    }

    #[test]
    fn a_leader_writes_dense_positions_and_a_batch_takes_one_per_record() {
        let (state, outcomes) = fold(&[
            set_leader(1, None),
            write(1, 0, &[b"a", b"b"]),
            write(1, 2, &[b"c"]),
        ]);
        assert_eq!(
            outcomes[1],
            Outcome::Accepted {
                seq: Seq(0),
                count: 2
            }
        );
        assert_eq!(
            outcomes[2],
            Outcome::Accepted {
                seq: Seq(2),
                count: 1
            }
        );
        assert_eq!(state.next_seq, Seq(3));
    }

    #[test]
    fn a_gap_a_superseded_leader_and_a_foreign_uuid_are_refused() {
        let (_, outcomes) = fold(&[
            set_leader(1, None),
            write(1, 1, &[b"gap"]),
            write(2, 0, &[b"foreign"]),
            set_leader(2, Some(1)),
            write(1, 0, &[b"stale"]),
        ]);
        assert!(matches!(outcomes[1], Outcome::Refused(_)));
        assert!(matches!(outcomes[2], Outcome::Refused(_)));
        assert!(matches!(outcomes[4], Outcome::Refused(v) if v.leader == Some(LeaderUuid(2))));
    }

    #[test]
    fn a_retry_is_acked_only_with_the_same_bytes() {
        let (state, outcomes) = fold(&[
            set_leader(1, None),
            write(1, 0, &[b"a"]),
            write(1, 0, &[b"a"]),
            write(1, 0, &[b"other"]),
        ]);
        assert_eq!(
            outcomes[2],
            Outcome::Duplicate {
                seq: Seq(0),
                count: 1
            }
        );
        assert!(matches!(outcomes[3], Outcome::Refused(_)));
        assert_eq!(state.next_seq, Seq(1));
    }

    #[test]
    fn a_retry_survives_a_leadership_change_but_not_a_truncation() {
        let (_, outcomes) = fold(&[
            set_leader(1, None),
            write(1, 0, &[b"a"]),
            set_leader(2, Some(1)),
            write(1, 0, &[b"a"]),
            truncate(2, 1),
            write(1, 0, &[b"a"]),
        ]);
        assert!(matches!(outcomes[3], Outcome::Duplicate { .. }));
        assert!(matches!(outcomes[5], Outcome::Truncated(v) if v.first_seq == Seq(1)));
    }

    #[test]
    fn set_leader_is_a_compare_and_set_and_raises_the_term() {
        let (state, outcomes) = fold(&[
            set_leader(1, None),
            set_leader(2, None),
            set_leader(2, Some(1)),
        ]);
        assert!(matches!(outcomes[0], Outcome::Leader(v) if v.leader == Some(LeaderUuid(1))));
        assert!(
            matches!(outcomes[1], Outcome::LeaderRefused(v) if v.leader == Some(LeaderUuid(1)))
        );
        assert!(matches!(outcomes[2], Outcome::Leader(_)));
        assert_eq!(state.leader, Some(LeaderUuid(2)));
        assert_eq!(state.term, 2);
    }

    #[test]
    fn the_unset_uuid_and_the_current_leader_never_win() {
        let (state, outcomes) = fold(&[
            set_leader(0, None),
            set_leader(1, None),
            set_leader(1, Some(1)),
        ]);
        assert!(matches!(outcomes[0], Outcome::LeaderRefused(v) if v.leader.is_none()));
        assert!(matches!(outcomes[2], Outcome::LeaderRefused(_)));
        assert_eq!(state.term, 1);
    }

    #[test]
    fn truncate_is_monotone_and_clamped_to_the_next_position() {
        let (state, _) = fold(&[
            set_leader(1, None),
            write(1, 0, &[b"a", b"b"]),
            truncate(1, 9),
            truncate(1, 1),
        ]);
        assert_eq!(state.first_seq, Seq(2));
    }

    #[test]
    fn truncate_is_fenced_like_a_write() {
        let (state, outcomes) = fold(&[
            truncate(1, 0),
            set_leader(1, None),
            write(1, 0, &[b"a", b"b"]),
            truncate(2, 1),
            set_leader(2, Some(1)),
            truncate(1, 2),
            truncate(2, 1),
        ]);
        assert!(matches!(outcomes[0], Outcome::TruncateRefused(v) if v.leader.is_none()));
        assert!(matches!(outcomes[3], Outcome::TruncateRefused(v) if v.first_seq == Seq(0)));
        assert!(
            matches!(outcomes[5], Outcome::TruncateRefused(v) if v.leader == Some(LeaderUuid(2)))
        );
        assert!(matches!(outcomes[6], Outcome::Trimmed(v) if v.first_seq == Seq(1)));
        assert_eq!(state.first_seq, Seq(1));
    }
}
