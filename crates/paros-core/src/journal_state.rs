//! The **journal state machine** (#204): one per journal, judged at apply.
//!
//! A journal exposes four calls — `Write`, `Read`, `Truncate`, `SetLeader`
//! (`docs/architecture.md`, section 2) — and every rule behind them is
//! judged here, when the replica's contiguous walk reaches the slot that
//! decided the call, in slot order, on every node alike. The state is four
//! scalars, [`JournalState`]: the current writer `(owner, generation)`, the
//! next position `next_seq` and the first retained position `first_seq`.
//!
//! - **`Write(generation, owner, seq, batch)`** ([`Command::Write`]) is
//!   accepted iff `(generation, owner)` is current and `seq == next_seq`; a
//!   batch of `n` records then occupies `[seq, seq + n)`. A retry with
//!   `seq < next_seq` that is *exactly* the write accepted at `seq` — same
//!   writer, same records — is an idempotent ack ([`Outcome::Duplicate`]);
//!   anything else below `next_seq` is refused, and below `first_seq` it is
//!   [`Outcome::Truncated`]. The log is the deduplication table: there is no
//!   per-client session ledger and nothing to expire.
//! - **`SetLeader(expected_gen, new_owner)`** ([`Control::SetLeader`]) is a
//!   pure compare-and-swap: it wins iff `expected_gen` is current, and the
//!   generation becomes `expected_gen + 1`. No lease, no clock.
//! - **`Truncate(generation, owner, up_to_seq)`** ([`Control::Truncate`],
//!   #228) is fenced like a `Write`: it is accepted iff `(generation, owner)`
//!   is current, and then raises `first_seq` to `up_to_seq`, clamped to
//!   `next_seq`. Monotone. A superseded or foreign caller is refused
//!   ([`Outcome::TruncateRefused`]) and nothing moves: anyone holding the
//!   tenant could otherwise truncate to a position that is not the owner's
//!   checkpoint.
//!
//! A refusal is answered in place and names the state it was judged against,
//! so an owner learns where the journal is (the next position) or that it was
//! superseded (a newer generation). Refusals, `Noop`s, control commands and
//! generation changes consume a slot and no position: positions are dense.
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

use crate::types::{ClientId, Command, Control, Entry, Generation, Seq};

/// The per-journal control state (#204): who may write, and where the
/// journal's dense positions stand. Folded from the log in slot order; the
/// same prefix folds to the same state on every node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JournalState {
    /// The client that may write under [`JournalState::generation`], `None`
    /// until the first `SetLeader` (generation zero is owned by nobody).
    pub owner: Option<ClientId>,
    /// The current writer generation.
    pub generation: Generation,
    /// The position the next accepted record takes.
    pub next_seq: Seq,
    /// The first position a reader may start at: every record below it was
    /// truncated.
    pub first_seq: Seq,
}

/// What applying one decided slot did to the journal (#204). A driver answers
/// the call that proposed the slot from it; the refusals carry the state they
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
    /// The write was refused: a stale or foreign writer, a position that is
    /// not the next one, a retry whose records differ from what was accepted
    /// there, or an empty batch. The state names the current writer and the
    /// next position.
    Refused(JournalState),
    /// The write asked for a position below `first_seq`: the records there
    /// are gone, so whether it was accepted is unknowable here. The owner
    /// treats it as ambiguous and reads the tail.
    Truncated(JournalState),
    /// The `SetLeader` won: the state after it (the new generation, the new
    /// owner, the next position the owner continues from).
    Leader(JournalState),
    /// The `SetLeader` lost: its expected generation is not current. The
    /// state names the current writer.
    LeaderRefused(JournalState),
    /// The `Truncate` applied: the state after it (`first_seq` raised, or
    /// left where a higher truncation already put it).
    Trimmed(JournalState),
    /// The `Truncate` was refused (#228): its `(generation, owner)` is not
    /// the current writer. Nothing moved; the state names the current writer.
    TruncateRefused(JournalState),
    /// A `Noop`: nothing moved.
    Noop,
}

impl JournalState {
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
            Command::Control(Control::SetLeader { expected, owner }) => {
                self.apply_set_leader(*expected, *owner)
            }
            Command::Control(Control::Truncate {
                generation,
                owner,
                up_to,
            }) => self.apply_truncate(*generation, *owner, *up_to),
            Command::Control(Control::Noop) => Outcome::Noop,
        };
        // Monotone in every scalar but the owner, and the owner moves only
        // with the generation.
        assert!(
            self.generation >= before.generation,
            "a journal's generation never decreases"
        );
        assert!(
            self.next_seq >= before.next_seq,
            "a journal's next position never decreases"
        );
        assert!(
            self.first_seq >= before.first_seq,
            "a journal's first position never decreases"
        );
        assert!(
            self.owner == before.owner || self.generation > before.generation,
            "a journal's owner changes only with its generation"
        );
        self.assert_invariants();
        outcome
    }

    fn apply_write<'a>(
        &mut self,
        entry: &Entry,
        accepted_at: impl Fn(Seq) -> Option<&'a Entry>,
    ) -> Outcome {
        if entry.seq < self.first_seq {
            return Outcome::Truncated(*self);
        }
        if entry.seq < self.next_seq {
            // A retry is answered from the log itself: exactly the write
            // accepted at this position — same writer, same records — is an
            // ack; anything else is a write that lost the position.
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
                _ => Outcome::Refused(*self),
            };
        }
        let current = self.is_current(entry.generation, entry.owner);
        if !current || entry.seq != self.next_seq || entry.records.is_empty() {
            return Outcome::Refused(*self);
        }
        let seq = self.next_seq;
        self.next_seq = Seq(seq.0 + entry.count());
        Outcome::Accepted {
            seq,
            count: entry.count(),
        }
    }

    fn apply_set_leader(&mut self, expected: Generation, owner: ClientId) -> Outcome {
        if expected != self.generation {
            return Outcome::LeaderRefused(*self);
        }
        self.generation = Generation(self.generation.0 + 1);
        self.owner = Some(owner);
        Outcome::Leader(*self)
    }

    fn apply_truncate(&mut self, generation: Generation, owner: ClientId, up_to: Seq) -> Outcome {
        if !self.is_current(generation, owner) {
            return Outcome::TruncateRefused(*self);
        }
        self.first_seq = self.first_seq.max(up_to.min(self.next_seq));
        Outcome::Trimmed(*self)
    }

    /// Whether `(generation, owner)` is the journal's current writer: the
    /// fence a `Write` and a `Truncate` are both judged against.
    #[must_use]
    pub fn is_current(&self, generation: Generation, owner: ClientId) -> bool {
        self.owner == Some(owner) && self.generation == generation
    }

    /// The state's own ordering.
    ///
    /// # Panics
    ///
    /// If `first_seq` passed `next_seq`, or a journal with a generation has
    /// no owner (or the reverse).
    pub fn assert_invariants(&self) {
        assert!(
            self.first_seq <= self.next_seq,
            "a journal's first position never passes its next one"
        );
        assert!(
            self.owner.is_some() == (self.generation.0 > 0),
            "a journal has an owner exactly from its first generation"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{JournalState, Outcome};
    use crate::types::{ClientId, Command, Control, Entry, Generation, Seq, Value};

    fn write(generation: u64, owner: u64, seq: u64, records: &[&[u8]]) -> Command {
        Command::Write(Entry {
            generation: Generation(generation),
            owner: ClientId(owner),
            seq: Seq(seq),
            records: records.iter().map(|r| Value(r.to_vec())).collect(),
        })
    }

    fn truncate(generation: u64, owner: u64, up_to: u64) -> Command {
        Command::Control(Control::Truncate {
            generation: Generation(generation),
            owner: ClientId(owner),
            up_to: Seq(up_to),
        })
    }

    fn set_leader(expected: u64, owner: u64) -> Command {
        Command::Control(Control::SetLeader {
            expected: Generation(expected),
            owner: ClientId(owner),
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
        let (state, outcomes) = fold(&[write(0, 1, 0, &[b"a"])]);
        assert!(matches!(outcomes[0], Outcome::Refused(_)));
        assert_eq!(state.next_seq, Seq(0));
    }

    #[test]
    fn an_owner_writes_dense_positions_and_a_batch_takes_one_per_record() {
        let (state, outcomes) = fold(&[
            set_leader(0, 1),
            write(1, 1, 0, &[b"a", b"b"]),
            write(1, 1, 2, &[b"c"]),
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
    fn a_gap_a_stale_generation_and_a_foreign_owner_are_refused() {
        let (_, outcomes) = fold(&[
            set_leader(0, 1),
            write(1, 1, 1, &[b"gap"]),
            write(1, 2, 0, &[b"foreign"]),
            set_leader(1, 2),
            write(1, 1, 0, &[b"stale"]),
        ]);
        assert!(matches!(outcomes[1], Outcome::Refused(_)));
        assert!(matches!(outcomes[2], Outcome::Refused(_)));
        assert!(matches!(outcomes[4], Outcome::Refused(s) if s.generation == Generation(2)));
    }

    #[test]
    fn a_retry_is_acked_only_with_the_same_bytes() {
        let (state, outcomes) = fold(&[
            set_leader(0, 1),
            write(1, 1, 0, &[b"a"]),
            write(1, 1, 0, &[b"a"]),
            write(1, 1, 0, &[b"other"]),
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
    fn a_retry_survives_an_ownership_change_but_not_a_truncation() {
        let (_, outcomes) = fold(&[
            set_leader(0, 1),
            write(1, 1, 0, &[b"a"]),
            set_leader(1, 2),
            write(1, 1, 0, &[b"a"]),
            truncate(2, 2, 1),
            write(1, 1, 0, &[b"a"]),
        ]);
        assert!(matches!(outcomes[3], Outcome::Duplicate { .. }));
        assert!(matches!(outcomes[5], Outcome::Truncated(s) if s.first_seq == Seq(1)));
    }

    #[test]
    fn set_leader_is_a_compare_and_swap() {
        let (state, outcomes) = fold(&[set_leader(0, 1), set_leader(0, 2), set_leader(1, 2)]);
        assert!(matches!(outcomes[0], Outcome::Leader(s) if s.generation == Generation(1)));
        assert!(matches!(outcomes[1], Outcome::LeaderRefused(s) if s.owner == Some(ClientId(1))));
        assert!(matches!(outcomes[2], Outcome::Leader(_)));
        assert_eq!(state.owner, Some(ClientId(2)));
        assert_eq!(state.generation, Generation(2));
    }

    #[test]
    fn truncate_is_monotone_and_clamped_to_the_next_position() {
        let (state, _) = fold(&[
            set_leader(0, 1),
            write(1, 1, 0, &[b"a", b"b"]),
            truncate(1, 1, 9),
            truncate(1, 1, 1),
        ]);
        assert_eq!(state.first_seq, Seq(2));
    }

    #[test]
    fn truncate_is_fenced_like_a_write() {
        let (state, outcomes) = fold(&[
            truncate(0, 1, 0),
            set_leader(0, 1),
            write(1, 1, 0, &[b"a", b"b"]),
            truncate(1, 2, 1),
            set_leader(1, 2),
            truncate(1, 1, 2),
            truncate(2, 2, 1),
        ]);
        assert!(matches!(outcomes[0], Outcome::TruncateRefused(s) if s.owner.is_none()));
        assert!(matches!(outcomes[3], Outcome::TruncateRefused(s) if s.first_seq == Seq(0)));
        assert!(
            matches!(outcomes[5], Outcome::TruncateRefused(s) if s.generation == Generation(2))
        );
        assert!(matches!(outcomes[6], Outcome::Trimmed(s) if s.first_seq == Seq(1)));
        assert_eq!(state.first_seq, Seq(1));
    }
}
