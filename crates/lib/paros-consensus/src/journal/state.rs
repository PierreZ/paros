//! The **journal state machine** (#204, #241): one per journal, judged at
//! apply.
//!
//! A journal exposes four calls — `Write`, `Read`, `Truncate`, `SetLeader`
//! (`docs/architecture/02-data-plane.md`, section 2) — and every rule behind them is
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
//!   its clients to draw fresh uuids, `docs/architecture/02-data-plane.md` §2.3). No lease,
//!   no clock.
//! - **`Truncate(leader_uuid, up_to_seq)`** ([`Control::Truncate`], #228) is
//!   fenced like a `Write`: it is accepted iff `leader_uuid` is current, and
//!   then raises `first_seq` to `up_to_seq`, clamped to `next_seq`. Monotone.
//!   A superseded or foreign caller is refused ([`Outcome::TruncateRefused`])
//!   and nothing moves: anyone holding the tenant could otherwise truncate to
//!   a position that is not the leader's checkpoint.
//!
//! A journal has one of two **writer modes** ([`WriterMode`]), fixed when it
//! is created (`docs/architecture/02-data-plane.md` §2, §2.4). The rules above are the
//! single-writer mode's. A **multi-writer** journal has no leader:
//!
//! - **`Write(batch)`** is a [`Command::Write`] under the unset uuid. It is
//!   accepted whenever it carries records, at `next_seq`, whatever position
//!   it names: the journal assigns the position at apply. There is no
//!   expected position and no deduplication, so a retried write may land
//!   twice (at-least-once).
//! - **`Truncate(up_to_seq)`** is a [`Control::Truncate`] under the unset
//!   uuid, and anyone may send it. It moves `first_seq` as above.
//! - **`SetLeader`** is refused.
//!
//! A call shaped for the other mode (a multi-writer call on a single-writer
//! journal, a leader uuid on a multi-writer one, any `SetLeader` on a
//! multi-writer one) is [`Outcome::WrongMode`], and nothing moves: a client
//! that holds the wrong mode learns it at once, and never writes unfenced
//! where it believes a fence holds.
//!
//! A refusal is answered in place and names the view it was judged against,
//! so a leader learns where the journal is (the next position) or that it
//! was superseded (another leader uuid). Refusals, `Noop`s, control commands
//! and leadership changes consume a slot and no position: positions are
//! dense.
//!
//! - **`Roll(next, replicas)`** ([`Command::Roll`], §4.7 of
//!   `docs/architecture/04-7-segments.md`) moves the tail [`Segment`]: it is
//!   valid iff `next` is the tail plus one, and then the tail becomes `next`,
//!   from the slot after the `Roll` ([`Outcome::Rolled`]). Any other `Roll`
//!   changes nothing ([`Outcome::RollIgnored`]). It is the same under both
//!   writer modes: a segment is a range of the log, not a right to write.
//! - **`Hold(segment, end, next_seq)`** ([`Command::Hold`], R7 and D9 of
//!   §4.7) extends the held prefix ([`JournalState::held`]): it is valid iff
//!   it names the segment right after the held prefix, that segment is
//!   sealed, and its end and position lie past the held prefix and inside
//!   the fold ([`Outcome::HeldTo`]). Any other `Hold` changes nothing
//!   ([`Outcome::HoldIgnored`]). Once a segment is held, a single-writer
//!   retry of a write inside the held prefix is [`Outcome::Held`]: the
//!   replicas of its segment hold it, and the acceptors may drop it. The
//!   answer is a function of the chosen prefix, so every node gives it
//!   (`OneAnswer` of `specs/tla/held-floor-hold/`).
//! - **`SetReplicas(segment, replicas)`** ([`Command::SetReplicas`], D10 of
//!   §4.7) gives the tail segment new replicas: it is valid iff `segment` is
//!   the tail and names no member that an earlier `SetReplicas` of the tail
//!   removed ([`JournalState::removed`]), and then
//!   [`JournalState::replicas`] is the new set from the slot after it
//!   ([`Outcome::Reseated`]), and the members of the set before it that the
//!   new one leaves out join the removed ones. A `SetReplicas` of another
//!   segment changes nothing ([`Outcome::ReseatIgnored`]), and nor does one
//!   that names a removed member ([`Outcome::ReseatRemoved`]): a removed
//!   member stopped at the `SetReplicas` that removed it, so it would keep
//!   no slot after a second one that named it again.
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

use crate::journal::{
    Command, Control, Entry, Hold, LeaderUuid, ReplicaSet, Roll, Segment, SegmentId, Seq,
    SetReplicas,
};
use crate::types::{NodeId, Slot};

/// Who may write a journal (`docs/architecture/02-data-plane.md` §2, decided on
/// 2026-10-04): fixed when the journal is created, and never changed. The
/// mode is configuration, recorded in the store's format marker with the
/// rest of [`crate::Config`], so a restart can never change it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WriterMode {
    /// One leader writes under its leader uuid (§2.3): `Write` and
    /// `Truncate` are fenced by it, and `SetLeader` moves it.
    #[default]
    Single,
    /// Anyone with access writes (§2.4): no leader, no fence, no
    /// deduplication, and an open `Truncate`.
    Multi,
}

/// The per-journal control state (#204, #241): who may write, and where the
/// journal's dense positions stand. Folded from the log in slot order; the
/// same prefix folds to the same state on every node.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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
    /// The tail segment of the log: raised by every valid `Roll`. A client
    /// never sees it ([`JournalState::view`] drops it); it is the log's, not
    /// the journal's. A sealed state written before segments existed reads
    /// as the first segment, which is where its log stands: no `Roll` was
    /// ever chosen in it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub segment: Segment,
    /// The replicas of the tail segment that the last valid `Roll` naming a
    /// set or the last valid `SetReplicas` named, whichever came later (R6,
    /// D4 and D10 of §4.7): a `Roll` with no set keeps the set of the
    /// segment before it. `None` until a `Roll` named one: the replicas of
    /// the first segment, which the bootstrap configuration names
    /// ([`crate::contract::Config::segment_replicas`]), keep every segment
    /// until then. A function of the chosen prefix like the rest of the
    /// state, so the floor seals it and a trim-point jump carries it: a
    /// leader routes the `Commit` of a slot to the replicas of its segment
    /// from its own fold (R4c, R4d), never from the chain.
    #[cfg_attr(feature = "serde", serde(default))]
    pub replicas: Option<ReplicaSet>,
    /// The held prefix (R7, D9 of §4.7): the last valid [`Hold`], whose
    /// segment and every segment before it are held. `None` until the first
    /// valid `Hold`. A function of the chosen prefix like the rest of the
    /// state, so the floor seals it and a trim-point jump carries it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub held: Option<Hold>,
    /// The members that a valid `SetReplicas` of the tail segment removed
    /// (D10 of §4.7): each was in the set before it and is out of the set
    /// after it. Disjoint from [`JournalState::replicas`], since a
    /// `SetReplicas` that names one again is refused. `None` until a
    /// `SetReplicas` of the tail removed a member; a valid `Roll` clears it.
    /// A function of the chosen prefix like the rest of the state, so a
    /// replica that the floor or a trim-point jump carried past the
    /// `SetReplicas` still knows that its segment ended there
    /// ([`crate::replica::Replica::moved_to`]), and a peer still serves it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub removed: Option<ReplicaSet>,
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
    /// The call is shaped for the other [`WriterMode`]: a multi-writer call
    /// (the unset uuid) on a single-writer journal, or a leader uuid or a
    /// `SetLeader` on a multi-writer journal. Nothing moved; the view is the
    /// one it was judged against.
    WrongMode(JournalView),
    /// A valid `Roll`: the tail segment after it.
    Rolled(Segment),
    /// A `Roll` that does not name the tail plus one: nothing moved. The
    /// segment is the tail it was judged against.
    RollIgnored(Segment),
    /// A valid `Hold`: the held prefix after it.
    HeldTo(Hold),
    /// A valid `SetReplicas`: the replicas of the tail segment after it.
    Reseated(ReplicaSet),
    /// A `SetReplicas` that does not name the tail segment: nothing moved.
    /// The segment is the tail it was judged against.
    ReseatIgnored(Segment),
    /// A `SetReplicas` of the tail that names this member, which an earlier
    /// `SetReplicas` of the tail removed ([`JournalState::removed`]):
    /// nothing moved.
    ReseatRemoved(NodeId),
    /// A `Hold` that does not extend the held prefix: nothing moved. It
    /// names the held prefix it was judged against.
    HoldIgnored(Option<Hold>),
    /// The write asked for a position inside the held prefix: its segment
    /// is held, so the replicas of the segment answer for it, and the
    /// acceptors may have dropped the records there. It is never an ack
    /// and carries no fence verdict: a write there with a stale uuid or
    /// other bytes is `Held` too. Nothing moved; the view is the one it was
    /// judged against.
    Held(JournalView),
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

    /// Apply one decided `command`, chosen at `slot`, under the journal's
    /// `mode`. `first` is the bootstrap set of the first segment
    /// ([`crate::contract::Config::segment_replicas`]), the set that a
    /// `SetReplicas` replaces while [`JournalState::replicas`] is `None`;
    /// every node of a deployment hands the same one. `accepted_at(seq)` answers "which write was accepted with its first
    /// record at `seq`?" for a single-writer retry below `next_seq`; it is
    /// only asked for a position at or above `first_seq`, and never on a
    /// multi-writer journal.
    ///
    /// # Panics
    ///
    /// If `slot` lies below the start of the tail segment (a fold never goes
    /// back), or a transition breaks the state's own ordering or
    /// monotonicity (a programmer error, never an operating condition).
    pub fn apply<'a>(
        &mut self,
        mode: WriterMode,
        first: Option<&ReplicaSet>,
        slot: Slot,
        command: &Command,
        accepted_at: impl Fn(Seq) -> Option<&'a Entry>,
    ) -> Outcome {
        self.assert_invariants();
        assert!(
            slot >= self.segment.start,
            "a slot is folded inside or after the tail segment"
        );
        if mode == WriterMode::Multi {
            assert!(
                self.leader.is_none(),
                "a multi-writer journal never has a leader"
            );
        }
        let before = self.clone();
        let outcome = self.judge(mode, first, slot, command, accepted_at);
        self.assert_mode(mode, &outcome);
        self.assert_transition(&before, slot, &outcome);
        self.assert_invariants();
        outcome
    }

    /// A single-writer write: accepted only under the current leader uuid,
    /// at `next_seq`; a retry of the accepted write is a duplicate.
    ///
    /// Proved in simulation (#241): with the uuid check dropped, 172 of the
    /// mutation hunt's 300 seeds go red on "a write is accepted only under a
    /// uuid won in the log" and the linearizability check.
    fn apply_write<'a>(
        &mut self,
        entry: &Entry,
        accepted_at: impl Fn(Seq) -> Option<&'a Entry>,
    ) -> Outcome {
        if entry.seq < self.first_seq {
            return Outcome::Truncated(self.view());
        }
        if self.held.is_some_and(|held| entry.seq < held.next_seq) {
            // Checked before the lookup: the acceptors may have dropped the
            // records of a held segment, so the log no longer answers here.
            assert!(
                entry.seq < self.next_seq,
                "the held prefix lies below the next position"
            );
            return Outcome::Held(self.view());
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
        assert!(
            entry.seq == self.next_seq,
            "an accepted write takes the position it names"
        );
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

    /// The postconditions of one `apply`, from the state `before` it: each
    /// scalar is monotone, and each outcome moves exactly its own scalars.
    fn assert_transition(&self, before: &Self, slot: Slot, outcome: &Outcome) {
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
        self.assert_segment(before, slot, outcome);
        // Negative space, per outcome: a refusal, a duplicate and a `Noop`
        // move nothing; each accepted transition moves exactly its own
        // scalars.
        match outcome {
            Outcome::Refused(_)
            | Outcome::Truncated(_)
            | Outcome::LeaderRefused(_)
            | Outcome::TruncateRefused(_)
            | Outcome::WrongMode(_)
            | Outcome::Duplicate { .. }
            | Outcome::RollIgnored(_)
            | Outcome::HoldIgnored(_)
            | Outcome::ReseatIgnored(_)
            | Outcome::ReseatRemoved(_)
            | Outcome::Held(_)
            | Outcome::Noop => {
                assert!(*self == *before, "a refused or no-op command moves nothing");
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
            Outcome::Rolled(_) => {
                assert!(
                    self.view() == before.view(),
                    "a Roll moves no position and no leader"
                );
                assert!(self.term == before.term, "a Roll keeps the term");
            }
            Outcome::HeldTo(_) | Outcome::Reseated(_) => {}
        }
        self.assert_held(before, outcome);
        self.assert_replicas(before, outcome);
        // The refusals name the view they were judged against.
        if let Outcome::Refused(judged)
        | Outcome::Truncated(judged)
        | Outcome::LeaderRefused(judged)
        | Outcome::TruncateRefused(judged)
        | Outcome::WrongMode(judged)
        | Outcome::Held(judged) = outcome
        {
            assert!(
                *judged == before.view(),
                "a refusal names the state it was judged against"
            );
        }
    }

    /// The held prefix's postconditions: it only grows, only a valid `Hold`
    /// moves it, and an ignored `Hold` names the prefix it was judged
    /// against.
    fn assert_held(&self, before: &Self, outcome: &Outcome) {
        if self.held != before.held {
            assert!(
                matches!(outcome, Outcome::HeldTo(_)),
                "only a valid Hold moves the held prefix"
            );
            assert!(
                before
                    .held
                    .is_none_or(|was| self.held.is_some_and(|now| now.segment > was.segment)),
                "the held prefix never goes back"
            );
        }
        if let Outcome::HoldIgnored(judged) = outcome {
            assert!(
                *judged == before.held,
                "an ignored Hold names the held prefix it was judged against"
            );
        }
        if let Outcome::HeldTo(hold) = outcome {
            assert!(
                self.held == Some(*hold),
                "a valid Hold reports the held prefix after it"
            );
            assert!(
                self.view() == before.view() && self.segment == before.segment,
                "a Hold moves no position, no leader and no segment"
            );
            assert!(self.term == before.term, "a Hold keeps the term");
        }
    }

    /// The replicas' postconditions: only a valid `Roll` or a valid
    /// `SetReplicas` moves them, a valid `SetReplicas` moves nothing else and
    /// reports the set after it, and an ignored one names the tail it was
    /// judged against.
    fn assert_replicas(&self, before: &Self, outcome: &Outcome) {
        if self.replicas != before.replicas {
            assert!(
                matches!(outcome, Outcome::Rolled(_) | Outcome::Reseated(_)),
                "only a valid Roll or SetReplicas moves the replicas"
            );
        }
        if let Outcome::Reseated(set) = outcome {
            assert!(
                self.replicas.as_ref() == Some(set),
                "a valid SetReplicas reports the replicas after it"
            );
            assert!(
                self.view() == before.view()
                    && self.segment == before.segment
                    && self.held == before.held,
                "a SetReplicas moves no position, no leader, no segment and no held prefix"
            );
            assert!(self.term == before.term, "a SetReplicas keeps the term");
        }
        if let Outcome::ReseatIgnored(judged) = outcome {
            assert!(
                *judged == before.segment,
                "an ignored SetReplicas names the tail it was judged against"
            );
        }
        if let Outcome::ReseatRemoved(member) = outcome {
            assert!(
                before.removed.as_ref().is_some_and(|r| r.contains(*member)),
                "a refused SetReplicas names a removed member"
            );
        }
        if self.removed != before.removed {
            assert!(
                matches!(outcome, Outcome::Rolled(_) | Outcome::Reseated(_)),
                "only a valid Roll or SetReplicas moves the removed members"
            );
        }
        if matches!(outcome, Outcome::Rolled(_)) {
            assert!(
                self.removed.is_none(),
                "a valid Roll clears the removed members"
            );
        }
        if let Outcome::Reseated(_) = outcome {
            // The removed members only grow inside a segment.
            for member in before.removed.iter().flat_map(ReplicaSet::members) {
                assert!(
                    self.removed.as_ref().is_some_and(|r| r.contains(*member)),
                    "a SetReplicas keeps every removed member removed"
                );
            }
        }
        // A removed member is never a replica of the tail: the set that a
        // valid `SetReplicas` gives never names one.
        assert!(
            self.removed_apart(),
            "a removed member is not a replica of the tail"
        );
    }

    /// The tail segment's postconditions: it never goes back, only a valid
    /// `Roll` moves it, to the next id from the slot after the `Roll`, and an
    /// ignored `Roll` names the tail it was judged against.
    fn assert_segment(&self, before: &Self, slot: Slot, outcome: &Outcome) {
        assert!(
            self.segment.id >= before.segment.id,
            "a journal's tail segment never goes back"
        );
        if self.segment != before.segment {
            assert!(
                matches!(outcome, Outcome::Rolled(_)),
                "only a valid Roll moves the tail segment"
            );
        }
        match outcome {
            Outcome::Rolled(tail) => {
                assert!(*tail == self.segment, "a Roll reports the tail after it");
                assert!(
                    tail.id == before.segment.id.successor(),
                    "a valid Roll opens the next segment"
                );
                assert!(
                    tail.start.0 == slot.0 + 1,
                    "a Roll ends the tail at its own slot"
                );
            }
            Outcome::RollIgnored(judged) => {
                assert!(
                    *judged == before.segment,
                    "an ignored Roll names the tail it was judged against"
                );
            }
            _ => {}
        }
    }

    /// A `Roll`, chosen at `slot`: valid iff its `next` is the tail segment
    /// plus one (R2 of §4.7, [`SegmentId::after`]). The fold of the chosen prefix is the
    /// same on every node, so the first valid `Roll` of a segment is the
    /// same slot everywhere, and a segment has one end (R3; `SegsAgree` of
    /// `specs/tla/multi-paxos-roll/`).
    fn apply_roll(&mut self, slot: Slot, roll: &Roll) -> Outcome {
        let next = self.segment.id.after(roll);
        if next == self.segment.id {
            return Outcome::RollIgnored(self.segment);
        }
        let before = self.segment;
        self.segment = before.rolled_at(slot);
        if let Some(named) = &roll.replicas {
            self.replicas = Some(named.clone());
        }
        self.removed = None;
        assert!(
            roll.replicas.is_none() || self.replicas == roll.replicas,
            "a valid Roll that names replicas gives them to the segment it opens"
        );
        assert!(
            self.segment.id == next,
            "a valid Roll opens the segment it names"
        );
        assert!(
            self.segment.start > slot,
            "the next segment opens past the Roll that ends the tail"
        );
        assert!(
            self.segment.start > before.start,
            "a valid Roll moves the tail forward"
        );
        Outcome::Rolled(self.segment)
    }

    /// A `Hold`: valid iff it names the segment right after the held prefix
    /// ([`SegmentId::FIRST`] when none is held), that segment is sealed (it
    /// lies below the tail), its end lies past the held prefix and at or
    /// below the start of the tail, and its position lies at or past the
    /// held prefix and at or below `next_seq`. The rule that the held
    /// segments are a prefix is `FloorTo = "prefix"` of
    /// `specs/tla/held-floor/` (its control `floor_any` loses a slot); the
    /// fold judges it, so a `Hold` that a leader proposed out of order
    /// changes nothing.
    fn apply_hold(&mut self, hold: Hold) -> Outcome {
        let expected = self
            .held
            .map_or(SegmentId::FIRST, |h| h.segment.successor());
        let after_end = self.held.map_or(Slot(0), |h| h.end);
        let after_seq = self.held.map_or(Seq(0), |h| h.next_seq);
        let valid = hold.segment == expected
            && hold.segment < self.segment.id
            && hold.end > after_end
            && hold.end <= self.segment.start
            && hold.next_seq >= after_seq
            && hold.next_seq <= self.next_seq;
        if !valid {
            return Outcome::HoldIgnored(self.held);
        }
        let before = self.held;
        self.held = Some(hold);
        assert!(
            before.is_none_or(|was| hold.segment == was.segment.successor()),
            "a valid Hold extends the held prefix by one segment"
        );
        assert!(hold.segment < self.segment.id, "a held segment is sealed");
        // Segment `k` ends past slot `k`: the end of the first lies past 0,
        // and each end lies past the one before it, so no guard asks it.
        assert!(
            hold.end.0 > hold.segment.0,
            "a held segment ends past its own id"
        );
        Outcome::HeldTo(hold)
    }

    /// A `SetReplicas`: valid iff it names the tail segment (D10 of §4.7).
    /// The fold of the chosen prefix is the same on every node, so every
    /// node gives each slot the same set, and a leader routes the `Commit`
    /// of a slot to the set that its fold gives there (`Route = "fold"` of
    /// `specs/tla/segment-reseat/`; its control `route_proposed`, a route
    /// by the proposed command, puts a slot at a replica that no chosen set
    /// names).
    fn apply_set_replicas(&mut self, set: &SetReplicas, first: Option<&ReplicaSet>) -> Outcome {
        if set.segment != self.segment.id {
            return Outcome::ReseatIgnored(self.segment);
        }
        if let Some(removed) = &self.removed
            && let Some(member) = set
                .replicas
                .members()
                .iter()
                .find(|m| removed.contains(**m))
        {
            probe!(
                reachable,
                "journal: a SetReplicas that names a removed member is refused"
            );
            return Outcome::ReseatRemoved(*member);
        }
        probe!(
            reachable,
            "journal: a SetReplicas gives the tail new replicas"
        );
        // The set before it: the tail's own, else the bootstrap set of the
        // first segment. A first segment with no bootstrap set is kept by
        // every node of the pool, which no `SetReplicas` moves.
        let old = self.replicas.as_ref().or(first);
        let gone = old.into_iter().flat_map(ReplicaSet::members).copied();
        let gone: Vec<NodeId> = gone
            .filter(|m| !set.replicas.contains(*m))
            .chain(self.removed.iter().flat_map(ReplicaSet::members).copied())
            .collect::<std::collections::BTreeSet<NodeId>>()
            .into_iter()
            .collect();
        self.removed = (!gone.is_empty()).then(|| ReplicaSet::new(gone));
        self.replicas = Some(set.replicas.clone());
        assert!(
            self.replicas.as_ref() == Some(&set.replicas),
            "a valid SetReplicas gives its set to the tail"
        );
        assert!(
            set.segment == self.segment.id,
            "a valid SetReplicas names the tail"
        );
        Outcome::Reseated(set.replicas.clone())
    }

    /// The verdict on `command` under `mode`, the state moved accordingly.
    ///
    /// Proved in simulation (#241): a single-writer journal that takes an
    /// unfenced write goes red on "an unfenced write to a single-writer
    /// journal is refused" (5 of 300 seeds), and a multi-writer journal that
    /// appends a fenced write on "a multi-writer write is accepted unfenced"
    /// (2 of 300).
    fn judge<'a>(
        &mut self,
        mode: WriterMode,
        first: Option<&ReplicaSet>,
        slot: Slot,
        command: &Command,
        accepted_at: impl Fn(Seq) -> Option<&'a Entry>,
    ) -> Outcome {
        match (mode, command) {
            (_, Command::Control(Control::Noop)) => Outcome::Noop,
            (_, Command::Roll(roll)) => self.apply_roll(slot, roll),
            (_, Command::Hold(hold)) => self.apply_hold(*hold),
            (_, Command::SetReplicas(set)) => self.apply_set_replicas(set, first),
            (WriterMode::Single, Command::Write(entry)) if !entry.leader.is_set() => {
                Outcome::WrongMode(self.view())
            }
            (WriterMode::Single, Command::Control(Control::Truncate { leader, .. }))
                if !leader.is_set() =>
            {
                Outcome::WrongMode(self.view())
            }
            (WriterMode::Single, Command::Write(entry)) => self.apply_write(entry, accepted_at),
            (WriterMode::Single, Command::Control(Control::SetLeader { new, old })) => {
                self.apply_set_leader(*new, *old)
            }
            (WriterMode::Single, Command::Control(Control::Truncate { leader, up_to })) => {
                self.apply_truncate(*leader, *up_to)
            }
            (WriterMode::Multi, Command::Write(entry)) if !entry.leader.is_set() => {
                self.apply_append(entry)
            }
            (WriterMode::Multi, Command::Control(Control::Truncate { leader, up_to }))
                if !leader.is_set() =>
            {
                self.trim(*up_to)
            }
            (
                WriterMode::Multi,
                Command::Write(_)
                | Command::Control(Control::Truncate { .. } | Control::SetLeader { .. }),
            ) => Outcome::WrongMode(self.view()),
        }
    }

    /// The writer mode's postconditions (#241): a multi-writer journal never
    /// gains a leader or a term, and judges no fence and no retry.
    fn assert_mode(&self, mode: WriterMode, outcome: &Outcome) {
        if mode == WriterMode::Multi {
            assert!(
                self.leader.is_none(),
                "a multi-writer journal never gains a leader"
            );
            assert!(self.term == 0, "a multi-writer journal never has a term");
            assert!(
                !matches!(
                    *outcome,
                    Outcome::Duplicate { .. }
                        | Outcome::Truncated(_)
                        | Outcome::Leader(_)
                        | Outcome::LeaderRefused(_)
                        | Outcome::TruncateRefused(_)
                ),
                "a multi-writer journal judges no fence and no retry"
            );
        }
    }

    /// A multi-writer write: accepted at the next position whenever it
    /// carries records, whatever position it names (#241, §2.4).
    ///
    /// Proved in simulation (#241): refusing a write whose position is not
    /// `next_seq` goes red on "a multi-writer journal accepts every unfenced
    /// write" (11 of 300 seeds).
    fn apply_append(&mut self, entry: &Entry) -> Outcome {
        assert!(
            !entry.leader.is_set(),
            "a multi-writer write names no leader"
        );
        if entry.records.is_empty() {
            return Outcome::Refused(self.view());
        }
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

    /// `SetLeader(new, old)`: a compare-and-set on the leader uuid.
    ///
    /// Proved in simulation (#241): without the compare on `old`, 83 of 300
    /// seeds go red on the linearizability check.
    fn apply_set_leader(&mut self, new: LeaderUuid, old: Option<LeaderUuid>) -> Outcome {
        // The unset uuid never leads, and the current leader cannot win its
        // own term again.
        if old != self.leader || !new.is_set() || Some(new) == self.leader {
            return Outcome::LeaderRefused(self.view());
        }
        // Restated one leg at a time (#269): a won SetLeader replaces the
        // leader it named with a different, set uuid.
        assert!(
            old == self.leader,
            "a won SetLeader named the leader in force"
        );
        assert!(new.is_set(), "a won SetLeader installs a set uuid");
        assert!(
            Some(new) != self.leader,
            "a won SetLeader installs a different uuid"
        );
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

    /// A single-writer `Truncate`: fenced by the leader uuid like a write.
    ///
    /// Proved in simulation (#228, #241): unfenced, 81 of 300 seeds go red
    /// on "a Truncate is accepted only from the writer in force".
    fn apply_truncate(&mut self, leader: LeaderUuid, up_to: Seq) -> Outcome {
        if !self.is_current(leader) {
            return Outcome::TruncateRefused(self.view());
        }
        self.trim(up_to)
    }

    /// Raise `first_seq` to `up_to`, clamped to `next_seq`, never lowered:
    /// an accepted `Truncate` of either mode.
    fn trim(&mut self, up_to: Seq) -> Outcome {
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

    /// Whether the state keeps its own ordering (the checks of
    /// [`Self::assert_invariants`], as a value): the gate of a state that
    /// comes from outside the core, such as the boundary state a driver
    /// hands a joining replica.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        self.first_seq <= self.next_seq
            && self.leader.is_some() == (self.term > 0)
            && self.leader.is_none_or(LeaderUuid::is_set)
            && (self.segment.id == SegmentId::FIRST) == (self.segment.start == Slot(0))
            && self.segment.start.0 >= self.segment.id.0
            && self.held.is_none_or(|held| self.holds(held))
            && self.removed_apart()
    }

    /// Whether no removed member is a replica of the tail
    /// ([`JournalState::removed`]).
    fn removed_apart(&self) -> bool {
        match (&self.removed, &self.replicas) {
            (Some(removed), Some(replicas)) => {
                removed.members().iter().all(|m| !replicas.contains(*m))
            }
            _ => true,
        }
    }

    /// Whether this state, sealed at a slot past `segment`, proves that every
    /// write of `segment` lies below `first_seq`, the `first_seq` of a later
    /// fold of the same log (its head): the state is in a later segment,
    /// and either every write before it is released (`first_seq` reached
    /// its `next_seq`), or its held prefix reaches `segment` and every
    /// write up to the held end is released. A lagging replica of
    /// `segment` that a trim point past it reaches has nothing left to copy
    /// (R6 and R7 of `docs/architecture/04-7-segments.md`; its model is
    /// `TrimProof` of `specs/tla/held-floor-trimmed/`). A `first_seq` below
    /// the state's own counts as the state's: a later fold never lowers it.
    ///
    /// # Panics
    ///
    /// If the held prefix ends past the fold: a broken state, a programmer
    /// error.
    #[must_use]
    pub fn releases(&self, segment: SegmentId, first_seq: Seq) -> bool {
        if self.segment.id <= segment {
            return false;
        }
        let first_seq = first_seq.max(self.first_seq);
        let all = first_seq >= self.next_seq;
        let held = self
            .held
            .is_some_and(|held| held.segment >= segment && first_seq >= held.next_seq);
        assert!(
            self.held.is_none_or(|h| h.next_seq <= self.next_seq),
            "a held prefix ends inside the fold"
        );
        assert!(
            first_seq >= self.first_seq,
            "the proof reads a first_seq at or above the state's"
        );
        all || held
    }

    /// Whether `held` fits this state as its held prefix: a sealed segment,
    /// whose end and position the fold reached.
    fn holds(&self, held: Hold) -> bool {
        held.segment < self.segment.id
            && held.end <= self.segment.start
            && held.end.0 > held.segment.0
            && held.next_seq <= self.next_seq
    }

    /// The state's own ordering.
    ///
    /// # Panics
    ///
    /// If `first_seq` passed `next_seq`, or a journal with a term has no
    /// leader (or the reverse), or the unset uuid leads, or the tail segment
    /// does not fit its id (the first segment starts at slot 0, and segment
    /// `k` at slot `k` or later: each `Roll` before it took a slot), or the
    /// held prefix does not fit the state (a held segment is sealed, and its
    /// end and position lie inside the fold).
    pub fn assert_invariants(&self) {
        assert!(
            self.removed_apart(),
            "a removed member is never a replica of the tail"
        );
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
        assert!(
            (self.segment.id == SegmentId::FIRST) == (self.segment.start == Slot(0)),
            "only the first segment starts at slot 0"
        );
        assert!(
            self.segment.start.0 >= self.segment.id.0,
            "each segment before the tail ended at a Roll of its own slot"
        );
        if let Some(held) = self.held {
            assert!(held.segment < self.segment.id, "a held segment is sealed");
            assert!(
                held.end <= self.segment.start,
                "a held segment ends at or before the tail"
            );
            assert!(
                held.end.0 > held.segment.0,
                "a held segment ends past the Roll of each segment before it"
            );
            assert!(
                held.next_seq <= self.next_seq,
                "the held prefix lies below the next position"
            );
        }
    }
}

#[cfg(test)]
mod tests;
