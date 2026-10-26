//! The election's record and its fold (#240, `docs/architecture/03-3-coordinators-and-placement.md` §3.3):
//! the one deterministic rule every watcher of an election journal runs, so
//! every watcher that folded the same records agrees on who leads which term.
//!
//! The journal is multi-writer (§2.4): candidates append blind, and the
//! journal orders the appends. The rule decides from that order alone, never
//! from a clock:
//!
//! - **Campaign** for term `t`: wins iff the fold is at term `t - 1`. Of two
//!   campaigns for one term, the first in the journal wins; the second, and
//!   any campaign for a term already taken, loses. A campaign deposes a live
//!   leader: the candidate decided, on its own clock, that the leader was
//!   gone (the lease is a liveness hint only). A wrong guess costs
//!   availability, never safety: the governed journal's leader uuid is the
//!   only fence (§2.3).
//! - **Renew** for term `t`: counts iff the fold names its writer the leader
//!   of `t` under its uuid. A renewal of a later term than the fold's is a
//!   leadership the fold missed (it started past the record that began it),
//!   and is adopted.
//! - **Resign** of term `t`: counts iff its writer leads `t`. With a
//!   successor, the successor leads `t + 1` at once (a hand-off); without
//!   one, the next campaign takes `t + 1`.
//!
//! **Every campaign and renewal describes the whole leadership** (the term,
//! the candidate, its leader uuid and interface), so the leader bounds the
//! journal by truncating it to its own latest renewal: a watcher that starts
//! there (a `Gap`, [`ElectionFold::gap`]) anchors on that record (or on the
//! campaign or hand-off that began the term, when the leader truncated
//! before its first renewal counted) and agrees
//! with every watcher that folded from the start. Until it meets an anchor,
//! such a watcher knows no leader. The rule assumes nothing else truncates
//! the journal: an election journal is the election's alone.
//!
//! **Only the record at the journal's floor anchors.** The floor is the
//! truncating leader's own anchor, a record its fold counted, so every fold
//! names that leader there. A record anywhere else proves nothing on its
//! own: a renewal may be a deposed leader's late one (hunt seed
//! 10124030878967114087 on 1f1f3c6 with PR #679: a fold that resumed at the tail adopted
//! `Renew(13)` written after `Campaign(14)` had deposed its writer below the
//! tail, and the next campaign for term 14 won in that fold alone). A fold
//! the floor outran ([`ElectionFold::resume_at_tail`]) keeps the records it
//! reads at the tail and anchors once a page reports the floor at one of
//! them ([`ElectionFold::floor_seen`]), or goes back to the floor
//! ([`ElectionFold::rewind`]).
//!
//! Anything in the journal that is not an election record (no [`MAGIC`]) is
//! ignored.

use paros_core::LeaderUuid;
use prost::Message;

use crate::wire::election as wire;
use crate::wire::{leader_uuid_from_proto, leader_uuid_to_proto};

/// The prefix of every election record: a leading zero byte, the name, and a
/// version.
pub const MAGIC: &[u8; 8] = b"\x00PRSELE\x01";

/// One candidate of an election: its id (unique among the candidates, never
/// zero) and its interface — where a request to it goes once it leads (the
/// `InterfaceRef` of §3.3).
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Candidate {
    /// Its id.
    pub id: u64,
    /// Its interface (an address).
    pub interface: String,
    /// The reference of the dynamic endpoint it serves its term at (#404
    /// (static vs dynamic endpoints)), encoded, opaque to the election: a
    /// caller aims it at `interface`. Empty until the leader publishes, and
    /// for an election whose leader serves no dynamic endpoint.
    pub reference: Vec<u8>,
}

/// An election record, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ElectionRecord {
    /// "I lead `term` under `uuid`", asked: wins iff the term is the next.
    Campaign {
        /// The term it asks for.
        term: u64,
        /// The candidate.
        candidate: Candidate,
        /// The leader uuid it leads the term under.
        uuid: LeaderUuid,
    },
    /// "I still lead `term` under `uuid`".
    Renew {
        /// The term it leads.
        term: u64,
        /// The leader.
        candidate: Candidate,
        /// Its leader uuid for the term.
        uuid: LeaderUuid,
    },
    /// The leader of `term` steps down, naming its successor (a hand-off)
    /// or none.
    Resign {
        /// The term it leads.
        term: u64,
        /// The leader's id.
        candidate: u64,
        /// Who leads `term + 1`, under which uuid.
        successor: Option<(Candidate, LeaderUuid)>,
    },
}

impl ElectionRecord {
    /// The term the record names.
    #[must_use]
    pub fn term(&self) -> u64 {
        match self {
            ElectionRecord::Campaign { term, .. }
            | ElectionRecord::Renew { term, .. }
            | ElectionRecord::Resign { term, .. } => *term,
        }
    }

    /// The record's bytes: [`MAGIC`] and the encoded record.
    ///
    /// # Panics
    ///
    /// On a record naming term 0, the zero candidate or the unset uuid: no
    /// leadership is ever that (a programmer error).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let leadership = |candidate: &Candidate, uuid: LeaderUuid| {
            assert!(candidate.id != 0, "a candidate id is never zero");
            assert!(uuid.is_set(), "a leadership names a set uuid");
            wire::Leadership {
                uuid: Some(leader_uuid_to_proto(uuid)),
                interface: candidate.interface.clone(),
                reference: candidate.reference.clone(),
            }
        };
        let (candidate, kind) = match self {
            ElectionRecord::Campaign {
                candidate, uuid, ..
            } => (
                candidate.id,
                wire::election_record::Kind::Campaign(leadership(candidate, *uuid)),
            ),
            ElectionRecord::Renew {
                candidate, uuid, ..
            } => (
                candidate.id,
                wire::election_record::Kind::Renew(leadership(candidate, *uuid)),
            ),
            ElectionRecord::Resign {
                candidate,
                successor,
                ..
            } => (
                *candidate,
                wire::election_record::Kind::Resign(match successor {
                    Some((next, uuid)) => {
                        let lead = leadership(next, *uuid);
                        wire::Resign {
                            successor: next.id,
                            uuid: lead.uuid,
                            interface: lead.interface,
                            reference: lead.reference,
                        }
                    }
                    None => wire::Resign::default(),
                }),
            ),
        };
        assert!(self.term() != 0, "no record names term 0");
        assert!(candidate != 0, "a candidate id is never zero");
        let mut record = MAGIC.to_vec();
        record.extend(
            wire::ElectionRecord {
                term: self.term(),
                candidate,
                kind: Some(kind),
            }
            .encode_to_vec(),
        );
        record
    }

    /// Read `record` back: `None` when it is no election record (no
    /// [`MAGIC`]), `Some(Err)` when it carries the magic but does not decode
    /// into a valid record.
    #[must_use]
    pub fn decode(record: &[u8]) -> Option<Result<Self, &'static str>> {
        let body = record.strip_prefix(MAGIC.as_slice())?;
        Some(Self::decode_body(body))
    }

    fn decode_body(body: &[u8]) -> Result<Self, &'static str> {
        let record =
            wire::ElectionRecord::decode(body).map_err(|_| "an election record does not decode")?;
        if record.term == 0 || record.candidate == 0 {
            return Err("an election record names a term and a candidate");
        }
        let leadership = |lead: wire::Leadership| {
            let uuid = leader_uuid_from_proto(lead.uuid);
            if uuid.is_set() {
                Ok((
                    Candidate {
                        id: record.candidate,
                        interface: lead.interface,
                        reference: lead.reference,
                    },
                    uuid,
                ))
            } else {
                Err("a leadership names a set uuid")
            }
        };
        match record.kind.ok_or("an election record names its kind")? {
            wire::election_record::Kind::Campaign(lead) => {
                let (candidate, uuid) = leadership(lead)?;
                Ok(ElectionRecord::Campaign {
                    term: record.term,
                    candidate,
                    uuid,
                })
            }
            wire::election_record::Kind::Renew(lead) => {
                let (candidate, uuid) = leadership(lead)?;
                Ok(ElectionRecord::Renew {
                    term: record.term,
                    candidate,
                    uuid,
                })
            }
            wire::election_record::Kind::Resign(resign) => {
                let successor = if resign.successor == 0 {
                    None
                } else {
                    let uuid = leader_uuid_from_proto(resign.uuid);
                    if !uuid.is_set() {
                        return Err("a hand-off names a set uuid");
                    }
                    Some((
                        Candidate {
                            id: resign.successor,
                            interface: resign.interface,
                            reference: resign.reference,
                        },
                        uuid,
                    ))
                };
                Ok(ElectionRecord::Resign {
                    term: record.term,
                    candidate: record.candidate,
                    successor,
                })
            }
        }
    }
}

/// Who leads, as a fold knows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leader {
    /// The term.
    pub term: u64,
    /// The leader.
    pub candidate: Candidate,
    /// The leader uuid it leads the term under: what it installs on the
    /// journal it governs.
    pub uuid: LeaderUuid,
    /// The position of the record that made it the leader (its campaign,
    /// the hand-off, or the renewal a fold anchored on).
    pub since: u64,
    /// The position of its latest record the fold counted: where the leader
    /// may truncate the journal to.
    pub anchor: u64,
}

/// What folding one record changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// A campaign won its term; `deposed` is the leader it replaced, if any
    /// (a takeover).
    Won {
        /// The previous leader's id.
        deposed: Option<u64>,
    },
    /// The leader handed the next term to its successor.
    HandedOff {
        /// The leader that resigned.
        from: u64,
    },
    /// The leader renewed.
    Renewed,
    /// The leader stepped down with no successor.
    Vacated,
    /// A renewal named a leadership the fold missed, and the fold adopted it
    /// (after a gap, or a later term).
    Adopted,
    /// A campaign for a term already taken lost.
    Lost {
        /// The candidate that lost.
        candidate: u64,
    },
    /// Not an election record, or one the rule does not count.
    Ignored,
}

/// The fold of one election journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElectionFold {
    /// The term the fold is at (0: no term yet).
    term: u64,
    /// Who leads it; `None` before the first campaign, after a resignation
    /// with no successor, or while the fold knows nothing.
    leader: Option<Leader>,
    /// Whether the fold folded every record that decides the term (from
    /// the start, or since an anchor after a gap).
    known: bool,
    /// The position a gap resumed the fold at, while it knows nothing: a
    /// campaign there is the leader's own (only the leader truncates).
    floor: Option<u64>,
    /// The term the fold was at when it resumed at the tail, while it
    /// knows nothing: the records past the tail raise the term without
    /// anchoring, so a rewind to the floor goes back to it.
    tail_term: Option<u64>,
    /// The records folded since the fold resumed at the tail, dense from
    /// the tail, while it knows nothing: none anchors on its own, but the
    /// one the floor reaches does ([`ElectionFold::floor_seen`]), and the
    /// ones past it are folded again from there.
    tail: Vec<(u64, Vec<u8>)>,
    /// The next position the fold expects.
    next: u64,
}

impl Default for ElectionFold {
    fn default() -> Self {
        Self::new()
    }
}

impl ElectionFold {
    /// The fold of an empty journal.
    #[must_use]
    pub fn new() -> Self {
        Self {
            term: 0,
            leader: None,
            known: true,
            floor: None,
            tail_term: None,
            tail: Vec::new(),
            next: 0,
        }
    }

    /// The term the fold is at.
    #[must_use]
    pub fn term(&self) -> u64 {
        self.term
    }

    /// Who leads, when the fold knows.
    #[must_use]
    pub fn leader(&self) -> Option<&Leader> {
        self.leader.as_ref()
    }

    /// Whether the fold knows the term (it is not waiting for an anchor).
    #[must_use]
    pub fn is_known(&self) -> bool {
        self.known
    }

    /// The next position the fold expects.
    #[must_use]
    pub fn next(&self) -> u64 {
        self.next
    }

    /// The positions below `floor` are gone (a reader met the journal's
    /// floor): the fold resumes there and knows nothing until it meets an
    /// anchor. A floor at or below the fold's next position changes nothing.
    ///
    /// # Panics
    ///
    /// If an assertion on its own postconditions fails: a programmer error.
    pub fn gap(&mut self, floor: u64) {
        if floor <= self.next {
            return;
        }
        self.next = floor;
        self.known = false;
        self.floor = Some(floor);
        self.leader = None;
        assert!(!self.known, "a fold past a gap knows nothing yet");
    }

    /// The floor outran a reader (#640 (election reader livelock)): the
    /// fold resumes at the journal's `tail`, past the floor `floor`, and
    /// knows nothing until the floor reaches a record it read there
    /// ([`ElectionFold::floor_seen`]) or it goes back to the floor
    /// ([`ElectionFold::rewind`]). Unlike [`ElectionFold::gap`], no record
    /// at the tail anchors it on its own: only the record at a leader's
    /// truncation floor is known to count. A campaign written at the tail
    /// may have lost (witness 1055341407506467323 on 9d93679: an operator's
    /// late campaign for a taken term), and a renewal there may be a deposed
    /// leader's, written after the campaign that deposed it, below the tail
    /// (hunt seed 10124030878967114087 on 1f1f3c6 with PR #679). A tail at or below the
    /// floor is the floor itself, a [`ElectionFold::gap`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own postconditions fails: a programmer error.
    pub fn resume_at_tail(&mut self, floor: u64, tail: u64) {
        if tail <= floor {
            self.gap(floor);
            return;
        }
        if tail <= self.next {
            return;
        }
        self.next = tail;
        self.known = false;
        self.floor = None;
        self.leader = None;
        self.tail_term.get_or_insert(self.term);
        // Every record held lies below the new floor: gone.
        self.tail.clear();
        assert!(!self.known, "a fold at the tail knows nothing yet");
        assert!(self.floor.is_none(), "the tail is no truncation floor");
    }

    /// How many records the fold holds from the tail
    /// ([`ElectionFold::resume_at_tail`]), while it knows nothing: one per
    /// page the tail wait returned, so a reader knows how long it waited
    /// there for the floor.
    #[must_use]
    pub fn held(&self) -> usize {
        self.tail.len()
    }

    /// An answer (a page, or a truncation) to a fold that knows nothing
    /// reported the journal's floor at `floor`. The floor is the leader's
    /// own anchor, so its record counts in every fold: when the fold holds
    /// that record (read at the tail, [`ElectionFold::resume_at_tail`], and
    /// kept across a [`ElectionFold::rewind`]), it anchors on it and folds
    /// the records past it again, with no read, so a floor that moves once
    /// per renewal period cannot outrun it (#640 (election reader
    /// livelock)). To a fold at the tail, a floor past every record held is
    /// where the next read starts, as a [`ElectionFold::gap`]. A floor
    /// below the records held is the one the fold skipped: the tail wait
    /// goes on, or the reader goes back to it. Whether the fold anchored.
    ///
    /// # Panics
    ///
    /// If an assertion on its own postconditions fails: a programmer error.
    pub fn floor_seen(&mut self, floor: u64) -> bool {
        if self.known {
            return false;
        }
        let Some(&(start, _)) = self.tail.first() else {
            if self.floor.is_none() && floor > self.next {
                // At the tail with nothing held: the floor's record is the
                // next read's first.
                self.replay(floor);
            }
            return false;
        };
        let end = self.tail.last().map_or(start, |(seq, _)| *seq + 1);
        if floor < start {
            return false;
        }
        if floor >= end && self.floor.is_some() {
            // Past every record held, and the reader is at a floor below
            // them: its answer, not this one, says where to resume.
            return false;
        }
        self.replay(floor);
        assert_eq!(
            self.floor.is_none(),
            self.known,
            "a floor's record anchors or is awaited"
        );
        self.known
    }

    /// Go back to `floor`, a position at or past the first record held at
    /// the tail, and fold the records held from there: the one at the floor
    /// anchors, the ones past it follow the rule. A floor past every record
    /// held leaves the fold waiting for its record.
    fn replay(&mut self, floor: u64) {
        assert!(!self.known, "only a fold that knows nothing replays");
        let held = std::mem::take(&mut self.tail);
        assert!(
            held.first().is_none_or(|(seq, _)| *seq <= floor),
            "a replay starts at a record held, or past them"
        );
        self.next = floor;
        self.floor = Some(floor);
        self.leader = None;
        // The records are folded again from the floor: the term they raised
        // goes back to what the fold knew before it met them.
        if let Some(term) = self.tail_term.take() {
            assert!(term <= self.term, "a term raised at the tail only grows");
            self.term = term;
        }
        for (seq, record) in held {
            if seq >= floor {
                self.absorb(seq, &record);
            }
        }
        assert!(self.next >= floor, "a replay ends at or past the floor");
    }

    /// A fold that knows nothing goes back to the journal's floor `floor`,
    /// to anchor on the record there: the leader's own, by the truncation
    /// rule. A fold at the tail ([`ElectionFold::resume_at_tail`]) that
    /// holds the floor's record folds from memory
    /// ([`ElectionFold::floor_seen`]); one whose floor lies below the
    /// records it holds resumes at the floor. Whether the fold still knows
    /// nothing, so the caller reads it on from [`ElectionFold::next`]; a
    /// fold that knows moves nowhere.
    ///
    /// # Panics
    ///
    /// If an assertion on its own postconditions fails: a programmer error.
    pub fn rewind(&mut self, floor: u64) -> bool {
        if self.known {
            return false;
        }
        if self.floor_seen(floor) {
            return false;
        }
        if self.floor.is_none() {
            // Still at the tail: the floor lies below every record held,
            // which stay held: the read from the floor may be answered
            // with a floor that moved onto one of them.
            assert!(floor < self.next, "a floor at or past the tail is seen");
            self.next = floor;
            self.floor = Some(floor);
            self.leader = None;
            // The records past the floor are folded again: the term they
            // raised goes back to what the fold knew before it met them.
            if let Some(term) = self.tail_term.take() {
                assert!(term <= self.term, "a term raised at the tail only grows");
                self.term = term;
            }
            assert_eq!(self.next, floor, "a rewound fold resumes at the floor");
        }
        assert!(!self.known, "a rewound fold knows nothing yet");
        assert!(
            self.floor.is_some(),
            "a rewound fold awaits its floor's record"
        );
        true
    }

    /// Fold the record at position `seq`.
    ///
    /// # Panics
    ///
    /// If `seq` is not the next position (positions arrive in order: a
    /// programmer error in the reader).
    pub fn absorb(&mut self, seq: u64, record: &[u8]) -> Change {
        assert_eq!(seq, self.next, "an election fold takes positions in order");
        self.next = seq + 1;
        if !self.known && self.floor.is_none() {
            // At the tail: held until the floor reaches one of them.
            self.tail.push((seq, record.to_vec()));
        } else if !self.known {
            // Read from the floor: the records held are read again.
            self.tail.clear();
        }
        let Some(Ok(record)) = ElectionRecord::decode(record) else {
            return Change::Ignored;
        };
        let change = if self.known {
            self.rule(seq, record)
        } else {
            self.anchor(seq, record)
        };
        assert!(
            self.leader
                .as_ref()
                .is_none_or(|leader| leader.term == self.term),
            "the leader leads the fold's term"
        );
        change
    }

    /// The rule, on a fold that knows the term.
    fn rule(&mut self, seq: u64, record: ElectionRecord) -> Change {
        match record {
            ElectionRecord::Campaign {
                term,
                candidate,
                uuid,
            } => {
                if term != self.term + 1 {
                    return Change::Lost {
                        candidate: candidate.id,
                    };
                }
                let deposed = self.leader.as_ref().map(|leader| leader.candidate.id);
                self.lead(term, candidate, uuid, seq);
                Change::Won { deposed }
            }
            ElectionRecord::Renew {
                term,
                candidate,
                uuid,
            } => {
                if let Some(leader) = self.leader.as_mut()
                    && leader.term == term
                    && leader.candidate.id == candidate.id
                    && leader.uuid == uuid
                {
                    leader.anchor = seq;
                    // A renewal republishes the interface: the leader names
                    // it once it serves (§3.3).
                    leader.candidate.interface = candidate.interface;
                    leader.candidate.reference = candidate.reference;
                    return Change::Renewed;
                }
                if term > self.term {
                    self.lead(term, candidate, uuid, seq);
                    return Change::Adopted;
                }
                Change::Ignored
            }
            ElectionRecord::Resign {
                term,
                candidate,
                successor,
            } => {
                let leads = self
                    .leader
                    .as_ref()
                    .is_some_and(|leader| leader.term == term && leader.candidate.id == candidate);
                if !leads {
                    return Change::Ignored;
                }
                if let Some((next, uuid)) = successor {
                    self.lead(term + 1, next, uuid, seq);
                    Change::HandedOff { from: candidate }
                } else {
                    self.leader = None;
                    Change::Vacated
                }
            }
        }
    }

    /// A fold that knows nothing: it anchors on the record at the very
    /// floor, a renewal (every renewal describes the whole leadership), a
    /// campaign or a hand-off (only the leader truncates, to its own anchor:
    /// its latest renewal, or the record that began its term), and counts
    /// nothing else. A renewal past the floor, or at the tail, may be a
    /// deposed leader's late one (hunt seed 10124030878967114087 on
    /// 86806ff): the campaign that deposed it lay below the fold's position.
    fn anchor(&mut self, seq: u64, record: ElectionRecord) -> Change {
        let at_floor = self.floor == Some(seq);
        match record {
            ElectionRecord::Renew {
                term,
                candidate,
                uuid,
            } if at_floor && term >= self.term => {
                self.lead(term, candidate, uuid, seq);
                Change::Adopted
            }
            ElectionRecord::Campaign {
                term,
                candidate,
                uuid,
            } if at_floor && term >= self.term => {
                self.lead(term, candidate, uuid, seq);
                Change::Adopted
            }
            // A hand-off begins its successor's term, as a campaign does:
            // the successor's anchor until it renews, so its truncation may
            // leave the hand-off at the floor. Unanchored, every later fold
            // knew no leader and every campaign past it lost, for good
            // (witness `replay-recipe 8483586457832106451
            // 53223:2760145812269184061` on 9bcbd0f: the successor truncated
            // to the hand-off, then rebooted).
            ElectionRecord::Resign {
                term,
                successor: Some((successor, uuid)),
                ..
            } if at_floor && term + 1 >= self.term => {
                self.lead(term + 1, successor, uuid, seq);
                Change::Adopted
            }
            other => {
                // Terms never go back: a later fold anchors at or past it.
                self.term = self.term.max(other.term());
                Change::Ignored
            }
        }
    }

    fn lead(&mut self, term: u64, candidate: Candidate, uuid: LeaderUuid, seq: u64) {
        assert!(term >= self.term, "a term never goes back");
        assert!(uuid.is_set(), "a leader leads under a set uuid");
        self.term = term;
        self.known = true;
        self.floor = None;
        self.tail_term = None;
        self.tail.clear();
        self.leader = Some(Leader {
            term,
            candidate,
            uuid,
            since: seq,
            anchor: seq,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn who(id: u64) -> Candidate {
        Candidate {
            id,
            interface: format!("10.0.0.{id}:4500"),
            reference: vec![u8::try_from(id % 256).unwrap_or(0)],
        }
    }

    fn campaign(term: u64, id: u64) -> Vec<u8> {
        ElectionRecord::Campaign {
            term,
            candidate: who(id),
            uuid: LeaderUuid(u128::from(term * 100 + id)),
        }
        .encode()
    }

    fn renew(term: u64, id: u64) -> Vec<u8> {
        ElectionRecord::Renew {
            term,
            candidate: who(id),
            uuid: LeaderUuid(u128::from(term * 100 + id)),
        }
        .encode()
    }

    fn fold(records: &[Vec<u8>]) -> (ElectionFold, Vec<Change>) {
        let mut fold = ElectionFold::new();
        let changes = records
            .iter()
            .enumerate()
            .map(|(seq, record)| fold.absorb(seq as u64, record))
            .collect();
        (fold, changes)
    }

    #[test]
    fn records_round_trip_and_foreign_bytes_are_ignored() {
        for record in [
            ElectionRecord::Campaign {
                term: 3,
                candidate: who(7),
                uuid: LeaderUuid(9),
            },
            ElectionRecord::Renew {
                term: 3,
                candidate: who(7),
                uuid: LeaderUuid(9),
            },
            ElectionRecord::Resign {
                term: 3,
                candidate: 7,
                successor: Some((who(8), LeaderUuid(11))),
            },
            ElectionRecord::Resign {
                term: 3,
                candidate: 7,
                successor: None,
            },
        ] {
            assert_eq!(ElectionRecord::decode(&record.encode()), Some(Ok(record)));
        }
        assert_eq!(ElectionRecord::decode(b"chain block"), None);
        let mut fold = ElectionFold::new();
        assert_eq!(fold.absorb(0, b"chain block"), Change::Ignored);
        assert_eq!(fold.absorb(1, MAGIC), Change::Ignored);
        assert_eq!(fold.leader(), None);
    }

    #[test]
    fn the_first_campaign_for_the_next_term_wins() {
        let (fold, changes) = fold(&[campaign(1, 1), campaign(1, 2), campaign(3, 2)]);
        assert_eq!(
            changes,
            vec![
                Change::Won { deposed: None },
                Change::Lost { candidate: 2 },
                Change::Lost { candidate: 2 },
            ]
        );
        let leader = fold.leader().expect("a leader");
        assert_eq!((leader.term, leader.candidate.id, leader.since), (1, 1, 0));
    }

    #[test]
    fn a_takeover_deposes_and_a_stale_renewal_is_ignored() {
        let (fold, changes) = fold(&[campaign(1, 1), renew(1, 1), campaign(2, 2), renew(1, 1)]);
        assert_eq!(
            changes,
            vec![
                Change::Won { deposed: None },
                Change::Renewed,
                Change::Won { deposed: Some(1) },
                Change::Ignored,
            ]
        );
        assert_eq!(fold.leader().map(|l| l.candidate.id), Some(2));
    }

    #[test]
    fn a_hand_off_skips_the_campaign_and_a_vacated_term_goes_to_the_next() {
        let hand_off = ElectionRecord::Resign {
            term: 1,
            candidate: 1,
            successor: Some((who(2), LeaderUuid(77))),
        }
        .encode();
        let (fold, changes) = fold(&[campaign(1, 1), hand_off.clone(), hand_off]);
        assert_eq!(changes[1], Change::HandedOff { from: 1 });
        assert_eq!(changes[2], Change::Ignored, "only the leader resigns");
        let leader = fold.leader().expect("the successor leads");
        assert_eq!((leader.term, leader.candidate.id), (2, 2));
        assert_eq!(leader.uuid, LeaderUuid(77));

        let vacate = ElectionRecord::Resign {
            term: 1,
            candidate: 1,
            successor: None,
        }
        .encode();
        let (fold, changes) = self::fold(&[campaign(1, 1), vacate, campaign(2, 3)]);
        assert_eq!(changes[1], Change::Vacated);
        assert_eq!(changes[2], Change::Won { deposed: None });
        assert_eq!(
            fold.leader().map(|l| (l.term, l.candidate.id)),
            Some((2, 3))
        );
    }

    #[test]
    fn a_fold_past_a_gap_anchors_on_a_renewal_and_agrees() {
        // The full history: 1 wins term 1, 2's campaign for term 1 loses, 1
        // renews, 3 takes term 2 over, 1's stale renewal, 3 renews.
        let log = [
            campaign(1, 1),
            campaign(1, 2),
            renew(1, 1),
            campaign(2, 3),
            renew(1, 1),
            renew(2, 3),
        ];
        let (whole, _) = fold(&log);
        // Every floor a truncation could leave: a fold from there agrees
        // once it meets a renewal of the leader.
        for floor in 0..log.len() {
            let mut cut = ElectionFold::new();
            cut.gap(floor as u64);
            for (seq, record) in log.iter().enumerate().skip(floor) {
                cut.absorb(seq as u64, record);
            }
            assert_eq!(
                cut.leader().map(|l| (l.term, l.candidate.id, l.uuid)),
                whole.leader().map(|l| (l.term, l.candidate.id, l.uuid)),
                "a fold from {floor} agrees with the whole fold"
            );
        }
        // A fold that met no anchor yet knows no leader: a resignation or a
        // campaign past the floor anchors nothing.
        let mut cut = ElectionFold::new();
        cut.gap(10);
        let resign = ElectionRecord::Resign {
            term: 2,
            candidate: 3,
            successor: None,
        }
        .encode();
        assert_eq!(cut.absorb(10, &resign), Change::Ignored);
        assert_eq!(cut.absorb(11, &campaign(3, 4)), Change::Ignored);
        assert!(!cut.is_known());
        assert_eq!(cut.leader(), None);
        assert_eq!(cut.term(), 3, "terms never go back past a gap");
    }

    #[test]
    fn a_fold_at_the_tail_anchors_only_at_the_floor_it_holds() {
        // 1 leads term 3 and truncated to its renewal at 4; a reader the
        // floor outran resumes at the tail, 6, where 2's campaign for the
        // taken term 3 lost, then 1 renewed.
        let mut cut = ElectionFold::new();
        cut.resume_at_tail(4, 6);
        assert_eq!(cut.next(), 6);
        assert_eq!(cut.absorb(6, &campaign(3, 2)), Change::Ignored);
        assert!(!cut.is_known(), "a campaign at the tail anchors nothing");
        assert_eq!(cut.absorb(7, &renew(3, 1)), Change::Ignored);
        assert!(!cut.is_known(), "a renewal at the tail anchors nothing");
        assert_eq!(cut.leader(), None);
        // The floor still at 4, below the tail: nothing.
        assert!(!cut.floor_seen(4));
        assert!(!cut.is_known());
        // 1 truncated to its renewal at 7, a record the fold holds: anchored.
        assert!(cut.floor_seen(7));
        assert!(cut.is_known());
        assert_eq!(
            cut.leader().map(|l| (l.term, l.candidate.id, l.since)),
            Some((3, 1, 7))
        );
        assert_eq!(cut.next(), 8);
        // A floor past every record held: the next read starts there, and
        // its record anchors.
        let mut ahead = ElectionFold::new();
        ahead.resume_at_tail(4, 6);
        assert_eq!(ahead.absorb(6, &renew(3, 1)), Change::Ignored);
        assert!(!ahead.floor_seen(8));
        assert_eq!(ahead.next(), 8);
        assert_eq!(ahead.absorb(8, &renew(3, 1)), Change::Adopted);
        // A tail at the floor is the floor: its record anchors.
        let mut at_floor = ElectionFold::new();
        at_floor.resume_at_tail(4, 4);
        assert_eq!(at_floor.absorb(4, &campaign(5, 9)), Change::Adopted);
    }

    #[test]
    fn a_deposed_leaders_late_renewal_at_the_tail_anchors_nothing() {
        // Hunt seed 10124030878967114087 on 1f1f3c6 with PR #679, in small: 1 led term
        // 13 and truncated to its renewal (0); 2 took term 14 over (1); 1,
        // not yet aware, renewed term 13 (2); 3, a fold the floor outran,
        // resumed at the tail (2) and campaigned for term 14 (3); 2 renewed
        // (4) and truncated to it.
        let log = [
            renew(13, 1),
            campaign(14, 2),
            renew(13, 1),
            campaign(14, 3),
            renew(14, 2),
        ];
        let mut whole = ElectionFold::new();
        whole.gap(0);
        for (seq, record) in log.iter().enumerate() {
            whole.absorb(seq as u64, record);
        }
        let leads = |fold: &ElectionFold| fold.leader().map(|l| (l.term, l.candidate.id, l.uuid));
        assert_eq!(leads(&whole).map(|l| (l.0, l.1)), Some((14, 2)));
        // 3's fold: at the tail, 1's late renewal anchors nothing, so 3's
        // own campaign never wins in its own fold.
        let mut tail = ElectionFold::new();
        tail.resume_at_tail(0, 2);
        assert_eq!(tail.absorb(2, &log[2]), Change::Ignored);
        assert_eq!(tail.absorb(3, &log[3]), Change::Ignored);
        assert_eq!(tail.leader(), None);
        assert_eq!(tail.term(), 14, "terms never go back at the tail");
        assert_eq!(tail.absorb(4, &log[4]), Change::Ignored);
        // 2 truncated to its renewal at 4: the fold holds it and agrees.
        assert!(tail.floor_seen(4));
        assert_eq!(leads(&tail), leads(&whole));
        // Or, before campaigning, it rewinds to the floor still at 0, which
        // it does not hold: a read from 0 folds the whole and agrees.
        let mut rewound = ElectionFold::new();
        rewound.resume_at_tail(0, 2);
        assert_eq!(rewound.absorb(2, &log[2]), Change::Ignored);
        assert!(rewound.rewind(0), "the floor's record is read");
        assert_eq!(rewound.next(), 0);
        for (seq, record) in log.iter().enumerate() {
            rewound.absorb(seq as u64, record);
        }
        assert_eq!(leads(&rewound), leads(&whole));
    }

    #[test]
    fn a_fold_at_the_tail_rewinds_to_the_floor_and_anchors() {
        // 1 leads term 4 and truncated to its renewal at 291, then died. A
        // reader the floor outran resumes at the tail, 292, where its own
        // campaign anchors nothing: rewound to the floor, the dead leader's
        // renewal anchors it.
        let mut cut = ElectionFold::new();
        cut.resume_at_tail(291, 292);
        assert_eq!(cut.absorb(292, &campaign(5, 2)), Change::Ignored);
        assert!(cut.rewind(291));
        assert_eq!(cut.next(), 291);
        assert_eq!(cut.absorb(291, &renew(4, 1)), Change::Adopted);
        assert!(cut.is_known());
        assert!(!cut.rewind(291), "a known fold never rewinds");
        // A fold at its floor already does not move, and is still read.
        let mut at_floor = ElectionFold::new();
        at_floor.gap(4);
        assert!(at_floor.rewind(4));
        assert_eq!(at_floor.next(), 4);
        // A fold at the tail that holds the floor's record folds it from
        // memory: no read.
        let mut held = ElectionFold::new();
        held.resume_at_tail(291, 292);
        assert_eq!(held.absorb(292, &renew(4, 1)), Change::Ignored);
        assert_eq!(held.absorb(293, &campaign(5, 2)), Change::Ignored);
        assert!(!held.rewind(292), "anchored from the records held");
        assert!(held.is_known());
        assert_eq!(
            held.leader().map(|l| (l.term, l.candidate.id)),
            Some((5, 2))
        );
        assert_eq!(held.next(), 294);
    }

    #[test]
    fn a_fold_rewound_to_a_still_floor_keeps_the_records_held() {
        // replay-recipe 170029643898500327 40945:14733731119186289879 on
        // ffd7a6e, in small: 1 leads term 1 and truncates every five
        // renewals; a reader the floor outran twice resumed at the tail,
        // 31, held two renewals there, and the floor stayed at 29.
        let mut cut = ElectionFold::new();
        cut.resume_at_tail(29, 31);
        assert_eq!(cut.absorb(31, &renew(1, 1)), Change::Ignored);
        assert_eq!(cut.absorb(32, &renew(1, 1)), Change::Ignored);
        assert_eq!(cut.held(), 2);
        assert!(!cut.floor_seen(29), "the floor stands below the tail");
        // Back to the floor, to read it: the records stay held.
        assert!(cut.rewind(29));
        assert_eq!(cut.next(), 29);
        assert_eq!(cut.held(), 2);
        // The read is answered truncated: the floor moved onto a record
        // held, which anchors, and the fold goes on past what it holds.
        assert!(cut.floor_seen(32));
        assert!(cut.is_known());
        assert_eq!(
            cut.leader().map(|l| (l.term, l.candidate.id, l.since)),
            Some((1, 1, 32))
        );
        assert_eq!(cut.next(), 33);
        assert_eq!(cut.held(), 0);
        // A floor past the records held answers a fold at a floor below
        // them with nothing: the reader resumes where its answer says.
        let mut past = ElectionFold::new();
        past.resume_at_tail(29, 31);
        assert_eq!(past.absorb(31, &renew(1, 1)), Change::Ignored);
        assert!(past.rewind(29));
        assert!(!past.floor_seen(40));
        assert_eq!(past.next(), 29);
        assert_eq!(past.held(), 1);
        // The read from the floor answered a page: the floor's record
        // anchors, and the records held are read again.
        assert_eq!(past.absorb(29, &renew(1, 1)), Change::Adopted);
        assert_eq!(past.held(), 0);
        assert_eq!(past.absorb(30, &renew(1, 1)), Change::Renewed);
        assert_eq!(past.absorb(31, &renew(1, 1)), Change::Renewed);
    }

    #[test]
    fn the_leader_truncating_to_its_own_campaign_leaves_an_anchor() {
        let mut cut = ElectionFold::new();
        cut.gap(4);
        assert_eq!(cut.absorb(4, &campaign(5, 9)), Change::Adopted);
        assert_eq!(cut.leader().map(|l| (l.term, l.since)), Some((5, 4)));
        assert_eq!(cut.absorb(5, &renew(5, 9)), Change::Renewed);
        assert_eq!(cut.leader().map(|l| l.anchor), Some(5));
    }

    #[test]
    fn the_successor_truncating_to_its_hand_off_leaves_an_anchor() {
        let hand_off = ElectionRecord::Resign {
            term: 4,
            candidate: 1,
            successor: Some((who(2), LeaderUuid(502))),
        }
        .encode();
        let log = [campaign(4, 1), renew(4, 1), hand_off];
        let (whole, _) = fold(&log);
        let leader = whole.leader().expect("the successor leads");
        assert_eq!((leader.term, leader.anchor), (5, 2), "its anchor");
        // The successor truncates to its anchor and never renews: a fold
        // from there knows the term, so the next campaign wins.
        let mut cut = ElectionFold::new();
        cut.gap(2);
        assert_eq!(cut.absorb(2, &log[2]), Change::Adopted);
        assert_eq!(
            cut.leader().map(|l| (l.term, l.candidate.id, l.uuid)),
            Some((5, 2, LeaderUuid(502)))
        );
        assert_eq!(
            cut.absorb(3, &campaign(6, 3)),
            Change::Won { deposed: Some(2) }
        );
        // Past the floor, a hand-off anchors nothing.
        let mut late = ElectionFold::new();
        late.gap(1);
        assert_eq!(late.absorb(1, b"chain block"), Change::Ignored);
        assert_eq!(late.absorb(2, &log[2]), Change::Ignored);
        assert!(!late.is_known());
    }
}
