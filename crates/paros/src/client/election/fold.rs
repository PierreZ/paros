//! The election's record and its fold (#240, `docs/architecture.md` §3.3):
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
//! there (a `Gap`, [`ElectionFold::gap`]) anchors on that record and agrees
//! with every watcher that folded from the start. Until it meets an anchor,
//! such a watcher knows no leader. The rule assumes nothing else truncates
//! the journal: an election journal is the election's alone.
//!
//! Anything in the journal that is not an election record (no [`MAGIC`]) is
//! ignored.

use paros_core::LeaderUuid;
use prost::Message;

use crate::rpc::election as wire;
use crate::rpc::{leader_uuid_from_proto, leader_uuid_to_proto};

/// The prefix of every election record: a leading zero byte, the name, and a
/// version.
pub const MAGIC: &[u8; 8] = b"\x00PRSELE\x01";

/// One candidate of an election: its id (unique among the candidates, never
/// zero) and its interface — where a request to it goes once it leads (the
/// `InterfaceRef` of §3.3).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Candidate {
    /// Its id.
    pub id: u64,
    /// Its interface (an address).
    pub interface: String,
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

    /// Fold the record at position `seq`.
    ///
    /// # Panics
    ///
    /// If `seq` is not the next position (positions arrive in order: a
    /// programmer error in the reader).
    pub fn absorb(&mut self, seq: u64, record: &[u8]) -> Change {
        assert_eq!(seq, self.next, "an election fold takes positions in order");
        self.next = seq + 1;
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

    /// A fold past a gap: it anchors on a renewal (every renewal describes
    /// the whole leadership), or on a campaign at the very floor (only the
    /// leader truncates, to its own record), and counts nothing else.
    fn anchor(&mut self, seq: u64, record: ElectionRecord) -> Change {
        let at_floor = self.floor == Some(seq);
        match record {
            ElectionRecord::Renew {
                term,
                candidate,
                uuid,
            } if term >= self.term => {
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
    fn the_leader_truncating_to_its_own_campaign_leaves_an_anchor() {
        let mut cut = ElectionFold::new();
        cut.gap(4);
        assert_eq!(cut.absorb(4, &campaign(5, 9)), Change::Adopted);
        assert_eq!(cut.leader().map(|l| (l.term, l.since)), Some((5, 4)));
        assert_eq!(cut.absorb(5, &renew(5, 9)), Change::Renewed);
        assert_eq!(cut.leader().map(|l| l.anchor), Some(5));
    }
}
