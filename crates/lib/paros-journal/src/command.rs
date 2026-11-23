//! The commands of a journal and its records (`docs/journal/02-data-plane.md`
//! §J2, §J2.8).
//!
//! A slot of the journal decides one [`Command`]. The journal fold
//! ([`crate::fold`]) judges it at apply, in slot order. These types are the
//! journal's own: the core's `Command` stays as it is for the users of the
//! library (§J4.3), and the journal does not make the core generic over the
//! command.

use paros_consensus::journal::{LeaderUuid, Seq};

use crate::codec::Release;

/// The leader value of a journal (§J2.3, §J2.4.1): who may write.
///
/// The journal state holds one leader value. `SetLeader(new, old)` is a
/// compare-and-set on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Leader {
    /// Nobody leads, and the journal is not multi-writer: every write and
    /// every truncate is refused until a `SetLeader`. A fresh journal starts
    /// here unless its initial configuration says multi-writer.
    Unset,
    /// Multi-writer: no leader. `SetLeader(None, old)` puts the journal here.
    Multi,
    /// Single-writer: the writer that holds this uuid writes.
    Uuid(LeaderUuid),
}

impl Leader {
    /// The single-writer leader `uuid`.
    ///
    /// # Panics
    ///
    /// When `uuid` is the unset uuid, which never leads.
    #[must_use]
    pub fn uuid(uuid: LeaderUuid) -> Self {
        assert!(uuid.is_set(), "the unset uuid never leads");
        let leader = Self::Uuid(uuid);
        assert!(leader.is_valid(), "a leader built from a set uuid is valid");
        leader
    }

    /// The leader that `SetLeader(new, _)` installs: `None` is multi-writer.
    ///
    /// # Panics
    ///
    /// When `new` is the unset uuid. The fold refuses such a `SetLeader`
    /// before it builds a leader from it.
    #[must_use]
    pub fn from_new(new: Option<LeaderUuid>) -> Self {
        let leader = new.map_or(Self::Multi, Self::uuid);
        assert!(leader != Self::Unset, "a SetLeader never installs Unset");
        assert!(leader.is_valid(), "an installed leader is valid");
        leader
    }

    /// Whether the value can be a journal's leader: a uuid leader holds a
    /// set uuid.
    #[must_use]
    pub fn is_valid(self) -> bool {
        match self {
            Self::Unset | Self::Multi => true,
            Self::Uuid(uuid) => uuid.is_set(),
        }
    }
}

/// A **record header** set by the client (§J2.8): an opaque key and value.
/// The journal never reads them, and a read cannot filter on them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Header {
    /// The key, opaque bytes.
    pub key: Vec<u8>,
    /// The value, opaque bytes.
    pub value: Vec<u8>,
}

/// The headers of one record (§J2.8, #1299 (record headers)).
///
/// The headers have two parts. The client part is opaque key and value
/// pairs. The paros part is the **reserved key space**: typed fields that
/// only paros writes, each with its own number in the codec
/// ([`crate::codec`]). In v1 the paros part holds only the leader time. A
/// client key can never collide with a paros field, because the two parts
/// are two sections of the header.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Headers {
    /// The leader time, a hint (§J2.8): it promises no order across a leader
    /// change or a seal and enters no safety rule. The leader stamps it at
    /// propose, one time for each write, from its host's clock, in
    /// nanoseconds ([`Command::stamped`]); a time that the client gives is
    /// replaced.
    pub leader_time: Option<u64>,
    /// The client's headers, in the order the client gave them.
    pub client: Vec<Header>,
}

/// One **record** of the journal: its headers and its payload. A `seq`
/// names one record (§J2.1).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Record {
    /// The headers of the record.
    pub headers: Headers,
    /// The payload, opaque bytes.
    pub payload: Vec<u8>,
}

impl Record {
    /// A record with no headers.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn new(payload: Vec<u8>) -> Self {
        let record = Self {
            headers: Headers::default(),
            payload,
        };
        assert!(
            record.headers.client.is_empty(),
            "a new record has no client header"
        );
        assert!(
            record.headers.leader_time.is_none(),
            "a new record has no leader time"
        );
        record
    }

    /// Whether `other` is the same record as the client wrote it: the same
    /// client headers and the same payload. The paros headers are not part
    /// of it: the leader time of a resend can differ, and the resend is
    /// still the same write (§J2.8).
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn same_client_record(&self, other: &Self) -> bool {
        let same = self.headers.client == other.headers.client && self.payload == other.payload;
        if same {
            assert!(
                self.payload.len() == other.payload.len(),
                "the same record has the same payload size"
            );
        }
        same
    }
}

/// The fence of a write (§J2.2, §J2.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fence {
    /// A single-writer write: fenced by the leader uuid, at the position
    /// `expected_seq`.
    Leader {
        /// The leader uuid the writer writes under.
        uuid: LeaderUuid,
        /// The position the first record of the write asks for.
        expected_seq: Seq,
    },
    /// A multi-writer write: no fence and no position. The journal gives
    /// the position at apply.
    Open,
}

/// A journal **`Write`**: a batch of records, accepted or refused whole, in
/// one slot (§J2.1).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Write {
    /// The fence of the write.
    pub fence: Fence,
    /// The records, in order.
    pub records: Vec<Record>,
}

impl Write {
    /// How many positions the write takes once accepted.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn count(&self) -> u64 {
        let count = self.records.len() as u64;
        assert!(
            usize::try_from(count).is_ok_and(|n| n == self.records.len()),
            "the count of a write is its number of records"
        );
        count
    }

    /// Whether `other` is the same write (§J2.2, #1320 (the wire shape that
    /// resolves `Below`)): the same fence (the same uuid and position) and
    /// the same records as the client wrote them. This is the compare of a
    /// resend at apply and of a `Resolve`.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn same_write(&self, other: &Self) -> bool {
        let same = self.fence == other.fence
            && self.records.len() == other.records.len()
            && self
                .records
                .iter()
                .zip(&other.records)
                .all(|(a, b)| a.same_client_record(b));
        if same {
            assert!(
                self.count() == other.count(),
                "the same write has the same count"
            );
        }
        same
    }
}

impl Command {
    /// The command with the leader time `time` in every record of a write
    /// (§J2.8): one time for each write, stamped by the leader at propose
    /// and carried in the chosen value, so every replica stores the same
    /// time and none stamps at apply. A time that the client gave is
    /// replaced, since the paros headers are the reserved key space. A
    /// command that is not a write holds no record and does not change.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn stamped(self, time: u64) -> Self {
        let Self::Write(mut write) = self else {
            return self;
        };
        let count = write.records.len();
        for record in &mut write.records {
            record.headers.leader_time = Some(time);
        }
        assert_eq!(write.records.len(), count, "a stamp keeps every record");
        assert!(
            write
                .records
                .iter()
                .all(|record| record.headers.leader_time == Some(time)),
            "every record of a write carries the one time of the write"
        );
        Self::Write(write)
    }
}

/// What one slot of a journal decides (§J2).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Command {
    /// A no-op: the election gap fill. It takes a slot and no position.
    Noop,
    /// A `Write`.
    Write(Write),
    /// `Truncate(up_to)` (§J2.6): fenced by `fence` in single-writer mode,
    /// `None` in multi-writer mode.
    Truncate {
        /// The leader uuid the caller truncates under, or `None` for an
        /// unfenced truncate.
        fence: Option<LeaderUuid>,
        /// The first position the caller still needs.
        up_to: Seq,
    },
    /// `SetLeader(new, old)` (§J2.3, §J2.4.1): a compare-and-set on the
    /// leader value. `new = None` makes the journal multi-writer.
    SetLeader {
        /// The leader from the next command on, `None` for multi-writer.
        new: Option<LeaderUuid>,
        /// The leader value the caller believes current.
        old: Leader,
    },
    /// **Reserved** (§J1.1, the upgrade minimum of v1): the command that a
    /// later release uses to move a journal to a new release. The v1 fold
    /// answers it [`crate::fold::Answer::Reserved`] and moves nothing.
    Upgrade {
        /// The release the journal would move to.
        release: Release,
    },
}

impl paros_consensus::types::Weigh for Command {
    /// The payload bytes of a write's records, as the core's command
    /// weighs them; every other command carries no payload and weighs
    /// nothing.
    fn weight(&self) -> u64 {
        match self {
            Self::Write(write) => write
                .records
                .iter()
                .map(|record| u64::try_from(record.payload.len()).expect("a payload fits in u64"))
                .sum(),
            _ => 0,
        }
    }
}

impl paros_consensus::types::Fingerprint for Command {
    /// The FNV-1a hash of the command's bytes ([`crate::codec`]): two equal
    /// commands encode to the same bytes. The core's Phase-2 tally checks a
    /// vote against the command of its round with it.
    fn fingerprint(&self) -> u64 {
        paros_consensus::types::fnv1a(
            paros_consensus::types::FNV_OFFSET,
            &crate::codec::encode_command(self),
        )
    }
}

#[cfg(test)]
mod tests;
