//! The **entries** of the store: what a member appends to the journal, one
//! entry in each journal record (`docs/architecture/04-roles.md` §4.4,
//! "The coordination store"; the model is `specs/tla/coord/`).
//!
//! - [`Entry::Open`] opens a session, with a transaction applied in the
//!   same step (`ZooKeeper`'s `multi` at connect, the `open` of the model,
//!   which also joins the latch).
//! - [`Entry::Txn`] is a transaction of a live session, numbered by its
//!   `op_seq`. A transaction with no operation and no check is a
//!   heartbeat (the `hb` of the model).
//! - [`Entry::Expire`] is the compare-and-set expiry of a session on the
//!   position of its last entry (the `expire` of the model).
//! - [`Entry::Checkpoint`] is a copy of the whole state of the fold at the
//!   read position `at` of the member that wrote it (the `ckpt` of the
//!   model).

use paros_journal::Seq;

use crate::path::Path;
use crate::tree::Tree;

/// The id of a session. The program draws it fresh, as it draws a leader
/// uuid (`docs/journal/02-data-plane.md` §J2.3): the store never reuses an
/// id, and a tombstone keeps an expired one out for ever. Zero is not an
/// id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub u64);

impl SessionId {
    /// Whether the id can name a session: it is not zero.
    #[must_use]
    pub fn is_set(self) -> bool {
        self.0 != 0
    }
}

/// The **term** of a turn: the position of the entry at which the holder of
/// the turn changed. One holder for each term, and the term of a parent
/// grows (TLC: `TermStep`, `OneSender` of `specs/tla/coord/`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(pub Seq);

/// One operation of a transaction (`ZooKeeper`'s `create`, `setData` and
/// `delete`, and Curator's `creatingParentsIfNeeded` for one node).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// Create the node `path` with `data`. Its parent exists and is not
    /// ephemeral. An ephemeral node belongs to the session of the entry and
    /// goes with it. A sequential node takes `path` as a prefix and the
    /// position of the entry as its suffix ([`Path::sequential`]).
    Create {
        /// The path, or the prefix of a sequential node.
        path: Path,
        /// The data.
        data: Vec<u8>,
        /// Whether the node goes with its session.
        ephemeral: bool,
        /// Whether the name takes the position of the entry.
        sequential: bool,
    },
    /// Create the persistent node `path` with no data if it does not exist;
    /// do nothing if it does. Its parent exists.
    Ensure {
        /// The path.
        path: Path,
    },
    /// Set the data of `path`, if its version is `version` (any version
    /// when `None`).
    SetData {
        /// The path.
        path: Path,
        /// The data.
        data: Vec<u8>,
        /// The version the node must have.
        version: Option<u64>,
    },
    /// Delete `path`, if its version is `version` (any version when
    /// `None`). The node has no children. Any session may delete any node
    /// (`ZooKeeper`'s rule, and Curator's revocation).
    Delete {
        /// The path.
        path: Path,
        /// The version the node must have.
        version: Option<u64>,
    },
}

/// A check of a transaction: the transaction applies only if every check
/// holds at its position.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Check {
    /// The node `path` exists at `version` (`ZooKeeper`'s `check`).
    Version {
        /// The path.
        path: Path,
        /// The version.
        version: u64,
    },
    /// The session of the entry holds the turn of `parent` at `term`: the
    /// **guard** of a holder ("my session holds the latch at term `t`",
    /// the `act` of the model). A deposed holder can still append; its
    /// guarded entries do nothing (TLC: `ActsByHolder`, control
    /// `no_guard`).
    Holds {
        /// The parent whose turn the session holds.
        parent: Path,
        /// The term of the turn.
        term: Term,
    },
}

/// A transaction: checks, then operations, all or none (`ZooKeeper`'s
/// `multi`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Txn {
    /// The checks.
    pub checks: Vec<Check>,
    /// The operations, in order.
    pub ops: Vec<Op>,
}

impl Txn {
    /// A transaction of `ops` with no check.
    #[must_use]
    pub fn ops(ops: Vec<Op>) -> Self {
        Self {
            checks: Vec::new(),
            ops,
        }
    }

    /// Whether the transaction holds nothing: a heartbeat.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checks.is_empty() && self.ops.is_empty()
    }
}

/// One entry of the store, in one journal record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// Session `session` opens, and `txn` applies in the same step. The
    /// transaction counts as `op_seq` 0 of the session.
    Open {
        /// The session.
        session: SessionId,
        /// The transaction.
        txn: Txn,
    },
    /// Transaction `op_seq` of session `session`. The first is 1, and each
    /// next one is the one after it: a member sends one entry of a session
    /// at a time.
    Txn {
        /// The session.
        session: SessionId,
        /// The number of the entry in the session.
        op_seq: u64,
        /// The transaction.
        txn: Txn,
    },
    /// Expire `session` if its last entry is at `last`.
    Expire {
        /// The session.
        session: SessionId,
        /// The position of its last entry that the sender saw.
        last: Seq,
    },
    /// The whole state of the fold at position `at`: the fold of every
    /// entry below `at`. The fold takes it only when it lands at `at`.
    Checkpoint {
        /// The read position of the member that wrote it.
        at: Seq,
        /// The state.
        tree: Box<Tree>,
    },
}

impl Entry {
    /// The session and the `op_seq` of an entry of a session (`Open` is 0).
    #[must_use]
    pub fn of_session(&self) -> Option<(SessionId, u64)> {
        match self {
            Self::Open { session, .. } => Some((*session, 0)),
            Self::Txn {
                session, op_seq, ..
            } => Some((*session, *op_seq)),
            Self::Expire { .. } | Self::Checkpoint { .. } => None,
        }
    }
}
