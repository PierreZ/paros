//! The **fold** of the store: a pure function from the entries, in journal
//! order, to a tree of nodes, a table of sessions and the turn of each
//! parent (`docs/architecture/04-roles.md` §4.4, "The coordination store";
//! the model is `specs/tla/coord/`, operator `Apply`).
//!
//! Every member runs this fold over the same journal, so every member
//! computes the same tree at the same position. The fold judges each entry
//! at its position, and applies it or voids it:
//!
//! - **Exactly-once effects over an at-least-once journal.** The fold keeps,
//!   for each live session, the last `op_seq` it applied: a copy at or below
//!   it does nothing ([`Fate::Duplicate`]; TLC: `OnceEach`, control
//!   `no_dedup`).
//! - **An expired session leaves a tombstone**: a late copy of any of its
//!   entries, its `Open` too, does nothing ([`Fate::Gone`]; TLC:
//!   `OnceEach`, control `no_tomb`; `EphemeralsEnd`, control `no_void`).
//! - **Expiry is a compare-and-set** on the position of the session's last
//!   entry; it deletes the session's ephemeral nodes in the same step
//!   (`ZooKeeper`'s `killSession`; TLC: `ExpireOnSilence`, control
//!   `expire_blind`).
//! - **The turn of a parent** is held by the session that owns the
//!   ephemeral child with the lowest position of create (Curator's
//!   `LeaderLatch` and `InterProcessMutex`). Its **term** is the position
//!   of the entry at which the holder changed (TLC: `TermStep`).
//! - **A guarded transaction** ([`Check::Holds`]) applies only while its
//!   session holds the turn at the term it names (TLC: `ActsByHolder`,
//!   control `no_guard`). A transaction that fails a check or an operation
//!   still counts as done: its `op_seq` is used.
//! - **A checkpoint counts only when it lands at its read position `at`**,
//!   and only when its state is the state of the fold there. A truncate goes
//!   only to a checkpoint that the fold took ([`Tree::checkpoints`]), so the
//!   first record above the floor gives a new member the whole state (TLC:
//!   `ViewTrue`, controls `ckpt_stale` and `trim_any`).
//!
//! An entry that does not decode, or a record that is not an entry of the
//! store, is void ([`Fate::Foreign`]): every member decodes the same bytes
//! the same way, so the fold stays the same on every member.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use paros_journal::Seq;

use crate::entry::{Check, Entry, Op, SessionId, Term, Txn};
use crate::path::Path;

/// A node of the tree (`ZooKeeper`'s `Stat`, in part).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    /// The data.
    pub data: Vec<u8>,
    /// The version of the data: 0 at create, one more at each change.
    pub version: u64,
    /// The position of the entry that created it (`ZooKeeper`'s `czxid`).
    pub created: Seq,
    /// The position of the entry that changed it last (`mzxid`).
    pub modified: Seq,
    /// The session of an ephemeral node (`ephemeralOwner`).
    pub owner: Option<SessionId>,
}

/// The row of a live session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Row {
    /// The position of its `Open`.
    pub opened: Seq,
    /// The position of its last entry that the fold applied (a heartbeat,
    /// a transaction that applied or failed, its `Open`).
    pub last: Seq,
    /// The `op_seq` of that entry.
    pub op_seq: u64,
}

/// The turn of a parent: who holds it, since which term.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Turn {
    /// The session that holds it.
    pub holder: SessionId,
    /// The position at which it took the turn.
    pub term: Term,
}

/// Why a transaction failed: the index of the check or the operation that
/// failed, and the reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TxnError {
    /// Whether a check failed (then `index` is in the checks) or an
    /// operation.
    pub check: bool,
    /// The index of the check or the operation.
    pub index: usize,
    /// The reason.
    pub kind: ErrorKind,
}

/// The reasons of a failed transaction, as `ZooKeeper` names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The node does not exist.
    NoNode,
    /// The node exists.
    NodeExists,
    /// The version is not the one the transaction names.
    BadVersion,
    /// The node has children.
    NotEmpty,
    /// The parent is ephemeral: an ephemeral node has no children.
    EphemeralParent,
    /// The session does not hold the turn at the term.
    NotHolder,
    /// The root cannot be created or deleted.
    Root,
    /// The name of a sequential node is too long.
    BadPath,
}

/// What the fold did with one entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fate {
    /// The session opened, and its transaction gave `result`.
    Opened {
        /// The session.
        session: SessionId,
        /// The result of its transaction.
        result: Result<(), TxnError>,
    },
    /// The transaction applied or failed. Either way its `op_seq` is used.
    Done {
        /// The session.
        session: SessionId,
        /// The number of the entry in the session.
        op_seq: u64,
        /// The result.
        result: Result<(), TxnError>,
    },
    /// A copy of an entry that the fold already applied: nothing.
    Duplicate {
        /// The session.
        session: SessionId,
        /// The number of the entry in the session.
        op_seq: u64,
    },
    /// An entry of a session that is not live (expired, or never opened):
    /// nothing.
    Gone {
        /// The session.
        session: SessionId,
        /// The number of the entry in the session.
        op_seq: u64,
    },
    /// The session expired, and its ephemeral nodes went with it.
    Expired {
        /// The session.
        session: SessionId,
    },
    /// An expiry that does not hold: the session is not live, or it wrote
    /// after `last`.
    ExpireVoid {
        /// The session.
        session: SessionId,
    },
    /// A checkpoint, taken or not.
    Checkpoint {
        /// Whether the fold took it.
        taken: bool,
    },
    /// A record that is not an entry of the store: nothing.
    Foreign,
}

/// A change that one entry made: what a watch sees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// A session opened.
    Opened(SessionId),
    /// A session expired.
    Expired(SessionId),
    /// A node was created.
    Created(Path),
    /// The data of a node changed.
    DataChanged(Path),
    /// A node was deleted.
    Deleted(Path),
    /// The turn of a parent changed: its new holder and term, or nobody.
    Turn(Path, Option<Turn>),
}

/// The fold of the store at one position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tree {
    pub(crate) next: Seq,
    pub(crate) nodes: BTreeMap<Path, Node>,
    pub(crate) sessions: BTreeMap<SessionId, Row>,
    pub(crate) tombs: BTreeSet<SessionId>,
    pub(crate) turns: BTreeMap<Path, Turn>,
    pub(crate) checkpoints: BTreeSet<Seq>,
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

impl Tree {
    /// The fold of an empty journal: the root and nothing else, before
    /// position 0.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn new() -> Self {
        Self::at(Seq(0))
    }

    /// The fold of a journal whose first position is `start` and whose
    /// records below it a truncate cut with nothing to keep: the root
    /// and nothing else. A store whose truncates go only to checkpoints
    /// never needs it; a program that opens a store on a journal that was
    /// written before does.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails.
    #[must_use]
    pub fn at(start: Seq) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            Path::root(),
            Node {
                data: Vec::new(),
                version: 0,
                created: start,
                modified: start,
                owner: None,
            },
        );
        let tree = Self {
            next: start,
            nodes,
            sessions: BTreeMap::new(),
            tombs: BTreeSet::new(),
            turns: BTreeMap::new(),
            checkpoints: BTreeSet::new(),
        };
        tree.assert_invariants();
        tree
    }

    /// The state that a checkpoint at position `seq` gives: the state of
    /// the fold below `seq`, when `entry` is a checkpoint that the fold of
    /// `seq` takes. A member that starts at the floor of the journal reads
    /// the record there, takes its state, and applies the record. Any other
    /// record at the floor gives nothing: the store's precondition (only the
    /// store truncates its journal) is broken.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn from_checkpoint(seq: Seq, entry: Option<&Entry>) -> Option<Self> {
        let Some(Entry::Checkpoint { at, tree }) = entry else {
            return None;
        };
        if *at != seq || tree.next != seq || !tree.is_consistent() {
            return None;
        }
        let tree = (**tree).clone();
        tree.assert_invariants();
        assert!(tree.next == seq, "a checkpoint state stops at its position");
        Some(tree)
    }

    /// The next position the fold applies.
    #[must_use]
    pub fn next(&self) -> Seq {
        self.next
    }

    /// The node at `path`.
    #[must_use]
    pub fn node(&self, path: &Path) -> Option<&Node> {
        self.nodes.get(path)
    }

    /// Every node, in path order.
    pub fn nodes(&self) -> impl Iterator<Item = (&Path, &Node)> {
        self.nodes.iter()
    }

    /// The children of `parent`, in path order.
    pub fn children<'a>(&'a self, parent: &'a Path) -> impl Iterator<Item = (&'a Path, &'a Node)> {
        let prefix = if parent.is_root() {
            "/".to_owned()
        } else {
            format!("{parent}/")
        };
        let start = prefix.clone();
        self.nodes
            .range::<str, _>((Bound::Included(start.as_str()), Bound::Unbounded))
            .take_while(move |(p, _)| p.as_str().starts_with(&prefix))
            .filter(move |(p, _)| p.parent().as_ref() == Some(parent))
    }

    /// The row of `session`, when it is live.
    #[must_use]
    pub fn session(&self, session: SessionId) -> Option<&Row> {
        self.sessions.get(&session)
    }

    /// Every live session, in id order.
    pub fn sessions(&self) -> impl Iterator<Item = (SessionId, &Row)> {
        self.sessions.iter().map(|(s, r)| (*s, r))
    }

    /// Whether `session` expired: the fold keeps its tombstone for ever.
    #[must_use]
    pub fn expired(&self, session: SessionId) -> bool {
        self.tombs.contains(&session)
    }

    /// The turn of `parent`.
    #[must_use]
    pub fn turn(&self, parent: &Path) -> Option<Turn> {
        self.turns.get(parent).copied()
    }

    /// The positions of the checkpoints that the fold took: the only
    /// positions a truncate of the store may go to.
    #[must_use]
    pub fn checkpoints(&self) -> &BTreeSet<Seq> {
        &self.checkpoints
    }

    /// Apply the record at position `seq`: `entry` is its entry, or `None`
    /// when the record is not an entry of the store.
    ///
    /// # Panics
    ///
    /// When `seq` is not [`Tree::next`]: the fold applies the journal in
    /// order, with no gap. That is a bug of the caller.
    pub fn apply(&mut self, seq: Seq, entry: Option<&Entry>) -> (Fate, Vec<Change>) {
        assert!(seq == self.next, "the fold applies the journal in order");
        let mut changes = Vec::new();
        let fate = match entry {
            None => Fate::Foreign,
            Some(Entry::Open { session, txn }) => self.open(seq, *session, txn, &mut changes),
            Some(Entry::Txn {
                session,
                op_seq,
                txn,
            }) => self.txn(seq, *session, *op_seq, txn, &mut changes),
            Some(Entry::Expire { session, last }) => self.expire(*session, *last, &mut changes),
            Some(Entry::Checkpoint { at, tree }) => {
                let taken = *at == seq && **tree == *self;
                if taken {
                    self.checkpoints.insert(seq);
                }
                Fate::Checkpoint { taken }
            }
        };
        self.next = Seq(seq.0 + 1);
        self.rehold(seq, &mut changes);
        assert!(self.next.0 == seq.0 + 1, "the fold moved one position");
        assert!(
            self.turns.values().all(|t| t.term.0 <= seq),
            "no term is above the entry that made it"
        );
        (fate, changes)
    }

    fn open(&mut self, seq: Seq, session: SessionId, txn: &Txn, changes: &mut Vec<Change>) -> Fate {
        if self.sessions.contains_key(&session) {
            return Fate::Duplicate { session, op_seq: 0 };
        }
        if self.tombs.contains(&session) || !session.is_set() {
            return Fate::Gone { session, op_seq: 0 };
        }
        self.sessions.insert(
            session,
            Row {
                opened: seq,
                last: seq,
                op_seq: 0,
            },
        );
        changes.push(Change::Opened(session));
        let result = self.run(seq, session, txn, changes);
        assert!(self.sessions.contains_key(&session), "the session is live");
        Fate::Opened { session, result }
    }

    fn txn(
        &mut self,
        seq: Seq,
        session: SessionId,
        op_seq: u64,
        txn: &Txn,
        changes: &mut Vec<Change>,
    ) -> Fate {
        let Some(row) = self.sessions.get_mut(&session) else {
            return Fate::Gone { session, op_seq };
        };
        if op_seq <= row.op_seq {
            return Fate::Duplicate { session, op_seq };
        }
        row.last = seq;
        row.op_seq = op_seq;
        let result = self.run(seq, session, txn, changes);
        let row = self.sessions[&session];
        assert!(row.op_seq == op_seq, "the transaction used its op_seq");
        assert!(row.last == seq, "the transaction renewed its session");
        Fate::Done {
            session,
            op_seq,
            result,
        }
    }

    fn expire(&mut self, session: SessionId, last: Seq, changes: &mut Vec<Change>) -> Fate {
        match self.sessions.get(&session) {
            Some(row) if row.last == last => {}
            _ => return Fate::ExpireVoid { session },
        }
        self.sessions.remove(&session);
        self.tombs.insert(session);
        let owned: Vec<Path> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.owner == Some(session))
            .map(|(p, _)| p.clone())
            .collect();
        for path in owned {
            self.nodes.remove(&path);
            changes.push(Change::Deleted(path));
        }
        changes.push(Change::Expired(session));
        assert!(
            self.nodes.values().all(|n| n.owner != Some(session)),
            "no node of an expired session is left"
        );
        Fate::Expired { session }
    }

    /// Run `txn` of `session` at `seq`: every check, then every operation
    /// on an overlay, then the overlay into the tree. All or none.
    fn run(
        &mut self,
        seq: Seq,
        session: SessionId,
        txn: &Txn,
        changes: &mut Vec<Change>,
    ) -> Result<(), TxnError> {
        for (index, check) in txn.checks.iter().enumerate() {
            let held = match check {
                Check::Version { path, version } => match self.nodes.get(path) {
                    None => Err(ErrorKind::NoNode),
                    Some(n) if n.version != *version => Err(ErrorKind::BadVersion),
                    Some(_) => Ok(()),
                },
                Check::Holds { parent, term } => {
                    let turn = self.turns.get(parent);
                    if turn.is_some_and(|t| t.holder == session && t.term == *term) {
                        Ok(())
                    } else {
                        Err(ErrorKind::NotHolder)
                    }
                }
            };
            held.map_err(|kind| TxnError {
                check: true,
                index,
                kind,
            })?;
        }
        let mut overlay = Overlay {
            base: &self.nodes,
            edits: BTreeMap::new(),
        };
        let mut done = Vec::new();
        for (index, op) in txn.ops.iter().enumerate() {
            let change = overlay.op(seq, session, op).map_err(|kind| TxnError {
                check: false,
                index,
                kind,
            })?;
            if let Some(change) = change {
                done.push(change);
            }
        }
        let edits = overlay.edits;
        for (path, node) in edits {
            match node {
                Some(node) => {
                    self.nodes.insert(path, node);
                }
                None => {
                    self.nodes.remove(&path);
                }
            }
        }
        changes.extend(done);
        assert!(self.nodes.contains_key(&Path::root()), "the root stays");
        Ok(())
    }

    /// The turns of the parents that `changes` touched, again.
    fn rehold(&mut self, seq: Seq, changes: &mut Vec<Change>) {
        let parents: BTreeSet<Path> = changes
            .iter()
            .filter_map(|c| match c {
                Change::Created(p) | Change::Deleted(p) => p.parent(),
                _ => None,
            })
            .collect();
        for parent in parents {
            let holder = self.holder_of(&parent);
            let old = self.turns.get(&parent).copied();
            match (old, holder) {
                (Some(t), Some(h)) if t.holder == h => {}
                (None, None) => {}
                (_, Some(holder)) => {
                    let turn = Turn {
                        holder,
                        term: Term(seq),
                    };
                    assert!(
                        old.is_none_or(|t| t.term.0 < seq),
                        "the term of a parent grows"
                    );
                    self.turns.insert(parent.clone(), turn);
                    changes.push(Change::Turn(parent, Some(turn)));
                }
                (Some(_), None) => {
                    self.turns.remove(&parent);
                    changes.push(Change::Turn(parent, None));
                }
            }
        }
    }

    /// The session that owns the ephemeral child of `parent` with the
    /// lowest position of create (the path breaks a tie: two creates of one
    /// transaction).
    fn holder_of(&self, parent: &Path) -> Option<SessionId> {
        self.children(parent)
            .filter_map(|(p, n)| n.owner.map(|s| ((n.created, p), s)))
            .min_by(|a, b| a.0.cmp(&b.0))
            .map(|(_, s)| s)
    }

    /// Whether the state keeps every rule of the fold: the check of a
    /// decoded checkpoint, which a peer wrote.
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        let root = self.nodes.get(&Path::root());
        if root.is_none_or(|r| r.owner.is_some()) {
            return false;
        }
        for (path, node) in &self.nodes {
            if node.created >= self.next && !path.is_root() {
                return false;
            }
            if node.modified < node.created || (node.modified >= self.next && !path.is_root()) {
                return false;
            }
            if let Some(owner) = node.owner
                && !self.sessions.contains_key(&owner)
            {
                return false;
            }
            if let Some(parent) = path.parent() {
                match self.nodes.get(&parent) {
                    Some(p) if p.owner.is_none() => {}
                    _ => return false,
                }
            }
        }
        for (session, row) in &self.sessions {
            if !session.is_set() || self.tombs.contains(session) {
                return false;
            }
            if row.opened > row.last || row.last >= self.next {
                return false;
            }
        }
        if self.checkpoints.iter().any(|c| *c >= self.next) {
            return false;
        }
        let mut parents: BTreeSet<Path> = self.turns.keys().cloned().collect();
        parents.extend(
            self.nodes
                .iter()
                .filter(|(_, n)| n.owner.is_some())
                .filter_map(|(p, _)| p.parent()),
        );
        parents.iter().all(|parent| {
            let turn = self.turns.get(parent);
            let holder = self.holder_of(parent);
            turn.map(|t| t.holder) == holder && turn.is_none_or(|t| t.term.0 < self.next)
        })
    }

    /// Check every rule of the fold. It walks the whole state: the walks
    /// call it after each step, and the fold calls it on a checkpoint.
    ///
    /// # Panics
    ///
    /// When a rule does not hold: a bug of the fold.
    pub fn assert_invariants(&self) {
        assert!(self.is_consistent(), "the fold keeps its rules: {self:?}");
        assert!(
            self.nodes.contains_key(&Path::root()),
            "the root is in every tree"
        );
    }
}

/// The edits of one transaction over the tree: a node, or `None` for a
/// deleted one.
struct Overlay<'a> {
    base: &'a BTreeMap<Path, Node>,
    edits: BTreeMap<Path, Option<Node>>,
}

impl Overlay<'_> {
    fn get(&self, path: &Path) -> Option<&Node> {
        match self.edits.get(path) {
            Some(edit) => edit.as_ref(),
            None => self.base.get(path),
        }
    }

    fn has_children(&self, path: &Path) -> bool {
        let prefix = if path.is_root() {
            "/".to_owned()
        } else {
            format!("{path}/")
        };
        let below = |p: &Path| p.as_str().starts_with(&prefix) && p != path;
        let based = self
            .base
            .range::<str, _>((Bound::Included(prefix.as_str()), Bound::Unbounded))
            .take_while(|(p, _)| p.as_str().starts_with(&prefix))
            .any(|(p, _)| below(p) && !matches!(self.edits.get(p), Some(None)));
        based || self.edits.iter().any(|(p, n)| n.is_some() && below(p))
    }

    fn op(&mut self, seq: Seq, session: SessionId, op: &Op) -> Result<Option<Change>, ErrorKind> {
        match op {
            Op::Create {
                path,
                data,
                ephemeral,
                sequential,
            } => {
                let path = if *sequential {
                    path.sequential(seq.0).map_err(|_| ErrorKind::BadPath)?
                } else {
                    path.clone()
                };
                let parent = path.parent().ok_or(ErrorKind::Root)?;
                match self.get(&parent) {
                    None => return Err(ErrorKind::NoNode),
                    Some(p) if p.owner.is_some() => return Err(ErrorKind::EphemeralParent),
                    Some(_) => {}
                }
                if self.get(&path).is_some() {
                    return Err(ErrorKind::NodeExists);
                }
                let node = Node {
                    data: data.clone(),
                    version: 0,
                    created: seq,
                    modified: seq,
                    owner: ephemeral.then_some(session),
                };
                self.edits.insert(path.clone(), Some(node));
                Ok(Some(Change::Created(path)))
            }
            Op::Ensure { path } => {
                if self.get(path).is_some() {
                    return Ok(None);
                }
                let create = Op::Create {
                    path: path.clone(),
                    data: Vec::new(),
                    ephemeral: false,
                    sequential: false,
                };
                self.op(seq, session, &create)
            }
            Op::SetData {
                path,
                data,
                version,
            } => {
                let node = self.get(path).ok_or(ErrorKind::NoNode)?;
                if version.is_some_and(|v| v != node.version) {
                    return Err(ErrorKind::BadVersion);
                }
                let mut node = node.clone();
                node.data.clone_from(data);
                node.version += 1;
                node.modified = seq;
                self.edits.insert(path.clone(), Some(node));
                Ok(Some(Change::DataChanged(path.clone())))
            }
            Op::Delete { path, version } => {
                if path.is_root() {
                    return Err(ErrorKind::Root);
                }
                let node = self.get(path).ok_or(ErrorKind::NoNode)?;
                if version.is_some_and(|v| v != node.version) {
                    return Err(ErrorKind::BadVersion);
                }
                if self.has_children(path) {
                    return Err(ErrorKind::NotEmpty);
                }
                self.edits.insert(path.clone(), None);
                Ok(Some(Change::Deleted(path.clone())))
            }
        }
    }
}

#[cfg(test)]
mod tests;
