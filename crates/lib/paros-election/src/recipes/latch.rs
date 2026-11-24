//! Curator's `LeaderLatch`: each candidate creates an ephemeral sequential
//! child of the latch's parent, and the session of the lowest child holds
//! the latch. The fold keeps the turn of the parent and its term
//! ([`crate::tree::Turn`]).

use crate::entry::{Check, Op, SessionId, Term, Txn};
use crate::path::Path;
use crate::tree::{Tree, Turn};

use super::ensure;

/// The name prefix of a candidate's node.
const PREFIX: &str = "n-";

/// A leader latch at a parent path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Latch {
    parent: Path,
    prefix: Path,
}

impl Latch {
    /// The latch whose candidates are children of `parent`.
    ///
    /// # Panics
    ///
    /// When `parent` is too long to hold a child.
    #[must_use]
    pub fn new(parent: Path) -> Self {
        let prefix = parent
            .child(PREFIX)
            .expect("the latch's parent holds a child");
        Self { parent, prefix }
    }

    /// The parent of the candidates.
    #[must_use]
    pub fn parent(&self) -> &Path {
        &self.parent
    }

    /// The operations that join the latch with `data` (the candidate's
    /// address, for example): the missing parents, then the candidate's
    /// ephemeral sequential node.
    #[must_use]
    pub fn join(&self, data: Vec<u8>) -> Vec<Op> {
        let mut ops = ensure(&self.parent);
        ops.push(Op::Create {
            path: self.prefix.clone(),
            data,
            ephemeral: true,
            sequential: true,
        });
        ops
    }

    /// The operations that leave the latch: delete every node of `session`
    /// under the parent (Curator's `close`, or a `LeaderSelector` that
    /// gives up its turn).
    #[must_use]
    pub fn leave(&self, tree: &Tree, session: SessionId) -> Vec<Op> {
        tree.children(&self.parent)
            .filter(|(_, node)| node.owner == Some(session))
            .map(|(path, _)| Op::Delete {
                path: path.clone(),
                version: None,
            })
            .collect()
    }

    /// Who holds the latch, since which term.
    #[must_use]
    pub fn holder(&self, tree: &Tree) -> Option<Turn> {
        tree.turn(&self.parent)
    }

    /// The candidates in the order they take the latch.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn candidates(&self, tree: &Tree) -> Vec<(SessionId, Vec<u8>)> {
        let mut nodes: Vec<_> = tree
            .children(&self.parent)
            .filter_map(|(path, node)| {
                node.owner
                    .map(|s| ((node.created, path.clone()), s, node.data.clone()))
            })
            .collect();
        nodes.sort_by(|a, b| a.0.cmp(&b.0));
        let order: Vec<_> = nodes.into_iter().map(|(_, s, d)| (s, d)).collect();
        assert!(
            order.first().map(|c| c.0) == self.holder(tree).map(|t| t.holder),
            "the first candidate holds the latch"
        );
        order
    }

    /// The guard of a holder at `term`.
    #[must_use]
    pub fn guard(&self, term: Term) -> Check {
        Check::Holds {
            parent: self.parent.clone(),
            term,
        }
    }

    /// The transaction `ops`, guarded by the latch at `term`: it applies
    /// only while the session holds the latch at that term.
    #[must_use]
    pub fn guarded(&self, term: Term, ops: Vec<Op>) -> Txn {
        Txn {
            checks: vec![self.guard(term)],
            ops,
        }
    }
}
