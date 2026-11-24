//! Curator's `SharedCount`: a number in the data of one persistent node,
//! changed by a compare-and-set on the node's version.

use crate::entry::{Check, Op, Txn};
use crate::path::Path;
use crate::tree::Tree;

use super::ensure;

/// A shared counter at a path. Its data is the value, eight bytes little
/// endian.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counter {
    path: Path,
}

impl Counter {
    /// The counter at `path`.
    #[must_use]
    pub fn new(path: Path) -> Self {
        Self { path }
    }

    /// The operations that create the counter at zero when it does not
    /// exist. Run them in their own transaction: the set of a counter that
    /// exists fails `NodeExists`.
    #[must_use]
    pub fn create(&self) -> Vec<Op> {
        let mut ops = self.path.parent().map_or_else(Vec::new, |p| ensure(&p));
        ops.push(Op::Create {
            path: self.path.clone(),
            data: 0u64.to_le_bytes().to_vec(),
            ephemeral: false,
            sequential: false,
        });
        ops
    }

    /// The value and the version, or `None` when the node is not there or
    /// its data is not a value.
    #[must_use]
    pub fn get(&self, tree: &Tree) -> Option<(u64, u64)> {
        let node = tree.node(&self.path)?;
        let bytes: [u8; 8] = node.data.as_slice().try_into().ok()?;
        Some((u64::from_le_bytes(bytes), node.version))
    }

    /// The transaction that sets the value to `value` if the version is
    /// still `version` (Curator's `trySetCount`).
    #[must_use]
    pub fn set(&self, value: u64, version: u64) -> Txn {
        Txn {
            checks: vec![Check::Version {
                path: self.path.clone(),
                version,
            }],
            ops: vec![Op::SetData {
                path: self.path.clone(),
                data: value.to_le_bytes().to_vec(),
                version: None,
            }],
        }
    }
}
