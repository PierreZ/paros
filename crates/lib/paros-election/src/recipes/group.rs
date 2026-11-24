//! Curator's `GroupMember`: each member holds an ephemeral child of the
//! group's parent, with its data; the children are the members.

use crate::entry::{Op, SessionId};
use crate::path::{Path, PathError};
use crate::tree::Tree;

use super::ensure;

/// A group at a parent path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    parent: Path,
}

/// One member of a group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMember {
    /// Its name.
    pub name: String,
    /// Its data.
    pub data: Vec<u8>,
    /// Its session.
    pub session: SessionId,
}

impl Group {
    /// The group whose members are children of `parent`.
    #[must_use]
    pub fn new(parent: Path) -> Self {
        Self { parent }
    }

    /// The operations that join the group as `name` with `data`.
    ///
    /// # Errors
    ///
    /// When `name` is not one name of a path.
    pub fn join(&self, name: &str, data: Vec<u8>) -> Result<Vec<Op>, PathError> {
        let path = self.parent.child(name)?;
        let mut ops = ensure(&self.parent);
        ops.push(Op::Create {
            path,
            data,
            ephemeral: true,
            sequential: false,
        });
        Ok(ops)
    }

    /// The members, in name order. A child that is not ephemeral is not a
    /// member.
    #[must_use]
    pub fn members(&self, tree: &Tree) -> Vec<GroupMember> {
        tree.children(&self.parent)
            .filter_map(|(path, node)| {
                node.owner.map(|session| GroupMember {
                    name: path.name().to_owned(),
                    data: node.data.clone(),
                    session,
                })
            })
            .collect()
    }
}
