//! The **path** of a node of the tree, as in `ZooKeeper`: `/` is the root,
//! and every other path is `/` then names joined by `/`.

use std::borrow::Borrow;
use std::fmt;

/// The most bytes of a path. A path is data that a peer can send, so the
/// store puts a limit on it (AGENTS.md, "never assert on what a peer can
/// send"). **Open**: the value; `ZooKeeper` has no limit of its own on a path,
/// only on a whole request (`jute.maxbuffer`).
pub const MAX_PATH: usize = 1024;

/// The path of a node: `/`, or `/a/b`. Each name is not empty and holds no
/// `/` and no NUL. A path never ends with `/`, except the root.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Path(String);

/// Why a text is not a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathError {
    /// The text does not start with `/`.
    NotAbsolute,
    /// A name is empty: `//`, or a `/` at the end.
    EmptyName,
    /// A name is `.` or `..`.
    Relative,
    /// The text holds a NUL byte.
    Nul,
    /// The text is longer than [`MAX_PATH`].
    TooLong,
}

impl Path {
    /// The root, `/`.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn root() -> Self {
        let root = Self("/".to_owned());
        assert!(root.is_root(), "the root is the root");
        root
    }

    /// The path that `text` names.
    ///
    /// # Errors
    ///
    /// When `text` breaks a rule of [`Path`].
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn new(text: &str) -> Result<Self, PathError> {
        if text.len() > MAX_PATH {
            return Err(PathError::TooLong);
        }
        if text.contains('\0') {
            return Err(PathError::Nul);
        }
        let Some(rest) = text.strip_prefix('/') else {
            return Err(PathError::NotAbsolute);
        };
        if rest.is_empty() {
            return Ok(Self::root());
        }
        for name in rest.split('/') {
            if name.is_empty() {
                return Err(PathError::EmptyName);
            }
            if name == "." || name == ".." {
                return Err(PathError::Relative);
            }
        }
        let path = Self(text.to_owned());
        assert!(!path.is_root(), "a path with a name is not the root");
        assert!(path.parent().is_some(), "a path with a name has a parent");
        Ok(path)
    }

    /// The path as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this is the root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0 == "/"
    }

    /// The parent, or `None` for the root.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        let cut = self.0.rfind('/').expect("a path starts with /");
        let parent = if cut == 0 {
            Self::root()
        } else {
            Self(self.0[..cut].to_owned())
        };
        assert!(parent.0.len() < self.0.len(), "a parent is shorter");
        Some(parent)
    }

    /// The child of this path named `name`.
    ///
    /// # Errors
    ///
    /// When `name` is not one name, or the child is too long.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn child(&self, name: &str) -> Result<Self, PathError> {
        if name.contains('/') || name.is_empty() {
            return Err(PathError::EmptyName);
        }
        let text = if self.is_root() {
            format!("/{name}")
        } else {
            format!("{}/{name}", self.0)
        };
        let child = Self::new(&text)?;
        assert!(
            child.parent().as_ref() == Some(self),
            "a child's parent is the path it was made from"
        );
        Ok(child)
    }

    /// The last name of the path, or the empty text for the root.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn name(&self) -> &str {
        let cut = self.0.rfind('/').expect("a path starts with /");
        &self.0[cut + 1..]
    }

    /// The path of a sequential node created at `prefix` by a record at
    /// position `seq`: the prefix and the position, in twenty digits, so
    /// that the names sort as the positions do (`ZooKeeper`'s sequential
    /// suffix is ten digits of a counter of the parent; the store uses the
    /// position, which is unique).
    ///
    /// # Errors
    ///
    /// When the name is too long.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn sequential(&self, seq: u64) -> Result<Self, PathError> {
        let text = format!("{}{seq:020}", self.0);
        let path = Self::new(&text)?;
        assert!(
            path.0.len() == self.0.len() + 20,
            "the sequential suffix is twenty digits"
        );
        Ok(path)
    }
}

/// A path borrows as its text, so a map of paths can range over the text of
/// a prefix (the children of `/a` are the paths from `/a/`). The order of a
/// path is the order of its text, so the two orders agree.
impl Borrow<str> for Path {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
