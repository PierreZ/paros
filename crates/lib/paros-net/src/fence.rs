//! The **cell fence** (#216, #539 (matchmaker cell fence)).
//!
//! A machine steps no peer batch of another cell, and its matchmaker host
//! answers no request of another cell: a machine at an address another cell
//! left, or a bad configuration, never reaches it. A matchmaker never
//! provisions a set for another cell's request (the hazard #539 closed).

/// A machine's cell fence: the cell whose messages it admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellFence {
    own: u64,
}

impl CellFence {
    /// The fence of cell `own` (`0` on a deployment without a cell).
    #[must_use]
    pub fn new(own: u64) -> Self {
        Self { own }
    }

    /// The cell this fence keeps.
    #[must_use]
    pub fn own(&self) -> u64 {
        self.own
    }

    /// Whether a peer message or a matchmaker request of cell `cell` passes:
    /// only one of this cell does.
    #[must_use]
    pub fn admits(&self, cell: u64) -> bool {
        cell == self.own
    }
}

impl From<u64> for CellFence {
    fn from(own: u64) -> Self {
        Self::new(own)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fence_admits_only_its_own_cell() {
        let fence = CellFence::new(7);
        assert_eq!(fence.own(), 7);
        assert!(fence.admits(7));
        assert!(!fence.admits(8));
        assert!(!fence.admits(0));
        assert_eq!(CellFence::from(7), fence);
    }
}
