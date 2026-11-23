//! A role: it uses a foundation, by module and by a re-export of the root.
use crate::types::Slot;
use crate::Ballot;
// A comment that names crate::node is not a use.
pub fn floor(_: Slot, _: Ballot) {}
