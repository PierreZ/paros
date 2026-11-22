//! A deployment: it uses a role and a foundation.
use crate::acceptor::floor;
use crate::types::Slot;
pub fn run(s: Slot, b: crate::Ballot) { floor(s, b) }
