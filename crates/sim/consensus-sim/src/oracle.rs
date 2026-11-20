//! The judges of a walk. Each one reads only what the driver saw (a write
//! that reached a disk, a message that left a party, a slot a party
//! applied, a read it answered) and shares no code with the core it
//! judges (#909 (oracles that do not judge)).

pub mod journal;
pub mod ledger;
pub mod quorum;
pub mod reads;
