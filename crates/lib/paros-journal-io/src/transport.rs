//! The **transport trait**: how the messages of the journal roles move
//! between processes.
//!
//! It is the transport of the generic host ([`paros_host::Transport`]) for
//! the journal's family: a backend implements `Transport<Journal>`. The
//! driver gives every send of the [`Host`](crate::Host) to the transport,
//! a send between two roles of one process too (§J4.2, rule 3): one queue
//! for local and remote delivery, so a test can reorder and drop local
//! messages too, and no role depends on being next to another.
//!
//! The contract, which [`crate::contract::transport`] checks: a message
//! that arrives is the message that was sent, to the node it names. A
//! message may be lost, repeated or reordered: every role takes that. The
//! transport maps each node to the process that serves it, and a process
//! receives only the messages of the nodes it serves.

pub use paros_host::Transport;
