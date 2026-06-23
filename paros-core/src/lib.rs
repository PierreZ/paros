//! `paros-core` — a sans-IO Multi-Paxos state machine.
//!
//! No I/O, no clock, no randomness, and zero external dependencies (std only).
//! The application drives it: feed events via [`RawNode::step`] and logical time
//! via [`RawNode::tick`], drain a batch of work via [`RawNode::ready`], and
//! acknowledge it via [`Ready::advance`]. The core *describes* the side effects
//! to perform; the caller *performs* them.
//!
//! # The durability contract
//!
//! Each [`Ready`] batch must be processed in order: **persist [`HardState`] →
//! send [`Message`]s (only once the state is durable) → apply committed values →
//! [`Ready::advance`]**. This persist-before-send edge is the heart of Paxos
//! safety; see [`Ready`] and [`HardState`] for the details.
//!
//! # The handshake is type-enforced
//!
//! [`RawNode::ready`] returns a [`Ready`] that holds the node's unique mutable
//! borrow, so calling `ready()` again before [`Ready::advance`] is a *compile*
//! error — not a runtime panic.
//!
//! Stage 0 pins this contract in the type system with **zero protocol logic**.

mod message;
mod node;
mod ready;
mod state;
mod storage;
mod types;

pub use message::Message;
pub use node::RawNode;
pub use ready::Ready;
pub use state::{Config, HardState};
pub use storage::Storage;
pub use types::{Ballot, ClientId, ClientSeq, Command, NodeId, Slot, Value};
