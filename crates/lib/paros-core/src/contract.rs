//! The **contract** between the core and its driver: the messages it sends,
//! the writes it asks for, the storage it boots from, its durable scalars and
//! its static configuration. Every deployment speaks it.

pub mod config;
pub mod hard_state;
pub mod limits;
pub mod mem_storage;
pub mod message;
pub mod storage;
pub mod write;

pub use config::Config;
pub use hard_state::HardState;
pub use limits::Limits;
pub use mem_storage::MemStorage;
pub use message::{Audience, Message, Party};
pub use storage::Storage;
pub use write::{AcceptorWrite, MustSync, WriteOp};
