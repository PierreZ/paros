//! The **storage trait** (§J4.2, rule 4): the one trait that every backend
//! of the journal implements, for every role.
//!
//! It is the storage of the generic host ([`paros_host::Storage`]) for the
//! journal's family: a backend implements `Storage<Journal>`. A backend
//! stores keys and bytes ([`Op`](crate::Op)) and reads a role's records
//! back as one range. The format is not the backend's: [`crate::layout`]
//! builds every key and every value. So one backend serves the acceptor,
//! the replica and later roles, and one contract suite
//! ([`crate::contract::storage`]) tests every backend.
//!
//! The contract, which [`crate::contract::storage`] checks:
//!
//! - **Atomic.** `commit` makes every op of the batch durable, or none of
//!   them: a batch is never torn, even by a crash.
//! - **In order.** The ops of a batch apply in order: a later op at the
//!   same key wins. A commit that returned `Ok` is seen by every later
//!   `scan`.
//! - **Durable.** With `sync`, a batch that returned `Ok` survives a crash.
//!   Without `sync`, it may be lost by a crash, but only together with every
//!   later batch.
//! - **Ranges.** `scan` gives every record of one role, in the order of
//!   [`Key::to_bytes`](crate::Key::to_bytes), and no record of another
//!   role. A record the backend finds corrupt is
//!   [`Entry::Corrupt`](crate::Entry::Corrupt).
//!
//! The error of a storage call ([`StorageError`]) means "unknown": the
//! batch may or may not be durable, and the roles of the batch stop.
//!
//! A contract suite certifies the API only. Crash safety is claimed only on
//! a disk that can inject faults (Pierre, 2026-11-19).

pub use paros_host::{Storage, StorageError};
