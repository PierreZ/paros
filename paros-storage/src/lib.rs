//! `paros-storage` — concrete implementations of [`paros_core::Storage`].
//!
//! The core defines the read-only [`paros_core::Storage`] port; this crate owns
//! the *writers*. The seeded faulty in-memory fake (fail-stop, corruption,
//! protocol-aware recovery) lands with the storage-fault milestone (Stage 4+).
//! Stage 0 is an empty scaffold.
