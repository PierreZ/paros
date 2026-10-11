//! paros authorization with Biscuit tokens (#245 Biscuit tokens, #400 keygen
//! and offline tokens).
//!
//! paros is its own issuer. A universe has Ed25519 root key pairs; a token's
//! authority block, signed by one of them, names a role. Any holder derives
//! a narrower token offline, macaroon style, because the key that signs the
//! next block travels inside the token. The verifier of a request adds facts
//! that describe the request and runs one Datalog policy: the meaning of a
//! role lives here, not in the token.
//!
//! Biscuit stays out of `paros` and `paros-core` (its wasm32 clock needs
//! JavaScript's `performance`): `paros` defines the frontend's `Authz`
//! trait, and this crate implements it ([`BiscuitAuthz`], #192 (the
//! frontend)). It is used by `parosctl`, `parosd` and `paros-sim`.
//!
//! **Deterministic first** (#245 rules 1 to 4):
//!
//! - every key comes from an [`Entropy`] the caller draws from its provider's
//!   random source: the OS-seeded thread RNG in production, the seeded RNG
//!   in the simulation; never a fixed or derived seed for a real key;
//! - the time is a fact the caller supplies, never `SystemTime::now()`;
//! - the authorizer's wall-clock budget is out of reach; evaluation is
//!   bounded by fact and iteration counts;
//! - a decision is allow or a refusal judged by its kind; no query result is
//!   read.
//!
//! A human reads and types names, never hex ids (decided on 2026-10-10):
//! tenants and journals by their full string names, root keys by their
//! labels. A key id exists only inside the token format.

mod entropy;
mod error;
mod frontend;
mod keys;
mod operation;
mod token;
mod verify;

pub use entropy::Entropy;
pub use error::Error;
pub use frontend::{BiscuitAuthz, operation, since_epoch};
pub use keys::{KeyRing, RootKey, RootPublicKey};
pub use operation::{Access, Class, Operation};
pub use token::{Grant, Restriction, Role, Token, derive, inspect, mint, seal};
pub use verify::{POLICY, Refusal, Request, TargetKind, authorize};
