//! `paros-wasm`: the site's wasm tools in one crate (#540), one cargo
//! feature per tool.
//!
//! - [`paxos`] (feature `paxos`): the engine of the step-through diagrams
//!   of the site's `/paxos/` pages. It drives `paros-consensus` by hand and
//!   depends on nothing else. It was the engine of the Paxos game; the
//!   game's web app is gone (#966 (delete the game)).
//! - [`journal`] (feature `journal`): the fold lab of the site's
//!   `/journal/calls/` page (#1363). It drives `paros-journal` by hand and
//!   depends on nothing else.
//! - The `playground` tool (a live paros universe on moonpool's simulator)
//!   was removed on 2026-11-19 with the journal layers; it is at the tag
//!   `v0.2.0-alpha.1`.
//!
//! Each tool is built alone into its own `.wasm` bundle
//! (`scripts/build-wasm.sh`), and adds no wasm-only protocol path: the code it runs is the code
//! paros ships.

#[cfg(feature = "paxos")]
pub mod paxos;

#[cfg(feature = "journal")]
pub mod journal;
