//! `paros-wasm`: the site's wasm tools in one crate (#540), one cargo
//! feature per tool.
//!
//! - [`paxos`] (feature `paxos`): the engine of the step-through diagrams
//!   of the site's `/paxos/` pages. It drives `paros-consensus` by hand and
//!   depends on nothing else. It was the engine of the Paxos game; the
//!   game's web app is gone (#966 (delete the game)).
//! - The `playground` tool (a live paros universe on moonpool's simulator)
//!   was removed on 2026-11-19 with the journal layers; it is at the tag
//!   `v0.2.0-alpha.1`. The `journal` tool (the labs and the live journal of
//!   the Journal pages) was removed on 2026-11-24 with the journal of
//!   design A; it is at the tag `journal-design-a`.
//!
//! Each tool is built alone into its own `.wasm` bundle
//! (`scripts/build-wasm.sh`), and adds no wasm-only protocol path: the code it runs is the code
//! paros ships.

#[cfg(feature = "paxos")]
pub mod steps;

#[cfg(feature = "paxos")]
pub mod paxos;
