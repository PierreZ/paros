//! `paros-wasm`: the site's wasm tools in one crate (#540), one cargo
//! feature per tool.
//!
//! - [`paxos`] (feature `paxos`): the engine of the step-through diagrams
//!   of the site's `/paxos/` pages. It drives `paros-core` by hand and
//!   depends on nothing else. It was the engine of the Paxos game; the
//!   game's web app is gone (#966 (delete the game)).
//! - [`playground`] (feature `playground`): the paros playground, one live
//!   paros universe on moonpool's simulated network, disk and clock, driven
//!   one step at a time. It depends on `paros` and
//!   `moonpool-sim`, default features off. The page is `website/sandbox/`.
//!
//! Each tool is built alone into its own `.wasm` bundle
//! (`scripts/build-wasm.sh`), so a Paxos page never downloads the simulator.
//! Neither tool depends on `paros-sim` or `parosd`, and neither adds a
//! wasm-only protocol path: the code they run is the code paros ships.

#[cfg(feature = "paxos")]
pub mod paxos;
#[cfg(feature = "playground")]
pub mod playground;
