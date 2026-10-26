//! The model layer: the pure folds and the types every layer above shares.
//! No I/O, no client, no machine: it imports only `paros_core` and the
//! generated wire messages, and it builds for wasm32.

pub mod address;
pub mod authz;
pub mod checkpoint;
pub mod corruption;
pub mod fleet;
pub mod footprint;
pub mod load;
pub mod machine;
pub mod name;
pub mod placement;
pub mod registry;
pub mod status;
pub mod tags;
pub mod tenant;
pub mod view;
