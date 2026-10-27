//! The commands of `parosctl`, the paros CLI (#220), and the support every
//! online command shares. The binary (`main.rs`) holds the command tree
//! only; `paros-bench` uses the same support (#722 (three products)).

#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    reason = "the commands of one binary and its bench, not a published API"
)]

pub mod cmd;
pub mod support;
