//! The model checkers (test only): each one runs a role against every
//! interleaving a seed draws and checks the role's safety properties. They
//! are not paper concepts, so they live apart from the protocol.
//!
//! - `handover` — the matchmaker-set handover (#125): the reconfigurer and
//!   the matchmakers.
//! - `proxy` — the proxy leader (§3.1, #142).
//! - `support` — the scaffolding both draw their schedules with.

mod handover;
mod proxy;
mod support;
