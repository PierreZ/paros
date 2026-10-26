//! The coordinator layer: the three coordinators of
//! `docs/architecture/04-roles.md`. The cell coordinator ([`cell`]) serves a
//! term over the cell control journal; the tenant coordinator ([`tenant`])
//! books each tenant's sets, moves, modes, roles and scrubs; the universe
//! coordinator ([`universe`]) joins cells and steps tenants across them.
//! Each is a client of the cell: it sits above [`crate::client`] and below
//! [`crate::machine`].

pub mod cell;
pub mod tenant;
pub mod universe;
