//! The replaced-founder scenario's operator steps (#423,
//! `crate::shape::replaced_founder`): once `crate::world::replaced_founder`
//! wiped a founder of the formed cell, client 0 admits the machine that
//! replaced it with `cell add-machine`, then runs `init` again over the
//! founders. The re-run meets formed founders and an admitted machine at a
//! listed address, and must find the cell.

use moonpool_sim::{SimContext, assert_reachable};
use paros::Address;

use super::FleetOps;
use crate::machine::Replacement;

/// How many `ADMIT`s the scenario forces at the replacement: an admission
/// the chaos interrupts is tried again, but the operator's other steps go on.
const MAX_ADMITS: u8 = 8;

/// The scenario's steps one operator took.
#[derive(Debug, Default)]
pub(super) struct Steps {
    /// The `ADMIT`s forced at the replacement.
    admits: u8,
    /// The re-run `init` was forced.
    reran: bool,
    /// The next `FLEET_INIT` runs `init` again, even on a cell this
    /// operator knows.
    pub(super) rerun_next: bool,
}

impl FleetOps {
    /// The operation the scenario forces on this operator next, if any:
    /// `ADMIT` while the replacement is idle, then `FLEET_INIT` once it is
    /// admitted. Client 0 only, and never in the middle of an operation.
    pub(in crate::chain_workload) fn replaced_founder_op(
        &mut self,
        ctx: &SimContext,
    ) -> Option<u8> {
        if self.client_id != 0
            || self.pending.is_some()
            || !crate::shape::replaced_founder(ctx.state())
        {
            return None;
        }
        match crate::machine::replacement(ctx.state())? {
            Replacement::Idle(_) if self.replaced.admits < MAX_ADMITS => {
                self.replaced.admits += 1;
                Some(super::super::ADMIT)
            }
            Replacement::Admitted if !self.replaced.reran => {
                assert_reachable!(
                    "init: an operator runs init again after admitting a founder's replacement"
                );
                self.replaced.reran = true;
                self.replaced.rerun_next = true;
                Some(super::super::FLEET_INIT)
            }
            _ => None,
        }
    }

    /// The machine an `ADMIT` aims at on a replaced-founder seed: the idle
    /// machine at the wiped founder's address. `None` on any other seed.
    pub(super) fn replaced_target(&self, ctx: &SimContext) -> Option<Address> {
        if self.client_id != 0 || !crate::shape::replaced_founder(ctx.state()) {
            return None;
        }
        match crate::machine::replacement(ctx.state())? {
            Replacement::Idle(addr) => {
                assert_reachable!("admit: an operator admits a founder's replacement");
                Some(addr)
            }
            Replacement::Admitted => None,
        }
    }
}
