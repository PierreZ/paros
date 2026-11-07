//! Rolling wipes (moonpool #344 (rolling wipes), decided on 2026-10-18):
//! the machine group's attrition wipes one machine at a time. Its regime
//! carries a wipe budget of one unhealed wipe, healed by a signal
//! (`crate::chaos_surfaces`). While a wiped machine is unhealed, moonpool
//! turns every later wipe it draws in the group into a plain crash, so two
//! wipes a few seconds apart never take two copies of one quorum.
//!
//! This module sends the signal, and only observes: it reads the facts the
//! audit port reports and calls [`WipeLedger::heal`]. A wiped machine is
//! healed when the cell no longer counts on its old node id: no registry
//! the machines fold holds a booking on it (no acceptor slot, no
//! `Role::Matchmaker` booking, no other slot), and no configuration in
//! force of a plan journal and no matchmaker set in force of a plan tenant
//! names it (`super::in_force::held`). The model is the registry as its
//! most advanced fold holds it, resumed at a restored checkpoint across a
//! truncation (#247).
//!
//! A wiped initial member heals like any machine (#685 (no founders)): the
//! plan journals and the special sets start on the initial members with no
//! booking, so the configurations and sets in force stand in for the
//! bookings there. One rule is about the first configuration itself: while
//! no vote names a plan, an address `init` lists never heals, because the
//! cell counts on its first configuration and the audit knows none of it
//! yet.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{StateHandle, WipeLedger, assert_always, assert_reachable};
use paros::core::journal::JournalIdentifier;
use paros::model::registry::{Registry, RegistryEvent};

use super::{MachineBoard, lock, machine_board};

/// The live bookings of every registry the machines fold, by registry.
#[derive(Default)]
pub(crate) struct CellBookings {
    registries: BTreeMap<JournalIdentifier, Bookings>,
}

/// One registry's live bookings, as its most advanced fold holds them.
#[derive(Default)]
struct Bookings {
    /// The next position the model expects.
    next: u64,
    /// The live bookings, to their node.
    held: BTreeMap<u64, u64>,
}

impl CellBookings {
    /// Whether any registry books a slot on `node`.
    fn holds(&self, node: u64) -> bool {
        self.registries
            .values()
            .any(|model| model.held.values().any(|held| *held == node))
    }

    /// Advance `journal`'s model by `event`, first folded at `seq`. Returns
    /// whether a booking left a node.
    fn folded(&mut self, journal: JournalIdentifier, seq: u64, event: &RegistryEvent) -> bool {
        let model = self.registries.entry(journal).or_default();
        // A position below the model's is folded already. One above it
        // waits for the checkpoint a restoring fold reports
        // (`Self::restored`): a whole fold reports nothing above a gap.
        if seq != model.next {
            return false;
        }
        model.next = seq + 1;
        match event {
            RegistryEvent::Booked { booking, node, .. } => {
                let before = model.held.insert(*booking, node.0);
                assert_always!(
                    before.is_none(),
                    "machine: a cell booking id is live once",
                    { "node" => node.0, "seq" => seq }
                );
                false
            }
            RegistryEvent::Granted { slots, .. } => {
                for slot in slots {
                    model.held.insert(slot.booking, slot.node.0);
                }
                false
            }
            RegistryEvent::Released { booking, node } => {
                let held = model.held.remove(booking);
                assert_always!(
                    held.is_none_or(|held| held == node.0),
                    "machine: a cell booking is released from its own node",
                    { "node" => node.0, "seq" => seq }
                );
                held.is_some()
            }
            RegistryEvent::Retired { id } => {
                let before = model.held.len();
                model.held.retain(|_, node| *node != id.0);
                model.held.len() < before
            }
            _ => false,
        }
    }

    /// A fold restored `journal` from the checkpoint at `seq` (`verified`
    /// is `None`): a truncation may have taken positions the model has not
    /// folded, so the model resumes from `state` when it stands at or
    /// below the checkpoint. A verified checkpoint changes nothing: the
    /// fold that verified it reports every position below it.
    fn restored(
        &mut self,
        journal: JournalIdentifier,
        seq: u64,
        verified: Option<bool>,
        state: &Registry,
    ) -> bool {
        let model = self.registries.entry(journal).or_default();
        if verified.is_some() || seq < model.next {
            return false;
        }
        model.held = state.bookings().map(|(id, b)| (id, b.node.0)).collect();
        model.next = seq + 1;
        true
    }
}

/// The process IPs of the machines no unhealed wipe needs to wait on:
/// every old id a wipe took from the machine unbooked in every registry and
/// not in `held` (the members in force, `super::in_force::held`), and,
/// while no vote names a plan, a machine `init` does not list. A machine
/// with no wipe at all qualifies too: its heal is a no-op, and a wipe of a
/// disk with no record lost nothing.
fn healable(board: &MachineBoard, held: &BTreeSet<u64>) -> Vec<String> {
    let planned = !board.voters.is_empty();
    board
        .ips
        .iter()
        .filter(|(addr, _)| planned || !board.initial_members.contains(*addr))
        .filter(|(addr, _)| {
            board
                .wiped
                .iter()
                .filter(|(wiped, _)| wiped == *addr)
                .all(|(_, old)| !board.bookings.holds(*old) && !held.contains(old))
        })
        .map(|(_, ip)| ip.clone())
        .collect()
}

/// Signal healed every wiped machine the cell holds nothing on (see the
/// module documentation). Called where that can become true: a machine
/// booted on an empty disk (its wipe is recognized there), and a booking
/// left a node. Draws nothing.
pub(crate) fn heal_wiped(state: &StateHandle) {
    // The audit's worlds first, then the board's lock.
    let held = super::in_force::held(state);
    let ips = healable(&lock(&machine_board(state)), &held);
    for ip in ips {
        if WipeLedger::heal(state, &ip) {
            assert_reachable!("machine: a wiped machine heals once the cell holds nothing on it");
            tracing::info!(%ip, "machine_wipe_healed");
        }
    }
}

/// A machine folded `event` at `seq` of the registry `journal`
/// ([`paros::observe::audit::Audit::system_folded`]).
pub(crate) fn registry_folded(
    state: &StateHandle,
    journal: JournalIdentifier,
    seq: u64,
    event: &RegistryEvent,
) {
    let moved = lock(&machine_board(state))
        .bookings
        .folded(journal, seq, event);
    if moved {
        heal_wiped(state);
    }
}

/// A machine folded the checkpoint at `seq` of the registry `journal`
/// ([`paros::observe::audit::Audit::checkpoint_folded`]).
pub(crate) fn registry_restored(
    state: &StateHandle,
    journal: JournalIdentifier,
    seq: u64,
    verified: Option<bool>,
    registry: &Registry,
) {
    let moved = lock(&machine_board(state))
        .bookings
        .restored(journal, seq, verified, registry);
    if moved {
        heal_wiped(state);
    }
}
