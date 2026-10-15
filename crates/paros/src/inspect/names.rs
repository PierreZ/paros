//! The names an inspected disk holds (#365): the cell's journals this disk
//! keeps a copy of, folded over each store's accepted prefix, answer the
//! administrative cell view ([`crate::view::cell_view`]) the way a founding
//! member answers it online.
//!
//! The fold walks the slots the store holds contiguously from its floor,
//! not only the slots below its chosen index: the chosen index is relaxed
//! (it rides the metainfo writes that happen anyway), so after a clean stop
//! it is usually far behind, often unset. The tail of that prefix may hold
//! a value the cell never chose. A name is a label for people, never
//! evidence: what the cell decided is what its quorum holds, and
//! `parosctl` online asks it.

use std::collections::BTreeMap;
use std::time::Duration;

use paros_core::{Command, Entry, JournalIdentifier, JournalState, NodeId, Seq, Slot, WriterMode};

use super::{DiskImage, StoreImage};
use crate::Address;
use crate::client::checkpoint::{Checkpointable, Folder};
use crate::client::election::ElectionFold;
use crate::fleet::FleetDirectory;
use crate::status::Hysteresis;
use crate::system::Registry;
use crate::tenant::TenantControl;
use crate::view::{CellFacts, Scope, cell_view};
use crate::wire::view::ViewReply;

/// The administrative cell view this disk's copies of the cell's journals
/// answer: its machines, tenants and journals by name. `None` when the disk
/// holds no record of a cell, or no whole copy of the cell's registry.
#[must_use]
pub fn names(image: &DiskImage) -> Option<ViewReply> {
    let Some(Ok(record)) = &image.record else {
        return None;
    };
    let (cell_id, founders, control, election, universe) = if let Some(plan) = record.formed() {
        (
            plan.cell_id,
            plan.members.clone(),
            plan.control,
            Some(plan.election),
            plan.fleet,
        )
    } else {
        let admission = record.admitted.as_ref()?;
        let founders: Vec<(NodeId, Address)> = admission
            .members
            .iter()
            .filter(|(id, _)| admission.founders.contains(id))
            .cloned()
            .collect();
        (
            admission.cell.cell_id,
            founders,
            admission.cell.cell,
            admission.cell.election,
            admission.cell.fleet,
        )
    };
    let stores: BTreeMap<JournalIdentifier, &StoreImage> = image
        .journals
        .iter()
        .filter_map(|j| j.store.as_ref().ok().map(|s| (j.journal, s)))
        .collect();
    let registry = fold(
        stores.get(&control)?,
        Registry::new(founders.iter().map(|(id, _)| *id)),
    )?;
    let directory = universe
        .and_then(|u| stores.get(&u))
        .and_then(|s| fold(s, FleetDirectory::default()))
        .filter(|d| d.fleet().is_some());
    let coordinator = election
        .and_then(|e| stores.get(&e))
        .and_then(|s| elected(s));
    let tenants: Vec<TenantControl> = registry
        .hosted()
        .filter_map(|t| registry.hosted_tenant(t).map(|h| (t, h.control)))
        .filter_map(|(tenant, journal)| {
            let store = stores.get(&JournalIdentifier::new(tenant, journal))?;
            fold(store, TenantControl::new(tenant, journal))
        })
        .collect();
    let facts = CellFacts {
        cell_id,
        answered_by: record.node_id,
        founders: &founders,
        control,
        election: election.unwrap_or(JournalIdentifier::UNSET),
        universe,
        registry: &registry,
        directory: directory.as_ref(),
        tenants: &tenants,
        coordinator,
    };
    // No window: the words are the disk's, judged once, never held back.
    let mut status = Hysteresis::new(Duration::ZERO);
    Some(cell_view(
        &Scope::Admin,
        &facts,
        (&mut status, Duration::ZERO),
    ))
}

/// The cell coordinator and its term, as the accepted prefix of the
/// election journal's copy names them.
fn elected(store: &StoreImage) -> Option<(u64, u64)> {
    let (first, records) = prefix_records(store);
    let mut fold = ElectionFold::new();
    fold.gap(first);
    for (seq, record) in records {
        if seq >= fold.next() {
            fold.gap(seq);
            fold.absorb(seq, &record);
        }
    }
    fold.leader().map(|l| (l.candidate.id, l.term))
}

/// `state` with the accepted prefix of `store` folded in: `None` when the
/// fold is not whole (a truncated prefix with no checkpoint to restore
/// from).
fn fold<S: Checkpointable + Clone>(store: &StoreImage, state: S) -> Option<S> {
    let (first, records) = prefix_records(store);
    let mut folder = Folder::new(state);
    folder.jump(first);
    for (seq, record) in records {
        let _ = folder.fold(seq, &record);
    }
    folder.is_whole().then(|| folder.state().clone())
}

/// The records of the store's accepted prefix, by position, and the first
/// position it holds: the slots from the floor applied in slot order over
/// the sealed state, each write judged as the replica judges it (#204), so
/// a refused write adds no record. The walk stops at the first slot the
/// store does not hold whole (a faulty or a missing one).
fn prefix_records(store: &StoreImage) -> (u64, Vec<(u64, Vec<u8>)>) {
    let mode = store
        .formatted
        .as_ref()
        .map_or(WriterMode::Single, |c| c.writer_mode);
    let mut state: JournalState = store.sealed;
    let first = state.first_seq.0;
    let slots: BTreeMap<Slot, &Command> = store
        .accepted
        .iter()
        .map(|(slot, _, command)| (*slot, command))
        .collect();
    let mut accepted: BTreeMap<u64, Entry> = BTreeMap::new();
    let mut records = Vec::new();
    for slot in store.floor.0.. {
        let Some(command) = slots.get(&Slot(slot)) else {
            break;
        };
        let outcome = state.apply(mode, command, |seq: Seq| accepted.get(&seq.0));
        if let (paros_core::Outcome::Accepted { seq, count }, Command::Write(entry)) =
            (outcome, command)
        {
            assert_eq!(
                count,
                entry.count(),
                "an accepted write occupies one position per record"
            );
            for (at, value) in (seq.0..).zip(&entry.records) {
                records.push((at, value.0.clone()));
            }
            accepted.insert(seq.0, entry.clone());
        }
    }
    // Pair of the apply walk: positions rise one record at a time.
    assert!(
        records.windows(2).all(|w| w[0].0 < w[1].0),
        "the accepted prefix's records rise in position"
    );
    (first, records)
}
