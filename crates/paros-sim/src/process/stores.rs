//! The acceptor's journal stores on the simulated disk (#188), and their provisioning.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::SimStorageProvider;
use moonpool_sim::{SimContext, SimTimeProvider, assert_reachable};

use super::Down;
use super::acceptor::Seat;
use super::stay_down;
use crate::audit::{NodeAudit, audit_world_for};
use crate::world::node_store::LedgeredJournal;
use crate::world::{ParkReason, StorageWorld};
use paros::{BootKind, Config, JournalStorage, JournalStoreConfig, JournalStores};

/// The acceptor's journal stores (#188): each journal's `JournalStorage` on
/// the node's simulated disk, opened as a process restart finds it, with the
/// operator's boot claim read off the journal's provisioning ledger (#147).
/// A journal down for good on this node — retired, or parked by a detected
/// corruption — is declined, and its audit is told it stays down.
pub(super) struct SimStores<'a> {
    pub(super) ctx: &'a SimContext,
    pub(super) seats: &'a mut Vec<Seat>,
    pub(super) ip: &'a str,
    pub(super) rank: u64,
    /// The simulated disk and the run's journal layout (#187, #261).
    pub(super) journal_store: (SimStorageProvider, JournalStoreConfig),
    /// The system board, on a seed that runs the system journals (#189):
    /// a spare's journal gets a seat here at runtime.
    pub(super) system: Option<Arc<Mutex<crate::audit::system::SystemBoard>>>,
}

/// The directory a journal's store lives in on a node's simulated disk
/// (#187, #188: one directory per journal, by its identifier, #235).
pub(super) fn journal_dir(journal: paros::JournalIdentifier) -> String {
    format!("paros/journals/{}/{}", journal.tenant.0, journal.journal.0)
}

/// Resolve an interrupted provisioning before a boot (#187): a journal
/// store's format marker lands only with the sync after the format, and a
/// process killed in between leaves the operator's ledger saying "begun"
/// and nothing else. The operator does what an operator would: looks at the
/// disk — made durable as it stands first (#348) — a store that carries
/// the marker was provisioned, one that does not was not, and its next boot
/// is a first boot again.
#[tracing::instrument(level = "debug", skip_all, fields(ip = %ip))]
pub(super) async fn resolve_provisioning(ctx: &SimContext, seats: &[Seat], ip: &str) {
    for seat in seats {
        resolve_seat_provisioning(ctx, seat, ip).await;
    }
}

/// [`resolve_provisioning`] for one journal: run at the top of every
/// incarnation and again at every open, the re-open of a quarantined journal
/// included (a format whose sync failed may have landed anyway).
async fn resolve_seat_provisioning(ctx: &SimContext, seat: &Seat, ip: &str) {
    resolve_journal_provisioning(ctx, &seat.world, seat.journal, ip).await;
}

/// [`resolve_seat_provisioning`] for `journal`'s store on `ip`'s disk, its
/// ledger in `world`: a seat's, or a replica's (#261).
pub(super) async fn resolve_journal_provisioning(
    ctx: &SimContext,
    world: &Mutex<StorageWorld>,
    journal: paros::JournalIdentifier,
    ip: &str,
) {
    let ambiguous = world
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .provisioning_ambiguous(ip);
    if !ambiguous {
        return;
    }
    // Make the store durable as it stands first (#348): after a failed
    // format sync in this process, a readable marker is only staged.
    let dir = journal_dir(journal);
    if !crate::world::settle::settle_store(ctx.storage(), &dir).await {
        return;
    }
    // Read the marker without opening the store: a probe that recovered and
    // repaired the journal would change what the boot it decides then finds.
    let formatted = JournalStorage::peek_formatted(ctx.storage(), &dir, journal)
        .await
        .unwrap_or(false);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    if formatted {
        guard.note_provisioned(ip);
    } else {
        guard.abandon_provisioning(ip);
    }
    assert_reachable!("journal store: an interrupted provisioning is resolved from the disk");
}

impl SimStores<'_> {
    fn seat(&self, journal: paros::JournalIdentifier) -> Option<&Seat> {
        self.seats.iter().find(|seat| seat.journal == journal)
    }

    /// A port on `journal`'s audit world and no journal's board — for a
    /// journal this node holds no seat for, and for the node's own facts.
    fn world_audit(&self, journal: paros::JournalIdentifier) -> NodeAudit<SimTimeProvider> {
        let audit = NodeAudit::new(
            self.ctx.time().clone(),
            audit_world_for(self.ctx.state(), journal),
        );
        match &self.system {
            Some(board) => audit.with_system(board.clone()),
            None => audit,
        }
    }
}

impl JournalStores for SimStores<'_> {
    type Store = LedgeredJournal;
    type Audit = NodeAudit<SimTimeProvider>;

    fn journals(&self) -> Vec<paros::JournalIdentifier> {
        self.seats
            .iter()
            .filter(|seat| !seat.created)
            .map(|seat| seat.journal)
            .collect()
    }

    async fn open(&mut self, journal: paros::JournalIdentifier) -> Option<(Self::Store, BootKind)> {
        let seat = self.seat(journal).filter(|seat| !seat.deleted)?;
        resolve_seat_provisioning(self.ctx, seat, self.ip).await;
        let (parked, boot) = {
            let guard = seat.world.lock().unwrap_or_else(PoisonError::into_inner);
            (
                guard.park_reason(self.ip),
                // The operator's claim (#147): an identity the world's
                // provisioning ledger knows is an existing member — a wiped
                // one included, which is the whole point — and any other is
                // a first boot the driver formats.
                if guard.provisioned(self.ip) {
                    BootKind::ExistingMember
                } else {
                    BootKind::FirstBoot
                },
            )
        };
        match parked {
            Some(ParkReason::Retired) => {
                stay_down(&seat.checker, Down::Retired(self.rank));
                return None;
            }
            Some(ParkReason::Corruption) => {
                stay_down(&seat.checker, Down::StorageParked(self.rank));
                return None;
            }
            Some(ParkReason::Wiped) | None => {}
        }
        let (provider, layout) = &self.journal_store;
        // A system or created journal (#189) is quiet: no injected damage
        // (a zero chaos window), and two syncs per commit, so a crash, a
        // cut mid-commit included, never leaves its last batch ambiguous (a
        // one-member journal could never repair it) and spends no budget.
        let (layout, cutoff) = if seat.quiet {
            (
                JournalStoreConfig {
                    durability: paros::journal::Durability::Ordered,
                    ..*layout
                },
                Duration::ZERO,
            )
        } else {
            (*layout, crate::CHAOS_DURATION)
        };
        let journal = JournalStorage::new(
            provider.clone(),
            journal_dir(journal),
            seat.config.clone(),
            layout,
        );
        let store = LedgeredJournal::new(
            journal,
            Arc::downgrade(&seat.world),
            self.ip.to_string(),
            (self.ctx.state().clone(), self.ctx.time().clone()),
            seat.checker.clone(),
            crate::world::node_store::DamagePolicy {
                chaos_until: cutoff,
                cut_budget: (layout.durability == paros::journal::Durability::Batched)
                    .then(|| seat.floor.saturating_sub(seat.clean_copies)),
                inject: !seat.quiet,
            },
            provider.clone(),
        );
        Some((store, boot))
    }

    fn audit(&self, journal: paros::JournalIdentifier) -> Self::Audit {
        self.seat(journal)
            .map_or_else(|| self.world_audit(journal), |seat| seat.audit.clone())
    }

    /// The node's own facts (#243) report to the main journal's audit world,
    /// on no journal's board: the run's one genesis journal, which every
    /// acceptor of the run serves or joins.
    fn node_audit(&self) -> Self::Audit {
        self.world_audit(crate::shape::identifiers(self.ctx.state()).main)
    }

    /// A journal created naming this node (#189, a spare's): a quiet seat
    /// under `config`, kept across incarnations (a restart re-folds the
    /// registry and asks again).
    fn create(&mut self, journal: paros::JournalIdentifier, config: Config) -> bool {
        let Some(board) = &self.system else {
            return false;
        };
        if let Some(seat) = self.seats.iter().find(|seat| seat.journal == journal) {
            return !seat.deleted;
        }
        let mut seat = Seat::quiet(self.ctx, journal, config, board);
        seat.created = true;
        self.seats.push(seat);
        true
    }

    /// A journal a storage fault quarantined whose store the world has
    /// parked for good (a detected persistent corruption) will never open
    /// again: the audit hears the node is down for it now, as it would from
    /// a one-journal node's fail-stop exit, rather than at a re-open the run
    /// may end before (a node that serves the system journals keeps running
    /// with the journal down; witness 18183308543219257601).
    fn quarantined(&mut self, journal: paros::JournalIdentifier) {
        if let Some(seat) = self.seat(journal)
            && seat
                .world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .park_reason(self.ip)
                == Some(ParkReason::Corruption)
        {
            stay_down(&seat.checker, Down::StorageParked(self.rank));
        }
    }

    fn delete(&mut self, journal: paros::JournalIdentifier) {
        if let Some(seat) = self.seats.iter_mut().find(|seat| seat.journal == journal) {
            seat.deleted = true;
        }
    }
}
