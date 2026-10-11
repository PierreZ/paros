//! **Name resolution** (#239, `docs/architecture.md` §3.5): a journal name,
//! `paros://<tenant>/<journal>` ([`crate::name::JournalName`]), to the
//! [`JournalIdentifier`] the protocol carries.
//!
//! Two hops, each a fold of a control journal read to its tail:
//!
//! 1. **Tenant**: the tenant's name to its `TenantId` and its control
//!    journal, through the universe directory (the universe tenant's
//!    control journal, [`FleetDirectory`]). Only a `READY` `users` tenant
//!    resolves: an `internal` tenant (the universe tenant, a cell tenant)
//!    has no name and is never resolved through this path, and a tenant
//!    mid-creation or mid-removal is refused.
//! 2. **Journal**: the journal's name to its `JournalId`, through the
//!    tenant's control journal (the [`TenantControl`] fold, #210). Only a
//!    live journal resolves.
//!
//! Only the entry roles resolve (§3.5): a client sends names and never reads
//! the universe tenant. Until the frontend exists (#192), `parosctl`
//! resolves with operator rights; the simulation resolves through the same
//! functions.
//!
//! **Names are reusable once a delete completes; ids never are** (§3.5,
//! §3.8). A recreated journal draws a fresh id, so an old id never aliases
//! a new name. A resolution can therefore go stale: [`JournalNames`] caches
//! each one, and a call that a server refuses as naming an unknown journal
//! drops it ([`JournalNames::stale`]); the next resolution reads the control
//! journal again.
//!
//! Draws no randomness and decides no retry: every choice is the caller's.

use std::collections::BTreeMap;

use moonpool_core::Providers;
use paros_core::{JournalIdentifier, TenantId};

use super::Client;
use super::checkpoint::{Folder, LoadOutcome, load};
use super::fleet::read_directory;
use crate::fleet::{FleetDirectory, Group, TenantState};
use crate::name::JournalName;
use crate::tenant::TenantControl;

/// What resolving a tenant's name came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantResolution {
    /// A `READY` `users` tenant holds the name.
    Resolved {
        /// The tenant.
        tenant: TenantId,
        /// Its control journal: where its journals' names live.
        control: JournalIdentifier,
        /// The universe directory's position the resolution was read at.
        at: u64,
    },
    /// No tenant holds the name.
    Unknown,
    /// The tenant holding the name is being created or removed.
    NotReady {
        /// The tenant.
        tenant: TenantId,
        /// Where it stands.
        state: TenantState,
    },
    /// The name is an `internal` tenant's: never resolved here.
    Internal,
    /// The universe directory could not be read to its tail.
    Unreadable(LoadOutcome),
}

/// Why a control journal could not be read to its tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unreadable {
    /// No server served a page.
    Unavailable,
    /// No server serves the control journal.
    UnknownJournal,
    /// The journal was truncated below the fold's cursor, and no
    /// checkpoint healed the gap.
    Truncated,
    /// A frontend refused the read (#192 (the frontend)).
    Denied,
}

/// What resolving a journal's name inside its tenant came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalResolution {
    /// A live journal holds the name.
    Resolved {
        /// The journal.
        journal: JournalIdentifier,
        /// The control journal's position the resolution was read at.
        at: u64,
    },
    /// No live journal holds the name.
    Unknown {
        /// The control journal's position the resolution was read at.
        at: u64,
    },
    /// The tenant's control journal could not be read to its tail.
    Unreadable(Unreadable),
}

/// What resolving a whole journal name came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NameResolution {
    /// The name's journal.
    Resolved(JournalIdentifier),
    /// The tenant hop did not resolve.
    Tenant(TenantResolution),
    /// The journal hop did not resolve.
    Journal(JournalResolution),
}

/// The tenant hop's verdict on a directory already folded to `at`.
///
/// # Panics
///
/// If the directory's fold breaks its own invariants (a name held by a tenant of another name or group).
#[must_use]
pub fn tenant_in(directory: &FleetDirectory, name: &[u8], at: u64) -> TenantResolution {
    if let Some((tenant, entry)) = directory.named(name) {
        if entry.groups.contains(Group::Internal) {
            return TenantResolution::Internal;
        }
        assert!(entry.groups.contains(Group::Users));
        assert_eq!(entry.name, name);
        if entry.state != TenantState::Ready {
            return TenantResolution::NotReady {
                tenant,
                state: entry.state,
            };
        }
        let control = JournalIdentifier::new(tenant, entry.control);
        assert!(control.is_set(), "a READY tenant has a control journal");
        return TenantResolution::Resolved {
            tenant,
            control,
            at,
        };
    }
    // An internal tenant is registered with no name: an empty name is
    // never a user's, whatever holds it.
    if name.is_empty() {
        return TenantResolution::Internal;
    }
    TenantResolution::Unknown
}

/// Resolve the tenant `name` through the universe directory `universe`,
/// read to its tail from server `first` on.
#[tracing::instrument(level = "debug", skip_all, fields(universe = %universe))]
pub async fn resolve_tenant<P: Providers>(
    client: &Client<P>,
    first: usize,
    universe: JournalIdentifier,
    name: &[u8],
) -> TenantResolution {
    match read_directory(client, first, universe).await {
        Ok(directory) => tenant_in(&directory, name, directory.next_seq()),
        Err(outcome) => TenantResolution::Unreadable(outcome),
    }
}

/// Read the tenant control journal `control` to its tail from server
/// `first` on, folded as its [`TenantControl`] (from its checkpoint when
/// the journal was truncated).
///
/// # Errors
///
/// The journal could not be read to its tail.
#[tracing::instrument(level = "debug", skip_all, fields(control = %control))]
pub async fn read_tenant_control<P: Providers>(
    client: &Client<P>,
    first: usize,
    control: JournalIdentifier,
) -> Result<TenantControl, Unreadable> {
    let mut folder = Folder::new(TenantControl::new(control.tenant, control.journal));
    match load(&mut folder, control, client, first, 0).await {
        LoadOutcome::Loaded { .. } => Ok(folder.state().clone()),
        LoadOutcome::Unavailable => Err(Unreadable::Unavailable),
        LoadOutcome::UnknownJournal => Err(Unreadable::UnknownJournal),
        LoadOutcome::Denied(_) => Err(Unreadable::Denied),
        LoadOutcome::Unhealed { .. } => Err(Unreadable::Truncated),
    }
}

/// The journal hop's verdict on a tenant control journal already folded.
///
/// # Panics
///
/// If the directory names a deleted journal by a live name.
#[must_use]
pub fn journal_in(
    directory: &TenantControl,
    control: JournalIdentifier,
    name: &[u8],
) -> JournalResolution {
    let at = directory.next_seq();
    match directory.named(name) {
        Some(id) => {
            assert!(id.is_set());
            assert!(
                !directory.is_deleted(id),
                "a live name never names a tombstone"
            );
            JournalResolution::Resolved {
                journal: JournalIdentifier::new(control.tenant, id),
                at,
            }
        }
        None => JournalResolution::Unknown { at },
    }
}

/// Resolve the journal `name` inside the tenant whose control journal is
/// `control`, read to its tail from server `first` on.
pub async fn resolve_journal<P: Providers>(
    client: &Client<P>,
    first: usize,
    control: JournalIdentifier,
    name: &[u8],
) -> JournalResolution {
    match read_tenant_control(client, first, control).await {
        Ok(directory) => journal_in(&directory, control, name),
        Err(why) => JournalResolution::Unreadable(why),
    }
}

/// Resolve `name` whole: its tenant through `universe`, then its journal
/// through the tenant's control journal.
///
/// # Panics
///
/// If the journal hop resolves outside the tenant the tenant hop found.
#[tracing::instrument(level = "debug", skip_all, fields(name = %name))]
pub async fn resolve<P: Providers>(
    client: &Client<P>,
    first: usize,
    universe: JournalIdentifier,
    name: &JournalName,
) -> NameResolution {
    let control = match resolve_tenant(client, first, universe, name.tenant().as_bytes()).await {
        TenantResolution::Resolved { control, .. } => control,
        other => return NameResolution::Tenant(other),
    };
    match resolve_journal(client, first, control, name.journal().as_bytes()).await {
        JournalResolution::Resolved { journal, .. } => {
            assert_eq!(
                journal.tenant, control.tenant,
                "a tenant's journal is in the tenant"
            );
            NameResolution::Resolved(journal)
        }
        other => NameResolution::Journal(other),
    }
}

/// One tenant's journal names, resolved through its control journal and
/// cached: a resolution is reused until a call refuses it as stale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalNames {
    control: JournalIdentifier,
    /// Each name's last resolution, with the position it was read at.
    cache: BTreeMap<Vec<u8>, (JournalIdentifier, u64)>,
}

impl JournalNames {
    /// The names of the tenant whose control journal is `control`.
    ///
    /// # Panics
    ///
    /// If `control` is unset.
    #[must_use]
    pub fn new(control: JournalIdentifier) -> Self {
        assert!(control.is_set(), "a tenant's control journal is set");
        Self {
            control,
            cache: BTreeMap::new(),
        }
    }

    /// The tenant's control journal.
    #[must_use]
    pub fn control(&self) -> JournalIdentifier {
        self.control
    }

    /// `name`'s cached resolution and the position it was read at.
    #[must_use]
    pub fn cached(&self, name: &[u8]) -> Option<(JournalIdentifier, u64)> {
        self.cache.get(name).copied()
    }

    /// `name`'s journal: the cached resolution, or a fresh one.
    pub async fn resolve<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
    ) -> JournalResolution {
        if let Some((journal, at)) = self.cached(name) {
            return JournalResolution::Resolved { journal, at };
        }
        self.refresh(client, first, name).await
    }

    /// `name`'s journal, read afresh from the control journal; the cache
    /// keeps what it came to.
    pub async fn refresh<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
    ) -> JournalResolution {
        let resolution = resolve_journal(client, first, self.control, name).await;
        self.absorb(name, resolution);
        resolution
    }

    /// Keep what a resolution of `name` came to: a resolved journal is
    /// cached (never over a resolution read later), an unknown name is
    /// dropped, an unreadable control journal changes nothing.
    ///
    /// # Panics
    ///
    /// If `resolution` names a journal of another tenant.
    pub fn absorb(&mut self, name: &[u8], resolution: JournalResolution) {
        match resolution {
            JournalResolution::Resolved { journal, at } => {
                assert_eq!(journal.tenant, self.control.tenant);
                if self.cache.get(name).is_none_or(|(_, cached)| *cached <= at) {
                    self.cache.insert(name.to_vec(), (journal, at));
                }
            }
            JournalResolution::Unknown { at } => {
                if self
                    .cache
                    .get(name)
                    .is_some_and(|(_, cached)| *cached <= at)
                {
                    self.cache.remove(name);
                }
            }
            JournalResolution::Unreadable(_) => {}
        }
    }

    /// A call on `journal`, resolved from `name`, was refused as naming an
    /// unknown journal: drop the resolution if it still names `journal`.
    /// Whether it did (the resolution was stale).
    ///
    /// # Panics
    ///
    /// Never: the assertion pins the postcondition.
    pub fn stale(&mut self, name: &[u8], journal: JournalIdentifier) -> bool {
        if self
            .cache
            .get(name)
            .is_some_and(|(cached, _)| *cached == journal)
        {
            self.cache.remove(name);
            assert!(self.cached(name).is_none());
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use paros_core::{AcceptorConfig, JournalId, NodeId, QuorumSystem, WriterMode};

    use super::*;
    use crate::tenant::{Desired, TenantCommand};

    fn create(request: u64, id: u64, name: &[u8]) -> Vec<u8> {
        TenantCommand::CreateJournal {
            request,
            id: JournalId(id),
            name: name.to_vec(),
            writer: WriterMode::Single,
            desired: Desired::DOUBLE,
            config: AcceptorConfig::new(vec![NodeId(1)], QuorumSystem::Majority),
        }
        .encode()
    }

    fn delete(request: u64, id: u64) -> Vec<u8> {
        TenantCommand::DeleteJournal {
            request,
            id: JournalId(id),
        }
        .encode()
    }

    const CONTROL: JournalIdentifier = JournalIdentifier {
        tenant: TenantId(0xacc),
        journal: JournalId(0xc0),
    };

    #[test]
    fn a_recreated_name_resolves_to_its_new_id_and_the_old_one_never_again() {
        let mut directory = TenantControl::new(CONTROL.tenant, CONTROL.journal);
        directory.fold(0, &create(1, 7, b"orders"));
        let first = journal_in(&directory, CONTROL, b"orders");
        assert_eq!(
            first,
            JournalResolution::Resolved {
                journal: JournalIdentifier::new(CONTROL.tenant, JournalId(7)),
                at: 1
            }
        );
        directory.fold(1, &delete(2, 7));
        assert_eq!(
            journal_in(&directory, CONTROL, b"orders"),
            JournalResolution::Unknown { at: 2 }
        );
        directory.fold(2, &create(3, 9, b"orders"));
        let again = journal_in(&directory, CONTROL, b"orders");
        assert_eq!(
            again,
            JournalResolution::Resolved {
                journal: JournalIdentifier::new(CONTROL.tenant, JournalId(9)),
                at: 3
            }
        );

        // The cache keeps the first resolution until a call refuses it.
        let mut names = JournalNames::new(CONTROL);
        names.absorb(b"orders", first);
        let old = JournalIdentifier::new(CONTROL.tenant, JournalId(7));
        assert_eq!(names.cached(b"orders"), Some((old, 1)));
        // A resolution read earlier never overwrites a later one.
        names.absorb(b"orders", again);
        names.absorb(b"orders", first);
        assert_eq!(
            names.cached(b"orders").map(|(j, _)| j.journal),
            Some(JournalId(9))
        );
        // A refusal of an id the cache no longer names changes nothing.
        assert!(!names.stale(b"orders", old));
        let new = JournalIdentifier::new(CONTROL.tenant, JournalId(9));
        assert!(names.stale(b"orders", new));
        assert_eq!(names.cached(b"orders"), None);
    }

    #[test]
    fn an_internal_or_unready_tenant_never_resolves() {
        let directory = FleetDirectory::default();
        assert_eq!(tenant_in(&directory, b"", 0), TenantResolution::Internal);
        assert_eq!(tenant_in(&directory, b"acme", 0), TenantResolution::Unknown);
    }
}
