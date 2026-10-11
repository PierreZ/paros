//! A founding member's matchmaker host (#190, `docs/architecture.md` §3.4):
//! every tenant's matchmaker set runs on the cell's founding members until
//! placement (#212), so every journal a machine serves is born with its
//! tenant's set.
//!
//! The host is the shipped matchmaker loop ([`crate::matchmaker`]) in a task
//! beside the node loop, on the node's own RPC edge: a founding member
//! answers the matchmaker contract at the address it serves its journals
//! at. A set's store is provisioned at the first request that names its
//! tenant: formatted, then named in the disk's `sets` file, the commit point
//! (as a created journal's record line is, #210). A set the file names is
//! an existing member's from then on, so a lost store is refused as
//! amnesia, never formatted again. A storage fault closes its set only; the
//! set re-opens at its next request.

use std::collections::BTreeSet;
use std::sync::Arc;

use moonpool_buggify::hint::Strike;
use moonpool_core::{Detach, Providers, SimulationResult, StorageProvider, TaskProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, MatchmakerConfig, TenantId};
use tokio_util::sync::CancellationToken;

use super::ProviderDisk;
use crate::driver::edge::MatchmakerInbox;
use crate::journal::JournalMatchmakerStorage;
use crate::matchmaker::{Host, HostedSet, SetOpener};
use crate::{Audit, BootKind, DriverTunables, Provisioned};

/// The identity a set's store is stamped with: its tenant and the unset
/// journal (a set's store is no journal's).
fn store_id(tenant: TenantId) -> JournalIdentifier {
    JournalIdentifier::new(tenant, JournalId::UNSET)
}

/// The sets file's text: one tenant id per line.
fn render(sets: &BTreeSet<TenantId>) -> String {
    let text: String = sets.iter().fold(String::new(), |mut text, tenant| {
        text.push_str(&tenant.0.to_string());
        text.push('\n');
        text
    });
    assert_eq!(text.lines().count(), sets.len(), "one line per set");
    text
}

/// The sets file's tenants.
///
/// # Errors
///
/// A line that is not a set tenant id.
fn parse(text: &str) -> Result<BTreeSet<TenantId>, String> {
    let sets = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| match line.trim().parse::<u64>() {
            Ok(0) => Err("an unset tenant in the sets file".to_string()),
            Ok(tenant) => Ok(TenantId(tenant)),
            Err(error) => Err(format!("bad sets line {line:?}: {error}")),
        })
        .collect::<Result<BTreeSet<TenantId>, String>>()?;
    assert!(
        sets.iter().all(|tenant| tenant.0 != 0),
        "every set is named"
    );
    Ok(sets)
}

/// The opener of a founding member's sets: provisions a tenant's set at its
/// first request, then opens it as an existing member's.
struct MachineSets<S> {
    disk: ProviderDisk<S>,
    config: MatchmakerConfig,
    /// The sets the disk's `sets` file names: provisioned here.
    provisioned: BTreeSet<TenantId>,
}

impl<S: StorageProvider + Clone + 'static> MachineSets<S> {
    fn store(&self, tenant: TenantId) -> JournalMatchmakerStorage<S> {
        JournalMatchmakerStorage::new(
            self.disk.provider().clone(),
            self.disk.matchmaker_dir(tenant),
            store_id(tenant),
            self.disk.layout(),
        )
    }

    /// Provision `tenant`'s set: format its store (an interrupted run
    /// resumes from the disk), then name it in the sets file. `false` when
    /// the disk failed: the request is dropped, and the next one tries
    /// again.
    async fn provision(&mut self, tenant: TenantId) -> bool {
        assert!(
            !self.provisioned.contains(&tenant),
            "a set is provisioned once"
        );
        let mut store = self.store(tenant);
        match crate::provision_matchmaker_store(&mut store, &self.config).await {
            Ok(Provisioned::Formatted) => {}
            Ok(Provisioned::Resumed) => {
                moonpool_assertions::reachable!(
                    "machine: a matchmaker set's provisioning resumed from the disk"
                );
                // The marker may be one a failed format sync left staged
                // (#348): durable before the sets file names the set.
                let dir = self.disk.matchmaker_dir(tenant);
                if let Err(error) = crate::journal::settle(self.disk.provider(), &dir).await {
                    tracing::warn!(tenant = tenant.0, %error, "matchmaker_set_unsettled");
                    return false;
                }
            }
            Err(error) => {
                tracing::warn!(tenant = tenant.0, %error, "matchmaker_set_unprovisioned");
                return false;
            }
        }
        // Hint: formatted, not recorded. A machine killed here formats the
        // set again at its next request, and finds the marker.
        let hinted = moonpool_buggify::hint!("matchmaker set formatted, not recorded", 0.05);
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!(
                "machine: a machine dies between a set's format and its record"
            );
        }
        hinted.await;
        // The sets file is the commit point: written after the format,
        // before the set serves.
        let mut sets = self.provisioned.clone();
        sets.insert(tenant);
        if let Err(error) = self.disk.write_sets(&render(&sets)).await {
            tracing::warn!(tenant = tenant.0, %error, "matchmaker_set_unrecorded");
            return false;
        }
        self.provisioned = sets;
        moonpool_assertions::reachable!("machine: a matchmaker set is provisioned");
        tracing::info!(tenant = tenant.0, "matchmaker_set_provisioned");
        true
    }
}

impl<S: StorageProvider + Clone + 'static> SetOpener for MachineSets<S> {
    type Storage = JournalMatchmakerStorage<S>;

    async fn open(&mut self, tenant: TenantId) -> Option<HostedSet<Self::Storage>> {
        // A request from a deployment that names no tenant opens nothing.
        if tenant.0 == 0 {
            return None;
        }
        if !self.provisioned.contains(&tenant) && !self.provision(tenant).await {
            return None;
        }
        assert!(
            self.provisioned.contains(&tenant),
            "a set is recorded before it serves"
        );
        Some(HostedSet {
            tenant,
            journals: Vec::new(),
            storage: self.store(tenant),
            boot: BootKind::ExistingMember,
        })
    }
}

/// Start member `config.id`'s matchmaker host on the node's edge `rpc`
/// (#190), served in the machine's `machine` group too (#404 (static vs
/// dynamic endpoints)): its sets on `disk`, each journal's facts reported
/// to the port `audits` makes for it.
///
/// # Errors
///
/// A registration the RPC runtime refuses.
// The edge and its group, the disk, the shape, the observer and the lifetime.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn<P, S, A, F>(
    providers: &P,
    rpc: &RpcHandle<P>,
    machine: Option<&moonpool_rpc::ServiceGroup<crate::rpc::methods::MachineInterface>>,
    disk: ProviderDisk<S>,
    config: MatchmakerConfig,
    tunables: &DriverTunables,
    audits: Arc<F>,
    shutdown: CancellationToken,
) -> SimulationResult<()>
where
    P: Providers,
    S: StorageProvider + Clone + 'static,
    A: Audit + Send + Sync + 'static,
    F: Fn(JournalIdentifier) -> A + Send + Sync + 'static,
{
    assert!(
        config.bootstrap.contains(&config.id),
        "a founding member hosts the sets it is a member of"
    );
    let inbox = MatchmakerInbox::serve(rpc, machine)?;
    let page = tunables.registry_page.clamp(1, paros_core::REGISTRY_PAGE);
    assert!(page >= 1, "a registry page holds a registration");
    providers
        .task()
        .spawn_task("paros-machine-matchmakers", async move {
            let provisioned = match disk.read_sets().await {
                Ok(None) => BTreeSet::new(),
                Ok(Some(text)) => match parse(&text) {
                    Ok(sets) => sets,
                    Err(error) => {
                        tracing::error!(%error, "matchmaker_sets_unreadable");
                        return;
                    }
                },
                // Never read as empty: a set formatted again over a lost
                // store would answer as a matchmaker it is not.
                Err(error) => {
                    tracing::warn!(%error, "matchmaker_sets_unread");
                    return;
                }
            };
            let id = config.id;
            let mut sets = MachineSets {
                disk,
                config,
                provisioned,
            };
            let factory = |journal: JournalIdentifier| audits(journal);
            let config = sets.config.clone();
            let host = Host {
                config: &config,
                page,
                factory: &factory,
                fail_stop: false,
            };
            let served = host
                .serve::<P, _, _, _>(
                    None,
                    inbox,
                    std::collections::BTreeMap::new(),
                    &mut sets,
                    shutdown,
                )
                .await;
            if let Err(error) = served {
                tracing::warn!(matchmaker = id.0, %error, "matchmaker_host_ended");
            }
        })
        .detach();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sets_file_round_trips_and_refuses_an_unset_tenant() {
        let sets: BTreeSet<TenantId> = [TenantId(7), TenantId(3)].into();
        assert_eq!(parse(&render(&sets)), Ok(sets));
        assert!(parse("0\n").is_err());
        assert!(parse("x\n").is_err());
    }
}
