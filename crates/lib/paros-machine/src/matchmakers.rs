//! A machine's matchmaker host (#190, `docs/architecture/03-4-tenant-modes.md` §3.4): every
//! tenant's matchmaker set is born on the cell's founding members, the
//! bootstrap generation, so every journal a machine serves is born with its
//! tenant's set. The cell coordinator then places each set with
//! `paros_model::placement` and hands it over onto the machines it booked (#501
//! (matchmaker sets off the founders)): any storage machine of the cell,
//! founding or admitted, hosts a later generation of a set the registry
//! books on it.
//!
//! The host is the shipped matchmaker loop ([`paros_roles::matchmaker`]) in a task
//! beside the node loop, on the node's own RPC edge: a machine answers the
//! matchmaker contract at the address it serves its journals at. A set's
//! store is provisioned at the first request that names its tenant:
//! formatted, then named in the disk's `sets` file, the commit point (as a
//! created journal's record line is, #210). A founding member provisions
//! any tenant's set, since it is a member of every bootstrap generation;
//! any other machine only a set the registry books on it, as the node loop
//! last folded it (`JournalStores::book_sets`). A set the file names is an
//! existing member's from then on, so a lost store is refused as amnesia,
//! never formatted again. A provisioned set stays open after its booking is
//! released: the handover froze it, and it answers a late proposer with its
//! successor, the chain a journal that boots at generation 0 walks to the
//! set in force. A storage fault closes its set only; the set re-opens at
//! its next request.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_buggify::hint::Strike;
use moonpool_core::{Detach, LocalStorageProvider, LocalTaskProvider, SimulationResult};
use moonpool_rpc::RpcHandle;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_core::matchmaker::MatchmakerConfig;
use paros_core::membership::MatchmakerId;
use paros_net::runtime::ParosProviders;
use tokio_util::sync::CancellationToken;

use super::ProviderDisk;
use paros_observe::audit::Audit;
use paros_roles::matchmaker::{Host, HostedSet, SetOpener};
use paros_roles::node::edge::MatchmakerInbox;
use paros_storage::boot::BootKind;
use paros_storage::journal::JournalMatchmakerStorage;
use paros_storage::provision::Provisioned;
use paros_tunables::DriverTunables;

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

/// What the host knows of each tenant's set, as the node loop last folded
/// the registry (#501 (matchmaker sets off the founders)): written by the
/// loop, read by the host's task.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SetBook {
    /// The tenants of the cell plan's journals: their set's generation 0 is
    /// the host's own bootstrap.
    pub(crate) plan: BTreeSet<TenantId>,
    /// The tenants whose set the registry books here.
    pub(crate) booked: BTreeSet<TenantId>,
    /// Each hosted tenant's generation 0: the machines it was born on (#685
    /// (no founders)).
    pub(crate) hosted: BTreeMap<TenantId, Vec<MatchmakerId>>,
}

impl SetBook {
    /// The generation 0 of `tenant`'s set, `own` for a plan tenant, or
    /// `None` while the fold names no such tenant.
    fn bootstrap(&self, tenant: TenantId, own: &[MatchmakerId]) -> Option<Vec<MatchmakerId>> {
        if self.plan.contains(&tenant) {
            return Some(own.to_vec());
        }
        let hosted = self.hosted.get(&tenant)?;
        assert!(!hosted.is_empty(), "a hosted tenant is born on machines");
        Some(hosted.clone())
    }
}

/// The host's [`SetBook`], shared with the node loop.
pub(crate) type SetBookings = Arc<Mutex<SetBook>>;

/// The opener of a machine's sets: provisions a tenant's set at its first
/// request, then opens it as an existing member's.
struct MachineSets<S> {
    disk: ProviderDisk<S>,
    config: MatchmakerConfig,
    /// The sets the disk's `sets` file names: provisioned here.
    provisioned: BTreeSet<TenantId>,
    /// The sets the registry books here.
    booked: SetBookings,
}

impl<S: LocalStorageProvider + Clone + 'static> MachineSets<S> {
    /// The configuration `tenant`'s set is formatted with: this cell's
    /// bootstrap, or the founding members of the cell a tenant born
    /// elsewhere comes from (#784 (coordinator handover)).
    fn config_for(&self, born: Option<&[paros_core::NodeId]>) -> MatchmakerConfig {
        let Some(born) = born else {
            return self.config.clone();
        };
        let mut bootstrap: Vec<MatchmakerId> = born.iter().map(|n| MatchmakerId(n.0)).collect();
        bootstrap.sort_unstable();
        bootstrap.dedup();
        assert!(!bootstrap.is_empty(), "a birth cell has founding members");
        MatchmakerConfig {
            id: self.config.id,
            bootstrap,
        }
    }

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
    async fn provision(&mut self, tenant: TenantId, config: &MatchmakerConfig) -> bool {
        assert!(
            !self.provisioned.contains(&tenant),
            "a set is provisioned once"
        );
        assert!(config.id == self.config.id, "a set is this machine's");
        let mut store = self.store(tenant);
        match paros_storage::provision::provision_matchmaker_store(&mut store, config).await {
            Ok(Provisioned::Formatted) => {}
            Ok(Provisioned::Resumed) => {
                moonpool_assertions::reachable!(
                    "machine: a matchmaker set's provisioning resumed from the disk"
                );
                // The marker may be one a failed format sync left staged
                // (#348): durable before the sets file names the set.
                let dir = self.disk.matchmaker_dir(tenant);
                if let Err(error) = paros_storage::journal::settle(self.disk.provider(), &dir).await
                {
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

impl<S: LocalStorageProvider + Clone + 'static> SetOpener for MachineSets<S> {
    type Storage = JournalMatchmakerStorage<S>;

    async fn open(
        &mut self,
        tenant: TenantId,
        (foreign, born): (bool, Option<Vec<paros_core::NodeId>>),
    ) -> Option<HostedSet<Self::Storage>> {
        // A request from a deployment that names no tenant opens nothing.
        if tenant.0 == 0 {
            return None;
        }
        // A tenant born in another cell (#784 (coordinator handover)):
        // its generation 0 is the machines it was born on there.
        let born_config = born.as_deref().map(|born| self.config_for(Some(born)));
        if born_config.is_some() {
            moonpool_assertions::reachable!(
                "machine: a set of a tenant born in another cell opens here"
            );
        }
        // The set's generation 0 (#685 (no founders)): a set of a tenant
        // the fold does not name yet opens later, never under a guessed
        // bootstrap that its format marker would then record.
        let (bootstrap, booked) = {
            let book = self.booked.lock().unwrap_or_else(PoisonError::into_inner);
            (
                book.bootstrap(tenant, &self.config.bootstrap),
                book.booked.contains(&tenant),
            )
        };
        let bootstrap = match (born_config, bootstrap) {
            (Some(config), _) => config.bootstrap,
            (None, Some(bootstrap)) => bootstrap,
            (None, None) => {
                moonpool_assertions::reachable!("machine: a set waits for its tenant to be folded");
                tracing::debug!(tenant = tenant.0, "matchmaker_set_unknown_tenant");
                return None;
            }
        };
        let config = MatchmakerConfig {
            id: self.config.id,
            bootstrap,
        };
        if !self.provisioned.contains(&tenant) {
            // A member of the set's generation 0 hosts it from its birth;
            // another machine only once it is booked here (#501
            // (matchmaker sets off the founders)). Another cell's request
            // never provisions a generation 0 (#539 (matchmaker cell
            // fence), #783 (cross the cell fence)): only a set booked here.
            let member = config.bootstrap.contains(&config.id) && !foreign;
            if !member && !booked {
                tracing::debug!(tenant = tenant.0, "matchmaker_set_not_booked");
                return None;
            }
            if !self.provision(tenant, &config).await {
                return None;
            }
            if !member {
                moonpool_assertions::reachable!("machine: an admitted machine hosts a set");
            }
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
            bootstrap: Some(config.bootstrap),
        })
    }
}

/// Start member `config.id`'s matchmaker host on the node's edge `rpc`
/// (#190), served in the machine's `machine` group too (#404 (static vs
/// dynamic endpoints)): its sets on `disk`, each journal's facts reported
/// to the port `audits` makes for it. A machine outside the bootstrap set
/// opens only the sets `booked` names (#501 (matchmaker sets off the
/// founders)). It serves only requests its `fence` admits (#539 (matchmaker
/// cell fence), #783 (cross the cell fence)).
///
/// The host runs on past `shutdown` until it ends the request it serves (a
/// set's boot, its durable write): the returned end resolves then, and the
/// node loop waits for it before the next incarnation hosts the same disk.
///
/// # Errors
///
/// A registration the RPC runtime refuses.
// The edge and its group, the disk, the shape, the observer and the lifetime.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn<P, S, A, F>(
    providers: &P,
    rpc: &RpcHandle<P>,
    machine: Option<&moonpool_rpc::ServiceGroup<paros_wire::methods::MachineInterface>>,
    disk: ProviderDisk<S>,
    (config, booked, fence): (MatchmakerConfig, SetBookings, paros_net::CellFence),
    tunables: &DriverTunables,
    audits: Arc<F>,
    shutdown: CancellationToken,
) -> SimulationResult<paros_roles::node::HostEnded>
where
    P: ParosProviders,
    S: LocalStorageProvider + Clone + 'static,
    A: Audit + Send + Sync + 'static,
    F: Fn(JournalIdentifier) -> A + Send + Sync + 'static,
{
    assert!(
        !config.bootstrap.is_empty(),
        "every set has a bootstrap generation"
    );
    let inbox = MatchmakerInbox::serve(rpc, machine)?;
    let page = tunables
        .registry_page
        .clamp(1, paros_core::contract::limits::REGISTRY_PAGE);
    assert!(page >= 1, "a registry page holds a registration");
    let (alive, ended) = paros_roles::node::HostEnded::pair();
    providers
        .local_task()
        .spawn_local_task("paros-machine-matchmakers", async move {
            // Held for the task's life: its drop is the host's end.
            let _alive = alive;
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
                booked,
            };
            let factory = |journal: JournalIdentifier| audits(journal);
            let config = sets.config.clone();
            let host = Host {
                config: &config,
                page,
                factory: &factory,
                fail_stop: false,
                fence,
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
    Ok(ended)
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
