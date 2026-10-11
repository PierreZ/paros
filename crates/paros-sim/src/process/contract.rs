//! The journal stores' contract suites on the simulated disk.

use async_trait::async_trait;
use moonpool_sim::{SimContext, SimulationResult, assert_always};

use paros::{Config, NodeId};

use moonpool_sim::SimStorageProvider;

/// The two contract suites against the library's journal stores
/// ([`paros::JournalStorage`], [`paros::JournalMatchmakerStorage`]) on the
/// simulated disk, each store in a directory of its own.
#[tracing::instrument(level = "debug", skip_all)]
async fn journal_contract_suites(provider: SimStorageProvider) {
    use paros::{
        JournalMatchmakerStorage, JournalStorage, JournalStoreConfig, LogStorage, MatchmakerStorage,
    };
    let config = Config {
        peers: vec![NodeId(0)],
        ..Config::new(NodeId(0), paros::JournalIdentifier::UNSET)
    };
    let store = JournalStoreConfig::small();
    let open_node = |provider: SimStorageProvider, dir: String, config: Config| async move {
        let mut node = JournalStorage::new(provider, dir, config, store);
        node.boot_scan().await.expect("a clean journal store boots");
        node
    };
    let mut instance = 0_u64;
    let fresh = || {
        instance += 1;
        open_node(
            provider.clone(),
            format!("journal-contract/node-{instance}"),
            config.clone(),
        )
    };
    let reopen = |old: JournalStorage<SimStorageProvider>| {
        let dir = old.dir().to_string();
        drop(old);
        open_node(provider.clone(), dir, config.clone())
    };
    Box::pin(paros::storage_contract_suite(fresh, reopen)).await;
    let open_registry = |provider: SimStorageProvider, dir: String| async move {
        let mut registry =
            JournalMatchmakerStorage::new(provider, dir, paros::JournalIdentifier::UNSET, store);
        registry
            .boot_scan()
            .await
            .expect("a clean journal registry boots");
        registry
    };
    let mut registry_instance = 0_u64;
    let fresh_registry = || {
        registry_instance += 1;
        open_registry(
            provider.clone(),
            format!("journal-contract/mm-{registry_instance}"),
        )
    };
    let reopen_registry = |old: JournalMatchmakerStorage<SimStorageProvider>| {
        let dir = old.dir().to_string();
        drop(old);
        open_registry(provider.clone(), dir)
    };
    Box::pin(paros::matchmaker_storage_contract_suite(
        fresh_registry,
        reopen_registry,
    ))
    .await;
}

/// The **contract-suite workload** (issue #21 item F): runs the shared
/// [`paros::storage_contract_suite`] and
/// [`paros::matchmaker_storage_contract_suite`] against the library's journal
/// stores over the simulation's own disk, inside one quiet iteration, so the
/// stores the simulation runs can never drift from the trait contract
/// [`paros::MemStorage`] pins.
pub(crate) struct ContractSuiteWorkload;

#[async_trait]
impl moonpool_sim::Workload for ContractSuiteWorkload {
    fn name(&self) -> &'static str {
        "storage-contract-suite"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        Box::pin(journal_contract_suites(ctx.storage().clone())).await;
        // The crash half the shared suite cannot express (an in-memory store
        // has no un-synced stage): a registration or a watermark raise that
        // was staged but never synced does not survive the incarnation, so a
        // reopen reads back exactly the last sync — the read-side pair of
        // the driver's persist-before-reply ordering.
        {
            use paros::{
                AcceptorConfig, Ballot, JournalId, JournalMatchmakerStorage, JournalStoreConfig,
                MatchmakerStorage, NodeId, Registration, RegistryStorage,
            };
            let j = JournalId(1);
            let config = Registration::belief(AcceptorConfig::new(
                vec![NodeId(0)],
                paros::QuorumSystem::Majority,
            ));
            let ballot = |round: u64| Ballot {
                round,
                node: NodeId(1),
            };
            let open = || {
                JournalMatchmakerStorage::new(
                    ctx.storage().clone(),
                    "journal-contract/mm-unsynced",
                    paros::JournalIdentifier::UNSET,
                    JournalStoreConfig::small(),
                )
            };
            let mut store = open();
            store.boot_scan().await.expect("a fresh registry boots");
            store
                .register(j, ballot(1), &config)
                .await
                .expect("register 1");
            store.sync().await.expect("sync 1");
            store
                .register(j, ballot(2), &config)
                .await
                .expect("register 2 (never synced)");
            store
                .set_gc_watermark(j, ballot(1))
                .await
                .expect("raise (never synced)");
            drop(store);
            let mut rebooted = open();
            rebooted.boot_scan().await.expect("the registry reopens");
            assert_always!(
                rebooted.registered() == vec![(j, ballot(1))]
                    && rebooted.registration(j, ballot(2)).is_none(),
                "matchmaker: an un-synced registration does not survive a crash"
            );
            assert_always!(
                rebooted.initial_state().gc_watermark(j) == Ballot::zero(),
                "matchmaker: an un-synced watermark raise does not survive a crash"
            );
        }
        Ok(())
    }
}
