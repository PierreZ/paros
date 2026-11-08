//! The control journals as a node follows them (#189, #210, #474):
//! `run_journals` with a `ControlPlan` over in-memory stores. A test writes
//! the registry's and a tenant control journal's commands with the library
//! client, as the coordinator does, and checks what each node then serves.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use moonpool_sim::SimProviders;
use paros_client::{Client, Writer, WriterOutcome};
use paros_core::journal::{JournalId, JournalIdentifier, LeaderUuid, TenantId, WriterMode};
use paros_core::membership::{AcceptorConfig, QuorumSystem};
use paros_core::{Config, NodeId, Value};
use paros_model::address::{Address, Names};
use paros_model::machine::{CachedRegistry, CellPlan};
use paros_model::registry::{
    BookingTarget, Class, GrantKey, HostedTenant, Role, Slot, SystemCommand,
};
use paros_model::tags::UserTags;
use paros_model::tenant::{Desired, StoreDurability, TenantCommand};
use paros_net::transport::Transport;
use paros_observe::audit::NoAudit;
use paros_storage::MemStorage;
use tokio_util::sync::CancellationToken;

use crate::node::host::{CellHost, FormedCell, Hosted, MachineFacts};
use crate::node::{BootKind, ControlPlan, DriverTunables, JournalStores, RunError, run_journals};
use crate::rig::{self, Rig};

/// The cell control journal: the registry.
const REGISTRY: JournalIdentifier = JournalIdentifier::new(TenantId(40), JournalId(1));

/// The hosted tenant and its control journal.
const TENANT: TenantId = TenantId(41);
const TENANT_CONTROL: JournalIdentifier = JournalIdentifier::new(TENANT, JournalId(2));

/// A journal the tenant creates.
const CREATED: JournalIdentifier = JournalIdentifier::new(TENANT, JournalId(3));

const INITIAL_MEMBERS: [u64; 3] = [1, 2, 3];
const FLEET: u64 = 9;

/// A node's stores, all in memory: a journal its control journals create
/// is provisioned on demand.
struct MemStores {
    /// The journals it serves from boot.
    served: Vec<JournalIdentifier>,
    /// The configuration of every journal it may open.
    configs: BTreeMap<JournalIdentifier, Config>,
    /// Every registry book the node loop cached, in order.
    caches: Rc<RefCell<Vec<CachedRegistry>>>,
}

impl JournalStores for MemStores {
    type Store = MemStorage;
    type Audit = NoAudit;

    fn journals(&self) -> Vec<JournalIdentifier> {
        self.served.clone()
    }

    async fn open(&mut self, journal: JournalIdentifier) -> Option<(MemStorage, BootKind)> {
        let config = self.configs.get(&journal)?.clone();
        Some((MemStorage::new(config), BootKind::FirstBoot))
    }

    fn audit(&self, _journal: JournalIdentifier) -> NoAudit {
        NoAudit
    }

    fn node_audit(&self) -> NoAudit {
        NoAudit
    }

    fn cache_registry(&self, cache: CachedRegistry) {
        self.caches.borrow_mut().push(cache);
    }

    fn create(
        &mut self,
        journal: JournalIdentifier,
        config: Config,
        _durability: paros_storage::journal::Durability,
    ) -> bool {
        self.configs.entry(journal).or_insert(config);
        true
    }
}

/// The registry's configuration on initial member `id`.
fn registry_config(id: u64) -> Config {
    let mut config = Config::new(NodeId(id), REGISTRY);
    config.peers = INITIAL_MEMBERS.iter().map(|id| NodeId(*id)).collect();
    config
}

/// The control plan of node `id`.
fn plan(id: u64) -> ControlPlan {
    ControlPlan {
        self_id: NodeId(id),
        class: Class::All,
        seeds: rig::pool(&INITIAL_MEMBERS),
        cell: REGISTRY,
        initial_members: INITIAL_MEMBERS.iter().map(|id| NodeId(*id)).collect(),
        matchmakers: Vec::new(),
        spares: Vec::new(),
        cached: Vec::new(),
        planned: Vec::new(),
    }
}

/// Each node's exit, by its id.
type Ends = Vec<(u64, Result<(), RunError>)>;

/// The cell's election journal: an initial member's plan names it.
const ELECTION: JournalIdentifier = JournalIdentifier::new(TenantId(40), JournalId(4));

/// An initial member's machine, formed under [`plan_of_cell`].
#[derive(Clone)]
struct InitialMember(FormedCell);

impl CellHost for InitialMember {
    fn control_journals(&self) -> paros_model::machine::ControlJournals {
        self.0.control_journals()
    }

    fn facts(&self) -> &MachineFacts {
        &self.0.facts
    }

    fn facts_mut(&mut self) -> &mut MachineFacts {
        &mut self.0.facts
    }

    fn formed(&self) -> Option<&FormedCell> {
        Some(&self.0)
    }

    fn serve<
        P: paros_net::runtime::ParosProviders,
        A: paros_observe::audit::Audit + Clone + Send + Sync + 'static,
    >(
        self,
        _: &P,
        _: &moonpool_rpc::RpcHandle<P>,
        _: &DriverTunables,
        _: (Option<std::sync::Arc<dyn paros_client::CallObserver>>, A),
        _: (CancellationToken, CancellationToken),
    ) -> Result<Hosted, RunError> {
        Ok(Hosted::default())
    }
}

/// The cell plan the initial members formed.
fn plan_of_cell() -> CellPlan {
    let plan = CellPlan {
        cell_id: 5,
        members: INITIAL_MEMBERS
            .iter()
            .map(|id| (NodeId(*id), Address::from(rig::addr(*id))))
            .collect(),
        control: REGISTRY,
        fleet: None,
        election: ELECTION,
        journals: vec![REGISTRY, ELECTION],
    };
    plan.check().expect("a valid test plan");
    plan
}

/// Initial member `id`'s machine.
fn initial_member_host(id: u64) -> InitialMember {
    let listen = rig::addr(id);
    InitialMember(FormedCell {
        facts: MachineFacts {
            node_id: NodeId(id),
            class: Class::All,
            locality: paros_model::machine::Locality::default(),
            name: format!("m{id}"),
            addr: Address::from(listen),
            listen,
            names: Names::literal(),
            incarnation: 1,
            machine_interface: Vec::new(),
            load: paros_model::load::LoadBoard::default(),
            // Metrics on: every call is metered and labelled.
            metrics: paros_observe::metrics::Metrics::on(),
            frontend: None,
        },
        plan: plan_of_cell(),
        ballot: paros_core::Ballot::default(),
        cached: None,
        token: None,
    })
}

/// Run the initial members (each serving the registry) and the joiners
/// `joiners` (serving nothing at boot) until `body` ends: each node's exit, and
/// `body`'s answer.
fn control_cell<T>(joiners: &[u64], body: impl AsyncFnOnce(SimProviders) -> T) -> (Ends, T) {
    let (ends, answer, _) = cell_of(joiners, false, body);
    (ends, answer)
}

/// [`control_cell`], each initial member a formed machine when `machines`: also
/// every registry book each node cached, by node.
fn cell_of<T>(
    joiners: &[u64],
    machines: bool,
    body: impl AsyncFnOnce(SimProviders) -> T,
) -> (Ends, T, BTreeMap<u64, Vec<CachedRegistry>>) {
    let mut rig = Rig::new();
    let client = rig.providers(rig::CLIENT_IP);
    let stop = CancellationToken::new();
    let mut caches = BTreeMap::new();
    let nodes: Vec<_> = INITIAL_MEMBERS
        .iter()
        .chain(joiners)
        .map(|id| {
            let initial_member = INITIAL_MEMBERS.contains(id);
            let books = Rc::new(RefCell::new(Vec::new()));
            caches.insert(*id, books.clone());
            let mut configs = BTreeMap::new();
            let mut served = Vec::new();
            if initial_member {
                configs.insert(REGISTRY, registry_config(*id));
                served.push(REGISTRY);
            }
            if initial_member && machines {
                let mut election = Config::new(NodeId(*id), ELECTION);
                election.peers = INITIAL_MEMBERS.iter().map(|id| NodeId(*id)).collect();
                election.writer_mode = WriterMode::Multi;
                configs.insert(ELECTION, election);
                served.push(ELECTION);
            }
            let stores = MemStores {
                served,
                configs,
                caches: books,
            };
            let host = (initial_member && machines).then(|| initial_member_host(*id));
            let (providers, stop, id) = (rig.node(*id), stop.clone(), *id);
            Box::pin(async move {
                let end = Box::pin(run_journals(
                    providers,
                    stores,
                    NodeId(id),
                    rig::addr(id).to_string(),
                    rig::pool(&INITIAL_MEMBERS),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Some(plan(id)),
                    host,
                    DriverTunables::default(),
                    Transport::Plaintext,
                    stop,
                ))
                .await;
                (id, end)
            })
        })
        .collect();
    let (ends, answer) = rig.run(async move {
        let answer = async {
            let answer = body(client).await;
            stop.cancel();
            answer
        };
        futures::join!(futures::future::join_all(nodes), answer)
    });
    let caches = caches
        .into_iter()
        .map(|(id, cached)| (id, cached.borrow().clone()))
        .collect();
    (ends, answer, caches)
}

fn clean(ends: Ends) {
    for (id, end) in ends {
        if let Err(error) = end {
            panic!("node {id} ended with {error:?}");
        }
    }
}

/// A writer of `journal` under its own uuid, claimed through `client`.
async fn writer(client: &Client<SimProviders>, journal: JournalIdentifier) -> Writer {
    let mut writer = Writer::with_uuid(journal, LeaderUuid(77));
    writer.claim(client, 0, false).await;
    writer
}

/// Write each of `records` alone: the outcome of each.
async fn write_each(
    client: &Client<SimProviders>,
    writer: &mut Writer,
    records: Vec<Vec<u8>>,
) -> Vec<WriterOutcome> {
    let mut outcomes = Vec::new();
    for record in records {
        outcomes.push(writer.write(client, vec![Value(record)], 0).await);
    }
    outcomes
}

fn all_written(outcomes: &[WriterOutcome]) {
    assert!(
        outcomes
            .iter()
            .all(|outcome| matches!(outcome, WriterOutcome::Written { .. })),
        "outcomes: {outcomes:?}"
    );
}

fn register(id: u64) -> Vec<u8> {
    register_at(id, &rig::addr(id).to_string(), 1)
}

/// Machine `id` registering at `addr` in incarnation `incarnation`.
fn register_at(id: u64, addr: &str, incarnation: u128) -> Vec<u8> {
    SystemCommand::RegisterNode {
        id: NodeId(id),
        addr: addr.to_string(),
        class: Class::All,
        locality: paros_model::machine::Locality::default(),
        name: format!("m{id}"),
        incarnation,
        machine_interface: Vec::new(),
    }
    .encode()
}

/// The registry commands that join the fleet and host [`TENANT`].
fn host_tenant() -> Vec<Vec<u8>> {
    vec![
        SystemCommand::JoinFleet {
            fleet_id: FLEET,
            cell_id: 5,
            version: paros_model::fleet::METADATA_VERSION,
            universe: None,
        }
        .encode(),
        SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(TENANT),
            tenant: TENANT,
            // The tenant is born on the initial members (#685 (no
            // founders)): each holds a slot of its control journal.
            slots: Some(
                INITIAL_MEMBERS
                    .iter()
                    .map(|id| Slot {
                        booking: 900 + id,
                        node: NodeId(*id),
                        role: Role::Acceptor,
                        target: BookingTarget::Journal(TENANT_CONTROL),
                    })
                    .collect(),
            ),
        }
        .encode(),
        SystemCommand::HostTenant {
            fleet_id: FLEET,
            tenant: TENANT,
            hosted: HostedTenant {
                control: TENANT_CONTROL.journal,
                name: b"acme".to_vec(),
                survives: paros_model::tenant::Survives::default(),

                members: INITIAL_MEMBERS.iter().copied().map(NodeId).collect(),
            },
        }
        .encode(),
    ]
}

/// The tenant command that creates [`CREATED`] on `members`.
fn create(members: &[u64]) -> Vec<u8> {
    TenantCommand::CreateJournal {
        request: 1,
        id: CREATED.journal,
        name: b"orders".to_vec(),
        writer: WriterMode::Single,
        desired: Desired::DEFAULT,
        config: AcceptorConfig::new(
            members.iter().copied().map(NodeId).collect(),
            QuorumSystem::Majority,
        ),
        tags: UserTags::none(),
        store: StoreDurability::Ordered,
    }
    .encode()
}

/// The registry command that books an acceptor slot of [`CREATED`] on
/// `node`.
fn book(booking: u64, node: u64) -> Vec<u8> {
    SystemCommand::BookCapacity {
        booking,
        node: NodeId(node),
        role: Role::Acceptor,
        target: BookingTarget::Journal(CREATED),
    }
    .encode()
}

/// Whether each of `ids` answers an `Inspect` of `journal` (it runs it).
async fn running(
    client: &Client<SimProviders>,
    ids: &[u64],
    journal: JournalIdentifier,
) -> Vec<bool> {
    let mut running = Vec::new();
    for id in ids {
        let index = client.index_of(*id).expect("a server of the client");
        running.push(client.inspect(index, journal).await.is_some());
    }
    running
}

#[test]
fn a_hosted_tenant_runs_on_the_initial_members_and_a_created_journal_where_it_is_booked() {
    let ids = [1, 2, 3, 4];
    let (ends, (hosted, created, wrote, dropped)) = control_cell(&[4], async |providers| {
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut registry = writer(&client, REGISTRY).await;
        let mut records: Vec<Vec<u8>> = ids.iter().map(|id| register(*id)).collect();
        records.extend(host_tenant());
        all_written(&write_each(&client, &mut registry, records).await);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let hosted = running(&client, &ids, TENANT_CONTROL).await;

        let mut tenant = writer(&client, TENANT_CONTROL).await;
        all_written(&write_each(&client, &mut tenant, vec![create(&[1, 2, 4])]).await);
        all_written(
            &write_each(
                &client,
                &mut registry,
                vec![book(11, 1), book(12, 2), book(13, 4)],
            )
            .await,
        );
        rig::sleep(&providers, Duration::from_secs(3)).await;
        let created = running(&client, &ids, CREATED).await;
        let mut journal = writer(&client, CREATED).await;
        let wrote = write_each(&client, &mut journal, vec![vec![1]]).await;

        let drop = SystemCommand::DropTenant {
            fleet_id: FLEET,
            tenant: TENANT,
        }
        .encode();
        all_written(&write_each(&client, &mut registry, vec![drop]).await);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let dropped = running(&client, &ids, TENANT_CONTROL).await;
        (hosted, created, wrote, dropped)
    });
    clean(ends);
    assert_eq!(
        hosted,
        vec![true, true, true, false],
        "the initial members serve the tenant control journal"
    );
    assert_eq!(
        created,
        vec![true, true, false, true],
        "the created journal runs where it is booked"
    );
    all_written(&wrote);
    assert_eq!(dropped, vec![false; 4], "a dropped tenant stops everywhere");
}

#[test]
fn a_released_slot_stops_its_journal_there_and_a_deleted_journal_stops_everywhere() {
    let ids = [1, 2, 3, 4];
    let (ends, (placed, released, deleted)) = control_cell(&[4], async |providers| {
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut registry = writer(&client, REGISTRY).await;
        let mut records: Vec<Vec<u8>> = ids.iter().map(|id| register(*id)).collect();
        records.extend(host_tenant());
        records.extend([book(11, 1), book(12, 2), book(14, 4)]);
        all_written(&write_each(&client, &mut registry, records).await);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut tenant = writer(&client, TENANT_CONTROL).await;
        all_written(&write_each(&client, &mut tenant, vec![create(&[1, 2, 4])]).await);
        rig::sleep(&providers, Duration::from_secs(3)).await;
        let placed = running(&client, &ids, CREATED).await;

        let release = SystemCommand::ReleaseCapacity { booking: 14 }.encode();
        all_written(&write_each(&client, &mut registry, vec![release]).await);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let released = running(&client, &ids, CREATED).await;

        let delete = TenantCommand::DeleteJournal {
            request: 2,
            id: CREATED.journal,
        }
        .encode();
        all_written(&write_each(&client, &mut tenant, vec![delete]).await);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let deleted = running(&client, &ids, CREATED).await;
        (placed, released, deleted)
    });
    clean(ends);
    assert_eq!(
        placed,
        vec![true, true, false, true],
        "every booked node runs it, booked before the create"
    );
    assert_eq!(
        released,
        vec![true, true, false, false],
        "the released slot's node stops it"
    );
    assert_eq!(
        deleted,
        vec![false; 4],
        "a deleted journal stops everywhere"
    );
}

#[test]
fn an_initial_members_book_follows_a_peer_the_registry_moved_and_is_cached() {
    let moved = "10.0.0.22:4500";
    let (ends, wrote, caches) = cell_of(&[], true, async |providers| {
        let client = rig::client(&providers, &INITIAL_MEMBERS);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut registry = writer(&client, REGISTRY).await;
        let mut records: Vec<Vec<u8>> = INITIAL_MEMBERS.iter().map(|id| register(*id)).collect();
        records.push(register_at(2, moved, 2));
        let wrote = write_each(&client, &mut registry, records).await;
        rig::sleep(&providers, Duration::from_secs(4)).await;
        wrote
    });
    clean(ends);
    all_written(&wrote);
    let last = caches[&1]
        .last()
        .expect("initial member 1 cached a book")
        .clone();
    assert_eq!(last.node, NodeId(1));
    assert!(
        last.machines
            .contains(&(NodeId(2), Address::parse(moved).expect("an address"))),
        "the cache holds the moved address: {last:?}"
    );
    assert!(
        caches[&1]
            .windows(2)
            .all(|w| w[0].position <= w[1].position),
        "the cache only moves forward"
    );
}

/// Fold `records` of `journal` into `follower`, from its cursor on.
fn fold_into(
    follower: &mut crate::node::system::ControlFollower<SimProviders>,
    journal: JournalIdentifier,
    records: Vec<Vec<u8>>,
) {
    let page = paros_core::replica::LogPage {
        from: paros_core::journal::Seq(follower.cursor(journal)),
        records: records.into_iter().map(Value).collect(),
        state: paros_core::journal::JournalView::default(),
    };
    let _ = follower.fold_local(journal, &page);
}

#[test]
fn a_tenants_journals_are_named_from_the_folds_a_node_follows() {
    let mut rig = Rig::new();
    let providers = rig.providers(rig::CLIENT_IP);
    let names = rig.run(async {
        let rpc = rig::runtime(&providers);
        let (mut follower, _inbox) = crate::node::system::ControlFollower::new(
            &plan(1),
            &rpc,
            &Names::literal(),
            Vec::new(),
            &DriverTunables::default(),
            CancellationToken::new(),
        )
        .expect("a valid plan");
        // The machines the tenant is born on register first (#685 (no
        // founders)).
        let mut records: Vec<Vec<u8>> = INITIAL_MEMBERS.iter().map(|id| register(*id)).collect();
        records.extend(host_tenant());
        fold_into(&mut follower, REGISTRY, records);
        assert!(
            follower.follows(TENANT_CONTROL),
            "a hosted tenant is followed"
        );
        fold_into(&mut follower, TENANT_CONTROL, vec![create(&[1, 2, 3])]);
        let name = |journal| crate::node::metered::journal_names(Some(&follower), None, journal);
        let other = JournalIdentifier::new(TENANT, JournalId(99));
        (name(TENANT_CONTROL), name(CREATED), name(other))
    });
    assert_eq!(names.0, ("acme".into(), "(control)".into()));
    assert_eq!(names.1, ("acme".into(), "orders".into()));
    assert_eq!(
        names.2,
        ("29".into(), "63".into()),
        "a journal it never created"
    );
}

/// A node never reads itself remotely: a `Read` is served from the serving
/// node's own fold, which the local follow reads already. An initial
/// member's seeds leave it out, and the only initial member of a cell asks
/// no one until it adopts a book (seed 9998526043558789590 on PR #895
/// (fix-main/acceptor-follow): that member, with no leader at boot, asked
/// only itself for the registry).
#[test]
fn a_node_is_never_its_own_seed() {
    let mut rig = Rig::new();
    let providers = rig.providers(rig::CLIENT_IP);
    let (seeds, alone, reads) = rig.run(async {
        let rpc = rig::runtime(&providers);
        let follower = |plan: &ControlPlan| {
            crate::node::system::ControlFollower::new(
                plan,
                &rpc,
                &Names::literal(),
                Vec::new(),
                &DriverTunables::default(),
                CancellationToken::new(),
            )
            .expect("a valid plan")
            .0
        };
        let seeds = follower(&plan(1)).seed_ids();
        let mut sole = plan(1);
        sole.seeds = rig::pool(&[1]);
        sole.initial_members = vec![NodeId(1)];
        let mut alone = follower(&sole);
        alone.poll_remote(&providers, |_| false);
        (seeds, alone.seed_ids(), alone.reads_in_flight())
    });
    assert_eq!(
        seeds,
        vec![NodeId(2), NodeId(3)],
        "the other initial members"
    );
    assert!(alone.is_empty(), "the only initial member keeps no seed");
    assert_eq!(reads, 0, "it reads nothing remotely");
}

/// The registry commands that host [`TENANT`] born on machine `on` alone.
fn host_tenant_on(on: u64) -> Vec<Vec<u8>> {
    vec![
        SystemCommand::JoinFleet {
            fleet_id: FLEET,
            cell_id: 5,
            version: paros_model::fleet::METADATA_VERSION,
            universe: None,
        }
        .encode(),
        SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(TENANT),
            tenant: TENANT,
            slots: Some(vec![Slot {
                booking: 900,
                node: NodeId(on),
                role: Role::Acceptor,
                target: BookingTarget::Journal(TENANT_CONTROL),
            }]),
        }
        .encode(),
        SystemCommand::HostTenant {
            fleet_id: FLEET,
            tenant: TENANT,
            hosted: HostedTenant {
                control: TENANT_CONTROL.journal,
                name: b"acme".to_vec(),
                survives: paros_model::tenant::Survives::default(),
                members: vec![NodeId(on)],
            },
        }
        .encode(),
    ]
}

/// #685 (no founders): a tenant born on an admitted machine runs its
/// control journal on no seed, so a node follows it from the machines it
/// is booked on too; a tenant born on a seed adds no target.
#[test]
fn a_tenant_control_journal_is_followed_from_its_booked_machines() {
    let seeds = |on: u64| {
        let mut rig = Rig::new();
        let providers = rig.providers(rig::CLIENT_IP);
        rig.run(async {
            let rpc = rig::runtime(&providers);
            let (mut follower, _inbox) = crate::node::system::ControlFollower::new(
                &plan(1),
                &rpc,
                &Names::literal(),
                Vec::new(),
                &DriverTunables::default(),
                CancellationToken::new(),
            )
            .expect("a valid plan");
            let mut records: Vec<Vec<u8>> = [1, 2, 3, 4].iter().map(|id| register(*id)).collect();
            records.extend(host_tenant_on(on));
            fold_into(&mut follower, REGISTRY, records);
            assert!(follower.follows(TENANT_CONTROL), "the tenant is followed");
            assert!(
                follower.member_seeds(REGISTRY).is_empty(),
                "the registry is read from the seeds alone"
            );
            follower
                .member_seeds(TENANT_CONTROL)
                .iter()
                .map(|c| c.address().to_string())
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(seeds(4), vec![rig::addr(4).to_string()]);
    assert!(seeds(2).is_empty(), "a seed is read as a seed already");
}
