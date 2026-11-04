//! A machine's start as `parosd` runs it (#246): `run_machine` on a
//! simulated disk and network, from an empty disk, a restart, and the
//! refusals the operator must act on. The waiting loop is in
//! `wait/tests.rs`.

use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{LocalStorageProvider as _, LocalTimeProvider as _, Providers as _};
use moonpool_sim::SimProviders;
use paros_core::{Ballot, NodeId};
use paros_net::transport::Transport;
use paros_net::well_known;
use paros_observe::audit::NoAudit;
use paros_tunables::DriverTunables;
use paros_wire::machine as wire;
use paros_wire::methods::{CellInitRpc, IdentifyRpc};
use tokio_util::sync::CancellationToken;

use super::{MachineAddresses, MachineError, MachineSettings, run_machine};
use crate::rig::{Rig, client, compact};
use crate::{AdminPin, Class, Locality, MachineRecord, ProviderDisk};
use paros_model::address::{Address, Names};

const IP: &str = "10.0.2.1";
const ROOT: &str = "/machine";

fn listen() -> SocketAddr {
    format!("{IP}:4700").parse().expect("a test address")
}

fn settings(class: Option<Class>, name: &str) -> MachineSettings {
    MachineSettings {
        class,
        locality: Locality::default(),
        name: name.into(),
        pin: AdminPin::open(),
        metrics: paros_observe::metrics::Metrics::off(),
        frontend: None,
    }
}

type Disk = ProviderDisk<<SimProviders as moonpool_core::Providers>::Storage>;

fn disk_of(providers: &SimProviders) -> Disk {
    ProviderDisk::new(providers.storage().clone(), ROOT, compact())
}

fn tunables() -> DriverTunables {
    DriverTunables::default()
}

/// The address of the machine at `ip`.
fn at(ip: &str) -> SocketAddr {
    format!("{ip}:4700").parse().expect("a test address")
}

/// Ask the machine at `to` who it is, from `providers`.
async fn identify_at(providers: &SimProviders, to: SocketAddr) -> wire::IdentifyAck {
    let rpc = client(providers);
    well_known::<SimProviders, IdentifyRpc>(&rpc, to)
        .try_get_reply(&wire::Identify::default())
        .await
        .expect("the machine answers")
}

/// Run the machine at [`IP`] under `settings` until `client` ends, then
/// shut it down; what its run ended with, and the client's answer.
async fn start<T>(
    providers: SimProviders,
    settings: MachineSettings,
    client: impl AsyncFnOnce(SimProviders) -> T,
) -> (Result<(), MachineError>, T) {
    let shutdown = CancellationToken::new();
    let tunables = tunables();
    // Boxed: a machine's whole run is a large future.
    let machine = Box::pin(run_machine(
        providers.clone(),
        disk_of(&providers),
        |_| NoAudit,
        &settings,
        MachineAddresses::new(listen(), None).expect("a dialable address"),
        Names::literal(),
        tunables,
        Transport::Plaintext,
        shutdown.clone(),
    ));
    let answer = async {
        let answer = client(providers.clone()).await;
        shutdown.cancel();
        answer
    };
    futures::join!(machine, answer)
}

/// Wait a moment, then ask the machine who it is.
async fn identify(providers: SimProviders) -> wire::IdentifyAck {
    let _ = providers.time().sleep(Duration::from_secs(1)).await;
    identify_at(&providers, listen()).await
}

/// The record on the disk of `providers`.
async fn record(providers: &SimProviders) -> MachineRecord {
    let text = disk_of(providers)
        .read_record()
        .await
        .expect("readable")
        .expect("a record");
    MachineRecord::parse(&text).expect("a valid record")
}

async fn write_record(providers: &SimProviders, record: &MachineRecord) {
    disk_of(providers)
        .write_record(&record.render())
        .await
        .expect("written");
}

#[test]
fn an_empty_disk_formats_a_new_machine_that_waits() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let (end, ack, held) = rig.run(async {
        let (end, ack) = start(providers.clone(), settings(None, "m0"), identify).await;
        (end, ack, record(&providers).await)
    });
    end.expect("a clean shutdown");
    assert_ne!(held.node_id, NodeId(0), "a minted identity");
    assert_eq!(
        ack.node_id, held.node_id.0,
        "it answers as the machine it minted"
    );
    assert_eq!(ack.cell_id, 0, "it waits for a cell");
    assert_eq!(held.class, Class::All, "an unset class formats every role");
    assert_eq!(held.name, "m0");
    assert_eq!(held.promised, Ballot::default());
}

#[test]
fn a_restart_keeps_the_identity_and_records_a_new_name_and_class() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let (first, second) = rig.run(async {
        let (end, _) = start(providers.clone(), settings(None, "m0"), identify).await;
        end.expect("a clean shutdown");
        let first = record(&providers).await;
        // Never joined a cell: it may take fewer roles.
        let (end, _) = start(
            providers.clone(),
            settings(Some(Class::Stateless), "m1"),
            identify,
        )
        .await;
        end.expect("a clean shutdown");
        (first, record(&providers).await)
    });
    assert_eq!(second.node_id, first.node_id, "the same machine");
    assert_eq!(second.name, "m1");
    assert_eq!(second.class, Class::Stateless);
}

#[test]
fn a_restart_records_each_changed_setting_alone() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let records = rig.run(async {
        let mut records = Vec::new();
        for changed in [
            settings(None, "m0"),
            settings(Some(Class::Stateless), "m0"),
            settings(None, "m1"),
            MachineSettings {
                locality: Locality::zone("z1"),
                ..settings(None, "m1")
            },
        ] {
            let (end, _) = start(providers.clone(), changed, identify).await;
            end.expect("a clean shutdown");
            records.push(record(&providers).await);
        }
        records
    });
    assert_eq!(records[1].class, Class::Stateless, "the class alone");
    assert_eq!(records[2].name, "m1", "the name alone");
    assert_eq!(
        records[2].class,
        Class::Stateless,
        "an unset class keeps the record's"
    );
    assert_eq!(
        records[3].locality,
        Locality::zone("z1"),
        "the locality alone"
    );
}

#[test]
fn a_formed_machine_never_takes_fewer_roles() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let end = rig.run(async {
        let (end, _) = start(providers.clone(), settings(None, "m0"), identify).await;
        end.expect("a clean shutdown");
        let mut held = record(&providers).await;
        // A plan with no promise beside it and no store on the disk: the
        // plan alone says it joined.
        let me = (held.node_id, Address::from(listen()));
        held.plan = Some((
            Ballot {
                round: 3,
                node: held.node_id,
            },
            crate::rig::plan(9, vec![me]),
        ));
        write_record(&providers, &held).await;
        let (end, ()) = start(
            providers.clone(),
            settings(Some(Class::Stateless), "m0"),
            async |_| (),
        )
        .await;
        end
    });
    assert!(matches!(end, Err(MachineError::Refused(_))), "{end:?}");
}

#[test]
fn a_machine_promised_in_cell_init_never_takes_fewer_roles() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let end = rig.run(async {
        let (end, _) = start(providers.clone(), settings(None, "m0"), identify).await;
        end.expect("a clean shutdown");
        let mut held = record(&providers).await;
        held.promised = Ballot {
            round: 3,
            node: NodeId(9),
        };
        write_record(&providers, &held).await;
        let (end, ()) = start(
            providers.clone(),
            settings(Some(Class::Stateless), "m0"),
            async |_| (),
        )
        .await;
        end
    });
    let Err(MachineError::Refused(error)) = end else {
        panic!("refused: {end:?}");
    };
    assert!(error.contains("PAROS_ROLES=all"), "{error}");
}

#[test]
fn stores_without_a_record_are_amnesia_and_refused() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let (end, ()) = rig.run(async {
        providers
            .storage()
            .create_dir_all(&format!("{ROOT}/journals/1/2"))
            .await
            .expect("a directory");
        start(providers.clone(), settings(None, "m0"), async |_| ()).await
    });
    let Err(MachineError::Refused(error)) = end else {
        panic!("refused: {end:?}");
    };
    assert!(error.contains("amnesia"), "{error}");
}

#[test]
fn a_damaged_record_is_refused() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let (end, ()) = rig.run(async {
        disk_of(&providers)
            .write_record("not a record")
            .await
            .expect("written");
        start(providers.clone(), settings(None, "m0"), async |_| ()).await
    });
    let Err(MachineError::Refused(error)) = end else {
        panic!("refused: {end:?}");
    };
    assert!(error.starts_with("machine record: "), "{error}");
}

#[test]
fn an_invalid_locality_is_refused_before_the_disk_is_touched() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let (end, held) = rig.run(async {
        let mut bad = settings(None, "m0");
        bad.locality = Locality::zone("a,b");
        let (end, ()) = start(providers.clone(), bad, async |_| ()).await;
        (
            end,
            disk_of(&providers).read_record().await.expect("readable"),
        )
    });
    assert!(matches!(end, Err(MachineError::Refused(_))), "{end:?}");
    assert!(held.is_none(), "no record was written");
}

#[test]
fn a_lone_machine_forms_its_cell_and_restarts_formed() {
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let (ack, formed, again) = rig.run(async {
        let (end, ack) = start(providers.clone(), settings(None, "m0"), async |p| {
            let _ = p.time().sleep(Duration::from_secs(1)).await;
            let rpc = client(&p);
            let request = wire::CellInit {
                members: vec![listen().to_string()],
                ..wire::CellInit::default()
            };
            well_known::<SimProviders, CellInitRpc>(&rpc, listen())
                .try_get_reply(&request)
                .await
                .expect("the machine answers")
        })
        .await;
        end.expect("a clean shutdown");
        let formed = record(&providers).await;
        let (end, again) = start(providers.clone(), settings(None, "m0"), identify).await;
        end.expect("a clean shutdown");
        (ack, formed, again)
    });
    assert_eq!(ack.refusal, "");
    let plan = formed.formed().expect("the record holds the cell");
    assert_eq!(plan.cell_id, ack.cell_id);
    assert_eq!(
        plan.members,
        vec![(formed.node_id, Address::from(listen()))]
    );
    assert_eq!(again.cell_id, ack.cell_id, "a restart serves its cell");
}

#[test]
fn a_wildcard_address_nobody_can_dial_is_refused() {
    let wildcard: SocketAddr = "0.0.0.0:4700".parse().expect("an address");
    assert!(MachineAddresses::new(wildcard, None).is_err());
    let advertised = Address::parse("db1.example:4700").expect("a name");
    let ok = MachineAddresses::new(wildcard, Some(advertised.clone())).expect("dialable");
    assert_eq!(ok.advertise, advertised);
    assert_eq!(ok.listen, wildcard);
    let bound = MachineAddresses::new(listen(), None).expect("dialable");
    assert_eq!(
        bound.advertise,
        Address::from(listen()),
        "advertises what it binds"
    );
    let wild = Address::from(wildcard);
    assert!(MachineAddresses::new(listen(), Some(wild)).is_err());
}

#[test]
fn a_machine_error_shows_its_reason() {
    for error in [
        MachineError::Invalid("bad".into()),
        MachineError::Refused("bad".into()),
        MachineError::Storage("bad".into()),
    ] {
        assert_eq!(error.to_string(), "bad");
    }
}

#[test]
fn a_member_that_missed_the_decree_is_taught_by_its_peer() {
    let mut rig = Rig::new();
    let machines: Vec<SimProviders> = FLEET.iter().map(|ip| rig.providers(ip)).collect();
    let operator = rig.providers("10.0.9.9");
    let (taught, cell_id) = rig.run(fleet(&machines, async |shutdown| {
        let _ = operator.time().sleep(Duration::from_secs(1)).await;
        let mut members = Vec::new();
        for ip in FLEET {
            let ack = identify_at(&operator, at(ip)).await;
            members.push((NodeId(ack.node_id), Address::from(at(ip))));
        }
        members.sort_unstable();
        let plan = crate::rig::plan(77, members.clone());
        // Only the first machine hears the vote: the second missed it.
        let ballot = Ballot {
            round: 5,
            node: members[0].0,
        };
        let rpc = client(&operator);
        let ack = well_known::<SimProviders, paros_wire::methods::FormCellRpc>(&rpc, at(FLEET[0]))
            .try_get_reply(&plan.form_request(ballot))
            .await
            .expect("the machine answers");
        assert!(ack.formed, "{ack:?}");
        // The teacher re-sends the vote each election renewal period.
        let _ = operator.time().sleep(Duration::from_secs(10)).await;
        let taught = identify_at(&operator, at(FLEET[1])).await;
        shutdown.cancel();
        (taught, plan.cell_id)
    }));
    assert_eq!(
        taught.cell_id, cell_id,
        "the second machine formed the cell"
    );
}

/// Send `request` over `M` to the machine at [`listen`].
async fn ask<M: paros_wire::methods::WellKnownMethod>(
    providers: &SimProviders,
    request: M::Request,
) -> M::Reply {
    let rpc = client(providers);
    well_known::<SimProviders, M>(&rpc, listen())
        .try_get_reply(&request)
        .await
        .expect("the machine answers")
}

#[test]
fn a_formed_machine_answers_views_and_resolve_from_its_cell() {
    use paros_wire::view;
    let mut rig = Rig::new();
    let providers = rig.providers(IP);
    let admin = Some(view::Scope {
        kind: Some(view::scope::Kind::Admin(view::AdminScope {})),
    });
    let (end, (init, replies, resolved)) =
        rig.run(start(providers.clone(), settings(None, "m0"), async |p| {
            let _ = p.time().sleep(Duration::from_secs(1)).await;
            let request = wire::CellInit {
                members: vec![listen().to_string()],
                ..wire::CellInit::default()
            };
            let init = ask::<CellInitRpc>(&p, request).await;
            // The cell elects its coordinator and writes its registry.
            let _ = p.time().sleep(Duration::from_secs(20)).await;
            let mut replies = Vec::new();
            for (scope, query) in [
                (
                    admin.clone(),
                    Some(view::view_request::Query::Cell(view::CellQuery {})),
                ),
                (
                    admin.clone(),
                    Some(view::view_request::Query::Balance(view::BalanceQuery {})),
                ),
                (
                    admin.clone(),
                    Some(view::view_request::Query::Universe(view::UniverseQuery {})),
                ),
                (
                    admin.clone(),
                    Some(view::view_request::Query::Tenant(view::TenantQuery {
                        name: b"nobody".to_vec(),
                    })),
                ),
                (admin.clone(), None),
                (
                    None,
                    Some(view::view_request::Query::Cell(view::CellQuery {})),
                ),
            ] {
                let reply =
                    ask::<paros_wire::methods::ViewRpc>(&p, view::ViewRequest { scope, query })
                        .await;
                replies.push((reply.refusal, reply.cell_id));
            }
            let resolved = ask::<paros_wire::methods::ResolveRpc>(
                &p,
                wire::Resolve {
                    tenant: b"nobody".to_vec(),
                },
            )
            .await;
            (init, replies, resolved)
        }));
    end.expect("a clean shutdown");
    assert_eq!(init.refusal, "");
    let cell = init.cell_id;
    let refusals: Vec<&str> = replies.iter().map(|(r, _)| r.as_str()).collect();
    assert_eq!(
        refusals,
        [
            "",
            "",
            // `cell init` alone writes no fleet directory: the client's
            // `initialize` does (#229).
            "not_universe",
            "unknown_tenant",
            "malformed",
            "malformed"
        ]
    );
    assert_eq!(replies[0].1, cell, "the cell view names its cell");
    assert_eq!(replies[1].1, cell, "the balance view names its cell");
    assert_eq!(resolved.refusal, "no_universe");
    assert_eq!(resolved.cell_id, cell);
}

/// Allows every call but one with the token `bad`.
struct NotBad;

impl paros_roles::frontend::Authz for NotBad {
    fn authorize(
        &self,
        token: &[u8],
        _: &paros_roles::frontend::Request<'_>,
    ) -> Result<(), paros_roles::frontend::Denial> {
        if token == b"bad" {
            Err(paros_roles::frontend::Denial::Forbidden)
        } else {
            Ok(())
        }
    }
}

/// `ResolveEntry` of `tenant` (and `journal`) with `token` at the machine
/// at `ip`.
async fn resolve_entry(
    operator: &SimProviders,
    ip: &str,
    (tenant, journal, token): (&str, &str, &[u8]),
) -> wire::ResolveAck {
    let rpc = client(operator);
    well_known::<SimProviders, paros_wire::methods::ResolveEntryRpc>(&rpc, at(ip))
        .try_get_reply(&wire::ResolveEntry {
            token: token.to_vec(),
            tenant: tenant.into(),
            journal: journal.into(),
        })
        .await
        .expect("the machine answers")
}

/// Each [`FLEET`] machine's frontend's answer to a `ResolveEntry` of
/// `t1`, of a journal in it, of an unknown tenant, with a denied token, and
/// of no name: its refusal, tenant, the tenant's cell and its endpoint's
/// size.
async fn entries(operator: &SimProviders) -> Vec<(String, u64, u64, usize)> {
    let mut entries = Vec::new();
    for ip in FLEET {
        for request in [
            ("t1", "", &b""[..]),
            ("t1", "orders", b""),
            ("nobody", "", b""),
            ("t1", "", b"bad"),
            ("", "", b""),
        ] {
            let ack = resolve_entry(operator, ip, request).await;
            entries.push((
                ack.refusal,
                ack.tenant,
                ack.tenant_cell,
                ack.tenant_cell_endpoint.len(),
            ));
        }
    }
    entries
}

/// The admitted machine's answers to the cell decree: it refuses both
/// phases as a machine in a cell.
async fn admitted_decree(operator: &SimProviders) -> [(String, u64, u64, usize); 2] {
    let rpc = client(operator);
    let prepare =
        well_known::<SimProviders, paros_wire::methods::PrepareCellRpc>(&rpc, at(FLEET[1]))
            .try_get_reply(&wire::PrepareCell::default())
            .await
            .expect("answers");
    let other = crate::rig::plan(5, vec![(NodeId(1), Address::from(at(FLEET[1])))]);
    let ballot = Ballot {
        round: 1,
        node: NodeId(1),
    };
    let form = well_known::<SimProviders, paros_wire::methods::FormCellRpc>(&rpc, at(FLEET[1]))
        .try_get_reply(&other.form_request(ballot))
        .await
        .expect("answers");
    [
        (prepare.refusal, u64::from(prepare.promised), 0, 0),
        (form.refusal, u64::from(form.formed), 0, 0),
    ]
}

/// The two machines of the fleet tests.
const FLEET: [&str; 2] = ["10.0.2.1", "10.0.2.2"];

/// Run the [`FLEET`] machines on their disks until `operate` ends.
async fn fleet<T>(
    machines: &[SimProviders],
    operate: impl AsyncFnOnce(CancellationToken) -> T,
) -> T {
    let shutdown = CancellationToken::new();
    // Both serve the frontend role (#723 (frontend as a parosd role)).
    let all = ["m0", "m1"].map(|name| MachineSettings {
        frontend: Some(paros_roles::frontend::FrontendRole {
            authz: std::sync::Arc::new(NotBad),
            epoch: Duration::ZERO,
            client: paros_client::ClientTunables::default(),
        }),
        ..settings(None, name)
    });
    let runs = machines.iter().zip(FLEET).zip(&all).map(|((p, ip), s)| {
        Box::pin(run_machine(
            p.clone(),
            disk_of(p),
            |_| NoAudit,
            s,
            MachineAddresses::new(at(ip), None).expect("a dialable address"),
            Names::literal(),
            tunables(),
            Transport::Plaintext,
            shutdown.clone(),
        ))
    });
    let (ends, out) = futures::join!(futures::future::join_all(runs), operate(shutdown.clone()));
    for end in ends {
        end.expect("a clean shutdown");
    }
    out
}

/// A client of the cell over `servers`.
fn cell_client(
    operator: &SimProviders,
    rpc: &moonpool_rpc::RpcHandle<SimProviders>,
    servers: &[(u64, Address)],
) -> paros_client::Client<SimProviders> {
    let servers = servers
        .iter()
        .map(|(id, addr)| paros_client::Server {
            id: *id,
            node: paros_net::NodeClient::named(rpc, Names::literal(), addr.clone()),
        })
        .collect();
    paros_client::Client::new(operator, servers, paros_client::ClientTunables::default())
}

/// `init` over the [`FLEET`] (one founder, one admitted machine), then
/// the `users` tenant `t1` created.
async fn init_fleet(
    operator: &SimProviders,
) -> (
    paros_client::initialize::Initialized,
    paros_core::journal::TenantId,
) {
    use paros_client::initialize::{InitParams, InitRun, initialize};
    let _ = operator.time().sleep(Duration::from_secs(1)).await;
    let rpc = client(operator);
    let listed: Vec<Address> = FLEET.iter().map(|ip| Address::from(at(ip))).collect();
    let run = initialize(
        operator,
        &rpc,
        &Names::literal(),
        &listed,
        |servers| cell_client(operator, &rpc, servers),
        InitParams {
            patience: Duration::from_secs(30),
            fleet_id: 41,
            leader_seed: 43,
            names: ("u".into(), "c".into()),
            token: Vec::new(),
            root_key: None,
            survive_machines: 0,
            idle: false,
        },
    )
    .await;
    let InitRun::Initialized(done) = run else {
        panic!("initialized: {run:?}");
    };
    let cell = cell_client(operator, &rpc, &done.servers);
    let mut session = paros_client::fleet::FleetSession::new(
        done.journals,
        47,
        paros_model::registry::Registry::new(done.members.iter().copied().map(NodeId)),
        cell.tunables().checkpoint_policy(),
    )
    .expect("a fleet session");
    let draw = paros_core::journal::JournalIdentifier::new(
        paros_core::journal::TenantId(77),
        paros_core::journal::JournalId(78),
    );
    let created = session
        .create_tenant(&cell, 0, b"t1", [draw], Duration::from_secs(30))
        .await
        .outcome;
    let paros_client::fleet::Step::Done { result, .. } = created else {
        panic!("created: {created:?}");
    };
    (done, result)
}

/// `Resolve` of `tenant` at the machine at `ip`.
async fn resolve(operator: &SimProviders, ip: &str, tenant: &[u8]) -> wire::ResolveAck {
    let rpc = client(operator);
    well_known::<SimProviders, paros_wire::methods::ResolveRpc>(&rpc, at(ip))
        .try_get_reply(&wire::Resolve {
            tenant: tenant.to_vec(),
        })
        .await
        .expect("the machine answers")
}

/// An admin `View` of `query` at the founder.
async fn admin_view(
    operator: &SimProviders,
    query: paros_wire::view::view_request::Query,
) -> paros_wire::view::ViewReply {
    use paros_wire::view;
    let rpc = client(operator);
    well_known::<SimProviders, paros_wire::methods::ViewRpc>(&rpc, at(FLEET[0]))
        .try_get_reply(&view::ViewRequest {
            scope: Some(view::Scope {
                kind: Some(view::scope::Kind::Admin(view::AdminScope {})),
            }),
            query: Some(query),
        })
        .await
        .expect("the founder answers")
}

#[test]
fn init_forms_a_fleet_that_every_machine_resolves_and_a_restart_keeps() {
    use paros_wire::view::{TenantQuery, UniverseQuery, view_request::Query};
    let mut rig = Rig::new();
    let machines: Vec<SimProviders> = FLEET.iter().map(|ip| rig.providers(ip)).collect();
    let operator = rig.providers("10.0.9.9");
    let (done, tenant, before, resolved, (views, entries), after) = rig.run(async {
        let (done, tenant, before, resolved, views) = fleet(&machines, async |shutdown| {
            let (done, tenant) = init_fleet(&operator).await;
            // The admitted machine follows the registry.
            let _ = operator.time().sleep(Duration::from_secs(10)).await;
            let mut before = Vec::new();
            let mut resolved = Vec::new();
            for ip in FLEET {
                before.push(identify_at(&operator, at(ip)).await);
                for name in [&b"t1"[..], b"nobody", b""] {
                    let ack = resolve(&operator, ip, name).await;
                    resolved.push((ack.refusal, ack.tenant, ack.machines.len()));
                }
            }
            let mut entries = entries(&operator).await;
            entries.extend(admitted_decree(&operator).await);
            let views = [
                admin_view(&operator, Query::Universe(UniverseQuery {})).await,
                admin_view(
                    &operator,
                    Query::Tenant(TenantQuery {
                        name: b"t1".to_vec(),
                    }),
                )
                .await,
            ];
            shutdown.cancel();
            (done, tenant, before, resolved, (views, entries))
        })
        .await;
        // Both restart from their disks into the cell.
        let after = fleet(&machines, async |shutdown| {
            let _ = operator.time().sleep(Duration::from_secs(5)).await;
            let mut after = Vec::new();
            for ip in FLEET {
                after.push(identify_at(&operator, at(ip)).await);
            }
            shutdown.cancel();
            after
        })
        .await;
        (done, tenant, before, resolved, views, after)
    });
    assert_eq!(done.members.len(), 1, "one founder");
    assert_eq!(
        done.admitted.len(),
        1,
        "the other listed machine is admitted"
    );
    assert_eq!(tenant.0, 77);
    let cell = done.journals.cell_id;
    for identity in before.iter().chain(&after) {
        assert_eq!(identity.cell_id, cell, "{identity:?}");
    }
    assert!(
        before[1].registry_position > 0,
        "the admitted machine follows the registry"
    );
    assert!(
        after[1].registry_position >= before[1].registry_position,
        "a restart starts from its cached fold"
    );
    let ok = |machines| (String::new(), 77, machines);
    let unknown = ("unknown_tenant".to_string(), 0, 0);
    let internal = ("internal".to_string(), 0, 0);
    assert_eq!(
        resolved,
        vec![
            ok(2),
            unknown.clone(),
            internal.clone(),
            ok(2),
            unknown,
            internal
        ],
        "both machines resolve the tenant over the cell's two machines"
    );
    let redirect = (String::new(), 77, cell, 1);
    let refused = |label: &str| (label.to_string(), 0, 0, 0);
    let per_machine = vec![
        redirect.clone(),
        redirect,
        refused("unknown_tenant"),
        refused("forbidden"),
        refused("malformed"),
    ];
    let decree = vec![refused("in_cell"), refused("other_cell")];
    assert_eq!(
        entries,
        [per_machine.clone(), per_machine, decree].concat(),
        "every frontend redirects to the tenant's cell"
    );
    assert_eq!(views[0].refusal, "", "the universe view");
    assert_eq!(views[1].refusal, "", "the tenant view");
}

#[test]
fn a_founding_member_links_its_own_matchmaker_where_it_advertises_now() {
    use paros_core::membership::MatchmakerId;
    let book = vec![
        (NodeId(1), "old:1".to_string()),
        (NodeId(2), "b:2".to_string()),
    ];
    let cached = vec![(NodeId(3), Address::parse("c:3").expect("an address"))];
    let links = super::founding_links(&book, (NodeId(1), "new:1"), &cached);
    assert_eq!(
        links,
        vec![
            (MatchmakerId(1), "new:1".to_string()),
            (MatchmakerId(2), "b:2".to_string()),
            (MatchmakerId(3), "c:3".to_string()),
        ]
    );
}
