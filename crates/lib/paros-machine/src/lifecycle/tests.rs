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
    let ips = ["10.0.2.1", "10.0.2.2"];
    let machines: Vec<SimProviders> = ips.iter().map(|ip| rig.providers(ip)).collect();
    let operator = rig.providers("10.0.9.9");
    let shutdown = CancellationToken::new();
    let first = settings(None, "m0");
    let second = settings(None, "m1");
    let (taught, cell_id) = rig.run(async {
        let runs = machines
            .iter()
            .zip(ips)
            .zip([&first, &second])
            .map(|((p, ip), s)| {
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
        let operate = async {
            let _ = operator.time().sleep(Duration::from_secs(1)).await;
            let mut members = Vec::new();
            for ip in ips {
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
            let ack =
                well_known::<SimProviders, paros_wire::methods::FormCellRpc>(&rpc, at(ips[0]))
                    .try_get_reply(&plan.form_request(ballot))
                    .await
                    .expect("the machine answers");
            assert!(ack.formed, "{ack:?}");
            // The teacher re-sends the vote each election renewal period.
            let _ = operator.time().sleep(Duration::from_secs(10)).await;
            let taught = identify_at(&operator, at(ips[1])).await;
            shutdown.cancel();
            (taught, plan.cell_id)
        };
        let (ends, out) = futures::join!(futures::future::join_all(runs), operate);
        for end in ends {
            end.expect("a clean shutdown");
        }
        out
    });
    assert_eq!(
        taught.cell_id, cell_id,
        "the second machine formed the cell"
    );
}
