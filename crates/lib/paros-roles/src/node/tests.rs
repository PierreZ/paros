//! The node loop as a deployment runs it: `run_node` over the library's
//! in-memory store, on a simulated network, called by the library's client.
//! A test pins what a client sees (a write read back, a refusal, a clean
//! stop); the sweep proves the protocol under faults.

use std::time::Duration;

use moonpool_sim::SimProviders;
use paros_client::{ClaimOutcome, Reader, ReaderOutcome, Writer, WriterOutcome};
use paros_core::journal::{JournalView, LeaderUuid, Seq};
use paros_core::{Config, NodeId, Value};
use paros_net::transport::Transport;
use paros_observe::audit::NoAudit;
use paros_storage::MemStorage;
use tokio_util::sync::CancellationToken;

use super::{BootKind, DriverTunables, RunError, run_node};
use crate::rig::{self, Rig, journal, pool};

const U: LeaderUuid = LeaderUuid(11);

/// The configuration of node `id` in a plain cell of `ids`.
fn config(id: u64, ids: &[u64]) -> Config {
    let mut config = Config::new(NodeId(id), journal());
    config.peers = ids.iter().map(|id| NodeId(*id)).collect();
    config
}

/// Run a plain cell of `ids` (each node on its own machine, a first boot
/// over an empty in-memory store) until `body` ends, then stop every node.
/// `body` is given the client machine's providers and each node's own
/// stop. Each node's exit, and `body`'s answer.
fn cell<T>(
    ids: &[u64],
    body: impl AsyncFnOnce(SimProviders, Vec<CancellationToken>) -> T,
) -> (Vec<Result<(), RunError>>, T) {
    let stores = ids
        .iter()
        .map(|id| (MemStorage::new(config(*id, ids)), BootKind::FirstBoot))
        .collect();
    cell_over(ids, stores, body)
}

/// [`cell`] over the given store and boot claim of each node.
fn cell_over<T>(
    ids: &[u64],
    stores: Vec<(MemStorage, BootKind)>,
    body: impl AsyncFnOnce(SimProviders, Vec<CancellationToken>) -> T,
) -> (Vec<Result<(), RunError>>, T) {
    let mut rig = Rig::new();
    let client = rig.providers(rig::CLIENT_IP);
    let stops: Vec<CancellationToken> = ids.iter().map(|_| CancellationToken::new()).collect();
    let nodes: Vec<_> = ids
        .iter()
        .zip(stores)
        .zip(&stops)
        .map(|((id, (storage, boot)), stop)| {
            let providers = rig.node(*id);
            let members = pool(ids);
            let shutdown = stop.clone();
            let id = *id;
            Box::pin(async move {
                run_node(
                    providers,
                    storage,
                    rig::addr(id).to_string(),
                    members,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    boot,
                    DriverTunables::default(),
                    Transport::Plaintext,
                    shutdown,
                    &NoAudit,
                )
                .await
            })
        })
        .collect();
    let all = stops.clone();
    rig.run(async move {
        let answer = async {
            let answer = body(client, stops).await;
            for stop in &all {
                stop.cancel();
            }
            answer
        };
        futures::join!(futures::future::join_all(nodes), answer)
    })
}

/// Every node of `ends` stopped cleanly.
fn clean(ends: Vec<Result<(), RunError>>) {
    for end in ends {
        end.expect("a clean stop");
    }
}

fn one(byte: u8) -> Vec<Value> {
    vec![Value(vec![byte])]
}

#[test]
fn a_plain_cell_takes_a_claim_and_a_write_and_reads_it_back() {
    let ids = [1, 2, 3];
    let (ends, (claimed, wrote, page)) = cell(&ids, async |providers, _| {
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut writer = Writer::with_uuid(journal(), U);
        let claimed = writer.claim(&client, 0, false).await;
        let wrote = writer.write(&client, one(7), 0).await;
        let mut reader = Reader::new(journal(), 0);
        let page = reader.next(&client, 1).await;
        (claimed, wrote, page)
    });
    clean(ends);
    assert!(
        matches!(claimed, ClaimOutcome::Won { .. }),
        "claimed: {claimed:?}"
    );
    assert!(
        matches!(
            wrote,
            WriterOutcome::Written {
                seq: 0,
                count: 1,
                ..
            }
        ),
        "wrote: {wrote:?}"
    );
    let ReaderOutcome::Records {
        from,
        records,
        state,
    } = page
    else {
        panic!("read: {page:?}");
    };
    assert_eq!(from, 0);
    assert_eq!(records, vec![vec![7]]);
    assert_eq!(
        state,
        JournalView {
            leader: Some(U),
            next_seq: Seq(1),
            first_seq: Seq(0),
        }
    );
}

const V: LeaderUuid = LeaderUuid(22);

#[test]
fn a_truncated_prefix_reads_as_a_gap_and_a_new_claim_supersedes_the_old_writer() {
    let ids = [1, 2, 3];
    let (ends, (truncated, gap, claimed, stale)) = cell(&ids, async |providers, _| {
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut writer = Writer::with_uuid(journal(), U);
        writer.claim(&client, 0, false).await;
        for byte in 0..4 {
            writer.write(&client, one(byte), 0).await;
        }
        let truncated = writer.truncate(&client, 2, 0).await;
        let gap = Reader::new(journal(), 0).next(&client, 2).await;
        let mut other = Writer::with_uuid(journal(), V);
        let claimed = other.claim_over(&client, 1, Some(U)).await;
        let stale = writer.write(&client, one(9), 2).await;
        (truncated, gap, claimed, stale)
    });
    clean(ends);
    assert!(
        matches!(
            truncated,
            Some(paros_client::TruncateOutcome::Applied { .. })
        ),
        "truncated: {truncated:?}"
    );
    let ReaderOutcome::Gap {
        from, resumed_at, ..
    } = gap
    else {
        panic!("read below the floor: {gap:?}");
    };
    assert_eq!((from, resumed_at), (0, 2));
    let ClaimOutcome::Won { state } = claimed else {
        panic!("claimed over U: {claimed:?}");
    };
    assert_eq!(state.leader, Some(V));
    assert_eq!(state.next_seq, Seq(4));
    let WriterOutcome::Superseded { state } = stale else {
        panic!("the old writer: {stale:?}");
    };
    assert_eq!(state.leader, Some(V), "the old writer learns its successor");
}

#[test]
fn a_plain_deployment_refuses_a_reconfiguration_and_answers_inspect() {
    let ids = [1, 2, 3];
    let (ends, (reconfigured, inspected, unknown)) = cell(&ids, async |providers, _| {
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut writer = Writer::with_uuid(journal(), U);
        writer.claim(&client, 0, false).await;
        let leader = client.leader().unwrap_or(0);
        let reconfigured = client
            .reconfigure(
                journal(),
                &[1, 2],
                paros_core::membership::QuorumSystem::default(),
                leader,
            )
            .await;
        let inspected = client.inspect(leader, journal()).await;
        let other = paros_core::journal::JournalIdentifier::new(
            paros_core::journal::TenantId(5),
            paros_core::journal::JournalId(10),
        );
        let unknown = Reader::new(other, 0).next(&client, 0).await;
        (reconfigured, inspected, unknown)
    });
    clean(ends);
    assert!(
        matches!(
            reconfigured,
            paros_client::ReconfigureOutcome::Refused { .. }
        ),
        "reconfigured: {reconfigured:?}"
    );
    let inspected = inspected.expect("the leader answers Inspect");
    assert!(inspected.leader, "the node asked leads");
    assert_eq!(inspected.members, vec![1, 2, 3]);
    assert!(inspected.matchmakers.is_empty(), "no matchmaker set");
    assert_eq!(unknown, ReaderOutcome::UnknownJournal);
}

#[test]
fn the_cell_keeps_writing_when_its_leader_stops_cleanly() {
    let ids = [1, 2, 3];
    let (ends, (first, after, page)) = cell(&ids, async |providers, stops| {
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let mut writer = Writer::with_uuid(journal(), U);
        writer.claim(&client, 0, false).await;
        let first = writer.write(&client, one(1), 0).await;
        let leader = client.leader().expect("a leader answered");
        stops[leader].cancel();
        rig::sleep(&providers, Duration::from_secs(3)).await;
        let other = (leader + 1) % ids.len();
        let after = writer.write(&client, one(2), other).await;
        let page = Reader::new(journal(), 0).next(&client, other).await;
        (first, after, page)
    });
    clean(ends);
    assert!(matches!(first, WriterOutcome::Written { seq: 0, .. }));
    assert!(
        matches!(after, WriterOutcome::Written { seq: 1, .. }),
        "after the stop: {after:?}"
    );
    let ReaderOutcome::Records { records, .. } = page else {
        panic!("read: {page:?}");
    };
    assert_eq!(records, vec![vec![1], vec![2]]);
}

/// Boot one node of a plain cell of one over `storage` under `boot`, and
/// stop it a moment later: how its run ended.
fn boot_alone(storage: MemStorage, boot: BootKind) -> Result<(), RunError> {
    let (mut ends, ()) = cell_over(&[1], vec![(storage, boot)], async |providers, _| {
        rig::sleep(&providers, Duration::from_millis(500)).await;
    });
    ends.pop().expect("one node")
}

#[test]
fn a_boot_claim_the_store_contradicts_is_refused() {
    use paros_storage::boot::BootRefusal;
    let fresh = MemStorage::new(config(1, &[1]));
    let formatted = |config: Config| {
        MemStorage::from_records(
            config,
            paros_core::HardState::default(),
            paros_core::Slot(0),
            Vec::new(),
            paros_core::journal::JournalState::default(),
        )
    };
    assert!(
        matches!(
            boot_alone(fresh.clone(), BootKind::ExistingMember),
            Err(RunError::Refused(BootRefusal::Amnesia))
        ),
        "a member with no marker"
    );
    assert!(
        matches!(
            boot_alone(formatted(config(1, &[1])), BootKind::FirstBoot),
            Err(RunError::Refused(BootRefusal::AlreadyFormatted))
        ),
        "a first boot over a marker"
    );
    // The operator's configuration is the store's own; its marker was
    // written under another membership.
    let mut edited = config(1, &[1]);
    edited.peers.push(NodeId(2));
    let mut moved = fresh.clone();
    futures::executor::block_on(paros_storage::LogStorage::format(&mut moved, &edited))
        .expect("formatted");
    let mismatch = boot_alone(moved, BootKind::ExistingMember);
    assert!(
        matches!(
            mismatch,
            Err(RunError::Refused(BootRefusal::ConfigMismatch))
        ),
        "a marker under another configuration: {mismatch:?}"
    );
    boot_alone(formatted(config(1, &[1])), BootKind::ExistingMember).expect("a member boots");
    boot_alone(fresh, BootKind::FirstBoot).expect("a first boot formats");
}
