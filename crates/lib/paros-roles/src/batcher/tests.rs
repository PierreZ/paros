//! A batcher in front of a cell: `run_batcher` on its own address over a
//! fixed list of nodes, called by the library client, as a writer of a
//! multi-writer journal calls it.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::SimProviders;
use paros_client::multi::append_request;
use paros_client::{Answered, Attempted, CallObserver};
use paros_client::{
    Client, Reader, ReaderOutcome, ResolveReport, Server, WriteOptions, WriteOutcome,
};
use paros_core::NodeId;
use paros_core::journal::WriterMode;
use paros_model::address::{Address, Names};
use paros_net::NodeClient;
use paros_net::transport::Transport;
use paros_wire::Write;
use tokio_util::sync::CancellationToken;

use super::{BatcherBook, BatcherSettings, run_batcher};
use crate::rig::{self, Deployment, clean, journal};

/// Where the batcher listens: on the client's machine.
fn listen() -> SocketAddr {
    format!("{}:4600", rig::CLIENT_IP)
        .parse()
        .expect("a test address")
}

/// The record count of every merged write the batcher sent.
#[derive(Default)]
struct Merges(Mutex<Vec<usize>>);

impl Merges {
    fn counts(&self) -> Vec<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl CallObserver for Merges {
    fn invoked(&self, _attempt: Attempted<'_>) -> Option<u64> {
        None
    }

    fn answered(&self, _token: u64, _answer: Answered<'_>) {}

    fn resolving(&self, _write: &Write) -> Option<u64> {
        None
    }

    fn resolved(&self, _token: u64, _report: &ResolveReport) {}

    fn merged(&self, write: &Write) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(write.records.len());
    }
}

/// A multi-writer cell of three.
fn deployment() -> Deployment {
    Deployment {
        peers: vec![1, 2, 3],
        writer_mode: WriterMode::Multi,
        ..Deployment::default()
    }
}

/// Run a batcher under `settings` in front of `deployment`'s nodes, and
/// `body` with a client of the batcher and a client of the nodes:
/// `body`'s answer, and the merged writes the batcher sent.
fn batched<T>(
    deployment: &Deployment,
    settings: impl FnOnce(BatcherSettings) -> BatcherSettings,
    body: impl AsyncFnOnce(Client<SimProviders>, Client<SimProviders>) -> T,
) -> (T, Vec<usize>) {
    let merges = Arc::new(Merges::default());
    let observer: Arc<dyn CallObserver> = merges.clone();
    let tunables = deployment.tunables;
    let settings = settings(BatcherSettings::from_tunables(&tunables));
    let pool = deployment.pool();
    let (ends, answer) = deployment.run(async |providers, _| {
        let stop = CancellationToken::new();
        let book = BatcherBook {
            nodes: pool
                .iter()
                .map(|id| (NodeId(*id), Address::from(rig::addr(*id))))
                .collect(),
            updates: None,
            skip: None,
            observer: Some(observer),
        };
        let batcher = run_batcher(
            providers.clone(),
            (listen(), &tunables, &Transport::Plaintext),
            settings,
            (book, Names::literal()),
            stop.clone(),
        );
        let calls = async {
            rig::sleep(&providers, Duration::from_secs(2)).await;
            let rpc = rig::runtime(&providers);
            let via = Client::new(
                &providers,
                vec![Server {
                    id: 1,
                    node: NodeClient::batcher(&rpc, Names::literal(), Address::from(listen())),
                }],
                rig::tunables(),
            );
            let answer = body(via, rig::client(&providers, &pool)).await;
            stop.cancel();
            answer
        };
        let (ran, answer) = futures::join!(batcher, calls);
        ran.expect("the batcher stops cleanly");
        answer
    });
    clean(ends);
    (answer, merges.counts())
}

/// Send each of `writes` through `client` at once: every outcome.
async fn send_all(client: &Client<SimProviders>, writes: &[Vec<Vec<u8>>]) -> Vec<WriteOutcome> {
    let sends = writes.iter().map(|records| async {
        client
            .write(
                &append_request(journal(), records.clone()),
                0,
                WriteOptions::default(),
            )
            .await
            .outcome
    });
    futures::future::join_all(sends).await
}

/// Every record of the journal, read from node 1.
async fn records(client: &Client<SimProviders>) -> Vec<Vec<u8>> {
    match Reader::new(journal(), 0).next(client, 0).await {
        ReaderOutcome::Records { records, .. } => records,
        other => panic!("read: {other:?}"),
    }
}

/// The positions `outcomes` were written at, sorted, each a range.
fn ranges(outcomes: &[WriteOutcome]) -> Vec<(u64, u64)> {
    let mut ranges: Vec<(u64, u64)> = outcomes
        .iter()
        .map(|outcome| match outcome {
            WriteOutcome::Written { seq, count, .. } => (*seq, *count),
            other => panic!("a write through the batcher: {other:?}"),
        })
        .collect();
    ranges.sort_unstable();
    ranges
}

fn sorted(mut records: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    records.sort();
    records
}

#[test]
fn writes_sent_at_once_each_get_their_own_range() {
    let writes = vec![vec![vec![1]], vec![vec![2], vec![3]], vec![vec![4]]];
    let ((outcomes, held), merges) = batched(
        &deployment(),
        |settings| settings,
        async |via, nodes| {
            let outcomes = send_all(&via, &writes).await;
            (outcomes, records(&nodes).await)
        },
    );
    assert_eq!(
        ranges(&outcomes),
        vec![(0, 1), (1, 2), (3, 1)],
        "the ranges tile the journal"
    );
    assert_eq!(sorted(held), vec![vec![1], vec![2], vec![3], vec![4]]);
    assert_eq!(merges, vec![4], "one merged write of every record");
}

#[test]
fn a_write_past_a_tight_batchers_limit_goes_alone() {
    let writes = vec![vec![vec![1], vec![2], vec![3]], vec![vec![4]]];
    let ((outcomes, held), merges) = batched(
        &deployment(),
        BatcherSettings::below_limits,
        async |via, nodes| {
            let outcomes = send_all(&via, &writes).await;
            (outcomes, records(&nodes).await)
        },
    );
    let counts: Vec<u64> = ranges(&outcomes).iter().map(|(_, count)| *count).collect();
    assert_eq!(sorted_counts(counts), vec![1, 3]);
    assert_eq!(sorted(held), vec![vec![1], vec![2], vec![3], vec![4]]);
    assert!(
        merges.iter().all(|count| *count <= super::TIGHT_RECORDS),
        "no merge past the tight limit: {merges:?}"
    );
}

fn sorted_counts(mut counts: Vec<u64>) -> Vec<u64> {
    counts.sort_unstable();
    counts
}

#[test]
fn a_merged_write_the_leader_finds_too_large_is_sent_again_write_by_write() {
    let mut deployment = deployment();
    deployment.tunables.max_batch_records = 2;
    let writes: Vec<Vec<Vec<u8>>> = (0..5).map(|byte| vec![vec![byte]]).collect();
    let ((outcomes, held), merges) = batched(
        &deployment,
        BatcherSettings::above_limits,
        async |via, nodes| {
            let outcomes = send_all(&via, &writes).await;
            (outcomes, records(&nodes).await)
        },
    );
    assert_eq!(
        ranges(&outcomes),
        (0..5).map(|seq| (seq, 1)).collect::<Vec<_>>(),
        "each write lands alone"
    );
    assert_eq!(
        sorted(held),
        (0..5).map(|byte| vec![byte]).collect::<Vec<_>>()
    );
    assert_eq!(merges, vec![5], "one merge, refused as too large");
}

#[test]
fn a_single_writer_write_is_refused_by_the_journal_not_merged() {
    // A single-writer journal: an unfenced write is the wrong mode.
    let deployment = Deployment::plain(&[1, 2, 3]);
    let (outcomes, merges) = batched(
        &deployment,
        |settings| settings,
        async |via, _| send_all(&via, &[vec![vec![1]]]).await,
    );
    assert!(
        matches!(outcomes.as_slice(), [WriteOutcome::WrongMode { .. }]),
        "outcomes: {outcomes:?}"
    );
    assert!(merges.is_empty(), "a lane of one write is no merge");
}

#[test]
fn a_fenced_write_passes_alone_with_its_fence() {
    use paros_client::Writer;
    use paros_client::write_request;
    let deployment = Deployment::plain(&[1, 2, 3]);
    let (outcomes, merges) = batched(
        &deployment,
        |settings| settings,
        async |via, nodes| {
            let mut writer = Writer::with_uuid(journal(), paros_core::journal::LeaderUuid(11));
            writer.claim(&nodes, 0, false).await;
            let entry = writer
                .entry(vec![paros_core::Value(vec![7])])
                .expect("the writer leads");
            let fenced = write_request(journal(), &entry);
            let first = via.write(&fenced, 0, WriteOptions::default()).await.outcome;
            let again = via.write(&fenced, 0, WriteOptions::default()).await.outcome;
            vec![first, again]
        },
    );
    assert!(
        matches!(
            outcomes.as_slice(),
            [
                WriteOutcome::Written {
                    seq: 0,
                    count: 1,
                    duplicate: false
                },
                WriteOutcome::Written {
                    seq: 0,
                    duplicate: true,
                    ..
                }
            ]
        ),
        "the write, then its re-send answered from the log: {outcomes:?}"
    );
    assert!(merges.is_empty(), "a fenced write is never merged");
}
