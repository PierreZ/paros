//! The data plane on a created journal (#451): on a BUGGIFY coin, a client
//! that created a journal, or reached one by name through a frontend, runs
//! the calls the genesis journals get on it: a batch of appends, a paging
//! read from a cursor, a truncation, and a `SetLeader` that supersedes the
//! first writer, whose next write must be refused. A multi-writer journal
//! takes unfenced appends and an open truncation instead.
//!
//! Every call goes through `paros::client` and is logged in the journal's
//! history (`Announce`, or the frontend's own log of what it forwards), so
//! the linearizability search judges each one; the oracles here are the
//! ones the history cannot state: the superseded writer.

use moonpool_sim::{assert_always, assert_reachable};
use paros::client::{
    ClaimOutcome, ReaderOutcome, TruncateOutcome, WriteOptions, WriteOutcome, Writer,
    WriterOutcome, multi,
};
use paros::{JournalIdentifier, Value};

use super::FleetOps;
use crate::client::ChainClient;

/// The most pages a paging read takes: one record each.
const PAGES: usize = 6;

/// `count` fresh records, each a random word.
fn records(count: usize) -> Vec<Value> {
    (0..count)
        .map(|_| {
            Value(
                moonpool_sim::sim_random_range(1..u64::MAX)
                    .to_le_bytes()
                    .to_vec(),
            )
        })
        .collect()
}

impl FleetOps {
    /// Run the data plane on `journal` through `client`, from server
    /// `first` on. The writer mode is learned from the first claim: a
    /// multi-writer journal refuses it as `wrong_mode`.
    pub(super) async fn exercise_created(
        &mut self,
        client: &ChainClient,
        journal: JournalIdentifier,
        first: usize,
    ) {
        let mut writer = Writer::new(journal, self.leader_seeds.next());
        match writer.claim(client, first, false).await {
            ClaimOutcome::Won { .. } => {
                assert_reachable!("created: a client claims a journal it reached");
                self.single_writer(client, writer, first).await;
            }
            ClaimOutcome::WrongMode { .. } => {
                assert_reachable!("created: a claim of a multi-writer journal is refused");
                Self::multi_writer(client, journal, first).await;
            }
            _ => {}
        }
    }

    /// A single-writer journal: appends, a paging read, a truncation, then
    /// a second writer's claim, after which the first writer's write is
    /// refused.
    async fn single_writer(&mut self, client: &ChainClient, mut writer: Writer, first: usize) {
        let journal = writer.journal();
        for count in [2, 1] {
            if let WriterOutcome::Written { .. } = writer.write(client, records(count), first).await
            {
                assert_reachable!("created: an owner appends a batch to a created journal");
            }
        }
        page(client, journal, first).await;
        let up_to = writer.next_seq().saturating_sub(1);
        if up_to > 0
            && let Some(TruncateOutcome::Applied { .. }) =
                writer.truncate(client, up_to, first).await
        {
            assert_reachable!("created: an owner truncates a created journal");
            page(client, journal, first).await;
        }
        let mut successor = Writer::new(journal, self.leader_seeds.next());
        if !matches!(
            successor.claim(client, first, false).await,
            ClaimOutcome::Won { .. }
        ) {
            return;
        }
        assert_reachable!("created: a second writer supersedes the first");
        // The first writer still believes it leads: its write goes out under
        // its own uuid, which a won `SetLeader` replaced. A client draws a
        // fresh uuid for every writer, so nothing reinstates it.
        let stale = writer.write(client, records(1), first).await;
        assert_always!(
            !matches!(stale, WriterOutcome::Written { .. }),
            "created: a superseded writer's write is never written"
        );
        if matches!(stale, WriterOutcome::Superseded { .. }) {
            assert_reachable!("created: a superseded writer's write is refused");
        }
        if let WriterOutcome::Written { .. } = successor.write(client, records(1), first).await {
            assert_reachable!("created: the successor appends after the supersede");
        }
    }

    /// A multi-writer journal: unfenced appends, a paging read and an open
    /// truncation.
    async fn multi_writer(client: &ChainClient, journal: JournalIdentifier, first: usize) {
        let mut tail = None;
        for count in [2, 1] {
            let request = multi::append_request(
                journal,
                records(count).into_iter().map(|value| value.0).collect(),
            );
            if let WriteOutcome::Written { seq, count, .. } = client
                .write(&request, first, WriteOptions::default())
                .await
                .outcome
            {
                assert_reachable!("created: a writer appends to a created multi-writer journal");
                tail = Some(seq + count);
            }
        }
        page(client, journal, first).await;
        let Some(tail) = tail else {
            return;
        };
        let request = multi::open_truncate_request(journal, tail.saturating_sub(1));
        if let TruncateOutcome::Applied { .. } = client.truncate(&request, first).await {
            assert_reachable!("created: a writer truncates a created multi-writer journal");
            page(client, journal, first).await;
        }
    }
}

/// Read `journal` from its start one record per page, up to [`PAGES`]
/// pages or its tail: a cursor below the floor resumes past it.
async fn page(client: &ChainClient, journal: JournalIdentifier, first: usize) {
    let mut reader = paros::client::Reader::new(journal, 0);
    for _ in 0..PAGES {
        let report = client.read_any(&reader.request(1, 0), first).await;
        match reader.absorb(report.outcome) {
            ReaderOutcome::Records { records, .. } if !records.is_empty() => {
                assert_reachable!("created: a client pages a created journal from a cursor");
            }
            ReaderOutcome::Gap { .. } => {
                assert_reachable!("created: a paging read resumes past a truncation");
            }
            _ => return,
        }
    }
}
