//! The data plane on a created journal (#451): on a BUGGIFY coin, a client
//! that created a journal, or reached one by name through a frontend, runs
//! the calls the genesis journals get on it: a batch of appends, a paging
//! read from a cursor, a truncation, and a `SetLeader` that supersedes the
//! first writer, whose next write must be refused. A multi-writer journal
//! takes unfenced appends and an open truncation instead.
//!
//! The load is the client's own alphabet (#658 (the full alphabet on
//! created journals)): `created_rounds` batches of records drawn as the
//! genesis journals' are (sizes, payload classes, `batch_records`), on a
//! coin a burst of `pipeline_depth` writes through the library's
//! `Pipeline`, pages of `read_limit` records waiting `read_wait_ms`, and a
//! truncation at a drawn floor, often inside a batch. Every acked write is
//! judged by its bytes against the journal's model (#651 (acked writes
//! judged by their bytes)).
//!
//! Every call goes through `paros::client` and is logged in the journal's
//! history (`Announce`, or the frontend's own log of what it forwards), so
//! the linearizability search judges each one; the oracles here are the
//! ones the history cannot state: the superseded writer.

use futures::future::join_all;
use moonpool_sim::{assert_always, assert_reachable, buggify_with_prob};
use paros::client::{
    ClaimOutcome, Pipeline, ReaderOutcome, TruncateOutcome, WriteOptions, WriteOutcome, Writer,
    WriterOutcome, multi,
};
use paros::{JournalIdentifier, Value};

use super::FleetOps;
use crate::audit::{AckedWrite, AuditWorld, audit_world_named};
use crate::chain::user_command_hash;
use crate::chain_workload::ChainWorkload;
use crate::client::ChainClient;

/// The most pages a paging read takes.
const PAGES: usize = 64;

/// The records a created journal holds when a gate counts it as a long log
/// (#658): more than the pages of one record a paging read once took.
pub(super) const LONG_LOG: u64 = 64;

/// The hash of each record of `records`, as the audit keeps them.
fn hashes(records: &[Value]) -> Vec<u64> {
    records
        .iter()
        .map(|record| user_command_hash(&record.0))
        .collect()
}

/// Tell `audit` a write of `sent` is written at `[seq, seq + count)`.
fn acked(audit: Option<&AuditWorld>, seq: u64, count: u64, sent: Vec<u64>) {
    if let Some(audit) = audit {
        audit.note_acked(AckedWrite {
            seq,
            count,
            records: sent,
        });
    }
}

impl FleetOps {
    /// Run the data plane on `journal` through `client`, from server
    /// `first` on. The writer mode is learned from the first claim: a
    /// multi-writer journal refuses it as `wrong_mode`. `audited` is the
    /// journal's identifier when the client knows it (a call by name
    /// through a frontend does not): its acked writes are judged by their
    /// bytes.
    pub(super) async fn exercise_created(
        &mut self,
        client: &ChainClient,
        journal: JournalIdentifier,
        first: usize,
        audited: Option<JournalIdentifier>,
    ) {
        let audit = audited.map(|journal| audit_world_named(&self.state, journal));
        let audit = audit.as_deref();
        let mut writer = Writer::new(journal, self.leader_seeds.next());
        match writer.claim(client, first, false).await {
            ClaimOutcome::Won { .. } => {
                assert_reachable!("created: a client claims a journal it reached");
                self.single_writer(client, writer, first, audit).await;
            }
            ClaimOutcome::WrongMode { .. } => {
                assert_reachable!("created: a claim of a multi-writer journal is refused");
                self.multi_writer(client, journal, first, audit).await;
            }
            _ => {}
        }
    }

    /// The records of one batch, drawn as the genesis journals' are
    /// (#658), registered with `audit` when there is one.
    fn records(&self, audit: Option<&AuditWorld>) -> Vec<Value> {
        let class = moonpool_sim::sim_random_range(0..u64::MAX);
        let seed = moonpool_sim::sim_random_range(0..u64::MAX);
        match audit {
            Some(audit) => ChainWorkload::draw_records(audit, &self.config, class, seed),
            None => ChainWorkload::draw_values(&self.config, class, seed),
        }
    }

    /// The rounds of batches a created journal takes: `created_rounds`,
    /// and at least 16 on a seed whose created journals move or change
    /// mode, so a long log is what moves (#658).
    fn rounds(&self) -> u64 {
        if crate::shape::drained_machine(&self.state) || crate::shape::changed_mode(&self.state) {
            self.config.created_rounds.max(16)
        } else {
            self.config.created_rounds
        }
    }

    /// A single-writer journal: rounds of appends, on a coin a pipelined
    /// burst, a paging read, a truncation, then a second writer's claim,
    /// after which the first writer's write is refused.
    async fn single_writer(
        &mut self,
        client: &ChainClient,
        mut writer: Writer,
        first: usize,
        audit: Option<&AuditWorld>,
    ) {
        let journal = writer.journal();
        for _ in 0..self.rounds() {
            let records = self.records(audit);
            let sent = hashes(&records);
            if let WriterOutcome::Written { seq, count, .. } =
                writer.write(client, records, first).await
            {
                assert_reachable!("created: an owner appends a batch to a created journal");
                acked(audit, seq, count, sent);
            }
        }
        if buggify_with_prob!(0.25) {
            writer = self.pipelined(client, writer, first, audit).await;
        }
        self.page(client, journal, first).await;
        // A floor drawn over the whole log, often inside a batch (#659).
        let next = writer.next_seq();
        let up_to = 1 + moonpool_sim::sim_random_range(0..next.max(1));
        if next > 1
            && let Some(TruncateOutcome::Applied { .. }) =
                writer.truncate(client, up_to.min(next), first).await
        {
            assert_reachable!("created: an owner truncates a created journal");
            self.page(client, journal, first).await;
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
        let stale = writer.write(client, self.records(audit), first).await;
        assert_always!(
            !matches!(stale, WriterOutcome::Written { .. }),
            "created: a superseded writer's write is never written"
        );
        if matches!(stale, WriterOutcome::Superseded { .. }) {
            assert_reachable!("created: a superseded writer's write is refused");
        }
        let records = self.records(audit);
        let sent = hashes(&records);
        if let WriterOutcome::Written { seq, count, .. } =
            successor.write(client, records, first).await
        {
            assert_reachable!("created: the successor appends after the supersede");
            acked(audit, seq, count, sent);
        }
    }

    /// A burst of `pipeline_depth` writes through the library's
    /// [`Pipeline`] (#597 (several writes in flight per journal)) on a
    /// machine-served journal (#658): every ticket's answer is its own.
    /// Returns the pipeline's writer.
    async fn pipelined(
        &self,
        client: &ChainClient,
        writer: Writer,
        first: usize,
        audit: Option<&AuditWorld>,
    ) -> Writer {
        let pipeline = Pipeline::new(writer);
        let mut opened = Vec::with_capacity(self.config.pipeline_depth);
        for _ in 0..self.config.pipeline_depth {
            let records = self.records(audit);
            let sent = hashes(&records);
            let Some(ticket) = pipeline.open(|_| records).await else {
                break;
            };
            opened.push((ticket, sent));
        }
        let pipeline = &pipeline;
        let results = join_all(
            opened
                .iter()
                .map(|(ticket, _)| pipeline.send(client, ticket.clone(), first)),
        )
        .await;
        for ((ticket, sent), result) in opened.into_iter().zip(results) {
            if let WriterOutcome::Written { seq, count, .. } = result.outcome {
                assert_always!(
                    seq == ticket.entry().seq.0 && count == ticket.entry().count(),
                    "chain: a pipelined write is written at the position its ticket opened",
                    {
                        "opened" => ticket.entry().seq.0,
                        "seq" => seq,
                        "count" => count
                    }
                );
                assert_reachable!("created: a pipelined burst lands on a machine-served journal");
                acked(audit, seq, count, sent);
            }
        }
        pipeline.writer()
    }

    /// A multi-writer journal: rounds of unfenced appends, a paging read
    /// and an open truncation.
    async fn multi_writer(
        &self,
        client: &ChainClient,
        journal: JournalIdentifier,
        first: usize,
        audit: Option<&AuditWorld>,
    ) {
        let mut tail = None;
        for _ in 0..self.rounds() {
            let records = self.records(audit);
            let sent = hashes(&records);
            let request =
                multi::append_request(journal, records.into_iter().map(|value| value.0).collect());
            if let WriteOutcome::Written { seq, count, .. } = client
                .write(&request, first, WriteOptions::default())
                .await
                .outcome
            {
                assert_reachable!("created: a writer appends to a created multi-writer journal");
                acked(audit, seq, count, sent);
                tail = Some(seq + count);
            }
        }
        self.page(client, journal, first).await;
        let Some(tail) = tail else {
            return;
        };
        let up_to = 1 + moonpool_sim::sim_random_range(0..tail.max(1));
        let request = multi::open_truncate_request(journal, up_to.min(tail));
        if let TruncateOutcome::Applied { .. } = client.truncate(&request, first).await {
            assert_reachable!("created: a writer truncates a created multi-writer journal");
            self.page(client, journal, first).await;
        }
    }

    /// Read `journal` from its start, `read_limit` records a page waiting
    /// up to `read_wait_ms`, up to [`PAGES`] pages or its tail: a cursor
    /// below the floor resumes past it.
    async fn page(&self, client: &ChainClient, journal: JournalIdentifier, first: usize) {
        let mut reader = paros::client::Reader::new(journal, 0);
        for _ in 0..PAGES {
            let request = reader.request(self.config.read_limit, self.config.read_wait_ms);
            let report = client.read_any(&request, first).await;
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
}
