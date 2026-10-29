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
//! The superseded writer's claim is precise: its write is never *accepted
//! anew*. The journal answers a write below its tail from the log
//! (`Outcome::Duplicate`): a write whose fate the writer never learned
//! (`Ambiguous`) leaves its `next_seq` behind a position the journal holds
//! under its uuid, and the alphabet repeats (an empty or one-byte record),
//! so its "new" write can be byte for byte the write it already holds there.
//! That answer is the dedup rule, not a stale write landing (hunt seed
//! 14177363183093160763 on 4a24b98).
//!
//! Every call goes through `paros::client` and is logged in the journal's
//! history (`Announce`, or the frontend's own log of what it forwards), so
//! the linearizability search judges each one; the oracles here are the
//! ones the history cannot state: the superseded writer.

use std::time::Duration;

use futures::future::join_all;
use moonpool_sim::{TimeProvider, assert_always, assert_reachable, buggify_with_prob};
use paros::client::{
    ClaimOutcome, Pipeline, ReaderOutcome, TruncateOutcome, WriteOptions, WriteOutcome, Writer,
    WriterOutcome, multi,
};
use paros::core::Value;
use paros::core::journal::JournalIdentifier;

use super::FleetOps;
use crate::audit::{AckedWrite, AuditWorld, audit_world_named};
use crate::client::ChainClient;
use crate::workload::ChainWorkload;
use crate::workload::MAX_BATCH_RECORDS;
use crate::workload::chain::user_command_hash;
use crate::workload::config::ChainConfig;

/// The most pages a paging read takes.
const PAGES: usize = 64;

/// The records a created journal holds when a gate counts it as a long log
/// (#658): more than the pages of one record a paging read once took.
pub(super) const LONG_LOG: u64 = 64;

/// The hash of each record of `records`, as the audit keeps them.
pub(super) fn hashes(records: &[Value]) -> Vec<u64> {
    records
        .iter()
        .map(|record| user_command_hash(&record.0))
        .collect()
}

/// Tell `audit` a write of `sent` is written at `[seq, seq + count)`.
pub(super) fn acked(audit: Option<&AuditWorld>, seq: u64, count: u64, sent: Vec<u64>) {
    if let Some(audit) = audit {
        audit.note_acked(AckedWrite {
            seq,
            count,
            records: sent,
        });
    }
}

/// The records of one batch under `config`, drawn as the genesis
/// journals' are (#658), registered with `audit` when there is one.
fn records_of(config: &ChainConfig, audit: Option<&AuditWorld>) -> Vec<Value> {
    let class = moonpool_sim::sim_random_range(0..u64::MAX);
    let seed = moonpool_sim::sim_random_range(0..u64::MAX);
    match audit {
        Some(audit) => ChainWorkload::draw_records(audit, config, class, seed),
        None => ChainWorkload::draw_values(config, class, seed),
    }
}

/// The bursts a batchers seed sends a journal it just created, until one
/// write of a burst is acked (#145 (batchers)).
const BURSTS: usize = 3;

/// The pause between two of those bursts: about a leader election on a
/// fresh journal.
const BURST_PAUSE: Duration = Duration::from_millis(1_500);

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
        (audited, batched): (Option<JournalIdentifier>, Option<ChainClient>),
    ) {
        let audit = audited.map(|journal| audit_world_named(&self.state, journal));
        let audit = audit.as_deref();
        let mut writer = Writer::new(journal, self.leader_seeds.next());
        match writer.claim(client, first, false).await {
            ClaimOutcome::Won { .. } => {
                assert_reachable!("created: a client claims a journal it reached");
                self.single_writer((client, batched.as_ref()), writer, first, audit)
                    .await;
            }
            ClaimOutcome::WrongMode { .. } => {
                assert_reachable!("created: a claim of a multi-writer journal is refused");
                if let Some(batched) = batched {
                    let _ = self.batched_burst(&batched, journal, first, audit).await;
                }
                self.multi_writer(client, journal, first, audit).await;
            }
            _ => {}
        }
    }

    /// The batchers scenario's burst (#145 (batchers),
    /// `crate::shape::batchers`): a multi-writer journal this client just
    /// created takes a burst through the batchers of `servers` at once,
    /// with no claim first, so a batcher lane holds several writes. A
    /// journal right after its create often has no leader yet, and a
    /// merged write then goes unanswered: a burst with no write acked is
    /// sent again after a pause, up to [`BURSTS`] bursts.
    pub(super) async fn batched_created(
        &mut self,
        servers: &[(u64, paros::model::address::Address)],
        journal: JournalIdentifier,
        first: usize,
    ) {
        assert_reachable!("created: a batchers seed bursts on a journal it created");
        let batched = self.connect_batched(servers);
        let audit = audit_world_named(&self.state, journal);
        // The batcher-limits scenario (#702 (batcher rare paths)): every
        // burst, so a batcher that learned a leader's limits from a split
        // takes the next wide writes.
        let every = crate::shape::batcher_limits(&self.state);
        for _ in 0..BURSTS {
            if self
                .batched_burst(&batched, journal, first, Some(&audit))
                .await
                > 0
                && !every
            {
                return;
            }
            if self.time.sleep(BURST_PAUSE).await.is_err() {
                return;
            }
        }
    }

    /// The records of one batch, drawn as the genesis journals' are
    /// (#658), registered with `audit` when there is one.
    pub(super) fn records(&self, audit: Option<&AuditWorld>) -> Vec<Value> {
        records_of(&self.config, audit)
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
    /// after which the first writer's write is refused. With `via_batcher`, the
    /// first round goes through a batcher, which passes a fenced write on
    /// alone (#145 (batchers)).
    async fn single_writer(
        &mut self,
        (client, via_batcher): (&ChainClient, Option<&ChainClient>),
        mut writer: Writer,
        first: usize,
        audit: Option<&AuditWorld>,
    ) {
        let journal = writer.journal();
        // On its own location, batches of up to the most records a write
        // carries, whatever the seed's `batch_records`: a floor then has
        // somewhere inside a batch to land (#659).
        let mut config = self.config;
        if buggify_with_prob!(0.5) {
            config.batch_records = MAX_BATCH_RECORDS;
        }
        let mut batches = Vec::new();
        // The writes whose fate the writer never learned: their positions
        // and sizes. The journal may hold one, and answer the writer's next
        // write at that position from the log.
        let mut unsettled = Vec::new();
        for round in 0..self.rounds() {
            let records = records_of(&config, audit);
            let sent = hashes(&records);
            let at = writer.next_seq();
            let via = via_batcher.filter(|_| round == 0).unwrap_or(client);
            match writer.write(via, records, first).await {
                WriterOutcome::Written { seq, count, .. } => {
                    assert_reachable!("created: an owner appends a batch to a created journal");
                    acked(audit, seq, count, sent);
                    batches.push((seq, count));
                }
                WriterOutcome::Ambiguous => unsettled.push((at, sent.len() as u64)),
                _ => {}
            }
        }
        if buggify_with_prob!(0.25) {
            writer = self
                .pipelined(client, writer, first, audit, &mut unsettled)
                .await;
        }
        self.page(client, journal, first).await;
        // A floor drawn over the whole log, or strictly inside a batch of
        // several records it wrote (#659).
        let next = writer.next_seq();
        let several: Vec<&(u64, u64)> = batches.iter().filter(|(_, count)| *count > 1).collect();
        let aim = !several.is_empty() && moonpool_sim::sim_random_range(0..2_u64) == 0;
        let inside = aim.then(|| {
            let pick = moonpool_sim::sim_random_range(0..several.len() as u64);
            let (seq, count) = *several[usize::try_from(pick).unwrap_or(0)];
            seq + 1 + moonpool_sim::sim_random_range(0..count - 1)
        });
        let up_to = inside.unwrap_or_else(|| 1 + moonpool_sim::sim_random_range(0..next.max(1)));
        if next > 1
            && let Some(TruncateOutcome::Applied { state }) =
                writer.truncate(client, up_to.min(next), first).await
        {
            assert_reachable!("created: an owner truncates a created journal");
            let floor = state.first_seq.0;
            if batches
                .iter()
                .any(|(seq, count)| *seq < floor && floor < seq + count)
            {
                assert_reachable!("created: a truncation floor lands inside a batch");
            }
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
        // fresh uuid for every writer, so nothing reinstates it. The one
        // `Written` the journal may answer is a `Duplicate`: the write names
        // a position of one of this writer's own unsettled writes, and the
        // journal holds that write there, byte for byte (the module doc).
        let at = writer.next_seq();
        let stale = writer.write(client, self.records(audit), first).await;
        let own = match &stale {
            WriterOutcome::Written {
                seq,
                count,
                duplicate,
                ..
            } => *duplicate && *seq == at && unsettled.contains(&(*seq, *count)),
            _ => true,
        };
        assert_always!(
            own,
            "created: a superseded writer's write is never written",
            {
                "written" => matches!(stale, WriterOutcome::Written { .. }),
                "at" => at,
                "unsettled" => unsettled.len()
            }
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
    /// Returns the pipeline's writer; a ticket whose fate stayed unknown
    /// goes into `unsettled` with its position and size.
    async fn pipelined(
        &self,
        client: &ChainClient,
        writer: Writer,
        first: usize,
        audit: Option<&AuditWorld>,
        unsettled: &mut Vec<(u64, u64)>,
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
            if matches!(result.outcome, WriterOutcome::Ambiguous) {
                unsettled.push((ticket.entry().seq.0, ticket.entry().count()));
            }
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

    /// A burst of `pipeline_depth` unfenced appends through the cell's
    /// batchers (#145 (batchers)), all in flight at once, so a batcher
    /// merges several of them into one slot. Each answer is its own write's
    /// range, judged by its bytes like any acked write. The writes acked.
    async fn batched_burst(
        &self,
        client: &ChainClient,
        journal: JournalIdentifier,
        first: usize,
        audit: Option<&AuditWorld>,
    ) -> usize {
        // On a coin, each write carries more records than a batcher below
        // the leader's limits takes (`paros::roles::batcher::TIGHT_RECORDS`): it
        // goes alone, and a merged write passes the limits sooner. Every
        // time on the batcher-limits scenario (#702 (batcher rare paths)).
        let wide = crate::shape::batcher_limits(&self.state) || buggify_with_prob!(0.3);
        let writes: Vec<(Vec<u64>, paros::wire::Write)> = (0..self.config.pipeline_depth)
            .map(|_| {
                // Never more than the workload's largest write
                // (`MAX_BATCH_RECORDS`), which one store entry holds.
                let mut records = self.records(audit);
                while wide && records.len() <= paros::roles::batcher::TIGHT_RECORDS {
                    records.extend(self.records(audit));
                }
                records.truncate(usize::try_from(MAX_BATCH_RECORDS).unwrap_or(usize::MAX));
                let sent = hashes(&records);
                let request = multi::append_request(
                    journal,
                    records.into_iter().map(|value| value.0).collect(),
                );
                (sent, request)
            })
            .collect();
        let results = join_all(
            writes
                .iter()
                .map(|(_, request)| client.write(request, first, WriteOptions::default())),
        )
        .await;
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for ((sent, _), report) in writes.into_iter().zip(results) {
            if let WriteOutcome::Written { seq, count, .. } = report.outcome {
                assert_always!(
                    count == sent.len() as u64,
                    "batcher: a write's range holds exactly its records",
                    { "count" => count, "sent" => sent.len() }
                );
                assert_reachable!("created: a batched append lands on a created journal");
                ranges.push((seq, count));
                acked(audit, seq, count, sent);
            }
        }
        ranges.sort_unstable();
        assert_always!(
            ranges.windows(2).all(|w| w[0].0 + w[0].1 <= w[1].0),
            "batcher: two writes never share a position",
            { "ranges" => format!("{ranges:?}") }
        );
        ranges.len()
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
