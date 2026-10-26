//! The checkpoint owner (#227, #230, #353): [`Checkpointer`], the async
//! loop that writes a checkpoint run over a [`Client`] and truncates below
//! it, and [`load`], which restores a journal's state from its floor. The
//! record format and the pure fold are in [`crate::model::checkpoint`].

use std::time::Duration;

use crate::ParosProviders;
use paros_core::{JournalIdentifier, LeaderUuid, Value};

use super::reader::{Reader, ReaderOutcome};
use super::writer::{Writer, WriterOutcome};
use super::{ClaimOutcome, Client, TruncateOutcome};
pub use crate::model::checkpoint::*;
pub use crate::tunables::CheckpointPolicy;

/// What [`Checkpointer::open`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenOutcome {
    /// The journal is claimed and folded to its tail: the owner may append.
    Open {
        /// The next position the owner writes.
        next_seq: u64,
        /// The fold restarted from a checkpoint at the floor.
        restarted: bool,
        /// The first checkpoint the fold found unequal to its own state
        /// (see [`LoadOutcome::Loaded`]).
        diverged: Option<u64>,
    },
    /// The claim did not win (see [`ClaimOutcome`]).
    NotClaimed(ClaimOutcome),
    /// Claimed, but the fold did not reach the tail the claim answered (no
    /// server served a page, the server asked lags it, or the floor is no
    /// checkpoint this fold can use).
    Behind(LoadOutcome),
}

/// What [`Checkpointer::load`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    /// Folded to `up_to`; `restarted` when it restarted from a checkpoint at
    /// the floor (a truncation overtook the cursor).
    Loaded {
        /// The fold's next position.
        up_to: u64,
        /// The fold jumped to the floor and restored there.
        restarted: bool,
        /// The first checkpoint the fold verified and found **unequal** to
        /// its own state, if any: an owner wrote a state that is not the
        /// fold of its journal. The fold reset on it, like every reader's.
        diverged: Option<u64>,
    },
    /// No server served a page.
    Unavailable,
    /// The server asked does not serve the journal.
    UnknownJournal,
    /// A frontend refused the read (#192 (the frontend)).
    Denied(crate::model::authz::Denial),
    /// The fold jumped a gap and no checkpoint healed it before the tail:
    /// the floor is not a checkpoint's.
    Unhealed {
        /// Where the fold stands.
        at: u64,
    },
}

/// What [`Checkpointer::append`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The write's verdict (see [`WriterOutcome`]); a written entry is folded.
    Written(WriterOutcome),
    /// Not sent: the entry starts with [`MAGIC`], and would read as a
    /// checkpoint.
    ReservedPrefix,
}

/// What [`Checkpointer::checkpoint`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointOutcome {
    /// The checkpoint is written at `seq` and the truncate to it answered
    /// `truncate` (a refusal or an ambiguity leaves the checkpoint mid-log;
    /// the next one truncates past it).
    Checkpointed {
        /// Where the checkpoint is.
        seq: u64,
        /// The truncation's verdict.
        truncate: Option<TruncateOutcome>,
    },
    /// The fold is not at the owner's next position (an append's verdict is
    /// still unknown, or the fold is not whole): nothing was written.
    NotFolded,
    /// The checkpoint is not known written (see [`WriterOutcome`]): refused,
    /// superseded, or still ambiguous — an ambiguous one may yet land
    /// mid-log, where every fold resets on it and the next checkpoint
    /// truncates past it.
    NotWritten(WriterOutcome),
}

/// A journal owner that checkpoints: a [`Writer`] and the [`Folder`] of its
/// own journal, kept in step, plus the [`CheckpointPolicy`].
#[derive(Clone, Debug)]
pub struct Checkpointer<S> {
    writer: Writer,
    folder: Folder<S>,
    policy: CheckpointPolicy,
    /// When the last checkpoint was written (or the owner opened).
    last: Duration,
}

impl<S: Checkpointable> Checkpointer<S> {
    /// An owner of `journal` over the empty `state`, leading under uuids
    /// drawn from `seed` ([`Writer::new`]).
    #[must_use]
    pub fn new(journal: JournalIdentifier, seed: u128, state: S, policy: CheckpointPolicy) -> Self {
        Self {
            writer: Writer::new(journal, seed),
            folder: Folder::new(state),
            policy,
            last: Duration::ZERO,
        }
    }

    /// An owner of `journal` over the empty `state`, leading under the one
    /// `uuid` it is given ([`Writer::with_uuid`]): an elected actor's term
    /// uuid (#240).
    #[must_use]
    pub fn with_uuid(
        journal: JournalIdentifier,
        uuid: LeaderUuid,
        state: S,
        policy: CheckpointPolicy,
    ) -> Self {
        Self {
            writer: Writer::with_uuid(journal, uuid),
            folder: Folder::new(state),
            policy,
            last: Duration::ZERO,
        }
    }

    /// The writer (its leader uuid and next position).
    #[must_use]
    pub fn writer(&self) -> &Writer {
        &self.writer
    }

    /// The fold.
    #[must_use]
    pub fn folder(&self) -> &Folder<S> {
        &self.folder
    }

    /// The state folded so far.
    #[must_use]
    pub fn state(&self) -> &S {
        self.folder.state()
    }

    /// Claim the journal, then fold it to the tail the claim answered.
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal()))]
    pub async fn open<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> OpenOutcome {
        let claim = self.writer.claim(client, first, false).await;
        self.opened(client, first, claim).await
    }

    /// [`Checkpointer::open`], claiming over `old` only (#653 (election
    /// term to installed leader), [`Writer::claim_over`]).
    pub async fn open_over<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
        old: Option<LeaderUuid>,
    ) -> OpenOutcome {
        let claim = self.writer.claim_over(client, first, old).await;
        self.opened(client, first, claim).await
    }

    /// Fold the journal to its tail once `claim` won it.
    async fn opened<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
        claim: ClaimOutcome,
    ) -> OpenOutcome {
        if !matches!(claim, ClaimOutcome::Won { .. } | ClaimOutcome::Owned { .. }) {
            return OpenOutcome::NotClaimed(claim);
        }
        self.last = client.now();
        match self.load(client, first, self.writer.next_seq()).await {
            LoadOutcome::Loaded {
                up_to,
                restarted,
                diverged,
            } if up_to >= self.writer.next_seq() => OpenOutcome::Open {
                next_seq: self.writer.next_seq(),
                restarted,
                diverged,
            },
            other => OpenOutcome::Behind(other),
        }
    }

    /// Fold the journal from where the fold stands up to `tail` (at least),
    /// from server `first` on: a `truncated` answer jumps to the floor and
    /// restarts from the checkpoint run there.
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal(), tail))]
    pub async fn load<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
        tail: u64,
    ) -> LoadOutcome {
        load(&mut self.folder, self.writer.journal(), client, first, tail).await
    }

    /// Append `record` as the owner (see [`Writer::write`]) and fold it once
    /// written at the fold's next position.
    pub async fn append<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        record: Vec<u8>,
        first: usize,
    ) -> AppendOutcome {
        if is_checkpoint(&record) {
            return AppendOutcome::ReservedPrefix;
        }
        let outcome = self
            .writer
            .write(client, vec![Value(record.clone())], first)
            .await;
        if let WriterOutcome::Written { seq, .. } = outcome
            && seq == self.folder.next_seq()
        {
            self.folder.fold(seq, &record);
        }
        AppendOutcome::Written(outcome)
    }

    /// Whether the policy asks for a checkpoint at `now`: the log since the
    /// last one (as folded, whoever wrote it) reached `factor ×` the state's
    /// size, or `interval` passed since this owner opened or last
    /// checkpointed, with any entry since. The time leg is the owner's own
    /// clock: an owner that opens afresh for every checkpoint reaches it only
    /// near an interval of 0.
    #[must_use]
    pub fn due(&self, now: Duration) -> bool {
        let (entries, bytes) = self.folder.since_checkpoint();
        if entries == 0 || !self.folder.is_whole() {
            return false;
        }
        let size = self.folder.state().checkpoint().len().max(1) as u64;
        bytes >= u64::from(self.policy.factor.max(1)).saturating_mul(size)
            || now.saturating_sub(self.last) >= self.policy.interval
    }

    /// Write a checkpoint, then truncate to it: [`Checkpointer::write_checkpoint`]
    /// and [`Checkpointer::truncate_to`].
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal()))]
    pub async fn checkpoint<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> CheckpointOutcome {
        match self.write_checkpoint(client, first).await {
            Ok(seq) => CheckpointOutcome::Checkpointed {
                seq,
                truncate: self.truncate_to(client, seq, first).await,
            },
            Err(outcome) => outcome,
        }
    }

    /// The first step alone: write the fold's state as a checkpoint run
    /// at the owner's next position, `End` included. On its own (a
    /// harness's crash between the two steps) it leaves the checkpoint
    /// mid-log. `Ok` names the run's `Begin`.
    ///
    /// # Errors
    ///
    /// [`CheckpointOutcome::NotFolded`] when the fold is not at the owner's
    /// next position; [`CheckpointOutcome::NotWritten`] when a batch is not
    /// known written (an ambiguous one may still land): the run has no
    /// `End`, and every fold ignores it.
    ///
    /// # Panics
    ///
    /// When a written run does not fold as one (the owner's own records).
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal(), seq = self.writer.next_seq()))]
    pub async fn write_checkpoint<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<u64, CheckpointOutcome> {
        let seq = self.run_start()?;
        let records = run(
            seq,
            &self.folder.state().checkpoint(),
            self.policy.chunk_bytes,
        );
        self.write_run(client, first, &records).await?;
        assert!(!self.folder.in_run(), "a written End closes the run");
        self.last = client.now();
        Ok(seq)
    }

    /// The explicit misbehaviour of an owner that stops inside its run
    /// (#353): write every record of the run but its `End`, then stop. No
    /// fold restores from it; the next owner's entry or run supersedes it.
    /// `Ok` names the run's `Begin`.
    ///
    /// # Errors
    ///
    /// As [`Checkpointer::write_checkpoint`].
    ///
    /// # Panics
    ///
    /// As [`Checkpointer::write_checkpoint`].
    pub async fn write_run_without_end<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<u64, CheckpointOutcome> {
        let seq = self.run_start()?;
        let mut records = run(
            seq,
            &self.folder.state().checkpoint(),
            self.policy.chunk_bytes,
        );
        records.pop();
        self.write_run(client, first, &records).await?;
        assert!(self.folder.in_run(), "a run with no End stays open");
        Ok(seq)
    }

    /// Where a run starts: the owner's next position, which the fold must
    /// stand at, whole.
    fn run_start(&self) -> Result<u64, CheckpointOutcome> {
        let seq = self.writer.next_seq();
        if !self.folder.is_whole() || self.folder.next_seq() != seq {
            return Err(CheckpointOutcome::NotFolded);
        }
        Ok(seq)
    }

    /// Write `records` (a run, or its prefix) at the owner's next position
    /// in batches under the policy's limits, folding each batch once
    /// written there; a node's `TooLarge` lowers the limits to its own.
    async fn write_run<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
        records: &[Vec<u8>],
    ) -> Result<(), CheckpointOutcome> {
        let mut limits = (
            self.policy.batch_records.max(1),
            self.policy.batch_bytes.max(1),
        );
        let mut at = 0;
        let mut batches = 0_u32;
        while at < records.len() {
            let rest = &records[at..];
            let take = CheckpointPolicy::batch_len(rest, limits);
            let expected = self.writer.next_seq();
            assert!(
                self.folder.next_seq() == expected,
                "the fold stands at the owner's next position"
            );
            let batch = rest[..take].iter().cloned().map(Value).collect();
            match self.writer.write(client, batch, first).await {
                WriterOutcome::Written { seq, count, .. }
                    if seq == expected && count == take as u64 =>
                {
                    for (position, record) in (seq..).zip(&rest[..take]) {
                        let folded = self.folder.fold(position, record);
                        assert!(
                            matches!(folded, Some(Folded::Run | Folded::Checkpoint { .. })),
                            "an owner's own run folds as a run"
                        );
                    }
                    at += take;
                    batches += 1;
                }
                WriterOutcome::TooLarge {
                    max_records,
                    max_bytes,
                } => {
                    let lowered = (
                        limits
                            .0
                            .min(usize::try_from(max_records).unwrap_or(usize::MAX)),
                        limits
                            .1
                            .min(usize::try_from(max_bytes).unwrap_or(usize::MAX)),
                    );
                    if CheckpointPolicy::batch_len(rest, lowered) >= take {
                        // One record is over the node's bytes: no batch
                        // holds it.
                        return Err(CheckpointOutcome::NotWritten(WriterOutcome::TooLarge {
                            max_records,
                            max_bytes,
                        }));
                    }
                    moonpool_assertions::reachable!("checkpoint: a node's limits split a run");
                    limits = lowered;
                }
                outcome => return Err(CheckpointOutcome::NotWritten(outcome)),
            }
        }
        if batches > 1 {
            moonpool_assertions::reachable!("checkpoint: a run over several batches");
        }
        Ok(())
    }

    /// The second step alone: the fenced `Truncate(up_to = seq)` (#228),
    /// `seq` a checkpoint this owner wrote. `None` when the writer no longer
    /// owns a generation: nothing is sent.
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal(), seq))]
    pub async fn truncate_to<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        seq: u64,
        first: usize,
    ) -> Option<TruncateOutcome> {
        self.writer.truncate(client, seq, first).await
    }
}

/// Fold `journal` into `folder` from where it stands up to `tail` (at
/// least), from server `first` on — what [`Checkpointer::load`] runs, for a
/// reader that owns nothing.
#[tracing::instrument(level = "debug", skip_all, fields(journal = %journal, tail))]
pub async fn load<P: ParosProviders, S: Checkpointable>(
    folder: &mut Folder<S>,
    journal: JournalIdentifier,
    client: &Client<P>,
    first: usize,
    tail: u64,
) -> LoadOutcome {
    let mut reader = Reader::new(journal, folder.next_seq());
    let mut restarted = false;
    let mut diverged = None;
    loop {
        match reader.next(client, first).await {
            ReaderOutcome::Records {
                from,
                records,
                state,
            } => {
                for (seq, record) in (from..).zip(&records) {
                    if let Some(Folded::Checkpoint {
                        verified: Some(false),
                        ..
                    }) = folder.fold(seq, record)
                    {
                        diverged = diverged.or(Some(seq));
                    }
                }
                let end = state.next_seq.0.max(tail);
                if records.is_empty() || folder.next_seq() >= end {
                    if !folder.is_whole() {
                        return LoadOutcome::Unhealed {
                            at: folder.next_seq(),
                        };
                    }
                    return LoadOutcome::Loaded {
                        up_to: folder.next_seq(),
                        restarted,
                        diverged,
                    };
                }
            }
            ReaderOutcome::Gap { resumed_at, .. } => {
                folder.jump(resumed_at);
                restarted = true;
            }
            ReaderOutcome::Unavailable => return LoadOutcome::Unavailable,
            ReaderOutcome::UnknownJournal => return LoadOutcome::UnknownJournal,
            ReaderOutcome::Denied(denial) => return LoadOutcome::Denied(denial),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A toy state: every entry's bytes, in order.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct Log(Vec<u8>);

    impl Checkpointable for Log {
        type Event = u64;

        fn apply(&mut self, seq: u64, record: &[u8]) -> u64 {
            self.0.extend_from_slice(record);
            seq
        }

        fn checkpoint(&self) -> Vec<u8> {
            self.0.clone()
        }

        fn restore(&mut self, _covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
            if state.first() == Some(&0xff) {
                return Err("not a log");
            }
            self.0 = state.to_vec();
            Ok(())
        }
    }

    /// Fold the run checkpointing `state` at `at`, in chunks of
    /// `chunk_bytes`: what its `End` folded to.
    fn fold_run(
        fold: &mut Folder<Log>,
        at: u64,
        state: &[u8],
        chunk_bytes: usize,
    ) -> Option<Folded<u64>> {
        let records = run(at, state, chunk_bytes);
        let (end, body) = records.split_last().unwrap();
        for (seq, record) in (at..).zip(body) {
            assert_eq!(fold.fold(seq, record), Some(Folded::Run));
        }
        fold.fold(at + body.len() as u64, end)
    }

    #[test]
    fn a_batch_takes_one_record_then_what_fits() {
        let records = vec![vec![0; 4], vec![0; 4], vec![0; 4]];
        assert_eq!(CheckpointPolicy::batch_len(&records, (8, 100)), 3);
        assert_eq!(CheckpointPolicy::batch_len(&records, (2, 100)), 2);
        assert_eq!(CheckpointPolicy::batch_len(&records, (8, 8)), 2);
        assert_eq!(CheckpointPolicy::batch_len(&records, (8, 1)), 1);
        assert_eq!(CheckpointPolicy::batch_len(&records, (0, 0)), 1);
    }

    #[test]
    fn the_policy_counts_entries_against_the_state_and_the_clock() {
        let policy = |factor| CheckpointPolicy {
            factor,
            interval: Duration::from_secs(10),
            chunk_bytes: 2,
            batch_records: 8,
            batch_bytes: 1 << 10,
        };
        // The toy state is every entry's bytes, so the log since the start
        // is exactly the state's size.
        let mut lazy = Checkpointer::new(
            JournalIdentifier::new(paros_core::TenantId(7), paros_core::JournalId(9)),
            1,
            Log::default(),
            policy(2),
        );
        assert!(!lazy.due(Duration::from_secs(100)), "nothing since");
        lazy.folder.fold(0, b"abcd");
        assert!(!lazy.due(Duration::from_secs(1)), "4 bytes against 2 × 4");
        assert!(lazy.due(Duration::from_secs(10)), "the clock says");
        let mut eager = Checkpointer::new(
            JournalIdentifier::new(paros_core::TenantId(7), paros_core::JournalId(9)),
            1,
            Log::default(),
            policy(1),
        );
        eager.folder.fold(0, b"abcd");
        assert!(eager.due(Duration::from_secs(1)), "4 bytes against 1 × 4");
        // A checkpoint resets the count.
        fold_run(&mut eager.folder, 1, b"abcd", 2);
        assert_eq!(eager.folder.since_checkpoint(), (0, 0));
        assert!(!eager.due(Duration::from_secs(100)));
    }
}
