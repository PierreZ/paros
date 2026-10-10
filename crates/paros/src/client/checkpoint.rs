//! Checkpoint and truncate (#227, #230): how any journal owner keeps its
//! journal bounded by live state rather than history, over the four calls
//! alone — paros-core never learns to compact.
//!
//! A journal owner folds its journal into some state. Once the log since its
//! last checkpoint has grown past a multiple of that state's size (or a time
//! bound passed), the owner:
//!
//! 1. writes the state as one **checkpoint record**, an ordinary fenced
//!    `Write` at the journal's next position `s` — the record says it covers
//!    every position below `s`;
//! 2. runs the fenced `Truncate(up_to = s)` (#228): the checkpoint becomes
//!    the journal's first record.
//!
//! The owner's own writes pause in between because one [`Checkpointer`]
//! holds the writer (`&mut self`). A reader loads by reading from the
//! journal's floor: the first record there is a checkpoint, which it
//! restores, and it folds forward from it.
//!
//! **The record format.** A checkpoint record is the 8-byte [`MAGIC`]
//! followed by a `paros.checkpoint.v1.CheckpointRecord` (`proto/checkpoint.proto`).
//! Every other record is the owner's own entry, untouched: paros still
//! decides nothing about it, and [`Checkpointer::append`] refuses an entry
//! that would read as a checkpoint. Both forms are read from day one:
//!
//! - `Inline { chunks }`: the state, chunked by key range (one chunk in M9);
//! - `Ref { journal_id, covers_up_to, end_seq, checksum }`: the chunks live
//!   in a separate checkpoint journal of the same tenant (Pulsar PIP-14's
//!   compacted ledger), checked by the CRC-32C of their concatenation.
//!
//! The writer emits `Inline` only (M9); writing `Ref` is #232's.
//!
//! **Crash handling.** A crash between the write and the truncate leaves a
//! checkpoint in the middle of the log: every fold **resets on it** (a fold
//! that held the full prefix verifies it first — [`Folded::Checkpoint`]'s
//! `verified`), and the next checkpoint truncates past it. A reader racing a
//! truncate is answered `truncated` naming the floor: it jumps there
//! ([`Folder::jump`]), where the checkpoint is, and restarts its fold from
//! it. A truncate from an owner superseded in between is refused (#228):
//! the checkpoint stays mid-log, harmless.
//!
//! The pure part, [`Folder`], is what every reader runs — the system
//! journals' node follower too; [`Checkpointer`] is the owner's async loop
//! over a [`Client`]. Like the rest of the client: provider-generic,
//! wasm-safe, no randomness, every outcome typed.

use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{JournalId, JournalIdentifier, LeaderUuid, Value};
use prost::Message as _;

use super::reader::{Reader, ReaderOutcome};
use super::writer::{Writer, WriterOutcome};
use super::{ClaimOutcome, Client, TruncateOutcome};
use crate::rpc::checkpoint as wire;

/// The prefix that marks a checkpoint record. A record that does not start
/// with it is an entry.
pub const MAGIC: &[u8; 8] = b"\x00PRSCKP\x01";

/// A state a journal owner folds, and can write down and read back.
///
/// `checkpoint` must be a pure function of the state — never of the history
/// that built it — so a fold that held every record and a fold restored from
/// a checkpoint compare equal byte for byte.
pub trait Checkpointable {
    /// What folding one entry yields (the caller's event).
    type Event;

    /// Fold the entry at position `seq` (positions arrive in order).
    fn apply(&mut self, seq: u64, record: &[u8]) -> Self::Event;

    /// The state, encoded.
    fn checkpoint(&self) -> Vec<u8>;

    /// Replace the state with `state`, a checkpoint covering every position
    /// below `covers_up_to` and written at `covers_up_to`: the next entry
    /// folded is past it.
    ///
    /// # Errors
    ///
    /// `state` does not decode; the state is left as it was.
    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str>;
}

/// Where a `Ref` checkpoint's state lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointRef {
    /// The checkpoint journal, in the same tenant.
    pub journal: JournalId,
    /// The horizon it was built for.
    pub covers_up_to: u64,
    /// Its records `[0, end_seq)` are the chunks.
    pub end_seq: u64,
    /// The CRC-32C of the chunks concatenated.
    pub checksum: u32,
}

/// A checkpoint record, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointRecord {
    /// The state inline: its chunks, in order.
    Inline {
        /// Every position below this one is covered.
        covers_up_to: u64,
        /// The state's chunks.
        chunks: Vec<Vec<u8>>,
    },
    /// The state in a checkpoint journal.
    Ref {
        /// Every position below this one is covered.
        covers_up_to: u64,
        /// Where the chunks are.
        at: CheckpointRef,
    },
}

impl CheckpointRecord {
    /// The position the record covers up to (exclusive).
    #[must_use]
    pub fn covers_up_to(&self) -> u64 {
        match self {
            CheckpointRecord::Inline { covers_up_to, .. }
            | CheckpointRecord::Ref { covers_up_to, .. } => *covers_up_to,
        }
    }

    /// The record's bytes: [`MAGIC`] and the encoded checkpoint.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let (covers_up_to, form) = match self {
            CheckpointRecord::Inline {
                covers_up_to,
                chunks,
            } => (
                *covers_up_to,
                wire::checkpoint_record::Form::Inline(wire::Inline {
                    chunks: chunks.clone(),
                }),
            ),
            CheckpointRecord::Ref { covers_up_to, at } => (
                *covers_up_to,
                wire::checkpoint_record::Form::Ref(wire::Ref {
                    journal_id: at.journal.0,
                    covers_up_to: at.covers_up_to,
                    end_seq: at.end_seq,
                    checksum: at.checksum,
                }),
            ),
        };
        let mut record = MAGIC.to_vec();
        record.extend(
            wire::CheckpointRecord {
                covers_up_to,
                form: Some(form),
            }
            .encode_to_vec(),
        );
        record
    }

    /// Read `record` back: `None` when it is not a checkpoint record (no
    /// [`MAGIC`]), `Some(Err)` when it carries the magic but does not decode.
    #[must_use]
    pub fn decode(record: &[u8]) -> Option<Result<Self, &'static str>> {
        let body = record.strip_prefix(MAGIC.as_slice())?;
        Some(Self::decode_body(body))
    }

    fn decode_body(body: &[u8]) -> Result<Self, &'static str> {
        let checkpoint = wire::CheckpointRecord::decode(body)
            .map_err(|_| "a checkpoint record does not decode")?;
        let covers_up_to = checkpoint.covers_up_to;
        match checkpoint.form.ok_or("a checkpoint names no form")? {
            wire::checkpoint_record::Form::Inline(inline) => Ok(CheckpointRecord::Inline {
                covers_up_to,
                chunks: inline.chunks,
            }),
            wire::checkpoint_record::Form::Ref(r) => {
                if r.covers_up_to != covers_up_to {
                    return Err("a checkpoint journal built for another horizon");
                }
                Ok(CheckpointRecord::Ref {
                    covers_up_to,
                    at: CheckpointRef {
                        journal: JournalId(r.journal_id),
                        covers_up_to: r.covers_up_to,
                        end_seq: r.end_seq,
                        checksum: r.checksum,
                    },
                })
            }
        }
    }
}

/// Whether `record` would read as a checkpoint.
#[must_use]
pub fn is_checkpoint(record: &[u8]) -> bool {
    record.starts_with(MAGIC)
}

/// The CRC-32C a `Ref` names: of the chunks concatenated, in order.
#[must_use]
pub fn chunks_checksum(chunks: &[Vec<u8>]) -> u32 {
    chunks
        .iter()
        .fold(0, |crc, chunk| crc32c::crc32c_append(crc, chunk))
}

/// What one record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Folded<E> {
    /// An entry, applied to a state that holds every position below it.
    Entry(E),
    /// A checkpoint covering every position below `covers_up_to`; the fold
    /// now holds its state. `verified` is `Some` when the fold already held
    /// every position below it and compared the checkpoint with its own
    /// state (`true`: equal), `None` when it restored from it.
    Checkpoint {
        /// The checkpoint's horizon (its own position).
        covers_up_to: u64,
        /// The comparison, when the fold could make it.
        verified: Option<bool>,
    },
    /// A `Ref` checkpoint a fold missing part of its prefix needs: read the
    /// chunks and hand them to [`Folder::restore_ref`] (or give up on it
    /// with [`Folder::skip`]). The fold does not move past it until then.
    NeedsRef(CheckpointRef),
    /// An entry above a gap: the fold cannot apply it, and waits for the
    /// next checkpoint.
    Skipped,
    /// A record carrying the magic that is no checkpoint this fold can use
    /// (it does not decode, its horizon is not its own position, or its
    /// state does not restore). Nothing changed.
    Unreadable(&'static str),
}

/// A reader's fold of one journal over a [`Checkpointable`] state: entries
/// applied in position order, checkpoints restored or verified, a gap jumped
/// and healed by the next checkpoint. Pure: every node and every client
/// folding the same records folds them the same way.
#[derive(Clone, Debug)]
pub struct Folder<S> {
    state: S,
    next: u64,
    /// Every position below `next` is in the state (from position 0, or
    /// from a checkpoint restored).
    whole: bool,
    /// Entries folded since the last checkpoint (or the log's start), and
    /// their bytes: the log a checkpoint would cover.
    since_entries: u64,
    since_bytes: u64,
}

impl<S: Checkpointable> Folder<S> {
    /// A fold of a journal from position 0 over `state` (the empty state).
    #[must_use]
    pub fn new(state: S) -> Self {
        Self {
            state,
            next: 0,
            whole: true,
            since_entries: 0,
            since_bytes: 0,
        }
    }

    /// The entries folded since the last checkpoint (or the log's start),
    /// and their bytes.
    #[must_use]
    pub fn since_checkpoint(&self) -> (u64, u64) {
        (self.since_entries, self.since_bytes)
    }

    /// The state folded so far.
    #[must_use]
    pub fn state(&self) -> &S {
        &self.state
    }

    /// The next position the fold expects.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next
    }

    /// Whether the state holds every position below [`Folder::next_seq`]:
    /// `false` after a gap, until a checkpoint heals it.
    #[must_use]
    pub fn is_whole(&self) -> bool {
        self.whole
    }

    /// The positions below `floor` are gone (a `truncated` answer, a page
    /// that starts past the cursor): move past them. The state no longer
    /// holds them, so the fold waits for a checkpoint — the one at the floor,
    /// when the floor is a checkpoint's.
    pub fn jump(&mut self, floor: u64) {
        if floor > self.next {
            self.next = floor;
            self.whole = false;
        }
    }

    /// Fold the record at position `seq`. `None` for a position already
    /// folded (pages may overlap); a position past the next one is a gap
    /// ([`Folder::jump`]) first.
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> Option<Folded<S::Event>> {
        if seq < self.next {
            return None;
        }
        self.jump(seq);
        let Some(decoded) = CheckpointRecord::decode(record) else {
            self.next = seq + 1;
            if !self.whole {
                return Some(Folded::Skipped);
            }
            self.since_entries += 1;
            self.since_bytes += record.len() as u64;
            return Some(Folded::Entry(self.state.apply(seq, record)));
        };
        let checkpoint = match decoded {
            Ok(checkpoint) if checkpoint.covers_up_to() == seq => checkpoint,
            Ok(_) => {
                self.next = seq + 1;
                return Some(Folded::Unreadable("a checkpoint not at its own horizon"));
            }
            Err(reason) => {
                self.next = seq + 1;
                return Some(Folded::Unreadable(reason));
            }
        };
        match checkpoint {
            CheckpointRecord::Inline { chunks, .. } => Some(self.restore(seq, &chunks.concat())),
            // A whole fold holds the state already: it has nothing to read.
            CheckpointRecord::Ref { .. } if self.whole => {
                self.next = seq + 1;
                self.since_entries = 0;
                self.since_bytes = 0;
                Some(Folded::Checkpoint {
                    covers_up_to: seq,
                    verified: None,
                })
            }
            CheckpointRecord::Ref { at, .. } => Some(Folded::NeedsRef(at)),
        }
    }

    /// Restore from the chunks a [`Folded::NeedsRef`] at `seq` named, read
    /// from its checkpoint journal; refused (unreadable) when their checksum
    /// is not the one the reference names.
    pub fn restore_ref(
        &mut self,
        seq: u64,
        at: &CheckpointRef,
        chunks: &[Vec<u8>],
    ) -> Folded<S::Event> {
        if seq < self.next {
            return Folded::Unreadable("a checkpoint already folded");
        }
        if chunks_checksum(chunks) != at.checksum {
            self.next = seq + 1;
            return Folded::Unreadable("a checkpoint journal that fails its checksum");
        }
        self.restore(seq, &chunks.concat())
    }

    /// Give up on the record at `seq` (a `Ref` whose journal could not be
    /// read): move past it, still waiting for a checkpoint.
    pub fn skip(&mut self, seq: u64) {
        if seq >= self.next {
            self.next = seq + 1;
        }
    }

    fn restore(&mut self, seq: u64, state: &[u8]) -> Folded<S::Event> {
        let verified = self.whole.then(|| self.state.checkpoint() == state);
        self.next = seq + 1;
        if verified == Some(true) {
            self.since_entries = 0;
            self.since_bytes = 0;
            return Folded::Checkpoint {
                covers_up_to: seq,
                verified,
            };
        }
        match self.state.restore(seq, state) {
            Ok(()) => {
                self.whole = true;
                self.since_entries = 0;
                self.since_bytes = 0;
                Folded::Checkpoint {
                    covers_up_to: seq,
                    verified,
                }
            }
            Err(reason) => Folded::Unreadable(reason),
        }
    }
}

/// When an owner checkpoints, from [`super::ClientTunables`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointPolicy {
    /// Checkpoint once the entries since the last checkpoint reach this many
    /// times the state's size (so checkpoints cost at most `1/k` extra
    /// writes). Floor 1.
    pub factor: u32,
    /// Checkpoint once this long has passed since the last one, with any
    /// entry since. Floor 0: after every entry.
    pub interval: Duration,
}

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
    pub async fn open<P: Providers>(&mut self, client: &Client<P>, first: usize) -> OpenOutcome {
        let claim = self.writer.claim(client, first, false).await;
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
    /// restarts from the checkpoint there; a `Ref` checkpoint is read from
    /// its checkpoint journal.
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal(), tail))]
    pub async fn load<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        tail: u64,
    ) -> LoadOutcome {
        load(&mut self.folder, self.writer.journal(), client, first, tail).await
    }

    /// Append `record` as the owner (see [`Writer::write`]) and fold it once
    /// written at the fold's next position.
    pub async fn append<P: Providers>(
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
    pub async fn checkpoint<P: Providers>(
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

    /// The first step alone: write the fold's state as an `Inline`
    /// checkpoint at the owner's next position. On its own (a harness's
    /// crash between the two steps) it leaves the checkpoint mid-log.
    ///
    /// # Errors
    ///
    /// [`CheckpointOutcome::NotFolded`] when the fold is not at the owner's
    /// next position; [`CheckpointOutcome::NotWritten`] when the write is not
    /// known written (an ambiguous one may still land).
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal(), seq = self.writer.next_seq()))]
    pub async fn write_checkpoint<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<u64, CheckpointOutcome> {
        let seq = self.writer.next_seq();
        if !self.folder.is_whole() || self.folder.next_seq() != seq {
            return Err(CheckpointOutcome::NotFolded);
        }
        let record = CheckpointRecord::Inline {
            covers_up_to: seq,
            chunks: vec![self.folder.state().checkpoint()],
        }
        .encode();
        match self
            .writer
            .write(client, vec![Value(record.clone())], first)
            .await
        {
            WriterOutcome::Written { seq: at, .. } if at == seq => {
                self.folder.fold(seq, &record);
                self.last = client.now();
                Ok(seq)
            }
            outcome => Err(CheckpointOutcome::NotWritten(outcome)),
        }
    }

    /// The second step alone: the fenced `Truncate(up_to = seq)` (#228),
    /// `seq` a checkpoint this owner wrote. `None` when the writer no longer
    /// owns a generation: nothing is sent.
    #[tracing::instrument(level = "debug", skip_all, fields(journal = %self.writer.journal(), seq))]
    pub async fn truncate_to<P: Providers>(
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
pub async fn load<P: Providers, S: Checkpointable>(
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
                let mut pending = None;
                for (seq, record) in (from..).zip(&records) {
                    match folder.fold(seq, record) {
                        Some(Folded::NeedsRef(at)) => {
                            pending = Some((seq, at));
                            break;
                        }
                        Some(Folded::Checkpoint {
                            verified: Some(false),
                            ..
                        }) => diverged = diverged.or(Some(seq)),
                        _ => {}
                    }
                }
                if let Some((seq, at)) = pending {
                    match read_chunks(client, journal, &at, first).await {
                        Some(chunks) => {
                            if matches!(
                                folder.restore_ref(seq, &at, &chunks),
                                Folded::Checkpoint { .. }
                            ) {
                                restarted = true;
                            }
                        }
                        None => folder.skip(seq),
                    }
                    reader = Reader::new(journal, folder.next_seq());
                    continue;
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
        }
    }
}

/// Read a `Ref` checkpoint's chunks: records `[0, end_seq)` of its
/// checkpoint journal, in the same tenant. `None` when they cannot all be
/// read.
async fn read_chunks<P: Providers>(
    client: &Client<P>,
    journal: JournalIdentifier,
    at: &CheckpointRef,
    first: usize,
) -> Option<Vec<Vec<u8>>> {
    let mut reader = Reader::new(JournalIdentifier::new(journal.tenant, at.journal), 0);
    let mut chunks = Vec::new();
    while reader.cursor() < at.end_seq {
        match reader.next(client, first).await {
            ReaderOutcome::Records { records, .. } if !records.is_empty() => {
                chunks.extend(records);
            }
            _ => return None,
        }
    }
    chunks.truncate(usize::try_from(at.end_seq).ok()?);
    Some(chunks)
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

    fn inline(at: u64, state: &[u8]) -> Vec<u8> {
        CheckpointRecord::Inline {
            covers_up_to: at,
            chunks: vec![state.to_vec()],
        }
        .encode()
    }

    #[test]
    fn both_forms_round_trip_and_an_entry_is_never_a_checkpoint() {
        let forms = [
            CheckpointRecord::Inline {
                covers_up_to: 9,
                chunks: vec![b"ab".to_vec(), b"c".to_vec()],
            },
            CheckpointRecord::Ref {
                covers_up_to: 9,
                at: CheckpointRef {
                    journal: JournalId(400),
                    covers_up_to: 9,
                    end_seq: 2,
                    checksum: chunks_checksum(&[b"ab".to_vec(), b"c".to_vec()]),
                },
            },
        ];
        for form in forms {
            assert_eq!(CheckpointRecord::decode(&form.encode()), Some(Ok(form)));
        }
        assert_eq!(CheckpointRecord::decode(b"an entry"), None);
        assert!(matches!(CheckpointRecord::decode(MAGIC), Some(Err(_))));
        assert_eq!(
            chunks_checksum(&[b"ab".to_vec(), b"c".to_vec()]),
            crc32c::crc32c(b"abc")
        );
    }

    #[test]
    fn a_gap_is_skipped_until_a_checkpoint_heals_it() {
        let mut fold = Folder::new(Log::default());
        assert_eq!(fold.fold(0, b"a"), Some(Folded::Entry(0)));
        assert_eq!(fold.fold(0, b"a"), None, "a position is folded once");
        // A truncation overtook the cursor: the fold jumps to the floor.
        fold.jump(5);
        assert!(!fold.is_whole());
        assert_eq!(fold.fold(5, b"x"), Some(Folded::Skipped));
        // A checkpoint mid-log (a crash before its truncate) heals it.
        assert_eq!(
            fold.fold(6, &inline(6, b"abcdex")),
            Some(Folded::Checkpoint {
                covers_up_to: 6,
                verified: None
            })
        );
        assert!(fold.is_whole());
        assert_eq!(fold.fold(7, b"y"), Some(Folded::Entry(7)));
        assert_eq!(fold.state(), &Log(b"abcdexy".to_vec()));
    }

    #[test]
    fn a_whole_fold_verifies_a_checkpoint_and_resets_on_a_wrong_one() {
        let mut fold = Folder::new(Log::default());
        fold.fold(0, b"a");
        assert_eq!(
            fold.fold(1, &inline(1, b"a")),
            Some(Folded::Checkpoint {
                covers_up_to: 1,
                verified: Some(true)
            })
        );
        assert_eq!(
            fold.fold(2, &inline(2, b"zz")),
            Some(Folded::Checkpoint {
                covers_up_to: 2,
                verified: Some(false)
            })
        );
        assert_eq!(fold.state(), &Log(b"zz".to_vec()), "the fold resets on it");
    }

    #[test]
    fn an_unusable_checkpoint_changes_nothing() {
        let mut fold = Folder::new(Log::default());
        fold.fold(0, b"a");
        // Not at its own horizon.
        assert!(matches!(
            fold.fold(1, &inline(0, b"")),
            Some(Folded::Unreadable(_))
        ));
        // Carries the magic, decodes to nothing.
        let mut junk = MAGIC.to_vec();
        junk.extend_from_slice(b"\xff\xff\xff");
        assert!(matches!(fold.fold(2, &junk), Some(Folded::Unreadable(_))));
        // A state that does not restore.
        fold.jump(4);
        assert!(matches!(
            fold.fold(4, &inline(4, b"\xff")),
            Some(Folded::Unreadable(_))
        ));
        assert!(!fold.is_whole());
        assert_eq!(fold.next_seq(), 5);
    }

    #[test]
    fn a_ref_is_read_only_by_a_fold_that_needs_it() {
        let chunks = vec![b"ab".to_vec(), b"c".to_vec()];
        let at = CheckpointRef {
            journal: JournalId(400),
            covers_up_to: 3,
            end_seq: 2,
            checksum: chunks_checksum(&chunks),
        };
        let record = CheckpointRecord::Ref {
            covers_up_to: 3,
            at,
        }
        .encode();
        let mut whole = Folder::new(Log::default());
        for (seq, entry) in (0..).zip([b"a", b"b", b"c"]) {
            whole.fold(seq, entry);
        }
        assert_eq!(
            whole.fold(3, &record),
            Some(Folded::Checkpoint {
                covers_up_to: 3,
                verified: None
            })
        );
        let mut jumped = Folder::new(Log::default());
        jumped.jump(3);
        assert_eq!(jumped.fold(3, &record), Some(Folded::NeedsRef(at)));
        assert_eq!(jumped.next_seq(), 3, "it waits on the reference");
        assert!(matches!(
            jumped.clone().restore_ref(3, &at, &[b"abd".to_vec()]),
            Folded::Unreadable(_)
        ));
        assert_eq!(
            jumped.restore_ref(3, &at, &chunks),
            Folded::Checkpoint {
                covers_up_to: 3,
                verified: None
            }
        );
        assert_eq!(jumped.state(), whole.state());
    }

    #[test]
    fn the_policy_counts_entries_against_the_state_and_the_clock() {
        let policy = |factor| CheckpointPolicy {
            factor,
            interval: Duration::from_secs(10),
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
        eager.folder.fold(1, &inline(1, b"abcd"));
        assert_eq!(eager.folder.since_checkpoint(), (0, 0));
        assert!(!eager.due(Duration::from_secs(100)));
    }
}
