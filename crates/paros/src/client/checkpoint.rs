//! Checkpoint and truncate (#227, #230, #353): how any journal owner keeps
//! its journal bounded by live state rather than history, over the four
//! calls alone — paros-core never learns to compact.
//!
//! A journal owner folds its journal into some state. Once the log since its
//! last checkpoint has grown past a multiple of that state's size (or a time
//! bound passed), the owner:
//!
//! 1. writes the state as a **checkpoint run** at the journal's next
//!    position `s`: a `Begin`, the state cut into small `Chunk` records, and
//!    an `End`, in ordinary fenced `Write` batches. `Begin` says the run
//!    covers every position below `s`;
//! 2. once `End` is written, runs the fenced `Truncate(up_to = s)` (#228):
//!    `Begin` becomes the journal's first record.
//!
//! The owner's own writes pause in between because one [`Checkpointer`]
//! holds the writer (`&mut self`): no entry is ever inside a run. A small
//! state fits one batch, so one slot; a large one takes as many small
//! batches as it needs, so no checkpoint is one large record (#353). A
//! reader loads by reading from the journal's floor: the run there is a
//! checkpoint, which it restores, and it folds forward from it.
//!
//! **The record format.** A run record is the 8-byte [`MAGIC`] followed by a
//! `paros.checkpoint.v1.CheckpointRecord` (`proto/checkpoint.proto`). Every
//! other record is the owner's own entry, untouched: paros still decides
//! nothing about it, and [`Checkpointer::append`] refuses an entry that
//! would read as a run record. `End` is the **commit point**: it names the
//! count of the run's chunks and the CRC-32C of the chunks concatenated. A
//! run with no `End`, or with a wrong count or checksum, is no checkpoint.
//! The retired `Inline` and `Ref` forms (#230) decode to no form and are
//! refused: no cell ever wrote `Ref`, and no cell outlives the change.
//!
//! **Crash handling.** A crash inside a run leaves it with no `End`: the
//! next record is another owner's entry or `Begin`, and every fold drops the
//! run it was collecting. A crash between `End` and the truncate leaves a
//! checkpoint in the middle of the log: every fold **resets on it** (a fold
//! that held the full prefix verifies it first — [`Folded::Checkpoint`]'s
//! `verified`), and the next checkpoint truncates past it. A reader racing a
//! truncate is answered `truncated` naming the floor: it jumps there
//! ([`Folder::jump`]), where a run begins, and restarts its fold from it. A
//! write or truncate from an owner superseded in between is refused (#228).
//!
//! The pure part, [`Folder`], is what every reader runs — the system
//! journals' node follower too; [`Checkpointer`] is the owner's async loop
//! over a [`Client`]. Like the rest of the client: provider-generic,
//! wasm-safe, no randomness, every outcome typed.

use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{JournalIdentifier, LeaderUuid, Value};
use prost::Message as _;

use super::reader::{Reader, ReaderOutcome};
use super::writer::{Writer, WriterOutcome};
use super::{ClaimOutcome, Client, TruncateOutcome};
use crate::rpc::checkpoint as wire;

/// The prefix that marks a checkpoint run record. A record that does not
/// start with it is an entry.
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
    /// below `covers_up_to`, whose run begins at `covers_up_to`: the next
    /// entry folded is past it.
    ///
    /// # Errors
    ///
    /// `state` does not decode; the state is left as it was.
    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str>;
}

/// One record of a checkpoint run, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointRecord {
    /// The start of a run, at position `covers_up_to`.
    Begin {
        /// Every position below this one is covered.
        covers_up_to: u64,
    },
    /// One piece of the state, in position order.
    Chunk(Vec<u8>),
    /// The commit point: the run's chunk count and checksum.
    End {
        /// The count of the run's chunks.
        chunks: u64,
        /// The CRC-32C of the chunks concatenated ([`chunks_checksum`]).
        checksum: u32,
    },
}

impl CheckpointRecord {
    /// The record's bytes: [`MAGIC`] and the encoded record.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let form = match self {
            CheckpointRecord::Begin { covers_up_to } => {
                wire::checkpoint_record::Form::Begin(wire::Begin {
                    covers_up_to: *covers_up_to,
                })
            }
            CheckpointRecord::Chunk(bytes) => wire::checkpoint_record::Form::Chunk(wire::Chunk {
                bytes: bytes.clone(),
            }),
            CheckpointRecord::End { chunks, checksum } => {
                wire::checkpoint_record::Form::End(wire::End {
                    chunks: *chunks,
                    checksum: *checksum,
                })
            }
        };
        let mut record = MAGIC.to_vec();
        record.extend(wire::CheckpointRecord { form: Some(form) }.encode_to_vec());
        record
    }

    /// Read `record` back: `None` when it is not a run record (no
    /// [`MAGIC`]), `Some(Err)` when it carries the magic but does not decode.
    #[must_use]
    pub fn decode(record: &[u8]) -> Option<Result<Self, &'static str>> {
        let body = record.strip_prefix(MAGIC.as_slice())?;
        Some(Self::decode_body(body))
    }

    fn decode_body(body: &[u8]) -> Result<Self, &'static str> {
        let checkpoint = wire::CheckpointRecord::decode(body)
            .map_err(|_| "a checkpoint record does not decode")?;
        Ok(
            match checkpoint.form.ok_or("a checkpoint record names no form")? {
                wire::checkpoint_record::Form::Begin(begin) => CheckpointRecord::Begin {
                    covers_up_to: begin.covers_up_to,
                },
                wire::checkpoint_record::Form::Chunk(chunk) => CheckpointRecord::Chunk(chunk.bytes),
                wire::checkpoint_record::Form::End(end) => CheckpointRecord::End {
                    chunks: end.chunks,
                    checksum: end.checksum,
                },
            },
        )
    }
}

/// Whether `record` would read as a checkpoint run record.
#[must_use]
pub fn is_checkpoint(record: &[u8]) -> bool {
    record.starts_with(MAGIC)
}

/// The CRC-32C an `End` names: of the chunks concatenated, in order.
#[must_use]
pub fn chunks_checksum(chunks: &[Vec<u8>]) -> u32 {
    chunks
        .iter()
        .fold(0, |crc, chunk| crc32c::crc32c_append(crc, chunk))
}

/// The run that checkpoints `state` at position `covers_up_to`, encoded: a
/// `Begin`, `state` cut into chunks of at most `chunk_bytes` bytes (floor
/// 1), and the `End` that commits them. An empty state has no chunk.
///
/// # Panics
///
/// Never: the run's shape is asserted.
#[must_use]
pub fn run(covers_up_to: u64, state: &[u8], chunk_bytes: usize) -> Vec<Vec<u8>> {
    let chunks: Vec<Vec<u8>> = state
        .chunks(chunk_bytes.max(1))
        .map(<[u8]>::to_vec)
        .collect();
    let end = CheckpointRecord::End {
        chunks: chunks.len() as u64,
        checksum: chunks_checksum(&chunks),
    };
    let mut records = Vec::with_capacity(chunks.len() + 2);
    records.push(CheckpointRecord::Begin { covers_up_to }.encode());
    records.extend(
        chunks
            .into_iter()
            .map(|c| CheckpointRecord::Chunk(c).encode()),
    );
    records.push(end.encode());
    assert!(records.len() >= 2, "a run holds its Begin and its End");
    assert!(
        records.iter().all(|r| is_checkpoint(r)),
        "every record of a run reads as one"
    );
    records
}

/// What one record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Folded<E> {
    /// An entry, applied to a state that holds every position below it.
    Entry(E),
    /// A run's `End`, valid: the fold now holds the checkpoint's state,
    /// which covers every position below `covers_up_to` (its `Begin`).
    /// `verified` is `Some` when the fold already held every position below
    /// it and compared the checkpoint with its own state (`true`: equal),
    /// `None` when it restored from it.
    Checkpoint {
        /// The checkpoint's horizon (the position of its `Begin`).
        covers_up_to: u64,
        /// The comparison, when the fold could make it.
        verified: Option<bool>,
    },
    /// A run's `Begin` or `Chunk`, collected (or a chunk of no run,
    /// dropped). The state is unchanged until the run's `End`.
    Run,
    /// An entry above a gap: the fold cannot apply it, and waits for the
    /// next checkpoint.
    Skipped,
    /// A record carrying the magic that is no checkpoint this fold can use
    /// (it does not decode, a `Begin` not at its own position, an `End`
    /// with no run or a wrong count or checksum, or a state that does not
    /// restore). The run it ends is dropped; nothing else changed.
    Unreadable(&'static str),
}

/// A run a fold is collecting: from its `Begin`, the chunks so far.
#[derive(Clone, Debug)]
struct OpenRun {
    /// The position of its `Begin`.
    begin: u64,
    /// The chunks concatenated.
    state: Vec<u8>,
    /// The chunks collected.
    chunks: u64,
    /// Their CRC-32C so far.
    checksum: u32,
}

/// A reader's fold of one journal over a [`Checkpointable`] state: entries
/// applied in position order, checkpoint runs collected and, at their
/// `End`, restored or verified, a gap jumped and healed by the next
/// checkpoint. Pure: every node and every client folding the same records
/// folds them the same way.
#[derive(Clone, Debug)]
pub struct Folder<S> {
    state: S,
    next: u64,
    /// Every position below `next` is in the state (from position 0, or
    /// from a checkpoint restored).
    whole: bool,
    /// The run being collected, if any: every position from its `Begin` up
    /// to `next` is one of its records.
    run: Option<OpenRun>,
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
            run: None,
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

    /// Whether the fold is inside a run with no `End` yet.
    #[must_use]
    pub fn in_run(&self) -> bool {
        self.run.is_some()
    }

    /// The positions below `floor` are gone (a `truncated` answer, a page
    /// that starts past the cursor): move past them. The state no longer
    /// holds them, so the fold waits for a checkpoint — the one at the floor,
    /// when the floor is a run's `Begin`. A run being collected lost records:
    /// it is dropped.
    pub fn jump(&mut self, floor: u64) {
        if floor > self.next {
            self.next = floor;
            self.whole = false;
            self.run = None;
        }
    }

    /// Fold the record at position `seq`. `None` for a position already
    /// folded (pages may overlap); a position past the next one is a gap
    /// ([`Folder::jump`]) first.
    ///
    /// # Panics
    ///
    /// When the fold's own bookkeeping breaks (a run with a hole): never on
    /// a record's content.
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> Option<Folded<S::Event>> {
        if seq < self.next {
            return None;
        }
        self.jump(seq);
        assert!(seq == self.next, "a fold takes the next position");
        self.next = seq + 1;
        let Some(decoded) = CheckpointRecord::decode(record) else {
            // An entry: the owner that wrote a run before it stopped inside
            // it, and the run has no `End`.
            self.drop_run();
            if !self.whole {
                return Some(Folded::Skipped);
            }
            self.since_entries += 1;
            self.since_bytes += record.len() as u64;
            return Some(Folded::Entry(self.state.apply(seq, record)));
        };
        let decoded = match decoded {
            Ok(decoded) => decoded,
            Err(reason) => {
                self.drop_run();
                return Some(Folded::Unreadable(reason));
            }
        };
        Some(match decoded {
            CheckpointRecord::Begin { covers_up_to } if covers_up_to != seq => {
                self.drop_run();
                Folded::Unreadable("a checkpoint run not at its own horizon")
            }
            CheckpointRecord::Begin { .. } => {
                // A new owner's run after one that stopped inside its own.
                self.drop_run();
                self.run = Some(OpenRun {
                    begin: seq,
                    state: Vec::new(),
                    chunks: 0,
                    checksum: 0,
                });
                Folded::Run
            }
            CheckpointRecord::Chunk(bytes) => {
                if let Some(run) = &mut self.run {
                    assert!(
                        run.begin + 1 + run.chunks == seq,
                        "a run's chunks follow its Begin with no hole"
                    );
                    run.checksum = crc32c::crc32c_append(run.checksum, &bytes);
                    run.state.extend_from_slice(&bytes);
                    run.chunks += 1;
                }
                Folded::Run
            }
            CheckpointRecord::End { chunks, checksum } => {
                let Some(run) = self.run.take() else {
                    return Some(Folded::Unreadable("a checkpoint end with no run"));
                };
                if run.chunks != chunks {
                    return Some(Folded::Unreadable(
                        "a checkpoint run with a wrong chunk count",
                    ));
                }
                if run.checksum != checksum {
                    return Some(Folded::Unreadable(
                        "a checkpoint run that fails its checksum",
                    ));
                }
                assert!(
                    run.begin + 1 + run.chunks == seq,
                    "a run's End follows its last chunk"
                );
                self.restore(run.begin, &run.state)
            }
        })
    }

    /// Drop the run being collected: it has no `End`, and never will.
    fn drop_run(&mut self) {
        if self.run.take().is_some() {
            moonpool_assertions::reachable!("checkpoint: a run with no end is ignored");
        }
    }

    /// Restore (or verify) from a valid run that began at `begin`; the fold
    /// already stands past its `End`.
    fn restore(&mut self, begin: u64, state: &[u8]) -> Folded<S::Event> {
        assert!(self.run.is_none(), "a run is restored once, at its End");
        assert!(begin < self.next, "a run's Begin is below its End");
        let verified = self.whole.then(|| self.state.checkpoint() == state);
        if verified == Some(true) {
            self.since_entries = 0;
            self.since_bytes = 0;
            return Folded::Checkpoint {
                covers_up_to: begin,
                verified,
            };
        }
        match self.state.restore(begin, state) {
            Ok(()) => {
                self.whole = true;
                self.since_entries = 0;
                self.since_bytes = 0;
                Folded::Checkpoint {
                    covers_up_to: begin,
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
    /// The most state bytes in one `Chunk` record. Floor 1: a chunk per
    /// byte, many records but a valid run. Keep it well under the server's
    /// batch bytes, or no batch holds a chunk.
    pub chunk_bytes: usize,
    /// The most run records in one `Write`. Floor 1: a batch per record.
    /// A node that answers `TooLarge` lowers it for the rest of the run.
    pub batch_records: usize,
    /// The most run record bytes in one `Write` (a batch always takes one
    /// record). Floor 1. A node that answers `TooLarge` lowers it for the
    /// rest of the run.
    pub batch_bytes: usize,
}

impl CheckpointPolicy {
    /// How many of `records` the next batch takes under `limits` (records,
    /// bytes): at least one, then as many as fit.
    fn batch_len(records: &[Vec<u8>], (max_records, max_bytes): (usize, usize)) -> usize {
        assert!(!records.is_empty(), "a batch takes a record");
        let mut bytes = 0;
        let mut taken = 0;
        for record in records.iter().take(max_records.max(1)) {
            if taken > 0 && bytes + record.len() > max_bytes {
                break;
            }
            bytes += record.len();
            taken += 1;
        }
        assert!(taken >= 1, "a batch always takes one record");
        taken
    }
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
    /// restarts from the checkpoint run there.
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
    pub async fn write_checkpoint<P: Providers>(
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
    pub async fn write_run_without_end<P: Providers>(
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
    async fn write_run<P: Providers>(
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
    fn every_record_round_trips_and_an_entry_is_never_a_checkpoint() {
        let records = [
            CheckpointRecord::Begin { covers_up_to: 9 },
            CheckpointRecord::Chunk(b"ab".to_vec()),
            CheckpointRecord::End {
                chunks: 2,
                checksum: chunks_checksum(&[b"ab".to_vec(), b"c".to_vec()]),
            },
        ];
        for record in records {
            assert_eq!(CheckpointRecord::decode(&record.encode()), Some(Ok(record)));
        }
        assert_eq!(CheckpointRecord::decode(b"an entry"), None);
        assert!(matches!(CheckpointRecord::decode(MAGIC), Some(Err(_))));
        assert_eq!(
            chunks_checksum(&[b"ab".to_vec(), b"c".to_vec()]),
            crc32c::crc32c(b"abc")
        );
        // A run cuts the state into chunks and commits them.
        let records = run(4, b"abcde", 2);
        assert_eq!(records.len(), 5, "Begin, three chunks, End");
        assert_eq!(
            CheckpointRecord::decode(&records[4]),
            Some(Ok(CheckpointRecord::End {
                chunks: 3,
                checksum: crc32c::crc32c(b"abcde")
            }))
        );
        assert_eq!(run(4, b"", 2).len(), 2, "an empty state has no chunk");
    }

    #[test]
    fn the_retired_forms_are_refused() {
        // `CheckpointRecord { covers_up_to: 3, inline: { chunks: ["a"] } }`,
        // as #230 wrote it: fields 1 and 2.
        let mut old = MAGIC.to_vec();
        old.extend_from_slice(&[0x08, 0x03, 0x12, 0x03, 0x0a, 0x01, b'a']);
        assert!(matches!(CheckpointRecord::decode(&old), Some(Err(_))));
        let mut fold = Folder::new(Log::default());
        assert!(matches!(fold.fold(0, &old), Some(Folded::Unreadable(_))));
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
            fold_run(&mut fold, 6, b"abcdex", 4),
            Some(Folded::Checkpoint {
                covers_up_to: 6,
                verified: None
            })
        );
        assert!(fold.is_whole());
        assert_eq!(fold.next_seq(), 10, "past the run's End");
        assert_eq!(fold.fold(10, b"y"), Some(Folded::Entry(10)));
        assert_eq!(fold.state(), &Log(b"abcdexy".to_vec()));
    }

    #[test]
    fn a_whole_fold_verifies_a_checkpoint_and_resets_on_a_wrong_one() {
        let mut fold = Folder::new(Log::default());
        fold.fold(0, b"a");
        assert_eq!(
            fold_run(&mut fold, 1, b"a", 1),
            Some(Folded::Checkpoint {
                covers_up_to: 1,
                verified: Some(true)
            })
        );
        assert_eq!(
            fold_run(&mut fold, 4, b"zz", 1),
            Some(Folded::Checkpoint {
                covers_up_to: 4,
                verified: Some(false)
            })
        );
        assert_eq!(fold.state(), &Log(b"zz".to_vec()), "the fold resets on it");
    }

    #[test]
    fn a_run_with_no_valid_end_is_never_restored() {
        let mut fold = Folder::new(Log::default());
        fold.jump(2);
        // A run its owner stopped inside: the next owner's entry drops it.
        let records = run(2, b"abc", 1);
        for (seq, record) in (2..).zip(&records[..3]) {
            assert_eq!(fold.fold(seq, record), Some(Folded::Run));
        }
        assert!(fold.in_run());
        assert_eq!(fold.fold(5, b"x"), Some(Folded::Skipped));
        assert!(!fold.in_run() && !fold.is_whole());
        // Its `End`, landing late, ends no run.
        assert!(matches!(
            fold.fold(6, &records[4]),
            Some(Folded::Unreadable(_))
        ));
        // A new `Begin` supersedes an open run.
        let again = run(7, b"abc", 1);
        for (seq, record) in (7..).zip(&again[..2]) {
            assert_eq!(fold.fold(seq, record), Some(Folded::Run));
        }
        assert_eq!(
            fold_run(&mut fold, 9, b"q", 1),
            Some(Folded::Checkpoint {
                covers_up_to: 9,
                verified: None
            })
        );
        // A wrong count, then a wrong checksum.
        let mut fold = Folder::new(Log::default());
        fold.jump(1);
        let records = run(1, b"ab", 1);
        let short = CheckpointRecord::End {
            chunks: 1,
            checksum: crc32c::crc32c(b"ab"),
        }
        .encode();
        for (seq, record) in (1..).zip(&records[..3]) {
            fold.fold(seq, record);
        }
        assert!(matches!(fold.fold(4, &short), Some(Folded::Unreadable(_))));
        let records = run(5, b"ab", 1);
        let wrong = CheckpointRecord::End {
            chunks: 2,
            checksum: crc32c::crc32c(b"ba"),
        }
        .encode();
        for (seq, record) in (5..).zip(&records[..3]) {
            fold.fold(seq, record);
        }
        assert!(matches!(fold.fold(8, &wrong), Some(Folded::Unreadable(_))));
        assert!(!fold.is_whole(), "nothing restored");
        // A gap inside a run drops it.
        let records = run(9, b"ab", 1);
        fold.fold(9, &records[0]);
        fold.jump(11);
        assert!(!fold.in_run());
        assert!(matches!(
            fold.fold(11, &records[3]),
            Some(Folded::Unreadable(_))
        ));
    }

    #[test]
    fn an_unusable_checkpoint_changes_nothing() {
        let mut fold = Folder::new(Log::default());
        fold.fold(0, b"a");
        // Not at its own horizon.
        assert!(matches!(
            fold.fold(1, &CheckpointRecord::Begin { covers_up_to: 0 }.encode()),
            Some(Folded::Unreadable(_))
        ));
        // Carries the magic, decodes to nothing.
        let mut junk = MAGIC.to_vec();
        junk.extend_from_slice(b"\xff\xff\xff");
        assert!(matches!(fold.fold(2, &junk), Some(Folded::Unreadable(_))));
        // A state that does not restore.
        fold.jump(4);
        assert!(matches!(
            fold_run(&mut fold, 4, b"\xff", 1),
            Some(Folded::Unreadable(_))
        ));
        assert!(!fold.is_whole());
        assert_eq!(fold.next_seq(), 7);
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
