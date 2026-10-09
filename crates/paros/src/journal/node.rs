//! [`JournalStorage`]: the node's [`LogStorage`] on `moonpool-journal`.

use std::collections::BTreeMap;

use moonpool_buggify::hint::Strike;
use moonpool_core::StorageProvider;
use moonpool_journal::{Batch, Journal, ReadError, Recovery};
use paros_core::{Ballot, Command, Config, HardState, JournalState, MustSync, Slot, Storage};
use serde::{Deserialize, Serialize};

use super::{
    JournalStoreConfig, ballot_id, commit_error, decode, encode, id_ballot, open_error, store_id,
    undecodable,
};
use crate::storage::{LogStorage, StorageError, StorageRecord, WriteOutcome};

/// The kind byte of an accepted (or learned) entry's identity.
const ACCEPTED: u8 = 1;

/// The node-unique scalars, kept in the journal's two-copy metainfo: the
/// ones whose loss no peer can repair, and the ones that must move with the
/// floor.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct NodeMeta {
    /// The format marker (#147) and the configuration the store was
    /// provisioned under (#207): set once, never cleared or edited.
    formatted: Option<Config>,
    /// The promised ballot.
    promise: Ballot,
    /// The chosen index as of the last metainfo write (relaxed: it rides
    /// the writes that happen anyway).
    chosen_index: Option<Slot>,
    /// What the slots below the floor folded to (#204).
    sealed: JournalState,
}

/// The node's durable store on `moonpool-journal`, over any moonpool
/// [`StorageProvider`]: Tokio's filesystem in production, the simulator's
/// disk under test. Slot `s` is the journal's position `s`, its ballot the
/// entry's identity; the scalars are the metainfo. See the [module
/// docs](super).
///
/// The store opens its journal in [`boot_scan`](LogStorage::boot_scan),
/// which is also where it loads; until then every accessor answers for an
/// empty store. A store with no journal on disk yet creates it at its first
/// sync.
pub struct JournalStorage<P: StorageProvider> {
    provider: P,
    dir: String,
    store: JournalStoreConfig,
    config: Config,
    journal: Option<Journal<P>>,
    meta: NodeMeta,
    /// The metainfo as of the last commit that wrote it (or the boot that
    /// read it): what a crash leaves on disk until the next one lands.
    durable_meta: NodeMeta,
    /// The metainfo changed (a promise, a format, a floor): the next sync
    /// writes it.
    meta_dirty: bool,
    /// The promise rose since the last sync: it reaches the disk before any
    /// entry it covers.
    promise_raised: bool,
    /// The latest chosen index, ahead of `meta.chosen_index` until the
    /// next metainfo write.
    chosen_index: Option<Slot>,
    first: Slot,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    faulty: BTreeMap<Slot, Ballot>,
    /// Entries staged since the last sync, by slot (a later write to a
    /// slot replaces an earlier one).
    staged: BTreeMap<Slot, (Ballot, Vec<u8>)>,
    /// A floor raised since the last sync.
    staged_floor: Option<Slot>,
    boot_facts: JournalBootFacts,
}

/// What a [`JournalStorage`]'s last boot found: observation for a harness's
/// reach gates, never a decision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalBootFacts {
    /// What opening the journal found and repaired.
    pub recovery: Recovery,
    /// Slots reported faulty: damaged entries whose identity survived.
    pub faulty: usize,
    /// The journal's floor is above zero: a truncation or a trim-point jump
    /// dropped a prefix.
    pub truncated: bool,
}

impl<P: StorageProvider> std::fmt::Debug for JournalStorage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalStorage")
            .field("dir", &self.dir)
            .field("node", &self.config.id.0)
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

impl<P: StorageProvider> JournalStorage<P> {
    /// A store for the node `config` names, kept under `dir` on `provider`.
    /// Nothing is read until the boot scan.
    #[must_use]
    pub fn new(
        provider: P,
        dir: impl Into<String>,
        config: Config,
        store: JournalStoreConfig,
    ) -> Self {
        Self {
            provider,
            dir: dir.into(),
            store,
            config,
            journal: None,
            meta: NodeMeta::default(),
            durable_meta: NodeMeta::default(),
            meta_dirty: false,
            promise_raised: false,
            chosen_index: None,
            first: Slot(0),
            accepted: BTreeMap::new(),
            faulty: BTreeMap::new(),
            staged: BTreeMap::new(),
            staged_floor: None,
            boot_facts: JournalBootFacts::default(),
        }
    }

    /// Whether the store of `journal` under `dir` carries its format marker,
    /// read from the journal's metainfo alone (`Journal::peek_meta`): no
    /// recovery, no repair, nothing created, so a probe never changes what
    /// the next boot finds. `false` where no journal exists.
    ///
    /// # Errors
    ///
    /// A [`StorageError::Corruption`] when no metainfo copy is valid or it
    /// does not decode, and the I/O verdict when the namespace cannot be
    /// read.
    pub async fn peek_formatted(
        provider: &P,
        dir: &str,
        journal: paros_core::JournalIdentifier,
    ) -> Result<bool, StorageError> {
        let Some(bytes) = Journal::peek_meta(provider, dir, store_id(journal))
            .await
            .map_err(|e| open_error(&e, StorageRecord::Promise))?
        else {
            return Ok(false);
        };
        let meta: NodeMeta = decode(&bytes).ok_or(undecodable(StorageRecord::Promise))?;
        Ok(meta.formatted.is_some())
    }

    /// What the last boot scan found ([`JournalBootFacts`]).
    #[must_use]
    pub fn boot_facts(&self) -> &JournalBootFacts {
        &self.boot_facts
    }

    /// The directory the journal lives in.
    #[must_use]
    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// How many entries the next sync commits (observation: a harness aims
    /// a power cut at a commit that writes entries).
    #[must_use]
    pub fn staged_entries(&self) -> usize {
        self.staged.len()
    }

    /// The slots the next sync commits entries for (observation: a harness
    /// records what a commit cut by a crash may have landed).
    pub fn staged_slots(&self) -> impl Iterator<Item = Slot> + '_ {
        self.staged.keys().copied()
    }

    /// Where `slot`'s persist record and entry live on disk, if the journal
    /// holds it (observation: a harness aims targeted damage at it).
    #[must_use]
    pub fn layout(&self, slot: Slot) -> Option<moonpool_journal::Layout> {
        self.journal.as_ref()?.layout(slot.0)
    }

    /// Every region of the journal a fault could hit: each held slot's
    /// record and entry, the two metainfo copies, every segment's two header
    /// copies (observation, like [`Self::layout`]). Empty before the journal
    /// exists.
    #[must_use]
    pub fn regions(&self) -> Vec<moonpool_core::LayoutRegion> {
        self.journal
            .as_ref()
            .map_or_else(Vec::new, Journal::regions)
    }

    /// Open the journal, if there is one, and read it back. The body of
    /// [`LogStorage::boot_scan`].
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn load(&mut self) -> Result<(), StorageError> {
        let opened = Journal::open(
            self.provider.clone(),
            &self.dir,
            store_id(self.config.journal),
            self.store.journal(),
        )
        .await
        .map_err(|e| open_error(&e, StorageRecord::Promise))?;
        let fresh = Self::new(
            self.provider.clone(),
            self.dir.clone(),
            self.config.clone(),
            self.store,
        );
        let Some((journal, recovery)) = opened else {
            // No journal on disk: an empty, unformatted store.
            *self = fresh;
            return Ok(());
        };
        *self = fresh;
        report(self.config.id.0, &recovery);
        self.meta = decode(journal.meta()).ok_or(undecodable(StorageRecord::Promise))?;
        self.durable_meta = self.meta.clone();
        self.first = Slot(journal.floor());
        self.chosen_index = self.meta.chosen_index;
        let replay = journal.replay(..).await.map_err(|_| StorageError::Io {
            record: StorageRecord::Store,
            outcome: WriteOutcome::Unknown,
        })?;
        for (position, read) in replay {
            let slot = Slot(position);
            match read {
                Ok(entry) => {
                    let (ballot, _) = id_ballot(&entry.id);
                    let command: Command =
                        decode(&entry.payload).ok_or(undecodable(StorageRecord::Accepted(slot)))?;
                    self.accepted.insert(slot, (ballot, command));
                }
                // CTRL's recoverable class: the value is lost, the vote's
                // identity is not. Never "nothing accepted here".
                Err(ReadError::Damaged { id, .. }) => {
                    self.faulty.insert(slot, id_ballot(&id).0);
                }
                Err(ReadError::Empty { .. } | ReadError::Io(_)) => {
                    return Err(StorageError::Io {
                        record: StorageRecord::Accepted(slot),
                        outcome: WriteOutcome::Unknown,
                    });
                }
            }
        }
        // Everything below the floor is chosen (pair of the live trim-point
        // jump), whatever chosen index the metainfo last carried.
        if let Some(below) = self.first.0.checked_sub(1)
            && self.chosen_index.is_none_or(|c| c.0 < below)
        {
            self.chosen_index = Some(Slot(below));
        }
        // Boot side of the write pairs: nothing below the floor, and a slot
        // is either accepted or faulty, never both.
        assert!(
            self.accepted.keys().next().is_none_or(|s| *s >= self.first),
            "a booted store holds nothing below its floor"
        );
        assert!(
            self.faulty.keys().all(|s| !self.accepted.contains_key(s)),
            "a slot is accepted or faulty, never both"
        );
        if !self.faulty.is_empty() {
            tracing::warn!(
                node = self.config.id.0,
                slots = self.faulty.len() as u64,
                "faulty_entry_reported"
            );
        }
        self.boot_facts = JournalBootFacts {
            faulty: self.faulty.len(),
            truncated: self.first.0 > 0,
            recovery,
        };
        self.journal = Some(journal);
        Ok(())
    }

    /// Raise the floor in memory: the image keeps nothing below it.
    fn raise_floor(&mut self, first: Slot) {
        self.first = self.first.max(first);
        let floor = self.first;
        self.accepted = self.accepted.split_off(&floor);
        self.faulty = self.faulty.split_off(&floor);
        assert!(
            self.accepted.keys().next().is_none_or(|s| *s >= floor),
            "no accepted record survives below the floor"
        );
    }

    /// Seal `state` at `first` unless the floor is already above it.
    fn seal(&mut self, first: Slot, state: JournalState) {
        if first >= self.first {
            self.meta.sealed = state;
            self.meta_dirty = true;
        }
    }

    /// The metainfo a commit may land ahead of the entries staged with it:
    /// the last durable one with only the promise and the format marker
    /// raised. The chosen index and the sealed state stay behind, since each
    /// covers entries (or a floor) the same flush has not committed yet.
    fn early_meta(&self) -> NodeMeta {
        NodeMeta {
            formatted: self.meta.formatted.clone(),
            promise: self.meta.promise,
            ..self.durable_meta.clone()
        }
    }

    /// Commit `batch`, creating the journal first if none is on disk yet
    /// (its first metainfo is [`Self::early_meta`]: a creation lands before
    /// the batch it opens for).
    async fn commit(&mut self, batch: Batch) -> Result<(), StorageError> {
        if self.journal.is_none() {
            let early = self.early_meta();
            let created = Journal::create(
                self.provider.clone(),
                &self.dir,
                store_id(self.config.journal),
                self.store.journal(),
                &encode(&early),
            )
            .await
            .map_err(|e| open_error(&e, StorageRecord::Promise))?;
            self.journal = Some(created);
            self.durable_meta = early;
        }
        let journal = self.journal.as_mut().expect("created above");
        journal.commit(batch).await.map_err(|e| commit_error(&e))
    }
}

/// Trace what opening the journal found and repaired on its own.
fn report(node: u64, recovery: &Recovery) {
    if recovery.torn > 0 {
        tracing::info!(
            node,
            records = u64::from(recovery.torn),
            "journal_torn_discarded"
        );
    }
    if !recovery.ambiguous.is_empty() {
        tracing::warn!(
            node,
            entries = recovery.ambiguous.len() as u64,
            "journal_ambiguous_batch"
        );
    }
    if recovery.rebuilt > 0 || recovery.headers_repaired > 0 || recovery.meta_repaired {
        tracing::info!(
            node,
            records = u64::from(recovery.rebuilt),
            headers = u64::from(recovery.headers_repaired),
            meta = recovery.meta_repaired,
            "journal_identifiers_repaired"
        );
    }
}

impl<P: StorageProvider> Storage for JournalStorage<P> {
    fn initial_state(&self) -> (HardState, Config) {
        let mut hard_state = HardState::default();
        hard_state.max_promised_ballot = self.meta.promise;
        hard_state.chosen_index = self.chosen_index;
        (hard_state, self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        self.first
    }

    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }

    fn sealed_state(&self) -> JournalState {
        self.meta.sealed
    }

    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty.iter().map(|(s, b)| (*s, *b)).collect()
    }
}

impl<P: StorageProvider> LogStorage for JournalStorage<P> {
    /// Open the journal and read it back (see the [module docs](super) for
    /// what damage means). A clean store, a store with faulty entries
    /// (reported through the read ports) and a store whose last batch a
    /// crash tore all boot; a journal that cannot be opened is a crash
    /// verdict.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.load().await
    }

    fn formatted_config(&self) -> Option<Config> {
        self.meta.formatted.clone()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn format(&mut self, config: &Config) -> Result<(), StorageError> {
        // The marker is set once, never edited (the driver refuses a
        // formatted store before it gets here).
        assert!(self.meta.formatted.is_none(), "a store is formatted once");
        self.meta.formatted = Some(config.clone());
        self.meta_dirty = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round))]
    async fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        // Write half of the promise pair: the core only ever raises it.
        assert!(
            ballot >= self.meta.promise,
            "a persisted promise never falls"
        );
        if ballot > self.meta.promise {
            self.meta.promise = ballot;
            self.meta_dirty = true;
            self.promise_raised = true;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0))]
    async fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        // A re-sent `Accept` the store already holds (the same ballot and
        // command, on disk or staged for the next sync) writes nothing: the
        // core's sync still precedes its reply, and it finds nothing to
        // commit. Re-committing it would cost a journal commit per re-send,
        // and a leader re-sending a page of rounds would keep every
        // acceptor's disk busy with copies while its tally crawled
        // (witness 5953348164786240469: 1,073 re-sends of one slot before
        // its quorum, the cluster stalled past the recovery tail).
        if self
            .accepted
            .get(&slot)
            .is_some_and(|(held, cmd)| *held == ballot && *cmd == command)
        {
            assert!(
                !self.faulty.contains_key(&slot),
                "a held entry is not faulty"
            );
            return Ok(());
        }
        self.staged.insert(slot, (ballot, encode(&command)));
        self.faulty.remove(&slot);
        self.accepted.insert(slot, (ballot, command));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0))]
    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.chosen_index = Some(slot);
        Ok(())
    }

    /// Up to three kinds of journal commit, each durable before the next: a
    /// raised promise alone, so it is durable before any entry it covers;
    /// the entries; then the floor and the metainfo, alone, so neither is
    /// durable before the entries under it. The chosen index is never a
    /// reason to write: a [`MustSync::Relaxed`] flush holding nothing else
    /// writes nothing, and the next metainfo write carries it.
    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0))]
    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        let _ = must_sync;
        if !self.meta_dirty && self.staged.is_empty() {
            return Ok(());
        }
        // The entries the flush's own floor leaves standing (the core may
        // stage a slot and then jump past it in the same flush: the jump
        // drops it, as it drops every slot below).
        let floor = self.staged_floor.take();
        let entries: Vec<_> = std::mem::take(&mut self.staged)
            .into_iter()
            .filter(|(slot, _)| floor.is_none_or(|f| *slot >= f))
            .collect();
        let meta = self.meta_dirty.then(|| {
            self.meta.chosen_index = self.chosen_index;
            encode(&self.meta)
        });
        // A raised promise is durable before any entry accepted under it,
        // and nothing else of the flush is: the promise's own commit carries
        // the last durable metainfo with the promise raised, never the new
        // chosen index or sealed state. A chosen index landed ahead of the
        // entry that makes its slot chosen would boot a node that applies
        // its stale lower-ballot accept as the decided value (#264, witness
        // 1126530436175411981: node 0 applied its own unchosen round-1
        // value at slot 0 after a power cut between the two commits).
        if meta.is_some() && self.promise_raised && !entries.is_empty() {
            let early = self.early_meta();
            assert!(
                early.promise == self.meta.promise,
                "the promise's own commit carries the raised promise"
            );
            assert!(
                early.chosen_index == self.durable_meta.chosen_index,
                "the promise's own commit never moves the chosen index"
            );
            let mut promise = Batch::new();
            promise.set_meta(encode(&early));
            self.commit(promise).await?;
            self.durable_meta = early;
            // The promise is durable and no entry under it is: a crash here
            // is the #264 shape, which the order of the commits makes safe.
            let hinted = moonpool_buggify::hint!("promise durable, entries staged");
            if hinted.strike() == Strike::Killed {
                moonpool_assertions::reachable!(
                    "a node crashes between its promise and its entries"
                );
            }
            hinted.await;
        }
        // Packed into batches that each fit one segment: a node catching up
        // a long log after a reboot stages more than one holds (witness
        // 6142474209351073489, refused `BatchTooLarge` on every boot before
        // the split). Nothing is acknowledged before the whole sync returns.
        let geometry = self.store.geometry;
        let mut staged = Batch::new();
        for (slot, (ballot, payload)) in entries {
            if !staged.is_empty() && !staged.fits_another(geometry, payload.len()) {
                assert!(staged.fits(geometry), "a packed batch fits one segment");
                self.commit(std::mem::take(&mut staged)).await?;
                // A packed batch is durable and the metainfo still covers
                // only the batches before it.
                let hinted = moonpool_buggify::hint!("entries durable, metainfo staged");
                if hinted.strike() == Strike::Killed {
                    moonpool_assertions::reachable!(
                        "a node crashes between its entry batches and its metainfo"
                    );
                }
                hinted.await;
            }
            staged.put(slot.0, ballot_id(ballot, ACCEPTED), payload);
        }
        // The floor and the metainfo ride the last batch: the journal writes
        // a batch's metainfo only once the batch is durable (moonpool#309),
        // so a durable chosen index always covers its entries, and every
        // earlier batch was synced before this one started. A chosen index
        // landing ahead of the entry that makes its slot chosen was the #264
        // shape (#176).
        if floor.is_some() || meta.is_some() || !staged.is_empty() {
            if let Some(floor) = floor {
                staged.truncate_prefix(floor.0);
            }
            let wrote_meta = meta.is_some();
            if let Some(meta) = meta {
                staged.set_meta(meta);
            }
            self.commit(staged).await?;
            if wrote_meta {
                self.durable_meta = self.meta.clone();
            }
        }
        self.meta_dirty = false;
        self.promise_raised = false;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, first = first.0))]
    async fn truncate(&mut self, first: Slot, sealed: JournalState) -> Result<(), StorageError> {
        self.seal(first, sealed);
        self.raise_floor(first);
        self.staged_floor = Some(self.staged_floor.map_or(first, |f| f.max(first)));
        self.meta_dirty = true;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, point = point.0))]
    async fn trimmed_to(&mut self, point: Slot, state: JournalState) -> Result<(), StorageError> {
        self.seal(point, state);
        let boundary = Slot(point.0.saturating_sub(1));
        if self.chosen_index.is_none_or(|ci| ci < boundary) {
            self.chosen_index = Some(boundary);
        }
        self.raise_floor(point);
        self.staged_floor = Some(self.staged_floor.map_or(point, |f| f.max(point)));
        self.meta_dirty = true;
        Ok(())
    }
}
