//! [`JournalStorage`]: the node's [`LogStorage`] on `moonpool-journal`.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_buggify::hint::Strike;
use moonpool_core::LocalStorageProvider;
use moonpool_journal::{Batch, Journal, ReadError, Recovery};
use paros_core::contract::MustSync;
use paros_core::journal::{Command, JournalState};
use paros_core::{Ballot, Config, HardState, Slot, Storage};
use serde::{Deserialize, Serialize};

use super::{
    JournalStoreConfig, ballot_id, commit_error, decode, encode, id_ballot, open_error, store_id,
    undecodable,
};
use crate::{
    LogStorage, ScrubPage, ScrubbedSlot, StorageError, StorageRecord, WriteOutcome, command_bytes,
};

/// The kind byte of an accepted (or learned) entry's identity.
const ACCEPTED: u8 = 1;

/// How far the chosen index may run ahead of the durable one before a sync
/// writes the metainfo for it alone (#602 (dark after a full restart)).
///
/// A booted node recovers every slot above its durable chosen index: a
/// leader re-proposes them all, one page at a time. Before #602 the chosen
/// index rode only the writes that happened anyway (a promise, a format, a
/// floor), so in steady state it stayed where the leader's election left
/// it. After a full-cell restart every journal's new leader re-proposed its
/// whole log: a Compose cell answered nothing for 30 s at a 100 ms tick and
/// for 90 s at a 10 ms tick, where elections deposed leaders mid-recovery.
/// Now a reboot rewinds a node's chosen index by less than this many slots,
/// so a recovery is about one page; the simulation judges that bound at
/// every boot ("a reboot rewinds the chosen index by less than a page").
pub const CHOSEN_INDEX_LAG: u64 = 64;

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
    /// the writes that happen anyway, and forces one only once it lags
    /// [`CHOSEN_INDEX_LAG`] slots, #602 (dark after a full restart)).
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
pub struct JournalStorage<P: LocalStorageProvider> {
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
    /// Slots whose durable copy a scrub found damaged (#362 (background
    /// scrubbing)): the next append of the slot writes it, even when it is
    /// the record held.
    damaged: BTreeSet<Slot>,
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

impl<P: LocalStorageProvider> std::fmt::Debug for JournalStorage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalStorage")
            .field("dir", &self.dir)
            .field("node", &self.config.id.0)
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

impl<P: LocalStorageProvider> JournalStorage<P> {
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
            damaged: BTreeSet::new(),
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
        journal: paros_core::journal::JournalIdentifier,
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
        super::drop_caches_for_open(&self.provider, &self.dir).await?;
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
        self.damaged = self.damaged.split_off(&floor);
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

    /// Whether the chosen index runs at least [`CHOSEN_INDEX_LAG`] slots
    /// ahead of the durable one, so the next sync writes the metainfo for it
    /// (#602 (dark after a full restart)).
    ///
    /// # Panics
    ///
    /// If the durable chosen index is ahead of the live one: a programmer
    /// error, never an operating condition.
    fn chosen_lags(&self) -> bool {
        let durable = self.durable_meta.chosen_index;
        let Some(live) = self.chosen_index else {
            assert!(durable.is_none(), "a durable chosen index has a live one");
            return false;
        };
        // One past the durable index: how many slots a boot already knows
        // are chosen.
        let known = durable.map_or(0, |slot| slot.0 + 1);
        assert!(
            known <= live.0 + 1,
            "the live chosen index never trails the durable one"
        );
        live.0 + 1 - known >= CHOSEN_INDEX_LAG
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
        let before = journal.rolls();
        let committed = journal.commit(batch).await.map_err(|e| commit_error(&e));
        let after = journal.rolls();
        assert!(
            after.onto_spare >= before.onto_spare && after.created >= before.created,
            "a journal's rolls only grow"
        );
        if after.onto_spare > before.onto_spare {
            moonpool_assertions::reachable!(
                "journal store: a commit rolled onto a recycled segment"
            );
        }
        committed
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
    if recovery.spares_removed > 0 {
        tracing::info!(
            node,
            spares = u64::from(recovery.spares_removed),
            "journal_spares_removed"
        );
    }
}

impl<P: LocalStorageProvider> Storage for JournalStorage<P> {
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

impl<P: LocalStorageProvider> LogStorage for JournalStorage<P> {
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
        // The marker records no count (#754 (counts off the format marker)).
        self.meta.formatted = Some(config.recorded());
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
        // A scrub found the durable copy damaged (#362 (background
        // scrubbing)): the same record is written again.
        let damaged = self.damaged.remove(&slot);
        if !damaged
            && self
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
    /// durable before the entries under it. The chosen index is a reason to
    /// write only once it runs [`CHOSEN_INDEX_LAG`] slots ahead of the
    /// durable one (#602 (dark after a full restart)): below that, a
    /// [`MustSync::Relaxed`] flush holding nothing else writes nothing, and
    /// the next metainfo write carries it.
    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0))]
    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        let _ = must_sync;
        if self.chosen_lags() {
            self.meta_dirty = true;
        }
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
            // Only a flush that raises the promise and stages entries reaches
            // here, a few per run: the rate is far above the default (#446
            // (a gate that never fires cannot fail the sweep)).
            let hinted = moonpool_buggify::hint!("promise durable, entries staged", 0.5);
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
                // only the batches before it. Only a sync of more than one
                // segment reaches here, rarely: the rate is far above the
                // default (#446).
                let hinted = moonpool_buggify::hint!("entries durable, metainfo staged", 0.5);
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

    /// Read a page of entries back from the journal's segments and judge
    /// each against its persist record (#362 (background scrubbing)): the
    /// slots held from `from` up, faulty ones included, none staged. An
    /// entry the journal reads intact but that is not the record held
    /// (another ballot or value) is damaged too.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, from = from.0))]
    async fn scrub(
        &mut self,
        from: Slot,
        limit: usize,
        max_bytes: u64,
    ) -> Result<ScrubPage, StorageError> {
        let start = from.max(self.first);
        let mut held: BTreeMap<Slot, (Ballot, u64)> = BTreeMap::new();
        let mut page = ScrubPage::default();
        let mut bytes = 0_u64;
        let slots = self
            .accepted
            .range(start..)
            .map(|(slot, (ballot, command))| (*slot, (*ballot, command_bytes(command))))
            .chain(
                self.faulty
                    .range(start..)
                    .map(|(slot, ballot)| (*slot, (*ballot, 1))),
            );
        let mut ordered: BTreeMap<Slot, (Ballot, u64)> = slots.collect();
        ordered.retain(|slot, _| !self.staged.contains_key(slot));
        for (slot, (ballot, size)) in ordered {
            if held.len() >= limit.max(1) || (bytes >= max_bytes && !held.is_empty()) {
                page.next = Some(slot);
                break;
            }
            bytes += size;
            held.insert(slot, (ballot, size));
        }
        let (Some(first), Some(last)) = (held.keys().next(), held.keys().next_back()) else {
            return Ok(page);
        };
        let Some(journal) = self.journal.as_ref() else {
            return Ok(page);
        };
        let replay = journal
            .replay(first.0..=last.0)
            .await
            .map_err(|_| StorageError::Io {
                record: StorageRecord::Store,
                outcome: WriteOutcome::Unknown,
            })?;
        for (position, read) in replay {
            let slot = Slot(position);
            let Some((ballot, size)) = held.get(&slot).copied() else {
                continue;
            };
            let own = self.accepted.get(&slot);
            let intact = match (&read, own) {
                (Ok(entry), Some((own_ballot, own_command))) => {
                    id_ballot(&entry.id).0 == *own_ballot
                        && decode::<Command>(&entry.payload).as_ref() == Some(own_command)
                }
                _ => false,
            };
            page.bytes += size;
            let scrubbed = match own {
                Some((own_ballot, own_command)) if intact => ScrubbedSlot::Intact {
                    ballot: *own_ballot,
                    digest: paros_core::journal::command_fingerprint(own_command),
                },
                _ => {
                    if own.is_some() {
                        // The durable copy is gone: the next append of the
                        // slot writes the record held again.
                        self.damaged.insert(slot);
                    }
                    moonpool_assertions::reachable!("journal: a scrub reads a damaged entry");
                    ScrubbedSlot::Damaged { ballot }
                }
            };
            page.slots.insert(slot, scrubbed);
        }
        // Pair of the read: every slot judged was one the store held.
        assert!(
            page.slots.keys().all(|s| held.contains_key(s)),
            "a scrub judges only the slots it asked for"
        );
        assert!(
            page.next
                .is_none_or(|next| page.slots.keys().all(|s| *s < next)),
            "a scrub page ends below its next slot"
        );
        Ok(page)
    }
}
