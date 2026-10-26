//! The **matchmaker**: a per-ballot acceptor-configuration registry (Matchmaker
//! Paxos §3.1–§3.2), as a sans-IO state machine beside [`crate::ColocatedNode`].
//!
//! A matchmaker separates *who are the acceptors* from *what value is chosen*.
//! It keeps a tiny durable map `ballot -> acceptor configuration` plus a GC
//! watermark, and is touched **only** on a round change (a leader change or a
//! reconfiguration), never on the command path. It is the source of truth that
//! lets a new leader discover **every configuration used below its own ballot**,
//! which is what the cross-configuration Phase 1 of #22 rests on.
//!
//! This module is the registry side only: the pure state machine, its durable
//! state, and its request / reply / refusal semantics — driven through the same
//! `step` → `ready` → `advance` shape as [`crate::ColocatedNode`], so the driver's
//! persist-before-reply ordering is structural rather than remembered. It is a
//! **separate handle the caller drives**: [`crate::ColocatedNode`] never steps a
//! matchmaker message, and a cluster deployed without matchmakers never
//! constructs one (see AGENTS.md, *Plain Multi-Paxos is first-class*).
//!
//! # Keying: whole ballots, not bare rounds
//!
//! The paper keys its log by round because rounds are statically partitioned
//! across proposers, so "one proposer owns round `i` and picks one config" is
//! what keeps matchmakers from ever disagreeing about a round's configuration
//! (§3.2). paros does not partition rounds that way: `on_check_leader` campaigns
//! at `max(promised, ballot).round + 1` with the node id as tiebreak, so two
//! nodes legitimately campaign at the *same* round with different ballots. The
//! registry is therefore keyed on the whole [`Ballot`] (`{ round, node }`,
//! totally ordered by `Ord for Ballot`): the ballot's `node` carries the
//! proposer identity, and the paper's no-disagreement property holds for the
//! same reason — **one ballot has exactly one proposer**. Concretely: two
//! candidates at the same round, `{5, node 1}` and `{5, node 2}`, occupy two
//! distinct registry keys (the lower one is a strict ancestor in the other's
//! history), and no path in this module ever keys on a bare round. This is the
//! one place paros diverges from the paper's model, and the tests pin it.
//!
//! # The contract, in one sentence each
//!
//! - **Write-once per ballot.** A ballot registered with configuration `C` is
//!   never observed later with a different configuration, by anyone, ever.
//! - **Registration is strictly monotone.** After registering `b`, a request
//!   at any `b' <= b` is refused, never quietly registered. The one exception
//!   is the *same request again* — `b` with the configuration already
//!   registered under it — which is answered idempotently from the **currently
//!   retained** history, without a second registration. That re-answer is not
//!   a replay of the first reply: GC between the two may have raised the
//!   watermark, so the retry's history can be a strict subset of the original
//!   (never a superset — the registry only ever grows *above* `b`). Whether a
//!   proposer may still act on such a shrunk history is the GC protocol's
//!   contract (§3.4–§3.5), not this state machine's.
//!   Monotonicity is what makes the returned history complete for every later
//!   ballot.
//! - **The history is complete below the request.** The reply for `b` names
//!   every configuration this matchmaker holds at a ballot `< b` and at or
//!   above its watermark. Under-reporting is the bug class the whole protocol
//!   exists to prevent.
//! - **The watermark is monotone** and never lets a request below it register.
//! - **Persist before reply.** A successful reply may only leave once its
//!   registration is fsync-durable — the driver's job, made structural by
//!   [`MatchmakerReady`]'s ordering (writes first, then replies); the exact
//!   analogue of the acceptor's persist-before-`Promise` rule on
//!   [`crate::HardState`].
//!
//! # Generations: the matchmaker set is itself a chosen value (#125)
//!
//! The matchmakers are the source of truth for configurations, so their own
//! membership cannot be a static fact forever: a matchmaker that dies or
//! loses its disk would be unreplaceable. Following §5 of the paper, the
//! matchmaker set carries a **generation**, and generation `g + 1` is chosen
//! by a **single-decree Paxos instance whose acceptors are generation `g`'s
//! matchmakers** ([`crate::DecreeRecord`] is the durable half every
//! matchmaker keeps). The handover is stop-the-world, which is acceptable
//! because matchmakers are idle whenever a leader is stable:
//!
//! ```text
//! Stop        a quorum of M_g freezes (durably; a frozen matchmaker registers
//!             nothing for g ever again) and answers with its registry + watermark
//! Bootstrap   the reconfigurer reconstructs (max watermark, union above it) and
//!             hands it to every member of the proposed M_{g+1}, stored *pending*
//! Decree      Phase 1 / Phase 2 over M_g choose the successor set (P2c adopts
//!             a competing proposal already voted, so two reconfigurers never
//!             install two successors)
//! Chosen      every matchmaker told: M_g members record the successor (the
//!             discovery chain), M_{g+1} members activate their pending bootstrap
//! ```
//!
//! Every message is **fenced by generation**: a request naming another
//! generation is refused with what this matchmaker knows (its current set,
//! or its successor), never served. A frozen matchmaker stays alive: it keeps
//! answering `Stop`, votes in the decree, and points late proposers at its
//! successor — "stopped" is a protocol freeze, not a process death.
//!
//! Replacement is also how paros recovers a matchmaker whose durable state is
//! unusable: there is deliberately no matchmaker-specific in-place disk
//! repair. Such a matchmaker is fenced out of the next generation and a fresh
//! one bootstrapped in its place from the surviving quorum's frozen registries.
//!
//! # What this proves, and what it does not
//!
//! Everything above is a property of **one matchmaker**: each registry, taken
//! alone, is write-once, monotone, complete below any ballot it answers, and
//! durable before it answers. The paper's safety argument (§3.3) rests on
//! something more: a proposer collects `MatchB` from `f + 1` of the `2f + 1`
//! matchmakers and runs Phase 1 against the **union** of their histories, and
//! it is the intersection of any two such `f + 1` sets that guarantees no
//! configuration used below the proposer's ballot is missed. That union, the
//! quorum it needs, and the cross-configuration Phase 1 it feeds belong to the
//! leader-side matchmaking phase (`node/matchmaking.rs`); the reconstruction
//! across generations rests on the same intersection (Appendix B: every
//! completed registration reached a quorum of `M_g`, which intersects the
//! frozen quorum the successor was reconstructed from).
//!
//! # Trust boundary of `Chosen`
//!
//! `Chosen` is the one **learner notification** among the reconfiguration
//! messages; its trust boundary is the wire contract, documented on
//! [`ReconfigureRequest::Chosen`].

mod generation;
mod message;
mod reconfigurer;
mod state;
mod storage;
mod write;

use std::collections::BTreeMap;

/// The handover's decree: the successor set, over the matchmakers of `M_g`.
pub type SuccessorDecree = crate::decree::Decree<MatchmakerId, Vec<MatchmakerId>>;
pub use self::message::{
    GcAck, GcRequest, MatchOutcome, MatchPurpose, MatchRefusal, MatchReply, MatchRequest,
    ReconfigureReply, ReconfigureRequest,
};
pub use self::reconfigurer::{
    MatchmakerReconfigurer, ReconfigurerPhase, ReconfigurerReady, ReconfigurerStep, Reconstruction,
    StartRefusal,
};
pub use self::state::{
    BootstrapPage, DecreeRecord, JournalRegistry, JournalScalars, MatchmakerConfig,
    MatchmakerHardState, MatchmakerPhase, PendingBootstrap, Registration, RegistrationKind,
};
pub(crate) use self::state::{raise_effective, resolved_phase, resolved_set};
pub use self::storage::{MemRegistry, RegistryStorage};
pub use self::write::{MatchmakerReady, MatchmakerWriteOp};
use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::retained::RetainedWindow;
use crate::types::{Ballot, JournalId, NodeId};

/// A journal this set holds no record for reads as this registry.
static EMPTY_REGISTRY: BTreeMap<Ballot, Registration> = BTreeMap::new();

/// The most registrations one `MatchB` page carries
/// ([`MatchOutcome::Registered`]). A registry retains one record per ballot
/// above its watermark, and a cluster that elects often between two GC
/// rounds accumulates them; answering the whole ledger in one message is
/// the same unbounded reply the log's `Promise` is paged to avoid
/// ([`crate::PROMISE_BATCH`]). A candidate re-asks with a cursor until the
/// answer is complete, and only a complete answer counts toward its
/// matchmaker quorum. The ceiling of a matchmaker's own page size
/// ([`Matchmaker::set_registry_page`], #338): a candidate checks a page
/// against this ceiling, never the sender's size.
pub const REGISTRY_PAGE: usize = 64;

// A registry page that carries nothing could never deliver a history.
const _: () = assert!(REGISTRY_PAGE > 0);

/// One bounded page of a registry window (`[from, ballot)` of one journal's
/// registry, in ballot order): at most `limit` records, and the ballot the
/// next page starts at, `None` when this page ends the answer.
///
/// A **belief** is left out when a later registration of the window carries
/// the same configuration (#570 (election duel outlasts the recovery tail)).
/// Every record the page keeps is the highest-ballot one of its
/// configuration in the window, and every reconfiguration is kept, so the
/// candidate's fold is unchanged: its union of the configurations above any
/// watermark is the same (a configuration has a record at or above the
/// watermark exactly when its highest one is), and its effective
/// configuration (the highest reconfiguration) is the same. The criterion
/// looks only forward, so a page asked from a cursor elides exactly what the
/// whole answer elides. That cross-page claim rests on [`Matchmaker::step`]
/// refusing a registration at or below the journal's highest ballot
/// (`Stale`): nothing lands inside a window a candidate is still paging. A
/// change that lets a lower ballot register must re-check it.
///
/// Without it, an answer cost one record per ballot ever registered above
/// the watermark, and a leaderless cluster registers one per failed campaign
/// while no leader can raise the watermark: on a one-record page, every
/// campaign's matchmaking took a round trip per earlier campaign, outlasted
/// the capped election timeout of every rival, and under `q1 = n` no Phase 1
/// ever won again (witness seed 16302262455227242419 on c739108: 184
/// campaigns, no leader through a 60 s recovery tail; green with it).
fn history_page<'a>(
    window: impl Iterator<Item = (&'a Ballot, &'a Registration)>,
    limit: usize,
) -> (BTreeMap<Ballot, Registration>, Option<Ballot>) {
    assert!(
        limit > 0,
        "a registry page carries at least one registration"
    );
    let window: Vec<(&Ballot, &Registration)> = window.collect();
    // Which records a later one repeats: one backward walk, remembering the
    // configurations already seen above.
    let mut above: Vec<&AcceptorConfig> = Vec::new();
    let mut kept: Vec<bool> = vec![false; window.len()];
    for (keep, (_, registration)) in kept.iter_mut().zip(&window).rev() {
        let repeated = above.contains(&&registration.config);
        *keep = registration.kind.is_reconfiguration() || !repeated;
        if !repeated {
            above.push(&registration.config);
        }
    }
    let elided = kept.iter().filter(|keep| !**keep).count();
    probe!(
        sometimes,
        elided > 0,
        "matchmaker: a page leaves out a belief a later registration repeats"
    );
    let mut records = window
        .iter()
        .zip(&kept)
        .filter(|(_, keep)| **keep)
        .map(|((ballot, registration), _)| (**ballot, (*registration).clone()));
    let page: BTreeMap<Ballot, Registration> = records.by_ref().take(limit).collect();
    let cursor = records.next().map(|(ballot, _)| ballot);
    // Postconditions: bounded, every configuration of the window still
    // named, and the cursor strictly past the page after a full one.
    assert!(page.len() <= limit, "a page honours its limit");
    assert!(
        cursor.is_none() || page.len() == limit,
        "a continuation cursor follows a full page"
    );
    if let Some(cursor) = cursor {
        assert!(
            page.keys().next_back().is_none_or(|last| *last < cursor),
            "the continuation lies past the page"
        );
    }
    assert!(
        window.len() == elided + kept.iter().filter(|keep| **keep).count(),
        "every record is kept or left out"
    );
    (page, cursor)
}

/// What a GC request did at this matchmaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcOutcome {
    /// The watermark rose (a durable write is staged).
    Raised,
    /// At or below the floor already in force: nothing changed.
    Unchanged,
    /// Not active for the addressed generation: refused.
    Refused,
}

/// The matchmaker state machine: the registry, the watermark, the generation
/// state, and one pending batch of writes and replies. Pure — no I/O, no
/// clock, no randomness; the driver ([`paros::run_matchmaker`](https://docs.rs/paros))
/// performs every side effect it describes.
#[derive(Clone, Debug)]
pub struct Matchmaker {
    config: MatchmakerConfig,
    hard_state: MatchmakerHardState,
    /// The set this matchmaker is active or frozen for, materialized from
    /// `hard_state` (and, at generation 0, from the bootstrap
    /// configuration) so [`Matchmaker::set`] can hand out a reference
    /// instead of cloning a membership on every caller's behalf — the audit
    /// port reads it on every reply, and an observation that allocates is a
    /// port that changes the shipped program. Kept in step by
    /// `refresh_set`, and `assert_invariants` says so.
    set: MatchmakerSet,
    /// Every journal's registry (#190): each registered `ballot ->
    /// registration` retained above that journal's GC watermark, strictly
    /// increasing in ballot order (the window keeps the order and the floor;
    /// the state machine keeps the "only ever appended above the highest"
    /// discipline). A journal appears once it has a record or a raised
    /// floor; each floor is a copy of the journal's durable `gc_watermark`,
    /// and `assert_invariants` says so.
    registries: BTreeMap<JournalId, RetainedWindow<Ballot, Registration>>,
    pending_writes: Vec<MatchmakerWriteOp>,
    pending_replies: Vec<MatchReply>,
    pending_reconfigure_replies: Vec<ReconfigureReply>,
    /// The most registrations one page carries (#338): a tunable in
    /// `1..=REGISTRY_PAGE`, the constant by default
    /// ([`Matchmaker::set_registry_page`]).
    page: usize,
}

impl Matchmaker {
    /// Boot a matchmaker from its durable storage: read the scalars once, then
    /// walk the registry record by record (a fresh matchmaker is an empty
    /// port). Restart and first boot are the same path, so a rebooted
    /// matchmaker answers exactly as it would have without the crash. A
    /// [`MatchmakerPhase::Fresh`] store resolves against `config.bootstrap`
    /// without a write: a bootstrap member is active for generation 0, any
    /// other matchmaker inactive.
    ///
    /// # Panics
    ///
    /// If the durable state violates the registry contract — a ballot the
    /// walk names but the port cannot serve, a registration below the
    /// watermark, a malformed configuration. That means corrupted storage that
    /// evaded the scan or a broken storage implementation; crashing beats
    /// answering from it.
    #[must_use]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = config.id.0)))]
    pub fn new<S: RegistryStorage>(config: &MatchmakerConfig, storage: &S) -> Self {
        let hard_state = storage.initial_state();
        let mut ledgers: BTreeMap<JournalId, BTreeMap<Ballot, Registration>> = BTreeMap::new();
        for (journal, ballot) in storage.registered() {
            let registration = storage
                .registration(journal, ballot)
                .expect("every registered ballot the walk names has a readable record");
            let previous = ledgers
                .entry(journal)
                .or_default()
                .insert(ballot, registration);
            assert!(
                previous.is_none(),
                "the registry walk names each ballot once"
            );
        }
        // The window each registry lives in enforces its own floor, so this
        // says the same thing in the *registry's* words, at the boot seam
        // where a corrupt store is what breaks it.
        assert!(
            ledgers.iter().all(|(journal, ledger)| ledger
                .keys()
                .next()
                .is_none_or(|lowest| *lowest >= hard_state.gc_watermark(*journal))),
            "no registration survives below the gc watermark"
        );
        let mut registries: BTreeMap<JournalId, RetainedWindow<Ballot, Registration>> = hard_state
            .journals
            .iter()
            .map(|(journal, scalars)| {
                (
                    *journal,
                    RetainedWindow::new(BTreeMap::new(), scalars.gc_watermark),
                )
            })
            .collect();
        for (journal, ledger) in ledgers {
            registries.insert(
                journal,
                RetainedWindow::new(ledger, hard_state.gc_watermark(journal)),
            );
        }
        let mut bootstrap = config.bootstrap.clone();
        bootstrap.sort_unstable();
        bootstrap.dedup();
        assert!(
            !bootstrap.is_empty(),
            "a matchmaker deployment bootstraps at least one matchmaker"
        );
        let set = resolved_set(&hard_state, &bootstrap);
        let matchmaker = Self {
            config: MatchmakerConfig {
                id: config.id,
                bootstrap,
            },
            hard_state,
            set,
            registries,
            pending_writes: Vec::new(),
            pending_replies: Vec::new(),
            pending_reconfigure_replies: Vec::new(),
            page: REGISTRY_PAGE,
        };
        matchmaker.assert_invariants();
        matchmaker
    }

    /// The driver sets the registry page size (#338): the most
    /// registrations one `Registered` answer carries before its
    /// continuation cursor. A per-matchmaker size: a candidate checks a
    /// page against [`REGISTRY_PAGE`], never against a size of its own, so
    /// a mixed set is legal. A small page makes a history span several
    /// pages.
    ///
    /// # Panics
    ///
    /// If `page` is zero or above [`REGISTRY_PAGE`]: a programmer error,
    /// never an operating condition.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, page)))]
    pub fn set_registry_page(&mut self, page: usize) {
        assert!(
            page > 0,
            "a registry page carries at least one registration"
        );
        assert!(
            page <= REGISTRY_PAGE,
            "a registry page never exceeds its ceiling"
        );
        self.page = page;
        assert!(self.page == page, "the driver's registry page is in force");
        self.assert_invariants();
    }

    /// The most registrations one page carries (#338), in
    /// `1..=REGISTRY_PAGE`.
    #[must_use]
    pub fn registry_page(&self) -> usize {
        self.page
    }

    /// This matchmaker's identity.
    #[must_use]
    pub fn id(&self) -> MatchmakerId {
        self.config.id
    }

    /// The durable scalars as they stand (including a write not yet flushed
    /// by the driver — the core applies a write the instant it decides it,
    /// exactly as [`crate::ColocatedNode`] does).
    #[must_use]
    pub fn hard_state(&self) -> &MatchmakerHardState {
        &self.hard_state
    }

    /// `journal`'s registry as it stands, in ballot order (same caveat as
    /// [`Self::hard_state`]); empty for a journal this set holds nothing of.
    #[must_use]
    pub fn registry(&self, journal: JournalId) -> &BTreeMap<Ballot, Registration> {
        self.registries
            .get(&journal)
            .map_or(&EMPTY_REGISTRY, RetainedWindow::entries)
    }

    /// Every journal this set holds a registry or a scalar for (#190), in
    /// journal order.
    ///
    /// # Panics
    ///
    /// If a journal with scalars has no registry window: a programmer
    /// error, never an operating condition.
    pub fn journals(&self) -> impl Iterator<Item = JournalId> + '_ {
        let journals = self.registries.keys().copied();
        // Every journal with scalars has its window (`new`, `window`).
        assert!(
            self.hard_state
                .journals
                .keys()
                .all(|journal| self.registries.contains_key(journal)),
            "every journal with scalars has a registry window"
        );
        journals
    }

    /// `journal`'s GC watermark, as it stands.
    #[must_use]
    pub fn gc_watermark(&self, journal: JournalId) -> Ballot {
        self.hard_state.gc_watermark(journal)
    }

    /// `journal`'s effective configuration, as it stands.
    #[must_use]
    pub fn effective(&self, journal: JournalId) -> Option<&(Ballot, AcceptorConfig)> {
        self.hard_state.effective(journal)
    }

    /// `journal`'s registry window, created empty at its floor on first use.
    fn window(&mut self, journal: JournalId) -> &mut RetainedWindow<Ballot, Registration> {
        let floor = self.hard_state.gc_watermark(journal);
        let window = self
            .registries
            .entry(journal)
            .or_insert_with(|| RetainedWindow::new(BTreeMap::new(), floor));
        assert!(
            window.floor() == floor,
            "a registry window sits at its journal's watermark"
        );
        window
    }

    /// The highest ballot registered in `journal`'s registry, or `None` on
    /// an empty one.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn highest(&self, journal: JournalId) -> Option<Ballot> {
        let highest = self
            .registries
            .get(&journal)
            .and_then(RetainedWindow::last_key);
        // Paired with `advance_gc_watermark`: what survives sits at or above
        // the durable floor.
        if let Some(highest) = highest {
            assert!(
                highest >= self.hard_state.gc_watermark(journal),
                "the highest registration sits at or above the watermark"
            );
        }
        highest
    }

    /// Where this matchmaker stands, with a fresh store resolved against the
    /// bootstrap set.
    #[must_use]
    pub fn phase(&self) -> MatchmakerPhase {
        resolved_phase(&self.hard_state, self.config.id, &self.config.bootstrap)
    }

    /// The set this matchmaker is active or frozen for: generation 0's
    /// bootstrap set, or the activated one.
    #[must_use]
    pub fn set(&self) -> &MatchmakerSet {
        &self.set
    }

    /// The set `hard_state` describes: generation 0's bootstrap set until a
    /// generation is durably activated or frozen.
    fn derive_set(&self) -> MatchmakerSet {
        resolved_set(&self.hard_state, &self.config.bootstrap)
    }

    /// Re-materialize [`Self::set`] after a durable generation change (the
    /// freeze and the activation are the only two).
    pub(super) fn refresh_set(&mut self) {
        self.set = self.derive_set();
        assert!(
            self.set.generation == self.hard_state.generation,
            "the materialized set is the durable generation"
        );
    }

    /// The chosen successor of this matchmaker's generation, if learned.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn successor(&self) -> Option<&MatchmakerSet> {
        if let Some(successor) = &self.hard_state.successor {
            assert!(
                successor.generation == self.hard_state.generation.next(),
                "a recorded successor is the next generation"
            );
            assert!(
                self.hard_state.phase == MatchmakerPhase::Stopped,
                "a generation with a successor is frozen"
            );
        }
        self.hard_state.successor.as_ref()
    }

    /// Answer one matchmaking request (the paper's `MatchA` handler, §3.2):
    ///
    /// - not active for the request's generation → [`MatchRefusal::Stopped`]
    ///   (frozen, with the successor if learned), [`MatchRefusal::Generation`]
    ///   (active elsewhere) or [`MatchRefusal::Inactive`];
    /// - below the watermark → [`MatchRefusal::BelowWatermark`];
    /// - not strictly above the highest registered ballot → the same request
    ///   again (the ballot is registered with exactly this configuration) is
    ///   answered idempotently from the currently retained history, with no
    ///   write; anything else is [`MatchRefusal::Stale`];
    /// - otherwise the history of configurations registered in
    ///   `[gc_watermark, ballot)` is computed (less every belief a later
    ///   registration repeats, `history_page`), `config` is registered under
    ///   `ballot` (a [`MatchmakerWriteOp::Register`] the driver must fsync
    ///   before the reply leaves), and the reply carries that history plus the
    ///   watermark.
    ///
    /// The re-answer is **not** a replay of the first reply. The registry only
    /// ever grows *above* an existing key, so the set of registrations below a
    /// registered ballot never gains a member — but GC can drop members below
    /// a raised watermark. A retry after GC therefore receives the first
    /// answer restricted to the current watermark (a strict subset, with the
    /// higher watermark reported beside it); the GC protocol, not this
    /// handler, is what makes acting on that subset safe.
    ///
    /// # Panics
    ///
    /// If processing exposes a broken internal invariant (a programmer error,
    /// never an operating condition — a stale or below-floor request is a
    /// refusal, not a panic).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, from = request.from.0, round = request.ballot.round)))]
    pub fn step(&mut self, request: MatchRequest) {
        self.assert_invariants();
        let MatchRequest {
            from,
            journal,
            ballot,
            config,
            purpose,
            generation,
            from_ballot,
        } = request;
        let kind = match purpose {
            MatchPurpose::Register(kind) => kind,
            MatchPurpose::Probe => {
                self.answer_probe(from, journal, ballot, generation);
                return;
            }
        };
        let registration = Registration { config, kind };
        let outcome = if let Some(refusal) = self.generation_refusal(generation) {
            MatchOutcome::Refused(refusal)
        } else if ballot < self.hard_state.gc_watermark(journal) {
            MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                watermark: self.hard_state.gc_watermark(journal),
            })
        } else if let Some(highest) = self.highest(journal).filter(|highest| ballot <= *highest) {
            match self.registry(journal).get(&ballot) {
                // The same request again: answered from the retained history
                // (which GC may have shrunk since the first answer),
                // registered once.
                Some(registered) if *registered == registration => {
                    self.page(journal, from_ballot, ballot)
                }
                _ => MatchOutcome::Refused(MatchRefusal::Stale { highest }),
            }
        } else {
            // Compute the page *before* registering, so the request's own
            // configuration never appears in its own answer.
            let page = self.page(journal, from_ballot, ballot);
            let previous = self.window(journal).insert(ballot, registration.clone());
            assert!(
                previous.is_none(),
                "a fresh registration lands on an unregistered ballot"
            );
            assert!(
                self.highest(journal) == Some(ballot),
                "a fresh registration becomes the registry's highest ballot"
            );
            // A *reconfiguration* registration also raises the effective
            // configuration — the monotone scalar GC never collects (see
            // `JournalScalars::effective`). Staged in the same batch as the
            // record, so the reply that reports it never escapes a
            // non-durable scalar.
            if registration.kind.is_reconfiguration()
                && raise_effective(
                    &mut self.hard_state.journal_mut(journal).effective,
                    ballot,
                    &registration.config,
                )
            {
                self.stage_scalars();
            }
            self.pending_writes.push(MatchmakerWriteOp::Register {
                journal,
                ballot,
                registration,
            });
            page
        };
        self.assert_registered_answer(&outcome, journal, ballot, generation);
        self.pending_replies.push(MatchReply {
            matchmaker: self.config.id,
            journal,
            to: from,
            ballot,
            generation,
            outcome,
        });
        self.assert_invariants();
    }

    /// The postconditions of a successful registration answer: the ballot
    /// is registered, the history is exactly the bounded window below it, and
    /// only an active matchmaker of the addressed generation ever registers.
    fn assert_registered_answer(
        &self,
        outcome: &MatchOutcome,
        journal: JournalId,
        ballot: Ballot,
        generation: MatchmakerGeneration,
    ) {
        let MatchOutcome::Registered {
            from_ballot,
            history,
            next_from_ballot,
            ..
        } = outcome
        else {
            return;
        };
        // Postconditions of a successful answer: the ballot is registered,
        // the history is exactly the window below it, and only an active
        // matchmaker of the addressed generation ever registers.
        assert!(
            self.registry(journal).contains_key(&ballot),
            "a Registered reply names a registered ballot"
        );
        assert!(
            history.keys().all(|b| *b < ballot),
            "a history stays strictly below the ballot it answers"
        );
        assert!(
            history
                .keys()
                .all(|b| *b >= self.hard_state.gc_watermark(journal)),
            "a history never reaches below the watermark"
        );
        assert!(
            history.keys().all(|b| *b >= *from_ballot),
            "a page starts at the cursor it names"
        );
        assert!(
            history.len() <= self.page,
            "a page carries at most REGISTRY_PAGE registrations"
        );
        if next_from_ballot.is_some() {
            assert!(
                history.len() == self.page,
                "a continuation cursor follows a full page"
            );
        }
        assert!(
            next_from_ballot
                .is_none_or(|next| history.keys().next_back().is_none_or(|last| next > *last)),
            "a continuation cursor lies past the page"
        );
        assert!(
            self.phase() == MatchmakerPhase::Active,
            "only an active matchmaker registers"
        );
        assert!(
            self.set().generation == generation,
            "only a matchmaker of the addressed generation registers"
        );
    }

    /// Answer a membership probe (#173): the effective configuration this
    /// matchmaker durably holds, fenced by generation exactly as a
    /// registration is, and **nothing registered** — no record, no raised
    /// highest ballot, no write. A probe below the watermark or at a stale
    /// ballot is answered all the same: it asks for a fact, not a slot in
    /// the ledger.
    fn answer_probe(
        &mut self,
        from: NodeId,
        journal: JournalId,
        ballot: Ballot,
        generation: MatchmakerGeneration,
    ) {
        let writes = self.pending_writes.len();
        let registered = self.registry(journal).contains_key(&ballot);
        let outcome = match self.generation_refusal(generation) {
            Some(refusal) => MatchOutcome::Refused(refusal),
            None => MatchOutcome::Probed {
                effective: self.hard_state.effective(journal).cloned(),
            },
        };
        // A probe leaves the matchmaker exactly as it found it.
        assert!(
            self.pending_writes.len() == writes,
            "a membership probe stages no write"
        );
        assert!(
            self.registry(journal).contains_key(&ballot) == registered,
            "a membership probe registers nothing"
        );
        self.pending_replies.push(MatchReply {
            matchmaker: self.config.id,
            journal,
            to: from,
            ballot,
            generation,
            outcome,
        });
        self.assert_invariants();
    }

    /// The later set a request at `generation` should address instead, as
    /// `Matchmaker::generation_refusal` names it: the successor of a
    /// stopped generation, or the generation in force here. `None` when no
    /// later set is known here (this matchmaker serves `generation`, or is
    /// inactive for it).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn newer_set(&self, generation: MatchmakerGeneration) -> Option<MatchmakerSet> {
        let newer = match self.judge_generation(generation)? {
            MatchRefusal::Stopped { successor } => successor,
            // An active matchmaker behind the request (a member of the
            // requester's generation too, not yet bootstrapped there)
            // refuses with the set it serves, an earlier one: that names
            // no later set (seed 4434657833621076437 of PR #625).
            MatchRefusal::Generation { current } if current.generation > generation => {
                Some(current)
            }
            _ => None,
        };
        assert!(
            newer.as_ref().is_none_or(|set| set.generation > generation),
            "a refusal names only a later generation"
        );
        newer
    }

    /// The generation fence for a matchmaking or GC request: `None` when
    /// this matchmaker is active for exactly `generation`.
    fn generation_refusal(&self, generation: MatchmakerGeneration) -> Option<MatchRefusal> {
        let refusal = self.judge_generation(generation);
        // The negative space of the generation fence: a request is served
        // only by an active matchmaker of exactly its generation.
        if refusal.is_none() {
            assert!(
                self.phase() == MatchmakerPhase::Active,
                "only an active matchmaker serves"
            );
            assert!(
                self.set().generation == generation,
                "a request is served at its generation"
            );
        }
        refusal
    }

    /// The refusal [`Matchmaker::generation_refusal`] answers, unchecked.
    fn judge_generation(&self, generation: MatchmakerGeneration) -> Option<MatchRefusal> {
        let current = self.set();
        match self.phase() {
            MatchmakerPhase::Active if current.generation == generation => None,
            MatchmakerPhase::Active => Some(MatchRefusal::Generation {
                current: current.clone(),
            }),
            MatchmakerPhase::Stopped if current.generation == generation => {
                Some(MatchRefusal::Stopped {
                    successor: self.hard_state.successor.clone(),
                })
            }
            MatchmakerPhase::Stopped => {
                // Frozen for another generation. A proposer *behind* this
                // generation learns the chain link if there is one, else the
                // set in force here — never `Inactive`, which would leave a
                // restarted node (it boots believing the bootstrap
                // generation) with no way to discover the generation it
                // must campaign in while the successor is still undecided
                // (the sweep found exactly that cluster: every campaign
                // refused, no leader for a whole run). A proposer *ahead*
                // of this generation is simply not served here.
                match &self.hard_state.successor {
                    Some(successor) if generation < current.generation.next() => {
                        Some(MatchRefusal::Stopped {
                            successor: Some(successor.clone()),
                        })
                    }
                    None if generation < current.generation => Some(MatchRefusal::Generation {
                        current: current.clone(),
                    }),
                    _ => Some(MatchRefusal::Inactive),
                }
            }
            MatchmakerPhase::Inactive | MatchmakerPhase::Fresh => Some(MatchRefusal::Inactive),
        }
    }

    /// Advance the GC watermark to `watermark` (§3.4: `w = max(w, i)`) for
    /// `generation`, dropping every registration below it, and stage the
    /// durable write. A request at or below the current floor is a no-op
    /// (monotone by construction, never an error); one addressing a
    /// generation this matchmaker is not active for is refused.
    ///
    /// The **effective configuration** scalar
    /// ([`MatchmakerHardState::effective`]) is deliberately untouched: the
    /// floor collects the per-ballot *records* a future Phase 1 may need,
    /// and "which acceptor set is in force" is not one of those obligations.
    /// Collecting it too is what let an ordinary leader's GC erase the last
    /// reconfiguration and re-elect a superseded configuration.
    ///
    /// # This is a correctness-critical primitive, not the GC protocol
    ///
    /// Nothing here checks that the collected configurations are no longer
    /// needed. The paper's garbage collection (§3.4–§3.5) is a *protocol*
    /// whose preconditions the leader establishes (`node/gc.rs`: every slot
    /// below its election fence held by a Phase-2 quorum of its own
    /// configuration, nothing chosen below its ballot above the fence) before
    /// it sends `GarbageA`; a floor raised above a configuration some future
    /// proposer still has to contact makes that proposer's history
    /// incomplete, which is a safety violation of the whole protocol — one
    /// this state machine can neither detect nor refuse, because the
    /// knowledge lives with the proposer.
    ///
    /// # Panics
    ///
    /// If raising the floor exposes a broken internal invariant.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, round = watermark.round)))]
    pub fn advance_gc_watermark(
        &mut self,
        journal: JournalId,
        generation: MatchmakerGeneration,
        watermark: Ballot,
    ) -> GcOutcome {
        self.assert_invariants();
        if self.generation_refusal(generation).is_some() {
            return GcOutcome::Refused;
        }
        if watermark <= self.hard_state.gc_watermark(journal) {
            return GcOutcome::Unchanged;
        }
        let writes = self.pending_writes.len();
        let siblings: Vec<(JournalId, Ballot)> = self
            .hard_state
            .journals
            .iter()
            .filter(|(j, _)| **j != journal)
            .map(|(j, scalars)| (*j, scalars.gc_watermark))
            .collect();
        self.window(journal).raise_floor(watermark);
        self.hard_state.journal_mut(journal).gc_watermark = watermark;
        self.pending_writes
            .push(MatchmakerWriteOp::SetGcWatermark { journal, watermark });
        // The write side of the boot pair `Matchmaker::new` re-asserts: the
        // registry floor and the durable watermark move together, in one op.
        assert!(
            self.window(journal).floor() == watermark,
            "the registry floor is the new watermark"
        );
        // #190: a journal's floor never moves because of a sibling's.
        assert!(
            siblings
                .iter()
                .all(|(j, floor)| self.hard_state.gc_watermark(*j) == *floor),
            "a raise moves no sibling journal's watermark"
        );
        assert!(
            self.pending_writes.len() == writes + 1,
            "a raise stages exactly one write"
        );
        self.assert_invariants();
        GcOutcome::Raised
    }

    /// Drain the pending batch. Holds the unique borrow until
    /// [`MatchmakerReady::advance`], so a second `ready()` before `advance()`
    /// is a compile error.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(matchmaker = self.config.id.0)))]
    pub fn ready(&mut self) -> MatchmakerReady<'_> {
        MatchmakerReady { matchmaker: self }
    }

    /// One `MatchB` page: at most the page size (#338) of registrations from
    /// `max(cursor, watermark)` up to (but not including) `ballot`, in
    /// ballot order, with the cursor the next page starts at when the
    /// window did not fit. A belief that a later registration of the window
    /// repeats is left out ([`history_page`], #570 (election duel outlasts
    /// the recovery tail)).
    fn page(&self, journal: JournalId, cursor: Option<Ballot>, ballot: Ballot) -> MatchOutcome {
        let watermark = self.hard_state.gc_watermark(journal);
        let from_ballot = cursor.unwrap_or(watermark).max(watermark);
        // The window's own bounded page: at most `REGISTRY_PAGE` records
        // below `ballot`, and the cursor the candidate re-asks with (`None`
        // means the answer is complete).
        let (history, next_from_ballot) = match self.registries.get(&journal) {
            Some(window) => history_page(window.range(from_ballot..ballot), self.page),
            None => (BTreeMap::new(), None),
        };
        assert!(
            from_ballot >= watermark,
            "a page never starts below the watermark"
        );
        assert!(
            history.keys().all(|b| *b < ballot),
            "a page stays strictly below the ballot it answers"
        );
        MatchOutcome::Registered {
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark: watermark,
            effective: self.hard_state.effective(journal).cloned(),
        }
    }

    /// One page of the frozen registries (#439 (operator calls name their
    /// journal)): the journals strictly above `after`, at most `page` of
    /// them, each its history, watermark and effective configuration — the
    /// frozen set a `StopB` hands the reconstruction (#190). A journal with
    /// nothing to say is left out. The second value is where the next page
    /// starts, `None` when this one ends the report.
    pub(super) fn frozen_page(
        &self,
        after: Option<JournalId>,
        page: usize,
    ) -> (BTreeMap<JournalId, JournalRegistry>, Option<JournalId>) {
        assert!(page > 0, "a frozen page carries a journal");
        let mut above = self
            .journals()
            .filter(|journal| after.is_none_or(|after| *journal > after))
            .map(|journal| {
                (
                    journal,
                    JournalRegistry {
                        gc_watermark: self.hard_state.gc_watermark(journal),
                        history: self.registry(journal).clone(),
                        effective: self.hard_state.effective(journal).cloned(),
                    },
                )
            })
            .filter(|(_, registry)| !registry.is_blank());
        let registries: BTreeMap<JournalId, JournalRegistry> = above.by_ref().take(page).collect();
        let next = above
            .next()
            .is_some()
            .then(|| registries.keys().next_back().copied())
            .flatten();
        assert!(
            registries.values().all(|registry| registry
                .history
                .keys()
                .next()
                .is_none_or(|b| *b >= registry.gc_watermark)),
            "a reported history starts at or above the watermark"
        );
        // A page cut short is full, and continues after its last journal.
        if next.is_some() {
            assert!(registries.len() == page, "a cut frozen page is full");
            assert!(
                next == registries.keys().next_back().copied(),
                "a cut frozen page continues after its last journal"
            );
        }
        (registries, next)
    }

    /// The cross-field checker, called at boot and at every public mutating
    /// entry point: no registration below the watermark, a frozen or
    /// activated generation with a durable membership, pending bootstraps
    /// strictly above it, and no pending bootstrap a recorded successor has
    /// already settled. (A registration's configuration is well-formed by
    /// construction: [`AcceptorConfig::new`](crate::membership::AcceptorConfig::new) is its only constructor.)
    fn assert_invariants(&self) {
        // The page size stays inside its tunable range (#338).
        assert!(self.page > 0, "a registry page is never empty");
        assert!(
            self.page <= REGISTRY_PAGE,
            "a registry page never exceeds its ceiling"
        );
        for (journal, window) in &self.registries {
            let watermark = self.hard_state.gc_watermark(*journal);
            assert!(
                window.first_key().is_none_or(|lowest| lowest >= watermark),
                "no registration survives below the gc watermark"
            );
            assert!(
                window.floor() == watermark,
                "the registry's floor is the durable gc watermark"
            );
        }
        assert!(
            self.hard_state
                .journals
                .keys()
                .all(|journal| self.registries.contains_key(journal)),
            "every journal with scalars has a registry window"
        );
        if self.hard_state.generation > MatchmakerGeneration(0) {
            assert!(
                !self.hard_state.members.is_empty(),
                "an activated generation has a durable membership"
            );
            assert!(
                self.hard_state.phase != MatchmakerPhase::Fresh,
                "an activated generation is never fresh"
            );
        }
        if let Some(successor) = &self.hard_state.successor {
            assert!(
                successor.generation == self.hard_state.generation.next(),
                "a recorded successor is the next generation"
            );
            assert!(
                self.hard_state.phase == MatchmakerPhase::Stopped,
                "a generation with a successor is frozen"
            );
            assert!(
                self.hard_state
                    .pending
                    .iter()
                    .all(|p| p.set.generation > successor.generation || p.set == *successor),
                "a settled generation keeps no pending bootstrap but the successor's"
            );
        }
        let current = self.set().generation;
        assert!(
            self.hard_state
                .pending
                .iter()
                .all(|p| p.set.generation > current),
            "a pending bootstrap is for a later generation"
        );
        assert!(
            self.hard_state
                .pending
                .iter()
                .all(|p| p.set.contains(self.config.id)),
            "a pending bootstrap is for a set this matchmaker is a member of"
        );
        assert!(
            self.config.bootstrap.windows(2).all(|w| w[0] < w[1]),
            "the bootstrap set is sorted and deduplicated"
        );
        assert!(
            self.set == self.derive_set(),
            "the materialized set is the one the durable scalars describe"
        );
        // The effective configuration is a *scalar*, not a record: it may
        // legitimately sit below the watermark (its record was collected),
        // but it never disagrees with a record the registry still holds.
        for (journal, scalars) in &self.hard_state.journals {
            if let Some((ballot, config)) = &scalars.effective {
                assert!(
                    self.registry(*journal)
                        .get(ballot)
                        .is_none_or(|r| r.kind.is_reconfiguration() && r.config == *config),
                    "the effective configuration agrees with its own retained record"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
