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
//! matchmakers** ([`crate::DecreeAcceptor`] is the durable half every
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
//! Three of the five reconfiguration messages are *acceptor* decisions the
//! matchmaker makes from its own durable state — `Stop` (freeze), and the
//! decree's `DecreePrepare`/`DecreeAccept` (promise and vote). `Bootstrap`
//! is a durable hand-off of a proposal. `Chosen` is the one **learner
//! notification**: the matchmaker records or activates the successor it is
//! told, without re-deriving the decision, on the precondition that only a
//! proposer holding the decree's Phase-2 quorum (or a node relaying such a
//! publication) emits it. See [`ReconfigureRequest::Chosen`].
//!
//! Trusting is not the same as staying silent. The one contradiction a
//! learner *can* see, it refuses: a `Chosen` for a generation whose
//! successor this matchmaker already recorded, naming a different set, is
//! answered with the ordinary refusal (which carries the recorded
//! successor) and applied to nothing. A wrong relay is precisely what
//! produces that message, and an `activated: false` `Learned` would have
//! made it indistinguishable from a duplicate.

mod generation;
#[cfg(test)]
mod handover_model;
mod message;
mod reconfigurer;
mod state;
mod storage;
mod write;

use std::collections::BTreeMap;

pub use self::message::{
    GcAck, GcRequest, MatchOutcome, MatchRefusal, MatchReply, MatchRequest, ReconfigureReply,
    ReconfigureRequest,
};
pub use self::reconfigurer::{
    MatchmakerReconfigurer, ReconfigurerPhase, ReconfigurerReady, ReconfigurerStep, Reconstruction,
    StartRefusal,
};
pub use self::state::{
    MatchmakerConfig, MatchmakerHardState, MatchmakerPhase, PendingBootstrap, Registration,
    RegistrationKind,
};
pub(crate) use self::state::{resolved_phase, resolved_set};
pub use self::storage::RegistryStorage;
pub use self::write::{MatchmakerReady, MatchmakerWriteOp};
use crate::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::types::Ballot;

/// The most registrations one `MatchB` page carries
/// ([`MatchOutcome::Registered`]). A registry retains one record per ballot
/// above its watermark, and a cluster that elects often between two GC
/// rounds accumulates them; answering the whole ledger in one message is
/// the same unbounded reply the log's `Promise` is paged to avoid
/// ([`crate::PROMISE_BATCH`]). A candidate re-asks with a cursor until the
/// answer is complete, and only a complete answer counts toward its
/// matchmaker quorum.
pub const REGISTRY_PAGE: usize = 64;

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
    /// Every registered `ballot -> registration`, strictly increasing in
    /// ballot order (a [`BTreeMap`] keeps the order; the state machine keeps
    /// the "only ever appended above the highest" discipline).
    registry: BTreeMap<Ballot, Registration>,
    pending_writes: Vec<MatchmakerWriteOp>,
    pending_replies: Vec<MatchReply>,
    pending_reconfigure_replies: Vec<ReconfigureReply>,
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
        let mut registry = BTreeMap::new();
        for ballot in storage.registered_ballots() {
            let registration = storage
                .registration(ballot)
                .expect("every registered ballot the walk names has a readable record");
            let previous = registry.insert(ballot, registration);
            assert!(
                previous.is_none(),
                "the registry walk names each ballot once"
            );
        }
        let mut bootstrap = config.bootstrap.clone();
        bootstrap.sort_unstable();
        bootstrap.dedup();
        let mut matchmaker = Self {
            config: MatchmakerConfig {
                id: config.id,
                bootstrap,
            },
            hard_state,
            set: MatchmakerSet::new(MatchmakerGeneration(0), Vec::new()),
            registry,
            pending_writes: Vec::new(),
            pending_replies: Vec::new(),
            pending_reconfigure_replies: Vec::new(),
        };
        matchmaker.refresh_set();
        matchmaker.assert_invariants();
        matchmaker
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

    /// The registry as it stands, in ballot order (same caveat as
    /// [`Self::hard_state`]).
    #[must_use]
    pub fn registry(&self) -> &BTreeMap<Ballot, Registration> {
        &self.registry
    }

    /// The highest registered ballot, or `None` on an empty registry.
    #[must_use]
    pub fn highest(&self) -> Option<Ballot> {
        self.registry.keys().next_back().copied()
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
    }

    /// The chosen successor of this matchmaker's generation, if learned.
    #[must_use]
    pub fn successor(&self) -> Option<&MatchmakerSet> {
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
    ///   `[gc_watermark, ballot)` is computed, `config` is registered under
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
            ballot,
            config,
            kind,
            generation,
            from_ballot,
        } = request;
        let registration = Registration { config, kind };
        let outcome = if !registration.config.is_well_formed() {
            // Wire hygiene, before anything is touched: a configuration
            // that does not admit its own quorum system is refused whole. A
            // registry that stored one would make every later tally over it
            // miscount, and the boot-time `assert_invariants` would crash
            // the process on the way back in — an assert on external input,
            // which is exactly what the doctrine forbids.
            MatchOutcome::Refused(MatchRefusal::Malformed)
        } else if let Some(refusal) = self.generation_refusal(generation) {
            MatchOutcome::Refused(refusal)
        } else if ballot < self.hard_state.gc_watermark {
            MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                watermark: self.hard_state.gc_watermark,
            })
        } else if let Some(highest) = self.highest().filter(|highest| ballot <= *highest) {
            match self.registry.get(&ballot) {
                // The same request again: answered from the retained history
                // (which GC may have shrunk since the first answer),
                // registered once.
                Some(registered) if *registered == registration => self.page(from_ballot, ballot),
                _ => MatchOutcome::Refused(MatchRefusal::Stale { highest }),
            }
        } else {
            // Compute the page *before* registering, so the request's own
            // configuration never appears in its own answer.
            let page = self.page(from_ballot, ballot);
            let previous = self.registry.insert(ballot, registration.clone());
            assert!(
                previous.is_none(),
                "a fresh registration lands on an unregistered ballot"
            );
            assert!(
                self.highest() == Some(ballot),
                "a fresh registration becomes the registry's highest ballot"
            );
            // A *reconfiguration* registration also raises the effective
            // configuration — the monotone scalar GC never collects (see
            // `MatchmakerHardState::effective`). Staged in the same batch as
            // the record, so the reply that reports it never escapes a
            // non-durable scalar.
            if registration.kind.is_reconfiguration()
                && self
                    .hard_state
                    .effective
                    .as_ref()
                    .is_none_or(|(held, _)| ballot > *held)
            {
                self.hard_state.effective = Some((ballot, registration.config.clone()));
                self.stage_scalars();
            }
            self.pending_writes.push(MatchmakerWriteOp::Register {
                ballot,
                registration,
            });
            page
        };
        if let MatchOutcome::Registered {
            from_ballot,
            history,
            next_from_ballot,
            ..
        } = &outcome
        {
            // Postconditions of a successful answer: the ballot is registered,
            // the history is exactly the window below it, and only an active
            // matchmaker of the addressed generation ever registers.
            assert!(
                self.registry.contains_key(&ballot),
                "a Registered reply names a registered ballot"
            );
            assert!(
                history.keys().all(|b| *b < ballot),
                "a history stays strictly below the ballot it answers"
            );
            assert!(
                history.keys().all(|b| *b >= self.hard_state.gc_watermark),
                "a history never reaches below the watermark"
            );
            assert!(
                history.keys().all(|b| *b >= *from_ballot),
                "a page starts at the cursor it names"
            );
            assert!(
                history.len() <= REGISTRY_PAGE,
                "a page carries at most REGISTRY_PAGE registrations"
            );
            assert!(
                next_from_ballot.is_none_or(|next| history.len() == REGISTRY_PAGE
                    && history.keys().next_back().is_none_or(|last| next > *last)),
                "a continuation cursor follows a full page"
            );
            assert!(
                self.phase() == MatchmakerPhase::Active && self.set().generation == generation,
                "only an active matchmaker of the addressed generation registers"
            );
        }
        self.pending_replies.push(MatchReply {
            matchmaker: self.config.id,
            to: from,
            ballot,
            generation,
            outcome,
        });
        self.assert_invariants();
    }

    /// The generation fence for a matchmaking or GC request: `None` when
    /// this matchmaker is active for exactly `generation`.
    fn generation_refusal(&self, generation: MatchmakerGeneration) -> Option<MatchRefusal> {
        let current = self.set().clone();
        match self.phase() {
            MatchmakerPhase::Active if current.generation == generation => None,
            MatchmakerPhase::Active => Some(MatchRefusal::Generation { current }),
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
                    None if generation < current.generation => {
                        Some(MatchRefusal::Generation { current })
                    }
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
        generation: MatchmakerGeneration,
        watermark: Ballot,
    ) -> GcOutcome {
        self.assert_invariants();
        if self.generation_refusal(generation).is_some() {
            return GcOutcome::Refused;
        }
        if watermark <= self.hard_state.gc_watermark {
            return GcOutcome::Unchanged;
        }
        self.hard_state.gc_watermark = watermark;
        self.registry = self.registry.split_off(&watermark);
        self.pending_writes
            .push(MatchmakerWriteOp::SetGcWatermark(watermark));
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

    /// One `MatchB` page: at most [`REGISTRY_PAGE`] registrations from
    /// `max(cursor, watermark)` up to (but not including) `ballot`, in
    /// ballot order, with the cursor the next page starts at when the
    /// window did not fit.
    fn page(&self, cursor: Option<Ballot>, ballot: Ballot) -> MatchOutcome {
        let from_ballot = cursor
            .unwrap_or(self.hard_state.gc_watermark)
            .max(self.hard_state.gc_watermark);
        let mut window = self.registry.range(from_ballot..ballot);
        let history: BTreeMap<Ballot, Registration> = window
            .by_ref()
            .take(REGISTRY_PAGE)
            .map(|(b, c)| (*b, c.clone()))
            .collect();
        // The next key the window would have yielded: the cursor the
        // candidate re-asks with. `None` means the answer is complete.
        let next_from_ballot = window.next().map(|(b, _)| *b);
        MatchOutcome::Registered {
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark: self.hard_state.gc_watermark,
            effective: self.hard_state.effective.clone(),
        }
    }

    /// Every registration this matchmaker retains — everything at or above the
    /// watermark, with no upper bound — the whole frozen registry a `StopB`
    /// hands the reconstruction. Its own method rather than `history_below` at
    /// a maximal ballot: "everything retained" is a different question from
    /// "everything below `b`", and a sentinel ballot said so only by
    /// arithmetic accident.
    pub(super) fn history_from_watermark(&self) -> BTreeMap<Ballot, Registration> {
        self.registry
            .range(self.hard_state.gc_watermark..)
            .map(|(b, c)| (*b, c.clone()))
            .collect()
    }

    /// The cross-field checker, called at boot and at every public mutating
    /// entry point: no registration below the watermark, every registration
    /// a well-formed configuration, a frozen or activated generation with a
    /// durable membership, pending bootstraps strictly above it, and no
    /// pending bootstrap a recorded successor has already settled.
    fn assert_invariants(&self) {
        assert!(
            self.registry
                .keys()
                .next()
                .is_none_or(|lowest| *lowest >= self.hard_state.gc_watermark),
            "no registration survives below the gc watermark"
        );
        for registration in self.registry.values() {
            assert!(
                !registration.config.members().is_empty(),
                "a registered configuration names at least one acceptor"
            );
            assert!(
                registration
                    .config
                    .members()
                    .windows(2)
                    .all(|w| w[0] < w[1]),
                "a registered membership is sorted and deduplicated"
            );
        }
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
                .all(|p| p.set.generation > current && p.set.contains(self.config.id)),
            "a pending bootstrap is for a later generation this matchmaker is a member of"
        );
        assert!(
            self.set == self.derive_set(),
            "the materialized set is the one the durable scalars describe"
        );
        // The effective configuration is a *scalar*, not a record: it may
        // legitimately sit below the watermark (its record was collected),
        // but it never disagrees with a record the registry still holds.
        if let Some((ballot, config)) = &self.hard_state.effective {
            assert!(
                self.registry
                    .get(ballot)
                    .is_none_or(|r| r.kind.is_reconfiguration() && r.config == *config),
                "the effective configuration agrees with its own retained record"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{AcceptorConfig, QuorumSystem};
    use crate::single_decree::DecreeAcceptor;
    use crate::types::NodeId;

    /// Review 3 of #133: a member whose own GC floor sits *above* the
    /// reconstructed one activates with the higher floor, and its registry is
    /// exactly the reconstruction at or above that floor — before and after a
    /// restart from the durable install.
    #[test]
    fn activation_applies_the_higher_of_the_local_and_reconstructed_floors() {
        let cfg =
            |n: u64| AcceptorConfig::new(vec![NodeId(n), NodeId(n + 1)], QuorumSystem::Majority);
        // Matchmaker 0, a member of M_0 = {0, 1, 2}: registers ballots 1..=6
        // and hears a GarbageA raising its floor to ballot 5.
        let mut mm = fresh(0);
        for round in 1..=6 {
            mm.step(MatchRequest::new(
                NodeId(1),
                ballot(round, 1),
                cfg(round),
                G0,
            ));
            mm.ready().advance();
        }
        assert_eq!(mm.advance_gc_watermark(G0, ballot(5, 1)), GcOutcome::Raised);
        mm.ready().advance();
        assert_eq!(
            mm.registry().len(),
            2,
            "ballots 5 and 6 survive the local floor"
        );
        // A reconstruction from *another* frozen quorum ({1, 2}) that never
        // heard the GarbageA: floor at ballot 2, history 2..=6.
        let history: BTreeMap<Ballot, Registration> = (2..=6)
            .map(|round| (ballot(round, 1), Registration::belief(cfg(round))))
            .collect();
        let successor = MatchmakerSet::new(
            MatchmakerGeneration(1),
            vec![MatchmakerId(0), MatchmakerId(3)],
        );
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(9),
            bootstrap: PendingBootstrap {
                set: successor.clone(),
                gc_watermark: ballot(2, 1),
                history,
                effective: None,
            },
        });
        mm.ready().advance();
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(9),
            generation: G0,
            successor: successor.clone(),
        });
        let ready = mm.ready();
        let installed = ready.writes().iter().find_map(|op| match op {
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => Some((scalars.gc_watermark, registrations.clone())),
            _ => None,
        });
        ready.advance();
        let (durable_floor, durable_registry) =
            installed.expect("the activation installs the registry");
        let expected: Vec<Ballot> = vec![ballot(5, 1), ballot(6, 1)];
        assert_eq!(*mm.set(), successor);
        assert_eq!(
            mm.hard_state().gc_watermark,
            ballot(5, 1),
            "the higher floor wins"
        );
        assert_eq!(mm.registry().keys().copied().collect::<Vec<_>>(), expected);
        assert_eq!(durable_floor, ballot(5, 1));
        assert_eq!(
            durable_registry.keys().copied().collect::<Vec<_>>(),
            expected
        );
        // The restart reads back exactly that.
        let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &TestRegistry::of(&mm));
        assert_eq!(*rebooted.set(), successor);
        assert_eq!(rebooted.hard_state().gc_watermark, ballot(5, 1));
        assert_eq!(
            rebooted.registry().keys().copied().collect::<Vec<_>>(),
            expected
        );
        assert_eq!(rebooted.phase(), MatchmakerPhase::Active);
    }

    /// The port over an in-memory registry, for the state-machine tests.
    #[derive(Default)]
    struct TestRegistry {
        hard_state: MatchmakerHardState,
        registry: BTreeMap<Ballot, Registration>,
    }

    impl TestRegistry {
        /// What a reboot of `mm` reads back: its durable scalars and records.
        fn of(mm: &Matchmaker) -> Self {
            Self {
                hard_state: mm.hard_state().clone(),
                registry: mm.registry().clone(),
            }
        }
    }

    impl RegistryStorage for TestRegistry {
        fn initial_state(&self) -> MatchmakerHardState {
            self.hard_state.clone()
        }

        fn registration(&self, ballot: Ballot) -> Option<Registration> {
            self.registry.get(&ballot).cloned()
        }

        fn registered_ballots(&self) -> Vec<Ballot> {
            self.registry.keys().copied().collect()
        }
    }

    const G0: MatchmakerGeneration = MatchmakerGeneration(0);

    fn mmconfig(id: u64, bootstrap: &[u64]) -> MatchmakerConfig {
        MatchmakerConfig {
            id: MatchmakerId(id),
            bootstrap: bootstrap.iter().copied().map(MatchmakerId).collect(),
        }
    }

    fn fresh(id: u64) -> Matchmaker {
        Matchmaker::new(&mmconfig(id, &[0, 1, 2]), &TestRegistry::default())
    }

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn config(members: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            members.iter().map(|n| NodeId(*n)).collect(),
            QuorumSystem::Majority,
        )
    }

    fn request(from: u64, ballot: Ballot, members: &[u64]) -> MatchRequest {
        MatchRequest::new(NodeId(from), ballot, config(members), G0)
    }

    fn drain(mm: &mut Matchmaker) -> (Vec<MatchmakerWriteOp>, Vec<MatchReply>) {
        let ready = mm.ready();
        let writes = ready.writes().to_vec();
        let replies = ready.replies().to_vec();
        ready.advance();
        (writes, replies)
    }

    fn drain_reconfigure(mm: &mut Matchmaker) -> (Vec<MatchmakerWriteOp>, Vec<ReconfigureReply>) {
        let ready = mm.ready();
        let writes = ready.writes().to_vec();
        let replies = ready.reconfigure_replies().to_vec();
        ready.advance();
        (writes, replies)
    }

    fn registered(reply: &MatchReply) -> (&BTreeMap<Ballot, Registration>, Ballot) {
        match &reply.outcome {
            MatchOutcome::Registered {
                history,
                gc_watermark,
                ..
            } => (history, *gc_watermark),
            MatchOutcome::Refused(r) => panic!("expected a registration, got {r:?}"),
        }
    }

    fn set(generation: u64, members: &[u64]) -> MatchmakerSet {
        MatchmakerSet::new(
            MatchmakerGeneration(generation),
            members.iter().copied().map(MatchmakerId).collect(),
        )
    }

    /// Review finding P7: a second `Chosen` for one generation naming a
    /// *different* successor is the shape a wrong relay produces. It is
    /// refused — nothing recorded, nothing activated — and the refusal names
    /// the successor this matchmaker holds.
    #[test]
    fn a_chosen_contradicting_the_recorded_successor_is_refused() {
        let mut mm = fresh(0);
        let recorded = set(1, &[0, 1, 2]);
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: recorded.clone(),
        });
        drain_reconfigure(&mut mm);
        assert_eq!(mm.successor(), Some(&recorded));
        // The same publication again is idempotent.
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: recorded.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Learned {
                activated: false,
                ..
            }
        ));
        assert!(writes.is_empty(), "a duplicate publication writes nothing");
        // A contradicting one is not.
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(6),
            generation: G0,
            successor: set(1, &[0, 3, 4]),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(
                &replies[0],
                ReconfigureReply::Refused { successor: Some(s), .. } if *s == recorded
            ),
            "the refusal names the recorded successor, not the contradicting one"
        );
        assert!(
            writes.is_empty(),
            "a contradiction changes no durable state"
        );
        assert_eq!(mm.successor(), Some(&recorded));
        assert_eq!(*mm.set(), set(0, &[0, 1, 2]), "and activates nothing");
    }

    /// Review finding P6: a matchmaker bootstrapped into a proposal that
    /// *lost* its decree must not carry that whole reconstructed registry in
    /// its durable scalars forever. The learn path prunes it — here at a
    /// spare, which is outside the chosen set and so never activates, the
    /// branch where nothing else would ever drop it.
    #[test]
    fn a_losing_bootstrap_is_dropped_when_the_successor_is_learned() {
        // Matchmaker 3 is a spare of M_0 = {0, 1, 2}: it is a member of the
        // proposed {0, 1, 3} and of nothing else.
        let mut mm = Matchmaker::new(&mmconfig(3, &[0, 1, 2]), &TestRegistry::default());
        assert_eq!(mm.phase(), MatchmakerPhase::Inactive);
        let losing = set(1, &[0, 1, 3]);
        let mut history = BTreeMap::new();
        history.insert(ballot(1, 1), Registration::belief(config(&[0, 1, 2])));
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: PendingBootstrap {
                set: losing.clone(),
                gc_watermark: Ballot::zero(),
                history: history.clone(),
                effective: None,
            },
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "the pending bootstrap is durable");
        assert!(matches!(&replies[0], ReconfigureReply::Bootstrapped { .. }));
        assert_eq!(mm.hard_state().pending.len(), 1);
        // A different set wins generation 1, and this matchmaker is not in it.
        let winner = set(1, &[0, 1, 2]);
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: winner.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(&replies[0], ReconfigureReply::Refused { .. }),
            "a spare outside the chosen set activates nothing"
        );
        assert!(
            mm.hard_state().pending.is_empty(),
            "the losing bootstrap is gone"
        );
        assert!(
            writes
                .iter()
                .any(|w| matches!(w, MatchmakerWriteOp::SetScalars(scalars) if scalars.pending.is_empty())),
            "the prune is durable"
        );
        // And the loser cannot come back: a resent bootstrap for a settled
        // generation is refused. (Here from the other side — a member of the
        // succeeded generation that recorded the successor.)
        let mut member = fresh(0);
        member.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: set(1, &[0, 1, 2]),
        });
        drain_reconfigure(&mut member);
        member.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: PendingBootstrap {
                set: set(1, &[0, 1, 3]),
                gc_watermark: Ballot::zero(),
                history,
                effective: None,
            },
        });
        let (writes, replies) = drain_reconfigure(&mut member);
        assert!(matches!(&replies[0], ReconfigureReply::Refused { .. }));
        assert!(writes.is_empty(), "a settled proposal is not stored");
        assert!(member.hard_state().pending.is_empty());
    }

    #[test]
    fn a_fresh_request_registers_and_reports_the_history_strictly_below() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert_eq!(
            writes,
            vec![MatchmakerWriteOp::Register {
                ballot: ballot(1, 1),
                registration: Registration::belief(config(&[0, 1, 2])),
            }]
        );
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].to, NodeId(1));
        assert_eq!(replies[0].generation, G0);
        let (history, watermark) = registered(&replies[0]);
        assert!(history.is_empty(), "the first registration has no past");
        assert_eq!(watermark, Ballot::zero());

        mm.step(request(2, ballot(3, 2), &[1, 2, 3]));
        let (_, replies) = drain(&mut mm);
        let (history, _) = registered(&replies[0]);
        // Exactly the ballots strictly below: `< b`, not `<= b`.
        assert_eq!(
            history.keys().copied().collect::<Vec<_>>(),
            vec![ballot(1, 1)]
        );
        assert_eq!(history[&ballot(1, 1)].config, config(&[0, 1, 2]));
        assert_eq!(mm.highest(), Some(ballot(3, 2)));
    }

    /// Review finding P10a: the registry's own wire hygiene. A
    /// configuration that does not admit its own quorum system is external
    /// input, and storing one would make every later tally over it
    /// miscount — and crash the process at the next boot, when
    /// `assert_invariants` reads it back. It is refused before anything is
    /// touched, exactly as a stale or below-floor ballot is.

    #[test]
    fn a_request_at_or_below_the_highest_is_refused_with_the_highest() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
        drain(&mut mm);
        // Strictly below.
        mm.step(request(2, ballot(4, 9), &[0, 1, 2]));
        // Same round, lower node: below in the total order.
        mm.step(request(0, ballot(5, 0), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty(), "a refusal writes nothing");
        for reply in &replies {
            assert_eq!(
                reply.outcome,
                MatchOutcome::Refused(MatchRefusal::Stale {
                    highest: ballot(5, 1)
                })
            );
        }
        assert_eq!(mm.registry().len(), 1);
    }

    #[test]
    fn the_same_request_again_is_answered_without_a_second_registration() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert_eq!(writes.len(), 2);
        let (first_history, _) = registered(&replies[1]);
        let first_history = first_history.clone();

        mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty(), "a duplicate never re-registers");
        let (history, _) = registered(&replies[0]);
        assert_eq!(*history, first_history, "the re-answer is the first answer");
        assert_eq!(mm.registry().len(), 2);
    }

    #[test]
    fn a_registered_ballot_with_different_bytes_is_refused_write_once() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        drain(&mut mm);
        mm.step(request(1, ballot(1, 1), &[0, 1]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty());
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Stale {
                highest: ballot(1, 1)
            })
        );
        assert_eq!(mm.registry()[&ballot(1, 1)].config, config(&[0, 1, 2]));
    }

    #[test]
    fn a_request_below_the_watermark_is_refused_and_the_history_starts_at_it() {
        let mut mm = fresh(0);
        for round in 1..=4 {
            mm.step(request(1, ballot(round, 1), &[0, 1, 2]));
        }
        drain(&mut mm);
        assert_eq!(mm.advance_gc_watermark(G0, ballot(3, 1)), GcOutcome::Raised);
        assert_eq!(
            mm.advance_gc_watermark(G0, ballot(2, 1)),
            GcOutcome::Unchanged,
            "the floor never lowers"
        );
        assert_eq!(
            mm.advance_gc_watermark(G0, ballot(3, 1)),
            GcOutcome::Unchanged,
            "re-raising is a no-op"
        );
        assert_eq!(
            mm.advance_gc_watermark(MatchmakerGeneration(1), ballot(9, 1)),
            GcOutcome::Refused,
            "another generation's floor is refused"
        );
        let (writes, _) = drain(&mut mm);
        assert_eq!(
            writes,
            vec![MatchmakerWriteOp::SetGcWatermark(ballot(3, 1))]
        );
        assert_eq!(
            mm.registry().keys().copied().collect::<Vec<_>>(),
            vec![ballot(3, 1), ballot(4, 1)],
            "collected registrations are dropped"
        );

        mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
        mm.step(request(1, ballot(6, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                watermark: ballot(3, 1)
            })
        );
        let (history, watermark) = registered(&replies[1]);
        assert_eq!(watermark, ballot(3, 1));
        assert_eq!(
            history.keys().copied().collect::<Vec<_>>(),
            vec![ballot(3, 1), ballot(4, 1)]
        );
    }

    /// The must-fix of #131's review: a retry after GC is answered from the
    /// *retained* history — a strict subset of the first answer, with the
    /// raised watermark beside it — and still registers nothing.
    #[test]
    fn a_retry_after_gc_is_answered_from_the_retained_history() {
        let mut mm = fresh(0);
        for round in 1..=3 {
            mm.step(request(1, ballot(round, 1), &[0, 1, 2]));
        }
        let (_, replies) = drain(&mut mm);
        let (first, watermark) = registered(&replies[2]);
        assert_eq!(
            first.keys().copied().collect::<Vec<_>>(),
            vec![ballot(1, 1), ballot(2, 1)]
        );
        assert_eq!(watermark, Ballot::zero());

        // The first reply is lost; GC moves the floor; the client retries.
        assert_eq!(mm.advance_gc_watermark(G0, ballot(2, 1)), GcOutcome::Raised);
        drain(&mut mm);
        mm.step(request(1, ballot(3, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty(), "a retry never re-registers");
        let (retry, watermark) = registered(&replies[0]);
        assert_eq!(
            retry.keys().copied().collect::<Vec<_>>(),
            vec![ballot(2, 1)],
            "the retry sees the retained window only"
        );
        assert_eq!(watermark, ballot(2, 1), "and the raised floor beside it");
        assert!(
            retry.iter().all(|(b, c)| first.get(b) == Some(c)),
            "a re-answer is a subset of the first answer, never a superset"
        );
    }

    /// The paros-specific keying: two proposers at the *same round* hold two
    /// distinct registry keys, ordered by node, so neither can overwrite the
    /// other and the lower one is an ancestor in the higher one's history. A
    /// registry keyed on the bare round would give them one key.
    #[test]
    fn ballots_at_one_round_from_two_proposers_are_distinct_keys() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
        mm.step(request(2, ballot(5, 2), &[3, 4, 5]));
        let (writes, replies) = drain(&mut mm);
        assert_eq!(writes.len(), 2, "two keys, two registrations");
        let (history, _) = registered(&replies[1]);
        assert_eq!(
            history.get(&ballot(5, 1)).map(|r| &r.config),
            Some(&config(&[0, 1, 2])),
            "the same-round lower ballot is in the higher one's history"
        );
        assert_eq!(mm.registry().len(), 2);
        assert_eq!(mm.registry()[&ballot(5, 1)].config, config(&[0, 1, 2]));
        assert_eq!(mm.registry()[&ballot(5, 2)].config, config(&[3, 4, 5]));

        // The reverse arrival order: the lower node's ballot is stale once
        // the higher node's is registered, and refused — never merged into it.
        let mut mm = fresh(1);
        mm.step(request(2, ballot(5, 2), &[3, 4, 5]));
        mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        assert_eq!(
            replies[1].outcome,
            MatchOutcome::Refused(MatchRefusal::Stale {
                highest: ballot(5, 2)
            })
        );
        assert_eq!(mm.registry().len(), 1);
    }

    #[test]
    fn a_restart_answers_exactly_as_the_original_would_have() {
        let mut mm = fresh(2);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        mm.step(request(2, ballot(2, 2), &[0, 1, 2, 3]));
        drain(&mut mm);
        // A reboot walks the durable records back through the port.
        let mut rebooted = Matchmaker::new(&mmconfig(2, &[0, 1, 2]), &TestRegistry::of(&mm));
        rebooted.step(request(1, ballot(3, 1), &[1, 2, 3]));
        mm.step(request(1, ballot(3, 1), &[1, 2, 3]));
        let (_, expected) = drain(&mut mm);
        let (_, observed) = drain(&mut rebooted);
        assert_eq!(observed, expected);
    }

    #[test]
    #[should_panic(expected = "no registration survives below the gc watermark")]
    fn a_registry_below_its_watermark_refuses_to_boot() {
        let mut store = TestRegistry::default();
        store
            .registry
            .insert(ballot(1, 1), Registration::belief(config(&[0])));
        store.hard_state.gc_watermark = ballot(2, 0);
        let _ = Matchmaker::new(&mmconfig(0, &[0]), &store);
    }

    #[test]
    fn a_configuration_is_normalized_and_sizes_its_quorum() {
        let config = AcceptorConfig::new(
            vec![NodeId(2), NodeId(0), NodeId(2), NodeId(1)],
            QuorumSystem::Majority,
        );
        assert_eq!(config.members(), vec![NodeId(0), NodeId(1), NodeId(2)]);
        let two: std::collections::BTreeSet<NodeId> = [NodeId(0), NodeId(1)].into_iter().collect();
        let one: std::collections::BTreeSet<NodeId> = [NodeId(0)].into_iter().collect();
        assert!(config.has_phase1_quorum(&two) && config.has_phase2_quorum(&two));
        assert!(!config.has_phase1_quorum(&one) && !config.has_phase2_quorum(&one));
    }

    // ---- generations (#125) ---------------------------------------------

    /// A fresh store resolves against the bootstrap set: a member is active
    /// for generation 0, a spare is inactive and refuses everything with
    /// `Inactive` — neither writes anything at boot.
    #[test]
    fn a_fresh_matchmaker_resolves_its_phase_from_the_bootstrap_set() {
        let member = fresh(0);
        assert_eq!(member.phase(), MatchmakerPhase::Active);
        assert_eq!(*member.set(), set(0, &[0, 1, 2]));
        let mut spare = Matchmaker::new(&mmconfig(7, &[0, 1, 2]), &TestRegistry::default());
        assert_eq!(spare.phase(), MatchmakerPhase::Inactive);
        spare.step(request(1, ballot(1, 1), &[0, 1, 2]));
        let (writes, replies) = drain(&mut spare);
        assert!(writes.is_empty());
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Inactive)
        );
    }

    /// A request fenced by another generation is refused with the current
    /// set, never served.
    #[test]
    fn a_request_for_another_generation_is_refused_with_the_current_set() {
        let mut mm = fresh(0);
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(1, 1),
            config(&[0, 1, 2]),
            MatchmakerGeneration(3),
        ));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty());
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Generation {
                current: set(0, &[0, 1, 2])
            })
        );
    }

    /// The freeze: a stop is durable before its answer, idempotent, and a
    /// frozen matchmaker registers nothing for its generation ever again —
    /// across a reboot too.
    #[test]
    fn a_stop_freezes_durably_and_is_idempotent() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        drain(&mut mm);
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(5),
            generation: G0,
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(writes.as_slice(), [MatchmakerWriteOp::SetScalars(s)] if s.phase == MatchmakerPhase::Stopped)
        );
        let ReconfigureReply::Stopped {
            generation,
            history,
            successor,
            ..
        } = &replies[0]
        else {
            panic!("expected Stopped, got {:?}", replies[0]);
        };
        assert_eq!(*generation, G0);
        assert_eq!(history.len(), 1);
        assert!(successor.is_none());
        // Idempotent: no second write.
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(6),
            generation: G0,
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(writes.is_empty(), "a re-sent stop writes nothing");
        assert!(matches!(replies[0], ReconfigureReply::Stopped { .. }));
        // Frozen, and still frozen after a reboot.
        for mm in [
            &mut mm.clone(),
            &mut Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &TestRegistry::of(&mm)),
        ] {
            mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
            let (writes, replies) = drain(mm);
            assert!(writes.is_empty());
            assert_eq!(
                replies[0].outcome,
                MatchOutcome::Refused(MatchRefusal::Stopped { successor: None })
            );
        }
        // A stop for another generation is refused with what is known.
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(5),
            generation: MatchmakerGeneration(4),
        });
        let (_, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Refused { current, phase: MatchmakerPhase::Stopped, .. } if *current == set(0, &[0, 1, 2])
        ));
    }

    /// The handover end to end at one matchmaker in both generations: stop,
    /// bootstrap (pending, refused to serve), decree votes (durable), chosen
    /// (activates the pending registry whole, at the reconstructed
    /// watermark), and the new generation serves from the reconstruction.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn a_chosen_successor_activates_its_pending_bootstrap() {
        let mut mm = fresh(0);
        mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
        drain(&mut mm);
        mm.step_reconfigure(ReconfigureRequest::Stop {
            from: NodeId(5),
            generation: G0,
        });
        drain_reconfigure(&mut mm);
        let successor = set(1, &[0, 3, 4]);
        let mut history = BTreeMap::new();
        history.insert(ballot(1, 1), Registration::belief(config(&[0, 1, 2])));
        history.insert(ballot(2, 2), Registration::belief(config(&[1, 2, 3])));
        let bootstrap = PendingBootstrap {
            set: successor.clone(),
            gc_watermark: ballot(1, 1),
            history,
            effective: None,
        };
        // A bootstrap for a set this matchmaker is not in is refused.
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: PendingBootstrap {
                set: set(1, &[3, 4, 5]),
                ..bootstrap.clone()
            },
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(writes.is_empty());
        assert!(matches!(replies[0], ReconfigureReply::Refused { .. }));
        mm.step_reconfigure(ReconfigureRequest::Bootstrap {
            from: NodeId(5),
            bootstrap: bootstrap.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "the pending bootstrap is durable");
        assert!(
            matches!(&replies[0], ReconfigureReply::Bootstrapped { set: s, .. } if *s == successor)
        );
        // Still frozen at generation 0: generation 1 is not served yet.
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(5, 1),
            config(&[0, 1, 2]),
            MatchmakerGeneration(1),
        ));
        let (_, replies) = drain(&mut mm);
        assert!(matches!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Inactive)
        ));
        // The decree over generation 0.
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(5),
            generation: G0,
            ballot: ballot(1, 5),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "a promise is durable");
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Promised { vote: None, .. }
        ));
        mm.step_reconfigure(ReconfigureRequest::DecreeAccept {
            from: NodeId(5),
            generation: G0,
            ballot: ballot(1, 5),
            members: successor.members.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert_eq!(writes.len(), 1, "a vote is durable");
        assert!(matches!(&replies[0], ReconfigureReply::Accepted { .. }));
        // A lower decree ballot is refused with the promise.
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(4),
            generation: G0,
            ballot: ballot(1, 4),
        });
        let (_, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(&replies[0], ReconfigureReply::Nacked { promised, .. } if *promised == ballot(1, 5))
        );
        // A higher one learns the vote (P2c's input).
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(4),
            generation: G0,
            ballot: ballot(2, 4),
        });
        let (_, replies) = drain_reconfigure(&mut mm);
        assert!(
            matches!(&replies[0], ReconfigureReply::Promised { vote: Some((b, m)), .. } if *b == ballot(1, 5) && *m == successor.members)
        );
        // Chosen: the successor is recorded for generation 0 and, being a
        // member holding the bootstrap, this matchmaker activates it.
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: successor.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Learned {
                activated: true,
                ..
            }
        ));
        assert!(
            matches!(writes.last(), Some(MatchmakerWriteOp::InstallRegistry { scalars, registrations }) if scalars.generation == MatchmakerGeneration(1) && registrations.len() == 2)
        );
        assert_eq!(mm.phase(), MatchmakerPhase::Active);
        assert_eq!(*mm.set(), successor);
        assert_eq!(mm.hard_state().gc_watermark, ballot(1, 1));
        assert!(mm.successor().is_none());
        assert_eq!(mm.hard_state().decree, DecreeAcceptor::default());
        // Generation 1 serves from the reconstruction; generation 0 is told
        // the chain link is gone from here (this matchmaker moved on).
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(5, 1),
            config(&[0, 1, 2]),
            MatchmakerGeneration(1),
        ));
        mm.step(request(1, ballot(6, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        let (history, wm) = registered(&replies[0]);
        assert_eq!(history.len(), 2);
        assert_eq!(wm, ballot(1, 1));
        assert_eq!(
            replies[1].outcome,
            MatchOutcome::Refused(MatchRefusal::Generation {
                current: successor.clone()
            })
        );
        // A reboot lands in generation 1 with the installed registry (plus
        // the one registration generation 1 took above).
        let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &TestRegistry::of(&mm));
        assert_eq!(*rebooted.set(), successor);
        assert_eq!(rebooted.registry().len(), 3);
    }

    /// A member of the succeeded generation that is *not* in the successor
    /// records the chain link (freezing on the spot if it was still active)
    /// and points late proposers at it forever.
    #[test]
    fn a_departed_matchmaker_answers_with_its_successor() {
        let mut mm = fresh(2);
        let successor = set(1, &[0, 1, 3]);
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: G0,
            successor: successor.clone(),
        });
        let (writes, replies) = drain_reconfigure(&mut mm);
        assert!(matches!(
            &replies[0],
            ReconfigureReply::Learned {
                activated: false,
                ..
            }
        ));
        assert_eq!(writes.len(), 2, "the freeze and the link are durable");
        assert_eq!(mm.phase(), MatchmakerPhase::Stopped);
        assert_eq!(mm.successor(), Some(&successor));
        mm.step(request(1, ballot(9, 1), &[0, 1, 2]));
        let (_, replies) = drain(&mut mm);
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Stopped {
                successor: Some(successor.clone())
            })
        );
        // A second, different "chosen" for the same generation is refused
        // (the first record stands; the audit judges the conflict).
        mm.step_reconfigure(ReconfigureRequest::Chosen {
            from: NodeId(6),
            generation: G0,
            successor: set(1, &[4, 5, 6]),
        });
        let (writes, _) = drain_reconfigure(&mut mm);
        assert!(writes.is_empty());
        assert_eq!(mm.successor(), Some(&successor));
    }
}
