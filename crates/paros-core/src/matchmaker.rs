//! The **matchmaker**: a per-ballot acceptor-configuration registry (Matchmaker
//! Paxos §3.1–§3.2), as a sans-IO state machine beside [`crate::RawNode`].
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
//! `step` → `ready` → `advance` shape as [`crate::RawNode`], so the driver's
//! persist-before-reply ordering is structural rather than remembered. It is a
//! **separate handle the caller drives**: [`crate::RawNode`] never steps a
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
//! # What this proves, and what it does not
//!
//! Everything above is a property of **one matchmaker**: each registry, taken
//! alone, is write-once, monotone, complete below any ballot it answers, and
//! durable before it answers. That is the whole of M4.1 (#119). The paper's
//! safety argument (§3.3) rests on something more: a proposer collects
//! `MatchB` from `f + 1` of the `2f + 1` matchmakers and runs Phase 1 against
//! the **union** of their histories, and it is the intersection of any two
//! such `f + 1` sets that guarantees no configuration used below the
//! proposer's ballot is missed. That union, the quorum it needs, and the
//! cross-configuration Phase 1 it feeds belong to the leader-side matchmaking
//! phase, the next issue; nothing here claims them, and the simulation that
//! exercises this module proves per-matchmaker correctness only.

use std::collections::BTreeMap;

use crate::state::QuorumSystem;
use crate::types::{Ballot, NodeId};

/// Stable identity of a matchmaker in the (fixed, for now) matchmaker set. A
/// distinct namespace from [`NodeId`]: a matchmaker is not an acceptor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerId(pub u64);

/// An acceptor configuration as registered with a matchmaker: a membership
/// plus the quorum system in force over it — [`crate::Config`] minus the
/// per-node `id`. The core never interprets it beyond storing and reporting it;
/// the leader-side matchmaking phase (a later issue) is what runs Phase 1
/// against it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AcceptorConfig {
    /// The full membership, sorted and deduplicated (a [`Vec`] keeps iteration
    /// deterministic without a map).
    pub members: Vec<NodeId>,
    /// The quorum system election and decide consult over `members`.
    pub quorum_system: QuorumSystem,
}

impl AcceptorConfig {
    /// A configuration over `members` (sorted and deduplicated here) under
    /// `quorum_system`.
    ///
    /// # Panics
    ///
    /// If `members` is empty: a configuration with no acceptor can never form
    /// a quorum, so registering one is a programmer error.
    #[must_use]
    pub fn new(mut members: Vec<NodeId>, quorum_system: QuorumSystem) -> Self {
        members.sort_unstable();
        members.dedup();
        assert!(
            !members.is_empty(),
            "an acceptor configuration names at least one acceptor"
        );
        Self {
            members,
            quorum_system,
        }
    }

    /// The number of acceptors that form a quorum over this configuration.
    ///
    /// # Panics
    ///
    /// If the quorum system cannot self-intersect over this membership: Paxos
    /// safety rests on any two quorums of one configuration sharing an
    /// acceptor (for the majority system, `2q > n`), and a configuration that
    /// breaks it must fail loudly rather than let two values be chosen for
    /// one slot.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        let n = self.members.len();
        let q = self.quorum_system.quorum_size(n);
        assert!(q >= 1, "a quorum requires at least one acceptor");
        assert!(2 * q > n, "any two quorums must intersect");
        q
    }

    /// Whether this configuration can be run at all: at least one acceptor,
    /// and a quorum system whose quorums all intersect (`2q > n`). The
    /// operating-condition twin of [`AcceptorConfig::quorum_size`]'s hard
    /// asserts: a boundary that takes a configuration from outside
    /// (`RawNode::reconfigure`) refuses a malformed one here instead of
    /// letting a later quorum tally panic on it.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let n = self.members.len();
        if n == 0 {
            return false;
        }
        let q = self.quorum_system.quorum_size(n);
        q >= 1 && 2 * q > n
    }

    /// Whether `node` is a member of this configuration.
    #[must_use]
    pub fn contains(&self, node: NodeId) -> bool {
        self.members.binary_search(&node).is_ok()
    }

    /// How many of `nodes` are members of this configuration.
    #[must_use]
    pub fn count_members<'a>(&self, nodes: impl IntoIterator<Item = &'a NodeId>) -> usize {
        nodes.into_iter().filter(|n| self.contains(**n)).count()
    }
}

/// The small, persisted-whole durable scalars of a matchmaker — the
/// registry's [`crate::HardState`]. Today that is the GC watermark alone; the
/// matchmaker-set generation tag (#22's later step) lands here as a new field,
/// which is why the struct is `#[non_exhaustive]` and built through
/// [`Default`].
///
/// The per-ballot registrations are deliberately **not** here: they are
/// persisted one record at a time and read back one record at a time through
/// [`RegistryStorage`], exactly as the accepted log is split from
/// [`crate::HardState`]. See [`RegistryStorage`] for why.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct MatchmakerHardState {
    /// The GC watermark (§3.4): a monotone floor below which no request may
    /// register and below which registrations have been dropped. This stage
    /// only *stores, enforces and reports* it — the protocol that decides when
    /// raising it is safe is a separate issue;
    /// [`Matchmaker::advance_gc_watermark`] is the primitive it will call. [`Ballot::zero`] is the "nothing
    /// collected" floor: no ballot sits below it.
    pub gc_watermark: Ballot,
}

/// The read-only recovery port of a matchmaker — the registry's
/// [`crate::Storage`], mirrored method for method. The **application**
/// implements it and owns *all* writes; the core only ever *reads back*, once
/// at construction, what the driver has already persisted through its write
/// extension (`paros::MatchmakerStorage`, the `NodeStorage` twin).
///
/// # Why a per-record port and not a state blob
///
/// A matchmaker could be booted from one `(registry map, watermark)` value —
/// the registry is small. It is not, on purpose, because the registry is
/// durable state that **will rot**, and the CTRL recovery story built for the
/// accepted log (Stages 7–8: `docs/analysis/storage/ctrl-multipaxos-restatement.md`)
/// only applies to state the core reads *record by record* through a port the
/// storage layer can classify at its seam:
///
/// - **Detection lives in the write layer's `boot_scan`, per record.** Each
///   registration is one checksummed record with its identity — the ballot —
///   in the checksummed region, so a torn, misdirected or bit-flipped
///   registration is classified *before* any byte reaches this port, exactly
///   as an accepted entry is. A blob would have one checksum for the whole
///   registry and one verdict: crash.
/// - **The tri-state lands here.** CTRL's insight for the log — a record whose
///   *value* is lost but whose *identity* survived must be reported as
///   `faulty`, never as "nothing here" — holds for the registry with the same
///   force: a lost registration answered as "no configuration below `b`"
///   under-reports a history, which is precisely the bug class matchmakers
///   exist to prevent. The repair is the log's too: the identity is known,
///   the other matchmakers hold the bytes. When that stage lands it adds a
///   defaulted `faulty_registrations()` beside [`Self::registration`], the
///   twin of [`crate::Storage::faulty_entries`], and the core reports the
///   ballot instead of a history that silently omits it.
/// - **Per-record writes are what make the seams honest.** The driver applies
///   one [`MatchmakerWriteOp`] per record and fsyncs the batch before the
///   reply leaves; a boot that reads records back one by one is the read-side
///   pair of that write ordering, and the audit compares the two.
///
/// Bootstrap and restart are the same path: a fresh matchmaker is an empty
/// port. All methods are infallible: a record that fails its integrity check
/// never reaches the core (the scan withholds it, and crashes or classifies).
pub trait RegistryStorage {
    /// The durable scalars to initialize the matchmaker with. Called once, at
    /// construction.
    fn initial_state(&self) -> MatchmakerHardState;

    /// The record registered under `ballot`, if any — the per-record read,
    /// the twin of [`crate::Storage::accepted`].
    fn registration(&self, ballot: Ballot) -> Option<Registration>;

    /// Every registered ballot in ascending order — the registry's
    /// identities, the twin of the `first_slot..=last_slot` walk. Each names a
    /// record [`Self::registration`] serves.
    fn registered_ballots(&self) -> Vec<Ballot>;
}

/// One ledger record: the configuration registered under a ballot, and
/// whether registering it was an **operator's reconfiguration** (a leader
/// moving the cluster to a new acceptor set, `RawNode::reconfigure`) rather
/// than a candidate restating the configuration it believed in force.
///
/// The flag is what makes the ledger answer "which configuration is in
/// force?" without treating every registration as a fact: an ordinary
/// campaign registers a *belief* (possibly stale, possibly abandoned), and a
/// ledger full of beliefs made "adopt the newest registration" flip-flop
/// between two candidates' beliefs forever. A reconfiguration registration is
/// an explicit request, and requests are monotone by ballot: the
/// highest-ballot one a matchmaker quorum holds is the **effective
/// configuration** — the one every ordinary campaign must register (see
/// `RawNode::on_match_reply`). Once a reconfiguration's matchmaking has
/// completed at a matchmaker quorum, quorum intersection hands that record
/// to every later campaign's matchmaking, so no later ordinary election can
/// reinstate a superseded configuration; before that it may be lost like any
/// proposal that never reached a quorum.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Registration {
    /// The acceptor configuration registered.
    pub config: AcceptorConfig,
    /// Whether this registration is a reconfiguration request.
    pub reconfiguration: bool,
}

impl Registration {
    /// A candidate's belief: the configuration it intends to run with.
    #[must_use]
    pub fn belief(config: AcceptorConfig) -> Self {
        Self {
            config,
            reconfiguration: false,
        }
    }

    /// A reconfiguration request: the configuration a leader moves to.
    #[must_use]
    pub fn reconfiguration(config: AcceptorConfig) -> Self {
        Self {
            config,
            reconfiguration: true,
        }
    }
}

/// A proposer's matchmaking request: "register `config` for `ballot`, and tell
/// me every configuration registered below it" (the paper's `MatchA`).
///
/// Evolution (a matchmaker-set generation tag, #22's later step) happens by
/// appending fields here and on the wire contract — never by re-keying the
/// registry or reshaping the reply.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchRequest {
    /// The requesting proposer.
    pub from: NodeId,
    /// The ballot to register under. One ballot has exactly one proposer, so
    /// `ballot.node` is the identity that keeps matchmakers from disagreeing.
    pub ballot: Ballot,
    /// The acceptor configuration the proposer intends to run `ballot` with.
    pub config: AcceptorConfig,
    /// Whether this is a reconfiguration request (see [`Registration`]).
    pub reconfiguration: bool,
}

impl MatchRequest {
    /// A candidate's request to register its belief `config` under `ballot`.
    #[must_use]
    pub fn new(from: NodeId, ballot: Ballot, config: AcceptorConfig) -> Self {
        Self {
            from,
            ballot,
            config,
            reconfiguration: false,
        }
    }

    /// A leader's request to register the reconfiguration to `config` under
    /// `ballot` (see [`Registration`]).
    #[must_use]
    pub fn reconfigure(from: NodeId, ballot: Ballot, config: AcceptorConfig) -> Self {
        Self {
            from,
            ballot,
            config,
            reconfiguration: true,
        }
    }

    /// The ledger record this request registers.
    #[must_use]
    pub fn registration(&self) -> Registration {
        Registration {
            config: self.config.clone(),
            reconfiguration: self.reconfiguration,
        }
    }
}

/// Why a matchmaker refused a request. Each variant carries enough for the
/// requester to make progress — the highest registered ballot, the watermark
/// — exactly as [`crate::Message::Nack`] carries `promised`; and, like
/// `Nack.promised`, the requester must treat it as a **diagnostic**, never as
/// trusted future-ballot input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchRefusal {
    /// The requested ballot is not strictly above every registered ballot
    /// (frankenpaxos: `round <= acceptorGroups.lastKey`), or it names a ballot
    /// already registered with a *different* configuration.
    Stale {
        /// The highest ballot this matchmaker has registered.
        highest: Ballot,
    },
    /// The requested ballot sits below the GC watermark: its configuration was
    /// (or may have been) collected, so nothing below the floor may register.
    BelowWatermark {
        /// The matchmaker's current watermark.
        watermark: Ballot,
    },
}

/// What a matchmaker answered a request with.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchOutcome {
    /// The configuration is registered (durably, once the driver has flushed
    /// the batch this reply travels in — see [`MatchmakerReady`]) and this is
    /// the paper's `MatchB`: every configuration registered at a ballot
    /// **strictly below** the request's and at or above the watermark.
    Registered {
        /// `ballot -> registration` for every registration in
        /// `[gc_watermark, request.ballot)`, in ballot order.
        history: BTreeMap<Ballot, Registration>,
        /// The watermark in force when the history was computed.
        gc_watermark: Ballot,
    },
    /// The request was refused; nothing was registered.
    Refused(MatchRefusal),
}

/// A matchmaker's answer to one [`MatchRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchReply {
    /// The answering matchmaker.
    pub matchmaker: MatchmakerId,
    /// The requester the reply is addressed to.
    pub to: NodeId,
    /// The request's ballot, echoed.
    pub ballot: Ballot,
    /// The answer.
    pub outcome: MatchOutcome,
}

/// A single semantic durable write the driver must apply to stable storage
/// and **fsync before** the batch's replies leave — every matchmaker write is
/// safety-critical, so there is no relaxed class here.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchmakerWriteOp {
    /// Register `config` under `ballot`. Append-only: `ballot` is strictly
    /// above every ballot the registry holds.
    Register {
        /// The ballot registered.
        ballot: Ballot,
        /// The record registered under it.
        registration: Registration,
    },
    /// Raise the durable GC watermark to `watermark` and drop every
    /// registration below it. Monotone: never below the current watermark.
    SetGcWatermark(Ballot),
}

/// One batch of matchmaker work, and the compile-time gate enforcing one batch
/// in flight — the matchmaker's [`crate::Ready`].
///
/// # Durability ordering — process the buckets in this order
///
/// 1. **Persist** [`MatchmakerReady::writes`] to stable storage, in order, and
///    fsync them. Every write here is safety-critical.
/// 2. **Send** [`MatchmakerReady::replies`] — *only after* step 1 is durable.
///    A `Registered` reply published before its registration is on disk is
///    the matchmaker's version of an un-promise: a crash then forgets a
///    configuration the proposer already believes every later leader will be
///    told about.
/// 3. Call [`MatchmakerReady::advance`] to release the gate.
#[must_use = "a MatchmakerReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct MatchmakerReady<'a> {
    matchmaker: &'a mut Matchmaker,
}

impl MatchmakerReady<'_> {
    /// The durable writes to persist and fsync **first** (step 1), in order.
    #[must_use]
    pub fn writes(&self) -> &[MatchmakerWriteOp] {
        &self.matchmaker.pending_writes
    }

    /// The replies to send **after** the writes are durable (step 2).
    #[must_use]
    pub fn replies(&self) -> &[MatchReply] {
        &self.matchmaker.pending_replies
    }

    /// Acknowledge the batch: clears the pending buckets and releases the
    /// unique borrow. Consumes `self` — the guard cannot be reused.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(matchmaker = self.matchmaker.id.0)))]
    pub fn advance(self) {
        self.matchmaker.pending_writes.clear();
        self.matchmaker.pending_replies.clear();
    }
}

/// The matchmaker state machine: the registry, the watermark, and one pending
/// batch of writes and replies. Pure — no I/O, no clock, no randomness; the
/// driver ([`paros::run_matchmaker`](https://docs.rs/paros)) performs every
/// side effect it describes.
#[derive(Clone, Debug)]
pub struct Matchmaker {
    id: MatchmakerId,
    hard_state: MatchmakerHardState,
    /// Every registered `ballot -> registration`, strictly increasing in
    /// ballot order (a [`BTreeMap`] keeps the order; the state machine keeps
    /// the "only ever appended above the highest" discipline).
    registry: BTreeMap<Ballot, Registration>,
    pending_writes: Vec<MatchmakerWriteOp>,
    pending_replies: Vec<MatchReply>,
}

impl Matchmaker {
    /// Boot a matchmaker from its durable storage: read the scalars once, then
    /// walk the registry record by record (a fresh matchmaker is an empty
    /// port). Restart and first boot are the same path, so a rebooted
    /// matchmaker answers exactly as it would have without the crash.
    ///
    /// # Panics
    ///
    /// If the durable state violates the registry contract — a ballot the
    /// walk names but the port cannot serve, a registration below the
    /// watermark, a malformed configuration. That means corrupted storage that
    /// evaded the scan or a broken storage implementation; crashing beats
    /// answering from it.
    #[must_use]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = id.0)))]
    pub fn new<S: RegistryStorage>(id: MatchmakerId, storage: &S) -> Self {
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
        let matchmaker = Self {
            id,
            hard_state,
            registry,
            pending_writes: Vec::new(),
            pending_replies: Vec::new(),
        };
        matchmaker.assert_invariants();
        matchmaker
    }

    /// This matchmaker's identity.
    #[must_use]
    pub fn id(&self) -> MatchmakerId {
        self.id
    }

    /// The durable scalars as they stand (including a raise not yet flushed
    /// by the driver — the core applies a write the instant it decides it,
    /// exactly as [`crate::RawNode`] does).
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

    /// Answer one matchmaking request (the paper's `MatchA` handler, §3.2):
    ///
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
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.id.0, from = request.from.0, round = request.ballot.round)))]
    pub fn step(&mut self, request: MatchRequest) {
        self.assert_invariants();
        let MatchRequest {
            from,
            ballot,
            config,
            reconfiguration,
        } = request;
        let registration = Registration {
            config,
            reconfiguration,
        };
        let outcome = if ballot < self.hard_state.gc_watermark {
            MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                watermark: self.hard_state.gc_watermark,
            })
        } else if let Some(highest) = self.highest().filter(|highest| ballot <= *highest) {
            match self.registry.get(&ballot) {
                // The same request again: answered from the retained history
                // (which GC may have shrunk since the first answer),
                // registered once.
                Some(registered) if *registered == registration => MatchOutcome::Registered {
                    history: self.history_below(ballot),
                    gc_watermark: self.hard_state.gc_watermark,
                },
                _ => MatchOutcome::Refused(MatchRefusal::Stale { highest }),
            }
        } else {
            // Compute the history *before* registering, so the request's own
            // configuration never appears in its own answer.
            let history = self.history_below(ballot);
            let previous = self.registry.insert(ballot, registration.clone());
            assert!(
                previous.is_none(),
                "a fresh registration lands on an unregistered ballot"
            );
            assert!(
                self.highest() == Some(ballot),
                "a fresh registration becomes the registry's highest ballot"
            );
            self.pending_writes.push(MatchmakerWriteOp::Register {
                ballot,
                registration,
            });
            MatchOutcome::Registered {
                history,
                gc_watermark: self.hard_state.gc_watermark,
            }
        };
        if let MatchOutcome::Registered { history, .. } = &outcome {
            // Postconditions of a successful answer: the ballot is registered,
            // and the history is exactly the window below it.
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
        }
        self.pending_replies.push(MatchReply {
            matchmaker: self.id,
            to: from,
            ballot,
            outcome,
        });
        self.assert_invariants();
    }

    /// Advance the GC watermark to `watermark` (§3.4: `w = max(w, i)`),
    /// dropping every registration below it, and stage the durable write.
    /// Returns whether the watermark actually rose; a request at or below the
    /// current floor is a no-op (monotone by construction, never an error).
    ///
    /// # This is a correctness-critical primitive, not the GC protocol
    ///
    /// Nothing here checks that the collected configurations are no longer
    /// needed. The paper's garbage collection (§3.4–§3.5) is a *protocol*: a
    /// proposer may send `GarbageA⟨i⟩` only after establishing one of three
    /// conditions (a value chosen in round `i`; Phase 1 at `i` found nothing
    /// chosen below; or the chosen value is safely replicated and a Phase 2
    /// quorum of `Ci` informed), and only then do the matchmakers drop rounds
    /// below `i`. **The caller must have established those preconditions**;
    /// a floor raised above a configuration some future proposer still has to
    /// contact makes that proposer's history incomplete, which is a safety
    /// violation of the whole protocol — one this state machine can neither
    /// detect nor refuse, because the knowledge lives with the proposer.
    /// Until the GC protocol lands (a later issue), the only callers are the
    /// driver's `GarbageCollect` RPC and the simulation that drives it, where
    /// no leader depends on the registry yet.
    ///
    /// # Panics
    ///
    /// If raising the floor exposes a broken internal invariant.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.id.0, round = watermark.round)))]
    pub fn advance_gc_watermark(&mut self, watermark: Ballot) -> bool {
        self.assert_invariants();
        if watermark <= self.hard_state.gc_watermark {
            return false;
        }
        self.hard_state.gc_watermark = watermark;
        self.registry = self.registry.split_off(&watermark);
        self.pending_writes
            .push(MatchmakerWriteOp::SetGcWatermark(watermark));
        self.assert_invariants();
        true
    }

    /// Drain the pending batch. Holds the unique borrow until
    /// [`MatchmakerReady::advance`], so a second `ready()` before `advance()`
    /// is a compile error.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(matchmaker = self.id.0)))]
    pub fn ready(&mut self) -> MatchmakerReady<'_> {
        MatchmakerReady { matchmaker: self }
    }

    /// Every registration in `[gc_watermark, ballot)`, in ballot order.
    fn history_below(&self, ballot: Ballot) -> BTreeMap<Ballot, Registration> {
        self.registry
            .range(self.hard_state.gc_watermark..ballot)
            .map(|(b, c)| (*b, c.clone()))
            .collect()
    }

    /// The cross-field checker, called at boot and at every public mutating
    /// entry point: no registration below the watermark, and every
    /// registration a well-formed configuration.
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
                !registration.config.members.is_empty(),
                "a registered configuration names at least one acceptor"
            );
            assert!(
                registration.config.members.windows(2).all(|w| w[0] < w[1]),
                "a registered membership is sorted and deduplicated"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn fresh(id: u64) -> Matchmaker {
        Matchmaker::new(MatchmakerId(id), &TestRegistry::default())
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

    fn drain(mm: &mut Matchmaker) -> (Vec<MatchmakerWriteOp>, Vec<MatchReply>) {
        let ready = mm.ready();
        let writes = ready.writes().to_vec();
        let replies = ready.replies().to_vec();
        ready.advance();
        (writes, replies)
    }

    fn registered(reply: &MatchReply) -> (&BTreeMap<Ballot, Registration>, Ballot) {
        match &reply.outcome {
            MatchOutcome::Registered {
                history,
                gc_watermark,
            } => (history, *gc_watermark),
            MatchOutcome::Refused(r) => panic!("expected a registration, got {r:?}"),
        }
    }

    #[test]
    fn a_fresh_request_registers_and_reports_the_history_strictly_below() {
        let mut mm = fresh(0);
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(1, 1),
            config(&[0, 1, 2]),
        ));
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
        let (history, watermark) = registered(&replies[0]);
        assert!(history.is_empty(), "the first registration has no past");
        assert_eq!(watermark, Ballot::zero());

        mm.step(MatchRequest::new(
            NodeId(2),
            ballot(3, 2),
            config(&[1, 2, 3]),
        ));
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

    #[test]
    fn a_request_at_or_below_the_highest_is_refused_with_the_highest() {
        let mut mm = fresh(0);
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(5, 1),
            config(&[0, 1, 2]),
        ));
        drain(&mut mm);
        // Strictly below.
        mm.step(MatchRequest::new(
            NodeId(2),
            ballot(4, 9),
            config(&[0, 1, 2]),
        ));
        // Same round, lower node: below in the total order.
        mm.step(MatchRequest::new(
            NodeId(0),
            ballot(5, 0),
            config(&[0, 1, 2]),
        ));
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
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(1, 1),
            config(&[0, 1, 2]),
        ));
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(2, 1),
            config(&[0, 1, 2]),
        ));
        let (writes, replies) = drain(&mut mm);
        assert_eq!(writes.len(), 2);
        let (first_history, _) = registered(&replies[1]);
        let first_history = first_history.clone();

        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(2, 1),
            config(&[0, 1, 2]),
        ));
        let (writes, replies) = drain(&mut mm);
        assert!(writes.is_empty(), "a duplicate never re-registers");
        let (history, _) = registered(&replies[0]);
        assert_eq!(*history, first_history, "the re-answer is the first answer");
        assert_eq!(mm.registry().len(), 2);
    }

    #[test]
    fn a_registered_ballot_with_different_bytes_is_refused_write_once() {
        let mut mm = fresh(0);
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(1, 1),
            config(&[0, 1, 2]),
        ));
        drain(&mut mm);
        mm.step(MatchRequest::new(NodeId(1), ballot(1, 1), config(&[0, 1])));
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
            mm.step(MatchRequest::new(
                NodeId(1),
                ballot(round, 1),
                config(&[0, 1, 2]),
            ));
        }
        drain(&mut mm);
        assert!(mm.advance_gc_watermark(ballot(3, 1)));
        assert!(
            !mm.advance_gc_watermark(ballot(2, 1)),
            "the floor never lowers"
        );
        assert!(
            !mm.advance_gc_watermark(ballot(3, 1)),
            "re-raising is a no-op"
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

        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(2, 1),
            config(&[0, 1, 2]),
        ));
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(6, 1),
            config(&[0, 1, 2]),
        ));
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
            mm.step(MatchRequest::new(
                NodeId(1),
                ballot(round, 1),
                config(&[0, 1, 2]),
            ));
        }
        let (_, replies) = drain(&mut mm);
        let (first, watermark) = registered(&replies[2]);
        assert_eq!(
            first.keys().copied().collect::<Vec<_>>(),
            vec![ballot(1, 1), ballot(2, 1)]
        );
        assert_eq!(watermark, Ballot::zero());

        // The first reply is lost; GC moves the floor; the client retries.
        assert!(mm.advance_gc_watermark(ballot(2, 1)));
        drain(&mut mm);
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(3, 1),
            config(&[0, 1, 2]),
        ));
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
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(5, 1),
            config(&[0, 1, 2]),
        ));
        mm.step(MatchRequest::new(
            NodeId(2),
            ballot(5, 2),
            config(&[3, 4, 5]),
        ));
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
        mm.step(MatchRequest::new(
            NodeId(2),
            ballot(5, 2),
            config(&[3, 4, 5]),
        ));
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(5, 1),
            config(&[0, 1, 2]),
        ));
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
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(1, 1),
            config(&[0, 1, 2]),
        ));
        mm.step(MatchRequest::new(
            NodeId(2),
            ballot(2, 2),
            config(&[0, 1, 2, 3]),
        ));
        drain(&mut mm);
        // A reboot walks the durable records back through the port.
        let mut rebooted = Matchmaker::new(MatchmakerId(2), &TestRegistry::of(&mm));
        rebooted.step(MatchRequest::new(
            NodeId(1),
            ballot(3, 1),
            config(&[1, 2, 3]),
        ));
        mm.step(MatchRequest::new(
            NodeId(1),
            ballot(3, 1),
            config(&[1, 2, 3]),
        ));
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
        let _ = Matchmaker::new(MatchmakerId(0), &store);
    }

    #[test]
    fn a_configuration_is_normalized_and_sizes_its_quorum() {
        let config = AcceptorConfig::new(
            vec![NodeId(2), NodeId(0), NodeId(2), NodeId(1)],
            QuorumSystem::Majority,
        );
        assert_eq!(config.members, vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(config.quorum_size(), 2);
    }
}
