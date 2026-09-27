//! The **replica tier** (#144, Compartmentalized Paxos §3.3,
//! Compartmentalization 3): [`ReplicaNode`], a node that learns and applies
//! the chosen log and never votes, and — for its mirror, the bare acceptor
//! that votes and applies nothing — the coupling analysis that says what an
//! acceptor must keep when it sheds the application.
//!
//! # `ReplicaNode`: the third deployment
//!
//! After the proxy leader (#142), the second process that runs one role
//! without the rest: a [`Replica`] over a durable chosen log, and nothing
//! else. The `Replica` role already consumed "slot `s` chose `v`" and
//! nothing more (a removed member or a spare heals exactly this way), so
//! what is new here is a node with **no [`Acceptor`](crate::acceptor::Acceptor)
//! at all**: it answers no `Prepare` and no `Accept`, is never a member of a
//! configuration, never sits in `Config::peers` or the node pool, acks no
//! beat and counts toward no quorum. It scales independently of the
//! acceptors — adding a replica adds application throughput and read
//! capacity, never a vote.
//!
//! - **In:** `Commit` (from a leader or a proxy leader, the two alike), the
//!   `CatchUpResponse` its own requests draw, the `InstallSnapshot` a peer
//!   serves when it asked below that peer's floor, and `Heartbeat` — for
//!   the commit watermark and the leader hint only. Every other message is
//!   not a replica's to hear and is ignored.
//! - **Out:** `CatchUpRequest`, to the beat's sender when its watermark is
//!   ahead of this replica's prefix, and on the driver's tick while an
//!   application repair or a faulty record is open. A replica serves nobody:
//!   healing it is the acceptors' job, as healing a lagging acceptor is.
//! - **Durable:** the chosen log through the same record surface a node
//!   uses — [`WriteOp::Learned`] where a node writes an accepted record, the
//!   relaxed [`WriteOp::SetChosenIndex`] from the walk, and the floor-moving
//!   [`WriteOp::Truncate`] / [`WriteOp::InstallSnapshot`] — never an
//!   [`WriteOp::Acceptor`] op. A boot scan reads it back through the same
//!   [`Storage`] port and [`Replica::from_boot`] rebuilds the prefix and
//!   the at-most-once ledger as it does on a node. The durable promise a
//!   snapshot install's ballot may leave in the store is never read: a
//!   replica has no promise to keep.
//! - **The application:** exactly the node's contract —
//!   [`ReplicaReady::committed`] in contiguous slot order, and
//!   [`ReplicaNode::open_app_repair`] when the driver's boot replay could not
//!   walk the whole prefix.
//! - **The reply:** [`Config::reply_owner`] names the one replica that owns
//!   a slot's client reply (§3.3, `slot % replica_count`). Nothing routes on
//!   it yet: the node a client asked keeps acking what it serves, until a
//!   client library that connects to replicas exists.
//!
//! # The couplings: what forces an acceptor to keep a chosen index?
//!
//! The bare acceptor ([`Application::Shed`](crate::Application::Shed)) was
//! decided on the condition that an acceptor stays a learner and sheds only
//! the application. That is a claim about the code, so here it is derived
//! from the code, coupling by coupling — every place `ColocatedNode` lets its
//! acceptor half read the replica half, or the other way round.
//!
//! **Load-bearing — an acceptor must keep them, so a bare acceptor keeps a
//! chosen index and a chosen prefix:**
//!
//! 1. **A chosen value becomes the authoritative record before the chosen
//!    index advances** (`mark_chosen`, `node/learn.rs`). This is the P2c
//!    chain the boot rebuild trusts ([`Replica::from_boot`] reads the chosen
//!    prefix out of the accepted records) and the upsert that overwrites a
//!    stale lower-ballot accept. The acceptor only knows *which* record to
//!    overwrite because the learner half told it the slot is chosen.
//! 2. **The compaction floor moves only inside the chosen prefix.** A
//!    decided `Truncate` is executed when the contiguous walk *applies* it
//!    (`ColocatedNode::compact`, "compaction never drops an undecided
//!    slot"), and the acceptor's below-floor `Nack` of a `Prepare` is sound
//!    only because every slot below the floor is chosen — the paper's
//!    acceptor-side persisted watermark. Without a chosen index an acceptor
//!    could never truncate, and its log would grow without bound.
//! 3. **The CTRL repair probe and the election recovery skip chosen slots**
//!    (`close_phase1(|slot| replica.is_chosen(slot))` and the recovery
//!    pump, `node/election.rs`): a faulty-reported slot already chosen
//!    re-replicates through commit and catch-up instead of blocking the
//!    leadership, and a recovered slot chosen underneath a page is not
//!    re-proposed.
//! 4. **The GC fence counts chosen indices** (`HeartbeatAck.chosen`,
//!    `node/gc.rs`): the condition that lets a configuration be forgotten
//!    is a Phase-2 quorum of `C_b` whose chosen index covers the election
//!    fence. An acceptor without one could never let GC complete.
//! 5. **Catch-up is served from the chosen prefix and the records beside
//!    it** (`serve_catchup`, `node/catch_up_snapshot.rs`): the entries a
//!    lagging node — or a replica — learns are read from `chosen`, each
//!    with the choosing ballot its accepted record holds. The acceptors are
//!    the durable tier; a replica tier is healed *from* them.
//! 6. **The handoff's decided tail and the successor's read fence**
//!    (`node/handoff.rs`) name chosen slots and a covered chosen index; a
//!    bare acceptor can lead, so it must be able to describe its tail.
//! 7. **The at-most-once ledger** (`Replica::seal`, the `sessions` of an
//!    `InstallSnapshot`): a truncation seals the `(client, seq) -> slot`
//!    facts it drops and a snapshot install hands them on. A bare acceptor
//!    applies nothing, but it truncates and it serves snapshots to replicas
//!    that do, so the ledger — derived by the walk, not by the application —
//!    stays. So does the fast path in `propose` that answers an identity
//!    already chosen at its first slot: it reads the ledger, not
//!    application state.
//!
//! **Reflecting only the colocation — a bare acceptor sheds them:**
//!
//! - **The `committed` output** of the walk ([`crate::Ready::committed`]):
//!   the application's input, nothing else reads it. A bare acceptor's is
//!   always empty; the walk still runs and still writes the chosen index.
//! - **The application repair** (`open_app_repair`): it re-emits
//!   `committed` for an application whose durable prefix is behind the
//!   chosen index. No application, nothing to repair; opening one on a bare
//!   acceptor is a programmer error.
//! - **The application snapshot.** A bare acceptor below the floor installs
//!   a peer's snapshot boundary into its *log* (the floor, the chosen index,
//!   the sealed ledger, the opaque bytes it keeps in custody) and restores
//!   no application from it. Which bytes it serves onward is the driver's
//!   (the custody it holds); the core only records the offer, as on any
//!   node.
//! - **The answer to a read.** A read-index or quorum read on a bare
//!   acceptor is still certified by the core — the certificate is a
//!   statement about the chosen prefix — but the state that answers it lives
//!   on a replica.
//!
//! So a bare acceptor is a `ColocatedNode` constructed with
//! `Application::Shed`: one code path for every learner line, and three
//! outputs it never produces. The GC rule stays the stronger one paros has
//! (`node/gc.rs`, *Re-read against a replica tier*).
//!
//! Hard `assert!`s throughout (AGENTS.md, *Assertion doctrine*).

use std::collections::{BTreeMap, BTreeSet};

use crate::ReadState;
use crate::membership::{AcceptorConfig, ReplicaId};
use crate::message::{Audience, Message};
use crate::node::READ_TTL_TICKS;
use crate::quorum_read::QuorumReads;
use crate::replica::Replica;
use crate::state::Config;
use crate::storage::Storage;
use crate::types::{Ballot, Command, NodeId, SessionEntry, Slot, Value};
use crate::write::WriteOp;

/// Monotone counters this incarnation, for the driver's audit report and
/// the examples: what a replica did, never what it decided.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicaCounters {
    /// Slots learned chosen for the first time here (a `Commit`, a
    /// catch-up replay).
    pub learned: u64,
    /// Learned slots that were already known with the same value.
    pub relearned: u64,
    /// `CatchUpRequest`s sent.
    pub catch_up_requests: u64,
    /// Snapshot boundaries installed.
    pub snapshots_installed: u64,
    /// Quorum reads opened here (#143 on a replica, §3.4).
    pub quorum_reads: u64,
    /// Messages that are not a replica's to hear (`Prepare`, `Accept`, …),
    /// or a beat from outside the pool.
    pub ignored: u64,
}

/// A replica that is not an acceptor: learns, applies, never votes. See the
/// module doc. Driven through the same `step` / `tick` → `ready` → `advance`
/// shape as every other handle.
#[derive(Clone, Debug)]
pub struct ReplicaNode {
    /// Who this replica is (outside the pool), the acceptors it pulls from
    /// before it has heard a leader, and the deployment's replica count.
    config: Config,
    /// The chosen prefix, the apply walk, the ledger, the repair cursor.
    replica: Replica,
    /// The compaction floor: the first slot whose record is still retained.
    floor: Slot,
    /// Retained chosen slots whose durable record the boot scan found
    /// unreadable (value lost, identity kept): pulled from the acceptors on
    /// the tick until a catch-up replay heals them.
    faulty: BTreeSet<Slot>,
    /// The node whose beat this replica heard last — where it pulls from.
    leader: Option<NodeId>,
    /// The acceptor configuration this replica believes in force — the one
    /// a quorum read asks a row of. The bootstrap membership, then whatever
    /// a beat carries on a matchmaker deployment (the follower's rule).
    acceptors: AcceptorConfig,
    /// The ballot `acceptors` is bound to here.
    acceptors_since: Ballot,
    /// The open quorum reads (§3.4): a row's watermarks, then this
    /// replica's own applied prefix.
    quorum_reads: QuorumReads<NodeId>,
    /// Logical time, for the reads' TTL.
    tick_count: u64,
    pending_writes: Vec<WriteOp>,
    pending_messages: Vec<(Audience, Message)>,
    pending_read_states: Vec<ReadState>,
    counters: ReplicaCounters,
}

impl ReplicaNode {
    /// Construct from a read-only [`Storage`] by reading the durable chosen
    /// log back in. Bootstrap and restart share this path, as on a node.
    ///
    /// Every record a replica holds is chosen — it writes nothing else — so
    /// the records above the durable chosen index (a `Commit` learned out of
    /// order) are learned again rather than treated as in flight, and the
    /// walk runs once: the first batch may carry the prefix they complete.
    ///
    /// # Panics
    ///
    /// If the configuration is malformed (an unsorted or duplicated pool, a
    /// bootstrap membership outside it, or a replica id *inside* it — a
    /// replica is never a member of anything) or the durable state breaks
    /// the write ordering (a floor past the chosen prefix, a hole in the
    /// retained chosen prefix).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all))]
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
        assert_replica_config_shape(&config);
        let floor = storage.first_slot();
        let chosen_index = hard_state.chosen_index;
        let first_unchosen = chosen_index.map_or(Slot(0), |ci| Slot(ci.0 + 1));
        assert!(
            floor <= first_unchosen,
            "the durable floor never outruns the durable chosen index"
        );
        let mut below: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let mut above: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        for s in floor.0..=storage.last_slot().0 {
            if let Some(record) = storage.accepted(Slot(s)) {
                if Slot(s) < first_unchosen {
                    below.insert(Slot(s), record);
                } else {
                    above.insert(Slot(s), record);
                }
            }
        }
        let faulty: BTreeSet<Slot> = storage
            .faulty_entries()
            .into_iter()
            .map(|(slot, _)| slot)
            .filter(|slot| *slot >= floor && *slot < first_unchosen)
            .collect();
        // The node's completeness check, restated for a replica: every
        // retained slot below the chosen prefix reads back as a record —
        // readable, or faulty with its identity — so the prefix a restart
        // resumes from has no silent hole.
        for s in floor.0..first_unchosen.0 {
            assert!(
                below.contains_key(&Slot(s)) || faulty.contains(&Slot(s)),
                "every retained slot below a replica's chosen prefix has a durable record"
            );
        }
        let replica = Replica::from_boot(chosen_index, storage.sealed_sessions(), &below);
        let acceptors = AcceptorConfig::new(config.peers.clone(), config.quorum_system);
        let mut node = Self {
            config,
            replica,
            floor,
            faulty,
            leader: None,
            acceptors,
            acceptors_since: Ballot::zero(),
            quorum_reads: QuorumReads::new(),
            tick_count: 0,
            pending_writes: Vec::new(),
            pending_messages: Vec::new(),
            pending_read_states: Vec::new(),
            counters: ReplicaCounters::default(),
        };
        for (slot, (_, command)) in above {
            node.replica.learn(slot, &command);
        }
        node.advance();
        node.assert_invariants();
        node
    }

    // ---- inputs ---------------------------------------------------------------

    /// The single wire entry point: `Commit`, `CatchUpResponse`,
    /// `InstallSnapshot` and `Heartbeat`. Everything else is not a
    /// replica's to hear and is ignored — in particular a `Prepare` or an
    /// `Accept`, which a replica never answers.
    ///
    /// # Panics
    ///
    /// If a slot already chosen here is relearned with a different value
    /// (two values chosen for one slot, caught where it lands), or an
    /// internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(replica = self.config.id.0)))]
    pub fn step(&mut self, msg: Message) {
        match msg {
            Message::Commit {
                ballot,
                slot,
                command,
                ..
            } => self.learn(slot, ballot, &command),
            Message::CatchUpResponse { entries, .. } => {
                for (slot, (ballot, command)) in entries {
                    self.learn(slot, ballot, &command);
                }
            }
            Message::InstallSnapshot {
                ballot,
                chosen_index,
                snapshot,
                sessions,
                ..
            } => self.install(ballot, chosen_index, snapshot, sessions),
            Message::Heartbeat {
                from,
                ballot,
                commit,
                config,
                ..
            } => self.on_heartbeat(from, ballot, commit, config),
            Message::PreReadAck {
                from,
                ctx,
                watermark,
                config_since,
            } => self.on_pre_read_ack(from, ctx, watermark, config_since),
            _ => self.counters.ignored += 1,
        }
        self.serve_quorum_reads();
        self.assert_invariants();
    }

    /// Advance logical time by one tick: while an application repair or a
    /// faulty record is open, pull the decided range from its first missing
    /// slot — from the leader heard last, or from every bootstrap acceptor
    /// when none was. The same once-per-tick cadence a node's repair pull
    /// uses; the heartbeat drives every other catch-up.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(replica = self.config.id.0)))]
    pub fn tick(&mut self) {
        self.tick_count += 1;
        self.quorum_reads.expire(self.tick_count, READ_TTL_TICKS);
        self.serve_quorum_reads();
        let first_faulty = self.faulty.first().copied();
        if let Some(from_slot) = self.replica.app_repair().or(first_faulty) {
            let targets: Vec<NodeId> = match self.leader {
                Some(leader) => vec![leader],
                None => self.config.peers.clone(),
            };
            for to in targets {
                self.request_catch_up(to, from_slot);
            }
        }
        self.assert_invariants();
    }

    /// Open an **application repair**, exactly as on a node
    /// ([`crate::ColocatedNode::open_app_repair`]): the driver's boot replay
    /// could not walk the whole chosen prefix, and the application's durable
    /// prefix stops just below `from`. The replica re-emits every decided
    /// command from `from` through [`ReplicaReady::committed`] as the values
    /// arrive, and pulls them on the tick.
    ///
    /// # Panics
    ///
    /// If `from` lies past the contiguous chosen prefix, or an internal
    /// invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(replica = self.config.id.0, from = from.0)))]
    pub fn open_app_repair(&mut self, from: Slot) {
        self.replica.open_app_repair(from);
        self.replica.pump_app_repair(self.floor);
        self.assert_invariants();
    }

    /// **Leaderless read on a replica** (Compartmentalized Paxos §3.4, the
    /// paper's own shape): ask a row of the acceptor configuration this
    /// replica believes in force for their vote watermarks, settle on the
    /// maximum once the row answered whole, and surface a [`ReadState`]
    /// carrying `ctx` through [`ReplicaReady::read_states`] once *this
    /// replica's* applied prefix covers it — the state that answers the read
    /// is this replica's own. The same tally and the same safety argument as
    /// [`crate::ColocatedNode::quorum_read_in`] (see [`crate::quorum_read`]);
    /// what differs is only that the reader is never an addressee of its own
    /// row, since a replica votes nothing. `row` is the driver's choice, as
    /// on a node; a row the configuration lacks falls back to the default.
    ///
    /// # Panics
    ///
    /// If a read is already open at `ctx` (a driver token is unique), or an
    /// internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(replica = self.config.id.0, ctx)))]
    pub fn quorum_read_in(&mut self, ctx: u64, row: Option<usize>) {
        let row = self.acceptors.read_row(ctx, row);
        let addressees = self.quorum_reads.open(
            ctx,
            row,
            self.acceptors.clone(),
            self.acceptors_since,
            self.tick_count,
            None,
        );
        self.counters.quorum_reads += 1;
        for to in addressees {
            self.pending_messages.push((
                Audience::Node(to),
                Message::PreRead {
                    reply_to: self.config.id,
                    ctx,
                },
            ));
        }
        self.assert_invariants();
    }

    /// Release the next bounded page of the apply walk after the caller has
    /// fully processed the previous batch — the node's
    /// [`crate::ColocatedNode::advance_recovery`], for the one continuation a
    /// replica has. A no-op when the walk is not deferred.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(replica = self.config.id.0)))]
    pub fn advance_recovery(&mut self) {
        self.advance();
        self.assert_invariants();
    }

    /// Borrow the replica to drain one batch. The returned [`ReplicaReady`]
    /// holds the unique `&mut` borrow, so a second `ready()` before
    /// [`ReplicaReady::advance`] is a **compile error**, as on the node.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(replica = self.config.id.0)))]
    pub fn ready(&mut self) -> ReplicaReady<'_> {
        ReplicaReady { node: self }
    }

    // ---- the learner ----------------------------------------------------------

    /// A leader's beat: follow its watermark, never its ballot. A replica
    /// promises nothing, so it acks nothing — its answer to a beat is a
    /// catch-up request when the beat's prefix is ahead of its own. On a
    /// matchmaker deployment the beat also carries the configuration the
    /// leader's ballot runs with, which the replica adopts by the follower's
    /// rule (strictly newer ballot) for the quorum reads it opens.
    fn on_heartbeat(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        commit: Option<Slot>,
        config: Option<AcceptorConfig>,
    ) {
        // Wire hygiene, as on a node: only a node of the pool is followed.
        if !self.in_pool(from) {
            self.counters.ignored += 1;
            return;
        }
        self.leader = Some(from);
        self.learn_config(ballot, config);
        if commit > self.replica.chosen_index() {
            self.request_catch_up(from, self.replica.first_unchosen());
        }
    }

    /// Adopt `config` when `ballot` is above the one the current belief is
    /// bound to, on a matchmaker deployment only (a plain beat carries none),
    /// and abandon every read opened against the superseded one — the row
    /// asked need not intersect the successor's columns.
    fn learn_config(&mut self, ballot: Ballot, config: Option<AcceptorConfig>) {
        let Some(config) = config else {
            return;
        };
        if !self.config.has_matchmakers() || ballot <= self.acceptors_since {
            return;
        }
        if !config.members().iter().all(|m| self.in_pool(*m)) {
            return;
        }
        self.acceptors = config;
        self.acceptors_since = ballot;
        self.quorum_reads.abandon_superseded(ballot);
    }

    /// Fold an acceptor's watermark into the read at `ctx`: the node's
    /// guards (`on_pre_read_ack`), then the tally; `step` serves after.
    fn on_pre_read_ack(
        &mut self,
        from: NodeId,
        ctx: u64,
        watermark: Option<Slot>,
        config_since: Option<Ballot>,
    ) {
        if !self.in_pool(from) {
            self.counters.ignored += 1;
            return;
        }
        let Some(read) = self.quorum_reads.get(ctx) else {
            return;
        };
        if !read.config().is_phase1_addressee(from, read.row()) {
            return;
        }
        // What the fold did matters only through the serve `step` runs next.
        let _ = self.quorum_reads.fold(ctx, from, watermark, config_since);
    }

    /// Serve every confirmed read this replica's applied prefix covers.
    fn serve_quorum_reads(&mut self) {
        let replica = &self.replica;
        let served = self.quorum_reads.serve(|index| replica.covers(index));
        self.pending_read_states.extend(
            served
                .into_iter()
                .map(|(ctx, index)| ReadState { ctx, index }),
        );
    }

    fn in_pool(&self, id: NodeId) -> bool {
        self.config.pool().binary_search(&id).is_ok()
    }

    fn request_catch_up(&mut self, to: NodeId, from_slot: Slot) {
        self.counters.catch_up_requests += 1;
        self.pending_messages.push((
            Audience::Node(to),
            Message::CatchUpRequest {
                from: self.config.id,
                from_slot,
            },
        ));
    }

    /// Learn `slot` chosen at `ballot` with `command`: persist the record,
    /// hand the fact to the replica, walk the prefix. Idempotent.
    fn learn(&mut self, slot: Slot, ballot: Ballot, command: &Command) {
        // Chosen, applied and truncated here: its effect is in the
        // application's state and its record is gone on purpose.
        if slot < self.floor {
            return;
        }
        if let Some(known) = self.replica.chosen_at(slot) {
            assert!(
                known == command,
                "a slot already chosen at a replica is relearned with the same value"
            );
            self.counters.relearned += 1;
            // A snapshot install can leave the prefix below a slot already
            // known, so a replay of that slot re-drives the walk, as on a
            // node.
            self.advance();
            return;
        }
        self.pending_writes.push(WriteOp::Learned {
            slot,
            ballot,
            command: command.clone(),
        });
        self.replica.learn(slot, command);
        self.faulty.remove(&slot);
        self.counters.learned += 1;
        self.advance();
    }

    /// Walk the contiguous prefix, execute a decided truncation *after* the
    /// walk, and pump an open application repair.
    fn advance(&mut self) {
        // The coupling a node asserts per applied slot — "the authoritative
        // record carries the applied command" — holds here by construction:
        // the record is `WriteOp::Learned` from the very command `learn`
        // handed the replica, and the boot scan reads the prefix back from
        // those records.
        let truncate_up_to = self.replica.advance(|_, _| true, &mut self.pending_writes);
        if let Some(up_to) = truncate_up_to {
            self.compact(up_to);
        }
        self.replica.pump_app_repair(self.floor);
    }

    /// Execute a decided `Truncate { up_to }` on this replica's log, as a
    /// node's `compact` does: clamped to the chosen prefix and below an open
    /// application repair's cursor, sealing the ledger records it drops.
    fn compact(&mut self, up_to: Slot) {
        let Some(ci) = self.replica.chosen_index() else {
            return;
        };
        let mut highest_drop = up_to.min(ci);
        if let Some(cursor) = self.replica.app_repair() {
            let Some(cap) = cursor.0.checked_sub(1) else {
                return;
            };
            highest_drop = highest_drop.min(Slot(cap));
        }
        let first = Slot(highest_drop.0 + 1).max(self.floor);
        if first <= self.floor {
            return;
        }
        let sealed: Vec<SessionEntry> = self.replica.seal(self.floor, first);
        self.pending_writes
            .push(WriteOp::Truncate { first, sealed });
        self.replica.truncate(first);
        self.floor = first;
        self.faulty = self.faulty.split_off(&first);
        assert!(
            self.floor <= self.replica.first_unchosen(),
            "a replica's compaction never drops an undecided slot"
        );
    }

    /// Install a peer's snapshot boundary: the node's guards
    /// (`on_install_snapshot`), without the promise — a replica adopts no
    /// ballot. The `ballot` is persisted with the install because the store's
    /// install op carries it; nothing here reads it back.
    fn install(
        &mut self,
        ballot: Ballot,
        chosen_index: Slot,
        snapshot: Value,
        mut sessions: Vec<SessionEntry>,
    ) {
        // Wire guard: a boundary with no floor one past it.
        if chosen_index.0 == u64::MAX {
            return;
        }
        // Never go backward; a snapshot *at* the prefix heals only an open
        // application repair (the node's Stage 8 exception).
        if let Some(ci) = self.replica.chosen_index() {
            if chosen_index < ci {
                return;
            }
            if chosen_index == ci && self.replica.app_repair().is_none() {
                return;
            }
        }
        // The boundary is the validation line for the ledger it carries.
        sessions.retain(|(_, _, slot)| *slot <= chosen_index);
        let old_floor = self.floor;
        self.replica.install(chosen_index, &sessions);
        self.floor = Slot(chosen_index.0 + 1);
        self.faulty = self.faulty.split_off(&self.floor);
        self.pending_writes.push(WriteOp::InstallSnapshot {
            chosen_index,
            ballot,
            snapshot,
            sessions,
        });
        self.counters.snapshots_installed += 1;
        assert!(
            self.floor >= old_floor,
            "a replica's snapshot install never lowers its floor"
        );
        assert!(
            self.floor == self.replica.first_unchosen(),
            "a replica's snapshot install raises the floor to its boundary"
        );
        // A `Commit` learned out of order may sit just above the boundary.
        self.advance();
    }

    // ---- invariants and accessors ---------------------------------------------

    /// The replica node's own cross-field invariants, plus the replica
    /// role's against its floor.
    ///
    /// # Panics
    ///
    /// Panics when an invariant is broken: a programmer error, never an
    /// operating condition.
    pub fn assert_invariants(&self) {
        assert!(
            self.floor <= self.replica.first_unchosen(),
            "a replica's floor never outruns its chosen prefix"
        );
        self.replica.assert_invariants(self.floor);
        assert!(
            self.faulty.first().is_none_or(|s| *s >= self.floor),
            "no faulty record survives below a replica's floor"
        );
        assert!(
            self.faulty.iter().all(|s| !self.replica.is_chosen(*s)),
            "a faulty record is one whose chosen value this replica cannot read"
        );
        // The negative space of "never votes": nothing this replica emits
        // is an acceptor's write or a vote.
        assert!(
            !self
                .pending_writes
                .iter()
                .any(|w| matches!(w, WriteOp::Acceptor(_))),
            "a replica never emits an acceptor write"
        );
        assert!(
            self.pending_messages.iter().all(|(_, m)| matches!(
                m,
                Message::CatchUpRequest { .. } | Message::PreRead { .. }
            )),
            "a replica sends nothing but catch-up requests and pre-reads"
        );
        assert!(
            self.quorum_reads
                .pending()
                .iter()
                .all(|r| r.watermarks().keys().all(|id| self.in_pool(*id))),
            "a replica folds no watermark from outside the pool"
        );
    }

    /// The configuration this replica booted with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The replica role: the chosen prefix, the ledger, the chosen gap.
    #[must_use]
    pub fn replica(&self) -> &Replica {
        &self.replica
    }

    /// The acceptor configuration this replica believes in force — the one
    /// its quorum reads ask a row of.
    #[must_use]
    pub fn acceptors(&self) -> &AcceptorConfig {
        &self.acceptors
    }

    /// The compaction floor: the first slot whose record is retained.
    #[must_use]
    pub fn first_slot(&self) -> Slot {
        self.floor
    }

    /// The node whose beat this replica heard last, if any.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// The replica that owns `slot`'s client reply
    /// ([`Config::reply_owner`]).
    #[must_use]
    pub fn reply_owner(&self, slot: Slot) -> Option<ReplicaId> {
        self.config.reply_owner(slot)
    }

    /// Monotone counters this incarnation.
    #[must_use]
    pub fn counters(&self) -> ReplicaCounters {
        self.counters
    }

    fn clear_pending(&mut self) {
        self.pending_writes.clear();
        self.pending_messages.clear();
        self.pending_read_states.clear();
        self.replica.clear_committed();
    }
}

/// The replica's configuration shape, asserted once at boot: the node's
/// rules for the pool and the bootstrap membership, and the one rule that
/// makes it a replica — its id is **outside** the pool.
///
/// # Panics
///
/// If the pool or the bootstrap membership is empty, unsorted or
/// duplicated, if the membership leaves the pool, or if the pool names this
/// replica.
fn assert_replica_config_shape(config: &Config) {
    assert!(
        !config.peers.is_empty(),
        "a replica knows at least one acceptor to learn from"
    );
    assert!(
        config.peers.windows(2).all(|w| w[0] < w[1]),
        "membership is sorted and deduplicated"
    );
    assert!(
        config.nodes.windows(2).all(|w| w[0] < w[1]),
        "the node pool is sorted and deduplicated"
    );
    assert!(
        config
            .peers
            .iter()
            .all(|p| config.pool().binary_search(p).is_ok()),
        "the bootstrap membership is drawn from the node pool"
    );
    assert!(
        config.pool().binary_search(&config.id).is_err(),
        "a replica is never in the node pool"
    );
}

/// One batch of a replica's work, and the compile-time gate that enforces
/// one batch in flight — the replica's [`crate::Ready`]. Process it in the
/// node's order: persist [`ReplicaReady::writes`], send
/// [`ReplicaReady::messages`], apply [`ReplicaReady::committed`], then
/// [`ReplicaReady::advance`].
#[must_use = "a ReplicaReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct ReplicaReady<'a> {
    node: &'a mut ReplicaNode,
}

impl ReplicaReady<'_> {
    /// The durable writes to persist first, in order: [`WriteOp::Learned`],
    /// [`WriteOp::SetChosenIndex`], [`WriteOp::Truncate`],
    /// [`WriteOp::InstallSnapshot`] — never an acceptor op.
    #[must_use]
    pub fn writes(&self) -> &[WriteOp] {
        &self.node.pending_writes
    }

    /// Outbound messages: catch-up requests and quorum-read `PreRead`s,
    /// each to one node.
    #[must_use]
    pub fn messages(&self) -> &[(Audience, Message)] {
        &self.node.pending_messages
    }

    /// Quorum reads this replica can now answer from its own state: the
    /// row confirmed the index and this replica applied at or past it. The
    /// node's [`crate::Ready::read_states`] contract — answer each one once
    /// the batch's `committed` is applied.
    #[must_use]
    pub fn read_states(&self) -> &[ReadState] {
        &self.node.pending_read_states
    }

    /// Newly chosen `(slot, command)` pairs to apply, in contiguous slot
    /// order, after the writes are durable — the node's
    /// [`crate::Ready::committed`] contract.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Command)] {
        self.node.replica.committed()
    }

    /// Acknowledge the batch: clears every bucket and releases the borrow.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(replica = self.node.config.id.0)))]
    pub fn advance(self) {
        self.node.clear_pending();
    }
}

#[cfg(test)]
mod tests;
