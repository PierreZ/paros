//! The **replica tier** (#144, Compartmentalized Paxos §3.3,
//! Compartmentalization 3): [`ReplicaNode`], a node that learns the chosen
//! log and never votes — since #186 a **read replica**: it serves journal
//! reads and applies nothing — and the coupling analysis that says what an
//! acceptor must keep beside its votes.
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
//! acceptors — adding a replica adds read capacity, never a vote.
//!
//! - **In:** `Commit` (from a leader or a proxy leader, the two alike), the
//!   `CatchUpResponse` its own requests draw, the `TrimmedTo` a peer serves
//!   when it asked below that peer's trim point, `Heartbeat` — for the
//!   commit watermark, the leader hint and, on a matchmaker deployment, the
//!   configuration in force, never the ballot — and the `PreReadAck`s its
//!   own quorum reads draw. Every other message is not a replica's to hear
//!   and is ignored.
//! - **Out:** `CatchUpRequest`, to the beat's sender when its watermark is
//!   ahead of this replica's prefix, and on the driver's tick while a faulty
//!   record is open; `PreRead`, to a row of acceptors, for a quorum read. A
//!   replica serves no peer: healing it is the acceptors' job, as healing a
//!   lagging acceptor is.
//! - **Reads:** the journal read ([`ReplicaNode::read_log`], #185) is served
//!   from this replica's own chosen prefix, so read load leaves the
//!   acceptors; the leaderless read (§3.4, the paper's own reader,
//!   [`ReplicaNode::quorum_read_in`]) runs the node's [`QuorumReads`] tally —
//!   a row's vote watermarks, the maximum — and [`ReplicaReady::read_states`]
//!   surfaces the read once *this replica's* prefix covers it. The reader is
//!   never an addressee of its own row (it votes nothing), and a read over a
//!   superseded configuration is abandoned when a beat names a newer one.
//! - **Durable:** the chosen log through the same record surface a node
//!   uses — [`WriteOp::Learned`] where a node writes an accepted record, the
//!   relaxed [`WriteOp::SetChosenIndex`] from the walk, and the floor-moving
//!   [`WriteOp::Truncate`] / [`WriteOp::TrimmedTo`] — never an
//!   [`WriteOp::Acceptor`] op. A boot scan reads it back through the same
//!   [`Storage`] port and [`Replica::from_boot`] rebuilds the prefix and
//!   the sealed journal state as it does on a node.
//! - **The walk:** [`ReplicaReady::committed`] in contiguous slot order, the
//!   slots the prefix just moved over — reported by the driver, handed to no
//!   application (#186: the client folds what it reads).
//! - **The reply:** [`Config::reply_owner`] names the one replica that owns
//!   a slot's client reply (§3.3, `slot % replica_count`). Nothing routes on
//!   it yet: the node a client asked keeps acking what it serves, until a
//!   client library that connects to replicas exists.
//!
//! # The couplings: what forces an acceptor to keep a chosen index?
//!
//! #144 introduced the *bare acceptor*, an acceptor that shed the
//! application and stayed a learner; #186 made every acceptor one, since
//! paros runs no application at all. That an acceptor must stay a learner
//! is a claim about the code, so here it is derived from the code, coupling
//! by coupling — every place `ColocatedNode` lets its acceptor half read the
//! replica half, or the other way round.
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
//!    it** (`serve_catchup`, `node/catch_up.rs`): the entries a
//!    lagging node — or a replica — learns are read from `chosen`, each
//!    with the choosing ballot its accepted record holds. The acceptors are
//!    the durable tier; a replica tier is healed *from* them.
//! 6. **The handoff's decided tail and the successor's fence**
//!    (`node/handoff.rs`) name chosen slots and a covered chosen index; a
//!    bare acceptor can lead, so it must be able to describe its tail.
//! 7. **The journal fold** (#204, `Replica::truncate`, the `state` of a
//!    `TrimmedTo`): the journal state machine is judged at apply by the
//!    walk, on every node and every replica alike; a truncation seals the
//!    state the slots it drops folded to and a trim-point jump hands it on.
//!    It is derived by the walk, never by an application, and it is what
//!    makes a read by position and a write's outcome identical everywhere.
//!
//! **What #144 shed and #186 deleted:** the application's input (the walk's
//! output, which survives only as the driver's report and the acks), the
//! application repair that re-emitted it, and the application snapshot a node
//! below the floor used to install — replaced by the bare `TrimmedTo` jump.
//! The GC rule stays the stronger one paros has (`node/gc.rs`, *Re-read
//! against a replica tier*).
//!
//! Hard `assert!`s throughout (AGENTS.md, *Assertion doctrine*).

use std::collections::{BTreeMap, BTreeSet};

use crate::ReadState;
use crate::journal_state::JournalState;
use crate::membership::{AcceptorConfig, ReplicaId};
use crate::message::{Audience, Message};
use crate::node::READ_TTL_TICKS;
use crate::quorum_read::QuorumReads;
use crate::replica::Replica;
use crate::state::Config;
use crate::storage::Storage;
use crate::types::{Ballot, Command, NodeId, Seq, Slot};
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
    /// Trim-point jumps taken (#186).
    pub trim_jumps: u64,
    /// Quorum reads opened here (#143 on a replica, §3.4).
    pub quorum_reads: u64,
    /// Messages that are not a replica's to hear (`Prepare`, `Accept`, …),
    /// or a beat from outside the pool.
    pub ignored: u64,
}

/// A replica that is not an acceptor: learns and serves reads, never votes. See the
/// module doc. Driven through the same `step` / `tick` → `ready` → `advance`
/// shape as every other handle.
#[derive(Clone, Debug)]
pub struct ReplicaNode {
    /// Who this replica is (outside the pool), the acceptors it pulls from
    /// before it has heard a leader, and the deployment's replica count.
    config: Config,
    /// The chosen prefix, the walk, the journal fold.
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
        let replica = Replica::from_boot(chosen_index, floor, storage.sealed_state(), &below);
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
    /// `TrimmedTo` and `Heartbeat`. Everything else is not a
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
            Message::TrimmedTo { point, state, .. } => self.trim_to(point, state),
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

    /// Advance logical time by one tick: while a faulty record is open, pull the decided range from its first missing
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
        if let Some(from_slot) = first_faulty {
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
        if !config.is_drawn_from(self.config.pool()) {
            return;
        }
        self.acceptors = config;
        self.acceptors_since = ballot;
        self.quorum_reads.abandon_superseded(ballot);
    }

    /// Fold an acceptor's watermark into the read at `ctx`: the node's
    /// guard, then the tally (which owns the row guard); `step` serves
    /// after.
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
            // A trim-point jump can leave the prefix below a slot already
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

    /// Walk the contiguous prefix and execute a decided truncation *after*
    /// the walk.
    fn advance(&mut self) {
        // Every faulty record here sits inside the chosen prefix (the boot
        // keeps only those): a hole in the journal fold, so the replica holds
        // the walk until it heals, exactly as a node's does.
        // The coupling a node asserts per applied slot — "the authoritative
        // record carries the applied command" — holds here by construction:
        // the record is `WriteOp::Learned` from the very command `learn`
        // handed the replica, and the boot scan reads the prefix back from
        // those records.
        let truncate_up_to = self.replica.advance(|_, _| true, &mut self.pending_writes);
        if let Some(up_to) = truncate_up_to {
            self.compact(up_to);
        }
    }

    /// Execute a decided truncation on this replica's log, as a node's
    /// `compact` does: clamped to what the journal fold lets go, sealing the
    /// state the dropped slots folded to.
    fn compact(&mut self, up_to: Slot) {
        let Some(target) = self.replica.compaction_target() else {
            return;
        };
        let highest_drop = up_to.min(target);
        let first = Slot(highest_drop.0 + 1).max(self.floor);
        if first <= self.floor {
            return;
        }
        let sealed = self.replica.truncate(first);
        self.pending_writes
            .push(WriteOp::Truncate { first, sealed });
        self.floor = first;
        self.faulty = self.faulty.split_off(&first);
        assert!(
            self.floor <= self.replica.first_unchosen(),
            "a replica's compaction never drops an undecided slot"
        );
    }

    /// Jump below a peer's trim point (#186): the node's
    /// `on_trimmed_to`, for a replica — drop what lies below `point`, move
    /// the chosen index to at least `point - 1`, raise the floor to `point`,
    /// take the journal state the peer's log folded to below it. A replica
    /// holds no promise, so there is nothing else to leave alone.
    fn trim_to(&mut self, point: Slot, state: JournalState) {
        if point <= self.floor {
            return;
        }
        let old_chosen_index = self.replica.chosen_index();
        let sealed = self.replica.trim_to(point, state);
        self.floor = point;
        self.faulty = self.faulty.split_off(&point);
        self.pending_writes.push(WriteOp::TrimmedTo {
            point,
            state: sealed,
        });
        self.counters.trim_jumps += 1;
        assert!(
            self.replica.chosen_index() >= old_chosen_index,
            "a replica's trim-point jump never rewinds its chosen index"
        );
        assert!(
            self.floor <= self.replica.first_unchosen(),
            "a replica's trim-point jump keeps its floor inside the prefix"
        );
        // A `Commit` learned out of order may sit just above the point.
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

    /// The replica role: the chosen prefix, the journal fold, the chosen gap.
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

    /// A **journal read** (#204) from this replica's journal fold — read
    /// replicas serve `Read` so read load leaves the acceptors. A pure read.
    #[must_use]
    pub fn read_log(&self, from: Seq, limit: usize, max_bytes: usize) -> crate::LogRead {
        self.replica.read(from, limit, max_bytes)
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
/// [`ReplicaReady::messages`], report [`ReplicaReady::committed`], then
/// [`ReplicaReady::advance`].
#[must_use = "a ReplicaReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct ReplicaReady<'a> {
    node: &'a mut ReplicaNode,
}

impl ReplicaReady<'_> {
    /// The durable writes to persist first, in order: [`WriteOp::Learned`],
    /// [`WriteOp::SetChosenIndex`], [`WriteOp::Truncate`],
    /// [`WriteOp::TrimmedTo`] — never an acceptor op.
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

    /// The `(slot, command)` pairs the prefix just walked over, in
    /// contiguous slot order, after the writes are durable — the node's
    /// [`crate::Ready::committed`] contract.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Command, crate::Outcome)] {
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
