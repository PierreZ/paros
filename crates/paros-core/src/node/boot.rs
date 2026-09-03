//! The node's **boot wiring**: the one path that turns durable state back
//! into a [`RawNode`]. Bootstrap and restart share it, so a rebooted node
//! answers exactly as it would have without the crash.
//!
//! Three steps, each with its own trust-boundary asserts: the configuration's
//! shape, the read-back of the durable log into the acceptor's working state,
//! and the volatile state every incarnation starts from — a Follower with no
//! leadership, whose allocator and dedup tables are *derived* from what was
//! read back rather than persisted beside it.

use super::{
    Acceptor, BTreeMap, Ballot, Command, Config, HEARTBEAT_TICKS, HandoffCounters,
    LeadershipOrigin, NodeRole, Proposer, RawNode, Replica, Slot, Storage,
};
use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerSet};

impl RawNode {
    /// Construct from a read-only [`Storage`] by reading durable state back in.
    /// Bootstrap and restart share this path. The volatile dedup tables
    /// (`applied_seq`, `inflight`) and the `chosen` map are rebuilt from the
    /// durable `accepted` log and `chosen_index`.
    ///
    /// # Panics
    ///
    /// If the configuration is malformed (membership not sorted/deduplicated,
    /// or missing this node's own id) or the durable state violates the write
    /// ordering contract (a floor past the chosen prefix). A broken invariant
    /// here means corrupted storage or a broken storage implementation;
    /// crashing beats running on it.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all))]
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
        assert_config_shape(&config);
        let acceptors = AcceptorConfig::new(config.peers.clone(), config.quorum_system);
        let matchmakers = MatchmakerSet::new(MatchmakerGeneration(0), config.matchmakers.clone());
        assert!(
            config
                .matchmakers
                .iter()
                .all(|m| config.matchmaker_pool().binary_search(m).is_ok()),
            "the bootstrap matchmaker set is drawn from the matchmaker pool"
        );
        let ballot = hard_state.max_promised_ballot;

        let (first_slot, accepted, faulty) = read_back_log(storage, hard_state.max_promised_ballot);
        let replica = Replica::from_boot(
            hard_state.chosen_index,
            storage.sealed_sessions(),
            &accepted,
        );

        // Next free slot: one past the highest accepted entry (readable or
        // faulty), or (when the log is empty, e.g. fully truncated) one past
        // the durable chosen index.
        let first_unchosen = hard_state.chosen_index.map_or(Slot(0), |ci| Slot(ci.0 + 1));
        let next_slot = accepted
            .keys()
            .chain(faulty.keys())
            .max()
            .map_or(first_unchosen, |s| Slot(s.0 + 1))
            .max(first_unchosen);
        // Trust-boundary re-assertion of the durable write ordering: a flushed
        // floor never outruns the flushed chosen index (only chosen slots are
        // truncated), and a durable chosen index implies its accept was flushed
        // in the same or an earlier sync, so the rebuilt `next_slot` sits at or
        // past the first unchosen slot.
        assert!(
            first_slot <= first_unchosen,
            "the durable floor never outruns the durable chosen index"
        );
        assert!(
            next_slot >= first_unchosen,
            "the rebuilt next slot never falls inside the chosen prefix"
        );
        // Completeness of the retained chosen prefix: every slot between the
        // floor and the first unchosen slot must read back as *some* durable
        // record — readable, or faulty-with-identity. A silent hole (record
        // fully lost, identity too) boots into a permanent wedge: catch-up
        // replay stops at the hole it cannot attribute, and campaigns start at
        // `min(first_faulty, first_unchosen)`, which never covers a slot no
        // record names. The boot scan is already O(N), so this stays a hard
        // per-slot assert — crash beats corruption.
        for s in first_slot.0..first_unchosen.0 {
            assert!(
                accepted.contains_key(&Slot(s)) || faulty.contains_key(&Slot(s)),
                "every retained slot below the chosen prefix has a durable record"
            );
        }

        let node = Self {
            config,
            acceptors,
            acceptors_since: Ballot::zero(),
            // The bootstrap configuration is bound to `Ballot::zero()`, so a
            // node that boots inside it was a member at exactly that ballot —
            // and a node that boots as a spare has not been a member of
            // anything, which is the same starting value.
            last_member_ballot: Ballot::zero(),
            config_id: hard_state.config_id,
            acceptor: Acceptor::new(hard_state.max_promised_ballot, accepted, first_slot, faulty),
            replica,
            pending_writes: Vec::new(),
            pending_messages: Vec::new(),
            pending_snapshot_offers: Vec::new(),
            pending_read_states: Vec::new(),
            pending_recovery_batch: None,
            tick_count: 0,
            role: NodeRole::Follower,
            leader: None,
            ballot,
            election_elapsed: 0,
            election_timeout: 0,
            needs_election_timeout: true,
            heartbeat_elapsed: 0,
            heartbeat_timeout: HEARTBEAT_TICKS,
            heartbeat_seq: 0,
            quorum_lost_step_downs: 0,
            proposer: {
                let mut proposer = Proposer::new();
                // The allocator frontier the durable log implies: one past the
                // highest record read back (see the local binding above).
                proposer.set_next_slot(next_slot);
                proposer
            },
            matchmaking: None,
            pending_match_requests: Vec::new(),
            pending_gc_requests: Vec::new(),
            matchmakers,
            gc: None,
            non_member_campaigns_skipped: 0,
            non_member_step_downs: 0,
            round_floor: 0,
            matchmaking_timeouts: 0,
            repair_step_downs: 0,
            repair_case1: 0,
            repair_case2: 0,
            repair_bytes: 0,
            leadership_origin: LeadershipOrigin::Elected,
            handoff_fence_elapsed: 0,
            handoff: HandoffCounters::default(),
            election_gap_fills: 0,
        };
        node.assert_invariants();
        node
    }
}

/// The configuration's shape, asserted once at boot: quorum arithmetic,
/// `contains` and broadcast all assume strictly sorted, deduplicated
/// memberships, and every set must be drawn from its pool.
///
/// # Panics
///
/// If any membership is empty, unsorted or duplicated, if the bootstrap
/// membership or matchmaker set leaves its pool, or if the pool does not name
/// this node.
fn assert_config_shape(config: &Config) {
    // Config shape: quorum arithmetic and broadcast both assume a strictly
    // sorted, deduplicated membership. A duplicated peer silently inflates
    // the quorum; a missing self silently deflates it.
    assert!(
        !config.peers.is_empty(),
        "the bootstrap membership names at least one acceptor"
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
        config.matchmakers.windows(2).all(|w| w[0] < w[1]),
        "the matchmaker set is sorted and deduplicated"
    );
    // The bootstrap membership is drawn from the pool, and this node is
    // in the pool. On a plain deployment (no `nodes`, no matchmakers) the
    // pool *is* the membership, so this is today's "membership includes
    // this node's own id": a node outside the acceptor set exists only
    // where a reconfiguration could add or remove it.
    assert!(
        config
            .peers
            .iter()
            .all(|p| config.pool().binary_search(p).is_ok()),
        "the bootstrap membership is drawn from the node pool"
    );
    assert!(
        config.pool().binary_search(&config.id).is_ok(),
        "the node pool includes this node's own id"
    );
    if !config.has_matchmakers() {
        assert!(
            config.peers.binary_search(&config.id).is_ok(),
            "a plain deployment's membership includes this node's own id"
        );
    }
    assert!(
        config
            .matchmakers
            .iter()
            .all(|m| config.matchmaker_pool().binary_search(m).is_ok()),
        "the bootstrap matchmaker set is drawn from the matchmaker pool"
    );
}

/// The acceptor's working state as a boot scan reads it back: the compaction
/// floor, the readable records, and the faulty entries.
type BootLog = (
    Slot,
    BTreeMap<Slot, (Ballot, Command)>,
    BTreeMap<Slot, Ballot>,
);

/// Rebuild the working accepted log from the durable per-slot log: the
/// readable records, the faulty entries (value lost, identity known — Stage 8)
/// and the compaction floor they sit above.
///
/// # Panics
///
/// If the durable state breaks the write-side ordering (a record or a faulty
/// entry above the durable promise) or the tri-state partition (a slot both
/// readable and faulty): corrupted storage, or a broken storage
/// implementation.
#[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all))]
fn read_back_log<S: Storage>(storage: &S, promised: Ballot) -> BootLog {
    let first_slot = storage.first_slot();
    // Scan the durable per-slot log (first_slot..=last_slot); gaps read back
    // as `None` and are skipped.
    let mut accepted: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
    let (first, last) = (first_slot.0, storage.last_slot().0);
    for s in first..=last {
        if let Some(record) = storage.accepted(Slot(s)) {
            // Boot-side pair of the write-side ordering (`on_accept` /
            // `start_accept_round` raise the promise in the same batch as
            // the append): no durable accept ever outranks the durable
            // promise. This scan is already O(N), so the per-record check
            // stays a hard assert — crash beats corruption.
            assert!(
                record.0 <= promised,
                "the durable promise dominates every accepted record"
            );
            accepted.insert(Slot(s), record);
        }
    }

    let mut faulty: BTreeMap<Slot, Ballot> = BTreeMap::new();
    // The boot scan's recoverable faulty entries (Stage 8): value lost,
    // identity known. They are *records this node accepted*, so they bound
    // `next_slot` exactly like readable records; they are simply unreadable
    // and reported as `faulty` instead of `have`. Nothing below the floor is
    // retained, and a slot never appears in both maps.
    for (slot, ballot) in storage.faulty_entries() {
        if slot < first_slot {
            continue;
        }
        assert!(
            !accepted.contains_key(&slot),
            "a faulty entry is never also a readable accepted record"
        );
        // Same boot-side promise domination as the readable scan above: a
        // faulty entry's identity ballot was a real accepted ballot once,
        // so the durable promise flushed with it still covers it.
        assert!(
            ballot <= promised,
            "the durable promise dominates every faulty record"
        );
        faulty.insert(slot, ballot);
    }

    (first_slot, accepted, faulty)
}
