//! The trace tier's last resident: the Chain application-state oracle.
//!
//! Protocol correctness lives in the **audit port** (`crate::audit`), where
//! each driver transition is folded into O(1) incremental state, and the
//! client-visible history lives in the workload that owns it. What is left
//! here reads the trace for one reason that genuinely needs it: the
//! *application*'s transitions are emitted by the storage layer as trace facts,
//! and the `sometimes_each` frontier joins them against the simulator's own
//! fault stream, which no driver callback can see.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use moonpool_sim::{
    Invariant, SIM_FAULT_EVENT_NAME, TraceEvent, TraceQuery, assert_always, assert_reachable,
    assert_sometimes, assert_sometimes_all, assert_sometimes_each,
};
use paros::{
    EV_AUTHORITY_RELINQUISHED, EV_COMPACTED, EV_CRASHED, EV_LEADER, EV_LEADERSHIP_RESIGNED,
};

/// Which trace stream one merged event came from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ChainStream {
    /// `leader_elected` — feeds the per-transition role dimension.
    Leader,
    /// A simulator fault record — feeds the fault-regime dimension.
    Fault,
    /// `compacted` — feeds the per-node floor dimension.
    Compacted,
    /// `chain_command_acked` — feeds the post-failover acknowledgement gate.
    Acked,
    /// `chain_snapshot_installed` — an application state jump.
    Snapshot,
    /// `snapshot_reset_for_recovery` — Stage 8: a corrupted application
    /// snapshot was reset, so the node's applied index legally restarts from
    /// zero (a local log replay, or a wait for a peer `InstallSnapshot`).
    Reset,
    /// `command_applied` — an ordinary application transition.
    Applied,
}

/// Cursor-based Chain-of-Blocks state-machine oracle. It observes only public
/// application facts; it does not share the state transition implementation.
///
/// This is the one checker that still reads the trace, and deliberately so: the
/// facts it needs are the *application*'s (`command_applied` /
/// `chain_snapshot_installed`, which the sim storage layer emits as it flushes),
/// and its `sometimes_each` frontier joins them against the simulator's own
/// fault stream — neither of which any driver-side audit callback can see.
///
/// Everything it reads is consumed through a **cursor**, and every "the latest
/// X before this transition" lookup is maintained incrementally by merging the
/// auxiliary streams into the same seq-ordered walk. One `observe` therefore
/// costs the events that arrived since the last one, not a re-scan of the run.
pub(crate) struct ChainAgreement {
    submitted_cursor: Cell<usize>,
    control_cursor: Cell<usize>,
    snapshot_cursor: Cell<usize>,
    applied_cursor: Cell<usize>,
    leader_cursor: Cell<usize>,
    fault_cursor: Cell<usize>,
    compacted_cursor: Cell<usize>,
    acked_cursor: Cell<usize>,
    reset_cursor: Cell<usize>,
    checksum_rejected_cursor: Cell<usize>,
    submitted: RefCell<BTreeMap<String, u64>>,
    state_by_index: RefCell<BTreeMap<u64, String>>,
    command_by_index: RefCell<BTreeMap<u64, String>>,
    node_index: RefCell<BTreeMap<u64, u64>>,
    /// The most recent `leader_elected`'s node, as of the transition being
    /// walked (`None` when no election has happened yet, or the event carried
    /// no node).
    latest_leader: Cell<Option<u64>>,
    /// The regime of the most recent fault: 0 none, 2 storage, 3 process,
    /// 1 anything else.
    fault_regime: Cell<i64>,
    /// Per node, the highest compaction floor established so far.
    floor_by_node: RefCell<BTreeMap<u64, u64>>,
    /// How many `leader_elected` events have been consumed.
    leaders_seen: Cell<usize>,
    /// Trace seq of the *second* election — the leader change acks are compared
    /// against.
    second_leader_seq: Cell<Option<u64>>,
    /// Highest `chain_command_acked` seq consumed so far.
    max_ack_seq: Cell<Option<u64>>,
    /// A `Noop` gap fill reached the application.
    noop_applied: Cell<bool>,
    safety_only: bool,
}

impl ChainAgreement {
    pub(crate) fn new() -> Self {
        Self {
            submitted_cursor: Cell::new(0),
            control_cursor: Cell::new(0),
            snapshot_cursor: Cell::new(0),
            applied_cursor: Cell::new(0),
            leader_cursor: Cell::new(0),
            fault_cursor: Cell::new(0),
            compacted_cursor: Cell::new(0),
            acked_cursor: Cell::new(0),
            reset_cursor: Cell::new(0),
            checksum_rejected_cursor: Cell::new(0),
            submitted: RefCell::new(BTreeMap::new()),
            state_by_index: RefCell::new(BTreeMap::new()),
            command_by_index: RefCell::new(BTreeMap::new()),
            node_index: RefCell::new(BTreeMap::new()),
            latest_leader: Cell::new(None),
            fault_regime: Cell::new(0),
            floor_by_node: RefCell::new(BTreeMap::new()),
            leaders_seen: Cell::new(0),
            second_leader_seq: Cell::new(None),
            max_ack_seq: Cell::new(None),
            noop_applied: Cell::new(false),
            safety_only: false,
        }
    }

    /// The safety-only flavor, paired with [`ChainWorkload::safety_only`]: it
    /// keeps every agreement check and drops the campaign-liveness guidance
    /// gates, which a deliberately broken cluster cannot be asked to satisfy.
    pub(crate) fn safety_only() -> Self {
        Self {
            safety_only: true,
            ..Self::new()
        }
    }

    fn fault_regime(kind: &str) -> i64 {
        if kind.contains("storage") {
            2
        } else if kind.contains("process") {
            3
        } else {
            1
        }
    }

    /// Fold the client's submission streams in; `submitted` maps a command to
    /// the trace seq at which it was first proposed.
    fn ingest_submitted(&self, q: &dyn TraceQuery) {
        let mut submitted = self.submitted.borrow_mut();
        for event in q.since("chain_command_submitted", &self.submitted_cursor) {
            if let Some(command) = event.str("cmd") {
                submitted.entry(command.to_owned()).or_insert(event.seq);
            }
        }
        for event in q.since("chain_control_submitted", &self.control_cursor) {
            if let Some(command) = event.str("cmd") {
                submitted.entry(command.to_owned()).or_insert(event.seq);
            }
        }
    }

    /// Every stream this oracle joins, merged by global trace sequence. The
    /// application transitions *must* be ordered against each other (separate
    /// cursors alone would lose their causal ordering), and the auxiliary
    /// streams ride along so each transition sees exactly the leader, fault and
    /// floor state established strictly before it.
    fn merged(&self, q: &dyn TraceQuery) -> Vec<(ChainStream, TraceEvent)> {
        let mut merged: Vec<(ChainStream, TraceEvent)> = Vec::new();
        for (stream, name, cursor) in [
            (ChainStream::Leader, EV_LEADER, &self.leader_cursor),
            (ChainStream::Fault, SIM_FAULT_EVENT_NAME, &self.fault_cursor),
            (ChainStream::Compacted, EV_COMPACTED, &self.compacted_cursor),
            (
                ChainStream::Acked,
                "chain_command_acked",
                &self.acked_cursor,
            ),
            (
                ChainStream::Snapshot,
                "chain_snapshot_installed",
                &self.snapshot_cursor,
            ),
            (
                ChainStream::Reset,
                "snapshot_reset_for_recovery",
                &self.reset_cursor,
            ),
            (
                ChainStream::Applied,
                "command_applied",
                &self.applied_cursor,
            ),
        ] {
            merged.extend(q.since(name, cursor).into_iter().map(|e| (stream, e)));
        }
        merged.sort_by_key(|(_, event)| event.seq);
        merged
    }

    /// Update the running join state from one auxiliary event.
    fn observe_auxiliary(&self, stream: ChainStream, event: &TraceEvent) {
        match stream {
            ChainStream::Leader => {
                self.latest_leader.set(event.u64("node"));
                let seen = self.leaders_seen.get() + 1;
                self.leaders_seen.set(seen);
                if seen == 2 {
                    self.second_leader_seq.set(Some(event.seq));
                }
            }
            ChainStream::Fault => {
                if let Some(kind) = event.str("kind") {
                    self.fault_regime.set(Self::fault_regime(kind));
                }
            }
            ChainStream::Compacted => {
                if let (Some(node), Some(first)) = (event.u64("node"), event.u64("first")) {
                    let mut floors = self.floor_by_node.borrow_mut();
                    let slot = floors.entry(node).or_insert(first);
                    *slot = (*slot).max(first);
                }
            }
            ChainStream::Acked => {
                let seq = self
                    .max_ack_seq
                    .get()
                    .map_or(event.seq, |m| m.max(event.seq));
                self.max_ack_seq.set(Some(seq));
            }
            ChainStream::Snapshot | ChainStream::Reset | ChainStream::Applied => {}
        }
    }

    /// Stage 8: a corrupted application snapshot was reset for recovery. The
    /// node's applied index legally restarts from zero — the replay that
    /// follows re-derives the *same* per-index states (the agreement check
    /// keeps holding), it merely re-walks them.
    fn observe_reset(&self, event: &TraceEvent) {
        let Some(node) = event.u64("node") else {
            return;
        };
        self.node_index.borrow_mut().remove(&node);
    }

    /// A snapshot install jumps the application state; it need not be
    /// contiguous, but it must never regress and must agree at its index.
    fn observe_snapshot(&self, event: &TraceEvent) {
        let (Some(node), Some(index), Some(state)) =
            (event.u64("node"), event.u64("index"), event.str("state"))
        else {
            return;
        };
        let mut per_node = self.node_index.borrow_mut();
        let previous = per_node.get(&node).copied();
        assert_always!(
            previous.is_none_or(|previous| index >= previous),
            "chain: a snapshot jump never moves the applied index backward",
            {
                "node" => node,
                "from" => previous.map_or(-1_i64, |p| i64::try_from(p).unwrap_or(i64::MAX)),
                "to" => index,
            }
        );
        per_node.insert(node, index);
        let mut states = self.state_by_index.borrow_mut();
        let prior = states.entry(index).or_insert_with(|| state.to_owned());
        assert_always!(
            prior == state,
            "chain: one state per applied index",
            {
                "node" => node,
                "index" => index,
                "expected_state" => prior,
                "observed_state" => state,
            }
        );
    }

    /// One ordinary application transition: contiguous locally, agreeing
    /// cluster-wide at its index, and only ever a command somebody proposed.
    fn observe_applied(&self, event: &TraceEvent) {
        if event.str("kind") == Some("noop") {
            self.noop_applied.set(true);
        }
        let (Some(node), Some(index), Some(state)) =
            (event.u64("node"), event.u64("index"), event.str("state"))
        else {
            return;
        };
        let mut per_node = self.node_index.borrow_mut();
        let expected = per_node.get(&node).copied().unwrap_or(0).saturating_add(1);
        assert_always!(
            index == expected,
            "chain: applies are contiguous per node",
            { "node" => node, "index" => index, "expected" => expected }
        );
        per_node.insert(node, index);
        drop(per_node);

        let Some(command) = event.str("cmd") else {
            return;
        };
        let mut commands = self.command_by_index.borrow_mut();
        let mut states = self.state_by_index.borrow_mut();
        let prior_command = commands.entry(index).or_insert_with(|| command.to_owned());
        let prior_state = states.entry(index).or_insert_with(|| state.to_owned());
        assert_always!(
            prior_command == command && prior_state == state,
            "chain: one state per applied index",
            {
                "node" => node,
                "index" => index,
                "expected_command" => prior_command,
                "observed_command" => command,
                "expected_state" => prior_state,
                "observed_state" => state,
            }
        );
        drop(states);
        drop(commands);

        let kind = event.str("kind").unwrap_or("unknown");
        // The was-proposed claim guards *client* commands: a control command
        // is minted inside the system — a leader's `Noop` gap fill, a `Snap`
        // marker, or a `Truncate` clamped by the #101 coupling to the covered
        // snapshot point — so only a `user` entry must trace back to a
        // submission.
        let proposed = kind != "user"
            || self
                .submitted
                .borrow()
                .get(command)
                .is_some_and(|submitted_seq| *submitted_seq < event.seq);
        assert_always!(
            proposed,
            "chain: applied command was proposed",
            {
                "node" => node,
                "index" => index,
                "kind" => kind,
                "command" => command,
            }
        );

        let role = i64::from(self.latest_leader.get() == Some(node));
        let floor = self.floor_by_node.borrow().get(&node).copied();
        let floor_relation = match (floor, event.u64("slot")) {
            (Some(first), Some(slot)) if slot >= first => 1,
            (Some(_), Some(_)) => 2,
            (None, _) | (Some(_), None) => 0,
        };
        assert_sometimes_each!(
            "chain: state frontier",
            [
                ("role", role),
                ("fault", self.fault_regime.get()),
                ("floor", floor_relation)
            ],
            [("applied_count", index)]
        );
    }

    /// The campaign-level coverage gates. Every "did this ever happen" question
    /// is an O(1) stream length or a running flag, so the tail costs nothing per
    /// pump.
    fn check_campaign_gates(&self, q: &dyn TraceQuery) {
        let leader_changed = self.leaders_seen.get() >= 2;
        let acknowledged_after_change = self
            .second_leader_seq
            .get()
            .is_some_and(|changed| self.max_ack_seq.get().is_some_and(|ack| ack > changed));
        if self.safety_only {
            if self.noop_applied.get() {
                assert_reachable!("chain: noop gap fill is applied");
            }
            return;
        }
        assert_sometimes!(
            acknowledged_after_change,
            "chain: proposal succeeds after leader change"
        );
        assert_sometimes!(
            q.len("chain_compact_accepted") != 0 && q.len(EV_COMPACTED) != 0,
            "chain: compact takes effect"
        );
        assert_sometimes!(
            q.len("chain_snapshot_installed") != 0,
            "chain: node recovers through snapshot install"
        );
        // Every way a sitting leader can stop being one: it resigned, it
        // crashed, or it cooperatively handed its authority to a successor.
        let old_leader_gone = q.len(EV_LEADERSHIP_RESIGNED) != 0
            || q.len(EV_CRASHED) != 0
            || q.len(EV_AUTHORITY_RELINQUISHED) != 0;
        assert_sometimes_all!(
            "chain: failover completed",
            [
                ("old leader gone", old_leader_gone),
                ("new leader elected", leader_changed),
                ("client acknowledged", acknowledged_after_change),
            ]
        );
    }
}

impl Default for ChainAgreement {
    fn default() -> Self {
        Self::new()
    }
}

impl Invariant for ChainAgreement {
    fn name(&self) -> &'static str {
        "chain_agreement"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        self.ingest_submitted(q);
        for _ in q.since("proposal_checksum_rejected", &self.checksum_rejected_cursor) {
            assert_reachable!("chain: an invalid proposal checksum is rejected");
        }
        for (stream, event) in self.merged(q) {
            match stream {
                ChainStream::Snapshot => self.observe_snapshot(&event),
                ChainStream::Reset => self.observe_reset(&event),
                ChainStream::Applied => self.observe_applied(&event),
                aux => self.observe_auxiliary(aux, &event),
            }
        }
        self.check_campaign_gates(q);
    }

    fn reset(&mut self) {
        self.submitted_cursor.set(0);
        self.control_cursor.set(0);
        self.snapshot_cursor.set(0);
        self.applied_cursor.set(0);
        self.leader_cursor.set(0);
        self.fault_cursor.set(0);
        self.compacted_cursor.set(0);
        self.acked_cursor.set(0);
        self.reset_cursor.set(0);
        self.checksum_rejected_cursor.set(0);
        self.submitted.get_mut().clear();
        self.state_by_index.get_mut().clear();
        self.command_by_index.get_mut().clear();
        self.node_index.get_mut().clear();
        self.floor_by_node.get_mut().clear();
        self.latest_leader.set(None);
        self.fault_regime.set(0);
        self.leaders_seen.set(0);
        self.second_leader_seq.set(None);
        self.max_ack_seq.set(None);
        self.noop_applied.set(false);
    }
}
