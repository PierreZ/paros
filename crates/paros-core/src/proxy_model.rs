//! A sans-IO **model checker** for the proxy leader (#142): the adversarial
//! campaign the delegation doctrine rests on, run over the real state
//! machines — [`ColocatedNode`]s and [`ProxyLeader`]s, so the wiring that
//! delegates, re-delegates, takes back and hands off is the wiring under
//! test — with a scheduler in place of the network, the disks and the
//! clock. The sibling of `matchmaker/handover_model.rs`, built on the same
//! scaffolding (`model_support`).
//!
//! Each seed draws a deployment (a majority of three, or a `2 × 2` grid of
//! four, with two proxies) and a schedule: clients propose at whoever
//! leads, with the driver's delegation override drawn per proposal (the
//! core's rule, a named proxy, or colocated); every message may be
//! dropped, duplicated or reordered; proxies crash at any step and reboot
//! **empty**; nodes crash at any step — before or after their batch is
//! durable — and reboot from exactly what their disk holds; a leader
//! resigns, or hands its authority off mid-round to a peer that re-delegates
//! what it inherited. After every step the model asserts:
//!
//! 1. **at most one value is chosen per slot** — over every `Commit` any
//!    party emits and every replica's chosen log;
//! 2. **every `Commit` a proxy emits is backed by a Phase-2 quorum of
//!    durable accepts at one ballot** — judged at the instant the proxy's
//!    batch is drained, against the accepts the nodes' disks hold;
//! 3. **a durable Phase-2 quorum never decides two values for a slot** —
//!    the same claim over the disks alone, whoever announced it.
//!
//! Then the faults stop. Some proxies stay **dead** for the tail (drawn per
//! seed), the network drops nothing, every node is rebooted alive, the
//! leader keeps proposing for a while, and the model asserts the liveness
//! claim behind [`ColocatedNode::take_back_delegated`]: every slot below
//! the leader's frontier is chosen at every node, no proxy holds a round
//! open, and the leader holds no delegated round — so a slot delegated to a
//! proxy that will never answer was taken back and decided colocated.
//!
//! **It bites.** With the proxy's decision made one `Accepted` short —
//! emitting the `Commit` on `quorum - 1` votes — claim 2 is red on the
//! first seed, and claim 3 follows within a few seeds once a leader change
//! re-proposes over the phantom decision. That mutation is the reason
//! claim 2 is judged against the disks and not against the proxy's own
//! tally.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use crate::membership::{AcceptorConfig, ProxyId, QuorumSystem};
use crate::message::{Message, Party};
use crate::model_support::{Mailbox, Rng};
use crate::node::{ColocatedNode, Delegation, ProposeResult};
use crate::proxy_leader::ProxyLeader;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{
    Ballot, ClientId, ClientSeq, Command, NodeId, Slot, Value, command_fingerprint,
};
use crate::write::{AcceptorWrite, WriteOp};

/// Seeds per campaign (`PROXY_MODEL_SEEDS` overrides; a long run is
/// `PROXY_MODEL_SEEDS=5000 cargo nextest run -p paros-core proxy_model`).
const SEEDS: u64 = 300;
/// Chaotic steps per seed (`PROXY_MODEL_STEPS` overrides).
const CHAOS_STEPS: usize = 600;
/// Quiet steps per seed after the faults stop.
const QUIET_STEPS: usize = 400;
/// Proxies in the deployment.
const PROXIES: usize = 2;
/// Messages in flight at most.
const MAILBOX: usize = 128;
/// The bounded drain that empties the network before the converged state
/// is judged.
const DRAIN_STEPS: usize = 4_000;
/// Rounds of "tick everyone, drain everything" the convergence check gets
/// before a stall is a finding.
const SETTLE_ROUNDS: usize = 200;

/// A node's disk: the durable state a reboot boots from, fed by the
/// `WriteOp`s its batches persist. The one place the model reads a
/// "durable accept" from.
#[derive(Clone, Default)]
struct Disk {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
}

impl Disk {
    fn apply(&mut self, op: &WriteOp) {
        match op {
            WriteOp::Acceptor(AcceptorWrite::SetPromise(b)) => {
                self.hard_state.max_promised_ballot = self.hard_state.max_promised_ballot.max(*b);
            }
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value,
            }) => {
                self.accepted.insert(*slot, (*ballot, value.clone()));
            }
            WriteOp::SetChosenIndex(s) => self.hard_state.chosen_index = Some(*s),
            // The model never proposes a `Truncate` and serves no snapshot.
            WriteOp::Truncate { .. } | WriteOp::InstallSnapshot { .. } => {
                unreachable!("the proxy model never compacts")
            }
        }
    }
}

/// The disk plus the deployment's configuration, as a `Storage` a reboot
/// reads.
struct Boot<'a> {
    disk: &'a Disk,
    config: Config,
}

impl Storage for Boot<'_> {
    fn initial_state(&self) -> (HardState, Config) {
        (self.disk.hard_state, self.config.clone())
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.disk.accepted.get(&slot).cloned()
    }
    fn first_slot(&self) -> Slot {
        Slot(0)
    }
    fn last_slot(&self) -> Slot {
        self.disk
            .accepted
            .keys()
            .next_back()
            .copied()
            .unwrap_or(Slot(0))
    }
}

/// One node: its disk and the live state machine when it is up.
struct Site {
    disk: Disk,
    live: Option<ColocatedNode>,
}

/// One proxy: the live state machine when it is up (it has no disk).
struct Proxy {
    live: Option<ProxyLeader>,
    /// Whether this proxy stays dead for the recovery tail.
    dead_for_good: bool,
}

/// A message in flight, to a node or a proxy.
#[derive(Clone)]
struct Envelope {
    to: Party,
    msg: Message,
}

/// The durable facts the model collects, from the disks alone.
#[derive(Default)]
struct Ledger {
    /// Every durable accept ever persisted: `(slot, ballot, vhash) -> nodes`.
    /// Never pruned: a disk's record may be overwritten by a later ballot,
    /// but the vote it once cast is what a decision rested on.
    accepts: BTreeMap<(Slot, Ballot, u64), BTreeSet<NodeId>>,
    /// Per slot, the one value chosen — observed from a `Commit`, a replica,
    /// or a durable quorum (claims 1 and 3).
    chosen: BTreeMap<Slot, u64>,
}

impl Ledger {
    /// Claims 1 and 3 at one observation: `slot` is chosen as `vhash`.
    fn observe_chosen(&mut self, slot: Slot, vhash: u64, where_: &str) {
        let known = self.chosen.entry(slot).or_insert(vhash);
        assert!(
            *known == vhash,
            "at most one value is chosen per slot ({where_}): slot {} saw {known:#x} and {vhash:#x}",
            slot.0
        );
    }

    /// Whether the durable accepts of `(slot, ballot, vhash)` form a Phase-2
    /// quorum of `config` in any column.
    fn quorum_backed(
        &self,
        config: &AcceptorConfig,
        slot: Slot,
        ballot: Ballot,
        vhash: u64,
    ) -> bool {
        self.accepts
            .get(&(slot, ballot, vhash))
            .is_some_and(|holders| config.has_phase2_quorum(holders))
    }
}

/// What the campaign reached, over every seed: each must fire at least
/// once per campaign (the `sometimes` of this checker).
#[derive(Default, Debug)]
struct Reach {
    /// A slot was decided by a proxy's `Commit`.
    decided_through_proxy: u64,
    /// A slot was decided colocated on the leader.
    decided_colocated: u64,
    /// A leader took a delegated round back.
    taken_back: u64,
    /// A taken-back round was decided colocated afterwards.
    decided_after_take_back: u64,
    /// A proxy's `Commit` arrived at a leader that had already taken the
    /// round back (the two verdicts agreed).
    commit_after_take_back: u64,
    /// A handoff successor re-delegated an inherited round.
    handoff_redelegated: u64,
    /// A proxy refreshed a round's leader hint on a re-delegation.
    leader_hint_refreshed: u64,
    /// A proxy crashed with rounds open.
    proxy_crashed_open: u64,
    /// A proxy relayed a `Nack` to the leader that delegated the round.
    nack_relayed: u64,
    /// A node rebooted from its disk.
    node_rebooted: u64,
    /// A node crashed before its batch was durable.
    crash_before_persist: u64,
    /// The driver named a proxy explicitly.
    delegated_by_override: u64,
    /// The driver ran a round colocated on a proxied deployment.
    colocated_by_override: u64,
    /// A delegation was ignored at a proxy as a closed round.
    ignored_closed: u64,
    /// A seed ran a grid.
    grid_seed: u64,
    /// A seed's tail ran with a proxy dead for good.
    tail_with_dead_proxy: u64,
    /// A proxy re-fanned-out a re-delegated round.
    refanned: u64,
    /// A round was superseded at a proxy by a newer ballot.
    superseded_at_proxy: u64,
}

impl Reach {
    fn assert_all(&self) {
        let counters = [
            ("decided_through_proxy", self.decided_through_proxy),
            ("decided_colocated", self.decided_colocated),
            ("taken_back", self.taken_back),
            ("decided_after_take_back", self.decided_after_take_back),
            ("commit_after_take_back", self.commit_after_take_back),
            ("handoff_redelegated", self.handoff_redelegated),
            ("leader_hint_refreshed", self.leader_hint_refreshed),
            ("proxy_crashed_open", self.proxy_crashed_open),
            ("nack_relayed", self.nack_relayed),
            ("node_rebooted", self.node_rebooted),
            ("crash_before_persist", self.crash_before_persist),
            ("delegated_by_override", self.delegated_by_override),
            ("colocated_by_override", self.colocated_by_override),
            ("ignored_closed", self.ignored_closed),
            ("grid_seed", self.grid_seed),
            ("tail_with_dead_proxy", self.tail_with_dead_proxy),
            ("refanned", self.refanned),
            ("superseded_at_proxy", self.superseded_at_proxy),
        ];
        for (name, count) in counters {
            assert!(
                count > 0,
                "the campaign reaches `{name}` at least once: {self:?}"
            );
        }
    }
}

struct World {
    rng: Rng,
    config: AcceptorConfig,
    pool: Vec<NodeId>,
    sites: Vec<Site>,
    proxies: Vec<Proxy>,
    network: Mailbox<Envelope>,
    ledger: Ledger,
    reach: Reach,
    /// Whether faults are still being injected.
    chaos: bool,
    /// The driver's take-back budget for this seed (re-delegations a proxy
    /// may swallow before the leader runs the round itself).
    take_back_after: u64,
    /// Next client sequence to propose.
    next_seq: u64,
    /// Slots a leader was seen holding delegated, with the proxy.
    delegated: BTreeMap<Slot, ProxyId>,
    /// Slots a leader took back.
    taken_back: BTreeSet<Slot>,
    /// Per proxy, the counters last observed (to attribute deltas).
    proxy_counters: Vec<crate::proxy_leader::ProxyCounters>,
    /// Whether `PROXY_MODEL_TRACE` is set: print the schedule as it runs.
    trace: bool,
}

impl World {
    fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let grid = rng.chance(1, 2);
        let (n, system) = if grid {
            (4, QuorumSystem::Grid { rows: 2, cols: 2 })
        } else {
            (3, QuorumSystem::Majority)
        };
        let pool: Vec<NodeId> = (0..n).map(NodeId).collect();
        let config = AcceptorConfig::new(pool.clone(), system);
        let take_back_after = 1 + rng.below(3);
        let mut world = Self {
            rng,
            config,
            pool,
            sites: (0..n)
                .map(|_| Site {
                    disk: Disk::default(),
                    live: None,
                })
                .collect(),
            proxies: (0..PROXIES)
                .map(|_| Proxy {
                    live: None,
                    dead_for_good: false,
                })
                .collect(),
            network: Mailbox::new(MAILBOX),
            ledger: Ledger::default(),
            reach: Reach::default(),
            chaos: true,
            take_back_after,
            next_seq: 1,
            delegated: BTreeMap::new(),
            taken_back: BTreeSet::new(),
            proxy_counters: vec![crate::proxy_leader::ProxyCounters::default(); PROXIES],
            trace: std::env::var("PROXY_MODEL_TRACE").is_ok(),
        };
        if grid {
            world.reach.grid_seed += 1;
        }
        for i in 0..n {
            world.boot_node(NodeId(i));
        }
        for p in 0..PROXIES {
            world.boot_proxy(ProxyId(p as u64));
        }
        world
    }

    fn node_config(&self, id: NodeId) -> Config {
        Config {
            id,
            peers: self.pool.clone(),
            quorum_system: self.config.quorum_system(),
            nodes: Vec::new(),
            matchmakers: Vec::new(),
            matchmaker_pool: Vec::new(),
            proxy_count: PROXIES,
        }
    }

    fn site(&mut self, id: NodeId) -> &mut Site {
        &mut self.sites[usize::try_from(id.0).expect("index")]
    }

    fn proxy(&mut self, id: ProxyId) -> &mut Proxy {
        &mut self.proxies[usize::try_from(id.0).expect("index")]
    }

    fn boot_node(&mut self, id: NodeId) {
        let config = self.node_config(id);
        let site = self.site(id);
        if site.disk.hard_state != HardState::default() {
            self.reach.node_rebooted += 1;
        }
        let site = self.site(id);
        let mut node = ColocatedNode::new(&Boot {
            disk: &site.disk,
            config,
        });
        let timeout = 3 + self.rng.below(6);
        node.set_election_timeout(timeout);
        self.site(id).live = Some(node);
        self.drain_node(id);
    }

    fn boot_proxy(&mut self, id: ProxyId) {
        let config = self.config.clone();
        self.proxy(id).live = Some(ProxyLeader::new(id, config));
    }

    fn send(&mut self, envelope: Envelope) {
        self.network.push(envelope, &mut self.rng);
    }

    // ---- draining ---------------------------------------------------------

    /// Drain a node's batch with the durability seams a real driver
    /// crosses: persist, then send (crash point A before the persist, B
    /// between persist and send). Then release the next bounded
    /// continuation.
    fn drain_node(&mut self, id: NodeId) {
        let chaos = self.chaos;
        let crash_before = chaos && self.rng.chance(1, 150);
        let crash_between = chaos && self.rng.chance(1, 150);
        let pool = self.pool.clone();
        let site = self.site(id);
        let Some(node) = site.live.as_mut() else {
            return;
        };
        let ready = node.ready();
        let writes = ready.writes().to_vec();
        let mut out = Vec::new();
        for (audience, msg) in ready.messages() {
            if let Some(proxy) = audience.proxy() {
                out.push(Envelope {
                    to: Party::Proxy(proxy),
                    msg: msg.clone(),
                });
            }
            for to in audience.resolve(&pool, id) {
                out.push(Envelope {
                    to: Party::Node(to),
                    msg: msg.clone(),
                });
            }
        }
        ready.advance();
        if crash_before && !writes.is_empty() {
            site.live = None;
            self.reach.crash_before_persist += 1;
            return;
        }
        for op in &writes {
            self.site(id).disk.apply(op);
            if let WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value,
            }) = op
            {
                let vhash = command_fingerprint(value);
                self.ledger
                    .accepts
                    .entry((*slot, *ballot, vhash))
                    .or_default()
                    .insert(id);
                // Claim 3: a durable quorum is a decision, whoever announces
                // it.
                if self
                    .ledger
                    .quorum_backed(&self.config, *slot, *ballot, vhash)
                {
                    self.ledger.observe_chosen(*slot, vhash, "durable quorum");
                }
            }
        }
        if crash_between {
            self.site(id).live = None;
            return;
        }
        for envelope in &out {
            if let Message::Commit { slot, command, .. } = &envelope.msg {
                self.ledger
                    .observe_chosen(*slot, command_fingerprint(command), "a node's Commit");
            }
        }
        for envelope in out {
            self.send(envelope);
        }
        let site = self.site(id);
        if let Some(node) = site.live.as_mut() {
            node.advance_recovery();
            if node.is_leader() {
                for (slot, proxy) in node.delegated_rounds() {
                    self.delegated.insert(slot, proxy);
                }
            }
        }
        // The continuation may have queued more: drain to a fixed point.
        let has_more = self
            .site(id)
            .live
            .as_ref()
            .is_some_and(ColocatedNode::ready_pending);
        if has_more {
            self.drain_node(id);
        }
    }

    /// Drain a proxy's batch: judge every `Commit` it emits (claim 2), then
    /// put its messages in flight.
    fn drain_proxy(&mut self, id: ProxyId) {
        let pool = self.pool.clone();
        let proxy = self.proxy(id);
        let Some(live) = proxy.live.as_mut() else {
            return;
        };
        let ready = live.ready();
        let mut out = Vec::new();
        for (audience, msg) in ready.messages() {
            for to in audience.resolve_from_proxy(&pool) {
                out.push(Envelope {
                    to: Party::Node(to),
                    msg: msg.clone(),
                });
            }
        }
        ready.advance();
        let counters = live.counters();
        let before = self.proxy_counters[usize::try_from(id.0).expect("index")];
        self.proxy_counters[usize::try_from(id.0).expect("index")] = counters;
        if counters.refanned > before.refanned {
            self.reach.refanned += 1;
        }
        if counters.superseded > before.superseded {
            self.reach.superseded_at_proxy += 1;
        }
        if counters.ignored > before.ignored {
            self.reach.ignored_closed += 1;
        }
        if counters.relayed_nacks > before.relayed_nacks {
            self.reach.nack_relayed += 1;
        }
        for envelope in &out {
            if let Message::Commit {
                from,
                ballot,
                slot,
                command,
            } = &envelope.msg
            {
                assert!(
                    *from == Party::Proxy(id),
                    "a proxy's Commit names the proxy"
                );
                let vhash = command_fingerprint(command);
                // Claim 2: the disks, not the proxy's tally, back the
                // decision.
                assert!(
                    self.ledger
                        .quorum_backed(&self.config, *slot, *ballot, vhash),
                    "a proxy's Commit is backed by a Phase-2 quorum of durable accepts at one ballot: proxy {} committed slot {} at {:?} with holders {:?}{}",
                    id.0,
                    slot.0,
                    ballot,
                    self.ledger.accepts.get(&(*slot, *ballot, vhash)),
                    self.dump()
                );
                self.ledger.observe_chosen(*slot, vhash, "a proxy's Commit");
                self.reach.decided_through_proxy += 1;
                if self.taken_back.contains(slot) {
                    self.reach.commit_after_take_back += 1;
                }
            }
        }
        for envelope in out {
            self.send(envelope);
        }
    }

    // ---- delivery ---------------------------------------------------------

    fn deliver_random(&mut self) {
        let Some(envelope) = self.network.take(&mut self.rng) else {
            return;
        };
        if self.chaos {
            if self.rng.chance(1, 10) {
                return; // dropped
            }
            if self.rng.chance(1, 10) {
                self.send(envelope.clone()); // duplicated
            }
        }
        self.deliver(envelope);
    }

    fn deliver(&mut self, envelope: Envelope) {
        if self.trace {
            eprintln!("  deliver to {}: {:?}", envelope.to, envelope.msg);
        }
        match envelope.to {
            Party::Node(id) => {
                let Some(node) = self.site(id).live.as_mut() else {
                    return;
                };
                node.step(envelope.msg);
                self.drain_node(id);
            }
            Party::Proxy(id) => {
                let Some(proxy) = self.proxy(id).live.as_mut() else {
                    return;
                };
                let delegator_before = match &envelope.msg {
                    Message::Accept { slot, .. } => proxy.delegator(*slot),
                    _ => None,
                };
                proxy.step(envelope.msg.clone());
                if let (Some(before), Message::Accept { slot, leader, .. }) =
                    (delegator_before, &envelope.msg)
                    && proxy.delegator(*slot) == Some(*leader)
                    && before != *leader
                {
                    self.reach.leader_hint_refreshed += 1;
                }
                self.drain_proxy(id);
            }
        }
    }

    // ---- the clock and the clients ------------------------------------

    /// One driver beat at every live node — tick, re-send, take back, feed
    /// the election clock — and at every live proxy.
    fn tick_all(&mut self) {
        let after = self.take_back_after;
        for p in 0..PROXIES {
            let id = ProxyId(p as u64);
            if let Some(proxy) = self.proxy(id).live.as_mut() {
                proxy.resend_pending();
                self.drain_proxy(id);
            }
        }
        for i in 0..self.pool.len() {
            let id = NodeId(i as u64);
            let taken_before = self.taken_back.len();
            let site = self.site(id);
            let Some(node) = site.live.as_mut() else {
                continue;
            };
            node.tick();
            if node.needs_election_timeout() {
                let timeout = 3 + self.rng.below(6);
                self.site(id)
                    .live
                    .as_mut()
                    .expect("live")
                    .set_election_timeout(timeout);
            }
            let node = self.site(id).live.as_mut().expect("live");
            node.resend_pending();
            let delegated_before: BTreeSet<Slot> = node
                .delegated_rounds()
                .into_iter()
                .map(|(s, _)| s)
                .collect();
            node.take_back_delegated(after);
            let delegated_after: BTreeSet<Slot> = node
                .delegated_rounds()
                .into_iter()
                .map(|(s, _)| s)
                .collect();
            let still_open: Vec<Slot> = delegated_before
                .difference(&delegated_after)
                .filter(|slot| node.proposer().rounds().contains_key(slot))
                .copied()
                .collect();
            self.taken_back.extend(still_open);
            if self.taken_back.len() > taken_before {
                self.reach.taken_back += 1;
            }
            self.drain_node(id);
        }
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.sites
            .iter()
            .enumerate()
            .filter(|(_, s)| s.live.as_ref().is_some_and(ColocatedNode::is_leader))
            .map(|(i, _)| NodeId(i as u64))
            .collect()
    }

    /// A client proposes at a leader, with the driver's delegation choice
    /// drawn per proposal.
    fn propose(&mut self) {
        let leaders = self.leaders();
        let Some(&leader) = self.rng.pick(&leaders) else {
            return;
        };
        let seq = self.next_seq;
        self.next_seq += 1;
        let delegation = match self.rng.below(10) {
            0..=5 => Delegation::Auto,
            6..=7 => Delegation::To(ProxyId(self.rng.below(PROXIES as u64))),
            _ => Delegation::Colocated,
        };
        let column = match self.config.quorum_system() {
            QuorumSystem::Grid { cols, .. } if self.rng.chance(1, 3) => {
                Some(usize::try_from(self.rng.below(cols as u64)).expect("index"))
            }
            _ => None,
        };
        let value = Value(seq.to_le_bytes().to_vec());
        let trace = self.trace;
        let node = self.site(leader).live.as_mut().expect("a leader is live");
        let result = node.propose_in(ClientId(1), ClientSeq(seq), value, column, delegation);
        if trace {
            eprintln!(
                "  propose seq={seq} at node {} ({delegation:?}, column {column:?}) -> {result:?}",
                leader.0
            );
        }
        if let ProposeResult::Accepted(slot) = result {
            match delegation {
                Delegation::To(_) if node.delegated_rounds().iter().any(|(s, _)| *s == slot) => {
                    self.reach.delegated_by_override += 1;
                }
                Delegation::Colocated => self.reach.colocated_by_override += 1,
                _ => {}
            }
        }
        self.drain_node(leader);
        // Now and then the leader hands off with this very round in flight:
        // the successor inherits it and re-delegates it, which is the case
        // the proxy's leader-hint refresh exists for.
        if self.chaos && self.rng.chance(1, 5) {
            self.handoff();
        }
    }

    // ---- faults -----------------------------------------------------------

    fn chaos_step(&mut self) {
        let draw = self.rng.below(100);
        if self.trace {
            eprintln!("step draw={draw}");
        }
        match draw {
            0..=44 => {
                for _ in 0..3 {
                    self.deliver_random();
                }
            }
            45..=62 => self.tick_all(),
            63..=79 => self.propose(),
            80..=84 => {
                let id = ProxyId(self.rng.below(PROXIES as u64));
                let proxy = self.proxy(id);
                if proxy.live.as_ref().is_some_and(|p| !p.rounds().is_empty()) {
                    self.reach.proxy_crashed_open += 1;
                }
                self.proxy(id).live = None;
            }
            85..=88 => {
                let id = ProxyId(self.rng.below(PROXIES as u64));
                if self.proxy(id).live.is_none() {
                    self.boot_proxy(id);
                }
            }
            89..=91 => {
                let id = NodeId(self.rng.below(self.pool.len() as u64));
                self.site(id).live = None;
            }
            92..=94 => {
                let id = NodeId(self.rng.below(self.pool.len() as u64));
                if self.site(id).live.is_none() {
                    self.boot_node(id);
                }
            }
            95..=96 => {
                let leaders = self.leaders();
                if let Some(&leader) = self.rng.pick(&leaders) {
                    self.site(leader).live.as_mut().expect("live").step_down();
                    self.drain_node(leader);
                }
            }
            _ => self.handoff(),
        }
    }

    /// A leader hands its authority to a random peer; the successor's
    /// inherited rounds are re-delegated.
    fn handoff(&mut self) {
        let leaders = self.leaders();
        let Some(&leader) = self.rng.pick(&leaders) else {
            return;
        };
        let node = self.site(leader).live.as_mut().expect("live");
        let candidates = node.handoff_candidates();
        let Some(&target) = self.rng.pick(&candidates) else {
            return;
        };
        let node = self.site(leader).live.as_mut().expect("live");
        let receipt = node.relinquish_to(target);
        if self.trace {
            eprintln!("  handoff {} -> {}: {receipt:?}", leader.0, target.0);
        }
        if let Some(receipt) = receipt
            && receipt.pending > 0
        {
            self.reach.handoff_redelegated += 1;
        }
        self.drain_node(leader);
    }

    // ---- checks -------------------------------------------------------------

    /// Claim 1 over every live replica.
    fn check_all(&mut self) {
        let mut seen = Vec::new();
        for (i, site) in self.sites.iter().enumerate() {
            if let Some(node) = &site.live {
                for (slot, command) in node.replica().chosen() {
                    seen.push((i, *slot, command_fingerprint(command)));
                }
            }
        }
        for (i, slot, vhash) in seen {
            self.ledger
                .observe_chosen(slot, vhash, &format!("node {i}'s replica"));
        }
    }

    fn quiet_step(&mut self, step: usize) {
        for i in 0..self.pool.len() {
            if self.sites[i].live.is_none() {
                self.boot_node(NodeId(i as u64));
            }
        }
        for p in 0..PROXIES {
            if self.proxies[p].live.is_none() && !self.proxies[p].dead_for_good {
                self.boot_proxy(ProxyId(p as u64));
            }
        }
        if step.is_multiple_of(3) {
            self.tick_all();
        }
        // The leader keeps proposing early in the tail, so a dead proxy is
        // handed rounds it will never answer and the take-back is exercised
        // on fresh delegations, not only on chaos leftovers.
        if step < QUIET_STEPS / 2 && step.is_multiple_of(7) {
            self.propose();
        }
        for _ in 0..8 {
            self.deliver_random();
        }
    }

    /// The liveness claim: with the faults stopped, every slot below the
    /// leader's frontier is chosen at every node, no proxy holds a round,
    /// and the leader holds no delegated round.
    fn assert_converged(&mut self, seed: u64) {
        for round in 0..SETTLE_ROUNDS {
            self.tick_all();
            let mut guard = 0;
            while !self.network.is_empty() && guard < DRAIN_STEPS {
                self.deliver_random();
                guard += 1;
            }
            self.check_all();
            if self.converged() {
                if round > 0 || self.proxies.iter().any(|p| p.dead_for_good) {
                    // A tail that needed a take-back to converge.
                }
                return;
            }
        }
        panic!(
            "seed {seed}: the recovery tail converges — every slot below the frontier chosen everywhere, no delegated round left{}",
            self.dump()
        );
    }

    fn converged(&self) -> bool {
        let leaders = self.leaders();
        let [leader] = leaders.as_slice() else {
            return false;
        };
        let leader = self.sites[usize::try_from(leader.0).expect("index")]
            .live
            .as_ref()
            .expect("live");
        let frontier = leader.proposer().next_slot();
        if !leader.delegated_rounds().is_empty() || !leader.proposer().rounds().is_empty() {
            return false;
        }
        if self
            .proxies
            .iter()
            .any(|p| p.live.as_ref().is_some_and(|l| !l.rounds().is_empty()))
        {
            return false;
        }
        self.sites.iter().all(|site| {
            site.live
                .as_ref()
                .is_some_and(|node| node.replica().first_unchosen() >= frontier)
        })
    }

    /// A one-line-per-party dump of the world, for a failing seed.
    fn dump(&self) -> String {
        let mut out = String::new();
        for (i, site) in self.sites.iter().enumerate() {
            let _ = write!(
                out,
                "\n  node {i}: promised {:?} chosen_index {:?} live={} role={:?} next_slot={:?} rounds={:?} delegated={:?}",
                site.disk.hard_state.max_promised_ballot,
                site.disk.hard_state.chosen_index,
                site.live.is_some(),
                site.live.as_ref().map(ColocatedNode::role),
                site.live.as_ref().map(|n| n.proposer().next_slot()),
                site.live
                    .as_ref()
                    .map(|n| n.proposer().rounds().keys().collect::<Vec<_>>()),
                site.live.as_ref().map(ColocatedNode::delegated_rounds),
            );
        }
        for (i, proxy) in self.proxies.iter().enumerate() {
            let _ = write!(
                out,
                "\n  proxy {i}: live={} dead_for_good={} rounds={:?} counters={:?}",
                proxy.live.is_some(),
                proxy.dead_for_good,
                proxy
                    .live
                    .as_ref()
                    .map(|p| p.rounds().by_slot().keys().collect::<Vec<_>>()),
                proxy.live.as_ref().map(ProxyLeader::counters),
            );
        }
        let _ = write!(out, "\n  in flight: {}", self.network.len());
        out
    }

    fn run(mut self, seed: u64, chaos_steps: usize) -> Reach {
        let trace = self.trace;
        if trace {
            eprintln!("seed {seed}: start ({:?})", self.config.quorum_system());
        }
        for _ in 0..chaos_steps {
            self.chaos_step();
            self.check_all();
        }
        if trace {
            eprintln!("seed {seed}: chaos over{}", self.dump());
        }
        self.chaos = false;
        // Some proxies stay dead for the tail: the take-back is the only
        // way their rounds decide.
        for p in 0..PROXIES {
            if self.rng.chance(1, 3) {
                self.proxies[p].dead_for_good = true;
                self.proxies[p].live = None;
            }
        }
        if self.proxies.iter().any(|p| p.dead_for_good) {
            self.reach.tail_with_dead_proxy += 1;
        }
        for step in 0..QUIET_STEPS {
            self.quiet_step(step);
            self.check_all();
        }
        self.assert_converged(seed);
        // Every slot decided after a take-back was decided colocated, on
        // the leader's own tally: the take-back did its job.
        if self
            .taken_back
            .iter()
            .any(|slot| self.ledger.chosen.contains_key(slot))
        {
            self.reach.decided_after_take_back += 1;
        }
        let colocated_decisions = self
            .ledger
            .chosen
            .keys()
            .filter(|slot| !self.delegated.contains_key(slot))
            .count();
        if colocated_decisions > 0 {
            self.reach.decided_colocated += 1;
        }
        self.reach
    }
}

impl ColocatedNode {
    /// Whether a drained node still has a batch to hand out: its bounded
    /// continuation queued more.
    fn ready_pending(&self) -> bool {
        !self.pending_messages().is_empty()
            || !self.pending_writes().is_empty()
            || self.pending_recovery_batch().is_some()
    }
}

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The campaign: every seed, then the reach counters.
#[test]
fn proxy_leaders_are_safe_under_chaos_and_live_after_it() {
    let seeds = env_or("PROXY_MODEL_SEEDS", SEEDS);
    let from = env_or("PROXY_MODEL_FROM", 1);
    let steps = usize::try_from(env_or("PROXY_MODEL_STEPS", CHAOS_STEPS as u64)).expect("steps");
    let mut total = Reach::default();
    for seed in from..=seeds {
        let reach = World::new(seed).run(seed, steps);
        total.decided_through_proxy += reach.decided_through_proxy;
        total.decided_colocated += reach.decided_colocated;
        total.taken_back += reach.taken_back;
        total.decided_after_take_back += reach.decided_after_take_back;
        total.commit_after_take_back += reach.commit_after_take_back;
        total.handoff_redelegated += reach.handoff_redelegated;
        total.leader_hint_refreshed += reach.leader_hint_refreshed;
        total.proxy_crashed_open += reach.proxy_crashed_open;
        total.nack_relayed += reach.nack_relayed;
        total.node_rebooted += reach.node_rebooted;
        total.crash_before_persist += reach.crash_before_persist;
        total.delegated_by_override += reach.delegated_by_override;
        total.colocated_by_override += reach.colocated_by_override;
        total.ignored_closed += reach.ignored_closed;
        total.grid_seed += reach.grid_seed;
        total.tail_with_dead_proxy += reach.tail_with_dead_proxy;
        total.refanned += reach.refanned;
        total.superseded_at_proxy += reach.superseded_at_proxy;
    }
    total.assert_all();
}
