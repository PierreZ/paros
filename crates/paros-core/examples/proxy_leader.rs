//! **Proxy leaders: the leader sequences, somebody else broadcasts.**
//!
//! Run it: `cargo run -p paros-core --example proxy_leader`
//!
//! The lesson after `quorum_read`, and the first **second deployment** in
//! this crate: beside the six [`ColocatedNode`]s of a `2 × 3` grid run two
//! [`ProxyLeader`]s — processes that are neither acceptors nor replicas,
//! hold nothing durable, and do one thing: fan a slot's `Accept` out to its
//! column, fold the `Accepted`s, and announce the `Commit`.
//!
//! # The bottleneck (Compartmentalized Paxos §3.1)
//!
//! A Multi-Paxos leader does two jobs per command. It *sequences* — picks
//! the slot — which is serial and cheap. And it *broadcasts*: sends the
//! `Accept` to every acceptor of the column, collects their `Accepted`s,
//! sends the `Commit` to every learner. With `f` faults to tolerate that is
//! `3f + 4` messages through one process per command, and it is why adding
//! acceptors or replicas makes a classic leader *slower*. Only the first
//! job has to be the leader's.
//!
//! # The decoupling
//!
//! The leader still allocates the slot and still opens the round, but
//! instead of fanning out it hands the very same `Accept` to a proxy
//! (`reply_to` names the proxy, `leader` still names the leader), and is
//! done until the `Commit` comes back. The proxy is a
//! [`paros_core::proposer::Rounds`] plus routing — the same Phase-2 tally
//! the proposer embeds, on another process — and it contributes nothing to
//! the decision: it votes on nothing, orders nothing, adopts no ballot, so
//! moving it cannot break agreement. Which proxy? `ProxyId(slot %
//! proxy_count)`, a pure function of the slot ([`ProxyId::of`]); a proxy
//! count of zero is exactly the colocated deployment of the other examples.
//!
//! # What the trace below shows
//!
//! 1. Node 1 leads. Three commands run **colocated** (the driver's
//!    override, [`Delegation::Colocated`]): the leader's message count per
//!    command is what a classic leader pays.
//! 2. Three commands run **delegated** ([`Delegation::Auto`]): the leader
//!    sends one `Accept` and receives one `Commit` per command — two
//!    messages, whatever the grid's width — and the two proxies alternate.
//!    (The leader is also an acceptor of two columns, and in that role it
//!    still accepts and answers; those are the acceptor's messages, counted
//!    apart.)
//! 3. A proxy dies holding a round. The leader re-delegates on every beat
//!    and, after its budget, **takes the round back** and runs it colocated:
//!    liveness under a dead proxy is the leader's, and the fallback is
//!    always the classic Phase 2.
//! 4. The leader hands off mid-round. The successor **re-delegates** the
//!    inherited round with `leader` naming itself, and the proxy — which
//!    still holds the round — refreshes the leader hint and completes it
//!    under the same ballot.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), §3.1; the frankenpaxos `ProxyLeader`
//! analysis in `docs/references/frankenpaxos/04-compartmentalization.md`.

use std::collections::BTreeMap;

use paros_core::{
    Ballot, ClientId, ClientSeq, ColocatedNode, Command, Config, Delegation, HardState, Message,
    NodeId, Party, ProxyId, ProxyLeader, QuorumSystem, Slot, Storage, Value,
};

/// Two rows of three: rows `{0, 1, 2}` and `{3, 4, 5}`, columns `{0, 3}`,
/// `{1, 4}` and `{2, 5}`.
const GRID: QuorumSystem = QuorumSystem::Grid { rows: 2, cols: 3 };
const NODES: u64 = 6;
const PROXIES: usize = 2;
/// The driver's take-back budget: re-delegations a proxy may swallow.
const TAKE_BACK_AFTER: u64 = 2;

/// An empty store: every node boots fresh.
struct FreshStore {
    config: Config,
}

impl Storage for FreshStore {
    fn initial_state(&self) -> (HardState, Config) {
        (HardState::default(), self.config.clone())
    }
    fn accepted(&self, _slot: Slot) -> Option<(Ballot, Command)> {
        None
    }
    fn first_slot(&self) -> Slot {
        Slot(0)
    }
    fn last_slot(&self) -> Slot {
        Slot(0)
    }
}

fn fresh(id: u64) -> ColocatedNode {
    let config = Config {
        id: NodeId(id),
        peers: (0..NODES).map(NodeId).collect(),
        quorum_system: GRID,
        proxy_count: PROXIES,
        ..Config::default()
    };
    ColocatedNode::new(&FreshStore { config })
}

/// A large `CheckQuorum` window: this example steps messages by hand.
const NO_CHECK_QUORUM: u64 = 1_000_000;

fn kind(m: &Message) -> &'static str {
    match m {
        Message::Accept { .. } => "Accept",
        Message::Accepted { .. } => "Accepted",
        Message::Commit { .. } => "Commit",
        Message::Nack { .. } => "Nack",
        Message::Heartbeat { .. } => "Heartbeat",
        Message::HeartbeatAck { .. } => "HeartbeatAck",
        Message::Relinquish { .. } => "Relinquish",
        _ => "other",
    }
}

fn show_party(p: Party) -> String {
    match p {
        Party::Node(n) => format!("node {}", n.0),
        Party::Proxy(p) => format!("proxy {}", p.0),
    }
}

/// The leader's messages per command, split by the role that sent or
/// received them.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
struct LeaderCost {
    /// Sent or received as the *leader*: the `Accept` it hands out (to a
    /// column or a proxy), the `Accepted`s it folds, the `Commit` it sends
    /// or the proxy's `Commit` it learns from.
    leader: usize,
    /// Sent or received as an *acceptor* of its columns: a proxy's fan-out
    /// reaching it, and the `Accepted` it answers.
    acceptor: usize,
}

/// The deployment: six nodes, two proxies, and the messages in flight.
struct Cluster {
    nodes: Vec<ColocatedNode>,
    proxies: Vec<Option<ProxyLeader>>,
    wire: Vec<(Party, Message)>,
    leader: NodeId,
    cost: LeaderCost,
}

impl Cluster {
    fn node(&mut self, id: NodeId) -> &mut ColocatedNode {
        let i = usize::try_from(id.0).expect("small id");
        &mut self.nodes[i]
    }

    fn proxy(&mut self, id: ProxyId) -> &mut Option<ProxyLeader> {
        let i = usize::try_from(id.0).expect("small id");
        &mut self.proxies[i]
    }

    /// Tally one message the leader sent or received.
    fn charge(&mut self, party: Party, msg: &Message) {
        if party != Party::Node(self.leader) {
            return;
        }
        match msg {
            // A proxy's fan-out reaching the leader as one of the column's
            // acceptors.
            Message::Accept { reply_to, .. } if *reply_to != Party::Node(self.leader) => {
                self.cost.acceptor += 1;
            }
            Message::Accept { .. } | Message::Accepted { .. } | Message::Commit { .. } => {
                self.cost.leader += 1;
            }
            _ => {}
        }
    }

    /// Drain `id`'s batch onto the wire, honouring the `Ready` contract.
    fn drain(&mut self, id: NodeId) {
        let pool: Vec<NodeId> = (0..NODES).map(NodeId).collect();
        let node = self.node(id);
        let ready = node.ready();
        let mut out = Vec::new();
        for (audience, m) in ready.messages() {
            if let Some(proxy) = audience.proxy() {
                out.push((Party::Proxy(proxy), m.clone()));
            }
            for to in audience.resolve(&pool, id) {
                out.push((Party::Node(to), m.clone()));
            }
        }
        ready.advance();
        node.advance_recovery();
        for (to, m) in &out {
            // What the leader sends: an `Accept` it hands out is the
            // leader's; an `Accepted` it answers a proxy with is the
            // acceptor's.
            if id == self.leader {
                match m {
                    Message::Accepted { .. } if matches!(to, Party::Proxy(_)) => {
                        self.cost.acceptor += 1;
                    }
                    Message::Accept { .. } | Message::Accepted { .. } | Message::Commit { .. } => {
                        self.cost.leader += 1;
                    }
                    _ => {}
                }
            }
        }
        self.wire.extend(out);
    }

    fn drain_proxy(&mut self, id: ProxyId) {
        let pool: Vec<NodeId> = (0..NODES).map(NodeId).collect();
        let Some(proxy) = self.proxy(id).as_mut() else {
            return;
        };
        let ready = proxy.ready();
        let mut out = Vec::new();
        for (audience, m) in ready.messages() {
            for to in audience.resolve_from_proxy(&pool) {
                out.push((Party::Node(to), m.clone()));
            }
        }
        ready.advance();
        self.wire.extend(out);
    }

    /// Deliver everything in flight for which `keep` holds, to quiescence.
    fn deliver(&mut self, keep: impl Fn(Party, &Message) -> bool) {
        loop {
            let Some(position) = self.wire.iter().position(|(to, m)| keep(*to, m)) else {
                return;
            };
            let (to, m) = self.wire.remove(position);
            self.charge(to, &m);
            match to {
                Party::Node(id) => {
                    self.node(id).step(m);
                    self.drain(id);
                }
                Party::Proxy(id) => {
                    if let Some(proxy) = self.proxy(id).as_mut() {
                        proxy.step(m);
                        self.drain_proxy(id);
                    }
                }
            }
        }
    }

    fn deliver_all(&mut self) {
        self.deliver(|_, _| true);
    }

    /// One driver beat at the leader: tick, re-send, take back.
    fn beat(&mut self) {
        let leader = self.leader;
        let node = self.node(leader);
        node.tick();
        node.resend_pending();
        node.take_back_delegated(TAKE_BACK_AFTER);
        self.drain(leader);
    }

    fn chosen_everywhere(&self, slot: Slot) -> bool {
        self.nodes
            .iter()
            .all(|n| n.replica().chosen_at(slot).is_some())
    }

    /// Propose one command at the leader under `delegation`, deliver to
    /// quiescence, and report the leader's cost for it.
    fn command(&mut self, seq: u64, text: &str, delegation: Delegation) -> (Slot, LeaderCost) {
        self.cost = LeaderCost::default();
        let leader = self.leader;
        let result = self.node(leader).propose_in(
            ClientId(7),
            ClientSeq(seq),
            Value(text.as_bytes().to_vec()),
            None,
            delegation,
        );
        let paros_core::ProposeResult::Accepted(slot) = result else {
            panic!("the leader admits the proposal: {result:?}");
        };
        self.drain(leader);
        self.deliver_all();
        assert!(self.chosen_everywhere(slot), "slot {} is chosen", slot.0);
        (slot, self.cost)
    }
}

fn main() {
    println!("== proxy leaders: the leader sequences, the proxies broadcast ==\n");
    let mut cluster = Cluster {
        nodes: (0..NODES).map(fresh).collect(),
        proxies: (0..PROXIES)
            .map(|p| {
                Some(ProxyLeader::new(
                    ProxyId(p as u64),
                    paros_core::AcceptorConfig::new((0..NODES).map(NodeId).collect(), GRID),
                ))
            })
            .collect(),
        wire: Vec::new(),
        leader: NodeId(1),
        cost: LeaderCost::default(),
    };
    elect(&mut cluster);
    let colocated = colocated_commands(&mut cluster);
    let delegated = delegated_commands(&mut cluster, colocated);
    assert!(
        delegated.leader < colocated.leader,
        "delegation takes the broadcast off the leader"
    );
    a_dead_proxy(&mut cluster);
    a_handoff_mid_round(&mut cluster);
    println!("\n== ok ==");
}

fn elect(cluster: &mut Cluster) {
    println!("-- 1. node 1 campaigns; six nodes in a 2 × 3 grid, two proxies");
    let leader = cluster.leader;
    cluster.node(leader).set_election_timeout(1);
    cluster.node(leader).tick();
    cluster.drain(leader);
    cluster.deliver_all();
    assert!(cluster.node(leader).is_leader(), "node 1 wins its election");
    cluster.node(leader).set_election_timeout(NO_CHECK_QUORUM);
    cluster.node(leader).tick();
    cluster.drain(leader);
    cluster.deliver_all();
}

/// Step 1: the classic leader's cost.
fn colocated_commands(cluster: &mut Cluster) -> LeaderCost {
    println!("\n-- 2. three commands run colocated (Delegation::Colocated)");
    let mut total = LeaderCost::default();
    for (seq, text) in [(1, "alpha"), (2, "bravo"), (3, "charlie")] {
        let (slot, cost) = cluster.command(seq, text, Delegation::Colocated);
        println!(
            "   slot {} = {text:?}: the leader sent or received {} messages as leader, {} as acceptor",
            slot.0, cost.leader, cost.acceptor
        );
        total.leader += cost.leader;
        total.acceptor += cost.acceptor;
    }
    assert!(
        cluster.node(cluster.leader).delegated_rounds().is_empty(),
        "nothing was delegated"
    );
    total
}

/// Step 2: the same commands through the proxies.
fn delegated_commands(cluster: &mut Cluster, colocated: LeaderCost) -> LeaderCost {
    println!("\n-- 3. three commands run delegated (Delegation::Auto: ProxyId(slot % 2))");
    let mut total = LeaderCost::default();
    for (seq, text) in [(4, "delta"), (5, "echo"), (6, "foxtrot")] {
        let (slot, cost) = cluster.command(seq, text, Delegation::Auto);
        let proxy = ProxyId::of(slot, PROXIES).expect("two proxies");
        assert_eq!(
            cost.leader, 2,
            "one Accept out to the proxy, one Commit back: two messages"
        );
        let decided = cluster
            .proxy(proxy)
            .as_ref()
            .expect("up")
            .counters()
            .decided;
        println!(
            "   slot {} = {text:?} via proxy {}: the leader sent or received {} messages as leader, {} as acceptor (proxy {} has decided {decided})",
            slot.0, proxy.0, cost.leader, cost.acceptor, proxy.0
        );
        total.leader += cost.leader;
        total.acceptor += cost.acceptor;
    }
    println!(
        "   colocated: {} leader messages for three commands; delegated: {} — two per command, whatever the width",
        colocated.leader, total.leader
    );
    total
}

/// Step 3: a proxy dies with a round; the leader takes it back.
fn a_dead_proxy(cluster: &mut Cluster) {
    println!(
        "\n-- 4. proxy 1 dies holding the next slot; the leader re-delegates, then takes it back"
    );
    let leader = cluster.leader;
    let result = cluster.node(leader).propose_in(
        ClientId(7),
        ClientSeq(7),
        Value(b"golf".to_vec()),
        None,
        Delegation::To(ProxyId(1)),
    );
    let paros_core::ProposeResult::Accepted(slot) = result else {
        panic!("admitted");
    };
    cluster.drain(leader);
    // The delegation is on the wire; proxy 1 dies before it lands.
    *cluster.proxy(ProxyId(1)) = None;
    cluster.deliver_all();
    assert_eq!(
        cluster.node(leader).delegated_rounds(),
        vec![(slot, ProxyId(1))],
        "the round is delegated and undecided"
    );
    println!(
        "   slot {}: delegated to proxy 1, which is dead — nothing is chosen",
        slot.0
    );
    let mut beats = 0;
    while !cluster.node(leader).delegated_rounds().is_empty() {
        cluster.beat();
        cluster.deliver_all();
        beats += 1;
        assert!(
            beats <= TAKE_BACK_AFTER + 1,
            "the take-back budget is honoured"
        );
    }
    println!(
        "   after {beats} beats (budget {TAKE_BACK_AFTER}) the leader took slot {} back and ran it colocated",
        slot.0
    );
    assert!(cluster.chosen_everywhere(slot));
    println!("   slot {} = \"golf\" chosen everywhere", slot.0);
    // Proxy 1 comes back, empty.
    *cluster.proxy(ProxyId(1)) = Some(ProxyLeader::new(
        ProxyId(1),
        paros_core::AcceptorConfig::new((0..NODES).map(NodeId).collect(), GRID),
    ));
}

/// Step 4: a handoff with a delegated round in flight.
fn a_handoff_mid_round(cluster: &mut Cluster) {
    println!("\n-- 5. node 1 hands off to node 4 with the next slot delegated and in flight");
    let leader = cluster.leader;
    let result = cluster.node(leader).propose_in(
        ClientId(7),
        ClientSeq(8),
        Value(b"hotel".to_vec()),
        None,
        Delegation::Auto,
    );
    let paros_core::ProposeResult::Accepted(slot) = result else {
        panic!("admitted");
    };
    // The core's own rule, which a handoff successor re-derives without
    // being told: the same proxy for the same slot.
    let proxy_id = ProxyId::of(slot, PROXIES).expect("two proxies");
    cluster.drain(leader);
    // The proxy receives the delegation and fans out, but no Accepted
    // reaches it yet: the round is open at the proxy, undecided.
    cluster.deliver(|to, m| matches!(to, Party::Proxy(_)) && matches!(m, Message::Accept { .. }));
    let proxy = cluster.proxy(proxy_id).as_ref().expect("up");
    assert_eq!(proxy.delegator(slot), Some(NodeId(1)));
    println!(
        "   proxy {} holds slot {} for {}",
        proxy_id.0,
        slot.0,
        show_party(Party::Node(proxy.delegator(slot).expect("open")))
    );
    let receipt = cluster
        .node(leader)
        .relinquish_to(NodeId(4))
        .expect("a settled leader hands off");
    assert_eq!(receipt.pending, 1, "the slot travels as a pending round");
    cluster.drain(leader);
    cluster.leader = NodeId(4);
    // The Relinquish lands first; the successor re-delegates the slot to
    // the proxy `slot % proxy_count` names — the one already holding it.
    cluster.deliver(|_, m| matches!(m, Message::Relinquish { .. }));
    assert!(cluster.node(NodeId(4)).is_leader());
    assert_eq!(
        cluster.node(NodeId(4)).delegated_rounds(),
        vec![(slot, proxy_id)],
        "the successor re-delegated the inherited round to the same proxy"
    );
    cluster.deliver_all();
    let ballot = cluster.node(NodeId(4)).ballot();
    assert_eq!(ballot.node, NodeId(1), "the same ballot, minted by node 1");
    assert!(cluster.chosen_everywhere(slot));
    let proxy = cluster.proxy(proxy_id).as_ref().expect("up").counters();
    println!(
        "   node 4 leads under ballot {}.{}; proxy {} refreshed the hint (refanned {}) and decided slot {} (decided {})",
        ballot.round, ballot.node.0, proxy_id.0, proxy.refanned, slot.0, proxy.decided
    );
    assert!(proxy.refanned >= 1, "the re-delegation re-fanned-out");
    let (kinds, _) = summary(cluster);
    println!("   messages left in flight: {kinds:?}");
}

fn summary(cluster: &Cluster) -> (BTreeMap<&'static str, usize>, usize) {
    let mut kinds = BTreeMap::new();
    for (_, m) in &cluster.wire {
        *kinds.entry(kind(m)).or_default() += 1;
    }
    (kinds, cluster.wire.len())
}
