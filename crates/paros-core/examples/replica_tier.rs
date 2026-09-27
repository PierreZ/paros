//! **The replica tier: acceptors vote, replicas apply.**
//!
//! Run it: `cargo run -p paros-core --example replica_tier`
//!
//! The lesson after `proxy_leader`, and the third deployment in this
//! crate. Every other example colocates the roles: each node is an
//! acceptor *and* a replica, so each node votes on the log and also runs
//! the application over it. Here the two are split onto different
//! processes:
//!
//! - three **bare acceptors** — [`ColocatedNode`]s with
//!   [`Application::Shed`] — vote, learn and keep the chosen log, and apply
//!   nothing;
//! - two **replicas** — [`ReplicaNode`]s — learn the chosen log from the
//!   leader's `Commit`s and apply it, and never vote.
//!
//! # Why split them (Compartmentalized Paxos §3.3)
//!
//! Adding an acceptor makes every quorum bigger, so it makes the protocol
//! *slower*. Adding a replica only adds a process that learns, so it adds
//! application throughput and read capacity for free. With the two roles on
//! the same process they cannot be scaled apart: this deployment can. It is
//! also the shape operators know from `FoundationDB`: a small durable log
//! tier plus a storage tier that runs the application.
//!
//! # What a bare acceptor keeps
//!
//! It stays a **learner**. It keeps its chosen index and the chosen values,
//! because Paxos needs them: an acceptor only truncates what is chosen, a
//! recovering leader skips what is chosen, garbage collection counts chosen
//! indices, and a lagging replica is healed *from* the acceptors. What it
//! sheds is only what the application needed: the `committed` output, the
//! application repair and the application snapshot. The full analysis is in
//! the module doc of `paros_core::replica_node`.
//!
//! # What the trace below shows
//!
//! 1. Node 0 leads. Six commands are chosen by the three acceptors alone.
//! 2. The acceptors hold the chosen prefix and hand the application nothing.
//! 3. One `Commit` to replica 11 is lost. Replica 11 applies up to the hole,
//!    then the leader's next beat shows it is behind, it asks for the
//!    missing range (`CatchUpRequest`), and the answer
//!    (`CatchUpResponse`) lets it apply the rest in order.
//! 4. Each slot has one **reply owner**, `slot % replica_count`
//!    ([`Config::reply_owner`]): the replica that answers the client for
//!    that slot, so each replica sends half of the replies.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), §2.3 and §3.3.

use paros_core::{
    Application, Audience, Ballot, ClientId, ClientSeq, ColocatedNode, Command, Config, HardState,
    Message, NodeId, ReplicaId, ReplicaNode, Slot, Storage, Value,
};

const ACCEPTORS: [u64; 3] = [0, 1, 2];
const REPLICAS: [u64; 2] = [10, 11];
/// A large `CheckQuorum` window: this example steps messages by hand.
const NO_CHECK_QUORUM: u64 = 1_000_000;

/// An empty store: every process boots fresh.
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

/// The deployment data every process shares: the acceptors, the replica
/// count and, for the acceptors, that they shed the application.
fn config(id: u64, application: Application) -> Config {
    Config {
        id: NodeId(id),
        peers: ACCEPTORS.iter().copied().map(NodeId).collect(),
        replica_count: REPLICAS.len(),
        application,
        ..Config::default()
    }
}

fn kind(m: &Message) -> &'static str {
    match m {
        Message::Prepare { .. } => "Prepare",
        Message::Promise { .. } => "Promise",
        Message::Accept { .. } => "Accept",
        Message::Accepted { .. } => "Accepted",
        Message::Commit { .. } => "Commit",
        Message::Heartbeat { .. } => "Heartbeat",
        Message::HeartbeatAck { .. } => "HeartbeatAck",
        Message::CatchUpRequest { .. } => "CatchUpRequest",
        Message::CatchUpResponse { .. } => "CatchUpResponse",
        _ => "other",
    }
}

/// The deployment: three bare acceptors, two replicas, what each replica
/// applied, and how many entries each acceptor handed an application.
struct Deployment {
    acceptors: Vec<ColocatedNode>,
    replicas: Vec<ReplicaNode>,
    applied: Vec<Vec<(Slot, Command)>>,
    acceptor_committed: usize,
    /// Print each delivery?
    verbose: bool,
}

impl Deployment {
    fn new() -> Self {
        Self {
            acceptors: ACCEPTORS
                .iter()
                .map(|id| {
                    ColocatedNode::new(&FreshStore {
                        config: config(*id, Application::Shed),
                    })
                })
                .collect(),
            replicas: REPLICAS
                .iter()
                .map(|id| {
                    ReplicaNode::new(&FreshStore {
                        config: config(*id, Application::Colocated),
                    })
                })
                .collect(),
            applied: vec![Vec::new(); REPLICAS.len()],
            acceptor_committed: 0,
            verbose: false,
        }
    }

    fn replica_index(to: NodeId) -> Option<usize> {
        REPLICAS.iter().position(|id| NodeId(*id) == to)
    }

    /// Drain acceptor `i`. The deployment map: `Learners` reaches the other
    /// acceptors *and* both replicas; everything else resolves as usual.
    fn drain_acceptor(&mut self, i: usize) -> Vec<(NodeId, Message)> {
        let pool: Vec<NodeId> = self.acceptors[i].config().pool().to_vec();
        let me = self.acceptors[i].config().id;
        let ready = self.acceptors[i].ready();
        self.acceptor_committed += ready.committed().len();
        let mut out = Vec::new();
        for (audience, msg) in ready.messages() {
            for to in audience.resolve(&pool, me) {
                out.push((to, msg.clone()));
            }
            if *audience == Audience::Learners {
                for id in REPLICAS {
                    out.push((NodeId(id), msg.clone()));
                }
            }
        }
        ready.advance();
        out
    }

    /// Drain replica `r`: its writes would be persisted here, its
    /// `committed` goes to the application, its messages to one node each.
    fn drain_replica(&mut self, r: usize) -> Vec<(NodeId, Message)> {
        let ready = self.replicas[r].ready();
        self.applied[r].extend(ready.committed().iter().cloned());
        let out = ready
            .messages()
            .iter()
            .filter_map(|(audience, msg)| match audience {
                Audience::Node(to) => Some((*to, msg.clone())),
                _ => None,
            })
            .collect();
        ready.advance();
        out
    }

    /// Deliver to quiescence, dropping what `keep` refuses.
    fn deliver(
        &mut self,
        mut queue: Vec<(NodeId, Message)>,
        keep: impl Fn(NodeId, &Message) -> bool,
    ) {
        queue.reverse();
        while !queue.is_empty() {
            let (to, msg) = queue.remove(0);
            if !keep(to, &msg) {
                if self.verbose {
                    println!("    x {:<15} to {:>2}  (lost)", kind(&msg), to.0);
                }
                continue;
            }
            if self.verbose {
                println!("    > {:<15} to {:>2}", kind(&msg), to.0);
            }
            if let Some(r) = Self::replica_index(to) {
                self.replicas[r].step(msg);
                queue.extend(self.drain_replica(r));
            } else {
                let i = usize::try_from(to.0).expect("an acceptor index");
                self.acceptors[i].step(msg);
                queue.extend(self.drain_acceptor(i));
            }
        }
    }

    fn applied_slots(&self, r: usize) -> Vec<u64> {
        self.applied[r].iter().map(|(s, _)| s.0).collect()
    }
}

fn main() {
    let mut d = Deployment::new();

    println!("== 1. node 0 is elected by the three acceptors");
    d.acceptors[0].set_election_timeout(1);
    d.acceptors[0].tick();
    let q = d.drain_acceptor(0);
    d.deliver(q, |_, _| true);
    assert!(d.acceptors[0].is_leader());
    d.acceptors[0].set_election_timeout(NO_CHECK_QUORUM);
    println!("   leader: node 0, ballot {:?}", d.acceptors[0].ballot());

    println!("\n== 2. six commands; the Commit for slot 2 never reaches replica 11");
    let lost = |to: NodeId, m: &Message| {
        !(to == NodeId(11) && matches!(m, Message::Commit { slot: Slot(2), .. }))
    };
    for seq in 1..=6_u64 {
        let _ = d.acceptors[0].propose(
            ClientId(7),
            ClientSeq(seq),
            Value(format!("cmd-{seq}").into_bytes()),
        );
        d.verbose = seq == 3;
        if d.verbose {
            println!("   the third command, message by message:");
        }
        let q = d.drain_acceptor(0);
        d.deliver(q, lost);
    }
    d.verbose = false;

    println!("\n== 3. the acceptors: a chosen prefix, no application");
    for n in &d.acceptors {
        println!(
            "   acceptor {}: chosen index {:?}, {} records, applied nothing",
            n.config().id.0,
            n.replica().chosen_index().map(|s| s.0),
            n.acceptor().records().len()
        );
        assert_eq!(n.replica().chosen_index(), Some(Slot(5)));
        assert_eq!(n.acceptor().records().len(), 6);
    }
    assert_eq!(
        d.acceptor_committed, 0,
        "a bare acceptor hands the application nothing"
    );

    println!("\n== 4. the replicas: replica 11 stops at the hole");
    for (r, id) in REPLICAS.iter().enumerate() {
        println!("   replica {id}: applied slots {:?}", d.applied_slots(r));
    }
    assert_eq!(d.applied_slots(0), vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(d.applied_slots(1), vec![0, 1]);
    assert_eq!(
        d.replicas[1].replica().chosen_gap(),
        Some((Slot(2), Slot(5))),
        "replica 11 knows slots 3..=5 are chosen, above the hole at 2"
    );

    println!("\n== 5. the leader beats; replica 11 sees it is behind and catches up");
    d.verbose = true;
    d.acceptors[0].tick();
    let q = d.drain_acceptor(0);
    d.deliver(q, |_, _| true);
    d.verbose = false;
    println!("   replica 11: applied slots {:?}", d.applied_slots(1));
    assert_eq!(d.applied_slots(1), vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(d.applied[0], d.applied[1], "one order on every replica");
    // The one message a replica ignored: the candidate's proactive
    // `CatchUpRequest` to every learner — a replica serves no peer.
    assert_eq!(d.replicas[0].counters().ignored, 1);

    println!("\n== 6. who replies to the client for each slot");
    for slot in 0..6 {
        let owner = d.replicas[0]
            .reply_owner(Slot(slot))
            .expect("a deployment with replicas has an owner per slot");
        println!(
            "   slot {slot}: {owner:?} (replica {})",
            REPLICAS[usize::try_from(owner.0).expect("a replica rank")]
        );
        assert_eq!(owner, ReplicaId(slot % 2));
    }
    println!("\nEach replica answers half of the clients; the acceptors answer none.");
}
