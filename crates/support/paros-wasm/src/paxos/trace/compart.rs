//! The compartmentalized page (#943 (compartmentalized diagram)): the roles
//! of Compartmentalized Paxos, each on its own lane.
//!
//! The game's worlds colocate every role in one node, so this page has its
//! own small driver, written as `paros-core`'s `proxy_leader` and
//! `replica_tier` examples are: three [`ColocatedNode`]s (the leader and the
//! acceptors), two [`ProxyLeader`]s, two [`ReplicaNode`]s and one
//! [`Batcher`]. The driver only moves messages: every message, every state
//! line and every quorum answer is read off those roles. The same write runs
//! on the colocated deployment too, so a reader sees which work left the
//! leader.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;

use paros_core::batcher::{Batch, BatchLimits, Batcher, Unbatcher};
use paros_core::contract::{Audience, MemStorage};
use paros_core::journal::{Command, Control, Entry, JournalIdentifier, Outcome, Seq, WriterMode};
use paros_core::membership::{AcceptorConfig, ProxyId, QuorumSystem};
use paros_core::node::{Delegation, ProposeResult};
use paros_core::{
    ColocatedNode, Config, Limits, Message, NodeId, ProxyLeader, ReplicaNode, Slot, Value,
};

use super::content::{self, Field};
use super::words::{self, Stage};
use super::{Chapter, Lane, LogCell, Scenario, Step, Tally, Trace, message_label};
use crate::paxos::view::{message_view, show_ballot, value_text};

/// The acceptors. Node 0 leads.
const NODES: [u64; 3] = [0, 1, 2];
/// The proxy leaders: a slot goes to proxy `slot % 2`.
const PROXIES: usize = 2;
/// The replicas.
const REPLICAS: usize = 2;
/// A replica's node id is this plus its rank: replicas and acceptors share
/// one id space on the wire.
const REPLICA_BASE: u64 = 10;
/// The leader's take-back budget: re-delegations a dead proxy may swallow.
const TAKE_BACK_AFTER: u64 = 2;
/// A large `CheckQuorum` window: the driver delivers by hand.
const NO_CHECK_QUORUM: u64 = 1_000_000;
/// The most messages one run may move before it is called a loop.
const BUDGET: usize = 512;
/// The most slots a lane's state looks at.
const SLOTS: u64 = 16;

const _: () = assert!(REPLICA_BASE > NODES.len() as u64);
const _: () = assert!(TAKE_BACK_AFTER >= 1);

/// One party of the page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Who {
    Node(u64),
    Proxy(u64),
    Replica(u64),
    Batcher,
}

impl Who {
    /// The party behind a node id on the wire.
    fn of(id: NodeId) -> Self {
        if id.0 >= REPLICA_BASE {
            Who::Replica(id.0 - REPLICA_BASE)
        } else {
            Who::Node(id.0)
        }
    }

    fn lane(self) -> String {
        match self {
            Who::Node(id) => format!("n{id}"),
            Who::Proxy(id) => format!("p{id}"),
            Who::Replica(id) => format!("r{id}"),
            Who::Batcher => "b0".to_string(),
        }
    }

    fn name(self) -> String {
        match self {
            Who::Node(id) => format!("node {id}"),
            Who::Proxy(id) => format!("proxy leader {id}"),
            Who::Replica(id) => format!("replica {id}"),
            Who::Batcher => "the batcher".to_string(),
        }
    }

    fn number(self) -> u64 {
        match self {
            Who::Node(id) | Who::Proxy(id) | Who::Replica(id) => id,
            Who::Batcher => 0,
        }
    }
}

/// The parties in lane order: the batcher, the leader, the proxy leaders,
/// the other acceptors, the replicas.
fn parties() -> Vec<Who> {
    let mut parties = vec![Who::Batcher, Who::Node(NODES[0])];
    parties.extend((0..PROXIES as u64).map(Who::Proxy));
    parties.extend(NODES[1..].iter().map(|id| Who::Node(*id)));
    parties.extend((0..REPLICAS as u64).map(Who::Replica));
    parties
}

/// The lanes of the page.
fn lanes() -> Vec<Lane> {
    parties()
        .into_iter()
        .map(|who| {
            let role = match who {
                Who::Node(_) => "node",
                Who::Proxy(_) => "proxy",
                Who::Replica(_) => "replica",
                Who::Batcher => "batcher",
            };
            let label = match who {
                Who::Batcher => "batcher".to_string(),
                other => other.name(),
            };
            Lane {
                id: who.lane(),
                label,
                role: role.to_string(),
                cell: None,
            }
        })
        .collect()
}

/// Which deployment a run uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    /// The leader sends every Accept and counts every vote; the acceptors
    /// learn the log.
    Colocated,
    /// Two proxy leaders send the Accepts and count the votes; two replicas
    /// learn the log and serve reads.
    Compartmentalized,
}

fn config(id: u64, layout: Layout) -> Config {
    let split = layout == Layout::Compartmentalized;
    Config {
        peers: NODES.iter().copied().map(NodeId).collect(),
        proxy_count: if split { PROXIES } else { 0 },
        replica_count: if split { REPLICAS } else { 0 },
        // Many writers, one journal: the batcher's journal (#145 (batchers)).
        writer_mode: WriterMode::Multi,
        ..Config::new(NodeId(id), JournalIdentifier::UNSET)
    }
}

/// The deployment of one run and the steps it recorded.
struct Run {
    layout: Layout,
    nodes: Vec<ColocatedNode>,
    proxies: Vec<Option<ProxyLeader>>,
    replicas: Vec<ReplicaNode>,
    batcher: Batcher<u64>,
    wire: VecDeque<(Who, Who, Message)>,
    steps: Vec<Step>,
    start: Vec<Vec<String>>,
    start_logs: Vec<Vec<LogCell>>,
    /// The votes each counter heard, by (counter, ballot, slot).
    heard: BTreeMap<(String, String, Slot), BTreeSet<NodeId>>,
    /// The reads each replica served, as lines of its state.
    reads: Vec<Vec<String>>,
    /// The read indexes a replica served on the message just delivered.
    served: Vec<Option<Slot>>,
    /// The (counter, slot) pairs whose votes already made a quorum.
    decided: BTreeSet<(String, Slot)>,
    /// The batch on its way to the leader, and its slot once proposed.
    batch: Option<(Batch<u64>, Option<Slot>)>,
    recording: bool,
}

impl Run {
    /// A deployment whose node 0 already leads: the election is the Multi-
    /// Paxos page's, so it is not recorded here.
    fn new(layout: Layout) -> Self {
        let split = layout == Layout::Compartmentalized;
        let acceptors = AcceptorConfig::new(
            NODES.iter().copied().map(NodeId).collect(),
            QuorumSystem::Majority,
        );
        let mut run = Self {
            layout,
            nodes: NODES
                .iter()
                .map(|id| {
                    ColocatedNode::new(&MemStorage::new(config(*id, layout)), Limits::DEFAULT)
                })
                .collect(),
            proxies: if split {
                (0..PROXIES as u64)
                    .map(|p| Some(ProxyLeader::new(ProxyId(p), acceptors.clone())))
                    .collect()
            } else {
                Vec::new()
            },
            replicas: if split {
                (0..REPLICAS as u64)
                    .map(|r| {
                        ReplicaNode::new(
                            &MemStorage::new(config(REPLICA_BASE + r, layout)),
                            Limits::DEFAULT,
                        )
                    })
                    .collect()
            } else {
                Vec::new()
            },
            batcher: Batcher::new(BatchLimits {
                max_records: 8,
                max_bytes: 1024,
                flush_records: 2,
            }),
            wire: VecDeque::new(),
            steps: Vec::new(),
            start: Vec::new(),
            start_logs: Vec::new(),
            heard: BTreeMap::new(),
            reads: vec![Vec::new(); REPLICAS],
            served: Vec::new(),
            decided: BTreeSet::new(),
            batch: None,
            recording: false,
        };
        let leader = &mut run.nodes[0];
        leader.set_election_timeout(1);
        leader.tick();
        run.drain(Who::Node(0));
        run.settle(|_, _, _| false);
        assert!(run.nodes[0].is_leader(), "node 0 wins its election");
        run.nodes[0].set_election_timeout(NO_CHECK_QUORUM);
        run.nodes[0].tick();
        run.drain(Who::Node(0));
        run.settle(|_, _, _| false);
        assert!(run.wire.is_empty(), "the election settles");
        run.start = run.state();
        run.start_logs = run.logs();
        run.recording = true;
        run
    }

    fn pool() -> Vec<NodeId> {
        NODES.iter().copied().map(NodeId).collect()
    }

    fn split(&self) -> bool {
        self.layout == Layout::Compartmentalized
    }

    /// Where a message to `audience` goes. A learner is every acceptor and,
    /// in the compartmentalized deployment, every replica.
    fn learners(&self, audience: &Audience, out: &mut Vec<Who>) {
        if *audience == Audience::Learners && self.split() {
            out.extend((0..REPLICAS as u64).map(Who::Replica));
        }
    }

    /// Put `who`'s batch on the wire, honouring the `Ready` contract.
    fn drain(&mut self, who: Who) {
        let pool = Self::pool();
        let mut out: Vec<(Who, Message)> = Vec::new();
        match who {
            Who::Node(id) => {
                let index = usize::try_from(id).expect("a small id");
                let ready = self.nodes[index].ready();
                let mut sent = Vec::new();
                for (audience, message) in ready.messages() {
                    let mut to: Vec<Who> = Vec::new();
                    if let Some(proxy) = audience.proxy() {
                        to.push(Who::Proxy(proxy.0));
                    }
                    to.extend(audience.resolve(&pool, NodeId(id)).into_iter().map(Who::of));
                    sent.push((audience.clone(), message.clone(), to));
                }
                ready.advance();
                self.nodes[index].advance_recovery();
                for (audience, message, mut to) in sent {
                    self.learners(&audience, &mut to);
                    out.extend(to.into_iter().map(|to| (to, message.clone())));
                }
            }
            Who::Proxy(id) => {
                let index = usize::try_from(id).expect("a small id");
                let Some(proxy) = self.proxies[index].as_mut() else {
                    return;
                };
                let ready = proxy.ready();
                let sent: Vec<(Audience, Message)> = ready.messages().to_vec();
                ready.advance();
                for (audience, message) in sent {
                    let mut to: Vec<Who> = audience
                        .resolve_from_proxy(&pool)
                        .into_iter()
                        .map(Who::of)
                        .collect();
                    self.learners(&audience, &mut to);
                    out.extend(to.into_iter().map(|to| (to, message.clone())));
                }
            }
            Who::Replica(rank) => {
                let index = usize::try_from(rank).expect("a small rank");
                let ready = self.replicas[index].ready();
                let served: Vec<Option<Slot>> =
                    ready.read_states().iter().map(|read| read.index).collect();
                for (audience, message) in ready.messages() {
                    if let Audience::Node(to) = audience {
                        out.push((Who::of(*to), message.clone()));
                    }
                }
                ready.advance();
                self.replicas[index].advance_recovery();
                self.reads[index].extend(served.iter().map(|index| match index {
                    Some(slot) => format!("read served at slot {}", slot.0),
                    None => "read served: empty log".to_string(),
                }));
                self.served.extend(served);
            }
            Who::Batcher => {}
        }
        self.wire
            .extend(out.into_iter().map(|(to, message)| (who, to, message)));
    }

    /// Whether `who` is up to take a message.
    fn up(&self, who: Who) -> bool {
        match who {
            Who::Proxy(id) => self.proxies[usize::try_from(id).expect("a small id")].is_some(),
            _ => true,
        }
    }

    /// Move every message in flight, in order, to quiescence. `lose` names
    /// the messages the network loses.
    fn settle(&mut self, loses: impl Fn(Who, Who, &Message) -> bool) {
        let mut moved = 0;
        while let Some((from, to, message)) = self.wire.pop_front() {
            moved += 1;
            assert!(moved <= BUDGET, "a run settles");
            let lost = loses(from, to, &message) || !self.up(to);
            if !lost {
                match to {
                    Who::Node(id) => {
                        self.nodes[usize::try_from(id).expect("a small id")].step(message.clone());
                    }
                    Who::Proxy(id) => {
                        if let Some(proxy) = &mut self.proxies[usize::try_from(id).expect("small")]
                        {
                            proxy.step(message.clone());
                        }
                    }
                    Who::Replica(rank) => {
                        self.replicas[usize::try_from(rank).expect("a small rank")]
                            .step(message.clone());
                    }
                    Who::Batcher => unreachable!("nobody sends to the batcher on the wire"),
                }
                self.drain(to);
            }
            if self.recording {
                self.record(from, to, &message, lost);
            }
        }
    }

    /// One message step.
    fn record(&mut self, from: Who, to: Who, message: &Message, lost: bool) {
        let view = message_view(
            0,
            from.number(),
            to.number(),
            0,
            message,
            QuorumSystem::Majority,
        );
        let carried = content::node_message(message, true);
        let mut says = says(from, to, message);
        if lost {
            if self.up(to) {
                says.push_str(" The network loses it.");
            } else {
                let _ = write!(
                    says,
                    " {} is down, so the message is lost.",
                    cap(&to.name())
                );
            }
        }
        let tally = if lost {
            None
        } else {
            self.count(from, to, message)
        };
        if let Some(tally) = &tally
            && let Some(slot) = tally.slot
            && tally.quorum
            && self.decided.insert((tally.at.clone(), Slot(slot)))
        {
            let _ = write!(
                says,
                " {} has votes from a majority, so slot {slot} is chosen.",
                cap(&to.name())
            );
        }
        for index in std::mem::take(&mut self.served) {
            let _ = match index {
                Some(slot) => write!(
                    says,
                    " {} has answers from a majority. It has learned slot {}, so it serves the \
                     read now.",
                    cap(&to.name()),
                    slot.0
                ),
                None => write!(
                    says,
                    " {} has answers from a majority, and no slot was voted, so it serves the \
                     read now.",
                    cap(&to.name())
                ),
            };
        }
        self.steps.push(Step {
            kind: if lost { "drop" } else { "deliver" }.to_string(),
            from: Some(from.lane()),
            to: Some(to.lane()),
            label: message_label(&view, true),
            phase: Some(view.phase.clone()),
            reply: view.reply,
            stage: words::stage_of(&view, true),
            says,
            notes: Vec::new(),
            fields: carried.fields,
            voice: carried.voice,
            state: self.state(),
            chosen: self.chosen(),
            tally,
            logs: self.logs(),
            broken: None,
        });
    }

    /// An event step: something a party does on its own.
    fn event(&mut self, label: &str, says: String) {
        self.steps.push(Step {
            kind: "event".to_string(),
            from: None,
            to: None,
            label: label.to_string(),
            phase: None,
            reply: false,
            stage: None,
            says,
            notes: Vec::new(),
            fields: Vec::new(),
            voice: String::new(),
            state: self.state(),
            chosen: self.chosen(),
            tally: None,
            logs: self.logs(),
            broken: None,
        });
    }

    /// The votes the receiver of an `Accepted` holds for its slot.
    fn count(&mut self, from: Who, to: Who, message: &Message) -> Option<Tally> {
        let Message::Accepted { ballot, slot, .. } = message else {
            return None;
        };
        let Who::Node(acceptor) = from else {
            return None;
        };
        let ballot_text = show_ballot(*ballot);
        let key = (to.lane(), ballot_text.clone(), *slot);
        let heard = self.heard.entry(key).or_default();
        heard.insert(NodeId(acceptor));
        // A colocated leader votes for its own round without a message.
        if let Who::Node(leader) = to {
            heard.insert(NodeId(leader));
        }
        let votes = heard.clone();
        let acceptors = self.nodes[0].acceptors().clone();
        let members: Vec<String> = parties()
            .into_iter()
            .filter(|who| matches!(who, Who::Node(id) if votes.contains(&NodeId(*id))))
            .map(Who::lane)
            .collect();
        Some(Tally {
            what: "votes".to_string(),
            at: to.lane(),
            ballot: ballot_text,
            slot: Some(slot.0),
            members,
            need: format!("{} of {}", NODES.len() / 2 + 1, NODES.len()),
            quorum: acceptors.has_phase2_quorum(&votes),
            line: None,
        })
    }

    /// A few short lines per lane, in lane order.
    fn state(&self) -> Vec<Vec<String>> {
        parties()
            .into_iter()
            .map(|who| match who {
                Who::Node(id) => self.node_lines(id),
                Who::Proxy(id) => self.proxy_lines(id),
                Who::Replica(rank) => {
                    let index = usize::try_from(rank).expect("small");
                    let replica = self.replicas.get(index);
                    let mut lines = vec![learned(
                        replica.and_then(|replica| replica.replica().chosen_index()),
                    )];
                    lines.extend(self.reads[index].iter().cloned());
                    lines
                }
                Who::Batcher => match self.batcher.pending() {
                    0 => vec!["empty".to_string()],
                    1 => vec!["1 write".to_string()],
                    n => vec![format!("{n} writes")],
                },
            })
            .collect()
    }

    fn node_lines(&self, id: u64) -> Vec<String> {
        let node = &self.nodes[usize::try_from(id).expect("small")];
        // Short lines: on a phone eight cards share the width.
        let mut lines = Vec::new();
        if node.is_leader() {
            lines.push(format!("leader {}", show_ballot(node.ballot())));
        }
        lines.push(match node.replica().chosen_index() {
            Some(slot) => format!("chosen to slot {}", slot.0),
            None => "nothing chosen".to_string(),
        });
        for (slot, _) in node.delegated_rounds() {
            lines.push(format!("slot {} out", slot.0));
        }
        lines
    }

    fn proxy_lines(&self, id: u64) -> Vec<String> {
        let Some(proxy) = &self
            .proxies
            .get(usize::try_from(id).expect("small"))
            .and_then(Option::as_ref)
        else {
            return vec![if self.split() { "down" } else { "" }.to_string()];
        };
        let mut lines: Vec<String> = (0..SLOTS)
            .map(Slot)
            .filter_map(|slot| {
                proxy
                    .delegator(slot)
                    .map(|_| format!("counts slot {}", slot.0))
            })
            .collect();
        if lines.is_empty() {
            lines.push("idle".to_string());
        }
        let decided = proxy.counters().decided;
        if decided > 0 {
            lines.push(format!("decided {decided}"));
        }
        lines
    }

    /// The log of each lane, in lane order.
    fn logs(&self) -> Vec<Vec<LogCell>> {
        parties()
            .into_iter()
            .map(|who| match who {
                Who::Node(id) => {
                    let node = &self.nodes[usize::try_from(id).expect("small")];
                    node.acceptor()
                        .records()
                        .iter()
                        .map(|(slot, (_, command))| {
                            cell(*slot, command, node.replica().chosen_at(*slot).is_some())
                        })
                        .collect()
                }
                Who::Replica(rank) => self
                    .replicas
                    .get(usize::try_from(rank).expect("small"))
                    .map(|replica| {
                        (0..SLOTS)
                            .map(Slot)
                            .filter_map(|slot| {
                                replica
                                    .replica()
                                    .chosen_at(slot)
                                    .map(|command| cell(slot, command, true))
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                Who::Proxy(_) | Who::Batcher => Vec::new(),
            })
            .collect()
    }

    /// What the acceptors hold chosen: `slot 0 = alpha`.
    fn chosen(&self) -> Vec<String> {
        let mut chosen: BTreeMap<Slot, String> = BTreeMap::new();
        for node in &self.nodes {
            for slot in (0..SLOTS).map(Slot) {
                if let Some(command) = node.replica().chosen_at(slot) {
                    chosen.insert(slot, value_text(command));
                }
            }
        }
        chosen
            .into_iter()
            .map(|(slot, value)| format!("slot {} = {value}", slot.0))
            .collect()
    }

    /// The client (or the batcher) hands node 0 a write.
    fn write(&mut self, records: &[&str], delegation: Delegation) -> Slot {
        let result = self.nodes[0].propose_in(
            Entry {
                leader: paros_core::journal::LeaderUuid::UNSET,
                seq: Seq(0),
                records: records
                    .iter()
                    .map(|record| Value(record.as_bytes().to_vec()))
                    .collect(),
            },
            None,
            delegation,
        );
        let ProposeResult::Accepted(slot) = result else {
            panic!("the leader admits the write: {result:?}");
        };
        self.drain(Who::Node(0));
        slot
    }

    /// A client writes `value` to node 0, and the run settles.
    fn client_writes(&mut self, value: &str) -> Slot {
        let delegation = if self.split() {
            Delegation::Auto
        } else {
            Delegation::Colocated
        };
        let slot = self.write(&[value], delegation);
        let says = if self.split() {
            let proxy = ProxyId::of(slot, PROXIES).expect("proxies");
            format!(
                "A client writes {value}. Node 0 gives it slot {}. Slot {} goes to proxy leader {} \
                 (slot mod {PROXIES}).",
                slot.0, slot.0, proxy.0
            )
        } else {
            format!(
                "A client writes {value}. Node 0 gives it slot {} and sends the Accepts itself.",
                slot.0
            )
        };
        self.event(&format!("the client writes {value}"), says);
        slot
    }

    /// Two writers send to the batcher, which sends one write to node 0.
    fn batch(&mut self, writes: &[(u64, &str)]) {
        for (writer, value) in writes {
            let flushed = self.batcher.push(
                JournalIdentifier::UNSET,
                *writer,
                vec![value.as_bytes().to_vec()],
            );
            let says = if flushed.is_empty() {
                format!(
                    "Writer {writer} sends {value} to the batcher. The batcher waits for more writes."
                )
            } else {
                format!(
                    "Writer {writer} sends {value} to the batcher. The batcher now holds enough \
                     writes, so it sends them as one write."
                )
            };
            self.event(&format!("writer {writer} writes {value}"), says);
            for batch in flushed {
                self.send_batch(batch);
            }
        }
    }

    /// The batcher's merged write reaches the leader, which proposes it in
    /// one slot.
    fn send_batch(&mut self, batch: Batch<u64>) {
        let records: Vec<String> = batch
            .records
            .iter()
            .map(|record| String::from_utf8_lossy(record).into_owned())
            .collect();
        let writers: Vec<String> = batch
            .writes
            .iter()
            .map(|(writer, count)| format!("writer {writer}: {count}"))
            .collect();
        let refs: Vec<&str> = records.iter().map(String::as_str).collect();
        let slot = self.write(&refs, Delegation::Auto);
        let proxy = ProxyId::of(slot, PROXIES).expect("proxies");
        self.steps.push(Step {
            kind: "deliver".to_string(),
            from: Some(Who::Batcher.lane()),
            to: Some(Who::Node(0).lane()),
            label: format!("Write · {} writes", batch.writes.len()),
            phase: Some("other".to_string()),
            reply: false,
            stage: Some(Stage {
                title: "Batch".to_string(),
                about: "The batcher puts the writes of many writers in one write. The leader \
                        gives the whole write one slot, so one round of Phase 2 chooses all \
                        of them."
                    .to_string(),
            }),
            says: format!(
                "The batcher sends {} as one write. Node 0 gives it one slot, slot {}, and \
                 hands it to proxy leader {}.",
                records.join(" and "),
                slot.0,
                proxy.0
            ),
            notes: Vec::new(),
            fields: vec![
                Field {
                    name: "records".to_string(),
                    value: records.join(", "),
                },
                Field {
                    name: "records per writer".to_string(),
                    value: writers.join("; "),
                },
            ],
            voice: format!("Please write {} as one write.", records.join(" and ")),
            state: self.state(),
            chosen: self.chosen(),
            tally: None,
            logs: self.logs(),
            broken: None,
        });
        self.batch = Some((batch, Some(slot)));
    }

    /// The batcher cuts the answer of its merged write into one answer per
    /// writer.
    fn unbatch(&mut self) {
        let (batch, slot) = self.batch.take().expect("a batch was sent");
        let slot = slot.expect("the batch has a slot");
        let Some(Outcome::Accepted { seq, count }) = self.nodes[0].replica().outcome_at(slot)
        else {
            panic!("the leader applied the batch");
        };
        let answers: Vec<String> = Unbatcher::scatter(&batch.writes, seq.0, *count)
            .into_iter()
            .map(|(writer, at, n)| {
                if n == 1 {
                    format!("writer {writer} gets position {at}")
                } else {
                    format!("writer {writer} gets positions {at} to {}", at + n - 1)
                }
            })
            .collect();
        self.event(
            "the batcher answers each writer",
            format!(
                "Slot {} is chosen, and its write starts at position {}. The batcher cuts the \
                 answer in one per writer: {}.",
                slot.0,
                seq.0,
                answers.join(", ")
            ),
        );
    }

    /// The leader's beat: tick, re-send the open rounds, take back a round
    /// whose proxy swallowed too many re-sends.
    fn beat(&mut self) -> Vec<(Slot, ProxyId)> {
        let leader = &mut self.nodes[0];
        leader.tick();
        leader.resend_pending();
        let taken = leader.take_back_delegated(TAKE_BACK_AFTER);
        self.drain(Who::Node(0));
        taken
    }

    fn finish(mut self, id: &str, label: &str) -> Scenario {
        let mut stages: Vec<Option<Stage>> =
            self.steps.iter().map(|step| step.stage.clone()).collect();
        words::fill_stages(&mut stages);
        for (step, stage) in self.steps.iter_mut().zip(stages) {
            step.stage = stage;
        }
        let chapters = self
            .steps
            .iter()
            .enumerate()
            .filter(|(_, step)| step.kind == "event")
            .map(|(at, step)| Chapter {
                title: step.label.clone(),
                at: at + 1,
            })
            .collect();
        let mut outcome = self.chosen().join(", ");
        if outcome.is_empty() {
            outcome = "no chosen slot".to_string();
        }
        let outcome = if self.split() {
            let learned: Vec<String> = self
                .replicas
                .iter()
                .enumerate()
                .map(|(rank, replica)| {
                    format!(
                        "replica {rank} {}",
                        learned(replica.replica().chosen_index())
                    )
                })
                .collect();
            format!("Chosen: {outcome}. {}.", cap(&learned.join(", ")))
        } else {
            format!("Chosen: {outcome}.")
        };
        Scenario {
            chapters,
            id: id.to_string(),
            label: label.to_string(),
            fork: 0,
            outcome,
            start: self.start,
            start_logs: self.start_logs,
            lanes: None,
            grid: None,
            steps: self.steps,
        }
    }
}

fn learned(index: Option<Slot>) -> String {
    match index {
        Some(slot) => format!("learned to slot {}", slot.0),
        None => "learned nothing".to_string(),
    }
}

fn cell(slot: Slot, command: &Command, chosen: bool) -> LogCell {
    let control = matches!(command, Command::Control(_));
    LogCell {
        slot: slot.0,
        value: if matches!(command, Command::Control(Control::Noop)) {
            "no-op".to_string()
        } else {
            value_text(command)
        },
        state: if chosen { "chosen" } else { "accepted" }.to_string(),
        control,
    }
}

/// `node 0` → `Node 0`.
fn cap(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().chain(chars).collect()
    })
}

/// One plain sentence on what a message does, from its variant and its two
/// parties.
fn says(from: Who, to: Who, message: &Message) -> String {
    let (sender, receiver) = (cap(&from.name()), to.name());
    match message {
        Message::Accept { slot, command, .. } if matches!(to, Who::Proxy(_)) => format!(
            "{sender} gives slot {} ({}) to {receiver}. The proxy leader sends the Accepts and \
             counts the votes, so {} does not.",
            slot.0,
            value_text(command),
            from.name()
        ),
        Message::Accept { slot, command, .. } => format!(
            "{sender} asks {receiver} to vote for {} in slot {}.",
            value_text(command),
            slot.0
        ),
        Message::Accepted { slot, .. } => {
            format!("{sender} votes for slot {} and tells {receiver}.", slot.0)
        }
        Message::Nack { .. } => format!("{sender} refuses: it promised a higher ballot."),
        Message::Commit { slot, command, .. } if matches!(to, Who::Replica(_)) => format!(
            "{sender} tells {receiver} that slot {} is chosen. The replica learns {}, and it \
             votes on nothing.",
            slot.0,
            value_text(command)
        ),
        Message::Commit { slot, .. } => {
            format!("{sender} tells {receiver} that slot {} is chosen.", slot.0)
        }
        Message::Heartbeat { .. } => {
            format!("{sender} tells {receiver} that it leads and how far the log is chosen.")
        }
        Message::HeartbeatAck { .. } => format!("{sender} answers the beat."),
        Message::PreRead { .. } => {
            format!("{sender} asks {receiver} for the highest slot that it voted in.")
        }
        Message::PreReadAck { watermark, .. } => match watermark {
            Some(slot) => format!("{sender} answers: slot {}.", slot.0),
            None => format!("{sender} answers: no slot yet."),
        },
        Message::CatchUpRequest { .. } => {
            format!("{sender} asks {receiver} for the chosen slots that it does not have.")
        }
        Message::CatchUpResponse { .. } => {
            format!("{sender} sends {receiver} the chosen slots that it asked for.")
        }
        _ => content::node_message(message, true).voice,
    }
}

/// The base run: two writes, one through each proxy leader.
fn base() -> Scenario {
    let mut run = Run::new(Layout::Compartmentalized);
    run.client_writes("alpha");
    run.settle(|_, _, _| false);
    run.client_writes("bravo");
    run.settle(|_, _, _| false);
    run.finish("base", "Proxy leaders and replicas")
}

/// The same two writes on the colocated deployment.
fn colocated() -> Scenario {
    let mut run = Run::new(Layout::Colocated);
    run.client_writes("alpha");
    run.settle(|_, _, _| false);
    run.client_writes("bravo");
    run.settle(|_, _, _| false);
    run.finish("colocated", "Before: the leader does all the work")
}

/// Two writers through the batcher: one slot for both.
fn batched() -> Scenario {
    let mut run = Run::new(Layout::Compartmentalized);
    run.batch(&[(1, "alpha"), (2, "bravo")]);
    run.settle(|_, _, _| false);
    run.unbatch();
    run.finish("batcher", "Two writers, one slot (batcher)")
}

/// A replica reads with no leader.
fn read() -> Scenario {
    let mut run = Run::new(Layout::Compartmentalized);
    run.client_writes("alpha");
    run.settle(|_, _, _| false);
    run.replicas[1].quorum_read_in(1, None);
    run.drain(Who::Replica(1));
    run.event(
        "replica 1 reads",
        "A client reads from replica 1. The replica asks the acceptors for the highest slot \
         that each one voted in. Node 0 answers as an acceptor only: no leader confirms \
         the read."
            .to_string(),
    );
    run.settle(|_, _, _| false);
    run.finish("read", "A read with no leader")
}

/// A proxy leader crashes holding a slot; the leader takes it back.
fn proxy_crash() -> Scenario {
    let mut run = Run::new(Layout::Compartmentalized);
    run.client_writes("alpha");
    run.settle(|_, _, _| false);
    run.proxies[1] = None;
    run.event(
        "proxy leader 1 crashes",
        "Proxy leader 1 crashes. It holds nothing on disk, so nothing is lost with it.".to_string(),
    );
    let slot = run.client_writes("bravo");
    run.settle(|_, _, _| false);
    let mut beats = 0;
    while !run.nodes[0].delegated_rounds().is_empty() {
        beats += 1;
        assert!(beats <= TAKE_BACK_AFTER + 1, "the take-back budget holds");
        let taken = run.beat();
        let says = if taken.is_empty() {
            format!(
                "Node 0 beats. Slot {} is not chosen yet, so it sends the slot to proxy leader \
                 1 again.",
                slot.0
            )
        } else {
            format!(
                "Node 0 beats. Proxy leader 1 did not answer {TAKE_BACK_AFTER} times, so node 0 \
                 takes slot {} back and sends the Accepts itself.",
                slot.0
            )
        };
        run.event("node 0 beats", says);
        run.settle(|_, _, _| false);
    }
    run.finish("proxy-crash", "What if a proxy leader crashes?")
}

/// The page: the compartmentalized deployment, the same writes colocated,
/// the batcher, a leaderless read, a proxy crash, and the handoff.
pub(super) fn trace(handoff: Scenario) -> Trace {
    super::assemble(
        "compartmentalized",
        lanes(),
        None,
        vec![
            base(),
            colocated(),
            batched(),
            read(),
            proxy_crash(),
            handoff,
        ],
    )
}
