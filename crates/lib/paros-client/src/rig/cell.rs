//! Fake nodes of one cell: they share one set of journals, each judged by
//! the core's [`JournalState::apply`], so a write, a claim and a truncation
//! get the verdict a real node gives. The node that leads judges; the
//! others redirect to it; every node serves reads. A test scripts the
//! replies the honest fold never gives, one per call, in order.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use moonpool_core::{LocalTaskProvider, ThreadProviders};
use moonpool_rpc::{IncomingRequest, RequestStream, RpcMethod};
use moonpool_sim::SimProviders;
use paros_core::Value;
use paros_core::journal::{
    Command, Control, Entry, JournalIdentifier, JournalState, Outcome, Seq, WriterMode,
};
use paros_net::inbound::serve_well_known;
use paros_wire::methods::{ReadRpc, SetLeaderRpc, TruncateRpc, WriteRpc};
use paros_wire::public::WriteOutcome as Wire;
use paros_wire::{
    Read, ReadAck, SetLeader, SetLeaderAck, Truncate, TruncateAck, Write, WriteAck,
    journal_view_to_proto, leader_from_proto, leader_uuid_from_proto,
};

use super::{listen, node_addr};

/// One journal of the cell: its mode, its state and its records.
struct Book {
    mode: WriterMode,
    state: JournalState,
    /// The record at each position, `None` below the floor.
    records: Vec<Option<Value>>,
    /// The write accepted with its first record at each position.
    accepted: BTreeMap<Seq, Entry>,
}

impl Book {
    fn apply(&mut self, command: &Command) -> Outcome {
        let accepted = self.accepted.clone();
        let outcome = self
            .state
            .apply(self.mode, command, |seq| accepted.get(&seq));
        match (&outcome, command) {
            (Outcome::Accepted { seq, .. }, Command::Write(entry)) => {
                self.accepted.insert(*seq, entry.clone());
                self.records.extend(entry.records.iter().cloned().map(Some));
            }
            (Outcome::Trimmed(view), _) => {
                for record in self.records.iter_mut().take(position(view.first_seq)) {
                    *record = None;
                }
            }
            _ => {}
        }
        outcome
    }

    fn read(&self, request: &Read) -> ReadAck {
        let state = Some(journal_view_to_proto(self.state.view()));
        if request.from_seq < self.state.first_seq.0 {
            return ReadAck {
                served: true,
                truncated: true,
                state,
                ..ReadAck::default()
            };
        }
        let records = self
            .records
            .iter()
            .skip(position(Seq(request.from_seq)))
            .take(position(Seq(request.limit.max(1))))
            .map(|record| record.clone().expect("a record above the floor").0)
            .collect();
        ReadAck {
            served: true,
            from_seq: request.from_seq,
            records,
            state,
            ..ReadAck::default()
        }
    }
}

fn position(seq: Seq) -> usize {
    usize::try_from(seq.0).expect("a test position")
}

/// What a fake node does with one call: answer it with a scripted reply,
/// hold it (never answered: the client's bound runs out), or judge it and
/// lose the answer (it landed, and the client never hears so).
pub enum Act<A> {
    Answer(A),
    Hold,
    Lose,
}

/// The replies a test scripts for one node, per call kind, in order. A
/// call with no scripted reply gets the honest one.
#[derive(Default)]
pub struct Script {
    pub writes: VecDeque<Act<WriteAck>>,
    pub reads: VecDeque<Act<ReadAck>>,
    pub set_leaders: VecDeque<Act<SetLeaderAck>>,
    pub truncates: VecDeque<Act<TruncateAck>>,
}

/// One call a fake node heard.
#[derive(Clone, Debug, PartialEq)]
pub enum Heard {
    Write(Write),
    Read(Read),
    SetLeader(SetLeader),
    Truncate(Truncate),
}

/// The fake cell: its journals, the node that leads, what each node heard
/// and the replies scripted for each.
#[derive(Clone, Default)]
pub struct Cell(Rc<RefCell<Inner>>);

#[derive(Default)]
struct Inner {
    books: BTreeMap<JournalIdentifier, Book>,
    leader: Option<u64>,
    heard: Vec<(u64, Heard)>,
    scripts: BTreeMap<u64, Script>,
    /// The replies held, kept so that no held call fails at once.
    held: Vec<Box<dyn std::any::Any>>,
}

impl Cell {
    /// A cell whose node `leader` leads.
    pub fn led_by(leader: u64) -> Self {
        let cell = Self::default();
        cell.0.borrow_mut().leader = Some(leader);
        cell
    }

    /// Create `journal` in `mode`, empty.
    pub fn create(&self, journal: JournalIdentifier, mode: WriterMode) {
        self.0.borrow_mut().books.insert(
            journal,
            Book {
                mode,
                state: JournalState::default(),
                records: Vec::new(),
                accepted: BTreeMap::new(),
            },
        );
    }

    /// Make node `leader` lead (`None`: no node leads, every node
    /// redirects naming none).
    pub fn lead(&self, leader: Option<u64>) {
        self.0.borrow_mut().leader = leader;
    }

    /// Where `journal` stands.
    pub fn state(&self, journal: JournalIdentifier) -> JournalState {
        self.0.borrow().books[&journal].state
    }

    /// The records `journal` holds from its floor up.
    pub fn records(&self, journal: JournalIdentifier) -> Vec<Value> {
        self.0.borrow().books[&journal]
            .records
            .iter()
            .flatten()
            .cloned()
            .collect()
    }

    /// Apply `command` to `journal` directly, as another client's decided
    /// call.
    pub fn apply(&self, journal: JournalIdentifier, command: &Command) -> Outcome {
        self.0
            .borrow_mut()
            .books
            .get_mut(&journal)
            .expect("a created journal")
            .apply(command)
    }

    /// Script the replies of node `node`.
    pub fn script(&self, node: u64, edit: impl FnOnce(&mut Script)) {
        edit(self.0.borrow_mut().scripts.entry(node).or_default());
    }

    /// Every call heard, with the node that heard it, in order.
    pub fn heard(&self) -> Vec<(u64, Heard)> {
        self.0.borrow().heard.clone()
    }

    /// The nodes that heard a write, in order.
    pub fn writes_heard(&self) -> Vec<u64> {
        self.heard()
            .into_iter()
            .filter_map(|(node, call)| matches!(call, Heard::Write(_)).then_some(node))
            .collect()
    }

    /// Start fake node `id` at [`node_addr`]`(id)`.
    pub async fn start(&self, providers: &SimProviders, id: u64) {
        let rpc = listen(providers, node_addr(id)).await;
        self.serve::<WriteRpc>(
            providers,
            &rpc,
            id,
            Heard::Write,
            |s| &mut s.writes,
            Inner::write,
        );
        self.serve::<ReadRpc>(
            providers,
            &rpc,
            id,
            Heard::Read,
            |s| &mut s.reads,
            Inner::read,
        );
        self.serve::<SetLeaderRpc>(
            providers,
            &rpc,
            id,
            Heard::SetLeader,
            |s| &mut s.set_leaders,
            Inner::set_leader,
        );
        self.serve::<TruncateRpc>(
            providers,
            &rpc,
            id,
            Heard::Truncate,
            |s| &mut s.truncates,
            Inner::truncate,
        );
    }

    fn serve<M>(
        &self,
        providers: &SimProviders,
        rpc: &moonpool_rpc::RpcHandle<SimProviders>,
        id: u64,
        heard: fn(M::Request) -> Heard,
        script: fn(&mut Script) -> &mut VecDeque<Act<M::Reply>>,
        honest: fn(&mut Inner, u64, &M::Request) -> M::Reply,
    ) where
        M: paros_wire::methods::WellKnownMethod + RpcMethod,
        M::Request: Clone,
        M::Reply: 'static,
    {
        let mut stream: RequestStream<M> = serve_well_known::<_, M>(rpc).expect("serve");
        let cell = self.clone();
        providers
            .local_task()
            .spawn_local_task("rig-node", async move {
                while let Some(IncomingRequest { request, reply }) = stream.recv().await {
                    let mut inner = cell.0.borrow_mut();
                    inner.heard.push((id, heard(request.clone())));
                    let act = script(inner.scripts.entry(id).or_default()).pop_front();
                    match act {
                        Some(Act::Hold) => inner.held.push(Box::new(reply)),
                        Some(Act::Lose) => {
                            honest(&mut inner, id, &request);
                            inner.held.push(Box::new(reply));
                        }
                        Some(Act::Answer(ack)) => {
                            reply.send(&ack);
                        }
                        None => {
                            let ack = honest(&mut inner, id, &request);
                            reply.send(&ack);
                        }
                    }
                }
            })
            .detach();
    }
}

impl Inner {
    fn book(&mut self, tenant: u64, journal: u64) -> Option<&mut Book> {
        self.books.get_mut(&ids(tenant, journal))
    }

    /// Whether node `id` judges a call: it leads.
    fn leads(&self, id: u64) -> bool {
        self.leader == Some(id)
    }

    fn write(&mut self, id: u64, request: &Write) -> WriteAck {
        let leads = self.leads(id);
        let leader = self.leader;
        let Some(book) = self.book(request.tenant, request.journal) else {
            return WriteAck {
                unknown_journal: true,
                ..WriteAck::default()
            };
        };
        if !leads {
            return WriteAck {
                leader,
                ..WriteAck::default()
            };
        }
        let entry = Entry {
            leader: leader_uuid_from_proto(request.leader),
            seq: Seq(request.seq),
            records: request.records.iter().cloned().map(Value).collect(),
        };
        match book.apply(&Command::Write(entry)) {
            Outcome::Accepted { seq, count } => ack(Wire::Accepted, seq.0, count),
            Outcome::Duplicate { seq, count } => ack(Wire::Duplicate, seq.0, count),
            Outcome::Refused(view) => refusal(Wire::Refused, view),
            Outcome::Truncated(view) => refusal(Wire::Truncated, view),
            Outcome::WrongMode(view) => refusal(Wire::WrongMode, view),
            other => panic!("a write applies to a write's verdict, not {other:?}"),
        }
    }

    fn read(&mut self, _id: u64, request: &Read) -> ReadAck {
        self.book(request.tenant, request.journal).map_or_else(
            || ReadAck {
                unknown_journal: true,
                ..ReadAck::default()
            },
            |book| book.read(request),
        )
    }

    fn set_leader(&mut self, id: u64, request: &SetLeader) -> SetLeaderAck {
        let leads = self.leads(id);
        let leader = self.leader;
        let Some(book) = self.book(request.tenant, request.journal) else {
            return SetLeaderAck {
                unknown_journal: true,
                ..SetLeaderAck::default()
            };
        };
        if !leads {
            return SetLeaderAck {
                leader,
                ..SetLeaderAck::default()
            };
        }
        let command = Command::Control(Control::SetLeader {
            new: leader_uuid_from_proto(request.new),
            old: leader_from_proto(request.old),
        });
        let (won, wrong_mode, view) = match book.apply(&command) {
            Outcome::Leader(view) => (true, false, view),
            Outcome::LeaderRefused(view) => (false, false, view),
            Outcome::WrongMode(view) => (false, true, view),
            other => panic!("a SetLeader applies to its verdict, not {other:?}"),
        };
        SetLeaderAck {
            decided: true,
            won,
            wrong_mode,
            state: Some(journal_view_to_proto(view)),
            ..SetLeaderAck::default()
        }
    }

    fn truncate(&mut self, id: u64, request: &Truncate) -> TruncateAck {
        let leads = self.leads(id);
        let leader = self.leader;
        let Some(book) = self.book(request.tenant, request.journal) else {
            return TruncateAck {
                unknown_journal: true,
                ..TruncateAck::default()
            };
        };
        if !leads {
            return TruncateAck {
                leader,
                ..TruncateAck::default()
            };
        }
        let command = Command::Control(Control::Truncate {
            leader: leader_uuid_from_proto(request.leader),
            up_to: Seq(request.up_to),
        });
        let (refused, wrong_mode, view) = match book.apply(&command) {
            Outcome::Trimmed(view) => (false, false, view),
            Outcome::TruncateRefused(view) => (true, false, view),
            Outcome::WrongMode(view) => (false, true, view),
            other => panic!("a Truncate applies to its verdict, not {other:?}"),
        };
        TruncateAck {
            decided: true,
            refused,
            wrong_mode,
            state: Some(journal_view_to_proto(view)),
            ..TruncateAck::default()
        }
    }
}

fn ack(outcome: Wire, seq: u64, count: u64) -> WriteAck {
    WriteAck {
        outcome: outcome.into(),
        seq,
        count,
        ..WriteAck::default()
    }
}

fn refusal(outcome: Wire, view: paros_core::journal::JournalView) -> WriteAck {
    WriteAck {
        outcome: outcome.into(),
        state: Some(journal_view_to_proto(view)),
        ..WriteAck::default()
    }
}

/// The journal `(tenant, journal)`.
pub fn ids(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier {
        tenant: paros_core::journal::TenantId(tenant),
        journal: paros_core::journal::JournalId(journal),
    }
}
