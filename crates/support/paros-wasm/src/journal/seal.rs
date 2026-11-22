//! The step-through of a reshape and a seal (#1365 (reshape and seal
//! step-through)), for the site's `/journal/segments/` page.
//!
//! A run drives the journal roles of `paros-journal` by hand: the acceptors
//! ([`JournalAcceptor`]), the leaders ([`JournalLeader`]) and the segment
//! replicas ([`SegmentReplica`]), and the value of the journal's matchmaker
//! register ([`RegisterValue`]). Every decision is the roles' own: the Phase 1
//! over the segments, the seal end, the gap fill, the Phase-2 tally, the fold
//! and its answers, the boundary state and `Resolve`. The driver only moves
//! messages, as the compartmentalized page's driver does for
//! `paros-consensus`, and records each move as one step of a
//! [`crate::steps::Trace`], the shape the Paxos pages draw.
//!
//! # What the driver does, and does not
//!
//! - Each message is a flight on the wire; a script delivers or drops it. A
//!   delivery calls the role's `step` (or `propose`, `observe`, `resolve`)
//!   and puts the effects on the wire.
//! - A disk write is durable at once: a role's persist batch applies in the
//!   step that emits it, and its sends leave in the same step. The page
//!   shows the write under the node before its reply moves.
//! - The register is one lane, `matchmakers`: a compare-and-set is one
//!   message to it, applied with [`RegisterValue::apply`] (a write already
//!   done at a majority; the Matchmakers page runs the reads and writes behind
//!   it), and one answer back to the caller. The other roles read each new
//!   value at once, as a role that watches the register does.
//! - The client keeps what a client keeps: its uuid, its writes and the
//!   answers it heard.
//!
//! No clock and no randomness: the same script gives the same run.
//!
//! # Break the rule
//!
//! The "Break the rule" run writes a seal that no leader computed: the end at
//! the first slot that no replica applied, with no Phase 1 over the old grid.
//! The register's guard takes it (the guard checks the state of the
//! segments, not the end), and a later leader writes a second value at a slot
//! that the old grid chose. The fault comes from the driver, never from a
//! change of `paros-journal`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use paros_journal::register::{Ballot, Change, ChangeRefusal, RegisterValue};
use paros_journal::roles::{
    AcceptorDisk, AcceptorEffect, Campaign, JournalAcceptor, JournalLeader, LeaderEffect,
    LeaderPhase, Message, Proposed, ReplicaDisk, ReplicaEffect, SegmentReplica,
};
use paros_journal::segment::{Grid, OpeningCall, SegmentId, SegmentRecord, SegmentState};
use paros_journal::{
    Command, Fence, Leader, LeaderUuid, NodeId, RELEASE, Record, Reply, ResolveAnswer, Seq, Slot,
    Write,
};

use crate::steps::story::Beats;
use crate::steps::{
    Flight, Lane, LogCell, NodeState, Scenario, Stage, Step, Tally, Trace, assemble, fill_stages,
};

/// The page: the file name under `website/site/src/traces/`.
pub const PAGE: &str = "journal-seal";

/// How many deliveries one `settle` may make before it is called a loop.
const BUDGET: usize = 256;

/// The highest slot a log strip shows.
const SLOTS: u64 = 8;

/// The client's uuid: it leads the journal (`SetLeader` in the setup).
const UUID: LeaderUuid = LeaderUuid(0xA);

/// The controller's node in the register's term.
const CONTROLLER: NodeId = NodeId(0);

/// Each segment takes three of acceptors `1..=4`, proposers 5 and 6 lead, and
/// replica 7 serves segment 1, replica 8 segment 2. One replica for each
/// segment keeps the drawing small; a replica set of `2F + 1` changes no
/// step of the seal.
const ACCEPTORS: [u64; 4] = [1, 2, 3, 4];
const PROPOSERS: [u64; 2] = [5, 6];
const REPLICAS: [(u64, u64); 2] = [(7, 1), (8, 2)];

/// The trace of the Segments page: the seal, a slow acceptor, and the rule
/// broken.
#[must_use]
pub fn trace() -> Trace {
    assemble(PAGE, lanes(), None, vec![seal(), slow(), broken()])
}

/// The trace of `page`, recorded now; `None` for another page.
#[must_use]
pub fn trace_of(page: &str) -> Option<Trace> {
    (page == PAGE).then(trace)
}

/// The lanes, in the order the diagram draws them.
#[must_use]
pub fn lanes() -> Vec<Lane> {
    let lane = |id: &str, label: &str, role: &str| Lane {
        id: id.to_string(),
        label: label.to_string(),
        role: role.to_string(),
        cell: None,
    };
    let mut lanes = vec![
        lane("c", "client", "client"),
        lane("ctl", "controller", "controller"),
        lane("m", "matchmakers", "matchmaker"),
    ];
    for p in PROPOSERS {
        lanes.push(lane(&format!("p{p}"), &format!("proposer {p}"), "proposer"));
    }
    for a in ACCEPTORS {
        lanes.push(lane(&format!("a{a}"), &format!("acceptor {a}"), "acceptor"));
    }
    for (r, _) in REPLICAS {
        lanes.push(lane(&format!("r{r}"), &format!("replica {r}"), "replica"));
    }
    lanes
}

// ---- the runs ----------------------------------------------------------------

/// The first steps of every run: segment 1 in force, and the client writes
/// w1.
fn opening() -> Run {
    let mut run = Run::new();
    run.beat("Segment 1 is in force: acceptors 1, 2 and 3 vote on its slots, and replica 7 applies them.");
    run.beat("The client writes w1 at seq 1. Two of the three acceptors vote, so slot 2 is chosen, and replica 7 applies it.")
        .write("w1", 1, "p5")
        .deliver("Write", "c", "p5")
        .settle("Accept", Some("p5"))
        .settle("Accepted", Some("p5"))
        .deliver("Commit", "p5", "r7")
        .deliver("Applied", "r7", "p5")
        .deliver("Reply", "p5", "c");
    run
}

/// The controller opens segment 2: acceptor 4 takes the place of acceptor 3.
fn reshape(run: &mut Run) {
    run.beat("The controller replaces acceptor 3 with acceptor 4. Its reshape writes segment 2, pending, with replica 8.")
        .reshape()
        .deliver("Reshape", "ctl", "m")
        .deliver("Done", "m", "ctl");
}

/// Proposer 6 campaigns for segment 2 and asks every acceptor.
fn campaign(run: &mut Run) {
    run.beat("Proposer 6 campaigns at ballot 2.6, bound to segment 2. Its Phase 1 asks the old grid and the new grid.")
        .campaign(6, 2, 2)
        .settle("Prepare", Some("p6"));
}

/// The leader of the old ballot learns of the new one, and lets its held
/// write go.
fn old_leader_stops(run: &mut Run) {
    run.beat("Proposer 5 is still at ballot 1.5. Its late Accepts meet the promise of ballot 2.6, so it stops and answers Unknown for w2.")
        .deliver("Accept", "p5", "a2")
        .deliver("Accept", "p5", "a3")
        .deliver("Nack", "a2", "p5")
        .deliver("Nack", "a3", "p5")
        .deliver("Reply", "p5", "c");
}

/// The base run: the seal finds the lone vote, and the resend is `Below`.
fn seal() -> Scenario {
    let mut run = opening();
    run.beat("The client writes w2 at seq 2. Only acceptor 1 gets its Accept now; the Accepts to acceptors 2 and 3 are slow.")
        .write("w2", 2, "p5")
        .deliver("Write", "c", "p5")
        .deliver("Accept", "p5", "a1")
        .deliver("Accepted", "a1", "p5");
    reshape(&mut run);
    campaign(&mut run);
    run.beat("Acceptors 1 and 2 promise, and acceptor 1 reports its vote for w2 at slot 3. Two of the three acceptors are a quorum.")
        .deliver("Promise", "a1", "p6")
        .deliver("Promise", "a2", "p6");
    run.beat("Proposer 6 writes the seal by compare-and-set: segment 1 ends at slot 4, and segment 2 is in force from slot 4.")
        .ask(
            "Where does proposer 6 end segment 1?",
            &[
                "At slot 3: w2 is not chosen there",
                "At slot 4: one above the vote it saw",
                "At slot 2: after the last answer the client heard",
            ],
            1,
            "The end is one above the highest vote that the Phase 1 saw in the old grid. Proposer 6 cannot know if w2 is chosen, so slot 3 stays in segment 1.",
        )
        .deliver("Seal", "p6", "m")
        .holds(|run| run.end_of(1) == Some(Slot(4)), "segment 1 ends at slot 4")
        .deliver("Done", "m", "p6");
    run.beat("Proposer 6 writes w2 again at slot 3, in its own grid. The vote that it found becomes a chosen value, and replica 7 applies it.")
        .settle("Accept", Some("p6"))
        .settle("Accepted", Some("p6"))
        .deliver("Commit", "p6", "r7")
        .deliver("Applied", "r7", "p6");
    run.beat("Replica 7 applied every slot of segment 1. It writes the boundary state into the record, and replica 8 starts its fold from it.")
        .deliver("Boundary", "r7", "m")
        .deliver("Done", "m", "r7");
    old_leader_stops(&mut run);
    run.beat("The client does not know what happened to w2, so it sends w2 again, now to proposer 6.")
        .ask(
            "What does the journal answer to the resend?",
            &[
                "Written, at seq 3",
                "Below: seq 2 is in an older segment",
                "Fenced: the uuid does not lead",
            ],
            1,
            "Segment 2 starts at seq 3, so the fold of segment 2 cannot judge a write at seq 2. It answers Below, and the client asks segment 1.",
        )
        .write("w2", 2, "p6")
        .deliver("Write", "c", "p6")
        .settle("Accept", Some("p6"))
        .settle("Accepted", Some("p6"))
        .deliver("Commit", "p6", "r8")
        .deliver("Applied", "r8", "p6")
        .deliver("Reply", "p6", "c")
        .holds(|run| run.heard("w2") == Some("Below"), "the resend is Below");
    run.beat("The client asks replica 7 of segment 1 with Resolve, and learns that w2 is written at seq 2.")
        .resolve("w2", "r7")
        .deliver("Resolve", "c", "r7")
        .deliver("Resolved", "r7", "c")
        .choice("What if acceptor 1 is slow?", "slow-acceptor", 4)
        .choice("Break the rule", "break-the-rule", 0);
    run.finish(
        "base",
        "A reshape and a seal",
        "w2 is written once, at seq 2 in segment 1. Segment 2 starts at seq 3.",
    )
}

/// What if acceptor 1 is slow: the Phase 1 does not see the lone vote.
fn slow() -> Scenario {
    let mut run = opening();
    run.beat("The client writes w2 at seq 2. Only acceptor 1 gets its Accept now; the Accepts to acceptors 2 and 3 are slow.")
        .write("w2", 2, "p5")
        .deliver("Write", "c", "p5")
        .deliver("Accept", "p5", "a1")
        .deliver("Accepted", "a1", "p5");
    reshape(&mut run);
    campaign(&mut run);
    run.beat("Acceptor 1 is slow. Acceptors 2, 3 and 4 promise, and none of them holds a vote at slot 3.")
        .deliver("Promise", "a2", "p6")
        .deliver("Promise", "a3", "p6")
        .deliver("Promise", "a4", "p6");
    run.beat("Proposer 6 seals segment 1 at slot 3. The vote of acceptor 1 is not below the end, so no later Phase 1 reads it in the old grid.")
        .ask(
            "Is it safe to leave out the vote of acceptor 1?",
            &[
                "Yes: one vote of three is not a choice",
                "No: w2 is lost for ever",
            ],
            0,
            "A value is chosen only at two of the three acceptors of a column. Any two acceptors meet any other two, so a Phase 1 that sees no vote proves that w2 is not chosen.",
        )
        .deliver("Seal", "p6", "m")
        .holds(|run| run.end_of(1) == Some(Slot(3)), "segment 1 ends at slot 3")
        .deliver("Done", "m", "p6")
        .deliver("Boundary", "r7", "m")
        .deliver("Done", "m", "r7")
        .deliver("Promise", "a1", "p6");
    old_leader_stops(&mut run);
    run.beat("The client sends w2 again. Segment 2 starts at seq 2, so its fold writes w2 there.")
        .write("w2", 2, "p6")
        .deliver("Write", "c", "p6")
        .settle("Accept", Some("p6"))
        .settle("Accepted", Some("p6"))
        .deliver("Commit", "p6", "r8")
        .deliver("Applied", "r8", "p6")
        .deliver("Reply", "p6", "c")
        .holds(
            |run| run.heard("w2") == Some("Written"),
            "the resend is written",
        );
    run.finish(
        "slow-acceptor",
        "What if acceptor 1 is slow?",
        "w2 is written once, at seq 2 in segment 2. Segment 1 ends at slot 3.",
    )
}

/// Break the rule: a seal end below a chosen vote.
fn broken() -> Scenario {
    let mut run = opening();
    run.beat("The client writes w2 at seq 2. Acceptors 2 and 3 vote, so w2 is chosen at slot 3, but the Commit to replica 7 is lost.")
        .write("w2", 2, "p5")
        .deliver("Write", "c", "p5")
        .deliver("Accept", "p5", "a2")
        .deliver("Accept", "p5", "a3")
        .deliver("Accepted", "a2", "p5")
        .deliver("Accepted", "a3", "p5")
        .drop("Commit", "p5", "r7");
    reshape(&mut run);
    run.beat("Break the rule: a sealer with no Phase 1 ends segment 1 at the first slot that no replica applied, slot 3.")
        .forge_seal(1, 3)
        .deliver("Seal", "p6", "m")
        .deliver("Done", "m", "p6")
        .deliver("Boundary", "r7", "m")
        .deliver("Done", "m", "r7");
    run.beat("Proposer 6 campaigns for segment 2, in force from slot 3. Acceptors 1, 3 and 4 promise. The vote of acceptor 3 is not below the end, so it does not count, and acceptor 2 is slow.")
        .campaign(6, 2, 2)
        .settle("Prepare", Some("p6"))
        .deliver("Promise", "a1", "p6")
        .deliver("Promise", "a3", "p6")
        .deliver("Promise", "a4", "p6");
    run.beat("Proposer 5 stops at the higher promise. The client gives up on w2 and writes w3 at seq 2, and proposer 6 puts it at slot 3.")
        .deliver("Accept", "p5", "a1")
        .deliver("Nack", "a1", "p5")
        .deliver("Reply", "p5", "c")
        .write("w3", 2, "p6")
        .deliver("Write", "c", "p6")
        .deliver("Accept", "p6", "a1")
        .deliver("Accept", "p6", "a4")
        .deliver("Accepted", "a1", "p6")
        .deliver("Accepted", "a4", "p6")
        .find(
            "What went wrong?",
            &[
                "The seal end is below a chosen vote",
                "Proposer 6 did not fill the gap with a no-op",
                "Acceptor 3 left the grid too early",
            ],
            0,
            "The end must be one above the highest vote that a Phase 1 of the old grid sees. Any two acceptors of the old column include acceptor 2 or 3, which hold w2, so that Phase 1 sees it, ends the segment at slot 4, and writes w2 again at slot 3.",
        )
        .holds(|run| run.broken().is_some(), "two values are chosen at slot 3");
    run.finish(
        "break-the-rule",
        "Break the rule: seal below a vote",
        "Slot 3 holds w2 in the grid of segment 1 and w3 in the grid of segment 2.",
    )
}

// ---- the driver --------------------------------------------------------------

/// What a flight carries.
#[derive(Clone, Debug)]
enum Payload {
    /// A message between two roles.
    Role(Message),
    /// The client's write to a leader.
    Write { token: u64, command: Command },
    /// A leader's answer to the client.
    Reply { token: u64, reply: Reply },
    /// A compare-and-set on the register.
    Change(Change),
    /// The register's answer: the change is done, or refused.
    Done {
        change: Change,
        refused: Option<ChangeRefusal>,
    },
    /// The client's `Resolve` at a replica.
    Resolve { name: String, write: Write },
    /// The replica's answer to `Resolve`.
    Resolved { name: String, answer: ResolveAnswer },
}

/// One message on the wire.
#[derive(Clone, Debug)]
struct InFlight {
    id: u64,
    fan: u64,
    from: String,
    to: String,
    payload: Payload,
}

/// One write of the client.
#[derive(Clone, Debug)]
struct Sent {
    name: String,
    write: Write,
}

/// One run of the roles, recorded as steps.
struct Run {
    register: RegisterValue,
    acceptors: BTreeMap<NodeId, JournalAcceptor>,
    acceptor_disks: BTreeMap<NodeId, AcceptorDisk>,
    replicas: BTreeMap<NodeId, SegmentReplica>,
    replica_disks: BTreeMap<NodeId, ReplicaDisk>,
    leaders: BTreeMap<NodeId, JournalLeader<u64>>,
    wire: Vec<InFlight>,
    next_id: u64,
    /// The client's writes by token, and the last answer it heard for each
    /// write's name.
    sent: BTreeMap<u64, Sent>,
    heard: BTreeMap<String, String>,
    next_token: u64,
    /// What each leader saw chosen: slot → value → the segment of the grid.
    chosen: BTreeMap<Slot, BTreeMap<String, u64>>,
    /// The answers each leader counted: (leader, `promises` or slot).
    counted: BTreeMap<(NodeId, Option<Slot>), BTreeSet<NodeId>>,
    /// The slots that each leader committed: (leader, slot).
    committed: BTreeSet<(NodeId, Slot)>,
    /// The disk writes of the step in progress, by lane.
    writes: BTreeMap<String, Vec<String>>,
    /// The engine's notes of the step in progress.
    notes: Vec<String>,
    recording: bool,
    steps: Vec<Step>,
    beats: Beats,
    start: Vec<Vec<String>>,
    start_logs: Vec<Vec<LogCell>>,
    start_nodes: Vec<NodeState>,
    start_wire: Vec<Flight>,
}

impl Run {
    /// Segment 1 in force, proposer 5 its leader, and the client's
    /// `SetLeader` and first write w0 applied. None of it is recorded.
    fn new() -> Self {
        let mut run = Self {
            register: RegisterValue::new(),
            acceptors: BTreeMap::new(),
            acceptor_disks: BTreeMap::new(),
            replicas: BTreeMap::new(),
            replica_disks: BTreeMap::new(),
            leaders: BTreeMap::new(),
            wire: Vec::new(),
            next_id: 1,
            sent: BTreeMap::new(),
            heard: BTreeMap::new(),
            next_token: 1,
            chosen: BTreeMap::new(),
            counted: BTreeMap::new(),
            committed: BTreeSet::new(),
            writes: BTreeMap::new(),
            notes: Vec::new(),
            recording: false,
            steps: Vec::new(),
            beats: Beats::default(),
            start: Vec::new(),
            start_logs: Vec::new(),
            start_nodes: Vec::new(),
            start_wire: Vec::new(),
        };
        for a in ACCEPTORS {
            let disk = AcceptorDisk::default();
            run.acceptors.insert(
                NodeId(a),
                JournalAcceptor::recover(
                    NodeId(a),
                    disk.clone(),
                    paros_journal::roles::PROMISE_BATCH,
                ),
            );
            run.acceptor_disks.insert(NodeId(a), disk);
        }
        run.register = run
            .register
            .apply(&Change::Elect { holder: CONTROLLER })
            .expect("the controller's first election");
        run.reshape_to(&[1, 2, 3], 7);
        run.settle_all();
        run.campaign(5, 1, 1);
        run.settle_all();
        let token = run.token();
        run.send(
            "c",
            "p5",
            Payload::Write {
                token,
                command: Command::SetLeader {
                    new: Some(UUID),
                    old: Leader::Unset,
                },
            },
            None,
        );
        run.settle_all();
        run.write("w0", 0, "p5");
        run.settle_all();
        assert!(
            run.wire.is_empty(),
            "the setup leaves no message on the wire"
        );
        run.writes.clear();
        run.notes.clear();
        run.recording = true;
        run.start = run.state_lines();
        run.start_logs = run.logs();
        run.start_nodes = run.nodes();
        run.start_wire = run.flights();
        run
    }

    // ---- the script's verbs ----

    /// Open a beat of the story.
    fn beat(&mut self, says: &str) -> &mut Self {
        self.beats.open(self.steps.len(), says);
        self
    }

    /// Ask a question before the open beat plays.
    fn ask(&mut self, asks: &str, answers: &[&str], right: usize, why: &str) -> &mut Self {
        self.beats.ask(asks, answers, right, why);
        self
    }

    /// Ask what went wrong, after the open beat played.
    fn find(&mut self, asks: &str, answers: &[&str], right: usize, why: &str) -> &mut Self {
        self.beats.find(asks, answers, right, why);
        self
    }

    /// End the open beat with a choice: open the run `run` at its beat
    /// `beat`.
    fn choice(&mut self, label: &str, run: &str, beat: usize) -> &mut Self {
        self.beats.choice(label, run, beat);
        self
    }

    /// Pin the answer of a question to the run.
    ///
    /// # Panics
    ///
    /// When `fact` is false: the roles changed, and the story must change.
    fn holds(&mut self, fact: impl Fn(&Self) -> bool, says: &str) -> &mut Self {
        assert!(fact(self), "the run no longer shows that {says}");
        self
    }

    /// The client sends the fenced write `name` at `seq` to `leader`: an
    /// event step. A write sent again keeps its records and its fence.
    fn write(&mut self, name: &str, seq: u64, leader: &str) -> &mut Self {
        let write = self
            .sent
            .values()
            .find(|sent| sent.name == name)
            .map_or_else(
                || Write {
                    fence: Fence::Leader {
                        uuid: UUID,
                        expected_seq: Seq(seq),
                    },
                    records: vec![Record::new(name.as_bytes().to_vec())],
                },
                |sent| sent.write.clone(),
            );
        let again = self.sent.values().any(|sent| sent.name == name);
        let token = self.token();
        self.sent.insert(
            token,
            Sent {
                name: name.to_string(),
                write: write.clone(),
            },
        );
        self.send(
            "c",
            leader,
            Payload::Write {
                token,
                command: Command::Write(write),
            },
            None,
        );
        let label = if again {
            format!("the client sends {name} again")
        } else {
            format!("the client writes {name}")
        };
        let says = if again {
            format!(
                "The client sends {name} again, the same records at the same seq {seq}, to {}.",
                party(leader)
            )
        } else {
            format!(
                "The client sends {name}, one record at seq {seq} under its uuid, to {}.",
                party(leader)
            )
        };
        self.event(&label, &says);
        self
    }

    /// The controller's reshape: segment 2 with acceptors 1, 2 and 4 and
    /// replica 8. An event step.
    fn reshape(&mut self) -> &mut Self {
        self.reshape_to(&[1, 2, 4], 8);
        self.event(
            "the controller reshapes",
            "The controller calls reshape: the next segment, its grid and its replica set, fenced by its term.",
        );
        self
    }

    /// Proposer `p` campaigns at round `round`, bound to `segment`. An event
    /// step.
    fn campaign(&mut self, p: u64, round: u64, segment: u64) -> &mut Self {
        let value = self.register.clone();
        let from = self.applied_from(&value);
        let ballot = Ballot {
            round,
            node: NodeId(p),
        };
        let (leader, effects) = JournalLeader::campaign(
            Campaign {
                ballot,
                segment: SegmentId(segment),
                from,
            },
            &value,
        )
        .expect("the segment of the campaign is in the register");
        self.leaders.insert(NodeId(p), leader);
        self.leader_effects(NodeId(p), effects);
        self.event(
            &format!("proposer {p} campaigns"),
            &format!(
                "Proposer {p} starts Phase 1 at ballot {}, bound to segment {segment}, from slot {}.",
                ballot_text(ballot),
                from.0
            ),
        );
        self
    }

    /// The client asks `replica` whether its write `name` took its seq. An
    /// event step.
    fn resolve(&mut self, name: &str, replica: &str) -> &mut Self {
        let write = self
            .sent
            .values()
            .find(|sent| sent.name == name)
            .expect("the client resolves a write it sent")
            .write
            .clone();
        self.send(
            "c",
            replica,
            Payload::Resolve {
                name: name.to_string(),
                write,
            },
            None,
        );
        self.event(
            &format!("the client resolves {name}"),
            &format!("The client sends Resolve for {name} to {}.", party(replica)),
        );
        self
    }

    /// A seal that no leader computed: the fault of the "Break the rule"
    /// run, sent from proposer 6's lane. An event step.
    fn forge_seal(&mut self, segment: u64, end: u64) -> &mut Self {
        self.send(
            "p6",
            "m",
            Payload::Change(Change::Seal {
                segment: SegmentId(segment),
                end: Slot(end),
            }),
            None,
        );
        self.event(
            "a seal with no Phase 1",
            &format!(
                "A seal of segment {segment} at slot {end} leaves, and no Phase 1 computed it."
            ),
        );
        self
    }

    /// Deliver the first message of `kind` from `from` to `to`.
    ///
    /// # Panics
    ///
    /// When no such message is on the wire.
    fn deliver(&mut self, kind: &str, from: &str, to: &str) -> &mut Self {
        let id = self
            .flight(kind, Some(from), Some(to))
            .unwrap_or_else(|| panic!("no {kind} from {from} to {to} on the wire"));
        self.move_flight(id, true);
        self
    }

    /// Drop the first message of `kind` from `from` to `to`.
    ///
    /// # Panics
    ///
    /// When no such message is on the wire.
    fn drop(&mut self, kind: &str, from: &str, to: &str) -> &mut Self {
        let id = self
            .flight(kind, Some(from), Some(to))
            .unwrap_or_else(|| panic!("no {kind} from {from} to {to} on the wire"));
        self.move_flight(id, false);
        self
    }

    /// Deliver every message of `kind` that `lane` sends or receives (any,
    /// when `None`), oldest first, until none is left.
    ///
    /// # Panics
    ///
    /// When none is on the wire, or the deliveries do not end.
    fn settle(&mut self, kind: &str, lane: Option<&str>) -> &mut Self {
        let mut moved = 0;
        while let Some(id) = self
            .flight(kind, lane, None)
            .or_else(|| self.flight(kind, None, lane).filter(|_| lane.is_some()))
        {
            self.move_flight(id, true);
            moved += 1;
            assert!(moved < BUDGET, "settle {kind} does not end");
        }
        assert!(moved > 0, "no {kind} to settle");
        self
    }

    /// Deliver everything: the setup only.
    fn settle_all(&mut self) {
        let mut moved = 0;
        while let Some(first) = self.wire.first() {
            let id = first.id;
            self.move_flight(id, true);
            moved += 1;
            assert!(moved < BUDGET, "the setup does not settle");
        }
    }

    /// The run, with its beats.
    fn finish(&mut self, id: &str, label: &str, outcome: &str) -> Scenario {
        let mut stages: Vec<Option<Stage>> =
            self.steps.iter().map(|step| step.stage.clone()).collect();
        fill_stages(&mut stages);
        for (step, stage) in self.steps.iter_mut().zip(stages) {
            step.stage = stage;
        }
        let chapters = self
            .steps
            .iter()
            .enumerate()
            .filter(|(_, step)| step.kind == "event")
            .map(|(at, step)| crate::steps::Chapter {
                title: step.label.clone(),
                at: at + 1,
            })
            .collect();
        let beats = std::mem::take(&mut self.beats).finish(self.steps.len());
        Scenario {
            id: id.to_string(),
            label: label.to_string(),
            fork: 0,
            start: self.start.clone(),
            start_logs: self.start_logs.clone(),
            start_nodes: self.start_nodes.clone(),
            start_wire: self.start_wire.clone(),
            steps: std::mem::take(&mut self.steps),
            beats,
            lanes: None,
            grid: None,
            chapters,
            outcome: outcome.to_string(),
        }
    }

    // ---- facts a script pins ----

    /// The end of `segment` in the register.
    fn end_of(&self, segment: u64) -> Option<Slot> {
        self.register.segment(SegmentId(segment))?.end()
    }

    /// The kind of the last answer the client heard for `name`.
    fn heard(&self, name: &str) -> Option<&str> {
        self.heard
            .get(name)
            .map(|answer| answer.split([' ', ':']).next().unwrap_or_default())
    }

    /// Two values chosen at one slot, in words.
    fn broken(&self) -> Option<String> {
        self.chosen
            .iter()
            .find(|(_, values)| values.len() > 1)
            .map(|(slot, values)| {
                let both: Vec<String> = values
                    .iter()
                    .map(|(value, segment)| format!("{value} in the grid of segment {segment}"))
                    .collect();
                format!(
                    "Two values are chosen at slot {}: {}.",
                    slot.0,
                    both.join(" and ")
                )
            })
    }

    // ---- the wire ----

    fn token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        token
    }

    /// Put one message on the wire. `fan` names the broadcast it belongs to.
    fn send(&mut self, from: &str, to: &str, payload: Payload, fan: Option<u64>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.wire.push(InFlight {
            id,
            fan: fan.unwrap_or(id),
            from: from.to_string(),
            to: to.to_string(),
            payload,
        });
        id
    }

    /// Send a group of messages from one lane: the messages of one kind in
    /// one group leave as one broadcast.
    fn send_all(&mut self, from: &str, out: Vec<(String, Payload)>) {
        let mut fans: BTreeMap<&'static str, u64> = BTreeMap::new();
        for (to, payload) in out {
            let kind = kind_of(&payload);
            let fan = fans.get(kind).copied();
            let id = self.send(from, &to, payload, fan);
            fans.entry(kind).or_insert(id);
        }
    }

    fn flight(&self, kind: &str, from: Option<&str>, to: Option<&str>) -> Option<u64> {
        self.wire
            .iter()
            .find(|f| {
                kind_of(&f.payload) == kind
                    && from.is_none_or(|from| f.from == from)
                    && to.is_none_or(|to| f.to == to)
            })
            .map(|f| f.id)
    }

    /// Deliver (or drop) the message `id`, and record the step.
    fn move_flight(&mut self, id: u64, deliver: bool) {
        let at = self
            .wire
            .iter()
            .position(|f| f.id == id)
            .expect("a message on the wire");
        let flight = self.wire.remove(at);
        let leaders_before = self.leader_lines();
        let counted = if deliver { self.arrive(&flight) } else { None };
        if !self.recording {
            return;
        }
        let (fields, voice) = content(&flight.payload);
        let says = if deliver {
            self.says(&flight, &leaders_before)
        } else {
            format!(
                "The {} from {} to {} is lost.",
                kind_of(&flight.payload),
                party(&flight.from),
                party(&flight.to)
            )
        };
        let step = Step {
            kind: if deliver { "deliver" } else { "drop" }.to_string(),
            from: Some(flight.from.clone()),
            to: Some(flight.to.clone()),
            label: label_of(&flight.payload),
            phase: Some(phase_of(&flight.payload).to_string()),
            reply: is_reply(&flight.payload),
            stage: stage_of(&flight.payload),
            says,
            notes: std::mem::take(&mut self.notes),
            fields,
            voice,
            state: Vec::new(),
            chosen: Vec::new(),
            tally: counted,
            logs: Vec::new(),
            broken: None,
            message: Some(flight.id),
            wire: Vec::new(),
            nodes: Vec::new(),
        };
        self.push(step);
    }

    /// An event step: the effects already on the wire, a label and a
    /// sentence.
    fn event(&mut self, label: &str, says: &str) {
        if !self.recording {
            return;
        }
        let step = Step {
            kind: "event".to_string(),
            from: None,
            to: None,
            label: label.to_string(),
            phase: None,
            reply: false,
            stage: None,
            says: says.to_string(),
            notes: std::mem::take(&mut self.notes),
            fields: Vec::new(),
            voice: String::new(),
            state: Vec::new(),
            chosen: Vec::new(),
            tally: None,
            logs: Vec::new(),
            broken: None,
            message: None,
            wire: Vec::new(),
            nodes: Vec::new(),
        };
        self.push(step);
    }

    /// Fill a step with the state of every lane after it, and keep it.
    fn push(&mut self, mut step: Step) {
        step.state = self.state_lines();
        step.chosen = self.chosen_lines();
        step.logs = self.logs();
        step.broken = self.broken();
        step.wire = self.flights();
        step.nodes = self.nodes();
        self.writes.clear();
        self.steps.push(step);
    }

    // ---- delivery: the roles act ----

    /// Hand `flight` to its receiver. Returns the tally that the delivery
    /// changed, if any.
    fn arrive(&mut self, flight: &InFlight) -> Option<Tally> {
        let to = flight.to.clone();
        match &flight.payload {
            Payload::Role(message) => self.step_role(flight, message),
            Payload::Write { token, command } => {
                let node = node_of(&to);
                let leader = self.leaders.get_mut(&node).expect("a leader");
                let (proposed, effects) = leader.propose(*token, command.clone());
                match proposed {
                    Proposed::Slot(slot) => self.notes.push(format!(
                        "{} holds the write at slot {}.",
                        party(&to),
                        slot.0
                    )),
                    Proposed::NotLeader => {
                        self.notes
                            .push(format!("{} does not write now.", party(&to)));
                    }
                }
                self.leader_effects(node, effects);
                None
            }
            Payload::Reply { token, reply } => {
                let name = self.sent.get(token).map(|sent| sent.name.clone());
                if let Some(name) = name {
                    self.heard.insert(name, reply_text(reply));
                }
                None
            }
            Payload::Change(change) => {
                self.apply_change(&flight.from, change);
                None
            }
            Payload::Done { refused, .. } => {
                // The controller acts on nothing more here.
                if to == "ctl" {
                    return None;
                }
                let node = node_of(&to);
                if self.leaders.contains_key(&node) {
                    let value = self.register.clone();
                    let leader = self.leaders.get_mut(&node).expect("a leader");
                    let effects = if refused.is_some() {
                        leader.refused()
                    } else {
                        leader.observe(&value)
                    };
                    self.leader_effects(node, effects);
                } else if self.replicas.contains_key(&node) {
                    self.observe_replica(node);
                }
                None
            }
            Payload::Resolve { name, write } => {
                let node = node_of(&to);
                let Fence::Leader { uuid, expected_seq } = write.fence else {
                    unreachable!("the client's writes are fenced");
                };
                // Each disk write of the story is durable at once, so the
                // batch that the answer waits on is on the disk.
                let (answer, _) = self.replicas[&node].resolve(uuid, expected_seq, &write.records);
                self.send(
                    &to,
                    &flight.from,
                    Payload::Resolved {
                        name: name.clone(),
                        answer,
                    },
                    None,
                );
                None
            }
            Payload::Resolved { name, answer } => {
                self.heard.insert(name.clone(), resolved_text(*answer));
                None
            }
        }
    }

    /// Hand a role message to the role of its receiver.
    fn step_role(&mut self, flight: &InFlight, message: &Message) -> Option<Tally> {
        let from = node_of(&flight.from);
        let node = node_of(&flight.to);
        if self.acceptors.contains_key(&node) {
            let effects = self
                .acceptors
                .get_mut(&node)
                .expect("an acceptor")
                .step(from, message.clone());
            self.acceptor_effects(node, effects);
            None
        } else if self.leaders.contains_key(&node) {
            let effects = self
                .leaders
                .get_mut(&node)
                .expect("a leader")
                .step(from, message.clone());
            let tally = self.count(node, from, message, &effects);
            self.leader_effects(node, effects);
            tally
        } else {
            let effects = self
                .replicas
                .get_mut(&node)
                .expect("a replica")
                .step(from, message.clone());
            self.replica_effects(node, effects);
            None
        }
    }

    /// The register runs `change` from `origin`.
    fn apply_change(&mut self, origin: &str, change: &Change) {
        match self.register.apply(change) {
            Ok(value) => {
                self.register = value;
                self.writes
                    .entry("m".to_string())
                    .or_default()
                    .push(change_text(change));
                self.notes
                    .push(format!("The matchmakers write: {}.", change_text(change)));
                self.send(
                    "m",
                    origin,
                    Payload::Done {
                        change: change.clone(),
                        refused: None,
                    },
                    None,
                );
                if let Change::Reshape { opening, .. } = change {
                    let id = self.register.newest().expect("a new segment").id();
                    for node in &opening.replicas {
                        let disk = ReplicaDisk::default();
                        let role = SegmentReplica::recover(
                            *node,
                            id,
                            opening.replicas.clone(),
                            Leader::Unset,
                            disk.clone(),
                        );
                        self.replicas.insert(*node, role);
                        self.replica_disks.insert(*node, disk);
                    }
                }
                // Every other role reads the new value.
                let replicas: Vec<NodeId> = self.replicas.keys().copied().collect();
                for node in replicas {
                    if lane_of(node) != origin {
                        self.observe_replica(node);
                    }
                }
                let leaders: Vec<NodeId> = self.leaders.keys().copied().collect();
                for node in leaders {
                    if lane_of(node) != origin {
                        let value = self.register.clone();
                        let effects = self
                            .leaders
                            .get_mut(&node)
                            .expect("a leader")
                            .observe(&value);
                        self.leader_effects(node, effects);
                    }
                }
            }
            Err(refusal) => {
                self.notes
                    .push(format!("The matchmakers refuse: {refusal:?}."));
                self.send(
                    "m",
                    origin,
                    Payload::Done {
                        change: change.clone(),
                        refused: Some(refusal),
                    },
                    None,
                );
            }
        }
    }

    fn observe_replica(&mut self, node: NodeId) {
        let value = self.register.clone();
        let effects = self
            .replicas
            .get_mut(&node)
            .expect("a replica")
            .observe(&value);
        self.replica_effects(node, effects);
    }

    /// The controller's reshape call, on the wire to the matchmakers.
    fn reshape_to(&mut self, column: &[u64], replica: u64) {
        let newest = self.register.newest().map(SegmentRecord::id);
        let grid = Grid::new(1, vec![column.iter().copied().map(NodeId).collect()])
            .expect("a v1 grid of one column");
        let change = Change::Reshape {
            opening: OpeningCall {
                term: self.register.term().number,
                expected: newest,
                grid,
                replicas: vec![NodeId(replica)],
            },
            release: RELEASE,
        };
        self.send("ctl", "m", Payload::Change(change), None);
    }

    /// The first slot a new leader reads: the applied prefix of the oldest
    /// segment that is not retired.
    fn applied_from(&self, value: &RegisterValue) -> Slot {
        let oldest = value
            .segments()
            .values()
            .find(|record| record.state() < SegmentState::Retired)
            .expect("a segment");
        oldest
            .replicas()
            .iter()
            .filter_map(|node| self.replicas.get(node)?.front())
            .chain(oldest.start())
            .max()
            .unwrap_or(Slot(0))
    }

    fn leader_effects(&mut self, node: NodeId, effects: Vec<LeaderEffect<u64>>) {
        let lane = lane_of(node);
        let mut out = Vec::new();
        for effect in effects {
            match effect {
                LeaderEffect::Send { to, message } => {
                    if let Message::Commit {
                        segment,
                        slot,
                        command,
                    } = &message
                    {
                        // The leader's Phase-2 tally chose the slot in its
                        // own grid.
                        let own = self.leaders[&node].segment();
                        self.chosen
                            .entry(*slot)
                            .or_default()
                            .entry(value_name(command))
                            .or_insert(own.0);
                        let _ = segment;
                    }
                    out.push((lane_of(to), Payload::Role(message)));
                }
                LeaderEffect::Register(change) => {
                    out.push(("m".to_string(), Payload::Change(change)));
                }
                LeaderEffect::Reply { token, reply } => {
                    out.push(("c".to_string(), Payload::Reply { token, reply }));
                }
            }
        }
        self.send_all(&lane, out);
    }

    fn acceptor_effects(&mut self, node: NodeId, effects: Vec<AcceptorEffect>) {
        let lane = lane_of(node);
        let mut out = Vec::new();
        for effect in effects {
            match effect {
                AcceptorEffect::Persist { id, ops } => {
                    let written: Vec<String> = ops.iter().map(acceptor_write).collect();
                    self.writes.entry(lane.clone()).or_default().extend(written);
                    self.acceptor_disks
                        .get_mut(&node)
                        .expect("an acceptor's disk")
                        .apply(id, &ops);
                }
                // The batch is durable at once, so the send leaves now.
                AcceptorEffect::Send { to, message, .. } => {
                    out.push((lane_of(to), Payload::Role(message)));
                }
            }
        }
        self.send_all(&lane, out);
    }

    fn replica_effects(&mut self, node: NodeId, effects: Vec<ReplicaEffect>) {
        let lane = lane_of(node);
        let mut out = Vec::new();
        for effect in effects {
            match effect {
                ReplicaEffect::Persist { id, ops } => {
                    let written: Vec<String> = ops.iter().map(replica_write).collect();
                    self.writes.entry(lane.clone()).or_default().extend(written);
                    self.replica_disks
                        .get_mut(&node)
                        .expect("a replica's disk")
                        .apply(id, &ops);
                }
                ReplicaEffect::Send { to, message, .. } => {
                    out.push((lane_of(to), Payload::Role(message)));
                }
                ReplicaEffect::Register { change, .. } => {
                    out.push(("m".to_string(), Payload::Change(change)));
                }
            }
        }
        self.send_all(&lane, out);
    }

    /// The tally a leader's step changed: a promise of its Phase 1, or a
    /// vote of a slot.
    fn count(
        &mut self,
        leader: NodeId,
        from: NodeId,
        message: &Message,
        effects: &[LeaderEffect<u64>],
    ) -> Option<Tally> {
        let role = &self.leaders[&leader];
        let ballot = role.ballot();
        match message {
            Message::Promise { ballot: b, .. } if *b == ballot => {
                let members = self.counted.entry((leader, None)).or_default();
                members.insert(from);
                let members = members.clone();
                Some(Tally {
                    what: "promises".to_string(),
                    at: lane_of(leader),
                    ballot: ballot_text(ballot),
                    slot: None,
                    members: members.iter().map(|node| lane_of(*node)).collect(),
                    need: "2 of the 3 acceptors of each grid".to_string(),
                    quorum: *role.phase() != LeaderPhase::Phase1,
                    line: None,
                })
            }
            Message::Accepted { ballot: b, slot } if *b == ballot => {
                let members = self.counted.entry((leader, Some(*slot))).or_default();
                members.insert(from);
                let members = members.clone();
                if effects.iter().any(|effect| {
                    matches!(effect, LeaderEffect::Send { message: Message::Commit { slot: s, .. }, .. } if s == slot)
                }) {
                    self.committed.insert((leader, *slot));
                }
                let chosen = self.committed.contains(&(leader, *slot));
                Some(Tally {
                    what: "votes".to_string(),
                    at: lane_of(leader),
                    ballot: ballot_text(ballot),
                    slot: Some(slot.0),
                    members: members.iter().map(|node| lane_of(*node)).collect(),
                    need: "2 of the 3 acceptors of its column".to_string(),
                    quorum: chosen,
                    line: None,
                })
            }
            _ => None,
        }
    }

    // ---- what the page draws ----

    /// The phase of each leader, for the sentence of a step.
    fn leader_lines(&self) -> BTreeMap<NodeId, String> {
        self.leaders
            .iter()
            .map(|(node, leader)| (*node, phase_text(leader.phase())))
            .collect()
    }

    /// One short sentence on a delivery.
    fn says(&self, flight: &InFlight, before: &BTreeMap<NodeId, String>) -> String {
        let to = party(&flight.to);
        let from = party(&flight.from);
        let wrote = self.writes.get(&flight.to).cloned().unwrap_or_default();
        let mut says = match &flight.payload {
            Payload::Role(Message::Prepare { ballot, .. }) if wrote.is_empty() => {
                if self.acceptors[&node_of(&flight.to)].promised() == *ballot {
                    format!("{to} already holds this promise, and answers again.")
                } else {
                    format!(
                        "{to} holds a promise above ballot {}, so it refuses.",
                        ballot_text(*ballot)
                    )
                }
            }
            Payload::Role(Message::Prepare { ballot, from: slot }) => format!(
                "{to} writes its promise for ballot {}, then sends its votes from slot {}.",
                ballot_text(*ballot),
                slot.0
            ),
            Payload::Role(Message::Promise { .. }) => {
                format!("{to} counts the promise of {}.", lower(&from))
            }
            Payload::Role(Message::Nack { promised, .. }) => format!(
                "{to} learns that a higher ballot, {}, holds the promise.",
                ballot_text(*promised)
            ),
            Payload::Role(Message::Accept { slot, command, .. }) if !wrote.is_empty() => format!(
                "{to} writes its vote for {} at slot {}, then answers.",
                value_name(command),
                slot.0
            ),
            Payload::Role(Message::Accept { ballot, slot, .. }) => {
                if self.acceptors[&node_of(&flight.to)]
                    .vote(*slot)
                    .is_some_and(|(voted, _)| voted == ballot)
                {
                    format!("{to} already holds this vote, and answers again.")
                } else {
                    format!(
                        "{to} holds a promise above ballot {}, so it refuses.",
                        ballot_text(*ballot)
                    )
                }
            }
            Payload::Role(Message::Accepted { slot, .. }) => {
                format!(
                    "{to} counts the vote of {} for slot {}.",
                    lower(&from),
                    slot.0
                )
            }
            Payload::Role(Message::Commit { slot, command, .. }) if !wrote.is_empty() => format!(
                "{to} writes slot {} = {} and applies it in order.",
                slot.0,
                value_name(command)
            ),
            Payload::Role(Message::Commit { slot, .. }) => {
                if self
                    .replicas
                    .get(&node_of(&flight.to))
                    .is_some_and(|replica| replica.chosen().contains_key(slot))
                {
                    format!("{to} already holds slot {}.", slot.0)
                } else {
                    format!(
                        "{to} does not hold slot {}: it is not in its segment.",
                        slot.0
                    )
                }
            }
            Payload::Role(Message::Applied { slot, answer, .. }) => format!(
                "{to} learns that slot {} applied: {}.",
                slot.0,
                answer_text(answer)
            ),
            Payload::Role(Message::Pull { .. }) => format!("{to} sends the slots it holds."),
            Payload::Write { .. } => format!("{to} takes the write."),
            Payload::Reply { reply, .. } => format!("The client hears: {}.", reply_text(reply)),
            Payload::Change(change) => match self.notes.iter().find(|n| n.contains("refuse")) {
                Some(_) => format!("The matchmakers refuse: {}.", change_text(change)),
                None => format!("The matchmakers write: {}.", change_text(change)),
            },
            Payload::Done { refused: None, .. } => format!("{to} learns that its write is done."),
            Payload::Done { .. } => format!("{to} learns that its write is refused."),
            Payload::Resolve { .. } => format!("{to} checks the seq in its fold of its segment."),
            Payload::Resolved { answer, .. } => {
                format!("The client learns: {}.", resolved_text(*answer))
            }
        };
        // A leader whose phase moved says so.
        for (node, leader) in &self.leaders {
            let now = phase_text(leader.phase());
            if before.get(node) != Some(&now) {
                says.push(' ');
                let _ = write!(says, "{} {}.", party(&lane_of(*node)), now);
            }
        }
        says
    }

    /// The short lines of every lane, in lane order.
    fn state_lines(&self) -> Vec<Vec<String>> {
        lanes()
            .iter()
            .map(|lane| match lane.id.as_str() {
                "c" => {
                    let mut lines = vec!["leader A".to_string()];
                    for (name, answer) in &self.heard {
                        lines.push(format!("{name}: {answer}"));
                    }
                    lines
                }
                "ctl" => vec![format!("term {}", self.register.term().number)],
                "m" => self
                    .register
                    .segments()
                    .values()
                    .map(|record| {
                        let mut line =
                            format!("segment {}: {}", record.id().0, state_text(record.state()));
                        if let Some(start) = record.start() {
                            let _ = write!(line, " · from slot {}", start.0);
                        }
                        if let Some(end) = record.end() {
                            let _ = write!(line, " · end {}", end.0);
                        }
                        if let Some(boundary) = record.boundary() {
                            let _ = write!(line, " · next_seq {}", boundary.next_seq.0);
                        }
                        line
                    })
                    .collect(),
                id => {
                    let node = node_of(id);
                    if let Some(leader) = self.leaders.get(&node) {
                        vec![
                            format!(
                                "ballot {} · segment {}",
                                ballot_text(leader.ballot()),
                                leader.segment().0
                            ),
                            phase_text(leader.phase()),
                        ]
                    } else if let Some(acceptor) = self.acceptors.get(&node) {
                        vec![format!("promise {}", ballot_text(acceptor.promised()))]
                    } else if let Some(replica) = self.replicas.get(&node) {
                        let mut lines = vec![format!("segment {}", replica.segment().0)];
                        if let Some(state) = replica.state() {
                            lines.push(format!("next_seq {}", state.next_seq().0));
                        } else {
                            lines.push("no base yet".to_string());
                        }
                        if let Some(end) = replica.end() {
                            lines.push(format!("end {}", end.0));
                        }
                        lines
                    } else {
                        vec!["not started".to_string()]
                    }
                }
            })
            .collect()
    }

    /// What is chosen, slot by slot.
    fn chosen_lines(&self) -> Vec<String> {
        self.chosen
            .iter()
            .map(|(slot, values)| {
                let names: Vec<&str> = values.keys().map(String::as_str).collect();
                format!("slot {} = {}", slot.0, names.join(" and "))
            })
            .collect()
    }

    /// The log strip of every lane: an acceptor's votes, a replica's
    /// chosen slots.
    fn logs(&self) -> Vec<Vec<LogCell>> {
        lanes()
            .iter()
            .map(|lane| {
                if !matches!(lane.role.as_str(), "acceptor" | "replica") {
                    return Vec::new();
                }
                let node = node_of(&lane.id);
                if let Some(acceptor) = self.acceptors.get(&node) {
                    (0..SLOTS)
                        .filter_map(|slot| {
                            let (_, command) = acceptor.vote(Slot(slot))?;
                            Some(cell(slot, command, "accepted"))
                        })
                        .collect()
                } else {
                    self.replicas.get(&node).map_or_else(Vec::new, |replica| {
                        replica
                            .chosen()
                            .iter()
                            .map(|(slot, command)| cell(slot.0, command, "chosen"))
                            .collect()
                    })
                }
            })
            .collect()
    }

    /// The typed state of every lane, with the disk writes of the step.
    fn nodes(&self) -> Vec<NodeState> {
        lanes()
            .iter()
            .map(|lane| {
                let mut state = NodeState {
                    alive: true,
                    writes: self.writes.get(&lane.id).cloned().unwrap_or_default(),
                    ..NodeState::default()
                };
                if lane.role == "acceptor" || lane.role == "proposer" || lane.role == "replica" {
                    let node = node_of(&lane.id);
                    if let Some(leader) = self.leaders.get(&node) {
                        state.role = Some(
                            match leader.phase() {
                                LeaderPhase::Open { .. } => "leader",
                                LeaderPhase::Stopped => "follower",
                                _ => "candidate",
                            }
                            .to_string(),
                        );
                        state.ballot = Some(ballot_text(leader.ballot()));
                    } else if let Some(acceptor) = self.acceptors.get(&node) {
                        state.promise = Some(ballot_text(acceptor.promised()));
                    }
                }
                state
            })
            .collect()
    }

    /// The messages on the wire, oldest first.
    fn flights(&self) -> Vec<Flight> {
        self.wire
            .iter()
            .map(|f| Flight {
                id: f.id,
                from: f.from.clone(),
                to: f.to.clone(),
                phase: phase_of(&f.payload).to_string(),
                label: label_of(&f.payload),
                fan: f.fan,
            })
            .collect()
    }
}

mod words;

use words::{
    acceptor_write, answer_text, ballot_text, cell, change_text, content, is_reply, kind_of,
    label_of, lane_of, lower, node_of, party, phase_of, phase_text, replica_write, reply_text,
    resolved_text, stage_of, state_text, value_name,
};

#[cfg(test)]
mod tests;
