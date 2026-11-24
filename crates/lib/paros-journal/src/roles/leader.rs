//! The **journal leader** (§J4.1, §J3.1): the consensus leader of one
//! ballot, from its Phase 1 to its writes.
//!
//! A leader belongs to one ballot and keeps its state in memory: a leader
//! that stops is a ballot that takes no more steps (`specs/tla/journal-state/`).
//!
//! The steps, each from the model (`Register`, `Phase1`, `SealWrite`,
//! `Open`, `Propose`, and CTRL's `ProbeDecide`, `LearnChosen` and
//! `AccRepair`):
//!
//! 0. **The bind** (§J3.4): the leader binds its ballot to the newest
//!    segment by compare-and-set on the register ([`Change::Bind`]), and
//!    takes its configuration and the segments its Phase 1 reads from the
//!    value of that write once it is done. The register takes a bind only
//!    above every bound ballot, so a higher ballot is bound to the same
//!    segment or a later one; a bind of the bound ballot is ok again while
//!    the newest segment is the one it is bound to (a lost reply). Before
//!    its Phase 1 the leader takes any value that names its ballot as
//!    bound, a later one too (model `Reread`, check `bind_late`). A
//!    leader never binds from a value it was
//!    given: a value read before a seal binds a ballot above the seal to
//!    the sealed grid, and two values are chosen (control `bind_stale`).
//! 1. **Phase 1** ([`phase1`]) asks every acceptor of every segment that is
//!    not retired, from the first slot it writes, and needs a Phase-1 quorum
//!    of each grid: `F + 1` of every column (§J5.1). It reads every slot of
//!    every such grid, sealed or not: one value for each slot rests on the
//!    promises alone, never on the seal end (§J3.1, `end_applied_safe` of
//!    `specs/tla/journal-state/`). Each slot takes the highest vote that the
//!    reads saw (P2c).
//! 2. **The seal**: when the leader's segment is pending, the leader is a
//!    first leader of it. It writes the end of the segment before, one above
//!    the highest slot where its Phase 1 saw a vote in the old grid, a CTRL
//!    faulty vote included (#1313), by compare-and-set from "no end". It
//!    opens only on a done write of its own end: a first leader whose end
//!    lost to another stops (control `end_read`).
//! 3. **The gap fill**: the leader writes every slot from the first slot it
//!    writes up to its next slot: the vote that its Phase 1 saw, or a `Noop`
//!    (#1233). Without the `Noop`, a slot below the end with no vote is a hole
//!    that no ballot writes, because the old grid is sealed (control
//!    `gap_off`). A slot whose top report is a faulty vote, and that the
//!    tri-state tally does not decide, is blocked: the leader proposes
//!    nothing there, and no new command takes it (CTRL, §J5.3).
//! 4. **Phase 2**: every slot is written in the leader's own grid, in one
//!    column (`slot % columns`), and is chosen at `F + 1` acceptors of that
//!    column (#1257). The tally is the core's [`Rounds`].
//! 5. **Commit**: a chosen slot goes to the replica set of its segment, and
//!    only there (#1271). The leader sends it again until a replica of the
//!    set answers `Applied`. With proxies the leader sends it too, on the
//!    proxy's `Chosen`: the proxy keeps nothing, so the leader is the one
//!    role that can send a `Commit` again, and a replica takes a second
//!    `Commit` of a slot as a no-op.
//! 6. **Held answers** (#1232): the answer of a command the leader proposed
//!    goes to its caller when the slot applied that command, else `Unknown`.
//!    A leader that stops answers `Unknown` for every held command.
//! 7. **The retire** (§J3.1, §J3.4): an open leader whose Phase 1 read a
//!    sealed segment asks its replicas `Holding`, and retires it by
//!    compare-and-set once `F + 1` replicas of its set answered that they
//!    hold every slot (control `retire_f`). The register refuses the retire
//!    until the boundary state is written (control `retire_norecord`); the
//!    leader asks again at its next tick. Only once a done value shows the
//!    segment retired does the leader say that the old acceptors may stop
//!    ([`LeaderEffect::Retired`], `RetireKept`).
//! 8. **The compaction** (§J3.2, #1318, `specs/tla/journal-compact/`): an
//!    open leader asks the replicas of each segment it reads for their
//!    compaction points (`Compaction`), and sends `Compact` to every
//!    acceptor it reads once `F + 1` replicas of the set compacted to a
//!    point (control `compact_floor`: the floor alone drops votes that no
//!    replica applied). A held command whose slot is below the answers of
//!    `F + 1` replicas is answered `Unknown`: its answer is gone. The gap
//!    fill skips every slot an acceptor dropped, and the next slot is above
//!    them.
//! 9. **CTRL repair** (§J5.3, `specs/tla/journal-ctrl/`): at each tick an
//!    open leader with a blocked slot asks a `Prepare` again of each
//!    acceptor that has not answered completely, and a `Learn` of each
//!    replica of the slot's segment. A late answer folds into the tally,
//!    which then decides the slot (`ProbeDecide`); a replica's `Learned`
//!    gives it the chosen command (`LearnChosen`). Either way the leader
//!    proposes that command at its ballot. A late answer that dropped the
//!    slot never enters the tally: the slot is chosen and applied at
//!    `F + 1` replicas, and the leader skips it (`ProbeSkip`). Once its round chooses it, the
//!    leader sends a `Repair` to each acceptor that reported the slot
//!    faulty, which writes the command at the higher of the two ballots in
//!    one write (`AccRepair`; control `ctrl_peer`: a value from a peer's
//!    vote that is not known chosen loses a chosen value).

pub mod phase1;

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::contract::limits::RESEND_BATCH;
use paros_consensus::membership::{AcceptorConfig, ProxyId};
use paros_consensus::proposer::{PromiseFold, Rounds};
use paros_consensus::types::Fingerprint;
use paros_consensus::{Ballot, NodeId, Slot};

use super::message::Message;
use super::ranges;
use crate::command::Command;
use crate::fold::Answer;
use crate::held::{Held, Reply};
use crate::register::{Change, RegisterValue};
use crate::segment::{SegmentId, SegmentRecord, SegmentState};

use phase1::{Phase1, Phase1Close};

/// The re-sends of a delegated round after which the leader gives it to the
/// next proxy: a proxy that crashed or lost its messages costs a few ticks.
pub const MOVE_AFTER: u64 = 3;

/// What the driver gives a new leader. The configuration is not in it:
/// the leader's bind on the register chooses it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Campaign {
    /// The leader's ballot.
    pub ballot: Ballot,
    /// The first slot the leader reads and writes: at or below the applied
    /// prefix of the oldest segment that its Phase 1 reads (every slot below
    /// it is applied at a replica of that segment). The applied prefix only
    /// grows, so a prefix that the driver read before the bind stays below
    /// it. A segment that retires before the bind is not read: the leader
    /// then raises the first slot to the start of the oldest segment it
    /// reads ([`JournalLeader::from`]).
    pub from: Slot,
}

/// What one step of the leader asks of its driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderEffect<T> {
    /// Send `message` to `to`. The leader has no disk: it waits on nothing.
    Send {
        /// The receiver.
        to: NodeId,
        /// The message.
        message: Message,
    },
    /// Run this compare-and-set on the journal's register, and give the
    /// leader the value once the write is done ([`JournalLeader::observe`]),
    /// or tell it of the refusal ([`JournalLeader::refused`]).
    Register(Change),
    /// Answer the caller of a held command.
    Reply {
        /// The caller's token.
        token: T,
        /// The answer.
        reply: Reply,
    },
    /// A done write retired `segment`: its acceptors that no later grid
    /// names may stop (§J3.4, `RetireKept`). Given once.
    Retired {
        /// The retired segment.
        segment: SegmentId,
    },
}

/// Where the leader is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderPhase {
    /// The bind of the ballot runs: the leader waits for the done value of
    /// its [`Change::Bind`].
    Binding,
    /// Phase 1 runs.
    Phase1,
    /// Phase 1 is done; the write of the seal end runs.
    Sealing {
        /// The end the leader writes.
        end: Slot,
    },
    /// The leader writes.
    Open {
        /// The first slot of its segment.
        start: Slot,
    },
    /// The leader stopped: a refused bind, a higher ballot, a lost seal,
    /// or the driver.
    Stopped,
}

/// The answer to a proposal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proposed {
    /// The command holds `slot`; its answer comes as a
    /// [`LeaderEffect::Reply`].
    Slot(Slot),
    /// The leader does not write (the bind, Phase 1, the seal, stopped): the command
    /// holds no slot, and the caller may send it elsewhere.
    NotLeader,
    /// The leader holds `window` answers already, the most its caller allows
    /// (decision 30: a limit on each size): the command holds no slot, and
    /// the caller may send it again once an answer came.
    Busy,
}

/// A segment as the leader's Phase 1 reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSegment {
    /// The segment.
    pub id: SegmentId,
    /// One configuration for each column of its grid
    /// ([`crate::segment::Grid::column_configs`]).
    pub columns: Vec<AcceptorConfig<NodeId>>,
    /// Its replica set.
    pub replicas: Vec<NodeId>,
    /// Its first slot, once it is in force.
    pub start: Option<Slot>,
    /// Its end, once it is sealed. Phase 1 reads every slot of the grid all
    /// the same: the end routes a slot to its replica set, it never hides a
    /// vote.
    pub end: Option<Slot>,
}

impl ReadSegment {
    /// The replicas of the set that must hold every slot before the retire:
    /// `F + 1`, the write quorum of a column of the grid (§J3.1).
    ///
    /// # Panics
    ///
    /// When the grid has no column.
    #[must_use]
    pub fn acks(&self) -> usize {
        assert!(!self.columns.is_empty(), "a grid has a column");
        let rows = self.columns[0].members().len();
        assert!(rows % 2 == 1, "a column has 2F + 1 acceptors");
        rows / 2 + 1
    }

    /// Every acceptor of the grid.
    pub fn acceptors(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.columns
            .iter()
            .flat_map(|c| c.members().iter().copied())
    }

    /// The column configuration of `slot`: `slot % columns`.
    ///
    /// # Panics
    ///
    /// When the grid has no column.
    #[must_use]
    pub fn column(&self, slot: Slot) -> &AcceptorConfig<NodeId> {
        assert!(!self.columns.is_empty(), "a grid has a column");
        let count = u64::try_from(self.columns.len()).expect("few columns");
        let index = usize::try_from(slot.0 % count).expect("a column index fits");
        &self.columns[index]
    }
}

/// A slot that is chosen and that no replica of its segment applied yet.
#[derive(Clone, Debug)]
struct Unapplied {
    segment: SegmentId,
    command: Command,
}

/// The consensus leader of one ballot.
#[derive(Clone, Debug)]
pub struct JournalLeader<T> {
    campaign: Campaign,
    /// The segment the bind chose, `None` while the bind runs.
    segment: Option<SegmentId>,
    /// The segments Phase 1 reads, oldest first; the last is the leader's.
    /// Empty while the bind runs.
    segments: Vec<ReadSegment>,
    phase: LeaderPhase,
    /// The seal end the leader asked the register for, kept once it opens
    /// or stops: an observation for a judge of why it stopped.
    seal_end: Option<Slot>,
    /// The phase the leader stopped from, once it stopped: an observation
    /// for a judge of why it stopped.
    stopped_from: Option<LeaderPhase>,
    phase1: Phase1,
    /// The votes Phase 1 found: the leader proposes each again.
    safe: BTreeMap<Slot, Command>,
    /// The slots whose top report is a faulty vote: no proposal until the
    /// tally decides them or a replica gives their chosen command.
    blocked: Vec<Slot>,
    /// The slots that were blocked and are proposed now: once one is
    /// chosen, the leader sends a CTRL repair to its faulty acceptors.
    repairs: BTreeSet<Slot>,
    rounds: Rounds<NodeId, Command>,
    /// The next slot a proposal takes.
    next: Slot,
    held: Held<T>,
    unapplied: BTreeMap<Slot, Unapplied>,
    /// The replicas of the sealed segment before the leader's own that
    /// answered that they hold every slot of it.
    holders: BTreeSet<NodeId>,
    /// Whether a done value showed that segment retired.
    retired: bool,
    /// The proxy leaders the leader delegates its rounds to; empty: the
    /// leader runs Phase 2 itself.
    proxies: Vec<NodeId>,
    /// The ranges that Phase 1 saw dropped: no proposal there.
    dropped: BTreeMap<Slot, Slot>,
    /// The compaction point and the first answered slot that each replica
    /// of a segment reported, by segment.
    compaction: BTreeMap<SegmentId, BTreeMap<NodeId, (Slot, Slot)>>,
}

impl<T> JournalLeader<T> {
    /// A leader for `campaign`, and the bind of its ballot: the leader
    /// starts Phase 1 once the done value of the bind reaches it
    /// ([`Self::observe`]), and stops when the register refuses the bind
    /// ([`Self::refused`]).
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn campaign(campaign: Campaign) -> (Self, Vec<LeaderEffect<T>>) {
        let ballot = campaign.ballot;
        let phase1 = Phase1::new(ballot, campaign.from, &[]);
        let leader = Self {
            campaign,
            segment: None,
            segments: Vec::new(),
            phase: LeaderPhase::Binding,
            seal_end: None,
            stopped_from: None,
            phase1,
            safe: BTreeMap::new(),
            blocked: Vec::new(),
            repairs: BTreeSet::new(),
            rounds: Rounds::new(),
            next: Slot(0),
            held: Held::new(),
            unapplied: BTreeMap::new(),
            holders: BTreeSet::new(),
            retired: false,
            proxies: Vec::new(),
            dropped: BTreeMap::new(),
            compaction: BTreeMap::new(),
        };
        leader.assert_invariants();
        (
            leader,
            vec![LeaderEffect::Register(Change::Bind { ballot })],
        )
    }

    /// The done value of the leader's bind: its configuration is the
    /// segment the register bound its ballot to, and its Phase 1 reads every
    /// segment of `value` up to that one that is not retired, its own
    /// pending segment included. Returns the `Prepare`s of Phase 1.
    fn bind_done(&mut self, value: &RegisterValue) -> Vec<LeaderEffect<T>> {
        assert!(self.phase == LeaderPhase::Binding, "a leader binds once");
        let (ballot, own) = value
            .binding()
            .expect("the value of the leader's own bind has a binding");
        assert!(
            ballot == self.campaign.ballot,
            "the value of the leader's own bind"
        );
        let own = value
            .segment(own)
            .expect("a ballot is bound to a segment the register holds");
        assert!(
            own.state() <= SegmentState::InForce,
            "the newest segment is pending or in force"
        );
        let own = own.id();
        let segments: Vec<ReadSegment> = value
            .segments()
            .values()
            .filter(|record| {
                record.id() <= own
                    && record.state() < SegmentState::Retired
                    && (record.state() != SegmentState::Pending || record.id() == own)
            })
            .map(|record| ReadSegment {
                id: record.id(),
                columns: record.grid().column_configs(),
                replicas: record.replicas().to_vec(),
                start: record.start(),
                end: record.end(),
            })
            .collect();
        assert!(
            segments.last().is_some_and(|s| s.id == own),
            "the leader's own segment is the last it reads"
        );
        assert!(
            segments.windows(2).all(|pair| pair[0].id < pair[1].id),
            "the leader reads each segment once, in order"
        );
        // The driver read `from` before the bind. A segment that retired
        // since is not read, and every slot below the start of the oldest
        // segment read is in a retired segment: chosen, and held by `F + 1`
        // replicas of its set. So the leader starts at that start at least,
        // and never fills a chosen slot of a retired segment.
        if let Some(start) = segments.first().and_then(|segment| segment.start)
            && start > self.campaign.from
        {
            probe!(
                reachable,
                "leader: a retire before the bind raises the first slot"
            );
            self.campaign.from = start;
        }
        assert!(
            segments
                .first()
                .and_then(|segment| segment.start)
                .is_none_or(|start| start <= self.campaign.from),
            "the leader reads from the start of its oldest segment or above"
        );
        self.phase1 = Phase1::new(self.campaign.ballot, self.campaign.from, &segments);
        self.segment = Some(own);
        self.segments = segments;
        self.phase = LeaderPhase::Phase1;
        probe!(reachable, "leader: a done bind starts Phase 1");
        self.prepares()
    }

    /// Delegate the Phase 2 of every round to `proxies` (§J4.1, the proxy
    /// leader): round `slot` goes to the proxy at `slot % proxies`, and a
    /// round that stalls moves to the next proxy every [`MOVE_AFTER`]
    /// re-sends. The leader learns each decision from the proxy's
    /// [`Message::Chosen`]. Called before the leader opens a round.
    ///
    /// # Panics
    ///
    /// When the leader already opened a round.
    pub fn delegate_to(&mut self, proxies: Vec<NodeId>) {
        assert!(
            self.rounds.is_empty() && self.unapplied.is_empty(),
            "the leader delegates before it opens a round"
        );
        self.proxies = proxies;
    }

    /// The leader's ballot.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.campaign.ballot
    }

    /// The first slot the leader reads and writes: the campaign's, raised at
    /// the bind to the start of the oldest segment it reads.
    #[must_use]
    pub fn from(&self) -> Slot {
        self.campaign.from
    }

    /// The segment the leader's ballot is bound to, once its bind is done.
    #[must_use]
    pub fn segment(&self) -> Option<SegmentId> {
        self.segment
    }

    /// The bound segment, past the bind.
    fn own(&self) -> SegmentId {
        self.segment.expect("a leader past its bind has a segment")
    }

    /// Where the leader is.
    #[must_use]
    pub fn phase(&self) -> &LeaderPhase {
        &self.phase
    }

    /// The seal end the leader asked the register for, if Phase 1 closed
    /// on the segment before its own. It stays once the leader opens or
    /// stops: an observer reads it to judge a stop.
    #[must_use]
    pub fn seal_end(&self) -> Option<Slot> {
        self.seal_end
    }

    /// The phase the leader was in when it stopped; `None` while it runs.
    /// An observer reads it to judge a stop: each phase stops for its own
    /// causes.
    #[must_use]
    pub fn stopped_from(&self) -> Option<&LeaderPhase> {
        self.stopped_from.as_ref()
    }

    /// The leader's Phase 1: the reports it gathered.
    #[must_use]
    pub fn phase1(&self) -> &Phase1 {
        &self.phase1
    }

    /// The segments the leader's Phase 1 reads, oldest first.
    #[must_use]
    pub fn segments(&self) -> &[ReadSegment] {
        &self.segments
    }

    /// The slots that Phase 1 left blocked (a faulty top report).
    #[must_use]
    pub fn blocked(&self) -> &[Slot] {
        &self.blocked
    }

    /// The segment that holds `slot`, among the segments the leader reads,
    /// once the leader knows its own start.
    #[must_use]
    pub fn segment_of(&self, slot: Slot) -> Option<&ReadSegment> {
        let LeaderPhase::Open { start } = self.phase else {
            return None;
        };
        let own = self.segments.last()?;
        if slot >= start {
            return Some(own);
        }
        self.segments
            .iter()
            .rev()
            .skip(1)
            .find(|segment| segment.start.is_some_and(|s| s <= slot))
    }

    /// Propose `command` for the caller `token`, at `time`, the clock of
    /// the leader's host in nanoseconds: a write carries it as its leader
    /// time (§J2.8, [`Command::stamped`]). The leader holds at most `window`
    /// answers, which its driver chooses: a proposal above it is
    /// [`Proposed::Busy`], so a leader whose rounds do not close (a column
    /// below quorum) holds a bounded memory (decision 30).
    ///
    /// # Panics
    ///
    /// When `window` is 0, or an assertion on its own postconditions fails.
    pub fn propose(
        &mut self,
        token: T,
        command: Command,
        time: u64,
        window: usize,
    ) -> (Proposed, Vec<LeaderEffect<T>>) {
        assert!(window > 0, "a leader holds at least one answer");
        if !matches!(self.phase, LeaderPhase::Open { .. }) {
            return (Proposed::NotLeader, Vec::new());
        }
        if self.held.len() >= window {
            probe!(reachable, "leader: a full window refuses a proposal");
            return (Proposed::Busy, Vec::new());
        }
        let command = command.stamped(time);
        let slot = self.next;
        self.next = Slot(slot.0 + 1);
        self.held.hold(slot, token, command.clone());
        let effects = self.open_round(slot, command);
        assert!(!self.held.is_empty(), "a proposal holds its answer");
        assert!(
            self.held.len() <= window,
            "the leader holds at most its window"
        );
        assert!(self.next > slot, "the next slot moves past a proposal");
        self.assert_invariants();
        (Proposed::Slot(slot), effects)
    }

    /// One step: a message from `from`. A message of another ballot, or a
    /// message that is not for a leader, changes nothing.
    pub fn step(&mut self, from: NodeId, message: Message) -> Vec<LeaderEffect<T>> {
        if self.phase == LeaderPhase::Stopped {
            return Vec::new();
        }
        let effects = match message {
            Message::Promise {
                ballot,
                from: at,
                page,
            } if ballot == self.campaign.ballot && self.phase == LeaderPhase::Phase1 => {
                match self.phase1.fold(from, at, page) {
                    PromiseFold::Continue(next) => {
                        probe!(reachable, "leader: a promise page asks for the next page");
                        vec![self.prepare(from, next)]
                    }
                    PromiseFold::Answered | PromiseFold::Ignored => self.after_promise(),
                }
            }
            Message::Promise {
                ballot,
                from: at,
                page,
            } if ballot == self.campaign.ballot && !self.blocked.is_empty() => {
                // A late answer folds into the tally of the blocked slots
                // (CTRL's repair probe). A slot that a complete answer
                // dropped is skipped first.
                let fold = self.phase1.fold(from, at, page);
                let dropped: Vec<Slot> = self
                    .blocked
                    .iter()
                    .copied()
                    .filter(|slot| self.phase1.dropped_by(*slot).is_some())
                    .collect();
                self.skip_blocked(&dropped);
                match fold {
                    PromiseFold::Continue(next) => vec![self.prepare(from, next)],
                    PromiseFold::Answered | PromiseFold::Ignored => self.probe_blocked(),
                }
            }
            Message::Learned {
                segment,
                slot,
                command,
            } if self.blocked.contains(&slot)
                && self
                    .segment_of(slot)
                    .is_some_and(|s| s.id == segment && s.replicas.contains(&from)) =>
            {
                probe!(
                    reachable,
                    "leader: a replica gives a blocked slot its command"
                );
                self.unblock(slot, command)
            }
            Message::Nack { ballot, promised }
                if ballot == self.campaign.ballot && promised > ballot =>
            {
                probe!(reachable, "leader: a higher promise stops the leader");
                self.stop()
            }
            Message::Accepted { ballot, slot } if ballot == self.campaign.ballot => {
                self.accepted(from, slot)
            }
            Message::Chosen { ballot, slot } if ballot == self.campaign.ballot => self.chosen(slot),
            Message::Applied {
                segment,
                slot,
                command,
                answer,
            } => self.applied(segment, slot, &command, &answer),
            Message::CompactedTo {
                segment,
                compacted,
                answers,
            } => self.compacted(from, segment, compacted, answers),
            Message::Holds { segment, full } => {
                if full
                    && let Some(sealed) = self.sealed()
                    && sealed.id == segment
                    && sealed.replicas.contains(&from)
                {
                    probe!(reachable, "leader: a replica holds the sealed segment");
                    self.holders.insert(from);
                }
                Vec::new()
            }
            _ => Vec::new(),
        };
        self.assert_invariants();
        effects
    }

    /// A register value that a done write gave. A sealing leader opens when
    /// the value holds its own end, and stops when it holds another, or
    /// when a later leader sealed its own segment already. An
    /// open leader stops when the value holds an end of its own segment: a
    /// later leader sealed it, so no round of its own above that end is
    /// ever chosen, and the acceptors of the segment may stop once it
    /// retires and never refuse the rounds that wait.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn observe(&mut self, value: &RegisterValue) -> Vec<LeaderEffect<T>> {
        if self.phase == LeaderPhase::Binding {
            let effects = match value.bound() {
                Some(bound) if bound == self.campaign.ballot => self.bind_done(value),
                Some(bound) if bound > self.campaign.ballot => {
                    // A higher ballot took the register: the leader's bind is
                    // refused, or was never written.
                    probe!(reachable, "leader: a higher bound ballot stops the leader");
                    self.stop()
                }
                _ => Vec::new(),
            };
            self.assert_invariants();
            return effects;
        }
        if let LeaderPhase::Open { .. } = self.phase {
            if value
                .segment(self.own())
                .and_then(SegmentRecord::end)
                .is_some()
            {
                probe!(
                    reachable,
                    "leader: a seal of its own segment stops the leader"
                );
                let effects = self.stop();
                self.assert_invariants();
                return effects;
            }
            let effects = self.observe_retire(value);
            self.assert_invariants();
            return effects;
        }
        if self.phase == LeaderPhase::Phase1 {
            // A done retire that comes during Phase 1 is the last value
            // that shows it: the leader keeps it, or it asks for the retire
            // for ever once it opens.
            if let [.., before, _] = &self.segments[..]
                && value
                    .segment(before.id)
                    .is_some_and(|record| record.state() >= SegmentState::Retired)
            {
                probe!(reachable, "leader: a retire seen in Phase 1 is kept");
                self.retired = true;
            }
            return Vec::new();
        }
        let LeaderPhase::Sealing { end } = self.phase else {
            return Vec::new();
        };
        let before = self
            .before_own()
            .expect("a sealing leader reads the segment before");
        let Some(done) = value.segment(before).and_then(SegmentRecord::end) else {
            return Vec::new();
        };
        let effects = if value
            .segment(self.own())
            .and_then(SegmentRecord::end)
            .is_some()
        {
            // A later leader sealed the leader's own segment: it takes no
            // more writes, and an open leader would hold its callers'
            // commands on acceptors that refuse them.
            probe!(
                reachable,
                "leader: a seal of its own segment stops a sealing leader"
            );
            self.stop()
        } else if done == end {
            probe!(reachable, "leader: a done seal end opens the leader");
            let own = self.segments.len() - 1;
            self.segments[own - 1].end = Some(end);
            self.segments[own].start = Some(end);
            self.open(end)
        } else {
            probe!(reachable, "leader: a seal end of another stops the leader");
            self.stop()
        };
        self.assert_invariants();
        effects
    }

    /// The register refused the leader's bind (a higher ballot is bound)
    /// or its seal (another end won, or the segment moved on). The leader
    /// stops. A refusal is never of the leader's own end: the register
    /// takes a seal of the end it holds "ok" again (§J3.4, "a change asked
    /// again"), so a seal asked again after a lost answer, whose first
    /// write a minority took, makes that write done and opens the leader.
    pub fn refused(&mut self) -> Vec<LeaderEffect<T>> {
        match self.phase {
            LeaderPhase::Binding => {
                probe!(reachable, "leader: a refused bind stops the leader");
                self.stop()
            }
            LeaderPhase::Sealing { .. } => {
                probe!(reachable, "leader: a refused seal stops the leader");
                self.stop()
            }
            _ => Vec::new(),
        }
    }

    /// Stop the leader: every held command is answered `Unknown`.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn stop(&mut self) -> Vec<LeaderEffect<T>> {
        if self.phase != LeaderPhase::Stopped {
            self.stopped_from = Some(self.phase.clone());
        }
        self.phase = LeaderPhase::Stopped;
        self.rounds.clear();
        self.unapplied.clear();
        self.holders.clear();
        self.compaction.clear();
        probe!(
            sometimes,
            !self.held.is_empty(),
            "leader: a stopped leader answers its held commands"
        );
        let effects: Vec<LeaderEffect<T>> = self
            .held
            .release_all()
            .into_iter()
            .map(|(token, reply)| LeaderEffect::Reply { token, reply })
            .collect();
        assert!(self.held.is_empty(), "a stopped leader holds nothing");
        effects
    }

    /// The periodic step: the `Prepare`s that Phase 1 still needs, then at
    /// most `page` open rounds' `Accept`s and the `Commit`s of the `page`
    /// lowest slots that no replica answered. The driver paces the
    /// re-sends with `page`: a re-send of every round floods a bounded
    /// network, and the flood loses the very messages that would close
    /// the rounds.
    ///
    /// # Panics
    ///
    /// When `page` is zero or above [`RESEND_BATCH`].
    pub fn tick(&mut self, page: usize) -> Vec<LeaderEffect<T>> {
        assert!(page > 0, "a re-send page carries at least one round");
        assert!(
            page <= RESEND_BATCH,
            "a re-send page is at most RESEND_BATCH"
        );
        match self.phase {
            LeaderPhase::Binding => vec![LeaderEffect::Register(Change::Bind {
                ballot: self.campaign.ballot,
            })],
            LeaderPhase::Phase1 => self.prepares(),
            LeaderPhase::Sealing { end } => vec![LeaderEffect::Register(Change::Seal {
                segment: self
                    .before_own()
                    .expect("a sealing leader reads the segment before"),
                end,
            })],
            LeaderPhase::Open { .. } => {
                let mut effects = Vec::new();
                for pending in self.rounds.resend_page(page) {
                    probe!(reachable, "leader: an open round is sent again");
                    if pending.proxy.is_some() {
                        effects.extend(self.delegate(pending.slot, &pending.command));
                    } else {
                        effects.extend(self.accepts(pending.slot, &pending.command));
                    }
                }
                let unapplied: Vec<(Slot, SegmentId, Command)> = self
                    .unapplied
                    .iter()
                    .take(page)
                    .map(|(slot, u)| (*slot, u.segment, u.command.clone()))
                    .collect();
                for (slot, segment, command) in unapplied {
                    probe!(reachable, "leader: a chosen slot is committed again");
                    effects.extend(self.commits(segment, slot, &command));
                }
                effects.extend(self.retire());
                effects.extend(self.compactions());
                effects.extend(self.asks(page));
                effects
            }
            LeaderPhase::Stopped => Vec::new(),
        }
    }

    /// The sealed segment before the leader's own, once the leader is open
    /// and until a done value shows it retired.
    fn sealed(&self) -> Option<&ReadSegment> {
        if self.retired || !matches!(self.phase, LeaderPhase::Open { .. }) {
            return None;
        }
        let [.., sealed, _] = &self.segments[..] else {
            return None;
        };
        assert!(
            sealed.end.is_some(),
            "an open leader's segment before is sealed"
        );
        Some(sealed)
    }

    /// The retire step of a tick: ask the replicas that did not answer
    /// "full" yet, and retire at `F + 1` holders.
    fn retire(&self) -> Vec<LeaderEffect<T>> {
        let Some(sealed) = self.sealed() else {
            return Vec::new();
        };
        let mut effects: Vec<LeaderEffect<T>> = sealed
            .replicas
            .iter()
            .filter(|replica| !self.holders.contains(replica))
            .map(|replica| LeaderEffect::Send {
                to: *replica,
                message: Message::Holding { segment: sealed.id },
            })
            .collect();
        if self.holders.len() >= sealed.acks() {
            probe!(reachable, "leader: F + 1 holders retire the sealed segment");
            effects.push(LeaderEffect::Register(Change::Retire {
                segment: sealed.id,
                holders: self.holders.len(),
                unrepaired_damage: false,
            }));
        }
        effects
    }

    /// A done value that shows the sealed segment retired: the old
    /// acceptors may stop. The replicas of a retired segment drop their
    /// copies and answer nothing, so a chosen slot of the segment that no
    /// replica answered yet is answered `Unknown` now: its resend would
    /// wait for ever, and the lowest unanswered slots fill the page of
    /// every tick.
    fn observe_retire(&mut self, value: &RegisterValue) -> Vec<LeaderEffect<T>> {
        let Some(sealed) = self.sealed() else {
            return Vec::new();
        };
        let segment = sealed.id;
        if value
            .segment(segment)
            .is_some_and(|record| record.state() >= SegmentState::Retired)
        {
            probe!(reachable, "leader: a done value shows the retire");
            self.retired = true;
            self.holders.clear();
            let mut gone: Vec<Slot> = self
                .unapplied
                .iter()
                .filter(|(_, u)| u.segment == segment)
                .map(|(slot, _)| *slot)
                .collect();
            // Every slot of a retired segment is chosen and held by `F + 1`
            // replicas: a round there decides nothing new, and its commit
            // would go to replicas that dropped their copies.
            let rounds: Vec<Slot> = self
                .rounds
                .by_slot()
                .keys()
                .copied()
                .filter(|slot| self.segment_of(*slot).is_some_and(|s| s.id == segment))
                .collect();
            for slot in rounds {
                self.rounds.close(slot);
                gone.push(slot);
            }
            let mut effects = vec![LeaderEffect::Retired { segment }];
            for slot in gone {
                probe!(
                    reachable,
                    "leader: a chosen slot of a retired segment is answered Unknown"
                );
                effects.extend(self.forget(slot));
            }
            return effects;
        }
        Vec::new()
    }

    fn prepares(&self) -> Vec<LeaderEffect<T>> {
        self.phase1
            .wanted()
            .into_iter()
            .map(|(to, from)| self.prepare(to, from))
            .collect()
    }

    /// The `Prepare` of the campaign's ballot to `to`, from slot `from`.
    fn prepare(&self, to: NodeId, from: Slot) -> LeaderEffect<T> {
        LeaderEffect::Send {
            to,
            message: Message::Prepare {
                ballot: self.campaign.ballot,
                from,
                segment: self.own(),
            },
        }
    }

    /// The read segment `segment`, when the leader reads it.
    fn read(&self, segment: SegmentId) -> Option<&ReadSegment> {
        self.segments.iter().find(|s| s.id == segment)
    }

    /// Forget a chosen slot whose answer is gone: the held command of its
    /// caller is answered `Unknown`, and the slot leaves the unapplied set.
    fn forget(&mut self, slot: Slot) -> Option<LeaderEffect<T>> {
        self.unapplied.remove(&slot);
        self.held
            .release(slot)
            .map(|(token, reply)| LeaderEffect::Reply { token, reply })
    }

    /// The segment before the leader's own, when the leader's is pending.
    /// The seal writes the end of the one and the start of the other in
    /// one write, so the leader's segment is pending exactly while the one
    /// before has no end ([`Self::assert_invariants`]).
    fn before_own(&self) -> Option<SegmentId> {
        let own = self.segments.last()?;
        let before = self.segments.iter().rev().nth(1)?;
        own.start.is_none().then_some(before.id)
    }

    fn after_promise(&mut self) -> Vec<LeaderEffect<T>> {
        let Some(Phase1Close {
            safe,
            blocked,
            end,
            dropped,
        }) = self.phase1.close(&self.segments)
        else {
            return Vec::new();
        };
        self.safe = safe;
        self.blocked = blocked;
        self.dropped = dropped;
        match (
            self.before_own(),
            self.segments.last().and_then(|s| s.start),
        ) {
            (Some(before), _) => {
                let end = end.expect("Phase 1 over the segment before gives an end");
                probe!(reachable, "leader: a first leader writes the seal end");
                self.phase = LeaderPhase::Sealing { end };
                self.seal_end = Some(end);
                vec![LeaderEffect::Register(Change::Seal {
                    segment: before,
                    end,
                })]
            }
            (None, Some(start)) => {
                probe!(reachable, "leader: a leader of a segment in force opens");
                self.open(start)
            }
            (None, None) => unreachable!("a segment that is not sealing is in force"),
        }
    }

    /// The tally of each blocked slot again, after a late answer: each slot
    /// it decides is proposed (CTRL, §J5.3).
    fn probe_blocked(&mut self) -> Vec<LeaderEffect<T>> {
        let decided: Vec<(Slot, Command)> = self
            .blocked
            .iter()
            .filter_map(|slot| {
                self.phase1
                    .decide(&self.segments, *slot)
                    .map(|command| (*slot, command))
            })
            .collect();
        probe!(
            sometimes,
            !decided.is_empty(),
            "leader: a late answer decides a blocked slot"
        );
        let mut effects = Vec::new();
        for (slot, command) in decided {
            effects.extend(self.unblock(slot, command));
        }
        effects
    }

    /// Each slot of `slots` leaves the blocked slots with no proposal: a
    /// complete answer dropped it, so it is chosen and applied at `F + 1`
    /// replicas (§J3.2), and no proposal and no repair is needed. The tally
    /// must not decide it: the answer that dropped it reports no vote
    /// there, and the other answers may predate the compaction
    /// (`specs/tla/journal-ctrl/`, `ProbeSkip`, and `ProbeDecide` with its
    /// control `ctrl_probe_drop`).
    fn skip_blocked(&mut self, slots: &[Slot]) {
        for slot in slots {
            probe!(
                reachable,
                "leader: a blocked slot that an answer dropped is skipped"
            );
            self.blocked.retain(|s| s != slot);
            ranges::add(&mut self.dropped, *slot, Slot(slot.0 + 1));
        }
        assert!(
            slots.iter().all(|slot| !self.blocked.contains(slot)),
            "a skipped slot is no longer blocked"
        );
    }

    /// `slot` leaves the blocked slots with `command`, the command that is
    /// safe there: an open leader proposes it now, a sealing leader when it
    /// opens.
    fn unblock(&mut self, slot: Slot, command: Command) -> Vec<LeaderEffect<T>> {
        let before = self.blocked.len();
        self.blocked.retain(|s| *s != slot);
        assert!(
            self.blocked.len() + 1 == before,
            "an unblocked slot was blocked"
        );
        self.repairs.insert(slot);
        match self.phase {
            LeaderPhase::Open { .. } => {
                assert!(slot < self.next, "a blocked slot is below the next slot");
                self.open_round(slot, command)
            }
            LeaderPhase::Sealing { .. } => {
                self.safe.insert(slot, command);
                Vec::new()
            }
            LeaderPhase::Binding | LeaderPhase::Phase1 | LeaderPhase::Stopped => {
                unreachable!("only a leader past Phase 1 holds blocked slots")
            }
        }
    }

    /// The asks of the blocked slots, at each tick of an open leader: a
    /// `Prepare` to each acceptor that has not answered completely, and a
    /// `Learn` to each replica of the segment of each blocked slot, at most
    /// `page` slots.
    fn asks(&self, page: usize) -> Vec<LeaderEffect<T>> {
        if self.blocked.is_empty() {
            return Vec::new();
        }
        probe!(reachable, "leader: a tick asks for a blocked slot");
        let mut effects = self.prepares();
        for slot in self.blocked.iter().take(page) {
            let segment = self
                .segment_of(*slot)
                .expect("a blocked slot is in a segment the leader reads");
            effects.extend(segment.replicas.iter().map(|to| LeaderEffect::Send {
                to: *to,
                message: Message::Learn {
                    segment: segment.id,
                    slot: *slot,
                },
            }));
        }
        effects
    }

    /// The leader writes from the first slot it reads: the votes Phase 1
    /// found, a `Noop` in each other slot below its next slot that no
    /// acceptor dropped and Phase 1 did not block, then the callers'
    /// commands, above every vote and every blocked slot.
    fn open(&mut self, start: Slot) -> Vec<LeaderEffect<T>> {
        self.phase = LeaderPhase::Open { start };
        let from = self.campaign.from;
        // A blocked slot counts: its faulty vote may be the chosen value, so
        // no new command takes it.
        let top = self
            .safe
            .keys()
            .chain(self.blocked.iter())
            .max()
            .map(|s| Slot(s.0 + 1));
        let dropped = ranges::top(&self.dropped).map(|s| Slot(s.0 + 1));
        let next = [Some(start), Some(from), top, dropped]
            .into_iter()
            .flatten()
            .max()
            .expect("a start");
        self.next = next;
        let mut effects = Vec::new();
        for slot in (from.0..next.0).map(Slot) {
            if ranges::contains(&self.dropped, slot) {
                probe!(reachable, "leader: a dropped slot is not proposed");
                continue;
            }
            if self.blocked.contains(&slot) {
                probe!(reachable, "leader: a blocked slot is not proposed");
                continue;
            }
            let command = self.safe.get(&slot).cloned().unwrap_or(Command::Noop);
            probe!(
                sometimes,
                command == Command::Noop,
                "leader: the gap fill writes a Noop"
            );
            probe!(
                sometimes,
                command != Command::Noop,
                "leader: the gap fill writes a vote again"
            );
            effects.extend(self.open_round(slot, command));
        }
        assert!(
            self.next == next,
            "the gap fill leaves the next slot as it set it"
        );
        effects
    }

    fn open_round(&mut self, slot: Slot, command: Command) -> Vec<LeaderEffect<T>> {
        if self.proxies.is_empty() {
            let effects = self.accepts(slot, &command);
            self.rounds
                .open(slot, self.campaign.ballot, command, None, None);
            return effects;
        }
        let index = u64::try_from(self.proxies.len()).expect("few proxies");
        self.rounds.open_delegated(
            slot,
            self.campaign.ballot,
            command.clone(),
            None,
            ProxyId(slot.0 % index),
        );
        self.delegate(slot, &command)
    }

    /// The delegation of the open round at `slot` to its proxy: the proxy
    /// at `slot % proxies`, moved to the next one every [`MOVE_AFTER`]
    /// re-sends of the round.
    fn delegate(&self, slot: Slot, command: &Command) -> Vec<LeaderEffect<T>> {
        let round = self
            .rounds
            .by_slot()
            .get(&slot)
            .expect("a delegation is of an open round");
        let count = u64::try_from(self.proxies.len()).expect("few proxies");
        let first = round.proxy().expect("a round with proxies is delegated").0;
        assert!(first < count, "a proxy id is an index of the proxies");
        assert_eq!(first, slot.0 % count, "a round starts at its slot's proxy");
        let moves = round.resends() / MOVE_AFTER;
        probe!(
            sometimes,
            moves > 0,
            "leader: a stalled round moves to another proxy"
        );
        let index = usize::try_from((first + moves) % count).expect("a proxy index");
        let own = self
            .segments
            .last()
            .expect("a leader reads its own segment");
        let segment = self
            .segment_of(slot)
            .expect("a slot the leader writes is in a segment it reads");
        vec![LeaderEffect::Send {
            to: self.proxies[index],
            message: Message::Delegate {
                ballot: self.campaign.ballot,
                slot,
                command: command.clone(),
                column: own.column(slot).members().to_vec(),
                segment: segment.id,
                replicas: segment.replicas.clone(),
            },
        }]
    }

    /// A proxy chose `slot` at the leader's ballot: the round closes, and
    /// the leader commits the slot until a replica applies it.
    fn chosen(&mut self, slot: Slot) -> Vec<LeaderEffect<T>> {
        let Some(round) = self.rounds.by_slot().get(&slot) else {
            return Vec::new();
        };
        if round.proxy().is_none() {
            return Vec::new();
        }
        let command = round.command().clone();
        probe!(reachable, "leader: a proxy chose a delegated round");
        self.decide(slot, command)
    }

    /// The `Accept`s of `slot`: to every acceptor of its column of the
    /// leader's own grid.
    fn accepts(&self, slot: Slot, command: &Command) -> Vec<LeaderEffect<T>> {
        let own = self
            .segments
            .last()
            .expect("a leader reads its own segment");
        own.column(slot)
            .members()
            .iter()
            .map(|to| LeaderEffect::Send {
                to: *to,
                message: Message::Accept {
                    ballot: self.campaign.ballot,
                    slot,
                    command: command.clone(),
                },
            })
            .collect()
    }

    fn accepted(&mut self, from: NodeId, slot: Slot) -> Vec<LeaderEffect<T>> {
        let Some(round) = self.rounds.by_slot().get(&slot) else {
            return Vec::new();
        };
        let hash = round.command().fingerprint();
        let column = self
            .segments
            .last()
            .expect("a leader reads its own segment")
            .column(slot)
            .clone();
        if !self
            .rounds
            .fold_accepted_in(&column, from, self.campaign.ballot, slot, hash)
        {
            return Vec::new();
        }
        let Some((_, command)) = self.rounds.decided(slot, &column) else {
            return Vec::new();
        };
        self.decide(slot, command)
    }

    /// `slot` is chosen with `command`: close its round, and commit it to
    /// the replica set of its segment until a replica applies it.
    fn decide(&mut self, slot: Slot, command: Command) -> Vec<LeaderEffect<T>> {
        self.rounds.close(slot);
        let segment = self
            .segment_of(slot)
            .expect("a slot the leader writes is in a segment it reads")
            .id;
        let mut effects = self.commits(segment, slot, &command);
        if self.repairs.remove(&slot) {
            probe!(reachable, "leader: a chosen blocked slot is repaired");
            effects.extend(
                self.phase1
                    .faulty_at(slot)
                    .into_iter()
                    .map(|to| LeaderEffect::Send {
                        to,
                        message: Message::Repair {
                            ballot: self.campaign.ballot,
                            slot,
                            command: command.clone(),
                        },
                    }),
            );
        }
        self.unapplied.insert(slot, Unapplied { segment, command });
        effects
    }

    /// The `Commit`s of `slot`: to every replica of its segment.
    fn commits(&self, segment: SegmentId, slot: Slot, command: &Command) -> Vec<LeaderEffect<T>> {
        let replicas = &self
            .read(segment)
            .expect("the leader reads the segment of a slot it chose")
            .replicas;
        replicas
            .iter()
            .map(|to| LeaderEffect::Send {
                to: *to,
                message: Message::Commit {
                    segment,
                    slot,
                    command: command.clone(),
                },
            })
            .collect()
    }

    fn applied(
        &mut self,
        segment: SegmentId,
        slot: Slot,
        command: &Command,
        answer: &Answer,
    ) -> Vec<LeaderEffect<T>> {
        if self
            .unapplied
            .get(&slot)
            .is_some_and(|u| u.segment == segment)
        {
            self.unapplied.remove(&slot);
        }
        self.held
            .applied(slot, command, answer)
            .map(|(token, reply)| LeaderEffect::Reply { token, reply })
            .into_iter()
            .collect()
    }

    /// The compaction step of a tick: ask the replicas of each segment in
    /// force for their compaction points, and send the floor of each
    /// segment that `F + 1` of them reached to every acceptor read.
    fn compactions(&self) -> Vec<LeaderEffect<T>> {
        let mut effects = Vec::new();
        for segment in self.segments.iter().filter(|s| s.start.is_some()) {
            effects.extend(segment.replicas.iter().map(|replica| LeaderEffect::Send {
                to: *replica,
                message: Message::Compaction {
                    segment: segment.id,
                },
            }));
            effects.extend(self.compact(segment.id));
        }
        effects
    }

    /// The `F + 1`-th highest of `points` (the replicas that reported):
    /// the slot below which `F + 1` replicas of the set reach.
    fn quorum_point(&self, segment: SegmentId, points: impl Iterator<Item = Slot>) -> Option<Slot> {
        let read = self.read(segment)?;
        let mut points: Vec<Slot> = points.collect();
        points.sort_unstable_by(|a, b| b.cmp(a));
        points.get(read.acks() - 1).copied()
    }

    /// The `Compact` of `segment` to every acceptor that the leader reads,
    /// once `F + 1` replicas of its set compacted above its start.
    fn compact(&self, segment: SegmentId) -> Vec<LeaderEffect<T>> {
        let Some(start) = self.read(segment).and_then(|s| s.start) else {
            return Vec::new();
        };
        let reports = self.compaction.get(&segment);
        let Some(below) = self.quorum_point(
            segment,
            reports
                .into_iter()
                .flat_map(|r| r.values().map(|(c, _)| *c)),
        ) else {
            return Vec::new();
        };
        if below <= start {
            return Vec::new();
        }
        probe!(reachable, "leader: F + 1 replicas compacted a segment");
        let acceptors: BTreeSet<NodeId> = self
            .segments
            .iter()
            .flat_map(ReadSegment::acceptors)
            .collect();
        acceptors
            .into_iter()
            .map(|to| LeaderEffect::Send {
                to,
                message: Message::Compact {
                    segment,
                    start,
                    below,
                },
            })
            .collect()
    }

    /// A replica of `segment` reported its compaction point and its first
    /// answered slot. A held command below the answers of `F + 1` replicas
    /// of the set is answered `Unknown`: no replica of the set gives its
    /// answer now that a quorum of them dropped it. An open round below
    /// the compaction point of `F + 1` replicas closes: its slot is chosen
    /// and applied, and an acceptor that dropped it answers no `Accept`
    /// (a Phase 1 that closed before the drop proposes it).
    fn compacted(
        &mut self,
        from: NodeId,
        segment: SegmentId,
        compacted: Slot,
        answers: Slot,
    ) -> Vec<LeaderEffect<T>> {
        if !matches!(self.phase, LeaderPhase::Open { .. })
            || !self
                .read(segment)
                .is_some_and(|s| s.replicas.contains(&from))
        {
            return Vec::new();
        }
        assert!(answers >= compacted, "a replica answers only what it keeps");
        let report = self
            .compaction
            .entry(segment)
            .or_default()
            .entry(from)
            .or_insert((compacted, answers));
        *report = (report.0.max(compacted), report.1.max(answers));
        let reports = &self.compaction[&segment];
        let (Some(below), Some(answered)) = (
            self.quorum_point(segment, reports.values().map(|(c, _)| *c)),
            self.quorum_point(segment, reports.values().map(|(_, a)| *a)),
        ) else {
            return Vec::new();
        };
        assert!(answered >= below, "F + 1 replicas answer what they keep");
        let in_segment = |slot: Slot| self.segment_of(slot).is_some_and(|s| s.id == segment);
        let applied: Vec<Slot> = self
            .rounds
            .by_slot()
            .keys()
            .copied()
            .filter(|slot| *slot < below && in_segment(*slot))
            .collect();
        let mut gone: Vec<Slot> = self
            .unapplied
            .range(..answered)
            .filter(|(_, u)| u.segment == segment)
            .map(|(slot, _)| *slot)
            .collect();
        for slot in applied {
            probe!(reachable, "leader: an open round below a compaction closes");
            self.rounds.close(slot);
            gone.push(slot);
        }
        let mut effects = Vec::new();
        for slot in gone {
            probe!(
                reachable,
                "leader: a chosen slot whose answer is gone is answered Unknown"
            );
            effects.extend(self.forget(slot));
        }
        effects.extend(self.compact(segment));
        effects
    }

    /// The leader's own rules.
    ///
    /// # Panics
    ///
    /// When an open leader does not know its start, a held command has no
    /// slot below the next slot, or an unapplied slot is open.
    pub fn assert_invariants(&self) {
        assert!(
            self.segment.is_none() == self.segments.is_empty(),
            "a bound leader reads its segments, an unbound one none"
        );
        assert!(
            self.segment.is_some()
                || matches!(self.phase, LeaderPhase::Binding | LeaderPhase::Stopped),
            "only the bind and a stop come before the configuration"
        );
        assert!(
            self.segment.is_none() || self.segments.last().map(|s| s.id) == self.segment,
            "the leader's own segment is the last it reads"
        );
        if let [.., before, own] = &self.segments[..] {
            assert!(
                own.start.is_none() == before.end.is_none(),
                "the seal ends the segment before and starts the next in one write"
            );
        }
        if let LeaderPhase::Open { start } = self.phase {
            assert!(
                self.segments.last().is_some_and(|s| s.start == Some(start)),
                "an open leader knows the start of its segment"
            );
            assert!(
                self.unapplied.keys().all(|slot| *slot < self.next),
                "a chosen slot is below the next slot"
            );
        } else {
            assert!(self.unapplied.is_empty(), "only an open leader chooses");
            assert!(
                self.holders.is_empty(),
                "only an open leader counts holders"
            );
        }
        match self.sealed() {
            Some(sealed) => assert!(
                self.holders.iter().all(|h| sealed.replicas.contains(h)),
                "a holder is a replica of the sealed segment"
            ),
            None => assert!(self.holders.is_empty(), "holders of a sealed segment only"),
        }
        assert!(
            self.unapplied
                .keys()
                .all(|slot| !self.rounds.by_slot().contains_key(slot)),
            "a chosen slot has no open round"
        );
        assert!(
            self.rounds
                .by_slot()
                .keys()
                .all(|slot| !ranges::contains(&self.dropped, *slot)),
            "the leader proposes nothing in a dropped range"
        );
    }
}

#[cfg(test)]
mod tests;
