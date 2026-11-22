//! The **journal leader** (§J4.1, §J3.1): the consensus leader of one
//! ballot, from its Phase 1 to its writes.
//!
//! A leader belongs to one ballot and keeps its state in memory: a leader
//! that stops is a ballot that takes no more steps (`specs/tla/journal-state/`).
//! Its ballot is bound to the newest segment of the register, its
//! configuration (the matchmakers bind it, §J3.4).
//!
//! The steps, each from the model (`Phase1`, `SealWrite`, `Open`,
//! `Propose`):
//!
//! 1. **Phase 1** ([`phase1`]) asks every acceptor of every segment that is
//!    not retired, from the first slot it writes, and needs a Phase-1 quorum
//!    of each grid: `F + 1` of every column (§J5.1). A sealed segment is read
//!    only below its end (§J3.1). Each slot takes the highest vote that the
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
//!    `gap_off`). A slot whose top report is a faulty vote is blocked: the
//!    leader proposes nothing there (CTRL, §J5.3).
//! 4. **Phase 2**: every slot is written in the leader's own grid, in one
//!    column (`slot % columns`), and is chosen at `F + 1` acceptors of that
//!    column (#1257). The tally is the core's [`Rounds`].
//! 5. **Commit**: a chosen slot goes to the replica set of its segment, and
//!    only there (#1271). The leader sends it again until a replica of the
//!    set answers `Applied`.
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

pub mod phase1;

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::contract::limits::RESEND_BATCH;
use paros_consensus::membership::{AcceptorConfig, ProxyId, QuorumSystem};
use paros_consensus::proposer::Rounds;
use paros_consensus::types::Fingerprint;
use paros_consensus::{Ballot, NodeId, Slot};

use super::message::Message;
use crate::command::Command;
use crate::fold::Answer;
use crate::held::{Held, Reply};
use crate::register::{Change, RegisterValue};
use crate::segment::{Grid, SegmentId, SegmentRecord, SegmentState};

use phase1::{Phase1, Phase1Close};

/// The re-sends of a delegated round after which the leader gives it to the
/// next proxy: a proxy that crashed or lost its messages costs a few ticks.
pub const MOVE_AFTER: u64 = 3;

/// What the driver gives a new leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Campaign {
    /// The leader's ballot.
    pub ballot: Ballot,
    /// The configuration the ballot is bound to: the newest segment.
    pub segment: SegmentId,
    /// The first slot the leader reads and writes: the applied prefix of
    /// the oldest segment that its Phase 1 reads (every slot below it is
    /// applied at a replica of that segment).
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
    /// The leader stopped: a higher ballot, a lost seal, or the driver.
    Stopped,
}

/// The answer to a proposal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proposed {
    /// The command holds `slot`; its answer comes as a
    /// [`LeaderEffect::Reply`].
    Slot(Slot),
    /// The leader does not write (Phase 1, the seal, stopped): the command
    /// holds no slot, and the caller may send it elsewhere.
    NotLeader,
}

/// A segment as the leader's Phase 1 reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSegment {
    /// The segment.
    pub id: SegmentId,
    /// One configuration for each column of its grid: a majority of the
    /// column's `2F + 1` acceptors.
    pub columns: Vec<AcceptorConfig<NodeId>>,
    /// Its replica set.
    pub replicas: Vec<NodeId>,
    /// Its first slot, once it is in force.
    pub start: Option<Slot>,
    /// Its end, once it is sealed: Phase 1 reads it only below the end.
    pub end: Option<Slot>,
}

impl ReadSegment {
    /// The column configurations of `grid`: a v1 grid's column is a
    /// majority of `2F + 1` (§J5.1), so `F + 1` of a column decides a slot
    /// and `F + 1` of every column elects.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn columns_of(grid: &Grid) -> Vec<AcceptorConfig<NodeId>> {
        let columns: Vec<AcceptorConfig<NodeId>> = grid
            .columns()
            .iter()
            .map(|column| AcceptorConfig::new(column.clone(), QuorumSystem::Majority))
            .collect();
        assert!(
            columns.len() == grid.columns().len(),
            "one configuration for each column"
        );
        assert!(
            columns.iter().all(|c| c.members().len() == grid.rows()),
            "a column has 2F + 1 acceptors"
        );
        columns
    }

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

    /// Whether Phase 1 reads `slot` here: below the end of a sealed segment.
    #[must_use]
    pub fn reads(&self, slot: Slot) -> bool {
        self.end.is_none_or(|end| slot < end)
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
    /// The segments Phase 1 reads, oldest first; the last is the leader's.
    segments: Vec<ReadSegment>,
    phase: LeaderPhase,
    phase1: Phase1,
    /// The votes Phase 1 found: the leader proposes each again.
    safe: BTreeMap<Slot, Command>,
    /// The slots whose top report is a faulty vote: no proposal.
    blocked: Vec<Slot>,
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
}

impl<T> JournalLeader<T> {
    /// A leader for `campaign`, from `value`, a register value that a done
    /// write gave. Returns the leader and the `Prepare`s of its Phase 1, or
    /// `None` when its segment is not in `value` or is past in force.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn campaign(
        campaign: Campaign,
        value: &RegisterValue,
    ) -> Option<(Self, Vec<LeaderEffect<T>>)> {
        let own = value.segment(campaign.segment)?;
        if own.state() > SegmentState::InForce {
            return None;
        }
        let segments: Vec<ReadSegment> = value
            .segments()
            .values()
            .filter(|record| record.id() <= campaign.segment)
            .filter(|record| {
                record.state() < SegmentState::Retired
                    && (record.state() != SegmentState::Pending || record.id() == campaign.segment)
            })
            .map(|record| ReadSegment {
                id: record.id(),
                columns: ReadSegment::columns_of(record.grid()),
                replicas: record.replicas().to_vec(),
                start: record.start(),
                end: record.end(),
            })
            .collect();
        assert!(
            segments.last().is_some_and(|s| s.id == campaign.segment),
            "the leader's own segment is the last it reads"
        );
        let phase1 = Phase1::new(campaign.ballot, campaign.from, &segments);
        let leader = Self {
            campaign,
            segments,
            phase: LeaderPhase::Phase1,
            phase1,
            safe: BTreeMap::new(),
            blocked: Vec::new(),
            rounds: Rounds::new(),
            next: Slot(0),
            held: Held::new(),
            unapplied: BTreeMap::new(),
            holders: BTreeSet::new(),
            retired: false,
            proxies: Vec::new(),
        };
        let effects = leader.prepares();
        leader.assert_invariants();
        Some((leader, effects))
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

    /// The proxy leaders of the leader.
    #[must_use]
    pub fn proxies(&self) -> &[NodeId] {
        &self.proxies
    }

    /// The leader's ballot.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.campaign.ballot
    }

    /// The segment the leader's ballot is bound to.
    #[must_use]
    pub fn segment(&self) -> SegmentId {
        self.campaign.segment
    }

    /// Where the leader is.
    #[must_use]
    pub fn phase(&self) -> &LeaderPhase {
        &self.phase
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

    /// Propose `command` for the caller `token`.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn propose(&mut self, token: T, command: Command) -> (Proposed, Vec<LeaderEffect<T>>) {
        if !matches!(self.phase, LeaderPhase::Open { .. }) {
            return (Proposed::NotLeader, Vec::new());
        }
        let slot = self.next;
        self.next = Slot(slot.0 + 1);
        self.held.hold(slot, token, command.clone());
        let effects = self.open_round(slot, command);
        assert!(!self.held.is_empty(), "a proposal holds its answer");
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
                    Some(next) => {
                        probe!(reachable, "leader: a promise page asks for the next page");
                        vec![LeaderEffect::Send {
                            to: from,
                            message: Message::Prepare {
                                ballot: self.campaign.ballot,
                                from: next,
                            },
                        }]
                    }
                    None => self.after_promise(),
                }
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
    /// the value holds its own end, and stops when it holds another.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn observe(&mut self, value: &RegisterValue) -> Vec<LeaderEffect<T>> {
        if let LeaderPhase::Open { .. } = self.phase {
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
        let effects = if done == end {
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

    /// The register refused the leader's seal: another seal won, or the
    /// segment moved on. The leader stops.
    pub fn refused(&mut self) -> Vec<LeaderEffect<T>> {
        match self.phase {
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
        self.phase = LeaderPhase::Stopped;
        self.rounds.clear();
        self.unapplied.clear();
        self.holders.clear();
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
    /// acceptors may stop.
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
            return vec![LeaderEffect::Retired { segment }];
        }
        Vec::new()
    }

    fn prepares(&self) -> Vec<LeaderEffect<T>> {
        self.phase1
            .wanted()
            .into_iter()
            .map(|(to, from)| LeaderEffect::Send {
                to,
                message: Message::Prepare {
                    ballot: self.campaign.ballot,
                    from,
                },
            })
            .collect()
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
        let Some(Phase1Close { safe, blocked, end }) = self.phase1.close(&self.segments) else {
            return Vec::new();
        };
        self.safe = safe;
        self.blocked = blocked;
        match (
            self.before_own(),
            self.segments.last().and_then(|s| s.start),
        ) {
            (Some(before), _) => {
                let end = end.expect("Phase 1 over the segment before gives an end");
                probe!(reachable, "leader: a first leader writes the seal end");
                self.phase = LeaderPhase::Sealing { end };
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

    /// The leader writes from the first slot it reads: the votes Phase 1
    /// found, a `Noop` in each other slot below its next slot, then the
    /// callers' commands.
    fn open(&mut self, start: Slot) -> Vec<LeaderEffect<T>> {
        self.phase = LeaderPhase::Open { start };
        let from = self.campaign.from;
        let top = self.safe.keys().next_back().map(|s| Slot(s.0 + 1));
        let next = [Some(start), Some(from), top]
            .into_iter()
            .flatten()
            .max()
            .expect("a start");
        self.next = next;
        let mut effects = Vec::new();
        for slot in (from.0..next.0).map(Slot) {
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
        let resends = self
            .rounds
            .by_slot()
            .get(&slot)
            .map_or(0, paros_consensus::proposer::Round::resends);
        let count = u64::try_from(self.proxies.len()).expect("few proxies");
        let moves = resends / MOVE_AFTER;
        probe!(
            sometimes,
            moves > 0,
            "leader: a stalled round moves to another proxy"
        );
        let index = usize::try_from((slot.0 + moves) % count).expect("a proxy index");
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
        self.unapplied.insert(
            slot,
            Unapplied {
                segment,
                command: command.clone(),
            },
        );
        self.commits(segment, slot, &command)
    }

    /// The `Commit`s of `slot`: to every replica of its segment.
    fn commits(&self, segment: SegmentId, slot: Slot, command: &Command) -> Vec<LeaderEffect<T>> {
        let replicas = &self
            .segments
            .iter()
            .find(|s| s.id == segment)
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

    /// The leader's own rules.
    ///
    /// # Panics
    ///
    /// When an open leader does not know its start, a held command has no
    /// slot below the next slot, or an unapplied slot is open.
    pub fn assert_invariants(&self) {
        assert!(
            self.segments
                .last()
                .is_some_and(|s| s.id == self.campaign.segment),
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
    }
}

#[cfg(test)]
mod tests;
