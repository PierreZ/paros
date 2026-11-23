//! The **driver of the controller** (`docs/journal/06-controller.md` §J6.2,
//! "The driver as built"; #1328 (paros-journal-control)): one
//! [`ControlDriver`] runs the [`Controller`] of one journal on the signals
//! of its processes, the journal's register and the host's placement.
//!
//! The driver is pure: it reads no clock (the program gives the time of
//! each call), draws nothing and does no I/O. It reaches the world through
//! two ports that the program implements for the journal:
//!
//! - [`ControlRegister`]: a done read of the journal's matchmaker register,
//!   and one register operation for a change (`paros_journal::register`).
//! - [`Cell`]: the placement of the host (the process of a node, its zone,
//!   the candidates, the matchmakers) and the rate limiter of the cell (one
//!   token for the journal, §J6.3).
//!
//! Each [`ControlDriver::poll`] does one step:
//!
//! 1. Read the register. A segment the driver sees for the first time is
//!    taken as opened (or sealed) now: a driver that just started waits the
//!    minimum interval before its first reshape, and the stall window before
//!    it reports a stalled seal.
//! 2. A controller with no term campaigns when the register names no holder,
//!    names this controller (it restarted and forgot its term), or names a
//!    holder whose process is down. A holder that is up or suspect keeps its
//!    term: a campaign then only races it.
//! 3. A holder builds its [`View`] from the signals and the cell, steps the
//!    role, and runs what the role asks: a register change at once, with its
//!    outcome given back; a copy, an abandon or a repair as a send that the
//!    program delivers ([`Polled::sends`]).
//! 4. The token: a holder of the register's term takes it before an
//!    expensive action (a reshape call, a heal's copy or its
//!    compare-and-set) and gives it back once the
//!    journal has no call, no heal, and no pending or sealed segment (a
//!    reshape ends when its old segment is retired). Both are
//!    idempotent at the cell, so a new holder gives back the token of a
//!    holder that crashed.
//!
//! The scrub is not built (the CTRL repair group of #1326): the driver gives
//! no pending repair and no data loss to `decide`.

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::NodeId;
use paros_consensus::membership::{FailureDomain, MatchmakerId, MatchmakerSet};
use paros_journal::register::{Change, RegisterValue};
use paros_journal::roles::CopyId;
use paros_journal::segment::{SegmentId, SegmentRecord, SegmentState};

use crate::decide::{Decision, Tunables, View};
use crate::measure::Liveness;
use crate::role::{Ask, Controller, Outcome, Standing};
use crate::signals::{ProcessId, Report, Signals, Windows};

/// The journal's register, as the driver reaches it.
pub trait ControlRegister {
    /// The value of the last done write, if the register exists.
    fn read(&mut self) -> Option<RegisterValue>;

    /// Run `change` as one register operation: done, refused, or no answer.
    fn run(&mut self, change: &Change) -> Outcome;
}

/// The host's placement and the cell's rate limiter, for one journal.
pub trait Cell {
    /// The process that serves `node`, if the placement knows it.
    fn process_of(&self, node: NodeId) -> Option<ProcessId>;

    /// The failure domain of `process`, if it has one.
    fn zone(&self, process: ProcessId) -> Option<FailureDomain>;

    /// The nodes that may join the journal, in the placement's order, for
    /// the register value `value`.
    fn candidates(&mut self, value: &RegisterValue) -> Vec<NodeId>;

    /// The journal's matchmaker set, and its members that are live.
    fn matchmakers(&self) -> (MatchmakerSet, BTreeSet<MatchmakerId>);

    /// The journal holds the token, or the cell has one free.
    fn token_free(&self) -> bool;

    /// The journal takes the token. Taking a token it holds changes nothing.
    fn take_token(&mut self);

    /// The journal gives back its token. Giving back a token it does not
    /// hold changes nothing.
    fn give_back(&mut self);
}

/// The numbers of the host: each one is **Open** (§J6), so this type has
/// no default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlConfig {
    /// The numbers of `decide`.
    pub tunables: Tunables,
    /// The suspicion windows of the signals.
    pub windows: Windows,
}

/// What one step gives the program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Polled {
    /// The decision of the controller, for its alerts; `None` when it did
    /// not decide (no term, or a call or a heal in flight).
    pub decision: Option<Decision>,
    /// The asks that the journal's roles run: a copy, an abandon, a repair.
    pub sends: Vec<Ask>,
}

/// When the driver first saw a segment, and its seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    opened: u64,
    sealed: Option<u64>,
}

/// The driver of one journal's controller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlDriver {
    controller: Controller,
    process: ProcessId,
    config: ControlConfig,
    signals: Signals,
    seen: BTreeMap<SegmentId, Seen>,
    now: u64,
}

impl ControlDriver {
    /// A driver for the controller `me`, which runs on `process`, started at
    /// `now`: no term, no signal, no segment seen.
    ///
    /// # Panics
    ///
    /// When the windows are not valid.
    #[must_use]
    pub fn new(me: NodeId, process: ProcessId, config: ControlConfig, now: u64) -> Self {
        config.windows.assert_valid();
        Self {
            controller: Controller::new(me),
            process,
            config,
            signals: Signals::new(now),
            seen: BTreeMap::new(),
            now,
        }
    }

    /// The controller role.
    #[must_use]
    pub fn controller(&self) -> &Controller {
        &self.controller
    }

    /// The signals heard.
    #[must_use]
    pub fn signals(&self) -> &Signals {
        &self.signals
    }

    /// The report of this controller's process: it is alive.
    #[must_use]
    pub fn report(&self) -> Report {
        Report::Beat {
            process: self.process,
        }
    }

    /// Take `report` at `now`. A report of the driver's own process is
    /// dropped: the controller knows it is alive, and its own beat must not
    /// hide that it hears no other process (`signal_age`).
    ///
    /// # Panics
    ///
    /// When `now` is before the last time the driver was given.
    pub fn publish(&mut self, now: u64, report: Report) {
        self.tick(now);
        if report.process() == self.process {
            probe!(reachable, "driver: its own report is dropped");
            return;
        }
        self.signals.publish(now, report);
    }

    /// One step at `now` (the module doc).
    ///
    /// # Panics
    ///
    /// When `now` is before the last time the driver was given, or when
    /// the cell places this controller on a process that is not the
    /// driver's own: the driver drops the reports of its own process, so it
    /// would read the other process as silent.
    pub fn poll(
        &mut self,
        now: u64,
        register: &mut impl ControlRegister,
        cell: &mut impl Cell,
    ) -> Polled {
        self.tick(now);
        assert!(
            cell.process_of(self.controller.me())
                .is_none_or(|process| process == self.process),
            "the cell places this controller on the driver's own process"
        );
        let Some(value) = register.read() else {
            return Polled::default();
        };
        self.see(&value);
        if self.controller.standing() == &Standing::Idle {
            if !self.should_campaign(&value, cell) {
                return Polled::default();
            }
            let change = self.controller.campaign();
            let outcome = register.run(&change);
            self.controller.elected(&outcome);
        }
        let mut polled = Polled::default();
        let Some(term) = self.controller.term() else {
            return polled;
        };
        // Plan on a read after the election: a done `Elect` changed it.
        let value = register.read().expect("the register exists");
        self.see(&value);
        let (ask, decision) = self.step(term, &value, cell);
        polled.decision = decision;
        let owns = self.owns(&value);
        self.run(ask, owns, register, cell, &mut polled);
        self.settle_token(register, cell);
        polled
    }

    /// The new replica of `copy` reports that its copy of `segment` is
    /// complete: the controller writes the new set, at `now`.
    ///
    /// # Panics
    ///
    /// When `now` is before the last time the driver was given.
    pub fn copied(
        &mut self,
        now: u64,
        segment: SegmentId,
        copy: CopyId,
        register: &mut impl ControlRegister,
        cell: &mut impl Cell,
    ) -> Polled {
        self.tick(now);
        let ask = self.controller.copied(segment, copy);
        let owns = register.read().is_some_and(|value| self.owns(&value));
        let mut polled = Polled::default();
        self.run(ask, owns, register, cell, &mut polled);
        self.settle_token(register, cell);
        polled
    }

    /// Whether this controller holds the term that `value` names: only
    /// such a holder takes the token. A deposed holder that does not know
    /// it yet still sends its heal in flight (the register refuses it for
    /// the term), but it never takes the token from the journal that the
    /// new holder may have given it back to.
    fn owns(&self, value: &RegisterValue) -> bool {
        let term = value.term();
        term.holder == Some(self.controller.me()) && self.controller.term() == Some(term.number)
    }

    /// Move the driver's time to `now`.
    fn tick(&mut self, now: u64) {
        assert!(now >= self.now, "the program's clock never goes back");
        self.now = now;
    }

    /// Note the segments of `value` that the driver sees for the first time,
    /// and their seals; forget the segments that left the register.
    fn see(&mut self, value: &RegisterValue) {
        let now = self.now;
        self.seen.retain(|id, _| value.segment(*id).is_some());
        for record in value.segments().values() {
            let seen = self.seen.entry(record.id()).or_insert(Seen {
                opened: now,
                sealed: None,
            });
            if record.state() >= SegmentState::Sealed && seen.sealed.is_none() {
                seen.sealed = Some(now);
            }
        }
        assert!(
            self.seen.len() == value.segments().len(),
            "one sight for each segment of the register"
        );
    }

    /// Whether an idle controller campaigns on `value` (the module doc).
    fn should_campaign(&self, value: &RegisterValue, cell: &impl Cell) -> bool {
        let Some(holder) = value.term().holder else {
            probe!(reachable, "driver: no holder, so a campaign");
            return true;
        };
        if holder == self.controller.me() {
            probe!(reachable, "driver: a restarted holder campaigns again");
            return true;
        }
        let down = cell.process_of(holder).is_none_or(|process| {
            self.signals
                .liveness(process, self.now, self.config.windows)
                == Liveness::Down
        });
        if down {
            probe!(reachable, "driver: the holder is down, so a campaign");
        } else {
            probe!(reachable, "driver: the holder is live, so no campaign");
        }
        down
    }

    /// The liveness of `node`: the liveness of its process. A node with no
    /// process is left out of the view: it counts as lost, and no heal
    /// replaces it.
    fn liveness_of(&self, node: NodeId, cell: &impl Cell) -> Option<Liveness> {
        let process = cell.process_of(node)?;
        Some(
            self.signals
                .liveness(process, self.now, self.config.windows),
        )
    }

    /// The view of the holder of `term` on `value`, and its step.
    fn step(
        &mut self,
        term: u64,
        value: &RegisterValue,
        cell: &mut impl Cell,
    ) -> (Ask, Option<Decision>) {
        let now = self.now;
        let candidates = cell.candidates(value);
        let nodes: BTreeSet<NodeId> = value
            .segments()
            .values()
            .flat_map(|r| {
                r.grid()
                    .columns()
                    .iter()
                    .flatten()
                    .chain(r.replicas())
                    .copied()
                    .collect::<Vec<_>>()
            })
            .chain(candidates.iter().copied())
            .collect();
        let liveness: BTreeMap<NodeId, Liveness> = nodes
            .iter()
            .filter_map(|n| self.liveness_of(*n, cell).map(|l| (*n, l)))
            .collect();
        let zones: BTreeMap<NodeId, FailureDomain> = nodes
            .iter()
            .filter_map(|n| {
                cell.process_of(*n)
                    .and_then(|p| cell.zone(p))
                    .map(|z| (*n, z))
            })
            .collect();
        let (matchmakers, matchmakers_live) = cell.matchmakers();
        let repairs = BTreeSet::new();
        let newest = value.newest().map(SegmentRecord::id);
        let leader_live = value
            .in_force()
            .is_some_and(|r| self.signals.writes(r.id(), now, self.config.windows));
        let since_last_reshape = newest.map(|s| now - self.seen[&s].opened);
        let seal_age = value
            .segments()
            .values()
            .filter(|r| r.state() == SegmentState::Sealed)
            .filter_map(|r| self.seen[&r.id()].sealed)
            .min()
            .map(|at| now - at);
        let view = View {
            me: self.controller.me(),
            term,
            register: value,
            liveness: &liveness,
            zones: &zones,
            candidates: &candidates,
            leader_live,
            matchmakers: &matchmakers,
            matchmakers_live: &matchmakers_live,
            repairs_pending: &repairs,
            data_lost: false,
            signal_age: self.signals.signal_age(now),
            since_last_reshape,
            seal_age,
            token_free: cell.token_free(),
        };
        self.controller.step(&view, &self.config.tunables)
    }

    /// Run `ask` and what follows from it: a register change runs at once
    /// and its outcome goes back to the role; a send goes to the program.
    fn run(
        &mut self,
        mut ask: Ask,
        owns: bool,
        register: &mut impl ControlRegister,
        cell: &mut impl Cell,
        polled: &mut Polled,
    ) {
        loop {
            match ask {
                Ask::Nothing => return,
                Ask::Call(change) => {
                    if owns {
                        probe!(reachable, "driver: an expensive action takes the token");
                        cell.take_token();
                    } else {
                        probe!(reachable, "driver: a deposed holder takes no token");
                    }
                    let outcome = register.run(&change);
                    match &change {
                        Change::Reshape { opening, .. } => {
                            self.controller.reshaped(opening, &outcome);
                            return;
                        }
                        Change::Rereplicate { .. } => {
                            ask = self.controller.rereplicated(&change, &outcome);
                            if matches!(ask, Ask::Call(_)) {
                                // No answer: the role asks it again at its
                                // next step.
                                probe!(reachable, "driver: a heal's write waits for the next step");
                                return;
                            }
                        }
                        other => panic!("a holder asks a reshape or a heal, not {other:?}"),
                    }
                }
                Ask::Copy { .. } => {
                    if owns {
                        cell.take_token();
                    } else {
                        probe!(reachable, "driver: a deposed holder takes no token");
                    }
                    polled.sends.push(ask);
                    return;
                }
                Ask::Abandon { .. } | Ask::Act(_) => {
                    polled.sends.push(ask);
                    return;
                }
            }
        }
    }

    /// Give back the token once the journal has no call, no heal, and no
    /// segment that a reshape opened and the seal and the retire have not
    /// ended (pending or sealed) on a read after the step. Only the holder
    /// that this read names, at its own term, gives it back: a deposed
    /// controller that ends its own heal (a done compare-and-set read back,
    /// or an idempotent resend answered "ok") leaves the token to the new
    /// holder, whose expensive action may run under it.
    fn settle_token(&self, register: &mut impl ControlRegister, cell: &mut impl Cell) {
        if self.controller.term().is_none() {
            return;
        }
        let Some(value) = register.read() else {
            return;
        };
        if !self.owns(&value) {
            probe!(reachable, "driver: a deposed holder gives back no token");
            return;
        }
        let busy = self.controller.call().is_some()
            || self.controller.heal().is_some()
            || value
                .segments()
                .values()
                .any(|r| matches!(r.state(), SegmentState::Pending | SegmentState::Sealed));
        if !busy {
            probe!(reachable, "driver: an idle journal gives back the token");
            cell.give_back();
        }
    }
}

/// The message of every `probe!` of the driver.
pub const PROBES: &[&str] = &[
    "driver: no holder, so a campaign",
    "driver: a restarted holder campaigns again",
    "driver: the holder is down, so a campaign",
    "driver: the holder is live, so no campaign",
    "driver: an expensive action takes the token",
    "driver: a heal's write waits for the next step",
    "driver: an idle journal gives back the token",
    "driver: its own report is dropped",
    "driver: a deposed holder takes no token",
    "driver: a deposed holder gives back no token",
];

#[cfg(test)]
mod tests;
