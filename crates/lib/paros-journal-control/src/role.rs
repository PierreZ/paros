//! The **controller role** (`docs/journal/06-controller.md` §J6.1, #1328
//! (paros-journal-control)): the election of the controller by a term in the
//! journal's matchmaker register, and its fenced `reshape` calls.
//!
//! The role is pure and stateless across a crash: a restarted controller is
//! a new [`Controller`], with no term and no plan (`specs/tla/grid-controller/`:
//! a crash forgets the term and the plan). It never runs a register
//! operation itself. It gives the driver a [`Change`] to run as one
//! register operation (`paros_journal::register::RegisterCall`: a read and a
//! compare-and-set write at one generation), and the driver gives back the
//! [`Outcome`]. The register's guards decide whether a change is safe; the
//! role only decides when to ask.
//!
//! - **The election**: [`Controller::campaign`] gives `Change::Elect`. A
//!   done write that names this controller makes it the holder of the term
//!   it wrote; anything else leaves it idle.
//! - **The plan**: [`Controller::step`] runs [`decide`] on the driver's view
//!   while the controller holds its term. A reshape plan becomes the call in
//!   flight; [`Controller::step`] gives that same call again until it is
//!   answered, and plans nothing else meanwhile. The register answers a call
//!   it already took with "ok" (§J6.1, the idempotent rule), so a retry
//!   after a lost reply is safe.
//! - **The stop**: a refusal for the term, or a view whose term is not the
//!   controller's, makes it idle: it was deposed.
//! - **The heal of a retired segment** (§J3.1, #1314, checked by
//!   `specs/tla/re-replication/`). A re-replication plan of [`decide`]
//!   becomes the heal in flight: one member that is down out, one new
//!   replica in, as in the model ([`decide`] plans one replacement, so
//!   each set the heal writes meets the zone rule on its own; the next
//!   decision heals the next member, #1496). The controller asks the driver to start
//!   the new replica for one copy ([`Ask::Copy`], a [`CopyId`] of its term),
//!   and writes the new set only on that copy's report that it is complete
//!   ([`Controller::copied`], control `count_start`), by a compare-and-set
//!   on the record that it read and on its term ([`Change::Rereplicate`],
//!   controls `cas_term`, `cas_blind`). A heal whose new replica is down,
//!   whose read is old, or whose term is gone ends with no write
//!   ([`Ask::Abandon`]): the new replica then drops its copy, and a new plan
//!   starts a new copy (control `report_sticky`).

use paros_consensus::NodeId;
use paros_journal::register::{Change, ChangeRefusal, RegisterValue};
use paros_journal::roles::CopyId;
use paros_journal::segment::{OpeningCall, SegmentId, SegmentRecord};

use crate::decide::{Action, Decision, Tunables, View, decide};
use crate::measure::Liveness;

/// Where the controller stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Standing {
    /// No term: a fresh or restarted controller, or one that was deposed.
    Idle,
    /// Its `Elect` is in flight.
    Campaigning,
    /// It holds `term`.
    Holding {
        /// The term it won.
        term: u64,
    },
}

/// The outcome of one register operation, as the driver saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A majority took the write: `value` holds.
    Done(RegisterValue),
    /// The guard refused the change on the value the operation read.
    Refused(ChangeRefusal),
    /// The operation ended with no answer: a later operation beat it, the
    /// set moved, or the reply was lost.
    NoAnswer,
}

/// What the controller asks the driver to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ask {
    /// Nothing.
    Nothing,
    /// Run this change as one register operation and give back its outcome.
    Call(Change),
    /// Act outside the register: a repair (§J6, the v1 duties), run by the
    /// journal's roles.
    Act(Action),
    /// Start a new replica `node` of the retired `segment` for the copy
    /// `copy` (`paros_journal::roles::SegmentReplica::join`), with `set`, the
    /// replica set the controller read, as the members to copy from. Asked
    /// again at each step until the copy reports: a node that already runs
    /// this copy keeps it. A node runs one copy of a segment, the highest
    /// [`CopyId`] it was asked: a higher copy replaces the one it runs, and
    /// a lower one is refused. A deposed controller keeps its heal until the
    /// register refuses it for the term, so a lower copy that replaced a
    /// complete higher one could leave the new holder's compare-and-set
    /// counting a node with nothing on it (`journal-sim`'s heal walk, the
    /// mutant `AnyCopy`; `specs/tla/heal-reuse/`, control `copy_any`). A
    /// node that serves the segment as a member refuses every copy: the ask
    /// may come from an old view. It judges that on a register value that it
    /// saw done after the ask arrived (control `stale_view`).
    Copy {
        /// The segment.
        segment: SegmentId,
        /// The new replica.
        node: NodeId,
        /// The set the controller read.
        set: Vec<NodeId>,
        /// The copy.
        copy: CopyId,
    },
    /// The heal of `copy` ended with no new set, as far as the controller
    /// knows: tell the new replica `node` (`SegmentReplica::abandon`). The
    /// heal may have committed (an "ok" can be lost), so the node acts only
    /// on a register value that it saw done after this ask arrived, and
    /// keeps its copy when that value names it (`specs/tla/heal-reuse/`,
    /// control `stale_view`).
    Abandon {
        /// The segment.
        segment: SegmentId,
        /// The new replica.
        node: NodeId,
        /// The copy.
        copy: CopyId,
    },
}

/// The heal of a retired segment in flight (#1314).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heal {
    /// The segment.
    pub segment: SegmentId,
    /// The record the controller read when it planned the heal.
    pub read: SegmentRecord,
    /// The member that is down.
    pub remove: NodeId,
    /// The new replica.
    pub add: NodeId,
    /// The copy of the new replica.
    pub copy: CopyId,
    /// Whether the copy reported that it is complete: the compare-and-set
    /// of the new set is then in flight.
    pub complete: bool,
}

impl Heal {
    /// The compare-and-set that writes the new set.
    #[must_use]
    pub fn change(&self) -> Change {
        Change::Rereplicate {
            segment: self.segment,
            read: self.read.clone(),
            term: self.copy.term,
            remove: self.remove,
            add: self.add,
        }
    }
}

/// The grid controller of one journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Controller {
    me: NodeId,
    standing: Standing,
    call: Option<OpeningCall>,
    heal: Option<Heal>,
    /// The number of the next copy in the term.
    copies: u64,
}

impl Controller {
    /// A controller with no term: at its start or after a crash.
    #[must_use]
    pub fn new(me: NodeId) -> Self {
        Self {
            me,
            standing: Standing::Idle,
            call: None,
            heal: None,
            copies: 0,
        }
    }

    /// This controller.
    #[must_use]
    pub fn me(&self) -> NodeId {
        self.me
    }

    /// Where it stands.
    #[must_use]
    pub fn standing(&self) -> &Standing {
        &self.standing
    }

    /// The reshape call in flight.
    #[must_use]
    pub fn call(&self) -> Option<&OpeningCall> {
        self.call.as_ref()
    }

    /// The heal in flight.
    #[must_use]
    pub fn heal(&self) -> Option<&Heal> {
        self.heal.as_ref()
    }

    /// The term it holds.
    #[must_use]
    pub fn term(&self) -> Option<u64> {
        match self.standing {
            Standing::Holding { term } => Some(term),
            Standing::Idle | Standing::Campaigning => None,
        }
    }

    /// Start an election: the change to run, `Elect` for this controller.
    ///
    /// # Panics
    ///
    /// When the controller is not idle: the driver campaigns only for a
    /// controller with no term.
    pub fn campaign(&mut self) -> Change {
        assert!(
            self.standing == Standing::Idle,
            "only an idle controller campaigns"
        );
        assert!(self.call.is_none(), "an idle controller has no call");
        assert!(self.heal.is_none(), "an idle controller has no heal");
        probe!(reachable, "role: a controller campaigns");
        self.standing = Standing::Campaigning;
        Change::Elect { holder: self.me }
    }

    /// The outcome of its `Elect`.
    ///
    /// # Panics
    ///
    /// When the controller is not campaigning.
    pub fn elected(&mut self, outcome: &Outcome) {
        assert!(
            self.standing == Standing::Campaigning,
            "an election outcome answers a campaign"
        );
        self.standing = match outcome {
            Outcome::Done(value) if value.term().holder == Some(self.me) => {
                probe!(reachable, "role: a controller wins a term");
                self.copies = 0;
                Standing::Holding {
                    term: value.term().number,
                }
            }
            Outcome::Done(_) | Outcome::Refused(_) | Outcome::NoAnswer => {
                probe!(reachable, "role: a campaign ends with no term");
                Standing::Idle
            }
        };
    }

    /// The next step of a controller that holds its term, on the driver's
    /// view: the call in flight again, the heal in flight, or the decision
    /// of [`decide`]. Gives the decision too, for its alerts.
    ///
    /// # Panics
    ///
    /// When the view is of another controller.
    pub fn step(&mut self, view: &View<'_>, tunables: &Tunables) -> (Ask, Option<Decision>) {
        assert!(view.me == self.me, "a view of this controller");
        let Some(term) = self.term() else {
            return (Ask::Nothing, None);
        };
        assert!(
            view.term == term,
            "a view carries the term the controller holds"
        );
        if let Some(heal) = self.heal.clone() {
            return (self.heal_step(view, &heal), None);
        }
        if let Some(call) = &self.call {
            probe!(reachable, "role: a call in flight is sent again");
            return (
                Ask::Call(Change::Reshape {
                    opening: call.clone(),
                    release: paros_journal::RELEASE,
                }),
                None,
            );
        }
        let decision = decide(view, tunables);
        let ask = match &decision.action {
            Action::Stop => {
                probe!(
                    reachable,
                    "role: a view of another term stops the controller"
                );
                self.standing = Standing::Idle;
                Ask::Nothing
            }
            Action::Rereplicate { segment, from, to } => {
                self.plan_heal(view, term, *segment, from, to)
            }
            Action::Reshape(call) => {
                self.call = Some(call.clone());
                Ask::Call(Change::Reshape {
                    opening: call.clone(),
                    release: paros_journal::RELEASE,
                })
            }
            Action::Repair { .. } => Ask::Act(decision.action.clone()),
            Action::Idle | Action::Wait(_) => Ask::Nothing,
        };
        (ask, Some(decision))
    }

    /// The outcome of its reshape call `call`. An outcome of an older call
    /// (from before a stop) changes nothing.
    ///
    /// # Panics
    ///
    /// When a done outcome does not hold the record the call opened: the
    /// driver gave the value of another operation.
    pub fn reshaped(&mut self, call: &OpeningCall, outcome: &Outcome) {
        if self.call.as_ref() != Some(call) {
            probe!(reachable, "role: an outcome of an older call is dropped");
            return;
        }
        match outcome {
            Outcome::Done(value) => {
                assert!(
                    value.segments().values().any(|r| r.opening() == call),
                    "a done reshape holds the record its call opened"
                );
                probe!(reachable, "role: a reshape is done");
                self.call = None;
            }
            Outcome::Refused(ChangeRefusal::Term { .. }) => {
                probe!(
                    reachable,
                    "role: a refusal for the term stops the controller"
                );
                self.call = None;
                self.standing = Standing::Idle;
            }
            Outcome::Refused(_) => {
                probe!(reachable, "role: a reshape is refused");
                self.call = None;
            }
            Outcome::NoAnswer => {
                probe!(reachable, "role: a reshape has no answer and is sent again");
            }
        }
    }
}

impl Controller {
    /// A heal from a re-replication plan of `decide`: the first member that
    /// the plan replaces, with its new replica, and a new copy.
    fn plan_heal(
        &mut self,
        view: &View<'_>,
        term: u64,
        segment: SegmentId,
        from: &[NodeId],
        to: &[NodeId],
    ) -> Ask {
        assert!(
            from.iter().zip(to).filter(|(old, new)| old != new).count() == 1,
            "a re-replication plan replaces exactly one member"
        );
        let (remove, add) = from
            .iter()
            .zip(to)
            .find(|(old, new)| old != new)
            .map(|(old, new)| (*old, *new))
            .expect("a re-replication changes the set");
        let read = view
            .register
            .segment(segment)
            .expect("decide plans on a record of the view")
            .clone();
        assert!(read.replicas() == from, "decide plans on the set it read");
        let copy = CopyId {
            term,
            number: self.copies,
        };
        self.copies += 1;
        probe!(reachable, "role: a heal starts a new copy");
        let heal = Heal {
            segment,
            read,
            remove,
            add,
            copy,
            complete: false,
        };
        let ask = Ask::Copy {
            segment,
            node: add,
            set: heal.read.replicas().to_vec(),
            copy,
        };
        self.heal = Some(heal);
        ask
    }

    /// The heal in flight, on the view: it ends when its new replica is
    /// down or its read is old, and, before its compare-and-set is sent,
    /// when the view's term is not the heal's (the controller is deposed
    /// and stops); else the copy, or its compare-and-set, is asked again.
    fn heal_step(&mut self, view: &View<'_>, heal: &Heal) -> Ask {
        let record = view.register.segment(heal.segment);
        let mut healed = heal.read.clone();
        let done = healed.replace_replica(heal.remove, heal.add).is_ok() && record == Some(&healed);
        if done {
            probe!(reachable, "role: a view shows the heal done");
            self.heal = None;
            return Ask::Nothing;
        }
        if !heal.complete && view.register.term().number != heal.copy.term {
            // A deposed controller: its copy can never be written into the
            // set, and the new replica refuses its asks. A sent heal waits
            // for its answer, which is a refusal for the term.
            probe!(reachable, "role: a heal of a term that is over ends");
            self.standing = Standing::Idle;
            return self.abandon();
        }
        if record != Some(&heal.read) {
            probe!(reachable, "role: a heal on an old read ends");
            return self.abandon();
        }
        let down = view.liveness.get(&heal.add) == Some(&Liveness::Down);
        if heal.complete {
            // Once the compare-and-set is sent, a copy of it can still be in
            // the network: the heal ends only on an answer, so the new
            // replica keeps its copy until the register took or refused the
            // change (a down new replica that holds the copy is a member
            // that a later heal replaces).
            if down {
                probe!(reachable, "role: a sent heal outlives a down new replica");
            }
            probe!(reachable, "role: a heal's compare-and-set is sent again");
            return Ask::Call(heal.change());
        }
        if down {
            probe!(reachable, "role: a heal whose new replica is down ends");
            self.abandon()
        } else {
            Ask::Copy {
                segment: heal.segment,
                node: heal.add,
                set: heal.read.replicas().to_vec(),
                copy: heal.copy,
            }
        }
    }

    /// End the heal in flight with no write.
    fn abandon(&mut self) -> Ask {
        let heal = self.heal.take().expect("a heal to end");
        Ask::Abandon {
            segment: heal.segment,
            node: heal.add,
            copy: heal.copy,
        }
    }

    /// The new replica of `copy` reports that its copy of `segment` is
    /// complete and durable: the controller writes the new set. A report of
    /// another copy changes nothing (control `report_sticky`).
    pub fn copied(&mut self, segment: SegmentId, copy: CopyId) -> Ask {
        let Some(heal) = &mut self.heal else {
            return Ask::Nothing;
        };
        if heal.segment != segment || heal.copy != copy {
            probe!(reachable, "role: a report of another copy is dropped");
            return Ask::Nothing;
        }
        probe!(reachable, "role: a complete copy is counted");
        heal.complete = true;
        Ask::Call(heal.change())
    }

    /// The outcome of the heal's compare-and-set `change`. An outcome of
    /// another change changes nothing.
    pub fn rereplicated(&mut self, change: &Change, outcome: &Outcome) -> Ask {
        let Some(heal) = &self.heal else {
            return Ask::Nothing;
        };
        if heal.change() != *change || !heal.complete {
            return Ask::Nothing;
        }
        match outcome {
            Outcome::Done(_) => {
                probe!(reachable, "role: a heal is done");
                self.heal = None;
                Ask::Nothing
            }
            Outcome::Refused(ChangeRefusal::Term { .. }) => {
                probe!(
                    reachable,
                    "role: a heal refused for the term stops the controller"
                );
                self.standing = Standing::Idle;
                self.abandon()
            }
            Outcome::Refused(_) => {
                probe!(reachable, "role: a heal is refused");
                self.abandon()
            }
            Outcome::NoAnswer => {
                probe!(reachable, "role: a heal has no answer and is sent again");
                Ask::Call(heal.change())
            }
        }
    }
}

/// The message of every `probe!` of the role.
pub const PROBES: &[&str] = &[
    "role: a controller campaigns",
    "role: a controller wins a term",
    "role: a campaign ends with no term",
    "role: a call in flight is sent again",
    "role: a view of another term stops the controller",
    "role: an outcome of an older call is dropped",
    "role: a reshape is done",
    "role: a refusal for the term stops the controller",
    "role: a reshape is refused",
    "role: a reshape has no answer and is sent again",
    "role: a heal starts a new copy",
    "role: a view shows the heal done",
    "role: a heal of a term that is over ends",
    "role: a heal on an old read ends",
    "role: a heal whose new replica is down ends",
    "role: a sent heal outlives a down new replica",
    "role: a heal's compare-and-set is sent again",
    "role: a report of another copy is dropped",
    "role: a complete copy is counted",
    "role: a heal is done",
    "role: a heal refused for the term stops the controller",
    "role: a heal is refused",
    "role: a heal has no answer and is sent again",
];

#[cfg(test)]
mod tests;
