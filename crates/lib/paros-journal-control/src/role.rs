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

use paros_consensus::NodeId;
use paros_journal::register::{Change, ChangeRefusal, RegisterValue};
use paros_journal::segment::OpeningCall;

use crate::decide::{Action, Decision, Tunables, View, decide};

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
    /// Act outside the register: a repair or a re-replication (§J6, the
    /// v1 duties), run by the journal's roles.
    Act(Action),
}

/// The grid controller of one journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Controller {
    me: NodeId,
    standing: Standing,
    call: Option<OpeningCall>,
}

impl Controller {
    /// A controller with no term: at its start or after a crash.
    #[must_use]
    pub fn new(me: NodeId) -> Self {
        Self {
            me,
            standing: Standing::Idle,
            call: None,
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
    /// view: the call in flight again, or the decision of [`decide`].
    /// Gives the decision too, for its alerts.
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
            Action::Reshape(call) => {
                self.call = Some(call.clone());
                Ask::Call(Change::Reshape {
                    opening: call.clone(),
                    release: paros_journal::RELEASE,
                })
            }
            Action::Repair { .. } | Action::Rereplicate { .. } => Ask::Act(decision.action.clone()),
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
];

#[cfg(test)]
mod tests;
