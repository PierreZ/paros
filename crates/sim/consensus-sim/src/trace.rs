//! Model-based trace checking (#1198 (trace checking)): a walk records the
//! steps it took, and TLC checks with [`moonpool_tla`] that the trace is a
//! behaviour of the flavor's TLA+ model in `specs/tla/`.
//!
//! Each flavor writes a trace spec next to its model (`DecreeTrace.tla`
//! beside `Decree.tla`): the model, forced to follow the events one by one
//! with a cursor `l`. A walk records an event only for a step that changes
//! the model's state, with every argument the step needs, so TLC follows
//! the trace with no search. A trace that the model refuses is a bug in the
//! driver, in the code or in the model; the [`Rejection`] names its first
//! event that does not match.
//!
//! This module holds what every flavor shares: the check of a spec folder,
//! the rank of a ballot, and the names of nodes and values.

use std::collections::BTreeSet;
use std::path::PathBuf;

use moonpool_tla::Value;
pub use moonpool_tla::{Rejection, Trace, TraceCheck, Verdict};
use paros_consensus::Ballot;

/// The folder of the TLA+ models, from the root of the workspace.
#[must_use]
pub fn specs_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../specs/tla")
}

/// The check of the trace spec `module` in `specs/tla/<folder>`.
#[must_use]
pub fn spec(folder: &str, module: &str) -> TraceCheck {
    TraceCheck::new(specs_dir().join(folder), module)
}

/// Whether TLC can run here. TLC (`tlc`) is in the nix dev shell, and CI
/// runs every test in it, so a missing TLC fails in CI and skips only on a
/// machine outside the shell.
///
/// # Panics
///
/// When TLC is missing and the variable `CI` is set.
#[must_use]
pub fn tlc_or_skip(check: &TraceCheck) -> bool {
    if check.tlc_available() {
        return true;
    }
    assert!(
        std::env::var_os("CI").is_none(),
        "TLC is missing in CI: run the tests in the nix dev shell"
    );
    eprintln!("skipped: TLC (`tlc`) is not installed; run in `nix develop`");
    false
}

/// The ballots of one run, ranked from 1 in the order of the code. A model
/// numbers its ballots `1..NB`; the code's ballot is `(round, node)`. Only
/// the order matters to Paxos, so the rank keeps all that a model reads.
#[derive(Debug, Default)]
pub struct Ranks {
    ballots: BTreeSet<Ballot>,
}

impl Ranks {
    /// The ranks of `ballots`.
    pub fn new(ballots: impl IntoIterator<Item = Ballot>) -> Self {
        Self {
            ballots: ballots.into_iter().collect(),
        }
    }

    /// How many distinct ballots the run used: the model's `NB`.
    #[must_use]
    pub fn count(&self) -> Value {
        int(self.ballots.len())
    }

    /// The rank of `ballot`, from 1. The zero ballot (no promise, no vote)
    /// is 0, the "none" of every model.
    ///
    /// # Panics
    ///
    /// When `ballot` is not a ballot of the run and not zero.
    #[must_use]
    pub fn of(&self, ballot: Ballot) -> Value {
        if ballot == Ballot::zero() {
            return Value::Int(0);
        }
        let below = self.ballots.range(..ballot).count();
        assert!(
            self.ballots.contains(&ballot),
            "{ballot:?} is not a ballot of the run"
        );
        int(below + 1)
    }
}

/// A count or an index as a TLA+ integer.
///
/// # Panics
///
/// When `n` does not fit an `i64` (never, for a walk).
#[must_use]
pub fn int(n: impl TryInto<i64>) -> Value {
    Value::Int(n.try_into().ok().expect("a walk's numbers fit an i64"))
}

/// The name of node `id` in a trace: `"n<id>"`.
#[must_use]
pub fn node(id: paros_consensus::NodeId) -> Value {
    Value::Str(format!("n{}", id.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_consensus::NodeId;

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    #[test]
    fn ranks_follow_the_order_of_the_code() {
        let ranks = Ranks::new([ballot(2, 1), ballot(1, 2), ballot(2, 0)]);
        assert_eq!(ranks.count(), Value::Int(3));
        assert_eq!(ranks.of(ballot(1, 2)), Value::Int(1));
        assert_eq!(ranks.of(ballot(2, 0)), Value::Int(2));
        assert_eq!(ranks.of(ballot(2, 1)), Value::Int(3));
        assert_eq!(ranks.of(Ballot::zero()), Value::Int(0));
    }

    #[test]
    #[should_panic(expected = "not a ballot of the run")]
    fn an_unknown_ballot_has_no_rank() {
        let _ = Ranks::new([ballot(1, 1)]).of(ballot(3, 1));
    }

    #[test]
    fn the_specs_folder_holds_the_models() {
        assert!(specs_dir().join("decree/Decree.tla").is_file());
        assert!(specs_dir().join("decree/DecreeTrace.cfg").is_file());
    }

    #[test]
    fn nodes_are_named_strings() {
        assert_eq!(node(NodeId(4)), Value::from("n4"));
    }
}
