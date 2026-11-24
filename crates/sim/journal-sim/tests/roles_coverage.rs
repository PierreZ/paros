//! The coverage gate of the journal roles (#1326 (the journal roles over the
//! core)): the roles walks in both shapes, branched from the runs that reach
//! a new site (`explore`), and the walk of the whole journal, whose
//! controller heals a retired segment's replica set with a new replica,
//! reach every `probe!` of `paros_journal::roles`, with no excused
//! probe. A probe that no walk reaches gets a walk move, a flavor or a
//! steer. It runs in release, in the `trace-edits` job of CI.

use journal_sim::campaign::try_kinds;
use journal_sim::roles::{RolesMutant, Shape, walk};
use journal_sim::whole::{self, WholeMutant};
use moonpool_assertions::AssertKind;
use paros_journal::roles::{REACHABLE, SOMETIMES};

/// The seeds of each roles walk, before the branches.
const SEEDS: u64 = 200;
/// The timelines that each roles walk branches from the runs that reach a
/// new site.
const BRANCHES: u64 = 10;

#[test]
fn the_walks_reach_every_probe_of_the_roles() {
    try_kinds(
        || {
            let mut runs = Vec::new();
            for shape in [Shape::NARROW, Shape::WIDE] {
                let report = walk(shape, RolesMutant::None, SEEDS, 3000)
                    .explore(BRANCHES)
                    .try_run()
                    .map_err(|error| error.to_string())?;
                runs.push(report.to_string());
            }
            let report = whole::walk(WholeMutant::None, 150, 6000)
                .try_run()
                .map_err(|error| error.to_string())?;
            runs.push(report.to_string());
            Ok(runs.join("\n"))
        },
        &[
            (AssertKind::Reachable, REACHABLE),
            (AssertKind::Sometimes, SOMETIMES),
        ],
    )
    .unwrap_or_else(|error| panic!("{error}"));
}
