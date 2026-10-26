//! The cell coordinator's installs (#653 (election term to installed
//! leader)): a term won in the election journal leads the cell only once
//! its coordinator installs the term's uuid on the cell control journal
//! (`paros::coordinator::cell::serve_term`), a compare-and-set over the
//! uuid it read. Each install is valid on its own, so the journal model
//! passes a deposed term-T coordinator that reads term T+1's uuid and
//! installs over it, while two coordinators take turns. What links the two
//! journals lives here: every uuid a candidacy leads a term under, learned
//! from the election records the clients send (`Announce`), and, once the
//! run is over, the won installs of election uuids walked in slot order.
//! An operator's uuid (a `CellSession` of its own) names no term and is not
//! judged.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable};
use paros::client::election::ElectionRecord;
use paros::{JournalIdentifier, LeaderUuid};

const TERMS_KEY: &str = "paros-installed-terms";

/// Per election journal, the term each leader uuid leads.
type Terms = Mutex<BTreeMap<LeaderUuid, u64>>;

fn terms(state: &StateHandle, election: JournalIdentifier) -> std::sync::Arc<Terms> {
    crate::state::published_arc(
        state,
        &crate::state::journal_key(TERMS_KEY, election),
        || Mutex::new(BTreeMap::new()),
    )
}

/// A client sent `records` to `election`, an election journal: learn the
/// term each campaign leads under its uuid, and the term a hand-off names
/// its successor's uuid for.
pub(crate) fn note_election_write(
    state: &StateHandle,
    election: JournalIdentifier,
    records: &[Vec<u8>],
) {
    let terms = terms(state, election);
    let mut terms = terms.lock().unwrap_or_else(PoisonError::into_inner);
    for record in records {
        let led = match ElectionRecord::decode(record) {
            Some(Ok(ElectionRecord::Campaign { term, uuid, .. })) => (uuid, term),
            Some(Ok(ElectionRecord::Resign {
                term,
                successor: Some((_, uuid)),
                ..
            })) => (uuid, term + 1),
            _ => continue,
        };
        // A uuid is minted for one term (`paros::client::election`): the
        // first record that names it is its term.
        let term = *terms.entry(led.0).or_insert(led.1);
        assert_always!(
            term == led.1,
            "election: a leader uuid leads one term",
            { "term" => led.1, "first" => term }
        );
    }
}

/// Every cell control journal's won installs of election uuids, in slot
/// order, never name an earlier term after a later one: no deposed
/// coordinator installs over a later term's uuid.
pub(crate) fn check_installs(state: &StateHandle) {
    for (election, cell) in crate::machine::governed(state) {
        let terms = terms(state, election)
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if terms.is_empty() {
            continue;
        }
        let world = super::audit_world_named(state, cell);
        let st = world.lock();
        let mut installed: Option<u64> = None;
        for (slot, uuid, won) in st.journal.set_leaders() {
            let Some(&term) = terms.get(&uuid) else {
                continue;
            };
            match (won, installed) {
                // A term may install again over an operator's claim: its
                // first install's answer was lost, so it served nothing and
                // tries again (witness 15425774694310578517: term 15 at slot
                // 144, an operator at 145, term 15 again at 146).
                (true, Some(last)) => {
                    assert_always!(
                        term >= last,
                        "election: no term installs on the cell journal after a later term",
                        { "slot" => slot, "term" => term, "installed" => last }
                    );
                    if term == last {
                        assert_reachable!("election: a term installs again on the cell journal");
                    }
                }
                (false, Some(last)) if term < last => {
                    assert_reachable!("election: a later term's install refuses an earlier term's");
                }
                _ => {}
            }
            if won {
                installed = Some(installed.map_or(term, |last| last.max(term)));
            }
        }
    }
}
