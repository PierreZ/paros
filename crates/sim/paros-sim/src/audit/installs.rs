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
use paros::core::journal::{JournalIdentifier, LeaderUuid};

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
        judge_installs(&terms, st.journal.set_leaders());
    }
}

/// The won installs of election uuids on one cell control journal, in slot
/// order (`set_leaders`: slot, uuid, won), judged against the term each
/// uuid leads (`terms`): no install names an earlier term after a later one.
fn judge_installs(
    terms: &BTreeMap<LeaderUuid, u64>,
    set_leaders: impl Iterator<Item = (u64, LeaderUuid, bool)>,
) {
    let mut installed: Option<u64> = None;
    for (slot, uuid, won) in set_leaders {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::audit::testing::red;
    use moonpool_sim::StateHandle;
    use paros::client::election::{Candidate, ElectionRecord};
    use paros::core::journal::{JournalId, JournalIdentifier, LeaderUuid, TenantId};

    use super::{judge_installs, note_election_write, terms};

    const ELECTION: JournalIdentifier = JournalIdentifier {
        tenant: TenantId(3),
        journal: JournalId(4),
    };

    fn candidate(id: u64) -> Candidate {
        Candidate {
            id,
            ..Candidate::default()
        }
    }

    fn campaign(term: u64, uuid: u128) -> Vec<u8> {
        ElectionRecord::Campaign {
            term,
            candidate: candidate(1),
            uuid: LeaderUuid(uuid),
        }
        .encode()
    }

    #[test]
    fn a_campaign_and_a_hand_off_teach_the_term_each_uuid_leads() {
        let state = StateHandle::new();
        let resign = ElectionRecord::Resign {
            term: 5,
            candidate: 1,
            successor: Some((candidate(2), LeaderUuid(60))),
        }
        .encode();
        let renew = ElectionRecord::Renew {
            term: 5,
            candidate: candidate(1),
            uuid: LeaderUuid(70),
        }
        .encode();
        assert!(!red(|| note_election_write(
            &state,
            ELECTION,
            &[campaign(5, 50), resign, renew, b"not a record".to_vec()]
        )));
        let learned = terms(&state, ELECTION).lock().unwrap().clone();
        let expected: BTreeMap<_, _> = [(LeaderUuid(50), 5), (LeaderUuid(60), 6)].into();
        assert_eq!(
            learned, expected,
            "a renewal and a foreign record teach nothing"
        );
        assert!(!red(|| note_election_write(
            &state,
            ELECTION,
            &[campaign(5, 50)]
        )));
        assert!(red(|| note_election_write(
            &state,
            ELECTION,
            &[campaign(7, 50)]
        )));
    }

    #[test]
    fn no_term_installs_after_a_later_term() {
        let terms: BTreeMap<_, _> = [(LeaderUuid(1), 1), (LeaderUuid(2), 2)].into();
        let install = |slot, uuid, won| (slot, LeaderUuid(uuid), won);
        assert!(!red(|| judge_installs(
            &terms,
            [
                install(0, 1, true),
                install(1, 99, true),
                install(2, 1, true),
                install(3, 2, true),
                install(4, 1, false),
            ]
            .into_iter()
        )));
        assert!(red(|| judge_installs(
            &terms,
            [install(0, 2, true), install(1, 1, true)].into_iter()
        )));
        // A lost install does not raise the term in force.
        assert!(!red(|| judge_installs(
            &terms,
            [install(0, 2, false), install(1, 1, true)].into_iter()
        )));
    }
}
