//! `VIEW` (#399 (admin CLI views)): one administrative view of the cell this
//! operator knows, through the library's `paros::client::views` — the code
//! `parosctl machine|cell|tenant|roles` prints. Each view is a request to
//! one cell, answered by a founding member from the cell's own journals and
//! filtered there by the caller's scope.
//!
//! The scope is drawn per call: the admin, or a tenant by one of the run's
//! tenant names (which may not exist, or may belong to another operator's
//! creation in flight). The query is drawn too: the cell, a tenant by name
//! (the scope's own, or on its own BUGGIFY location another one), or the
//! universe. The oracles judge every answer:
//!
//! - **scope**: an answer to a tenant scope names only that tenant, and of
//!   each machine only its id, name, failure domain and up/down state; a
//!   tenant scope is refused the cell, the universe and every other tenant;
//! - **one cell**: an answer from a member of the cell names that cell;
//! - **spread**: a tenant view names only the machines its journals use
//!   (and the coordinator);
//! - **founders**: an admin cell view marks exactly the founding members;
//! - **forward**: a member's registry position never goes back, in the
//!   order one operator saw its answers (two operators' replies may cross
//!   on the wire: an answer folded earlier can arrive later);
//! - **status** (#191 (status)): every journal shows a word, and the word
//!   an admin answer judged is the one its own triple gives (the desired
//!   acceptor count, the placed configuration and the machines it names),
//!   re-derived here through the configuration's quorum predicates; the
//!   slots per class `parosctl status` prints are the bookings the answer
//!   lists. Gates: a journal shows degraded, and one an operator saw
//!   degraded shows healthy again from the same member.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use moonpool_sim::{
    SimContext, assert_always, assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::client::views::{ViewOutcome, ask, request};
use paros::status::{Word, placed, summarize};
use paros::view::{Scope, within_tenant_scope};
use paros::wire::view::{CellQuery, TenantQuery, UniverseQuery, ViewReply, view_request::Query};

use super::{FleetOps, NAMES};

const POSITIONS_KEY: &str = "paros-view-positions";
const WORDS_KEY: &str = "paros-view-words";

impl FleetOps {
    /// `VIEW`: ask the cell this operator knows for one view.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn view(&mut self, ctx: &SimContext, draw: u64) {
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("view: a view finds no cell formed yet");
            return;
        };
        let named = |n: u64| NAMES[usize::try_from(n % NAMES.len() as u64).unwrap_or(0)];
        let scope = if draw.is_multiple_of(2) {
            Scope::Admin
        } else {
            Scope::Tenant(named(draw >> 1).to_vec())
        };
        // A tenant scope asks its own tenant half the time: the rest are
        // refused.
        let query = match (draw >> 3) % if matches!(scope, Scope::Admin) { 3 } else { 4 } {
            0 => Query::Cell(CellQuery {}),
            1 => Query::Universe(UniverseQuery {}),
            _ => {
                let name = match &scope {
                    Scope::Tenant(own) if !buggify_with_prob!(0.2) => own.clone(),
                    _ => named(draw >> 5).to_vec(),
                };
                Query::Tenant(TenantQuery { name })
            }
        };
        let servers: Vec<_> = cell.servers.iter().map(|(_, addr)| addr.clone()).collect();
        let outcome = ask(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &servers,
            cell.first(draw),
            &request(&scope, query.clone()),
            self.patience,
        )
        .await;
        match outcome {
            ViewOutcome::Answered(reply) => {
                judge_answer(
                    ctx,
                    (&scope, &query, &reply),
                    &cell.journals,
                    &cell.members,
                    self.client_id,
                );
            }
            ViewOutcome::Refused(reply) => judge_refusal(&scope, &query, &reply),
            ViewOutcome::Unreachable => {
                assert_reachable!("view: no member of the cell answered a view");
            }
        }
    }
}

/// Judge an answered view against the scope that asked it.
fn judge_answer(
    ctx: &SimContext,
    (scope, query, reply): (&Scope, &Query, &ViewReply),
    journals: &paros::machine::ControlJournals,
    members: &[u64],
    client: u64,
) {
    let member = members.contains(&reply.answered_by);
    if member {
        assert_always!(
            reply.cell_id == journals.cell_id,
            "view: an answer from a member names its cell",
            { "cell" => journals.cell_id, "named" => reply.cell_id }
        );
        forward(ctx, reply, client);
    } else {
        // A founding member's address that now serves another cell (#216).
        assert_reachable!("view: a view answered by another cell's machine");
    }
    judge_status(ctx, reply, matches!(scope, Scope::Admin), client);
    if let Scope::Tenant(own) = scope {
        assert_always!(
            within_tenant_scope(reply, own),
            "view: a tenant scope sees only its own spread",
            { "answered_by" => reply.answered_by }
        );
        assert_always!(
            matches!(query, Query::Tenant(t) if t.name == *own),
            "view: a tenant scope is answered only its own tenant"
        );
        assert_sometimes!(true, "view: a tenant scope is answered its spread");
    }
    match query {
        Query::Tenant(_) => {
            let used: Vec<u64> = reply
                .tenants
                .iter()
                .flat_map(|t| &t.journals)
                .flat_map(|j| j.acceptors.iter().chain(&j.matchmakers).copied())
                .chain(std::iter::once(reply.coordinator))
                .collect();
            assert_always!(
                reply.machines.iter().all(|m| used.contains(&m.node_id)),
                "view: a tenant view names only the machines it uses"
            );
        }
        Query::Cell(_) if member => {
            let mut founders: Vec<u64> = reply
                .machines
                .iter()
                .filter(|m| m.founder)
                .map(|m| m.node_id)
                .collect();
            founders.sort_unstable();
            let mut expected = members.to_vec();
            expected.sort_unstable();
            assert_always!(
                founders == expected,
                "view: a cell view marks exactly the founding members",
                { "marked" => founders.len(), "founders" => expected.len() }
            );
            assert_sometimes!(true, "view: an admin cell view is answered");
        }
        Query::Universe(_) => {
            assert_sometimes!(
                !reply.cells.is_empty(),
                "view: a universe view names its cells"
            );
        }
        Query::Cell(_) => {}
    }
}

/// Judge a refused view: a tenant scope asking beyond its own tenant is
/// always refused `forbidden`; nothing else is.
fn judge_refusal(scope: &Scope, query: &Query, reply: &ViewReply) {
    let beyond = match (scope, query) {
        (Scope::Admin, _) => false,
        (Scope::Tenant(own), Query::Tenant(t)) => t.name != *own,
        (Scope::Tenant(_), _) => true,
    };
    if reply.refusal == "forbidden" {
        assert_reachable!("view: a tenant scope is refused beyond its own tenant");
    }
    assert_always!(
        beyond == (reply.refusal == "forbidden"),
        "view: forbidden is the answer exactly beyond the scope",
        { "refusal" => reply.refusal.clone() }
    );
    assert_always!(
        reply.refusal != "malformed",
        "view: a well-formed view is never malformed"
    );
}

/// A member's registry position never goes back: its fold only moves on.
/// Judged per operator, whose views are sequential, so its answers arrive
/// in the order the member gave them (witness seed 13981928377280924207:
/// one operator's answer at 3 arrived after another's at 4).
fn forward(ctx: &SimContext, reply: &ViewReply, client: u64) {
    if reply.registry_at == 0 {
        return;
    }
    let positions = crate::state::published_arc(ctx.state(), POSITIONS_KEY, || {
        Mutex::new(BTreeMap::<(u64, u64), u64>::new())
    });
    let mut positions = positions.lock().unwrap_or_else(PoisonError::into_inner);
    let seen = positions.entry((client, reply.answered_by)).or_insert(0);
    assert_always!(
        reply.registry_at >= *seen,
        "view: a member's registry position never goes back",
        { "member" => reply.answered_by, "seen" => *seen, "now" => reply.registry_at }
    );
    *seen = reply.registry_at;
}

/// The status words of an answer (#191 (status)). Every journal shows a
/// word. An admin answer names every detail of the machines its journals
/// use, so its judged word is re-derived from its own triple; a tenant
/// scope's machines are narrowed, so only its words are checked.
fn judge_status(ctx: &SimContext, reply: &ViewReply, admin: bool, client: u64) {
    let words = crate::state::published_arc(ctx.state(), WORDS_KEY, || {
        Mutex::new(BTreeMap::<(u64, u64, u64, u64), Word>::new())
    });
    let mut words = words.lock().unwrap_or_else(PoisonError::into_inner);
    for tenant in &reply.tenants {
        for journal in &tenant.journals {
            let shown = Word::parse(&journal.word);
            assert_always!(
                shown.is_some() && Word::parse(&journal.observed).is_some(),
                "status: every journal shows a word",
                { "word" => journal.word.clone(), "kind" => journal.kind.clone() }
            );
            let Some(shown) = shown else { continue };
            assert_sometimes!(shown == Word::Degraded, "status: a journal shows degraded");
            let key = (client, reply.answered_by, tenant.tenant, journal.journal);
            if let Some(before) = words.insert(key, shown) {
                assert_sometimes!(
                    before == Word::Degraded && shown == Word::Healthy,
                    "status: a degraded journal shows healthy again"
                );
            }
            if admin {
                assert_always!(
                    Word::parse(&journal.observed) == expected_word(reply, journal),
                    "status: a word matches the triple it was judged from",
                    { "observed" => journal.observed.clone(), "kind" => journal.kind.clone() }
                );
            }
        }
    }
    if admin {
        // `parosctl status`'s slots per class are the bookings listed.
        let mut listed: BTreeMap<String, u64> = BTreeMap::new();
        for m in reply.machines.iter().filter(|m| m.standing != "retired") {
            *listed.entry(m.class.clone()).or_default() += m.bookings.len() as u64;
        }
        let summary = summarize(reply);
        assert_always!(
            summary.slots.iter().all(|(class, (booked, _))| listed
                .get(class)
                .copied()
                .unwrap_or(0)
                == *booked),
            "status: the slots per class are the bookings listed"
        );
    }
}

/// The word `journal`'s own triple gives, derived independently of
/// `paros::status::judge`: the members up and not retired are live, and of
/// those the ones not draining stay.
fn expected_word(reply: &ViewReply, journal: &paros::wire::view::JournalView) -> Option<Word> {
    let placed = placed(journal)?;
    let machine = |id: u64| reply.machines.iter().find(|m| m.node_id == id);
    let live: std::collections::BTreeSet<paros::NodeId> = placed
        .members()
        .iter()
        .copied()
        .filter(|id| machine(id.0).is_some_and(|m| m.up && m.standing != "retired"))
        .collect();
    if !placed.has_phase1_quorum(&live) || !placed.has_phase2_quorum(&live) {
        return Some(Word::Unavailable);
    }
    let staying = live
        .iter()
        .filter(|id| machine(id.0).is_some_and(|m| m.standing != "draining"))
        .count();
    let desired = usize::try_from(journal.desired_acceptors).unwrap_or(usize::MAX);
    Some(if staying == placed.members().len() && desired == staying {
        Word::Healthy
    } else {
        Word::Degraded
    })
}
