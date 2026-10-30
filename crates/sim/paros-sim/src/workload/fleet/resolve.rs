//! `Resolve` at the entry endpoint (#216): after the library resolves a
//! tenant's name through the universe directory (#239), the operator asks the
//! machines themselves, as a client handed only addresses does
//! (`paros::client::resolve`). The entry endpoint is every machine, from a
//! drawn one on: idle machines do not serve `Resolve` and are passed over,
//! founding and admitted machines answer from their own folds.
//!
//! The same name then resolves through the machines' resolver role (#233
//! (resolver)), with a drawn token: a resolver names the tenant's cell and
//! that cell's entry endpoint, and a machine there names the rest. Its
//! oracles: a token without the right is never redirected, a token with the
//! right is never denied, a never-created name is never redirected, and a
//! tenant this operator created is sent to the cell it was created in.

use std::collections::BTreeSet;

use moonpool_sim::{
    Providers as _, RandomProvider as _, TimeProvider as _, assert_always, assert_reachable,
    assert_sometimes, buggify_with_prob,
};
use paros::client::resolve::{Redirection, Resolution, resolve, resolve_through};
use paros::core::journal::TenantId;

use crate::frontend::Presented;

use super::{FleetOps, removed_in, with_names};

/// A name no operator ever creates (the operators draw from `NAMES`).
const NEVER_CREATED: &[u8] = b"umbrella";

impl FleetOps {
    /// Resolve `name` at the entry endpoint, from machine `first` on, and
    /// judge the answer: a resolved tenant is never a removed one, it is
    /// served by the operator's cell, and an internal or never-created name
    /// never resolves. `created` is the tenant this operator's creation of
    /// `name` just made.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn resolve_at_entry(
        &self,
        first: usize,
        name: &[u8],
        created: Option<TenantId>,
    ) {
        let Some(cell) = &self.cell else {
            return;
        };
        if self.machines.is_empty() {
            return;
        }
        // A name nobody created, now and then: it must never resolve.
        let name = if buggify_with_prob!(0.1) {
            NEVER_CREATED
        } else {
            name
        };
        let mut entry = self.machines.clone();
        entry.rotate_left(first % self.machines.len());
        let removed = removed_in(&self.state, cell.fleet);
        let resolution = resolve(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &entry,
            name,
            self.patience,
        )
        .await;
        assert_always!(
            resolution != Resolution::Malformed,
            "resolve: a machine's answer decodes"
        );
        match &resolution {
            Resolution::Resolved(resolved) => {
                assert_always!(
                    !removed.contains(&resolved.tenant),
                    "resolve: a tenant name never resolves to a removed tenant",
                    { "tenant" => resolved.tenant.0 }
                );
                assert_always!(
                    !name.is_empty() && name != NEVER_CREATED,
                    "resolve: only a created user's tenant name resolves"
                );
                assert_always!(
                    resolved.cell_id == cell.journals.cell_id,
                    "resolve: a tenant resolves to the cell that serves it",
                    { "cell" => resolved.cell_id, "known" => cell.journals.cell_id }
                );
                assert_always!(
                    resolved.universe_id != 0,
                    "resolve: a resolution names its universe"
                );
                self.judge_cell_name(resolved.cell_id, &resolved.cell_name);
                assert_always!(
                    !resolved.universe_name.is_empty(),
                    "resolve: a resolution gives its universe's name"
                );
                assert_reachable!("resolve: an entry machine resolves a tenant");
                judge_batchers(resolved);
                judge_frontends(resolved);
                self.judge_zones(resolved);
                self.read_ranked(resolved, first).await;
            }
            Resolution::Refused {
                refusal,
                cell_id,
                cell_name,
                ..
            } => {
                if !cell_name.is_empty() {
                    self.judge_cell_name(*cell_id, cell_name);
                }
                // A second cell (#232 (multiple cells)) answers for its own
                // tenants only; its machine is in the entry endpoint too.
                assert_always!(
                    refusal != "other_cell" || crate::machine::second_cell_founded(&self.state),
                    "resolve: one universe of one cell never names another cell"
                );
                if refusal == "unknown_tenant" {
                    assert_reachable!("resolve: an entry machine knows no tenant by the name");
                }
                if refusal == "internal" {
                    assert_reachable!("resolve: an entry machine refuses an internal name");
                }
            }
            Resolution::Unavailable | Resolution::Malformed => {}
        }
        if let Some(created) = created
            && name != NEVER_CREATED
        {
            assert_sometimes!(
                matches!(&resolution, Resolution::Resolved(r) if r.tenant == created),
                "resolve: an entry machine resolves the tenant its creation made"
            );
        }
        if buggify_with_prob!(0.5) {
            self.resolve_through_resolver(&entry, name, created).await;
        }
    }

    /// Resolve `name` through the resolvers of `entry` (#233 (resolver))
    /// with a drawn token, and judge both hops. `created` is the tenant
    /// this operator's creation of `name` just made.
    async fn resolve_through_resolver(
        &self,
        entry: &[paros::model::address::Address],
        name: &[u8],
        created: Option<TenantId>,
    ) {
        let Some(cell) = &self.cell else {
            return;
        };
        // A tenant token names its tenant: an empty name (an internal
        // tenant's) has none to mint one for, so an admin asks for it.
        let presented = match self.connector.providers().random().random_range(0..10u32) {
            _ if name.is_empty() => Presented::Admin,
            0 => Presented::OtherTenant,
            1 => Presented::Garbage,
            2 => Presented::Expired,
            3 => Presented::UnknownKey,
            4 => Presented::Admin,
            _ => Presented::Own,
        };
        let allowed = matches!(presented, Presented::Own | Presented::Admin);
        let now = self.connector.providers().time().now();
        let tenant = String::from_utf8_lossy(name);
        let token = crate::frontend::token(&self.state, &tenant, "", presented, now);
        let (redirection, resolution) = resolve_through(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            entry,
            (name, b""),
            &token,
            self.patience,
        )
        .await;
        assert_always!(
            redirection != Redirection::Malformed,
            "resolver: a resolver's answer decodes"
        );
        match &redirection {
            Redirection::Redirected(redirect) => {
                assert_always!(
                    allowed,
                    "resolver: a token without the right is never redirected"
                );
                assert_always!(
                    !name.is_empty() && name != NEVER_CREATED,
                    "resolver: only a created user's tenant name is redirected"
                );
                // A resolver may answer from the directory it last read
                // (static stability): a redirect to a tenant removed since
                // is a stale hint, so only the second hop is judged on it.
                if created == Some(redirect.tenant) {
                    assert_always!(
                        redirect.cell_id == cell.journals.cell_id,
                        "resolver: a tenant is sent to the cell it was created in",
                        { "cell" => redirect.cell_id, "known" => cell.journals.cell_id }
                    );
                }
                assert_reachable!("resolver: a client is redirected to its tenant's cell");
                if let Resolution::Resolved(resolved) = &resolution
                    && resolved.tenant == redirect.tenant
                {
                    assert_always!(
                        resolved.cell_id == redirect.cell_id,
                        "resolver: the redirected cell resolves its own tenant",
                        { "cell" => resolved.cell_id, "redirect" => redirect.cell_id }
                    );
                    assert_reachable!("resolver: a client resolves its tenant in two hops");
                }
            }
            Redirection::Refused { refusal, .. } => {
                let denied = matches!(refusal.as_str(), "invalid_token" | "expired" | "forbidden");
                assert_always!(
                    !(allowed && denied),
                    "resolver: a token with the right is never denied",
                    { "refusal" => refusal.clone() }
                );
                if denied {
                    assert_reachable!("resolver: a token without the right is denied");
                }
            }
            Redirection::Unavailable | Redirection::Malformed => {}
        }
        if let Some(created) = created
            && name != NEVER_CREATED
            && presented == Presented::Own
        {
            assert_sometimes!(
                matches!(&redirection, Redirection::Redirected(r) if r.tenant == created),
                "resolver: a resolver redirects the tenant its creation made"
            );
        }
    }

    /// Read the tenant's control journal through the machines `resolved`
    /// names, ranked for this client's zone (#673 (zoned resolve)): from
    /// a machine of its zone when the answer names one, else from machine
    /// `first` on.
    async fn read_ranked(&self, resolved: &paros::client::resolve::Resolved, first: usize) {
        // A machine of its own zone first saves a cross-zone hop.
        let ranked = resolved.clone().prefer_zone(&self.zone);
        let home = ranked
            .machines
            .first()
            .is_some_and(|(id, _)| ranked.in_zone(*id, &self.zone));
        assert_always!(
            home == ranked
                .machines
                .iter()
                .any(|(id, _)| ranked.in_zone(*id, &self.zone)),
            "resolve: a client asks a machine of its zone first when one is named"
        );
        // The answer's references serve the data plane (#404
        // (static vs dynamic endpoints)): the tenant's control
        // journal, read through the machines it names.
        let start = if home { 0 } else { first };
        if self
            .connector
            .resolved(&ranked)
            .journal_state(ranked.control, start)
            .await
            .is_some()
        {
            assert_reachable!("resolve: a client reads its tenant through the answered machines");
            if home {
                assert_reachable!("resolve: a client reads its tenant from its own zone first");
            }
        }
    }

    /// Judge the zones a resolution gave its machines (#673 (zoned
    /// resolve)): each is the zone the machine's layout placed it in, as
    /// the machine registered it, whenever the board knows the machine.
    /// Every machine of the layout stands in a zone, so the registry holds
    /// one for every machine that registered.
    fn judge_zones(&self, resolved: &paros::client::resolve::Resolved) {
        for (id, zone) in &resolved.zones {
            let Some(truth) = crate::machine::zone_of(&self.state, id.0) else {
                continue;
            };
            assert_always!(
                *zone == truth,
                "resolve: a machine's zone is the one it stands in",
                { "node" => id.0, "answered" => zone.clone(), "truth" => truth }
            );
        }
        if !resolved.zones.is_empty() {
            assert_reachable!("resolve: a resolution names its machines' zones");
        }
    }

    /// Judge the name a resolution gave cell `cell_id` (#239 (names at the
    /// edge)): every resolution names the cell, and always by the name the
    /// first one gave: a cell's name never changes.
    fn judge_cell_name(&self, cell_id: u64, name: &[u8]) {
        assert_always!(
            !name.is_empty(),
            "resolve: a resolution gives its cell's name",
            { "cell" => cell_id }
        );
        let first = with_names(&self.state, |names| {
            names
                .cells
                .entry(cell_id)
                .or_insert_with(|| name.to_vec())
                .clone()
        });
        assert_always!(
            first == name,
            "resolve: a cell's name never changes",
            { "cell" => cell_id }
        );
        assert_reachable!("resolve: a resolution names its cell by name");
    }
}

impl FleetOps {
    /// Every founding member of `cell` that was never wiped answers
    /// `Resolve` after chaos, by `deadline` (#457 (hard-coded tunables)): a
    /// verdict or a refusal, never `unavailable` for good. The machines'
    /// `Resolve` folds are internal readers of the registry and the universe
    /// directory; under chaos an answer is often cut short
    /// (`resolve_answer_within`), and a cut answer once reset both folds to
    /// position 0, so a fold longer than the bound never answered again.
    /// This is the escape check of that loop: the cause is gone, the
    /// machine must answer by itself.
    ///
    /// The check waits for each answer as long as a machine may take over
    /// one, plus its round trip, whatever this operator's own patience
    /// (#549 (resolve oracle red)). A machine answers one request at a
    /// time, and an answer reads two tails through the cell's machines: it
    /// routinely takes longer than a short patience (450 to 900 ms against 537 ms on
    /// witness 6245465788840461241): the operator gave up on every ask, and
    /// its abandoned asks queued ahead of the next one, so each answer came
    /// back to a caller already gone. That is an impatient client, not a
    /// silent machine.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn resolve_answers(
        &self,
        ctx: &moonpool_sim::SimContext,
        cell: &super::Cell,
        deadline: std::time::Duration,
    ) {
        use moonpool_sim::TimeProvider;
        let patience = self
            .patience
            .max(crate::shape::resolve_answer_ceiling() + crate::shape::RESOLVE_ROUND_TRIP);
        // Every member is asked in each round, so one member that stays
        // silent cannot use up the others' time. A wipe is read again each
        // round and at the verdict: the replaced-founder scenario may strike
        // `LATE_WINDOW` into the tail, and the board learns of a wipe only
        // at the replacement's boot, so a founder wiped during this check
        // is excused like one wiped before it (witness recipe
        // `12026813195170844786 216871:7753195129087799838` on edde689: the
        // wipe landed between the first ask and the verdict).
        let excused = |silent: &mut Vec<(u64, _)>| {
            let wiped = crate::machine::wiped_ids(ctx.state());
            silent.retain(|(id, _)| !wiped.contains(id));
        };
        let mut silent: Vec<(u64, _)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();
        excused(&mut silent);
        while !silent.is_empty() && ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            let mut still = Vec::new();
            for (id, addr) in silent {
                let resolution = resolve(
                    self.connector.providers(),
                    self.connector.rpc(),
                    self.connector.names(),
                    std::slice::from_ref(&addr),
                    NEVER_CREATED,
                    patience,
                )
                .await;
                if resolution == Resolution::Unavailable {
                    still.push((id, addr));
                }
            }
            silent = still;
            excused(&mut silent);
            if !silent.is_empty()
                && ctx
                    .time()
                    .sleep(std::time::Duration::from_millis(100))
                    .await
                    .is_err()
            {
                break;
            }
        }
        let silent: Vec<u64> = silent.into_iter().map(|(id, _)| id).collect();
        if ctx.shutdown().is_cancelled() {
            return;
        }
        assert_always!(
            silent.is_empty(),
            "resolve: every founding member answers after chaos",
            { "cell" => cell.journals.cell_id, "silent" => format!("{silent:?}") }
        );
        assert_reachable!("resolve: every founding member's answer is judged after chaos");
    }
}

/// A tenant's batchers (#145 (batchers)) are machines of its cell, each
/// named once.
fn judge_batchers(resolved: &paros::client::resolve::Resolved) {
    let named: BTreeSet<u64> = resolved.batchers.iter().map(|(id, _)| id.0).collect();
    assert_always!(
        named.len() == resolved.batchers.len()
            && resolved
                .batchers
                .iter()
                .all(|(id, _)| resolved.machines.iter().any(|(m, _)| m == id)),
        "resolve: a tenant's batchers are machines of its cell, each once",
        { "batchers" => resolved.batchers.len(), "machines" => resolved.machines.len() }
    );
    if !resolved.batchers.is_empty() {
        assert_reachable!("resolve: a resolution names the tenant's batchers");
    }
}

/// A tenant's frontends (#723 (frontend as a parosd role)) are machines of
/// its cell, each named once.
fn judge_frontends(resolved: &paros::client::resolve::Resolved) {
    let named: BTreeSet<u64> = resolved.frontends.iter().map(|(id, _)| id.0).collect();
    assert_always!(
        named.len() == resolved.frontends.len()
            && resolved
                .frontends
                .iter()
                .all(|(id, _)| resolved.machines.iter().any(|(m, _)| m == id)),
        "resolve: a tenant's frontends are machines of its cell, each once",
        { "frontends" => resolved.frontends.len(), "machines" => resolved.machines.len() }
    );
    if !resolved.frontends.is_empty() {
        assert_reachable!("resolve: a resolution names the tenant's frontends");
    }
}
