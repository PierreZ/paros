//! `Resolve` at the entry endpoint (#216): after the library resolves a
//! tenant's name through the universe directory (#239), the operator asks the
//! machines themselves, as a client handed only addresses does
//! (`paros::client::resolve`). The entry endpoint is every machine, from a
//! drawn one on: idle machines do not serve `Resolve` and are passed over,
//! founding and admitted machines answer from their own folds.

use std::collections::BTreeSet;

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes, buggify_with_prob};
use paros::TenantId;
use paros::client::resolve::{Resolution, resolve};

use super::{FleetOps, with_names};

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
        let removed: BTreeSet<TenantId> =
            with_names(&self.state, |names| names.removed.keys().copied().collect());
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
                assert_reachable!("resolve: an entry machine resolves a tenant");
            }
            Resolution::Refused { refusal, .. } => {
                assert_always!(
                    refusal != "other_cell",
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
    }
}
