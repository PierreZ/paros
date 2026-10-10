//! Names for what `parosctl` prints (#399): hex ids are for local debugging
//! only, so every command names a machine, a cell, the universe, a tenant
//! and a journal by its name.
//!
//! [`Labels`] is one admin cell view (`crate::views`) of the servers' cell,
//! read once per command: its machines, its tenants and their journals.
//! Anything the view does not name — no answer, an entity of another cell
//! — prints as its short hex id, the debugging form.

use std::collections::BTreeMap;

use paros::client::views::{ViewOutcome, ask, request};
use paros::view::Scope;
use paros::wire::view::{CellQuery, ViewReply, view_request::Query};
use paros::{JournalIdentifier, TenantId};

use crate::output::{record_text, short};
use crate::views::Asker;

/// The names one cell view gave.
#[derive(Clone, Debug, Default)]
pub struct Labels {
    machines: BTreeMap<u64, String>,
    tenants: BTreeMap<u64, String>,
    journals: BTreeMap<(u64, u64), String>,
    cells: BTreeMap<u64, String>,
    universe: Option<(u64, String)>,
}

impl Labels {
    /// The names of the servers' cell, from one admin cell view; none when
    /// no server answered it.
    pub async fn fetch(asker: &Asker<'_>) -> Self {
        let request = request(&Scope::Admin, Query::Cell(CellQuery {}));
        match ask(
            asker.providers,
            asker.rpc,
            asker.names,
            &asker.servers,
            0,
            &request,
            asker.timeout,
        )
        .await
        {
            ViewOutcome::Answered(reply) => Self::of(&reply),
            ViewOutcome::Refused(_) | ViewOutcome::Unreachable => Self::default(),
        }
    }

    /// The names `reply` gives.
    pub fn of(reply: &ViewReply) -> Self {
        let mut labels = Self::default();
        for machine in &reply.machines {
            let name = if machine.name.is_empty() {
                machine.addr.clone()
            } else {
                machine.name.clone()
            };
            if !name.is_empty() {
                labels.machines.insert(machine.node_id, name);
            }
        }
        for tenant in &reply.tenants {
            let name = match tenant.kind.as_str() {
                "users" => record_text(&tenant.name),
                kind if tenant.name.is_empty() => kind.to_string(),
                kind => format!("{kind}:{}", record_text(&tenant.name)),
            };
            labels.tenants.insert(tenant.tenant, name.clone());
            for journal in &tenant.journals {
                let journal_name = if journal.name.is_empty() {
                    format!("({})", journal.kind)
                } else {
                    record_text(&journal.name)
                };
                labels.journals.insert(
                    (tenant.tenant, journal.journal),
                    format!("{name}/{journal_name}"),
                );
            }
        }
        if !reply.cell_name.is_empty() {
            labels
                .cells
                .insert(reply.cell_id, record_text(&reply.cell_name));
        }
        if reply.universe_id != 0 {
            labels.universe = Some((reply.universe_id, record_text(&reply.universe_name)));
        }
        labels
    }

    /// Machine `id`'s name.
    pub fn machine(&self, id: u64) -> String {
        self.machines.get(&id).cloned().unwrap_or_else(|| short(id))
    }

    /// Machines' names, comma-separated, in brackets.
    pub fn machines(&self, ids: &[u64]) -> String {
        let names: Vec<String> = ids.iter().map(|id| self.machine(*id)).collect();
        format!("[{}]", names.join(","))
    }

    /// The machine named `name`, among `among`.
    pub fn machine_named(&self, name: &str, among: &[u64]) -> Option<u64> {
        let found: Vec<u64> = among
            .iter()
            .copied()
            .filter(|id| self.machines.get(id).is_some_and(|n| n == name))
            .collect();
        match found.as_slice() {
            [one] => Some(*one),
            _ => None,
        }
    }

    /// Tenant `tenant`'s name.
    pub fn tenant(&self, tenant: TenantId) -> String {
        self.tenants
            .get(&tenant.0)
            .cloned()
            .unwrap_or_else(|| short(tenant.0))
    }

    /// Journal `journal`'s name, `TENANT/JOURNAL`; `id:` and its short ids
    /// when the view does not name it.
    pub fn journal(&self, journal: JournalIdentifier) -> String {
        self.journals
            .get(&(journal.tenant.0, journal.journal.0))
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "id:{}/{}",
                    short(journal.tenant.0),
                    short(journal.journal.0)
                )
            })
    }

    /// Cell `cell_id`'s name.
    pub fn cell(&self, cell_id: u64) -> String {
        if cell_id == 0 {
            return "none".to_string();
        }
        self.cells
            .get(&cell_id)
            .cloned()
            .unwrap_or_else(|| short(cell_id))
    }

    /// The universe `universe_id`'s name.
    pub fn universe(&self, universe_id: u64) -> String {
        match &self.universe {
            Some((id, name)) if *id == universe_id && !name.is_empty() => name.clone(),
            _ => short(universe_id),
        }
    }
}
