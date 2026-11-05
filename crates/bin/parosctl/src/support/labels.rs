//! Names for what `parosctl` prints (#399): hex ids are for local debugging
//! only, so every command names a machine, a cell, the universe, a tenant
//! and a journal by its name.
//!
//! [`Labels`] is one admin cell view (`crate::support::view`) of the servers' cell,
//! read once per command: its machines, its tenants and their journals. A
//! command that follows a tenant to another cell adds that cell's names.
//! Anything the view does not name — no answer, an entity of another cell
//! — prints as its short hex id, the debugging form.

use std::collections::BTreeMap;
use std::time::Duration;

use moonpool_core::CompioProviders;
use moonpool_rpc::RpcHandle;
use paros::model::address::{Address, Names};

use paros::client::views::{ViewOutcome, ask, request};
use paros::core::journal::{JournalIdentifier, TenantId};
use paros::model::view::Scope;
use paros::wire::view::{CellQuery, ViewReply, view_request::Query};

use crate::support::output::{record_text, short};
use crate::support::view::Asker;

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
        Self::fetch_from(
            (asker.providers, asker.rpc, asker.names),
            &asker.servers,
            asker.timeout,
        )
        .await
    }

    /// The names of the cell of `servers` (another cell's entry endpoint,
    /// #232 (multiple cells)), from one admin cell view; none when no server
    /// answered it.
    pub async fn fetch_from(
        (providers, rpc, names): (&CompioProviders, &RpcHandle<CompioProviders>, &Names),
        servers: &[Address],
        timeout: Duration,
    ) -> Self {
        let request = request(&Scope::Admin, Query::Cell(CellQuery {}));
        match ask(providers, rpc, names, servers, 0, &request, timeout).await {
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

    /// Take the names `other` gives that these do not.
    pub fn absorb(&mut self, other: Self) {
        for (id, name) in other.machines {
            self.machines.entry(id).or_insert(name);
        }
        for (id, name) in other.tenants {
            self.tenants.entry(id).or_insert(name);
        }
        for (id, name) in other.journals {
            self.journals.entry(id).or_insert(name);
        }
        for (id, name) in other.cells {
            self.cells.entry(id).or_insert(name);
        }
        if self.universe.is_none() {
            self.universe = other.universe;
        }
    }

    /// The names of the universe's cells and tenants, from its directory
    /// (#713 (bucket placement)): what a universe command that read the
    /// directory prints a refusal with.
    pub fn from_directory(directory: &paros::model::fleet::FleetDirectory) -> Self {
        let mut labels = Self::default();
        for (cell_id, _) in directory.cells() {
            let name = directory.cell_name(cell_id);
            if !name.is_empty() {
                labels.cells.insert(cell_id, record_text(name));
            }
        }
        for (tenant, entry) in directory.tenants() {
            if entry.kind == paros::model::fleet::TenantKind::Users {
                labels.tenants.insert(tenant.0, record_text(&entry.name));
            }
        }
        labels
    }

    /// Name machine `id` `name`, unless a view already named it.
    pub fn name_machine(&mut self, id: u64, name: &str) {
        self.machines.entry(id).or_insert_with(|| name.to_string());
    }

    /// Machine `id`'s name.
    pub fn machine(&self, id: u64) -> String {
        self.machines.get(&id).cloned().unwrap_or_else(|| short(id))
    }

    /// Machines' names, in name order, comma-separated, in brackets.
    pub fn machines(&self, ids: &[u64]) -> String {
        let mut names: Vec<String> = ids.iter().map(|id| self.machine(*id)).collect();
        names.sort();
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

#[cfg(test)]
mod tests {
    use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
    use paros::model::fleet::{
        CellKind, CellState, FleetCommand, FleetDirectory, FleetEntry, Survives, buckets,
    };
    use paros::model::tags::UserTags;
    use paros::wire::view::{JournalView, MachineView, TenantView, ViewReply};

    use super::Labels;
    use crate::support::output::short;

    fn machine(node_id: u64, name: &str, addr: &str) -> MachineView {
        MachineView {
            node_id,
            name: name.into(),
            addr: addr.into(),
            ..MachineView::default()
        }
    }

    fn tenant(id: u64, kind: &str, name: &str, journals: Vec<JournalView>) -> TenantView {
        TenantView {
            tenant: id,
            kind: kind.into(),
            name: name.as_bytes().to_vec(),
            journals,
            ..TenantView::default()
        }
    }

    fn journal(id: u64, kind: &str, name: &str) -> JournalView {
        JournalView {
            journal: id,
            kind: kind.into(),
            name: name.as_bytes().to_vec(),
            ..JournalView::default()
        }
    }

    fn id(tenant: u64, journal: u64) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(tenant), JournalId(journal))
    }

    fn reply() -> ViewReply {
        ViewReply {
            cell_id: 0xce11,
            cell_name: b"west".to_vec(),
            universe_id: 0xf1ee7,
            universe_name: b"prod".to_vec(),
            machines: vec![
                machine(1, "alpha", "10.0.0.1:4500"),
                machine(2, "", "10.0.0.2:4500"),
                machine(3, "", ""),
            ],
            tenants: vec![
                tenant(
                    10,
                    "users",
                    "acme",
                    vec![journal(11, "users", "orders"), journal(12, "control", "")],
                ),
                tenant(20, "cell", "", vec![]),
                tenant(30, "fleet", "main", vec![]),
            ],
            ..ViewReply::default()
        }
    }

    #[test]
    fn a_view_names_every_machine_tenant_journal_cell_and_the_universe() {
        let labels = Labels::of(&reply());
        assert_eq!(labels.machine(1), "alpha");
        assert_eq!(labels.machine(2), "10.0.0.2:4500", "the address names it");
        assert_eq!(labels.machine(3), short(3), "no name, no address");
        assert_eq!(labels.machines(&[2, 1]), "[10.0.0.2:4500,alpha]");
        assert_eq!(labels.tenant(TenantId(10)), "acme", "a users tenant");
        assert_eq!(labels.tenant(TenantId(20)), "cell", "a kind without a name");
        assert_eq!(labels.tenant(TenantId(30)), "fleet:main");
        assert_eq!(labels.tenant(TenantId(40)), short(40));
        assert_eq!(labels.journal(id(10, 11)), "acme/orders");
        assert_eq!(labels.journal(id(10, 12)), "acme/(control)");
        assert_eq!(
            labels.journal(id(10, 13)),
            format!("id:{}/{}", short(10), short(13))
        );
        assert_eq!(labels.cell(0xce11), "west");
        assert_eq!(labels.cell(0), "none");
        assert_eq!(labels.cell(7), short(7));
        assert_eq!(labels.universe(0xf1ee7), "prod");
        assert_eq!(labels.universe(5), short(5), "another universe");
    }

    #[test]
    fn an_unnamed_cell_or_universe_shows_its_short_id() {
        let labels = Labels::of(&ViewReply {
            cell_id: 9,
            universe_id: 8,
            ..ViewReply::default()
        });
        assert_eq!(labels.cell(9), short(9));
        assert_eq!(labels.universe(8), short(8), "an empty name");
        let none = Labels::of(&ViewReply::default());
        assert_eq!(none.universe(0), short(0));
    }

    #[test]
    fn absorb_keeps_the_names_it_already_holds() {
        let mut labels = Labels::of(&reply());
        let mut other = Labels::of(&ViewReply {
            cell_id: 0xce11,
            cell_name: b"east".to_vec(),
            universe_id: 0xbeef,
            universe_name: b"staging".to_vec(),
            machines: vec![machine(1, "beta", ""), machine(4, "delta", "")],
            tenants: vec![tenant(
                10,
                "users",
                "other",
                vec![journal(11, "users", "x"), journal(14, "users", "y")],
            )],
            ..ViewReply::default()
        });
        other.cells.insert(0xc2, "north".into());
        other.tenants.insert(50, "new".into());
        labels.absorb(other);
        assert_eq!(labels.machine(1), "alpha", "kept");
        assert_eq!(labels.machine(4), "delta", "added");
        assert_eq!(labels.tenant(TenantId(10)), "acme");
        assert_eq!(labels.tenant(TenantId(50)), "new");
        assert_eq!(labels.journal(id(10, 11)), "acme/orders");
        assert_eq!(labels.journal(id(10, 14)), "other/y");
        assert_eq!(labels.cell(0xce11), "west");
        assert_eq!(labels.cell(0xc2), "north");
        assert_eq!(labels.universe(0xf1ee7), "prod");
        let mut empty = Labels::default();
        empty.absorb(Labels::of(&reply()));
        assert_eq!(empty.universe(0xf1ee7), "prod", "taken when none is held");
    }

    #[test]
    fn a_machine_is_found_by_its_name_only_when_one_holds_it() {
        let mut labels = Labels::of(&reply());
        assert_eq!(labels.machine_named("alpha", &[1, 2]), Some(1));
        assert_eq!(labels.machine_named("alpha", &[2]), None, "not among");
        assert_eq!(labels.machine_named("nobody", &[1, 2]), None);
        labels.name_machine(5, "alpha");
        labels.name_machine(1, "renamed");
        assert_eq!(labels.machine(1), "alpha", "a name is never replaced");
        assert_eq!(labels.machine_named("alpha", &[1, 5]), None, "two hold it");
    }

    #[test]
    fn a_directory_names_its_cells_and_its_users_tenants() {
        let fleet = 0xf1ee7;
        let mut directory = FleetDirectory::default();
        let entries = [
            FleetCommand::FormFleet {
                control: id(1, 2),
                name: b"prod".to_vec(),
            },
            FleetCommand::AddCell {
                cell_id: 0xce11,
                control: id(3, 4),
                name: b"west".to_vec(),
                kind: CellKind::default(),
                endpoint: Vec::new(),
            },
            FleetCommand::AddCell {
                cell_id: 0xce12,
                control: id(5, 6),
                name: Vec::new(),
                kind: CellKind::default(),
                endpoint: Vec::new(),
            },
            FleetCommand::MarkCell {
                cell_id: 0xce11,
                state: CellState::Ready,
            },
            FleetCommand::RegisterTenant {
                control: id(10, 11),
                name: b"acme".to_vec(),
                cell_id: 0xce11,
                survives: Survives::Az,
                tags: UserTags::none(),
                placement: buckets::Placement::Bucket,
            },
        ];
        for (seq, command) in entries.into_iter().enumerate() {
            directory.fold(seq as u64, &FleetEntry::new(fleet, command).encode());
        }
        let labels = Labels::from_directory(&directory);
        assert_eq!(labels.cell(0xce11), "west");
        assert_eq!(labels.cell(0xce12), short(0xce12), "an unnamed cell");
        assert_eq!(labels.tenant(TenantId(10)), "acme");
        assert_eq!(
            labels.tenant(TenantId(1)),
            short(1),
            "the fleet tenant is not a users tenant"
        );
    }
}
