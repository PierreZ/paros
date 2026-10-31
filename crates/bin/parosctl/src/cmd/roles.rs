//! `parosctl roles` (#399): who holds which role now, and which slots
//! each machine reserves. A `slot:ROLE` row counts the slots one machine
//! reserves for one target: a journal, a matchmaker set, a tenant's
//! `(footprint)` (the slots its birth reserves, #476), or a role of the
//! tenant as a whole (`(tenant-wide)`, its frontends).

use std::collections::BTreeMap;

use clap::Args;
use paros::model::view::Scope;
use paros::wire::view::{self as wire, CellQuery, TenantQuery, view_request::Query};
use serde_json::json;

use crate::support::connect::Ending;
use crate::support::output::{Printer, note, table};
use crate::support::view::{
    Asker, find_machine, journal_label, machine_names, refusal_text, reply_json, text,
};

/// `parosctl roles`: who holds which role now.
#[derive(Args, Debug)]
pub struct RolesArgs {
    /// Every role of this cell (the servers' cell by default).
    #[arg(long, conflicts_with_all = ["tenant", "machine"])]
    cell: Option<String>,
    /// The roles that serve this tenant.
    #[arg(long, conflicts_with = "machine")]
    tenant: Option<String>,
    /// The roles this machine holds.
    #[arg(long)]
    machine: Option<String>,
}

/// One role holder, or the slots one machine reserves for one target.
pub struct Holding {
    pub role: String,
    pub holder: u64,
    pub tenant: Vec<u8>,
    pub journal: String,
    /// How many slots the row stands for: 0 for a role held now.
    pub slots: usize,
}

/// Where a row sorts: the coordinators, then the roles held, then the
/// slots reserved.
fn rank(role: &str) -> u8 {
    match role {
        "cell-coordinator" => 0,
        "tenant-coordinator" => 1,
        _ if role.starts_with("slot:") => 3,
        _ => 2,
    }
}

impl Holding {
    pub fn row(&self, reply: &wire::ViewReply) -> [String; 4] {
        let names = machine_names(reply);
        let role = if self.slots > 1 {
            format!("{} x{}", self.role, self.slots)
        } else {
            self.role.clone()
        };
        [
            role,
            names
                .get(&self.holder)
                .cloned()
                .unwrap_or_else(|| "-".to_string()),
            text(&self.tenant),
            self.journal.clone(),
        ]
    }
}

/// What a booking reserves its slot for, in words: a journal, a
/// matchmaker set, the tenant's footprint (the slots its birth reserves),
/// or a role of the tenant as a whole (its frontends).
fn booking_target(booking: &wire::BookingView, journals: &BTreeMap<(u64, u64), String>) -> String {
    if booking.tenant_level && booking.role == "frontend" {
        "(tenant-wide)".to_string()
    } else if booking.tenant_level {
        "(footprint)".to_string()
    } else if booking.journal != 0 {
        journals
            .get(&(booking.tenant, booking.journal))
            .cloned()
            .unwrap_or_else(|| "-".to_string())
    } else {
        "(matchmaker set)".to_string()
    }
}

/// Every role holder an answer names: the coordinator, every journal's
/// acceptors and matchmakers, and the slots each machine reserves, one row
/// per machine and target. A slot of a tenant the answer does not name (an
/// internal tenant, or a tenant outside the scope) is left out. The rows
/// sort by kind, then by machine name, tenant and journal.
pub fn roles(reply: &wire::ViewReply) -> Vec<Holding> {
    let mut roles = Vec::new();
    if reply.coordinator != 0 {
        roles.push(Holding {
            role: "cell-coordinator".to_string(),
            holder: reply.coordinator,
            tenant: Vec::new(),
            journal: format!("term {}", reply.coordinator_term),
            slots: 0,
        });
        // The cell coordinator is the tenant coordinator of every tenant it
        // hosts until placement (#212, #225).
        for tenant in reply.tenants.iter().filter(|t| t.kind == "users") {
            roles.push(Holding {
                role: "tenant-coordinator".to_string(),
                holder: reply.coordinator,
                tenant: tenant.name.clone(),
                journal: "-".to_string(),
                slots: 0,
            });
        }
    }
    for tenant in &reply.tenants {
        for journal in &tenant.journals {
            for (role, ids) in [
                ("acceptor", &journal.acceptors),
                ("matchmaker", &journal.matchmakers),
            ] {
                roles.extend(ids.iter().map(|id| Holding {
                    role: role.to_string(),
                    holder: *id,
                    tenant: tenant.name.clone(),
                    journal: journal_label(journal),
                    slots: 0,
                }));
            }
        }
    }
    let tenant_names: BTreeMap<u64, Vec<u8>> = reply
        .tenants
        .iter()
        .map(|t| (t.tenant, t.name.clone()))
        .collect();
    let journal_names: BTreeMap<(u64, u64), String> = reply
        .tenants
        .iter()
        .flat_map(|t| {
            t.journals
                .iter()
                .map(move |j| ((t.tenant, j.journal), journal_label(j)))
        })
        .collect();
    let mut slots: BTreeMap<(u64, String, Vec<u8>, String), usize> = BTreeMap::new();
    for machine in &reply.machines {
        for booking in &machine.bookings {
            let Some(tenant) = tenant_names.get(&booking.tenant) else {
                continue;
            };
            let key = (
                machine.node_id,
                format!("slot:{}", booking.role),
                tenant.clone(),
                booking_target(booking, &journal_names),
            );
            *slots.entry(key).or_default() += 1;
        }
    }
    roles.extend(
        slots
            .into_iter()
            .map(|((holder, role, tenant, journal), slots)| Holding {
                role,
                holder,
                tenant,
                journal,
                slots,
            }),
    );
    let names = machine_names(reply);
    roles.sort_by(|a, b| {
        (
            rank(&a.role),
            names.get(&a.holder),
            &a.tenant,
            &a.journal,
            &a.role,
        )
            .cmp(&(
                rank(&b.role),
                names.get(&b.holder),
                &b.tenant,
                &b.journal,
                &b.role,
            ))
    });
    roles
}

/// `parosctl roles`.
pub async fn roles_cmd(asker: &Asker<'_>, out: &Printer, args: RolesArgs) -> Ending {
    // A tenant's roles are read off its cell's whole view: a tenant view
    // names only the machines its journals use, not the stateless machines
    // that hold its frontends, proxy leaders and batchers.
    let reply = match &args.tenant {
        Some(tenant) => match asker
            .ask_followed(Query::Tenant(TenantQuery {
                name: tenant.as_bytes().to_vec(),
            }))
            .await
        {
            Ok((reply, _)) if !matches!(asker.scope, Scope::Admin) => Ok(reply),
            Ok((_, servers)) => match asker.ask_at(&servers, Query::Cell(CellQuery {})).await {
                Ok(Ok(reply)) => Ok(reply),
                Ok(Err(refused)) => {
                    note(&refusal_text(&refused));
                    Err(Ending::Refused)
                }
                Err(ending) => Err(ending),
            },
            Err(ending) => Err(ending),
        },
        None => asker.cell(args.cell.as_deref()).await,
    };
    let reply = match reply {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let holder = match &args.machine {
        Some(name) => match find_machine(&reply, name) {
            Ok(m) => Some(m.node_id),
            Err(ending) => return ending,
        },
        None => None,
    };
    let rows: Vec<[String; 4]> = roles(&reply)
        .into_iter()
        .filter(|r| holder.is_none_or(|h| r.holder == h))
        .filter(|r| {
            args.tenant
                .as_ref()
                .is_none_or(|t| r.tenant == t.as_bytes())
        })
        .map(|r| r.row(&reply))
        .collect();
    out.emit(
        || table(["ROLE", "HOLDER", "TENANT", "JOURNAL"], &rows),
        || {
            let mut doc = reply_json(&reply);
            if let Some(holder) = holder {
                doc["holder"] = json!(holder);
            }
            doc
        },
    );
    Ending::Success
}
