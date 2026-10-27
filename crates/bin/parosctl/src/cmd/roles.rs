//! `parosctl roles` (#399): who holds which role now.

use std::collections::BTreeMap;

use clap::Args;
use paros::wire::view::{self as wire, TenantQuery, view_request::Query};
use serde_json::json;

use crate::support::connect::Ending;
use crate::support::output::{Printer, table};
use crate::support::view::{Asker, find_machine, journal_label, machine_names, reply_json, text};

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

/// One role holder.
pub struct Holding {
    pub role: String,
    pub holder: u64,
    pub tenant: Vec<u8>,
    pub journal: String,
}

impl Holding {
    pub fn row(&self, reply: &wire::ViewReply) -> [String; 4] {
        let names = machine_names(reply);
        [
            self.role.clone(),
            names
                .get(&self.holder)
                .cloned()
                .unwrap_or_else(|| "-".to_string()),
            text(&self.tenant),
            self.journal.clone(),
        ]
    }
}

/// Every role holder an answer names: the coordinator, every journal's
/// acceptors and matchmakers, and every booking.
pub fn roles(reply: &wire::ViewReply) -> Vec<Holding> {
    let mut roles = Vec::new();
    if reply.coordinator != 0 {
        roles.push(Holding {
            role: "cell-coordinator".to_string(),
            holder: reply.coordinator,
            tenant: Vec::new(),
            journal: format!("term {}", reply.coordinator_term),
        });
        // The cell coordinator is the tenant coordinator of every tenant it
        // hosts until placement (#212, #225).
        for tenant in reply.tenants.iter().filter(|t| t.kind == "users") {
            roles.push(Holding {
                role: "tenant-coordinator".to_string(),
                holder: reply.coordinator,
                tenant: tenant.name.clone(),
                journal: "-".to_string(),
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
    for machine in &reply.machines {
        for booking in &machine.bookings {
            roles.push(Holding {
                role: format!("booked:{}", booking.role),
                holder: machine.node_id,
                tenant: tenant_names
                    .get(&booking.tenant)
                    .cloned()
                    .unwrap_or_default(),
                journal: if booking.journal == 0 {
                    "(matchmaker set)".to_string()
                } else {
                    journal_names
                        .get(&(booking.tenant, booking.journal))
                        .cloned()
                        .unwrap_or_else(|| "-".to_string())
                },
            });
        }
    }
    roles
}

/// `parosctl roles`.
pub async fn roles_cmd(asker: &Asker<'_>, out: &Printer, args: RolesArgs) -> Ending {
    let reply = match &args.tenant {
        Some(tenant) => {
            asker
                .ask(Query::Tenant(TenantQuery {
                    name: tenant.as_bytes().to_vec(),
                }))
                .await
        }
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
