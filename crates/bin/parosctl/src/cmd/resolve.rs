//! `parosctl resolve <tenant>` (#216): ask the entry endpoint (`--servers`)
//! which references serve a tenant, as a client does first
//! (`paros::client::resolve`). Any machine of a cell answers it from its
//! folds of the universe directory and the registry.

use std::time::Duration;

use clap::Args;
use moonpool_core::CompioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::resolve::{Resolution, resolve};
use paros::model::address::{Address, Names};
use serde_json::json;

use crate::support::connect::Ending;
use crate::support::output::{Printer, note, record_text, short};

/// `parosctl resolve`.
#[derive(Args, Debug)]
pub struct ResolveArgs {
    /// The tenant's name.
    tenant: String,
}

/// `parosctl resolve <tenant>` through the entry endpoint `entry`.
pub async fn run(
    providers: &CompioProviders,
    rpc: &RpcHandle<CompioProviders>,
    names: &Names,
    entry: &[Address],
    timeout: Duration,
    out: &Printer,
    args: ResolveArgs,
) -> Ending {
    let tenant = args.tenant;
    match resolve(providers, rpc, names, entry, tenant.as_bytes(), timeout).await {
        Resolution::Resolved(resolved) => {
            let machines: Vec<String> = resolved
                .machines
                .iter()
                .map(|(_, addr)| addr.to_string())
                .collect();
            // The tenant's frontends (#723 (frontend as a parosd role)):
            // where `--frontends` points a tenant's calls.
            let frontends: Vec<String> = resolved
                .frontends
                .iter()
                .map(|(_, addr)| addr.to_string())
                .collect();
            out.emit(
                || {
                    format!(
                        "tenant={tenant} universe={} cell={} machines={} frontends={}",
                        named(&resolved.universe_name, resolved.universe_id),
                        named(&resolved.cell_name, resolved.cell_id),
                        machines.join(","),
                        frontends.join(",")
                    )
                },
                || {
                    json!({
                        "tenant": tenant,
                        "tenant_id": resolved.tenant.0,
                        "control_journal": resolved.control.journal.0,
                        "cell": resolved.cell_id,
                        "cell_name": record_text(&resolved.cell_name),
                        "universe": resolved.universe_id,
                        "universe_name": record_text(&resolved.universe_name),
                        "machines": resolved
                            .machines
                            .iter()
                            .map(|(id, addr)| json!({"node": id.0, "addr": addr.to_string()}))
                            .collect::<Vec<_>>(),
                        "frontends": resolved
                            .frontends
                            .iter()
                            .map(|(id, addr)| json!({"node": id.0, "addr": addr.to_string()}))
                            .collect::<Vec<_>>(),
                        "at": resolved.at,
                    })
                },
            );
            Ending::Success
        }
        Resolution::Refused {
            refusal,
            cell_id,
            tenant_cell,
            cell_name,
            tenant_cell_name,
        } => {
            let detail = match refusal.as_str() {
                "unknown_tenant" => format!("no tenant is named {tenant:?}"),
                "not_ready" => format!("tenant {tenant:?} is being created or removed"),
                "internal" => format!("{tenant:?} is not a user's tenant"),
                "other_cell" => format!(
                    "tenant {tenant:?} is served by cell {}, not by cell {}",
                    named(&tenant_cell_name, tenant_cell),
                    named(&cell_name, cell_id)
                ),
                "no_universe" => format!(
                    "cell {} serves no universe directory: is the universe initialized?",
                    named(&cell_name, cell_id)
                ),
                other => format!("refused: {other}"),
            };
            note(&detail);
            Ending::Refused
        }
        Resolution::Unavailable => {
            note("no machine of the entry endpoint answered");
            Ending::Unreachable
        }
        Resolution::Malformed => {
            note("a machine answered with a resolution that does not decode");
            Ending::Refused
        }
    }
}

/// An entity's name, or its short hex id when the answer names none (#239
/// (names at the edge)): hex ids are for debugging only.
fn named(name: &[u8], id: u64) -> String {
    if name.is_empty() {
        short(id)
    } else {
        record_text(name)
    }
}
