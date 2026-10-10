//! `parosctl resolve <tenant>` (#216): ask the entry endpoint (`--servers`)
//! which references serve a tenant, as a client does first
//! (`paros::client::resolve`). Any machine of a cell answers it from its
//! folds of the universe directory and the registry.

use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::resolve::{Resolution, resolve};
use paros::{Address, Names};
use serde_json::json;

use crate::Ending;
use crate::output::{Printer, note, short};

/// `parosctl resolve`.
#[derive(Args, Debug)]
pub struct ResolveArgs {
    /// The tenant's name.
    tenant: String,
}

/// `parosctl resolve <tenant>` through the entry endpoint `entry`.
pub async fn run(
    providers: &TokioProviders,
    rpc: &RpcHandle<TokioProviders>,
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
            out.emit(
                || {
                    format!(
                        "tenant={tenant} cell={} machines={}",
                        short(resolved.cell_id),
                        machines.join(",")
                    )
                },
                || {
                    json!({
                        "tenant": tenant,
                        "tenant_id": resolved.tenant.0,
                        "control_journal": resolved.control.journal.0,
                        "cell": resolved.cell_id,
                        "universe": resolved.universe_id,
                        "machines": resolved
                            .machines
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
        } => {
            let detail = match refusal.as_str() {
                "unknown_tenant" => format!("no tenant is named {tenant:?}"),
                "not_ready" => format!("tenant {tenant:?} is being created or removed"),
                "internal" => format!("{tenant:?} is not a user's tenant"),
                "other_cell" => format!(
                    "tenant {tenant:?} is served by cell {}, not by cell {}",
                    short(tenant_cell),
                    short(cell_id)
                ),
                "no_universe" => format!(
                    "cell {} serves no universe directory: is the universe initialized?",
                    short(cell_id)
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
