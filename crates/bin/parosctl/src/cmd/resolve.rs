//! `parosctl resolve <tenant>` (#216): ask the entry endpoint (`--servers`)
//! which references serve a tenant, as a client does first
//! (`paros::client::resolve`). Any machine of a cell answers it from its
//! folds of the universe directory and the registry.
//!
//! With a token (`--token`, `--token-file`, `PAROS_TOKEN`), the servers are
//! the universe's resolvers (#233 (resolver)): a resolver checks the token
//! and names the tenant's cell, then a machine of that cell answers the
//! rest. The answer names the cell the resolver sent the client to.

use std::time::Duration;

use clap::Args;
use moonpool_core::CompioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::resolve::{Redirection, Resolution, resolve, resolve_through};
use paros::model::address::{Address, Names};
use serde_json::json;

use crate::support::connect::Ending;
use crate::support::output::{Printer, note, record_text, short};

/// `parosctl resolve`.
#[derive(Args, Debug)]
pub struct ResolveArgs {
    /// The tenant's name.
    tenant: String,
    /// The zone the client stands in (#673 (zoned resolve)): the machines
    /// and frontends of this zone are listed first.
    #[arg(long, default_value = "")]
    zone: String,
    /// The token the resolvers check (#233 (resolver)), from the global
    /// `--token`: `None` asks the machines directly.
    #[arg(skip)]
    pub token: Option<Vec<u8>>,
}

/// Resolve `tenant` at `entry`: through its resolvers with `token`, else
/// at its machines.
async fn ask(
    providers: &CompioProviders,
    rpc: &RpcHandle<CompioProviders>,
    names: &Names,
    (entry, timeout): (&[Address], Duration),
    tenant: &str,
    token: Option<Vec<u8>>,
) -> Resolution {
    let Some(token) = token else {
        return resolve(providers, rpc, names, entry, tenant.as_bytes(), timeout).await;
    };
    let (redirection, resolution) = resolve_through(
        providers,
        rpc,
        names,
        entry,
        (tenant.as_bytes(), b""),
        &token,
        timeout,
    )
    .await;
    if let Redirection::Redirected(redirect) = &redirection {
        note(&format!(
            "a resolver of cell {} sends tenant {tenant:?} to cell {}",
            short(redirect.resolver_cell),
            named(&redirect.cell_name, redirect.cell_id)
        ));
    }
    resolution
}

/// `parosctl resolve <tenant>` through the entry endpoint `entry`: through
/// its resolvers when the caller presents a token.
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
    let resolution = ask(providers, rpc, names, (entry, timeout), &tenant, args.token).await;
    match resolution {
        Resolution::Resolved(resolved) => {
            let resolved = resolved.prefer_zone(&args.zone);
            let zone =
                |id: &paros::core::NodeId| resolved.zones.get(id).cloned().unwrap_or_default();
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
                            .map(|(id, addr)| {
                                json!({"node": id.0, "addr": addr.to_string(), "zone": zone(id)})
                            })
                            .collect::<Vec<_>>(),
                        "frontends": resolved
                            .frontends
                            .iter()
                            .map(|(id, addr)| {
                                json!({"node": id.0, "addr": addr.to_string(), "zone": zone(id)})
                            })
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
                "invalid_token" => "the resolver does not trust the token".to_string(),
                "expired" => "the token expired".to_string(),
                "forbidden" => format!("the token does not allow resolving tenant {tenant:?}"),
                "no_endpoint" => {
                    format!(
                        "the universe directory records no entry endpoint for tenant {tenant:?}'s cell"
                    )
                }
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
