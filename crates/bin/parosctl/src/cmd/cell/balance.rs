//! `parosctl cell balance [--pause|--resume]` (#687 (rebalance a cell)): how
//! the cell's balance stands, from the `CellBalance` view, or a pause or a
//! resume of the balancer, through the cell control journal.
//!
//! The output says only what helps an operator act: a balanced cell is one
//! line; anything else names what holds the balance and, for a gap, the
//! machines of its zone. Everything else is in `--json`.

use moonpool_core::CompioProviders;
use paros::client::Client;
use paros::model::address::Address;
use paros::wire::view::view_request::Query;
use paros::wire::view::{BalanceMoveView, BalanceQuery, CellBalance};
use serde_json::{Value, json};

use crate::cmd::tenant::interrupted;
use crate::support::connect::Ending;
use crate::support::output::{Printer, record_text, short, table};
use crate::support::view::{Asker, indent};

/// `parosctl cell balance`: the view.
pub async fn show(asker: &Asker<'_>, out: &Printer) -> Ending {
    let reply = match asker.ask(Query::Balance(BalanceQuery {})).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let balance = reply.balance.unwrap_or_default();
    out.emit(|| balance_text(&balance), || balance_json(&balance));
    Ending::Success
}

/// `parosctl cell balance --pause` or `--resume`: `SetBalance` under an
/// idempotency id drawn for the run.
pub async fn set(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
    out: &Printer,
    pause: bool,
) -> Ending {
    let mut session = match super::session(asker, client, servers).await {
        Ok(session) => session,
        Err(ending) => return ending,
    };
    let request = crate::cmd::tenant::nonzero(asker.providers);
    match session.set_balance(client, 0, request, pause).await {
        Ok(paused) => {
            let moving = session.registry().balance().moving().is_some();
            let outcome = if paused { "paused" } else { "resumed" };
            out.emit(
                || {
                    if paused && moving {
                        "paused: no move starts; the move in flight still ends".to_string()
                    } else if paused {
                        "paused: no move starts".to_string()
                    } else {
                        "resumed".to_string()
                    }
                },
                || json!({ "outcome": outcome, "paused": paused, "moving": moving }),
            );
            Ending::Success
        }
        Err(stop) => interrupted(&stop),
    }
}

/// A machine's name, its short id when it has none.
fn machine(name: &str, id: u64) -> String {
    if name.is_empty() {
        short(id)
    } else {
        name.to_string()
    }
}

/// A journal as `tenant/journal`, by name when it has one.
fn journal(m: &BalanceMoveView) -> String {
    let part = |name: &[u8], id: u64| {
        if name.is_empty() {
            short(id)
        } else {
            record_text(name)
        }
    };
    format!(
        "{}/{}",
        part(&m.tenant_name, m.tenant),
        part(&m.journal_name, m.journal)
    )
}

/// One move, as a sentence.
fn move_text(m: &BalanceMoveView) -> String {
    format!(
        "journal {} from {} to {} ({})",
        journal(m),
        machine(&m.from_name, m.from),
        machine(&m.to_name, m.to),
        m.reason
    )
}

/// The view as text: what holds the balance, in one line when nothing does.
pub fn balance_text(b: &CellBalance) -> String {
    let mut lines = vec![match b.state.as_str() {
        "balanced" => format!("balanced: every zone is within {} role(s)", b.spread),
        "moving" => match &b.moving {
            Some(m) => format!("moving {}", move_text(m)),
            None => "moving".to_string(),
        },
        "paused" => "paused: no move starts (parosctl cell balance --resume)".to_string(),
        "repairing" => {
            let name = machine(&b.repair_machine_name, b.repair_machine);
            let why = match b.repair.as_str() {
                "draining" => format!("machine {name} is draining"),
                "stranded" => format!("machine {name} leaves and still holds roles"),
                _ => format!("machine {name} is down or slow"),
            };
            format!("repairing: {why}; its moves come first")
        }
        "unbalanced" | "stuck" => match &b.gap {
            Some(g) => {
                let gap = format!(
                    "{} hosts {} role(s), {} hosts {} (spread {})",
                    machine(&g.from_name, g.from),
                    g.from_roles,
                    machine(&g.to_name, g.to),
                    g.to_roles,
                    b.spread
                );
                if b.state == "stuck" {
                    format!(
                        "stuck in zone {}: {gap}; every journal on the first also has a role on the second",
                        g.zone
                    )
                } else {
                    format!("unbalanced in zone {}: {gap}; a move starts soon", g.zone)
                }
            }
            None => b.state.clone(),
        },
        other => other.to_string(),
    }];
    if b.paused && b.state == "moving" {
        lines.push("paused: no other move starts".to_string());
    }
    // The machines of the gap's zone: what an operator weighs before adding
    // or draining one.
    if let Some(g) = &b.gap {
        let rows: Vec<[String; 2]> = b
            .machines
            .iter()
            .filter(|m| m.zone == g.zone)
            .map(|m| [machine(&m.name, m.node_id), m.roles.to_string()])
            .collect();
        lines.push(indent(&table(["MACHINE", "ROLES"], &rows)));
    }
    // A move that gave up is worth a look; a move that ended is not.
    if let Some(last) = b.ended.last().filter(|m| m.outcome == "abandoned") {
        lines.push(format!("last move abandoned: {}", move_text(last)));
    }
    lines.join("\n")
}

fn move_json(m: &BalanceMoveView) -> Value {
    json!({
        "tenant": m.tenant,
        "tenant_name": record_text(&m.tenant_name),
        "journal": m.journal,
        "journal_name": record_text(&m.journal_name),
        "from": m.from,
        "from_name": m.from_name,
        "to": m.to,
        "to_name": m.to_name,
        "reason": m.reason,
        "started": m.started,
        "ended": m.ended,
        "outcome": m.outcome,
    })
}

/// The view as one JSON document: every field, ids and names both.
pub fn balance_json(b: &CellBalance) -> Value {
    json!({
        "state": b.state,
        "paused": b.paused,
        "spread": b.spread,
        "moving": b.moving.as_ref().map(move_json),
        "repair": (!b.repair.is_empty()).then(|| json!({
            "why": b.repair,
            "machine": b.repair_machine,
            "machine_name": b.repair_machine_name,
        })),
        "gap": b.gap.as_ref().map(|g| json!({
            "zone": g.zone,
            "reason": g.reason,
            "from": g.from,
            "from_name": g.from_name,
            "from_roles": g.from_roles,
            "to": g.to,
            "to_name": g.to_name,
            "to_roles": g.to_roles,
        })),
        "ended": b.ended.iter().map(move_json).collect::<Vec<_>>(),
        "machines": b.machines.iter().map(|m| json!({
            "node_id": m.node_id,
            "name": m.name,
            "zone": m.zone,
            "roles": m.roles,
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros::wire::view::{BalanceGap, BalanceMachine};

    fn moved(outcome: &str) -> BalanceMoveView {
        BalanceMoveView {
            tenant: 1,
            journal: 2,
            tenant_name: b"acme".to_vec(),
            journal_name: b"orders".to_vec(),
            from: 3,
            from_name: "m1".into(),
            to: 4,
            to_name: "m2".into(),
            reason: "roles".into(),
            started: 9,
            ended: 0,
            outcome: outcome.into(),
        }
    }

    #[test]
    fn a_balanced_cell_is_one_line() {
        let b = CellBalance {
            state: "balanced".into(),
            spread: 1,
            ended: vec![moved("done")],
            machines: vec![BalanceMachine {
                node_id: 3,
                name: "m1".into(),
                zone: "z".into(),
                roles: 2,
            }],
            ..CellBalance::default()
        };
        assert_eq!(balance_text(&b), "balanced: every zone is within 1 role(s)");
    }

    #[test]
    fn a_gap_names_its_machines_and_its_zone_table() {
        let b = CellBalance {
            state: "unbalanced".into(),
            spread: 1,
            gap: Some(BalanceGap {
                zone: "z".into(),
                reason: "roles".into(),
                from: 3,
                from_name: "m1".into(),
                from_roles: 5,
                to: 4,
                to_name: "m2".into(),
                to_roles: 1,
            }),
            machines: vec![
                BalanceMachine {
                    node_id: 3,
                    name: "m1".into(),
                    zone: "z".into(),
                    roles: 5,
                },
                BalanceMachine {
                    node_id: 9,
                    name: "far".into(),
                    zone: "y".into(),
                    roles: 0,
                },
            ],
            ..CellBalance::default()
        };
        let text = balance_text(&b);
        assert!(
            text.starts_with("unbalanced in zone z: m1 hosts 5 role(s), m2 hosts 1 (spread 1)")
        );
        assert!(text.contains("m1"));
        assert!(
            !text.contains("far"),
            "another zone's machines say nothing here"
        );
    }

    #[test]
    fn a_move_in_flight_and_an_abandoned_move_are_named() {
        let b = CellBalance {
            state: "moving".into(),
            paused: true,
            moving: Some(moved("")),
            ended: vec![moved("abandoned")],
            ..CellBalance::default()
        };
        let text = balance_text(&b);
        assert!(text.starts_with("moving journal acme/orders from m1 to m2 (roles)"));
        assert!(text.contains("paused: no other move starts"));
        assert!(text.contains("last move abandoned: journal acme/orders"));
    }

    #[test]
    fn a_repair_says_which_machine_holds_the_balance() {
        let b = CellBalance {
            state: "repairing".into(),
            repair: "draining".into(),
            repair_machine: 3,
            repair_machine_name: "m1".into(),
            ..CellBalance::default()
        };
        assert_eq!(
            balance_text(&b),
            "repairing: machine m1 is draining; its moves come first"
        );
    }
}
