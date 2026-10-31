//! `parosctl cell add-machine <addr>` (#216): admit an idle machine into the
//! cell the servers belong to, through `paros::client::cell`. The machine
//! is registered in the cell control journal, then told its cell with
//! `Admit`. A machine never joins a cell on its own: this call is the
//! authority. A re-run resumes, and a machine already in the cell is left
//! unchanged.
//!
//! `parosctl cell drain-machine <machine>` (#474 (journals on admitted
//! machines)): mark a registered machine draining in the cell control
//! journal. The cell coordinator then moves every journal off it.
//!
//! `parosctl cell remove-machine <machine>` (#359): retire a drained machine
//! for good. Its `node_id` is tombstoned: it never registers again.
//!
//! `parosctl cell maintenance <machine> --for <duration>` (`--zone` for a
//! failure domain, #359): the cell coordinator re-places no role of a
//! machine the window covers while it is down. `--for 0s` ends the window.
//!
//! `parosctl cell tag <cell> [k:v…]` and `parosctl cell tag-machine
//! <machine> [k:v…]` (#575 (key:value tags)): set a cell's user tags in the
//! universe directory, or a registered machine's in the cell control
//! journal, whole, under an idempotency id drawn for the run.
//!
//! `parosctl cell configure --control-plane auto|manual [--survive
//! machines=F]` (#685 (special tenants are tenants)): set how the control
//! plane's `F` is chosen. `auto` grows it with the storage machines up to
//! `F` (default 1); `manual` keeps `F`, refused when the cell cannot place
//! it.

use std::collections::BTreeMap;
use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::CompioProviders;
use paros::client::Client;
use paros::client::bootstrap::{cell_members, control_journals};
use paros::client::cell::{CellSession, Removal};
use paros::client::fleet::Step;
use paros::core::NodeId;
use paros::model::address::Address;
use paros::model::registry::MaintenanceScope;
use paros::model::registry::control_plane::{ControlPlaneMode, DEFAULT_TARGET};
use paros::wire::view::UniverseQuery;
use paros::wire::view::view_request::Query;
use serde_json::json;

use crate::cmd::tenant::{interrupted, leader_seed, refused, steps};
use crate::support::connect::Ending;
use crate::support::labels::Labels;
use crate::support::output::{Printer, note, table};
use crate::support::view::{
    Asker, coordinator_text, indent, machine_names, reply_json, selected, selector, tag, text,
    user_tags_text,
};

/// `parosctl cell`.
#[derive(Args, Debug)]
pub struct CellArgs {
    /// How long a step interrupted by a moving leader or a machine still
    /// starting is retried, in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
    #[command(subcommand)]
    pub command: CellAdminCommand,
}

#[derive(Subcommand, Debug)]
pub enum CellAdminCommand {
    /// Admit an idle machine into the cell: register it in the cell control
    /// journal, then send it `Admit`. Send it to any member (`--servers`).
    AddMachine {
        /// The idle machine's address, `HOST:PORT`: a literal or a name,
        /// resolved as it is dialed. The registry records the address the
        /// machine advertises (#257).
        addr: String,
    },
    /// Drain a registered machine: the cell coordinator moves every journal
    /// off it (#474). Send it to any member (`--servers`).
    DrainMachine {
        /// The machine's name, or its node id in full (16 hex digits).
        machine: String,
    },
    /// Remove a drained machine from the cell for good (#359): its node id
    /// is tombstoned. Drain it first, and wait until it holds no slot.
    RemoveMachine {
        /// The machine's name, or its node id in full (16 hex digits).
        machine: String,
    },
    /// Open a maintenance window over a machine or, with `--zone`, a
    /// failure domain (#359): the cell coordinator re-places no role of a
    /// machine it covers while the machine is down. `--for 0s` ends it.
    Maintenance {
        /// The machine's name or full node id, or the failure domain with
        /// `--zone`.
        target: String,
        /// The target is a failure domain (a zone), not a machine.
        #[arg(long)]
        zone: bool,
        /// How long the window lasts, from when the cell coordinator reads
        /// it: `30s`, `10m`, `2h`; `0s` ends it.
        #[arg(long = "for", value_parser = parse_window)]
        window: Duration,
    },
    /// Set a registered machine's user tags, whole: the tags given replace
    /// the ones it has, and none clears them (#575). Its `paros/` tags come
    /// from its own settings.
    TagMachine {
        /// The machine's name, or its node id in full (16 hex digits).
        machine: String,
        /// The tags, `key:value` each.
        tags: Vec<String>,
    },
    /// Set a cell's user tags in the universe directory, whole (#575).
    Tag {
        /// The cell's name.
        cell: String,
        /// The tags, `key:value` each.
        tags: Vec<String>,
    },
    /// Set the control plane's durability (#685): the system's own journals,
    /// the cell control journal, the election journal and the universe
    /// directory. `auto` grows it with the storage machines up to the
    /// `--survive` given (default `machines=1`); `manual` keeps it, and is
    /// refused when the cell has fewer than `2M+1` storage machines.
    Configure {
        /// How the control plane's `F` is chosen: `auto` or `manual`.
        #[arg(long = "control-plane", value_parser = ["auto", "manual"])]
        control_plane: String,
        /// The machine failures it survives (`machines=M`, 0 to 3): auto's
        /// target, or manual's value. Manual needs it.
        #[arg(long = "survive", value_name = "machines=M", value_parser = crate::cmd::journal::parse_survive)]
        survive_machines: Option<u32>,
    },
    /// List the universe's cells and their state (#399).
    List {
        /// Only the cells whose tags match: `key`, `key:value`, comma
        /// separated, all of them (#575).
        #[arg(short = 'l', long)]
        selector: Option<String>,
    },
    /// Show a cell: its state, its coordinator, its slots per class and
    /// its machines by standing (#399). The servers' cell; a name checks
    /// that the servers are that cell's.
    Show {
        /// The cell's name.
        cell: Option<String>,
    },
}

/// `parosctl cell …`, through `client` (the cell's `servers`, id and address
/// each). `add-machine` sends `token` with its `Admit`: a machine that pins
/// a root key checks it (#245 (Biscuit tokens)).
#[allow(clippy::too_many_lines)] // one arm per subcommand
pub async fn run(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
    labels: &Labels,
    out: &Printer,
    args: CellArgs,
    token: Vec<u8>,
) -> Ending {
    let addr = match args.command {
        CellAdminCommand::AddMachine { addr } => addr,
        CellAdminCommand::DrainMachine { machine } => {
            return drain(asker, client, servers, labels, out, &machine).await;
        }
        CellAdminCommand::RemoveMachine { machine } => {
            return remove(asker, client, servers, labels, out, &machine).await;
        }
        CellAdminCommand::Maintenance {
            target,
            zone,
            window,
        } => {
            let ctx = (asker, client, servers);
            return maintenance(ctx, labels, out, (&target, zone), window).await;
        }
        CellAdminCommand::Configure {
            control_plane,
            survive_machines,
        } => {
            return match control_plane_mode(&control_plane, survive_machines) {
                Some(mode) => configure(asker, client, servers, out, mode).await,
                None => Ending::Refused,
            };
        }
        CellAdminCommand::TagMachine { machine, tags } => {
            return tag_machine(asker, client, servers, (labels, out), &machine, &tags).await;
        }
        CellAdminCommand::Tag { cell, tags } => {
            let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
            let mut fleet =
                match crate::cmd::tenant::open_session(asker.providers, client, &ids).await {
                    Ok(fleet) => fleet,
                    Err(ending) => return ending,
                };
            return crate::cmd::tenant::set_tags(
                asker.providers,
                client,
                (out, labels),
                (&mut fleet, Duration::from_millis(args.patience_ms)),
                paros::client::fleet::TagTarget::Cell(cell.as_bytes()),
                &cell,
                &tags,
            )
            .await;
        }
        _ => unreachable!("the views are answered in crate::support::view"),
    };
    let target = match Address::parse(&addr) {
        Ok(target) => target,
        Err(error) => {
            note(&format!("bad machine address {addr:?}: {error}"));
            return Ending::Refused;
        }
    };
    let mut session = match session(asker, client, servers).await {
        Ok(session) => session.with_token(token),
        Err(ending) => return ending,
    };
    let journals = session.journals();
    let patience = Duration::from_millis(args.patience_ms);
    let run = session
        .add_machine(
            asker.providers,
            asker.rpc,
            client,
            asker.names,
            0,
            &target,
            patience,
        )
        .await;
    match run.outcome {
        Step::Done { result, .. } => {
            let outcome = if run.steps.is_empty() {
                "unchanged"
            } else {
                "admitted"
            };
            out.emit(
                || {
                    format!(
                        "{outcome} machine={target} cell={}{}",
                        labels.cell(journals.cell_id),
                        if run.steps.is_empty() {
                            String::new()
                        } else {
                            format!(" steps={}", steps(&run.steps).join(","))
                        }
                    )
                },
                || {
                    json!({
                        "outcome": outcome,
                        "node": result.0,
                        "cell": journals.cell_id,
                        "addr": target.to_string(),
                        "steps": steps(&run.steps),
                    })
                },
            );
            Ending::Success
        }
        Step::Refused(refusal) => refused(out, &refusal, labels),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// A session over the cell the servers belong to: its control journals, and
/// its founding members among the servers.
async fn session(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
) -> Result<CellSession, Ending> {
    let Some(journals) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Err(Ending::Unreachable);
    };
    // The founding members: the cell control journal's members, with the
    // addresses of the servers named among them.
    let Some(members) = cell_members(client, journals.cell).await else {
        note("no server served the cell control journal");
        return Err(Ending::Unreachable);
    };
    let founders: Vec<(NodeId, Address)> = servers
        .iter()
        .filter(|(id, _)| members.contains(id))
        .map(|(id, addr)| (NodeId(*id), addr.clone()))
        .collect();
    let members: Vec<NodeId> = members.into_iter().map(NodeId).collect();
    Ok(CellSession::new(
        journals,
        &members,
        founders,
        leader_seed(asker.providers),
        client.tunables().checkpoint_policy(),
    ))
}

/// A duration as `500ms`, `30s`, `10m` or `2h`.
fn parse_window(text: &str) -> Result<Duration, String> {
    let (digits, unit) = text
        .find(|c: char| !c.is_ascii_digit())
        .map_or((text, ""), |at| text.split_at(at));
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("{text:?}: a duration is a number and a unit (ms, s, m, h)"))?;
    match unit {
        // `--for 0` ends a window: zero needs no unit.
        "" if value == 0 => Ok(Duration::ZERO),
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_secs(value.saturating_mul(60))),
        "h" => Ok(Duration::from_secs(value.saturating_mul(3600))),
        _ => Err(format!("{text:?}: the unit is ms, s, m or h")),
    }
}

/// An open session over the servers' cell, and the registered machine
/// `machine` names in it.
async fn open_on(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
    labels: &Labels,
    machine: &str,
) -> Result<(CellSession, NodeId), Ending> {
    let mut session = session(asker, client, servers).await?;
    if let Err(stop) = session.open(client, 0).await {
        return Err(interrupted(&stop));
    }
    let Some(node) = registered_machine(&session, labels, machine) else {
        note(&format!("no registered machine is named {machine}"));
        return Err(Ending::Refused);
    };
    Ok((session, node))
}

/// `parosctl cell remove-machine <machine>` (#359): `RetireNode` for the
/// drained machine `machine` names.
async fn remove(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
    labels: &Labels,
    out: &Printer,
    machine: &str,
) -> Ending {
    let (mut session, node) = match open_on(asker, client, servers, labels, machine).await {
        Ok(opened) => opened,
        Err(ending) => return ending,
    };
    let name = labels.machine(node.0);
    match session.remove(client, 0, node).await {
        Ok(Removal::Removed | Removal::Unchanged) => {
            out.emit(
                || format!("removed machine={name}"),
                || json!({ "outcome": "removed", "node": node.0 }),
            );
            Ending::Success
        }
        Ok(Removal::Unknown) => {
            note(&format!("no registered machine is named {machine}"));
            Ending::Refused
        }
        Ok(Removal::NotDraining) => {
            note(&format!(
                "machine {name} is not draining: run `parosctl cell drain-machine` first"
            ));
            Ending::Refused
        }
        Ok(Removal::NotDrained { slots }) => {
            note(&format!(
                "machine {name} still holds {slots} slot(s): wait until the drain ends"
            ));
            Ending::Refused
        }
        Err(stop) => interrupted(&stop),
    }
}

/// `parosctl cell maintenance <machine|zone> --for <duration>` (#359).
async fn maintenance(
    (asker, client, servers): (&Asker<'_>, &Client<CompioProviders>, &[(u64, Address)]),
    labels: &Labels,
    out: &Printer,
    (target, zone): (&str, bool),
    window: Duration,
) -> Ending {
    let for_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
    let (mut session, scope, label) = if zone {
        let mut session = match session(asker, client, servers).await {
            Ok(session) => session,
            Err(ending) => return ending,
        };
        if let Err(stop) = session.open(client, 0).await {
            return interrupted(&stop);
        }
        (
            session,
            MaintenanceScope::Zone(target.to_string()),
            format!("zone={target}"),
        )
    } else {
        match open_on(asker, client, servers, labels, target).await {
            Ok((session, node)) => (
                session,
                MaintenanceScope::Machine(node),
                format!("machine={}", labels.machine(node.0)),
            ),
            Err(ending) => return ending,
        }
    };
    match session.maintenance(client, 0, scope, for_ms).await {
        Ok(true) => {
            let outcome = if for_ms == 0 { "ended" } else { "maintenance" };
            out.emit(
                || format!("{outcome} {label} for_ms={for_ms}"),
                || json!({ "outcome": outcome, "target": target, "zone": zone, "for_ms": for_ms }),
            );
            Ending::Success
        }
        Ok(false) => {
            note(&format!("no machine of the cell is named {target}"));
            Ending::Refused
        }
        Err(stop) => interrupted(&stop),
    }
}

/// The registered machine `machine` names, by name or node id in full.
fn registered_machine(session: &CellSession, labels: &Labels, machine: &str) -> Option<NodeId> {
    let registered: Vec<u64> = session.registry().nodes().map(|(id, _)| id.0).collect();
    match labels.machine_named(machine, &registered) {
        Some(id) => Some(NodeId(id)),
        None => match paros::model::name::parse_full(machine) {
            Some(id) if registered.contains(&id) => Some(NodeId(id)),
            _ => None,
        },
    }
}

/// `parosctl cell tag-machine <machine> [k:v…]` (#575 (key:value tags)).
async fn tag_machine(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
    (labels, out): (&Labels, &Printer),
    machine: &str,
    texts: &[String],
) -> Ending {
    let tags = match crate::cmd::journal::user_tags(texts) {
        Ok(tags) => tags,
        Err(ending) => return ending,
    };
    let mut session = match session(asker, client, servers).await {
        Ok(session) => session,
        Err(ending) => return ending,
    };
    if let Err(stop) = session.open(client, 0).await {
        return interrupted(&stop);
    }
    let Some(node) = registered_machine(&session, labels, machine) else {
        note(&format!("no registered machine is named {machine}"));
        return Ending::Refused;
    };
    let request = crate::cmd::tenant::nonzero(asker.providers);
    match session
        .set_machine_tags(client, 0, (node, request), &tags)
        .await
    {
        Ok(true) => {
            out.emit(
                || {
                    format!(
                        "tagged machine={} tags={}",
                        labels.machine(node.0),
                        crate::cmd::tenant::tags_label(&tags)
                    )
                },
                || json!({ "outcome": "tagged", "node": node.0, "tags": crate::cmd::tenant::tags_label(&tags) }),
            );
            Ending::Success
        }
        Ok(false) => {
            note(&format!(
                "refused: the registry does not hold {machine}, or it is retired"
            ));
            Ending::Refused
        }
        Err(stop) => interrupted(&stop),
    }
}

/// The mode `--control-plane` and `--survive` name (#685): auto takes
/// `--survive` as its target (default 1), manual needs it.
fn control_plane_mode(
    control_plane: &str,
    survive_machines: Option<u32>,
) -> Option<ControlPlaneMode> {
    match (control_plane, survive_machines) {
        ("auto", target) => Some(ControlPlaneMode::Auto {
            target: target.unwrap_or(DEFAULT_TARGET),
        }),
        (_, Some(failures)) => Some(ControlPlaneMode::Manual { failures }),
        (_, None) => {
            note("--control-plane manual needs --survive machines=M");
            None
        }
    }
}

/// `parosctl cell configure --control-plane` (#685): `SetControlPlane`
/// under an idempotency id drawn for the run.
async fn configure(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
    out: &Printer,
    mode: ControlPlaneMode,
) -> Ending {
    let mut session = match session(asker, client, servers).await {
        Ok(session) => session,
        Err(ending) => return ending,
    };
    let request = crate::cmd::tenant::nonzero(asker.providers);
    match session
        .configure_control_plane(client, 0, request, mode)
        .await
    {
        Ok(true) => {
            let plane = session.registry().control_plane(1);
            out.emit(
                || format!("configured control-plane={mode} storage={}", plane.storage),
                || {
                    json!({
                        "outcome": "configured",
                        "mode": mode.as_str(),
                        "machines": mode.failures(),
                        "storage": plane.storage,
                    })
                },
            );
            Ending::Success
        }
        Ok(false) => {
            let storage = session.registry().storage_machines();
            note(&format!(
                "refused: control-plane={mode} needs {} storage machines, the cell has {storage}",
                paros::model::registry::control_plane::acceptors(mode.failures())
            ));
            Ending::Refused
        }
        Err(stop) => interrupted(&stop),
    }
}

/// `parosctl cell drain-machine <machine>` (#474): `DrainNode` for the
/// registered machine `machine` names, unless it is draining already.
async fn drain(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[(u64, Address)],
    labels: &Labels,
    out: &Printer,
    machine: &str,
) -> Ending {
    let (mut session, node) = match open_on(asker, client, servers, labels, machine).await {
        Ok(opened) => opened,
        Err(ending) => return ending,
    };
    match session.drain(client, 0, node).await {
        Ok(wrote) => {
            let outcome = if wrote { "draining" } else { "unchanged" };
            out.emit(
                || format!("{outcome} machine={}", labels.machine(node.0)),
                || json!({ "outcome": outcome, "node": node.0 }),
            );
            Ending::Success
        }
        Err(stop) => interrupted(&stop),
    }
}

// `cell list` and `cell show`: views of the cell (#399).

/// `parosctl cell list`: the universe's cells.
pub async fn cell_list(asker: &Asker<'_>, out: &Printer, chosen: Option<&str>) -> Ending {
    let chosen = match selector(chosen) {
        Ok(chosen) => chosen,
        Err(ending) => return ending,
    };
    let mut reply = match asker.ask(Query::Universe(UniverseQuery {})).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    reply.cells.retain(|c| selected(&chosen, &c.tags));
    let rows: Vec<[String; 5]> = reply
        .cells
        .iter()
        .map(|c| {
            [
                text(&c.name),
                tag(&c.tags, paros::model::tags::reserved::KIND),
                c.state.clone(),
                c.tenants.to_string(),
                user_tags_text(&c.tags),
            ]
        })
        .collect();
    out.emit(
        || {
            format!(
                "universe {}\n{}",
                text(&reply.universe_name),
                table(["CELL", "KIND", "STATE", "TENANTS", "TAGS"], &rows)
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

/// `parosctl cell show [<cell>]`: the cell's state, its coordinator, its
/// slots per class and its machines by standing.
pub async fn cell_show(asker: &Asker<'_>, out: &Printer, cell: Option<&str>) -> Ending {
    let reply = match asker.cell(cell).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let names = machine_names(&reply);
    let mut slots: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    let mut standing: BTreeMap<&str, usize> = BTreeMap::new();
    for m in &reply.machines {
        if m.standing != "retired" {
            let class = paros::model::tags::value_of(&m.tags, paros::model::tags::reserved::CLASS)
                .unwrap_or("-");
            let entry = slots.entry(class).or_default();
            entry.0 += m.booked;
            entry.1 += m.capacity;
        }
        *standing.entry(m.standing.as_str()).or_default() += 1;
    }
    let down = reply.machines.iter().filter(|m| !m.up).count();
    let slow = reply.machines.iter().filter(|m| m.up && m.slow).count();
    let users = reply.tenants.iter().filter(|t| t.kind == "users").count();
    let slot_rows: Vec<[String; 4]> = slots
        .iter()
        .map(|(class, (booked, total))| {
            [
                (*class).to_string(),
                booked.to_string(),
                total.saturating_sub(*booked).to_string(),
                total.to_string(),
            ]
        })
        .collect();
    out.emit(
        || {
            format!(
                "cell:         {}\nuniverse:     {}\nstate:        {}\ncoordinator:  {}\nmachines:     {} ({}; {} down, {} slow)\ntenants:      {users}\nslots:\n{}",
                text(&reply.cell_name),
                text(&reply.universe_name),
                if reply.cell_state.is_empty() { "-" } else { &reply.cell_state },
                coordinator_text(&reply, &names),
                reply.machines.len(),
                standing
                    .iter()
                    .map(|(s, n)| format!("{n} {s}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                down,
                slow,
                indent(&table(["CLASS", "BOOKED", "FREE", "TOTAL"], &slot_rows)),
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_takes_a_unit_except_zero() {
        assert_eq!(parse_window("0"), Ok(Duration::ZERO));
        assert_eq!(parse_window("30m"), Ok(Duration::from_mins(30)));
        assert!(parse_window("5").is_err());
    }

    #[test]
    fn a_control_plane_mode_is_auto_by_default_and_manual_needs_its_value() {
        assert_eq!(
            control_plane_mode("auto", None),
            Some(ControlPlaneMode::Auto {
                target: DEFAULT_TARGET
            })
        );
        assert_eq!(
            control_plane_mode("auto", Some(2)),
            Some(ControlPlaneMode::Auto { target: 2 })
        );
        assert_eq!(
            control_plane_mode("manual", Some(0)),
            Some(ControlPlaneMode::Manual { failures: 0 })
        );
        assert_eq!(control_plane_mode("manual", None), None);
    }
}
