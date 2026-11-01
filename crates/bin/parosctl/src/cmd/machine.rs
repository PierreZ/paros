//! `parosctl machine list|show` (#399): the cell's machines and their
//! busyness (#424).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use clap::{Args, Subcommand};
use paros::client::load::LoadOutcome;
use paros::wire::view::{self as wire};
use serde_json::{Value, json};

use crate::cmd::roles::roles;
use crate::support::connect::Ending;
use crate::support::output::{Printer, table};
use crate::support::view::{
    Asker, find_machine, indent, machine_json, machine_names, machine_state, reply_json, selected,
    selector, tag, text, user_tags_text,
};

/// `parosctl machine`.
#[derive(Args, Debug)]
pub struct MachineArgs {
    #[command(subcommand)]
    command: MachineCommand,
}

#[derive(Subcommand, Debug)]
enum MachineCommand {
    /// List the cell's machines: name, address, class, slots booked of
    /// total, zone, standing, up or down, load and user tags.
    List {
        /// The cell: refused unless the servers are its machines.
        #[arg(long)]
        cell: Option<String>,
        /// Only the machines whose tags match: `key`, `key:value`, comma
        /// separated, all of them (#575 (key:value tags)).
        #[arg(short = 'l', long)]
        selector: Option<String>,
    },
    /// Show one machine: its details, its bookings and the journals it
    /// serves.
    Show {
        /// The machine's name.
        machine: String,
    },
}

pub async fn machine(asker: &Asker<'_>, out: &Printer, args: MachineArgs) -> Ending {
    match args.command {
        MachineCommand::List { cell, selector } => {
            machine_list(asker, out, (cell.as_deref(), selector.as_deref())).await
        }
        MachineCommand::Show { machine } => {
            let reply = match asker.cell(None).await {
                Ok(reply) => reply,
                Err(ending) => return ending,
            };
            let m = match find_machine(&reply, &machine) {
                Ok(m) => m,
                Err(ending) => return ending,
            };
            let load = asker
                .loads(&wire::ViewReply {
                    machines: vec![m.clone()],
                    ..wire::ViewReply::default()
                })
                .await
                .remove(&m.node_id);
            let roles: Vec<[String; 4]> = roles(&reply)
                .into_iter()
                .filter(|r| r.holder == m.node_id)
                .map(|r| r.row(&reply))
                .collect();
            out.emit(
                || {
                    let mut text = format!(
                        "name:            {}\naddress:         {}\nroles:           {}\nslots:           {} booked of {}\nzone:            {}\nrack:            {}\ntags:            {}\nstanding:        {}\nstate:           {}\nfounding member: {}\ncell:            {}",
                        machine_names(&reply)[&m.node_id],
                        m.addr,
                        tag(&m.tags, paros::model::tags::reserved::ROLES),
                        m.booked,
                        m.capacity,
                        tag(&m.tags, paros::model::tags::reserved::ZONE),
                        tag(&m.tags, paros::model::tags::reserved::RACK),
                        user_tags_text(&m.tags),
                        m.standing,
                        machine_state(m),
                        if m.founder { "yes" } else { "no" },
                        self::text(&reply.cell_name),
                    );
                    let _ = write!(text, "\n{}", load_block(load.as_ref()));
                    let _ = write!(
                        text,
                        "\nroles:\n{}",
                        indent(&table(["ROLE", "HOLDER", "TENANT", "JOURNAL"], &roles))
                    );
                    text
                },
                || {
                    let mut doc = reply_json(&reply);
                    doc["machine"] = machine_json(m);
                    doc["machine"]["load"] = load_json(load.as_ref());
                    doc
                },
            );
            Ending::Success
        }
    }
}

/// `parosctl machine list`: the cell's machines, with how busy each is.
async fn machine_list(
    asker: &Asker<'_>,
    out: &Printer,
    (cell, chosen): (Option<&str>, Option<&str>),
) -> Ending {
    let chosen = match selector(chosen) {
        Ok(chosen) => chosen,
        Err(ending) => return ending,
    };
    let mut reply = match asker.cell(cell).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    reply.machines.retain(|m| selected(&chosen, &m.tags));
    let loads = asker.loads(&reply).await;
    let rows: Vec<[String; 11]> = reply
        .machines
        .iter()
        .map(|m| {
            let [cpu, machine, disk] = load_columns(loads.get(&m.node_id));
            [
                machine_names(&reply)[&m.node_id].clone(),
                m.addr.clone(),
                tag(&m.tags, paros::model::tags::reserved::ROLES),
                format!("{}/{}", m.booked, m.capacity),
                tag(&m.tags, paros::model::tags::reserved::ZONE),
                m.standing.clone(),
                machine_state(m),
                cpu,
                machine,
                disk,
                user_tags_text(&m.tags),
            ]
        })
        .collect();
    out.emit(
        || {
            table(
                [
                    "NAME", "ADDRESS", "ROLES", "SLOTS", "ZONE", "STANDING", "STATE", "CPU",
                    "MACHINE", "DISK", "TAGS",
                ],
                &rows,
            )
        },
        || {
            let mut doc = reply_json(&reply);
            attach_loads(&mut doc, &reply, &loads);
            doc
        },
    );
    Ending::Success
}

/// A share as a whole percent.
fn percent(share: f64) -> String {
    format!("{:.0}%", share * 100.0)
}

/// The `CPU`, `MACHINE` and `DISK` columns of one machine: `no metrics`
/// when it did not answer, `-` for a value it does not have.
fn load_columns(load: Option<&LoadOutcome>) -> [String; 3] {
    let Some(LoadOutcome::Answered(ack)) = load else {
        return ["no metrics".to_string(), String::new(), String::new()];
    };
    if !ack.windowed {
        return ["-".to_string(), "-".to_string(), "-".to_string()];
    }
    [
        percent(ack.cpu_cores),
        percent(ack.machine_cpu),
        ack.disk
            .as_ref()
            .map_or_else(|| "-".to_string(), |disk| percent(disk.busy)),
    ]
}

/// A duration in seconds, one decimal.
fn seconds(nanos: u64) -> String {
    format!("{:.1} s", Duration::from_nanos(nanos).as_secs_f64())
}

/// A rate in bytes per second, in MB/s.
fn megabytes(bps: f64) -> String {
    format!("{:.1} MB/s", bps / 1_000_000.0)
}

/// `machine show`'s `load` block.
fn load_block(load: Option<&LoadOutcome>) -> String {
    let Some(LoadOutcome::Answered(ack)) = load else {
        return "load:            no metrics".to_string();
    };
    if !ack.windowed {
        return "load:            - (no full window yet)".to_string();
    }
    let mut text = format!(
        "load ({} window, ended {} ago)\n  cpu          {:.2} cores (of {})\n  machine cpu  {}",
        seconds(ack.elapsed_ns),
        seconds(ack.window_age_ns),
        ack.cpu_cores,
        ack.cores,
        percent(ack.machine_cpu),
    );
    if ack.has_run_loop {
        let _ = write!(text, "\n  run loop     {} busy", percent(ack.run_loop_busy));
    }
    match &ack.disk {
        Some(disk) => {
            let _ = write!(
                text,
                "\n  disk         {} busy ({}), queue {}\n  disk reads   {:.0}/s  {}\n  disk writes  {:.0}/s  {}",
                percent(disk.busy),
                disk.device,
                disk.queue_depth,
                disk.reads_hz,
                megabytes(disk.read_bps),
                disk.writes_hz,
                megabytes(disk.write_bps),
            );
        }
        None => text.push_str("\n  disk         - (device not known)"),
    }
    text
}

/// One machine's load as JSON, with FDB's key names where they exist;
/// `null` when it did not answer.
fn load_json(load: Option<&LoadOutcome>) -> Value {
    let Some(LoadOutcome::Answered(ack)) = load else {
        return Value::Null;
    };
    if !ack.windowed {
        return json!({ "windowed": false });
    }
    let secs = |nanos: u64| Duration::from_nanos(nanos).as_secs_f64();
    json!({
        "windowed": true,
        "elapsed_seconds": secs(ack.elapsed_ns),
        "window_age_seconds": secs(ack.window_age_ns),
        "cpu": { "usage_cores": ack.cpu_cores, "cores": ack.cores },
        "machine": { "cpu": { "logical_core_utilization": ack.machine_cpu } },
        "run_loop_busy": ack.has_run_loop.then_some(ack.run_loop_busy),
        "disk": ack.disk.as_ref().map(|disk| json!({
            "device": disk.device,
            "busy": disk.busy,
            "queue_depth": disk.queue_depth,
            "reads": { "hz": disk.reads_hz },
            "writes": { "hz": disk.writes_hz },
            "read_bytes": { "hz": disk.read_bps },
            "written_bytes": { "hz": disk.write_bps },
        })),
    })
}

/// Put each machine's load into the `machines` of a JSON answer.
fn attach_loads(doc: &mut Value, reply: &wire::ViewReply, loads: &BTreeMap<u64, LoadOutcome>) {
    let Some(machines) = doc.get_mut("machines").and_then(Value::as_array_mut) else {
        return;
    };
    for (entry, machine) in machines.iter_mut().zip(&reply.machines) {
        entry["load"] = load_json(loads.get(&machine.node_id));
    }
}
