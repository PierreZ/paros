//! The machine record (#196): `<data-dir>/machine`, a machine's identity and
//! its cell, kept beside the stores and rewritten whole and atomically.
//!
//! A machine is **formatted** on its first start, on an empty data
//! directory: its `node_id` is minted then, at random (#225), so a wiped
//! volume comes back as a new machine and "a wiped identity never rejoins"
//! holds by construction. A data directory that holds stores but no record
//! lost its identity, and is refused as amnesia.
//!
//! The rendezvous join list is stored too and re-read on every boot (the
//! environment's, when given, wins and is recorded). Once `init` forms the
//! cell, the record holds its plan: first as **pending** on the seed that
//! runs `init` (a re-run resumes it, never redraws it), then as **formed**
//! — the commit point, written after every journal store of the plan is
//! formatted and the provisioning record names them ([`DirLedger::form`]).
//!
//! ```text
//! node_id 6150928431937019931
//! class storage
//! capacity 1
//! failure_domain zone-a
//! rendezvous seeds:4500
//! plan formed 912873
//! member 6150928431937019931 10.0.0.2:4500
//! journal 2/1
//! journal 256/256
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use paros::machine::{CellLedger, CellPlan, Class};
use paros::{Config, JournalKey, JournalStorage, JournalStoreConfig, NodeId, QuorumSystem};

use crate::record::{Record, parse_key, write_atomically};
use crate::stores::{journal_dir, path_str};

/// The record's file name under the data directory.
const FILE: &str = "machine";

/// The role the provisioning record names for a machine's stores.
pub const ROLE: &str = "machine";

/// Where a machine's plan stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanState {
    /// Recorded by the seed running `init`, before any seed formed it.
    Pending,
    /// Formed here: this machine serves the plan's journals.
    Formed,
}

/// What the record holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineRecord {
    /// The machine's identity, minted at format.
    pub node_id: NodeId,
    /// Its class, fixed at format.
    pub class: Class,
    /// Its capacity.
    pub capacity: u64,
    /// Its failure domain.
    pub failure_domain: String,
    /// Its rendezvous join list, as configured (unresolved).
    pub rendezvous: String,
    /// Its cell's plan, and where it stands.
    pub plan: Option<(PlanState, CellPlan)>,
}

impl MachineRecord {
    /// Read the record under `data_dir`; `None` when there is none.
    ///
    /// # Errors
    ///
    /// The file exists but cannot be read, or is not a record.
    pub fn read(data_dir: &Path) -> io::Result<Option<Self>> {
        let text = match fs::read_to_string(data_dir.join(FILE)) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        Self::parse(&text)
            .map(Some)
            .map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))
    }

    /// Write the record under `data_dir`, durably.
    ///
    /// # Errors
    ///
    /// Any filesystem failure.
    pub fn write(&self, data_dir: &Path) -> io::Result<()> {
        write_atomically(data_dir, FILE, &self.render())
    }

    /// The formed plan, if this machine has a cell.
    #[must_use]
    pub fn formed(&self) -> Option<&CellPlan> {
        match &self.plan {
            Some((PlanState::Formed, plan)) => Some(plan),
            _ => None,
        }
    }

    fn render(&self) -> String {
        let mut text = format!(
            "node_id {}\nclass {}\ncapacity {}\nfailure_domain {}\nrendezvous {}\n",
            self.node_id.0,
            self.class.as_str(),
            self.capacity,
            self.failure_domain,
            self.rendezvous
        );
        if let Some((state, plan)) = &self.plan {
            let state = match state {
                PlanState::Pending => "pending",
                PlanState::Formed => "formed",
            };
            let _ = writeln!(text, "plan {state} {}", plan.cell_id);
            for (id, addr) in &plan.members {
                let _ = writeln!(text, "member {} {addr}", id.0);
            }
            for journal in &plan.journals {
                let _ = writeln!(text, "journal {journal}");
            }
        }
        text
    }

    fn parse(text: &str) -> Result<Self, String> {
        let mut fields: BTreeMap<&str, &str> = BTreeMap::new();
        let mut plan: Option<(PlanState, u64)> = None;
        let mut members = Vec::new();
        let mut journals = Vec::new();
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let (key, value) = line.split_once(' ').unwrap_or((line, ""));
            match key {
                "node_id" | "class" | "capacity" | "failure_domain" | "rendezvous" => {
                    fields.insert(key, value);
                }
                "plan" => {
                    let (state, cell) = value
                        .split_once(' ')
                        .ok_or_else(|| format!("bad plan line {line:?}"))?;
                    let state = match state {
                        "pending" => PlanState::Pending,
                        "formed" => PlanState::Formed,
                        _ => return Err(format!("bad plan state {state:?}")),
                    };
                    let cell = cell.parse().map_err(|e| format!("bad cell id: {e}"))?;
                    plan = Some((state, cell));
                }
                "member" => {
                    let (id, addr) = value
                        .split_once(' ')
                        .ok_or_else(|| format!("bad member line {line:?}"))?;
                    members.push((
                        NodeId(id.parse().map_err(|e| format!("bad member id: {e}"))?),
                        addr.parse::<SocketAddr>()
                            .map_err(|e| format!("bad member address: {e}"))?,
                    ));
                }
                "journal" => {
                    journals
                        .push(parse_key(value).ok_or_else(|| format!("bad journal {value:?}"))?);
                }
                _ => return Err(format!("unknown machine record key {key:?}")),
            }
        }
        let field = |name: &str| {
            fields
                .get(name)
                .copied()
                .ok_or_else(|| format!("the machine record names no {name}"))
        };
        let plan = match plan {
            Some((state, cell_id)) => {
                let plan = CellPlan {
                    cell_id,
                    members,
                    journals,
                };
                plan.check()?;
                Some((state, plan))
            }
            None => None,
        };
        Ok(Self {
            node_id: NodeId(
                field("node_id")?
                    .parse()
                    .map_err(|e| format!("bad node id: {e}"))?,
            ),
            class: field("class")?.parse()?,
            capacity: field("capacity")?
                .parse()
                .map_err(|e| format!("bad capacity: {e}"))?,
            failure_domain: field("failure_domain")?.to_string(),
            rendezvous: field("rendezvous")?.to_string(),
            plan,
        })
    }
}

/// The core configuration of `journal` on member `node_id` of `plan`: plain
/// Multi-Paxos over every member, under a majority (a cell's bootstrap is
/// the matchmaker-free exception, `docs/architecture.md` §3.1).
#[must_use]
pub fn journal_config(plan: &CellPlan, node_id: NodeId, journal: JournalKey) -> Config {
    let members: Vec<NodeId> = plan.members.iter().map(|(id, _)| *id).collect();
    Config {
        journal,
        id: node_id,
        peers: members.clone(),
        nodes: members,
        quorum_system: QuorumSystem::Majority,
        ..Config::default()
    }
}

/// The [`CellLedger`] of a machine's data directory.
pub struct DirLedger {
    /// The data directory.
    pub data_dir: PathBuf,
    /// The store layout.
    pub layout: JournalStoreConfig,
    /// The machine's record, as last written.
    pub record: MachineRecord,
}

impl DirLedger {
    fn commit(&mut self, plan: Option<(PlanState, CellPlan)>) -> Result<(), String> {
        let mut record = self.record.clone();
        record.plan = plan;
        record
            .write(&self.data_dir)
            .map_err(|e| format!("machine record: {e}"))?;
        self.record = record;
        Ok(())
    }
}

impl CellLedger for DirLedger {
    fn pending(&self) -> Option<CellPlan> {
        match &self.record.plan {
            Some((PlanState::Pending, plan)) => Some(plan.clone()),
            _ => None,
        }
    }

    fn record_pending(&mut self, plan: &CellPlan) -> Result<(), String> {
        self.commit(Some((PlanState::Pending, plan.clone())))
    }

    async fn form(&mut self, plan: &CellPlan) -> Result<(), String> {
        if self.record.formed() == Some(plan) {
            return Ok(());
        }
        let provider = moonpool_core::TokioStorageProvider::new();
        let node_id = self.record.node_id;
        for &journal in &plan.journals {
            let mut store = JournalStorage::new(
                provider.clone(),
                path_str(&journal_dir(&self.data_dir, journal)),
                journal_config(plan, node_id, journal),
                self.layout,
            );
            paros::provision_store(&mut store)
                .await
                .map_err(|e| format!("journal {journal}: {e}"))?;
        }
        Record {
            role: ROLE.into(),
            id: node_id.0,
            journals: plan.journals.iter().copied().collect::<BTreeSet<_>>(),
        }
        .write(&self.data_dir)
        .map_err(|e| format!("provisioning record: {e}"))?;
        self.commit(Some((PlanState::Formed, plan.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(plan: Option<(PlanState, CellPlan)>) -> MachineRecord {
        MachineRecord {
            node_id: NodeId(u64::MAX - 3),
            class: Class::Storage,
            capacity: 4,
            failure_domain: "zone-a".into(),
            rendezvous: "seeds:4500".into(),
            plan,
        }
    }

    #[test]
    fn a_record_round_trips_with_and_without_its_plan() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        assert_eq!(MachineRecord::read(dir.path()).expect("readable"), None);
        let bare = record(None);
        bare.write(dir.path()).expect("written");
        assert_eq!(
            MachineRecord::read(dir.path()).expect("readable"),
            Some(bare)
        );
        let plan = CellPlan {
            cell_id: 912_873,
            members: vec![(NodeId(5), "10.0.0.2:4500".parse().expect("an address"))],
            journals: vec![paros::machine::CELL_CONTROL, JournalKey::default()],
        };
        let formed = record(Some((PlanState::Formed, plan.clone())));
        formed.write(dir.path()).expect("written");
        let read = MachineRecord::read(dir.path())
            .expect("readable")
            .expect("present");
        assert_eq!(read.formed(), Some(&plan));
    }

    #[test]
    fn a_damaged_record_is_an_error_not_an_absence() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        fs::write(dir.path().join(FILE), "node_id x\n").expect("written");
        assert!(MachineRecord::read(dir.path()).is_err());
    }
}
