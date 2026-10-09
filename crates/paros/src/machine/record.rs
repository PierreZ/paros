//! The machine record (#196): a machine's identity and its cell, the one
//! record a machine keeps beside its journal stores, rewritten whole by its
//! [`MachineDisk`](super::MachineDisk).
//!
//! A machine is **formatted** on its first start, on an empty disk: its
//! `node_id` is minted then, at random (#225), so a wiped disk comes back as
//! a new machine and "a wiped identity never rejoins" holds by construction.
//! A disk that holds stores but no record lost its identity, and is refused
//! as amnesia ([`super::run_machine`]).
//!
//! The rendezvous join list is stored too and re-read on every start (the
//! configuration's, when given, wins and is recorded). Once `init` forms the
//! cell, the record holds its plan: first as **pending** on the seed that
//! runs `init` (a re-run resumes it, never redraws it), then as **formed** —
//! the commit point, written after every journal store of the plan is
//! formatted ([`super::MachineDisk::provision`]).
//!
//! ```text
//! node_id 6150928431937019931
//! class storage
//! capacity 1
//! failure_domain zone-a
//! rendezvous seeds:4500
//! plan formed 912873
//! member 6150928431937019931 10.0.0.2:4500
//! control 11986532017395081213/5302873011246751929
//! fleet 7240096361733624127/14183513009914637262
//! journal 11986532017395081213/5302873011246751929
//! journal 7240096361733624127/14183513009914637262
//! journal 2965734451981346203/9861377130450924019
//! ```

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;

use paros_core::{Config, JournalIdentifier, NodeId, QuorumSystem};

use super::{CellPlan, Class};

/// Parse a journal identifier written `<tenant>/<journal>` (#235).
fn parse_identifier(text: &str) -> Option<JournalIdentifier> {
    text.contains('/').then(|| text.parse().ok()).flatten()
}

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
    /// The formed plan, if this machine has a cell.
    #[must_use]
    pub fn formed(&self) -> Option<&CellPlan> {
        match &self.plan {
            Some((PlanState::Formed, plan)) => Some(plan),
            _ => None,
        }
    }

    /// The record as text, the form a [`MachineDisk`](super::MachineDisk) keeps.
    #[must_use]
    pub fn render(&self) -> String {
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
            let _ = writeln!(text, "control {}", plan.control);
            if let Some(fleet) = plan.fleet {
                let _ = writeln!(text, "fleet {fleet}");
            }
            for journal in &plan.journals {
                let _ = writeln!(text, "journal {journal}");
            }
        }
        text
    }

    /// The record [`MachineRecord::render`] wrote.
    ///
    /// # Errors
    ///
    /// The text is not a record: a damaged record is an error, never an
    /// absence.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut fields: BTreeMap<&str, &str> = BTreeMap::new();
        let mut plan: Option<(PlanState, u64)> = None;
        let mut members = Vec::new();
        let mut journals = Vec::new();
        let mut control = None;
        let mut fleet = None;
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
                    journals.push(
                        parse_identifier(value).ok_or_else(|| format!("bad journal {value:?}"))?,
                    );
                }
                "control" => {
                    control = Some(
                        parse_identifier(value).ok_or_else(|| format!("bad control {value:?}"))?,
                    );
                }
                "fleet" => {
                    fleet = Some(
                        parse_identifier(value).ok_or_else(|| format!("bad fleet {value:?}"))?,
                    );
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
                    control: control.ok_or("the machine record's plan names no control journal")?,
                    fleet,
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
pub fn journal_config(plan: &CellPlan, node_id: NodeId, journal: JournalIdentifier) -> Config {
    let members: Vec<NodeId> = plan.members.iter().map(|(id, _)| *id).collect();
    Config {
        peers: members.clone(),
        nodes: members,
        quorum_system: QuorumSystem::Majority,
        ..Config::new(node_id, journal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{JournalId, TenantId};

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
        let bare = record(None);
        assert_eq!(MachineRecord::parse(&bare.render()), Ok(bare));
        let identifier =
            |tenant, journal| JournalIdentifier::new(TenantId(tenant), JournalId(journal));
        let plan = CellPlan {
            cell_id: 912_873,
            members: vec![(NodeId(5), "10.0.0.2:4500".parse().expect("an address"))],
            control: identifier(0x51, 0x52),
            fleet: Some(identifier(0x61, 0x62)),
            journals: vec![
                identifier(0x51, 0x52),
                identifier(0x61, 0x62),
                identifier(0x71, 0x72),
            ],
        };
        let formed = record(Some((PlanState::Formed, plan.clone())));
        let read = MachineRecord::parse(&formed.render()).expect("a record");
        assert_eq!(read.formed(), Some(&plan));
        assert_eq!(read, formed);
        let pending = record(Some((PlanState::Pending, plan)));
        let read = MachineRecord::parse(&pending.render()).expect("a record");
        assert_eq!(read.formed(), None);
        assert_eq!(read, pending);
    }

    #[test]
    fn a_damaged_record_is_an_error() {
        assert!(MachineRecord::parse("node_id x\n").is_err());
        assert!(MachineRecord::parse("node_id 3\n").is_err());
        assert!(MachineRecord::parse(&format!("{}bogus 1\n", record(None).render())).is_err());
    }
}
