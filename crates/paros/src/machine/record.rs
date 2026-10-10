//! The machine record (#196, #277): a machine's identity, its standing in
//! the cell decree and its cell, the one record a machine keeps beside its
//! journal stores, rewritten whole by its [`MachineDisk`](super::MachineDisk).
//!
//! A machine is **formatted** on its first start, on an empty disk: its
//! `node_id` is minted then, at random (#225), so a wiped disk comes back as
//! a new machine and "a wiped identity never rejoins" holds by construction.
//! A disk that holds stores but no record lost its identity, and is refused
//! as amnesia ([`super::run_machine`]).
//!
//! The record is the machine's acceptor state in the cell decree (#277): its
//! **promise** (`promised`, absent while it promised nothing), raised by a
//! `PrepareCell` before the answer leaves, and its **vote** — the plan it
//! accepted and the ballot it accepted it at. Accepting is forming: the vote
//! is written after every journal store of the plan is formatted
//! ([`super::MachineDisk::provision`]), so the plan line is the commit point.
//!
//! A machine that `cell add-machine` admitted (#216) has no vote: its record
//! holds the **admission** instead — the cell, its control journals and the
//! cell's machines it knew then (`admitted`, `control`, `fleet`, `election`,
//! `peer`).
//! A record holds a vote or an admission, never both.
//!
//! ```text
//! node_id 6150928431937019931
//! class storage
//! capacity 1
//! failure_domain zone-a
//! promised 4417/6150928431937019931
//! plan 912873 4417/6150928431937019931
//! member 6150928431937019931 10.0.0.2:4500
//! control 11986532017395081213/5302873011246751929
//! fleet 7240096361733624127/14183513009914637262
//! election 11986532017395081213/1735003470911288215
//! journal 11986532017395081213/1735003470911288215
//! journal 11986532017395081213/5302873011246751929
//! journal 7240096361733624127/14183513009914637262
//! journal 2965734451981346203/9861377130450924019
//! ```

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;

use paros_core::{Ballot, Config, JournalIdentifier, NodeId, QuorumSystem, WriterMode};

use super::{Admission, CellPlan, Class, ControlJournals};

/// Parse a journal identifier written `<tenant>/<journal>` (#235).
fn parse_identifier(text: &str) -> Option<JournalIdentifier> {
    text.contains('/').then(|| text.parse().ok()).flatten()
}

/// Parse a ballot written `<round>/<node>`.
fn parse_ballot(text: &str) -> Option<Ballot> {
    let (round, node) = text.split_once('/')?;
    Some(Ballot {
        round: round.parse().ok()?,
        node: NodeId(node.parse().ok()?),
    })
}

/// The admission an `admitted` line and its `control`, `fleet`, `election`
/// and `peer` lines name, checked; `None` with no `admitted` line (and no
/// peer).
fn admission(
    admitted: Option<u64>,
    control: Option<JournalIdentifier>,
    (fleet, election): (Option<JournalIdentifier>, Option<JournalIdentifier>),
    peers: Vec<(NodeId, SocketAddr)>,
) -> Result<Option<Admission>, String> {
    let Some(cell_id) = admitted else {
        if !peers.is_empty() {
            return Err("the machine record names peers and no admission".into());
        }
        return Ok(None);
    };
    let admission = Admission {
        cell: ControlJournals {
            cell_id,
            cell: control.ok_or("the machine record's admission names no control journal")?,
            fleet,
            election,
        },
        members: peers,
    };
    admission.check()?;
    Ok(Some(admission))
}

/// Parse a `member` or `peer` line's value, `<node_id> <addr>`.
fn parse_member(value: &str) -> Result<(NodeId, SocketAddr), String> {
    let (id, addr) = value
        .split_once(' ')
        .ok_or_else(|| format!("bad member line {value:?}"))?;
    Ok((
        NodeId(id.parse().map_err(|e| format!("bad member id: {e}"))?),
        addr.parse::<SocketAddr>()
            .map_err(|e| format!("bad member address: {e}"))?,
    ))
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
    /// Its promise in the cell decree: no plan under a lower ballot is
    /// accepted. The zero ballot while it promised nothing.
    pub promised: Ballot,
    /// Its vote, which is its cell: the plan it accepted (and formed) and
    /// the ballot it accepted it at.
    pub plan: Option<(Ballot, CellPlan)>,
    /// Its admission into a cell by `cell add-machine` (#216), which is its
    /// cell when it has no vote.
    pub admitted: Option<Admission>,
}

impl MachineRecord {
    /// The formed plan, if this machine has a cell.
    #[must_use]
    pub fn formed(&self) -> Option<&CellPlan> {
        self.plan.as_ref().map(|(_, plan)| plan)
    }

    /// The cell this machine belongs to, founded or admitted: 0 while it is
    /// idle.
    #[must_use]
    pub fn cell_id(&self) -> u64 {
        self.formed()
            .map(|plan| plan.cell_id)
            .or(self.admitted.as_ref().map(|a| a.cell.cell_id))
            .unwrap_or(0)
    }

    /// The record as text, the form a [`MachineDisk`](super::MachineDisk) keeps.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text = format!(
            "node_id {}\nclass {}\ncapacity {}\nfailure_domain {}\n",
            self.node_id.0,
            self.class.as_str(),
            self.capacity,
            self.failure_domain,
        );
        if self.promised != Ballot::default() {
            let p = self.promised;
            let _ = writeln!(text, "promised {}/{}", p.round, p.node.0);
        }
        if let Some((ballot, plan)) = &self.plan {
            let _ = writeln!(
                text,
                "plan {} {}/{}",
                plan.cell_id, ballot.round, ballot.node.0
            );
            for (id, addr) in &plan.members {
                let _ = writeln!(text, "member {} {addr}", id.0);
            }
            let _ = writeln!(text, "control {}", plan.control);
            if let Some(fleet) = plan.fleet {
                let _ = writeln!(text, "fleet {fleet}");
            }
            let _ = writeln!(text, "election {}", plan.election);
            for journal in &plan.journals {
                let _ = writeln!(text, "journal {journal}");
            }
        }
        if let Some(admission) = &self.admitted {
            let _ = writeln!(text, "admitted {}", admission.cell.cell_id);
            let _ = writeln!(text, "control {}", admission.cell.cell);
            if let Some(fleet) = admission.cell.fleet {
                let _ = writeln!(text, "fleet {fleet}");
            }
            if let Some(election) = admission.cell.election {
                let _ = writeln!(text, "election {election}");
            }
            for (id, addr) in &admission.members {
                let _ = writeln!(text, "peer {} {addr}", id.0);
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
        let mut plan: Option<(Ballot, u64)> = None;
        let mut promised = Ballot::default();
        let mut members = Vec::new();
        let mut peers = Vec::new();
        let mut admitted: Option<u64> = None;
        let mut journals = Vec::new();
        let mut control = None;
        let mut fleet = None;
        let mut election = None;
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let (key, value) = line.split_once(' ').unwrap_or((line, ""));
            let identifier =
                || parse_identifier(value).ok_or_else(|| format!("bad {key} {value:?}"));
            match key {
                "node_id" | "class" | "capacity" | "failure_domain" => {
                    fields.insert(key, value);
                }
                "promised" => {
                    promised =
                        parse_ballot(value).ok_or_else(|| format!("bad promise {value:?}"))?;
                }
                "plan" => {
                    let (cell, ballot) = value
                        .split_once(' ')
                        .ok_or_else(|| format!("bad plan line {line:?}"))?;
                    let cell = cell.parse().map_err(|e| format!("bad cell id: {e}"))?;
                    let ballot = parse_ballot(ballot)
                        .ok_or_else(|| format!("bad plan ballot {ballot:?}"))?;
                    plan = Some((ballot, cell));
                }
                "admitted" => {
                    admitted = Some(
                        value
                            .parse()
                            .map_err(|e| format!("bad admitted cell id: {e}"))?,
                    );
                }
                "member" => members.push(parse_member(value)?),
                "peer" => peers.push(parse_member(value)?),
                "journal" => journals.push(identifier()?),
                "control" => control = Some(identifier()?),
                "fleet" => fleet = Some(identifier()?),
                "election" => election = Some(identifier()?),
                _ => return Err(format!("unknown machine record key {key:?}")),
            }
        }
        let field = |name: &str| {
            fields
                .get(name)
                .copied()
                .ok_or_else(|| format!("the machine record names no {name}"))
        };
        if plan.is_some() && admitted.is_some() {
            return Err("the machine record holds a vote and an admission".into());
        }
        let admitted = admission(admitted, control, (fleet, election), peers)?;
        let plan = match plan {
            Some((ballot, cell_id)) => {
                let plan = CellPlan {
                    cell_id,
                    members,
                    control: control.ok_or("the machine record's plan names no control journal")?,
                    fleet,
                    election: election
                        .ok_or("the machine record's plan names no election journal")?,
                    journals,
                };
                plan.check()?;
                if ballot.round == 0 || ballot > promised {
                    return Err("the machine record's vote is not under its promise".into());
                }
                Some((ballot, plan))
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
            promised,
            plan,
            admitted,
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
        // The election journal takes campaigns from every candidate (#240).
        writer_mode: if journal == plan.election {
            WriterMode::Multi
        } else {
            WriterMode::Single
        },
        ..Config::new(node_id, journal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{JournalId, TenantId};

    fn ballot(round: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(5),
        }
    }

    fn record(promised: Ballot, plan: Option<(Ballot, CellPlan)>) -> MachineRecord {
        MachineRecord {
            node_id: NodeId(u64::MAX - 3),
            class: Class::Storage,
            capacity: 4,
            failure_domain: "zone-a".into(),
            promised,
            plan,
            admitted: None,
        }
    }

    #[test]
    fn a_record_round_trips_with_and_without_its_plan() {
        let bare = record(Ballot::default(), None);
        assert_eq!(MachineRecord::parse(&bare.render()), Ok(bare));
        let promised = record(ballot(9), None);
        assert_eq!(MachineRecord::parse(&promised.render()), Ok(promised));
        let identifier =
            |tenant, journal| JournalIdentifier::new(TenantId(tenant), JournalId(journal));
        let plan = CellPlan {
            cell_id: 912_873,
            members: vec![(NodeId(5), "10.0.0.2:4500".parse().expect("an address"))],
            control: identifier(0x51, 0x52),
            election: identifier(0x51, 0x53),
            fleet: Some(identifier(0x61, 0x62)),
            journals: vec![
                identifier(0x51, 0x52),
                identifier(0x51, 0x53),
                identifier(0x61, 0x62),
                identifier(0x71, 0x72),
            ],
        };
        let formed = record(ballot(9), Some((ballot(7), plan.clone())));
        let read = MachineRecord::parse(&formed.render()).expect("a record");
        assert_eq!(read.formed(), Some(&plan));
        assert_eq!(read, formed);
        let above = record(ballot(7), Some((ballot(9), plan)));
        assert!(
            MachineRecord::parse(&above.render()).is_err(),
            "a vote never outranks the promise"
        );
    }

    #[test]
    fn an_admitted_record_round_trips_and_never_holds_a_vote_too() {
        let identifier =
            |tenant, journal| JournalIdentifier::new(TenantId(tenant), JournalId(journal));
        let admission = Admission {
            cell: ControlJournals {
                cell_id: 912_873,
                cell: identifier(0x51, 0x52),
                election: Some(identifier(0x51, 0x53)),
                fleet: None,
            },
            members: vec![
                (NodeId(5), "10.0.0.2:4500".parse().expect("an address")),
                (NodeId(8), "10.0.0.3:4500".parse().expect("an address")),
            ],
        };
        let admitted = MachineRecord {
            admitted: Some(admission.clone()),
            ..record(Ballot::default(), None)
        };
        let read = MachineRecord::parse(&admitted.render()).expect("a record");
        assert_eq!(read, admitted);
        assert_eq!(read.cell_id(), 912_873);
        assert_eq!(record(Ballot::default(), None).cell_id(), 0);
        let plan = CellPlan {
            cell_id: 912_873,
            members: vec![(NodeId(5), "10.0.0.2:4500".parse().expect("an address"))],
            control: identifier(0x51, 0x52),
            election: identifier(0x51, 0x53),
            fleet: None,
            journals: vec![identifier(0x51, 0x52), identifier(0x51, 0x53)],
        };
        let both = MachineRecord {
            admitted: Some(admission),
            ..record(ballot(9), Some((ballot(7), plan)))
        };
        assert!(
            MachineRecord::parse(&both.render()).is_err(),
            "a record holds a vote or an admission"
        );
        let bare = record(Ballot::default(), None).render();
        assert!(MachineRecord::parse(&format!("{bare}peer 5 10.0.0.2:4500\n")).is_err());
    }

    #[test]
    fn a_damaged_record_is_an_error() {
        assert!(MachineRecord::parse("node_id x\n").is_err());
        assert!(MachineRecord::parse("node_id 3\n").is_err());
        let bare = record(Ballot::default(), None).render();
        assert!(MachineRecord::parse(&format!("{bare}bogus 1\n")).is_err());
        assert!(MachineRecord::parse(&format!("{bare}promised 3\n")).is_err());
    }
}
