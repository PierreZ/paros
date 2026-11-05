//! The values each field of an operator block may take now (#844
//! (playground action blocks)), read from the last [`Topology`] only: the
//! fleet directory, the cells and the `Inspect` of every journal. Nothing
//! here asks a machine; the page fills its dropdowns from it.

use serde::{Deserialize, Serialize};

use super::model::Topology;

/// The valid values of the block fields, after one topology.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Choices {
    /// The cells, `cell-1` first: where a machine or a tenant can go.
    pub cells: Vec<String>,
    /// The machines in no cell: `add machine` admits one of them.
    pub idle: Vec<usize>,
    /// The machines in a cell that run: `kill`, `drain` and `maintenance`
    /// name one of them.
    pub running: Vec<usize>,
    /// The machine `kill` strikes by default: the running machine that
    /// leads the most journals (the lowest index on a tie), else the first
    /// running machine.
    pub busiest: Option<usize>,
    /// The machines a crash holds down: `restart` names one of them.
    pub down: Vec<usize>,
    /// The running machines on the network: `cut off` names one of them.
    pub connected: Vec<usize>,
    /// The machines cut from the network: `heal` names one of them.
    pub cut: Vec<usize>,
    /// The users' tenants (not the internal ones), by name.
    pub tenants: Vec<String>,
    /// The users' journals, as `tenant/journal`: not a tenant's control or
    /// election journal, which its coordinator writes, never a client.
    pub journals: Vec<String>,
}

impl Choices {
    /// The choices `topology` allows.
    #[must_use]
    pub fn of(topology: &Topology) -> Self {
        let count = topology.machines.len().max(topology.zones.len());
        let mut in_cell = vec![false; count];
        for cell in &topology.cells {
            for m in &cell.machines {
                if let Some(slot) = in_cell.get_mut(*m) {
                    *slot = true;
                }
            }
        }
        let down = sorted(&topology.down);
        let cut = sorted(&topology.isolated);
        let running: Vec<usize> = (0..count)
            .filter(|m| in_cell[*m] && !down.contains(m))
            .collect();
        let connected = running
            .iter()
            .copied()
            .filter(|m| !cut.contains(m))
            .collect();
        let users = topology.tenants.iter().filter(|t| t.kind == "users");
        Self {
            cells: topology.cells.iter().map(|c| c.name.clone()).collect(),
            idle: (0..count).filter(|m| !in_cell[*m]).collect(),
            busiest: busiest(topology, &running),
            running,
            down,
            connected,
            cut,
            tenants: users.clone().map(|t| t.name.clone()).collect(),
            journals: users
                .flat_map(|t| {
                    // The control journal comes first (`TenantPlace::journals`);
                    // an election journal is the coordinator's, not a client's.
                    t.journals
                        .iter()
                        .skip(1)
                        .filter(|j| j.name != "election")
                        .map(move |j| format!("{}/{}", t.name, j.name))
                })
                .collect(),
        }
    }
}

/// `list`, sorted, each machine once.
fn sorted(list: &[usize]) -> Vec<usize> {
    let mut out = list.to_vec();
    out.sort_unstable();
    out.dedup();
    out
}

/// The machine of `running` that leads the most journals, the lowest index
/// on a tie; the first of `running` when none leads.
fn busiest(topology: &Topology, running: &[usize]) -> Option<usize> {
    let leads = |m: usize| {
        topology
            .tenants
            .iter()
            .flat_map(|t| &t.journals)
            .filter(|j| j.leader == Some(m))
            .count()
    };
    running
        .iter()
        .copied()
        .max_by(|a, b| leads(*a).cmp(&leads(*b)).then(b.cmp(a)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::playground::model::{CellView, JournalPlace, Quorum, TenantPlace};

    fn journal(name: &str, leader: Option<usize>) -> JournalPlace {
        JournalPlace {
            name: name.into(),
            key: format!("1:{name}"),
            members: vec![0, 1, 2],
            quorum: Quorum::default(),
            leader,
            matchmakers: Vec::new(),
            hosts: vec![0, 1, 2],
            chosen: None,
            acceptors: Vec::new(),
        }
    }

    fn tenant(name: &str, kind: &str, journals: Vec<JournalPlace>) -> TenantPlace {
        TenantPlace {
            name: name.into(),
            kind: kind.into(),
            cell: "cell-1".into(),
            journals,
        }
    }

    /// Five machines: cell-1 over 0, 1 and 2, cell-2 over 4, machine 3
    /// idle. Machine 1 leads two journals, machine 2 one.
    fn topology() -> Topology {
        Topology {
            step: 0,
            time_ms: 0,
            machines: vec![
                Some("a".into()),
                Some("b".into()),
                Some("c".into()),
                None,
                Some("e".into()),
            ],
            nodes: vec![None; 5],
            zones: vec!["z1".into(); 5],
            classes: Vec::new(),
            bookings: Vec::new(),
            down: Vec::new(),
            isolated: Vec::new(),
            cells: vec![
                CellView {
                    name: "cell-1".into(),
                    machines: vec![0, 1, 2],
                },
                CellView {
                    name: "cell-2".into(),
                    machines: vec![4],
                },
            ],
            tenants: vec![
                tenant("universe", "universe", vec![journal("control", Some(2))]),
                tenant(
                    "acme",
                    "users",
                    vec![
                        journal("control", None),
                        journal("orders", Some(1)),
                        journal("events", Some(1)),
                        journal("election", Some(1)),
                    ],
                ),
                tenant("globex", "users", vec![journal("control", None)]),
            ],
        }
    }

    #[test]
    fn a_fresh_universe_lists_its_cells_and_idle_machines() {
        let choices = Choices::of(&topology());
        assert_eq!(choices.cells, ["cell-1", "cell-2"]);
        assert_eq!(choices.idle, [3]);
        assert_eq!(choices.running, [0, 1, 2, 4]);
        assert_eq!(choices.connected, [0, 1, 2, 4]);
        assert!(choices.down.is_empty() && choices.cut.is_empty());
    }

    #[test]
    fn only_the_users_tenants_and_their_data_journals_are_listed() {
        let choices = Choices::of(&topology());
        assert_eq!(choices.tenants, ["acme", "globex"]);
        assert_eq!(choices.journals, ["acme/orders", "acme/events"]);
    }

    #[test]
    fn kill_strikes_the_machine_that_leads_the_most_journals() {
        assert_eq!(Choices::of(&topology()).busiest, Some(1));
    }

    #[test]
    fn after_a_crash_only_the_crashed_machine_can_restart() {
        let mut topo = topology();
        topo.down = vec![1];
        let choices = Choices::of(&topo);
        assert_eq!(choices.down, [1]);
        assert_eq!(choices.running, [0, 2, 4]);
        // The busiest machine is down: the next one that leads takes it.
        assert_eq!(choices.busiest, Some(2));
    }

    #[test]
    fn after_a_cut_only_the_cut_machine_can_heal() {
        let mut topo = topology();
        topo.isolated = vec![4, 4];
        let choices = Choices::of(&topo);
        assert_eq!(choices.cut, [4]);
        assert_eq!(choices.connected, [0, 1, 2]);
        assert_eq!(choices.running, [0, 1, 2, 4]);
    }

    #[test]
    fn with_no_leader_the_first_running_machine_is_struck() {
        let mut topo = topology();
        topo.tenants.clear();
        assert_eq!(Choices::of(&topo).busiest, Some(0));
        topo.cells.clear();
        let choices = Choices::of(&topo);
        assert_eq!(choices.busiest, None);
        assert_eq!(choices.idle, [0, 1, 2, 3, 4]);
    }
}
