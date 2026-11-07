//! The machines' names (#257, #349): the name tables, the advertised addresses, the renames and where a machine binds.

use moonpool_sim::{
    ScriptedResolver, ShardBook, SimulationError, SimulationResult, StateHandle, assert_always,
    assert_reachable,
};
use paros::core::NodeId;
use paros::machine::MachineAddresses;
use paros::model::address::Address;

use crate::machine::OnDisk;
use crate::machine::board::{MachineBoard, lock, machine_board};
use crate::roles::Deployment;

/// The port every simulated machine binds and advertises.
pub(crate) const MACHINE_PORT: u16 = 4500;

pub(crate) const NAMES_KEY: &str = "paros-machine-names";

pub(crate) const OPERATOR_NAMES_KEY: &str = "paros-operator-names";

pub(crate) const GATE_NAMES_KEY: &str = "paros-gate-names";

/// The machines' name table (#257): moonpool's `ScriptedResolver`, which
/// the machines resolve each other's advertised names through, and which a
/// machine that moves repoints. A name a renamed machine left keeps the IP
/// it had, where nobody listens now (#349): its peers reach it only at the
/// name the registry holds.
///
/// The run's shard book resolves the replicas the machines declare through
/// the gate table ([`gate_table`]), not this one.
pub(crate) fn name_table(state: &StateHandle) -> ScriptedResolver {
    crate::state::published_arc(state, NAMES_KEY, ScriptedResolver::new)
        .as_ref()
        .clone()
}

/// The operators' name table (#349): the entry endpoints the workload's
/// clients dial. A rank's name follows its machine wherever it is, renamed
/// or not, as an operator who renames a machine updates its own entry.
pub(crate) fn operator_table(state: &StateHandle) -> ScriptedResolver {
    crate::state::published_arc(state, OPERATOR_NAMES_KEY, ScriptedResolver::new)
        .as_ref()
        .clone()
}

/// How the workload's clients resolve the machines' addresses (#257).
pub(crate) fn names(state: &StateHandle) -> paros::model::address::Names {
    paros::model::address::Names::new(operator_table(state))
}

/// How the machines resolve each other's addresses (#257, #349).
pub(crate) fn machine_names(state: &StateHandle) -> paros::model::address::Names {
    paros::model::address::Names::new(name_table(state))
}

/// The gate's name table: the run's shard book resolves the replicas the
/// machines declare through it. A journal names its members, and a set
/// its matchmakers, by the address its lanes dial, and moonpool counts a
/// wipe or damage only against a replica it can map to a process, by IP.
/// Recipe 9163977011002237483 141611:4883898652655768089 on c88d808: a
/// member named `machine-1.paros:4500`, wiped, counted as a clean copy,
/// and rot took the last one of slot 0. So this table maps a name to the
/// **process IP** of the machine that holds the copies, where the dial
/// tables point at the address it bound: a moved machine binds one outside
/// the topology (`10.250.<rank>.<move>`, [`boot_listen`]), which is no
/// process to moonpool, and resolved through the dial table the gate let
/// every wipe past it. Witness `replay-main 6827363754888045337` on
/// 0fdd015 (#865 (founders out)): the cell tenant's set was thinned to the
/// one matchmaker on a moved initial member, and moonpool's attrition
/// wiped that member with the set's only registry.
pub(crate) fn gate_table(state: &StateHandle) -> ScriptedResolver {
    crate::state::published_arc(state, GATE_NAMES_KEY, || {
        let table = ScriptedResolver::new();
        if let Some(book) = ShardBook::of(state) {
            book.resolve_names(table.clone());
        }
        table
    })
    .as_ref()
    .clone()
}

/// Point `host` at `ip` in both dial tables, and at `process`, the IP of
/// the process that listens there, in the gate's.
pub(crate) fn point(
    state: &StateHandle,
    host: &str,
    ip: std::net::IpAddr,
    process: std::net::IpAddr,
) {
    name_table(state).set(host, vec![ip]);
    operator_table(state).set(host, vec![ip]);
    gate_table(state).set(host, vec![process]);
}

/// Point the replica name of matchmaker `id` (`matchmaker-<id>`, the
/// host of `paros::roles`' `matchmaker_name`) at `process`, the IP of the
/// process that serves it, in the gate's table alone: a matchmaker names
/// the successor its decree binds it to by id, and the shard gate judges
/// a wipe of that process against it (#685 (no founders)). A wiped
/// machine's new id gets its own entry; the old id keeps pointing at the
/// process the wipe took, which the book counts as not intact.
pub(crate) fn point_matchmaker(state: &StateHandle, id: u64, process: &str) {
    let Ok(process) = process.parse::<std::net::IpAddr>() else {
        return;
    };
    gate_table(state).set(&format!("matchmaker-{id}"), vec![process]);
}

/// The host a machine of rank `rank` advertises on a seed whose machines
/// advertise names (#257).
pub(crate) fn machine_host(rank: usize) -> String {
    format!("machine-{rank}.paros")
}

/// The host the machine of rank `rank` advertises after its `count`-th
/// rename (#349).
pub(crate) fn renamed_host(rank: usize, count: u8) -> String {
    format!("machine-{rank}-{count}.paros")
}

/// The address the machine of rank `rank` at `ip` advertises: its name on
/// a seed whose machines advertise names, else its literal address.
pub(crate) fn advertised(
    layout: &crate::shape::MachineLayout,
    rank: usize,
    ip: &str,
) -> SimulationResult<Address> {
    let text = if layout.named {
        format!("{}:{MACHINE_PORT}", machine_host(rank))
    } else {
        format!("{ip}:{MACHINE_PORT}")
    };
    Address::parse(&text).map_err(SimulationError::InvalidState)
}

/// The machines' advertised addresses (#257), in rank order: what an
/// operator lists and dials.
pub(crate) fn machine_addrs(
    state: &StateHandle,
    deployment: &Deployment,
) -> SimulationResult<Vec<Address>> {
    let layout = crate::shape::machine_layout(state, deployment.machines().len());
    if layout.named {
        // A name resolves from the start, to the machine's process IP until
        // it boots elsewhere.
        let board = machine_board(state);
        let board = lock(&board);
        for (rank, ip) in deployment.machines().iter().enumerate() {
            if !board.listens.contains_key(&rank)
                && let Ok(ip) = ip.parse::<std::net::IpAddr>()
            {
                point(state, &machine_host(rank), ip, ip);
            }
        }
    }
    deployment
        .machines()
        .iter()
        .enumerate()
        .map(|(rank, ip)| advertised(&layout, rank, ip))
        .collect()
}

/// Whether the machine of rank `rank` may come back under a new name (#349).
/// Until the registry holds the new name, nobody can send to it, so an initial
/// member renamed is a member its peers cannot reach, and the registry's write
/// needs a majority of the others. So: every initial member formed (one still
/// waiting to form needs every member at its listed address for its `cell init`
/// Phase 1, so it never joins), no initial member was wiped, no other one was
/// renamed, and the cell is not of two (each member is the other's majority). A
/// machine outside the initial members is in no quorum; it is renamed only
/// while every initial member keeps its name, so the initial members its
/// admission names still answer. On a seed whose initial members keep their
/// names (`MachineLayout::initial_members_keep_names`), only a machine outside
/// them is renamed.
///
/// Its callers also rename an initial member only once every renamed machine
/// outside them is registered at its new name ([`renamed_outside_registered`]).
/// That gate applies after the rename draws, so the draw schedule does not
/// change.
pub(crate) fn may_rename(
    board: &MachineBoard,
    layout: &crate::shape::MachineLayout,
    rank: usize,
) -> bool {
    let initial_member_renamed = board.renamed.keys().any(|r| *r < layout.initial_members);
    if rank >= layout.initial_members {
        // After an initial member drained on a cell of fewer than three initial
        // members, a special set may sit alone on an admitted machine, with the
        // drained initial member still the sole acceptor of the cell's plan
        // journals: renamed, that machine is reachable only at its dead old
        // name, and no coordinator is left to register its new one. The other
        // side of the initial member limit below (replay-main
        // 16369298230464926129 on 52a8fda).
        let strands = board.initial_member_drains > 0 && layout.initial_members < 3;
        if strands {
            assert_reachable!(
                "machine: an admitted machine keeps its name after an initial member drained"
            );
        }
        return !initial_member_renamed && !strands;
    }
    // An initial member renamed after another one drained (#685 (special
    // tenants are tenants) step 5) may hold the cell tenant's set with the
    // drained initial member alone: its peers reach it only at its dead old
    // name, and no coordinator is left to register its new one (sweep seed
    // 380679206758150886 on c58adec). This is the limit the docs list for a
    // moved machine; the harness never puts a run past it. The admitted arm
    // above guards its other side.
    !layout.initial_members_keep_names
        && board.initial_member_drains == 0
        && layout.initial_members != 2
        && all_initial_members_formed(board)
        && board.wiped.is_empty()
        && board
            .renamed
            .keys()
            .all(|r| *r == rank || *r >= layout.initial_members)
}

/// Whether every machine outside the initial members that came back under a new
/// name is registered there: its own durable cached registry fold names it at
/// that name (#501 (matchmaker sets off the founders), #529 (sets oracles)). An
/// initial member is renamed only then. An admitted machine finds its
/// coordinator only through the initial members its book names, and the
/// coordinator reaches it only at the address the registry holds. If an initial
/// member is renamed before the machine registers, the machine's book names
/// only the initial member's dead old name and the registry names the machine
/// only at its dead old address: neither side reaches the other again, and no
/// set or journal moves onto the machine. This is the limit
/// `docs/architecture/` lists for a moved machine; the harness never puts a run
/// past it. Witness: recipe `6959312946067892202 110064:7756325629573379374` on
/// b93880a, a cell with one initial member whose admitted machine was renamed
/// at 37.3 s and whose initial member was renamed at 38.1 s.
pub(crate) fn renamed_outside_registered(
    board: &MachineBoard,
    layout: &crate::shape::MachineLayout,
) -> bool {
    board
        .renamed
        .iter()
        .filter(|(rank, _)| **rank >= layout.initial_members)
        .all(|(rank, name)| renamed_registered(board, *rank, name))
}

/// Whether the machine of `rank`, renamed `name`, is registered there: its
/// own durable cached registry fold names it at that name.
pub(crate) fn renamed_registered(board: &MachineBoard, rank: usize, name: &Address) -> bool {
    let Ok(rank_addr) = Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(rank))) else {
        return false;
    };
    board
        .nodes
        .get(&rank_addr)
        .and_then(|id| board.cached.get(id).map(|cache| (id, cache)))
        .is_some_and(|(id, cache)| cache.address(NodeId(*id)) == Some(name))
}

/// Whether every initial member formed its cell.
pub(crate) fn all_initial_members_formed(board: &MachineBoard) -> bool {
    board.initial_members.iter().all(|addr| {
        board
            .nodes
            .get(addr)
            .is_some_and(|n| board.formed.contains(n))
    })
}

/// The machines admitted into cell `cell_id` that were renamed this run and
/// are still on their disks (#349, #501 (matchmaker sets off the founders)):
/// each one's id and the name it advertises now.
pub(crate) fn renamed_admitted(state: &StateHandle, cell_id: u64) -> Vec<(u64, Address)> {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .renamed
        .iter()
        .filter_map(|(rank, name)| {
            let rank_addr =
                Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(*rank))).ok()?;
            let node = *board.nodes.get(&rank_addr)?;
            board
                .admitted
                .get(&node)
                .filter(|cell| cell.cell_id == cell_id)
                .map(|_| (node, name.clone()))
        })
        .collect()
}

/// The initial members renamed this run and still on their disks (#349):
/// each one's id and the name it advertises now.
pub(crate) fn renamed_initial_members(
    state: &StateHandle,
    initial_members: usize,
) -> Vec<(u64, Address)> {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .renamed
        .iter()
        .filter(|(rank, _)| **rank < initial_members)
        .filter_map(|(rank, name)| {
            let rank_addr =
                Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(*rank))).ok()?;
            let node = *board.nodes.get(&rank_addr)?;
            board
                .voters
                .contains_key(&node)
                .then(|| (node, name.clone()))
        })
        .collect()
}

/// Where the machine of rank `rank` binds at this boot (#257), and the
/// address it advertises (#349): its process IP at its first boot; on a
/// seed whose machines advertise names, a reboot may land at a new address
/// (the run's `move_pct`), as a container that Docker restarts gets a new
/// IP. The name follows the machine: the table is repointed before the
/// machine binds, and its peers resolve the name again after a failed dial.
/// Moonpool routes a connection by the address a listener bound, so a moved
/// machine binds an address of its own outside the topology
/// (`10.250.<rank>.<move>`); the network faults that strike by process IP
/// pass it by while it is there. The shard gate does not: it resolves the
/// machine's names through [`gate_table`], to its process IP.
///
/// A machine in a cell may also come back under a new name (the run's
/// `rename_pct`, #349): `machine-<rank>-<n>.paros`, at a new address. Its
/// old name keeps the old address, where nobody listens, so its peers reach
/// it only once the cell's registry holds the new name. A machine whose
/// disk is empty is a new one: it advertises its rank's name again.
pub(crate) fn boot_listen(
    state: &StateHandle,
    layout: &crate::shape::MachineLayout,
    rank: usize,
    my_ip: &str,
    disk: OnDisk,
) -> SimulationResult<(std::net::SocketAddr, Address)> {
    let own: std::net::IpAddr = my_ip
        .parse()
        .map_err(|e| SimulationError::InvalidState(format!("bad machine ip {my_ip}: {e}")))?;
    let original = advertised(layout, rank, my_ip)?;
    if !layout.named {
        return Ok((std::net::SocketAddr::new(own, MACHINE_PORT), original));
    }
    // Read from the audit before the board is locked.
    let needed = super::board::plan_quorum_holders(state);
    let board = machine_board(state);
    let mut board = lock(&board);
    let rebooted = board.listens.contains_key(&rank);
    if disk == OnDisk::Empty && board.renamed.remove(&rank).is_some() {
        assert_reachable!("machine: a wiped renamed machine advertises its rank's name again");
    }
    let forced = board.rename_next == Some(rank);
    if forced {
        board.rename_next = None;
    }
    // A machine no plan journal's quorum, and no special set's majority,
    // can do without keeps its name (`super::board::plan_quorum_holders`):
    // renamed, it would hold its journals until the cell registers the new
    // name, in the very journal it alone serves, or every rebooted journal
    // would wait on a probe its set can no longer answer.
    let keeps = board
        .nodes
        .get(&original)
        .is_some_and(|node| needed.contains(node));
    let renames = rebooted
        && disk == OnDisk::InCell
        && may_rename(&board, layout, rank)
        && !keeps
        && (forced || moonpool_sim::sim_random_range(0_u32..100_u32) < layout.rename_pct)
        && (rank >= layout.initial_members || renamed_outside_registered(&board, layout));
    let moves =
        renames || (rebooted && moonpool_sim::sim_random_range(0_u32..100_u32) < layout.move_pct);
    let ip = if moves {
        let count = board.moves.entry(rank).or_default();
        *count = count.wrapping_add(1).max(1);
        let rank_octet = u8::try_from(rank).unwrap_or(u8::MAX);
        let new_ip = std::net::IpAddr::from([10, 250, rank_octet, *count]);
        assert_reachable!("machine: a machine comes back at a new address");
        tracing::info!(rank, from = %board.listens[&rank], to = %new_ip, "machine_moved");
        new_ip
    } else {
        board.listens.get(&rank).copied().unwrap_or(own)
    };
    board.listens.insert(rank, ip);
    if renames {
        let count = board.renames.entry(rank).or_default();
        *count = count.wrapping_add(1).max(1);
        let host = renamed_host(rank, *count);
        let new_name = Address::parse(&format!("{host}:{MACHINE_PORT}"))
            .map_err(SimulationError::InvalidState)?;
        assert_reachable!("machine: a machine comes back under a new name");
        tracing::info!(rank, %new_name, "machine_renamed");
        board.renamed.insert(rank, new_name);
        point(state, &host, ip, own);
    } else if board.renamed.contains_key(&rank) {
        let count = board.renames.get(&rank).copied().unwrap_or(1);
        point(state, &renamed_host(rank, count), ip, own);
    } else {
        point(state, &machine_host(rank), ip, own);
    }
    // The operator's entry for the rank follows the machine (#349).
    operator_table(state).set(&machine_host(rank), vec![ip]);
    let advertise = board.renamed.get(&rank).cloned().unwrap_or(original);
    Ok((std::net::SocketAddr::new(ip, MACHINE_PORT), advertise))
}

/// An operator's slip now and then (#257): a wildcard listen address and no
/// advertised one, which the start refuses. The operator then sets both; a
/// simulated machine binds its own address, never a wildcard (moonpool
/// routes a connection by the address a listener bound).
pub(crate) fn operator_slip() {
    if moonpool_sim::buggify_with_prob!(0.05) {
        let wildcard = std::net::SocketAddr::from(([0, 0, 0, 0], MACHINE_PORT));
        let refused = MachineAddresses::new(wildcard, None);
        assert_always!(
            refused.is_err(),
            "machine: a wildcard listen address with no advertised one is refused"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A matchmaker's id names its process in the gate's table only: a
    /// wiped machine's old id still points at the process the wipe took.
    #[test]
    fn a_matchmaker_id_names_its_process_in_the_gate_table() {
        let state = StateHandle::new();
        point_matchmaker(&state, 17, "10.0.6.1");
        assert_eq!(
            gate_table(&state).addresses("matchmaker-17"),
            Some(vec!["10.0.6.1".parse().expect("an ip")])
        );
        assert_eq!(name_table(&state).addresses("matchmaker-17"), None);
        assert_eq!(operator_table(&state).addresses("matchmaker-17"), None);
        point_matchmaker(&state, 18, "10.0.6.1");
        assert_eq!(
            gate_table(&state).addresses("matchmaker-17"),
            Some(vec!["10.0.6.1".parse().expect("an ip")])
        );
        // A name that is no address is not pointed.
        point_matchmaker(&state, 19, "machine-1.paros");
        assert_eq!(gate_table(&state).addresses("matchmaker-19"), None);
    }

    /// A name points at the address a machine bound in the dial tables,
    /// and at its process in the gate's: a moved machine stays a process
    /// to the shard gate.
    #[test]
    fn a_moved_machine_keeps_its_process_ip_in_the_gate_table() {
        let state = StateHandle::new();
        let process: std::net::IpAddr = "10.0.6.2".parse().expect("an ip");
        let moved: std::net::IpAddr = "10.250.1.3".parse().expect("an ip");
        point(&state, "machine-1.paros", moved, process);
        assert_eq!(
            name_table(&state).addresses("machine-1.paros"),
            Some(vec![moved])
        );
        assert_eq!(
            operator_table(&state).addresses("machine-1.paros"),
            Some(vec![moved])
        );
        assert_eq!(
            gate_table(&state).addresses("machine-1.paros"),
            Some(vec![process])
        );
    }

    fn layout(initial_members: usize) -> crate::shape::MachineLayout {
        crate::shape::MachineLayout {
            initial_members,
            listed: initial_members,
            machines: Vec::new(),
            named: true,
            move_pct: 0,
            rename_pct: 100,
            initial_members_keep_names: false,
            reserved: false,
        }
    }

    #[test]
    fn an_admitted_machine_keeps_its_name_after_an_initial_member_of_a_small_cell_drained() {
        let mut board = MachineBoard::default();
        assert!(
            may_rename(&board, &layout(1), 1),
            "no initial member drained yet"
        );
        board.initial_member_drains = 1;
        assert!(!may_rename(&board, &layout(1), 1));
        assert!(!may_rename(&board, &layout(2), 2));
        // Three initial members keep a staying majority: a set never sits alone
        // on an admitted machine with a drained initial member.
        assert!(may_rename(&board, &layout(3), 3));
    }
}
