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

/// The machines' name table (#257): moonpool's `ScriptedResolver`, which
/// the machines resolve each other's advertised names through, and which a
/// machine that moves repoints. A name a renamed machine left keeps the IP
/// it had, where nobody listens now (#349): its peers reach it only at the
/// name the registry holds.
///
/// The run's shard book resolves the replicas the machines declare through
/// it too: a journal names its members by the address its lanes dial, and
/// moonpool counts a wipe or damage only against a replica it can map to a
/// process. Recipe 9163977011002237483 141611:4883898652655768089 on
/// c88d808: a member named `machine-1.paros:4500`, wiped, counted as a
/// clean copy, and rot took the last one of slot 0.
pub(crate) fn name_table(state: &StateHandle) -> ScriptedResolver {
    crate::state::published_arc(state, NAMES_KEY, || {
        let table = ScriptedResolver::new();
        if let Some(book) = ShardBook::of(state) {
            book.resolve_names(table.clone());
        }
        table
    })
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

/// Point `host` at `ip` in both tables.
pub(crate) fn point(state: &StateHandle, host: &str, ip: std::net::IpAddr) {
    name_table(state).set(host, vec![ip]);
    operator_table(state).set(host, vec![ip]);
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
        let table = name_table(state);
        for (rank, ip) in deployment.machines().iter().enumerate() {
            if !board.listens.contains_key(&rank)
                && let Ok(ip) = ip.parse::<std::net::IpAddr>()
            {
                table.set(&machine_host(rank), vec![ip]);
                operator_table(state).set(&machine_host(rank), vec![ip]);
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

/// Whether the machine of rank `rank` may come back under a new name
/// (#349). Until the registry holds the new name, nobody can send to it, so
/// a founding member renamed is a member its peers cannot reach, and the
/// registry's write needs a majority of the others. So: every founding
/// member formed (one still waiting to form needs every member at its listed
/// address for its `cell init` Phase 1, so it never joins), no founding
/// member was wiped, no other one was renamed, and the cell is not of two
/// (each member is the other's majority). A machine outside the founders is in
/// no quorum; it is renamed only while every founding member keeps its
/// name, so the founders its admission names still answer. On a seed whose
/// founding members keep their names (`MachineLayout::founders_keep_names`),
/// only a machine outside them is renamed.
///
/// Its callers also rename a founding member only once every renamed machine
/// outside them is registered at its new name ([`renamed_outside_registered`]).
/// That gate applies after the rename draws, so the draw schedule does not
/// change.
pub(crate) fn may_rename(
    board: &MachineBoard,
    layout: &crate::shape::MachineLayout,
    rank: usize,
) -> bool {
    let founder_renamed = board.renamed.keys().any(|r| *r < layout.founders);
    if rank >= layout.founders {
        return !founder_renamed;
    }
    !layout.founders_keep_names
        && layout.founders != 2
        && all_founders_formed(board)
        && board.wiped.is_empty()
        && board
            .renamed
            .keys()
            .all(|r| *r == rank || *r >= layout.founders)
}

/// Whether every machine outside the founding members that came back under a
/// new name is registered there: its own durable cached registry fold names
/// it at that name (#501 (matchmaker sets off the founders), #529 (sets
/// oracles)). A founding member is renamed only then. An admitted machine
/// finds its coordinator only through the founding members its book names,
/// and the coordinator reaches it only at the address the registry holds. If
/// a founding member is renamed before the machine registers, the machine's
/// book names only the founder's dead old name and the registry names the
/// machine only at its dead old address: neither side reaches the other
/// again, and no set or journal moves onto the machine. This is the limit
/// `docs/architecture/` lists for a moved machine; the harness never puts
/// a run past it. Witness: recipe `6959312946067892202
/// 110064:7756325629573379374` on b93880a, a one-founder cell whose admitted
/// machine was renamed at 37.3 s and whose founder was renamed at 38.1 s.
pub(crate) fn renamed_outside_registered(
    board: &MachineBoard,
    layout: &crate::shape::MachineLayout,
) -> bool {
    board
        .renamed
        .iter()
        .filter(|(rank, _)| **rank >= layout.founders)
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

/// Whether every founding member formed its cell.
pub(crate) fn all_founders_formed(board: &MachineBoard) -> bool {
    board.founders.iter().all(|addr| {
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

/// The founding members renamed this run and still on their disks (#349):
/// each one's id and the name it advertises now.
pub(crate) fn renamed_founders(state: &StateHandle, founders: usize) -> Vec<(u64, Address)> {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .renamed
        .iter()
        .filter(|(rank, _)| **rank < founders)
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
/// pass it by while it is there.
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
    let renames = rebooted
        && disk == OnDisk::InCell
        && may_rename(&board, layout, rank)
        && (forced || moonpool_sim::sim_random_range(0_u32..100_u32) < layout.rename_pct)
        && (rank >= layout.founders || renamed_outside_registered(&board, layout));
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
        point(state, &host, ip);
    } else if board.renamed.contains_key(&rank) {
        let count = board.renames.get(&rank).copied().unwrap_or(1);
        point(state, &renamed_host(rank, count), ip);
    } else {
        name_table(state).set(&machine_host(rank), vec![ip]);
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
