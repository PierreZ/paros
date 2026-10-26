//! **The cell's side of the universe** (#513 (fleet Init duel), #225,
//! `docs/architecture/03-1-cell-tenant-and-bootstrap.md` §3.1, §3.7): the cell coordinator writes the
//! cell's `JoinFleet`, and each tenant's footprint, `HostTenant` and
//! `DropTenant`, into the cell control journal, its one writer.
//!
//! `init`'s fleet half adds the cell to the universe directory (`AddCell`).
//! The cell coordinator reads that directory, which the cell hosts, and
//! writes `JoinFleet` with the directory's fleet id once the directory names
//! this cell and its control journal. The operator then waits for the join
//! and marks the cell `READY` ([`paros_client::fleet`]). So `init` never
//! claims the cell control journal and never fences the coordinator.
//!
//! Before #513 the operator claimed the cell control journal for this one
//! write. The coordinator it fenced resigned, the next term installed a
//! fresh uuid, and that uuid fenced the operator in turn. With slow links in
//! the recovery tail, each claim lost to the next install, and `init` did not
//! end (witness 1522711066683701314 on a0c4aa6: the cell write came back
//! `Superseded`, red on "fleet: an operation an operator stopped in ends in
//! the recovery tail").
//!
//! The coordinator follows the directory at its install (an interrupted
//! operation) and at every watch round ([`follow`]). A read of the directory
//! that does not reach its tail writes nothing: the next round reads again.
//!
//! **Tenant operations** (#225, decided on 2026-10-15). The same reading
//! writes the cell's step of each tenant operation the directory holds in
//! flight for this cell ([`tenant_steps`]). For a `REGISTERING` tenant the
//! cell neither hosts nor dropped: its birth footprint first
//! (`GrantCapacity`, #476 (tenant footprint)), then `HostTenant` once the
//! footprint is granted. For a `REMOVING` tenant it has not dropped:
//! `DropTenant`. The operator writes only the
//! directory steps, and waits for the cell's step in between
//! ([`paros_client::fleet`]). So a tenant's creation or removal never
//! claims the cell control journal either. The directory entry is the work
//! order: the coordinator acts on every entry, whoever registered it. A
//! `HostTenant` decided from a stale directory, after a removal moved the
//! entry to `REMOVING`, is refused at apply once the tombstone landed.
//!
//! **Another cell's universe** (#232 (multiple cells)). A cell whose plan
//! names no fleet journal (an idle cell, formed by `init --idle`) is never
//! joined by [`follow`]: `universe add-cell` asks its coordinator to join
//! with `JoinUniverse` ([`join`]). The request carries the universe
//! pointer: the directory's journal and the machines of the cell that hosts
//! it. The coordinator reads the directory there, with a client over those
//! machines, and writes `JoinFleet` with the pointer once the directory
//! holds this cell under its cell tenant's control journal. From then on
//! [`follow`] reads the directory at the pointer ([`directory_reader`]),
//! and the tenant steps above run as in the cell that hosts the universe.

use paros_core::{JournalIdentifier, TenantId};
use paros_net::runtime::ParosProviders;

use paros_client::Client;
use paros_client::cell::CellSession;
use paros_client::checkpoint::{Folder, LoadOutcome, load};
use paros_client::fleet::Interrupted;
use paros_client::universe::{JoinAnswer, JoinRequest};
use paros_model::fleet::{CellState, FleetDirectory, METADATA_VERSION, TenantKind, TenantState};
use paros_model::footprint::{self, Footprint};
use paros_model::machine::ControlJournals;
use paros_model::registry::{GrantKey, HostedTenant, Registry, SystemCommand};

/// The `JoinFleet` the universe directory `directory` asks of the cell
/// `journals` names: the directory holds a fleet and an entry for this
/// cell, not being removed, whose cell tenant has this cell's control
/// journal. `None` while the directory asks nothing of this cell.
#[must_use]
pub(crate) fn joining(
    directory: &FleetDirectory,
    journals: ControlJournals,
) -> Option<SystemCommand> {
    let fleet_id = directory.fleet()?;
    assert_ne!(fleet_id, 0, "a formed fleet has an id");
    let entry = directory.cell(journals.cell_id)?;
    if entry.state == CellState::Removing {
        return None;
    }
    if entry.control_tenant != journals.cell.tenant {
        return None;
    }
    let tenant = directory.tenant(entry.control_tenant)?;
    if tenant.control != journals.cell.journal {
        return None;
    }
    assert_ne!(journals.cell_id, 0, "a cell entry names a set cell id");
    Some(SystemCommand::JoinFleet {
        fleet_id,
        cell_id: journals.cell_id,
        version: METADATA_VERSION,
        universe: None,
    })
}

/// The cell step of each tenant operation that the universe directory
/// `directory` holds in flight for this cell (#225; the tenant half of #513
/// (fleet Init duel)), at most one per tenant, from the cell `registry`.
/// For each `users` tenant of cell `cell_id` that the cell has not dropped:
///
/// - `REGISTERING` and not hosted: first its birth footprint (#476), a
///   `GrantCapacity` that books every slot [`footprint::plan`] finds, or a
///   refusal when the slots do not fit. A refusal waits until every founding
///   member registered or was marked down ([`footprint::ready`]): an
///   unregistered founder that is up offers no slot yet. Then, once the footprint is granted, `HostTenant`.
///   A refused footprint asks nothing more: the operator marks the tenant
///   `ERROR`.
/// - `REMOVING`: `DropTenant`.
///
/// The list is empty until the cell `registry` folds joined the directory's
/// fleet under `cell_id`: before that, the fold refuses a tenant entry.
#[must_use]
pub(crate) fn tenant_steps(
    directory: &FleetDirectory,
    registry: &Registry,
    cell_id: u64,
) -> Vec<SystemCommand> {
    let Some(fleet_id) = directory.fleet() else {
        return Vec::new();
    };
    let joined = registry.fleet();
    if joined.is_none_or(|f| (f.fleet_id, f.cell_id) != (fleet_id, cell_id)) {
        return Vec::new();
    }
    let mut steps = Vec::new();
    for (tenant, entry) in directory.tenants() {
        if entry.cell_id != cell_id || entry.kind != TenantKind::Users {
            continue;
        }
        assert!(tenant.is_set(), "a directory entry names a set tenant");
        assert!(
            entry.control.is_set(),
            "a directory entry names a set control journal"
        );
        // A dropped tenant is never hosted again, and its tombstone holds.
        if registry.dropped(tenant) {
            continue;
        }
        match entry.state {
            TenantState::Registering if !registry.hosts(tenant) => {
                let step = match registry.footprint(tenant) {
                    Some(grant) if grant.granted() => Some(SystemCommand::HostTenant {
                        fleet_id,
                        tenant,
                        hosted: HostedTenant {
                            control: entry.control,
                            name: entry.name.clone(),
                            survives: entry.survives,
                        },
                    }),
                    // The operator marks the tenant ERROR.
                    Some(_) => None,
                    None => footprint_grant(registry, tenant),
                };
                steps.extend(step);
            }
            // The tombstone goes down whether the cell hosts the tenant or
            // not: a stale host step can then never host it.
            TenantState::Removing => {
                steps.push(SystemCommand::DropTenant { fleet_id, tenant });
            }
            _ => {}
        }
    }
    assert!(
        steps.len() <= directory.tenants().count(),
        "at most one step per tenant"
    );
    steps
}

/// The `GrantCapacity` that records `tenant`'s birth footprint from
/// `registry` (#476): every slot when the plan fits, a refusal when it does
/// not and every founding member registered or was marked down, else
/// `None` (a founder that registers later may offer the slots).
fn footprint_grant(registry: &Registry, tenant: TenantId) -> Option<SystemCommand> {
    assert!(
        registry.footprint(tenant).is_none(),
        "a footprint is recorded once"
    );
    let slots = match footprint::plan(registry, tenant, &Footprint::birth()) {
        Ok(slots) => Some(slots),
        Err(_) if !footprint::ready(registry) => return None,
        Err(_) => {
            if registry.genesis().any(|node| registry.get(node).is_none()) {
                moonpool_assertions::reachable!(
                    "footprint: a founder marked down is counted absent"
                );
            }
            None
        }
    };
    // The negative space: a footprint is withheld only for a founder that
    // may still register.
    assert!(
        slots.is_some() || footprint::ready(registry),
        "a footprint is refused only once no founder may still register"
    );
    assert!(
        slots.as_ref().is_none_or(|slots| !slots.is_empty()),
        "a granted footprint books slots"
    );
    Some(SystemCommand::GrantCapacity {
        key: GrantKey::Footprint(tenant),
        tenant,
        slots,
    })
}

/// Follow the universe directory through the term's `session`. Fold the
/// directory into `directory` from where it stands. Then join the cell to
/// the universe when the cell has not joined and the directory holds it.
/// Then write the cell step of each tenant operation the directory holds in
/// flight ([`tenant_steps`]). Returns how many entries it wrote.
///
/// # Errors
///
/// A write did not land: the term writes nothing more.
///
/// # Panics
///
/// If the session is not open (a programmer error), or a written entry is
/// not in the session's fold.
pub(crate) async fn follow<P: ParosProviders>(
    client: &Client<P>,
    session: &mut CellSession,
    directory: &mut Folder<FleetDirectory>,
) -> Result<usize, Interrupted> {
    assert!(
        session.is_open(),
        "only an open session follows the universe"
    );
    let journals = session.journals();
    let Some((universe, reader)) = directory_reader(client, journals, session.registry()) else {
        return Ok(0);
    };
    if !matches!(
        load(directory, universe, &reader, 0, 0).await,
        LoadOutcome::Loaded { .. }
    ) {
        return Ok(0);
    }
    let mut wrote = 0;
    if session.registry().fleet().is_none()
        && let Some(join) = joining(directory.state(), journals)
    {
        session.record(client, 0, &join).await?;
        // The fold judges the join at apply: another fleet's id is refused,
        // so the cell names this fleet or none.
        let joined = session.registry().fleet();
        assert!(
            joined.is_none_or(|f| f.cell_id == journals.cell_id),
            "a cell joins under its own id"
        );
        moonpool_assertions::reachable!("fleet: a step joins the cell to the fleet");
        wrote += 1;
    }
    // A tenant's birth takes two steps, its footprint then its host: a
    // second pass writes the host of a footprint the first one granted.
    for _ in 0..TENANT_PASSES {
        let steps = tenant_steps(directory.state(), session.registry(), journals.cell_id);
        if steps.is_empty() {
            break;
        }
        for step in steps {
            session.record(client, 0, &step).await?;
            note(session.registry(), &step);
            wrote += 1;
        }
    }
    Ok(wrote)
}

/// Where this cell reads the universe directory, and a client that reads
/// it: its own fleet journal through its own machines when it hosts the
/// universe tenant, else the universe pointer its join recorded (#232
/// (multiple cells)), through the machines the pointer names. `None` while
/// the cell knows no universe.
pub(crate) fn directory_reader<P: ParosProviders>(
    client: &Client<P>,
    journals: ControlJournals,
    registry: &Registry,
) -> Option<(JournalIdentifier, Client<P>)> {
    if let Some(pointer) = registry.universe() {
        return Some((pointer.directory, client.elsewhere(&pointer.hosts)));
    }
    journals.fleet.map(|fleet| (fleet, client.clone()))
}

/// Answer `universe add-cell`'s join request (#232 (multiple cells)) as the
/// cell coordinator, through the term's `session`: read the universe
/// directory at the request's pointer, and once it holds this cell under
/// its cell tenant's control journal, write `JoinFleet` with the pointer.
/// A cell that joined this universe answers `Joined` at once; one that
/// joined another, or hosts a universe tenant of its own, `OtherUniverse`.
///
/// # Panics
///
/// If the session is not open (a programmer error).
pub(crate) async fn join<P: ParosProviders>(
    client: &Client<P>,
    session: &mut CellSession,
    request: &JoinRequest,
) -> JoinAnswer {
    assert!(session.is_open(), "only an open session joins a universe");
    let journals = session.journals();
    match session.registry().fleet() {
        Some(joined)
            if joined.fleet_id == request.fleet_id && joined.cell_id == journals.cell_id =>
        {
            return JoinAnswer::Joined;
        }
        Some(_) => return JoinAnswer::OtherUniverse,
        None => {}
    }
    if journals.fleet.is_some() {
        // A cell `init` formed hosts a universe tenant of its own.
        return JoinAnswer::OtherUniverse;
    }
    let reader = client.elsewhere(&request.pointer.hosts);
    let mut directory = Folder::new(FleetDirectory::default());
    if !matches!(
        load(&mut directory, request.pointer.directory, &reader, 0, 0).await,
        LoadOutcome::Loaded { .. }
    ) {
        return JoinAnswer::Unavailable;
    }
    if directory.state().fleet() != Some(request.fleet_id) {
        return JoinAnswer::NotListed;
    }
    let Some(SystemCommand::JoinFleet {
        fleet_id,
        cell_id,
        version,
        ..
    }) = joining(directory.state(), journals)
    else {
        return JoinAnswer::NotListed;
    };
    let join = SystemCommand::JoinFleet {
        fleet_id,
        cell_id,
        version,
        universe: Some(request.pointer.clone()),
    };
    if session.record(client, 0, &join).await.is_err() {
        return JoinAnswer::Unavailable;
    }
    match session.registry().fleet() {
        Some(joined) if joined.fleet_id == request.fleet_id => {
            moonpool_assertions::reachable!("universe: a cell joins a universe another cell hosts");
            JoinAnswer::Joined
        }
        Some(_) => JoinAnswer::OtherUniverse,
        None => JoinAnswer::Unavailable,
    }
}

/// How many times one [`follow`] asks [`tenant_steps`]: a tenant's birth
/// is a footprint, then a host. A step the fold refused is asked again at
/// the next watch round, so the bound also ends a round.
const TENANT_PASSES: usize = 2;

/// Check that the recorded tenant `step` is in the fold `registry`.
fn note(registry: &Registry, step: &SystemCommand) {
    match *step {
        SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(tenant),
            ref slots,
            ..
        } => {
            // The fold refuses a slot whose machine left meanwhile: the
            // next round plans again from the fold.
            if let Some(grant) = registry.footprint(tenant) {
                assert_eq!(
                    grant.granted(),
                    slots.is_some(),
                    "a recorded footprint is the one written"
                );
                moonpool_assertions::reachable!("fleet: a step records a tenant's footprint");
            }
        }
        SystemCommand::HostTenant { tenant, .. } => {
            // The fold refuses a tenant dropped meanwhile.
            assert!(
                registry.hosts(tenant) || registry.dropped(tenant),
                "a hosted tenant is in the fold"
            );
            moonpool_assertions::reachable!("fleet: a step hosts a tenant on the cell");
        }
        SystemCommand::DropTenant { tenant, .. } => {
            assert!(registry.dropped(tenant), "a dropped tenant is tombstoned");
            assert!(!registry.hosts(tenant), "a dropped tenant is not hosted");
            moonpool_assertions::reachable!("fleet: a step drops a tenant from the cell");
        }
        _ => unreachable!("a tenant step books, hosts or drops"),
    }
}

#[cfg(test)]
mod tests {
    use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};

    use super::*;
    use paros_client::checkpoint::Checkpointable;
    use paros_model::fleet::{FleetCommand, FleetEntry};

    fn journals() -> ControlJournals {
        ControlJournals {
            cell_id: 9,
            cell: JournalIdentifier::new(TenantId(5), JournalId(6)),
            election: None,
            fleet: Some(JournalIdentifier::new(TenantId(7), JournalId(8))),
        }
    }

    fn apply(directory: &mut FleetDirectory, seq: u64, command: FleetCommand) {
        directory.apply(seq, &FleetEntry::new(42, command).encode());
    }

    #[test]
    fn the_directory_asks_a_join_only_once_it_holds_this_cell() {
        let journals = journals();
        let mut directory = FleetDirectory::default();
        assert_eq!(joining(&directory, journals), None, "no fleet yet");
        apply(
            &mut directory,
            0,
            FleetCommand::FormFleet {
                control: journals.fleet.expect("a fleet journal"),
                name: Vec::new(),
            },
        );
        assert_eq!(joining(&directory, journals), None, "no cell entry yet");
        // Another cell's entry asks nothing of this one.
        let mut other = directory.clone();
        apply(
            &mut other,
            1,
            FleetCommand::AddCell {
                cell_id: 10,
                control: JournalIdentifier::new(TenantId(11), JournalId(12)),
                name: Vec::new(),
                kind: paros_model::fleet::CellKind::default(),
                endpoint: Vec::new(),
            },
        );
        assert_eq!(joining(&other, journals), None);
        // An entry for this cell id with another control journal neither.
        let mut moved = directory.clone();
        apply(
            &mut moved,
            1,
            FleetCommand::AddCell {
                cell_id: journals.cell_id,
                control: JournalIdentifier::new(TenantId(5), JournalId(13)),
                name: Vec::new(),
                kind: paros_model::fleet::CellKind::default(),
                endpoint: Vec::new(),
            },
        );
        assert_eq!(joining(&moved, journals), None);
        apply(
            &mut directory,
            1,
            FleetCommand::AddCell {
                cell_id: journals.cell_id,
                control: journals.cell,
                name: Vec::new(),
                kind: paros_model::fleet::CellKind::default(),
                endpoint: Vec::new(),
            },
        );
        assert_eq!(
            joining(&directory, journals),
            Some(SystemCommand::JoinFleet {
                fleet_id: 42,
                cell_id: journals.cell_id,
                version: METADATA_VERSION,
                universe: None,
            })
        );
        // A cell being removed is not joined again.
        apply(
            &mut directory,
            2,
            FleetCommand::MarkCell {
                cell_id: journals.cell_id,
                state: CellState::Removing,
            },
        );
        assert_eq!(joining(&directory, journals), None);
    }

    fn founder(capacity: u64) -> SystemCommand {
        SystemCommand::RegisterNode {
            id: NodeId(1),
            addr: "n1".to_owned(),
            class: paros_model::machine::Class::Storage,
            capacity,
            locality: paros_model::machine::Locality::default(),
            name: String::new(),
            incarnation: 1,
            machine_interface: Vec::new(),
        }
    }

    /// A directory whose fleet holds this cell `READY` and the `users`
    /// tenant 20 `REGISTERING` on it.
    fn registering(journals: ControlJournals) -> FleetDirectory {
        let mut directory = FleetDirectory::default();
        let fleet = journals.fleet.expect("a fleet journal");
        let commands = [
            FleetCommand::FormFleet {
                control: fleet,
                name: Vec::new(),
            },
            FleetCommand::AddCell {
                cell_id: journals.cell_id,
                control: journals.cell,
                name: Vec::new(),
                kind: paros_model::fleet::CellKind::default(),
                endpoint: Vec::new(),
            },
            FleetCommand::MarkCell {
                cell_id: journals.cell_id,
                state: CellState::Ready,
            },
            FleetCommand::RegisterTenant {
                control: JournalIdentifier::new(TenantId(20), JournalId(21)),
                name: b"a".to_vec(),
                cell_id: journals.cell_id,
                survives: paros_model::tenant::Survives::Az,
                tags: paros_model::tags::UserTags::none(),
            },
        ];
        for (seq, command) in (0..).zip(commands) {
            apply(&mut directory, seq, command);
        }
        directory
    }

    #[test]
    fn a_refused_footprint_asks_nothing_more_until_the_removal() {
        let journals = journals();
        let mut directory = registering(journals);
        let tenant = TenantId(20);
        let mut registry = Registry::new([NodeId(1)]);
        let commands = [
            joining(&directory, journals).expect("the directory asks the join"),
            founder(1),
        ];
        for (seq, command) in (0..).zip(&commands) {
            registry.apply(seq, &command.encode());
        }
        // One slot: the footprint does not fit, and every founder registered.
        let steps = tenant_steps(&directory, &registry, journals.cell_id);
        assert_eq!(
            steps,
            vec![SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(tenant),
                tenant,
                slots: None,
            }]
        );
        registry.apply(2, &steps[0].encode());
        assert!(registry.footprint(tenant).is_some_and(|g| !g.granted()));
        // The operator marks the tenant ERROR: the cell never hosts it.
        assert!(tenant_steps(&directory, &registry, journals.cell_id).is_empty());
        apply(
            &mut directory,
            4,
            FleetCommand::MarkTenant {
                tenant,
                state: TenantState::Removing,
            },
        );
        assert_eq!(
            tenant_steps(&directory, &registry, journals.cell_id),
            vec![SystemCommand::DropTenant {
                fleet_id: 42,
                tenant
            }]
        );
    }

    #[test]
    fn a_founder_marked_down_lets_the_cell_refuse_a_footprint() {
        let journals = journals();
        let directory = registering(journals);
        let tenant = TenantId(20);
        let mut registry = Registry::new([NodeId(1), NodeId(2)]);
        let commands = [
            joining(&directory, journals).expect("the directory asks the join"),
            founder(1),
        ];
        for (seq, command) in (0..).zip(&commands) {
            registry.apply(seq, &command.encode());
        }
        // Founder 2 never registered and is up: the cell withholds its answer.
        assert!(tenant_steps(&directory, &registry, journals.cell_id).is_empty());
        let down = SystemCommand::MachineDown {
            id: NodeId(2),
            incarnation: 0,
        };
        registry.apply(2, &down.encode());
        assert!(!registry.liveness(NodeId(2)).up);
        // Founder 2 is marked down: the footprint is refused as it stands.
        assert_eq!(
            tenant_steps(&directory, &registry, journals.cell_id),
            vec![SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(tenant),
                tenant,
                slots: None,
            }]
        );
    }

    #[test]
    fn the_coordinator_hosts_a_registering_tenant_and_drops_a_removing_one() {
        let journals = journals();
        let mut directory = registering(journals);
        let tenant = TenantId(20);
        assert_eq!(
            directory.tenant(tenant).map(|t| t.state),
            Some(TenantState::Registering)
        );
        let mut registry = Registry::new([NodeId(1)]);
        // Before the cell joined, it hosts nothing: its fold would refuse it.
        assert!(tenant_steps(&directory, &registry, journals.cell_id).is_empty());
        let mut seq = 0;
        let mut write = |registry: &mut Registry, command: &SystemCommand| {
            registry.apply(seq, &command.encode());
            seq += 1;
        };
        write(
            &mut registry,
            &joining(&directory, journals).expect("the directory asks the join"),
        );
        // The founding member has not registered: it offers no slot, and
        // the cell refuses no footprint before it does.
        assert!(tenant_steps(&directory, &registry, journals.cell_id).is_empty());
        write(&mut registry, &founder(16));
        let steps = tenant_steps(&directory, &registry, journals.cell_id);
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            SystemCommand::GrantCapacity { key: GrantKey::Footprint(t), slots: Some(_), .. }
                if *t == tenant
        ));
        // Another cell's tenant asks nothing of this one.
        assert!(tenant_steps(&directory, &registry, journals.cell_id + 1).is_empty());
        write(&mut registry, &steps[0]);
        assert!(
            registry
                .footprint(tenant)
                .is_some_and(paros_model::registry::Grant::granted)
        );
        let steps = tenant_steps(&directory, &registry, journals.cell_id);
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            SystemCommand::HostTenant { fleet_id: 42, tenant: t, hosted }
                if *t == tenant && hosted.control == JournalId(21) && hosted.name == b"a"
        ));
        write(&mut registry, &steps[0]);
        assert!(registry.hosts(tenant));
        assert!(tenant_steps(&directory, &registry, journals.cell_id).is_empty());
        apply(
            &mut directory,
            4,
            FleetCommand::MarkTenant {
                tenant,
                state: TenantState::Removing,
            },
        );
        let steps = tenant_steps(&directory, &registry, journals.cell_id);
        assert_eq!(
            steps,
            vec![SystemCommand::DropTenant {
                fleet_id: 42,
                tenant
            }]
        );
        write(&mut registry, &steps[0]);
        assert!(registry.dropped(tenant));
        assert!(!registry.hosts(tenant));
        // A dropped tenant asks nothing more, and is never hosted again.
        assert!(tenant_steps(&directory, &registry, journals.cell_id).is_empty());
    }
}
