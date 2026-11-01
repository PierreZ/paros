//! What a served term answers: the machines' `Register` (#349) and the journal requests (#210).

use std::sync::PoisonError;
use std::time::Duration;

use moonpool_core::LocalTimeProvider;
use moonpool_rpc::RpcHandle;
use paros_client::capacity::{CapacityAnswer, CapacityRequest};
use paros_client::cell::CellSession;
use paros_client::journals::{JournalAnswer, JournalRequest};
use paros_client::universe::{JoinAnswer, JoinRequest};
use paros_client::{Client, bootstrap};
use paros_core::NodeId;
use paros_model::address::{Address, Names};
use paros_model::machine::incarnation_from_halves;
use paros_model::registry::SystemCommand;
use paros_net::runtime::ParosProviders;
use paros_net::{Inbound, ReplySender};
use paros_wire::machine as wire;
use tokio_util::sync::CancellationToken;

use crate::cell::watch::{Watch, identified, spawn_probe};
use crate::cell::{TermGroup, Watching, capacity, park};
use crate::tenant::TenantDesk;

/// The next request `inbound` holds, or never when there is no endpoint.
pub(crate) async fn next_request<M: moonpool_rpc::RpcMethod, T, U: 'static>(
    inbound: Option<&mut Inbound<M, T, U>>,
) -> Option<(T, ReplySender<U>)> {
    match inbound {
        Some(inbound) => inbound.recv().await,
        None => std::future::pending().await,
    }
}

/// Answer the `Register` requests (#349) and journal requests (#210) the
/// term's `group` receives until `until`; with no group, only wait. The
/// deadline is absolute: a request does not push the candidate's next step
/// back. A journal request goes through `desk` while this member serves the
/// term, and is refused `not_coordinator` by a group that outlived its term;
/// a desk the request found superseded stops the wait, so the term ends at
/// once.
pub(crate) async fn answer_until<P: ParosProviders>(
    (providers, rpc, client, names): (&P, &RpcHandle<P>, &Client<P>, &Names),
    cell: (u64, Duration),
    until: Duration,
    mut group: Option<&mut TermGroup>,
    (watching, parked, desk): (
        &mut Watching,
        &mut Option<Watch>,
        &mut Option<(u64, TenantDesk)>,
    ),
    shutdown: &CancellationToken,
) {
    loop {
        let (registers, requests, capacities, joins) = match group.as_deref_mut() {
            Some(g) => (
                Some(&mut g.registers),
                Some(&mut g.requests),
                Some(&mut g.capacities),
                Some(&mut g.joins),
            ),
            None => (None, None, None, None),
        };
        let left = until.saturating_sub(providers.time().now());
        if left.is_zero() || shutdown.is_cancelled() {
            return;
        }
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            Some((request, reply)) = next_request(registers) => {
                let ack =
                    register(providers, rpc, client, names, cell, (watching, parked), request)
                        .await;
                reply.send(ack);
            }
            Some((request, reply)) = next_request(requests) => {
                let answer = match (JournalRequest::from_wire(&request), desk.as_mut()) {
                    (Err(_), _) => JournalAnswer::Malformed,
                    (Ok(request), Some((_, desk))) => {
                        let session = watching.as_mut().map(|(session, _)| session);
                        let answer = desk.answer(client, &request, session).await;
                        if watching.as_ref().is_some_and(|(session, _)| !session.is_open()) {
                            // The term's first write that did not land ends
                            // its watch, as in `Watch::round`.
                            park(watching, parked);
                        }
                        reply.send(answer.to_wire());
                        if desk.superseded() {
                            return;
                        }
                        continue;
                    }
                    (Ok(_), None) => {
                        moonpool_assertions::reachable!(
                            "coordinator: a request to an ended term's group is refused"
                        );
                        JournalAnswer::NotCoordinator
                    }
                };
                reply.send(answer.to_wire());
            }
            Some((request, reply)) = next_request(capacities) => {
                // A tenant coordinator's capacity request (#476): decided
                // through the term's session, its booking ids drawn by the
                // desk.
                let answer = match (CapacityRequest::from_wire(&request), desk.as_mut()) {
                    (Err(_), _) => CapacityAnswer::Malformed,
                    (Ok(_), None) => CapacityAnswer::NotCoordinator,
                    (Ok(request), Some((_, desk))) => match watching.as_mut() {
                        None => CapacityAnswer::Unavailable,
                        Some((session, _)) => {
                            let answer = capacity::grant(client, session, &request, || {
                                desk.draw_booking()
                            })
                            .await;
                            if !session.is_open() {
                                park(watching, parked);
                            }
                            answer
                        }
                    },
                };
                reply.send(answer.to_wire());
            }
            Some((request, reply)) = next_request(joins) => {
                // `universe add-cell` (#232 (multiple cells)): the cell
                // half of a join, written through the term's session.
                let answer = match (JoinRequest::from_wire(&request), desk.as_ref()) {
                    (Err(_), _) => JoinAnswer::Malformed,
                    (Ok(_), None) => JoinAnswer::NotCoordinator,
                    (Ok(request), Some(_)) => match watching.as_mut() {
                        None => JoinAnswer::Unavailable,
                        Some((session, _)) => {
                            let answer = crate::universe::join(client, session, &request).await;
                            if !session.is_open() {
                                park(watching, parked);
                            }
                            answer
                        }
                    },
                };
                reply.send(answer.to_wire());
            }
            _ = providers.time().sleep(left) => return,
        }
    }
}

/// The cell's address book for a `Register` from `id`, from a fold caught
/// up to the tail when the term's fold does not name `id`; `None` when the
/// catch-up failed. The term's fold moves only on its own writes, and an
/// operator's `cell add-machine` registers machines between them. The
/// machine takes `unknown_machine` as final, so it is answered only from a
/// fold at the tail (witness: recipe 1769568628990727465
/// 102806:3654508340332248788 on PR #691 (client write inputs): an admitted
/// machine renamed while it asked a term whose fold predated its
/// registration was never registered at its new name).
pub(crate) async fn book_naming<P: ParosProviders>(
    session: &mut CellSession,
    client: &Client<P>,
    id: NodeId,
) -> Option<Vec<(NodeId, Address)>> {
    let book = paros_model::machine::cell_book(session.founders(), session.registry());
    if book.iter().any(|(member, _)| *member == id) {
        return Some(book);
    }
    let before = session.position();
    if !session.refresh(client, 0).await {
        return None;
    }
    assert!(
        session.position() >= before,
        "a refreshed fold never goes back"
    );
    moonpool_assertions::reachable!("coordinator: a register catches its fold up first");
    Some(paros_model::machine::cell_book(
        session.founders(),
        session.registry(),
    ))
}

/// Answer a machine's `Register` (#349): register the address it advertises
/// now in the cell control journal, through the served term's session, when
/// the cell's address book does not hold it. Refused while this member
/// serves no term. A write that does not land ends the term's watch, as in
/// [`Watch::round`]. The machine's `Identify` waits at most `patience`, one
/// renewal period, so a request to an address nobody answers at does not
/// cost the term its renewals. An `Identify` that does not answer in time
/// goes on in a task with the client's request timeout, and the machine's
/// next `Register` reads its answer: a link slower than one renewal period
/// stays slow for the whole run, and the inline check alone refused such a
/// machine forever (#454 (control plane meets chaos): witness
/// 3556034011252864713, a founding member renamed onto a link with 81 ms
/// of extra latency, under a 100 ms renewal period).
#[tracing::instrument(level = "debug", skip_all, fields(cell = cell_id))]
pub(crate) async fn register<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    (cell_id, patience): (u64, Duration),
    (watching, parked): (&mut Watching, &mut Option<Watch>),
    request: wire::Register,
) -> wire::RegisterAck {
    let refuse = |refusal: &str| wire::RegisterAck {
        registered: false,
        refusal: refusal.into(),
    };
    let Some(identity) = request.identity else {
        return refuse("malformed");
    };
    let (Ok(addr), Some((class, locality))) = (
        Address::parse(&identity.addr),
        paros_model::machine::identity_tags(&identity),
    ) else {
        return refuse("malformed");
    };
    let id = NodeId(identity.node_id);
    if id.0 == 0 {
        return refuse("malformed");
    }
    if identity.cell_id != cell_id {
        return refuse("other_cell");
    }
    let Some((session, watch)) = watching.as_mut() else {
        return refuse("not_coordinator");
    };
    let Some(book) = book_naming(session, client, id).await else {
        return refuse("unavailable");
    };
    let Some((_, known)) = book.iter().find(|(member, _)| *member == id) else {
        return refuse("unknown_machine");
    };
    let registered = wire::RegisterAck {
        registered: true,
        refusal: String::new(),
    };
    // Registered already: the registry holds this address, this name and
    // this machine interface. A founding member the registry never held
    // registers once, for its name (#399), and again on every start that
    // serves another machine interface (#404).
    let held = session.registry().get(id).is_some_and(|node| {
        (&node.name, &node.machine_interface, node.class)
            == (&identity.name, &identity.machine_interface, class)
    });
    if *known == addr && held {
        return registered;
    }
    // The machine answers there as itself, and as the incarnation that
    // asked: a late request from an earlier start names an address the
    // machine left.
    let incarnation = incarnation_from_halves(identity.incarnation_high, identity.incarnation_low);
    let timeout = client.tunables().request_timeout;
    let probed = watch.probes.get(&id).and_then(|probe| {
        (probe.addr == addr && probe.incarnation == incarnation).then(|| {
            *probe
                .answered
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        })
    });
    match probed {
        Some(Some(true)) => {
            moonpool_assertions::reachable!(
                "coordinator: a slow machine registers through its probe"
            );
        }
        // A probe of this address still runs: its answer is read next time.
        Some(None) => return refuse("unreachable"),
        Some(Some(false)) | None => {
            let answered = identified(
                bootstrap::identify(providers, rpc, names, &addr, patience.min(timeout)).await,
                id,
                incarnation,
            );
            if !answered {
                if timeout > patience {
                    let probe =
                        spawn_probe(providers, rpc, names, &addr, (id, incarnation), timeout);
                    watch.probes.insert(id, probe);
                }
                return refuse("unreachable");
            }
        }
    }
    watch.probes.remove(&id);
    let command = SystemCommand::RegisterNode {
        id,
        addr: addr.to_string(),
        class,
        locality,
        name: identity.name,
        incarnation,
        machine_interface: identity.machine_interface,
    };
    if session.record(client, 0, &command).await.is_err() {
        // The term's first write that did not land ends its watch.
        park(watching, parked);
        return refuse("unavailable");
    }
    // The registry refuses fewer roles than it holds (`PAROS_ROLES`).
    if session.registry().get(id).is_some_and(|n| n.class != class) {
        return refuse("fewer_roles");
    }
    let book = paros_model::machine::cell_book(session.founders(), session.registry());
    if !book.iter().any(|entry| *entry == (id, addr.clone())) {
        // The registry refused it (a changed class): the book stands.
        return refuse("unknown_machine");
    }
    moonpool_assertions::reachable!("coordinator: a moved machine registers its new address");
    tracing::info!(cell = cell_id, node = id.0, %addr, "machine_address_registered");
    registered
}
