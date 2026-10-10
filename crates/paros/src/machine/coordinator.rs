//! **The cell coordinator** (#240, `docs/architecture.md` §3.3, §3.7): every
//! founding member campaigns for it in the cell's election journal through
//! [`crate::client::election`], and the winner leads the cell control journal
//! for its term.
//!
//! The candidate runs in a task of its own beside the node loop, through a
//! [`Client`] over the cell's members, exactly as any other client of the
//! cell would. A term, once won, is served in the actor's order (§3.3):
//!
//! 1. **Install** the term's uuid on the cell control journal with
//!    `SetLeader(uuid, current)` (`SetLeader(uuid, unset)` for the cell's
//!    first coordinator), then fold that journal to its tail
//!    ([`CellSession::open`]).
//! 2. **Finish in-flight work**: admit every machine the registry holds that
//!    is neither a founding member nor retired. `Admit` is idempotent, so a
//!    machine an earlier term admitted answers that it is in the cell.
//! 3. **Publish** the interface: the next renewal names the member's
//!    address ([`Election::publish`]).
//! 4. **Stop at the first refused write**: a claim that loses ends the term,
//!    and the coordinator resigns. Its writer sends nothing once superseded.
//!
//! Until admin calls become requests to the coordinator (#212, #225), an
//! admin session still claims the cell control journal and fences the
//! coordinator. The coordinator does not fight back: it writes only when a
//! term starts.
//!
//! The candidate draws no randomness of its own: its seed and its two
//! BUGGIFY decisions (a stalled leader, a hand-off) are drawn on the node
//! loop before the task starts, and the backoff jitter derives from the seed.

use std::sync::Arc;
use std::time::Duration;

use moonpool_core::{Detach, Providers, RandomProvider, TaskProvider, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use tokio_util::sync::CancellationToken;

use super::{CellPlan, ControlJournals, FormedCell};
use crate::client::cell::CellSession;
use crate::client::checkpoint::CheckpointPolicy;
use crate::client::election::{Candidate, Election, ElectionTunables, Leader, Step, hand_off};
use crate::client::fleet::{Interrupted, Stage, Step as FleetStep};
use crate::client::{CallObserver, ClaimOutcome, Client, ClientTunables, Server};
use crate::rpc::NodeClient;
use crate::system::NodeStanding;
use crate::{Address, DriverTunables, Names};

/// What serving one term's duties came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermDuty {
    /// The term is installed, the cell control journal folded to its tail
    /// and every in-flight admission asked again: `admitted` of them
    /// finished in this term.
    Served {
        /// The admissions this term finished.
        admitted: usize,
    },
    /// The install lost: another uuid leads the cell control journal now.
    /// The term is over for this actor.
    Refused,
    /// The install or the fold did not end: try again at the next step.
    Unavailable,
}

/// Serve the duties of the term `leader` (this actor's) on the cell's
/// control journals: install the term's uuid, fold the cell control journal
/// to its tail, then finish every admission in flight, through `client`.
/// The cell coordinator runs it for every term it wins; a harness candidate
/// runs the very same code.
///
/// # Panics
///
/// If `leader` carries the unset uuid, or a postcondition fails (a
/// programmer error).
#[tracing::instrument(level = "debug", skip_all, fields(term = leader.term, candidate = leader.candidate.id))]
pub async fn serve_term<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    (journals, founders): (ControlJournals, Vec<(NodeId, Address)>),
    leader: &Leader,
    policy: CheckpointPolicy,
) -> TermDuty {
    assert!(leader.uuid.is_set(), "a term's uuid is set");
    let mut session = CellSession::with_leader(journals, founders, leader.uuid, policy);
    match session.open(client, 0).await {
        Ok(()) => {}
        Err(Interrupted::NotClaimed {
            outcome: ClaimOutcome::Lost { .. },
            ..
        }) => {
            moonpool_assertions::reachable!("coordinator: a lost install ended the term");
            return TermDuty::Refused;
        }
        Err(_) => return TermDuty::Unavailable,
    }
    let founder = |id: NodeId| session.founders().iter().any(|(f, _)| *f == id);
    let in_flight: Vec<Address> = session
        .registry()
        .nodes()
        .filter(|(id, n)| n.standing != NodeStanding::Retired && !founder(*id))
        .filter_map(|(_, n)| Address::parse(&n.addr).ok())
        .collect();
    let mut admitted = 0;
    for target in in_flight {
        if let FleetStep::Done {
            last: Some(Stage::Admit),
            ..
        } = session
            .admit_step(providers, rpc, client, names, 0, &target)
            .await
        {
            admitted += 1;
            moonpool_assertions::sometimes!(
                true,
                "coordinator: a successor finished an in-flight admission"
            );
        }
    }
    assert!(
        session.registry().nodes().count() >= admitted,
        "an admission finished is a registered machine"
    );
    TermDuty::Served { admitted }
}

/// The election's timing, from the driver's tunables.
#[must_use]
pub fn election_tunables(tunables: &DriverTunables) -> ElectionTunables {
    ElectionTunables {
        lease: tunables.election_lease,
        renew_every: tunables.election_renew,
        compact_after: tunables.election_compact_after,
    }
}

/// The library client of the cell's members, each dialed by its advertised
/// address, resolved at each call (#257).
fn cell_client<P: Providers>(providers: &P, rpc: &RpcHandle<P>, formed: &FormedCell) -> Client<P> {
    let servers = formed
        .plan
        .members
        .iter()
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::named(rpc, formed.facts.names.clone(), addr.clone()),
        })
        .collect();
    Client::new(providers, servers, ClientTunables::default())
}

/// A founding member's candidacy, as the node loop hands it to the task.
struct Candidacy {
    me: NodeId,
    addr: Address,
    names: Names,
    plan: CellPlan,
    seed: u128,
    tunables: ElectionTunables,
    /// The leader stops renewing for one lease and a half after its first
    /// term's duties (a BUGGIFY decision: the takeover's way in).
    stall: bool,
    /// The leader hands its first term on to another founding member after
    /// its duties (a BUGGIFY decision).
    hand_off: bool,
}

/// Start the candidacy of the founding member `formed` for the cell
/// coordinator, in a task that stops with `shutdown`. Draws its seed and its
/// BUGGIFY decisions here, on the node loop. Nothing starts when the plan
/// names no election journal or the tunables are not a working election.
pub(crate) fn spawn<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    formed: &FormedCell,
    tunables: &DriverTunables,
    observer: Option<Arc<dyn CallObserver>>,
    shutdown: CancellationToken,
) {
    let election = election_tunables(tunables);
    if !election.is_valid() || !formed.plan.election.is_set() {
        return;
    }
    let candidacy = Candidacy {
        me: formed.facts.node_id,
        addr: formed.facts.addr.clone(),
        names: formed.facts.names.clone(),
        plan: formed.plan.clone(),
        seed: providers.random().random(),
        tunables: election,
        stall: moonpool_buggify::buggify_with_prob!(0.3),
        hand_off: formed.plan.members.len() > 1 && moonpool_buggify::buggify_with_prob!(0.25),
    };
    let mut client = cell_client(providers, rpc, formed).with_shutdown(shutdown.clone());
    if let Some(observer) = observer {
        client = client.with_observer(observer);
    }
    let providers = providers.clone();
    let rpc = rpc.clone();
    providers
        .task()
        .spawn_task(
            "paros-cell-coordinator",
            campaign(providers.clone(), rpc, client, candidacy, shutdown),
        )
        .detach();
}

/// A jitter in `[0, lease / 2]` derived from `seed` and the step count.
fn jitter(seed: u128, step: u64, lease: Duration) -> Duration {
    let low = u64::try_from(seed & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(seed >> 64).unwrap_or(0);
    let mut x = low ^ high ^ step.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    let half = u64::try_from(lease.as_millis() / 2).unwrap_or(u64::MAX);
    Duration::from_millis(x % half.saturating_add(1))
}

/// The founding member after `me` in id order: the successor a hand-off
/// names.
fn successor(plan: &CellPlan, me: NodeId) -> Option<Candidate> {
    let others: Vec<NodeId> = plan
        .members
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| *id != me)
        .collect();
    others
        .iter()
        .find(|id| **id > me)
        .or_else(|| others.first())
        .map(|id| Candidate {
            id: id.0,
            interface: String::new(),
        })
}

/// The candidate's loop: step the election, serve every term it wins, and
/// stop with `shutdown`.
#[tracing::instrument(level = "debug", skip_all, fields(node = candidacy.me.0, cell = candidacy.plan.cell_id))]
async fn campaign<P: Providers>(
    providers: P,
    rpc: RpcHandle<P>,
    client: Client<P>,
    candidacy: Candidacy,
    shutdown: CancellationToken,
) {
    let Candidacy {
        me,
        addr,
        names,
        plan,
        seed,
        tunables,
        mut stall,
        hand_off: mut handing,
    } = candidacy;
    let journals = plan.control_journals();
    let founders = plan.members.clone();
    let policy = client.tunables().checkpoint_policy();
    let pace = tunables.renew_every / 4;
    let mut election = Election::new(
        client.clone(),
        plan.election,
        Candidate {
            id: me.0,
            interface: String::new(),
        },
        seed,
        tunables,
    );
    // The term whose duties are done.
    let mut served: Option<u64> = None;
    let mut steps = 0_u64;
    while !shutdown.is_cancelled() {
        steps += 1;
        match election.step(jitter(seed, steps, tunables.lease)).await {
            Step::Leading { leader, .. } if served != Some(leader.term) => {
                let duty = serve_term(
                    &providers,
                    &rpc,
                    &client,
                    &names,
                    (journals, founders.clone()),
                    &leader,
                    policy,
                )
                .await;
                match duty {
                    TermDuty::Served { .. } => {
                        served = Some(leader.term);
                        election.publish(addr.to_string());
                    }
                    TermDuty::Refused => {
                        election.resign(None).await;
                    }
                    TermDuty::Unavailable => {}
                }
            }
            Step::Leading { leader, .. } if handing => {
                handing = false;
                if let Some(next) = successor(&plan, me)
                    && let Some(uuid) = election.resign(Some(next)).await
                {
                    moonpool_assertions::reachable!(
                        "coordinator: a coordinator handed its term on"
                    );
                    hand_off(&client, journals.cell, leader.uuid, uuid, 0).await;
                }
            }
            Step::Leading { .. } if stall => {
                // A leader that stops calling: no renewal for longer than a
                // lease, so the other members take over.
                stall = false;
                moonpool_assertions::reachable!("coordinator: a coordinator skipped its renewals");
                let _ = providers
                    .time()
                    .sleep(tunables.lease + tunables.lease / 2)
                    .await;
            }
            Step::Deposed { .. } => {
                served = None;
                moonpool_assertions::reachable!(
                    "coordinator: a deposed coordinator stopped acting"
                );
            }
            Step::Leading { .. } | Step::Following { .. } => {}
        }
        let _ = providers.time().sleep(pace).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_jitter_stays_within_half_a_lease_and_moves_with_the_step() {
        let lease = Duration::from_millis(3000);
        let draws: Vec<Duration> = (0..64).map(|step| jitter(0xABCD, step, lease)).collect();
        assert!(draws.iter().all(|d| *d <= lease / 2));
        assert!(draws.windows(2).any(|w| w[0] != w[1]));
    }

    #[test]
    fn the_successor_is_the_next_founder_by_id_and_wraps() {
        let addr: Address = "10.0.0.1:1".parse().expect("an address");
        let plan = CellPlan {
            cell_id: 1,
            members: vec![
                (NodeId(3), addr.clone()),
                (NodeId(7), addr.clone()),
                (NodeId(9), addr),
            ],
            control: paros_core::JournalIdentifier::new(
                paros_core::TenantId(1),
                paros_core::JournalId(1),
            ),
            election: paros_core::JournalIdentifier::new(
                paros_core::TenantId(1),
                paros_core::JournalId(2),
            ),
            fleet: None,
            journals: Vec::new(),
        };
        assert_eq!(successor(&plan, NodeId(3)).map(|c| c.id), Some(7));
        assert_eq!(successor(&plan, NodeId(9)).map(|c| c.id), Some(3));
    }
}
