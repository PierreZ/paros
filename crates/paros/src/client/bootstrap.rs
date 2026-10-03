//! Bootstrap calls (#196, #216): forming a cell with `init`, and learning a
//! deployment's node ids from its addresses.
//!
//! A machine's id is random, minted at format (#225), so an operator knows
//! addresses — a rendezvous name, a join list — never ids. These calls
//! bridge the two, with the discipline of the rest of the client: one
//! bounded attempt per call, every outcome typed, no randomness (the seed
//! that runs `init` mints the cell's id).

use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use moonpool_rpc::{ErrorReason, RpcHandle};
use paros_core::{JournalId, JournalKey, NodeId, TenantId};

use super::Client;
use super::outcome::SetLeaderOutcome;
use crate::machine::{CELL_CONTROL, CellPlan};
use crate::rpc::machine as wire;
use crate::rpc::methods::{InitRpc, InspectRpc};
use crate::rpc::{InspectRequest, Read, well_known};

/// What one `Init` sent to a seed came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitOutcome {
    /// The seed formed the cell (or resumed a formation) over every seed.
    Formed(CellPlan),
    /// The seed answered, and refused: its label (`not_a_seed`,
    /// `seed_unreachable`, `other_cell`, `cell_exists`, `stateless_seed`,
    /// `storage`).
    Refused(String),
    /// The seed answered with a plan that does not decode.
    Malformed,
    /// A machine answered there without the machine endpoint: it serves a
    /// cell already.
    NotWaiting,
    /// Nothing decided within the patience: the machine, or another seed,
    /// is not up yet, or the answer was lost (`Init` is resumable: send it
    /// again).
    Unreachable,
}

/// How long one `Init` attempt waits before it is sent again.
const INIT_ATTEMPT: Duration = Duration::from_secs(10);
/// The pause between two `Init` attempts.
const INIT_RETRY: Duration = Duration::from_millis(500);

/// Send `Init` to the waiting seed at `seed`, again while nothing answers
/// or the seed finds another seed not up yet (`seed_unreachable`: a machine
/// still starting), for up to `patience`; then [`InitOutcome::Unreachable`]. Each attempt waits up
/// to [`INIT_ATTEMPT`]: the seed calls every other seed before it answers.
/// Re-sending is safe: a seed resumes the plan it recorded, never redraws.
pub async fn init<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    seed: SocketAddr,
    patience: Duration,
) -> InitOutcome {
    let time = providers.time();
    let deadline = time.now() + patience;
    let client = well_known::<P, InitRpc>(rpc, seed);
    loop {
        let remaining = deadline.saturating_sub(time.now());
        let reply = time
            .timeout(
                remaining.min(INIT_ATTEMPT),
                client.try_get_reply(&wire::Init {}),
            )
            .await;
        match reply {
            Ok(Ok(ack)) if ack.initialized => {
                return CellPlan::from_wire(ack.cell_id, &ack.members, &ack.journals)
                    .map_or(InitOutcome::Malformed, InitOutcome::Formed);
            }
            // Another seed is not up yet: nothing was decided, and `init`
            // resumes, so ask again — the seeds of a fresh deployment start
            // in any order.
            Ok(Ok(ack)) if ack.refusal == "seed_unreachable" => {}
            Ok(Ok(ack)) => return InitOutcome::Refused(ack.refusal),
            Ok(Err(error)) if *error.reason() == ErrorReason::EndpointNotFound => {
                return InitOutcome::NotWaiting;
            }
            _ => {}
        }
        if time.now() >= deadline || time.sleep(INIT_RETRY).await.is_err() {
            return InitOutcome::Unreachable;
        }
    }
}

/// Learn the node id of every server in `addrs` from its own `Inspect`
/// (`node`), each asked once within `timeout`. A server that does not
/// answer is left out; the order of `addrs` is kept.
pub async fn discover<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    addrs: &[SocketAddr],
    timeout: Duration,
) -> Vec<(u64, SocketAddr)> {
    let mut found = Vec::with_capacity(addrs.len());
    for &addr in addrs {
        let client = well_known::<P, InspectRpc>(rpc, addr);
        let request = InspectRequest {
            journal: JournalKey::UNSET.journal.0,
            tenant: JournalKey::UNSET.tenant.0,
        };
        if let Ok(Ok(reply)) = providers
            .time()
            .timeout(timeout, client.try_get_reply(&request))
            .await
            && reply.node != 0
        {
            found.push((reply.node, addr));
        }
    }
    found
}

/// What claiming a formed cell's control journal came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimCellOutcome {
    /// The coordinator now owns the cell control journal: `init` is done.
    Claimed {
        /// The generation it owns.
        generation: u64,
    },
    /// The cell control journal already has an owner: the cell was
    /// initialized before.
    AlreadyInitialized {
        /// Its owner.
        owner: Option<u64>,
        /// Its generation.
        generation: u64,
    },
    /// No server confirmed the journal's state or decided the claim.
    Unavailable,
    /// The claim's answer never came: it may have won. Re-run `init`.
    Ambiguous,
}

/// The cell step's last move (`docs/architecture.md` §3.1): the first cell
/// coordinator `coordinator` claims the cell control journal with
/// `SetLeader(expected_gen = 0)`, through `client` (the cell's members). A
/// journal that already has an owner was initialized before. A freshly
/// formed cell is still electing its first leader, so an attempt that finds
/// no server to confirm the state is retried, `retry_backoff` apart, for up
/// to `patience`.
pub async fn claim_cell<P: Providers>(
    client: &Client<P>,
    coordinator: NodeId,
    patience: Duration,
) -> ClaimCellOutcome {
    let deadline = client.time.now() + patience;
    loop {
        let outcome = claim_cell_once(client, coordinator).await;
        if outcome != ClaimCellOutcome::Unavailable
            || client.time.now() >= deadline
            || !client.pause(client.tunables.retry_backoff).await
        {
            return outcome;
        }
    }
}

async fn claim_cell_once<P: Providers>(
    client: &Client<P>,
    coordinator: NodeId,
) -> ClaimCellOutcome {
    let read = Read {
        journal: CELL_CONTROL.journal.0,
        tenant: CELL_CONTROL.tenant.0,
        from_seq: 0,
        limit: 1,
        wait_ms: 0,
    };
    let Some(state) = client.read_any(&read, 0).await.outcome.state() else {
        return ClaimCellOutcome::Unavailable;
    };
    if state.generation.0 > 0 {
        return ClaimCellOutcome::AlreadyInitialized {
            owner: state.owner.map(|o| o.0),
            generation: state.generation.0,
        };
    }
    let first = client.leader().unwrap_or(0);
    match client
        .set_leader(CELL_CONTROL, 0, coordinator.0, first)
        .await
    {
        SetLeaderOutcome::Won { state } => ClaimCellOutcome::Claimed {
            generation: state.generation.0,
        },
        SetLeaderOutcome::Lost { state } => ClaimCellOutcome::AlreadyInitialized {
            owner: state.owner.map(|o| o.0),
            generation: state.generation.0,
        },
        SetLeaderOutcome::Ambiguous | SetLeaderOutcome::Malformed => ClaimCellOutcome::Ambiguous,
        SetLeaderOutcome::Redirect { .. } | SetLeaderOutcome::UnknownJournal => {
            ClaimCellOutcome::Unavailable
        }
    }
}

/// The default user journal a toy cell serves from formation (`256/256`):
/// the static assignment that stands in for placement until #212.
pub const TOY_JOURNAL: JournalKey = JournalKey::new(TenantId::FIRST_USER, JournalId::FIRST_USER);
