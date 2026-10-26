//! TLS on some seeds (decided on 2026-10-11): a dice rolled once at the
//! start of a seed turns TLS on for every runtime of the run, or leaves the
//! run on plaintext. On a TLS seed the run has one certificate authority,
//! drawn from the seed, and every process builds its transport with the
//! shipped `paros::net::tls` code: the machines, the matchmakers, the proxies,
//! the replicas, the joiners and the frontends serve and dial with a
//! machine's or a frontend's certificate; the workload's clients dial with
//! an admin's.
//!
//! **One mode per seed.** A TLS seed draws how its servers treat a caller
//! without a certificate (`ClientCertificates`): off, optional or required
//! (mutual TLS). Every serving process uses the seed's mode, as `parosd`
//! uses the one `PAROS_TLS_CLIENT_AUTH` of its deployment. Under `off` and
//! `optional` a client presents its certificate on a coin; under
//! `required` it always does.
//!
//! **Fresh certificates at every boot.** A process issues its leaf when it
//! boots, from a key drawn from the seed, so a reboot is also a
//! certificate rotation. The issuer's own fault site (`paros::net::tls::Flaw`:
//! short-lived, not yet valid, a name dropped, the wrong usage) fires as it
//! would in `parosctl tls`; the harness reissues a flawed leaf, as an
//! operator's deploy check would, so a run is never judged on a process
//! that cannot open a session. The flawed handshakes themselves are
//! moonpool's campaign (`sim-rpc-tls`).
//!
//! The clock that validates a certificate is [`EPOCH`] plus the simulated
//! time, as the frontend checks a token.

use std::sync::Arc;

use moonpool_sim::{SimContext, SimulationError, SimulationResult, StateHandle, assert_reachable};
use paros::Transport;
use paros::net::tls::{
    Authority, ClientCertificates, KeySeed, Material, ProviderUtc, Role, UtcTime, Validity,
};

use crate::frontend::EPOCH;
use crate::state::published_arc;

const TLS_KEY: &str = "paros-tls";

/// The universe every simulated identity names.
const UNIVERSE: &str = "sim";

/// How long the run's authority and every leaf stay valid: far beyond any
/// run, so expiry never ends a session in the harness.
const VALIDITY_SECS: u64 = 10 * 365 * 86_400;

/// How many times a flawed leaf is reissued before the run gives up.
const REISSUE_LIMIT: usize = 64;

/// A seed's TLS: its authority and its mode.
pub(crate) struct SeedTls {
    authority: Authority,
    callers: ClientCertificates,
}

/// A validity window that holds for the whole run.
fn validity() -> Validity {
    let start = UtcTime::from_unix_seconds(EPOCH.as_secs()).minus(86_400);
    Validity::starting(start, VALIDITY_SECS)
}

/// A key seed drawn from the run's randomness.
fn seed() -> KeySeed {
    let mut bytes = [0u8; 32];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&moonpool_sim::sim_random::<u64>().to_le_bytes());
    }
    KeySeed::Bytes(bytes)
}

/// The seed's TLS, rolled by whoever asks first: `None` on a plaintext
/// seed (about half of them).
pub(crate) fn seed_tls(state: &StateHandle) -> Arc<Option<SeedTls>> {
    published_arc(state, TLS_KEY, || {
        if !moonpool_sim::sim_random_bool(0.5) {
            return None;
        }
        let callers = ClientCertificates::ALL
            [moonpool_sim::sim_random_range(0..ClientCertificates::ALL.len())];
        let authority = Authority::new("sim authority", validity(), seed()).ok()?;
        tracing::info!(callers = %callers, "tls_seed");
        assert_reachable!("tls: a seed runs every session over TLS");
        match callers {
            ClientCertificates::Off => assert_reachable!("tls: servers ask for no certificate"),
            ClientCertificates::Optional => {
                assert_reachable!("tls: servers verify an optional certificate");
            }
            ClientCertificates::Required => assert_reachable!("tls: servers require mutual TLS"),
        }
        Some(SeedTls { authority, callers })
    })
}

impl SeedTls {
    /// A clean leaf for `name`, a `role`: a flawed one is reissued.
    fn material(&self, role: Role, name: &str) -> SimulationResult<Material> {
        for _ in 0..REISSUE_LIMIT {
            let spec = paros::net::tls::leaf(UNIVERSE, role, name, &[], validity(), seed());
            let issued = self
                .authority
                .issue(&spec)
                .map_err(|e| SimulationError::InvalidState(format!("tls leaf: {e}")))?;
            if issued.flaw.is_none() {
                return Ok(Material::issued(&issued, &self.authority));
            }
            assert_reachable!("tls: a flawed certificate is reissued");
        }
        Err(SimulationError::InvalidState(
            "tls: no clean certificate in the reissue budget".into(),
        ))
    }
}

fn clock(ctx: &SimContext) -> ProviderUtc<moonpool_sim::SimTimeProvider> {
    ProviderUtc::new(EPOCH, ctx.time().clone())
}

fn tls_error(error: impl std::fmt::Display) -> SimulationError {
    SimulationError::InvalidState(format!("tls: {error}"))
}

/// The transport of a serving process, `name` a `role` (a machine or a
/// frontend): plaintext on a plaintext seed.
pub(crate) fn serving(ctx: &SimContext, role: Role, name: &str) -> SimulationResult<Transport> {
    let tls = seed_tls(ctx.state());
    let Some(tls) = tls.as_ref() else {
        return Ok(Transport::Plaintext);
    };
    let material = tls.material(role, name)?;
    paros::net::tls::serving(&material, tls.callers, clock(ctx)).map_err(tls_error)
}

/// The transport of a workload's client: plaintext on a plaintext seed;
/// an admin's certificate always under mutual TLS, on a coin otherwise.
pub(crate) fn dialing(ctx: &SimContext, name: &str) -> SimulationResult<Transport> {
    let tls = seed_tls(ctx.state());
    let Some(tls) = tls.as_ref() else {
        return Ok(Transport::Plaintext);
    };
    let roots = [tls.authority.certificate().clone()];
    let present = tls.callers == ClientCertificates::Required || moonpool_sim::sim_random_bool(0.5);
    let certificate = if present {
        let material = tls.material(Role::Admin, name)?;
        Some((material.chain, material.key))
    } else {
        assert_reachable!("tls: a client dials without a certificate");
        None
    };
    paros::net::tls::dialing(&roots, certificate, clock(ctx)).map_err(tls_error)
}
