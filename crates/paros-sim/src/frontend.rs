//! The frontends (#192 (the frontend)): the shipped
//! `paros::frontend::run_frontend` in front of the machines, with the
//! Biscuit authorization `paros-frontend` ships
//! (`paros_authz_biscuit::BiscuitAuthz`, #245), and the passes the
//! workload's clients present to them.
//!
//! A frontend is stateless: it fronts the cell's founding members, learns
//! the cell from them, and caches its resolutions. Its group's attrition
//! kills it like any process; it comes back with nothing cached. Its own
//! choices (a cold cache, a stale resolution answered as unknown, a call
//! routed to another server than the known leader) are inline BUGGIFY sites
//! in `paros`.
//!
//! The run's root keys are drawn per seed (#245 (Biscuit tokens)): every
//! token is signed with one key, the one `init` writes in the universe
//! entry and every frontend pins. A second key is rotated in beside it on a
//! BUGGIFY location (`parosctl universe key add`), so a frontend trusts it
//! only once it read the universe entry again. A third key is added and
//! removed, and a fourth is on no list. The clock a token's expiry is
//! checked against is [`EPOCH`] plus the simulated time, on the frontend,
//! on a pinned machine and on the client that mints.
//!
//! On half the seeds every machine pins the signing key ([`machine_pin`]):
//! `init` and `cell add-machine` then present an `admin` token.
//!
//! A frontend's clock may be skewed from the minter's by up to
//! [`MAX_SKEW`] either way (#450, a knob per frontend boot), so a token
//! near its expiry ([`Presented::ShortLived`]) is judged by the frontend's
//! own clock; the client judges the answer against that bound.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationError, SimulationResult, StateHandle, TimeProvider,
    assert_always, assert_reachable,
};
use paros::client::ClientTunables;
use paros::fleet::RootKeyEntry;
use paros::frontend::{FrontendSettings, run_frontend};
use paros::machine::AdminPin;
use paros::name::JournalName;
use paros::{Address, Audit, DriverTunables, JournalIdentifier};
use paros_authz_biscuit::{
    BiscuitAuthz, Class, Entropy, Grant, KeyRing, MAX_BLOCKS, Restriction, Role, RootKey, Token,
    derive, mint, seal,
};

use crate::process::dispatch;
use crate::roles::{FRONTEND_GROUP, Role as ProcessRole};

/// The port every simulated frontend binds.
const FRONTEND_PORT: u16 = 4600;

/// The wall clock at the simulation's zero, since the Unix epoch: a fixed
/// instant, so a token's expiry depends on the simulated time alone.
pub(crate) const EPOCH: Duration = Duration::from_secs(1_790_000_000);

/// How long a token the workload mints stays valid.
const TOKEN_LIFE: Duration = Duration::from_secs(3_600);

/// The most a frontend's clock is skewed from the minter's, either way
/// (#450): the extreme of its `frontend_clock_skew_ms` knob. A token's
/// expiry is judged against it, never buggified.
pub(crate) const MAX_SKEW: Duration = Duration::from_millis(2_000);

/// How far a short-lived token's expiry lies from its minting, either way:
/// beyond [`MAX_SKEW`], so some are expired for every frontend, some valid
/// for every frontend, and some judged by the skew alone.
const SHORT_SPREAD_MS: u64 = 6_000;

const _: () = assert!(
    SHORT_SPREAD_MS as u128 > MAX_SKEW.as_millis(),
    "a short-lived token's spread passes the skew"
);

/// The expiry of a short-lived token minted at `elapsed` (#450): drawn by
/// `draw` within [`SHORT_SPREAD_MS`] of it, either way.
pub(crate) fn short_expiry(elapsed: Duration, draw: u64) -> Duration {
    let spread = Duration::from_millis(SHORT_SPREAD_MS);
    let offset = Duration::from_millis(draw % (2 * SHORT_SPREAD_MS + 1));
    (elapsed + spread).saturating_sub(offset)
}

/// How long a frontend waits for one machine's answer. Below the
/// workload's own timeout, so a frontend that moves on to the next server
/// answers before its caller gives up.
const FORWARD_TIMEOUT: Duration = Duration::from_millis(600);

const KEYS_KEY: &str = "paros-frontend-keys";

/// The run's root keys (#245).
pub(crate) struct RootKeys {
    /// The key every token is signed with: the one `init` writes and every
    /// frontend and pinned machine pins.
    signing: RootKey,
    /// A key rotated in beside it on a BUGGIFY location: it signs a token
    /// on its own coin.
    rotated: RootKey,
    /// A key added and then removed: it signs no token.
    spare: RootKey,
    /// A key on no list.
    unknown: RootKey,
    /// Whether every machine pins the signing key.
    pinned: bool,
    /// Whether an operator ever tried to add the rotated key: before that,
    /// no token it signs is served.
    rotation: AtomicBool,
}

fn entropy() -> Entropy {
    let word = || moonpool_sim::sim_random_range(0_u64..u64::MAX);
    Entropy::from_words([word(), word(), word(), word()])
}

/// The run's root keys, drawn once per seed.
fn root_keys(state: &StateHandle) -> Arc<RootKeys> {
    crate::state::published_arc(state, KEYS_KEY, || RootKeys {
        signing: RootKey::generate("current", &entropy()),
        rotated: RootKey::generate("next", &entropy()),
        spare: RootKey::generate("spare", &entropy()),
        unknown: RootKey::generate("stranger", &entropy()),
        pinned: moonpool_sim::sim_random_range(0_u32..2) == 0,
        rotation: AtomicBool::new(false),
    })
}

/// The universe entry's record of `key`.
fn entry(key: &RootKey) -> RootKeyEntry {
    let public = key.public();
    RootKeyEntry {
        key_id: public.key_id(),
        label: public.label().to_string(),
        public_key: public.key_text(),
    }
}

/// The key `init` writes in the universe entry: the signing key.
pub(crate) fn signing_entry(state: &StateHandle) -> RootKeyEntry {
    entry(&root_keys(state).signing)
}

/// The key a rotation adds.
pub(crate) fn rotated_entry(state: &StateHandle) -> RootKeyEntry {
    entry(&root_keys(state).rotated)
}

/// The key an operator adds and removes.
pub(crate) fn spare_entry(state: &StateHandle) -> RootKeyEntry {
    entry(&root_keys(state).spare)
}

/// Note that an operator is about to add the rotated key: from now on a
/// token it signs may be served.
pub(crate) fn note_rotation(state: &StateHandle) {
    root_keys(state).rotation.store(true, Ordering::SeqCst);
}

/// Whether an operator ever tried to add the rotated key.
pub(crate) fn rotation_tried(state: &StateHandle) -> bool {
    root_keys(state).rotation.load(Ordering::SeqCst)
}

/// Whether every machine of the run pins the signing key.
pub(crate) fn machines_pinned(state: &StateHandle) -> bool {
    root_keys(state).pinned
}

/// A machine's bootstrap pin (#245): the signing key on a pinned seed,
/// open on the others.
pub(crate) fn machine_pin(state: &StateHandle) -> AdminPin {
    let keys = root_keys(state);
    if !keys.pinned {
        return AdminPin::open();
    }
    match KeyRing::new([keys.signing.public()]) {
        Ok(ring) => AdminPin::new(Arc::new(BiscuitAuthz::new(ring)), EPOCH),
        Err(_) => AdminPin::open(),
    }
}

/// The token an operator presents for `init` and `cell add-machine`: an
/// `admin` token on a pinned seed, none on the others (a pin-less machine
/// reads none).
pub(crate) fn operator_token(state: &StateHandle, elapsed: Duration) -> Vec<u8> {
    if machines_pinned(state) {
        token(state, "", "", Presented::Admin, elapsed)
    } else {
        Vec::new()
    }
}

/// The wall clock `elapsed` after the simulation's zero.
fn wall(elapsed: Duration) -> SystemTime {
    SystemTime::UNIX_EPOCH + EPOCH + elapsed
}

/// The frontends' addresses, in rank order: what a client is configured
/// with.
pub(crate) fn frontend_addrs(deployment: &crate::roles::Deployment) -> Vec<Address> {
    deployment
        .frontends()
        .iter()
        .filter_map(|ip| Address::parse(&format!("{ip}:{FRONTEND_PORT}")).ok())
        .collect()
}

/// The token a client presents: its kind, as the workload draws it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Presented {
    /// The tenant's own token, signed with the current key.
    Own,
    /// The tenant's own token, signed with the key rotated in.
    Rotated,
    /// Another tenant's token.
    OtherTenant,
    /// The tenant's own token, past its expiry.
    Expired,
    /// The tenant's own token, signed with a key no ring holds.
    UnknownKey,
    /// Bytes that are no token.
    Garbage,
    /// An `admin` token.
    Admin,
    /// The tenant's own token, derived read-only (#400).
    ReadOnly,
    /// The tenant's own token, derived to the one journal it names.
    OwnJournal,
    /// The tenant's own token, derived to another journal.
    OtherJournal,
    /// The tenant's own token, sealed: nothing derives from it (#450).
    Sealed,
    /// The tenant's own token, derived to the journal class only: a data
    /// call is denied, a tag served (#450).
    ClassJournal,
    /// The tenant's own token, derived to its own tenant: served (#450).
    TenantBlock,
    /// The tenant's own token, derived to another tenant (#450).
    ForeignBlock,
    /// The tenant's own token, derived with an expiry already past (#450).
    EarlierExpiry,
    /// The tenant's own token, derived past `MAX_BLOCKS` blocks: refused
    /// before any check (#450).
    DeepChain,
    /// The tenant's own token, expiring near its minting, either way: the
    /// frontend's clock decides (#450).
    ShortLived,
}

impl Presented {
    /// The kind `draw` picks: the tenant's own token most of the time.
    pub(crate) fn drawn(draw: u64) -> Self {
        match draw % 20 {
            0 => Self::OtherTenant,
            1 => Self::Expired,
            2 => Self::UnknownKey,
            3 => Self::Garbage,
            4 => Self::Rotated,
            5 => Self::Admin,
            6 => Self::ReadOnly,
            7 => Self::OwnJournal,
            8 => Self::OtherJournal,
            9 => Self::Sealed,
            10 => Self::ClassJournal,
            11 => Self::TenantBlock,
            12 => Self::ForeignBlock,
            13 => Self::EarlierExpiry,
            14 => Self::DeepChain,
            15 => Self::ShortLived,
            _ => Self::Own,
        }
    }

    /// Whether a frontend may refuse it as `invalid_token` though it is
    /// within its rights: a token the rotated key signed, while the frontend
    /// has not read that key in the universe entry yet.
    pub(crate) fn may_be_unknown(self) -> bool {
        self == Self::Rotated
    }

    /// Whether a frontend may serve it at all, now: a token the rotated key
    /// signed only once an operator tried to add the key.
    pub(crate) fn may_be_served(self, state: &StateHandle) -> bool {
        self != Self::Rotated || rotation_tried(state)
    }
}

/// The token of kind `presented` for `tenant` (a derived one names
/// `journal`), minted at `elapsed` after the simulation's zero.
pub(crate) fn token(
    state: &StateHandle,
    tenant: &str,
    journal: &str,
    presented: Presented,
    elapsed: Duration,
) -> Vec<u8> {
    token_expiring(state, (tenant, journal), presented, elapsed, None)
}

/// The derived block of `presented` for `tenant` and `journal`; `None` for
/// a token derived from nothing.
fn restriction(
    presented: Presented,
    (tenant, journal): (&str, &str),
    now: SystemTime,
) -> Option<Restriction> {
    let restriction = match presented {
        Presented::ReadOnly => Restriction {
            read_only: true,
            ..Restriction::default()
        },
        Presented::OwnJournal => Restriction {
            journal: Some(journal.into()),
            ..Restriction::default()
        },
        Presented::OtherJournal => Restriction {
            journal: Some(format!("{journal}-other")),
            ..Restriction::default()
        },
        Presented::ClassJournal => Restriction {
            classes: Some(vec![Class::Journal]),
            ..Restriction::default()
        },
        Presented::TenantBlock | Presented::DeepChain => Restriction {
            tenant: Some(tenant.into()),
            ..Restriction::default()
        },
        Presented::ForeignBlock => Restriction {
            tenant: Some(format!("{tenant}-other")),
            ..Restriction::default()
        },
        // Past for every frontend's clock, however skewed.
        Presented::EarlierExpiry => Restriction {
            expires: Some(now.checked_sub(MAX_SKEW * 3).unwrap_or(now)),
            ..Restriction::default()
        },
        _ => return None,
    };
    Some(restriction)
}

/// [`token`], with the expiry of a [`Presented::ShortLived`] token:
/// `expires` after the simulation's zero.
pub(crate) fn token_expiring(
    state: &StateHandle,
    (tenant, journal): (&str, &str),
    presented: Presented,
    elapsed: Duration,
    expires: Option<Duration>,
) -> Vec<u8> {
    let keys = root_keys(state);
    let now = wall(elapsed);
    let derived = restriction(presented, (tenant, journal), now);
    let (key, role, minted) = match presented {
        Presented::Own
        | Presented::ReadOnly
        | Presented::OwnJournal
        | Presented::OtherJournal
        | Presented::Sealed
        | Presented::ClassJournal
        | Presented::TenantBlock
        | Presented::ForeignBlock
        | Presented::EarlierExpiry
        | Presented::DeepChain => (&keys.signing, Role::Tenant(tenant.into()), now),
        // Minted well before its earliest expiry, so the expiry is after
        // its minting.
        Presented::ShortLived => (
            &keys.signing,
            Role::Tenant(tenant.into()),
            now.checked_sub(Duration::from_millis(SHORT_SPREAD_MS) * 2)
                .unwrap_or(now),
        ),
        Presented::Rotated => (&keys.rotated, Role::Tenant(tenant.into()), now),
        Presented::OtherTenant => (&keys.signing, Role::Tenant(format!("{tenant}-other")), now),
        Presented::Expired => (
            &keys.signing,
            Role::Tenant(tenant.into()),
            now.checked_sub(TOKEN_LIFE * 2).unwrap_or(now),
        ),
        Presented::UnknownKey => (&keys.unknown, Role::Tenant(tenant.into()), now),
        Presented::Admin => (&keys.signing, Role::Admin, now),
        Presented::Garbage => return b"not a token".to_vec(),
    };
    let grant = Grant {
        role,
        subject: "sim".into(),
        expires: expires.map_or(minted + TOKEN_LIFE, wall),
    };
    let minted = mint(key, &grant, minted, &entropy()).and_then(|token| match &derived {
        // Past `MAX_BLOCKS` blocks: the authority block and one more
        // derived block than the limit allows.
        Some(block) if presented == Presented::DeepChain => {
            (0..MAX_BLOCKS).try_fold(token, |token, _| derive(&token, block, &entropy()))
        }
        Some(block) => derive(&token, block, &entropy()),
        None if presented == Presented::Sealed => {
            let sealed = seal(&token)?;
            assert_always!(
                derive(
                    &sealed,
                    &Restriction {
                        read_only: true,
                        ..Restriction::default()
                    },
                    &entropy()
                )
                .is_err(),
                "frontend: nothing derives from a sealed token"
            );
            Ok(sealed)
        }
        None => Ok(token),
    });
    minted.map_or_else(
        |error| {
            assert_always!(
                false,
                "frontend: the workload mints every token it draws",
                { "error" => error.to_string() }
            );
            Vec::new()
        },
        |token: Token| token.as_bytes().to_vec(),
    )
}

/// A frontend in the simulation.
pub(crate) struct FrontendProcess;

impl FrontendProcess {
    pub(crate) fn chaotic() -> Self {
        Self
    }
}

#[async_trait]
impl Process for FrontendProcess {
    fn name(&self) -> &'static str {
        FRONTEND_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        dispatch(
            ctx,
            "every frontend process is mapped to the frontend role",
            "a frontend",
            |role| match role {
                ProcessRole::Frontend(rank) => Some(rank),
                _ => None,
            },
            |deployment, rank, my_ip| async move {
                Box::pin(run_frontend_role(ctx, &deployment, rank, &my_ip)).await
            },
        )
        .await
    }
}

/// Run the shipped frontend at `my_ip`, in front of the founding members.
async fn run_frontend_role(
    ctx: &SimContext,
    deployment: &crate::roles::Deployment,
    rank: usize,
    my_ip: &str,
) -> SimulationResult<()> {
    let ip: std::net::IpAddr = my_ip
        .parse()
        .map_err(|e| SimulationError::InvalidState(format!("bad frontend ip {my_ip}: {e}")))?;
    let layout = crate::shape::machine_layout(ctx.state(), deployment.machines().len());
    let addrs = crate::machine::machine_addrs(ctx.state(), deployment)?;
    let cell: Vec<Address> = addrs.into_iter().take(layout.founders).collect();
    assert_always!(
        !cell.is_empty(),
        "frontend: a frontend fronts a founding member"
    );
    let keys = root_keys(ctx.state());
    // #450: this frontend's clock is skewed from the minter's, a knob drawn
    // at each boot (default none; extreme up to `MAX_SKEW`, either way).
    let skew = Duration::from_millis(moonpool_sim::buggify_knob!(
        0_u64,
        1_u64..u64::try_from(MAX_SKEW.as_millis()).unwrap_or(0) + 1
    ));
    let epoch = if moonpool_sim::sim_random_range(0_u32..2) == 0 {
        EPOCH + skew
    } else {
        EPOCH.saturating_sub(skew)
    };
    if !skew.is_zero() {
        assert_reachable!("frontend: a frontend runs with a skewed clock");
    }
    // The pin: the key `init` writes. A key rotated in later is trusted
    // once the frontend read the universe entry again (#245).
    let ring = KeyRing::new([keys.signing.public()])
        .map_err(|e| SimulationError::InvalidState(format!("key ring: {e}")))?;
    let settings = FrontendSettings {
        listen: SocketAddr::new(ip, FRONTEND_PORT),
        cell,
        names: crate::machine::names(ctx.state()),
        epoch,
        tunables: DriverTunables::default(),
        transport: crate::tls::serving(ctx, paros::tls::Role::Frontend, my_ip)?,
        client: ClientTunables {
            request_timeout: FORWARD_TIMEOUT,
            read_timeout: FORWARD_TIMEOUT,
            ..ClientTunables::default()
        },
    };
    let audit = FrontendAudit {
        state: ctx.state().clone(),
        calls: Arc::new(crate::chain_workload::system::Announce::of_machine(
            ctx.state(),
            ctx.time().clone(),
            FRONTEND_CLIENT_BASE + rank as u64,
        )),
    };
    tracing::info!(ip = %ip, at = ?ctx.time().now(), "frontend_booting");
    run_frontend(
        ctx.providers().clone(),
        settings,
        Arc::new(BiscuitAuthz::new(ring)),
        audit,
        ctx.shutdown().clone(),
    )
    .await
}

/// The first client id a frontend's forwarded calls log under (#192 (the
/// frontend)): above the machines' own clients (`1 << 32` on).
const FRONTEND_CLIENT_BASE: u64 = 1 << 33;

/// What a frontend reports: each resolution, judged against the tenant
/// board (the control journal as the machines folded it), and each call it
/// forwards, which joins its journal's appended set and history as any
/// client's does.
#[derive(Clone)]
struct FrontendAudit {
    state: StateHandle,
    calls: Arc<crate::chain_workload::system::Announce>,
}

impl Audit for FrontendAudit {
    fn call_observer(&self) -> Option<Arc<dyn paros::client::CallObserver>> {
        Some(self.calls.clone())
    }

    fn frontend_resolved(
        &self,
        name: &JournalName,
        journal: JournalIdentifier,
        control: JournalIdentifier,
        at: u64,
    ) {
        assert_always!(
            journal.tenant == control.tenant,
            "frontend: a name resolves inside its tenant",
            { "journal" => journal.to_string() }
        );
        let board = crate::audit::tenants::tenant_board(&self.state);
        let recorded =
            crate::audit::tenants::lock(&board).named_at(control, name.journal().as_bytes(), at);
        if let Ok(recorded) = recorded {
            assert_always!(
                recorded == Some(journal.journal),
                "frontend: a resolved name is the journal its directory records there",
                {
                    "at" => at,
                    "resolved" => journal.journal.0,
                    "recorded" => recorded.map_or(0, |j| j.0)
                }
            );
            assert_reachable!("frontend: a resolution is judged against the directory");
        }
    }

    fn frontend_unknown(&self, name: &JournalName, control: JournalIdentifier, at: u64) {
        let board = crate::audit::tenants::tenant_board(&self.state);
        let recorded =
            crate::audit::tenants::lock(&board).named_at(control, name.journal().as_bytes(), at);
        if let Ok(recorded) = recorded {
            assert_always!(
                recorded.is_none(),
                "frontend: a name is answered unknown only where its directory holds no journal",
                { "at" => at, "recorded" => recorded.map_or(0, |j| j.0) }
            );
            assert_reachable!("frontend: an unknown name is judged against the directory");
        }
    }
}
