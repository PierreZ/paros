//! The connection every online command shares: the options every command
//! takes ([`Global`]), the client-only RPC runtime ([`Runtime`]), the
//! library client of the servers or the frontends, and how a command ends
//! ([`Ending`]).

use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use clap::Args;
use moonpool_core::{CompioProviders, CompioResolver};
use moonpool_rpc::{RpcConfig, RpcHandle};
use paros::client::{Client, ClientTunables, bootstrap};
use paros::model::address::{Address, Names};
use paros::net::resolve;
use paros::net::tls::{Files, Pem};

use crate::support::{names, view};

/// Options every command takes.
#[derive(Args, Debug)]
pub struct Global {
    /// The servers to ask, comma-separated: `HOST:PORT` — a name that
    /// resolves to several machines (a Compose alias) stands for
    /// them all, and each server's node id is learned from its own
    /// `Inspect` — or `ID=HOST:PORT` to name the id outright (its 16 hex
    /// digits). A host is an IP or a name; a name of one machine is
    /// resolved each time it is dialed (#257).
    #[arg(long, env = "PAROSCTL_SERVERS", value_delimiter = ',', global = true)]
    pub servers: Vec<ServerArg>,
    /// The frontends to send the journal calls (`write`, `read`, `tail`,
    /// `truncate`, `set-leader`, `journal tag`) through, comma-separated `HOST:PORT`
    /// (#192 (the frontend)): each call carries the token (`--token`) and
    /// names its journal by name. The other commands still ask the
    /// servers.
    #[arg(long, env = "PAROSCTL_FRONTENDS", value_delimiter = ',', global = true)]
    pub frontends: Vec<String>,
    /// Print JSON, one document per answer, instead of text.
    #[arg(long, global = true)]
    pub json: bool,
    /// How long one call waits for its answer, in milliseconds.
    #[arg(long, default_value = "5000", global = true)]
    pub timeout_ms: u64,
    /// Ask a view as tenant `NAME`, not as an admin (#399): the cell shows
    /// that tenant's own spread only. The machines answer views without a
    /// token (the network is the trust boundary, §3.5), so every caller may
    /// narrow itself so.
    #[arg(long, value_name = "NAME", global = true)]
    pub as_tenant: Option<String>,
    /// The universe's authority, PEM (`parosctl tls`): every session is
    /// TLS, and a server must prove a certificate it signed.
    #[arg(long, env = "PAROSCTL_TLS_CA", global = true)]
    pub tls_ca: Option<std::path::PathBuf>,
    /// The certificate to present to the servers, PEM (mutual TLS): an
    /// `admin` one from `parosctl tls init`.
    #[arg(long, env = "PAROSCTL_TLS_CERT", global = true)]
    pub tls_cert: Option<std::path::PathBuf>,
    /// The certificate's private key, PEM.
    #[arg(long, env = "PAROSCTL_TLS_KEY", global = true)]
    pub tls_key: Option<std::path::PathBuf>,
    /// The authority, the PEM text itself instead of a file.
    #[arg(
        long,
        env = "PAROSCTL_TLS_CA_PEM",
        hide_env_values = true,
        allow_hyphen_values = true,
        global = true,
        conflicts_with = "tls_ca"
    )]
    pub tls_ca_pem: Option<String>,
    /// The certificate, the PEM text itself instead of a file.
    #[arg(
        long,
        env = "PAROSCTL_TLS_CERT_PEM",
        hide_env_values = true,
        allow_hyphen_values = true,
        global = true,
        conflicts_with = "tls_cert"
    )]
    pub tls_cert_pem: Option<String>,
    /// The certificate's private key, the PEM text itself instead of a file.
    #[arg(
        long,
        env = "PAROSCTL_TLS_KEY_PEM",
        hide_env_values = true,
        allow_hyphen_values = true,
        global = true,
        conflicts_with = "tls_key"
    )]
    pub tls_key_pem: Option<String>,
    /// The Biscuit token every call presents, base64url (#245 (Biscuit
    /// tokens)): a tenant or `admin` token through the frontends, and the
    /// `admin` token `init` and `cell add-machine` send to an idle machine
    /// that pins a root key. Given before the command.
    #[arg(long, env = "PAROS_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
    /// A file that holds the token, instead of `--token`.
    #[arg(long, env = "PAROS_TOKEN_FILE", conflicts_with = "token")]
    pub token_file: Option<std::path::PathBuf>,
}

/// One server entry: an explicit node id, or the addresses a name stands
/// for.
#[derive(Clone, Debug)]
pub struct ServerArg {
    pub id: Option<u64>,
    pub addrs: Vec<Address>,
}

impl FromStr for ServerArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('=') {
            Some((id, addr)) => Ok(Self {
                id: Some(
                    names::parse_node_id(id.trim()).map_err(|e| format!("bad id in {s:?}: {e}"))?,
                ),
                addrs: vec![
                    Address::parse(addr.trim())
                        .map_err(|e| format!("bad address in {s:?}: {e}"))?,
                ],
            }),
            None => Ok(Self {
                id: None,
                addrs: resolve::expand(s.trim())
                    .map_err(|e| format!("bad address in {s:?}: {e}"))?,
            }),
        }
    }
}

impl Global {
    /// Every address named, in order, without duplicates.
    pub fn addrs(&self) -> Vec<Address> {
        let mut addrs = Vec::new();
        for addr in self.servers.iter().flat_map(|s| s.addrs.iter().cloned()) {
            if !addrs.contains(&addr) {
                addrs.push(addr);
            }
        }
        addrs
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// The token every call presents (#245): `--token`, else
    /// `--token-file`, else none.
    pub fn token(&self) -> Result<Option<Vec<u8>>, String> {
        let text = match (&self.token, &self.token_file) {
            (Some(text), _) => text.clone(),
            (None, Some(path)) => {
                std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?
            }
            (None, None) => return Ok(None),
        };
        let token = paros_authz_biscuit::Token::from_text(&text).map_err(|e| e.to_string())?;
        Ok(Some(token.as_bytes().to_vec()))
    }
}

/// How a command ended, as an exit code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ending {
    /// Done.
    Success,
    /// Answered, and not what was asked.
    Refused,
    /// May or may not have happened.
    Ambiguous,
    /// Nobody answered: nothing happened.
    Unreachable,
}

impl From<Ending> for ExitCode {
    fn from(ending: Ending) -> Self {
        match ending {
            Ending::Success => ExitCode::SUCCESS,
            Ending::Refused => ExitCode::from(3),
            Ending::Ambiguous => ExitCode::from(4),
            Ending::Unreachable => ExitCode::from(5),
        }
    }
}

/// The client-only RPC runtime, driven on a task of its own for the life
/// of the process.
pub struct Runtime {
    pub providers: CompioProviders,
    pub rpc: RpcHandle<CompioProviders>,
    /// The operating system's resolver: names are resolved as they are
    /// dialed (#257).
    pub names: Names,
}

pub fn runtime(global: &Global) -> Result<Runtime, String> {
    let config = RpcConfig {
        max_frame_bytes: paros::tunables::MAX_FRAME_BYTES,
        ..RpcConfig::default()
    };
    let transport = Files {
        cert: Pem::pick(
            ("PAROSCTL_TLS_CERT", global.tls_cert.as_deref()),
            ("PAROSCTL_TLS_CERT_PEM", global.tls_cert_pem.as_deref()),
        ),
        key: Pem::pick(
            ("PAROSCTL_TLS_KEY", global.tls_key.as_deref()),
            ("PAROSCTL_TLS_KEY_PEM", global.tls_key_pem.as_deref()),
        ),
        ca: Pem::pick(
            ("PAROSCTL_TLS_CA", global.tls_ca.as_deref()),
            ("PAROSCTL_TLS_CA_PEM", global.tls_ca_pem.as_deref()),
        ),
    }
    .dialing()?;
    let providers = CompioProviders::new();
    let (run, rpc) = transport
        .client_only(providers.clone(), config)
        .map_err(|e| format!("client RPC runtime: {e}"))?;
    compio::runtime::spawn(async move {
        let error = run.await;
        tracing::warn!(%error, "client RPC runtime failed");
    })
    .detach();
    Ok(Runtime {
        providers,
        rpc,
        names: Names::new(CompioResolver::new()),
    })
}

/// The library client of `servers` (id and address each).
pub fn client(
    runtime: &Runtime,
    servers: &[(u64, Address)],
    timeout: Duration,
) -> Client<CompioProviders> {
    Client::connect_named(
        &runtime.providers,
        &runtime.rpc,
        &runtime.names,
        servers,
        tunables(timeout),
    )
}

/// The client's tunables: `timeout` bounds each request and each read.
fn tunables(timeout: Duration) -> ClientTunables {
    ClientTunables {
        request_timeout: timeout,
        read_timeout: timeout,
        ..ClientTunables::default()
    }
}

/// A client of the frontends `global` names (#192 (the frontend)), each
/// call carrying the token (`--token`, `--token-file`, `PAROS_TOKEN`).
pub fn frontend_client(
    runtime: &Runtime,
    global: &Global,
) -> Result<Client<CompioProviders>, String> {
    let token = global.token()?.ok_or_else(|| {
        "no token for the frontends: pass --token or --token-file, or set PAROS_TOKEN".to_string()
    })?;
    let pass = std::sync::Arc::new(paros::net::Pass::new(token));
    let mut servers = Vec::new();
    for (index, entry) in global.frontends.iter().enumerate() {
        let address =
            Address::parse(entry.trim()).map_err(|e| format!("frontend {entry:?}: {e}"))?;
        servers.push(paros::client::Server {
            // A frontend has no node id: the index names it, and no answer
            // through it ever names a node.
            id: index as u64 + 1,
            node: paros::net::NodeClient::frontend(
                &runtime.rpc,
                runtime.names.clone(),
                address,
                pass.clone(),
            ),
        });
    }
    Ok(Client::new(
        &runtime.providers,
        servers,
        tunables(global.timeout()),
    ))
}

/// The servers `global` names, each with its node id: an explicit one, or
/// the one its own `Inspect` reports (#196: ids are random). A server that
/// does not answer is left out.
pub async fn servers(runtime: &Runtime, global: &Global) -> Vec<(u64, Address)> {
    let mut known = Vec::new();
    let mut unknown = Vec::new();
    for server in &global.servers {
        match server.id {
            Some(id) => known.push((id, server.addrs[0].clone())),
            None => unknown.extend(server.addrs.iter().cloned()),
        }
    }
    let found = bootstrap::discover(
        &runtime.providers,
        &runtime.rpc,
        &runtime.names,
        &unknown,
        global.timeout(),
    )
    .await;
    known.extend(found);
    known
}

/// The client for a cell command: through the frontends when `fronted`,
/// else over the servers, each with its node id.
pub async fn connect(
    runtime: &Runtime,
    global: &Global,
    fronted: bool,
) -> Result<(Vec<(u64, Address)>, Client<CompioProviders>), ExitCode> {
    if fronted {
        return frontend_client(runtime, global)
            .map(|client| (Vec::new(), client))
            .map_err(|error| {
                eprintln!("parosctl: {error}");
                ExitCode::FAILURE
            });
    }
    let servers = servers(runtime, global).await;
    if servers.is_empty() {
        eprintln!("parosctl: no server answered with its node id");
        return Err(Ending::Unreachable.into());
    }
    let client = client(runtime, &servers, global.timeout());
    Ok((servers, client))
}

/// The view asker over `global`'s servers, in its `--as-tenant` scope.
pub fn asker<'a>(runtime: &'a Runtime, global: &Global) -> view::Asker<'a> {
    view::Asker {
        providers: &runtime.providers,
        rpc: &runtime.rpc,
        names: &runtime.names,
        servers: global.addrs(),
        scope: match &global.as_tenant {
            Some(tenant) => paros::model::view::Scope::Tenant(tenant.as_bytes().to_vec()),
            None => paros::model::view::Scope::Admin,
        },
        timeout: global.timeout(),
    }
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;
    use std::time::Duration;

    use paros::model::view::Scope;

    use super::{Ending, ServerArg, asker, frontend_client, runtime, tunables};
    use crate::support::testing::{address, admin_token, global};

    #[test]
    fn a_server_is_an_address_or_an_id_and_an_address() {
        let plain: ServerArg = " 10.0.0.1:4500 ".parse().expect("valid");
        assert_eq!(
            (plain.id, plain.addrs),
            (None, vec![address("10.0.0.1:4500")])
        );
        let named: ServerArg = "00000000000000ab = 10.0.0.2:4500".parse().expect("valid");
        assert_eq!(
            (named.id, named.addrs),
            (Some(0xab), vec![address("10.0.0.2:4500")])
        );
        let bad_id = "zz=10.0.0.1:1".parse::<ServerArg>().expect_err("refused");
        assert!(
            bad_id.starts_with("bad id in \"zz=10.0.0.1:1\": "),
            "{bad_id}"
        );
        let bad_pair = "00000000000000ab=nope"
            .parse::<ServerArg>()
            .expect_err("refused");
        assert!(
            bad_pair.starts_with("bad address in \"00000000000000ab=nope\": "),
            "{bad_pair}"
        );
        let bad = "nope".parse::<ServerArg>().expect_err("refused");
        assert!(bad.starts_with("bad address in \"nope\": "), "{bad}");
    }

    #[test]
    fn the_addresses_keep_their_order_without_repeats() {
        let servers = global(&[
            "--servers",
            "10.0.0.2:1,10.0.0.1:1,00000000000000ab=10.0.0.2:1",
        ]);
        assert_eq!(
            servers.addrs(),
            [address("10.0.0.2:1"), address("10.0.0.1:1")]
        );
        assert_eq!(servers.timeout(), Duration::from_secs(5), "the default");
        assert_eq!(
            global(&["--timeout-ms", "250"]).timeout(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn a_token_is_its_text_or_its_file_and_none_without_either() {
        assert_eq!(global(&[]).token(), Ok(None));
        let text = admin_token();
        let given = global(&["--token", &text])
            .token()
            .expect("valid")
            .expect("given");
        assert!(!given.is_empty());
        let dir = tempfile::tempdir().expect("a directory");
        let file = dir.path().join("admin.token");
        std::fs::write(&file, &text).expect("written");
        let read = global(&["--token-file", file.to_str().expect("UTF-8")]).token();
        assert_eq!(read, Ok(Some(given)), "the same token");
        let missing = dir.path().join("none");
        let error = global(&["--token-file", missing.to_str().expect("UTF-8")])
            .token()
            .expect_err("refused");
        assert!(
            error.starts_with(&format!("{}: ", missing.display())),
            "{error}"
        );
        assert!(global(&["--token", "garbage"]).token().is_err());
    }

    #[test]
    fn each_ending_has_its_exit_code() {
        assert_eq!(ExitCode::from(Ending::Success), ExitCode::SUCCESS);
        assert_eq!(ExitCode::from(Ending::Refused), ExitCode::from(3));
        assert_eq!(ExitCode::from(Ending::Ambiguous), ExitCode::from(4));
        assert_eq!(ExitCode::from(Ending::Unreachable), ExitCode::from(5));
    }

    #[test]
    fn the_timeout_bounds_each_request_and_each_read() {
        let set = tunables(Duration::from_millis(70));
        assert_eq!(set.request_timeout, Duration::from_millis(70));
        assert_eq!(set.read_timeout, Duration::from_millis(70));
    }

    #[test]
    fn a_partial_tls_setting_is_refused_by_its_variable() {
        let refused = runtime(&global(&["--tls-cert", "/c.pem"]))
            .err()
            .expect("refused");
        assert!(refused.contains("PAROSCTL_TLS_CA"), "{refused}");
    }

    #[test]
    fn the_runtime_asks_in_the_scope_named_and_the_frontends_need_a_token() {
        compio::runtime::Runtime::new()
            .expect("a runtime")
            .block_on(async {
                let admin = global(&["--servers", "10.0.0.1:1"]);
                let rt = runtime(&admin).expect("plain transport");
                let asked = asker(&rt, &admin);
                assert_eq!(asked.scope, Scope::Admin);
                assert_eq!(asked.servers, [address("10.0.0.1:1")]);
                let scoped = global(&["--as-tenant", "acme"]);
                assert_eq!(asker(&rt, &scoped).scope, Scope::Tenant(b"acme".to_vec()));
                let none = frontend_client(&rt, &global(&["--frontends", "10.0.0.1:1"]));
                assert_eq!(
                    none.err(),
                    Some(
                        "no token for the frontends: pass --token or --token-file, or set \
                         PAROS_TOKEN"
                            .to_string()
                    )
                );
                let token = admin_token();
                let bad = frontend_client(
                    &rt,
                    &global(&["--frontends", "10.0.0.1:1,nope", "--token", &token]),
                );
                assert_eq!(
                    bad.err()
                        .as_deref()
                        .map(|e| e.starts_with("frontend \"nope\": ")),
                    Some(true)
                );
                let two = frontend_client(
                    &rt,
                    &global(&["--frontends", "10.0.0.1:1, 10.0.0.2:1", "--token", &token]),
                )
                .expect("a client");
                assert_eq!(two.server_count(), 2);
            });
    }
}
