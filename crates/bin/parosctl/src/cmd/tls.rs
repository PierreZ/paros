//! `parosctl tls` (`paros::net::tls`): the universe's certificates, offline.
//!
//! One command makes everything a deployment needs: `parosctl tls init`
//! writes the authority, a certificate per machine, per frontend and per
//! admin, and prints the variables each process reads. `ca` and `issue`
//! do the same one file at a time (a new machine, a renewal); `show`
//! prints what a file holds and `verify` checks it as a handshake would.
//!
//! Every leaf names its holder `spiffe://<universe>/<role>/<name>`, and a
//! machine's or a frontend's also proves `paros`, the name every client
//! expects (`paros::net::tls::SERVER_NAME`). Keys are Ed25519 from the
//! operating system's entropy. A file is never overwritten, and a key file
//! is readable by its owner only.

use std::fmt::Write as _;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Args, Subcommand};
use paros::net::tls::rustls::RootCertStore;
use paros::net::tls::rustls::client::WebPkiServerVerifier;
use paros::net::tls::rustls::client::danger::ServerCertVerifier;
use paros::net::tls::rustls::pki_types::{ServerName, UnixTime};
use paros::net::tls::rustls::server::WebPkiClientVerifier;
use paros::net::tls::{
    Authority, CertificateDer, KeySeed, Role, SERVER_NAME, SubjectName, SystemUtc, UtcClock,
    UtcTime, Validity,
};
use serde_json::{Value, json};

use crate::cmd::key::{parse_label, read, write_new};
use crate::support::connect::Ending;
use crate::support::output::{Printer, note, table};

/// How far back a new certificate's window starts: a host whose clock is
/// a little behind the issuer's still accepts it.
const SKEW_SECS: u64 = 3_600;
const DAY_SECS: u64 = 86_400;
/// The authority's file names in its directory.
const CA_CERT: &str = "ca.crt";
const CA_KEY: &str = "ca.key";

/// `parosctl tls`.
#[derive(Args, Debug)]
pub struct TlsArgs {
    #[command(subcommand)]
    command: TlsCommand,
}

#[derive(Subcommand, Debug)]
enum TlsCommand {
    /// Write everything a deployment needs in one step: the authority, a
    /// certificate per machine, frontend and admin, and the variables
    /// each process reads. Refuses to overwrite a file.
    Init {
        /// The directory to write to (created if missing).
        #[arg(long)]
        out: PathBuf,
        /// The machines, comma-separated: each name (the host of its
        /// `PAROS_ADVERTISE`, e.g. `parosd-1`) is also a name the
        /// certificate proves.
        #[arg(long, value_delimiter = ',', required = true, value_parser = parse_label)]
        machines: Vec<String>,
        /// The frontends, comma-separated.
        #[arg(long, value_delimiter = ',', value_parser = parse_label)]
        frontends: Vec<String>,
        /// The operators, comma-separated: each gets a client certificate
        /// for `parosctl`.
        #[arg(long, value_delimiter = ',', default_value = "admin", value_parser = parse_label)]
        admins: Vec<String>,
        /// The applications' clients, comma-separated.
        #[arg(long, value_delimiter = ',', value_parser = parse_label)]
        clients: Vec<String>,
        #[command(flatten)]
        common: Common,
        /// How long the authority stays valid, in days.
        #[arg(long, default_value = "3650")]
        ca_days: u64,
    },
    /// Write a new authority: `ca.crt` and `ca.key` (mode 0600).
    Ca {
        /// The directory to write to (created if missing).
        #[arg(long)]
        out: PathBuf,
        /// The universe it signs for, as in every identity.
        #[arg(long, default_value = "universe", value_parser = parse_label)]
        universe: String,
        /// How long it stays valid, in days.
        #[arg(long, default_value = "3650")]
        days: u64,
    },
    /// Issue one certificate with the authority in `--ca-dir`:
    /// `<name>.crt` (the chain) and `<name>.key` (mode 0600).
    Issue {
        /// Its holder's name: a machine's, a frontend's, an operator's.
        #[arg(value_parser = parse_label)]
        name: String,
        /// What it is: `machine`, `frontend`, `admin` or `client`.
        #[arg(long)]
        role: Role,
        /// The authority's directory (`ca.crt`, `ca.key`).
        #[arg(long)]
        ca_dir: PathBuf,
        /// The directory to write to (default: `--ca-dir`).
        #[arg(long)]
        out: Option<PathBuf>,
        /// More DNS names it proves, comma-separated (a machine's or a
        /// frontend's name is one already).
        #[arg(long, value_delimiter = ',')]
        host: Vec<String>,
        /// IP addresses it proves, comma-separated.
        #[arg(long, value_delimiter = ',')]
        ip: Vec<IpAddr>,
        #[command(flatten)]
        common: Common,
    },
    /// Print what certificate files hold: each certificate's identity,
    /// names, use, issuer and validity.
    Show {
        /// The PEM files.
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// Check a certificate as a handshake would: signed by the authority,
    /// valid now, and fit for its use (`paros` as a server, a client of
    /// mutual TLS).
    Verify {
        /// The certificate chain file.
        file: PathBuf,
        /// The authority's certificate.
        #[arg(long)]
        ca: PathBuf,
    },
}

/// The options a leaf takes.
#[derive(Args, Debug)]
struct Common {
    /// The universe, as in every identity: the name `parosctl init`
    /// gives it.
    #[arg(long, default_value = "universe", value_parser = parse_label)]
    universe: String,
    /// How long each certificate stays valid, in days.
    #[arg(long, default_value = "365")]
    days: u64,
}

pub fn run(out: &Printer, args: TlsArgs) -> Ending {
    let result = match args.command {
        TlsCommand::Init {
            out: dir,
            machines,
            frontends,
            admins,
            clients,
            common,
            ca_days,
        } => {
            let holders = [
                (Role::Machine, machines),
                (Role::Frontend, frontends),
                (Role::Admin, admins),
                (Role::Client, clients),
            ];
            init(out, &dir, &holders, &common, ca_days)
        }
        TlsCommand::Ca {
            out: dir,
            universe,
            days,
        } => new_authority(&dir, &universe, days).map(|written| {
            out.emit(|| written.text(), || written.json());
        }),
        TlsCommand::Issue {
            name,
            role,
            ca_dir,
            out: dir,
            host,
            ip,
            common,
        } => {
            let extra: Vec<SubjectName> = host
                .into_iter()
                .map(SubjectName::Dns)
                .chain(ip.into_iter().map(SubjectName::Ip))
                .collect();
            load_authority(&ca_dir)
                .and_then(|authority| {
                    issue(
                        &authority,
                        dir.as_deref().unwrap_or(&ca_dir),
                        role,
                        &name,
                        &extra,
                        &common,
                    )
                })
                .map(|written| out.emit(|| written.text(), || written.json()))
        }
        TlsCommand::Show { files } => show(out, &files),
        TlsCommand::Verify { file, ca } => return verify(out, &file, &ca),
    };
    match result {
        Ok(()) => Ending::Success,
        Err(error) => {
            note(&error);
            Ending::Refused
        }
    }
}

/// Now, as the host's clock tells it.
fn now() -> Result<UtcTime, String> {
    SystemUtc
        .now_utc()
        .ok_or_else(|| "the host clock is before 1970".to_string())
}

fn window(days: u64) -> Result<Validity, String> {
    if days == 0 {
        return Err("a certificate lives at least one day".into());
    }
    Ok(Validity::starting(
        now()?.minus(SKEW_SECS),
        days.saturating_mul(DAY_SECS).saturating_add(SKEW_SECS),
    ))
}

/// A file pair written: what and where.
struct Written {
    role: Option<Role>,
    name: String,
    identity: String,
    cert: PathBuf,
    key: PathBuf,
    not_after: UtcTime,
}

impl Written {
    fn text(&self) -> String {
        format!(
            "{} {} cert={} key={} until={}",
            self.role.map_or("authority", Role::as_str),
            self.identity,
            self.cert.display(),
            self.key.display(),
            date(self.not_after)
        )
    }

    fn json(&self) -> Value {
        json!({
            "role": self.role.map_or("authority", Role::as_str),
            "name": self.name,
            "identity": self.identity,
            "cert": self.cert.display().to_string(),
            "key": self.key.display().to_string(),
            "not_after": self.not_after.unix_seconds(),
        })
    }
}

fn ensure_dir(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))
}

/// Refuse to start when any of `paths` exists: a run writes all or nothing
/// it can foresee.
fn refuse_existing(paths: &[PathBuf]) -> Result<(), String> {
    match paths.iter().find(|path| path.exists()) {
        Some(path) => Err(format!(
            "{}: already exists; parosctl tls never overwrites a file",
            path.display()
        )),
        None => Ok(()),
    }
}

fn new_authority(dir: &Path, universe: &str, days: u64) -> Result<Written, String> {
    let cert = dir.join(CA_CERT);
    let key = dir.join(CA_KEY);
    refuse_existing(&[cert.clone(), key.clone()])?;
    ensure_dir(dir)?;
    let name = format!("{universe} authority");
    let authority =
        Authority::new(&name, window(days)?, KeySeed::Random).map_err(|e| e.to_string())?;
    write_new(&key, authority.key_pem().trim_end(), true)?;
    write_new(&cert, authority.certificate_pem().trim_end(), false)?;
    Ok(Written {
        role: None,
        name: name.clone(),
        identity: name,
        cert,
        key,
        not_after: authority.validity().not_after,
    })
}

fn load_authority(dir: &Path) -> Result<Authority, String> {
    let cert = read(&dir.join(CA_CERT))?;
    let key = read(&dir.join(CA_KEY))?;
    Authority::from_pem(&cert, &key).map_err(|e| format!("{}: {e}", dir.display()))
}

/// The name a holder's own name also proves: an IP or a DNS name.
fn own_name(name: &str) -> SubjectName {
    name.parse::<IpAddr>()
        .map_or_else(|_| SubjectName::Dns(name.to_string()), SubjectName::Ip)
}

fn issue(
    authority: &Authority,
    dir: &Path,
    role: Role,
    name: &str,
    extra: &[SubjectName],
    common: &Common,
) -> Result<Written, String> {
    let cert = dir.join(format!("{name}.crt"));
    let key = dir.join(format!("{name}.key"));
    refuse_existing(&[cert.clone(), key.clone()])?;
    ensure_dir(dir)?;
    let mut names = Vec::new();
    if role.usage().serves() {
        names.push(own_name(name));
    }
    names.extend(extra.iter().cloned());
    let validity = window(common.days)?;
    if validity.not_after > authority.validity().not_after {
        return Err(format!(
            "the authority expires on {}, before the certificate would: issue fewer --days or \
             renew the authority",
            date(authority.validity().not_after)
        ));
    }
    let spec = paros::net::tls::leaf(
        &common.universe,
        role,
        name,
        &names,
        validity,
        KeySeed::Random,
    );
    let issued = authority.issue(&spec).map_err(|e| e.to_string())?;
    write_new(&key, issued.key_pem().trim_end(), true)?;
    write_new(&cert, issued.chain_pem().trim_end(), false)?;
    Ok(Written {
        role: Some(role),
        name: name.to_string(),
        identity: paros::net::tls::identity(&common.universe, role, name),
        cert,
        key,
        not_after: issued.validity.not_after,
    })
}

fn init(
    out: &Printer,
    dir: &Path,
    holders: &[(Role, Vec<String>); 4],
    common: &Common,
    ca_days: u64,
) -> Result<(), String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut planned = vec![dir.join(CA_CERT), dir.join(CA_KEY)];
    for (_, names) in holders {
        for name in names {
            if !seen.insert(name.as_str()) {
                return Err(format!("{name:?} is named twice: one name, one file pair"));
            }
            planned.push(dir.join(format!("{name}.crt")));
            planned.push(dir.join(format!("{name}.key")));
        }
    }
    refuse_existing(&planned)?;
    if common.days > ca_days {
        return Err("--days is longer than --ca-days: the authority would expire first".into());
    }
    let ca = new_authority(dir, &common.universe, ca_days)?;
    let authority = load_authority(dir)?;
    let mut written = vec![ca];
    for (role, names) in holders {
        for name in names {
            written.push(issue(&authority, dir, *role, name, &[], common)?);
        }
    }
    out.emit(
        || init_text(&written),
        || {
            json!({
                "universe": common.universe,
                "files": written.iter().map(Written::json).collect::<Vec<_>>(),
            })
        },
    );
    Ok(())
}

/// What `init` prints: the files, then each process's variables.
fn init_text(written: &[Written]) -> String {
    let rows: Vec<[String; 4]> = written
        .iter()
        .map(|w| {
            [
                w.cert.display().to_string(),
                w.role.map_or("authority", Role::as_str).to_string(),
                w.identity.clone(),
                date(w.not_after),
            ]
        })
        .collect();
    let mut text = table(["FILE", "ROLE", "IDENTITY", "VALID UNTIL"], &rows);
    let ca = &written[0].cert;
    text.push_str("\n\nSet these variables. Keep every .key file secret.\n");
    for w in &written[1..] {
        let (who, prefix) = match w.role {
            Some(Role::Machine) => ("parosd", "PAROS_TLS"),
            Some(Role::Frontend) => ("paros-frontend", "PAROS_FRONTEND_TLS"),
            Some(Role::Admin) => ("parosctl", "PAROSCTL_TLS"),
            Some(Role::Client) | None => ("client", "PAROSCTL_TLS"),
        };
        let _ = writeln!(
            text,
            "  {who} {}: {prefix}_CERT={} {prefix}_KEY={} {prefix}_CA={}",
            w.name,
            w.cert.display(),
            w.key.display(),
            ca.display()
        );
    }
    text.push_str(
        "A server asks every caller for a certificate (mutual TLS). Set \
         PAROS_TLS_CLIENT_AUTH=optional to admit callers without one.",
    );
    text
}

/// A UTC time as `YYYY-MM-DD`.
fn date(time: UtcTime) -> String {
    // Civil from days (Howard Hinnant's algorithm), to avoid a date crate.
    let days = i64::try_from(time.unix_seconds() / DAY_SECS).unwrap_or(i64::MAX);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// One certificate, as `show` prints it.
struct Summary {
    subject: String,
    issuer: String,
    names: Vec<String>,
    authority: bool,
    server: bool,
    client: bool,
    not_before: UtcTime,
    not_after: UtcTime,
}

impl Summary {
    fn of(der: &CertificateDer<'_>) -> Result<Self, String> {
        let (_, parsed) = x509_parser::parse_x509_certificate(der)
            .map_err(|e| format!("not a certificate: {e}"))?;
        let common_name = |name: &x509_parser::x509::X509Name<'_>| {
            name.iter_common_name()
                .next()
                .and_then(|cn| cn.as_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        let names = paros::net::tls::CertificateNames::of(der);
        let mut listed: Vec<String> = names.uris.iter().map(|uri| format!("uri:{uri}")).collect();
        listed.extend(names.dns.iter().map(|dns| format!("dns:{dns}")));
        listed.extend(names.ips.iter().map(|ip| format!("ip:{ip}")));
        let (server, client) = parsed
            .extended_key_usage()
            .ok()
            .flatten()
            .map_or((false, false), |usage| {
                (usage.value.server_auth, usage.value.client_auth)
            });
        let seconds = |time: x509_parser::time::ASN1Time| {
            UtcTime::from_unix_seconds(u64::try_from(time.timestamp()).unwrap_or(0))
        };
        Ok(Self {
            subject: common_name(parsed.subject()),
            issuer: common_name(parsed.issuer()),
            names: listed,
            authority: parsed.is_ca(),
            server,
            client,
            not_before: seconds(parsed.validity().not_before),
            not_after: seconds(parsed.validity().not_after),
        })
    }

    fn kind(&self) -> &'static str {
        match (self.authority, self.server, self.client) {
            (true, _, _) => "authority",
            (false, true, true) => "server+client",
            (false, true, false) => "server",
            (false, false, true) => "client",
            (false, false, false) => "no use",
        }
    }

    /// `valid`, `expired` or `not yet valid` at `now`, with the days left.
    fn state(&self, now: UtcTime) -> String {
        if now < self.not_before {
            "not yet valid".to_string()
        } else if now > self.not_after {
            "expired".to_string()
        } else {
            let days = (self.not_after.unix_seconds() - now.unix_seconds()) / DAY_SECS;
            format!("valid, {days} days left")
        }
    }

    fn text(&self, now: UtcTime) -> String {
        let names = if self.names.is_empty() {
            "none".to_string()
        } else {
            self.names.join(",")
        };
        format!(
            "{} use={} names={} issuer={:?} from={} until={} ({})",
            if self.subject.is_empty() {
                "(no common name)"
            } else {
                &self.subject
            },
            self.kind(),
            names,
            self.issuer,
            date(self.not_before),
            date(self.not_after),
            self.state(now)
        )
    }

    fn json(&self, now: UtcTime) -> Value {
        json!({
            "subject": self.subject,
            "issuer": self.issuer,
            "use": self.kind(),
            "names": self.names,
            "not_before": self.not_before.unix_seconds(),
            "not_after": self.not_after.unix_seconds(),
            "state": self.state(now),
        })
    }
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let chain = paros::net::tls::read_certificates(read(path)?.as_bytes())
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if chain.is_empty() {
        return Err(format!("{}: no certificate", path.display()));
    }
    Ok(chain)
}

fn show(out: &Printer, files: &[PathBuf]) -> Result<(), String> {
    let now = now()?;
    for path in files {
        for (index, der) in certificates(path)?.iter().enumerate() {
            let summary = Summary::of(der).map_err(|e| format!("{}: {e}", path.display()))?;
            out.emit(
                || format!("{}[{index}] {}", path.display(), summary.text(now)),
                || {
                    let mut doc = summary.json(now);
                    doc["file"] = json!(path.display().to_string());
                    doc["index"] = json!(index);
                    doc
                },
            );
        }
    }
    Ok(())
}

/// A check `verify` ran, and its verdict.
struct Check {
    what: &'static str,
    verdict: Result<(), String>,
}

fn verify(out: &Printer, file: &Path, ca: &Path) -> Ending {
    let checks = match verify_checks(file, ca) {
        Ok(checks) => checks,
        Err(error) => {
            note(&error);
            return Ending::Refused;
        }
    };
    let passed = checks.iter().all(|check| check.verdict.is_ok());
    for check in &checks {
        out.emit(
            || match &check.verdict {
                Ok(()) => format!("{}: ok", check.what),
                Err(error) => format!("{}: refused: {error}", check.what),
            },
            || {
                json!({
                    "check": check.what,
                    "ok": check.verdict.is_ok(),
                    "error": check.verdict.as_ref().err(),
                })
            },
        );
    }
    if passed {
        Ending::Success
    } else {
        Ending::Refused
    }
}

fn verify_checks(file: &Path, ca: &Path) -> Result<Vec<Check>, String> {
    let chain = certificates(file)?;
    let mut roots = RootCertStore::empty();
    for root in certificates(ca)? {
        roots
            .add(root)
            .map_err(|e| format!("{}: {e}", ca.display()))?;
    }
    let roots = Arc::new(roots);
    let provider = Arc::new(paros::net::tls::rustls::crypto::ring::default_provider());
    let summary = Summary::of(&chain[0]).map_err(|e| format!("{}: {e}", file.display()))?;
    let now = UnixTime::since_unix_epoch(std::time::Duration::from_secs(now()?.unix_seconds()));
    let (leaf, intermediates) = chain.split_first().ok_or("an empty chain")?;
    let mut checks = Vec::new();
    if summary.server {
        let verifier =
            WebPkiServerVerifier::builder_with_provider(Arc::clone(&roots), Arc::clone(&provider))
                .build()
                .map_err(|e| e.to_string())?;
        let name = ServerName::try_from(SERVER_NAME).map_err(|e| e.to_string())?;
        checks.push(Check {
            what: "server (paros)",
            verdict: verifier
                .verify_server_cert(leaf, intermediates, &name, &[], now)
                .map(|_| ())
                .map_err(|e| e.to_string()),
        });
    }
    if summary.client {
        let verifier = WebPkiClientVerifier::builder_with_provider(roots, provider)
            .build()
            .map_err(|e| e.to_string())?;
        checks.push(Check {
            what: "client (mutual TLS)",
            verdict: verifier
                .verify_client_cert(leaf, intermediates, now)
                .map(|_| ())
                .map_err(|e| e.to_string()),
        });
    }
    if checks.is_empty() {
        return Err(format!(
            "{}: a {} certificate, neither a server nor a client one",
            file.display(),
            summary.kind()
        ));
    }
    Ok(checks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_are_civil() {
        assert_eq!(date(UtcTime::from_unix_seconds(0)), "1970-01-01");
        assert_eq!(
            date(UtcTime::from_unix_seconds(1_906_502_400)),
            "2030-06-01"
        );
        assert_eq!(date(UtcTime::from_unix_seconds(951_782_400)), "2000-02-29");
    }

    #[test]
    fn init_issue_show_and_verify() {
        let dir = tempfile::tempdir().expect("tempdir");
        let out = Printer::new(true);
        let common = Common {
            universe: "prod".into(),
            days: 30,
        };
        let holders = [
            (
                Role::Machine,
                vec!["parosd-1".to_string(), "10.0.0.2".to_string()],
            ),
            (Role::Frontend, vec!["front".to_string()]),
            (Role::Admin, vec!["admin".to_string()]),
            (Role::Client, Vec::new()),
        ];
        init(&out, dir.path(), &holders, &common, 3650).expect("init");
        // Never overwrites.
        assert!(init(&out, dir.path(), &holders, &common, 3650).is_err());
        let machine = dir.path().join("parosd-1.crt");
        let summary = Summary::of(&certificates(&machine).expect("chain")[0]).expect("parse");
        assert_eq!(summary.kind(), "server+client");
        assert_eq!(
            summary.names,
            vec![
                "uri:spiffe://prod/machine/parosd-1".to_string(),
                "dns:paros".to_string(),
                "dns:parosd-1".to_string()
            ]
        );
        assert_eq!(summary.issuer, "prod authority");
        let ip = Summary::of(&certificates(&dir.path().join("10.0.0.2.crt")).expect("chain")[0])
            .expect("parse");
        assert!(ip.names.contains(&"ip:10.0.0.2".to_string()));
        let ca = dir.path().join(CA_CERT);
        let checks = verify_checks(&machine, &ca).expect("checks");
        assert_eq!(checks.len(), 2);
        assert!(checks.iter().all(|check| check.verdict.is_ok()));
        let admin = verify_checks(&dir.path().join("admin.crt"), &ca).expect("checks");
        assert_eq!(admin.len(), 1);
        assert!(admin[0].verdict.is_ok());
        // Another authority does not vouch for it.
        let other = tempfile::tempdir().expect("tempdir");
        new_authority(other.path(), "prod", 10).expect("ca");
        let refused = verify_checks(&machine, &other.path().join(CA_CERT)).expect("checks");
        assert!(refused.iter().all(|check| check.verdict.is_err()));
        // `issue` adds one more machine with the same authority.
        let authority = load_authority(dir.path()).expect("authority");
        let more = issue(
            &authority,
            dir.path(),
            Role::Machine,
            "parosd-4",
            &[SubjectName::Dns("db.example.org".into())],
            &common,
        )
        .expect("issue");
        assert_eq!(more.identity, "spiffe://prod/machine/parosd-4");
        assert!(
            verify_checks(&more.cert, &ca)
                .expect("checks")
                .iter()
                .all(|c| c.verdict.is_ok())
        );
        // The files the servers and parosctl read build their transports.
        let files = paros::net::tls::Files {
            cert: ("CERT", Some(paros::net::tls::Pem::File(&machine))),
            key: (
                "KEY",
                Some(paros::net::tls::Pem::File(&dir.path().join("parosd-1.key"))),
            ),
            ca: ("CA", Some(paros::net::tls::Pem::File(&ca))),
        };
        assert!(
            files
                .serving(paros::net::tls::ClientCertificates::Required)
                .is_ok_and(|transport| transport.is_encrypted())
        );
        // A certificate that outlives its authority is refused.
        let long = Common {
            universe: "prod".into(),
            days: 20_000,
        };
        assert!(issue(&authority, dir.path(), Role::Admin, "late", &[], &long).is_err());
        assert!(init(&out, other.path(), &holders, &long, 10).is_err());
    }
}
