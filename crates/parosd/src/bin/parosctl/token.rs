//! `parosctl token mint|derive|inspect` (#400): Biscuit tokens, offline.
//!
//! `mint` signs a new token with a root key: `admin`, or `tenant` for one
//! tenant named by its name. `derive` narrows a token macaroon style with
//! no key at all: the key that signs the new block is inside the token.
//! `inspect` prints the blocks, and checks the signature given a key.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use clap::{Args, Subcommand, ValueEnum};
use paros_authz_biscuit::{Grant, KeyRing, Restriction, Role, RootKey, RootPublicKey, Token};
use serde_json::json;

use crate::Ending;
use crate::key::{entropy, read, write_new};
use crate::output::{Printer, note};

/// `parosctl token`.
#[derive(Args, Debug)]
pub struct TokenArgs {
    #[command(subcommand)]
    command: TokenCommand,
}

/// A role as given on the command line.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum RoleArg {
    /// The universe and everything below it.
    Admin,
    /// The data plane and journals of one tenant (`--tenant`).
    Tenant,
}

/// Where a command reads its input token.
#[derive(Args, Debug)]
struct TokenInput {
    /// The token, base64url. Else `--token-file`, else `PAROS_TOKEN`.
    token: Option<String>,
    /// A file that holds the token.
    #[arg(long, conflicts_with = "token")]
    token_file: Option<PathBuf>,
}

/// Where a command writes its token: stdout, or a new file (mode 0600).
#[derive(Args, Debug)]
struct TokenOutput {
    /// Write the token to this new file instead of printing it.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum TokenCommand {
    /// Sign a new token with a root key.
    Mint {
        /// The `.private` key file.
        #[arg(long)]
        key: PathBuf,
        /// The role granted.
        #[arg(long, value_enum)]
        role: RoleArg,
        /// The tenant's name, for `--role tenant`.
        #[arg(long, required_if_eq("role", "tenant"))]
        tenant: Option<String>,
        /// How long the token is valid: `90s`, `30m`, `12h`, `7d`.
        #[arg(long, value_parser = parse_duration)]
        ttl: Duration,
        /// Who the token is for: a label for logs.
        #[arg(long)]
        subject: String,
        #[command(flatten)]
        output: TokenOutput,
    },
    /// Derive a narrower token, offline and with no key.
    Derive {
        #[command(flatten)]
        input: TokenInput,
        /// Only operations that change nothing.
        #[arg(long)]
        read_only: bool,
        /// Only requests to this tenant, by name.
        #[arg(long)]
        tenant: Option<String>,
        /// Only requests to this journal, by name.
        #[arg(long)]
        journal: Option<String>,
        /// An earlier expiry, from now: `90s`, `30m`, `12h`, `7d`.
        #[arg(long, value_parser = parse_duration)]
        ttl: Option<Duration>,
        /// Seal the result: nothing can be derived from it any more.
        #[arg(long)]
        seal: bool,
        #[command(flatten)]
        output: TokenOutput,
    },
    /// Print a token's blocks; with `--key`, check its signature first.
    Inspect {
        #[command(flatten)]
        input: TokenInput,
        /// A `.public` (or `.private`) key file to check the signature.
        #[arg(long)]
        key: Option<PathBuf>,
    },
}

/// A duration as `<n><unit>`, the unit one of `s`, `m`, `h`, `d`.
fn parse_duration(text: &str) -> Result<Duration, String> {
    let split = text.len().saturating_sub(1);
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| format!("{text:?}: expected <n>s, <n>m, <n>h or <n>d"))?;
    let seconds = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => return Err(format!("{text:?}: expected <n>s, <n>m, <n>h or <n>d")),
    };
    if number == 0 {
        return Err(format!("{text:?}: a duration is positive"));
    }
    number
        .checked_mul(seconds)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("{text:?}: too long"))
}

impl TokenInput {
    fn read(&self) -> Result<Token, String> {
        let text = match (&self.token, &self.token_file) {
            (Some(token), _) => token.clone(),
            (None, Some(path)) => read(path)?,
            (None, None) => std::env::var("PAROS_TOKEN")
                .map_err(|_| "no token: pass one, --token-file, or set PAROS_TOKEN".to_string())?,
        };
        Token::from_text(&text).map_err(|e| e.to_string())
    }
}

impl TokenOutput {
    fn write(&self, out: &Printer, token: &Token) -> Result<(), String> {
        let text = token.to_text();
        match &self.out {
            Some(path) => {
                write_new(path, &text, true)?;
                out.emit(
                    || format!("token={}", path.display()),
                    || json!({ "token_file": path.display().to_string() }),
                );
            }
            None => out.emit(|| text.clone(), || json!({ "token": text })),
        }
        Ok(())
    }
}

pub fn run(out: &Printer, args: TokenArgs) -> Ending {
    let result = match args.command {
        TokenCommand::Mint {
            key,
            role,
            tenant,
            ttl,
            subject,
            output,
        } => mint(out, &key, role, tenant, ttl, subject, &output),
        TokenCommand::Derive {
            input,
            read_only,
            tenant,
            journal,
            ttl,
            seal,
            output,
        } => {
            let restriction = Restriction {
                read_only,
                tenant,
                journal,
                classes: None,
                expires: ttl.map(|ttl| SystemTime::now() + ttl),
            };
            derive(out, &input, &restriction, seal, &output)
        }
        TokenCommand::Inspect { input, key } => inspect(out, &input, key.as_deref()),
    };
    match result {
        Ok(()) => Ending::Success,
        Err(error) => {
            note(&error);
            Ending::Refused
        }
    }
}

fn mint(
    out: &Printer,
    key: &std::path::Path,
    role: RoleArg,
    tenant: Option<String>,
    ttl: Duration,
    subject: String,
    output: &TokenOutput,
) -> Result<(), String> {
    let key = RootKey::from_file(&read(key)?).map_err(|e| e.to_string())?;
    let role = match (role, tenant) {
        (RoleArg::Admin, None) => Role::Admin,
        (RoleArg::Tenant, Some(tenant)) => Role::Tenant(tenant),
        (RoleArg::Admin, Some(_)) => return Err("--tenant is for --role tenant".to_string()),
        (RoleArg::Tenant, None) => return Err("--role tenant needs --tenant".to_string()),
    };
    // The minting clock is this machine's: parosctl runs outside any cell.
    let now = SystemTime::now();
    let grant = Grant {
        role,
        subject,
        expires: now + ttl,
    };
    let token =
        paros_authz_biscuit::mint(&key, &grant, now, &entropy()).map_err(|e| e.to_string())?;
    output.write(out, &token)
}

fn derive(
    out: &Printer,
    input: &TokenInput,
    restriction: &Restriction,
    seal: bool,
    output: &TokenOutput,
) -> Result<(), String> {
    let token = input.read()?;
    let derived = if restriction_is_empty(restriction) {
        if !seal {
            return Err("derive needs a restriction or --seal".to_string());
        }
        token
    } else {
        paros_authz_biscuit::derive(&token, restriction, &entropy()).map_err(|e| e.to_string())?
    };
    let derived = if seal {
        paros_authz_biscuit::seal(&derived).map_err(|e| e.to_string())?
    } else {
        derived
    };
    output.write(out, &derived)
}

fn restriction_is_empty(restriction: &Restriction) -> bool {
    !restriction.read_only
        && restriction.tenant.is_none()
        && restriction.journal.is_none()
        && restriction.expires.is_none()
}

fn inspect(out: &Printer, input: &TokenInput, key: Option<&std::path::Path>) -> Result<(), String> {
    let token = input.read()?;
    let ring = match key {
        Some(path) => {
            let key = RootPublicKey::from_file(&read(path)?).map_err(|e| e.to_string())?;
            Some(KeyRing::new([key]).map_err(|e| e.to_string())?)
        }
        None => None,
    };
    let text = paros_authz_biscuit::inspect(&token, ring.as_ref()).map_err(|e| e.to_string())?;
    out.emit(
        || text.trim_end().to_string(),
        || json!({ "verified": ring.is_some(), "blocks": text }),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_duration;
    use std::time::Duration;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_hours(2)));
        assert_eq!(parse_duration("7d"), Ok(Duration::from_hours(168)));
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("5w").is_err());
    }
}
