//! `parosctl key generate|show|bootstrap` (#400): root key pairs, offline.
//!
//! A universe has its own root key pairs (#245 Biscuit tokens): never share
//! one between two universes. The key and its id come from the thread RNG,
//! which the OS seeds, through the provider's random source. A public key
//! also has a line, `<label>:<key_id>:ed25519/<hex>`, which every command
//! and variable that takes a key file takes in its place.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::{RandomProvider, TokioRandomProvider};
use paros_authz_biscuit::{Entropy, Role, RootKey, RootPublicKey};
use serde_json::json;

use crate::Ending;
use crate::output::{Printer, note};

/// `parosctl key`.
#[derive(Args, Debug)]
pub struct KeyArgs {
    #[command(subcommand)]
    command: KeyCommand,
}

#[derive(Subcommand, Debug)]
enum KeyCommand {
    /// Write a new root key pair: `<label>.private` (mode 0600) and
    /// `<label>.public`. Refuses to overwrite a file.
    Generate {
        /// The directory to write to.
        #[arg(long)]
        out: PathBuf,
        /// The key pair's name, e.g. `prod-2026-10`: letters, digits, `.`,
        /// `_` and `-`. It names the files and the key in every output.
        #[arg(long, value_parser = parse_label)]
        label: String,
    },
    /// Print the public half of a `.private` or `.public` file, as its
    /// line.
    Show {
        /// The key file.
        file: PathBuf,
    },
    /// Everything a new universe needs, in one command: a root key pair, an
    /// `admin` token, and a `tenant` token per `--tenant`. Prints the public
    /// key's line and each token. The private key is kept only with
    /// `--out`: without it, no token can be minted from this key again.
    Bootstrap {
        /// The key pair's name.
        #[arg(long, value_parser = parse_label, default_value = "root-1")]
        label: String,
        /// A tenant to mint a token for, by name; repeat it for more.
        #[arg(long = "tenant")]
        tenants: Vec<String>,
        /// How long the tokens are valid: `90s`, `30m`, `12h`, `7d`.
        #[arg(long, value_parser = crate::token::parse_duration)]
        ttl: Duration,
        /// A directory to write the key pair to, as `key generate` does.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

/// A key label: it names files, so letters, digits, `.`, `_` and `-` only.
pub fn parse_label(text: &str) -> Result<String, String> {
    if paros_authz_biscuit::is_label(text) {
        Ok(text.to_string())
    } else {
        Err(format!("{text:?}: letters, digits, '.', '_' and '-' only"))
    }
}

/// Entropy for one key or block, from the provider's random source.
pub fn entropy() -> Entropy {
    let random = TokioRandomProvider::new();
    Entropy::from_words([
        random.random(),
        random.random(),
        random.random(),
        random.random(),
    ])
}

/// Write `content` to a new file, readable by its owner only when `private`.
pub fn write_new(path: &Path, content: &str, private: bool) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    let mut file = options
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    file.write_all(content.as_bytes())
        .and_then(|()| file.write_all(b"\n"))
        .map_err(|e| format!("{}: {e}", path.display()))
}

/// Read a key file.
pub fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn public_json(key: &RootPublicKey) -> serde_json::Value {
    json!({
        "key_id": key.key_id(),
        "label": key.label(),
        "public_key": key.key_text(),
    })
}

pub fn run(out: &Printer, args: KeyArgs) -> Ending {
    let result = match args.command {
        KeyCommand::Generate { out: dir, label } => generate(out, &dir, &label),
        KeyCommand::Show { file } => read(&file)
            .and_then(|text| RootPublicKey::from_file(&text).map_err(|e| e.to_string()))
            .map(|key| out.emit(|| key.to_line(), || public_json(&key))),
        KeyCommand::Bootstrap {
            label,
            tenants,
            ttl,
            out: dir,
        } => bootstrap(out, &label, &tenants, ttl, dir.as_deref()),
    };
    match result {
        Ok(()) => Ending::Success,
        Err(error) => {
            note(&error);
            Ending::Refused
        }
    }
}

fn generate(out: &Printer, dir: &Path, label: &str) -> Result<(), String> {
    let key = RootKey::generate(label, &entropy());
    let (private, public) = write_pair(dir, &key)?;
    out.emit(
        || {
            format!(
                "key={} private={} public={}",
                key.label(),
                private.display(),
                public.display()
            )
        },
        || {
            json!({
                "key": key.label(),
                "key_id": key.key_id(),
                "private": private.display().to_string(),
                "public": public.display().to_string(),
            })
        },
    );
    Ok(())
}

/// Write `key` to `dir` as `<label>.private` and `<label>.public`, each a
/// new file.
fn write_pair(dir: &Path, key: &RootKey) -> Result<(PathBuf, PathBuf), String> {
    let private = dir.join(format!("{}.private", key.label()));
    let public = dir.join(format!("{}.public", key.label()));
    if public.exists() {
        return Err(format!("{}: already exists", public.display()));
    }
    write_new(&private, &key.to_file(), true)?;
    write_new(&public, &key.public().to_file(), false)?;
    Ok((private, public))
}

fn bootstrap(
    out: &Printer,
    label: &str,
    tenants: &[String],
    ttl: Duration,
    dir: Option<&Path>,
) -> Result<(), String> {
    let key = RootKey::generate(label, &entropy());
    let admin = crate::token::sign(&key, Role::Admin, "admin".to_string(), ttl)?;
    let mut minted = Vec::with_capacity(tenants.len());
    for tenant in tenants {
        let role = Role::Tenant(tenant.clone());
        minted.push((tenant, crate::token::sign(&key, role, tenant.clone(), ttl)?));
    }
    let files = dir.map(|dir| write_pair(dir, &key)).transpose()?;
    let line = key.public().to_line();
    out.emit(
        || {
            let mut lines = vec![format!("key={line}"), format!("admin={}", admin.to_text())];
            for (tenant, token) in &minted {
                lines.push(format!("tenant.{tenant}={}", token.to_text()));
            }
            if let Some((private, public)) = &files {
                lines.push(format!("private={}", private.display()));
                lines.push(format!("public={}", public.display()));
            }
            lines.join("\n")
        },
        || {
            let tenants: serde_json::Map<String, serde_json::Value> = minted
                .iter()
                .map(|(tenant, token)| ((*tenant).clone(), json!(token.to_text())))
                .collect();
            json!({
                "key": line,
                "admin": admin.to_text(),
                "tenants": tenants,
                "private": files.as_ref().map(|(private, _)| private.display().to_string()),
                "public": files.as_ref().map(|(_, public)| public.display().to_string()),
            })
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_label;

    #[test]
    fn labels_are_file_names() {
        assert!(parse_label("prod-2026.10_a").is_ok());
        assert!(parse_label("").is_err());
        assert!(parse_label("../x").is_err());
        assert!(parse_label(".hidden").is_err());
        assert!(parse_label("a b").is_err());
    }
}
