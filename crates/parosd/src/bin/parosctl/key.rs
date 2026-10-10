//! `parosctl key generate|show` (#400): root key pairs, offline.
//!
//! A universe has its own root key pairs (#245 Biscuit tokens): never share
//! one between two universes. The key and its id come from the thread RNG,
//! which the OS seeds, through the provider's random source.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use moonpool_core::{RandomProvider, TokioRandomProvider};
use paros_authz_biscuit::{Entropy, RootKey, RootPublicKey};
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
    /// Print the public half of a `.private` or `.public` file.
    Show {
        /// The key file.
        file: PathBuf,
    },
}

/// A key label: it names files, so letters, digits, `.`, `_` and `-` only.
fn parse_label(text: &str) -> Result<String, String> {
    let valid = !text.is_empty()
        && !text.starts_with('.')
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if valid {
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
            .map(|key| {
                out.emit(
                    || format!("{} {}", key.label(), key.key_text()),
                    || public_json(&key),
                );
            }),
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
    let private = dir.join(format!("{label}.private"));
    let public = dir.join(format!("{label}.public"));
    if public.exists() {
        return Err(format!("{}: already exists", public.display()));
    }
    write_new(&private, &key.to_file(), true)?;
    write_new(&public, &key.public().to_file(), false)?;
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
