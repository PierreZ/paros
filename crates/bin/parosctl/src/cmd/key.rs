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

use crate::support::connect::Ending;
use crate::support::output::{Printer, note};

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
pub fn parse_label(text: &str) -> Result<String, String> {
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
    use clap::Parser;
    use paros_authz_biscuit::{RootKey, RootPublicKey};

    use super::{KeyArgs, parse_label, run};
    use crate::support::connect::Ending;
    use crate::support::output::Printer;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        key: KeyArgs,
    }

    fn key(out: &Printer, line: &[&str]) -> Ending {
        let mut words = vec!["key"];
        words.extend_from_slice(line);
        run(out, Cli::try_parse_from(words).expect("valid").key)
    }

    #[test]
    fn a_generated_pair_is_written_once_and_shown_by_its_label() {
        let dir = tempfile::tempdir().expect("a directory");
        let at = dir.path().to_str().expect("UTF-8");
        let out = Printer::new(false);
        assert_eq!(
            key(&out, &["generate", "--out", at, "--label", "demo"]),
            Ending::Success
        );
        let (private, public) = (
            dir.path().join("demo.private"),
            dir.path().join("demo.public"),
        );
        assert_eq!(
            out.printed(),
            [format!(
                "key=demo private={} public={}",
                private.display(),
                public.display()
            )]
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |path| {
                std::fs::metadata(path)
                    .expect("written")
                    .permissions()
                    .mode()
            };
            assert_eq!(mode(&private) & 0o777, 0o600, "the private key is private");
            assert_ne!(mode(&public) & 0o077, 0, "the public key is not");
        }
        let pair = RootKey::from_file(&std::fs::read_to_string(&private).expect("read"))
            .expect("a private key file");
        let shown = RootPublicKey::from_file(&std::fs::read_to_string(&public).expect("read"))
            .expect("a public key file");
        assert_eq!(pair.public().key_text(), shown.key_text(), "one pair");
        assert_eq!(
            key(&out, &["generate", "--out", at, "--label", "demo"]),
            Ending::Refused,
            "never overwritten"
        );
        let json = Printer::new(true);
        assert_eq!(
            key(&json, &["show", public.to_str().expect("UTF-8")]),
            Ending::Success
        );
        let doc: serde_json::Value = serde_json::from_str(&json.printed()[0]).expect("JSON");
        assert_eq!(
            doc,
            serde_json::json!({
                "key_id": shown.key_id(),
                "label": "demo",
                "public_key": shown.key_text(),
            })
        );
        let text = Printer::new(false);
        key(&text, &["show", public.to_str().expect("UTF-8")]);
        assert_eq!(text.printed(), [format!("demo {}", shown.key_text())]);
        let missing = dir.path().join("none.public");
        assert_eq!(
            key(&out, &["show", missing.to_str().expect("UTF-8")]),
            Ending::Refused
        );
        let garbage = dir.path().join("garbage.public");
        std::fs::write(&garbage, "not a key").expect("written");
        assert_eq!(
            key(&out, &["show", garbage.to_str().expect("UTF-8")]),
            Ending::Refused,
            "not a key file"
        );
    }

    #[test]
    fn a_generated_pair_names_its_id_in_json() {
        let dir = tempfile::tempdir().expect("a directory");
        let out = Printer::new(true);
        let at = dir.path().to_str().expect("UTF-8");
        assert_eq!(
            key(&out, &["generate", "--out", at, "--label", "k1"]),
            Ending::Success
        );
        let doc: serde_json::Value = serde_json::from_str(&out.printed()[0]).expect("JSON");
        assert_eq!(doc["key"], "k1");
        assert!(doc["key_id"].is_u64());
        assert_eq!(
            doc["private"],
            dir.path().join("k1.private").display().to_string()
        );
    }

    #[test]
    fn labels_are_file_names() {
        assert!(parse_label("prod-2026.10_a").is_ok());
        assert!(parse_label("").is_err());
        assert!(parse_label("../x").is_err());
        assert!(parse_label(".hidden").is_err());
        assert!(parse_label("a b").is_err());
    }
}
