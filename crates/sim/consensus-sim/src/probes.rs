//! The coverage probes that `paros-consensus` declares.
//!
//! A `probe!` site is in the assertion table only once some run evaluates
//! it, so a probe that no walk reaches is invisible to a table. The list of
//! every probe comes from the source of the core: each `probe!(` call, and
//! the last string literal before its closing parenthesis, which is its
//! message (`paros-consensus/src/probe.rs`).

use std::fs;
use std::path::{Path, PathBuf};

/// The bytes of a message that an assertion slot keeps (moonpool's
/// `SLOT_MSG_LEN` less its terminator).
pub const SLOT_KEEPS: usize = 63;

/// One `probe!` site of the core.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Probe {
    /// The file, relative to `paros-consensus/src`.
    pub file: String,
    /// The message, whole, as the source writes it.
    pub message: String,
}

impl Probe {
    /// The name the assertion table reports for this probe: the message cut
    /// to [`SLOT_KEEPS`] bytes, at a character boundary. A longer message
    /// is still one slot (the slot is the hash of the whole message), but
    /// the table shows only this much of it.
    #[must_use]
    pub fn key(&self) -> &str {
        let mut end = self.message.len().min(SLOT_KEEPS);
        while !self.message.is_char_boundary(end) {
            end -= 1;
        }
        &self.message[..end]
    }
}

/// The source folder of `paros-consensus`, beside this crate.
#[must_use]
pub fn core_source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../lib/paros-consensus/src")
}

/// Every `probe!` site of the core, sorted by file then message.
///
/// # Panics
///
/// When the source of the core cannot be read, or a `probe!` call has no
/// message: both are a broken checkout, not a gap.
#[must_use]
pub fn core_probes() -> Vec<Probe> {
    let root = core_source();
    let mut files = Vec::new();
    collect(&root, &mut files);
    let mut probes = Vec::new();
    for path in files {
        // `probe.rs` defines the macro; its doc comment names `probe!(`.
        if path.file_name().is_some_and(|name| name == "probe.rs") {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let file = path
            .strip_prefix(&root)
            .map_or_else(|_| path.display().to_string(), |p| p.display().to_string());
        for message in messages(&text) {
            probes.push(Probe {
                file: file.clone(),
                message,
            });
        }
    }
    probes.sort();
    probes
}

fn collect(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|error| panic!("read {}: {error}", dir.display()));
    for entry in entries {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            collect(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

/// The message of each `probe!(` call in `text`.
fn messages(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("probe!(") {
        let body = &rest[at + "probe!(".len()..];
        let (message, used) = call(body);
        let message = message.unwrap_or_else(|| panic!("a probe! call has no message: {body:.80}"));
        found.push(message);
        rest = &body[used..];
    }
    found
}

/// Scan one call up to its closing parenthesis: the last string literal in
/// it, and the bytes it used.
fn call(body: &str) -> (Option<String>, usize) {
    let bytes = body.as_bytes();
    let mut depth = 1_usize;
    let mut last = None;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return (last, index + 1);
                }
            }
            b'"' => {
                let start = index + 1;
                index = start;
                while index < bytes.len() && bytes[index] != b'"' {
                    if bytes[index] == b'\\' {
                        index += 1;
                    }
                    index += 1;
                }
                last = Some(body[start..index].to_owned());
            }
            _ => {}
        }
        index += 1;
    }
    (last, bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_probe_message_is_its_last_literal() {
        let text = r#"
            probe!(reachable, "a: one");
            probe!(
                sometimes,
                f(x, "not this") && (y),
                "b: two"
            );
        "#;
        assert_eq!(messages(text), vec!["a: one", "b: two"]);
    }

    #[test]
    fn the_core_declares_probes_the_table_tells_apart() {
        let probes = core_probes();
        assert!(probes.len() > 50, "only {} probes", probes.len());
        let mut keys: Vec<&str> = probes.iter().map(Probe::key).collect();
        keys.sort_unstable();
        let before = keys.len();
        keys.dedup();
        assert_eq!(
            keys.len(),
            before,
            "two probes share the name the table keeps"
        );
    }

    #[test]
    fn a_long_message_is_cut_where_the_table_cuts_it() {
        let probe = Probe {
            file: String::new(),
            message: "é".repeat(40),
        };
        assert_eq!(probe.key().len(), 62, "cut at a character boundary");
    }
}
