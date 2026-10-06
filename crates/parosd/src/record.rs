//! The provisioning record (#208): `<data-dir>/provisioned`, the
//! machine's memory of which journal stores it formatted, kept **outside
//! the stores**.
//!
//! A store's format marker says "this store was formatted"; it cannot say
//! "this machine meant to serve it", because an interrupted formation and a
//! completed one leave the same marker. The record is written only once
//! every store it names is formatted durably, so:
//!
//! - a cell's formation (#196, [`crate::machine_record`]) formats every
//!   journal of the plan, then writes this record, then commits the plan;
//!   an interrupted formation resumes from the disk (a store already
//!   formatted under the same configuration is left as it is);
//! - a journal the directory created on this node (#189) is a first boot
//!   until its store has booted once, and an existing member from then on
//!   — the record, not the journal's directory, remembers which.
//!
//! The record is a small text file, rewritten whole and atomically (a
//! temporary file, `fsync`, `rename`, `fsync` of the directory):
//!
//! ```text
//! role machine
//! id 6150928431937019931
//! journal 2965734451981346203/9861377130450924019
//! ```
//!
//! A journal is named by its identifier `<tenant>/<journal>` (#235).

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use paros::JournalIdentifier;
#[cfg(test)]
use paros::{JournalId, TenantId};

/// Parse a journal identifier written `<tenant>/<journal>` (#235), the form
/// [`JournalIdentifier`]'s `Display` renders.
#[must_use]
pub fn parse_identifier(text: &str) -> Option<JournalIdentifier> {
    text.contains('/').then(|| text.parse().ok()).flatten()
}

/// The record's file name under the data directory.
const FILE: &str = "provisioned";

/// What the record holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// The role the data directory was provisioned for.
    pub role: String,
    /// The identity in that role's list.
    pub id: u64,
    /// The journals whose stores are provisioned (a node's; empty for a
    /// matchmaker or a replica).
    pub journals: BTreeSet<JournalIdentifier>,
}

impl Record {
    /// The record's path under `data_dir`.
    #[must_use]
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join(FILE)
    }

    /// Read the record under `data_dir`; `None` when there is none.
    ///
    /// # Errors
    ///
    /// The file exists but cannot be read, or is not a record.
    pub fn read(data_dir: &Path) -> io::Result<Option<Self>> {
        let text = match fs::read_to_string(Self::path(data_dir)) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        Self::parse(&text)
            .map(Some)
            .map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))
    }

    fn parse(text: &str) -> Result<Self, String> {
        let mut role = None;
        let mut id = None;
        let mut journals = BTreeSet::new();
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let (key, value) = line
                .split_once(' ')
                .ok_or_else(|| format!("bad record line {line:?}"))?;
            match key {
                "role" => role = Some(value.to_string()),
                "id" => {
                    id = Some(
                        value
                            .parse()
                            .map_err(|e| format!("bad id {value:?}: {e}"))?,
                    );
                }
                "journal" => {
                    let journal =
                        parse_identifier(value).ok_or_else(|| format!("bad journal {value:?}"))?;
                    journals.insert(journal);
                }
                _ => return Err(format!("unknown record key {key:?}")),
            }
        }
        Ok(Self {
            role: role.ok_or("the record names no role")?,
            id: id.ok_or("the record names no id")?,
            journals,
        })
    }

    fn render(&self) -> String {
        let mut text = format!("role {}\nid {}\n", self.role, self.id);
        for journal in &self.journals {
            text.push_str("journal ");
            text.push_str(&journal.to_string());
            text.push('\n');
        }
        text
    }

    /// Write the record under `data_dir`, durably: on return it survives a
    /// crash, and a crash before it leaves the previous record (or none).
    ///
    /// # Errors
    ///
    /// Any filesystem failure.
    pub fn write(&self, data_dir: &Path) -> io::Result<()> {
        write_atomically(data_dir, FILE, &self.render())
    }

    /// Whether this record was written for `role` `id`.
    ///
    /// # Errors
    ///
    /// A description of the mismatch: the data directory belongs to
    /// another identity.
    pub fn check(&self, role: &str, id: u64) -> Result<(), String> {
        if self.role == role && self.id == id {
            return Ok(());
        }
        Err(format!(
            "the data directory was provisioned for {} {}, not {role} {id}",
            self.role, self.id
        ))
    }
}

/// Write `text` to `data_dir/name` whole and durably: a temporary file,
/// `fsync`, `rename`, `fsync` of the directory. On return it survives a
/// crash; a crash before it leaves the previous file (or none).
///
/// # Errors
///
/// Any filesystem failure.
pub fn write_atomically(data_dir: &Path, name: &str, text: &str) -> io::Result<()> {
    fs::create_dir_all(data_dir)?;
    let staged = data_dir.join(format!("{name}.tmp"));
    {
        let mut file = File::create(&staged)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&staged, data_dir.join(name))?;
    sync_dir(data_dir)
}

/// Make a directory's entries durable (the rename above).
#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips_and_names_its_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(Record::read(dir.path()).expect("read"), None);
        let record = Record {
            role: "node".into(),
            id: 3,
            journals: [
                JournalIdentifier::new(TenantId(7), JournalId(9)),
                JournalIdentifier::new(TenantId(300), JournalId(9_000)),
            ]
            .into_iter()
            .collect(),
        };
        record.write(dir.path()).expect("write");
        let read = Record::read(dir.path()).expect("read").expect("a record");
        assert_eq!(read, record);
        assert!(read.check("node", 3).is_ok());
        assert!(read.check("node", 4).is_err());
        assert!(read.check("replica", 3).is_err());
        assert!(!dir.path().join("provisioned.tmp").exists());
    }

    #[test]
    fn a_damaged_record_is_an_error_not_an_absence() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(Record::path(dir.path()), "role node\n").expect("write");
        assert!(Record::read(dir.path()).is_err());
    }
}
