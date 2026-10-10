//! **Names at the edge** (#239, `docs/architecture.md` §3.5): what a person
//! types and reads, never what the protocol carries.
//!
//! A user addresses a journal by name, `paros://<tenant>/<journal>`
//! (`<tenant>/<journal>` on the command line). The URI names data, not a
//! location: it has no host. The wire carries the
//! `(TenantId, JournalId)` [`JournalIdentifier`] alone; a name is a label and
//! the id is the identity. A tenant's name lives in the universe directory
//! (`crate::fleet::FleetDirectory`), a journal's in its tenant's control
//! journal (`crate::system::Directory`). The entry roles resolve them
//! (`crate::client::names`); past them nothing knows a name.
//!
//! Where an id is still printed for operators, it is printed as abbreviated
//! hex, the way git abbreviates a commit hash ([`Abbreviations`]): the
//! shortest prefix of its 16 hex digits, at least [`MIN_ABBREV`] long, that
//! no other id of the same listing shares. A command that takes an id takes
//! a unique prefix ([`match_prefix`]).
//!
//! Pure and provider-free: no I/O, no randomness, wasm-safe.

use core::fmt;
use core::str::FromStr;

use paros_core::JournalIdentifier;

/// The URI scheme of a journal name.
pub const SCHEME: &str = "paros://";

/// The longest name a half of a [`JournalName`] may have, in bytes.
pub const MAX_NAME_BYTES: usize = 255;

/// A journal's name: its tenant's name and its own, inside that tenant.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JournalName {
    tenant: String,
    journal: String,
}

/// Why a text is not a [`JournalName`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NameError {
    /// No `/` between the tenant and the journal.
    NoSeparator,
    /// A half is empty.
    Empty,
    /// A half holds a `/`, a control character or a space.
    BadCharacter,
    /// A half is longer than [`MAX_NAME_BYTES`].
    TooLong,
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoSeparator => {
                "a journal name is <tenant>/<journal> or paros://<tenant>/<journal>"
            }
            Self::Empty => "a tenant name and a journal name are both non-empty",
            Self::BadCharacter => "a name holds no '/', no space and no control character",
            Self::TooLong => "a name is at most 255 bytes",
        })
    }
}

impl std::error::Error for NameError {}

/// Check one half of a name.
fn check(half: &str) -> Result<(), NameError> {
    if half.is_empty() {
        return Err(NameError::Empty);
    }
    if half.len() > MAX_NAME_BYTES {
        return Err(NameError::TooLong);
    }
    if half
        .chars()
        .any(|c| c == '/' || c.is_whitespace() || c.is_control())
    {
        return Err(NameError::BadCharacter);
    }
    Ok(())
}

impl JournalName {
    /// The name of journal `journal` in tenant `tenant`.
    ///
    /// # Errors
    ///
    /// A half is empty, too long, or holds a `/`, a space or a control
    /// character.
    ///
    /// # Panics
    ///
    /// Never: the assertion pins the postcondition.
    pub fn new(tenant: &str, journal: &str) -> Result<Self, NameError> {
        check(tenant)?;
        check(journal)?;
        let name = Self {
            tenant: tenant.to_string(),
            journal: journal.to_string(),
        };
        assert!(!name.tenant.is_empty() && !name.journal.is_empty());
        Ok(name)
    }

    /// The tenant's name.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// The journal's name, inside its tenant.
    #[must_use]
    pub fn journal(&self) -> &str {
        &self.journal
    }

    /// The command line's form, `<tenant>/<journal>`.
    #[must_use]
    pub fn short(&self) -> String {
        format!("{}/{}", self.tenant, self.journal)
    }
}

impl fmt::Display for JournalName {
    /// The URI, `paros://<tenant>/<journal>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SCHEME}{}/{}", self.tenant, self.journal)
    }
}

impl FromStr for JournalName {
    type Err = NameError;

    /// `paros://<tenant>/<journal>` or `<tenant>/<journal>`.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let rest = text.strip_prefix(SCHEME).unwrap_or(text);
        let (tenant, journal) = rest.split_once('/').ok_or(NameError::NoSeparator)?;
        Self::new(tenant, journal)
    }
}

/// The shortest abbreviation printed, in hex digits.
pub const MIN_ABBREV: usize = 6;

/// The hex digits of a `u64`: all of them.
pub const FULL_HEX: usize = 16;

const _: () = assert!(MIN_ABBREV <= FULL_HEX);
const _: () = assert!(MIN_ABBREV > 0);

/// An id's full form: 16 lowercase hex digits.
///
/// # Panics
///
/// Never: a `u64` has 16 hex digits.
#[must_use]
pub fn full_hex(id: u64) -> String {
    let text = format!("{id:016x}");
    assert_eq!(text.len(), FULL_HEX);
    text
}

/// The abbreviations of one listing's ids (git's `--abbrev`): one length
/// for the whole listing, the shortest at least [`MIN_ABBREV`] at which no
/// two distinct ids share a prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Abbreviations {
    len: usize,
}

impl Abbreviations {
    /// The abbreviations of `ids` (duplicates allowed).
    ///
    /// # Panics
    ///
    /// Never: distinct ids differ in a digit.
    pub fn new(ids: impl IntoIterator<Item = u64>) -> Self {
        let mut ids: Vec<String> = ids.into_iter().map(full_hex).collect();
        ids.sort_unstable();
        ids.dedup();
        // Two distinct ids share a prefix of `len` digits only if two
        // neighbours in sorted order do.
        let shared = ids
            .windows(2)
            .map(|pair| {
                pair[0]
                    .bytes()
                    .zip(pair[1].bytes())
                    .take_while(|(a, b)| a == b)
                    .count()
            })
            .max()
            .unwrap_or(0);
        assert!(shared < FULL_HEX, "distinct ids differ in a digit");
        let len = (shared + 1).clamp(MIN_ABBREV, FULL_HEX);
        assert!((MIN_ABBREV..=FULL_HEX).contains(&len));
        Self { len }
    }

    /// The listing's length, in hex digits.
    #[must_use]
    pub fn digits(&self) -> usize {
        self.len
    }

    /// Whether the listing prints ids in full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len == FULL_HEX
    }

    /// `id`, abbreviated.
    #[must_use]
    pub fn id(&self, id: u64) -> String {
        let mut text = full_hex(id);
        text.truncate(self.len);
        text
    }

    /// `journal`, abbreviated: `<tenant>/<journal>`.
    #[must_use]
    pub fn journal(&self, journal: JournalIdentifier) -> String {
        format!(
            "{}/{}",
            self.id(journal.tenant.0),
            self.id(journal.journal.0)
        )
    }
}

/// What a hex prefix matched among a listing's ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrefixMatch {
    /// Exactly one id.
    One(u64),
    /// No id.
    None,
    /// Several ids: a longer prefix is needed.
    Ambiguous(Vec<u64>),
}

/// Why a text is not a hex prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixError {
    /// Empty, or longer than 16 digits.
    Length,
    /// A character that is not a hex digit.
    NotHex,
}

impl fmt::Display for PrefixError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Length => "an id is 1 to 16 hex digits",
            Self::NotHex => "an id is hex digits",
        })
    }
}

impl std::error::Error for PrefixError {}

/// A hex prefix as typed: lowercased, checked.
///
/// # Errors
///
/// Empty, longer than 16 digits, or not hex.
pub fn parse_prefix(text: &str) -> Result<String, PrefixError> {
    if text.is_empty() || text.len() > FULL_HEX {
        return Err(PrefixError::Length);
    }
    if !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(PrefixError::NotHex);
    }
    Ok(text.to_ascii_lowercase())
}

/// A full id: exactly 16 hex digits, never `0` (unset, §3.8).
#[must_use]
pub fn parse_full(prefix: &str) -> Option<u64> {
    if prefix.len() != FULL_HEX {
        return None;
    }
    u64::from_str_radix(prefix, 16).ok().filter(|id| *id != 0)
}

/// The ids of `candidates` whose full hex starts with `prefix` (as
/// [`parse_prefix`] returns it). A full 16-digit prefix matches its own id
/// whether or not it is listed: a full id needs no listing.
pub fn match_prefix(prefix: &str, candidates: impl IntoIterator<Item = u64>) -> PrefixMatch {
    if let Some(id) = parse_full(prefix) {
        return PrefixMatch::One(id);
    }
    let mut found: Vec<u64> = candidates
        .into_iter()
        .filter(|id| *id != 0 && full_hex(*id).starts_with(prefix))
        .collect();
    found.sort_unstable();
    found.dedup();
    match found.len() {
        0 => PrefixMatch::None,
        1 => PrefixMatch::One(found[0]),
        _ => PrefixMatch::Ambiguous(found),
    }
}

#[cfg(test)]
mod tests {
    use paros_core::{JournalId, TenantId};

    use super::*;

    #[test]
    fn a_name_parses_from_both_forms_and_prints_as_a_uri() {
        let uri: JournalName = "paros://acme/orders".parse().unwrap();
        let short: JournalName = "acme/orders".parse().unwrap();
        assert_eq!(uri, short);
        assert_eq!(uri.tenant(), "acme");
        assert_eq!(uri.journal(), "orders");
        assert_eq!(uri.to_string(), "paros://acme/orders");
        assert_eq!(uri.short(), "acme/orders");
        assert_eq!(uri.to_string().parse::<JournalName>().unwrap(), uri);
    }

    #[test]
    fn a_malformed_name_is_refused() {
        assert_eq!("acme".parse::<JournalName>(), Err(NameError::NoSeparator));
        assert_eq!("/orders".parse::<JournalName>(), Err(NameError::Empty));
        assert_eq!("acme/".parse::<JournalName>(), Err(NameError::Empty));
        assert_eq!("paros:///x".parse::<JournalName>(), Err(NameError::Empty));
        assert_eq!(
            "acme/or/ders".parse::<JournalName>(),
            Err(NameError::BadCharacter)
        );
        assert_eq!(
            "ac me/orders".parse::<JournalName>(),
            Err(NameError::BadCharacter)
        );
        let long = "a".repeat(MAX_NAME_BYTES + 1);
        assert_eq!(
            format!("{long}/x").parse::<JournalName>(),
            Err(NameError::TooLong)
        );
    }

    #[test]
    fn abbreviations_widen_until_the_listing_is_unambiguous() {
        let lone = Abbreviations::new([0x2c94_f1aa_0000_0001]);
        assert_eq!(lone.digits(), MIN_ABBREV);
        assert_eq!(lone.id(0x2c94_f1aa_0000_0001), "2c94f1");
        // Two ids sharing their first eight digits print with nine.
        let pair = Abbreviations::new([0x2c94_f1aa_0000_0001, 0x2c94_f1aa_1000_0000]);
        assert_eq!(pair.digits(), 9);
        assert_eq!(pair.id(0x2c94_f1aa_0000_0001), "2c94f1aa0");
        assert_eq!(pair.id(0x2c94_f1aa_1000_0000), "2c94f1aa1");
        // A small id keeps its leading zeros: every id has 16 digits.
        assert_eq!(Abbreviations::new([7]).id(7), "000000");
        let journal = JournalIdentifier::new(TenantId(0xab << 56), JournalId(0xcd << 56));
        assert_eq!(lone.journal(journal), "ab0000/cd0000");
        assert!(Abbreviations::new([1, 2]).is_full());
    }

    #[test]
    fn a_prefix_matches_one_id_none_or_several() {
        let ids = [
            0x2c94_f1aa_0000_0001,
            0x2c94_f1aa_1000_0000,
            0x7000_0000_0000_0000,
        ];
        assert_eq!(match_prefix("7", ids), PrefixMatch::One(ids[2]));
        assert_eq!(match_prefix("2c94f1aa0", ids), PrefixMatch::One(ids[0]));
        assert_eq!(
            match_prefix("2c94", ids),
            PrefixMatch::Ambiguous(vec![ids[0], ids[1]])
        );
        assert_eq!(match_prefix("ff", ids), PrefixMatch::None);
        // A full id needs no listing; the unset one is never an id.
        assert_eq!(
            match_prefix("00000000000000ff", ids),
            PrefixMatch::One(0xff)
        );
        assert_eq!(match_prefix("0000000000000000", ids), PrefixMatch::None);
        assert_eq!(parse_prefix("2C94"), Ok("2c94".to_string()));
        assert_eq!(parse_prefix(""), Err(PrefixError::Length));
        assert_eq!(parse_prefix("xyz"), Err(PrefixError::NotHex));
        assert_eq!(parse_prefix(&"f".repeat(17)), Err(PrefixError::Length));
    }
}
