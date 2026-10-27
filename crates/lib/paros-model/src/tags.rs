//! **Key:value tags** (#575 (key:value tags), `docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.9):
//! one mechanism for what tenants, journals, machines and cells say about
//! themselves.
//!
//! A [`Tag`] is a key and a value. Two kinds of key exist:
//!
//! - **Reserved keys**, under [`RESERVED_PREFIX`] (`paros/`). Only the
//!   control plane writes them, each through its own command, so a user can
//!   never fake one. Each entity's fold parses its reserved tags into typed
//!   policies (`crate::fleet::TenantKind`, `crate::fleet::Movable`,
//!   `crate::machine::Class`, `crate::fleet::CellKind`) and never reads the
//!   string again: policies are explicit types, never flags. [`reserved`]
//!   names every reserved key.
//! - **User keys**: any other key. paros never interprets them. A
//!   [`UserTags`] set holds at most [`MAX_TAGS`] of them; it is judged where
//!   it arrives, and a request over the cap is refused, never a panic.
//!
//! A [`Selector`] picks entities by their tags (`team:payments,env:prod`;
//! `tenant.team:payments` matches a journal by its tenant's tags: no tag is
//! inherited or copied).
//!
//! Pure and provider-free: no I/O, no randomness, wasm-safe.

use core::fmt;
use core::str::FromStr;
use std::collections::BTreeMap;

use paros_wire::common as wire;

/// The prefix of every reserved key: only the control plane writes one.
pub const RESERVED_PREFIX: &str = "paros/";

/// The most user tags one entity carries.
pub const MAX_TAGS: usize = 32;

/// The longest key, in bytes.
pub const MAX_KEY_BYTES: usize = 63;

/// The longest value, in bytes.
pub const MAX_VALUE_BYTES: usize = 255;

/// The prefix of a selector term that reads a journal's tenant's tags.
pub const TENANT_SCOPE: &str = "tenant.";

const _: () = assert!(RESERVED_PREFIX.len() < MAX_KEY_BYTES);
const _: () = assert!(MAX_TAGS > 0 && MAX_KEY_BYTES > 0 && MAX_VALUE_BYTES > 0);

/// The reserved keys (#575): each one is written by its own command only.
pub mod reserved {
    /// A tenant's kind: `internal` or `users`.
    pub const KIND: &str = "paros/kind";
    /// An internal tenant's role: `universe` or `cell`.
    pub const ROLE: &str = "paros/role";
    /// Whether a tenant may move to another cell: `true` or `false`.
    pub const MOVABLE: &str = "paros/movable";
    /// A machine's class: `storage` or `stateless`.
    pub const CLASS: &str = "paros/class";
    /// A machine's zone, its failure domain.
    pub const ZONE: &str = "paros/zone";
    /// A machine's rack, inside its zone.
    pub const RACK: &str = "paros/rack";
    /// A regional cell's region.
    pub const REGION: &str = "paros/region";
    /// A multi-region cell's regions, `+`-separated.
    pub const REGIONS: &str = "paros/regions";
    /// A multi-region cell's witness region.
    pub const WITNESS: &str = "paros/witness";
}

/// Why a text or a set is not a valid tag or tag set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TagError {
    /// A `key:value` text holds no `:`.
    NoSeparator,
    /// The key is empty, or has an empty half around its `/`.
    EmptyKey,
    /// The key is longer than [`MAX_KEY_BYTES`].
    KeyTooLong,
    /// The key holds a character outside `a-z`, `0-9`, `-`, `_`, or more
    /// than one `/`.
    BadKey,
    /// The value is longer than [`MAX_VALUE_BYTES`].
    ValueTooLong,
    /// The value holds a control character or a `,`.
    BadValue,
    /// A set names one key twice.
    Duplicate(String),
    /// A set holds more than [`MAX_TAGS`] user tags.
    TooMany,
    /// A user set names a reserved key: only the control plane writes one.
    Reserved(String),
    /// A reserved key this entity does not carry, or a reserved value its
    /// policy does not know.
    UnknownReserved(String),
}

impl TagError {
    /// The refusal's label on the wire and in `parosctl --json`.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            TagError::NoSeparator
            | TagError::EmptyKey
            | TagError::KeyTooLong
            | TagError::BadKey
            | TagError::ValueTooLong
            | TagError::BadValue
            | TagError::Duplicate(_) => "malformed_tag",
            TagError::TooMany => "too_many_tags",
            TagError::Reserved(_) | TagError::UnknownReserved(_) => "reserved_tag",
        }
    }
}

impl fmt::Display for TagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TagError::NoSeparator => f.write_str("a tag is key:value"),
            TagError::EmptyKey => f.write_str("a tag key is [prefix/]name, both non-empty"),
            TagError::KeyTooLong => write!(f, "a tag key is at most {MAX_KEY_BYTES} bytes"),
            TagError::BadKey => {
                f.write_str("a tag key holds only a-z, 0-9, '-' and '_', and at most one '/'")
            }
            TagError::ValueTooLong => write!(f, "a tag value is at most {MAX_VALUE_BYTES} bytes"),
            TagError::BadValue => f.write_str("a tag value holds no control character and no ','"),
            TagError::Duplicate(key) => write!(f, "the tag key {key} is named twice"),
            TagError::TooMany => write!(f, "an entity carries at most {MAX_TAGS} user tags"),
            TagError::Reserved(key) => {
                write!(f, "{key} is reserved: only paros writes a paros/ key")
            }
            TagError::UnknownReserved(key) => write!(f, "{key} is not a reserved tag here"),
        }
    }
}

impl std::error::Error for TagError {}

/// Check a key: `[prefix/]name`, each half non-empty, of `a-z`, `0-9`, `-`
/// and `_`, at most [`MAX_KEY_BYTES`] bytes.
fn check_key(key: &str) -> Result<(), TagError> {
    if key.len() > MAX_KEY_BYTES {
        return Err(TagError::KeyTooLong);
    }
    let mut halves = key.split('/');
    let first = halves.next().unwrap_or_default();
    let second = halves.next();
    if halves.next().is_some() {
        return Err(TagError::BadKey);
    }
    for half in core::iter::once(first).chain(second) {
        if half.is_empty() {
            return Err(TagError::EmptyKey);
        }
        let valid = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_';
        if !half.chars().all(valid) {
            return Err(TagError::BadKey);
        }
    }
    Ok(())
}

/// Check a value: printable UTF-8 with no `,`, at most [`MAX_VALUE_BYTES`].
fn check_value(value: &str) -> Result<(), TagError> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(TagError::ValueTooLong);
    }
    if value.chars().any(|c| c.is_control() || c == ',') {
        return Err(TagError::BadValue);
    }
    Ok(())
}

/// Whether `key` is reserved: under [`RESERVED_PREFIX`].
#[must_use]
pub fn is_reserved(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX)
}

/// The value of `key` in a wire list of tags (a view's), if it holds it.
#[must_use]
pub fn value_of<'a>(tags: &'a [wire::Tag], key: &str) -> Option<&'a str> {
    tags.iter()
        .find(|tag| tag.key == key)
        .map(|tag| tag.value.as_str())
}

/// One `key:value` tag, checked.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tag {
    key: String,
    value: String,
}

impl Tag {
    /// The tag `key:value`.
    ///
    /// # Errors
    ///
    /// The key or the value is malformed (see [`TagError`]).
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Result<Self, TagError> {
        let (key, value) = (key.into(), value.into());
        check_key(&key)?;
        check_value(&value)?;
        Ok(Self { key, value })
    }

    /// A reserved tag the control plane writes.
    ///
    /// # Panics
    ///
    /// If `key` is not reserved, or the tag is malformed: reserved tags are
    /// paros's own constants and typed values, never external input.
    #[must_use]
    pub fn reserved(key: &str, value: impl Into<String>) -> Self {
        assert!(is_reserved(key), "a reserved tag has a paros/ key");
        Self::new(key, value).expect("a reserved tag is well formed")
    }

    /// Its key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Its value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Whether its key is reserved.
    #[must_use]
    pub fn is_reserved(&self) -> bool {
        is_reserved(&self.key)
    }

    /// The wire's form.
    #[must_use]
    pub fn to_wire(&self) -> wire::Tag {
        wire::Tag {
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }

    /// Read one wire tag back.
    ///
    /// # Errors
    ///
    /// The tag is malformed.
    pub fn from_wire(tag: wire::Tag) -> Result<Self, TagError> {
        Self::new(tag.key, tag.value)
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.key, self.value)
    }
}

impl FromStr for Tag {
    type Err = TagError;

    /// `key:value`, split at the first `:` (a key holds none).
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (key, value) = text.split_once(':').ok_or(TagError::NoSeparator)?;
        Self::new(key, value)
    }
}

/// An entity's tags, each key once, in key order: reserved and user tags
/// together, as a view shows them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Tags(BTreeMap<String, String>);

impl Tags {
    /// No tag.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The set of `tags`.
    ///
    /// # Errors
    ///
    /// A key is named twice.
    pub fn of(tags: impl IntoIterator<Item = Tag>) -> Result<Self, TagError> {
        let mut set = BTreeMap::new();
        for tag in tags {
            if set.contains_key(&tag.key) {
                return Err(TagError::Duplicate(tag.key));
            }
            set.insert(tag.key, tag.value);
        }
        Ok(Self(set))
    }

    /// The value of `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// How many tags it holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether it holds none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every tag, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Every tag, in key order.
    #[must_use]
    pub fn tags(&self) -> Vec<Tag> {
        self.0
            .iter()
            .map(|(key, value)| Tag {
                key: key.clone(),
                value: value.clone(),
            })
            .collect()
    }

    /// The wire's form: one tag per key, in key order.
    #[must_use]
    pub fn to_wire(&self) -> Vec<wire::Tag> {
        self.tags().iter().map(Tag::to_wire).collect()
    }

    /// Read a wire list back.
    ///
    /// # Errors
    ///
    /// A tag is malformed, or a key is named twice.
    pub fn from_wire(tags: Vec<wire::Tag>) -> Result<Self, TagError> {
        Self::of(
            tags.into_iter()
                .map(Tag::from_wire)
                .collect::<Result<Vec<_>, _>>()?,
        )
    }

    /// Split into the reserved tags (key to value) and the user tags.
    ///
    /// # Errors
    ///
    /// The user tags are over the cap.
    pub fn split(self) -> Result<(BTreeMap<String, String>, UserTags), TagError> {
        let (reserved, user): (BTreeMap<_, _>, BTreeMap<_, _>) =
            self.0.into_iter().partition(|(key, _)| is_reserved(key));
        Ok((reserved, UserTags::new(Tags(user))?))
    }

    /// `self` with `tags` added after it (a reserved tag a view shows
    /// beside the user tags).
    ///
    /// # Panics
    ///
    /// If both name a key: an entity's reserved and user tags never share
    /// one.
    #[must_use]
    pub fn with(mut self, tags: impl IntoIterator<Item = Tag>) -> Self {
        for tag in tags {
            let held = self.0.insert(tag.key, tag.value);
            assert!(held.is_none(), "reserved and user tags never share a key");
        }
        self
    }

    /// The tags in their text form, `key:value`, comma-separated.
    #[must_use]
    pub fn label(&self) -> String {
        self.tags()
            .iter()
            .map(Tag::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// An entity's user tags: no reserved key, at most [`MAX_TAGS`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct UserTags(Tags);

impl UserTags {
    /// No user tag.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// The user tags `tags`.
    ///
    /// # Errors
    ///
    /// A key is reserved, or there are more than [`MAX_TAGS`].
    pub fn new(tags: Tags) -> Result<Self, TagError> {
        if let Some((key, _)) = tags.iter().find(|(key, _)| is_reserved(key)) {
            return Err(TagError::Reserved(key.to_string()));
        }
        if tags.len() > MAX_TAGS {
            return Err(TagError::TooMany);
        }
        Ok(Self(tags))
    }

    /// The user tags `key:value` texts name.
    ///
    /// # Errors
    ///
    /// A text is malformed, a key is reserved or named twice, or there are
    /// more than [`MAX_TAGS`].
    pub fn parse<S: AsRef<str>>(texts: &[S]) -> Result<Self, TagError> {
        let tags = texts
            .iter()
            .map(|text| text.as_ref().parse::<Tag>())
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(Tags::of(tags)?)
    }

    /// The tags.
    #[must_use]
    pub fn tags(&self) -> &Tags {
        &self.0
    }

    /// Whether it holds none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The wire's form.
    #[must_use]
    pub fn to_wire(&self) -> Vec<wire::Tag> {
        self.0.to_wire()
    }

    /// Read a wire list back.
    ///
    /// # Errors
    ///
    /// A tag is malformed or reserved, a key is named twice, or there are
    /// more than [`MAX_TAGS`].
    pub fn from_wire(tags: Vec<wire::Tag>) -> Result<Self, TagError> {
        Self::new(Tags::from_wire(tags)?)
    }
}

/// One term of a [`Selector`].
#[derive(Clone, Debug, PartialEq, Eq)]
struct Term {
    /// Whether it reads the tenant's tags (`tenant.key`), not the entity's.
    tenant: bool,
    key: String,
    /// `None`: the key is present, with any value.
    value: Option<String>,
}

/// A selector (`-l`): comma-separated terms, all of which must match.
/// `key:value` asks for the value, `key` for the key with any value, and
/// `tenant.key:value` reads a journal's tenant's tags instead of its own.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Selector(Vec<Term>);

impl Selector {
    /// Whether the selector has no term (it matches everything).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `own` (the entity's tags) and `tenant` (its tenant's, for a
    /// journal) match every term. A `tenant.` term never matches without a
    /// tenant.
    #[must_use]
    pub fn matches(&self, own: &Tags, tenant: Option<&Tags>) -> bool {
        self.0.iter().all(|term| {
            let tags = if term.tenant { tenant } else { Some(own) };
            let held = tags.and_then(|tags| tags.get(&term.key));
            match (&term.value, held) {
                (_, None) => false,
                (None, Some(_)) => true,
                (Some(want), Some(held)) => want == held,
            }
        })
    }
}

impl FromStr for Selector {
    type Err = TagError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut terms = Vec::new();
        for term in text.split(',').filter(|term| !term.is_empty()) {
            let (scoped, tenant) = match term.strip_prefix(TENANT_SCOPE) {
                Some(rest) => (rest, true),
                None => (term, false),
            };
            let (key, value) = match scoped.split_once(':') {
                Some((key, value)) => {
                    check_value(value)?;
                    (key, Some(value.to_string()))
                }
                None => (scoped, None),
            };
            check_key(key)?;
            terms.push(Term {
                tenant,
                key: key.to_string(),
                value,
            });
        }
        Ok(Self(terms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(texts: &[&str]) -> Result<UserTags, TagError> {
        UserTags::parse(texts)
    }

    #[test]
    fn a_tag_reads_back_from_its_text_and_its_wire() {
        for text in ["team:payments", "env:", "disk:nvme", "k8s/app:orders:v2"] {
            let tag: Tag = text.parse().unwrap();
            assert_eq!(tag.to_string(), text);
            assert_eq!(Tag::from_wire(tag.to_wire()), Ok(tag));
        }
        let tag: Tag = "a/b:x:y".parse().unwrap();
        assert_eq!((tag.key(), tag.value()), ("a/b", "x:y"));
    }

    #[test]
    fn a_malformed_tag_is_refused_never_a_panic() {
        let refused = |text: &str| text.parse::<Tag>().unwrap_err();
        assert_eq!(refused("team"), TagError::NoSeparator);
        assert_eq!(refused(":x"), TagError::EmptyKey);
        assert_eq!(refused("a/:x"), TagError::EmptyKey);
        assert_eq!(refused("/a:x"), TagError::EmptyKey);
        assert_eq!(refused("Team:x"), TagError::BadKey);
        assert_eq!(refused("a/b/c:x"), TagError::BadKey);
        assert_eq!(refused("a.b:x"), TagError::BadKey);
        assert_eq!(refused("a b:x"), TagError::BadKey);
        assert_eq!(refused("a:x,y"), TagError::BadValue);
        assert_eq!(refused("a:x\ny"), TagError::BadValue);
        let long_key = format!("{}:x", "k".repeat(MAX_KEY_BYTES + 1));
        assert_eq!(refused(&long_key), TagError::KeyTooLong);
        let longest_key = format!("{}:x", "k".repeat(MAX_KEY_BYTES));
        assert!(longest_key.parse::<Tag>().is_ok());
        let long_value = format!("k:{}", "v".repeat(MAX_VALUE_BYTES + 1));
        assert_eq!(refused(&long_value), TagError::ValueTooLong);
        let longest_value = format!("k:{}", "é".repeat(MAX_VALUE_BYTES / 2));
        assert!(longest_value.parse::<Tag>().is_ok());
    }

    #[test]
    fn user_tags_refuse_a_reserved_key_a_repeat_and_the_cap() {
        assert!(user(&["team:a", "env:prod"]).is_ok());
        assert_eq!(
            user(&["paros/kind:internal"]),
            Err(TagError::Reserved("paros/kind".into()))
        );
        assert_eq!(
            user(&["team:a", "team:b"]),
            Err(TagError::Duplicate("team".into()))
        );
        let at_cap: Vec<String> = (0..MAX_TAGS).map(|i| format!("k{i}:v")).collect();
        assert!(UserTags::parse(&at_cap).is_ok());
        let over: Vec<String> = (0..=MAX_TAGS).map(|i| format!("k{i}:v")).collect();
        assert_eq!(UserTags::parse(&over), Err(TagError::TooMany));
        assert_eq!(TagError::TooMany.label(), "too_many_tags");
        assert_eq!(TagError::Reserved(String::new()).label(), "reserved_tag");
    }

    #[test]
    fn a_split_separates_reserved_from_user_tags() {
        let tags = Tags::of(
            ["paros/class:storage", "disk:nvme"]
                .iter()
                .map(|t| t.parse().unwrap()),
        )
        .unwrap();
        let (reserved, user) = tags.clone().split().unwrap();
        assert_eq!(
            reserved.get(reserved::CLASS).map(String::as_str),
            Some("storage")
        );
        assert_eq!(user.tags().get("disk"), Some("nvme"));
        assert_eq!(user.tags().get(reserved::CLASS), None);
        let back = user
            .tags()
            .clone()
            .with([Tag::reserved(reserved::CLASS, "storage")]);
        assert_eq!(back, tags);
        assert_eq!(back.label(), "disk:nvme,paros/class:storage");
    }

    #[test]
    fn a_selector_matches_every_term_and_reads_the_tenant_only_when_asked() {
        let own = Tags::of(
            ["team:payments", "env:prod"]
                .iter()
                .map(|t| t.parse().unwrap()),
        )
        .unwrap();
        let tenant = Tags::of(["team:core".parse().unwrap()]).unwrap();
        let pick = |text: &str, tenant: Option<&Tags>| {
            text.parse::<Selector>().unwrap().matches(&own, tenant)
        };
        assert!(pick("", None));
        assert!(pick("team:payments", None));
        assert!(pick("team:payments,env:prod", None));
        assert!(!pick("team:payments,env:dev", None));
        assert!(pick("env", None));
        assert!(!pick("disk", None));
        assert!(pick("tenant.team:core", Some(&tenant)));
        assert!(!pick("tenant.team:payments", Some(&tenant)));
        assert!(!pick("tenant.team:core", None));
        assert!("Team:x".parse::<Selector>().is_err());
        assert!("a:b\n".parse::<Selector>().is_err());
    }
}
