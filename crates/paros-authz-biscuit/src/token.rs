//! Minting, deriving, sealing and printing tokens, all offline.
//!
//! The authority block names a role, the tenant for the `tenant` role (by
//! its name), a `subject` label and an expiry check. A derived block holds
//! checks only: a fact in a derived block is visible to that block alone,
//! so it can never widen a token. Every value from a caller enters through
//! a Datalog parameter, never through the source text.

use std::fmt::Write as _;
use std::time::SystemTime;

use biscuit_auth::builder::{self, BlockBuilder, Check, Fact, Term};
use biscuit_auth::datalog::SymbolTable;
use biscuit_auth::{Biscuit, PrivateKey, UnverifiedBiscuit};

use crate::keys::KeyRing;
use crate::{Class, Entropy, Error, RootKey};

/// What a token's authority block grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// The universe, and everything below it.
    Admin,
    /// The data plane and journals of the one tenant named.
    Tenant(String),
}

impl Role {
    /// The fact's value: `role("admin")`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Tenant(_) => "tenant",
        }
    }
}

/// The content of an authority block.
#[derive(Clone, Debug)]
pub struct Grant {
    /// The role granted.
    pub role: Role,
    /// Who the token is for: a label for logs, never read by a policy.
    pub subject: String,
    /// The last instant the token is valid.
    pub expires: SystemTime,
}

/// The checks a derived block adds. Each `None` or `false` adds nothing.
#[derive(Clone, Debug, Default)]
pub struct Restriction {
    /// Only operations that change nothing.
    pub read_only: bool,
    /// Only requests to this tenant, by name.
    pub tenant: Option<String>,
    /// Only requests to this journal, by name.
    pub journal: Option<String>,
    /// Only these operation classes.
    pub classes: Option<Vec<Class>>,
    /// An earlier expiry.
    pub expires: Option<SystemTime>,
}

impl Restriction {
    fn is_empty(&self) -> bool {
        !self.read_only
            && self.tenant.is_none()
            && self.journal.is_none()
            && self.classes.is_none()
            && self.expires.is_none()
    }
}

/// A serialized token. Its text form is base64url, Biscuit's own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token(Vec<u8>);

impl Token {
    /// A token from its bytes, unchecked.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The token's bytes, as requests carry them.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// A token from its base64url text.
    ///
    /// # Errors
    ///
    /// [`Error::BadToken`] when the text is not a token.
    pub fn from_text(text: &str) -> Result<Self, Error> {
        let token = UnverifiedBiscuit::from_base64(text.trim())?;
        Ok(Self(token.to_vec()?))
    }

    /// The token's base64url text.
    ///
    /// # Panics
    ///
    /// Never: the bytes came from a parsed or built token.
    #[must_use]
    pub fn to_text(&self) -> String {
        UnverifiedBiscuit::from(&self.0)
            .and_then(|token| token.to_base64())
            .expect("a token's bytes always parse")
    }

    fn unverified(&self) -> Result<UnverifiedBiscuit, Error> {
        Ok(UnverifiedBiscuit::from(&self.0)?)
    }
}

/// A check from Datalog source with one parameter set.
fn check_with(source: &str, name: &str, value: Term) -> Result<Check, Error> {
    let mut check = Check::try_from(source)?;
    check.set(name, value)?;
    Ok(check)
}

fn expiry_check(expires: SystemTime) -> Result<Check, Error> {
    check_with(
        "check if time($t), $t <= {expires}",
        "expires",
        builder::date(&expires),
    )
}

/// A new token signed by `key`, valid at `now` until `grant.expires`.
///
/// # Errors
///
/// [`Error::BadArgument`] when the expiry is not after `now` or a name is
/// empty; [`Error::BadToken`] when Biscuit refuses the block.
pub fn mint(
    key: &RootKey,
    grant: &Grant,
    now: SystemTime,
    entropy: &Entropy,
) -> Result<Token, Error> {
    if grant.expires <= now {
        return Err(Error::BadArgument("the expiry is not in the future".into()));
    }
    let mut block = Biscuit::builder()
        .fact(builder::fact("role", &[builder::string(grant.role.name())]))?
        .fact(builder::fact("subject", &[builder::string(&grant.subject)]))?
        .check(expiry_check(grant.expires)?)?
        .root_key_id(key.key_id());
    if let Role::Tenant(tenant) = &grant.role {
        if tenant.is_empty() {
            return Err(Error::BadArgument("a tenant token names a tenant".into()));
        }
        let fact: Fact = builder::fact("tenant", &[builder::string(tenant)]);
        block = block.fact(fact)?;
    }
    let token = block.build_with_rng(
        key.private_key(),
        SymbolTable::default(),
        &mut entropy.rng(),
    )?;
    Ok(Token(token.to_vec()?))
}

/// The derived block of `restriction`.
fn restriction_block(restriction: &Restriction) -> Result<BlockBuilder, Error> {
    let mut block = BlockBuilder::new();
    if restriction.read_only {
        block = block.check(Check::try_from(r#"check if access("read")"#)?)?;
    }
    if let Some(tenant) = &restriction.tenant {
        block = block.check(check_with(
            "check if target_tenant({tenant})",
            "tenant",
            builder::string(tenant),
        )?)?;
    }
    if let Some(journal) = &restriction.journal {
        block = block.check(check_with(
            "check if target_journal({journal})",
            "journal",
            builder::string(journal),
        )?)?;
    }
    if let Some(classes) = &restriction.classes {
        let set = classes
            .iter()
            .map(|class| builder::string(class.name()))
            .collect();
        block = block.check(check_with(
            "check if op_class($c), {classes}.contains($c)",
            "classes",
            builder::set(set),
        )?)?;
    }
    if let Some(expires) = restriction.expires {
        block = block.check(expiry_check(expires)?)?;
    }
    Ok(block)
}

/// A narrower token derived from `token`, offline, macaroon style: the key
/// that signs the new block is the last one inside `token`, and the next
/// one comes from `entropy`. No root key is needed and no signature is
/// checked.
///
/// # Errors
///
/// [`Error::BadArgument`] for an empty restriction; [`Error::BadToken`]
/// when `token` does not parse or is sealed.
pub fn derive(token: &Token, restriction: &Restriction, entropy: &Entropy) -> Result<Token, Error> {
    if restriction.is_empty() {
        return Err(Error::BadArgument(
            "a derived token restricts something".into(),
        ));
    }
    let next = PrivateKey::new_with_rng(
        biscuit_auth::builder::Algorithm::Ed25519,
        &mut entropy.rng(),
    );
    let derived = token
        .unverified()?
        .append_with_key(&next, restriction_block(restriction)?)
        .map_err(|error| match error {
            biscuit_auth::error::Token::AppendOnSealed
            | biscuit_auth::error::Token::AlreadySealed => {
                Error::BadToken("the token is sealed: nothing can be derived from it".into())
            }
            other => other.into(),
        })?;
    Ok(Token(derived.to_vec()?))
}

/// `token` sealed: its last key is replaced by a signature, so nothing can
/// be derived from it any more.
///
/// # Errors
///
/// [`Error::BadToken`] when `token` does not parse or is already sealed.
pub fn seal(token: &Token) -> Result<Token, Error> {
    Ok(Token(token.unverified()?.seal()?.to_vec()?))
}

/// Every block of `token` as Datalog source, and its root key id. With a
/// ring, the signature is checked first.
///
/// # Errors
///
/// [`Error::BadToken`] when `token` does not parse or, with a ring, its
/// signature does not verify.
pub fn inspect(token: &Token, ring: Option<&KeyRing>) -> Result<String, Error> {
    let unverified = token.unverified()?;
    if let Some(ring) = ring {
        Biscuit::from(token.as_bytes(), |id| ring.choose(id))?;
    }
    let mut text = match unverified.root_key_id() {
        Some(id) => format!("root key id: {id}\n"),
        None => "root key id: none\n".to_string(),
    };
    for index in 0..unverified.block_count() {
        let source = unverified.print_block_source(index)?;
        let _ = writeln!(text, "block {index}:\n{source}");
    }
    Ok(text)
}
