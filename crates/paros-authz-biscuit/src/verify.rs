//! The verifier of one request: signature, then one Datalog policy.
//!
//! The verifier adds facts that describe the request, never the caller:
//! the time, the operation, its class and access, and the target tenant,
//! its kind and the target journal, each by name. The meaning of a role is
//! [`POLICY`], here in the server, so a role changes without new tokens.
//!
//! #245 rules 2 to 4: the wall-clock budget is out of reach (a day, not
//! `Duration::MAX`, which overflows `start + max_time`); facts and
//! iterations bound the evaluation and count deterministically; the
//! execution time is never read; a refusal is judged by its kind only.

use std::time::{Duration, SystemTime};

use biscuit_auth::builder::{self, Fact};
use biscuit_auth::error::{FailedCheck, Logic, Token as TokenError};
use biscuit_auth::{AuthorizerBuilder, AuthorizerLimits, Biscuit};

use crate::{KeyRing, Operation, Token};

/// The policy every request is checked against. A policy sees only the
/// authority block and the verifier's facts; every check of every block
/// must pass as well.
pub const POLICY: &str = r#"
allow if role("admin");
allow if role("tenant"), tenant($t), target_tenant($t), target_kind("users"),
         op_class($c), ["tenant-view", "journal", "data"].contains($c);
deny if true;
"#;

/// A token larger than this is refused before it is parsed.
pub const MAX_TOKEN_BYTES: usize = 4096;
/// A token with more blocks than this is refused.
pub const MAX_BLOCKS: usize = 8;

/// The kind of the tenant a request targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetKind {
    /// A tenant of the tenant API, named.
    Users,
    /// A universe or cell tenant: it has no resolvable name, so a request
    /// to it names no `target_tenant`, and only `admin` reaches it.
    Internal,
}

/// What a request asks, as the verifier states it.
#[derive(Clone, Debug)]
pub struct Request<'a> {
    /// The call.
    pub operation: Operation,
    /// The target tenant's name, for a `users` tenant.
    pub tenant: Option<&'a str>,
    /// The target tenant's kind, when the call targets a tenant.
    pub kind: Option<TargetKind>,
    /// The target journal's name, when the call names a journal.
    pub journal: Option<&'a str>,
    /// The verifier's clock, from its provider.
    pub now: SystemTime,
}

/// Why a request is refused. Judged by kind only: never by which check
/// failed first, since the engine's order is per process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The token does not parse, is too large, or its signature or root
    /// key id does not verify.
    InvalidToken,
    /// An expiry check failed.
    Expired,
    /// Any other check, or the policy, refused the request.
    Forbidden,
}

fn limits() -> AuthorizerLimits {
    AuthorizerLimits {
        max_facts: 1000,
        max_iterations: 100,
        max_time: Duration::from_hours(24),
    }
}

fn request_facts(request: &Request<'_>) -> Vec<Fact> {
    let op = request.operation;
    let mut facts = vec![
        builder::fact("time", &[builder::date(&request.now)]),
        builder::fact("operation", &[builder::string(op.name())]),
        builder::fact("op_class", &[builder::string(op.class().name())]),
        builder::fact("access", &[builder::string(op.access().name())]),
    ];
    if let Some(kind) = request.kind {
        let name = match kind {
            TargetKind::Users => "users",
            TargetKind::Internal => "internal",
        };
        facts.push(builder::fact("target_kind", &[builder::string(name)]));
    }
    if let (Some(tenant), Some(TargetKind::Users)) = (request.tenant, request.kind) {
        facts.push(builder::fact("target_tenant", &[builder::string(tenant)]));
    }
    if let Some(journal) = request.journal {
        facts.push(builder::fact("target_journal", &[builder::string(journal)]));
    }
    facts
}

/// Whether `failed` holds an expiry check. The set of failed checks does
/// not depend on evaluation order; which one is first does.
fn expired(failed: &[FailedCheck]) -> bool {
    failed.iter().any(|check| {
        let rule = match check {
            FailedCheck::Block(check) => &check.rule,
            FailedCheck::Authorizer(check) => &check.rule,
        };
        rule.starts_with("check if time(")
    })
}

/// Check `token` for `request` against the root keys of `ring`.
///
/// # Errors
///
/// The [`Refusal`] that stops the request.
///
/// # Panics
///
/// Never: [`POLICY`] is a constant that parses (a test proves it).
pub fn authorize(token: &Token, ring: &KeyRing, request: &Request<'_>) -> Result<(), Refusal> {
    if token.as_bytes().len() > MAX_TOKEN_BYTES {
        return Err(Refusal::InvalidToken);
    }
    let biscuit =
        Biscuit::from(token.as_bytes(), |id| ring.choose(id)).map_err(|_| Refusal::InvalidToken)?;
    if biscuit.block_count() > MAX_BLOCKS {
        return Err(Refusal::InvalidToken);
    }
    let mut builder = AuthorizerBuilder::new();
    for fact in request_facts(request) {
        builder = builder.fact(fact).map_err(|_| Refusal::Forbidden)?;
    }
    let mut authorizer = builder
        .code(POLICY)
        .expect("the policy parses")
        .set_limits(limits())
        .build(&biscuit)
        .map_err(|_| Refusal::InvalidToken)?;
    match authorizer.authorize() {
        Ok(_) => Ok(()),
        Err(TokenError::FailedLogic(
            Logic::Unauthorized { checks, .. } | Logic::NoMatchingPolicy { checks },
        )) if expired(&checks) => Err(Refusal::Expired),
        Err(_) => Err(Refusal::Forbidden),
    }
}
