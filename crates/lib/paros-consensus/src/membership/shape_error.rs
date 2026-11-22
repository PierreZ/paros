//! Why a membership was refused: the result of the fallible constructors
//! ([`AcceptorConfig::try_new`], [`AcceptorConfig::try_zoned`],
//! [`AcceptorConfig::try_with_counts`], [`MatchmakerSet::try_new`]).
//!
//! A configuration can come from a peer's message or from an operator. Both
//! are inputs that a correct caller can get wrong, so the core answers them
//! with a result and never panics on them (decision 30 of the rework). The
//! panicking constructors stay for the values that a program builds itself.

use core::fmt;

#[cfg(doc)]
use super::{AcceptorConfig, MatchmakerSet};

/// Why a membership cannot be built. The text of each variant is the text
/// of the panic of the matching panicking constructor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ShapeError {
    /// The membership names no member: no quorum can form.
    NoMember,
    /// The quorum system does not admit the membership: a grid that does not
    /// tile it, or quorums that do not cross-intersect.
    QuorumNotAdmitted,
    /// One member is named twice with two failure domains.
    TwoDomains,
    /// A labeled configuration does not label each member: the wire named
    /// more or fewer domains than members.
    UnlabeledMember,
    /// A failure domain label is longer than
    /// [`super::MAX_FAILURE_DOMAIN_BYTES`].
    DomainTooLong,
    /// A proxy or replica count is above [`super::MAX_ROLE_COUNT`].
    RoleCountOverCap,
}

impl ShapeError {
    /// The rule that the refused membership breaks, as one sentence.
    #[must_use]
    pub const fn rule(self) -> &'static str {
        match self {
            Self::NoMember => "a membership names at least one member",
            Self::QuorumNotAdmitted => "an acceptor configuration admits its quorum system",
            Self::TwoDomains => "a member of a configuration has one failure domain",
            Self::UnlabeledMember => "a configuration labels every member or none",
            Self::DomainTooLong => "a configuration's failure domains are short labels",
            Self::RoleCountOverCap => "a configuration's role counts are at most the cap",
        }
    }
}

impl fmt::Display for ShapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.rule())
    }
}

impl core::error::Error for ShapeError {}

#[cfg(test)]
mod tests {
    use super::ShapeError;

    /// Each refusal names a distinct rule, so a log line tells them apart.
    #[test]
    fn each_refusal_names_its_own_rule() {
        let all = [
            ShapeError::NoMember,
            ShapeError::QuorumNotAdmitted,
            ShapeError::TwoDomains,
            ShapeError::UnlabeledMember,
            ShapeError::DomainTooLong,
            ShapeError::RoleCountOverCap,
        ];
        for (i, a) in all.iter().enumerate() {
            assert_eq!(a.to_string(), a.rule());
            assert_ne!(a.rule(), "");
            for b in &all[i + 1..] {
                assert_ne!(a.rule(), b.rule());
            }
        }
    }
}
