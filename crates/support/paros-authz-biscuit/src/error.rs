//! The errors of the offline tools: key files, minting and derivation.

use std::fmt;

/// Why a key or token operation failed. A refused request is a
/// [`crate::Refusal`], not an `Error`.
#[derive(Debug)]
pub enum Error {
    /// A key file or key string does not parse.
    BadKey(String),
    /// A token does not parse, or Biscuit refused to build it.
    BadToken(String),
    /// An argument has no meaning, e.g. an expiry in the past.
    BadArgument(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadKey(why) => write!(f, "bad key: {why}"),
            Self::BadToken(why) => write!(f, "bad token: {why}"),
            Self::BadArgument(why) => write!(f, "bad argument: {why}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<biscuit_auth::error::Token> for Error {
    fn from(error: biscuit_auth::error::Token) -> Self {
        Self::BadToken(error.to_string())
    }
}
