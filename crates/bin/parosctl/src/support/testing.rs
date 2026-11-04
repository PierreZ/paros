//! What the unit tests share: the global options from a command line and
//! an admin token.

use std::time::{Duration, SystemTime};

use clap::Parser;
use paros_authz_biscuit::{Entropy, Grant, Role, RootKey};

use crate::support::connect::Global;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    global: Global,
}

/// The global options of `parosctl LINE…`.
pub fn global(line: &[&str]) -> Global {
    let mut words = vec!["parosctl"];
    words.extend_from_slice(line);
    Cli::try_parse_from(words).expect("valid").global
}

/// An admin token, valid for an hour, signed by a fixed root key.
pub fn admin_token() -> String {
    let key = RootKey::generate("demo", &Entropy::from_bytes([5; 32]));
    let now = SystemTime::now();
    let grant = Grant {
        role: Role::Admin,
        subject: "ops".into(),
        expires: now + Duration::from_secs(3600),
    };
    paros_authz_biscuit::mint(&key, &grant, now, &Entropy::from_bytes([6; 32]))
        .expect("minted")
        .to_text()
}

/// The address `text` names.
pub fn address(text: &str) -> paros::model::address::Address {
    paros::model::address::Address::parse(text).expect("valid")
}
