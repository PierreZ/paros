//! A root public key on the command line (#245 (Biscuit tokens)), shared by
//! `parosd`, `paros-frontend` and `parosctl`: the key's line,
//! `<label>:<key_id>:ed25519/<hex>` (`parosctl key bootstrap` prints it), or
//! the path of a `.public` (or `.private`) key file.

use paros_authz_biscuit::RootPublicKey;

/// `arg` as a root public key: a key line when it holds `:ed25519/`, else a
/// key file's path. A clap `value_parser`.
///
/// # Errors
///
/// A bad key line, or a file that cannot be read or holds no key.
pub fn parse(arg: &str) -> Result<RootPublicKey, String> {
    if arg.contains(":ed25519/") {
        return RootPublicKey::from_line(arg).map_err(|error| error.to_string());
    }
    let text = std::fs::read_to_string(arg).map_err(|error| {
        if arg.contains(':') {
            format!(
                "{arg}: neither a key line <label>:<key_id>:ed25519/<hex> nor a key file: {error}"
            )
        } else {
            format!("{arg}: {error}")
        }
    })?;
    RootPublicKey::from_file(&text).map_err(|error| format!("{arg}: {error}"))
}
