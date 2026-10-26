//! The commit `parosctl bench` reports (#194 (throughput benchmark)):
//! `PAROS_COMMIT` from the environment, else `git rev-parse HEAD`, else
//! nothing (an image built without `.git`).

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=PAROS_COMMIT");
    let commit = std::env::var("PAROS_COMMIT").ok().or_else(|| {
        let out = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    });
    if let Some(commit) = commit.filter(|c| !c.is_empty()) {
        println!("cargo:rustc-env=PAROS_COMMIT={commit}");
    }
    // A new commit moves `HEAD` or the branch it names.
    for path in ["../../../.git/HEAD", "../../../.git/refs/heads"] {
        if std::path::Path::new(path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}
