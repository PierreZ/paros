//! Compiles the Paros wire messages with prost. The transport is
//! moonpool-rpc, whose bodies are any `prost::Message`: only the messages are
//! generated here — the `service` blocks in the protos document the method
//! set, and each method's identity lives in `src/rpc/methods.rs`.
//!
//! The protos are parsed by `protox`, a protobuf compiler in pure Rust, so a
//! build (and `cargo install parosd`) needs no `protoc` on the machine.
//!
//! The build also writes `wire_messages.rs`: one `message!(path)` line for
//! each message of the crate, which the property test of the codecs runs
//! over (`src/tests/property.rs`). A test build derives `arbitrary::Arbitrary`
//! on each generated type, so the test draws its values from bytes.

use std::fmt::Write as _;

/// The Rust module of each proto package (see `src/lib.rs`).
const MODULES: &[(&str, &str)] = &[
    ("paros.common.v1", "common"),
    ("paros.v1", "public"),
    ("paros.internal.v1", "internal"),
    ("paros.matchmaker.v1", "matchmaker"),
    ("paros.system.v1", "system"),
    ("paros.fleet.v1", "fleet"),
    ("paros.machine.v1", "machine"),
    ("paros.checkpoint.v1", "checkpoint"),
    ("paros.election.v1", "election"),
    ("paros.tenant.v1", "tenant"),
    ("paros.view.v1", "view"),
];

/// The derive a test build puts on each generated message and enum.
const TEST_DERIVE: &str = "#[cfg_attr(test, derive(arbitrary::Arbitrary))]";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The types both contracts speak, compiled on their own so they land in
    // exactly one Rust module.
    let common = protox::compile(["proto/common.proto"], ["proto"])?;
    // The contracts, pointed at that module instead of re-generating the
    // shared types once per package.
    let contracts = protox::compile(
        [
            "proto/paros.proto",
            "proto/internal.proto",
            "proto/matchmaker.proto",
            "proto/system.proto",
            "proto/machine.proto",
            "proto/checkpoint.proto",
            "proto/election.proto",
            "proto/fleet.proto",
            "proto/tenant.proto",
            "proto/view.proto",
        ],
        ["proto"],
    )?;
    let mut list = String::from("{\n");
    for file in common.file.iter().chain(&contracts.file) {
        let package = file.package();
        let Some((_, module)) = MODULES.iter().find(|(p, _)| *p == package) else {
            continue;
        };
        for message in &file.message_type {
            assert!(
                message.nested_type.is_empty(),
                "a nested message needs its module path here"
            );
            writeln!(list, "message!(crate::{module}::{});", message.name())?;
        }
    }
    list.push_str("}\n");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(out.join("wire_messages.rs"), list)?;
    prost_build::Config::new()
        .type_attribute(".", TEST_DERIVE)
        .compile_fds(common)?;
    prost_build::Config::new()
        .type_attribute(".", TEST_DERIVE)
        .extern_path(".paros.common.v1", "crate::common")
        .compile_fds(contracts)?;
    Ok(())
}
