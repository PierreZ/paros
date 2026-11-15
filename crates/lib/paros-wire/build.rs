//! Compiles the Paros wire messages with prost. The transport is
//! moonpool-rpc, whose bodies are any `prost::Message`: only the messages are
//! generated here — the `service` blocks in the protos document the method
//! set, and each method's identity lives in `src/rpc/methods.rs`.
//!
//! The protos are parsed by `protox`, a protobuf compiler in pure Rust, so a
//! build (and `cargo install parosd`) needs no `protoc` on the machine.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The types both contracts speak, compiled on their own so they land in
    // exactly one Rust module.
    prost_build::Config::new()
        .compile_fds(protox::compile(["proto/common.proto"], ["proto"])?)?;
    // The contracts, pointed at that module instead of re-generating the
    // shared types once per package.
    prost_build::Config::new()
        .extern_path(".paros.common.v1", "crate::common")
        .compile_fds(protox::compile(
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
        )?)?;
    Ok(())
}
