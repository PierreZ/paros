//! Compiles the Paros gRPC contract with tonic/prost.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The types both contracts speak, compiled on their own so they land in
    // exactly one Rust module.
    tonic_prost_build::configure()
        // Connections are supplied by moonpool-hyper over provider networking.
        .build_transport(false)
        .compile_protos(&["proto/common.proto"], &["proto"])?;
    // The two contracts, pointed at that module instead of re-generating the
    // shared types once per package.
    tonic_prost_build::configure()
        .build_transport(false)
        .extern_path(".paros.common.v1", "crate::grpc::common")
        .compile_protos(
            &[
                "proto/paros.proto",
                "proto/internal.proto",
                "proto/matchmaker.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
