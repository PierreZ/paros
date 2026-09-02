//! Compiles the Paros gRPC contract with tonic/prost.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        // Connections are supplied by moonpool-hyper over provider networking.
        .build_transport(false)
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
