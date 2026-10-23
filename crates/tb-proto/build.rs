fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");

    let files = [
        proto_dir.join("common.proto"),
        proto_dir.join("transport_api.proto"),
        proto_dir.join("core.proto"),
        proto_dir.join("rule_engine.proto"),
        proto_dir.join("edqs.proto"),
    ];

    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }

    prost_build::Config::new().compile_protos(&files, &[proto_dir])?;
    Ok(())
}
