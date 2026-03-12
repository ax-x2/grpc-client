fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from("src/proto");
    std::fs::create_dir_all(&out_dir)?;

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(&out_dir)
        .compile_protos(
            &["proto/geyser.proto", "proto/solana-storage.proto"],
            &["proto/"],
        )?;

    println!("cargo:rerun-if-changed=proto/geyser.proto");
    println!("cargo:rerun-if-changed=proto/solana-storage.proto");

    Ok(())
}
