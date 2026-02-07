fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("lightnode_descriptor.bin");

    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(&["proto/lightnode.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/lightnode.proto");
    Ok(())
}
