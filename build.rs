fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }
    tonic_build::configure()
        .build_server(true)
        .compile_protos(&["proto/lightnode.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/lightnode.proto");
    Ok(())
}
