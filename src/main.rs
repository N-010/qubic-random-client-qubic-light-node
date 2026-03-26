mod app;
mod codec;
mod config;
mod dns;
mod frame;
mod grpc_api;
mod network;
mod peer_api;
mod state;
mod types;

pub(crate) mod lightnodepb {
    tonic::include_proto!("lightnode");
}

pub(crate) const LIGHTNODE_FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("lightnode_descriptor");

#[tokio::main]
async fn main() -> std::io::Result<()> {
    app::run().await
}
