macro_rules! println {
    ($($arg:tt)*) => {
        crate::logging::info(format_args!($($arg)*))
    };
}

macro_rules! eprintln {
    ($($arg:tt)*) => {
        crate::logging::error(format_args!($($arg)*))
    };
}

mod app;
mod codec;
mod config;
mod dns;
mod frame;
mod grpc_api;
mod logging;
mod network;
mod peer_api;
mod pending;
mod state;
mod types;

pub(crate) mod lightnodepb {
    tonic::include_proto!("lightnode");
}

pub(crate) const LIGHTNODE_FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("lightnode_descriptor");

#[tokio::main]
async fn main() -> std::io::Result<()> {
    logging::init();
    app::run().await
}
