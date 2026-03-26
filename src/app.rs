use crate::config::Config;
use crate::dns::fetch_seed_peers_from_dns;
use crate::grpc_api::run_grpc_server;
use crate::network::{accept_loop, dial_loop};
use crate::state::NodeState;
use crate::types::ApiState;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

pub(crate) async fn run() -> std::io::Result<()> {
    let mut config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => err.exit(),
    };

    if config.dns_bootstrap && config.seed_peers.is_empty() {
        let dns_lite_peers = if config.dns_lite_peers == 0 {
            (config.target_outbound * 3).max(8)
        } else {
            config.dns_lite_peers
        };

        match fetch_seed_peers_from_dns(config.peer_port, dns_lite_peers, config.dns_timeout).await
        {
            Ok(mut peers) => {
                if peers.is_empty() {
                    println!("DNS bootstrap returned no peers.");
                } else {
                    peers.sort_unstable();
                    peers.dedup();
                    println!("DNS bootstrap: loaded {} peers.", peers.len());
                    config.seed_peers.extend(peers);
                    config.seed_peers.sort_unstable();
                    config.seed_peers.dedup();
                }
            }
            Err(err) => {
                eprintln!("DNS bootstrap failed: {err}");
            }
        }
    }

    let state = Arc::new(Mutex::new(NodeState::new(
        config.max_seen,
        config.max_known_peers,
        &config.seed_peers,
    )));
    let latest_epoch_tick = Arc::new(AtomicU64::new(0));
    let listener = TcpListener::bind(config.listen_addr).await?;

    println!(
        "Qubic light relay started at {} | peer_port={} | target_outbound={} | max_incoming={} | max_known_peers={} | seed_peers={} | traffic_log={} | grpc={}({})",
        config.listen_addr,
        config.peer_port,
        config.target_outbound,
        config.max_incoming,
        config.max_known_peers,
        config.seed_peers.len(),
        config.traffic_log,
        config.grpc_enabled,
        config.grpc_listen_addr
    );
    if config.seed_peers.is_empty() {
        println!("No seed peers configured. Use --peer <ip[:port]> to join the public network.");
    }

    let shared_config = Arc::new(config);

    if shared_config.grpc_enabled {
        let grpc_state = ApiState {
            node_state: Arc::clone(&state),
            latest_epoch_tick: Arc::clone(&latest_epoch_tick),
            config: Arc::clone(&shared_config),
        };
        tokio::spawn(async move {
            if let Err(err) = run_grpc_server(grpc_state).await {
                eprintln!("gRPC server stopped: {err}");
            }
        });
    }

    let accept_state = Arc::clone(&state);
    let accept_latest_epoch_tick = Arc::clone(&latest_epoch_tick);
    let accept_config = Arc::clone(&shared_config);
    tokio::spawn(async move {
        accept_loop(
            listener,
            accept_state,
            accept_latest_epoch_tick,
            accept_config,
        )
        .await;
    });

    let dial_state = Arc::clone(&state);
    let dial_latest_epoch_tick = Arc::clone(&latest_epoch_tick);
    let dial_config = Arc::clone(&shared_config);
    tokio::spawn(async move {
        dial_loop(dial_state, dial_latest_epoch_tick, dial_config).await;
    });

    tokio::signal::ctrl_c().await?;
    println!("Shutdown signal received, stopping.");
    Ok(())
}
