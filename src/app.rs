use crate::config::Config;
use crate::dns::fetch_seed_peers_from_dns;
use crate::grpc_api::run_grpc_server;
use crate::network::{NetworkResources, accept_loop, dial_loop};
use crate::pending::PendingRequests;
use crate::state::{DedupWindow, NodeState};
use crate::types::ApiState;
use crate::verified::TrustedNetworkState;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};

pub(crate) async fn run() -> std::io::Result<()> {
    let mut config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => err.exit(),
    };
    let configured_seed_peers = config.seed_peers.clone();

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
        config.max_known_peers,
        &configured_seed_peers,
    )));
    let dedup = Arc::new(DedupWindow::new(config.max_seen));
    let pending_requests = Arc::new(PendingRequests::default());
    {
        let mut locked = state.lock().await;
        locked.set_reconnect_interval(config.reconnect_interval);
        for peer in &config.seed_peers {
            if !configured_seed_peers.contains(peer) {
                let _ = locked.add_dns_peer(*peer);
            }
        }
    }
    let latest_epoch_tick = Arc::new(AtomicU64::new(0));
    let trusted_network = Arc::new(TrustedNetworkState::default());
    let outbound_budget = Arc::new(Semaphore::new(crate::network::GLOBAL_OUTBOUND_QUEUE_BYTES));
    let listener = TcpListener::bind(config.listen_addr).await?;

    println!(
        "Qubic light relay started at {} | peer_port={} | target_outbound={} | max_incoming={} | max_known_peers={} | peer_write_timeout_ms={} | seed_peers={} | traffic_log={} | grpc={}({}) | critical_threshold={} | emergency_dns={}",
        config.listen_addr,
        config.peer_port,
        config.target_outbound,
        config.max_incoming,
        config.max_known_peers,
        config.peer_write_timeout.as_millis(),
        config.seed_peers.len(),
        config.traffic_log,
        config.grpc_enabled,
        config.grpc_listen_addr,
        config.critical_peer_threshold,
        config.emergency_dns_bootstrap,
    );
    if config.seed_peers.is_empty() {
        println!("No seed peers configured. Use --peer <ip[:port]> to join the public network.");
    }

    let shared_config = Arc::new(config);

    let grpc_state = if shared_config.grpc_enabled {
        let grpc_state = ApiState {
            node_state: Arc::clone(&state),
            dedup: Arc::clone(&dedup),
            pending_requests: Arc::clone(&pending_requests),
            trusted_network: Arc::clone(&trusted_network),
            outbound_budget: Arc::clone(&outbound_budget),
            config: Arc::clone(&shared_config),
        };
        Some(grpc_state)
    } else {
        None
    };

    let network_resources = NetworkResources::new(
        Arc::clone(&state),
        Arc::clone(&dedup),
        Arc::clone(&pending_requests),
        Arc::clone(&latest_epoch_tick),
        Arc::clone(&trusted_network),
        Arc::clone(&outbound_budget),
        Arc::clone(&shared_config),
    );
    let accept_resources = network_resources.clone();
    tokio::spawn(async move {
        accept_loop(listener, accept_resources).await;
    });

    tokio::spawn(async move {
        dial_loop(network_resources).await;
    });

    if let Some(grpc_state) = grpc_state {
        tokio::select! {
            result = run_grpc_server(grpc_state) => return result,
            result = tokio::signal::ctrl_c() => result?,
        }
    } else {
        tokio::signal::ctrl_c().await?;
    }
    println!("Shutdown signal received, stopping.");
    Ok(())
}
