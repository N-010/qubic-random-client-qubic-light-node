use crate::LIGHTNODE_FILE_DESCRIPTOR_SET;
use crate::codec::{parse_wallet_public_key, tx_id_from_bytes};
use crate::lightnodepb;
use crate::network::broadcast_transaction_to_network;
use crate::peer_api::{query_balance, query_tick_transactions};
use crate::types::{
    ApiState, BalanceResponse, TickStatus, TickTransaction, tick_status_from_packed,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(crate) struct GrpcService {
    pub(crate) api: ApiState,
}

pub(crate) async fn run_grpc_server(api_state: ApiState) -> std::io::Result<()> {
    let service = GrpcService {
        api: api_state.clone(),
    };
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(LIGHTNODE_FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    println!("gRPC listening on {}", api_state.config.grpc_listen_addr);

    Server::builder()
        .add_service(reflection)
        .add_service(lightnodepb::light_node_server::LightNodeServer::new(
            service,
        ))
        .serve(api_state.config.grpc_listen_addr)
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))
}

#[tonic::async_trait]
impl lightnodepb::light_node_server::LightNode for GrpcService {
    async fn get_status(
        &self,
        _request: Request<lightnodepb::GetStatusRequest>,
    ) -> Result<Response<lightnodepb::GetStatusResponse>, Status> {
        let packed = self.api.latest_epoch_tick.load(Ordering::Relaxed);
        let cached = {
            let cached_from_state = self.api.node_state.lock().await.latest_tick();
            if cached_from_state.is_some() {
                cached_from_state
            } else {
                tick_status_from_packed(packed)
            }
        };
        if let Some(status) = cached {
            Ok(Response::new(lightnodepb::GetStatusResponse {
                ok: true,
                source: "cache".to_string(),
                status: Some(map_tick_status(status)),
                warning: String::new(),
                error: String::new(),
            }))
        } else {
            Ok(Response::new(lightnodepb::GetStatusResponse {
                ok: false,
                source: String::new(),
                status: None,
                warning: String::new(),
                error: "No tick data in local cache yet. Wait for incoming network messages."
                    .to_string(),
            }))
        }
    }

    async fn get_balance(
        &self,
        request: Request<lightnodepb::GetBalanceRequest>,
    ) -> Result<Response<lightnodepb::GetBalanceResponse>, Status> {
        let wallet = request.into_inner().wallet;
        let public_key = parse_wallet_public_key(&wallet).map_err(Status::invalid_argument)?;

        match query_balance(
            Arc::clone(&self.api.node_state),
            Arc::clone(&self.api.config),
            &wallet,
            public_key,
        )
        .await
        {
            Ok(balance) => Ok(Response::new(lightnodepb::GetBalanceResponse {
                ok: true,
                balance: Some(map_balance(balance)),
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(lightnodepb::GetBalanceResponse {
                ok: false,
                balance: None,
                error: err,
            })),
        }
    }

    async fn get_tick_transactions(
        &self,
        request: Request<lightnodepb::GetTickTransactionsRequest>,
    ) -> Result<Response<lightnodepb::GetTickTransactionsResponse>, Status> {
        let tick = request.into_inner().tick;
        match query_tick_transactions(
            Arc::clone(&self.api.node_state),
            Arc::clone(&self.api.config),
            tick,
        )
        .await
        {
            Ok(transactions) => Ok(Response::new(lightnodepb::GetTickTransactionsResponse {
                ok: true,
                tick,
                transactions: transactions
                    .into_iter()
                    .map(map_transaction)
                    .collect::<Vec<_>>(),
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(lightnodepb::GetTickTransactionsResponse {
                ok: false,
                tick,
                transactions: Vec::new(),
                error: err,
            })),
        }
    }

    async fn broadcast_transaction(
        &self,
        request: Request<lightnodepb::BroadcastTransactionRequest>,
    ) -> Result<Response<lightnodepb::BroadcastTransactionResponse>, Status> {
        let tx_bytes = request.into_inner().tx_bytes;
        if tx_bytes.is_empty() {
            return Ok(Response::new(lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: "Transaction payload is empty".to_string(),
            }));
        }

        let tx_id = tx_id_from_bytes(&tx_bytes);
        match broadcast_transaction_to_network(Arc::clone(&self.api.node_state), &tx_bytes).await {
            Ok(_) => Ok(Response::new(lightnodepb::BroadcastTransactionResponse {
                ok: true,
                tx_id,
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: err,
            })),
        }
    }
}

fn map_tick_status(status: TickStatus) -> lightnodepb::TickStatus {
    lightnodepb::TickStatus {
        epoch: status.epoch as u32,
        tick: status.tick,
        initial_tick: status.initial_tick,
        tick_duration_ms: status.tick_duration_ms as u32,
        aligned_votes: status.aligned_votes as u32,
        misaligned_votes: status.misaligned_votes as u32,
    }
}

fn map_balance(balance: BalanceResponse) -> lightnodepb::Balance {
    lightnodepb::Balance {
        wallet: balance.wallet,
        public_key_hex: balance.public_key_hex,
        tick: balance.tick,
        spectrum_index: balance.spectrum_index,
        incoming_amount: balance.incoming_amount,
        outgoing_amount: balance.outgoing_amount,
        balance: balance.balance,
        number_of_incoming_transfers: balance.number_of_incoming_transfers,
        number_of_outgoing_transfers: balance.number_of_outgoing_transfers,
        latest_incoming_transfer_tick: balance.latest_incoming_transfer_tick,
        latest_outgoing_transfer_tick: balance.latest_outgoing_transfer_tick,
    }
}

fn map_transaction(tx: TickTransaction) -> lightnodepb::Transaction {
    lightnodepb::Transaction {
        source_public_key_hex: tx.source_public_key_hex,
        destination_public_key_hex: tx.destination_public_key_hex,
        amount: tx.amount,
        tick: tx.tick,
        input_type: tx.input_type as u32,
        input_size: tx.input_size as u32,
        input_hex: tx.input_hex,
        signature_hex: tx.signature_hex,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use crate::frame::{BROADCAST_TRANSACTION_TYPE, build_request_frame};
    use crate::lightnodepb::light_node_server::LightNode;
    use crate::state::NodeState;
    use pretty_assertions::assert_eq;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;

    fn test_config() -> Arc<Config> {
        Arc::new(Config {
            listen_addr: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), DEFAULT_PORT),
            api_timeout: Duration::from_secs(1),
            grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT)),
            grpc_enabled: true,
            peer_port: DEFAULT_PORT,
            target_outbound: 8,
            max_incoming: 32,
            max_seen: 1_000,
            max_known_peers: 1_000,
            reconnect_interval: Duration::from_millis(2_000),
            relay_all: false,
            dns_bootstrap: false,
            dns_lite_peers: 0,
            dns_timeout: Duration::from_secs(1),
            traffic_log: false,
            seed_peers: Vec::new(),
        })
    }

    fn test_service(node_state: Arc<Mutex<NodeState>>) -> GrpcService {
        GrpcService {
            api: ApiState {
                node_state,
                latest_epoch_tick: Arc::new(AtomicU64::new(0)),
                config: test_config(),
            },
        }
    }

    #[tokio::test]
    async fn broadcast_transaction_success() {
        let node_state = Arc::new(Mutex::new(NodeState::new(1_000, 1_000, &[])));
        let (peer_tx, mut peer_rx) = mpsc::channel::<Arc<[u8]>>(1);
        {
            let mut locked = node_state.lock().await;
            let peer = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), DEFAULT_PORT);
            let registered = locked.register_session(peer, true, peer_tx, DEFAULT_PORT);
            assert_eq!(registered.is_some(), true);
        }

        let service = test_service(Arc::clone(&node_state));
        let request = Request::new(lightnodepb::BroadcastTransactionRequest {
            tx_bytes: vec![1, 2, 3],
        });

        let response = LightNode::broadcast_transaction(&service, request)
            .await
            .expect("broadcast_transaction should return grpc response")
            .into_inner();

        assert_eq!(
            response,
            lightnodepb::BroadcastTransactionResponse {
                ok: true,
                tx_id: tx_id_from_bytes(&[1, 2, 3]),
                error: String::new(),
            }
        );

        let outbound_frame = peer_rx
            .recv()
            .await
            .expect("peer should receive broadcast frame");
        let expected_frame = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, &[1, 2, 3])
            .expect("broadcast frame should be buildable");
        assert_eq!(&*outbound_frame, expected_frame.as_slice());
    }

    #[tokio::test]
    async fn broadcast_transaction_empty_payload() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, 1_000, &[]))));
        let request = Request::new(lightnodepb::BroadcastTransactionRequest {
            tx_bytes: Vec::new(),
        });

        let response = LightNode::broadcast_transaction(&service, request)
            .await
            .expect("broadcast_transaction should return grpc response")
            .into_inner();

        assert_eq!(
            response,
            lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: "Transaction payload is empty".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn broadcast_transaction_no_peers_maps_to_error_response() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, 1_000, &[]))));
        let request = Request::new(lightnodepb::BroadcastTransactionRequest {
            tx_bytes: vec![7, 8, 9],
        });

        let response = LightNode::broadcast_transaction(&service, request)
            .await
            .expect("broadcast_transaction should return grpc response")
            .into_inner();

        assert_eq!(
            response,
            lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: "No connected peers available for broadcast".to_string(),
            }
        );
    }
}
