use crate::frame::is_bogon;
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

pub(crate) async fn fetch_seed_peers_from_dns(
    peer_port: u16,
    lite_peers: usize,
    timeout: Duration,
) -> Result<Vec<SocketAddrV4>, String> {
    let requested_lite = lite_peers.max(1);
    let url =
        format!("https://api.qubic.global/random-peers?service=bobNode&litePeers={requested_lite}");

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|err| format!("cannot build HTTP client: {err}"))?;

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("request failed: {err}"))?;

    if !response.status().is_success() {
        return Err(format!("HTTP status {}", response.status()));
    }

    let root: Value = response
        .json()
        .await
        .map_err(|err| format!("invalid JSON: {err}"))?;

    let mut peers = Vec::<SocketAddrV4>::new();
    collect_peers_from_json(&root, "litePeers", peer_port, &mut peers);

    peers.sort_unstable();
    peers.dedup();
    Ok(peers)
}

fn collect_peers_from_json(
    root: &Value,
    field_name: &str,
    peer_port: u16,
    out: &mut Vec<SocketAddrV4>,
) {
    let Some(array) = root.get(field_name).and_then(Value::as_array) else {
        return;
    };

    for item in array {
        let Some(ip_raw) = item.as_str() else {
            continue;
        };
        let Ok(ip) = ip_raw.parse::<Ipv4Addr>() else {
            continue;
        };
        if is_bogon(&ip) {
            continue;
        }
        out.push(SocketAddrV4::new(ip, peer_port));
    }
}
