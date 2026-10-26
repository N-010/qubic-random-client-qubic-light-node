use crate::frame::is_bogon;
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

const MAX_BOOTSTRAP_RESPONSE_BYTES: usize = 1024 * 1024;

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

    let root = read_bootstrap_json(response, MAX_BOOTSTRAP_RESPONSE_BYTES).await?;

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

async fn read_bootstrap_json(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Value, String> {
    const TOO_LARGE: &str = "bootstrap response exceeds byte limit";
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(TOO_LARGE.to_string());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| format!("response read failed: {err}"))?
    {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(TOO_LARGE.to_string());
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|err| format!("invalid JSON: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn bootstrap_body_limit_covers_fixed_and_chunked_responses() {
        for chunked in [false, true] {
            for length in [32, 33] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(stream.read_u8().await.unwrap());
                    }
                    let mut body = br#"{"litePeers":[]}"#.to_vec();
                    body.resize(length, b' ');
                    let response = if chunked {
                        format!(
                            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                            body.len(),
                            String::from_utf8(body).unwrap()
                        )
                    } else {
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            String::from_utf8(body).unwrap()
                        )
                    };
                    let _ = stream.write_all(response.as_bytes()).await;
                });
                let response = reqwest::Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_secs(2))
                    .build()
                    .unwrap()
                    .get(format!("http://{address}"))
                    .send()
                    .await
                    .unwrap();
                let result = read_bootstrap_json(response, 32).await;
                server.await.unwrap();
                if length == 32 {
                    assert_eq!(result.unwrap(), serde_json::json!({"litePeers": []}));
                } else {
                    assert_eq!(result.unwrap_err(), "bootstrap response exceeds byte limit");
                }
            }
        }
    }
}
