use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use std::env;
use std::ffi::OsString;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

pub(crate) const DEFAULT_PORT: u16 = 21841;
#[cfg(test)]
pub(crate) const DEFAULT_GRPC_PORT: u16 = 50051;

const DEFAULT_API_TIMEOUT_MS: u64 = 6_000;
const DEFAULT_TARGET_OUTBOUND: usize = 8;
const DEFAULT_MAX_KNOWN_PEERS: usize = 500;
const DEFAULT_RECONNECT_MS: u64 = 2_000;
const DEFAULT_PEER_WRITE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PEER_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PEER_HANDSHAKE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PEER_FRAME_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
const DEFAULT_DNS_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_EMERGENCY_DNS_BACKOFF_INITIAL_MS: u64 = 10_000; // 10с
const DEFAULT_EMERGENCY_DNS_BACKOFF_MAX_MS: u64 = 300_000; // 5мин
const MIN_API_TIMEOUT_MS: u64 = 1_000;
const MIN_RECONNECT_MS: u64 = 200;
const MIN_DNS_TIMEOUT_MS: u64 = 500;
const MIN_PEER_CONNECT_TIMEOUT_MS: u64 = 500;
const MIN_PEER_HANDSHAKE_TIMEOUT_MS: u64 = 500;
const MIN_PEER_FRAME_TIMEOUT_MS: u64 = 1_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Config {
    pub(crate) api_timeout: Duration,
    pub(crate) grpc_listen_addr: SocketAddr,
    pub(crate) peer_port: u16,
    pub(crate) target_outbound: usize,
    pub(crate) max_known_peers: usize,
    pub(crate) reconnect_interval: Duration,
    pub(crate) peer_write_timeout: Duration,
    pub(crate) peer_connect_timeout: Duration,
    pub(crate) peer_handshake_timeout: Duration,
    pub(crate) peer_frame_timeout: Duration,
    pub(crate) max_frame_bytes: usize,
    pub(crate) dns_bootstrap: bool,
    pub(crate) dns_lite_peers: usize,
    pub(crate) dns_timeout: Duration,
    pub(crate) traffic_log: bool,
    pub(crate) seed_peers: Vec<SocketAddrV4>,
    pub(crate) critical_peer_threshold: usize,
    pub(crate) emergency_dns_bootstrap: bool,
    pub(crate) emergency_dns_backoff_initial_ms: u64,
    pub(crate) emergency_dns_backoff_max_ms: u64,
}

#[derive(Debug, Parser)]
#[command(name = "QubicLightNode", version, about = "Qubic RandomClient backend")]
struct Cli {
    #[arg(
        long = "peer",
        value_name = "IP[:PORT]",
        help = "Seed peer; can be repeated, and plain IP uses --peer-port",
        help_heading = "P2P"
    )]
    seed_peer_args: Vec<String>,

    #[arg(
        long = "peer-port",
        value_name = "PORT",
        default_value_t = DEFAULT_PORT,
        help = "Default remote port for discovery and --peer values without a port",
        help_heading = "P2P"
    )]
    peer_port: u16,

    #[arg(
        long = "target-outbound",
        value_name = "N",
        default_value_t = DEFAULT_TARGET_OUTBOUND,
        help = "Desired number of outbound peer connections to keep",
        help_heading = "P2P"
    )]
    target_outbound: usize,

    #[arg(
        long = "max-known-peers",
        value_name = "N",
        default_value_t = DEFAULT_MAX_KNOWN_PEERS,
        help = "Maximum number of discovered peers kept in memory",
        help_heading = "P2P"
    )]
    max_known_peers: usize,

    #[arg(
        long = "reconnect-ms",
        value_name = "MS",
        default_value_t = DEFAULT_RECONNECT_MS,
        help = "Delay between outbound reconnect attempts in milliseconds",
        help_heading = "P2P"
    )]
    reconnect_ms: u64,

    #[arg(
        long = "peer-write-timeout-ms",
        value_name = "MS",
        default_value_t = DEFAULT_PEER_WRITE_TIMEOUT_MS,
        help = "Disconnect a peer when a TCP frame write exceeds this duration",
        help_heading = "P2P"
    )]
    peer_write_timeout_ms: u64,

    #[arg(
        long = "peer-connect-timeout-ms",
        value_name = "MS",
        default_value_t = DEFAULT_PEER_CONNECT_TIMEOUT_MS,
        help = "Abort an outbound TCP connect attempt after this duration",
        help_heading = "P2P"
    )]
    peer_connect_timeout_ms: u64,

    #[arg(
        long = "peer-handshake-timeout-ms",
        value_name = "MS",
        default_value_t = DEFAULT_PEER_HANDSHAKE_TIMEOUT_MS,
        help = "Require a valid Qubic peer exchange within this duration",
        help_heading = "P2P"
    )]
    peer_handshake_timeout_ms: u64,

    #[arg(
        long = "peer-frame-timeout-ms",
        value_name = "MS",
        default_value_t = DEFAULT_PEER_FRAME_TIMEOUT_MS,
        help = "Disconnect a peer that does not finish an announced frame",
        help_heading = "P2P"
    )]
    peer_frame_timeout_ms: u64,

    #[arg(
        long = "max-frame-bytes",
        value_name = "BYTES",
        default_value_t = DEFAULT_MAX_FRAME_BYTES,
        help = "Maximum accepted Qubic frame size (minimum 65551; maximum 16777215 bytes)",
        help_heading = "P2P"
    )]
    max_frame_bytes: usize,

    #[arg(
        long = "traffic-log",
        help = "Log RX and TX activity for network frames",
        help_heading = "Diagnostics"
    )]
    traffic_log: bool,

    #[arg(
        long = "no-dns-bootstrap",
        help = "Disable bootstrap peer fetches from api.qubic.global",
        help_heading = "Bootstrap"
    )]
    no_dns_bootstrap: bool,

    #[arg(
        long = "dns-lite-peers",
        value_name = "N",
        default_value_t = 0,
        help = "Requested lite peer count from DNS bootstrap; 0 keeps auto mode",
        help_heading = "Bootstrap"
    )]
    dns_lite_peers: usize,

    #[arg(
        long = "dns-timeout-ms",
        value_name = "MS",
        default_value_t = DEFAULT_DNS_TIMEOUT_MS,
        help = "Timeout for DNS bootstrap requests in milliseconds",
        help_heading = "Bootstrap"
    )]
    dns_timeout_ms: u64,

    #[arg(
        long = "critical-peer-threshold",
        value_name = "N",
        default_value_t = 0,
        help = "Trigger emergency DNS when outbound peers drop below this; 0 uses half of target-outbound (minimum 1)",
        help_heading = "Bootstrap"
    )]
    critical_peer_threshold: usize,

    #[arg(
        long = "no-emergency-dns",
        help = "Disable emergency DNS bootstrap when outbound peer count is critically low",
        help_heading = "Bootstrap"
    )]
    no_emergency_dns: bool,

    #[arg(
        long = "emergency-dns-backoff-initial-ms",
        value_name = "MS",
        default_value_t = DEFAULT_EMERGENCY_DNS_BACKOFF_INITIAL_MS,
        help = "Initial backoff for emergency DNS retries in milliseconds",
        help_heading = "Bootstrap"
    )]
    emergency_dns_backoff_initial_ms: u64,

    #[arg(
        long = "emergency-dns-backoff-max-ms",
        value_name = "MS",
        default_value_t = DEFAULT_EMERGENCY_DNS_BACKOFF_MAX_MS,
        help = "Maximum backoff for emergency DNS retries in milliseconds",
        help_heading = "Bootstrap"
    )]
    emergency_dns_backoff_max_ms: u64,

    #[arg(
        long = "api-timeout-ms",
        value_name = "MS",
        default_value_t = DEFAULT_API_TIMEOUT_MS,
        help = "End-to-end deadline for peer-backed API queries in milliseconds",
        help_heading = "API"
    )]
    api_timeout_ms: u64,

    #[arg(
        long = "grpc-listen",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:50051",
        help = "Bind address for the gRPC API server",
        help_heading = "API"
    )]
    grpc_listen: SocketAddr,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self, clap::Error> {
        Self::from_args(env::args_os())
    }

    fn from_args<I, S>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString> + Clone,
    {
        let cli = Cli::try_parse_from(args)?;

        let mut seed_peers = Vec::with_capacity(cli.seed_peer_args.len());
        for value in cli.seed_peer_args {
            seed_peers.push(
                parse_peer_arg(&value, cli.peer_port)
                    .map_err(|err| Cli::command().error(ErrorKind::ValueValidation, err))?,
            );
        }

        seed_peers.sort_unstable();
        seed_peers.dedup();

        if seed_peers.len() > cli.max_known_peers {
            return Err(Cli::command().error(
                ErrorKind::ValueValidation,
                format!(
                    "--max-known-peers ({}) must be at least the number of unique --peer values ({})",
                    cli.max_known_peers,
                    seed_peers.len()
                ),
            ));
        }
        if cli.target_outbound > cli.max_known_peers {
            return Err(Cli::command().error(
                ErrorKind::ValueValidation,
                format!(
                    "--target-outbound ({}) must not exceed --max-known-peers ({})",
                    cli.target_outbound, cli.max_known_peers
                ),
            ));
        }
        let critical_peer_threshold = if cli.critical_peer_threshold == 0 && cli.target_outbound > 0
        {
            (cli.target_outbound / 2).max(1)
        } else {
            cli.critical_peer_threshold
        };
        if critical_peer_threshold > cli.target_outbound {
            return Err(Cli::command().error(
                ErrorKind::ValueValidation,
                format!(
                    "--critical-peer-threshold ({critical_peer_threshold}) must not exceed --target-outbound ({})",
                    cli.target_outbound
                ),
            ));
        }
        if !(crate::frame::MIN_OPERATIONAL_FRAME_BYTES..=crate::frame::MAX_FRAME_SIZE)
            .contains(&cli.max_frame_bytes)
        {
            return Err(Cli::command().error(
                ErrorKind::ValueValidation,
                format!(
                    "--max-frame-bytes must be between {} and {}",
                    crate::frame::MIN_OPERATIONAL_FRAME_BYTES,
                    crate::frame::MAX_FRAME_SIZE
                ),
            ));
        }

        let emergency_dns_backoff_initial_ms =
            cli.emergency_dns_backoff_initial_ms.max(MIN_DNS_TIMEOUT_MS);
        let emergency_dns_backoff_max_ms = cli
            .emergency_dns_backoff_max_ms
            .max(emergency_dns_backoff_initial_ms);

        Ok(Config {
            api_timeout: Duration::from_millis(cli.api_timeout_ms.max(MIN_API_TIMEOUT_MS)),
            grpc_listen_addr: cli.grpc_listen,
            peer_port: cli.peer_port,
            target_outbound: cli.target_outbound,
            max_known_peers: cli.max_known_peers,
            reconnect_interval: Duration::from_millis(cli.reconnect_ms.max(MIN_RECONNECT_MS)),
            peer_write_timeout: Duration::from_millis(cli.peer_write_timeout_ms),
            peer_connect_timeout: Duration::from_millis(
                cli.peer_connect_timeout_ms.max(MIN_PEER_CONNECT_TIMEOUT_MS),
            ),
            peer_handshake_timeout: Duration::from_millis(
                cli.peer_handshake_timeout_ms
                    .max(MIN_PEER_HANDSHAKE_TIMEOUT_MS),
            ),
            peer_frame_timeout: Duration::from_millis(
                cli.peer_frame_timeout_ms.max(MIN_PEER_FRAME_TIMEOUT_MS),
            ),
            max_frame_bytes: cli.max_frame_bytes,
            dns_bootstrap: !cli.no_dns_bootstrap,
            dns_lite_peers: cli.dns_lite_peers,
            dns_timeout: Duration::from_millis(cli.dns_timeout_ms.max(MIN_DNS_TIMEOUT_MS)),
            traffic_log: cli.traffic_log,
            seed_peers,
            critical_peer_threshold,
            emergency_dns_bootstrap: !cli.no_emergency_dns,
            emergency_dns_backoff_initial_ms,
            emergency_dns_backoff_max_ms,
        })
    }
}

fn parse_peer_arg(value: &str, default_port: u16) -> Result<SocketAddrV4, String> {
    if let Ok(addr) = value.parse::<SocketAddrV4>() {
        return Ok(addr);
    }
    if let Ok(ip) = value.parse::<Ipv4Addr>() {
        return Ok(SocketAddrV4::new(ip, default_port));
    }
    Err(format!(
        "Invalid peer value: {value}. Expected ip or ip:port"
    ))
}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use clap::error::ErrorKind;
    use pretty_assertions::assert_eq;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;

    fn parse_config(args: &[&str]) -> Config {
        Config::from_args(std::iter::once("QubicLightNode").chain(args.iter().copied()))
            .expect("config should parse")
    }

    #[test]
    fn help_flag_returns_help_error() {
        let err =
            Config::from_args(["QubicLightNode", "--help"]).expect_err("help should not parse");
        assert_eq!(err.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn help_output_includes_argument_descriptions() {
        let err =
            Config::from_args(["QubicLightNode", "--help"]).expect_err("help should not parse");
        let help = err.to_string();

        assert!(help.contains("Seed peer; can be repeated, and plain IP uses --peer-port"));
        assert!(help.contains("Desired number of outbound peer connections to keep"));
        assert!(help.contains("Bind address for the gRPC API server"));
        assert!(!help.contains("\n      --port "));
        for removed_option in [
            "--listen-ip",
            "--max-incoming",
            "--max-seen",
            "--relay-all",
            "--no-grpc",
        ] {
            assert!(!help.contains(removed_option));
        }
    }

    #[test]
    fn uses_defaults_without_args() {
        assert_eq!(
            parse_config(&[]),
            Config {
                api_timeout: Duration::from_millis(6_000),
                grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT)),
                peer_port: DEFAULT_PORT,
                target_outbound: 8,
                max_known_peers: 500,
                reconnect_interval: Duration::from_millis(2_000),
                peer_write_timeout: Duration::from_millis(5_000),
                peer_connect_timeout: Duration::from_millis(5_000),
                peer_handshake_timeout: Duration::from_millis(5_000),
                peer_frame_timeout: Duration::from_millis(30_000),
                max_frame_bytes: 1024 * 1024,
                dns_bootstrap: true,
                dns_lite_peers: 0,
                dns_timeout: Duration::from_millis(5_000),
                traffic_log: false,
                seed_peers: Vec::new(),
                critical_peer_threshold: 4,
                emergency_dns_bootstrap: true,
                emergency_dns_backoff_initial_ms: 10_000,
                emergency_dns_backoff_max_ms: 300_000,
            }
        );
    }

    #[test]
    fn peer_without_port_uses_peer_port_and_deduplicates() {
        assert_eq!(
            parse_config(&[
                "--peer-port",
                "30000",
                "--peer",
                "1.2.3.4",
                "--peer",
                "1.2.3.4:30000",
            ])
            .seed_peers,
            vec![SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 30000)]
        );
    }

    #[test]
    fn clamps_timeout_values_to_safety_floors() {
        let config = parse_config(&[
            "--api-timeout-ms",
            "1",
            "--reconnect-ms",
            "2",
            "--dns-timeout-ms",
            "3",
            "--peer-connect-timeout-ms",
            "4",
            "--peer-handshake-timeout-ms",
            "5",
            "--peer-frame-timeout-ms",
            "6",
        ]);

        assert_eq!(config.api_timeout, Duration::from_millis(1_000));
        assert_eq!(config.reconnect_interval, Duration::from_millis(200));
        assert_eq!(config.dns_timeout, Duration::from_millis(500));
        assert_eq!(config.peer_connect_timeout, Duration::from_millis(500));
        assert_eq!(config.peer_handshake_timeout, Duration::from_millis(500));
        assert_eq!(config.peer_frame_timeout, Duration::from_millis(1_000));
    }

    #[test]
    fn normalizes_emergency_dns_max_after_initial() {
        let config = parse_config(&[
            "--emergency-dns-backoff-initial-ms",
            "100",
            "--emergency-dns-backoff-max-ms",
            "1",
        ]);

        assert_eq!(config.emergency_dns_backoff_initial_ms, 500);
        assert_eq!(config.emergency_dns_backoff_max_ms, 500);
    }

    #[test]
    fn peer_write_timeout_is_configurable() {
        assert_eq!(
            parse_config(&["--peer-write-timeout-ms", "1234"]).peer_write_timeout,
            Duration::from_millis(1_234)
        );
    }

    #[test]
    fn automatic_critical_threshold_is_one_for_positive_small_target() {
        assert_eq!(
            parse_config(&["--target-outbound", "1"]).critical_peer_threshold,
            1
        );
        assert_eq!(
            parse_config(&["--target-outbound", "0"]).critical_peer_threshold,
            0
        );
    }

    #[test]
    fn invalid_peer_value_returns_error() {
        let err = Config::from_args(["QubicLightNode", "--peer", "bad-value"])
            .expect_err("peer should be invalid");
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
        assert_eq!(
            err.to_string(),
            "error: Invalid peer value: bad-value. Expected ip or ip:port\n\nUsage: QubicLightNode [OPTIONS]\n\nFor more information, try '--help'.\n"
        );
    }

    #[test]
    fn rejects_frame_limit_outside_protocol_range() {
        let minimum = crate::frame::MIN_OPERATIONAL_FRAME_BYTES;
        let below_minimum = (minimum - 1).to_string();
        for value in [below_minimum.as_str(), "16777216"] {
            let err = Config::from_args(["QubicLightNode", "--max-frame-bytes", value])
                .expect_err("invalid frame limit should be rejected");
            assert_eq!(err.kind(), ErrorKind::ValueValidation);
        }
        let minimum = minimum.to_string();
        assert_eq!(
            parse_config(&["--max-frame-bytes", &minimum]).max_frame_bytes,
            crate::frame::MIN_OPERATIONAL_FRAME_BYTES
        );
    }

    #[test]
    fn rejects_more_manual_peers_than_pool_capacity() {
        let err = Config::from_args([
            "QubicLightNode",
            "--max-known-peers",
            "1",
            "--peer",
            "1.1.1.1",
            "--peer",
            "2.2.2.2",
        ])
        .expect_err("manual peers must fit into the configured pool");

        assert_eq!(err.kind(), ErrorKind::ValueValidation);
        assert!(err.to_string().contains("must be at least"));
    }

    #[test]
    fn rejects_outbound_target_larger_than_known_peer_pool() {
        let err = Config::from_args([
            "QubicLightNode",
            "--target-outbound",
            "33",
            "--max-known-peers",
            "32",
        ])
        .expect_err("outbound target must fit in the known peer pool");

        assert_eq!(err.kind(), ErrorKind::ValueValidation);
        assert!(err.to_string().contains("must not exceed"));
    }

    #[test]
    fn rejects_unreachable_critical_peer_threshold() {
        for args in [
            ["--target-outbound", "2", "--critical-peer-threshold", "3"],
            ["--target-outbound", "0", "--critical-peer-threshold", "1"],
        ] {
            let err = Config::from_args(std::iter::once("QubicLightNode").chain(args))
                .expect_err("critical threshold must be reachable");
            assert_eq!(err.kind(), ErrorKind::ValueValidation);
            assert!(
                err.to_string()
                    .contains("must not exceed --target-outbound")
            );
        }
    }
}
