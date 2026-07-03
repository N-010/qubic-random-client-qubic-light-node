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
const DEFAULT_MAX_INCOMING: usize = 32;
const DEFAULT_MAX_SEEN: usize = 65_536;
const DEFAULT_MAX_KNOWN_PEERS: usize = 500;
const DEFAULT_RECONNECT_MS: u64 = 2_000;
const DEFAULT_PEER_WRITE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_DNS_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_EMERGENCY_DNS_BACKOFF_INITIAL_MS: u64 = 10_000; // 10с
const DEFAULT_EMERGENCY_DNS_BACKOFF_MAX_MS: u64 = 300_000; // 5мин
const MIN_API_TIMEOUT_MS: u64 = 1_000;
const MIN_RECONNECT_MS: u64 = 200;
const MIN_DNS_TIMEOUT_MS: u64 = 500;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Config {
    pub(crate) listen_addr: SocketAddrV4,
    pub(crate) api_timeout: Duration,
    pub(crate) grpc_listen_addr: SocketAddr,
    pub(crate) grpc_enabled: bool,
    pub(crate) peer_port: u16,
    pub(crate) target_outbound: usize,
    pub(crate) max_incoming: usize,
    pub(crate) max_seen: usize,
    pub(crate) max_known_peers: usize,
    pub(crate) reconnect_interval: Duration,
    pub(crate) peer_write_timeout: Duration,
    pub(crate) relay_all: bool,
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
#[command(name = "QubicLightNode", version, about = "Qubic light relay node")]
struct Cli {
    #[arg(
        long = "peer",
        value_name = "IP[:PORT]",
        help = "Seed peer; can be repeated, and plain IP uses --peer-port",
        help_heading = "P2P"
    )]
    seed_peer_args: Vec<String>,

    #[arg(
        long = "port",
        value_name = "PORT",
        default_value_t = DEFAULT_PORT,
        help = "Local TCP listen port for incoming Qubic peers",
        help_heading = "P2P"
    )]
    listen_port: u16,

    #[arg(
        long = "peer-port",
        value_name = "PORT",
        default_value_t = DEFAULT_PORT,
        help = "Default remote port for discovery and --peer values without a port",
        help_heading = "P2P"
    )]
    peer_port: u16,

    #[arg(
        long = "listen-ip",
        value_name = "IPV4",
        default_value = "0.0.0.0",
        help = "IPv4 address to bind the TCP listener to",
        help_heading = "P2P"
    )]
    listen_ip: Ipv4Addr,

    #[arg(
        long = "target-outbound",
        value_name = "N",
        default_value_t = DEFAULT_TARGET_OUTBOUND,
        help = "Desired number of outbound peer connections to keep",
        help_heading = "P2P"
    )]
    target_outbound: usize,

    #[arg(
        long = "max-incoming",
        value_name = "N",
        default_value_t = DEFAULT_MAX_INCOMING,
        help = "Maximum number of incoming peer sessions",
        help_heading = "P2P"
    )]
    max_incoming: usize,

    #[arg(
        long = "max-seen",
        value_name = "N",
        default_value_t = DEFAULT_MAX_SEEN,
        help = "Deduplication window size for seen frame hashes",
        help_heading = "P2P"
    )]
    max_seen: usize,

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
        long = "relay-all",
        help = "Relay frames even when dejavu is non-zero",
        help_heading = "Relay"
    )]
    relay_all: bool,

    #[arg(
        long = "traffic-log",
        help = "Log RX, TX, and relay activity for network frames",
        help_heading = "Relay"
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

    #[arg(
        long = "no-grpc",
        help = "Disable the gRPC API server",
        help_heading = "API"
    )]
    no_grpc: bool,
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

        Ok(Config {
            listen_addr: SocketAddrV4::new(cli.listen_ip, cli.listen_port),
            api_timeout: Duration::from_millis(cli.api_timeout_ms.max(MIN_API_TIMEOUT_MS)),
            grpc_listen_addr: cli.grpc_listen,
            grpc_enabled: !cli.no_grpc,
            peer_port: cli.peer_port,
            target_outbound: cli.target_outbound,
            max_incoming: cli.max_incoming,
            max_seen: cli.max_seen,
            max_known_peers: cli.max_known_peers,
            reconnect_interval: Duration::from_millis(cli.reconnect_ms.max(MIN_RECONNECT_MS)),
            peer_write_timeout: Duration::from_millis(cli.peer_write_timeout_ms),
            relay_all: cli.relay_all,
            dns_bootstrap: !cli.no_dns_bootstrap,
            dns_lite_peers: cli.dns_lite_peers,
            dns_timeout: Duration::from_millis(cli.dns_timeout_ms.max(MIN_DNS_TIMEOUT_MS)),
            traffic_log: cli.traffic_log,
            seed_peers,
            critical_peer_threshold: if cli.critical_peer_threshold == 0 && cli.target_outbound > 0
            {
                (cli.target_outbound / 2).max(1)
            } else {
                cli.critical_peer_threshold
            },
            emergency_dns_bootstrap: !cli.no_emergency_dns,
            emergency_dns_backoff_initial_ms: cli
                .emergency_dns_backoff_initial_ms
                .max(MIN_DNS_TIMEOUT_MS),
            emergency_dns_backoff_max_ms: cli
                .emergency_dns_backoff_max_ms
                .max(cli.emergency_dns_backoff_initial_ms),
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
        assert!(help.contains("Disable the gRPC API server"));
    }

    #[test]
    fn uses_defaults_without_args() {
        assert_eq!(
            parse_config(&[]),
            Config {
                listen_addr: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), DEFAULT_PORT),
                api_timeout: Duration::from_millis(6_000),
                grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT)),
                grpc_enabled: true,
                peer_port: DEFAULT_PORT,
                target_outbound: 8,
                max_incoming: 32,
                max_seen: 65_536,
                max_known_peers: 500,
                reconnect_interval: Duration::from_millis(2_000),
                peer_write_timeout: Duration::from_millis(5_000),
                relay_all: false,
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
        ]);

        assert_eq!(config.api_timeout, Duration::from_millis(1_000));
        assert_eq!(config.reconnect_interval, Duration::from_millis(200));
        assert_eq!(config.dns_timeout, Duration::from_millis(500));
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
}
