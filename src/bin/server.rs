use anyhow::Result;
use clap::Parser;
use nym_node_requests::api::v1::node::models::NodeDescription;
use nym_rpc::api_server::{TcpProxyHttpConfig, start_http_server};
use nym_rpc::tcp_proxy_server::TcpProxyServer;
use nym_rpc::watchdog::WatchdogConfig;
use std::path::PathBuf;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "nym-rpc-server")]
#[command(
    about = "Receives RPC requests through the mixnet, forwards them to the specified RPC provider, and replies with the response via SURBs"
)]
struct Args {
    /// Listen address for the HTTP API. Keep on loopback / behind a firewall
    /// for public deployments: /api/v1/stats exposes traffic-sensitive counters
    #[clap(long, default_value = "127.0.0.1")]
    api_listen_address: String,

    // listen port for the HTTP API
    #[clap(long, default_value = "8080")]
    api_listen_port: u16,

    // path to the NYM client configuration directory
    #[clap(short, long, default_value = "/tmp/nym-rpc-server")]
    config_dir: PathBuf,

    // nym env filepath to create the nym client with
    // if none provided we use the default mainnet env file
    #[clap(short, long)]
    env: Option<String>,

    /// Pin all sessions to this upstream (host:port). REQUIRED for public
    /// operation — without it clients choose the upstream (open relay).
    #[clap(long)]
    pin_upstream: Option<String>,

    /// Close sessions with no traffic in either direction for this many seconds
    #[clap(long, default_value_t = 3600)]
    session_idle_timeout: u64,

    /// Maximum number of concurrent sessions allowed
    #[clap(long, default_value_t = 512)]
    max_sessions: usize,

    /// Send a liveness self-ping through the mixnet every this many seconds
    #[clap(long, default_value_t = WatchdogConfig::DEFAULT_INTERVAL_SECS)]
    watchdog_interval: u64,

    /// Exit (for systemd to restart us) when no self-ping has returned for
    /// this many seconds. Must be longer than --watchdog-interval
    #[clap(long, default_value_t = WatchdogConfig::DEFAULT_TIMEOUT_SECS)]
    watchdog_timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    nym_bin_common::logging::setup_tracing_logger();
    let args = Args::parse();
    let startup_time = std::time::Instant::now();

    info!("Starting Nym TCP Proxy Server...");

    let config_dir = args.config_dir.to_string_lossy().to_string();
    let http_bind_address = format!("{}:{}", args.api_listen_address, args.api_listen_port);

    info!("HTTP API enabled on {}", http_bind_address);

    let description = NodeDescription {
        moniker: "nym-rpc-server".to_string(),
        details: "Nym RPC server".to_string(),
        website: "https://github.com/ezio-auditore-1375/nym-rpc".to_string(),
        security_contact: "".to_string(), // Could be made configurable
    };

    let http_config = TcpProxyHttpConfig {
        bind_address: http_bind_address.parse()?,
        description,
        expose_system_info: true,
    };

    match &args.pin_upstream {
        Some(pin) => info!("Upstream pinning ACTIVE: all sessions forced to {}", pin),
        None => warn!(
            "Upstream pinning DISABLED: clients may choose any upstream address (open relay). \
             Pass --pin-upstream <host:port> for public operation."
        ),
    }

    let watchdog_config = WatchdogConfig::new(args.watchdog_interval, args.watchdog_timeout)?;
    info!(
        "Mixnet watchdog: self-ping every {}s, fatal after {}s without a round trip",
        args.watchdog_interval, args.watchdog_timeout
    );

    let mut server = TcpProxyServer::new(
        &config_dir,
        args.env,
        args.pin_upstream,
        args.session_idle_timeout,
        args.max_sessions,
        watchdog_config,
    )
    .await?;

    // Start HTTP server if enabled
    let _http_handle = start_http_server(
        server.nym_address(),
        startup_time,
        &http_config,
        server.stats(),
        server.sessions(),
        server.watchdog(),
    )
    .await?;

    info!("✓ Server initialized successfully");
    info!("Nym address: {}", server.nym_address());

    info!("HTTP API available at: http://{}", http_bind_address);
    info!("Try: http://{}/api/v1/description", http_bind_address);
    info!("Try: http://{}/api/v1/health", http_bind_address);

    info!("Starting TCP proxy server...");

    // Turn SIGTERM (systemd stop) and Ctrl+C into a shutdown request instead
    // of letting the default signal action kill the process mid-flight.
    let shutdown_tx = server.disconnect_signal();
    tokio::spawn(async move {
        wait_for_termination_signal().await;
        info!("Termination signal received, shutting down...");
        let _ = shutdown_tx.send(()).await;
    });

    // Run the server
    let result = server.run_with_shutdown().await;
    if let Err(e) = &result {
        error!("Server error: {}", e);
    }

    // Disconnect on every exit path, fatal ones included, so the SDK flushes
    // its reply store. Bounded: a dead mixnet client must not keep the process
    // from exiting and being restarted by the supervisor.
    match tokio::time::timeout(DISCONNECT_TIMEOUT, server.disconnect()).await {
        Ok(()) => info!("Mixnet client disconnected"),
        Err(_) => warn!(
            "Mixnet client did not disconnect within {:?}, exiting anyway",
            DISCONNECT_TIMEOUT
        ),
    }

    result?;
    info!("Server shut down gracefully");
    Ok(())
}

/// Upper bound for the mixnet client teardown at exit. Well below systemd's
/// default 90 s stop timeout.
const DISCONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Resolves on the first SIGTERM or SIGINT.
async fn wait_for_termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
