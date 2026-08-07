use anyhow::Result;
use clap::Parser;
use nym_node_requests::api::v1::node::models::NodeDescription;
use nym_rpc::api_server::{TcpProxyHttpConfig, start_http_server};
use nym_rpc::tcp_proxy_server::TcpProxyServer;
use std::path::PathBuf;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "nym-rpc-server")]
#[command(
    about = "Receives RPC requests through the mixnet, forwards them to the specified RPC provider, and replies with the response via SURBs"
)]
struct Args {
    // listen address for the HTTP API
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

    let mut server = TcpProxyServer::new(
        &config_dir,
        args.env,
        args.pin_upstream,
        args.session_idle_timeout,
        args.max_sessions,
    )
    .await?;

    // Start HTTP server if enabled
    let _http_handle = start_http_server(
        server.nym_address(),
        startup_time,
        &http_config,
        server.stats(),
        server.sessions(),
    )
    .await?;

    info!("✓ Server initialized successfully");
    info!("Nym address: {}", server.nym_address());

    info!("HTTP API available at: http://{}", http_bind_address);
    info!("Try: http://{}/api/v1/description", http_bind_address);
    info!("Try: http://{}/api/v1/health", http_bind_address);

    info!("Starting TCP proxy server...");

    // Run the server
    if let Err(e) = server.run_with_shutdown().await {
        error!("Server error: {}", e);
        return Err(e);
    }

    info!("Server shut down gracefully");
    Ok(())
}
