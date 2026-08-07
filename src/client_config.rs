pub const ZCASH_DEFAULT_PORT: u16 = 8137;
pub const ZCASH_DEFAULT_UPSTREAM: &str = "127.0.0.1:8137";
/// Published after first deployment; empty means "no default yet".
pub const ZCASH_SERVICE_NYM_ADDRESS: &str = "";

#[derive(Debug, Clone, PartialEq)]
pub enum ClientMode {
    Http { rpc_provider: String },
    Raw { upstream: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    pub mode: ClientMode,
    pub listen_address: String,
    pub listen_port: u16,
    pub exit_node_address: Option<String>, // None = pick random known exit
}

#[derive(Debug, Default)]
pub struct ClientArgs {
    pub raw: bool,
    pub zcash: bool,
    pub rpc_provider: Option<String>,
    pub upstream: Option<String>,
    pub listen_address: String,
    pub listen_port: Option<u16>,
    pub exit_node_address: Option<String>,
}

pub fn resolve_client_config(a: ClientArgs) -> anyhow::Result<ClientConfig> {
    let raw = a.raw || a.zcash;
    if raw {
        let upstream = a.upstream.unwrap_or_else(|| ZCASH_DEFAULT_UPSTREAM.to_string());
        let listen_port = a.listen_port.unwrap_or(ZCASH_DEFAULT_PORT);
        let exit_node_address = a.exit_node_address.or_else(|| {
            (a.zcash && !ZCASH_SERVICE_NYM_ADDRESS.is_empty())
                .then(|| ZCASH_SERVICE_NYM_ADDRESS.to_string())
        });
        if a.zcash && exit_node_address.is_none() {
            anyhow::bail!(
                "--zcash needs an exit node address (-x) until a default service address is published"
            );
        }
        Ok(ClientConfig { mode: ClientMode::Raw { upstream }, listen_address: a.listen_address, listen_port, exit_node_address })
    } else {
        let rpc_provider = a.rpc_provider
            .ok_or_else(|| anyhow::anyhow!("--rpc-provider is required unless --raw/--zcash"))?;
        Ok(ClientConfig {
            mode: ClientMode::Http { rpc_provider },
            listen_address: a.listen_address,
            listen_port: a.listen_port.unwrap_or(8545),
            exit_node_address: a.exit_node_address,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ClientArgs {
        ClientArgs { listen_address: "127.0.0.1".into(), ..Default::default() }
    }

    #[test]
    fn zcash_preset_defaults() {
        let cfg = resolve_client_config(ClientArgs {
            zcash: true,
            exit_node_address: Some("dummyaddr".into()),
            ..base()
        }).unwrap();
        assert_eq!(cfg.mode, ClientMode::Raw { upstream: "127.0.0.1:8137".into() });
        assert_eq!(cfg.listen_port, 8137);
        assert_eq!(cfg.exit_node_address, Some("dummyaddr".into()));
    }

    #[test]
    fn zcash_without_exit_address_errors_while_no_default_published() {
        assert!(resolve_client_config(ClientArgs { zcash: true, ..base() }).is_err());
    }

    #[test]
    fn raw_mode_custom_upstream_and_port() {
        let cfg = resolve_client_config(ClientArgs {
            raw: true,
            upstream: Some("127.0.0.1:9067".into()),
            listen_port: Some(9999),
            exit_node_address: Some("x".into()),
            ..base()
        }).unwrap();
        assert_eq!(cfg.mode, ClientMode::Raw { upstream: "127.0.0.1:9067".into() });
        assert_eq!(cfg.listen_port, 9999);
    }

    #[test]
    fn http_mode_requires_rpc_provider() {
        assert!(resolve_client_config(base()).is_err());
    }

    #[test]
    fn http_mode_default_port_8545() {
        let cfg = resolve_client_config(ClientArgs {
            rpc_provider: Some("https://mainnet.infura.io/v3/KEY".into()),
            ..base()
        }).unwrap();
        assert_eq!(cfg.listen_port, 8545);
        assert!(matches!(cfg.mode, ClientMode::Http { .. }));
    }
}
