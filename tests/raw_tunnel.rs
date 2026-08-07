//! End-to-end: TCP client → mixnet → pinned server → local echo upstream.
//! Talks to the live mixnet: run with `cargo test --test raw_tunnel -- --ignored`.
use anyhow::Result;
use nym_rpc::tcp_proxy_client::TcpProxyClient;
use nym_rpc::tcp_proxy_server::TcpProxyServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
#[ignore]
async fn raw_bytes_roundtrip_through_mixnet() -> Result<()> {
    // 1. Echo upstream
    let echo = TcpListener::bind("127.0.0.1:19999").await?;
    tokio::spawn(async move {
        loop {
            let (mut s, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    s.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    // 2. Pinned proxy server on the mixnet
    let dir = tempfile::TempDir::new()?;
    let mut server = TcpProxyServer::new(
        dir.path().to_str().unwrap(),
        None,
        Some("127.0.0.1:19999".into()),
        3600,
        512,
    )
    .await?;
    let server_addr = *server.nym_address();
    tokio::spawn(async move { server.run_with_shutdown().await });

    // 3. Raw client — upstream value is ignored by the pinned server
    let client = TcpProxyClient::new(
        server_addr,
        "ignored:1".into(),
        "127.0.0.1",
        "19888",
        30,
        None,
        1,
    )
    .await?;
    tokio::spawn(async move { client.run().await });
    tokio::time::sleep(std::time::Duration::from_secs(15)).await; // mixnet warm-up

    // 4. Round-trip binary payload (gRPC-ish frame, not valid HTTP/1.1)
    let mut conn = TcpStream::connect("127.0.0.1:19888").await?;
    let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
    conn.write_all(&payload).await?;
    let mut received = vec![0u8; payload.len()];
    tokio::time::timeout(
        std::time::Duration::from_secs(120),
        conn.read_exact(&mut received),
    )
    .await??;
    assert_eq!(received, payload);
    Ok(())
}
