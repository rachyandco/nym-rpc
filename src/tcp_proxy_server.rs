//! Listens for incoming messages from the mixnet and forwards them to the upstream server.
//! Main difference from official TcpProxyServer is that it expects the upstream address to be provided in the first message.
//! This allows for arbitrary RPC providers to be used.
//! I try to keep the code as close as possible to the official TcpProxyServer.
//! - Creating a mixnet client
//! - Handling incoming messages from the mixnet
//! - Forward to the upstream server
//! - Handle the response from the upstream server and send it back to the mixnet
//! - Uses surbs to anonymize the response
//! TODO: allowlist providers, test session_handler

use crate::common::{extract_upstream_header, resolve_upstream};
use anyhow::Result;
use dashmap::{DashMap, DashSet};
use nym_network_defaults::setup_env;
use nym_sdk::mixnet::Recipient;
use nym_sdk::mixnet::{
    AnonymousSenderTag, MixnetClient, MixnetClientBuilder, MixnetClientSender, MixnetMessageSender,
    NymNetworkDetails, StoragePaths,
};
use nym_sdk::tcp_proxy::utils::{MessageBuffer, Payload, ProxiedMessage};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::sync::watch::Receiver;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use uuid::Uuid;

type UpstreamAddress = String;

/// Monitoring counters for a `TcpProxyServer` instance, exposed via the HTTP
/// stats endpoint. All fields use relaxed ordering since these are simple
/// independent counters with no ordering requirements between them.
#[derive(Default)]
pub struct ProxyStats {
    pub total_sessions: AtomicU64,
    pub bytes_to_upstream: AtomicU64,
    pub bytes_from_upstream: AtomicU64,
}

/// Removes a session's entry from the shared sessions map when dropped, so the
/// session is pruned regardless of which path `session_handler` exits through
/// (normal loop break, idle timeout, or an early `?` on a connection/write error).
struct SessionPruneGuard {
    sessions: Arc<DashMap<Uuid, UpstreamAddress>>,
    session_id: Uuid,
}

impl Drop for SessionPruneGuard {
    fn drop(&mut self) {
        self.sessions.remove(&self.session_id);
        info!(
            "Session {} removed, {} active",
            self.session_id,
            self.sessions.len()
        );
    }
}

/// Aborts the detached upstream→client reader task when dropped, so a session
/// that ends (idle timeout, close, or error) cannot keep relaying upstream
/// bytes via SURB after the handler has exited.
struct AbortOnDrop(tokio::task::JoinHandle<Result<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub struct TcpProxyServer {
    sessions: Arc<DashMap<Uuid, UpstreamAddress>>,
    stats: Arc<ProxyStats>,
    mixnet_client: MixnetClient,
    mixnet_client_sender: Arc<RwLock<MixnetClientSender>>,
    tx: tokio::sync::watch::Sender<Option<(ProxiedMessage, AnonymousSenderTag)>>,
    rx: tokio::sync::watch::Receiver<Option<(ProxiedMessage, AnonymousSenderTag)>>,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    cancel_token: CancellationToken,
    pinned_upstream: Option<String>,
    session_idle_timeout: std::time::Duration,
    max_sessions: usize,
}

impl TcpProxyServer {
    // create mixnet client
    pub async fn new(
        config_dir: &str,
        env: Option<String>,
        pinned_upstream: Option<String>,
        idle_timeout_secs: u64,
        max_sessions: usize,
    ) -> Result<Self> {
        info!("creating client");
        debug!("Loading env file: {:?}", env);
        setup_env(env);

        // use specified directory
        let config_dir = PathBuf::from(config_dir);
        let storage_paths = StoragePaths::new_from_dir(&config_dir)?;
        // default network is mainnet
        let net = NymNetworkDetails::new_from_env();

        // Create mixnet client
        let client = MixnetClientBuilder::new_with_default_storage(storage_paths)
            .await?
            .network_details(net)
            .build()?;

        // Connect to mixnet
        let client = client.connect_to_mixnet().await?;

        // Since we're splitting the client in the main thread, we have to wrap the sender side of the client in an Arc<RwLock>>.
        let sender = Arc::new(RwLock::new(client.split_sender()));

        // Used for passing the incoming Mixnet message => session_handler().
        let (tx, rx) =
            tokio::sync::watch::channel::<Option<(ProxiedMessage, AnonymousSenderTag)>>(None);

        // Our shutdown signal channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);

        info!("Client created: {}", client.nym_address());

        Ok(TcpProxyServer {
            sessions: Arc::new(DashMap::new()),
            stats: Arc::new(ProxyStats::default()),
            mixnet_client: client,
            mixnet_client_sender: sender,
            tx,
            rx,
            cancel_token: CancellationToken::new(),
            shutdown_tx,
            shutdown_rx,
            pinned_upstream,
            session_idle_timeout: std::time::Duration::from_secs(idle_timeout_secs),
            max_sessions,
        })
    }

    pub async fn run_with_shutdown(&mut self) -> Result<()> {
        let handle_token = self.cancel_token.child_token();
        let rx = self.rx();
        let mixnet_sender = self.mixnet_client_sender();
        let tx = self.tx.clone();
        let sessions = self.sessions();
        let stats = self.stats();
        let pinned_upstream = self.pinned_upstream.clone();
        let session_idle_timeout = self.session_idle_timeout;
        let max_sessions = self.max_sessions;

        let mut shutdown_rx =
            std::mem::replace(&mut self.shutdown_rx, tokio::sync::mpsc::channel(1).1);

        // Then get the message stream: poll this for incoming messages
        let message_stream = self.mixnet_client_mut();

        loop {
            tokio::select! {
                Some(()) = shutdown_rx.recv() => {
                    debug!("Received shutdown signal, stopping TcpProxyServer");
                    handle_token.cancel();
                    break;
                }
                // On our Mixnet client getting a new message:
                // - Check if the attached sessionID exists.
                // - If !sessionID, spawn a new session_handler() task.
                // - Send the message down tx => rx in our handler.
                message = message_stream.next() => {
                    if let Some(new_message) = message {
                        let mut message: ProxiedMessage = match bincode::deserialize(&new_message.message) {
                            Ok(msg) => {
                                debug!("received: {}", msg);
                                msg
                            },
                            Err(e) => {
                                error!("Failed to deserialize ProxiedMessage: {}", e);
                                continue;
                            }
                        };

                        let session_id = message.session_id();

                        // The client adds the UPSTREAM header only to message_id == 1, but
                        // the mixnet does not preserve ordering: a later message can create
                        // the session before message 1 arrives. Strip the header from every
                        // message_id == 1 — whether or not it creates the session — so the
                        // header bytes never reach the upstream write path.
                        let header_upstream = if message.message_id() == 1 {
                            match message.message() {
                                Payload::Data(data) => {
                                    let (upstream, stripped_data) = extract_upstream_header(data);
                                    message.message = Payload::Data(stripped_data);
                                    Some(upstream)
                                }
                                Payload::Close => None,
                            }
                        } else {
                            None
                        };

                        if !sessions.contains_key(&session_id) {
                            debug!("Got message for a new session");

                            if sessions.len() >= max_sessions {
                                error!("Session limit {} reached, dropping new session {}", max_sessions, session_id);
                                continue;
                            }

                            let client_upstream = match message.message() {
                                // Empty when the session-creating message is not message 1
                                // (out-of-order arrival) — resolve_upstream then falls back
                                // to the pin, or drops the session when no pin is set,
                                // exactly as before.
                                Payload::Data(_) => header_upstream.unwrap_or_default(),
                                Payload::Close => {
                                    error!("First message cannot be Close");
                                    continue;
                                }
                            };

                            let upstream_address = match resolve_upstream(pinned_upstream.as_deref(), &client_upstream) {
                                Some(addr) => addr,
                                None => {
                                    error!("No upstream available for session {}, dropping", session_id);
                                    continue;
                                }
                            };
                            if pinned_upstream.is_some() && client_upstream != upstream_address {
                                debug!("Ignoring client-supplied upstream {:?} (pinned)", client_upstream);
                            }
                            info!("Upstream address: {}", upstream_address);

                            // Store session
                            sessions.insert(session_id, upstream_address.clone());
                            stats.total_sessions.fetch_add(1, Ordering::Relaxed);

                            tokio::spawn(Self::session_handler(
                                upstream_address,
                                session_id,
                                rx.clone(),
                                mixnet_sender.clone(),
                                handle_token.clone(),
                                sessions.clone(),
                                session_idle_timeout,
                                stats.clone(),
                            ));

                            info!("Spawned a new session handler: {}", session_id);
                        }

                        debug!("Sending message for session {}", session_id);

                        if let Some(sender_tag) = new_message.sender_tag {
                            if let Err(e) = tx.send(Some((message, sender_tag))) {
                                error!("Failed to send ProxiedMessage: {}", e);
                            }
                        } else {
                            error!("No sender tag found, we can't send a reply without it!");
                        }
                    }
                }
            }
        }

        self.shutdown_rx = shutdown_rx;
        Ok(())
    }

    // The main body of our logic, triggered on each received new sessionID. To deal with assumptions about
    // streaming we have to implement an abstract session for each set of outgoing messages atop each connection, with message
    // IDs to deal with the fact that the mixnet does not enforce message ordering.
    //
    // There is an initial thread which does a bunch of setup logic:
    // - Create a TcpStream connecting to our upstream server process.
    // - Split incoming TcpStream into OwnedReadHalf and OwnedWriteHalf for concurrent read/write.
    // - Create an Arc to store our session SURB - used for anonymous replies.
    //
    // Then we spawn 2 tasks:
    // - 'Incoming' thread => deals with parsing and storing the SURB (used in Mixnet replies), deserialising and passing the incoming data from the Mixnet to the upstream server.
    // - 'Outgoing' thread => frames bytes coming from TcpStream (the server) and deals with ordering + sending reply anonymously => Mixnet.
    #[allow(clippy::too_many_arguments)]
    async fn session_handler(
        upstream_address: String,
        session_id: Uuid,
        mut rx: Receiver<Option<(ProxiedMessage, AnonymousSenderTag)>>,
        sender: Arc<RwLock<MixnetClientSender>>,
        cancel_token: CancellationToken,
        sessions: Arc<DashMap<Uuid, UpstreamAddress>>,
        idle_timeout: std::time::Duration,
        stats: Arc<ProxyStats>,
    ) -> Result<()> {
        // Ensures the session is pruned from the shared map on every exit path,
        // including early returns via `?` (e.g. connect/write errors), not just
        // a normal fall-through past the bottom of the loop.
        let _session_prune_guard = SessionPruneGuard {
            sessions,
            session_id,
        };

        let global_surb = Arc::new(RwLock::new(None));
        let last_activity = Arc::new(std::sync::Mutex::new(tokio::time::Instant::now()));

        debug!(
            "Connecting to upstream server {} for session {}",
            upstream_address, session_id
        );
        let stream = TcpStream::connect(&upstream_address).await?;

        // Split our tcpstream into OwnedRead and OwnedWrite halves for concurrent read/writing.
        let (read, mut write) = stream.into_split();
        // Used for anonymous replies per session. Initially parsed from the incoming message.
        let send_side_surb = Arc::clone(&global_surb);
        let last_activity_up = Arc::clone(&last_activity);
        let stats_up = Arc::clone(&stats);

        let upstream_reader = tokio::spawn(async move {
            let mut message_id = 0;
            // Since we're just trying to pipe whatever bytes our client/server are normally sending to each other,
            // the bytescodec is fine to use here; we're trying to avoid modifying this stream e.g. in the process of Sphinx packet
            // creation and adding padding to the payload whilst also sidestepping the need to manually manage an intermediate buffer of the
            // incoming bytes from the tcp stream and writing them to our server with our Nym client.
            let codec = tokio_util::codec::BytesCodec::new();
            let mut framed_read = tokio_util::codec::FramedRead::new(read, codec);

            // While able to read from OwnedReadHalf of TcpStream:
            // - Keep track of outgoing messageIDs.
            // - Read and store incoming SURB.
            // - Send serialised reply => Mixnet via SURB.
            // - If tick() returns true, close session.
            while let Some(Ok(bytes)) = framed_read.next().await {
                *last_activity_up.lock().unwrap() = tokio::time::Instant::now();
                debug!("Server received {} bytes", bytes.len());
                stats_up
                    .bytes_from_upstream
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                let reply =
                    ProxiedMessage::new(Payload::Data(bytes.to_vec()), session_id, message_id);
                message_id += 1;
                let surb = send_side_surb.read().await;
                if let Some(surb) = *surb {
                    sender
                        .write()
                        .await
                        .send_reply(surb, bincode::serialize(&reply)?)
                        .await?
                }
                info!(
                    "Sent reply with id {} for session {}",
                    message_id, session_id
                );
            }
            Ok::<(), anyhow::Error>(())
        });
        // Declared after `_session_prune_guard` so it drops first on any exit
        // path (including `?`): the reader is aborted, then the prune guard
        // removes the session from the map and logs the removal.
        let _upstream_reader_guard = AbortOnDrop(upstream_reader);

        let messages_accounter = Arc::new(DashSet::new());
        messages_accounter.insert(1);

        let mut msg_buffer = MessageBuffer::new();
        // Use an Interval rather than a fresh `sleep` per loop iteration: `rx` is
        // a watch channel woken by EVERY inbound mixnet message across ALL
        // sessions, so a recreated sleep future would never fire under load and
        // idle sessions would never be reaped. An Interval keeps its deadline
        // across recreated `tick()` futures.
        let mut flush_interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                    _ = rx.changed() => {
                        let value = rx.borrow_and_update().clone();
                        if let Some((message, surb)) = value {
                            if message.session_id() != session_id {
                                continue;
                            }

                            *last_activity.lock().unwrap() = tokio::time::Instant::now();

                            let payload_len = match message.message() {
                                Payload::Data(data) => data.len(),
                                Payload::Close => 0,
                            };
                            stats
                                .bytes_to_upstream
                                .fetch_add(payload_len as u64, Ordering::Relaxed);

                            msg_buffer.push(message);

                            let local_surb = Arc::clone(&global_surb);
                            {
                                *local_surb.write().await = Some(surb);
                            }

                            let should_close = msg_buffer.tick(&mut write).await?;
                            if should_close {
                                info!("Closing write end of session: {}", session_id);
                                break;
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    _ = flush_interval.tick() => {
                        msg_buffer.tick(&mut write).await?;

                        if last_activity.lock().unwrap().elapsed() > idle_timeout {
                            info!("Session {} idle > {:?}, closing", session_id, idle_timeout);
                            break;
                        }
                    }
            }
        }
        // This times out after 60 seconds by default.
        // `_session_prune_guard` prunes the session from the shared map on drop here.
        Ok(())
    }

    pub fn disconnect_signal(&self) -> tokio::sync::mpsc::Sender<()> {
        self.shutdown_tx.clone()
    }

    pub fn nym_address(&self) -> &Recipient {
        self.mixnet_client.nym_address()
    }

    pub fn mixnet_client_mut(&mut self) -> &mut MixnetClient {
        &mut self.mixnet_client
    }

    pub fn sessions(&self) -> Arc<DashMap<Uuid, UpstreamAddress>> {
        Arc::clone(&self.sessions)
    }

    pub fn stats(&self) -> Arc<ProxyStats> {
        Arc::clone(&self.stats)
    }

    pub fn mixnet_client_sender(&self) -> Arc<RwLock<MixnetClientSender>> {
        Arc::clone(&self.mixnet_client_sender)
    }

    pub fn tx(&self) -> tokio::sync::watch::Sender<Option<(ProxiedMessage, AnonymousSenderTag)>> {
        self.tx.clone()
    }

    pub fn rx(&self) -> tokio::sync::watch::Receiver<Option<(ProxiedMessage, AnonymousSenderTag)>> {
        self.rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_run_with_shutdown() -> Result<()> {
        let config_dir = TempDir::new()?;
        let mut server =
            TcpProxyServer::new(config_dir.path().to_str().unwrap(), None, None, 3600, 512).await?;

        // Getter for shutdown signal tx
        let shutdown_tx = server.disconnect_signal();

        let server_handle = tokio::spawn(async move { server.run_with_shutdown().await });

        // Let it start up
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        // Kill server
        shutdown_tx.send(()).await?;

        // Wait for shutdown in handle + check Result != err
        server_handle.await??;

        Ok(())
    }
}
