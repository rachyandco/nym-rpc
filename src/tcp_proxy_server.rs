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
use crate::watchdog::{Watchdog, WatchdogConfig, run_watchdog};
use anyhow::Result;
use dashmap::{DashMap, DashSet};
use nym_network_defaults::setup_env;
use nym_sdk::mixnet::Recipient;
use nym_sdk::mixnet::{
    AnonymousSenderTag, IncludedSurbs, MixnetClient, MixnetClientBuilder, MixnetClientSender,
    MixnetMessageSender, NymNetworkDetails, StoragePaths,
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
    watchdog: Arc<Watchdog>,
    watchdog_interval: std::time::Duration,
}

impl TcpProxyServer {
    // create mixnet client
    pub async fn new(
        config_dir: &str,
        env: Option<String>,
        pinned_upstream: Option<String>,
        idle_timeout_secs: u64,
        max_sessions: usize,
        watchdog_config: WatchdogConfig,
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
            watchdog: Watchdog::new(watchdog_config.timeout),
            watchdog_interval: watchdog_config.interval,
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

        let spawn_rx = rx.clone();
        let spawn_token = handle_token.clone();
        let spawn_sessions = Arc::clone(&sessions);
        let spawn_stats = Arc::clone(&stats);
        let spawn_session = move |upstream_address: String, session_id: Uuid| {
            tokio::spawn(Self::session_handler(
                upstream_address,
                session_id,
                spawn_rx.clone(),
                mixnet_sender.clone(),
                spawn_token.clone(),
                spawn_sessions.clone(),
                session_idle_timeout,
                spawn_stats.clone(),
            ));
        };

        // Liveness watchdog: pings our own Nym address and cancels
        // `watchdog_dead` if no ping completes a round trip within the timeout.
        let watchdog = Arc::clone(&self.watchdog);
        let watchdog_dead = CancellationToken::new();
        let watchdog_shutdown = CancellationToken::new();
        let self_address = *self.nym_address();
        let ping_sender = self.mixnet_client_sender();
        let watchdog_task = tokio::spawn(run_watchdog(
            Arc::clone(&watchdog),
            self.watchdog_interval,
            move |ping: Vec<u8>| {
                let sender = Arc::clone(&ping_sender);
                async move {
                    sender
                        .read()
                        .await
                        .send_message(self_address, ping, IncludedSurbs::ExposeSelfAddress)
                        .await
                        .map_err(anyhow::Error::from)
                }
            },
            watchdog_dead.clone(),
            watchdog_shutdown.clone(),
        ));

        // Then get the message stream: poll this for incoming messages
        let message_stream = self.mixnet_client_mut();

        let result = Self::message_loop(
            message_stream,
            &mut shutdown_rx,
            &handle_token,
            &tx,
            &sessions,
            &stats,
            &watchdog,
            &watchdog_dead,
            pinned_upstream.as_deref(),
            max_sessions,
            spawn_session,
        )
        .await;

        watchdog_shutdown.cancel();
        watchdog_task.abort();
        self.shutdown_rx = shutdown_rx;
        result
    }

    /// Drives the inbound mixnet message loop until either a shutdown is
    /// requested (Ok) or the mixnet client is found dead (Err), which happens
    /// when the message stream terminates or the watchdog reports that no
    /// self-ping has completed a round trip within its timeout. The SDK does
    /// not recover from a lost gateway connection, so staying alive would leave
    /// the exit unreachable while looking healthy to the supervisor. Callers
    /// must treat the error as fatal and exit so systemd can restart us.
    ///
    /// Watchdog pings arriving on the stream are handed to `watchdog` and never
    /// reach session handling.
    #[allow(clippy::too_many_arguments)]
    async fn message_loop<S, F>(
        mut message_stream: S,
        shutdown_rx: &mut tokio::sync::mpsc::Receiver<()>,
        handle_token: &CancellationToken,
        tx: &tokio::sync::watch::Sender<Option<(ProxiedMessage, AnonymousSenderTag)>>,
        sessions: &DashMap<Uuid, UpstreamAddress>,
        stats: &ProxyStats,
        watchdog: &Watchdog,
        watchdog_dead: &CancellationToken,
        pinned_upstream: Option<&str>,
        max_sessions: usize,
        mut spawn_session: F,
    ) -> Result<()>
    where
        S: tokio_stream::Stream<Item = nym_sdk::mixnet::ReconstructedMessage> + Unpin,
        F: FnMut(String, Uuid),
    {
        loop {
            tokio::select! {
                Some(()) = shutdown_rx.recv() => {
                    debug!("Received shutdown signal, stopping TcpProxyServer");
                    handle_token.cancel();
                    return Ok(());
                }
                _ = watchdog_dead.cancelled() => {
                    error!(
                        "Watchdog declared the mixnet client dead: no self-ping returned within {:?}. \
                         Shutting down so the supervisor can restart us.",
                        watchdog.timeout()
                    );
                    handle_token.cancel();
                    anyhow::bail!("mixnet watchdog timed out: no self-ping round trip within {:?}", watchdog.timeout());
                }
                // On our Mixnet client getting a new message:
                // - Check if the attached sessionID exists.
                // - If !sessionID, spawn a new session_handler() task.
                // - Send the message down tx => rx in our handler.
                message = message_stream.next() => {
                    let Some(new_message) = message else {
                        error!(
                            "Mixnet message stream terminated: the embedded mixnet client is dead \
                             and cannot recover. Shutting down so the supervisor can restart us."
                        );
                        handle_token.cancel();
                        anyhow::bail!("mixnet message stream terminated unexpectedly");
                    };

                    // Our own watchdog ping coming back: proves the send and
                    // receive paths both work. Never session traffic.
                    if watchdog.observe(&new_message.message) {
                        continue;
                    }

                    {
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

                            let upstream_address = match resolve_upstream(pinned_upstream, &client_upstream) {
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

                            spawn_session(upstream_address, session_id);

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

    /// Disconnects the embedded mixnet client and waits for its tasks to stop.
    ///
    /// Call this on every exit path, after `run_with_shutdown` has returned.
    /// The SDK only marks its on-disk reply store as valid while shutting down
    /// through `disconnect`; a process that just exits leaves the store
    /// unflushed, and the next start rejects it and archives it as
    /// `persistent_reply_store.sqlite._<timestamp>.corrupted`.
    pub async fn disconnect(self) {
        self.cancel_token.cancel();
        self.mixnet_client.disconnect().await;
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

    pub fn watchdog(&self) -> Arc<Watchdog> {
        Arc::clone(&self.watchdog)
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
    use crate::watchdog::Watchdog;
    use nym_sdk::mixnet::ReconstructedMessage;
    use tempfile::TempDir;

    type ProxiedWatchValue = Option<(ProxiedMessage, AnonymousSenderTag)>;

    fn loop_fixture() -> (
        tokio::sync::watch::Sender<ProxiedWatchValue>,
        tokio::sync::watch::Receiver<ProxiedWatchValue>,
        CancellationToken,
        DashMap<Uuid, UpstreamAddress>,
        ProxyStats,
    ) {
        let (tx, watch_rx) = tokio::sync::watch::channel(None);
        (
            tx,
            watch_rx,
            CancellationToken::new(),
            DashMap::new(),
            ProxyStats::default(),
        )
    }

    #[tokio::test]
    async fn message_loop_errors_when_stream_terminates() {
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (tx, _watch_rx, token, sessions, stats) = loop_fixture();

        let result = TcpProxyServer::message_loop(
            tokio_stream::empty::<ReconstructedMessage>(),
            &mut shutdown_rx,
            &token,
            &tx,
            &sessions,
            &stats,
            &Watchdog::new(std::time::Duration::from_secs(300)),
            &CancellationToken::new(),
            Some("127.0.0.1:9"),
            512,
            |_addr: String, _id: Uuid| panic!("no session must spawn"),
        )
        .await;

        assert!(
            result.is_err(),
            "a terminated mixnet stream means the client is dead and must be a fatal error"
        );
        assert!(
            token.is_cancelled(),
            "existing sessions must be cancelled when the mixnet client dies"
        );
    }

    #[tokio::test]
    async fn message_loop_exits_cleanly_on_shutdown_signal() {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        shutdown_tx.send(()).await.unwrap();
        let (tx, _watch_rx, token, sessions, stats) = loop_fixture();

        let result = TcpProxyServer::message_loop(
            tokio_stream::pending::<ReconstructedMessage>(),
            &mut shutdown_rx,
            &token,
            &tx,
            &sessions,
            &stats,
            &Watchdog::new(std::time::Duration::from_secs(300)),
            &CancellationToken::new(),
            Some("127.0.0.1:9"),
            512,
            |_addr: String, _id: Uuid| panic!("no session must spawn"),
        )
        .await;

        assert!(result.is_ok(), "a requested shutdown is not an error");
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn message_loop_spawns_session_and_dispatches_message() {
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (tx, watch_rx, token, sessions, stats) = loop_fixture();

        let session_id = Uuid::new_v4();
        let msg = ProxiedMessage::new(Payload::Data(b"hello".to_vec()), session_id, 1);
        let incoming = ReconstructedMessage {
            message: bincode::serialize(&msg).unwrap(),
            sender_tag: Some(AnonymousSenderTag::from_bytes([7u8; 16])),
        };

        let mut spawned = Vec::new();
        let result = TcpProxyServer::message_loop(
            tokio_stream::iter(vec![incoming]),
            &mut shutdown_rx,
            &token,
            &tx,
            &sessions,
            &stats,
            &Watchdog::new(std::time::Duration::from_secs(300)),
            &CancellationToken::new(),
            Some("127.0.0.1:9137"),
            512,
            |addr: String, id: Uuid| spawned.push((addr, id)),
        )
        .await;

        // The stream ends after the one message, so the loop still exits fatally,
        // but the message must have been processed first.
        assert!(result.is_err());
        assert_eq!(spawned, vec![("127.0.0.1:9137".to_string(), session_id)]);
        assert!(sessions.contains_key(&session_id));
        assert_eq!(stats.total_sessions.load(Ordering::Relaxed), 1);
        assert!(
            watch_rx.borrow().is_some(),
            "message must be dispatched to session handlers"
        );
    }

    #[tokio::test]
    async fn message_loop_feeds_watchdog_pings_to_watchdog_not_sessions() {
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (tx, watch_rx, token, sessions, stats) = loop_fixture();
        let watchdog = Watchdog::new(std::time::Duration::from_secs(300));

        let ping = watchdog.make_ping();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let age_before = watchdog.last_pong_age();
        let incoming = ReconstructedMessage {
            message: ping,
            sender_tag: None,
        };

        let result = TcpProxyServer::message_loop(
            tokio_stream::iter(vec![incoming]),
            &mut shutdown_rx,
            &token,
            &tx,
            &sessions,
            &stats,
            &watchdog,
            &CancellationToken::new(),
            Some("127.0.0.1:9"),
            512,
            |_addr: String, _id: Uuid| panic!("a watchdog ping must never open a session"),
        )
        .await;

        assert!(
            result.is_err(),
            "stream ended, loop exits fatally as before"
        );
        assert!(
            watchdog.last_pong_age() < age_before,
            "the returned ping must be credited as a pong"
        );
        assert!(sessions.is_empty());
        assert_eq!(stats.total_sessions.load(Ordering::Relaxed), 0);
        assert!(
            watch_rx.borrow().is_none(),
            "nothing dispatched to session handlers"
        );
    }

    #[tokio::test]
    async fn message_loop_errors_when_watchdog_declares_death() {
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (tx, _watch_rx, token, sessions, stats) = loop_fixture();
        let dead = CancellationToken::new();
        dead.cancel();

        let result = TcpProxyServer::message_loop(
            tokio_stream::pending::<ReconstructedMessage>(),
            &mut shutdown_rx,
            &token,
            &tx,
            &sessions,
            &stats,
            &Watchdog::new(std::time::Duration::from_secs(300)),
            &dead,
            Some("127.0.0.1:9"),
            512,
            |_addr: String, _id: Uuid| panic!("no session must spawn"),
        )
        .await;

        assert!(
            result.is_err(),
            "a dead mixnet client detected by the watchdog must be a fatal error"
        );
        assert!(
            token.is_cancelled(),
            "existing sessions must be cancelled when the watchdog fires"
        );
    }

    /// Reply stores the SDK rejected at startup and archived, by file name.
    fn archived_reply_stores(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".corrupted"))
            .collect()
    }

    /// Talks to the live mixnet, like `test_run_with_shutdown`.
    #[tokio::test]
    async fn reply_store_is_reusable_after_shutdown() -> Result<()> {
        let config_dir = TempDir::new()?;
        let dir = config_dir.path().to_str().unwrap();

        let server =
            TcpProxyServer::new(dir, None, None, 3600, 512, WatchdogConfig::default()).await?;
        server.disconnect().await;

        // The gateway authenticates with a second-resolution timestamp and
        // rejects a login that is not strictly newer than the previous one,
        // so an immediate reconnect fails for reasons unrelated to this test.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Second start in the same state directory, as after a service restart.
        let server =
            TcpProxyServer::new(dir, None, None, 3600, 512, WatchdogConfig::default()).await?;
        server.disconnect().await;

        let archived = archived_reply_stores(config_dir.path());
        assert!(
            archived.is_empty(),
            "the reply store written by the first run was rejected on restart: {archived:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_run_with_shutdown() -> Result<()> {
        let config_dir = TempDir::new()?;
        let mut server = TcpProxyServer::new(
            config_dir.path().to_str().unwrap(),
            None,
            None,
            3600,
            512,
            WatchdogConfig::default(),
        )
        .await?;

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
