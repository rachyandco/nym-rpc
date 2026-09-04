//! Mixnet liveness watchdog.
//!
//! The embedded Nym client can die in ways the message stream never reports:
//! in production the send side lost its gateway channel while the receive
//! stream stayed open, so the server looped "healthy" for hours while every
//! outbound packet was dropped. The only signal that proves both directions
//! work is a round trip: send a tagged ping to our own Nym address and wait
//! for it to come back through the mixnet.

use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Prefix that marks a mixnet payload as a watchdog ping. Session traffic is
/// bincode-encoded `ProxiedMessage`s and never starts with these bytes.
pub const WATCHDOG_MAGIC: &[u8] = b"nym-rpc-watchdog/v1:";

/// Watchdog tuning as taken from the CLI. `timeout` must exceed `interval`,
/// otherwise the very first liveness check after startup would fail before a
/// ping had any chance to return.
#[derive(Debug, Clone, Copy)]
pub struct WatchdogConfig {
    pub interval: Duration,
    pub timeout: Duration,
}

impl WatchdogConfig {
    pub const DEFAULT_INTERVAL_SECS: u64 = 60;
    pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

    pub fn new(interval_secs: u64, timeout_secs: u64) -> Result<Self> {
        anyhow::ensure!(
            interval_secs > 0,
            "watchdog interval must be at least 1 second"
        );
        anyhow::ensure!(
            timeout_secs > interval_secs,
            "watchdog timeout ({timeout_secs}s) must be longer than the ping interval ({interval_secs}s)"
        );
        Ok(Self {
            interval: Duration::from_secs(interval_secs),
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self::new(Self::DEFAULT_INTERVAL_SECS, Self::DEFAULT_TIMEOUT_SECS)
            .expect("default watchdog config is valid")
    }
}

/// Round-trip liveness tracker shared between the watchdog task, the inbound
/// message loop (which feeds it pongs) and the HTTP health endpoint.
pub struct Watchdog {
    timeout: Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    last_pong: Instant,
    /// Nonces of pings sent but not yet seen again, with the time they were
    /// issued so stale ones can be pruned.
    outstanding: HashMap<u64, Instant>,
}

impl Watchdog {
    /// `timeout` is how long we tolerate without a completed round trip before
    /// `is_alive` turns false. The clock starts at construction, which gives a
    /// freshly started server one full timeout of grace.
    pub fn new(timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            timeout,
            inner: Mutex::new(Inner {
                last_pong: Instant::now(),
                outstanding: HashMap::new(),
            }),
        })
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Time since the last ping made it back through the mixnet (or since
    /// startup if none has yet).
    pub fn last_pong_age(&self) -> Duration {
        self.inner.lock().unwrap().last_pong.elapsed()
    }

    pub fn is_alive(&self) -> bool {
        self.last_pong_age() <= self.timeout
    }

    /// Builds a ping payload with a fresh random nonce and remembers the nonce
    /// so the matching pong can be verified. Nonces older than the timeout are
    /// dropped here so the set cannot grow while the mixnet is dead.
    pub fn make_ping(&self) -> Vec<u8> {
        let nonce: u64 = rand::random();
        let mut inner = self.inner.lock().unwrap();
        let timeout = self.timeout;
        inner
            .outstanding
            .retain(|_, sent| sent.elapsed() <= timeout);
        inner.outstanding.insert(nonce, Instant::now());

        let mut payload = WATCHDOG_MAGIC.to_vec();
        payload.extend_from_slice(&nonce.to_be_bytes());
        payload
    }

    /// Inspects an inbound mixnet payload. Returns `true` when it is watchdog
    /// traffic (and must not be handed to session logic), regardless of
    /// whether it was a valid pong. Only a pong whose nonce we issued and have
    /// not seen before refreshes liveness — anyone can send us tagged bytes,
    /// but they cannot guess the nonce.
    pub fn observe(&self, payload: &[u8]) -> bool {
        let Some(rest) = payload.strip_prefix(WATCHDOG_MAGIC) else {
            return false;
        };
        let Ok(nonce_bytes) = <[u8; 8]>::try_from(rest) else {
            warn!("watchdog: tagged message with malformed nonce, ignoring");
            return true;
        };
        let nonce = u64::from_be_bytes(nonce_bytes);

        let mut inner = self.inner.lock().unwrap();
        if inner.outstanding.remove(&nonce).is_some() {
            inner.last_pong = Instant::now();
            info!("watchdog: pong received, mixnet round trip OK");
        } else {
            warn!("watchdog: pong with unknown or replayed nonce, ignoring");
        }
        true
    }
}

/// Sends a ping every `interval` via `send` and, after each one, checks
/// whether a round trip has completed within the watchdog's timeout. When it
/// has not, `dead` is cancelled: the caller must treat that as fatal and exit
/// so the supervisor restarts the process. Returns when `shutdown` fires.
///
/// A failed or hung `send` is logged but not fatal on its own — the timeout
/// is the single source of truth, which keeps a transient hiccup from
/// restarting a server that is otherwise fine.
pub async fn run_watchdog<F, Fut>(
    watchdog: Arc<Watchdog>,
    interval: Duration,
    mut send: F,
    dead: CancellationToken,
    shutdown: CancellationToken,
) where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {
                let ping = watchdog.make_ping();
                // Bound the send: a dead client whose input queue never drains
                // must not park us here and starve the liveness check below.
                match tokio::time::timeout(interval, send(ping)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => error!("watchdog: failed to send self-ping: {e}"),
                    Err(_) => error!("watchdog: self-ping send did not complete within {interval:?}"),
                }
                if !watchdog.is_alive() {
                    error!(
                        "watchdog: no self-ping returned through the mixnet for {:?} \
                         (timeout {:?}). The embedded mixnet client is dead; shutting \
                         down so the supervisor can restart us.",
                        watchdog.last_pong_age(),
                        watchdog.timeout()
                    );
                    dead.cancel();
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn config_rejects_timeout_not_longer_than_interval() {
        assert!(WatchdogConfig::new(60, 60).is_err());
        assert!(WatchdogConfig::new(60, 30).is_err());
        assert!(WatchdogConfig::new(0, 30).is_err());
        assert!(WatchdogConfig::new(60, 61).is_ok());
    }

    #[test]
    fn observe_records_pong_for_a_ping_it_issued() {
        let wd = Watchdog::new(Duration::from_secs(300));
        let ping = wd.make_ping();
        std::thread::sleep(Duration::from_millis(20));
        let age_before = wd.last_pong_age();

        assert!(wd.observe(&ping), "our own ping must be recognised");
        assert!(
            wd.last_pong_age() < age_before,
            "a valid pong must refresh the last-pong timestamp"
        );
    }

    #[test]
    fn observe_swallows_but_does_not_credit_unknown_nonce() {
        let wd = Watchdog::new(Duration::from_secs(300));
        let mut forged = WATCHDOG_MAGIC.to_vec();
        forged.extend_from_slice(&42u64.to_be_bytes());
        std::thread::sleep(Duration::from_millis(20));
        let age_before = wd.last_pong_age();

        assert!(
            wd.observe(&forged),
            "watchdog-tagged bytes never reach sessions"
        );
        assert!(
            wd.last_pong_age() >= age_before,
            "a forged pong must not count as liveness"
        );
    }

    #[test]
    fn observe_ignores_regular_traffic() {
        let wd = Watchdog::new(Duration::from_secs(300));
        assert!(!wd.observe(b"some bincode session payload"));
        assert!(!wd.observe(&[]));
    }

    #[test]
    fn a_nonce_is_only_good_once() {
        let wd = Watchdog::new(Duration::from_secs(300));
        let ping = wd.make_ping();
        assert!(wd.observe(&ping));
        std::thread::sleep(Duration::from_millis(20));
        let age_before = wd.last_pong_age();
        wd.observe(&ping);
        assert!(
            wd.last_pong_age() >= age_before,
            "replayed pong must not refresh"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn is_alive_flips_after_timeout_without_pongs() {
        let wd = Watchdog::new(Duration::from_secs(300));
        assert!(wd.is_alive());
        tokio::time::advance(Duration::from_secs(301)).await;
        assert!(!wd.is_alive());
    }

    #[tokio::test(start_paused = true)]
    async fn run_watchdog_declares_death_when_pings_never_return() {
        let wd = Watchdog::new(Duration::from_secs(300));
        let dead = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sent_in = Arc::clone(&sent);

        let task = tokio::spawn(run_watchdog(
            Arc::clone(&wd),
            Duration::from_secs(60),
            move |_ping: Vec<u8>| {
                sent_in.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Ok(()) }
            },
            dead.clone(),
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(600), dead.cancelled())
            .await
            .expect("watchdog must declare death within the timeout window");
        task.await.unwrap();
        assert!(
            sent.load(std::sync::atomic::Ordering::SeqCst) >= 5,
            "pings must keep being sent while waiting"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn run_watchdog_declares_death_even_if_send_hangs_forever() {
        let wd = Watchdog::new(Duration::from_secs(300));
        let dead = CancellationToken::new();
        let shutdown = CancellationToken::new();

        let task = tokio::spawn(run_watchdog(
            Arc::clone(&wd),
            Duration::from_secs(60),
            move |_ping: Vec<u8>| async {
                // A dead SDK client whose input channel never drains.
                std::future::pending::<()>().await;
                Ok(())
            },
            dead.clone(),
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(900), dead.cancelled())
            .await
            .expect("a hanging send must not stall the liveness check");
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn run_watchdog_stays_quiet_while_pings_come_back() {
        let wd = Watchdog::new(Duration::from_secs(300));
        let dead = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let wd_in = Arc::clone(&wd);

        let task = tokio::spawn(run_watchdog(
            Arc::clone(&wd),
            Duration::from_secs(60),
            move |ping: Vec<u8>| {
                let wd = Arc::clone(&wd_in);
                async move {
                    wd.observe(&ping);
                    Ok(())
                }
            },
            dead.clone(),
            shutdown.clone(),
        ));

        // Step the paused clock the way real time moves: one interval at a time.
        for _ in 0..60 {
            tokio::time::advance(Duration::from_secs(60)).await;
        }
        assert!(
            !dead.is_cancelled(),
            "healthy round trips must never trip the watchdog"
        );
        assert!(wd.is_alive());

        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn run_watchdog_keeps_going_when_a_send_fails() {
        let wd = Watchdog::new(Duration::from_secs(300));
        let dead = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let wd_in = Arc::clone(&wd);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_in = Arc::clone(&calls);

        let task = tokio::spawn(run_watchdog(
            Arc::clone(&wd),
            Duration::from_secs(60),
            move |ping: Vec<u8>| {
                let wd = Arc::clone(&wd_in);
                let n = calls_in.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if n == 0 {
                        anyhow::bail!("gateway hiccup");
                    }
                    wd.observe(&ping);
                    Ok(())
                }
            },
            dead.clone(),
            shutdown.clone(),
        ));

        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(60)).await;
        }
        assert!(
            !dead.is_cancelled(),
            "one failed send is not death; the timeout decides"
        );
        shutdown.cancel();
        task.await.unwrap();
    }
}
