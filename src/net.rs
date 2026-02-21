#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tracing::warn;

use crate::GossipConfig;

#[cfg(test)]
static KEEPALIVE_APPLY_CALLS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn test_reset_keepalive_apply_calls() {
    KEEPALIVE_APPLY_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn test_keepalive_apply_calls() -> usize {
    KEEPALIVE_APPLY_CALLS.load(Ordering::Relaxed)
}

pub(crate) fn apply_tcp_keepalive(stream: &TcpStream, config: &GossipConfig) {
    #[cfg(test)]
    KEEPALIVE_APPLY_CALLS.fetch_add(1, Ordering::Relaxed);

    let Some(keepalive) = config.tcp_keepalive.as_ref() else {
        return;
    };

    let sock = SockRef::from(stream);
    let mut ka = TcpKeepalive::new()
        .with_time(keepalive.idle)
        .with_interval(keepalive.interval);

    // Keepalive retries is platform-specific; apply it where supported.
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "cygwin",
    ))]
    if let Some(retries) = keepalive.retries {
        ka = ka.with_retries(retries);
    }

    if let Err(e) = sock.set_tcp_keepalive(&ka) {
        warn!(error = %e, "failed to set TCP keepalive");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TcpKeepaliveConfig;
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[test]
    fn default_config_enables_tcp_keepalive() {
        let cfg = GossipConfig::default();
        assert!(cfg.tcp_keepalive.is_some());
    }

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        (client, server)
    }

    #[tokio::test]
    async fn apply_sets_so_keepalive_when_enabled() {
        let (client, _server) = connected_pair().await;

        let cfg = GossipConfig {
            tcp_keepalive: Some(TcpKeepaliveConfig {
                idle: Duration::from_secs(4),
                interval: Duration::from_secs(2),
                retries: Some(3),
            }),
            ..GossipConfig::default()
        };

        // Ensure we start from "off" so the test is meaningful.
        let sock = SockRef::from(&client);
        sock.set_keepalive(false).unwrap();
        assert!(!sock.keepalive().unwrap());

        apply_tcp_keepalive(&client, &cfg);

        assert!(sock.keepalive().unwrap());
    }

    #[tokio::test]
    async fn apply_is_noop_when_disabled() {
        let (client, _server) = connected_pair().await;
        let mut cfg = GossipConfig::default();
        cfg.tcp_keepalive = None;

        let sock = SockRef::from(&client);
        sock.set_keepalive(false).unwrap();
        assert!(!sock.keepalive().unwrap());

        apply_tcp_keepalive(&client, &cfg);

        assert!(!sock.keepalive().unwrap());
    }

    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "cygwin",
    ))]
    #[tokio::test]
    async fn apply_sets_time_interval_and_retries_when_supported() {
        let (client, _server) = connected_pair().await;

        let cfg = GossipConfig {
            tcp_keepalive: Some(TcpKeepaliveConfig {
                idle: Duration::from_secs(7),
                interval: Duration::from_secs(9),
                retries: Some(2),
            }),
            ..GossipConfig::default()
        };

        apply_tcp_keepalive(&client, &cfg);

        let sock = SockRef::from(&client);
        // These getters and options are OS-dependent. We expect whole-second precision
        // and allow for platforms that clamp values.
        let time = sock.keepalive_time().unwrap();
        assert!(time >= Duration::from_secs(7), "keepalive_time={time:?}");

        let interval = sock.keepalive_interval().unwrap();
        assert!(
            interval >= Duration::from_secs(9),
            "keepalive_interval={interval:?}"
        );

        // Not supported on all targets; this test module is gated to targets
        // where socket2 exposes the getter.
        assert_eq!(sock.keepalive_retries().unwrap(), 2);
    }
}
