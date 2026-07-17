use bytes::Bytes;
use std::sync::{Mutex, OnceLock};
use tokio::sync::Notify;
use tokio::time::{Duration, sleep};

struct RawCapture {
    messages: Mutex<Vec<Bytes>>,
    notify: Notify,
}

fn raw_capture() -> &'static RawCapture {
    static CAPTURE: OnceLock<RawCapture> = OnceLock::new();
    CAPTURE.get_or_init(|| RawCapture {
        messages: Mutex::new(Vec::new()),
        notify: Notify::new(),
    })
}

pub fn record_raw_payload(payload: Bytes) {
    let capture = raw_capture();
    {
        let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
        guard.push(payload);
    }
    capture.notify.notify_waiters();
}

pub fn drain_raw_payloads() -> Vec<Bytes> {
    let capture = raw_capture();
    let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
    guard.drain(..).collect()
}

/// Fast-path check for [`fire_pubsub_subscriber_rmw_hook`]: uninstalled cost
/// is a single relaxed atomic load.
#[cfg(any(test, feature = "test-helpers"))]
static PUBSUB_SUBSCRIBER_RMW_HOOK_INSTALLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Test-only hook fired inside every routed-pubsub subscriber-map
/// read-modify-write window (between `load_full` and `store` in the
/// `RoutedPubSub` subscribe/remove writers). Tests install it to widen the
/// window deterministically and exercise writer/writer races.
#[cfg(any(test, feature = "test-helpers"))]
static PUBSUB_SUBSCRIBER_RMW_HOOK: OnceLock<std::sync::Arc<dyn Fn() + Send + Sync>> =
    OnceLock::new();

/// Installs the process-wide subscriber-map RMW hook. One-shot: returns
/// `false` (and leaves the existing hook in place) if a hook was already
/// installed. Installed hooks must self-filter (e.g. by thread id) because
/// they fire for every subscriber-map writer in the process.
#[cfg(any(test, feature = "test-helpers"))]
pub fn install_pubsub_subscriber_rmw_hook(hook: std::sync::Arc<dyn Fn() + Send + Sync>) -> bool {
    let installed = PUBSUB_SUBSCRIBER_RMW_HOOK.set(hook).is_ok();
    if installed {
        PUBSUB_SUBSCRIBER_RMW_HOOK_INSTALLED.store(true, std::sync::atomic::Ordering::Release);
    }
    installed
}

/// Fires the installed subscriber-map RMW hook, if any. Called by the
/// `RoutedPubSub` writers between `load_full` and `store`.
#[cfg(any(test, feature = "test-helpers"))]
#[inline]
pub fn fire_pubsub_subscriber_rmw_hook() {
    if !PUBSUB_SUBSCRIBER_RMW_HOOK_INSTALLED.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    if let Some(hook) = PUBSUB_SUBSCRIBER_RMW_HOOK.get() {
        hook();
    }
}

pub async fn wait_for_raw_payload(timeout: Duration) -> Option<Bytes> {
    let capture = raw_capture();
    {
        let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
        if let Some(payload) = guard.pop() {
            return Some(payload);
        }
    }

    tokio::select! {
        _ = capture.notify.notified() => {
            let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
            guard.pop()
        }
        _ = sleep(timeout) => None,
    }
}
