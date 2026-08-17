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

/// Hook type fired around every `RoutedPubSub::note_interest` background
/// registry dispatch (register/unregister), keyed by `(topic_key,
/// present)`. Tests install this to pause a dispatch mid-flight — right
/// before the registry call it is about to make — so they can force a
/// specific interleaving between two temporally-overlapping interest
/// transitions deterministically instead of relying on incidental
/// scheduler timing.
///
#[cfg(any(test, feature = "test-helpers"))]
pub type InterestDispatchHook = std::sync::Arc<
    dyn Fn(u64, bool) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

#[cfg(any(test, feature = "test-helpers"))]
static PUBSUB_INTEREST_DISPATCH_HOOK_INSTALLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(any(test, feature = "test-helpers"))]
static PUBSUB_INTEREST_DISPATCH_HOOK: OnceLock<Mutex<Option<InterestDispatchHook>>> =
    OnceLock::new();

#[cfg(any(test, feature = "test-helpers"))]
fn interest_dispatch_hook_slot() -> &'static Mutex<Option<InterestDispatchHook>> {
    PUBSUB_INTEREST_DISPATCH_HOOK.get_or_init(|| Mutex::new(None))
}

/// Installs (replacing any previous) process-wide interest-dispatch hook.
/// Unlike the subscriber RMW hook this is re-installable, since tests that
/// exercise this path run serially against a fresh, per-test topic key.
#[cfg(any(test, feature = "test-helpers"))]
pub fn install_pubsub_interest_dispatch_hook(hook: InterestDispatchHook) {
    *interest_dispatch_hook_slot()
        .lock()
        .expect("interest dispatch hook mutex poisoned") = Some(hook);
    PUBSUB_INTEREST_DISPATCH_HOOK_INSTALLED.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(any(test, feature = "test-helpers"))]
pub fn clear_pubsub_interest_dispatch_hook() {
    *interest_dispatch_hook_slot()
        .lock()
        .expect("interest dispatch hook mutex poisoned") = None;
    PUBSUB_INTEREST_DISPATCH_HOOK_INSTALLED.store(false, std::sync::atomic::Ordering::Release);
}

/// Process-wide lock serializing every use of the global
/// [`PUBSUB_INTEREST_DISPATCH_HOOK`]. Mirrors
/// `lifecycle::RECORDER_INSTALL_LOCK`: the hook is shared, mutable, global
/// state fired from every `RoutedPubSub` interest-dispatch loop in the
/// process, and the default parallel test harness runs many
/// `#[tokio::test]` functions concurrently in that one process. Without a
/// single shared lock, one test's own interest-dispatch calls can be routed
/// through a *different*, concurrently running test's hook closure and pause
/// on a `Notify` that only the other test ever signals — the closure fires
/// for `(topic_key, present)` regardless of which test's topic it was meant
/// for, since the hook has no notion of test identity.
#[cfg(any(test, feature = "test-helpers"))]
static PUBSUB_INTEREST_DISPATCH_HOOK_INSTALL_LOCK: Mutex<()> = Mutex::new(());

/// RAII guard serializing access to the process-wide pubsub
/// interest-dispatch hook. Acquires
/// `PUBSUB_INTEREST_DISPATCH_HOOK_INSTALL_LOCK` for its entire lifetime and
/// clears the hook on drop, so a test that installs a hook and a test that
/// merely requires no hook be active can never run concurrently and observe
/// or interfere with each other's dispatches.
///
/// This is the only sanctioned way to touch the hook from a test: acquire
/// this guard first (via [`Self::install`] or [`Self::exclusive`]), then —
/// while still holding it — call [`install_pubsub_interest_dispatch_hook`]
/// directly if the hook itself needs to change partway through the test.
#[cfg(any(test, feature = "test-helpers"))]
#[must_use = "the hook is cleared when this guard is dropped"]
pub struct PubsubInterestDispatchHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl PubsubInterestDispatchHookGuard {
    /// Acquires exclusive access and installs `hook`.
    pub fn install(hook: InterestDispatchHook) -> Self {
        let guard = Self::exclusive();
        install_pubsub_interest_dispatch_hook(hook);
        guard
    }

    /// Acquires exclusive access without installing a hook, clearing any
    /// leftover one — for a test that requires the hook be absent for its
    /// own duration (mutual exclusion against a concurrently running
    /// [`Self::install`]).
    pub fn exclusive() -> Self {
        let lock = PUBSUB_INTEREST_DISPATCH_HOOK_INSTALL_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clear_pubsub_interest_dispatch_hook();
        Self { _lock: lock }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
impl Drop for PubsubInterestDispatchHookGuard {
    fn drop(&mut self) {
        clear_pubsub_interest_dispatch_hook();
    }
}

/// Fires the installed interest-dispatch hook, if any, awaiting the future
/// it returns. Called by `RoutedPubSub`'s interest-dispatch loop
/// immediately before issuing the register/unregister registry call.
#[inline]
#[cfg(any(test, feature = "test-helpers"))]
pub async fn fire_pubsub_interest_dispatch_hook(topic_key: u64, present: bool) {
    if !PUBSUB_INTEREST_DISPATCH_HOOK_INSTALLED.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let hook = interest_dispatch_hook_slot()
        .lock()
        .expect("interest dispatch hook mutex poisoned")
        .clone();
    if let Some(hook) = hook {
        hook(topic_key, present).await;
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub struct SilentPooledConnection {
    _peer: tokio::io::DuplexStream,
}

/// Installs a connected pooled stream whose peer endpoint remains open but
/// never reads or writes. This models a paused process or UDP black hole
/// without a live registry whose legitimate background traffic can race the
/// liveness assertion.
#[cfg(any(test, feature = "test-helpers"))]
pub fn install_silent_pooled_connection(
    registry: &crate::GossipRegistryHandle,
    peer_id: crate::PeerId,
    peer_addr: std::net::SocketAddr,
) -> SilentPooledConnection {
    use crate::connection_pool::{
        BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
        LockFreeStreamHandle,
    };
    use std::sync::Arc;

    let (local, peer) = tokio::io::duplex(64 * 1024);
    let (stream_handle, writer, reader) = LockFreeStreamHandle::new(
        local,
        peer_addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let mut connection = LockFreeConnection::new(peer_addr, ConnectionDirection::Outbound);
    connection.set_state(ConnectionState::Connected);
    connection.stream_handle = Some(Arc::new(stream_handle));
    connection.embedded_peer_id = Some(peer_id.clone());
    connection.task_tracker.set_writer(writer.abort_handle());
    if let Some(reader) = reader {
        connection.task_tracker.set_reader(reader.abort_handle());
    }
    registry.registry.connection_pool.add_connection_by_peer_id(
        peer_id,
        peer_addr,
        Arc::new(connection),
    );

    SilentPooledConnection { _peer: peer }
}

#[cfg(any(test, feature = "test-helpers"))]
pub fn tie_break_cooldown_active(
    registry: &crate::GossipRegistryHandle,
    peer_id: &crate::PeerId,
) -> bool {
    registry.registry.tie_break_cooldown_active(peer_id)
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
