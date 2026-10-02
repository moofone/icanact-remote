//! A node that shuts down explicitly closes its TLS sessions with
//! `close_notify`; the surviving peer treats that as an expected close (no
//! "read error"/"unexpected exit" warnings) while still retiring the session.
//! A peer that vanishes without the alert (owner drop = forced abort) is still
//! reported as a diagnosable read failure.
mod common;

use std::fmt::Write as _;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, registry::LookupSpan};

use common::{create_ordered_tls_pair, seed_peer, wait_for_condition, wait_for_pair_connection};

const READ_ERROR: &str = "IO task read error";
const CURRENT_EXIT: &str = "transport_io_task_exit_current_connection";
const WRITER_EXIT: &str = "Background writer task EXITED";

fn captured() -> &'static Mutex<Vec<String>> {
    static LOG: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::new()))
}

struct Message(String);
impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        }
    }
}

/// Records WARN-and-above messages emitted by the library.
struct WarnCapture;
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for WarnCapture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() > Level::WARN {
            return;
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        if let Ok(mut log) = captured().lock() {
            log.push(message.0);
        }
    }
}

fn warned(needle: &str) -> bool {
    captured()
        .lock()
        .map(|log| log.iter().any(|m| m.contains(needle)))
        .unwrap_or(false)
}

fn run(test: impl std::future::Future<Output = ()> + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(8 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(test);
        })
        .expect("spawn")
        .join()
        .expect("test panicked");
}

#[test]
fn explicit_shutdown_is_an_orderly_close_but_owner_drop_stays_a_diagnosable_failure() {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(WarnCapture))
        .expect("single global subscriber for this test binary");

    run(async {
        // --- explicit shutdown: close_notify, no failure warnings, prompt retirement ---
        let (high, low) = create_ordered_tls_pair("graceful_tls_a", "graceful_tls_b")
            .await
            .expect("pair");
        seed_peer(&high, &low).await.expect("seed");
        assert!(
            wait_for_pair_connection(&high, &low, Duration::from_secs(10)).await,
            "pair must connect"
        );
        assert!(
            wait_for_condition(Duration::from_secs(10), || async {
                high.registry
                    .has_connection_to_peer(&low.registry.peer_id)
                    .await
                    && low
                        .registry
                        .has_connection_to_peer(&high.registry.peer_id)
                        .await
            })
            .await,
            "both sides must hold the session"
        );
        captured().lock().unwrap().clear();

        let low_peer_id = low.registry.peer_id.clone();
        low.shutdown().await;

        let ok = wait_for_condition(Duration::from_secs(5), || async {
            !high.registry.has_connection_to_peer(&low_peer_id).await
        })
        .await;
        assert!(
            ok,
            "the surviving node must retire the session promptly after an orderly close"
        );
        // Let the IO-task exit path finish logging before asserting absence.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !warned(READ_ERROR) && !warned(CURRENT_EXIT) && !warned(WRITER_EXIT),
            "an orderly TLS close must not be logged as a failure: {:?}",
            captured().lock().unwrap()
        );
        high.shutdown().await;

        // --- owner drop: forced abort, peer sees truncation and says so ---
        let (high, low) = create_ordered_tls_pair("graceful_tls_c", "graceful_tls_d")
            .await
            .expect("pair");
        seed_peer(&high, &low).await.expect("seed");
        assert!(wait_for_pair_connection(&high, &low, Duration::from_secs(10)).await);
        assert!(
            wait_for_condition(Duration::from_secs(10), || async {
                high.registry
                    .has_connection_to_peer(&low.registry.peer_id)
                    .await
                    && low
                        .registry
                        .has_connection_to_peer(&high.registry.peer_id)
                        .await
            })
            .await,
            "both sides must hold the session"
        );
        captured().lock().unwrap().clear();

        drop(low);

        let flagged = {
            let start = std::time::Instant::now();
            let mut seen = false;
            while start.elapsed() < Duration::from_secs(5) {
                if warned(READ_ERROR) {
                    seen = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            seen
        };
        assert!(
            flagged,
            "a peer vanishing without close_notify must stay a diagnosable read failure: {:?}",
            captured().lock().unwrap()
        );
        high.shutdown().await;
    });
}
