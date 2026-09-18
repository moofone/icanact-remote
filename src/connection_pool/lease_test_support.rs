use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use tokio::sync::Notify;

/// Connection-local, test-only gate for proving that a lease wake selected the
/// writer's lease branch rather than relying on socket readiness or a timer.
/// It is deliberately not available through the `test-helpers` feature.
pub(crate) struct LeaseWakeGate {
    armed: AtomicBool,
    observed: Notify,
    release: Notify,
}

impl LeaseWakeGate {
    pub(crate) fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            observed: Notify::new(),
            release: Notify::new(),
        }
    }

    pub(crate) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    pub(crate) async fn wait_observed(&self) {
        self.observed.notified().await;
    }

    pub(crate) fn release(&self) {
        self.armed.store(false, Ordering::Release);
        self.release.notify_one();
    }

    pub(crate) async fn hold_lease_branch(&self) {
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        self.observed.notify_one();
        self.release.notified().await;
    }
}

#[derive(Default)]
struct PublicationState {
    armed: bool,
    publisher_entered: bool,
    close_observed: bool,
    released: bool,
}

/// Scoped synchronous gate for the publication/close linearization test.
/// Unlike a simultaneous-start barrier, each side acknowledges the actual
/// state transition it observed before the test releases the other side.
pub(crate) struct PublicationGate {
    state: Mutex<PublicationState>,
    changed: Condvar,
}

impl PublicationGate {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(PublicationState::default()),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn arm(&self) {
        let mut state = self.state.lock().expect("publication gate lock");
        state.armed = true;
    }

    pub(crate) fn wait_publisher_entered(&self) {
        let mut state = self.state.lock().expect("publication gate lock");
        while !state.publisher_entered {
            state = self.changed.wait(state).expect("publication gate wait");
        }
    }

    pub(crate) fn wait_close_observed(&self) {
        let mut state = self.state.lock().expect("publication gate lock");
        while !state.close_observed {
            state = self.changed.wait(state).expect("publication gate wait");
        }
    }

    pub(crate) fn publisher_transition(&self) {
        let mut state = self.state.lock().expect("publication gate lock");
        if !state.armed {
            return;
        }
        state.publisher_entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("publication gate wait");
        }
    }

    pub(crate) fn close_transition(&self) {
        let mut state = self.state.lock().expect("publication gate lock");
        if state.armed {
            state.close_observed = true;
            self.changed.notify_all();
        }
    }

    pub(crate) fn release(&self) {
        let mut state = self.state.lock().expect("publication gate lock");
        state.released = true;
        state.armed = false;
        self.changed.notify_all();
    }
}
