use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use tokio::sync::Notify;

/// Connection-local, test-only gate for proving that a lease wake selected the
/// writer's lease branch rather than relying on socket readiness or a timer.
/// It is deliberately not available through the `test-helpers` feature.
pub(crate) struct LeaseWakeGate {
    armed: AtomicBool,
    observed: Notify,
    release: Notify,
    registered: Notify,
    selection_armed: AtomicBool,
    selected_count: AtomicUsize,
    selected: Notify,
}

impl LeaseWakeGate {
    pub(crate) fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            observed: Notify::new(),
            release: Notify::new(),
            registered: Notify::new(),
            selection_armed: AtomicBool::new(false),
            selected_count: AtomicUsize::new(0),
            selected: Notify::new(),
        }
    }

    pub(crate) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    pub(crate) async fn wait_registered(&self) {
        self.registered.notified().await;
    }

    pub(crate) fn parking_select_registered(&self) {
        if self.armed.load(Ordering::Acquire) {
            self.registered.notify_one();
        }
    }

    pub(crate) fn maintenance_duration(
        &self,
        duration: std::time::Duration,
    ) -> std::time::Duration {
        if self.armed.load(Ordering::Acquire) {
            std::time::Duration::from_secs(3600)
        } else {
            duration
        }
    }

    pub(crate) fn arm_selection(&self) {
        self.selected_count.store(0, Ordering::Release);
        self.selection_armed.store(true, Ordering::Release);
    }

    pub(crate) fn observe_selection(&self) {
        if self.selection_armed.load(Ordering::Acquire) {
            self.selected_count.fetch_add(1, Ordering::AcqRel);
            self.selected.notify_waiters();
        }
    }

    pub(crate) async fn wait_selected(&self, count: usize) {
        loop {
            if self.selected_count.load(Ordering::Acquire) >= count {
                return;
            }
            self.selected.notified().await;
        }
    }

    pub(crate) fn disarm_selection(&self) {
        self.selection_armed.store(false, Ordering::Release);
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
