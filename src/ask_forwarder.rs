use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;

use crate::{AskResponder, GossipError, RemoteConnection, Result};

struct ForwardTask {
    destination: RemoteConnection,
    actor_id: u64,
    type_hash: u32,
    payload: Bytes,
    responder: AskResponder,
    timeout: Option<Duration>,
    use_combined_timeout: bool,
    timeout_reply: Option<Bytes>,
    error_reply: Option<Bytes>,
}

struct AskForwarderInner {
    workers: Vec<mpsc::Sender<ForwardTask>>,
    next_worker: AtomicUsize,
}

const MAX_INFLIGHT_PER_WORKER: usize = 16;

#[derive(Clone)]
pub struct AskForwarder {
    inner: Arc<AskForwarderInner>,
}

pub trait AskForwardObserver: Send + Sync {
    fn record_success(&self);
    fn record_error(&self);
}

impl AskForwarder {
    pub fn new(workers: usize, capacity: usize) -> Self {
        Self::new_with_observer(workers, capacity, None)
    }

    pub fn new_with_observer(
        workers: usize,
        capacity: usize,
        completion_observer: Option<Arc<dyn AskForwardObserver>>,
    ) -> Self {
        let workers = workers.max(1);
        let capacity = capacity.max(128);
        let max_inflight = capacity.clamp(1, MAX_INFLIGHT_PER_WORKER);

        let mut worker_senders = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, mut rx) = mpsc::channel::<ForwardTask>(capacity);
            let worker_observer = completion_observer.clone();
            let handle = tokio::spawn(async move {
                let mut inflight = FuturesUnordered::new();
                let mut rx_closed = false;

                loop {
                    while inflight.len() < max_inflight {
                        match rx.try_recv() {
                            Ok(task) => inflight.push(Box::pin(run_forward_task_isolated(task))),
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                rx_closed = true;
                                break;
                            }
                        }
                    }

                    if rx_closed && inflight.is_empty() {
                        break;
                    }

                    tokio::select! {
                        maybe_task = rx.recv(), if can_receive_more(rx_closed, inflight.len(), max_inflight) => {
                            match maybe_task {
                                Some(task) => inflight.push(Box::pin(run_forward_task_isolated(task))),
                                None => rx_closed = true,
                            }
                        }
                        Some(completed) = inflight.next(), if !inflight.is_empty() => {
                            if let Some(completed) = completed {
                                handle_completed_forward(completed, worker_observer.as_deref());
                            }
                        }
                    }
                }
            });
            worker_senders.push(tx);
            std::mem::drop(handle);
        }

        let inner = Arc::new(AskForwarderInner {
            workers: worker_senders,
            next_worker: AtomicUsize::new(0),
        });

        Self { inner }
    }

    pub fn try_forward_actor_ask_no_timeout(
        &self,
        destination: RemoteConnection,
        actor_id: u64,
        type_hash: u32,
        payload: Bytes,
        responder: AskResponder,
    ) -> Result<()> {
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            timeout: None,
            use_combined_timeout: false,
            timeout_reply: None,
            error_reply: None,
        })
    }

    pub fn try_forward_actor_ask_with_timeout(
        &self,
        destination: RemoteConnection,
        actor_id: u64,
        type_hash: u32,
        payload: Bytes,
        timeout: Duration,
        responder: AskResponder,
        timeout_reply: Bytes,
        error_reply: Bytes,
    ) -> Result<()> {
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            timeout: Some(timeout),
            use_combined_timeout: false,
            timeout_reply: Some(timeout_reply),
            error_reply: Some(error_reply),
        })
    }

    pub fn try_forward_actor_ask_combined_timeout(
        &self,
        destination: RemoteConnection,
        actor_id: u64,
        type_hash: u32,
        payload: Bytes,
        timeout: Duration,
        responder: AskResponder,
        timeout_reply: Bytes,
        error_reply: Bytes,
    ) -> Result<()> {
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            timeout: Some(timeout),
            use_combined_timeout: true,
            timeout_reply: Some(timeout_reply),
            error_reply: Some(error_reply),
        })
    }

    fn try_send_task(&self, task: ForwardTask) -> Result<()> {
        let worker_count = self.inner.workers.len();
        let worker_idx = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        self.inner.workers[worker_idx]
            .try_send(task)
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => GossipError::WriteQueueFull,
                mpsc::error::TrySendError::Closed(_) => GossipError::Shutdown,
            })?;
        Ok(())
    }
}

struct CompletedForward {
    responder: AskResponder,
    response: Result<Bytes>,
    timeout_reply: Option<Bytes>,
    error_reply: Option<Bytes>,
}

fn can_receive_more(rx_closed: bool, inflight_len: usize, max_inflight: usize) -> bool {
    !rx_closed && inflight_len < max_inflight
}

/// ACTOR_REM_2 R16k: isolate a panicking forwarded-ask future so it kills only
/// that one forward, not the shared worker task. Without this, a panic anywhere
/// in the awaited forward chain unwinds the whole worker; every subsequent send
/// to that worker's channel then maps to `GossipError::Shutdown`, permanently
/// losing `1/workers` of forwarding capacity per panic. Returns `None` when the
/// forward panicked (its responder is dropped, so the caller fails/times out as
/// it would for any transport error).
async fn run_forward_task_isolated(task: ForwardTask) -> Option<CompletedForward> {
    std::panic::AssertUnwindSafe(run_forward_task(task))
        .catch_unwind()
        .await
        .ok()
}

async fn run_forward_task(task: ForwardTask) -> CompletedForward {
    let response = match task.timeout {
        Some(timeout) if task.use_combined_timeout => {
            task.destination
                .ask_actor_frame(task.actor_id, task.type_hash, task.payload, timeout)
                .await
        }
        Some(timeout) => tokio::time::timeout(
            timeout,
            task.destination.ask_actor_frame_no_timeout(
                task.actor_id,
                task.type_hash,
                task.payload,
            ),
        )
        .await
        .map_err(|_| GossipError::Timeout)
        .and_then(|reply| reply),
        None => {
            task.destination
                .ask_actor_frame_no_timeout(task.actor_id, task.type_hash, task.payload)
                .await
        }
    };

    CompletedForward {
        responder: task.responder,
        response,
        timeout_reply: task.timeout_reply,
        error_reply: task.error_reply,
    }
}

fn handle_completed_forward(
    completed: CompletedForward,
    completion_observer: Option<&dyn AskForwardObserver>,
) {
    match completed.response {
        Ok(reply) => {
            if let Some(observer) = completion_observer {
                observer.record_success();
            }
            let _ = completed.responder.try_reply_bytes(reply);
        }
        Err(GossipError::Timeout) => {
            if let Some(observer) = completion_observer {
                observer.record_error();
            }
            if let Some(reply) = completed.timeout_reply {
                let _ = completed.responder.try_reply_bytes(reply);
            }
        }
        Err(_) => {
            if let Some(observer) = completion_observer {
                observer.record_error();
            }
            if let Some(reply) = completed.error_reply {
                let _ = completed.responder.try_reply_bytes(reply);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receive_guard_disables_reads_at_inflight_limit() {
        assert!(can_receive_more(
            false,
            MAX_INFLIGHT_PER_WORKER - 1,
            MAX_INFLIGHT_PER_WORKER
        ));
        assert!(!can_receive_more(
            false,
            MAX_INFLIGHT_PER_WORKER,
            MAX_INFLIGHT_PER_WORKER
        ));
        assert!(!can_receive_more(
            false,
            MAX_INFLIGHT_PER_WORKER + 1,
            MAX_INFLIGHT_PER_WORKER
        ));
    }

    #[test]
    fn receive_guard_disables_reads_after_channel_closes() {
        assert!(!can_receive_more(true, 0, MAX_INFLIGHT_PER_WORKER));
    }
}
