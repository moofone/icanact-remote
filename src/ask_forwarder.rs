use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

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
    shutdown: AtomicBool,
    worker_abort_handles: Vec<AbortHandle>,
}

const MAX_INFLIGHT_PER_WORKER: usize = 16;

impl Drop for AskForwarderInner {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for handle in &self.worker_abort_handles {
            handle.abort();
        }
    }
}

#[derive(Clone)]
pub struct AskForwarder {
    inner: Arc<AskForwarderInner>,
}

impl AskForwarder {
    pub fn new(workers: usize, capacity: usize) -> Self {
        let workers = workers.max(1);
        let capacity = capacity.max(128);
        let max_inflight = capacity.min(MAX_INFLIGHT_PER_WORKER).max(1);
        let shutdown = AtomicBool::new(false);

        let mut abort_handles = Vec::with_capacity(workers);
        let mut worker_senders = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, mut rx) = mpsc::channel::<ForwardTask>(capacity);
            let handle = tokio::spawn(async move {
                let mut inflight = FuturesUnordered::new();
                let mut rx_closed = false;

                loop {
                    while inflight.len() < max_inflight {
                        match rx.try_recv() {
                            Ok(task) => inflight.push(Box::pin(run_forward_task(task))),
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
                        maybe_task = rx.recv(), if !rx_closed => {
                            match maybe_task {
                                Some(task) => inflight.push(Box::pin(run_forward_task(task))),
                                None => rx_closed = true,
                            }
                        }
                        Some(completed) = inflight.next(), if !inflight.is_empty() => {
                            handle_completed_forward(completed);
                        }
                    }
                }
            });
            worker_senders.push(tx);
            abort_handles.push(handle.abort_handle());
        }

        let inner = Arc::new(AskForwarderInner {
            workers: worker_senders,
            next_worker: AtomicUsize::new(0),
            shutdown,
            worker_abort_handles: abort_handles,
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
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }

        let worker_count = self.inner.workers.len();
        let worker_idx = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        self.inner.workers[worker_idx]
            .try_send(ForwardTask {
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
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => GossipError::WriteQueueFull,
                mpsc::error::TrySendError::Closed(_) => GossipError::Shutdown,
            })?;
        Ok(())
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
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }

        let worker_count = self.inner.workers.len();
        let worker_idx = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        self.inner.workers[worker_idx]
            .try_send(ForwardTask {
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
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => GossipError::WriteQueueFull,
                mpsc::error::TrySendError::Closed(_) => GossipError::Shutdown,
            })?;
        Ok(())
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
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }

        let worker_count = self.inner.workers.len();
        let worker_idx = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        self.inner.workers[worker_idx]
            .try_send(ForwardTask {
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

async fn run_forward_task(task: ForwardTask) -> CompletedForward {
    let response = match task.timeout {
        Some(timeout) if task.use_combined_timeout => task
            .destination
            .ask_actor_frame(task.actor_id, task.type_hash, task.payload, timeout)
            .await,
        Some(timeout) => tokio::time::timeout(
            timeout,
            task.destination
                .ask_actor_frame_no_timeout(task.actor_id, task.type_hash, task.payload),
        )
        .await
        .map_err(|_| GossipError::Timeout)
        .and_then(|reply| reply),
        None => task
            .destination
            .ask_actor_frame_no_timeout(task.actor_id, task.type_hash, task.payload)
            .await,
    };

    CompletedForward {
        responder: task.responder,
        response,
        timeout_reply: task.timeout_reply,
        error_reply: task.error_reply,
    }
}

fn handle_completed_forward(completed: CompletedForward) {
    match completed.response {
        Ok(reply) => {
            let _ = completed.responder.try_reply_bytes(reply);
        }
        Err(GossipError::Timeout) => {
            if let Some(reply) = completed.timeout_reply {
                let _ = completed.responder.try_reply_bytes(reply);
            }
        }
        Err(_) => {
            if let Some(reply) = completed.error_reply {
                let _ = completed.responder.try_reply_bytes(reply);
            }
        }
    }
}
