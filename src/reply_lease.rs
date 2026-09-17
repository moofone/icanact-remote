use bytes::Bytes;
use std::fmt;
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::connection_pool::LockFreeStreamHandle;
use crate::framing;

/// An exact-sized, shared response payload.
///
/// Payloads are copied at the API boundary so byte reservations describe the
/// allocation retained by the lease rather than an arbitrary `Bytes` slice.
#[derive(Clone, PartialEq, Eq)]
pub struct ReplyPayload(Bytes);

impl ReplyPayload {
    pub fn copy_from_slice(payload: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(payload))
    }

    pub const fn from_static(payload: &'static [u8]) -> Self {
        Self(Bytes::from_static(payload))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl AsRef<[u8]> for ReplyPayload {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for ReplyPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReplyPayload").field(&self.0.len()).finish()
    }
}

/// Connection-independent bounds for outstanding leased replies.
pub struct ReplyDeliveryBudget {
    job_permits: Arc<Semaphore>,
    byte_permits: Arc<Semaphore>,
    cancelled_reply: Arc<ReplyPayload>,
}

impl fmt::Debug for ReplyDeliveryBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyDeliveryBudget")
            .field("job_limit", &self.job_permits.available_permits())
            .field("byte_limit", &self.byte_permits.available_permits())
            .finish_non_exhaustive()
    }
}

impl ReplyDeliveryBudget {
    pub fn new(
        job_limit: usize,
        byte_limit: usize,
        cancelled_reply: ReplyPayload,
    ) -> crate::Result<Self> {
        if job_limit == 0 || byte_limit == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reply delivery limits must be non-zero",
            )
            .into());
        }
        // Tokio reserves the high semaphore bits for internal state. Check
        // before construction so invalid configuration returns an error rather
        // than triggering Semaphore::new's assertion.
        const MAX_SEMAPHORE_PERMITS: usize = usize::MAX >> 3;
        if job_limit > MAX_SEMAPHORE_PERMITS || byte_limit > MAX_SEMAPHORE_PERMITS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reply delivery limits exceed semaphore permit range",
            )
            .into());
        }
        if cancelled_reply.len() > crate::MAX_STREAM_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cancelled reply exceeds the streaming payload limit",
            )
            .into());
        }
        Ok(Self {
            job_permits: Arc::new(Semaphore::new(job_limit)),
            byte_permits: Arc::new(Semaphore::new(byte_limit)),
            cancelled_reply: Arc::new(cancelled_reply),
        })
    }

    pub(crate) fn try_reserve(
        &self,
        max_reply_bytes: usize,
    ) -> std::result::Result<
        (
            tokio::sync::OwnedSemaphorePermit,
            tokio::sync::OwnedSemaphorePermit,
        ),
        crate::GossipError,
    > {
        let max_reply_bytes = u32::try_from(max_reply_bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reply reservation exceeds semaphore permit range",
            )
        })?;
        if max_reply_bytes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reply reservation must be non-zero",
            )
            .into());
        }
        let job = self.job_permits.clone().try_acquire_owned().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "reply job budget is exhausted",
            )
        })?;
        let bytes = match self
            .byte_permits
            .clone()
            .try_acquire_many_owned(max_reply_bytes)
        {
            Ok(permit) => permit,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "reply byte budget is exhausted",
                )
                .into());
            }
        };
        Ok((job, bytes))
    }

    pub(crate) fn cancelled_reply(&self) -> Arc<ReplyPayload> {
        Arc::clone(&self.cancelled_reply)
    }
}

/// Why admission did not produce a lease.
pub enum ReplyLeaseAdmissionError {
    /// Capacity or transport state was unavailable before the responder claim.
    Unavailable {
        responder: crate::AskResponder,
        error: crate::GossipError,
    },
    /// A sibling already claimed this ask.
    ClaimUnavailable(crate::GossipError),
    /// A claimed fallback still could not reserve transport capacity.
    FallbackUnavailable {
        fallback: crate::ImmediateReplyFallback,
    },
}

impl ReplyLeaseAdmissionError {
    pub fn error(&self) -> &crate::GossipError {
        match self {
            Self::Unavailable { error, .. } | Self::ClaimUnavailable(error) => error,
            Self::FallbackUnavailable { fallback } => fallback.error(),
        }
    }

    pub fn into_responder(self) -> Option<crate::AskResponder> {
        match self {
            Self::Unavailable { responder, .. } => Some(responder),
            Self::ClaimUnavailable(_) | Self::FallbackUnavailable { .. } => None,
        }
    }
}

impl fmt::Debug for ReplyLeaseAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyLeaseAdmissionError")
            .field("error", self.error())
            .finish()
    }
}

/// The single-use capability for one leased reply.
pub struct ReplyLease {
    pub(crate) record: Arc<crate::connection_pool::reply_slots::ReplySlotRecord>,
    pub(crate) stream_handle: Arc<LockFreeStreamHandle>,
    reply_observer: Option<Arc<dyn crate::AskReplyObserver>>,
    observer_notified: bool,
    transferred: bool,
}

impl fmt::Debug for ReplyLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyLease")
            .field("correlation_id", &self.record.correlation_id())
            .field("instance_id", &self.record.instance_id())
            .field("handle_instance_id", &self.stream_handle.instance_id())
            .finish_non_exhaustive()
    }
}

impl ReplyLease {
    pub(crate) fn new(
        stream_handle: Arc<LockFreeStreamHandle>,
        record: Arc<crate::connection_pool::reply_slots::ReplySlotRecord>,
    ) -> Self {
        Self {
            record,
            stream_handle,
            reply_observer: None,
            observer_notified: false,
            transferred: false,
        }
    }

    fn with_observer(
        stream_handle: Arc<LockFreeStreamHandle>,
        record: Arc<crate::connection_pool::reply_slots::ReplySlotRecord>,
        reply_observer: Option<Arc<dyn crate::AskReplyObserver>>,
    ) -> Self {
        Self {
            record,
            stream_handle,
            reply_observer,
            observer_notified: false,
            transferred: false,
        }
    }

    fn notify_observer(&mut self, payload: &ReplyPayload) {
        if self.observer_notified {
            return;
        }
        if let Some(observer) = &self.reply_observer {
            observer.reply_claimed(Bytes::copy_from_slice(payload.as_ref()));
            self.observer_notified = true;
        }
    }

    /// Publish one response to the existing connection IO owner without
    /// waiting for peer acknowledgement.
    pub fn try_reply_bytes(mut self, payload: ReplyPayload) -> crate::Result<()> {
        if let Err(error) = self.record.publish(payload.clone()) {
            self.record.cancel();
            return Err(error);
        }
        self.notify_observer(&payload);
        self.record.activate();
        self.transferred = true;
        Ok(())
    }

    /// Publish a response and retain the lease until its terminal flush.
    pub async fn reply_bytes(mut self, payload: ReplyPayload) -> crate::Result<()> {
        if let Err(error) = self.record.publish(payload.clone()) {
            self.record.cancel();
            return Err(error);
        }
        self.notify_observer(&payload);
        self.record.activate();
        let result = self.record.wait_complete().await;
        self.transferred = true;
        result
    }
}

impl Drop for ReplyLease {
    fn drop(&mut self) {
        if !self.transferred {
            self.record.cancel();
        }
    }
}

pub(crate) fn reserve_for_responder(
    responder: crate::AskResponder,
    budget: &ReplyDeliveryBudget,
    max_reply_bytes: usize,
    claimed: bool,
) -> std::result::Result<ReplyLease, ReplyLeaseAdmissionError> {
    let stream_handle = match responder.stream_handle_for_lease() {
        Ok(handle) => handle,
        Err(error) => {
            if claimed {
                return Err(ReplyLeaseAdmissionError::FallbackUnavailable {
                    fallback: crate::ImmediateReplyFallback::from_claimed_responder(
                        responder, error,
                    ),
                });
            }
            return Err(ReplyLeaseAdmissionError::Unavailable { responder, error });
        }
    };

    let (job_permit, byte_permit) = match budget.try_reserve(max_reply_bytes) {
        Ok(permits) => permits,
        Err(error) => {
            if claimed {
                return Err(ReplyLeaseAdmissionError::FallbackUnavailable {
                    fallback: crate::ImmediateReplyFallback::from_claimed_responder(
                        responder, error,
                    ),
                });
            }
            return Err(ReplyLeaseAdmissionError::Unavailable { responder, error });
        }
    };

    let terminal_payload = budget.cancelled_reply();
    if max_reply_bytes == 0 || max_reply_bytes > crate::MAX_STREAM_SIZE {
        let error = std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reply reservation is outside the streaming payload limit",
        )
        .into();
        if claimed {
            return Err(ReplyLeaseAdmissionError::FallbackUnavailable {
                fallback: crate::ImmediateReplyFallback::from_claimed_responder(responder, error),
            });
        }
        return Err(ReplyLeaseAdmissionError::Unavailable { responder, error });
    }
    if terminal_payload.len() > max_reply_bytes {
        let error = std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reply reservation is smaller than its cancellation payload",
        )
        .into();
        if claimed {
            return Err(ReplyLeaseAdmissionError::FallbackUnavailable {
                fallback: crate::ImmediateReplyFallback::from_claimed_responder(responder, error),
            });
        }
        return Err(ReplyLeaseAdmissionError::Unavailable { responder, error });
    }
    let terminal_frame_len =
        framing::ASK_RESPONSE_HEADER_LEN.saturating_add(terminal_payload.len());
    if terminal_frame_len > stream_handle.max_message_size() {
        let error = crate::GossipError::InvalidConfig(format!(
            "max_message_size={} cannot carry the terminal leased reply",
            stream_handle.max_message_size()
        ));
        if claimed {
            return Err(ReplyLeaseAdmissionError::FallbackUnavailable {
                fallback: crate::ImmediateReplyFallback::from_claimed_responder(responder, error),
            });
        }
        return Err(ReplyLeaseAdmissionError::Unavailable { responder, error });
    }
    let reply_observer = responder.reply_observer_for_lease();
    let record = match stream_handle.reply_slots().try_reserve(
        responder.correlation_id(),
        max_reply_bytes,
        terminal_payload,
        job_permit,
        byte_permit,
    ) {
        Ok(record) => record,
        Err(error) => {
            if claimed {
                return Err(ReplyLeaseAdmissionError::FallbackUnavailable {
                    fallback: crate::ImmediateReplyFallback::from_claimed_responder(
                        responder, error,
                    ),
                });
            }
            return Err(ReplyLeaseAdmissionError::Unavailable { responder, error });
        }
    };

    if !claimed && let Err(error) = responder.claim_for_lease() {
        record.discard();
        return Err(ReplyLeaseAdmissionError::ClaimUnavailable(error));
    }

    Ok(ReplyLease::with_observer(
        stream_handle,
        record,
        reply_observer,
    ))
}
