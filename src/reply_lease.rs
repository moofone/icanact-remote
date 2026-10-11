#[cfg(test)]
use bytes::Bytes;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::connection_pool::LockFreeStreamHandle;
use crate::framing;

/// A complete response owner with a retained-allocation receipt.
///
/// Moving a Vec, boxed slice, pooled payload, or ForwardPayload preserves its
/// original allocation. Arbitrary Bytes views are deliberately not accepted:
/// their visible length cannot prove the size of the backing owner (REMOTE-2/5).
/// Use `copy_from_slice` only when an explicit copy is intended.
pub struct ReplyPayload(crate::ForwardPayload);

#[cfg(test)]
thread_local! {
    // Boundary instrumentation only: this is not a global allocator or a claim
    // about Bytes/Arc metadata allocations elsewhere in the transport.
    static PAYLOAD_BOUNDARY_COUNTS: std::cell::Cell<(usize, usize, usize)> =
        const { std::cell::Cell::new((0, 0, 0)) };
}

impl ReplyPayload {
    /// Explicitly promote this owner for deliberate shared fanout. Ordinary
    /// reply movement stays unique and never performs this allocation.
    pub fn into_shared(self) -> Self {
        Self(self.0.into_shared())
    }

    /// Clone only an already-promoted fanout owner. Full retained capacity is
    /// preserved and charged independently by every admitted transport lease.
    pub fn shared_clone(&self) -> crate::Result<Self> {
        let payload = self.0.shared_clone()?;
        #[cfg(test)]
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| {
            let (copy_requests, copied_bytes, clones) = counts.get();
            counts.set((copy_requests, copied_bytes, clones + 1));
        });
        Ok(Self(payload))
    }

    pub fn copy_from_slice(payload: &[u8]) -> Self {
        #[cfg(test)]
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| {
            let (copy_requests, copied_bytes, clones) = counts.get();
            counts.set((copy_requests + 1, copied_bytes + payload.len(), clones));
        });
        Self(crate::ForwardPayload::copy_from_slice(payload))
    }

    pub fn from_static(payload: &'static [u8]) -> Self {
        Self(crate::ForwardPayload::from_static(payload))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Entire retained allocation, including unused capacity.
    pub fn retained_bytes(&self) -> usize {
        self.0.retained_bytes()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Move the native allocation directly into the writer's owned frame state.
    pub(crate) fn into_forward_payload(self) -> crate::ForwardPayload {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn into_bytes(self) -> Bytes {
        self.0.into_bytes()
    }

    #[cfg(test)]
    pub(crate) fn from_owner<T>(owner: T, retained_bytes: usize) -> Self
    where
        T: AsRef<[u8]> + Send + Sync + 'static,
    {
        Self(crate::ForwardPayload::from_test_owner(
            owner,
            retained_bytes,
        ))
    }
}

impl From<crate::ForwardPayload> for ReplyPayload {
    fn from(payload: crate::ForwardPayload) -> Self {
        Self(payload)
    }
}

impl From<Vec<u8>> for ReplyPayload {
    fn from(payload: Vec<u8>) -> Self {
        Self(payload.into())
    }
}

impl From<Box<[u8]>> for ReplyPayload {
    fn from(payload: Box<[u8]>) -> Self {
        Self(payload.into())
    }
}

impl From<crate::typed::PooledPayload> for ReplyPayload {
    fn from(payload: crate::typed::PooledPayload) -> Self {
        Self(payload.into())
    }
}

impl TryFrom<crate::AlignedBytes> for ReplyPayload {
    type Error = crate::GossipError;

    fn try_from(payload: crate::AlignedBytes) -> crate::Result<Self> {
        crate::ForwardPayload::try_from(payload).map(Self)
    }
}

impl AsRef<[u8]> for ReplyPayload {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PartialEq for ReplyPayload {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for ReplyPayload {}

impl fmt::Debug for ReplyPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyPayload")
            .field("len", &self.len())
            .field("retained_bytes", &self.retained_bytes())
            .finish()
    }
}

/// Connection-independent count/byte bounds, registered once per connection.
///
/// Registration retains the cancellation owner for that connection's lifetime.
/// Every reply thereafter transfers a numeric charge, without semaphore owners,
/// shared-payload handles, heap nodes or reference-count operations.
pub struct ReplyDeliveryBudget {
    state: Arc<ReplyBudgetState>,
}

pub(crate) struct ReplyBudgetState {
    jobs: AtomicUsize,
    bytes: AtomicUsize,
    pub(crate) cancelled_reply: ReplyPayload,
}

impl ReplyBudgetState {
    fn take(counter: &AtomicUsize, amount: usize) -> crate::Result<()> {
        let available = counter.load(Ordering::Acquire);
        let remaining = available
            .checked_sub(amount)
            .ok_or(crate::GossipError::WriteQueueFull)?;
        counter
            .compare_exchange(available, remaining, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| crate::GossipError::WriteQueueFull)?;
        Ok(())
    }

    pub(crate) fn try_reserve(&self, bytes: usize) -> crate::Result<()> {
        if bytes == 0 {
            return Err(crate::GossipError::InvalidConfig(
                "reply reservation must be non-zero".into(),
            ));
        }
        Self::take(&self.jobs, 1)?;
        if let Err(error) = Self::take(&self.bytes, bytes) {
            self.jobs.fetch_add(1, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn release(&self, bytes: usize) {
        self.bytes.fetch_add(bytes, Ordering::Release);
        self.jobs.fetch_add(1, Ordering::Release);
    }
}

impl fmt::Debug for ReplyDeliveryBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyDeliveryBudget")
            .field("available_jobs", &self.state.jobs.load(Ordering::Acquire))
            .field("available_bytes", &self.state.bytes.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// A scoped registration for one exact stream and one delivery budget.
///
/// Create this once, then borrow it for every lease. A connection admits at most
/// 64 registrations and 64 simultaneous lease generations. Registration storage
/// and its terminal payload remain connection-owned until teardown; no record
/// can redirect its numeric budget index to another session or registration.
pub struct ReplyDeliverySession<'a> {
    stream: &'a Arc<LockFreeStreamHandle>,
    budget_index: usize,
}

impl fmt::Debug for ReplyDeliverySession<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyDeliverySession")
            .field("instance_id", &self.stream.instance_id())
            .field("budget_index", &self.budget_index)
            .finish()
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
        // Preserve the documented configuration bound while removing semaphore
        // representation and owned-permit traffic from the actual hot path.
        const MAX_REPLY_BUDGET: usize = usize::MAX >> 3;
        if job_limit > MAX_REPLY_BUDGET || byte_limit > MAX_REPLY_BUDGET {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reply delivery limits exceed the supported budget range",
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
        if cancelled_reply.retained_bytes() > byte_limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reply byte limit is smaller than the cancellation payload",
            )
            .into());
        }
        Ok(Self {
            state: Arc::new(ReplyBudgetState {
                jobs: AtomicUsize::new(job_limit),
                bytes: AtomicUsize::new(byte_limit),
                cancelled_reply,
            }),
        })
    }

    pub fn bind<'a>(
        &self,
        session: &'a crate::ReplySession,
    ) -> crate::Result<ReplyDeliverySession<'a>> {
        let stream = session.stream_handle();
        let budget_index = stream.reply_slots().bind_budget(Arc::clone(&self.state))?;
        Ok(ReplyDeliverySession {
            stream,
            budget_index,
        })
    }

    #[cfg(test)]
    fn try_reserve(&self, bytes: usize) -> crate::Result<ReplyBudgetReservation<'_>> {
        self.state.try_reserve(bytes)?;
        Ok(ReplyBudgetReservation {
            state: &self.state,
            bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn available_test_permits(&self) -> (usize, usize) {
        (
            self.state.jobs.load(Ordering::Acquire),
            self.state.bytes.load(Ordering::Acquire),
        )
    }
}

#[cfg(test)]
struct ReplyBudgetReservation<'a> {
    state: &'a ReplyBudgetState,
    bytes: usize,
}

#[cfg(test)]
impl Drop for ReplyBudgetReservation<'_> {
    fn drop(&mut self) {
        self.state.release(self.bytes);
    }
}

/// Why admission did not produce a lease.
pub enum ReplyLeaseAdmissionError<'a> {
    /// Capacity or transport state was unavailable before the responder claim.
    Unavailable {
        responder: crate::AskResponder<'a>,
        error: crate::GossipError,
    },
    /// A sibling already claimed this ask.
    ClaimUnavailable(crate::GossipError),
    /// A claimed fallback still could not reserve transport capacity.
    FallbackUnavailable {
        fallback: crate::ImmediateReplyFallback<'a>,
    },
}

impl<'a> ReplyLeaseAdmissionError<'a> {
    pub fn error(&self) -> &crate::GossipError {
        match self {
            Self::Unavailable { error, .. } | Self::ClaimUnavailable(error) => error,
            Self::FallbackUnavailable { fallback } => fallback.error(),
        }
    }

    pub fn into_responder(self) -> Option<crate::AskResponder<'a>> {
        match self {
            Self::Unavailable { responder, .. } => Some(responder),
            Self::ClaimUnavailable(_) | Self::FallbackUnavailable { .. } => None,
        }
    }
}

impl fmt::Debug for ReplyLeaseAdmissionError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyLeaseAdmissionError")
            .field("error", self.error())
            .finish()
    }
}

/// The single-use capability for one leased reply.
///
/// Reservation and writer state use connection-preallocated storage. A completed
/// lease releases delivery permits before waking its waiter; its storage slot is
/// reusable after the final internal observer drops. Retained observers therefore
/// apply bounded local backpressure and cannot mutate a later slot generation.
pub struct ReplyLease<'a> {
    pub(crate) record: crate::connection_pool::reply_slots::ReplySlotLease<'a>,
    reply_observer: Option<Arc<dyn crate::AskReplyObserver>>,
    observer_notified: bool,
    transferred: bool,
}

impl fmt::Debug for ReplyLease<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyLease")
            .field("correlation_id", &self.record.correlation_id())
            .field("instance_id", &self.record.instance_id())
            .finish_non_exhaustive()
    }
}

impl<'a> ReplyLease<'a> {
    #[cfg(test)]
    pub(crate) fn new(
        _stream_handle: Arc<LockFreeStreamHandle>,
        record: crate::connection_pool::reply_slots::ReplySlotLease<'a>,
    ) -> Self {
        Self {
            record,
            reply_observer: None,
            observer_notified: false,
            transferred: false,
        }
    }

    fn with_observer(
        record: crate::connection_pool::reply_slots::ReplySlotLease<'a>,
        reply_observer: Option<Arc<dyn crate::AskReplyObserver>>,
        observer_notified: bool,
    ) -> Self {
        Self {
            record,
            reply_observer,
            observer_notified,
            transferred: false,
        }
    }

    fn publish(&mut self, payload: ReplyPayload) -> crate::Result<()> {
        let observer = &self.reply_observer;
        let notified = &mut self.observer_notified;
        self.record.publish_observed(payload, |payload| {
            if !*notified && let Some(observer) = observer {
                *notified = true;
                observer.reply_claimed_borrowed(crate::ReplyPayloadRef::new(
                    None,
                    payload.as_ref(),
                    payload.len(),
                ));
            }
        })
    }

    /// Publish one response to the existing connection IO owner without
    /// waiting for peer acknowledgement.
    pub fn try_reply_bytes(mut self, payload: ReplyPayload) -> crate::Result<()> {
        if let Err(error) = self.publish(payload) {
            if matches!(error, crate::GossipError::ConnectionClosed(_)) {
                // Close won the publication linearization. The claim remains
                // consumed, but it must not activate a terminal frame or
                // notify an observer after transport teardown.
                self.transferred = true;
            } else {
                self.record.cancel();
            }
            return Err(error);
        }
        self.record.activate();
        self.transferred = true;
        Ok(())
    }

    /// Publish a response and retain the lease until its terminal flush.
    pub async fn reply_bytes(mut self, payload: ReplyPayload) -> crate::Result<()> {
        if let Err(error) = self.publish(payload) {
            if matches!(error, crate::GossipError::ConnectionClosed(_)) {
                self.transferred = true;
            } else {
                self.record.cancel();
            }
            return Err(error);
        }
        self.record.activate();
        let result = self.record.wait_complete().await;
        self.transferred = true;
        result
    }
}

impl Drop for ReplyLease<'_> {
    fn drop(&mut self) {
        if !self.transferred {
            self.record.cancel();
        }
    }
}

pub(crate) fn reserve_for_responder<'a, 's>(
    responder: crate::AskResponder<'a>,
    session: &'s ReplyDeliverySession<'_>,
    max_reply_bytes: usize,
    claimed: bool,
) -> std::result::Result<ReplyLease<'s>, ReplyLeaseAdmissionError<'a>> {
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

    if !Arc::ptr_eq(stream_handle, session.stream) {
        let error = crate::GossipError::InvalidConfig(
            "reply delivery registration belongs to another physical stream".into(),
        );
        if claimed {
            return Err(ReplyLeaseAdmissionError::FallbackUnavailable {
                fallback: crate::ImmediateReplyFallback::from_claimed_responder(responder, error),
            });
        }
        return Err(ReplyLeaseAdmissionError::Unavailable { responder, error });
    }
    let terminal_payload = &session
        .stream
        .reply_slots()
        .budget(session.budget_index)
        .cancelled_reply;
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
    if terminal_payload.retained_bytes() > max_reply_bytes {
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
    let (reply_observer, observer_notified) = responder.reply_observer_for_lease();
    let record = match session.stream.reply_slots().try_reserve_bound(
        responder.correlation_id(),
        max_reply_bytes,
        session.budget_index,
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
        record,
        reply_observer,
        observer_notified,
    ))
}

#[cfg(test)]
mod owned_payload_tests {
    use super::*;

    #[test]
    fn warm_bound_reservation_and_writer_tokens_allocate_no_shared_owners() {
        use crate::ask_forwarder::allocation_evidence;

        let slots = crate::connection_pool::reply_slots::ReplySlots::new(
            93,
            "127.0.0.1:40703".parse().unwrap(),
            Arc::new(tokio::sync::Notify::new()),
        );
        let budget =
            ReplyDeliveryBudget::new(64, 64 * 32, ReplyPayload::from_static(b"cancel")).unwrap();
        let budget_index = slots.bind_budget(Arc::clone(&budget.state)).unwrap();
        let owner = slots.writer_owner().unwrap();
        let slot_owners = Arc::strong_count(&slots);
        let budget_owners = Arc::strong_count(&budget.state);
        let (_, counts) = allocation_evidence::measure(|| {
            for id in 1..=256 {
                let record = slots.try_reserve_bound(id, 32, budget_index).unwrap();
                record.publish(ReplyPayload::from_static(b"reply")).unwrap();
                record.activate();
                drop(record);
                let token = slots.pop_ready().unwrap();
                {
                    let record = owner.record(&token).unwrap();
                    assert_eq!(record.normal_payload().unwrap().as_ref(), b"reply");
                    record.note_normal_outcome();
                    record.finish();
                }
                owner.finish(token);
            }
        });
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        assert_eq!(Arc::strong_count(&slots), slot_owners);
        assert_eq!(Arc::strong_count(&budget.state), budget_owners);
        assert_eq!(budget.available_test_permits(), (64, 64 * 32));
        assert_eq!(slots.free_count(), 64);
        assert_eq!(slots.stats_snapshot().normal_completions, 256);
    }

    #[test]
    fn bound_terminal_owner_and_registration_storage_have_explicit_limits() {
        let slots = crate::connection_pool::reply_slots::ReplySlots::new(
            96,
            "127.0.0.1:40708".parse().unwrap(),
            Arc::new(tokio::sync::Notify::new()),
        );
        let owner = slots.writer_owner().unwrap();
        let budget = ReplyDeliveryBudget::new(1, 32, ReplyPayload::from_static(b"cancel")).unwrap();
        let mut first = None;
        for _ in 0..64 {
            let index = slots.bind_budget(Arc::clone(&budget.state)).unwrap();
            first.get_or_insert(index);
        }
        assert!(matches!(
            slots.bind_budget(Arc::clone(&budget.state)),
            Err(crate::GossipError::WriteQueueFull)
        ));
        let record = slots.try_reserve_bound(1, 32, first.unwrap()).unwrap();
        record.cancel();
        let token = slots.pop_ready().unwrap();
        {
            let view = owner.record(&token).unwrap();
            let terminal = view.terminal_payload().unwrap();
            assert_eq!(terminal.as_ref(), b"cancel");
            assert_eq!(
                terminal.as_ref().as_ptr(),
                budget.state.cancelled_reply.as_ref().as_ptr()
            );
            assert_eq!(budget.available_test_permits(), (0, 0));
            view.note_terminal_outcome();
            view.finish();
        }
        owner.finish(token);
        drop(record);
        assert_eq!(budget.available_test_permits(), (1, 32));
        assert_eq!(slots.free_count(), 64);
        assert_eq!(slots.stats_snapshot().terminal_completions, 1);
    }

    #[test]
    fn registration_charges_terminal_capacity_instead_of_visible_length() {
        let slots = crate::connection_pool::reply_slots::ReplySlots::new(
            97,
            "127.0.0.1:40709".parse().unwrap(),
            Arc::new(tokio::sync::Notify::new()),
        );
        let mut backing = Vec::with_capacity(16 * 1024 * 1024 + 1);
        backing.push(1);
        let capacity = backing.capacity();
        let oversized = ReplyDeliveryBudget::new(1, capacity, ReplyPayload::from(backing)).unwrap();
        assert_eq!(oversized.state.cancelled_reply.len(), 1);
        assert!(matches!(
            slots.bind_budget(Arc::clone(&oversized.state)),
            Err(crate::GossipError::WriteQueueFull)
        ));
        assert_eq!(Arc::strong_count(&oversized.state), 1);
        let admissible =
            ReplyDeliveryBudget::new(1, 32, ReplyPayload::from_static(b"cancel")).unwrap();
        assert!(slots.bind_budget(Arc::clone(&admissible.state)).is_ok());
    }

    #[test]
    fn original_owner_pointer_and_capacity_survive_reply_transfer() {
        let mut bytes = Vec::with_capacity(4096);
        bytes.extend_from_slice(b"reply");
        let original = bytes.as_ptr();
        let capacity = bytes.capacity();
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| counts.set((0, 0, 0)));
        let payload = ReplyPayload::from(bytes);
        assert_eq!(payload.as_ref().as_ptr(), original);
        assert_eq!(payload.retained_bytes(), capacity);
        let written = payload.into_bytes();
        assert_eq!(written.as_ptr(), original);
        assert_eq!(written.as_ref(), b"reply");
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| assert_eq!(counts.get(), (0, 0, 0)));
    }

    #[tokio::test]
    async fn unobserved_lease_publication_transfers_without_boundary_copy_or_clone() {
        use crate::connection_pool::{BufferConfig, ChannelId};
        use std::sync::atomic::AtomicBool;

        let budget = ReplyDeliveryBudget::new(1, 32, ReplyPayload::from_static(b"cancel")).unwrap();
        let (io, _peer) = tokio::io::duplex(4096);
        let (handle, writer, reader) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:40618".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let handle = Arc::new(handle);
        let reply_session =
            crate::AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
                .reply_session()
                .unwrap();
        let delivery = budget.bind(&reply_session).unwrap();
        let responder = crate::AskResponder::from_stream_handle(
            1,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        );
        let lease = responder.try_reply_lease(&delivery, 32).unwrap();
        let payload = ReplyPayload::from(b"reply".to_vec());
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| counts.set((0, 0, 0)));
        lease.try_reply_bytes(payload).unwrap();
        // There is no await between resetting and reading the thread-local
        // counters, so unrelated IO work cannot contaminate this observation.
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| assert_eq!(counts.get(), (0, 0, 0)));
        handle.shutdown();
        let _ = writer.await;
        if let Some(reader) = reader {
            let _ = reader.await;
        }
        assert_eq!(budget.available_test_permits(), (1, 32));
    }

    #[test]
    fn explicit_copy_and_clone_controls_are_observable() {
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| counts.set((0, 0, 0)));
        let payload = ReplyPayload::copy_from_slice(b"copy").into_shared();
        let alias = payload.shared_clone().unwrap();
        assert_eq!(alias.as_ref().as_ptr(), payload.as_ref().as_ptr());
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| assert_eq!(counts.get(), (1, 4, 1)));
    }

    #[test]
    fn opaque_hidden_backing_is_rejected_without_copying() {
        let backing = Bytes::from(vec![7; 4096]);
        let offset = (crate::PAYLOAD_ALIGNMENT
            - (backing.as_ptr() as usize) % crate::PAYLOAD_ALIGNMENT)
            % crate::PAYLOAD_ALIGNMENT;
        let opaque = crate::AlignedBytes::from_bytes(backing.slice(offset..offset + 1)).unwrap();
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| counts.set((0, 0, 0)));
        assert!(ReplyPayload::try_from(opaque).is_err());
        PAYLOAD_BOUNDARY_COUNTS.with(|counts| assert_eq!(counts.get(), (0, 0, 0)));
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    struct TypedLeasePayload(u64);
    crate::wire_type!(
        TypedLeasePayload,
        "reply_lease::owned_payload_tests::TypedLeasePayload"
    );

    #[tokio::test]
    async fn typed_lease_requires_owner_capacity_and_accepts_exact_capacity_budget() {
        use crate::connection_pool::{BufferConfig, ChannelId};
        use std::sync::atomic::AtomicBool;

        for reserve_owner in [false, true] {
            let pooled = crate::typed::encode_typed_pooled(&TypedLeasePayload(42)).unwrap();
            let payload = ReplyPayload::from(pooled);
            let retained = payload.retained_bytes();
            let wire_len = payload.len();
            assert!(
                retained > wire_len,
                "typed fixture must retain spare storage"
            );
            let reserved = if reserve_owner { retained } else { wire_len };
            let budget =
                ReplyDeliveryBudget::new(1, retained, ReplyPayload::from_static(b"x")).unwrap();
            let (io, _peer) = tokio::io::duplex(4096);
            let (handle, writer, reader) = LockFreeStreamHandle::new(
                io,
                "127.0.0.1:40619".parse().unwrap(),
                ChannelId::TellAsk,
                BufferConfig::default(),
                None,
                None,
            );
            let handle = Arc::new(handle);
            let reply_session =
                crate::AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
                    .reply_session()
                    .unwrap();
            let delivery = budget.bind(&reply_session).unwrap();
            let responder = crate::AskResponder::from_stream_handle(
                1,
                Arc::clone(&handle),
                Arc::new(AtomicBool::new(false)),
            );
            let lease = responder.try_reply_lease(&delivery, reserved).unwrap();
            let result = lease.try_reply_bytes(payload);
            if reserve_owner {
                result.expect("full owner capacity must be admissible");
            } else {
                assert!(matches!(
                    result,
                    Err(crate::GossipError::MessageTooLarge { size, max })
                        if size == retained && max == wire_len
                ));
            }
            handle.shutdown();
            let _ = writer.await;
            if let Some(reader) = reader {
                let _ = reader.await;
            }
            assert_eq!(budget.available_test_permits(), (1, retained));
        }
    }

    #[test]
    fn cancellation_budget_charges_capacity_not_visible_bytes() {
        let mut bytes = Vec::with_capacity(4096);
        bytes.push(1);
        let capacity = bytes.capacity();
        assert!(ReplyDeliveryBudget::new(1, capacity - 1, ReplyPayload::from(bytes)).is_err());
        assert!(ReplyDeliveryBudget::new(1, 1, ReplyPayload::from_static(b"x")).is_ok());
    }

    #[test]
    fn rejected_byte_admission_returns_count_credit_and_drop_restores_progress() {
        let budget =
            ReplyDeliveryBudget::new(1, 32, ReplyPayload::from_static(b"cancel")).expect("budget");
        assert!(budget.try_reserve(33).is_err());
        assert_eq!(budget.available_test_permits(), (1, 32));
        let reservation = budget.try_reserve(32).expect("full admission");
        assert!(budget.try_reserve(1).is_err());
        assert_eq!(budget.available_test_permits(), (0, 0));
        drop(reservation);
        assert_eq!(budget.available_test_permits(), (1, 32));
        let successor = budget.try_reserve(32).expect("successor makes progress");
        drop(successor);
        assert_eq!(budget.available_test_permits(), (1, 32));
    }
}
