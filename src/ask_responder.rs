use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::{Buf, Bytes};

use crate::{GossipError, Result, connection_pool::LockFreeStreamHandle, framing};

/// R-E (P1): shared gate for the typed/pooled deferred-reply path
/// (`AskResponder::reply_typed` -> `send_response_pooled`), mirroring
/// `send_response_auto_bytes`'s size gate (R-9) instead of duplicating it.
/// A payload above `MAX_STREAM_SIZE` is rejected locally (every receiver
/// hard-rejects a larger stream as FATAL); a payload above the streaming
/// threshold, or whose inline-encoded size would exceed `max_message_size`,
/// is auto-streamed with its original pooled owner and separate prefix.
/// Both paths use nonblocking, capacity-accounted owner admission.
/// Checking only the streaming
/// threshold here (as this used to) would pick the inline branch below for
/// a payload the inline gate then has to refuse whenever `max_message_size`
/// is smaller than the threshold -- streaming can still deliver it in
/// bounded chunks, so `MessageTooLarge` must stay reserved for payloads
/// that cannot be sent at all (> `MAX_STREAM_SIZE`), mirroring
/// `LockFreeStreamHandle::should_stream_response`.
async fn send_pooled_via_stream_handle(
    stream_handle: &LockFreeStreamHandle,
    correlation_id: u32,
    payload: crate::typed::PooledPayload,
    prefix: Option<[u8; 16]>,
    payload_len: usize,
) -> Result<()> {
    let inline_payload_limit = stream_handle
        .max_message_size()
        .saturating_sub(framing::ASK_RESPONSE_HEADER_LEN);
    let streaming =
        payload_len > stream_handle.streaming_threshold() || payload_len > inline_payload_limit;
    stream_handle
        .send_response_pooled_owned(correlation_id, payload, prefix, payload_len, streaming)
        .await
}

/// Copy an (already above-threshold, so rare) pooled typed payload plus its
/// optional debug type-hash prefix into one contiguous `Bytes` for
/// `stream_response_bytes`, which needs owned, slice-able `Bytes` to chunk
/// without an extra copy per chunk.
#[cfg(test)]
fn pooled_payload_into_bytes(
    prefix: Option<[u8; 16]>,
    mut payload: crate::typed::PooledPayload,
) -> Bytes {
    let prefix_len = prefix.map(|p| p.len()).unwrap_or(0);
    let mut buf = bytes::BytesMut::with_capacity(payload.remaining() + prefix_len);
    if let Some(prefix) = prefix {
        buf.extend_from_slice(&prefix);
    }
    while payload.has_remaining() {
        let chunk = payload.chunk();
        let n = chunk.len();
        buf.extend_from_slice(chunk);
        payload.advance(n);
    }
    buf.freeze()
}

/// ACTOR_REM_2 R15: reject a second reply for one ask. Returns `true` if this
/// call is the first to claim the shared single-use guard (i.e. it may send).
#[inline]
fn claim_reply(used: &AtomicBool) -> Result<()> {
    if used.swap(true, Ordering::AcqRel) {
        return Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "duplicate reply for a single ask correlation was suppressed",
        )));
    }
    Ok(())
}

pub struct TellContext<'a> {
    authenticated_peer_id: Option<&'a crate::PeerId>,
}

impl<'a> TellContext<'a> {
    pub(crate) fn new(authenticated_peer_id: Option<&'a crate::PeerId>) -> Self {
        Self {
            authenticated_peer_id,
        }
    }

    pub fn authenticated_peer_id(&self) -> Option<&crate::PeerId> {
        self.authenticated_peer_id
    }
}

/// A borrowed view of the first reply claimed for one deferred ask.
///
/// The view is valid only for the duration of [`AskReplyObserver::reply_claimed_borrowed`].
/// It can describe a typed reply as two segments: an optional type-hash prefix
/// and the pooled payload body. Observers that need retention must reserve
/// their own bounded storage and copy while the callback is running.
pub struct ReplyPayloadRef<'a> {
    prefix: Option<&'a [u8]>,
    payload: &'a [u8],
    len: usize,
}

impl<'a> ReplyPayloadRef<'a> {
    pub(crate) fn new(prefix: Option<&'a [u8]>, payload: &'a [u8], len: usize) -> Self {
        Self {
            prefix,
            payload,
            len,
        }
    }

    pub fn prefix(&self) -> Option<&'a [u8]> {
        self.prefix
    }

    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Copy this borrowed wire view into the owned value used by the original
    /// observer contract.
    pub fn to_bytes(&self) -> Bytes {
        let mut owned = Vec::with_capacity(self.len);
        if let Some(prefix) = self.prefix {
            owned.extend_from_slice(prefix);
        }
        owned.extend_from_slice(self.payload);
        Bytes::from(owned)
    }
}

/// Observes the first reply claimed for one deferred ask.
///
/// The observer is called at most once for a context, after the responder's
/// shared single-use claim succeeds and before transport enqueue. The callback
/// must remain nonblocking: it runs on the responder's reply path, including
/// the synchronous `try_reply_*` paths.
pub trait AskReplyObserver: Send + Sync {
    /// The original owned-value callback. Keep this required method stable so
    /// observers implemented against the public API before borrowed views were
    /// added remain source-compatible.
    fn reply_claimed(&self, payload: Bytes);

    /// Borrowed observation for callers that can copy or consume the payload
    /// synchronously. The default bridge preserves the owned callback contract.
    fn reply_claimed_borrowed(&self, payload: ReplyPayloadRef<'_>) {
        self.reply_claimed(payload.to_bytes());
    }
}

#[derive(Clone)]
enum AskResponseSink<'a> {
    StreamHandle(&'a Arc<LockFreeStreamHandle>),
    DeferredWriter(&'a Arc<ResponseWriter>),
    #[cfg(any(test, feature = "test-helpers"))]
    OwnedStreamHandle(Arc<LockFreeStreamHandle>),
}

#[derive(Clone)]
enum ReplyClaim<'a> {
    Scoped(&'a AtomicBool),
    #[cfg(any(test, feature = "test-helpers"))]
    Owned(Arc<AtomicBool>),
}

impl std::ops::Deref for ReplyClaim<'_> {
    type Target = AtomicBool;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Scoped(used) => used,
            #[cfg(any(test, feature = "test-helpers"))]
            Self::Owned(used) => used,
        }
    }
}

impl AskResponseSink<'_> {
    fn stream_handle(&self) -> Result<&Arc<LockFreeStreamHandle>> {
        match self {
            Self::StreamHandle(stream) => Ok(stream),
            Self::DeferredWriter(writer) => writer.stream_handle(),
            #[cfg(any(test, feature = "test-helpers"))]
            Self::OwnedStreamHandle(stream) => Ok(stream),
        }
    }

    fn try_send_response_owned(
        &self,
        correlation_id: u32,
        payload: crate::ForwardPayload,
        immediate: bool,
    ) -> Result<()> {
        self.stream_handle()?
            .try_send_response_owned(correlation_id, payload, immediate)
    }

    async fn send_response_owned(
        &self,
        correlation_id: u32,
        payload: crate::ForwardPayload,
    ) -> Result<()> {
        self.stream_handle()?
            .send_response_owned_wait(correlation_id, payload)
            .await
    }

    async fn send_response_bytes(
        &self,
        correlation_id: u32,
        payload: crate::ForwardPayload,
    ) -> Result<()> {
        self.send_response_owned(correlation_id, payload).await
    }

    fn try_send_response_bytes(
        &self,
        correlation_id: u32,
        payload: crate::ForwardPayload,
    ) -> Result<()> {
        self.try_send_response_owned(correlation_id, payload, false)
    }

    fn try_send_response_bytes_immediate(
        &self,
        correlation_id: u32,
        payload: crate::ForwardPayload,
    ) -> Result<()> {
        self.try_send_response_owned(correlation_id, payload, true)
    }

    async fn send_response_pooled(
        &self,
        correlation_id: u32,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        send_pooled_via_stream_handle(
            self.stream_handle()?,
            correlation_id,
            payload,
            prefix,
            payload_len,
        )
        .await
    }
}

enum AskContextSink<'a> {
    StreamHandle(&'a Arc<LockFreeStreamHandle>),
    DeferredWriter(&'a Arc<ResponseWriter>),
}

pub struct AskContext<'a> {
    correlation_id: u32,
    request_id: Option<u64>,
    authenticated_peer_id: Option<&'a crate::PeerId>,
    sink: AskContextSink<'a>,
    /// Scoped single-use guard for this inbound ask. Responders borrow this
    /// fixed field; deferring transfers the claim into a bound session token.
    /// Neither path allocates a per-ask shared guard.
    used: AtomicBool,
    reply_observer: Option<Arc<dyn AskReplyObserver>>,
}

impl<'a> AskContext<'a> {
    /// Construct the production scoped context in external qualification tests.
    #[cfg(feature = "test-helpers")]
    pub fn from_stream_handle_for_test(
        correlation_id: u32,
        stream_handle: &'a Arc<LockFreeStreamHandle>,
    ) -> Self {
        Self::from_stream_handle_with_request_id(correlation_id, stream_handle, None, None)
    }

    pub(crate) fn from_stream_handle_with_request_id(
        correlation_id: u32,
        stream_handle: &'a Arc<LockFreeStreamHandle>,
        authenticated_peer_id: Option<&'a crate::PeerId>,
        request_id: Option<u64>,
    ) -> Self {
        Self {
            correlation_id,
            request_id,
            authenticated_peer_id,
            sink: AskContextSink::StreamHandle(stream_handle),
            used: AtomicBool::new(false),
            reply_observer: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_writer(
        correlation_id: u32,
        writer: &'a Arc<ResponseWriter>,
        authenticated_peer_id: Option<&'a crate::PeerId>,
    ) -> Self {
        Self::from_writer_with_request_id(correlation_id, writer, authenticated_peer_id, None)
    }

    pub(crate) fn from_writer_with_request_id(
        correlation_id: u32,
        writer: &'a Arc<ResponseWriter>,
        authenticated_peer_id: Option<&'a crate::PeerId>,
        request_id: Option<u64>,
    ) -> Self {
        Self {
            correlation_id,
            request_id,
            authenticated_peer_id,
            sink: AskContextSink::DeferredWriter(writer),
            used: AtomicBool::new(false),
            reply_observer: None,
        }
    }

    pub fn correlation_id(&self) -> u32 {
        self.correlation_id
    }

    /// Stable caller-controlled identity carried out-of-band by an actor ask.
    /// `None` denotes a legacy ask without a request identity.
    pub fn request_id(&self) -> Option<u64> {
        self.request_id
    }

    pub fn authenticated_peer_id(&self) -> Option<&crate::PeerId> {
        self.authenticated_peer_id
    }

    /// Attach a nonblocking observer for the one reply eventually claimed by
    /// this ask. Existing callers that do not attach one keep the zero-copy
    /// response paths unchanged.
    pub fn with_reply_observer(mut self, observer: Arc<dyn AskReplyObserver>) -> Self {
        self.reply_observer = Some(observer);
        self
    }

    /// Borrow this ask's single-use claim. The responder cannot outlive its
    /// context; use a bound `ReplySession` token for deferred ownership.
    pub fn responder(&self) -> AskResponder<'_> {
        let sink = match &self.sink {
            AskContextSink::StreamHandle(stream) => AskResponseSink::StreamHandle(stream),
            AskContextSink::DeferredWriter(writer) => AskResponseSink::DeferredWriter(writer),
        };
        AskResponder {
            correlation_id: self.correlation_id,
            sink,
            used: ReplyClaim::Scoped(&self.used),
            reply_observer: self.reply_observer.clone(),
            observer_notified: false,
        }
    }

    /// Bind deferred reply storage once to this physical connection.
    pub fn reply_session(&self) -> Result<ReplySession> {
        self.responder().reply_session()
    }

    /// Move the sole reply claim into bounded, exact-session storage.
    pub fn into_forward_reply(self, session: &ReplySession) -> Result<ForwardReplyToken> {
        self.responder().into_forward_reply(session)
    }
}

#[derive(Clone)]
pub struct AskResponder<'a> {
    correlation_id: u32,
    sink: AskResponseSink<'a>,
    /// Shared, scoped single-use claim (see [`AskContext::used`]). Clones borrow
    /// the same field; production responders cannot outlive their context.
    used: ReplyClaim<'a>,
    reply_observer: Option<Arc<dyn AskReplyObserver>>,
    /// Kept with the claimed responder when an immediate enqueue is transferred
    /// into a lease, preventing fallback retry from notifying twice.
    observer_notified: bool,
}

/// Maximum preallocated deferred claims per physical reply endpoint.
pub const MAX_REPLY_SESSION_CAPACITY: usize = 65_552;
const DEFAULT_REPLY_SESSION_CAPACITY: usize = 64;
const TOKEN_READY: u64 = 1;
const TOKEN_CLAIMED: u64 = 2;
const TOKEN_GENERATION_MAX: u64 = u64::MAX >> 2;
static NEXT_REPLY_SESSION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A bounded deferred reply claim for one exact physical stream.
///
/// Copies are harmless: only one full-generation transition can claim or cancel
/// a token. The owning `ReplySession` must be retained until every accepted token
/// is completed or cancelled. Dropping a token alone does not release its slot;
/// queue owners must cancel rejected/drained work. Metadata lifetime is bounded
/// by that session; forwarding additionally applies its configured job deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForwardReplyToken {
    session_id: u64,
    index: usize,
    generation: u64,
}

struct ReplySessionEntry {
    correlation_id: u32,
    observer: Option<Arc<dyn AskReplyObserver>>,
    observer_notified: bool,
}

struct ReplySessionCell {
    state: std::sync::atomic::AtomicU64,
    entry: std::cell::UnsafeCell<Option<ReplySessionEntry>>,
}

// SAFETY: the free queue gives one admission writer exclusive EMPTY storage.
// Release publication makes the entry visible. A full-generation READY ->
// CLAIMED CAS gives exactly one guard access and exclusive destruction. Reuse
// is possible only after that guard destroys the entry and recycles its index.
unsafe impl Sync for ReplySessionCell {}

/// A cold-bound reply endpoint with fixed metadata and no per-token owner.
///
/// This endpoint retains one exact physical stream. It never resolves a peer
/// address or a replacement stream. Admission saturates with `WriteQueueFull`;
/// no hidden queue, allocation, timeout task, or eviction is created per reply.
pub struct ReplySession {
    session_id: u64,
    stream: Arc<LockFreeStreamHandle>,
    cells: Box<[ReplySessionCell]>,
    free: crossbeam_queue::ArrayQueue<usize>,
}

impl std::fmt::Debug for ReplySession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplySession")
            .field("session_id", &self.session_id)
            .field("stream_instance_id", &self.stream.instance_id())
            .field("capacity", &self.cells.len())
            .finish()
    }
}

impl ReplySession {
    fn new(stream: Arc<LockFreeStreamHandle>) -> Result<Self> {
        Self::with_capacity(stream, DEFAULT_REPLY_SESSION_CAPACITY)
    }

    fn with_capacity(stream: Arc<LockFreeStreamHandle>, capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > MAX_REPLY_SESSION_CAPACITY {
            return Err(GossipError::InvalidConfig(format!(
                "reply session capacity must be 1..={MAX_REPLY_SESSION_CAPACITY}"
            )));
        }
        let session_id = NEXT_REPLY_SESSION_ID
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| {
                if id == 0 { None } else { id.checked_add(1) }
            })
            .map_err(|_| GossipError::WriteQueueFull)?;
        let free = crossbeam_queue::ArrayQueue::new(capacity);
        let cells = (0..capacity)
            .map(|index| {
                let _ = free.push(index);
                ReplySessionCell {
                    state: std::sync::atomic::AtomicU64::new(0),
                    entry: std::cell::UnsafeCell::new(None),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            session_id,
            stream,
            cells,
            free,
        })
    }

    pub(crate) fn stream_handle(&self) -> &Arc<LockFreeStreamHandle> {
        &self.stream
    }

    pub fn instance_id(&self) -> u64 {
        self.stream.instance_id()
    }

    pub fn capacity(&self) -> usize {
        self.cells.len()
    }

    pub fn accepts_token(&self, token: &ForwardReplyToken) -> bool {
        token.session_id == self.session_id && token.index < self.cells.len()
    }

    fn admit(&self, responder: &AskResponder<'_>) -> Result<ForwardReplyToken> {
        let stream = responder.sink.stream_handle()?;
        if !Arc::ptr_eq(stream, &self.stream) {
            return Err(GossipError::InvalidConfig(
                "reply token belongs to a different physical stream".into(),
            ));
        }
        let index = self.free.pop().ok_or(GossipError::WriteQueueFull)?;
        let cell = &self.cells[index];
        let previous = cell.state.load(Ordering::Acquire);
        let generation = previous >> 2;
        if generation == TOKEN_GENERATION_MAX {
            // Retire the cell permanently. No stale token can ever match a
            // reissued generation, including after counter exhaustion.
            return Err(GossipError::WriteQueueFull);
        }
        if let Err(error) = claim_reply(&responder.used) {
            let recycled = self.free.push(index);
            debug_assert!(recycled.is_ok());
            return Err(error);
        }
        let generation = generation + 1;
        let entry = ReplySessionEntry {
            correlation_id: responder.correlation_id,
            observer: responder.reply_observer.clone(),
            observer_notified: responder.observer_notified,
        };
        // SAFETY: this free index belongs exclusively to this admission; no
        // token for the new generation exists before the following release.
        unsafe { *cell.entry.get() = Some(entry) };
        cell.state
            .store((generation << 2) | TOKEN_READY, Ordering::Release);
        Ok(ForwardReplyToken {
            session_id: self.session_id,
            index,
            generation,
        })
    }

    fn claim(&self, token: ForwardReplyToken) -> Result<ReplyTokenGuard<'_>> {
        if !self.accepts_token(&token) {
            return Err(GossipError::InvalidConfig(
                "reply token belongs to another endpoint".into(),
            ));
        }
        let cell = &self.cells[token.index];
        cell.state
            .compare_exchange(
                (token.generation << 2) | TOKEN_READY,
                (token.generation << 2) | TOKEN_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "reply token was completed, cancelled, or superseded",
                ))
            })?;
        Ok(ReplyTokenGuard {
            session: self,
            token,
        })
    }

    /// Cancel only this exact token. Stale, duplicate and foreign tokens cannot
    /// release a successor's resources or affect its writer.
    pub fn cancel(&self, token: ForwardReplyToken) {
        self.try_cancel(token);
    }

    fn try_cancel(&self, token: ForwardReplyToken) -> bool {
        if !self.accepts_token(&token) {
            return false;
        }
        let cell = &self.cells[token.index];
        if cell
            .state
            .compare_exchange(
                (token.generation << 2) | TOKEN_READY,
                (token.generation << 2) | TOKEN_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        drop(ReplyTokenGuard {
            session: self,
            token,
        });
        true
    }

    pub fn try_reply_owned(
        &self,
        token: ForwardReplyToken,
        payload: crate::ForwardPayload,
    ) -> Result<()> {
        let mut guard = self.claim(token)?;
        let correlation_id = guard.observe(&payload);
        self.stream
            .try_send_response_owned(correlation_id, payload, false)
    }

    /// Nonblocking terminal/control reply on the reserved immediate lane.
    pub fn try_reply_owned_immediate(
        &self,
        token: ForwardReplyToken,
        payload: crate::ForwardPayload,
    ) -> Result<()> {
        let mut guard = self.claim(token)?;
        let correlation_id = guard.observe(&payload);
        self.stream
            .try_send_response_owned(correlation_id, payload, true)
    }

    /// Retain exclusive token ownership if bounded enqueue rejects the payload.
    pub fn try_reply_owned_with_fallback(
        &self,
        token: ForwardReplyToken,
        payload: crate::ForwardPayload,
        immediate: bool,
    ) -> std::result::Result<(), ReplySessionTryError<'_>> {
        let mut guard = self
            .claim(token)
            .map_err(ReplySessionTryError::ClaimUnavailable)?;
        let correlation_id = guard.observe(&payload);
        match self
            .stream
            .try_send_response_owned(correlation_id, payload, immediate)
        {
            Ok(()) => Ok(()),
            Err(error) => Err(ReplySessionTryError::Enqueue(ReplySessionFallback {
                guard,
                error,
            })),
        }
    }

    pub async fn reply_owned(
        &self,
        token: ForwardReplyToken,
        payload: crate::ForwardPayload,
    ) -> Result<()> {
        let mut guard = self.claim(token)?;
        let correlation_id = guard.observe(&payload);
        self.stream
            .send_response_owned_wait(correlation_id, payload)
            .await
    }
}

/// Distinguishes an unavailable token from a claimed, retryable enqueue failure.
pub enum ReplySessionTryError<'a> {
    ClaimUnavailable(GossipError),
    Enqueue(ReplySessionFallback<'a>),
}

impl std::fmt::Debug for ReplySessionTryError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClaimUnavailable(error) => {
                f.debug_tuple("ClaimUnavailable").field(error).finish()
            }
            Self::Enqueue(fallback) => f.debug_tuple("Enqueue").field(fallback.error()).finish(),
        }
    }
}

/// A failed token enqueue retains its exact endpoint and exclusive generation.
pub struct ReplySessionFallback<'a> {
    guard: ReplyTokenGuard<'a>,
    error: GossipError,
}

impl ReplySessionFallback<'_> {
    pub fn error(&self) -> &GossipError {
        &self.error
    }

    pub fn try_reply_owned(
        mut self,
        payload: crate::ForwardPayload,
        immediate: bool,
    ) -> Result<()> {
        let correlation_id = self.guard.observe(&payload);
        self.guard
            .session
            .stream
            .try_send_response_owned(correlation_id, payload, immediate)
    }

    pub async fn reply_owned(mut self, payload: crate::ForwardPayload) -> Result<()> {
        let correlation_id = self.guard.observe(&payload);
        self.guard
            .session
            .stream
            .send_response_owned_wait(correlation_id, payload)
            .await
    }
}

/// Scoped queue-publication transaction. An uncommitted rejection restores the
/// original context claim only after cancelling its exact unpublished token.
pub struct ForwardReplyAdmission<'a> {
    session: &'a ReplySession,
    used: &'a AtomicBool,
    token: ForwardReplyToken,
    committed: bool,
}

impl ForwardReplyAdmission<'_> {
    pub fn token(&self) -> ForwardReplyToken {
        self.token
    }

    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ForwardReplyAdmission<'_> {
    fn drop(&mut self) {
        if !self.committed && self.session.try_cancel(self.token) {
            self.used.store(false, Ordering::Release);
        }
    }
}

struct ReplyTokenGuard<'a> {
    session: &'a ReplySession,
    token: ForwardReplyToken,
}

impl ReplyTokenGuard<'_> {
    fn observe(&mut self, payload: &crate::ForwardPayload) -> u32 {
        let cell = &self.session.cells[self.token.index];
        // SAFETY: the guard uniquely owns CLAIMED for this immutable token
        // generation. Neither cancellation nor another claimant can touch it.
        let entry = unsafe { (&mut *cell.entry.get()).as_mut().unwrap() };
        if !entry.observer_notified
            && payload.len() <= crate::MAX_STREAM_SIZE
            && let Some(observer) = &entry.observer
        {
            // Set first so an observer panic cannot produce a second callback.
            entry.observer_notified = true;
            observer.reply_claimed_borrowed(ReplyPayloadRef::new(
                None,
                payload.as_ref(),
                payload.len(),
            ));
        }
        entry.correlation_id
    }
}

impl Drop for ReplyTokenGuard<'_> {
    fn drop(&mut self) {
        let cell = &self.session.cells[self.token.index];
        // SAFETY: only this full-generation claimant may destroy the entry.
        // Drop payload metadata before publishing availability to admission.
        let entry = unsafe { (&mut *cell.entry.get()).take() };
        cell.state
            .store(self.token.generation << 2, Ordering::Release);
        let recycled = self.session.free.push(self.token.index);
        debug_assert!(recycled.is_ok(), "reply token recycles exactly once");
        // Observer destruction is user code. Recycling first and containing an
        // unwind preserves unrelated queue cleanup and cancellation progress.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(entry)));
    }
}

impl Drop for ReplySession {
    fn drop(&mut self) {
        for cell in &mut self.cells {
            // Exclusive endpoint destruction excludes every borrowed guard.
            // Remaining numeric tokens carry no dereferenceable pointer.
            cell.entry.get_mut().take();
        }
    }
}

/// Exclusive fallback ownership after an immediate reply was rejected before
/// it entered the writer. The original responder's shared single-reply claim
/// remains held, so sibling responders cannot race the selected fallback.
///
/// Constructed only when the caller itself claimed the single-use reply
/// guard and a subsequent enqueue attempt was rejected — never when the
/// claim was already taken by a sibling responder (see [`TryReplyError`]).
pub struct ImmediateReplyFallback<'a> {
    pub(crate) responder: AskResponder<'a>,
    error: GossipError,
}

impl<'a> ImmediateReplyFallback<'a> {
    pub(crate) fn from_claimed_responder(responder: AskResponder<'a>, error: GossipError) -> Self {
        Self { responder, error }
    }

    /// The immediate-enqueue error that selected this fallback path.
    pub fn error(&self) -> &GossipError {
        &self.error
    }

    /// Retry through the ordinary nonblocking response queue while retaining
    /// exclusive ownership of the ask correlation.
    pub fn try_reply_bytes(self, response: crate::ForwardPayload) -> Result<()> {
        self.responder
            .sink
            .try_send_response_bytes(self.responder.correlation_id, response)
    }

    /// Retry through the reserved immediate queue while retaining exclusive
    /// ownership of the ask correlation.
    pub fn try_reply_bytes_immediate(self, response: crate::ForwardPayload) -> Result<()> {
        self.responder
            .sink
            .try_send_response_bytes_immediate(self.responder.correlation_id, response)
    }

    /// Fall back to the asynchronous response path while retaining exclusive
    /// ownership of the ask correlation.
    pub async fn reply_bytes(self, response: crate::ForwardPayload) -> Result<()> {
        self.responder
            .sink
            .send_response_bytes(self.responder.correlation_id, response)
            .await
    }

    /// Retry with a complete owner, preserving its retained-capacity receipt.
    pub fn try_reply_owned(self, response: crate::ForwardPayload) -> Result<()> {
        self.responder
            .sink
            .try_send_response_owned(self.responder.correlation_id, response, false)
    }

    /// Retry with a complete owner on the reserved immediate response queue.
    pub fn try_reply_owned_immediate(self, response: crate::ForwardPayload) -> Result<()> {
        self.responder
            .sink
            .try_send_response_owned(self.responder.correlation_id, response, true)
    }

    /// Await bounded admission without cloning or normalizing the complete owner.
    pub async fn reply_owned(self, response: crate::ForwardPayload) -> Result<()> {
        self.responder
            .sink
            .send_response_owned(self.responder.correlation_id, response)
            .await
    }

    /// Transfer this already-claimed fallback into a bounded transport lease.
    /// The transfer path deliberately does not call `claim_reply` again.
    pub fn try_into_reply_lease<'s>(
        self,
        session: &'s crate::ReplyDeliverySession<'_>,
        max_reply_bytes: usize,
    ) -> std::result::Result<crate::ReplyLease<'s>, crate::ReplyLeaseAdmissionError<'a>> {
        crate::reply_lease::reserve_for_responder(self.responder, session, max_reply_bytes, true)
    }
}

/// Outcome of a `*_with_fallback` reply attempt that failed to send inline.
///
/// The two failure modes are not interchangeable: a claim that was already
/// taken by a sibling responder means a reply for this ask is already sent
/// or in flight, and retrying would send a duplicate on the same correlation
/// id. A rejected enqueue means this call owns the sole claim and the bytes
/// were never sent, so it is safe — and necessary — to retry.
pub enum TryReplyError<'a> {
    /// The single-use reply guard was already claimed by another responder
    /// for this ask. There is nothing to retry; the reply must be dropped.
    ClaimUnavailable(GossipError),
    /// This call claimed the guard, but the nonblocking enqueue was
    /// rejected. The fallback retains exclusive ownership and may retry.
    Enqueue(ImmediateReplyFallback<'a>),
}

impl TryReplyError<'_> {
    /// The underlying error, regardless of which case produced it.
    pub fn error(&self) -> &GossipError {
        match self {
            Self::ClaimUnavailable(error) => error,
            Self::Enqueue(fallback) => fallback.error(),
        }
    }
}

impl<'a> AskResponder<'a> {
    pub fn try_reply_lease<'s>(
        self,
        session: &'s crate::ReplyDeliverySession<'_>,
        max_reply_bytes: usize,
    ) -> std::result::Result<crate::ReplyLease<'s>, crate::ReplyLeaseAdmissionError<'a>> {
        crate::reply_lease::reserve_for_responder(self, session, max_reply_bytes, false)
    }

    pub(crate) fn claim_for_lease(&self) -> Result<()> {
        claim_reply(&self.used)
    }

    pub(crate) fn reply_observer_for_lease(&self) -> (Option<Arc<dyn AskReplyObserver>>, bool) {
        (self.reply_observer.clone(), self.observer_notified)
    }

    pub(crate) fn stream_handle_for_lease(&self) -> Result<&Arc<LockFreeStreamHandle>> {
        self.sink.stream_handle()
    }

    /// Cold setup for a deferred reply endpoint. The endpoint retains this
    /// exact stream once; per-reply tokens never clone it or resolve an address.
    pub fn reply_session(&self) -> Result<ReplySession> {
        ReplySession::new(Arc::clone(self.sink.stream_handle()?))
    }

    pub fn reply_session_with_capacity(&self, capacity: usize) -> Result<ReplySession> {
        ReplySession::with_capacity(Arc::clone(self.sink.stream_handle()?), capacity)
    }

    /// Reserve a deferred claim without losing sibling retry on rejected queue
    /// publication. Commit only after the queue has accepted the numeric token.
    pub fn prepare_forward_reply<'b>(
        &'b self,
        session: &'b ReplySession,
    ) -> Result<ForwardReplyAdmission<'b>> {
        let token = session.admit(self)?;
        Ok(ForwardReplyAdmission {
            session,
            used: &self.used,
            token,
            committed: false,
        })
    }

    pub fn into_forward_reply(self, session: &ReplySession) -> Result<ForwardReplyToken> {
        session.admit(&self)
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn from_stream_handle(
        correlation_id: u32,
        stream_handle: Arc<LockFreeStreamHandle>,
        used: Arc<AtomicBool>,
    ) -> Self {
        Self::from_stream_handle_with_observer(correlation_id, stream_handle, used, None)
    }

    #[cfg(feature = "test-helpers")]
    pub fn from_stream_handle_for_test(
        correlation_id: u32,
        stream_handle: Arc<LockFreeStreamHandle>,
        used: Arc<AtomicBool>,
    ) -> Self {
        Self::from_stream_handle(correlation_id, stream_handle, used)
    }

    #[cfg(any(test, feature = "test-helpers"))]
    fn from_stream_handle_with_observer(
        correlation_id: u32,
        stream_handle: Arc<LockFreeStreamHandle>,
        used: Arc<AtomicBool>,
        reply_observer: Option<Arc<dyn AskReplyObserver>>,
    ) -> Self {
        Self {
            correlation_id,
            sink: AskResponseSink::OwnedStreamHandle(stream_handle),
            used: ReplyClaim::Owned(used),
            reply_observer,
            observer_notified: false,
        }
    }

    /// Attach an explicitly requested observer without changing the single-use
    /// claim or a previously recorded notification outcome.
    pub fn with_reply_observer(mut self, observer: Arc<dyn AskReplyObserver>) -> Self {
        self.reply_observer = Some(observer);
        self
    }

    pub fn correlation_id(&self) -> u32 {
        self.correlation_id
    }

    /// Reply with a proven complete owner, without opaque-Bytes normalization.
    /// Waits for bounded writer-queue admission while retaining the same owner.
    /// Success means the writer owns this response; cancellation before admission
    /// drops the response without publishing a partial stream.
    pub async fn reply_owned(mut self, response: crate::ForwardPayload) -> Result<()> {
        claim_reply(&self.used)?;
        let len = response.len();
        self.notify_observer_parts(None, response.as_ref(), len);
        self.sink
            .send_response_owned(self.correlation_id, response)
            .await
    }

    /// Nonblocking inline reply retaining a bounded, proven complete owner.
    pub fn try_reply_owned(mut self, response: crate::ForwardPayload) -> Result<()> {
        claim_reply(&self.used)?;
        self.notify_observer_parts(None, response.as_ref(), response.len());
        self.sink
            .try_send_response_owned(self.correlation_id, response, false)
    }

    /// Immediate reply retaining the original owner and its full capacity.
    pub fn try_reply_owned_immediate(mut self, response: crate::ForwardPayload) -> Result<()> {
        claim_reply(&self.used)?;
        self.notify_observer_parts(None, response.as_ref(), response.len());
        self.sink
            .try_send_response_owned(self.correlation_id, response, true)
    }

    /// Try complete-owner admission while retaining exclusive fallback authority.
    /// The rejected payload is dropped; a caller needing to retain that exact
    /// payload through backpressure should use [`Self::reply_owned`] instead.
    pub fn try_reply_owned_with_fallback(
        mut self,
        response: crate::ForwardPayload,
    ) -> std::result::Result<(), TryReplyError<'a>> {
        if let Err(error) = claim_reply(&self.used) {
            return Err(TryReplyError::ClaimUnavailable(error));
        }
        self.notify_observer_parts(None, response.as_ref(), response.len());
        match self
            .sink
            .try_send_response_owned(self.correlation_id, response, false)
        {
            Ok(()) => Ok(()),
            Err(error) => Err(TryReplyError::Enqueue(ImmediateReplyFallback {
                responder: self,
                error,
            })),
        }
    }

    pub async fn reply(mut self, response: crate::ForwardPayload) -> Result<()> {
        claim_reply(&self.used)?;
        self.notify_observer_parts(None, response.as_ref(), response.len());
        self.sink
            .send_response_bytes(self.correlation_id, response)
            .await
    }

    pub async fn reply_bytes(self, response: crate::ForwardPayload) -> Result<()> {
        self.reply(response).await
    }

    /// A rejected enqueue still consumes the reply claim, so the response is
    /// lost if the caller does not retry from the returned error. Use
    /// [`Self::try_reply_bytes_with_fallback`] when the caller needs an
    /// exclusive retry or fallback path.
    pub fn try_reply_bytes(mut self, response: crate::ForwardPayload) -> Result<()> {
        claim_reply(&self.used)?;
        self.notify_observer_parts(None, response.as_ref(), response.len());
        self.sink
            .try_send_response_bytes(self.correlation_id, response)
    }

    /// Try the ordinary nonblocking queue, distinguishing a claim already
    /// held by a sibling responder (nothing to retry — drop it) from a
    /// rejected enqueue while this call owns the claim (retryable via the
    /// returned fallback, which cannot be raced by a sibling).
    pub fn try_reply_bytes_with_fallback(
        mut self,
        response: crate::ForwardPayload,
    ) -> std::result::Result<(), TryReplyError<'a>> {
        if let Err(error) = claim_reply(&self.used) {
            return Err(TryReplyError::ClaimUnavailable(error));
        }
        self.notify_observer_parts(None, response.as_ref(), response.len());
        match self
            .sink
            .try_send_response_bytes(self.correlation_id, response)
        {
            Ok(()) => Ok(()),
            Err(error) => Err(TryReplyError::Enqueue(ImmediateReplyFallback {
                responder: self,
                error,
            })),
        }
    }

    /// Await bounded complete-owner admission without a speculative clone.
    /// Success transfers the owner to the writer. Cancellation before admission
    /// drops it; connection closure and impossible retained capacity return errors.
    pub async fn reply_bytes_guaranteed(self, response: crate::ForwardPayload) -> Result<()> {
        self.reply_owned(response).await
    }

    /// Try to reply through the connection's immediate nonblocking queue.
    ///
    /// This is intended for small control-plane responses whose delivery must
    /// not be starved by ordinary gossip/control traffic. It still never
    /// awaits or creates a detached task. A rejected enqueue still consumes
    /// the reply claim, preserving at-most-once ownership across sibling
    /// responders. Use [`Self::try_reply_bytes_immediate_with_fallback`] when
    /// the caller needs an exclusive retry or fallback path.
    pub fn try_reply_bytes_immediate(mut self, response: crate::ForwardPayload) -> Result<()> {
        claim_reply(&self.used)?;
        self.notify_observer_parts(None, response.as_ref(), response.len());
        self.sink
            .try_send_response_bytes_immediate(self.correlation_id, response)
    }

    /// Try the reserved immediate queue, distinguishing a claim already held
    /// by a sibling responder (nothing to retry — drop it) from a rejected
    /// enqueue while this call owns the claim (retryable via the returned
    /// fallback, which cannot be raced by a sibling).
    pub fn try_reply_bytes_immediate_with_fallback(
        mut self,
        response: crate::ForwardPayload,
    ) -> std::result::Result<(), TryReplyError<'a>> {
        if let Err(error) = claim_reply(&self.used) {
            return Err(TryReplyError::ClaimUnavailable(error));
        }
        self.notify_observer_parts(None, response.as_ref(), response.len());
        match self
            .sink
            .try_send_response_bytes_immediate(self.correlation_id, response)
        {
            Ok(()) => Ok(()),
            Err(error) => Err(TryReplyError::Enqueue(ImmediateReplyFallback {
                responder: self,
                error,
            })),
        }
    }

    pub async fn reply_typed<M>(mut self, value: &M) -> Result<()>
    where
        M: crate::typed::WireEncode,
    {
        claim_reply(&self.used)?;
        let payload = crate::typed::encode_typed_pooled(value)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<M>(payload);
        if self.reply_observer.is_some() && payload_len <= crate::MAX_STREAM_SIZE {
            self.notify_observer_parts(
                prefix.as_ref().map(|prefix| prefix.as_slice()),
                payload.chunk(),
                payload_len,
            );
        }
        self.sink
            .send_response_pooled(self.correlation_id, payload, prefix, payload_len)
            .await
    }

    #[inline]
    fn notify_observer_parts(&mut self, prefix: Option<&[u8]>, payload: &[u8], len: usize) {
        if len <= crate::MAX_STREAM_SIZE
            && !self.observer_notified
            && let Some(observer) = &self.reply_observer
        {
            observer.reply_claimed_borrowed(ReplyPayloadRef::new(prefix, payload, len));
            self.observer_notified = true;
        }
    }
}

impl std::fmt::Debug for AskResponder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AskResponder")
            .field("correlation_id", &self.correlation_id)
            .finish()
    }
}

#[cfg(test)]
mod contract_tests {
    use super::AskResponder;

    // Authority-voter replies must have an explicit immediate, nonblocking
    // response path so a saturated ordinary control queue cannot starve the
    // quorum round-trip.
    #[allow(dead_code)]
    fn immediate_reply_api_is_available(responder: AskResponder) {
        let _ = responder.try_reply_bytes_immediate(crate::ForwardPayload::from_static(b""));
    }
}

pub(crate) struct ResponseWriter {
    addr: SocketAddr,
    stream_handle: OnceLock<Arc<LockFreeStreamHandle>>,
}

impl ResponseWriter {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream_handle: OnceLock::new(),
        }
    }

    /// Bind once to the physical stream that created this response writer.
    /// A delayed responder must never resolve a replacement stream at send time.
    pub(crate) fn bind_stream_handle(
        &self,
        stream_handle: Arc<LockFreeStreamHandle>,
    ) -> Result<()> {
        self.stream_handle.set(stream_handle).map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "response writer is already bound to its physical stream",
            ))
        })
    }

    pub(crate) fn stream_handle(&self) -> Result<&Arc<LockFreeStreamHandle>> {
        self.stream_handle.get().ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("response stream handle is not available for {}", self.addr),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_pool::{BufferConfig, ChannelId};
    use std::sync::Mutex;

    #[tokio::test]
    async fn session_tokens_reject_foreign_and_stale_claims_without_owner_churn() {
        use crate::ask_forwarder::allocation_evidence;

        let (io, mut peer) = tokio::io::duplex(4096);
        let (handle, writer_task, _) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:40704".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let handle = Arc::new(handle);
        let first_context = AskContext::from_stream_handle_with_request_id(1, &handle, None, None);
        let session = first_context
            .responder()
            .reply_session_with_capacity(1)
            .unwrap();
        let foreign = first_context
            .responder()
            .reply_session_with_capacity(1)
            .unwrap();
        let references = Arc::strong_count(&handle);
        let (token, counts) = allocation_evidence::measure(|| {
            let responder = first_context.responder();
            let rejected = responder.prepare_forward_reply(&session).unwrap();
            drop(rejected);
            assert!(!first_context.used.load(Ordering::Acquire));
            responder.into_forward_reply(&session).unwrap()
        });
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        assert_eq!(Arc::strong_count(&handle), references);
        let successor_context =
            AskContext::from_stream_handle_with_request_id(2, &handle, None, None);
        assert!(matches!(
            successor_context.responder().into_forward_reply(&session),
            Err(GossipError::WriteQueueFull)
        ));
        assert!(!successor_context.used.load(Ordering::Acquire));
        assert!(matches!(
            foreign.try_reply_owned(token, crate::ForwardPayload::from_static(b"foreign")),
            Err(GossipError::InvalidConfig(_))
        ));
        session.cancel(token);
        let successor = successor_context
            .responder()
            .into_forward_reply(&session)
            .unwrap();
        assert_ne!(successor.generation, token.generation);
        let (_, stale_counts) = allocation_evidence::measure(|| session.cancel(token));
        assert_eq!(stale_counts.allocations, 0);
        assert_eq!(
            session.free.len(),
            0,
            "stale cancellation cannot release the successor"
        );
        session
            .reply_owned(successor, crate::ForwardPayload::from_static(b"successor"))
            .await
            .unwrap();
        let mut wire = [0_u8; framing::ASK_RESPONSE_FRAME_HEADER_LEN + b"successor".len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read_exact(&mut peer, &mut wire),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            &wire[framing::ASK_RESPONSE_FRAME_HEADER_LEN..],
            b"successor"
        );
        assert_eq!(session.free.len(), 1);
        handle.shutdown();
        let _ = writer_task.await;
    }

    #[tokio::test]
    async fn reply_token_generation_exhaustion_never_reissues_or_claims_context() {
        let (io, _peer) = tokio::io::duplex(4096);
        let (handle, writer_task, _) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:40705".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let handle = Arc::new(handle);
        let context = AskContext::from_stream_handle_with_request_id(1, &handle, None, None);
        let session = context.responder().reply_session_with_capacity(1).unwrap();
        session.cells[0]
            .state
            .store(TOKEN_GENERATION_MAX << 2, Ordering::Release);
        assert!(matches!(
            context.responder().into_forward_reply(&session),
            Err(GossipError::WriteQueueFull)
        ));
        assert_eq!(session.free.len(), 0);
        assert!(!context.used.load(Ordering::Acquire));
        assert!(matches!(
            context.responder().into_forward_reply(&session),
            Err(GossipError::WriteQueueFull)
        ));
        assert!(!context.used.load(Ordering::Acquire));
        handle.shutdown();
        let _ = writer_task.await;
    }

    #[tokio::test]
    async fn owned_reply_observer_borrows_original_storage_and_preserves_claim() {
        use std::sync::atomic::AtomicUsize;

        struct BorrowedObserver(AtomicUsize);

        impl AskReplyObserver for BorrowedObserver {
            fn reply_claimed(&self, _: Bytes) {
                panic!("owned reply must use the borrowed observer seam");
            }

            fn reply_claimed_borrowed(&self, payload: ReplyPayloadRef<'_>) {
                self.0
                    .store(payload.payload().as_ptr() as usize, Ordering::Release);
            }
        }

        let (io, mut peer) = tokio::io::duplex(4096);
        let (stream, task, _) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:40695".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let stream = Arc::new(stream);
        let observer = Arc::new(BorrowedObserver(AtomicUsize::new(0)));
        let responder =
            AskResponder::from_stream_handle(73, stream.clone(), Arc::new(AtomicBool::new(false)))
                .with_reply_observer(observer.clone());
        let sibling = responder.clone();
        let payload = crate::ForwardPayload::from(b"owned-reply".to_vec());
        let pointer = payload.as_ref().as_ptr() as usize;
        responder.reply_owned(payload).await.unwrap();
        assert_eq!(observer.0.load(Ordering::Acquire), pointer);
        let duplicate = sibling
            .reply_owned(crate::ForwardPayload::from_static(b"duplicate"))
            .await;
        assert!(matches!(
            duplicate,
            Err(GossipError::Network(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists
        ));
        let mut wire = [0_u8; framing::ASK_RESPONSE_FRAME_HEADER_LEN + b"owned-reply".len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read_exact(&mut peer, &mut wire),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            &wire[framing::ASK_RESPONSE_FRAME_HEADER_LEN..],
            b"owned-reply"
        );
        stream.shutdown();
        let _ = task.await;
    }

    #[derive(Default)]
    struct RecordingReplyObserver {
        payloads: Mutex<Vec<Bytes>>,
    }

    impl AskReplyObserver for RecordingReplyObserver {
        fn reply_claimed(&self, payload: Bytes) {
            self.payloads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(payload);
        }
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct ObservedTypedReply {
        value: u64,
    }
    crate::wire_type!(ObservedTypedReply, "ask_responder::ObservedTypedReply");

    #[tokio::test]
    async fn owned_deferred_fallback_preserves_claim_and_recovers_after_binding() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12353".parse().unwrap()));
        let context = AskContext::from_writer(17, &writer, None);
        let sibling = context.responder();
        let fallback = match context
            .responder()
            .try_reply_owned_with_fallback(crate::ForwardPayload::from_static(b"first"))
        {
            Err(TryReplyError::Enqueue(fallback)) => fallback,
            Ok(()) => panic!("unbound writer must reject enqueue"),
            Err(TryReplyError::ClaimUnavailable(error)) => {
                panic!("first responder must own the claim: {error:?}")
            }
        };
        assert!(is_duplicate(&sibling.try_reply_owned_immediate(
            crate::ForwardPayload::from_static(b"duplicate")
        )));

        let (io, mut peer) = tokio::io::duplex(4096);
        let (handle, writer_task, _) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:12353".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let handle = Arc::new(handle);
        writer.bind_stream_handle(Arc::clone(&handle)).unwrap();
        fallback
            .try_reply_owned_immediate(crate::ForwardPayload::from_static(b"recovered"))
            .unwrap();
        let mut wire = [0_u8; framing::ASK_RESPONSE_FRAME_HEADER_LEN + b"recovered".len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read_exact(&mut peer, &mut wire),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            &wire[framing::ASK_RESPONSE_FRAME_HEADER_LEN..],
            b"recovered"
        );
        handle.shutdown();
        let _ = writer_task.await;
    }

    #[tokio::test]
    async fn deferred_writer_rebinding_cannot_retarget_an_old_responder() {
        let address = "127.0.0.1:12355".parse().unwrap();
        let writer = Arc::new(ResponseWriter::new(address));
        let context = AskContext::from_writer(19, &writer, None);
        let old_responder = context.responder();
        let (first_io, _first_peer) = tokio::io::duplex(4096);
        let (second_io, _second_peer) = tokio::io::duplex(4096);
        let (first, first_task, _) = LockFreeStreamHandle::new(
            first_io,
            address,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let (second, second_task, _) = LockFreeStreamHandle::new(
            second_io,
            address,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let first = Arc::new(first);
        let second = Arc::new(second);
        writer.bind_stream_handle(Arc::clone(&first)).unwrap();
        let rejected = writer.bind_stream_handle(Arc::clone(&second));
        assert!(matches!(
            rejected,
            Err(GossipError::Network(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert!(Arc::ptr_eq(&writer.stream_handle().unwrap(), &first));
        assert!(Arc::ptr_eq(
            &old_responder.stream_handle_for_lease().unwrap(),
            &first
        ));
        first.shutdown();
        second.shutdown();
        let _ = first_task.await;
        let _ = second_task.await;
    }

    #[test]
    fn owned_fallback_never_confuses_a_duplicate_with_rejected_enqueue() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12354".parse().unwrap()));
        let context = AskContext::from_writer(18, &writer, None);
        let first = context.responder();
        let second = context.responder();
        assert!(matches!(
            first.try_reply_owned_with_fallback(crate::ForwardPayload::from_static(b"first")),
            Err(TryReplyError::Enqueue(_))
        ));
        assert!(matches!(
            second.try_reply_owned_with_fallback(crate::ForwardPayload::from_static(b"second")),
            Err(TryReplyError::ClaimUnavailable(_))
        ));
    }

    #[test]
    fn ask_context_exposes_authenticated_peer_id() {
        let peer_id = crate::KeyPair::new_for_testing("ask-context-peer").peer_id();
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12345".parse().unwrap()));
        let context = AskContext::from_writer_with_request_id(7, &writer, Some(&peer_id), Some(42));

        assert_eq!(context.correlation_id(), 7);
        assert_eq!(context.request_id(), Some(42));
        assert_eq!(context.authenticated_peer_id(), Some(&peer_id));
    }

    #[test]
    fn tell_context_exposes_authenticated_peer_id() {
        let peer_id = crate::KeyPair::new_for_testing("tell-context-peer").peer_id();
        let context = TellContext::new(Some(&peer_id));

        assert_eq!(context.authenticated_peer_id(), Some(&peer_id));
    }

    #[test]
    fn legacy_owned_reply_observer_remains_source_compatible_and_one_shot() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12349".parse().unwrap()));
        let observer = Arc::new(RecordingReplyObserver::default());
        let context = AskContext::from_writer(14, &writer, None)
            .with_reply_observer(Arc::clone(&observer) as Arc<dyn AskReplyObserver>);

        // The writer has no bound stream, so the transport enqueue fails after
        // the claim. The observer must still record the response: a caller may
        // need the bytes to replay a side effect whose direct reply was lost.
        let first_result = context
            .responder()
            .try_reply_bytes(crate::ForwardPayload::from_static(b"reply"));
        assert!(first_result.is_err());

        let duplicate_result = context
            .responder()
            .try_reply_bytes(crate::ForwardPayload::from_static(b"again"));
        assert!(is_duplicate(&duplicate_result));

        let payloads = observer
            .payloads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(payloads.as_slice(), [Bytes::from_static(b"reply")]);
    }

    #[tokio::test]
    async fn reply_observer_copies_pooled_typed_wire_payload() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12350".parse().unwrap()));
        let observer = Arc::new(RecordingReplyObserver::default());
        let context = AskContext::from_writer(15, &writer, None)
            .with_reply_observer(Arc::clone(&observer) as Arc<dyn AskReplyObserver>);
        let reply = ObservedTypedReply { value: 7 };

        // The response writer is intentionally unbound; the observer still
        // receives the complete type-prefixed wire payload before enqueue.
        let result = context.responder().reply_typed(&reply).await;
        assert!(result.is_err());

        let payloads = observer
            .payloads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let encoded = crate::typed::encode_typed_pooled(&reply).unwrap();
        let (payload, prefix, _) = crate::typed::typed_payload_parts::<ObservedTypedReply>(encoded);
        let expected = pooled_payload_into_bytes(prefix, payload);
        assert_eq!(payloads.as_slice(), [expected]);
    }

    fn is_duplicate(result: &Result<()>) -> bool {
        matches!(
            result,
            Err(GossipError::Network(e)) if e.kind() == std::io::ErrorKind::AlreadyExists
        )
    }

    #[test]
    fn second_reply_for_same_ask_is_rejected() {
        // ACTOR_REM_2 R15: responders minted from one AskContext (and their
        // clones) share a single-use guard, so only the first reply may reach
        // the wire — a duplicate Response frame could otherwise complete an
        // unrelated ask once the connection's correlation id wraps.
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12346".parse().unwrap()));
        let context = AskContext::from_writer(7, &writer, None);

        // Two independent responders from the same context.
        let first = context.responder();
        let second = context.responder();

        // The first claims the guard (the send itself only fails here because no
        // stream handle is bound in this unit test — the guard is still taken).
        let first_result = first.try_reply_bytes(crate::ForwardPayload::from_static(b"a"));
        let second_result = second.try_reply_bytes(crate::ForwardPayload::from_static(b"b"));

        assert!(
            !is_duplicate(&first_result),
            "first reply must not be treated as a duplicate: {first_result:?}"
        );
        assert!(
            is_duplicate(&second_result),
            "second reply from a sibling responder must be rejected: {second_result:?}"
        );

        // A clone of a responder shares the guard too.
        let ctx2 = AskContext::from_writer(9, &writer, None);
        let responder = ctx2.responder();
        let clone = responder.clone();
        let _ = responder.try_reply_bytes(crate::ForwardPayload::from_static(b"x"));
        let clone_result = clone.try_reply_bytes(crate::ForwardPayload::from_static(b"y"));
        assert!(
            is_duplicate(&clone_result),
            "a clone must not send a second reply: {clone_result:?}"
        );
    }

    #[test]
    fn rejected_immediate_reply_retains_exclusive_fallback_ownership() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12347".parse().unwrap()));
        let context = AskContext::from_writer(11, &writer, None);

        let immediate = context.responder();
        let sibling = context.responder();
        let immediate_result =
            immediate.try_reply_bytes_immediate(crate::ForwardPayload::from_static(b"fast"));
        assert!(
            !is_duplicate(&immediate_result),
            "the owner sees the underlying enqueue failure, not a duplicate"
        );
        assert!(
            is_duplicate(&sibling.try_reply_bytes(crate::ForwardPayload::from_static(b"sibling"))),
            "a rejected immediate enqueue must not release ownership to a sibling"
        );

        let retry_context = AskContext::from_writer(12, &writer, None);
        let retry_sibling = retry_context.responder();
        let fallback = match retry_context
            .responder()
            .try_reply_bytes_immediate_with_fallback(crate::ForwardPayload::from_static(b"fast"))
        {
            Ok(()) => panic!("unbound writer must reject immediate enqueue"),
            Err(TryReplyError::ClaimUnavailable(error)) => {
                panic!("this call owns the claim, not a stale sibling: {error:?}")
            }
            Err(TryReplyError::Enqueue(fallback)) => fallback,
        };
        assert!(
            is_duplicate(
                &retry_sibling.try_reply_bytes(crate::ForwardPayload::from_static(b"sibling"))
            ),
            "fallback ownership must remain exclusive to the original responder"
        );
        let retry_result =
            fallback.try_reply_bytes(crate::ForwardPayload::from_static(b"fallback"));
        assert!(
            !is_duplicate(&retry_result),
            "the original owner may choose a nonblocking fallback path"
        );
    }

    /// A sibling responder whose reply arrives after another sibling already
    /// claimed the shared guard must see `ClaimUnavailable`, never `Enqueue` —
    /// otherwise a caller that retries every `Err` through the fallback path
    /// would send a duplicate response for an ask that was already answered.
    #[tokio::test]
    async fn lease_transfer_does_not_re_notify_observer() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12351".parse().unwrap()));
        let observer = Arc::new(RecordingReplyObserver::default());
        let context = AskContext::from_writer(16, &writer, None)
            .with_reply_observer(Arc::clone(&observer) as Arc<dyn AskReplyObserver>);
        let fallback = match context
            .responder()
            .try_reply_bytes_with_fallback(crate::ForwardPayload::from_static(b"first"))
        {
            Err(TryReplyError::Enqueue(fallback)) => fallback,
            Ok(()) => panic!("an unbound writer must reject the initial enqueue"),
            Err(TryReplyError::ClaimUnavailable(error)) => {
                panic!("the responder should own the claim: {error:?}")
            }
        };

        let (io, _peer) = tokio::io::duplex(4096);
        let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:12352".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        writer.bind_stream_handle(Arc::new(handle)).unwrap();
        let budget =
            crate::ReplyDeliveryBudget::new(1, 32, crate::ReplyPayload::from_static(b"cancelled"))
                .unwrap();
        let reply_session = context.reply_session().unwrap();
        let delivery = budget.bind(&reply_session).unwrap();
        let lease = fallback.try_into_reply_lease(&delivery, 9).unwrap();
        lease
            .try_reply_bytes(crate::ReplyPayload::from_static(b"second"))
            .unwrap();
        assert_eq!(
            observer
                .payloads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1,
            "transferring an already-observed fallback into a lease must not notify twice"
        );

        writer.stream_handle().unwrap().shutdown();
        let _ = writer_task.await;
    }

    #[tokio::test]
    async fn polled_reply_future_drop_cancels_after_publication() {
        let (io, _peer) = tokio::io::duplex(4096);
        let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:12353".parse().expect("test address"),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let handle = Arc::new(handle);
        let budget =
            crate::ReplyDeliveryBudget::new(1, 32, crate::ReplyPayload::from_static(b"cancelled"))
                .expect("valid budget");
        let reply_session = AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
            .reply_session()
            .unwrap();
        let delivery = budget.bind(&reply_session).unwrap();
        let lease = AskResponder::from_stream_handle(
            17,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        )
        .try_reply_lease(&delivery, 9)
        .expect("lease admission");
        let record = lease.record.clone();
        {
            let future = lease.reply_bytes(crate::ReplyPayload::from_static(b"reply"));
            tokio::pin!(future);
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            assert!(matches!(
                future.as_mut().poll(&mut cx),
                std::task::Poll::Pending
            ));
            assert!(
                record.normal_payload().is_some(),
                "poll must publish before waiting"
            );
        }
        assert!(
            record.is_cancelled(),
            "dropping a polled reply must cancel the lease"
        );

        handle.shutdown();
        let _ = writer_task.await;
    }

    #[test]
    fn claim_already_taken_by_sibling_is_not_an_enqueue_fallback() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12348".parse().unwrap()));
        let context = AskContext::from_writer(13, &writer, None);

        let first = context.responder();
        let second = context.responder();

        // First reply claims the shared guard (its send fails only because no
        // stream handle is bound in this unit test).
        let _ = first.try_reply_bytes_with_fallback(crate::ForwardPayload::from_static(b"first"));

        match second.try_reply_bytes_with_fallback(crate::ForwardPayload::from_static(b"second")) {
            Ok(()) => panic!("a claim already held by a sibling must not succeed"),
            Err(TryReplyError::Enqueue(_)) => panic!(
                "a claim already held by a sibling must not be reported as a \
                 retryable enqueue rejection — that would let a caller resend \
                 a duplicate reply"
            ),
            Err(TryReplyError::ClaimUnavailable(error)) => {
                assert!(
                    is_duplicate(&Err(error)),
                    "expected the duplicate-claim error"
                );
            }
        }
    }
}

/// R-E (P1): PR #152 (R-9) gated only the `Bytes` deferred-reply paths
/// (`send_response_auto`/`send_response_auto_bytes`) with a streaming
/// threshold + MAX_STREAM_SIZE check. The typed/pooled deferred-reply path
/// (`AskResponder::reply_typed` -> `send_response_pooled`) still wrote one
/// inline Response frame unconditionally, so a typed reply above the peer's
/// `max_message_size` tore the connection down, and a typed reply >= 2^27
/// bytes panicked `checked_body_len`'s `.expect` in the replying task.
#[cfg(test)]
mod size_gate_tests {
    use super::*;
    use crate::connection_pool::{BufferConfig, ChannelId, LockFreeStreamHandle};
    use tokio::io::AsyncReadExt;

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct BigReply {
        data: Vec<u8>,
    }
    crate::wire_type!(BigReply, "ask_responder::size_gate_tests::BigReply");

    /// A typed deferred reply above the streaming threshold must be
    /// auto-streamed (StreamResponseStart), not written as one inline
    /// Response frame — mirroring `send_response_auto_bytes` (R-9).
    #[tokio::test]
    async fn qa_re_large_typed_reply_streams_not_inlines() {
        let (client, mut peer) = tokio::io::duplex(8 * 1024);
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9990".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);
        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(42, stream_handle.clone(), used);

        // Above the default ~1MB streaming threshold, well under MAX_STREAM_SIZE.
        let big = BigReply {
            data: vec![0u8; 2 * 1024 * 1024],
        };
        responder.reply_typed(&big).await.unwrap();

        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        peer.read_exact(&mut ctrl).await.unwrap();
        let kind = crate::framing::decode_control(ctrl).unwrap().kind;
        assert_eq!(
            kind,
            crate::framing::WireKind::StreamResponseStart,
            "large typed reply must stream (R-E), not emit an inline Response frame"
        );

        stream_handle.shutdown();
        drop(peer);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// A typed deferred reply at/above 2^27 bytes (the V5 27-bit frame body
    /// length limit) must return `MessageTooLarge`, never panic
    /// `checked_body_len`'s `.expect` in the replying task.
    #[tokio::test]
    async fn qa_re_typed_reply_at_2_27_bytes_errors_not_panics() {
        let (client, _peer) = tokio::io::duplex(8 * 1024);
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9991".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);
        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(43, stream_handle.clone(), used);

        // >= 2^27 bytes (also > MAX_STREAM_SIZE).
        let huge = BigReply {
            data: vec![0u8; (1usize << 27) + 4096],
        };
        let err = responder.reply_typed(&huge).await.unwrap_err();
        assert!(
            matches!(err, crate::GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        stream_handle.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// A handle whose `max_message_size` (via `ReadContext`) sits far below
    /// the default streaming threshold (~1 MiB, from `BufferConfig::default`)
    /// -- every reply built from it below is well under the streaming
    /// threshold and `MAX_STREAM_SIZE`, so it takes the inline branch, not
    /// `stream_response_bytes`.
    fn small_message_stream_handle(
        port: u16,
        max_message_size: usize,
    ) -> (
        Arc<LockFreeStreamHandle>,
        tokio::task::JoinHandle<()>,
        tokio::io::DuplexStream,
    ) {
        let (client, peer) = tokio::io::duplex(8 * 1024);
        let read_context = crate::connection_pool::ReadContext {
            authenticated_peer_id: None,
            streaming_state_handoff: None,
            registry_weak: std::sync::Weak::new(),
            peer_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            session_source: format!("127.0.0.1:{port}").parse().unwrap(),
            peer_id: None,
            max_message_size,
            expected_schema_hash: None,
            aligned_pool: Arc::new(crate::AlignedBytesPool::default()),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            format!("127.0.0.1:{port}").parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_context),
        );
        let stream_handle = Arc::new(stream_handle);
        assert!(
            max_message_size < stream_handle.streaming_threshold(),
            "test setup: max_message_size must sit below the streaming \
             threshold so the reply below takes the inline path"
        );
        (stream_handle, task, peer)
    }

    /// PR #183 review, round 3: a typed reply that genuinely cannot be sent
    /// at all -- at or above `MAX_STREAM_SIZE`, so not even streaming can
    /// deliver it -- must still be rejected locally with `MessageTooLarge`.
    /// (A payload merely too big for the inline branch but still streamable
    /// must not be: see
    /// `qa_re_pooled_reply_streams_when_inline_would_exceed_max_message_size`.)
    #[tokio::test]
    async fn qa_re_pooled_reply_over_max_stream_size_is_still_rejected() {
        let max_message_size = 64;
        let (stream_handle, task, _peer) = small_message_stream_handle(9993, max_message_size);
        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(44, stream_handle.clone(), used);

        let huge = BigReply {
            data: vec![0u8; crate::MAX_STREAM_SIZE + 1],
        };
        let err = responder.reply_typed(&huge).await.unwrap_err();
        assert!(
            matches!(err, crate::GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        stream_handle.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// The complete-owner nonblocking lane must enforce the inline frame limit
    /// even when the streaming threshold is higher.
    #[tokio::test]
    async fn qa_re_nonblocking_bytes_reply_over_max_message_size_is_rejected() {
        let max_message_size = 64;
        let (stream_handle, task, _peer) = small_message_stream_handle(9994, max_message_size);
        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(45, stream_handle.clone(), used);

        let payload_len = max_message_size - crate::framing::ASK_RESPONSE_HEADER_LEN + 1;
        let err = responder
            .try_reply_bytes(crate::ForwardPayload::from(vec![0u8; payload_len]))
            .unwrap_err();
        assert!(
            matches!(err, crate::GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        stream_handle.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// PR #183 review, round 3: a typed reply comfortably under the
    /// streaming threshold but whose inline-encoded size would exceed
    /// `max_message_size` must still be delivered by streaming, not
    /// refused -- mirrors
    /// `LockFreeStreamHandle::auto_response_streams_when_inline_would_exceed_max_message_size`
    /// for the pooled reply path.
    #[tokio::test]
    async fn qa_re_pooled_reply_streams_when_inline_would_exceed_max_message_size() {
        let max_message_size = 128;
        let (stream_handle, task, mut peer) = small_message_stream_handle(9995, max_message_size);
        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(46, stream_handle.clone(), used);

        let payload_len = max_message_size;
        assert!(
            crate::framing::ASK_RESPONSE_HEADER_LEN + payload_len > max_message_size,
            "test setup: payload must not fit inline under max_message_size"
        );
        let big = BigReply {
            data: vec![7u8; payload_len],
        };
        responder
            .reply_typed(&big)
            .await
            .expect("a reply streaming can deliver must not be refused");

        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        tokio::io::AsyncReadExt::read_exact(&mut peer, &mut ctrl)
            .await
            .unwrap();
        let kind = crate::framing::decode_control(ctrl).unwrap().kind;
        assert_eq!(
            kind,
            crate::framing::WireKind::StreamResponseStart,
            "must stream, not attempt (and fail) an inline Response frame"
        );

        stream_handle.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }
}
