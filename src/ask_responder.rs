use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwapOption;
use bytes::{Buf, Bytes};

use crate::{GossipError, Result, connection_pool::LockFreeStreamHandle, framing};

/// R-E (P1): nonblocking/"immediate" reply lanes (`try_reply_bytes`,
/// `try_reply_bytes_immediate`) cannot enter the async auto-streaming path
/// (`stream_response_bytes`) without either blocking or duplicating its
/// queue/permit machinery in a sync context. Above the streaming threshold
/// they therefore reject the reply outright instead of ever building an
/// oversize inline frame (peer teardown, see `send_response_auto_bytes`/R-9)
/// or, at >= 2^27 bytes, panicking `framing::checked_body_len`'s `.expect`.
/// Callers with a payload this large must use the async `reply`/`reply_typed`
/// path, which auto-streams (mirrors `send_response_auto_bytes`).
#[inline]
fn reject_oversize_for_nonblocking_lane(
    stream_handle: &LockFreeStreamHandle,
    payload_len: usize,
) -> Result<()> {
    let threshold = stream_handle.streaming_threshold();
    if payload_len > threshold {
        return Err(GossipError::MessageTooLarge {
            size: payload_len,
            max: threshold,
        });
    }
    Ok(())
}

/// R-E (P1): shared gate for the typed/pooled deferred-reply path
/// (`AskResponder::reply_typed` -> `send_response_pooled`), mirroring
/// `send_response_auto_bytes`'s size gate (R-9) instead of duplicating it.
/// A payload above `MAX_STREAM_SIZE` is rejected locally (every receiver
/// hard-rejects a larger stream as FATAL); a payload above the streaming
/// threshold, or whose inline-encoded size would exceed `max_message_size`,
/// is auto-streamed via `stream_response_bytes` — the same machinery the
/// `Bytes` reply path uses, reached here via one copy of the (rare,
/// already-oversized) payload into `Bytes`. Checking only the streaming
/// threshold here (as this used to) would pick the inline branch below for
/// a payload the inline gate then has to refuse whenever `max_message_size`
/// is smaller than the threshold -- streaming can still deliver it in
/// bounded chunks, so `MessageTooLarge` must stay reserved for payloads
/// that cannot be sent at all (>= `MAX_STREAM_SIZE`), mirroring
/// `LockFreeStreamHandle::should_stream_response`.
async fn send_pooled_via_stream_handle(
    stream_handle: &LockFreeStreamHandle,
    correlation_id: u32,
    payload: crate::typed::PooledPayload,
    prefix: Option<[u8; 16]>,
    payload_len: usize,
) -> Result<()> {
    if payload_len > crate::MAX_STREAM_SIZE {
        return Err(GossipError::MessageTooLarge {
            size: payload_len,
            max: crate::MAX_STREAM_SIZE,
        });
    }
    let inline_payload_limit = stream_handle
        .max_message_size()
        .saturating_sub(framing::ASK_RESPONSE_HEADER_LEN);
    if payload_len > stream_handle.streaming_threshold() || payload_len > inline_payload_limit {
        let bytes = pooled_payload_into_bytes(prefix, payload);
        return stream_handle.stream_response_bytes(bytes, correlation_id).await;
    }
    let header = framing::try_write_ask_response_header(
        crate::MessageType::Response,
        correlation_id,
        payload_len,
    )?;
    let prefix_len = prefix.as_ref().map(|bytes| bytes.len()).unwrap_or(0) as u8;
    stream_handle
        .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
        .await
}

/// Copy an (already above-threshold, so rare) pooled typed payload plus its
/// optional debug type-hash prefix into one contiguous `Bytes` for
/// `stream_response_bytes`, which needs owned, slice-able `Bytes` to chunk
/// without an extra copy per chunk.
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

#[derive(Clone)]
enum AskResponseSink {
    StreamHandle(Arc<LockFreeStreamHandle>),
    DeferredWriter(Arc<ResponseWriter>),
}

impl AskResponseSink {
    async fn send_response_bytes(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                stream_handle
                    .send_response_auto_bytes(correlation_id, payload)
                    .await
            }
            Self::DeferredWriter(writer) => {
                writer.send_response_bytes(correlation_id, payload).await
            }
        }
    }

    fn try_send_response_bytes(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                reject_oversize_for_nonblocking_lane(stream_handle, payload.len())?;
                let header = framing::try_write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    payload.len(),
                )?;
                stream_handle
                    .write_header_and_payload_control_inline_nonblocking(header, 16, payload)
            }
            Self::DeferredWriter(writer) => writer.try_send_response_bytes(correlation_id, payload),
        }
    }

    fn try_send_response_bytes_immediate(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                reject_oversize_for_nonblocking_lane(stream_handle, payload.len())?;
                let header = framing::try_write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    payload.len(),
                )?;
                stream_handle.write_header_and_payload_control_inline_immediate_nonblocking(
                    header, 16, payload,
                )
            }
            Self::DeferredWriter(writer) => {
                writer.try_send_response_bytes_immediate(correlation_id, payload)
            }
        }
    }

    async fn send_response_pooled(
        &self,
        correlation_id: u32,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                send_pooled_via_stream_handle(
                    stream_handle,
                    correlation_id,
                    payload,
                    prefix,
                    payload_len,
                )
                .await
            }
            Self::DeferredWriter(writer) => {
                writer
                    .send_response_pooled(correlation_id, payload, prefix, payload_len)
                    .await
            }
        }
    }
}

enum AskContextSink<'a> {
    StreamHandle(&'a Arc<LockFreeStreamHandle>),
    DeferredWriter(&'a Arc<ResponseWriter>),
}

pub struct AskContext<'a> {
    correlation_id: u32,
    authenticated_peer_id: Option<&'a crate::PeerId>,
    sink: AskContextSink<'a>,
    /// ACTOR_REM_2 R15: shared single-use guard for this inbound ask. Every
    /// responder minted from this context (and their clones) share it, so at
    /// most one reply for the correlation ever reaches the wire — a second
    /// would otherwise complete an unrelated ask after correlation-id reuse
    /// wraps.
    used: Arc<AtomicBool>,
}

impl<'a> AskContext<'a> {
    pub(crate) fn from_stream_handle(
        correlation_id: u32,
        stream_handle: &'a Arc<LockFreeStreamHandle>,
        authenticated_peer_id: Option<&'a crate::PeerId>,
    ) -> Self {
        Self {
            correlation_id,
            authenticated_peer_id,
            sink: AskContextSink::StreamHandle(stream_handle),
            used: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn from_writer(
        correlation_id: u32,
        writer: &'a Arc<ResponseWriter>,
        authenticated_peer_id: Option<&'a crate::PeerId>,
    ) -> Self {
        Self {
            correlation_id,
            authenticated_peer_id,
            sink: AskContextSink::DeferredWriter(writer),
            used: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn correlation_id(&self) -> u32 {
        self.correlation_id
    }

    pub fn authenticated_peer_id(&self) -> Option<&crate::PeerId> {
        self.authenticated_peer_id
    }

    pub fn responder(&self) -> AskResponder {
        match &self.sink {
            AskContextSink::StreamHandle(stream_handle) => AskResponder::from_stream_handle(
                self.correlation_id,
                (*stream_handle).clone(),
                Arc::clone(&self.used),
            ),
            AskContextSink::DeferredWriter(writer) => AskResponder::from_writer(
                self.correlation_id,
                (*writer).clone(),
                Arc::clone(&self.used),
            ),
        }
    }
}

#[derive(Clone)]
pub struct AskResponder {
    correlation_id: u32,
    sink: AskResponseSink,
    /// ACTOR_REM_2 R15: shared single-use guard (see [`AskContext::used`]).
    /// Clones share the same `Arc`, so only the first reply across all clones
    /// and sibling responders for this correlation reaches the wire.
    used: Arc<AtomicBool>,
}

/// Exclusive fallback ownership after an immediate reply was rejected before
/// it entered the writer. The original responder's shared single-reply claim
/// remains held, so sibling responders cannot race the selected fallback.
///
/// Constructed only when the caller itself claimed the single-use reply
/// guard and a subsequent enqueue attempt was rejected — never when the
/// claim was already taken by a sibling responder (see [`TryReplyError`]).
pub struct ImmediateReplyFallback {
    responder: AskResponder,
    error: GossipError,
}

impl ImmediateReplyFallback {
    /// The immediate-enqueue error that selected this fallback path.
    pub fn error(&self) -> &GossipError {
        &self.error
    }

    /// Retry through the ordinary nonblocking response queue while retaining
    /// exclusive ownership of the ask correlation.
    pub fn try_reply_bytes(self, response: Bytes) -> Result<()> {
        self.responder
            .sink
            .try_send_response_bytes(self.responder.correlation_id, response)
    }

    /// Retry through the reserved immediate queue while retaining exclusive
    /// ownership of the ask correlation.
    pub fn try_reply_bytes_immediate(self, response: Bytes) -> Result<()> {
        self.responder
            .sink
            .try_send_response_bytes_immediate(self.responder.correlation_id, response)
    }

    /// Fall back to the asynchronous response path while retaining exclusive
    /// ownership of the ask correlation.
    pub async fn reply_bytes(self, response: Bytes) -> Result<()> {
        self.responder
            .sink
            .send_response_bytes(self.responder.correlation_id, response)
            .await
    }
}

/// Outcome of a `*_with_fallback` reply attempt that failed to send inline.
///
/// The two failure modes are not interchangeable: a claim that was already
/// taken by a sibling responder means a reply for this ask is already sent
/// or in flight, and retrying would send a duplicate on the same correlation
/// id. A rejected enqueue means this call owns the sole claim and the bytes
/// were never sent, so it is safe — and necessary — to retry.
pub enum TryReplyError {
    /// The single-use reply guard was already claimed by another responder
    /// for this ask. There is nothing to retry; the reply must be dropped.
    ClaimUnavailable(GossipError),
    /// This call claimed the guard, but the nonblocking enqueue was
    /// rejected. The fallback retains exclusive ownership and may retry.
    Enqueue(ImmediateReplyFallback),
}

impl TryReplyError {
    /// The underlying error, regardless of which case produced it.
    pub fn error(&self) -> &GossipError {
        match self {
            Self::ClaimUnavailable(error) => error,
            Self::Enqueue(fallback) => fallback.error(),
        }
    }
}

impl AskResponder {
    pub(crate) fn from_stream_handle(
        correlation_id: u32,
        stream_handle: Arc<LockFreeStreamHandle>,
        used: Arc<AtomicBool>,
    ) -> Self {
        Self {
            correlation_id,
            sink: AskResponseSink::StreamHandle(stream_handle),
            used,
        }
    }

    pub(crate) fn from_writer(
        correlation_id: u32,
        writer: Arc<ResponseWriter>,
        used: Arc<AtomicBool>,
    ) -> Self {
        Self {
            correlation_id,
            sink: AskResponseSink::DeferredWriter(writer),
            used,
        }
    }

    pub fn correlation_id(&self) -> u32 {
        self.correlation_id
    }

    pub async fn reply(self, response: Bytes) -> Result<()> {
        claim_reply(&self.used)?;
        self.sink
            .send_response_bytes(self.correlation_id, response)
            .await
    }

    pub async fn reply_bytes(self, response: Bytes) -> Result<()> {
        self.reply(response).await
    }

    /// A rejected enqueue still consumes the reply claim, so the response is
    /// lost if the caller does not retry from the returned error. Use
    /// [`Self::try_reply_bytes_with_fallback`] when the caller needs an
    /// exclusive retry or fallback path.
    pub fn try_reply_bytes(self, response: Bytes) -> Result<()> {
        claim_reply(&self.used)?;
        self.sink
            .try_send_response_bytes(self.correlation_id, response)
    }

    /// Try the ordinary nonblocking queue, distinguishing a claim already
    /// held by a sibling responder (nothing to retry — drop it) from a
    /// rejected enqueue while this call owns the claim (retryable via the
    /// returned fallback, which cannot be raced by a sibling).
    pub fn try_reply_bytes_with_fallback(
        self,
        response: Bytes,
    ) -> std::result::Result<(), TryReplyError> {
        if let Err(error) = claim_reply(&self.used) {
            return Err(TryReplyError::ClaimUnavailable(error));
        }
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

    /// Reply through the ordinary queue with a hard delivery guarantee: if
    /// this call wins the single-use claim, the reply is either sent
    /// inline or, on a rejected enqueue, retried through the awaitable,
    /// backpressured response path before this call returns. There is no
    /// state in which this call holds the claim but the reply is neither
    /// sent nor retried.
    ///
    /// Returns `Err` only when a sibling responder already held the claim
    /// (nothing for this call to do — delivery is that sibling's
    /// responsibility under the same guarantee) or when the connection is
    /// gone.
    pub async fn reply_bytes_guaranteed(self, response: Bytes) -> Result<()> {
        match self.try_reply_bytes_with_fallback(response.clone()) {
            Ok(()) => Ok(()),
            Err(TryReplyError::ClaimUnavailable(error)) => Err(error),
            Err(TryReplyError::Enqueue(fallback)) => fallback.reply_bytes(response).await,
        }
    }

    /// Try to reply through the connection's immediate nonblocking queue.
    ///
    /// This is intended for small control-plane responses whose delivery must
    /// not be starved by ordinary gossip/control traffic. It still never
    /// awaits or creates a detached task. A rejected enqueue still consumes
    /// the reply claim, preserving at-most-once ownership across sibling
    /// responders. Use [`Self::try_reply_bytes_immediate_with_fallback`] when
    /// the caller needs an exclusive retry or fallback path.
    pub fn try_reply_bytes_immediate(self, response: Bytes) -> Result<()> {
        claim_reply(&self.used)?;
        self.sink
            .try_send_response_bytes_immediate(self.correlation_id, response)
    }

    /// Try the reserved immediate queue, distinguishing a claim already held
    /// by a sibling responder (nothing to retry — drop it) from a rejected
    /// enqueue while this call owns the claim (retryable via the returned
    /// fallback, which cannot be raced by a sibling).
    pub fn try_reply_bytes_immediate_with_fallback(
        self,
        response: Bytes,
    ) -> std::result::Result<(), TryReplyError> {
        if let Err(error) = claim_reply(&self.used) {
            return Err(TryReplyError::ClaimUnavailable(error));
        }
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

    pub async fn reply_typed<M>(self, value: &M) -> Result<()>
    where
        M: crate::typed::WireEncode,
    {
        claim_reply(&self.used)?;
        let payload = crate::typed::encode_typed_pooled(value)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<M>(payload);
        self.sink
            .send_response_pooled(self.correlation_id, payload, prefix, payload_len)
            .await
    }
}

impl std::fmt::Debug for AskResponder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AskResponder")
            .field("correlation_id", &self.correlation_id)
            .finish()
    }
}

#[cfg(test)]
mod contract_tests {
    use super::AskResponder;
    use bytes::Bytes;

    // Authority-voter replies must have an explicit immediate, nonblocking
    // response path so a saturated ordinary control queue cannot starve the
    // quorum round-trip.
    #[allow(dead_code)]
    fn immediate_reply_api_is_available(responder: AskResponder) {
        let _ = responder.try_reply_bytes_immediate(Bytes::new());
    }
}

pub(crate) struct ResponseWriter {
    addr: SocketAddr,
    stream_handle: ArcSwapOption<LockFreeStreamHandle>,
}

impl ResponseWriter {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream_handle: ArcSwapOption::empty(),
        }
    }

    pub(crate) fn bind_stream_handle(&self, stream_handle: Arc<LockFreeStreamHandle>) {
        self.stream_handle.store(Some(stream_handle));
    }

    fn stream_handle(&self) -> Result<Arc<LockFreeStreamHandle>> {
        self.stream_handle.load_full().ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("response stream handle is not available for {}", self.addr),
            ))
        })
    }

    async fn send_response_bytes(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        self.stream_handle()?
            .send_response_auto_bytes(correlation_id, payload)
            .await
    }

    fn try_send_response_bytes(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        let stream_handle = self.stream_handle()?;
        reject_oversize_for_nonblocking_lane(&stream_handle, payload.len())?;
        let header = framing::try_write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        )?;
        stream_handle.write_header_and_payload_control_inline_nonblocking(header, 16, payload)
    }

    fn try_send_response_bytes_immediate(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        let stream_handle = self.stream_handle()?;
        reject_oversize_for_nonblocking_lane(&stream_handle, payload.len())?;
        let header = framing::try_write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        )?;
        stream_handle
            .write_header_and_payload_control_inline_immediate_nonblocking(header, 16, payload)
    }

    async fn send_response_pooled(
        &self,
        correlation_id: u32,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        let stream_handle = self.stream_handle()?;
        send_pooled_via_stream_handle(&stream_handle, correlation_id, payload, prefix, payload_len)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ask_context_exposes_authenticated_peer_id() {
        let peer_id = crate::KeyPair::new_for_testing("ask-context-peer").peer_id();
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12345".parse().unwrap()));
        let context = AskContext::from_writer(7, &writer, Some(&peer_id));

        assert_eq!(context.correlation_id(), 7);
        assert_eq!(context.authenticated_peer_id(), Some(&peer_id));
    }

    #[test]
    fn tell_context_exposes_authenticated_peer_id() {
        let peer_id = crate::KeyPair::new_for_testing("tell-context-peer").peer_id();
        let context = TellContext::new(Some(&peer_id));

        assert_eq!(context.authenticated_peer_id(), Some(&peer_id));
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
        let first_result = first.try_reply_bytes(Bytes::from_static(b"a"));
        let second_result = second.try_reply_bytes(Bytes::from_static(b"b"));

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
        let _ = responder.try_reply_bytes(Bytes::from_static(b"x"));
        let clone_result = clone.try_reply_bytes(Bytes::from_static(b"y"));
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
        let immediate_result = immediate.try_reply_bytes_immediate(Bytes::from_static(b"fast"));
        assert!(
            !is_duplicate(&immediate_result),
            "the owner sees the underlying enqueue failure, not a duplicate"
        );
        assert!(
            is_duplicate(&sibling.try_reply_bytes(Bytes::from_static(b"sibling"))),
            "a rejected immediate enqueue must not release ownership to a sibling"
        );

        let retry_context = AskContext::from_writer(12, &writer, None);
        let retry_sibling = retry_context.responder();
        let fallback = match retry_context
            .responder()
            .try_reply_bytes_immediate_with_fallback(Bytes::from_static(b"fast"))
        {
            Ok(()) => panic!("unbound writer must reject immediate enqueue"),
            Err(TryReplyError::ClaimUnavailable(error)) => {
                panic!("this call owns the claim, not a stale sibling: {error:?}")
            }
            Err(TryReplyError::Enqueue(fallback)) => fallback,
        };
        assert!(
            is_duplicate(&retry_sibling.try_reply_bytes(Bytes::from_static(b"sibling"))),
            "fallback ownership must remain exclusive to the original responder"
        );
        let retry_result = fallback.try_reply_bytes(Bytes::from_static(b"fallback"));
        assert!(
            !is_duplicate(&retry_result),
            "the original owner may choose a nonblocking fallback path"
        );
    }

    /// A sibling responder whose reply arrives after another sibling already
    /// claimed the shared guard must see `ClaimUnavailable`, never `Enqueue` —
    /// otherwise a caller that retries every `Err` through the fallback path
    /// would send a duplicate response for an ask that was already answered.
    #[test]
    fn claim_already_taken_by_sibling_is_not_an_enqueue_fallback() {
        let writer = Arc::new(ResponseWriter::new("127.0.0.1:12348".parse().unwrap()));
        let context = AskContext::from_writer(13, &writer, None);

        let first = context.responder();
        let second = context.responder();

        // First reply claims the shared guard (its send fails only because no
        // stream handle is bound in this unit test).
        let _ = first.try_reply_bytes_with_fallback(Bytes::from_static(b"first"));

        match second.try_reply_bytes_with_fallback(Bytes::from_static(b"second")) {
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

    /// Same gap, the nonblocking `Bytes` reply lane
    /// (`try_reply_bytes` -> `try_send_response_bytes`): its own
    /// `reject_oversize_for_nonblocking_lane` pre-check only compares
    /// against the streaming threshold.
    #[tokio::test]
    async fn qa_re_nonblocking_bytes_reply_over_max_message_size_is_rejected() {
        let max_message_size = 64;
        let (stream_handle, task, _peer) = small_message_stream_handle(9994, max_message_size);
        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(45, stream_handle.clone(), used);

        let payload_len = max_message_size - crate::framing::ASK_RESPONSE_HEADER_LEN + 1;
        let err = responder
            .try_reply_bytes(Bytes::from(vec![0u8; payload_len]))
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
