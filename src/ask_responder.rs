use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwapOption;
use bytes::Bytes;

use crate::{GossipError, Result, connection_pool::LockFreeStreamHandle, framing};

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
                let header = framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    payload.len(),
                );
                stream_handle
                    .write_header_and_payload_control_inline_nonblocking(header, 16, payload)
            }
            Self::DeferredWriter(writer) => writer.try_send_response_bytes(correlation_id, payload),
        }
    }

    fn try_send_response_bytes_immediate(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                let header = framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    payload.len(),
                );
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
                let header = framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    payload_len,
                );
                let prefix_len = prefix.as_ref().map(|bytes| bytes.len()).unwrap_or(0) as u8;
                stream_handle
                    .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
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

    pub fn try_reply_bytes(self, response: Bytes) -> Result<()> {
        claim_reply(&self.used)?;
        self.sink
            .try_send_response_bytes(self.correlation_id, response)
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

    /// Try the reserved immediate queue and retain exclusive reply ownership
    /// on rejection. The returned fallback can retry or select another reply
    /// path without permitting a sibling responder to enqueue a duplicate.
    pub fn try_reply_bytes_immediate_with_fallback(
        self,
        response: Bytes,
    ) -> std::result::Result<(), ImmediateReplyFallback> {
        if let Err(error) = claim_reply(&self.used) {
            return Err(ImmediateReplyFallback {
                responder: self,
                error,
            });
        }
        match self
            .sink
            .try_send_response_bytes_immediate(self.correlation_id, response)
        {
            Ok(()) => Ok(()),
            Err(error) => Err(ImmediateReplyFallback {
                responder: self,
                error,
            }),
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
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );
        self.stream_handle()?
            .write_header_and_payload_control_inline_nonblocking(header, 16, payload)
    }

    fn try_send_response_bytes_immediate(&self, correlation_id: u32, payload: Bytes) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );
        self.stream_handle()?
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
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload_len,
        );
        let prefix_len = prefix.as_ref().map(|bytes| bytes.len()).unwrap_or(0) as u8;
        stream_handle
            .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
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
            Err(fallback) => fallback,
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
}
