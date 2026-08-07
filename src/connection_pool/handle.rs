/// Handle to send messages through a persistent connection - LOCK-FREE
#[derive(Clone)]
pub struct ConnectionHandle<T = ()> {
    pub addr: SocketAddr,
    // Stream-based writer path (TCP/TLS/Noise/QUIC stream transports).
    stream_handle: Option<Arc<LockFreeStreamHandle>>,
    schema_hash: Option<u64>,
    // Correlation tracker for ask/response
    correlation: Arc<CorrelationTracker>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for ConnectionHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle")
            .field("addr", &self.addr)
            .field("stream_handle", &self.stream_handle)
            .field("schema_hash", &self.schema_hash)
            .finish()
    }
}

/// Deferred ask handle backed by the per-connection correlation tracker.
///
/// This is the correct way to "delegate" awaiting a response:
/// - the request is sent immediately,
/// - the returned handle can be moved to another task and awaited later,
/// - dropping the handle cancels the pending slot to keep resources bounded.
pub(crate) struct PendingAsk {
    correlation_id: u32,
    correlation: Arc<CorrelationTracker>,
    timeout: Duration,
    active: bool,
}

impl PendingAsk {
    pub(crate) fn correlation_id(&self) -> u32 {
        self.correlation_id
    }

    pub(crate) async fn wait(mut self) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation_id;
        let timeout = self.timeout;
        let correlation = Arc::clone(&self.correlation);
        let result = correlation.wait_for_response(correlation_id, timeout).await;
        self.active = false;
        result.map(crate::AlignedBytes::into_bytes)
    }
}

impl Drop for PendingAsk {
    fn drop(&mut self) {
        if self.active {
            self.correlation.cancel(self.correlation_id);
        }
    }
}

impl std::fmt::Debug for PendingAsk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAsk")
            .field("correlation_id", &self.correlation_id)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl<T> ConnectionHandle<T> {
    fn new_stream(
        addr: SocketAddr,
        stream_handle: Arc<LockFreeStreamHandle>,
        correlation: Arc<CorrelationTracker>,
    ) -> Self {
        let schema_hash = stream_handle.schema_hash();
        Self {
            addr,
            stream_handle: Some(stream_handle),
            schema_hash,
            correlation,
            _marker: PhantomData,
        }
    }

    /// Instance id of the specific stream-handle backing this connection
    /// handle, if any. Callers that need to retire *this exact* connection
    /// instance (rather than "whatever is currently indexed for the peer")
    /// use this together with `addr` and
    /// `ConnectionPool::remove_connection_instance_by_id`.
    #[inline]
    pub(crate) fn instance_id(&self) -> Option<u64> {
        self.stream_handle
            .as_ref()
            .map(|handle| handle.instance_id())
    }

    #[inline]
    fn stream_handle(&self) -> Result<&Arc<LockFreeStreamHandle>> {
        self.stream_handle.as_ref().ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("stream writer is not available for {}", self.addr),
            ))
        })
    }

    async fn write_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_bytes_control(data).await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// See the note on `LockFreeStreamHandle::write_trusted_bytes_ask`.
    async fn write_trusted_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_trusted_bytes_control(data).await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    async fn write_header_and_payload_control_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .write_header_and_payload_control_inline(header, header_len, payload)
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    fn write_header_and_payload_control_inline_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .write_header_and_payload_control_inline_nonblocking(header, header_len, payload)
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    async fn write_header_and_payload_ask_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .write_header_and_payload_ask_inline(header, header_len, payload)
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    async fn write_routed_actor_ask(
        &self,
        correlation_id: u32,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.stream_handle()?
            .write_routed_actor_ask(correlation_id, actor_id, type_hash, payload)
            .await
    }

    async fn write_direct_ask_inline(&self, header: [u8; 16], payload: bytes::Bytes) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_direct_ask_inline(header, payload).await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// Returns true if the underlying IO task has exited and the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.stream_handle
            .as_ref()
            .map(|h| h.exit_flag.load(Ordering::Acquire))
            .unwrap_or(false)
    }

    /// Observability hook for tests/diagnostics: total bytes written to the socket by the IO task.
    pub fn bytes_written(&self) -> usize {
        self.stream_handle
            .as_ref()
            .map(|h| h.bytes_written())
            .unwrap_or(0)
    }

    /// Observability hook for tests/diagnostics: is the connection currently in streaming mode.
    pub fn is_streaming_active(&self) -> bool {
        self.stream_handle
            .as_ref()
            .map(|h| h.is_streaming_active())
            .unwrap_or(false)
    }

    /// Observability hook for tests/diagnostics: number of queued write operations attempted.
    pub fn sequence_number(&self) -> usize {
        self.stream_handle
            .as_ref()
            .map(|h| h.sequence_number())
            .unwrap_or(0)
    }

    /// Send a pre-serialized, complete V5 frame (or several concatenated)
    /// through this connection - LOCK-FREE.
    ///
    /// PR #183 review, round 14: `data` must already be one or more
    /// complete, well-formed V5 frames -- this method adds no framing of
    /// its own, and this crate's wire protocol has no raw-passthrough read
    /// mode for it to fall back to if `data` isn't. Every read path
    /// (`read_message_step`/`read_message_step_poll`/
    /// `read_message_step_nonblocking` in `read_pipeline.rs`)
    /// unconditionally decodes a V5 control word from whatever arrives on
    /// the connection and fails it if the bytes don't decode; there is no
    /// way to send genuinely unframed bytes and have the peer treat them
    /// as anything other than a desynchronizing parse failure, regardless
    /// of what this method's gate does. See `WritePayload::Single`'s doc
    /// comment (`connection_pool/types.rs`) for the full invariant this
    /// crate enforces on writes through this path.
    pub async fn send_data(&self, data: Vec<u8>) -> Result<()> {
        self.write_bytes_control(bytes::Bytes::from(data)).await
    }

    /// Send `data` as-is, with no *additional* framing layered on top of
    /// it by this method.
    ///
    /// PR #183 review, round 14: this docstring used to say "without any
    /// framing," which described a capability this wire protocol does not
    /// have -- see the note on `send_data` above for the evidence. `data`
    /// must already be one or more complete, well-formed V5 frames; "raw"
    /// here means this method doesn't wrap or reframe it, not that the
    /// peer will accept genuinely unframed content.
    pub async fn send_raw_bytes(&self, data: bytes::Bytes) -> Result<()> {
        self.write_bytes_control(data).await
    }

    /// Local admission check for an inline (non-streaming) send: `max_message_size`
    /// bounds the *encoded* frame body (this send's fixed header length plus
    /// the payload), not the payload alone -- passing a bare payload length
    /// under-counts every structured frame kind by its header size and lets a
    /// payload through that the receiver still hard-rejects as
    /// `MessageTooLarge` once the header is added, tearing the whole
    /// connection down (`read_pipeline`'s `MessageTooLarge` checks) for every
    /// other actor sharing it. `fixed_header_len` must be the same constant
    /// (e.g. `ACTOR_TELL_HEADER_LEN`) the header this send builds will add;
    /// `0` for the raw-header sends (`tell_bytes`/`tell_typed`) whose bare
    /// length control word makes body_len == payload_len with no separate
    /// structured header -- for those, this is also what stops an oversize
    /// length from bleeding into the `WireKind` bits `decode_control` reads
    /// back, since config validation always keeps `max_message_size` at or
    /// under the V5 27-bit body-length limit. Mirrors
    /// `ask_responder::reject_oversize_for_nonblocking_lane`.
    fn reject_oversize_inline(&self, fixed_header_len: usize, payload_len: usize) -> Result<()> {
        let Some(stream_handle) = self.stream_handle.as_ref() else {
            return Ok(());
        };
        framing::reject_oversize_for_inline_send(
            fixed_header_len,
            payload_len,
            stream_handle.max_message_size(),
        )
    }

    /// Send a response payload with framing, without copying the payload.
    pub async fn send_response_bytes(
        &self,
        correlation_id: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.reject_oversize_inline(framing::ASK_RESPONSE_HEADER_LEN, payload.len())?;
        let header = framing::try_write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        )?;

        self.write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    /// Send an ask NACK: same frame kind and header shape as a normal
    /// response (`send_response_bytes`), zero-length payload, reason packed
    /// into the header's reserved bytes (`framing::write_ask_nack_header`).
    pub async fn send_ask_nack(
        &self,
        correlation_id: u32,
        reason: crate::framing::AskNackReason,
    ) -> Result<()> {
        let header = framing::write_ask_nack_header(correlation_id, reason);
        self.write_header_and_payload_control_inline(header, 16, bytes::Bytes::new())
            .await
    }

    /// Send a gossip payload with framing, without copying the payload.
    pub async fn send_gossip_payload(&self, payload: bytes::Bytes) -> Result<()> {
        self.reject_oversize_inline(framing::GOSSIP_HEADER_LEN, payload.len())?;
        let header = framing::try_write_gossip_frame_prefix(payload.len())?;
        self.write_header_and_payload_control_inline(
            header,
            crate::framing::GOSSIP_FRAME_HEADER_LEN as u8,
            payload,
        )
        .await
    }

    /// Send a routed PubSub payload with framing, without copying the payload.
    pub async fn send_pubsub_payload(&self, payload: bytes::Bytes) -> Result<()> {
        self.reject_oversize_inline(framing::PUBSUB_HEADER_LEN, payload.len())?;
        let header = framing::try_write_pubsub_frame_prefix(payload.len())?;
        self.write_header_and_payload_control_inline(
            header,
            crate::framing::PUBSUB_FRAME_HEADER_LEN as u8,
            payload,
        )
        .await
    }

    /// Try to send a routed PubSub payload without awaiting on the write queue.
    ///
    /// R-D: this MUST use the normal (non-immediate) write queue. The
    /// `immediate_write_queue` is a small, fixed-size lane reserved
    /// exclusively for latency-critical control-plane replies (see the
    /// invariant documented on `LockFreeStreamHandle::immediate_write_queue`
    /// in `stream_writer.rs`); routing pubsub DATA-plane traffic through it
    /// both shrinks pubsub burst admission (128 slots vs. the normal queue's
    /// `DEFAULT_ASK_WINDOW * 8` = 1024) and lets a pubsub burst consume the
    /// capacity a control-plane reply needs, defeating the reservation.
    pub fn try_send_pubsub_payload(&self, payload: bytes::Bytes) -> Result<()> {
        self.reject_oversize_inline(framing::PUBSUB_HEADER_LEN, payload.len())?;
        let header = framing::try_write_pubsub_frame_prefix(payload.len())?;
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_header_and_payload_control_inline_nonblocking(
                header,
                crate::framing::PUBSUB_FRAME_HEADER_LEN as u8,
                payload,
            )
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// Try to send a routed PubSub payload from a pooled buffer without allocating.
    ///
    /// R-D: see `try_send_pubsub_payload` — must stay on the normal write
    /// queue, never the reserved immediate control-reply lane.
    pub fn try_send_pubsub_payload_pooled(
        &self,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        self.reject_oversize_inline(framing::PUBSUB_HEADER_LEN, payload_len)?;
        let header = framing::try_write_pubsub_frame_prefix(payload_len)?;
        let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_pooled_control_inline_nonblocking(
                header,
                crate::framing::PUBSUB_FRAME_HEADER_LEN as u8,
                prefix,
                prefix_len,
                payload,
            )
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// Send a response using the inline write queue (never streaming).
    pub async fn send_response_auto(
        &self,
        correlation_id: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .send_response_auto(payload, correlation_id)
                .await
        } else {
            self.send_response_bytes(correlation_id, payload).await
        }
    }

    /// Send a response with owned Bytes using the inline write queue (never streaming).
    ///
    /// # Arguments
    /// * `correlation_id` - The correlation ID from the original request
    /// * `payload` - The response payload as owned Bytes
    pub async fn send_response_auto_bytes(
        &self,
        correlation_id: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .send_response_auto_bytes(correlation_id, payload)
                .await
        } else {
            self.send_response_bytes(correlation_id, payload).await
        }
    }

    /// Send a response payload using a Buf without copying.
    pub async fn send_response_buf<B>(
        &self,
        correlation_id: u32,
        mut payload: B,
        payload_len: usize,
    ) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        self.reject_oversize_inline(framing::ASK_RESPONSE_HEADER_LEN, payload_len)?;
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            let header = framing::try_write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                payload_len,
            )?;
            // `header` was built from the caller-declared `payload_len`,
            // not from `payload` itself -- the two can disagree (that
            // disagreement is exactly the bug `write_buf_control_checked`
            // guards against), so this must use the checked form, not the
            // single-argument one that trusts `payload.remaining()` alone.
            let expected_len = header.len() + payload_len;
            let buf = bytes::Bytes::copy_from_slice(&header).chain(payload); // ALLOW_COPY
            stream_handle
                .write_buf_control_checked(buf, expected_len)
                .await
        } else {
            let bytes = payload.copy_to_bytes(payload.remaining());
            self.send_response_bytes(correlation_id, bytes).await
        }
    }

    /// Send a response payload using a pooled payload without dynamic dispatch.
    pub async fn send_response_pooled(
        &self,
        correlation_id: u32,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        self.reject_oversize_inline(framing::ASK_RESPONSE_HEADER_LEN, payload_len)?;
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            let header = framing::try_write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                payload_len,
            )?;
            let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
            stream_handle
                .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// Send a complete, pre-serialized V5 frame (or several concatenated)
    /// without copying - TRUE ZERO-COPY.
    ///
    /// PR #183 review, round 14: same contract as `send_data` above --
    /// `data` must already be complete frame(s); see that method's doc
    /// comment for why.
    pub async fn send_bytes_zero_copy(&self, data: bytes::Bytes) -> Result<()> {
        self.write_bytes_control(data).await
    }

    /// Stream a large message directly - MAXIMUM PERFORMANCE
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .stream_large_message(msg, type_hash, actor_id)
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "stream_large_message requires a stream-based connection",
            )))
        }
    }

    /// Canonical owned-Bytes stream path; avoids the slice wrapper's copy.
    pub async fn stream_large_message_bytes(
        &self,
        payload: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .stream_large_message_bytes(payload, type_hash, actor_id)
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "stream_large_message_bytes requires a stream-based connection",
            )))
        }
    }

    /// Get the streaming threshold for this connection
    ///
    /// Messages larger than this threshold should be sent via streaming
    /// rather than through the write queue to prevent message loss.
    pub fn streaming_threshold(&self) -> usize {
        self.stream_handle
            .as_ref()
            .map(|h| h.streaming_threshold())
            .unwrap_or(STREAMING_THRESHOLD)
    }

    /// Tell using owned bytes to avoid payload copies.
    pub async fn tell_bytes(&self, data: bytes::Bytes) -> Result<()> {
        match self.try_tell_bytes(data.clone()) {
            Ok(()) => Ok(()),
            Err(GossipError::WriteQueueFull) => {
                // `try_tell_bytes` already ran `reject_oversize_inline` before
                // returning `WriteQueueFull`, so `data` is known in-bounds here.
                let mut header = [0u8; 16];
                header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());

                self.write_header_and_payload_control_inline(header, 4, data)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// Non-blocking tell. Returns `GossipError::WriteQueueFull` on backpressure.
    ///
    /// This raw header carries only a bare length, not a full V5 control word
    /// (kind:5|body_len:27 packed via `framing::encode_control`) -- with no
    /// local bound, a payload at or beyond the 27-bit body-length field would
    /// bleed into the bits `decode_control` reads back as the `WireKind`,
    /// desyncing the peer's frame parser with no local diagnostic at all.
    /// `reject_oversize_inline` closes that: it is always at least as tight
    /// as the 27-bit limit, since config validation caps `max_message_size`
    /// there.
    pub fn try_tell_bytes(&self, data: bytes::Bytes) -> Result<()> {
        self.reject_oversize_inline(0, data.len())?;
        let mut header = [0u8; 16];
        header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());
        self.write_header_and_payload_control_inline_nonblocking(header, 4, data)
    }

    /// Tell an actor using the direct actor frame envelope (MessageType::ActorTell).
    pub async fn tell_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.reject_oversize_inline(framing::ACTOR_TELL_HEADER_LEN, payload.len())?;
        let header =
            crate::framing::try_write_actor_tell_header(actor_id, type_hash, payload.len())?;
        self.write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    /// Try to send an ActorTell frame without awaiting on the write queue.
    ///
    /// Returns `GossipError::WriteQueueFull` if the per-connection write queue is full.
    pub fn try_tell_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.reject_oversize_inline(framing::ACTOR_TELL_HEADER_LEN, payload.len())?;
        let header =
            crate::framing::try_write_actor_tell_header(actor_id, type_hash, payload.len())?;
        self.write_header_and_payload_control_inline_nonblocking(header, 16, payload)
    }

    /// Ask an actor using the direct actor frame envelope (MessageType::ActorAsk).
    pub async fn ask_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let response = self
            .ask_actor_frame_aligned(actor_id, type_hash, payload, timeout)
            .await?;
        Ok(response.into_bytes())
    }

    /// Ask an actor using the direct actor frame envelope (MessageType::ActorAsk), aligned response.
    pub async fn ask_actor_frame_aligned(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
    ) -> Result<crate::AlignedBytes> {
        let started_at = Instant::now();
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        if let Err(e) = self
            .write_routed_actor_ask(correlation_id, actor_id, type_hash, payload)
            .await
        {
            // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
            warn!(
                addr = %self.addr,
                actor_id,
                type_hash,
                correlation_id,
                error = %e,
                stream_instance_id = ?self.stream_handle.as_ref().map(|handle| handle.instance_id()),
                stream_closed = self.is_closed(),
                bytes_written = self.bytes_written(),
                "transport_ask_write_failed"
            );
            return Err(e);
        }

        let result = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await;
        if let Err(error) = &result {
            match error {
                crate::GossipError::ConnectionDropped => {
                    warn!(
                        addr = %self.addr,
                        actor_id,
                        type_hash,
                        correlation_id,
                        elapsed_ms = started_at.elapsed().as_millis(),
                        timeout_ms = timeout.as_millis(),
                        stream_instance_id = ?self.stream_handle.as_ref().map(|handle| handle.instance_id()),
                        stream_closed = self.is_closed(),
                        bytes_written = self.bytes_written(),
                        "transport_ask_connection_dropped"
                    );
                }
                crate::GossipError::Timeout => {
                    debug!(
                        addr = %self.addr,
                        actor_id,
                        type_hash,
                        correlation_id,
                        elapsed_ms = started_at.elapsed().as_millis(),
                        timeout_ms = timeout.as_millis(),
                        stream_instance_id = ?self.stream_handle.as_ref().map(|handle| handle.instance_id()),
                        stream_closed = self.is_closed(),
                        bytes_written = self.bytes_written(),
                        "transport_ask_response_timeout"
                    );
                }
                _ => {}
            }
        }
        if result.is_ok() {
            let _ = slot.disarm();
        }
        result
    }

    /// Ask an actor using the direct actor frame envelope without timeout allocation.
    pub async fn ask_actor_frame_no_timeout(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> Result<bytes::Bytes> {
        let response = self
            .ask_actor_frame_no_timeout_aligned(actor_id, type_hash, payload)
            .await?;
        Ok(response.into_bytes())
    }

    /// Ask an actor using the direct actor frame envelope without timeout allocation, aligned response.
    pub async fn ask_actor_frame_no_timeout_aligned(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> Result<crate::AlignedBytes> {
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        if let Err(e) = self
            .write_routed_actor_ask(correlation_id, actor_id, type_hash, payload)
            .await
        {
            // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
            return Err(e);
        }

        let result = self
            .correlation
            .wait_for_response_no_timeout(correlation_id)
            .await;
        if result.is_ok() {
            let _ = slot.disarm();
        }
        result
    }

    /// Send an actor ask frame and return a deferred handle that can be awaited later.
    pub(crate) async fn ask_actor_frame_deferred(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
    ) -> Result<PendingAsk> {
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        if let Err(e) = self
            .write_routed_actor_ask(correlation_id, actor_id, type_hash, payload)
            .await
        {
            // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
            return Err(e);
        }

        Ok(PendingAsk {
            // Transfer slot ownership to PendingAsk: its own Drop becomes
            // responsible for cancelling the reservation if the handle is
            // dropped without being awaited.
            correlation_id: slot.disarm(),
            correlation: self.correlation.clone(),
            timeout,
            active: true,
        })
    }

    /// Tell with typed payload (rkyv) and debug-only type hash verification.
    pub async fn tell_typed<M>(&self, value: &M) -> Result<()>
    where
        M: crate::typed::WireEncode,
    {
        let payload = crate::typed::encode_typed_pooled(value)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<M>(payload);
        // Same bare-length raw header as `try_tell_bytes` (body_len ==
        // payload_len, no separate structured header -- hence 0) and the
        // same overflow-into-`WireKind` hazard -- see `reject_oversize_inline`.
        self.reject_oversize_inline(0, payload_len)?;
        let mut header = [0u8; 16];
        header[..4].copy_from_slice(&(payload_len as u32).to_be_bytes());
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
            stream_handle
                .write_pooled_control_inline(header, 4, prefix, prefix_len, payload)
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// Send a pre-formatted, complete V5 frame (already has its control
    /// word and any structured header -- not a bare length prefix over
    /// unframed content).
    ///
    /// PR #183 review, round 14: same contract as `send_data` above --
    /// `message` must already be complete frame(s); see that method's doc
    /// comment for why.
    pub async fn send_binary_message(&self, message: bytes::Bytes) -> Result<()> {
        // Message already has length prefix, send as-is.
        self.write_bytes_control(message).await
    }

    /// Send a single tell payload.
    pub async fn tell(&self, message: bytes::Bytes) -> Result<()> {
        self.tell_bytes(message).await
    }

    /// Direct access to try_send for maximum performance testing
    pub fn try_send_direct(&self, _data: &[u8]) -> Result<()> {
        // Direct TCP doesn't support try_send - would need try_lock
        Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "use tell() for direct TCP writes",
        )))
    }

    /// Send raw bytes through existing connection.
    pub async fn send_raw(&self, data: bytes::Bytes) -> Result<()> {
        self.tell_bytes(data).await
    }

    /// Ask method for request-response.
    /// Returns response as Bytes.
    pub async fn ask(&self, request: bytes::Bytes) -> Result<bytes::Bytes> {
        // Use default timeout of 30 seconds
        self.ask_with_timeout_bytes(request, Duration::from_secs(30))
            .await
    }

    /// Ask using owned bytes to avoid payload copies. Returns response as Bytes.
    pub async fn ask_bytes(&self, request: bytes::Bytes) -> Result<bytes::Bytes> {
        self.ask_with_timeout_bytes(request, Duration::from_secs(30))
            .await
    }

    /// Ask with typed request/response (rkyv) and debug-only type hash verification.
    pub async fn ask_typed<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            > + rkyv::Deserialize<Resp, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, Duration::from_secs(30))
            .await?;
        crate::typed::decode_typed::<Resp>(response.as_ref())
    }

    /// Ask with typed request/response and custom timeout.
    pub async fn ask_typed_with_timeout<Req, Resp>(
        &self,
        request: &Req,
        timeout: Duration,
    ) -> Result<Resp>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            > + rkyv::Deserialize<Resp, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, timeout)
            .await?;
        crate::typed::decode_typed::<Resp>(response.as_ref())
    }

    /// Ask with typed request and return a zero-copy archived response.
    pub async fn ask_typed_archived<Req, Resp>(
        &self,
        request: &Req,
    ) -> Result<crate::typed::ArchivedBytes<Resp>>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, Duration::from_secs(30))
            .await?;
        crate::typed::decode_typed_archived::<Resp>(response)
    }

    /// Ask with typed request and custom timeout, returning a zero-copy archived response.
    pub async fn ask_typed_archived_with_timeout<Req, Resp>(
        &self,
        request: &Req,
        timeout: Duration,
    ) -> Result<crate::typed::ArchivedBytes<Resp>>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, timeout)
            .await?;
        crate::typed::decode_typed_archived::<Resp>(response)
    }

    async fn ask_with_timeout_pooled(
        &self,
        mut payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        self.reject_oversize_inline(framing::ASK_RESPONSE_HEADER_LEN, payload_len)?;
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        let header = framing::try_write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            payload_len,
        )?;
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
            if let Err(e) = stream_handle
                .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
                .await
            {
                // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
                return Err(e);
            }
        } else {
            let mut body = BytesMut::with_capacity(payload_len);
            if let Some(prefix) = prefix {
                body.extend_from_slice(&prefix); // ALLOW_COPY
            }
            let payload_bytes = payload.copy_to_bytes(payload.remaining());
            body.extend_from_slice(payload_bytes.as_ref()); // ALLOW_COPY
            if let Err(e) = self
                .write_header_and_payload_ask_inline(header, 16, body.freeze())
                .await
            {
                // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
                return Err(e);
            }
        }

        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
        // wait_for_response always cleans up slot state on terminal returns
        // (Ok / Err); disarming skips the redundant Drop-time cancel CAS.
        let _ = slot.disarm();
        Ok(response.into_bytes())
    }

    /// Ask using owned bytes and custom timeout. Returns response as Bytes.
    pub async fn ask_with_timeout_bytes(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        self.reject_oversize_inline(framing::ASK_RESPONSE_HEADER_LEN, request.len())?;
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();

        let header = framing::try_write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        )?;

        if let Err(e) = self
            .write_header_and_payload_ask_inline(header, 16, request)
            .await
        {
            // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
            return Err(e);
        }

        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
        // wait_for_response always cleans up slot state on terminal returns
        // (Ok / Err); disarming skips the redundant Drop-time cancel CAS.
        let _ = slot.disarm();
        Ok(response.into_bytes())
    }

    /// Fast-path direct ask that bypasses the actor message handler.
    ///
    /// This is optimized for high-throughput request-response scenarios where
    /// the server can generate responses directly without spawning actor tasks.
    ///
    /// Wire format: [length:4][type:1][correlation_id:4][payload_len:4][payload:N]
    ///
    /// Uses the connection-local `correlation_id` as the frame's stable
    /// `request_id` too (it's already guaranteed nonzero by
    /// `CorrelationTracker::allocate`, which never hands out 0). That's
    /// stable enough for this method's own contract (no timeout/retry
    /// wrapping happens here), but it does NOT survive a transport reset --
    /// a caller that needs an id stable across reconnects/retries (e.g. to
    /// dedupe a retried ask server-side) must supply its own via
    /// `ask_direct_with_id`.
    pub async fn ask_direct(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        self.reject_oversize_inline(framing::DIRECT_ASK_HEADER_LEN, request.len())?;
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        self.ask_direct_on_slot(correlation_id, u64::from(correlation_id), request, timeout, slot)
            .await
    }

    /// Like `ask_direct`, but the caller supplies a stable `request_id` that
    /// survives across a transport reset/retry (unlike the connection-local
    /// `correlation_id`, which is recycled on every reconnect). Fail-closed:
    /// `request_id` must be nonzero -- 0 is the wire's reserved "absent"
    /// sentinel (see `framing::write_direct_ask_header`) and is rejected
    /// before anything is sent, rather than silently accepted as a
    /// valid-looking id that could collide across independent asks.
    pub async fn ask_direct_with_id(
        &self,
        request_id: u64,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        if request_id == 0 {
            return Err(GossipError::InvalidConfig(
                "ask_direct_with_id: request_id must be nonzero".to_string(),
            ));
        }
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        self.ask_direct_on_slot(correlation_id, request_id, request, timeout, slot)
            .await
    }

    async fn ask_direct_on_slot(
        &self,
        correlation_id: u32,
        request_id: u64,
        request: bytes::Bytes,
        timeout: Duration,
        slot: SlotGuard<'_>,
    ) -> Result<bytes::Bytes> {
        // Build DirectAsk header
        let header =
            framing::try_write_direct_ask_header(correlation_id, request_id, request.len())?;

        // Write header + payload inline (fast path)
        if let Err(e) = self.write_direct_ask_inline(header, request).await {
            // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
            return Err(e);
        }

        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
        // wait_for_response always cleans up slot state on terminal returns
        // (Ok / Err); disarming skips the redundant Drop-time cancel CAS.
        let _ = slot.disarm();
        Ok(response.into_bytes())
    }

    /// Fast-path direct ask without timeout allocation (benchmarking/hot path).
    ///
    /// See `ask_direct`'s doc for why `correlation_id` doubles as
    /// `request_id` here.
    pub async fn ask_direct_no_timeout(&self, request: bytes::Bytes) -> Result<bytes::Bytes> {
        self.reject_oversize_inline(framing::DIRECT_ASK_HEADER_LEN, request.len())?;
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();

        // Build DirectAsk header
        let header = framing::try_write_direct_ask_header(
            correlation_id,
            u64::from(correlation_id),
            request.len(),
        )?;

        // Write header + payload inline (fast path)
        if let Err(e) = self.write_direct_ask_inline(header, request).await {
            // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
            return Err(e);
        }

        let response = self
            .correlation
            .wait_for_response_no_timeout(correlation_id)
            .await?;
        let _ = slot.disarm();
        Ok(response.into_bytes())
    }

    pub async fn ask_streaming_bytes(
        &self,
        payload: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let stream_handle = self.stream_handle().map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "ask_streaming_bytes requires a stream-based connection",
            ))
        })?;
        if payload.is_empty() {
            return self.ask_actor_frame(actor_id, type_hash, payload, timeout).await;
        }
        let chunk_size = stream_handle.max_stream_chunk_size()?;
        // `SlotGuard` cancels this exact correlation slot on every `?` or
        // cancellation path below. Only a successful terminal response calls
        // `disarm`, after `wait_for_response` has removed the waiter.
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        let gate_guard = stream_handle.acquire_streaming_mode().await?;
        let stream_id = stream_handle.allocate_stream_id()?;
        // R-9: reject locally at MAX_STREAM_SIZE — every receiver hard-rejects a
        // larger stream as a FATAL error, so sending it would tear the
        // connection down with collateral loss. Cap before emitting any frame.
        if payload.len() > crate::MAX_STREAM_SIZE {
            return Err(GossipError::MessageTooLarge {
                size: payload.len(),
                max: crate::MAX_STREAM_SIZE,
            });
        }
        let total_size = u32::try_from(payload.len()).map_err(|_| GossipError::MessageTooLarge {
            size: payload.len(),
            max: crate::MAX_STREAM_SIZE,
        })?;
        let first_len = payload.len().min(chunk_size);
        let first_header = crate::framing::try_write_stream_request_start_header(
            stream_id,
            correlation_id,
            total_size,
            actor_id,
            type_hash,
            first_len,
        )?;
        stream_handle.write_bytes_vectored(
            first_header,
            payload.slice(..first_len),
        ).await?;
        // Armed only after StreamStart was accepted by the FIFO. From here on,
        // every cancellation path must release the peer-side reassembly.
        let mut abort_guard = StreamAbortGuard::new(stream_handle, stream_id);
        let mut offset = first_len;
        let mut index = 1u32;
        while offset < payload.len() {
            let end = (offset + chunk_size).min(payload.len());
            let header = crate::framing::try_write_stream_data_header(
                false,
                stream_id,
                index,
                end - offset,
            )?;
            if let Err(error) = stream_handle.write_bytes_vectored(
                header,
                payload.slice(offset..end),
            ).await {
                return Err(error);
            }
            offset = end;
            index = match index.checked_add(1) {
                Some(index) => index,
                None => {
                    return Err(GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "stream chunk index exhausted",
                    )));
                }
            };
        }
        // All stream frames are now queued in FIFO order. A caller cancelling
        // while it waits for the response cannot leave a partial reassembly.
        abort_guard.disarm();
        drop(gate_guard);
        let response = self.correlation.wait_for_response(correlation_id, timeout).await?;
        let _ = slot.disarm();
        Ok(response.into_bytes())
    }

    /// Send an ask and return a handle that can be awaited later.
    ///
    /// This is useful for:
    /// - delegating the "wait for response" to another task,
    /// - checking correlation IDs for diagnostics/tests without misusing `ReplyTo`,
    /// - keeping resources bounded (dropping cancels the pending slot).
    pub(crate) async fn ask_deferred(&self, request: bytes::Bytes) -> Result<PendingAsk> {
        self.ask_deferred_with_timeout_bytes(request, Duration::from_secs(30))
            .await
    }

    /// Deferred ask using owned bytes and a custom timeout.
    pub(crate) async fn ask_deferred_with_timeout_bytes(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<PendingAsk> {
        self.reject_oversize_inline(framing::ASK_RESPONSE_HEADER_LEN, request.len())?;
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();

        let header = framing::try_write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        )?;

        if let Err(e) = self
            .write_header_and_payload_ask_inline(header, 16, request)
            .await
        {
            // SlotGuard `slot` will cancel on scope exit; no explicit call needed.
            return Err(e);
        }

        Ok(PendingAsk {
            // Transfer slot ownership to PendingAsk: its own Drop becomes
            // responsible for cancelling the reservation if the handle is
            // dropped without being awaited.
            correlation_id: slot.disarm(),
            correlation: self.correlation.clone(),
            timeout,
            active: true,
        })
    }

    /// Batch ask in a single write, returning deferred handles for each request.
    #[allow(dead_code)]
    pub(crate) async fn ask_batch_deferred(
        &self,
        requests: &[&[u8]],
        timeout: Duration,
    ) -> Result<Vec<PendingAsk>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        // Validate every request's encoded size *before* touching the
        // allocator. `reject_oversize_inline` used to run inside the loop
        // below, after `BytesMut::with_capacity(total_size)` had already
        // reserved space for the whole batch -- so a batch with one
        // oversized request (which must return `MessageTooLarge`) still
        // paid for a `total_size` allocation sized off that same oversized
        // request first. Computing `total_size` only from lengths that have
        // already cleared the gate means a caller-supplied giant request can
        // never reach the allocator at all, let alone abort the process on
        // it before this check would have rejected it.
        let mut total_size = 0usize;
        for request in requests {
            self.reject_oversize_inline(framing::ASK_RESPONSE_HEADER_LEN, request.len())?;
            total_size += framing::ASK_RESPONSE_FRAME_HEADER_LEN + request.len();
        }

        // Hold each reservation in an RAII guard so a partial-batch failure
        // (allocate err, write err, etc.) auto-cancels every slot we already
        // claimed via `Vec<SlotGuard>` Drop.
        let mut slots: Vec<SlotGuard<'_>> = Vec::with_capacity(requests.len());
        let mut batch_message = bytes::BytesMut::with_capacity(total_size);

        for request in requests {
            let slot = self.correlation.allocate()?;

            let header = framing::try_write_ask_response_header(
                crate::MessageType::Ask,
                slot.id(),
                request.len(),
            )?;
            batch_message.extend_from_slice(&header); // ALLOW_COPY
            batch_message.extend_from_slice(request); // ALLOW_COPY
            slots.push(slot);
        }

        // Each request's header was built from `request.len()` above and
        // every request individually cleared `reject_oversize_inline`
        // before concatenation, but the aggregate `batch_message` is
        // expected to exceed `max_message_size` by design (it is the sum of
        // N independently-admitted requests, not one frame) -- so this must
        // use the trusted lane, not the generic one, which would otherwise
        // reject a legitimately large batch on the same bare length ceiling
        // that protects arbitrary caller bytes.
        let send_result = if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .write_trusted_bytes_ask(batch_message.freeze())
                .await
        } else {
            self.write_trusted_bytes_control(batch_message.freeze())
                .await
        };
        // Send-failure path: returning Err drops `slots`, which cancels every
        // reservation. No explicit per-id cancel loop needed.
        send_result?;

        let handles = slots
            .into_iter()
            .map(|slot| PendingAsk {
                // Transfer slot ownership: PendingAsk's own Drop is now
                // responsible for cancelling this reservation if the handle
                // is abandoned without being awaited.
                correlation_id: slot.disarm(),
                correlation: self.correlation.clone(),
                timeout,
                active: true,
            })
            .collect();
        Ok(handles)
    }

    /// Zero-copy vectored write for header + payload in single syscall
    /// This eliminates the need to copy payload data into frame buffer
    pub async fn write_bytes_vectored<const N: usize>(
        &self,
        header: [u8; N],
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_bytes_vectored(header, payload).await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// Abort a V5 stream without delivering its partial payload.
    pub async fn abort_stream(&self, stream_id: u32, reason: u32) -> Result<()> {
        self.stream_handle()?.abort_stream(stream_id, reason).await
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_owned_chunks(chunks)
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }
}

#[cfg(test)]
mod ask_nack_send_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn test_addr() -> SocketAddr {
        "127.0.0.1:19998".parse().expect("valid test addr")
    }

    #[tokio::test]
    async fn send_ask_nack_writes_the_wire_nack_frame() {
        let (io, mut peer) = tokio::io::duplex(256);
        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);
        let correlation = CorrelationTracker::new();
        let conn: ConnectionHandle =
            ConnectionHandle::new_stream(test_addr(), stream_handle, correlation);

        conn.send_ask_nack(77, crate::framing::AskNackReason::HandlerError)
            .await
            .expect("send_ask_nack must succeed");

        let mut frame = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
        peer.read_exact(&mut frame)
            .await
            .expect("peer must receive the NACK frame");

        let control = crate::framing::decode_control(frame[..4].try_into().unwrap())
            .expect("valid control word");
        assert_eq!(control.kind, crate::framing::WireKind::Response);
        assert_eq!(u32::from_be_bytes(frame[4..8].try_into().unwrap()), 77);
        assert_eq!(
            crate::framing::ask_nack_reason(&frame[4..]),
            Some(crate::framing::AskNackReason::HandlerError)
        );
    }
}

/// R-D (P1): routed-pubsub DATA-plane frames must never share admission
/// capacity with the `immediate_write_queue` that PR #157 reserved exclusively
/// for latency-critical control-plane replies (see the invariant documented
/// at `stream_writer.rs` on `LockFreeStreamHandle::immediate_write_queue`).
/// Before the fix, `try_send_pubsub_payload{,_pooled}` enqueued onto that same
/// 128-slot lane, which both (a) shrank pubsub burst admission from the
/// normal write queue's 1024 slots down to 128, and (b) let a pubsub burst
/// fill the lane so a genuine `AskResponder::try_reply_bytes_immediate` call
/// gets `WriteQueueFull` even though its capacity was supposed to be reserved.
#[cfg(test)]
mod pubsub_lane_tests {
    use super::*;
    use crate::ask_responder::AskResponder;

    fn test_addr() -> SocketAddr {
        "127.0.0.1:19999".parse().expect("valid test addr")
    }

    /// Builds a `ConnectionHandle` (the exact production entry point routed
    /// pubsub sends through) plus the underlying stream handle so the test can
    /// also mint an `AskResponder` for the reserved control-reply path.
    ///
    /// The duplex's peer end is intentionally never read from, and the test
    /// bodies never `.await` before their assertions, so the background writer
    /// task (spawned but not yet polled) never drains either queue — both
    /// stay genuinely full for the duration of the check.
    fn make_handle() -> (ConnectionHandle, Arc<LockFreeStreamHandle>, JoinHandle<()>) {
        let (client, _peer) = tokio::io::duplex(64);
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);
        let correlation = CorrelationTracker::new();
        let conn = ConnectionHandle::new_stream(test_addr(), stream_handle.clone(), correlation);
        (conn, stream_handle, task)
    }

    /// RED (starvation): fill the immediate lane with routed-pubsub frames via
    /// the production `try_send_pubsub_payload` path, then assert a
    /// control-plane immediate reply is STILL admitted. Fails today because
    /// pubsub and control replies share the same 128-slot
    /// `immediate_write_queue`.
    #[tokio::test]
    async fn pubsub_burst_must_not_starve_reserved_control_reply_capacity() {
        let (conn, stream_handle, _task) = make_handle();

        let mut filled = 0usize;
        for _ in 0..256 {
            match conn.try_send_pubsub_payload(bytes::Bytes::from_static(b"pubsub-burst")) {
                Ok(()) => filled += 1,
                Err(_) => break,
            }
        }
        assert!(
            filled >= 128,
            "expected the pubsub burst to saturate at least the historical \
             128-slot immediate lane; only {filled} frames were admitted"
        );

        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(1, stream_handle.clone(), used);
        let result = responder.try_reply_bytes_immediate(bytes::Bytes::from_static(b"reply"));
        assert!(
            result.is_ok(),
            "control-plane reply must not be starved by a pubsub burst sharing \
             the reserved immediate lane (R-D): {result:?}"
        );
    }

    /// RED (capacity regression): pre-#157, routed pubsub shared the normal
    /// write queue (`DEFAULT_ASK_WINDOW * 8` = 1024 slots). Routing it through
    /// the 128-slot immediate lane instead silently drops burst frames 129+
    /// that the old queue would have absorbed.
    #[tokio::test]
    async fn pubsub_burst_capacity_must_not_regress_below_normal_write_queue() {
        let (conn, _stream_handle, _task) = make_handle();

        let mut filled = 0usize;
        for _ in 0..2000 {
            match conn.try_send_pubsub_payload(bytes::Bytes::from_static(b"x")) {
                Ok(()) => filled += 1,
                Err(_) => break,
            }
        }
        assert!(
            filled >= 1024,
            "pubsub burst capacity regressed: only {filled} frames were \
             admitted before WriteQueueFull (expected >= 1024, matching the \
             normal write queue restored by the R-D fix)"
        );
    }
}

/// Oversized INLINE sends (tell/ask/pubsub) were unguarded: a payload above
/// the peer's `max_message_size` built a valid frame that the receiver then
/// hard-rejected as `MessageTooLarge`, tearing the whole connection down for
/// every other actor sharing it; a payload at/above the V5 27-bit
/// body-length limit panicked `framing::checked_body_len`'s old `.expect()`.
/// `reject_oversize_inline` (and the framing.rs `Result` fix) close both --
/// every family below must fail locally instead, and the connection must
/// still carry a normal-size message afterward.
#[cfg(test)]
mod oversized_inline_send_gate_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct GateTestMsg {
        data: Vec<u8>,
    }
    crate::wire_type!(
        GateTestMsg,
        "connection_pool::handle::oversized_inline_send_gate_tests::GateTestMsg"
    );

    fn test_addr() -> SocketAddr {
        "127.0.0.1:29998".parse().expect("valid test addr")
    }

    /// `max_message_size` defaults to `MASTER_BUFFER_SIZE` (1 MiB) when no
    /// `ReadContext` is supplied (see `LockFreeStreamHandle::new`) -- small
    /// enough that "one byte over the limit" tests allocate ~1 MiB, not
    /// hundreds of MiB, and still exercise the exact gate a real connection
    /// (configured with `GossipConfig::max_message_size`) would apply.
    fn make_handle() -> (
        ConnectionHandle,
        Arc<LockFreeStreamHandle>,
        JoinHandle<()>,
        tokio::io::DuplexStream,
    ) {
        let (client, peer) = tokio::io::duplex(4 * 1024 * 1024);
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);
        let correlation = CorrelationTracker::new();
        let conn = ConnectionHandle::new_stream(test_addr(), stream_handle.clone(), correlation);
        (conn, stream_handle, task, peer)
    }

    /// One byte over the default `max_message_size` (`MASTER_BUFFER_SIZE`).
    const OVERSIZED: usize = MASTER_BUFFER_SIZE + 1;

    /// After an oversized send was rejected, a normal-size send on the same
    /// connection must still go through and decode as a clean V5 frame --
    /// proving the gate rejected the oversized payload locally instead of
    /// desyncing or tearing down the connection.
    async fn assert_connection_still_carries_a_normal_message(
        conn: &ConnectionHandle,
        peer: &mut tokio::io::DuplexStream,
    ) {
        conn.tell_bytes(bytes::Bytes::from_static(b"still-alive"))
            .await
            .expect("connection must still accept a normal-size send");
        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        peer.read_exact(&mut ctrl)
            .await
            .expect("connection must still deliver bytes to the peer");
        let control = crate::framing::decode_control(ctrl)
            .expect("subsequent frame must decode as a valid V5 control word");
        assert_eq!(control.kind, crate::framing::WireKind::Gossip);
        assert_eq!(control.body_len, b"still-alive".len());
    }

    #[tokio::test]
    async fn raw_tell_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .tell_bytes(bytes::Bytes::from(vec![0u8; OVERSIZED]))
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn try_raw_tell_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .try_tell_bytes(bytes::Bytes::from(vec![0u8; OVERSIZED]))
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn actor_frame_tell_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .tell_actor_frame(7, 9, bytes::Bytes::from(vec![0u8; OVERSIZED]))
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn try_actor_frame_tell_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .try_tell_actor_frame(7, 9, bytes::Bytes::from(vec![0u8; OVERSIZED]))
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn typed_tell_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let big = GateTestMsg {
            data: vec![0u8; OVERSIZED],
        };
        let err = conn.tell_typed(&big).await.unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn bytes_ask_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .ask_with_timeout_bytes(bytes::Bytes::from(vec![0u8; OVERSIZED]), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn typed_ask_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let big = GateTestMsg {
            data: vec![0u8; OVERSIZED],
        };
        let err = conn
            .ask_typed::<GateTestMsg, GateTestMsg>(&big)
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// `ask_actor_frame` -> `ask_actor_frame_aligned` -> `write_routed_actor_ask`
    /// (`stream_writer.rs`), the routed-ask family sharing a connection-local
    /// route slot cache.
    #[tokio::test]
    async fn routed_actor_ask_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .ask_actor_frame(
                7,
                9,
                bytes::Bytes::from(vec![0u8; OVERSIZED]),
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn pubsub_publish_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .try_send_pubsub_payload(bytes::Bytes::from(vec![0u8; OVERSIZED]))
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// A 2^27-byte payload is caught by `reject_oversize_inline`
    /// (`max_message_size` defaults to `MASTER_BUFFER_SIZE`, 1 MiB, far below
    /// 2^27) before `framing::write_actor_tell_header` is ever called, so
    /// this proves the entry point itself never lets a caller observe a
    /// panic at this size -- not that `framing`'s own 27-bit boundary
    /// handling is exercised here. That boundary (the old panicking
    /// `checked_body_len`/`encode_control`) is covered directly, independent
    /// of any entry-point gate, by
    /// `framing::tests::oversize_body_returns_message_too_large_not_panic`.
    #[tokio::test]
    async fn actor_frame_tell_at_2_27_bytes_errors_not_panics() {
        let (conn, _stream_handle, _task, _peer) = make_handle();
        let huge = bytes::Bytes::from(vec![0u8; (1usize << 27) + 4096]);
        let err = conn.tell_actor_frame(1, 1, huge).await.unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
    }

    /// The raw-tell header (`try_tell_bytes`) is a bare length with no
    /// `WireKind` bits of its own (see the module note on
    /// `reject_oversize_inline`): before this fix, a length at/above 2^27
    /// would bleed into what `decode_control` reads back as the kind,
    /// desyncing the peer's parser with no local diagnostic. Proving nothing
    /// was ever enqueued is the strongest form of "no corrupted frame":
    /// there is no frame at all.
    #[tokio::test]
    async fn raw_tell_oversize_never_enqueues_a_corrupted_frame() {
        let (conn, _stream_handle, _task, _peer) = make_handle();
        let before = conn.sequence_number();
        let err = conn
            .try_tell_bytes(bytes::Bytes::from(vec![0u8; (1usize << 27) + 4096]))
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_eq!(
            conn.sequence_number(),
            before,
            "an oversized raw tell must be rejected before anything is queued"
        );
    }

    /// `max_message_size` bounds the *encoded* frame body, not the raw
    /// payload: `payload.len()` alone sits exactly at the limit (a
    /// payload-only check would admit it), but every structured frame kind
    /// below adds a fixed header on top, so the true encoded body exceeds
    /// the limit and must still be rejected locally -- not enqueued, sent,
    /// and then torn down by the peer's own `MessageTooLarge` rejection.
    #[tokio::test]
    async fn actor_frame_tell_body_len_with_header_overhead_over_limit_is_rejected() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let payload = bytes::Bytes::from(vec![0u8; MASTER_BUFFER_SIZE]);
        let err = conn.tell_actor_frame(7, 9, payload).await.unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn bytes_ask_body_len_with_header_overhead_over_limit_is_rejected() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let request = bytes::Bytes::from(vec![0u8; MASTER_BUFFER_SIZE]);
        let err = conn
            .ask_with_timeout_bytes(request, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn pubsub_publish_body_len_with_header_overhead_over_limit_is_rejected() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let payload = bytes::Bytes::from(vec![0u8; MASTER_BUFFER_SIZE]);
        let err = conn.try_send_pubsub_payload(payload).unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// A fresh handle has no bound route yet, so this goes through the
    /// unbound-route fallback branch of `write_routed_actor_ask`
    /// (`ACTOR_ASK_HEADER_LEN` = 28 bytes overhead, wider than the routed
    /// branch's 12) -- the larger of the two overheads this gate must get
    /// right.
    #[tokio::test]
    async fn routed_actor_ask_body_len_with_header_overhead_over_limit_is_rejected() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let payload = bytes::Bytes::from(vec![0u8; MASTER_BUFFER_SIZE]);
        let err = conn
            .ask_actor_frame(7, 9, payload, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn send_response_bytes_body_len_with_header_overhead_over_limit_is_rejected() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let payload = bytes::Bytes::from(vec![0u8; MASTER_BUFFER_SIZE]);
        let err = conn.send_response_bytes(1, payload).await.unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// `send_response_bytes`/`send_response_buf`/`send_response_pooled` never
    /// called the size gate at all: a response above `max_message_size` (but
    /// below the 27-bit wire ceiling, so `framing` alone would not catch it)
    /// was still enqueued and the peer fatally rejected it, tearing the
    /// connection down. Each response path must now reject locally instead.
    #[tokio::test]
    async fn send_response_bytes_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let err = conn
            .send_response_bytes(1, bytes::Bytes::from(vec![0u8; OVERSIZED]))
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    #[tokio::test]
    async fn send_response_buf_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let payload = bytes::Bytes::from(vec![0u8; OVERSIZED]);
        let payload_len = payload.len();
        let err = conn
            .send_response_buf(1, payload, payload_len)
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// PR #183 review, second round: `send_response_buf` builds its header
    /// from the caller-declared `payload_len`, then chains it onto the
    /// caller-supplied `Buf` -- which is a *different* value with its own,
    /// independent `remaining()`. A caller passing an in-bounds
    /// `payload_len` alongside a `Buf` whose actual `remaining()` is larger
    /// must not be allowed to reach the wire: unlike every other oversize
    /// case in this suite, this does not produce a well-formed frame the
    /// peer cleanly rejects as `MessageTooLarge` -- it writes bytes past
    /// the frame boundary the header already declared, and the peer reads
    /// that tail as the next control word. A payload-only equality check
    /// would not catch this if it only compared against `max_message_size`;
    /// it has to compare the buffer's actual length against the header's
    /// declared length directly.
    #[tokio::test]
    async fn send_response_buf_whose_remaining_exceeds_declared_payload_len_is_rejected() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        // Both individually well within max_message_size -- this must not be
        // caught by the size gate, only by the declared-vs-actual mismatch.
        let declared_payload_len = 8;
        let actual_payload = bytes::Bytes::from(vec![0u8; 4096]);
        let err = conn
            .send_response_buf(1, actual_payload, declared_payload_len)
            .await
            .unwrap_err();
        assert!(
            !matches!(err, GossipError::MessageTooLarge { .. }),
            "a small declared length must not be reported as MessageTooLarge: {err:?}"
        );
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// Same gap, the other direction: a `Buf` whose actual `remaining()` is
    /// *smaller* than the header's declared length would leave the frame
    /// short -- the peer either blocks waiting for bytes that were never
    /// declared as a separate message, or consumes a later, unrelated
    /// write's bytes as this frame's tail. Also must be refused, not merely
    /// the over-length direction.
    #[tokio::test]
    async fn send_response_buf_whose_remaining_is_less_than_declared_payload_len_is_rejected() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let declared_payload_len = 4096;
        let actual_payload = bytes::Bytes::from(vec![0u8; 8]);
        let err = conn
            .send_response_buf(1, actual_payload, declared_payload_len)
            .await
            .unwrap_err();
        assert!(
            !matches!(err, GossipError::MessageTooLarge { .. }),
            "a mismatched-but-small declared length must not be reported as \
             MessageTooLarge: {err:?}"
        );
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// The gate must not false-reject when `remaining()` and the declared
    /// length genuinely agree -- proving the mismatch checks above are not
    /// simply rejecting every `Buf` write.
    #[tokio::test]
    async fn send_response_buf_with_matching_declared_and_actual_length_is_written() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let payload = bytes::Bytes::from_static(b"consistent");
        let payload_len = payload.len();
        conn.send_response_buf(1, payload.clone(), payload_len)
            .await
            .expect("a Buf whose remaining() matches the declared length must be sent");
        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        peer.read_exact(&mut ctrl).await.unwrap();
        let control = crate::framing::decode_control(ctrl).unwrap();
        assert_eq!(control.kind, crate::framing::WireKind::Response);
        assert_eq!(
            control.body_len,
            crate::framing::ASK_RESPONSE_HEADER_LEN + payload_len
        );
    }

    #[tokio::test]
    async fn send_response_pooled_over_max_message_size_errors_and_connection_survives() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let big = GateTestMsg {
            data: vec![0u8; OVERSIZED],
        };
        let pooled = crate::typed::encode_typed_pooled(&big).unwrap();
        let payload_len = pooled.len();
        let err = conn
            .send_response_pooled(1, pooled, None, payload_len)
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_connection_still_carries_a_normal_message(&conn, &mut peer).await;
    }

    /// `ask_batch_deferred` used to validate each request's size *inside*
    /// the loop that also allocates the shared `batch_message` buffer,
    /// after `BytesMut::with_capacity(total_size)` had already reserved
    /// space for the full (unvalidated) batch. Reordering to validate every
    /// length first means an oversized member is still rejected correctly
    /// -- this pins that observable contract; the allocation-avoidance
    /// itself is a structural property of the reorder (see the fix's
    /// comment), not something a safe unit test can force an allocator
    /// abort to prove without risking the test process.
    #[tokio::test]
    async fn ask_batch_deferred_rejects_a_batch_with_an_oversized_member() {
        let (conn, _stream_handle, _task, _peer) = make_handle();
        let small = b"ok".as_slice();
        let big = vec![0u8; OVERSIZED];
        let requests: Vec<&[u8]> = vec![small, &big, small];
        let err = conn
            .ask_batch_deferred(&requests, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
    }

    /// Baseline coverage: `ask_batch_deferred` had none before this PR.
    /// Every request in a valid batch reaches the wire as its own Ask
    /// frame, in order, in the one write.
    #[tokio::test]
    async fn ask_batch_deferred_sends_every_request_as_its_own_frame() {
        let (conn, _stream_handle, _task, mut peer) = make_handle();
        let requests: Vec<&[u8]> = vec![b"one".as_slice(), b"two".as_slice()];
        let handles = conn
            .ask_batch_deferred(&requests, Duration::from_secs(5))
            .await
            .expect("a valid batch must be accepted");
        assert_eq!(handles.len(), requests.len());

        for request in &requests {
            let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
            peer.read_exact(&mut ctrl).await.unwrap();
            let control = crate::framing::decode_control(ctrl).unwrap();
            assert_eq!(control.kind, crate::framing::WireKind::Ask);
            assert_eq!(
                control.body_len,
                crate::framing::ASK_RESPONSE_HEADER_LEN + request.len()
            );
            let mut rest = vec![0u8; crate::framing::ASK_RESPONSE_HEADER_LEN + request.len()];
            peer.read_exact(&mut rest).await.unwrap();
            assert_eq!(&rest[rest.len() - request.len()..], *request);
        }
    }

    fn make_handle_with_max_message_size(
        max_message_size: usize,
    ) -> (
        ConnectionHandle,
        Arc<LockFreeStreamHandle>,
        JoinHandle<()>,
        tokio::io::DuplexStream,
    ) {
        let (client, peer) = tokio::io::duplex(4 * 1024 * 1024);
        let read_context = ReadContext {
            streaming_state_handoff: None,
            registry_weak: std::sync::Weak::new(),
            peer_addr: test_addr(),
            session_source: test_addr(),
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
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_context),
        );
        let stream_handle = Arc::new(stream_handle);
        let correlation = CorrelationTracker::new();
        let conn = ConnectionHandle::new_stream(test_addr(), stream_handle.clone(), correlation);
        (conn, stream_handle, task, peer)
    }

    /// PR #183 review, round 5: `WritePayload::Single` (the generic,
    /// caller-facing variant) is now gated with a bare `max_message_size`
    /// ceiling. `ask_batch_deferred`'s pre-concatenated batch must *not* go
    /// through that lane -- its aggregate is expected to exceed
    /// `max_message_size` by design (each request was already admitted
    /// individually) -- so it was moved to the new `TrustedFrame` variant
    /// instead. This proves that move actually happened: a batch whose
    /// aggregate exceeds `max_message_size`, but whose every individual
    /// request stays within it, must still be accepted and sent whole.
    #[tokio::test]
    async fn ask_batch_deferred_aggregate_over_max_message_size_still_succeeds() {
        let max_message_size = 64;
        let (conn, _stream_handle, _task, mut peer) =
            make_handle_with_max_message_size(max_message_size);

        // Every request comfortably fits under max_message_size on its own,
        // but three of them together do not.
        let request = vec![9u8; max_message_size - crate::framing::ASK_RESPONSE_HEADER_LEN];
        let requests: Vec<&[u8]> = vec![&request, &request, &request];
        let aggregate_frame_bytes: usize = requests
            .iter()
            .map(|r| crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN + r.len())
            .sum();
        assert!(
            aggregate_frame_bytes > max_message_size,
            "test setup: the aggregate must exceed max_message_size for this to prove anything"
        );

        let handles = conn
            .ask_batch_deferred(&requests, Duration::from_secs(5))
            .await
            .expect(
                "a batch whose aggregate exceeds max_message_size, but whose members do \
                     not, must still be accepted",
            );
        assert_eq!(handles.len(), requests.len());

        let mut received = vec![0u8; aggregate_frame_bytes];
        peer.read_exact(&mut received)
            .await
            .expect("the whole batch must reach the wire in one piece");
    }
}
