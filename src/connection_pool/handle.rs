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

    /// Send pre-serialized data through this connection - LOCK-FREE
    pub async fn send_data(&self, data: Vec<u8>) -> Result<()> {
        self.write_bytes_control(bytes::Bytes::from(data)).await
    }

    /// Send raw bytes without any framing.
    pub async fn send_raw_bytes(&self, data: bytes::Bytes) -> Result<()> {
        self.write_bytes_control(data).await
    }

    /// Send a response payload with framing, without copying the payload.
    pub async fn send_response_bytes(
        &self,
        correlation_id: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );

        self.write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    /// Send a gossip payload with framing, without copying the payload.
    pub async fn send_gossip_payload(&self, payload: bytes::Bytes) -> Result<()> {
        let header = framing::write_gossip_frame_prefix(payload.len());
        self.write_header_and_payload_control_inline(
            header,
            crate::framing::GOSSIP_FRAME_HEADER_LEN as u8,
            payload,
        )
        .await
    }

    /// Send a routed PubSub payload with framing, without copying the payload.
    pub async fn send_pubsub_payload(&self, payload: bytes::Bytes) -> Result<()> {
        let header = framing::write_pubsub_frame_prefix(payload.len());
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
        let header = framing::write_pubsub_frame_prefix(payload.len());
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
        let header = framing::write_pubsub_frame_prefix(payload_len);
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
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            let header = framing::write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                payload_len,
            );
            let buf = bytes::Bytes::copy_from_slice(&header).chain(payload); // ALLOW_COPY
            stream_handle.write_buf_control(buf).await
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
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            let header = framing::write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                payload_len,
            );
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

    /// Send bytes without copying - TRUE ZERO-COPY
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
                let mut header = [0u8; 16];
                header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());

                self.write_header_and_payload_control_inline(header, 4, data)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// Non-blocking tell. Returns `GossipError::WriteQueueFull` on backpressure.
    pub fn try_tell_bytes(&self, data: bytes::Bytes) -> Result<()> {
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
        let header = crate::framing::write_actor_tell_header(actor_id, type_hash, payload.len());
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
        let header = crate::framing::write_actor_tell_header(actor_id, type_hash, payload.len());
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

    /// Send a pre-formatted binary message (already has length prefix).
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
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();
        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            payload_len,
        );
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
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();

        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );

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
    pub async fn ask_direct(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();

        // Build DirectAsk header
        let header = framing::write_direct_ask_header(correlation_id, request.len());

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
    pub async fn ask_direct_no_timeout(&self, request: bytes::Bytes) -> Result<bytes::Bytes> {
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();

        // Build DirectAsk header
        let header = framing::write_direct_ask_header(correlation_id, request.len());

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
        let first_header = crate::framing::write_stream_request_start_header(
            stream_id,
            correlation_id,
            total_size,
            actor_id,
            type_hash,
            first_len,
        );
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
            let header = crate::framing::write_stream_data_header(
                false,
                stream_id,
                index,
                end - offset,
            );
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
        let slot = self.correlation.allocate()?;
        let correlation_id = slot.id();

        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );

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

        // Hold each reservation in an RAII guard so a partial-batch failure
        // (allocate err, write err, etc.) auto-cancels every slot we already
        // claimed via `Vec<SlotGuard>` Drop.
        let mut slots: Vec<SlotGuard<'_>> = Vec::with_capacity(requests.len());

        // Pre-calculate total message size to avoid growth reallocations.
        let total_size: usize = requests
            .iter()
            .map(|req| framing::ASK_RESPONSE_FRAME_HEADER_LEN + req.len())
            .sum();
        let mut batch_message = bytes::BytesMut::with_capacity(total_size);

        for request in requests {
            let slot = self.correlation.allocate()?;

            let header = framing::write_ask_response_header(
                crate::MessageType::Ask,
                slot.id(),
                request.len(),
            );
            batch_message.extend_from_slice(&header); // ALLOW_COPY
            batch_message.extend_from_slice(request); // ALLOW_COPY
            slots.push(slot);
        }

        let send_result = if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_bytes_ask(batch_message.freeze()).await
        } else {
            self.write_bytes_control(batch_message.freeze()).await
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
