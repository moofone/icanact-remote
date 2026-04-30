/// Handle to send messages through a persistent connection - LOCK-FREE
#[derive(Clone)]
pub struct ConnectionHandle<T = ()> {
    pub addr: SocketAddr,
    // Stream-based writer path (TCP/TLS/Noise/QUIC stream transports).
    stream_handle: Option<Arc<LockFreeStreamHandle>>,
    // Native UDP datagram writer path.
    udp_writer: Option<UdpTransportWriter>,
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
            .field("udp_writer", &self.udp_writer)
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
    correlation_id: u16,
    correlation: Arc<CorrelationTracker>,
    timeout: Duration,
}

impl PendingAsk {
    pub(crate) fn correlation_id(&self) -> u16 {
        self.correlation_id
    }

    pub(crate) async fn wait(self) -> Result<bytes::Bytes> {
        let this = std::mem::ManuallyDrop::new(self);
        let correlation_id = this.correlation_id;
        let timeout = this.timeout;
        // `wait(self)` consumes the handle, so a successful/terminal wait no longer needs
        // the drop-time cancellation path that is only for abandoned pending asks.
        let correlation = unsafe { std::ptr::read(&this.correlation) };
        let response = correlation.wait_for_response(correlation_id, timeout).await?;
        Ok(response.into_bytes())
    }
}

impl Drop for PendingAsk {
    fn drop(&mut self) {
        self.correlation.cancel(self.correlation_id);
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
            udp_writer: None,
            schema_hash,
            correlation,
            _marker: PhantomData,
        }
    }

    fn new_udp(
        addr: SocketAddr,
        udp_socket: Arc<UdpSocket>,
        write_queue_capacity: usize,
        schema_hash: Option<u64>,
        correlation: Arc<CorrelationTracker>,
    ) -> Self {
        Self {
            addr,
            stream_handle: None,
            udp_writer: Some(crate::transport::make_datagram_writer(
                udp_socket,
                addr,
                write_queue_capacity,
            )),
            schema_hash,
            correlation,
            _marker: PhantomData,
        }
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

    #[inline]
    fn udp_writer(&self) -> Option<&UdpTransportWriter> {
        self.udp_writer.as_ref()
    }

    async fn write_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_bytes_control(data).await
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer.send_bytes(data).await
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
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer
                .send_header_and_payload16(header, header_len, payload)
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
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer.try_send_header_and_payload16(header, header_len, payload)
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    async fn write_header_and_payload_control_inline32(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .write_header_and_payload_control_inline32(header, payload)
                .await
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer.send_header_and_payload32(header, payload).await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    fn write_header_and_payload_control_inline32_nonblocking(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_header_and_payload_control_inline32_nonblocking(header, payload)
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer.try_send_header_and_payload32(header, payload)
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
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer
                .send_header_and_payload16(header, header_len, payload)
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    async fn write_header_and_payload_ask_inline32(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle
                .write_header_and_payload_ask_inline32(header, payload)
                .await
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer.send_header_and_payload32(header, payload).await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    async fn write_direct_ask_inline(&self, header: [u8; 16], payload: bytes::Bytes) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_direct_ask_inline(header, payload).await
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer
                .send_header_and_payload16(
                    header,
                    crate::framing::DIRECT_ASK_FRAME_HEADER_LEN as u8,
                    payload,
                )
                .await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    fn schema_hash(&self) -> Option<u64> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.schema_hash()
        } else {
            self.schema_hash
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
        correlation_id: u16,
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
    pub fn try_send_pubsub_payload(&self, payload: bytes::Bytes) -> Result<()> {
        let header = framing::write_pubsub_frame_prefix(payload.len());
        self.write_header_and_payload_control_inline_nonblocking(
            header,
            crate::framing::PUBSUB_FRAME_HEADER_LEN as u8,
            payload,
        )
    }

    /// Send a response using the inline write queue (never streaming).
    pub async fn send_response_auto(
        &self,
        correlation_id: u16,
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
        correlation_id: u16,
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
        correlation_id: u16,
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
        correlation_id: u16,
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
            let header = framing::write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                payload_len,
            );
            self.udp_writer()
                .ok_or_else(|| {
                    GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        format!("connection {} has no writer path", self.addr),
                    ))
                })?
                .send_header_prefix_pooled(header, 16, prefix, payload)
                .await
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
                "stream_large_message is not supported on UDP datagram transport",
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
            .unwrap_or(UDP_MAX_DATAGRAM_SIZE.saturating_sub(crate::framing::LENGTH_PREFIX_LEN))
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
        let schema_hash = self.schema_hash();
        let header = crate::framing::write_actor_frame_header(
            crate::MessageType::ActorTell,
            0,
            actor_id,
            type_hash,
            schema_hash,
            payload.len(),
        );
        self.write_header_and_payload_control_inline32(header, payload)
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
        let schema_hash = self.schema_hash();
        let header = crate::framing::write_actor_frame_header(
            crate::MessageType::ActorTell,
            0,
            actor_id,
            type_hash,
            schema_hash,
            payload.len(),
        );
        self.write_header_and_payload_control_inline32_nonblocking(header, payload)
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
        let correlation_id = self.correlation.allocate();
        let schema_hash = self.schema_hash();
        let header = crate::framing::write_actor_frame_header(
            crate::MessageType::ActorAsk,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload.len(),
        );
        if let Err(e) = self.write_header_and_payload_ask_inline32(header, payload).await {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response(correlation_id, timeout)
            .await
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
        let correlation_id = self.correlation.allocate();
        let schema_hash = self.schema_hash();
        let header = crate::framing::write_actor_frame_header(
            crate::MessageType::ActorAsk,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload.len(),
        );

        if let Err(e) = self.write_header_and_payload_ask_inline32(header, payload).await {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response_no_timeout(correlation_id)
            .await
    }

    /// Send an actor ask frame and return a deferred handle that can be awaited later.
    pub(crate) async fn ask_actor_frame_deferred(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
    ) -> Result<PendingAsk> {
        let correlation_id = self.correlation.allocate();
        let schema_hash = self.schema_hash();
        let header = crate::framing::write_actor_frame_header(
            crate::MessageType::ActorAsk,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload.len(),
        );

        if let Err(e) = self.write_header_and_payload_ask_inline32(header, payload).await {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        Ok(PendingAsk {
            correlation_id,
            correlation: self.correlation.clone(),
            timeout,
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
            self.udp_writer()
                .ok_or_else(|| {
                    GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        format!("connection {} has no writer path", self.addr),
                    ))
                })?
                .send_header_prefix_pooled(header, 4, prefix, payload)
                .await
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
        let correlation_id = self.correlation.allocate();
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
                self.correlation.cancel(correlation_id);
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
                self.correlation.cancel(correlation_id);
                return Err(e);
            }
        }

        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
        Ok(response.into_bytes())
    }

    /// Ask using owned bytes and custom timeout. Returns response as Bytes.
    pub async fn ask_with_timeout_bytes(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation.allocate();

        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );

        if let Err(e) = self
            .write_header_and_payload_ask_inline(header, 16, request)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
        Ok(response.into_bytes())
    }

    /// Fast-path direct ask that bypasses the actor message handler.
    ///
    /// This is optimized for high-throughput request-response scenarios where
    /// the server can generate responses directly without spawning actor tasks.
    ///
    /// Wire format: [length:4][type:1][correlation_id:2][payload_len:4][payload:N]
    pub async fn ask_direct(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation.allocate();

        // Build DirectAsk header
        let header = framing::write_direct_ask_header(correlation_id, request.len());

        // Write header + payload inline (fast path)
        if let Err(e) = self.write_direct_ask_inline(header, request).await {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
        Ok(response.into_bytes())
    }

    /// Fast-path direct ask without timeout allocation (benchmarking/hot path).
    pub async fn ask_direct_no_timeout(&self, request: bytes::Bytes) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation.allocate();

        // Build DirectAsk header
        let header = framing::write_direct_ask_header(correlation_id, request.len());

        // Write header + payload inline (fast path)
        if let Err(e) = self.write_direct_ask_inline(header, request).await {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        let response = self
            .correlation
            .wait_for_response_no_timeout(correlation_id)
            .await?;
        Ok(response.into_bytes())
    }

    /// Zero-copy streaming ask that takes Bytes directly.
    ///
    /// This version avoids copying the payload data by using Bytes::slice()
    /// to create views into the original buffer for each chunk. Only the
    /// small headers (52 bytes each) are copied.
    ///
    /// Use this when you already have a Bytes buffer to avoid an extra copy.
    ///
    /// # Arguments
    /// * `payload` - The message payload as Bytes (will be sliced, not copied)
    /// * `type_hash` - The type hash for the message
    /// * `actor_id` - The target actor ID
    /// * `timeout` - How long to wait for a response
    ///
    /// # Returns
    /// The response bytes from the actor
    pub async fn ask_streaming_bytes(
        &self,
        payload: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        use crate::{MessageType, StreamHeader, current_timestamp_nanos};

        let stream_handle = self.stream_handle().map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "ask_streaming_bytes is not supported on UDP datagram transport",
            ))
        })?;
        let chunk_size = stream_handle.max_stream_chunk_size()?;

        // Allocate correlation ID for the response
        let correlation_id = self.correlation.allocate();

        // Acquire streaming mode atomically
        while stream_handle
            .streaming_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tokio::task::yield_now().await;
        }

        // Ensure we release streaming mode on exit
        let _guard = StreamingGuard {
            flag: stream_handle.streaming_active.clone(),
        };

        // Generate unique stream ID
        let stream_id = current_timestamp_nanos();

        let schema_hash = stream_handle.schema_hash();

        // Helper to build stream header bytes (52 bytes total for header-only messages)
        fn build_stream_header_bytes(
            msg_type: MessageType,
            header: &StreamHeader,
            correlation_id: u16,
            schema_hash: Option<u64>,
        ) -> bytes::Bytes {
            // Message format: [length:4][type:1][correlation_id:2][reserved:9][header:36]
            let inner_size = 12 + StreamHeader::SERIALIZED_SIZE;
            let mut message = bytes::BytesMut::with_capacity(4 + inner_size);

            message.extend_from_slice(&(inner_size as u32).to_be_bytes()); // ALLOW_COPY
            message.put_u8(msg_type as u8);
            message.extend_from_slice(&correlation_id.to_be_bytes()); // ALLOW_COPY
            let mut reserved = [0u8; 9];
            crate::framing::write_schema_hash(&mut reserved, schema_hash);
            message.extend_from_slice(&reserved); // ALLOW_COPY 9 reserved bytes
            message.extend_from_slice(&header.to_bytes()); // ALLOW_COPY
            message.freeze()
        }

        // Helper to build chunk header bytes (for use with vectored write)
        fn build_chunk_header_bytes(
            header: &StreamHeader,
            correlation_id: u16,
            chunk_len: usize,
            schema_hash: Option<u64>,
        ) -> bytes::Bytes {
            // Message format: [length:4][type:1][correlation_id:2][reserved:9][header:36]
            // (chunk data follows separately via vectored write)
            let inner_size = 12 + StreamHeader::SERIALIZED_SIZE + chunk_len;
            let mut message =
                bytes::BytesMut::with_capacity(4 + 12 + StreamHeader::SERIALIZED_SIZE);

            message.extend_from_slice(&(inner_size as u32).to_be_bytes()); // ALLOW_COPY
            message.put_u8(MessageType::StreamData as u8);
            message.extend_from_slice(&correlation_id.to_be_bytes()); // ALLOW_COPY
            let mut reserved = [0u8; 9];
            crate::framing::write_schema_hash(&mut reserved, schema_hash);
            message.extend_from_slice(&reserved); // ALLOW_COPY 9 reserved bytes
            message.extend_from_slice(&header.to_bytes()); // ALLOW_COPY
            message.freeze()
        }

        let total_size = payload.len();

        // Send StreamStart header
        let start_header = StreamHeader {
            stream_id,
            total_size: total_size as u64,
            chunk_size: 0,
            chunk_index: 0,
            type_hash,
            actor_id,
        };

        let start_msg = build_stream_header_bytes(
            MessageType::StreamStart,
            &start_header,
            correlation_id,
            schema_hash,
        );
        if stream_handle
            .streaming_queue
            .try_push(StreamingCommand::WriteBytes(start_msg))
            .is_err()
        {
            self.correlation.cancel(correlation_id);
            return Err(GossipError::WriteQueueFull);
        }

        // Stream chunks using zero-copy slicing
        let mut offset = 0;
        let mut chunk_index = 0;

        while offset < total_size {
            let chunk_end = std::cmp::min(offset + chunk_size, total_size);
            let chunk_len = chunk_end - offset;

            // Zero-copy slice of the original Bytes buffer
            let chunk_data = payload.slice(offset..chunk_end);

            let data_header = StreamHeader {
                stream_id,
                total_size: total_size as u64,
                chunk_size: chunk_len as u32,
                chunk_index,
                type_hash,
                actor_id,
            };

            // Build header (52 bytes - small, ok to copy)
            let header_bytes =
                build_chunk_header_bytes(&data_header, correlation_id, chunk_len, schema_hash);

            // Send header + chunk data via OwnedChunks for vectored I/O
            // This avoids copying the chunk data into the header buffer
            if stream_handle
                .streaming_queue
                .try_push(StreamingCommand::OwnedChunks(vec![
                    header_bytes,
                    chunk_data,
                ]))
                .is_err()
            {
                self.correlation.cancel(correlation_id);
                return Err(GossipError::WriteQueueFull);
            }

            // Yield periodically to prevent blocking
            if chunk_index % 10 == 0 {
                let _ = stream_handle
                    .streaming_queue
                    .try_push(StreamingCommand::Flush);
                tokio::task::yield_now().await;
            }

            offset = chunk_end;
            chunk_index += 1;
        }

        // Send StreamEnd
        let end_msg = build_stream_header_bytes(
            MessageType::StreamEnd,
            &start_header,
            correlation_id,
            schema_hash,
        );
        if stream_handle
            .streaming_queue
            .try_push(StreamingCommand::WriteBytes(end_msg))
            .is_err()
        {
            self.correlation.cancel(correlation_id);
            return Err(GossipError::WriteQueueFull);
        }
        let _ = stream_handle
            .streaming_queue
            .try_push(StreamingCommand::Flush);

        debug!(
            "✅ STREAMING ASK (zero-copy): Streamed {} bytes in {} chunks, waiting for response",
            total_size, chunk_index
        );

        // Wait for response
        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
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
        let correlation_id = self.correlation.allocate();

        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );

        if let Err(e) = self
            .write_header_and_payload_ask_inline(header, 16, request)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        Ok(PendingAsk {
            correlation_id,
            correlation: self.correlation.clone(),
            timeout,
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

        let mut correlation_ids = Vec::with_capacity(requests.len());

        // Pre-calculate total message size to avoid growth reallocations.
        let total_size: usize = requests
            .iter()
            .map(|req| framing::ASK_RESPONSE_FRAME_HEADER_LEN + req.len())
            .sum();
        let mut batch_message = bytes::BytesMut::with_capacity(total_size);

        for request in requests {
            let correlation_id = self.correlation.allocate();
            correlation_ids.push(correlation_id);

            let header = framing::write_ask_response_header(
                crate::MessageType::Ask,
                correlation_id,
                request.len(),
            );
            batch_message.extend_from_slice(&header); // ALLOW_COPY
            batch_message.extend_from_slice(request); // ALLOW_COPY
        }

        let send_result = if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_bytes_ask(batch_message.freeze()).await
        } else {
            self.write_bytes_control(batch_message.freeze()).await
        };
        if let Err(e) = send_result {
            for correlation_id in correlation_ids {
                self.correlation.cancel(correlation_id);
            }
            return Err(e);
        }

        let handles = correlation_ids
            .into_iter()
            .map(|correlation_id| PendingAsk {
                correlation_id,
                correlation: self.correlation.clone(),
                timeout,
            })
            .collect();
        Ok(handles)
    }

    /// High-performance streaming API - send structured data with custom framing - LOCK-FREE
    pub async fn stream_send<M>(&self, data: &M) -> Result<()>
    where
        M: for<'a> rkyv::Serialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let stream_handle = self.stream_handle().map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "stream_send is not supported on UDP datagram transport",
            ))
        })?;
        // Serialize the data using rkyv for maximum performance
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(data)
            .map_err(crate::GossipError::Serialization)?;

        // Create stream frame: [frame_type, channel_id, flags, seq_id[2], payload_len[4]]
        let frame_header = StreamFrameHeader {
            frame_type: StreamFrameType::Data as u8,
            channel_id: ChannelId::TellAsk as u8,
            flags: 0,
            sequence_id: stream_handle.next_frame_sequence_id(),
            payload_len: payload.len() as u32,
        };

        let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header)
            .map_err(crate::GossipError::Serialization)?;

        // Combine header and payload for single write
        let mut combined = bytes::BytesMut::with_capacity(header_bytes.len() + payload.len());
        combined.extend_from_slice(&header_bytes); // ALLOW_COPY
        combined.extend_from_slice(&payload); // ALLOW_COPY

        // Enqueue into the background writer - NO MUTEX!
        stream_handle.write_bytes_nonblocking(combined.freeze())
    }

    /// High-performance streaming API - send batch of structured data - LOCK-FREE
    pub async fn stream_send_batch<M>(&self, batch: &[M]) -> Result<()>
    where
        M: for<'a> rkyv::Serialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let stream_handle = self.stream_handle().map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "stream_send_batch is not supported on UDP datagram transport",
            ))
        })?;
        if batch.is_empty() {
            return Ok(());
        }

        // Pre-allocate buffer for entire batch
        let mut total_payload = Vec::new();

        for item in batch {
            let payload = rkyv::to_bytes::<rkyv::rancor::Error>(item)
                .map_err(crate::GossipError::Serialization)?;

            let frame_header = StreamFrameHeader {
                frame_type: StreamFrameType::Data as u8,
                channel_id: ChannelId::TellAsk as u8,
                flags: if std::ptr::eq(item, batch.last().unwrap()) {
                    0
                } else {
                    StreamFrameFlags::More as u8
                },
                sequence_id: stream_handle.next_frame_sequence_id(),
                payload_len: payload.len() as u32,
            };

            let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header)
                .map_err(crate::GossipError::Serialization)?;
            total_payload.extend_from_slice(&header_bytes); // ALLOW_COPY
            total_payload.extend_from_slice(&payload); // ALLOW_COPY
        }

        // Enqueue into the background writer - NO MUTEX!
        stream_handle.write_bytes_nonblocking(bytes::Bytes::from(total_payload))
    }

    /// Get truly lock-free streaming handle - direct access to the internal handle
    pub fn get_lock_free_stream(&self) -> &Arc<LockFreeStreamHandle> {
        self.stream_handle
            .as_ref()
            .expect("lock-free stream handle is unavailable for UDP transport")
    }

    /// Zero-copy vectored write for header + payload in single syscall
    /// This eliminates the need to copy payload data into frame buffer
    pub async fn write_bytes_vectored(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_bytes_vectored(header, payload).await
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer.send_bytes_vectored(header, payload).await
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        if let Some(stream_handle) = self.stream_handle.as_ref() {
            stream_handle.write_owned_chunks(chunks)
        } else if let Some(udp_writer) = self.udp_writer() {
            udp_writer.try_send_chunks(chunks.as_slice())
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("connection {} has no writer path", self.addr),
            )))
        }
    }
}
