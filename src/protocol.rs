use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::{Buf, Bytes};
use tracing::{info, warn};

use crate::{
    GossipError, PeerId, Result,
    handle::{
        MessageReadResult, handle_raw_ask_request, handle_response_message, send_inline_response,
        send_inline_response_aligned, send_pooled_response, send_streaming_response,
    },
    registry::{ActorResponse, GossipRegistry, RegistryMessage},
};

/// How long an in-progress stream may go without receiving a chunk before it is
/// reaped. Measured from the last chunk, not from the start, so a legitimately
/// slow or large transfer is not evicted mid-flight.
const STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Absolute ceiling on a single stream's lifetime, so a peer cannot hold a slot
/// open forever by trickling one chunk just inside the idle timeout.
const MAX_STREAM_LIFETIME: std::time::Duration = std::time::Duration::from_secs(600);

/// Per-connection streaming state for managing partial streams
#[derive(Debug)]
pub struct StreamingState {
    active_streams: HashMap<u64, InProgressStream>,
    max_concurrent_streams: usize,
}

/// A stream that is currently being assembled
#[derive(Debug)]
struct InProgressStream {
    stream_id: u64,
    total_size: u64,
    type_hash: u32,
    actor_id: u64,
    correlation_id: u32,
    schema_hash: Option<u64>,
    received_size: usize,
    /// Pre-allocated aligned buffer for final message assembly.
    buffer: crate::PooledAlignedBuffer,
    /// Chunk stride used to calculate offsets.
    chunk_stride: Option<usize>,
    /// Bitmap of chunk indices already written, one bit per chunk.
    ///
    /// Sized once the stride is known (from the first chunk). Without it,
    /// completion was decided purely on the sum of received bytes, so a
    /// duplicated chunk could stand in for one that never arrived and the
    /// stream would assemble with an unwritten (zero-filled) hole.
    received_chunks: Vec<u64>,
    /// Number of chunks this stream expects, known once the stride is.
    expected_chunks: Option<usize>,
    /// Count of chunk indices seen more than once, for diagnostics.
    duplicate_chunks: u32,
    /// Timestamp when stream started (bounds total lifetime).
    started_at: std::time::Instant,
    /// Timestamp of the most recent chunk (drives idle reaping).
    last_activity: std::time::Instant,
}

impl InProgressStream {
    /// Records `chunk_index` as received. Returns `false` if it was already
    /// recorded (a duplicate), in which case the caller must not write the
    /// payload or advance `received_size` again.
    fn mark_chunk_received(&mut self, chunk_index: usize) -> bool {
        let word = chunk_index / 64;
        let bit = 1u64 << (chunk_index % 64);
        if word >= self.received_chunks.len() {
            return false;
        }
        if self.received_chunks[word] & bit != 0 {
            self.duplicate_chunks = self.duplicate_chunks.saturating_add(1);
            return false;
        }
        self.received_chunks[word] |= bit;
        true
    }

    /// True once every expected chunk index has been written.
    ///
    /// A stream whose stride is not yet known has received no chunks, so it is
    /// complete only if it declared no payload at all.
    fn all_chunks_received(&self) -> bool {
        let Some(expected) = self.expected_chunks else {
            return self.total_size == 0;
        };
        if expected == 0 {
            return true;
        }
        let full_words = expected / 64;
        if self.received_chunks[..full_words].iter().any(|w| *w != u64::MAX) {
            return false;
        }
        let remainder = expected % 64;
        if remainder == 0 {
            return true;
        }
        let mask = (1u64 << remainder) - 1;
        self.received_chunks[full_words] & mask == mask
    }
}

impl StreamingState {
    pub fn new() -> Self {
        Self {
            active_streams: HashMap::new(),
            max_concurrent_streams: 16, // Reasonable limit
        }
    }

    pub fn start_stream_with_correlation(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        schema_hash: Option<u64>,
    ) -> Result<()> {
        if self.active_streams.len() >= self.max_concurrent_streams {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "Too many concurrent streams",
            )));
        }

        let total_size = usize::try_from(header.total_size).map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stream size overflows usize",
            ))
        })?;

        if total_size > crate::MAX_STREAM_SIZE {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stream size {} exceeds MAX_STREAM_SIZE {}",
                    total_size,
                    crate::MAX_STREAM_SIZE
                ),
            )));
        }

        // Only insert if not already exists to avoid resetting progress on duplicate start frames
        if !self.active_streams.contains_key(&header.stream_id) {
            // Bound aggregate eager allocation across all in-flight streams on
            // this connection. A StreamStart pre-allocates its full declared
            // size, so cap the sum to keep a peer from opening many max-size
            // streams and forcing ~1 GiB of eager allocation (DoS). Summed on
            // demand over the (<= max_concurrent_streams) active entries.
            let inflight_bytes: usize = self
                .active_streams
                .values()
                .map(|s| s.total_size as usize)
                .sum();
            if inflight_bytes.saturating_add(total_size) > crate::MAX_INFLIGHT_STREAM_BYTES {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::ResourceBusy,
                    format!(
                        "per-connection in-flight stream budget exceeded: {} in flight + {} requested > {}",
                        inflight_bytes,
                        total_size,
                        crate::MAX_INFLIGHT_STREAM_BYTES
                    ),
                )));
            }
            let buffer = crate::PooledAlignedBuffer::with_len(total_size, pool);
            let stream = InProgressStream {
                stream_id: header.stream_id,
                total_size: header.total_size,
                type_hash: header.type_hash,
                actor_id: header.actor_id,
                correlation_id,
                schema_hash,
                received_size: 0,
                buffer,
                chunk_stride: None,
                received_chunks: Vec::new(),
                expected_chunks: None,
                duplicate_chunks: 0,
                started_at: std::time::Instant::now(),
                last_activity: std::time::Instant::now(),
            };
            self.active_streams.insert(header.stream_id, stream);
        }
        Ok(())
    }

    pub fn add_chunk_with_correlation(
        &mut self,
        header: crate::StreamHeader,
        chunk_data: Bytes,
        schema_hash: Option<u64>,
    ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
        // If stream doesn't exist, we might have missed the start frame or it was cleaned up
        if !self.active_streams.contains_key(&header.stream_id) {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk for unknown stream_id={}", header.stream_id),
            )));
        }

        let stream = self
            .active_streams
            .get_mut(&header.stream_id)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Received chunk for unknown stream_id={}", header.stream_id),
                ))
            })?;

        if header.total_size != stream.total_size {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stream total_size mismatch",
            )));
        }

        if stream.schema_hash != schema_hash {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stream schema hash mismatch",
            )));
        }

        if stream.received_size + chunk_data.len() > stream.total_size as usize {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk overflow for stream_id={}", header.stream_id),
            )));
        }

        if header.chunk_size as usize != chunk_data.len() {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk_size does not match chunk_data length",
            )));
        }

        if stream.chunk_stride.is_none() && header.chunk_size > 0 {
            let stride = header.chunk_size as usize;
            stream.chunk_stride = Some(stride);
            // The stride fixes how many chunks this stream consists of, which
            // in turn sizes the received-chunk bitmap.
            let expected = (stream.total_size as usize).div_ceil(stride);
            stream.expected_chunks = Some(expected);
            stream.received_chunks = vec![0u64; expected.div_ceil(64)];
        }

        let stride = stream.chunk_stride.unwrap_or(header.chunk_size as usize);
        if header.chunk_size as usize > stride {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk_size exceeds stream stride",
            )));
        }

        let chunk_index = header.chunk_index as usize;
        let offset = chunk_index
            .checked_mul(stride)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("chunk offset overflow for stream_id={}", header.stream_id),
                ))
            })?;
        let end = offset + chunk_data.len();
        if end > stream.total_size as usize {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk overflow for stream_id={}", header.stream_id),
            )));
        }

        if let Some(expected) = stream.expected_chunks {
            if chunk_index >= expected {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "chunk_index={} is past the {} chunks declared by stream_id={}",
                        chunk_index, expected, header.stream_id
                    ),
                )));
            }

            // Senders chunk with a fixed stride, so every chunk but the last is
            // exactly one stride and the last is the remainder. Pinning this
            // keeps the stride (and therefore the chunk count the completion
            // check relies on) consistent with what actually arrives.
            let want = if chunk_index + 1 == expected {
                stream.total_size as usize - offset
            } else {
                stride
            };
            if chunk_data.len() != want {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "chunk_index={} of stream_id={} has {} bytes, expected {}",
                        chunk_index,
                        header.stream_id,
                        chunk_data.len(),
                        want
                    ),
                )));
            }
        }

        // A repeated chunk index must not advance `received_size`: completion is
        // decided per chunk, and double-counting bytes is exactly what let a
        // retransmit stand in for a chunk that never arrived.
        if !stream.mark_chunk_received(chunk_index) {
            warn!(
                stream_id = header.stream_id,
                chunk_index = chunk_index,
                duplicates = stream.duplicate_chunks,
                "Ignoring duplicate stream chunk"
            );
            return Ok(None);
        }

        // CRITICAL_PATH: write chunk directly into final buffer.
        stream.buffer.as_mut_slice()[offset..end].copy_from_slice(&chunk_data);
        stream.received_size += chunk_data.len();
        // Progress, so the idle reaper must not treat this stream as stalled.
        stream.last_activity = std::time::Instant::now();

        if stream.all_chunks_received() {
            self.assemble_complete_message_with_correlation(header.stream_id)
        } else {
            Ok(None)
        }
    }

    pub fn finalize_stream_with_correlation(
        &mut self,
        stream_id: u64,
        schema_hash: Option<u64>,
    ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
        // StreamEnd received - assemble the message
        if let Some(stream) = self.active_streams.get(&stream_id) {
            if stream.schema_hash != schema_hash {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "stream schema hash mismatch",
                )));
            }
        }
        self.assemble_complete_message_with_correlation(stream_id)
    }

    fn assemble_complete_message_with_correlation(
        &mut self,
        stream_id: u64,
    ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
        let stream = self.active_streams.remove(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Cannot finalize unknown stream_id={}", stream_id),
            ))
        })?;

        // ACTOR_REM_2 R9: the buffer is pre-allocated (zero-filled) to the
        // declared `total_size`. A StreamEnd that arrives before every byte was
        // received must NOT deliver that partially-filled buffer as if it were a
        // complete message — a malicious peer (StreamStart(N) then an immediate
        // StreamEnd) or a sender that aborts mid-transfer would otherwise inject
        // zero-padded / truncated payloads. Reject the incomplete assembly.
        if (stream.received_size as u64) < stream.total_size {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stream_id={} finalized incomplete: {} of {} bytes received",
                    stream_id, stream.received_size, stream.total_size
                ),
            )));
        }

        // The byte count alone cannot prove the buffer is fully written -- that
        // was the gap a duplicated chunk exploited. Require every chunk index.
        if !stream.all_chunks_received() {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stream_id={} finalized with missing chunks ({} duplicates seen)",
                    stream_id, stream.duplicate_chunks
                ),
            )));
        }

        let correlation_id = stream.correlation_id;
        let schema_hash = stream.schema_hash;
        let complete_data = stream.buffer.into_aligned_bytes().into_bytes();

        info!(
            "✅ STREAMING: Assembled complete message for stream_id={} ({} bytes for actor={}, type_hash=0x{:x}, correlation_id={})",
            stream.stream_id,
            complete_data.len(),
            stream.actor_id,
            stream.type_hash,
            correlation_id
        );

        Ok(Some((complete_data, correlation_id, schema_hash)))
    }

    /// Number of streams currently in progress.
    pub fn active_stream_count(&self) -> usize {
        self.active_streams.len()
    }

    /// Clean up in-progress streams that have stopped making progress.
    ///
    /// Reaping is keyed on *idle* time, not age since the stream started. A
    /// large or slow-but-healthy transfer legitimately runs longer than the
    /// timeout, and evicting it mid-flight fails the sender's ask with no
    /// server-side diagnostic. `MAX_STREAM_LIFETIME` remains as a backstop so a
    /// peer cannot hold a slot open indefinitely by trickling one chunk forever.
    pub fn cleanup_stale(&mut self) {
        self.cleanup_stale_with(STREAM_IDLE_TIMEOUT, MAX_STREAM_LIFETIME);
    }

    /// `cleanup_stale` with explicit bounds, so tests do not have to wait out
    /// the production timeouts.
    pub(crate) fn cleanup_stale_with(
        &mut self,
        idle_timeout: std::time::Duration,
        max_lifetime: std::time::Duration,
    ) {
        let before_count = self.active_streams.len();

        self.active_streams.retain(|stream_id, stream| {
            let idle = stream.last_activity.elapsed();
            let age = stream.started_at.elapsed();

            if idle > idle_timeout {
                warn!(
                    stream_id = stream_id,
                    idle_secs = idle.as_secs(),
                    age_secs = age.as_secs(),
                    received_size = stream.received_size,
                    expected_size = stream.total_size,
                    "Cleaning up idle stream - no chunk arrived within the idle timeout"
                );
                return false;
            }

            if age > max_lifetime {
                warn!(
                    stream_id = stream_id,
                    idle_secs = idle.as_secs(),
                    age_secs = age.as_secs(),
                    received_size = stream.received_size,
                    expected_size = stream.total_size,
                    "Cleaning up stream that exceeded its maximum lifetime"
                );
                return false;
            }

            true
        });

        let removed = before_count - self.active_streams.len();
        if removed > 0 {
            info!(
                removed_count = removed,
                remaining = self.active_streams.len(),
                "Cleaned up stale in-progress streams"
            );
        }
    }
}

impl Default for StreamingState {
    fn default() -> Self {
        Self::new()
    }
}

fn registry_message_sender_peer_id(msg: &RegistryMessage) -> Option<&PeerId> {
    match msg {
        RegistryMessage::DeltaGossip { delta, .. }
        | RegistryMessage::DeltaGossipResponse { delta, .. } => Some(&delta.sender_peer_id),
        RegistryMessage::FullSyncRequest { sender_peer_id, .. }
        | RegistryMessage::FullSync { sender_peer_id, .. }
        | RegistryMessage::FullSyncResponse { sender_peer_id, .. } => Some(sender_peer_id),
        RegistryMessage::PeerHealthReport { reporter, .. } => Some(reporter),
        RegistryMessage::PeerHealthQuery { sender, .. } => Some(sender),
        RegistryMessage::PeerListGossip { .. } => None,
    }
}

/// Process a single read result result using the shared protocol logic.
///
/// This handles:
/// - Gossip messages -> registry.handle_incoming_message
/// - Raw Asks -> handle_raw_ask_request
/// - Responses -> handle_response_message
/// - Actor messages -> registry.actor_message_handler
/// - Streaming messages -> state.streaming (assembly) -> handler
pub(crate) async fn process_read_result(
    result: MessageReadResult,
    streaming_state: &mut StreamingState,
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    response_correlation: Option<&crate::connection_pool::CorrelationTracker>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    authenticated_peer_id: Option<&PeerId>,
) -> Result<()> {
    match result {
        MessageReadResult::Gossip(msg, _correlation_id) => {
            let authenticated_peer_id = authenticated_peer_id
                .or_else(|| response_connection.and_then(|conn| conn.embedded_peer_id.as_ref()));
            // Fail-closed: any gossip frame that carries a claimed sender
            // identity must be attributable to this connection's authenticated
            // identity. Drop if it mismatches OR if there is no authenticated
            // identity to verify against — mirroring the fail-closed PubSub path
            // below. On the live mutually-authenticated TLS path
            // `authenticated_peer_id` is always present, so this only closes
            // non-TLS / pre-identification paths where a forged `sender_peer_id`
            // would otherwise be accepted unchecked.
            if let Some(claimed) = registry_message_sender_peer_id(&msg) {
                match authenticated_peer_id {
                    Some(authenticated) if authenticated == claimed => {}
                    Some(authenticated) => {
                        warn!(
                            peer = %peer_addr,
                            authenticated_peer_id = %authenticated,
                            claimed_peer_id = %claimed,
                            "Dropping gossip message with mismatched authenticated peer identity"
                        );
                        return Ok(());
                    }
                    None => {
                        warn!(
                            peer = %peer_addr,
                            claimed_peer_id = %claimed,
                            "Dropping gossip message with a claimed sender but no authenticated peer identity"
                        );
                        return Ok(());
                    }
                }
            }

            if let Err(e) =
                crate::connection_pool::handle_incoming_message(registry.clone(), peer_addr, msg)
                    .await
            {
                warn!(error = %e, "Failed to process gossip message");
            }
        }
        MessageReadResult::AskRaw {
            correlation_id,
            payload,
        } => {
            handle_raw_ask_request(registry, peer_addr, correlation_id, &payload).await;
        }
        MessageReadResult::Response {
            correlation_id,
            payload,
        } => {
            handle_response_message(
                registry,
                peer_addr,
                correlation_id,
                payload,
                response_correlation,
            )
            .await;
        }
        MessageReadResult::PubSub { payload } => {
            let authenticated_peer_id = authenticated_peer_id
                .or_else(|| response_connection.and_then(|conn| conn.embedded_peer_id.as_ref()));
            if let Some(authenticated_peer_id) = authenticated_peer_id {
                if let Some(handler) = registry.pubsub_ingress_handler.load().as_ref() {
                    if let Err(e) = handler.handle(authenticated_peer_id, payload) {
                        warn!(peer = %peer_addr, error = %e, "Failed to process PubSub frame");
                    }
                }
            } else {
                warn!(peer = %peer_addr, "Dropping PubSub frame without authenticated peer identity");
            }
        }
        MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload,
        } => {
            // Handle actor message directly
            let corr_id = if msg_type == crate::MessageType::ActorAsk as u8 {
                correlation_id
            } else {
                0
            };
            let response_mode = if msg_type == crate::MessageType::ActorAsk as u8 {
                ResponseMode::AutoStream
            } else {
                ResponseMode::InlineOnly
            };
            handle_assembled_message(
                registry,
                peer_addr,
                authenticated_peer_id.or_else(|| {
                    response_connection.and_then(|conn| conn.embedded_peer_id.as_ref())
                }),
                actor_id,
                type_hash,
                payload,
                corr_id,
                schema_hash,
                response_connection,
                response_mode,
            )
            .await;
        }
        MessageReadResult::Streaming {
            msg_type,
            correlation_id,
            schema_hash,
            stream_header,
            chunk_data,
        } => {
            // Handle streaming messages
            match msg_type {
                msg_type
                    if msg_type == crate::MessageType::StreamStart as u8
                        || msg_type == crate::MessageType::StreamResponseStart as u8 =>
                {
                    let pool = registry.connection_pool.aligned_bytes_pool();
                    if let Err(e) = streaming_state.start_stream_with_correlation(
                        stream_header,
                        correlation_id,
                        pool,
                        schema_hash,
                    ) {
                        warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                    }
                }
                msg_type
                    if msg_type == crate::MessageType::StreamData as u8
                        || msg_type == crate::MessageType::StreamResponseData as u8 =>
                {
                    // Ensure stream is started (auto-start)
                    let pool = registry.connection_pool.aligned_bytes_pool();
                    if let Err(e) = streaming_state.start_stream_with_correlation(
                        stream_header,
                        correlation_id,
                        pool,
                        schema_hash,
                    ) {
                        let _ = e;
                    }

                    if let Ok(Some((complete_data, corr_id, schema_hash))) = streaming_state
                        .add_chunk_with_correlation(stream_header, chunk_data, schema_hash)
                    {
                        if msg_type == crate::MessageType::StreamResponseData as u8 {
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                response_correlation,
                            )
                            .await;
                        } else {
                            handle_assembled_message(
                                registry,
                                peer_addr,
                                authenticated_peer_id.or_else(|| {
                                    response_connection
                                        .and_then(|conn| conn.embedded_peer_id.as_ref())
                                }),
                                stream_header.actor_id,
                                stream_header.type_hash,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                corr_id,
                                schema_hash,
                                response_connection,
                                ResponseMode::AutoStream,
                            )
                            .await;
                        }
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamEnd as u8 => {
                    // StreamEnd indicates the end of an incoming REQUEST (streaming tell/ask)
                    if let Ok(Some((complete_data, corr_id, schema_hash))) = streaming_state
                        .finalize_stream_with_correlation(stream_header.stream_id, schema_hash)
                    {
                        handle_assembled_message(
                            registry,
                            peer_addr,
                            authenticated_peer_id.or_else(|| {
                                response_connection.and_then(|conn| conn.embedded_peer_id.as_ref())
                            }),
                            stream_header.actor_id,
                            stream_header.type_hash,
                            crate::AlignedBytes::from_bytes(complete_data)
                                .expect("stream buffer must be aligned"),
                            corr_id,
                            schema_hash,
                            response_connection,
                            ResponseMode::AutoStream,
                        )
                        .await;
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamResponseEnd as u8 => {
                    // StreamResponseEnd indicates the end of an incoming RESPONSE (from a remote ask)
                    if let Ok(Some((complete_data, corr_id, _schema_hash))) = streaming_state
                        .finalize_stream_with_correlation(stream_header.stream_id, schema_hash)
                    {
                        if corr_id != 0 {
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                response_correlation,
                            )
                            .await;
                        } else {
                            // Ignore streaming response with correlation_id=0
                        }
                    }
                }
                _ => {
                    warn!("Unknown streaming message type: 0x{:02x}", msg_type);
                }
            }
        }
        MessageReadResult::Raw(_payload) => {
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                if std::env::var("ICANACT_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
                    crate::test_helpers::record_raw_payload(_payload.clone());
                }
            }
        }
        MessageReadResult::DirectAsk {
            correlation_id,
            payload,
        } => {
            // Fast-path DirectAsk - bypasses handler and RegistryMessage overhead
            // V4 wire format carries a 32-bit correlation id.
            // But 'payload' here contains only the [payload:N] part
            // For benchmarking: echo the payload back immediately using DirectResponse
            let header =
                crate::framing::write_direct_response_header(correlation_id, payload.len());

            // Send DirectResponse using connection pool
            let pool = &registry.connection_pool;
            if let Some(conn) = pool.get_connection_by_addr(&peer_addr) {
                if let Some(ref stream_handle) = conn.stream_handle {
                    let payload_bytes: bytes::Bytes = payload.into();
                    if let Err(e) = stream_handle
                        .write_direct_response_inline(header, payload_bytes)
                        .await
                    {
                        warn!(peer = %peer_addr, error = %e, correlation_id, "Failed to send DirectResponse");
                    }
                }
            }
        }
        MessageReadResult::DirectResponse {
            correlation_id,
            payload,
        } => {
            // Fast-path DirectResponse
            // The payload is the raw response data (no length prefix)
            // V4 wire format carries a 32-bit correlation id.
            // But 'payload' here contains only the [payload:N] part
            // Deliver to correlation tracker - zero-copy using the payload directly
            handle_response_message(
                registry,
                peer_addr,
                correlation_id,
                payload,
                response_correlation,
            )
            .await;
        }
    }

    Ok(())
}

enum ResponseMode {
    InlineOnly,
    AutoStream,
}

fn should_stream_response(
    registry: &Arc<GossipRegistry>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    response_len: usize,
    response_mode: ResponseMode,
) -> bool {
    // Absolute correctness bound: the reader rejects frames larger than max_message_size.
    // `msg_len` on the wire is ASK_RESPONSE_HEADER_LEN + payload_len (length prefix excluded).
    let inline_payload_limit = registry
        .config
        .max_message_size
        .saturating_sub(crate::framing::ASK_RESPONSE_HEADER_LEN);
    if response_len > inline_payload_limit {
        return true;
    }

    match response_mode {
        ResponseMode::InlineOnly => false,
        ResponseMode::AutoStream => {
            // Prefer streaming for large-ish payloads to avoid huge inline frames.
            let _ = response_connection; // threshold is currently a global constant
            let threshold = crate::connection_pool::STREAMING_THRESHOLD;
            response_len > threshold
        }
    }
}

async fn handle_assembled_message(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    authenticated_peer_id: Option<&PeerId>,
    actor_id: u64,
    type_hash: u32,
    complete_data: crate::AlignedBytes,
    corr_id: u32,
    schema_hash: Option<u64>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    response_mode: ResponseMode,
) {
    // Complete message assembled - route to actor
    // corr_id == 0 means tell (fire-and-forget), non-zero means ask (expects response)
    let correlation_opt = if corr_id == 0 { None } else { Some(corr_id) };
    if let Some(expected) = registry.config.schema_hash {
        // CRITICAL_PATH: schema/version hash validation gate.
        if schema_hash != Some(expected) {
            warn!(
                peer = %peer_addr,
                expected = format_args!("{:016x}", expected),
                received = schema_hash.map(|hash| format!("{hash:016x}")).unwrap_or_else(|| "none".to_string()),
                "Rejected actor payload due to schema hash mismatch"
            );
            return;
        }
    }
    let response = if corr_id == 0 {
        if let Some(cell) = registry.actor_tell_handler_sync_context.load_full() {
            cell.handle(
                actor_id,
                type_hash,
                complete_data,
                crate::TellContext::new(authenticated_peer_id),
            )
            .map(|_| None)
        } else if let Some(cell) = registry.actor_tell_handler_sync.load_full() {
            cell.handle(actor_id, type_hash, complete_data)
                .map(|_| None)
        } else {
            registry
                .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                .await
        }
    } else if let Some(cell) = registry.actor_ask_immediate_handler_sync.load_full() {
        if cell.can_handle(actor_id, type_hash) {
            cell.handle(actor_id, type_hash, complete_data)
                .map(|disposition| match disposition {
                    crate::registry::AskDisposition::Immediate(response) => Some(response),
                    crate::registry::AskDisposition::ImmediateBytes(response) => {
                        Some(ActorResponse::Bytes(response))
                    }
                    crate::registry::AskDisposition::ImmediateAligned(response) => {
                        Some(ActorResponse::Aligned(response))
                    }
                    crate::registry::AskDisposition::ImmediatePooled {
                        payload,
                        prefix,
                        payload_len,
                    } => Some(ActorResponse::Pooled {
                        payload,
                        prefix,
                        payload_len,
                    }),
                    crate::registry::AskDisposition::Deferred => None,
                })
        } else if let Some(cell) = registry.actor_ask_handler_sync.load_full() {
            if let Some(stream_handle) =
                response_connection.and_then(|conn| conn.stream_handle.as_ref().cloned())
            {
                let context = crate::AskContext::from_stream_handle(
                    corr_id,
                    &stream_handle,
                    authenticated_peer_id,
                );
                cell.handle(actor_id, type_hash, complete_data, context)
                    .map(|disposition| match disposition {
                        crate::registry::AskDisposition::Immediate(response) => Some(response),
                        crate::registry::AskDisposition::ImmediateBytes(response) => {
                            Some(ActorResponse::Bytes(response))
                        }
                        crate::registry::AskDisposition::ImmediateAligned(response) => {
                            Some(ActorResponse::Aligned(response))
                        }
                        crate::registry::AskDisposition::ImmediatePooled {
                            payload,
                            prefix,
                            payload_len,
                        } => Some(ActorResponse::Pooled {
                            payload,
                            prefix,
                            payload_len,
                        }),
                        crate::registry::AskDisposition::Deferred => None,
                    })
            } else {
                registry
                    .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                    .await
            }
        } else {
            registry
                .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                .await
        }
    } else if let Some(cell) = registry.actor_ask_handler_sync.load_full() {
        if let Some(stream_handle) =
            response_connection.and_then(|conn| conn.stream_handle.as_ref().cloned())
        {
            let context = crate::AskContext::from_stream_handle(
                corr_id,
                &stream_handle,
                authenticated_peer_id,
            );
            cell.handle(actor_id, type_hash, complete_data, context)
                .map(|disposition| match disposition {
                    crate::registry::AskDisposition::Immediate(response) => Some(response),
                    crate::registry::AskDisposition::ImmediateBytes(response) => {
                        Some(ActorResponse::Bytes(response))
                    }
                    crate::registry::AskDisposition::ImmediateAligned(response) => {
                        Some(ActorResponse::Aligned(response))
                    }
                    crate::registry::AskDisposition::ImmediatePooled {
                        payload,
                        prefix,
                        payload_len,
                    } => Some(ActorResponse::Pooled {
                        payload,
                        prefix,
                        payload_len,
                    }),
                    crate::registry::AskDisposition::Deferred => None,
                })
        } else {
            registry
                .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                .await
        }
    } else {
        registry
            .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
            .await
    };

    if let Ok(Some(response)) = response {
        // Only send response for asks (non-zero correlation_id)
        if corr_id != 0 {
            match response {
                ActorResponse::Bytes(response) => {
                    if should_stream_response(
                        registry,
                        response_connection,
                        response.len(),
                        response_mode,
                    ) {
                        send_streaming_response(registry, peer_addr, corr_id, response).await;
                    } else {
                        send_inline_response(registry, peer_addr, corr_id, response).await;
                    }
                }
                ActorResponse::Aligned(response) => {
                    if should_stream_response(
                        registry,
                        response_connection,
                        response.len(),
                        response_mode,
                    ) {
                        let bytes = response.into_bytes();
                        send_streaming_response(registry, peer_addr, corr_id, bytes).await;
                    } else {
                        send_inline_response_aligned(registry, peer_addr, corr_id, response).await;
                    }
                }
                ActorResponse::Pooled {
                    payload,
                    prefix,
                    payload_len,
                } => {
                    if should_stream_response(
                        registry,
                        response_connection,
                        payload_len,
                        response_mode,
                    ) {
                        // Fallback to copying for oversize pooled responses so the caller doesn't
                        // time out on a valid response.
                        let mut buf = bytes::BytesMut::with_capacity(payload_len);
                        if let Some(p) = prefix {
                            buf.extend_from_slice(&p); // ALLOW_COPY
                        }
                        let mut payload = payload;
                        while payload.has_remaining() {
                            let chunk = payload.chunk();
                            if chunk.is_empty() {
                                break;
                            }
                            buf.extend_from_slice(chunk); // ALLOW_COPY
                            let len = chunk.len();
                            payload.advance(len);
                        }
                        let bytes = buf.freeze();

                        send_streaming_response(registry, peer_addr, corr_id, bytes).await;
                    } else {
                        send_pooled_response(
                            registry,
                            peer_addr,
                            corr_id,
                            payload,
                            prefix,
                            payload_len,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn streaming_rejects_oversize() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 1,
            total_size: (crate::MAX_STREAM_SIZE as u64) + 1,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };

        assert!(
            state
                .start_stream_with_correlation(header, 1, pool, None)
                .is_err()
        );
    }

    #[test]
    fn streaming_enforces_per_connection_inflight_budget() {
        // A peer must not be able to force unbounded eager allocation by opening
        // many max-size streams: the SUM of declared in-flight sizes is capped.
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let per = crate::MAX_STREAM_SIZE as u64;
        let allowed = crate::MAX_INFLIGHT_STREAM_BYTES / crate::MAX_STREAM_SIZE;

        for i in 0..allowed as u64 {
            let header = crate::StreamHeader {
                stream_id: i,
                total_size: per,
                chunk_size: 0,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .start_stream_with_correlation(header, 1, pool.clone(), None)
                .expect("streams within the budget are accepted");
        }

        // One more max-size stream would push the aggregate over the budget.
        let header = crate::StreamHeader {
            stream_id: 9999,
            total_size: per,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        assert!(
            state
                .start_stream_with_correlation(header, 1, pool, None)
                .is_err(),
            "aggregate in-flight stream allocation must be bounded per connection"
        );
    }

    #[test]
    fn streaming_reassembles_into_final_buffer() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let start = crate::StreamHeader {
            stream_id: 42,
            total_size: 8,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 7,
        };
        state
            .start_stream_with_correlation(start, 9, pool.clone(), None)
            .unwrap();

        let chunk0 = crate::StreamHeader {
            stream_id: 42,
            total_size: 8,
            chunk_size: 4,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 7,
        };
        let chunk1 = crate::StreamHeader {
            stream_id: 42,
            total_size: 8,
            chunk_size: 4,
            chunk_index: 1,
            type_hash: 1,
            actor_id: 7,
        };

        assert!(
            state
                .add_chunk_with_correlation(chunk0, Bytes::from_static(b"abcd"), None)
                .unwrap()
                .is_none()
        );
        let assembled = state
            .add_chunk_with_correlation(chunk1, Bytes::from_static(b"efgh"), None)
            .unwrap()
            .expect("assembled");
        assert_eq!(assembled.0.as_ref(), b"abcdefgh");
        assert_eq!(assembled.1, 9);
        assert_eq!(assembled.2, None);
    }

    #[test]
    fn finalize_incomplete_stream_is_rejected() {
        // ACTOR_REM_2 R9: a StreamEnd arriving before all declared bytes are
        // received must NOT deliver the pre-allocated, zero-padded buffer as a
        // complete message.
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let start = crate::StreamHeader {
            stream_id: 7,
            total_size: 8,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        state
            .start_stream_with_correlation(start, 3, pool.clone(), None)
            .unwrap();

        // Only 4 of the declared 8 bytes are delivered.
        let chunk0 = crate::StreamHeader {
            stream_id: 7,
            total_size: 8,
            chunk_size: 4,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        assert!(
            state
                .add_chunk_with_correlation(chunk0, Bytes::from_static(b"abcd"), None)
                .unwrap()
                .is_none(),
            "a 4-of-8 stream is not complete"
        );

        // StreamEnd (finalize) arrives early: must error, not deliver padding.
        assert!(
            state.finalize_stream_with_correlation(7, None).is_err(),
            "R9: finalizing an incomplete stream must be rejected"
        );
    }

    #[test]
    fn finalize_empty_stream_is_allowed() {
        // A legitimately zero-length payload (total_size == 0) is complete on
        // finalize and must not be rejected by the R9 completeness check.
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let start = crate::StreamHeader {
            stream_id: 8,
            total_size: 0,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        state
            .start_stream_with_correlation(start, 5, pool.clone(), None)
            .unwrap();
        let out = state
            .finalize_stream_with_correlation(8, None)
            .expect("empty stream finalizes")
            .expect("empty stream yields a message");
        assert_eq!(out.0.as_ref(), b"");
        assert_eq!(out.1, 5);
    }

    #[tokio::test]
    async fn schema_hash_mismatch_rejects_actor_payload() {
        use crate::{GossipConfig, KeyPair};

        struct TestHandler {
            hits: Arc<AtomicUsize>,
        }

        impl crate::registry::ActorMessageHandler for TestHandler {
            fn handle_actor_message(
                &self,
                _actor_id: u64,
                _type_hash: u32,
                _payload: crate::AlignedBytes,
                _correlation_id: Option<u32>,
            ) -> crate::registry::ActorMessageFuture<'_> {
                let hits = self.hits.clone();
                Box::pin(async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                })
            }
        }

        let config = GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("schema_hash_test")),
            schema_hash: Some(0xAABBCCDDEEFF0011),
            ..Default::default()
        };
        let registry = Arc::new(GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            config,
        ));
        registry.connection_pool.set_registry(registry.clone());

        let hits = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(TestHandler { hits: hits.clone() });
        registry.set_actor_message_handler(handler).await;

        let pool = Arc::new(crate::AlignedBytesPool::default());
        let payload = crate::AlignedBytes::from_pooled_slice(b"payload", pool);

        handle_assembled_message(
            &registry,
            "127.0.0.1:0".parse().unwrap(),
            None,
            1,
            0xDEAD_BEEF,
            payload,
            0,
            Some(0x1122334455667788),
            None,
            ResponseMode::InlineOnly,
        )
        .await;

        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    /// Helpers for the chunk-integrity tests below.
    mod chunk_integrity {
        use super::*;

        pub(super) const STRIDE: usize = 8;

        pub(super) fn start(state: &mut StreamingState, stream_id: u64, total_size: u64) {
            let pool = Arc::new(crate::AlignedBytesPool::default());
            let header = crate::StreamHeader {
                stream_id,
                total_size,
                chunk_size: 0,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .start_stream_with_correlation(header, 7, pool, None)
                .expect("stream start is accepted");
        }

        pub(super) fn chunk(
            state: &mut StreamingState,
            stream_id: u64,
            total_size: u64,
            chunk_index: u32,
            payload: &[u8],
        ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
            let header = crate::StreamHeader {
                stream_id,
                total_size,
                chunk_size: payload.len() as u32,
                chunk_index,
                type_hash: 0,
                actor_id: 0,
            };
            state.add_chunk_with_correlation(header, Bytes::copy_from_slice(payload), None)
        }
    }

    /// A duplicated chunk must not be able to stand in for a chunk that never
    /// arrived. Counting only received *bytes* let `received_size` reach
    /// `total_size` while a hole in the pre-allocated (zero-filled) buffer was
    /// never written, so a corrupt, zero-padded payload was delivered to the
    /// actor as a complete message.
    #[test]
    fn duplicate_chunk_never_substitutes_for_a_missing_chunk() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 2) as u64;
        start(&mut state, 1, total);

        let first = [0xAAu8; STRIDE];
        assert!(
            chunk(&mut state, 1, total, 0, &first)
                .expect("first chunk is accepted")
                .is_none(),
            "a single chunk of a two-chunk stream must not complete it"
        );

        // Chunk 0 again -- a retransmit. Chunk 1 never arrives.
        let completed = chunk(&mut state, 1, total, 0, &first);

        match completed {
            Err(_) => {}
            Ok(None) => {}
            Ok(Some((data, _, _))) => panic!(
                "stream completed on a duplicated chunk while chunk 1 was never \
                 received; delivered {} bytes with tail {:?}",
                data.len(),
                &data[STRIDE..]
            ),
        }
    }

    /// A duplicate that arrives alongside the full set must not corrupt the
    /// payload or the accounting: the assembled message is still byte-exact.
    #[test]
    fn duplicate_chunk_does_not_corrupt_a_complete_stream() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 2) as u64;
        start(&mut state, 2, total);

        let first = [0xAAu8; STRIDE];
        let second = [0xBBu8; STRIDE];

        assert!(chunk(&mut state, 2, total, 0, &first).unwrap().is_none());
        // Retransmit of chunk 0 before chunk 1 lands.
        let _ = chunk(&mut state, 2, total, 0, &first);
        let completed = chunk(&mut state, 2, total, 1, &second)
            .expect("final chunk is accepted")
            .expect("stream completes once every chunk has arrived");

        let mut expected = Vec::new();
        expected.extend_from_slice(&first);
        expected.extend_from_slice(&second);
        assert_eq!(
            completed.0.as_ref(),
            expected.as_slice(),
            "assembled payload must be byte-exact despite the duplicate"
        );
    }

    /// Chunks may legitimately arrive out of order; completion must still be
    /// exact and the payload correctly placed.
    #[test]
    fn out_of_order_chunks_still_assemble_byte_exactly() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 3) as u64;
        start(&mut state, 3, total);

        let c0 = [0x11u8; STRIDE];
        let c1 = [0x22u8; STRIDE];
        let c2 = [0x33u8; STRIDE];

        assert!(chunk(&mut state, 3, total, 2, &c2).unwrap().is_none());
        assert!(chunk(&mut state, 3, total, 0, &c0).unwrap().is_none());
        let completed = chunk(&mut state, 3, total, 1, &c1)
            .expect("final chunk is accepted")
            .expect("stream completes once every chunk has arrived");

        let mut expected = Vec::new();
        expected.extend_from_slice(&c0);
        expected.extend_from_slice(&c1);
        expected.extend_from_slice(&c2);
        assert_eq!(completed.0.as_ref(), expected.as_slice());
    }

    /// A chunk index past the declared end of the stream must be rejected
    /// rather than silently ignored or written out of range.
    #[test]
    fn chunk_index_beyond_declared_length_is_rejected() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 2) as u64;
        start(&mut state, 4, total);

        let payload = [0xCCu8; STRIDE];
        assert!(
            chunk(&mut state, 4, total, 7, &payload).is_err(),
            "chunk_index 7 of a two-chunk stream must be rejected"
        );
    }

    /// A stream that keeps receiving chunks must not be reaped just because it
    /// has been running a long time. Reaping keyed on age-since-start killed
    /// legitimately slow or large transfers mid-flight, failing the sender's
    /// ask with no server-side diagnostic.
    #[test]
    fn actively_progressing_stream_survives_past_the_idle_timeout() {
        use chunk_integrity::{STRIDE, chunk, start};
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(40);
        let max_lifetime = Duration::from_secs(60);

        let mut state = StreamingState::new();
        let total = (STRIDE * 4) as u64;
        start(&mut state, 10, total);

        // Trickle chunks, each comfortably inside the idle window, but let the
        // stream's total age pass the idle timeout several times over.
        for idx in 0..4u32 {
            std::thread::sleep(Duration::from_millis(15));
            let payload = [0x5Au8; STRIDE];
            let _ = chunk(&mut state, 10, total, idx, &payload);
            state.cleanup_stale_with(idle_timeout, max_lifetime);
            if idx < 3 {
                assert_eq!(
                    state.active_stream_count(),
                    1,
                    "a stream that received a chunk {}ms ago was reaped while still progressing",
                    15
                );
            }
        }
    }

    /// The reaper must still collect a stream that genuinely stalls.
    #[test]
    fn idle_stream_is_still_reaped() {
        use chunk_integrity::{STRIDE, start};
        use std::time::Duration;

        let mut state = StreamingState::new();
        start(&mut state, 11, (STRIDE * 2) as u64);
        assert_eq!(state.active_stream_count(), 1);

        std::thread::sleep(Duration::from_millis(30));
        state.cleanup_stale_with(Duration::from_millis(10), Duration::from_secs(60));

        assert_eq!(
            state.active_stream_count(),
            0,
            "a stream with no activity past the idle timeout must be reaped"
        );
    }

    /// A peer must not be able to hold a slot open forever by trickling chunks
    /// just inside the idle window.
    #[test]
    fn stream_exceeding_max_lifetime_is_reaped_even_while_progressing() {
        use chunk_integrity::{STRIDE, chunk, start};
        use std::time::Duration;

        let mut state = StreamingState::new();
        let total = (STRIDE * 4) as u64;
        start(&mut state, 12, total);

        std::thread::sleep(Duration::from_millis(20));
        let payload = [0x5Au8; STRIDE];
        let _ = chunk(&mut state, 12, total, 0, &payload);

        // Chunk just arrived, so it is not idle -- but its lifetime is spent.
        state.cleanup_stale_with(Duration::from_secs(60), Duration::from_millis(10));

        assert_eq!(
            state.active_stream_count(),
            0,
            "the absolute lifetime backstop must still reap a trickling stream"
        );
    }

    /// The existing zero-length stream contract must keep working.
    #[test]
    fn empty_stream_finalizes_without_chunks() {
        use chunk_integrity::start;

        let mut state = StreamingState::new();
        start(&mut state, 5, 0);

        let completed = state
            .finalize_stream_with_correlation(5, None)
            .expect("empty stream finalizes")
            .expect("empty stream yields a message");
        assert_eq!(completed.0.len(), 0);
    }
}
