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
    correlation_id: u16,
    schema_hash: Option<u64>,
    received_size: usize,
    /// Pre-allocated aligned buffer for final message assembly.
    buffer: crate::PooledAlignedBuffer,
    /// Chunk stride used to calculate offsets.
    chunk_stride: Option<usize>,
    /// Timestamp when stream started (for stale cleanup)
    started_at: std::time::Instant,
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
        correlation_id: u16,
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
                started_at: std::time::Instant::now(),
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
    ) -> Result<Option<(Bytes, u16, Option<u64>)>> {
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
            stream.chunk_stride = Some(header.chunk_size as usize);
        }

        let stride = stream.chunk_stride.unwrap_or(header.chunk_size as usize);
        if header.chunk_size as usize > stride {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk_size exceeds stream stride",
            )));
        }

        let offset = header.chunk_index as usize * stride;
        let end = offset + chunk_data.len();
        if end > stream.total_size as usize {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk overflow for stream_id={}", header.stream_id),
            )));
        }

        // CRITICAL_PATH: write chunk directly into final buffer.
        stream.buffer.as_mut_slice()[offset..end].copy_from_slice(&chunk_data);
        stream.received_size += chunk_data.len();

        // Check if we have all chunks (when total matches expected size)
        if stream.received_size >= stream.total_size as usize {
            self.assemble_complete_message_with_correlation(header.stream_id)
        } else {
            Ok(None)
        }
    }

    pub fn finalize_stream_with_correlation(
        &mut self,
        stream_id: u64,
        schema_hash: Option<u64>,
    ) -> Result<Option<(Bytes, u16, Option<u64>)>> {
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
    ) -> Result<Option<(Bytes, u16, Option<u64>)>> {
        let stream = self.active_streams.remove(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Cannot finalize unknown stream_id={}", stream_id),
            ))
        })?;

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

    /// Clean up stale streams that have been incomplete for too long.
    pub fn cleanup_stale(&mut self) {
        use std::time::Duration;
        const STREAM_TIMEOUT: Duration = Duration::from_secs(60);

        let before_count = self.active_streams.len();

        self.active_streams.retain(|stream_id, stream| {
            let age = stream.started_at.elapsed();
            if age > STREAM_TIMEOUT {
                warn!(
                    stream_id = stream_id,
                    age_secs = age.as_secs(),
                    received_size = stream.received_size,
                    expected_size = stream.total_size,
                    "Cleaning up stale stream - StreamEnd never arrived"
                );
                false // Remove this entry
            } else {
                true // Keep this entry
            }
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
            if matches!(msg, RegistryMessage::ActorMessage { .. }) {
                warn!(
                    peer = %peer_addr,
                    "Registry ActorMessage is no longer supported in v3; use ActorTell/ActorAsk frames"
                );
                return Ok(());
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
                if let Some(handler) = registry.pubsub_ingress_handler.load_full() {
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
            // Wire format from sender: [type:1][correlation_id:2][payload_len:4][payload:N]
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
            // Wire format from sender: [type:1][correlation_id:2][payload_len:4][payload:N]
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
    actor_id: u64,
    type_hash: u32,
    complete_data: crate::AlignedBytes,
    corr_id: u16,
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
        if let Some(cell) = registry.actor_tell_handler_sync.load_full() {
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
                let context = crate::AskContext::from_stream_handle(corr_id, &stream_handle);
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
            let context = crate::AskContext::from_stream_handle(corr_id, &stream_handle);
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
                _correlation_id: Option<u16>,
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
}
