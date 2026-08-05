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

/// How long an in-progress stream may go without genuine byte progress
/// before it is reaped. This is the ONLY lifetime bound applied to a
/// stream: it is measured from the last time bytes actually advanced --
/// never from stream start, and never merely from the last time the reader
/// was polled -- so a slow-but-honest transfer that keeps advancing runs to
/// completion no matter how long it takes, while a peer that stops
/// advancing (including one that never sends a single byte after
/// `StreamStart`) loses its slot and reserved bytes within this window.
///
/// Also doubles as the base tombstone TTL for a reaped/rejected stream id
/// (see `reject_stream` / `tombstone_reaped_stream`): a tombstone hit
/// refreshes its own timer the same way genuine stream activity does, so a
/// sender still trickling into a dead id keeps being silently discarded
/// instead of eventually falling through to a fatal "unknown stream_id".
const STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Per-connection streaming state for managing partial streams
#[derive(Debug)]
pub struct StreamingState {
    active_streams: HashMap<u64, InProgressStream>,
    /// Rejected stream ids are quarantined so their trailing frames can be
    /// consumed without allocating or tearing down unrelated traffic.
    rejected_streams: HashMap<u64, std::time::Instant>,
    max_concurrent_streams: usize,
}

const MAX_REJECTED_STREAMS: usize = 32;

/// A validated, not-yet-committed V5 chunk destination. The IO task receives
/// plaintext directly into this range and commits it only after every byte was
/// read, so a torn frame can never advance the integrity bitmap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamChunkReservation {
    stream_id: u64,
    chunk_index: usize,
    offset: usize,
    len: usize,
}

impl StreamChunkReservation {
    pub(crate) fn len(self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// Completed direct-read V5 stream. Its payload owns the same pooled aligned
/// allocation that was reserved at StreamStart.
#[derive(Debug)]
pub(crate) struct CompletedV5Stream {
    pub actor_id: u64,
    pub type_hash: u32,
    pub correlation_id: u32,
    pub is_response: bool,
    pub payload: crate::AlignedBytes,
}

/// A stream that is currently being assembled
#[derive(Debug)]
struct InProgressStream {
    stream_id: u64,
    total_size: u64,
    type_hash: u32,
    actor_id: u64,
    correlation_id: u32,
    is_response: bool,
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
        if self.received_chunks[..full_words]
            .iter()
            .any(|w| *w != u64::MAX)
        {
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
            rejected_streams: HashMap::new(),
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
        self.start_stream_with_correlation_and_kind(
            header,
            correlation_id,
            pool,
            schema_hash,
            false,
        )
    }

    pub fn start_stream_with_correlation_and_kind(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        schema_hash: Option<u64>,
        is_response: bool,
    ) -> Result<()> {
        // A repeated StreamStart must never be reclassified as resource
        // pressure merely because the connection is already at capacity. The
        // subsequent duplicate chunk remains a fatal protocol error.
        if self.active_streams.contains_key(&header.stream_id) {
            return Ok(());
        }
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
            is_response,
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
        Ok(())
    }

    /// Starts a V5 stream and reserves its first data-bearing Start frame.
    pub(crate) fn begin_v5_stream(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        is_response: bool,
        first_chunk_len: usize,
    ) -> Result<StreamChunkReservation> {
        if first_chunk_len > header.total_size as usize {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 StreamStartData has invalid first chunk length",
            )));
        }
        self.start_stream_with_correlation_and_kind(
            header,
            correlation_id,
            pool,
            None,
            is_response,
        )?;
        self.reserve_v5_chunk(header.stream_id, 0, first_chunk_len)
    }

    /// Convert only bounded resource-pressure rejections into a stream-local
    /// discard. Malformed frames stay fatal protocol errors.
    pub(crate) fn begin_v5_stream_or_discard(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        is_response: bool,
        first_chunk_len: usize,
    ) -> Result<Option<StreamChunkReservation>> {
        // TCP preserves frame order: every trailing frame for the rejected
        // generation arrives before a later StreamStart. A new start is
        // therefore an unambiguous generation boundary, not a reason to
        // suppress a legitimate retry until the tombstone expires.
        self.rejected_streams.remove(&header.stream_id);
        match self.begin_v5_stream(header, correlation_id, pool, is_response, first_chunk_len) {
            Ok(reservation) => Ok(Some(reservation)),
            Err(error) if is_resource_busy(&error) => {
                self.reject_stream(header.stream_id)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Validates a V5 chunk before the reader receives it. This does not mark
    /// the bitmap; callers must call `commit_v5_chunk` after a complete read.
    pub(crate) fn reserve_v5_chunk(
        &mut self,
        stream_id: u64,
        chunk_index: u32,
        chunk_len: usize,
    ) -> Result<StreamChunkReservation> {
        let stream = self.active_streams.get_mut(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk for unknown stream_id={stream_id}"),
            ))
        })?;
        if chunk_len == 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk is empty",
            )));
        }
        let chunk_index = chunk_index as usize;
        let stride = match stream.chunk_stride {
            Some(stride) => stride,
            None => {
                stream.chunk_stride = Some(chunk_len);
                let expected = (stream.total_size as usize).div_ceil(chunk_len);
                stream.expected_chunks = Some(expected);
                stream.received_chunks = vec![0u64; expected.div_ceil(64)];
                chunk_len
            }
        };
        let expected = stream.expected_chunks.expect("stride sets expected chunks");
        if chunk_index >= expected {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk index out of range",
            )));
        }
        let offset = chunk_index.checked_mul(stride).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk offset overflow",
            ))
        })?;
        let remaining = stream.total_size as usize - offset;
        let expected_len = remaining.min(stride);
        if chunk_len != expected_len {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk length does not match its index",
            )));
        }
        let word = chunk_index / 64;
        let bit = 1u64 << (chunk_index % 64);
        if stream.received_chunks[word] & bit != 0 {
            stream.duplicate_chunks = stream.duplicate_chunks.saturating_add(1);
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "duplicate V5 stream chunk",
            )));
        }
        Ok(StreamChunkReservation {
            stream_id,
            chunk_index,
            offset,
            len: chunk_len,
        })
    }

    pub(crate) fn reserve_v5_chunk_or_discard(
        &mut self,
        stream_id: u64,
        chunk_index: u32,
        chunk_len: usize,
    ) -> Result<Option<StreamChunkReservation>> {
        if let Some(tombstoned_at) = self.rejected_streams.get_mut(&stream_id) {
            // A sender still trickling chunks into a reaped/rejected id is
            // making a good-faith delivery attempt; refresh the tombstone so
            // it keeps being silently discarded instead of aging out and
            // falling through to `reserve_v5_chunk`'s fatal "unknown
            // stream_id" once the original TTL window passes.
            *tombstoned_at = std::time::Instant::now();
            return Ok(None);
        }
        self.reserve_v5_chunk(stream_id, chunk_index, chunk_len)
            .map(Some)
    }

    fn reject_stream(&mut self, stream_id: u64) -> Result<()> {
        self.rejected_streams
            .retain(|_, at| at.elapsed() <= STREAM_IDLE_TIMEOUT);
        if !self.rejected_streams.contains_key(&stream_id)
            && self.rejected_streams.len() >= MAX_REJECTED_STREAMS
        {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "too many rejected streams",
            )));
        }
        self.rejected_streams
            .insert(stream_id, std::time::Instant::now());
        Ok(())
    }

    /// Returns the writable target range for the next read into
    /// `reservation`. This function is called on every poll of the
    /// underlying socket read -- including polls where nothing is actually
    /// transferred -- so it must stay free of any side effect on the
    /// stream's activity timer. Callers advance progress explicitly via
    /// `record_v5_chunk_progress` after a read that returns at least one
    /// byte.
    ///
    /// Returns a `NotFound` network error (checked by `is_stream_reaped_error`)
    /// if the owning stream was reaped while this reservation was still
    /// live. Callers must treat that as a per-stream discard, not a fatal
    /// connection error: the remainder of the in-flight chunk still has to
    /// be drained off the wire to keep framing intact, but the stream's
    /// backing buffer is already gone.
    pub(crate) fn v5_chunk_target(
        &mut self,
        reservation: StreamChunkReservation,
        read: usize,
    ) -> Result<&mut [u8]> {
        let stream = self
            .active_streams
            .get_mut(&reservation.stream_id)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "stream reaped while its chunk reservation was still live",
                ))
            })?;
        if read > reservation.len {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream read exceeds reserved chunk",
            )));
        }
        let start = reservation.offset + read;
        let end = reservation.offset + reservation.len;
        Ok(&mut stream.buffer.as_mut_slice()[start..end])
    }

    /// Records that bytes actually landed in `reservation`'s target range,
    /// keeping the owning stream out of the idle reaper. Must only be
    /// called after a read that returned at least one byte -- crediting
    /// progress for a poll that transferred nothing is exactly what let a
    /// stalled peer's live reservation pin a slot indefinitely.
    pub(crate) fn record_v5_chunk_progress(&mut self, reservation: StreamChunkReservation) {
        if let Some(stream) = self.active_streams.get_mut(&reservation.stream_id) {
            stream.last_activity = std::time::Instant::now();
        }
    }

    pub(crate) fn commit_v5_chunk(
        &mut self,
        reservation: StreamChunkReservation,
    ) -> Result<Option<CompletedV5Stream>> {
        let completed = {
            let stream = self
                .active_streams
                .get_mut(&reservation.stream_id)
                .ok_or_else(|| {
                    GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "stream vanished",
                    ))
                })?;
            let word = reservation.chunk_index / 64;
            let bit = 1u64 << (reservation.chunk_index % 64);
            if stream.received_chunks[word] & bit != 0 {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "duplicate V5 stream chunk commit",
                )));
            }
            stream.received_chunks[word] |= bit;
            stream.received_size += reservation.len;
            stream.last_activity = std::time::Instant::now();
            stream.all_chunks_received()
        };
        if !completed {
            return Ok(None);
        }
        let stream = self
            .active_streams
            .remove(&reservation.stream_id)
            .expect("completed stream exists");
        Ok(Some(CompletedV5Stream {
            actor_id: stream.actor_id,
            type_hash: stream.type_hash,
            correlation_id: stream.correlation_id,
            is_response: stream.is_response,
            payload: stream.buffer.into_aligned_bytes(),
        }))
    }

    pub fn add_chunk_with_correlation(
        &mut self,
        header: crate::StreamHeader,
        chunk_data: Bytes,
        _schema_hash: Option<u64>,
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

        if header.total_size != 0 && header.total_size != stream.total_size {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stream total_size mismatch",
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
        let offset = chunk_index.checked_mul(stride).ok_or_else(|| {
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

    pub fn metadata_for(&self, stream_id: u64) -> Option<(u64, u32, u32, bool)> {
        self.active_streams.get(&stream_id).map(|stream| {
            (
                stream.actor_id,
                stream.type_hash,
                stream.correlation_id,
                stream.is_response,
            )
        })
    }

    /// Drop a partial stream on the V5 cold-path abort signal. Its final
    /// allocation is returned to the pool and no partial data is delivered.
    pub fn abort_stream(&mut self, stream_id: u64) -> bool {
        self.active_streams.remove(&stream_id).is_some()
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

    /// Number of tombstoned (rejected or reaped) stream ids currently
    /// quarantined. Exposed for tests that verify the table stays bounded.
    #[cfg(test)]
    fn rejected_stream_count(&self) -> usize {
        self.rejected_streams.len()
    }

    /// Clean up in-progress streams that have stopped making genuine byte
    /// progress. A stream is never reaped merely for its total age: as long
    /// as bytes keep landing, however slowly, it survives, so a large or
    /// slow-but-healthy transfer is never evicted mid-flight. The idle bound
    /// alone is what keeps a stalled peer -- one that stops advancing bytes,
    /// including one that never sends any after `StreamStart` -- from
    /// pinning a slot and its reserved bytes indefinitely.
    pub fn cleanup_stale(&mut self) {
        self.cleanup_stale_with(STREAM_IDLE_TIMEOUT);
    }

    /// `cleanup_stale` with an explicit bound, so tests do not have to wait
    /// out the production timeout.
    pub(crate) fn cleanup_stale_with(&mut self, idle_timeout: std::time::Duration) {
        let before_count = self.active_streams.len();
        self.rejected_streams
            .retain(|_, at| at.elapsed() <= idle_timeout);

        // A stream the reaper drops is not necessarily done as far as the
        // sender is concerned -- its next in-flight chunk is already on the
        // wire. Collect the reaped ids so they can be tombstoned into
        // `rejected_streams` below: without that, the late chunk hits
        // "unknown stream_id", a fatal protocol error that tears down the
        // whole connection instead of just this stream.
        let mut reaped_ids = Vec::new();
        self.active_streams.retain(|stream_id, stream| {
            let idle = stream.last_activity.elapsed();
            if idle > idle_timeout {
                warn!(
                    stream_id = stream_id,
                    idle_secs = idle.as_secs(),
                    age_secs = stream.started_at.elapsed().as_secs(),
                    received_size = stream.received_size,
                    expected_size = stream.total_size,
                    "Cleaning up stream with no byte progress within the idle timeout"
                );
                reaped_ids.push(*stream_id);
                return false;
            }

            true
        });

        for stream_id in &reaped_ids {
            self.tombstone_reaped_stream(*stream_id);
        }

        let removed = before_count - self.active_streams.len();
        if removed > 0 {
            info!(
                removed_count = removed,
                remaining = self.active_streams.len(),
                "Cleaned up stale in-progress streams"
            );
        }
    }

    /// Quarantines a reaped stream id the same way `reject_stream` quarantines
    /// a resource-pressure rejection, but the reaper itself must never fail:
    /// if the quarantine table is already full, evict the oldest tombstone to
    /// make room rather than dropping the new one. A recently reaped id is
    /// exactly the one a sender is about to retry against, so it is the
    /// tombstone most worth keeping.
    fn tombstone_reaped_stream(&mut self, stream_id: u64) {
        if !self.rejected_streams.contains_key(&stream_id)
            && self.rejected_streams.len() >= MAX_REJECTED_STREAMS
        {
            if let Some(oldest) = self
                .rejected_streams
                .iter()
                .min_by_key(|(_, at)| **at)
                .map(|(id, _)| *id)
            {
                self.rejected_streams.remove(&oldest);
            }
        }
        self.rejected_streams
            .insert(stream_id, std::time::Instant::now());
    }
}

impl Default for StreamingState {
    fn default() -> Self {
        Self::new()
    }
}

fn is_resource_busy(error: &GossipError) -> bool {
    matches!(error, GossipError::Network(io) if io.kind() == std::io::ErrorKind::ResourceBusy)
}

/// True if `error` is `v5_chunk_target`'s "reservation vanished" signal --
/// the owning stream was reaped while a chunk read into it was still in
/// flight. The caller must drain and discard the remainder of that one
/// chunk instead of treating this as a fatal protocol error, mirroring how
/// a resource-pressure rejection is drained rather than tearing down the
/// connection (see `DiscardStreamPayload` in `connection_pool::read_pipeline`).
pub(crate) fn is_stream_reaped_error(error: &GossipError) -> bool {
    matches!(error, GossipError::Network(io) if io.kind() == std::io::ErrorKind::NotFound)
}

fn registry_message_sender_peer_id(msg: &RegistryMessage) -> Option<&PeerId> {
    match msg {
        RegistryMessage::DeltaGossip { delta, .. }
        | RegistryMessage::DeltaGossipResponse { delta, .. } => Some(&delta.sender_peer_id),
        RegistryMessage::FullSyncRequest { sender_peer_id, .. }
        | RegistryMessage::FullSync { sender_peer_id, .. }
        | RegistryMessage::FullSyncResponse { sender_peer_id, .. } => Some(sender_peer_id),
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
    // R-11: this connection's own session discriminator -- see
    // `ReadContext::session_source`. Threaded to
    // `handle_incoming_message` so the restart-sequence exemption is
    // scoped to the exact connection that armed it.
    session_source: SocketAddr,
    response_correlation: Option<&crate::connection_pool::CorrelationTracker>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    authenticated_peer_id: Option<&PeerId>,
) -> Result<()> {
    match result {
        MessageReadResult::RouteBound => {}
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

            if let Err(e) = crate::connection_pool::handle_incoming_message(
                registry.clone(),
                peer_addr,
                session_source,
                authenticated_peer_id.cloned(),
                msg,
            )
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
                    let is_response = msg_type == crate::MessageType::StreamResponseStart as u8;
                    if let Err(e) = streaming_state.start_stream_with_correlation_and_kind(
                        stream_header,
                        correlation_id,
                        pool,
                        schema_hash,
                        is_response,
                    ) {
                        warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                        return Ok(());
                    }
                    if chunk_data.is_empty() {
                        return Ok(());
                    }
                    let metadata = streaming_state.metadata_for(stream_header.stream_id);
                    if let Ok(Some((complete_data, corr_id, schema_hash))) = streaming_state
                        .add_chunk_with_correlation(stream_header, chunk_data, schema_hash)
                    {
                        if metadata
                            .map(|(_, _, _, response)| response)
                            .unwrap_or(is_response)
                        {
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                response_correlation,
                            )
                            .await;
                        } else if let Some((actor_id, type_hash, _, _)) = metadata {
                            handle_assembled_message(
                                registry,
                                peer_addr,
                                authenticated_peer_id.or_else(|| {
                                    response_connection
                                        .and_then(|conn| conn.embedded_peer_id.as_ref())
                                }),
                                actor_id,
                                type_hash,
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
                msg_type
                    if msg_type == crate::MessageType::StreamData as u8
                        || msg_type == crate::MessageType::StreamResponseData as u8 =>
                {
                    let Some((actor_id, type_hash, _, is_response)) =
                        streaming_state.metadata_for(stream_header.stream_id)
                    else {
                        warn!(
                            stream_id = stream_header.stream_id,
                            "Dropping stream chunk without start"
                        );
                        return Ok(());
                    };
                    if let Ok(Some((complete_data, corr_id, schema_hash))) = streaming_state
                        .add_chunk_with_correlation(stream_header, chunk_data, schema_hash)
                    {
                        if is_response {
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
                                actor_id,
                                type_hash,
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
        MessageReadResult::StreamAbort { stream_id, reason } => {
            if !streaming_state.abort_stream(stream_id) {
                warn!(
                    stream_id,
                    reason, "Ignoring V5 stream abort for unknown stream"
                );
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
            // Fast-path DirectAsk - bypasses handler and RegistryMessage overhead.
            // The payload contains only the direct frame body. There is no
            // registered application handler for DirectAsk, so in production
            // builds we must not fabricate a response from the request bytes.
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
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
            #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
            {
                let _ = payload;
                warn!(
                    peer = %peer_addr,
                    correlation_id,
                    "Received DirectAsk request - no handler registered, dropping"
                );
            }
        }
        MessageReadResult::DirectResponse {
            correlation_id,
            payload,
        } => {
            // Fast-path DirectResponse
            // The payload is the raw response data (no length prefix)
            // The payload contains only the direct frame body.
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
    _schema_hash: Option<u64>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    response_mode: ResponseMode,
) {
    // Complete message assembled - route to actor
    // corr_id == 0 means tell (fire-and-forget), non-zero means ask (expects response)
    let correlation_opt = if corr_id == 0 { None } else { Some(corr_id) };
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

    #[test]
    fn finalize_empty_stream_survives_exhausted_pool() {
        let pool = Arc::new(crate::AlignedBytesPool::new(2));
        let _checked_out: Vec<_> =
            std::iter::from_fn(|| (pool.available_count() > 0).then(|| pool.get_buffer(64)))
                .collect();
        assert_eq!(pool.available_count(), 0, "pool must be fully checked out");

        let mut state = StreamingState::new();
        let start = crate::StreamHeader {
            stream_id: 99,
            total_size: 0,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        state
            .start_stream_with_correlation(start, 11, pool, None)
            .expect("zero-length stream must start with an exhausted pool");
        let out = state
            .finalize_stream_with_correlation(99, None)
            .expect("zero-length stream must finalize")
            .expect("empty stream yields a message");
        assert_eq!(out.0.as_ref(), b"");
        assert_eq!(out.1, 11);
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

        let mut state = StreamingState::new();
        let total = (STRIDE * 4) as u64;
        start(&mut state, 10, total);

        // Trickle chunks, each comfortably inside the idle window, but let the
        // stream's total age pass the idle timeout several times over.
        for idx in 0..4u32 {
            std::thread::sleep(Duration::from_millis(15));
            let payload = [0x5Au8; STRIDE];
            let _ = chunk(&mut state, 10, total, idx, &payload);
            state.cleanup_stale_with(idle_timeout);
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

    /// There is no separate wall-clock cap on a stream's total age: as long
    /// as bytes keep landing, a transfer survives no matter how long the
    /// whole stream has been open or how many reaper ticks pass in the
    /// meantime. This used to be capped at a fixed `MAX_STREAM_LIFETIME`
    /// measured from stream start, which reaped a still-progressing stream
    /// (and, via the live-reservation race, its whole connection) purely
    /// for being old -- exercised here by letting many reaper ticks elapse,
    /// each individually well past what that old cap allowed relative to
    /// this test's timescale, while the stream keeps receiving chunks.
    #[test]
    fn stream_making_progress_is_never_reaped_for_its_total_age() {
        use chunk_integrity::{STRIDE, chunk, start};
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(20);
        let old_cap_stand_in = Duration::from_millis(60);

        let mut state = StreamingState::new();
        // Declares one more chunk than the loop ever sends, so the stream
        // never completes mid-test and vanishing from `active_streams`
        // (delivered, not reaped) can't be confused with the reap this test
        // is guarding against.
        let total = (STRIDE * 9) as u64;
        start(&mut state, 13, total);

        let mut total_elapsed = Duration::ZERO;
        for idx in 0..8u32 {
            std::thread::sleep(Duration::from_millis(10));
            total_elapsed += Duration::from_millis(10);
            let payload = [0x5Au8; STRIDE];
            let _ = chunk(&mut state, 13, total, idx, &payload);
            state.cleanup_stale_with(idle_timeout);
            assert_eq!(
                state.active_stream_count(),
                1,
                "a progressing stream must not be reaped for its total age"
            );
        }
        assert!(
            total_elapsed > old_cap_stand_in,
            "test must actually run past the old absolute-cap stand-in to be meaningful"
        );
    }

    /// The reaper must still collect a stream that genuinely stalls, and
    /// release both its slot and its reserved inflight-byte budget.
    #[test]
    fn idle_stream_is_reaped_and_releases_its_slot_and_reserved_bytes() {
        use std::time::Duration;

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let per = crate::MAX_STREAM_SIZE as u64;
        let allowed = crate::MAX_INFLIGHT_STREAM_BYTES / crate::MAX_STREAM_SIZE;

        // Fully commit the per-connection inflight budget across the max
        // number of max-size streams it allows, none of which ever receive
        // a chunk.
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
        assert_eq!(state.active_stream_count(), allowed);

        // The budget is fully committed: one more max-size stream must be
        // rejected while the others are still active.
        let blocked = crate::StreamHeader {
            stream_id: 999,
            total_size: per,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        assert!(
            state
                .start_stream_with_correlation(blocked, 1, pool.clone(), None)
                .is_err(),
            "inflight budget must be fully committed by the existing streams"
        );

        std::thread::sleep(Duration::from_millis(30));
        state.cleanup_stale_with(Duration::from_millis(10));

        assert_eq!(
            state.active_stream_count(),
            0,
            "streams with no activity past the idle timeout must be reaped"
        );

        // Every slot and its reserved bytes must be released: the full set
        // of max-size streams can be admitted again from scratch.
        for i in 1000..(1000 + allowed as u64) {
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
                .expect("reaping stalled streams must release their reserved inflight bytes");
        }
        assert_eq!(state.active_stream_count(), allowed);
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

    #[test]
    fn v5_direct_read_commits_the_final_allocation_without_a_chunk_copy() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(1));
        let start = crate::StreamHeader {
            stream_id: 77,
            total_size: 5,
            chunk_size: 3,
            chunk_index: 0,
            type_hash: 9,
            actor_id: 11,
        };
        let first = state
            .begin_v5_stream(start, 13, pool, false, 3)
            .expect("reserve start chunk");
        let first_ptr = {
            let target = state.v5_chunk_target(first, 0).expect("first target");
            target.copy_from_slice(b"abc");
            target.as_ptr()
        };
        assert!(state.commit_v5_chunk(first).unwrap().is_none());

        let second = state.reserve_v5_chunk(77, 1, 2).expect("reserve tail");
        {
            let target = state.v5_chunk_target(second, 0).expect("tail target");
            target.copy_from_slice(b"de");
        }
        let complete = state
            .commit_v5_chunk(second)
            .unwrap()
            .expect("complete stream");
        assert_eq!(complete.payload.as_ref(), b"abcde");
        assert_eq!(complete.payload.as_ref().as_ptr(), first_ptr);
        assert_eq!(
            (complete.payload.as_ref().as_ptr() as usize) % crate::PAYLOAD_ALIGNMENT,
            0
        );
    }

    #[test]
    fn v5_abort_discards_partial_assembly_without_delivery() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(1));
        let reservation = state
            .begin_v5_stream(
                crate::StreamHeader {
                    stream_id: 88,
                    total_size: 4,
                    chunk_size: 2,
                    chunk_index: 0,
                    type_hash: 1,
                    actor_id: 2,
                },
                3,
                pool,
                false,
                2,
            )
            .unwrap();
        state
            .v5_chunk_target(reservation, 0)
            .unwrap()
            .copy_from_slice(b"ab");
        assert!(state.abort_stream(88));
        assert!(!state.abort_stream(88));
        assert!(state.commit_v5_chunk(reservation).is_err());
    }

    #[test]
    fn qa_r8_resource_rejection_tombstones_trailing_chunks() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(1));

        for stream_id in 1..=16 {
            let reservation = state
                .begin_v5_stream(
                    crate::StreamHeader {
                        stream_id,
                        total_size: 1,
                        chunk_size: 1,
                        chunk_index: 0,
                        type_hash: 0,
                        actor_id: 0,
                    },
                    0,
                    pool.clone(),
                    false,
                    1,
                )
                .expect("fill the active-stream budget");
            let target = state.v5_chunk_target(reservation, 0).unwrap();
            target[0] = 1;
        }

        let rejected = state
            .begin_v5_stream_or_discard(
                crate::StreamHeader {
                    stream_id: 17,
                    total_size: 2,
                    chunk_size: 1,
                    chunk_index: 0,
                    type_hash: 0,
                    actor_id: 0,
                },
                0,
                pool,
                false,
                1,
            )
            .expect("resource pressure is a stream-local rejection");
        assert!(rejected.is_none());
        assert!(
            state
                .reserve_v5_chunk_or_discard(17, 1, 1)
                .expect("trailing chunk is discarded")
                .is_none()
        );
        assert_eq!(state.active_stream_count(), 16);

        // Once pressure clears, a later StreamStart is a new generation on
        // the ordered transport and must not be silently discarded because
        // its old generation was tombstoned.
        assert!(state.abort_stream(1));
        assert!(
            state
                .begin_v5_stream_or_discard(
                    crate::StreamHeader {
                        stream_id: 17,
                        total_size: 1,
                        chunk_size: 1,
                        chunk_index: 0,
                        type_hash: 0,
                        actor_id: 0,
                    },
                    0,
                    Arc::new(crate::AlignedBytesPool::new(1)),
                    false,
                    1,
                )
                .expect("a retry start is valid after pressure clears")
                .is_some()
        );
    }

    /// The reap-tombstone table must stay bounded no matter how many streams
    /// are reaped over its lifetime: `MAX_REJECTED_STREAMS` is enforced by
    /// evicting the oldest tombstone, never by growing without bound.
    ///
    /// Drives `tombstone_reaped_stream` directly (the exact function
    /// `cleanup_stale_with` calls for every id it reaps) rather than forcing
    /// real streams through real idle timeouts, so the bound is exercised
    /// deterministically and without sleeping out the tombstone TTL.
    #[test]
    fn rejected_stream_table_stays_bounded_under_many_reaped_ids() {
        let mut state = StreamingState::new();

        for stream_id in 0..(MAX_REJECTED_STREAMS as u64 * 3) {
            state.tombstone_reaped_stream(stream_id);
            assert!(
                state.rejected_stream_count() <= MAX_REJECTED_STREAMS,
                "tombstone table exceeded its bound after reaping stream_id={stream_id}: {} > {}",
                state.rejected_stream_count(),
                MAX_REJECTED_STREAMS
            );
        }

        assert_eq!(
            state.rejected_stream_count(),
            MAX_REJECTED_STREAMS,
            "table should settle at its cap once more ids than it can hold have been reaped"
        );

        // The most recently reaped ids -- the ones a sender is most likely
        // to still be retrying against -- must be the ones retained.
        let newest = MAX_REJECTED_STREAMS as u64 * 3 - 1;
        assert!(
            state
                .reserve_v5_chunk_or_discard(newest, 0, 1)
                .expect("recently reaped id must still be tombstoned")
                .is_none(),
            "the most recently reaped id must be retained, not evicted"
        );
    }

    /// A single large chunk that trickles in slowly must not be reaped
    /// mid-read while the reader holds a live reservation. Progress must be
    /// recorded explicitly via `record_v5_chunk_progress` -- mirroring what
    /// the IO reader does after each successful partial read -- rather than
    /// as a side effect of merely asking for the next write target, since a
    /// chunk larger than one idle window (e.g. a 1 MiB chunk at <17 KB/s) is
    /// polled for its target many times before any of those polls actually
    /// transfers a byte.
    #[test]
    fn partial_v5_chunk_progress_keeps_stream_alive_past_idle_timeout() {
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(40);

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 200,
            total_size: 64,
            chunk_size: 64,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let reservation = state
            .begin_v5_stream(header, 1, pool, false, 64)
            .expect("reserve the single large chunk");

        // Trickle the one large chunk in small pieces, each comfortably
        // inside the idle window, but let total elapsed time cross the idle
        // window several times over before the chunk ever fully commits.
        let mut read = 0usize;
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(15));
            let target = state
                .v5_chunk_target(reservation, read)
                .expect("reservation is still live");
            target[..8].copy_from_slice(&[0xAB; 8]);
            read += 8;
            state.record_v5_chunk_progress(reservation);
            state.cleanup_stale_with(idle_timeout);
            assert_eq!(
                state.active_stream_count(),
                1,
                "a stream receiving partial-chunk byte progress must not be reaped mid-read"
            );
        }
    }

    /// Merely asking for the write target (polling, with no bytes actually
    /// transferred) must not count as progress -- otherwise a peer that
    /// opens a stream's reservation and then never sends another byte would
    /// still get its idle timer refreshed on every poll of the IO reader,
    /// pinning the slot and its reservation forever.
    #[test]
    fn polling_the_chunk_target_without_reading_bytes_does_not_refresh_activity() {
        use std::time::Duration;

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 210,
            total_size: 64,
            chunk_size: 64,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let reservation = state
            .begin_v5_stream(header, 1, pool, false, 64)
            .expect("reserve the chunk");

        std::thread::sleep(Duration::from_millis(15));
        // The reader is polled repeatedly (e.g. the socket is not yet
        // readable) but no byte is ever transferred, so progress is never
        // recorded.
        for _ in 0..5 {
            let _ = state
                .v5_chunk_target(reservation, 0)
                .expect("reservation is still live");
        }

        state.cleanup_stale_with(Duration::from_millis(10));
        assert_eq!(
            state.active_stream_count(),
            0,
            "polling for a write target without transferring bytes must not block idle reaping"
        );
    }

    /// Once the reaper removes a stream (idle timeout, no absolute cap), the
    /// sender's next in-flight chunk for that id must be a clean per-stream
    /// rejection, not a fatal "unknown stream_id" that tears down the whole
    /// connection. This requires the reaped id to be tombstoned into
    /// `rejected_streams`, mirroring the existing resource-pressure
    /// rejection path.
    #[test]
    fn reaped_stream_tombstones_so_late_chunk_is_a_clean_rejection_not_a_fatal_teardown() {
        use std::time::Duration;

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 201,
            total_size: 16,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let _ = state
            .begin_v5_stream(header, 1, pool, false, 8)
            .expect("start stream and reserve its first chunk");

        // Idle out with nothing further arriving for this stream, then reap.
        std::thread::sleep(Duration::from_millis(20));
        state.cleanup_stale_with(Duration::from_millis(5));
        assert_eq!(
            state.active_stream_count(),
            0,
            "the stream must have been reaped for the test to be meaningful"
        );

        // The sender, unaware the stream was reaped, sends its next chunk.
        let result = state.reserve_v5_chunk_or_discard(201, 1, 8);
        assert!(
            result.is_ok(),
            "a late chunk for a reaped stream must not be a fatal protocol error: {result:?}"
        );
        assert!(
            result.unwrap().is_none(),
            "a late chunk for a reaped stream must be discarded, not accepted as a fresh reservation"
        );
    }

    /// A sender that keeps trickling chunks into a reaped id well past the
    /// tombstone's original TTL must keep getting them silently discarded,
    /// not eventually fall through to a fatal "unknown stream_id" once the
    /// tombstone would otherwise have aged out. Each hit must refresh the
    /// tombstone's own timer, the same way real stream activity refreshes a
    /// live stream's.
    #[test]
    fn tombstone_is_refreshed_by_repeated_late_chunks_past_its_original_ttl() {
        use std::time::Duration;

        let ttl = Duration::from_millis(30);

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 301,
            total_size: 8,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let _ = state
            .begin_v5_stream(header, 1, pool, false, 8)
            .expect("start stream and reserve its first chunk");

        std::thread::sleep(ttl + Duration::from_millis(10));
        state.cleanup_stale_with(ttl);
        assert_eq!(
            state.active_stream_count(),
            0,
            "stream must be reaped first"
        );

        // Keep hitting the tombstone at intervals shorter than the TTL, but
        // whose SUM comfortably exceeds it -- each hit must push the expiry
        // back out, so the tombstone must never actually lapse.
        for _ in 0..4 {
            std::thread::sleep(ttl / 2);
            state.cleanup_stale_with(ttl); // prunes any tombstone that truly expired
            let result = state
                .reserve_v5_chunk_or_discard(301, 1, 8)
                .expect("a trickling late chunk must never be a fatal protocol error");
            assert!(
                result.is_none(),
                "a trickling late chunk must keep being discarded, not accepted as fresh"
            );
        }
    }

    #[test]
    fn v5_direct_read_keeps_alternating_streams_byte_exact() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(2));
        let reserve_start = |state: &mut StreamingState, id, pool: Arc<crate::AlignedBytesPool>| {
            state.begin_v5_stream(
                crate::StreamHeader {
                    stream_id: id,
                    total_size: 4,
                    chunk_size: 2,
                    chunk_index: 0,
                    type_hash: id as u32,
                    actor_id: id,
                },
                id as u32,
                pool,
                false,
                2,
            )
        };
        let a0 = reserve_start(&mut state, 100, pool.clone()).unwrap();
        state.v5_chunk_target(a0, 0).unwrap().copy_from_slice(b"ab");
        let b0 = reserve_start(&mut state, 200, pool).unwrap();
        state.v5_chunk_target(b0, 0).unwrap().copy_from_slice(b"12");
        assert!(state.commit_v5_chunk(b0).unwrap().is_none());
        assert!(state.commit_v5_chunk(a0).unwrap().is_none());

        let a1 = state.reserve_v5_chunk(100, 1, 2).unwrap();
        state.v5_chunk_target(a1, 0).unwrap().copy_from_slice(b"cd");
        let b1 = state.reserve_v5_chunk(200, 1, 2).unwrap();
        state.v5_chunk_target(b1, 0).unwrap().copy_from_slice(b"34");
        let b = state.commit_v5_chunk(b1).unwrap().unwrap();
        let a = state.commit_v5_chunk(a1).unwrap().unwrap();
        assert_eq!(a.payload.as_ref(), b"abcd");
        assert_eq!(b.payload.as_ref(), b"1234");
    }
}
