/// Commands for the per-connection writer.
#[derive(Debug)]
enum WriteCommand {
    /// Queued payload writes (tell/ask/control frames).
    Payload(WritePayload),
    /// Latency-sensitive data-plane write; write and flush as soon as the IO owner sees it.
    ImmediatePayload(WritePayload),
    /// Ask payload writes that should trigger low-latency ask flush behavior.
    AskPayload(WritePayload),
}

/// Commands for streaming operations.
// The direct write command remains part of the writer-owned transport command set.
#[allow(dead_code)]
enum StreamingCommand {
    /// Direct write bytes for streaming.
    WriteBytes(bytes::Bytes),
    /// Flush the writer.
    Flush,
    /// Vectored write for header + payload (zero-copy).
    VectoredWrite(VectoredSendItem),
    /// Batch of owned chunks for streaming (zero-copy).
    OwnedChunks(Vec<bytes::Bytes>),
    /// Pooled streaming response. The command retains the pooled payload and
    /// generates each frame header on demand, so admission never materializes
    /// a second contiguous payload allocation.
    PooledResponse(Box<PooledStreamingResponse>),
    /// Bytes-backed streaming response. The command retains one ref-counted
    /// payload and generates each frame header lazily instead of expanding a
    /// large response into one queue command per frame.
    BytesResponse(Box<BytesStreamingResponse>),
    /// Abort a partially transmitted stream. This stays on the streaming FIFO
    /// so it cannot overtake data chunks that were already accepted.
    Abort { stream_id: u32, reason: u32 },
}

/// A response stream backed by one pooled payload allocation.
///
/// `payload_len` includes the optional debug/type prefix while
/// `payload_remaining` counts only bytes still available from `payload`.
/// Frame headers are deliberately generated on demand instead of retaining a
/// header per chunk; the command therefore owns exactly the pooled payload plus
/// a small amount of bounded stream state.
struct PooledStreamingResponse {
    stream_id: u32,
    correlation_id: u32,
    payload_len: usize,
    chunk_size: usize,
    chunk_count: usize,
    prefix: Option<[u8; 16]>,
    prefix_len: usize,
    prefix_sent: usize,
    payload: crate::typed::PooledPayload,
    payload_remaining: usize,
    frame_index: usize,
    frame_offset: usize,
}

/// A lazily framed response backed by an already-owned `Bytes` payload.
struct BytesStreamingResponse {
    stream_id: u32,
    correlation_id: u32,
    payload: bytes::Bytes,
    payload_len: usize,
    chunk_size: usize,
    chunk_count: usize,
    frame_index: usize,
    frame_offset: usize,
}

impl BytesStreamingResponse {
    fn new(
        stream_id: u32,
        correlation_id: u32,
        payload: bytes::Bytes,
        chunk_size: usize,
    ) -> Self {
        let payload_len = payload.len();
        let chunk_count = if payload_len == 0 {
            1
        } else {
            payload_len
                .saturating_add(chunk_size.saturating_sub(1))
                .checked_div(chunk_size)
                .unwrap_or(usize::MAX)
        };
        Self {
            stream_id,
            correlation_id,
            payload,
            payload_len,
            chunk_size,
            chunk_count,
            frame_index: 0,
            frame_offset: 0,
        }
    }

    fn frame_payload_len(&self, frame_index: usize) -> usize {
        if frame_index == 0 {
            return self.payload_len.min(self.chunk_size);
        }
        let consumed = self
            .chunk_size
            .saturating_add(frame_index.saturating_sub(1).saturating_mul(self.chunk_size));
        self.payload_len.saturating_sub(consumed).min(self.chunk_size)
    }

    fn frame_header(&self, frame_index: usize) -> InlineFrameHeader {
        if frame_index == 0 {
            InlineFrameHeader::from_array(crate::framing::write_stream_response_start_header(
                self.stream_id,
                self.correlation_id,
                self.payload_len as u32,
                self.frame_payload_len(frame_index),
            ))
        } else {
            InlineFrameHeader::from_array(crate::framing::write_stream_data_header(
                true,
                self.stream_id,
                frame_index as u32,
                self.frame_payload_len(frame_index),
            ))
        }
    }

    fn wire_len(&self) -> usize {
        crate::framing::STREAM_RESPONSE_START_FRAME_HEADER_LEN
            + self.payload_len
            + self
                .chunk_count
                .saturating_sub(1)
                .saturating_mul(crate::framing::STREAM_DATA_FRAME_HEADER_LEN)
    }
}

impl PooledStreamingResponse {
    fn new(
        stream_id: u32,
        correlation_id: u32,
        payload_len: usize,
        chunk_size: usize,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        expected_payload_len: usize,
    ) -> Self {
        let chunk_count = if payload_len == 0 {
            1
        } else {
            payload_len
                .saturating_add(chunk_size.saturating_sub(1))
                .checked_div(chunk_size)
                .unwrap_or(usize::MAX)
        };
        let prefix_len = prefix.as_ref().map(|value| value.len()).unwrap_or(0);
        Self {
            stream_id,
            correlation_id,
            payload_len,
            chunk_size,
            chunk_count,
            prefix,
            prefix_len,
            prefix_sent: 0,
            payload,
            payload_remaining: expected_payload_len,
            frame_index: 0,
            frame_offset: 0,
        }
    }

    fn frame_payload_len(&self, frame_index: usize) -> usize {
        if frame_index == 0 {
            return self.payload_len.min(self.chunk_size);
        }
        let consumed = self
            .chunk_size
            .saturating_add(frame_index.saturating_sub(1).saturating_mul(self.chunk_size));
        self.payload_len.saturating_sub(consumed).min(self.chunk_size)
    }

    fn frame_header(&self, frame_index: usize) -> InlineFrameHeader {
        if frame_index == 0 {
            InlineFrameHeader::from_array(crate::framing::write_stream_response_start_header(
                self.stream_id,
                self.correlation_id,
                self.payload_len as u32,
                self.frame_payload_len(frame_index),
            ))
        } else {
            InlineFrameHeader::from_array(crate::framing::write_stream_data_header(
                true,
                self.stream_id,
                frame_index as u32,
                self.frame_payload_len(frame_index),
            ))
        }
    }

    fn wire_len(&self) -> usize {
        crate::framing::STREAM_RESPONSE_START_FRAME_HEADER_LEN
            + self.payload_len
            + self
                .chunk_count
                .saturating_sub(1)
                .saturating_mul(crate::framing::STREAM_DATA_FRAME_HEADER_LEN)
    }
}

/// Connection-local queue used for immediate streaming responses generated by
/// the read pipeline. Unlike the shared producer queue, this queue can account
/// retained `Bytes` and refuse admission before a burst becomes unbounded.
struct LocalStreamingQueue {
    queue: std::collections::VecDeque<StreamingCommand>,
    queued_bytes: usize,
    /// A response command has been handed to the IO owner and has not yet
    /// reached its terminal Flush. The retained command is not included in
    /// `queued_bytes`, so response admission accounts for it separately while
    /// the read side remains free to drain the peer.
    response_in_flight: bool,
    wire_blocked: bool,
    /// Reserve room for one maximum-sized response when reading another
    /// frame. A single response larger than the byte cap is admitted only
    /// while the queue is otherwise empty; a second response is backpressured
    /// until the first one drains.
    response_reserve_bytes: usize,
    /// Reserve command slots for the next maximum-sized response as well as
    /// bytes. The command cap is independent from the retained-payload cap,
    /// so byte-only admission can otherwise consume the last slots before a
    /// response is expanded into its chunk frames.
    #[cfg_attr(not(test), allow(dead_code))]
    response_reserve_commands: usize,
    /// One response that arrived after the bounded queue filled. Keeping the
    /// complete command batch here preserves the consumed ask's response
    /// without opening an unbounded side queue; `is_full` stops reads until
    /// the current response drains and `pop_front` promotes this batch.
    deferred: Option<Vec<StreamingCommand>>,
}

impl LocalStreamingQueue {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_response_reserve(STREAM_CHUNK_SIZE)
    }

    fn with_response_reserve(max_message_size: usize) -> Self {
        let response_reserve_bytes = std::cmp::min(
            max_message_size.max(STREAM_CHUNK_SIZE),
            STREAMING_RESPONSE_QUEUE_BYTE_CAP,
        );
        let response_reserve_commands = max_message_size
            .saturating_add(STREAM_CHUNK_SIZE.saturating_sub(1))
            .checked_div(STREAM_CHUNK_SIZE)
            .unwrap_or(1)
            .saturating_add(2)
            .min(STREAMING_RESPONSE_QUEUE_COMMAND_CAP);
        Self {
            queue: std::collections::VecDeque::new(),
            queued_bytes: 0,
            response_in_flight: false,
            response_reserve_bytes,
            response_reserve_commands,
            deferred: None,
            wire_blocked: false,
        }
    }

    fn pop_front(&mut self) -> Option<StreamingCommand> {
        if self.queue.is_empty() && !self.response_in_flight {
            if let Some(deferred) = self.deferred.take() {
                self.queued_bytes = deferred.iter().map(streaming_command_bytes).sum();
                self.queue.extend(deferred);
            }
        }
        let command = self.queue.pop_front()?;
        self.queued_bytes = self
            .queued_bytes
            .saturating_sub(streaming_command_bytes(&command));
        self.response_in_flight = !matches!(&command, StreamingCommand::Flush);
        Some(command)
    }

    fn set_wire_blocked(&mut self, blocked: bool) {
        self.wire_blocked = blocked;
    }

    fn wire_blocked(&self) -> bool {
        self.wire_blocked
    }

    fn has_pending(&self) -> bool {
        !self.queue.is_empty() || self.deferred.is_some()
    }

    /// Return whether a response with this command/byte footprint can be
    /// retained without copying it first. If the bounded queue cannot fit it,
    /// the single deferred-response slot is the admission path.
    fn can_admit_response(&self, command_count: usize, response_bytes: usize) -> bool {
        if self.deferred.is_some() {
            return false;
        }
        let fits_queue = self.queue.len().saturating_add(command_count)
            <= STREAMING_RESPONSE_QUEUE_COMMAND_CAP
            && self.queued_bytes.saturating_add(response_bytes)
                <= STREAMING_RESPONSE_QUEUE_BYTE_CAP;
        // The deferred slot is itself a valid admission result. Keep the
        // footprint arguments in the predicate so this remains the single
        // preflight point for pooled responses, even when the queue cannot
        // fit the complete response in its bounded resident window.
        fits_queue || self.deferred.is_none()
    }

    fn is_full(&self) -> bool {
        if self.deferred.is_some() {
            return true;
        }
        if self.queue.is_empty() {
            return false;
        }
        self.queue
            .len()
            .saturating_add(self.response_reserve_commands)
            > STREAMING_RESPONSE_QUEUE_COMMAND_CAP
            || self.queued_bytes >= STREAMING_RESPONSE_QUEUE_BYTE_CAP
            || self
                .queued_bytes
                .saturating_add(self.response_reserve_bytes)
                > STREAMING_RESPONSE_QUEUE_BYTE_CAP
    }

    fn try_extend<I>(&mut self, commands: I) -> Result<()>
    where
        I: IntoIterator<Item = StreamingCommand>,
    {
        let commands: Vec<_> = commands.into_iter().collect();
        let added_bytes: usize = commands.iter().map(streaming_command_bytes).sum();
        let exceeds_cap = self.queue.len().saturating_add(commands.len())
            > STREAMING_RESPONSE_QUEUE_COMMAND_CAP
            || self.queued_bytes.saturating_add(added_bytes) > STREAMING_RESPONSE_QUEUE_BYTE_CAP;
        let admit_single_oversize =
            self.queue.is_empty() && !self.response_in_flight && exceeds_cap;
        if exceeds_cap && !admit_single_oversize {
            if self.deferred.is_some() {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "immediate streaming response queue deferred slot is full",
                )));
            }
            self.deferred = Some(commands);
            return Ok(());
        }
        if admit_single_oversize {
            self.response_in_flight = true;
        }
        self.queued_bytes = self.queued_bytes.saturating_add(added_bytes);
        self.queue.extend(commands);
        Ok(())
    }
}

fn streaming_command_bytes(command: &StreamingCommand) -> usize {
    match command {
        StreamingCommand::WriteBytes(data) => data.len(),
        StreamingCommand::Flush => 0,
        StreamingCommand::VectoredWrite(item) => item.header.len() + item.payload.len(),
        StreamingCommand::OwnedChunks(chunks) => chunks.iter().map(bytes::Bytes::len).sum(),
        StreamingCommand::PooledResponse(response) => response.wire_len(),
        StreamingCommand::BytesResponse(response) => response.wire_len(),
        StreamingCommand::Abort { stream_id, reason } => crate::framing::write_stream_abort_header(*stream_id, *reason).len(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamingSource {
    Local,
    Shared,
}

/// Pick the next streaming source without allowing a continuously replenished
/// local response queue to starve producer-owned shared streams.
fn choose_streaming_source(
    prefer_shared: bool,
    local_ready: bool,
    shared_ready: bool,
) -> Option<StreamingSource> {
    match (local_ready, shared_ready, prefer_shared) {
        (false, false, _) => None,
        (true, false, _) => Some(StreamingSource::Local),
        (false, true, _) => Some(StreamingSource::Shared),
        (true, true, true) => Some(StreamingSource::Shared),
        (true, true, false) => Some(StreamingSource::Local),
    }
}

/// Consumer-owned progress for one streaming command. A command can be much
/// larger than the transport's writable capacity, so the IO task retains it
/// across turns and writes only a bounded prefix before returning to inbound
/// reads. `from_shared_queue` preserves the existing queue-capacity contract:
/// producers are notified only when the popped command is fully consumed.
struct PendingStreamingCommand {
    command: StreamingCommand,
    offset: usize,
    from_shared_queue: bool,
    yield_after_frame: bool,
}

impl PendingStreamingCommand {
    fn shared(command: StreamingCommand) -> Self {
        Self {
            command,
            offset: 0,
            from_shared_queue: true,
            yield_after_frame: false,
        }
    }

    fn local(command: StreamingCommand) -> Self {
        Self {
            command,
            offset: 0,
            from_shared_queue: false,
            yield_after_frame: false,
        }
    }
}

impl std::fmt::Debug for StreamingCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamingCommand::WriteBytes(bytes) => {
                f.debug_tuple("WriteBytes").field(&bytes.len()).finish()
            }
            StreamingCommand::Flush => f.write_str("Flush"),
            StreamingCommand::VectoredWrite(item) => f
                .debug_struct("VectoredWrite")
                .field("header_len", &item.header.len())
                .field("payload_len", &item.payload.len())
                .finish(),
            StreamingCommand::OwnedChunks(chunks) => f
                .debug_struct("OwnedChunks")
                .field("chunk_count", &chunks.len())
                .field("total_len", &chunks.iter().map(|c| c.len()).sum::<usize>())
                .finish(),
            StreamingCommand::PooledResponse(response) => f
                .debug_struct("PooledResponse")
                .field("payload_len", &response.payload_len)
                .field("chunk_count", &response.chunk_count)
                .finish(),
            StreamingCommand::BytesResponse(response) => f
                .debug_struct("BytesResponse")
                .field("payload_len", &response.payload_len)
                .field("chunk_count", &response.chunk_count)
                .finish(),
            StreamingCommand::Abort { stream_id, reason } => f
                .debug_struct("Abort")
                .field("stream_id", stream_id)
                .field("reason", reason)
                .finish(),
        }
    }
}
