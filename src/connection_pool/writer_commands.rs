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
#[expect(
    dead_code,
    reason = "the direct write command remains part of the writer-owned transport command set"
)]
enum StreamingCommand {
    /// Direct write bytes for streaming.
    WriteBytes(bytes::Bytes),
    /// Flush the writer.
    Flush,
    /// Vectored write for header + payload (zero-copy).
    VectoredWrite(VectoredSendItem),
    /// Batch of owned chunks for streaming (zero-copy).
    OwnedChunks(Vec<bytes::Bytes>),
    /// Abort a partially transmitted stream. This stays on the streaming FIFO
    /// so it cannot overtake data chunks that were already accepted.
    Abort { stream_id: u32, reason: u32 },
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
}

impl PendingStreamingCommand {
    fn shared(command: StreamingCommand) -> Self {
        Self {
            command,
            offset: 0,
            from_shared_queue: true,
        }
    }

    fn local(command: StreamingCommand) -> Self {
        Self {
            command,
            offset: 0,
            from_shared_queue: false,
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
            StreamingCommand::Abort { stream_id, reason } => f
                .debug_struct("Abort")
                .field("stream_id", stream_id)
                .field("reason", reason)
                .finish(),
        }
    }
}
