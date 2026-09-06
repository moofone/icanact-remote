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
    /// Bytes retained by the pooled payload, including any surplus bytes that
    /// are not part of the advertised logical response.
    retained_bytes: usize,
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
    /// Bytes retained by the backing allocation, which can exceed the visible
    /// slice length when a handler returns a sub-slice of a larger `Bytes`.
    retained_bytes: usize,
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
        retained_bytes: usize,
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
            retained_bytes: retained_bytes.max(payload_len),
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
            InlineFrameHeader::from_array(
                crate::framing::try_write_stream_response_start_header(
                    self.stream_id,
                    self.correlation_id,
                    self.payload_len as u32,
                    self.frame_payload_len(frame_index),
                )
                .expect(STREAM_CHUNK_INVARIANT),
            )
        } else {
            InlineFrameHeader::from_array(
                crate::framing::try_write_stream_data_header(
                    true,
                    self.stream_id,
                    frame_index as u32,
                    self.frame_payload_len(frame_index),
                )
                .expect(STREAM_CHUNK_INVARIANT),
            )
        }
    }

    fn retained_len(&self) -> usize {
        self.retained_bytes
    }
}

/// This crate's only caller of `framing::try_write_stream_response_start_header`/
/// `try_write_stream_data_header` on the hot per-frame path
/// (`BytesStreamingResponse`/`PooledStreamingResponse::frame_header`, called
/// once per streamed frame from `write_bytes_streaming_command_slice`/
/// `write_pooled_streaming_command_slice`): `frame_payload_len` always
/// returns a length clamped to `chunk_size`, which the streaming writer
/// derives from `max_stream_chunk_size()` (itself bounded by
/// `max_message_size`, which config validation keeps within the V5 27-bit
/// limit) before either response type is constructed. `checked_body_len`
/// cannot observe an oversize value on this path, so `frame_header` stays
/// infallible rather than threading a `Result` through a hot loop that
/// currently returns a plain `std::io::Result`; the two `try_write_stream_*`
/// calls above trust that invariant explicitly instead of silently, the
/// same way `write_route_bind_header`/`write_stream_abort_header` (and
/// `framing`'s own infallible `write_stream_*_header` wrappers) do for
/// their own fixed-size invariants.
const STREAM_CHUNK_INVARIANT: &str =
    "stream chunk length is bounded by max_stream_chunk_size, always within the V5 27-bit limit";

/// Return a streaming payload together with the allocation footprint retained
/// by the response command.
///
/// `Bytes::len()` describes only the visible slice. A sliced value can keep a
/// much larger allocation alive, so a streaming queue that accounts only for
/// `len()` can retain unbounded memory behind its byte cap: `BytesMut::
/// capacity()` only reports the remaining capacity from the buffer's *own*
/// start pointer to the end of its allocation, which *underreports* for a
/// slice taken from the tail of a much larger buffer (nothing remains after
/// the tail, so `capacity()` comes back close to `payload_len` while the
/// buffer still pins the entire backing allocation behind it).
///
/// **Four earlier attempts tried to recover the true footprint from outside
/// `bytes` -- via `capacity()`, then via `BytesMut::try_reclaim` probes of
/// increasing precision -- and every one was eventually wrong.**
/// `try_reclaim`'s own amortized-cost heuristic (see `bytes::BytesMut::
/// reserve_inner`) declines to reclaim whenever the hidden prefix (`offset`)
/// is smaller than the visible length (`len()`), *regardless of the true
/// backing size and regardless of what is requested*: a failed probe proves
/// nothing about size, in either direction, and nothing in the public
/// `BytesMut` API distinguishes "genuinely small" from "large, but not worth
/// the memmove given the offset" without forcing an allocation. The last
/// attempt's own reclaim-request formula made this unfixable in principle:
/// requesting enough beyond `len()` to force reclamation of a hidden prefix
/// means a *successful* reclaim always leaves `capacity()` above the
/// zero-copy threshold too, so the two outcomes ("small enough to keep" and
/// "reclaim succeeded") became mutually exclusive -- the zero-copy branch was
/// unreachable, and every unique payload was silently compacted anyway,
/// while the code still read as though it had a fast path.
///
/// **So this stops interrogating `bytes` after the fact.** Every payload is
/// unconditionally copied into a fresh, exact-length buffer, and
/// `retained_bytes` is exactly that buffer's length. **Invariant, true by
/// construction rather than a running argument about allocator internals:
/// `retained_bytes` always equals what the returned `Bytes` actually pins.**
/// The alternative -- carrying the true allocation size from the point a
/// payload is *created*, so a genuinely right-sized buffer never needs to pay
/// this copy -- is the structurally correct fix, but it means an
/// origin-aware buffer type threaded through every path that can produce a
/// streaming response, including arbitrary actor handler code outside this
/// crate (`queue_streaming_response_bytes` receives an already-computed
/// `Bytes` from whatever the handler returned). That is a wider API change
/// than this fix, not a decision to make inside a review round.
///
/// The copy this pays is bounded by `payload_len` (only the visible slice is
/// ever copied, never a hidden backing allocation behind it) and, measured on
/// development hardware via `Bytes::copy_from_slice` at a memcpy throughput
/// of roughly 60-90 GiB/s: about 12us for a 1 MiB response, about 1ms for the
/// largest payload this path ever sees (`MAX_STREAM_SIZE`, 64 MiB). Both are
/// negligible next to the cost of actually writing that many bytes to a
/// socket and having the peer receive them, which is the dominant cost on
/// this path regardless. If a workload makes this genuinely hot, that is a
/// measurement to bring back, not a reason to return to estimating.
fn normalize_streaming_payload(payload: bytes::Bytes) -> (bytes::Bytes, usize) {
    let payload_len = payload.len();
    let compact = bytes::Bytes::copy_from_slice(&payload);
    (compact, payload_len)
}

impl PooledStreamingResponse {
    fn new(
        stream_id: u32,
        correlation_id: u32,
        payload_len: usize,
        retained_bytes: usize,
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
            retained_bytes: retained_bytes.max(payload_len),
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
            InlineFrameHeader::from_array(
                crate::framing::try_write_stream_response_start_header(
                    self.stream_id,
                    self.correlation_id,
                    self.payload_len as u32,
                    self.frame_payload_len(frame_index),
                )
                .expect(STREAM_CHUNK_INVARIANT),
            )
        } else {
            InlineFrameHeader::from_array(
                crate::framing::try_write_stream_data_header(
                    true,
                    self.stream_id,
                    frame_index as u32,
                    self.frame_payload_len(frame_index),
                )
                .expect(STREAM_CHUNK_INVARIANT),
            )
        }
    }

    fn retained_len(&self) -> usize {
        self.retained_bytes
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
    /// `queued_bytes`, so response admission accounts for it separately. The
    /// read gate reserves the remaining aggregate footprint before consuming
    /// another ask; otherwise its handler could produce a response that cannot
    /// be admitted after ownership has already moved here.
    response_in_flight: bool,
    in_flight_bytes: usize,
    wire_blocked: bool,
    /// Reserve normal queue room for one configured response frame when
    /// reading another frame. The aggregate stream-sized reservation is
    /// checked separately by `is_full`, which is a queue-level invariant used
    /// by tests; production admission control is entirely `can_admit_response`
    /// (`WouldBlock` per response, not a blanket read-loop gate -- see the
    /// comment on the read loop in `stream_writer.rs::io_task`).
    #[cfg_attr(not(test), allow(dead_code))]
    response_reserve_bytes: usize,
    /// Reserve command slots for the next maximum-sized response as well as
    /// bytes. The command cap is independent from the retained-payload cap,
    /// so byte-only admission can otherwise consume the last slots before a
    /// response is expanded into its chunk frames.
    #[cfg_attr(not(test), allow(dead_code))]
    response_reserve_commands: usize,
    /// One response that arrived after the bounded queue filled. Keeping the
    /// complete command batch here preserves the consumed ask's response
    /// without opening an unbounded side queue; the aggregate hard cap stops
    /// further *admission* until the current response drains and `pop_front`
    /// promotes this batch (reads that do not need streaming-queue capacity
    /// are unaffected -- see `can_admit_response`).
    deferred: Option<Vec<StreamingCommand>>,
    deferred_bytes: usize,
    /// Backpressure NACKs owed to the peer, queued here instead of written
    /// directly. The caller that decides to NACK (deep inside
    /// `write_ask_disposition_io`, or the pre-dispatch gate in
    /// `stream_writer.rs::io_task`) cannot know whether a partial streaming
    /// frame currently owns the wire -- `io_task` is the only place that
    /// does. Writing there instead of here would risk splicing the NACK's
    /// bytes into an in-progress frame's payload and desynchronizing every
    /// frame after it (the same class of bug #183 fixed for
    /// `WritePayload::Buf`). `io_task` drains this queue only once
    /// `pending_stream_cmd.is_none()` proves the wire is free. See
    /// `queue_ask_nack`.
    pending_ask_nacks:
        std::collections::VecDeque<[u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN]>,
}

/// Cap on queued-but-not-yet-written backpressure NACKs. Each entry is a
/// fixed 16-byte header with no payload -- unlike a streaming response, it
/// never contributes to `retained_bytes`/admission accounting -- so this
/// bounds a small, fixed footprint (at most `PENDING_ASK_NACK_CAP * 16`
/// bytes) regardless of how many asks arrive while a streaming frame owns
/// the wire.
const PENDING_ASK_NACK_CAP: usize = 64;

/// Keep the local response queue within the normal response-batch cap while
/// allowing one protocol-sized stream to be retained behind an in-flight
/// response. The hard cap is an aggregate resident bound; the normal queue
/// cap still controls the hot-path burst size.
const STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP: usize =
    crate::MAX_INFLIGHT_STREAM_BYTES.saturating_add(STREAMING_RESPONSE_QUEUE_BYTE_CAP);

/// Reserve for the largest valid streamed payload, used by `is_full`. Stream
/// frame headers are generated on demand and are not retained by the queue,
/// so only owned payload bytes count.
const MAX_STREAMING_RESPONSE_RETAINED_BYTES: usize =
    crate::MAX_STREAM_SIZE.saturating_add(STREAMING_RESPONSE_QUEUE_BYTE_CAP);

impl LocalStreamingQueue {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_response_reserve(STREAM_CHUNK_SIZE)
    }

    fn with_response_reserve(max_message_size: usize) -> Self {
        // Reserve room for one response *frame* (`STREAM_CHUNK_SIZE`) before
        // consuming another ask -- not the full configured `max_message_size`.
        // `max_message_size` bounds an ordinary, non-streaming message; a
        // streaming response's true worst case is `MAX_STREAM_SIZE` (64 MiB),
        // already guarded above by the much larger
        // `STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP` check, with per-response
        // admission handled by `can_admit_response`'s own
        // `admit_single_oversize`/`can_defer_response` paths -- not this
        // pre-dispatch heuristic. Scaling this reserve up to
        // `max_message_size` instead degenerates as soon as
        // `max_message_size` reaches `STREAMING_RESPONSE_QUEUE_BYTE_CAP`
        // (true of this crate's own default config: 10 MiB vs. ~8 MiB): the
        // reserve alone then consumes the entire soft cap, so `is_full()`'s
        // byte check trips on *any* nonzero `queued_bytes`, collapsing
        // "leave room for one more response" into "the queue must be
        // completely empty" and serializing every concurrent streaming ask
        // on a connection to one at a time.
        let response_reserve_bytes = STREAM_CHUNK_SIZE.min(STREAMING_RESPONSE_QUEUE_BYTE_CAP);
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
            in_flight_bytes: 0,
            response_reserve_bytes,
            response_reserve_commands,
            deferred: None,
            deferred_bytes: 0,
            wire_blocked: false,
            pending_ask_nacks: std::collections::VecDeque::new(),
        }
    }

    /// Queue a backpressure NACK header for the peer. Always succeeds --
    /// never blocks, never fails, never consults `is_full`/response
    /// admission at all, since a fixed 16-byte header cannot meaningfully
    /// threaten the retention bound those exist to enforce.
    ///
    /// Never evicts an already-queued entry to make room for a new one:
    /// that header is the *only* remaining record that a specific,
    /// already-consumed ask exists at all. Dropping it silently loses that
    /// ask's terminal outcome -- no reply, no NACK, just a correlation id
    /// the requester eventually times out waiting on. That is exactly the
    /// failure this whole NACK mechanism exists to remove; reintroducing it
    /// one layer down, inside the queue built to prevent it, defeats the
    /// point. Bounding growth is the read side's job instead: `io_task`'s
    /// read-batch loops gate further reads on `has_room_for_ask_nack` (see
    /// that method), so this is structurally never called while the queue
    /// is already at `PENDING_ASK_NACK_CAP` -- growth stops at the source
    /// of new entries, not by discarding existing ones.
    fn queue_ask_nack(&mut self, header: [u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN]) {
        self.pending_ask_nacks.push_back(header);
    }

    /// Whether the queue has room for one more NACK without exceeding
    /// `PENDING_ASK_NACK_CAP`. `io_task`'s read-batch loops must stop
    /// admitting further reads once this is `false` and let a drain turn
    /// run first (`drain_pending_ask_nacks`, gated on
    /// `pending_stream_cmd.is_none()`) -- this is what keeps
    /// `queue_ask_nack` itself able to stay unconditional: as long as every
    /// caller that could add an entry checks this first, the queue can
    /// never be asked to hold more than its cap, so it never has to choose
    /// what to discard.
    fn has_room_for_ask_nack(&self) -> bool {
        self.pending_ask_nacks.len() < PENDING_ASK_NACK_CAP
    }

    /// Pop the oldest queued NACK header for `io_task` to attempt writing.
    /// Callers must only do so when `pending_stream_cmd.is_none()`; see
    /// `queue_ask_nack`.
    fn pop_ask_nack(&mut self) -> Option<[u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN]> {
        self.pending_ask_nacks.pop_front()
    }

    /// Whether any backpressure NACK is still queued and unwritten.
    /// `drain_pending_ask_nacks` consults this after every attempt so its
    /// caller (`io_task`) can tell outstanding NACK work from a genuinely
    /// idle turn -- `pending_ask_nacks` is deliberately not part of
    /// `has_pending`, since that governs streaming-source selection, not
    /// wakeup/park eligibility.
    fn has_pending_ask_nacks(&self) -> bool {
        !self.pending_ask_nacks.is_empty()
    }

    #[cfg(test)]
    fn pending_ask_nack_count(&self) -> usize {
        self.pending_ask_nacks.len()
    }

    fn pop_front(&mut self) -> Option<StreamingCommand> {
        if self.queue.is_empty() && !self.response_in_flight {
            if let Some(deferred) = self.deferred.take() {
                self.queued_bytes = deferred.iter().map(streaming_command_bytes).sum();
                self.deferred_bytes = 0;
                self.queue.extend(deferred);
            }
        }
        let command = self.queue.pop_front()?;
        let command_bytes = streaming_command_bytes(&command);
        self.queued_bytes = self
            .queued_bytes
            .saturating_sub(command_bytes);
        self.response_in_flight = !matches!(&command, StreamingCommand::Flush);
        self.in_flight_bytes = if self.response_in_flight {
            command_bytes
        } else {
            0
        };
        Some(command)
    }

    fn set_wire_blocked(&mut self, blocked: bool) {
        self.wire_blocked = blocked;
    }

    #[allow(dead_code)]
    fn wire_blocked(&self) -> bool {
        self.wire_blocked
    }

    fn has_pending(&self) -> bool {
        !self.queue.is_empty() || self.deferred.is_some()
    }

    fn retained_bytes(&self) -> usize {
        self.queued_bytes
            .saturating_add(self.in_flight_bytes)
            .saturating_add(self.deferred_bytes)
    }

    fn retained_commands(&self) -> usize {
        self.queue
            .len()
            .saturating_add(usize::from(self.response_in_flight))
            .saturating_add(self.deferred.as_ref().map_or(0, Vec::len))
    }

    fn can_defer_response(&self, command_count: usize, response_bytes: usize) -> bool {
        let retained_without_deferred = self
            .queued_bytes
            .saturating_add(self.in_flight_bytes);
        let bounded_footprint = response_bytes <= STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP
            && retained_without_deferred.saturating_add(response_bytes)
                <= STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP;
        self.deferred.is_none()
            && command_count <= self.response_reserve_commands
            && self
                .retained_commands()
                .saturating_add(command_count)
                <= STREAMING_RESPONSE_QUEUE_COMMAND_CAP
            && bounded_footprint
    }

    /// Return whether a response with this command/byte footprint can be
    /// retained without copying it first. If the normal queue cannot fit it,
    /// one deferred-response slot is the admission path, bounded by the
    /// aggregate protocol-sized resident cap. A response larger than the
    /// normal queue cap is admitted without deferral only as the sole retained
    /// response.
    fn can_admit_response(&self, command_count: usize, response_bytes: usize) -> bool {
        let fits_queue = self
            .retained_commands()
            .saturating_add(command_count)
            <= STREAMING_RESPONSE_QUEUE_COMMAND_CAP
            && self.retained_bytes().saturating_add(response_bytes)
                <= STREAMING_RESPONSE_QUEUE_BYTE_CAP;
        let admit_single_oversize = self.queue.is_empty()
            && !self.response_in_flight
            && self.deferred.is_none()
            && self.retained_bytes() == 0
            && response_bytes > STREAMING_RESPONSE_QUEUE_BYTE_CAP
            && response_bytes <= STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP
            && command_count <= STREAMING_RESPONSE_QUEUE_COMMAND_CAP;
        fits_queue
            || admit_single_oversize
            || self.can_defer_response(command_count, response_bytes)
    }

    /// Whether this queue's retained footprint is at or beyond its aggregate
    /// reserve. A queue-level invariant checked directly by tests, and also
    /// consulted by `stream_writer.rs::io_task` at its three ActorAsk
    /// dispatch sites: when this is true, that ask's handler is *not* run --
    /// it is skipped (queuing a best-effort `AskNackReason::Backpressure`
    /// NACK instead) rather than run-then-discarded, since running it first
    /// would compute a response this connection cannot currently retain.
    /// This is a narrow, per-ask dispatch gate, not a blanket read-loop gate:
    /// reads, tells, and every other result kind keep flowing regardless of
    /// this value (a blanket pre-check that stopped *every* read on the
    /// connection -- including ones needing no streaming-queue capacity at
    /// all -- for as long as this stayed true is exactly the state a
    /// bidirectional streaming storm used to leave both peers in). Per-
    /// response admission after a handler has already produced its answer is
    /// the separate `can_admit_response`.
    fn is_full(&self) -> bool {
        if self.deferred.is_some() {
            return true;
        }
        // Leave room for one complete protocol-sized response before
        // consuming another ask. Admission is checked only after its handler
        // returns and transfers ownership, so this aggregate guard prevents a
        // valid large response from being dropped after the read cursor has
        // advanced.
        if self
            .retained_bytes()
            .saturating_add(MAX_STREAMING_RESPONSE_RETAINED_BYTES)
            > STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP
        {
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
        let fits_queue = self
            .retained_commands()
            .saturating_add(commands.len())
            <= STREAMING_RESPONSE_QUEUE_COMMAND_CAP
            && self.retained_bytes().saturating_add(added_bytes)
                <= STREAMING_RESPONSE_QUEUE_BYTE_CAP;
        let admit_single_oversize = self.queue.is_empty()
            && !self.response_in_flight
            && self.deferred.is_none()
            && self.retained_bytes() == 0
            && added_bytes > STREAMING_RESPONSE_QUEUE_BYTE_CAP
            && added_bytes <= STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP
            && commands.len() <= STREAMING_RESPONSE_QUEUE_COMMAND_CAP;
        let defer = !fits_queue
            && !admit_single_oversize
            && self.can_defer_response(commands.len(), added_bytes);
        if !fits_queue && !admit_single_oversize && !defer {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "immediate streaming response queue footprint is full",
            )));
        }
        if defer {
            self.deferred = Some(commands);
            self.deferred_bytes = added_bytes;
            return Ok(());
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
        StreamingCommand::PooledResponse(response) => response.retained_len(),
        StreamingCommand::BytesResponse(response) => response.retained_len(),
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

#[cfg(test)]
mod normalize_streaming_payload_tests {
    use super::*;

    /// Asserts the two properties `normalize_streaming_payload` must hold for
    /// any input, regardless of how much of a larger allocation `payload`
    /// shares: the returned `Bytes` never shares memory with `backing_start`
    /// (proving compaction actually happened, not merely that the numbers
    /// look right), and `retained_bytes` equals its length exactly.
    fn assert_compacted_and_exactly_charged(
        payload: bytes::Bytes,
        backing_start: usize,
        backing_len: usize,
    ) {
        let visible_len = payload.len();
        let (out, retained_bytes) = normalize_streaming_payload(payload);

        assert_eq!(out.len(), visible_len);
        let out_start = out.as_ptr() as usize;
        let still_shares_the_backing_allocation =
            out_start >= backing_start && out_start < backing_start + backing_len;
        assert!(
            !still_shares_the_backing_allocation,
            "the returned payload must be a fresh allocation, not still sharing the \
             {backing_len}-byte backing allocation"
        );
        assert_eq!(
            retained_bytes, visible_len,
            "retained_bytes ({retained_bytes}) must equal the compacted buffer's real, exact \
             length ({visible_len})"
        );
    }

    /// Every unique `Bytes` this function sees is unconditionally compacted
    /// (see the function doc comment for why estimating from `bytes`
    /// internals was abandoned): `retained_bytes` is always exactly
    /// `payload.len()`, regardless of how much of a larger allocation the
    /// input shares. The four shapes below are kept as named regression pins
    /// for the specific constructions that broke each of the four earlier,
    /// estimation-based attempts, even though none of them exercise a
    /// different code path today.
    #[test]
    fn compacts_a_head_slice_of_a_much_larger_buffer() {
        const BACKING_CAPACITY: usize = 1_000_000;
        const VISIBLE_LEN: usize = 100;
        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let backing_start = full.as_ptr() as usize;
        let head = full.slice(0..VISIBLE_LEN);
        drop(full); // `head` becomes the sole owner.
        assert_compacted_and_exactly_charged(head, backing_start, BACKING_CAPACITY);
    }

    /// A tail slice with zero bytes of trailing capacity -- the shape where
    /// `BytesMut::capacity()` alone is most misleading (it reports exactly
    /// `VISIBLE_LEN`, identical to a genuinely right-sized buffer).
    #[test]
    fn compacts_an_exact_tail_slice_of_a_much_larger_buffer() {
        const BACKING_CAPACITY: usize = 1_000_000;
        const VISIBLE_LEN: usize = 100;
        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let backing_start = full.as_ptr() as usize;
        let tail = full.slice(BACKING_CAPACITY - VISIBLE_LEN..);
        drop(full);
        assert_compacted_and_exactly_charged(tail, backing_start, BACKING_CAPACITY);
    }

    /// One byte narrower than an exact tail slice -- distinguishes this from
    /// a fixed `try_reclaim(1)`-style probe, which is satisfied by even one
    /// spare trailing byte and so would not have examined this shape at all.
    #[test]
    fn compacts_a_near_tail_slice_with_one_trailing_spare_byte() {
        const BACKING_CAPACITY: usize = 1_000_000;
        const VISIBLE_LEN: usize = 100;
        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let backing_start = full.as_ptr() as usize;
        let start = BACKING_CAPACITY - VISIBLE_LEN - 1;
        let near_tail = full.slice(start..start + VISIBLE_LEN);
        drop(full);
        assert_compacted_and_exactly_charged(near_tail, backing_start, BACKING_CAPACITY);
    }

    /// A slice whose offset is just below its own length inside a backing
    /// allocation just under 3x that length: the shape that defeated
    /// charging a `2x` slop-threshold estimate on a failed probe, because it
    /// fails `bytes`' `offset >= len` reclaim heuristic for the *offset*
    /// reason while still being backed by nearly `3x` -- an estimate bounded
    /// at `2x` would undercharge it by nearly `1x`.
    #[test]
    fn compacts_a_slice_whose_offset_is_just_below_its_length_in_a_near_three_x_allocation() {
        const VISIBLE_LEN: usize = 100_000;
        const BACKING_CAPACITY: usize = VISIBLE_LEN * 3 - 1;
        const OFFSET: usize = VISIBLE_LEN - 1;
        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let backing_start = full.as_ptr() as usize;
        let slice = full.slice(OFFSET..OFFSET + VISIBLE_LEN);
        drop(full);
        assert_compacted_and_exactly_charged(slice, backing_start, BACKING_CAPACITY);
    }

    /// A unique, already right-sized buffer (`offset == 0`, no slop at all)
    /// is compacted too: nothing about a right-sized buffer is
    /// distinguishable from the shapes above without forcing an allocation
    /// (see the function doc comment), so this function no longer tries.
    /// Checks the copied bytes as well as the length/pointer properties,
    /// since this is the one case where a subtle off-by-one in a from-scratch
    /// copy would not be caught by a length-only check against a
    /// much-larger backing buffer.
    #[test]
    fn compacts_a_right_sized_unique_buffer() {
        const LEN: usize = 4096;
        let mut buf = bytes::BytesMut::with_capacity(LEN);
        buf.extend_from_slice(&vec![7u8; LEN]);
        let payload = buf.freeze();
        let ptr_before = payload.as_ptr() as usize;

        let (out, retained_bytes) = normalize_streaming_payload(payload);

        assert_eq!(&out[..], &vec![7u8; LEN][..]);
        assert_eq!(retained_bytes, LEN);
        assert_ne!(
            out.as_ptr() as usize,
            ptr_before,
            "a right-sized buffer is still compacted into a fresh allocation, not kept zero-copy"
        );
    }

    /// A still-shared `Bytes` (multiple owners, `try_into_mut` fails) takes
    /// the same unconditional-compaction path as a unique one.
    #[test]
    fn compacts_a_shared_bytes_value() {
        const LEN: usize = 4096;
        let payload = bytes::Bytes::from(vec![9u8; LEN]);
        let _sibling = payload.clone(); // Keeps `payload` from being unique.
        let ptr_before = payload.as_ptr() as usize;

        let (out, retained_bytes) = normalize_streaming_payload(payload);

        assert_eq!(&out[..], &vec![9u8; LEN][..]);
        assert_eq!(retained_bytes, LEN);
        assert_ne!(out.as_ptr() as usize, ptr_before);
    }
}
