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

    fn retained_len(&self) -> usize {
        self.retained_bytes
    }
}

/// A unique buffer is kept zero-copy only while its backing allocation is
/// within this factor of the visible payload. Beyond it, the backing
/// allocation is treated as slop from a larger scratch buffer (e.g. a small
/// slice taken from the head or tail of a much bigger read/response buffer)
/// rather than a right-sized response, and is compacted instead. `2x`
/// tolerates ordinary allocator rounding without letting a small logical
/// response retain an arbitrarily larger allocation.
const STREAMING_PAYLOAD_RETAIN_SLOP_FACTOR: usize = 2;

/// Return a streaming payload together with the allocation footprint retained
/// by the response command.
///
/// `Bytes::len()` describes only the visible slice. A sliced value can keep a
/// much larger allocation alive, so a streaming queue that accounts only for
/// `len()` can retain unbounded memory behind its byte cap. Unique `Bytes`
/// values can expose their existing capacity without a copy, but
/// `BytesMut::capacity()` only reports the remaining capacity from the
/// buffer's *own* start pointer to the end of its allocation. For a slice
/// taken at offset 0 of a much larger buffer that overreports (the whole
/// backing allocation, correctly rejected by the slop check below), but for a
/// slice taken from the *tail* of a much larger buffer it *underreports*:
/// nothing remains after the tail, so `capacity()` comes back close to
/// `payload_len` -- passing the slop check and staying zero-copy -- while the
/// buffer still pins the entire backing allocation behind it, invisible to
/// `capacity()` alone.
///
/// `BytesMut::try_reclaim` closes that gap. It never allocates (`bytes`
/// guarantees it only ever reuses storage the handle already owns), so a
/// probe is safe to call unconditionally: when the sole owner has enough
/// *reclaimable* room behind its current view to satisfy the request --
/// exactly the tail-slice shape above -- it copies the view back to the true
/// start of the allocation and `capacity()` reports the real size from then
/// on. A buffer with no such hidden room (the common, honestly-sized case)
/// returns `false` at the cost of a few comparisons: no copy, no allocation,
/// no change to `capacity()`.
///
/// The probe must request more than the *currently visible* capacity can
/// already satisfy, or `try_reclaim` takes its cheap "nothing to do" path
/// without ever looking behind the view: a slice with even one spare byte
/// after it already satisfies a request for "1 more byte", so a fixed
/// `try_reclaim(1)` never reclaims (and so never sees) a hidden prefix for
/// that shape -- a *near*-tail slice, one byte narrower than the adversarial
/// exact-tail case above, sails through undetected.
///
/// `try_reclaim(additional)`'s `additional` is bytes *beyond the buffer's
/// current `len()`*, and its success test is against the true backing
/// allocation measured from its real start pointer -- not against the
/// visible capacity, and not a desired *total* capacity. Concretely (see
/// `bytes::BytesMut::reserve_inner`): it reclaims iff
/// `true_backing_capacity >= len() + additional`. Requesting
/// `slop_threshold + 1` therefore only ever reclaims when
/// `true_backing_capacity >= payload_len + slop_threshold + 1` -- with the
/// default 2x factor, that is `3 * payload_len + 1`, not the `2 *
/// payload_len` this probe is actually trying to bound. Anything backed by
/// *less* than `3 * payload_len + 1` but still more than the intended `2 *
/// payload_len` slop bound -- e.g. a tail slice backed by 2.5x its visible
/// length -- silently escapes: `try_reclaim` declines to look behind the
/// view, `capacity()` keeps reporting the small unreclaimed figure, and it
/// passes the slop check below unreclaimed and uncompacted.
///
/// The request must instead target the threshold itself: asking for
/// `slop_threshold - payload_len + 1` more than `len()` (i.e. `payload_len`,
/// with the default factor) makes the reclaim succeed exactly when
/// `true_backing_capacity > slop_threshold` -- precisely the condition the
/// slop check below needs to see. Every buffer that currently looks small
/// enough to pass is forced through a real reclaim attempt first, which
/// pulls in the true backing size whenever a hidden prefix would have
/// pushed it over the threshold. Only buffers within
/// `STREAMING_PAYLOAD_RETAIN_SLOP_FACTOR` of their visible length, by this
/// now-trustworthy accounting, keep the zero-copy path; everything else --
/// oversized unique buffers as well as shared/owner-backed values -- is
/// compacted once on this streaming-only path so accounting always matches
/// the real footprint. The ordinary actor-message queue and its hot path are
/// unchanged.
fn normalize_streaming_payload(payload: bytes::Bytes) -> (bytes::Bytes, usize) {
    let payload_len = payload.len();
    let slop_threshold = payload_len.saturating_mul(STREAMING_PAYLOAD_RETAIN_SLOP_FACTOR);
    match payload.try_into_mut() {
        Ok(mut buffer) => {
            // `try_reclaim(additional)` reclaims (moves the view back to the
            // allocation's true start) iff `true_backing_capacity >= len() +
            // additional`. Requesting merely "1 more byte" is satisfied by
            // any slice with so much as a single spare byte after it, so it
            // never examines (let alone reclaims) a hidden prefix in that
            // case -- the near-tail shape that slips past the slop check
            // below undetected. Requesting `slop_threshold + 1` (rather than
            // `slop_threshold - payload_len + 1`) makes the same mistake one
            // level up: since `additional` is counted beyond `len()`, not
            // beyond the threshold, that only reclaims once the true backing
            // capacity reaches `payload_len + slop_threshold + 1` --
            // `3 * payload_len + 1` at the default factor, not the `2 *
            // payload_len` this is meant to bound. Requesting `slop_threshold
            // - payload_len + 1` more than `len()` instead guarantees the
            // fast path is only taken when the visible capacity is *already*
            // large enough to fail the check on its own; every buffer that
            // currently looks small enough to pass is forced through the
            // real reclaim attempt, which pulls in the true backing size
            // whenever a hidden prefix would have pushed it over the
            // threshold.
            let reclaim_request = slop_threshold
                .saturating_sub(payload_len)
                .saturating_add(1);
            let _ = buffer.try_reclaim(reclaim_request);
            if buffer.capacity() <= slop_threshold {
                let retained_bytes = buffer.capacity().max(payload_len);
                (buffer.freeze(), retained_bytes)
            } else {
                let compact = bytes::Bytes::copy_from_slice(&buffer[..]);
                (compact, payload_len)
            }
        }
        Err(payload) => {
            let compact = bytes::Bytes::copy_from_slice(&payload);
            (compact, payload_len)
        }
    }
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
#[cfg_attr(not(test), allow(dead_code))]
const MAX_STREAMING_RESPONSE_RETAINED_BYTES: usize =
    crate::MAX_STREAM_SIZE.saturating_add(STREAMING_RESPONSE_QUEUE_BYTE_CAP);

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
    /// reserve. A queue-level invariant checked directly by tests; NOT used to
    /// gate the IO task's read loop (a blanket pre-check there previously
    /// stopped every read on the connection -- including ones needing no
    /// streaming-queue capacity at all -- for as long as this stayed true,
    /// which is exactly the state a bidirectional streaming storm leaves both
    /// peers in). Per-response admission is `can_admit_response`.
    #[cfg_attr(not(test), allow(dead_code))]
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

    /// A unique `Bytes` reclaimed via `try_into_mut` reports the *remaining*
    /// capacity of its backing allocation from its own start pointer, not the
    /// visible slice length. A response handler that returns a small slice of
    /// a much larger scratch buffer (e.g. a sub-slice taken at offset 0) is
    /// still the sole owner, so the old accounting counted the whole backing
    /// allocation against the admission reserve for a response that is
    /// logically tiny. That inflated count could trip `is_full`/admission
    /// checks for a payload nowhere near the real byte cap. Retained-bytes
    /// accounting must track what this response logically holds onto
    /// (`payload.len()` once compacted), not incidental backing slop.
    #[test]
    fn does_not_retain_full_backing_capacity_for_a_small_slice_of_a_large_buffer() {
        const BACKING_CAPACITY: usize = 1_000_000;
        const VISIBLE_LEN: usize = 100;

        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let small = full.slice(0..VISIBLE_LEN);
        // Drop the sibling handle so `small` is the sole owner and
        // `try_into_mut` takes the zero-copy path this test targets.
        drop(full);

        let (payload, retained_bytes) = normalize_streaming_payload(small);

        assert_eq!(payload.len(), VISIBLE_LEN);
        assert!(
            retained_bytes <= VISIBLE_LEN * 4,
            "retained_bytes must reflect the logical payload size ({VISIBLE_LEN}), not the \
             {BACKING_CAPACITY}-byte backing allocation; got {retained_bytes}"
        );
    }

    /// The test above slices at offset 0, where `BytesMut::capacity()` is
    /// honest: it reports the *whole* backing allocation, so the slop check
    /// correctly rejects it and this passes whether or not the offset is
    /// accounted for. `capacity()` only measures from a buffer's own start
    /// pointer to the end of its allocation, so it stops being honest for a
    /// slice taken from the *tail* instead: a unique 100-byte tail slice of a
    /// 1MB buffer reports `capacity() == 100` (nothing left after it), not
    /// 1MB -- passing the slop guard and staying zero-copy while pinning the
    /// entire 1MB allocation, with `retained_bytes` recording only the small
    /// figure. Repeated responses of this shape defeat the streaming queue's
    /// hard byte cap entirely. A slice from the head proves nothing about
    /// this path; this one constructs the adversarial tail shape directly.
    #[test]
    fn does_not_retain_full_backing_capacity_for_a_tail_slice_of_a_large_buffer() {
        const BACKING_CAPACITY: usize = 1_000_000;
        const VISIBLE_LEN: usize = 100;

        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let backing_start = full.as_ptr() as usize;
        let tail = full.slice(BACKING_CAPACITY - VISIBLE_LEN..);
        // Drop the sibling handle so `tail` is the sole owner and
        // `try_into_mut` takes the zero-copy path this test targets.
        drop(full);

        let (payload, retained_bytes) = normalize_streaming_payload(tail);

        assert_eq!(payload.len(), VISIBLE_LEN);

        // The bug this test targets is not a wrong *value* in `retained_bytes`
        // taken alone -- both the buggy and fixed paths can report a small
        // number here. It is a mismatch between that number and what the
        // *returned payload* actually keeps alive. A buggy path still shares
        // storage with the 1MB backing allocation (its data pointer falls
        // inside that allocation's address range); a correctly compacted
        // payload is a fresh, independent allocation and cannot land there.
        let payload_start = payload.as_ptr() as usize;
        let still_shares_the_backing_allocation =
            payload_start >= backing_start && payload_start < backing_start + BACKING_CAPACITY;
        assert!(
            !still_shares_the_backing_allocation,
            "the returned payload must not still share the {BACKING_CAPACITY}-byte backing \
             allocation with a tail slice this small -- it must be compacted into its own, \
             right-sized buffer"
        );
        assert!(
            retained_bytes <= VISIBLE_LEN * 4,
            "retained_bytes ({retained_bytes}) must reflect the logical payload size \
             ({VISIBLE_LEN}) once compacted, not the {BACKING_CAPACITY}-byte backing allocation"
        );
    }

    /// Review finding: `try_reclaim(1)` only moves the visible view back to
    /// the allocation's true start when the *currently visible* capacity
    /// cannot already satisfy the request. A slice with even one spare byte
    /// after it already satisfies "1 more byte" without reclaiming anything,
    /// so the previous test's *exact* tail slice (zero bytes after it, so
    /// `try_reclaim(1)` is forced to act) proves nothing about this shape.
    /// A unique 100-byte slice ending one byte before the end of a 1MB
    /// allocation reports `capacity() == 101` -- passes the 2x slop check
    /// against `VISIBLE_LEN == 100` -- while still pinning the entire 1MB
    /// allocation, exactly the same defeat as the exact-tail case, just one
    /// byte narrower.
    #[test]
    fn does_not_retain_full_backing_capacity_for_a_near_tail_slice_with_trailing_spare_bytes() {
        const BACKING_CAPACITY: usize = 1_000_000;
        const VISIBLE_LEN: usize = 100;
        const TRAILING_SPARE_BYTES: usize = 1;

        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let backing_start = full.as_ptr() as usize;
        let start = BACKING_CAPACITY - VISIBLE_LEN - TRAILING_SPARE_BYTES;
        let near_tail = full.slice(start..start + VISIBLE_LEN);
        // Drop the sibling handle so `near_tail` is the sole owner and
        // `try_into_mut` takes the zero-copy path this test targets.
        drop(full);

        let (payload, retained_bytes) = normalize_streaming_payload(near_tail);

        assert_eq!(payload.len(), VISIBLE_LEN);

        let payload_start = payload.as_ptr() as usize;
        let still_shares_the_backing_allocation =
            payload_start >= backing_start && payload_start < backing_start + BACKING_CAPACITY;
        assert!(
            !still_shares_the_backing_allocation,
            "the returned payload must not still share the {BACKING_CAPACITY}-byte backing \
             allocation with a near-tail slice this small (only {TRAILING_SPARE_BYTES} spare \
             byte(s) after it) -- it must be compacted into its own, right-sized buffer"
        );
        assert!(
            retained_bytes <= VISIBLE_LEN * 4,
            "retained_bytes ({retained_bytes}) must reflect the logical payload size \
             ({VISIBLE_LEN}) once compacted, not the {BACKING_CAPACITY}-byte backing allocation"
        );
    }

    /// Review finding: `try_reclaim(additional)` measures `additional` beyond
    /// `len()`, not beyond `slop_threshold`, and its success test is against
    /// the *true* backing allocation, not the visible capacity. The old
    /// `try_reclaim(slop_threshold + 1)` request therefore only ever actually
    /// reclaims once the true backing capacity reaches roughly `3 *
    /// payload_len + 1` -- every buffer backed by *less* than that but still
    /// more than the intended `2 * payload_len` slop bound escapes
    /// undetected. The three tests above all use a 1MB backing allocation
    /// against a 100-byte slice (10,000x), which clears even the buggy
    /// `3x` threshold and so cannot distinguish the bug from the fix. This
    /// one lands exactly in the gap the buggy request never exercised: a
    /// tail slice backed by 2.5x its visible length -- inside `(2x, 3x)` --
    /// must still be compacted.
    #[test]
    fn does_not_retain_full_backing_capacity_for_a_tail_slice_backed_by_between_two_and_three_times_its_length()
     {
        const VISIBLE_LEN: usize = 100_000;
        const BACKING_CAPACITY: usize = VISIBLE_LEN * 5 / 2; // 2.5x: strictly inside (2x, 3x).

        let mut buf = bytes::BytesMut::with_capacity(BACKING_CAPACITY);
        buf.extend_from_slice(&vec![0u8; BACKING_CAPACITY]);
        let full = buf.freeze();
        let backing_start = full.as_ptr() as usize;
        let tail = full.slice(BACKING_CAPACITY - VISIBLE_LEN..);
        // Drop the sibling handle so `tail` is the sole owner and
        // `try_into_mut` takes the zero-copy path this test targets.
        drop(full);

        let (payload, retained_bytes) = normalize_streaming_payload(tail);

        assert_eq!(payload.len(), VISIBLE_LEN);

        let payload_start = payload.as_ptr() as usize;
        let still_shares_the_backing_allocation =
            payload_start >= backing_start && payload_start < backing_start + BACKING_CAPACITY;
        assert!(
            !still_shares_the_backing_allocation,
            "a tail slice backed by {BACKING_CAPACITY} bytes (2.5x its {VISIBLE_LEN}-byte \
             visible length -- inside the (2x, 3x) band the old `slop_threshold + 1` request \
             never actually reclaimed) must be compacted, not retained zero-copy"
        );
        assert!(
            retained_bytes <= VISIBLE_LEN * STREAMING_PAYLOAD_RETAIN_SLOP_FACTOR,
            "retained_bytes ({retained_bytes}) must respect the \
             {STREAMING_PAYLOAD_RETAIN_SLOP_FACTOR}x accounting bound once compacted, not the \
             {BACKING_CAPACITY}-byte backing allocation"
        );
    }

    /// A unique `Bytes` whose backing allocation is already right-sized (no
    /// meaningful slop) keeps the zero-copy path: no compaction copy, and
    /// `retained_bytes` reflects the real (small) footprint.
    #[test]
    fn keeps_zero_copy_for_a_right_sized_unique_buffer() {
        const LEN: usize = 4096;
        let mut buf = bytes::BytesMut::with_capacity(LEN);
        buf.extend_from_slice(&vec![7u8; LEN]);
        let payload = buf.freeze();
        let ptr_before = payload.as_ptr();

        let (out, retained_bytes) = normalize_streaming_payload(payload);

        assert_eq!(out.len(), LEN);
        assert_eq!(retained_bytes, LEN);
        assert_eq!(
            out.as_ptr(),
            ptr_before,
            "a right-sized unique buffer must stay zero-copy"
        );
    }
}
