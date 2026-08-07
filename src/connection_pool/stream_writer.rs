/// Maximum streaming bytes offered before the connection owner returns to its
/// read side. Ordinary tell/ask commands never use this slice machinery.
const STREAM_WRITE_SLICE_BYTES: usize = 64 * 1024;

async fn write_vectored_all<S>(
    stream: &mut S,
    slices: &[std::io::IoSlice<'_>],
) -> std::io::Result<usize>
where
    S: AsyncWrite + Unpin,
{
    let total_len: usize = slices.iter().map(|slice| slice.len()).sum();
    if total_len == 0 {
        return Ok(0);
    }
    if !stream.is_write_vectored() {
        for slice in slices {
            stream.write_all(slice.as_ref()).await?;
        }
        return Ok(total_len);
    }

    let written = stream.write_vectored(slices).await?;
    if written == total_len {
        return Ok(written);
    }
    let mut index = 0usize;
    let mut offset = written;
    while index < slices.len() && offset >= slices[index].len() {
        offset -= slices[index].len();
        index += 1;
    }
    if index < slices.len() {
        if offset < slices[index].len() {
            stream.write_all(&slices[index].as_ref()[offset..]).await?;
            index += 1;
        }
        while index < slices.len() {
            stream.write_all(slices[index].as_ref()).await?;
            index += 1;
        }
    }
    Ok(total_len)
}

/// Perform at most one socket write for a streaming slice. A short write is
/// returned to the caller, which retains the command offset for the next turn;
/// this avoids awaiting the remainder of a large frame while the peer's read
/// side is constrained.
async fn write_vectored_once<S>(
    stream: &mut S,
    slices: &[std::io::IoSlice<'_>],
) -> std::io::Result<usize>
where
    S: AsyncWrite + Unpin,
{
    let Some(first) = slices.iter().find(|slice| !slice.is_empty()) else {
        return Ok(0);
    };
    let result = if stream.is_write_vectored() {
        stream.write_vectored(slices).await
    } else {
        std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, first.as_ref())).await
    }?;
    if result == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "streaming write made no progress",
        ));
    }
    Ok(result)
}

/// Bound on a single attempt to write an ask-backpressure NACK header (see
/// `write_ask_nack_header_bounded`). The `io_task` read loop that calls
/// this owns the connection's socket read side too, so a write that parks
/// indefinitely on a peer that has stopped draining would park reads with
/// it -- exactly the shape of deadlock `icanact-remote#186` closes for the
/// streaming slice writer. A NACK is best-effort, not a delivery guarantee,
/// so unlike a streaming response frame it can simply be abandoned once an
/// attempt makes zero progress rather than retried forever.
const STREAM_WRITE_SLICE_TIMEOUT: Duration = Duration::from_millis(250);

/// See `STREAM_WRITE_SLICE_TIMEOUT`. Only reached once a NACK write has
/// already committed some bytes to the wire (so it can no longer be
/// abandoned without corrupting later frames on this connection) and then
/// stalls -- the backstop for a peer that is truly gone, not merely slow.
const STREAM_WRITE_STUCK_TEARDOWN: Duration = Duration::from_secs(30);

/// Write one already-built ask-NACK header without risking parking the
/// caller's read loop on a peer that has stopped draining. A plain
/// `write_all` is the wrong shape for this: it loops over as many
/// `poll_write` calls as it takes and is therefore unsafe to cancel -- a
/// timeout firing between two of those internal polls would abandon a
/// *partially written* frame, corrupting wire framing for every later frame
/// on this connection (this is exactly the mistake the read path's earlier,
/// now-deleted `write_ask_nack_direct` made -- see the review history on
/// `icanact-remote#186`). `write_vectored_once` instead performs
/// exactly one `poll_write` cycle per call, so each attempt here is
/// individually safe to bound with `STREAM_WRITE_SLICE_TIMEOUT` -- per the
/// `AsyncWrite` contract, a `Pending` result never writes a partial byte, so
/// a timeout before any byte of the header has gone out (`offset == 0`) can
/// always be abandoned cleanly: nothing was committed to the wire, and the
/// peer simply times out on this ask instead of getting a fast NACK. Once
/// any byte of the header *has* gone out, the frame is underway and can no
/// longer be abandoned safely, so from that point this retries with the same
/// per-attempt bound until either it completes or `STREAM_WRITE_STUCK_TEARDOWN`
/// elapses with zero further progress, at which point the caller tears the
/// connection down rather than leaving it wedged mid-frame forever.
///
/// Does **not** decide *when* it is safe to call: that is
/// `drain_pending_ask_nacks`'s job (only ever called while
/// `pending_stream_cmd.is_none()`, so no partial streaming frame owns the
/// wire). Writing here unconditionally would let this NACK's bytes splice
/// into an in-progress frame's payload and desynchronize every frame after
/// it -- the same class of bug #183 fixed for `WritePayload::Buf`.
///
/// Returns `Ok(true)` if the NACK was written, `Ok(false)` if it was
/// abandoned cleanly before committing any bytes (the caller should keep
/// reading either way -- an ask NACK is best-effort, not a delivery
/// guarantee), or `Err` on a real write error or a stuck mid-frame write
/// (the caller should tear the connection down, same as any other write
/// failure in this file).
async fn write_ask_nack_header_bounded<S>(
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    header: [u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN],
) -> std::io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let mut offset = 0usize;
    let mut stuck_since: Option<Instant> = None;
    loop {
        match tokio::time::timeout(
            STREAM_WRITE_SLICE_TIMEOUT,
            write_vectored_once(stream, &[std::io::IoSlice::new(&header[offset..])]),
        )
        .await
        {
            // `write_vectored_once` already folds a real zero-byte write
            // into `Err(WriteZero)` before returning, so this arm is
            // unreachable through that call path today. Checked locally
            // anyway rather than relying on that callee detail: `offset`
            // is nonempty-until-completion here, so `n == 0` taking this
            // branch would add nothing, never reach `offset >=
            // header.len()`, and reset `stuck_since` -- clearing the one
            // signal that would otherwise let the stuck-mid-frame teardown
            // timer above ever fire, letting a conforming `AsyncWrite`
            // that starts returning `Ok(0)` pin this loop in a CPU-burning
            // spin with no `Pending` and no timeout in between. Same class
            // of bug R4 fixed for `OwnedChunks`' raw unwrapped-write tail.
            Ok(Ok(0)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "ask NACK write made no progress",
                ));
            }
            Ok(Ok(n)) => {
                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                *bytes_since_flush += n;
                offset += n;
                if offset >= header.len() {
                    return Ok(true);
                }
                stuck_since = None;
            }
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => {
                if offset == 0 {
                    return Ok(false);
                }
                let wedged_since = *stuck_since.get_or_insert_with(Instant::now);
                if wedged_since.elapsed() >= STREAM_WRITE_STUCK_TEARDOWN {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "ask backpressure NACK write stuck mid-frame",
                    ));
                }
            }
        }
    }
}

/// Drain `LocalStreamingQueue`'s queued backpressure NACKs onto the wire.
/// **Callers must only invoke this while `pending_stream_cmd.is_none()`** --
/// see `write_ask_nack_header_bounded` and `LocalStreamingQueue::queue_ask_nack`
/// for why writing during a partial frame would corrupt it. Bounded to a
/// small burst per call (`MAX_PER_TURN`) so a backlog of queued NACKs cannot
/// starve the read loop's return to reading; the rest wait for a later call.
/// Stops (without error) at the first attempt that makes zero progress --
/// likely the socket itself has no room right now, so further attempts this
/// turn would just burn through more `STREAM_WRITE_SLICE_TIMEOUT` waits for
/// the same result. That specific queued NACK is lost (best-effort, not a
/// delivery guarantee, per `queue_ask_nack`); the rest stay queued for next
/// time.
///
/// Returns `Ok(true)` if any NACK remains queued once this call returns --
/// either the `MAX_PER_TURN` cap was hit with the queue still non-empty, or
/// the zero-progress stop left entries behind it. The caller must treat that
/// as outstanding work (e.g. `did_work = true`) rather than letting the turn
/// look idle: `pending_ask_nacks` is not visible to `has_pending` or the
/// pre-park checks, so nothing else would prevent the I/O task from parking
/// with a NACK still owed to a peer.
async fn drain_pending_ask_nacks<S>(
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    local_streaming_queue: &mut LocalStreamingQueue,
) -> std::io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    const MAX_PER_TURN: usize = 8;
    for _ in 0..MAX_PER_TURN {
        let Some(header) = local_streaming_queue.pop_ask_nack() else {
            return Ok(false);
        };
        if !write_ask_nack_header_bounded(stream, bytes_written_counter, bytes_since_flush, header)
            .await?
        {
            return Ok(local_streaming_queue.has_pending_ask_nacks());
        }
    }
    Ok(local_streaming_queue.has_pending_ask_nacks())
}

/// Write one bounded slice of a lazily framed `Bytes` response. Returning a
/// frame-boundary yield lets the scheduler interleave another streaming source
/// without materializing the remaining response into per-frame commands.
async fn write_bytes_streaming_command_slice<S>(
    stream: &mut S,
    pending_offset: &mut usize,
    response: &mut BytesStreamingResponse,
) -> std::io::Result<(usize, bool, bool)>
where
    S: AsyncWrite + Unpin,
{
    while response.frame_index < response.chunk_count {
        let header = response.frame_header(response.frame_index);
        let header_bytes = header.as_slice();
        let frame_payload_len = response.frame_payload_len(response.frame_index);
        let frame_total_len = header_bytes.len() + frame_payload_len;

        if response.frame_offset >= frame_total_len {
            response.frame_index += 1;
            response.frame_offset = 0;
            continue;
        }

        let budget = STREAM_WRITE_SLICE_BYTES.min(frame_total_len - response.frame_offset);
        let header_bytes_offered = if response.frame_offset < header_bytes.len() {
            (header_bytes.len() - response.frame_offset).min(budget)
        } else {
            0
        };
        let data_budget = budget - header_bytes_offered;
        let mut slices = [std::io::IoSlice::new(&[]); 2];
        let mut slice_count = 0usize;

        if header_bytes_offered > 0 {
            let header_start = response.frame_offset;
            slices[slice_count] = std::io::IoSlice::new(
                &header_bytes[header_start..header_start + header_bytes_offered],
            );
            slice_count += 1;
        }
        if data_budget > 0 {
            let frame_payload_start = if response.frame_index == 0 {
                0
            } else {
                response
                    .chunk_size
                    .saturating_add(
                        response
                            .frame_index
                            .saturating_sub(1)
                            .saturating_mul(response.chunk_size),
                    )
            };
            let payload_offset = frame_payload_start
                .saturating_add(response.frame_offset.saturating_sub(header_bytes.len()));
            slices[slice_count] = std::io::IoSlice::new(
                &response.payload[payload_offset..payload_offset + data_budget],
            );
            slice_count += 1;
        }

        let written = write_vectored_once(stream, &slices[..slice_count]).await?;
        response.frame_offset += written;
        *pending_offset += written;
        let frame_complete = response.frame_offset >= frame_total_len;
        if frame_complete {
            response.frame_index += 1;
            response.frame_offset = 0;
        }
        let complete = response.frame_index >= response.chunk_count;
        // Keep each frame contiguous on the wire, then hand the remaining
        // Bytes response back to the source alternator. This lets normal
        // writes and inbound response batches make progress between frames
        // without materializing the remaining response into queue commands.
        return Ok((written, complete, frame_complete && !complete));
    }

    Ok((0, true, false))
}

/// Write one bounded slice of a pooled response without converting its
/// payload into `Bytes`. The pooled allocation remains owned by the pending
/// command until the terminal frame has been written.
async fn write_pooled_streaming_command_slice<S>(
    stream: &mut S,
    pending_offset: &mut usize,
    response: &mut PooledStreamingResponse,
) -> std::io::Result<(usize, bool, bool)>
where
    S: AsyncWrite + Unpin,
{
    while response.frame_index < response.chunk_count {
        let header = response.frame_header(response.frame_index);
        let header_bytes = header.as_slice();
        let frame_payload_len = response.frame_payload_len(response.frame_index);
        let frame_total_len = header_bytes.len() + frame_payload_len;

        if response.frame_offset >= frame_total_len {
            response.frame_index += 1;
            response.frame_offset = 0;
            continue;
        }

        let budget = STREAM_WRITE_SLICE_BYTES.min(frame_total_len - response.frame_offset);
        let header_bytes_offered = if response.frame_offset < header_bytes.len() {
            (header_bytes.len() - response.frame_offset).min(budget)
        } else {
            0
        };
        let data_budget = budget - header_bytes_offered;
        let mut slices = [std::io::IoSlice::new(&[]); 3];
        let mut slice_count = 0usize;

        if header_bytes_offered > 0 {
            let header_start = response.frame_offset;
            slices[slice_count] = std::io::IoSlice::new(
                &header_bytes[header_start..header_start + header_bytes_offered],
            );
            slice_count += 1;
        }

        if data_budget > 0 {
            let mut remaining = data_budget;

            if response.prefix_sent < response.prefix_len {
                let prefix = response.prefix.as_ref().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "pooled response prefix state is inconsistent",
                    )
                })?;
                let prefix_start = response.prefix_sent;
                let prefix_available = response.prefix_len.saturating_sub(prefix_start);
                let take = remaining.min(prefix_available);
                if take > 0 {
                    slices[slice_count] = std::io::IoSlice::new(
                        &prefix[prefix_start..prefix_start + take],
                    );
                    slice_count += 1;
                    remaining -= take;
                }
            }

            if remaining > 0 {
                if response.payload_remaining == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "pooled response payload ended before the advertised length",
                    ));
                }
                let chunk = response.payload.chunk();
                let take = remaining
                    .min(response.payload_remaining)
                    .min(chunk.len());
                if take == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "pooled response returned an empty chunk while bytes remain",
                    ));
                }
                slices[slice_count] = std::io::IoSlice::new(&chunk[..take]);
                slice_count += 1;
            }
        }

        let written = write_vectored_once(stream, &slices[..slice_count]).await?;
        let data_written = written.saturating_sub(header_bytes_offered);
        let prefix_written = data_written
            .min(response.prefix_len.saturating_sub(response.prefix_sent));
        response.prefix_sent = response.prefix_sent.saturating_add(prefix_written);
        let payload_written = data_written.saturating_sub(prefix_written);
        if payload_written > response.payload_remaining {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "pooled response wrote past its advertised payload length",
            ));
        }
        if payload_written > 0 {
            response.payload.advance(payload_written);
            response.payload_remaining -= payload_written;
        }

        response.frame_offset += written;
        *pending_offset += written;
        let frame_complete = response.frame_offset >= frame_total_len;
        if frame_complete {
            response.frame_index += 1;
            response.frame_offset = 0;
        }
        let complete = response.frame_index >= response.chunk_count;
        if complete
            && (response.prefix_sent != response.prefix_len || response.payload_remaining != 0)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "pooled response stream completed before all payload bytes were written",
            ));
        }
        return Ok((written, complete, frame_complete && !complete));
    }

    Ok((0, true, false))
}

async fn write_streaming_command_slice<S>(
    stream: &mut S,
    pending: &mut PendingStreamingCommand,
) -> std::io::Result<(usize, bool)>
where
    S: AsyncWrite + Unpin,
{
    pending.yield_after_frame = false;
    if let StreamingCommand::BytesResponse(response) = &mut pending.command {
        let (written, complete, yield_after_frame) =
            write_bytes_streaming_command_slice(stream, &mut pending.offset, response).await?;
        pending.yield_after_frame = yield_after_frame;
        return Ok((written, complete));
    }
    if let StreamingCommand::PooledResponse(response) = &mut pending.command {
        let (written, complete, yield_after_frame) = write_pooled_streaming_command_slice(
            stream,
            &mut pending.offset,
            response,
        )
        .await?;
        pending.yield_after_frame = yield_after_frame;
        return Ok((written, complete));
    }

    let offset = pending.offset;
    let (written, total_len) = match &pending.command {
        StreamingCommand::WriteBytes(data) => {
            if offset >= data.len() {
                return Ok((0, true));
            }
            let end = (offset + STREAM_WRITE_SLICE_BYTES).min(data.len());
            let written = write_vectored_once(
                stream,
                &[std::io::IoSlice::new(&data[offset..end])],
            )
            .await?;
            (written, data.len())
        }
        StreamingCommand::Flush => {
            stream.flush().await?;
            return Ok((0, true));
        }
        StreamingCommand::Abort { stream_id, reason } => {
            let header = crate::framing::write_stream_abort_header(*stream_id, *reason);
            if offset >= header.len() {
                return Ok((0, true));
            }
            let end = (offset + STREAM_WRITE_SLICE_BYTES).min(header.len());
            let written = write_vectored_once(
                stream,
                &[std::io::IoSlice::new(&header[offset..end])],
            )
            .await?;
            (written, header.len())
        }
        StreamingCommand::VectoredWrite(command) => {
            let header = command.header.as_slice();
            let total_len = header.len() + command.payload.len();
            if offset >= total_len {
                return Ok((0, true));
            }
            let budget = STREAM_WRITE_SLICE_BYTES.min(total_len - offset);
            let mut slices = [std::io::IoSlice::new(&[]), std::io::IoSlice::new(&[])];
            let slice_count;
            if offset < header.len() {
                let header_end = (offset + budget).min(header.len());
                slices[0] = std::io::IoSlice::new(&header[offset..header_end]);
                let header_bytes = header_end - offset;
                if header_bytes < budget {
                    slices[1] =
                        std::io::IoSlice::new(&command.payload[..budget - header_bytes]);
                    slice_count = 2;
                } else {
                    slice_count = 1;
                }
            } else {
                let payload_offset = offset - header.len();
                slices[0] = std::io::IoSlice::new(
                    &command.payload[payload_offset..payload_offset + budget],
                );
                slice_count = 1;
            }
            (
                write_vectored_once(stream, &slices[..slice_count]).await?,
                total_len,
            )
        }
        StreamingCommand::OwnedChunks(chunks) => {
            let total_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
            if offset >= total_len {
                return Ok((0, true));
            }
            let mut skip = offset;
            let mut remaining = STREAM_WRITE_SLICE_BYTES.min(total_len - offset);
            const MAX_IOV: usize = 64;
            let mut storage: [MaybeUninit<std::io::IoSlice<'_>>; MAX_IOV] = unsafe {
                MaybeUninit::<[MaybeUninit<std::io::IoSlice<'_>>; MAX_IOV]>::uninit()
                    .assume_init()
            };
            let mut count = 0usize;
            for chunk in chunks {
                if skip >= chunk.len() {
                    skip -= chunk.len();
                    continue;
                }
                let start = skip;
                skip = 0;
                let take = remaining.min(chunk.len() - start);
                storage[count].write(std::io::IoSlice::new(&chunk[start..start + take]));
                count += 1;
                remaining -= take;
                if remaining == 0 || count == MAX_IOV {
                    break;
                }
            }
            let slices = unsafe {
                std::slice::from_raw_parts(
                    storage.as_ptr() as *const std::io::IoSlice<'_>,
                    count,
                )
            };
            let written = write_vectored_once(stream, slices).await?;
            (written, total_len)
        }
        StreamingCommand::PooledResponse(_) => unreachable!(
            "pooled responses are handled by write_pooled_streaming_command_slice"
        ),
        StreamingCommand::BytesResponse(_) => unreachable!(
            "bytes responses are handled by write_bytes_streaming_command_slice"
        ),
    };
    pending.offset += written;
    Ok((written, pending.offset == total_len))
}

#[inline]
fn finish_streaming_command_slice(
    pending: PendingStreamingCommand,
    complete: bool,
    streaming_queue: &StreamingQueue,
    yielded_slot: &mut Option<PendingStreamingCommand>,
    pending_slot: &mut Option<PendingStreamingCommand>,
) {
    if complete {
        if pending.from_shared_queue {
            streaming_queue.notify_space();
        }
    } else if pending.yield_after_frame && !pending.from_shared_queue {
        *yielded_slot = Some(pending);
    } else {
        *pending_slot = Some(pending);
    }
}

#[inline]
fn should_flush_stream_output(
    bytes_since_flush: usize,
    pending_stream_cmd: Option<&PendingStreamingCommand>,
    yielded_stream_cmd: Option<&PendingStreamingCommand>,
) -> bool {
    bytes_since_flush > 0 && pending_stream_cmd.is_none() && yielded_stream_cmd.is_none()
}

#[inline]
fn is_streaming_admission_backpressure(error: &crate::GossipError) -> bool {
    matches!(
        error,
        crate::GossipError::Network(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
    )
}

/// Truly lock-free streaming handle with dedicated background writer
#[derive(Clone)]
pub struct LockFreeStreamHandle {
    /// Unique per-handle id used to ignore disconnect callbacks from stale connections.
    instance_id: u64,
    addr: SocketAddr,
    channel_id: ChannelId,
    sequence_counter: Arc<AtomicUsize>,
    /// Direction-local stream IDs. IDs never wrap on a live connection.
    next_stream_id: Arc<AtomicU32>,
    bytes_written: Arc<AtomicUsize>, // This tracks actual TCP bytes written
    shutdown_signal: Arc<AtomicBool>,
    exit_flag: Arc<AtomicBool>,
    exit_notify: Arc<Notify>,
    /// Set by a caller that is authoritatively retiring THIS instance while
    /// its `correlation` tracker is still shared with a live sibling
    /// instance for the same peer (see
    /// `LockFreeConnection::abort_tasks_keep_correlation`). The IO task's
    /// `ExitGuard` checks this before its own peer/addr-based supersession
    /// inference, which can otherwise lag the caller's already-published
    /// decision by a check-then-act window.
    known_superseded: Arc<AtomicBool>,
    flush_pending: Arc<AtomicBool>,
    /// Atomic flag for coordinating streaming mode. Read by the IO task and
    /// `is_streaming_active` as a cheap observability signal; the actual mutual
    /// exclusion is enforced by `stream_gate`. True while a stream's frames
    /// are being queued onto the wire (gate held); an ask that has finished
    /// queueing its frames releases the gate and clears this flag *before*
    /// awaiting its response (T3, 2026-07-17 QA).
    streaming_active: Arc<AtomicBool>,
    /// ACTOR_REM_2 R16e: single-permit gate serializing `stream_large_message` /
    /// `stream_response` on this handle. Losers park on the permit instead of
    /// busy-spinning a failed CAS + `yield_now` for the winner's entire stream
    /// duration (which starved the loser and burned a scheduler slot per poll).
    stream_gate: Arc<tokio::sync::Semaphore>,
    /// Outbound route bindings for this exact transport instance.  The gate
    /// keeps a newly enqueued RouteBind immediately ahead of its first ask.
    outbound_routes: Arc<crate::route_interning::RouteTable>,
    route_bind_gate: Arc<tokio::sync::Mutex<()>>,
    /// Lock-free write queue for payload writes
    write_queue: Arc<WriteQueue>,
    /// Reserved lock-free queue for latency-critical control replies. This is
    /// intentionally independent from `write_queue`: ordinary traffic must
    /// not consume the admission capacity needed for a control-plane reply.
    immediate_write_queue: Arc<WriteQueue>,
    /// Bounded streaming command queue for background task.
    streaming_queue: Arc<StreamingQueue>,
    /// Buffer configuration that determines sizes and thresholds
    buffer_config: BufferConfig,
    /// Max allowed frame payload size (msg_len, excluding 4-byte length prefix).
    ///
    /// This comes from the registry config and is used to bound streaming chunk sizes so
    /// streaming frames themselves never exceed the reader limit.
    max_message_size: usize,
    /// Optional schema/version hash for protocol guardrails.
    schema_hash: Option<u64>,
    /// Gates `write_routed_actor_ask` (and so `RouteBind`) behind a fresh
    /// outbound connection's own identifying `FullSync` being enqueued
    /// first. `true` (not gated) for every handle by default; only
    /// `finalize_new_outbound_connection` ever arms this via
    /// `begin_identify_gate`, immediately after construction and before
    /// the handle is shared with anything that could enqueue onto it.
    identify_ready: Arc<AtomicBool>,
}

impl LockFreeStreamHandle {
    fn next_instance_id() -> u64 {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// Arm the identify gate: `write_routed_actor_ask` on this handle will
    /// park in [`Self::wait_until_identified`] until [`Self::mark_identified`]
    /// is called. Only ever called by `finalize_new_outbound_connection`,
    /// immediately after construction and before this handle is shared with
    /// anything that could enqueue onto it -- so there is no window in
    /// which a caller could observe the default (not gated) state and race
    /// ahead of the arm.
    pub(crate) fn begin_identify_gate(&self) {
        self.identify_ready.store(false, Ordering::Release);
    }

    /// Signal that this connection's identifying frame has been enqueued.
    /// Wakes any `write_routed_actor_ask` parked in
    /// [`Self::wait_until_identified`] so it can now enqueue behind it.
    pub(crate) fn mark_identified(&self) {
        self.identify_ready.store(true, Ordering::Release);
        self.exit_notify.notify_waiters();
    }

    /// Park until this handle is either identified or dead. A freshly
    /// gated outbound connection's `write_routed_actor_ask` -- and so its
    /// `RouteBind` -- must never reach the write queue ahead of the
    /// identifying frame; this is the wait side of that guarantee, keyed on
    /// the exact same `exit_notify` every teardown path (`abort_tasks`,
    /// the IO task's own `ExitGuard`) already wakes on, so a connection
    /// that fails or tears down before ever being identified wakes any
    /// waiter with an error instead of hanging it forever. Returns
    /// immediately for every handle except one gated by
    /// [`Self::begin_identify_gate`] that has not yet been identified.
    async fn wait_until_identified(&self) -> Result<()> {
        loop {
            // Subscribe before checking: a `notify_waiters()` that lands
            // between the check and a bare `.await` would otherwise be
            // missed. `Notified` created here observes any notification
            // from this point on, even one delivered before it is polled.
            let notified = self.exit_notify.notified();
            if self.identify_ready.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.exit_flag.load(Ordering::Acquire) {
                return Err(GossipError::ConnectionClosed(self.addr));
            }
            notified.await;
        }
    }

    fn allocate_stream_id(&self) -> Result<u32> {
        // R-5: per-handle stream ids take the ODD partition (1, 3, 5, ...) —
        // disjoint from the process-global direct-response allocator's EVEN
        // ids — so a direct streaming response and a handle-initiated stream on
        // the same connection can never collide on `stream_id` (a collision
        // keys two live streams to one id and tears the connection down as a
        // duplicate start).
        //
        // A CAS loop (not fetch_add) so exhaustion shuts the connection down at
        // the u32::MAX sentinel WITHOUT wrapping the counter back to 1 — a wrap
        // would hand out id 1 again on a still-live connection (stream-id
        // reuse). u32::MAX is the odd sentinel (never handed out); the last id
        // returned is u32::MAX - 2. Each odd id is handed out at most once.
        loop {
            let current = self.next_stream_id.load(Ordering::Acquire);
            if current == u32::MAX {
                self.signal_shutdown();
                return Err(GossipError::Shutdown);
            }
            // current is odd and in [1, u32::MAX - 2]; +2 stays odd and <= u32::MAX.
            let next = current + 2;
            if self
                .next_stream_id
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(current);
            }
        }
    }

    /// Create a new lock-free streaming handle with background writer task
    ///
    /// Returns the handle, the spawned IO task, and an optional reader task.
    pub fn new<S>(
        stream: S,
        addr: SocketAddr,
        channel_id: ChannelId,
        buffer_config: BufferConfig,
        schema_hash: Option<u64>,
        read_context: Option<ReadContext>,
    ) -> (Self, JoinHandle<()>, Option<JoinHandle<()>>)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let instance_id = Self::next_instance_id();
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let streaming_active = Arc::new(AtomicBool::new(false));
        let stream_gate = Arc::new(tokio::sync::Semaphore::new(1));
        let outbound_routes = Arc::new(crate::route_interning::RouteTable::new());
        let route_bind_gate = Arc::new(tokio::sync::Mutex::new(()));
        let flush_pending = Arc::new(AtomicBool::new(false));
        let exit_flag = Arc::new(AtomicBool::new(false));
        let exit_notify = Arc::new(Notify::new());
        let known_superseded = Arc::new(AtomicBool::new(false));

        // Create shared counter for actual TCP bytes written
        let bytes_written = Arc::new(AtomicUsize::new(0));

        // Keep immediate control replies in a separate bounded queue. Normal
        // traffic can saturate its own queue without rejecting a critical reply.
        let write_queue = WriteQueue::new(buffer_config.write_queue_capacity(), addr);
        let immediate_write_queue = WriteQueue::new(128, addr);
        let streaming_queue = StreamingQueue::new(buffer_config.write_queue_capacity(), addr);

        let max_message_size = read_context
            .as_ref()
            .map(|ctx| ctx.max_message_size)
            .unwrap_or(MASTER_BUFFER_SIZE);
        // Spawn background writer task with exclusive TCP access - NO MUTEX!
        let writer_handle = {
            let shutdown_signal = shutdown_signal.clone();
            let bytes_written_for_task = bytes_written.clone();
            let streaming_active_for_task = streaming_active.clone();
            let flush_pending_for_task = flush_pending.clone();
            let exit_flag_for_task = exit_flag.clone();
            let exit_notify_for_task = exit_notify.clone();
            let known_superseded_for_task = known_superseded.clone();
            let writer_addr = addr;
            let writer_channel_id = channel_id;
            let write_queue = write_queue.clone();
            let immediate_write_queue = immediate_write_queue.clone();
            let streaming_queue = streaming_queue.clone();

            tokio::spawn(async move {
                info!(
                    addr = %writer_addr,
                    channel_id = ?writer_channel_id,
                    "🚀 Background writer task started"
                );
                Self::io_task(
                    stream,
                    shutdown_signal,
                    bytes_written_for_task,
                    flush_pending_for_task,
                    streaming_active_for_task,
                    write_queue,
                    immediate_write_queue,
                    streaming_queue,
                    read_context,
                    instance_id,
                    exit_flag_for_task,
                    exit_notify_for_task,
                    known_superseded_for_task,
                )
                .await;
                // CRITICAL: Log when writer exits - this helps diagnose silent writer deaths
                warn!(
                    addr = %writer_addr,
                    channel_id = ?writer_channel_id,
                    "⚠️ Background writer task EXITED - no more writes possible on this connection!"
                );
            })
        };

        (
            Self {
                instance_id,
                addr,
                channel_id,
                sequence_counter: Arc::new(AtomicUsize::new(0)),
                next_stream_id: Arc::new(AtomicU32::new(1)),
                bytes_written, // This now tracks actual TCP bytes written
                shutdown_signal,
                flush_pending,
                exit_flag,
                exit_notify,
                known_superseded,
                streaming_active,
                stream_gate,
                outbound_routes,
                route_bind_gate,
                write_queue,
                immediate_write_queue,
                streaming_queue,
                buffer_config,
                max_message_size,
                schema_hash,
                identify_ready: Arc::new(AtomicBool::new(true)),
            },
            writer_handle,
            None,
        )
    }

    /// Single IO task - owns the TLS stream for both read and write.
    /// OPTIMIZED FOR MAXIMUM THROUGHPUT - NO MUTEX NEEDED!
    #[allow(clippy::too_many_arguments)]
    async fn io_task<S>(
        stream: S,
        shutdown_signal: Arc<AtomicBool>,
        bytes_written_counter: Arc<AtomicUsize>, // Track ALL bytes written to TCP
        flush_pending: Arc<AtomicBool>,
        streaming_active: Arc<AtomicBool>,
        write_queue: Arc<WriteQueue>,
        immediate_write_queue: Arc<WriteQueue>,
        streaming_queue: Arc<StreamingQueue>,
        read_context: Option<ReadContext>,
        instance_id: u64,
        exit_flag: Arc<AtomicBool>,
        exit_notify: Arc<Notify>,
        known_superseded: Arc<AtomicBool>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // CRITICAL_PATH: owner-batched send queue + vectored TLS writes.
        use std::io::IoSlice;

        async fn flush_inline32_batch<S>(
            stream: &mut S,
            headers: &mut Vec<[u8; 32]>,
            payloads: &mut Vec<bytes::Bytes>,
        ) -> std::io::Result<usize>
        where
            S: AsyncWrite + Unpin,
        {
            if headers.is_empty() {
                return Ok(0);
            }

            let mut total_len = 0usize;
            const MAX_IOV: usize = OWNER_BATCH_SIZE * 2;
            let mut storage: [MaybeUninit<IoSlice<'_>>; MAX_IOV] = unsafe {
                MaybeUninit::<[MaybeUninit<IoSlice<'_>>; MAX_IOV]>::uninit().assume_init()
            };

            let count = headers.len().min(payloads.len());
            for idx in 0..count {
                storage[idx * 2].write(IoSlice::new(&headers[idx]));
                storage[idx * 2 + 1].write(IoSlice::new(&payloads[idx]));
                total_len += headers[idx].len() + payloads[idx].len();
            }

            let slices = unsafe {
                std::slice::from_raw_parts(storage.as_ptr() as *const IoSlice<'_>, count * 2)
            };
            let written = write_vectored_all(stream, slices).await?;
            headers.clear();
            payloads.clear();
            debug_assert_eq!(written, total_len);
            Ok(written)
        }

        async fn flush_direct_ask_batch<S>(
            stream: &mut S,
            headers: &mut Vec<[u8; 16]>,
            payloads: &mut Vec<bytes::Bytes>,
        ) -> std::io::Result<usize>
        where
            S: AsyncWrite + Unpin,
        {
            if headers.is_empty() {
                return Ok(0);
            }

            let mut total_len = 0usize;
            const MAX_IOV: usize = OWNER_BATCH_SIZE * 2;
            let mut storage: [MaybeUninit<IoSlice<'_>>; MAX_IOV] = unsafe {
                MaybeUninit::<[MaybeUninit<IoSlice<'_>>; MAX_IOV]>::uninit().assume_init()
            };

            let count = headers.len().min(payloads.len());
            for idx in 0..count {
                storage[idx * 2].write(IoSlice::new(&headers[idx]));
                storage[idx * 2 + 1].write(IoSlice::new(&payloads[idx]));
                total_len += headers[idx].len() + payloads[idx].len();
            }

            let slices = unsafe {
                std::slice::from_raw_parts(storage.as_ptr() as *const IoSlice<'_>, count * 2)
            };
            let written = write_vectored_all(stream, slices).await?;
            headers.clear();
            payloads.clear();
            debug_assert_eq!(written, total_len);
            Ok(written)
        }

        fn read_batch_limit_for(result: &ReadIoResult) -> usize {
            match result {
                ReadIoResult::DirectAsk { .. } => ASK_READ_BATCH_LIMIT,
                ReadIoResult::ActorAsk { .. } => ASK_READ_BATCH_LIMIT,
                ReadIoResult::Generic(crate::handle::MessageReadResult::Actor {
                    msg_type, ..
                }) if *msg_type == crate::MessageType::ActorAsk as u8 => ASK_READ_BATCH_LIMIT,
                ReadIoResult::Generic(crate::handle::MessageReadResult::DirectAsk { .. })
                | ReadIoResult::Generic(crate::handle::MessageReadResult::DirectResponse {
                    ..
                })
                | ReadIoResult::Generic(crate::handle::MessageReadResult::Response { .. }) => {
                    ASK_READ_BATCH_LIMIT
                }
                _ => READ_BATCH_LIMIT,
            }
        }

        struct ExitGuard {
            flag: Arc<AtomicBool>,
            notify: Arc<Notify>,
            // Writer-owned queues. On teardown we must wake any sender parked in
            // `push()` on a full queue, otherwise it hangs forever (the queue's
            // space notifier is only fired by `pop()`, which has stopped).
            write_queue: Arc<WriteQueue>,
            immediate_write_queue: Arc<WriteQueue>,
            streaming_queue: Arc<StreamingQueue>,
            response_correlation: Option<Arc<CorrelationTracker>>,
            registry_weak: Option<std::sync::Weak<GossipRegistry>>,
            peer_addr: Option<SocketAddr>,
            peer_id: Option<crate::PeerId>,
            session_source: Option<SocketAddr>,
            instance_id: u64,
            known_superseded: Arc<AtomicBool>,
        }

        impl Drop for ExitGuard {
            fn drop(&mut self) {
                self.flag.store(true, Ordering::Release);
                self.notify.notify_waiters();
                // Wake parked `push()` callers so they observe `closed` and
                // return `ConnectionClosed` rather than hanging on a full queue.
                self.write_queue.mark_closed_and_wake();
                self.immediate_write_queue.mark_closed_and_wake();
                self.streaming_queue.mark_closed_and_wake();
                // Authoritative signal from a caller that is deliberately
                // retiring this exact instance while its correlation tracker
                // is still shared with a live sibling (e.g.
                // `retire_displaced_expected`/`unpublish_rejected_outbound_candidate`
                // via `abort_tasks_keep_correlation`). Trust it ahead of the
                // peer/addr-based inference below, which reads pool state
                // this same caller may have already mutated (or not yet
                // re-published) by the time this Drop runs.
                let mut superseded = self.known_superseded.load(Ordering::Acquire);
                let mut should_cancel_pending = !superseded;
                if let (Some(registry_weak), Some(peer_addr)) =
                    (self.registry_weak.as_ref(), self.peer_addr)
                    && let Some(registry) = registry_weak.upgrade()
                {
                    // Guard against mis-attributing disconnects from stale/duplicate
                    // connections. Tie-breaker drops and replacements are expected and
                    // must not mark the peer failed or cancel pending asks on the current link.
                    let expected_instance = self.instance_id;
                    let peer_id_hint = self.peer_id.clone();
                    let pool = &registry.connection_pool;
                    let peer_id = peer_id_hint.or_else(|| pool.get_peer_id_by_addr(&peer_addr));

                    if let (Some(peer_id), Some(session_source)) =
                        (peer_id.as_ref(), self.session_source)
                    {
                        let registry = registry.clone();
                        let peer_id = peer_id.clone();
                        tokio::spawn(async move {
                            registry
                                .release_connection_scoped_claims(&peer_id, session_source)
                                .await;
                        });
                    }

                    // Skip re-deriving supersession when `known_superseded`
                    // already settled it above: this inference reads pool
                    // state the SAME caller that set that flag may have
                    // already mutated (its own address-alias sweep) or not
                    // yet re-published (a sibling's own indexing step can
                    // still be pending), so it must never downgrade an
                    // authoritative "yes" back to "no".
                    if !superseded
                        && let Some(peer_id) = peer_id.as_ref()
                        && let Some(current) = pool.get_connection_by_peer_id(peer_id)
                        && let Some(handle) = current.stream_handle.as_ref()
                        && handle.instance_id() != expected_instance
                    {
                        superseded = true;
                        debug!(
                            peer = %peer_addr,
                            peer_id = %peer_id,
                            exiting_instance = expected_instance,
                            current_instance = handle.instance_id(),
                            "IO task exited for stale connection; skipping pending cancel/failure handling"
                        );
                    } else if !superseded
                        && peer_id.is_none()
                        && let Some(current) = pool.get_lock_free_connection(peer_addr)
                        && let Some(handle) = current.stream_handle.as_ref()
                        && handle.instance_id() != expected_instance
                    {
                        superseded = true;
                        debug!(
                            peer = %peer_addr,
                            exiting_instance = expected_instance,
                            current_instance = handle.instance_id(),
                            "IO task exited for stale addr-mapped connection; skipping pending cancel/failure handling"
                        );
                    }

                    if superseded {
                        should_cancel_pending = false;
                        // The exiting IO task's OWN instance is superseded —
                        // the peer's current session (or, address-only, the
                        // addr-indexed session) is a DIFFERENT stream
                        // instance. `should_cancel_pending = false` above
                        // suppresses pending-request cancellation and
                        // peer-wide failure accounting/consensus/gossip
                        // signalling, which must never fire for a superseded
                        // exit. It must NOT, however, suppress retiring the
                        // exiting instance's own bookkeeping: this call is
                        // the sole production caller that can retire it, and
                        // skipping it here leaves a zombie
                        // `connections_by_addr` alias plus a leaked
                        // `connection_counter` contribution forever (nothing
                        // else will ever find this instance again once it is
                        // no longer indexed by address). This goes straight
                        // to the pool's instance-scoped cleanup rather than
                        // through `GossipRegistry::handle_peer_connection_failure`:
                        // that function's own superseded-instance branch is
                        // reached only when `peer_id` is resolvable; for the
                        // address-only branch above (`peer_id` unknown) it
                        // instead falls through to its unconditional
                        // peer-wide tail (marks the peer failed, may trigger
                        // consensus) — exactly the accounting this path must
                        // never fire. Calling the pool method directly is
                        // correct and synchronous for both branches, and
                        // never touches the current (winning) session: the
                        // compare-and-remove inside is keyed on
                        // `expected_instance`'s own identity.
                        let retired =
                            pool.remove_connection_instance_by_id(peer_addr, expected_instance);
                        if retired.is_none() {
                            // Not found at `peer_addr` — either a fresh
                            // reconnect already reindexed that address, or a
                            // different teardown path (e.g. a concurrent
                            // `ReplaceExisting` tie-break's
                            // `disconnect_connection_instance`) already
                            // retired this exact instance. Route through the
                            // shared ownership table rather than
                            // decrementing unconditionally: it releases the
                            // compensating count exactly once if it is still
                            // outstanding, and is a safe no-op both when
                            // another path already released it and when this
                            // instance (a rejected outbound candidate that
                            // `unpublish_rejected_outbound_candidate` aborted
                            // without ever counting it) was never counted at
                            // all — which is exactly what prevents this
                            // fallback from underflowing `connection_counter`
                            // for a candidate that never bumped it.
                            pool.release_displaced_connection_count(expected_instance);
                        }
                    }

                    if should_cancel_pending {
                        warn!(
                            peer = %peer_addr,
                            peer_id = ?peer_id,
                            stream_instance_id = expected_instance,
                            "transport_io_task_exit_current_connection"
                        );
                        if let Some(correlation) = self.response_correlation.as_ref() {
                            correlation.cancel_all();
                        }
                        tokio::spawn(async move {
                            if let Err(e) = registry
                                .handle_peer_connection_failure(peer_addr, Some(expected_instance))
                                .await
                            {
                                warn!(
                                    peer = %peer_addr,
                                    error = %e,
                                    "IO task failure handling failed"
                                );
                            }
                        });
                    }
                    return;
                }

                if superseded {
                    return;
                }
                warn!(
                    peer = ?self.peer_addr,
                    peer_id = ?self.peer_id,
                    stream_instance_id = self.instance_id,
                    "transport_io_task_exit_without_registry"
                );
                if let Some(correlation) = self.response_correlation.as_ref() {
                    correlation.cancel_all();
                }
            }
        }

        let _exit_guard = ExitGuard {
            flag: exit_flag,
            notify: exit_notify,
            write_queue: write_queue.clone(),
            immediate_write_queue: immediate_write_queue.clone(),
            streaming_queue: streaming_queue.clone(),
            response_correlation: read_context
                .as_ref()
                .and_then(|ctx| ctx.response_correlation.clone()),
            registry_weak: read_context.as_ref().map(|ctx| ctx.registry_weak.clone()),
            peer_addr: read_context.as_ref().map(|ctx| ctx.peer_addr),
            peer_id: read_context.as_ref().and_then(|ctx| ctx.peer_id.clone()),
            session_source: read_context.as_ref().map(|ctx| ctx.session_source),
            instance_id,
            known_superseded,
        };

        let perf = if IoPerfCounters::enabled() {
            Some(IoPerfCounters::global())
        } else {
            None
        };
        let mut perf_last = Instant::now();
        let perf_interval = perf
            .map(|_| IoPerfCounters::interval())
            .unwrap_or_else(|| Duration::from_secs(1));
        if perf.is_some() {
            if let Some(ctx) = read_context.as_ref() {
                info!(peer = %ctx.peer_addr, "IO PERF enabled");
            } else {
                info!("IO PERF enabled (no read context)");
            }
        }

        let mut stream = stream;

        // Larger batches for higher throughput (reduced syscalls)
        const OWNER_BATCH_SIZE: usize = 64;
        const READ_BATCH_LIMIT: usize = 2048;
        const ASK_READ_BATCH_LIMIT: usize = 8192;
        // Large stream slices yield to the normal queue and socket read side.
        const STREAM_ACTIVE_WRITE_BATCH: usize = 16;
        // Drain a bounded priority burst before regular traffic. Admission is
        // reserved by `immediate_write_queue`; this budget preserves regular
        // traffic progress if control replies arrive continuously.
        const IMMEDIATE_WRITE_BATCH: usize = 8;

        let mut bytes_since_flush = 0;

        // Pre-allocate reusable buffers to avoid allocations in the hot loop
        let mut write_chunks: Vec<bytes::Bytes> = Vec::with_capacity(OWNER_BATCH_SIZE * 2);
        let mut owner_batch: Vec<WriteCommand> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut direct_ask_headers: Vec<[u8; 16]> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut direct_ask_payloads: Vec<bytes::Bytes> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut inline32_headers: Vec<[u8; 32]> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut inline32_payloads: Vec<bytes::Bytes> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut response_batch = ResponseBatch::new(READ_BATCH_LIMIT);
        let mut direct_response_batch = DirectResponseBatch::new(READ_BATCH_LIMIT);
        let mut pending_cmd: Option<WriteCommand> = None;
        let mut pending_immediate_cmd: Option<WriteCommand> = None;
        let mut pending_stream_cmd: Option<PendingStreamingCommand> = None;
        // A local response that just completed a frame yields here instead of
        // being forced ahead of shared streaming work. Keeping the pending
        // command out of `LocalStreamingQueue` preserves its in-flight byte
        // accounting while allowing the source scheduler to alternate.
        let mut yielded_stream_cmd: Option<PendingStreamingCommand> = None;
        // Alternate local response frames with producer-owned shared stream
        // frames whenever both are ready. This is state local to the IO owner
        // and adds no synchronization to the messaging hot path.
        let mut prefer_shared_streaming = true;
        let max_message_size = read_context
            .as_ref()
            .map(|ctx| ctx.max_message_size)
            .unwrap_or(STREAM_CHUNK_SIZE);
        let mut local_streaming_queue =
            LocalStreamingQueue::with_response_reserve(max_message_size);
        let mut read_state = read_context.as_ref().map(|_| ReadState::new());
        let mut streaming_state = match read_context
            .as_ref()
            .and_then(|ctx| ctx.streaming_state_handoff.as_ref())
        {
            // R-6: inherit the accept path's first-frame StreamingState so a
            // multi-chunk StreamStart that began as the connection's first
            // frame is continued here, not rejected as "unknown stream_id"
            // against a fresh state. Race the handoff against a shutdown check
            // so a cancelled/errored accept path (or connection shutdown during
            // the wait) cannot hang the IO task forever; the accept path
            // notifies on success AND on its error path, and this loop is the
            // defensive backstop. On shutdown/no-handoff fall back to a fresh
            // state and let the main loop's shutdown check / read error exit.
            Some(handoff) => {
                loop {
                    tokio::select! {
                        _ = handoff.ready.notified() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                            if shutdown_signal.load(Ordering::Acquire) {
                                break;
                            }
                        }
                    }
                }
                Some(
                    handoff
                        .cell
                        .lock()
                        .ok()
                        .and_then(|mut g| g.take())
                        .unwrap_or_else(crate::protocol::StreamingState::new),
                )
            }
            None => read_context
                .as_ref()
                .map(|_| crate::protocol::StreamingState::new()),
        };
        let mut last_cleanup = std::time::Instant::now();

        while !shutdown_signal.load(Ordering::Acquire) {
            let mut total_bytes_written = 0;
            let mut did_work = false;
            let mut wrote_ask_payload = false;
            let mut wrote_actor_responses = false;
            let mut wrote_fast_responses = false;
            // A partial streaming frame owns the wire. Response batches
            // accumulated while it is in flight must survive into the turn
            // that completes that frame; clearing them here would silently
            // drop responses that were intentionally deferred.
            if pending_stream_cmd.is_none() {
                response_batch.clear();
                direct_response_batch.clear();
            }

            // Complete one bounded piece of the current frame before handling
            // normal writes and inbound reads. A partial frame stays ahead of
            // every other streaming command, preserving wire framing.
            let next_stream = if let Some(pending) = pending_stream_cmd.take() {
                Some(pending)
            } else {
                let source = choose_streaming_source(
                    prefer_shared_streaming,
                    local_streaming_queue.has_pending() || yielded_stream_cmd.is_some(),
                    streaming_queue.has_pending(),
                );
                match source {
                    Some(StreamingSource::Local) => {
                        if let Some(pending) = yielded_stream_cmd.take() {
                            prefer_shared_streaming = true;
                            Some(pending)
                        } else if let Some(command) = local_streaming_queue.pop_front() {
                            prefer_shared_streaming = true;
                            Some(PendingStreamingCommand::local(command))
                        } else {
                            streaming_queue.pop().map(|command| {
                                prefer_shared_streaming = false;
                                PendingStreamingCommand::shared(command)
                            })
                        }
                    }
                    Some(StreamingSource::Shared) => {
                        if let Some(command) = streaming_queue.pop() {
                            prefer_shared_streaming = false;
                            Some(PendingStreamingCommand::shared(command))
                        } else if let Some(pending) = yielded_stream_cmd.take() {
                            prefer_shared_streaming = true;
                            Some(pending)
                        } else {
                            local_streaming_queue.pop_front().map(|command| {
                                prefer_shared_streaming = true;
                                PendingStreamingCommand::local(command)
                            })
                        }
                    }
                    None => None,
                }
            };
            if let Some(mut pending) = next_stream {
                did_work = true;
                let command_is_flush = matches!(&pending.command, StreamingCommand::Flush);
                let (written, complete) =
                    match write_streaming_command_slice(&mut stream, &mut pending).await {
                        Ok(result) => result,
                        Err(error) => {
                            error!(%error, "streaming write error");
                            return;
                        }
                    };
                bytes_written_counter.fetch_add(written, Ordering::Relaxed);
                total_bytes_written += written;
                if command_is_flush {
                    flush_pending.store(false, Ordering::Release);
                    bytes_since_flush = 0;
                }
                finish_streaming_command_slice(
                    pending,
                    complete,
                    &streaming_queue,
                    &mut yielded_stream_cmd,
                    &mut pending_stream_cmd,
                );
            }
            local_streaming_queue.set_wire_blocked(pending_stream_cmd.is_some());

            // A partial streaming frame owns the wire until complete. Reads may
            // still run below, but no normal/response write can interleave and
            // corrupt the frame boundary.
            if pending_stream_cmd.is_none() {
                // Queued backpressure NACKs (see `LocalStreamingQueue::queue_ask_nack`)
                // are only ever safe to write here, now that the wire is
                // proven free of a partial frame.
                match drain_pending_ask_nacks(
                    &mut stream,
                    &bytes_written_counter,
                    &mut bytes_since_flush,
                    &mut local_streaming_queue,
                )
                .await
                {
                    // Entries survived the bounded per-turn drain: treat this
                    // turn as having done work so the loop revisits the top
                    // (and this drain) again instead of falling into the
                    // `!did_work` pre-park/idle-select path below with a NACK
                    // still queued and no other event left to wake it.
                    Ok(more_pending) => {
                        if more_pending {
                            did_work = true;
                        }
                    }
                    Err(e) => {
                        warn!(
                            peer = ?read_context.as_ref().map(|c| c.peer_addr),
                            error = %e,
                            "Failed to drain queued ask backpressure NACKs"
                        );
                        return;
                    }
                }
                // ACTOR_REM_2 R8: service the normal write queue when no partial
                // stream frame is outstanding. The batch remains bounded during
                // active streaming so control traffic is not starved.
                {
                let normal_batch_limit = if streaming_active.load(Ordering::Acquire) {
                    STREAM_ACTIVE_WRITE_BATCH
                } else {
                    OWNER_BATCH_SIZE
                };
                // Reuse pre-allocated buffers instead of creating new ones
                write_chunks.clear();
                owner_batch.clear();
                inline32_headers.clear();
                inline32_payloads.clear();

                // A priority command that raced the idle select still leads
                // the next batch. Drain only a bounded burst so the normal
                // queue cannot be starved by a sustained control flood.
                if let Some(cmd) = pending_immediate_cmd.take() {
                    owner_batch.push(cmd);
                }

                while owner_batch.len() < IMMEDIATE_WRITE_BATCH {
                    match immediate_write_queue.pop() {
                        Some(command) => owner_batch.push(command),
                        None => break,
                    }
                }

                if let Some(cmd) = pending_cmd.take() {
                    owner_batch.push(cmd);
                }

                let regular_batch_end = owner_batch.len() + normal_batch_limit;
                while owner_batch.len() < regular_batch_end {
                    match write_queue.pop() {
                        Some(command) => owner_batch.push(command),
                        None => break,
                    }
                }

                if !owner_batch.is_empty() {
                    did_work = true;
                    for command in owner_batch.drain(..) {
                        let is_ask_payload = matches!(&command, WriteCommand::AskPayload(_));
                        let is_immediate_payload =
                            matches!(&command, WriteCommand::ImmediatePayload(_));
                        if is_immediate_payload {
                            immediate_write_queue.notify_space();
                        } else {
                            write_queue.notify_space();
                        }
                        let payload = match command {
                            WriteCommand::Payload(payload)
                            | WriteCommand::ImmediatePayload(payload) => payload,
                            WriteCommand::AskPayload(payload) => {
                                wrote_ask_payload = true;
                                payload
                            }
                        };
                        let ask_write_start = if is_ask_payload && perf.is_some() {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        if !matches!(&payload, WritePayload::DirectAskInline { .. })
                            && !direct_ask_headers.is_empty()
                        {
                            let bytes_written = match flush_direct_ask_batch(
                                &mut stream,
                                &mut direct_ask_headers,
                                &mut direct_ask_payloads,
                            )
                            .await
                            {
                                Ok(n) => n,
                                Err(_) => return,
                            };
                            bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                            total_bytes_written += bytes_written;
                        }
                        match payload {
                            WritePayload::Single(data)
                            | WritePayload::Framed(data)
                            | WritePayload::TrustedFrame(data) => write_chunks.push(data),
                            WritePayload::HeaderPayload { header, payload } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                write_chunks.push(header);
                                write_chunks.push(payload);
                            }
                            WritePayload::HeaderInline {
                                header,
                                header_len,
                                payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }

                                let header_len = header_len as usize;
                                let mut header_off = 0usize;
                                let mut payload_off = 0usize;
                                let payload_len = payload.len();

                                while header_off < header_len || payload_off < payload_len {
                                    let h = &header[header_off..header_len];
                                    let p = &payload[payload_off..];
                                    let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                    let slice_count = if h.is_empty() {
                                        slices[0] = IoSlice::new(p);
                                        1
                                    } else if p.is_empty() {
                                        slices[0] = IoSlice::new(h);
                                        1
                                    } else {
                                        2
                                    };

                                    match write_vectored_all(&mut stream, &slices[..slice_count])
                                        .await
                                    {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                            if header_off < header_len {
                                                let h_rem = header_len - header_off;
                                                if n < h_rem {
                                                    header_off += n;
                                                    continue;
                                                } else {
                                                    header_off = header_len;
                                                    payload_off += n - h_rem;
                                                }
                                            } else {
                                                payload_off += n;
                                            }
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInlineAligned {
                                header,
                                header_len,
                                payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }

                                let header_len = header_len as usize;
                                let mut header_off = 0usize;
                                let mut payload_off = 0usize;
                                let payload_len = payload.len();
                                let payload_bytes = payload.as_ref();

                                while header_off < header_len || payload_off < payload_len {
                                    let h = &header[header_off..header_len];
                                    let p = &payload_bytes[payload_off..];
                                    let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                    let slice_count = if h.is_empty() {
                                        slices[0] = IoSlice::new(p);
                                        1
                                    } else if p.is_empty() {
                                        slices[0] = IoSlice::new(h);
                                        1
                                    } else {
                                        2
                                    };

                                    match write_vectored_all(&mut stream, &slices[..slice_count])
                                        .await
                                    {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                            if header_off < header_len {
                                                let h_rem = header_len - header_off;
                                                if n < h_rem {
                                                    header_off += n;
                                                    continue;
                                                } else {
                                                    header_off = header_len;
                                                    payload_off += n - h_rem;
                                                }
                                            } else {
                                                payload_off += n;
                                            }
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInline32 { header, payload } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }
                                inline32_headers.push(header);
                                inline32_payloads.push(payload);
                                if inline32_headers.len() == OWNER_BATCH_SIZE {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                            }
                            WritePayload::HeaderPooled {
                                header,
                                prefix,
                                mut payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    const MAX_IOV: usize = 64;
                                    // Use drain to preserve buffer capacity
                                    let mut idx = 0;
                                    let mut iov: [MaybeUninit<IoSlice<'_>>; MAX_IOV] = unsafe {
                                        MaybeUninit::<[MaybeUninit<IoSlice<'_>>; MAX_IOV]>::uninit()
                                            .assume_init()
                                    };

                                    for chunk in &write_chunks {
                                        iov[idx].write(IoSlice::new(&chunk));
                                        idx += 1;
                                        if idx == MAX_IOV {
                                            let slices = unsafe {
                                                std::slice::from_raw_parts(
                                                    iov.as_ptr() as *const IoSlice<'_>,
                                                    idx,
                                                )
                                            };
                                            match write_vectored_all(&mut stream, slices).await {
                                                Ok(bytes_written) => {
                                                    bytes_written_counter.fetch_add(
                                                        bytes_written,
                                                        Ordering::Relaxed,
                                                    );
                                                    total_bytes_written += bytes_written;
                                                }
                                                Err(_) => return,
                                            }
                                            idx = 0;
                                        }
                                    }

                                    if idx > 0 {
                                        let slices = unsafe {
                                            std::slice::from_raw_parts(
                                                iov.as_ptr() as *const IoSlice<'_>,
                                                idx,
                                            )
                                        };
                                        match write_vectored_all(&mut stream, slices).await {
                                            Ok(bytes_written) => {
                                                bytes_written_counter
                                                    .fetch_add(bytes_written, Ordering::Relaxed);
                                                total_bytes_written += bytes_written;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                    write_chunks.clear();
                                }

                                if (stream.write_all(&header).await).is_err() {
                                    return;
                                }
                                bytes_written_counter.fetch_add(header.len(), Ordering::Relaxed);
                                total_bytes_written += header.len();

                                if let Some(prefix) = prefix {
                                    if (stream.write_all(&prefix).await).is_err() {
                                        return;
                                    }
                                    bytes_written_counter
                                        .fetch_add(prefix.len(), Ordering::Relaxed);
                                    total_bytes_written += prefix.len();
                                }

                                while payload.has_remaining() {
                                    match stream.write_buf(&mut payload).await {
                                        Ok(0) => return, // R-7: WriteZero mid-frame -> teardown (break would drop the remaining payload and desync the wire)
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInlinePooled {
                                header,
                                header_len,
                                prefix,
                                prefix_len,
                                mut payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    // Use drain to preserve buffer capacity
                                    let mut slices = Vec::with_capacity(write_chunks.len());
                                    for chunk in &write_chunks {
                                        slices.push(IoSlice::new(&chunk));
                                    }
                                    match write_vectored_all(&mut stream, &slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter
                                                .fetch_add(bytes_written, Ordering::Relaxed);
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                let header_len = header_len as usize;
                                let prefix_len = prefix_len as usize;
                                let mut header_off = 0usize;
                                let mut prefix_off = 0usize;

                                if let Some(prefix) = prefix {
                                    while header_off < header_len || prefix_off < prefix_len {
                                        let h = &header[header_off..header_len];
                                        let p = &prefix[prefix_off..prefix_len];
                                        let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                        let slice_count = if h.is_empty() {
                                            slices[0] = IoSlice::new(p);
                                            1
                                        } else if p.is_empty() {
                                            slices[0] = IoSlice::new(h);
                                            1
                                        } else {
                                            2
                                        };

                                        match write_vectored_all(
                                            &mut stream,
                                            &slices[..slice_count],
                                        )
                                        .await
                                        {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter
                                                    .fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                if header_off < header_len {
                                                    let h_rem = header_len - header_off;
                                                    if n < h_rem {
                                                        header_off += n;
                                                        continue;
                                                    } else {
                                                        header_off = header_len;
                                                        prefix_off += n - h_rem;
                                                    }
                                                } else {
                                                    prefix_off += n;
                                                }
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                } else {
                                    while header_off < header_len {
                                        let h = &header[header_off..header_len];
                                        match write_vectored_all(&mut stream, &[IoSlice::new(h)])
                                            .await
                                        {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter
                                                    .fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                header_off += n;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                while payload.has_remaining() {
                                    match stream.write_buf(&mut payload).await {
                                        Ok(0) => return, // R-7: WriteZero mid-frame -> teardown (break would drop the remaining payload and desync the wire)
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::Buf { mut buf, .. } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    // Use drain to preserve buffer capacity
                                    let mut slices = Vec::with_capacity(write_chunks.len());
                                    for chunk in &write_chunks {
                                        slices.push(IoSlice::new(&chunk));
                                    }
                                    match write_vectored_all(&mut stream, &slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter
                                                .fetch_add(bytes_written, Ordering::Relaxed);
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                while buf.has_remaining() {
                                    match stream.write_buf(&mut buf).await {
                                        Ok(0) => return, // R-7: WriteZero mid-frame -> teardown (break would drop the remaining payload and desync the wire)
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::DirectAskInline { header, payload } => {
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                direct_ask_headers.push(header);
                                direct_ask_payloads.push(payload);
                                if direct_ask_headers.len() == OWNER_BATCH_SIZE {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                            }
                        }
                        if let (Some(perf), Some(start)) = (perf, ask_write_start) {
                            perf.ask_write_calls.fetch_add(1, Ordering::Relaxed);
                            perf.ask_write_ns
                                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }
                        if is_immediate_payload && total_bytes_written > 0 {
                            let _ = stream.flush().await;
                            total_bytes_written = 0;
                            flush_pending.store(false, Ordering::Release);
                        }
                    }
                }

                if !inline32_headers.is_empty() {
                    let bytes_written = match flush_inline32_batch(
                        &mut stream,
                        &mut inline32_headers,
                        &mut inline32_payloads,
                    )
                    .await
                    {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                    total_bytes_written += bytes_written;
                }

                if !direct_ask_headers.is_empty() {
                    let bytes_written = match flush_direct_ask_batch(
                        &mut stream,
                        &mut direct_ask_headers,
                        &mut direct_ask_payloads,
                    )
                    .await
                    {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                    total_bytes_written += bytes_written;
                }

                if !write_chunks.is_empty() {
                    let bytes_written = match write_chunks_batched(&mut stream, &write_chunks).await
                    {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                    total_bytes_written += bytes_written;
                    write_chunks.clear();
                }
                }
            }

            bytes_since_flush += total_bytes_written;
            if should_flush_stream_output(
                bytes_since_flush,
                pending_stream_cmd.as_ref(),
                yielded_stream_cmd.as_ref(),
            ) {
                let _ = stream.flush().await;
                bytes_since_flush = 0;
                flush_pending.store(false, Ordering::Release);
            }

            if let (Some(ctx), Some(state), Some(streaming_state)) = (
                read_context.as_ref(),
                read_state.as_mut(),
                streaming_state.as_mut(),
            ) {
                if last_cleanup.elapsed() >= std::time::Duration::from_secs(30) {
                    streaming_state.cleanup_stale();
                    last_cleanup = std::time::Instant::now();
                }

                if did_work {
                    let mut reads = 0usize;
                    let mut read_batch_limit = READ_BATCH_LIMIT;
                    while reads < read_batch_limit
                        && !local_streaming_queue.is_full()
                        // A read that dispatches to an unknown actor, a
                        // missing handler, or backpressure can queue a NACK
                        // (`LocalStreamingQueue::queue_ask_nack`). Admitting
                        // reads past the point where that queue has no more
                        // room would force it to either evict an
                        // already-consumed ask's only remaining record
                        // (silently losing its terminal outcome) or grow
                        // without bound; stopping here instead lets the
                        // drain at the top of the next turn make room first.
                        && local_streaming_queue.has_room_for_ask_nack()
                        && (pending_stream_cmd.is_none()
                            || (response_batch.total_bytes() < RESPONSE_BATCH_BYTE_CAP
                                && direct_response_batch.total_bytes()
                                    < RESPONSE_BATCH_BYTE_CAP))
                    {
                        // R-I: cap per-turn byte accumulation independent of
                        // the frame-count cap above. Checked every iteration
                        // (covering both the normal and the fast-io `continue`
                        // paths below) so a peer packing its ask window with
                        // max-size response frames cannot force unbounded
                        // memory growth before `read_batch_limit` frames are
                        // seen. See `flush_response_batch_if_over_byte_cap`.
                        if pending_stream_cmd.is_none()
                            && let Err(e) = flush_response_batch_if_over_byte_cap(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write response batch (byte cap)"
                            );
                            return;
                        }
                        if pending_stream_cmd.is_none()
                            && let Err(e) = flush_direct_response_batch_if_over_byte_cap(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut direct_response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write direct response batch (byte cap)"
                            );
                            return;
                        }

                        let read_start = perf.map(|_| Instant::now());
                        let read_result =
                            match read_message_step_nonblocking(&mut stream, state, ctx, streaming_state).await {
                                Ok(result) => result,
                                Err(e) => {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "IO task read error"
                                    );
                                    return;
                                }
                            };
                        if let (Some(perf), Some(start)) = (perf, read_start) {
                            if read_result.progressed || read_result.result.is_some() {
                                perf.read_calls.fetch_add(1, Ordering::Relaxed);
                                perf.read_ns.fetch_add(
                                    start.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }

                        if let Some(result) = read_result.result {
                            reads += 1;
                            read_batch_limit = read_batch_limit.max(read_batch_limit_for(&result));
                            let fast_result = match try_handle_fast_io(
                                result,
                                ctx,
                                &mut stream,
                                &bytes_written_counter,
                                &mut bytes_since_flush,
                                &mut response_batch,
                                &mut local_streaming_queue,
                                &mut direct_response_batch,
                                &mut wrote_fast_responses,
                                perf,
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(e) => {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to process fast IO message"
                                    );
                                    if is_streaming_admission_backpressure(&e) {
                                        break;
                                    }
                                    None
                                }
                            };
                            let Some(result) = fast_result else {
                                continue;
                            };
                            if let Some(registry) = ctx.registry_weak.upgrade() {
                                if let Err(e) = process_read_result_io(
                                    result,
                                    streaming_state,
                                    &registry,
                                    ctx.peer_addr,
                                    ctx.session_source,
                                    ctx.peer_id.as_ref(),
                                    ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                    ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                    &mut local_streaming_queue,
                                    &mut direct_response_batch,
                                    perf,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to process message on IO task"
                                    );
                                    if is_streaming_admission_backpressure(&e) {
                                        break;
                                    }
                                }
                            } else {
                                warn!(
                                    peer = %ctx.peer_addr,
                                    "Registry dropped, stopping IO task"
                                );
                                return;
                            }
                        } else if !read_result.progressed {
                            break;
                        }
                    }
                    if pending_stream_cmd.is_none() && !response_batch.is_empty() {
                        if let Err(e) = write_response_batch(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write response batch"
                            );
                            return;
                        }
                        wrote_actor_responses = true;
                    }
                    if pending_stream_cmd.is_none() && !direct_response_batch.is_empty() {
                        if let Err(e) = write_direct_response_batch(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut direct_response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write direct response batch"
                            );
                            return;
                        }
                        wrote_fast_responses = true;
                    }
                }
            }

            // Ask RTT fast path: when this loop writes ask requests and/or actor responses,
            // flush immediately to avoid waiting on generic throughput-oriented flush thresholds.
            if (wrote_ask_payload || wrote_actor_responses || wrote_fast_responses)
                && bytes_since_flush > 0
            {
                let _ = stream.flush().await;
                bytes_since_flush = 0;
                flush_pending.store(false, Ordering::Release);
            }

            if !did_work {
                if let (Some(ctx), Some(state), Some(streaming_state)) = (
                    read_context.as_ref(),
                    read_state.as_mut(),
                    streaming_state.as_mut(),
                ) {
                    // Pre-park drain: clear each queue's pending flag and
                    // re-check for a command that raced the last drain. A push
                    // landing after the clear stores a wakeup permit, so the
                    // select below cannot park forever with frames queued.
                    if let Some(cmd) = immediate_write_queue.prepare_park() {
                        pending_immediate_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = write_queue.prepare_park() {
                        pending_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = streaming_queue.prepare_park() {
                        pending_stream_cmd = Some(PendingStreamingCommand::shared(cmd));
                        continue;
                    }
                    tokio::select! {
                        // Idle path: block waiting for socket readability.
                        read_result = read_message_step_poll(&mut stream, state, ctx, streaming_state, true) => {
                            let read_start = perf.map(|_| Instant::now());
                            let read_result = match read_result {
                                Ok(result) => result,
                                Err(e) => {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "IO task read error"
                                    );
                                    return;
                                }
                            };

                            if let (Some(perf), Some(start)) = (perf, read_start) {
                                if read_result.progressed || read_result.result.is_some() {
                                    perf.read_calls.fetch_add(1, Ordering::Relaxed);
                                    perf.read_ns.fetch_add(
                                        start.elapsed().as_nanos() as u64,
                                        Ordering::Relaxed,
                                    );
                                }
                            }

                            if let Some(result) = read_result.result {
                                let fast_result = match try_handle_fast_io(
                                    result,
                                    ctx,
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                    &mut local_streaming_queue,
                                    &mut direct_response_batch,
                                    &mut wrote_fast_responses,
                                    perf,
                                )
                                .await
                                {
                                    Ok(result) => result,
                                    Err(e) => {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            error = %e,
                                            "Failed to process fast IO message"
                                        );
                                        if is_streaming_admission_backpressure(&e) {
                                            // The idle select arm has no inner
                                            // drain loop, so `continue` returns
                                            // to the writer loop and lets the
                                            // queued response drain. A bare
                                            // `break` would tear down the
                                            // connection on transient pressure.
                                            continue;
                                        }
                                        None
                                    }
                                };
                                if let Some(result) = fast_result {
                                    if let Some(registry) = ctx.registry_weak.upgrade() {
                                        if let Err(e) = process_read_result_io(
                                            result,
                                            streaming_state,
                                            &registry,
                                            ctx.peer_addr,
                                            ctx.session_source,
                                            ctx.peer_id.as_ref(),
                                            ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                            ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                            &mut stream,
                                            &bytes_written_counter,
                                            &mut bytes_since_flush,
                                            &mut response_batch,
                                            &mut local_streaming_queue,
                                            &mut direct_response_batch,
                                            perf,
                                        )
                                        .await
                                        {
                                            warn!(
                                                peer = %ctx.peer_addr,
                                                error = %e,
                                                "Failed to process message on IO task"
                                            );
                                            if is_streaming_admission_backpressure(&e) {
                                                continue;
                                            }
                                        }
                                    } else {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            "Registry dropped, stopping IO task"
                                        );
                                        return;
                                    }
                                }
                            }

                            // Under load the socket can become readable with many frames queued.
                            // The old "idle path" processed only a single frame per wake-up,
                            // which inflates RTT and caps ActorAsk throughput on server-heavy links.
                            //
                            // Drain additional frames non-blocking to batch handler + response writes.
                            let mut drained = 0usize;
                            let mut drain_batch_limit = READ_BATCH_LIMIT;
                            while drained < drain_batch_limit
                                && !local_streaming_queue.is_full()
                                // See the identical check in the primary
                                // drain loop above.
                                && local_streaming_queue.has_room_for_ask_nack()
                                && (pending_stream_cmd.is_none()
                                    || (response_batch.total_bytes() < RESPONSE_BATCH_BYTE_CAP
                                        && direct_response_batch.total_bytes()
                                            < RESPONSE_BATCH_BYTE_CAP))
                            {
                                // R-I: same per-turn byte cap as the primary
                                // drain loop above; see
                                // `flush_response_batch_if_over_byte_cap`.
                                if pending_stream_cmd.is_none()
                                    && let Err(e) = flush_response_batch_if_over_byte_cap(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write response batch (byte cap)"
                                    );
                                    return;
                                }
                                if pending_stream_cmd.is_none()
                                    && let Err(e) = flush_direct_response_batch_if_over_byte_cap(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut direct_response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write direct response batch (byte cap)"
                                    );
                                    return;
                                }

                                let read_start = perf.map(|_| Instant::now());
                                let next = match read_message_step_nonblocking(&mut stream, state, ctx, streaming_state).await {
                                    Ok(r) => r,
                                    Err(e) => {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            error = %e,
                                            "IO task read error"
                                        );
                                        return;
                                    }
                                };
                                if let (Some(perf), Some(start)) = (perf, read_start) {
                                    if next.progressed || next.result.is_some() {
                                        perf.read_calls.fetch_add(1, Ordering::Relaxed);
                                        perf.read_ns.fetch_add(
                                            start.elapsed().as_nanos() as u64,
                                            Ordering::Relaxed,
                                        );
                                    }
                                }

                                if let Some(result) = next.result {
                                    drained += 1;
                                    drain_batch_limit =
                                        drain_batch_limit.max(read_batch_limit_for(&result));
                                    let fast_result = match try_handle_fast_io(
                                        result,
                                        ctx,
                                        &mut stream,
                                        &bytes_written_counter,
                                        &mut bytes_since_flush,
                                        &mut response_batch,
                                        &mut local_streaming_queue,
                                        &mut direct_response_batch,
                                        &mut wrote_fast_responses,
                                        perf,
                                    )
                                    .await
                                    {
                                        Ok(result) => result,
                                        Err(e) => {
                                            warn!(
                                                peer = %ctx.peer_addr,
                                                error = %e,
                                                "Failed to process fast IO message"
                                            );
                                            if is_streaming_admission_backpressure(&e) {
                                                break;
                                            }
                                            None
                                        }
                                    };
                                    let Some(result) = fast_result else {
                                        continue;
                                    };
                                    if let Some(registry) = ctx.registry_weak.upgrade() {
                                        if let Err(e) = process_read_result_io(
                                            result,
                                            streaming_state,
                                            &registry,
                                            ctx.peer_addr,
                                            ctx.session_source,
                                            ctx.peer_id.as_ref(),
                                            ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                            ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                            &mut stream,
                                            &bytes_written_counter,
                                            &mut bytes_since_flush,
                                            &mut response_batch,
                                            &mut local_streaming_queue,
                                            &mut direct_response_batch,
                                            perf,
                                        )
                                        .await
                                            {
                                                warn!(
                                                    peer = %ctx.peer_addr,
                                                    error = %e,
                                                    "Failed to process message on IO task"
                                                );
                                                if is_streaming_admission_backpressure(&e) {
                                                    break;
                                                }
                                            }
                                    } else {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            "Registry dropped, stopping IO task"
                                        );
                                        return;
                                    }
                                } else if !next.progressed {
                                    break;
                                }
                            }

                            if pending_stream_cmd.is_none() && !response_batch.is_empty() {
                                if let Err(e) = write_response_batch(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write response batch"
                                    );
                                    return;
                                }
                            }
                            if pending_stream_cmd.is_none() && !direct_response_batch.is_empty() {
                                if let Err(e) = write_direct_response_batch(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut direct_response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write direct response batch"
                                    );
                                    return;
                                }
                            }
                            // Ensure request/response traffic does not sit in TLS buffers on
                            // quiet links. The idle branch has no outer fast-flush checkpoint
                            // after this select arm, so flush direct/actor responses here.
                            if bytes_since_flush > 0 {
                                let _ = stream.flush().await;
                                bytes_since_flush = 0;
                                flush_pending.store(false, Ordering::Release);
                            }
                        }
                        // Wake on new outbound writes even if the socket is currently idle for reads.
                        // Without this, a mostly-write workload (e.g., initial gossip propagation)
                        // can stall until an unrelated read event occurs.
                        _ = immediate_write_queue.data_notify.notified() => {
                            pending_immediate_cmd = immediate_write_queue.pop();
                        }
                        _ = write_queue.data_notify.notified() => {
                            pending_cmd = write_queue.pop();
                        }
                        _ = immediate_write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = streaming_queue.data_notify.notified() => {
                            // Wake on streaming commands; drained at the top of the loop.
                        }
                    }
                } else {
                    // Pre-park drain; see the read-armed variant above.
                    if let Some(cmd) = immediate_write_queue.prepare_park() {
                        pending_immediate_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = write_queue.prepare_park() {
                        pending_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = streaming_queue.prepare_park() {
                        pending_stream_cmd = Some(PendingStreamingCommand::shared(cmd));
                        continue;
                    }
                    tokio::select! {
                        _ = streaming_queue.data_notify.notified() => {
                            // Wake on streaming commands; drained at the top of the loop.
                        }
                        _ = immediate_write_queue.data_notify.notified() => {
                            pending_immediate_cmd = immediate_write_queue.pop();
                        }
                        _ = write_queue.data_notify.notified() => {
                            pending_cmd = write_queue.pop();
                        }
                        _ = immediate_write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                    }
                }
            }

            if let Some(perf) = perf {
                if perf_last.elapsed() >= perf_interval {
                    let (
                        read_calls,
                        read_ns,
                        handle_calls,
                        handle_ns,
                        write_calls,
                        write_ns,
                        ask_write_calls,
                        ask_write_ns,
                    ) = perf.snapshot_and_reset();
                    let read_us = read_ns as f64 / 1000.0;
                    let handle_us = handle_ns as f64 / 1000.0;
                    let write_us = write_ns as f64 / 1000.0;
                    let ask_write_us = ask_write_ns as f64 / 1000.0;
                    info!(
                        read_calls,
                        handle_calls,
                        write_calls,
                        ask_write_calls,
                        read_us = read_us,
                        handle_us = handle_us,
                        write_us = write_us,
                        ask_write_us = ask_write_us,
                        read_avg_us = read_us / (read_calls.max(1) as f64),
                        handle_avg_us = handle_us / (handle_calls.max(1) as f64),
                        write_avg_us = write_us / (write_calls.max(1) as f64),
                        ask_write_avg_us = ask_write_us / (ask_write_calls.max(1) as f64),
                        "IO PERF"
                    );
                    perf_last = Instant::now();
                }
            }
        }
    }

    /// The invariant this method enforces, for every `WritePayload` this
    /// crate can enqueue: **no frame may be enqueued whose real byte count
    /// disagrees with its own control word.** Every `write_*_header`
    /// constructor in `framing.rs` encodes `body_len` as
    /// `fixed_header_len + payload_len` for the exact lengths it was called
    /// with (see `checked_body_len`); nothing downstream re-derives or
    /// re-checks that promise before the bytes actually go on the wire. A
    /// caller that builds a header from one length and then supplies
    /// (header, payload) pieces whose real, combined length disagrees would
    /// desync the peer's frame parser -- worse than a merely oversize
    /// frame, which the peer at least rejects cleanly as `MessageTooLarge`
    /// without losing its place in the stream.
    ///
    /// This runs once, at the single point every inline (non-streaming)
    /// write funnels through -- called from all four `enqueue_*` functions
    /// below -- so it does not matter whether the caller went through
    /// `ConnectionHandle::reject_oversize_inline`'s pre-check, a narrower
    /// pre-check (`ask_responder`'s streaming-threshold-only lanes), or no
    /// pre-check at all: an inline send whose declared and actual lengths
    /// disagree, or whose declared length exceeds `max_message_size`,
    /// cannot reach the write queue. A pre-check upstream of this one is
    /// still worth keeping where it exists (it can reject before spending a
    /// header build, an `AlignedBytes` conversion, etc.), but none of them
    /// is load-bearing for correctness anymore -- this is.
    ///
    /// For every header-carrying variant, "real byte count" is computed
    /// from the exact slices the write loop (`io_task`, below) actually
    /// puts on the wire for that variant -- `header_len`-truncated arrays,
    /// an optional `prefix`, `Buf::remaining()` for pooled payloads -- not
    /// just `payload.len()`, so this cannot be fooled by a header/payload
    /// pair built from two different lengths.
    ///
    /// PR #183 review, round 10: this used to be documented as a list of
    /// call sites (which public method constructs which variant, and what
    /// each one happens to validate). That list has been revised twice
    /// across this review, and a path not on it turned up again both
    /// times, because a hand-maintained list of call sites is exactly as
    /// complete as its last audit. Stated instead as an exhaustive
    /// property of `WritePayload` the enum: every variant falls into
    /// exactly one of three categories, and the `match` a few lines below
    /// this comment is exhaustive over them -- adding a variant that
    /// doesn't fit any of them fails to compile, not just fails to be
    /// remembered.
    ///
    /// PR #183 review, round 11: round 10 put `Single` in the "parse it to
    /// find out" category below, which was itself the error -- a sniff
    /// that infers "framed" or "opaque" from content cannot tell an
    /// opaque caller's payload from a genuine frame prefix, because the
    /// two are indistinguishable by construction, and arbitrary data lands
    /// on a plausible-looking control word routinely (roughly 15/32 of
    /// random leading bytes), not as some crafted edge case. Content
    /// cannot answer "is this a frame," so the caller has to declare it
    /// instead: `Single` is now unconditionally opaque (bare length
    /// check, content never inspected), and `Framed` is the new lane for
    /// a caller that is genuinely declaring "this is a frame" and wants
    /// it validated as one.
    ///
    /// - **Self-built, trusted bytes: `TrustedFrame`.** Exempt
    ///   unconditionally. Constructible only via `pub(crate)` methods (see
    ///   the variant's own doc comment for the exhaustive list of internal
    ///   call sites), so every possible producer is enumerable by
    ///   visibility, not by convention, and each one already built the
    ///   complete, valid bytes itself before enqueueing -- there is
    ///   nothing left here to check.
    /// - **Caller-declared opaque bytes: `Single`.** No framing contract,
    ///   and none inferred -- `reject_oversize_opaque` (below) checks only
    ///   the total byte count against `max_message_size`. This is the
    ///   lane every current public "send these bytes" method uses
    ///   (`write_bytes_control`/`write_bytes_ask`/`write_bytes_nonblocking`
    ///   and everything built on them), because all of them are documented
    ///   as carrying unframed data.
    /// - **Caller-declared frame(s): `Framed`.** The caller is asserting
    ///   this content is one or more complete V5 frames by choosing this
    ///   constructor over `Single`'s, so `reject_oversize_framed` (below)
    ///   trusts that enough to parse: it decodes as many complete frames
    ///   off the front as the buffer will yield, checking each one's own
    ///   `body_len` against `max_message_size` (so several valid frames
    ///   concatenated are judged the way separate writes would have been),
    ///   and refuses outright any content that begins a frame it does not
    ///   fully supply -- a `Framed` write cannot assert "the rest is
    ///   coming in a later call" the way an internal `TrustedFrame`
    ///   producer can assert "I already built the rest myself." Content
    ///   that never starts looking like a frame at all falls back to the
    ///   same bare length ceiling `Single` uses.
    /// - **Everything else (`Buf`, and the header-carrying variants)** has
    ///   an independently-declared length or header slice to check the
    ///   real bytes against, so it was never subject to this
    ///   opaque-vs-framed ambiguity in the first place:
    ///   `HeaderPayload`/`HeaderInline`/`HeaderInlineAligned`/
    ///   `HeaderInline32`/`HeaderPooled`/`HeaderInlinePooled`/
    ///   `DirectAskInline` carry a header slice with its own embedded
    ///   control word: decode it, and compare the total it declares
    ///   against the real total the write loop will produce for that
    ///   exact command. `Buf` carries no header slice to decode a control
    ///   word from, only a caller-declared `expected_len` captured when
    ///   the header it is chained onto was built: compare that against
    ///   `buf.remaining()`.
    fn reject_oversize_write_payload(&self, payload: &WritePayload) -> Result<()> {
        // `Buf` has no header slice to decode a control word from; check it
        // against its own caller-declared `expected_len` and return early.
        if let WritePayload::Buf { buf, expected_len } = payload {
            let actual_len = buf.remaining();
            if actual_len != *expected_len {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Buf write declared {expected_len} bytes but the buffer \
                         actually has {actual_len} remaining -- refusing to send a \
                         frame whose header would disagree with its own body"
                    ),
                )));
            }
            let body_len = expected_len.saturating_sub(framing::LENGTH_PREFIX_LEN);
            if body_len > self.max_message_size {
                return Err(GossipError::MessageTooLarge {
                    size: body_len,
                    max: self.max_message_size,
                });
            }
            return Ok(());
        }
        if matches!(payload, WritePayload::TrustedFrame(_)) {
            return Ok(());
        }
        if let WritePayload::Single(data) = payload {
            return self.reject_oversize_opaque(data);
        }
        if let WritePayload::Framed(data) = payload {
            return self.reject_oversize_framed(data);
        }

        // Every other variant: pair the header bytes the write loop will
        // actually decode a control word from with the *real* total byte
        // count (header + optional prefix + payload) that loop will
        // actually put on the wire for this exact command.
        let (control_source, actual_total): (&[u8], usize) = match payload {
            WritePayload::HeaderPayload { header, payload } => {
                (header.as_ref(), header.len() + payload.len())
            }
            WritePayload::HeaderInline {
                header,
                header_len,
                payload,
            } => {
                let header_len = *header_len as usize;
                (&header[..], header_len + payload.len())
            }
            WritePayload::HeaderInlineAligned {
                header,
                header_len,
                payload,
            } => {
                let header_len = *header_len as usize;
                (&header[..], header_len + payload.len())
            }
            WritePayload::HeaderInline32 { header, payload } => {
                // Always sent as the full fixed-size array -- there is no
                // `header_len` field because this header kind has no
                // variable-length inline form (see `write_header_and_payload_control_inline32`).
                (&header[..], header.len() + payload.len())
            }
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            } => {
                let prefix_len = prefix.as_ref().map(bytes::Bytes::len).unwrap_or(0);
                (
                    header.as_ref(),
                    header.len() + prefix_len + payload.remaining(),
                )
            }
            WritePayload::HeaderInlinePooled {
                header,
                header_len,
                prefix,
                prefix_len,
                payload,
            } => {
                let header_len = *header_len as usize;
                // The write loop only ever sends the prefix bytes when
                // `prefix` is `Some` (see `io_task`'s `HeaderInlinePooled`
                // arm) -- a stale, unused `prefix_len` when `prefix` is
                // `None` must not count toward the real byte total.
                let prefix_len = if prefix.is_some() {
                    *prefix_len as usize
                } else {
                    0
                };
                (&header[..], header_len + prefix_len + payload.remaining())
            }
            WritePayload::DirectAskInline { header, payload } => {
                // Always sent as the full fixed-size array (see
                // `write_direct_ask_inline`) -- no separate length field.
                (&header[..], header.len() + payload.len())
            }
            WritePayload::Single(_)
            | WritePayload::Framed(_)
            | WritePayload::TrustedFrame(_)
            | WritePayload::Buf { .. } => {
                unreachable!(
                    "Single/Framed/TrustedFrame return early above this match; Buf is handled before it"
                )
            }
        };

        if control_source.len() < framing::LENGTH_PREFIX_LEN {
            return Ok(());
        }
        let mut control_word = [0u8; framing::LENGTH_PREFIX_LEN];
        control_word.copy_from_slice(&control_source[..framing::LENGTH_PREFIX_LEN]);
        let Some(control) = framing::decode_control(control_word) else {
            return Ok(());
        };

        let expected_total = framing::LENGTH_PREFIX_LEN + control.body_len;
        if actual_total != expected_total {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "frame's control word declares {expected_total} total bytes but \
                     the write would actually produce {actual_total} -- refusing to \
                     enqueue a frame whose header disagrees with its own body"
                ),
            )));
        }
        if control.body_len > self.max_message_size {
            return Err(GossipError::MessageTooLarge {
                size: control.body_len,
                max: self.max_message_size,
            });
        }
        Ok(())
    }

    /// Size gate for `WritePayload::Single` -- see the note on it above.
    /// `Single` is unconditionally opaque: the only thing checked is the
    /// total byte count against `max_message_size`. Content is never
    /// inspected, decoded, or walked -- that used to happen here (rounds 6
    /// and 10), and it was the bug: a check that infers "this looks like a
    /// frame" from content cannot tell an opaque caller's payload from a
    /// genuine frame prefix, because arbitrary bytes land on a
    /// plausible-looking `WireKind` + length routinely, not as some
    /// contrived adversarial case. See `reject_oversize_framed` below for
    /// the lane a caller uses when it is genuinely declaring "this is a
    /// frame" by construction of the `WritePayload` it built, rather than
    /// this method inferring it from the bytes.
    fn reject_oversize_opaque(&self, data: &[u8]) -> Result<()> {
        let body_len = data.len().saturating_sub(framing::LENGTH_PREFIX_LEN);
        if body_len > self.max_message_size {
            return Err(GossipError::MessageTooLarge {
                size: body_len,
                max: self.max_message_size,
            });
        }
        Ok(())
    }

    /// Size gate for `WritePayload::Framed` -- see the note on
    /// `reject_oversize_write_payload` above. Choosing `Framed`'s
    /// constructors over `Single`'s is the caller's explicit declaration
    /// that `data` is one or more complete V5 frames, so unlike
    /// `reject_oversize_opaque`, this trusts that declaration enough to
    /// parse: it walks the buffer decoding as many complete frames off the
    /// front as it will yield instead of assuming any one interpretation.
    ///
    /// Each successfully decoded frame is checked against
    /// `max_message_size` on its own `body_len`, not the buffer's
    /// aggregate length -- two valid 100-byte frames concatenated into one
    /// 208-byte `Framed` write are exactly as acceptable as two separate
    /// writes of them would have been, even though 208 alone would fail a
    /// whole-buffer ceiling. The walk stops -- successfully -- the moment a
    /// control word fails to decode, since at that point the remaining
    /// bytes are no longer reliably frame-shaped and fall back to the bare
    /// length ceiling `Single` always uses: still sound with no control
    /// word to trust, since a complete frame whose declared body exceeds
    /// `max_message_size` is, by construction, at least that many bytes
    /// long in total. The walk stops with an *error* if a control word
    /// decodes but the buffer does not contain that frame's complete
    /// declared bytes -- see the rejection below. A caller that declared
    /// "this is a frame" and then supplied an incomplete one gets no more
    /// benefit of the doubt than one that supplied garbage.
    ///
    /// `offset` strictly increases by at least `LENGTH_PREFIX_LEN` each
    /// iteration (every decoded frame is at least that many bytes), so
    /// this terminates in at most `data.len() / LENGTH_PREFIX_LEN`
    /// iterations -- no adversarially-crafted content can loop it.
    fn reject_oversize_framed(&self, data: &[u8]) -> Result<()> {
        let mut offset = 0usize;
        while data.len() - offset >= framing::LENGTH_PREFIX_LEN {
            let mut control_word = [0u8; framing::LENGTH_PREFIX_LEN];
            control_word.copy_from_slice(&data[offset..offset + framing::LENGTH_PREFIX_LEN]);
            let Some(control) = framing::decode_control(control_word) else {
                break;
            };
            let frame_total = framing::LENGTH_PREFIX_LEN + control.body_len;
            if data.len() - offset < frame_total {
                // PR #183 review, round 10: a `Framed` write that begins a
                // frame it does not complete is refused outright, not
                // judged as if the missing bytes simply weren't there --
                // otherwise a caller could split one frame's header
                // (declaring an arbitrary, possibly oversized body) from
                // its body across separate `Framed` writes: each
                // individual call would supply too few bytes to look like
                // a complete frame, so each one would pass, while the peer
                // reassembled the whole frame from the continuous TCP
                // stream anyway (write() boundaries are invisible on the
                // wire). A `Framed` write cannot assert "the rest is
                // coming in a later call" the way an internal
                // `TrustedFrame` producer can assert "I already built the
                // rest myself."
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "control word at offset {offset} declares a {frame_total}-byte \
                         frame (kind {:?}, body_len {}) but this write only supplies \
                         {} bytes from that offset -- refusing a Framed write that \
                         begins a frame it does not complete; a frame split across \
                         separate public calls cannot be validated per-call",
                        control.kind,
                        control.body_len,
                        data.len() - offset,
                    ),
                )));
            }
            if control.body_len > self.max_message_size {
                return Err(GossipError::MessageTooLarge {
                    size: control.body_len,
                    max: self.max_message_size,
                });
            }
            offset += frame_total;
        }
        let remainder = data.len() - offset;
        if remainder > 0 {
            let body_len = remainder.saturating_sub(framing::LENGTH_PREFIX_LEN);
            if body_len > self.max_message_size {
                return Err(GossipError::MessageTooLarge {
                    size: body_len,
                    max: self.max_message_size,
                });
            }
        }
        Ok(())
    }

    async fn enqueue_write(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.reject_oversize_write_payload(&payload)?;
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        let command = WriteCommand::Payload(payload);
        match self.write_queue.try_push(command) {
            Ok(()) => {
                self.write_queue.notify_data();
                Ok(())
            }
            Err(command) => self.write_queue.push(command).await,
        }
    }

    async fn enqueue_ask_write(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.reject_oversize_write_payload(&payload)?;
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        let command = WriteCommand::AskPayload(payload);
        match self.write_queue.try_push(command) {
            Ok(()) => {
                self.write_queue.notify_data();
                Ok(())
            }
            Err(command) => self.write_queue.push(command).await,
        }
    }

    fn enqueue_write_nonblocking(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.reject_oversize_write_payload(&payload)?;
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        match self.write_queue.try_push(WriteCommand::Payload(payload)) {
            Ok(()) => {
                self.write_queue.notify_data();
                Ok(())
            }
            Err(_) => Err(GossipError::WriteQueueFull),
        }
    }

    fn enqueue_immediate_write_nonblocking(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.reject_oversize_write_payload(&payload)?;
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        match self
            .immediate_write_queue
            .try_push(WriteCommand::ImmediatePayload(payload))
        {
            Ok(()) => {
                self.immediate_write_queue.notify_data();
                Ok(())
            }
            Err(_) => Err(GossipError::WriteQueueFull),
        }
    }

    pub async fn write_bytes_ask(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_ask_write(WritePayload::Single(data)).await
    }

    /// See `write_bytes_ask` above for the opaque lane. This is the
    /// counterpart for a caller that is declaring `data` is one or more
    /// complete V5 frames -- see `WritePayload::Framed`'s doc comment.
    pub async fn write_framed_bytes_ask(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_ask_write(WritePayload::Framed(data)).await
    }

    /// `pub(crate)`, deliberately not `pub`: constructs `WritePayload::TrustedFrame`,
    /// which `reject_oversize_write_payload` never validates. See that
    /// variant's own doc comment for exactly which callers this crate lets
    /// use it and why each is safe. Nothing outside this crate can reach
    /// this method, so nothing outside this crate can express an
    /// unvalidated write this way -- that restriction is enforced by
    /// visibility, not by a comment asking callers to be careful.
    pub(crate) async fn write_trusted_bytes_ask(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_ask_write(WritePayload::TrustedFrame(data))
            .await
    }

    /// Queue a compact ActorAsk, installing its connection-local route first.
    /// Both frames use the one ordered writer queue; the small gate only spans
    /// enqueueing, so no response wait or network I/O is serialized here.
    /// Falls back to the uncompact `ActorAsk` frame when the route table has
    /// no slot available for a new (actor_id, type_hash) pair.
    pub async fn write_routed_actor_ask(
        &self,
        correlation_id: u32,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        // Fast pre-check before anything else, using the smaller of the two
        // possible header overheads below (`ROUTED_ACTOR_ASK_HEADER_LEN`):
        // an inline ask over `max_message_size` gets a fatal `MessageTooLarge`
        // read error from the receiver, tearing the whole connection down for
        // every other actor sharing it. This cannot false-reject -- neither
        // branch below ever needs less than this overhead -- so a clearly
        // oversized ask never waits on identification or takes the
        // route-bind gate for nothing. It is not sufficient on its own: the
        // unbound-route fallback below adds the larger `ACTOR_ASK_HEADER_LEN`
        // instead, and is re-checked precisely at that point.
        crate::framing::reject_oversize_for_inline_send(
            crate::framing::ROUTED_ACTOR_ASK_HEADER_LEN,
            payload.len(),
            self.max_message_size,
        )?;

        // A brand-new outbound connection gates this behind its own
        // identifying FullSync (see `begin_identify_gate`/`mark_identified`
        // in `finalize_new_outbound_connection`): a `RouteBind` must never
        // reach the write queue ahead of it, or the acceptor drops the
        // connection outright. No-op (returns immediately) for every other
        // handle, and for this same handle once identified.
        self.wait_until_identified().await?;

        // `route_bind_gate` serializes all routed asks, and `outbound_routes`
        // has no other mutator (the IO/parse path uses the separate inbound
        // table) — this is the serialization of bind publication with routed
        // asks. A second `write_routed_actor_ask` therefore cannot observe a
        // route until this holder drops the gate. On cancellation, locals drop
        // in reverse declaration order: the R-3 bind guard (declared below)
        // runs `remove_unbound` BEFORE `_route_guard` releases the gate, so the
        // next ask sees the route gone and re-binds rather than reusing a
        // stale `needs_bind == false` slot.
        let _route_guard = self.route_bind_gate.lock().await;
        let route = crate::route_interning::RouteKey { actor_id, type_hash };
        let Some((route_slot, needs_bind)) = self.outbound_routes.slot_for(route) else {
            // The connection-local slot space is exhausted (either the
            // MAX_ROUTES_PER_CONNECTION cap, or, vanishingly rarely, the
            // slot-id counter itself). Slots are never unilaterally recycled:
            // the peer only learns a slot is free via an explicit unbind it
            // acknowledges, and no such handshake exists today, so evicting
            // one here could hand a still-referenced id to a new
            // (actor_id, type_hash) and let the peer misroute a compact
            // frame to the wrong actor. Falling back to the uncompact
            // `ActorAsk` frame is always safe instead: it carries actor_id
            // and type_hash directly and needs no connection-local slot at
            // all, so this ask (and every later one on this connection) still
            // succeeds rather than the connection being torn down.
            //
            // This frame's header is `ACTOR_ASK_HEADER_LEN` (28 bytes), wider
            // than the `ROUTED_ACTOR_ASK_HEADER_LEN` (12 bytes) the pre-check
            // above allowed for -- re-check precisely against this branch's
            // real overhead before spending a header build on a payload that
            // would still exceed `max_message_size` once framed.
            crate::framing::reject_oversize_for_inline_send(
                crate::framing::ACTOR_ASK_HEADER_LEN,
                payload.len(),
                self.max_message_size,
            )?;
            let header = crate::framing::try_write_actor_ask_header(
                correlation_id,
                actor_id,
                type_hash,
                payload.len(),
            )?;
            return self
                .write_header_and_payload_ask_inline32(header, payload)
                .await;
        };
        if needs_bind {
            let bind = crate::framing::write_route_bind_header(route_slot, actor_id, type_hash);
            // R-3: arm an RAII rollback for the fresh allocation. If this future
            // is dropped while the bind enqueue below is parked on a full write
            // queue (routine under load — every ask is timeout-wrapped), the
            // guard's Drop removes the route so the next ask re-emits RouteBind
            // instead of sending RoutedActorAsk for a slot the peer never
            // learned ("unknown route slot" -> connection teardown). Disarmed
            // only after the enqueue returns Ok — that is the commit point; the
            // writer task owns retry/teardown beyond it.
            let mut bind_guard = crate::route_interning::UnboundRouteGuard::new(
                self.outbound_routes.as_ref(),
                route_slot,
                route,
            );
            if let Err(error) = self
                .write_trusted_bytes_control(bytes::Bytes::copy_from_slice(&bind))
                .await
            {
                // guard drops armed -> remove_unbound(route_slot, route)
                return Err(error);
            }
            bind_guard.disarm();
        }
        let header = crate::framing::try_write_routed_actor_ask_header(
            correlation_id,
            route_slot,
            payload.len(),
        )?;
        self.write_header_and_payload_ask_inline(header, 16, payload)
            .await
    }

    pub async fn write_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write(WritePayload::Single(data)).await
    }

    /// See `write_bytes_control` above for the opaque lane. This is the
    /// counterpart for a caller that is declaring `data` is one or more
    /// complete V5 frames -- see `WritePayload::Framed`'s doc comment.
    pub async fn write_framed_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write(WritePayload::Framed(data)).await
    }

    /// See the note on `write_trusted_bytes_ask` above.
    pub(crate) async fn write_trusted_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write(WritePayload::TrustedFrame(data)).await
    }

    pub async fn write_header_and_payload_control(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderPayload { header, payload })
            .await
    }

    /// Write header + payload inline without permit allocation for tell messages.
    /// This is a fast path for high-throughput tell operations.
    pub async fn write_header_and_payload_control_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
        .await
    }

    pub async fn write_header_and_payload_control_inline_aligned(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: crate::AlignedBytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInlineAligned {
            header,
            header_len,
            payload,
        })
        .await
    }

    pub async fn write_header_and_payload_control_inline32(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInline32 { header, payload })
            .await
    }

    /// Non-blocking variant of `write_header_and_payload_control_inline32`.
    ///
    /// This avoids awaiting on the write queue and is useful for building sync `tell` APIs
    /// on top of the background writer task.
    pub fn write_header_and_payload_control_inline32_nonblocking(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderInline32 { header, payload })
    }

    /// Non-blocking variant of `write_header_and_payload_control_inline`.
    pub fn write_header_and_payload_control_inline_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
    }

    pub fn write_header_and_payload_control_inline_immediate_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_immediate_write_nonblocking(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
    }

    pub async fn write_header_and_payload_ask(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderPayload { header, payload })
            .await
    }

    /// Write header + payload inline for ask messages.
    pub async fn write_header_and_payload_ask_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
        .await
    }

    pub async fn write_header_and_payload_ask_inline32(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderInline32 { header, payload })
            .await
    }

    /// Write DirectAsk header + payload inline (fast path for direct ask)
    /// Wire format: [length:4][type:1][correlation_id:4][payload_len:4][payload:N]
    pub async fn write_direct_ask_inline(
        &self,
        header: [u8; 16], // DIRECT_ASK_FRAME_HEADER_LEN
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::DirectAskInline { header, payload })
            .await
    }

    /// Write DirectResponse inline (same format as DirectAsk)
    /// Wire format: [length:4][type:1][correlation_id:4][payload_len:4][payload:N]
    pub async fn write_direct_response_inline(
        &self,
        header: [u8; 16], // DIRECT_RESPONSE_FRAME_HEADER_LEN
        payload: bytes::Bytes,
    ) -> Result<()> {
        // DirectResponse has same wire format as DirectAsk, so reuse the implementation
        self.write_direct_ask_inline(header, payload).await
    }

    pub async fn write_pooled_control(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderPooled {
            header,
            prefix,
            payload,
        })
        .await
    }

    pub async fn write_pooled_control_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
        .await
    }

    pub fn write_pooled_control_inline_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
    }

    pub fn write_pooled_control_inline_immediate_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_immediate_write_nonblocking(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
    }

    pub async fn write_pooled_ask(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderPooled {
            header,
            prefix,
            payload,
        })
        .await
    }

    pub async fn write_pooled_ask_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
        .await
    }

    /// Send a generic `Buf` payload, deriving the declared length from
    /// `buf.remaining()` itself. A single-argument call site cannot express
    /// the mismatch `reject_oversize_write_payload` guards against: that
    /// check exists because a header can be built from one length while a
    /// *separately supplied* `buf` carries a different one, and here there
    /// is no second, independent length to disagree with `buf` in the
    /// first place -- `remaining()` *is* the declared length. This is a
    /// genuinely safe call shape, not a validation-skipping stub. For a
    /// caller that builds a header from a length declared independently of
    /// `buf` (see `WritePayload::Buf`), use `write_buf_control_checked`
    /// instead, which validates the two against each other.
    pub async fn write_buf_control<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let expected_len = buf.remaining();
        self.enqueue_write(WritePayload::Buf {
            buf: Box::new(buf),
            expected_len,
        })
        .await
    }

    /// Checked sibling of `write_buf_control` for a caller that builds its
    /// header from an `expected_len` declared independently of `buf` (see
    /// `WritePayload::Buf`). `reject_oversize_write_payload` rejects the
    /// write outright if `buf.remaining()` disagrees with it -- a mismatch
    /// here means the header this `buf` was chained onto promises a
    /// different body than what is actually being written, desyncing the
    /// peer's parser, not just sending an oversize-but-well-formed frame.
    pub async fn write_buf_control_checked<B>(&self, buf: B, expected_len: usize) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        self.enqueue_write(WritePayload::Buf {
            buf: Box::new(buf),
            expected_len,
        })
        .await
    }

    /// See `write_buf_control` above.
    pub async fn write_buf_ask<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let expected_len = buf.remaining();
        self.enqueue_ask_write(WritePayload::Buf {
            buf: Box::new(buf),
            expected_len,
        })
        .await
    }

    /// See `write_buf_control_checked` above.
    pub async fn write_buf_ask_checked<B>(&self, buf: B, expected_len: usize) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        self.enqueue_ask_write(WritePayload::Buf {
            buf: Box::new(buf),
            expected_len,
        })
        .await
    }

    /// Enqueue bytes for the background writer (non-blocking).
    pub fn write_bytes_nonblocking(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::Single(data))
    }

    /// See `write_bytes_nonblocking` above for the opaque lane. This is the
    /// counterpart for a caller that is declaring `data` is one or more
    /// complete V5 frames -- see `WritePayload::Framed`'s doc comment.
    pub fn write_framed_bytes_nonblocking(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::Framed(data))
    }

    /// Enqueue header + payload without concatenating.
    pub fn write_header_and_payload_nonblocking(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderPayload { header, payload })
    }

    /// Enqueue header + payload (legacy name kept for compatibility).
    pub fn write_header_and_payload_nonblocking_checked(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderPayload { header, payload })
    }

    /// Flush the writer immediately - used for low-latency ask operations
    pub fn flush_immediately(&self) -> Result<()> {
        // Coalesce flush requests to avoid flooding the writer task.
        if self
            .flush_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            match self.streaming_queue.try_push(StreamingCommand::Flush) {
                Ok(()) => Ok(()),
                Err(_) => {
                    // Queue is full (backpressure). Drop the explicit flush request and allow
                    // the writer's periodic/threshold flush to handle it.
                    self.flush_pending.store(false, Ordering::Release);
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    /// Write data with vectored batching - still no blocking
    pub fn write_vectored_nonblocking(&self, data_chunks: &[&[u8]]) -> Result<()> {
        if data_chunks.is_empty() {
            return Ok(());
        }

        // Use BytesMut for efficient concatenation
        let total_len: usize = data_chunks.iter().map(|chunk| chunk.len()).sum();
        let mut combined_buffer = bytes::BytesMut::with_capacity(total_len);

        for chunk in data_chunks {
            combined_buffer.extend_from_slice(chunk); // ALLOW_COPY
        }

        self.write_bytes_nonblocking(combined_buffer.freeze())
    }

    /// Write large data in chunks to avoid blocking.
    ///
    /// PR #183 review, round 7: the size gate lives at the granularity of
    /// one enqueued `WritePayload` -- and each chunk here used to be its
    /// own separate `Single` write. A chunk holding only the beginning of
    /// the data has no way to see the whole write's total length; it can
    /// only see its own (small, chunk-sized) length, so a write whose
    /// total exceeds `max_message_size` could be split into pieces every
    /// one of which passed the per-fragment check. A fragment has no
    /// independent meaning to validate -- the length that matters belongs
    /// to the whole logical write, so this validates `data` once, as a
    /// whole, *before* any chunking happens.
    ///
    /// PR #183 review, round 11: that up-front check is
    /// `reject_oversize_opaque` (a bare length ceiling), the same one
    /// `write_bytes_nonblocking` applies to a `Single` write -- not
    /// `reject_oversize_framed`'s per-frame walk. `data` here is exactly
    /// as opaque to this method as it is to `write_bytes_nonblocking`;
    /// nothing about calling this in chunks makes it more likely to be a
    /// V5 frame, and parsing it as one would reject legitimate opaque data
    /// whose bytes happen to look like an incomplete frame, the same
    /// content-sniffing bug that motivated splitting `Framed` out of
    /// `Single` in the first place. Round 7's actual requirement was
    /// "check the total length once, before chunking, instead of per
    /// fragment" -- a bare length ceiling on the whole buffer satisfies
    /// that exactly as well as a frame walk would, without inferring
    /// anything about the content's shape.
    ///
    /// Once `data` has passed that check, each chunk is enqueued as
    /// `TrustedFrame` rather than `Single` -- re-running any size gate on
    /// an already-validated slice would be meaningless (see above), and
    /// could even reject a fragment of legitimately-sized content if
    /// `chunk_size` happened to exceed `max_message_size`. See
    /// `WritePayload::TrustedFrame`'s doc comment for the other callers
    /// this same trust boundary applies to.
    ///
    /// Returns the first enqueue error encountered, rather than the
    /// previous unconditional `Ok(())` that discarded every per-chunk
    /// result -- a discarded `MessageTooLarge`, queue-full, or
    /// connection-closed error here meant the caller was told a write
    /// succeeded when part or all of it never reached the wire.
    ///
    /// PR #183 review, round 9: if a later chunk fails after earlier chunks
    /// were already enqueued, those earlier bytes are already queued for
    /// (or already on) the wire -- the peer has received, or will receive,
    /// part of a frame it will never get the rest of. Saying "the caller
    /// must treat this as fatal" in a doc comment does not make it true: a
    /// caller that merely propagates the error and keeps using this handle
    /// would let its next write get appended right where this frame's
    /// missing tail should have been, desyncing the peer's parser exactly
    /// the way this whole review cycle has been closing off. So this
    /// enforces it instead of documenting it -- once at least one chunk has
    /// been enqueued, any later failure calls `shutdown()` before
    /// returning the error. Every `enqueue_*` method checks
    /// `shutdown_signal` before doing anything else, so no further write on
    /// this handle can succeed after that -- the connection is poisoned,
    /// not merely reported as failed, and the writer task tears it down
    /// once it observes the signal.
    ///
    /// This is the "poison the connection" resolution, not "reserve
    /// capacity up front": `WriteQueue` (`crossbeam_queue::ArrayQueue`) has
    /// no reservation primitive, and building one that is actually correct
    /// against concurrent producers would mean changing the admission
    /// protocol every `enqueue_*` method shares -- a materially larger,
    /// riskier change than this call site (which nothing in this crate
    /// currently calls) justifies. A failure on the very first chunk is
    /// still a clean pre-write rejection with nothing poisoned, since
    /// nothing has been enqueued yet.
    pub fn write_chunked_nonblocking(&self, data: &[u8], chunk_size: usize) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.reject_oversize_opaque(data)?;

        let mut enqueued_any = false;
        for chunk in data.chunks(chunk_size) {
            match self.enqueue_write_nonblocking(WritePayload::TrustedFrame(
                bytes::Bytes::copy_from_slice(chunk), /* ALLOW_COPY */
            )) {
                Ok(()) => enqueued_any = true,
                Err(err) => {
                    if enqueued_any {
                        self.shutdown();
                    }
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    /// Get channel ID
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Get total bytes written
    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// True when a streaming send/response is in progress and the IO task will avoid
    /// draining the normal payload queue (streaming takes exclusive priority).
    pub fn is_streaming_active(&self) -> bool {
        self.streaming_active.load(Ordering::Acquire)
    }

    /// Get sequence counter
    pub fn sequence_number(&self) -> usize {
        self.sequence_counter.load(Ordering::Relaxed)
    }

    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Mark this connection shut down and wake any writer parked in the idle
    /// select so it observes the flag at the next loop iteration. Without the
    /// wake, an idle connection's writer never re-checks `shutdown_signal` and
    /// `wait_for_exit` / `task.await` hangs forever (R-2). The queue data
    /// notifiers are consumer-only, so these permits are never stolen by a
    /// producer (producers wait on `space_notify`).
    fn signal_shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Release);
        self.write_queue.wake_consumer();
        self.streaming_queue.wake_consumer();
    }

    /// Shutdown the background writer task
    pub fn shutdown(&self) {
        self.signal_shutdown();
    }

    /// Wait until the IO task exits.
    pub async fn wait_for_exit(&self) {
        while !self.exit_flag.load(Ordering::Acquire) {
            self.exit_notify.notified().await;
        }
    }

    /// Get the streaming threshold for this connection
    ///
    /// Returns the threshold above which messages should be streamed rather than
    /// queued through the background writer. This is always derived from the buffer size.
    pub fn streaming_threshold(&self) -> usize {
        self.buffer_config.streaming_threshold()
    }

    /// The peer's configured frame-body-length ceiling (see the `max_message_size`
    /// field doc). Inline (non-streaming) sends must stay at or under this before
    /// a header is even built: the receiver hard-rejects anything larger as a
    /// fatal read error and tears the whole connection down.
    pub(crate) fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    pub fn schema_hash(&self) -> Option<u64> {
        self.schema_hash
    }

    fn max_stream_chunk_size(&self) -> Result<usize> {
        // The first request frame has the largest V5 stream metadata header;
        // every chunk must fit even when it is carried by StreamStartData.
        const STREAM_FRAME_OVERHEAD: usize = crate::framing::STREAM_REQUEST_START_HEADER_LEN;

        let max_chunk = self.max_message_size.saturating_sub(STREAM_FRAME_OVERHEAD);
        if max_chunk == 0 {
            return Err(GossipError::InvalidConfig(format!(
                "max_message_size={} too small for streaming (overhead={})",
                self.max_message_size, STREAM_FRAME_OVERHEAD
            )));
        }
        Ok(std::cmp::min(STREAM_CHUNK_SIZE, max_chunk))
    }

    /// ACTOR_REM_2 R16e: acquire exclusive streaming mode on this handle without
    /// busy-spinning. A concurrent streamer parks on the single-permit gate (FIFO,
    /// no CPU burn) instead of looping `compare_exchange` + `yield_now` for the
    /// holder's entire stream. The returned guard releases the permit and clears
    /// the `streaming_active` observability flag on drop.
    async fn acquire_streaming_mode(&self) -> Result<StreamingGuard> {
        let permit = self
            .stream_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| GossipError::Shutdown)?;
        self.streaming_active.store(true, Ordering::Release);
        Ok(StreamingGuard {
            flag: self.streaming_active.clone(),
            _permit: permit,
        })
    }

    /// Canonical owned-Bytes request/tell stream path. Payload bytes are only
    /// sliced for vectored TLS writes; no application payload copy is made.
    pub async fn stream_large_message_bytes(
        &self,
        payload: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let chunk_size = self.max_stream_chunk_size()?;
        let _guard = self.acquire_streaming_mode().await?;
        let stream_id = self.allocate_stream_id()?;
        // R-9: reject locally at MAX_STREAM_SIZE — receivers hard-reject a larger
        // stream as a FATAL error, so sending it would tear the connection down.
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
        self.streaming_queue.push(StreamingCommand::VectoredWrite(VectoredSendItem {
            header: InlineFrameHeader::from_array(crate::framing::try_write_stream_request_start_header(
                stream_id,
                0,
                total_size,
                actor_id,
                type_hash,
                first_len,
            )?),
            payload: payload.slice(..first_len),
        })).await?;
        let mut abort_guard = StreamAbortGuard::new(self, stream_id);
        let mut offset = first_len;
        let mut index = 1u32;
        while offset < payload.len() {
            let end = (offset + chunk_size).min(payload.len());
            self.streaming_queue.push(StreamingCommand::VectoredWrite(VectoredSendItem {
                header: InlineFrameHeader::from_array(crate::framing::try_write_stream_data_header(
                    false,
                    stream_id,
                    index,
                    end - offset,
                )?),
                payload: payload.slice(offset..end),
            })).await?;
            offset = end;
            index = index.checked_add(1).ok_or_else(|| GossipError::Network(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "stream chunk index exhausted"),
            ))?;
        }
        self.streaming_queue.push(StreamingCommand::Flush).await?;
        abort_guard.disarm();
        Ok(())
    }

    /// Copying convenience wrapper for callers that only own a slice.
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        self.stream_large_message_bytes(bytes::Bytes::copy_from_slice(msg), type_hash, actor_id)
            .await
    }

    /// Stream a response back to the caller, using streaming protocol for large payloads.
    /// This is used when a response exceeds the streaming threshold.
    ///
    /// NOTE: This method copies the payload. For zero-copy streaming responses,
    /// use `stream_response_bytes` with pre-owned `Bytes` instead.
    ///
    /// # Arguments
    /// * `payload` - The response payload bytes
    /// * `correlation_id` - The correlation ID from the original request (for response matching)
    pub async fn stream_response(&self, payload: &[u8], correlation_id: u32) -> Result<()> {
        // Convert to Bytes and use zero-copy implementation
        self.stream_response_bytes(bytes::Bytes::copy_from_slice(payload), correlation_id) // ALLOW_COPY
            .await
    }

    /// Stream an owned response using V5 StartData plus zero-copy data chunks.
    pub async fn stream_response_bytes(
        &self,
        payload: bytes::Bytes,
        correlation_id: u32,
    ) -> Result<()> {
        if payload.is_empty() {
            return self.write_response_inline(payload, correlation_id).await;
        }
        let chunk_size = self.max_stream_chunk_size()?;
        let _guard = self.acquire_streaming_mode().await?;
        let stream_id = self.allocate_stream_id()?;
        // R-9: reject locally at MAX_STREAM_SIZE — receivers hard-reject a larger
        // stream as a FATAL error, so sending it would tear the connection down.
        if payload.len() > crate::MAX_STREAM_SIZE {
            return Err(GossipError::MessageTooLarge {
                size: payload.len(),
                max: crate::MAX_STREAM_SIZE,
            });
        }
        let total_size = u32::try_from(payload.len()).map_err(|_| {
            GossipError::MessageTooLarge {
                size: payload.len(),
                max: crate::MAX_STREAM_SIZE,
            }
        })?;
        let first_len = payload.len().min(chunk_size);
        let first_header = crate::framing::try_write_stream_response_start_header(
            stream_id,
            correlation_id,
            total_size,
            first_len,
        )?;
        self.streaming_queue
            .push(StreamingCommand::VectoredWrite(VectoredSendItem {
                header: InlineFrameHeader::from_array(first_header),
                payload: payload.slice(..first_len),
            }))
            .await?;
        let mut abort_guard = StreamAbortGuard::new(self, stream_id);
        let mut offset = first_len;
        let mut index = 1u32;
        while offset < payload.len() {
            let end = (offset + chunk_size).min(payload.len());
            let header = crate::framing::try_write_stream_data_header(
                true,
                stream_id,
                index,
                end - offset,
            )?;
            self.streaming_queue
            .push(StreamingCommand::VectoredWrite(VectoredSendItem {
                    header: InlineFrameHeader::from_array(header),
                    payload: payload.slice(offset..end),
                }))
                .await?;
            offset = end;
            index = index.checked_add(1).ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "stream chunk index exhausted",
                ))
            })?;
        }
        self.streaming_queue.push(StreamingCommand::Flush).await?;
        abort_guard.disarm();
        Ok(())
    }

    /// Send a response, streaming it when it exceeds the streaming threshold
    /// (R-9) and otherwise using the inline write queue.
    /// Write a response as a single inline Response frame (the <= streaming
    /// threshold path). Shared by `send_response_auto(_bytes)` and the empty
    /// branch of `stream_response_bytes` so the auto-stream decision does not
    /// recurse between them (R-9).
    async fn write_response_inline(
        &self,
        payload: bytes::Bytes,
        correlation_id: u32,
    ) -> Result<()> {
        let header = framing::try_write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        )?;
        self.write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    /// R-9: stream whenever the payload exceeds the streaming threshold, OR
    /// its inline-encoded size (`ASK_RESPONSE_HEADER_LEN` + payload) would
    /// exceed `max_message_size`. The two thresholds are independent knobs
    /// (`streaming_threshold` comes from `BufferConfig`, `max_message_size`
    /// from the peer's negotiated config), so a small `max_message_size`
    /// alone -- a perfectly ordinary configuration, not an edge case --
    /// otherwise selects the inline branch for a payload the inline gate
    /// then has to refuse. That would trade "sends a frame the peer
    /// rejects" for "refuses to send a response streaming could have
    /// delivered fine in bounded chunks" -- a capability regression, not a
    /// fix. `MessageTooLarge` is reserved for what genuinely cannot be sent
    /// at all: `stream_response_bytes` itself still rejects payloads at or
    /// above `MAX_STREAM_SIZE`. Mirrors the direct read-loop path's
    /// `inline_payload_limit`/`should_stream` in `read_pipeline.rs`.
    fn should_stream_response(&self, payload_len: usize) -> bool {
        let inline_payload_limit = self
            .max_message_size
            .saturating_sub(framing::ASK_RESPONSE_HEADER_LEN);
        payload_len > self.streaming_threshold() || payload_len > inline_payload_limit
    }

    pub async fn send_response_auto(
        &self,
        payload: bytes::Bytes,
        correlation_id: u32,
    ) -> Result<()> {
        if self.should_stream_response(payload.len()) {
            return self.stream_response_bytes(payload, correlation_id).await;
        }
        self.write_response_inline(payload, correlation_id).await
    }

    /// Send a response with owned Bytes, streaming it when it exceeds the
    /// streaming threshold (R-9) and otherwise using the inline write queue.
    ///
    /// # Arguments
    /// * `correlation_id` - The correlation ID from the original request
    /// * `payload` - The response payload as owned Bytes
    ///
    /// # Returns
    /// Ok(()) on success, or an error if sending failed
    pub async fn send_response_auto_bytes(
        &self,
        correlation_id: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if self.should_stream_response(payload.len()) {
            return self.stream_response_bytes(payload, correlation_id).await;
        }
        self.write_response_inline(payload, correlation_id).await
    }

    /// Cold-path cancellation for a partially queued V5 stream.
    pub async fn abort_stream(&self, stream_id: u32, reason: u32) -> Result<()> {
        self.write_trusted_bytes_control(bytes::Bytes::copy_from_slice(
            &crate::framing::write_stream_abort_header(stream_id, reason),
        ))
        .await
    }

    /// Drop-safe cancellation for a partially queued V5 stream.
    ///
    /// The abort uses the same FIFO as stream chunks, so it is ordered after
    /// every chunk accepted before cancellation. If that FIFO cannot accept an
    /// abort, shut the transport down: a closed socket releases the peer's
    /// reassembly immediately, whereas silently losing the abort leaks it for
    /// the idle timeout.
    fn try_abort_stream(&self, stream_id: u32, reason: u32) {
        if self
            .streaming_queue
            .try_push(StreamingCommand::Abort { stream_id, reason })
            .is_err()
        {
            self.signal_shutdown();
        }
    }

    /// Zero-copy vectored write for header + payload in single operation
    /// This eliminates copying payload data into frame buffer - optimal for streaming
    pub async fn write_bytes_vectored<const N: usize>(
        &self,
        header: [u8; N],
        payload: bytes::Bytes,
    ) -> Result<()> {
        // Create vectored command that preserves both header and payload as separate Bytes
        let command = VectoredSendItem {
            header: InlineFrameHeader::from_array(header),
            payload,
        };

        // A stream's StartData and Data frames must share one FIFO queue.
        // Waiting for capacity preserves frame order under backpressure.
        self.streaming_queue
            .push(StreamingCommand::VectoredWrite(command))
            .await
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        // Send chunks as a batch via the bounded streaming queue for optimal vectored I/O.
        let command = StreamingCommand::OwnedChunks(chunks);
        self.streaming_queue
            .try_push(command)
            .map_err(|_| GossipError::WriteQueueFull)?;

        Ok(())
    }
}

/// Arms after `StreamStart` has entered the streaming FIFO. Dropping an async
/// sender while it is backpressured must release the peer's pre-allocation,
/// but `Drop` may not await. The guard therefore publishes an ordered abort on
/// the same queue and fails closed if the queue is no longer usable.
struct StreamAbortGuard<'a> {
    handle: &'a LockFreeStreamHandle,
    stream_id: u32,
    armed: bool,
}

impl<'a> StreamAbortGuard<'a> {
    fn new(handle: &'a LockFreeStreamHandle, stream_id: u32) -> Self {
        Self { handle, stream_id, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StreamAbortGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.handle.try_abort_stream(self.stream_id, 1);
        }
    }
}

/// Guard to ensure streaming_active is released on drop. Holds the streaming
/// gate permit (R16e) so the next waiter is admitted, and clears the
/// observability flag, when the stream finishes or is dropped/cancelled.
struct StreamingGuard {
    flag: Arc<AtomicBool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for StreamingGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

impl Debug for LockFreeStreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockFreeStreamHandle")
            .field("addr", &self.addr)
            .field("channel_id", &self.channel_id)
            .field("bytes_written", &self.bytes_written.load(Ordering::Relaxed))
            .field("sequence", &self.sequence_counter.load(Ordering::Relaxed))
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg(any(test, feature = "test-helpers", debug_assertions))]
#[allow(dead_code)]
enum DirectPayloadError {
    HeaderTooShort,
    PayloadTruncated { expected: usize, available: usize },
}

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
#[allow(dead_code)]
fn parse_direct_message_payload<'a>(
    msg_data: &'a [u8],
) -> std::result::Result<&'a [u8], DirectPayloadError> {
    if msg_data.len() < crate::framing::DIRECT_ASK_HEADER_LEN {
        return Err(DirectPayloadError::HeaderTooShort);
    }

    let payload_len =
        u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;
    let payload_start = crate::framing::DIRECT_ASK_HEADER_LEN;
    let payload_end = payload_start + payload_len;

    if msg_data.len() < payload_end {
        return Err(DirectPayloadError::PayloadTruncated {
            expected: payload_len,
            available: msg_data.len().saturating_sub(payload_start),
        });
    }

    Ok(&msg_data[payload_start..payload_end])
}

#[cfg(test)]
mod route_interning_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    async fn run_routed_ask_route_reuse_body() {
        let (client, mut peer) = tokio::io::duplex(1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9901".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );

        writer
            .write_routed_actor_ask(17, 7, 9, bytes::Bytes::from_static(b"one"))
            .await
            .unwrap();
        let mut bind = [0u8; crate::framing::ROUTE_BIND_FRAME_HEADER_LEN];
        peer.read_exact(&mut bind).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(bind[..4].try_into().unwrap()).unwrap().kind,
            crate::framing::WireKind::RouteBind
        );

        let mut first = [0u8; crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN + 3];
        peer.read_exact(&mut first).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(first[..4].try_into().unwrap()).unwrap().kind,
            crate::framing::WireKind::RoutedActorAsk
        );
        assert_eq!(&first[crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN..], b"one");

        writer
            .write_routed_actor_ask(18, 7, 9, bytes::Bytes::from_static(b"two"))
            .await
            .unwrap();
        let mut second = [0u8; crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN + 3];
        peer.read_exact(&mut second).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(second[..4].try_into().unwrap()).unwrap().kind,
            crate::framing::WireKind::RoutedActorAsk
        );
        assert_eq!(&second[crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN..], b"two");

        writer.shutdown();
        task.await.unwrap();
    }

    // R-2 (#144 regression): this test hung forever on remote main — every
    // routed ask and its bind drained fine, but `writer.shutdown()` only stored
    // the flag without waking the writer parked in the idle select, so
    // `task.await` never returned (plain `cargo test` never terminated). The
    // timeout converts any future regression from a suite-wide hang into a
    // clean test failure and STAYS after the fix.
    #[tokio::test]
    async fn first_routed_ask_binds_before_compact_frame_and_reuses_the_route() {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_routed_ask_route_reuse_body(),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "routed-ask route-reuse flow must not hang (R-2): shutdown must wake the parked writer"
        );
    }

    /// R-2: same route-reuse invariant on the multi-thread flavor, so the
    /// contract is pinned independently of the runtime flavor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn qa_r2_route_reuse_multithread() {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_routed_ask_route_reuse_body(),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "routed-ask route-reuse flow must not hang on the multi-thread runtime (R-2)"
        );
    }

    /// R-2: the exact lost-wakeup point — `shutdown()` must wake a writer parked
    /// in the idle select. Before the fix it only stored the flag, so a parked
    /// writer never re-checked it and `task` (the writer's JoinHandle) hung
    /// forever. Two `yield_now`s let the current-thread runtime run the spawned
    /// writer to its idle park point (no frames queued, no read context) before
    /// shutdown is called, making the regression deterministic rather than
    /// timing-dependent.
    #[tokio::test]
    async fn qa_r2_shutdown_wakes_parked_writer() {
        let (client, _peer) = tokio::io::duplex(128);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9903".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        writer.shutdown();
        let exited = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
        assert!(
            exited.is_ok(),
            "shutdown() must wake a writer parked in the idle select (R-2)"
        );
    }

    /// R-8: cancellation must be ordered after the accepted StreamStart. The
    /// abort used to go through the normal queue, which is serviced between
    /// streaming turns and could overtake queued stream data.
    #[tokio::test]
    async fn qa_r8_cancelled_stream_abort_follows_stream_start() {
        let (client, mut peer) = tokio::io::duplex(1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9905".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_id = 7;
        let start = crate::framing::try_write_stream_request_start_header(stream_id, 1, 1, 2, 3, 1)
            .unwrap();
        writer
            .streaming_queue
            .push(StreamingCommand::VectoredWrite(VectoredSendItem {
                header: InlineFrameHeader::from_array(start),
                payload: bytes::Bytes::from_static(b"x"),
            }))
            .await
            .unwrap();
        drop(StreamAbortGuard::new(&writer, stream_id));

        let mut received_start = vec![0u8; start.len() + 1];
        peer.read_exact(&mut received_start).await.unwrap();
        assert_eq!(&received_start[..start.len()], &start);
        assert_eq!(&received_start[start.len()..], b"x");

        let expected_abort = crate::framing::write_stream_abort_header(stream_id, 1);
        let mut abort = [0u8; crate::framing::STREAM_DATA_HEADER_LEN + crate::framing::LENGTH_PREFIX_LEN];
        peer.read_exact(&mut abort).await.unwrap();
        assert_eq!(abort, expected_abort);

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task)
            .await
            .expect("writer exits after R-8 cancellation test");
    }

    #[tokio::test]
    async fn failed_first_bind_does_not_publish_a_route_for_retry() {
        let (client, _peer) = tokio::io::duplex(128);
        let (writer, _task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9902".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        writer.shutdown();
        assert!(writer
            .write_routed_actor_ask(17, 7, 9, bytes::Bytes::from_static(b"one"))
            .await
            .is_err());
        let (_, retry_is_fresh) = writer
            .outbound_routes
            .slot_for(crate::route_interning::RouteKey { actor_id: 7, type_hash: 9 })
            .unwrap();
        assert!(retry_is_fresh, "failed bind must not publish its route");
    }

    /// P1: a full route table must fall back to the uncompact `ActorAsk`
    /// frame (which carries `actor_id`/`type_hash` directly and needs no
    /// connection-local slot) instead of permanently failing every new ask
    /// on the connection with a misleading `GossipError::Shutdown`.
    #[tokio::test]
    async fn route_table_full_falls_back_to_uncompact_actor_ask_instead_of_failing() {
        let (client, mut peer) = tokio::io::duplex(1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9910".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );

        for i in 0..crate::route_interning::MAX_ROUTES_PER_CONNECTION as u64 {
            writer
                .outbound_routes
                .slot_for(crate::route_interning::RouteKey {
                    actor_id: i,
                    type_hash: 1,
                })
                .expect("table has room until MAX_ROUTES_PER_CONNECTION");
        }

        let result = writer
            .write_routed_actor_ask(1, 999_999, 42, bytes::Bytes::from_static(b"payload"))
            .await;
        assert!(
            result.is_ok(),
            "a full route table must fall back to an uncompact ask, not fail the send: {result:?}"
        );

        let mut header = [0u8; crate::framing::ACTOR_ASK_FRAME_HEADER_LEN];
        peer.read_exact(&mut header).await.unwrap();
        let control =
            crate::framing::decode_control(header[..4].try_into().unwrap()).unwrap();
        assert_eq!(
            control.kind,
            crate::framing::WireKind::ActorAsk,
            "fallback must use the uncompact ActorAsk frame, which needs no connection-local slot"
        );
        let correlation_id = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let actor_id = u64::from_be_bytes(header[8..16].try_into().unwrap());
        let type_hash = u32::from_be_bytes(header[16..20].try_into().unwrap());
        assert_eq!(correlation_id, 1);
        assert_eq!(actor_id, 999_999);
        assert_eq!(type_hash, 42);

        let mut payload = [0u8; 7];
        peer.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"payload");

        writer.shutdown();
        task.await.unwrap();
    }

    /// P1: the fallback must be sustained, not a one-shot escape hatch — every
    /// subsequent new route on a full table must also succeed via fallback,
    /// existing (already-interned) routes must keep using the compact frame,
    /// and the connection must stay open throughout (no teardown).
    #[tokio::test]
    async fn route_table_full_keeps_connection_open_for_known_and_new_routes() {
        let (client, mut peer) = tokio::io::duplex(4096);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9911".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );

        for i in 0..crate::route_interning::MAX_ROUTES_PER_CONNECTION as u64 {
            writer
                .outbound_routes
                .slot_for(crate::route_interning::RouteKey {
                    actor_id: i,
                    type_hash: 1,
                })
                .expect("table has room until MAX_ROUTES_PER_CONNECTION");
        }

        // A known (already-interned) route still uses the compact frame.
        writer
            .write_routed_actor_ask(10, 0, 1, bytes::Bytes::from_static(b"known"))
            .await
            .expect("an already-interned route must not be affected by a full table");
        let mut known_header = [0u8; crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN];
        peer.read_exact(&mut known_header).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(known_header[..4].try_into().unwrap())
                .unwrap()
                .kind,
            crate::framing::WireKind::RoutedActorAsk,
            "an already-known route is unaffected by the table being full"
        );
        let mut known_payload = [0u8; 5];
        peer.read_exact(&mut known_payload).await.unwrap();
        assert_eq!(&known_payload, b"known");

        // Two distinct brand-new routes both fall back cleanly; the
        // connection must not be torn down after the first one.
        for (correlation_id, actor_id) in [(20u32, 1_000_000u64), (21u32, 1_000_001u64)] {
            let result = writer
                .write_routed_actor_ask(
                    correlation_id,
                    actor_id,
                    7,
                    bytes::Bytes::from_static(b"new"),
                )
                .await;
            assert!(
                result.is_ok(),
                "sustained fallback must keep succeeding for new routes: {result:?}"
            );
            let mut header = [0u8; crate::framing::ACTOR_ASK_FRAME_HEADER_LEN];
            peer.read_exact(&mut header).await.unwrap();
            assert_eq!(
                crate::framing::decode_control(header[..4].try_into().unwrap())
                    .unwrap()
                    .kind,
                crate::framing::WireKind::ActorAsk
            );
            let mut payload = [0u8; 3];
            peer.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"new");
        }

        assert!(
            !writer.shutdown_signal.load(std::sync::atomic::Ordering::Relaxed),
            "a full route table must never trigger connection shutdown"
        );

        writer.shutdown();
        task.await.unwrap();
    }

    /// R-3: cancelling `write_routed_actor_ask` while its RouteBind enqueue is
    /// parked on a full write queue must roll back the freshly-allocated route.
    /// Without the RAII guard the route stays marked bound, so the next ask
    /// sends a RoutedActorAsk for a slot the peer never learned -> the peer
    /// returns "unknown route slot" and tears the whole connection down.
    ///
    /// Current-thread flavor is deliberate: the spawned writer must NOT drain
    /// the queue during the test (otherwise it keeps a frame in-flight and
    /// leaves queue space, so the bind's `try_push` would succeed). None of the
    /// awaits below yield until the final `task.await`, so the writer never runs
    /// until cleanup and the queue stays genuinely full.
    #[tokio::test]
    async fn qa_r3_cancelled_bind_enqueue_unbinds_route() {
        let (client, peer) = tokio::io::duplex(8);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9904".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default().with_write_queue_capacity(128),
            None,
            None,
        );

        // Saturate the write queue. Each push completes without yielding while
        // there is space; a zero-duration timeout turns the parked (129th) push
        // into a clean "queue is full" signal — no sleep-based synchronization.
        // PR #183 review, round 11: `write_bytes_control` is the opaque
        // `Single` lane -- content is never inspected, so this plain
        // literal (which happens to decode as a valid-but-far-too-large V5
        // control word if it were parsed) is unaffected either way.
        let pad = bytes::Bytes::from_static(b"padpadpadpad");
        while let Ok(Ok(())) = tokio::time::timeout(
            std::time::Duration::from_millis(0),
            writer.write_bytes_control(pad.clone()),
        )
        .await
        {}

        // A routed ask for a fresh route: slot_for marks it bound, then the
        // RouteBind enqueue parks on the full queue and is cancelled at once.
        let route = crate::route_interning::RouteKey { actor_id: 42, type_hash: 7 };
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(0),
            writer.write_routed_actor_ask(1, 42, 7, bytes::Bytes::from_static(b"ask")),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "the RouteBind enqueue should park on the full write queue"
        );

        // R-3: the cancelled fresh allocation must roll back, so the next
        // slot_for still needs a bind. Before the fix the route stayed bound
        // (needs_bind == false) and the next ask sent RoutedActorAsk for a slot
        // the peer never learned.
        let (_, needs_bind) = writer.outbound_routes.slot_for(route).unwrap();
        assert!(
            needs_bind,
            "cancelled bind enqueue must roll back the route (R-3)"
        );

        // Cleanup: drop the unread peer half so the writer's blocked write
        // errors and the task exits; the current-thread runtime drains it at
        // task.await.
        drop(peer);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// R-5: per-handle stream ids occupy the odd partition (disjoint from the
    /// direct-response allocator's even ids), so they can never collide on
    /// `stream_id` with a direct streaming response on the same connection.
    #[tokio::test]
    async fn qa_r5_handle_stream_ids_are_odd() {
        let (client, _peer) = tokio::io::duplex(64);
        let (writer, _task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9905".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let mut ids = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let id = writer.allocate_stream_id().unwrap();
            assert!(
                id != 0 && id % 2 == 1,
                "handle stream id must be odd and nonzero, got {id}"
            );
            assert!(ids.insert(id), "handle stream id reused: {id}");
        }
    }

    /// R-5 (review follow-up): the per-handle allocator must exhaust at the
    /// u32::MAX sentinel WITHOUT wrapping back to 1 (which would reuse id 1 on
    /// a still-live connection). u32::MAX - 2 is the last id handed out;
    /// u32::MAX is the sentinel that shuts the connection down.
    #[tokio::test]
    async fn qa_r5_handle_allocator_exhausts_without_wrapping_to_one() {
        let (client, _peer) = tokio::io::duplex(64);
        let (writer, _task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9906".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        // Park the allocator one id before the sentinel.
        writer
            .next_stream_id
            .store(u32::MAX - 2, Ordering::Release);
        assert_eq!(writer.allocate_stream_id().unwrap(), u32::MAX - 2);
        // u32::MAX is the sentinel: exhaustion -> Shutdown, no wrap to 1.
        assert!(writer.allocate_stream_id().is_err());
        // Subsequent allocations stay shut down (never hand out id 1 again).
        assert!(writer.allocate_stream_id().is_err());
        assert_eq!(
            writer.next_stream_id.load(Ordering::Acquire),
            u32::MAX,
            "counter must rest on the sentinel, not wrap to 1"
        );
    }

    /// R-9: a streaming payload larger than MAX_STREAM_SIZE must fail locally
    /// (MessageTooLarge) before any frame is emitted -- every receiver hard-
    /// rejects such a stream as a FATAL error, so sending it would tear the
    /// connection down with collateral loss.
    #[tokio::test]
    async fn qa_r9_oversized_stream_fails_locally() {
        let (client, _peer) = tokio::io::duplex(64);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9907".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let oversized = bytes::BytesMut::zeroed(crate::MAX_STREAM_SIZE + 1).freeze();
        let err = writer
            .stream_large_message_bytes(oversized, 1, 7)
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// R-9: a deferred reply above the streaming threshold is streamed (a
    /// StreamResponseStart frame), not written as one inline Response frame
    /// (which the peer would reject as MessageTooLarge, and which would panic
    /// the frame header length check at >= 2^27 bytes).
    #[tokio::test]
    async fn qa_r9_large_deferred_reply_streams_not_inlines() {
        let (client, peer) = tokio::io::duplex(8 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9908".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        // Above the default ~1MB streaming threshold.
        let payload = bytes::BytesMut::zeroed(2 * 1024 * 1024).freeze();
        writer.send_response_auto_bytes(42, payload).await.unwrap();
        // The first frame on the wire must be a StreamResponseStart, not an
        // inline Response.
        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        let mut peer = peer;
        peer.read_exact(&mut ctrl).await.unwrap();
        let kind = crate::framing::decode_control(ctrl).unwrap().kind;
        assert_eq!(
            kind,
            crate::framing::WireKind::StreamResponseStart,
            "large deferred reply must stream (R-9)"
        );
        writer.shutdown();
        drop(peer);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }
}

/// PR #183 review: the size gate landed only on `ConnectionHandle`'s entry
/// points. Every caller that reaches `LockFreeStreamHandle` some other way
/// (`stream_writer.rs`'s own `write_response_inline`/`send_response_auto*`,
/// `ask_responder`'s pooled/nonblocking lanes, `pool_connect.rs`'s gossip
/// responses, the debug-only raw-ask echo in `handle.rs`) built a header via
/// a `write_*_header` call (which only enforces the V5 27-bit wire ceiling)
/// and enqueued it directly, with no `max_message_size` check anywhere on
/// that path. These tests exercise `reject_oversize_write_payload` -- the
/// backstop in `enqueue_write`/`enqueue_ask_write`/
/// `enqueue_write_nonblocking`/`enqueue_immediate_write_nonblocking` -- at
/// the entry points those bypassing callers actually use, so a future
/// caller added anywhere in the crate inherits the gate automatically
/// instead of needing its own copy.
#[cfg(test)]
mod write_payload_size_gate_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn small_message_read_context(port: u16, max_message_size: usize) -> ReadContext {
        ReadContext {
            streaming_state_handoff: None,
            registry_weak: std::sync::Weak::new(),
            peer_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            session_source: format!("127.0.0.1:{port}").parse().unwrap(),
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
        }
    }

    /// PR #183 review, round 3: `max_message_size` (via `ReadContext`) is
    /// set far below the default streaming threshold (~1 MiB, from
    /// `BufferConfig::default`). A payload comfortably under the streaming
    /// threshold but whose inline-encoded body would exceed
    /// `max_message_size` must still be *delivered* -- streamed in bounded
    /// chunks -- not refused. Rejecting it outright (the first version of
    /// this gate) traded "sends a frame the peer rejects" for "refuses to
    /// send a response streaming could deliver fine": a capability
    /// regression, not a fix.
    #[tokio::test]
    async fn auto_response_streams_when_inline_would_exceed_max_message_size() {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 128;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9950".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9950, max_message_size)),
        );
        assert!(
            max_message_size < writer.streaming_threshold(),
            "test setup: max_message_size must sit below the streaming \
             threshold so this is the case that used to reach the inline path"
        );

        // Comfortably under the streaming threshold, but
        // ASK_RESPONSE_HEADER_LEN + payload_len > max_message_size.
        let payload_len = max_message_size;
        assert!(
            crate::framing::ASK_RESPONSE_HEADER_LEN + payload_len > max_message_size,
            "test setup: payload must not fit inline under max_message_size"
        );
        let payload = bytes::Bytes::from(vec![7u8; payload_len]);
        writer
            .send_response_auto_bytes(1, payload.clone())
            .await
            .expect("a payload streaming can deliver must not be refused");

        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        peer.read_exact(&mut ctrl).await.unwrap();
        let control = crate::framing::decode_control(ctrl).unwrap();
        assert_eq!(
            control.kind,
            crate::framing::WireKind::StreamResponseStart,
            "must stream, not attempt (and fail) an inline Response frame"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// The other half of the same fix: a payload that genuinely cannot be
    /// sent at all -- at or above `MAX_STREAM_SIZE`, so not even streaming
    /// can deliver it -- must still be rejected locally with
    /// `MessageTooLarge`, exactly as before.
    #[tokio::test]
    async fn auto_response_over_max_stream_size_is_still_rejected() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9953".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let oversize = bytes::Bytes::from(vec![0u8; crate::MAX_STREAM_SIZE + 1]);
        let err = writer
            .send_response_auto_bytes(1, oversize)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// `write_header_and_payload_control` (the `Bytes`-header `HeaderPayload`
    /// primitive `pool_connect.rs`'s gossip-response sends and the
    /// debug-only raw-ask echo in `handle.rs` use) has no `max_message_size`
    /// pre-check of its own anywhere upstream of it -- this proves the same
    /// backstop covers it too.
    #[tokio::test]
    async fn header_payload_over_max_message_size_is_rejected() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9951".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9951, max_message_size)),
        );

        let payload_len = max_message_size - crate::framing::GOSSIP_HEADER_LEN + 1;
        let payload = bytes::Bytes::from(vec![0u8; payload_len]);
        let header = bytes::Bytes::copy_from_slice(
            &crate::framing::try_write_gossip_frame_prefix(payload.len()).unwrap(),
        );
        let err = writer
            .write_header_and_payload_control(header, payload)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// A rejected oversize write must not bump `sequence_number` -- mirroring
    /// `raw_tell_oversize_never_enqueues_a_corrupted_frame`
    /// (`connection_pool/handle.rs`) for this backstop: nothing was ever
    /// queued, so the observability counter must not move either. Uses
    /// `write_header_and_payload_control` directly (bypassing
    /// `send_response_auto_bytes`'s stream-or-inline decision entirely) so
    /// this stays a test of the `enqueue_write` choke point itself, not of
    /// which path a caller's payload size happens to route through.
    #[tokio::test]
    async fn rejected_oversize_write_does_not_advance_sequence_number() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9952".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9952, max_message_size)),
        );

        let before = writer.sequence_number();
        let payload_len = max_message_size - crate::framing::GOSSIP_HEADER_LEN + 1;
        let payload = bytes::Bytes::from(vec![0u8; payload_len]);
        let header = bytes::Bytes::copy_from_slice(
            &crate::framing::try_write_gossip_frame_prefix(payload.len()).unwrap(),
        );
        let err = writer
            .write_header_and_payload_control(header, payload)
            .await
            .unwrap_err();
        assert!(matches!(err, GossipError::MessageTooLarge { .. }));
        assert_eq!(
            writer.sequence_number(),
            before,
            "an oversized inline write must be rejected before anything is queued"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// PR #183 review, round 5: `WritePayload::Single` -- constructed by the
    /// public, generic `write_bytes_nonblocking`/`write_bytes_control`/
    /// `write_bytes_ask` entry points -- used to be exempt from
    /// `reject_oversize_write_payload` entirely. A caller can hand
    /// `write_bytes_nonblocking` a complete, well-formed V5 frame (a real
    /// control word plus a payload) it built by hand; the early-return
    /// exemption let that frame's declared body exceed `max_message_size`
    /// and reach the wire regardless, where the peer fatally rejects it.
    /// This builds exactly that: a genuine self-contained Gossip frame
    /// whose control word declares a body over `max_message_size`, passed
    /// as one `Bytes` blob to the public entry point, not to any internal
    /// primitive.
    ///
    /// PR #183 review, round 11: `write_bytes_nonblocking`'s `Single` lane
    /// is opaque now (see the module doc comment above
    /// `reject_oversize_write_payload`), so this frame is rejected by the
    /// same bare length ceiling any other oversized opaque blob would hit
    /// -- not because its control word is decoded and its `body_len`
    /// checked. The "sanity" assertions below still confirm the setup is a
    /// well-formed frame (useful documentation of the shape), but that
    /// structure is incidental to why this call is rejected now.
    #[tokio::test]
    async fn write_bytes_nonblocking_rejects_a_genuine_oversize_self_contained_frame() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9953".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9953, 64)),
        );

        let payload_len = 64 - crate::framing::GOSSIP_HEADER_LEN + 1;
        let payload = vec![0u8; payload_len];
        let header = crate::framing::write_gossip_frame_prefix(payload.len());
        let mut frame = Vec::with_capacity(header.len() + payload.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&payload);
        // Sanity: this really is one genuine, complete, self-contained V5
        // frame whose declared body exceeds max_message_size -- not just an
        // oversize blob of unstructured bytes. (Incidental to the
        // rejection now -- see the doc comment above.)
        let control = crate::framing::decode_control(frame[..4].try_into().unwrap()).unwrap();
        assert_eq!(control.kind, crate::framing::WireKind::Gossip);
        assert_eq!(
            control.body_len,
            crate::framing::GOSSIP_HEADER_LEN + payload_len
        );
        assert!(control.body_len > 64);

        let err = writer
            .write_bytes_nonblocking(bytes::Bytes::from(frame))
            .unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// The same gate must not false-reject a normal-size `Single` write --
    /// proving the fix is a real ceiling, not an unconditional rejection.
    #[tokio::test]
    async fn write_bytes_nonblocking_accepts_a_frame_within_max_message_size() {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9954".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9954, 64)),
        );

        let payload_len = 64 - crate::framing::GOSSIP_HEADER_LEN;
        let payload = vec![7u8; payload_len];
        let header = crate::framing::write_gossip_frame_prefix(payload.len());
        let mut frame = Vec::with_capacity(header.len() + payload.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&payload);
        let expected = frame.clone();

        writer
            .write_bytes_nonblocking(bytes::Bytes::from(frame))
            .expect("a frame within max_message_size must be accepted");

        let mut received = vec![0u8; expected.len()];
        AsyncReadExt::read_exact(&mut peer, &mut received)
            .await
            .expect("connection must deliver the frame to the peer");
        assert_eq!(received, expected);

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// PR #183 review, round 6 (moved to the `Framed` lane in round 11):
    /// two individually-valid frames concatenated into one `Framed` write
    /// must not be rejected just because their *aggregate* length exceeds
    /// `max_message_size` -- that bound applies per frame, the same way it
    /// would if these had been two separate `write_framed_bytes_nonblocking`
    /// calls. This is the shape that distinguishes a whole-buffer length
    /// ceiling (over-rejects this) from a per-frame walk (accepts it): each
    /// frame here is exactly at the 64-byte limit on its own, but the
    /// concatenated buffer is 136 bytes -- comfortably past
    /// `max_message_size + LENGTH_PREFIX_LEN` (68).
    ///
    /// This lives on `write_framed_bytes_nonblocking`, not
    /// `write_bytes_nonblocking`, because round 11 moved the per-frame walk
    /// off the opaque `Single` lane entirely -- see the module doc comment
    /// above `reject_oversize_write_payload` and
    /// `write_bytes_nonblocking_judges_two_concatenated_frames_by_total_length_not_by_parsing_them`
    /// below for the opaque lane's equivalent (judged by length alone,
    /// content never inspected).
    #[tokio::test]
    async fn write_framed_bytes_nonblocking_accepts_two_valid_frames_whose_aggregate_exceeds_max_message_size()
     {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9955".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9955, max_message_size)),
        );

        let payload_len = max_message_size - crate::framing::GOSSIP_HEADER_LEN;
        let make_frame = |fill: u8| {
            let payload = vec![fill; payload_len];
            let header = crate::framing::write_gossip_frame_prefix(payload.len());
            let mut frame = Vec::with_capacity(header.len() + payload.len());
            frame.extend_from_slice(&header);
            frame.extend_from_slice(&payload);
            frame
        };
        let frame_a = make_frame(1);
        let frame_b = make_frame(2);
        assert_eq!(
            crate::framing::decode_control(frame_a[..4].try_into().unwrap())
                .unwrap()
                .body_len,
            max_message_size,
            "test setup: each frame's own body must sit exactly at the limit"
        );

        let mut both = Vec::with_capacity(frame_a.len() + frame_b.len());
        both.extend_from_slice(&frame_a);
        both.extend_from_slice(&frame_b);
        assert!(
            both.len() > max_message_size + crate::framing::LENGTH_PREFIX_LEN,
            "test setup: the concatenated buffer must exceed a whole-buffer ceiling"
        );
        let expected = both.clone();

        writer
            .write_framed_bytes_nonblocking(bytes::Bytes::from(both))
            .expect(
                "two frames each within max_message_size must be accepted, even though \
                 their concatenated length is not",
            );

        let mut received = vec![0u8; expected.len()];
        AsyncReadExt::read_exact(&mut peer, &mut received)
            .await
            .expect("connection must deliver both frames to the peer");
        assert_eq!(received, expected);

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// The per-frame walk must still catch a genuinely oversize frame when
    /// it is not alone in the buffer -- proving round 6's fix widens what
    /// is *accepted*, not what is *checked*. A valid frame followed by one
    /// whose own declared body exceeds `max_message_size` must still be
    /// refused, exactly as it would if the oversize frame had been sent by
    /// itself. Moved to `write_framed_bytes_nonblocking` in round 11, same
    /// reasoning as the aggregate-acceptance test above.
    #[tokio::test]
    async fn write_framed_bytes_nonblocking_rejects_an_oversize_frame_following_a_valid_one() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9956".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9956, max_message_size)),
        );

        let valid_payload_len = max_message_size - crate::framing::GOSSIP_HEADER_LEN;
        let valid_header = crate::framing::write_gossip_frame_prefix(valid_payload_len);
        let mut valid_frame = Vec::with_capacity(valid_header.len() + valid_payload_len);
        valid_frame.extend_from_slice(&valid_header);
        valid_frame.extend_from_slice(&vec![1u8; valid_payload_len]);

        let oversize_payload_len = max_message_size - crate::framing::GOSSIP_HEADER_LEN + 1;
        let oversize_header = crate::framing::write_gossip_frame_prefix(oversize_payload_len);
        let mut oversize_frame = Vec::with_capacity(oversize_header.len() + oversize_payload_len);
        oversize_frame.extend_from_slice(&oversize_header);
        oversize_frame.extend_from_slice(&vec![2u8; oversize_payload_len]);

        let mut both = Vec::with_capacity(valid_frame.len() + oversize_frame.len());
        both.extend_from_slice(&valid_frame);
        both.extend_from_slice(&oversize_frame);

        let err = writer
            .write_framed_bytes_nonblocking(bytes::Bytes::from(both))
            .unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// PR #183 review, round 11: the coordinator's exact evidence for why
    /// content-sniffing was the wrong tool -- the *same* two-valid-frames
    /// buffer that `write_framed_bytes_nonblocking` correctly parses and
    /// accepts above must, on the opaque `write_bytes_nonblocking` lane,
    /// be judged purely on total length and nothing else. Its total length
    /// (136 bytes) exceeds `max_message_size` (64), so the opaque lane
    /// rejects it -- not because it "looks like two frames", but because
    /// `Single` never looks at content at all, and 136 bytes of *anything*
    /// opaque exceeds the ceiling. This is the correct, content-blind
    /// behavior: an opaque caller whose payload happens to be exactly this
    /// shape gets the same answer any other 136-byte opaque payload would.
    #[tokio::test]
    async fn write_bytes_nonblocking_judges_two_concatenated_frames_by_total_length_not_by_parsing_them()
     {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9995".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9995, max_message_size)),
        );

        let payload_len = max_message_size - crate::framing::GOSSIP_HEADER_LEN;
        let make_frame = |fill: u8| {
            let payload = vec![fill; payload_len];
            let header = crate::framing::write_gossip_frame_prefix(payload.len());
            let mut frame = Vec::with_capacity(header.len() + payload.len());
            frame.extend_from_slice(&header);
            frame.extend_from_slice(&payload);
            frame
        };
        let mut both = make_frame(1);
        both.extend_from_slice(&make_frame(2));
        assert!(both.len() > max_message_size + crate::framing::LENGTH_PREFIX_LEN);

        let err = writer
            .write_bytes_nonblocking(bytes::Bytes::from(both))
            .unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge (bare length ceiling), got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// Genuinely unframed opaque bytes (content that would never decode as
    /// a V5 control word even if this lane parsed content, which it does
    /// not) must still be judged against the bare length ceiling -- the
    /// opaque lane must not become a way to bypass `max_message_size`
    /// altogether.
    #[tokio::test]
    async fn write_bytes_nonblocking_rejects_oversize_unframed_bytes() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9957".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9957, max_message_size)),
        );

        // The leading 4 bytes here are not a valid control word (kind bits
        // 31 has no `WireKind` mapping), so this can never decode as a
        // frame -- it must be judged by the bare length ceiling on the
        // whole buffer, like any other opaque blob.
        let mut opaque = vec![0xFFu8; max_message_size + 32];
        opaque[0] = 0xFF;
        assert!(
            crate::framing::decode_control(opaque[..4].try_into().unwrap()).is_none(),
            "test setup: the leading bytes must not decode as a valid control word"
        );

        let err = writer
            .write_bytes_nonblocking(bytes::Bytes::from(opaque))
            .unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// PR #183 review, round 10 (moved to the `Framed` lane in round 11):
    /// the adversarial sequence that used to defeat the size gate entirely
    /// -- enqueue a frame's 4-byte control word alone, declaring a body far
    /// larger than `max_message_size`, then supply the body across
    /// separate, individually small calls. Each call on its own used to
    /// look too short to be judged against the declared `body_len` (the
    /// walk fell through to a bare-length check on whatever few bytes were
    /// actually present), so every call passed while the peer reassembled
    /// the whole oversized frame from the continuous TCP stream -- separate
    /// `write()` calls have no boundary once the bytes are on the wire.
    ///
    /// The fix refuses the *first* call outright: a `Framed` write whose
    /// content begins a frame it does not complete is no longer a
    /// "not enough information yet" case, it is a rejected one. This test
    /// asserts exactly that -- the header-alone call must fail -- which is
    /// sufficient to close the sequence, since a caller who never gets past
    /// the first call can never reach the second.
    ///
    /// Round 11 moved this from `write_bytes_nonblocking` to
    /// `write_framed_bytes_nonblocking`: the vulnerability this closes only
    /// applies to a caller that has declared "this is a frame" and is
    /// splitting it across calls anyway. An *opaque* caller (`Single`)
    /// makes no such declaration and gets no such parsing -- see
    /// `write_bytes_nonblocking_does_not_reject_bytes_that_merely_look_like_an_incomplete_frame`
    /// below.
    #[tokio::test]
    async fn write_framed_bytes_nonblocking_rejects_a_frame_header_split_from_its_body_across_calls()
     {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9965".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9965, max_message_size)),
        );

        let oversized_body_len = max_message_size * 100;
        let header_alone = bytes::Bytes::copy_from_slice(&crate::framing::encode_control(
            crate::framing::WireKind::Gossip,
            oversized_body_len,
        ));
        assert_eq!(
            header_alone.len(),
            crate::framing::LENGTH_PREFIX_LEN,
            "test setup: this call supplies only the 4-byte control word, no body"
        );

        let err = writer
            .write_framed_bytes_nonblocking(header_alone)
            .unwrap_err();
        assert!(
            matches!(err, GossipError::Network(_)),
            "expected the header-alone call to be refused as an incomplete \
             frame, got {err:?}"
        );

        // The exploit's second half -- supplying the declared body across
        // further small `Framed` writes -- is unreachable once the first
        // call above is refused; a well-behaved caller stops there. Included
        // only to document the shape this closes, not because reaching it
        // would be meaningful: a caller that ignored the first error and
        // sent body fragments anyway would just be sending unrelated,
        // independently-judged `Framed` writes at that point.
        let body_fragment = bytes::Bytes::from(vec![0u8; 16]);
        let _ = writer.write_framed_bytes_nonblocking(body_fragment);

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// Parity for the ask lane: the same header-alone call must be refused
    /// through `write_framed_bytes_ask`, not just
    /// `write_framed_bytes_nonblocking`.
    #[tokio::test]
    async fn write_framed_bytes_ask_rejects_a_frame_header_split_from_its_body_across_calls() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9966".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9966, max_message_size)),
        );

        let oversized_body_len = max_message_size * 100;
        let header_alone = bytes::Bytes::copy_from_slice(&crate::framing::encode_control(
            crate::framing::WireKind::Gossip,
            oversized_body_len,
        ));

        let err = writer
            .write_framed_bytes_ask(header_alone)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GossipError::Network(_)),
            "expected the header-alone call to be refused as an incomplete \
             frame, got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// The rejection is not conditioned on size -- a header declaring a
    /// body that would itself fit under `max_message_size`, split from its
    /// body just the same, must also be refused. Otherwise the rule would
    /// only close the oversized case, leaving the general "frame split
    /// across public calls" shape open for anything small enough to slip
    /// under the ceiling. Moved to `write_framed_bytes_nonblocking` in
    /// round 11, same reasoning as the two tests above.
    #[tokio::test]
    async fn write_framed_bytes_nonblocking_rejects_an_incomplete_frame_even_when_body_len_is_within_max_message_size()
     {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9967".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9967, max_message_size)),
        );

        // Declares a 32-byte body -- comfortably under max_message_size --
        // but this call supplies only the 4-byte header, not the body.
        let header_alone = bytes::Bytes::copy_from_slice(&crate::framing::encode_control(
            crate::framing::WireKind::Gossip,
            32,
        ));

        let err = writer
            .write_framed_bytes_nonblocking(header_alone)
            .unwrap_err();
        assert!(
            matches!(err, GossipError::Network(_)),
            "an incomplete frame must be refused regardless of whether its \
             declared body would itself have fit under max_message_size, \
             got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// PR #183 review, round 11: the exact regression this round's finding
    /// identified. The same header-alone bytes that
    /// `write_framed_bytes_nonblocking` correctly refuses above (a
    /// caller-declared frame that doesn't supply its own body) must be
    /// *accepted* on the opaque `write_bytes_nonblocking` lane, because an
    /// opaque caller never declared this was a frame -- it is 4 bytes of
    /// arbitrary data that happen to decode as a plausible control word if
    /// parsed, and this lane does not parse. Real serialized payloads hit
    /// this pattern routinely (roughly 15/32 of random leading bytes land
    /// on a valid `WireKind`); a sender-side gate that refuses what the
    /// peer would accept as ordinary opaque payload bytes is its own
    /// defect, not a safety property.
    #[tokio::test]
    async fn write_bytes_nonblocking_does_not_reject_bytes_that_merely_look_like_an_incomplete_frame()
     {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9996".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9996, max_message_size)),
        );

        let oversized_body_len = max_message_size * 100;
        let opaque_payload = bytes::Bytes::copy_from_slice(&crate::framing::encode_control(
            crate::framing::WireKind::Gossip,
            oversized_body_len,
        ));
        let expected = opaque_payload.clone();

        writer.write_bytes_nonblocking(opaque_payload).expect(
            "an opaque write must not be refused just because it would decode as an \
             incomplete frame if parsed -- the opaque lane never parses",
        );

        let mut received = vec![0u8; expected.len()];
        AsyncReadExt::read_exact(&mut peer, &mut received)
            .await
            .expect("connection must deliver the opaque bytes to the peer");
        assert_eq!(received, expected.as_ref());

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }
}

/// PR #183 review, round 6: `write_buf_control`/`write_buf_ask` regained a
/// single-argument form (see the doc comment on `write_buf_control` above)
/// after round 2 had made both two-argument-only, breaking every
/// single-argument caller's build. These tests exercise that restored
/// one-argument call shape directly -- nothing else in this crate calls
/// `write_buf_control`/`write_buf_ask` without the second argument, so
/// without a test here the arity fix would have no coverage at all.
#[cfg(test)]
mod write_buf_control_single_arg_tests {
    use super::*;

    fn small_message_read_context(port: u16, max_message_size: usize) -> ReadContext {
        ReadContext {
            streaming_state_handoff: None,
            registry_weak: std::sync::Weak::new(),
            peer_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            session_source: format!("127.0.0.1:{port}").parse().unwrap(),
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
        }
    }

    fn make_writer(port: u16, max_message_size: usize) -> (LockFreeStreamHandle, JoinHandle<()>) {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            format!("127.0.0.1:{port}").parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(port, max_message_size)),
        );
        (writer, task)
    }

    /// The one-argument form must derive `expected_len` from `buf.remaining()`
    /// and still enforce `max_message_size` against it -- proving this is a
    /// real validating call shape, not a stub that happens to compile.
    #[tokio::test]
    async fn write_buf_control_single_arg_rejects_a_buf_over_max_message_size() {
        let (writer, task) = make_writer(9958, 64);
        // `body_len` is derived as `buf.remaining() - LENGTH_PREFIX_LEN`
        // (consistent with every other variant's size gate, which bounds
        // the post-control-word body, not the raw total) -- 69 remaining
        // bytes yields body_len 65, one past the 64-byte limit.
        let buf = bytes::Bytes::from(vec![0u8; 69]);
        let err = writer.write_buf_control(buf).await.unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { size: 65, max: 64 }),
            "expected MessageTooLarge{{size: 65, max: 64}} (69 bytes minus the \
             4-byte length-prefix allowance), got {err:?}"
        );
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// The one-argument form must still deliver a `buf` within
    /// `max_message_size` -- the arity restoration must not have turned it
    /// into an unconditional rejection either.
    #[tokio::test]
    async fn write_buf_control_single_arg_accepts_a_buf_within_max_message_size() {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9959".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9959, 64)),
        );
        let data = vec![9u8; 60];
        let buf = bytes::Bytes::from(data.clone());
        writer
            .write_buf_control(buf)
            .await
            .expect("a buf within max_message_size must be accepted");

        let mut received = vec![0u8; data.len()];
        AsyncReadExt::read_exact(&mut peer, &mut received)
            .await
            .expect("connection must deliver the buf to the peer");
        assert_eq!(received, data);

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// Parity check for the ask-lane sibling: same call shape, same
    /// derivation, same enforcement.
    #[tokio::test]
    async fn write_buf_ask_single_arg_rejects_a_buf_over_max_message_size() {
        let (writer, task) = make_writer(9960, 64);
        let buf = bytes::Bytes::from(vec![0u8; 69]);
        let err = writer.write_buf_ask(buf).await.unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { size: 65, max: 64 }),
            "expected MessageTooLarge{{size: 65, max: 64}}, got {err:?}"
        );
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }
}

/// PR #183 review, round 7: `write_chunked_nonblocking` used to validate
/// (and enqueue) each chunk as an independent `Single` write, so a frame
/// whose declared body exceeded `max_message_size` could be split into
/// pieces small enough that every individual fragment passed on its own --
/// and it discarded every per-chunk `Result`, always returning `Ok(())`
/// regardless of what actually reached the write queue.
#[cfg(test)]
mod write_chunked_nonblocking_tests {
    use super::*;

    fn small_message_read_context(port: u16, max_message_size: usize) -> ReadContext {
        ReadContext {
            streaming_state_handoff: None,
            registry_weak: std::sync::Weak::new(),
            peer_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            session_source: format!("127.0.0.1:{port}").parse().unwrap(),
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
        }
    }

    /// A frame whose declared body exceeds `max_message_size` must be
    /// rejected even when `chunk_size` is small enough that every
    /// individual fragment would pass the per-`Single` size gate on its
    /// own -- this is the shape that distinguishes up-front whole-buffer
    /// validation (rejects this) from the old per-fragment validation
    /// (every fragment individually looked fine, so nothing ever caught
    /// it).
    #[tokio::test]
    async fn write_chunked_nonblocking_rejects_an_oversize_frame_split_into_small_chunks() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9961".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9961, max_message_size)),
        );

        let payload_len = max_message_size - crate::framing::GOSSIP_HEADER_LEN + 1;
        let header = crate::framing::write_gossip_frame_prefix(payload_len);
        let mut frame = Vec::with_capacity(header.len() + payload_len);
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&vec![0u8; payload_len]);
        let control = crate::framing::decode_control(frame[..4].try_into().unwrap()).unwrap();
        assert!(
            control.body_len > max_message_size,
            "test setup: the frame's own declared body must exceed the limit"
        );

        // Every fragment here is far shorter than max_message_size (and
        // shorter than LENGTH_PREFIX_LEN for most of them), so a
        // per-fragment-only check would have accepted every single one.
        let chunk_size = 8;
        assert!(chunk_size < max_message_size);

        let err = writer
            .write_chunked_nonblocking(&frame, chunk_size)
            .unwrap_err();
        assert!(
            matches!(err, GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// A legitimately-sized buffer must still be delivered intact once
    /// chunked -- the up-front validation must not have turned this into
    /// an unconditional rejection.
    #[tokio::test]
    async fn write_chunked_nonblocking_delivers_a_buffer_within_max_message_size() {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 64;
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9962".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9962, max_message_size)),
        );

        // PR #183 review, round 11: `write_chunked_nonblocking`'s up-front
        // check is the same opaque bare-length ceiling `write_bytes_nonblocking`
        // uses -- content is never inspected, so an ordinary 0,1,2,3,...
        // sequence (which would have decoded as a valid-but-incomplete
        // Gossip control word if this were parsed as a frame) is unaffected.
        let data: Vec<u8> = (0u8..40).collect();
        writer
            .write_chunked_nonblocking(&data, 6)
            .expect("a buffer within max_message_size, chunked, must be accepted");

        let mut received = vec![0u8; data.len()];
        tokio::io::AsyncReadExt::read_exact(&mut peer, &mut received)
            .await
            .expect("connection must deliver every chunk to the peer");
        assert_eq!(received, data);

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// A later chunk's enqueue failure must surface as an error, not be
    /// silently discarded -- proving the caller can tell a chunked write
    /// only partially reached the wire, instead of being told (incorrectly)
    /// that it fully succeeded. Uses the queue-notify test hook to force
    /// `exit_flag` after exactly two chunks have already been pushed,
    /// deterministically reproducing "a later chunk fails after earlier
    /// chunks were already enqueued" without racing a real queue-full
    /// condition.
    ///
    /// PR #183 review, round 8: `queue_notify_hook` is process-global and
    /// fires for *every* queue's push, not just this connection's -- under
    /// default (parallel) test execution, this test's two siblings in this
    /// same module also call `write_chunked_nonblocking`, and their pushes
    /// used to fire this test's hook too, corrupting `successful_pushes`
    /// and making the `n == 1` trigger race. Two changes close that: this
    /// connection's own address (`9963`, distinct from every other test in
    /// this crate) scopes the hook so only *this* queue's pushes can fire
    /// it, and `queue_notify_hook::lock()` is held for the entire
    /// install-through-uninstall span so no other hook-installing test can
    /// overwrite or erase this one's entry in the shared slot while it is
    /// active. See the module doc comment on `queue_notify_hook` in
    /// `constants.rs` for the full reasoning.
    #[tokio::test]
    async fn write_chunked_nonblocking_surfaces_a_later_chunk_failure_instead_of_ok() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 4096;
        let addr: SocketAddr = "127.0.0.1:9963".parse().unwrap();
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(small_message_read_context(9963, max_message_size)),
        );

        let successful_pushes = Arc::new(AtomicUsize::new(0));
        let exit_flag = writer.exit_flag.clone();
        let successful_pushes_for_hook = successful_pushes.clone();
        let _hook_guard = queue_notify_hook::lock();
        queue_notify_hook::install(
            addr,
            Arc::new(move || {
                let n = successful_pushes_for_hook.fetch_add(1, Ordering::SeqCst);
                if n == 1 {
                    // Fires after the second chunk's push has already
                    // succeeded -- the third chunk's
                    // `enqueue_write_nonblocking` call must observe this and
                    // fail before pushing.
                    exit_flag.store(true, Ordering::Release);
                }
            }),
        );

        // Chunked content is unframed opaque bytes here (no valid V5
        // control word), so the up-front `reject_oversize_single` check
        // takes the bare-length fallback path and passes trivially -- this
        // test isolates the per-chunk enqueue failure, not the size gate.
        let data = vec![0xABu8; 40];
        let chunk_size = 10;
        let result = writer.write_chunked_nonblocking(&data, chunk_size);

        queue_notify_hook::uninstall();
        drop(_hook_guard);

        let err = result.unwrap_err();
        assert!(
            matches!(err, GossipError::ConnectionClosed(_)),
            "expected the third chunk's enqueue to observe exit_flag and \
             fail with ConnectionClosed, got {err:?}"
        );
        assert_eq!(
            successful_pushes.load(Ordering::SeqCst),
            2,
            "exactly two chunks must have been successfully enqueued before \
             the failure -- proving this is a *later* chunk failing, not the \
             first"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// PR #183 review, round 9: a `WriteQueueFull` on a later chunk after
    /// earlier chunks already succeeded is exactly the "recoverable-looking
    /// error" the coordinator warned about -- unlike `ConnectionClosed`, a
    /// caller could reasonably read `WriteQueueFull` as "try again later"
    /// and keep using this handle, letting its next write land right where
    /// this frame's missing tail should have been. The connection must
    /// instead come out of this call already poisoned: a subsequent write
    /// must fail too, not silently succeed onto a torn stream.
    ///
    /// Forces genuine backpressure (not a synthetic flag) by directly
    /// filling the write queue to exactly one slot short of capacity
    /// before calling `write_chunked_nonblocking`, on a single-threaded
    /// test runtime with no `.await` between the fill and the call -- the
    /// background writer task cannot drain anything in between, so the
    /// first chunk takes the last slot and the second genuinely finds the
    /// queue full.
    #[tokio::test]
    async fn write_chunked_nonblocking_poisons_the_connection_after_a_later_chunk_fails() {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let max_message_size = 4096;
        let addr: SocketAddr = "127.0.0.1:9964".parse().unwrap();
        let buffer_config = BufferConfig::default().with_write_queue_capacity(128);
        let write_queue_capacity = buffer_config.write_queue_capacity();
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            addr,
            ChannelId::TellAsk,
            buffer_config,
            None,
            Some(small_message_read_context(9964, max_message_size)),
        );

        // Leave exactly one free slot: the first chunk below takes it, the
        // second finds the queue genuinely full.
        for _ in 0..write_queue_capacity - 1 {
            writer
                .write_queue
                .try_push(WriteCommand::Payload(WritePayload::TrustedFrame(
                    bytes::Bytes::from_static(b"filler"),
                )))
                .expect("test setup: queue has capacity for the filler");
        }

        // Unframed opaque bytes (no valid V5 control word), so the
        // up-front `reject_oversize_single` check takes the bare-length
        // fallback and passes trivially -- this test isolates the
        // per-chunk enqueue failure, not the size gate.
        let data = vec![0xCDu8; 20];
        assert!(
            crate::framing::decode_control(data[..4].try_into().unwrap()).is_none(),
            "test setup: the leading bytes must not decode as a valid control word"
        );
        let chunk_size = 10;
        let result = writer.write_chunked_nonblocking(&data, chunk_size);

        let err = result.unwrap_err();
        assert!(
            matches!(err, GossipError::WriteQueueFull),
            "expected the second chunk to genuinely find the queue full, \
             got {err:?}"
        );

        // Drain the queue before checking the follow-up write: without
        // this, `next` below would fail merely because the queue is still
        // literally full (a coincidence of this test's setup), not because
        // the connection was poisoned -- that would pass whether or not
        // `write_chunked_nonblocking` poisons anything, proving nothing.
        // Draining leaves room to accept a write, so a subsequent failure
        // can only be explained by the poison, not by backpressure.
        while writer.write_queue.pop().is_some() {}

        // The connection must be poisoned, not merely reported as failed:
        // any further write on this handle must also fail, so nothing can
        // ever be appended after the torn frame that was already queued.
        let next = writer.write_bytes_nonblocking(bytes::Bytes::from_static(b"next"));
        assert!(
            next.is_err(),
            "a later write must not succeed onto a connection that already \
             had a torn frame queued on it, even with queue space free"
        );
        assert!(
            matches!(next.unwrap_err(), GossipError::Shutdown),
            "the later write must fail specifically because the connection \
             was poisoned (shutdown_signal), not because of unrelated \
             backpressure"
        );
        assert!(
            writer.shutdown_signal.load(Ordering::Acquire),
            "write_chunked_nonblocking must have poisoned the connection by \
             signaling shutdown once a later chunk failed after earlier \
             chunks were already enqueued"
        );

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }
}

/// PR #183 review, round 3: the declared-vs-actual check landed for `Buf`
/// only. The invariant is general -- **no `WritePayload` may be enqueued
/// whose real byte count disagrees with its own control word** -- and every
/// header-carrying variant is equally capable of a caller building a header
/// from one length while supplying `payload`/`prefix` pieces of a different
/// actual length. Each of these tests builds exactly that mismatch, in both
/// directions (actual longer than declared, and actual shorter), through
/// the real public `write_*` method for that variant, and asserts the
/// write is refused with something other than `MessageTooLarge` (proving
/// it is the mismatch check, not the size gate, doing the rejecting: every
/// payload here is tiny, far under the default `max_message_size`).
#[cfg(test)]
mod write_payload_length_mismatch_tests {
    use super::*;

    fn make_writer(port: u16) -> (LockFreeStreamHandle, JoinHandle<()>) {
        let (client, _peer) = tokio::io::duplex(64 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            format!("127.0.0.1:{port}").parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        (writer, task)
    }

    fn assert_mismatch_not_size(err: &GossipError) {
        assert!(
            !matches!(err, GossipError::MessageTooLarge { .. }),
            "expected the length-mismatch check to reject this, not the \
             size gate (every payload here is tiny): {err:?}"
        );
    }

    #[tokio::test]
    async fn header_payload_actual_longer_than_declared_is_rejected() {
        let (writer, task) = make_writer(9970);
        let header = bytes::Bytes::copy_from_slice(
            &crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 8)
                .unwrap(),
        );
        let payload = bytes::Bytes::from(vec![0u8; 100]); // header declares 8, actual 100
        let err = writer
            .write_header_and_payload_control(header, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_payload_actual_shorter_than_declared_is_rejected() {
        let (writer, task) = make_writer(9971);
        let header = bytes::Bytes::copy_from_slice(
            &crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 100)
                .unwrap(),
        );
        let payload = bytes::Bytes::from(vec![0u8; 8]); // header declares 100, actual 8
        let err = writer
            .write_header_and_payload_control(header, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_inline_actual_longer_than_declared_is_rejected() {
        let (writer, task) = make_writer(9972);
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 8)
                .unwrap();
        let payload = bytes::Bytes::from(vec![0u8; 100]);
        let err = writer
            .write_header_and_payload_control_inline(header, 16, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_inline_actual_shorter_than_declared_is_rejected() {
        let (writer, task) = make_writer(9973);
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 100)
                .unwrap();
        let payload = bytes::Bytes::from(vec![0u8; 8]);
        let err = writer
            .write_header_and_payload_control_inline(header, 16, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    fn aligned_payload(len: usize) -> crate::AlignedBytes {
        let pool = std::sync::Arc::new(crate::AlignedBytesPool::new(1));
        crate::AlignedBytes::from_pooled_slice(&vec![0u8; len], pool)
    }

    #[tokio::test]
    async fn header_inline_aligned_actual_longer_than_declared_is_rejected() {
        let (writer, task) = make_writer(9974);
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 8)
                .unwrap();
        let err = writer
            .write_header_and_payload_control_inline_aligned(header, 16, aligned_payload(100))
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_inline_aligned_actual_shorter_than_declared_is_rejected() {
        let (writer, task) = make_writer(9975);
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 100)
                .unwrap();
        let err = writer
            .write_header_and_payload_control_inline_aligned(header, 16, aligned_payload(8))
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_inline32_actual_longer_than_declared_is_rejected() {
        let (writer, task) = make_writer(9976);
        let header = crate::framing::try_write_actor_ask_header(1, 7, 9, 8).unwrap();
        let payload = bytes::Bytes::from(vec![0u8; 100]);
        let err = writer
            .write_header_and_payload_control_inline32(header, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_inline32_actual_shorter_than_declared_is_rejected() {
        let (writer, task) = make_writer(9977);
        let header = crate::framing::try_write_actor_ask_header(1, 7, 9, 100).unwrap();
        let payload = bytes::Bytes::from(vec![0u8; 8]);
        let err = writer
            .write_header_and_payload_control_inline32(header, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    fn pooled_payload(len: usize) -> crate::typed::PooledPayload {
        crate::typed::PooledPayload::try_from_pooled_bytes(len, |buf| buf.resize(len, 0u8))
            .expect("test pooled payload allocation")
    }

    #[tokio::test]
    async fn header_pooled_actual_longer_than_declared_is_rejected() {
        let (writer, task) = make_writer(9978);
        let header = bytes::Bytes::copy_from_slice(
            &crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 8)
                .unwrap(),
        );
        let err = writer
            .write_pooled_control(header, None, pooled_payload(100))
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_pooled_actual_shorter_than_declared_is_rejected() {
        let (writer, task) = make_writer(9979);
        let header = bytes::Bytes::copy_from_slice(
            &crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 100)
                .unwrap(),
        );
        let err = writer
            .write_pooled_control(header, None, pooled_payload(8))
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// `write_pooled_ask_inline` is the exact primitive
    /// `ask_responder::send_pooled_via_stream_handle` uses in production.
    #[tokio::test]
    async fn header_inline_pooled_actual_longer_than_declared_is_rejected() {
        let (writer, task) = make_writer(9980);
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 8)
                .unwrap();
        let err = writer
            .write_pooled_ask_inline(header, 16, None, 0, pooled_payload(100))
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn header_inline_pooled_actual_shorter_than_declared_is_rejected() {
        let (writer, task) = make_writer(9981);
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 100)
                .unwrap();
        let err = writer
            .write_pooled_ask_inline(header, 16, None, 0, pooled_payload(8))
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// A `prefix` declared `Some` must also be accounted for on its own --
    /// not just the payload. The header below declares a body sized for a
    /// 16-byte prefix plus an 8-byte payload; the payload actually matches
    /// (8 bytes), but `prefix_len` under-declares how much of the 16-byte
    /// `prefix` array the write loop will actually send (only its first 8
    /// bytes, per `prefix[prefix_off..prefix_len]` in `io_task`), so the
    /// real total is short by exactly the missing prefix bytes.
    #[tokio::test]
    async fn header_inline_pooled_prefix_len_mismatch_is_rejected() {
        let (writer, task) = make_writer(9982);
        // Header declares body_len = ASK_RESPONSE_HEADER_LEN + 16 (intended
        // prefix) + 8 (payload) = 36.
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 24)
                .unwrap();
        let prefix = [0u8; 16];
        // `prefix_len` claims only 8 of the 16 prefix bytes will be sent --
        // the payload alone is honest (8 bytes), so only this field is the
        // lie.
        let err = writer
            .write_pooled_ask_inline(header, 16, Some(prefix), 8, pooled_payload(8))
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn direct_ask_inline_actual_longer_than_declared_is_rejected() {
        let (writer, task) = make_writer(9983);
        let header = crate::framing::try_write_direct_ask_header(1, 1, 8).unwrap();
        let payload = bytes::Bytes::from(vec![0u8; 100]);
        let err = writer
            .write_direct_ask_inline(header, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    #[tokio::test]
    async fn direct_ask_inline_actual_shorter_than_declared_is_rejected() {
        let (writer, task) = make_writer(9984);
        let header = crate::framing::try_write_direct_ask_header(1, 1, 100).unwrap();
        let payload = bytes::Bytes::from(vec![0u8; 8]);
        let err = writer
            .write_direct_ask_inline(header, payload)
            .await
            .unwrap_err();
        assert_mismatch_not_size(&err);
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// A matching declared/actual length must not false-reject -- proving
    /// the mismatch checks above are not simply rejecting every write.
    #[tokio::test]
    async fn header_inline_pooled_with_matching_lengths_is_written() {
        let (writer, task) = make_writer(9985);
        let header =
            crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 8)
                .unwrap();
        writer
            .write_pooled_ask_inline(header, 16, None, 0, pooled_payload(8))
            .await
            .expect("matching declared and actual lengths must be accepted");
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }
}
